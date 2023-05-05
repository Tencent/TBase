/*-------------------------------------------------------------------------
 *
 * execRemote.c
 *
 *      Functions to execute commands on remote Datanodes
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *      src/backend/pgxc/pool/execRemote.c
 *
 *-------------------------------------------------------------------------
 */

#include <time.h>
#include "postgres.h"
#include "access/twophase.h"
#include "access/gtm.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/relscan.h"
#include "catalog/pg_type.h"
#include "catalog/pgxc_node.h"
#include "commands/prepare.h"
#include "executor/executor.h"
#include "gtm/gtm_c.h"
#include "libpq/libpq.h"
#include "miscadmin.h"
#include "pgxc/execRemote.h"
#include "tcop/tcopprot.h"
#include "executor/nodeSubplan.h"
#include "nodes/nodeFuncs.h"
#include "pgstat.h"
#include "nodes/nodes.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/var.h"
#include "pgxc/copyops.h"
#include "pgxc/nodemgr.h"
#include "pgxc/poolmgr.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_rusage.h"
#include "utils/tuplesort.h"
#include "utils/snapmgr.h"
#include "utils/builtins.h"
#include "tcop/utility.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "parser/parse_type.h"
#include "parser/parsetree.h"
#include "pgxc/xc_maintenance_mode.h"
#include "catalog/pgxc_class.h"
#ifdef __TBASE__
#include "commands/explain_dist.h"
#include "executor/execParallel.h"
#include "executor/nodeModifyTable.h"
#include "nodes/print.h"
#include "optimizer/pathnode.h"
#include "pgxc/squeue.h"
#include "postmaster/postmaster.h"
#include "utils/syscache.h"
#endif
/*
 * We do not want it too long, when query is terminating abnormally we just
 * want to read in already available data, if datanode connection will reach a
 * consistent state after that, we will go normal clean up procedure: send down
 * ABORT etc., if data node is not responding we will signal pooler to drop
 * the connection.
 * It is better to drop and recreate datanode connection then wait for several
 * seconds while it being cleaned up when, for example, cancelling query.
 */
#define END_QUERY_TIMEOUT    1000

/* Declarations used by guc.c */
int PGXLRemoteFetchSize;

#ifdef __TBASE__
int g_in_plpgsql_exec_fun = 0;
bool PlpgsqlDebugPrint = false;

bool need_global_snapshot = false;
List *executed_node_list = NULL;
#endif

#ifdef __TBASE__
/* GUC parameter */
int DataRowBufferSize = 0;  /* MBytes */

#define DATA_ROW_BUFFER_SIZE(n) (DataRowBufferSize * 1024 * 1024 * (n))
#endif

typedef struct
{
    xact_callback function;
    void *fparams;
} abort_callback_type;

struct find_params_context
{
    RemoteParam *rparams;
    Bitmapset *defineParams;
    List *subplans;
};

/*
 * Buffer size does not affect performance significantly, just do not allow
 * connection buffer grows infinitely
 */
#define COPY_BUFFER_SIZE 8192
#define PRIMARY_NODE_WRITEAHEAD 1024 * 1024

/*
 * Flag to track if a temporary object is accessed by the current transaction
 */
static bool temp_object_included = false;
static abort_callback_type dbcleanup_info = { NULL, NULL };

static int    pgxc_node_begin(int conn_count, PGXCNodeHandle ** connections,
				GlobalTransactionId gxid, bool need_tran_block, bool readOnly);

static PGXCNodeAllHandles *get_exec_connections(RemoteQueryState *planstate,
                     ExecNodes *exec_nodes,
                     RemoteQueryExecType exec_type,
                     bool is_global_session);


static bool pgxc_start_command_on_connection(PGXCNodeHandle *connection,
                    RemoteQueryState *remotestate, Snapshot snapshot);

static void pgxc_node_remote_count(int *dnCount, int dnNodeIds[],
        int *coordCount, int coordNodeIds[]);
static char *pgxc_node_remote_prepare(char *prepareGID, bool localNode, bool implicit);
static bool pgxc_node_remote_finish(char *prepareGID, bool commit,
                        char *nodestring, GlobalTransactionId gxid,
                        GlobalTransactionId prepare_gxid);
static bool
pgxc_node_remote_prefinish(char *prepareGID, char *nodestring);

#ifdef __TBASE__
/*static void pgxc_node_remote_abort_subtxn(void);*/
/*static void pgxc_node_remote_commit_subtxn(void);*/
static void pgxc_abort_connections(PGXCNodeAllHandles *all_handles);
static void pgxc_node_remote_commit(TranscationType txn_type, bool need_release_handle);
static void pgxc_node_remote_abort(TranscationType txn_type, bool need_release_handle);
static int pgxc_node_remote_commit_internal(PGXCNodeAllHandles *handles, TranscationType txn_type);
#endif

static void pgxc_connections_cleanup(ResponseCombiner *combiner);

static bool determine_param_types(Plan *plan,  struct find_params_context *context);


#define REMOVE_CURR_CONN(combiner) \
    if ((combiner)->current_conn < --((combiner)->conn_count)) \
    { \
        (combiner)->connections[(combiner)->current_conn] = \
                (combiner)->connections[(combiner)->conn_count]; \
    } \
    else \
        (combiner)->current_conn = 0

#define MAX_STATEMENTS_PER_TRAN 10

/* Variables to collect statistics */
static int    total_transactions = 0;
static int    total_statements = 0;
static int    total_autocommit = 0;
static int    nonautocommit_2pc = 0;
static int    autocommit_2pc = 0;
static int    current_tran_statements = 0;
static int *statements_per_transaction = NULL;
static int *nodes_per_transaction = NULL;

/*
 * statistics collection: count a statement
 */
static void
stat_statement()
{
    total_statements++;
    current_tran_statements++;
}

/*
 * clean memory related to stat transaction
 */
void
clean_stat_transaction(void)
{
	if(!nodes_per_transaction)
	{
		return ;
	}

	free(nodes_per_transaction);
	nodes_per_transaction = NULL;
}

/*
 * To collect statistics: count a transaction
 */
static void
stat_transaction(int node_count)
{
    total_transactions++;

    if (!statements_per_transaction)
    {
        statements_per_transaction = (int *) malloc((MAX_STATEMENTS_PER_TRAN + 1) * sizeof(int));
        memset(statements_per_transaction, 0, (MAX_STATEMENTS_PER_TRAN + 1) * sizeof(int));
    }
    if (current_tran_statements > MAX_STATEMENTS_PER_TRAN)
        statements_per_transaction[MAX_STATEMENTS_PER_TRAN]++;
    else
        statements_per_transaction[current_tran_statements]++;
    current_tran_statements = 0;
    if (node_count > 0 && node_count <= NumDataNodes)
    {
        if (!nodes_per_transaction)
        {
            nodes_per_transaction = (int *) malloc(NumDataNodes * sizeof(int));
            memset(nodes_per_transaction, 0, NumDataNodes * sizeof(int));
        }
        nodes_per_transaction[node_count - 1]++;
    }
}


/*
 * Output collected statistics to the log
 */
static void
stat_log()
{
    elog(DEBUG1, "Total Transactions: %d Total Statements: %d", total_transactions, total_statements);
    elog(DEBUG1, "Autocommit: %d 2PC for Autocommit: %d 2PC for non-Autocommit: %d",
         total_autocommit, autocommit_2pc, nonautocommit_2pc);
    if (total_transactions)
    {
        if (statements_per_transaction)
        {
            int            i;

            for (i = 0; i < MAX_STATEMENTS_PER_TRAN; i++)
                elog(DEBUG1, "%d Statements per Transaction: %d (%d%%)",
                     i, statements_per_transaction[i], statements_per_transaction[i] * 100 / total_transactions);

        elog(DEBUG1, "%d+ Statements per Transaction: %d (%d%%)",
             MAX_STATEMENTS_PER_TRAN, statements_per_transaction[MAX_STATEMENTS_PER_TRAN], statements_per_transaction[MAX_STATEMENTS_PER_TRAN] * 100 / total_transactions);
		}

        if (nodes_per_transaction)
        {
            int            i;

            for (i = 0; i < NumDataNodes; i++)
                elog(DEBUG1, "%d Nodes per Transaction: %d (%d%%)",
                     i + 1, nodes_per_transaction[i], nodes_per_transaction[i] * 100 / total_transactions);
        }
    }
}


/*
 * Create a structure to store parameters needed to combine responses from
 * multiple connections as well as state information
 */
void
InitResponseCombiner(ResponseCombiner *combiner, int node_count,
                       CombineType combine_type)
{
    combiner->node_count = node_count;
    combiner->connections = NULL;
    combiner->conn_count = 0;
    combiner->combine_type = combine_type;
    combiner->current_conn_rows_consumed = 0;
    combiner->command_complete_count = 0;
    combiner->request_type = REQUEST_TYPE_NOT_DEFINED;
    combiner->description_count = 0;
    combiner->copy_in_count = 0;
    combiner->copy_out_count = 0;
    combiner->copy_file = NULL;
    combiner->errorMessage = NULL;
    combiner->errorDetail = NULL;
    combiner->errorHint = NULL;
    combiner->tuple_desc = NULL;
    combiner->probing_primary = false;
    combiner->returning_node = InvalidOid;
    combiner->currentRow = NULL;
    combiner->rowBuffer = NIL;
    combiner->tapenodes = NULL;
    combiner->merge_sort = false;
    combiner->extended_query = false;
    combiner->tapemarks = NULL;
    combiner->tuplesortstate = NULL;
    combiner->cursor = NULL;
    combiner->update_cursor = NULL;
    combiner->cursor_count = 0;
    combiner->cursor_connections = NULL;
    combiner->remoteCopyType = REMOTE_COPY_NONE;
#ifdef __TBASE__
    combiner->dataRowBuffer  = NULL;
    combiner->dataRowMemSize = NULL;
    combiner->nDataRows      = NULL;
    combiner->tmpslot        = NULL;
    combiner->recv_datarows  = 0;
    combiner->prerowBuffers  = NULL;
    combiner->is_abort = false;
	combiner->recv_instr_htbl = NULL;
#endif
}


/*
 * Parse out row count from the command status response and convert it to integer
 */
static int
parse_row_count(const char *message, size_t len, uint64 *rowcount)
{
    int            digits = 0;
    int            pos;

    *rowcount = 0;
    /* skip \0 string terminator */
    for (pos = 0; pos < len - 1; pos++)
    {
        if (message[pos] >= '0' && message[pos] <= '9')
        {
            *rowcount = *rowcount * 10 + message[pos] - '0';
            digits++;
        }
        else
        {
            *rowcount = 0;
            digits = 0;
        }
    }
    return digits;
}

/*
 * Convert RowDescription message to a TupleDesc
 */
TupleDesc
create_tuple_desc(char *msg_body, size_t len)
{
    TupleDesc     result;
    int         i, nattr;
    uint16        n16;

    /* get number of attributes */
    memcpy(&n16, msg_body, 2);
    nattr = ntohs(n16);
    msg_body += 2;

    result = CreateTemplateTupleDesc(nattr, false);

    /* decode attributes */
    for (i = 1; i <= nattr; i++)
    {
        AttrNumber    attnum;
        char        *attname;
        char        *typname;
        Oid         oidtypeid;
        int32         typemode, typmod;

        attnum = (AttrNumber) i;

        /* attribute name */
        attname = msg_body;
        msg_body += strlen(attname) + 1;

        /* type name */
        typname = msg_body;
        msg_body += strlen(typname) + 1;

        /* table OID, ignored */
        msg_body += 4;

        /* column no, ignored */
        msg_body += 2;

        /* data type OID, ignored */
        msg_body += 4;

        /* type len, ignored */
        msg_body += 2;

        /* type mod */
        memcpy(&typemode, msg_body, 4);
        typmod = ntohl(typemode);
        msg_body += 4;

        /* PGXCTODO text/binary flag? */
        msg_body += 2;

        /* Get the OID type and mode type from typename */
        parseTypeString(typname, &oidtypeid, NULL, false);

        TupleDescInitEntry(result, attnum, attname, oidtypeid, typmod, 0);
    }
    return result;
}

/*
 * Handle CopyOutCommandComplete ('c') message from a Datanode connection
 */
static void
HandleCopyOutComplete(ResponseCombiner *combiner)
{
    if (combiner->request_type == REQUEST_TYPE_ERROR)
        return;
    if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
        combiner->request_type = REQUEST_TYPE_COPY_OUT;
    if (combiner->request_type != REQUEST_TYPE_COPY_OUT)
        /* Inconsistent responses */
        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                 errmsg("Unexpected response from the Datanodes for 'c' message, current request type %d", combiner->request_type)));
    /* Just do nothing, close message is managed by the Coordinator */
    combiner->copy_out_count++;
}

/*
 * Handle CommandComplete ('C') message from a Datanode connection
 */
static void
HandleCommandComplete(ResponseCombiner *combiner, char *msg_body, size_t len, PGXCNodeHandle *conn)
{// #lizard forgives
    int             digits = 0;
    EState           *estate = combiner->ss.ps.state;

    /*
     * If we did not receive description we are having rowcount or OK response
     */
    if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
        combiner->request_type = REQUEST_TYPE_COMMAND;
    /* Extract rowcount */
    if (combiner->combine_type != COMBINE_TYPE_NONE && estate)
    {
        uint64    rowcount;
        digits = parse_row_count(msg_body, len, &rowcount);
        if (digits > 0)
        {
            /* Replicated write, make sure they are the same */
            if (combiner->combine_type == COMBINE_TYPE_SAME)
            {
                if (combiner->command_complete_count)
                {
                    /*
                     * Replicated command may succeed on on node and fail on
                     * another. The example is if distributed table referenced
                     * by a foreign key constraint defined on a partitioned
                     * table. If command deletes rows from the replicated table
                     * they may be referenced on one Datanode but not on other.
                     * So, replicated command on each Datanode either affects
                     * proper number of rows, or returns error. Here if
                     * combiner got an error already, we allow to report it,
                     * not the scaring data corruption message.
                     */
                    if (combiner->errorMessage == NULL && rowcount != estate->es_processed)
#ifdef __TBASE__
                    {
                        /*
                          * In extend query protocol, need to set connection to idle
                          */
                        if (combiner->extended_query &&
                            conn->state == DN_CONNECTION_STATE_QUERY)                    
                        {
                            PGXCNodeSetConnectionState(conn, DN_CONNECTION_STATE_IDLE);
#ifdef     _PG_REGRESS_
                            elog(LOG, "RESPONSE_COMPLETE_2 set node %s, remote pid %d DN_CONNECTION_STATE_IDLE", conn->nodename, conn->backend_pid);
#endif
                        }
#endif
                        /* There is a consistency issue in the database with the replicated table */
                        ereport(ERROR,
                                (errcode(ERRCODE_DATA_CORRUPTED),
                                 errmsg("Write to replicated table returned different results from the Datanodes")));
#ifdef __TBASE__
                    }
#endif
                }
                else
                    /* first result */
                    estate->es_processed = rowcount;
            }
            else
                estate->es_processed += rowcount;
#ifdef __TBASE__
            combiner->DML_processed += rowcount;
#endif
        }
        else
            combiner->combine_type = COMBINE_TYPE_NONE;
    }

    /* If response checking is enable only then do further processing */
    if (conn->ck_resp_rollback)
    {
        if (strcmp(msg_body, "ROLLBACK") == 0)
        {
            /*
             * Subsequent clean up routine will be checking this flag
             * to determine nodes where to send ROLLBACK PREPARED.
             * On current node PREPARE has failed and the two-phase record
             * does not exist, so clean this flag as if PREPARE was not sent
             * to that node and avoid erroneous command.
             */
            conn->ck_resp_rollback = false;
            /*
             * Set the error, if none, to force throwing.
             * If there is error already, it will be thrown anyway, do not add
             * this potentially confusing message
             */
            if (combiner->errorMessage == NULL)
            {
                MemoryContext oldcontext = MemoryContextSwitchTo(ErrorContext);
                combiner->errorMessage =
                                pstrdup("unexpected ROLLBACK from remote node");
                MemoryContextSwitchTo(oldcontext);
                /*
                 * ERRMSG_PRODUCER_ERROR
                 * Messages with this code are replaced by others, if they are
                 * received, so if node will send relevant error message that
                 * one will be replaced.
                 */
                combiner->errorCode[0] = 'X';
                combiner->errorCode[1] = 'X';
                combiner->errorCode[2] = '0';
                combiner->errorCode[3] = '1';
                combiner->errorCode[4] = '0';
            }
        }
    }
    combiner->command_complete_count++;
}

/*
 * Handle RowDescription ('T') message from a Datanode connection
 */
static bool
HandleRowDescription(ResponseCombiner *combiner, char *msg_body, size_t len)
{
    if (combiner->request_type == REQUEST_TYPE_ERROR)
        return false;
    if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
        combiner->request_type = REQUEST_TYPE_QUERY;
    if (combiner->request_type != REQUEST_TYPE_QUERY)
    {
        /* Inconsistent responses */
        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                 errmsg("Unexpected response from the Datanodes for 'T' message, current request type %d", combiner->request_type)));
    }
	
	/* should ignore received tuple desc if already got one to avoid duplicate name issue */
	if (combiner->ss.ps.plan != NULL &&
		IsA(combiner->ss.ps.plan, RemoteQuery) &&
		((RemoteQuery *) combiner->ss.ps.plan)->ignore_tuple_desc)
	{
		return false;
	}
	
    /* Increment counter and check if it was first */
    if (combiner->description_count == 0)
    {
        combiner->description_count++;
        combiner->tuple_desc = create_tuple_desc(msg_body, len);
        return true;
    }
    combiner->description_count++;
    return false;
}

#ifdef __SUBSCRIPTION__
/*
 * Handle Apply ('4') message from a Datanode connection
 */
static void
HandleApplyDone(ResponseCombiner *combiner)
{
    if (combiner->request_type == REQUEST_TYPE_ERROR)
        return;
    if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
        combiner->request_type = REQUEST_TYPE_COMMAND;
    if (combiner->request_type != REQUEST_TYPE_COMMAND)
    {
        /* Inconsistent responses */
        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                 errmsg("Unexpected response from the Datanodes for '4' message, current request type %d", combiner->request_type)));
    }

    combiner->command_complete_count++;
}
#endif


/*
 * Handle CopyInResponse ('G') message from a Datanode connection
 */
static void
HandleCopyIn(ResponseCombiner *combiner)
{
    if (combiner->request_type == REQUEST_TYPE_ERROR)
        return;
    if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
        combiner->request_type = REQUEST_TYPE_COPY_IN;
    if (combiner->request_type != REQUEST_TYPE_COPY_IN)
    {
        /* Inconsistent responses */
        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                 errmsg("Unexpected response from the Datanodes for 'G' message, current request type %d", combiner->request_type)));
    }
    /*
     * The normal PG code will output an G message when it runs in the
     * Coordinator, so do not proxy message here, just count it.
     */
    combiner->copy_in_count++;
}

/*
 * Handle CopyOutResponse ('H') message from a Datanode connection
 */
static void
HandleCopyOut(ResponseCombiner *combiner)
{
    if (combiner->request_type == REQUEST_TYPE_ERROR)
        return;
    if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
        combiner->request_type = REQUEST_TYPE_COPY_OUT;
    if (combiner->request_type != REQUEST_TYPE_COPY_OUT)
    {
        /* Inconsistent responses */
        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                 errmsg("Unexpected response from the Datanodes for 'H' message, current request type %d", combiner->request_type)));
    }
    /*
     * The normal PG code will output an H message when it runs in the
     * Coordinator, so do not proxy message here, just count it.
     */
    combiner->copy_out_count++;
}

/*
 * Handle CopyOutDataRow ('d') message from a Datanode connection
 */
static void
HandleCopyDataRow(ResponseCombiner *combiner, char *msg_body, size_t len)
{// #lizard forgives
    if (combiner->request_type == REQUEST_TYPE_ERROR)
        return;
    if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
        combiner->request_type = REQUEST_TYPE_COPY_OUT;

    /* Inconsistent responses */
    if (combiner->request_type != REQUEST_TYPE_COPY_OUT)
        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                 errmsg("Unexpected response from the Datanodes for 'd' message, current request type %d", combiner->request_type)));

    /* count the row */
    combiner->processed++;

    /* Output remote COPY operation to correct location */
    switch (combiner->remoteCopyType)
    {
        case REMOTE_COPY_FILE:
            /* Write data directly to file */
            fwrite(msg_body, 1, len, combiner->copy_file);
            break;
        case REMOTE_COPY_STDOUT:
            /* Send back data to client */
            pq_putmessage('d', msg_body, len);
            break;
        case REMOTE_COPY_TUPLESTORE:
            /*
             * Do not store trailing \n character.
             * When tuplestore data are loaded to a table it automatically
             * inserts line ends.
             */
            tuplestore_putmessage(combiner->tuplestorestate, len-1, msg_body);
            break;
        case REMOTE_COPY_NONE:
        default:
            Assert(0); /* Should not happen */
    }
}

/*
 * Handle DataRow ('D') message from a Datanode connection
 * The function returns true if data row is accepted and successfully stored
 * within the combiner.
 */
static bool
HandleDataRow(ResponseCombiner *combiner, char *msg_body, size_t len, Oid node)
{// #lizard forgives
    /* We expect previous message is consumed */
    Assert(combiner->currentRow == NULL);

    if (combiner->request_type == REQUEST_TYPE_ERROR)
        return false;

    if (combiner->request_type != REQUEST_TYPE_QUERY)
    {
        /* Inconsistent responses */
        char data_buf[4096];
        
        snprintf(data_buf, len, "%s", msg_body);
        if(len > 4095)
            len = 4095;
        data_buf[len] = 0;
        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                 errmsg("Unexpected response from the data nodes for 'D' message, current request type %d, data %s",
                     combiner->request_type, data_buf)));
    }

    /*
     * If we got an error already ignore incoming data rows from other nodes
     * Still we want to continue reading until get CommandComplete
     */
    if (combiner->errorMessage)
        return false;

    /*
     * Replicated INSERT/UPDATE/DELETE with RETURNING: receive only tuples
     * from one node, skip others as duplicates
     */
    if (combiner->combine_type == COMBINE_TYPE_SAME)
    {
        /* Do not return rows when probing primary, instead return when doing
         * first normal node. Just save some CPU and traffic in case if
         * probing fails.
         */
        if (combiner->probing_primary)
            return false;
        if (OidIsValid(combiner->returning_node))
        {
            if (combiner->returning_node != node)
                return false;
        }
        else
            combiner->returning_node = node;
    }

    /*
     * We are copying message because it points into connection buffer, and
     * will be overwritten on next socket read
     */
    combiner->currentRow = (RemoteDataRow) palloc(sizeof(RemoteDataRowData) + len);
    memcpy(combiner->currentRow->msg, msg_body, len);
    combiner->currentRow->msglen = len;
    combiner->currentRow->msgnode = node;

    return true;
}

/*
 * Handle ErrorResponse ('E') message from a Datanode connection
 */
static void
HandleError(ResponseCombiner *combiner, char *msg_body, size_t len, PGXCNodeHandle *conn)
{// #lizard forgives
#ifdef __TBASE__
#define APPEND_LENGTH 128
    char *message_trans = NULL;
#endif
    /* parse error message */
    char *code = NULL;
    char *message = NULL;
    char *detail = NULL;
    char *hint = NULL;
    int   offset = 0;

    /*
     * Scan until point to terminating \0
     */
    while (offset + 1 < len)
    {
        /* pointer to the field message */
        char *str = msg_body + offset + 1;

        switch (msg_body[offset])
        {
            case 'C':    /* code */
                code = str;
                break;
            case 'M':    /* message */
                message = str;
                break;
            case 'D':    /* details */
                detail = str;
                break;

            case 'H':    /* hint */
                hint = str;
                break;

            /* Fields not yet in use */
            case 'S':    /* severity */
            case 'R':    /* routine */
            case 'P':    /* position string */
            case 'p':    /* position int */
            case 'q':    /* int query */
            case 'W':    /* where */
            case 'F':    /* file */
            case 'L':    /* line */
            default:
                break;
        }

        /* code, message and \0 */
        offset += strlen(str) + 2;
    }

    /*
     * We may have special handling for some errors, default handling is to
     * throw out error with the same message. We can not ereport immediately
     * because we should read from this and other connections until
     * ReadyForQuery is received, so we just store the error message.
     * If multiple connections return errors only first one is reported.
     *
     * The producer error may be hiding primary error, so if previously received
     * error is a producer error allow it to be overwritten.
     */
    if (combiner->errorMessage == NULL ||
            MAKE_SQLSTATE(combiner->errorCode[0], combiner->errorCode[1],
                          combiner->errorCode[2], combiner->errorCode[3],
                          combiner->errorCode[4]) == ERRCODE_PRODUCER_ERROR)
    {
        MemoryContext oldcontext = MemoryContextSwitchTo(ErrorContext);
#ifdef __TBASE__
#ifdef     _PG_REGRESS_
        (void)message_trans;
        combiner->errorMessage = pstrdup(message);
#else
        /* output more details */
        if (message)
        {
            message_trans = palloc(strlen(message)+APPEND_LENGTH);
            snprintf(message_trans, strlen(message)+APPEND_LENGTH,
                "nodename:%s,backend_pid:%d,message:%s", conn->nodename,conn->backend_pid,message);
        }
        
        combiner->errorMessage = message_trans;
#endif
#endif
        /* Error Code is exactly 5 significant bytes */
        if (code)
            memcpy(combiner->errorCode, code, 5);
        if (detail)
            combiner->errorDetail = pstrdup(detail);
        if (hint)
            combiner->errorHint = pstrdup(hint);
        MemoryContextSwitchTo(oldcontext);
    }

    /*
     * If the PREPARE TRANSACTION command fails for whatever reason, we don't
     * want to send down ROLLBACK PREPARED to this node. Otherwise, it may end
     * up rolling back an unrelated prepared transaction with the same GID as
     * used by this transaction
     */
    if (conn->ck_resp_rollback)
        conn->ck_resp_rollback = false;

    /*
     * If Datanode have sent ErrorResponse it will never send CommandComplete.
     * Increment the counter to prevent endless waiting for it.
     */
    combiner->command_complete_count++;
}

/*
 * HandleCmdComplete -
 *    combine deparsed sql statements execution results
 *
 * Input parameters:
 *    commandType is dml command type
 *    combineTag is used to combine the completion result
 *    msg_body is execution result needed to combine
 *    len is msg_body size
 */
void
HandleCmdComplete(CmdType commandType, CombineTag *combine,
                        const char *msg_body, size_t len)
{// #lizard forgives
    int    digits = 0;
    uint64    originrowcount = 0;
    uint64    rowcount = 0;
    uint64    total = 0;

    if (msg_body == NULL)
        return;

    /* if there's nothing in combine, just copy the msg_body */
    if (strlen(combine->data) == 0)
    {
        strcpy(combine->data, msg_body);
        combine->cmdType = commandType;
        return;
    }
    else
    {
        /* commandType is conflict */
        if (combine->cmdType != commandType)
            return;

        /* get the processed row number from msg_body */
        digits = parse_row_count(msg_body, len + 1, &rowcount);
        elog(DEBUG1, "digits is %d\n", digits);
        Assert(digits >= 0);

        /* no need to combine */
        if (digits == 0)
            return;

        /* combine the processed row number */
        parse_row_count(combine->data, strlen(combine->data) + 1, &originrowcount);
        elog(DEBUG1, "originrowcount is %lu, rowcount is %lu\n", originrowcount, rowcount);
        total = originrowcount + rowcount;

    }

    /* output command completion tag */
    switch (commandType)
    {
        case CMD_SELECT:
            strcpy(combine->data, "SELECT");
            break;
        case CMD_INSERT:
            snprintf(combine->data, COMPLETION_TAG_BUFSIZE,
               "INSERT %u %lu", 0, total);
            break;
        case CMD_UPDATE:
            snprintf(combine->data, COMPLETION_TAG_BUFSIZE,
                     "UPDATE %lu", total);
            break;
        case CMD_DELETE:
            snprintf(combine->data, COMPLETION_TAG_BUFSIZE,
                     "DELETE %lu", total);
            break;
        default:
            strcpy(combine->data, "");
            break;
    }

}

/*
 * HandleDatanodeCommandId ('M') message from a Datanode connection
 */
static void
HandleDatanodeCommandId(ResponseCombiner *combiner, char *msg_body, size_t len)
{
    uint32        n32;
    CommandId    cid;

    Assert(msg_body != NULL);
    Assert(len >= 2);

    /* Get the command Id */
    memcpy(&n32, &msg_body[0], 4);
    cid = ntohl(n32);

    /* If received command Id is higher than current one, set it to a new value */
    if (cid > GetReceivedCommandId())
        SetReceivedCommandId(cid);
}

/*
 * Record waited-for XIDs received from the remote nodes into the transaction
 * state
 */
static void
HandleWaitXids(char *msg_body, size_t len)
{
    int xid_count;
    uint32        n32;
    int cur;
    int i;

    /* Get the xid count */
    xid_count = len / sizeof (TransactionId);

    cur = 0;
    for (i = 0; i < xid_count; i++)
    {
        Assert(cur < len);
        memcpy(&n32, &msg_body[cur], sizeof (TransactionId));
        cur = cur + sizeof (TransactionId);
        TransactionRecordXidWait(ntohl(n32));
    }
}

#ifdef __USE_GLOBAL_SNAPSHOT__
static void
HandleGlobalTransactionId(char *msg_body, size_t len)
{
    GlobalTransactionId xid;

    Assert(len == sizeof (GlobalTransactionId));
    memcpy(&xid, &msg_body[0], sizeof (GlobalTransactionId));

    SetTopTransactionId(xid);
}
#endif
/*
 * Examine the specified combiner state and determine if command was completed
 * successfully
 */
bool
validate_combiner(ResponseCombiner *combiner)
{// #lizard forgives
    /* There was error message while combining */
    if (combiner->errorMessage)
    {
		elog(LOG, "validate_combiner there is errorMessage in combiner, errorMessage: %s", combiner->errorMessage);
        return false;
    }
    /* Check if state is defined */
    if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
    {
        elog(LOG, "validate_combiner request_type is REQUEST_TYPE_NOT_DEFINED");
        return false;
    }

    /* Check all nodes completed */
    if ((combiner->request_type == REQUEST_TYPE_COMMAND
            || combiner->request_type == REQUEST_TYPE_QUERY)
            && combiner->command_complete_count != combiner->node_count)
    {
		elog(DEBUG1, "validate_combiner request_type is %d, command_complete_count:%d not equal node_count:%d", combiner->request_type, combiner->command_complete_count, combiner->node_count);
        return false;
    }

    /* Check count of description responses */
    if ((combiner->request_type == REQUEST_TYPE_QUERY && combiner->description_count != combiner->node_count) 
        && !IsA(combiner,RemoteSubplanState))
    {
        elog(LOG, "validate_combiner request_type is REQUEST_TYPE_QUERY, description_count:%d not equal node_count:%d", combiner->description_count, combiner->node_count);
        return false;
    }

    /* Check count of copy-in responses */
    if (combiner->request_type == REQUEST_TYPE_COPY_IN
            && combiner->copy_in_count != combiner->node_count)
    {
        elog(LOG, "validate_combiner request_type is REQUEST_TYPE_COPY_IN, copy_in_count:%d not equal node_count:%d", combiner->copy_in_count, combiner->node_count);
        return false;
    }

    /* Check count of copy-out responses */
    if (combiner->request_type == REQUEST_TYPE_COPY_OUT
            && combiner->copy_out_count != combiner->node_count)
    {
        elog(LOG, "validate_combiner request_type is REQUEST_TYPE_COPY_OUT, copy_out_count:%d not equal node_count:%d", combiner->copy_out_count, combiner->node_count);
        return false;
    }

    /* Add other checks here as needed */

    /* All is good if we are here */
    return true;
}

/*
 * Close combiner and free allocated memory, if it is not needed
 */
void
CloseCombiner(ResponseCombiner *combiner)
{// #lizard forgives
    /* in some cases the memory is allocated in connections handle, we can not free it and it is not necessary to free it here, because the memory context will be reset. */
#if 0
    if (combiner->connections)
    {
        pfree(combiner->connections);
        combiner->connections = NULL;
    }
#endif
    if (combiner->tuple_desc)
    {
        FreeTupleDesc(combiner->tuple_desc);
        combiner->tuple_desc = NULL;
    }
    if (combiner->errorMessage)
    {
        pfree(combiner->errorMessage);
        combiner->errorMessage = NULL;
    }
    if (combiner->errorDetail)
    {
        pfree(combiner->errorDetail);
        combiner->errorDetail = NULL;
    }
    if (combiner->errorHint)
    {
        pfree(combiner->errorHint);
        combiner->errorHint = NULL;
    }
    if (combiner->cursor_connections)
    {
        pfree(combiner->cursor_connections);
        combiner->cursor_connections = NULL;
    }
    if (combiner->tapenodes)
    {
        pfree(combiner->tapenodes);
        combiner->tapenodes = NULL;
    }
    if (combiner->tapemarks)
    {
        pfree(combiner->tapemarks);
        combiner->tapemarks = NULL;
    }
#ifdef __TBASE__
	if (combiner->recv_instr_htbl)
	{
		hash_destroy(combiner->recv_instr_htbl);
		combiner->recv_instr_htbl = NULL;
	}
#endif
}

/*
 * Validate combiner and release storage freeing allocated memory
 */
static bool
ValidateAndCloseCombiner(ResponseCombiner *combiner)
{
    bool        valid = validate_combiner(combiner);

    CloseCombiner(combiner);

    return valid;
}

/*
 * It is possible if multiple steps share the same Datanode connection, when
 * executor is running multi-step query or client is running multiple queries
 * using Extended Query Protocol. After returning next tuple ExecRemoteQuery
 * function passes execution control to the executor and then it can be given
 * to the same RemoteQuery or to different one. It is possible that before
 * returning a tuple the function do not read all Datanode responses. In this
 * case pending responses should be read in context of original RemoteQueryState
 * till ReadyForQuery message and data rows should be stored (buffered) to be
 * available when fetch from that RemoteQueryState is requested again.
 * BufferConnection function does the job.
 * If a RemoteQuery is going to use connection it should check connection state.
 * DN_CONNECTION_STATE_QUERY indicates query has data to read and combiner
 * points to the original RemoteQueryState. If combiner differs from "this" the
 * connection should be buffered.
 */
void
BufferConnection(PGXCNodeHandle *conn, bool need_prefetch)
{
    ResponseCombiner *combiner = conn->combiner;
    MemoryContext oldcontext;

    if (combiner == NULL || conn->state != DN_CONNECTION_STATE_QUERY)
    {
        return;
    }

    elog(DEBUG2, "Buffer connection %u to step %s", conn->nodeoid, combiner->cursor);

    /*
     * When BufferConnection is invoked CurrentContext is related to other
     * portal, which is trying to control the connection.
     * TODO See if we can find better context to switch to
     */
    oldcontext = MemoryContextSwitchTo(combiner->ss.ps.ps_ResultTupleSlot->tts_mcxt);

    /* Verify the connection is in use by the combiner */
    combiner->current_conn = 0;
    while (combiner->current_conn < combiner->conn_count)
    {
        if (combiner->connections[combiner->current_conn] == conn)
            break;
        combiner->current_conn++;
    }
    Assert(combiner->current_conn < combiner->conn_count);

    if (combiner->tapemarks == NULL)
        combiner->tapemarks = (ListCell**) palloc0(combiner->conn_count * sizeof(ListCell*));

    /*
     * If current bookmark for the current tape is not set it means either
     * first row in the buffer is from the current tape or no rows from
     * the tape in the buffer, so if first row is not from current
     * connection bookmark the last cell in the list.
     */
    if (combiner->tapemarks[combiner->current_conn] == NULL &&
            list_length(combiner->rowBuffer) > 0)
    {
        RemoteDataRow dataRow = (RemoteDataRow) linitial(combiner->rowBuffer);
        if (dataRow->msgnode != conn->nodeoid)
            combiner->tapemarks[combiner->current_conn] = list_tail(combiner->rowBuffer);
    }
    
    /*
     * Buffer data rows until data node return number of rows specified by the
     * fetch_size parameter of last Execute message (PortalSuspended message)
     * or end of result set is reached (CommandComplete message)
     */
    while (true)
    {
        int res;

        /* Move to buffer currentRow (received from the data node) */
        if (combiner->currentRow)
        {
#ifdef __TBASE__
            if (combiner->merge_sort)
            {
                int node_index = combiner->current_conn;
                
                if (combiner->dataRowBuffer && combiner->dataRowBuffer[node_index])
                {
                    combiner->tmpslot->tts_datarow = combiner->currentRow;

                    tuplestore_puttupleslot(combiner->dataRowBuffer[node_index], combiner->tmpslot);
                    
                    combiner->nDataRows[node_index]++;

                    pfree(combiner->currentRow);
                }
                else
                {
                    if (!combiner->prerowBuffers)
                    {
                        combiner->prerowBuffers = (List **)palloc0(combiner->conn_count * sizeof(List*));
                    }

                    if (!combiner->dataRowMemSize)
                    {
                        combiner->dataRowMemSize = (long *)palloc0(sizeof(long) * combiner->conn_count);
                    }
                            
                    combiner->prerowBuffers[node_index] = lappend(combiner->prerowBuffers[node_index],
                                                                    combiner->currentRow);

                    combiner->dataRowMemSize[node_index] += combiner->currentRow->msglen;

                    if (combiner->dataRowMemSize[node_index] >= DATA_ROW_BUFFER_SIZE(1))
                    {
                        /* init datarow buffer */
                        if (!combiner->dataRowBuffer)
                        {
                            combiner->dataRowBuffer = (Tuplestorestate **)palloc0(sizeof(Tuplestorestate *) * combiner->conn_count);
                        }

                        if (!combiner->dataRowBuffer[node_index])
                        {
                            combiner->dataRowBuffer[node_index] = tuplestore_begin_datarow(false, work_mem, NULL);

                            if (enable_statistic)
                            {
                                elog(LOG, "BufferConnection:connection %d dataRowMemSize %ld exceed max rowBufferSize %d, need to store"
                                      " datarow in tuplestore.", node_index, combiner->dataRowMemSize[node_index],
                                      DATA_ROW_BUFFER_SIZE(1));
                            }
                        }
                        
                        /* data row count in tuplestore */
                        if (!combiner->nDataRows)
                        {
                            combiner->nDataRows = (int *)palloc0(sizeof(int) * combiner->conn_count);
                        }

                        if (!combiner->tmpslot)
                        {
                            TupleDesc desc = CreateTemplateTupleDesc(1, false);
                            combiner->tmpslot = MakeSingleTupleTableSlot(desc);
                        }
                    }
                }
            }
            else
            {
#endif        
                if (combiner->dataRowBuffer && combiner->dataRowBuffer[0])
                {
                    /* put into tuplestore */
                    combiner->tmpslot->tts_datarow = combiner->currentRow;

                    tuplestore_puttupleslot(combiner->dataRowBuffer[0], combiner->tmpslot);
                    
                    combiner->nDataRows[0]++;

                    pfree(combiner->currentRow);
                }
                else
                {
                    /* init datarow size */
                    if (!combiner->dataRowMemSize)
                    {
                        combiner->dataRowMemSize = (long *)palloc(sizeof(long));
                        combiner->dataRowMemSize[0] = 0;
                    }
                    
                    combiner->rowBuffer = lappend(combiner->rowBuffer,
                                                  combiner->currentRow);

                    combiner->dataRowMemSize[0] += combiner->currentRow->msglen;

                    if (combiner->dataRowMemSize[0] >= DATA_ROW_BUFFER_SIZE(1))
                    {
                                            /* init datarow buffer */
                        if (!combiner->dataRowBuffer)
                        {
                            combiner->dataRowBuffer = (Tuplestorestate **)palloc0(sizeof(Tuplestorestate *));
                        }

                        if (!combiner->dataRowBuffer[0])
                        {
                            combiner->dataRowBuffer[0] = tuplestore_begin_datarow(false, work_mem, NULL);

                            if (enable_statistic)
                            {
                                elog(LOG, "BufferConnection:dataRowMemSize %ld exceed max rowBufferSize %d, need to store"
                                      " datarow in tuplestore.", combiner->dataRowMemSize[0],
                                      DATA_ROW_BUFFER_SIZE(1));
                            }
                        }

                        /* data row count in tuplestore */
                        if (!combiner->nDataRows)
                        {
                            combiner->nDataRows = (int *)palloc(sizeof(int));
                            combiner->nDataRows[0] = 0;
                        }

                        if (!combiner->tmpslot)
                        {
                            TupleDesc desc = CreateTemplateTupleDesc(1, false);
                            combiner->tmpslot = MakeSingleTupleTableSlot(desc);
                        }
                    }
                }
#ifdef __TBASE__
            }
#endif
            combiner->currentRow = NULL;
        }

        res = handle_response(conn, combiner);
        /*
         * If response message is a DataRow it will be handled on the next
         * iteration.
         * PortalSuspended will cause connection state change and break the loop
         * The same is for CommandComplete, but we need additional handling -
         * remove connection from the list of active connections.
         * We may need to add handling error response
         */

        /* Most often result check first */
        if (res == RESPONSE_DATAROW)
        {
            /*
             * The row is in the combiner->currentRow, on next iteration it will
             * be moved to the buffer
             */
            continue;
        }

        if (res == RESPONSE_EOF)
        {
#ifdef __TBASE__
		    if (need_prefetch)
            {
                /*
                 * We encountered incomplete message, try to read more.
                 * Here if we read timeout, then we move to other connections to read, because we
                 * easily got deadlock if a specific cursor run as producer on two nodes. If we can
                 * consume data from all all connections, we can break the deadlock loop.
                 */
                bool   bComplete          = false;
                DNConnectionState state   = DN_CONNECTION_STATE_IDLE;
                int    i                  = 0;
                int    ret 			      = 0;
                PGXCNodeHandle *save_conn = NULL;
                struct timeval timeout;
                timeout.tv_sec  	      = 0;
                timeout.tv_usec 	      = 1000;

                save_conn = conn;
                while (1)
                {
                    conn  = save_conn;
                    state = conn->state; /* Save the connection state. */
                    ret   = pgxc_node_receive(1, &conn, &timeout);
                    if (DNStatus_OK == ret)
                    {
                        /* We got data, handle it. */
                        break;
                    }
                    else if (DNStatus_ERR == ret)
                    {
                        ereport(ERROR,
                                (errcode(ERRCODE_INTERNAL_ERROR),
                                        errmsg("Failed to receive more data from data node %u", conn->nodeoid)));
                    }
                    else
                    {
                        /* Restore the saved state of connection. */
                        conn->state = state;
                    }

                    /* Try to read data from other connections. */
                    for (i = 0; i < combiner->conn_count; i ++)
                    {
                        conn  = combiner->connections[i];
                        if (save_conn != conn && conn != NULL)
                        {
                            /* Save the connection state. */
                            state = conn->state;
                            if (state == DN_CONNECTION_STATE_QUERY)
                            {
                                ret = pgxc_node_receive(1, &conn, &timeout);
                                if (DNStatus_OK == ret)
                                {
                                    /* We got data, prefetch it. */
                                    bComplete = PreFetchConnection(conn, i);
                                    if (bComplete)
                                    {
                                        /* Receive Complete on one connection, we need retry to read from current_conn. */
                                        break;
                                    }
                                    else
                                    {
                                        /* Maybe Suspend or Expired, just move to next connection and read. */
                                        continue;
                                    }
                                }
                                else if (DNStatus_EXPIRED == ret)
                                {
                                    /* Restore the saved state of connection. */
                                    conn->state = state;
                                    continue;
                                }
                                else
                                {
                                    ereport(ERROR,
                                            (errcode(ERRCODE_INTERNAL_ERROR),
                                                    errmsg("Failed to receive more data from data node %u", conn->nodeoid)));
                                }
                            }
                        }
                    }
                }
                continue;
            }
            else
            {
                /* incomplete message, read more */
                if (pgxc_node_receive(1, &conn, NULL))
                {
                    PGXCNodeSetConnectionState(conn,
                                               DN_CONNECTION_STATE_ERROR_FATAL);
                    add_error_message(conn, "Failed to fetch from data node");
                }
            }
#else
            /* incomplete message, read more */
            if (pgxc_node_receive(1, &conn, NULL))
            {
                PGXCNodeSetConnectionState(conn,
                        DN_CONNECTION_STATE_ERROR_FATAL);
                add_error_message(conn, "Failed to fetch from data node");
            }
#endif
        }

        /*
         * End of result set is reached, so either set the pointer to the
         * connection to NULL (combiner with sort) or remove it from the list
         * (combiner without sort)
         */
        else if (res == RESPONSE_COMPLETE)
        {
            /*
             * If combiner is doing merge sort we should set reference to the
             * current connection to NULL in the array, indicating the end
             * of the tape is reached. FetchTuple will try to access the buffer
             * first anyway.
             * Since we remove that reference we can not determine what node
             * number was this connection, but we need this info to find proper
             * tuple in the buffer if we are doing merge sort. So store node
             * number in special array.
             * NB: We can not test if combiner->tuplesortstate is set here:
             * connection may require buffering inside tuplesort_begin_merge
             * - while pre-read rows from the tapes, one of the tapes may be
             * the local connection with RemoteSubplan in the tree. The
             * combiner->tuplesortstate is set only after tuplesort_begin_merge
             * returns.
             */
            if (combiner->merge_sort)
            {
                combiner->connections[combiner->current_conn] = NULL;
                if (combiner->tapenodes == NULL)
                    combiner->tapenodes = (Oid *)
                            palloc0(combiner->conn_count * sizeof(Oid));
                combiner->tapenodes[combiner->current_conn] = conn->nodeoid;
            }
            else
            {
                /* Remove current connection, move last in-place, adjust current_conn */
                if (combiner->current_conn < --combiner->conn_count)
                    combiner->connections[combiner->current_conn] = combiner->connections[combiner->conn_count];
                else
                    combiner->current_conn = 0;
            }
            /*
             * If combiner runs Simple Query Protocol we need to read in
             * ReadyForQuery. In case of Extended Query Protocol it is not
             * sent and we should quit.
             */
            if (combiner->extended_query)
                break;
#ifdef __TBASE__
            if (conn->state == DN_CONNECTION_STATE_ERROR_FATAL)
            {
                ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Unexpected FATAL ERROR on Connection to Datanode %s pid %d",
                             conn->nodename, conn->backend_pid)));
            }
#endif
        }
        else if (res == RESPONSE_ERROR)
        {
            if (combiner->extended_query)
            {
                /*
                 * Need to sync connection to enable receiving commands
                 * by the datanode
                 */
                if (pgxc_node_send_sync(conn) != 0)
                {
                    ereport(ERROR,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("Failed to sync msg to node %s backend_pid:%d", conn->nodename, conn->backend_pid)));
                }
#ifdef _PG_REGRESS_
                ereport(LOG,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("Succeed to sync msg to node %s backend_pid:%d", conn->nodename, conn->backend_pid)));                    
#endif

            }
        }
        else if (res == RESPONSE_SUSPENDED || res == RESPONSE_READY)
        {
            /* Now it is OK to quit */
            break;
        }
    }
    Assert(conn->state != DN_CONNECTION_STATE_QUERY);
    MemoryContextSwitchTo(oldcontext);
    conn->combiner = NULL;
}
/*
 * Prefetch data from specific connection.
 */
bool PreFetchConnection(PGXCNodeHandle *conn, int32 node_index)
{// #lizard forgives
    bool              bComplete  = false;
    DNConnectionState state   = DN_CONNECTION_STATE_IDLE;
    int                  ret          = 0;
    ResponseCombiner *combiner   = conn->combiner;
    MemoryContext     oldcontext;
    struct timeval timeout;
    timeout.tv_sec              = 0;
    timeout.tv_usec           = 1000;

    
    if (combiner == NULL)
    {
        return bComplete;
    }


    elog(DEBUG1, "PreFetchConnection connection %u to step %s", conn->nodeoid, combiner->cursor);

    
    /*
     * When BufferConnection is invoked CurrentContext is related to other
     * portal, which is trying to control the connection.
     * TODO See if we can find better context to switch to
     */
    oldcontext = MemoryContextSwitchTo(combiner->ss.ps.ps_ResultTupleSlot->tts_mcxt);    
    if (combiner->tapemarks == NULL)
    {
        combiner->tapemarks = (ListCell**) palloc0(combiner->conn_count * sizeof(ListCell*));
    }

    /*
     * If current bookmark for the current tape is not set it means either
     * first row in the buffer is from the current tape or no rows from
     * the tape in the buffer, so if first row is not from current
     * connection bookmark the last cell in the list.
     */
    if (combiner->tapemarks[node_index] == NULL &&
            list_length(combiner->rowBuffer) > 0)
    {
        RemoteDataRow dataRow = (RemoteDataRow) linitial(combiner->rowBuffer);
        if (dataRow->msgnode != conn->nodeoid)
        {
            combiner->tapemarks[node_index] = list_tail(combiner->rowBuffer);
        }
    }

    if (combiner->merge_sort)
    {
        if (!combiner->prerowBuffers)
            combiner->prerowBuffers = (List **)palloc0(combiner->conn_count * sizeof(List*));
    }

    /*
     * Buffer data rows until data node return number of rows specified by the
     * fetch_size parameter of last Execute message (PortalSuspended message)
     * or end of result set is reached (CommandComplete message)
     */
    while (true)
    {
        int res;

        /* Move to buffer currentRow (received from the data node) */
        if (combiner->currentRow)
        {
            int msglen = combiner->currentRow->msglen;

            if (combiner->merge_sort)
            {
                if (!combiner->dataRowMemSize)
                {
                    combiner->dataRowMemSize = (long *)palloc0(sizeof(long) * combiner->conn_count);
                }

                if (combiner->dataRowMemSize[node_index] >= DATA_ROW_BUFFER_SIZE(1))
                {
                    /* init datarow buffer */
                    if (!combiner->dataRowBuffer)
                    {
                        combiner->dataRowBuffer = (Tuplestorestate **)palloc0(sizeof(Tuplestorestate *) * combiner->conn_count);
                    }

                    if (!combiner->dataRowBuffer[node_index])
                    {
                        combiner->dataRowBuffer[node_index] = tuplestore_begin_datarow(false, work_mem / NumDataNodes, NULL);

                        if (enable_statistic)
                        {
                            elog(LOG, "PreFetchConnection:connection %d dataRowMemSize %ld exceed max rowBufferSize %d, need to store"
                                  " datarow in tuplestore.", node_index, combiner->dataRowMemSize[node_index],
                                  DATA_ROW_BUFFER_SIZE(1));
                        }
                    }
                    
                    /* data row count in tuplestore */
                    if (!combiner->nDataRows)
                    {
                        combiner->nDataRows = (int *)palloc0(sizeof(int) * combiner->conn_count);
                    }

                    if (!combiner->tmpslot)
                    {
                        TupleDesc desc = CreateTemplateTupleDesc(1, false);
                        combiner->tmpslot = MakeSingleTupleTableSlot(desc);
                    }

                    combiner->tmpslot->tts_datarow = combiner->currentRow;

                    tuplestore_puttupleslot(combiner->dataRowBuffer[node_index], combiner->tmpslot);
                    
                    combiner->nDataRows[node_index]++;

                    pfree(combiner->currentRow);

                    combiner->currentRow = NULL;
                }
                else
                {
                    combiner->prerowBuffers[node_index] = lappend(combiner->prerowBuffers[node_index],
                                                                    combiner->currentRow);

                    combiner->dataRowMemSize[node_index] += msglen;

                    combiner->currentRow = NULL;
                }
            }
            else
            {
                /* init datarow size */
                if (!combiner->dataRowMemSize)
                {
                    combiner->dataRowMemSize = (long *)palloc(sizeof(long));
                    combiner->dataRowMemSize[0] = 0;
                }

                /* exceed buffer size, store into tuplestore */
                if (combiner->dataRowMemSize[0] >= DATA_ROW_BUFFER_SIZE(1))
                {                
                    /* init datarow buffer */
                    if (!combiner->dataRowBuffer)
                    {
                        combiner->dataRowBuffer = (Tuplestorestate **)palloc0(sizeof(Tuplestorestate *));
                    }

                    if (!combiner->dataRowBuffer[0])
                    {
                        combiner->dataRowBuffer[0] = tuplestore_begin_datarow(false, work_mem / NumDataNodes, NULL);

                        if (enable_statistic)
                        {
                            elog(LOG, "PreFetchConnection:dataRowMemSize %ld exceed max rowBufferSize %d, need to store"
                                  " datarow in tuplestore.", combiner->dataRowMemSize[0],
                                  DATA_ROW_BUFFER_SIZE(1));
                        }
                    }

                    /* data row count in tuplestore */
                    if (!combiner->nDataRows)
                    {
                        combiner->nDataRows = (int *)palloc(sizeof(int));
                        combiner->nDataRows[0] = 0;
                    }

                    if (!combiner->tmpslot)
                    {
                        TupleDesc desc = CreateTemplateTupleDesc(1, false);
                        combiner->tmpslot = MakeSingleTupleTableSlot(desc);
                    }

                    /* put into tuplestore */
                    combiner->tmpslot->tts_datarow = combiner->currentRow;

                    tuplestore_puttupleslot(combiner->dataRowBuffer[0], combiner->tmpslot);
                    
                    combiner->nDataRows[0]++;

                    pfree(combiner->currentRow);

                    combiner->currentRow = NULL;
                }
                else
                {
                    combiner->dataRowMemSize[0] += msglen;
                    
                    combiner->rowBuffer = lappend(combiner->rowBuffer,
                                          combiner->currentRow);
                    
                    combiner->currentRow = NULL;
                }
            }
        }

        res = handle_response(conn, combiner);
        /*
         * If response message is a DataRow it will be handled on the next
         * iteration.
         * PortalSuspended will cause connection state change and break the loop
         * The same is for CommandComplete, but we need additional handling -
         * remove connection from the list of active connections.
         * We may need to add handling error response
         */

        /* Most often result check first */
        if (res == RESPONSE_DATAROW)
        {
            /*
             * The row is in the combiner->currentRow, on next iteration it will
             * be moved to the buffer
             */
            continue;
        }

        /* incomplete message, read more */
        if (res == RESPONSE_EOF)
        {
            /* Here we will keep waiting without timeout. */
            state = conn->state;
            ret = pgxc_node_receive(1, &conn, &timeout);            
            if (DNStatus_ERR == ret)
            {                
                PGXCNodeSetConnectionState(conn, DN_CONNECTION_STATE_ERROR_FATAL);
                add_error_message(conn, "Failed to fetch from data node");
                MemoryContextSwitchTo(oldcontext);
                return false;
            }
            else if (DNStatus_EXPIRED == ret)
            {
                /* Restore the connection state. */
                conn->state = state;
                MemoryContextSwitchTo(oldcontext);
                return false;
            }
        }

        /*
         * End of result set is reached, so either set the pointer to the
         * connection to NULL (combiner with sort) or remove it from the list
         * (combiner without sort)
         */
        else if (res == RESPONSE_COMPLETE)
        {
            if (combiner->extended_query)
            {
                /*
                 * If combiner is doing merge sort we should set reference to the
                 * current connection to NULL in the array, indicating the end
                 * of the tape is reached. FetchTuple will try to access the buffer
                 * first anyway.
                 * Since we remove that reference we can not determine what node
                 * number was this connection, but we need this info to find proper
                 * tuple in the buffer if we are doing merge sort. So store node
                 * number in special array.
                 * NB: We can not test if combiner->tuplesortstate is set here:
                 * connection may require buffering inside tuplesort_begin_merge
                 * - while pre-read rows from the tapes, one of the tapes may be
                 * the local connection with RemoteSubplan in the tree. The
                 * combiner->tuplesortstate is set only after tuplesort_begin_merge
                 * returns.
                 */
                if (combiner->merge_sort)
                {
                    combiner->connections[node_index] = NULL;
                    if (combiner->tapenodes == NULL)
                    {
                        combiner->tapenodes = (Oid *)palloc0(combiner->conn_count * sizeof(Oid));
                    }
                    combiner->tapenodes[node_index] = conn->nodeoid;

                    if (enable_statistic)
                    {
                        if (combiner->dataRowBuffer && combiner->dataRowBuffer[node_index])
                        {
                            elog(LOG, "connection %d put %d datarows into tuplestore.", node_index, combiner->nDataRows[node_index]);
                        }
                    }
                }            
                else if (combiner->extended_query)
                {    
                    
#ifdef _PG_REGRESS_            
                    ereport(LOG,
                                (errcode(ERRCODE_INTERNAL_ERROR),
                                 errmsg("CommandComplete remove extend query connection %s backend_pid:%d", conn->nodename, conn->backend_pid)));                    
#endif
                    /* Remove connection with node_index, move last in-place. Here the node_index > current_conn*/
                    if (node_index < --combiner->conn_count)
                    {
                        combiner->connections[node_index] = combiner->connections[combiner->conn_count];
                        combiner->current_conn_rows_consumed = 0;
                        bComplete = true;

                        if (combiner->current_conn >= combiner->conn_count)
                            combiner->current_conn = node_index;
                    }
                }

                /*
                 * If combiner runs Simple Query Protocol we need to read in
                 * ReadyForQuery. In case of Extended Query Protocol it is not
                 * sent and we should quit.
                 */
                break;
            }        
            else if (conn->state == DN_CONNECTION_STATE_ERROR_FATAL)
            {
                ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Unexpected FATAL ERROR on Connection to Datanode %s pid %d",
                             conn->nodename, conn->backend_pid)));
            }
        }
        else if (res == RESPONSE_READY)
        {
#ifdef _PG_REGRESS_            
            ereport(LOG,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("ReadyForQuery remove connection %s backend_pid:%d", conn->nodename, conn->backend_pid)));                    
#endif

            /* If we are doing merge sort clean current connection and return
             * NULL, otherwise remove current connection, move last in-place,
             * adjust current_conn and continue if it is not last connection */
            if (combiner->merge_sort)
            {
                combiner->connections[node_index] = NULL;                
                bComplete = true;
                break;
            }
            
            if (node_index < --combiner->conn_count)
            {
                combiner->connections[node_index] = combiner->connections[combiner->conn_count];
                combiner->current_conn_rows_consumed = 0;
                bComplete = true;

                if (combiner->current_conn >= combiner->conn_count)
                    combiner->current_conn = node_index;
            }
            
            bComplete = true;
            break;
        }
        else if (res == RESPONSE_ERROR)
        {
            if (combiner->extended_query)
            {
                /*
                 * Need to sync connection to enable receiving commands
                 * by the datanode
                 */
                if (pgxc_node_send_sync(conn) != 0)
                {
                    ereport(ERROR,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("Failed to sync msg to node %s backend_pid:%d", conn->nodename, conn->backend_pid)));                    

                    
                }
#ifdef _PG_REGRESS_
                ereport(LOG,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("Succeed to sync msg to node %s backend_pid:%d", conn->nodename, conn->backend_pid)));                    
#endif
            }
        }
        else if (res == RESPONSE_SUSPENDED || res == RESPONSE_READY)
        {
            /* Now it is OK to quit */
            break;
        }
        else if (res == RESPONSE_TUPDESC)
        {
            ExecSetSlotDescriptor(combiner->ss.ps.ps_ResultTupleSlot,
                                  combiner->tuple_desc);
            /* Now slot is responsible for freeng the descriptor */
            combiner->tuple_desc = NULL;
        }
    }
    Assert(conn->state != DN_CONNECTION_STATE_QUERY);
    MemoryContextSwitchTo(oldcontext);

    if (combiner->errorMessage)
    {
        pgxc_node_report_error(combiner);
    }
    return bComplete;
}

/*
 * copy the datarow from combiner to the given slot, in the slot's memory
 * context
 */
static void
CopyDataRowTupleToSlot(ResponseCombiner *combiner, TupleTableSlot *slot)
{
    RemoteDataRow     datarow;
    MemoryContext    oldcontext;
    oldcontext = MemoryContextSwitchTo(slot->tts_mcxt);
    datarow = (RemoteDataRow) palloc(sizeof(RemoteDataRowData) + combiner->currentRow->msglen);
    datarow->msgnode = combiner->currentRow->msgnode;
    datarow->msglen = combiner->currentRow->msglen;
    memcpy(datarow->msg, combiner->currentRow->msg, datarow->msglen);
    ExecStoreDataRowTuple(datarow, slot, true);
    pfree(combiner->currentRow);
    combiner->currentRow = NULL;
    MemoryContextSwitchTo(oldcontext);
}


/*
 * FetchTuple
 *
        Get next tuple from one of the datanode connections.
 * The connections should be in combiner->connections, if "local" dummy
 * connection presents it should be the last active connection in the array.
 *      If combiner is set up to perform merge sort function returns tuple from
 * connection defined by combiner->current_conn, or NULL slot if no more tuple
 * are available from the connection. Otherwise it returns tuple from any
 * connection or NULL slot if no more available connections.
 *         Function looks into combiner->rowBuffer before accessing connection
 * and return a tuple from there if found.
 *         Function may wait while more data arrive from the data nodes. If there
 * is a locally executed subplan function advance it and buffer resulting rows
 * instead of waiting.
 */
TupleTableSlot *
FetchTuple(ResponseCombiner *combiner)
{// #lizard forgives
    PGXCNodeHandle *conn;
    TupleTableSlot *slot;
    Oid             nodeOid = -1;
#ifdef __TBASE__
    TimestampTz begin = 0;
    TimestampTz end   = 0;
#endif

    /*
     * Case if we run local subplan.
     * We do not have remote connections, so just get local tuple and return it
     */
	if (outerPlanState(combiner)
#ifdef __TBASE__
	    /* 
		 * if dn_instrument is not null, means this node is initialized for recv
		 * instrument from remote, not execute it locally too.
		 */
	    && ((outerPlanState(combiner))->dn_instrument == NULL)
#endif
		)
    {
        RemoteSubplanState *planstate = (RemoteSubplanState *) combiner;
        RemoteSubplan *plan = (RemoteSubplan *) combiner->ss.ps.plan;
        /* Advance subplan in a loop until we have something to return */
        for (;;)
        {
            Datum     value = (Datum) 0;
            bool     isnull = false;
            int     numnodes;
            int        i;

            slot = ExecProcNode(outerPlanState(combiner));
            /* If locator is not defined deliver all the results */
            if (planstate->locator == NULL)
                return slot;

            /*
             * If NULL tuple is returned we done with the subplan, finish it up and
             * return NULL
             */
            if (TupIsNull(slot))
                return NULL;

            /* Get partitioning value if defined */
            if (plan->distributionKey != InvalidAttrNumber)
                value = slot_getattr(slot, plan->distributionKey, &isnull);

            /* Determine target nodes */
#ifdef __COLD_HOT__
            numnodes = GET_NODES(planstate->locator, value, isnull, 0, true, NULL);
#else
            numnodes = GET_NODES(planstate->locator, value, isnull, NULL);
#endif
            for (i = 0; i < numnodes; i++)
            {
                /* Deliver the node */
                if (planstate->dest_nodes[i] == PGXCNodeId-1)
                    return slot;
            }
        }
    }

#ifdef __TBASE__
    if (enable_statistic)
    {
        begin = GetCurrentTimestamp();
    }
#endif
    /*
     * Get current connection
     */
    if (combiner->conn_count > combiner->current_conn)
        conn = combiner->connections[combiner->current_conn];
    else
        conn = NULL;

    /*
     * If doing merge sort determine the node number.
     * It may be needed to get buffered row.
     */
    if (combiner->merge_sort)
    {
        Assert(conn || combiner->tapenodes);
        nodeOid = conn ? conn->nodeoid :
                         combiner->tapenodes[combiner->current_conn];
        Assert(OidIsValid(nodeOid));
    }

READ_ROWBUFFER:
    /*
     * First look into the row buffer.
     * When we are performing merge sort we need to get from the buffer record
     * from the connection marked as "current". Otherwise get first.
     */
    if (list_length(combiner->rowBuffer) > 0)
    {
        RemoteDataRow dataRow;

        Assert(combiner->currentRow == NULL);

        if (combiner->merge_sort)
        {
            ListCell *lc;
            ListCell *prev;

            elog(DEBUG1, "Getting buffered tuple from node %x", nodeOid);

            prev = combiner->tapemarks[combiner->current_conn];
            if (prev)
            {
                /*
                 * Start looking through the list from the bookmark.
                 * Probably the first cell we check contains row from the needed
                 * node. Otherwise continue scanning until we encounter one,
                 * advancing prev pointer as well.
                 */
                while((lc = lnext(prev)) != NULL)
                {
                    dataRow = (RemoteDataRow) lfirst(lc);
                    if (dataRow->msgnode == nodeOid)
                    {
                        combiner->currentRow = dataRow;
                        break;
                    }
                    prev = lc;
                }
            }
            else
            {
                /*
                 * Either needed row is the first in the buffer or no such row
                 */
                lc = list_head(combiner->rowBuffer);
                dataRow = (RemoteDataRow) lfirst(lc);
                if (dataRow->msgnode == nodeOid)
                    combiner->currentRow = dataRow;
                else
                    lc = NULL;
            }
            
            if (lc)
            {
                /*
                 * Delete cell from the buffer. Before we delete we must check
                 * the bookmarks, if the cell is a bookmark for any tape.
                 * If it is the case we are deleting last row of the current
                 * block from the current tape. That tape should have bookmark
                 * like current, and current bookmark will be advanced when we
                 * read the tape once again.
                 */
                int i;
                for (i = 0; i < combiner->conn_count; i++)
                {
                    if (combiner->tapemarks[i] == lc)
                        combiner->tapemarks[i] = prev;
                }
                elog(DEBUG1, "Found buffered tuple from node %x", nodeOid);
                combiner->rowBuffer = list_delete_cell(combiner->rowBuffer,
                                                       lc, prev);
            }
            elog(DEBUG1, "Update tapemark");
            combiner->tapemarks[combiner->current_conn] = prev;
        }
        else
        {
            dataRow = (RemoteDataRow) linitial(combiner->rowBuffer);
            combiner->currentRow = dataRow;
            combiner->rowBuffer = list_delete_first(combiner->rowBuffer);
        }
    }

#ifdef __TBASE__
    /* fetch datarow from prerowbuffers */
    if (!combiner->currentRow)
    {
        if (combiner->merge_sort)
        {
            RemoteDataRow dataRow;
            int node_index = combiner->current_conn;

            if (combiner->prerowBuffers &&
                list_length(combiner->prerowBuffers[node_index]) > 0)
            {
                dataRow = (RemoteDataRow) linitial(combiner->prerowBuffers[node_index]);
                combiner->currentRow = dataRow;
                combiner->prerowBuffers[node_index] = list_delete_first(combiner->prerowBuffers[node_index]);
            }
        }
    }

    if (!combiner->currentRow)
    {
        /* fetch data in tuplestore */
        if (combiner->merge_sort)
        {
            int node_index = combiner->current_conn;
            
            if (combiner->dataRowBuffer && combiner->dataRowBuffer[node_index])
            {
                if (tuplestore_gettupleslot(combiner->dataRowBuffer[node_index], 
                                           true, true, combiner->tmpslot))
                {
                    combiner->tmpslot->tts_shouldFreeRow = false;
                    combiner->currentRow = combiner->tmpslot->tts_datarow;
                    combiner->nDataRows[node_index]--;
                }
                else
                {
                    /* sanity check */
                    if (combiner->nDataRows[node_index] != 0)
                    {
                        elog(ERROR, "connection %d has %d datarows left in tuplestore.", 
                                     node_index, combiner->nDataRows[node_index]);
                    }

                    if (enable_statistic)
                    {
                        elog(LOG, "fetch all datarows from %d tuplestore.", node_index);
                    }

                    /* 
                      * datarows fetched from tuplestore in memory will be freed by caller, 
                      * we do not need to free them in tuplestore_end, tuplestore_set_tupdeleted
                      * avoid to free memtuples in tuplestore_end.
                      */
                    //tuplestore_set_tupdeleted(combiner->dataRowBuffer[node_index]);
                    tuplestore_end(combiner->dataRowBuffer[node_index]);
                    combiner->dataRowBuffer[node_index] = NULL;
                    combiner->dataRowMemSize[node_index] = 0;
                }
            }
        }
        else
        {
            if (combiner->dataRowBuffer && combiner->dataRowBuffer[0])
            {
                if (tuplestore_gettupleslot(combiner->dataRowBuffer[0], 
                                           true, true, combiner->tmpslot))
                {
                    combiner->tmpslot->tts_shouldFreeRow = false;
                    combiner->currentRow = combiner->tmpslot->tts_datarow;
                    combiner->nDataRows[0]--;
                }
                else
                {
                    /* sanity check */
                    if (combiner->nDataRows[0] != 0)
                    {
                        elog(ERROR, "%d datarows left in tuplestore.", combiner->nDataRows[0]);
                    }

                    if (enable_statistic)
                    {
                        elog(LOG, "fetch all datarows from tuplestore.");
                    }

                    /* 
                      * datarows fetched from tuplestore in memory will be freed by caller, 
                      * we do not need to free them in tuplestore_end, tuplestore_set_tupdeleted
                      * avoid to free memtuples in tuplestore_end.
                      */
                    //tuplestore_set_tupdeleted(combiner->dataRowBuffer[0]);
                    tuplestore_end(combiner->dataRowBuffer[0]);
                    combiner->dataRowBuffer[0] = NULL;
                    combiner->dataRowMemSize[0] = 0;
                }
            }
        }
    }
#endif

    /* If we have node message in the currentRow slot, and it is from a proper
     * node, consume it.  */
    if (combiner->currentRow)
    {
        Assert(!combiner->merge_sort ||
               combiner->currentRow->msgnode == nodeOid);
        slot = combiner->ss.ps.ps_ResultTupleSlot;
        CopyDataRowTupleToSlot(combiner, slot);

#ifdef __TBASE__
        if (enable_statistic)
        {
            end = GetCurrentTimestamp();

            /* 
              * usually, we need more time to get first tuple, so
              * do not include that time.
              */
            if (combiner->recv_total_time == -1)
            {
                combiner->recv_tuples++;
                combiner->recv_total_time = 0;
            }
            else
            {
                combiner->recv_tuples++;
                combiner->recv_total_time += (end - begin);
            }
        }
#endif

        return slot;
    }

    while (conn)
    {
        int res;

        /* Going to use a connection, buffer it if needed */
        CHECK_OWNERSHIP(conn, combiner);

        /*
         * If current connection is idle it means portal on the data node is
         * suspended. Request more and try to get it
         */
        if (combiner->extended_query &&
                conn->state == DN_CONNECTION_STATE_IDLE)
        {
            /*
             * We do not allow to suspend if querying primary node, so that
             * only may mean the current node is secondary and subplan was not
             * executed there yet. Return and go on with second phase.
             */
            if (combiner->probing_primary)
            {
                return NULL;
            }

            if (pgxc_node_send_execute(conn, combiner->cursor, PGXLRemoteFetchSize) != 0)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("Failed to send execute cursor '%s' to node %u", combiner->cursor, conn->nodeoid)));
            }

            if (pgxc_node_send_flush(conn) != 0)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("Failed flush cursor '%s' node %u", combiner->cursor, conn->nodeoid)));
            }

            if (pgxc_node_receive(1, &conn, NULL))
            {
                ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("Failed receive data from node %u cursor '%s'", conn->nodeoid, combiner->cursor)));
            }
        }

        /* read messages */
        res = handle_response(conn, combiner);
        if (res == RESPONSE_DATAROW)
        {
            slot = combiner->ss.ps.ps_ResultTupleSlot;
            CopyDataRowTupleToSlot(combiner, slot);
            combiner->current_conn_rows_consumed++;

            /*
             * If we are running simple query protocol, yield the connection
             * after we process PGXLRemoteFetchSize rows from the connection.
             * This should allow us to consume rows quickly from other
             * connections, while this node gets chance to generate more rows
             * which would then be processed in the next iteration.
             */
            if (!combiner->extended_query &&
                combiner->current_conn_rows_consumed >= PGXLRemoteFetchSize)
            {
                if (++combiner->current_conn >= combiner->conn_count)
                {
                    combiner->current_conn = 0;
                }
                combiner->current_conn_rows_consumed = 0;
            }

#ifdef __TBASE__
            if (enable_statistic)
            {
                end = GetCurrentTimestamp();

                /* 
                  * usually, we need more time to get first tuple, so
                  * do not include that time.
                  */
                if (combiner->recv_total_time == -1)
                {
                    combiner->recv_tuples++;
                    combiner->recv_total_time = 0;
                }
                else
                {
                    combiner->recv_tuples++;
                    combiner->recv_total_time += (end - begin);
                }

            }
#endif

            return slot;
        }
        else if (res == RESPONSE_EOF)
        {
#ifdef __TBASE__
            /* 
             * We encountered incomplete message, try to read more.
             * Here if we read timeout, then we move to other connections to read, because we
             * easily got deadlock if a specific cursor run as producer on two nodes. If we can
             * consume data from all all connections, we can break the deadlock loop.
             */            
            bool   bComplete          = false;
            DNConnectionState state   = DN_CONNECTION_STATE_IDLE;
            int    i                  = 0;
            int    ret                   = 0;
            PGXCNodeHandle *save_conn = NULL;
            struct timeval timeout;
            timeout.tv_sec            = 0;
            timeout.tv_usec           = 1000;    

            save_conn = conn;
            while (1)
            {
                conn  = save_conn;
                state = conn->state; /* Save the connection state. */
                ret   = pgxc_node_receive(1, &conn, &timeout);
                if (DNStatus_OK == ret)
                {
                    /* We got data, handle it. */
                    break;
                }
                else if (DNStatus_ERR == ret)
                {
                    ereport(ERROR,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("Failed to receive more data from data node %u", conn->nodeoid)));
                }
                else
                {
                    /* Restore the saved state of connection. */
                    conn->state = state;
                }

                /* Try to read data from other connections. */
                for (i = 0; i < combiner->conn_count; i ++)
                {
                    conn  = combiner->connections[i];
                    if (save_conn != conn && conn != NULL)
                    {
                        /* Save the connection state. */
                        state = conn->state;
                        if (state == DN_CONNECTION_STATE_QUERY)
                        {
                            ret = pgxc_node_receive(1, &conn, &timeout);
                            if (DNStatus_OK == ret)
                            {
                                /* We got data, prefetch it. */
                                bComplete = PreFetchConnection(conn, i);
                                if (bComplete)
                                {
                                    /* Receive Complete on one connection, we need retry to read from current_conn. */
                                    break;
                                }
                                else
                                {
                                    /* Maybe Suspend or Expired, just move to next connection and read. */
                                    continue;
                                }
                            }
                            else if (DNStatus_EXPIRED == ret)
                            {
                                /* Restore the saved state of connection. */
                                conn->state = state;
                                continue;
                            }
                            else
                            {
                                ereport(ERROR,
                                        (errcode(ERRCODE_INTERNAL_ERROR),
                                         errmsg("Failed to receive more data from data node %u", conn->nodeoid)));
                            }
                        }
                    }
                }    
            }
            continue;
#else
            /* incomplete message, read more */
            if (pgxc_node_receive(1, &conn, NULL))
                ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("Failed to receive more data from data node %u", conn->nodeoid)));
            continue;
#endif
        }
        else if (res == RESPONSE_SUSPENDED)
        {
            /*
             * If we are doing merge sort or probing primary node we should
             * remain on the same node, so query next portion immediately.
             * Otherwise leave node suspended and fetch lazily.
             */
            if (combiner->merge_sort || combiner->probing_primary)
            {
                if (pgxc_node_send_execute(conn, combiner->cursor, PGXLRemoteFetchSize) != 0)
                    ereport(ERROR,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("Failed to send execute cursor '%s' to node %u", combiner->cursor, conn->nodeoid)));
                if (pgxc_node_send_flush(conn) != 0)
                    ereport(ERROR,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("Failed flush cursor '%s' node %u", combiner->cursor, conn->nodeoid)));
                if (pgxc_node_receive(1, &conn, NULL))
                    ereport(ERROR,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("Failed receive node from node %u cursor '%s'", conn->nodeoid, combiner->cursor)));
                continue;
            }

            /*
             * Tell the node to fetch data in background, next loop when we 
             * pgxc_node_receive, data is already there, so we can run faster
             * */
            if (pgxc_node_send_execute(conn, combiner->cursor, PGXLRemoteFetchSize) != 0)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("Failed to send execute cursor '%s' to node %u", combiner->cursor, conn->nodeoid)));
            }

            if (pgxc_node_send_flush(conn) != 0)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("Failed flush cursor '%s' node %u", combiner->cursor, conn->nodeoid)));
            }

            if (++combiner->current_conn >= combiner->conn_count)
            {
                combiner->current_conn = 0;
            }
            combiner->current_conn_rows_consumed = 0;
            conn = combiner->connections[combiner->current_conn];
        }
        else if (res == RESPONSE_COMPLETE)
        {        
            if (conn->state == DN_CONNECTION_STATE_ERROR_FATAL)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                                errmsg("Unexpected FATAL ERROR on Connection to Datanode %s pid %d",
                                       conn->nodename, conn->backend_pid)));
            }

            /*
             * In case of Simple Query Protocol we should receive ReadyForQuery
             * before removing connection from the list. In case of Extended
             * Query Protocol we may remove connection right away.
             */
            if (combiner->extended_query)
            {
                /* If we are doing merge sort clean current connection and return
                 * NULL, otherwise remove current connection, move last in-place,
                 * adjust current_conn and continue if it is not last connection */
                if (combiner->merge_sort)
                {
                    combiner->connections[combiner->current_conn] = NULL;

                    /* data left in row_buffer, read it */
                    if ((combiner->prerowBuffers && list_length(combiner->prerowBuffers[combiner->current_conn]) > 0) || 
                        (combiner->dataRowBuffer && combiner->dataRowBuffer[combiner->current_conn]))
                    {    
                            elog(DEBUG1, "FetchTuple:data left in rowbuffer while merge_sort.");
                            conn = NULL;
                            goto READ_ROWBUFFER;
                    } 

#ifdef __TBASE__
                    if (enable_statistic)
                    {
                        bool done = false;
                        int i = 0;
                
                        if (combiner->merge_sort)
                        {
                            for (i = 0; i < combiner->conn_count; i++)
                            {
                                if (combiner->connections[i] != NULL)
                                    break;
                            }
                
                            if (i == combiner->conn_count)
                                done = true;
                        }
                        else
                        {
                            if (combiner->conn_count == 0)
                                done = true;
                        }
                
                        if (done)
                        {
                            elog(LOG, "Number of datarows fetched in FetchTuple: %lu datarows.", combiner->recv_datarows);
                            combiner->recv_datarows = 0;
                        }
                    }
#endif

                    return NULL;
                }
                
                REMOVE_CURR_CONN(combiner);
                if (combiner->conn_count > 0)
                {
                    conn = combiner->connections[combiner->current_conn];
                    combiner->current_conn_rows_consumed = 0;
                }
                else
                {
                    /* data left in row_buffer, read it */
                    if (list_length(combiner->rowBuffer) > 0 ||
                        (combiner->dataRowBuffer && combiner->dataRowBuffer[0]))
                    {
                        elog(DEBUG1, "FetchTuple:data left in rowbuffer in extended_query.");
                        goto READ_ROWBUFFER;
                    }

#ifdef __TBASE__
                    if (enable_statistic)
                    {
                        bool done = false;
                        int i = 0;
                
                        if (combiner->merge_sort)
                        {
                            for (i = 0; i < combiner->conn_count; i++)
                            {
                                if (combiner->connections[i] != NULL)
                                    break;
                            }
                
                            if (i == combiner->conn_count)
                                done = true;
                        }
                        else
                        {
                            if (combiner->conn_count == 0)
                                done = true;
                        }
                
                        if (done)
                        {
                            elog(LOG, "Number of datarows fetched in FetchTuple: %lu datarows.", combiner->recv_datarows);
                            combiner->recv_datarows = 0;
                        }
                    }
#endif
                                        
                    return NULL;
                }
            }
        }
        else if (res == RESPONSE_ERROR)
        {
            /*
             * If doing Extended Query Protocol we need to sync connection,
             * otherwise subsequent commands will be ignored.
             */
            if (combiner->extended_query)
            {
                if (pgxc_node_send_sync(conn) != 0)
                {
                    ereport(ERROR,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("Failed to sync msg to node %s backend_pid:%d", conn->nodename, conn->backend_pid)));
                }
            
#ifdef _PG_REGRESS_
                    ereport(LOG,
                                (errcode(ERRCODE_INTERNAL_ERROR),
                                 errmsg("Succeed to sync msg to node %s backend_pid:%d", conn->nodename, conn->backend_pid)));                    
#endif
            }
            /*
             * Do not wait for response from primary, it needs to wait
             * for other nodes to respond. Instead go ahead and send query to
             * other nodes. It will fail there, but we can continue with
             * normal cleanup.
             */
            if (combiner->probing_primary)
            {
                REMOVE_CURR_CONN(combiner);
                return NULL;
            }
        }
        else if (res == RESPONSE_READY)
        {
            /* If we are doing merge sort clean current connection and return
             * NULL, otherwise remove current connection, move last in-place,
             * adjust current_conn and continue if it is not last connection */
            if (combiner->merge_sort)
            {
                combiner->connections[combiner->current_conn] = NULL;

                /* data left in row_buffer, read it */
                if ((combiner->prerowBuffers && list_length(combiner->prerowBuffers[combiner->current_conn]) > 0) || 
                    (combiner->dataRowBuffer && combiner->dataRowBuffer[combiner->current_conn]))
                {    
                        elog(DEBUG1, "FetchTuple:data left in rowbuffer while merge_sort.");
                        conn = NULL;
                        goto READ_ROWBUFFER;
                } 

                
#ifdef __TBASE__
                if (enable_statistic)
                {
                    bool done = false;
                    int i = 0;
            
                    if (combiner->merge_sort)
                    {
                        for (i = 0; i < combiner->conn_count; i++)
                        {
                            if (combiner->connections[i] != NULL)
                                break;
                        }
            
                        if (i == combiner->conn_count)
                            done = true;
                    }
                    else
                    {
                        if (combiner->conn_count == 0)
                            done = true;
                    }
            
                    if (done)
                    {
                        elog(LOG, "Number of datarows fetched in FetchTuple: %lu datarows.", combiner->recv_datarows);
                        combiner->recv_datarows = 0;
                    }
                }
#endif
                return NULL;
            }

            REMOVE_CURR_CONN(combiner);
            if (combiner->conn_count > 0)
                conn = combiner->connections[combiner->current_conn];
            else
            {
                /* data left in row_buffer, read it */
                if (list_length(combiner->rowBuffer) > 0 ||
                    (combiner->dataRowBuffer && combiner->dataRowBuffer[0]))
                {
                    elog(DEBUG1, "FetchTuple:data left in rowbuffer in simple_query.");
                    goto READ_ROWBUFFER;
                }
#ifdef __TBASE__
                if (enable_statistic)
                {
                    bool done = false;
                    int i = 0;
            
                    if (combiner->merge_sort)
                    {
                        for (i = 0; i < combiner->conn_count; i++)
                        {
                            if (combiner->connections[i] != NULL)
                                break;
                        }
            
                        if (i == combiner->conn_count)
                            done = true;
                    }
                    else
                    {
                        if (combiner->conn_count == 0)
                            done = true;
                    }
            
                    if (done)
                    {
                        elog(LOG, "Number of datarows fetched in FetchTuple: %lu datarows.", combiner->recv_datarows);
                        combiner->recv_datarows = 0;
                    }
                }
#endif
                return NULL;
            }
        }
        else if (res == RESPONSE_TUPDESC)
        {
            ExecSetSlotDescriptor(combiner->ss.ps.ps_ResultTupleSlot,
                                  combiner->tuple_desc);
            /* Now slot is responsible for freeng the descriptor */
            combiner->tuple_desc = NULL;
        }
        else if (res == RESPONSE_ASSIGN_GXID)
        {
            /* Do nothing. It must have been handled in handle_response() */
        }
        else if (res == RESPONSE_WAITXIDS)
        {
            /* Do nothing. It must have been handled in handle_response() */
        }
        else
        {
            // Can not get here?
            Assert(false);
        }
    }

#ifdef __TBASE__
    if (enable_statistic)
    {
        bool done = false;
        int i = 0;

        if (combiner->merge_sort)
        {
            for (i = 0; i < combiner->conn_count; i++)
            {
                if (combiner->connections[i] != NULL)
                    break;
            }

            if (i == combiner->conn_count)
                done = true;
        }
        else
        {
            if (combiner->conn_count == 0)
                done = true;
        }

        if (done)
        {
            elog(LOG, "Number of datarows fetched in FetchTuple: %lu datarows.", combiner->recv_datarows);
            combiner->recv_datarows = 0;
        }
    }
#endif

    return NULL;
}

#ifdef __TBASE__
static int
pgxc_node_receive_copy_begin(const int conn_count, PGXCNodeHandle ** connections,
                         struct timeval * timeout, ResponseCombiner *combiner)
{// #lizard forgives
    int         ret   = 0;
    int         count = conn_count;
    PGXCNodeHandle *to_receive[conn_count];

    /* make a copy of the pointers to the connections */
    memcpy(to_receive, connections, conn_count * sizeof(PGXCNodeHandle *));

    /*
     * Read results.
     * Note we try and read from Datanode connections even if there is an error on one,
     * so as to avoid reading incorrect results on the next statement.
     * Other safegaurds exist to avoid this, however.
     */
    while (count > 0)
    {
        int i = 0;
        ret = pgxc_node_receive(count, to_receive, timeout);
        if (DNStatus_ERR == ret)
        {
            elog(LOG, "pgxc_node_receive_copy_begin pgxc_node_receive data from node number:%d failed", count);
            return EOF;
        }
        
        while (i < count)
        {
            int result =  handle_response(to_receive[i], combiner);
            elog(DEBUG5, "Received response %d on connection to node %s",
                    result, to_receive[i]->nodename);
            switch (result)
            {
                case RESPONSE_EOF: /* have something to read, keep receiving */
                    i++;
                    break;
                case RESPONSE_COMPLETE:
                    if (to_receive[i]->state != DN_CONNECTION_STATE_ERROR_FATAL)
                    {
                        /* Continue read until ReadyForQuery */
                        break;
                    }
                    else
                    {
                        /* error occurred, set buffer logically empty */
                        to_receive[i]->inStart = 0;
                        to_receive[i]->inCursor = 0;
                        to_receive[i]->inEnd = 0;
                    }
                    /* fallthru */
                case RESPONSE_READY:
                    /* fallthru */
                case RESPONSE_COPY:
                    /* Handling is done, do not track this connection */
                    count--;
                    /* Move last connection in place */
                    if (i < count)
                        to_receive[i] = to_receive[count];
                    break;
                case RESPONSE_ERROR:
                    /* no handling needed, just wait for ReadyForQuery */
                    break;

                case RESPONSE_WAITXIDS:
                case RESPONSE_ASSIGN_GXID:
                case RESPONSE_TUPDESC:
                    break;

                case RESPONSE_DATAROW:
                    combiner->currentRow = NULL;
                    break;

                default:
                    /* Inconsistent responses */
                    add_error_message(to_receive[i], "Unexpected response from the Datanodes");
                    elog(DEBUG1, "Unexpected response from the Datanodes, result = %d, request type %d", result, combiner->request_type);
                    /* Stop tracking and move last connection in place */
                    count--;
                    if (i < count)
                        to_receive[i] = to_receive[count];
            }
        }
    }

    return 0;
}

#endif

/*
 * Handle responses from the Datanode connections
 */
int
pgxc_node_receive_responses(const int conn_count, PGXCNodeHandle ** connections,
                         struct timeval * timeout, ResponseCombiner *combiner)
{// #lizard forgives
#ifdef __TWO_PHASE_TRANS__
    bool        has_errmsg = false;
    int         connection_index;
#endif
    int            ret   = 0;
    int            count = conn_count;
    PGXCNodeHandle *to_receive[conn_count];
    int         func_ret = 0;
    int         last_fatal_conn_count = 0;
#ifdef __TWO_PHASE_TRANS__
    int         receive_to_connections[conn_count];
#endif
    
    /* make a copy of the pointers to the connections */
    memcpy(to_receive, connections, conn_count * sizeof(PGXCNodeHandle *));

    /*
     * Read results.
     * Note we try and read from Datanode connections even if there is an error on one,
     * so as to avoid reading incorrect results on the next statement.
     * Other safegaurds exist to avoid this, however.
     */
    while (count > 0)
    {
        int i = 0;
        ret = pgxc_node_receive(count, to_receive, timeout);
        if (DNStatus_ERR == ret)
        {
            int conn_loop;
            int rece_idx = 0;
            int fatal_conn_inner;
#ifdef __TWO_PHASE_TRANS__
            has_errmsg = true;
#endif
            elog(LOG, "pgxc_node_receive_responses pgxc_node_receive data from node number:%d failed", count);
            /* mark has error */
            func_ret = -1;
            memset((char*)to_receive, 0, conn_count*sizeof(PGXCNodeHandle *));
            
            fatal_conn_inner = 0;
            for(conn_loop = 0; conn_loop < conn_count;conn_loop++)
            {
#ifdef __TWO_PHASE_TRANS__            
                receive_to_connections[conn_loop] = conn_loop;
#endif                
                if (connections[conn_loop]->state != DN_CONNECTION_STATE_ERROR_FATAL)
                {
                    to_receive[rece_idx] = connections[conn_loop];
#ifdef __TWO_PHASE_TRANS__
                    receive_to_connections[rece_idx] = conn_loop;
#endif
                    rece_idx++;
                }
                else
                {
                    fatal_conn_inner++;
                }
            }
            
            if (last_fatal_conn_count == fatal_conn_inner)
            {
#ifdef __TWO_PHASE_TRANS__
                /* 
                 *if exit abnormally reset response_operation for next call 
                 */
                g_twophase_state.response_operation = OTHER_OPERATIONS;
#endif
                return EOF;
            }
            last_fatal_conn_count = fatal_conn_inner;
            
            count = rece_idx;
        }         
        
        while (i < count)
        {
            int32 nbytes = 0;
			int result =  handle_response(to_receive[i], combiner);
#ifdef __TBASE__
#ifdef     _PG_REGRESS_            
            elog(LOG, "Received response %d on connection to node %s",
                    result, to_receive[i]->nodename);
#else
            elog(DEBUG5, "Received response %d on connection to node %s",
                    result, to_receive[i]->nodename);
#endif
#endif
            switch (result)
            {
                case RESPONSE_EOF: /* have something to read, keep receiving */
                    i++;
                    break;
                case RESPONSE_COMPLETE:
                    if (to_receive[i]->state != DN_CONNECTION_STATE_ERROR_FATAL)
                    {
                        /* Continue read until ReadyForQuery */
                        break;
                    }
                    else
                    {
                        /* error occurred, set buffer logically empty */
                        to_receive[i]->inStart = 0;
                        to_receive[i]->inCursor = 0;
                        to_receive[i]->inEnd = 0;
                    }
                    /* fallthru */
                case RESPONSE_READY:
                    /* fallthru */
#ifdef __TWO_PHASE_TRANS__
                    if (!has_errmsg)
                    {
                        connection_index = i;
                    }
                    else
                    {
                        connection_index = receive_to_connections[i];
                    }
                    UpdateLocalTwoPhaseState(result, to_receive[i], connection_index, combiner->errorMessage);
#endif
                case RESPONSE_COPY:
                    /* try to read every byte from peer. */
                    nbytes = pgxc_node_is_data_enqueued(to_receive[i]);
                    if (nbytes)
                    {
                        int32               ret    = 0;
                        DNConnectionState estate =  DN_CONNECTION_STATE_IDLE;
                        /* Have data in buffer, try to receive and retry. */
                        elog(DEBUG1, "Pending response %d bytes on connection to node %s, pid %d try to read again. ", nbytes, to_receive[i]->nodename, to_receive[i]->backend_pid);
                        estate                 = to_receive[i]->state;
                        to_receive[i]->state = DN_CONNECTION_STATE_QUERY;                        
                        ret = pgxc_node_receive(1, &to_receive[i], NULL);
                        if (ret)
                        {
                            switch (ret)
                            {
                                case DNStatus_ERR:                    
                                    {
                                        elog(LOG, "pgxc_node_receive Pending data %d bytes from node:%s pid:%d failed for ERROR:%s. ", nbytes, to_receive[i]->nodename, to_receive[i]->backend_pid, strerror(errno));                                        
                                        break;
                                    }
                                    
                                case DNStatus_EXPIRED:                            
                                    {
                                        elog(LOG, "pgxc_node_receive Pending data %d bytes from node:%s pid:%d failed for EXPIRED. ", nbytes, to_receive[i]->nodename, to_receive[i]->backend_pid);
                                        break;
                                    }    
                                default:
                                    {
                                        /* Can not be here.*/
                                        break;
                                    }
                            }
                        }
                        to_receive[i]->state = estate;
                        i++;
                        break;                        
                    }
                    
                    /* Handling is done, do not track this connection */
                    count--;
                    /* Move last connection in place */
                    if (i < count)
                        to_receive[i] = to_receive[count];
                    break;
                case RESPONSE_ERROR:
                    /* no handling needed, just wait for ReadyForQuery */
#ifdef __TWO_PHASE_TRANS__
                    if (!has_errmsg)
                    {
                        connection_index = i;
                    }
                    else
                    {
                        connection_index = receive_to_connections[i];
                    }
                    UpdateLocalTwoPhaseState(result, to_receive[i], connection_index, combiner->errorMessage);
#endif
                    break;

                case RESPONSE_WAITXIDS:
                case RESPONSE_ASSIGN_GXID:
                case RESPONSE_TUPDESC:
                    break;

                case RESPONSE_DATAROW:
                    combiner->currentRow = NULL;
                    break;

                default:
                    /* Inconsistent responses */
                    add_error_message(to_receive[i], "Unexpected response from the Datanodes");
                    elog(DEBUG1, "Unexpected response from the Datanodes, result = %d, request type %d", result, combiner->request_type);
                    /* Stop tracking and move last connection in place */
                    count--;
                    if (i < count)
                        to_receive[i] = to_receive[count];
            }
        }
    }

#ifdef __TWO_PHASE_TRANS__
    g_twophase_state.response_operation = OTHER_OPERATIONS;
#endif
    return func_ret;
}

/*
 * Read next message from the connection and update the combiner
 * and connection state accordingly
 * If we are in an error state we just consume the messages, and do not proxy
 * Long term, we should look into cancelling executing statements
 * and closing the connections.
 * It returns if states need to be handled
 * Return values:
 * RESPONSE_EOF - need to receive more data for the connection
 * RESPONSE_READY - got ReadyForQuery
 * RESPONSE_COMPLETE - done with the connection, but not yet ready for query.
 * Also this result is output in case of error
 * RESPONSE_SUSPENDED - got PortalSuspended
 * RESPONSE_TUPLEDESC - got tuple description
 * RESPONSE_DATAROW - got data row
 * RESPONSE_COPY - got copy response
 * RESPONSE_BARRIER_OK - barrier command completed successfully
 */
int
handle_response(PGXCNodeHandle *conn, ResponseCombiner *combiner)
{// #lizard forgives
    char       *msg;
    int            msg_len;
    char        msg_type;

    for (;;)
    {
        /*
         * If we are in the process of shutting down, we
         * may be rolling back, and the buffer may contain other messages.
         * We want to avoid a procarray exception
         * as well as an error stack overflow.
         */
        if (proc_exit_inprogress)
        {
            PGXCNodeSetConnectionState(conn, DN_CONNECTION_STATE_ERROR_FATAL);
        }

        /*
         * Don't read from from the connection if there is a fatal error.
         * We still return RESPONSE_COMPLETE, not RESPONSE_ERROR, since
         * Handling of RESPONSE_ERROR assumes sending SYNC message, but
         * State DN_CONNECTION_STATE_ERROR_FATAL indicates connection is
         * not usable.
         */
        if (conn->state == DN_CONNECTION_STATE_ERROR_FATAL)
        {
#ifdef     _PG_REGRESS_
            elog(LOG, "RESPONSE_COMPLETE_1 from node %s, remote pid %d", conn->nodename, conn->backend_pid);
#endif
            return RESPONSE_COMPLETE;
        }

        /* No data available, exit */
        if (!HAS_MESSAGE_BUFFERED(conn))
            return RESPONSE_EOF;

        Assert(conn->combiner == combiner || conn->combiner == NULL);

        /* TODO handle other possible responses */
        msg_type = get_message(conn, &msg_len, &msg);
        elog(DEBUG5, "handle_response - received message %c, node %s, "
                "current_state %d", msg_type, conn->nodename, conn->state);
        
#ifdef __TBASE__
        conn->last_command = msg_type;

        /*
         * Add some protection code when receiving a messy message,
         * close the connection, and throw error
         */
        if (msg_len < 0)
        {
            PGXCNodeSetConnectionState(conn,
                    DN_CONNECTION_STATE_ERROR_FATAL);
#ifdef __TBASE__
			elog(DEBUG5, "handle_response, fatal_conn=%p, fatal_conn->nodename=%s, fatal_conn->sock=%d, "
				"fatal_conn->read_only=%d, fatal_conn->transaction_status=%c, "
				"fatal_conn->sock_fatal_occurred=%d, conn->backend_pid=%d, fatal_conn->error=%s", 
				conn, conn->nodename, conn->sock, conn->read_only, conn->transaction_status,
				conn->sock_fatal_occurred, conn->backend_pid,  conn->error);
#endif
            closesocket(conn->sock);
            conn->sock = NO_SOCKET;
			conn->sock_fatal_occurred = true;

            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Received messy message from node:%s host:%s port:%d pid:%d, "
                            "inBuffer:%p inSize:%lu inStart:%lu inEnd:%lu inCursor:%lu msg_len:%d, "
                            "This probably means the remote node terminated abnormally "
                            "before or while processing the request. ",
                            conn->nodename, conn->nodehost, conn->nodeport, conn->backend_pid,
                            conn->inBuffer, conn->inSize, conn->inStart, conn->inEnd, conn->inCursor, msg_len)));
        }
#endif

        switch (msg_type)
        {
            case '\0':            /* Not enough data in the buffer */
                return RESPONSE_EOF;
                
            case 'c':            /* CopyToCommandComplete */
                HandleCopyOutComplete(combiner);
                break;
                
            case 'C':            /* CommandComplete */
                HandleCommandComplete(combiner, msg, msg_len, conn);
                conn->combiner = NULL;
                /* 
                 * In case of simple query protocol, wait for the ReadyForQuery
                 * before marking connection as Idle
                 */
                if (combiner->extended_query &&
                    conn->state == DN_CONNECTION_STATE_QUERY)                    
                {
                    PGXCNodeSetConnectionState(conn, DN_CONNECTION_STATE_IDLE);
#ifdef     _PG_REGRESS_
                    elog(LOG, "RESPONSE_COMPLETE_2 set node %s, remote pid %d DN_CONNECTION_STATE_IDLE", conn->nodename, conn->backend_pid);
#endif
                }
#ifdef     _PG_REGRESS_
                elog(LOG, "RESPONSE_COMPLETE_2 from node %s, remote pid %d", conn->nodename, conn->backend_pid);
#endif
                return RESPONSE_COMPLETE;
                
            case 'T':            /* RowDescription */
#ifdef DN_CONNECTION_DEBUG
                Assert(!conn->have_row_desc);
                conn->have_row_desc = true;
#endif
                if (HandleRowDescription(combiner, msg, msg_len))
                    return RESPONSE_TUPDESC;
                break;
                
            case 'D':            /* DataRow */
#ifdef DN_CONNECTION_DEBUG
                Assert(conn->have_row_desc);
#endif
                
#ifdef __TBASE__
                if (enable_statistic)
                {
                    conn->recv_datarows++;
                    combiner->recv_datarows++;
                }
#endif
                /* Do not return if data row has not been actually handled */
                if (HandleDataRow(combiner, msg, msg_len, conn->nodeoid))
                {
                    elog(DEBUG1, "HandleDataRow from node %s, remote pid %d", conn->nodename, conn->backend_pid);
                    return RESPONSE_DATAROW;
                }
                break;
                
            case 's':            /* PortalSuspended */
                /* No activity is expected on the connection until next query */
                PGXCNodeSetConnectionState(conn, DN_CONNECTION_STATE_IDLE);
                return RESPONSE_SUSPENDED;
                
            case '1': /* ParseComplete */
            case '2': /* BindComplete */
            case '3': /* CloseComplete */
            case 'n': /* NoData */
                /* simple notifications, continue reading */
                break;
#ifdef __SUBSCRIPTION__
            case '4': /* ApplyDone */
                PGXCNodeSetConnectionState(conn, DN_CONNECTION_STATE_IDLE);
                HandleApplyDone(combiner);
                return RESPONSE_READY;
                break;
#endif
            case 'G': /* CopyInResponse */
                PGXCNodeSetConnectionState(conn, DN_CONNECTION_STATE_COPY_IN);
                HandleCopyIn(combiner);
                /* Done, return to caller to let it know the data can be passed in */
                return RESPONSE_COPY;
                
            case 'H': /* CopyOutResponse */
                PGXCNodeSetConnectionState(conn, DN_CONNECTION_STATE_COPY_OUT);
                HandleCopyOut(combiner);
                return RESPONSE_COPY;
                
            case 'd': /* CopyOutDataRow */
                PGXCNodeSetConnectionState(conn, DN_CONNECTION_STATE_COPY_OUT);
                HandleCopyDataRow(combiner, msg, msg_len);
                break;
                
            case 'E':            /* ErrorResponse */
                HandleError(combiner, msg, msg_len, conn);
                add_error_message_from_combiner(conn, combiner);
                /*
                 * In case the remote node was running an extended query
                 * protocol and reported an error, it will keep ignoring all
                 * subsequent commands until it sees a SYNC message. So make
                 * sure that we send down SYNC even before sending a ROLLBACK
                 * command
                 */
             
                #ifdef __TBASE__
                combiner->errorNode   = conn->nodename;
                combiner->backend_pid = conn->backend_pid;
                #endif

                if (conn->in_extended_query)
                {
                    conn->needSync = true;
                }
#ifdef     _PG_REGRESS_
                elog(LOG, "HandleError from node %s, remote pid %d, errorMessage:%s", 
                        conn->nodename, conn->backend_pid, combiner->errorMessage);
#endif
                return RESPONSE_ERROR;
                
            case 'A':            /* NotificationResponse */
            case 'N':            /* NoticeResponse */
            case 'S':            /* SetCommandComplete */
                /*
                 * Ignore these to prevent multiple messages, one from each
                 * node. Coordinator will send one for DDL anyway
                 */
                break;
                
            case 'Z':            /* ReadyForQuery */
            {
                /*
                 * Return result depends on previous connection state.
                 * If it was PORTAL_SUSPENDED Coordinator want to send down
                 * another EXECUTE to fetch more rows, otherwise it is done
                 * with the connection
                 */
                conn->transaction_status = msg[0];
                PGXCNodeSetConnectionState(conn, DN_CONNECTION_STATE_IDLE);
                conn->combiner = NULL;

				elog(DEBUG5, "remote_node %s remote_pid %d, conn->transaction_status %c", conn->nodename, conn->backend_pid, conn->transaction_status);
#ifdef DN_CONNECTION_DEBUG
                conn->have_row_desc = false;
#endif

#ifdef __TBASE__
                if (enable_statistic)
                {
                    elog(LOG, "ConnectionFetchDatarows: remote_node %s remote_pid %d, datarows %ld", conn->nodename, conn->backend_pid, conn->recv_datarows);
                }
#endif

#ifdef     _PG_REGRESS_
                elog(LOG, "ReadyForQuery from node %s, remote pid %d", conn->nodename, conn->backend_pid);
#endif
                return RESPONSE_READY;
            }
            
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
            case 'Y':            /* ReadyForCommit */
            {
                PGXCNodeSetConnectionState(conn, DN_CONNECTION_STATE_IDLE);
                conn->combiner = NULL;
#ifdef DN_CONNECTION_DEBUG
                conn->have_row_desc = false;
#endif
                elog(LOG, "ReadyForQuery from node %s, remote pid %d", conn->nodename, conn->backend_pid);
                return RESPONSE_READY;
            }
#endif
            case 'M':            /* Command Id */
                HandleDatanodeCommandId(combiner, msg, msg_len);
                break;
                
            case 'b':
                PGXCNodeSetConnectionState(conn, DN_CONNECTION_STATE_IDLE);
                return RESPONSE_BARRIER_OK;
                
            case 'I':            /* EmptyQuery */
#ifdef     _PG_REGRESS_
                elog(LOG, "RESPONSE_COMPLETE_3 from node %s, remote pid %d", conn->nodename, conn->backend_pid);
#endif
                return RESPONSE_COMPLETE;
                
            case 'W':
                HandleWaitXids(msg, msg_len);    
                return RESPONSE_WAITXIDS;
                
            case 'x':
                #ifdef __USE_GLOBAL_SNAPSHOT__
                HandleGlobalTransactionId(msg, msg_len);
                #else
                elog(ERROR, "should not set global xid");
                #endif
                return RESPONSE_ASSIGN_GXID;
                
#ifdef __TBASE__
			case 'i': /* Remote Instrument */
				if (msg_len > 0)
					HandleRemoteInstr(msg, msg_len, conn->nodeid, combiner);
				/* just break to return EOF. */
				break;
#endif
            default:
                /* sync lost? */
                elog(WARNING, "Received unsupported message type: %c", msg_type);
                PGXCNodeSetConnectionState(conn, DN_CONNECTION_STATE_ERROR_FATAL);
                /* stop reading */
                elog(LOG, "RESPONSE_COMPLETE_4 from node %s, remote pid %d", conn->nodename, conn->backend_pid);
                return RESPONSE_COMPLETE;
        }
    }
    /* never happen, but keep compiler quiet */
    return RESPONSE_EOF;
}

/*
 * Has the data node sent Ready For Query
 */

bool
is_data_node_ready(PGXCNodeHandle * conn)
{
    char        *msg;
    int            msg_len;
    char        msg_type;
    size_t      data_len = 0;

    for (;;)
    {
        /* No data available, exit */
        if (!HAS_MESSAGE_BUFFERED(conn))
        {
            return false;
        }    

        
        /*
        * If the length of one data row is longger than MaxAllocSize>>1, 
        * it seems there was something wrong,
        * to close this connection should be a better way to save reading loop and avoid overload read buffer.
        */
        data_len = ntohl(*((uint32_t *) ((conn)->inBuffer + (conn)->inCursor + 1)));
        if (data_len >= (MaxAllocSize>>1))
        {
            elog(LOG, "size:%lu too big in buffer, close socket on node:%u now", data_len, conn->nodeoid);
            close(conn->sock);
            conn->sock = NO_SOCKET;
            return true;
        }


        msg_type = get_message(conn, &msg_len, &msg);
        if ('Z' == msg_type)
        {
            /*
             * Return result depends on previous connection state.
             * If it was PORTAL_SUSPENDED Coordinator want to send down
             * another EXECUTE to fetch more rows, otherwise it is done
             * with the connection
             */
            conn->last_command = msg_type;
            conn->transaction_status = msg[0];
            PGXCNodeSetConnectionState(conn, DN_CONNECTION_STATE_IDLE);
            conn->combiner = NULL;
            return true;
        }
    }
    /* never happen, but keep compiler quiet */
    return false;
}

/*
 * Send BEGIN command to the Datanodes or Coordinators and receive responses.
 * Also send the GXID for the transaction.
 */
static int
pgxc_node_begin(int conn_count, PGXCNodeHandle **connections,
				GlobalTransactionId gxid, bool need_tran_block, bool readOnly)
{
#define    SET_CMD_LENGTH 128
    int            i;
    struct timeval *timeout = NULL;
    ResponseCombiner combiner;
    TimestampTz timestamp = GetCurrentGTMStartTimestamp();
    PGXCNodeHandle *new_connections[conn_count];
    int new_count = 0;
    char            *init_str = NULL;
    char             set_cmd[SET_CMD_LENGTH];
#ifdef __TBASE__
    const char*     begin_cmd = "BEGIN";
    const char*     begin_subtxn_cmd = "BEGIN_SUBTXN";
    const char*        begin_both_cmd = "BEGIN;BEGIN_SUBTXN";
    const char*     cmd = begin_cmd;
    bool            need_send_begin = false;
#endif
    /*
     * If no remote connections, we don't have anything to do
     */
    if (conn_count == 0)
        return 0;

    for (i = 0; i < conn_count; i++)
    {
        if (!readOnly && !IsConnFromDatanode())
            connections[i]->read_only = false;
        /*
         * PGXC TODO - A connection should not be in DN_CONNECTION_STATE_QUERY
         * state when we are about to send a BEGIN TRANSACTION command to the
         * node. We should consider changing the following to an assert and fix
         * any bugs reported
         */
        if (connections[i]->state == DN_CONNECTION_STATE_QUERY)
			BufferConnection(connections[i], false);

		/* Send global session id */
		if (pgxc_node_send_sessionid(connections[i]))
		{
			elog(WARNING, "pgxc_node_begin sending session id failed");
			return EOF;
		}

        /* Send GXID and check for errors */
        if (pgxc_node_send_gxid(connections[i], gxid))
        {
			elog(WARNING, "pgxc_node_begin gxid %u is invalid.", gxid);
            return EOF;
        }

		/* Send timestamp and check for errors */
		if (GlobalTimestampIsValid(timestamp) &&
			pgxc_node_send_timestamp(connections[i], timestamp))
		{
			elog(WARNING, "pgxc_node_begin sending timestamp fails: local start"
					" timestamp" INT64_FORMAT, timestamp);
			return EOF;
		}
        if (IS_PGXC_DATANODE && GlobalTransactionIdIsValid(gxid))
        {
            need_tran_block = true;
        }
        else if (IS_PGXC_REMOTE_COORDINATOR)
        {
            need_tran_block = false;
        }

#ifdef __TBASE__
        if (need_tran_block && 'I' == connections[i]->transaction_status )
        {
            need_send_begin = true;
        }

		if (connections[i]->plpgsql_need_begin_sub_txn &&
			'I' == connections[i]->transaction_status)
		{
			need_send_begin = true;
			cmd = begin_both_cmd;
			connections[i]->plpgsql_need_begin_txn = false;
			connections[i]->plpgsql_need_begin_sub_txn = false;
			if (PlpgsqlDebugPrint)
			{
				elog(LOG, "[PLPGSQL] pgxc_node_begin cmd:%s conn->plpgsql_need_begin_txn "
						"was true, and conn->plpgsql_need_begin_sub_txn was true. "
						"in_plpgsql_exec_fun:%d", cmd, g_in_plpgsql_exec_fun);
			}
		}
		else if (connections[i]->plpgsql_need_begin_txn &&
				'I' == connections[i]->transaction_status)
		{
			need_send_begin = true;
			connections[i]->plpgsql_need_begin_txn = false;
			if (PlpgsqlDebugPrint)
			{
				elog(LOG, "[PLPGSQL] pgxc_node_begin cmd:%s conn->plpgsql_need_begin_txn "
						"was true, g_in_plpgsql_exec_fun:%d, conn->plpgsql_need_begin_sub_txn:%d",
							cmd, g_in_plpgsql_exec_fun, connections[i]->plpgsql_need_begin_sub_txn);
			}
		}
		else if (connections[i]->plpgsql_need_begin_sub_txn)
		{
			need_send_begin = true;
			cmd = begin_subtxn_cmd;
			connections[i]->plpgsql_need_begin_sub_txn = false;
			if (PlpgsqlDebugPrint)
			{
				elog(LOG, "[PLPGSQL] pgxc_node_begin cmd:%s conn->plpgsql_need_begin_sub_txn was"
						" true, g_in_plpgsql_exec_fun:%d, conn->plpgsql_need_begin_txn:%d",
							cmd, g_in_plpgsql_exec_fun, connections[i]->plpgsql_need_begin_txn);
			}
			if ('T' != connections[i]->transaction_status)
			{
				elog(PANIC, "[PLPGSQL] pgxc_node_begin need_begin_sub_txn wrong"
						"transaction_status[%c]", connections[i]->transaction_status);
			}
		}

		/*
		 * If exec savepoint command, we make sure begin should send(NB:can be
		 * sent only once) before send savepoint
		 */
		if ('I' == connections[i]->transaction_status && SavepointDefined())
		{
			need_send_begin = true;
		}

		/* 
		 * Send the Coordinator info down to the PGXC node at the beginning of
		 * transaction, In this way, Datanode can print this Coordinator info
		 * into logfile, and those infos can be found in Datanode logifile if
		 * needed during debugging
		 */
		if (need_send_begin && IS_PGXC_COORDINATOR)
		{
			pgxc_node_send_coord_info(connections[i], MyProcPid, MyProc->lxid);
		}
#endif

		elog(DEBUG5, "[PLPGSQL] pgxc_node_begin need_tran_block %d,"
				"connections[%d]->transaction_status %c need_send_begin:%d",
				need_tran_block, i, connections[i]->transaction_status,
				need_send_begin);

        /* Send BEGIN if not already in transaction */
        if (need_send_begin)
        {
            /* Send the BEGIN TRANSACTION command and check for errors */
            if (pgxc_node_send_query(connections[i], cmd))
            {
                return EOF;
            }

			elog(DEBUG5, "pgxc_node_begin send %s to node %s, pid:%d", cmd,
					connections[i]->nodename, connections[i]->backend_pid);
			new_connections[new_count++] = connections[i];
			/* if send begin, register current connection */
			register_transaction_handles(connections[i]);
        }
    }

    /*
     * If we did not send a BEGIN command to any node, we are done. Otherwise,
     * we need to check for any errors and report them
     */
    if (new_count == 0)
        return 0;

    InitResponseCombiner(&combiner, new_count, COMBINE_TYPE_NONE);
    /*
     * Make sure there are zeroes in unused fields
     */
    memset(&combiner, 0, sizeof(ScanState));

    /* Receive responses */
    if (pgxc_node_receive_responses(new_count, new_connections, timeout, &combiner))
    {
        elog(WARNING, "pgxc_node_begin receive response fails.");
        return EOF;
    }
    /* Verify status */
    if (!ValidateAndCloseCombiner(&combiner))
    {
        elog(LOG, "pgxc_node_begin validating response fails.");
        return EOF;
    }
    /* Send virtualXID to the remote nodes using SET command */
    
    snprintf(set_cmd, SET_CMD_LENGTH, "%u", MyProc->lxid);
    PGXCNodeSetParam(true, "coordinator_lxid", set_cmd, 0);    

    /* after transactions are started send down local set commands */
    init_str = PGXCNodeGetTransactionParamStr();

#if 0    
    if (PoolManagerSetCommand(new_connections, new_count, POOL_CMD_LOCAL_SET, init_str) < 0)
    {        
        elog(ERROR, "pgxc_node_begin TBase ERROR SET, query:%s, new_count:%d", init_str, new_count);
    }
#endif

    if (init_str)
    {
        for (i = 0; i < new_count; i++)
        {
			pgxc_node_set_query(new_connections[i], init_str);
			elog(DEBUG5, "pgxc_node_begin send %s to node %s, pid:%d", init_str,
					new_connections[i]->nodename, new_connections[i]->backend_pid);
        }
    }

    /* No problem, let's get going */
    return 0;
}


/*
 * Execute DISCARD ALL command on all allocated nodes to remove all session
 * specific stuff before releasing them to pool for reuse by other sessions.
 */
static void
pgxc_node_remote_cleanup_all(void)
{
    PGXCNodeAllHandles *handles = get_current_handles();
    PGXCNodeHandle *new_connections[handles->co_conn_count + handles->dn_conn_count];
    int                new_conn_count = 0;
    int                i;
	/* if it's called by sub-commit or sub-abort, DO NOT reset global_session */
	char		   *resetcmd = "RESET ALL;"
                               "RESET SESSION AUTHORIZATION;"
                               "RESET transaction_isolation;"
                               "RESET global_session";

    elog(DEBUG5, "pgxc_node_remote_cleanup_all - handles->co_conn_count %d,"
            "handles->dn_conn_count %d", handles->co_conn_count,
            handles->dn_conn_count);
    /*
     * We must handle reader and writer connections both since even a read-only
     * needs to be cleaned up.
     */
    if (handles->co_conn_count + handles->dn_conn_count == 0)
    {
        pfree_pgxc_all_handles(handles);
        return;
    }

	/* Do not cleanup connections if we have prepared statements on nodes */
	if (HaveActiveDatanodeStatements())
	{
		return;
	}

    /*
     * Send down snapshot followed by DISCARD ALL command.
     */
    for (i = 0; i < handles->co_conn_count; i++)
    {
        PGXCNodeHandle *handle = handles->coord_handles[i];

        /* At this point connection should be in IDLE state */
        if (handle->state != DN_CONNECTION_STATE_IDLE)
        {
            PGXCNodeSetConnectionState(handle, DN_CONNECTION_STATE_ERROR_FATAL);
            continue;
        }

#ifdef __TBASE__
		/* 
		 * At the end of the transaction, 
		 * clean up the CN info sent to the DN in pgxc_node_begin
		 */
		if (IS_PGXC_COORDINATOR)
		{
			pgxc_node_send_coord_info(handle, 0, 0);
		}
#endif
        /*
         * We must go ahead and release connections anyway, so do not throw
         * an error if we have a problem here.
         */
        if (pgxc_node_send_query(handle, resetcmd))
        {
            ereport(WARNING,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Failed to clean up data nodes")));
            PGXCNodeSetConnectionState(handle, DN_CONNECTION_STATE_ERROR_FATAL);
            continue;
        }
        new_connections[new_conn_count++] = handle;
        handle->combiner = NULL;
    }
    for (i = 0; i < handles->dn_conn_count; i++)
    {
        PGXCNodeHandle *handle = handles->datanode_handles[i];

        /* At this point connection should be in IDLE state */
        if (handle->state != DN_CONNECTION_STATE_IDLE)
        {
            PGXCNodeSetConnectionState(handle, DN_CONNECTION_STATE_ERROR_FATAL);
            continue;
        }

#ifdef __TBASE__
		/* 
		 * At the end of the transaction, 
		 * clean up the CN info sent to the DN in pgxc_node_begin
		 */
		if (IS_PGXC_COORDINATOR)
		{
			pgxc_node_send_coord_info(handle, 0, 0);
		}
#endif
        /*
         * We must go ahead and release connections anyway, so do not throw
         * an error if we have a problem here.
         */
        if (pgxc_node_send_query(handle, resetcmd))
        {
            ereport(WARNING,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Failed to clean up data nodes")));
            PGXCNodeSetConnectionState(handle, DN_CONNECTION_STATE_ERROR_FATAL);
            continue;
        }
        new_connections[new_conn_count++] = handle;
        handle->combiner = NULL;
    }

    if (new_conn_count)
    {
        ResponseCombiner combiner;
        InitResponseCombiner(&combiner, new_conn_count, COMBINE_TYPE_NONE);
        /* Receive responses */
        pgxc_node_receive_responses(new_conn_count, new_connections, NULL, &combiner);
        CloseCombiner(&combiner);
    }
    pfree_pgxc_all_handles(handles);
}

/*
 * Count how many coordinators and datanodes are involved in this transaction
 * so that we can save that information in the GID
 */
static void
pgxc_node_remote_count(int *dnCount, int dnNodeIds[],
        int *coordCount, int coordNodeIds[])
{// #lizard forgives
    int i;
    PGXCNodeAllHandles *handles = get_current_handles();

    *dnCount = *coordCount = 0;
    for (i = 0; i < handles->dn_conn_count; i++)
    {
        PGXCNodeHandle *conn = handles->datanode_handles[i];
        /*
         * Skip empty slots
         */
        if (conn->sock == NO_SOCKET)
            continue;
        else if (conn->transaction_status == 'T')
        {
            if (!conn->read_only)
            {
                dnNodeIds[*dnCount] = conn->nodeid;
                *dnCount = *dnCount + 1;
            }
        }
    }

    for (i = 0; i < handles->co_conn_count; i++)
    {
        PGXCNodeHandle *conn = handles->coord_handles[i];
        /*
         * Skip empty slots
         */
        if (conn->sock == NO_SOCKET)
            continue;
        else if (conn->transaction_status == 'T')
        {
            if (!conn->read_only)
            {
                coordNodeIds[*coordCount] = conn->nodeid;
                *coordCount = *coordCount + 1;
            }
        }
    }
    pfree_pgxc_all_handles(handles);
}

/*
 * Prepare nodes which ran write operations during the transaction.
 * Read only remote transactions are committed and connections are released
 * back to the pool.
 * Function returns the list of nodes where transaction is prepared, including
 * local node, if requested, in format expected by the GTM server.
 * If something went wrong the function tries to abort prepared transactions on
 * the nodes where it succeeded and throws error. A warning is emitted if abort
 * prepared fails.
 * After completion remote connection handles are released.
 */
static char *
pgxc_node_remote_prepare(char *prepareGID, bool localNode, bool implicit)
{// #lizard forgives
    bool             isOK = true;
    StringInfoData     nodestr;
    char            *prepare_cmd = (char *) palloc (64 + strlen(prepareGID));
    char            *abort_cmd;
    GlobalTransactionId startnodeXid;
#ifdef __USE_GLOBAL_SNAPSHOT__
    GlobalTransactionId auxXid;
#endif
    char           *commit_cmd = "COMMIT TRANSACTION";
    int                i;
    ResponseCombiner combiner;
    PGXCNodeHandle **connections = NULL;
    int                conn_count = 0;
	/* get current transaction handles that we register when pgxc_node_begin */
	PGXCNodeAllHandles *handles = get_current_txn_handles();
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
    GlobalTimestamp global_prepare_ts = InvalidGlobalTimestamp;
#endif
#ifdef __TWO_PHASE_TRANS__
    /* conn_state_index record index in g_twophase_state.conn_state or g_twophase_state.datanode_state */
    int             conn_state_index = 0; 
    int             twophase_index = 0;
    StringInfoData  partnodes;
#endif
    connections = (PGXCNodeHandle**)palloc(sizeof(PGXCNodeHandle*) * (TBASE_MAX_DATANODE_NUMBER + TBASE_MAX_COORDINATOR_NUMBER));
    if (connections == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory for connections")));
    }
    
    initStringInfo(&nodestr);
    if (localNode)
        appendStringInfoString(&nodestr, PGXCNodeName);

    sprintf(prepare_cmd, "PREPARE TRANSACTION '%s'", prepareGID);
#ifdef __TWO_PHASE_TESTS__
    twophase_in = IN_REMOTE_PREPARE;
#endif

#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
	if(implicit)
	{
        if(enable_distri_print)
        {
			elog(LOG, "prepare remote transaction xid %d gid %s", GetTopTransactionIdIfAny(), prepareGID);
        }
        global_prepare_ts = GetGlobalTimestampGTM();

#ifdef __TWO_PHASE_TESTS__
    if (PART_PREPARE_GET_TIMESTAMP == twophase_exception_case)
    {
        global_prepare_ts = 0;
    }
#endif
		if(!GlobalTimestampIsValid(global_prepare_ts)){
            ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
             errmsg("failed to get global timestamp for PREPARED command")));
        }
        if(enable_distri_print)
        {
			elog(LOG, "prepare phase get global prepare timestamp gid %s, time " INT64_FORMAT, prepareGID, global_prepare_ts);
        }
        SetGlobalPrepareTimestamp(global_prepare_ts);
	}
#endif

#ifdef __TWO_PHASE_TRANS__
    /* 
     *g_twophase_state is cleared under the following circumstances:
     *1.after explicit 'prepare transaction', since implicit twophase trans can be created after explicit prepare;
     *2.in CleanupTransaction after AbortTransaction;
     *3.in FinishPreparedTransaction;
     *under all the above situations, g_twophase_state must be cleared here
     */
#if 0     
    Assert('\0' == g_twophase_state.gid[0]);
    Assert(TWO_PHASE_INITIALTRANS == g_twophase_state.state);
#endif    
    ClearLocalTwoPhaseState();
    get_partnodes(handles, &partnodes);
    
    /* 
     *if conn->readonly == true, that is no update in this trans, do not record in g_twophase_state 
     */
    if ('\0' != partnodes.data[0])
    {
        g_twophase_state.is_start_node = true;
        /* strlen of prepareGID is checked in MarkAsPreparing, it satisfy strlen(gid) >= GIDSIZE  */
        strncpy(g_twophase_state.gid, prepareGID, strlen(prepareGID) + 1);
        g_twophase_state.state = TWO_PHASE_PREPARING;
        SetLocalTwoPhaseStateHandles(handles);
        strncpy(g_twophase_state.start_node_name, PGXCNodeName, 
                strnlen(PGXCNodeName, NAMEDATALEN) + 1);
        strncpy(g_twophase_state.participants, partnodes.data, partnodes.len + 1);
        /* 
         *if startnode participate this twophase trans, then send CurrentTransactionId, 
         *or just send 0 as TransactionId 
         */
         
        startnodeXid = GetCurrentTransactionId();
        g_twophase_state.start_xid = startnodeXid;

        if (enable_distri_print)
        {
            if ('\0' == g_twophase_state.start_node_name[0])
            {
                elog(PANIC, "remote prepare record remote 2pc on node:%s,  gid: %s, startnode:%s, startxid: %u, "
                                             "partnodes:%s, localxid: %u", PGXCNodeName, g_twophase_state.gid, 
                                                 g_twophase_state.start_node_name, 
                                                 g_twophase_state.start_xid, 
                                                 g_twophase_state.participants, 
                                                 g_twophase_state.start_xid);
            }
            else
            {
                elog(LOG, "remote prepare record remote 2pc on node:%s,  gid: %s, startnode:%s, startxid: %u, "
                                             "partnodes:%s, localxid: %u", PGXCNodeName, g_twophase_state.gid, 
                                                 g_twophase_state.start_node_name, 
                                                 g_twophase_state.start_xid, 
                                                 g_twophase_state.participants, 
                                                 g_twophase_state.start_xid);
            }
        }
    }
#endif

    for (i = 0; i < handles->dn_conn_count; i++)
    {
        PGXCNodeHandle *conn = handles->datanode_handles[i];

        /*
         * If something went wrong already we have nothing to do here. The error
         * will be reported at the end of the function, and we will rollback
         * remotes as part of the error handling.
         * Just skip to clean up section and check if we have already prepared
         * somewhere, we should abort that prepared transaction.
         */
        if (!isOK)
            goto prepare_err;

        /*
         * Skip empty slots
         */
        if (conn->sock == NO_SOCKET)
        {
            elog(ERROR, "pgxc_node_remote_prepare, remote node %s's connection handle is invalid, backend_pid: %d",
                 conn->nodename, conn->backend_pid);
        }
        else if (conn->transaction_status == 'T')
        {
            /* Read in any pending input */
            if (conn->state != DN_CONNECTION_STATE_IDLE)
				BufferConnection(conn, false);

            if (conn->read_only)
            {

#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
				if(implicit)
				{
                    if(enable_distri_print)
                    {
						elog(LOG, "send prepare timestamp for xid %d gid %s prepare ts " INT64_FORMAT,GetTopTransactionIdIfAny(),
                                                        prepareGID, global_prepare_ts);
                    }
                    if (pgxc_node_send_prepare_timestamp(conn, global_prepare_ts))
                    {
                        ereport(ERROR,
                                (errcode(ERRCODE_INTERNAL_ERROR),
								 errmsg("failed to send global prepare committs for PREPARED command")));
					}
                }
#endif
                /* Send down prepare command */
                if (pgxc_node_send_query(conn, commit_cmd))
                {
                    /*
                     * not a big deal, it was read only, the connection will be
                     * abandoned later.
                     */
                    ereport(LOG,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("failed to send COMMIT command to "
                                "the node %u", conn->nodeoid)));
                }
                else
                {
                    /* Read responses from these */
                    connections[conn_count++] = conn;
                }
            }
            else
            {
#ifdef __TWO_PHASE_TRANS__
                /* 
                 *only record connections that satisfy !conn->readonly 
                 */
                twophase_index = g_twophase_state.datanode_index;
                g_twophase_state.datanode_state[twophase_index].is_participant = true;
                g_twophase_state.datanode_state[twophase_index].handle_idx = i;
                g_twophase_state.datanode_state[twophase_index].state = g_twophase_state.state;
#endif

#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
				if(implicit)
				{
                    if(enable_distri_print)
                    {
						elog(LOG, "send prepare timestamp for xid %d gid %s prepare ts " INT64_FORMAT,GetTopTransactionIdIfAny(),
                                                        prepareGID, global_prepare_ts);
                    }
                    if (pgxc_node_send_prepare_timestamp(conn, global_prepare_ts))
                    {
#ifdef __TWO_PHASE_TRANS__
                        /* record connection error */
                        g_twophase_state.datanode_state[twophase_index].conn_state = 
                            TWO_PHASE_SEND_TIMESTAMP_ERROR;
                        g_twophase_state.datanode_state[twophase_index].state = 
                            TWO_PHASE_PREPARE_ERROR;
#endif
                        ereport(ERROR,
                                (errcode(ERRCODE_INTERNAL_ERROR),
								 errmsg("failed to send global prepare committs for PREPARED command")));
					}
                }
#endif

#ifdef __TWO_PHASE_TRANS__
                if (pgxc_node_send_starter(conn, PGXCNodeName))
                {
                    /* record connection error */
                    g_twophase_state.datanode_state[twophase_index].conn_state = 
                        TWO_PHASE_SEND_STARTER_ERROR;
                    g_twophase_state.datanode_state[twophase_index].state = 
                        TWO_PHASE_PREPARE_ERROR;
                    ereport(ERROR,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("failed to send startnode for PREPARED command")));
                }
                if (pgxc_node_send_startxid(conn, startnodeXid))
                {
                    /* record connection error */
                    g_twophase_state.datanode_state[twophase_index].conn_state = 
                        TWO_PHASE_SEND_STARTXID_ERROR;
                    g_twophase_state.datanode_state[twophase_index].state = 
                        TWO_PHASE_PREPARE_ERROR;
                    ereport(ERROR,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("failed to send startxid for PREPARED command")));
                }
                Assert(0 != partnodes.len);
                if (enable_distri_print)
                {
                    elog(LOG, "twophase trans: %s, partnodes: %s", prepareGID, partnodes.data);
                }
                if (pgxc_node_send_partnodes(conn, partnodes.data))
                {
                    /* record connection error */
                    g_twophase_state.datanode_state[twophase_index].conn_state = 
                        TWO_PHASE_SEND_PARTICIPANTS_ERROR;
                    g_twophase_state.datanode_state[twophase_index].state = 
                        TWO_PHASE_PREPARE_ERROR;
                    ereport(ERROR,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("failed to send partnodes for PREPARED command")));
                }
#endif 
                /* Send down prepare command */
                if (pgxc_node_send_query(conn, prepare_cmd))
                {
#ifdef __TWO_PHASE_TRANS__
                    /* record connection error */
                    g_twophase_state.datanode_state[twophase_index].conn_state = 
                        TWO_PHASE_SEND_QUERY_ERROR;
                    g_twophase_state.datanode_state[twophase_index].state = 
                        TWO_PHASE_PREPARE_ERROR;
#endif
                    /*
                     * That is the trouble, we really want to prepare it.
                     * Just emit warning so far and go to clean up.
                     */
                    isOK = false;
                    ereport(WARNING,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("failed to send PREPARE TRANSACTION command to "
                                "the node %u", conn->nodeoid)));
                }
                else
                {
                    char *nodename = get_pgxc_nodename(conn->nodeoid);
                    if (nodestr.len > 0)
                        appendStringInfoChar(&nodestr, ',');
                    appendStringInfoString(&nodestr, nodename);
                    /* Read responses from these */
                    connections[conn_count++] = conn;
                    /*
                     * If it fails on remote node it would just return ROLLBACK.
                     * Set the flag for the message handler so the response is
                     * verified.
                     */
                    conn->ck_resp_rollback = true;
#ifdef __TWO_PHASE_TRANS__
                    g_twophase_state.datanode_state[twophase_index].conn_state = 
                        TWO_PHASE_HEALTHY;
                    twophase_index = g_twophase_state.connections_num;
                    /* 
                     * g_twophase_state.connections stores relation among connections, coord_state, datanode_state
                     * for example, in receive_response, receive a readyforquery from connection[i]
                     * if (g_twophase_state.connections[i].node_type == coordinator) then 
                     * twophase_index = g_twophase_state.connections[i].conn_trans_state_index
                     * and we update g_twophase_state.coord_state[twophase_index] = COMMITTED OR ABORTTED
                     */
                    g_twophase_state.connections[twophase_index].node_type = PGXC_NODE_DATANODE;
                    g_twophase_state.connections[twophase_index].conn_trans_state_index = 
                        g_twophase_state.datanode_index;
                    g_twophase_state.connections_num++;
                    g_twophase_state.datanode_index++;
#endif
                }
            }
        }
        else if (conn->transaction_status == 'E')
        {
            /*
             * Probably can not happen, if there was a error the engine would
             * abort anyway, even in case of explicit PREPARE.
             * Anyway, just in case...
             */
            isOK = false;
            ereport(WARNING,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("remote node %u is in error state", conn->nodeoid)));
        }
    }

    for (i = 0; i < handles->co_conn_count; i++)
    {
        PGXCNodeHandle *conn = handles->coord_handles[i];

        /*
         * If something went wrong already we have nothing to do here. The error
         * will be reported at the end of the function, and we will rollback
         * remotes as part of the error handling.
         * Just skip to clean up section and check if we have already prepared
         * somewhere, we should abort that prepared transaction.
         */
        if (!isOK)
            goto prepare_err;

        /*
         * Skip empty slots
         */
        if (conn->sock == NO_SOCKET)
        {
            elog(ERROR, "pgxc_node_remote_prepare, remote node %s's connection handle is invalid, backend_pid: %d",
                 conn->nodename, conn->backend_pid);
        }
        else if (conn->transaction_status == 'T')
        {
            if (conn->read_only)
            {
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
				if(implicit)
				{
                    if(enable_distri_print)
                    {
						elog(LOG, "send prepare timestamp for xid %d gid %s prepare ts " INT64_FORMAT,GetTopTransactionIdIfAny(),
                                                        prepareGID, global_prepare_ts);
                    }
                    if (pgxc_node_send_prepare_timestamp(conn, global_prepare_ts))
                    {
                        ereport(ERROR,
                                (errcode(ERRCODE_INTERNAL_ERROR),
								 errmsg("failed to send global prepare committs for PREPARED command")));
					}
                }
#endif
                /* Send down prepare command */
                if (pgxc_node_send_query(conn, commit_cmd))
                {
                    /*
                     * not a big deal, it was read only, the connection will be
                     * abandoned later.
                     */
                    ereport(LOG,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("failed to send COMMIT command to "
                                "the node %u", conn->nodeoid)));
                }
                else
                {
                    /* Read responses from these */
                    connections[conn_count++] = conn;
                }
            }
            else
            {
#ifdef __TWO_PHASE_TRANS__
                twophase_index = g_twophase_state.coord_index;
                g_twophase_state.coord_state[twophase_index].is_participant = true;
                g_twophase_state.coord_state[twophase_index].handle_idx = i; 
                g_twophase_state.coord_state[twophase_index].state = g_twophase_state.state;
#endif

#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
				if(implicit)
				{
                    if(enable_distri_print)
                    {
						elog(LOG, "send prepare timestamp for xid %d gid %s prepare ts " INT64_FORMAT,GetTopTransactionIdIfAny(),
                                                        prepareGID, global_prepare_ts);
                    }
                    if (pgxc_node_send_prepare_timestamp(conn, global_prepare_ts))
                    {
#ifdef __TWO_PHASE_TRANS__
                        /* record connection error */
                        g_twophase_state.coord_state[twophase_index].conn_state = 
                            TWO_PHASE_SEND_TIMESTAMP_ERROR;
                        g_twophase_state.coord_state[twophase_index].state = 
                            TWO_PHASE_PREPARE_ERROR;
#endif
                        ereport(ERROR,
                                (errcode(ERRCODE_INTERNAL_ERROR),
								 errmsg("failed to send global prepare committs for PREPARED command")));
					}
                }
#endif

#ifdef __TWO_PHASE_TRANS__
                if (pgxc_node_send_starter(conn, PGXCNodeName))
                {
                    /* record connection error */
                    g_twophase_state.coord_state[twophase_index].conn_state = 
                        TWO_PHASE_SEND_STARTER_ERROR;
                    g_twophase_state.coord_state[twophase_index].state = 
                        TWO_PHASE_PREPARE_ERROR;
                    ereport(ERROR,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("failed to send startnode for PREPARED command")));
                }
                if (pgxc_node_send_startxid(conn, startnodeXid))
                {
                    /* record connection error */
                    g_twophase_state.coord_state[twophase_index].conn_state = 
                        TWO_PHASE_SEND_STARTXID_ERROR;
                    g_twophase_state.coord_state[twophase_index].state = 
                        TWO_PHASE_PREPARE_ERROR;
                    ereport(ERROR,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("failed to send startxid for PREPARED command")));
                }
                Assert(0 != partnodes.len);
                if (enable_distri_print)
                {
                    elog(LOG, "twophase trans: %s, partnodes: %s", prepareGID, partnodes.data);
                }
                if (pgxc_node_send_partnodes(conn, partnodes.data))
                {
                    /* record connection error */
                    g_twophase_state.coord_state[twophase_index].conn_state = 
                        TWO_PHASE_SEND_PARTICIPANTS_ERROR;
                    g_twophase_state.coord_state[twophase_index].state = 
                        TWO_PHASE_PREPARE_ERROR;
                    ereport(ERROR,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("failed to send partnodes for PREPARED command")));
                }
#endif

                /* Send down prepare command */
                if (pgxc_node_send_query(conn, prepare_cmd))
                {
#ifdef __TWO_PHASE_TRANS__
                    /* record connection error */
                    g_twophase_state.coord_state[twophase_index].conn_state = 
                        TWO_PHASE_SEND_QUERY_ERROR;
                    g_twophase_state.coord_state[twophase_index].state = 
                        TWO_PHASE_PREPARE_ERROR;
#endif
                    /*
                     * That is the trouble, we really want to prepare it.
                     * Just emit warning so far and go to clean up.
                     */
                    isOK = false;
                    ereport(WARNING,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("failed to send PREPARE TRANSACTION command to "
                                "the node %u", conn->nodeoid)));
                }
                else
                {
                    char *nodename = get_pgxc_nodename(conn->nodeoid);
                    if (nodestr.len > 0)
                        appendStringInfoChar(&nodestr, ',');
                    appendStringInfoString(&nodestr, nodename);
                    /* Read responses from these */
                    connections[conn_count++] = conn;
                    /*
                     * If it fails on remote node it would just return ROLLBACK.
                     * Set the flag for the message handler so the response is
                     * verified.
                     */
                    conn->ck_resp_rollback = true;
#ifdef __TWO_PHASE_TRANS__
                    g_twophase_state.coord_state[twophase_index].conn_state = 
                        TWO_PHASE_HEALTHY;
                    twophase_index = g_twophase_state.connections_num;
                    g_twophase_state.connections[twophase_index].conn_trans_state_index = 
                        g_twophase_state.coord_index;
                    g_twophase_state.connections[twophase_index].node_type = 
                        PGXC_NODE_COORDINATOR;
                    g_twophase_state.connections_num++;
                    g_twophase_state.coord_index++;
#endif
                }
            }
        }
        else if (conn->transaction_status == 'E')
        {
            /*
             * Probably can not happen, if there was a error the engine would
             * abort anyway, even in case of explicit PREPARE.
             * Anyway, just in case...
             */
            isOK = false;
            ereport(WARNING,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("remote node %u is in error state", conn->nodeoid)));
        }
    }

    SetSendCommandId(false);
#ifdef __TWO_PHASE_TESTS__
    if (PREPARE_ERROR_SEND_QUERY == twophase_exception_case ||
        PREPARE_ERROR_RESPONSE_ERROR == twophase_exception_case)
    {
        isOK = false;
    }
    if (PART_ABORT_SEND_ROLLBACK == twophase_exception_case)
    {
        elog(ERROR,"PART_ABORT_SEND_ROLLBACK in twophase tests");
    }
#endif

    if (!isOK)
        goto prepare_err;

    /* exit if nothing has been prepared */
    if (conn_count > 0)
    {
        int result;
        /*
         * Receive and check for any errors. In case of errors, we don't bail out
         * just yet. We first go through the list of connections and look for
         * errors on each connection. This is important to ensure that we run
         * an appropriate ROLLBACK command later on (prepared transactions must be
         * rolled back with ROLLBACK PREPARED commands).
         *
         * PGXCTODO - There doesn't seem to be a solid mechanism to track errors on
         * individual connections. The transaction_status field doesn't get set
         * every time there is an error on the connection. The combiner mechanism is
         * good for parallel proessing, but I think we should have a leak-proof
         * mechanism to track connection status
         */
        InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
#ifdef __TWO_PHASE_TRANS__
        g_twophase_state.response_operation = REMOTE_PREPARE;
#endif
        /* Receive responses */
        result = pgxc_node_receive_responses(conn_count, connections, NULL, &combiner);
        if (result || !validate_combiner(&combiner))
            goto prepare_err;
        else
            CloseCombiner(&combiner);

        /* Before exit clean the flag, to avoid unnecessary checks */
        for (i = 0; i < conn_count; i++)
            connections[i]->ck_resp_rollback = false;

        clear_handles();
        pfree_pgxc_all_handles(handles);
    }

    pfree(prepare_cmd);
#ifdef __TWO_PHASE_TRANS__
    g_twophase_state.state = TWO_PHASE_PREPARE_END;
    if (partnodes.maxlen)
    {
        resetStringInfo(&partnodes);
        pfree(partnodes.data);
    }
#endif

    if (connections)
    {
        pfree(connections);
        connections = NULL;
    }
    return nodestr.data;

prepare_err:
#ifdef __TWO_PHASE_TRANS__
    if (partnodes.maxlen)
    {
        resetStringInfo(&partnodes);
        pfree(partnodes.data);
    }
#endif


#ifdef __TBASE__
    /* read ReadyForQuery from connections which sent commit/commit prepared */
    if (!isOK)
    {
        if (conn_count > 0)
        {
            ResponseCombiner combiner3;
            InitResponseCombiner(&combiner3, conn_count, COMBINE_TYPE_NONE);
#ifdef __TWO_PHASE_TRANS__
            g_twophase_state.response_operation = REMOTE_PREPARE_ERROR;
#endif
            /* Receive responses */
            pgxc_node_receive_responses(conn_count, connections, NULL, &combiner3);
            CloseCombiner(&combiner3);
        }
    }
#endif

#ifdef __TWO_PHASE_TESTS__
    twophase_in = IN_PREPARE_ERROR;
#endif

     abort_cmd = (char *) palloc (64 + strlen(prepareGID));
    sprintf(abort_cmd, "ROLLBACK PREPARED '%s'", prepareGID);
#ifdef __USE_GLOBAL_SNAPSHOT__

    auxXid = GetAuxilliaryTransactionId();
#endif
    conn_count = 0;
#ifdef __TWO_PHASE_TRANS__
    g_twophase_state.connections_num = 0;
    conn_state_index = 0;
#endif
    for (i = 0; i < handles->dn_conn_count; i++)
    {
        PGXCNodeHandle *conn = handles->datanode_handles[i];

        /*
         * PREPARE succeeded on that node, roll it back there
         */
        if (conn->ck_resp_rollback)
        {
            conn->ck_resp_rollback = false;

            if (conn->state != DN_CONNECTION_STATE_IDLE)
            {
                ereport(WARNING,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("Error while PREPARING transaction %s on "
                             "node %s. Administrative action may be required "
                             "to abort this transaction on the node",
                             prepareGID, conn->nodename)));
                continue;
            }

            /* sanity checks */
            Assert(conn->sock != NO_SOCKET);
#ifdef __TWO_PHASE_TRANS__
            /* update datanode_state = TWO_PHASE_ABORTTING in prepare_err */
            while (g_twophase_state.datanode_index >= conn_state_index && 
                  g_twophase_state.datanode_state[conn_state_index].handle_idx != i)
            {
                conn_state_index++;
            }
            if (g_twophase_state.datanode_index < conn_state_index)
            {
                elog(ERROR, "in pgxc_node_remote_prepare can not find twophase_state for node %s", conn->nodename);
            }
            g_twophase_state.datanode_state[conn_state_index].state = TWO_PHASE_ABORTTING;
#endif

            /* Send down abort prepared command */
#ifdef __USE_GLOBAL_SNAPSHOT__
            if (pgxc_node_send_gxid(conn, auxXid))
            {
#ifdef __TWO_PHASE_TRANS__
                g_twophase_state.datanode_state[conn_state_index].conn_state = 
                    TWO_PHASE_SEND_GXID_ERROR;
				g_twophase_state.datanode_state[conn_state_index].state =
					TWO_PHASE_ABORT_ERROR;
#endif
                /*
                 * Prepared transaction is left on the node, but we can not
                 * do anything with that except warn the user.
                 */
                ereport(WARNING,
                        (errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("failed to send xid %u to "
								"the node %u", auxXid, conn->nodeoid)));
            }
#endif

            if (pgxc_node_send_query(conn, abort_cmd))
            {
#ifdef __TWO_PHASE_TRANS__
                g_twophase_state.datanode_state[conn_state_index].conn_state = 
                    TWO_PHASE_SEND_QUERY_ERROR;
                g_twophase_state.datanode_state[conn_state_index].state = TWO_PHASE_ABORT_ERROR;
#endif
                /*
                 * Prepared transaction is left on the node, but we can not
                 * do anything with that except warn the user.
                 */
                ereport(WARNING,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("failed to send ABORT PREPARED command to "
                                "the node %u", conn->nodeoid)));
            }
            else
            {
                /* Read responses from these */
                connections[conn_count++] = conn;
#ifdef __TWO_PHASE_TRANS__
                g_twophase_state.datanode_state[conn_state_index].conn_state = TWO_PHASE_HEALTHY;
                twophase_index = g_twophase_state.connections_num;
                g_twophase_state.connections[twophase_index].conn_trans_state_index = conn_state_index;
                g_twophase_state.connections[twophase_index].node_type = PGXC_NODE_DATANODE;
                g_twophase_state.connections_num++;
                conn_state_index++;
#endif
            }
        }
    }
#ifdef __TWO_PHASE_TRANS__
    conn_state_index = 0;
#endif
    for (i = 0; i < handles->co_conn_count; i++)
    {
        PGXCNodeHandle *conn = handles->coord_handles[i];

        if (conn->ck_resp_rollback)
        {
            conn->ck_resp_rollback = false;

            if (conn->state != DN_CONNECTION_STATE_IDLE)
            {
                ereport(WARNING,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("Error while PREPARING transaction %s on "
                             "node %s. Administrative action may be required "
                             "to abort this transaction on the node",
                             prepareGID, conn->nodename)));
                continue;
            }

            /* sanity checks */
            Assert(conn->sock != NO_SOCKET);
#ifdef __TWO_PHASE_TRANS__
            while (g_twophase_state.coord_index >= conn_state_index && 
                  g_twophase_state.coord_state[conn_state_index].handle_idx != i)
            {
                conn_state_index++;
            }
            if (g_twophase_state.coord_index < conn_state_index)
            {
                elog(ERROR, "in pgxc_node_remote_prepare can not find twophase_state for node %s", conn->nodename);
            }
            g_twophase_state.coord_state[conn_state_index].state = TWO_PHASE_ABORTTING;
#endif
            /* Send down abort prepared command */
#ifdef __USE_GLOBAL_SNAPSHOT__
            if (pgxc_node_send_gxid(conn, auxXid))
            {
#ifdef __TWO_PHASE_TRANS__
                g_twophase_state.coord_state[conn_state_index].conn_state = 
                    TWO_PHASE_SEND_GXID_ERROR;
				g_twophase_state.coord_state[conn_state_index].state =
					TWO_PHASE_ABORT_ERROR;
#endif
                /*
                 * Prepared transaction is left on the node, but we can not
                 * do anything with that except warn the user.
                 */
                ereport(WARNING,
                        (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("failed to send xid %u to "
								"the node %u", auxXid, conn->nodeoid)));
            }
#endif

            if (pgxc_node_send_query(conn, abort_cmd))
            {
#ifdef __TWO_PHASE_TRANS__
                g_twophase_state.coord_state[conn_state_index].conn_state = 
                    TWO_PHASE_SEND_QUERY_ERROR;
                g_twophase_state.coord_state[conn_state_index].state = TWO_PHASE_ABORT_ERROR;
#endif
                /*
                 * Prepared transaction is left on the node, but we can not
                 * do anything with that except warn the user.
                 */
                ereport(WARNING,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("failed to send ABORT PREPARED command to "
                                "the node %u", conn->nodeoid)));
            }
            else
            {
                /* Read responses from these */
                connections[conn_count++] = conn;
#ifdef __TWO_PHASE_TRANS__
                g_twophase_state.coord_state[conn_state_index].conn_state = TWO_PHASE_HEALTHY;
                twophase_index = g_twophase_state.connections_num;
                g_twophase_state.connections[twophase_index].conn_trans_state_index = conn_state_index;
                g_twophase_state.connections[twophase_index].node_type = PGXC_NODE_COORDINATOR;
                g_twophase_state.connections_num++;
                conn_state_index++;
#endif
            }
        }
    }
    if (conn_count > 0)
    {
        /* Just read out responses, throw error from the first combiner */
        ResponseCombiner combiner2;
        InitResponseCombiner(&combiner2, conn_count, COMBINE_TYPE_NONE);
#ifdef __TWO_PHASE_TRANS__
        g_twophase_state.response_operation = REMOTE_PREPARE_ABORT;
#endif
        /* Receive responses */
        pgxc_node_receive_responses(conn_count, connections, NULL, &combiner2);
        CloseCombiner(&combiner2);
    }

    /*
     * If the flag is set we are here because combiner carries error message
     */
    if (isOK)
        pgxc_node_report_error(&combiner);
    else
        elog(ERROR, "failed to PREPARE transaction on one or more nodes");

	if (!temp_object_included && !PersistentConnections)
    {
        /* Clean up remote sessions */
		pgxc_node_remote_cleanup_all();
        release_handles(false);
    }
    
    clear_handles();

    pfree_pgxc_all_handles(handles);
    pfree(abort_cmd);

#if 0
    /*
     * If the flag is set we are here because combiner carries error message
     */
    if (isOK)
        pgxc_node_report_error(&combiner);
    else
        elog(ERROR, "failed to PREPARE transaction on one or more nodes");
#endif

    if (connections)
    {
        pfree(connections);
        connections = NULL;
    }
    
    return NULL;
}

#ifdef __TBASE__
/*
 * Commit transactions on remote nodes.
 * If barrier lock is set wait while it is released.
 * Release remote connection after completion.
 *
 * For DDL, DN will commit before CN does.
 * Because DDL normally has conflict locks, when CN gets committed,
 * DNs will be in a consistent state for blocked user transactions.
 */
static void
pgxc_node_remote_commit(TranscationType txn_type, bool need_release_handle)
{
    int conn_count = 0;

    if (!is_txn_has_parallel_ddl)
    {
        /* normal cases */
        conn_count = pgxc_node_remote_commit_internal(get_current_handles(), txn_type);
    }
    else
    {
        /* make sure first DN then CN */
        conn_count =  pgxc_node_remote_commit_internal(get_current_dn_handles(), txn_type);
        conn_count += pgxc_node_remote_commit_internal(get_current_cn_handles(), txn_type);
    }

    stat_transaction(conn_count);

    if (!temp_object_included && !PersistentConnections && need_release_handle)
        {
            /* Clean up remote sessions */
        pgxc_node_remote_cleanup_all();
            release_handles(false);
        }

    clear_handles();
}

/*
 * Commit transactions on remote nodes.
 * If barrier lock is set wait while it is released.
 * Release remote connection after completion.
 */
static int
pgxc_node_remote_commit_internal(PGXCNodeAllHandles *handles, TranscationType txn_type)
#else
static void
pgxc_node_remote_commit(TranscationType txn_type, bool need_release_handle)
#endif
{
	int				result = 0;
	char		   *commitCmd = NULL;
	int				i;
	ResponseCombiner combiner;
	PGXCNodeHandle **connections = NULL;
	int				conn_count = 0;

#ifdef __TBASE__
    switch (txn_type)
    {
        case TXN_TYPE_CommitTxn:
            commitCmd = "COMMIT TRANSACTION";
            break;
        case TXN_TYPE_CommitSubTxn:
            commitCmd = "COMMIT_SUBTXN";
            break;
        default:
            elog(PANIC, "pgxc_node_remote_commit invalid TranscationType:%d", txn_type);
            break;
    }
#endif
    
#if 0
    GlobalTimestamp   global_committs = InvalidGlobalTimestamp; 

    if(IS_PGXC_COORDINATOR)
    {
        global_committs = GetGlobalTimestampGTM();
        if(!GlobalTimestampIsValid(global_committs)){
            ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
             errmsg("failed to get global timestamp for commit command")));
        }
        elog(DEBUG8, "commit get global commit timestamp xid %d time " INT64_FORMAT, 
                GetTopTransactionIdIfAny(), global_committs);
        SetGlobalCommitTimestamp(global_committs);/* Save for local commit */
    }
#endif

    /* palloc will FATAL when out of memory */
    connections = (PGXCNodeHandle**)palloc(sizeof(PGXCNodeHandle*) * (TBASE_MAX_DATANODE_NUMBER + TBASE_MAX_COORDINATOR_NUMBER));
    
    SetSendCommandId(false);

    /*
     * Barrier:
     *
     * We should acquire the BarrierLock in SHARE mode here to ensure that
     * there are no in-progress barrier at this point. This mechanism would
     * work as long as LWLock mechanism does not starve a EXCLUSIVE lock
     * requester
     */
    LWLockAcquire(BarrierLock, LW_SHARED);

    for (i = 0; i < handles->dn_conn_count; i++)
    {
        PGXCNodeHandle *conn = handles->datanode_handles[i];

        /* Skip empty slots */
        if (conn->sock == NO_SOCKET)
            continue;

        /*
         * We do not need to commit remote node if it is not in transaction.
         * If transaction is in error state the commit command will cause
         * rollback, that is OK
         */
        //if (conn->transaction_status != 'I' && (!is_subtxn || (is_subtxn && NodeHasBeginSubTxn(conn->nodeoid))))
        if ((conn->transaction_status != 'I' && TXN_TYPE_CommitTxn == txn_type) ||
             (conn->transaction_status != 'I' && TXN_TYPE_CommitSubTxn == txn_type && NodeHasBeginSubTxn(conn->nodeoid)))
        {
            /* Read in any pending input */
            if (conn->state != DN_CONNECTION_STATE_IDLE)
            {
				BufferConnection(conn, false);
            }

#if 0    
            if(IS_PGXC_COORDINATOR)
            {
                if (pgxc_node_send_global_timestamp(conn, global_committs))
                {
                    ereport(ERROR,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("failed to send global committs for commit command")));
                }
            }
#endif

            if (pgxc_node_send_query(conn, commitCmd))
            {
                /*
                 * Do not bother with clean up, just bomb out. The error handler
                 * will invoke RollbackTransaction which will do the work.
                 */
                ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("pgxc_node_remote_commit failed to send COMMIT command to the node %s, pid:%d, for %s",
                                conn->nodename, conn->backend_pid, strerror(errno))));
            }
            else
            {
                /* Read responses from these */
                connections[conn_count++] = conn;
            }
        }
    }

    for (i = 0; i < handles->co_conn_count; i++)
    {
        PGXCNodeHandle *conn = handles->coord_handles[i];

        /* Skip empty slots */
        if (conn->sock == NO_SOCKET)
            continue;

        /*
         * We do not need to commit remote node if it is not in transaction.
         * If transaction is in error state the commit command will cause
         * rollback, that is OK
         */
        //if (conn->transaction_status != 'I' && (!is_subtxn || (is_subtxn && NodeHasBeginSubTxn(conn->nodeoid))))
        if ((conn->transaction_status != 'I' && TXN_TYPE_CommitTxn == txn_type) ||
             (conn->transaction_status != 'I' && TXN_TYPE_CommitSubTxn == txn_type && NodeHasBeginSubTxn(conn->nodeoid)))
        {

#if 0    
            if(IS_PGXC_COORDINATOR)
            {
                if (pgxc_node_send_global_timestamp(conn, global_committs))
                {
                    ereport(ERROR,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("failed to send global committs for commit command")));
                }
            }
#endif

            if (pgxc_node_send_query(conn, commitCmd))
            {
                /*
                 * Do not bother with clean up, just bomb out. The error handler
                 * will invoke RollbackTransaction which will do the work.
                 */
                ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("pgxc_node_remote_commit failed to send COMMIT command to the node %s, pid:%d, for %s",
                                conn->nodename, conn->backend_pid, strerror(errno))));
            }
            else
            {
                /* Read responses from these */
                connections[conn_count++] = conn;
            }
        }
    }

    /*
     * Release the BarrierLock.
     */
    LWLockRelease(BarrierLock);

    if (conn_count)
    {
        InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
        
        /* Receive responses */
        result = pgxc_node_receive_responses(conn_count, connections, NULL, &combiner);
        if (result)
        {
            elog(LOG, "pgxc_node_remote_commit pgxc_node_receive_responses of COMMIT failed");
            result = EOF;
        }
        else if (!validate_combiner(&combiner))
        {
            elog(LOG, "pgxc_node_remote_commit validate_combiner responese of COMMIT failed");
            result = EOF;
        }    

		if (result)
		{
			if (combiner.errorMessage)
			{
				pgxc_node_report_error(&combiner);
			}
			else
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to COMMIT the transaction on one or more nodes")));
			}
		}
		CloseCombiner(&combiner);
	}

#ifndef __TBASE__
	stat_transaction(conn_count);

	if (!temp_object_included && !PersistentConnections && need_release_handle)
		{
			/* Clean up remote sessions */
			pgxc_node_remote_cleanup_all();
			release_handles(false);
		}
	
	clear_handles();
#endif

    pfree_pgxc_all_handles(handles);

	if (connections)
	{
		pfree(connections);
		connections = NULL;
	}

#ifdef __TBASE__
	return conn_count;
#endif
}

/*
 * Set the node begin transaction in plpgsql function
 */
static void
SetPlpgsqlTransactionBegin(PGXCNodeHandle *conn)
{
	Oid nodeOid = conn->nodeoid;

	if (NeedBeginTxn() && !NodeHasBeginTxn(nodeOid))
	{
		conn->plpgsql_need_begin_txn = true;
		SetNodeBeginTxn(nodeOid);
		if (PlpgsqlDebugPrint)
		{
			elog(LOG, "[PLPGSQL] ExecRemoteUtility conn nodename:%s "
					"backendpid:%d sock:%d nodeoid:%u need_begin_txn",
					conn->nodename, conn->backend_pid, conn->sock,
					conn->nodeoid);
		}
	}
	if (NeedBeginSubTxn() && !NodeHasBeginSubTxn(nodeOid))
	{
		conn->plpgsql_need_begin_sub_txn = true;
		SetNodeBeginSubTxn(nodeOid);
		if (PlpgsqlDebugPrint)
		{
			elog(LOG, "[PLPGSQL] ExecRemoteUtility conn nodename:%s "
					"backendpid:%d sock:%d nodeoid:%u need_begin_sub_txn",
					conn->nodename, conn->backend_pid, conn->sock,
					conn->nodeoid);
		}
	}
}

#ifdef __TWO_PHASE_TRANS__
void InitLocalTwoPhaseState(void)
{
    int participants_capacity;
    g_twophase_state.is_start_node = false;
    g_twophase_state.in_pg_clean = false;
    g_twophase_state.is_readonly = false;
    g_twophase_state.is_after_prepare = false;
    g_twophase_state.gid = (char *)MemoryContextAllocZero(TopMemoryContext, GIDSIZE);
    g_twophase_state.state = TWO_PHASE_INITIALTRANS;
    g_twophase_state.coord_index = g_twophase_state.datanode_index = 0;
    g_twophase_state.isprinted = false;
    g_twophase_state.start_node_name[0] = '\0';
    g_twophase_state.handles = NULL;
    g_twophase_state.start_xid = 0;
    g_twophase_state.connections_num = 0;
    g_twophase_state.response_operation = OTHER_OPERATIONS;
    
    if (IS_PGXC_COORDINATOR)
    {
        g_twophase_state.coord_state = (ConnTransState *)MemoryContextAllocZero(TopMemoryContext, 
                                    TBASE_MAX_COORDINATOR_NUMBER * sizeof(ConnTransState));
    }
    else
    {
        g_twophase_state.coord_state = NULL;
    }
    g_twophase_state.datanode_state = (ConnTransState *)MemoryContextAllocZero(TopMemoryContext, 
                                    TBASE_MAX_DATANODE_NUMBER * sizeof(ConnTransState));
    /* since participates conclude nodename and  ","*/
    participants_capacity = (NAMEDATALEN+1) * (TBASE_MAX_DATANODE_NUMBER + TBASE_MAX_COORDINATOR_NUMBER);
    g_twophase_state.participants = (char *)MemoryContextAllocZero(TopMemoryContext, participants_capacity);
    g_twophase_state.connections = (AllConnNodeInfo *)MemoryContextAllocZero(TopMemoryContext,
                                  (TBASE_MAX_DATANODE_NUMBER + TBASE_MAX_COORDINATOR_NUMBER) *
                                  sizeof(AllConnNodeInfo));
}

void SetLocalTwoPhaseStateHandles(PGXCNodeAllHandles *handles)
{
    g_twophase_state.handles = handles;
    g_twophase_state.connections_num = 0;
    g_twophase_state.coord_index = 0;
    g_twophase_state.datanode_index = 0;
}

void UpdateLocalTwoPhaseState(int result, PGXCNodeHandle *response_handle, int conn_index, char *errmsg)
{// #lizard forgives
    int index = 0;
    int twophase_index = 0;
    TwoPhaseTransState state = TWO_PHASE_INITIALTRANS;

    if (RESPONSE_READY != result && RESPONSE_ERROR != result)
    {
        return;
    }
    
    if (g_twophase_state.response_operation == OTHER_OPERATIONS ||
      !IsTransactionState() || 
      g_twophase_state.state == TWO_PHASE_INITIALTRANS)
    {
       return ;
    }
    Assert(NULL != g_twophase_state.handles);
    if (RESPONSE_ERROR == result)
    {
        switch (g_twophase_state.state)
        {
            case TWO_PHASE_PREPARING:
                /* receive response in pgxc_node_remote_prepare or at the begining of prepare_err */
                if (REMOTE_PREPARE == g_twophase_state.response_operation || 
                  REMOTE_PREPARE_ERROR == g_twophase_state.response_operation)
                {
                    state = TWO_PHASE_PREPARE_ERROR;
#ifdef __TWO_PHASE_TESTS__
                    if (IN_REMOTE_PREPARE == twophase_in && 
                        PART_PREPARE_RESPONSE_ERROR == twophase_exception_case)
                    {
                        complish = true;
                        if (run_pg_clean)
                        {
                            elog(STOP, "PART_PREPARE_RESPONSE_ERROR in pg_clean tests");
                        }
                    }
#endif
                }
                else if (REMOTE_PREPARE_ABORT == g_twophase_state.response_operation)
                {
                    state = TWO_PHASE_ABORT_ERROR;
#ifdef __TWO_PHASE_TESTS__
                    if (IN_PREPARE_ERROR == twophase_in && 
                        PREPARE_ERROR_RESPONSE_ERROR == twophase_exception_case)
                    {
                        complish = true;
                        elog(ERROR, "PREPARE_ERROR_RESPONSE_ERROR in pg_clean tests");
                    }
#endif
                }
            case TWO_PHASE_PREPARED:
                break;

            case TWO_PHASE_COMMITTING:
                if (REMOTE_FINISH_COMMIT == g_twophase_state.response_operation)
                {
                    state = TWO_PHASE_COMMIT_ERROR;
                }
            case TWO_PHASE_COMMITTED:
                break;
                
            case TWO_PHASE_ABORTTING:
                if (REMOTE_FINISH_ABORT == g_twophase_state.response_operation ||
                  REMOTE_ABORT == g_twophase_state.response_operation)
                {
                    state = TWO_PHASE_ABORT_ERROR;
                }
            case TWO_PHASE_ABORTTED:
                break;
            default:
                Assert((result < TWO_PHASE_INITIALTRANS) || (result > TWO_PHASE_ABORT_ERROR));
                return;
        }
        
        if (TWO_PHASE_INITIALTRANS != state)
        {
            /* update coord_state or datanode_state */
            twophase_index = g_twophase_state.connections[conn_index].conn_trans_state_index;
            if (PGXC_NODE_COORDINATOR == g_twophase_state.connections[conn_index].node_type)
            {
                g_twophase_state.coord_state[twophase_index].state = state;
                if (enable_distri_print)
                {
                    index = g_twophase_state.coord_state[twophase_index].handle_idx;
                    elog(LOG, "In UpdateLocalTwoPhaseState connections[%d] imply node: %s, g_twophase_state.coord_state[%d] imply node: %s", 
                            conn_index, 
                            response_handle->nodename, 
                            twophase_index, 
                            g_twophase_state.handles->coord_handles[index]->nodename);
                }
            }
            else
            {
                g_twophase_state.datanode_state[twophase_index].state = state;
                if (enable_distri_print)
                {
                    index = g_twophase_state.datanode_state[twophase_index].handle_idx;
                    elog(LOG, "In UpdateLocalTwoPhaseState connections[%d] imply node: %s, g_twophase_state.datanode_state[%d] imply node: %s", 
                            conn_index, 
                            response_handle->nodename, 
                            twophase_index, 
                            g_twophase_state.handles->datanode_handles[index]->nodename);
                }
            }
        }
        return;
    }
    else if (RESPONSE_READY == result && NULL == errmsg)
    {
        switch (g_twophase_state.state)
        {
            case TWO_PHASE_PREPARING:
                /* receive response in pgxc_node_remote_prepare or at the begining of prepare_err */
                if (REMOTE_PREPARE == g_twophase_state.response_operation || 
                  REMOTE_PREPARE_ERROR == g_twophase_state.response_operation)
                {
                    state = TWO_PHASE_PREPARED;
                }
                else if (REMOTE_PREPARE_ABORT == g_twophase_state.response_operation)
                {
                    state = TWO_PHASE_ABORTTED;
                }
            case TWO_PHASE_PREPARED:
                break;
        
            case TWO_PHASE_COMMITTING:
                if (REMOTE_FINISH_COMMIT == g_twophase_state.response_operation)
                {
                    state = TWO_PHASE_COMMITTED;
                }
            case TWO_PHASE_COMMITTED:
                break;
            case TWO_PHASE_ABORTTING:
                if (REMOTE_FINISH_ABORT == g_twophase_state.response_operation ||
                  REMOTE_ABORT == g_twophase_state.response_operation)
                {
                    state = TWO_PHASE_ABORTTED;
                }
            case TWO_PHASE_ABORTTED:
                break;
            default:
                Assert((result < TWO_PHASE_INITIALTRANS) || (result > TWO_PHASE_ABORT_ERROR));
                return;
        }

        if (TWO_PHASE_INITIALTRANS != state)
        {
            twophase_index = g_twophase_state.connections[conn_index].conn_trans_state_index;
            if (PGXC_NODE_COORDINATOR == g_twophase_state.connections[conn_index].node_type)
            {
                g_twophase_state.coord_state[twophase_index].state = state;
                if (enable_distri_print)
                {
                    index = g_twophase_state.coord_state[twophase_index].handle_idx;
                    elog(LOG, "In UpdateLocalTwoPhaseState connections[%d] imply node: %s, g_twophase_state.coord_state[%d] imply node: %s", 
                            conn_index, 
                            response_handle->nodename, 
                            twophase_index, 
                            g_twophase_state.handles->coord_handles[index]->nodename);
                }
            }
            else
            {
                g_twophase_state.datanode_state[twophase_index].state = state;
                if (enable_distri_print)
                {
                    index = g_twophase_state.datanode_state[twophase_index].handle_idx;
                    elog(LOG, "In UpdateLocalTwoPhaseState connections[%d] imply node: %s, g_twophase_state.datanode_state[%d] imply node: %s", 
                            conn_index, 
                            response_handle->nodename, 
                            twophase_index, 
                            g_twophase_state.handles->datanode_handles[index]->nodename);
                }
            }
        }
        return;
    }
}


void ClearLocalTwoPhaseState(void)
{
    if (enable_distri_print)
    {
        if (TWO_PHASE_PREPARED == g_twophase_state.state && 
            IsXidImplicit(g_twophase_state.gid))
        {
            elog(LOG, "clear g_twophase_state of transaction '%s' in state '%s'", 
                g_twophase_state.gid, GetTransStateString(g_twophase_state.state));
        }
    }
    g_twophase_state.in_pg_clean = false;
    g_twophase_state.is_start_node = false;
    g_twophase_state.is_readonly = false;
    g_twophase_state.is_after_prepare = false;
    g_twophase_state.gid[0] = '\0';
    g_twophase_state.state = TWO_PHASE_INITIALTRANS;
    g_twophase_state.coord_index = 0;
    g_twophase_state.datanode_index = 0;
    g_twophase_state.isprinted = false;
    g_twophase_state.start_node_name[0] = '\0';
    g_twophase_state.handles = NULL;
    g_twophase_state.participants[0] = '\0';
    g_twophase_state.start_xid = 0;
    g_twophase_state.connections_num = 0;
    g_twophase_state.response_operation = OTHER_OPERATIONS;

}

char *GetTransStateString(TwoPhaseTransState state)
{// #lizard forgives
    switch (state)
    {
        case TWO_PHASE_INITIALTRANS:
            return "TWO_PHASE_INITIALTRANS";
        case TWO_PHASE_PREPARING:
            return "TWO_PHASE_PREPARING";
        case TWO_PHASE_PREPARED:
            return "TWO_PHASE_PREPARED";
        case TWO_PHASE_PREPARE_ERROR:
            return "TWO_PHASE_PREPARE_ERROR";
        case TWO_PHASE_COMMITTING:
            return "TWO_PHASE_COMMITTING";
        case TWO_PHASE_COMMIT_END:
            return "TWO_PHASE_COMMIT_END";
        case TWO_PHASE_COMMITTED:
            return "TWO_PHASE_COMMITTED";
        case TWO_PHASE_COMMIT_ERROR:
            return "TWO_PHASE_COMMIT_ERROR";
        case TWO_PHASE_ABORTTING:
            return "TWO_PHASE_ABORTTING";
        case TWO_PHASE_ABORT_END:
            return "TWO_PHASE_ABORT_END";
        case TWO_PHASE_ABORTTED:
            return "TWO_PHASE_ABORTTED";
        case TWO_PHASE_ABORT_ERROR:
            return "TWO_PHASE_ABORT_ERROR";
        case TWO_PHASE_UNKNOW_STATUS:
            return "TWO_PHASE_UNKNOW_STATUS";
        default:
            return NULL;
    }
    return NULL;
}

char *GetConnStateString(ConnState state)
{// #lizard forgives
    switch (state)
    {
        case TWO_PHASE_HEALTHY:
            return "TWO_PHASE_HEALTHY";
        case TWO_PHASE_SEND_GXID_ERROR:
            return "TWO_PHASE_SEND_GXID_ERROR";
        case TWO_PHASE_SEND_TIMESTAMP_ERROR:
            return "TWO_PHASE_SEND_TIMESTAMP_ERROR";
        case TWO_PHASE_SEND_STARTER_ERROR:
            return "TWO_PHASE_SEND_STARTER_ERROR";
        case TWO_PHASE_SEND_STARTXID_ERROR:
            return "TWO_PHASE_SEND_STARTXID_ERROR";
        case TWO_PHASE_SEND_PARTICIPANTS_ERROR:
            return "TWO_PHASE_SEND_PARTICIPANTS_ERROR";
        case TWO_PHASE_SEND_QUERY_ERROR:
            return "TWO_PHASE_SEND_QUERY_ERROR";
        default:
            return NULL;
    }
    return NULL;
}


void get_partnodes(PGXCNodeAllHandles * handles, StringInfo participants)
{// #lizard forgives
    int i;
    PGXCNodeHandle *conn;
    const char *gid;
    bool is_readonly = true;
    gid = GetPrepareGID();
    
    initStringInfo(participants);

    /* start node participate the twophase transaction */
    if (IS_PGXC_LOCAL_COORDINATOR && 
        (isXactWriteLocalNode() || !IsXidImplicit(gid)))
    {
        appendStringInfo(participants, "%s,", PGXCNodeName);
    }

    for (i = 0; i < handles->dn_conn_count; i++)
    {
        conn = handles->datanode_handles[i];
        if (conn->sock == NO_SOCKET)
        {
            elog(ERROR, "get_partnodes, remote node %s's connection handle is invalid, backend_pid: %d",
                 conn->nodename, conn->backend_pid);
        }
        else if (conn->transaction_status == 'T')
        {
            if (!conn->read_only)
            {
                is_readonly = false;
                appendStringInfo(participants, "%s,", conn->nodename);
            }
        }
        else if (conn->transaction_status == 'E')
        {
            elog(ERROR, "get_partnodes, remote node %s is in error state, backend_pid: %d",
                 conn->nodename, conn->backend_pid);
        }
    }
 
    for (i = 0; i < handles->co_conn_count; i++)
    {
        conn = handles->coord_handles[i];
        if (conn->sock == NO_SOCKET)
        {
            elog(ERROR, "get_partnodes, remote node %s's connection handle is invalid, backend_pid: %d",
                 conn->nodename, conn->backend_pid);
        }
        else if (conn->transaction_status == 'T')
        {
            if (!conn->read_only)
            {
                is_readonly = false;
                appendStringInfo(participants, "%s,", conn->nodename);
            }
        }
        else if (conn->transaction_status == 'E')
        {
            elog(ERROR, "get_partnodes, remote node %s is in error state, backend_pid: %d",
                 conn->nodename, conn->backend_pid);
        }
    }
    if (is_readonly && !IsXidImplicit(gid))
    {
        g_twophase_state.is_readonly = true;
    }
}
#endif
/*
 * Rollback transactions on remote nodes.
 * Release remote connection after completion.
 */
static void
pgxc_node_remote_abort(TranscationType txn_type, bool need_release_handle)
{// #lizard forgives
#ifdef __TBASE__
#define ROLLBACK_PREPARED_CMD_LEN 256
	bool                  force_release_handle = false;
#endif
    int                ret    = -1;
    int                result = 0;
    char           *rollbackCmd = NULL;
    int                i;
    ResponseCombiner combiner;
    PGXCNodeHandle **connections = NULL;
    int                conn_count = 0;

    PGXCNodeHandle **sync_connections = NULL;
    int                sync_conn_count = 0;
    PGXCNodeAllHandles *handles = NULL;
    bool            rollback_implict_txn = false;
#ifdef __TWO_PHASE_TRANS__
    int             twophase_index = 0;
#endif

#ifdef __TBASE__
    switch (txn_type)
    {
        case TXN_TYPE_RollbackTxn:
            if ('\0' != g_twophase_state.gid[0])//NULL != GetPrepareGID())
            {
                rollbackCmd = palloc0(ROLLBACK_PREPARED_CMD_LEN);
                snprintf(rollbackCmd, ROLLBACK_PREPARED_CMD_LEN, "rollback prepared '%s'", g_twophase_state.gid);//GetPrepareGID());
                rollback_implict_txn = true;
#ifdef __TWO_PHASE_TESTS__
                twophase_in = IN_REMOTE_ABORT;
#endif
            }
            else
            {
                rollbackCmd = "ROLLBACK TRANSACTION";
            }
            break;
        case TXN_TYPE_RollbackSubTxn:
            rollbackCmd = "ROLLBACK_SUBTXN";
            break;
        case TXN_TYPE_CleanConnection:
            return;
        default:
            elog(PANIC, "pgxc_node_remote_abort invalid TranscationType:%d", txn_type);
            break;
    }
#endif

    handles = get_current_handles();

    /* palloc will FATAL when out of memory .*/
    connections = (PGXCNodeHandle**)palloc(sizeof(PGXCNodeHandle*) * (TBASE_MAX_DATANODE_NUMBER + TBASE_MAX_COORDINATOR_NUMBER));
    sync_connections = (PGXCNodeHandle**)palloc(sizeof(PGXCNodeHandle*) * (TBASE_MAX_DATANODE_NUMBER + TBASE_MAX_COORDINATOR_NUMBER));
    

    SetSendCommandId(false);

    elog(DEBUG5, "pgxc_node_remote_abort - dn_conn_count %d, co_conn_count %d",
            handles->dn_conn_count, handles->co_conn_count);

    /* Send Sync if needed. */
    for (i = 0; i < handles->dn_conn_count; i++)
    {
        PGXCNodeHandle *conn = handles->datanode_handles[i];

        /* Skip empty slots */
        if (conn->sock == NO_SOCKET)
        {
            continue;
        }
        
        if (conn->transaction_status != 'I')
        {
            /* Read in any pending input */
            if (conn->state != DN_CONNECTION_STATE_IDLE)
            {
				BufferConnection(conn, false);
            }

            /*
             * If the remote session was running extended query protocol when
             * it failed, it will expect a SYNC message before it accepts any
             * other command
             */
            if (conn->needSync)
            {
                ret = pgxc_node_send_sync(conn);
                if (ret)
                {
                    add_error_message(conn,
                        "Failed to send SYNC command");
                    ereport(LOG,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("Failed to send SYNC command nodename:%s, pid:%d", conn->nodename, conn->backend_pid)));
                }
                else
                {
                    /* Read responses from these */
                    sync_connections[sync_conn_count++] = conn;
                    result = EOF;
					elog(DEBUG5, "send SYNC command to CN nodename %s, backend_pid %d", conn->nodename, conn->backend_pid);
                }
            }
        }
    }

    for (i = 0; i < handles->co_conn_count; i++)
    {
        PGXCNodeHandle *conn = handles->coord_handles[i];

        /* Skip empty slots */
        if (conn->sock == NO_SOCKET)
        {
            continue;
        }

        if (conn->transaction_status != 'I')
        {
            /* Send SYNC if the remote session is expecting one */
            if (conn->needSync)
            {
                ret = pgxc_node_send_sync(conn);
                if (ret)
                {
                    add_error_message(conn,
                        "Failed to send SYNC command nodename");
                    ereport(LOG,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("Failed to send SYNC command nodename:%s, pid:%d", conn->nodename, conn->backend_pid)));
                }
                else
                {
                    /* Read responses from these */
                    sync_connections[sync_conn_count++] = conn;
                    result = EOF;
					elog(DEBUG5, "send SYNC command to DN nodename %s, backend_pid %d", conn->nodename, conn->backend_pid);
                }
            }
        }
    }

    if (sync_conn_count)
    {
        InitResponseCombiner(&combiner, sync_conn_count, COMBINE_TYPE_NONE);
        /* Receive responses */
        result = pgxc_node_receive_responses(sync_conn_count, sync_connections, NULL, &combiner);
        if (result)
        {
            elog(LOG, "pgxc_node_remote_abort pgxc_node_receive_responses of SYNC failed");
            result = EOF;
        }
        else if (!validate_combiner(&combiner))
        {
            elog(LOG, "pgxc_node_remote_abort validate_combiner responese of SYNC failed");
            result = EOF;
        }

        if (result)
        {
            if (combiner.errorMessage)
            {
                ereport(LOG,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("Failed to send SYNC to on one or more nodes errmsg:%s", combiner.errorMessage)));
            }
            else
            {
                ereport(LOG,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("Failed to send SYNC to on one or more nodes")));
            }
        }
        CloseCombiner(&combiner);
    }        

#ifdef _PG_REGRESS_
    {
        int ii = 0;
        for (ii = 0; ii < sync_conn_count; ii++)
        {
            if (pgxc_node_is_data_enqueued(sync_connections[ii]))
            {
                elog(PANIC, "pgxc_node_remote_abort data left over in fd:%d, remote backendpid:%d",
                            sync_connections[ii]->sock, sync_connections[ii]->backend_pid);
            }
        }
    }
#endif

#ifdef __TWO_PHASE_TRANS__
    if (TWO_PHASE_ABORTTING == g_twophase_state.state && rollback_implict_txn)
    {
        SetLocalTwoPhaseStateHandles(handles);
    }
#endif    

    for (i = 0; i < handles->dn_conn_count; i++)
    {
        PGXCNodeHandle *conn = handles->datanode_handles[i];

        /* Skip empty slots */
        if (conn->sock == NO_SOCKET)
            continue;

        elog(DEBUG5, "node %s, conn->transaction_status %c",
                conn->nodename,
                conn->transaction_status);

        if ((conn->transaction_status != 'I' && TXN_TYPE_RollbackTxn == txn_type) ||
            (rollback_implict_txn && conn->ck_resp_rollback && TXN_TYPE_RollbackTxn == txn_type) || 
            (conn->transaction_status != 'I' && TXN_TYPE_RollbackSubTxn == txn_type && NodeHasBeginSubTxn(conn->nodeoid)))
        {
#ifdef __TWO_PHASE_TRANS__
            if (rollback_implict_txn)
            {
                twophase_index = g_twophase_state.datanode_index;
                g_twophase_state.datanode_state[twophase_index].handle_idx = i;
                g_twophase_state.datanode_state[twophase_index].is_participant = true;
                g_twophase_state.datanode_state[twophase_index].state = TWO_PHASE_ABORTTING;
            }
#endif            
            /*
             * Do not matter, is there committed or failed transaction,
             * just send down rollback to finish it.
             */
            if (pgxc_node_send_rollback(conn, rollbackCmd))
            {
#ifdef __TWO_PHASE_TRANS__
                if (rollback_implict_txn)
                {
                    twophase_index = g_twophase_state.datanode_index;
                    g_twophase_state.datanode_state[twophase_index].state = TWO_PHASE_ABORT_ERROR;
                    g_twophase_state.datanode_state[twophase_index].conn_state = TWO_PHASE_SEND_QUERY_ERROR;
                    g_twophase_state.datanode_index++;
                }
#endif                
                result = EOF;
                add_error_message(conn,
                        "failed to send ROLLBACK TRANSACTION command");

                ereport(LOG,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("Failed to send ROLLBACK TRANSACTION command nodename:%s, pid:%d", conn->nodename, conn->backend_pid)));                

            }
            else
            {
#ifdef __TWO_PHASE_TRANS__
                if (rollback_implict_txn)
                {
                    twophase_index = g_twophase_state.connections_num;
                    g_twophase_state.connections[twophase_index].conn_trans_state_index = 
                        g_twophase_state.datanode_index;
                    g_twophase_state.connections[twophase_index].node_type = 
                        PGXC_NODE_DATANODE;
                    twophase_index = g_twophase_state.datanode_index;
                    g_twophase_state.datanode_state[twophase_index].conn_state = 
                        TWO_PHASE_HEALTHY;
                    g_twophase_state.datanode_index++;
                    g_twophase_state.connections_num++;
                }
#endif
                /* Read responses from these */
                connections[conn_count++] = conn;
                if (conn->ck_resp_rollback)
                {
                    conn->ck_resp_rollback = false;
                }
            }
        }
    }

    for (i = 0; i < handles->co_conn_count; i++)
    {
        PGXCNodeHandle *conn = handles->coord_handles[i];

        /* Skip empty slots */
        if (conn->sock == NO_SOCKET)
            continue;        

        if ((conn->transaction_status != 'I' && TXN_TYPE_RollbackTxn == txn_type) ||
            (rollback_implict_txn && conn->ck_resp_rollback && TXN_TYPE_RollbackTxn == txn_type) || 
            (conn->transaction_status != 'I' && TXN_TYPE_RollbackSubTxn == txn_type && NodeHasBeginSubTxn(conn->nodeoid)))
        {            
#ifdef __TWO_PHASE_TRANS__
            if (rollback_implict_txn)
            {
                twophase_index = g_twophase_state.coord_index;
                g_twophase_state.coord_state[twophase_index].handle_idx = i;
                g_twophase_state.coord_state[twophase_index].is_participant = true;
                g_twophase_state.coord_state[twophase_index].state = 
                    TWO_PHASE_ABORTTING;
            }
#endif            
            /*
             * Do not matter, is there committed or failed transaction,
             * just send down rollback to finish it.
             */
            if (pgxc_node_send_rollback(conn, rollbackCmd))
            {
#ifdef __TWO_PHASE_TRANS__
                if (rollback_implict_txn)
                {
                    twophase_index = g_twophase_state.coord_index;
                    g_twophase_state.coord_state[twophase_index].state = 
                        TWO_PHASE_ABORT_ERROR;
                    g_twophase_state.coord_state[twophase_index].conn_state = 
                        TWO_PHASE_SEND_QUERY_ERROR;
                    g_twophase_state.coord_index++;
                }
#endif                
                result = EOF;
                add_error_message(conn,
                        "failed to send ROLLBACK TRANSACTION command");
                ereport(LOG,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("Failed to send ROLLBACK TRANSACTION command nodename:%s, pid:%d", conn->nodename, conn->backend_pid)));                
            }
            else
            {
#ifdef __TWO_PHASE_TRANS__
                if (rollback_implict_txn)
                {
                    twophase_index = g_twophase_state.connections_num;
                    g_twophase_state.connections[twophase_index].conn_trans_state_index = 
                        g_twophase_state.coord_index;
                    g_twophase_state.connections[twophase_index].node_type = 
                        PGXC_NODE_COORDINATOR;
                    g_twophase_state.coord_state[twophase_index].conn_state = TWO_PHASE_HEALTHY;
                    g_twophase_state.coord_index++;
                    g_twophase_state.connections_num++;
                }
#endif
                /* Read responses from these */
                connections[conn_count++] = conn;
                if (conn->ck_resp_rollback)
                {
                    conn->ck_resp_rollback = false;
                }
            }
        }
    }

    if (conn_count)
    {
        InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
#ifdef __TWO_PHASE_TRANS__
        if (rollback_implict_txn)
        {
            g_twophase_state.response_operation = REMOTE_ABORT;
        }
#endif
        /* Receive responses */
        result = pgxc_node_receive_responses(conn_count, connections, NULL, &combiner);
        if (result)
        {
            elog(LOG, "pgxc_node_remote_abort pgxc_node_receive_responses of ROLLBACK failed");
            result = EOF;
        }
        else if (!validate_combiner(&combiner))
        {
#ifdef _PG_REGRESS_
            elog(LOG, "pgxc_node_remote_abort validate_combiner responese of ROLLBACK failed");
#else
            elog(LOG, "pgxc_node_remote_abort validate_combiner responese of ROLLBACK failed");
#endif
            result = EOF;
        }    

        if (result)        
        {
            if (combiner.errorMessage)
            {
                ereport(LOG,
                        (errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send ROLLBACK to on one or more nodes errmsg:%s", combiner.errorMessage)));
            }
            else
            {
                ereport(LOG,
                        (errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send ROLLBACK to on one or more nodes")));
            }
        }
        CloseCombiner(&combiner);
    }

    stat_transaction(conn_count);
    
#ifdef __TBASE__
    force_release_handle = validate_handles();
    if (force_release_handle)
    {
        elog(LOG, "found bad remote node connections, force release handles now");
        release_handles(true);
    }
#endif    
    
	/* do not cleanup remote session for subtrans */
	if (!temp_object_included && need_release_handle)
        {
            /* Clean up remote sessions */
		pgxc_node_remote_cleanup_all();
			release_handles(false);
        }

    clear_handles();
    pfree_pgxc_all_handles(handles);

    if (connections)
    {
        pfree(connections);
        connections = NULL;
    }

    if (sync_connections)
    {
        pfree(sync_connections);
        sync_connections = NULL;
    }
}

/*
 * Begin COPY command
 * The copy_connections array must have room for NumDataNodes items
 */
void
DataNodeCopyBegin(RemoteCopyData *rcstate)
{// #lizard forgives
    int i;
    List *nodelist = rcstate->rel_loc->rl_nodeList;
    PGXCNodeHandle **connections;
    bool need_tran_block;
    GlobalTransactionId gxid;
    ResponseCombiner combiner;
    Snapshot snapshot = GetActiveSnapshot();
    int conn_count = list_length(nodelist);
    CommandId cid = GetCurrentCommandId(true);

    /* Get needed datanode connections */
    if (!rcstate->is_from && IsLocatorReplicated(rcstate->rel_loc->locatorType))
    {
        /* Connections is a single handle to read from */
        connections = (PGXCNodeHandle **) palloc(sizeof(PGXCNodeHandle *));
        connections[0] = get_any_handle(nodelist);
        conn_count = 1;
    }
    else
    {
        PGXCNodeAllHandles *pgxc_handles;
		pgxc_handles = get_handles(nodelist, NULL, false, true, true);
        connections = pgxc_handles->datanode_handles;
        Assert(pgxc_handles->dn_conn_count == conn_count);
        pfree(pgxc_handles);
    }

    /*
     * If more than one nodes are involved or if we are already in a
     * transaction block, we must the remote statements in a transaction block
     */
    need_tran_block = (conn_count > 1) || (TransactionBlockStatusCode() == 'T');

    elog(DEBUG1, "conn_count = %d, need_tran_block = %s", conn_count,
            need_tran_block ? "true" : "false");

    /* Gather statistics */
    stat_statement();
    stat_transaction(conn_count);

    gxid = GetCurrentTransactionId();

    /* Start transaction on connections where it is not started */

	if (pgxc_node_begin(conn_count, connections, gxid, need_tran_block, false))
    {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("Could not begin transaction on data nodes.")));
    }

    /*
     * COPY TO do not use locator, it just takes connections from it, and
     * we do not look up distribution data type in this case.
     * So always use LOCATOR_TYPE_RROBIN to avoid errors because of not
     * defined partType if real locator type is HASH or MODULO.
     * Create locator before sending down query, because createLocator may
     * fail and we leave with dirty connections.
     * If we get an error now datanode connection will be clean and error
     * handler will issue transaction abort.
     */
#ifdef _MIGRATE_
    rcstate->locator = createLocator(
                rcstate->is_from ? rcstate->rel_loc->locatorType
                        : LOCATOR_TYPE_RROBIN,
                rcstate->is_from ? RELATION_ACCESS_INSERT : RELATION_ACCESS_READ,
                rcstate->dist_type,
                LOCATOR_LIST_POINTER,
                conn_count,
                (void *) connections,
                NULL,
                false,
                rcstate->rel_loc->groupId, rcstate->rel_loc->coldGroupId,
                rcstate->sec_dist_type, rcstate->rel_loc->secAttrNum,
                rcstate->rel_loc->relid);
#else
    rcstate->locator = createLocator(
                rcstate->is_from ? rcstate->rel_loc->locatorType
                        : LOCATOR_TYPE_RROBIN,
                rcstate->is_from ? RELATION_ACCESS_INSERT : RELATION_ACCESS_READ,
                rcstate->dist_type,
                LOCATOR_LIST_POINTER,
                conn_count,
                (void *) connections,
                NULL,
                false);
#endif

    /* Send query to nodes */
    for (i = 0; i < conn_count; i++)
    {
        CHECK_OWNERSHIP(connections[i], NULL);

        if (snapshot && pgxc_node_send_snapshot(connections[i], snapshot))
        {
            add_error_message(connections[i], "Can not send request");
            pfree(connections);
            freeLocator(rcstate->locator);
            rcstate->locator = NULL;
            return;
        }
#ifdef __TBASE__
        if (pgxc_node_send_cmd_id(connections[i], cid) < 0)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Failed to send command ID to Datanodes")));
        }
#endif
        if (pgxc_node_send_query(connections[i], rcstate->query_buf.data) != 0)
        {
            add_error_message(connections[i], "Can not send request");
            pfree(connections);
            freeLocator(rcstate->locator);
            rcstate->locator = NULL;
            return;
        }
    }

    /*
     * We are expecting CopyIn response, but do not want to send it to client,
     * caller should take care about this, because here we do not know if
     * client runs console or file copy
     */
    InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
    /*
     * Make sure there are zeroes in unused fields
     */
    memset(&combiner, 0, sizeof(ScanState));

    /* Receive responses */
    if (pgxc_node_receive_copy_begin(conn_count, connections, NULL, &combiner)
            || !ValidateAndCloseCombiner(&combiner))
    {
        DataNodeCopyFinish(conn_count, connections);
        freeLocator(rcstate->locator);
        rcstate->locator = NULL;
        return;
    }
    pfree(connections);
}


/*
 * Send a data row to the specified nodes
 */
int
DataNodeCopyIn(char *data_row, int len,
        int conn_count, PGXCNodeHandle** copy_connections,
        bool binary)
{// #lizard forgives
    /* size + data row + \n in CSV mode */
    int msgLen = 4 + len + (binary ? 0 : 1);
    int nLen = htonl(msgLen);
    int i;

    for(i = 0; i < conn_count; i++)
    {
        PGXCNodeHandle *handle = copy_connections[i];
        if (handle->state == DN_CONNECTION_STATE_COPY_IN)
        {
            /* precalculate to speed up access */
            int bytes_needed = handle->outEnd + 1 + msgLen;

            /* flush buffer if it is almost full */
            if (bytes_needed > COPY_BUFFER_SIZE)
            {
                int to_send = handle->outEnd;

                /* First look if data node has sent a error message */
                int read_status = pgxc_node_read_data(handle, true);
                if (read_status == EOF || read_status < 0)
                {
                    add_error_message(handle, "failed to read data from data node");
                    return EOF;
                }

                if (handle->inStart < handle->inEnd)
                {
                    ResponseCombiner combiner;
                    InitResponseCombiner(&combiner, 1, COMBINE_TYPE_NONE);
                    /*
                     * Make sure there are zeroes in unused fields
                     */
                    memset(&combiner, 0, sizeof(ScanState));

                    /*
                     * Validate the combiner but only if we see a proper
                     * resposne for our COPY message. The problem is that
                     * sometimes we might receive async messages such as
                     * 'M' which is used to send back command ID generated and
                     * consumed by the datanode. While the message gets handled
                     * in handle_response(), we don't want to declare receipt
                     * of an invalid message below.
                     *
                     * If there is an actual error of some sort then the
                     * connection state is will be set appropriately and we
                     * shall catch that subsequently.
                     */
                    if (handle_response(handle, &combiner) == RESPONSE_COPY &&
                        !ValidateAndCloseCombiner(&combiner))
                        return EOF;
                }

                if (DN_CONNECTION_STATE_ERROR(handle))
                    return EOF;

                /*
                 * Try to send down buffered data if we have
                 */
                if (to_send && send_some(handle, to_send) < 0)
                {
                    add_error_message(handle, "failed to send data to data node");
                    return EOF;
                }
            }

            if (ensure_out_buffer_capacity(bytes_needed, handle) != 0)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_OUT_OF_MEMORY),
                         errmsg("out of memory")));
            }

            handle->outBuffer[handle->outEnd++] = 'd';
            memcpy(handle->outBuffer + handle->outEnd, &nLen, 4);
            handle->outEnd += 4;
            memcpy(handle->outBuffer + handle->outEnd, data_row, len);
            handle->outEnd += len;
            if (!binary)
                handle->outBuffer[handle->outEnd++] = '\n';

            handle->in_extended_query = false;
        }
        else
        {
            add_error_message(handle, "Invalid data node connection");
            return EOF;
        }
    }
    return 0;
}

uint64
DataNodeCopyOut(PGXCNodeHandle** copy_connections,
                              int conn_count, FILE* copy_file)
{
    ResponseCombiner combiner;
    uint64        processed;
    bool         error;

    InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_SUM);
    /*
     * Make sure there are zeroes in unused fields
     */
    memset(&combiner, 0, sizeof(ScanState));
    combiner.processed = 0;
    /* If there is an existing file where to copy data, pass it to combiner */
    if (copy_file)
    {
        combiner.copy_file = copy_file;
        combiner.remoteCopyType = REMOTE_COPY_FILE;
    }
    else
    {
        combiner.copy_file = NULL;
        combiner.remoteCopyType = REMOTE_COPY_STDOUT;
    }
    error = (pgxc_node_receive_responses(conn_count, copy_connections, NULL, &combiner) != 0);

    processed = combiner.processed;

    if (!ValidateAndCloseCombiner(&combiner) || error)
    {
        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                 errmsg("Unexpected response from the data nodes when combining, request type %d", combiner.request_type)));
    }

    return processed;
}


uint64
DataNodeCopyStore(PGXCNodeHandle** copy_connections,
                                int conn_count, Tuplestorestate* store)
{
    ResponseCombiner combiner;
    uint64        processed;
    bool         error;

    InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_SUM);
    /*
     * Make sure there are zeroes in unused fields
     */
    memset(&combiner, 0, sizeof(ScanState));
    combiner.processed = 0;
    combiner.remoteCopyType = REMOTE_COPY_TUPLESTORE;
    combiner.tuplestorestate = store;

    error = (pgxc_node_receive_responses(conn_count, copy_connections, NULL, &combiner) != 0);

    processed = combiner.processed;

    if (!ValidateAndCloseCombiner(&combiner) || error)
    {
        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                 errmsg("Unexpected response from the data nodes when combining, request type %d", combiner.request_type)));
    }

    return processed;
}


/*
 * Finish copy process on all connections
 */
void
DataNodeCopyFinish(int conn_count, PGXCNodeHandle** connections)
{// #lizard forgives
    int        i;
    ResponseCombiner combiner;
    bool         error = false;
    for (i = 0; i < conn_count; i++)
    {
        PGXCNodeHandle *handle = connections[i];

        error = true;
        if (handle->state == DN_CONNECTION_STATE_COPY_IN || handle->state == DN_CONNECTION_STATE_COPY_OUT)
            error = DataNodeCopyEnd(handle, false);
    }
    
    /*
     * Make sure there are zeroes in unused fields
     */
    memset(&combiner, 0, sizeof(ScanState));
    InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
    error = (pgxc_node_receive_responses(conn_count, connections, NULL, &combiner) != 0) || error;    
    if (!validate_combiner(&combiner) || error)
    {
        if (combiner.errorMessage)
            pgxc_node_report_error(&combiner);
        else
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Error while running COPY")));
    }
    else
        CloseCombiner(&combiner);
}

/*
 * End copy process on a connection
 */
bool
DataNodeCopyEnd(PGXCNodeHandle *handle, bool is_error)
{
    int         nLen = htonl(4);

    if (handle == NULL)
        return true;

    /* msgType + msgLen */
    if (ensure_out_buffer_capacity(handle->outEnd + 1 + 4, handle) != 0)
        return true;

    if (is_error)
        handle->outBuffer[handle->outEnd++] = 'f';
    else
        handle->outBuffer[handle->outEnd++] = 'c';

    memcpy(handle->outBuffer + handle->outEnd, &nLen, 4);
    handle->outEnd += 4;

    handle->in_extended_query = false;
    /* We need response right away, so send immediately */
    if (pgxc_node_flush(handle) < 0)
        return true;

    return false;
}

#ifdef __TBASE__
/*
 * Get Node connections of all Datanodes
 */
PGXCNodeAllHandles *
get_exec_connections_all_dn(bool is_global_session)
{
	PGXCNodeAllHandles *pgxc_handles_connections = get_exec_connections(NULL, NULL, EXEC_ON_DATANODES, is_global_session);
	return pgxc_handles_connections;
}

#endif

/*
 * Get Node connections depending on the connection type:
 * Datanodes Only, Coordinators only or both types
 * If exec_nodes is NIL and exec_type is EXEC_ON_ALL_NODES
 * connect to all nodes except myself
 */
static PGXCNodeAllHandles *
get_exec_connections(RemoteQueryState *planstate,
                     ExecNodes *exec_nodes,
                     RemoteQueryExecType exec_type,
                     bool is_global_session)
{// #lizard forgives
    List        *nodelist = NIL;
#ifdef __TBASE__
    List        *temp_list = NIL;
#endif
    List        *primarynode = NIL;
    List       *coordlist = NIL;
    PGXCNodeHandle *primaryconnection;
    int            co_conn_count, dn_conn_count;
    bool        is_query_coord_only = false;
    PGXCNodeAllHandles *pgxc_handles = NULL;
	bool        missing_ok = (exec_nodes ? exec_nodes->missing_ok : false);

#ifdef __TBASE__
    if (IsParallelWorker())
    {
        if (EXEC_ON_CURRENT != exec_type)
        {
            if (NULL == exec_nodes)
            {
                elog(PANIC, "In parallel worker, exec_nodes can't be NULL!");
            }
            
            if (exec_nodes->accesstype != RELATION_ACCESS_READ && exec_nodes->accesstype != RELATION_ACCESS_READ_FQS)
            {
                elog(PANIC, "Only read access can run in parallel worker!");
            }

            if (exec_type != EXEC_ON_DATANODES)
            {
                elog(PANIC, "Parallel worker can only run query on datanodes!");
            }
        }
    }
#endif

    /*
     * If query is launched only on Coordinators, we have to inform get_handles
     * not to ask for Datanode connections even if list of Datanodes is NIL.
     */
    if (exec_type == EXEC_ON_COORDS)
        is_query_coord_only = true;

    if (exec_type == EXEC_ON_CURRENT)
        return get_current_handles();

    if (exec_nodes)
    {
        if (exec_nodes->en_expr)
        {
            /* execution time determining of target Datanodes */
            bool isnull;
            ExecNodes *nodes;
			Datum partvalue;
			ExprState *estate;
#ifdef __COLD_HOT__
            bool secisnull;
            Datum secValue;
#endif
			RelationLocInfo *rel_loc_info;
			if (exec_nodes->rewrite_done)
			{
				partvalue = exec_nodes->rewrite_value;
				isnull = exec_nodes->isnull;
			}
			else
			{
				estate = ExecInitExpr(exec_nodes->en_expr,
                                             (PlanState *) planstate);
			    /* For explain, no need to execute expr. */
			    if (planstate->eflags != EXEC_FLAG_EXPLAIN_ONLY)
			    	partvalue = ExecEvalExpr(estate,
                                           planstate->combiner.ss.ps.ps_ExprContext,
                                           &isnull);
			}
			
			rel_loc_info = GetRelationLocInfo(exec_nodes->en_relid);

#ifdef __COLD_HOT__
            if (exec_nodes->sec_en_expr)
            {
                estate = ExecInitExpr(exec_nodes->sec_en_expr,
                                             (PlanState *) planstate);
				/* For explain, no need to execute expr. */
				if (planstate->eflags != EXEC_FLAG_EXPLAIN_ONLY)
                secValue = ExecEvalExpr(estate,
                                        planstate->combiner.ss.ps.ps_ExprContext,
                                        &secisnull);
            }
            else
            {
                secisnull = true;
                secValue = 0;
            }
#endif

			if (planstate->eflags == EXEC_FLAG_EXPLAIN_ONLY)
				nodes = GetRelationNodesForExplain(rel_loc_info,
												   exec_nodes->accesstype);
			else
            /* PGXCTODO what is the type of partvalue here */
            nodes = GetRelationNodes(rel_loc_info,
                                     partvalue,
                                     isnull,
#ifdef __COLD_HOT__
                                     secValue,
                                     secisnull,
#endif
                                     exec_nodes->accesstype);
            /*
             * en_expr is set by pgxc_set_en_expr only for distributed
             * relations while planning DMLs, hence a select for update
             * on a replicated table here is an assertion
             */
            Assert(!(exec_nodes->accesstype == RELATION_ACCESS_READ_FOR_UPDATE &&
                        IsRelationReplicated(rel_loc_info)));

            if (nodes)
            {
                nodelist = nodes->nodeList;
                primarynode = nodes->primarynodelist;
                pfree(nodes);
            }
            FreeRelationLocInfo(rel_loc_info);
        }
        else if (OidIsValid(exec_nodes->en_relid))
        {
            RelationLocInfo *rel_loc_info = GetRelationLocInfo(exec_nodes->en_relid);
#ifdef __COLD_HOT__
            ExecNodes *nodes = GetRelationNodes(rel_loc_info, 0, true, 0, true, exec_nodes->accesstype);
#else
            ExecNodes *nodes = GetRelationNodes(rel_loc_info, 0, true, exec_nodes->accesstype);
#endif
            /*
             * en_relid is set only for DMLs, hence a select for update on a
             * replicated table here is an assertion
             */
            Assert(!(exec_nodes->accesstype == RELATION_ACCESS_READ_FOR_UPDATE &&
                        IsRelationReplicated(rel_loc_info)));

            /* Use the obtained list for given table */
            if (nodes)
                nodelist = nodes->nodeList;

            /*
             * Special handling for ROUND ROBIN distributed tables. The target
             * node must be determined at the execution time
             */
            if (rel_loc_info->locatorType == LOCATOR_TYPE_RROBIN && nodes)
            {
                nodelist = nodes->nodeList;
                primarynode = nodes->primarynodelist;
            }
            else if (nodes)
            {
                if (exec_type == EXEC_ON_DATANODES || exec_type == EXEC_ON_ALL_NODES)
                {
                    nodelist = exec_nodes->nodeList;
                    primarynode = exec_nodes->primarynodelist;
                }
            }

            if (nodes)
                pfree(nodes);
            FreeRelationLocInfo(rel_loc_info);
        }
        else
        {
            if (exec_type == EXEC_ON_DATANODES || exec_type == EXEC_ON_ALL_NODES)
                nodelist = exec_nodes->nodeList;
            else if (exec_type == EXEC_ON_COORDS)
                coordlist = exec_nodes->nodeList;

            primarynode = exec_nodes->primarynodelist;
        }
    }

    /* Set node list and DN number */
    if (list_length(nodelist) == 0 &&
        (exec_type == EXEC_ON_ALL_NODES ||
         exec_type == EXEC_ON_DATANODES))
    {
        /* Primary connection is included in this number of connections if it exists */
        dn_conn_count = NumDataNodes;
    }
    else
    {
        if (exec_type == EXEC_ON_DATANODES || exec_type == EXEC_ON_ALL_NODES)
        {
            if (primarynode)
                dn_conn_count = list_length(nodelist) + 1;
            else
                dn_conn_count = list_length(nodelist);
        }
        else
            dn_conn_count = 0;
    }

    /* Set Coordinator list and Coordinator number */
    if ((list_length(nodelist) == 0 && exec_type == EXEC_ON_ALL_NODES) ||
        (list_length(coordlist) == 0 && exec_type == EXEC_ON_COORDS))
    {
        coordlist = GetAllCoordNodes();
        co_conn_count = list_length(coordlist);
    }
    else
    {
        if (exec_type == EXEC_ON_COORDS)
            co_conn_count = list_length(coordlist);
        else
            co_conn_count = 0;
    }
    
	if ((list_length(nodelist) == 0 && exec_type == EXEC_ON_ALL_NODES))
	{
		nodelist = GetAllDataNodes();
		dn_conn_count = NumDataNodes;
	}
	
#ifdef __TBASE__
    if (IsParallelWorker())
    {
        int32   i          = 0;
        int32   worker_num = 0;
        int32    length       = 0;    
        int32   step        = 0;
        int32   begin_node = 0;
        int32   end_node   = 0;
        ParallelWorkerStatus *parallel_status   = NULL;
        ListCell             *node_list_item    = NULL;
        parallel_status = planstate->parallel_status;
        temp_list       = nodelist;
        nodelist        = NULL;

        worker_num = ExecGetForWorkerNumber(parallel_status);
        length = list_length(temp_list);

        if(worker_num)
            step = DIVIDE_UP(length, worker_num);
        /* Last worker. */
        if (ParallelWorkerNumber == (worker_num - 1))
        {
            begin_node = ParallelWorkerNumber * step;
            end_node   = length;
        }
        else
        {
            begin_node = ParallelWorkerNumber * step;
            end_node   = begin_node + step;
        }

        /* Form the execNodes of our own node. */
        i = 0;
        foreach(node_list_item, temp_list)
        {
            int    node = lfirst_int(node_list_item);

            if (i >= begin_node && i < end_node)
            {
                nodelist = lappend_int(nodelist, node);
            }

            if (i >= end_node)
            {
                break;
            }
        }
        list_free(temp_list);        
        exec_nodes->nodeList = nodelist;
        dn_conn_count = list_length(nodelist);
        if (list_length(coordlist) != 0)
        {
            elog(PANIC, "Parallel worker can not run query on coordinator!");
        }
    }        
#endif

    /* Get other connections (non-primary) */
	pgxc_handles = get_handles(nodelist, coordlist, is_query_coord_only, is_global_session, !missing_ok);
    if (!pgxc_handles)
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("Could not obtain connection from pool")));

    /* Get connection for primary node, if used */
    if (primarynode)
    {
        /* Let's assume primary connection is always a Datanode connection for the moment */
        PGXCNodeAllHandles *pgxc_conn_res;
		pgxc_conn_res = get_handles(primarynode, NULL, false, is_global_session, true);

        /* primary connection is unique */
        primaryconnection = pgxc_conn_res->datanode_handles[0];

        pfree(pgxc_conn_res);

        if (!primaryconnection)
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Could not obtain connection from pool")));
        pgxc_handles->primary_handle = primaryconnection;
    }

	if (missing_ok)
	{
		/* compact handle list exclude missing nodes */
		int i = 0;
		while (dn_conn_count && i < dn_conn_count)
		{
			if (DN_CONNECTION_STATE_ERROR(pgxc_handles->datanode_handles[i]))
			{
				/* find last healthy handle */
				while (dn_conn_count - 1 > i &&
				       DN_CONNECTION_STATE_ERROR(pgxc_handles->datanode_handles[dn_conn_count - 1]))
					dn_conn_count--;
				
				/* replace bad handle with last healthy handle */
				pgxc_handles->datanode_handles[i] =
					pgxc_handles->datanode_handles[dn_conn_count - 1];
				/* exclude bad handle */
				pgxc_handles->datanode_handles[dn_conn_count - 1] = NULL;
				dn_conn_count--;
			}
			i++;
		}
		
		i = 0;
		while (co_conn_count && i < co_conn_count)
		{
			if (DN_CONNECTION_STATE_ERROR(pgxc_handles->coord_handles[i]))
			{
				/* find last healthy handle */
				while (co_conn_count - 1 > i &&
				       DN_CONNECTION_STATE_ERROR(pgxc_handles->coord_handles[co_conn_count - 1]))
					co_conn_count--;
				
				/* replace bad handle with last healthy handle */
				pgxc_handles->coord_handles[i] =
					pgxc_handles->coord_handles[co_conn_count - 1];
				/* exclude bad handle */
				pgxc_handles->coord_handles[co_conn_count - 1] = NULL;
				co_conn_count--;
			}
			i++;
		}
	}
	
    /* Depending on the execution type, we still need to save the initial node counts */
    pgxc_handles->dn_conn_count = dn_conn_count;
    pgxc_handles->co_conn_count = co_conn_count;

    return pgxc_handles;
}


static bool
pgxc_start_command_on_connection(PGXCNodeHandle *connection,
                                    RemoteQueryState *remotestate,
                                    Snapshot snapshot)
{// #lizard forgives
    CommandId    cid;
    ResponseCombiner *combiner = (ResponseCombiner *) remotestate;
    RemoteQuery    *step = (RemoteQuery *) combiner->ss.ps.plan;
    CHECK_OWNERSHIP(connection, combiner);

    elog(DEBUG5, "pgxc_start_command_on_connection - node %s, state %d",
            connection->nodename, connection->state);

    /*
     * Scan descriptor would be valid and would contain a valid snapshot
     * in cases when we need to send out of order command id to data node
     * e.g. in case of a fetch
     */
#ifdef __TBASE__
    if (snapshot)
    {
        cid = snapshot->curcid;

        if (cid == InvalidCommandId)
        {
            elog(LOG, "commandId in snapshot is invalid.");
            cid = GetCurrentCommandId(false);
        }
    }
    else
    {
        cid = GetCurrentCommandId(false);
    }
#else
    cid = GetCurrentCommandId(false);
#endif

    if (pgxc_node_send_cmd_id(connection, cid) < 0 )
        return false;

    if (snapshot && pgxc_node_send_snapshot(connection, snapshot))
        return false;
	if ((step->statement && step->statement[0] != '\0') ||
		step->cursor ||
		remotestate->rqs_num_params)
    {
        /* need to use Extended Query Protocol */
        int    fetch = 0;
        bool    prepared = false;
        char    nodetype = PGXC_NODE_DATANODE;
		ExecNodes *exec_nodes = step->exec_nodes;

        /* if prepared statement is referenced see if it is already
         * exist */
		if (exec_nodes && exec_nodes->need_rewrite == true)
			prepared = false;
		if (step->statement)
            prepared =
                ActivateDatanodeStatementOnNode(step->statement,
                        PGXCNodeGetNodeId(connection->nodeoid,
                            &nodetype));
		if (prepared && exec_nodes && exec_nodes->need_rewrite == true)
			prepared = false;

        /*
         * execute and fetch rows only if they will be consumed
         * immediately by the sorter
         */
        if (step->cursor)
#ifdef __TBASE__
        {
            /* we need all rows one time */
            if (step->dml_on_coordinator)
                fetch = 0;
            else
                fetch = 1;
        }
#else
            fetch = 1;
#endif
        combiner->extended_query = true;

        if (pgxc_node_send_query_extended(connection,
                            prepared ? NULL : step->sql_statement,
                            step->statement,
                            step->cursor,
                            remotestate->rqs_num_params,
                            remotestate->rqs_param_types,
                            remotestate->paramval_len,
                            remotestate->paramval_data,
                            step->has_row_marks ? true : step->read_only,
                            fetch) != 0)
            return false;
    }
    else
    {
        combiner->extended_query = false;
        if (pgxc_node_send_query(connection, step->sql_statement) != 0)
            return false;
    }
    return true;
}

/*
 * Get snapshot and gxid for remote utility.
 */
void
GetGlobInfoForRemoteUtility(RemoteQuery *node, GlobalTransactionId *gxid,
							Snapshot *snapshot)
{
	bool                utility_need_transcation = true;

#ifdef __TBASE__
	/* Some DDL such as ROLLBACK, SET does not need transaction */
	utility_need_transcation =
			(!ExecDDLWithoutAcquireXid(node->parsetree) && !node->is_set);

	if (utility_need_transcation)
#endif		
	{
		elog(LOG, "[SAVEPOINT] node->sql_statement:%s", node->sql_statement);
		*gxid = GetCurrentTransactionId();
	}
	
	if (ActiveSnapshotSet())
		*snapshot = GetActiveSnapshot();

#ifdef __TBASE__	
	if (utility_need_transcation)
#endif
	{
		if (!GlobalTransactionIdIsValid(*gxid))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to get next transaction ID")));
	}

#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
	if(!IS_PGXC_LOCAL_COORDINATOR)
	{
		/* 
		 * Distributed DDLs only dispatch from the requested coordinator, thus
		 * we skip sending gxid to avoid cycling.
		 *
		 * Note: except for 'set_config_option'.
		 */
		*gxid = InvalidTransactionId;
	}

#endif
}

/*
 * Send snapshot/cmdid/query to remote node.
 */
void
SendTxnInfo(RemoteQuery *node, PGXCNodeHandle *conn,
			CommandId cid, Snapshot snapshot)
{
	if (conn->state == DN_CONNECTION_STATE_QUERY)
		BufferConnection(conn, false);
	if (snapshot && pgxc_node_send_snapshot(conn, snapshot))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("Failed to send snapshot to %s", conn->nodename)));
	}
	if (pgxc_node_send_cmd_id(conn, cid) < 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("Failed to send command ID to %s", conn->nodename)));
	}

	if (pgxc_node_send_query(conn, node->sql_statement) != 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("Failed to send command to %s", conn->nodename)));
	}
}

/*
 * Check response of remote connection.
 */
bool
CheckRemoteRespond(PGXCNodeHandle *conn, ResponseCombiner *combiner,
					int *index, int *conn_count)
{
	int res = handle_response(conn, combiner);
	if (res == RESPONSE_EOF)
	{
		(*index)++;
	}
	else if (res == RESPONSE_COMPLETE)
	{
		/* Ignore, wait for ReadyForQuery */
		if (conn->state == DN_CONNECTION_STATE_ERROR_FATAL)
		{
			ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("Unexpected FATAL ERROR on Connection to "
							"Datanode %s pid %d",
						conn->nodename, conn->backend_pid)));
		}
	}
	else if (res == RESPONSE_ERROR)
	{
		/* Ignore, wait for ReadyForQuery */
	}
	else if (res == RESPONSE_READY)
	{
		if ((*index) < --(*conn_count))
			return true;
	}
	else if (res == RESPONSE_TUPDESC)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("Unexpected response from %s pid %d",
						conn->nodename, conn->backend_pid)));
	}
	else if (res == RESPONSE_DATAROW)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("Unexpected response from %s pid %d",
						conn->nodename, conn->backend_pid)));
	}
	return false;
}

/*
 * Receive remote response and chek receive status.
 */
void RemoteReceiveAndCheck(int conn_count, PGXCNodeHandle **conns,
							ResponseCombiner *combiner)
{
	/*
     * Stop if all commands are completed or we got a data row and
     * initialized state node for subsequent invocations
     */
    while (conn_count > 0)
    {
        int		i = 0;
		bool	remote_ready = false;

        /* Wait until one of the connections has data available */
        if (pgxc_node_receive(conn_count,
                              conns,
                              NULL))
        {
            /*
             * Got error
             * TODO(Tbase): How do we check the error here?
             */
            break;
        }

        while (i < conn_count)
        {
            PGXCNodeHandle *conn = NULL;
			if (remote_ready)
			{
				conns[i] = conns[conn_count];
			}
			conn = conns[i];
            remote_ready = CheckRemoteRespond(conn, combiner, &i, &conn_count);
        }
    }
}

#ifdef __TBASE__
/*
 * Send ddl to leader cn, the function only be invoked
 * in parallel ddl mode.
 */
void
LeaderCnExecRemoteUtility(RemoteQuery *node,
								PGXCNodeHandle *leader_cn_conn,
								ResponseCombiner *combiner,
								bool need_tran_block,
								GlobalTransactionId gxid,
								Snapshot snapshot,
								CommandId cid)
{
	int cn_cout = 1;
	char *init_str = PGXCNodeGetSessionParamStr();
	if (init_str)
	{
		pgxc_node_set_query(leader_cn_conn, init_str);
	}
	
	SetPlpgsqlTransactionBegin(leader_cn_conn);
	if (pgxc_node_begin(cn_cout, &leader_cn_conn, gxid,
						need_tran_block, false))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("Could not begin transaction on leader coordinator")));
	}

	/* Send other txn related messages to leader cn */
	SendTxnInfo(node, leader_cn_conn, cid, snapshot);

	RemoteReceiveAndCheck(cn_cout, &leader_cn_conn, combiner);
}

/*
 * Execute utility statement on multiple Datanodes
 * It does approximately the same as
 *
 * RemoteQueryState *state = ExecInitRemoteQuery(plan, estate, flags);
 * Assert(TupIsNull(ExecRemoteQuery(state));
 * ExecEndRemoteQuery(state)
 *
 * But does not need an Estate instance and does not do some unnecessary work,
 * like allocating tuple slots.
 */
void
ExecRemoteUtility(RemoteQuery *node, PGXCNodeHandle *leader_cn_conn, ParallelDDLRemoteType type)
#else
ExecRemoteUtility(RemoteQuery *node)
#endif
{
    RemoteQueryState *remotestate;
    ResponseCombiner *combiner;
    bool              force_autocommit = node->force_autocommit;
    RemoteQueryExecType exec_type = node->exec_type;
    GlobalTransactionId gxid = InvalidGlobalTransactionId;
    Snapshot snapshot = NULL;
    PGXCNodeAllHandles *pgxc_connections;
	int					co_conn_count = 0;
	int					dn_conn_count = 0;
    bool        need_tran_block;
    ExecDirectType        exec_direct_type = node->exec_direct_type;
    int            i;
    CommandId    cid = GetCurrentCommandId(true);    

    if (!force_autocommit)
        RegisterTransactionLocalNode(true);

    remotestate = makeNode(RemoteQueryState);
    combiner = (ResponseCombiner *)remotestate;
    InitResponseCombiner(combiner, 0, node->combine_type);

    /*
     * Do not set global_session if it is a utility statement. 
     * Avoids CREATE NODE error on cluster configuration.
     */
    pgxc_connections = get_exec_connections(NULL, node->exec_nodes, exec_type, 
                                            exec_direct_type != EXEC_DIRECT_UTILITY);

#ifdef __TBASE__
	if (type == EXCLUED_LEADER_DDL)
	{
		delete_leadercn_handle(pgxc_connections, leader_cn_conn);
	}
#endif

    dn_conn_count = pgxc_connections->dn_conn_count;
    co_conn_count = pgxc_connections->co_conn_count;

    /* exit right away if no nodes to run command on */
    if (dn_conn_count == 0 && co_conn_count == 0)
    {
        pfree_pgxc_all_handles(pgxc_connections);
        return;
    }

    if (force_autocommit)
        need_tran_block = false;
    else
        need_tran_block = true;

    /*
	 * Commands launched through EXECUTE DIRECT do not need start a
	 * transaction
	 */
    if (exec_direct_type == EXEC_DIRECT_UTILITY)
    {
        need_tran_block = false;

        /* This check is not done when analyzing to limit dependencies */
        if (IsTransactionBlock())
			ereport(ERROR,
					(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
					 errmsg("cannot run EXECUTE DIRECT with utility inside a "
							"transaction block")));
    }

	GetGlobInfoForRemoteUtility(node, &gxid, &snapshot);

#ifdef __TBASE__    
	if (type == ONLY_LEADER_DDL)
    {
		LeaderCnExecRemoteUtility(node, leader_cn_conn, combiner,
									need_tran_block, gxid, snapshot, cid);
		pfree_pgxc_all_handles(pgxc_connections);
		pgxc_node_report_error(combiner);
		return;
    }
	else
    {
        /* Set node begin transaction in plpgsql function for CN/DN */
        for (i = 0; i < dn_conn_count; i++)
        {
            SetPlpgsqlTransactionBegin(pgxc_connections->datanode_handles[i]);
        }  
        
        for (i = 0; i < co_conn_count; i++)
        {
            SetPlpgsqlTransactionBegin(pgxc_connections->coord_handles[i]);
        }     
	}
#endif 

    /*
	 * DDL will firstly be executed on coordinators then datanodes
	 * which will avoid deadlocks in cluster.
	 * Let us assume that user sql and ddl hold conflict locks,
	 * then there will be two situations:
	 * 1. The coordinator is not locked, user sql will see datanodes with no lock.
	 * 2. The coordinator is locked, user sql will wait for ddl to complete.
     *
     * Send BEGIN control command to all coordinator nodes
     */
    if (pgxc_node_begin(co_conn_count,
                        pgxc_connections->coord_handles,
                        gxid,
                        need_tran_block,
                        false))
    {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("Could not begin transaction on coordinators")));
    }

    /* Send other txn related messages to coordinator nodes */
    for (i = 0; i < co_conn_count; i++)
    {
        PGXCNodeHandle *conn = pgxc_connections->coord_handles[i];
		SendTxnInfo(node, conn, cid, snapshot);
    }

    /*
     * Stop if all commands are completed or we got a data row and
     * initialized state node for subsequent invocations
     */
	RemoteReceiveAndCheck(co_conn_count, 
                              pgxc_connections->coord_handles,
							combiner);

#ifdef __TBASE__
	if (LOCAL_PARALLEL_DDL && combiner && combiner->errorMessage)
            {
		pfree_pgxc_all_handles(pgxc_connections);
		pgxc_node_report_error(combiner);
    }
#endif

	/*
	 * Send BEGIN control command to all data nodes
	 */
	if (pgxc_node_begin(dn_conn_count,
						pgxc_connections->datanode_handles,
						gxid,
						need_tran_block,
						false))	
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Could not begin transaction on Datanodes")));
	}

	/* Send other txn related messages to data nodes */
	for (i = 0; i < dn_conn_count; i++)
	{
		PGXCNodeHandle *conn = pgxc_connections->datanode_handles[i];
		SendTxnInfo(node, conn, cid, snapshot);
	}

	RemoteReceiveAndCheck(dn_conn_count, 
							  pgxc_connections->datanode_handles,
							combiner);

	/*
	 * We have processed all responses from nodes and if we have error message
	 * pending we can report it. All connections should be in consistent state
	 * now and so they can be released to the pool after ROLLBACK.
	 */
	pfree_pgxc_all_handles(pgxc_connections);
	pgxc_node_report_error(combiner);
}


/*
 * Called when the backend is ending.
 */
void
PGXCNodeCleanAndRelease(int code, Datum arg)
{

    /* Disconnect from Pooler, if any connection is still held Pooler close it */
    PoolManagerDisconnect();

    /* Close connection with GTM */
    CloseGTM();

    /* Dump collected statistics to the log */
    stat_log();
}

static void
ExecCloseRemoteStatementInternal(const char *stmt_name, List *nodelist)
{
    PGXCNodeAllHandles *all_handles;
    PGXCNodeHandle      **connections;
    ResponseCombiner    combiner;
    int                    conn_count;
    int                 i;

    /* Exit if nodelist is empty */
    if (list_length(nodelist) == 0)
        return;

    /* get needed Datanode connections */
	all_handles = get_handles(nodelist, NIL, false, true, true);
    conn_count = all_handles->dn_conn_count;
    connections = all_handles->datanode_handles;

    for (i = 0; i < conn_count; i++)
    {
        if (connections[i]->state == DN_CONNECTION_STATE_QUERY)
			BufferConnection(connections[i], false);
        if (pgxc_node_send_close(connections[i], true, stmt_name) != 0)
        {
            /*
             * statements are not affected by statement end, so consider
             * unclosed statement on the Datanode as a fatal issue and
             * force connection is discarded
             */
            PGXCNodeSetConnectionState(connections[i],
                    DN_CONNECTION_STATE_ERROR_FATAL);
            ereport(WARNING,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Failed to close Datanode statemrnt")));
        }
        if (pgxc_node_send_sync(connections[i]) != 0)
        {
            PGXCNodeSetConnectionState(connections[i],
                    DN_CONNECTION_STATE_ERROR_FATAL);
            ereport(WARNING,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Failed to close Datanode statement")));
            
            ereport(LOG,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Failed to sync msg to node %s backend_pid:%d", connections[i]->nodename, connections[i]->backend_pid)));
                
        }
        PGXCNodeSetConnectionState(connections[i], DN_CONNECTION_STATE_CLOSE);
    }

    InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
    /*
     * Make sure there are zeroes in unused fields
     */
    memset(&combiner, 0, sizeof(ScanState));

    while (conn_count > 0)
    {
        if (pgxc_node_receive(conn_count, connections, NULL))
        {
            for (i = 0; i < conn_count; i++)
                PGXCNodeSetConnectionState(connections[i],
                        DN_CONNECTION_STATE_ERROR_FATAL);

            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Failed to close Datanode statement")));
        }
        i = 0;
        while (i < conn_count)
        {
            int res = handle_response(connections[i], &combiner);
            if (res == RESPONSE_EOF)
            {
                i++;
            }
            else if (res == RESPONSE_READY ||
                    connections[i]->state == DN_CONNECTION_STATE_ERROR_FATAL)
            {
                if (--conn_count > i)
                    connections[i] = connections[conn_count];
            }
        }
    }

    ValidateAndCloseCombiner(&combiner);
    pfree_pgxc_all_handles(all_handles);
}

/*
 * close remote statement needs to be inside a transaction so that syscache can be accessed
 */
void
ExecCloseRemoteStatement(const char *stmt_name, List *nodelist)
{
    bool need_abort = false;

    if (IsTransactionIdle())
    {
        StartTransactionCommand();
        need_abort = true;
    }

    ExecCloseRemoteStatementInternal(stmt_name, nodelist);

    if (need_abort)
    {
        AbortCurrentTransaction();
    }
}

/*
 * DataNodeCopyInBinaryForAll
 *
 * In a COPY TO, send to all Datanodes PG_HEADER for a COPY TO in binary mode.
 */
int
DataNodeCopyInBinaryForAll(char *msg_buf, int len, int conn_count,
                                      PGXCNodeHandle** connections)
{
    int         i;
    int msgLen = 4 + len;
    int nLen = htonl(msgLen);

    for (i = 0; i < conn_count; i++)
    {
        PGXCNodeHandle *handle = connections[i];
        if (handle->state == DN_CONNECTION_STATE_COPY_IN)
        {
            /* msgType + msgLen */
            if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
            {
                ereport(ERROR,
                    (errcode(ERRCODE_OUT_OF_MEMORY),
                    errmsg("out of memory")));
            }

            handle->outBuffer[handle->outEnd++] = 'd';
            memcpy(handle->outBuffer + handle->outEnd, &nLen, 4);
            handle->outEnd += 4;
            memcpy(handle->outBuffer + handle->outEnd, msg_buf, len);
            handle->outEnd += len;
        }
        else
        {
            add_error_message(handle, "Invalid Datanode connection");
            return EOF;
        }
    }

    return 0;
}

/*
 * Encode parameter values to format of DataRow message (the same format is
 * used in Bind) to prepare for sending down to Datanodes.
 * The data row is copied to RemoteQueryState.paramval_data.
 */
void
SetDataRowForExtParams(ParamListInfo paraminfo, RemoteQueryState *rq_state)
{// #lizard forgives
    StringInfoData buf;
    uint16 n16;
    int i;
    int real_num_params = 0;
    RemoteQuery *node = (RemoteQuery*) rq_state->combiner.ss.ps.plan;

    /* If there are no parameters, there is no data to BIND. */
    if (!paraminfo)
        return;

    Assert(!rq_state->paramval_data);

    /*
     * It is necessary to fetch parameters
     * before looking at the output value.
     */
    for (i = 0; i < paraminfo->numParams; i++)
    {
        ParamExternData *param;

        param = &paraminfo->params[i];

        if (!OidIsValid(param->ptype) && paraminfo->paramFetch != NULL)
            (*paraminfo->paramFetch) (paraminfo, i + 1);

        /*
         * This is the last parameter found as useful, so we need
         * to include all the previous ones to keep silent the remote
         * nodes. All the parameters prior to the last usable having no
         * type available will be considered as NULL entries.
         */
        if (OidIsValid(param->ptype))
            real_num_params = i + 1;
    }

    /*
     * If there are no parameters available, simply leave.
     * This is possible in the case of a query called through SPI
     * and using no parameters.
     */
    if (real_num_params == 0)
    {
        rq_state->paramval_data = NULL;
        rq_state->paramval_len = 0;
        return;
    }

    initStringInfo(&buf);

    /* Number of parameter values */
    n16 = htons(real_num_params);
    appendBinaryStringInfo(&buf, (char *) &n16, 2);

    /* Parameter values */
    for (i = 0; i < real_num_params; i++)
    {
        ParamExternData *param = &paraminfo->params[i];
        uint32 n32;

        /*
         * Parameters with no types are considered as NULL and treated as integer
         * The same trick is used for dropped columns for remote DML generation.
         */
        if (param->isnull || !OidIsValid(param->ptype))
        {
            n32 = htonl(-1);
            appendBinaryStringInfo(&buf, (char *) &n32, 4);
        }
        else
        {
            Oid        typOutput;
            bool    typIsVarlena;
            Datum    pval;
            char   *pstring;
            int        len;

            /* Get info needed to output the value */
            getTypeOutputInfo(param->ptype, &typOutput, &typIsVarlena);

            /*
             * If we have a toasted datum, forcibly detoast it here to avoid
             * memory leakage inside the type's output routine.
             */
            if (typIsVarlena)
                pval = PointerGetDatum(PG_DETOAST_DATUM(param->value));
            else
                pval = param->value;

            /* Convert Datum to string */
            pstring = OidOutputFunctionCall(typOutput, pval);

            /* copy data to the buffer */
            len = strlen(pstring);
            n32 = htonl(len);
            appendBinaryStringInfo(&buf, (char *) &n32, 4);
            appendBinaryStringInfo(&buf, pstring, len);
        }
    }


    /*
     * If parameter types are not already set, infer them from
     * the paraminfo.
     */
    if (node->rq_num_params > 0)
    {
        /*
         * Use the already known param types for BIND. Parameter types
         * can be already known when the same plan is executed multiple
         * times.
         */
        if (node->rq_num_params != real_num_params)
            elog(ERROR, "Number of user-supplied parameters do not match "
                        "the number of remote parameters");
        rq_state->rqs_num_params = node->rq_num_params;
        rq_state->rqs_param_types = node->rq_param_types;
    }
    else
    {
        rq_state->rqs_num_params = real_num_params;
        rq_state->rqs_param_types = (Oid *) palloc(sizeof(Oid) * real_num_params);
        for (i = 0; i < real_num_params; i++)
            rq_state->rqs_param_types[i] = paraminfo->params[i].ptype;
    }

    /* Assign the newly allocated data row to paramval */
    rq_state->paramval_data = buf.data;
    rq_state->paramval_len = buf.len;
}

/*
 * Clear per transaction remote information
 */
void
AtEOXact_Remote(void)
{
    PGXCNodeResetParams(true);
    reset_transaction_handles();
}

/*
 * Invoked when local transaction is about to be committed.
 * If nodestring is specified commit specified prepared transaction on remote
 * nodes, otherwise commit remote nodes which are in transaction.
 */
void
PreCommit_Remote(char *prepareGID, char *nodestring, bool preparedLocalNode)
{// #lizard forgives
    struct rusage        start_r;
    struct timeval        start_t;

    if (log_gtm_stats)
        ResetUsageCommon(&start_r, &start_t);

    /*
     * Made node connections persistent if we are committing transaction
     * that touched temporary tables. We never drop that flag, so after some
     * transaction has created a temp table the session's remote connections
     * become persistent.
     * We do not need to set that flag if transaction that has created a temp
     * table finally aborts - remote connections are not holding temporary
     * objects in this case.
     */
    if (IS_PGXC_LOCAL_COORDINATOR &&
        (MyXactFlags & XACT_FLAGS_ACCESSEDTEMPREL))
        temp_object_included = true;


    /*
     * OK, everything went fine. At least one remote node is in PREPARED state
     * and the transaction is successfully prepared on all the involved nodes.
     * Now we are ready to commit the transaction. We need a new GXID to send
     * down the remote nodes to execute the forthcoming COMMIT PREPARED
     * command. So grab one from the GTM and track it. It will be closed along
     * with the main transaction at the end.
     */
    if (nodestring)
    {
        Assert(preparedLocalNode);
        if(enable_distri_print)
        {
            elog(LOG, "2PC commit precommit remote gid %s", prepareGID);
        }
        pgxc_node_remote_finish(prepareGID, true, nodestring,
                                GetAuxilliaryTransactionId(),
                                GetTopGlobalTransactionId());
    }

    if(!IsTwoPhaseCommitRequired(preparedLocalNode))
    {
        
        if(enable_distri_debug && GetTopGlobalTransactionId() && GetGlobalXidNoCheck())
        {
            if(enable_distri_print)
            {
                elog(LOG, "Non-2PC Commit transaction top xid %u", GetTopGlobalTransactionId());
            }
            LogCommitTranGTM(GetTopGlobalTransactionId(), 
                             GetGlobalXid(),
                             NULL, 
                             1, 
                             true,
                             true, 
                             InvalidGlobalTimestamp, 
                             InvalidGlobalTimestamp);
            is_distri_report = true;
        }
        pgxc_node_remote_commit(TXN_TYPE_CommitTxn, true);
    }

    if (log_gtm_stats)
        ShowUsageCommon("PreCommit_Remote", &start_r, &start_t);

    
}

/*
 * Whether node need clean: last command is not finished
 * 'Z' message: ready for query
 * 'C' message: command complete
 */
static inline bool
node_need_clean(PGXCNodeHandle *handle)
{
	return handle->state != DN_CONNECTION_STATE_IDLE ||
		(('Z' != handle->last_command) && ('C' != handle->last_command));
}

/*
 * Do abort processing for the transaction. We must abort the transaction on
 * all the involved nodes. If a node has already prepared a transaction, we run
 * ROLLBACK PREPARED command on the node. Otherwise, a simple ROLLBACK command
 * is sufficient.
 *
 * We must guard against the case when a transaction is prepared succefully on
 * all the nodes and some error occurs after we send a COMMIT PREPARED message
 * to at lease one node. Such a transaction must not be aborted to preserve
 * global consistency. We handle this case by recording the nodes involved in
 * the transaction at the GTM and keep the transaction open at the GTM so that
 * its reported as "in-progress" on all the nodes until resolved
 *
 *   SPECIAL WARNGING:
 *   ONLY LOG LEVEL ELOG CALL allowed here, else will cause coredump or resource leak in some rare condition.
 */
 
bool
PreAbort_Remote(TranscationType txn_type, bool need_release_handle)
{// #lizard forgives
    /*
     * We are about to abort current transaction, and there could be an
     * unexpected error leaving the node connection in some state requiring
     * clean up, like COPY or pending query results.
     * If we are running copy we should send down CopyFail message and read
     * all possible incoming messages, there could be copy rows (if running
     * COPY TO) ErrorResponse, ReadyForQuery.
     * If there are pending results (connection state is DN_CONNECTION_STATE_QUERY)
     * we just need to read them in and discard, all necessary commands are
     * already sent. The end of input could be CommandComplete or
     * PortalSuspended, in either case subsequent ROLLBACK closes the portal.
     */
    bool                 cancel_ret   = false;
    PGXCNodeAllHandles *all_handles;
    PGXCNodeHandle       **clean_nodes = NULL;
    int                    node_count = 0;
    int                    cancel_dn_count = 0, cancel_co_count = 0;
    int                    *cancel_dn_list = NULL;
    int                    *cancel_co_list = NULL;
    int                 i;
    struct rusage        start_r;
    struct timeval        start_t;

	if (!is_pgxc_handles_init())
	{
		return true;
	}

    clean_nodes = (PGXCNodeHandle**)palloc(sizeof(PGXCNodeHandle*) * (NumCoords + NumDataNodes));
    cancel_dn_list = (int*)palloc(sizeof(int) * NumDataNodes);
    cancel_co_list = (int*)palloc(sizeof(int) * NumCoords);


    if (log_gtm_stats)
        ResetUsageCommon(&start_r, &start_t);
    all_handles = get_current_handles();
    /*
     * Find "dirty" coordinator connections.
     * COPY is never running on a coordinator connections, we just check for
     * pending data.
     */
    for (i = 0; i < all_handles->co_conn_count; i++)
    {
        PGXCNodeHandle *handle = all_handles->coord_handles[i];
		if (handle->sock != NO_SOCKET)
		{		
			if (node_need_clean(handle))
			{
				/*
				 * Forget previous combiner if any since input will be handled by
				 * different one.
				 */
				handle->combiner = NULL;
				clean_nodes[node_count++] = handle;				
				cancel_co_list[cancel_co_count++] = PGXCNodeGetNodeId(handle->nodeoid, NULL);			
				
#ifdef _PG_REGRESS_	
				ereport(LOG,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("PreAbort_Remote node:%s pid:%d status:%d need clean.", handle->nodename, handle->backend_pid, handle->state)));													
#endif
                if (handle->in_extended_query)
                {                
                    if (pgxc_node_send_sync(handle))
                    {
                        ereport(LOG,
                                (errcode(ERRCODE_INTERNAL_ERROR),
                                 errmsg("Failed to sync msg to node:%s pid:%d when abort", handle->nodename, handle->backend_pid)));                                    
                    }
                    
#ifdef _PG_REGRESS_            
                    ereport(LOG,
                                (errcode(ERRCODE_INTERNAL_ERROR),
                                 errmsg("Succeed to sync msg to node:%s pid:%d when abort", handle->nodename, handle->backend_pid)));                                                    
#endif
                }

            }
            else
            {

                if (handle->needSync)
                {
                    ereport(LOG,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("Invalid node:%s pid:%d needSync flag", handle->nodename, handle->backend_pid)));
                }

                
#ifdef _PG_REGRESS_    
                ereport(LOG,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("PreAbort_Remote node:%s pid:%d status:%d no need clean.", handle->nodename, handle->backend_pid, handle->state)));                                                    
#endif
            }
        }
#ifdef __TBASE__
		else
		{
			elog(LOG, "PreAbort_Remote cn node %s pid %d, invalid socket %d!", handle->nodename, handle->backend_pid, handle->sock);
		}
#endif
    }

    /*
     * The same for data nodes, but cancel COPY if it is running.
     */
    for (i = 0; i < all_handles->dn_conn_count; i++)
    {
        PGXCNodeHandle *handle = all_handles->datanode_handles[i];
		if (handle->sock != NO_SOCKET)
		{		
			if (handle->state == DN_CONNECTION_STATE_COPY_IN ||
				handle->state == DN_CONNECTION_STATE_COPY_OUT)
			{
#ifdef _PG_REGRESS_	
				ereport(LOG,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("PreAbort_Remote node:%s pid:%d status:%d need clean.", handle->nodename, handle->backend_pid, handle->state))); 												
#endif
                if (handle->in_extended_query)
                {                
                    if (pgxc_node_send_sync(handle))
                    {
                        ereport(LOG,
                                (errcode(ERRCODE_INTERNAL_ERROR),
                                 errmsg("Failed to sync msg to node:%s pid:%d when abort", handle->nodename, handle->backend_pid)));                                    

                    }
#ifdef _PG_REGRESS_            
                    ereport(LOG,
                                (errcode(ERRCODE_INTERNAL_ERROR),
                                 errmsg("Succeed to sync msg to node:%s pid:%d when abort", handle->nodename, handle->backend_pid)));                                                    
#endif

                }
                

                DataNodeCopyEnd(handle, true);
                /*
                 * Forget previous combiner if any since input will be handled by
                 * different one.
                 */
                handle->combiner = NULL;
#ifdef __TBASE__
                /*
                 * if datanode report error, there is no need to send cancel to it, 
                 * and would not wait this datanode reponse. 
                 */
                if ('E' != handle->transaction_status)
                {
                    clean_nodes[node_count++] = handle;                
                    cancel_dn_list[cancel_dn_count++] = PGXCNodeGetNodeId(handle->nodeoid, NULL);
                }
#endif				
			}
			else if (node_need_clean(handle))
			{
				/*
				 * Forget previous combiner if any since input will be handled by
				 * different one.
				 */
				handle->combiner = NULL;
				clean_nodes[node_count++] = handle;				
				cancel_dn_list[cancel_dn_count++] = PGXCNodeGetNodeId(handle->nodeoid, NULL);
#ifdef _PG_REGRESS_	
				ereport(LOG,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("PreAbort_Remote node:%s pid:%d status:%d need clean.", handle->nodename, handle->backend_pid, handle->state)));												
#endif

                if (handle->in_extended_query)
                {                
                    if (pgxc_node_send_sync(handle))
                    {
                        ereport(LOG,
                                (errcode(ERRCODE_INTERNAL_ERROR),
                                 errmsg("Failed to sync msg to node:%s pid:%d when abort", handle->nodename, handle->backend_pid)));                                    
                    }
#ifdef _PG_REGRESS_            
                    ereport(LOG,
                                (errcode(ERRCODE_INTERNAL_ERROR),
                                 errmsg("Succeed to sync msg to node:%s pid:%d when abort", handle->nodename, handle->backend_pid)));                                                    
#endif

                }
            }
            else
            {
                if (handle->needSync)
                {
                    ereport(LOG,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("Invalid node:%s pid:%d needSync flag", handle->nodename, handle->backend_pid)));                                                        
                }
#ifdef _PG_REGRESS_    
                ereport(LOG,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("PreAbort_Remote node:%s pid:%d status:%d no need clean.", handle->nodename, handle->backend_pid, handle->state)));                                                
#endif

            }
        }
#ifdef __TBASE__
		else
		{
			elog(LOG, "PreAbort_Remote dn node %s pid %d, invalid socket %d!", handle->nodename, handle->backend_pid, handle->sock);
		}
#endif
    }

    if (cancel_co_count || cancel_dn_count)
    {
        
        /*
         * Cancel running queries on the datanodes and the coordinators.
         */
        cancel_ret = PoolManagerCancelQuery(cancel_dn_count, 
                                            cancel_dn_list, 
                                            cancel_co_count,
                                            cancel_co_list,
                                            SIGNAL_SIGINT);
        if (!cancel_ret)
        {
            elog(LOG, "PreAbort_Remote cancel query failed");
        }
    }

    /*
     * Now read and discard any data from the connections found "dirty"
     */
    if (node_count > 0)
    {            
        ResponseCombiner combiner;    

        InitResponseCombiner(&combiner, node_count, COMBINE_TYPE_NONE);
        combiner.extended_query = clean_nodes[0]->in_extended_query;
        /*
         * Make sure there are zeroes in unused fields
         */
        memset(&combiner, 0, sizeof(ScanState));
        combiner.connections = clean_nodes;
        combiner.conn_count = node_count;
        combiner.request_type = REQUEST_TYPE_ERROR;
        combiner.is_abort = true;
    
        pgxc_connections_cleanup(&combiner);

        /* prevent pfree'ing local variable */
        combiner.connections = NULL;

        CloseCombiner(&combiner);
    }

    pgxc_abort_connections(all_handles);
    
#ifdef _PG_REGRESS_    
    {
        int nbytes = 0;
        int ii = 0;
        for (ii = 0; ii < node_count; ii++)
        {
            nbytes = pgxc_node_is_data_enqueued(clean_nodes[ii]);
            if (nbytes)
            {
                elog(PANIC, "PreAbort_Remote %d bytes data left over in fd:%d remote backendpid:%d nodename:%s",
                            nbytes, clean_nodes[ii]->sock, clean_nodes[ii]->backend_pid, clean_nodes[ii]->nodename);
            }
        }
    }
#endif

    if (clean_nodes)
    {
        pfree(clean_nodes);
        clean_nodes = NULL;
    }

    if (cancel_dn_list)
    {
        pfree(cancel_dn_list);
        cancel_dn_list = NULL;
    }

    if (cancel_co_list)
    {
        pfree(cancel_co_list);
        cancel_co_list = NULL;
    }    

    pgxc_node_remote_abort(txn_type, need_release_handle);

	/*
	 * Drop the connections to ensure aborts are handled properly.
	 *
	 * XXX We should really be consulting PersistentConnections parameter and
	 * keep the connections if its set. But as a short term measure, to address
	 * certain issues for aborted transactions, we drop the connections.
	 * Revisit and fix the issue
	 */
	elog(DEBUG5, "temp_object_included %d", temp_object_included);
	/* cleanup and release handles is already done in pgxc_node_remote_abort */
#if 0	
	if (release_handle)
	{
		if (!temp_object_included)
		{
			/* Clean up remote sessions */
			pgxc_node_remote_cleanup_all();
			release_handles();
		}
	}
	
	clear_handles();
#endif
    pfree_pgxc_all_handles(all_handles);

    if (log_gtm_stats)
        ShowUsageCommon("PreAbort_Remote", &start_r, &start_t);

    return true;
}


/*
 * Invoked when local transaction is about to be prepared.
 * If invoked on a Datanode just commit transaction on remote connections,
 * since secondary sessions are read only and never need to be prepared.
 * Otherwise run PREPARE on remote connections, where writable commands were
 * sent (connections marked as not read-only).
 * If that is explicit PREPARE (issued by client) notify GTM.
 * In case of implicit PREPARE not involving local node (ex. caused by
 * INSERT, UPDATE or DELETE) commit prepared transaction immediately.
 * Return list of node names where transaction was actually prepared, include
 * the name of the local node if localNode is true.
 */
char *
PrePrepare_Remote(char *prepareGID, bool localNode, bool implicit)
{// #lizard forgives
    /* Always include local node if running explicit prepare */
#ifdef __TBASE__
    int    ret = 0;
#endif
    char *nodestring;
    struct rusage        start_r;
    struct timeval        start_t;

    if (log_gtm_stats)
        ResetUsageCommon(&start_r, &start_t);

    /*
     * Primary session is doing 2PC, just commit secondary processes and exit
     */
    if (IS_PGXC_DATANODE)
    {
        pgxc_node_remote_commit(TXN_TYPE_CommitTxn, true);
        return NULL;
    }
    if(enable_distri_print)
    {
        elog(LOG, "2PC commit PrePrepare_Remote xid %d", GetTopTransactionIdIfAny());
    }
    nodestring = pgxc_node_remote_prepare(prepareGID,
                                                !implicit || localNode,
                                                implicit);
#ifdef __TWO_PHASE_TESTS__
    twophase_in = IN_OTHER;
#endif
    if(!nodestring)
    {
        elog(ERROR, "Remote Prepare Transaction gid:%s Failed", prepareGID);
    }
#ifdef __TWO_PHASE_TRANS__
    if (nodestring && (!localNode || !IsXidImplicit(prepareGID)))
    {
        g_twophase_state.state = TWO_PHASE_PREPARED;
    }
#endif

    if (!implicit && IS_PGXC_LOCAL_COORDINATOR)
    {
        /* Save the node list and gid on GTM. */
        ret = StartPreparedTranGTM(GetTopGlobalTransactionId(), prepareGID, nodestring);
#ifdef __TWO_PHASE_TESTS__
        if (PART_PREPARE_PREPARE_GTM == twophase_exception_case)
        {
            complish = true;
            elog(ERROR, "ALL_PREPARE_PREPARE_GTM is running");
        }
#endif
#ifdef __TBASE__
        if (ret)
        {
            elog(ERROR, "Prepare Transaction gid:%s on GTM failed", prepareGID);
        }
#endif
    }

    if(enable_distri_print)
    {
        elog(LOG, "commit phase xid %d implicit %d local node %d prepareGID %s", 
                            GetTopTransactionIdIfAny(), implicit, localNode, prepareGID);
    }
    /*
     * If no need to commit on local node go ahead and commit prepared
     * transaction right away.
     */
    if (implicit && !localNode && nodestring)
    {
#ifdef __TWO_PHASE_TRANS__        
        /* 
         * if start node not participate, still record 2pc before  pgxc_node_remote_finish,  
         * and it will be flushed and sync to slave node when record commit_timestamp
         */
        record_2pc_involved_nodes_xid(prepareGID, PGXCNodeName, g_twophase_state.start_xid, 
            g_twophase_state.participants, g_twophase_state.start_xid);
#endif
        pgxc_node_remote_finish(prepareGID, true, nodestring,
                                GetAuxilliaryTransactionId(),
                                GetTopGlobalTransactionId());
        pfree(nodestring);
        nodestring = NULL;
    }

    if (log_gtm_stats)
        ShowUsageCommon("PrePrepare_Remote", &start_r, &start_t);

    return nodestring;
}

/*
 * Invoked immediately after local node is prepared.
 * Notify GTM about completed prepare.
 */
void
PostPrepare_Remote(char *prepareGID, bool implicit)
{
#ifndef __TBASE__
    struct rusage        start_r;
    struct timeval        start_t;

    if (log_gtm_stats)
        ResetUsageCommon(&start_r, &start_t);

    if (!implicit)
        PrepareTranGTM(GetTopGlobalTransactionId());

    if (log_gtm_stats)
        ShowUsageCommon("PostPrepare_Remote", &start_r, &start_t);
#endif
    reset_transaction_handles();
}

/*
 * Returns true if 2PC is required for consistent commit: if there was write
 * activity on two or more nodes within current transaction.
 */
bool
IsTwoPhaseCommitRequired(bool localWrite)
{// #lizard forgives
	PGXCNodeAllHandles *handles = NULL;
    bool                found = localWrite;
    int                 i = 0;
#ifdef __TBASE__
    int                                     sock_fatal_count = 0;
#endif

    /* Never run 2PC on Datanode-to-Datanode connection */
    if (IS_PGXC_DATANODE)
        return false;

    if (MyXactFlags & XACT_FLAGS_ACCESSEDTEMPREL)
    {
        elog(DEBUG1, "Transaction accessed temporary objects - "
                "2PC will not be used and that can lead to data inconsistencies "
                "in case of failures");
        return false;
    }

    /*
     * If no XID assigned, no need to run 2PC since neither coordinator nor any
     * remote nodes did write operation
     */
    if (!TransactionIdIsValid(GetTopTransactionIdIfAny()))
        return false;

#ifdef __TBASE__
	handles = get_sock_fatal_handles();
	sock_fatal_count = handles->dn_conn_count + handles->co_conn_count;

	for (i = 0; i < handles->dn_conn_count; i++)
	{
		PGXCNodeHandle *conn = handles->datanode_handles[i];

		elog(LOG, "IsTwoPhaseCommitRequired, fatal_conn=%p, fatal_conn->nodename=%s, fatal_conn->sock=%d, "
			"fatal_conn->read_only=%d, fatal_conn->transaction_status=%c, "
			"fatal_conn->sock_fatal_occurred=%d, conn->backend_pid=%d, fatal_conn->error=%s", 
			conn, conn->nodename, conn->sock, conn->read_only, conn->transaction_status,
			conn->sock_fatal_occurred, conn->backend_pid,  conn->error);
	}

	for (i = 0; i < handles->co_conn_count; i++)
	{
		PGXCNodeHandle *conn = handles->coord_handles[i];

		elog(LOG, "IsTwoPhaseCommitRequired, fatal_conn=%p, fatal_conn->nodename=%s, fatal_conn->sock=%d, "
			"fatal_conn->read_only=%d, fatal_conn->transaction_status=%c, "
			"fatal_conn->sock_fatal_occurred=%d, conn->backend_pid=%d, fatal_conn->error=%s", 
			conn, conn->nodename, conn->sock, conn->read_only, conn->transaction_status,
			conn->sock_fatal_occurred, conn->backend_pid,  conn->error);
	}
	pfree_pgxc_all_handles(handles);

	if (sock_fatal_count != 0)
	{
		elog(ERROR, "IsTwoPhaseCommitRequired, Found %d sock fatal handles exist", sock_fatal_count);
	}
#endif
    /* get current transaction handles that we register when pgxc_node_begin */
	handles = get_current_txn_handles();
    for (i = 0; i < handles->dn_conn_count; i++)
    {
        PGXCNodeHandle *conn = handles->datanode_handles[i];

#ifdef __TBASE__
		elog(DEBUG5, "IsTwoPhaseCommitRequired, conn->nodename=%s, conn->sock=%d, conn->read_only=%d, conn->transaction_status=%c", 
			conn->nodename, conn->sock, conn->read_only, conn->transaction_status);
#endif
		if (conn->sock == NO_SOCKET)
        {
            elog(ERROR, "IsTwoPhaseCommitRequired, remote node %s's connection handle is invalid, backend_pid: %d",
                 conn->nodename, conn->backend_pid);
        }
        else if (!conn->read_only && conn->transaction_status == 'T')
        {
            if (found)
            {
                pfree_pgxc_all_handles(handles);
                return true; /* second found */
            }    
            else
            {
                found = true; /* first found */
            }
        }
        else if (conn->transaction_status == 'E')
        {
            elog(ERROR, "IsTwoPhaseCommitRequired, remote node %s is in error state, backend_pid: %d",
                    conn->nodename, conn->backend_pid);
        }
    }
    for (i = 0; i < handles->co_conn_count; i++)
    {
        PGXCNodeHandle *conn = handles->coord_handles[i];

#ifdef __TBASE__
		elog(DEBUG5, "IsTwoPhaseCommitRequired, conn->nodename=%s, conn->sock=%d, conn->read_only=%d, conn->transaction_status=%c", 
			conn->nodename, conn->sock, conn->read_only, conn->transaction_status);
#endif
		if (conn->sock == NO_SOCKET)
        {
            elog(ERROR, "IsTwoPhaseCommitRequired, remote node %s's connection handle is invalid, backend_pid: %d",
                 conn->nodename, conn->backend_pid);
        }
        else if (!conn->read_only && conn->transaction_status == 'T')
        {
            if (found)
            {
                pfree_pgxc_all_handles(handles);
                return true; /* second found */
            }
            else
            {
                found = true; /* first found */
            }
        }
        else if (conn->transaction_status == 'E')
        {
            elog(ERROR, "IsTwoPhaseCommitRequired, remote node %s is in error state, backend_pid: %d",
                 conn->nodename, conn->backend_pid);
        }
    }
    pfree_pgxc_all_handles(handles);

#ifdef __TBASE__
	elog(DEBUG5, "IsTwoPhaseCommitRequired return false");
#endif

    return false;
}


/* An additional phase for explicitly prepared transactions */

static bool
pgxc_node_remote_prefinish(char *prepareGID, char *nodestring)
{// #lizard forgives
    PGXCNodeHandle       *connections[TBASE_MAX_COORDINATOR_NUMBER + TBASE_MAX_DATANODE_NUMBER];
    int                    conn_count = 0;
    ResponseCombiner    combiner;
    PGXCNodeAllHandles *pgxc_handles;
    char               *nodename;
    List               *nodelist = NIL;
    List               *coordlist = NIL;
    int                    i;
    GlobalTimestamp    global_committs;
    /*
     * Now based on the nodestring, run COMMIT/ROLLBACK PREPARED command on the
     * remote nodes and also finish the transaction locally is required
     */
    elog(DEBUG8, "pgxc_node_remote_prefinish nodestring %s gid %s", nodestring, prepareGID);
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
    elog(DEBUG8, "pgxc_node_remote_prefinish start GTM timestamp nodestring %s gid %s", nodestring, prepareGID);
    global_committs = GetGlobalTimestampGTM();
#ifdef __TWO_PHASE_TESTS__
    if (ALL_PREPARE_REMOTE_PREFINISH == twophase_exception_case)
    {
        global_committs = 0;
        complish = true;
    }
#endif
    if(!GlobalTimestampIsValid(global_committs))
    {
        ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
             errmsg("failed to get global timestamp for prefinish command gid %s", prepareGID)));
    }
    if(enable_distri_print)
    {
        elog(LOG, "prefinish phase get global timestamp gid %s, time " INT64_FORMAT, prepareGID, global_committs);
    }
    SetGlobalPrepareTimestamp(global_committs);/* Save for local commit */
    EndExplicitGlobalPrepare(prepareGID);
#endif

    
    nodename = strtok(nodestring, ",");
    while (nodename != NULL)
    {
        int        nodeIndex;
        char    nodetype;

        /* Get node type and index */
        nodetype = PGXC_NODE_NONE;
        nodeIndex = PGXCNodeGetNodeIdFromName(nodename, &nodetype);
        if (nodetype == PGXC_NODE_NONE)
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                     errmsg("PGXC Node %s: object not defined",
                            nodename)));

        /* Check if node is requested is the self-node or not */
        if (nodetype == PGXC_NODE_COORDINATOR)
        {
            if (nodeIndex != PGXCNodeId - 1)
                coordlist = lappend_int(coordlist, nodeIndex);
        }
        else
            nodelist = lappend_int(nodelist, nodeIndex);

        nodename = strtok(NULL, ",");
    }

    if (nodelist == NIL && coordlist == NIL)
        return false;

	pgxc_handles = get_handles(nodelist, coordlist, false, true, true);

    for (i = 0; i < pgxc_handles->dn_conn_count; i++)
    {
        PGXCNodeHandle *conn = pgxc_handles->datanode_handles[i];

        if (pgxc_node_send_gid(conn, prepareGID))
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("failed to send GID to the node %u",
                            conn->nodeoid)));
        }
    
        if (pgxc_node_send_prefinish_timestamp(conn, global_committs))
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("failed to send prefinish timestamp to the node %u",  conn->nodeoid)));
        }
        
        if (pgxc_node_flush(conn))
        {
            /*
             * Do not bother with clean up, just bomb out. The error handler
             * will invoke RollbackTransaction which will do the work.
             */
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("pgxc_node_remote_prefinish failed to send prefinish command to the node %u for %s", conn->nodeoid, strerror(errno))));
        }
        else
        {
            /* Read responses from these */
            connections[conn_count++] = conn;
            elog(DEBUG8, "prefinish add connection");
        }

    }

    for (i = 0; i < pgxc_handles->co_conn_count; i++)
    {
        PGXCNodeHandle *conn = pgxc_handles->coord_handles[i];

        if (pgxc_node_send_gid(conn, prepareGID))
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("failed to send GID to the node %u",
                            conn->nodeoid)));
        }
    
        if (pgxc_node_send_prefinish_timestamp(conn, global_committs))
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("failed to send prefinish timestamp to the node %u",  conn->nodeoid)));
        }
        
        if (pgxc_node_flush(conn))
        {
            /*
             * Do not bother with clean up, just bomb out. The error handler
             * will invoke RollbackTransaction which will do the work.
             */
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("pgxc_node_remote_prefinish failed to send prefinish command to the node %u for %s", conn->nodeoid, strerror(errno))));
        }
        else
        {
            /* Read responses from these */
            connections[conn_count++] = conn;
            elog(DEBUG8, "prefinish add connection");
        }

    }

    if (conn_count)
    {
        InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
        /* Receive responses */
        if (pgxc_node_receive_responses(conn_count, connections, NULL, &combiner))
        {
            if (combiner.errorMessage)
                pgxc_node_report_error(&combiner);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("Failed to COMMIT the transaction on one or more nodes")));
        }
        else
            CloseCombiner(&combiner);
    }

    pfree_pgxc_all_handles(pgxc_handles);

    return true;
}



/*
 * Execute COMMIT/ABORT PREPARED issued by the remote client on remote nodes.
 * Contacts GTM for the list of involved nodes and for work complete
 * notification. Returns true if prepared transaction on local node needs to be
 * finished too.
 */
bool
FinishRemotePreparedTransaction(char *prepareGID, bool commit)
{// #lizard forgives
    char                   *nodestring;
    char                    *savenodestring PG_USED_FOR_ASSERTS_ONLY;
    GlobalTransactionId        gxid, prepare_gxid;
    bool                    prepared_local = false;

#ifdef __TWO_PHASE_TRANS__
	/*
	 * Since g_twophase_state is cleared after prepare phase,
	 * g_twophase_state shoud be assigned here
	 */
	strncpy(g_twophase_state.gid, prepareGID, GIDSIZE);
	strncpy(g_twophase_state.start_node_name, PGXCNodeName, NAMEDATALEN);
	g_twophase_state.state = TWO_PHASE_PREPARED;
	g_twophase_state.is_start_node = true;
#endif

    /*
     * Get the list of nodes involved in this transaction.
     *
     * This function returns the GXID of the prepared transaction. It also
     * returns a fresh GXID which can be used for running COMMIT PREPARED
     * commands on the remote nodes. Both these GXIDs can then be either
     * committed or aborted together.
     *
     * XXX While I understand that we get the prepared and a new GXID with a
     * single call, it doesn't look nicer and create confusion. We should
     * probably split them into two parts. This is used only for explicit 2PC
     * which should not be very common in XC
     *
     * In xc_maintenance_mode mode, we don't fail if the GTM does not have
     * knowledge about the prepared transaction. That may happen for various
     * reasons such that an earlier attempt cleaned up it from GTM or GTM was
     * restarted in between. The xc_maintenance_mode is a kludge to come out of
     * such situations. So it seems alright to not be too strict about the
     * state
     */
#ifdef __TWO_PHASE_TESTS__
    if (ALL_PREPARE_FINISH_REMOTE_PREPARED == twophase_exception_case)
    {
        complish = true;
        elog(ERROR, "ALL_PREPARE_FINISH_REMOTE_PREPARED complish");
    }
#endif
    if ((GetGIDDataGTM(prepareGID, &gxid, &prepare_gxid, &nodestring) < 0) &&
        !xc_maintenance_mode && !g_twophase_state.is_readonly)
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("prepared transaction with identifier \"%s\" does not exist",
                        prepareGID)));

    /*
     * Please note that with xc_maintenance_mode = on, COMMIT/ROLLBACK PREPARED will not
     * propagate to remote nodes. Only GTM status is cleaned up.
     */
#ifdef __USE_GLOBAL_SNAPSHOT__
    if (xc_maintenance_mode)
    {
        if (commit)
        {
            pgxc_node_remote_commit(TXN_TYPE_CommitTxn, true);
            CommitPreparedTranGTM(prepare_gxid, gxid, 0, NULL);
        }
        else
        {
            pgxc_node_remote_abort(TXN_TYPE_RollbackTxn, true);
            RollbackTranGTM(prepare_gxid);
            RollbackTranGTM(gxid);
        }
        return false;
    }
#endif

#ifdef __TWO_PHASE_TRANS__
    /* 
     * not allowed user commit residual transaction in xc_maintenance_mode, 
     * since we need commit them in unified timestamp
     */
    if (xc_maintenance_mode)
    {
        elog(ERROR, "can not commit transaction '%s' in xc_maintainence_mode", prepareGID);
    }

    if (nodestring)
    {
        strncpy(g_twophase_state.participants, nodestring,((NAMEDATALEN+1) * (TBASE_MAX_DATANODE_NUMBER + TBASE_MAX_COORDINATOR_NUMBER)));
    }
#endif

#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
    if (nodestring)
    {
        savenodestring = MemoryContextStrdup(TopMemoryContext, nodestring);
        pgxc_node_remote_prefinish(prepareGID, savenodestring);
    }
#endif
    prepared_local = pgxc_node_remote_finish(prepareGID, commit, nodestring,
                                             gxid, prepare_gxid);
    free(nodestring);
#ifdef __USE_GLOBAL_SNAPSHOT__

    if (commit)
    {
        /*
         * XXX For explicit 2PC, there will be enough delay for any
         * waited-committed transactions to send a final COMMIT message to the
         * GTM.
         */
        CommitPreparedTranGTM(prepare_gxid, gxid, 0, NULL);
    }
    else
    {
        RollbackTranGTM(prepare_gxid);
        RollbackTranGTM(gxid);
    }
#endif

#ifdef __TBASE__
    FinishGIDGTM(prepareGID);
#endif
    SetCurrentHandlesReadonly();
    return prepared_local;
}





/*
 * Complete previously prepared transactions on remote nodes.
 * Release remote connection after completion.
 */
static bool
pgxc_node_remote_finish(char *prepareGID, bool commit,
                        char *nodestring, GlobalTransactionId gxid,
                        GlobalTransactionId prepare_gxid)
{// #lizard forgives
    char               *finish_cmd;
    PGXCNodeHandle **connections = NULL;
    int                    conn_count = 0;
    ResponseCombiner    combiner;
    PGXCNodeAllHandles *pgxc_handles;
    bool                prepared_local = false;
    char               *nodename;
    List               *nodelist = NIL;
    List               *coordlist = NIL;
    int                    i;
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
    GlobalTimestamp    global_committs;
#endif
#ifdef __TWO_PHASE_TRANS__
    /* 
     *any send error in twophase trans will set all_conn_healthy to false 
     *all transaction call pgxc_node_remote_finish is twophase trans
     *only called by starter: set g_twophase_state just before send msg to remote nodes
     */
    bool               all_conn_healthy = true;
    int                twophase_index = 0;
#endif
#ifdef __TWO_PHASE_TESTS__
    if (ALL_PREPARE_REMOTE_FINISH == twophase_exception_case)
    {
        complish = true;
        elog(ERROR, "stop transaction '%s' in ALL_PREPARE_REMOTE_FINISH", prepareGID);
    }
    
    if (PART_ABORT_REMOTE_FINISH == twophase_exception_case)
    {
        complish = false;
        elog(ERROR, "PART_ABORT_REMOTE_FINISH complete");
    }
    if (PART_COMMIT_SEND_TIMESTAMP == twophase_exception_case || 
        PART_COMMIT_SEND_QUERY == twophase_exception_case ||
        PART_COMMIT_RESPONSE_ERROR == twophase_exception_case)
    {
        twophase_in = IN_REMOTE_FINISH;
    }
#endif

    connections = (PGXCNodeHandle**)palloc(sizeof(PGXCNodeHandle*) * (TBASE_MAX_DATANODE_NUMBER + TBASE_MAX_COORDINATOR_NUMBER));
    if (connections == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory for connections")));
    }
    
    /*
     * Now based on the nodestring, run COMMIT/ROLLBACK PREPARED command on the
     * remote nodes and also finish the transaction locally is required
     */
    elog(DEBUG8, "pgxc_node_remote_finish nodestring %s gid %s", nodestring, prepareGID);
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
    elog(DEBUG8, "pgxc_node_remote_finish start GTM timestamp nodestring %s gid %s", nodestring, prepareGID);
    if(delay_before_acquire_committs)
    {
        pg_usleep(delay_before_acquire_committs);
    }

    global_committs = GetGlobalTimestampGTM();
    if(!GlobalTimestampIsValid(global_committs)){
        ereport(ERROR,
        (errcode(ERRCODE_INTERNAL_ERROR),
         errmsg("failed to get global timestamp for %s PREPARED command",
                commit ? "COMMIT" : "ROLLBACK")));
    }
    if(enable_distri_print)
    {
        elog(LOG, "commit phase get global commit timestamp gid %s, time " INT64_FORMAT, prepareGID, global_committs);
        //record_2pc_commit_timestamp(prepareGID, global_committs);
    }
    
    if(delay_after_acquire_committs)
    {
        pg_usleep(delay_after_acquire_committs);
    }
    SetGlobalCommitTimestamp(global_committs);/* Save for local commit */
#endif

    nodename = strtok(nodestring, ",");
    while (nodename != NULL)
    {
        int        nodeIndex;
        char    nodetype;

        /* Get node type and index */
        nodetype = PGXC_NODE_NONE;
        nodeIndex = PGXCNodeGetNodeIdFromName(nodename, &nodetype);
        if (nodetype == PGXC_NODE_NONE)
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                     errmsg("PGXC Node %s: object not defined",
                            nodename)));

        /* Check if node is requested is the self-node or not */
        if (nodetype == PGXC_NODE_COORDINATOR)
        {
            if (nodeIndex == PGXCNodeId - 1)
                prepared_local = true;
            else
                coordlist = lappend_int(coordlist, nodeIndex);
        }
        else
            nodelist = lappend_int(nodelist, nodeIndex);

        nodename = strtok(NULL, ",");
    }

    if (nodelist == NIL && coordlist == NIL)
        return prepared_local;


	pgxc_handles = get_handles(nodelist, coordlist, false, true, true);
#ifdef __TWO_PHASE_TRANS__
    SetLocalTwoPhaseStateHandles(pgxc_handles);
#endif

    finish_cmd = (char *) palloc(64 + strlen(prepareGID));

    if (commit)
        sprintf(finish_cmd, "COMMIT PREPARED '%s'", prepareGID);
    else
        sprintf(finish_cmd, "ROLLBACK PREPARED '%s'", prepareGID);

    if(enable_distri_debug)
    {
        int node_count = pgxc_handles->dn_conn_count + pgxc_handles->co_conn_count;

        if(prepared_local)
        {
            node_count++;
        }
        if(enable_distri_print)
        {
            elog(LOG, "Commit transaction prepareGID %s nodestring %s top xid %u auxid %u "INT64_FORMAT" "INT64_FORMAT,
                prepareGID, nodestring, GetAuxilliaryTransactionId(), GetTopGlobalTransactionId(),
                GetGlobalPrepareTimestamp(), GetGlobalCommitTimestamp());
        }
        LogCommitTranGTM(GetTopGlobalTransactionId(), prepareGID, 
                             nodestring, 
                             node_count,
                             true,
                             true, 
                             GetGlobalPrepareTimestamp(), 
                             GetGlobalCommitTimestamp());
        is_distri_report = true;
    }
    
#ifdef __TWO_PHASE_TRANS__
    g_twophase_state.state = commit ? TWO_PHASE_COMMITTING : TWO_PHASE_ABORTTING;
    /*
     * transaction start node record 2pc file just before
     * send query to remote participants
     */
    if (IS_PGXC_LOCAL_COORDINATOR && commit)
    {
        /*
         * record commit timestamp in 2pc file for start node
         */
        record_2pc_commit_timestamp(prepareGID, global_committs);
    }
#endif    

    for (i = 0; i < pgxc_handles->dn_conn_count; i++)
    {
        PGXCNodeHandle *conn = pgxc_handles->datanode_handles[i];
#ifdef __TWO_PHASE_TRANS__
        twophase_index = g_twophase_state.datanode_index;
        g_twophase_state.datanode_state[twophase_index].is_participant = true;
        g_twophase_state.datanode_state[twophase_index].handle_idx = i;
        g_twophase_state.datanode_state[twophase_index].state = g_twophase_state.state;
#endif        

#ifdef __USE_GLOBAL_SNAPSHOT__
        if (pgxc_node_send_gxid(conn, gxid))
        {
#ifdef __TWO_PHASE_TRANS__
            // *record conn state :send gxid fail
            g_twophase_state.datanode_state[twophase_index].conn_state = 
                TWO_PHASE_SEND_GXID_ERROR;
            g_twophase_state.datanode_state[twophase_index] = 
                (commit == true) ? TWO_PHASE_COMMIT_ERROR : TWO_PHASE_ABORT_ERROR;
            all_conn_healthy = false;
            g_twophase_state.datanode_index++;
            continue;
#endif
        }
#endif

#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__        
        if (pgxc_node_send_global_timestamp(conn, global_committs))
        {
#ifdef __TWO_PHASE_TRANS__
            g_twophase_state.datanode_state[twophase_index].conn_state = 
                TWO_PHASE_SEND_TIMESTAMP_ERROR;
            g_twophase_state.datanode_state[twophase_index].state = 
                (commit == true) ? TWO_PHASE_COMMIT_ERROR : TWO_PHASE_ABORT_ERROR;
            all_conn_healthy = false;
            g_twophase_state.datanode_index++;
            continue;
#endif
        }
#endif

        if (pgxc_node_send_query(conn, finish_cmd))
        {
#ifdef __TWO_PHASE_TRANS__
            // record conn state :send gxid fail
            g_twophase_state.datanode_state[twophase_index].conn_state = 
                TWO_PHASE_SEND_QUERY_ERROR;
            g_twophase_state.datanode_state[twophase_index].state = 
                (commit == true) ? TWO_PHASE_COMMIT_ERROR : TWO_PHASE_ABORT_ERROR;
            all_conn_healthy = false;
            g_twophase_state.datanode_index++;
            continue;
#endif
        }
        else
        {
            /* Read responses from these */
            connections[conn_count++] = conn;
#ifdef __TWO_PHASE_TRANS__
            g_twophase_state.datanode_state[twophase_index].conn_state = 
                TWO_PHASE_HEALTHY;
            twophase_index = g_twophase_state.connections_num;
            g_twophase_state.connections[twophase_index].node_type = PGXC_NODE_DATANODE;
            g_twophase_state.connections[twophase_index].conn_trans_state_index = 
                g_twophase_state.datanode_index;
            g_twophase_state.connections_num++;
            g_twophase_state.datanode_index++;
#endif
        }
    }

    /* Make sure datanode commit first */
    if (conn_count && is_txn_has_parallel_ddl)
    {
        InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
#ifdef __TWO_PHASE_TRANS__
        g_twophase_state.response_operation =
                (commit == true) ? REMOTE_FINISH_COMMIT : REMOTE_FINISH_ABORT;
#endif
        /* Receive responses */
        if (pgxc_node_receive_responses(conn_count, connections, NULL, &combiner) ||
            !validate_combiner(&combiner))
        {
            if (combiner.errorMessage)
                pgxc_node_report_error(&combiner);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                                errmsg("Failed to COMMIT the transaction on one or more nodes")));
        }
        else
            CloseCombiner(&combiner);

        conn_count = 0;
    }

	for (i = 0; i < pgxc_handles->co_conn_count; i++)
	{
		PGXCNodeHandle *conn = pgxc_handles->coord_handles[i];
#ifdef __TWO_PHASE_TRANS__
        twophase_index = g_twophase_state.coord_index;
        g_twophase_state.coord_state[twophase_index].is_participant = true;
        g_twophase_state.coord_state[twophase_index].handle_idx = i;
        g_twophase_state.coord_state[twophase_index].state = g_twophase_state.state;
#endif

#ifdef __USE_GLOBAL_SNAPSHOT__
        if (pgxc_node_send_gxid(conn, gxid))
        {
#ifdef __TWO_PHASE_TRANS__
            g_twophase_state.coord_state[twophase_index].conn_state = 
                TWO_PHASE_SEND_GXID_ERROR;
            g_twophase_state.coord_state[twophase_index] = 
                (commit == true) ? TWO_PHASE_COMMIT_ERROR : TWO_PHASE_ABORT_ERROR;
            all_conn_healthy = false;
            g_twophase_state.coord_index++;
            continue;
#endif
        }
#endif

#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__        
        if (pgxc_node_send_global_timestamp(conn, global_committs))
        {
#ifdef __TWO_PHASE_TRANS__
            g_twophase_state.coord_state[twophase_index].conn_state = 
                TWO_PHASE_SEND_TIMESTAMP_ERROR;
            g_twophase_state.coord_state[twophase_index].state = 
                (commit == true) ? TWO_PHASE_COMMIT_ERROR : TWO_PHASE_ABORT_ERROR;
            all_conn_healthy = false;
            g_twophase_state.coord_index++;
            continue;
#endif
        }
#endif

        if (pgxc_node_send_query(conn, finish_cmd))
        {
#ifdef __TWO_PHASE_TRANS__
            g_twophase_state.coord_state[twophase_index].conn_state = 
                TWO_PHASE_SEND_QUERY_ERROR;
            g_twophase_state.coord_state[twophase_index].state = 
                (commit == true) ? TWO_PHASE_COMMIT_ERROR : TWO_PHASE_ABORT_ERROR;
            all_conn_healthy = false;
            g_twophase_state.coord_index++;
            continue;
#endif
        }
        else
        {
            /* Read responses from these */
            connections[conn_count++] = conn;
#ifdef __TWO_PHASE_TRANS__
            g_twophase_state.coord_state[twophase_index].conn_state = TWO_PHASE_HEALTHY;
            twophase_index = g_twophase_state.connections_num;
            g_twophase_state.connections[twophase_index].node_type = PGXC_NODE_COORDINATOR;
            g_twophase_state.connections[twophase_index].conn_trans_state_index = 
                g_twophase_state.coord_index;
            g_twophase_state.connections_num++;
            g_twophase_state.coord_index++;
#endif
        }
    }

    if (conn_count)
    {
        InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
#ifdef __TWO_PHASE_TRANS__
        g_twophase_state.response_operation = 
            (commit == true) ? REMOTE_FINISH_COMMIT : REMOTE_FINISH_ABORT;
#endif
        /* Receive responses */
        if (pgxc_node_receive_responses(conn_count, connections, NULL, &combiner) ||
                !validate_combiner(&combiner))
        {
            if (combiner.errorMessage)
                pgxc_node_report_error(&combiner);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("Failed to COMMIT the transaction on one or more nodes")));
        }
        else
            CloseCombiner(&combiner);
    }
    
#ifdef __TWO_PHASE_TRANS__
    if (all_conn_healthy == false)
    {
        if (commit)
        {
            elog(ERROR, "Failed to send COMMIT PREPARED '%s' to one or more nodes", prepareGID);
        }
        else
        {
            elog(ERROR, "Failed to send ROLLBACK PREPARED '%s' to one or more nodes", prepareGID);
        }
    }
#endif    

	if (!temp_object_included && !PersistentConnections)
    {
        /* Clean up remote sessions */
		pgxc_node_remote_cleanup_all();
        release_handles(false);
    }
    clear_handles();
    pfree_pgxc_all_handles(pgxc_handles);
	reset_transaction_handles();
    pfree(finish_cmd);

#ifdef __TWO_PHASE_TRANS__
    if (prepared_local)
    {
        g_twophase_state.state = (commit == true) ? TWO_PHASE_COMMIT_END : TWO_PHASE_ABORT_END;
    }
    else
    {
        g_twophase_state.state = (commit == true) ? TWO_PHASE_COMMITTED : TWO_PHASE_ABORTTED;
        ClearLocalTwoPhaseState();
    }
#endif    

    if (connections)
    {
        pfree(connections);
        connections = NULL;
    }
    
    return prepared_local;
}

/*****************************************************************************
 *
 * Simplified versions of ExecInitRemoteQuery, ExecRemoteQuery and
 * ExecEndRemoteQuery: in XCP they are only used to execute simple queries.
 *
 *****************************************************************************/
RemoteQueryState *
ExecInitRemoteQuery(RemoteQuery *node, EState *estate, int eflags)
{
    RemoteQueryState   *remotestate;
    ResponseCombiner   *combiner;

    remotestate = makeNode(RemoteQueryState);
	remotestate->eflags = eflags;
    combiner = (ResponseCombiner *) remotestate;
    InitResponseCombiner(combiner, 0, node->combine_type);
    combiner->ss.ps.plan = (Plan *) node;
    combiner->ss.ps.state = estate;
    combiner->ss.ps.ExecProcNode = ExecRemoteQuery;

    combiner->ss.ps.qual = NULL;

    combiner->request_type = REQUEST_TYPE_QUERY;

    ExecInitResultTupleSlot(estate, &combiner->ss.ps);
    ExecAssignResultTypeFromTL((PlanState *) remotestate);

    /*
     * If there are parameters supplied, get them into a form to be sent to the
     * Datanodes with bind message. We should not have had done this before.
     */
    SetDataRowForExtParams(estate->es_param_list_info, remotestate);

    /* We need expression context to evaluate */
    if (node->exec_nodes && node->exec_nodes->en_expr)
    {
        Expr *expr = node->exec_nodes->en_expr;

        if (IsA(expr, Var) && ((Var *) expr)->vartype == TIDOID)
        {
            /* Special case if expression does not need to be evaluated */
        }
        else
        {
            /* prepare expression evaluation */
            ExecAssignExprContext(estate, &combiner->ss.ps);
        }
    }

    return remotestate;
}


/*
 * Execute step of PGXC plan.
 * The step specifies a command to be executed on specified nodes.
 * On first invocation connections to the data nodes are initialized and
 * command is executed. Further, as well as within subsequent invocations,
 * responses are received until step is completed or there is a tuple to emit.
 * If there is a tuple it is returned, otherwise returned NULL. The NULL result
 * from the function indicates completed step.
 * The function returns at most one tuple per invocation.
 */
TupleTableSlot *
ExecRemoteQuery(PlanState *pstate)
{// #lizard forgives
    RemoteQueryState *node = castNode(RemoteQueryState, pstate);
    ResponseCombiner *combiner = (ResponseCombiner *) node;
    RemoteQuery    *step = (RemoteQuery *) combiner->ss.ps.plan;
    TupleTableSlot *resultslot = combiner->ss.ps.ps_ResultTupleSlot;

    if (!node->query_Done)
    {
        GlobalTransactionId gxid = InvalidGlobalTransactionId;
        Snapshot        snapshot = GetActiveSnapshot();
        PGXCNodeHandle **connections = NULL;
        PGXCNodeHandle *primaryconnection = NULL;
        int                i;
        int                regular_conn_count = 0;
        int                total_conn_count = 0;
        bool            need_tran_block;
        PGXCNodeAllHandles *pgxc_connections;

        /*
         * Get connections for Datanodes only, utilities and DDLs
         * are launched in ExecRemoteUtility
         */
        pgxc_connections = get_exec_connections(node, step->exec_nodes,
                                                step->exec_type,
                                                true);

        if (step->exec_type == EXEC_ON_DATANODES)
        {
            connections = pgxc_connections->datanode_handles;
            total_conn_count = regular_conn_count = pgxc_connections->dn_conn_count;
#ifdef __TBASE__
			if (regular_conn_count > 1)
			{
				need_global_snapshot = true;
			}
			else if (regular_conn_count == 1 && !need_global_snapshot)
			{
				int nodeid = PGXCNodeGetNodeId(connections[0]->nodeoid, NULL);
				MemoryContext old = MemoryContextSwitchTo(TopTransactionContext);
				
				executed_node_list = list_append_unique_int(executed_node_list, nodeid);
				
				MemoryContextSwitchTo(old);

				if (list_length(executed_node_list) > 1)
				{
					need_global_snapshot = true;
				}
			}
#endif
        }
        else if (step->exec_type == EXEC_ON_COORDS)
        {
            connections = pgxc_connections->coord_handles;
            total_conn_count = regular_conn_count = pgxc_connections->co_conn_count;
#ifdef __TBASE__
            need_global_snapshot = true;
#endif
        }
		else if (step->exec_type == EXEC_ON_ALL_NODES)
		{
			total_conn_count = regular_conn_count =
				pgxc_connections->dn_conn_count + pgxc_connections->co_conn_count;
			
			connections = palloc(mul_size(total_conn_count, sizeof(PGXCNodeHandle *)));
			memcpy(connections, pgxc_connections->datanode_handles,
			       pgxc_connections->dn_conn_count * sizeof(PGXCNodeHandle *));
			memcpy(connections + pgxc_connections->dn_conn_count, pgxc_connections->coord_handles,
			       pgxc_connections->co_conn_count * sizeof(PGXCNodeHandle *));
			
			need_global_snapshot = g_set_global_snapshot;
		}

#ifdef __TBASE__
        /* set snapshot as needed */
        if (!g_set_global_snapshot && SetSnapshot(pstate->state))
        {
            snapshot = pstate->state->es_snapshot;
        }
#endif

        primaryconnection = pgxc_connections->primary_handle;

#ifdef __TBASE__
        /* initialize */
        combiner->recv_node_count = regular_conn_count;
        combiner->recv_tuples      = 0;
        combiner->recv_total_time = -1;
        combiner->recv_datarows = 0;
#endif
        /*
         * Primary connection is counted separately but is included in total_conn_count if used.
         */
        if (primaryconnection)
            regular_conn_count--;

        /*
         * We save only regular connections, at the time we exit the function
         * we finish with the primary connection and deal only with regular
         * connections on subsequent invocations
         */
        combiner->node_count = regular_conn_count;

		/*
		 * Start transaction on data nodes if we are in explicit transaction
		 * or going to use extended query protocol or write to multiple nodes
		 */
		if (step->force_autocommit)
			need_tran_block = false;
		else
			need_tran_block = step->cursor ||
					(!step->read_only && total_conn_count > 1) ||
					(TransactionBlockStatusCode() == 'T');

#ifdef __TBASE__
		/* Set plpgsql transaction begin for all connections */
		if (primaryconnection)
		{
			SetPlpgsqlTransactionBegin(primaryconnection);
		}

		for (i = 0; i < regular_conn_count; i++)
		{
			SetPlpgsqlTransactionBegin(connections[i]);
		}
#endif 
        stat_statement();
        stat_transaction(total_conn_count);

        gxid = GetCurrentTransactionIdIfAny();
        /* See if we have a primary node, execute on it first before the others */
        if (primaryconnection)
        {    
            //elog(LOG, "[PLPGSQL]ExecRemoteQuery has primaryconnection");
            //primaryconnection->read_only = true;
#ifdef __TBASE__
			combiner->connections = &primaryconnection;
			combiner->conn_count = 1;
			combiner->current_conn = 0;
#endif
			if (pgxc_node_begin(1, &primaryconnection, gxid, need_tran_block,
								step->read_only))
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Could not begin transaction on data node:%s.",
								 primaryconnection->nodename)));

			/* If explicit transaction is needed gxid is already sent */
			if (!pgxc_start_command_on_connection(primaryconnection,
												  node,
												  snapshot))
			{
				pgxc_node_remote_abort(TXN_TYPE_RollbackTxn, true);
				pfree_pgxc_all_handles(pgxc_connections);

				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send command to data nodes")));
			}
			Assert(combiner->combine_type == COMBINE_TYPE_SAME);

			pgxc_node_receive(1, &primaryconnection, NULL);
			/* Make sure the command is completed on the primary node */
			while (true)
			{
				int res = handle_response(primaryconnection, combiner);
				if (res == RESPONSE_READY)
					break;
				else if (res == RESPONSE_EOF)
					pgxc_node_receive(1, &primaryconnection, NULL);
				else if (res == RESPONSE_COMPLETE || res == RESPONSE_ERROR)
				{
					if (res == RESPONSE_COMPLETE &&
						primaryconnection->state == DN_CONNECTION_STATE_ERROR_FATAL)
					{
						ereport(ERROR,
								(errcode(ERRCODE_INTERNAL_ERROR),
								 errmsg("Unexpected FATAL ERROR on Connection to Datanode %s pid %d",
										primaryconnection->nodename,
										primaryconnection->backend_pid)));

					}
				    /* Get ReadyForQuery */
					continue;
				}
				else if (res == RESPONSE_ASSIGN_GXID)
					continue;
				else
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Unexpected response from data node")));
			}
			if (combiner->errorMessage)
				pgxc_node_report_error(combiner);
		}

#ifdef __TBASE__
        if (regular_conn_count > 0)
        {
            combiner->connections = connections;
            combiner->conn_count = regular_conn_count;
            combiner->current_conn = 0;
        }
#endif
        for (i = 0; i < regular_conn_count; i++)
        {
            //connections[i]->read_only = true;
#ifdef __TBASE__
			connections[i]->recv_datarows = 0;
#endif

			if (pgxc_node_begin(1, &connections[i], gxid, need_tran_block,
								step->read_only))
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Could not begin transaction on data node:%s.",
								 connections[i]->nodename)));

			/* If explicit transaction is needed gxid is already sent */
			if (!pgxc_start_command_on_connection(connections[i], node, snapshot))
			{
				pgxc_node_remote_abort(TXN_TYPE_RollbackTxn, true);
				pfree_pgxc_all_handles(pgxc_connections);
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send command to data nodes")));
			}
			connections[i]->combiner = combiner;
		}

		if (step->cursor)
		{
			int conn_size = regular_conn_count * sizeof(PGXCNodeHandle *);
			combiner->cursor = step->cursor;
			combiner->cursor_count = regular_conn_count;
			combiner->cursor_connections = (PGXCNodeHandle **)palloc(conn_size);
			memcpy(combiner->cursor_connections, connections, conn_size);
		}

        combiner->connections = connections;
        combiner->conn_count = regular_conn_count;
        combiner->current_conn = 0;

        if (combiner->cursor_count)
        {
            combiner->conn_count = combiner->cursor_count;
            memcpy(connections, combiner->cursor_connections,
                   combiner->cursor_count * sizeof(PGXCNodeHandle *));
            combiner->connections = connections;
        }

        node->query_Done = true;

        if (step->sort)
        {
            SimpleSort *sort = step->sort;

            /*
             * First message is already in the buffer
             * Further fetch will be under tuplesort control
             * If query does not produce rows tuplesort will not
             * be initialized
             */
            combiner->tuplesortstate = tuplesort_begin_merge(
                                   resultslot->tts_tupleDescriptor,
                                   sort->numCols,
                                   sort->sortColIdx,
                                   sort->sortOperators,
                                   sort->sortCollations,
                                   sort->nullsFirst,
                                   combiner,
                                   work_mem);
        }
    }

    if (combiner->tuplesortstate)
    {
        if (tuplesort_gettupleslot((Tuplesortstate *) combiner->tuplesortstate,
                                      true, true, resultslot, NULL))
            return resultslot;
        else
            ExecClearTuple(resultslot);
    }
    else
    {
        TupleTableSlot *slot = FetchTuple(combiner);
        if (!TupIsNull(slot))
            return slot;
    }

    if (combiner->errorMessage)
        pgxc_node_report_error(combiner);

#ifdef __TBASE__
    if (enable_statistic)
    {
        elog(LOG, "FetchTuple: recv_node_count:%d, recv_tuples:%lu, "
                    "recv_total_time:%ld, avg_time:%lf.",
                    combiner->recv_node_count,combiner->recv_tuples,
                    combiner->recv_total_time,
                    combiner->recv_tuples ? ((double)combiner->recv_total_time)/
                    ((double)combiner->recv_tuples) : -1);
    }
#endif
    return NULL;
}

/* ----------------------------------------------------------------
 *        ExecReScanRemoteQuery
 * ----------------------------------------------------------------
 */
void
ExecReScanRemoteQuery(RemoteQueryState *node)
{
    ResponseCombiner *combiner = (ResponseCombiner *)node;

    /*
     * If we haven't queried remote nodes yet, just return. If outerplan'
     * chgParam is not NULL then it will be re-scanned by ExecProcNode,
     * else - no reason to re-scan it at all.
     */
    if (!node->query_Done)
        return;

    /*
     * If we execute locally rescan local copy of the plan
     */
    if (outerPlanState(node))
        ExecReScan(outerPlanState(node));

    /*
     * Consume any possible pending input
     */
    pgxc_connections_cleanup(combiner);

    /* misc cleanup */
    combiner->command_complete_count = 0;
    combiner->description_count = 0;

    /*
     * Force query is re-bound with new parameters
     */
    node->query_Done = false;

}

/*
 * Clean up and discard any data on the data node connections that might not
 * handled yet, including pending on the remote connection.
 */
static void
pgxc_connections_cleanup(ResponseCombiner *combiner)
{// #lizard forgives    
    int32    ret     = 0;
    struct timeval timeout;
    timeout.tv_sec  = END_QUERY_TIMEOUT / 1000;
    timeout.tv_usec = (END_QUERY_TIMEOUT % 1000) * 1000;
            
    /* clean up the buffer */
    list_free_deep(combiner->rowBuffer);
    combiner->rowBuffer = NIL;
#ifdef __TBASE__
    /* clean up tuplestore */
    if (combiner->merge_sort)
    {
        if (combiner->dataRowBuffer)
        {
            int i = 0;

            for (i = 0; i < combiner->conn_count; i++)
            {
                if (combiner->dataRowBuffer[i])
                {
                    tuplestore_end(combiner->dataRowBuffer[i]);
                    combiner->dataRowBuffer[i] = NULL;
                }
            }

            pfree(combiner->dataRowBuffer);
            combiner->dataRowBuffer = NULL;
        }

        if (combiner->prerowBuffers)
        {
            int i = 0;

            for (i = 0; i < combiner->conn_count; i++)
            {
                if (combiner->prerowBuffers[i])
                {
                    list_free_deep(combiner->prerowBuffers[i]);
                    combiner->prerowBuffers[i] = NULL;
                }
            }

            pfree(combiner->prerowBuffers);
            combiner->prerowBuffers = NULL;
        }
    }
    else
    {
        if (combiner->dataRowBuffer && combiner->dataRowBuffer[0])
        {
            tuplestore_end(combiner->dataRowBuffer[0]);
            combiner->dataRowBuffer[0] = NULL;

            pfree(combiner->dataRowBuffer);
            combiner->dataRowBuffer = NULL;
        }
    }

    if (combiner->nDataRows)
    {
        pfree(combiner->nDataRows);
        combiner->nDataRows = NULL;
    }

    if (combiner->dataRowMemSize)
    {
        pfree(combiner->dataRowMemSize);
        combiner->dataRowMemSize = NULL;
    }
#endif

    /*
     * Read in and discard remaining data from the connections, if any
     */
    combiner->current_conn = 0;
    while (combiner->conn_count > 0)
    {
        int res;
        PGXCNodeHandle *conn = combiner->connections[combiner->current_conn];

        /*
         * Possible if we are doing merge sort.
         * We can do usual procedure and move connections around since we are
         * cleaning up and do not care what connection at what position
         */
        if (conn == NULL)
        {
            REMOVE_CURR_CONN(combiner);
            continue;
        }

        /* throw away current message that may be in the buffer */
        if (combiner->currentRow)
        {
            pfree(combiner->currentRow);
            combiner->currentRow = NULL;
        }

        ret = pgxc_node_receive(1, &conn, &timeout);
        if (ret)
        {
            switch (ret)
            {
                case DNStatus_ERR:                    
                    {
						elog(DEBUG1, "Failed to read response from data node:%s pid:%d when ending query for ERROR, node status:%d", conn->nodename, conn->backend_pid, conn->state);
                        break;
                    }
                    
                case DNStatus_EXPIRED:                            
                    {
                        elog(DEBUG1, "Failed to read response from data node:%s pid:%d when ending query for EXPIRED, node status:%d", conn->nodename, conn->backend_pid, conn->state);
                        break;
                    }    
                default:
                    {
                        /* Can not be here.*/
                        break;
                    }
            }
        }

        res = handle_response(conn, combiner);
        if (res == RESPONSE_EOF)
        {
            /* Continue to consume the data, until all connections are abosulately done. */
            if (pgxc_node_is_data_enqueued(conn) && !proc_exit_inprogress)
            {
                DNConnectionState state = DN_CONNECTION_STATE_IDLE;
                state       = conn->state;
                conn->state = DN_CONNECTION_STATE_QUERY;

                ret = pgxc_node_receive(1, &conn, &timeout);
                if (ret)
                {
                    switch (ret)
                    {
                        case DNStatus_ERR:                    
                            {
								elog(DEBUG1, "Failed to read response from data node:%u when ending query for ERROR, state:%d", conn->nodeoid, conn->state);
                                break;
                            }
                            
                        case DNStatus_EXPIRED:                            
                            {
                                elog(DEBUG1, "Failed to read response from data node:%u when ending query for EXPIRED, state:%d", conn->nodeoid, conn->state);
                                break;
                            }    
                        default:
                            {
                                /* Can not be here.*/
                                break;
                            }
                    }
                }
                
                if (DN_CONNECTION_STATE_IDLE == state)
                {
                    conn->state = state;
                }
                continue;
            }
            else if (!proc_exit_inprogress)
            {
                if (conn->state == DN_CONNECTION_STATE_QUERY)
                {
                    combiner->current_conn = (combiner->current_conn + 1) % combiner->conn_count;
                    continue;
                }
            }
        }
        else if (RESPONSE_ERROR == res)
        {
            if (conn->needSync)
            {                
                if (pgxc_node_send_sync(conn))
                {
                    ereport(LOG,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("Failed to sync msg to node:%s pid:%d when clean", conn->nodename, conn->backend_pid)));                                    
                }
                
#ifdef _PG_REGRESS_            
                ereport(LOG,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("Succeed to sync msg to node:%s pid:%d when clean", conn->nodename, conn->backend_pid)));                                                    
#endif
                conn->needSync = false;

            }
            continue;
        }
#ifdef __TBASE__
        else if (RESPONSE_COPY == res)
        {
            if (combiner->is_abort && conn->state == DN_CONNECTION_STATE_COPY_IN)
            {
                DataNodeCopyEnd(conn, true);
            }
        }
#endif
        
            
        /* no data is expected */
        if (conn->state == DN_CONNECTION_STATE_IDLE || conn->state == DN_CONNECTION_STATE_ERROR_FATAL)
        {
                        
#ifdef _PG_REGRESS_    
            int32 nbytes = pgxc_node_is_data_enqueued(conn);
            if (nbytes && conn->state == DN_CONNECTION_STATE_IDLE)
            {

                ereport(LOG,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("node:%s pid:%d status:%d %d bytes dataleft over.", conn->nodename, conn->backend_pid, conn->state, nbytes)));                                             

            }
#endif
            REMOVE_CURR_CONN(combiner);
            continue;
        }

        /*
         * Connection owner is different, so no our data pending at
         * the connection, nothing to read in.
         */
        if (conn->combiner && conn->combiner != combiner)
        {
            elog(LOG, "pgxc_connections_cleanup is different, remove connection:%s", conn->nodename);
            REMOVE_CURR_CONN(combiner);
            continue;
        }
        
    }

    /*
     * Release tuplesort resources
     */
    if (combiner->tuplesortstate)
    {
        /*
         * Free these before tuplesort_end, because these arrays may appear
         * in the tuplesort's memory context, tuplesort_end deletes this
         * context and may invalidate the memory.
         * We still want to free them here, because these may be in different
         * context.
         */
        if (combiner->tapenodes)
        {
            pfree(combiner->tapenodes);
            combiner->tapenodes = NULL;
        }
        if (combiner->tapemarks)
        {
            pfree(combiner->tapemarks);
            combiner->tapemarks = NULL;
        }
        /*
         * tuplesort_end invalidates minimal tuple if it is in the slot because
         * deletes the TupleSort memory context, causing seg fault later when
         * releasing tuple table
         */
        ExecClearTuple(combiner->ss.ps.ps_ResultTupleSlot);
        tuplesort_end((Tuplesortstate *) combiner->tuplesortstate);
        combiner->tuplesortstate = NULL;
    }
}


/*
 * End the remote query
 */
void
ExecEndRemoteQuery(RemoteQueryState *node)
{
    ResponseCombiner *combiner = (ResponseCombiner *) node;

    /*
     * Clean up remote connections
     */
    pgxc_connections_cleanup(combiner);

    /*
     * Clean up parameters if they were set, since plan may be reused
     */
    if (node->paramval_data)
    {
        pfree(node->paramval_data);
        node->paramval_data = NULL;
        node->paramval_len = 0;
    }

    CloseCombiner(combiner);
    pfree(node);
}


/**********************************************
 *
 * Routines to support RemoteSubplan plan node
 *
 **********************************************/


/*
 * The routine walks recursively over the plan tree and changes cursor names of
 * RemoteSubplan nodes to make them different from launched from the other
 * datanodes. The routine changes cursor names in place, so caller should
 * take writable copy of the plan tree.
 */
void
RemoteSubplanMakeUnique(Node *plan, int unique, int pid)
{
    if (plan == NULL)
        return;

    if (IsA(plan, List))
    {
        ListCell *lc;
        foreach(lc, (List *) plan)
        {
			RemoteSubplanMakeUnique(lfirst(lc), unique, pid);
        }
        return;
    }

    /*
     * Transform SharedQueue name
     */
    if (IsA(plan, RemoteSubplan))
    {
	    /*
	     * add node information and pid to make it unique
	     */
        ((RemoteSubplan *)plan)->unique = ((int64)unique << 32) | pid;
    }

    /* Otherwise it is a Plan descendant */
	RemoteSubplanMakeUnique((Node *) ((Plan *) plan)->lefttree, unique, pid);
	RemoteSubplanMakeUnique((Node *) ((Plan *) plan)->righttree, unique, pid);
    /* Tranform special cases */
    switch (nodeTag(plan))
    {
        case T_Append:
            RemoteSubplanMakeUnique((Node *) ((Append *) plan)->appendplans,
									unique, pid);
            break;
        case T_MergeAppend:
            RemoteSubplanMakeUnique((Node *) ((MergeAppend *) plan)->mergeplans,
									unique, pid);
            break;
        case T_BitmapAnd:
            RemoteSubplanMakeUnique((Node *) ((BitmapAnd *) plan)->bitmapplans,
									unique, pid);
            break;
        case T_BitmapOr:
            RemoteSubplanMakeUnique((Node *) ((BitmapOr *) plan)->bitmapplans,
									unique, pid);
            break;
        case T_SubqueryScan:
            RemoteSubplanMakeUnique((Node *) ((SubqueryScan *) plan)->subplan,
									unique, pid);
            break;
        default:
            break;
    }
}

static bool
determine_param_types_walker(Node *node, struct find_params_context *context)
{// #lizard forgives
    if (node == NULL)
        return false;

    if (IsA(node, Param))
    {
        Param *param = (Param *) node;
        int paramno = param->paramid;

        if (param->paramkind == PARAM_EXEC &&
                bms_is_member(paramno, context->defineParams))
        {
            RemoteParam *cur = context->rparams;
            while (cur->paramkind != PARAM_EXEC || cur->paramid != paramno)
                cur++;
            cur->paramtype = param->paramtype;
            context->defineParams = bms_del_member(context->defineParams,
                                                   paramno);
            return bms_is_empty(context->defineParams);
        }
    }

    if(IsA(node, SubPlan) && context->subplans)
    {
        Plan *plan;
        SubPlan *subplan = (SubPlan *)node;

        plan = (Plan *)list_nth(context->subplans, subplan->plan_id - 1);

        if (determine_param_types(plan, context))
            return true;
    }
        
    return expression_tree_walker(node, determine_param_types_walker,
                                  (void *) context);

}

/*
 * Scan expressions in the plan tree to find Param nodes and get data types
 * from them
 */
static bool
determine_param_types(Plan *plan,  struct find_params_context *context)
{// #lizard forgives
    Bitmapset *intersect;

    if (plan == NULL)
        return false;

    intersect = bms_intersect(plan->allParam, context->defineParams);
    if (bms_is_empty(intersect))
    {
        /* the subplan does not depend on params we are interested in */
        bms_free(intersect);
        return false;
    }
    bms_free(intersect);

    /* scan target list */
    if (expression_tree_walker((Node *) plan->targetlist,
                               determine_param_types_walker,
                               (void *) context))
        return true;
    /* scan qual */
    if (expression_tree_walker((Node *) plan->qual,
                               determine_param_types_walker,
                               (void *) context))
        return true;

    /* Check additional node-type-specific fields */
    switch (nodeTag(plan))
    {
        case T_Result:
            if (expression_tree_walker((Node *) ((Result *) plan)->resconstantqual,
                                       determine_param_types_walker,
                                       (void *) context))
                return true;
            break;

        case T_SeqScan:
        case T_SampleScan:
        case T_CteScan:
            break;

        case T_IndexScan:
            if (expression_tree_walker((Node *) ((IndexScan *) plan)->indexqual,
                                       determine_param_types_walker,
                                       (void *) context))
                return true;
            break;

        case T_IndexOnlyScan:
            if (expression_tree_walker((Node *) ((IndexOnlyScan *) plan)->indexqual,
                                       determine_param_types_walker,
                                       (void *) context))
                return true;
            break;

        case T_BitmapIndexScan:
            if (expression_tree_walker((Node *) ((BitmapIndexScan *) plan)->indexqual,
                                       determine_param_types_walker,
                                       (void *) context))
                return true;
            break;

        case T_BitmapHeapScan:
            if (expression_tree_walker((Node *) ((BitmapHeapScan *) plan)->bitmapqualorig,
                                       determine_param_types_walker,
                                       (void *) context))
                return true;
            break;

        case T_TidScan:
            if (expression_tree_walker((Node *) ((TidScan *) plan)->tidquals,
                                       determine_param_types_walker,
                                       (void *) context))
                return true;
            break;

        case T_SubqueryScan:
            if (determine_param_types(((SubqueryScan *) plan)->subplan, context))
                return true;
            break;

        case T_FunctionScan:
            if (expression_tree_walker((Node *) ((FunctionScan *) plan)->functions,
                                       determine_param_types_walker,
                                       (void *) context))
                return true;
            break;

        case T_ValuesScan:
            if (expression_tree_walker((Node *) ((ValuesScan *) plan)->values_lists,
                                       determine_param_types_walker,
                                       (void *) context))
                return true;
            break;

        case T_ModifyTable:
            {
                ListCell   *l;

                foreach(l, ((ModifyTable *) plan)->plans)
                {
                    if (determine_param_types((Plan *) lfirst(l), context))
                        return true;
                }
            }
            break;

        case T_RemoteSubplan:
            break;

        case T_Append:
            {
                ListCell   *l;

                foreach(l, ((Append *) plan)->appendplans)
                {
                    if (determine_param_types((Plan *) lfirst(l), context))
                        return true;
                }
            }
            break;

        case T_MergeAppend:
            {
                ListCell   *l;

                foreach(l, ((MergeAppend *) plan)->mergeplans)
                {
                    if (determine_param_types((Plan *) lfirst(l), context))
                        return true;
                }
            }
            break;

        case T_BitmapAnd:
            {
                ListCell   *l;

                foreach(l, ((BitmapAnd *) plan)->bitmapplans)
                {
                    if (determine_param_types((Plan *) lfirst(l), context))
                        return true;
                }
            }
            break;

        case T_BitmapOr:
            {
                ListCell   *l;

                foreach(l, ((BitmapOr *) plan)->bitmapplans)
                {
                    if (determine_param_types((Plan *) lfirst(l), context))
                        return true;
                }
            }
            break;

        case T_NestLoop:
            if (expression_tree_walker((Node *) ((Join *) plan)->joinqual,
                                       determine_param_types_walker,
                                       (void *) context))
                return true;
            break;

        case T_MergeJoin:
            if (expression_tree_walker((Node *) ((Join *) plan)->joinqual,
                                       determine_param_types_walker,
                                       (void *) context))
                return true;
            if (expression_tree_walker((Node *) ((MergeJoin *) plan)->mergeclauses,
                                       determine_param_types_walker,
                                       (void *) context))
                return true;
            break;

        case T_HashJoin:
            if (expression_tree_walker((Node *) ((Join *) plan)->joinqual,
                                       determine_param_types_walker,
                                       (void *) context))
                return true;
            if (expression_tree_walker((Node *) ((HashJoin *) plan)->hashclauses,
                                       determine_param_types_walker,
                                       (void *) context))
                return true;
            break;

        case T_Limit:
            if (expression_tree_walker((Node *) ((Limit *) plan)->limitOffset,
                                       determine_param_types_walker,
                                       (void *) context))
                return true;
            if (expression_tree_walker((Node *) ((Limit *) plan)->limitCount,
                                       determine_param_types_walker,
                                       (void *) context))
                return true;
            break;

        case T_RecursiveUnion:
            break;

        case T_LockRows:
            break;

        case T_WindowAgg:
            if (expression_tree_walker((Node *) ((WindowAgg *) plan)->startOffset,
                                       determine_param_types_walker,
                                       (void *) context))
            if (expression_tree_walker((Node *) ((WindowAgg *) plan)->endOffset,
                                       determine_param_types_walker,
                                       (void *) context))
            break;

        case T_Gather:
        case T_Hash:
        case T_Agg:
        case T_Material:
        case T_Sort:
        case T_Unique:
        case T_SetOp:
        case T_Group:
            break;

        default:
            elog(ERROR, "unrecognized node type: %d",
                 (int) nodeTag(plan));
    }

    /* check initplan if exists */
    if(plan->initPlan)
    {
        ListCell *l;

        foreach(l, plan->initPlan)
        {
            SubPlan  *subplan = (SubPlan *) lfirst(l);

            if(determine_param_types_walker((Node *)subplan, context))
                return true;
        }
    }

    /* recurse into subplans */
    return determine_param_types(plan->lefttree, context) ||
            determine_param_types(plan->righttree, context);
}


RemoteSubplanState *
ExecInitRemoteSubplan(RemoteSubplan *node, EState *estate, int eflags)
{// #lizard forgives
    RemoteStmt            rstmt;
    RemoteSubplanState *remotestate;
    ResponseCombiner   *combiner;
    CombineType            combineType;
    struct rusage        start_r;
    struct timeval        start_t;
#ifdef _MIGRATE_
    Oid                 groupid = InvalidOid;
    Oid                    reloid  = InvalidOid;
    ListCell *table;
#endif

    if (log_remotesubplan_stats)
        ResetUsageCommon(&start_r, &start_t);

#ifdef __AUDIT__
    memset((void *)(&rstmt), 0, sizeof(RemoteStmt));
#endif

    remotestate = makeNode(RemoteSubplanState);
    combiner = (ResponseCombiner *) remotestate;
    /*
     * We do not need to combine row counts if we will receive intermediate
     * results or if we won't return row count.
     */
    if (IS_PGXC_DATANODE || estate->es_plannedstmt->commandType == CMD_SELECT)
    {
        combineType = COMBINE_TYPE_NONE;
        remotestate->execOnAll = node->execOnAll;
    }
    else
    {
        if (node->execOnAll)
            combineType = COMBINE_TYPE_SUM;
        else
            combineType = COMBINE_TYPE_SAME;
        /*
         * If we are updating replicated table we should run plan on all nodes.
         * We are choosing single node only to read
         */
        remotestate->execOnAll = true;
    }
    remotestate->execNodes = list_copy(node->nodeList);
    InitResponseCombiner(combiner, 0, combineType);
    combiner->ss.ps.plan = (Plan *) node;
    combiner->ss.ps.state = estate;
    combiner->ss.ps.ExecProcNode = ExecRemoteSubplan;
#ifdef __TBASE__
	if (estate->es_instrument)
	{
		HASHCTL		ctl;
		
		ctl.keysize = sizeof(RemoteInstrKey);
		ctl.entrysize = sizeof(RemoteInstr);
		
		combiner->recv_instr_htbl = hash_create("Remote Instrument", 8 * NumDataNodes,
		                                        &ctl, HASH_ELEM | HASH_BLOBS);
	}
	combiner->remote_parallel_estimated = false;
#endif
    combiner->ss.ps.qual = NULL;

    combiner->request_type = REQUEST_TYPE_QUERY;

    ExecInitResultTupleSlot(estate, &combiner->ss.ps);
    ExecAssignResultTypeFromTL((PlanState *) remotestate);

#ifdef __TBASE__
    if (IS_PGXC_COORDINATOR && !g_set_global_snapshot)
    {
        if (!need_global_snapshot)
        {
            int node_count = list_length(node->nodeList);
            
            if (node_count > 1)
            {
                need_global_snapshot = true;
            }
            else if (node_count == 1)
            {
                MemoryContext old = MemoryContextSwitchTo(TopTransactionContext);
                executed_node_list = list_append_unique_int(executed_node_list, linitial_int(node->nodeList));
                MemoryContextSwitchTo(old);

                if (list_length(executed_node_list) > 1)
                {
                    need_global_snapshot = true;
                }
            }
        }
    }
#endif

    /*
     * We optimize execution if we going to send down query to next level
     */
    remotestate->local_exec = false;
    if (IS_PGXC_DATANODE)
    {
        if (remotestate->execNodes == NIL)
        {
            /*
             * Special case, if subplan is not distributed, like Result, or
             * query against catalog tables only.
             * We are only interested in filtering out the subplan results and
             * get only those we are interested in.
             * XXX we may want to prevent multiple executions in this case
             * either, to achieve this we will set single execNode on planning
             * time and this case would never happen, this code branch could
             * be removed.
             */
            remotestate->local_exec = true;
        }
        else if (!remotestate->execOnAll)
        {
            /*
             * XXX We should change planner and remove this flag.
             * We want only one node is producing the replicated result set,
             * and planner should choose that node - it is too hard to determine
             * right node at execution time, because it should be guaranteed
             * that all consumers make the same decision.
             * For now always execute replicated plan on local node to save
             * resources.
             */

            /*
             * Make sure local node is in execution list
             */
            if (list_member_int(remotestate->execNodes, PGXCNodeId-1))
            {
                list_free(remotestate->execNodes);
                remotestate->execNodes = NIL;
                remotestate->local_exec = true;
            }
            else
            {
                /*
                 * To support, we need to connect to some producer, so
                 * each producer should be prepared to serve rows for random
                 * number of consumers. It is hard, because new consumer may
                 * connect after producing is started, on the other hand,
                 * absence of expected consumer is a problem too.
                 */
                ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                         errmsg("Getting replicated results from remote node is not supported")));
            }
        }
    }

    /*
     * If we are going to execute subplan locally or doing explain initialize
     * the subplan. Otherwise have remote node doing that.
     */
    if (remotestate->local_exec || (eflags & EXEC_FLAG_EXPLAIN_ONLY))
    {
        outerPlanState(remotestate) = ExecInitNode(outerPlan(node), estate,
                                                   eflags);
        if (node->distributionNodes)
        {
            Oid         distributionType = InvalidOid;
            TupleDesc     typeInfo;

            typeInfo = combiner->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor;
            if (node->distributionKey != InvalidAttrNumber)
            {
                Form_pg_attribute attr;
                attr = typeInfo->attrs[node->distributionKey - 1];
                distributionType = attr->atttypid;
            }
            /* Set up locator */
#ifdef _MIGRATE_        
            foreach(table, estate->es_range_table)
            {
                RangeTblEntry *tbl_entry = (RangeTblEntry *)lfirst(table);
                if (tbl_entry->rtekind == RTE_RELATION)
                {
                    reloid = tbl_entry->relid;
                    elog(DEBUG5, "[ExecInitRemoteSubplan]reloid=%d", reloid);
                    break;
                }
            }
            groupid = GetRelGroup(reloid);
            elog(DEBUG5, "[ExecInitRemoteSubplan]groupid=%d", groupid);
    
            remotestate->locator = createLocator(node->distributionType,
                                                 RELATION_ACCESS_INSERT,
                                                 distributionType,
                                                 LOCATOR_LIST_LIST,
                                                 0,
                                                 (void *) node->distributionNodes,
                                                 (void **) &remotestate->dest_nodes,
                                                 false,
                                                 groupid, InvalidOid, InvalidOid, InvalidAttrNumber,
                                                 InvalidOid);
#else
            remotestate->locator = createLocator(node->distributionType,
                                                 RELATION_ACCESS_INSERT,
                                                 distributionType,
                                                 LOCATOR_LIST_LIST,
                                                 0,
                                                 (void *) node->distributionNodes,
                                                 (void **) &remotestate->dest_nodes,
                                                 false);
#endif
        }
        else
            remotestate->locator = NULL;
    }

    /*
     * Encode subplan if it will be sent to remote nodes
     */
    if (remotestate->execNodes && !(eflags & EXEC_FLAG_EXPLAIN_ONLY))
    {
        ParamListInfo ext_params;
        /* Encode plan if we are going to execute it on other nodes */
        rstmt.type = T_RemoteStmt;
        if (node->distributionType == LOCATOR_TYPE_NONE && IS_PGXC_DATANODE)
        {
            /*
             * There are cases when planner can not determine distribution of a
             * subplan, in particular it does not determine distribution of
             * subquery nodes. Such subplans executed from current location
             * (node) and combine all results, like from coordinator nodes.
             * However, if there are multiple locations where distributed
             * executor is running this node, and there are more of
             * RemoteSubplan plan nodes in the subtree there will be a problem -
             * Instances of the inner RemoteSubplan nodes will be using the same
             * SharedQueue, causing error. To avoid this problem we should
             * traverse the subtree and change SharedQueue name to make it
             * unique.
             */
			RemoteSubplanMakeUnique((Node *) outerPlan(node), PGXCNodeId - 1, MyProcPid);
            elog(DEBUG3, "RemoteSubplanMakeUnique for LOCATOR_TYPE_NONE unique: %d, cursor: %s",
                 PGXCNodeId, node->cursor);
        }
        rstmt.planTree = outerPlan(node);
        /*
         * If datanode launch further execution of a command it should tell
         * it is a SELECT, otherwise secondary data nodes won't return tuples
         * expecting there will be nothing to return.
         */
        if (IsA(outerPlan(node), ModifyTable))
        {
            rstmt.commandType = estate->es_plannedstmt->commandType;
            rstmt.hasReturning = estate->es_plannedstmt->hasReturning;
            rstmt.resultRelations = estate->es_plannedstmt->resultRelations;
        }
        else
        {
            rstmt.commandType = CMD_SELECT;
            rstmt.hasReturning = false;
            rstmt.resultRelations = NIL;
        }
        rstmt.rtable = estate->es_range_table;
        rstmt.subplans = estate->es_plannedstmt->subplans;
        rstmt.nParamExec = estate->es_plannedstmt->nParamExec;
        ext_params = estate->es_param_list_info;
        rstmt.nParamRemote = (ext_params ? ext_params->numParams : 0) +
                bms_num_members(node->scan.plan.allParam);
        if (rstmt.nParamRemote > 0)
        {
            Bitmapset *tmpset;
            int i;
            int paramno;

            /* Allocate enough space */
            rstmt.remoteparams = (RemoteParam *) palloc(rstmt.nParamRemote *
                                                        sizeof(RemoteParam));
            paramno = 0;
            if (ext_params)
            {
                for (i = 0; i < ext_params->numParams; i++)
                {
                    ParamExternData *param = &ext_params->params[i];
                    /*
                     * If parameter type is not yet defined but can be defined
                     * do that
                     */
                    if (!OidIsValid(param->ptype) && ext_params->paramFetch)
                        (*ext_params->paramFetch) (ext_params, i + 1);

                    /*
                     * If the parameter type is still not defined, assume that
                     * it is unused. But we put a default INT4OID type for such
                     * unused parameters to keep the parameter pushdown code
                     * happy.
                     *
                     * These unused parameters are never accessed during
                     * execution and we will just a null value for these
                     * "dummy" parameters. But including them here ensures that
                     * we send down the parameters in the correct order and at
                     * the position that the datanode needs
                     */
                    if (OidIsValid(param->ptype))
                    {
						rstmt.remoteparams[paramno].paramused =
							bms_is_member(i, node->initPlanParams) ? REMOTE_PARAM_INITPLAN : REMOTE_PARAM_SUBPLAN;
                        rstmt.remoteparams[paramno].paramtype = param->ptype;
                    }
                    else
                    {
						rstmt.remoteparams[paramno].paramused = REMOTE_PARAM_UNUSED;
                        rstmt.remoteparams[paramno].paramtype = INT4OID;
                    }

                    rstmt.remoteparams[paramno].paramkind = PARAM_EXTERN;
                    rstmt.remoteparams[paramno].paramid = i + 1;
                    paramno++;
                }
                /* store actual number of parameters */
                rstmt.nParamRemote = paramno;
            }

            if (!bms_is_empty(node->scan.plan.allParam))
            {
                Bitmapset *defineParams = NULL;
                tmpset = bms_copy(node->scan.plan.allParam);
                while ((i = bms_first_member(tmpset)) >= 0)
                {
                    ParamExecData *prmdata;

                    prmdata = &(estate->es_param_exec_vals[i]);
                    rstmt.remoteparams[paramno].paramkind = PARAM_EXEC;
                    rstmt.remoteparams[paramno].paramid = i;
                    rstmt.remoteparams[paramno].paramtype = prmdata->ptype;
					rstmt.remoteparams[paramno].paramused =
						bms_is_member(i, node->initPlanParams) ? REMOTE_PARAM_INITPLAN : REMOTE_PARAM_SUBPLAN;
                    /* Will scan plan tree to find out data type of the param */
                    if (prmdata->ptype == InvalidOid)
                        defineParams = bms_add_member(defineParams, i);
                    paramno++;
                }
                /* store actual number of parameters */
                rstmt.nParamRemote = paramno;
                bms_free(tmpset);
                if (!bms_is_empty(defineParams))
                {
                    struct find_params_context context;
                    bool all_found;

                    context.rparams = rstmt.remoteparams;
                    context.defineParams = defineParams;
                    context.subplans = estate->es_plannedstmt->subplans;

                    all_found = determine_param_types(node->scan.plan.lefttree,
                                                      &context);
                    /*
                     * Remove not defined params from the list of remote params.
                     * If they are not referenced no need to send them down
                     */
                    if (!all_found)
                    {
                        for (i = 0; i < rstmt.nParamRemote; i++)
                        {
                            if (rstmt.remoteparams[i].paramkind == PARAM_EXEC &&
                                    bms_is_member(rstmt.remoteparams[i].paramid,
                                                  context.defineParams))
                            {
                                /* Copy last parameter inplace */
                                rstmt.nParamRemote--;
                                if (i < rstmt.nParamRemote)
                                    rstmt.remoteparams[i] =
                                        rstmt.remoteparams[rstmt.nParamRemote];
                                /* keep current in the same position */
                                i--;
                            }
                        }
                    }
                    bms_free(context.defineParams);
                }
            }
            remotestate->nParamRemote = rstmt.nParamRemote;
            remotestate->remoteparams = rstmt.remoteparams;
        }
        else
            rstmt.remoteparams = NULL;
        rstmt.rowMarks = estate->es_plannedstmt->rowMarks;
        rstmt.distributionKey = node->distributionKey;
        rstmt.distributionType = node->distributionType;
        rstmt.distributionNodes = node->distributionNodes;
        rstmt.distributionRestrict = node->distributionRestrict;
#ifdef __TBASE__
        rstmt.parallelWorkerSendTuple = node->parallelWorkerSendTuple;
        if(IsParallelWorker())
        {
            rstmt.parallelModeNeeded = true;
        }
        else
        {
            rstmt.parallelModeNeeded = estate->es_plannedstmt->parallelModeNeeded;
        }

        if (estate->es_plannedstmt->haspart_tobe_modify)
        {
            rstmt.haspart_tobe_modify = estate->es_plannedstmt->haspart_tobe_modify;
            rstmt.partrelindex = estate->es_plannedstmt->partrelindex;
            rstmt.partpruning = bms_copy(estate->es_plannedstmt->partpruning);
        }
        else
        {
            rstmt.haspart_tobe_modify = false;
            rstmt.partrelindex = 0;
            rstmt.partpruning = NULL;
        }
#endif

        /*
         * A try-catch block to ensure that we don't leave behind a stale state
         * if nodeToString fails for whatever reason.
         *
         * XXX We should probably rewrite it someday by either passing a
         * context to nodeToString() or remembering this information somewhere
         * else which gets reset in case of errors. But for now, this seems
         * enough.
         */
        PG_TRY();
        {
            set_portable_output(true);
#ifdef __AUDIT__
            /* 
             * parseTree and queryString will be only send once while 
             * init the first RemoteSubplan in the whole plan tree
             */
            if (IS_PGXC_COORDINATOR && IsConnFromApp() && 
                estate->es_plannedstmt->parseTree != NULL &&
                estate->es_remote_subplan_num == 0)
            {
                rstmt.queryString = estate->es_sourceText;
                rstmt.parseTree = estate->es_plannedstmt->parseTree;
                estate->es_remote_subplan_num++;
            }
#endif
            remotestate->subplanstr = nodeToString(&rstmt);
#ifdef __AUDIT__
            rstmt.queryString = NULL;
            rstmt.parseTree = NULL;
			elog_node_display(DEBUG5, "SendPlanMessage", &rstmt, Debug_pretty_print);
#endif
        }
        PG_CATCH();
        {
            set_portable_output(false);
            PG_RE_THROW();
        }
        PG_END_TRY();
        set_portable_output(false);

        /*
         * Connect to remote nodes and send down subplan.
         */
        if (!(eflags & EXEC_FLAG_SUBPLAN))
        {
#ifdef __TBASE__
            remotestate->eflags = eflags;
            /* In parallel worker, no init, do it until we start to run. */
            if (IsParallelWorker())
            {
#endif
                
#ifdef __TBASE__
            }
            else
            {
                //ExecFinishInitRemoteSubplan(remotestate);
#ifdef __TBASE__
                /* Not parallel aware, build connections. */
                if (!node->scan.plan.parallel_aware)
                {
                    ExecFinishInitRemoteSubplan(remotestate);
                }
                else
                {
                    /* In session process, if we are under gather, do nothing. */
                }
#endif
            }
#endif
        }
    }
    remotestate->bound = false;
    /*
     * It does not makes sense to merge sort if there is only one tuple source.
     * By the contract it is already sorted
     */
    if (node->sort && remotestate->execOnAll &&
            list_length(remotestate->execNodes) > 1)
        combiner->merge_sort = true;

    if (log_remotesubplan_stats)
        ShowUsageCommon("ExecInitRemoteSubplan", &start_r, &start_t);

    return remotestate;
}


void
ExecFinishInitRemoteSubplan(RemoteSubplanState *node)
{// #lizard forgives
    ResponseCombiner   *combiner = (ResponseCombiner *) node;
    RemoteSubplan         *plan = (RemoteSubplan *) combiner->ss.ps.plan;
    EState               *estate = combiner->ss.ps.state;
    Oid                   *paramtypes = NULL;
    GlobalTransactionId gxid = InvalidGlobalTransactionId;
    Snapshot            snapshot;
    TimestampTz            timestamp;
    int                 i;
    bool                is_read_only;
    char                cursor[NAMEDATALEN];
    
#ifdef __TBASE__
    node->finish_init = true;
#endif
    /*
     * Name is required to store plan as a statement
     */
    Assert(plan->cursor);

    if (plan->unique)
		snprintf(cursor, NAMEDATALEN, "%s_"INT64_FORMAT, plan->cursor, plan->unique);
    else
        strncpy(cursor, plan->cursor, NAMEDATALEN);

    /* If it is alreaty fully initialized nothing to do */
    if (combiner->connections)
        return;

    /* local only or explain only execution */
    if (node->subplanstr == NULL)
        return;

    /* 
     * Check if any results are planned to be received here.
     * Otherwise it does not make sense to send out the subplan.
     */
    if (IS_PGXC_DATANODE && plan->distributionRestrict && 
            !list_member_int(plan->distributionRestrict, PGXCNodeId - 1))
        return;

    /*
     * Acquire connections and send down subplan where it will be stored
     * as a prepared statement.
     * That does not require transaction id or snapshot, so does not send them
     * here, postpone till bind.
     */
    if (node->execOnAll)
    {
        PGXCNodeAllHandles *pgxc_connections;
		pgxc_connections = get_handles(node->execNodes, NIL, false, true, true);
        combiner->conn_count = pgxc_connections->dn_conn_count;
        combiner->connections = pgxc_connections->datanode_handles;
        combiner->current_conn = 0;
        pfree(pgxc_connections);
    }
    else
    {
        combiner->connections = (PGXCNodeHandle **) palloc(sizeof(PGXCNodeHandle *));
        combiner->connections[0] = get_any_handle(node->execNodes);
        combiner->conn_count = 1;
        combiner->current_conn = 0;
    }

    gxid = GetCurrentTransactionIdIfAny();

    /* extract parameter data types */
    if (node->nParamRemote > 0)
    {
        paramtypes = (Oid *) palloc(node->nParamRemote * sizeof(Oid));
        for (i = 0; i < node->nParamRemote; i++)
            paramtypes[i] = node->remoteparams[i].paramtype;
    }
    /* send down subplan */
    snapshot = GetActiveSnapshot();
    timestamp = GetCurrentGTMStartTimestamp();

#ifdef __TBASE__
    /* set snapshot as needed */
    if (!g_set_global_snapshot && SetSnapshot(estate))
    {
        snapshot = estate->es_snapshot;
    }
#endif
    /*
     * Datanode should not send down statements that may modify
     * the database. Potgres assumes that all sessions under the same
     * postmaster have different xids. That may cause a locking problem.
     * Shared locks acquired for reading still work fine.
     */
    is_read_only = IS_PGXC_DATANODE ||
            !IsA(outerPlan(plan), ModifyTable);

#ifdef __TBASE__
	/* Set plpgsql transaction begin for all connections */
	for (i = 0; i < combiner->conn_count; i++)
	{
		SetPlpgsqlTransactionBegin(combiner->connections[i]);
	}
#endif 

#if 0
    for (i = 0; i < combiner->conn_count; i++)
    {
        PGXCNodeHandle *connection_tmp = combiner->connections[i];
        if (g_in_plpgsql_exec_fun && need_begin_txn)
        {
            connection_tmp->plpgsql_need_begin_txn = true;
            elog(LOG, "[PLPGSQL] ExecFinishInitRemoteSubplan conn nodename:%s backendpid:%d sock:%d need_begin_txn", 
                    connection_tmp->nodename, connection_tmp->backend_pid, connection_tmp->sock);
        }
         if (g_in_plpgsql_exec_fun && need_begin_sub_txn)
        {
            connection_tmp->plpgsql_need_begin_sub_txn = true;
            elog(LOG, "[PLPGSQL] ExecFinishInitRemoteSubplan conn nodename:%s backendpid:%d sock:%d need_begin_sub_txn", 
                    connection_tmp->nodename, connection_tmp->backend_pid, connection_tmp->sock);
        }
    }

    if (g_in_plpgsql_exec_fun && need_begin_txn)
    {
        need_begin_txn =  false;
        elog(LOG, "[PLPGSQL] ExecFinishInitRemoteSubplan need_begin_txn set to false");
    }
    if (g_in_plpgsql_exec_fun && need_begin_sub_txn)
    {
        need_begin_sub_txn = false;
        elog(LOG, "[PLPGSQL] ExecFinishInitRemoteSubplan need_begin_sub_txn set to false");
    }
#endif 

    for (i = 0; i < combiner->conn_count; i++)
    {
        PGXCNodeHandle *connection = combiner->connections[i];

		if (pgxc_node_begin(1, &connection, gxid, true, is_read_only))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Could not begin transaction on data node:%s.",
							 connection->nodename)));

        if (pgxc_node_send_timestamp(connection, timestamp))
        {
            combiner->conn_count = 0;
            pfree(combiner->connections);
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Failed to send command to data nodes")));
        }
        if (snapshot && pgxc_node_send_snapshot(connection, snapshot))
        {
            combiner->conn_count = 0;
            pfree(combiner->connections);
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Failed to send snapshot to data nodes")));
        }
        if (pgxc_node_send_cmd_id(connection, estate->es_snapshot->curcid) < 0 )
        {
            combiner->conn_count = 0;
            pfree(combiner->connections);
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Failed to send command ID to data nodes")));
        }
        pgxc_node_send_plan(connection, cursor, "Remote Subplan",
							node->subplanstr, node->nParamRemote, paramtypes, estate->es_instrument);

		if (enable_statistic)
		{
			elog(LOG, "Plan Message:pid:%d,remote_pid:%d,remote_ip:%s,"
					  "remote_port:%d,fd:%d,cursor:%s",
				      MyProcPid, connection->backend_pid, connection->nodehost,
					  connection->nodeport, connection->sock, cursor);
		}
		
		if (pgxc_node_flush(connection))
		{
			combiner->conn_count = 0;
			pfree(combiner->connections);
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to send subplan to data nodes")));
		}
	}
}


static void
append_param_data(StringInfo buf, Oid ptype, int pused, Datum value, bool isnull)
{
    uint32 n32;

    /* Assume unused parameters to have null values */
    if (!pused)
        ptype = INT4OID;

    if (isnull)
    {
        n32 = htonl(-1);
        appendBinaryStringInfo(buf, (char *) &n32, 4);
    }
    else
    {
        Oid        typOutput;
        bool    typIsVarlena;
        Datum    pval;
        char   *pstring;
        int        len;

        /* Get info needed to output the value */
        getTypeOutputInfo(ptype, &typOutput, &typIsVarlena);

        /*
         * If we have a toasted datum, forcibly detoast it here to avoid
         * memory leakage inside the type's output routine.
         */
        if (typIsVarlena)
            pval = PointerGetDatum(PG_DETOAST_DATUM(value));
        else
            pval = value;

        /* Convert Datum to string */
        pstring = OidOutputFunctionCall(typOutput, pval);

        /* copy data to the buffer */
        len = strlen(pstring);
        n32 = htonl(len);
        appendBinaryStringInfo(buf, (char *) &n32, 4);
        appendBinaryStringInfo(buf, pstring, len);
    }
}


static int
encode_parameters(int nparams, RemoteParam *remoteparams,
                             PlanState *planstate, char** result)
{
    EState            *estate = planstate->state;
    StringInfoData    buf;
    uint16             n16;
    int             i;
    ExprContext       *econtext;
    MemoryContext     oldcontext;

    if (planstate->ps_ExprContext == NULL)
        ExecAssignExprContext(estate, planstate);

    econtext = planstate->ps_ExprContext;
    oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
    MemoryContextReset(econtext->ecxt_per_tuple_memory);

    initStringInfo(&buf);

    /* Number of parameter values */
    n16 = htons(nparams);
    appendBinaryStringInfo(&buf, (char *) &n16, 2);

    /* Parameter values */
    for (i = 0; i < nparams; i++)
    {
        RemoteParam *rparam = &remoteparams[i];
        int ptype = rparam->paramtype;
        int pused = rparam->paramused;
        if (rparam->paramkind == PARAM_EXTERN)
        {
            ParamExternData *param;
            param = &(estate->es_param_list_info->params[rparam->paramid - 1]);
            append_param_data(&buf, ptype, pused, param->value, param->isnull);
        }
        else
        {
            ParamExecData *param;
            param = &(estate->es_param_exec_vals[rparam->paramid]);
            if (param->execPlan)
            {
                /* Parameter not evaluated yet, so go do it */
                ExecSetParamPlan((SubPlanState *) param->execPlan,
                                 planstate->ps_ExprContext);
                /* ExecSetParamPlan should have processed this param... */
                Assert(param->execPlan == NULL);
            }
            if (!param->done)
                param->isnull = true;
            append_param_data(&buf, ptype, pused, param->value, param->isnull);

        }
    }

    /* Take data from the buffer */
    *result = palloc(buf.len);
    memcpy(*result, buf.data, buf.len);
    MemoryContextSwitchTo(oldcontext);
    return buf.len;
}

/*
 * Encode executor context for EvalPlanQual process including:
 * the number of epqTuples, the ctid and xc_node_id of each tuple.
 */
static int
encode_epqcontext(PlanState *planstate, char **result)
{
	EState 		   *estate = planstate->state;
	StringInfoData	buf;
	uint16 			n16;
	uint32          n32;
	int             ntuples = list_length(estate->es_range_table);
	int             i;
	ExprContext	   *econtext;
	MemoryContext 	oldcontext;
	
	if (planstate->ps_ExprContext == NULL)
		ExecAssignExprContext(estate, planstate);
	
	econtext = planstate->ps_ExprContext;
	oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
	
	initStringInfo(&buf);
	
	/* Number of epq tuples */
	n16 = htons(ntuples);
	appendBinaryStringInfo(&buf, (char *) &n16, 2);
	
	for (i = 0; i < ntuples; i++)
	{
		ItemPointerData tid;
		int16           rtidx;
		int             nodeid;
		
		if (estate->es_epqTuple[i] == NULL)
		{
			memset(&tid, 0, sizeof(ItemPointerData));
			rtidx = 0;
			nodeid = 0;
		}
		else
		{
			tid = estate->es_epqTuple[i]->t_self;
			rtidx = i + 1;
			nodeid = estate->es_epqTuple[i]->t_xc_node_id;
		}
		
		n16 = htons(rtidx);
		appendBinaryStringInfo(&buf, (char *) &n16, 2);
		n16 = htons(tid.ip_blkid.bi_hi);
		appendBinaryStringInfo(&buf, (char *) &n16, 2);
		n16 = htons(tid.ip_blkid.bi_lo);
		appendBinaryStringInfo(&buf, (char *) &n16, 2);
		n16 = htons(tid.ip_posid);
		appendBinaryStringInfo(&buf, (char *) &n16, 2);
		n32 = htonl(nodeid);
		appendBinaryStringInfo(&buf, (char *) &n32, 4);
	}
	
	/* Take data from the buffer */
	*result = palloc(buf.len);
	memcpy(*result, buf.data, buf.len);
	MemoryContextSwitchTo(oldcontext);
	return buf.len;
}

TupleTableSlot *
ExecRemoteSubplan(PlanState *pstate)
{// #lizard forgives
    RemoteSubplanState *node = castNode(RemoteSubplanState, pstate);
    ResponseCombiner *combiner = (ResponseCombiner *) node;
    RemoteSubplan  *plan = (RemoteSubplan *) combiner->ss.ps.plan;
    EState           *estate = combiner->ss.ps.state;
    TupleTableSlot *resultslot = combiner->ss.ps.ps_ResultTupleSlot;
    struct rusage    start_r;
    struct timeval        start_t;
#ifdef __TBASE__
    int count = 0;
#endif
#ifdef __TBASE__
	if ((node->eflags & EXEC_FLAG_EXPLAIN_ONLY) != 0)
		return NULL;
	
    if (!node->local_exec && (!node->finish_init) && (!(node->eflags & EXEC_FLAG_SUBPLAN)))
    {
        if(node->execNodes)
        {
            ExecFinishInitRemoteSubplan(node);
        }
        else
        {
            return NULL;
        }
    }
#endif
    /* 
     * We allow combiner->conn_count == 0 after node initialization
     * if we figured out that current node won't receive any result
     * because of distributionRestrict is set by planner.
     * But we should distinguish this case from others, when conn_count is 0.
     * That is possible if local execution is chosen or data are buffered 
     * at the coordinator or data are exhausted and node was reset.
     * in last two cases connections are saved to cursor_connections and we
     * can check their presence.  
     */
    if (!node->local_exec && combiner->conn_count == 0 && 
            combiner->cursor_count == 0)
        return NULL;

    if (log_remotesubplan_stats)
        ResetUsageCommon(&start_r, &start_t);

primary_mode_phase_two:
    if (!node->bound)
    {
        int fetch = 0;
        int paramlen = 0;
		int epqctxlen = 0;
        char *paramdata = NULL;
		char *epqctxdata = NULL;
		
        /*
         * Conditions when we want to execute query on the primary node first:
         * Coordinator running replicated ModifyTable on multiple nodes
         */
        bool primary_mode = combiner->probing_primary ||
                (IS_PGXC_COORDINATOR &&
                 combiner->combine_type == COMBINE_TYPE_SAME &&
                 OidIsValid(primary_data_node) &&
                 combiner->conn_count > 1 && !g_UseDataPump);
        char cursor[NAMEDATALEN];
#ifdef __TBASE__
		StringInfo shardmap = NULL;
#endif

        if (plan->cursor)
        {
            fetch = PGXLRemoteFetchSize;
            if (plan->unique)
				snprintf(cursor, NAMEDATALEN, "%s_"INT64_FORMAT, plan->cursor, plan->unique);
            else
                strncpy(cursor, plan->cursor, NAMEDATALEN);
        }
        else
            cursor[0] = '\0';

#ifdef __TBASE__
        if(g_UseDataPump)
        {
            /* fetch all */
            fetch = 0;
        }

        /* get connection's count and handle */
        if (combiner->conn_count)
        {
            count = combiner->conn_count;
        }
        else
        {
            if (combiner->cursor)
            {
                if (!combiner->probing_primary)
                {
                    count = combiner->cursor_count;
                }
                else
                {
                    count = combiner->conn_count;
                }
            }
        }

        /* initialize */
        combiner->recv_node_count = count;
        combiner->recv_tuples     = 0;
        combiner->recv_total_time = -1;
        combiner->recv_datarows = 0;
#endif

        /*
         * Send down all available parameters, if any is used by the plan
         */
        if (estate->es_param_list_info ||
                !bms_is_empty(plan->scan.plan.allParam))
            paramlen = encode_parameters(node->nParamRemote,
                                         node->remoteparams,
                                         &combiner->ss.ps,
                                         &paramdata);

		if (estate->es_epqTuple != NULL)
			epqctxlen = encode_epqcontext(&combiner->ss.ps, &epqctxdata);

#ifdef __TBASE__
		/*
		 * consider whether to distribute shard map info
		 * we do that when:
		 *  1. this is a DN node
		 *  2. plan distribution is by shard
		 *  3. target of distribution is not in our group
		 */
		if (IS_PGXC_DATANODE && node->execNodes != NIL &&
		    plan->distributionType == LOCATOR_TYPE_SHARD)
		{
			ListCell *cell;
			
			foreach(cell, node->execNodes)
			{
				if (!list_member_int(PGXCGroupNodeList, lfirst_int(cell)))
					shardmap = SerializeShardmap();
			}
		}
#endif
        /*
         * The subplan being rescanned, need to restore connections and
         * re-bind the portal
         */
        if (combiner->cursor)
        {
            int i;

            /*
             * On second phase of primary mode connections are properly set,
             * so do not copy.
             */
            if (!combiner->probing_primary)
            {
                combiner->conn_count = combiner->cursor_count;
                memcpy(combiner->connections, combiner->cursor_connections,
                            combiner->cursor_count * sizeof(PGXCNodeHandle *));
            }

            for (i = 0; i < combiner->conn_count; i++)
            {
                PGXCNodeHandle *conn = combiner->connections[i];

                CHECK_OWNERSHIP(conn, combiner);

                /* close previous cursor only on phase 1 */
                if (!primary_mode || !combiner->probing_primary)
                    pgxc_node_send_close(conn, false, combiner->cursor);

                /*
                 * If we now should probe primary, skip execution on non-primary
                 * nodes
                 */
                if (primary_mode && !combiner->probing_primary &&
                        conn->nodeoid != primary_data_node)
                    continue;

                /* rebind */
                pgxc_node_send_bind(conn, combiner->cursor, combiner->cursor,
									paramlen, paramdata, epqctxlen, epqctxdata, shardmap);
                if (enable_statistic)
                {
                    elog(LOG, "Bind Message:pid:%d,remote_pid:%d,remote_ip:%s,remote_port:%d,fd:%d,cursor:%s",
                              MyProcPid, conn->backend_pid, conn->nodehost, conn->nodeport, conn->sock, cursor);
                }
                /* execute */
                pgxc_node_send_execute(conn, combiner->cursor, fetch);
                /* submit */
                if (pgxc_node_send_flush(conn))
                {
                    combiner->conn_count = 0;
                    pfree(combiner->connections);
                    ereport(ERROR,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("Failed to send command to data nodes")));
                }

                /*
                 * There could be only one primary node, but can not leave the
                 * loop now, because we need to close cursors.
                 */
                if (primary_mode && !combiner->probing_primary)
                {
                    combiner->current_conn = i;
                }
            }
        }
        else if (node->execNodes)
        {
            CommandId        cid;
            int             i;

            /*
             * There are prepared statement, connections should be already here
             */
            Assert(combiner->conn_count > 0);

            combiner->extended_query = true;
            cid = estate->es_snapshot->curcid;

            for (i = 0; i < combiner->conn_count; i++)
            {
                PGXCNodeHandle *conn = combiner->connections[i];

#ifdef __TBASE__
                conn->recv_datarows = 0;
#endif

                CHECK_OWNERSHIP(conn, combiner);

                /*
                 * If we now should probe primary, skip execution on non-primary
                 * nodes
                 */
                if (primary_mode && !combiner->probing_primary &&
                        conn->nodeoid != primary_data_node)
                    continue;

                /*
                 * Update Command Id. Other command may be executed after we
                 * prepare and advanced Command Id. We should use one that
                 * was active at the moment when command started.
                 */
                if (pgxc_node_send_cmd_id(conn, cid))
                {
                    combiner->conn_count = 0;
                    pfree(combiner->connections);
                    ereport(ERROR,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("Failed to send command ID to data nodes")));
                }

                /*
                 * Resend the snapshot as well since the connection may have
                 * been buffered and use by other commands, with different
                 * snapshot. Set the snapshot back to what it was
                 */
                if (pgxc_node_send_snapshot(conn, estate->es_snapshot))
                {
                    combiner->conn_count = 0;
                    pfree(combiner->connections);
                    ereport(ERROR,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("Failed to send snapshot to data nodes")));
                }

                /* bind */
				pgxc_node_send_bind(conn, cursor, cursor, paramlen, paramdata,
				                    epqctxlen, epqctxdata, shardmap);

                if (enable_statistic)
                {
                    elog(LOG, "Bind Message:pid:%d,remote_pid:%d,remote_ip:%s,remote_port:%d,fd:%d,cursor:%s",
                              MyProcPid, conn->backend_pid, conn->nodehost, conn->nodeport, conn->sock, cursor);
                }
                /* execute */
                pgxc_node_send_execute(conn, cursor, fetch);

                /* submit */
                if (pgxc_node_send_flush(conn))
                {
                    combiner->conn_count = 0;
                    pfree(combiner->connections);
                    ereport(ERROR,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("Failed to send command to data nodes")));
                }

                /*
                 * There could be only one primary node, so if we executed
                 * subquery on the phase one of primary mode we can leave the
                 * loop now.
                 */
                if (primary_mode && !combiner->probing_primary)
                {
                    combiner->current_conn = i;
                    break;
                }
            }

            /*
             * On second phase of primary mode connections are backed up
             * already, so do not copy.
             */
            if (primary_mode)
            {
                if (combiner->probing_primary)
                {
                    combiner->cursor = pstrdup(cursor);
                }
                else
                {
                    combiner->cursor = pstrdup(cursor);
                    combiner->cursor_count = combiner->conn_count;
                    combiner->cursor_connections = (PGXCNodeHandle **) palloc(
                                combiner->conn_count * sizeof(PGXCNodeHandle *));
                    memcpy(combiner->cursor_connections, combiner->connections,
                                combiner->conn_count * sizeof(PGXCNodeHandle *));
                }
            }
            else
            {
                combiner->cursor = pstrdup(cursor);
                combiner->cursor_count = combiner->conn_count;
                combiner->cursor_connections = (PGXCNodeHandle **) palloc(
                            combiner->conn_count * sizeof(PGXCNodeHandle *));
                memcpy(combiner->cursor_connections, combiner->connections,
                            combiner->conn_count * sizeof(PGXCNodeHandle *));
            }
        }

        if (combiner->merge_sort)
        {
            /*
             * Requests are already made and sorter can fetch tuples to populate
             * sort buffer.
             */
            combiner->tuplesortstate = tuplesort_begin_merge(
                                       resultslot->tts_tupleDescriptor,
                                       plan->sort->numCols,
                                       plan->sort->sortColIdx,
                                       plan->sort->sortOperators,
                                       plan->sort->sortCollations,
                                       plan->sort->nullsFirst,
                                       combiner,
                                       work_mem);
        }
        if (primary_mode)
        {
            if (combiner->probing_primary)
            {
                combiner->probing_primary = false;
                node->bound = true;
            }
            else
                combiner->probing_primary = true;
        }
        else
            node->bound = true;
    }

    if (combiner->tuplesortstate)
    {
        if (tuplesort_gettupleslot((Tuplesortstate *) combiner->tuplesortstate,
                                   true, true, resultslot, NULL))
        {
            if (log_remotesubplan_stats)
                ShowUsageCommon("ExecRemoteSubplan", &start_r, &start_t);
            return resultslot;
        }
    }
    else
    {
        TupleTableSlot *slot = FetchTuple(combiner);
        if (!TupIsNull(slot))
        {
            if (log_remotesubplan_stats)
                ShowUsageCommon("ExecRemoteSubplan", &start_r, &start_t);
            return slot;
        }
        else if (combiner->probing_primary)
            /* phase1 is successfully completed, run on other nodes */
            goto primary_mode_phase_two;
    }
    if (combiner->errorMessage)
        pgxc_node_report_error(combiner);

    if (log_remotesubplan_stats)
        ShowUsageCommon("ExecRemoteSubplan", &start_r, &start_t);

#ifdef __TBASE__
    if (enable_statistic)
    {
        elog(LOG, "FetchTuple: recv_node_count:%d, recv_tuples:%lu, "
                    "recv_total_time:%ld, avg_time:%lf.",
                    combiner->recv_node_count,combiner->recv_tuples,
                    combiner->recv_total_time,
                    combiner->recv_tuples ? ((double)combiner->recv_total_time)/
                    ((double)combiner->recv_tuples) : -1);
    }
#endif
    return NULL;
}


void
ExecReScanRemoteSubplan(RemoteSubplanState *node)
{
    ResponseCombiner *combiner = (ResponseCombiner *)node;

    /*
     * If we haven't queried remote nodes yet, just return. If outerplan'
     * chgParam is not NULL then it will be re-scanned by ExecProcNode,
     * else - no reason to re-scan it at all.
     */
    if (!node->bound)
        return;

    /*
     * If we execute locally rescan local copy of the plan
     */
    if (outerPlanState(node))
        ExecReScan(outerPlanState(node));

    /*
     * Consume any possible pending input
     */
    pgxc_connections_cleanup(combiner);

    /* misc cleanup */
    combiner->command_complete_count = 0;
    combiner->description_count = 0;

    /*
     * Force query is re-bound with new parameters
     */
    node->bound = false;
#ifdef __TBASE__
    node->eflags &= ~(EXEC_FLAG_DISCONN);
#endif
}

#ifdef __TBASE__
/*
 * ExecShutdownRemoteSubplan
 * 
 * for instrumentation only, init full planstate tree,
 * then attach recieved remote instrumenation.
 */
void
ExecShutdownRemoteSubplan(RemoteSubplanState *node)
{
	ResponseCombiner    *combiner = &node->combiner;
	PlanState           *ps = &combiner->ss.ps;
	Plan                *plan = ps->plan;
	EState              *estate = ps->state;
	
	/* do nothing if explain only or execute locally */
	if ((node->eflags & EXEC_FLAG_EXPLAIN_ONLY) != 0 || node->local_exec)
		return;
	
	elog(DEBUG1, "shutdown remote subplan worker %d, plan_node_id %d", ParallelWorkerNumber, plan->plan_node_id);
	
	if (estate->es_instrument)
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);
		AttachRemoteInstrContext ctx;
		
		if (!ps->lefttree)
			ps->lefttree = ExecInitNode(plan->lefttree, estate, EXEC_FLAG_EXPLAIN_ONLY);

		ctx.htab = combiner->recv_instr_htbl;
		ctx.node_idx_List = ((RemoteSubplan *) plan)->nodeList;
		ctx.printed_nodes = NULL;
		AttachRemoteInstr(ps->lefttree, &ctx);
		
		MemoryContextSwitchTo(oldcontext);
	}
}

void
ExecFinishRemoteSubplan(RemoteSubplanState *node)
{// #lizard forgives
    ResponseCombiner *combiner = (ResponseCombiner *)node;
    RemoteSubplan    *plan = (RemoteSubplan *) combiner->ss.ps.plan;
    int i;
    int dn_count = 0;
    int *dn_list = NULL;
    char cursor[NAMEDATALEN];

	if ((node->eflags & EXEC_FLAG_EXPLAIN_ONLY) != 0)
		return;
	
    if (!node->bound)
    {
        if (g_DataPumpDebug)
        {
            elog(LOG, "ExecFinishRemoteSubplan: pid %d not bound, ExecDisconnectRemoteSubplan.", MyProcPid);
        }
        
        ExecDisconnectRemoteSubplan(node);
    }
    else
    {    
        dn_list = (int*)palloc(sizeof(int) * NumDataNodes);

        if (plan->cursor)
        {
            if (plan->unique)
				snprintf(cursor, NAMEDATALEN, "%s_"INT64_FORMAT, plan->cursor, plan->unique);
            else
                strncpy(cursor, plan->cursor, NAMEDATALEN);
        }
        else
            cursor[0] = '\0';
        
        if (g_DataPumpDebug)
        {
            elog(LOG, "ExecFinishRemoteSubplan: pid %d cursor %s send signal sigusr2 to end query.", MyProcPid, cursor);
        }

        for (i = 0; i < combiner->conn_count; i++)
        {
            PGXCNodeHandle *conn = combiner->connections[i];

            if (conn)
            {
                dn_list[dn_count++] = PGXCNodeGetNodeId(conn->nodeoid, NULL);    
            }
        }

        if (dn_count)
        {
            bool ret = PoolManagerCancelQuery(dn_count, 
                                              dn_list, 
                                              0,
                                              NULL,
                                              SIGNAL_SIGUSR2);
            if (!ret)
            {
                elog(LOG, "ExecFinishRemoteSubplan %d with cursor %s failed.", MyProcPid, cursor);
            }

            if (g_DataPumpDebug)
            {
                elog(LOG, "ExecFinishRemoteSubplan %d with cursor %s, dn_count %d", MyProcPid, cursor, dn_count);
            }
        }

        if (dn_count)
        {
            pfree(dn_list);
            dn_list = NULL;
        }

        /* consume left data in socket */
        pgxc_connections_cleanup(combiner);
    }
}

void
ExecDisconnectRemoteSubplan(RemoteSubplanState *node)
{// #lizard forgives
    ResponseCombiner *combiner = (ResponseCombiner *)node;
    RemoteSubplan    *plan = (RemoteSubplan *) combiner->ss.ps.plan;
    int i;
    int count;
    PGXCNodeHandle **connections = NULL;

    if (list_length(plan->distributionRestrict) > 1)
    {
        if (!node->finish_init)
        {
            ExecFinishInitRemoteSubplan(node);
#if 0
            PGXCNodeAllHandles *pgxc_connections;
            pgxc_connections = get_handles(node->execNodes, NIL, false, true);
            combiner->conn_count = pgxc_connections->dn_conn_count;
            combiner->connections = pgxc_connections->datanode_handles;
            combiner->current_conn = 0;
            pfree(pgxc_connections);
#endif
        }

        combiner->extended_query = true;
        
        for (i = 0; i < combiner->conn_count; i++)
        {
            PGXCNodeHandle *conn;
            char            cursor[NAMEDATALEN];

            if (plan->cursor)
            {
                if (plan->unique)
					snprintf(cursor, NAMEDATALEN, "%s_"INT64_FORMAT, plan->cursor, plan->unique);
                else
                    strncpy(cursor, plan->cursor, NAMEDATALEN);
            }
            else
                cursor[0] = '\0';

            conn = combiner->connections[i];

            if (conn)
            {
                CHECK_OWNERSHIP(conn, combiner);
				
                if (pgxc_node_send_disconnect(conn, cursor, list_length(plan->distributionRestrict)) != 0)
                    ereport(ERROR,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("Failed to disconnect from node %d.", conn->nodeoid)));
				if (g_DataPumpDebug)
				{
					elog(LOG, "send disconnect to node %d with cursor %s, remote_pid %d, nconsumers %d.", conn->nodeoid, cursor,
						       conn->backend_pid, list_length(plan->distributionRestrict));
				}
            }
        }

        node->bound = true;
        node->eflags |= EXEC_FLAG_DISCONN;

        connections = (PGXCNodeHandle **)palloc(combiner->conn_count * sizeof(PGXCNodeHandle *));

        memcpy(connections, combiner->connections,
            combiner->conn_count * sizeof(PGXCNodeHandle *));
        count = combiner->conn_count;

        while (count > 0)
        {
            if (pgxc_node_receive(count,
                                  connections, NULL))
                ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("Failed to disconnect remote subplan")));
            i = 0;
            while (i < count)
            {
                int res = handle_response(connections[i], combiner);
                if (res == RESPONSE_EOF)
                {
                    i++;
                }
                else if (res == RESPONSE_READY ||
                         res == RESPONSE_COMPLETE)
                {
                    /* Done, connection is reade for query */
                    if (--count > i)
                        connections[i] =
                                connections[count];
                }
                else if (res == RESPONSE_DATAROW)
                {
                    /*
                     * If we are finishing slowly running remote subplan while it
                     * is still working (because of Limit, for example) it may
                     * produce one or more tuples between connection cleanup and
                     * handling Close command. One tuple does not cause any problem,
                     * but if it will not be read the next tuple will trigger
                     * assertion failure. So if we got a tuple, just read and
                     * discard it here.
                     */
                    pfree(combiner->currentRow);
                    combiner->currentRow = NULL;
                }
                else if (RESPONSE_ERROR == res)
                {
                    if (connections[i]->needSync)
                    {                
                        if (pgxc_node_send_sync(connections[i]))
                        {
                            ereport(LOG,
                                    (errcode(ERRCODE_INTERNAL_ERROR),
                                     errmsg("Failed to sync msg to node:%s pid:%d when clean", connections[i]->nodename, connections[i]->backend_pid)));                                    
                        }
                        
                        connections[i]->needSync = false;

                    }
                    i++;
                }
                /* Ignore other possible responses */
            }
        }

        pfree(connections);
    }
}

/* Abort the connections, ensure the connections are clean and empty. */
void pgxc_abort_connections(PGXCNodeAllHandles *all_handles)
{// #lizard forgives    
    int  ret             = false;
    int  i                  = 0;
    bool need_loop_check = false;
    bool need_sync = true;
	int read_status;

    if (all_handles)
    {                
        while (1)
        {
            need_loop_check = false;
            elog(DEBUG1, "pgxc_abort_connections %d coordinator to handle", all_handles->co_conn_count);
            for (i = 0; i < all_handles->co_conn_count; i++)
            {
                PGXCNodeHandle *handle = all_handles->coord_handles[i];
				if (handle->sock != NO_SOCKET)
                {
                    if (handle->state != DN_CONNECTION_STATE_IDLE || !node_ready_for_query(handle) || pgxc_node_is_data_enqueued(handle))
                    {
                        elog(DEBUG1, "pgxc_abort_connections node:%s not ready for query, status:%d", handle->nodename, handle->state);
						if (handle->sock != NO_SOCKET)
                        {
							read_status = pgxc_node_flush_read(handle);
							if (read_status == EOF || read_status < 0)
							{
								/* Can not read - no more actions, just discard connection */
								handle->state = DN_CONNECTION_STATE_ERROR_FATAL;
								add_error_message(handle, "unexpected EOF on datanode connection.");
								elog(LOG, "unexpected EOF on node:%s pid:%d, read_status:%d, EOF:%d",
										handle->nodename, handle->backend_pid, read_status, EOF);
								return;
							}

                            handle->state = DN_CONNECTION_STATE_IDLE;
                        }                
                        /* Clear any previous error messages */
                        handle->error[0] = '\0';
                    }
                    else
                    {
                        elog(DEBUG1, "pgxc_abort_connections node:%s ready for query", handle->nodename);
                    }
                }
                else 
                {
                    if (handle->nodename[0] != '\0')
                    {
                        elog(WARNING, "pgxc_abort_connections cn node:%s invalid socket %u!", handle->nodename, handle->sock);
                    }
                    else
                    {
                        elog(WARNING, "pgxc_abort_connections cn node, invalid socket %u!", handle->sock);    
                    }
                }
                
            }
            
            /*
             * The same for datanode nodes.
             */
            elog(DEBUG1, "pgxc_abort_connections %d datanode to handle", all_handles->dn_conn_count);
            for (i = 0; i < all_handles->dn_conn_count; i++)
            {
                PGXCNodeHandle *handle = all_handles->datanode_handles[i];
				if (handle->sock != NO_SOCKET)
                {
                    if (handle->state != DN_CONNECTION_STATE_IDLE || !node_ready_for_query(handle) || pgxc_node_is_data_enqueued(handle))
                    {
                        elog(DEBUG1, "pgxc_abort_connections node:%s not ready for query, status:%d", handle->nodename, handle->state);
						read_status = pgxc_node_flush_read(handle);
						if (read_status == EOF || read_status < 0)
						{
							/* Can not read - no more actions, just discard connection */
							handle->state = DN_CONNECTION_STATE_ERROR_FATAL;
							add_error_message(handle, "unexpected EOF on datanode connection.");
							elog(LOG, "unexpected EOF on node:%s pid:%d, read_status:%d, EOF:%d",
									handle->nodename, handle->backend_pid, read_status, EOF);
							return;
						}

                        handle->state = DN_CONNECTION_STATE_IDLE;
                                        
                        /* Clear any previous error messages */
                        handle->error[0] = '\0';
                    }
                    else
                    {
                        elog(DEBUG1, "pgxc_abort_connections node:%s ready for query", handle->nodename);
                    }
                }
                else 
                {
                    if (handle->nodename[0] != '\0')
                    {
                        elog(WARNING, "pgxc_abort_connections dn node:%s invalid socket %u!", handle->nodename, handle->sock);
                    }
                    else
                    {
                        elog(WARNING, "pgxc_abort_connections dn node, invalid socket %u!", handle->sock);    
                    }
                }
            }

            /* Recheck connection status. */
            elog(DEBUG1, " Begin to recheck pgxc_abort_connections %d coordinator handle", all_handles->co_conn_count);
            for (i = 0; i < all_handles->co_conn_count; i++)
            {
                PGXCNodeHandle *handle = all_handles->coord_handles[i];
				if (handle->sock != NO_SOCKET)
                {
                    if (handle->state != DN_CONNECTION_STATE_IDLE || !node_ready_for_query(handle) || pgxc_node_is_data_enqueued(handle))
                    {
                        elog(DEBUG1, "pgxc_abort_connections recheck node:%s not ready for query, status:%d, sync", handle->nodename, handle->state);
                        
						if (need_sync)
                        {
                            ret = pgxc_node_send_sync(handle);
                            if (ret != 0)
                                elog(WARNING, "pgxc_abort_connections failed to send sync to node %s", handle->nodename);
                        }
                        
                        need_loop_check = true;

						if (proc_exit_inprogress)
						{
							handle->state = DN_CONNECTION_STATE_IDLE;
							handle->last_command = 'Z';
						}
                    }
                    else
                    {
                        elog(DEBUG1, "pgxc_abort_connections recheck node:%s ready for query", handle->nodename);
                    }
                }
                else 
                {
                    if (handle->nodename[0] != '\0')
                    {
                        elog(WARNING, "pgxc_abort_connections cn node:%s invalid socket %u!", handle->nodename, handle->sock);
                    }
                    else
                    {
                        elog(WARNING, "pgxc_abort_connections cn node, invalid socket %u!", handle->sock);    
                    }
                }            
            }
            
            /*
             * recheck datanode nodes.
             */
            elog(DEBUG1, " Begin to recheck pgxc_abort_connections %d datanode handle", all_handles->dn_conn_count);
            for (i = 0; i < all_handles->dn_conn_count; i++)
            {
                PGXCNodeHandle *handle = all_handles->datanode_handles[i];
				if (handle->sock != NO_SOCKET)
                {
                    if (handle->state != DN_CONNECTION_STATE_IDLE || !node_ready_for_query(handle) || pgxc_node_is_data_enqueued(handle))
                    {
                        elog(DEBUG1, "pgxc_abort_connections recheck node:%s not ready for query, status:%d, sync", handle->nodename, handle->state);
                        
						if (need_sync)
                        {
                            ret = pgxc_node_send_sync(handle);
                            if (ret != 0)
                                elog(WARNING, "pgxc_abort_connections failed to send sync to node %s", handle->nodename);
                        }

                        need_loop_check = true;

						if (proc_exit_inprogress)
						{
							handle->state = DN_CONNECTION_STATE_IDLE;
							handle->last_command = 'Z';
						}
                    }
                    else
                    {
                        elog(DEBUG1, "pgxc_abort_connections recheck node:%s ready for query", handle->nodename);
                    }
                }
                else 
                {
                    if (handle->nodename[0] != '\0')
                    {
                        elog(WARNING, "pgxc_abort_connections dn node:%s invalid socket %u!", handle->nodename, handle->sock);
                    }
                    else
                    {
                        elog(WARNING, "pgxc_abort_connections dn node, invalid socket %u!", handle->sock);    
                    }
                }
            }

            need_sync = false; 
            /* no need to recheck, break the loop. */
            if (!need_loop_check)
            {
                break;
            }
        }
    }
}
#endif

void
ExecEndRemoteSubplan(RemoteSubplanState *node)
{// #lizard forgives
    int32             count    = 0;
    ResponseCombiner *combiner = (ResponseCombiner *)node;
    RemoteSubplan    *plan = (RemoteSubplan *) combiner->ss.ps.plan;
    int i;
    struct rusage    start_r;
    struct timeval        start_t;

    if (log_remotesubplan_stats)
        ResetUsageCommon(&start_r, &start_t);

    if (outerPlanState(node))
        ExecEndNode(outerPlanState(node));
    if (node->locator)
        freeLocator(node->locator);

    /*
     * Consume any possible pending input
     */
    if (node->bound)
    {
        pgxc_connections_cleanup(combiner);
    }    
    
    /*
     * Update coordinator statistics
     */
    if (IS_PGXC_COORDINATOR)
    {
        EState *estate = combiner->ss.ps.state;
        
        /* init node_count with conn_count */
        combiner->node_count = combiner->conn_count;
        if (estate->es_num_result_relations > 0 && estate->es_processed > 0)
        {
            switch (estate->es_plannedstmt->commandType)
            {
                case CMD_INSERT:
                    /* One statement can insert into only one relation */
                    pgstat_count_remote_insert(
                                estate->es_result_relations[0].ri_RelationDesc,
                                estate->es_processed);
                    break;
                case CMD_UPDATE:
                case CMD_DELETE:
                    {
                        /*
                         * We can not determine here how many row were updated
                         * or delete in each table, so assume same number of
                         * affected row in each table.
                         * If resulting number of rows is 0 because of rounding,
                         * increment each counter at least on 1.
                         */
                        int        i;
                        int     n;
                        bool     update;

                        update = (estate->es_plannedstmt->commandType == CMD_UPDATE);
                        n = estate->es_processed / estate->es_num_result_relations;
                        if (n == 0)
                            n = 1;
                        for (i = 0; i < estate->es_num_result_relations; i++)
                        {
                            Relation r;
                            r = estate->es_result_relations[i].ri_RelationDesc;
                            if (update)
                                pgstat_count_remote_update(r, n);
                            else
                                pgstat_count_remote_delete(r, n);
                        }
                    }
                    break;
                default:
                    /* nothing to count */
                    break;
            }
        }
    }

    /*
     * Close portals. While cursors_connections exist there are open portals
     */
    if (combiner->cursor)
    {
        /* Restore connections where there are active statements */
        combiner->conn_count = combiner->cursor_count;
        combiner->node_count = combiner->conn_count;
        
        memcpy(combiner->connections, combiner->cursor_connections,
                    combiner->cursor_count * sizeof(PGXCNodeHandle *));
        for (i = 0; i < combiner->cursor_count; i++)
        {
            PGXCNodeHandle *conn;

            conn = combiner->cursor_connections[i];

            CHECK_OWNERSHIP(conn, combiner);

            if (pgxc_node_send_close(conn, false, combiner->cursor) != 0)
            {
                ereport(LOG,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("Failed to close data node cursor")));
            }
        }
        /* The cursor stuff is not needed */
        combiner->cursor = NULL;
        combiner->cursor_count = 0;
        pfree(combiner->cursor_connections);
        combiner->cursor_connections = NULL;
    }

    /* Close statements, even if they never were bound */
    for (i = 0; i < combiner->conn_count; i++)
    {
        PGXCNodeHandle *conn;
        char            cursor[NAMEDATALEN];

        if (plan->cursor)
        {
            if (plan->unique)
				snprintf(cursor, NAMEDATALEN, "%s_"INT64_FORMAT, plan->cursor, plan->unique);
            else
                strncpy(cursor, plan->cursor, NAMEDATALEN);
        }
        else
            cursor[0] = '\0';

        conn = combiner->connections[i];

		/* connection can be null in sort, forget it */
		if (!conn)
		{
			combiner->conn_count--;
			combiner->connections[i] =
					combiner->connections[combiner->conn_count];
			i--;
			continue;
		}

        CHECK_OWNERSHIP(conn, combiner);

        if (pgxc_node_send_close(conn, true, cursor) != 0)
        {
            ereport(LOG,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Failed to close data node statement")));
        }
        
        /* Send SYNC and wait for ReadyForQuery */
        if (pgxc_node_send_sync(conn) != 0)
        {
            ereport(LOG,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Failed to sync msg to node %s backend_pid:%d", conn->nodename, conn->backend_pid)));
        }
        /*
         * Formally connection is not in QUERY state, we set the state to read
         * CloseDone and ReadyForQuery responses. Upon receiving ReadyForQuery
         * state will be changed back to IDLE and conn->coordinator will be
         * cleared.
         */
        PGXCNodeSetConnectionState(conn, DN_CONNECTION_STATE_CLOSE);
    }

    count = combiner->conn_count;
    while (count > 0)
    {
        if (pgxc_node_receive(count,
                              combiner->connections, NULL))
        {
            ereport(LOG,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("Failed to close remote subplan")));
        }

        i = 0;
        while (i < count)
        {
            int res = handle_response(combiner->connections[i], combiner);
            if (res == RESPONSE_EOF)
            {
                i++;
            }
            else if (res == RESPONSE_COMPLETE && combiner->connections[i]->state == DN_CONNECTION_STATE_ERROR_FATAL)
            {
                /* connection is bad, forget it */
                if (--count > i)
                    combiner->connections[i] =
                            combiner->connections[count];
            }
            else if (res == RESPONSE_READY)
            {
                /* Done, connection is reade for query */
                if (--count > i)
                    combiner->connections[i] =
                            combiner->connections[count];
            }
            else if (res == RESPONSE_DATAROW)
            {
                /*
                 * If we are finishing slowly running remote subplan while it
                 * is still working (because of Limit, for example) it may
                 * produce one or more tuples between connection cleanup and
                 * handling Close command. One tuple does not cause any problem,
                 * but if it will not be read the next tuple will trigger
                 * assertion failure. So if we got a tuple, just read and
                 * discard it here.
                 */
                pfree(combiner->currentRow);
                combiner->currentRow = NULL;
            }
            /* Ignore other possible responses */
        }
    }

    ValidateAndCloseCombiner(combiner);
    combiner->conn_count = 0;

    if (log_remotesubplan_stats)
        ShowUsageCommon("ExecEndRemoteSubplan", &start_r, &start_t);
}

/*
 * pgxc_node_report_error
 * Throw error from Datanode if any.
 */
void
pgxc_node_report_error(ResponseCombiner *combiner)
{// #lizard forgives
    /* If no combiner, nothing to do */
    if (!combiner)
        return;
    if (combiner->errorMessage)
    {
        char *code = combiner->errorCode;
#ifndef _PG_REGRESS_
        if ((combiner->errorDetail == NULL) && (combiner->errorHint == NULL))
            ereport(ERROR,
                    (errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
                    errmsg("node:%s, backend_pid:%d, %s", combiner->errorNode, combiner->backend_pid, combiner->errorMessage)));
        else if ((combiner->errorDetail != NULL) && (combiner->errorHint != NULL))
            ereport(ERROR,
                    (errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
                    errmsg("node:%s, backend_pid:%d, %s", combiner->errorNode, combiner->backend_pid, combiner->errorMessage),
                    errdetail("%s", combiner->errorDetail),
                    errhint("%s", combiner->errorHint)));
        else if (combiner->errorDetail != NULL)
            ereport(ERROR,
                    (errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
                    errmsg("node:%s, backend_pid:%d, %s", combiner->errorNode, combiner->backend_pid, combiner->errorMessage),
                    errdetail("%s", combiner->errorDetail)));
        else
            ereport(ERROR,
                    (errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
                    errmsg("node:%s, backend_pid:%d, %s", combiner->errorNode, combiner->backend_pid, combiner->errorMessage),
                    errhint("%s", combiner->errorHint)));
#else
        if ((combiner->errorDetail == NULL) && (combiner->errorHint == NULL))
            ereport(ERROR,
                    (errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
                    errmsg("%s", combiner->errorMessage)));
        else if ((combiner->errorDetail != NULL) && (combiner->errorHint != NULL))
            ereport(ERROR,
                    (errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
                    errmsg("%s", combiner->errorMessage),
                    errdetail("%s", combiner->errorDetail),
                    errhint("%s", combiner->errorHint)));
        else if (combiner->errorDetail != NULL)
            ereport(ERROR,
                    (errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
                    errmsg("%s", combiner->errorMessage),
                    errdetail("%s", combiner->errorDetail)));
        else
            ereport(ERROR,
                    (errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
                    errmsg("%s", combiner->errorMessage),
                    errhint("%s", combiner->errorHint)));
#endif
    }
}


/*
 * get_success_nodes:
 * Currently called to print a user-friendly message about
 * which nodes the query failed.
 * Gets all the nodes where no 'E' (error) messages were received; i.e. where the
 * query ran successfully.
 */
static ExecNodes *
get_success_nodes(int node_count, PGXCNodeHandle **handles, char node_type, StringInfo failednodes)
{
    ExecNodes *success_nodes = NULL;
    int i;

    for (i = 0; i < node_count; i++)
    {
        PGXCNodeHandle *handle = handles[i];
        int nodenum = PGXCNodeGetNodeId(handle->nodeoid, &node_type);

        if (handle->error[0])
        {
            if (!success_nodes)
                success_nodes = makeNode(ExecNodes);
            success_nodes->nodeList = lappend_int(success_nodes->nodeList, nodenum);
        }
        else
        {
            if (failednodes->len == 0)
                appendStringInfo(failednodes, "Error message received from nodes:");
#ifndef _PG_REGRESS_
            appendStringInfo(failednodes, " %s#%d",
                (node_type == PGXC_NODE_COORDINATOR ? "coordinator" : "datanode"),
                nodenum + 1);
#endif
        }
    }
    return success_nodes;
}

/*
 * pgxc_all_success_nodes: Uses get_success_nodes() to collect the
 * user-friendly message from coordinator as well as datanode.
 */
void
pgxc_all_success_nodes(ExecNodes **d_nodes, ExecNodes **c_nodes, char **failednodes_msg)
{
    PGXCNodeAllHandles *connections = get_exec_connections(NULL, NULL, EXEC_ON_ALL_NODES, true);
    StringInfoData failednodes;
    initStringInfo(&failednodes);

    *d_nodes = get_success_nodes(connections->dn_conn_count,
                                 connections->datanode_handles,
                                 PGXC_NODE_DATANODE,
                                 &failednodes);

    *c_nodes = get_success_nodes(connections->co_conn_count,
                                 connections->coord_handles,
                                 PGXC_NODE_COORDINATOR,
                                 &failednodes);

    if (failednodes.len == 0)
        *failednodes_msg = NULL;
    else
        *failednodes_msg = failednodes.data;

    pfree_pgxc_all_handles(connections);
}


/*
 * set_dbcleanup_callback:
 * Register a callback function which does some non-critical cleanup tasks
 * on xact success or abort, such as tablespace/database directory cleanup.
 */
void set_dbcleanup_callback(xact_callback function, void *paraminfo, int paraminfo_size)
{
    void *fparams;

    fparams = MemoryContextAlloc(TopMemoryContext, paraminfo_size);
    memcpy(fparams, paraminfo, paraminfo_size);

    dbcleanup_info.function = function;
    dbcleanup_info.fparams = fparams;
}

/*
 * AtEOXact_DBCleanup: To be called at post-commit or pre-abort.
 * Calls the cleanup function registered during this transaction, if any.
 */
void AtEOXact_DBCleanup(bool isCommit)
{
    if (dbcleanup_info.function)
        (*dbcleanup_info.function)(isCommit, dbcleanup_info.fparams);

    /*
     * Just reset the callbackinfo. We anyway don't want this to be called again,
     * until explicitly set.
     */
    dbcleanup_info.function = NULL;
    if (dbcleanup_info.fparams)
    {
        pfree(dbcleanup_info.fparams);
        dbcleanup_info.fparams = NULL;
    }
}

char *
GetImplicit2PCGID(const char *implicit2PC_head, bool localWrite)
{// #lizard forgives
    int dnCount = 0, coordCount = 0;
    int *dnNodeIds       = NULL;
    int *coordNodeIds = NULL;
    MemoryContext oldContext = CurrentMemoryContext;
    StringInfoData str;

    dnNodeIds = (int*)palloc(sizeof(int) * TBASE_MAX_DATANODE_NUMBER);
    if (dnNodeIds == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory for dnNodeIds")));
    }

    coordNodeIds = (int*)palloc(sizeof(int) * TBASE_MAX_COORDINATOR_NUMBER);
    if (coordNodeIds == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory for coordNodeIds")));
    }

    oldContext = MemoryContextSwitchTo(TopTransactionContext);
    initStringInfo(&str);
    /*
     * Check how many coordinators and datanodes are involved in this
     * transaction.
     * MAX_IMPLICIT_2PC_STR_LEN (5 + 21 + 64 + 1 + 5 + 5)
     */
    
    pgxc_node_remote_count(&dnCount, dnNodeIds, &coordCount, coordNodeIds);
    appendStringInfo(&str, "%s%u:%s:%c:%d:%d",
            implicit2PC_head,
            GetTopTransactionId(),
            PGXCNodeName,
            localWrite ? 'T' : 'F',
            dnCount,
            coordCount + (localWrite ? 1 : 0));
    
#if 0
    for (i = 0; i < dnCount; i++)
        appendStringInfo(&str, ":%d", dnNodeIds[i]);
    for (i = 0; i < coordCount; i++)
        appendStringInfo(&str, ":%d", coordNodeIds[i]);

    if (localWrite)
        appendStringInfo(&str, ":%d", PGXCNodeIdentifier);
#endif
    MemoryContextSwitchTo(oldContext);

    if (dnNodeIds)
    {
        pfree(dnNodeIds);
        dnNodeIds = NULL;
    }

    if (coordNodeIds)
    {
        pfree(coordNodeIds);
        coordNodeIds = NULL;
    }

    return str.data;
}

#ifdef __TBASE__
void
ExecRemoteSubPlanInitializeDSM(RemoteSubplanState *node,
                               ParallelContext *pcxt)
{
    ParallelWorkerStatus *worker_status = NULL;
    worker_status  = GetParallelWorkerStatusInfo(pcxt->toc);
    node->parallel_status = worker_status;
}

void
ExecRemoteQueryInitializeDSM(RemoteQueryState *node,
                               ParallelContext *pcxt)
{
    ParallelWorkerStatus *worker_status = NULL;
    worker_status  = GetParallelWorkerStatusInfo(pcxt->toc);
    node->parallel_status = worker_status;
}

void
ExecRemoteSubPlanInitDSMWorker(RemoteSubplanState *node,
                               ParallelWorkerContext *pwcxt)
{
    int32                  i                    = 0;
    int32                  length            = 0;
    int32                  worker_num        = 0;
    int32                  step                = 0;
    int32                 begin_node         = 0; /* include */
    int32                 end_node             = 0; /* exclude */
    ParallelWorkerStatus  *worker_status    = NULL;
    List                     *locla_exec_nodes = NULL;
    ListCell              *node_list_item   = NULL;

	worker_status  = GetParallelWorkerStatusInfo(pwcxt->toc);
    worker_num = ExecGetForWorkerNumber(worker_status);
    node->parallel_status = worker_status;    
    if (node->execOnAll)
    {
        length = list_length(node->execNodes);
        locla_exec_nodes = node->execNodes;
        node->execNodes  = NULL;  

        if (worker_num)
            step = DIVIDE_UP(length, worker_num);
        /* Last worker. */
        if (ParallelWorkerNumber == (worker_num - 1))
        {
            begin_node = ParallelWorkerNumber * step;
            end_node   = length;
        }
        else
        {
            begin_node = ParallelWorkerNumber * step;
            end_node   = begin_node + step;
        }

        /* Form the execNodes of our own node. */
        i = 0;
        foreach(node_list_item, locla_exec_nodes)
        {
            int    nodeid = lfirst_int(node_list_item);

            if (i >= begin_node && i < end_node)
            {
                node->execNodes = lappend_int(node->execNodes, nodeid);
            }

            if (i >= end_node)
            {
                break;
            }

            i++;
        }
        list_free(locla_exec_nodes);
    }
    else
    {
        elog(PANIC, "only reditribution on all nodes can run in worker");
    }    
}
void
ExecRemoteQueryInitializeDSMWorker(RemoteQueryState *node,
                                   ParallelWorkerContext *pwcxt)
{
    int32                 worker_num    = 0;
    ParallelWorkerStatus *worker_status = NULL;
    ResponseCombiner     *combiner      = NULL;
    RemoteQuery          *step          = NULL;

    combiner               = (ResponseCombiner *) node;
    step                   = (RemoteQuery *) combiner->ss.ps.plan;
	worker_status  		  = GetParallelWorkerStatusInfo(pwcxt->toc);
    worker_num            = ExecGetForWorkerNumber(worker_status);
    node->parallel_status = worker_status;    
    worker_num              = worker_num; /* keep compiler quiet. */
    if (step->exec_type != EXEC_ON_DATANODES)
    {
        elog(PANIC, "Only datanode remote quern can run in worker");
    }
}

/*
 * pgxc_append_param_val:
 * Append the parameter value for the SET clauses of the UPDATE statement.
 * These values are the table attribute values from the dataSlot.
 */
static void
pgxc_append_param_val(StringInfo buf, Datum val, Oid valtype)
{
    /* Convert Datum to string */
    char *pstring;
    int len;
    uint32 n32;
    Oid        typOutput;
    bool    typIsVarlena;

    /* Get info needed to output the value */
    getTypeOutputInfo(valtype, &typOutput, &typIsVarlena);
    /*
     * If we have a toasted datum, forcibly detoast it here to avoid
     * memory leakage inside the type's output routine.
     */
    if (typIsVarlena)
        val = PointerGetDatum(PG_DETOAST_DATUM(val));

    pstring = OidOutputFunctionCall(typOutput, val);

    /* copy data to the buffer */
    len = strlen(pstring);
    n32 = htonl(len);
    appendBinaryStringInfo(buf, (char *) &n32, 4);
    appendBinaryStringInfo(buf, pstring, len);
}

/*
 * pgxc_append_param_junkval:
 * Append into the data row the parameter whose value cooresponds to the junk
 * attributes in the source slot, namely ctid or node_id.
 */
static void
pgxc_append_param_junkval(TupleTableSlot *slot, AttrNumber attno,
                          Oid valtype, StringInfo buf)
{
    bool isNull;

    if (slot && attno != InvalidAttrNumber)
    {
        /* Junk attribute positions are saved by ExecFindJunkAttribute() */
        Datum val = ExecGetJunkAttribute(slot, attno, &isNull);
        /* shouldn't ever get a null result... */
        if (isNull)
            elog(ERROR, "NULL junk attribute");

        pgxc_append_param_val(buf, val, valtype);
    }
}

/* handle escape char '''  in source char sequence */
static char*
handleEscape(char *source, bool *special_case)
{// #lizard forgives
    char escape = '\''; /* escape char */

    int len = strlen(source);

    char *des = (char *)palloc(len * 3 + 1);

    int i = 0;

    int index = 0;

    /* when meet escape char, add another escape char */
    for(i = 0; i < len; i++)
    {
        if(source[i] == escape)
        {
            des[index++] = '\'';
            des[index++] = source[i];
        }
        else if(source[i] == '\r')
        {
            des[index++] = '\\';
            des[index++] = 'r';
            (*special_case) = true;
        }
        else if(source[i] == '\n')
        {
            des[index++] = '\\';
            des[index++] = 'n';
            (*special_case) = true;
        }
        else if(source[i] == '\t')
        {
            des[index++] = '\\';
            des[index++] = 't';
            (*special_case) = true;
        }
        else if(source[i] == '\b')
        {
            des[index++] = '\\';
            des[index++] = 'b';
            (*special_case) = true;
        }
        else if(source[i] == '\f')
        {
            des[index++] = '\\';
            des[index++] = 'f';
            (*special_case) = true;
        }
        else if(source[i] == '\v')
        {
            des[index++] = '\\';
            des[index++] = 'v';
            (*special_case) = true;
        }
        else
        {
            des[index++] = source[i];
        }
    }

    des[index++] = '\0';

    pfree(source);
    return des;
}


static void
SetDataRowParams(ModifyTableState *mtstate, RemoteQueryState *node, TupleTableSlot *sourceSlot, TupleTableSlot *dataSlot,
                        JunkFilter *ri_junkFilter)
{// #lizard forgives
    CmdType    operation = mtstate->operation;
    StringInfoData    buf;
    StringInfoData    select_buf;
    uint16 numparams = node->rqs_num_params;
    TupleDesc         tdesc = dataSlot->tts_tupleDescriptor;
    int                attindex;
    int                numatts = tdesc->natts;
    ResponseCombiner *combiner = (ResponseCombiner *) node;
    RemoteQuery    *step = (RemoteQuery *) combiner->ss.ps.plan;
	Oid            *param_types = step->rq_param_types;
    Form_pg_attribute att;
    Oid         typeOutput;
    bool        typIsVarlena;
    char        *columnValue;
    char        *typename;
    bool special_case = false;
    int         cols = 0;

    initStringInfo(&buf);
    initStringInfo(&select_buf);

    switch(operation)
    {
        case CMD_INSERT:
        {
            /* ensure we have all values */
            slot_getallattrs(dataSlot);

            {
                uint16 params_nbo = htons(numparams); /* Network byte order */
                appendBinaryStringInfo(&buf, (char *) &params_nbo, sizeof(params_nbo));
            }
            
            for (attindex = 0; attindex < numatts; attindex++)
            {
                uint32 n32;
                Assert(attindex < numparams);

				if (dataSlot->tts_isnull[attindex] || !OidIsValid(param_types[attindex]))
                {
                    n32 = htonl(-1);
                    appendBinaryStringInfo(&buf, (char *) &n32, 4);
                }
                else
                    pgxc_append_param_val(&buf, dataSlot->tts_values[attindex], tdesc->attrs[attindex]->atttypid);
            }

            if (attindex != numparams)
                elog(ERROR, "INSERT DataRowParams mismatch with dataSlot.");

            node->paramval_data = buf.data;
            node->paramval_len = buf.len;

            if (mtstate->mt_onconflict == ONCONFLICT_UPDATE)
            {
                appendStringInfoString(&select_buf, step->sql_select_base);
                appendStringInfoString(&select_buf, " where ");
#if 0
                numparams = node->ss_num_params;

                {
                    uint16 params_nbo = htons(numparams); /* Network byte order */
                    appendBinaryStringInfo(&select_buf, (char *) &params_nbo, sizeof(params_nbo));
                }
#endif
                cols = 0;
                for (attindex = 0; attindex < numatts; attindex++)
                {
                    special_case = false;

                    if (!bms_is_member(attindex + 1, step->conflict_cols))
                        continue;
                    
                    if (dataSlot->tts_isnull[attindex])
                    {
                        if(cols > 0)
                        {
                            appendStringInfo(&select_buf, " and "); 
                        }
                        att = tdesc->attrs[attindex];
                        appendStringInfo(&select_buf, "%s is NULL", att->attname.data);
                    
                        //n32 = htonl(-1);
                        //appendBinaryStringInfo(&select_buf, (char *) &n32, 4);
                    }
                    else
                    {
                        if(cols > 0)
                        {
                            appendStringInfo(&select_buf, " and "); 
                        }
                        att = tdesc->attrs[attindex];
                        getTypeOutputInfo(att->atttypid, &typeOutput, &typIsVarlena);           
                        columnValue = DatumGetCString(OidFunctionCall1(typeOutput, dataSlot->tts_values[attindex]));
	    				typename = get_typename(att->atttypid);
                        columnValue = handleEscape(columnValue, &special_case);
                        if(special_case)
                            appendStringInfo(&select_buf, "%s = E'%s'::%s", att->attname.data, columnValue, typename);
                        else
                            appendStringInfo(&select_buf, "%s = '%s'::%s", att->attname.data, columnValue, typename);
                        //pgxc_append_param_val(&select_buf, dataSlot->tts_values[attindex], tdesc->attrs[attindex]->atttypid);
                    }

                    cols++;
                }

                if (step->forUpadte)
                    appendStringInfoString(&select_buf, " FOR UPDATE");
                else
                    appendStringInfoString(&select_buf, " FOR NO KEY UPDATE");

                //node->ss_paramval_data = select_buf.data;
                //node->ss_paramval_len = select_buf.len;

                step->sql_select = select_buf.data;
            }
        }
        break;
        case CMD_UPDATE:
        {
            /* ensure we have all values */
            slot_getallattrs(dataSlot);
            
            {
                uint16 params_nbo = htons(numparams); /* Network byte order */
                appendBinaryStringInfo(&buf, (char *) &params_nbo, sizeof(params_nbo));
            }
            
            for (attindex = 0; attindex < numatts; attindex++)
            {
                uint32 n32;
                Assert(attindex < numparams);

				if (dataSlot->tts_isnull[attindex] || !OidIsValid(param_types[attindex]))
                {
                    n32 = htonl(-1);
                    appendBinaryStringInfo(&buf, (char *) &n32, 4);
                }
                else
                    pgxc_append_param_val(&buf, dataSlot->tts_values[attindex], tdesc->attrs[attindex]->atttypid);
            }

            if (attindex != numparams - 2)
                elog(ERROR, "UPDATE DataRowParams mismatch with dataSlot.");

            if (step->action == UPSERT_NONE)
            {
                pgxc_append_param_junkval(sourceSlot, ri_junkFilter->jf_junkAttNo,
                              TIDOID, &buf);
                pgxc_append_param_junkval(sourceSlot, ri_junkFilter->jf_xc_node_id,
                                          INT4OID, &buf);
            }
            else
            {
                pgxc_append_param_junkval(sourceSlot, step->jf_ctid,
                              TIDOID, &buf);
                pgxc_append_param_junkval(sourceSlot, step->jf_xc_node_id,
                                          INT4OID, &buf);
            }

            node->paramval_data = buf.data;
            node->paramval_len = buf.len;
        }
        break;
        case CMD_DELETE:
        {
            {
                uint16 params_nbo = htons(numparams); /* Network byte order */
                appendBinaryStringInfo(&buf, (char *) &params_nbo, sizeof(params_nbo));
            }
        
            pgxc_append_param_junkval(sourceSlot, ri_junkFilter->jf_junkAttNo,
                          TIDOID, &buf);
            pgxc_append_param_junkval(sourceSlot, ri_junkFilter->jf_xc_node_id,
                                      INT4OID, &buf);

            node->paramval_data = buf.data;
            node->paramval_len = buf.len;
        }
        break;
        default:
            elog(ERROR, "unexpected CmdType in SetDataRowParams.");
            break;
    }
}

static void 
remember_prepared_node(RemoteQueryState *rstate, int node)
{
    int32 wordindex  = 0;
    int32 wordoffset = 0;
    if (node > MAX_NODES_NUMBER)
    {
        elog(ERROR, "invalid nodeid:%d is bigger than maximum node number of the cluster",node);
    }

    wordindex  = node/UINT32_BITS_NUM;    
    wordoffset = node % UINT32_BITS_NUM;
    SET_BIT(rstate->dml_prepared_mask[wordindex], wordoffset);
}

static bool 
is_node_prepared(RemoteQueryState *rstate, int node)
{
    int32 wordindex  = 0;
    int32 wordoffset = 0;
	if (node >= MAX_NODES_NUMBER)
    {
        elog(ERROR, "invalid nodeid:%d is bigger than maximum node number of the cluster", node);
    }

    wordindex  = node/UINT32_BITS_NUM;    
    wordoffset = node % UINT32_BITS_NUM;
    return BIT_SET(rstate->dml_prepared_mask[wordindex], wordoffset) != 0;
}

/*
  *   ExecRemoteDML----execute DML on coordinator
  *   return true if insert/update/delete successfully, else false.
  *
  *   If DML target relation has unshippable triggers, we have to do DML on coordinator.
  *   Construct remote query about DML on plan phase, then adopt parse/bind/execute to
  *   execute the DML with the triggers on coordinator.
  * 
  */
bool
ExecRemoteDML(ModifyTableState *mtstate, ItemPointer tupleid, HeapTuple oldtuple,
              TupleTableSlot *slot, TupleTableSlot *planSlot, EState *estate, EPQState *epqstate,
              bool canSetTag, TupleTableSlot **returning, UPSERT_ACTION *result,
              ResultRelInfo *resultRelInfo, int rel_index)
{// #lizard forgives
    CmdType    operation = mtstate->operation;
    RemoteQueryState *node = (RemoteQueryState *)mtstate->mt_remoterels[rel_index];
    ResponseCombiner *combiner = (ResponseCombiner *) node;
    RemoteQuery    *step = (RemoteQuery *) combiner->ss.ps.plan; 
    ExprContext    *econtext = combiner->ss.ps.ps_ExprContext;
    ExprContext    *proj_econtext = mtstate->ps.ps_ExprContext;
    PGXCNodeAllHandles *pgxc_connections = NULL;
    GlobalTransactionId gxid = InvalidGlobalTransactionId;
    Snapshot        snapshot = GetActiveSnapshot();
    PGXCNodeHandle **connections = NULL;
    int                i = 0;
    int                regular_conn_count = 0;
    TupleTableSlot *tupleslot = NULL;
    int nodeid;

    if (operation == CMD_INSERT && mtstate->mt_onconflict == ONCONFLICT_UPDATE)
    {
        if (step->action == UPSERT_NONE)
        {
            step->action = UPSERT_SELECT;
        }
    }

    if (step->action != UPSERT_UPDATE)
    {
        /*
         * Get connections for remote query
         */
        econtext->ecxt_scantuple = slot;
        pgxc_connections = get_exec_connections(node, step->exec_nodes,
                                                step->exec_type,
                                                true);

        Assert(step->exec_type == EXEC_ON_DATANODES);
        
        connections = pgxc_connections->datanode_handles;
        regular_conn_count = pgxc_connections->dn_conn_count;

        combiner->conns = pgxc_connections->datanode_handles;
        combiner->ccount = pgxc_connections->dn_conn_count;
    }
    else
    {
        /* reset connection */
        connections = combiner->conns;
        regular_conn_count = combiner->ccount;
    }

    /* get all parameters for execution */
    SetDataRowParams(mtstate, node, planSlot, slot, resultRelInfo->ri_junkFilter);

    /* need to send commandid to datanode */
    SetSendCommandId(true);

    Assert(regular_conn_count == 1);

    nodeid = PGXCNodeGetNodeId(connections[i]->nodeoid, NULL);
    
    /* need transaction during execution, but only send begin to datanode once */
    if (!is_node_prepared(node, nodeid))
    {
        gxid = GetCurrentTransactionIdIfAny();
        
		if (pgxc_node_begin(1, &connections[i], gxid, true, false))
        {
            elog(ERROR, "Could not begin transaction on datanode in ExecRemoteDML, nodeid:%d.",
                         connections[i]->nodeid);
        }

        remember_prepared_node(node, nodeid);
    }

    /*
      * For update/delete and simple insert, we can send SQL statement  to datanode through
      * parse/bind/execute.
      * 
      * UPSERT is a little different, we separate UPSERT into insert, select and update.
      *
      * Step 1, we send select...for update to find the conflicted tuple and lock it; if we
      * succeed, then do the update and finish. Else, it means no conflict tuple now, we can do
      * the insert.
      * 
      * Step 2, if insert succeed, then finish; else insert must conflict with others, this must be 
      * unexepected, but it does happen, goto step 1.
      * We have to repeat step 1 and step 2, until finish.
      */
    if (operation == CMD_UPDATE || operation == CMD_DELETE ||
        (operation == CMD_INSERT && mtstate->mt_onconflict != ONCONFLICT_UPDATE))
    {
        if (!pgxc_start_command_on_connection(connections[i], node, snapshot))
        {
            pgxc_node_remote_abort(TXN_TYPE_RollbackTxn, true);
            pfree_pgxc_all_handles(pgxc_connections);
            elog(ERROR, "Failed to send command to datanode in ExecRemoteDML, nodeid:%d.",
                        connections[i]->nodeid);
        }
    }
    else
UPSERT:
    {
        /* insert...on conflict do update */
        char       *paramval_data = node->paramval_data;        
        int            paramval_len = node->paramval_len;        
        Oid           *rqs_param_types = node->rqs_param_types;    
        int            rqs_num_params = node->rqs_num_params;
        char       *sql = step->sql_statement;
        char       *statement = step->statement;

        /* reset connection */
        connections = combiner->conns;
        regular_conn_count = combiner->ccount;

        switch(step->action)
        {
            /* select first, try to find conflict tupe */
            case UPSERT_SELECT:
            {
                node->paramval_data = node->ss_paramval_data;
                node->paramval_len = node->ss_paramval_len;
                node->rqs_param_types = node->ss_param_types;
                node->rqs_num_params = node->ss_num_params;
                step->sql_statement = step->sql_select;
                step->statement = step->select_cursor;

                if (!pgxc_start_command_on_connection(connections[i], node, snapshot))
                {
                    pgxc_node_remote_abort(TXN_TYPE_RollbackTxn, true);
                    pfree_pgxc_all_handles(pgxc_connections);
                    elog(ERROR, "Failed to send up_select to datanode in ExecRemoteDML, nodeid:%d.",
                                connections[i]->nodeid);
                }

                node->paramval_data = paramval_data;
                node->paramval_len = paramval_len;
                node->rqs_param_types = rqs_param_types;
                node->rqs_num_params = rqs_num_params;
                step->sql_statement = sql;
                step->statement = statement;

                break;
            }
            /* no conflict tuple found, try to insert */
            case UPSERT_INSERT:
            {
                if (!pgxc_start_command_on_connection(connections[i], node, snapshot))
                {
                    pgxc_node_remote_abort(TXN_TYPE_RollbackTxn, true);
                    pfree_pgxc_all_handles(pgxc_connections);
                    elog(ERROR, "Failed to send up_insert to datanode in ExecRemoteDML, nodeid:%d.",
                                connections[i]->nodeid);
                }
                                    
                break;
            }
            case UPSERT_UPDATE:
            {
                node->paramval_data = node->su_paramval_data;
                node->paramval_len = node->su_paramval_len;
                node->rqs_param_types = node->su_param_types;
                node->rqs_num_params = node->su_num_params;
                step->sql_statement = step->sql_update;
                step->statement = step->update_cursor;
                mtstate->operation = CMD_UPDATE;
                
                /* do update */
                proj_econtext->ecxt_scantuple = mtstate->mt_existing;
                proj_econtext->ecxt_innertuple = slot;
                proj_econtext->ecxt_outertuple = NULL;

                if (ExecQual(resultRelInfo->ri_onConflictSetWhere, proj_econtext))
                {
                    Datum        datum;
                    bool        isNull;
                    ItemPointer conflictTid;

                    datum = ExecGetJunkAttribute(mtstate->mt_existing,
                                                 step->jf_ctid,
                                                 &isNull);
                    /* shouldn't ever get a null result... */
                    if (isNull)
                        elog(ERROR, "ctid is NULL");

                    conflictTid = (ItemPointer) DatumGetPointer(datum);

                    Assert(oldtuple == NULL);

                    oldtuple = heap_form_tuple(mtstate->mt_existing->tts_tupleDescriptor, 
                                               mtstate->mt_existing->tts_values, 
                                               mtstate->mt_existing->tts_isnull);
                    
                    /* Project the new tuple version */
                    ExecProject(resultRelInfo->ri_onConflictSetProj);

                    /* Execute UPDATE with projection */
                    *returning = ExecRemoteUpdate(mtstate, conflictTid, oldtuple,
                                            mtstate->mt_conflproj, mtstate->mt_existing,
                                            &mtstate->mt_epqstate, mtstate->ps.state,
                                            canSetTag);
                }

                node->paramval_data = paramval_data;
                node->paramval_len = paramval_len;
                node->rqs_param_types = rqs_param_types;
                node->rqs_num_params = rqs_num_params;
                step->sql_statement = sql;
                step->statement = statement;
                mtstate->operation = CMD_INSERT;
                step->action = UPSERT_NONE;

                *result = UPSERT_UPDATE;    

                return true;
            }
            default:
                elog(ERROR, "unexpected UPSERT action.");
                break;
        }
    }

    /* handle reponse */
    combiner->connections = connections;
    combiner->conn_count = regular_conn_count;
    combiner->current_conn = 0;
    combiner->DML_processed = 0;

    /* 
      * get response from remote datanode while executing DML,
      * we just need to check the affected rows only.
      */
    while((tupleslot = FetchTuple(combiner)) != NULL)
    {
        /* we do nothing until we get the commandcomplete response, except for UPSERT case */
        mtstate->mt_existing = tupleslot;
    }
    
    if (combiner->errorMessage)
        pgxc_node_report_error(combiner);

    if (combiner->DML_processed)
    {
        if (combiner->DML_processed > 1)
        {
            elog(ERROR, "RemoteDML affects %d rows, more than one row.", combiner->DML_processed);
        }

        /* UPSERT: if select succeed, we can do update directly */
        if (step->action == UPSERT_SELECT)
        {
            step->action = UPSERT_UPDATE;
            goto UPSERT;
        }
        else if (step->action == UPSERT_INSERT)
        {
            step->action = UPSERT_NONE;
            *result = UPSERT_INSERT;
        }
        
        /* DML succeed */
        return true;
    }
    else
    {
        /* UPSERT: if select failed, we do insert instead */
        if (step->action == UPSERT_SELECT)
        {
            step->action = UPSERT_INSERT;
            goto UPSERT;
        }
        /* UPSERT: insert failed, conflict tuple found, re-check the constraints. */
        else if (step->action == UPSERT_INSERT)
        {
            step->action = UPSERT_SELECT;
            goto UPSERT;
        }
        
        return false;
    }

    return true;    /* keep compiler quiet */
}

void SetCurrentHandlesReadonly(void)
{
    int i = 0;
    PGXCNodeHandle        *conn     = NULL;
	PGXCNodeAllHandles *handles = NULL;
    
	if (!is_pgxc_handles_init())
	{
		return;
	}

	handles = get_current_handles();
    
    for (i = 0; i < handles->dn_conn_count; i++)
    {
        conn = handles->datanode_handles[i];

        /* Skip empty slots */
        if (conn->sock == NO_SOCKET)
            continue;

        conn->read_only = true;
    }

    for (i = 0; i < handles->co_conn_count; i++)
    {
        conn = handles->coord_handles[i];

        /* Skip empty slots */
        if (conn->sock == NO_SOCKET)
            continue;

        conn->read_only = true;
    }
    pfree_pgxc_all_handles(handles);
}

/*
 * Currently, This function will only be called by CommitSubTranscation.
 * 1. When coordinator, need to send commit_subtxn and clean up connection,
 *       and will not release handle.
 * 2. When datanode, need to send commit transcation and clean up connection,
 *      and will not release handle.
 * 3. When non-subtxn, the parameter of pgxc_node_remote_commit will be 
 *      (TXN_TYPE_CommitTxn , need_release_handle = true)
 */
void
SubTranscation_PreCommit_Remote(void)
{
	MemoryContext old;
	MemoryContext temp = AllocSetContextCreate(TopMemoryContext,
												  "SubTransaction remote commit context",
												  ALLOCSET_DEFAULT_SIZES);
	old = MemoryContextSwitchTo(temp);
    /* Only local coord can send down commit_subtxn when exec plpgsql */
#ifdef _PG_ORCL_
	if (InPlpgsqlFunc())
#else
    if (InPlpgsqlFunc() && IS_PGXC_LOCAL_COORDINATOR)
#endif
    {
        pgxc_node_remote_commit(TXN_TYPE_CommitSubTxn, false);
    }
    else if (IS_PGXC_DATANODE)
    {
        pgxc_node_remote_commit(TXN_TYPE_CommitTxn, false);
    }
	MemoryContextSwitchTo(old);
	MemoryContextDelete(temp);
}

/*
 * Currently, This function will only be called by AbortSubTranscation.
 * 1. When coordinator, need to send rollback_subtxn and clean up connection,
 *       and will not release handle.
 * 2. When datanode, need to send rollback transcation and clean up connection,
 *      and will not release handle.
 * 3. When non-subtxn, the parameter of pgxc_node_remote_abort will be 
 *      (TXN_TYPE_RollbackTxn , need_release_handle = true)
 */

void
SubTranscation_PreAbort_Remote(void)
{
    /* Only local coord can send down commit_subtxn when exec plpgsql */
    if (InPlpgsqlFunc() && IS_PGXC_LOCAL_COORDINATOR)
    {
        PreAbort_Remote(TXN_TYPE_RollbackSubTxn, false);
    }
    else if (IS_PGXC_LOCAL_COORDINATOR)
    {
        PreAbort_Remote(TXN_TYPE_CleanConnection, false);
    }
    else if (IS_PGXC_DATANODE)
    {
        PreAbort_Remote(TXN_TYPE_RollbackTxn, false);
    }
    
}

bool
SetSnapshot(EState *state)
{
    bool result = false;
    
    if (state && state->es_plannedstmt && 
        state->es_plannedstmt->need_snapshot)
    { 
        Snapshot snap;
        
        if (state->es_snapshot)
        {
            elog(ERROR, "Unexpected snapshot");
        }

        if (need_global_snapshot)
        {
            snap = GetTransactionSnapshot();
        }
        else
        {
            snap = GetLocalTransactionSnapshot();
        }

        PushActiveSnapshot(snap);
        state->es_snapshot = RegisterSnapshot(snap);
        /* snapshot set */
        state->es_plannedstmt->need_snapshot = false;

        result = true;
    }

    return result;
}
#endif
