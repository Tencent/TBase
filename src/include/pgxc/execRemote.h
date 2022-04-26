/*-------------------------------------------------------------------------
 *
 * execRemote.h
 *
 *      Functions to execute commands on multiple Datanodes
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/execRemote.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef EXECREMOTE_H
#define EXECREMOTE_H
#include "locator.h"
#include "nodes/nodes.h"
#include "pgxcnode.h"
#include "planner.h"
#ifdef XCP
#include "squeue.h"
#include "remotecopy.h"
#endif
#include "access/tupdesc.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"
#include "nodes/pg_list.h"
#include "tcop/dest.h"
#include "tcop/pquery.h"
#include "utils/snapshot.h"
#ifdef __TBASE__
#include "access/parallel.h"
#endif
#include "access/xact.h"

/* Outputs of handle_response() */
#define RESPONSE_EOF EOF
#define RESPONSE_COMPLETE 0
#define RESPONSE_SUSPENDED 1
#define RESPONSE_TUPDESC 2
#define RESPONSE_DATAROW 3
#define RESPONSE_COPY 4
#define RESPONSE_BARRIER_OK 5
#ifdef XCP
#define RESPONSE_ERROR 6
#define RESPONSE_READY 10
#define RESPONSE_WAITXIDS 11
#define RESPONSE_ASSIGN_GXID 12
#endif

#ifdef __TBASE__
#define RESPONSE_INSTR 13
#define     UINT32_BITS_NUM              32
#define     WORD_NUMBER_FOR_NODES      (MAX_NODES_NUMBER / UINT32_BITS_NUM)

#define CLEAR_BIT(data, bit) data = (~(1 << (bit)) & (data))
#define SET_BIT(data, bit)   data = ((1 << (bit)) | (data))
#define BIT_CLEAR(data, bit) (0 == ((1 << (bit)) & (data))) 
#define BIT_SET(data, bit)   ((1 << (bit)) & (data)) 

extern int DataRowBufferSize;

extern bool need_global_snapshot;
extern List *executed_node_list;
#endif

typedef enum
{
    REQUEST_TYPE_NOT_DEFINED,    /* not determined yet */
    REQUEST_TYPE_COMMAND,        /* OK or row count response */
    REQUEST_TYPE_QUERY,            /* Row description response */
    REQUEST_TYPE_COPY_IN,        /* Copy In response */
    REQUEST_TYPE_COPY_OUT,        /* Copy Out response */
    REQUEST_TYPE_ERROR            /* Error, ignore responses */
}    RequestType;

/*
 * Type of requests associated to a remote COPY OUT
 */
typedef enum
{
    REMOTE_COPY_NONE,        /* Not defined yet */
    REMOTE_COPY_STDOUT,        /* Send back to client */
    REMOTE_COPY_FILE,        /* Write in file */
    REMOTE_COPY_TUPLESTORE    /* Store data in tuplestore */
} RemoteCopyType;

/*
 * Type of remote param from init-plan or subplan
 */
typedef enum
{
	REMOTE_PARAM_UNUSED,
	REMOTE_PARAM_INITPLAN,
	REMOTE_PARAM_SUBPLAN
} RemoteParamType;

/* Combines results of INSERT statements using multiple values */
typedef struct CombineTag
{
    CmdType cmdType;                        /* DML command type */
    char    data[COMPLETION_TAG_BUFSIZE];    /* execution result combination data */
} CombineTag;

/*
 * Common part for all plan state nodes needed to access remote datanodes
 * ResponseCombiner must be the first field of the plan state node so we can
 * typecast
 */
typedef struct ResponseCombiner
{
    ScanState    ss;                        /* its first field is NodeTag */
    int            node_count;                /* total count of participating nodes */
    PGXCNodeHandle **connections;        /* Datanode connections being combined */
    int            conn_count;                /* count of active connections */
    int            current_conn;            /* used to balance load when reading from connections */
    long        current_conn_rows_consumed;
    CombineType combine_type;            /* see CombineType enum */
    int            command_complete_count; /* count of received CommandComplete messages */
    RequestType request_type;            /* see RequestType enum */
    TupleDesc    tuple_desc;                /* tuple descriptor to be referenced by emitted tuples */
    int            description_count;        /* count of received RowDescription messages */
    int            copy_in_count;            /* count of received CopyIn messages */
    int            copy_out_count;            /* count of received CopyOut messages */
    FILE       *copy_file;              /* used if copy_dest == COPY_FILE */
    uint64        processed;                /* count of data rows handled */
    char        errorCode[5];            /* error code to send back to client */
    char       *errorMessage;            /* error message to send back to client */
    char       *errorDetail;            /* error detail to send back to client */
    char       *errorHint;                /* error hint to send back to client */
    Oid            returning_node;            /* returning replicated node */
    RemoteDataRow currentRow;            /* next data ro to be wrapped into a tuple */
    /* TODO use a tuplestore as a rowbuffer */
    List        *rowBuffer;                /* buffer where rows are stored when connection
                                         * should be cleaned for reuse by other RemoteQuery */
    /*
     * To handle special case - if there is a simple sort and sort connection
     * is buffered. If EOF is reached on a connection it should be removed from
     * the array, but we need to know node number of the connection to find
     * messages in the buffer. So we store nodenum to that array if reach EOF
     * when buffering
     */
    Oid        *tapenodes;
    /*
     * If some tape (connection) is buffered, contains a reference on the cell
     * right before first row buffered from this tape, needed to speed up
     * access to the data
     */
    ListCell  **tapemarks;
#ifdef __TBASE__
    List            **prerowBuffers;    /* 
                                          *used for each connection in prefetch with merge_sort, 
                                          * put datarows in each rowbuffer in order
                                                                            */
    Tuplestorestate **dataRowBuffer;    /* used for prefetch */
    long             *dataRowMemSize;    /* size of datarow in memory */
    int             *nDataRows;         /* number of datarows in tuplestore */
    TupleTableSlot  *tmpslot;           
    char*            errorNode;            /* node Oid, who raise an error, set when handle_response */
    int              backend_pid;        /* backend_pid, who raise an error, set when handle_response */
    bool             is_abort;
#endif
    bool        merge_sort;             /* perform mergesort of node tuples */
    bool        extended_query;         /* running extended query protocol */
    bool        probing_primary;        /* trying replicated on primary node */
    void       *tuplesortstate;            /* for merge sort */
    /* COPY support */
    RemoteCopyType remoteCopyType;
    Tuplestorestate *tuplestorestate;
    /* cursor support */
    char       *cursor;                    /* cursor name */
    char       *update_cursor;            /* throw this cursor current tuple can be updated */
    int            cursor_count;            /* total count of participating nodes */
    PGXCNodeHandle **cursor_connections;/* data node connections being combined */
#ifdef __TBASE__
    /* statistic information for debug */
    int         recv_node_count;       /* number of recv nodes */
    uint64      recv_tuples;           /* number of recv tuples */
    TimestampTz recv_total_time;       /* total time to recv tuples */
    /* used for remoteDML */
    int32      DML_processed;         /* count of DML data rows handled on remote nodes */
    PGXCNodeHandle **conns;        
    int                 ccount;    
    uint64     recv_datarows;
	
	/* for remote instrument */
	HTAB            *recv_instr_htbl;        /* received str hash table for each plan_node_id */
	bool    remote_parallel_estimated;  /* hint for remote instrument in parallel mode */
#endif
}    ResponseCombiner;

typedef struct RemoteQueryState
{
    ResponseCombiner combiner;            /* see ResponseCombiner struct */
    bool        query_Done;                /* query has been sent down to Datanodes */
    /*
     * While we are not supporting grouping use this flag to indicate we need
     * to initialize collecting of aggregates from the DNs
     */
    bool        initAggregates;
    /* Simple DISTINCT support */
    FmgrInfo   *eqfunctions;             /* functions to compare tuples */
    MemoryContext tmp_ctx;                /* separate context is needed to compare tuples */
    /* Support for parameters */
    char       *paramval_data;        /* parameter data, format is like in BIND */
    int            paramval_len;        /* length of parameter values data */
    Oid           *rqs_param_types;    /* Types of the remote params */
    int            rqs_num_params;

    int            eflags;            /* capability flags to pass to tuplestore */
    bool        eof_underlying; /* reached end of underlying plan? */

#ifdef __TBASE__
    ParallelWorkerStatus *parallel_status; /*Shared storage for parallel worker .*/

    /* parameters for insert...on conflict do update */
    char       *ss_paramval_data;        
    int            ss_paramval_len;        
    Oid           *ss_param_types;
    int            ss_num_params;

    char       *su_paramval_data;        
    int            su_paramval_len;        
    Oid           *su_param_types;    
    int            su_num_params;

    uint32        dml_prepared_mask[WORD_NUMBER_FOR_NODES]; 
#endif
}    RemoteQueryState;

typedef struct RemoteParam
{
    ParamKind     paramkind;        /* kind of parameter */
    int            paramid;        /* numeric ID for parameter */
    Oid            paramtype;        /* pg_type OID of parameter's datatype */
    int            paramused;        /* is param used */
} RemoteParam;


/*
 * Execution state of a RemoteSubplan node
 */
typedef struct RemoteSubplanState
{
    ResponseCombiner combiner;            /* see ResponseCombiner struct */
    char       *subplanstr;                /* subplan encoded as a string */
    bool        bound;                    /* subplan is sent down to the nodes */
    bool        local_exec;             /* execute subplan on this datanode */
    Locator    *locator;                /* determine destination of tuples of
                                         * locally executed plan */
    int        *dest_nodes;                /* allocate once */
    List       *execNodes;                /* where to execute subplan */
    /* should query be executed on all (true) or any (false) node specified
     * in the execNodes list */
    bool         execOnAll;
    int            nParamRemote;    /* number of params sent from the master node */
    RemoteParam *remoteparams;  /* parameter descriptors */
    
#ifdef __TBASE__
    bool        finish_init;
    int32       eflags;                       /* estate flag. */
    ParallelWorkerStatus *parallel_status; /* Shared storage for parallel worker. */
#endif
} RemoteSubplanState;


/*
 * Data needed to set up a PreparedStatement on the remote node and other data
 * for the remote executor
 */
typedef struct RemoteStmt
{
    NodeTag        type;

    CmdType        commandType;    /* select|insert|update|delete */

    bool        hasReturning;    /* is it insert|update|delete RETURNING? */

#ifdef __TBASE__
    bool        parallelModeNeeded;     /* is parallel needed? */
    bool        parallelWorkerSendTuple;/* can parallel workers send tuples to remote? */
#endif

    struct Plan *planTree;                /* tree of Plan nodes */

    List       *rtable;                    /* list of RangeTblEntry nodes */

    /* rtable indexes of target relations for INSERT/UPDATE/DELETE */
    List       *resultRelations;    /* integer list of RT indexes, or NIL */

    List       *subplans;        /* Plan trees for SubPlan expressions */

    int            nParamExec;        /* number of PARAM_EXEC Params used */

    int            nParamRemote;    /* number of params sent from the master node */

    RemoteParam *remoteparams;  /* parameter descriptors */

    List       *rowMarks;

    char        distributionType;

    AttrNumber    distributionKey;

    List       *distributionNodes;

    List       *distributionRestrict;
#ifdef __TBASE__
    /* used for interval partition */
    bool        haspart_tobe_modify;
    Index        partrelindex;
    Bitmapset    *partpruning;
#endif

#ifdef __AUDIT__
    const char  *queryString;
    Query         *parseTree;
#endif
} RemoteStmt;

#ifdef __TBASE__
typedef enum
{
    TXN_TYPE_CommitTxn,
    TXN_TYPE_CommitSubTxn,
    TXN_TYPE_RollbackTxn,
    TXN_TYPE_RollbackSubTxn,
    TXN_TYPE_CleanConnection,

    TXN_TYPE_Butt
}TranscationType;
#endif


extern int PGXLRemoteFetchSize;


#ifdef __TBASE__
extern PGDLLIMPORT int g_in_plpgsql_exec_fun;
#endif


typedef void (*xact_callback) (bool isCommit, void *args);

/* Copy command just involves Datanodes */
extern void DataNodeCopyBegin(RemoteCopyData *rcstate);
extern int DataNodeCopyIn(char *data_row, int len, int conn_count,
                          PGXCNodeHandle** copy_connections,
                          bool binary);
extern uint64 DataNodeCopyOut(PGXCNodeHandle** copy_connections,
                              int conn_count, FILE* copy_file);
extern uint64 DataNodeCopyStore(PGXCNodeHandle** copy_connections,
                                int conn_count, Tuplestorestate* store);
extern void DataNodeCopyFinish(int conn_count, PGXCNodeHandle** connections);
extern int DataNodeCopyInBinaryForAll(char *msg_buf, int len, int conn_count,
                                      PGXCNodeHandle** connections);
extern bool DataNodeCopyEnd(PGXCNodeHandle *handle, bool is_error);

#ifdef __TBASE__
extern PGXCNodeAllHandles *get_exec_connections_all_dn(bool is_global_session);
#endif

extern RemoteQueryState *ExecInitRemoteQuery(RemoteQuery *node, EState *estate, int eflags);
extern TupleTableSlot* ExecRemoteQuery(PlanState *pstate);
extern void ExecReScanRemoteQuery(RemoteQueryState *node);
extern void ExecEndRemoteQuery(RemoteQueryState *step);
extern void RemoteSubplanMakeUnique(Node *plan, int unique, int pid);
extern RemoteSubplanState *ExecInitRemoteSubplan(RemoteSubplan *node, EState *estate, int eflags);
extern void ExecFinishInitRemoteSubplan(RemoteSubplanState *node);
extern TupleTableSlot* ExecRemoteSubplan(PlanState *pstate);
extern void ExecEndRemoteSubplan(RemoteSubplanState *node);
extern void ExecReScanRemoteSubplan(RemoteSubplanState *node);
#ifdef __TBASE__
extern void ExecRemoteUtility(RemoteQuery *node,
								PGXCNodeHandle *leader_cn_conn,
								ParallelDDLRemoteType type);
#else
extern void ExecRemoteUtility(RemoteQuery *node);
#endif

extern bool    is_data_node_ready(PGXCNodeHandle * conn);

extern int handle_response(PGXCNodeHandle *conn, ResponseCombiner *combiner);
extern void HandleCmdComplete(CmdType commandType, CombineTag *combine, const char *msg_body,
                                    size_t len);

#define CHECK_OWNERSHIP(conn, node) \
    do { \
        if ((conn)->state == DN_CONNECTION_STATE_QUERY && \
                (conn)->combiner && \
                (conn)->combiner != (ResponseCombiner *) (node)) \
			BufferConnection(conn, true); \
        (conn)->combiner = (ResponseCombiner *) (node); \
    } while(0)

extern TupleTableSlot *FetchTuple(ResponseCombiner *combiner);
extern void InitResponseCombiner(ResponseCombiner *combiner, int node_count,
                       CombineType combine_type);
extern void CloseCombiner(ResponseCombiner *combiner);
extern void BufferConnection(PGXCNodeHandle *conn, bool need_prefetch);
extern bool PreFetchConnection(PGXCNodeHandle *conn, int32 node_index);

extern void ExecRemoteQueryReScan(RemoteQueryState *node, ExprContext *exprCtxt);

extern void SetDataRowForExtParams(ParamListInfo params, RemoteQueryState *rq_state);

extern void ExecCloseRemoteStatement(const char *stmt_name, List *nodelist);
extern char *PrePrepare_Remote(char *prepareGID, bool localNode, bool implicit);
extern void PostPrepare_Remote(char *prepareGID, bool implicit);
extern void PreCommit_Remote(char *prepareGID, char *nodestring, bool preparedLocalNode);
extern bool    PreAbort_Remote(TranscationType txn_type, bool need_release_handle);
#ifdef __TBASE__
extern void SubTranscation_PreCommit_Remote(void);
extern void SubTranscation_PreAbort_Remote(void);
#endif
extern void AtEOXact_Remote(void);
extern bool IsTwoPhaseCommitRequired(bool localWrite);
extern bool FinishRemotePreparedTransaction(char *prepareGID, bool commit);
extern char *GetImplicit2PCGID(const char *implicit2PC_head, bool localWrite);

extern void pgxc_all_success_nodes(ExecNodes **d_nodes, ExecNodes **c_nodes, char **failednodes_msg);
extern void AtEOXact_DBCleanup(bool isCommit);

extern void set_dbcleanup_callback(xact_callback function, void *paraminfo, int paraminfo_size);
#ifdef __TBASE__
extern void ExecRemoteSubPlanInitializeDSM(RemoteSubplanState *node, ParallelContext *pcxt);
extern void ExecRemoteQueryInitializeDSM(RemoteQueryState *node, ParallelContext *pcxt);
extern void ExecRemoteSubPlanInitDSMWorker(RemoteSubplanState *node,
                                                  ParallelWorkerContext *pwcxt);
extern void ExecRemoteQueryInitializeDSMWorker(RemoteQueryState *node,
                                               ParallelWorkerContext *pwcxt);

extern bool ExecRemoteDML(ModifyTableState *mtstate, ItemPointer tupleid, HeapTuple oldtuple,
              TupleTableSlot *slot, TupleTableSlot *planSlot, EState *estate, EPQState *epqstate,
              bool canSetTag, TupleTableSlot **returning, UPSERT_ACTION *result,
              ResultRelInfo *resultRelInfo, int rel_index);
extern void ExecDisconnectRemoteSubplan(RemoteSubplanState *node);
extern void SetCurrentHandlesReadonly(void);

extern TupleDesc create_tuple_desc(char *msg_body, size_t len);

extern void ExecFinishRemoteSubplan(RemoteSubplanState *node);
extern void ExecShutdownRemoteSubplan(RemoteSubplanState *node);
extern bool SetSnapshot(EState *state);

extern void ExecRemoteUtility_ParallelDDLMode(RemoteQuery *node,
							PGXCNodeHandle *leader_cn_handle);
extern void LeaderCnExecRemoteUtility(RemoteQuery *node,
								PGXCNodeHandle *leader_cn_conn,
								ResponseCombiner *combiner,
								bool need_tran_block,
								GlobalTransactionId gxid,
								Snapshot snapshot,
								CommandId cid);
#endif

extern void GetGlobInfoForRemoteUtility(RemoteQuery *node,
										GlobalTransactionId *gxid,
										Snapshot *snapshot);
extern void SendTxnInfo(RemoteQuery *node, PGXCNodeHandle *conn,
						CommandId cid, Snapshot snapshot);
extern bool CheckRemoteRespond(PGXCNodeHandle *conn,
								ResponseCombiner *combiner,
								int *index, int *conn_count);
extern void RemoteReceiveAndCheck(int conn_count,
									PGXCNodeHandle **conns,
									ResponseCombiner *combiner);

#ifdef __SUBSCRIPTION__
extern void pgxc_node_report_error(ResponseCombiner *combiner);
extern int pgxc_node_receive_responses(const int conn_count, PGXCNodeHandle ** connections,
                         struct timeval * timeout, ResponseCombiner *combiner);
extern bool validate_combiner(ResponseCombiner *combiner);
#endif

#ifdef __TWO_PHASE_TRANS__
extern char *get_nodelist(char * prepareGID, bool localNode, bool implicit);
extern void InitLocalTwoPhaseState(void);
extern void SetLocalTwoPhaseStateHandles(PGXCNodeAllHandles * handles);
extern void UpdateLocalTwoPhaseState(int result, PGXCNodeHandle * response_handle, int conn_index, char * errmsg);
extern void ClearLocalTwoPhaseState(void);
extern char *GetTransStateString(TwoPhaseTransState state);
extern char *GetConnStateString(ConnState state);
extern void get_partnodes(PGXCNodeAllHandles * handles, StringInfo participants);
extern void clean_stat_transaction(void);
#endif

#endif
