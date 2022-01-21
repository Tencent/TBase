/*-------------------------------------------------------------------------
 *
 * gtm-client.c
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *      $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
/* Time in seconds to wait for a response from GTM */
/* We should consider making this a GUC */
#ifndef CLIENT_GTM_TIMEOUT
#ifdef  GTM_DEBUG
#define CLIENT_GTM_TIMEOUT 3600
#else
#define CLIENT_GTM_TIMEOUT 20
#endif
#endif
#define MAX_RETRY_SLEEP_MICRO 1000000

#include <time.h>
#include <gtm/gtm_client.h>
#include "postgres.h"
#include "utils/timestamp.h"
#include "gtm/gtm_c.h"

#include "gtm/gtm_ip.h"
#include "gtm/libpq-fe.h"
#include "gtm/libpq-int.h"

#include "gtm/gtm_client.h"
#include "gtm/gtm_msg.h"
#include "gtm/gtm_xlog.h"
#include "gtm/gtm_serialize.h"
#include "gtm/register.h"
#include "gtm/assert.h"
#include "pgxc/pgxc.h"

extern bool Backup_synchronously;

#ifdef __XLOG__
extern char  *application_name;
#endif

void GTM_FreeResult(GTM_Result *result, GTM_PGXCNodeType remote_type);

static GTM_Result *makeEmptyResultIfIsNull(GTM_Result *oldres);
static int commit_prepared_transaction_internal(GTM_Conn *conn,
                                                GlobalTransactionId gxid, GlobalTransactionId prepared_gxid,
                                                int waited_xid_count,
                                                GlobalTransactionId *waited_xids,
                                                bool is_backup);
static int start_prepared_transaction_internal(GTM_Conn *conn, GlobalTransactionId gxid, char *gid,
                                               char *nodestring, bool is_backup);
static int
log_commit_transaction_internal(GTM_Conn *conn, 
                                                GlobalTransactionId gxid, 
                                                const char *gid,
                                                  const char *nodestring,
                                                  int node_count,
                                                  bool isGlobal,
                                                  bool isCommit,
                                                  GlobalTimestamp prepare_ts, 
                                                  GlobalTimestamp commit_ts);
static int
log_scan_transaction_internal(GTM_Conn *conn, 
                                              GlobalTransactionId gxid, 
                                              const char *node_string, 
                                              GlobalTimestamp     start_ts,
                                              GlobalTimestamp     local_start_ts,
                                              GlobalTimestamp     local_complete_ts,
                                              int scan_type,
                                              const char *rel_name,
                                             int64  scan_number);
static int prepare_transaction_internal(GTM_Conn *conn, GlobalTransactionId gxid, bool is_backup);
static int abort_transaction_internal(GTM_Conn *conn, GlobalTransactionId gxid, bool is_backup);
static int abort_transaction_multi_internal(GTM_Conn *conn, int txn_count, GlobalTransactionId *gxid,
                                            int *txn_count_out, int *status_out, bool is_backup);
static int open_sequence_internal(GTM_Conn *conn, GTM_SequenceKey key, GTM_Sequence increment,
                                  GTM_Sequence minval, GTM_Sequence maxval,
                                  GTM_Sequence startval, bool cycle,
                                  GlobalTransactionId gxid, bool is_backup);
static int get_next_internal(GTM_Conn *conn, GTM_SequenceKey key,
                  char *coord_name, int coord_procid, GTM_Sequence range,
                  GTM_Sequence *result, GTM_Sequence *rangemax, bool is_backup);
static int set_val_internal(GTM_Conn *conn, GTM_SequenceKey key,
                 char *coord_name, int coord_procid, GTM_Sequence nextval,
                 bool iscalled, bool is_backup);
static int reset_sequence_internal(GTM_Conn *conn, GTM_SequenceKey key, bool is_backup);
static int commit_transaction_internal(GTM_Conn *conn, GlobalTransactionId gxid,
        int waited_xid_count,
        GlobalTransactionId *waited_xids,
        bool is_backup);
static int close_sequence_internal(GTM_Conn *conn, GTM_SequenceKey key,
        GlobalTransactionId gxid, bool is_backup);
static int rename_sequence_internal(GTM_Conn *conn, GTM_SequenceKey key,
        GTM_SequenceKey newkey, GlobalTransactionId gxid, bool is_backup);
static int alter_sequence_internal(GTM_Conn *conn, GTM_SequenceKey key, GTM_Sequence increment,
        GTM_Sequence minval, GTM_Sequence maxval,
        GTM_Sequence startval, GTM_Sequence lastval, bool cycle,
        bool is_restart, bool is_backup);
static int node_register_worker(GTM_Conn *conn, GTM_PGXCNodeType type, const char *host, GTM_PGXCNodePort port,
                                char *node_name, char *datafolder,
                                GTM_PGXCNodeStatus status, bool is_backup);
static int node_unregister_worker(GTM_Conn *conn, GTM_PGXCNodeType type, const char * node_name, bool is_backup);
static int report_barrier_internal(GTM_Conn *conn, const char *barrier_id, bool is_backup);
/*
 * Make an empty result if old one is null.
 */
static GTM_Result *
makeEmptyResultIfIsNull(GTM_Result *oldres)
{
    GTM_Result *res = NULL;

    if (oldres == NULL)
    {
        res = (GTM_Result *) malloc(sizeof(GTM_Result));
        memset(res, 0, sizeof(GTM_Result));
    }
    else
        return oldres;

    return res;
}

/*
 * Connection Management API
 */
GTM_Conn *
connect_gtm(const char *connect_string)
{
    return PQconnectGTM(connect_string);
}

void
disconnect_gtm(GTM_Conn *conn)
{
    GTMPQfinish(conn);
}

/*
 * begin_replication_initial_sync() acquires several locks to prepare
 * for copying internal transaction, xid and sequence information
 * to the standby node at its startup.
 *
 * returns 1 on success, 0 on failure.
 */
int
begin_replication_initial_sync(GTM_Conn *conn)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;

     /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(MSG_NODE_BEGIN_REPLICATION_INIT, sizeof (GTM_MessageType), conn))
        goto send_failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
    if (gtmpqWaitTimed(true, false, conn, finish_time) ||
        gtmpqReadData(conn) < 0)
        goto receive_failed;

    if ((res = GTMPQgetResult(conn)) == NULL)
        goto receive_failed;

    if (res->gr_status == GTM_RESULT_OK)
    {
        Assert(res->gr_type == NODE_BEGIN_REPLICATION_INIT_RESULT);
        if (res->gr_type != NODE_BEGIN_REPLICATION_INIT_RESULT)
        {
            fprintf(stderr, "res->gr_type %d not match, expected %d.\n", res->gr_type, NODE_BEGIN_REPLICATION_INIT_RESULT);
            goto receive_failed;
        }
    }
    else
        return 0;

    return 1;

receive_failed:
send_failed:
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return 0;
}

/*
 * end_replication_initial_sync() releases several locks
 * after copying internal transaction, xid and sequence information
 * to the standby node at its startup.
 *
 * returns 1 on success, 0 on failure.
 */
int
end_replication_initial_sync(GTM_Conn *conn)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;

     /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(MSG_NODE_END_REPLICATION_INIT, sizeof (GTM_MessageType), conn))
        goto send_failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
    if (gtmpqWaitTimed(true, false, conn, finish_time) ||
        gtmpqReadData(conn) < 0)
        goto receive_failed;

    if ((res = GTMPQgetResult(conn)) == NULL)
        goto receive_failed;

    if (res->gr_status == GTM_RESULT_OK)
    {
        Assert(res->gr_type == NODE_END_REPLICATION_INIT_RESULT);
        if (res->gr_type != NODE_END_REPLICATION_INIT_RESULT)
        {
            fprintf(stderr, "res->gr_type %d not match, expected %d.\n", res->gr_type, NODE_END_REPLICATION_INIT_RESULT);
            goto receive_failed;
        }
    }

    return 1;

receive_failed:
send_failed:
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return 0;
}

/*
 * get_node_list()
 *
 * returns a number of nodes on success, -1 on failure.
 */
size_t
get_node_list(GTM_Conn *conn, GTM_PGXCNodeInfo *data, size_t maxlen)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;
    size_t num_node;
    int i;

     /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(MSG_NODE_LIST, sizeof (GTM_MessageType), conn))
        goto send_failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
    if (gtmpqWaitTimed(true, false, conn, finish_time) ||
        gtmpqReadData(conn) < 0)
        goto receive_failed;

    if ((res = GTMPQgetResult(conn)) == NULL)
        goto receive_failed;

    if (res->gr_status == GTM_RESULT_OK)
    {
        Assert(res->gr_type == NODE_LIST_RESULT);
        if (res->gr_type != NODE_LIST_RESULT)
        {
            fprintf(stderr, "res->gr_type %d not match, expected %d.\n", res->gr_type, NODE_LIST_RESULT);
            goto receive_failed;
        }
    }

    /*
     * Do something here.
     */
    num_node = res->gr_resdata.grd_node_list.num_node;

    fprintf(stderr, "get_node_list: num_node=%ld\n", num_node);
    if (num_node > maxlen)
    {
        fprintf(stderr, "Error: number of nodes %zu greater than maximum", num_node);
        goto receive_failed;
    }

    for (i = 0; i < num_node; i++)
    {
        memcpy(&data[i], res->gr_resdata.grd_node_list.nodeinfo[i], sizeof(GTM_PGXCNodeInfo));
    }

    return num_node;

receive_failed:
send_failed:
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;
}

/*
 * get_next_gxid()
 *
 * returns the next gxid on success, InvalidGlobalTransactionId on failure.
 */
GlobalTransactionId
get_next_gxid(GTM_Conn *conn)
{// #lizard forgives
    GTM_Result *res = NULL;
    GlobalTransactionId next_gxid;
    time_t finish_time;

     /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(MSG_TXN_GET_NEXT_GXID, sizeof (GTM_MessageType), conn))
        goto send_failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
    if (gtmpqWaitTimed(true, false, conn, finish_time) ||
        gtmpqReadData(conn) < 0)
        goto receive_failed;

    if ((res = GTMPQgetResult(conn)) == NULL)
        goto receive_failed;

    fprintf(stderr, "GTMPQgetResult() done.\n");
    fflush(stderr);

    next_gxid = res->gr_resdata.grd_next_gxid;

    if (res->gr_status == GTM_RESULT_OK)
    {
        Assert(res->gr_type == TXN_GET_NEXT_GXID_RESULT);
        if (res->gr_type != TXN_GET_NEXT_GXID_RESULT)
        {
            fprintf(stderr,"res->gr_type %d not match, expected %d.\n", res->gr_type, TXN_GET_NEXT_GXID_RESULT);
            goto receive_failed;
        }
    }

    /* FIXME: should be a number of gxids */
    return next_gxid;

receive_failed:
send_failed:
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return InvalidGlobalTransactionId;
}

/*
 * get_txn_gxid_list()
 *
 * returns a number of gxid on success, -1 on failure.
 */
uint32
get_txn_gxid_list(GTM_Conn *conn, GTM_Transactions *txn)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;
    int txn_count;

     /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(MSG_TXN_GXID_LIST, sizeof (GTM_MessageType), conn))
        goto send_failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
    if (gtmpqWaitTimed(true, false, conn, finish_time) ||
        gtmpqReadData(conn) < 0)
        goto receive_failed;

    if ((res = GTMPQgetResult(conn)) == NULL)
        goto receive_failed;

    if (res->gr_status == GTM_RESULT_OK)
    {
        Assert(res->gr_type == TXN_GXID_LIST_RESULT);
        if (res->gr_type != TXN_GXID_LIST_RESULT)
        {
            fprintf(stderr, "res->gr_type %d not match, expected %d.\n", res->gr_type, TXN_GXID_LIST_RESULT);
            goto receive_failed;
        }
    }

    txn_count = gtm_deserialize_transactions(txn,
                         res->gr_resdata.grd_txn_gid_list.ptr,
                         res->gr_resdata.grd_txn_gid_list.len);

    return txn_count;

receive_failed:
send_failed:
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;
}

/*
 * get_sequence_list()
 *
 * returns a number of sequences on success, -1 on failure.
 * Returned seq_list is pointing to GTM_Result structure, the data should be
 * copied before the next call to getResult.
 */
size_t
get_sequence_list(GTM_Conn *conn, GTM_SeqInfo **seq_list)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;

     /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(MSG_SEQUENCE_LIST, sizeof (GTM_MessageType), conn))
        goto send_failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
    if (gtmpqWaitTimed(true, false, conn, finish_time) ||
        gtmpqReadData(conn) < 0)
        goto receive_failed;

    if ((res = GTMPQgetResult(conn)) == NULL)
        goto receive_failed;

    if (res->gr_status == GTM_RESULT_OK)
    {
        Assert(res->gr_type == SEQUENCE_LIST_RESULT);
        if (res->gr_type != SEQUENCE_LIST_RESULT)
        {
            fprintf(stderr, "res->gr_type %d not match, expected %d.\n", res->gr_type, SEQUENCE_LIST_RESULT);
            goto receive_failed;
        }
    }

    *seq_list = res->gr_resdata.grd_seq_list.seq;

    return res->gr_resdata.grd_seq_list.seq_count;

receive_failed:
send_failed:
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;
}

#ifdef __TBASE__
int
bkup_global_timestamp(GTM_Conn *conn, GlobalTimestamp timestamp)
{

     /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(MSG_BKUP_GLOBAL_TIMESTAMP, sizeof (GTM_MessageType), conn) ||
        gtmpqPutnchar((char *)&timestamp, sizeof(GlobalTimestamp), conn))
        goto send_failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    return 0;

send_failed:
    return -1;

}


Get_GTS_Result
get_global_timestamp(GTM_Conn *conn)
{// #lizard forgives
    GTM_Result    *res = NULL;
    Get_GTS_Result ret = {InvalidGlobalTimestamp,false};
    time_t finish_time;
    
     /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(MSG_GETGTS, sizeof (GTM_MessageType), conn))
        goto send_failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
    if (gtmpqWaitTimed(true, false, conn, finish_time) ||
        gtmpqReadData(conn) < 0)
        goto receive_failed;

    if ((res = GTMPQgetResult(conn)) == NULL)
        goto receive_failed;

    if (res->gr_status == GTM_RESULT_OK)
    {
        ret.gts = res->gr_resdata.grd_gts.grd_gts;
        ret.gtm_readonly = res->gr_resdata.grd_gts.gtm_readonly;
        
        return ret;
    }
    else
        return ret;

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return ret;
}


int
check_gtm_status(GTM_Conn *conn, int *status, GTM_Timestamp *master,XLogRecPtr *master_ptr,int *standby_count,int **slave_is_sync, GTM_Timestamp **standby
        ,XLogRecPtr **slave_flush_ptr,char **application_name[GTM_MAX_WALSENDER],int timeout_seconds)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;

     /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(MSG_CHECK_GTM_STATUS, sizeof (GTM_MessageType), conn))
        goto send_failed;

    if (gtmpqPutInt(timeout_seconds,sizeof(int),conn))
        goto send_failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    /* add two seconds to allow extra wait */
    finish_time = time(NULL) + timeout_seconds + 2;
    if (gtmpqWaitTimed(true, false, conn, finish_time) ||
        gtmpqReadData(conn) < 0)
        goto receive_failed;

    if ((res = GTMPQgetResult(conn)) == NULL)
        goto receive_failed;

    if (GTM_RESULT_OK == res->gr_status)
    {
        *status  =  res->gr_resdata.grd_gts.node_status;
        *master  =  res->gr_resdata.grd_gts.grd_gts;
        *master_ptr = res->gr_resdata.grd_gts.master_flush;

        *standby_count    = res->gr_resdata.grd_gts.standby_count;
        *slave_is_sync    = res->gr_resdata.grd_gts.slave_is_sync;
        *slave_flush_ptr  = res->gr_resdata.grd_gts.slave_flush_ptr;
        *standby          = res->gr_resdata.grd_gts.slave_timestamp;
        *application_name = res->gr_resdata.grd_gts.application_name;

        return GTM_RESULT_OK;
    }
    else
    {
        return GTM_RESULT_ERROR;
    }

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return GTM_RESULT_ERROR;
}

/*
 * to get GTM statistics info
 */
int
get_gtm_statistics(GTM_Conn *conn, int clear_flag, int timeout_seconds, GTM_StatisticsResult** result)
{
    GTM_Result *res = NULL;
    time_t finish_time;

    /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(MSG_GET_STATISTICS, sizeof (GTM_MessageType), conn))
        goto send_failed;

    if (gtmpqPutInt(clear_flag,sizeof(int),conn))
        goto send_failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    /* add two seconds to allow extra wait */
    finish_time = time(NULL) + timeout_seconds + 2;
    if (gtmpqWaitTimed(true, false, conn, finish_time) ||
        gtmpqReadData(conn) < 0)
        goto receive_failed;

    if ((res = GTMPQgetResult(conn)) == NULL)
        goto receive_failed;

    if (GTM_RESULT_OK == res->gr_status)
    {
        *result = &(res->gr_resdata.statistic_result);
        return GTM_RESULT_OK;
    }
    else
    {
        return GTM_RESULT_ERROR;
    }

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return GTM_RESULT_ERROR;
}

/*
 * to get gtm error log
 */
int
get_gtm_errlog(GTM_Conn *conn, int timeout_seconds, char** errlog, int* len)
{
    GTM_Result *res = NULL;
    time_t finish_time;

    /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(MSG_GET_ERRORLOG, sizeof (GTM_MessageType), conn))
        goto send_failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    /* add two seconds to allow extra wait */
    finish_time = time(NULL) + timeout_seconds + 2;
    if (gtmpqWaitTimed(true, false, conn, finish_time) ||
        gtmpqReadData(conn) < 0)
        goto receive_failed;

    if ((res = GTMPQgetResult(conn)) == NULL)
        goto receive_failed;

    if (GTM_RESULT_OK == res->gr_status)
    {
        *errlog = res->grd_errlog.errlog;
        *len = res->grd_errlog.len;
        return GTM_RESULT_OK;
    }
    else
    {
        return GTM_RESULT_ERROR;
    }

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return GTM_RESULT_ERROR;
}

#endif
/*
 * Transaction Management API
 */

int
bkup_begin_transaction(GTM_Conn *conn, GTM_IsolationLevel isolevel,
                       bool read_only,
                       const char *global_sessionid,
                       uint32 client_id, GTM_Timestamp timestamp)
{// #lizard forgives
    uint32 global_sessionid_len = global_sessionid ?
        strlen(global_sessionid) + 1 : 1;
    char *eos = "\0";

     /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(MSG_BKUP_TXN_BEGIN, sizeof (GTM_MessageType), conn) ||
        gtmpqPutInt(isolevel, sizeof (GTM_IsolationLevel), conn) ||
        gtmpqPutc(read_only, conn) ||
        gtmpqPutInt(global_sessionid_len, sizeof (uint32), conn) ||
        gtmpqPutnchar(global_sessionid ? global_sessionid : eos,
            global_sessionid_len, conn) ||
        gtmpqPutInt(client_id, sizeof (uint32), conn) ||
        gtmpqPutnchar((char *)&timestamp, sizeof(GTM_Timestamp), conn))
        goto send_failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    return 0;

send_failed:
    return -1;

}

int
bkup_begin_transaction_gxid(GTM_Conn *conn, GlobalTransactionId gxid,
                            GTM_IsolationLevel isolevel, bool read_only,
                            const char *global_sessionid,
                            uint32 client_id, GTM_Timestamp timestamp)
{// #lizard forgives
    uint32 global_sessionid_len = global_sessionid ?
        strlen(global_sessionid) + 1 : 1;
    char *eos = "\0";

     /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(MSG_BKUP_TXN_BEGIN_GETGXID, sizeof (GTM_MessageType), conn) ||
        gtmpqPutInt(gxid, sizeof(GlobalTransactionId), conn) ||
        gtmpqPutInt(isolevel, sizeof (GTM_IsolationLevel), conn) ||
        gtmpqPutc(read_only, conn) ||
        gtmpqPutInt(global_sessionid_len, sizeof (uint32), conn) ||
        gtmpqPutnchar(global_sessionid ? global_sessionid : eos,
            global_sessionid_len, conn) ||
        gtmpqPutInt(client_id, sizeof (uint32), conn) ||
        gtmpqPutnchar((char *)&timestamp, sizeof(GTM_Timestamp), conn))
        goto send_failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    return 0;

send_failed:
    return -1;
}

GlobalTransactionId
begin_transaction(GTM_Conn *conn, GTM_IsolationLevel isolevel,
        const char *global_sessionid,
        GTM_Timestamp *timestamp)
{// #lizard forgives
    bool txn_read_only = false;
    GTM_Result *res = NULL;
    time_t finish_time;
    uint32 global_sessionid_len = global_sessionid ?
        strlen(global_sessionid) + 1 : 1;
    char *eos = "\0";

     /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(MSG_TXN_BEGIN_GETGXID, sizeof (GTM_MessageType), conn) ||
        gtmpqPutInt(isolevel, sizeof (GTM_IsolationLevel), conn) ||
        gtmpqPutc(txn_read_only, conn) ||
        gtmpqPutInt(global_sessionid_len, sizeof (uint32), conn) ||
        gtmpqPutnchar(global_sessionid ? global_sessionid : eos,
            global_sessionid_len, conn))
        goto send_failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
    if (gtmpqWaitTimed(true, false, conn, finish_time) ||
        gtmpqReadData(conn) < 0)
        goto receive_failed;

    if ((res = GTMPQgetResult(conn)) == NULL)
        goto receive_failed;

    if (res->gr_status == GTM_RESULT_OK)
    {
        if (timestamp)
            *timestamp = res->gr_resdata.grd_gxid_tp.timestamp;

        return res->gr_resdata.grd_gxid_tp.gxid;
    }
    else
        return InvalidGlobalTransactionId;

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return InvalidGlobalTransactionId;
}


int
bkup_begin_transaction_autovacuum(GTM_Conn *conn, GlobalTransactionId gxid,
                                  GTM_IsolationLevel isolevel, uint32 client_id)
{// #lizard forgives
     /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(MSG_BKUP_TXN_BEGIN_GETGXID_AUTOVACUUM, sizeof (GTM_MessageType), conn) ||
        gtmpqPutInt(gxid, sizeof(GlobalTransactionId), conn) ||
        gtmpqPutInt(isolevel, sizeof (GTM_IsolationLevel), conn) ||
        gtmpqPutInt(client_id, sizeof (uint32), conn))
        goto send_failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    return 0;

send_failed:
    return -1;
}
/*
 * Transaction Management API
 * Begin a transaction for an autovacuum worker process
 */
GlobalTransactionId
begin_transaction_autovacuum(GTM_Conn *conn, GTM_IsolationLevel isolevel)
{// #lizard forgives
    bool txn_read_only = false;
    GTM_Result *res = NULL;
    time_t finish_time;

     /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(MSG_TXN_BEGIN_GETGXID_AUTOVACUUM, sizeof (GTM_MessageType), conn) ||
        gtmpqPutInt(isolevel, sizeof (GTM_IsolationLevel), conn) ||
        gtmpqPutc(txn_read_only, conn))
        goto send_failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
    if (gtmpqWaitTimed(true, false, conn, finish_time) ||
        gtmpqReadData(conn) < 0)
        goto receive_failed;

    if ((res = GTMPQgetResult(conn)) == NULL)
        goto receive_failed;

    if (res->gr_status == GTM_RESULT_OK)
        return res->gr_resdata.grd_gxid;
    else
        return InvalidGlobalTransactionId;

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return InvalidGlobalTransactionId;
}

int
bkup_commit_transaction(GTM_Conn *conn, GlobalTransactionId gxid)
{
    return commit_transaction_internal(conn, gxid, 0, NULL, true);
}


int
commit_transaction(GTM_Conn *conn, GlobalTransactionId gxid,
        int waited_xid_count, GlobalTransactionId *waited_xids)
{
    if (waited_xid_count == 0)
    {
        int txn_count_out;
        int status_out;
        int status;
        status = commit_transaction_multi(conn, 1, &gxid, &txn_count_out,
                &status_out);
        return status;
    }
    else
        return commit_transaction_internal(conn, gxid, waited_xid_count,
                waited_xids, false);
}


static int
commit_transaction_internal(GTM_Conn *conn, GlobalTransactionId gxid,
        int waited_xid_count, GlobalTransactionId *waited_xids,
        bool is_backup)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;
    long   retry_sleep = 1000;

retry:
     /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(is_backup ? MSG_BKUP_TXN_COMMIT : MSG_TXN_COMMIT, sizeof (GTM_MessageType), conn) ||
        gtmpqPutnchar((char *)&gxid, sizeof (GlobalTransactionId), conn) ||
        gtmpqPutInt(waited_xid_count, sizeof (int), conn))
        goto send_failed;

    if (waited_xid_count > 0)
    {
        if (gtmpqPutnchar((char *) waited_xids, waited_xid_count * sizeof (GlobalTransactionId), conn))
            goto send_failed;
    }

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    if (!is_backup)
    {
        finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
        if (gtmpqWaitTimed(true, false, conn, finish_time) ||
            gtmpqReadData(conn) < 0)
            goto receive_failed;

        if ((res = GTMPQgetResult(conn)) == NULL)
            goto receive_failed;

        if (res->gr_status == GTM_RESULT_OK)
        {
            Assert(res->gr_type == TXN_COMMIT_RESULT);
            Assert(res->gr_resdata.grd_gxid == gxid);
            if (res->gr_type != TXN_COMMIT_RESULT)
            {
                fprintf(stderr, "res->gr_type %d not match, expected %d.\n", res->gr_type, TXN_COMMIT_RESULT);
                goto receive_failed;
            }

            if (waited_xid_count > 0)
            {
                if (res->gr_resdata.grd_eof_txn.status == STATUS_DELAYED)
                {
                    /*
                     * GTM may decide to delay the transaction commit if one or
                     * more of the XIDs we had waited to finish for hasn't yet
                     * made to the GTM. While this window is very small, we
                     * need to guard against that to ensure that a transaction
                     * which is already seen as committed by datanodes is not
                     * reported as in-progress by GTM. Also, we don't wait at
                     * the GTM for other transactions to finish because that
                     * could potentially lead to deadlocks. So instead just
                     * sleep for a while (1ms right now) and retry the
                     * operation.
                     *
                     * Since the transactions we are waiting for are in fact
                     * already committed and hence we don't see a reason why we
                     * might end up in an inifinite loop. Nevertheless, it
                     * might make sense to flash a warning and proceed after
                     * certain number of retries
                     */
                    if (retry_sleep <= MAX_RETRY_SLEEP_MICRO)
                    {
                        retry_sleep = retry_sleep * 2;
                        if (retry_sleep > MAX_RETRY_SLEEP_MICRO)
                            retry_sleep = MAX_RETRY_SLEEP_MICRO;
                    }
                    pg_usleep(retry_sleep);
                    goto retry;
                }
            }
        }

        return res->gr_status;
    }
    return GTM_RESULT_OK;

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;
}

int
commit_prepared_transaction(GTM_Conn *conn,
        GlobalTransactionId gxid,
        GlobalTransactionId prepared_gxid,
        int waited_xid_count,
        GlobalTransactionId *waited_xids)
{
    return commit_prepared_transaction_internal(conn, gxid, prepared_gxid,
            waited_xid_count, waited_xids, false);
}

int
bkup_commit_prepared_transaction(GTM_Conn *conn, GlobalTransactionId gxid, GlobalTransactionId prepared_gxid)
{
    return commit_prepared_transaction_internal(conn, gxid, prepared_gxid, 0,
            NULL, true);
}

static int
commit_prepared_transaction_internal(GTM_Conn *conn,
        GlobalTransactionId gxid,
        GlobalTransactionId prepared_gxid,
        int waited_xid_count,
        GlobalTransactionId *waited_xids,
        bool is_backup)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;
    long   retry_sleep = 1000;

retry:
    /* Start the message */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(is_backup ? MSG_BKUP_TXN_COMMIT_PREPARED : MSG_TXN_COMMIT_PREPARED, sizeof (GTM_MessageType), conn) ||
        gtmpqPutnchar((char *)&gxid, sizeof (GlobalTransactionId), conn) ||
        gtmpqPutnchar((char *)&prepared_gxid, sizeof (GlobalTransactionId), conn) ||
        gtmpqPutInt(waited_xid_count, sizeof (int), conn))
        goto send_failed;

    if (waited_xid_count > 0)
    {
        if (gtmpqPutnchar((char *) waited_xids, waited_xid_count * sizeof (GlobalTransactionId), conn))
            goto send_failed;
    }

    /* Finish the message */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backends gets it */
    if (gtmpqFlush(conn))
        goto send_failed;

    if (!is_backup)
    {
        finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
        if (gtmpqWaitTimed(true, false, conn, finish_time) ||
            gtmpqReadData(conn) < 0)
            goto receive_failed;

        if ((res = GTMPQgetResult(conn)) == NULL)
            goto receive_failed;

        if (res->gr_status == GTM_RESULT_OK)
        {
            Assert(res->gr_type == TXN_COMMIT_PREPARED_RESULT);
            Assert(res->gr_resdata.grd_gxid == gxid);
            if (res->gr_type != TXN_COMMIT_PREPARED_RESULT)
            {
                fprintf(stderr, "res->gr_type %d not match, expected %d.\n", res->gr_type, TXN_COMMIT_PREPARED_RESULT);
                goto receive_failed;
            }

            if (waited_xid_count > 0)
            {
                if (res->gr_resdata.grd_eof_txn.status == STATUS_DELAYED)
                {
                    /* See comments in commit_transaction_internal() */
                    if (retry_sleep <= MAX_RETRY_SLEEP_MICRO)
                    {
                        retry_sleep = retry_sleep * 2;
                        if (retry_sleep > MAX_RETRY_SLEEP_MICRO)
                            retry_sleep = MAX_RETRY_SLEEP_MICRO;
                    }
                    pg_usleep(retry_sleep);
                    goto retry;
                }
            }
        }

        return res->gr_status;
    }
    return GTM_RESULT_OK;

send_failed:
receive_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;
}

int
abort_transaction(GTM_Conn *conn, GlobalTransactionId gxid)
{
    return abort_transaction_internal(conn, gxid, false);
}

int
bkup_abort_transaction(GTM_Conn *conn, GlobalTransactionId gxid)
{
    return abort_transaction_internal(conn, gxid, true);
}

static int
abort_transaction_internal(GTM_Conn *conn, GlobalTransactionId gxid, bool is_backup)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;

     /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(is_backup ? MSG_BKUP_TXN_ROLLBACK : MSG_TXN_ROLLBACK, sizeof (GTM_MessageType), conn) ||
        gtmpqPutnchar((char *)&gxid, sizeof (GlobalTransactionId), conn))
        goto send_failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    if (!is_backup)
    {
        finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
        if (gtmpqWaitTimed(true, false, conn, finish_time) ||
            gtmpqReadData(conn) < 0)
            goto receive_failed;

        if ((res = GTMPQgetResult(conn)) == NULL)
            goto receive_failed;

        if (res->gr_status == GTM_RESULT_OK)
        {
            Assert(res->gr_type == TXN_ROLLBACK_RESULT);
            Assert(res->gr_resdata.grd_gxid == gxid);
            if (res->gr_type != TXN_ROLLBACK_RESULT)
            {
                fprintf(stderr, "res->gr_type %d not match, expected %d.\n", res->gr_type, TXN_ROLLBACK_RESULT);
                goto receive_failed;
            }
        }

        return res->gr_status;
    }
    return GTM_RESULT_OK;

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;

}

int
backup_start_prepared_transaction(GTM_Conn *conn, GlobalTransactionId gxid, char *gid,
                                  char *nodestring)
{
    Assert(nodestring && gid && conn);

    return start_prepared_transaction_internal(conn, gxid, gid, nodestring, true);
}

int
start_prepared_transaction(GTM_Conn *conn, GlobalTransactionId gxid, char *gid,
                           char *nodestring)
{
    return start_prepared_transaction_internal(conn, gxid, gid, nodestring, false);
}

static int
start_prepared_transaction_internal(GTM_Conn *conn, GlobalTransactionId gxid, char *gid,
                           char *nodestring, bool is_backup)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;

    Assert(nodestring);

     /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(is_backup ? MSG_BKUP_TXN_START_PREPARED : MSG_TXN_START_PREPARED, sizeof (GTM_MessageType), conn) ||
        gtmpqPutnchar((char *)&gxid, sizeof (GlobalTransactionId), conn) ||
        /* Send also GID for an explicit prepared transaction */
        gtmpqPutInt(strlen(gid), sizeof (GTM_StrLen), conn) ||
        gtmpqPutnchar((char *) gid, strlen(gid), conn) ||
        gtmpqPutInt(strlen(nodestring), sizeof (GTM_StrLen), conn) ||
        gtmpqPutnchar((char *) nodestring, strlen(nodestring), conn))
        goto send_failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    if (is_backup)
        return GTM_RESULT_OK;

    finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
    if (gtmpqWaitTimed(true, false, conn, finish_time) ||
        gtmpqReadData(conn) < 0)
        goto receive_failed;

    if ((res = GTMPQgetResult(conn)) == NULL)
        goto receive_failed;

    if (res->gr_status == GTM_RESULT_OK)
    {
        Assert(res->gr_type == TXN_START_PREPARED_RESULT);
        Assert(res->gr_resdata.grd_gxid == gxid);
        if (res->gr_type != TXN_START_PREPARED_RESULT)
        {
            fprintf(stderr, "res->gr_type %d not match, expected %d.\n", res->gr_type, TXN_START_PREPARED_RESULT);
            goto receive_failed;
        }
    }

    return res->gr_status;

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;
}

int
log_commit_transaction(GTM_Conn *conn, GlobalTransactionId gxid, const char *gid,
                           const char *nodestring, int node_count, bool isGlobal, bool isCommit, 
                           GlobalTimestamp prepare_ts, GlobalTimestamp commit_ts)
{
    return log_commit_transaction_internal(conn, 
                                           gxid, 
                                           gid, 
                                           nodestring, 
                                           node_count, 
                                           isGlobal, 
                                           isCommit, 
                                           prepare_ts, 
                                           commit_ts);
}

static int
log_commit_transaction_internal(GTM_Conn *conn, 
                                                GlobalTransactionId gxid, 
                                                const char *gid,
                                                  const char *nodestring,
                                                  int node_count,
                                                  bool isGlobal,
                                                  bool isCommit,
                                                  GlobalTimestamp prepare_ts, 
                                                  GlobalTimestamp commit_ts)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;
    int type = isGlobal? MSG_TXN_LOG_GLOBAL_COMMIT: MSG_TXN_LOG_COMMIT;

    Assert(nodestring);

     /* Start the message. */
    if(nodestring)
    {
        if (gtmpqPutMsgStart('C', true, conn) ||
            gtmpqPutInt(type, sizeof (GTM_MessageType), conn) ||
            gtmpqPutnchar((char *)&gxid, sizeof (GlobalTransactionId), conn) ||
            /* Send also GID for an explicit prepared transaction */
            gtmpqPutInt(strlen(gid), sizeof (GTM_StrLen), conn) ||
            gtmpqPutnchar((char *) gid, strlen(gid), conn) ||
            gtmpqPutInt(strlen(nodestring), sizeof (GTM_StrLen), conn) ||
            gtmpqPutnchar((char *) nodestring, strlen(nodestring), conn) ||
            gtmpqPutInt(node_count, sizeof (int), conn) ||
            gtmpqPutInt(isCommit, sizeof (int), conn) ||
            gtmpqPutnchar((char *) &prepare_ts, sizeof(GlobalTimestamp), conn) ||
            gtmpqPutnchar((char *) &commit_ts, sizeof(GlobalTimestamp), conn)
            )
            goto send_failed;
    }
    else
    {
        
        if (gtmpqPutMsgStart('C', true, conn) ||
            gtmpqPutInt(type, sizeof (GTM_MessageType), conn) ||
            gtmpqPutnchar((char *)&gxid, sizeof (GlobalTransactionId), conn) ||
            /* Send also GID for an explicit prepared transaction */
            gtmpqPutInt(strlen(gid), sizeof (GTM_StrLen), conn) ||
            gtmpqPutnchar((char *) gid, strlen(gid), conn) ||
            gtmpqPutInt(0, sizeof (GTM_StrLen), conn) ||
            gtmpqPutInt(node_count, sizeof (int), conn) ||
            gtmpqPutInt(isCommit, sizeof (int), conn) ||
            gtmpqPutnchar((char *) &prepare_ts, sizeof(GlobalTimestamp), conn) ||
            gtmpqPutnchar((char *) &commit_ts, sizeof(GlobalTimestamp), conn)
            )
            goto send_failed;

    }
    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
    if (gtmpqWaitTimed(true, false, conn, finish_time) ||
        gtmpqReadData(conn) < 0)
        goto receive_failed;

    if ((res = GTMPQgetResult(conn)) == NULL)
        goto receive_failed;

    if (res->gr_status == GTM_RESULT_OK)
    {
        Assert(res->gr_type == TXN_LOG_TRANSACTION_RESULT);
        Assert(res->gr_resdata.grd_gxid == gxid);
        if (res->gr_type != TXN_LOG_TRANSACTION_RESULT)
        {
            fprintf(stderr, "res->gr_type %d not match, expected %d.\n", res->gr_type, TXN_LOG_TRANSACTION_RESULT);
            goto receive_failed;
        }
    }

    return res->gr_status;

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;
}


int
log_scan_transaction(GTM_Conn *conn,
                             GlobalTransactionId gxid, 
                              const char *node_string, 
                              GlobalTimestamp     start_ts,
                              GlobalTimestamp     local_start_ts,
                              GlobalTimestamp     local_complete_ts,
                              int scan_type,
                              const char *rel_name,
                             int64  scan_number)
{
    return log_scan_transaction_internal(conn, 
                                         gxid,
                                         node_string,
                                         start_ts,
                                         local_start_ts,
                                         local_complete_ts,
                                         scan_type,
                                         rel_name,
                                         scan_number);
}

static int
log_scan_transaction_internal(GTM_Conn *conn, 
                                              GlobalTransactionId gxid, 
                                              const char *node_string, 
                                              GlobalTimestamp     start_ts,
                                              GlobalTimestamp     local_start_ts,
                                              GlobalTimestamp     local_complete_ts,
                                              int scan_type,
                                              const char *rel_name,
                                             int64  scan_number)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;

     /* Start the message. */

 if (gtmpqPutMsgStart('C', true, conn) ||
         gtmpqPutInt(MSG_TXN_LOG_SCAN, sizeof (GTM_MessageType), conn) ||
         gtmpqPutnchar((char *)&gxid, sizeof (GlobalTransactionId), conn) ||
         /* Send also GID for an explicit prepared transaction */
         gtmpqPutInt(strlen(node_string), sizeof (GTM_StrLen), conn) ||
         gtmpqPutnchar((char *) node_string, strlen(node_string), conn) ||
         gtmpqPutnchar((char *) &start_ts, sizeof(GlobalTimestamp), conn) ||
         gtmpqPutnchar((char *) &local_start_ts, sizeof(GlobalTimestamp), conn) ||
         gtmpqPutnchar((char *) &local_complete_ts, sizeof(GlobalTimestamp), conn) ||
         gtmpqPutInt(scan_type, sizeof (int), conn) ||
         gtmpqPutInt(strlen(rel_name), sizeof (GTM_StrLen), conn) ||
         gtmpqPutnchar((char *) rel_name, strlen(rel_name), conn) ||
         gtmpqPutnchar((char *) &scan_number, sizeof(int64), conn)
     )
        goto send_failed;


    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
    if (gtmpqWaitTimed(true, false, conn, finish_time) ||
        gtmpqReadData(conn) < 0)
        goto receive_failed;

    if ((res = GTMPQgetResult(conn)) == NULL)
        goto receive_failed;

    if (res->gr_status == GTM_RESULT_OK)
    {
        Assert(res->gr_type == TXN_LOG_SCAN_RESULT);
        Assert(res->gr_resdata.grd_gxid == gxid);
        if (res->gr_type != TXN_LOG_SCAN_RESULT)
        {
            fprintf(stderr, "res->gr_type %d not match, expected %d.\n", res->gr_type, TXN_LOG_SCAN_RESULT);
            goto receive_failed;
        }
    }

    return res->gr_status;

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;
}




int
prepare_transaction(GTM_Conn *conn, GlobalTransactionId gxid)
{
    return prepare_transaction_internal(conn, gxid, false);
}

int
bkup_prepare_transaction(GTM_Conn *conn, GlobalTransactionId gxid)
{
    return prepare_transaction_internal(conn, gxid, true);
}

static int
prepare_transaction_internal(GTM_Conn *conn, GlobalTransactionId gxid, bool is_backup)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;

     /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(is_backup ? MSG_BKUP_TXN_PREPARE : MSG_TXN_PREPARE, sizeof (GTM_MessageType), conn) ||
        gtmpqPutnchar((char *)&gxid, sizeof (GlobalTransactionId), conn))
        goto send_failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    if (!is_backup)
    {
        finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
        if (gtmpqWaitTimed(true, false, conn, finish_time) ||
            gtmpqReadData(conn) < 0)
            goto receive_failed;

        if ((res = GTMPQgetResult(conn)) == NULL)
            goto receive_failed;

        if (res->gr_status == GTM_RESULT_OK)
        {
            Assert(res->gr_type == TXN_PREPARE_RESULT);
            Assert(res->gr_resdata.grd_gxid == gxid);
            if (res->gr_type != TXN_PREPARE_RESULT)
            {
                fprintf(stderr, "res->gr_type %d not match, expected %d.\n", res->gr_type, TXN_PREPARE_RESULT);
                goto receive_failed;
            }
        }

        return res->gr_status;
    }
    return GTM_RESULT_OK;

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;
}

int
get_gid_data(GTM_Conn *conn,
             GTM_IsolationLevel isolevel,
             char *gid,
             GlobalTransactionId *gxid,
             GlobalTransactionId *prepared_gxid,
             char **nodestring)
{// #lizard forgives
    bool txn_read_only = false;
    GTM_Result *res = NULL;
    time_t finish_time;

    /* Start the message */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(MSG_TXN_GET_GID_DATA, sizeof (GTM_MessageType), conn) ||
        gtmpqPutInt(isolevel, sizeof (GTM_IsolationLevel), conn) ||
        gtmpqPutc(txn_read_only, conn) ||
        /* Send also GID for an explicit prepared transaction */
        gtmpqPutInt(strlen(gid), sizeof (GTM_StrLen), conn) ||
        gtmpqPutnchar((char *) gid, strlen(gid), conn))
        goto send_failed;

    /* Finish the message */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
    if (gtmpqWaitTimed(true, false, conn, finish_time) ||
        gtmpqReadData(conn) < 0)
        goto receive_failed;

    if ((res = GTMPQgetResult(conn)) == NULL)
        goto receive_failed;

    if (res->gr_status == GTM_RESULT_OK)
    {
        *gxid = res->gr_resdata.grd_txn_get_gid_data.gxid;
        *prepared_gxid = res->gr_resdata.grd_txn_get_gid_data.prepared_gxid;
        *nodestring = res->gr_resdata.grd_txn_get_gid_data.nodestring;
    }

    return res->gr_status;

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;
}
#ifdef __TBASE__
int
finish_gid_gtm(GTM_Conn *conn, char *gid)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;

    /* Start the message */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(MSG_TXN_FINISH_GID, sizeof (GTM_MessageType), conn) ||
        /* Send also GID for an explicit prepared transaction */
        gtmpqPutInt(strlen(gid), sizeof (GTM_StrLen), conn) ||
        gtmpqPutnchar((char *) gid, strlen(gid), conn))
        goto send_failed;

    /* Finish the message */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
    if (gtmpqWaitTimed(true, false, conn, finish_time) ||
        gtmpqReadData(conn) < 0)
        goto receive_failed;

    if ((res = GTMPQgetResult(conn)) == NULL)
        goto receive_failed;

    if (res->gr_status == GTM_RESULT_OK)
    {
        return res->gr_finish_status;
    }
    else
    {
        return res->gr_status;
    }

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;
}

int
get_gtm_store_status(GTM_Conn *conn, GTMStorageStatus *status)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;

    /* Start the message */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(MSG_LIST_GTM_STORE, sizeof (GTM_MessageType), conn) )
        goto send_failed;

    /* Finish the message */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
    if (gtmpqWaitTimed(true, false, conn, finish_time) ||
        gtmpqReadData(conn) < 0)
        goto receive_failed;

    if ((res = GTMPQgetResult(conn)) == NULL)
        goto receive_failed;

    if (res->gr_status == GTM_RESULT_OK)
    {
        memcpy((char*)status, (char*)&res->gtm_status, sizeof(GTMStorageStatus));
        return res->gr_finish_status;
    }
    else
    {
        return res->gr_status;
    }

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;
}
/*
 * get_storage_file()
 *
 * get storage file of primary.
 */
#ifdef  __XLOG__
size_t
get_storage_file(GTM_Conn *conn, char **data,XLogRecPtr *start_pos,TimeLineID *timeLineID)
#else
get_storage_file(GTM_Conn *conn, char **data)
#endif
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;

     /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(MSG_GET_STORAGE, sizeof (GTM_MessageType), conn))
        goto send_failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
    if (gtmpqWaitTimed(true, false, conn, finish_time) ||
        gtmpqReadData(conn) < 0)
        goto receive_failed;

    if ((res = GTMPQgetResult(conn)) == NULL)
        goto receive_failed;

    if (res->gr_status == GTM_RESULT_OK)
    {
        Assert(res->gr_type == STORAGE_TRANSFER_RESULT);
        if (res->gr_type != STORAGE_TRANSFER_RESULT)
        {
            fprintf(stderr, "res->gr_type %d not match, expected %d.\n", res->gr_type, STORAGE_TRANSFER_RESULT);
            goto receive_failed;
        }
    }

#ifdef __XLOG__
    *start_pos  = res->grd_storage_data.start_pos;
    *timeLineID = res->grd_storage_data.time_line;
#endif 

    *data = res->grd_storage_data.data;

    return (size_t)res->grd_storage_data.len;

receive_failed:
send_failed:
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;
}


int32
get_storage_sequence_list(GTM_Conn *conn, GTM_StoredSeqInfo **store_seq)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;

     /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(MSG_LIST_GTM_STORE_SEQ, sizeof (GTM_MessageType), conn))
        goto send_failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
    if (gtmpqWaitTimed(true, false, conn, finish_time) ||
        gtmpqReadData(conn) < 0)
        goto receive_failed;

    if ((res = GTMPQgetResult(conn)) == NULL)
        goto receive_failed;

    if (res->gr_status == GTM_RESULT_OK)
        Assert(res->gr_type != MSG_LIST_GTM_STORE_RESULT);

    *store_seq = res->grd_store_seq.seqs;

    return res->grd_store_seq.count;

receive_failed:
send_failed:
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;
}

int32
get_storage_transaction_list(GTM_Conn *conn, GTM_StoredTransactionInfo **store_txn)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;

     /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(MSG_LIST_GTM_STORE_TXN, sizeof (GTM_MessageType), conn))
        goto send_failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
    if (gtmpqWaitTimed(true, false, conn, finish_time) ||
        gtmpqReadData(conn) < 0)
        goto receive_failed;

    if ((res = GTMPQgetResult(conn)) == NULL)
        goto receive_failed;

    if (res->gr_status == GTM_RESULT_OK)
        Assert(res->gr_type != MSG_LIST_GTM_TXN_STORE_RESULT);

    *store_txn = res->grd_store_txn.txns;

    return res->grd_store_txn.count;

receive_failed:
send_failed:
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;
}

int32
check_storage_sequence(GTM_Conn *conn, GTMStorageSequneceStatus **store_seq, bool need_fix)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;

     /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(MSG_CHECK_GTM_STORE_SEQ, sizeof (GTM_MessageType), conn) ||
        gtmpqPutInt((int32)need_fix, sizeof (int32), conn))
        goto send_failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
    if (gtmpqWaitTimed(true, false, conn, finish_time) ||
        gtmpqReadData(conn) < 0)
        goto receive_failed;

    if ((res = GTMPQgetResult(conn)) == NULL)
        goto receive_failed;

    if (res->gr_status == GTM_RESULT_OK)
        Assert(res->gr_type != MSG_CHECK_GTM_SEQ_STORE_RESULT);

    *store_seq = res->grd_store_check_seq.seqs;
    return res->grd_store_check_seq.count;

receive_failed:
send_failed:
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;
}

int32
check_storage_transaction(GTM_Conn *conn, GTMStorageTransactionStatus **store_txn, bool need_fix)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;

     /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(MSG_CHECK_GTM_STORE_TXN, sizeof (GTM_MessageType), conn) ||
        gtmpqPutInt((int32)need_fix, sizeof (int32), conn))
        goto send_failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
    if (gtmpqWaitTimed(true, false, conn, finish_time) ||
        gtmpqReadData(conn) < 0)
        goto receive_failed;

    if ((res = GTMPQgetResult(conn)) == NULL)
        goto receive_failed;

    if (res->gr_status == GTM_RESULT_OK)
        Assert(res->gr_type != MSG_CHECK_GTM_TXN_STORE_RESULT);

    *store_txn = res->grd_store_check_txn.txns;
    return res->grd_store_check_txn.count;

receive_failed:
send_failed:
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;
}

static int
rename_db_sequence_internal(GTM_Conn *conn, GTM_SequenceKey key, GTM_SequenceKey newkey,
        GlobalTransactionId gxid, bool is_backup)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;

    /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(is_backup ? MSG_BKUP_DB_SEQUENCE_RENAME : MSG_DB_SEQUENCE_RENAME, sizeof (GTM_MessageType), conn) ||
        gtmpqPutInt(key->gsk_keylen, 4, conn) ||
        gtmpqPutnchar(key->gsk_key, key->gsk_keylen, conn)||
        gtmpqPutInt(newkey->gsk_keylen, 4, conn) ||
        gtmpqPutnchar(newkey->gsk_key, newkey->gsk_keylen, conn) ||
        gtmpqPutnchar((char *)&gxid, sizeof (GlobalTransactionId), conn))
        goto send_failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    if (!is_backup)
    {
        finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
        if (gtmpqWaitTimed(true, false, conn, finish_time) ||
            gtmpqReadData(conn) < 0)
            goto receive_failed;

        if ((res = GTMPQgetResult(conn)) == NULL)
            goto receive_failed;

        return res->gr_status;
    }
    return GTM_RESULT_OK;

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;
}

int
rename_db_sequence(GTM_Conn *conn, GTM_SequenceKey key, GTM_SequenceKey newkey,
        GlobalTransactionId gxid)
{    
    return rename_db_sequence_internal(conn, key, newkey, gxid, false);
}

#endif
/*
 * Snapshot Management API
 */
GTM_SnapshotData *
get_snapshot(GTM_Conn *conn, GlobalTransactionId gxid, bool canbe_grouped)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;
    GTM_ResultType res_type PG_USED_FOR_ASSERTS_ONLY;

    res_type = canbe_grouped ? SNAPSHOT_GET_MULTI_RESULT : SNAPSHOT_GET_RESULT;

     /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(canbe_grouped ? MSG_SNAPSHOT_GET_MULTI : MSG_SNAPSHOT_GET, sizeof (GTM_MessageType), conn) ||
        gtmpqPutInt(1, sizeof (int), conn) ||
        gtmpqPutnchar((char *)&gxid, sizeof (GlobalTransactionId), conn))
        goto send_failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
    if (gtmpqWaitTimed(true, false, conn, finish_time) ||
        gtmpqReadData(conn) < 0)
        goto receive_failed;

    if ((res = GTMPQgetResult(conn)) == NULL)
        goto receive_failed;

    if (res->gr_status == GTM_RESULT_OK)
    {
        Assert(res->gr_type == res_type);
        if (res->gr_type != res_type)
        {
            fprintf(stderr, "res->gr_type %d not match, expected %d.\n", res->gr_type, res_type);
            goto receive_failed;
        }
        /*
         * !!FIXME - The following assertion fails when snapshots are requested
         * in non-grouping mode. We did some investigations and it appears that
         * GTMProxy_ProxyCommand() fails to record the incoming GXID and later
         * sends down a wrong GXID to the client. We should probably look at
         * populating cmd_data member before proxying message to the GTM
         *
         * Commenting out the assertion till then
         *
         *    Assert(res->gr_resdata.grd_txn_snap_multi.gxid == gxid);
         */
        return &(res->gr_snapshot);
    }
    else
        return NULL;


receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return NULL;
}

/*
 * Sequence Management API
 */
int
open_sequence(GTM_Conn *conn, GTM_SequenceKey key, GTM_Sequence increment,
              GTM_Sequence minval, GTM_Sequence maxval,
              GTM_Sequence startval,
              bool cycle,
              GlobalTransactionId gxid)
{
    return open_sequence_internal(conn, key, increment, minval, maxval,
            startval, cycle, gxid, false);
}

int
bkup_open_sequence(GTM_Conn *conn, GTM_SequenceKey key, GTM_Sequence increment,
                   GTM_Sequence minval, GTM_Sequence maxval,
                   GTM_Sequence startval,
                   bool cycle,
                   GlobalTransactionId gxid)
{
    return open_sequence_internal(conn, key, increment, minval, maxval,
            startval, cycle, gxid, true);
}

static int
open_sequence_internal(GTM_Conn *conn, GTM_SequenceKey key, GTM_Sequence increment,
                       GTM_Sequence minval, GTM_Sequence maxval,
                       GTM_Sequence startval, bool cycle,
                       GlobalTransactionId gxid,
                       bool is_backup
                       )
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;

     /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(is_backup ? MSG_BKUP_SEQUENCE_INIT : MSG_SEQUENCE_INIT, sizeof (GTM_MessageType), conn) ||
        gtmpqPutInt(key->gsk_keylen, 4, conn) ||
        gtmpqPutnchar(key->gsk_key, key->gsk_keylen, conn) ||
        gtmpqPutnchar((char *)&increment, sizeof (GTM_Sequence), conn) ||
        gtmpqPutnchar((char *)&minval, sizeof (GTM_Sequence), conn) ||
        gtmpqPutnchar((char *)&maxval, sizeof (GTM_Sequence), conn) ||
        gtmpqPutnchar((char *)&startval, sizeof (GTM_Sequence), conn) ||
        gtmpqPutc(cycle, conn) ||
        gtmpqPutnchar((char *)&gxid, sizeof (GlobalTransactionId), conn))
        goto send_failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    if (!is_backup)
    {
        finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
        if (gtmpqWaitTimed(true, false, conn, finish_time) ||
            gtmpqReadData(conn) < 0)
            goto receive_failed;

        if ((res = GTMPQgetResult(conn)) == NULL)
            goto receive_failed;

        return res->gr_status;
    }
    return GTM_RESULT_OK;

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;
}

int
alter_sequence(GTM_Conn *conn, GTM_SequenceKey key, GTM_Sequence increment,
               GTM_Sequence minval, GTM_Sequence maxval,
               GTM_Sequence startval, GTM_Sequence lastval, bool cycle,
               bool is_restart)
{
    return alter_sequence_internal(conn, key, increment, minval, maxval,
            startval, lastval, cycle, is_restart, false);
}

int
bkup_alter_sequence(GTM_Conn *conn, GTM_SequenceKey key, GTM_Sequence increment,
                    GTM_Sequence minval, GTM_Sequence maxval,
                    GTM_Sequence startval, GTM_Sequence lastval, bool cycle,
                    bool is_restart)
{
    return alter_sequence_internal(conn, key, increment, minval, maxval,
            startval, lastval, cycle, is_restart, true);
}

static int
alter_sequence_internal(GTM_Conn *conn, GTM_SequenceKey key, GTM_Sequence increment,
                        GTM_Sequence minval, GTM_Sequence maxval,
                        GTM_Sequence startval, GTM_Sequence lastval, bool cycle,
                        bool is_restart, bool is_backup)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;

    /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(is_backup ? MSG_BKUP_SEQUENCE_ALTER : MSG_SEQUENCE_ALTER, sizeof (GTM_MessageType), conn) ||
        gtmpqPutInt(key->gsk_keylen, 4, conn) ||
        gtmpqPutnchar(key->gsk_key, key->gsk_keylen, conn) ||
        gtmpqPutnchar((char *)&increment, sizeof (GTM_Sequence), conn) ||
        gtmpqPutnchar((char *)&minval, sizeof (GTM_Sequence), conn) ||
        gtmpqPutnchar((char *)&maxval, sizeof (GTM_Sequence), conn) ||
        gtmpqPutnchar((char *)&startval, sizeof (GTM_Sequence), conn) ||
        gtmpqPutnchar((char *)&lastval, sizeof (GTM_Sequence), conn) ||
        gtmpqPutc(cycle, conn) ||
        gtmpqPutc(is_restart, conn))
        goto send_failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    if (!is_backup)
    {
        finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
        if (gtmpqWaitTimed(true, false, conn, finish_time) ||
            gtmpqReadData(conn) < 0)
            goto receive_failed;

        if ((res = GTMPQgetResult(conn)) == NULL)
            goto receive_failed;

        return res->gr_status;
    }
    return GTM_RESULT_OK;

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;
}

int
close_sequence(GTM_Conn *conn, GTM_SequenceKey key, GlobalTransactionId gxid)
{
    return close_sequence_internal(conn, key, gxid, false);
}

int
bkup_close_sequence(GTM_Conn *conn, GTM_SequenceKey key,
        GlobalTransactionId gxid)
{
    return close_sequence_internal(conn, key, gxid, true);
}

static int
close_sequence_internal(GTM_Conn *conn, GTM_SequenceKey key,
        GlobalTransactionId gxid,
        bool is_backup)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;

     /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(is_backup ? MSG_BKUP_SEQUENCE_CLOSE : MSG_SEQUENCE_CLOSE, sizeof (GTM_MessageType), conn) ||
        gtmpqPutInt(key->gsk_keylen, 4, conn) ||
        gtmpqPutnchar(key->gsk_key, key->gsk_keylen, conn) ||
        gtmpqPutnchar((char *)&key->gsk_type, sizeof(GTM_SequenceKeyType), conn) ||
        gtmpqPutnchar((char *)&gxid, sizeof (GlobalTransactionId), conn))
        goto send_failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    if (!is_backup)
    {
        finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
        if (gtmpqWaitTimed(true, false, conn, finish_time) ||
            gtmpqReadData(conn) < 0)
            goto receive_failed;

        if ((res = GTMPQgetResult(conn)) == NULL)
            goto receive_failed;

        return res->gr_status;
    }
    return GTM_RESULT_OK;

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;
}

int
rename_sequence(GTM_Conn *conn, GTM_SequenceKey key, GTM_SequenceKey newkey,
        GlobalTransactionId gxid)
{
    return rename_sequence_internal(conn, key, newkey, gxid, false);
}

int
bkup_rename_sequence(GTM_Conn *conn, GTM_SequenceKey key,
        GTM_SequenceKey newkey, GlobalTransactionId gxid)
{
    return rename_sequence_internal(conn, key, newkey, gxid, true);
}

static int
rename_sequence_internal(GTM_Conn *conn, GTM_SequenceKey key, GTM_SequenceKey newkey,
        GlobalTransactionId gxid, bool is_backup)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;

    /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(is_backup ? MSG_BKUP_SEQUENCE_RENAME : MSG_SEQUENCE_RENAME, sizeof (GTM_MessageType), conn) ||
        gtmpqPutInt(key->gsk_keylen, 4, conn) ||
        gtmpqPutnchar(key->gsk_key, key->gsk_keylen, conn)||
        gtmpqPutInt(newkey->gsk_keylen, 4, conn) ||
        gtmpqPutnchar(newkey->gsk_key, newkey->gsk_keylen, conn) ||
        gtmpqPutnchar((char *)&gxid, sizeof (GlobalTransactionId), conn))
        goto send_failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    if (!is_backup)
    {
        finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
        if (gtmpqWaitTimed(true, false, conn, finish_time) ||
            gtmpqReadData(conn) < 0)
            goto receive_failed;

        if ((res = GTMPQgetResult(conn)) == NULL)
            goto receive_failed;

        return res->gr_status;
    }
    return GTM_RESULT_OK;

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;
}

/*
 * Copy the database sequences from src database
 */
int
copy_database_sequence(GTM_Conn *conn, GTM_SequenceKey src_key, GTM_SequenceKey dest_key,
                         GlobalTransactionId gxid)
{
    GTM_Result *res = NULL;
    time_t finish_time;

    /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(MSG_SEQUENCE_COPY, sizeof (GTM_MessageType), conn) ||
        gtmpqPutInt(src_key->gsk_keylen, 4, conn) ||
        gtmpqPutnchar(src_key->gsk_key, src_key->gsk_keylen, conn)||
        gtmpqPutInt(dest_key->gsk_keylen, 4, conn) ||
        gtmpqPutnchar(dest_key->gsk_key, dest_key->gsk_keylen, conn) ||
        gtmpqPutnchar((char *)&gxid, sizeof (GlobalTransactionId), conn))
        goto send_failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
    if (gtmpqWaitTimed(true, false, conn, finish_time) ||
        gtmpqReadData(conn) < 0)
        goto receive_failed;

    if ((res = GTMPQgetResult(conn)) == NULL)
        goto receive_failed;

    return res->gr_status;

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;
}

/*
 * Request from GTM current value of the specified sequence in the specified
 * distributed session.
 * Function returns GTM_RESULT_OK if the current value is defined, it sets
 * the *result parameter in this case.
 * Other return value means a problem. Check GTMPQerrorMessage(conn) for details
 * about the problem.
 */
int
get_current(GTM_Conn *conn, GTM_SequenceKey key,
            char *coord_name, int coord_procid, GTM_Sequence *result)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;
    int    coord_namelen = coord_name ? strlen(coord_name) : 0;

    /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(MSG_SEQUENCE_GET_CURRENT, sizeof (GTM_MessageType), conn) ||
        gtmpqPutInt(key->gsk_keylen, 4, conn) ||
        gtmpqPutnchar(key->gsk_key, key->gsk_keylen, conn) ||
        gtmpqPutInt(coord_namelen, 4, conn) ||
        (coord_namelen > 0 && gtmpqPutnchar(coord_name, coord_namelen, conn)) ||
        gtmpqPutInt(coord_procid, 4, conn))
        goto send_failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
    if (gtmpqWaitTimed(true, false, conn, finish_time) ||
        gtmpqReadData(conn) < 0)
        goto receive_failed;

    if ((res = GTMPQgetResult(conn)) == NULL)
        goto receive_failed;

    if (res->gr_status == GTM_RESULT_OK)
        *result = res->gr_resdata.grd_seq.seqval;

    return res->gr_status;

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return GTM_RESULT_COMM_ERROR;
}

/*
 * Submit to GTM new next value of the specified sequence in the specified
 * distributed session. The nextval parameter is the new value, if is called
 * is set to false the nextval will be the next value returned from the sequence
 * by nextval() function, if true the function returns incremented value.
 * Function returns GTM_RESULT_OK if it succeedes.
 * Other return value means a problem. Check GTMPQerrorMessage(conn) for details
 * about the problem.
 */
int
set_val(GTM_Conn *conn, GTM_SequenceKey key, char *coord_name,
        int coord_procid, GTM_Sequence nextval, bool iscalled)
{
    return set_val_internal(conn, key, coord_name, coord_procid, nextval,
                            iscalled, false);
}

int
bkup_set_val(GTM_Conn *conn, GTM_SequenceKey key, char *coord_name,
             int coord_procid, GTM_Sequence nextval, bool iscalled)
{
    return set_val_internal(conn, key, coord_name, coord_procid, nextval,
                            iscalled, true);
}

static int
set_val_internal(GTM_Conn *conn, GTM_SequenceKey key,
                 char *coord_name, int coord_procid, GTM_Sequence nextval,
                 bool iscalled, bool is_backup)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;
    int    coord_namelen = coord_name ? strlen(coord_name) : 0;

    /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(is_backup ? MSG_BKUP_SEQUENCE_SET_VAL : MSG_SEQUENCE_SET_VAL, sizeof (GTM_MessageType), conn) ||
        gtmpqPutInt(key->gsk_keylen, 4, conn) ||
        gtmpqPutnchar(key->gsk_key, key->gsk_keylen, conn) ||
        gtmpqPutInt(coord_namelen, 4, conn) ||
        (coord_namelen > 0 && gtmpqPutnchar(coord_name, coord_namelen, conn)) ||
        gtmpqPutInt(coord_procid, 4, conn) ||
        gtmpqPutnchar((char *)&nextval, sizeof (GTM_Sequence), conn) ||
        gtmpqPutc(iscalled, conn))
        goto send_failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    if (!is_backup)
    {
        finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
        if (gtmpqWaitTimed(true, false, conn, finish_time) ||
            gtmpqReadData(conn) < 0)
            goto receive_failed;

        if ((res = GTMPQgetResult(conn)) == NULL)
            goto receive_failed;

        return res->gr_status;
    }
    return GTM_RESULT_OK;

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return GTM_RESULT_COMM_ERROR;
}

/*
 * Rexuest from GTM next value of the specified sequence.
 * Function returns GTM_RESULT_OK if it succeedes, it sets the *result parameter
 * in this case.
 * Other return value means a problem. Check GTMPQerrorMessage(conn) for details
 * about the problem.
 */
int
get_next(GTM_Conn *conn, GTM_SequenceKey key,
     char *coord_name, int coord_procid, GTM_Sequence range,
     GTM_Sequence *result, GTM_Sequence *rangemax)
{
    return get_next_internal(conn, key, coord_name, coord_procid,
                             range, result, rangemax, false);
}

int
bkup_get_next(GTM_Conn *conn, GTM_SequenceKey key,
     char *coord_name, int coord_procid, GTM_Sequence range,
     GTM_Sequence *result, GTM_Sequence *rangemax)
{
    return get_next_internal(conn, key, coord_name, coord_procid,
                             range, result, rangemax, true);
}

static int
get_next_internal(GTM_Conn *conn, GTM_SequenceKey key,
                  char *coord_name, int coord_procid, GTM_Sequence range,
                  GTM_Sequence *result, GTM_Sequence *rangemax, bool is_backup)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;
    int    coord_namelen = coord_name ? strlen(coord_name) : 0;

    /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(is_backup ? MSG_BKUP_SEQUENCE_GET_NEXT : MSG_SEQUENCE_GET_NEXT, sizeof (GTM_MessageType), conn) ||
        gtmpqPutInt(key->gsk_keylen, 4, conn) ||
        gtmpqPutnchar(key->gsk_key, key->gsk_keylen, conn) ||
        gtmpqPutInt(coord_namelen, 4, conn) ||
        (coord_namelen > 0 && gtmpqPutnchar(coord_name, coord_namelen, conn)) ||
        gtmpqPutInt(coord_procid, 4, conn) ||
        gtmpqPutnchar((char *)&range, sizeof (GTM_Sequence), conn))
        goto send_failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    if (!is_backup)
    {
        finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
        if (gtmpqWaitTimed(true, false, conn, finish_time) ||
            gtmpqReadData(conn) < 0)
            goto receive_failed;

        if ((res = GTMPQgetResult(conn)) == NULL)
            goto receive_failed;

        if (res->gr_status == GTM_RESULT_OK)
        {
            *result = res->gr_resdata.grd_seq.seqval;
            *rangemax = res->gr_resdata.grd_seq.rangemax;
        }
        return res->gr_status;
    }
    return GTM_RESULT_OK;

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return GTM_RESULT_COMM_ERROR;
}

int
reset_sequence(GTM_Conn *conn, GTM_SequenceKey key)
{
    return reset_sequence_internal(conn, key, false);
}

int
bkup_reset_sequence(GTM_Conn *conn, GTM_SequenceKey key)
{
    return reset_sequence_internal(conn, key, true);
}

static int
reset_sequence_internal(GTM_Conn *conn, GTM_SequenceKey key, bool is_backup)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;

     /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(is_backup ? MSG_BKUP_SEQUENCE_RESET : MSG_SEQUENCE_RESET, sizeof (GTM_MessageType), conn) ||
        gtmpqPutInt(key->gsk_keylen, 4, conn) ||
        gtmpqPutnchar(key->gsk_key, key->gsk_keylen, conn))
        goto send_failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    if (!is_backup)
    {
        finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
        if (gtmpqWaitTimed(true, false, conn, finish_time) ||
            gtmpqReadData(conn) < 0)
            goto receive_failed;

        if ((res = GTMPQgetResult(conn)) == NULL)
            goto receive_failed;

        return res->gr_status;
    }
    return GTM_RESULT_OK;

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;
}

int
clean_session_sequence(GTM_Conn *conn, char *coord_name, int coord_procid)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;
    int    coord_namelen = coord_name ? strlen(coord_name) : 0;

     /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(MSG_CLEAN_SESSION_SEQ, sizeof (GTM_MessageType), conn) ||
        gtmpqPutInt(coord_namelen, 4, conn) ||
        (coord_namelen > 0 && gtmpqPutnchar(coord_name, coord_namelen, conn)) ||
        gtmpqPutInt(coord_procid, 4, conn))
        goto send_failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    
    
    finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
    if (gtmpqWaitTimed(true, false, conn, finish_time) ||
        gtmpqReadData(conn) < 0)
        goto receive_failed;

    if ((res = GTMPQgetResult(conn)) == NULL)
        goto receive_failed;

    return res->gr_status;
    

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;
}


/*
 * rc would be 0 on success, non-zero on gtm_getnameinfo_all() failure.
 */
char *
node_get_local_addr(GTM_Conn *conn, char *buf, size_t buflen, int *rc)
{
    char local_host[NI_MAXHOST];
    char local_port[NI_MAXSERV];

    *rc = 0;

    memset(local_host, 0, sizeof(local_host));
    memset(local_port, 0, sizeof(local_port));
    memset(buf, 0, buflen);

    if (conn->remote_type != GTM_NODE_GTM_PROXY)
    {
        if (gtm_getnameinfo_all(&conn->laddr.addr, conn->laddr.salen,
                    local_host, sizeof(local_host),
                    local_port, sizeof(local_port),
                    NI_NUMERICSERV))
        {
            *rc = gtm_getnameinfo_all(&conn->laddr.addr, conn->laddr.salen,
                              local_host, sizeof(local_host),
                              local_port, sizeof(local_port),
                              NI_NUMERICHOST | NI_NUMERICSERV);
        }
    }

    if (local_host[0] != '\0')
        strncpy(buf, local_host, buflen);

    return buf;
}

/*
 * Register a Node on GTM
 * Seen from a Node viewpoint, we do not know if we are directly connected to GTM
 * or go through a proxy, so register 0 as proxy number.
 * This number is modified at proxy level automatically.
 *
 * node_register() returns 0 on success, -1 on failure.
 *
 * is_backup indicates the message should be *_BKUP_* message
 */
int node_register(GTM_Conn *conn,
            GTM_PGXCNodeType type,
            GTM_PGXCNodePort port,
            char *node_name,
            char *datafolder)
{
    char host[1024];
    int rc;

    node_get_local_addr(conn, host, sizeof(host), &rc);
    if (rc != 0)
    {
        return -1;
    }

    return node_register_worker(conn, type, host, port, node_name, datafolder,
            NODE_CONNECTED, false);
}

int node_register_internal(GTM_Conn *conn,
                           GTM_PGXCNodeType type,
                           const char *host,
                           GTM_PGXCNodePort port,
                           char *node_name,
                           char *datafolder,
                           GTM_PGXCNodeStatus status)
{
    return node_register_worker(conn, type, host, port, node_name, datafolder,
            status, false);
}

int bkup_node_register_internal(GTM_Conn *conn,
                                GTM_PGXCNodeType type,
                                const char *host,
                                GTM_PGXCNodePort port,
                                char *node_name,
                                char *datafolder,
                                GTM_PGXCNodeStatus status)
{
    return node_register_worker(conn, type, host, port, node_name, datafolder,
            status, true);
}

static int node_register_worker(GTM_Conn *conn,
                                GTM_PGXCNodeType type,
                                const char *host,
                                GTM_PGXCNodePort port,
                                char *node_name,
                                char *datafolder,
                                GTM_PGXCNodeStatus status,
                                bool is_backup)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;
    char proxy_name[] = "";

    /*
     * We should be very careful about the format of the message.
     * Host name and its length is needed only when registering
     * GTM Proxy.
     * In other case, they must not be included in the message.
     * PGXCTODO: FIXME How would this work in the new scenario
     * Fix that for GTM and GTM-proxy
     */
    if (gtmpqPutMsgStart('C', true, conn) ||
        /* Message Type */
        gtmpqPutInt(is_backup? MSG_BKUP_NODE_REGISTER : MSG_NODE_REGISTER, sizeof (GTM_MessageType), conn) ||
        /* Node Type to Register */
        gtmpqPutnchar((char *)&type, sizeof(GTM_PGXCNodeType), conn) ||
        /* Node name length */
        gtmpqPutInt(strlen(node_name), sizeof (GTM_StrLen), conn) ||
        /* Node name (var-len) */
        gtmpqPutnchar(node_name, strlen(node_name), conn) ||
        /* Host name length */
        gtmpqPutInt(strlen(host), sizeof (GTM_StrLen), conn) ||
        /* Host name (var-len) */
        gtmpqPutnchar(host, strlen(host), conn) ||
        /* Port number */
        gtmpqPutnchar((char *)&port, sizeof(GTM_PGXCNodePort), conn) ||
        /* Proxy name length (zero if connected to GTM directly) */
        gtmpqPutInt(strlen(proxy_name), sizeof (GTM_StrLen), conn) ||
        /* Proxy name (var-len) */
        gtmpqPutnchar(proxy_name, strlen(proxy_name), conn) ||
        /* Proxy ID (zero if connected to GTM directly) */
        /* Data Folder length */
        gtmpqPutInt(strlen(datafolder), sizeof (GTM_StrLen), conn) ||
        /* Data Folder (var-len) */
        gtmpqPutnchar(datafolder, strlen(datafolder), conn) ||
        /* Node Status */
        gtmpqPutInt(status, sizeof(GTM_PGXCNodeStatus), conn))
    {
        goto send_failed;
    }

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
    {
        goto send_failed;
    }

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
    {
        goto send_failed;
    }

    if (!is_backup)
    {
        finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
        if (gtmpqWaitTimed(true, false, conn, finish_time) ||
            gtmpqReadData(conn) < 0)
        {
            goto receive_failed;
        }

        if ((res = GTMPQgetResult(conn)) == NULL)
        {
            goto receive_failed;
        }

        /* Check on node type and node name */
        if (res->gr_status == GTM_RESULT_OK)
        {
            Assert(res->gr_resdata.grd_node.type == type);
            Assert((strcmp(res->gr_resdata.grd_node.node_name,node_name) == 0));
            if (res->gr_type != NODE_REGISTER_RESULT)
            {
                fprintf(stderr, "res->gr_type %d not match, expected %d.\n", res->gr_type, NODE_REGISTER_RESULT);
                goto receive_failed;
            }
        }

        return res->gr_status;
    }
    return GTM_RESULT_OK;

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;
}

int node_unregister(GTM_Conn *conn, GTM_PGXCNodeType type, const char * node_name)
{
    return node_unregister_worker(conn, type, node_name, false);
}

int bkup_node_unregister(GTM_Conn *conn, GTM_PGXCNodeType type, const char * node_name)
{
    return node_unregister_worker(conn, type, node_name, true);
}

static int node_unregister_worker(GTM_Conn *conn, GTM_PGXCNodeType type, const char * node_name, bool is_backup)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;

    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(is_backup ? MSG_BKUP_NODE_UNREGISTER : MSG_NODE_UNREGISTER, sizeof (GTM_MessageType), conn) ||
        gtmpqPutnchar((char *)&type, sizeof(GTM_PGXCNodeType), conn) ||
        /* Node name length */
        gtmpqPutInt(strlen(node_name), sizeof (GTM_StrLen), conn) ||
        /* Node name (var-len) */
        gtmpqPutnchar(node_name, strlen(node_name), conn) )
        goto send_failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    if (!is_backup)
    {
        finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
        if (gtmpqWaitTimed(true, false, conn, finish_time) ||
            gtmpqReadData(conn) < 0)
            goto receive_failed;

        if ((res = GTMPQgetResult(conn)) == NULL)
            goto receive_failed;

        /* Check on node type and node name */
        if (res->gr_status == GTM_RESULT_OK)
        {
            Assert(res->gr_resdata.grd_node.type == type);
            Assert( (strcmp(res->gr_resdata.grd_node.node_name, node_name) == 0) );
        }

        return res->gr_status;
    }
    return GTM_RESULT_OK;

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;
}


void
GTM_FreeResult(GTM_Result *result, GTM_PGXCNodeType remote_type)
{
    if (result == NULL)
        return;
    gtmpqFreeResultData(result, remote_type);
    free(result);
}

int
backend_disconnect(GTM_Conn *conn, bool is_postmaster, GTM_PGXCNodeType type, char *node_name)
{// #lizard forgives
    /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(MSG_BACKEND_DISCONNECT, sizeof (GTM_MessageType), conn) ||
        gtmpqPutc(is_postmaster, conn))
        goto send_failed;

    /*
     * Then send node type and node name if backend is a postmaster to
     * disconnect the correct node.
     */
    if (is_postmaster)
    {
        if (gtmpqPutnchar((char *)&type, sizeof(GTM_PGXCNodeType), conn) ||
            /* Node name length */
            gtmpqPutInt(strlen(node_name), sizeof (GTM_StrLen), conn) ||
            /* Node name (var-len) */
            gtmpqPutnchar(node_name, strlen(node_name), conn))
            goto send_failed;
    }

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    return 1;

send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;
}

int
begin_transaction_multi(GTM_Conn *conn, int txn_count, GTM_IsolationLevel *txn_isolation_level,
            bool *txn_read_only, GTMProxy_ConnID *txn_connid,
            int *txn_count_out, GlobalTransactionId *gxid_out, GTM_Timestamp *ts_out)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;
    int i;

    /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn)) /* FIXME: no proxy header */
        goto send_failed;

    if (gtmpqPutInt(MSG_TXN_BEGIN_GETGXID_MULTI, sizeof (GTM_MessageType), conn) ||
        gtmpqPutInt(txn_count, sizeof(int), conn))
        goto send_failed;

    for (i = 0; i < txn_count; i++)
    {
		if (gtmpqPutInt(txn_isolation_level[i], sizeof(int), conn) ||
            gtmpqPutc(txn_read_only[i], conn) ||
            gtmpqPutInt(txn_connid[i], sizeof(int), conn))
        {
            goto send_failed;
        }
    }

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
    if (gtmpqWaitTimed(true, false, conn, finish_time) ||
        gtmpqReadData(conn) < 0)
        goto receive_failed;

    if ((res = GTMPQgetResult(conn)) == NULL)
        goto receive_failed;

    if (res->gr_status == GTM_RESULT_OK)
    {
        memcpy(txn_count_out, &res->gr_resdata.grd_txn_get_multi.txn_count, sizeof(int));
        memcpy(gxid_out, res->gr_resdata.grd_txn_get_multi.txn_gxid,
                sizeof(GlobalTransactionId) *
                res->gr_resdata.grd_txn_get_multi.txn_count);
        memcpy(ts_out, &res->gr_resdata.grd_txn_get_multi.timestamp, sizeof(GTM_Timestamp));
    }

    return res->gr_status;

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;
}


int
bkup_begin_transaction_multi(GTM_Conn *conn, int txn_count,
                             GlobalTransactionId *gxid, GTM_IsolationLevel *isolevel,
                             bool *read_only,
                             const char *txn_global_sessionid[], 
                             uint32 *client_id,
                             GTMProxy_ConnID *txn_connid)
{// #lizard forgives
    int ii;

    /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn)) /* FIXME: no proxy header */
        goto send_failed;

    if (gtmpqPutInt(MSG_BKUP_TXN_BEGIN_GETGXID_MULTI, sizeof (GTM_MessageType), conn) ||
        gtmpqPutInt(txn_count, sizeof(int), conn))
        goto send_failed;

    for (ii = 0; ii < txn_count; ii++)
    {
        if (gtmpqPutInt(gxid[ii], sizeof(GlobalTransactionId), conn) ||
            gtmpqPutInt(isolevel[ii], sizeof(GTM_IsolationLevel), conn) ||
            gtmpqPutc(read_only[ii], conn) ||
            gtmpqPutInt(strlen(txn_global_sessionid[ii]) + 1, sizeof(uint32), conn) ||
            gtmpqPutnchar(txn_global_sessionid[ii], strlen(txn_global_sessionid[ii]) + 1, conn) ||
            gtmpqPutInt(client_id[ii], sizeof (uint32), conn) ||
            gtmpqPutInt(txn_connid[ii], sizeof(GTMProxy_ConnID), conn))
            goto send_failed;
    }

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    return 0;

send_failed:
    return -1;
}

int
bkup_commit_transaction_multi(GTM_Conn *conn, int txn_count,
        GlobalTransactionId *gxid)
{// #lizard forgives
    int ii;

    if (gtmpqPutMsgStart('C', true, conn)) /* FIXME: no proxy header */
        goto send_failed;

    if (gtmpqPutInt(MSG_BKUP_TXN_COMMIT_MULTI, sizeof (GTM_MessageType), conn) ||
        gtmpqPutInt(txn_count, sizeof(int), conn))
        goto send_failed;

    for (ii = 0; ii < txn_count; ii++)
    {
        if (gtmpqPutnchar((char *)&gxid[ii], sizeof (GlobalTransactionId), conn))
              goto send_failed;
    }

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    return GTM_RESULT_OK;

send_failed:
    return -1;
}


int
commit_transaction_multi(GTM_Conn *conn, int txn_count, GlobalTransactionId *gxid,
             int *txn_count_out, int *status_out)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;
    int i;

    /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn)) /* FIXME: no proxy header */
        goto send_failed;

    if (gtmpqPutInt(MSG_TXN_COMMIT_MULTI, sizeof (GTM_MessageType), conn) ||
        gtmpqPutInt(txn_count, sizeof(int), conn))
        goto send_failed;

    for (i = 0; i < txn_count; i++)
    {
        if (gtmpqPutnchar((char *)&gxid[i],
                  sizeof (GlobalTransactionId), conn))
              goto send_failed;
    }

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
    if (gtmpqWaitTimed(true, false, conn, finish_time) ||
        gtmpqReadData(conn) < 0)
        goto receive_failed;

    if ((res = GTMPQgetResult(conn)) == NULL)
        goto receive_failed;

    if (res->gr_status == GTM_RESULT_OK)
    {
        memcpy(txn_count_out, &res->gr_resdata.grd_txn_get_multi.txn_count, sizeof(int));
        memcpy(status_out, &res->gr_resdata.grd_txn_rc_multi.status, sizeof(int) * (*txn_count_out));
    }

    return res->gr_status;

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;
}

int
abort_transaction_multi(GTM_Conn *conn, int txn_count, GlobalTransactionId *gxid,
                        int *txn_count_out, int *status_out)
{
    return abort_transaction_multi_internal(conn, txn_count, gxid, txn_count_out, status_out, false);
}

int
bkup_abort_transaction_multi(GTM_Conn *conn, int txn_count, GlobalTransactionId *gxid)
{
    int txn_count_out;
    int status_out[GTM_MAX_GLOBAL_TRANSACTIONS];

    return abort_transaction_multi_internal(conn, txn_count, gxid, &txn_count_out, status_out, true);
}

static int
abort_transaction_multi_internal(GTM_Conn *conn, int txn_count, GlobalTransactionId *gxid,
                                 int *txn_count_out, int *status_out, bool is_backup)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;
    int i;

    /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn)) /* FIXME: no proxy header */
        goto send_failed;

    if (gtmpqPutInt(is_backup ? MSG_BKUP_TXN_ROLLBACK_MULTI : MSG_TXN_ROLLBACK_MULTI, sizeof (GTM_MessageType), conn) ||
        gtmpqPutInt(txn_count, sizeof(int), conn))
        goto send_failed;

    for (i = 0; i < txn_count; i++)
    {
        if (gtmpqPutnchar((char *)&gxid[i],
                  sizeof (GlobalTransactionId), conn))
              goto send_failed;
    }

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    if (!is_backup)
    {
        finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
        if (gtmpqWaitTimed(true, false, conn, finish_time) ||
            gtmpqReadData(conn) < 0)
            goto receive_failed;

        if ((res = GTMPQgetResult(conn)) == NULL)
            goto receive_failed;

        if (res->gr_status == GTM_RESULT_OK)
        {
            memcpy(txn_count_out, &res->gr_resdata.grd_txn_get_multi.txn_count, sizeof(int));
            memcpy(status_out, &res->gr_resdata.grd_txn_rc_multi.status, sizeof(int) * (*txn_count_out));
        }

        return res->gr_status;
    }
    return GTM_RESULT_OK;

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;
}

int
snapshot_get_multi(GTM_Conn *conn, int txn_count, GlobalTransactionId *gxid,
           int *txn_count_out, int *status_out,
           GlobalTransactionId *xmin_out, GlobalTransactionId *xmax_out,
           GlobalTransactionId *recent_global_xmin_out, int32 *xcnt_out)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;
    int i;

    /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn)) /* FIXME: no proxy header */
        goto send_failed;

    if (gtmpqPutInt(MSG_SNAPSHOT_GET_MULTI, sizeof (GTM_MessageType), conn) ||
        gtmpqPutInt(txn_count, sizeof(int), conn))
        goto send_failed;

    for (i = 0; i < txn_count; i++)
    {
        if (gtmpqPutnchar((char *)&gxid[i],
                  sizeof (GlobalTransactionId), conn))
              goto send_failed;
    }

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
    if (gtmpqWaitTimed(true, false, conn, finish_time) ||
        gtmpqReadData(conn) < 0)
        goto receive_failed;

    if ((res = GTMPQgetResult(conn)) == NULL)
        goto receive_failed;

    if (res->gr_status == GTM_RESULT_OK)
    {
        memcpy(txn_count_out, &res->gr_resdata.grd_txn_get_multi.txn_count, sizeof(int));
        memcpy(status_out, &res->gr_resdata.grd_txn_rc_multi.status, sizeof(int) * (*txn_count_out));
        memcpy(xmin_out, &res->gr_snapshot.sn_xmin, sizeof(GlobalTransactionId));
        memcpy(xmax_out, &res->gr_snapshot.sn_xmax, sizeof(GlobalTransactionId));
        memcpy(xcnt_out, &res->gr_snapshot.sn_xcnt, sizeof(int32));
    }

    return res->gr_status;

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;
}

/*
 * Barrier
 */

int
report_barrier(GTM_Conn *conn, const char *barrier_id)
{
    return(report_barrier_internal(conn, barrier_id, false));
}

int
bkup_report_barrier(GTM_Conn *conn, char *barrier_id)
{
    return(report_barrier_internal(conn, barrier_id, true));

}

static int
report_barrier_internal(GTM_Conn *conn, const char *barrier_id, bool is_backup)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;
    int barrier_id_len = strlen(barrier_id) + 1;
    

    /* Send the message */
    if (gtmpqPutMsgStart('C', true, conn)) /* FIXME: not proxy header --> proxy shold handle this separately */
        goto send_failed;
    if (gtmpqPutInt(is_backup ? MSG_BKUP_BARRIER : MSG_BARRIER, sizeof(GTM_MessageType), conn) ||
        gtmpqPutInt(barrier_id_len, sizeof(int), conn) ||
        gtmpqPutnchar(barrier_id, barrier_id_len, conn))
        goto send_failed;
    /* Flush the message */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;
    /* Flush to ensure backend gets it */
    if (gtmpqFlush(conn))
        goto send_failed;

    /* Handle the response */
    if (!is_backup)
    {
        finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
        if (gtmpqWaitTimed(true, false, conn, finish_time) ||
            gtmpqReadData(conn) < 0)
            goto receive_failed;

        if ((res = GTMPQgetResult(conn)) == NULL)
            goto receive_failed;

        return res->gr_status;
    }
    return GTM_RESULT_OK;

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;
}

#ifdef __XLOG__
    
int
set_begin_replication(GTM_Conn *conn,const char *application_name,const char *node_name)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;
    int len;

    if (gtmpqPutMsgStart('C', true, conn))
    {
        goto send_failed;
    }

    if(gtmpqPutInt(MSG_START_REPLICATION,sizeof(GTM_MessageType),conn))
        goto send_failed;

    len = strlen(node_name) + 1;

    if (gtmpqPutInt(len, sizeof(int), conn))
        goto send_failed;

    if(gtmpqPutnchar(node_name,len ,conn))
        goto send_failed;

    len = strlen(application_name) + 1;
    if (gtmpqPutInt(len, sizeof(int), conn))
        goto send_failed;

    if(gtmpqPutnchar(application_name,len ,conn))
        goto send_failed;

    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    if (gtmpqFlush(conn))
        goto send_failed;

    finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
    if (gtmpqWaitTimed(true, false, conn, finish_time) ||
        gtmpqReadData(conn) < 0)
    {
        goto receive_failed;
    }
    if ((res = GTMPQgetResult(conn)) == NULL)
    {
        goto receive_failed;
    }

    return res->gr_type == MSG_REPLICATION_START_RESULT_SUCCESS ? 0 : 1;

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;
}

#endif

/*
 * Backup to Standby
 */

int
set_begin_backup(GTM_Conn *conn, int64 identifier, int64 lsn, GlobalTimestamp gts)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;

    if (gtmpqPutMsgStart('C', true, conn))
    {
        goto send_failed;
    }
    
    if(gtmpqPutInt(MSG_BEGIN_BACKUP ,
                   sizeof(GTM_MessageType), conn))
    {
        goto send_failed;
    }

    if(gtmpqPutnchar((char*)&identifier, sizeof(identifier), conn))
    {
        goto send_failed;
    }

    if(gtmpqPutnchar((char*)&lsn, sizeof(lsn), conn))
    {
        goto send_failed;
    }

    if(gtmpqPutnchar((char*)&gts, sizeof(gts), conn))
    {
        goto send_failed;
    }
    
    if (gtmpqPutMsgEnd(conn))
    {
        goto send_failed;
    }
    if (gtmpqFlush(conn))
    {
        goto send_failed;
    }
    finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
    if (gtmpqWaitTimed(true, false, conn, finish_time) ||
        gtmpqReadData(conn) < 0)
    {
        goto receive_failed;
    }
    if ((res = GTMPQgetResult(conn)) == NULL)
    {
        goto receive_failed;
    }
    return res->gr_resdata.backup_result ? 0 : 1;

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;
}


int
set_end_backup(GTM_Conn *conn, bool begin)
{
    GTM_Result *res = NULL;
    time_t finish_time;

    if (gtmpqPutMsgStart('C', true, conn))
    {
        goto send_failed;
    }
    if(gtmpqPutInt(MSG_END_BACKUP,
                   sizeof(GTM_MessageType), conn))
    {
        goto send_failed;
    }
    if (gtmpqPutMsgEnd(conn))
    {
        goto send_failed;
    }
    if (gtmpqFlush(conn))
    {
        goto send_failed;
    }
    finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
    if (gtmpqWaitTimed(true, false, conn, finish_time) ||
        gtmpqReadData(conn) < 0)
    {
        goto receive_failed;
    }
    if ((res = GTMPQgetResult(conn)) == NULL)
    {
        goto receive_failed;
    }
    return res->gr_status;

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;
}

/*
 * Sync with standby
 */
int
gtm_sync_standby(GTM_Conn *conn)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t finish_time;

    if (gtmpqPutMsgStart('C', true, conn))
        goto send_failed;

    if (gtmpqPutInt(MSG_SYNC_STANDBY, sizeof(GTM_MessageType), conn))
        goto send_failed;

    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    if (gtmpqFlush(conn))
        goto send_failed;

    finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;

    if (gtmpqWaitTimed(true, false, conn, finish_time) ||
        gtmpqReadData(conn) < 0)
        goto receive_failed;

    if ((res = GTMPQgetResult(conn)) == NULL)
        goto receive_failed;

    return res->gr_status;

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;
}

/*
 * Submit to GTM information about started distributed session.
 * The information is the session identifier consisting of coordinator name and
 * pid of the master process, and the BackendId of the master process.
 * The BackendId is used to track session end. BackendIds are the sequential
 * numbers from 1 to max_connections, and they are unique among active sessions
 * under the same postmaster. So if another session on the same coordinator with
 * the same BackendId is registering, that means the previous session is closed
 * and all resources assigned to it could be released.
 */
int
register_session(GTM_Conn *conn, const char *coord_name, int coord_procid,
                 int coord_backendid)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t         finish_time;
    int32        len = strlen(coord_name);

    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(MSG_REGISTER_SESSION, sizeof (GTM_MessageType), conn) ||
        gtmpqPutInt(len, sizeof(len), conn) ||
        gtmpqPutnchar(coord_name, len, conn) ||
        gtmpqPutInt(coord_procid, sizeof(coord_procid), conn) ||
        gtmpqPutInt(coord_backendid, sizeof(coord_backendid), conn))
    {
        goto send_failed;
    }

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
    {
        goto send_failed;
    }

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
    {
        goto send_failed;
    }

    finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
    if (gtmpqWaitTimed(true, false, conn, finish_time) ||
        gtmpqReadData(conn) < 0)
    {
        goto receive_failed;
    }

    if ((res = GTMPQgetResult(conn)) == NULL)
    {
        goto receive_failed;
    }

    return res->gr_status;

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;
}

int
report_global_xmin(GTM_Conn *conn, const char *node_name,
        GTM_PGXCNodeType type, GlobalTransactionId gxid,
        GlobalTransactionId *global_xmin,
        GlobalTransactionId *latest_completed_xid,
        int *errcode)
{// #lizard forgives
    GTM_Result *res = NULL;
    time_t         finish_time;

    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(MSG_REPORT_XMIN, sizeof (GTM_MessageType), conn) ||
        gtmpqPutnchar((char *)&gxid, sizeof(GlobalTransactionId), conn) ||
        gtmpqPutInt(type, sizeof (GTM_PGXCNodeType), conn) ||
        gtmpqPutInt(strlen(node_name), sizeof (GTM_StrLen), conn) ||
        gtmpqPutnchar(node_name, strlen(node_name), conn))
    {
        goto send_failed;
    }

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
    {
        goto send_failed;
    }

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
    {
        goto send_failed;
    }

    finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
    if (gtmpqWaitTimed(true, false, conn, finish_time) ||
        gtmpqReadData(conn) < 0)
    {
        goto receive_failed;
    }

    if ((res = GTMPQgetResult(conn)) == NULL)
    {
        goto receive_failed;
    }

    if (res->gr_status == GTM_RESULT_OK)
    {
        *latest_completed_xid = res->gr_resdata.grd_report_xmin.latest_completed_xid;
        *global_xmin = res->gr_resdata.grd_report_xmin.global_xmin;
        *errcode = res->gr_resdata.grd_report_xmin.errcode;
    }
    return res->gr_status;

receive_failed:
send_failed:
    conn->result = makeEmptyResultIfIsNull(conn->result);
    conn->result->gr_status = GTM_RESULT_COMM_ERROR;
    return -1;

}

