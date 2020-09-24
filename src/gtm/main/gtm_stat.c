/*-------------------------------------------------------------------------
 *
 * gtm_stat.c
 *
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
#include "gtm/gtm_c.h"
#include "gtm/gtm.h"
#include "gtm/gtm_stat.h"
#include "gtm/gtm_msg.h"
#include "gtm/libpq.h"
#include "gtm/pqformat.h"
#include <sys/timeb.h>

extern int32  GTM_StoreGetUsedSeq(void);
extern int32  GTM_StoreGetUsedTxn(void);


GTM_Statistics GTMStatistics;

/*
 * Init global gtm statistic handle
 */
void
GTM_InitGtmStatistics(void)
{
    GTMStatistics.stat_start_time = time(NULL);;
    SpinLockInit(&GTMStatistics.lock);
}

/*
 * Init the worker statistics's handle
 */
static void
GTM_InitStatisticsInfo(GTM_WorkerStatistics *stat_handle)
{
    int i = 0;
    for (i = 0; i < CMD_STATISTICS_TYPE_COUNT; i++)
    {
        pg_atomic_init_u32(&stat_handle->cmd_statistics[i].total_request_times, 0);
        pg_atomic_init_u32(&stat_handle->cmd_statistics[i].total_costtime, 0);
        pg_atomic_init_u32(&stat_handle->cmd_statistics[i].max_costtime, 0);
        pg_atomic_init_u32(&stat_handle->cmd_statistics[i].min_costtime, PG_UINT32_MAX);
    }
}

/*
 * Reset the worker statistics's handle
 */
static void
GTM_ResetStatisticsInfo(GTM_WorkerStatistics *stat_handle)
{
    int i = 0;
    for (i = 0; i < CMD_STATISTICS_TYPE_COUNT; i++)
    {
        pg_atomic_write_u32(&stat_handle->cmd_statistics[i].total_request_times, 0);
        pg_atomic_write_u32(&stat_handle->cmd_statistics[i].total_costtime, 0);
        pg_atomic_write_u32(&stat_handle->cmd_statistics[i].max_costtime, 0);
        pg_atomic_write_u32(&stat_handle->cmd_statistics[i].min_costtime, PG_UINT32_MAX);
    }
}

/*
 * Init the statistics item
 */
static void
GTM_InitStatisticsItemArray(GTM_StatisticsItem *cmd_item)
{
    int i = 0;
    for (i = 0; i < CMD_STATISTICS_TYPE_COUNT; i++)
    {
        cmd_item[i].total_request_times = 0;
        cmd_item[i].total_costtime = 0;
        cmd_item[i].max_costtime = 0;
        cmd_item[i].min_costtime = PG_UINT32_MAX;
    }
}

/*
 * Init worker thread's statistics handle
 * only worker thread need to call
 */
void
GTM_InitStatisticsHandle(void)
{
    GTM_ThreadInfo *thrinfo = GetMyThreadInfo;
    MemoryContext oldContext;

    AssertState(thrinfo->stat_handle == NULL);

    oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

    thrinfo->stat_handle = palloc(sizeof(GTM_WorkerStatistics));
    if (thrinfo->stat_handle == NULL)
        ereport(ERROR, (ENOMEM, errmsg("Out of memory")));

    GTM_InitStatisticsInfo(thrinfo->stat_handle);

    MemoryContextSwitchTo(oldContext);
}

/*
 * Update statistics, when completing a command
 */
void
GTM_UpdateStatistics(GTM_WorkerStatistics* stat_handle, GTM_MessageType mtype, uint32 costtime)
{
    GTM_StatisticsCmd mCmd;
    GTM_StatisticsInfo* stat_info = NULL;

    if (mtype == MSG_GETGTS)
    {
        mCmd = CMD_GETGTS;
    }
    else if (mtype == MSG_SEQUENCE_GET_NEXT)
    {
        mCmd = CMD_SEQUENCE_GET_NEXT;
    }
    else if (mtype == MSG_TXN_START_PREPARED)
    {
        mCmd = CMD_TXN_START_PREPARED;
    }
    else
    {
        return;
    }

    stat_info = &stat_handle->cmd_statistics[mCmd];
    pg_atomic_fetch_add_u32(&stat_info->total_request_times, 1);
    pg_atomic_fetch_add_u32(&stat_info->total_costtime, costtime);

    if (costtime > pg_atomic_read_u32(&stat_info->max_costtime))
    {
        pg_atomic_write_u32(&stat_info->max_costtime, costtime);
    }

    if (costtime < pg_atomic_read_u32(&stat_info->min_costtime))
    {
        pg_atomic_write_u32(&stat_info->min_costtime, costtime);
    }
}

/*
 * Combine the statistics of each thread and calculate the result
 */
static void
GTM_GetMergeResult(int clear_flag, pg_time_t *stat_start_time, pg_time_t *stat_end_time, GTM_StatisticsItem *result)
{
    GTM_ThreadInfo *thrinfo = NULL;
    GTM_WorkerStatistics *stat_handle = NULL;
    uint32 max_costtime = 0;
    uint32 min_costtime = 0;
    uint32 i = 0;
    uint32 j = 0;

    GTM_InitStatisticsItemArray(result);

    SpinLockAcquire(&GTMStatistics.lock);
    GTM_RWLockAcquire(&GTMThreads->gt_lock, GTM_LOCKMODE_READ);

    /* Combine data from each thread */
    for (i = 0; i < GTMThreads->gt_array_size; i++)
    {
        thrinfo = GTMThreads->gt_threads[i];
        if(NULL == thrinfo)
        {
            elog(DEBUG1, "thread %d exits.", i);
            continue;
        }

        if(false == thrinfo->thr_epoll_ok || NULL == thrinfo->stat_handle)
        {
            continue;
        }

        stat_handle = thrinfo->stat_handle;
        for (j = 0; j < CMD_STATISTICS_TYPE_COUNT; j++)
        {
            result[j].total_request_times += pg_atomic_read_u32(&stat_handle->cmd_statistics[j].total_request_times);
            result[j].total_costtime += pg_atomic_read_u32(&stat_handle->cmd_statistics[j].total_costtime);
            max_costtime = pg_atomic_read_u32(&stat_handle->cmd_statistics[j].max_costtime);
            min_costtime = pg_atomic_read_u32(&stat_handle->cmd_statistics[j].min_costtime);
            if (result[j].max_costtime < max_costtime)
            {
                result[j].max_costtime = max_costtime;
            }

            if (result[j].min_costtime > min_costtime)
            {
                result[j].min_costtime = min_costtime;
            }
        }

        if (clear_flag)
        {
            GTM_ResetStatisticsInfo(stat_handle);
        }
    }

    *stat_start_time = GTMStatistics.stat_start_time;
    *stat_end_time = time(NULL);
    for (i = 0; i < CMD_STATISTICS_TYPE_COUNT; i++)
    {
        result[i].avg_costtime = (result[i].total_request_times == 0) ? 0 :
                                 result[i].total_costtime / result[i].total_request_times;
    }

    if (clear_flag)
    {
        GTMStatistics.stat_start_time = *stat_end_time;
    }

    GTM_RWLockRelease(&GTMThreads->gt_lock);
    SpinLockRelease(&GTMStatistics.lock);
}

/*
 * Process MSG_GET_STATISTICS message
 */
void
ProcessGetStatisticsCommand(Port *myport, StringInfo message)
{
    int32 used_seq = 0;
    int32 used_txn = 0;
    int clear_flag = 0;
    int i = 0;
    StringInfoData buf;
    pg_time_t stat_start_time = 0;
    pg_time_t stat_end_time = 0;
    GTM_StatisticsItem result_info[CMD_STATISTICS_TYPE_COUNT];

    clear_flag = pq_getmsgint(message, sizeof (int));
    pq_getmsgend(message);

    GTM_GetMergeResult(clear_flag, &stat_start_time, &stat_end_time, result_info);
    used_seq = GTM_StoreGetUsedSeq();
    used_txn = GTM_StoreGetUsedTxn();

    pq_beginmessage(&buf, 'S');
    pq_sendint(&buf, MSG_GET_GTM_STATISTICS_RESULT, 4);

    if (myport->remote_type == GTM_NODE_GTM_PROXY)
    {
        GTM_ProxyMsgHeader proxyhdr;
        proxyhdr.ph_conid = myport->conn_id;
        pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
    }

    pq_sendint64(&buf, stat_start_time);
    pq_sendint64(&buf, stat_end_time);
    pq_sendint(&buf, GTM_MAX_SEQ_NUMBER - used_seq, sizeof(int32));
    pq_sendint(&buf, MAX_PREPARED_TXN - used_txn, sizeof(int32));
    for (i = 0; i < CMD_STATISTICS_TYPE_COUNT; i++)
    {
        pq_sendint(&buf, result_info[i].total_request_times, sizeof(int32));
        pq_sendint(&buf, result_info[i].avg_costtime, sizeof(int32));
        pq_sendint(&buf, result_info[i].max_costtime, sizeof(int32));
        pq_sendint(&buf, result_info[i].min_costtime, sizeof(int32));
    }

    pq_endmessage(myport, &buf);

    if (myport->remote_type != GTM_NODE_GTM_PROXY)
    {
        /* Don't flush to the backup because this does not change the internal status */
        pq_flush(myport);
    }
}
