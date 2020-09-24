/*-------------------------------------------------------------------------
 *
 * gtm_stat_error.c

 *	  collect error logs of gtm
 *
 * Copyright (c) 2020-Present TBase development team, Tencent
 *
 * IDENTIFICATION
 *	  src/gtm/main/gtm_stat_error.c
 *
 *-------------------------------------------------------------------------
 */

#include <unistd.h>
#include "gtm/gtm.h"

#include "gtm/elog.h"
#include "gtm/palloc.h"
#include "gtm/gtm_lock.h"
#include "gtm/gtm_stat_error.h"
#include "gtm/gtm_msg.h"
#include "gtm/libpq.h"
#include "gtm/pqformat.h"

static int gtm_err_log_min = ERROR;
static int gtm_errmsg_size = GTM_MAX_ERRMSG_SIZE;
static int gtm_max_errlog_tuple_len = sizeof(GTM_ErrLog) + GTM_MAX_ERRMSG_SIZE;

GTM_LogCollector GlobalLogCollector;
void GTM_ErrorLogCollector(ErrorData *edata, StringInfo buff);

/*
 * Build data pump buffer.
 */
DataPumpBuf*
GTM_BuildDataPumpBuf(uint32 size)
{
    DataPumpBuf *buff = NULL;
    buff = (DataPumpBuf*)palloc0(sizeof(DataPumpBuf));
    if (NULL == buff)
    {
        return NULL;
    }

    buff->length = size * 1024;
    buff->buf = (char*)palloc0(buff->length);
    if (NULL == buff->buf)
    {
        pfree(buff);
        return NULL;
    }

    SpinLockInit(&(buff->pointer_lock));

    buff->head  		  = 0;
    buff->tail  		  = 0;
    buff->wrap_around    = 0;
    buff->border		  = INVALID_BORDER;

    return buff;
}

/*
 * Destroy data pump buffer.
 */
void
GTM_DestroyDataPumpBuf(DataPumpBuf *buff)
{
    pfree(buff->buf);
    pfree(buff);
    return;
}

/*
 * Thread-level log collector
 * call by each thread's send_message_to_server_log, can't log any error log
 */
void
GTM_ErrorLogCollector(ErrorData *edata, StringInfo buff)
{
    GTM_ThreadInfo *thrinfo = GetMyThreadInfo;
    uint32 errmsg_len = 0;
    uint32 free_space = 0;
    GTM_ErrLog err_info;
    DataPumpBuf* datapump_buff = thrinfo->datapump_buff;

    if (edata->elevel < gtm_err_log_min || 0 == buff->len)
    {
        return;
    }

    errmsg_len = Min(buff->len, gtm_errmsg_size - 1);

    err_info.proc_id = getpid();
    err_info.error_no = edata->saved_errno;
    err_info.log_time = time(NULL);
    err_info.err_level = edata->elevel;
    err_info.errmsg_len = errmsg_len;

    free_space = FreeSpace(datapump_buff);
    if (free_space < sizeof(GTM_ErrLog) + errmsg_len)
    {
        return;
    }

    PutData(datapump_buff, (char*) &err_info, sizeof(GTM_ErrLog));
    PutData(datapump_buff, buff->data, errmsg_len);
    SetBorder(datapump_buff);
}

/*
 * Init the global log collector
 */
int
GTM_InitLogCollector(void)
{
    MemoryContext oldContext;
    oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

    GlobalLogCollector.tmp_buff = palloc(gtm_max_errlog_tuple_len);
    if (NULL == GlobalLogCollector.tmp_buff)
    {
        elog(ERROR, "Failed to create tmpBuf, out of memory.");
        MemoryContextSwitchTo(oldContext);
        return -1;
    }

    GlobalLogCollector.bloom_filter = BloomCreate(GTM_BLOOM_FILTER_SIZE, 2, 0, 97);
    if (NULL == GlobalLogCollector.bloom_filter)
    {
        elog(ERROR, "Failed to create bloom filter, out of memory.");
        pfree(GlobalLogCollector.tmp_buff);
        MemoryContextSwitchTo(oldContext);
        return -1;
    }

    GlobalLogCollector.datapump_buff = GTM_BuildDataPumpBuf(GTM_GLOBAL_ERRLOG_DATAPUMP_SIZE);
    if (NULL == GlobalLogCollector.datapump_buff)
    {
        elog(ERROR, "Failed to datapump buf, out of memory.");
        BloomDestroy(GlobalLogCollector.bloom_filter);
        pfree(GlobalLogCollector.tmp_buff);
        MemoryContextSwitchTo(oldContext);
        return -1;
    }

    SpinLockInit(&GlobalLogCollector.lock);
    pg_atomic_init_u32(&GlobalLogCollector.full, 0);

    MemoryContextSwitchTo(oldContext);
    return 0;
}

/*
 * Deinit the global log collector
 */
void
GTM_DeInitLogCollector(void)
{
    if (GlobalLogCollector.tmp_buff != NULL)
    {
        pfree(GlobalLogCollector.tmp_buff);
        GlobalLogCollector.tmp_buff = NULL;
    }

    if (GlobalLogCollector.bloom_filter != NULL)
    {
        BloomDestroy(GlobalLogCollector.bloom_filter);
        GlobalLogCollector.bloom_filter = NULL;
    }

    if (GlobalLogCollector.datapump_buff != NULL)
    {
        GTM_DestroyDataPumpBuf(GlobalLogCollector.datapump_buff);
        GlobalLogCollector.datapump_buff = NULL;
    }
}

/*
 * Get a log tuple from datapump buff
 */
static int
GTM_GetLogTupleFromDataPump(DataPumpBuf* dataPumpBuf, char* buf)
{
    char* data = NULL;
    uint32 data_len = 0;
    uint32 offset = 0;
    GTM_ErrLog* err_info = NULL;
    uint32 tuple_len = 0;

    data = GetData(dataPumpBuf, &data_len);
    if (NULL == data)
    {
        /* no data */
        return -1;
    }

    if (data_len < sizeof(GTM_ErrLog))
    {
        /* copy the last part of datapumpbuff to temp buff */
        memcpy(buf, data, data_len);
        offset = data_len;

        IncDataOff(dataPumpBuf, data_len);
        data = GetData(dataPumpBuf, &data_len);
        AssertState(data != NULL);
        /* copy the rest */
        memcpy((char*)buf + offset, data, sizeof(GTM_ErrLog) - offset);
        data += (sizeof(GTM_ErrLog) - offset);

        err_info = (GTM_ErrLog*)buf;
        tuple_len = sizeof(GTM_ErrLog) + err_info->errmsg_len;

        memcpy((char*)buf + sizeof(GTM_ErrLog), data, err_info->errmsg_len);
        IncDataOff(dataPumpBuf, tuple_len - offset);
    }
    else
    {
        err_info = (GTM_ErrLog*)data;
        tuple_len = sizeof(GTM_ErrLog) + err_info->errmsg_len;
        if (data_len < tuple_len)
        {
            memcpy(buf, data, data_len);
            offset = data_len;

            IncDataOff(dataPumpBuf, data_len);
            data = GetData(dataPumpBuf, &data_len);
            AssertState(data != NULL);

            memcpy((char*)buf + offset, data, tuple_len - offset);
            IncDataOff(dataPumpBuf, tuple_len - offset);
        }
        else
        {
            memcpy((char*)buf, data, tuple_len);
            IncDataOff(dataPumpBuf, tuple_len);
        }
    }

    return 0;
}

/*
 * Collect errlog data from various threads and eliminate duplication
 */
void
GTM_ProcessLogCollection(void)
{
    GTM_ThreadInfo *thrinfo = NULL;
    DataPumpBuf* datapump_buff = NULL;
    DataPumpBuf* global_datapump_buff = GlobalLogCollector.datapump_buff;
    BLOOM *bloom_filter = GlobalLogCollector.bloom_filter;
    char *tmp_buff = GlobalLogCollector.tmp_buff;
    GTM_ErrLog* err_info = NULL;
    int errmsg_len = 0;
    uint32 i = 0;
    char *msg = NULL;

    GTM_RWLockAcquire(&GTMThreads->gt_lock, GTM_LOCKMODE_READ);

    for (i = 0; i < GTMThreads->gt_array_size; i++)
    {
        thrinfo = GTMThreads->gt_threads[i];
        if(NULL == thrinfo)
        {
            elog(DEBUG1, "thread %d exits.", i);
            continue;
        }

        datapump_buff = thrinfo->datapump_buff;
        if (NULL == datapump_buff)
        {
            continue;
        }

        if (pg_atomic_read_u32(&GlobalLogCollector.full))
        {
            break;
        }

        while (FreeSpace(global_datapump_buff) >= gtm_max_errlog_tuple_len)
        {
            if (GTM_GetLogTupleFromDataPump(datapump_buff, tmp_buff))
            {
                break;
            }

            err_info = (GTM_ErrLog*)tmp_buff;
            if (!BloomCheckAndAdd(bloom_filter, err_info->errmsg, err_info->errmsg_len))
            {
                /* replace \n with space */
                msg = err_info->errmsg;
                for (i = 0; i < err_info->errmsg_len; i++)
                {
                    if (msg[i] == '\n' || msg[i] == '\t' || msg[i] == '\r')
                    {
                        msg[i] = ' ';
                    }
                }

                /* serialize */
                errmsg_len = err_info->errmsg_len;
                err_info->proc_id = htonl(err_info->proc_id);
                err_info->error_no = htonl(err_info->error_no);
                err_info->log_time = htobe64(err_info->log_time);
                err_info->err_level = htonl(err_info->err_level);
                err_info->errmsg_len = htonl(err_info->errmsg_len);

                /* put err log into global datapumpbuff */
                PutData(global_datapump_buff, (char*) err_info, sizeof(GTM_ErrLog) + errmsg_len);
                SetBorder(global_datapump_buff);
            }
        }

        if (FreeSpace(global_datapump_buff) < gtm_max_errlog_tuple_len)
        {
            pg_atomic_write_u32(&GlobalLogCollector.full, 1);
            elog(DEBUG1, "global datapump buff is full.");
        }
    }

    GTM_RWLockRelease(&GTMThreads->gt_lock);
}

/*
 * Process MSG_GET_ERRORLOG message
 */
void
ProcessGetErrorlogCommand(Port *myport, StringInfo message)
{
    char* data = NULL;
    uint32 data_len = 0;
    uint32 total_len = 0;
    StringInfoData buf;
    DataPumpBuf* global_datapump_buff = GlobalLogCollector.datapump_buff;
    BLOOM *bloom_filter = GlobalLogCollector.bloom_filter;

    pq_getmsgend(message);

    SpinLockAcquire(&GlobalLogCollector.lock);

    pq_beginmessage(&buf, 'S');
    pq_sendint(&buf, MSG_GET_GTM_ERRORLOG_RESULT, 4);

    if (myport->remote_type == GTM_NODE_GTM_PROXY)
    {
        GTM_ProxyMsgHeader proxyhdr;
        proxyhdr.ph_conid = myport->conn_id;
        pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
    }

    data = GetData(global_datapump_buff, &data_len);
    while (NULL != data)
    {
        total_len += data_len;
        /* check max len，if the producer is faster than the consumer, it may block here */
        if (total_len >= GTM_GLOBAL_ERRLOG_DATAPUMP_SIZE)
        {
            pg_atomic_write_u32(&GlobalLogCollector.full, 1);
        }

        pq_sendbytes(&buf, data, data_len);

        IncDataOff(global_datapump_buff, data_len);
        data = GetData(global_datapump_buff, &data_len);
    }

    /* clear bitmap */
    BloomReset(bloom_filter);
    if (pg_atomic_read_u32(&GlobalLogCollector.full))
    {
        pg_atomic_write_u32(&GlobalLogCollector.full, 0);
    }

    SpinLockRelease(&GlobalLogCollector.lock);

    pq_endmessage(myport, &buf);

    if (myport->remote_type != GTM_NODE_GTM_PROXY)
    {
        /* Don't flush to the backup because this does not change the internal status */
        pq_flush(myport);
    }
}