/*
 * Tencent is pleased to support the open source community by making TBase available.  
 * 
 * Copyright (C) 2019 THL A29 Limited, a Tencent company.  All rights reserved.
 * 
 * TBase is licensed under the BSD 3-Clause License, except for the third-party component listed below. 
 * 
 * A copy of the BSD 3-Clause License is included in this file.
 * 
 * Other dependencies and licenses:
 * 
 * Open Source Software Licensed Under the PostgreSQL License: 
 * --------------------------------------------------------------------
 * 1. Postgres-XL XL9_5_STABLE
 * Portions Copyright (c) 2015-2016, 2ndQuadrant Ltd
 * Portions Copyright (c) 2012-2015, TransLattice, Inc.
 * Portions Copyright (c) 2010-2017, Postgres-XC Development Group
 * Portions Copyright (c) 1996-2015, The PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, The Regents of the University of California
 * 
 * Terms of the PostgreSQL License: 
 * --------------------------------------------------------------------
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written agreement
 * is hereby granted, provided that the above copyright notice and this
 * paragraph and the following two paragraphs appear in all copies.
 * 
 * IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
 * DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * 
 * THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
 * ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 * 
 * 
 * Terms of the BSD 3-Clause License:
 * --------------------------------------------------------------------
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 * 
 * 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation 
 * and/or other materials provided with the distribution.
 * 
 * 3. Neither the name of THL A29 Limited nor the names of its contributors may be used to endorse or promote products derived from this software without 
 * specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, 
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS 
 * BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE 
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT 
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH 
 * DAMAGE.
 * 
 */
/*-------------------------------------------------------------------------
 *
 * gtm_xlog.c
 *        Functionalities of GTM Standby
 *
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *        src/gtm/main/gtm_xlog.c
 *
 *-------------------------------------------------------------------------
 */
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/timeb.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <string.h>
#include <stdio.h>
#include <signal.h>
#include <inttypes.h>
#include <sys/epoll.h>
#include <gtm/gtm_standby.h>
#include <gtm/gtm_xlog_internal.h>
#include <gtm/standby_utils.h>
#include <time.h>
#include <sys/time.h>
#include <gtm/gtm_c.h>

#include "gtm/gtm.h"
#include "gtm/gtm_standby.h"
#include "gtm/libpq-int.h"
#include "gtm/gtm_xlog_internal.h"
#include "gtm/gtm_xlog.h"
#include "gtm/elog.h"
#include "gtm/gtm_store.h"
#include "gtm/pqformat.h"
#include "gtm/libpq.h"
#include "utils/pg_crc.h"

#undef MIN
#define MIN(a,b) ((a) < (b) ? (a) : (b))
#define BLANK_CHARACTERS " \t\n"

extern bool enalbe_gtm_xlog_debug;

extern GTM_ThreadInfo    *g_basebackup_thread;
extern bool                 enable_sync_commit;
extern bool              first_init;
extern int               max_wal_sender;
extern int32             g_GTMStoreMapFile;
extern size_t            g_GTMStoreSize;
extern GTMControlHeader  *g_GTM_Store_Header;
extern GTM_RWLock        *g_GTM_Store_Head_Lock;
extern char              *g_GTMStoreMapAddr;

extern char  *synchronous_standby_names;

extern char   *g_checkpointMapperBuff;
extern uint32 *g_checkpointDirtyStart;
extern uint32 *g_checkpointDirtySize;

extern char            *recovery_target_timestamp;
extern GlobalTimestamp  recovery_timestamp;
extern bool             recovery_pitr_mode;

extern int  GTMStartupGTSDelta;

static bool      g_recovery_finish;
static bool     *g_GTMStoreDirtyMap;
static GTM_MutexLock g_CheckPointLock;
extern enum GTM_PromoteStatus promote_status;

XLogCtlData     *XLogCtl;
XLogSyncStandby *XLogSync;
XLogSyncConfig  *SyncConfig;

volatile bool  SyncReady;
/* Protects ControlData */
GTM_RWLock       ControlDataLock;
ControlFileData *ControlData;

/* The size of ControlData */
ssize_t          g_GTMControlDataSize;

static int XLogRecPtrCompLess(void *lhs,void *rhs);
static int XLogRecPtrCompGreater(void *lhs,void *rhs) ;

static uint64     XLogRecPtrToBytePos(XLogRecPtr ptr);
static XLogRecPtr XLogBytePosToStartRecPtr(uint64 byte);
static XLogRecPtr XLogBytePosToEndRecPtr(uint64 byte);
static uint32     XLogRecPtrToBuffIdx(XLogRecPtr ptr);
static uint64     XLogRecPtrToFileOffset(XLogRecPtr ptr);
static XLogRecPtr XLogPtrToPageEnd(XLogRecPtr ptr);
static XLogRecPtr XLogPtrToNextPageHead(XLogRecPtr ptr);

static uint64_t XLogDistance(XLogRecPtr lhs,XLogRecPtr rhs);

static void WALInsertLockAcquire(void);
static void ReleaseXLogInsertLock(void);
static void UpdateInsertInfo(XLogRecPtr ,XLogRecPtr);

static char * XLogAssemble(void);
static void   RedoXLogRecord(XLogRecord *rec,XLogRecPtr pos);
static uint32 RedoRangeOverwrite(XLogCmdRangerOverWrite *cmd);
static uint32 RedoCheckPoint(XLogCmdCheckPoint *cmd,XLogRecPtr pos) ;
static uint32 RedoTimestamp(XLogRecGts *cmd);

static int64 ReadXLogToBuff(uint64 segment_no);
static char* XLogDataAddPageHeader(XLogRecPtr start,char *data,size_t* len);
static void  XLogWrite(XLogRecPtr req);
static void  InitXLogCmdHeader(XLogCmdHeader *header,uint32 type);

static XLogRecPtr WaitXLogInsertionsToFinish(XLogRecPtr upto);
static XLogRecPtr XLogCheckPointInsert(void);

static void GenerateStatusFile(uint64 segment_no);
static void AddXLogGts(uint64 segment_no);
static void OpenXLogFile(XLogSegNo segment_no,XLogRecPtr flush);

static void ReleaseXLogRegisterBuff(void);

static XLogRecPtr GetReplicationSendRequestPtr(GTM_StandbyReplication *replication);

static int  GTM_GetXLogDataFromXLogWriteBuff(char *data,XLogRecPtr start,int max_len);

static void AppendXLogRegisterBuff(XLogRegisterBuff *buff,XLogRecData *data);

static void GTM_PrintControlData();

static void ReleaseXLogRecordWriteLocks(void);

static void NotifyWaitingQueue(void);

static void gtm_init_replication_data(GTM_StandbyReplication *replication);

static void GTM_RecoveryUpdateMetaData(XLogRecPtr redo_end_pos,XLogRecPtr preXLogRecord,uint64 segment_no,int idx);

static bool CheckSyncStandbyInList(char *application_name);

/* string process tools */
static char * strip(char *s);
static bool is_contain(const char *pattern,char ch);
static char * skip_to_next(char *s,const char *token);
static bool start_with_ignore_case(const char *s,const char *pattern);


static void clear_notify_queue(void);
static void init_standby_replication(void);
static void init_syncconfig(void);
static void init_xlogsync(void);
static void clear_syncconfig(void);
static void load_syncconfig(void);
static void load_xlogsync(void);
static void init_sync_structures(void);

static long long
getSystemTime()
{
    struct timeb t;
    ftime(&t);
    return 1000 * t.time + t.millitm;
}

char *
GetFormatedCommandLine(char *ans,int size,const char *cmd,char *file_name,char *relative_path)
{// #lizard forgives
    int  i = 0;
    int  offset = 0;
    bool meet_percent = false;

    for(i = 0; cmd[i]; i++)
    {
        if(i >= MAX_COMMAND_LEN || offset + 1 >= size)
        {
            elog(LOG,"command line %s too long",cmd);
            exit(1);
        }

        switch(cmd[i])
        {
            case 'f':
                if(meet_percent == true)
                    offset += snprintf(ans + offset ,MAX_COMMAND_LEN - offset,"%s",file_name);
                else
                    ans[offset++] = cmd[i];

                meet_percent = false;
                break;
            case 'p':
                if(meet_percent == true)
                    offset += snprintf(ans + offset ,MAX_COMMAND_LEN - offset,"%s",relative_path);
                else
                    ans[offset++] = cmd[i];

                meet_percent = false;
                break;
            case '%':
                if(meet_percent == true)
                {
                   meet_percent = false; /* use %% to transferred meaning */
                   ans[offset++] = '%';
                }
                else
                    meet_percent = true;
                break;
            default:
                if(meet_percent == true)
                {
                    elog(LOG,"unknow format %%%c",cmd[i]);
                    exit(1);
                }
                ans[offset++] = cmd[i];
                break;
        }
    }

    ans[offset] = 0;
    return ans;
}

static void
GTM_UpdateReplicationPos(GTM_StandbyReplication *replication,TimeLineID timeline,XLogRecPtr write_ptr,XLogRecPtr flush_ptr,XLogRecPtr replay_ptr)
{
    GTM_MutexLockAcquire(&replication->pos_status_lck);

    Assert(replication->flush_ptr   <= flush_ptr);
    Assert(replication->write_ptr   <= write_ptr);
    Assert(replication->replay_ptr  <= replay_ptr);

    Assert(replay_ptr != InvalidXLogRecPtr);
    Assert(write_ptr  != InvalidXLogRecPtr);
    Assert(flush_ptr  != InvalidXLogRecPtr);


    replication->flush_ptr   = flush_ptr;
    replication->write_ptr   = write_ptr;
    replication->replay_ptr  = replay_ptr;
    replication->time_line   = timeline;

    /* update send_ptr to current send pos when replication first started */
    if(replication->send_ptr == InvalidXLogRecPtr)
        replication->send_ptr = write_ptr;

    GTM_MutexLockRelease(&replication->pos_status_lck);

    CheckSyncReplication(replication,flush_ptr);
}

/* 
 * Check the waiting queue with given replication result
 */
void
CheckSyncReplication(GTM_StandbyReplication *replication,XLogRecPtr ptr)
{// #lizard forgives
    bool notify_queue_already = false;

    if (!replication->is_sync)
        return ;
    
    /* compare with local buff */
    if (replication->next_sync_pos != InvalidXLogRecPtr && ptr < replication->next_sync_pos)
        return ;

    GTM_MutexLockAcquire(&XLogSync->check_mutex);

    /* wakeup pending threads in case of invalid XLogSync->head_ptr */
    if (XLogSync->head_ptr == InvalidXLogRecPtr)
    {
        NotifyWaitingQueue();
        notify_queue_already = true;
    }

    /* save XLogSync->head_ptr to local buff to avoid lock contention */
    if (replication->next_sync_pos != XLogSync->head_ptr)
    {
        replication->next_sync_pos = XLogSync->head_ptr;
        replication->sync_hint     = false;
    }

    /* increase the counter and remember that we have added for the current XLogSync->head */
    if (ptr >= replication->next_sync_pos && !replication->sync_hint)
    {
        XLogSync->head_xlog_hints++;
        replication->sync_hint = true;
    }

    /* wakeup pending threads */
    if (!notify_queue_already && XLogSync->head_xlog_hints >= SyncConfig->required_sync_num)
        NotifyWaitingQueue();

    GTM_MutexLockRelease(&XLogSync->check_mutex);
}

static int
XLogRecPtrCompLess(void *lhs,void *rhs)
{

   return *((XLogRecPtr *) lhs) < *((XLogRecPtr *) rhs);
}

/*
 * XLogPtr compare function used in heap.
 */
static int
XLogRecPtrCompGreater(void *lhs,void *rhs)
{
    return *((XLogRecPtr *) lhs) > *((XLogRecPtr *) rhs);
}

/*
 * Get max sync xlog position with the given number of sync standbys
 * you have to lock check_mutex lock beforce you call it.
 */
static XLogRecPtr
GetMaxSyncStandbyCompletePtr()
{
    /* static variables , avoid frequent memory allocation */
    static  XLogRecPtr  ptrs[GTM_MAX_WALSENDER];
    static  XLogRecPtr  *temp_key   = NULL;
    static  XLogRecPtr  *temp_value = NULL;

    gtm_ListCell   *cell                = NULL;
    GTM_StandbyReplication *replication = NULL;
    int i = 0;
    int heap_count = 0;
    heap h;

    /*
     * We used heap the calculate the n-th greatest sync xlog position in replication
     * and n is SyncConfig->required_sync_num which ensure xlog is flush to n standbys.
     */
    heap_create(&h,0,XLogRecPtrCompGreater);

    gtm_foreach(cell,XLogSync->sync_standbys)
    {
        replication = (GTM_StandbyReplication *)gtm_lfirst(cell);

        ptrs[i] = GetReplicationFlushPtr(replication);

        heap_insert(&h,ptrs + i,NULL);

        if(enalbe_gtm_xlog_debug)
            elog(LOG,"GetMaxSyncStandbyCompletePtr standby: %s heap insert %X/%X"
                    ,replication->application_name,(uint32)(ptrs[i]>>32),(uint32)(ptrs[i]));

        if (heap_count >= SyncConfig->required_sync_num)
        {
            heap_delmin(&h,(void **)&temp_key,(void **)&temp_value);
        }
        else
        {
            heap_count++;
        }
    }

    /* if there is not engouh stanndbys,return InvalidXLogRecPtr. */
    if (heap_count < SyncConfig->required_sync_num)
    {
        heap_destroy(&h);
        if(enalbe_gtm_xlog_debug)
            elog(LOG,"GetMaxSyncStandbyCompletePtr no enough standby count:%d required:%d",heap_count,SyncConfig->required_sync_num);
        return InvalidXLogRecPtr;
    }

    /* pop until the last one */
    while(heap_delmin(&h,(void **)&temp_key,(void **)&temp_value))
    { ; }

    heap_destroy(&h);

    if(enalbe_gtm_xlog_debug)
    {
		if(temp_key)
	        	elog(LOG,"GetMaxSyncStandbyCompletePtr result %X/%X",(uint32)((*temp_key)>>32),(uint32)(*temp_key));
		else
	        	elog(LOG,"GetMaxSyncStandbyCompletePtr result 0");
			
    }

    return temp_key ? *temp_key : InvalidXLogRecPtr;
}

/*
 * Notify waiting threads
 */
static void
NotifyWaitingQueue(void)
{// #lizard forgives
    XLogRecPtr check_ptr = InvalidXLogRecPtr;
    XLogRecPtr *key      = NULL;
    XLogWaiter *waiter   = NULL;
    bool      notify_one = false;
    int       ret = 0;

    

    /* get max sync xlog position */
    check_ptr = GetMaxSyncStandbyCompletePtr();
    if (check_ptr == InvalidXLogRecPtr)
    {
        if(enalbe_gtm_xlog_debug)
            elog(LOG,"NotifyWaitingQueue check_ptr is null,skip");
        return ;
    }

    GTM_MutexLockAcquire(&XLogSync->wait_queue_mutex);
	XLogSync->synced_lsn = XLogSync->synced_lsn > check_ptr ? XLogSync->synced_lsn : check_ptr;

    while(ret = heap_min(&XLogSync->wait_queue,(void **)&key,(void **)&waiter),ret)
    {
        if(*key > check_ptr)
            break;

        if(enalbe_gtm_xlog_debug)
            elog(LOG,"NotifyWaitingQueue notify %X/%X ",(uint32)((*key)>>32),(uint32)(*key));

        heap_delmin(&XLogSync->wait_queue,(void **)&key,(void **)&waiter);

        notify_one = true;

        GTM_MutexLockAcquire(&waiter->lock);
        waiter->finished = true;
        GTM_CVSignal(&waiter->cv);
        GTM_MutexLockRelease(&waiter->lock);
    }

    if (notify_one)
    {
        if (ret == 0)
            XLogSync->head_ptr = InvalidXLogRecPtr;
        else
            XLogSync->head_ptr = *key;
        XLogSync->head_xlog_hints = 0;
    }

    GTM_MutexLockRelease(&XLogSync->wait_queue_mutex);
}

static XLogRecPtr
GetReplicationSendRequestPtr(GTM_StandbyReplication *replication)
{
    XLogRecPtr  ptr;
    GTM_MutexLockAcquire(&replication->send_request_lck);
    ptr = replication->send_request;
    GTM_MutexLockRelease(&replication->send_request_lck);

    return ptr;
}

XLogRecPtr
GetReplicationFlushPtr(GTM_StandbyReplication *replication)
{
    XLogRecPtr ptr;

    GTM_MutexLockAcquire(&replication->pos_status_lck);
    ptr = replication->flush_ptr;
    GTM_MutexLockRelease(&replication->pos_status_lck);
    return ptr;
}

void
NotifyReplication(XLogRecPtr ptr)
{
    int i;

    for( i = 0; i < max_wal_sender ; i++)
    {
        if(g_StandbyReplication[i].is_use == false)
            continue;

        GTM_MutexLockAcquire(&g_StandbyReplication[i].send_request_lck);
        if(g_StandbyReplication[i].send_ptr < ptr)
        {
            g_StandbyReplication[i].send_request = ptr;
            GTM_CVSignal(&g_StandbyReplication[i].xlog_to_send);
        }
        GTM_MutexLockRelease(&g_StandbyReplication[i].send_request_lck);
    }
}

/*
 * Wait until required xlog position is flushed to standbys.
 */void
WaitSyncComplete(XLogRecPtr ptr)
{
    XLogWaiter *waiter = NULL;

    if(!enable_sync_commit)
       return ;

    if(enalbe_gtm_xlog_debug)
        elog(LOG,"WaitSyncComplete %X/%X wait",(uint32)(ptr>>32),(uint32)ptr);

    waiter = &GetMyThreadInfo->xlog_waiter;
    waiter->pos      = ptr;
    waiter->finished = false;

    GTM_MutexLockAcquire(&XLogSync->wait_queue_mutex);
	/* avoid reload changes which will cause hug up*/
    if(XLogSync->synced_lsn >= ptr)
	{
	    if(enalbe_gtm_xlog_debug)
		    elog(LOG,"WaitSyncComplete %X/%X early finished",(uint32)(ptr>>32),(uint32)ptr);
		GTM_MutexLockRelease(&XLogSync->wait_queue_mutex);
        return ;
	}
    heap_insert(&XLogSync->wait_queue,&waiter->pos,waiter);
    GTM_MutexLockRelease(&XLogSync->wait_queue_mutex);

    /* wait could already finished,if we don't check now, we might wait forever. */
    GTM_MutexLockAcquire(&waiter->lock);
    if (waiter->finished) {
        GTM_MutexLockRelease(&waiter->lock);
        return ;
    }

    /* make sure finished flag is set to true.  */
    while (true)
    {
        GTM_CVWait(&waiter->cv,&waiter->lock);
        if (waiter->finished) {
            GTM_MutexLockRelease(&waiter->lock);
            if(enalbe_gtm_xlog_debug)
                elog(LOG,"WaitSyncComplete %X/%X finished",(uint32)(ptr>>32),(uint32)ptr);
            return ;
        }
    }
}

XLogRecPtr
GetStandbyWriteBuffPos(void)
{
    XLogRecPtr result;

    GTM_RWLockAcquire(&XLogCtl->standby_info_lck,GTM_LOCKMODE_READ);
    result = XLogCtl->write_buff_pos;
    GTM_RWLockRelease(&XLogCtl->standby_info_lck);

    return result;
}

XLogRecPtr
GetStandbyApplyPos(void)
{
    XLogRecPtr result;

    GTM_RWLockAcquire(&XLogCtl->standby_info_lck,GTM_LOCKMODE_READ);
    result = XLogCtl->apply;
    GTM_RWLockRelease(&XLogCtl->standby_info_lck);

    return result;
}

void
UpdateStandbyWriteBuffPos(XLogRecPtr pos)
{
    if(enalbe_gtm_xlog_debug)
        elog(LOG,"update write buff pos to %X/%X",
             (uint32_t)(pos >>32),
             (uint32_t)pos);
    GTM_RWLockAcquire(&XLogCtl->standby_info_lck,GTM_LOCKMODE_WRITE);
    XLogCtl->write_buff_pos = pos;
    GTM_RWLockRelease(&XLogCtl->standby_info_lck);
}

void
UpdateStandbyApplyPos(XLogRecPtr pos)
{
    if(enalbe_gtm_xlog_debug)
        elog(LOG,"update apply pos to %X/%X",
             (uint32_t)(pos >>32),
             (uint32_t)pos);
    GTM_RWLockAcquire(&XLogCtl->standby_info_lck,GTM_LOCKMODE_WRITE);
    XLogCtl->apply = pos;
    GTM_RWLockRelease(&XLogCtl->standby_info_lck);
}

XLogRecPtr
GetXLogFlushRecPtr()
{
    XLogRecPtr result;

    SpinLockAcquire(&XLogCtl->walwirte_info_lck);
    result = XLogCtl->LogwrtResult.Flush;
    SpinLockRelease(&XLogCtl->walwirte_info_lck);

    return result;
}

void
ProcessStartReplicationCommand(Port *myport, StringInfo message)
{
    StringInfoData    buf;
    GTM_ThreadInfo  *thr = GetMyThreadInfo;
    int             namelen;
    const char      *node_name;
    const char      *replication_name;

    namelen          = pq_getmsgint(message,sizeof(int));
    node_name        = pq_getmsgbytes(message,namelen);
    namelen          = pq_getmsgint(message,sizeof(int));
    replication_name = pq_getmsgbytes(message,namelen);
    pq_getmsgend(message);

    /* disconnect from current thread */
    if(epoll_ctl(thr->thr_efd,EPOLL_CTL_DEL,myport->sock,NULL) != 0)
    {
        elog(LOG,"epoll delete fails %s",strerror(errno));
        goto fail_process;
    }

    /* transfer to walsender thread */
    if(gtm_standby_resign_to_walsender(myport,node_name,replication_name) == false)
    {
        elog(LOG,"not enough walsender thread");
        goto fail_process;
    }

    elog(LOG,"replication %s connected",replication_name);

    pq_beginmessage(&buf, 'S');
    pq_sendint(&buf, MSG_REPLICATION_START_RESULT_SUCCESS, 4);
    pq_endmessage(myport, &buf);
    pq_flush(myport);
    return ;

fail_process:
    pq_beginmessage(&buf, 'S');
    pq_sendint(&buf, MSG_REPLICATION_START_RESULT_FAIL, 4);
    pq_endmessage(myport, &buf);
    pq_flush(myport);
}

bool
GTM_SendGetReplicationStatusRequest(GTM_Conn *conn)
{
    /* Start the message. */
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(MSG_GET_REPLICATION_STATUS, sizeof (GTM_MessageType), conn))
        goto send_failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(conn))
        goto send_failed;

    if(enalbe_gtm_xlog_debug)
    {
        elog(LOG,"send requst replication status command to standby");
    }

    send_failed:
    elog(LOG,"Error couldn't send request replication status command to standby");
    return false;
}

static int
GTM_GetXLogDataFromXLogWriteBuff(char *data,XLogRecPtr start,int len)
{
    /* lock the segment in case of segment switch */
    GTM_RWLockAcquire(&XLogCtl->segment_lck,GTM_LOCKMODE_READ);

    /* check whether current segment is what we want */
    if(XLogInCurrentSegment(start) == false)
    {
        GTM_RWLockRelease(&XLogCtl->segment_lck);
        return Send_Data_Not_Found;
    }

    memcpy(data,XLogCtl->writerBuff + XLogRecPtrToBuffIdx(start), len);
    GTM_RWLockRelease(&XLogCtl->segment_lck);

    return Send_OK;
}

int
GTM_GetReplicationResultIfAny(GTM_StandbyReplication *replication,Port *port)
{
    struct epoll_event events[1];
    int efd = GetMyThreadInfo->thr_efd;
    int n;
    StringInfoData input_message;
    int qtype;

    if(pq_hasdataleft(port) == false)
    {
        /* immediate return if not data */
        n = epoll_wait(efd,events,1,0);

        if(n == 0)
            return 0;

        if(!(events->events & EPOLLIN))
            return EOF;
    }

    initStringInfo(&input_message);

    /*
     * Get message type code from the frontend.
     */
    qtype = pq_getbyte(port);

    if (qtype == EOF)            /* frontend disconnected */
    {
        ereport(DEBUG1,
                (EPROTO,
                 errmsg("unexpected EOF on client connection")));
        return EOF;
    }

    if (pq_getmessage(port, &input_message, 0))
        return EOF;

    Assert(qtype == 'C');

    {
        XLogRecPtr  write_ptr;
        XLogRecPtr  flush_ptr;
        XLogRecPtr  replay_ptr;
        TimeLineID  timeline;
        int value;

        value   = pq_getmsgint(&input_message, sizeof(int));
        write_ptr  = (XLogRecPtr) pq_getmsgint64(&input_message);
        flush_ptr  = (XLogRecPtr) pq_getmsgint64(&input_message);
        replay_ptr = (XLogRecPtr) pq_getmsgint64(&input_message);
        timeline   = pq_getmsgint(&input_message, sizeof(TimeLineID));

        if(enalbe_gtm_xlog_debug)
        {
            elog(LOG, "Get replication result : write %X/%X,flush %X/%X, replay_ptr %X/%X ,timeline %d value:%d",
                 (uint32) (write_ptr >> 32),
                 (uint32) write_ptr,
                 (uint32) (flush_ptr >> 32),
                 (uint32) flush_ptr,
                 (uint32) (replay_ptr >> 32),
                 (uint32) replay_ptr,
                 timeline,
                 value
            );
        }

        pq_getmsgend(&input_message);

        GTM_UpdateReplicationPos(replication,timeline,write_ptr,flush_ptr,replay_ptr);
    }
    return 1;
}

static int
ReadXLogFileToBuffIntern(GTM_XLogSegmentBuff *buff,TimeLineID timeline,XLogSegNo segment_no)
{
    char     path[MAXFNAMELEN];
    int      fd;
    ssize_t  bytes;

    GTMXLogFileName(path,timeline,segment_no);

    fd = open(path,O_RDONLY);
    if(fd == -1)
    {
        elog(LOG,"Fail to open xlog %s : %s",path,strerror(errno));
        return Send_Error;
    }

    buff->total_length = 0;

    for(;;)
    {
        bytes = read(fd,buff->buff + buff->total_length,GTM_XLOG_SEG_SIZE - buff->total_length);

        if(bytes < 0)
        {
            elog(LOG,"Read xlog file %s fails : %s",path,strerror(errno));
            close(fd);
            return Send_Error;
        }

        if(bytes == 0)
            break;

        buff->total_length += bytes;
    }

    buff->segment_no = segment_no;
    close(fd);

    if(enalbe_gtm_xlog_debug)
        elog(LOG,"read xlog file %s with bytes %d",path,buff->total_length);

    return Send_OK;
}

static int
ReadXLogFileToBuff(GTM_XLogSegmentBuff *buff,TimeLineID timeline,XLogSegNo segment_no)
{
    char path[MAXFNAMELEN];

    Assert(GetCurrentSegmentNo() > segment_no);

    GTMXLogFileName(path,timeline,segment_no);

    if(enalbe_gtm_xlog_debug)
        elog(LOG,"read xlog file %s for replication",path);

    if(access(path,F_OK) >= 0)
        return ReadXLogFileToBuffIntern(buff,timeline,segment_no);

    /* forward time line */
    timeline++;
    GTMXLogFileName(path,timeline,segment_no);
    if(access(path,F_OK) < 0)
    {
        elog(LOG,"xlog file %s not found ,that is not support to happen.",path);
        /* need to tell the standby that the required xlog is not available */
        return Send_XlogFile_Not_Found;
    }

    if(enalbe_gtm_xlog_debug)
        elog(LOG,"read xlog file %s for replication",path);

    return ReadXLogFileToBuffIntern(buff,timeline,segment_no);
}

static void
SendXLogFileData(StringInfo out_buff,XLogRecPtr start_pos,char *data,int len)
{
#define XLOG_MAX_PER_SEND 4096

    int send_bytes;
    int loop = 0;
    int i ;

    XLogRecPtr  end_pos = start_pos + len;

    if(enalbe_gtm_xlog_debug)
        elog(LOG,"send xlog from %X/%X to %X/%X %d",
             (uint32)(start_pos>>32),
             (uint32)start_pos,
             (uint32)(end_pos>>32),
             (uint32)end_pos,
             len);

    //dump_data(start_pos,data,len);

    pq_sendint64(out_buff,start_pos);

    pq_sendint(out_buff,len,sizeof(int));

    loop = (len - 1)/ XLOG_MAX_PER_SEND + 1;

    pq_sendint(out_buff,loop,sizeof(int));

    for(i = 0; i < loop ;i++)
    {
        send_bytes = len;
        if(send_bytes > XLOG_MAX_PER_SEND)
            send_bytes = XLOG_MAX_PER_SEND;

        pq_sendint(out_buff,send_bytes,sizeof(int));
        pq_sendbytes(out_buff,data,send_bytes);

        len  -= send_bytes;
        data += send_bytes;
    }

    Assert(len == 0);
}


static int
SendXLogDataFromFileBuffInternal(GTM_StandbyReplication *replication,StringInfo out_buff)
{
    int         send_size;
    int         offset ;

    XLogRecPtr            pos = replication->send_ptr;
    GTM_XLogSegmentBuff *buff = &replication->xlog_read_buff;

    offset  = XLogRecPtrToBuffIdx(pos);

    Assert(buff->total_length >= offset);
    send_size = buff->total_length - offset;

    if(enalbe_gtm_xlog_debug)
        elog(LOG,"send pos %X/%X size seg:%ld file_length: %d offset :%d ",
             (uint32_t)(pos >> 32),
             (uint32_t)(pos),buff->segment_no,buff->total_length,offset);

    Assert(send_size >= 0);

    SendXLogFileData(out_buff,pos,buff->buff + offset,send_size);

    return Send_OK;
}

static int
GetXLogFileSize(TimeLineID timeline,XLogSegNo segment_no)
{
    char path[MAXFNAMELEN];
    struct stat statbuf;

    GTMXLogFileName(path,timeline,segment_no);

    if(stat(path,&statbuf) == 0)
        return statbuf.st_size;

    /* forward time line */
    timeline++;
    GTMXLogFileName(path,timeline,segment_no);
    if(stat(path,&statbuf) == 0)
        return statbuf.st_size;

    Assert(false);
    return -1;
}

static int
SendXLogDataFromFileBuff(GTM_StandbyReplication *replication,StringInfo message_buff)
{
    int ret = Send_OK;
    GTM_XLogSegmentBuff *local_buff      = &replication->xlog_read_buff;
    XLogSegNo            request_segment = GetSegmentNo(replication->send_ptr);

    /* if we have reach the end */
    if(GetXLogFileSize(replication->time_line,request_segment) == XLogRecPtrToBuffIdx(replication->send_ptr))
    {
        request_segment++;
        replication->send_ptr = request_segment * GTM_XLOG_SEG_SIZE;

        return Send_Data_Not_Found;
    }

    ret = ReadXLogFileToBuff(local_buff,replication->time_line,request_segment);
    if(ret != Send_OK)
        return ret;

    SendXLogDataFromFileBuffInternal(replication,message_buff);

    request_segment++;
    replication->send_ptr = request_segment  * GTM_XLOG_SEG_SIZE;

    return Send_OK;
}

static uint64_t
XLogLeftDistanceToSegEnd(XLogRecPtr pos)
{
    return GTM_XLOG_SEG_SIZE - (pos % GTM_XLOG_SEG_SIZE);
}

static int
SendXLogDataFromXLogBuff(GTM_StandbyReplication *replication,StringInfo out_message)
{
    uint64_t      len;
    int           ret;

    len  = XLogDistance(GetReplicationSendRequestPtr(replication),replication->send_ptr);

    if(len > XLogLeftDistanceToSegEnd(replication->send_ptr))
        len = XLogLeftDistanceToSegEnd(replication->send_ptr);

    ret  = GTM_GetXLogDataFromXLogWriteBuff(replication->xlog_read_buff.buff,replication->send_ptr,len);

    if(ret == Send_Data_Not_Found)
        return ret;

    SendXLogFileData(out_message,replication->send_ptr,replication->xlog_read_buff.buff,len);

    replication->send_ptr += len;

    return Send_OK;
}

static int
SendXLogData(GTM_StandbyReplication *replication,StringInfo buf)
{
    int  ret;

    for(;;)
    {
        if(XLogInCurrentSegment(replication->send_ptr))
        {
            ret = SendXLogDataFromXLogBuff(replication,buf);
            if(ret != Send_Data_Not_Found)
                return ret;

            if(enalbe_gtm_xlog_debug)
                elog(LOG,"request data %X/%X not found in xlog buff",
                     (uint32)(replication->send_ptr>>32),
                     (uint32)replication->send_ptr);
        }

        ret = SendXLogDataFromFileBuff(replication,buf);
        if(ret == Send_Data_Not_Found)
            continue;
        return ret;
    }
}

bool
SendXLogContext(GTM_StandbyReplication *replication,Port *port)
{
    int bytes;
    StringInfoData out_message;

    pq_beginmessage(&out_message, 'S');
    pq_sendint(&out_message, MSG_REPLICATION_CONTENT, 4);
    /* send the processing result status to the standby */
    pq_sendint(&out_message, Send_OK, sizeof(int));
    pq_sendint64(&out_message, GetReplicationSendRequestPtr(replication));

    /* request send reply */
    pq_sendint(&out_message, 1, sizeof(int));

    bytes = SendXLogData(replication,&out_message);

    if (bytes == Send_XlogFile_Not_Found)
    {
        pfree(out_message.data);
        out_message.data = NULL;
        pq_beginmessage(&out_message, 'S');
        pq_sendint(&out_message, MSG_REPLICATION_CONTENT, 4);
        /* send the processing result status to the standby */
        pq_sendint(&out_message, Send_XlogFile_Not_Found, sizeof(int));
    }
    else if(bytes == Send_Error)
        goto send_fail;

    pq_endmessage(port,&out_message);

    if(pq_flush(port))
        goto send_fail;

    if (bytes == Send_XlogFile_Not_Found)
    {
        return false;
    }

    return true;

send_fail:
    elog(LOG,"send fails");
    return false;
}

/*
 * Check whehter there is any xlog to send
 */
bool
GTM_HasXLogToSend(GTM_StandbyReplication *replication)
{
    if(replication->send_ptr == InvalidXLogRecPtr)
        return false;

    GTM_MutexLockAcquire(&replication->send_request_lck);

    while(replication->send_request <= replication->send_ptr)
    {
        if(GTM_CVTimeWait(&replication->xlog_to_send,&replication->send_request_lck,1) == ETIMEDOUT)
        {
           /* acquire the lastest flush ptr in case of nobody updates it. */
           replication->send_request =  GetXLogFlushRecPtr();
           GTM_MutexLockRelease(&replication->send_request_lck) ;
           return false;
        }
    }

    GTM_MutexLockRelease(&replication->send_request_lck) ;
    return true;
}

XLogSegNo
GetSegmentNo(XLogRecPtr ptr)
{
    return (ptr / GTM_XLOG_SEG_SIZE);
}

/* Init xlog file and relative data structure */
void
GTM_XLogFileInit(char *data_dir)
{
    XLogRecPtr flush;
    uint64     segment_no;
    int        fd;

    flush      = XLogCtl->LogwrtResult.Flush;
    segment_no = flush / GTM_XLOG_SEG_SIZE;

    g_GTMStoreDirtyMap = (bool *)palloc0(g_GTMStoreSize * sizeof(bool));
    memset(g_GTMStoreDirtyMap,0,g_GTMStoreSize * sizeof(bool));

    if(Recovery_IsStandby())
        return ;

    if(first_init)
    {
        NewXLogFile(segment_no);

        XLogCtl->LogwrtResult.Flush = InvalidXLogRecPtr;

        XLogFlush(flush);

        return ;
    }

    fd = XLogCtl->xlog_fd;
    if(fd != 0)
        close(fd);

    if(flush % GTM_XLOG_SEG_SIZE == 0)
    {
        segment_no++;
        NewXLogFile(segment_no);
    }
    else
        OpenXLogFile(segment_no,flush);
    XLogCtl->currentSegment = segment_no;
}

/* Init Xlog Ctl structure during startup */
void
GTM_XLogCtlDataInit(void)
{
    XLogRecPtr    flush;
    XLogCtlInsert *Insert;
    int i;

    XLogCtl = (XLogCtlData *)palloc(sizeof(XLogCtlData));
    if(XLogCtl == NULL)
    {
        elog(LOG, "memory insufficicent.");
        exit(1);
    }

    Assert(ControlData);

    flush = XLogBytePosToEndRecPtr(ControlData->CurrBytePos);

    memset(XLogCtl->writerBuff,0,sizeof(XLogCtl->writerBuff));

    XLogCtl->xlog_fd        = 0;
    XLogCtl->thisTimeLineID = ControlData->thisTimeLineID;
    XLogCtl->currentSegment = flush / GTM_XLOG_SEG_SIZE;

    XLogCtl->LogwrtResult.Write = flush;
    XLogCtl->LogwrtResult.Flush = flush;

    GTM_RWLockInit(&XLogCtl->segment_lck);
    GTM_MutexLockInit(&XLogCtl->walwrite_lck);
    SpinLockInit(&XLogCtl->walwirte_info_lck);
    SpinLockInit(&XLogCtl->timeline_lck);

    SpinLockInit(&XLogCtl->segment_gts_lck);
    XLogCtl->segment_max_gts       = 0;
    XLogCtl->segment_max_timestamp = 0;

    for(i = 0 ; i < NUM_XLOGINSERT_LOCKS; i++)
    {
        GTM_MutexLockInit(&XLogCtl->insert_lck[i].l);
        SpinLockInit(&XLogCtl->insert_lck[i].m);

        XLogCtl->insert_lck[i].start = InvalidXLogRecPtr;
        XLogCtl->insert_lck[i].end   = InvalidXLogRecPtr;
    }

    /* Insert section */
    Insert = &XLogCtl->Insert;

    SpinLockInit(&Insert->insertpos_lck);
    Insert->CurrBytePos = ControlData->CurrBytePos;
    Insert->PrevBytePos = ControlData->PrevBytePos;

    g_checkpointMapperBuff = (char *)palloc(g_GTMStoreSize);
    g_checkpointDirtySize  = (uint32 *)palloc(sizeof(uint32) * g_GTMStoreSize);
    g_checkpointDirtyStart = (uint32 *)palloc(sizeof(uint32) * g_GTMStoreSize);

    if(enalbe_gtm_xlog_debug || enable_gtm_debug)
    {
        elog(LOG,"Read ControlData CurrBytePos to %"PRIu64" PrevBytePos to %"PRIu64"",Insert->CurrBytePos,Insert->PrevBytePos);
        elog(LOG,"Read ControlData EndOfXLog to %X/%X PreEndOfXLog to %X/%X",
             (uint32)(XLogBytePosToEndRecPtr(Insert->CurrBytePos) >> 32),
             (uint32)XLogBytePosToEndRecPtr(Insert->CurrBytePos),
             (uint32)(XLogBytePosToEndRecPtr(Insert->PrevBytePos) >> 32),
             (uint32)XLogBytePosToEndRecPtr(Insert->PrevBytePos)
        );
    }

    GTM_MutexLockInit(&g_CheckPointLock);
}

/* Use to commit modification for mapper file */
void
BeforeReplyToClientXLogTrigger(void)
{
    GTM_ThreadInfo *thr = GetMyThreadInfo;
    XLogRecPtr  endPos;
    long long start_time;
    long long end_time;

    ReleaseXLogRecordWriteLocks();

    if(Recovery_IsStandby())
        return ;

    if(thr->register_buff == NULL)
        return;

    if(thr->register_buff->rdata_len == 0)
    {
        ReleaseXLogRegisterBuff();
        return ;
    }

    start_time = getSystemTime();

    /* release thread lock ,so that we don't block GTM_StoreLock in xlog flush waiting. */
    if(thr->handle_standby)
        GTM_RWLockRelease(&thr->thr_lock);

    endPos = XLogInsert();

    XLogFlush(endPos);
    WaitSyncComplete(endPos);

    if(thr->handle_standby)
        GTM_RWLockAcquire(&thr->thr_lock, GTM_LOCKMODE_WRITE);

    end_time = getSystemTime();

    if(end_time - start_time > warnning_time_cost)
        elog(LOG, "BeforeReplyToClientXLogTrigger lsn %X/%X cost %lld ms", (uint32)(endPos >> 32), (uint32)endPos, end_time - start_time);
}

/* Read xlog file to buff */
static int64
ReadXLogToBuff(uint64 segment_no)
{
    ssize_t nbytes;
    char    xlog_path[MAXFNAMELEN];
    int     fd;
    struct stat statbuf;

    GTMXLogFileName(xlog_path,XLogCtl->thisTimeLineID,segment_no);

    fd = open(xlog_path,O_RDWR, S_IRUSR | S_IWUSR);

    if(fd < 0)
    {
        elog(LOG,"ReadXLogToBuff: Fail to open xlog %s, err_msg:%s", xlog_path, strerror(errno));
        return -1;
    }

    if (fstat(fd, &statbuf) < 0)
    {
        close(fd);
        elog(LOG, "ReadXLog stat file:%s failed for:%s.", xlog_path, strerror(errno));
        return -1;
    }

    if(statbuf.st_size > GTM_XLOG_SEG_SIZE)
    {
        close(fd);
        elog(LOG, "ReadXLog file %s size larger than %d",xlog_path,GTM_XLOG_SEG_SIZE);
        return -1;
    }

    nbytes = read(fd, XLogCtl->writerBuff, statbuf.st_size);

    if(nbytes != statbuf.st_size)
    {
        close(fd);
        elog(LOG, "ReadXLog read file %s failed for:%s.", xlog_path, strerror(errno));
        return -1;
    }

    close(fd);

    return nbytes;
}

/* truncate xlog file to remove dirty data during crash */
static bool
TruncateXLogFile(uint64 segment_no,offset_t size)
{
    char    xlog_path[MAXFNAMELEN];
    int     fd;
    struct stat statbuf;

    GTMXLogFileName(xlog_path,XLogCtl->thisTimeLineID,segment_no);

    fd = open(xlog_path,O_RDWR, S_IRUSR | S_IWUSR);

    if(fd < 0)
    {
        elog(LOG,"TruncateXLogFile: Fail to open xlog %s, err_msg:%s", xlog_path, strerror(errno));
        return -1;
    }

    if (fstat(fd, &statbuf) < 0)
    {
        close(fd);
        elog(LOG, "ReadXLog stat file:%s failed for:%s.", xlog_path, strerror(errno));
        return -1;
    }

    if(statbuf.st_size > GTM_XLOG_SEG_SIZE)
    {
        close(fd);
        elog(LOG, "ReadXLog file %s size larger than %d",xlog_path,GTM_XLOG_SEG_SIZE);
        return -1;
    }

    if(ftruncate(fd,size) < 0)
    {
        close(fd);
        elog(LOG,"fail to truncate file %s : %s",xlog_path,strerror(errno));
        return false;
    }
    close(fd);

    return true;
}

/* Open mapper file which uesd in recovery */
void
OpenMapperFile(char *data_dir)
{
    char  path[NODE_STRING_MAX_LENGTH];
    struct stat statbuf;

    snprintf(path, NODE_STRING_MAX_LENGTH, "%s/%s", data_dir, GTM_MAP_FILE_NAME);
    g_GTMStoreMapFile = open(GTM_MAP_FILE_NAME, O_RDWR, S_IRUSR | S_IWUSR);
    if (g_GTMStoreMapFile < 0)
    {
        elog(LOG, "OpenMapperFile open file:%s failed for:%s exit", GTM_MAP_FILE_NAME, strerror(errno));
        exit(1);
    }
    else
    {
        if (fstat(g_GTMStoreMapFile, &statbuf) < 0)
        {
            close(g_GTMStoreMapFile);
            elog(LOG, "OpenMapperFile stat file:%s failed for:%s.", GTM_MAP_FILE_NAME, strerror(errno));
            exit(1);
        }

        if (statbuf.st_size != g_GTMStoreSize)
        {
            close(g_GTMStoreMapFile);
            elog(LOG, "OpenMapperFile stat file:%s size:%zu not equal required size:%zu, file maybe corrupted, try to create a new file.", GTM_MAP_FILE_NAME, statbuf.st_size, g_GTMStoreSize);
            exit(1);
        }
    }
}

/* Close mapper file after the recovery */
void
CloseMapperFile(void)
{
    if(g_GTMStoreMapFile != 0)
    {
        fsync(g_GTMStoreMapFile);
        close(g_GTMStoreMapFile);
    }
}

static XLogRecPtr
GTM_RedoerWaitForData(XLogRecPtr pos)
{
    XLogRecPtr current;

    if(enalbe_gtm_xlog_debug)
    {
        elog(LOG,"Redo wait for %X/%X",(uint32_t)(pos>>32),(uint32_t)pos);
    }

    while(true)
    {
        current = GetStandbyWriteBuffPos();
        if(current >= pos)
        {
            if(enalbe_gtm_xlog_debug)
                elog(LOG,"Redo finish wait for %X/%X get current pos %X/%X",
                     (uint32_t)(pos>>32),(uint32_t)pos,
                     (uint32_t )(current>>32),(uint32_t)current);
            return current;
        }

        if(GTM_SHUTTING_DOWN == GTMTransactions.gt_gtm_state || Recovery_IsStandby() == false)
            return InvalidXLogRecPtr;

        pg_usleep(1000);
    }


    Assert(false);
}

static XLogRecPtr
XLogUsageBytesAdd(XLogRecPtr start,int bytes)
{
    int offset;
    int left;
    XLogRecPtr  ori = start;

    while(bytes != 0)
    {
        left = MIN(UsableBytesInPage,GTM_XLOG_BLCKSZ - start % GTM_XLOG_BLCKSZ);

        if(left >= bytes)
        {
            offset = start % GTM_XLOG_BLCKSZ;

            if(offset < sizeof(XLogPageHeaderData))
                start = start - offset + sizeof(XLogPageHeaderData);

            start += bytes;
            if(enalbe_gtm_xlog_debug)
                elog(LOG,"start %X/%X bytes %d Result: %X/%X",
                     (uint32_t)(ori >> 32),
                     (uint32_t) ori,
                     bytes,
                     (uint32_t)(start >> 32),
                     (uint32_t) start);
            return start ;
        }

        bytes -= left;
        start = start - start % GTM_XLOG_BLCKSZ + GTM_XLOG_BLCKSZ;
    }

    //unreachable code
    return InvalidXLogRecPtr;
}

/*
 * Recovery xlog online and apply to mapper file .
 * you have to guarantee that no dirty data given
 */
void
GTM_ThreadWalRedoer_Internal()
{// #lizard forgives
    char       *xlog_buff;
    char       *xlog_rec;
    uint64      segment_no;
    uint64      idx;
    uint64      page_offset;
    ssize_t     bytes_read;
    ssize_t     desired_bytes;
    ssize_t     read_size;
    ssize_t     cur_xlog_size;
    XLogRecPtr  redo_pos;
    XLogRecPtr  redo_end_pos;
    XLogRecPtr  preXLogRecord;
    XLogRecPtr  startPos;
    XLogRecPtr  xlog_processed_pos;
    XLogRecord  *record_header;
    bool        read_header;
    pg_crc32    crc;
    int         rec_offset = 0;
    XLogPageHeaderData header;
    int         i = 0;
    int         current_buff_size = 2 * UsableBytesInSegment;

    xlog_buff = XLogCtl->writerBuff;

    xlog_rec = palloc(current_buff_size);
    if(xlog_rec == NULL)
    {
        elog(LOG,"GTM_XLogRecovery insufficient memory");
        goto fail_process;
    }
    record_header = (XLogRecord  *)xlog_rec;

    Assert(ControlData != NULL);

    /* init */
    startPos      = XLogCtl->apply;
    segment_no    = startPos / GTM_XLOG_SEG_SIZE;

    /* how many bytes we currently want to read */
    desired_bytes = sizeof(XLogRecord);
    /* how many bytes we have read */
    bytes_read    = 0;
    /* have we read the XLogRecord header? */
    read_header   = false;
    /* last xlog position  */
    preXLogRecord = InvalidXLogRecPtr;
    /* current reading  xlog record */
    redo_pos      = InvalidXLogRecPtr;
    /* last complete xlog record */
    redo_end_pos  = InvalidXLogRecPtr;
    /* offset of the current reading xlog position */
    idx           = startPos % GTM_XLOG_SEG_SIZE;

    xlog_buff += idx;

    elog(LOG,"start redo thread from %X/%X",(uint32)(startPos >> 32),(uint32)startPos);

    GTM_RedoerWaitForData(startPos);

    for(;;)
    {
        cur_xlog_size = 0;

        /* there are still bytes to read */
        for(;;)
        {
            xlog_processed_pos  = segment_no * GTM_XLOG_SEG_SIZE + idx;

            UpdateStandbyApplyPos(xlog_processed_pos);

            if(GTM_SHUTTING_DOWN == GTMTransactions.gt_gtm_state || Recovery_IsStandby() == false)
                break;

            if(idx >= cur_xlog_size)
            {
                /* we wait for at least one page or bytes we desire */
                if(xlog_processed_pos % GTM_XLOG_BLCKSZ == 0)
                    xlog_processed_pos += GTM_XLOG_BLCKSZ;
                else
                    xlog_processed_pos = XLogPtrToNextPageHead(xlog_processed_pos);

                /* we wait for desired bytes */
                xlog_processed_pos = MIN(xlog_processed_pos,XLogUsageBytesAdd(segment_no * GTM_XLOG_SEG_SIZE + idx ,desired_bytes));
                xlog_processed_pos = GTM_RedoerWaitForData(xlog_processed_pos);

                if(xlog_processed_pos  == InvalidXLogRecPtr)
                    break;

                cur_xlog_size = XLogRecPtrToBuffIdx(xlog_processed_pos);

                if(GetCurrentSegmentNo() != segment_no)
                    break;
            }

            if(GTM_SHUTTING_DOWN == GTMTransactions.gt_gtm_state || Recovery_IsStandby() == false)
                break ;

            read_size   = desired_bytes;
            page_offset = idx % GTM_XLOG_BLCKSZ;

            /* if we are at the head of a page,skip the page header */
            if(page_offset == 0)
            {
                memcpy(&header,xlog_buff,sizeof(XLogPageHeaderData));
                xlog_buff += sizeof(XLogPageHeaderData);
                idx       += sizeof(XLogPageHeaderData);

                if(header.xlp_magic != GTM_XLOG_PAGE_MAGIC)
                {
                    elog(LOG,"xlog %X/%X page header validation fails",(uint32)(redo_pos >> 32),(uint32)redo_pos);
                    exit(1);
                }

                /* read to the end of the page if we can */
                read_size = MIN(read_size,UsableBytesInPage);
            }
            else
            {
                /* read to the end of the page if we can */
                read_size = MIN(read_size,GTM_XLOG_BLCKSZ - page_offset);
            }

            read_size = MIN(read_size,cur_xlog_size - idx);

            /* first readable byte means it's the start */
            if(redo_pos == InvalidXLogRecPtr)
                redo_pos = segment_no * GTM_XLOG_SEG_SIZE + idx;

            memcpy(xlog_rec + rec_offset,xlog_buff,(size_t)read_size);

            idx           += read_size;
            xlog_buff     += read_size;
            desired_bytes -= read_size;
            bytes_read    += read_size;
            rec_offset    += read_size;

            /* we have read the XLogRecord struct */
            if(!read_header && bytes_read >= sizeof(XLogRecord))
            {
                read_header   = true;
                /* update bytes we want to read */
                desired_bytes = record_header->xl_tot_len;

                if(desired_bytes > current_buff_size)
                {
                    char *new_buff = palloc(desired_bytes + sizeof(XLogRecord));
                    memcpy(new_buff,xlog_rec,cur_xlog_size);
                    cur_xlog_size = desired_bytes + sizeof(XLogRecord);
                    pfree(xlog_rec);
                    xlog_rec = new_buff;
                }
                if(enalbe_gtm_xlog_debug)
                    elog(LOG,"Xlog %X/%X get size %lu",
                         (uint32)(redo_pos >> 32),(uint32)redo_pos,desired_bytes);
            }

            /* we have finish read the current xlog record */
            if(desired_bytes == 0)
            {
                /* compose crc */
                INIT_CRC32C(crc);
                COMP_CRC32C(crc,xlog_rec + sizeof(XLogRecord),record_header->xl_tot_len);
                COMP_CRC32C(crc,record_header,offsetof(XLogRecord,xl_crc));
                FIN_CRC32C(crc);

                Assert(redo_pos != InvalidXLogRecPtr);

                /* validate */
                if(crc != record_header->xl_crc)
                {
                    elog(LOG,"Xlog %X/%X crc validation fails cal:%u recorded: %u",
                         (uint32)(redo_pos >> 32),(uint32)redo_pos,crc,record_header->xl_crc);
                    exit(1);
                }

                if(preXLogRecord != InvalidXLogRecPtr && preXLogRecord != record_header->xl_prev)
                {
                    elog(LOG,"Xlog %X/%X validation prelink fails should be %X/%X ,"
                                 "but read %X/%X"
                            ,(uint32)(redo_pos >> 32),(uint32)redo_pos
                            ,(uint32)(preXLogRecord >> 32),(uint32)(preXLogRecord)
                            ,(uint32)(record_header->xl_prev >> 32),(uint32)(record_header->xl_prev));
                    exit(1);
                }

                if(enalbe_gtm_xlog_debug)
                    elog(LOG,"Recovery xlog at position %X/%X with pre %X/%X",
                         (uint32)(redo_pos >> 32),(uint32)redo_pos,
                         (uint32)(preXLogRecord >> 32),(uint32)preXLogRecord);

                RedoXLogRecord(record_header,redo_pos);

                /* calculate the end of current xlog */
                redo_end_pos  = segment_no * GTM_XLOG_SEG_SIZE + idx;

                /* clear to read next xlog record */
                desired_bytes = sizeof(XLogRecord);
                bytes_read    = 0;
                read_header   = false;
                preXLogRecord = redo_pos;
                redo_pos      = InvalidXLogRecPtr;
                rec_offset    = 0;

                SpinLockAcquire(&XLogCtl->Insert.insertpos_lck);
                XLogCtl->Insert.CurrBytePos = XLogRecPtrToBytePos(redo_end_pos);
                XLogCtl->Insert.PrevBytePos = XLogRecPtrToBytePos(preXLogRecord);
                SpinLockRelease(&XLogCtl->Insert.insertpos_lck);
            }
        }

        if(GTM_SHUTTING_DOWN == GTMTransactions.gt_gtm_state)
        {
            elog(LOG,"redoer shutting down");
            break;
        }

        /* if we promote a slave ,we first do checkpoint ,so it must reach here */
        if(Recovery_IsStandby() == false)
        {
            elog(LOG,"promote slave,redo thread finish");
            break;
        }

        segment_no++;
        idx = 0;
        xlog_buff = XLogCtl->writerBuff;
    }

    Assert(redo_end_pos != InvalidXLogRecPtr);
    elog(LOG,"redo exit,recovery finish upto %X/%X",(uint32)(redo_end_pos >> 32) ,(uint32)redo_end_pos);

    if(enalbe_gtm_xlog_debug)
    {
        elog(LOG,"XLogRecovery to CurrBytePos to %"PRIu64" PrevBytePos to %"PRIu64"",XLogCtl->Insert.CurrBytePos,XLogCtl->Insert.PrevBytePos);
        elog(LOG,"XLogRecovery to EndOfXLog to %X/%X PreEndOfXLog to %X/%X",
             (uint32)(XLogBytePosToEndRecPtr(XLogCtl->Insert.CurrBytePos) >> 32),
             (uint32)XLogBytePosToEndRecPtr(XLogCtl->Insert.CurrBytePos),
             (uint32)(XLogBytePosToEndRecPtr(XLogCtl->Insert.PrevBytePos) >> 32),
             (uint32)XLogBytePosToEndRecPtr(XLogCtl->Insert.PrevBytePos)
        );
    }

    XLogCtl->currentSegment = segment_no;
    XLogCtl->last_write_idx = idx;

    XLogCtl->LogwrtResult.Flush = redo_end_pos;
    XLogCtl->LogwrtResult.Write = redo_end_pos;

    if(xlog_rec != NULL)
        pfree(xlog_rec);

    for(i = 0; i < g_GTMStoreSize;i++)
        g_GTMStoreDirtyMap[i] = true;

    if(Recovery_IsStandby())
        DoSlaveCheckPoint(false);
    else
        DoCheckPoint(false);

    return ;

    fail_process:

    if(xlog_rec != NULL)
        pfree(xlog_rec);

    elog(LOG,"gtm recovery fails");
    exit(1);
}

void
GTM_RecoveryUpdateMetaData(XLogRecPtr redo_end_pos,XLogRecPtr preXLogRecord,uint64 segment_no,int idx)
{
    Assert(redo_end_pos != InvalidXLogRecPtr);
    elog(LOG,"recovery finish upto %X/%X",(uint32)(redo_end_pos >> 32) ,(uint32)redo_end_pos);

    if(redo_end_pos == InvalidXLogRecPtr)
    {
        elog(LOG,"recovery fails");
        exit(1);
    }

    if(TruncateXLogFile(segment_no,redo_end_pos % GTM_XLOG_SEG_SIZE) == false)
        exit(1);
        
    /* update byte pos */
    XLogCtl->Insert.CurrBytePos = XLogRecPtrToBytePos(redo_end_pos);
    XLogCtl->Insert.PrevBytePos = XLogRecPtrToBytePos(preXLogRecord);

    if(enalbe_gtm_xlog_debug)
    {
        elog(LOG,"XLogRecovery to CurrBytePos to %"PRIu64" PrevBytePos to %"PRIu64"",XLogCtl->Insert.CurrBytePos,XLogCtl->Insert.PrevBytePos);
        elog(LOG,"XLogRecovery to EndOfXLog to %X/%X PreEndOfXLog to %X/%X",
             (uint32)(XLogBytePosToEndRecPtr(XLogCtl->Insert.CurrBytePos) >> 32),
             (uint32)XLogBytePosToEndRecPtr(XLogCtl->Insert.CurrBytePos),
             (uint32)(XLogBytePosToEndRecPtr(XLogCtl->Insert.PrevBytePos) >> 32),
             (uint32)XLogBytePosToEndRecPtr(XLogCtl->Insert.PrevBytePos)
        );
    }

    XLogCtl->currentSegment = segment_no;
    XLogCtl->last_write_idx = idx;

    XLogCtl->LogwrtResult.Flush = redo_end_pos;
    XLogCtl->LogwrtResult.Write = redo_end_pos;

    ControlData->prevCheckPoint = ControlData->checkPoint;
    ControlData->checkPoint     = preXLogRecord;

    ControlData->PrevBytePos = XLogCtl->Insert.PrevBytePos;
    ControlData->CurrBytePos = XLogCtl->Insert.CurrBytePos;

    return ;
}

/* Recovery Xlog and apply to mapper file */
void
GTM_XLogRecovery(XLogRecPtr startPos,char *data_dir)
{// #lizard forgives
    char       *xlog_buff;
    char       *xlog_rec;
    uint64      segment_no;
    uint64      idx;
    uint64      page_offset;
    ssize_t     bytes_read;
    ssize_t     desired_bytes;
    ssize_t     read_size;
    ssize_t     cur_xlog_size;
    XLogRecPtr  redo_pos;
    XLogRecPtr  redo_end_pos;
    XLogRecPtr  preXLogRecord;
    XLogRecord  *record_header;
    bool        read_header;
    pg_crc32    crc;
    int         rec_offset = 0;
    XLogPageHeaderData header;
    bool        has_error       = false;
    bool        validate_mode   = false;
    bool        validate_count = GTM_XLOG_RECOVERY_GTS_VALIDATE_NUM;
    GlobalTimestamp last_validate_gts = InvalidGTS;

    /* return if no xlog when firstly inited */
    if(startPos == FIRST_XLOG_REC)
        return ;

    xlog_buff = XLogCtl->writerBuff;

    elog(LOG,"start recovery from %X/%X",(uint32)(startPos >> 32),(uint32)startPos);

    OpenMapperFile(data_dir);

    /* One record must not larger then UsableBytesInSegment */
    xlog_rec = palloc(UsableBytesInSegment);
    if(xlog_rec == NULL)
    {
        elog(LOG,"GTM_XLogRecovery insufficient memory");
        goto exit_process;
    }
    record_header = (XLogRecord  *)xlog_rec;

    Assert(ControlData != NULL);

    segment_no    = startPos / GTM_XLOG_SEG_SIZE;

    cur_xlog_size = ReadXLogToBuff(segment_no);
    if(cur_xlog_size < 0)
        goto exit_process;

    /* how many bytes we currently want to read */
    desired_bytes = sizeof(XLogRecord);
    /* how many bytes we have read */
    bytes_read    = 0;
    /* have we read the XLogRecord header? */
    read_header   = false;
    /* last xlog postion  */
    preXLogRecord = InvalidXLogRecPtr;
    /* current reading  xlog record */
    redo_pos      = InvalidXLogRecPtr;
    /* last complete xlog recored */
    redo_end_pos  = InvalidXLogRecPtr;
    /* offset of the current reading xlog position */
    idx           = startPos % GTM_XLOG_SEG_SIZE;

    xlog_buff += idx;

    g_recovery_finish = false;

    for(;;)
    {
        /* there are still bytes to read */
        while(idx < cur_xlog_size)
        {
            read_size   = desired_bytes;

            page_offset = idx % GTM_XLOG_BLCKSZ;

            /* if we are at the head of a page,skip the page header */
            if(page_offset == 0)
            {
                memcpy(&header,xlog_buff,sizeof(XLogPageHeaderData));
                xlog_buff += sizeof(XLogPageHeaderData);
                idx       += sizeof(XLogPageHeaderData);

                if(header.xlp_magic != GTM_XLOG_PAGE_MAGIC)
                {
                    elog(LOG,"Xlog %X/%X page header validation fails",(uint32)(redo_pos >> 32),(uint32)redo_pos);
                    has_error = true;
                    break;
                }

                /* read to the end of the page if we can */
                read_size = MIN(read_size,UsableBytesInPage);
            }
            else
            {
                /* read to the end of the page if we can */
                read_size = MIN(read_size,GTM_XLOG_BLCKSZ - page_offset);
            }

            /* first readable byte means it's the start */
            if(redo_pos == InvalidXLogRecPtr)
                redo_pos = segment_no * GTM_XLOG_SEG_SIZE + idx;

            memcpy(xlog_rec + rec_offset,xlog_buff,read_size);

            idx            += read_size;
            xlog_buff      += read_size;
            desired_bytes -= read_size;
            bytes_read     += read_size;
            rec_offset     += read_size;

            /* we have read the XLogRecord struct */
            if(!read_header && bytes_read >= sizeof(XLogRecord))
            {
                read_header   = true;
                /* update bytes we want to read */
                desired_bytes = record_header->xl_tot_len;
            }

            /* we have finish read the current xlog record */
            if(desired_bytes == 0)
            {
                /* compose crc */
                INIT_CRC32C(crc);
                COMP_CRC32C(crc,xlog_rec + sizeof(XLogRecord),record_header->xl_tot_len);
                COMP_CRC32C(crc,record_header,offsetof(XLogRecord,xl_crc));
                FIN_CRC32C(crc);

                Assert(redo_pos != InvalidXLogRecPtr);

                if(enalbe_gtm_xlog_debug)
                    elog(LOG,"Recovey xlog at position %X/%X with pre %X/%X length %u ",
                         (uint32)(redo_pos >> 32),(uint32)redo_pos,
                         (uint32)(preXLogRecord >> 32),(uint32)preXLogRecord,record_header->xl_tot_len);

                /* validate */
                if(crc != record_header->xl_crc)
                {
                    elog(LOG,"Xlog %X/%X crc validation fails cal:%u recorded: %u",
                         (uint32)(redo_pos >> 32),(uint32)redo_pos,crc,record_header->xl_crc);
                    has_error = true;
                    break;
                }

                if(preXLogRecord != InvalidXLogRecPtr && preXLogRecord != record_header->xl_prev)
                {
                    elog(LOG,"Xlog %X/%X validation prelink fails",(uint32)(redo_pos >> 32),(uint32)redo_pos);
                    has_error = true;
                    break;
                }

                if(validate_mode)
                {
                    if(last_validate_gts > record_header->xl_timestamp)
                    {
                        elog(PANIC,"gts reverse in recovery founded from %ld to %ld at %X/%X",
                            last_validate_gts,record_header->xl_timestamp,(uint32)(redo_pos >> 32),(uint32)redo_pos);
                    }

                    last_validate_gts = record_header->xl_timestamp;
                    if(enalbe_gtm_xlog_debug)
                    {
                        elog(LOG,"validate xlog at %X/%X has gts %ld",
                            (uint32)(redo_pos >> 32),(uint32)redo_pos,record_header->xl_timestamp);
                    }

                    validate_count--;
                    if(validate_count == 0)
                        goto exit_process;
                }
                else
                {
                    RedoXLogRecord(record_header,redo_pos);
                    if(g_recovery_finish)
                    {
                        GTM_RecoveryUpdateMetaData(redo_end_pos,preXLogRecord,segment_no,idx);
                        validate_mode = true;
                        last_validate_gts = record_header->xl_timestamp;
                    }
                }

                /* calculate the end of current xlog */
                redo_end_pos  = segment_no * GTM_XLOG_SEG_SIZE + idx;

                /* clear to read next xlog record */
                desired_bytes = sizeof(XLogRecord);
                bytes_read    = 0;
                read_header   = false;
                preXLogRecord = redo_pos;
                redo_pos      = InvalidXLogRecPtr;
                rec_offset    = 0;
            }
        }

        cur_xlog_size = ReadXLogToBuff(segment_no + 1);
        if(cur_xlog_size < 0)
        {
            //crash recovery to the end of file
            if(recovery_timestamp == InvalidGTS)
            { 
                GTM_RecoveryUpdateMetaData(redo_end_pos,preXLogRecord,segment_no,idx);
                g_recovery_finish = true;
            }
            goto exit_process;
        }
        else if(has_error)
        {
            elog(LOG,"Recovery ends with an error,probably a bug");
            exit(1);
        }

        segment_no++;
        idx = 0;
        xlog_buff = XLogCtl->writerBuff;
    }

exit_process:

    CloseMapperFile();
    if(xlog_rec != NULL)
        pfree(xlog_rec);

    if(g_recovery_finish)
    {
        if(validate_count != 0)
        {
            elog(WARNING,"gtm recovery finished, but due to lack of xlog,validation could not complete");
        }
        return ;
    }
    else 
    {
        elog(LOG,"gtm recovery fails");
        exit(1);
    }
}

static void
GTM_PrintControlData()
{
    elog(LOG,"ControlData context:");
    elog(LOG,"ControlData->gtm_control_version    = %d",ControlData->gtm_control_version);
    elog(LOG,"ControlData->xlog_seg_size          = %d",ControlData->xlog_seg_size);
    elog(LOG,"ControlData->xlog_blcksz            = %d",ControlData->xlog_blcksz);
    elog(LOG,"ControlData->state                  = %d",ControlData->state);
    elog(LOG,"ControlData->CurrBytePos            = %"PRIu64,ControlData->CurrBytePos);
    elog(LOG,"ControlData->PrevBytePos            = %"PRIu64,ControlData->PrevBytePos);
    elog(LOG,"ControlData->thisTimeLineID         = %d",ControlData->thisTimeLineID);
    elog(LOG,"ControlData->prevCheckPoint         = %X/%X",(uint32_t)(ControlData->prevCheckPoint >> 32),(uint32_t)ControlData->prevCheckPoint);
    elog(LOG,"ControlData->checkPoint             = %X/%X",(uint32_t)(ControlData->checkPoint >> 32),(uint32_t)ControlData->checkPoint);
    elog(LOG,"ControlData->gts                    = %lu",ControlData->gts);
    elog(LOG,"ControlData->time                   = %lu",ControlData->time);
}

/* Init ControlData during startup */
int
GTM_ControlDataInit(void)
{// #lizard forgives
    int fd;
    struct stat statbuf;
    pg_crc32 crc;
    uint64 nbytes;

    g_GTMControlDataSize = sizeof(ControlFileData);

    GTM_RWLockInit(&ControlDataLock);

    Assert(ControlData == NULL);

    ControlData = palloc0(g_GTMControlDataSize);
    if (NULL == ControlData)
    {
        elog(LOG, "GTM_ControlDataInit control file:%s failed for:%s.", GTM_CONTROL_FILE, strerror(errno));
        return GTM_STORE_ERROR;
    }

    fd = open(GTM_CONTROL_FILE,O_RDONLY);

    /* it means the first time we start gtm */
    if(fd < 0)
    {
        ControlData->gtm_control_version = GTM_CONTROL_VERSION;
        ControlData->xlog_seg_size       = GTM_XLOG_SEG_SIZE;
        ControlData->xlog_blcksz         = GTM_XLOG_BLCKSZ;
        ControlData->state               = DB_SHUTDOWNED;
        ControlData->CurrBytePos         = FIRST_USABLE_BYTE;
        ControlData->PrevBytePos         = FIRST_USABLE_BYTE;
        ControlData->thisTimeLineID      = FIRST_TIMELINE_ID;
        ControlData->prevCheckPoint      = InvalidXLogRecPtr;
        ControlData->checkPoint          = FIRST_XLOG_REC;
        ControlData->gts                 = FirstGlobalTimestamp;
        ControlData->time                = time(NULL);

        ControlDataSync(false);

        first_init = true;
    }
    else
    {
        if (fstat(fd, &statbuf) < 0)
        {
            elog(LOG, "GTM_ControlDataInit stat file:%s failed for:%s.", GTM_CONTROL_FILE, strerror(errno));
            close(fd);
            return GTM_STORE_ERROR;
        }

        if (statbuf.st_size != g_GTMControlDataSize && statbuf.st_size != 80)
        {
            close(fd);
            elog(LOG, "GTM_ControlDataInit stat file:%s size:%zu not equal required size:%zu, file maybe corrupted, try to create a new file.", GTM_CONTROL_FILE, statbuf.st_size, g_GTMControlDataSize);
            return GTM_STORE_ERROR;
        }

        nbytes = read(fd, ControlData, statbuf.st_size);
        if (nbytes != statbuf.st_size)
        {
            elog(LOG, "GTM_ControlDataInit read file:%s failed for:%s, reqiured size:%zu, read size:%zu.", GTM_CONTROL_FILE, strerror(errno), g_GTMControlDataSize, nbytes);
            goto fail_process;
        }

        INIT_CRC32C(crc);
        COMP_CRC32C(crc,ControlData,offsetof(ControlFileData,crc));
        FIN_CRC32C(crc);

        if(ControlData->crc != crc)
        {
            elog(LOG, "GTM_ControlDataInit crc validate fails (recorded: %u file: %u) .", ControlData->crc,crc);
            goto fail_process;
        }

        if(ControlData->xlog_seg_size != GTM_XLOG_SEG_SIZE)
        {
            elog(LOG, "GTM_ControlDataInit GTM_XLOG_SEG_SIZE validate fails (recorded: %u file: %u) .", GTM_XLOG_SEG_SIZE, ControlData->xlog_seg_size);
            goto fail_process;
        }

        if(ControlData->xlog_blcksz != GTM_XLOG_BLCKSZ)
        {
            elog(LOG, "GTM_ControlDataInit GTM_XLOG_BLCKSZ validate fails (recorded: %u file: %u) .",GTM_XLOG_BLCKSZ, ControlData->xlog_blcksz);
            goto fail_process;
        }

        if(ControlData->gtm_control_version != GTM_CONTROL_VERSION)
        {
            elog(LOG, "GTM_ControlDataInit gtm_control_version validate fails (recorded: %d file: %d) .",GTM_CONTROL_VERSION, ControlData->gtm_control_version);
            goto fail_process;
        }

        first_init = false;
        close(fd);


    }

    GTM_PrintControlData();

    return GTM_STORE_OK;

    fail_process:
    if(fd > 0)
        close(fd);
    pfree(ControlData);

    return GTM_STORE_ERROR;
}

/* sync control data to disk */
void
ControlDataSync(bool update_time)
{
    int fd;
    int nbyte;
    int ret;
    pg_crc32 crc;

    Assert(ControlData != NULL);

    if(update_time)
    {
        ControlData->time  = time(NULL);
        ControlData->gts   = GetNextGlobalTimestamp();
    }

    INIT_CRC32C(crc);
    COMP_CRC32C(crc,ControlData,offsetof(ControlFileData,crc));
    FIN_CRC32C(crc);

    ControlData->crc = crc;

    fd = open(GTM_CONTROL_FILE_TMP,O_WRONLY| O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);

    if (fd < 0)
    {
        elog(LOG, "ControlDataSync: fail to open file %s, err_msg %s", GTM_CONTROL_FILE_TMP, strerror(errno));
        exit(1);
    }

    nbyte = write(fd, ControlData, sizeof(ControlFileData));

    if(nbyte != sizeof(ControlFileData))
    {
        elog(LOG, "write control data fails");
        close(fd);
        exit(1);
    }

    fsync(fd);
    close(fd);

    ret = rename(GTM_CONTROL_FILE_TMP,GTM_CONTROL_FILE);
    if(ret != 0)
    {
        elog(LOG, "rename control data file fails");
        exit(1);
    }

    if(enalbe_gtm_xlog_debug)
    {
        elog(LOG, "ControlDataSync ok current_bytes: %"PRIu64" pre_bytes %"PRIu64"",ControlData->CurrBytePos,ControlData->PrevBytePos);
    }
}

static char *
strip(char *s)
{
    int i = 0;
    int head = 0;
    int tail = 0;
    int p = 0;
    int len = strlen(s);

    while(s[head] != '\0' && isblank(s[head]))
    {
        head++;
    }

    if( head == len )
    {
        s[0] = '\0';
        return s;
    }

    tail = len - 1;

    while(tail >= 0 && isblank(s[tail]))
    {
        tail--;
    }

    for(i = head ; i <= tail ; i++)
        s[p++] = s[i];

    s[p] = '\0';

    return s;
}

static bool
is_contain(const char *pattern,char ch)
{
   while(*pattern && *pattern != ch)
   {
        pattern++;
   }

    return *pattern == ch;
}

static char *
skip_to_next(char *s,const char *token)
{
    while(*s && is_contain(token,*s))
    {
       s++;
    }

    return s;
}

static bool
start_with_ignore_case(const char *s,const char *pattern) 
{
    int i = 0; 
    for( i = 0; pattern[i] && s[i] ; i++)
    {
        if(toupper(pattern[i]) != toupper(s[i]))
            return false;
    }

    return pattern[i] == '\0';
}

/* Init sync replication data */
void
GTM_StandbyBaseinit(void)
{
    init_sync_structures();
    load_sync_structures();
}

/* Print xlog command for debug */
uint32
PrintRedoRangeOverwrite(XLogCmdRangerOverWrite *cmd)
{
    elog(LOG,"RangeOverwrite %d %d",cmd->offset,cmd->bytes);

    return cmd->bytes + sizeof(XLogCmdRangerOverWrite);
}

uint32
PrintRedoCheckPoint(XLogCmdCheckPoint *cmd)
{
    elog(LOG,"PrintRedoCheckPoint %"PRIu64" ",cmd->gts);
    return sizeof(XLogCmdCheckPoint);
}

uint32
PrintRedoTimestamp(XLogRecGts *cmd)
{
    elog(LOG,"PrintRedoCheckPoint %"PRIu64" ",cmd->gts);
    return sizeof(XLogCmdCheckPoint);
}

/* Redo relative xlog command */
static uint32
RedoRangeOverwrite(XLogCmdRangerOverWrite *cmd)
{// #lizard forgives
    size_t nbytes;

    if(enalbe_gtm_xlog_debug)
        PrintRedoRangeOverwrite(cmd);

    if(Recovery_IsStandby() && recovery_pitr_mode == false && promote_status == GTM_PRPMOTE_NORMAL)
    {
        memcpy(g_GTMStoreMapAddr + cmd->offset,cmd->data,cmd->bytes);

        if(cmd->offset == 0 && cmd->bytes >= sizeof(GTMControlHeader))
        {
            SetNextGlobalTimestamp(g_GTM_Store_Header->m_next_gts + GTM_GTS_ONE_SECOND * GTMStartupGTSDelta);
            if(enalbe_gtm_xlog_debug)
            {
                elog(LOG,"redo header");
                GTM_PrintControlHeader();
            }
        }
    }
    else
    {
        nbytes = lseek(g_GTMStoreMapFile, cmd->offset, SEEK_SET);
        if (nbytes != cmd->offset)
        {
            elog(LOG, "could not seek map file for: %s", strerror(errno));
            exit(1);
        }

        nbytes = write(g_GTMStoreMapFile, cmd->data, cmd->bytes);
        if (cmd->bytes != nbytes)
        {
            elog(LOG, "could not write map for: %s, required bytes:%d, return bytes:%zu", strerror(errno), cmd->bytes, nbytes);
            exit(1);
        }
    }

    return cmd->bytes + sizeof(XLogCmdRangerOverWrite);
}

static uint32
RedoCheckPoint(XLogCmdCheckPoint *cmd,XLogRecPtr pos)
{
    if(enalbe_gtm_xlog_debug)
        PrintRedoCheckPoint(cmd);

    SetCurrentTimeLineID(cmd->timeline);

    if(Recovery_IsStandby() && recovery_pitr_mode == false && promote_status == GTM_PRPMOTE_NORMAL)
        DoCheckPoint(false);

    return sizeof(XLogCmdCheckPoint);
}

static uint32
RedoTimestamp(XLogRecGts *cmd)
{
    if(enalbe_gtm_xlog_debug)
        PrintRedoTimestamp(cmd);
    return sizeof(XLogRecGts);
}

/* Redo one xlog record */
static void
RedoXLogRecord(XLogRecord *rec,XLogRecPtr pos)
{// #lizard forgives
    uint32   len;
    char    *data;
    uint32   cmd_size;
    XLogCmdHeader *header;
    static GlobalTimestamp last_timestamp = InvalidGTS;

    len = rec->xl_tot_len;

    if(recovery_timestamp != InvalidGTS &&  rec->xl_timestamp > recovery_timestamp)
    {
        elog(LOG,"recovery finish upto gts: %ld",last_timestamp);
        g_recovery_finish = true;
        return ;
    }

    if(last_timestamp > rec->xl_timestamp)
    {
        elog(PANIC,"timestamp reverse in recovery from %lu to %lu",last_timestamp,rec->xl_timestamp);
    }

    if(enalbe_gtm_xlog_debug)
        elog(LOG,"recovery timestamp %lu",rec->xl_timestamp);

    last_timestamp = rec->xl_timestamp;

    data = (char *)rec + sizeof(XLogRecord);

    while(len > 0)
    {
        header = (XLogCmdHeader *)data;

        switch (header->type)
        {
            case XLOG_CMD_RANGE_OVERWRITE:
                cmd_size = RedoRangeOverwrite((XLogCmdRangerOverWrite *)data);
                break;
            case XLOG_CMD_CHECK_POINT:
                cmd_size = RedoCheckPoint((XLogCmdCheckPoint *)data,pos);
                break;
            case XLOG_REC_GTS:
                cmd_size = RedoTimestamp((XLogRecGts *)data);
                break;
            default:
                elog(LOG, "unrecognize xlog command type %d",header->type);
                exit(1);
        }

        len  -= cmd_size;
        data += cmd_size;
    }

}

void
DoCheckPoint(bool shutdown)
{
    GTM_MutexLockAcquire(&g_CheckPointLock);

    if(Recovery_IsStandby())
        DoSlaveCheckPoint(true);
    else
        DoMasterCheckPoint(shutdown);

    GTM_MutexLockRelease(&g_CheckPointLock);
}

void
DoSlaveCheckPoint(bool write_check_point)
{
    int nbytes;
    XLogRecPtr flush_ptr;

    flush_ptr = GetStandbyWriteBuffPos();

    nbytes = (int) lseek(g_GTMStoreMapFile, 0, SEEK_SET);
    if (nbytes != 0)
    {
        elog(LOG, "could not seek map file for: %s", strerror(errno));
        exit(1);
    }

    nbytes = (int) write(g_GTMStoreMapFile, g_GTMStoreMapAddr, g_GTMStoreSize);
    if (g_GTMStoreSize != nbytes)
    {
        elog(LOG, "could not write map for: %s, required bytes:%d, return bytes:%d", strerror(errno), 0, nbytes);
        exit(1);
    }

    if(fsync(g_GTMStoreMapFile))
    {
        elog(LOG, "couldn't sync file %s",strerror(errno));
        exit(1);
    }

    if(write_check_point)
    {
        GTM_RWLockAcquire(&ControlDataLock,GTM_LOCKMODE_WRITE);

        ControlData->prevCheckPoint  = ControlData->checkPoint;
        ControlData->checkPoint      = flush_ptr - flush_ptr % GTM_XLOG_SEG_SIZE ;  // point to the head of segment

        ControlDataSync(true);

        GTM_RWLockRelease(&ControlDataLock);
    }
}

/* Do checkpoint */
void
DoMasterCheckPoint(bool shutdown)
{// #lizard forgives
    int nbytes;
    int size;
    int write_start;
    int i;
    int ret;
    XLogRecPtr flush_ptr;
    int idx;

    Assert(g_GTMStoreMapFile != -1);

    elog(LOG,"Start to dump checkpoint");

    idx = 0;

    XLogBeginInsert();

    /* lock all the write of mapper file */
    GTM_StoreLock();

    WALInsertLockAcquire();

    /* we lock header lock here ,because we want to shorten the interval of header lock holding */
    GTM_RWLockAcquire(g_GTM_Store_Head_Lock,GTM_LOCKMODE_READ);
    memcpy(g_checkpointMapperBuff,g_GTMStoreMapAddr,g_GTMStoreSize);

    for(i = 0 ; i < g_GTMStoreSize ; i++)
    {
        while(i < g_GTMStoreSize && !g_GTMStoreDirtyMap[i]) i++;

        write_start = i;

        if( i == g_GTMStoreSize )
            break;

        size = 0;
        while(i < g_GTMStoreSize && g_GTMStoreDirtyMap[i])
        {
            g_GTMStoreDirtyMap[i] = false;
            size++;
            i++;
        }

        g_checkpointDirtySize[idx]    = size;
        g_checkpointDirtyStart[idx++] = write_start;
    }

    XLogRegisterCheckPoint();

    /* checkpoint command always starts at the beginning of xlog */
    SwitchXLogFile();

    /* insert checkpoint record to the xlog */
    flush_ptr = XLogCheckPointInsert();

    /* if we are about to shutdown then we hold the lock to the end */
    /* in case of there are someone still writes */
    if(!shutdown)
    {
        GTM_RWLockRelease(g_GTM_Store_Head_Lock);
        GTM_StoreUnLock();
    }

    /* writes all dirty section */
    for(i = 0 ; i < idx ; i++)
    {
        write_start = g_checkpointDirtyStart[i];
        size        = g_checkpointDirtySize[i];

        nbytes = (int) lseek(g_GTMStoreMapFile, write_start, SEEK_SET);
        if (nbytes != write_start)
        {
            elog(LOG, "could not seek map file for: %s", strerror(errno));
            exit(1);
        }

        nbytes = (int) write(g_GTMStoreMapFile, g_checkpointMapperBuff + write_start, size);
        if (size != nbytes)
        {
            elog(LOG, "could not write map for: %s, required bytes:%d, return bytes:%d", strerror(errno), size, nbytes);
            exit(1);
        }
    }

    ret = fsync(g_GTMStoreMapFile);
    if (ret)
    {
        elog(LOG, "could not fsync map file for: %s", strerror(errno));
        exit(1);
    }

    ReleaseXLogInsertLock();

    XLogFlush(flush_ptr);

    /* save checkpoint position to ControlData */
    GTM_RWLockAcquire(&ControlDataLock,GTM_LOCKMODE_WRITE);

    ControlData->thisTimeLineID = GetCurrentTimeLineID();
    ControlData->prevCheckPoint = ControlData->checkPoint;
    ControlData->checkPoint      = flush_ptr - flush_ptr % GTM_XLOG_SEG_SIZE ;  // point to the head of segment

    ControlDataSync(true);

    elog(LOG, "Checkpoint done");
    GTM_RWLockRelease(&ControlDataLock);
}

/* wait until all the xlog before upto to copy to the buff */
/* and returns the last xlog positon that we can flush */
static XLogRecPtr
WaitXLogInsertionsToFinish(XLogRecPtr upto)
{
    uint64        bytepos;
    XLogRecPtr    reservedUpto;
    XLogRecPtr    finishedUpto;
    XLogInsertLock *XLogInsertLocks;
    XLogCtlInsert  *Insert;
    XLogRecPtr      insertingat;
    int     i;
    int lock_id;

    XLogInsertLocks = XLogCtl->insert_lck;
    Insert = &XLogCtl->Insert;

    /* Read the current insert position */
    SpinLockAcquire(&Insert->insertpos_lck);
    bytepos = Insert->CurrBytePos;
    SpinLockRelease(&Insert->insertpos_lck);
    reservedUpto = XLogBytePosToEndRecPtr(bytepos);

    /*
     * Loop through all the locks, sleeping on any in-progress insert older
     * than 'upto'.
     *
     * finishedUpto is our return value, indicating the point upto which all
     * the WAL insertions have been finished. Initialize it to the head of
     * reserved WAL, and as we iterate through the insertion locks, back it
     * out for any insertion that's still in progress.
     */
    finishedUpto = reservedUpto;

    lock_id = GetMyThreadInfo->insert_lock_id;

    for (i = 0; i < NUM_XLOGINSERT_LOCKS; i++)
    {
        if(i == lock_id)
            continue;

        insertingat = InvalidXLogRecPtr;

        do
        {
            if(GTM_MutexLockConditionalAcquire(&XLogInsertLocks[i].l))
            {
                GTM_MutexLockRelease(&XLogInsertLocks[i].l);
                insertingat = InvalidXLogRecPtr;
                break;
            }

            SpinLockAcquire(&XLogInsertLocks[i].m);
            insertingat = XLogInsertLocks[i].start;
            SpinLockRelease(&XLogInsertLocks[i].m);

        } while (insertingat < upto);

        if (insertingat != InvalidXLogRecPtr && insertingat < finishedUpto)
        {
            Assert(insertingat >= upto);
            finishedUpto = insertingat;
        }
    }

    /* we can only flush to the end of the segment at most*/
    if(GetSegmentNo(finishedUpto - 1) > GetSegmentNo(upto - 1))
        return upto;

    Assert(finishedUpto >= upto);

    return finishedUpto;
}

/*
 * Convert an "usable byte position" to the start of XLogRecPtr.
 */
static XLogRecPtr
XLogBytePosToStartRecPtr(uint64 byte)
{
    uint64 page_offset = byte % UsableBytesInPage;
    return (byte / UsableBytesInPage) * GTM_XLOG_BLCKSZ + page_offset + (page_offset == 0 ? 0 : sizeof(XLogPageHeaderData));;
}

/*
 * Convert an "usable byte position" to the end of XLogRecPtr.
 */
static XLogRecPtr
XLogBytePosToEndRecPtr(uint64 byte)
{
    uint64 page_offset = byte % UsableBytesInPage;
    return (byte / UsableBytesInPage) * GTM_XLOG_BLCKSZ + page_offset + (page_offset == 0 ? 0 : sizeof(XLogPageHeaderData));
}

/*
 * Convert an XLogRecPtr to an "usable byte position".
 */
static uint64
XLogRecPtrToBytePos(XLogRecPtr ptr)
{
    uint64 page_offset = ptr % GTM_XLOG_BLCKSZ;

    if(page_offset <= sizeof(XLogPageHeaderData))
        return (ptr / GTM_XLOG_BLCKSZ) * UsableBytesInPage;
    return (ptr / GTM_XLOG_BLCKSZ) * UsableBytesInPage + page_offset - sizeof(XLogPageHeaderData);
}

/*
 * Convert an XLogRecPtr to a offset in segment.
 */
static uint32
XLogRecPtrToBuffIdx(XLogRecPtr ptr)
{
    return ptr % GTM_XLOG_SEG_SIZE;
}

static uint64
XLogRecPtrToFileOffset(XLogRecPtr ptr)
{
    return ptr % GTM_XLOG_SEG_SIZE;
}

/*
 * Begin constructing a WAL record. This must be called before the
 * XLogRegister* functions and XLogInsert().
 * regiseter_buff will be freed in writer thread.
 */
void
XLogBeginInsert(void)
{
    GTM_ThreadInfo *thr = GetMyThreadInfo;
    XLogRegisterBuff *registered_buffer;

    //ReleaseXLogRegisterBuff();

    registered_buffer = (XLogRegisterBuff *)palloc(sizeof(XLogRegisterBuff));

    if (registered_buffer == NULL)
    {
        elog(LOG,"out of memory");
        exit(1);
    }

    registered_buffer->rdata_head = NULL;
    registered_buffer->rdata_rear = NULL;
    registered_buffer->rdata_len  = 0;

    thr->register_buff = registered_buffer;
}

/*
 * Init xlog command header 
 */
static void
InitXLogCmdHeader(XLogCmdHeader *header,uint32 type)
{
    header->type = type;
}

static void
AppendXLogRegisterBuff(XLogRegisterBuff *buff,XLogRecData *data)
{
    buff->rdata_len += data->len;

    if(buff->rdata_head == NULL)
        buff->rdata_head = buff->rdata_rear = data;
    else
    {
        buff->rdata_rear->next = data;
        buff->rdata_rear = data;
    }
    data->next = NULL;
}

/*
 * Register a overwrite action with specific range in map file with the WAL record being constructed.
 * This must be called for any map file modification.
 */
void
XLogRegisterRangeOverwrite(offset_t offset,
                           int32_t len,char *data)

{
    XLogRecData *rec_data = NULL;
    XLogCmdRangerOverWrite *cmd = NULL;
    XLogRegisterBuff *reg_buff = GetMyThreadInfo->register_buff;
    int i;

    rec_data = (XLogRecData *)palloc(sizeof(XLogRecData));

    if(rec_data == NULL)
    {
        elog(LOG,"out of memory");
        exit(1);
    }

    cmd = (XLogCmdRangerOverWrite *)palloc(sizeof(XLogCmdRangerOverWrite) + len);
    if(cmd == NULL)
    {
        elog(LOG,"out of memory");
        exit(1);
    }

    memcpy(cmd->data,data,len);
    cmd->offset = offset;
    cmd->bytes = len;

    InitXLogCmdHeader(&cmd->hdr,XLOG_CMD_RANGE_OVERWRITE);

    rec_data->data = (char *)cmd;
    rec_data->len  = sizeof(XLogCmdRangerOverWrite) + len;

    AppendXLogRegisterBuff(reg_buff,rec_data);

    if(enalbe_gtm_xlog_debug)
        elog(LOG,"%lu %d",offset,len);

    i = offset;
    while(len)
    {
        g_GTMStoreDirtyMap[i++] = true;
        len--;
    }

}

/*
 * Register a overwrite action with specific range in map file with the WAL record being constructed.
 * This must be called for any map file modification.
 */
void
XLogRegisterCheckPoint(void)
{
    XLogRecData *rec_data = NULL;
    XLogCmdCheckPoint *cmd = NULL;
    XLogRegisterBuff *reg_buff = GetMyThreadInfo->register_buff;

    rec_data = (XLogRecData *)palloc(sizeof(XLogRecData));

    if(rec_data == NULL)
    {
        elog(LOG,"out of memory");
        exit(1);
    }

    cmd = (XLogCmdCheckPoint *)palloc(sizeof(XLogCmdCheckPoint));
    if(cmd == NULL)
    {
        elog(LOG,"out of memory");
        exit(1);
    }

    InitXLogCmdHeader(&cmd->hdr,XLOG_CMD_CHECK_POINT);
    cmd->gts      = GetNextGlobalTimestamp();
    cmd->timeline = GetCurrentTimeLineID();
    cmd->time     = time(NULL);

    rec_data->data = (char *)cmd;
    rec_data->len  = sizeof(XLogCmdCheckPoint);

    AppendXLogRegisterBuff(reg_buff,rec_data);
}

/*
 * Register a overwrite action with specific range in map file with the WAL record being constructed.
 * This must be called for any map file modification.
 */
void
XLogRegisterTimeStamp(void)
{
    XLogRecData *rec_data = NULL;
    XLogRecGts *cmd = NULL;
    XLogRegisterBuff *reg_buff = GetMyThreadInfo->register_buff;

    rec_data = (XLogRecData *)palloc(sizeof(XLogRecData));

    if(rec_data == NULL)
    {
        elog(LOG,"out of memory");
        exit(1);
    }

    cmd = (XLogRecGts *)palloc(sizeof(XLogRecGts));
    if(cmd == NULL)
    {
        elog(LOG,"out of memory");
        exit(1);
    }

    InitXLogCmdHeader(&cmd->hdr,XLOG_REC_GTS);
    cmd->gts       = GetNextGlobalTimestamp();

    rec_data->data = (char *)cmd;
    rec_data->len  = sizeof(XLogRecGts);

    AppendXLogRegisterBuff(reg_buff,rec_data);
}

static void
XLogSegmentGtsMaxUpdate(char *raw_xlog_data)
{
    XLogRecord *record = (XLogRecord *)raw_xlog_data;

    Assert(record->xl_timestamp != InvalidGTS);
    
    SpinLockAcquire(&XLogCtl->segment_gts_lck);
    if(XLogCtl->segment_max_gts <= record->xl_timestamp)
    {   
        XLogCtl->segment_max_gts        = record->xl_timestamp;
        XLogCtl->segment_max_timestamp  = record->xl_time;
    }
    SpinLockRelease(&XLogCtl->segment_gts_lck);
}

/*
 * insert xlog checkpoint record into xlog
 */
static XLogRecPtr
XLogCheckPointInsert()
{
    char *data;
    char *data_with_pageheader;
    XLogCtlInsert *Insert = &XLogCtl->Insert;
    uint64        startbytepos;
    uint64        endbytepos;
    uint64        prevbytepos;
    uint64      size ;
    uint64      byte_left;
    XLogRecord *rec;
    XLogRecPtr    StartPos;
    XLogRecPtr    EndPos;
    XLogRegisterBuff *reg_buff = GetMyThreadInfo->register_buff;

    Assert(reg_buff && reg_buff->rdata_len != 0);

    size = sizeof(XLogRecord) + reg_buff->rdata_len;

    data = XLogAssemble();

    /*
     * The duration the spinlock needs to be held is minimized by minimizing
     * the calculations that have to be done while holding the lock. The
     * current tip of reserved WAL is kept in CurrBytePos, as a byte position
     * that only counts "usable" bytes in WAL, that is, it excludes all WAL
     * page headers. The mapping between "usable" byte positions and physical
     * positions (XLogRecPtrs) can be done outside the locked region, and
     * because the usable byte position doesn't include any headers, reserving
     * X bytes from WAL is almost as simple as "CurrBytePos += X".
     */
    rec  = (XLogRecord *)data;
    SpinLockAcquire(&Insert->insertpos_lck);

    if(enalbe_gtm_xlog_debug)
    {
        elog(LOG,"Begin byte size %"PRIu64" %"PRIu64" %"PRIu64"",size,Insert->CurrBytePos,Insert->PrevBytePos);
    }

    byte_left = Insert->CurrBytePos % UsableBytesInSegment;
    

    if(byte_left == 0)
    {
        startbytepos = Insert->CurrBytePos;
        endbytepos   = startbytepos + size;
        prevbytepos  = Insert->PrevBytePos;

        Insert->CurrBytePos = endbytepos;
        Insert->PrevBytePos = startbytepos;
    }
    else
    {
        startbytepos = Insert->CurrBytePos - byte_left + UsableBytesInSegment;
        endbytepos   = startbytepos + size;
        prevbytepos  = Insert->PrevBytePos;

        Insert->CurrBytePos = endbytepos;
        Insert->PrevBytePos = startbytepos;
    }

    rec->xl_timestamp = XLogCtl->segment_max_gts;
    rec->xl_time      = time(NULL);

    if(enalbe_gtm_xlog_debug)
    {
        elog(LOG,"End byte size %"PRIu64" %"PRIu64" %"PRIu64"",size,Insert->CurrBytePos,Insert->PrevBytePos);
    }
    SpinLockRelease(&Insert->insertpos_lck);

    StartPos = XLogBytePosToStartRecPtr(startbytepos);
    EndPos   = XLogBytePosToEndRecPtr(endbytepos);

    UpdateInsertInfo(StartPos,EndPos);

    rec->xl_prev = XLogBytePosToStartRecPtr(prevbytepos);

    if(rec->xl_prev % GTM_XLOG_BLCKSZ == 0)
        rec->xl_prev += sizeof(XLogPageHeaderData);


    /* Finish crc calculation */
    COMP_CRC32C(rec->xl_crc, rec,offsetof(XLogRecord, xl_crc));
    FIN_CRC32C(rec->xl_crc);

    if(enalbe_gtm_xlog_debug)
        elog(LOG,"StartPos: %X/%X Prelink: %X/%X Size:%d crc:%u",
             (uint32)(StartPos >> 32),(uint32)StartPos,
             (uint32)(rec->xl_prev >> 32),(uint32)rec->xl_prev,
             rec->xl_tot_len,
             rec->xl_crc);

    XLogSegmentGtsMaxUpdate(data);

    /* get xlog record with page header added */
    data_with_pageheader = XLogDataAddPageHeader(StartPos,data,&size);
    /* copy xlog record to xlog buff */
    CopyXLogRecordToBuff(data_with_pageheader,StartPos,EndPos,size);

    ReleaseXLogRegisterBuff();

    pfree(data);
    pfree(data_with_pageheader);

    if(enalbe_gtm_xlog_debug)
        elog(LOG,"return %"PRIu64"",EndPos);

    return EndPos;
}

static void ReleaseXLogRecordWriteLocks(void)
{
    GTM_ThreadInfo *thr = GetMyThreadInfo;
    int i ;

    if(thr == NULL || thr->xlog_inserting == false)
        return ;

    thr->xlog_inserting = false;
    for(i = 0; i < thr->current_write_number;i++)
    {

        if(thr->write_counters[i] == 0 )
            GTM_RWLockRelease(thr->write_locks_hold[i]);
        else if(thr->write_counters[i] > 1)
            elog(PANIC,"write counts too many");
    }
    thr->current_write_number = 0;
}

/*
 * Insert a xlog record into xlog buff 
 */
XLogRecPtr
XLogInsert(void)
{// #lizard forgives
    char *data;
    char *data_with_pageheader;
    XLogCtlInsert *Insert = &XLogCtl->Insert;
    uint64        startbytepos;
    uint64        endbytepos;
    uint64        prevbytepos;
    uint64        size ;
    XLogRecord    *rec;
    XLogRecPtr    StartPos;
    XLogRecPtr    EndPos;
    XLogRegisterBuff *reg_buff = GetMyThreadInfo->register_buff;

    Assert(reg_buff && reg_buff->rdata_len != 0);

    size = sizeof(XLogRecord) + reg_buff->rdata_len;

    data = XLogAssemble();

    WALInsertLockAcquire();

    /*
     * The duration the spinlock needs to be held is minimized by minimizing
     * the calculations that have to be done while holding the lock. The
     * current tip of reserved WAL is kept in CurrBytePos, as a byte position
     * that only counts "usable" bytes in WAL, that is, it excludes all WAL
     * page headers. The mapping between "usable" byte positions and physical
     * positions (XLogRecPtrs) can be done outside the locked region, and
     * because the usable byte position doesn't include any headers, reserving
     * X bytes from WAL is almost as simple as "CurrBytePos += X".
     */
    rec  = (XLogRecord *)data;
    SpinLockAcquire(&Insert->insertpos_lck);

    if(enalbe_gtm_xlog_debug)
    {
        elog(LOG,"Begin byte size %"PRIu64" %"PRIu64" %"PRIu64"",size,Insert->CurrBytePos,Insert->PrevBytePos);
    }

    startbytepos = Insert->CurrBytePos;
    endbytepos   = startbytepos + size;
    prevbytepos  = Insert->PrevBytePos;
    Insert->CurrBytePos = endbytepos  ;
    Insert->PrevBytePos = startbytepos;

    rec->xl_timestamp = GetNextGlobalTimestamp();
    rec->xl_time      = time(NULL);

    if(enalbe_gtm_xlog_debug)
    {
        elog(LOG,"End byte size %"PRIu64" %"PRIu64" %"PRIu64"",size,Insert->CurrBytePos,Insert->PrevBytePos);
    }

    SpinLockRelease(&Insert->insertpos_lck);

    StartPos = XLogBytePosToStartRecPtr(startbytepos);
    EndPos   = XLogBytePosToEndRecPtr(endbytepos);

    Assert(StartPos + size <= EndPos);

    UpdateInsertInfo(StartPos,EndPos);

    

    rec->xl_prev = XLogBytePosToStartRecPtr(prevbytepos);

    if(rec->xl_prev % GTM_XLOG_BLCKSZ == 0)
        rec->xl_prev += sizeof(XLogPageHeaderData);


    COMP_CRC32C(rec->xl_crc, rec,offsetof(XLogRecord, xl_crc));
    FIN_CRC32C(rec->xl_crc);

    {
        size_t ori = size;
        if(enalbe_gtm_xlog_debug)
            elog(LOG,"before xlog StartPos: %"PRIu64" xlog EndPos: %"PRIu64" ori_size: %"PRIu64" with_header_size: %"PRIu64"",StartPos,EndPos,ori,size);

        XLogSegmentGtsMaxUpdate(data);
        data_with_pageheader = XLogDataAddPageHeader(StartPos,data,&size);

        if(enalbe_gtm_xlog_debug)
            elog(LOG,"after xlog StartPos: %"PRIu64" xlog EndPos: %"PRIu64" ori_size: %"PRIu64" with_header_size: %"PRIu64"",StartPos,EndPos,ori,size);
    }

    if(enalbe_gtm_xlog_debug)
        elog(LOG,"StartPos: %X/%X Prelink: %X/%X Size:%d crc:%u",
             (uint32)(StartPos >> 32),(uint32)StartPos,
             (uint32)(rec->xl_prev >> 32),(uint32)rec->xl_prev,
             rec->xl_tot_len,
             rec->xl_crc);


    for(;;)
    {
        if(!XLogInCurrentSegment(StartPos))
            pg_usleep(10);
        else
            break;
    }

    CopyXLogRecordToBuff(data_with_pageheader,StartPos,EndPos,size);

    ReleaseXLogInsertLock();

    ReleaseXLogRegisterBuff();

    pfree(data);
    pfree(data_with_pageheader);

    return EndPos;
}

/*
 * Move xlog ptr to the end of page
 */
static XLogRecPtr
XLogPtrToPageEnd(XLogRecPtr ptr)
{
    return ( ptr / GTM_XLOG_BLCKSZ + 1 ) * GTM_XLOG_BLCKSZ - 1;
}

/*
 * Move xlog ptr to the next page head
 */
static XLogRecPtr
XLogPtrToNextPageHead(XLogRecPtr ptr)
{
    uint64 left = ptr % GTM_XLOG_BLCKSZ;
    if(left == 0)
        return ptr;
    return ( ptr / GTM_XLOG_BLCKSZ + 1 ) * GTM_XLOG_BLCKSZ ;
}

/*
 * Calaulate bytes between xlog
 */
static uint64_t
XLogDistance(XLogRecPtr lhs,XLogRecPtr rhs)
{
    if(lhs > rhs)
        return  lhs - rhs;
    return rhs - lhs ;
}

/*
 * Calculate xlog len with pageheader
 */
static size_t
GetXLogRecLenWithPageHeader(XLogRecPtr start,size_t len)
{
    size_t page_left = 0;
    size_t start_offset = start % GTM_XLOG_BLCKSZ;
    size_t addition = 0;
    size_t use_page = 0;

    if(start_offset >= sizeof(XLogPageHeaderData))
        page_left = GTM_XLOG_BLCKSZ - start_offset;
    else
        page_left = UsableBytesInPage;

    if(start_offset == 0 || start_offset == sizeof(XLogPageHeaderData))
        addition = sizeof(XLogPageHeaderData);

    if(page_left >= len)
        return len + addition;

    use_page = (len - page_left) / UsableBytesInPage;

    if((len - page_left) % UsableBytesInPage > 0 )
        use_page++;

    return addition + len + use_page * sizeof(XLogPageHeaderData);
}

/*
 * Add page header for a xlog record
 */
static char*
XLogDataAddPageHeader(XLogRecPtr start,char *data,size_t *len)
{
    uint64 ans_offset  = 0;
    uint64 data_offset = 0;
    uint64 size;
    size_t actual_size;
    uint64 nleft;
    XLogPageHeaderData header;
    XLogRecPtr next_page_head;
    char *ans;

    if(start % GTM_XLOG_BLCKSZ == sizeof(XLogPageHeaderData))
        start -= sizeof(XLogPageHeaderData);

    actual_size = GetXLogRecLenWithPageHeader(start,*len);
    ans = palloc(actual_size);

    if(ans == NULL)
    {
        elog(LOG,"memory insufficient");
        exit(1);
    }

    header.xlp_magic    = GTM_XLOG_PAGE_MAGIC;
    header.xlp_info     = 0;
    header.xlp_pageaddr = InvalidXLogRecPtr;

    nleft = *len;
    next_page_head = XLogPtrToNextPageHead(start);

    /* if start is not head of page,then we copy to the end of page */
    if(start != next_page_head)
    {
        size = XLogPtrToPageEnd(start) - start + 1;
        if(size > nleft)
            size = nleft;
        memcpy(ans,data,size);
        ans_offset  += size;
        data_offset += size;
        nleft       -= size;
    }

    Assert(nleft >= 0);

    while(nleft > 0)
    {
        size = MIN(nleft,UsableBytesInPage);

        header.xlp_pageaddr = next_page_head;

        memcpy(ans + ans_offset ,&header ,sizeof(XLogPageHeaderData));
        ans_offset += sizeof(XLogPageHeaderData);

        memcpy(ans + ans_offset ,data + data_offset ,size);
        ans_offset  += size;
        data_offset += size;
        nleft       -= size;

        next_page_head += GTM_XLOG_BLCKSZ;
    }

    Assert(actual_size == ans_offset);

    *len = actual_size;
    return ans;
}

/* 
 * Release register buff for thr current thread 
 */
static void
ReleaseXLogRegisterBuff(void)
{
    XLogRegisterBuff *reg_buff = GetMyThreadInfo->register_buff;
    XLogRecData *head;
    XLogRecData *save;

    if(reg_buff == NULL)
        return ;

    for(head = reg_buff->rdata_head; head != NULL; )
    {
        save = head;
        head = head->next;

        pfree(save->data);
        pfree(save);
    }

    pfree(reg_buff);

    GetMyThreadInfo->register_buff = NULL;
}

XLogSegNo
GetCurrentSegmentNo(void)
{
    XLogSegNo currentSegment;

    GTM_RWLockAcquire(&XLogCtl->segment_lck,GTM_LOCKMODE_READ);
    currentSegment = XLogCtl->currentSegment;
    GTM_RWLockRelease(&XLogCtl->segment_lck);

    return currentSegment;
}

/*
 * Check whether pos is in current open segment file
 */
bool
XLogInCurrentSegment(XLogRecPtr pos)
{
    uint64 currentSegment;

    GTM_RWLockAcquire(&XLogCtl->segment_lck,GTM_LOCKMODE_READ);
    currentSegment = XLogCtl->currentSegment;
    GTM_RWLockRelease(&XLogCtl->segment_lck);

    return currentSegment == GetSegmentNo(pos) ? true : false;
}

/*
 * Writer request position to xlog file 
 */
static void
XLogWrite(XLogRecPtr req)
{// #lizard forgives
    uint64  nleft;
    uint64  total_write;
    uint64  start_pos;
    int     written;
    uint64  end_pos;
    XLogRecPtr flush_pos;
    long long  start_time;
    long long  end_time;

    end_pos = XLogRecPtrToFileOffset(req);

    GTM_MutexLockAcquire(&XLogCtl->walwrite_lck);

    SpinLockAcquire(&XLogCtl->walwirte_info_lck);
    flush_pos = XLogCtl->LogwrtResult.Flush;
    SpinLockRelease(&XLogCtl->walwirte_info_lck);

    if(flush_pos >= req)
    {
        elog(DEBUG1,"XLogWrite request %"PRIu64" but already %"PRIu64" return ",req,flush_pos);
        GTM_MutexLockRelease(&XLogCtl->walwrite_lck);
        return ;
    }

    start_pos = XLogCtl->last_write_idx;

    if(end_pos == 0)
        nleft  = GTM_XLOG_SEG_SIZE - start_pos;
    else
    {
        Assert(start_pos <= end_pos);
        nleft  = end_pos - start_pos;
    }

    total_write = nleft;

    Assert(nleft <= GTM_XLOG_SEG_SIZE);

    if(nleft == 0)
    {
        GTM_MutexLockRelease(&XLogCtl->walwrite_lck);
        return ;
    }

    if(enalbe_gtm_xlog_debug)
    {
        elog(LOG,"XLogWrite req write to %X/%X  %"PRIu64" bytes",
             (uint32_t)(req >> 32),
             (uint32_t)req,nleft);
    }

    start_time = getSystemTime();

    do
    {
        errno = 0;
        written = write(XLogCtl->xlog_fd, XLogCtl->writerBuff + start_pos, nleft);
        if (written <= 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            elog(LOG,"write fails %s",strerror(errno));
            exit(1);
        }

        nleft     -= written;
        start_pos += written;

        if(nleft != 0)
        {
            elog(LOG,"write fails %s",strerror(errno));
            exit(1);
        }
    } while (nleft > 0);

    XLogCtl->last_write_idx = end_pos;

    fsync(XLogCtl->xlog_fd);
    
    end_time = getSystemTime();
    if(end_time - start_time > warnning_time_cost)
        elog(LOG, "XLogWrite size %ld lsn %X/%X cost %lld ms", total_write, (uint32)(req >> 32), (uint32)req, end_time - start_time);

    SpinLockAcquire(&XLogCtl->walwirte_info_lck);
    Assert(XLogCtl->LogwrtResult.Write  <= req);
    XLogCtl->LogwrtResult.Write = req;
    XLogCtl->LogwrtResult.Flush = req;
    SpinLockRelease(&XLogCtl->walwirte_info_lck);

    GTM_MutexLockRelease(&XLogCtl->walwrite_lck);
}

/* To make sure xlog flush to disk as much as we can  */
bool
XLogBackgroundFlush(void)
{
    //TODO
    return true;
}

/* To make sure xlog ptr lower than ptr -1 have successfully been flush to disk */
void
XLogFlush(XLogRecPtr ptr)
{
    XLogRecPtr flush_pos;
    XLogRecPtr write_pos;

    if(enalbe_gtm_xlog_debug)
        elog(LOG,"flush to %X/%X",(uint32)(ptr >> 32),(uint32)ptr);

    if(ptr == InvalidXLogRecPtr)
        return ;

    SpinLockAcquire(&XLogCtl->walwirte_info_lck);
    flush_pos = XLogCtl->LogwrtResult.Flush;
    SpinLockRelease(&XLogCtl->walwirte_info_lck);

    if(flush_pos >= ptr)
        return ;

    if(Recovery_IsStandby() == false)
        write_pos = WaitXLogInsertionsToFinish(ptr);
    else
        write_pos = ptr;

    /* Only now we can notify flush because we can guarantee all the xlog data are in buff */
    NotifyReplication(write_pos);

    XLogWrite(write_pos);
}

/*
 * Assemble xlog record for register buff
 */
static char *
XLogAssemble(void)
{
    char *data;
    char *p_data;
    XLogRecord  *record;
    XLogRecData *rec_data;
    pg_crc32c crc;
    XLogRegisterBuff* registered_buffer;

    registered_buffer = GetMyThreadInfo->register_buff;

    /* total length of xlog record including XLogRecord header */
    data = palloc(sizeof(XLogRecord) + registered_buffer->rdata_len);
    record = (XLogRecord *) data;
    p_data = data + sizeof(XLogRecord); /* skip XlogRecord header */

    /*
     * Calculate CRC of the data
     *
     * Note that the record header isn't added into the CRC initially since we
     * don't know the prev-link yet.  Thus, the CRC will represent the CRC of
     * the whole record in the order: rdata , then record header.
     */

    INIT_CRC32C(crc);
    for(rec_data = registered_buffer->rdata_head; rec_data != NULL; rec_data = rec_data->next)
    {
        COMP_CRC32C(crc, rec_data->data,rec_data->len);
        memcpy(p_data,rec_data->data,rec_data->len);

        p_data += rec_data->len;
    }

    record->xl_crc = crc;
    record->xl_tot_len = registered_buffer->rdata_len;
    record->xl_prev = InvalidXLogRecPtr;
    record->xl_info = 0;
    record->xl_timestamp = InvalidGTS;
    record->xl_time = 0;

    return data;
}

/*
 * Get a wal insert lock which is held during copy xlog to buff
 */
static void
WALInsertLockAcquire(void)
{
    GTM_ThreadInfo *thr = GetMyThreadInfo;

    thr->insert_lock_id = thr->insert_try_lock_id;

    if(!GTM_MutexLockConditionalAcquire(&XLogCtl->insert_lck[thr->insert_lock_id].l))
    {
        if(!GTM_MutexLockAcquire(&XLogCtl->insert_lck[thr->insert_lock_id].l))
        {
            elog(LOG,"mutex acquire fault");
            exit(1);
        }

        thr->insert_try_lock_id = ( thr->insert_try_lock_id + 1 ) % NUM_XLOGINSERT_LOCKS;
    }
}

/*
 * After finishing copy xlog record to the buff,we release wal insert lock
 */
static void
ReleaseXLogInsertLock(void)
{
    GTM_ThreadInfo *thr = GetMyThreadInfo;

    SpinLockAcquire(&XLogCtl->insert_lck[thr->insert_lock_id].m);
    XLogCtl->insert_lck[thr->insert_lock_id].start = InvalidXLogRecPtr;
    XLogCtl->insert_lck[thr->insert_lock_id].end   = InvalidXLogRecPtr;
    SpinLockRelease(&XLogCtl->insert_lck[thr->insert_lock_id].m);

    Assert(thr->insert_lock_id != -1);

    if(!GTM_MutexLockRelease(&XLogCtl->insert_lck[thr->insert_lock_id].l))
        exit(1);

    thr->insert_lock_id = -1;
}

/*
 * While holding wal insert lock ,once we know our insert position then we update it,
 * so that others will know how far to can flush
 */
static void
UpdateInsertInfo(XLogRecPtr start,XLogRecPtr end)
{
    GTM_ThreadInfo *thr = GetMyThreadInfo;

    Assert(thr->insert_lock_id != -1);
    SpinLockAcquire(&XLogCtl->insert_lck[thr->insert_lock_id].m);

    XLogCtl->insert_lck[thr->insert_lock_id].start = start;
    XLogCtl->insert_lck[thr->insert_lock_id].end   = end;

    if(enalbe_gtm_xlog_debug)
        elog(LOG,"UpdateInsertInfo lock id : %d upadte to rec %"PRIu64"",thr->insert_lock_id,start);

    SpinLockRelease(&XLogCtl->insert_lck[thr->insert_lock_id].m);
}

TimeLineID
GetCurrentTimeLineID(void)
{
    TimeLineID current;

    SpinLockAcquire(&XLogCtl->timeline_lck);
    current = XLogCtl->thisTimeLineID;
    SpinLockRelease(&XLogCtl->timeline_lck);

    return current;
}

void
SetCurrentTimeLineID(TimeLineID timeline)
{
    SpinLockAcquire(&XLogCtl->timeline_lck);
    XLogCtl->thisTimeLineID = timeline;
    SpinLockRelease(&XLogCtl->timeline_lck);
}


/*
 * Open a already-exist xlog file
 */
static void
OpenXLogFile(XLogSegNo segment_no,XLogRecPtr flush)
{
    char path_buff[MAXFNAMELEN];
    int fd;
    int offset;
    TimeLineID timeline;

    timeline = GetCurrentTimeLineID();

    GTMXLogFileName(path_buff,timeline,segment_no);

    GTM_MutexLockAcquire(&XLogCtl->walwrite_lck);

    if(XLogCtl->xlog_fd != 0)
        close(XLogCtl->xlog_fd);

    fd = open(path_buff, O_RDWR);
    if(fd == -1)
    {
        elog(LOG,"Xlog %s fails to open : %s",path_buff,strerror(errno));
        exit(1);
    }

    offset = flush % GTM_XLOG_SEG_SIZE;

    if(lseek(fd,0,SEEK_SET) != 0)
    {
        elog(LOG,"Xlog %s fails to read %d : %s",path_buff,offset,strerror(errno));
        exit(1);
    }

    if(read(fd,XLogCtl->writerBuff,offset) != offset)
    {
        elog(LOG,"Xlog %s fails to read %d : %s",path_buff,offset,strerror(errno));
        exit(1);
    }

    XLogCtl->last_write_idx = offset;
    XLogCtl->xlog_fd = fd;

    GTM_MutexLockRelease(&XLogCtl->walwrite_lck);
}

/*
 * New an empty xlog file 
 */
void
NewXLogFile(XLogSegNo segment_no)
{
    char path_buff[MAXFNAMELEN];
    int fd;
    TimeLineID timeline;

    timeline = GetCurrentTimeLineID();

    GTMXLogFileName(path_buff,timeline,segment_no);

    GTM_MutexLockAcquire(&XLogCtl->walwrite_lck);

    if(XLogCtl->xlog_fd != 0)
        close(XLogCtl->xlog_fd);

    fd = open(path_buff, O_WRONLY| O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    if(fd == -1)
    {
        elog(LOG,"Xlog %s fails to create : %s",path_buff,strerror(errno));
        exit(1);
    }

    if(enalbe_gtm_xlog_debug)
        elog(LOG,"switch last_write_idx from %"PRIu64" to 0",XLogCtl->last_write_idx);

    XLogCtl->last_write_idx = 0;
    XLogCtl->xlog_fd = fd;
    XLogCtl->currentSegment = segment_no;

    memset(XLogCtl->writerBuff,0,sizeof(XLogCtl->writerBuff));

    if(enalbe_gtm_xlog_debug)
    {
        elog(LOG,"switch to xlog %s",path_buff);
    }

    GTM_MutexLockRelease(&XLogCtl->walwrite_lck);
}

static void
GenerateStatusFile(uint64 segment_no)
{
    char archiveStatusPath[MAXFNAMELEN];
    TimeLineID timeline  = FIRST_TIMELINE_ID;
    FILE *fp             = NULL;

    timeline = GetCurrentTimeLineID();

    GTMXLogFileStatusReadyName(archiveStatusPath,timeline,segment_no);

    fp = fopen(archiveStatusPath,"w");

    if(fp == NULL)
    {
        elog(LOG,"could not create archive status file \"%s\": %s",archiveStatusPath,strerror(errno));
        return;
    }

    if(fclose(fp) != 0)
    {
        elog(LOG,"could not write archive status file \"%s\": %s",archiveStatusPath,strerror(errno));
        exit(1);
    }
}

static void
AddXLogGts(uint64 segment_no)
{
    char archiveStatusPath[MAXFNAMELEN];
    TimeLineID timeline  = FIRST_TIMELINE_ID;
    FILE *fp             = NULL;
    GlobalTimestamp      gts ;
    pg_time_t            time;

    timeline = GetCurrentTimeLineID();

    GTMXLogFileGtsName(archiveStatusPath,timeline,segment_no);

    fp = fopen(archiveStatusPath,"w");

    if(fp == NULL)
    {
        elog(LOG,"could not create archive status file \"%s\": %s",archiveStatusPath,strerror(errno));
        return;
    }

    SpinLockAcquire(&XLogCtl->segment_gts_lck);
    gts  = XLogCtl->segment_max_gts ;
    time = XLogCtl->segment_max_timestamp;
    SpinLockRelease(&XLogCtl->segment_gts_lck);

    fprintf(fp,"global_timestamp:%lu\n",gts);
    fprintf(fp,"gtm_time:%lu",time);

    if(fclose(fp) != 0)
    {
        elog(LOG,"could not write archive status file \"%s\": %s",archiveStatusPath,strerror(errno));
        exit(1);
    }
}

/* Only one thread could call this */
/* You have to make sure all the data in current segment have been flush */
void
SwitchXLogFile(void)
{
    uint64 current_segment;
    uint64 allocated_bytes;
    XLogRecPtr  allocated;

    GTM_RWLockAcquire(&XLogCtl->segment_lck,GTM_LOCKMODE_WRITE);
    current_segment = XLogCtl->currentSegment;
    GTM_RWLockRelease(&XLogCtl->segment_lck);

    if(Recovery_IsStandby())
    {
        allocated = GetStandbyWriteBuffPos();
    }
    else
    {
        AddXLogGts(current_segment);

        SpinLockAcquire(&XLogCtl->Insert.insertpos_lck);
        allocated_bytes = XLogCtl->Insert.CurrBytePos;
        SpinLockRelease(&XLogCtl->Insert.insertpos_lck);

        allocated = XLogBytePosToEndRecPtr(allocated_bytes);
    }

    XLogFlush(MIN((current_segment + 1) * GTM_XLOG_SEG_SIZE,allocated));

    if(!Recovery_IsStandby())
        GenerateStatusFile(current_segment);

    if(Recovery_IsStandby())
    {
        if(enalbe_gtm_xlog_debug)
        {
            elog(LOG, "wait for apply complete apply next_segment %ld",current_segment + 1);
        }
        /* wait for redo thread to finish data fetch */
        while(GetStandbyApplyPos() < allocated)
        {
            pg_usleep(10);
        }

        if(enalbe_gtm_xlog_debug)
        {
            elog(LOG, "finish wait for apply complete apply next_segment %ld",current_segment + 1);
        }
    }


    GTM_RWLockAcquire(&XLogCtl->segment_lck,GTM_LOCKMODE_WRITE);

    NewXLogFile(current_segment + 1);

    GTM_RWLockRelease(&XLogCtl->segment_lck);
}

/* ShutDown xlog relative global structure for gtm shutdown */
void
XLogCtlShutDown(void)
{
    SpinLockAcquire(&XLogCtl->Insert.insertpos_lck);
    ControlData->PrevBytePos = XLogCtl->Insert.PrevBytePos;
    ControlData->CurrBytePos = XLogCtl->Insert.CurrBytePos;
}

/* 
 * Copy one xlog record to xlog buff
 * Switch xlog file if necessary
 */
bool
CopyXLogRecordToBuff(char *data,XLogRecPtr start,XLogRecPtr end,size_t size)
{// #lizard forgives
    uint64     write_pos;
    uint64     write_byte;
    XLogRecPtr finish_write_pos;

    if(start % GTM_XLOG_BLCKSZ == sizeof(XLogPageHeaderData))
        start -= sizeof(XLogPageHeaderData);

    write_pos        = XLogRecPtrToBuffIdx(start);
    finish_write_pos = start ;

    if(enalbe_gtm_xlog_debug)
        elog(LOG,"CopyXLogRecordToBuff start_ptr:%X/%X end_ptr:%X/%X size:%ld",
             (uint32_t) (start >> 32),
             (uint32_t) (start),
             (uint32_t) (end >> 32),
             (uint32_t) (end),
             size);

    while(size != 0)
    {
        /* calculate bytes left for the current segment. */
        write_byte = GTM_XLOG_SEG_SIZE - write_pos ;

        if(write_byte > size)
            write_byte = size;

        Assert(write_pos + write_byte <= GTM_XLOG_SEG_SIZE);

        memcpy(XLogCtl->writerBuff + write_pos,data,write_byte);

        size             -= write_byte;
        write_pos        += write_byte;
        data             += write_byte;
        finish_write_pos += write_byte;

        /* as the last record at current segment ,we have to switch xlog */
        if(Recovery_IsStandby() == false && write_pos == GTM_XLOG_SEG_SIZE)
        {
            if(enalbe_gtm_xlog_debug)
                elog(LOG,"CopyXLogRecordToBuff start_ptr:%X/%X switch xlog",
                     (uint32_t)(start >> 32),
                     (uint32_t)start);

            SwitchXLogFile();
            write_pos = 0;
        }
    }
    Assert(write_pos % GTM_XLOG_SEG_SIZE == XLogRecPtrToBuffIdx(end));

    return true;
}


static void
gtm_init_replication_data(GTM_StandbyReplication *replication)
{
    GTM_XLogSegmentBuff *buff;

    GTM_MutexLockInit(&replication->lock);

    replication->is_use = false;

    GTM_MutexLockInit(&replication->pos_status_lck);
    GTM_CVInit(&replication->pos_update_cv);
    replication->flush_ptr         = InvalidXLogRecPtr;
    replication->replay_ptr        = InvalidXLogRecPtr;
    replication->write_ptr         = InvalidXLogRecPtr;
    replication->time_line         = FIRST_TIMELINE_ID;

    replication->send_ptr          = InvalidXLogRecPtr;

    GTM_CVInit(&replication->xlog_to_send);
    GTM_MutexLockInit(&replication->send_request_lck);
    replication->send_request      = InvalidXLogRecPtr;

    replication->walsender_thread  = NULL;
    replication->port              = NULL;

    buff = &replication->xlog_read_buff;

    buff->total_length = -1;
    buff->segment_no   = 0;

    replication->is_sync  = false;
}

void
gtm_close_replication(GTM_StandbyReplication *replication)
{
    GTM_XLogSegmentBuff *buff;

    GTM_MutexLockAcquire(&replication->lock);

    replication->is_use = false;

    GTM_MutexLockAcquire(&replication->pos_status_lck);

    replication->flush_ptr         = InvalidXLogRecPtr;
    replication->replay_ptr        = InvalidXLogRecPtr;
    replication->write_ptr         = InvalidXLogRecPtr;
    replication->time_line         = FIRST_TIMELINE_ID;

    GTM_MutexLockRelease(&replication->pos_status_lck);

    replication->send_ptr          = InvalidXLogRecPtr;

    GTM_MutexLockAcquire(&replication->send_request_lck);
    replication->send_request      = InvalidXLogRecPtr;
    GTM_MutexLockRelease(&replication->send_request_lck);

    buff = &replication->xlog_read_buff;

    buff->total_length = -1;
    buff->segment_no   = 0;

    replication->is_sync  = false;

    GTM_MutexLockRelease(&replication->lock);
}

bool
gtm_standby_resign_to_walsender(Port *port,const char *node_name,const char *replication_name)
{
    int i;
    GTM_StandbyReplication *replication;

    for(i = 0; i < max_wal_sender ;i++)
    {
        replication = g_StandbyReplication + i;

        GTM_MutexLockAcquire(&replication->lock);

        if(replication->is_use == false)
        {
	    	if(enalbe_gtm_xlog_debug)
				elog(LOG,"gtm_standby_resign_to_walsender node_name:%s replication_name:%s",node_name,replication_name);
			
            replication->is_use  = true ;
            replication->port    = port ;

            memcpy(replication->application_name,replication_name,strlen(replication_name) + 1);
            memcpy(replication->node_name,node_name,strlen(node_name) + 1);

            if(enable_sync_commit && IsInSyncStandbyList(replication_name))
            {
                replication->is_sync = true;

				GTM_MutexLockAcquire(&XLogSync->check_mutex);
                RegisterNewSyncStandby(replication);
				GTM_MutexLockRelease(&XLogSync->check_mutex);
            }
            else
                replication->is_sync = false;

            GTM_MutexLockRelease(&replication->lock);
            return true;
        }

        GTM_MutexLockRelease(&replication->lock);
    }

    elog(LOG,"Error not enough wal sender ,please increase max_wal_sender to proper a number.");
    return false;
}

GTM_StandbyReplication *
register_self_to_standby_replication()
{
    int i ;
    GTM_StandbyReplication *replication;

    for(i = 0; i < max_wal_sender ;i++)
    {
        replication = g_StandbyReplication + i;

        GTM_MutexLockAcquire(&replication->lock);

        if(replication->walsender_thread == NULL)
        {
            replication->walsender_thread = GetMyThreadInfo;
            GTM_MutexLockRelease(&replication->lock);
            return replication ;
        }

        GTM_MutexLockRelease(&replication->lock);
    }
    return NULL;
}

XLogwrtResult
GetCurrentXLogwrtResult()
{
    XLogwrtResult result;

    SpinLockAcquire(&XLogCtl->walwirte_info_lck);
    result = XLogCtl->LogwrtResult;
    SpinLockRelease(&XLogCtl->walwirte_info_lck);

    return result;
}

bool
IsXLogFileExist(const char *file)
{
    return access(file,F_OK) == 0 ? true : false;
}

void
ValidXLogRecoveryCondition()
{
    if(recovery_target_timestamp != NULL)
    {
        recovery_timestamp = atoll(recovery_target_timestamp);

//        if(ControlData->gts  > recovery_timestamp)
//        {
//            elog(LOG,"Base file time time stamp %ld is older than %ld",ControlData->gts,recovery_timestamp);
//            exit(1);
//        }
//
        return ;
    }
    else
    {
        elog(LOG,"error recovery_target_timestamp is not provided.");
        exit(1);
    }
}

XLogRecPtr
GetMinReplicationRequiredLocation()
{
    int i = 0;
    GTM_StandbyReplication  *replication = NULL;
    XLogRecPtr  ret = InvalidXLogRecPtr;
    XLogRecPtr  tmp = InvalidXLogRecPtr;

    for (i = 0; i < max_wal_sender; i++)
    {
        replication = g_StandbyReplication + i;

        GTM_MutexLockAcquire(&replication->lock);
        if(replication->is_use)
        {
            tmp = GetReplicationSendRequestPtr(replication);

            if(ret == InvalidXLogRecPtr || tmp < ret)
                ret = tmp;
        }
        GTM_MutexLockRelease(&replication->lock);
    }

    return ret;
}

/*
 * Check whether it's a sync standby.
 */
bool
IsInSyncStandbyList(const char *application_name)
{
    gtm_ListCell *cell = NULL;

    if(enalbe_gtm_xlog_debug)
		elog(LOG,"IsInSyncStandbyList %s",application_name);

	GTM_MutexLockAcquire(&SyncConfig->lck);

    gtm_foreach(cell,SyncConfig->sync_application_targets)
    {
        if(enalbe_gtm_xlog_debug)
            elog(LOG,"IsInSyncStandbyList target %s",(char *)gtm_lfirst(cell));

        if(strcmp((char *)gtm_lfirst(cell),application_name) == 0)
        {
        	
			GTM_MutexLockRelease(&SyncConfig->lck);
            return true;
        }
    }

	GTM_MutexLockRelease(&SyncConfig->lck);
    return false;
}

bool
CheckSyncStandbyInList(char *application_name)
{
    gtm_ListCell *cell = NULL;
    gtm_foreach(cell,XLogSync->sync_standbys)
    {
        if(strcmp(application_name,((GTM_StandbyReplication *)(gtm_lfirst(cell)))->application_name) == 0 )
        {
		return true;
        }
    }

    return false;
}

/*
 * Register a new sync standby to XLogSync->sync_standbys.
 */
void
RegisterNewSyncStandby(GTM_StandbyReplication *replication)
{
    MemoryContext oldContext;

    oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

    if(CheckSyncStandbyInList(replication->application_name))
    {
        elog(LOG,"Standby %s already exist in sync list",replication->application_name);
	return ;
    }

    XLogSync->sync_standbys = gtm_lappend(XLogSync->sync_standbys,replication);

    SyncReady = (gtm_list_length(XLogSync->sync_standbys) >= SyncConfig->required_sync_num);

    if(enalbe_gtm_xlog_debug)
        elog(LOG,"add SyncReady %d current lenth %d requireed %d",SyncReady,gtm_list_length(XLogSync->sync_standbys), SyncConfig->required_sync_num);

    MemoryContextSwitchTo(oldContext);

    if(enalbe_gtm_xlog_debug)
        elog(LOG,"RegisterNewSyncStandby %s",replication->application_name);
}


/*
 * Remove a sync standby from XLogSync->sync_standbys.
 */
void
RemoveSyncStandby(GTM_StandbyReplication *replication)
{
    MemoryContext oldContext;
    oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

    if(enalbe_gtm_xlog_debug)
        elog(LOG,"RemoveSyncStandby %s",replication->application_name);

    XLogSync->sync_standbys = gtm_list_delete(XLogSync->sync_standbys,replication);
    SyncReady = (gtm_list_length(XLogSync->sync_standbys) >= SyncConfig->required_sync_num);

    if(enalbe_gtm_xlog_debug)
        elog(LOG,"remove SyncReady %d current lenth %d requireed %d",SyncReady,gtm_list_length(XLogSync->sync_standbys), SyncConfig->required_sync_num);

    MemoryContextSwitchTo(oldContext);
}

void 
clear_notify_queue(void)
{
    XLogRecPtr *key    = NULL;
    XLogWaiter *waiter = NULL;
    int         ret    = 0;
	
	while(ret = heap_min(&XLogSync->wait_queue,(void **)&key,(void **)&waiter),ret)
    {
        heap_delmin(&XLogSync->wait_queue,(void **)&key,(void **)&waiter);

        GTM_MutexLockAcquire(&waiter->lock);
        waiter->finished = true;
        GTM_CVSignal(&waiter->cv);
        GTM_MutexLockRelease(&waiter->lock);
    }
}

void 
init_standby_replication(void)
{
    int i = 0;
    g_StandbyReplication = palloc(max_wal_sender * sizeof(GTM_StandbyReplication));
    if(g_StandbyReplication == NULL)
    {
        elog(LOG,"memory insufficient");
        exit(1);
    }

    for(i = 0; i < max_wal_sender ; i++)
        gtm_init_replication_data(g_StandbyReplication + i);
}

void 
init_syncconfig(void)
{
	SyncConfig = (XLogSyncConfig *)malloc(sizeof(XLogSyncConfig));
    if(SyncConfig == NULL)
    {
        elog(LOG,"memory insufficient");
        exit(1);
    }
    SyncConfig->required_sync_num = 0;
    SyncConfig->sync_application_targets = NULL;
    GTM_MutexLockInit(&SyncConfig->lck);
}

void 
init_xlogsync(void)
{
	XLogSync  = (XLogSyncStandby *)malloc(sizeof(XLogSyncStandby));
    if(XLogSync == NULL)
    {
        elog(LOG,"memory insufficient");
        exit(1);
    }
    XLogSync->head_xlog_hints = 0;
    XLogSync->head_ptr = InvalidXLogRecPtr;
    XLogSync->sync_standbys = NULL;
    heap_create(&XLogSync->wait_queue,0,XLogRecPtrCompLess);
    GTM_MutexLockInit(&XLogSync->check_mutex);
    GTM_MutexLockInit(&XLogSync->wait_queue_mutex);
}

void 
clear_syncconfig(void)
{
	gtm_ListCell *lc = NULL;
		
	gtm_foreach(lc, SyncConfig->sync_application_targets)
	{
		free((char *)gtm_lfirst(lc));
	}
	gtm_list_free(SyncConfig->sync_application_targets);
	
	SyncConfig->sync_application_targets = NULL;
	SyncConfig->required_sync_num = 0;

	return ;
}

void 
load_syncconfig(void)
{
    char *sync_names = NULL;
    char *original_sync_names = NULL;
    char *stop = NULL;
    int  sync_standby_num = 0;
    bool any_mode = false;

    MemoryContext oldContext;

	GTM_MutexLockAcquire(&SyncConfig->lck);

	clear_syncconfig();

	if(!enable_sync_commit)
	{
		GTM_MutexLockRelease(&SyncConfig->lck);
		return ;
	}

	oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

	original_sync_names = pstrdup(synchronous_standby_names);
    sync_names = strip(original_sync_names);

    if(start_with_ignore_case(sync_names,"any"))
    {
        any_mode = true;
        sync_names += 3;
		sync_names = strip(sync_names);

        SyncConfig->required_sync_num = strtol(sync_names,&stop,10);

        if(stop == sync_names)
        {
            elog(LOG,"expect a number after any in synchronous_standby_names");
            exit(1);
        }

		sync_names = stop;

		elog(LOG,"sync mode any %d",SyncConfig->required_sync_num);

        sync_names = skip_to_next(sync_names,"("BLANK_CHARACTERS);
    }

    stop = NULL;
    while(sync_names = strtok_r(sync_names,",)"BLANK_CHARACTERS,&stop),sync_names)
    {
        SyncConfig->sync_application_targets = gtm_lappend(SyncConfig->sync_application_targets,(void *)strdup(sync_names));
		elog(LOG,"sync target %s",sync_names);
        sync_standby_num++;
		sync_names = NULL;
    }

    if(!any_mode)
        SyncConfig->required_sync_num = sync_standby_num;
	
    elog(LOG,"sync slave number %d",SyncConfig->required_sync_num);

    if(SyncConfig->required_sync_num == 0)
	enable_sync_commit = false;

    pfree(original_sync_names);
    MemoryContextSwitchTo(oldContext);
	
	GTM_MutexLockRelease(&SyncConfig->lck);
}

void 
load_xlogsync(void)
{
	GTM_StandbyReplication *replication = NULL;
	int i = 0;

	GTM_MutexLockAcquire(&XLogSync->check_mutex);
	GTM_MutexLockAcquire(&XLogSync->wait_queue_mutex);

	for(i = 0; i < max_wal_sender ;i++)
	{
		replication = g_StandbyReplication + i;
		if(!replication->is_use)
			continue;

		GTM_MutexLockAcquire(&replication->lock);

		replication->next_sync_pos = InvalidXLogRecPtr;
		replication->sync_hint = false;

		if(IsInSyncStandbyList(replication->application_name))
		{
			if(!CheckSyncStandbyInList(replication->application_name))
			{
				replication->is_sync = true;
				RegisterNewSyncStandby(replication);
			}
		}
		else
		{
			replication->is_sync = false;
			RemoveSyncStandby(replication);
		}

		GTM_MutexLockRelease(&replication->lock);
	}

	if(!enable_sync_commit)
		clear_notify_queue();

	GTM_MutexLockRelease(&XLogSync->wait_queue_mutex);

	NotifyWaitingQueue();

	SyncReady = (gtm_list_length(XLogSync->sync_standbys) >= SyncConfig->required_sync_num);

	GTM_MutexLockRelease(&XLogSync->check_mutex);
}

void 
init_sync_structures(void)
{
	MemoryContext oldContext;
	
	oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

	init_standby_replication();
	init_syncconfig();
	init_xlogsync();

	MemoryContextSwitchTo(oldContext);
}

void 
load_sync_structures(void)
{
	load_syncconfig();
	load_xlogsync();
}

