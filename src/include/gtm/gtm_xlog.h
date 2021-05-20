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
 * gtm_xlog.h
 *
 *
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/gtm/gtm_xlog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GTM_XLOG_H
#define GTM_XLOG_H

#include "gtm/gtm.h"
#include "gtm/gtm_c.h"
#include "gtm/gtm_lock.h"
#include "gtm/gtm_checkpoint.h"
#include "gtm/heap.h"
#include "access/xlogdefs.h"
#include "gtm/gtm_xlog_internal.h"

#define NUM_XLOGINSERT_LOCKS  8

#define XLOG_KEEP_ALIVE_TIME 10
typedef uint64 offset_t;

typedef struct XLogCtlInsert
{    
    s_lock_t    insertpos_lck;           /* protects CurrBytePos and PrevBytePos */     
    uint64      CurrBytePos;             /* usable bytes which exclude page header */    
    uint64      PrevBytePos;    
    char        pad[PG_CACHE_LINE_SIZE]; /* improve preformance in cpu cache line */
} XLogCtlInsert;

typedef struct XLogInsertLock
{
    GTM_MutexLock    l;
    s_lock_t         m;                /* protect start, end */
    XLogRecPtr       start;
    XLogRecPtr       end;
    char             pad[PG_CACHE_LINE_SIZE];
} XLogInsertLock;

typedef struct XLogwrtResult
{
    XLogRecPtr    Write;            /* last byte + 1 written out */
    XLogRecPtr    Flush;            /* last byte + 1 flushed */
} XLogwrtResult;

enum XLogSendResult
{
    Send_OK               = 1,
    Send_No_data          = 0,
    Send_Data_Not_Found   = -1,
    Send_Error                = -2,
    Send_XlogFile_Not_Found   = -3
};

typedef struct XLogCtlData
{
    XLogCtlInsert  Insert;

    char           writerBuff[XLOG_SEG_SIZE];

    XLogInsertLock insert_lck[NUM_XLOGINSERT_LOCKS];
    
    GTM_RWLock     segment_lck;
    uint64         currentSegment;

    s_lock_t         segment_gts_lck;
    GlobalTimestamp  segment_max_gts;
    pg_time_t        segment_max_timestamp;

    GTM_MutexLock  walwrite_lck;
    uint64         last_write_idx;
    
    s_lock_t       walwirte_info_lck;
    XLogwrtResult  LogwrtResult;

    int            xlog_fd;

    s_lock_t       timeline_lck;
    TimeLineID     thisTimeLineID;

    /* standby mode */
    GTM_RWLock     standby_info_lck;
    XLogRecPtr     write_buff_pos;   /* how far we write xlog in buff */
    XLogRecPtr     apply;            /* how far we redo xlog */
    
} XLogCtlData;

typedef struct XLogSyncConfig {
	GTM_MutexLock    lck;
    volatile sig_atomic_t  required_sync_num;
    gtm_List *sync_application_targets;
} XLogSyncConfig;

typedef struct XLogSyncStandby
{
    gtm_List *sync_standbys; /* alive standbys which means they are in walsenders */

    GTM_MutexLock check_mutex;
    int           head_xlog_hints;
    XLogRecPtr    head_ptr;   /* temp of the head of wait_queue , not always correct */
	XLogRecPtr    synced_lsn;

    GTM_MutexLock wait_queue_mutex;
    heap wait_queue;
} XLogSyncStandby;

extern XLogCtlData     *XLogCtl;
extern ControlFileData *ControlData;
extern ssize_t          g_GTMControlDataSize;
extern GTM_RWLock       ControlDataLock;
extern XLogSyncStandby  *XLogSync;
extern XLogSyncConfig   *SyncConfig;
extern volatile bool  SyncReady;

extern XLogRecPtr GetStandbyWriteBuffPos(void);
extern XLogRecPtr GetStandbyApplyPos(void);

extern void UpdateStandbyWriteBuffPos(XLogRecPtr pos);
extern void UpdateStandbyApplyPos(XLogRecPtr pos);

extern void WaitXLogWriteUntil(XLogRecPtr write_pos);

XLogRecPtr GetXLogFlushRecPtr(void);

extern bool  CopyXLogRecordToBuff(char *data,XLogRecPtr start,XLogRecPtr end,uint64 size);
extern void  GTM_ThreadWalRedoer_Internal();

/*
 * Request xlog status
 */
extern bool GTM_SendGetReplicationStatusRequest(GTM_Conn *conn);

extern int GTM_GetReplicationResultIfAny(GTM_StandbyReplication *replication,Port *port);

extern bool SendXLogContext(GTM_StandbyReplication *replication,Port *port);

extern void ProcessStartReplicationCommand(Port *myport, StringInfo message);

extern bool GTM_HasXLogToSend(GTM_StandbyReplication *replication);

extern bool GTM_GetReplicationXLogData();

extern XLogwrtResult GetCurrentXLogwrtResult(void);

/*
 * Xlog Init Function.
 */
extern int  GTM_ControlDataInit(void);
extern void GTM_XLogRecovery(XLogRecPtr startPos,char *data_dir);
extern void GTM_XLogCtlDataInit(void);
extern void GTM_XLogFileInit(char *data_dir);
extern void ControlDataSync(bool update_time);
extern void GTM_XLogSyncDataInit(void);

extern TimeLineID GetCurrentTimeLineID(void);

extern void NotifyReplication(XLogRecPtr ptr);
extern void WaitSyncComplete(XLogRecPtr ptr);
extern void CheckSyncReplication(GTM_StandbyReplication *replication,XLogRecPtr ptr);

extern void BeforeReplyToClientXLogTrigger(void);
extern void XLogInsterInit(void);
extern void XLogCtlShutDown(void);

/*
 * Xlog command registers.
 */ 
extern void XLogRegisterRangeOverwrite(offset_t offset,
                                  int32_t len,char *data);
extern void XLogRegisterCheckPoint(void);
extern void XLogRegisterTimeStamp(void);

extern void DoCheckPoint(bool shutdown);
extern bool XLogBackgroundFlush(void);
/*
 * Xlog insert related command.
 */ 
extern void XLogBeginInsert(void);
extern XLogRecPtr XLogInsert(void);
extern void XLogFlush(XLogRecPtr ptr);

extern void OpenMapperFile(char *data_dir);
extern void CloseMapperFile(void);

extern void DoSlaveCheckPoint(bool  write_check_point);
extern void DoMasterCheckPoint(bool shutdown);

extern uint32 PrintRedoRangeOverwrite(XLogCmdRangerOverWrite *cmd);
extern uint32 PrintRedoCheckPoint(XLogCmdCheckPoint *cmd);
extern uint32 PrintRedoTimestamp(XLogRecGts *cmd);

extern TimeLineID GetCurrentTimeLineID(void);
extern void SetCurrentTimeLineID(TimeLineID timeline);
extern bool XLogInCurrentSegment(XLogRecPtr pos);
extern void SwitchXLogFile(void);
extern void NewXLogFile(XLogSegNo segment_no);
extern XLogSegNo  GetSegmentNo(XLogRecPtr ptr);

extern GTM_StandbyReplication *g_StandbyReplication;

extern GTM_Conn *GTM_ActiveConn;

void GTM_StandbyBaseinit(void);

extern bool gtm_standby_resign_to_walsender(Port *port,const char *node_name,const char *replication_name);
extern GTM_StandbyReplication * register_self_to_standby_replication(void);

extern void gtm_close_replication(GTM_StandbyReplication *replication);

extern void GTM_ThreadWalRedoer_Internal(void);

extern XLogRecPtr GetReplicationFlushPtr(GTM_StandbyReplication *replication);

extern XLogSegNo  GetCurrentSegmentNo(void);
extern bool       IsXLogFileExist(const char *file);

extern void ValidXLogRecoveryCondition(void);
extern XLogRecPtr GetMinReplicationRequiredLocation(void);

extern char* GetFormatedCommandLine(char *buff,int size,const char *data,char *file_name,char *relative_path);

extern bool IsInSyncStandbyList(const char *application_name);
extern void RegisterNewSyncStandby(GTM_StandbyReplication *replication);
extern void RemoveSyncStandby(GTM_StandbyReplication *replication);
extern void load_sync_structures(void);
#endif /* GTM_XLOG_H */

