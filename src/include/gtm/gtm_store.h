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
 * gtm_store.h
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * Portions Copyright (c) 2012-2018 TBase Development Group
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#ifndef _GTM_STORE_H
#define _GTM_STORE_H
#include "gtm/libpq-be.h"
#include "gtm/gtm_c.h"
#include "gtm/gtm_gxid.h"
#include "gtm/gtm_lock.h"
#include "gtm/gtm_list.h"
#include "gtm/stringinfo.h"


#define GTM_MAX_TIMER_ENTRY_NUMBER  128
#define GTM_TIMER_NAP               1 /* 1 second. */
typedef enum
{
    GTM_TIMER_TYPE_ONCE = 0,
    GTM_TIMER_TYPE_LOOP = 1,
    GTM_TIMER_TYPE_BUTTY
}GTM_TIMER_TYPE;
typedef struct
{
    GTM_TIMER_TYPE time_type;
    time_t         start_time; /* start time of the timer. */
    time_t         interval;   /* interval of the timer. */
    
    void          *param;
    void          *(* timer_routine)(void*);
    bool           bactive;      /* active or not. */
    bool           balloced;  /* alloced or not. */
}GTM_TimerEntry;


#define  LOCK_STORE_CRASH_HANDL_TIMEOUT 30    /* 30 second */

#ifdef __XLOG__
extern void  GTM_StoreSizeInit(void);
#endif
extern int32 GTM_StoreMasterInit(char *data_dir);
extern int32 GTM_StoreShutDown(void);

extern int32 GTM_StoreReserveXid(int32 count);
extern int32 GTM_StoreGlobalTimestamp(GlobalTimestamp gts);
extern int32 GTM_StoreReserveSeqValue(GTMStorageHandle handle, int32 value);

extern GTMStorageHandle GTM_StoreSeqCreate(GTM_SeqInfo *raw_seq, char *gid);
extern GTMStorageHandle GTM_StoreLoadSeq(GTM_SeqInfo *raw_seq);

extern int32 GTM_StoreMarkSeqCalled(GTMStorageHandle seq_handle);
extern int32 GTM_StoreResetSeq(GTMStorageHandle seq_handle);
extern int32 GTM_StoreCloseSeq(GTMStorageHandle seq_handle);
extern int32 GTM_StoreDropSeq(GTMStorageHandle seq_handle);
extern int32 GTM_StoreRestore(GlobalTimestamp *gts, GlobalTransactionId *gxid, GlobalTransactionId *global_xmin);
extern int32 GTM_StoreSeqAlter(GTM_SeqInfo  *raw_seq, GTMStorageHandle  seq_handle, bool restart);
extern int32 GTM_StoreSetSeqValue(GTMStorageHandle seq_handle, GTM_Sequence value, bool is_called);
extern int32 GTM_StoreSeqRename(GTMStorageHandle seq_handle, char  *pre_key, char  *cur_key, int32 pre_key_type, int32 cur_key_type);
extern int32 GTM_StoreSyncSeqValue(GTMStorageHandle seq_handle, GTM_Sequence value);
extern int32 GTM_StoreSetSeqReserve(GTMStorageHandle seq_handle, bool reserve);
extern int32 GTM_CommitSyncSeq(GTMStorageHandle handle);
extern int32 GTM_StoreDropAllSeqInDatabase(GTM_SequenceKey seq_database_key);


extern int32 GTM_StoreBeginPrepareTxn(char *gid, char *node_string);
extern int32 GTM_StoreLogTransaction(GlobalTransactionId gxid,
                                        const char *gid, 
                                        const char *node_string, 
                                        int node_count, 
                                        int isGlobal, 
                                        int isCommit, 
                                        GlobalTimestamp prepare_ts, 
                                        GlobalTimestamp commit_ts);
extern int32 GTM_StoreLogScan(GlobalTransactionId gxid,
                                 const char *nodestring,
                                GlobalTimestamp start_ts,
                                GlobalTimestamp local_start_ts,
                                GlobalTimestamp local_complete_ts,
                                int scan_type,
                                 const char *rel_name,
                                 int64 scan_number);
extern GTMStorageHandle GTM_StoreGetPreparedTxnInfo(char *gid, GlobalTransactionId *gxid, char **nodestring);
extern int32 GTM_StoreCommitTxn(char *gid);
extern int32 GTM_StoreAbortTxn(char *gid);
extern GTMStorageHandle GTM_StoreAllocTxn(char *gid);
extern int32 GTM_StoreStandbyInit(char *data_dir, char *data, uint32 length);
extern int32 GTM_StoreFinishTxn(char *gid);

extern void ProcessStorageTransferCommand(Port *myport, StringInfo message);
extern void ProcessGetGTMHeaderCommand(Port *myport, StringInfo message);
extern void ProcessListStorageSequenceCommand(Port *myport, StringInfo message);
extern void ProcessListStorageTransactionCommand(Port *myport, StringInfo message);
extern void ProcessCheckStorageSequenceCommand(Port *myport, StringInfo message);
extern void ProcessCheckStorageTransactionCommand(Port *myport, StringInfo message);
extern bool GTM_StoreLock(void);
extern bool GTM_StoreUnLock(void);

extern int  GTM_DeactiveTimer(GTM_TimerHandle handle);
extern int  GTM_ActiveTimer(GTM_TimerHandle handle);
extern int  GTM_RemoveTimer(GTM_TimerHandle handle);
extern GTM_TimerHandle GTM_AddTimer(void *(* func)(void*), GTM_TIMER_TYPE type, time_t interval, void *para);
extern void *LockStoreStandbyCrashHandler(void *param);
extern bool GTM_StoreLockStatus(void);
extern bool GTM_StoreGetSysInfo(int64 *identifier, int64 *lsn, GlobalTimestamp *gts);
extern void GTM_PrintControlHeader(void);
extern GTMStorageHandle *GTM_StoreGetAllSeqInDatabase(GTM_SequenceKey seq_database_key, int32 *number);
extern void GTM_StoreGetSeqKey(GTMStorageHandle handle, char *key);
extern void GTM_StoreGetSeqCreateInfo(GTMStorageHandle handle, GTM_SeqCreateInfo *seq_info);
#endif
