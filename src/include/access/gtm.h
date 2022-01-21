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
 * gtm.h
 * 
 *      Module interfacing with GTM definitions
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef ACCESS_GTM_H
#define ACCESS_GTM_H

#include "gtm/gtm_c.h"
#ifdef __TBASE__
#include "fmgr.h"
#define GTM_NAME_LEN     SEQ_KEY_MAX_LENGTH
typedef struct
{
    char new[GTM_NAME_LEN];
    char old[GTM_NAME_LEN];
}RenameInfo;

typedef struct
{
    int32 gsk_type;
    char  new[GTM_NAME_LEN];
    char  old[GTM_NAME_LEN];
}DropInfo;

typedef struct
{
    int32 gsk_type;
    char  name[GTM_NAME_LEN];    
}CreateInfo;
#endif

/* Configuration variables */
extern char *GtmHost;
extern int GtmPort;
extern bool gtm_backup_barrier;

extern bool IsXidFromGTM;
extern GlobalTransactionId currentGxid;

#ifdef __TBASE__
extern char *NewGtmHost;
extern int     NewGtmPort;
#endif

extern int reconnect_gtm_retry_times;
extern int reconnect_gtm_retry_interval;

extern bool IsGTMConnected(void);
extern void InitGTM(void);
extern void CloseGTM(void);
extern GTM_Timestamp 
GetGlobalTimestampGTM(void);
extern GlobalTransactionId BeginTranGTM(GTM_Timestamp *timestamp, const char *globalSession);
extern GlobalTransactionId BeginTranAutovacuumGTM(void);
extern int CommitTranGTM(GlobalTransactionId gxid, int waited_xid_count,
        GlobalTransactionId *waited_xids);
extern int RollbackTranGTM(GlobalTransactionId gxid);
extern int StartPreparedTranGTM(GlobalTransactionId gxid,
                                char *gid,
                                char *nodestring);
extern int
LogCommitTranGTM(GlobalTransactionId gxid,
                     const char *gid,
                     const char *nodestring,
                     int  node_count,
                     bool isGlobal,
                     bool isCommit,
                     GlobalTimestamp prepare_timestamp,
                     GlobalTimestamp commit_timestamp);
extern int
LogScanGTM( GlobalTransactionId gxid, 
                              const char *node_string, 
                              GlobalTimestamp     start_ts,
                              GlobalTimestamp     local_start_ts,
                              GlobalTimestamp     local_complete_ts,
                              int    scan_type,
                              const char *rel_name,
                             int64  scan_number);
extern int PrepareTranGTM(GlobalTransactionId gxid);
extern int GetGIDDataGTM(char *gid,
                         GlobalTransactionId *gxid,
                         GlobalTransactionId *prepared_gxid,
                         char **nodestring);
extern int CommitPreparedTranGTM(GlobalTransactionId gxid,
                                 GlobalTransactionId prepared_gxid,
                                 int waited_xid_count,
                                 GlobalTransactionId *waited_xids);

extern GTM_Snapshot GetSnapshotGTM(GlobalTransactionId gxid, bool canbe_grouped);

/* Node registration APIs with GTM */
extern int RegisterGTM(GTM_PGXCNodeType type);
extern int UnregisterGTM(GTM_PGXCNodeType type);

/* Sequence interface APIs with GTM */
extern GTM_Sequence GetCurrentValGTM(char *seqname);
extern GTM_Sequence GetNextValGTM(char *seqname,
                    GTM_Sequence range, GTM_Sequence *rangemax);
extern int SetValGTM(char *seqname, GTM_Sequence nextval, bool iscalled);
extern int CreateSequenceGTM(char *seqname, GTM_Sequence increment, 
        GTM_Sequence minval, GTM_Sequence maxval, GTM_Sequence startval,
        bool cycle);
extern int AlterSequenceGTM(char *seqname, GTM_Sequence increment,
        GTM_Sequence minval, GTM_Sequence maxval, GTM_Sequence startval,
                            GTM_Sequence lastval, bool cycle, bool is_restart);
extern int DropSequenceGTM(char *name, GTM_SequenceKeyType type);
extern int RenameSequenceGTM(char *seqname, const char *newseqname);
extern int CopyDataBaseSequenceGTM(char *src_dbname, char *dest_dbname);
extern void CleanGTMSeq(void);
/* Barrier */
extern int ReportBarrierGTM(const char *barrier_id);
extern int ReportGlobalXmin(GlobalTransactionId gxid,
        GlobalTransactionId *global_xmin,
        GlobalTransactionId *latest_completed_xid);

#ifdef __TBASE__
extern void  RegisterSeqCreate(char *name, int32 type);
extern void  RegisterSeqDrop(char *name, int32 type);
extern void  RegisterRenameSequence(char *new, char *old);
extern void  FinishSeqOp(bool commit);
extern int32 GetGTMCreateSeq(char **create_info);
extern int32 GetGTMDropSeq(char **drop_info);
extern int32 GetGTMRenameSeq(char **rename_info);
extern void  RestoreSeqCreate(CreateInfo *create_info, int32 count);
extern void  RestoreSeqDrop(DropInfo *drop_info_array, int32 count);
extern void  RestoreSeqRename(RenameInfo *rename_info_array, int32 count);
extern int   FinishGIDGTM(char *gid);
extern Datum pg_list_gtm_store(PG_FUNCTION_ARGS);
extern Datum pg_list_storage_sequence(PG_FUNCTION_ARGS);
extern Datum pg_list_storage_transaction(PG_FUNCTION_ARGS);
extern Datum pg_check_storage_sequence(PG_FUNCTION_ARGS);
extern Datum pg_check_storage_transaction(PG_FUNCTION_ARGS);
extern void  CheckGTMConnection(void);
extern int32 RenameDBSequenceGTM(const char *seqname, const char *newseqname);
#endif
#endif /* ACCESS_GTM_H */
