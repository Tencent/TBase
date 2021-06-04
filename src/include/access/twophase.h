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
 * twophase.h
 *      Two-phase-commit related declarations.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/twophase.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TWOPHASE_H
#define TWOPHASE_H

#include "access/xlogdefs.h"
#include "datatype/timestamp.h"
#include "storage/lock.h"

#include "gtm/gtm_c.h"

#define GIDSIZE (200 + 24)
/*
 * GlobalTransactionData is defined in twophase.c; other places have no
 * business knowing the internal definition.
 */
typedef struct GlobalTransactionData *GlobalTransaction;

/* GUC variable */
extern int    max_prepared_xacts;

extern int  transaction_threshold;

#ifdef __TBASE__
extern bool enable_2pc_recovery_info;
#endif

#ifdef __TWO_PHASE_TRANS__
extern bool enable_2pc_file_cache;
extern bool enable_2pc_file_check;
extern bool enable_2pc_entry_key_check;
extern bool enable_2pc_entry_trace;

extern int record_2pc_cache_size;
extern int record_2pc_entry_size;
#endif

extern Size TwoPhaseShmemSize(void);
extern void TwoPhaseShmemInit(void);

extern void AtAbort_Twophase(void);
extern void PostPrepare_Twophase(void);

extern PGPROC *TwoPhaseGetDummyProc(TransactionId xid);
extern BackendId TwoPhaseGetDummyBackendId(TransactionId xid);

extern GlobalTransaction MarkAsPreparing(TransactionId xid, const char *gid,
                TimestampTz prepared_at,
                Oid owner, Oid databaseid);
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
extern void EndGlobalPrepare(GlobalTransaction gxact, bool isImplicit);
extern void EndExplicitGlobalPrepare(char *gid);
#endif

extern void EndPrepare(GlobalTransaction gxact);
extern void StartPrepare(GlobalTransaction gxact);
extern bool StandbyTransactionIdIsPrepared(TransactionId xid);

extern TransactionId PrescanPreparedTransactions(TransactionId **xids_p,
                            int *nxids_p);
extern void StandbyRecoverPreparedTransactions(void);
extern void RecoverPreparedTransactions(void);

extern void CheckPointTwoPhase(XLogRecPtr redo_horizon);

extern void FinishPreparedTransaction(const char *gid, bool isCommit);

extern void CheckPreparedTransactionLock(const char *gid);

extern void PrepareRedoAdd(char *buf, XLogRecPtr start_lsn,
               XLogRecPtr end_lsn);
extern void PrepareRedoRemove(TransactionId xid, bool giveWarning);
extern void restoreTwoPhaseData(void);
/* TWOPHASE_H */

#ifdef __TWO_PHASE_TRANS__
extern void record_2pc_redo_remove_gid_xid(TransactionId xid);
extern void record_2pc_involved_nodes_xid(const char * tid, 
                                        char * startnode, 
                                        GlobalTransactionId startxid, 
                                        char * nodestring, 
                                        GlobalTransactionId xid);
extern void record_2pc_commit_timestamp(const char *tid, GlobalTimestamp commit_timestamp);
extern void remove_2pc_records(const char *tid, bool record_in_xlog);
extern void rename_2pc_records(const char *tid, TimestampTz timestamp);
extern void record_2pc_readonly(const char *gid);

extern char *get_2pc_info_from_cache(const char *tid);
extern char *get_2pc_list_from_cache(int *count);

extern void Record2pcCacheInit(void);
extern Size Record2pcCacheSize(void);
#endif

#endif                            /* TWOPHASE_H */
