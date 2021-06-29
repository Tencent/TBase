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
 * transam.h
 *      postgres transaction access method support code
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/access/transam.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TRANSAM_H
#define TRANSAM_H

#include "access/xlogdefs.h"
#ifdef PGXC
#include "gtm/gtm_c.h"
#endif

/* ----------------
 *        Special transaction ID values
 *
 * BootstrapTransactionId is the XID for "bootstrap" operations, and
 * FrozenTransactionId is used for very old tuples.  Both should
 * always be considered valid.
 *
 * FirstNormalTransactionId is the first "normal" transaction id.
 * Note: if you need to change it, you must change pg_class.h as well.
 * ----------------
 */
#define InvalidTransactionId        ((TransactionId) 0)
#define BootstrapTransactionId        ((TransactionId) 1)
#define FrozenTransactionId            ((TransactionId) 2)
#define FirstNormalTransactionId    ((TransactionId) 3)
#define MaxTransactionId            ((TransactionId) 0xFFFFFFFF)

/* ----------------
 *        transaction ID manipulation macros
 * ----------------
 */
#define TransactionIdIsValid(xid)        ((xid) != InvalidTransactionId)
#define TransactionIdIsNormal(xid)        ((xid) >= FirstNormalTransactionId)
#define TransactionIdEquals(id1, id2)    ((id1) == (id2))
#define TransactionIdStore(xid, dest)    (*(dest) = (xid))
#define StoreInvalidTransactionId(dest) (*(dest) = InvalidTransactionId)

/* advance a transaction ID variable, handling wraparound correctly */
#define TransactionIdAdvance(dest)    \
    do { \
        (dest)++; \
        if ((dest) < FirstNormalTransactionId) \
            (dest) = FirstNormalTransactionId; \
    } while(0)

/* back up a transaction ID variable, handling wraparound correctly */
#define TransactionIdRetreat(dest)    \
    do { \
        (dest)--; \
    } while ((dest) < FirstNormalTransactionId)

/* compare two XIDs already known to be normal; this is a macro for speed */
#define NormalTransactionIdPrecedes(id1, id2) \
    (AssertMacro(TransactionIdIsNormal(id1) && TransactionIdIsNormal(id2)), \
    (int32) ((id1) - (id2)) < 0)

/* compare two XIDs already known to be normal; this is a macro for speed */
#define NormalTransactionIdFollows(id1, id2) \
    (AssertMacro(TransactionIdIsNormal(id1) && TransactionIdIsNormal(id2)), \
    (int32) ((id1) - (id2)) > 0)

/* ----------
 *        Object ID (OID) zero is InvalidOid.
 *
 *        OIDs 1-9999 are reserved for manual assignment (see the files
 *        in src/include/catalog/).
 *
 *        OIDS 10000-16383 are reserved for assignment during initdb
 *        using the OID generator.  (We start the generator at 10000.)
 *
 *        OIDs beginning at 16384 are assigned from the OID generator
 *        during normal multiuser operation.  (We force the generator up to
 *        16384 as soon as we are in normal operation.)
 *
 * The choices of 10000 and 16384 are completely arbitrary, and can be moved
 * if we run low on OIDs in either category.  Changing the macros below
 * should be sufficient to do this.
 *
 * NOTE: if the OID generator wraps around, we skip over OIDs 0-16383
 * and resume with 16384.  This minimizes the odds of OID conflict, by not
 * reassigning OIDs that might have been assigned during initdb.
 * ----------
 */
#define FirstBootstrapObjectId    10000
#define FirstNormalObjectId        16384

/*
 * VariableCache is a data structure in shared memory that is used to track
 * OID and XID assignment state.  For largely historical reasons, there is
 * just one struct with different fields that are protected by different
 * LWLocks.
 *
 * Note: xidWrapLimit and oldestXidDB are not "active" values, but are
 * used just to generate useful messages when xidWarnLimit or xidStopLimit
 * are exceeded.
 */
typedef struct VariableCacheData
{
    /*
     * These fields are protected by OidGenLock.
     */
    Oid            nextOid;        /* next OID to assign */
    uint32        oidCount;        /* OIDs available before must do XLOG work */

    /*
     * These fields are protected by XidGenLock.
     */
    TransactionId nextXid;        /* next XID to assign */

    TransactionId oldestXid;    /* cluster-wide minimum datfrozenxid */
    TransactionId xidVacLimit;    /* start forcing autovacuums here */
    TransactionId xidWarnLimit; /* start complaining here */
    TransactionId xidStopLimit; /* refuse to advance nextXid beyond here */
    TransactionId xidWrapLimit; /* where the world ends */
    Oid            oldestXidDB;    /* database with minimum datfrozenxid */

    /*
     * These fields are protected by CommitTsLock
     */
    TransactionId oldestCommitTsXid;
    TransactionId newestCommitTsXid;

    /*
     * These fields are protected by ProcArrayLock.
     */
    TransactionId latestCompletedXid;    /* newest XID that has committed or
                                         * aborted */
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
    GlobalTimestamp latestCommitTs;
    GlobalTimestamp latestGTS;            /* latest gts of current node. */
#endif

    /*
     * These fields are protected by CLogTruncationLock
     */
    TransactionId oldestClogXid;    /* oldest it's safe to look up in clog */

} VariableCacheData;

typedef VariableCacheData *VariableCache;


/* ----------------
 *        extern declarations
 * ----------------
 */

/* in transam/xact.c */
extern bool TransactionStartedDuringRecovery(void);

/* in transam/varsup.c */
extern PGDLLIMPORT VariableCache ShmemVariableCache;

/*
 * prototypes for functions in transam/transam.c
 */
extern bool TransactionIdDidCommit(TransactionId transactionId);
extern bool TransactionIdDidAbort(TransactionId transactionId);
extern bool TransactionIdIsKnownCompleted(TransactionId transactionId);
extern void TransactionIdAbort(TransactionId transactionId);
extern void TransactionIdCommitTree(TransactionId xid, int nxids, TransactionId *xids);
extern void TransactionIdAsyncCommitTree(TransactionId xid, int nxids, TransactionId *xids, XLogRecPtr lsn);
extern void TransactionIdAbortTree(TransactionId xid, int nxids, TransactionId *xids);
extern bool TransactionIdPrecedes(TransactionId id1, TransactionId id2);
extern bool TransactionIdPrecedesOrEquals(TransactionId id1, TransactionId id2);
extern bool TransactionIdFollows(TransactionId id1, TransactionId id2);
extern bool TransactionIdFollowsOrEquals(TransactionId id1, TransactionId id2);
extern TransactionId TransactionIdLatest(TransactionId mainxid,
                    int nxids, const TransactionId *xids);
extern XLogRecPtr TransactionIdGetCommitLSN(TransactionId xid);

/* in transam/varsup.c */
#ifdef PGXC  /* PGXC_DATANODE */
extern void SetNextTransactionId(TransactionId xid);
extern void SetForceXidFromGTM(bool value);
extern bool GetForceXidFromGTM(void);
#ifdef __USE_GLOBAL_SNAPSHOT__
extern TransactionId GetNewTransactionId(bool isSubXact, bool *timestamp_received, GTM_Timestamp *timestamp);
#else
extern TransactionId GetNewTransactionId(bool isSubXact);
extern void SetLocalTransactionId(TransactionId xid);
extern void StoreGlobalXid(const char *globalXid);
extern void StoreLocalGlobalXid(const char *globalXid);

#ifdef __TWO_PHASE_TRANS__
extern void StoreStartNode(const char *startnode);
extern void StorePartNodes(const char *partNodes);
extern void StoreStartXid(TransactionId transactionid);
#endif

#endif

#else
extern TransactionId GetNewTransactionId(bool isSubXact);
#endif /* PGXC */
#ifdef XCP
extern bool TransactIdIsCurentGlobalTransacId(TransactionId xid);
extern TransactionId GetNextTransactionId(void);
extern void ExtendLogs(TransactionId xid);
extern int GetNumSubTransactions(void);
extern TransactionId *GetSubTransactions(void);
#endif
extern TransactionId ReadNewTransactionId(void);
extern void SetTransactionIdLimit(TransactionId oldest_datfrozenxid,
                      Oid oldest_datoid);
extern void AdvanceOldestClogXid(TransactionId oldest_datfrozenxid);
extern bool ForceTransactionIdLimitUpdate(void);
extern Oid    GetNewObjectId(void);
extern bool is_distri_report;
/* GUC parameter */
extern bool enable_distri_print;
extern bool enable_distri_debug;
extern bool enable_distri_visibility_print;
extern int  delay_before_acquire_committs;
extern int  delay_after_acquire_committs;


#endif                            /* TRAMSAM_H */
