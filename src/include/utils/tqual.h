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
 * tqual.h
 *      POSTGRES "time qualification" definitions, ie, tuple visibility rules.
 *
 *      Should be moved/renamed...    - vadim 07/28/98
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/tqual.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TQUAL_H
#define TQUAL_H

#include "utils/snapshot.h"
#include "access/xlogdefs.h"


/* Static variables representing various special snapshot semantics */
extern PGDLLIMPORT SnapshotData SnapshotSelfData;
extern PGDLLIMPORT SnapshotData SnapshotAnyData;
extern PGDLLIMPORT SnapshotData CatalogSnapshotData;

#define SnapshotSelf        (&SnapshotSelfData)
#define SnapshotAny            (&SnapshotAnyData)
/*
#ifdef _MIGRATE_
#define SnapshotNow            (&SnapshotNowData)
#endif
*/

#ifdef _MIGRATE_
#define     SHARD_VISIBLE_MODE_VISIBLE     0
#define        SHARD_VISIBLE_MODE_HIDDEN   1
#define        SHARD_VISIBLE_MODE_ALL    2
#endif




/* This macro encodes the knowledge of which snapshots are MVCC-safe */
#define IsMVCCSnapshot(snapshot)  \
    ((snapshot)->satisfies == HeapTupleSatisfiesMVCC || \
     (snapshot)->satisfies == HeapTupleSatisfiesHistoricMVCC)

/*
 * HeapTupleSatisfiesVisibility
 *        True iff heap tuple satisfies a time qual.
 *
 * Notes:
 *    Assumes heap tuple is valid.
 *    Beware of multiple evaluations of snapshot argument.
 *    Hint bits in the HeapTuple's t_infomask may be updated as a side effect;
 *    if so, the indicated buffer is marked dirty.
 */
#define HeapTupleSatisfiesVisibility(tuple, snapshot, buffer) \
    ((*(snapshot)->satisfies) (tuple, snapshot, buffer))

/* Result codes for HeapTupleSatisfiesVacuum */
typedef enum
{
    HEAPTUPLE_DEAD,                /* tuple is dead and deletable */
    HEAPTUPLE_LIVE,                /* tuple is live (committed, no deleter) */
    HEAPTUPLE_RECENTLY_DEAD,    /* tuple is dead, but not deletable yet */
    HEAPTUPLE_INSERT_IN_PROGRESS,    /* inserting xact is still in progress */
    HEAPTUPLE_DELETE_IN_PROGRESS    /* deleting xact is still in progress */
} HTSV_Result;

/* These are the "satisfies" test routines for the various snapshot types */
extern bool HeapTupleSatisfiesMVCC(HeapTuple htup,
                       Snapshot snapshot, Buffer buffer);
extern bool HeapTupleSatisfiesSelf(HeapTuple htup,
                       Snapshot snapshot, Buffer buffer);
extern bool HeapTupleSatisfiesAny(HeapTuple htup,
                      Snapshot snapshot, Buffer buffer);
extern bool HeapTupleSatisfiesToast(HeapTuple htup,
                        Snapshot snapshot, Buffer buffer);
extern bool HeapTupleSatisfiesDirty(HeapTuple htup,
                        Snapshot snapshot, Buffer buffer);
extern bool HeapTupleSatisfiesHistoricMVCC(HeapTuple htup,
                               Snapshot snapshot, Buffer buffer);

#ifdef __STORAGE_SCALABLE__
extern bool HeapTupleSatisfiesUnshard(HeapTuple htup, Snapshot snapshot, Buffer buffer);
#endif

/* Special "satisfies" routines with different APIs */
extern HTSU_Result HeapTupleSatisfiesUpdate(HeapTuple htup,
                         CommandId curcid, Buffer buffer);
extern HTSV_Result HeapTupleSatisfiesVacuum(HeapTuple htup,
                         TransactionId OldestXmin, Buffer buffer);
extern bool HeapTupleIsSurelyDead(HeapTuple htup,
                      TransactionId OldestXmin);

extern void HeapTupleSetHintBits(HeapTupleHeader tuple, Buffer buffer,
                     uint16 infomask, TransactionId xid);
extern bool HeapTupleHeaderIsOnlyLocked(HeapTupleHeader tuple);
/*
#ifdef _MIGRATE_
extern bool HeapTupleSatisfiesNow(HeapTupleHeader tuple,
                      Snapshot snapshot, Buffer buffer);

#endif
*/
/*
 * To avoid leaking too much knowledge about reorderbuffer implementation
 * details this is implemented in reorderbuffer.c not tqual.c.
 */
struct HTAB;
extern bool ResolveCminCmaxDuringDecoding(struct HTAB *tuplecid_data,
                              Snapshot snapshot,
                              HeapTuple htup,
                              Buffer buffer,
                              CommandId *cmin, CommandId *cmax);

/*
 * We don't provide a static SnapshotDirty variable because it would be
 * non-reentrant.  Instead, users of that snapshot type should declare a
 * local variable of type SnapshotData, and initialize it with this macro.
 */
#define InitDirtySnapshot(snapshotdata)  \
    ((snapshotdata).satisfies = HeapTupleSatisfiesDirty)

/*
 * Similarly, some initialization is required for SnapshotToast.  We need
 * to set lsn and whenTaken correctly to support snapshot_too_old.
 */
#define InitToastSnapshot(snapshotdata, l, w)  \
    ((snapshotdata).satisfies = HeapTupleSatisfiesToast, \
     (snapshotdata).lsn = (l),                    \
     (snapshotdata).whenTaken = (w))

#endif                            /* TQUAL_H */
