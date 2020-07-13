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
 * snapshot.h
 *      POSTGRES snapshot definition
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/utils/snapshot.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SNAPSHOT_H
#define SNAPSHOT_H

#include "access/htup.h"
#include "access/xlogdefs.h"
#include "datatype/timestamp.h"
#include "lib/pairingheap.h"
#include "storage/buf.h"
#ifdef _MIGRATE_
#include "nodes/bitmapset.h"
#include "pgxc/shardmap.h"
#endif


typedef struct SnapshotData *Snapshot;

#define InvalidSnapshot        ((Snapshot) NULL)

/*
 * We use SnapshotData structures to represent both "regular" (MVCC)
 * snapshots and "special" snapshots that have non-MVCC semantics.
 * The specific semantics of a snapshot are encoded by the "satisfies"
 * function.
 */
typedef bool (*SnapshotSatisfiesFunc) (HeapTuple htup,
                                       Snapshot snapshot, Buffer buffer);

/*
 * Struct representing all kind of possible snapshots.
 *
 * There are several different kinds of snapshots:
 * * Normal MVCC snapshots
 * * MVCC snapshots taken during recovery (in Hot-Standby mode)
 * * Historic MVCC snapshots used during logical decoding
 * * snapshots passed to HeapTupleSatisfiesDirty()
 * * snapshots used for SatisfiesAny, Toast, Self where no members are
 *     accessed.
 *
 * TODO: It's probably a good idea to split this struct using a NodeTag
 * similar to how parser and executor nodes are handled, with one type for
 * each different kind of snapshot to avoid overloading the meaning of
 * individual fields.
 */
typedef struct SnapshotData
{
    SnapshotSatisfiesFunc satisfies;    /* tuple test function */

    /*
     * The remaining fields are used only for MVCC snapshots, and are normally
     * just zeroes in special snapshots.  (But xmin and xmax are used
     * specially by HeapTupleSatisfiesDirty.)
     *
     * An MVCC snapshot can never see the effects of XIDs >= xmax. It can see
     * the effects of all older XIDs except those listed in the snapshot. xmin
     * is stored as an optimization to avoid needing to search the XID arrays
     * for most tuples.
     */
    TransactionId xmin;            /* all XID < xmin are visible to me */
    TransactionId xmax;            /* all XID >= xmax are invisible to me */

    /*
     * For normal MVCC snapshot this contains the all xact IDs that are in
     * progress, unless the snapshot was taken during recovery in which case
     * it's empty. For historic MVCC snapshots, the meaning is inverted, i.e.
     * it contains *committed* transactions between xmin and xmax.
     *
     * note: all ids in xip[] satisfy xmin <= xip[i] < xmax
     */
    TransactionId *xip;
    uint32        xcnt;            /* # of xact ids in xip[] */
#ifdef PGXC  /* PGXC_COORD */
    uint32        max_xcnt;        /* Max # of xact in xip[] */
#endif

#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
	/*
	 * global timestamp at which the statement/transaction starts
	 */
	GlobalTimestamp start_ts;

	bool			local;		/* local snapshot */

	TransactionId *prepare_xip;
	GlobalTimestamp *prepare_xip_ts;

    uint32        prepare_xcnt;

	TransactionId *prepare_subxip;
	GlobalTimestamp *prepare_subxip_ts;

    uint32        prepare_subxcnt;

    TransactionId prepare_xmin;

    int64        number_visible_tuples;
    int64        scanned_tuples_before_prepare;
    int64        scanned_tuples_after_prepare;
    int64        scanned_tuples_after_committed;
    int64        scanned_tuples_after_abort;
#endif

    /*
     * For non-historic MVCC snapshots, this contains subxact IDs that are in
     * progress (and other transactions that are in progress if taken during
     * recovery). For historic snapshot it contains *all* xids assigned to the
     * replayed transaction, including the toplevel xid.
     *
     * note: all ids in subxip[] are >= xmin, but we don't bother filtering
     * out any that are >= xmax
     */
    TransactionId *subxip;
    int32        subxcnt;        /* # of xact ids in subxip[] */
    bool        suboverflowed;    /* has the subxip array overflowed? */

    bool        takenDuringRecovery;    /* recovery-shaped snapshot? */
    bool        copied;            /* false if it's a static snapshot */

    CommandId    curcid;            /* in my xact, CID < curcid are visible */

    /*
     * An extra return value for HeapTupleSatisfiesDirty, not used in MVCC
     * snapshots.
     */
    uint32        speculativeToken;

    /*
     * Book-keeping information, used by the snapshot manager
     */
    uint32        active_count;    /* refcount on ActiveSnapshot stack */
    uint32        regd_count;        /* refcount on RegisteredSnapshots */
    pairingheap_node ph_node;    /* link in the RegisteredSnapshots heap */

    TimestampTz whenTaken;        /* timestamp when snapshot was taken */
    XLogRecPtr    lsn;            /* position in the WAL stream when taken */
    
#ifdef __TBASE__
    int         groupsize;
    Bitmapset    *shardgroup;
    char        sg_filler[SHARD_TABLE_BITMAP_SIZE];
#endif

    
} SnapshotData;

/*
 * Result codes for HeapTupleSatisfiesUpdate.  This should really be in
 * tqual.h, but we want to avoid including that file elsewhere.
 */
typedef enum
{
    HeapTupleMayBeUpdated,
    HeapTupleInvisible,
    HeapTupleSelfUpdated,
    HeapTupleUpdated,
    HeapTupleBeingUpdated,
    HeapTupleWouldBlock            /* can be returned by heap_tuple_lock */
} HTSU_Result;

#ifdef __TBASE__
#define    SnapshotGetShardTable(snapshot) ((snapshot)->shardgroup)
#endif



#endif                            /* SNAPSHOT_H */
