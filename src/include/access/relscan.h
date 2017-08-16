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
 * relscan.h
 *      POSTGRES relation scan descriptor definitions.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/relscan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RELSCAN_H
#define RELSCAN_H

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/itup.h"
#include "access/tupdesc.h"
#include "storage/spin.h"
#include "access/gtm.h"


/*
 * Shared state for parallel heap scan.
 *
 * Each backend participating in a parallel heap scan has its own
 * HeapScanDesc in backend-private memory, and those objects all contain
 * a pointer to this structure.  The information here must be sufficient
 * to properly initialize each new HeapScanDesc as workers join the scan,
 * and it must act as a font of block numbers for those workers.
 */
typedef struct ParallelHeapScanDescData
{
    Oid            phs_relid;        /* OID of relation to scan */
    bool        phs_syncscan;    /* report location to syncscan logic? */
    BlockNumber phs_nblocks;    /* # blocks in relation at start of scan */
	slock_t		phs_mutex;		/* mutual exclusion for setting startblock */
    BlockNumber phs_startblock; /* starting block number */
	pg_atomic_uint64 phs_nallocated;	/* number of blocks allocated to
										 * workers so far. */
    char        phs_snapshot_data[FLEXIBLE_ARRAY_MEMBER];
}            ParallelHeapScanDescData;

typedef struct HeapScanDescData
{
    /* scan parameters */
    Relation    rs_rd;            /* heap relation descriptor */
    Snapshot    rs_snapshot;    /* snapshot to see */
    int            rs_nkeys;        /* number of scan keys */
    ScanKey        rs_key;            /* array of scan key descriptors */
    bool        rs_bitmapscan;    /* true if this is really a bitmap scan */
    bool        rs_samplescan;    /* true if this is really a sample scan */
    bool        rs_pageatatime; /* verify visibility page-at-a-time? */
    bool        rs_allow_strat; /* allow or disallow use of access strategy */
    bool        rs_allow_sync;    /* allow or disallow use of syncscan */
    bool        rs_temp_snap;    /* unregister snapshot at scan end? */

    /* state set up at initscan time */
    BlockNumber rs_nblocks;        /* total number of blocks in rel */
    BlockNumber rs_startblock;    /* block # to start at */
    BlockNumber rs_numblocks;    /* max number of blocks to scan */
    /* rs_numblocks is usually InvalidBlockNumber, meaning "scan whole rel" */
    BufferAccessStrategy rs_strategy;    /* access strategy for reads */
    bool        rs_syncscan;    /* report location to syncscan logic? */

    /* scan current state */
    bool        rs_inited;        /* false = scan not init'd yet */

    HeapTupleData rs_ctup;        /* current tuple in scan, if any */
    BlockNumber rs_cblock;        /* current block # in scan, if any */
    Buffer        rs_cbuf;        /* current buffer in scan, if any */
    /* NB: if rs_cbuf is not InvalidBuffer, we hold a pin on that buffer */
    ParallelHeapScanDesc rs_parallel;    /* parallel scan information */

#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
    /* statistic account */
    int64        rs_scan_number;            /* scanned number of tuples */
    int64        rs_valid_number;         /* number of tuples validated by HeapTupleSatisfiesMVCC */
    int64        rs_invalid_number;      /* number of tuples not validated by HeapTupleSatisfiesMVCC */
    GlobalTimestamp rs_scan_start_timestamp; /* start timestamp on local node */
#endif
    /* these fields only used in page-at-a-time mode and for bitmap scans */
    int            rs_cindex;        /* current tuple's index in vistuples */
    int            rs_ntuples;        /* number of visible tuples on page */
    OffsetNumber rs_vistuples[MaxHeapTuplesPerPage];    /* their offsets */
}            HeapScanDescData;

/*
 * We use the same IndexScanDescData structure for both amgettuple-based
 * and amgetbitmap-based index scans.  Some fields are only relevant in
 * amgettuple-based scans.
 */
typedef struct IndexScanDescData
{
    /* scan parameters */
    Relation    heapRelation;    /* heap relation descriptor, or NULL */
    Relation    indexRelation;    /* index relation descriptor */
    Snapshot    xs_snapshot;    /* snapshot to see */
    int            numberOfKeys;    /* number of index qualifier conditions */
    int            numberOfOrderBys;    /* number of ordering operators */
    ScanKey        keyData;        /* array of index qualifier descriptors */
    ScanKey        orderByData;    /* array of ordering op descriptors */
    bool        xs_want_itup;    /* caller requests index tuples */
    bool        xs_temp_snap;    /* unregister snapshot at scan end? */

    /* signaling to index AM about killing index tuples */
    bool        kill_prior_tuple;    /* last-returned tuple is dead */
    bool        ignore_killed_tuples;    /* do not return killed entries */
    bool        xactStartedInRecovery;    /* prevents killing/seeing killed
                                         * tuples */

    /* index access method's private state */
    void       *opaque;            /* access-method-specific info */

    /*
     * In an index-only scan, a successful amgettuple call must fill either
     * xs_itup (and xs_itupdesc) or xs_hitup (and xs_hitupdesc) to provide the
     * data returned by the scan.  It can fill both, in which case the heap
     * format will be used.
     */
    IndexTuple    xs_itup;        /* index tuple returned by AM */
    TupleDesc    xs_itupdesc;    /* rowtype descriptor of xs_itup */
    HeapTuple    xs_hitup;        /* index data returned by AM, as HeapTuple */
    TupleDesc    xs_hitupdesc;    /* rowtype descriptor of xs_hitup */

    /* xs_ctup/xs_cbuf/xs_recheck are valid after a successful index_getnext */
    HeapTupleData xs_ctup;        /* current heap tuple, if any */
    Buffer        xs_cbuf;        /* current heap buffer in scan, if any */
    /* NB: if xs_cbuf is not InvalidBuffer, we hold a pin on that buffer */
    bool        xs_recheck;        /* T means scan keys must be rechecked */

    /*
     * When fetching with an ordering operator, the values of the ORDER BY
     * expressions of the last returned tuple, according to the index.  If
     * xs_recheckorderby is true, these need to be rechecked just like the
     * scan keys, and the values returned here are a lower-bound on the actual
     * values.
     */
    Datum       *xs_orderbyvals;
    bool       *xs_orderbynulls;
    bool        xs_recheckorderby;

    /* state data for traversing HOT chains in index_getnext */
    bool        xs_continue_hot;    /* T if must keep walking HOT chain */

    
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
    /* statistic account */
    int64        xs_scan_number;         /* scanned number of tuples */
    GlobalTimestamp xs_scan_start_timestamp; /* start timestamp on local node */
#endif

    /* parallel index scan information, in shared memory */
    ParallelIndexScanDesc parallel_scan;
}            IndexScanDescData;

/* Generic structure for parallel scans */
typedef struct ParallelIndexScanDescData
{
    Oid            ps_relid;
    Oid            ps_indexid;
    Size        ps_offset;        /* Offset in bytes of am specific structure */
    char        ps_snapshot_data[FLEXIBLE_ARRAY_MEMBER];
}            ParallelIndexScanDescData;

/* Struct for heap-or-index scans of system tables */
typedef struct SysScanDescData
{
    Relation    heap_rel;        /* catalog being scanned */
    Relation    irel;            /* NULL if doing heap scan */
    HeapScanDesc scan;            /* only valid in heap-scan case */
    IndexScanDesc iscan;        /* only valid in index-scan case */
    Snapshot    snapshot;        /* snapshot to unregister at end of scan */
}            SysScanDescData;

#ifdef __TBASE__
extern bool enable_distri_print;
extern bool enable_distri_debug;
extern bool enable_distri_visibility_print;
typedef enum
{
    SCAN = 0,
    SNAPSHOT_SCAN,
    SNAPSHOT_SCAN_BEFORE_PREPARE,
    SNAPSHOT_SCAN_AFTER_PREPARE,
    SNAPSHOT_SCAN_AFTER_COMMITTED,
    SNAPSHOT_SCAN_AFTER_ABORT,
    SEQ_SCAN,
    BITMAP_SCAN,
    INDEX_SCAN,
    PARALLEL_SEQ_SCAN,
    PARALLEL_BITMAP_SCAN,
    PARALLEL_INDEX_SCAN,
    INSERT_TUPLES
}ScanType;

#endif

#endif                            /* RELSCAN_H */
