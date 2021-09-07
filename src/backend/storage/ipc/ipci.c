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
 * ipci.c
 *      POSTGRES inter-process communication initialization code.
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *      src/backend/storage/ipc/ipci.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/clog.h"
#include "access/commit_ts.h"
#include "access/heapam.h"
#include "access/multixact.h"
#include "access/nbtree.h"
#include "access/subtrans.h"
#include "access/twophase.h"
#include "commands/async.h"
#include "miscadmin.h"
#include "pgstat.h"
#ifdef PGXC
#include "pgxc/nodemgr.h"
#include "postmaster/clustermon.h"
#endif
#include "postmaster/autovacuum.h"
#include "postmaster/clean2pc.h"
#include "postmaster/clustermon.h"
#include "postmaster/bgworker_internals.h"
#include "postmaster/bgwriter.h"
#include "postmaster/postmaster.h"
#include "replication/logicallauncher.h"
#include "replication/slot.h"
#include "replication/walreceiver.h"
#include "replication/walsender.h"
#include "replication/origin.h"
#include "storage/bufmgr.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/pg_shmem.h"
#include "storage/pmsignal.h"
#include "storage/predicate.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"
#include "storage/sinvaladt.h"
#include "storage/spin.h"
#ifdef XCP
#include "pgxc/pgxc.h"
#include "pgxc/squeue.h"
#include "pgxc/pause.h"
#endif
#include "utils/backend_random.h"
#ifdef _MLS_
#include "utils/mls.h"
#endif
#include "utils/snapmgr.h"

#ifdef __AUDIT_FGA__
#include "audit/audit_fga.h"
#endif

#ifdef _MIGRATE_
#include "pgxc/shardmap.h"
#endif
#ifdef __TBASE__
#include "storage/nodelock.h"
#include "commands/vacuum.h"
#include "libpq/auth.h"
#endif

#ifdef __AUDIT__
#include "postmaster/auditlogger.h"
#endif

#ifdef __STORAGE_SCALABLE__
#include "replication/logical_statistic.h"
#endif

shmem_startup_hook_type shmem_startup_hook = NULL;

static Size total_addin_request = 0;
static bool addin_request_allowed = true;


/*
 * RequestAddinShmemSpace
 *        Request that extra shmem space be allocated for use by
 *        a loadable module.
 *
 * This is only useful if called from the _PG_init hook of a library that
 * is loaded into the postmaster via shared_preload_libraries.  Once
 * shared memory has been allocated, calls will be ignored.  (We could
 * raise an error, but it seems better to make it a no-op, so that
 * libraries containing such calls can be reloaded if needed.)
 */
void
RequestAddinShmemSpace(Size size)
{
    if (IsUnderPostmaster || !addin_request_allowed)
        return;                    /* too late */
    total_addin_request = add_size(total_addin_request, size);
}


/*
 * CreateSharedMemoryAndSemaphores
 *        Creates and initializes shared memory and semaphores.
 *
 * This is called by the postmaster or by a standalone backend.
 * It is also called by a backend forked from the postmaster in the
 * EXEC_BACKEND case.  In the latter case, the shared memory segment
 * already exists and has been physically attached to, but we have to
 * initialize pointers in local memory that reference the shared structures,
 * because we didn't inherit the correct pointer values from the postmaster
 * as we do in the fork() scenario.  The easiest way to do that is to run
 * through the same code as before.  (Note that the called routines mostly
 * check IsUnderPostmaster, rather than EXEC_BACKEND, to detect this case.
 * This is a bit code-wasteful and could be cleaned up.)
 *
 * If "makePrivate" is true then we only need private memory, not shared
 * memory.  This is true for a standalone backend, false for a postmaster.
 */
void
CreateSharedMemoryAndSemaphores(bool makePrivate, int port)
{// #lizard forgives
    PGShmemHeader *shim = NULL;

    if (!IsUnderPostmaster)
    {
        PGShmemHeader *seghdr;
        Size        size;
        int            numSemas;

        /* Compute number of semaphores we'll need */
        numSemas = ProcGlobalSemas();
        numSemas += SpinlockSemas();

        /*
         * Size of the Postgres shared-memory block is estimated via
         * moderately-accurate estimates for the big hogs, plus 100K for the
         * stuff that's too small to bother with estimating.
         *
         * We take some care during this phase to ensure that the total size
         * request doesn't overflow size_t.  If this gets through, we don't
         * need to be so careful during the actual allocation phase.
         */
        size = 100000;
        size = add_size(size, PGSemaphoreShmemSize(numSemas));
        size = add_size(size, SpinlockSemaSize());
        size = add_size(size, hash_estimate_size(SHMEM_INDEX_SIZE,
                                                 sizeof(ShmemIndexEnt)));
        size = add_size(size, BufferShmemSize());
        size = add_size(size, LockShmemSize());
        size = add_size(size, PredicateLockShmemSize());
        size = add_size(size, ProcGlobalShmemSize());
        size = add_size(size, XLOGShmemSize());
        size = add_size(size, CLOGShmemSize());
        size = add_size(size, CommitTsShmemSize());
#ifdef __TBASE__        
        size = add_size(size, GTSTrackSize());
        size = add_size(size, RecoveryGTMHostSize());
#endif
#ifdef __TBASE_DEBUG__
        size = add_size(size, SnapTableShmemSize());
#endif
        size = add_size(size, SUBTRANSShmemSize());
        size = add_size(size, TwoPhaseShmemSize());
        size = add_size(size, BackgroundWorkerShmemSize());
        size = add_size(size, MultiXactShmemSize());
        size = add_size(size, LWLockShmemSize());
        size = add_size(size, ProcArrayShmemSize());
        size = add_size(size, BackendStatusShmemSize());
        size = add_size(size, SInvalShmemSize());
        size = add_size(size, PMSignalShmemSize());
        size = add_size(size, ProcSignalShmemSize());
        size = add_size(size, CheckpointerShmemSize());
        size = add_size(size, AutoVacuumShmemSize());
        size = add_size(size, ReplicationSlotsShmemSize());
        size = add_size(size, ReplicationOriginShmemSize());
        size = add_size(size, WalSndShmemSize());
        size = add_size(size, WalRcvShmemSize());
		size = add_size(size, Clean2pcShmemSize());
#ifdef XCP
        if (IS_PGXC_DATANODE)
            size = add_size(size, SharedQueueShmemSize());
        if (IS_PGXC_COORDINATOR)
            size = add_size(size, ClusterLockShmemSize());
        size = add_size(size, ClusterMonitorShmemSize());
#endif
        size = add_size(size, ApplyLauncherShmemSize());
        size = add_size(size, SnapMgrShmemSize());
        size = add_size(size, BTreeShmemSize());
        size = add_size(size, SyncScanShmemSize());
        size = add_size(size, AsyncShmemSize());
#ifdef PGXC
        size = add_size(size, NodeTablesShmemSize());
#ifdef __TBASE__
        size = add_size(size, NodeHashTableShmemSize());
#endif
#endif

        size = add_size(size, BackendRandomShmemSize());
#ifdef EXEC_BACKEND
        size = add_size(size, ShmemBackendArraySize());
#endif

#ifdef USE_MODULE_MSGIDS
        size = add_size(size, MsgModuleShmemSize());
#endif

#ifdef __AUDIT_FGA__
        size = add_size(size, AuditFgaShmemSize());
#endif

        /* freeze the addin request size and include it */
        addin_request_allowed = false;
        size = add_size(size, total_addin_request);

        /* might as well round it off to a multiple of a typical page size */
        size = add_size(size, 8192 - (size % 8192));

#ifdef _MIGRATE_
        size = add_size(size, ShardMapShmemSize());
#endif
#ifdef _SHARDING_
        size = add_size(size, ShardBarrierShmemSize());
#endif
#ifdef _MLS_
        size = add_size(size, MlsShmemSize());
#endif
#ifdef __TBASE__
        size = add_size(size, UserAuthShmemSize());
        size = add_size(size, NodeLockShmemSize());
        size = add_size(size, ShardStatisticShmemSize());
        size = add_size(size, QueryAnalyzeInfoShmemSize());
#endif
#ifdef __AUDIT__
        size = add_size(size, AuditLoggerShmemSize());
#endif
#ifdef __STORAGE_SCALABLE__
        size = add_size(size, PubStatDataShmemSize(g_PubStatHashSize, g_PubTableStatHashSize));
        size = add_size(size, SubStatDataShmemSize(g_SubStatHashSize, g_SubTableStatHashSize));
#endif
#ifdef __TWO_PHASE_TRANS__
		size = add_size(size, Record2pcCacheSize());
#endif
#ifdef __COLD_HOT__
        size = add_size(size, DualWriteTableSize()); 
#endif
        elog(DEBUG3, "invoking IpcMemoryCreate(size=%zu)", size);

        /*
         * Create the shmem segment
         */
        seghdr = PGSharedMemoryCreate(size, makePrivate, port, &shim);

        InitShmemAccess(seghdr);

        /*
         * Create semaphores
         */
        PGReserveSemaphores(numSemas, port);

        /*
         * If spinlocks are disabled, initialize emulation layer (which
         * depends on semaphores, so the order is important here).
         */
#ifndef HAVE_SPINLOCKS
        SpinlockSemaInit();
#endif
    }
    else
    {
        /*
         * We are reattaching to an existing shared memory segment. This
         * should only be reached in the EXEC_BACKEND case, and even then only
         * with makePrivate == false.
         */
#ifdef EXEC_BACKEND
        Assert(!makePrivate);
#else
        elog(PANIC, "should be attached to shared memory already");
#endif
    }

    /*
     * Set up shared memory allocation mechanism
     */
    if (!IsUnderPostmaster)
        InitShmemAllocation();

    /*
     * Now initialize LWLocks, which do shared memory allocation and are
     * needed for InitShmemIndex.
     */
    CreateLWLocks();

    /*
     * Set up shmem.c index hashtable
     */
    InitShmemIndex();

    /*
     * Set up xlog, clog, and buffers
     */
    XLOGShmemInit();
    CLOGShmemInit();
    CommitTsShmemInit();
#ifdef __TBASE__
    GTSTrackInit();
    RecoveryGTMHostInit();
#endif

#ifdef __TBASE_DEBUG__
    InitSnapBufTable();
#endif
    SUBTRANSShmemInit();
    MultiXactShmemInit();
    InitBufferPool();

    /*
     * Set up lock manager
     */
    InitLocks();

    /*
     * Set up predicate lock manager
     */
    InitPredicateLocks();

    /*
     * Set up process table
     */
    if (!IsUnderPostmaster)
        InitProcGlobal();
    CreateSharedProcArray();
    CreateSharedBackendStatus();
    TwoPhaseShmemInit();
    BackgroundWorkerShmemInit();

    /*
     * Set up shared-inval messaging
     */
    CreateSharedInvalidationState();

    /*
     * Set up interprocess signaling mechanisms
     */
    PMSignalShmemInit();
    ProcSignalShmemInit();
    CheckpointerShmemInit();
    AutoVacuumShmemInit();
    ReplicationSlotsShmemInit();
    ReplicationOriginShmemInit();
    WalSndShmemInit();
    WalRcvShmemInit();
    ApplyLauncherShmemInit();

	Clean2pcShmemInit();

#ifdef XCP
    /*
     * Set up distributed executor's shared queues
     */
    if (IS_PGXC_DATANODE)
        SharedQueuesInit();
    if (IS_PGXC_COORDINATOR)
        ClusterLockShmemInit();
    ClusterMonitorShmemInit();
#endif

    /*
     * Set up other modules that need some shared memory space
     */
    SnapMgrInit();
    BTreeShmemInit();
    SyncScanShmemInit();
    AsyncShmemInit();
    BackendRandomShmemInit();

#ifdef __AUDIT_FGA__
    AuditFgaShmemInit();
#endif    

#ifdef PGXC
    NodeTablesShmemInit();
#endif

#ifdef _MIGRATE_
    if (IS_PGXC_COORDINATOR)
    {
        ShardMapShmemInit_CN();
    }
    else if (IS_PGXC_DATANODE)
    {
        ShardMapShmemInit_DN();
    }    
#endif

#ifdef _SHARDING_
    ShardBarrierShmemInit();
#endif

#ifdef __TBASE__
    NodeLockShmemInit();
    ShardStatisticShmemInit();
    QueryAnalyzeInfoInit();
    UserAuthShmemInit();
#endif

#ifdef _MLS_
    MlsShmemInit();
#endif


#ifdef __AUDIT__
    AuditLoggerShmemInit();
#endif

#ifdef __STORAGE_SCALABLE__
    InitPubStatData(g_PubStatHashSize, g_PubTableStatHashSize);
    InitSubStatData(g_SubStatHashSize, g_SubTableStatHashSize);
#endif

#ifdef __TWO_PHASE_TRANS__
	Record2pcCacheInit();
#endif

#ifdef __COLD_HOT__
    DualWriteCtlInit();
#endif

#ifdef EXEC_BACKEND

    /*
     * Alloc the win32 shared backend array
     */
    if (!IsUnderPostmaster)
        ShmemBackendArrayAllocation();
#endif

#ifdef USE_MODULE_MSGIDS
    MsgModuleShmemInit();
#endif

    /* Initialize dynamic shared memory facilities. */
    if (!IsUnderPostmaster)
        dsm_postmaster_startup(shim);

    /*
     * Now give loadable modules a chance to set up their shmem allocations
     */
    if (shmem_startup_hook)
        shmem_startup_hook();
}
