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
 * procsignal.c
 *      Routines for interprocess signalling
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *      src/backend/storage/ipc/procsignal.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <unistd.h>

#include "access/parallel.h"
#include "commands/async.h"
#include "miscadmin.h"
#include "replication/walsender.h"
#include "storage/latch.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/sinval.h"
#include "tcop/tcopprot.h"
#ifdef PGXC
#include "pgxc/poolutils.h"
#endif
#ifdef __TBASE__
#include "executor/execParallel.h"
#endif

/*
 * The SIGUSR1 signal is multiplexed to support signalling multiple event
 * types. The specific reason is communicated via flags in shared memory.
 * We keep a boolean flag for each possible "reason", so that different
 * reasons can be signaled to a process concurrently.  (However, if the same
 * reason is signaled more than once nearly simultaneously, the process may
 * observe it only once.)
 *
 * Each process that wants to receive signals registers its process ID
 * in the ProcSignalSlots array. The array is indexed by backend ID to make
 * slot allocation simple, and to avoid having to search the array when you
 * know the backend ID of the process you're signalling.  (We do support
 * signalling without backend ID, but it's a bit less efficient.)
 *
 * The flags are actually declared as "volatile sig_atomic_t" for maximum
 * portability.  This should ensure that loads and stores of the flag
 * values are atomic, allowing us to dispense with any explicit locking.
 */
typedef struct
{
    pid_t        pss_pid;
    sig_atomic_t pss_signalFlags[NUM_PROCSIGNALS];
} ProcSignalSlot;

/*
 * We reserve a slot for each possible BackendId, plus one for each
 * possible auxiliary process type.  (This scheme assumes there is not
 * more than one of any auxiliary process type at a time.)
 */
#define NumProcSignalSlots    (MaxBackends + NUM_AUXPROCTYPES)

static ProcSignalSlot *ProcSignalSlots = NULL;
static volatile ProcSignalSlot *MyProcSignalSlot = NULL;

static bool CheckProcSignal(ProcSignalReason reason);
static void CleanupProcSignalState(int status, Datum arg);

/*
 * ProcSignalShmemSize
 *        Compute space needed for procsignal's shared memory
 */
Size
ProcSignalShmemSize(void)
{
    return NumProcSignalSlots * sizeof(ProcSignalSlot);
}

/*
 * ProcSignalShmemInit
 *        Allocate and initialize procsignal's shared memory
 */
void
ProcSignalShmemInit(void)
{
    Size        size = ProcSignalShmemSize();
    bool        found;

    ProcSignalSlots = (ProcSignalSlot *)
        ShmemInitStruct("ProcSignalSlots", size, &found);

    /* If we're first, set everything to zeroes */
    if (!found)
        MemSet(ProcSignalSlots, 0, size);
}

/*
 * ProcSignalInit
 *        Register the current process in the procsignal array
 *
 * The passed index should be my BackendId if the process has one,
 * or MaxBackends + aux process type if not.
 */
void
ProcSignalInit(int pss_idx)
{
    volatile ProcSignalSlot *slot;

    Assert(pss_idx >= 1 && pss_idx <= NumProcSignalSlots);

    slot = &ProcSignalSlots[pss_idx - 1];

    /* sanity check */
    if (slot->pss_pid != 0)
        elog(LOG, "process %d taking over ProcSignal slot %d, but it's not empty",
             MyProcPid, pss_idx);

    /* Clear out any leftover signal reasons */
    MemSet(slot->pss_signalFlags, 0, NUM_PROCSIGNALS * sizeof(sig_atomic_t));

    /* Mark slot with my PID */
    slot->pss_pid = MyProcPid;

    /* Remember slot location for CheckProcSignal */
    MyProcSignalSlot = slot;

    /* Set up to release the slot on process exit */
    on_shmem_exit(CleanupProcSignalState, Int32GetDatum(pss_idx));
}

/*
 * CleanupProcSignalState
 *        Remove current process from ProcSignalSlots
 *
 * This function is called via on_shmem_exit() during backend shutdown.
 */
static void
CleanupProcSignalState(int status, Datum arg)
{
    int            pss_idx = DatumGetInt32(arg);
    volatile ProcSignalSlot *slot;

    slot = &ProcSignalSlots[pss_idx - 1];
    Assert(slot == MyProcSignalSlot);

    /*
     * Clear MyProcSignalSlot, so that a SIGUSR1 received after this point
     * won't try to access it after it's no longer ours (and perhaps even
     * after we've unmapped the shared memory segment).
     */
    MyProcSignalSlot = NULL;

    /* sanity check */
    if (slot->pss_pid != MyProcPid)
    {
        /*
         * don't ERROR here. We're exiting anyway, and don't want to get into
         * infinite loop trying to exit
         */
        elog(LOG, "process %d releasing ProcSignal slot %d, but it contains %d",
             MyProcPid, pss_idx, (int) slot->pss_pid);
        return;                    /* XXX better to zero the slot anyway? */
    }

    slot->pss_pid = 0;
}

/*
 * SendProcSignal
 *        Send a signal to a Postgres process
 *
 * Providing backendId is optional, but it will speed up the operation.
 *
 * On success (a signal was sent), zero is returned.
 * On error, -1 is returned, and errno is set (typically to ESRCH or EPERM).
 *
 * Not to be confused with ProcSendSignal
 */
int
SendProcSignal(pid_t pid, ProcSignalReason reason, BackendId backendId)
{
    volatile ProcSignalSlot *slot;

    if (backendId != InvalidBackendId)
    {
        slot = &ProcSignalSlots[backendId - 1];

        /*
         * Note: Since there's no locking, it's possible that the target
         * process detaches from shared memory and exits right after this
         * test, before we set the flag and send signal. And the signal slot
         * might even be recycled by a new process, so it's remotely possible
         * that we set a flag for a wrong process. That's OK, all the signals
         * are such that no harm is done if they're mistakenly fired.
         */
        if (slot->pss_pid == pid)
        {
            /* Atomically set the proper flag */
            slot->pss_signalFlags[reason] = true;
            /* Send signal */
            return kill(pid, SIGUSR1);
        }
    }
    else
    {
        /*
         * BackendId not provided, so search the array using pid.  We search
         * the array back to front so as to reduce search overhead.  Passing
         * InvalidBackendId means that the target is most likely an auxiliary
         * process, which will have a slot near the end of the array.
         */
        int            i;

        for (i = NumProcSignalSlots - 1; i >= 0; i--)
        {
            slot = &ProcSignalSlots[i];

            if (slot->pss_pid == pid)
            {
                /* the above note about race conditions applies here too */

                /* Atomically set the proper flag */
                slot->pss_signalFlags[reason] = true;
                /* Send signal */
                return kill(pid, SIGUSR1);
            }
        }
    }

    errno = ESRCH;
    return -1;
}

/*
 * CheckProcSignal - check to see if a particular reason has been
 * signaled, and clear the signal flag.  Should be called after receiving
 * SIGUSR1.
 */
static bool
CheckProcSignal(ProcSignalReason reason)
{
    volatile ProcSignalSlot *slot = MyProcSignalSlot;

    if (slot != NULL)
    {
        /* Careful here --- don't clear flag if we haven't seen it set */
        if (slot->pss_signalFlags[reason])
        {
            slot->pss_signalFlags[reason] = false;
            return true;
        }
    }

    return false;
}

/*
 * procsignal_sigusr1_handler - handle SIGUSR1 signal.
 */
void
procsignal_sigusr1_handler(SIGNAL_ARGS)
{// #lizard forgives
    int            save_errno = errno;

    if (CheckProcSignal(PROCSIG_CATCHUP_INTERRUPT))
        HandleCatchupInterrupt();

    if (CheckProcSignal(PROCSIG_NOTIFY_INTERRUPT))
        HandleNotifyInterrupt();

#ifdef PGXC
    if (CheckProcSignal(PROCSIG_PGXCPOOL_RELOAD))
        HandlePoolerReload();

    if (CheckProcSignal(PROCSIG_PGXCPOOL_REFRESH))
        HandlePoolerRefresh();
#endif
    if (CheckProcSignal(PROCSIG_PARALLEL_MESSAGE))
        HandleParallelMessageInterrupt();

#ifdef __TBASE__
    if (CheckProcSignal(PROCSIG_PARALLEL_EXIT))
        HandleParallelExecutionError();
#endif

    if (CheckProcSignal(PROCSIG_WALSND_INIT_STOPPING))
        HandleWalSndInitStopping();

    if (CheckProcSignal(PROCSIG_RECOVERY_CONFLICT_DATABASE))
        RecoveryConflictInterrupt(PROCSIG_RECOVERY_CONFLICT_DATABASE);

    if (CheckProcSignal(PROCSIG_RECOVERY_CONFLICT_TABLESPACE))
        RecoveryConflictInterrupt(PROCSIG_RECOVERY_CONFLICT_TABLESPACE);

    if (CheckProcSignal(PROCSIG_RECOVERY_CONFLICT_LOCK))
        RecoveryConflictInterrupt(PROCSIG_RECOVERY_CONFLICT_LOCK);

    if (CheckProcSignal(PROCSIG_RECOVERY_CONFLICT_SNAPSHOT))
        RecoveryConflictInterrupt(PROCSIG_RECOVERY_CONFLICT_SNAPSHOT);

    if (CheckProcSignal(PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK))
        RecoveryConflictInterrupt(PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK);

    if (CheckProcSignal(PROCSIG_RECOVERY_CONFLICT_BUFFERPIN))
        RecoveryConflictInterrupt(PROCSIG_RECOVERY_CONFLICT_BUFFERPIN);

    SetLatch(MyLatch);

    latch_sigusr1_handler();

    errno = save_errno;
}
