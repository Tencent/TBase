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
 * worker_internal.h
 *      Internal headers shared by logical replication workers.
 *
 * Portions Copyright (c) 2016-2017, PostgreSQL Global Development Group
 *
 * src/include/replication/worker_internal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef WORKER_INTERNAL_H
#define WORKER_INTERNAL_H

#include <signal.h>

#include "access/xlogdefs.h"
#include "catalog/pg_subscription.h"
#include "datatype/timestamp.h"
#include "storage/lock.h"

typedef struct LogicalRepWorker
{
    /* Time at which this worker was launched. */
    TimestampTz launch_time;

    /* Indicates if this slot is used or free. */
    bool        in_use;

    /* Increased everytime the slot is taken by new worker. */
    uint16        generation;

    /* Pointer to proc array. NULL if not running. */
    PGPROC       *proc;

    /* Database id to connect to. */
    Oid            dbid;

    /* User to use for connection (will be same as owner of subscription). */
    Oid            userid;

    /* Subscription id for the worker. */
    Oid            subid;

    /* Used for initial table synchronization. */
    Oid            relid;
    char        relstate;
    XLogRecPtr    relstate_lsn;
    slock_t        relmutex;

    /* Stats. */
    XLogRecPtr    last_lsn;
    TimestampTz last_send_time;
    TimestampTz last_recv_time;
    XLogRecPtr    reply_lsn;
    TimestampTz reply_time;
} LogicalRepWorker;

/* Main memory context for apply worker. Permanent during worker lifetime. */
extern MemoryContext ApplyContext;

/* libpqreceiver connection */
extern struct WalReceiverConn *wrconn;

/* Worker and subscription objects. */
extern Subscription *MySubscription;
extern LogicalRepWorker *MyLogicalRepWorker;

extern bool in_remote_transaction;

extern void logicalrep_worker_attach(int slot);
extern LogicalRepWorker *logicalrep_worker_find(Oid subid, Oid relid,
                       bool only_running);
extern List *logicalrep_workers_find(Oid subid, bool only_running);
extern void logicalrep_worker_launch(Oid dbid, Oid subid, const char *subname,
                         Oid userid, Oid relid);
extern void logicalrep_worker_stop(Oid subid, Oid relid);
extern void logicalrep_worker_stop_at_commit(Oid subid, Oid relid);
extern void logicalrep_worker_wakeup(Oid subid, Oid relid);
extern void logicalrep_worker_wakeup_ptr(LogicalRepWorker *worker);

extern int    logicalrep_sync_worker_count(Oid subid);

extern char *LogicalRepSyncTableStart(XLogRecPtr *origin_startpos);
void        process_syncing_tables(XLogRecPtr current_lsn);
void invalidate_syncing_table_states(Datum arg, int cacheid,
                                uint32 hashvalue);

static inline bool
am_tablesync_worker(void)
{
    return OidIsValid(MyLogicalRepWorker->relid);
}

#ifdef __SUBSCRIPTION__
extern bool am_tbase_subscript_dispatch_worker(void);
extern bool IsColdMoveTBaseSubscription(void);
extern bool IsClusterSyncTBaseSubscription(void);
extern void logical_apply_dispatch(StringInfo s);
#endif

#endif                            /* WORKER_INTERNAL_H */
