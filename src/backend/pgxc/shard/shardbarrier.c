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
#include "postgres.h"

#include "funcapi.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "storage/shmem.h"
#include "storage/relfilenode.h"
#include "storage/spin.h"
#include "storage/lwlock.h"
#include "storage/lockdefs.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"
#include "utils/builtins.h"
#include "pgxc/shardmap.h"
#include "utils/fmgroids.h"

#define MAX_BARRIER_SHARDS    256

typedef struct ShardBarrierTag
{
    RelFileNode    rel;
    ShardID        sid;
    int16       reserved;
}ShardBarrierTag;

typedef struct ShardBarrierEnt
{
    ShardBarrierTag    tag;
    int32        flags;
    BackendId    pid;
    TimestampTz start_time;
}ShardBarrierEnt;

typedef struct ShardBarrierInfo
{
    int32    n_shards;
}ShardBarrierInfo;

/* in share memory */
static ShardBarrierInfo *g_barrier_shards_info = NULL;
static HTAB                *g_barrier_shards_ht = NULL;

/* process local */
static bool has_shard_barriered = false;
static ShardBarrierTag barriered_shard;

void ShardBarrierShmemInit(void)
{
    bool found;
    HASHCTL        info;

    g_barrier_shards_info = (ShardBarrierInfo *)ShmemInitStruct("BarrierShardInfo",
                                                sizeof(ShardBarrierInfo),
                                                &found);

    if(!found)
    {
        g_barrier_shards_info->n_shards = 0;
    }
    
    /* init hash table */
    info.keysize   = sizeof(ShardBarrierTag);
    info.entrysize = sizeof(ShardBarrierEnt);
    info.hash = tag_hash;

    g_barrier_shards_ht = ShmemInitHash("ShardBarrierHashTable",
                                  MAX_BARRIER_SHARDS, 
                                  MAX_BARRIER_SHARDS,
                                  &info,
                                  HASH_ELEM | HASH_FUNCTION);
}

Size ShardBarrierShmemSize(void)
{
    Size size;
    
    size = 0;    
    /* hash table size */
    size = hash_estimate_size(MAX_BARRIER_SHARDS, sizeof(ShardBarrierEnt));
    
    /* management info */
    size = add_size(size, MAXALIGN64(sizeof(ShardBarrierInfo)));

    return size;
}

void AddShardBarrier(RelFileNode rel, ShardID sid, BackendId pid)
{
    bool found;
    ShardBarrierEnt *ent;
    ShardBarrierTag tag;

    if(!ShardIDIsValid(sid))
    {
        elog(ERROR, "add shard barrier failed. because sid %d is invalid.", sid);
    }

    if(has_shard_barriered)
    {
        elog(ERROR, "only one shard can be barriered at the same time.");
    }
    
    tag.rel = rel;
    tag.sid = sid;
    tag.reserved = 0;

    LWLockAcquire(ShardBarrierLock, LW_EXCLUSIVE);
    if(g_barrier_shards_info->n_shards >= MAX_BARRIER_SHARDS)
    {
        LWLockRelease(ShardBarrierLock);
        elog(ERROR, "too many shards are vacuuming right now, please try it later.");
    }
    
    ent = (ShardBarrierEnt *)hash_search(g_barrier_shards_ht, (void *)&tag, HASH_ENTER, &found);

    if(!found)
    {
        ent->flags = 0;
        ent->pid = pid;
        ent->start_time = GetCurrentTimestamp();
        has_shard_barriered = true;
        memcpy(&barriered_shard, &tag, sizeof(ShardBarrierTag));
        g_barrier_shards_info->n_shards++;
    }
    LWLockRelease(ShardBarrierLock);

    if(found)
    {
        elog(INFO, "shard barrier is already exist.");
    }
}

void RemoveOneShardBarrier(RelFileNode rel, ShardID sid)
{
    bool found = false;
    ShardBarrierTag tag;

    if(!ShardIDIsValid(sid))
    {
        elog(ERROR, "add shard barrier failed. because sid %d is invalid.", sid);
    }
    
    tag.rel = rel;
    tag.sid = sid;
    tag.reserved = 0;

    LWLockAcquire(ShardBarrierLock, LW_EXCLUSIVE);
    (void)hash_search(g_barrier_shards_ht, (void *)&tag, HASH_REMOVE, &found);

    if(found)
    {
        has_shard_barriered = false;
        memset(&barriered_shard, 0, sizeof(ShardBarrierTag));
        g_barrier_shards_info->n_shards--;
    }
    
    LWLockRelease(ShardBarrierLock);

    if(!found)
    {
        elog(INFO, "shard %d barrier of relation %d/%d/%d is not exist exist.", 
                    sid, rel.dbNode, rel.spcNode, rel.relNode);
    }
}

void RemoveShardBarrier()
{
    if(!has_shard_barriered)
        return;

    RemoveOneShardBarrier(barriered_shard.rel, barriered_shard.sid);
}


bool IsShardBarriered(RelFileNode rel, ShardID sid)
{
    
    bool found;
    ShardBarrierTag tag;

    if(!ShardIDIsValid(sid))
    {
        return false;
    }

    if(g_barrier_shards_info->n_shards == 0)
        return false;

    if(LocalHasShardBarriered(rel, sid))
        return true;

    tag.rel = rel;
    tag.sid = sid;
    tag.reserved = 0;

    LWLockAcquire(ShardBarrierLock, LW_SHARED);
    (void)hash_search(g_barrier_shards_ht, (void *)&tag, HASH_FIND, &found);
    LWLockRelease(ShardBarrierLock);
    
    return found;
}

bool LocalHasShardBarriered(RelFileNode rel, ShardID sid)
{
    if(!has_shard_barriered)
        return false;

    if(RelFileNodeEquals(rel,barriered_shard.rel) && sid == barriered_shard.sid)
        return true;

    return false;
}

void ATEOXact_CleanUpShardBarrier(void)
{
#if 0
    if(has_shard_barriered)
    {
        elog(ERROR, "remove shard barrier[%d/%d/%d|%d] because of exception.",
                    barriered_shard.rel.dbNode,
                    barriered_shard.rel.spcNode,
                    barriered_shard.rel.relNode,
                    barriered_shard.sid);
    }
#endif
    RemoveShardBarrier();    
}

typedef struct
{
    int    currIdx;
    int max_barriers;
    ShardBarrierEnt bars[MAX_BARRIER_SHARDS];    
} BarrierShardState;

#define BARRIER_SHARD_COLUMN_NUM 7
Datum pg_stat_barrier_shards(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx = NULL;
    BarrierShardState   *bar_status  = NULL;
    Datum        values[BARRIER_SHARD_COLUMN_NUM];
    bool        nulls[BARRIER_SHARD_COLUMN_NUM];
    HeapTuple    tuple;
    Datum        result;

    if (SRF_IS_FIRSTCALL())
    {        
        TupleDesc   tupdesc;
        MemoryContext oldcontext;
        
        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
                * Switch to memory context appropriate for multiple function calls
                */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples */
        tupdesc = CreateTemplateTupleDesc(BARRIER_SHARD_COLUMN_NUM, false);
        TupleDescInitEntry(tupdesc, (AttrNumber) 1, "pid",
                         INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 2, "dbid",
                         INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 3, "spcid",
                         INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 4, "relid",
                         INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 5, "shardid",
                         INT2OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 6, "start",
                         TIMESTAMPTZOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 7, "flags",
                         INT4OID, -1, 0);


        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        bar_status = (BarrierShardState *) palloc(sizeof(BarrierShardState));
        funcctx->user_fctx = (void *) bar_status;

        {
            HASH_SEQ_STATUS scan_status;
            ShardBarrierEnt  *item;
            int n_items = 0;

            LWLockAcquire(ShardBarrierLock, LW_SHARED);
            hash_seq_init(&scan_status, g_barrier_shards_ht);
            while ((item = (ShardBarrierEnt *) hash_seq_search(&scan_status)) != NULL)
            {
                memcpy(&bar_status->bars[n_items++],item, sizeof(ShardBarrierEnt));
            }

            bar_status->max_barriers = n_items;

            LWLockRelease(ShardBarrierLock);
        }
        bar_status->currIdx = 0;

        MemoryContextSwitchTo(oldcontext);
    }

      funcctx = SRF_PERCALL_SETUP();
    bar_status  = (BarrierShardState *) funcctx->user_fctx;

    if(bar_status->currIdx >= bar_status->max_barriers)
        SRF_RETURN_DONE(funcctx);

    MemSet(values, 0, sizeof(values));
    MemSet(nulls,  0, sizeof(nulls));

    values[0] = Int32GetDatum(bar_status->bars[bar_status->currIdx].pid);
    values[1] = Int32GetDatum(bar_status->bars[bar_status->currIdx].tag.rel.dbNode);
    values[2] = Int32GetDatum(bar_status->bars[bar_status->currIdx].tag.rel.spcNode);
    values[3] = Int32GetDatum(bar_status->bars[bar_status->currIdx].tag.rel.relNode);
    values[4] = Int16GetDatum(bar_status->bars[bar_status->currIdx].tag.sid);
    values[5] = TimestampTzGetDatum(bar_status->bars[bar_status->currIdx].start_time);
    values[6] = Int32GetDatum(bar_status->bars[bar_status->currIdx].flags);

    bar_status->currIdx++;
    tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
    result = HeapTupleGetDatum(tuple);
    SRF_RETURN_NEXT(funcctx, result);
}

