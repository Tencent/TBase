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
#include "catalog/indexing.h"
#include "access/skey.h"
#include "access/htup.h"
#include "access/heapam.h"
#include "access/genam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "catalog/pgxc_shard_map.h"


bool is_group_sharding_inited(Oid group)
{
    SysScanDesc scan;
    HeapTuple   oldtup;
    Relation    sharrel;
    bool         result;
    ScanKeyData skey;

    sharrel = heap_open(PgxcShardMapRelationId, AccessShareLock);
    ScanKeyInit(&skey,
                Anum_pgxc_shard_map_nodegroup,
                BTEqualStrategyNumber, 
                F_OIDEQ,
                ObjectIdGetDatum(group));    
    
    scan    = systable_beginscan(sharrel,
                                  PgxcShardMapGroupIndexId, 
                                  true,
                                  NULL, 
                                  1, 
                                  &skey);                              

    oldtup  = systable_getnext(scan);

    result  = HeapTupleIsValid(oldtup);

    systable_endscan(scan);
    heap_close(sharrel, AccessShareLock);

    return result;
}

/* called when the first table created */
void InitShardMap(int32 nShardGroup, int32 nNodes, Oid *nodes, int16 extend, Oid nodeGroup)
{
    Relation    shardmaprel;
    HeapTuple    htup;
    bool        nulls[Natts_pgxc_shard_map];
    Datum        values[Natts_pgxc_shard_map];
    int        iShard;
    int        iAttr;
    //int        shards_of_dn;

    if(is_group_sharding_inited(nodeGroup))
    {
        return;
    }
    

    if(nNodes <= 0)
    {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("Cannot initiate shard map when cluster has not any datanode")));
    }

    //shards_of_dn = (nShardGroup + nNodes - 1) / nNodes;
    
    /*make and insert shard map record*/
    for(iAttr = 0; iAttr < Natts_pgxc_shard_map; iAttr++)
    {
        nulls[iAttr]    =    false;
        values[iAttr]    =    (Datum)0;
    }

    values[Anum_pgxc_shard_map_nodegroup-1]            = ObjectIdGetDatum(nodeGroup);
    values[Anum_pgxc_shard_map_ncopy-1]             = UInt16GetDatum(1);
    values[Anum_pgxc_shard_map_copy1-1]                = ObjectIdGetDatum(InvalidOid);
    values[Anum_pgxc_shard_map_copy2-1]                = ObjectIdGetDatum(InvalidOid);
    values[Anum_pgxc_shard_map_primarystatus-1]        = CharGetDatum(SHARD_MAP_STATUS_USING);
    values[Anum_pgxc_shard_map_status1-1]            = CharGetDatum(SHARD_MAP_STATUS_UNDEFINED);
    values[Anum_pgxc_shard_map_status2-1]            = CharGetDatum(SHARD_MAP_STATUS_UNDEFINED);
    values[Anum_pgxc_shard_map_extend-1]             = UInt16GetDatum(extend);

    shardmaprel = heap_open(PgxcShardMapRelationId, AccessExclusiveLock);
    
    for(iShard = 0; iShard < nShardGroup; iShard++)
    {        
        values[Anum_pgxc_shard_map_shardgroupid - 1]     = UInt32GetDatum(iShard);        
        //values[Anum_pgxc_shard_map_primarycopy  - 1]    = ObjectIdGetDatum(nodes[iShard % nNodes]);
        values[Anum_pgxc_shard_map_primarycopy  - 1]    = ObjectIdGetDatum(nodes[iShard % nNodes]);

        htup = heap_form_tuple(shardmaprel->rd_att, values, nulls);

        CatalogTupleInsert(shardmaprel, htup);
    }    

    CommandCounterIncrement();

    heap_close(shardmaprel, AccessExclusiveLock);

    RegisterInvalidShmemShardMap(nodeGroup, ShardOpType_create);
}

bool 
is_already_inited()
{
    SysScanDesc scan;
    HeapTuple  oldtup;
    Relation sharrel;
    bool result;

    sharrel = heap_open(PgxcShardMapRelationId, AccessShareLock);

    scan    = systable_beginscan(sharrel,
                              InvalidOid, 
                              false,
                              NULL, 
                              0, 
                              NULL);

    oldtup  = systable_getnext(scan);

    result  = HeapTupleIsValid(oldtup);

    systable_endscan(scan);
    heap_close(sharrel, AccessShareLock);

    return result;
}

bool NodeHasShard(Oid node)
{
    Relation    shardmapRel;
    HeapTuple   oldtup;
    ScanKeyData skey;
    SysScanDesc scan;
    bool result;

    result = false;
    
    shardmapRel = heap_open(PgxcShardMapRelationId, AccessShareLock);
    ScanKeyInit(&skey,
                    Anum_pgxc_shard_map_primarycopy,
                    BTEqualStrategyNumber, F_OIDEQ,
                    ObjectIdGetDatum(node));
    
    scan = systable_beginscan(shardmapRel,
                                PgxcShardMapNodeIndexId,true,
                                NULL,
                                1,&skey);

    oldtup = systable_getnext(scan);

    if(HeapTupleIsValid(oldtup))
    {
        result = true;
    }
    
    systable_endscan(scan);

    heap_close(shardmapRel,AccessShareLock);
    return result;
}


void UpdateRelationShardMap(Oid group, Oid from_node, Oid to_node,
                                int shardgroup_num,    int* shardgroups)
{
    Relation    shardmapRel;
    HeapTuple   oldtup;
    HeapTuple   newtup;
    ScanKeyData skey[2];
    SysScanDesc scan;
    int i;
    
    Datum        new_record[Natts_pgxc_shard_map];
    bool        new_record_nulls[Natts_pgxc_shard_map];
    bool        new_record_repl[Natts_pgxc_shard_map];

    shardmapRel = heap_open(PgxcShardMapRelationId, AccessExclusiveLock);

    MemSet(new_record, 0, sizeof(new_record));
    MemSet(new_record_nulls, false, sizeof(new_record_nulls));
    MemSet(new_record_repl, false, sizeof(new_record_repl));

    new_record_repl[Anum_pgxc_shard_map_primarycopy - 1] = true;

    for(i = 0; i < shardgroup_num; i++)
    {
        if(i > 0 && shardgroups[i] <= shardgroups[i - 1])
        {
            elog(ERROR,"MOVE DATA: sharding group array is not sorted");
        }

        ScanKeyInit(&skey[0],
                    Anum_pgxc_shard_map_nodegroup,
                    BTEqualStrategyNumber, F_INT4EQ,
                    ObjectIdGetDatum(group));
        
        ScanKeyInit(&skey[1],
                    Anum_pgxc_shard_map_shardgroupid,
                    BTEqualStrategyNumber, F_INT4EQ,
                    Int32GetDatum(shardgroups[i]));

        scan = systable_beginscan(shardmapRel,
                                    PgxcShardMapShardIndexId,true,
                                    NULL,
                                    2, skey);
    
        oldtup = systable_getnext(scan);

        if(!HeapTupleIsValid(oldtup))
        {
            elog(ERROR,"update shard map failed: shardgroup id[%d] is not exist.", shardgroups[i]);
        }
        
        /* just need to update primarycopy*/
        new_record[Anum_pgxc_shard_map_primarycopy-1]    = ObjectIdGetDatum(to_node);

        newtup = heap_modify_tuple(oldtup, RelationGetDescr(shardmapRel),
                                       new_record,
                                       new_record_nulls, new_record_repl);

        CatalogTupleUpdate(shardmapRel, &newtup->t_self, newtup);

        systable_endscan(scan);
        
        pfree(newtup);
    }

    CommandCounterIncrement();

    heap_close(shardmapRel, AccessExclusiveLock);
    
    //RegisterInvalidShmemShardMap(group);
}

void DropShardMap_Node(Oid group)
{
    Relation    shardmapRel;
    HeapTuple   oldtup;
    SysScanDesc scan;
    Form_pgxc_shard_map shard_map;

    shardmapRel = heap_open(PgxcShardMapRelationId, AccessExclusiveLock);

        
    scan = systable_beginscan(shardmapRel,
                                InvalidOid,
                                false,
                                NULL,
                                0,
                                NULL);

    oldtup = systable_getnext(scan);    
    while(HeapTupleIsValid(oldtup))
    {
        shard_map = (Form_pgxc_shard_map)GETSTRUCT(oldtup);

        if(shard_map->disgroup == group)
        {
            simple_heap_delete(shardmapRel,&oldtup->t_self);
        }
        oldtup = systable_getnext(scan);
    }    

    systable_endscan(scan);

    CommandCounterIncrement();
    
    heap_close(shardmapRel,AccessExclusiveLock);

    RegisterInvalidShmemShardMap(group, ShardOpType_drop);
}

