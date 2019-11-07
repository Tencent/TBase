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

#include "access/skey.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/heapam.h"
#include "access/genam.h"
#include "catalog/indexing.h"
#include "catalog/pgxc_key_values.h"
#include "pgxc/nodemgr.h"
#include "pgxc/shardmap.h"
#include "utils/syscache.h"
#include "utils/catcache.h"
#include "utils/relcache.h"
#include "utils/rel.h"
#include "utils/elog.h"
#include "utils/fmgroids.h"
#include "utils/tqual.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "access/xact.h"
#include "miscadmin.h"
#include "catalog/pgxc_shard_map.h"
#include "utils/lsyscache.h"
#include "catalog/heap.h"



static Oid IsKeyValuesDup(Oid db, Oid rel, char *value)
{
    HeapTuple    tup;
    tup = SearchSysCache(SHARDKEYVALUE,
                          ObjectIdGetDatum(db),
                          ObjectIdGetDatum(rel),
                          CStringGetDatum(value), 
                          0);    
    if (HeapTupleIsValid(tup)) 
    {
        ReleaseSysCache(tup);    
        return true;
    }

    return false;
}

void CreateKeyValues(Oid db, Oid rel, int32 nValues, char **keyvalues, Oid nodeGroup, Oid coldGroup)
{
    Relation    kvrel;
    HeapTuple    htup;
    bool        nulls[Natts_pgxc_key_value];
    Datum        values[Natts_pgxc_key_value];
    NameData    name;
    Relation     relation;
    int            i;
    
    for (i = 0; i < nValues; i++)
    {
        if (true == IsKeyValuesDup(db, rel, keyvalues[i]))
        {
            elog(ERROR , "value:%s has been already created on db:%u, rel:%u", keyvalues[i], db, rel);
        }
    }
    
    /* make and insert shard map record */
    for(i = 0; i < Natts_pgxc_key_value; i++)
    {
        nulls[i]    =    false;
        values[i]    =    (Datum)0;
    }

    kvrel = heap_open(PgxcKeyValueRelationId, AccessExclusiveLock);
    
    for(i = 0; i < nValues; i++)
    {        
        namestrcpy(&name, keyvalues[i]);
        values[Anum_pgxc_key_valuew_db - 1]            = ObjectIdGetDatum(db);
        values[Anum_pgxc_key_values_rel - 1]        = ObjectIdGetDatum(rel);
        values[Anum_pgxc_key_value_value - 1]        = NameGetDatum(&name);;
        values[Anum_pgxc_key_value_group -1]        = ObjectIdGetDatum(nodeGroup);
        values[Anum_pgxc_key_value_cold_group -1]    = ObjectIdGetDatum(coldGroup);
        htup = heap_form_tuple(kvrel->rd_att, values, nulls);

        CatalogTupleInsert(kvrel, htup);
    }    

    CommandCounterIncrement();

    heap_close(kvrel, AccessExclusiveLock);

    /* tell other backend to refresh backend relcache of the rel */
    relation = heap_open(rel,AccessShareLock);
    CacheInvalidateRelcache(relation);
    heap_close(relation,AccessShareLock);
}


/*
 * Check Key Value hot and cold group valid
 */
void CheckKeyValueGroupValid(Oid hot, Oid cold, bool create_table)
{// #lizard forgives
    HeapScanDesc scan;
    HeapTuple      tup;
    Form_pgxc_key_value pgxc_key_value;
    Relation rel;

    rel = heap_open(PgxcKeyValueRelationId, AccessShareLock);    
    scan = heap_beginscan_catalog(rel, 0, NULL);
    tup = heap_getnext(scan,ForwardScanDirection);
    
    while(HeapTupleIsValid(tup))
    {
        pgxc_key_value = (Form_pgxc_key_value)GETSTRUCT(tup);

        if (OidIsValid(cold))
        {
            if (OidIsValid(pgxc_key_value->nodegroup))
            {
                if(cold == pgxc_key_value->nodegroup)
                {
                     elog(ERROR, "cold group %u conflict exist table:%u key value:%s hot group %u", cold, pgxc_key_value->reloid, NameStr(pgxc_key_value->keyvalue), pgxc_key_value->nodegroup);
                    
                }
            }

            if (create_table)
            {
                if (OidIsValid(pgxc_key_value->coldnodegroup))
                {
                    if(cold == pgxc_key_value->coldnodegroup)
                    {
                         elog(ERROR, "cold group %u conflict exist key value cold group %u", cold, pgxc_key_value->coldnodegroup);
                        
                    }
                }    
            }
        }        

        if (OidIsValid(hot))
        {
            if (OidIsValid(pgxc_key_value->coldnodegroup))
            {
                if(hot == pgxc_key_value->coldnodegroup)
                {
                     elog(ERROR, "hot group %u conflict exist key value cold group %u", hot, pgxc_key_value->coldnodegroup);
                    
                }
            }            
        }
        
        tup = heap_getnext(scan,ForwardScanDirection);
    }

    heap_endscan(scan);
    heap_close(rel, AccessShareLock);
}

/*
 * Check Key Value hot and cold group valid
 */
void CheckPgxcGroupValid(Oid tablehot)
{
    HeapScanDesc scan;
    HeapTuple      tup;
    Form_pgxc_key_value pgxc_key_value;
    Relation rel;

    if (!OidIsValid(tablehot))
    {
        return;
    }
    
    rel  = heap_open(PgxcKeyValueRelationId, AccessShareLock);    
    scan = heap_beginscan_catalog(rel, 0, NULL);
    tup  = heap_getnext(scan,ForwardScanDirection);
    
    while(HeapTupleIsValid(tup))
    {
        pgxc_key_value = (Form_pgxc_key_value)GETSTRUCT(tup);
        
        if (OidIsValid(pgxc_key_value->nodegroup))
        {
            if(tablehot == pgxc_key_value->nodegroup)
            {
                 elog(ERROR, "table hot group %u conflict exist key value hot group %u", tablehot, pgxc_key_value->nodegroup);                
            }
        }        
        
        tup = heap_getnext(scan,ForwardScanDirection);
    }

    heap_endscan(scan);
    heap_close(rel, AccessShareLock);
}

bool GatherRelationKeyValueGroup(Oid relation, int32 *hot, Oid **group, int32 *cold, Oid **coldgroup)
{// #lizard forgives
    bool         found   = false;
    bool         exist   = false;
    int32        i       = 0;
    int32        hotNum  = 0;
    int32        coldNum = 0;
    Oid          hotGroup[MAX_SHARDING_NODE_GROUP];
    Oid          coldGroup[MAX_SHARDING_NODE_GROUP];
    
    HeapScanDesc scan;
    HeapTuple     tup;
    Form_pgxc_key_value pgxc_key_value;
    Relation rel;

    rel = heap_open(PgxcKeyValueRelationId, AccessShareLock);    
    scan = heap_beginscan_catalog(rel, 0, NULL);
    tup = heap_getnext(scan,ForwardScanDirection);
    
    exist = false;
    while(HeapTupleIsValid(tup))
    {
        pgxc_key_value = (Form_pgxc_key_value)GETSTRUCT(tup);

        if(relation == pgxc_key_value->reloid)
        {
            exist = true;
            if (OidIsValid(pgxc_key_value->nodegroup))
            {
                found = false;
                for (i = 0; i < hotNum; i++)
                {
                    if (hotGroup[i] == pgxc_key_value->nodegroup)
                    {
                        found = true;
                        break;
                    }
                }

                if (!found && i < MAX_SHARDING_NODE_GROUP)
                {
                    hotGroup[i] = pgxc_key_value->nodegroup;
                    hotNum++;
                }
            }

            if (OidIsValid(pgxc_key_value->coldnodegroup))
            {
                found = false;
                for (i = 0; i < coldNum; i++)
                {
                    if (coldGroup[i] == pgxc_key_value->coldnodegroup)
                    {
                        found = true;
                        break;
                    }
                }

                if (!found && i < MAX_SHARDING_NODE_GROUP)
                {
                    coldGroup[i] = pgxc_key_value->coldnodegroup;
                    coldNum++;
                }
            }
        }
        
        tup = heap_getnext(scan,ForwardScanDirection);
    }

    heap_endscan(scan);
    heap_close(rel, AccessShareLock);
    if (exist)
    {
        *hot = hotNum;
        if (*hot)
        {    
            *group = (Oid*)palloc(sizeof(Oid) * hotNum);
            memcpy((void*)*group, (void*)hotGroup, sizeof(Oid) * hotNum);
        }

        *cold = coldNum;
        if (*cold)
        {
            
            *coldgroup = (Oid*)palloc(sizeof(Oid) * coldNum);
            memcpy((void*)*coldgroup, (void*)coldGroup, sizeof(Oid) * coldNum);
        }
    }
    return exist;    
}

void GetRelationSecondGroup(Oid rel, Oid **groups, int32 *nGroup)
{// #lizard forgives
    bool        dup;
    int32       i;
    int32       numGroup = 0;
    Relation    keyvalue;    
    SysScanDesc scan;
    HeapTuple    tuple;
    Form_pgxc_key_value pgxc_keyvalue;
    ScanKeyData skey[2];
    Oid         groupvec[MAX_SHARDING_NODE_GROUP];
    
    keyvalue = heap_open(PgxcKeyValueRelationId, AccessShareLock);
    
    ScanKeyInit(&skey[0],
                    Anum_pgxc_key_valuew_db,
                    BTEqualStrategyNumber, F_OIDEQ,
                    ObjectIdGetDatum(MyDatabaseId));
    
    ScanKeyInit(&skey[1],
                    Anum_pgxc_key_values_rel,
                    BTEqualStrategyNumber, F_OIDEQ,
                    ObjectIdGetDatum(rel));    
    
    scan = systable_beginscan(keyvalue,
                                PgxcShardKeyGroupIndexID,true,
                                NULL,
                                2,
                                skey);

    numGroup = 0;
    tuple = systable_getnext(scan);
    while(HeapTupleIsValid(tuple))
    {
        pgxc_keyvalue = (Form_pgxc_key_value) GETSTRUCT(tuple);
        /* hot node group */
        dup = false;
        for (i = 0; i < numGroup; i++)
        {
            if (groupvec[i] == pgxc_keyvalue->nodegroup)
            {
                dup = true;
                break;
            }
        }

        if (!dup)
        {
            groupvec[numGroup] = pgxc_keyvalue->nodegroup;
            numGroup++;
        }

        /* cold node group */
        if (OidIsValid(pgxc_keyvalue->coldnodegroup))
        {
            dup = false;
            for (i = 0; i < numGroup; i++)
            {
                if (groupvec[i] == pgxc_keyvalue->coldnodegroup)
                {
                    dup = true;
                    break;
                }
            }

            if (!dup)
            {
                groupvec[numGroup] = pgxc_keyvalue->coldnodegroup;
                numGroup++;
            }
        }
        tuple = systable_getnext(scan);
    }
    
    systable_endscan(scan);
    heap_close(keyvalue,AccessShareLock);
    
    if (numGroup)
    {
        *nGroup = numGroup;
        *groups = (Oid*)palloc(sizeof(Oid) * numGroup);
        memcpy((char*)*groups, (char*)groupvec, sizeof(Oid) * numGroup);
    }    
    else
    {
        *nGroup = 0;
    }
}

char *BuildKeyValueCheckoverlapsStr(Oid hotgroup, Oid coldgroup)
{// #lizard forgives
    int32 offset        = 0;
    int32 i             = 0;
    int32 hotNumber     = 0;
    int32 coldNumber    = 0;
    Oid   *hotGroupMem  = NULL;
    Oid   *coldGroupMem = NULL;
    char  *string       = NULL;
    char  *nodename     = NULL;
    
    if (OidIsValid(hotgroup))
    {            
        if (!is_group_sharding_inited(hotgroup))
        {
            elog(ERROR, "please initialize group:%u sharding map first", hotgroup);
        }
        
        hotNumber = get_pgxc_groupmembers(hotgroup, &hotGroupMem);
        offset    = 0;
        string    = (char*)palloc0(BLCKSZ);        
        offset    = snprintf(string, BLCKSZ, "CHECK OVERLAPS ");
        for (i = 0; i < hotNumber; )
        {
            nodename = get_pgxc_nodename(hotGroupMem[i]);
            offset  += snprintf(string + offset, BLCKSZ - offset, "%s", nodename);
            pfree(nodename);
            
            i++;
            if (i < hotNumber)
            {
                offset  += snprintf(string + offset, BLCKSZ - offset, ",");
            }
        }
        pfree(hotGroupMem);
    
            
        if (OidIsValid(coldgroup))
        {
            if (!is_group_sharding_inited(coldgroup))
            {
                elog(ERROR, "please initialize group:%u sharding map first", coldgroup);
            }
            
            coldNumber = get_pgxc_groupmembers(coldgroup, &coldGroupMem);                
            if (coldNumber)
            {
                offset  += snprintf(string + offset, BLCKSZ - offset, " TO ");
                for (i = 0; i < coldNumber; )
                {
                    nodename = get_pgxc_nodename(coldGroupMem[i]);
                    offset  += snprintf(string + offset, BLCKSZ - offset, "%s", nodename);
                    pfree(nodename);
                    
                    i++;
                    if (i < coldNumber)
                    {
                        offset  += snprintf(string + offset, BLCKSZ - offset, ",");
                    }
                }
                pfree(coldGroupMem);
            }                
        }
        offset  += snprintf(string + offset, BLCKSZ - offset, ";");
        return string;
    }    
    return NULL;
}

char *BuildRelationCheckoverlapsStr(DistributeBy *distributeby,
                                       PGXCSubCluster *subcluster)
{// #lizard forgives
    int32 offset     = 0;
    int32 i          = 0;
    int32 hotNumber  = 0;
    int32 coldNumber = 0;
    int32 groupNum   = 0;
    Oid group[2]     = {InvalidOid}; /* max 2 groups to distribute */
    Oid   *hotGroupMem  = NULL;
    Oid   *coldGroupMem = NULL;
    char  *string       = NULL;
    char  *nodename     = NULL;
    if (distributeby)
    {
        if(DISTTYPE_SHARD == distributeby->disttype)
        {
            groupNum = GetDistributeGroup(subcluster, distributeby->disttype, group);            
            if (groupNum)
            {
                if (!is_group_sharding_inited(group[0]))
                {
                    elog(ERROR, "please initialize group:%u sharding map first", group[0]);
                }
                hotNumber = get_pgxc_groupmembers(group[0], &hotGroupMem);
                
                if (groupNum > 1)
                {
                    if (!is_group_sharding_inited(group[1]))
                    {
                        elog(ERROR, "please initialize group:%u sharding map first", group[1]);
                    }
                    coldNumber = get_pgxc_groupmembers(group[1], &coldGroupMem);
                }
            }

            if (hotNumber || coldNumber)
            {
                offset = 0;
                string = (char*)palloc0(BLCKSZ);
                offset = snprintf(string, BLCKSZ, "CHECK OVERLAPS ");
                for (i = 0; i < hotNumber; )
                {
                    nodename = get_pgxc_nodename(hotGroupMem[i]);
                    offset  += snprintf(string + offset, BLCKSZ - offset, "%s", nodename);
                    pfree(nodename);
                    
                    i++;
                    if (i < hotNumber)
                    {
                        offset  += snprintf(string + offset, BLCKSZ - offset, ",");
                    }
                }
                pfree(hotGroupMem);
                
                if (coldNumber)
                {
                    offset  += snprintf(string + offset, BLCKSZ - offset, " TO ");
                    for (i = 0; i < coldNumber; )
                    {
                        nodename = get_pgxc_nodename(coldGroupMem[i]);
                        offset  += snprintf(string + offset, BLCKSZ - offset, "%s", nodename);
                        pfree(nodename);
                        
                        i++;
                        if (i < coldNumber)
                        {
                            offset  += snprintf(string + offset, BLCKSZ - offset, ",");
                        }
                    }
                    pfree(coldGroupMem);
                }    

                offset  += snprintf(string + offset, BLCKSZ - offset, ";");
                return string;
            }
        }                
    }
    return NULL;
}

Oid GetKeyValuesGroup(Oid db, Oid rel, char *value, Oid *coldgroup)
{
    Oid         group;
    HeapTuple    tup;
    Form_pgxc_key_value keyvalue;
    tup = SearchSysCache(SHARDKEYVALUE,
                          ObjectIdGetDatum(db),
                          ObjectIdGetDatum(rel),
                          CStringGetDatum(value), 
                          0);    
    if (!HeapTupleIsValid(tup)) 
    {
        return InvalidOid;
    }
    
    keyvalue = (Form_pgxc_key_value) GETSTRUCT(tup);
    group    = keyvalue->nodegroup;
    *coldgroup = keyvalue->coldnodegroup;
    ReleaseSysCache(tup);    
    return group;
}

bool IsKeyValues(Oid db, Oid rel, char *value)
{
    return SearchSysCacheExists(SHARDKEYVALUE,
                                  ObjectIdGetDatum(db),
                                  ObjectIdGetDatum(rel),
                                  CStringGetDatum(value), 
                                  0);    
}

