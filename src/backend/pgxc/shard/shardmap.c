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
#include "storage/extentmapping.h"
#include "storage/s_lock.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "storage/spin.h"
#include "storage/lwlock.h"
#include "storage/lockdefs.h"
#include "storage/proc.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/inval.h"
#include "pgxc/shardmap.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/nodemgr.h"
#include "pgxc/locator.h"
#include "nodes/bitmapset.h"
#include "access/htup.h"
#include "access/hash.h"
#include "access/xact.h"
#include "access/skey.h"
#include "access/heapam.h"
#include "access/sdir.h"
#include "access/htup_details.h"
#include "access/stratnum.h"
#include "access/relscan.h"
#include "access/genam.h"
#include "access/hash.h"
#include "catalog/pgxc_shard_map.h"
#include "catalog/pgxc_group.h"
#include "catalog/indexing.h"
#include "catalog/pgxc_class.h"
#include "catalog/heap.h"
#include "catalog/storage_xlog.h"
#include "nodes/makefuncs.h"
#include "commands/tablecmds.h"
#include "commands/vacuum.h"
#include "postmaster/bgwriter.h"
#include "miscadmin.h"
#include "pgxc/groupmgr.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/regproc.h"
#include "catalog/namespace.h"
#include <sys/stat.h>
#ifdef __COLD_HOT__
#include "catalog/pgxc_key_values.h"
#include "utils/memutils.h"
#include "postmaster/postmaster.h"
#include "utils/ruleutils.h"
#endif

/* 12 month for a year */
#define COLD_HOT_INTERVAL_YEAR 12

bool g_IsExtension;
bool enable_cold_hot_router_print;
extern bool trace_extent;

typedef struct
{
    Oid     group;                    /* group sharding into */
    int32   shardMaxGlblNdIdx;        /* max global node index */
    int32   shardMapStatus;         /* shard map status */
    int32   shmemNumShardGroups;    /* number of shard groups */
    int32   shmemNumShards;            /* number of shards */
    int32   shmemNumShardNodes;        /* number of nodes in the sharding group*/

    int32   shmemshardnodes[MAX_GROUP_NODE_NUMBER];/* node index of this group, ordered by node name */
    int32   shmemNodeMap[TBASE_MAX_DATANODE_NUMBER];    /* map for global node index to group native node index */
    ShardMapItemDef shmemshardmap[1];    /* shard map info */
}GroupShardInfo;

typedef struct
{
    uint64            group;
}GroupLookupTag;

typedef struct
{    
    GroupLookupTag tag;
    int64           shardIndex;                /* Associated into  ShardNodeGroupInfo index */
} GroupLookupEnt;

typedef struct 
{
    bool           inited;
    bool           needLock;                      /* whether we need lock */
    slock_t           lock[MAX_SHARDING_NODE_GROUP]; /* locks to protect used fields */
    bool            used[MAX_SHARDING_NODE_GROUP];

    GroupShardInfo *members[MAX_SHARDING_NODE_GROUP];
}ShardNodeGroupInfo;

/* As DN, only one group has shard info. */
typedef struct 
{
    bool           inited;
    bool           needLock;                      /* whether we need lock */
    slock_t           lock; /* locks to protect used fields */
    bool            used;

    GroupShardInfo *members;
}ShardNodeGroupInfo_DN;

/*For CN*/
static ShardNodeGroupInfo *g_GroupShardingMgr = NULL;
static HTAB               *g_GroupHashTab     = NULL;

/*For DN*/
static ShardNodeGroupInfo_DN *g_GroupShardingMgr_DN = NULL;

/* For local DN received from parent node */
static bool g_ShardMapValid = false;
static ShardMapItemDef g_ShardMap[SHARD_MAP_GROUP_NUM];

/* used for datanodes */
Bitmapset                      *g_DatanodeShardgroupBitmap  = NULL;

typedef struct
{
    int32 nGroups;
    Oid   group[MAX_SHARDING_NODE_GROUP];
    int32 optype[MAX_SHARDING_NODE_GROUP];
}ShardingGroupInfo;


/* Working status for pg_stat_table_shard */
typedef struct
{
    int          currIdx;
    ShardStat shardstat[SHARD_MAP_GROUP_NUM];    
} ShardStat_State;
/* used to record sharding group info */
static ShardingGroupInfo g_UpdateShardingGroupInfo = {0, {InvalidOid}};

/*        shard statistic management 
 *
 *  stat all shards' info including:
 *    ntuples_select: number of tuples scanned in each shard
 *    ntuples_insert: number of tuples inserted in each shard
 *    ntuples_update: number of tuples updated in each shard
 *    ntuples_delete: number of tuples deleted in each shard
 *    ntuples: number of tuples in each shard
 *    size: size of all tuples in each shard
 */

typedef struct 
{
    pg_atomic_uint64 ntuples_select;
    pg_atomic_uint64 ntuples_insert;
    pg_atomic_uint64 ntuples_update;
    pg_atomic_uint64 ntuples_delete;
    pg_atomic_uint64 ntuples;
    pg_atomic_uint64 size;
} ShardStatistic;

typedef struct
{
    ShardStatistic stat;
    pg_crc32c      crc;
} ShardRecord;

ShardStatistic *shardStatInfo = NULL;

#define SHARD_STATISTIC_FILE_PATH "pg_stat/shard.stat"

/* GUC used for shard statistic */
bool   g_StatShardInfo = true;
int    g_MaxSessionsPerPool = 250;
int    g_ShardInfoFlushInterval = 60;

typedef struct
{
    int    currIdx;
    ShardStatistic *rec;
} ShmMgr_State;

bool  show_all_shard_stat = false;

#ifdef __COLD_HOT__
/* lock access info, use on datanode */
typedef struct 
{      
    bool    needlock;
    bool    colddata;
}AccessControl;

AccessControl  *g_AccessCtl    = NULL;

#define ACCESS_CONTROL_FILE_NAME        "cold_data_node.flag"

/* dual tag, used on coordinator */
#define MAX_DUAL_WRITE_TABLE 1024
typedef struct 
{
    Oid        relation;
    int32      attr;
    int32      timeoffset;
}DTag;
    
typedef struct
{
    DTag        tag;
    char       table[NAMEDATALEN]; 
    char       column[NAMEDATALEN]; 
    char       value[NAMEDATALEN]; 
}DualWriteRecord;
#define DUAL_WRITE_HASH_SCANF_ELEMENT 4

typedef struct 
{    
    HTAB     *dwhash;
    slock_t    lock;/* lock to protect the below fields */
    bool    needlock;
    int32   entrynum;
}DualWriteCtl;

DualWriteCtl   *g_DualWriteCtl = NULL;

#define DUAL_WRITE_FILE_NAME        "dual_write_list.stat"
#define DUAL_WRITE_BACKUP_FILE_NAME "dual_write_list.backup"

bool g_EnableKeyValue  = true;
bool g_EnableDualWrite = true;
List *g_TempKeyValueList  = NIL;
bool g_EnableColdHotVisible = false;

typedef struct
{
    Oid      table;
    NameData value;        
    Oid      hotGroup;
    Oid      coldGroup;
}KeyValuePair;

/* 2000-01-01 00:00:00 */
static struct pg_tm g_keyvalue_base_time = { 0,
                                             0,
                                             0,
                                             1,
                                             1,      /* origin 0, not 1 */
                                             1990,    
                                             1,
                                             1,
                                             0,
                                             0,
                                             NULL
                                            };
static TimestampTz  g_keyvalue_base_timestamp = 0;

#endif

/* function declaration */
static void   SyncShardMapList_CN(bool force);
static void   SyncShardMapList_DN(bool force);
static void   SyncShardMapList_Node_CN(void);
static bool   SyncShardMapList_Node_DN(void);
static void   InsertShardMap_CN(int32 map, Form_pgxc_shard_map record);
static void   InsertShardMap_DN(Form_pgxc_shard_map record);
static void   ShardMapInitDone_CN(int32 map, Oid group, bool lock);
static void   ShardMapInitDone_DN(Oid group, bool need_lock);
static int    cmp_int32(const void *p1, const void *p2);
static bool   GroupShardingTabExist(Oid group, bool lock, int *group_index);
static int32  AllocShardMap(bool extend, Oid groupoid);
static Size   ShardMapShmemSize_Node_CN(void);
static Size   ShardMapShmemSize_Node_DN(void);
static int*   decide_shardgroups_to_move(Oid group, Oid fromnode, int *shardgroup_num);
static void   RemoveShardMapEntry(Oid group);
static void   FreshGroupShardMap(Oid group);
static void   BuildDatanodeVisibilityMap(Form_pgxc_shard_map tuple, Oid self_oid);
static void GetShardNodes_CN(Oid group, int32 ** nodes, int32 *num_nodes, bool *isextension);
static void GetShardNodes_DN(Oid group, int32 ** nodes, int32 *num_nodes, bool *isextension);

extern Datum  pg_stat_table_shard(PG_FUNCTION_ARGS);
extern Datum  pg_stat_all_shard(PG_FUNCTION_ARGS);

static void RefreshShardStatistic(ShardStat *shardstat);

#ifdef __COLD_HOT__
static Datum pg_set_cold_access(void);
static Datum pg_clear_cold_access(void);
static bool AddDualWriteInfo(Oid relation, AttrNumber attr, int32 gap, char *table, char *column, char *value);
static long compute_keyvalue_hash(Oid type, Datum value);

#endif

/*----------------------------------------------------------
 *
 *        Shard Map In Share Memory
 *
 -----------------------------------------------------------*/

void InvalidateShmemShardMap(bool iscommit)
{// #lizard forgives
    int32 i = 0;
    
    if (g_UpdateShardingGroupInfo.nGroups)
    {
        if(iscommit)
        {
            if (IS_PGXC_COORDINATOR)
            {
                /* tell others use lock to access shard map */
                g_GroupShardingMgr->needLock = true;
                
                LWLockAcquire(ShardMapLock, LW_EXCLUSIVE);
                for (i = 0; i < g_UpdateShardingGroupInfo.nGroups; i++)
                {
                    if (ShardOpType_create == g_UpdateShardingGroupInfo.optype[i])
                    {
                        /* refresh shard map of given group. */
                        FreshGroupShardMap(g_UpdateShardingGroupInfo.group[i]);
                    }
                    else if (ShardOpType_drop == g_UpdateShardingGroupInfo.optype[i])
                    {
                        RemoveShardMapEntry(g_UpdateShardingGroupInfo.group[i]);
                    }                    
                }
                LWLockRelease(ShardMapLock);
                /* reset flag */
                g_GroupShardingMgr->needLock = false;
            }
            else if (IS_PGXC_DATANODE)
            {
                /* tell others use lock to access shard map */
                g_GroupShardingMgr_DN->needLock = true;                
                for (i = 0; i < g_UpdateShardingGroupInfo.nGroups; i++)
                {
                    if (ShardOpType_create == g_UpdateShardingGroupInfo.optype[i])
                    {
                        /* force fresh shard map of DN. SyncShardMapList will take care of lock itself, no need to take lock here. */
                        SyncShardMapList(true);
                    }
                    else if (ShardOpType_drop == g_UpdateShardingGroupInfo.optype[i])
                    {
                        LWLockAcquire(ShardMapLock, LW_EXCLUSIVE);
                        RemoveShardMapEntry(g_UpdateShardingGroupInfo.group[i]);
                        LWLockRelease(ShardMapLock);
                    }
                }                
                /* reset flag */
                g_GroupShardingMgr_DN->needLock = false;
            }
        
            g_UpdateShardingGroupInfo.nGroups = 0;
        }
        else
        {
            g_UpdateShardingGroupInfo.nGroups = 0;
        }
    }
}

void RegisterInvalidShmemShardMap(Oid group, int32 op)
{
    if (g_UpdateShardingGroupInfo.nGroups < MAX_SHARDING_NODE_GROUP)
    {
        g_UpdateShardingGroupInfo.group[g_UpdateShardingGroupInfo.nGroups]  = group;
        g_UpdateShardingGroupInfo.optype[g_UpdateShardingGroupInfo.nGroups] = op;
        g_UpdateShardingGroupInfo.nGroups++;        
    }
    else
    {
        elog(ERROR, "too many groups created");
    }

    // TODO:
    //hold_select_shard();
}

/*
 * ShardMapShmem In DN will only has one GroupShardInfo info.
 * Since DN only store its own group shardmap info. 
 */
void ShardMapShmemInit_CN(void)
{
    bool found;
    int  i = 0;
    GroupShardInfo *groupshard = NULL;
    char name[MAXPGPATH];
    HASHCTL        info;

    /* init hash table */
    info.keysize   = sizeof(GroupLookupTag);
    info.entrysize = sizeof(GroupLookupEnt);
    info.hash = tag_hash;

    g_GroupHashTab = ShmemInitHash("Group Sharding info look up",
                                  MAX_SHARDING_NODE_GROUP, 
                                  MAX_SHARDING_NODE_GROUP,
                                  &info,
                                  HASH_ELEM | HASH_FUNCTION);    

    if (!g_GroupHashTab)
    {
        elog(FATAL, "invalid shmem status when creating sharding hash ");
    }
    
    g_GroupShardingMgr = (ShardNodeGroupInfo *)ShmemInitStruct("Group shard mgr",
                                                                sizeof(ShardNodeGroupInfo),
                                                                &found);
    if (found)
    {
        elog(FATAL, "invalid shmem status when creating Group shard mgr ");
    }
    g_GroupShardingMgr->inited   = false;
    g_GroupShardingMgr->needLock = false;
    
    groupshard = (GroupShardInfo *)ShmemInitStruct("Group shard major",
                                                        MAXALIGN64(sizeof(GroupShardInfo)) + MAXALIGN64(sizeof(ShardMapItemDef)) * (SHARD_MAP_GROUP_NUM - 1),
                                                        &found);

    if (found)
    {
        elog(FATAL, "invalid shmem status when creating Group shard major ");
    }
    
    /* init first major group */
    groupshard->group                 = InvalidOid;
    groupshard->shardMapStatus      = SHMEM_SHRADMAP_STATUS_UNINITED;
    groupshard->shmemNumShardGroups = SHARD_MAP_GROUP_NUM;
    groupshard->shmemNumShards      = SHARD_MAP_SHARD_NUM;
    groupshard->shmemNumShardNodes  = 0;
    memset(groupshard->shmemshardnodes, 0X00, sizeof(groupshard->shmemshardnodes));
    memset(groupshard->shmemshardmap, 0X00, sizeof(ShardMapItemDef) * SHARD_MAP_GROUP_NUM);

    SpinLockInit(&g_GroupShardingMgr->lock[0]);
    g_GroupShardingMgr->used[0]     = false;
    g_GroupShardingMgr->members[0]  = groupshard;

    /* init extension group */
    for (i = FIRST_EXTENSION_GROUP; i < LAST_EXTENSION_GROUP; i++)
    {
        snprintf(name, MAXPGPATH, "Extension group %d", i);

        groupshard = (GroupShardInfo *)ShmemInitStruct(name,
                                                        MAXALIGN64(sizeof(GroupShardInfo)) + MAXALIGN64(sizeof(ShardMapItemDef)) * (EXTENSION_SHARD_MAP_GROUP_NUM - 1),
                                                        &found);
        if (found)
        {
            elog(FATAL, "invalid shmem status when creating %s ", name);
        }
        
        /* init first major group */
        groupshard->group            = InvalidOid;
        groupshard->shardMapStatus = SHMEM_SHRADMAP_STATUS_UNINITED;
        groupshard->shmemNumShardGroups = EXTENSION_SHARD_MAP_GROUP_NUM;
        groupshard->shmemNumShards      = EXTENSION_SHARD_MAP_SHARD_NUM;
        groupshard->shmemNumShardNodes  = 0;
        memset(groupshard->shmemshardnodes, 0X00, sizeof(groupshard->shmemshardnodes));
        memset(groupshard->shmemshardmap, 0X00, sizeof(ShardMapItemDef) * EXTENSION_SHARD_MAP_GROUP_NUM);

        SpinLockInit(&g_GroupShardingMgr->lock[i]);
        g_GroupShardingMgr->used[i]     = false;
        g_GroupShardingMgr->members[i]  = groupshard;
    }
}


void ShardMapShmemInit_DN(void)
{
    bool found;
    GroupShardInfo *groupshard = NULL;

    g_GroupShardingMgr_DN = (ShardNodeGroupInfo_DN *)ShmemInitStruct("DN Group shard mgr",
                                                                sizeof(ShardNodeGroupInfo_DN),
                                                                &found);
    if (found)
    {
        elog(FATAL, "invalid shmem status when creating Group shard mgr ");
    }
    g_GroupShardingMgr_DN->inited   = false;
    g_GroupShardingMgr_DN->needLock = false;
    
    groupshard = (GroupShardInfo *)ShmemInitStruct("Group shard major",
                                                        MAXALIGN64(sizeof(GroupShardInfo)) + MAXALIGN64(sizeof(ShardMapItemDef)) * (SHARD_MAP_GROUP_NUM - 1),
                                                        &found);

    if (found)
    {
        elog(FATAL, "invalid shmem status when creating Group shard major ");
    }
    
    /* init first major group */
    groupshard->group                 = InvalidOid;
    groupshard->shardMapStatus      = SHMEM_SHRADMAP_STATUS_UNINITED;
    groupshard->shmemNumShardGroups = SHARD_MAP_GROUP_NUM;
    groupshard->shmemNumShards      = SHARD_MAP_SHARD_NUM;
    groupshard->shmemNumShardNodes  = 0;
    memset(groupshard->shmemshardnodes, 0X00, sizeof(groupshard->shmemshardnodes));
    memset(groupshard->shmemshardmap, 0X00, sizeof(ShardMapItemDef) * SHARD_MAP_GROUP_NUM);

    SpinLockInit(&g_GroupShardingMgr_DN->lock);
    g_GroupShardingMgr_DN->used     = false;
    g_GroupShardingMgr_DN->members  = groupshard;

    

    /* DN need to construct g_DatanodeShardgroupBitmap */
    g_DatanodeShardgroupBitmap = (Bitmapset *) ShmemInitStruct("Data node bitmap",
                                                         MAXALIGN64(SHARD_TABLE_BITMAP_SIZE),
                                                      &found);

    if(!found)
    {
        g_DatanodeShardgroupBitmap = bms_make((char *)g_DatanodeShardgroupBitmap, SHARD_MAP_GROUP_NUM);
    }    
    
}


bool   MajorShardAlreadyExist(void)
{
    SyncShardMapList(false);    
    if (IS_PGXC_COORDINATOR)
    {
        return g_GroupShardingMgr->used[MAJOR_SHARD_NODE_GROUP];
    }
    else if (IS_PGXC_DATANODE)
    {
        return g_GroupShardingMgr_DN->used;
    }

    return false;/* should not reach here */
}

void SyncShardMapList(bool force)
{
    if(isRestoreMode)
    {
        return;
    }

    if (IS_PGXC_COORDINATOR)
    {
        SyncShardMapList_CN(force);
    }
    else if (IS_PGXC_DATANODE)
    {
        SyncShardMapList_DN(force);
    }
}

static void SyncShardMapList_CN(bool force)
{
    if (!IS_PGXC_COORDINATOR)
    {
        elog(ERROR, "SyncShardMapList_CN should only be called in coordinator");
    }

    if (!force && g_UpdateShardingGroupInfo.nGroups == 0 && g_GroupShardingMgr->inited)
    {
        return;
    }
    /* tell others use lock to access shard map */
    g_GroupShardingMgr->needLock = true;

    LWLockAcquire(ShardMapLock, LW_EXCLUSIVE);    

	/* in case of race conditions */
	if (!force && g_UpdateShardingGroupInfo.nGroups == 0 && g_GroupShardingMgr->inited)
	{
		LWLockRelease(ShardMapLock);
		g_GroupShardingMgr->needLock = false;
		return;
	}
    
	g_GroupShardingMgr->inited = false;
    SyncShardMapList_Node_CN();
    g_GroupShardingMgr->inited = true;
        
    LWLockRelease(ShardMapLock);
    
    /*reset flag*/
    g_GroupShardingMgr->needLock = false;
}

static void SyncShardMapList_DN(bool force)
{
    if (!IsPostmasterEnvironment)
    {
        return;
    }
    
    if (!IS_PGXC_DATANODE)
    {
        elog(ERROR, "SyncShardMapList_DN should only be called in datanode");
    }

    if (!force && g_UpdateShardingGroupInfo.nGroups == 0 && g_GroupShardingMgr_DN->inited)
    {
        if (!g_GroupShardingMgr_DN->used)
        {
            elog(LOG, "SyncShardMapList_DN GroupShardingMgr is not inited yet.");
        }
        return;
    }
    /* tell others use lock to access shard map */
    g_GroupShardingMgr_DN->needLock = true;

	LWLockAcquire(ShardMapLock, LW_EXCLUSIVE);

	/* in case of race conditions */
	if (!force && g_UpdateShardingGroupInfo.nGroups == 0 && g_GroupShardingMgr_DN->inited)
	{
		LWLockRelease(ShardMapLock);
		g_GroupShardingMgr_DN->needLock = false;
		return;
	}
	g_GroupShardingMgr_DN->inited = false;
    
    g_GroupShardingMgr_DN->inited = SyncShardMapList_Node_DN();
        
    LWLockRelease(ShardMapLock);
    
    /*reset flag*/
    g_GroupShardingMgr_DN->needLock = false;
}


/* read shard map from system table and fill in the share memory */
/*
 * On datanode, if pgxc_shard_map tuple's primarycopy is exactly the datanode itself,
 * then this shard is visible, and should be a member of g_DatanodeShardgroupBitmap.
 */

static void SyncShardMapList_Node_CN(void)
{
    int32 shard_mgr = INVALID_SHARDMAP_ID;        
    HeapTuple  oldtup;
    Relation shardrel;
    Form_pgxc_shard_map  pgxc_shard;
    Form_pgxc_shard_map  firsttup = NULL;    /* flag for invalid null shardmap. */
    ScanKeyData          skey;
    SysScanDesc           sysscan;
    Relation              rel;

    HeapScanDesc          scan;    
    HeapTuple             tuple;
    Oid                  groupoid;
    
    /* cross check to ensure one node can only be in one node group */    
    rel = heap_open(PgxcGroupRelationId, AccessShareLock);

    scan = heap_beginscan_catalog(rel, 0, NULL);
    while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
    {        
        groupoid = HeapTupleGetOid(tuple);

        if (is_group_sharding_inited(groupoid))
        {
            /* If the group sharding has not been created. */
            if (!GroupShardingTabExist(groupoid, false, &shard_mgr))
            {
                /* Now to create a new shard mapping mgr */
                shard_mgr = AllocShardMap(true, groupoid);
                if (INVALID_SHARDMAP_ID == shard_mgr)
                {
                    elog(ERROR, "too many sharding group defined");
                }
            }            
            
            /* build sharding table */
            firsttup  = NULL;
            shardrel  = heap_open(PgxcShardMapRelationId, AccessShareLock);
            ScanKeyInit(&skey,
                Anum_pgxc_shard_map_nodegroup,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(groupoid));
            
            sysscan = systable_beginscan(shardrel,
                                      PgxcShardMapGroupIndexId, 
                                      true,
                                      NULL, 1, &skey);            
            
            while(HeapTupleIsValid(oldtup = systable_getnext(sysscan)))
            {
                if(NULL == firsttup)
                {
                    if (NULL == oldtup)
                    {
                        elog(ERROR, "invalid status, group:%u has no sharding info", groupoid);
                    }
                    
                    firsttup = (Form_pgxc_shard_map)GETSTRUCT(oldtup);    
                }
                
                pgxc_shard = (Form_pgxc_shard_map)GETSTRUCT(oldtup);
                InsertShardMap_CN(shard_mgr, pgxc_shard);                
            }
            systable_endscan(sysscan);
            heap_close(shardrel, AccessShareLock);    
            ShardMapInitDone_CN(shard_mgr, groupoid, false);            
        }
    }
    heap_endscan(scan);
    heap_close(rel, AccessShareLock);
}
static bool SyncShardMapList_Node_DN(void)
{        
    HeapTuple  oldtup;
    Relation shardrel;
    Form_pgxc_shard_map  pgxc_shard;
    ScanKeyData          skey;
    SysScanDesc           sysscan;
    Oid                     curr_groupoid = InvalidOid;
    Oid                  self_node_oid = InvalidOid;

    if (!IS_PGXC_DATANODE)
    {
        elog(ERROR, "SyncShardMapList_Node_DN should only be called in datanode");
        return false;
    }

	self_node_oid = get_pgxc_nodeoid_extend(PGXCNodeName, PGXCMainClusterName);
	if (InvalidOid == self_node_oid)
	{
		elog(LOG, "SyncShardMapList_Node_DN failed to get nodeoid, node:%s", PGXCNodeName);
		return false;
	}
	curr_groupoid = GetGroupOidByNode(self_node_oid);
	if (InvalidOid == curr_groupoid)
	{
		elog(LOG, "SyncShardMapList_Node_DN failed to get groupoid, node:%s, nodeoid:%d", PGXCNodeName, self_node_oid);
		return false;
	}			
	
	if (is_group_sharding_inited(curr_groupoid))
	{
		bms_clear(g_DatanodeShardgroupBitmap);
		
		/* 
		 * If sharding of the group has not been inited, or this sharding map is in use but 
		 * store overdue information, possibly caused by group syncing backend crashing right
		 * before the shmem sync.
		 */
		if (!g_GroupShardingMgr_DN->used || curr_groupoid != g_GroupShardingMgr_DN->members->group)
		{
			/* 
			 * Datanodes can only be in one node group, so we save the effort of
			 * removing entry and skip right into resetting the mgr.
			 */
			g_GroupShardingMgr_DN->members->shardMapStatus = SHMEM_SHRADMAP_STATUS_LOADING;
			SpinLockAcquire(&g_GroupShardingMgr_DN->lock);
			g_GroupShardingMgr_DN->members->group = curr_groupoid;
			g_GroupShardingMgr_DN->used = true;
			SpinLockRelease(&g_GroupShardingMgr_DN->lock);
		}
			
		shardrel = heap_open(PgxcShardMapRelationId, AccessShareLock);
		ScanKeyInit(&skey,
			Anum_pgxc_shard_map_nodegroup,
			BTEqualStrategyNumber, F_OIDEQ,
			ObjectIdGetDatum(curr_groupoid));

		sysscan = systable_beginscan(shardrel,
								  PgxcShardMapGroupIndexId, 
								  true,
								  NULL, 1, &skey);
		
		while(HeapTupleIsValid(oldtup = systable_getnext(sysscan)))
		{
			pgxc_shard = (Form_pgxc_shard_map)GETSTRUCT(oldtup);
			InsertShardMap_DN(pgxc_shard);

			/* 
			 * If node is DN AND pgxc_shard_map tuple's primary copy is itself,
			 * Add this shardid to bitmap.
			 */
			BuildDatanodeVisibilityMap(pgxc_shard, self_node_oid);
		}
		systable_endscan(sysscan);
		heap_close(shardrel, AccessShareLock);	
		ShardMapInitDone_DN(curr_groupoid, false);					
	}
	else
	{
		elog(LOG, "SyncShardMapList_Node_DN group %d is not inited.", curr_groupoid);
		return false;
	}

    return true;
}


bool GroupShardingTabExist(Oid group, bool lock, int *group_index)
{
    bool           found;
    GroupLookupTag tag;
    GroupLookupEnt *ent;
    tag.group =  group;

    if (!IS_PGXC_COORDINATOR)
    {
        elog(ERROR, "GroupShardingTabExist should only be called in coordinator");
    }

    if (lock)
    {
        LWLockAcquire(ShardMapLock, LW_SHARED);
    }
    ent = (GroupLookupEnt*)hash_search(g_GroupHashTab, (void *) &tag, HASH_FIND, &found);    
    if (lock)
    {
        LWLockRelease(ShardMapLock);
    }

    if(found && group_index)
    {
        *group_index = (int32)ent->shardIndex;
    }
    
    return found;
}

static void InsertShardMap_CN(int32 map, Form_pgxc_shard_map record)
{
    if (!IS_PGXC_COORDINATOR)
    {
        elog(ERROR, "InsertShardMap_CN should only be called in coordinator");
    }

    if (map != INVALID_SHARDMAP_ID)
    {
        int32 nodeindex;        
        if (record->shardgroupid <= g_GroupShardingMgr->members[map]->shmemNumShardGroups && record->shardgroupid >= 0)
        {
            char node_type = get_pgxc_nodetype(record->primarycopy);
            nodeindex = PGXCNodeGetNodeId(record->primarycopy, &node_type);
            if (nodeindex < 0)
            {
				elog(ERROR,
					"InsertShardMap_CN get node:%u for index failed",
					record->primarycopy);
            }
            
            g_GroupShardingMgr->members[map]->shmemshardmap[record->shardgroupid].primarycopy  = record->primarycopy;
            g_GroupShardingMgr->members[map]->shmemshardmap[record->shardgroupid].shardgroupid = record->shardgroupid;
            g_GroupShardingMgr->members[map]->shmemshardmap[record->shardgroupid].nodeindex       = nodeindex;
        }
        else
        {
			elog(ERROR,
				"invalid pgxc_shard_map record with shardgroupid: %d, map %d "
				"and shmemNum: %d",
				record->shardgroupid, map,
				g_GroupShardingMgr->members[map]->shmemNumShardGroups);
        }        
    }
}


static void InsertShardMap_DN(Form_pgxc_shard_map record)
{
    int32 nodeindex;        
    if (!IS_PGXC_DATANODE)
    {
        elog(ERROR, "InsertShardMap_DN should only be called in datanode");
    }
    
    if (record->shardgroupid <= g_GroupShardingMgr_DN->members->shmemNumShardGroups && record->shardgroupid >= 0)
    {
        char node_type = get_pgxc_nodetype(record->primarycopy);
        nodeindex = PGXCNodeGetNodeId(record->primarycopy, &node_type);
        if (nodeindex < 0)
        {
			elog(ERROR,
				"InsertShardMap_DN get node:%u for index failed",
				record->primarycopy);
        }
        
        g_GroupShardingMgr_DN->members->shmemshardmap[record->shardgroupid].primarycopy  = record->primarycopy;
        g_GroupShardingMgr_DN->members->shmemshardmap[record->shardgroupid].shardgroupid = record->shardgroupid;
        g_GroupShardingMgr_DN->members->shmemshardmap[record->shardgroupid].nodeindex     = nodeindex;
    }
    else
    {
		elog(ERROR,
			"InsertShardMap_DN has invalid pgxc_shard_map record with shardgroupid: "
			"%d and shmemNum: %d",
			record->shardgroupid, g_GroupShardingMgr_DN->members->shmemNumShardGroups);
    }
}

static void ShardMapInitDone_CN(int32 map, Oid group, bool need_lock)
{// #lizard forgives
    bool           found = false;
    bool           dup = false;
    int32          i;
    int32          j;
    int32          nodeindex = 0;
    int32          nodeCnt = 0;
    int32           maxNodeIndex = 0;
    ShardMapItemDef item;
    GroupLookupEnt *ent       = NULL;
    GroupLookupTag tag;
    tag.group =  group;

    if(!IS_PGXC_COORDINATOR)
    {
        elog(ERROR, "ShardMapInitDone_CN should only be called in coordinator");
        return;
    }

    if(map < 0 || map >= MAX_SHARDING_NODE_GROUP)
    {
        elog(ERROR, "group index %d is invalid.", map);
        return;
    }
    
    if(group != g_GroupShardingMgr->members[map]->group)
    {
        elog(ERROR, "groupoid in slot %d oid:%u is not group %d", map, g_GroupShardingMgr->members[map]->group, group);
        return;
    }

    if (need_lock)
    {
        LWLockAcquire(ShardMapLock, LW_EXCLUSIVE);
    }

    /* Add group node to hashmap. */
    g_GroupShardingMgr->members[map]->shardMapStatus = SHMEM_SHRADMAP_STATUS_USING;        
    ent = (GroupLookupEnt*)hash_search(g_GroupHashTab, (void *) &tag, HASH_ENTER, &found);
    if (!ent)
    {
        /* Failed to add the hash table entry. */        
        /* Release the shard map entry. */
        SpinLockAcquire(&g_GroupShardingMgr->lock[map]);
        g_GroupShardingMgr->used[map] = false;
        SpinLockRelease(&g_GroupShardingMgr->lock[map]);

        if (need_lock)
        {
            LWLockRelease(ShardMapLock);
        }
        elog(ERROR, "ShardMapInitDone_CN corrupted shared hash table");
    }
    ent->shardIndex = map;    

    {
            
        /* init shard group nodes of the shard map */
        nodeCnt = 0;
        memset(g_GroupShardingMgr->members[map]->shmemshardnodes, 0Xff, sizeof(g_GroupShardingMgr->members[map]->shmemshardnodes));

        for (i = 0; i < g_GroupShardingMgr->members[map]->shmemNumShardGroups; i++)
        {
            dup = false;
            item = g_GroupShardingMgr->members[map]->shmemshardmap[i];
            for (j = 0; j < nodeCnt; j++)
            {
                if (g_GroupShardingMgr->members[map]->shmemshardnodes[j] == item.nodeindex)
                {
                    dup = true;
                    break;
                }
            }
            
            /* all node index are in shmemNumShardNodes */
            if (!dup)
            {
                /* store the max global node index. */
                maxNodeIndex = maxNodeIndex < item.nodeindex ? item.nodeindex :  maxNodeIndex;
                g_GroupShardingMgr->members[map]->shmemshardnodes[nodeCnt] = item.nodeindex;
                nodeCnt++;
            }
        }

        g_GroupShardingMgr->members[map]->shmemNumShardNodes = nodeCnt;
        g_GroupShardingMgr->members[map]->shardMaxGlblNdIdx  = maxNodeIndex + 2; /* 1 is enough, just in case*/

        qsort(g_GroupShardingMgr->members[map]->shmemshardnodes, 
                nodeCnt, 
                sizeof(int32), 
                cmp_int32);
        
        /*init all element to invalid big. */
        memset(g_GroupShardingMgr->members[map]->shmemNodeMap, 0XFF, sizeof(int32) * TBASE_MAX_DATANODE_NUMBER);
        for (i = 0; i < nodeCnt; i++)
        {
            nodeindex = g_GroupShardingMgr->members[map]->shmemshardnodes[i];
            /* Store the group native index of the node into the node global map. */
            g_GroupShardingMgr->members[map]->shmemNodeMap[nodeindex] = i;
        }
    }
    g_GroupShardingMgr->members[map]->shardMapStatus = SHMEM_SHRADMAP_STATUS_USING;
    
    if (need_lock)
    {
        LWLockRelease(ShardMapLock);
    }
}


static void ShardMapInitDone_DN(Oid group, bool need_lock)
{
	bool           dup = false;
	int32		   maxNodeIndex = 0;
	int32          i;
	int32          j;
	int32          nodeindex = 0;
	int32          nodeCnt = 0;
	ShardMapItemDef item;

	if(!IS_PGXC_DATANODE)
	{
		elog(ERROR, "ShardMapInitDone_DN should only be called in datanode");
		return;
	}
	
	if(group != g_GroupShardingMgr_DN->members->group)
	{
		/* PANIC here is to reset shmem, although a more elegant way should be provided by ShardMapShmem AM */
		elog(PANIC, "groupoid %d in mgr is not group %d", g_GroupShardingMgr_DN->members->group, group);
		return;
	}

    if (need_lock)
    {
        LWLockAcquire(ShardMapLock, LW_EXCLUSIVE);
    }
    
    /* init shard group nodes of the shard map */
    nodeCnt = 0;
    memset(g_GroupShardingMgr_DN->members->shmemshardnodes, 0Xff, sizeof(g_GroupShardingMgr_DN->members->shmemshardnodes));

    for (i = 0; i < g_GroupShardingMgr_DN->members->shmemNumShardGroups; i++)
    {
        dup = false;
        item = g_GroupShardingMgr_DN->members->shmemshardmap[i];
        for (j = 0; j < nodeCnt; j++)
        {
            if (g_GroupShardingMgr_DN->members->shmemshardnodes[j] == item.nodeindex)
            {
                dup = true;
                break;
            }
        }
        
        /* all node index are in shmemNumShardNodes */
        if (!dup)
        {
            g_GroupShardingMgr_DN->members->shmemshardnodes[nodeCnt] = item.nodeindex;
            /* store the max global node index. */
            maxNodeIndex = maxNodeIndex < item.nodeindex ? item.nodeindex :  maxNodeIndex;
            nodeCnt++;
        }
    }
    
    g_GroupShardingMgr_DN->members->shmemNumShardNodes = nodeCnt;
    g_GroupShardingMgr_DN->members->shardMaxGlblNdIdx  = maxNodeIndex + 2; /* 1 is enough, just in case*/
    qsort(g_GroupShardingMgr_DN->members->shmemshardnodes, 
            nodeCnt, 
            sizeof(int32), 
            cmp_int32);
    
    /*init all element to invalid big. */
    memset(g_GroupShardingMgr_DN->members->shmemNodeMap, 0XFF, sizeof(int32) * TBASE_MAX_DATANODE_NUMBER);
    for (i = 0; i < nodeCnt; i++)
    {
        nodeindex = g_GroupShardingMgr_DN->members->shmemshardnodes[i];
        /* Store the group native index of the node into the node global map. */
        g_GroupShardingMgr_DN->members->shmemNodeMap[nodeindex] = i;
    }
    g_GroupShardingMgr_DN->members->shardMapStatus = SHMEM_SHRADMAP_STATUS_USING;
    
    if (need_lock)
    {
        LWLockRelease(ShardMapLock);
    }
}


static int cmp_int32(const void *p1, const void *p2)
{
    int i1;
    int i2;
    
    if(!p1 || !p2)
        elog(ERROR,"cmp_int32:args cannot be null");

    i1 = *(int32 *)p1;
    i2 = *(int32 *)p2;

    if(i1 == i2) return 0;
    if(i1 < i2) return -1;

    return 1;
}
/* Alloc a shard map entry in shared memory. */
static int32 AllocShardMap(bool extend, Oid groupoid)
{
    int32 i;
    int32 index = INVALID_SHARDMAP_ID;

    if(!IS_PGXC_COORDINATOR)
    {
        elog(ERROR, "AllocShardMap should only be called in coordinator");
        return INVALID_SHARDMAP_ID;
    }
    
    if (!extend)
    {
        SpinLockAcquire(&g_GroupShardingMgr->lock[MAJOR_SHARD_NODE_GROUP]);
        if (!g_GroupShardingMgr->used[MAJOR_SHARD_NODE_GROUP])
        {
            index = MAJOR_SHARD_NODE_GROUP;
            g_GroupShardingMgr->used[MAJOR_SHARD_NODE_GROUP] = true;
            g_GroupShardingMgr->members[MAJOR_SHARD_NODE_GROUP]->group = groupoid;
        }    
        SpinLockRelease(&g_GroupShardingMgr->lock[MAJOR_SHARD_NODE_GROUP]);
    }
    else
    {
        for (i = FIRST_EXTENSION_GROUP; i < LAST_EXTENSION_GROUP; i++)
        {
            SpinLockAcquire(&g_GroupShardingMgr->lock[i]);
            if (!g_GroupShardingMgr->used[i])
            {
                g_GroupShardingMgr->used[i] = true;
                g_GroupShardingMgr->members[i]->group = groupoid;
                index = i;
                SpinLockRelease(&g_GroupShardingMgr->lock[i]);
                break;
            }
            SpinLockRelease(&g_GroupShardingMgr->lock[i]);
        }
    }
    return index;
}

Size ShardMapShmemSize(void)
{
    if(IS_PGXC_COORDINATOR || IS_PGXC_DATANODE)
    {
        return ShardMapShmemSize_Node_CN();
    }
    else if (IS_PGXC_DATANODE)
    {
        return ShardMapShmemSize_Node_DN();
    }
    else
    {
        return 0;
    }
}

static Size ShardMapShmemSize_Node_CN(void)
{
    Size size;
    
    size = 0;    
    /* hash table size */
    // hash_estimate_size(MAX_SHARDING_NODE_GROUP, sizeof(GroupLookupEnt));
    
    /* management info */
    size = add_size(size, MAXALIGN64(sizeof(ShardNodeGroupInfo)));

    /* struct self */
    size = add_size(size, MAXALIGN64(sizeof(GroupShardInfo)) * MAX_SHARDING_NODE_GROUP);

    /* non extension router info */
    size = add_size(size, mul_size(MAXALIGN64(sizeof(ShardMapItemDef)), SHARD_MAP_SHARD_NUM));

    /* extension router info */
    size = add_size(size, mul_size(mul_size(MAXALIGN64(sizeof(ShardMapItemDef)), EXTENSION_SHARD_MAP_GROUP_NUM), (LAST_EXTENSION_GROUP - FIRST_EXTENSION_GROUP)));

    /* shardmap bitmap info. Only used in datanode */
    size = add_size(size, MAXALIGN64(SHARD_TABLE_BITMAP_SIZE));    

    /* hash table, here just double the element size, in case of memory corruption */
    size = add_size(size, mul_size(MAX_SHARDING_NODE_GROUP * 2 , MAXALIGN64(sizeof(GroupLookupEnt))));
    return size;
}

static Size ShardMapShmemSize_Node_DN(void)
{
    Size size;
    
    size = 0;    
    /* hash table size */
    // hash_estimate_size(MAX_SHARDING_NODE_GROUP, sizeof(GroupLookupEnt));
    
    /* management info */
    size = add_size(size, MAXALIGN64(sizeof(ShardNodeGroupInfo_DN)));

    /* struct self */
    size = add_size(size, MAXALIGN64(sizeof(GroupShardInfo)));

    /* non extension router info */
    size = add_size(size, mul_size(MAXALIGN64(sizeof(ShardMapItemDef)), SHARD_MAP_SHARD_NUM));

    /* shardmap bitmap info. Only used in datanode */
    size = add_size(size, MAXALIGN64(SHARD_TABLE_BITMAP_SIZE));    

    return size;
}

#if 0
List* ShardMapRouter(Oid group, Oid relation, Oid type, Datum dvalue, RelationAccessType accessType)
{    
    long         hashvalue                    = 0;

    /* not the key value, use common map strategy */
    hashvalue = compute_hash(type, dvalue, LOCATOR_TYPE_SHARD);

    /* no second distribute key, trade as hot data*/
    return list_make1_int(GetNodeIndexByHashValue(group, hashvalue));
}
#endif
int32  GetNodeIndexByHashValue(Oid group, long hashvalue)
{// #lizard forgives    
    int            shardIdx;
    int            nodeIdx = 0;
    bool           needLock = false;
    bool           found;
    GroupLookupTag tag;
    GroupLookupEnt *ent;
    ShardMapItemDef *shardgroup;
    int slot = 0;
    
    if(IS_PGXC_COORDINATOR && !OidIsValid(group))
    {
        elog(PANIC, "[GetNodeIndexByHashValue]group oid can not be invalid.");
    }

    if (IS_PGXC_COORDINATOR)
    {
        needLock  = g_GroupShardingMgr->needLock;
        if (needLock)
        {
            LWLockAcquire(ShardMapLock, LW_SHARED);
        }

        tag.group =  group;
        ent = (GroupLookupEnt*)hash_search(g_GroupHashTab, (void *) &tag, HASH_FIND, &found);            
        if (!found)
        {
            elog(ERROR , "no shard group of %u found", group);
        }

        slot = ent->shardIndex;

        shardIdx     = abs(hashvalue) % (g_GroupShardingMgr->members[slot]->shmemNumShards);
        shardgroup   = &g_GroupShardingMgr->members[slot]->shmemshardmap[shardIdx];
        nodeIdx      = shardgroup->nodeindex;
    }
    else if (IS_PGXC_DATANODE)
    {
        needLock  = g_GroupShardingMgr_DN->needLock;
        if (needLock)
        {
            LWLockAcquire(ShardMapLock, LW_SHARED);
        }

        shardIdx     = abs(hashvalue) % (g_GroupShardingMgr_DN->members->shmemNumShards);
		if (g_ShardMapValid)
			shardgroup = &g_ShardMap[shardIdx];
		else
        shardgroup   = &g_GroupShardingMgr_DN->members->shmemshardmap[shardIdx];
        nodeIdx      = shardgroup->nodeindex;
    }

    if (needLock)
    {
        LWLockRelease(ShardMapLock);
    }
    return nodeIdx;
}

/* Get node index map of group. */
void  GetGroupNodeIndexMap(Oid group, int32 *map)
{// #lizard forgives
    bool           needLock = false;
    bool           found;
    GroupLookupTag tag;
    GroupLookupEnt *ent;
    int slot = 0;
    
    if(!OidIsValid(group))
    {
        elog(ERROR, "GetGroupNodeIndexMap group oid invalid.");
    }

    if (IS_PGXC_COORDINATOR)
    {
        needLock  = g_GroupShardingMgr->needLock;
        if (needLock)
        {
            LWLockAcquire(ShardMapLock, LW_SHARED);
        }

        tag.group =  group;
        ent = (GroupLookupEnt*)hash_search(g_GroupHashTab, (void *) &tag, HASH_FIND, &found);            
        if (!found)
        {
            elog(ERROR , "no shard group of %u found", group);
        }

        slot = ent->shardIndex;
        memcpy(map, g_GroupShardingMgr->members[slot]->shmemNodeMap, sizeof(int32) * g_GroupShardingMgr->members[slot]->shardMaxGlblNdIdx);
    }
    else if (IS_PGXC_DATANODE)
    {
        if (group != g_GroupShardingMgr_DN->members->group)
        {
            elog(ERROR, "GetGroupNodeIndexMap group oid:%u is not the stored group:%u.", group, g_GroupShardingMgr_DN->members->group);    
        }
        
        needLock  = g_GroupShardingMgr_DN->needLock;
        if (needLock)
        {
            LWLockAcquire(ShardMapLock, LW_SHARED);
        }
        memcpy(map, g_GroupShardingMgr_DN->members->shmemNodeMap, sizeof(int32) * g_GroupShardingMgr_DN->members->shardMaxGlblNdIdx);
    }

    if (needLock)
    {
        LWLockRelease(ShardMapLock);
    }
}



void GetShardNodes(Oid group, int32 ** nodes, int32 *num_nodes, bool *isextension)
{
    if (IS_PGXC_COORDINATOR)
    {
        GetShardNodes_CN(group, nodes, num_nodes, isextension);
    }
    else if (IS_PGXC_DATANODE)
    {
        GetShardNodes_DN(group, nodes, num_nodes, isextension);
    }
}



static void GetShardNodes_CN(Oid group, int32 ** nodes, int32 *num_nodes, bool *isextension)
{// #lizard forgives
    bool           found;
    bool           needLock;
    int             nNodes = 0;
    GroupLookupTag tag;
    GroupLookupEnt *ent;
    tag.group =  group;        

    if (!IS_PGXC_COORDINATOR)
    {
        elog(ERROR, "GetShardNodes should only be called in coordinator");
    }
    

    SyncShardMapList(false);

    needLock  = g_GroupShardingMgr->needLock;
    if (needLock)
    {
        LWLockAcquire(ShardMapLock, LW_SHARED);
    }
    
    ent = (GroupLookupEnt*)hash_search(g_GroupHashTab, (void *) &tag, HASH_FIND, &found);            
    if (!found)
    {
        elog(ERROR , "corrupted catalog, no shard group of %u found", group);
    }

    nNodes = g_GroupShardingMgr->members[ent->shardIndex]->shmemNumShardNodes;
    
    if(num_nodes)
        *num_nodes = nNodes;    
    
    if(nodes)
    {
        *nodes = (int32 *)palloc0((nNodes) * sizeof(int32));
        memcpy((char*)*nodes, (char*)&(g_GroupShardingMgr->members[ent->shardIndex]->shmemshardnodes), (nNodes) * sizeof(int32));
    }
    
    if(isextension)
    {
        if (g_GroupShardingMgr->members[ent->shardIndex]->shmemNumShards == SHARD_MAP_SHARD_NUM)
            *isextension = false;
        else if(g_GroupShardingMgr->members[ent->shardIndex]->shmemNumShards == EXTENSION_SHARD_MAP_SHARD_NUM)
            *isextension= true;
        else
            elog(ERROR, "shards(%d) of group is invalid ", g_GroupShardingMgr->members[ent->shardIndex]->shmemNumShards);
    }
    
    if (needLock)
    {
        LWLockRelease(ShardMapLock);
    }
}

static void GetShardNodes_DN(Oid group, int32 ** nodes, int32 *num_nodes, bool *isextension)
{// #lizard forgives
    bool           needLock;
    int             nNodes = 0;    


    if (!IS_PGXC_DATANODE)
    {
        elog(ERROR, "InsertShardMap_DN should only be called in datanode");
    }
    

    SyncShardMapList(false);

    needLock  = g_GroupShardingMgr_DN->needLock;
    if (needLock)
    {
        LWLockAcquire(ShardMapLock, LW_SHARED);
    }

    if (!g_GroupShardingMgr_DN->used || g_GroupShardingMgr_DN->members->group != group)
    {
        elog(ERROR , "[GetShardNodes_DN]corrupted catalog, no shard group of %u found", group);
    }
    
    nNodes = g_GroupShardingMgr_DN->members->shmemNumShardNodes;
    
    if(num_nodes)
        *num_nodes = nNodes;    
    
    if(nodes)
    {
        *nodes = (int32 *)palloc0((nNodes) * sizeof(int32));
        memcpy((char*)*nodes, (char*)&(g_GroupShardingMgr_DN->members->shmemshardnodes), (nNodes) * sizeof(int32));
    }
    
    if(isextension)
    {
        if (g_GroupShardingMgr_DN->members->shmemNumShards == SHARD_MAP_SHARD_NUM)
            *isextension = false;
        else if(g_GroupShardingMgr_DN->members->shmemNumShards == EXTENSION_SHARD_MAP_SHARD_NUM)
            *isextension= true;
        else
            elog(ERROR, "shards(%d) of group is invalid ", g_GroupShardingMgr_DN->members->shmemNumShards);
    }
    
    if (needLock)
    {
        LWLockRelease(ShardMapLock);
    }
}


void PrepareMoveData(MoveDataStmt* stmt)
{// #lizard forgives
    Oid       group_oid;
    Oid        *dnoids = NULL;
    int     ndns = 0;
    Value *fromvalue;
    Value *tovalue;    
    int i;
    bool found;

    fromvalue = &(stmt->from_node->val);
    tovalue = &(stmt->to_node->val);

    if (IsA(fromvalue,String))
    {
        stmt->fromid = get_pgxc_nodeoid(strVal(fromvalue));
        if(!OidIsValid(stmt->fromid))
        {
            elog(ERROR,"datanode[%s] is not exist",strVal(fromvalue));
        }
    }
    else if(IsA(fromvalue,Integer))
    {
        stmt->fromid = intVal(fromvalue);
    }
    else
    {
        elog(ERROR, "PgxcMoveData error");
    }

    
    if (IsA(tovalue,String))
    {
        stmt->toid = get_pgxc_nodeoid(strVal(tovalue));
        if(!OidIsValid(stmt->toid))
        {
            elog(ERROR,"datanode[%s] is not exist",strVal(tovalue));
        }
    }
    else if(IsA(tovalue,Integer))
    {
        stmt->toid = intVal(tovalue);
    }
    else
    {
        elog(ERROR, "PgxcMoveData error");
    }

    
    group_oid = get_pgxc_groupoid(strVal(stmt->group));
    if (!OidIsValid(group_oid))
    {
        elog(ERROR, "group with name:%s not found", strVal(stmt->group));
    }

    /* check if the group contains this two datanodes */
    ndns = get_pgxc_groupmembers(group_oid, &dnoids);

    if(ndns <= 0)
    {
        elog(ERROR, "group %s dose not contain datanode %s or datanode %s", strVal(stmt->group), strVal(fromvalue), strVal(tovalue));
    }

    found = false;
    for(i = 0; i < ndns; i++)
    {
        if(stmt->fromid == dnoids[i])
            found = true;
    }
    if(!found)
    {
        elog(ERROR, "group %s dose not contain datanode %s", strVal(stmt->group), strVal(fromvalue));
    }

    found = false;
    for(i = 0; i < ndns; i++)
    {
        if(stmt->toid== dnoids[i])
            found = true;
    }
    if(!found)
    {
        elog(ERROR, "group %s dose not contain datanode %s", strVal(stmt->group), strVal(tovalue));
    }

    //GetShardNodes(group_oid, NULL, NULL, &stmt->isextension);
    SyncShardMapList(false);
    
    switch (stmt->strategy)
    {
        case MOVE_DATA_STRATEGY_SHARD:
            {
                //elog(ERROR,"shard-based data moving is not supported in coordinator.");
                ListCell     *lc;
                A_Const     *con;
                Value        *val;
                int            shard;
                int            shard_idx = 0;

                stmt->num_shard = list_length(stmt->shards);
                if(list_length(stmt->shards) == 0)
                    elog(ERROR, "shard list must be assigned");
                stmt->arr_shard = palloc0(stmt->num_shard * sizeof(int32));
                foreach(lc,stmt->shards)
                {
                    con = (A_Const *)lfirst(lc);
                    val = &(con->val);
                    shard = intVal(val);
                    if(shard < 0)
                        elog(ERROR, "%d shard is invalid, shard to move must be greater than or equal with zero", shard);
                    stmt->arr_shard[shard_idx++] = shard;
                }

                qsort(stmt->arr_shard, 
                        stmt->num_shard, 
                        sizeof(int32), 
                        cmp_int32);
                stmt->split_point = -1;
            }
            break;
        case MOVE_DATA_STRATEGY_NODE:
            {
                stmt->arr_shard = decide_shardgroups_to_move(group_oid, stmt->fromid, &stmt->num_shard);
                stmt->split_point = stmt->arr_shard[stmt->num_shard - 1];
                if(NodeHasShard(stmt->toid))
                {
                    elog(ERROR, "shard must be moved to a new node which is ready for data but no shard");
                }
            }
            break;
        case  MOVE_DATA_STRATEGY_AT:
            elog(ERROR, "Move Strategy MOVE_DATA_STRATEGY_AT is not supported in coordinator");
            break;
        default:
            elog(ERROR, "Invalid Move Stratety %d", stmt->strategy);
    }

    pfree(dnoids);
}

static int* 
decide_shardgroups_to_move(Oid group, Oid fromnode, int *shardgroup_num)
{
    Relation    shardmapRel;
    ScanKeyData skey[2];
    SysScanDesc scan;
    HeapTuple    tuple;
    Form_pgxc_shard_map pgxc_shard;
    List * tuplist;
    int *shardgroups;
    int i;

    tuplist = NULL;
    
    shardmapRel = heap_open(PgxcShardMapRelationId, AccessShareLock);
    
    ScanKeyInit(&skey[0],
                    Anum_pgxc_shard_map_nodegroup,
                    BTEqualStrategyNumber, F_OIDEQ,
                    ObjectIdGetDatum(group));
    
    ScanKeyInit(&skey[1],
                    Anum_pgxc_shard_map_primarycopy,
                    BTEqualStrategyNumber, F_OIDEQ,
                    ObjectIdGetDatum(fromnode));    
    
    scan = systable_beginscan(shardmapRel,
                                PgxcShardMapGroupIndexId,true,
                                NULL,
                                2,
                                skey);

    tuple = systable_getnext(scan);

    while(HeapTupleIsValid(tuple))
    {
        pgxc_shard = (Form_pgxc_shard_map) GETSTRUCT(tuple);
        tuplist = lappend_int(tuplist,pgxc_shard->shardgroupid);
        tuple = systable_getnext(scan);
    }

    if(list_length(tuplist) == 0)
    {
        elog(ERROR, "datanode[%d] has no shardgroup to be moved",fromnode);
    }
    else if(list_length(tuplist) == 1)
    {
        elog(ERROR, "datanode[%d] has only one shardgroup, it cannot be splited",fromnode);
    }

    *shardgroup_num = list_length(tuplist)/2;
    shardgroups = (int*)palloc0((*shardgroup_num) * sizeof(int));
    
    for (i = 0; i < *shardgroup_num; i++)
    {
        int shardgroupid;
        shardgroupid = list_nth_int(tuplist,i);
        shardgroups[i] = shardgroupid;
    }

    systable_endscan(scan);
    heap_close(shardmapRel,AccessShareLock);

    list_free(tuplist);

    return shardgroups;
}

void PgxcMoveData_Node(MoveDataStmt* stmt)
{    
    Oid group_oid;
    group_oid = get_pgxc_groupoid(strVal(stmt->group));
    if (!OidIsValid(group_oid))
    {
        elog(ERROR, "group with name:%s not found", strVal(stmt->group));
    }

    UpdateRelationShardMap(group_oid, stmt->fromid,stmt->toid,stmt->num_shard,stmt->arr_shard);
}

/*
 * Diff from CN, DN should truncate all non-shard tables in to-node when split one dn to two.
 */
void PgxcMoveData_DN(MoveDataStmt* stmt)
{// #lizard forgives    
    //Value *fromvalue;
    Value *tovalue;
    bool    isFrom;

    //fromvalue = &(stmt->from_node->val);
    tovalue = &(stmt->to_node->val);
    isFrom = true;

    /* Only tonode and strategy equals to MOVE_DATA_STRATEGY_AT should truncate all non-shard tables */
    if(IsA(tovalue,String) && strncmp(strVal(tovalue), PGXCNodeName, NAMEDATALEN)==0)
    {
        isFrom = false;
    }
    else
    {
        elog(LOG, "[PgxcMoveData_DN]Nove data tonode:%s not datanode itself:%s, "
                  "no need to truncate all non-shard tables.", strVal(tovalue), PGXCNodeName);
        return;
    }


    switch(stmt->strategy)
    {
        case MOVE_DATA_STRATEGY_SHARD:
            {
                elog(LOG,"[PgxcMoveData_DN]When using strategy MOVE_DATA_STRATEGY_SHARD, no need to"
                         " truncate all non-shard tables.");
            }
            break;
         case MOVE_DATA_STRATEGY_AT:
            {
                if(!isFrom)
                {
                    List * relnames = NULL;
                    List * nsnames = NULL;
                    List * relrvs = NULL;
                    /* get rel list except shard table */
                    GetNotShardRelations(false, &relnames, &nsnames);
                    
                    if(list_length(relnames) != 0)
                    {
                        ListCell *relcell;
                        ListCell *nscell;
                        RangeVar * rv;
                        char     *relname;
                        char    *nsname;
                        TruncateStmt * stmt = (TruncateStmt*)makeNode(TruncateStmt);

                        stmt->behavior = DROP_CASCADE;
                        stmt->restart_seqs = false;

                        forboth(relcell, relnames, nscell, nsnames)
                        {
                            relname = (char *)lfirst(relcell);
                            nsname = (char *)lfirst(nscell);
                            rv = makeRangeVar(nsname, relname, -1);
                            
                            elog(LOG, "truncate relation %s because of moving data in new datanode",relname);
                            relrvs = lappend(relrvs, rv);
                        }

                        stmt->relations = relrvs;        
                        ExecuteTruncate(stmt);
                        list_free_deep(relnames);
                        list_free_deep(nsnames);
                        list_free_deep(relrvs);
                    }
                }
            }
            break;
        case MOVE_DATA_STRATEGY_NODE:
            //elog(ERROR, "Move Strategy MOVE_DATA_STRATEGY_NODE is not supported in data node");
            {
                if(!isFrom)
                {
                    List * relnames = NULL;
                    List * nsnames = NULL;
                    List * relrvs = NULL;
                    /* get rel list except shard table */
                    GetNotShardRelations(false, &relnames, &nsnames);
                    
                    if(list_length(relnames) != 0)
                    {
                        ListCell *relcell;
                        ListCell *nscell;
                        RangeVar * rv;
                        char     *relname;
                        char    *nsname;
                        TruncateStmt * stmt = (TruncateStmt*)makeNode(TruncateStmt);

                        stmt->behavior = DROP_CASCADE;
                        stmt->restart_seqs = false;

                        forboth(relcell, relnames, nscell, nsnames)
                        {
                            relname = (char *)lfirst(relcell);
                            nsname = (char *)lfirst(nscell);
                            rv = makeRangeVar(nsname, relname, -1);
                            
                            elog(LOG, "truncate relation %s because of moving data in new datanode",relname);
                            relrvs = lappend(relrvs, rv);
                        }

                        stmt->relations = relrvs;        
                        ExecuteTruncate(stmt);
                        list_free_deep(relnames);
                        list_free_deep(nsnames);
                        list_free_deep(relrvs);
                    }
                }
            }
            break;
        default:
            elog(ERROR, "Invalid Move Strategy %d", stmt->strategy);
            break;
    }
}


void UpdateReplicaRelNodes(Oid newnodeid)
{
    Relation    rel;
    HeapTuple    oldtup;
    HeapScanDesc scan;
    Form_pgxc_class pgxc_class;
    Oid *old_oid_array;
#ifdef __COLD_HOT__
    Oid *new_oid_array[2] = {NULL};
    int new_num[2] = {0};
#else
    Oid *new_oid_array;
    int new_num;
#endif
    int old_num;

    if(!OidIsValid(newnodeid))
    {
        elog(ERROR,"add invalid node oid to relation nodes");
    }

    rel = heap_open(PgxcClassRelationId, RowExclusiveLock);

    scan = heap_beginscan_catalog(rel,0,NULL);
    
    oldtup = heap_getnext(scan,ForwardScanDirection);
    
    while(HeapTupleIsValid(oldtup))
    {
        pgxc_class = (Form_pgxc_class)GETSTRUCT(oldtup);

        if(pgxc_class->pclocatortype != LOCATOR_TYPE_REPLICATED)
        {
            oldtup = heap_getnext(scan,ForwardScanDirection);
            continue;
        }

        old_num = get_pgxc_classnodes(pgxc_class->pcrelid, &old_oid_array);

        if(oidarray_contian_oid(old_oid_array, old_num, newnodeid))
        {
            oldtup = heap_getnext(scan,ForwardScanDirection);
            continue;
        }

#ifdef __COLD_HOT__
        new_oid_array[0] = add_node_list(old_oid_array, old_num, &newnodeid, 1, new_num);
        
        /* Sort once again the newly-created array of node Oids to maintain consistency */
        new_oid_array[0] = SortRelationDistributionNodes(new_oid_array[0], new_num[0]);
#else
        new_oid_array = add_node_list(old_oid_array, old_num, &newnodeid, 1, &new_num);
        
        /* Sort once again the newly-created array of node Oids to maintain consistency */
        new_oid_array = SortRelationDistributionNodes(new_oid_array, new_num);
#endif
        /* Update pgxc_class entry */
        PgxcClassAlter(pgxc_class->pcrelid,
                   '\0',
                   0,
#ifdef __COLD_HOT__
                   0,
#endif
                   0,
                   0,
                   new_num,
                   new_oid_array,
                   PGXC_CLASS_ALTER_NODES);

        oldtup = heap_getnext(scan,ForwardScanDirection);
    }

    
    heap_endscan(scan);
    heap_close(rel, RowExclusiveLock);
}

/*
 * fresh shard map in share memory.
 * groupoid can be InvalidOid when this function called in datanode
*/
void ForceRefreshShardMap(Oid groupoid)
{
    LWLockAcquire(ShardMapLock, LW_EXCLUSIVE);
    if(!OidIsValid(groupoid))
    {    
        if (IS_PGXC_COORDINATOR)
        {
			g_GroupShardingMgr->inited = false;
            SyncShardMapList_Node_CN();
            g_GroupShardingMgr->inited = true;

        }
        else if (IS_PGXC_DATANODE)
        {    
			g_GroupShardingMgr_DN->inited = false;
            g_GroupShardingMgr_DN->inited = SyncShardMapList_Node_DN();
        }
    }
    LWLockRelease(ShardMapLock);
	
        /*
         * Invalidate the relcache after refresh shard map in shmem,
         * because Relation->rd_locator_info changed.
         */
	CacheInvalidateRelcacheAll();
}

/*
 * upper level has taken care of the locks
 */
static void RemoveShardMapEntry(Oid group)
{
    bool           found;
    int32          map = 0;
    GroupLookupTag tag;
    GroupLookupEnt *ent;
    tag.group =  group;    

    if (IS_PGXC_COORDINATOR)
    {
        ent = (GroupLookupEnt*)hash_search(g_GroupHashTab, (void *) &tag, HASH_FIND, &found);
        if (!ent)
        {            
            return;
        }
        map = ent->shardIndex;    

        if (!hash_search(g_GroupHashTab, (void *) &tag, HASH_REMOVE, NULL))
        {
            elog(ERROR, "hash table corrupted");
        }

        SpinLockAcquire(&g_GroupShardingMgr->lock[map]);
        g_GroupShardingMgr->used[map]          =  false;
        g_GroupShardingMgr->members[map]->group =  InvalidOid;
        SpinLockRelease(&g_GroupShardingMgr->lock[map]);    
    }
    else if (IS_PGXC_DATANODE)
    {
        SpinLockAcquire(&g_GroupShardingMgr_DN->lock);
        if (g_GroupShardingMgr_DN->members->group == group)
        {
            g_GroupShardingMgr_DN->used = false;
            g_GroupShardingMgr_DN->members->group = InvalidOid;
            bms_clear(g_DatanodeShardgroupBitmap);
        }
        SpinLockRelease(&g_GroupShardingMgr_DN->lock);
    }
}

/* sync shard map list of a group */
static void FreshGroupShardMap(Oid group)
{// #lizard forgives
    bool  newcreate = false;
    SysScanDesc scan;
    HeapTuple  oldtup;
    Relation shardrel;
    Form_pgxc_shard_map  pgxc_shard;
    int32 shard_mgr = INVALID_SHARDMAP_ID;        
    ScanKeyData skey;
    Oid   self_node_oid = InvalidOid;

    /* 
     * In datanode, one can only see the shardmap belongs to single group,
     * so it's ok to clear the g_DatanodeShardgroupBitmap here.
     */
    if (IS_PGXC_DATANODE)
    {
        bms_clear(g_DatanodeShardgroupBitmap);
    }

    self_node_oid = get_pgxc_nodeoid_extend(PGXCNodeName, PGXCMainClusterName);
    /* build sharding table */
    shardrel = heap_open(PgxcShardMapRelationId, AccessShareLock);
    ScanKeyInit(&skey,
        Anum_pgxc_shard_map_nodegroup,
        BTEqualStrategyNumber, F_OIDEQ,
        ObjectIdGetDatum(group));
    
    scan = systable_beginscan(shardrel,
                              PgxcShardMapGroupIndexId, 
                              true,
                              SnapshotSelf, 1, &skey);

    /* no such tuples exist, so remove the existing map entry */
    oldtup = systable_getnext(scan);
    if (!HeapTupleIsValid(oldtup))
    {
        RemoveShardMapEntry(group);
    }
    
    while(HeapTupleIsValid(oldtup))
    {
        pgxc_shard = (Form_pgxc_shard_map)GETSTRUCT(oldtup);

        if (IS_PGXC_COORDINATOR)
        {
            if (INVALID_SHARDMAP_ID == shard_mgr)
            {
                /* If the group sharding has not been created */
                newcreate = !GroupShardingTabExist(group, false, &shard_mgr);
                if (newcreate)
                {
                    /* here, we try to get a new shard mapping mgr */
                    shard_mgr = AllocShardMap(pgxc_shard->extended, group);
                    if (INVALID_SHARDMAP_ID == shard_mgr)
                    {
                        elog(ERROR, "too many sharding group defined");
                    }
                }
            }
            InsertShardMap_CN(shard_mgr, pgxc_shard);    
        }
        else if (IS_PGXC_DATANODE)
        {
            shard_mgr = 0; /*set to 0, shows oldtup is valid*/
            InsertShardMap_DN(pgxc_shard);    
        }
        
                            
        oldtup = systable_getnext(scan);
        if (IS_PGXC_DATANODE)
        {
            BuildDatanodeVisibilityMap(pgxc_shard, self_node_oid);
        }
    }

    systable_endscan(scan);
    heap_close(shardrel, AccessShareLock);    

    if (IS_PGXC_COORDINATOR)
    {
        if(INVALID_SHARDMAP_ID != shard_mgr)
        {
            ShardMapInitDone_CN(shard_mgr, group, false);    
        }
    }
    else if (IS_PGXC_DATANODE)
    {
        if(INVALID_SHARDMAP_ID != shard_mgr)
        {
            ShardMapInitDone_DN(group, false);    
        }
    }    
}

/*
 * It's caller's responsibilty to ensure g_DatanodeShardgroupBitmap is cleared.
 */
static void BuildDatanodeVisibilityMap(Form_pgxc_shard_map tuple, Oid self_oid)
{
    Bitmapset *tmp = NULL;
    
    if (tuple->primarycopy == self_oid)
    {
        tmp = bms_add_member(g_DatanodeShardgroupBitmap, tuple->shardgroupid);
        if(tmp != g_DatanodeShardgroupBitmap)
        {
            elog(PANIC, "build shard group bitmap failed. new bitmap maybe not in share memory");
        }
        elog(DEBUG1, "shardid %d belongs to datanode:%s", tuple->shardgroupid, PGXCNodeName);
    }
    else
    {
        elog(DEBUG1, "shardid %d belongs to datanode whose oid:%d, NOT datanode itself:%s", 
                 tuple->shardgroupid, tuple->primarycopy, PGXCNodeName);
    }
}


Bitmapset * 
CopyShardGroups_DN(Bitmapset * dest)
{
    SyncShardMapList(false);    

    /* Only one group exists in  */
    if(g_GroupShardingMgr_DN->members->shardMapStatus != SHMEM_SHRADMAP_STATUS_USING)
    {
        return NULL;
    }

	LWLockAcquire(ShardMapLock, LW_SHARED);	
    memcpy(dest, g_DatanodeShardgroupBitmap, SHARD_TABLE_BITMAP_SIZE);
	LWLockRelease(ShardMapLock);
	
    return dest;
}


int GetGroupSize()
{
    return 1;
}



/*
 * Get ShardId in datanode
 */
int32 EvaluateShardId(Oid type, bool isNull, Datum dvalue, 
                           Oid secType, bool isSecNull, Datum secValue, Oid relid)
{    
    long        hashvalue;
    Oid         table;
    long        sechashvalue;
    char        *value;
    Relation     rel;
    int16        typlen;
    bool        typbyval;
    char        typalign;
    char        typdelim;
    Oid         typioparam;
    Oid         typiofunc;
    
    /* primary distribute key or secondary distribute key is NULL */
    if (isNull)
    {
        return NullShardId;
    }    

    rel = heap_open(relid, NoLock);
    if (RELATION_IS_CHILD(rel))
    {
        table = RELATION_GET_PARENT(rel);
    }
    else
    {
        table = relid;
    }
    heap_close(rel,NoLock);

    if (!g_EnableKeyValue)
    {
        /* not the key value, use common map strategy */
        hashvalue = compute_hash(type, dvalue, LOCATOR_TYPE_SHARD); 
        return  abs(hashvalue) % MAX_SHARDS;
    }

    /* check whether the value is key value */
    get_type_io_data(type, IOFunc_output,
                     &typlen, &typbyval,
                     &typalign, &typdelim,
                     &typioparam, &typiofunc);
    value = OidOutputFunctionCall(typiofunc, dvalue);
    if (!IsKeyValues(MyDatabaseId, table, value) && !InTempKeyValueList(relid, value, NULL, NULL))
    {        
        /* not the key value, use common map strategy */
        hashvalue = compute_hash(type, dvalue, LOCATOR_TYPE_SHARD); 
        pfree(value);
        return  abs(hashvalue) % MAX_SHARDS;;
    }
    pfree(value);
    
    /* secondary sharding map */
    hashvalue = compute_hash(type, dvalue, LOCATOR_TYPE_SHARD);
    if(!isSecNull)
    {        
        sechashvalue = compute_keyvalue_hash(secType, secValue);
    }
    else
    {
        sechashvalue = 0;
    }

    return abs(hashvalue + sechashvalue) % MAX_SHARDS;
}

int
TruncateShard(Oid reloid, ShardID sid, int pausetime)
{
    ExtentID eid = InvalidExtentID;
    int    tuples = 0;
    Relation rel = NULL;
    Oid        toastoid = InvalidOid;

    StartTransactionCommand();
    rel = heap_open(reloid, AccessShareLock);
    toastoid = rel->rd_rel->reltoastrelid;

    if(!RelationHasExtent(rel))
    {
        elog(ERROR, "only sharded table can be truncated by shard.");
    }

    /*
     * step 1: add shard barrier
     */
    AddShardBarrier(rel->rd_node, sid, MyProcPid);
    heap_close(rel,AccessShareLock);
    CommitTransactionCommand();

    /*
     * step 2: do checkpoint, make sure dirty page of this shard flushed to storage. 
     */
    RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT);

    /*
     * step 3: start remove index items and recycle storage space
     */
    StartTransactionCommand();
    eid = RelOidGetShardScanHead(reloid, sid);
    
    while(ExtentIdIsValid(eid))
    {
        int deleted_tuples = 0;

        rel = heap_open(reloid, RowExclusiveLock);
        /*
         * delete this extent's tuples and their index entries
         */
        truncate_extent_tuples(rel, 
                                eid * PAGES_PER_EXTENTS, 
                                (eid+1) * PAGES_PER_EXTENTS, 
                                false, 
                                &deleted_tuples);
        tuples += deleted_tuples;

        /*
         * end transaction 
         */
        heap_close(rel, RowExclusiveLock);
        rel = NULL;
        CommitTransactionCommand();

        /*
         * start another transaction
         */
        StartTransactionCommand();

        rel = heap_open(reloid, AccessExclusiveLock);

        RelationOpenSmgr(rel);
#ifndef DISABLE_FALLOCATE
        log_smgrdealloc(&rel->rd_node, eid, SMGR_DEALLOC_FREESTORAGE);
        smgrdealloc(rel->rd_smgr, MAIN_FORKNUM, eid * PAGES_PER_EXTENTS);
        if(trace_extent)
        {
            ereport(LOG,
                (errmsg("[trace extent]Dealloc:[rel:%d/%d/%d]"
                        "[eid:%d, flags=FREESTORAGE]",
                        rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
                        eid)));
        }
#else
        log_smgrdealloc(&rel->rd_node, eid, SMGR_DEALLOC_REINIT);
        reinit_extent_pages(rel, eid);

        if(trace_extent)
        {
            ereport(LOG,
                (errmsg("[trace extent]Dealloc:[rel:%d/%d/%d]"
                        "[eid:%d, flags=REINIT_PAGE]",
                        rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
                        eid)));
        }
#endif        
        /* 
         * detach extent
         */
        FreeExtent(rel, eid);
        heap_close(rel, AccessExclusiveLock);
        rel = NULL;
        
        eid = RelOidGetShardScanHead(reloid, sid);

        if(ExtentIdIsValid(eid) && pausetime > 0)
            pg_usleep(pausetime);
    }
    CommitTransactionCommand();

    StartTransactionCommand();

    /*
     * step 4: invalidate buf page.
     */
#ifndef DISABLE_FALLOCATE
    rel = heap_open(reloid, AccessShareLock);    
    DropRelfileNodeShardBuffers(rel->rd_node, sid);
    heap_close(rel, AccessShareLock);
#endif

    /*
     * step 5: release barrier
     */
    RemoveShardBarrier();
    CommitTransactionCommand();
    
    if(OidIsValid(toastoid))
    {
        TruncateShard(toastoid, sid, pausetime);
    }
    
    return tuples;
}
void StatShardRelation(Oid relid, ShardStat *shardstat, int32 shardnumber)
{
    int32        shardid;
    Relation     rel;
    HeapScanDesc scan;
    HeapTuple    tuple;
    BufferAccessStrategy bstrategy;
    Snapshot    snapshot;
    
    rel = heap_open(relid, AccessShareLock);
    if (rel->rd_locator_info)
    {
        if (LOCATOR_TYPE_SHARD == rel->rd_locator_info->locatorType)
        {
            snapshot = GetTransactionSnapshot();
            scan  = heap_beginscan(rel,snapshot,0,NULL);        
            
            /* set proper strategy */
            bstrategy = GetAccessStrategy(BAS_BULKREAD_STAT_SHARD);
            if (scan->rs_strategy)
            {
                FreeAccessStrategy(scan->rs_strategy);
            }
            
            scan->rs_strategy = bstrategy;            
            tuple = heap_getnext(scan,ForwardScanDirection);            
            while(HeapTupleIsValid(tuple))
            {
                shardid = HeapTupleGetShardId(tuple);
                if (shardid >= shardnumber)
                {
                    elog(ERROR, "invalid shard id:%d of shard tuple in relation:%u", shardid, relid);    
                }
                else
                {
                    shardstat[shardid].shard = shardid;
                    shardstat[shardid].count ++;
                    shardstat[shardid].size += tuple->t_len;
                }
                tuple   = heap_getnext(scan,ForwardScanDirection);
            }

            heap_endscan(scan);
        }
    }
    heap_close(rel, AccessShareLock);
}


void StatShardAllRelations(ShardStat *shardstat, int32 shardnumber)
{
    Oid              relid;
    Relation         classRel;
    HeapTuple         tuple;
    HeapScanDesc     relScan;
    
    classRel = heap_open(RelationRelationId, AccessShareLock);
    relScan = heap_beginscan_catalog(classRel, 0, NULL);
    /*
     * Here we only process main tables, skip other kind relations
     */
    while ((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
    {
        Form_pg_class classForm = (Form_pg_class) GETSTRUCT(tuple);
        
        if (classForm->relkind != RELKIND_RELATION &&
            classForm->relkind != RELKIND_MATVIEW)
            continue;

        relid = HeapTupleGetOid(tuple);
        StatShardRelation(relid, shardstat, shardnumber);
    }
    heap_endscan(relScan);
    heap_close(classRel, AccessShareLock);
}


Datum
pg_stat_table_shard(PG_FUNCTION_ARGS)
{// #lizard forgives
#define  COLUMN_NUM 2

    FuncCallContext *funcctx;
    ShardStat_State *status;
    ListCell *lc            = NULL;

    if (SRF_IS_FIRSTCALL())
    {
        Oid    nsp;
        Oid    rel = InvalidOid;
        List   *relInfoList;
        List   *search_path;
        char   *table;
        text   *tbl;
        TupleDesc    tupdesc;
        MemoryContext oldcontext;
        
        /* get table oid */
        tbl = (text*)PG_GETARG_CSTRING(0);
        if (!tbl)
        {
            elog(ERROR, "no table specified");
        }

        table = text_to_cstring(tbl);
        relInfoList = stringToQualifiedNameList(table);
        switch (list_length(relInfoList))
        {
            case 1:
            {
                /* only relation */
                search_path = fetch_search_path(true);            
                foreach(lc, search_path)
                {
                    nsp = lfirst_oid(lc);
                    if (OidIsValid(nsp))            
                    {
                        rel = get_relname_relid(strVal(linitial(relInfoList)), nsp);
                        if (OidIsValid(rel))
                        {
                            break;
                        }
                    }
                }                        
                list_free(search_path);
                break;
            }
            case 2:
            {
                /* schema.relation */                    
                nsp = get_namespace_oid(strVal(linitial(relInfoList)), false);    
                rel = get_relname_relid(strVal(lsecond(relInfoList)), nsp);
                break;
            }
            case 3:
            {
                /* database.schema.relation */                            
                nsp = get_namespace_oid(strVal(lsecond(relInfoList)), false);
                rel = get_relname_relid(strVal(lthird(relInfoList)), nsp);
                break;
            }

            default:
            {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                     errmsg("table %s not exist", table)));
            }

        }
        /* free name list */
        pfree(table);
        list_free_deep(relInfoList);

        if (!OidIsValid(rel))
        {
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                     errmsg("table %s not exist", table)));
        }
        
        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * Switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples */
        tupdesc = CreateTemplateTupleDesc(COLUMN_NUM, false);
        TupleDescInitEntry(tupdesc, (AttrNumber) 1, "shardid",
                           INT4OID, -1, 0);

        TupleDescInitEntry(tupdesc, (AttrNumber) 2, "tuplecount",
                           INT8OID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        status = (ShardStat_State *) palloc0(sizeof(ShardStat_State));
        funcctx->user_fctx = (void *) status;
        status->currIdx = 0;    
        
        StatShardRelation(rel, status->shardstat, SHARD_MAP_GROUP_NUM);        
        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    status  = (ShardStat_State *) funcctx->user_fctx;
    
    while (status->currIdx < SHARD_MAP_SHARD_NUM)
    {
        Datum        values[COLUMN_NUM];
        bool        nulls[COLUMN_NUM];
        HeapTuple    tuple;
        Datum        result;
        
        if (!status->shardstat[status->currIdx].count)
        {
            status->currIdx++;
            continue;
        }
        
        /*
         * Form tuple with appropriate data.
         */
        MemSet(values, 0, sizeof(values));
        MemSet(nulls,  0, sizeof(nulls));

        values[0] = Int32GetDatum(status->shardstat[status->currIdx].shard);
        values[1] = Int64GetDatum(status->shardstat[status->currIdx].count);
        
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        status->currIdx++;
        SRF_RETURN_NEXT(funcctx, result);
    }

    SRF_RETURN_DONE(funcctx);
}


Datum
pg_stat_all_shard(PG_FUNCTION_ARGS)
{
#define  COLUMN_NUM 2

    FuncCallContext *funcctx;
    ShardStat_State *status;

    if (SRF_IS_FIRSTCALL())
    {        
        TupleDesc    tupdesc;
        MemoryContext oldcontext;
        

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * Switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples */
        tupdesc = CreateTemplateTupleDesc(COLUMN_NUM, false);
        TupleDescInitEntry(tupdesc, (AttrNumber) 1, "shardid",
                           INT4OID, -1, 0);

        TupleDescInitEntry(tupdesc, (AttrNumber) 2, "tuplecount",
                           INT8OID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        status = (ShardStat_State *) palloc0(sizeof(ShardStat_State));
        funcctx->user_fctx = (void *) status;
        status->currIdx = 0;    
        
        StatShardAllRelations(status->shardstat, SHARD_MAP_GROUP_NUM);    

        RefreshShardStatistic(status->shardstat);
            
        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    status  = (ShardStat_State *) funcctx->user_fctx;
    
    while (status->currIdx < SHARD_MAP_SHARD_NUM)
    {
        Datum        values[COLUMN_NUM];
        bool        nulls[COLUMN_NUM];
        HeapTuple    tuple;
        Datum        result;
        
        if (!status->shardstat[status->currIdx].count)
        {
            status->currIdx++;
            continue;
        }
        
        /*
         * Form tuple with appropriate data.
         */
        MemSet(values, 0, sizeof(values));
        MemSet(nulls,  0, sizeof(nulls));

        values[0] = Int32GetDatum(status->shardstat[status->currIdx].shard);
        values[1] = Int64GetDatum(status->shardstat[status->currIdx].count);
        
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        status->currIdx++;
        SRF_RETURN_NEXT(funcctx, result);
    }

    SRF_RETURN_DONE(funcctx);
}

#define DUAL_WRITE_FMT "%u,%d,%d,%s,%s,%s\n"

Datum
pg_begin_table_dual_write(PG_FUNCTION_ARGS)
{// #lizard forgives
    int16        typlen;
    int32        gap;
    int32        partitionStrategy = 0;
    bool        typbyval;
    bool        bexist;
    char        typalign;
    char        typdelim;
    Oid         typioparam;
    Oid         typiofunc;
    Datum        dvalue;
    
    Oid    nsp;
    Oid    rel = InvalidOid;
    Oid    type;
    
    AttrNumber attribute;
    struct stat stat_buf;
    
    List   *relInfoList  = NULL;
    List   *search_path  = NULL;
    ListCell *lc         = NULL;
    text   *tbl;
    text   *column;
    text   *value;
    char   *table;
    char   *columnstr;     
    char   *valuestr;
    
    Relation                 relation      = NULL;
    Form_pg_partition_interval routerinfo   = NULL;    
    FILE                     *fp          = NULL;    
    char             returnstr[MAXPGPATH] = {0};

    if(!IS_PGXC_COORDINATOR)
    {
        elog(ERROR, "[table_dual_write]can only called on coordinator");
    }
    
    /* get table oid */
    tbl = (text*)PG_GETARG_CSTRING(0);
    if (!tbl)
    {
        elog(ERROR, "[table_dual_write]no table specified");
    }
    table = text_to_cstring(tbl);
    relInfoList = stringToQualifiedNameList(table);
    switch (list_length(relInfoList))
    {
        case 1:
        {
            /* only relation */
            search_path = fetch_search_path(true);            
            foreach(lc, search_path)
            {
                nsp = lfirst_oid(lc);
                if (OidIsValid(nsp))            
                {
                    rel = get_relname_relid(strVal(linitial(relInfoList)), nsp);
                    if (OidIsValid(rel))
                    {
                        break;
                    }
                }
            }                        
            list_free(search_path);
            break;
        }
        case 2:
        {
            /* schema.relation */                    
            nsp = get_namespace_oid(strVal(linitial(relInfoList)), false);    
            rel = get_relname_relid(strVal(lsecond(relInfoList)), nsp);
            break;
        }
        case 3:
        {
            /* database.schema.relation */    
            nsp = get_namespace_oid(strVal(lsecond(relInfoList)), false);
            rel = get_relname_relid(strVal(lthird(relInfoList)), nsp);
            break;
        }

        default:
        {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("[table_dual_write]table '%s' not exist", table)));
        }

    }

    /* free name list */    
    list_free_deep(relInfoList);

    if (!OidIsValid(rel))
    {
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("[table_dual_write]table '%s' not exist", table)));
    }

    /* handle column */
    column = (text*)PG_GETARG_CSTRING(1);
    if (!column)
    {
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("[table_dual_write]we need column info")));
    }
    columnstr = text_to_cstring(column);
    attribute = get_attnum(rel, (const char *)(columnstr));
    if (InvalidAttrNumber == attribute)
    {
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                  errmsg("[table_dual_write]no such column:%s", columnstr)));
    }
        
    relation     = relation_open(rel, NoLock);
    routerinfo     = relation->rd_partitions_info;
    if (routerinfo)
    {
        if (routerinfo->partpartkey != attribute)
        {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                  errmsg("[table_dual_write]dual write attribute must be the partitioned key")));
        }
    }
    else
    {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
              errmsg("[table_dual_write]only partitioned table can set dual write")));
    }
    partitionStrategy = routerinfo->partinterval_type;
    if (partitionStrategy == IntervalType_Month &&
        routerinfo->partinterval_int == 12)
    {
        partitionStrategy = IntervalType_Year;
    }
    relation_close(relation, NoLock);

    /* handle value info */
    value = (text*)PG_GETARG_CSTRING(2);
    if (!value)
    {
        elog(ERROR, "[table_dual_write]no table specified");
    }
    valuestr = text_to_cstring(value);
    type     = get_atttype(rel, attribute);
    get_type_io_data(type, IOFunc_input,
                     &typlen, &typbyval,
                     &typalign, &typdelim,
                     &typioparam, &typiofunc);
    dvalue = OidInputFunctionCall(typiofunc, valuestr, typioparam, -1);

    gap = get_timestamptz_gap(dvalue, partitionStrategy);
    /* here, parameter check has done, we have record data onto disk now */
    LWLockAcquire(DualWriteLock, LW_EXCLUSIVE);
    
    g_DualWriteCtl->needlock = true; /* tell others to use lock */
    
    /* record dual write info into disk */    
    bexist = AddDualWriteInfo(rel, attribute, gap, table, columnstr, valuestr); 
    if (bexist)
    {
        /* already exist, no need to flush file */
        g_DualWriteCtl->needlock = false;
        LWLockRelease(DualWriteLock);
        snprintf(returnstr, MAXPGPATH, "success");
        PG_RETURN_TEXT_P(cstring_to_text(returnstr));
    }
    snprintf(returnstr, MAXPGPATH,DUAL_WRITE_FMT, rel, attribute, gap, table, columnstr, valuestr);
    pfree(table);
    /* sync info into file */
    if (0 == stat(DUAL_WRITE_FILE_NAME, &stat_buf))
    {
        /* use major file */
        fp = AllocateFile(DUAL_WRITE_FILE_NAME, "a+");
        if (!fp)
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("[table_dual_write]could not open file \"%s\": %m",
                            DUAL_WRITE_FILE_NAME)));
        fprintf(fp, "%s", returnstr);
        fflush(fp);
        if (ferror(fp))
        {
            FreeFile(fp);
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("[table_dual_write]could not write file \"%s\": %m",
                            DUAL_WRITE_FILE_NAME)));
        }
        FreeFile(fp);
    }    
    else if (0 == stat(DUAL_WRITE_BACKUP_FILE_NAME, &stat_buf))
    {
        /* use backup file */
        fp = AllocateFile(DUAL_WRITE_BACKUP_FILE_NAME, "a");
        if (!fp)
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("[table_dual_write]could not open file \"%s\": %m",
                            DUAL_WRITE_BACKUP_FILE_NAME)));
        fprintf(fp, "%s", returnstr);
        fflush(fp);
        if (ferror(fp))
        {
            FreeFile(fp);
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("[table_dual_write]could not write file \"%s\": %m",
                            DUAL_WRITE_BACKUP_FILE_NAME)));
        }
        FreeFile(fp);

        /* rename backup file to current file */
        if (rename(DUAL_WRITE_BACKUP_FILE_NAME, DUAL_WRITE_FILE_NAME) != 0)
        {
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("[table_dual_write]could not rename file \"%s\" to \"%s\": %m",
                            DUAL_WRITE_BACKUP_FILE_NAME, DUAL_WRITE_FILE_NAME)));
        }
    }
    else
    {
        /* use major file */
        fp = AllocateFile(DUAL_WRITE_FILE_NAME, "w+");
        if (!fp)
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("[table_dual_write]could not open file \"%s\": %m",
                            DUAL_WRITE_FILE_NAME)));
        fprintf(fp, "%s", returnstr);
        fflush(fp);
        if (ferror(fp))
        {
            FreeFile(fp);
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("[table_dual_write]could not write file \"%s\": %m",
                            DUAL_WRITE_FILE_NAME)));
        }
        FreeFile(fp);
    }    
    g_DualWriteCtl->needlock = false;/* tell others to release lock */
    LWLockRelease(DualWriteLock);
    
    /* print dual write log */
    elog(LOG, "[table_dual_write]set begin dual write:%s", returnstr);
    
    snprintf(returnstr, MAXPGPATH, "success");
    PG_RETURN_TEXT_P(cstring_to_text(returnstr));
}

typedef struct
{
    HASH_SEQ_STATUS status;
}DualWriteStatInfo;

/*
 * check valid dual info
 */
static 
bool ValidDualWriteInfo(void)
{
    bool  removed = false;
    DualWriteRecord *ent;
    Relation         rel = NULL;
    HASH_SEQ_STATUS status;

    hash_seq_init(&status, g_DualWriteCtl->dwhash);
    
    while (((ent = (DualWriteRecord *) hash_seq_search(&status)) != NULL))
    {
        rel = RelationIdGetRelation(ent->tag.relation);
        if (NULL == rel)
        {
            ent = hash_search(g_DualWriteCtl->dwhash, (void *) &ent->tag, HASH_REMOVE, NULL);
            g_DualWriteCtl->entrynum--;    
            removed = true;
        }
        else
        {
            RelationClose(rel);
        }
    }
    return removed;
}

static void DumpDualWriteInfo(FILE *file, bool needlock)
{
    HASH_SEQ_STATUS status;
    DualWriteRecord *ent;
    
    if (needlock)
    {
        LWLockAcquire(DualWriteLock, LW_SHARED);
    }
    hash_seq_init(&status, g_DualWriteCtl->dwhash);

    while ((ent = (DualWriteRecord *) hash_seq_search(&status)) != NULL)
    {
        fprintf(file, DUAL_WRITE_FMT, ent->tag.relation, ent->tag.attr, ent->tag.timeoffset, ent->table, ent->column, ent->value);
    }
    
    if (needlock)
    {
        LWLockRelease(DualWriteLock);
    }
}

/*
 * Add dual write info
 */
static 
bool RemoveDualWriteInfo(Oid relation, AttrNumber attr, Datum value)
{
    int32 gap;
    DTag  tag;
    int32        partitionStrategy        = 0;
    Relation                  rel            = NULL;
    Form_pg_partition_interval routerinfo   = NULL;
    bool        found;
    
    /* get partition stragegy first */
    rel          = relation_open(relation, NoLock);
    routerinfo   = rel->rd_partitions_info;
    if (routerinfo)
    {
        if (attr == routerinfo->partpartkey)
        {
            partitionStrategy = routerinfo->partinterval_type;
            if (partitionStrategy == IntervalType_Month &&
                routerinfo->partinterval_int == 12)
            {
                partitionStrategy = IntervalType_Year;
            }
        }
    }
    relation_close(rel, NoLock);
    
    gap             = get_timestamptz_gap(value, partitionStrategy);
    tag.relation     = relation;
    tag.attr         = attr;
    tag.timeoffset  = gap;

    hash_search(g_DualWriteCtl->dwhash, (void *) &tag, HASH_REMOVE, &found);     
    if (found)
    {
        g_DualWriteCtl->entrynum--;    
        if (g_DualWriteCtl->entrynum < 0)
        {
            g_DualWriteCtl->entrynum = 0;
        }
    }
    return found;
}

Datum
pg_stat_dual_write(PG_FUNCTION_ARGS)
{// #lizard forgives
#define ATTR_NUM 3
    FuncCallContext     *funcctx;
    DualWriteStatInfo    *qInfo;
    DualWriteRecord     *ent;

    if(!IS_PGXC_COORDINATOR)
    {
        elog(ERROR, "can only called on coordinator");
    }
    
    if (SRF_IS_FIRSTCALL())
    {        
        bool          bremove;
        MemoryContext oldcontext;
        TupleDesc      tupdesc;
        FILE          *fp;

        LWLockAcquire(DualWriteLock, LW_EXCLUSIVE);
        bremove = ValidDualWriteInfo();
        LWLockRelease(DualWriteLock);
        if (bremove)
        {
            /* remove dual write info from disk */
            if (rename(DUAL_WRITE_FILE_NAME, DUAL_WRITE_BACKUP_FILE_NAME) != 0)
            {
                ereport(ERROR,
                        (errcode_for_file_access(),
                         errmsg("could not rename file \"%s\" to \"%s\": %m",
                                 DUAL_WRITE_FILE_NAME, DUAL_WRITE_BACKUP_FILE_NAME)));
            }
            
            /* no backup file either, create new file */
            fp = AllocateFile(DUAL_WRITE_FILE_NAME, "w+");
            if (!fp)
            {
                ereport(ERROR,
                        (errcode_for_file_access(),
                         errmsg("could not open file \"%s\": %m",
                                DUAL_WRITE_FILE_NAME)));
            }
            DumpDualWriteInfo(fp, false);
            fflush(fp);
            if (ferror(fp))
            {
                FreeFile(fp);
                ereport(ERROR,
                        (errcode_for_file_access(),
                         errmsg("could not write file \"%s\": %m",
                                DUAL_WRITE_FILE_NAME)));
            }
            FreeFile(fp);

            /* remove backup file */
            if (unlink(DUAL_WRITE_BACKUP_FILE_NAME) < 0)
            {
                ereport(WARNING,
                        (errcode_for_file_access(),
                         errmsg("could not unlink file \"%s\": %m", DUAL_WRITE_BACKUP_FILE_NAME)));
            }
        }
        
        
        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(ATTR_NUM, false);
        TupleDescInitEntry(tupdesc, (AttrNumber) 1, "relation",
                           TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 2, "attribute",
                           TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 3, "value",
                           TEXTOID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        funcctx->user_fctx = palloc0(sizeof(DualWriteStatInfo));
        qInfo = (DualWriteStatInfo*)funcctx->user_fctx;
        
        LWLockAcquire(DualWriteLock, LW_SHARED);
        hash_seq_init(&qInfo->status, g_DualWriteCtl->dwhash);
        MemoryContextSwitchTo(oldcontext);
    }


    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    qInfo = (DualWriteStatInfo*)funcctx->user_fctx; 
    if (((ent = (DualWriteRecord *) hash_seq_search(&qInfo->status)) != NULL))
    {
        /* for each row */
        Datum        values[ATTR_NUM];
        bool        nulls[ATTR_NUM];
        HeapTuple    tuple;        

        MemSet(values, 0, sizeof(values));
        MemSet(nulls, 0, sizeof(nulls));

        values[0] = PointerGetDatum(cstring_to_text((const char *) ent->table));
        values[1] = PointerGetDatum(cstring_to_text((const char *) ent->column));
        values[2] = PointerGetDatum(cstring_to_text((const char *) ent->value));
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    else
    {
        /* nothing left */
        LWLockRelease(DualWriteLock);
        SRF_RETURN_DONE(funcctx);
    }
}

Datum
pg_stop_table_dual_write(PG_FUNCTION_ARGS)
{// #lizard forgives
    int16        typlen;
    bool        typbyval;
    bool        bexist;
    char        typalign;
    char        typdelim;
    Oid         type;
    Oid         typioparam;
    Oid         typiofunc;
    Datum        dvalue;
    
    Oid    nsp;
    Oid    rel = InvalidOid;
    
    AttrNumber attribute;    
    List   *relInfoList;
    List   *search_path;
    ListCell *lc   = NULL;
    text   *tbl    = NULL;
    text   *column = NULL;
    text   *value  = NULL;
    
    char   *table      = NULL;
    char   *columnstr = NULL;    
    char   *valuestr  = NULL;
    
    Relation                 relation      = NULL;
    Form_pg_partition_interval routerinfo   = NULL;    
    FILE       *fp = NULL;    
    char    returnstr[MAXPGPATH] = {0};

    if(!IS_PGXC_COORDINATOR)
    {
        elog(ERROR, "[table_dual_write]can only called on coordinator");
    }
    
    /* get table oid */
    tbl = (text*)PG_GETARG_CSTRING(0);
    if (!tbl)
    {
        elog(ERROR, "[table_dual_write]no table specified");
    }

    table = text_to_cstring(tbl);
    relInfoList = stringToQualifiedNameList(table);
    switch (list_length(relInfoList))
    {
        case 1:
        {
            /* only relation */
            search_path = fetch_search_path(true);            
            foreach(lc, search_path)
            {
                nsp = lfirst_oid(lc);
                if (OidIsValid(nsp))            
                {
                    rel = get_relname_relid(strVal(linitial(relInfoList)), nsp);
                    if (OidIsValid(rel))
                    {
                        break;
                    }
                }
            }                        
            list_free(search_path);
            break;
        }
        case 2:
        {
            /* schema.relation */                    
            nsp = get_namespace_oid(strVal(linitial(relInfoList)), false);    
            rel = get_relname_relid(strVal(lsecond(relInfoList)), nsp);
            break;
        }
        case 3:
        {
            /* database.schema.relation */
            nsp = get_namespace_oid(strVal(lsecond(relInfoList)), false);
            rel = get_relname_relid(strVal(lthird(relInfoList)), nsp);
            break;
        }

        default:
        {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("[table_dual_write]table '%s' not exist", table)));
        }

    }
    /* free name list */    
    list_free_deep(relInfoList);

    if (!OidIsValid(rel))
    {
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("[table_dual_write]table '%s' not exist", table)));
    }    
    
    /* handle column */
    column = (text*)PG_GETARG_CSTRING(1);
    if (!column)
    {
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("[table_dual_write]we need column info")));
    }
    columnstr = text_to_cstring(column);
    attribute = get_attnum(rel, (const char *)(columnstr));
    if (InvalidAttrNumber == attribute)
    {
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                  errmsg("[table_dual_write]no such column:%s", columnstr)));
    }
        
    relation     = relation_open(rel, NoLock);
    routerinfo     = relation->rd_partitions_info;
    if (routerinfo)
    {
        if (routerinfo->partpartkey != attribute)
        {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                  errmsg("[table_dual_write]dual write attribute must be the partitioned key")));
        }
    }
    else
    {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
              errmsg("[table_dual_write]only partitioned table can set dual write")));
    }
    relation_close(relation, NoLock);

    /* handle value info */
    value = (text*)PG_GETARG_CSTRING(2);
    if (!value)
    {
        elog(ERROR, "[table_dual_write]no table specified");
    }
    valuestr = text_to_cstring(value);
    type = get_atttype(rel, attribute);
    get_type_io_data(type, IOFunc_input,
                     &typlen, &typbyval,
                     &typalign, &typdelim,
                     &typioparam, &typiofunc);
    dvalue = OidInputFunctionCall(typiofunc, valuestr, typioparam, -1);
    /* remove dual write info from share memory */
    LWLockAcquire(DualWriteLock, LW_EXCLUSIVE);
    bexist    = RemoveDualWriteInfo(rel, attribute, dvalue);    
    if (!bexist)
    {
        /* no such element, return now */
        LWLockRelease(DualWriteLock);
        snprintf(returnstr, MAXPGPATH, "success");
        PG_RETURN_TEXT_P(cstring_to_text(returnstr));
    }
    
    /* remove dual write info from disk */
    if (rename(DUAL_WRITE_FILE_NAME, DUAL_WRITE_BACKUP_FILE_NAME) != 0)
    {
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("[table_dual_write]could not rename file \"%s\" to \"%s\": %m",
                         DUAL_WRITE_FILE_NAME, DUAL_WRITE_BACKUP_FILE_NAME)));
    }
    
    /* no backup file either, create new file */
    fp = AllocateFile(DUAL_WRITE_FILE_NAME, "w+");
    if (!fp)
    {
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("[table_dual_write]could not open file \"%s\": %m",
                        DUAL_WRITE_FILE_NAME)));
    }
    DumpDualWriteInfo(fp, false);
    fflush(fp);
    if (ferror(fp))
    {
        FreeFile(fp);
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("[table_dual_write]could not write file \"%s\": %m",
                        DUAL_WRITE_FILE_NAME)));
    }
    FreeFile(fp);

    /* remove backup file */
    if (unlink(DUAL_WRITE_BACKUP_FILE_NAME) < 0)
    {
        ereport(WARNING,
                (errcode_for_file_access(),
                 errmsg("[table_dual_write]could not unlink file \"%s\": %m", DUAL_WRITE_BACKUP_FILE_NAME)));
    }
    LWLockRelease(DualWriteLock);
    snprintf(returnstr, MAXPGPATH, "success");
    elog(LOG, "[table_dual_write]set dual write stop for table:%s column:%s value:%s", table, columnstr, valuestr);
    pfree(table);
    PG_RETURN_TEXT_P(cstring_to_text(returnstr));
}

Datum  
pg_set_node_cold_access(PG_FUNCTION_ARGS)
{
    return pg_set_cold_access();
}
Datum  
pg_clear_node_cold_access(PG_FUNCTION_ARGS)
{
    return pg_clear_cold_access();
}

typedef struct
{
    int32                index;
}AccessControlStatInfo;

Datum  
pg_stat_node_access(PG_FUNCTION_ARGS)
{
#define ACCESS_CONTROL_ATTR_NUM  1
    FuncCallContext         *funcctx;
    AccessControlStatInfo    *qInfo;
    HeapTuple                tuple;        

    Datum        values[ACCESS_CONTROL_ATTR_NUM];
    bool        nulls[ACCESS_CONTROL_ATTR_NUM];

    if(!IS_PGXC_DATANODE)
    {
        elog(ERROR, "can only called on datanode");
    }
    
    if (SRF_IS_FIRSTCALL())
    {
        MemoryContext oldcontext;
        TupleDesc    tupdesc;

        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(ACCESS_CONTROL_ATTR_NUM, false);
        TupleDescInitEntry(tupdesc, (AttrNumber) 1, "access",
                           TEXTOID, -1, 0);
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        funcctx->user_fctx = palloc0(sizeof(AccessControlStatInfo));
        qInfo = (AccessControlStatInfo*)funcctx->user_fctx;

        g_AccessCtl->needlock = true;
        LWLockAcquire(ColdAccessLock, LW_SHARED);
        MemoryContextSwitchTo(oldcontext);
    }
    
    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    qInfo = (AccessControlStatInfo*)funcctx->user_fctx; 
    if (qInfo->index < 1)
    {
        MemSet(values, 0, sizeof(values));
        MemSet(nulls, 0, sizeof(nulls));

        values[0] = PointerGetDatum(cstring_to_text((const char *) (g_AccessCtl->colddata ? "cold" : "hot")));
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        qInfo->index++;
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));     
    }
    else
    {
        /* nothing left */
        LWLockRelease(ColdAccessLock);
        SRF_RETURN_DONE(funcctx);
    }    
}


/*
 * Estimate space needed for shard statistic hashtable 
 */
Size
ShardStatisticShmemSize(void)
{
    Size space = 0;
    int nelems = MAX_SHARDS;
    int npools = (MaxBackends / g_MaxSessionsPerPool) + 1;
    Size pool_size = mul_size(nelems, sizeof(ShardStatistic));

    space = mul_size(npools, pool_size);
    
    return space;
}

/*
 * Initialize shard statistic info in shared memory
 */
void
ShardStatisticShmemInit(void)
{
    bool found;
    int nelems = MAX_SHARDS;
    int npools = (MaxBackends / g_MaxSessionsPerPool) + 1;
    Size pool_size = mul_size(nelems, sizeof(ShardStatistic));

    
    shardStatInfo = (ShardStatistic *)
        ShmemInitStruct("Shard Statistic Info",
                        mul_size(npools, pool_size),
                        &found);


    if (!found)
    {
        int i = 0;
        int max_elems = nelems * npools;

        for (i = 0; i < max_elems; i++)
        {
            pg_atomic_init_u64(&shardStatInfo[i].ntuples_select, 0);
            pg_atomic_init_u64(&shardStatInfo[i].ntuples_insert, 0);
            pg_atomic_init_u64(&shardStatInfo[i].ntuples_update, 0);
            pg_atomic_init_u64(&shardStatInfo[i].ntuples_delete, 0);
            pg_atomic_init_u64(&shardStatInfo[i].ntuples, 0);
            pg_atomic_init_u64(&shardStatInfo[i].size, 0);
        }
    }
}


/*
 * update shard statistic info by each backend which do select/insert/update/delete.
 */
void
UpdateShardStatistic(CmdType cmd, ShardID sid, int64 new_size, int64 old_size)
{
    int index = 0;
    int nelems = MAX_SHARDS;
    int npools = (MaxBackends / g_MaxSessionsPerPool) + 1;

    if (!ShardIDIsValid(sid))
        return;

    index = (MyProc->pgprocno % npools) * nelems + sid;
    
    switch(cmd)
    {
        case CMD_SELECT:
            {
                pg_atomic_fetch_add_u64(&shardStatInfo[index].ntuples_select, 1);
            }
            break;
        case CMD_UPDATE:
            {
                pg_atomic_fetch_add_u64(&shardStatInfo[index].ntuples_update, 1);

                if (new_size > old_size)
                {
                    int64 size = new_size - old_size;

                    pg_atomic_fetch_add_u64(&shardStatInfo[index].size, size);
                }
                else if (new_size < old_size)
                {
                    int64 size = old_size - new_size;

                    pg_atomic_fetch_sub_u64(&shardStatInfo[index].size, size);
                }
            }
            break;
        case CMD_INSERT:
            {
                pg_atomic_fetch_add_u64(&shardStatInfo[index].ntuples_insert, 1);

                pg_atomic_fetch_add_u64(&shardStatInfo[index].ntuples, 1);

                pg_atomic_fetch_add_u64(&shardStatInfo[index].size, new_size);
            }
            break;
        case CMD_DELETE:
            {
                pg_atomic_fetch_add_u64(&shardStatInfo[index].ntuples_delete, 1);

                pg_atomic_fetch_sub_u64(&shardStatInfo[index].ntuples, 1);

                pg_atomic_fetch_sub_u64(&shardStatInfo[index].size, old_size);
            }
            break;
        default:
            elog(LOG, "Unsupported CmdType %d in UpdateShardStatistic", cmd);
            break;
    }
}

static void
InitShardStatistic(ShardStatistic *stat)
{
    pg_atomic_init_u64(&stat->ntuples, 0);
    pg_atomic_init_u64(&stat->ntuples_delete, 0);
    pg_atomic_init_u64(&stat->ntuples_update, 0);
    pg_atomic_init_u64(&stat->ntuples_insert, 0);
    pg_atomic_init_u64(&stat->ntuples_select, 0);
    pg_atomic_init_u64(&stat->size, 0);
}

/* fetch from shard statistic from source, add to dest */
static void
FetchAddShardStatistic(ShardStatistic *dest, ShardStatistic *src)
{
    uint64 result = 0;

    result = pg_atomic_read_u64(&src->ntuples);
    pg_atomic_fetch_add_u64(&dest->ntuples, result);

    result = pg_atomic_read_u64(&src->ntuples_delete);
    pg_atomic_fetch_add_u64(&dest->ntuples_delete, result);

    result = pg_atomic_read_u64(&src->ntuples_update);
    pg_atomic_fetch_add_u64(&dest->ntuples_update, result);

    result = pg_atomic_read_u64(&src->ntuples_insert);
    pg_atomic_fetch_add_u64(&dest->ntuples_insert, result);

    result = pg_atomic_read_u64(&src->ntuples_select);
    pg_atomic_fetch_add_u64(&dest->ntuples_select, result);

    result = pg_atomic_read_u64(&src->size);
    pg_atomic_fetch_add_u64(&dest->size, result);
}
/* write shard statistic info into file */
void
FlushShardStatistic(void)
{
    int i      = 0;
    int j      = 0;
    int fd     = 0;
    int ret    = 0;
    int nelems = MAX_SHARDS;
    int npools = (MaxBackends / g_MaxSessionsPerPool) + 1;
    int size   = sizeof(ShardRecord) * nelems;
    static ShardRecord rec[MAX_SHARDS];

    /* init all shard records */
    for (i = 0; i < nelems; i++)
    {
        InitShardStatistic(&rec[i].stat);
        INIT_CRC32C(rec[i].crc);
    }

    /* merge shard statistic */
    for (i = 0; i < npools; i++)
    {
        for (j = 0; j < nelems; j++)
        {
            int index = i * nelems + j;

            FetchAddShardStatistic(&rec[j].stat, &shardStatInfo[index]);
        }
    }

    /* calculate crc for each record */
    for (i = 0; i < nelems; i++)
    {
        COMP_CRC32C(rec[i].crc, (char *)&rec[i].stat, sizeof(ShardStatistic));
        FIN_CRC32C(rec[i].crc);
    }

    /* write record into file */
    fd = open(SHARD_STATISTIC_FILE_PATH, O_RDWR | O_TRUNC | O_CREAT, S_IRUSR | S_IWUSR);

    if (fd == -1)
    {
        return;
    }

    ret = write(fd, rec, size);
    if (ret != size)
    {
        close(fd);
        return;
    }

    close(fd);
}

void
RecoverShardStatistic(void)
{
    int i      = 0;
    int fd     = 0;
    int ret    = 0;
    int nelems = MAX_SHARDS;
    int size   = sizeof(ShardRecord) * nelems;
    ShardRecord *rec = (ShardRecord *)malloc(size);

    if(access(SHARD_STATISTIC_FILE_PATH, F_OK) == 0)
    {
        fd = open(SHARD_STATISTIC_FILE_PATH, O_RDONLY, S_IRUSR | S_IWUSR);
        if (fd == -1)
        {
            elog(LOG, "could not open file \"%s\"", SHARD_STATISTIC_FILE_PATH);
            goto end;
        }

        ret = read(fd, rec, size);
        if (ret != size)
        {
            elog(LOG, "failed to read file \"%s\"", SHARD_STATISTIC_FILE_PATH);
            close(fd);
            goto end;
        }

        close(fd);

        for (i = 0; i < nelems; i++)
        {
            pg_crc32c crc;

            INIT_CRC32C(crc);

            COMP_CRC32C(crc, (char *)&rec[i].stat, sizeof(ShardStatistic));
            FIN_CRC32C(crc);

            if (crc == rec[i].crc)
            {
                FetchAddShardStatistic(&shardStatInfo[i], &rec[i].stat);
            }
            else
            {
                elog(LOG, "shard %d statistic info CRC-ERROR", i);
            }
        }
    }

end:
    free(rec);
}

static void
RefreshShardStatistic(ShardStat *shardstat)
{
    int i = 0;
    int nelems = MAX_SHARDS;

    if (!shardstat)
    {
        return;
    }

    for (i = 0; i < nelems; i++)
    {
        if (shardstat[i].count)
        {
            pg_atomic_fetch_add_u64(&shardStatInfo[i].ntuples, shardstat[i].count);

            pg_atomic_fetch_add_u64(&shardStatInfo[i].size, shardstat[i].size);
        }
    }
}
void
ResetShardStatistic(void)
{
    int i = 0;
    int j = 0;
    int nelems = MAX_SHARDS;
    int npools = (MaxBackends / g_MaxSessionsPerPool) + 1;

    for (i = 0; i < nelems; i++)
    {
        for (j = 0; j < npools; j++)
        {
            int index = j * nelems + i;

            pg_atomic_init_u64(&shardStatInfo[index].ntuples, 0);
            pg_atomic_init_u64(&shardStatInfo[index].size, 0);
        }
    }
}

/* display shard statistic info */
Datum  
tbase_shard_statistic(PG_FUNCTION_ARGS)
{// #lizard forgives
#define NCOLUMNS 9
    FuncCallContext     *funcctx;
    ShmMgr_State    *status  = NULL;
    ShardStatistic *rec;
    int nelems = MAX_SHARDS;
    int npools = (MaxBackends / g_MaxSessionsPerPool) + 1;
    int size   = sizeof(ShardStatistic) * nelems;
    
    if (SRF_IS_FIRSTCALL())
    {        
        MemoryContext oldcontext;
        TupleDesc      tupdesc;
        int i = 0;
        int j = 0;
        
        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        
        tupdesc = CreateTemplateTupleDesc(NCOLUMNS, false);
        TupleDescInitEntry(tupdesc, (AttrNumber) 1, "group_name",
                           TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 2, "node_name",
                           TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 3, "shard_id",
                           INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 4, "ntups_select",
                           INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 5, "ntups_insert",
                           INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 6, "ntups_update",
                           INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 7, "ntups_delete",
                           INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 8, "size",
                           INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 9, "ntups",
                           INT8OID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        status = (ShmMgr_State *) palloc(sizeof(ShmMgr_State));
        funcctx->user_fctx = (void *) status;

        status->currIdx = 0;
        rec = (ShardStatistic *)palloc(size);

        for (i = 0; i < nelems; i++)
        {
            InitShardStatistic(&rec[i]);
        }

		LWLockAcquire(ShardMapLock, LW_SHARED);	
        for (i = 0; i < npools; i++)
        {
            for (j = 0; j < nelems; j++)
            {
                if (show_all_shard_stat || IS_PGXC_COORDINATOR ||
                   !g_DatanodeShardgroupBitmap ||
                   g_GroupShardingMgr_DN->members->shardMapStatus != SHMEM_SHRADMAP_STATUS_USING ||
                   bms_is_member(j, g_DatanodeShardgroupBitmap))
                {
                                   
                    int index = i * nelems + j;

                    FetchAddShardStatistic(&rec[j], &shardStatInfo[index]);
                }
            }
        }
		LWLockRelease(ShardMapLock);

        status->rec = rec;
        MemoryContextSwitchTo(oldcontext);
    }


    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    status  = (ShmMgr_State *) funcctx->user_fctx;
    rec = status->rec;
    
    while (status->currIdx < nelems)
    {
        Datum        values[NCOLUMNS];
        bool        nulls[NCOLUMNS];
        HeapTuple    tuple;
        Datum        result;
        char        *group_name = NULL;

        MemSet(values, 0, sizeof(values));
        MemSet(nulls,  0, sizeof(nulls));

        group_name = GetMyGroupName();

        if (group_name)
        {
            values[0] = CStringGetTextDatum(group_name);
        }
        else
        {
            nulls[0] = true;
        }
        values[1] = CStringGetTextDatum(PGXCNodeName);
        values[2] = Int32GetDatum(status->currIdx);
        values[3] = Int64GetDatum(pg_atomic_read_u64(&rec[status->currIdx].ntuples_select));
        values[4] = Int64GetDatum(pg_atomic_read_u64(&rec[status->currIdx].ntuples_insert));
        values[5] = Int64GetDatum(pg_atomic_read_u64(&rec[status->currIdx].ntuples_update));
        values[6] = Int64GetDatum(pg_atomic_read_u64(&rec[status->currIdx].ntuples_delete));
        values[7] = Int64GetDatum(pg_atomic_read_u64(&rec[status->currIdx].size));
        values[8] = Int64GetDatum(pg_atomic_read_u64(&rec[status->currIdx].ntuples));

        status->currIdx++;

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    }
    
    SRF_RETURN_DONE(funcctx);
}

#ifdef __COLD_HOT__
static void LoadAccessControlInfo(void)
{    
    struct stat stat_buf;
    if (0 == stat(ACCESS_CONTROL_FILE_NAME, &stat_buf))
    {
        g_AccessCtl->colddata = true;
    }
}

/*
 * Add dual write info
 */
static bool 
AddDualWriteInfo(Oid relation, AttrNumber attr, int32 gap, char *table, char *column, char *value)
{
    bool  found;
    DTag  tag;
    DualWriteRecord *ent;    

    tag.relation     = relation;
    tag.attr         = attr;
    tag.timeoffset  = gap;    
    
    ent = (DualWriteRecord*)hash_search(g_DualWriteCtl->dwhash, (void *) &tag, HASH_ENTER, &found);    
    if (!ent)
    {
        elog(ERROR, "corrupted shared hash table");
    }
    
    if (!found)
    {
        snprintf(ent->table, NAMEDATALEN, "%s", table);
        snprintf(ent->column, NAMEDATALEN, "%s", column);
        snprintf(ent->value, NAMEDATALEN, "%s", value);
        g_DualWriteCtl->entrynum++;
    }
    return found;
}

/*
 * Only called when startup, no lock needed
 */
static void LoadDualWriteInfo(void)
{// #lizard forgives    
    char       *relOid;
    char       *columnidx;
    char       *gapptr;
    char       *table;
    char       *columnstr;   
    char       *valuestr;
    char       *p;
    
    struct stat stat_buf;
    FILE       *fp = NULL;    
    char       buffer[MAXPGPATH];
    /* try major file */    
    if (0 == stat(DUAL_WRITE_FILE_NAME, &stat_buf))
    {
        /* use major file */
        fp = AllocateFile(DUAL_WRITE_FILE_NAME, "r+");
        if (!fp)
        {
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("could not open file \"%s\": %m",
                            DUAL_WRITE_FILE_NAME)));
        }        
    }
    else if (0 == stat(DUAL_WRITE_BACKUP_FILE_NAME, &stat_buf))
    {
        /* use backup file */
        fp = AllocateFile(DUAL_WRITE_BACKUP_FILE_NAME, "r+");
        if (!fp)
        {
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("could not open file \"%s\": %m",
                            DUAL_WRITE_BACKUP_FILE_NAME)));
        }
    }    

    if (fp)
    {
        while (fgets(buffer,  MAXPGPATH, fp))
        {
            p = buffer;
            
            relOid    = p;
            columnidx = NULL;
            columnstr = NULL;
            gapptr    = NULL;
            valuestr  = NULL;
            table     = NULL;
            
            /* split string */
            while (*p != '\0')
            {
                if (',' == *p)
                {
                    if (NULL == columnidx)
                    {
                        columnidx = p + 1;                    
                    }
                    else if (NULL == gapptr)
                    {
                        gapptr = p + 1;                    
                    }
                    else if (NULL == table)
                    {
                        table = p + 1;                    
                    }
                    else if (NULL == columnstr)
                    {
                        columnstr = p + 1;                    
                    }
                    else if (NULL == valuestr)
                    {
                        valuestr = p + 1;    
                    }
                    *p = '\0'; 
                }
                else if ('\n' == *p)
                {
                    *p = '\0'; 
                }
                p++;
            }
            AddDualWriteInfo((Oid)atoi(relOid), (AttrNumber)atoi(columnidx), (int32)atoi(gapptr), table, columnstr, valuestr);    
        }
        FreeFile(fp);
    }
}


Size DualWriteTableSize(void)
{
    Size size = 0;
    
    if (IS_PGXC_COORDINATOR)
    {
        size += hash_estimate_size(MAX_DUAL_WRITE_TABLE, sizeof(DualWriteRecord));
        size = add_size(size, MAXALIGN64(sizeof(DualWriteCtl)));
    }    
    else if (IS_PGXC_DATANODE)
    {
        size = add_size(size, MAXALIGN64(sizeof(AccessControl)));
    }
    return size;
}


void DualWriteCtlInit(void)
{
    if(IS_PGXC_COORDINATOR)
    {
        bool         found;
        HASHCTL        info;
        g_DualWriteCtl = (DualWriteCtl *)ShmemInitStruct("Dual write control",
                                                                sizeof(DualWriteCtl),
                                                                &found);
        if (found)
        {
            elog(FATAL, "invalid shmem status when dual write control ");
        }
        
        g_DualWriteCtl->dwhash   = NULL;
        SpinLockInit(&g_DualWriteCtl->lock);
        g_DualWriteCtl->needlock = false;
        g_DualWriteCtl->entrynum = 0;

        /* init dual write hash table */
        info.keysize   = sizeof(DTag);
        info.entrysize = sizeof(DualWriteRecord);
        info.hash = tag_hash;

        g_DualWriteCtl->dwhash = ShmemInitHash("dual write hash table",
                                      MAX_DUAL_WRITE_TABLE, 
                                      MAX_DUAL_WRITE_TABLE,
                                      &info,
                                      HASH_ELEM | HASH_FUNCTION);    

        if (!g_DualWriteCtl->dwhash)
        {
            elog(FATAL, "invalid shmem status when creating dual write hash table");
        }    
        LoadDualWriteInfo();
        g_AccessCtl = NULL;
    }
    else if(IS_PGXC_DATANODE)
    {
        bool         found;
        g_AccessCtl = (AccessControl *)ShmemInitStruct("access control",
                                                        sizeof(AccessControl),
                                                        &found);
        if (found)
        {
            elog(FATAL, "invalid shmem status when create access control");
        }        
         
        LoadAccessControlInfo();
        g_DualWriteCtl = NULL;
    }
}

/*
 * Return node access status, true : cold , false : hot
 */
bool pg_get_node_access(void)
{
    return g_AccessCtl->colddata ? true : false;
}

static Datum
pg_set_cold_access(void)
{
    struct stat stat_buf;    
    char   returnstr[MAXPGPATH] = {0};
    FILE   *fp;

    if(!IS_PGXC_DATANODE)
    {
        elog(ERROR, "can only called on datanode");
    }    
    
    /* remove dual write info from share memory */
    g_AccessCtl->needlock = true;
    LWLockAcquire(ColdAccessLock, LW_EXCLUSIVE);
    if (0 == stat(ACCESS_CONTROL_FILE_NAME, &stat_buf))
    {
        
    }    
    else
    {
        fp = AllocateFile(ACCESS_CONTROL_FILE_NAME, "w+");
        if (!fp)
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("could not open file \"%s\": %m",
                            ACCESS_CONTROL_FILE_NAME)));        
        FreeFile(fp);
    }
    g_AccessCtl->colddata = true;
    LWLockRelease(ColdAccessLock);
    
    g_AccessCtl->needlock = false;/* tell others to release lock */
    snprintf(returnstr, MAXPGPATH, "success");
    PG_RETURN_TEXT_P(cstring_to_text(returnstr));
}

static Datum
pg_clear_cold_access(void)
{
    char   returnstr[MAXPGPATH] = {0};
    struct stat stat_buf;    

	if(!IS_PGXC_DATANODE)
	{
		elog(ERROR, "can only called on datanode");
	}	
    
    /* remove dual write info from share memory */
    g_AccessCtl->needlock = true;
    LWLockAcquire(ColdAccessLock, LW_EXCLUSIVE);

    /* remove backup file */
    if (0 == stat(ACCESS_CONTROL_FILE_NAME, &stat_buf))
    {
        if (unlink(ACCESS_CONTROL_FILE_NAME) < 0)
        {
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("could not unlink file \"%s\": %m", ACCESS_CONTROL_FILE_NAME)));
        }
    }
    g_AccessCtl->colddata = false;
    LWLockRelease(ColdAccessLock);
    g_AccessCtl->needlock = false;/* tell others to release lock */
    snprintf(returnstr, MAXPGPATH, "success");
    PG_RETURN_TEXT_P(cstring_to_text(returnstr));
}

/*
 * Need dual write or not
 */
bool NeedDualWrite(Oid relation, AttrNumber attr, Datum value)
{// #lizard forgives
    bool  found;
    bool  needlock;
    int32 gap;
    DTag  tag;
    int32        partitionStrategy        = 0;
    Relation                  rel            = NULL;
    Form_pg_partition_interval routerinfo   = NULL;

    needlock = g_DualWriteCtl->needlock;

    //LogDualWriteInfo(needlock);
    
    /*no one is trying to add dual write logical */
    if (0 == g_DualWriteCtl->entrynum && false == needlock)
    {
        return false;
    }

    /* get partition stragegy first */
    rel          = relation_open(relation, NoLock);
    routerinfo   = rel->rd_partitions_info;
    if (routerinfo)
    {
        if (attr == routerinfo->partpartkey)
        {
            partitionStrategy = routerinfo->partinterval_type;
            if (partitionStrategy == IntervalType_Month &&
                routerinfo->partinterval_int == 12)
            {
                partitionStrategy = IntervalType_Year;
            }
        }
    }
    relation_close(rel, NoLock);
  
    
    gap             = get_timestamptz_gap(value, partitionStrategy);
    tag.relation     = relation;
    tag.attr         = attr;
    tag.timeoffset  = gap;
    
    
    if (needlock)
    {
        LWLockAcquire(DualWriteLock, LW_SHARED);
    }
    
    (void)hash_search(g_DualWriteCtl->dwhash, (void *) &tag, HASH_FIND, &found);    

    if (needlock)
    {
        LWLockRelease(DualWriteLock);
    }
    return found;
}

/*
 * check value whether temp key value
 */
static bool IsTempKeyValue(KeyValuePair* p_KeyValuePair, Oid relation, char *value, Oid *hotGroup, Oid *coldGroup)
{
    if (p_KeyValuePair && OidIsValid(p_KeyValuePair->table))
    {
        if (p_KeyValuePair->table == relation)
        {
            if (0 == strncmp(value,NameStr(p_KeyValuePair->value), NAMEDATALEN))
            {
                if (hotGroup)
                {
                    *hotGroup  = p_KeyValuePair->hotGroup;
                }

                if (coldGroup)
                {
                    *coldGroup = p_KeyValuePair->coldGroup;
                }
                return true;
            }
        }
    }
    return false;
}

/*
 * check value in temp key value list
 */
bool InTempKeyValueList(Oid relation, char *value, Oid *hotGroup, Oid *coldGroup)
{
    ListCell *lc;
    KeyValuePair *p_KeyValuePair = NULL;
    if (NIL != g_TempKeyValueList)
    {
        foreach(lc, g_TempKeyValueList)
        {
            p_KeyValuePair = (KeyValuePair*)lfirst(lc);
            if (IsTempKeyValue(p_KeyValuePair, relation, value, hotGroup, coldGroup))
            {
                return true;
            }
        }
    }
    
    return false;
}

/*
 * Get group's oid by group name.
 */
static Oid GetGroupOidByName(char *groupname)
{
    if (!groupname)
    {
        return InvalidOid;
    }
    return get_pgxc_groupoid(groupname);
}

/*
 * Get table's oid by table name.
 */
static Oid GetTableOidByName(char *schema_tblname)
{
    char schema[STRINGLENGTH]      = {0};
    char tablename[STRINGLENGTH] = {0};
    char *tmp;
    Oid  schema_oid;
    Oid  table_oid;
    if (!schema_tblname)
    {
        return InvalidOid;
    }
    
    tmp = strchr(schema_tblname, '.');
    memcpy(schema, schema_tblname, tmp - schema_tblname);
    memcpy(tablename, tmp + 1, strlen(schema_tblname) - (tmp - schema_tblname + 1));
    /* get table schema */
    schema_oid = get_namespace_oid(schema, false);
    if(InvalidOid == schema_oid)
    {
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_SCHEMA),
                 errmsg("schema \"%s\" does not exist.", schema)));
    }
    /* get table name */

    table_oid = get_relname_relid(tablename, schema_oid);
    if(InvalidOid == table_oid)
    {
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                 errmsg("table \"%s\" does not exist.", tablename)));
    }
    
    return table_oid;
}

/*
 * extract tblNames, value, hotGroupName, coldGroupName from user's input string.
 * string: public.test1,public.test2,public.test3,public.test4 5 gp0 gp1
 */
static bool ExtractPara(const char *str, List **p_tblNames, char *value, Size *valueLength, char *hotGroupName, char *coldGroupName)
{// #lizard forgives
    char buffer[STRINGLENGTH]   = {0};
    Size cnt                    = 1;
    char *p_str                 = buffer;
    char *mid                   = buffer;
    char *tmp                   = NULL;
    const char *tmp2                = NULL;
    char *singleTableName        = NULL;
    Size length                    = 0;
    Size continuousSpaceCount   = 0;
    Size headSpaceCount            = 0;
    MemoryContext old_cxt;

    if (NULL == str)
    {
        return false;
    }
    
    if (str[0] == '\0')
    {
        return false;
    }

    /* tmp points to the first non-space character in str */
    tmp2 = str;
    while (isspace(*tmp2))
    {
        headSpaceCount++;
        tmp2++;
    }
    
    old_cxt = MemoryContextSwitchTo(TopMemoryContext);
    
    /*erase last spaces*/
    memcpy(buffer, tmp2, strlen(str) - headSpaceCount);
    tmp = &buffer[strlen(str) - 1 - headSpaceCount];
    while (isspace(*tmp))
    {
        *tmp = '\0';
        tmp--;
    }
    
    if (*p_str == ' ')
    {
        p_str++;
    }

    /* first tablename to [last-1] tablename*/
    while ((mid = strchr(p_str, ',')))
    {
        tmp = mid;
        while (*(mid-1) == ' ')
            mid--;
        length = mid - p_str;
        singleTableName = (char*)palloc0(length+1);
        memcpy(singleTableName, p_str, length);
        *p_tblNames = lappend(*p_tblNames, (void*)singleTableName);

        p_str = tmp + 1;
        while (*p_str == ' ')
            p_str++;
    }

    while ((mid = strchr(p_str, ' ')))
    {
        continuousSpaceCount++;
        tmp = mid;
        while (*(mid-1) == ' ')
            mid--;
        length = mid - p_str;
        /* position of last tablename */
        if (1 == cnt)
        {
            singleTableName = (char*)palloc0(length+1);
            memcpy(singleTableName, p_str, length);
            *p_tblNames = lappend(*p_tblNames, (void*)singleTableName);
        }
        /* position of value */
        else if (2 == cnt)
        {
            memcpy(value, p_str, length);
            *valueLength = length;
        }
        /* position of hotgroupname */
        else if (3 == cnt)
        {
            memcpy(hotGroupName, p_str, length);
        }
        cnt++;
        p_str = tmp + 1;
        while (*p_str == ' ')
            p_str++;
    }

    /* 
     *validate user input by check space count.
     *in normal case, total continuous space count between strings will be 3.
     */
    if (3 != continuousSpaceCount)
    {
        MemoryContextSwitchTo(old_cxt);
        ereport(ERROR,
                (errcode(ERRCODE_WARNING),
                 errmsg("Invalid input. Correct format: set temp_key_value='schmea.tbl,schema2.tbl2 value hotgp coldgp'" )));
        return false;
    }

    /* position of coldgroupname */
    cnt = 0;
    while (*p_str != '\0')
    {
        coldGroupName[cnt] = *p_str;
        p_str++;
        cnt++;
    }
    
    MemoryContextSwitchTo(old_cxt);
    return true;
}

/*
 * Set temp Key value as g_TempKeyValueList's member
 */
void SetTempKeyValueList(const char *str, void *extra)
{// #lizard forgives
    List      *tblNames                      = NIL;
    ListCell  *lc                          = NULL;
    char      value[STRINGLENGTH]           = {0};
    Size       valueLength                  = 0;
    char      hotGroupName[STRINGLENGTH]  = {0};
    char      coldGroupName[STRINGLENGTH] = {0};
    Oid       hotGroupOid                 = InvalidOid;
    Oid          coldGroupOid                = InvalidOid;
    KeyValuePair *p_KeyValuePair          = NULL;
    MemoryContext old_cxt;
    
    old_cxt = MemoryContextSwitchTo(TopMemoryContext);

    /* set more than once will cover last set operation.*/
    if (g_TempKeyValueList)
    {
        list_free_deep(g_TempKeyValueList);
        g_TempKeyValueList = NIL;
    }
    
    /*
     * str format: schemaA.tbl1,schemaB.tbl2 value hotgroupname coldgroupname
     */
    if (ExtractPara(str, &tblNames, value, &valueLength, hotGroupName, coldGroupName))
    {
        if (!IS_PGXC_DATANODE)
        {
            hotGroupOid  = GetGroupOidByName(hotGroupName);
            coldGroupOid = GetGroupOidByName(coldGroupName);
            /*validate hotGroupOid and coldGroupOid*/
            if (InvalidOid == hotGroupOid || InvalidOid == coldGroupOid)
            {
                MemoryContextSwitchTo(old_cxt);
                if (InvalidOid == hotGroupOid)
                {
                    ereport(ERROR,
                            (errcode(ERRCODE_UNDEFINED_OBJECT),
                             errmsg("hot group \"%s\" does not exist", 
                             hotGroupName)));
                }
                if (InvalidOid == coldGroupOid)
                {
                    ereport(ERROR,
                            (errcode(ERRCODE_UNDEFINED_OBJECT),
                             errmsg("cold group \"%s\" does not exist", 
                             coldGroupName)));
                }
                g_TempKeyValueList = NIL;
                return ;
            }
        }
                
        foreach(lc, tblNames)
        {
            /*set single key value*/
            p_KeyValuePair = (KeyValuePair*)palloc0(sizeof(KeyValuePair));
            
            p_KeyValuePair->table       = GetTableOidByName((char*)lfirst(lc));
            memcpy(p_KeyValuePair->value.data, value, valueLength); 
            p_KeyValuePair->hotGroup  = hotGroupOid;
            p_KeyValuePair->coldGroup = coldGroupOid;
            /*add to list*/
            g_TempKeyValueList = lappend(g_TempKeyValueList, (void*)p_KeyValuePair);
        }
    }

    /*free talNames itself*/
    if (tblNames)
    {
        list_free_deep(tblNames);    
    }
    MemoryContextSwitchTo(old_cxt);

    return ;
}

/* We use second offset as key value offset */
long get_keyvalue_offset(TimestampTz endtime)
{
    long secs      = 0;
    int  microsecs = 0;
    
    if (0 == g_keyvalue_base_timestamp)
    {
        if (tm2timestamp(&g_keyvalue_base_time, 0, NULL, &g_keyvalue_base_timestamp))
        {
            ereport(ERROR,
                    (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                        errmsg("transfer timestamp failed")));
        }
    }
    
    TimestampDifference(g_keyvalue_base_timestamp, endtime, &secs, &microsecs);
    if (0 == secs && 0 == microsecs)
    {
        ereport(ERROR,
                    (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                        errmsg("invalid timestamp value when get second offset, timestamp value must be after 2000-01:01-00:00:00")));
    }

    return secs;
}

static long 
compute_keyvalue_hash(Oid type, Datum value)
{
    if (PARTITION_KEY_IS_TIMESTAMP(type))
    {
        return get_keyvalue_offset(value);
    }
    else
    {
        return compute_hash(type, value, LOCATOR_TYPE_SHARD);    
    }    
}

/*
 * get hot gap by partition interval type
 */
int32 GetHotDataGap(int32 interval)
{
    int gap;
    switch (interval)
    {
        case IntervalType_Month:
        {
            if (g_ManualHotDataGapWithMonths)
            {
                gap = date_diff(&g_ManualHotDataTime);

                return gap + 1;
            }
            
            return  g_ColdDataThreashold / 30;
        }
        
        case IntervalType_Day:
        {
            if (g_ManualHotDataGapWithDays)
            {
                gap = date_diff_indays(&g_ManualHotDataTime);

                return gap + 1;
            }
            
            return  g_ColdDataThreashold;
        }

        default:
        {
            ereport(ERROR,
                    (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                        errmsg("partition interval %d not support hot and cold seperation", interval)));
            return -1;
        }
    }   
}

/* 
    if temp_cold_date guc option is available, new mod needs to be checked whether treating as cold date.
    reture true, this data would be treated as cold data, else as hot data.
*/
static bool IsTempColdData(Datum secValue, RelationAccessType access, int32 interval,
                                int step, Datum startValue)
{
    if ((RELATION_ACCESS_INSERT == access || RELATION_ACCESS_UPDATE == access) && ('\0' != g_TempColdDate[0]))
    {
        if (is_sec_meet_temp_cold_date(secValue, interval, step, startValue))
        {
            /* return this is one cold data */
            return true;
        }

        elog(ERROR, "Get invalid data when temp_cold_date(%d-%d-%d) is set",
            g_TempColdDataTime.tm_year, g_TempColdDataTime.tm_mon, g_TempColdDataTime.tm_mday);
    }

    return false;
}


/*
 * Data is hot or not
 */
bool IsHotData(Datum secValue, RelationAccessType access, int32 interval,
                  int step, Datum startValue)
{
    Timestamp hotDataTime;    
    
	if (enable_cold_hot_router_print)
	{
		elog(LOG, "IsHotData Check value "INT64_FORMAT" access %d interval %d step %d "INT64_FORMAT,
		     DatumGetInt64(secValue),
		     access, interval, step,
		     DatumGetInt64(startValue));
	}


    /* trade temp cold data as cold data. checking is needed if data would satisfy temp_cold_date guc option */
    if (true == IsTempColdData(secValue, access, interval, step, startValue))
    {
    	if (enable_cold_hot_router_print)
	    {
		    elog(LOG, "Return from TempColdData Value: %s", g_TempColdDate ? g_TempColdDate : "(null)");
	    }
        return false;
    }
#if 0
    gap = get_timestamptz_diff(secValue, interval);

    return gap < GetHotDataGap(interval);
#endif
    if (tm2timestamp(&g_ManualHotDataTime, 0, NULL, &hotDataTime) != 0)
    {
        ereport(ERROR,
            (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
             errmsg("timestamp out of range")));
    }

	if (enable_cold_hot_router_print)
	{
		elog(LOG,"IsHotData Check hotDateTime "INT64_FORMAT
				" Manual hot data time "
				"{ tm_sec:%d tm_min:%d tm_hour:%d tm_mday:%d tm_mon:%d tm_year:%d tm_wday:%d tm_yday:%d"
				"  tm_isdst:%d tm_gmtoff:%ld tm_zone:%s } ret: %d",
             (int64)hotDataTime,
			 g_ManualHotDataTime.tm_sec,
			 g_ManualHotDataTime.tm_min,
			 g_ManualHotDataTime.tm_hour,
			 g_ManualHotDataTime.tm_mday,
			 g_ManualHotDataTime.tm_mon,
			 g_ManualHotDataTime.tm_year,
			 g_ManualHotDataTime.tm_wday,
			 g_ManualHotDataTime.tm_yday,
			 g_ManualHotDataTime.tm_isdst,
			 g_ManualHotDataTime.tm_gmtoff,
			 g_ManualHotDataTime.tm_zone,
			 ((Timestamp)secValue >= hotDataTime));
	}

    return ((Timestamp)secValue >= hotDataTime);
}


List* ShardMapRouter(Oid group, Oid coldgroup, Oid relation, Oid type, Datum dvalue, AttrNumber secAttr, Oid secType, 
                    bool isSecNull, Datum secValue, RelationAccessType accessType)
{// #lizard forgives    
    int16        typlen                      = 0;
    bool        typbyval                  = false;
    bool        bdualwrite                  = false;
    char        typalign                  = '\0';
    char        typdelim                  = '\0';    
    Oid         typioparam                  = InvalidOid ;
    Oid         typiofunc                  = InvalidOid;
    Oid         keyValueGroup              = InvalidOid;
    Oid         secColdGroup              = InvalidOid;
    long        hashvalue                  = 0;
    long        sechashvalue              = 0;
    int32        i                          = 0;    
    char        *value                      = NULL;
    char        *timestr     = NULL;
    List        *list                      = NULL;
    int         dn_num                      = 0;
    int32        *datanodes                  = NULL;    
    int32         partitionStrategy          = 0;
    int32        interval_step            = 0;
    TimestampTz  start_timestamp          = 0;
    Relation                 rel          = NULL;
    Form_pg_partition_interval routerinfo = NULL;    
	bool         router_log_print         = false;

    rel = relation_open(relation, NoLock);

    if (RELATION_IS_CHILD(rel))
    {
        relation = RELATION_GET_PARENT(rel);
    }

    relation_close(rel, NoLock);

	router_log_print = (enable_cold_hot_router_print && accessType == RELATION_ACCESS_INSERT &&
						(RELATION_IS_INTERVAL(rel) || RELATION_IS_CHILD(rel)));

    if (g_EnableKeyValue)
    {
        /* check whether the value is key value */
        get_type_io_data(type, IOFunc_output,
                         &typlen, &typbyval,
                         &typalign, &typdelim,
                         &typioparam, &typiofunc);
        value = OidOutputFunctionCall(typiofunc, dvalue);
    }

    if (g_EnableDualWrite)
    {
        if (!isSecNull && PARTITION_KEY_IS_TIMESTAMP(secType) && secAttr != InvalidAttrNumber && 
            accessType != RELATION_ACCESS_READ && accessType != RELATION_ACCESS_READ_FQS)
        {
            bdualwrite = NeedDualWrite(relation, secAttr, secValue);
            if (bdualwrite)
            {
                elog(LOG, "distribute key:%s timestamp:%s need dual write", value, timestamptz_to_str((TimestampTz) secValue));
            }
        }
    }
    else
    {
        bdualwrite = false;
    }
    
	if (router_log_print)
	{
		elog(LOG, "Group %d coldgroup %d relation %d secAttr %d isSecNull %d dualwrite %d",
		     group, coldgroup, relation, secAttr, isSecNull, bdualwrite);
	}
    
    /* get partition stragegy first */
    if (!isSecNull && secAttr != InvalidAttrNumber)
    {
        rel          = relation_open(relation, NoLock);
        routerinfo     = rel->rd_partitions_info;
        if (routerinfo)
        {
            if (secAttr == routerinfo->partpartkey)
            {
                partitionStrategy = routerinfo->partinterval_type;

                if (partitionStrategy == IntervalType_Month &&
					routerinfo->partinterval_int == COLD_HOT_INTERVAL_YEAR)
                {
                    partitionStrategy = IntervalType_Year;
                }
            }

            interval_step = routerinfo->partinterval_int;
            start_timestamp = routerinfo->partstartvalue_ts;

			if (router_log_print)
			{
                elog(LOG, "has routerinfo %d", partitionStrategy);
			}
        }
		else if (router_log_print)
        {
            elog(LOG, "no routerinfo %d", partitionStrategy);
        }

        relation_close(rel, NoLock);
    }

    if (g_EnableKeyValue)
    {    
        if (!InTempKeyValueList(relation, value, &keyValueGroup, &secColdGroup))
        {
            keyValueGroup = GetKeyValuesGroup(MyDatabaseId, relation, value, &secColdGroup);
        }
        pfree(value);
        value = NULL;
    }
    
    if (InvalidOid == keyValueGroup)
    {        
        /* not the key value, use common map strategy */
        hashvalue = compute_hash(type, dvalue, LOCATOR_TYPE_SHARD);

        /* no second distribute key, trade as hot data*/
        if (InvalidAttrNumber == secAttr)
        {
            return list_make1_int(GetNodeIndexByHashValue(group, hashvalue));
        }
        
        /* we have exact second distribute key */
        if (!isSecNull)
        {
            if (!bdualwrite)
            {
                /* has cold data strategy */
                if (OidIsValid(coldgroup))
                {
                    /* hot data, only timestamp field can be divided into hot and cold data */
                    if (PARTITION_KEY_IS_TIMESTAMP(secType) && IsHotData(secValue, accessType, partitionStrategy, interval_step, start_timestamp))
                    {
                        return list_make1_int(GetNodeIndexByHashValue(group, hashvalue));
                    }
                    else
                    {
                        /* cold data */
                        return list_make1_int(GetNodeIndexByHashValue(coldgroup, hashvalue));
                    }
                    
                }
                else
                {
                    /* only timestamp field can be divided into hot and cold data */
                    if (PARTITION_KEY_IS_TIMESTAMP(secType) && !IsHotData(secValue, accessType, partitionStrategy, interval_step, start_timestamp))
                    {
                        /* for first distribute column */
                        get_type_io_data(type, IOFunc_output,
                                         &typlen, &typbyval,
                                         &typalign, &typdelim,
                                         &typioparam, &typiofunc);
                        value = OidOutputFunctionCall(typiofunc, dvalue);

                        /* for timestamp */
                        get_type_io_data(TIMESTAMPOID, IOFunc_output,
                                         &typlen, &typbyval,
                                         &typalign, &typdelim,
                                         &typioparam, &typiofunc);
                        
                        timestr = OidOutputFunctionCall(typiofunc,    secValue);
                
                        elog(ERROR, "distribute key:%s timestamp:%s is cold data, but table has no cold group", 
                            value, timestr);
                    }
                    return list_make1_int(GetNodeIndexByHashValue(group, hashvalue));
                }
            }
            else
            {
                /* need dual write, read access route to hot group */
                if (RELATION_ACCESS_READ == accessType ||
                    RELATION_ACCESS_READ_FQS == accessType)
                {
                    return list_make1_int(GetNodeIndexByHashValue(group, hashvalue));
                }
                else
                {
                    /* if meet temp_cold_date guc option, modifications are only send to cold nodes */
                    if (PARTITION_KEY_IS_TIMESTAMP(secType) && true == IsTempColdData(secValue, accessType, partitionStrategy, interval_step, start_timestamp))
                    {
                        if (OidIsValid(coldgroup))
                        {
                            list = lappend_int(list, GetNodeIndexByHashValue(coldgroup, hashvalue));
                            return list;
                        }
                        elog(ERROR, "NO cold datanode to send when temp_cold_date(%d-%d-%d) is set",
                            g_TempColdDataTime.tm_year, g_TempColdDataTime.tm_mon, g_TempColdDataTime.tm_mday);
                    }
                    
                    list = list_make1_int(GetNodeIndexByHashValue(group, hashvalue));
                    if (OidIsValid(coldgroup))
                    {
                        list = lappend_int(list, GetNodeIndexByHashValue(coldgroup, hashvalue));
                    }                    
                    return list;
                }
            }
        }
        else
        {
            /* now we have send query to hot and cold group the same time */
            list = list_make1_int(GetNodeIndexByHashValue(group, hashvalue));    
            if (OidIsValid(coldgroup))
            {
                list = lappend_int(list, GetNodeIndexByHashValue(coldgroup, hashvalue));    
            }
            return list;
        }        
    }    

    /* no second distribute key, trade as hot data*/
    if (InvalidAttrNumber == secAttr)
    {
        hashvalue     = compute_hash(type, dvalue, LOCATOR_TYPE_SHARD);    
        return list_make1_int(GetNodeIndexByHashValue(keyValueGroup, hashvalue));
    }
    
    /* second key is NULL, we need use special route stragegy */
    if (isSecNull)
    {        
        if (RELATION_ACCESS_INSERT == accessType)
        {
            
            /* hot data*/        
            GetShardNodes(keyValueGroup, &datanodes, &dn_num, NULL);

            /* route to the first node in group */
            list = list_make1_int(datanodes[0]);
            pfree(datanodes);
        }
        else
        {
        
            /* route to all nodes in group */
            GetShardNodes(keyValueGroup, &datanodes, &dn_num, NULL);
            for(i = 0; i < dn_num; i++)
            {                    
                list = lappend_int(list, datanodes[i]);
            }
            pfree(datanodes);    

            /* got cold group */
            if (OidIsValid(secColdGroup))
            {
                GetShardNodes(secColdGroup, &datanodes, &dn_num, NULL);
                for(i = 0; i < dn_num; i++)
                {                    
                    list = lappend_int(list, datanodes[i]);
                }
                pfree(datanodes);
            }
        }
        return list;            
    }
    
    /* secondary sharding map */
    hashvalue     = compute_hash(type, dvalue, LOCATOR_TYPE_SHARD);    
    sechashvalue = compute_keyvalue_hash(secType, secValue);
    
    /* no dual write, normal access strategy */
    if (!bdualwrite)
    {
        if (OidIsValid(secColdGroup))
        {
            /* hot data, only timestamp field can be divided into hot and cold data */
            if (PARTITION_KEY_IS_TIMESTAMP(secType) && IsHotData(secValue, accessType, partitionStrategy, interval_step, start_timestamp))
            {
                return list_make1_int(GetNodeIndexByHashValue(keyValueGroup, hashvalue + sechashvalue));
            }
            else
            {
                return list_make1_int(GetNodeIndexByHashValue(secColdGroup, hashvalue + sechashvalue));
            }
        }
        else
        {
            if (!IsHotData(secValue, accessType, partitionStrategy, interval_step, start_timestamp))
            {
                /* for first distribute column */
                get_type_io_data(type, IOFunc_output,
                                 &typlen, &typbyval,
                                 &typalign, &typdelim,
                                 &typioparam, &typiofunc);
                value = OidOutputFunctionCall(typiofunc, dvalue);

                /* for timestamp */
                get_type_io_data(TIMESTAMPOID, IOFunc_output,
                                 &typlen, &typbyval,
                                 &typalign, &typdelim,
                                 &typioparam, &typiofunc);
                
                timestr = OidOutputFunctionCall(typiofunc,    secValue);
                
                elog(ERROR, "key value distribute key:%s timestamp:%s is cold data, but table has no cold group", 
                    value, timestr);
            }
            return list_make1_int(GetNodeIndexByHashValue(keyValueGroup, hashvalue + sechashvalue));
        }
    }
    else
    {
        if (RELATION_ACCESS_READ == accessType ||
            RELATION_ACCESS_READ_FQS == accessType)
        {
            return list_make1_int(GetNodeIndexByHashValue(keyValueGroup, hashvalue + sechashvalue));
        }
        else
        {
            list = list_make1_int(GetNodeIndexByHashValue(keyValueGroup, hashvalue + sechashvalue));            
            if (OidIsValid(secColdGroup))
            {
                list = lappend_int(list, GetNodeIndexByHashValue(secColdGroup, hashvalue + sechashvalue));
            }
            return list;
        }
    }
}

/*
 * Prune hot data when creating scan plan
 */
void PruneHotData(Oid relation, Bitmapset *children)
{// #lizard forgives
    int32  i;
    int32  indexnow = 0;
    int32  indexspecial = 0;
    bool   lock;
    bool   skip_first = false;
    bool   skip_temp_first = false;
    TimestampTz tmnow;    
    TimestampTz tmspecial;
    Relation                 rel          = NULL;
    Form_pg_partition_interval routerinfo   = NULL;    
    RelationLocInfo          *rd_locator_info = NULL;
    
    rel             = relation_open(relation, NoLock);
    routerinfo        = rel->rd_partitions_info;
    rd_locator_info = rel->rd_locator_info;

    /* we only prune tables with two distribution attributes */
    if (InvalidAttrNumber == rd_locator_info->secAttrNum)
    {
        relation_close(rel, NoLock);
        return;
    }

    
    if (routerinfo)
    {
        lock = g_AccessCtl->needlock;
        if (lock)
        {
            LWLockAcquire(ColdAccessLock, LW_SHARED);
        }        
        
        if(routerinfo->partinterval_type != IntervalType_Day 
            && routerinfo->partinterval_type != IntervalType_Month)
        {
            relation_close(rel, NoLock);
            return;
        }

        if (tm2timestamp(&g_ManualHotDataTime, 0, NULL, &tmnow) != 0)
        {
            ereport(ERROR,
                (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                 errmsg("timestamp out of range")));
        }
        
        //tmnow     = GetCurrentTimestamp();    
        /* here we use the MAX_NUM_PARTITIONS to get actual offset of the partition */
        indexnow = GetPartitionIndex(routerinfo->partstartvalue_ts,
                                     routerinfo->partinterval_int,
                                     routerinfo->partinterval_type,
                                     routerinfo->partnparts,
                                     tmnow);
        
        if (routerinfo->partinterval_type == IntervalType_Day &&
            routerinfo->partinterval_int == 1)
        {
            fsec_t current_sec;
            struct pg_tm start_time;

            /* timestamp convert to posix struct */
            if(timestamp2tm(routerinfo->partstartvalue_ts, NULL, &start_time, &current_sec, NULL, NULL) != 0)
            {
                ereport(ERROR,
                            (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                             errmsg("timestamp out of range")));
            }

            skip_first = is_first_day_from_start(routerinfo->partinterval_int, routerinfo->partinterval_type, &start_time, &g_ManualHotDataTime);

            if ('\0' != g_TempHotDate[0])
            {
                skip_temp_first = is_first_day_from_start(routerinfo->partinterval_int, routerinfo->partinterval_type, &start_time, &g_TempHotDataTime);
            }
        }

        if ('\0' != g_TempHotDate[0])
        {
            if (tm2timestamp(&g_TempHotDataTime, 0, NULL, &tmspecial) != 0)
            {
                ereport(ERROR,
                    (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                     errmsg("timestamp out of range")));
            }
            
            indexspecial = GetPartitionIndex(routerinfo->partstartvalue_ts,
                                     routerinfo->partinterval_int,
                                     routerinfo->partinterval_type,
                                     routerinfo->partnparts,
                                     tmspecial);
        }
        
        
        if (indexnow >= 0)
        {
            if (g_AccessCtl->colddata)
            {
                if ('\0' == g_TempHotDate[0])
                {
#if 0
                    /* all data is hot data */
                    hot_gap = GetHotDataGap(interval);
                    if (indexnow < hot_gap)
                    {
                        /* clear all tables */
                        bms_clear(children);
                    }
                    else 
                    {
                        /* remove hot tables */
                        for (i = (indexnow - hot_gap + 1); i <= indexnow; i++)
                        {
                            bms_del_member(children, i);
                        }
                    }    
#endif
                    /* remove hot tables */
                    for (i = indexnow; i < routerinfo->partnparts; i++)
                    {    
                        if (i == indexnow && skip_first)
                        {
                            continue;
                        }
                        bms_del_member(children, i);
                    }
                }
                else
                {
#if 0
                    /* if we set temp_hot_data, keep only the specified gap on cold datanode */
                    for (i = 0; i <= indexnow; i++)
                    {
                        if (i != indexspecial)
                        {
                            bms_del_member(children, i);
                        }
                    }
#endif
                    /* remove hot tables */
                    for (i = indexspecial; i < routerinfo->partnparts; i++)
                    {
                        if (i == indexspecial && skip_temp_first)
                        {
                            continue;
                        }
                        bms_del_member(children, i);
                    }
                }
            }
            else
            {    
                if ('\0' == g_TempHotDate[0])
                {
#if 0
                    /* hot nodes */
                    hot_gap = GetHotDataGap(interval);
                    if (indexnow < hot_gap)
                    {
                        /* all data is hot*/                    
                    }
                    else if (indexnow > hot_gap)
                    {
                        /* remove cold tables */
                        for (i = 0; i <= (indexnow - hot_gap); i++)
                        {
                            bms_del_member(children, i);
                        }
                    }
#endif
                    /* remove cold tables */
                    for (i = 0; i < indexnow; i++)
                    {
                        bms_del_member(children, i);
                    }
                }
                else
                {
#if 0
                    /* if we set temp_hot_data, keep only the specified gap on hot datanode */
                    for (i = 0; i <= indexnow; i++)
                    {
                        if (i != indexspecial)
                        {
                            bms_del_member(children, i);
                        }
                    }
#endif
                    /* remove cold tables */
                    for (i = 0; i < indexspecial; i++)
                    {
                        bms_del_member(children, i);
                    }
                }
            }
        }

        if (lock)
        {
            LWLockRelease(ColdAccessLock);
        }        
    }
    relation_close(rel, NoLock);
}

bool
ScanNeedExecute(Relation rel)
{// #lizard forgives
    bool result = true;
    int32  indexnow = 0;
    int32  indexspecial = 0;
    TimestampTz tmnow;    
    bool   lock;
    bool   skip_first = false;
    bool   skip_temp_first = false;
    TimestampTz tmspecial;
    Form_pg_partition_interval routerinfo   = NULL;    
    RelationLocInfo           *rd_locator_info = NULL;
        
    if (rel && IS_PGXC_DATANODE)
    {
        if (RELATION_IS_CHILD(rel))
        {    
            Oid parent_oid = RELATION_GET_PARENT(rel);
            Relation parent_rel = heap_open(parent_oid, AccessShareLock);
            int child_index = RelationGetChildIndex(parent_rel, RelationGetRelid(rel));
            
            routerinfo        = parent_rel->rd_partitions_info;
            rd_locator_info = parent_rel->rd_locator_info;

            if (rd_locator_info && AttributeNumberIsValid(rd_locator_info->secAttrNum) && child_index >= 0)
            {
                if (routerinfo)
                {
                    lock = g_AccessCtl->needlock;
                    if (lock)
                    {
                        LWLockAcquire(ColdAccessLock, LW_SHARED);
                    }    
                    
                    if(routerinfo->partinterval_type == IntervalType_Day ||
                       routerinfo->partinterval_type == IntervalType_Month)
                    {
                        if (tm2timestamp(&g_ManualHotDataTime, 0, NULL, &tmnow) != 0)
                        {
                            ereport(ERROR,
                                (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                                 errmsg("timestamp out of range")));
                        }
        
                        indexnow = GetPartitionIndex(routerinfo->partstartvalue_ts,
                                                     routerinfo->partinterval_int,
                                                     routerinfo->partinterval_type,
                                                     routerinfo->partnparts,
                                                     tmnow);

                        if ('\0' != g_TempHotDate[0])
                        {
                            if (tm2timestamp(&g_TempHotDataTime, 0, NULL, &tmspecial) != 0)
                            {
                                ereport(ERROR,
                                    (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                                     errmsg("timestamp out of range")));
                            }
                            
                            indexspecial = GetPartitionIndex(routerinfo->partstartvalue_ts,
                                                     routerinfo->partinterval_int,
                                                     routerinfo->partinterval_type,
                                                     routerinfo->partnparts,
                                                     tmspecial);
                        }

                        if (routerinfo->partinterval_type == IntervalType_Day &&
                            routerinfo->partinterval_int == 1)
                        {
                            fsec_t current_sec;
                            struct pg_tm start_time;

                            /* timestamp convert to posix struct */
                            if(timestamp2tm(routerinfo->partstartvalue_ts, NULL, &start_time, &current_sec, NULL, NULL) != 0)
                            {
                                ereport(ERROR,
                                            (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                                             errmsg("timestamp out of range")));
                            }

                            skip_first = is_first_day_from_start(routerinfo->partinterval_int, routerinfo->partinterval_type, &start_time, &g_ManualHotDataTime);

                            if ('\0' != g_TempHotDate[0])
                            {
                                skip_temp_first = is_first_day_from_start(routerinfo->partinterval_int, routerinfo->partinterval_type, &start_time, &g_TempHotDataTime);
                            }
                        }

                        if (indexnow >= 0)
                        {
                            if (g_AccessCtl->colddata)
                            {
                                if ('\0' == g_TempHotDate[0])
                                {
                                    if (child_index >= indexnow && child_index < routerinfo->partnparts)
                                    {
                                        result = false;

                                        if (child_index == indexnow && skip_first)
                                        {
                                            result = true;
                                        }
                                    }
                                }
                                else
                                {
                                    if (child_index >= indexspecial && child_index < routerinfo->partnparts)
                                    {
                                        result = false;

                                        if (child_index == indexspecial && skip_temp_first)
                                        {
                                            result = true;
                                        }
                                    }
                                }
                            }
                            else
                            {    
                                if ('\0' == g_TempHotDate[0])
                                {
                                    if (child_index < indexnow)
                                    {
                                        result = false;
                                    }
                                }
                                else
                                {
                                    if (child_index < indexspecial)
                                    {
                                        result = false;
                                    }
                                }
                            }
                        }
                    }

                    if (lock)
                    {
                        LWLockRelease(ColdAccessLock);
                    }    
                }
            }

            heap_close(parent_rel, AccessShareLock);
        }
    }

    return result;
}

static bool 
RangeNeedDualWrite(Oid relation, AttrNumber attr, Datum minValue, Datum maxValue,
                           bool equalMin, bool equalMax)
{// #lizard forgives
    bool  found;
    bool  needlock;
    int32 minGap;
    int32 MaxGap;
    int32        partitionStrategy        = 0;
    Relation                  rel            = NULL;
    Form_pg_partition_interval routerinfo   = NULL;
    HASH_SEQ_STATUS scan_status;
    DualWriteRecord *item;

    needlock = g_DualWriteCtl->needlock;
    
    /*no one is trying to add dual write logical */
    if (0 == g_DualWriteCtl->entrynum && false == needlock)
    {
        return false;
    }

    /* get partition stragegy first */
    rel          = relation_open(relation, NoLock);
    routerinfo   = rel->rd_partitions_info;
    if (routerinfo)
    {
        if (attr == routerinfo->partpartkey)
        {
            partitionStrategy = routerinfo->partinterval_type;
            if (partitionStrategy == IntervalType_Month &&
                routerinfo->partinterval_int == 12)
            {
                partitionStrategy = IntervalType_Year;
            }
        }
    }
    relation_close(rel, NoLock);

    if (minValue)
    {
        minGap = get_timestamptz_gap(minValue, partitionStrategy);
    }

    if (maxValue)
    {
        MaxGap = get_timestamptz_gap(maxValue, partitionStrategy);
    }

    hash_seq_init(&scan_status, g_DualWriteCtl->dwhash);

    if (needlock)
    {
        LWLockAcquire(DualWriteLock, LW_SHARED);
    }

    found = false;
    while ((item = (DualWriteRecord *) hash_seq_search(&scan_status)) != NULL)
    {
        if (minValue && maxValue)
        {
            if (minGap <= item->tag.timeoffset &&
                MaxGap >= item->tag.timeoffset)
            {
                found = true;
            }
        }
        else if (minValue)
        {
            if (minGap <= item->tag.timeoffset)
            {
                found = true;
            }
        }
        else if (maxValue)
        {
            if (MaxGap >= item->tag.timeoffset)
            {
                found = true;
            }
        }

        if (found)
        {
             hash_seq_term(&scan_status);
            break;
        }
    }    

    if (needlock)
    {
        LWLockRelease(DualWriteLock);
    }

    return found;
}
List* GetShardMapRangeList(Oid group, Oid coldgroup, Oid relation, Oid type, Datum dvalue, AttrNumber secAttr, Oid secType, 
                    Datum minValue, Datum maxValue, bool equalMin, bool equalMax, RelationAccessType accessType)
{// #lizard forgives    
    int16         typlen;
    bool         typbyval;
    char         typalign;
    char         typdelim;
    Oid          typioparam;
    Oid          typiofunc;
    Oid          keyValueGroup;
    Oid          keyValueColdGroup;
    long         hashvalue;
    int32         i;
    char         *value;
    List         *list = NULL;
    
    int           hot_num          = 0;
    int           cold_num          = 0;
    int32         *hot_data_nodes  = NULL;
    int32         *cold_data_nodes = NULL;
    int32         partitionStrategy          = 0;
    Relation                 rel          = NULL;
    int32        interval_step            = 0;
    TimestampTz  start_timestamp          = 0;
    Form_pg_partition_interval routerinfo = NULL;    
    
    if (!PARTITION_KEY_IS_TIMESTAMP(secType))
    {
        return NULL;
    }    

    rel = relation_open(relation, NoLock);

    if (RELATION_IS_CHILD(rel))
    {
        relation = RELATION_GET_PARENT(rel);
    }

    relation_close(rel, NoLock);


    /* get partition stragegy first */
    if (secAttr != InvalidAttrNumber)
    {
        rel          = relation_open(relation, NoLock);
        routerinfo     = rel->rd_partitions_info;
        if (routerinfo)
        {
            if (secAttr == routerinfo->partpartkey)
            {
                partitionStrategy = routerinfo->partinterval_type;

                if (partitionStrategy == IntervalType_Month &&
                    routerinfo->partinterval_int == 12)
                {
                    partitionStrategy = IntervalType_Year;
                }
            }
            else
            {
                relation_close(rel, NoLock);
                return NULL;
            }

            interval_step = routerinfo->partinterval_int;
            start_timestamp = routerinfo->partstartvalue_ts;
        }
        relation_close(rel, NoLock);
    }
    else
    {
        return NULL;
    }

    
    /* check whether the value is key value */
    get_type_io_data(type, IOFunc_output,
                     &typlen, &typbyval,
                     &typalign, &typdelim,
                     &typioparam, &typiofunc);
    value = OidOutputFunctionCall(typiofunc, dvalue);        
    
    keyValueGroup = GetKeyValuesGroup(MyDatabaseId, relation, value, &keyValueColdGroup);
    pfree(value);
    value = NULL;

    if (g_EnableDualWrite)
    {
        if (PARTITION_KEY_IS_TIMESTAMP(secType) && accessType == RELATION_ACCESS_UPDATE)
        {
            if (RangeNeedDualWrite(relation, secAttr, minValue, maxValue, equalMin, equalMax))
            {
                if (InvalidOid == keyValueGroup)
                {
                    hashvalue = compute_hash(type, dvalue, LOCATOR_TYPE_SHARD); 

                    list = list_make1_int(GetNodeIndexByHashValue(group, hashvalue));

                    if (OidIsValid(coldgroup))
                    {
                        list = lappend_int(list, GetNodeIndexByHashValue(coldgroup, hashvalue));
                    }
                }
                else
                {
                    GetShardNodes(keyValueGroup, &hot_data_nodes, &hot_num, NULL);
                    for(i = 0; i < hot_num; i++)
                    {                    
                        list = lappend_int(list, hot_data_nodes[i]);
                    }
                    pfree(hot_data_nodes);
                    hot_data_nodes = NULL;    

                    /* all cold */
                    if (OidIsValid(keyValueColdGroup))
                    {
                        GetShardNodes(keyValueColdGroup, &cold_data_nodes, &cold_num, NULL);
                        for(i = 0; i < cold_num; i++)
                        {                    
                            list = lappend_int(list, cold_data_nodes[i]);
                        }
                        pfree(cold_data_nodes);
                        cold_data_nodes = NULL;
                    }
                }

                return list;
            }
        }
    }
    
    if (InvalidOid == keyValueGroup)
    {    
        /* no cold data stragey */
        if (!OidIsValid(coldgroup))
        {
            return NULL;
        }
        
        /* not the key value, use common map strategy */
        hashvalue = compute_hash(type, dvalue, LOCATOR_TYPE_SHARD); 
        
        /* range value, both cold and hot group */
        if (minValue && maxValue)
        {            
            if (IsHotData(minValue, accessType, partitionStrategy, interval_step, start_timestamp) && IsHotData(maxValue, accessType, partitionStrategy, interval_step, start_timestamp))
            {                /* all hot data */
                list = list_make1_int(GetNodeIndexByHashValue(group, hashvalue));                                    
            }
            else if (!IsHotData(minValue, accessType, partitionStrategy, interval_step, start_timestamp) && !IsHotData(maxValue, accessType, partitionStrategy, interval_step, start_timestamp))
            {
                /* all cold data */
                list = list_make1_int(GetNodeIndexByHashValue(coldgroup, hashvalue));
            }
            else if(!IsHotData(minValue, accessType, partitionStrategy, interval_step, start_timestamp) && IsHotData(maxValue, accessType, partitionStrategy, interval_step, start_timestamp))
            {
                /* range across cold and hot group */
                list = list_make1_int(GetNodeIndexByHashValue(group, hashvalue));
                list = lappend_int(list, GetNodeIndexByHashValue(coldgroup, hashvalue));                
            }
            return list;    
        }
        else if (minValue)
        {
            /* timestamp >=, all query in hot node */
            if (IsHotData(minValue, accessType, partitionStrategy, interval_step, start_timestamp))
            {
                list = list_make1_int(GetNodeIndexByHashValue(group, hashvalue));
            }
            else
            {
                list = list_make1_int(GetNodeIndexByHashValue(group, hashvalue));
                list = lappend_int(list, GetNodeIndexByHashValue(coldgroup, hashvalue));    
            }
            return list;
        }
        else if (maxValue)
        {
            /* <= timestamp, both hot and cold need query */
            if (IsHotData(maxValue, accessType, partitionStrategy, interval_step, start_timestamp))
            {
                list = list_make1_int(GetNodeIndexByHashValue(group, hashvalue));                
                list = lappend_int(list, GetNodeIndexByHashValue(coldgroup, hashvalue));
            }
            else
            {
                /* only cold group */
                list = list_make1_int(GetNodeIndexByHashValue(coldgroup, hashvalue));
            }
            return list;
        }
    }            

    /* no cold group */
    if (!OidIsValid(keyValueColdGroup))
    {
        return NULL;
    }
    
    /* secondary sharding map */
    hashvalue = compute_hash(type, dvalue, LOCATOR_TYPE_SHARD); 
    if (minValue && maxValue)
    {
        if (IsHotData(minValue, accessType, partitionStrategy, interval_step, start_timestamp) && IsHotData(maxValue, accessType, partitionStrategy, interval_step, start_timestamp))
        {    
            /* all hot */    
            GetShardNodes(keyValueGroup, &hot_data_nodes, &hot_num, NULL);
            for(i = 0; i < hot_num; i++)
            {                    
                list = lappend_int(list, hot_data_nodes[i]);
            }
            pfree(hot_data_nodes);
            hot_data_nodes = NULL;                                
        }
        else if (!IsHotData(minValue, accessType, partitionStrategy, interval_step, start_timestamp) && !IsHotData(maxValue, accessType, partitionStrategy, interval_step, start_timestamp))
        {
            /* all cold */
            GetShardNodes(keyValueColdGroup, &cold_data_nodes, &cold_num, NULL);
            for(i = 0; i < cold_num; i++)
            {                    
                list = lappend_int(list, cold_data_nodes[i]);
            }
            pfree(cold_data_nodes);
            cold_data_nodes = NULL;
        }
        else if(!IsHotData(minValue, accessType, partitionStrategy, interval_step, start_timestamp) && IsHotData(maxValue, accessType, partitionStrategy, interval_step, start_timestamp))
        {
            /* range across cold and hot group */
            /* all hot */    
            GetShardNodes(keyValueGroup, &hot_data_nodes, &hot_num, NULL);
            for(i = 0; i < hot_num; i++)
            {                    
                list = lappend_int(list, hot_data_nodes[i]);
            }
            pfree(hot_data_nodes);
            hot_data_nodes = NULL;    

            /* all cold */
            GetShardNodes(keyValueColdGroup, &cold_data_nodes, &cold_num, NULL);
            for(i = 0; i < cold_num; i++)
            {                    
                list = lappend_int(list, cold_data_nodes[i]);
            }
            pfree(cold_data_nodes);
            cold_data_nodes = NULL;
        }
        return list;    
    }
    else if (minValue)
    {        
        if (IsHotData(minValue, accessType, partitionStrategy, interval_step, start_timestamp))
        {
            /* all hot */    
            GetShardNodes(keyValueGroup, &hot_data_nodes, &hot_num, NULL);
            for(i = 0; i < hot_num; i++)
            {                    
                list = lappend_int(list, hot_data_nodes[i]);
            }
            pfree(hot_data_nodes);
            hot_data_nodes = NULL;
        }
        else
        {
            /* all nodes */
            return NULL;
        }
    }    
    else if (maxValue)
    {        
        if (!IsHotData(maxValue, accessType, partitionStrategy, interval_step, start_timestamp))
        {
            /* all cold */
            GetShardNodes(keyValueColdGroup, &cold_data_nodes, &cold_num, NULL);
            for(i = 0; i < cold_num; i++)
            {                    
                list = lappend_int(list, cold_data_nodes[i]);
            }
            pfree(cold_data_nodes);
            cold_data_nodes = NULL;
        }
        else
        {
            /* all nodes */
            return NULL;
        }
    }
    return list;
}

/* serialize shard map info for dispatching to lower DNs */
StringInfo 
SerializeShardmap(void)
{
	GroupShardInfo *info;
	StringInfo      data;
	int             i;
	
	if (!IS_PGXC_DATANODE)
		elog(ERROR, "shouldn't try to serialize group shard info on CN");
	
	info = g_GroupShardingMgr_DN->members;
	data = makeStringInfo();
	
	appendStringInfo(data, "%d", info->shmemNumShards);
	for (i = 0; i < info->shmemNumShards; i++)
	{
		appendStringInfo(data, ",%d",
		                 info->shmemshardmap[i].nodeindex);
	}

	return data;
}

/*
 * Deserialize shard map info into g_ShardMap, these information
 * comes from parent DN and will replace local info for distribution
 * across multi groups.
 */
void
DeserializeShardmap(const char *data)
{
	char    *tmp_head = (char *) data;
	char    *tmp_pos;
	int      num_shards, i;
	
	num_shards = (int) strtod(tmp_head, &tmp_pos);
	tmp_head = tmp_pos + 1;
	
	if (num_shards != SHARD_MAP_SHARD_NUM)
	{
		/*
		 * for now num_shards should always be SHARD_MAP_GROUP_NUM
		 * since SHARD_MAP_SHARD_NUM == EXTENSION_SHARD_MAP_SHARD_NUM
		 * but maybe it will change someday, error out to avoid more
		 * critical error.
		 */
		elog(ERROR, "deserializing invalid num of shard map, %d", num_shards);
	}
	
	for (i = 0; i < num_shards; i++)
	{
		g_ShardMap[i].shardgroupid = i;
		g_ShardMap[i].nodeindex = (int) strtod(tmp_head, &tmp_pos);
		tmp_head = tmp_pos + 1;
	}
	
	/* enable remote shard map info */
	g_ShardMapValid = true;
}

/* g_ShardMap is a static array, simply disable it by another static bool */
void
InvalidRemoteShardmap(void)
{
	g_ShardMapValid = false;
}

/*
 * return group oid of this node in
 * return invalid if it's not in a group or it's a CN.
 */
Oid
GetMyGroupOid(void)
{
	if (IS_PGXC_DATANODE)
		return g_GroupShardingMgr_DN->members->group;
	else
		return InvalidOid;
}
#endif
