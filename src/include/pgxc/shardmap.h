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
#ifndef _SHARDMAP_H_
#define _SHARDMAP_H_

#include "postgres.h"
#include "nodes/bitmapset.h"
#include "nodes/parsenodes.h"
#include "utils/dynahash.h"
#include "utils/relcache.h"
#include "pgxc/locator.h"
#include "storage/relfilenode.h"
#include "nodes/plannodes.h"

//#include "utils/snapshot.h"

/* the following macro have same define */
#if 0
#define SHARD_MAP_GROUP_NUM 262144        /*2^18  MUST be 2^n, cannot be greater than HASH_SIZE*/
#define SHARD_MAP_SHARD_NUM    262144      /*2^18  size:1*/
#endif

#define SHARD_MAP_GROUP_NUM 4096        /*2^12  MUST be 2^n, cannot be greater than HASH_SIZE*/
#define SHARD_MAP_SHARD_NUM    4096          /*2^12  size:1*/

#define EXTENSION_SHARD_MAP_GROUP_NUM   4096        /*2^12  MUST be 2^n, cannot be greater than HASH_SIZE*/
#define EXTENSION_SHARD_MAP_SHARD_NUM    4096      /*2^12  size:1*/


#define MAX_GROUP_NODE_NUMBER     1024            /* max node number per group */
#define MAX_SHARDING_NODE_GROUP 256            /* max sharding node group number */

#define MAJOR_SHARD_NODE_GROUP  0
#define FIRST_EXTENSION_GROUP   1
#define LAST_EXTENSION_GROUP    MAX_SHARDING_NODE_GROUP
#define INVALID_SHARDMAP_ID     -1
#define PGXC_INVALID_NODE_IDX   -1

#define SHMEM_SHRADMAP_STATUS_UNINITED     0     /* never to be writed */
#define SHMEM_SHRADMAP_STATUS_UNDEFINED    1     /* pgxc_shard_map is empty */
#define SHMEM_SHRADMAP_STATUS_LOADING    2     /* pgxc_shard_map is empty */
#define SHMEM_SHRADMAP_STATUS_USING        3     /* could be used */
#define SHMEM_SHRADMAP_STATUS_INVALID    4     /* pgxc_shard_map has be updated, but shmem has not be sync */

#define STRINGLENGTH 1024   /* string buffer length */

extern int32       GetNodeIndexByHashValue(Oid group, long shardIdx);
extern Bitmapset  *g_DatanodeShardgroupBitmap;
extern List       *g_TempKeyValueList;
extern bool         g_IsExtension;

extern bool g_StatShardInfo;
extern int  g_MaxSessionsPerPool;
extern int  g_ShardInfoFlushInterval;

#define SHARD_TABLE_BITMAP_SIZE \
    (BITMAPSET_SIZE(WORDNUM(SHARD_MAP_GROUP_NUM) + 1))

typedef struct ShardMapItemDef
{
    int32    shardgroupid;
    Oid        primarycopy;
    int32   nodeindex;    /* node index, set when load sharding map */
}ShardMapItemDef;

typedef struct
{
    int32  shard;
    int64  count;
    int64  size;
}ShardStat;

typedef enum
{
    ShardOpType_create = 0,
    ShardOpType_drop   = 1,
    ShardOpType_butty
}ShardOpType;

extern bool  show_all_shard_stat;

#ifdef __COLD_HOT__
extern bool    g_EnableKeyValue;
extern bool    g_EnableDualWrite;
extern bool    g_EnableColdHotVisible;
#endif

extern void ShardMapShmemInit_CN(void);
extern void ShardMapShmemInit_DN(void);
extern bool MajorShardAlreadyExist(void);
extern void SyncShardMapList(bool force);
extern void InvalidateShmemShardMap(bool iscommit);
extern void RegisterInvalidShmemShardMap(Oid group, int32 op);
extern Size ShardMapShmemSize(void);
//extern List* ShardMapRouter(Oid group, Oid relation, Oid type, Datum dvalue, RelationAccessType accessType);
extern void GetShardNodes(Oid group, int32 ** nodes, int32 *num_nodes, bool *isextension);
extern void PrepareMoveData(MoveDataStmt* stmt);
extern void PgxcMoveData_Node(MoveDataStmt* stmt);
extern void PgxcMoveData_DN(MoveDataStmt* stmt);
extern void UpdateReplicaRelNodes(Oid newnodeid);
extern void ForceRefreshShardMap(Oid groupoid);
extern int  GetGroupSize(void);
extern Bitmapset* CopyShardGroups_DN(Bitmapset * dest);

/*
 * Get ShardId in datanode
 */
extern int32 EvaluateShardId(Oid type, bool isNull, Datum dvalue, 
                           Oid secType, bool isSecNull, Datum secValue, Oid relid);

extern int TruncateShard(Oid reloid, ShardID sid, int pausetime);

/* shard barrier */
extern void ShardBarrierShmemInit(void);
extern Size ShardBarrierShmemSize(void);
extern void AddShardBarrier(RelFileNode rel, ShardID sid, BackendId pid);
extern void RemoveOneShardBarrier(RelFileNode rel, ShardID sid);
extern void RemoveShardBarrier(void);
extern bool IsShardBarriered(RelFileNode rel, ShardID sid);
extern bool LocalHasShardBarriered(RelFileNode rel, ShardID sid);
extern void ATEOXact_CleanUpShardBarrier(void);

extern void   StatShardRelation(Oid relid, ShardStat *shardstat, int32 shardnumber);
extern void   StatShardAllRelations(ShardStat *shardstat, int32 shardnumber);
extern void   GetGroupNodeIndexMap(Oid group, int32 *map);

extern Datum pg_begin_table_dual_write(PG_FUNCTION_ARGS);

extern Datum pg_stat_dual_write(PG_FUNCTION_ARGS);

extern Datum pg_stop_table_dual_write(PG_FUNCTION_ARGS);

extern Datum pg_set_node_cold_access(PG_FUNCTION_ARGS);

extern Datum pg_clear_node_cold_access(PG_FUNCTION_ARGS);

extern Datum pg_stat_node_access(PG_FUNCTION_ARGS);

extern Size ShardStatisticShmemSize(void);

extern void ShardStatisticShmemInit(void);

extern void UpdateShardStatistic(CmdType cmd, ShardID sid, int64 new_size, int64 old_size);

extern void FlushShardStatistic(void);

extern void RecoverShardStatistic(void);

extern void ResetShardStatistic(void);

extern Datum tbase_shard_statistic(PG_FUNCTION_ARGS);

#ifdef __COLD_HOT__
extern Size DualWriteTableSize(void);
extern void DualWriteCtlInit(void);
extern bool pg_get_node_access(void);
extern bool NeedDualWrite(Oid relation, AttrNumber attr, Datum value);

extern bool InTempKeyValueList(Oid relation, char *value, Oid *hotGroup, Oid *coldGroup);

extern void SetTempKeyValueList(const char *str, void *extra);

extern long get_keyvalue_offset(TimestampTz endtime);

extern int32 GetHotDataGap(int32 interval);

extern bool IsHotData(Datum secValue, RelationAccessType access, int32 interval,
                  int step, Datum startValue);

extern List* ShardMapRouter(Oid group, Oid coldgroup, Oid relation, Oid type, Datum dvalue, AttrNumber secAttr, Oid secType, 
                    bool isSecNull, Datum secValue, RelationAccessType accessType);

extern void PruneHotData(Oid relation, Bitmapset *children);

extern bool ScanNeedExecute(Relation rel);

extern List* GetShardMapRangeList(Oid group, Oid coldgroup, Oid relation, Oid type, Datum dvalue, AttrNumber secAttr, Oid secType, 
                    Datum minValue, Datum maxValue, bool  equalMin, bool  equalMax, RelationAccessType accessType);

extern StringInfo SerializeShardmap(void);
extern void DeserializeShardmap(const char *data);
extern void InvalidRemoteShardmap(void);
extern Oid GetMyGroupOid(void);
#endif

#endif /*_SHARDMAP_H_*/
