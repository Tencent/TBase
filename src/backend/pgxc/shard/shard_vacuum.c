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

#include <stdlib.h>
#include <errno.h>
#include <ctype.h>

#include "postgres.h"
#include "fmgr.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/lsyscache.h"
#include "pgxc/shard_vacuum.h"
#include "pgxc/pgxc.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pgxc_class.h"
#include "catalog/pgxc_shard_map.h"
#include "catalog/namespace.h"
#include "nodes/bitmapset.h"
#include "utils/builtins.h"
#include "utils/tqual.h"
#include "utils/snapshot.h"
#include "utils/ruleutils.h"
#include "access/genam.h"
#include "catalog/indexing.h"
#include "utils/fmgroids.h"


static void
split_shard_from_tablename(char *str, char **shards_str, char **tablename_str, int *sleep_interval);

static char *trimwhitespace(char *str);



Datum
vacuum_hidden_shards(PG_FUNCTION_ARGS)
{// #lizard forgives
    //arg: text   "321,87654,89765"
    char         *arg = NULL;
    char        *shards_str = NULL;
    char        *tablename_str = NULL;
    int            sleep_interval = 0;
    List        *table_oid_list = NULL;
    List         *shards = NULL;
    ListCell     *lc;
    int            shard;
    Bitmapset     *shardmap = NULL;
    Bitmapset    *to_vacuum = NULL;
    int64        vacuumed_rows = 0;
    Snapshot    vacuum_snapshot = NULL;
    bool        need_new_snapshot = false;
    SnapshotSatisfiesFunc snapfunc_tmp = NULL;

    Oid            reloid;

    if(!IS_PGXC_DATANODE)
        elog(ERROR, "this function can only be called in datanode");
    
#ifdef _PUB_SUB_RELIABLE_
    /* set this is internal delete operation, should not be subscripted by pubsub or kafka */
    wal_set_internal_stream();
#endif
    /* get shards to be vacuumed */    
    arg = text_to_cstring(PG_GETARG_TEXT_P(0));
    split_shard_from_tablename(arg, &shards_str, &tablename_str, &sleep_interval);
    
    shards = string_to_shard_list(shards_str);
    table_oid_list = string_to_reloid_list(tablename_str);    

    vacuum_snapshot = GetActiveSnapshot();
    if(!vacuum_snapshot)
    {
        need_new_snapshot = true;
        PushActiveSnapshot(GetTransactionSnapshot());
        vacuum_snapshot = GetActiveSnapshot();
    }
    shardmap = vacuum_snapshot->shardgroup;

    if(!need_new_snapshot)
        snapfunc_tmp = vacuum_snapshot->satisfies;
    vacuum_snapshot->satisfies = HeapTupleSatisfiesUnshard;

    check_shardlist_visiblility(shards, SHARD_VISIBLE_CHECK_HIDDEN);

    /* check if the shard is hidden */
    foreach(lc, shards)
    {
        shard = lfirst_int(lc);

        if(bms_is_member(shard, shardmap))
            elog(ERROR, "shard %d is exist in share memory shardmap right now, it can not to be vacuumed", shard);
        
        to_vacuum = bms_add_member(to_vacuum, shard);
    }

    list_free(shards);

    /* get shard relations */
    if(list_length(table_oid_list) == 0)
        table_oid_list = GetShardRelations_NoChild(false);

    /* vacumm rows belong to specifiend shards */
    foreach(lc, table_oid_list)
    {
        Relation shardrel;
        reloid = lfirst_oid(lc);
        shardrel = heap_open(reloid,RowExclusiveLock);
        
        vacuumed_rows += vacuum_shard_internal(shardrel, to_vacuum, vacuum_snapshot, sleep_interval, true);
        
        heap_close(shardrel,RowExclusiveLock);
    }

    bms_free(to_vacuum);

    if(!need_new_snapshot)
        vacuum_snapshot->satisfies = snapfunc_tmp;
    
    if(need_new_snapshot)
        PopActiveSnapshot();
#ifdef _PUB_SUB_RELIABLE_
    /* reset this property of session to default */
    wal_reset_stream();
#endif

    PG_RETURN_INT64(vacuumed_rows);
}


List *
string_to_shard_list(char *str)
{// #lizard forgives
    List     *shards = NIL;
    char     *shard_str = NULL;
    int    shard;

    if(str == NULL)
        return NULL;

    shard_str = strtok(str, ", ");

    while(shard_str)
    {
        errno = 0;    /* To distinguish success/failure after call */
        //shard = strtol(str, NULL, 10);
        shard = pg_atoi(shard_str, sizeof(int32), '\0');

        /* Check for various possible errors */

          if ((errno == ERANGE && (shard == INT_MAX || shard == INT_MIN))
               || (errno != 0 && shard == 0)) {
               elog(ERROR, "shard is invalid:%s", shard_str);
           }

        if(shard >= INT_MAX || shard < 0)
            elog(ERROR, "shard is over the range:%d", shard);

        shards = lappend_int(shards, (int)shard);
        
        shard_str = strtok(NULL, ", ");
    }

    return shards;
}

List *
string_to_reloid_list(char *str)
{// #lizard forgives
    List    *tablename_list = NULL;
    List     *tableoids = NULL;
    char    *table_str = NULL;
    char    *orig_table_str = NULL;
    ListCell    *tablename_cl = NULL;

    table_str = strtok(str, ",");

    while(table_str)
    {
        tablename_list = lappend(tablename_list, table_str);
        table_str = strtok(NULL, ", ");
    }

    if(list_length(tablename_list) == 0)
        return NULL;

    foreach(tablename_cl, tablename_list)
    {
        char     *str1 = NULL;
        char     *str2 = NULL;
        char     *schema = NULL;
        char     *relname = NULL;
        Oid        reloid = InvalidOid;
        
        table_str = lfirst(tablename_cl);
        if(table_str == NULL)
            continue;

        orig_table_str = pstrdup(table_str);

        str1 = strtok(table_str, ". ");
        if(str1)
            str2 = strtok(NULL, ". ");

        if(str1 && str2)
        {
            schema = str1;
            relname = str2;
        }
        else if(str1)
        {
            relname = str1;
        }
        else
        {
            elog(ERROR, "table name %s is invalid.", orig_table_str);
        }

        if(schema)
        {
            Oid namespace_oid;
            namespace_oid = get_namespace_oid(schema, false);
            reloid = get_relname_relid(relname, namespace_oid);
        }
        else
        {
            reloid = RelnameGetRelid(relname);
        }

        if(InvalidOid == reloid)            
            elog(ERROR, "table %s is not exist.", orig_table_str);

        tableoids = list_append_unique_oid(tableoids, reloid);
    }

    list_free(tablename_list);

    return tableoids;
}

void
split_shard_from_tablename(char *str, char **shards_str, char **tablename_str, int *sleep_interval)
{// #lizard forgives    
    char *clean_str = NULL;
    int pos = 0;

    *shards_str = NULL;
    *tablename_str = NULL;
    
    if(strlen(str) == 0)
    {
        return;
    }

    clean_str = trimwhitespace(str);
    
    if(strlen(clean_str) == 0)
    {
        return;
    }

    while(clean_str[pos] != '#' && clean_str[pos] != '\0') 
        pos++;

    if(pos == 0)
    {
        *shards_str = NULL;
    }
    else
    {
        *shards_str = (char *)palloc0(pos + 1);
        memcpy(*shards_str, clean_str, pos);
    }

    if(clean_str[pos] == '\0')
    {
        *tablename_str = NULL;
        *sleep_interval = VACUUM_SHARD_SLEEP_INTERVAL_DEFALUT;
        return;
    }

    pos++;
    clean_str = clean_str + pos;
    pos = 0;
    
    while(clean_str[pos] != '#' && clean_str[pos] != '\0') 
        pos++;

    if(pos == 0)
    {
        *tablename_str = NULL;
    }
    else
    {
        *tablename_str = (char *)palloc0(pos + 1);
        memcpy(*tablename_str, clean_str, pos);
    }
    
    if(clean_str[pos] == '\0')
    {
        *sleep_interval = VACUUM_SHARD_SLEEP_INTERVAL_DEFALUT;
        return;
    }

    pos++;
    clean_str = clean_str + pos;
    pos = 0;

    while(clean_str[pos] != '#' && clean_str[pos] != '\0') 
        pos++;

    if(pos == 0)
    {
        *sleep_interval = VACUUM_SHARD_SLEEP_INTERVAL_DEFALUT;
    }
    else
    {
        char *str_sleep_interval = NULL;
        str_sleep_interval = (char *)palloc0(pos + 1);
        memcpy(str_sleep_interval, clean_str, pos);

        *sleep_interval = pg_atoi(str_sleep_interval, sizeof(int32), '\0');
        pfree(str_sleep_interval);
    }
    
/*
    if(clean_str[0] == '#')
    {
        *tablename_str = strtok(clean_str, "#");
    }
    else
    {
        *shards_str = strtok(clean_str, "#");

        if(*shards_str)
            *tablename_str = strtok(NULL, "#");
    }
*/    
    return;
}

int64 vacuum_shard(Relation rel, Bitmapset *to_vacuum, Snapshot vacuum_snapshot, bool to_delete)
{
    return vacuum_shard_internal(rel, to_vacuum, vacuum_snapshot, VACUUM_SHARD_SLEEP_INTERVAL_DEFALUT, to_delete);
}

int64 vacuum_shard_internal(Relation rel, Bitmapset *to_vacuum, Snapshot vacuum_snapshot, int sleep_interval, bool to_delete)
{// #lizard forgives
    HeapScanDesc scan;
    HeapTuple    tup;
    int64 n = 0;
    int tuples = 0;

    if(!IS_PGXC_DATANODE)
        return 0;

    if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
        return 0;

    if(RELATION_IS_INTERVAL(rel))
    {
        List         *childs = NULL;
        ListCell    *lc;
        Oid     child;
        childs = RelationGetAllPartitions(rel);

        foreach(lc, childs)
        {
            Relation shardrel;
            child = lfirst_oid(lc);
            shardrel = heap_open(child, RowExclusiveLock);
            n += vacuum_shard_internal(shardrel, to_vacuum, vacuum_snapshot, sleep_interval, to_delete);
            heap_close(shardrel,RowExclusiveLock);
        }
    }    

    scan = heap_beginscan(rel, vacuum_snapshot, 0, NULL);
    tup = heap_getnext(scan,ForwardScanDirection);
    
    while(HeapTupleIsValid(tup))
    {
        if(!to_vacuum || bms_is_member(HeapTupleGetShardId(tup),to_vacuum))
        {
            n++;
            if(to_delete)
                simple_heap_delete(rel, &tup->t_self);
            
            if(to_delete)
            {
                tuples++;
				if(tuples > 2000)
                {
                    tuples = 0;
                    pg_usleep(sleep_interval * 1000);
                }
            }
        }
        tup = heap_getnext(scan,ForwardScanDirection);
    }

    heap_endscan(scan);

    return n;
}


char *trimwhitespace(char *str)
{
  char *end;

  // Trim leading space
  while(isspace(*str)) str++;

  if(*str == 0)  // All spaces?
    return str;

  // Trim trailing space
  end = str + strlen(str) - 1;
  while(end > str && isspace(*end)) end--;

  // Write new null terminator
  *(end+1) = 0;

  return str;
}

void check_shardlist_visiblility(List *shard_list, ShardVisibleCheckMode visible_mode)
{
    Relation    shardmapRel;
    HeapTuple   oldtup;
    ScanKeyData skey[2];
    SysScanDesc scan;
    
    ListCell    *shard_lc = NULL;
    int     shard = 0;

    if(!IS_PGXC_DATANODE)
        elog(ERROR, "this function can only be called in datanode");
    
    shardmapRel = heap_open(PgxcShardMapRelationId, AccessShareLock);
    foreach(shard_lc, shard_list)
    {
        shard = lfirst_int(shard_lc);

        /* in datanode, we use invalidoid */
        ScanKeyInit(&skey[0],
                    Anum_pgxc_shard_map_nodegroup,
                    BTEqualStrategyNumber, F_OIDEQ,
                    Int32GetDatum(InvalidOid));

        ScanKeyInit(&skey[1],
                    Anum_pgxc_shard_map_shardgroupid,
                    BTEqualStrategyNumber, F_INT4EQ,
                    Int32GetDatum(shard));
            
        scan = systable_beginscan(shardmapRel,
                                    PgxcShardMapShardIndexId,true,
                                    NULL,
                                    2,
                                    skey);
    
        oldtup = systable_getnext(scan);

        if(visible_mode == SHARD_VISIBLE_CHECK_VISIBLE && !HeapTupleIsValid(oldtup))
        {
            elog(ERROR,"sharding group[%d] is not exist in node[%s]", shard, PGXCNodeName);
        }
        else if(visible_mode == SHARD_VISIBLE_CHECK_HIDDEN && HeapTupleIsValid(oldtup))
        {
            elog(ERROR,"sharding group[%d] is visible in node[%s]", shard, PGXCNodeName);
        }

        systable_endscan(scan);
    }
    
    heap_close(shardmapRel,AccessShareLock);
}

List * GetShardRelations_NoChild(bool is_contain_replic)
{
    HeapScanDesc scan;
    HeapTuple     tup;
    Form_pgxc_class pgxc_class;
    List *result;
    Relation rel;
    Relation onerel;

    rel = heap_open(PgxcClassRelationId, AccessShareLock);    
    scan = heap_beginscan_catalog(rel, 0, NULL);
    tup = heap_getnext(scan, ForwardScanDirection);
    result = NULL;

    while(HeapTupleIsValid(tup))
    {
        pgxc_class = (Form_pgxc_class)GETSTRUCT(tup);

        if(pgxc_class->pclocatortype == LOCATOR_TYPE_SHARD
            || (is_contain_replic && pgxc_class->pclocatortype == LOCATOR_TYPE_REPLICATED))
        {
            onerel = heap_open(pgxc_class->pcrelid, AccessShareLock);
            
            if(!RELATION_IS_CHILD(onerel))
                result = lappend_oid(result,pgxc_class->pcrelid);

            heap_close(onerel,AccessShareLock);
        }

        tup = heap_getnext(scan,ForwardScanDirection);
    }

    heap_endscan(scan);
    heap_close(rel,AccessShareLock);

    return result;    
}

