/*-------------------------------------------------------------------------
 *
 * pgxc_class.c
 *    routines to support manipulation of the pgxc_class relation
 *
 * Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "catalog/pgxc_class.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#ifdef _MIGRATE_
#include "utils/lsyscache.h"
#include "utils/fmgroids.h"
#endif
#include "pgxc/locator.h"
#include "utils/array.h"

/*
 * PgxcClassCreate
 *        Create a pgxc_class entry
 */
#ifdef _MIGRATE_
void
PgxcClassCreate(Oid pcrelid,
                char pclocatortype,
                int pcattnum,
#ifdef __COLD_HOT__
                int secAttnum,
#endif
                int pchashalgorithm,
                int pchashbuckets,
#ifdef __COLD_HOT__
                int *numnodes,
                Oid **nodes,
#else
                int numnodes,
                Oid *nodes,
#endif
                int groupnum,
                Oid *group)
#else
void
PgxcClassCreate(Oid pcrelid,
                char pclocatortype,
                int pcattnum,
                int pchashalgorithm,
                int pchashbuckets,
                int numnodes,
                Oid *nodes)
#endif
{// #lizard forgives
    Relation    pgxcclassrel;
    HeapTuple    htup;
    bool        nulls[Natts_pgxc_class];
    Datum        values[Natts_pgxc_class];
    int        i;
#ifndef __COLD_HOT__
    oidvector    *nodes_array;
#else
    oidvector    *nodes_array[2] = {NULL};
#endif

    /* Build array of Oids to be inserted */
#ifdef __COLD_HOT__
    for (i = 0; i < groupnum; i++)
    {
        nodes_array[i] = buildoidvector(nodes[i], numnodes[i]);
    }
#else
    nodes_array = buildoidvector(nodes, numnodes);
#endif
    /* Iterate through attributes initializing nulls and values */
    for (i = 0; i < Natts_pgxc_class; i++)
    {
        nulls[i]  = false;
        values[i] = (Datum) 0;
    }

    /* should not happen */
    if (pcrelid == InvalidOid)
    {
        elog(ERROR,"pgxc class relid invalid.");
        return;
    }
    
#ifdef _MIGRATE_
    values[Anum_pgxc_class_distribute_group - 1] = ObjectIdGetDatum(group[0]);
    values[Anum_pgxc_class_cold_distribute_group - 1] = ObjectIdGetDatum(group[1]);
#endif
    values[Anum_pgxc_class_pcrelid - 1]   = ObjectIdGetDatum(pcrelid);
    values[Anum_pgxc_class_pclocatortype - 1] = CharGetDatum(pclocatortype);

    if (pclocatortype == LOCATOR_TYPE_HASH || pclocatortype == LOCATOR_TYPE_MODULO)
    {
        values[Anum_pgxc_class_pcattnum - 1] = UInt16GetDatum(pcattnum);
        values[Anum_pgxc_class_pchashalgorithm - 1] = UInt16GetDatum(pchashalgorithm);
        values[Anum_pgxc_class_pchashbuckets - 1] = UInt16GetDatum(pchashbuckets);
        values[Anum_pgxc_class_second_distribute - 1] = UInt16GetDatum(InvalidAttrNumber);
    }
#ifdef _MIGRATE_
    else if (LOCATOR_TYPE_SHARD == pclocatortype)
    {
        values[Anum_pgxc_class_pcattnum - 1] = UInt16GetDatum(pcattnum);
        values[Anum_pgxc_class_pchashalgorithm - 1] = UInt16GetDatum(pchashalgorithm);
        values[Anum_pgxc_class_pchashbuckets - 1] = UInt16GetDatum(pchashbuckets);
        values[Anum_pgxc_class_second_distribute - 1] = UInt16GetDatum(secAttnum);
    }    
#endif

    /* Node information */
#ifdef __COLD_HOT__
    values[Anum_pgxc_class_nodes - 1]      = PointerGetDatum(nodes_array[0]);
    if (!nodes_array[1])
    {
        nulls[Anum_pgxc_class_cold_nodes - 1]  = true;
    }
    else
    {
        values[Anum_pgxc_class_cold_nodes - 1] = PointerGetDatum(nodes_array[1]);
    }
#else
    values[Anum_pgxc_class_nodes - 1] = PointerGetDatum(nodes_array);
#endif
    /* Open the relation for insertion */
    pgxcclassrel = heap_open(PgxcClassRelationId, RowExclusiveLock);

    htup = heap_form_tuple(pgxcclassrel->rd_att, values, nulls);

    CatalogTupleInsert(pgxcclassrel, htup);

    heap_close(pgxcclassrel, RowExclusiveLock);
}


/*
 * PgxcClassAlter
 *        Modify a pgxc_class entry with given data
 */
void
PgxcClassAlter(Oid pcrelid,
               char pclocatortype,
               int pcattnum,
#ifdef __COLD_HOT__
               int secattnum,
#endif
               int pchashalgorithm,
               int pchashbuckets,
#ifdef __COLD_HOT__
               int *numnodes,
               Oid **nodes,
#else
               int numnodes,
               Oid *nodes,
#endif
               PgxcClassAlterType type)
{
    Relation    rel;
    HeapTuple    oldtup, newtup;
    oidvector  *nodes_array;
    Datum        new_record[Natts_pgxc_class];
    bool        new_record_nulls[Natts_pgxc_class];
    bool        new_record_repl[Natts_pgxc_class];

    Assert(OidIsValid(pcrelid));

    rel = heap_open(PgxcClassRelationId, RowExclusiveLock);
    oldtup = SearchSysCacheCopy1(PGXCCLASSRELID,
                                 ObjectIdGetDatum(pcrelid));

    if (!HeapTupleIsValid(oldtup)) /* should not happen */
        elog(ERROR, "cache lookup failed for pgxc_class %u", pcrelid);

    /* Initialize fields */
    MemSet(new_record, 0, sizeof(new_record));
    MemSet(new_record_nulls, false, sizeof(new_record_nulls));
    MemSet(new_record_repl, false, sizeof(new_record_repl));

    /* Fields are updated depending on operation type */
    switch (type)
    {
        case PGXC_CLASS_ALTER_DISTRIBUTION:
            new_record_repl[Anum_pgxc_class_pclocatortype - 1] = true;
            new_record_repl[Anum_pgxc_class_pcattnum - 1] = true;
            new_record_repl[Anum_pgxc_class_pchashalgorithm - 1] = true;
            new_record_repl[Anum_pgxc_class_pchashbuckets - 1] = true;
            break;
        case PGXC_CLASS_ALTER_NODES:
            new_record_repl[Anum_pgxc_class_nodes - 1] = true;

            /* Build array of Oids to be inserted */
#ifdef __COLD_HOT__
            nodes_array = buildoidvector(nodes[0], numnodes[0]);
#else
            nodes_array = buildoidvector(nodes, numnodes);
#endif
            break;
        case PGXC_CLASS_ALTER_ALL:
        default:
            new_record_repl[Anum_pgxc_class_pcrelid - 1] = true;
            new_record_repl[Anum_pgxc_class_pclocatortype - 1] = true;
            new_record_repl[Anum_pgxc_class_pcattnum - 1] = true;
            new_record_repl[Anum_pgxc_class_pchashalgorithm - 1] = true;
            new_record_repl[Anum_pgxc_class_pchashbuckets - 1] = true;
            new_record_repl[Anum_pgxc_class_nodes - 1] = true;

            /* Build array of Oids to be inserted */
#ifdef __COLD_HOT__
            nodes_array = buildoidvector(nodes[0], numnodes[0]);
#else
            nodes_array = buildoidvector(nodes, numnodes);
#endif
    }

    /* Set up new fields */
    /* Relation Oid */
    if (new_record_repl[Anum_pgxc_class_pcrelid - 1])
        new_record[Anum_pgxc_class_pcrelid - 1] = ObjectIdGetDatum(pcrelid);

    /* Locator type */
    if (new_record_repl[Anum_pgxc_class_pclocatortype - 1])
        new_record[Anum_pgxc_class_pclocatortype - 1] = CharGetDatum(pclocatortype);

    /* Attribute number of distribution column */
    if (new_record_repl[Anum_pgxc_class_pcattnum - 1])
        new_record[Anum_pgxc_class_pcattnum - 1] = UInt16GetDatum(pcattnum);

    /* Hash algorithm type */
    if (new_record_repl[Anum_pgxc_class_pchashalgorithm - 1])
        new_record[Anum_pgxc_class_pchashalgorithm - 1] = UInt16GetDatum(pchashalgorithm);

    /* Hash buckets */
    if (new_record_repl[Anum_pgxc_class_pchashbuckets - 1])
        new_record[Anum_pgxc_class_pchashbuckets - 1] = UInt16GetDatum(pchashbuckets);

    /* Node information */
    if (new_record_repl[Anum_pgxc_class_nodes - 1])
        new_record[Anum_pgxc_class_nodes - 1] = PointerGetDatum(nodes_array);

#ifdef _MIGRATE_
    new_record_nulls[Anum_pgxc_class_cold_distribute_group - 1] = true;
    new_record_nulls[Anum_pgxc_class_second_distribute - 1] = true;
    new_record_nulls[Anum_pgxc_class_cold_nodes - 1] = true;
#endif
    /* Update relation */
    newtup = heap_modify_tuple(oldtup, RelationGetDescr(rel),
                               new_record,
                               new_record_nulls, new_record_repl);
    CatalogTupleUpdate(rel, &oldtup->t_self, newtup);

    heap_close(rel, RowExclusiveLock);
}

/*
 * RemovePGXCClass():
 *        Remove extended PGXC information
 */
void
RemovePgxcClass(Oid pcrelid)
{
    Relation  relation;
    HeapTuple tup;

    /*
     * Delete the pgxc_class tuple.
     */
    relation = heap_open(PgxcClassRelationId, RowExclusiveLock);
    tup = SearchSysCache(PGXCCLASSRELID,
                         ObjectIdGetDatum(pcrelid),
                         0, 0, 0);

    if (!HeapTupleIsValid(tup)) /* should not happen */
        elog(ERROR, "cache lookup failed for pgxc_class %u", pcrelid);

    simple_heap_delete(relation, &tup->t_self);

    ReleaseSysCache(tup);

    heap_close(relation, RowExclusiveLock);
}


#ifdef _MIGRATE_
/*
  * logic on datanode.
  * Distribute key must registered in datanode, it will be used to filter when data node would be splited.
  */
void
RegisterDistributeKey(Oid pcrelid,
                char pclocatortype,
                int pcattnum,
                int secattnum,
                int pchashalgorithm,
                int pchashbuckets)
{
    Relation    pgxcclassrel;
    HeapTuple    htup;
    bool        nulls[Natts_pgxc_class];
    Datum        values[Natts_pgxc_class];
    int        i;
    //oidvector  *nodes_array;
    

    /* Iterate through attributes initializing nulls and values */
    for (i = 0; i < Natts_pgxc_class; i++)
    {
        nulls[i]  = false;
        values[i] = (Datum) 0;
    }

    /* should not happen */
    if (pcrelid == InvalidOid)
    {
        elog(ERROR,"pgxc class relid invalid.");
        return;
    }

    values[Anum_pgxc_class_pcrelid - 1]   = ObjectIdGetDatum(pcrelid);
    values[Anum_pgxc_class_pclocatortype - 1] = CharGetDatum(pclocatortype);

    if (pclocatortype == LOCATOR_TYPE_HASH || pclocatortype == LOCATOR_TYPE_MODULO)
    {
        values[Anum_pgxc_class_pcattnum - 1] = UInt16GetDatum(pcattnum);
        values[Anum_pgxc_class_pchashalgorithm - 1] = UInt16GetDatum(pchashalgorithm);
        values[Anum_pgxc_class_second_distribute - 1] = UInt16GetDatum(secattnum);
        values[Anum_pgxc_class_pchashbuckets - 1] = UInt16GetDatum(pchashbuckets);
    }
    else if (LOCATOR_TYPE_SHARD == pclocatortype)
    {
        values[Anum_pgxc_class_pcattnum - 1] = UInt16GetDatum(pcattnum);
        values[Anum_pgxc_class_pchashalgorithm - 1] = UInt16GetDatum(pchashalgorithm);
        values[Anum_pgxc_class_second_distribute - 1] = UInt16GetDatum(secattnum);
        values[Anum_pgxc_class_pchashbuckets - 1] = UInt16GetDatum(pchashbuckets);
        
    }

    /* Node information */
    nulls[Anum_pgxc_class_nodes - 1] = true; 
    //nulls[Anum_pgxc_class_cold_nodes - 1] = true; 

    /* Open the relation for insertion */
    pgxcclassrel = heap_open(PgxcClassRelationId, RowExclusiveLock);

    htup = heap_form_tuple(pgxcclassrel->rd_att, values, nulls);

    CatalogTupleInsert(pgxcclassrel, htup);

    heap_close(pgxcclassrel, RowExclusiveLock);
}

Oid GetRelGroup(Oid reloid)
{
    Oid          group_oid  = InvalidOid;
    HeapTuple     tuple; 
    Form_pgxc_class classtuple   = NULL;

    tuple = SearchSysCache(PGXCCLASSRELID,
                             ObjectIdGetDatum(reloid),
                             0, 0, 0);

    if (HeapTupleIsValid(tuple)) 
    {
        classtuple = (Form_pgxc_class)GETSTRUCT(tuple);
        group_oid = classtuple->pgroup;
            
        ReleaseSysCache(tuple);
        return group_oid;
    }
    else
    {
        elog(DEBUG1, "cache lookup failed for pgxc_class %u", reloid);
    }
    
    return InvalidOid;
}

Oid GetRelColdGroup(Oid reloid)
{
    Oid          group_oid  = InvalidOid;
    HeapTuple     tuple; 
    Form_pgxc_class classtuple   = NULL;

    tuple = SearchSysCache(PGXCCLASSRELID,
                             ObjectIdGetDatum(reloid),
                             0, 0, 0);

    if (HeapTupleIsValid(tuple)) 
    {
        classtuple = (Form_pgxc_class)GETSTRUCT(tuple);
        group_oid = classtuple->pcoldgroup;
            
        ReleaseSysCache(tuple);
        return group_oid;
    }
    else
    {
        elog(DEBUG1, "cache lookup failed for pgxc_class %u", reloid);
    }
    
    return InvalidOid;
}

void GetNotShardRelations(bool is_contain_replic, List **rellist, List **nslist)
{// #lizard forgives
    HeapScanDesc scan;
    HeapTuple     tup;
    Form_pgxc_class pgxc_class;
    Relation rel;

    rel = heap_open(PgxcClassRelationId, AccessShareLock);    
    scan = heap_beginscan_catalog(rel,0,NULL);
    tup = heap_getnext(scan,ForwardScanDirection);

    while(HeapTupleIsValid(tup))
    {
        char *relname;
        char *nsname;
        Oid      nsoid;
        pgxc_class = (Form_pgxc_class)GETSTRUCT(tup);

        if(pgxc_class->pcattnum == 0)
        {
            tup = heap_getnext(scan,ForwardScanDirection);
            continue;
        }

        if(is_contain_replic)
        {
            if(pgxc_class->pclocatortype != LOCATOR_TYPE_SHARD)
            {
                relname = get_rel_name(pgxc_class->pcrelid);
                if(rellist)
                {
                    *rellist = lappend(*rellist, relname);
                }
                nsoid = get_rel_namespace(pgxc_class->pcrelid);
                nsname = get_namespace_name(nsoid);
                if(nslist)
                {
                    *nslist = lappend(*nslist, nsname);
                }
            }
        }
        else
        {
            if(pgxc_class->pclocatortype != LOCATOR_TYPE_SHARD
                && pgxc_class->pclocatortype != LOCATOR_TYPE_REPLICATED)
            {
                relname = get_rel_name(pgxc_class->pcrelid);
                if(rellist)
                {
                    *rellist = lappend(*rellist, relname);
                }
                nsoid = get_rel_namespace(pgxc_class->pcrelid);
                nsname = get_namespace_name(nsoid);
                if(nslist)
                {
                    *nslist = lappend(*nslist, nsname);
                }
            }
        }
        
        tup = heap_getnext(scan,ForwardScanDirection);
    }

    heap_endscan(scan);
    heap_close(rel,AccessShareLock);
}

List * GetShardRelations(bool is_contain_replic)
{
    HeapScanDesc scan;
    HeapTuple     tup;
    Form_pgxc_class pgxc_class;
    List *result;
    Relation rel;

    rel = heap_open(PgxcClassRelationId, AccessShareLock);    
    scan = heap_beginscan_catalog(rel,0,NULL);
    tup = heap_getnext(scan,ForwardScanDirection);
    result = NULL;

    while(HeapTupleIsValid(tup))
    {
        pgxc_class = (Form_pgxc_class)GETSTRUCT(tup);

        if(pgxc_class->pclocatortype == LOCATOR_TYPE_SHARD
            || (is_contain_replic && pgxc_class->pclocatortype == LOCATOR_TYPE_REPLICATED))
        {
            result = lappend_oid(result,pgxc_class->pcrelid);
        }

        tup = heap_getnext(scan,ForwardScanDirection);
    }

    heap_endscan(scan);
    heap_close(rel,AccessShareLock);

    return result;    
}


bool GroupHasRelations(Oid group)
{
    bool         exist;
    HeapScanDesc scan;
    HeapTuple     tup;
    Form_pgxc_class pgxc_class;
    Relation rel;

    rel = heap_open(PgxcClassRelationId, AccessShareLock);    
    scan = heap_beginscan_catalog(rel,0,NULL);
    tup = heap_getnext(scan,ForwardScanDirection);
    
    exist = false;
    while(HeapTupleIsValid(tup))
    {
        pgxc_class = (Form_pgxc_class)GETSTRUCT(tup);

        if(pgxc_class->pclocatortype == LOCATOR_TYPE_SHARD && 
            group == pgxc_class->pgroup)
        {
            exist = true;
            break;
        }
        tup = heap_getnext(scan,ForwardScanDirection);
    }

    heap_endscan(scan);
    heap_close(rel,AccessShareLock);

    return exist;    
}
typedef enum PgxcClassRelationType
{
    PGXC_CLASS_REPLICATION_ALL,
    PGXC_CLASS_REPLICATION_GROUP,
    PGXC_CLASS_SHARD_HOT_GROUP,
    PGXC_CLASS_SHARD_COLD_GROUP,
    PGXC_CLASS_NONE
} PgxcClassRelationType;

/* modify pgxc_class with different types */
void
ModifyPgxcClass(PgxcClassModifyType type, PgxcClassModifyData *data)
{// #lizard forgives
    PgxcClassRelationType relation_type = PGXC_CLASS_REPLICATION_ALL;
    
    switch(type)
    {
        case PGXC_CLASS_ADD_NODE:
        case PGXC_CLASS_DROP_NODE:
            {
                HeapScanDesc scan;
                HeapTuple     tup;
                Form_pgxc_class pgxc_class;
                Relation rel;
                oidvector *nodelist;
                int nkeys = 2;
                
                if (!OidIsValid(data->node))
                    return;

                /*
                  * we need modify shard tables in given group, add node to nodelist,
                  * or remove node from nodelist.
                  */
                /* init scan key */
                while(relation_type < PGXC_CLASS_NONE)
                {
                    ScanKeyData key[2];

                    switch(relation_type)
                    {                    
                        case PGXC_CLASS_REPLICATION_ALL:
                        {
                            ScanKeyInit(&key[0],
                                        Anum_pgxc_class_pclocatortype,
                                        BTEqualStrategyNumber, F_CHAREQ,
                                        CharGetDatum('R'));
                            ScanKeyInit(&key[1],
                                        Anum_pgxc_class_distribute_group,
                                        BTEqualStrategyNumber, F_OIDEQ,
                                        ObjectIdGetDatum(0));
                            break;
                        }
                        case PGXC_CLASS_REPLICATION_GROUP:
                        {
                            ScanKeyInit(&key[0],
                                        Anum_pgxc_class_pclocatortype,
                                        BTEqualStrategyNumber, F_CHAREQ,
                                        CharGetDatum('R'));
                            ScanKeyInit(&key[1],
                                        Anum_pgxc_class_distribute_group,
                                        BTEqualStrategyNumber, F_OIDEQ,
                                        ObjectIdGetDatum(data->group));
                            break;
                        }
                        case PGXC_CLASS_SHARD_HOT_GROUP:
                        {
                            ScanKeyInit(&key[0],
                                        Anum_pgxc_class_pclocatortype,
                                        BTEqualStrategyNumber, F_CHAREQ,
                                        CharGetDatum('S'));
                            ScanKeyInit(&key[1],
                                        Anum_pgxc_class_distribute_group,
                                        BTEqualStrategyNumber, F_OIDEQ,
                                        ObjectIdGetDatum(data->group));
                            break;
                        }
                        case PGXC_CLASS_SHARD_COLD_GROUP:
                        {
                            ScanKeyInit(&key[0],
                                        Anum_pgxc_class_pclocatortype,
                                        BTEqualStrategyNumber, F_CHAREQ,
                                        CharGetDatum('S'));
                            ScanKeyInit(&key[1],
                                        Anum_pgxc_class_cold_distribute_group,
                                        BTEqualStrategyNumber, F_OIDEQ,
                                        ObjectIdGetDatum(data->group));
                            break;
                        }
                        default:
                            break;
                    }

                    rel = heap_open(PgxcClassRelationId, AccessExclusiveLock);    

                    scan = heap_beginscan_catalog(rel, nkeys, key);

                    tup = heap_getnext(scan, ForwardScanDirection);

                    while(HeapTupleIsValid(tup))
                    {
                        int num = 0;
                        oidvector *oldoids = NULL;
                        
                        pgxc_class = (Form_pgxc_class)GETSTRUCT(tup);

                        if (relation_type == PGXC_CLASS_SHARD_COLD_GROUP)
                        {
                            oidvector *hotoids = &pgxc_class->nodeoids;
                            int dim1 = hotoids->dim1;
                            
                            oldoids = (oidvector *)((char *)hotoids + sizeof(oidvector) + sizeof(Oid) * dim1);
                            num = Anum_pgxc_class_cold_nodes;
                        }
                        else
                        {
                            oldoids = &pgxc_class->nodeoids;
                            num = Anum_pgxc_class_nodes;
                        }

                        if (oidvector_member(oldoids, data->node))
                        {
                            if (type == PGXC_CLASS_DROP_NODE)
                            {
                                HeapTuple    newtup;
                                Datum *values;
                                bool  *isnull;
                                bool  *replace;
                                
                                nodelist = oidvector_remove(oldoids, data->node);

                                values = (Datum*)palloc0(Natts_pgxc_class * sizeof(Datum));
                                isnull = (bool *)palloc0(Natts_pgxc_class * sizeof(bool));
                                replace = (bool *)palloc0(Natts_pgxc_class * sizeof(bool));

                                replace[num - 1] = true;
                                values[num - 1] = PointerGetDatum(nodelist);

                                newtup = heap_modify_tuple(tup, RelationGetDescr(rel), values, isnull, replace);        
                                CatalogTupleUpdate(rel, &newtup->t_self, newtup);

                                pfree(values);
                                pfree(isnull);
                                pfree(replace);
                                pfree(nodelist);
                                pfree(newtup);
                            }
                            else if (type == PGXC_CLASS_ADD_NODE)
                            {
                                if (relation_type == PGXC_CLASS_REPLICATION_ALL)
                                {
                                    elog(LOG, "node %d already in nodelist of table %d with group %d in pgxc_class.",
                                                 data->node, pgxc_class->pcrelid, pgxc_class->pgroup);
                                }
                                else
                                {
                                    heap_endscan(scan);
                                    heap_close(rel,AccessExclusiveLock);
                                    elog(ERROR, "node %d already in nodelist of table %d with group %d in pgxc_class.",
                                                 data->node, pgxc_class->pcrelid, pgxc_class->pgroup);
                                }
                            }
                            else
                            {
                                heap_endscan(scan);
                                heap_close(rel,AccessExclusiveLock);
                                elog(ERROR, "unknow PgxcClassModifyType %d.", type);
                            }
                        }
                        else
                        {
                            if (type == PGXC_CLASS_DROP_NODE)
                            {
                                elog(LOG, "node %d not in nodelist of table %d with group %d in pgxc_class.",
                                            data->node, pgxc_class->pcrelid, pgxc_class->pgroup);
                            }
                            else if (type == PGXC_CLASS_ADD_NODE)
                            {
                                HeapTuple    newtup;
                                Datum *values;
                                bool  *isnull;
                                bool  *replace;
                                
                                nodelist = oidvector_append(oldoids, data->node);

                                values = (Datum*)palloc0(Natts_pgxc_class * sizeof(Datum));
                                isnull = (bool *)palloc0(Natts_pgxc_class * sizeof(bool));
                                replace = (bool *)palloc0(Natts_pgxc_class * sizeof(bool));

                                replace[num - 1] = true;
                                values[num - 1] = PointerGetDatum(nodelist);

                                newtup = heap_modify_tuple(tup, RelationGetDescr(rel), values, isnull, replace);        
                                CatalogTupleUpdate(rel, &newtup->t_self, newtup);

                                pfree(values);
                                pfree(isnull);
                                pfree(replace);
                                pfree(nodelist);
                                pfree(newtup);
                            }
                        }
                        
                        tup = heap_getnext(scan, ForwardScanDirection);
                    }

                    heap_endscan(scan);
                    heap_close(rel,AccessExclusiveLock);
                    if (OidIsValid(data->group))
                    {
                        relation_type++;
                    }
                    else
                    {
                        relation_type = PGXC_CLASS_NONE;
                    }
                }
            }
        default:
            break;
    }
}

void CheckPgxcClassGroupValid(Oid group, Oid cold, bool create_key_value)    
{// #lizard forgives
    HeapScanDesc scan;
    HeapTuple    tup;
    Form_pgxc_class pgxc_class;    
    Relation rel;

    rel = heap_open(PgxcClassRelationId, AccessShareLock);    
    scan = heap_beginscan_catalog(rel, 0, NULL);
    tup = heap_getnext(scan,ForwardScanDirection);
    

    while(HeapTupleIsValid(tup))
    {
        pgxc_class = (Form_pgxc_class)GETSTRUCT(tup);

        if (OidIsValid(group))
        {
            if (OidIsValid(pgxc_class->pcoldgroup))
            {
                if (group == pgxc_class->pcoldgroup)
                {
                     elog(ERROR, "hot group %u conflict exist table cold group %u", group, pgxc_class->pcoldgroup);
                }
            }
        }

        
        if (OidIsValid(cold))
        {
            if (OidIsValid(pgxc_class->pgroup))
            {
                if (cold == pgxc_class->pgroup)
                {
                     elog(ERROR, "cold group %u conflict exist table hot group %u", cold, pgxc_class->pgroup);
                }
            }

            if (create_key_value)
            {
                if (OidIsValid(pgxc_class->pcoldgroup))
                {
                    if (cold == pgxc_class->pcoldgroup)
                    {
                         elog(ERROR, "cold group %u conflict exist table cold group %u", cold, pgxc_class->pcoldgroup);
                    }
                }
            }
        }

        tup = heap_getnext(scan,ForwardScanDirection);
    }
    
    heap_endscan(scan);
    heap_close(rel,AccessShareLock);
}

/*
 * Ensure key value hot group does not conflict table's hot group
 */
void CheckPgxcClassGroupConfilct(Oid keyvaluehot)    
{
    HeapScanDesc scan;
    HeapTuple    tup;
    Form_pgxc_class pgxc_class;    
    Relation rel;

    if (!OidIsValid(keyvaluehot))
    {
        return;
    }
    
    rel = heap_open(PgxcClassRelationId, AccessShareLock);    
    scan = heap_beginscan_catalog(rel, 0, NULL);
    tup = heap_getnext(scan,ForwardScanDirection);

    while(HeapTupleIsValid(tup))
    {
        pgxc_class = (Form_pgxc_class)GETSTRUCT(tup);
        
        if (OidIsValid(pgxc_class->pgroup))
        {
            if (keyvaluehot == pgxc_class->pgroup)
            {
                 elog(ERROR, "key value hot group %u conflict exist table:%u hot group %u", keyvaluehot, pgxc_class->pcrelid, pgxc_class->pgroup);
            }
        }
        tup = heap_getnext(scan,ForwardScanDirection);
    }
    
    heap_endscan(scan);
    heap_close(rel,AccessShareLock);
}

#endif
