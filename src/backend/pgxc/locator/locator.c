/*-------------------------------------------------------------------------
 *
 * locator.c
 *        Functions that help manage table location information such as
 * partitioning and replication information.
 *
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *        $$
 *
 *-------------------------------------------------------------------------
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>

#include "postgres.h"
#include "access/skey.h"
#include "access/gtm.h"
#include "access/relscan.h"
#include "catalog/indexing.h"
#include "catalog/pg_type.h"
#include "nodes/pg_list.h"
#include "nodes/nodeFuncs.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/tqual.h"
#include "utils/syscache.h"
#include "utils/varbit.h"
#include "nodes/nodes.h"
#include "optimizer/clauses.h"
#include "optimizer/pgxcship.h"
#include "parser/parse_coerce.h"
#include "pgxc/nodemgr.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"

#include "catalog/pgxc_class.h"
#include "catalog/pgxc_node.h"
#include "catalog/namespace.h"
#include "access/hash.h"

#ifdef XCP

#include "utils/date.h"
#include "utils/memutils.h"

#ifdef __COLD_HOT__

#include "catalog/pgxc_key_values.h"
#include "pgxc/shardmap.h"

#endif

/*
 * Locator details are private
 */
struct _Locator
{
    /*
     * Determine target nodes for value.
     * Resulting nodes are stored to the results array.
     * Function returns number of node references written to the array.
     */
    int            (*locatefunc) (Locator *self, Datum value, bool isnull,
#ifdef __COLD_HOT__
                               Datum secValue, bool secIsNull,
#endif
                                bool *hasprimary);

    Oid            dataType;         /* values of that type are passed to locateNodes function */
    LocatorListType listType;
    bool        primary;
#ifdef _MIGRATE_
    Oid         groupid;        /* only used by LOCATOR_TYPE_SHARD */
    char        locatorType;    /* locator type */
    int          nodeindexMap[TBASE_MAX_DATANODE_NUMBER];   /* map for global node index to local */
#endif
#ifdef __COLD_HOT__
    bool        need_shardmap_router;
    Oid         coldGroupId;    /* cold group oid if exist */
    AttrNumber  secAttrNum;     /* second distribute column's attrNumber */
    Oid         secDataType;    /* second distribute column's datatype */
    Oid         relid;          
    RelationAccessType accessType;
    int         indexMap[TBASE_MAX_DATANODE_NUMBER]; /* map for global node index to local for cold-hot */
#endif
    /* locator-specific data */
    /* XXX: move them into union ? */
    int            roundRobinNode; /* for LOCATOR_TYPE_RROBIN */
    LocatorHashFunc    hashfunc; /* for LOCATOR_TYPE_HASH */
    int         valuelen; /* 1, 2 or 4 for LOCATOR_TYPE_MODULO */

    int            nodeCount; /* How many nodes are in the map */
    void       *nodeMap; /* map index to node reference according to listType */
    void       *results; /* array to output results */
};

#endif

#ifdef __COLD_HOT__
typedef struct
{
    Oid  opno;
    void *expr;
    bool isswap;
}DisQual;
#endif

Oid        primary_data_node = InvalidOid;
int        num_preferred_data_nodes = 0;
Oid        preferred_data_node[MAX_PREFERRED_NODES];

#ifdef XCP

static int modulo_value_len(Oid dataType);

static int locate_static(Locator *self, Datum value, bool isnull,
#ifdef __COLD_HOT__
                Datum secValue, bool secIsNull,
#endif
                bool *hasprimary);

static int locate_roundrobin(Locator *self, Datum value, bool isnull,
#ifdef __COLD_HOT__
                       Datum secValue, bool secIsNull,
#endif
                       bool *hasprimary);

static int locate_modulo_random(Locator *self, Datum value, bool isnull,
#ifdef __COLD_HOT__
                              Datum secValue, bool secIsNull,
#endif
                              bool *hasprimary);

static int locate_hash_insert(Locator *self, Datum value, bool isnull,
#ifdef __COLD_HOT__
                        Datum secValue, bool secIsNull,
#endif
                        bool *hasprimary);

static int locate_hash_select(Locator *self, Datum value, bool isnull,
#ifdef __COLD_HOT__
                        Datum secValue, bool secIsNull,
#endif
                        bool *hasprimary);

#ifdef _MIGRATE_

static int locate_shard_insert(Locator *self, Datum value, bool isnull,
#ifdef __COLD_HOT__
                                    Datum secValue, bool secIsNull,
#endif
                                    bool *hasprimary);

static int locate_shard_select(Locator *self, Datum value, bool isnull,
#ifdef __COLD_HOT__
                            Datum secValue, bool secIsNull,
#endif
                            bool *hasprimary);
                   
#endif

static int locate_modulo_insert(Locator *self, Datum value, bool isnull,
#ifdef __COLD_HOT__
                           Datum secValue, bool secIsNull,
#endif 
                           bool *hasprimary);

static int locate_modulo_select(Locator *self, Datum value, bool isnull,
#ifdef __COLD_HOT__
                            Datum secValue, bool secIsNull,
#endif
                            bool *hasprimary);

static Expr * pgxc_find_distcol_expr(Index varno,
                       AttrNumber attrNum,
                       Node *quals);


#ifdef __COLD_HOT__

static List * pgxc_find_distcol_exprs(Index varno,
                                     AttrNumber attrNum,
                                    Node *quals);

static ExecNodes *GetRelationTimeStampRangeNodes(RelationLocInfo *rel_loc_info, 
                                Datum valueForDistCol, 
                                bool isValueNull, 
                                Oid typeOfValueForDistCol,                
                                Oid typeOfValueForSecDistCol,
                                Datum minForSecCol, 
                                Datum maxForSecCol, 
                                bool  equalMin,
                                bool  equalMax,
                                RelationAccessType accessType);

static bool IsConstAligned(Oid reloid, Datum constvalue, AttrNumber secAttr);

static bool TimeStampRange(Oid op);

#endif
#endif

static bool DatanodeInGroup(oidvector* nodeoids, Oid nodeoid);


/*
 * GetPreferredReplicationNode
 * Pick any Datanode from given list, however fetch a preferred node first.
 */
List *
GetPreferredReplicationNode(List *relNodes)
{
    ListCell    *item;
    int            nodeid = -1;

    if (list_length(relNodes) <= 0)
        elog(ERROR, "a list of nodes should have at least one node");

    foreach(item, relNodes)
    {
        int cnt_nodes;
        char nodetype = PGXC_NODE_DATANODE;
        for (cnt_nodes = 0;
                cnt_nodes < num_preferred_data_nodes && nodeid < 0;
                cnt_nodes++)
        {
            if (PGXCNodeGetNodeId(preferred_data_node[cnt_nodes],
                                  &nodetype) == lfirst_int(item))
			{
                nodeid = lfirst_int(item);
        }
		}
        if (nodeid >= 0)
		{
            break;
    }
	}
    if (nodeid < 0)
	{
        return list_make1_int(list_nth_int(relNodes,
                    ((unsigned int) random()) % list_length(relNodes)));
	}

    return list_make1_int(nodeid);
}

/*
 * GetAnyDataNode
 * Pick any data node from given set, but try a preferred node
 */
int
GetAnyDataNode(Bitmapset *nodes)
{
    Bitmapset  *preferred = NULL;
    int            i, nodeid;
    int            nmembers = 0;
    int            members[NumDataNodes];

    for (i = 0; i < num_preferred_data_nodes; i++)
    {
        char ntype = PGXC_NODE_DATANODE;
        nodeid = PGXCNodeGetNodeId(preferred_data_node[i], &ntype);

        /* OK, found one */
        if (bms_is_member(nodeid, nodes))
		{
            preferred = bms_add_member(preferred, nodeid);
    }
	}

    /*
     * If no preferred data nodes or they are not in the desired set, pick up
     * from the original set.
     */
    if (bms_is_empty(preferred))
	{
        preferred = bms_copy(nodes);
	}

    /*
     * Load balance.
     * We can not get item from the set, convert it to array
     */
    while ((nodeid = bms_first_member(preferred)) >= 0)
        members[nmembers++] = nodeid;
    bms_free(preferred);

    /* If there is a single member nothing to balance */
    if (nmembers == 1)
	{
        return members[0];
	}

    /*
     * In general, the set may contain any number of nodes, and if we save
     * previous returned index for load balancing the distribution won't be
     * flat, because small set will probably reset saved value, and lower
     * indexes will be picked up more often.
     * So we just get a random value from 0..nmembers-1.
     */
    return members[((unsigned int) random()) % nmembers];
}

/*
 * compute_modulo
 *    Computes modulo of two 64-bit unsigned values.
 */
static int
compute_modulo(uint64 numerator, uint64 denominator)
{
    Assert(denominator > 0);

    return numerator % denominator;
}

/*
 * GetRelationDistColumn - Returns the name of the hash or modulo distribution column
 * First hash distribution is checked
 * Retuens NULL if the table is neither hash nor modulo distributed
 */
char *
GetRelationDistColumn(RelationLocInfo * rel_loc_info)
{
char *pColName;

    pColName = NULL;

    pColName = GetRelationHashColumn(rel_loc_info);
    if (pColName == NULL)
	{
        pColName = GetRelationModuloColumn(rel_loc_info);
	}

    return pColName;
}

#ifdef _MIGRATE_

/*
 * IsTypeDistributable
 * Returns whether the data type is distributable using a column value.
 */
bool
IsTypeDistributable(Oid col_type)
{// #lizard forgives
    if(col_type == INT8OID
    || col_type == INT2OID
    || col_type == OIDOID
    || col_type == INT4OID
    || col_type == BOOLOID
    || col_type == CHAROID
    || col_type == NAMEOID
    || col_type == INT2VECTOROID
    || col_type == TEXTOID
    || col_type == OIDVECTOROID
    || col_type == FLOAT4OID
    || col_type == FLOAT8OID
    || col_type == ABSTIMEOID
    || col_type == RELTIMEOID
    || col_type == CASHOID
    || col_type == BPCHAROID
    || col_type == BYTEAOID
    || col_type == VARCHAROID
    || col_type == DATEOID
    || col_type == TIMEOID
    || col_type == TIMESTAMPOID
    || col_type == TIMESTAMPTZOID
    || col_type == INTERVALOID
    || col_type == TIMETZOID
    || col_type == NUMERICOID
#ifdef _PG_ORCL_
    || col_type == VARCHAR2OID
    || col_type == NVARCHAR2OID
#endif
    )
	{
        return true;
	}

    return false;
}

#endif

/*
 * Returns whether or not the data type is hash distributable with PG-XC
 * PGXCTODO - expand support for other data types!
 */
bool
IsTypeHashDistributable(Oid col_type)
{
    return (hash_func_ptr(col_type) != NULL);
}

/*
 * GetRelationHashColumn - return hash column for relation.
 *
 * Returns NULL if the relation is not hash partitioned.
 */
char *
GetRelationHashColumn(RelationLocInfo * rel_loc_info)
{
    char       *column_str = NULL;

    if (rel_loc_info == NULL)
	{
        column_str = NULL;
	}
    else if (rel_loc_info->locatorType != LOCATOR_TYPE_HASH)
	{
        column_str = NULL;
	}
    else
    {
        int            len = strlen(rel_loc_info->partAttrName);

        column_str = (char *) palloc(len + 1);
        strncpy(column_str, rel_loc_info->partAttrName, len + 1);
    }

    return column_str;
}

/*
 * IsDistColumnForRelId - return whether or not column for relation is used for hash or modulo distribution
 *
 */
bool
IsDistColumnForRelId(Oid relid, char *part_col_name)
{
    RelationLocInfo *rel_loc_info;

    /* if no column is specified, we're done */
    if (!part_col_name)
	{
        return false;
	}

    /* if no locator, we're done too */
    if (!(rel_loc_info = GetRelationLocInfo(relid)))
	{
        return false;
	}

    /* is the table distributed by column value */
    if (!IsRelationDistributedByValue(rel_loc_info))
	{
        return false;
	}

    /* does the column name match the distribution column */
    return !strcmp(part_col_name, rel_loc_info->partAttrName);
}


/*
 * Returns whether or not the data type is modulo distributable with PG-XC
 * PGXCTODO - expand support for other data types!
 */
bool
IsTypeModuloDistributable(Oid col_type)
{
    return (modulo_value_len(col_type) != -1);
}

/*
 * GetRelationModuloColumn - return modulo column for relation.
 *
 * Returns NULL if the relation is not modulo partitioned.
 */
char *
GetRelationModuloColumn(RelationLocInfo * rel_loc_info)
{
    char       *column_str = NULL;

    if (rel_loc_info == NULL)
	{
        column_str = NULL;
	}
    else if (rel_loc_info->locatorType != LOCATOR_TYPE_MODULO)
	{
        column_str = NULL;
	}
    else
    {
        int    len = strlen(rel_loc_info->partAttrName);

        column_str = (char *) palloc(len + 1);
        strncpy(column_str, rel_loc_info->partAttrName, len + 1);
    }

    return column_str;
}

/*
 * Update the round robin node for the relation
 *
 * PGXCTODO - may not want to bother with locking here, we could track
 * these in the session memory context instead...
 */
int
GetRoundRobinNode(Oid relid)
{
    int            ret_node;
    Relation    rel = relation_open(relid, AccessShareLock);

    Assert (IsLocatorReplicated(rel->rd_locator_info->locatorType) ||
            rel->rd_locator_info->locatorType == LOCATOR_TYPE_RROBIN);

    ret_node = lfirst_int(rel->rd_locator_info->roundRobinNode);

    /* Move round robin indicator to next node */
    if (rel->rd_locator_info->roundRobinNode->next != NULL)
	{
        rel->rd_locator_info->roundRobinNode = rel->rd_locator_info->roundRobinNode->next;
	}
    else
	{
        /* reset to first one */
        rel->rd_locator_info->roundRobinNode = rel->rd_locator_info->rl_nodeList->head;
	}

    relation_close(rel, AccessShareLock);

    return ret_node;
}

/*
 * IsTableDistOnPrimary
 *
 * Does the table distribution list include the primary node?
 */
bool
IsTableDistOnPrimary(RelationLocInfo *rel_loc_info)
{
    ListCell *item;

    if (!OidIsValid(primary_data_node) ||
        rel_loc_info == NULL ||
        list_length(rel_loc_info->rl_nodeList = 0))
	{
        return false;
	}

    foreach(item, rel_loc_info->rl_nodeList)
    {
        char ntype = PGXC_NODE_DATANODE;
        if (PGXCNodeGetNodeId(primary_data_node, &ntype) == lfirst_int(item))
		{
            return true;
    }
	}
    return false;
}


/*
 * IsLocatorInfoEqual
 * Check equality of given locator information
 */
bool
IsLocatorInfoEqual(RelationLocInfo *rel_loc_info1, RelationLocInfo *rel_loc_info2)
{
    List *nodeList1, *nodeList2;
    Assert(rel_loc_info1 && rel_loc_info2);

    nodeList1 = rel_loc_info1->rl_nodeList;
    nodeList2 = rel_loc_info2->rl_nodeList;

    /* Same relation? */
    if (rel_loc_info1->relid != rel_loc_info2->relid)
	{
        return false;
	}

    /* Same locator type? */
    if (rel_loc_info1->locatorType != rel_loc_info2->locatorType)
	{
        return false;
	}

    /* Same attribute number? */
    if (rel_loc_info1->partAttrNum != rel_loc_info2->partAttrNum)
	{
        return false;
	}

    /* Same node list? */
    if (list_difference_int(nodeList1, nodeList2) != NIL ||
        list_difference_int(nodeList2, nodeList1) != NIL)
	{
        return false;
	}

    /* Everything is equal */
    return true;
}

/*
 * ConvertToLocatorType
 *        get locator distribution type
 * We really should just have pgxc_class use disttype instead...
 */
char
ConvertToLocatorType(int disttype)
{
    char        loctype = LOCATOR_TYPE_NONE;

    switch (disttype)
    {
        case DISTTYPE_HASH:
            loctype = LOCATOR_TYPE_HASH;
            break;
        case DISTTYPE_ROUNDROBIN:
            loctype = LOCATOR_TYPE_RROBIN;
            break;
        case DISTTYPE_REPLICATION:
            loctype = LOCATOR_TYPE_REPLICATED;
            break;
        case DISTTYPE_MODULO:
            loctype = LOCATOR_TYPE_MODULO;
            break;
#ifdef _SHARDING_
        case DISTTYPE_SHARD:
            loctype = LOCATOR_TYPE_SHARD;
            break;
#endif
        default:
            ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg("Invalid distribution type")));
            break;
    }

    return loctype;
}


/*
 * GetLocatorType - Returns the locator type of the table
 *
 */
char
GetLocatorType(Oid relid)
{
    char        ret = '\0';

    RelationLocInfo *ret_loc_info = GetRelationLocInfo(relid);

    if (ret_loc_info != NULL)
	{
        ret = ret_loc_info->locatorType;
	}

    return ret;
}


/*
 * Return a list of all Datanodes.
 * We assume all tables use all nodes in the prototype, so just return a list
 * from first one.
 */
List *
GetAllDataNodes(void)
{
    int            i;
    List       *nodeList = NIL;

    for (i = 0; i < NumDataNodes; i++)
        nodeList = lappend_int(nodeList, i);

    return nodeList;
}

/*
 * Return a list of all Coordinators
 * This is used to send DDL to all nodes and to clean up pooler connections.
 * Do not put in the list the local Coordinator where this function is launched.
 */
List *
GetAllCoordNodes(void)
{
    int            i;
    List       *nodeList = NIL;

    for (i = 0; i < NumCoords; i++)
    {
        /*
         * Do not put in list the Coordinator we are on,
         * it doesn't make sense to connect to the local Coordinator.
         */

        if (i != PGXCNodeId - 1)
		{
            nodeList = lappend_int(nodeList, i);
    }
	}

    return nodeList;
}

/*
 * Return a list of all Coordinators.
 * Including local Coordinator.
 * This is used to clean up pooler connections.
 */
List *
GetEntireCoordNodes(void)
{
    int i;
    List *nodeList = NIL;

    for (i = 0; i < NumCoords; i++)
    {
        nodeList = lappend_int(nodeList, i);
    }

    return nodeList;
}


static bool DatanodeInGroup(oidvector* nodeoids, Oid nodeoid)
{
    int j = 0;
    bool found = false;

    for (j = 0; j <nodeoids->dim1; j++)
    {
        if (nodeoids->values[j] == nodeoid)
        {
            found = true;
            break;
        }
    }

    return found;
}

/*
 * Build locator information associated with the specified relation.
 */
void
RelationBuildLocator(Relation rel)
{// #lizard forgives
    Relation    pcrel;
    ScanKeyData    skey;
    SysScanDesc    pcscan;
    HeapTuple    htup;
    MemoryContext    oldContext;
    RelationLocInfo    *relationLocInfo;
    int        j;
    Form_pgxc_class    pgxc_class;
    bool            node_in_group =  false;
    Oid                curr_nodeoid;

    ScanKeyInit(&skey,
                Anum_pgxc_class_pcrelid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(RelationGetRelid(rel)));

    pcrel = heap_open(PgxcClassRelationId, AccessShareLock);
    pcscan = systable_beginscan(pcrel, PgxcClassPgxcRelIdIndexId, true,
                                SnapshotSelf, 1, &skey);
    htup = systable_getnext(pcscan);

    if (!HeapTupleIsValid(htup))
    {
        /* Assume local relation only */
        rel->rd_locator_info = NULL;
        systable_endscan(pcscan);
        heap_close(pcrel, AccessShareLock);
        return;
    }

    pgxc_class = (Form_pgxc_class) GETSTRUCT(htup);

    oldContext = MemoryContextSwitchTo(CacheMemoryContext);

    relationLocInfo = (RelationLocInfo *) palloc(sizeof(RelationLocInfo));
    rel->rd_locator_info = relationLocInfo;

    relationLocInfo->relid = RelationGetRelid(rel);
    relationLocInfo->locatorType = pgxc_class->pclocatortype;

    relationLocInfo->partAttrNum = pgxc_class->pcattnum;

    relationLocInfo->partAttrName = get_attname(relationLocInfo->relid, pgxc_class->pcattnum);

#ifdef _MIGRATE_
    relationLocInfo->groupId = pgxc_class->pgroup;
#endif

#ifdef __COLD_HOT__
    relationLocInfo->coldGroupId = pgxc_class->pcoldgroup;
    relationLocInfo->secAttrNum = pgxc_class->psecondattnum;
    relationLocInfo->secAttrName = get_attname(relationLocInfo->relid, pgxc_class->psecondattnum);
#endif

    relationLocInfo->rl_nodeList = NIL;


#ifdef _MIGRATE_
    SyncShardMapList(false);

    curr_nodeoid = get_pgxc_nodeoid_extend(PGXCNodeName, PGXCMainClusterName);
    if (InvalidOid == curr_nodeoid)
    {
		elog(ERROR, "no such node:%s on PGXCMainClusterName %s PGXCClustername %s", PGXCNodeName, PGXCMainClusterName,
		     PGXCClusterName);
    }
    
    node_in_group = DatanodeInGroup(&(pgxc_class->nodeoids), curr_nodeoid);

    if (IS_PGXC_COORDINATOR || (IS_PGXC_DATANODE && node_in_group))
    {
        if(relationLocInfo->locatorType == LOCATOR_TYPE_SHARD)
        {
            int32     dn_num;
            int32  *datanodes;
#ifdef __COLD_HOT__
            int32    nGroup;
            Oid     *groups;
            int      i;
            Oid      relationId;
#endif

            /* major map */
            GetShardNodes(pgxc_class->pgroup, &datanodes, &dn_num, NULL);
            for(j = 0; j < dn_num; j++)
            {                            
                relationLocInfo->rl_nodeList = list_append_unique_int(relationLocInfo->rl_nodeList, datanodes[j]);
            }
            pfree(datanodes);
#ifdef __COLD_HOT__
            if (IS_PGXC_COORDINATOR)
            {
                /* cold group if exist */
                if (OidIsValid(pgxc_class->pcoldgroup))
                {
                    GetShardNodes(pgxc_class->pcoldgroup, &datanodes, &dn_num, NULL);
                    for(j = 0; j < dn_num; j++)
                    {                
						relationLocInfo->rl_nodeList = list_append_unique_int(relationLocInfo->rl_nodeList,
						                                                      datanodes[j]);
                    }
                    pfree(datanodes);
                }

                relationId = relationLocInfo->relid;

                if (RELATION_IS_CHILD(rel))
                {
                    relationId = RELATION_GET_PARENT(rel);
                }

                /* secondary group */
                GetRelationSecondGroup(relationId, &groups, &nGroup);
                for (i = 0; i < nGroup; i++)
                {
                    GetShardNodes(groups[i], &datanodes, &dn_num, NULL);
                    for(j = 0; j < dn_num; j++)
                    {                
						relationLocInfo->rl_nodeList = list_append_unique_int(relationLocInfo->rl_nodeList,
						                                                      datanodes[j]);
                    }
                    pfree(datanodes);
                }
                
                if (nGroup)
                {
                    pfree(groups);
                }
            }
#endif
        }
        else
        {
#endif

            for (j = 0; j < pgxc_class->nodeoids.dim1; j++)
            {
                char ntype = PGXC_NODE_DATANODE;
                int nid = PGXCNodeGetNodeId(pgxc_class->nodeoids.values[j], &ntype);
                relationLocInfo->rl_nodeList = lappend_int(relationLocInfo->rl_nodeList, nid);
            }
            
#ifdef _MIGRATE_
        }
#endif
        /*
         * If the locator type is round robin, we set a node to
         * use next time. In addition, if it is replicated,
         * we choose a node to use for balancing reads.
         */
        if (relationLocInfo->locatorType == LOCATOR_TYPE_RROBIN
            || IsLocatorReplicated(relationLocInfo->locatorType))
        {
            int offset;
            /*
             * pick a random one to start with,
             * since each process will do this independently
             */
            offset = compute_modulo(abs(rand()), list_length(relationLocInfo->rl_nodeList));

            relationLocInfo->roundRobinNode = relationLocInfo->rl_nodeList->head; /* initialize */
            for (j = 0; j < offset && relationLocInfo->roundRobinNode->next != NULL; j++)
                relationLocInfo->roundRobinNode = relationLocInfo->roundRobinNode->next;
        }
#ifdef _MIGRATE_
    }
#endif

    systable_endscan(pcscan);
    heap_close(pcrel, AccessShareLock);

    MemoryContextSwitchTo(oldContext);
}

/*
 * GetLocatorRelationInfo - Returns the locator information for relation,
 * in a copy of the RelationLocatorInfo struct in relcache
 */
RelationLocInfo *
GetRelationLocInfo(Oid relid)
{
    RelationLocInfo *ret_loc_info = NULL;
    Relation    rel = relation_open(relid, AccessShareLock);

    /* Relation needs to be valid */
    Assert(rel->rd_isvalid);

    if (rel->rd_locator_info)
	{
        ret_loc_info = CopyRelationLocInfo(rel->rd_locator_info);
	}

    relation_close(rel, AccessShareLock);

    return ret_loc_info;
}

/*
 * Get the distribution type of relation.
 */
char
GetRelationLocType(Oid relid)
{
    RelationLocInfo *locinfo = GetRelationLocInfo(relid);
    if (!locinfo)
	{
        return LOCATOR_TYPE_NONE;
	}

    return locinfo->locatorType;
}

/*
 * Copy the RelationLocInfo struct
 */
RelationLocInfo *
CopyRelationLocInfo(RelationLocInfo * src_info)
{
    RelationLocInfo *dest_info;

    Assert(src_info);

    dest_info = (RelationLocInfo *) palloc0(sizeof(RelationLocInfo));

    dest_info->relid = src_info->relid;
    dest_info->locatorType = src_info->locatorType;
    dest_info->partAttrNum = src_info->partAttrNum;
    if (src_info->partAttrName)
	{
        dest_info->partAttrName = pstrdup(src_info->partAttrName);
	}
#ifdef _MIGRATE_
    dest_info->groupId     = src_info->groupId;
#endif
#ifdef __COLD_HOT__
    dest_info->coldGroupId = src_info->coldGroupId;
    dest_info->secAttrNum = src_info->secAttrNum;
    if (src_info->secAttrName)
    {
        dest_info->secAttrName = pstrdup(src_info->secAttrName);
    }
#endif
    if (src_info->rl_nodeList)
	{
        dest_info->rl_nodeList = list_copy(src_info->rl_nodeList);
	}
    /* Note, for round robin, we use the relcache entry */

    return dest_info;
}


/*
 * Free RelationLocInfo struct
 */
void
FreeRelationLocInfo(RelationLocInfo *relationLocInfo)
{
    if (relationLocInfo)
    {
        if (relationLocInfo->partAttrName)
		{
            pfree(relationLocInfo->partAttrName);
		}

#ifdef __COLD_HOT__
		if (relationLocInfo->secAttrName)
		{
			pfree(relationLocInfo->secAttrName);
		}
#endif

		list_free(relationLocInfo->rl_nodeList);

        pfree(relationLocInfo);
    }
}


/*
 * Free the contents of the ExecNodes expression */
void
FreeExecNodes(ExecNodes **exec_nodes)
{
    ExecNodes *tmp_en = *exec_nodes;

    /* Nothing to do */
    if (!tmp_en)
	{
        return;
	}
    list_free(tmp_en->primarynodelist);
    list_free(tmp_en->nodeList);
    pfree(tmp_en);
    *exec_nodes = NULL;
}


#ifdef XCP

/*
 * Determine value length in bytes for specified type for a module locator.
 * Return -1 if module locator is not supported for the type.
 */
static int
modulo_value_len(Oid dataType)
{// #lizard forgives
    switch (dataType)
    {
        case BOOLOID:
        case CHAROID:
            return 1;
        case INT2OID:
            return 2;
        case INT4OID:
        case ABSTIMEOID:
        case RELTIMEOID:
        case DATEOID:
            return 4;
        case INT8OID:
            return 8;
        default:
            return -1;
    }
}


LocatorHashFunc
hash_func_ptr(Oid dataType)
{// #lizard forgives
    switch (dataType)
    {
        case INT8OID:
        case CASHOID:
            return hashint8;
        case INT2OID:
            return hashint2;
        case OIDOID:
            return hashoid;
        case INT4OID:
        case ABSTIMEOID:
        case RELTIMEOID:
        case DATEOID:
            return hashint4;
#ifdef __TBASE__
        case FLOAT4OID:
            return hashfloat4;
        case FLOAT8OID:
            return hashfloat8;
        case JSONBOID:
            return jsonb_hash;
#endif
        case BOOLOID:
        case CHAROID:
            return hashchar;
        case NAMEOID:
            return hashname;
        case VARCHAROID:
        case TEXTOID:
#ifdef _PG_ORCL_
        case  VARCHAR2OID:
        case  NVARCHAR2OID:
#endif
            return hashtext;
        case OIDVECTOROID:
            return hashoidvector;
        case BPCHAROID:
            return hashbpchar;
        case BYTEAOID:
            return hashvarlena;
        case TIMEOID:
            return time_hash;
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
            return timestamp_hash;
        case INTERVALOID:
            return interval_hash;
        case TIMETZOID:
            return timetz_hash;
        case NUMERICOID:
            return hash_numeric;
        case UUIDOID:
            return uuid_hash;
		case BITOID:
		case VARBITOID:
			return bithash;
        default:
            return NULL;
    }
}

#ifdef _MIGRATE_

Locator *
createLocator(char locatorType, RelationAccessType accessType,
              Oid dataType, LocatorListType listType, int nodeCount,
              void *nodeList, void **result, bool primary, Oid groupid,
              Oid coldGroupId, Oid secDataType, AttrNumber secAttrNum,
              Oid relid)
#else
Locator *
createLocator(char locatorType, RelationAccessType accessType,
              Oid dataType, LocatorListType listType, int nodeCount,
              void *nodeList, void **result, bool primary)
#endif              
{// #lizard forgives
    Locator    *locator;
    ListCell   *lc;
    void        *nodeMap = NULL;
    int         i;

    locator = (Locator *) palloc(sizeof(Locator));
    locator->dataType = dataType;
    locator->listType = listType;
    locator->nodeCount = nodeCount;
#ifdef _MIGRATE_
    locator->groupid  = InvalidOid;
    locator->locatorType = locatorType;
    locator->coldGroupId = InvalidOid;
    locator->accessType = accessType;
    locator->secAttrNum = InvalidAttrNumber;
    locator->secDataType = InvalidOid;
    locator->need_shardmap_router = false;
    locator->relid = InvalidOid;
    memset(locator->indexMap, 0xff, sizeof(int) * TBASE_MAX_DATANODE_NUMBER);
#endif
    
    /* Create node map */
    switch (listType)
    {
        case LOCATOR_LIST_NONE:
            /* No map, return indexes */
            break;
        case LOCATOR_LIST_INT:
            /* Copy integer array */
            nodeMap = palloc(nodeCount * sizeof(int));
            memcpy(nodeMap, nodeList, nodeCount * sizeof(int));
            break;
        case LOCATOR_LIST_OID:
            /* Copy array of Oids */
            nodeMap = palloc(nodeCount * sizeof(Oid));
            memcpy(nodeMap, nodeList, nodeCount * sizeof(Oid));
            break;
        case LOCATOR_LIST_POINTER:
            /* Copy array of Oids */
            nodeMap = palloc(nodeCount * sizeof(void *));
            memcpy(nodeMap, nodeList, nodeCount * sizeof(void *));
            break;
        case LOCATOR_LIST_LIST:
            /* Create map from list */
        {
            List *l = (List *) nodeList;
            locator->nodeCount = list_length(l);
            if (IsA(l, IntList))
            {
                int *intptr;
                nodeMap = palloc(locator->nodeCount * sizeof(int));
                intptr = (int *) nodeMap;
				foreach(lc, l)
				{
					*intptr++ = lfirst_int(lc);
				}
                locator->listType = LOCATOR_LIST_INT;
            }
            else if (IsA(l, OidList))
            {
                Oid *oidptr;
                nodeMap = palloc(locator->nodeCount * sizeof(Oid));
                oidptr = (Oid *) nodeMap;
				foreach(lc, l)
				{
					*oidptr++ = lfirst_oid(lc);
				}
                locator->listType = LOCATOR_LIST_OID;
            }
            else if (IsA(l, List))
            {
                void **voidptr;
                nodeMap = palloc(locator->nodeCount * sizeof(void *));
                voidptr = (void **) nodeMap;
				foreach(lc, l)
				{
					*voidptr++ = lfirst(lc);
				}
                locator->listType = LOCATOR_LIST_POINTER;
            }
            else
            {
                /* can not get here */
                Assert(false);
            }
            break;
        }
    }
    /*
     * Determine locatefunc, allocate results, set up parameters
     * specific to locator type
     */
    switch (locatorType)
    {
#ifdef _MIGRATE_
        case LOCATOR_TYPE_SHARD:
            if (RELATION_ACCESS_INSERT == accessType)
            {
                locator->locatefunc = locate_shard_insert;
                locator->nodeMap = nodeMap;
                locator->groupid = groupid;;
                if (OidIsValid(relid) && IS_PGXC_COORDINATOR)
                {
                    locator->coldGroupId = coldGroupId;
                    locator->secAttrNum = secAttrNum;
                    locator->secDataType = secDataType;
                    locator->relid = relid;

                    if (AttributeNumberIsValid(secAttrNum))
                    {
                        if (OidIsValid(coldGroupId))
                        {
                            locator->need_shardmap_router = true;
                        }
                        else
                        {
                            int32    nGroup;
                            Oid     *groups;
                            
                            GetRelationSecondGroup(relid, &groups, &nGroup);

                            if (nGroup)
                            {
                                locator->need_shardmap_router = true;

                                pfree(groups);
                            }
                        }
                    }
                }
                switch (locator->listType)
                {
                    case LOCATOR_LIST_NONE:
                    case LOCATOR_LIST_INT:
                        locator->results = palloc(sizeof(int));
                        break;
                    case LOCATOR_LIST_OID:
                        locator->results = palloc(sizeof(Oid));
                        break;
                    case LOCATOR_LIST_POINTER:
                        GetGroupNodeIndexMap(locator->groupid, locator->nodeindexMap);
                        locator->results = palloc(sizeof(void *));
                        if (locator->need_shardmap_router)
                        {
                            int i = 0;
                            ListCell *cell;
                            Relation rel = heap_open(locator->relid, NoLock);

                            i = 0;
                            foreach(cell, rel->rd_locator_info->rl_nodeList)
                            {
                                int index = lfirst_int(cell);

                                locator->indexMap[index] = i;
                                i++;
                            }

                            heap_close(rel, NoLock);
                        }
                        break;
                    case LOCATOR_LIST_LIST:
                        /* Should never happen */
                        Assert(false);
                        break;
                }
            }
            else
            {
                locator->locatefunc = locate_shard_select;
                locator->nodeMap = nodeMap;
                locator->groupid = groupid;
                if (OidIsValid(relid) && IS_PGXC_COORDINATOR)
                {
                    locator->coldGroupId = coldGroupId;
                    locator->secAttrNum = secAttrNum;
                    locator->secDataType = secDataType;
                    locator->relid = relid;

                    if (AttributeNumberIsValid(secAttrNum))
                    {
                        if (OidIsValid(coldGroupId))
                        {
                            locator->need_shardmap_router = true;
                        }
                        else
                        {
                            int32    nGroup;
                            Oid     *groups;
                            
                            GetRelationSecondGroup(relid, &groups, &nGroup);

                            if (nGroup)
                            {
                                locator->need_shardmap_router = true;

                                pfree(groups);
                            }
                        }
                    }
                }
                switch (locator->listType)
                {
                    case LOCATOR_LIST_NONE:
                    case LOCATOR_LIST_INT:
                        locator->results = palloc(locator->nodeCount * sizeof(int));
                        break;
                    case LOCATOR_LIST_OID:
                        locator->results = palloc(locator->nodeCount * sizeof(Oid));
                        break;
                    case LOCATOR_LIST_POINTER:
                        GetGroupNodeIndexMap(locator->groupid, locator->nodeindexMap);
                        locator->results = palloc(locator->nodeCount * sizeof(void *));
                        if (locator->need_shardmap_router)
                        {
                            int i = 0;
                            ListCell *cell;
                            Relation rel = heap_open(locator->relid, NoLock);

                            i = 0;
                            foreach(cell, rel->rd_locator_info->rl_nodeList)
                            {
                                int index = lfirst_int(cell);

                                locator->indexMap[index] = i;
                                i++;
                            }

                            heap_close(rel, NoLock);
                        }
                        break;
                    case LOCATOR_LIST_LIST:
                        /* Should never happen */
                        Assert(false);
                        break;
                }
            }
            
            break;
#endif
    
        case LOCATOR_TYPE_REPLICATED:
            if (accessType == RELATION_ACCESS_INSERT ||
                    accessType == RELATION_ACCESS_UPDATE ||
                    accessType == RELATION_ACCESS_READ_FQS ||
                    accessType == RELATION_ACCESS_READ_FOR_UPDATE)
            {
                locator->locatefunc = locate_static;
                if (nodeMap == NULL)
                {
                    /* no map, prepare array with indexes */
                    int *intptr;
                    nodeMap = palloc(locator->nodeCount * sizeof(int));
                    intptr = (int *) nodeMap;
                    for (i = 0; i < locator->nodeCount; i++)
                        *intptr++ = i;
                }
                locator->nodeMap = nodeMap;
                locator->results = nodeMap;
            }
            else
            {
                /* SELECT, use random node.. */
                locator->locatefunc = locate_modulo_random;
                locator->nodeMap = nodeMap;
                switch (locator->listType)
                {
                    case LOCATOR_LIST_NONE:
                    case LOCATOR_LIST_INT:
                        locator->results = palloc(sizeof(int));
                        break;
                    case LOCATOR_LIST_OID:
                        locator->results = palloc(sizeof(Oid));
                        break;
                    case LOCATOR_LIST_POINTER:
                        locator->results = palloc(sizeof(void *));
                        break;
                    case LOCATOR_LIST_LIST:
                        /* Should never happen */
                        Assert(false);
                        break;
                }
                locator->roundRobinNode = -1;
            }
            break;
        case LOCATOR_TYPE_RROBIN:
            if (accessType == RELATION_ACCESS_INSERT)
            {
                locator->locatefunc = locate_roundrobin;
                locator->nodeMap = nodeMap;
                switch (locator->listType)
                {
                    case LOCATOR_LIST_NONE:
                    case LOCATOR_LIST_INT:
                        locator->results = palloc(sizeof(int));
                        break;
                    case LOCATOR_LIST_OID:
                        locator->results = palloc(sizeof(Oid));
                        break;
                    case LOCATOR_LIST_POINTER:
                        locator->results = palloc(sizeof(void *));
                        break;
                    case LOCATOR_LIST_LIST:
                        /* Should never happen */
                        Assert(false);
                        break;
                }
                /* randomize choice of the initial node */
                locator->roundRobinNode = (abs(rand()) % locator->nodeCount) - 1;
            }
            else
            {
                locator->locatefunc = locate_static;
                if (nodeMap == NULL)
                {
                    /* no map, prepare array with indexes */
                    int *intptr;
                    nodeMap = palloc(locator->nodeCount * sizeof(int));
                    intptr = (int *) nodeMap;
                    for (i = 0; i < locator->nodeCount; i++)
                        *intptr++ = i;
                }
                locator->nodeMap = nodeMap;
                locator->results = nodeMap;
            }
            break;
        case LOCATOR_TYPE_HASH:
            if (accessType == RELATION_ACCESS_INSERT)
            {
                locator->locatefunc = locate_hash_insert;
                locator->nodeMap = nodeMap;
                switch (locator->listType)
                {
                    case LOCATOR_LIST_NONE:
                    case LOCATOR_LIST_INT:
                        locator->results = palloc(sizeof(int));
                        break;
                    case LOCATOR_LIST_OID:
                        locator->results = palloc(sizeof(Oid));
                        break;
                    case LOCATOR_LIST_POINTER:
                        locator->results = palloc(sizeof(void *));
                        break;
                    case LOCATOR_LIST_LIST:
                        /* Should never happen */
                        Assert(false);
                        break;
                }
            }
            else
            {
                locator->locatefunc = locate_hash_select;
                locator->nodeMap = nodeMap;
                switch (locator->listType)
                {
                    case LOCATOR_LIST_NONE:
                    case LOCATOR_LIST_INT:
                        locator->results = palloc(locator->nodeCount * sizeof(int));
                        break;
                    case LOCATOR_LIST_OID:
                        locator->results = palloc(locator->nodeCount * sizeof(Oid));
                        break;
                    case LOCATOR_LIST_POINTER:
                        locator->results = palloc(locator->nodeCount * sizeof(void *));
                        break;
                    case LOCATOR_LIST_LIST:
                        /* Should never happen */
                        Assert(false);
                        break;
                }
            }

            locator->hashfunc = hash_func_ptr(dataType);
            if (locator->hashfunc == NULL)
                ereport(ERROR, (errmsg("Error: unsupported data type for HASH locator: %d\n",
                                   dataType)));
            break;
        case LOCATOR_TYPE_MODULO:
            if (accessType == RELATION_ACCESS_INSERT)
            {
                locator->locatefunc = locate_modulo_insert;
                locator->nodeMap = nodeMap;
                switch (locator->listType)
                {
                    case LOCATOR_LIST_NONE:
                    case LOCATOR_LIST_INT:
                        locator->results = palloc(sizeof(int));
                        break;
                    case LOCATOR_LIST_OID:
                        locator->results = palloc(sizeof(Oid));
                        break;
                    case LOCATOR_LIST_POINTER:
                        locator->results = palloc(sizeof(void *));
                        break;
                    case LOCATOR_LIST_LIST:
                        /* Should never happen */
                        Assert(false);
                        break;
                }
            }
            else
            {
                locator->locatefunc = locate_modulo_select;
                locator->nodeMap = nodeMap;
                switch (locator->listType)
                {
                    case LOCATOR_LIST_NONE:
                    case LOCATOR_LIST_INT:
                        locator->results = palloc(locator->nodeCount * sizeof(int));
                        break;
                    case LOCATOR_LIST_OID:
                        locator->results = palloc(locator->nodeCount * sizeof(Oid));
                        break;
                    case LOCATOR_LIST_POINTER:
                        locator->results = palloc(locator->nodeCount * sizeof(void *));
                        break;
                    case LOCATOR_LIST_LIST:
                        /* Should never happen */
                        Assert(false);
                        break;
                }
            }

            locator->valuelen = modulo_value_len(dataType);
            if (locator->valuelen == -1)
                ereport(ERROR, (errmsg("Error: unsupported data type for MODULO locator: %d\n",
                                   dataType)));
            break;
        default:
            ereport(ERROR, (errmsg("Error: no such supported locator type: %c\n",
                                   locatorType)));
    }

    if (result)
	{
        *result = locator->results;
	}

    return locator;
}


void
freeLocator(Locator *locator)
{
    pfree(locator->nodeMap);
    /*
     * locator->nodeMap and locator->results may point to the same memory,
     * do not free it twice
     */
    if (locator->results != locator->nodeMap)
	{
        pfree(locator->results);
	}
    pfree(locator);
}


/*
 * Each time return the same predefined results
 */
static int
locate_static(Locator *self, Datum value, bool isnull,
#ifdef __COLD_HOT__
                Datum secValue, bool secIsNull,
#endif
                bool *hasprimary)
{
    /* TODO */
    if (hasprimary)
	{
        *hasprimary = false;
	}
    return self->nodeCount;
}


/*
 * Each time return one next node, in round robin manner
 */
static int
locate_roundrobin(Locator *self, Datum value, bool isnull,
#ifdef __COLD_HOT__
                       Datum secValue, bool secIsNull,
#endif
                       bool *hasprimary)
{// #lizard forgives
    /* TODO */
    if (hasprimary)
	{
        *hasprimary = false;
	}
    if (++self->roundRobinNode >= self->nodeCount)
	{
        self->roundRobinNode = 0;
	}
    switch (self->listType)
    {
        case LOCATOR_LIST_NONE:
            ((int *) self->results)[0] = self->roundRobinNode;
            break;
        case LOCATOR_LIST_INT:
            ((int *) self->results)[0] =
                    ((int *) self->nodeMap)[self->roundRobinNode];
            break;
        case LOCATOR_LIST_OID:
            ((Oid *) self->results)[0] =
                    ((Oid *) self->nodeMap)[self->roundRobinNode];
            break;
        case LOCATOR_LIST_POINTER:
            ((void **) self->results)[0] =
                    ((void **) self->nodeMap)[self->roundRobinNode];
            break;
        case LOCATOR_LIST_LIST:
            /* Should never happen */
            Assert(false);
            break;
    }
    return 1;
}

/*
 * Each time return one node, in a random manner
 * This is similar to locate_modulo_select, but that
 * function does not use a random modulo..
 */
static int
locate_modulo_random(Locator *self, Datum value, bool isnull,
#ifdef __COLD_HOT__
                              Datum secValue, bool secIsNull,
#endif
                              bool *hasprimary)
{
    int offset;

    if (hasprimary)
	{
        *hasprimary = false;
	}

    Assert(self->nodeCount > 0);
    offset = compute_modulo(abs(rand()), self->nodeCount);
    switch (self->listType)
    {
        case LOCATOR_LIST_NONE:
            ((int *) self->results)[0] = offset;
            break;
        case LOCATOR_LIST_INT:
            ((int *) self->results)[0] =
                    ((int *) self->nodeMap)[offset];
            break;
        case LOCATOR_LIST_OID:
            ((Oid *) self->results)[0] =
                    ((Oid *) self->nodeMap)[offset];
            break;
        case LOCATOR_LIST_POINTER:
            ((void **) self->results)[0] =
                    ((void **) self->nodeMap)[offset];
            break;
        case LOCATOR_LIST_LIST:
            /* Should never happen */
            Assert(false);
            break;
    }
    return 1;
}

/*
 * Calculate hash from supplied value and use modulo by nodeCount as an index
 */
static int
locate_hash_insert(Locator *self, Datum value, bool isnull,
#ifdef __COLD_HOT__
                        Datum secValue, bool secIsNull,
#endif
                        bool *hasprimary)
{// #lizard forgives
    int index;
    if (hasprimary)
	{
        *hasprimary = false;
	}
    if (isnull)
	{
        index = 0;
	}
    else
    {
        unsigned int hash32;

        hash32 = (unsigned int) DatumGetInt32(DirectFunctionCall1(self->hashfunc, value));

        index = compute_modulo(hash32, self->nodeCount);
    }
    switch (self->listType)
    {
        case LOCATOR_LIST_NONE:
            ((int *) self->results)[0] = index;
            break;
        case LOCATOR_LIST_INT:
            ((int *) self->results)[0] = ((int *) self->nodeMap)[index];
            break;
        case LOCATOR_LIST_OID:
            ((Oid *) self->results)[0] = ((Oid *) self->nodeMap)[index];
            break;
        case LOCATOR_LIST_POINTER:
            ((void **) self->results)[0] = ((void **) self->nodeMap)[index];
            break;
        case LOCATOR_LIST_LIST:
            /* Should never happen */
            Assert(false);
            break;
    }
    return 1;
}

#ifdef _MIGRATE_

static int locate_shard_insert(Locator *self, Datum value, bool isnull,
#ifdef __COLD_HOT__
                                    Datum secValue, bool secIsNull,
#endif
                                    bool *hasprimary)
{// #lizard forgives
    int global_index = 0;
    int local_index  = 0;
    
    if (hasprimary)
    {
        *hasprimary = false;
    }
    
    if (isnull)
    {
        long         hashvalue                    = 0;
        
        global_index = GetNodeIndexByHashValue(self->groupid, hashvalue);
    }
    else
    {    
        if (self->need_shardmap_router)
        {
            int i = 0;
            ListCell *cell;
            List *nodelist = ShardMapRouter(self->groupid, self->coldGroupId, self->relid,
                                            self->dataType, value, self->secAttrNum, self->secDataType,
                                            secIsNull, secValue, self->accessType);

            i = 0;
            foreach(cell, nodelist)
            {
                global_index = lfirst_int(cell);

                switch (self->listType)
                {
                    case LOCATOR_LIST_NONE:
                        {
                            ((int *) self->results)[i] = global_index;
                        }
                        break;
                    case LOCATOR_LIST_INT:
                        {
                            ((int *) self->results)[i] = global_index;
                        }
                        break;
                    case LOCATOR_LIST_OID:
                        {
                            ((Oid *) self->results)[i] = ((Oid *) self->nodeMap)[global_index];
                        }
                        break;
                        
                    case LOCATOR_LIST_POINTER:
                        {
                            if (self->need_shardmap_router)
                            {
                                local_index = self->indexMap[global_index];    

                                if (local_index == -1)
                                {
                                    elog(ERROR, "could not map global_index %d to local", global_index);
                                }
                            }
                            else
                            {
                                local_index = self->nodeindexMap[global_index];    
                            }
                            ((void **) self->results)[i] = ((void **) self->nodeMap)[local_index];
                        }
                        break;
                    case LOCATOR_LIST_LIST:
                        /* Should never happen */
                        Assert(false);
                        break;
                }
                i++;
            }

            return i;
        }
        else
        {
            long         hashvalue                    = 0;

            hashvalue = compute_hash(self->dataType, value, LOCATOR_TYPE_SHARD);

            global_index = GetNodeIndexByHashValue(self->groupid, hashvalue);
        }
    }    
    
    switch (self->listType)
    {
        case LOCATOR_LIST_NONE:
            {
                ((int *) self->results)[0] = global_index;
            }
            break;
        case LOCATOR_LIST_INT:
            {
                ((int *) self->results)[0] = global_index;
            }
            break;
        case LOCATOR_LIST_OID:
            {
                ((Oid *) self->results)[0] = ((Oid *) self->nodeMap)[global_index];
            }
            break;
            
        case LOCATOR_LIST_POINTER:
            {
                if (self->need_shardmap_router)
                {
                    local_index = self->indexMap[global_index];    

                    if (local_index == -1)
                    {
                        elog(ERROR, "could not map global_index %d to local", global_index);
                    }
                }
                else
                {
                    Assert(global_index >= 0);
                    local_index = self->nodeindexMap[global_index];    
                }
                ((void **) self->results)[0] = ((void **) self->nodeMap)[local_index];
            }
            break;
        case LOCATOR_LIST_LIST:
            /* Should never happen */
            Assert(false);
            break;
    }
    
    /* set to 1, since will route to only one node. */
    return 1;    
}


static int locate_shard_select(Locator *self, Datum value, bool isnull,
#ifdef __COLD_HOT__
                            Datum secValue, bool secIsNull,
#endif
                            bool *hasprimary)
{// #lizard forgives
    int local_index  = 0;
    int    global_index = 0;
    
    if (hasprimary)
    {
        *hasprimary = false;
    }
    
    if (isnull)
    {
        int i;
        switch (self->listType)
        {
            case LOCATOR_LIST_NONE:
                for (i = 0; i < self->nodeCount; i++)
                    ((int *) self->results)[i] = i;
                break;
            case LOCATOR_LIST_INT:
                memcpy(self->results, self->nodeMap,
                       self->nodeCount * sizeof(int));
                break;
            case LOCATOR_LIST_OID:
                memcpy(self->results, self->nodeMap,
                       self->nodeCount * sizeof(Oid));
                break;
            case LOCATOR_LIST_POINTER:
                memcpy(self->results, self->nodeMap,
                       self->nodeCount * sizeof(void *));
                break;
            case LOCATOR_LIST_LIST:
                /* Should never happen */
                Assert(false);
                break;
        }
        return self->nodeCount;
    }
    else
    {
        if (self->need_shardmap_router)
        {
            int i = 0;
            ListCell *cell;
            List *nodelist = ShardMapRouter(self->groupid, self->coldGroupId, self->relid,
                                            self->dataType, value, self->secAttrNum, self->secDataType,
                                            secIsNull, secValue, self->accessType);

            i = 0;
            foreach(cell, nodelist)
            {
                global_index = lfirst_int(cell);

                switch (self->listType)
                {
                    case LOCATOR_LIST_NONE:
                        {
                            ((int *) self->results)[i] = global_index;
                        }
                        break;
                    case LOCATOR_LIST_INT:
                        {
                            ((int *) self->results)[i] = global_index;
                        }
                        break;
                    case LOCATOR_LIST_OID:
                        {
                            ((Oid *) self->results)[i] = ((Oid *) self->nodeMap)[global_index];
                        }
                        break;
                        
                    case LOCATOR_LIST_POINTER:
                        {
                            if (self->need_shardmap_router)
                            {
                                local_index = self->indexMap[global_index];    
                                
                                if (local_index == -1)
                                {
                                    elog(ERROR, "could not map global_index %d to local", global_index);
                                }
                            }
                            else
                            {
                                local_index = self->nodeindexMap[global_index];    
                            }
                            ((void **) self->results)[i] = ((void **) self->nodeMap)[local_index];
                        }
                        break;
                    case LOCATOR_LIST_LIST:
                        /* Should never happen */
                        Assert(false);
                        break;
                }
                i++;
            }

            return i;
        }
        else
        {
            long         hashvalue                    = 0;

            hashvalue = compute_hash(self->dataType, value, LOCATOR_TYPE_SHARD);

            global_index = GetNodeIndexByHashValue(self->groupid, hashvalue);
			Assert(global_index >= 0);
            
            switch (self->listType)
            {
                case LOCATOR_LIST_NONE:
                    ((int *) self->results)[0] = global_index;
                    break;
                case LOCATOR_LIST_INT:
                    ((int *) self->results)[0] = global_index;
                    break;
                case LOCATOR_LIST_OID:
                    ((Oid *) self->results)[0] = ((Oid *) self->nodeMap)[global_index];
                    break;
                case LOCATOR_LIST_POINTER:
                    local_index        = self->nodeindexMap[global_index];
                    ((void **) self->results)[0] = ((void **) self->nodeMap)[local_index];
                    break;
                case LOCATOR_LIST_LIST:
                    /* Should never happen */
                    Assert(false);
                    break;
            }

            /* set to 1, since will route to only one node. */
            return 1;
        }
    }
}

#endif


/*
 * Calculate hash from supplied value and use modulo by nodeCount as an index
 * if value is NULL assume no hint and return all the nodes.
 */
static int
locate_hash_select(Locator *self, Datum value, bool isnull,
#ifdef __COLD_HOT__
                        Datum secValue, bool secIsNull,
#endif
                        bool *hasprimary)
{// #lizard forgives
    if (hasprimary)
	{
        *hasprimary = false;
	}
    if (isnull)
    {
        int i;
        switch (self->listType)
        {
            case LOCATOR_LIST_NONE:
                for (i = 0; i < self->nodeCount; i++)
                    ((int *) self->results)[i] = i;
                break;
            case LOCATOR_LIST_INT:
                memcpy(self->results, self->nodeMap,
                       self->nodeCount * sizeof(int));
                break;
            case LOCATOR_LIST_OID:
                memcpy(self->results, self->nodeMap,
                       self->nodeCount * sizeof(Oid));
                break;
            case LOCATOR_LIST_POINTER:
                memcpy(self->results, self->nodeMap,
                       self->nodeCount * sizeof(void *));
                break;
            case LOCATOR_LIST_LIST:
                /* Should never happen */
                Assert(false);
                break;
        }
        return self->nodeCount;
    }
    else
    {
        unsigned int hash32;
        int          index;

        hash32 = (unsigned int) DatumGetInt32(DirectFunctionCall1(self->hashfunc, value));

        index = compute_modulo(hash32, self->nodeCount);
        switch (self->listType)
        {
            case LOCATOR_LIST_NONE:
                ((int *) self->results)[0] = index;
                break;
            case LOCATOR_LIST_INT:
                ((int *) self->results)[0] = ((int *) self->nodeMap)[index];
                break;
            case LOCATOR_LIST_OID:
                ((Oid *) self->results)[0] = ((Oid *) self->nodeMap)[index];
                break;
            case LOCATOR_LIST_POINTER:
                ((void **) self->results)[0] = ((void **) self->nodeMap)[index];
                break;
            case LOCATOR_LIST_LIST:
                /* Should never happen */
                Assert(false);
                break;
        }
        return 1;
    }
}


/*
 * Use modulo of supplied value by nodeCount as an index
 */
static int
locate_modulo_insert(Locator *self, Datum value, bool isnull,
#ifdef __COLD_HOT__
                           Datum secValue, bool secIsNull,
#endif 
                           bool *hasprimary)
{// #lizard forgives
    int index;
    if (hasprimary)
	{
        *hasprimary = false;
	}
    if (isnull)
	{
        index = 0;
	}
    else
    {
        uint64 val;

        if (self->valuelen == 8)
		{
            val = (uint64) (GET_8_BYTES(value));
		}
        else if (self->valuelen == 4)
		{
            val = (uint64) (GET_4_BYTES(value));
		}
        else if (self->valuelen == 2)
		{
            val = (uint64) (GET_2_BYTES(value));
		}
        else if (self->valuelen == 1)
		{
            val = (uint64) (GET_1_BYTE(value));
		}
        else
		{
            val = 0;
		}

        index = compute_modulo(val, self->nodeCount);
    }
    switch (self->listType)
    {
        case LOCATOR_LIST_NONE:
            ((int *) self->results)[0] = index;
            break;
        case LOCATOR_LIST_INT:
            ((int *) self->results)[0] = ((int *) self->nodeMap)[index];
            break;
        case LOCATOR_LIST_OID:
            ((Oid *) self->results)[0] = ((Oid *) self->nodeMap)[index];
            break;
        case LOCATOR_LIST_POINTER:
            ((void **) self->results)[0] = ((void **) self->nodeMap)[index];
            break;
        case LOCATOR_LIST_LIST:
            /* Should never happen */
            Assert(false);
            break;
    }
    return 1;
}


/*
 * Use modulo of supplied value by nodeCount as an index
 * if value is NULL assume no hint and return all the nodes.
 */
static int
locate_modulo_select(Locator *self, Datum value, bool isnull,
#ifdef __COLD_HOT__
                            Datum secValue, bool secIsNull,
#endif
                            bool *hasprimary)
{// #lizard forgives
    if (hasprimary)
	{
        *hasprimary = false;
	}
    if (isnull)
    {
        int i;
        switch (self->listType)
        {
            case LOCATOR_LIST_NONE:
                for (i = 0; i < self->nodeCount; i++)
                    ((int *) self->results)[i] = i;
                break;
            case LOCATOR_LIST_INT:
                memcpy(self->results, self->nodeMap,
                       self->nodeCount * sizeof(int));
                break;
            case LOCATOR_LIST_OID:
                memcpy(self->results, self->nodeMap,
                       self->nodeCount * sizeof(Oid));
                break;
            case LOCATOR_LIST_POINTER:
                memcpy(self->results, self->nodeMap,
                       self->nodeCount * sizeof(void *));
                break;
            case LOCATOR_LIST_LIST:
                /* Should never happen */
                Assert(false);
                break;
        }
        return self->nodeCount;
    }
    else
    {
        uint64    val;
        int     index;

        if (self->valuelen == 8)
		{
            val = (uint64) (GET_8_BYTES(value));
		}
        else if (self->valuelen == 4)
		{
            val = (unsigned int) (GET_4_BYTES(value));
		}
        else if (self->valuelen == 2)
		{
            val = (unsigned int) (GET_2_BYTES(value));
		}
        else if (self->valuelen == 1)
		{
            val = (unsigned int) (GET_1_BYTE(value));
		}
        else
		{
            val = 0;
		}

        index = compute_modulo(val, self->nodeCount);

        switch (self->listType)
        {
            case LOCATOR_LIST_NONE:
                ((int *) self->results)[0] = index;
                break;
            case LOCATOR_LIST_INT:
                ((int *) self->results)[0] = ((int *) self->nodeMap)[index];
                break;
            case LOCATOR_LIST_OID:
                ((Oid *) self->results)[0] = ((Oid *) self->nodeMap)[index];
                break;
            case LOCATOR_LIST_POINTER:
                ((void **) self->results)[0] = ((void **) self->nodeMap)[index];
                break;
            case LOCATOR_LIST_LIST:
                /* Should never happen */
                Assert(false);
                break;
        }
        return 1;
    }
}


int
GET_NODES(Locator *self, Datum value, bool isnull,
#ifdef __COLD_HOT__
                 Datum secValue, bool secIsNull,
#endif
                 bool *hasprimary)
{
#ifdef __COLD_HOT__
    return (*self->locatefunc) (self, value, isnull, secValue, secIsNull, hasprimary);
#else
    return (*self->locatefunc) (self, value, isnull, hasprimary);
#endif
}

#ifdef __TBASE__

char
getLocatorDisType(Locator *self)
{
    return self->locatorType;
}

bool
IsDistributedColumn(AttrNumber attr, RelationLocInfo *relation_loc_info)
{
    bool result = false;
    
    if (relation_loc_info && IsLocatorDistributedByValue(relation_loc_info->locatorType) &&
       (attr == relation_loc_info->partAttrNum || attr == relation_loc_info->secAttrNum))
    {
        result = true;
    }

    return result;
}

/*
 * Calculate the tuple replication times based on replication type and number
 * of target nodes.
 */
int
calcDistReplications(char distributionType, Bitmapset *nodes)
{
	if (!nodes)
	{
		return 1;
	}

	if (IsLocatorReplicated(distributionType) ||
		IsLocatorNone(distributionType))
	{
		return  bms_num_members(nodes);
	}

	return 1;
}

#endif

void *
getLocatorResults(Locator *self)
{
    return self->results;
}


void *
getLocatorNodeMap(Locator *self)
{
    return self->nodeMap;
}


int
getLocatorNodeCount(Locator *self)
{
    return self->nodeCount;
}

#endif

/*
 * GetRelationNodes
 *
 * Get list of relation nodes
 * If the table is replicated and we are reading, we can just pick one.
 * If the table is partitioned, we apply partitioning column value, if possible.
 *
 * If the relation is partitioned, partValue will be applied if present
 * (indicating a value appears for partitioning column), otherwise it
 * is ignored.
 *
 * preferredNodes is only used when for replicated tables. If set, it will
 * use one of the nodes specified if the table is replicated on it.
 * This helps optimize for avoiding introducing additional nodes into the
 * transaction.
 *
 * The returned List is a copy, so it should be freed when finished.
 */
ExecNodes *
GetRelationNodes(RelationLocInfo *rel_loc_info, Datum valueForDistCol,
                bool isValueNull,
#ifdef __COLD_HOT__
                Datum valueForSecDistCol, bool isSecValueNull,
#endif
                RelationAccessType accessType)
{// #lizard forgives
    ExecNodes    *exec_nodes;
    int            *nodenums;
    int            i, count;
    Locator        *locator;
    Oid typeOfValueForDistCol = InvalidOid;
#ifdef __COLD_HOT__
    Oid typeOfValueForSecDistCol = InvalidOid;
#endif

    if (rel_loc_info == NULL)
	{
        return NULL;
	}


    if (IsLocatorDistributedByValue(rel_loc_info->locatorType))
    {
        /* A sufficient lock level needs to be taken at a higher level */
        Relation rel = relation_open(rel_loc_info->relid, NoLock);
        TupleDesc    tupDesc = RelationGetDescr(rel);
        Form_pg_attribute *attr = tupDesc->attrs;
        /* Get the hash type of relation */
        typeOfValueForDistCol = attr[rel_loc_info->partAttrNum - 1]->atttypid;

#ifdef __COLD_HOT__
        if (AttributeNumberIsValid(rel_loc_info->secAttrNum))
        {
            typeOfValueForSecDistCol = attr[rel_loc_info->secAttrNum - 1]->atttypid;
        }
#endif

        relation_close(rel, NoLock);
    }

    exec_nodes = makeNode(ExecNodes);
    exec_nodes->baselocatortype = rel_loc_info->locatorType;
    exec_nodes->accesstype = accessType;
    
#ifdef  _MIGRATE_
    locator = createLocator(rel_loc_info->locatorType,
                            accessType,
                            typeOfValueForDistCol,
                            LOCATOR_LIST_LIST,
                            0,
                            (void *)rel_loc_info->rl_nodeList,
                            (void **)&nodenums,
                            false,
                            rel_loc_info->groupId, rel_loc_info->coldGroupId,
                            typeOfValueForSecDistCol, rel_loc_info->secAttrNum,
                            rel_loc_info->relid);
#else

    locator = createLocator(rel_loc_info->locatorType,
                            accessType,
                            typeOfValueForDistCol,
                            LOCATOR_LIST_LIST,
                            0,
                            (void *)rel_loc_info->rl_nodeList,
                            (void **)&nodenums,
                            false);
#endif
#ifdef __COLD_HOT__
    count = GET_NODES(locator, valueForDistCol, isValueNull, valueForSecDistCol, isSecValueNull, NULL);
#else
    count = GET_NODES(locator, valueForDistCol, isValueNull, NULL);
#endif

    for (i = 0; i < count; i++)
        exec_nodes->nodeList = lappend_int(exec_nodes->nodeList, nodenums[i]);

    freeLocator(locator);
    return exec_nodes;
}

/*
 * GetRelationNodesForExplain
 * This is just for explain statement, just pick one datanode.
 * The returned List is a copy, so it should be freed when finished.
 */
ExecNodes *
GetRelationNodesForExplain(RelationLocInfo *rel_loc_info,
				RelationAccessType accessType)
{
	ExecNodes	*exec_nodes;
	exec_nodes = makeNode(ExecNodes);
	exec_nodes->baselocatortype = rel_loc_info->locatorType;
	exec_nodes->accesstype = accessType;
	exec_nodes->nodeList = lappend_int(exec_nodes->nodeList, 1);
	return exec_nodes;
}

/*
 * GetRelationNodesByQuals
 * A wrapper around GetRelationNodes to reduce the node list by looking at the
 * quals. varno is assumed to be the varno of reloid inside the quals. No check
 * is made to see if that's correct.
 */
ExecNodes *
GetRelationNodesByQuals(Oid reloid, RelationLocInfo *rel_loc_info,
            Index varno, Node *quals, RelationAccessType relaccess, Node **dis_qual, Node **sec_quals)
{// #lizard forgives
#define ONE_SECOND_DATUM 1000000
    Expr            *distcol_expr = NULL;
	ExecNodes *exec_nodes = NULL;
    Datum            distcol_value;
    bool            distcol_isnull;
#ifdef __COLD_HOT__
    int             i = 0;
    List            *seccol_list = NULL;
    List            *seccol_value_list = NULL;
    Datum            seccol_value = 0;
    bool            seccol_isnull = true;
    Oid                seccol_type = InvalidOid;
    Oid             distcol_type = InvalidOid;
    Oid                *opArray            = NULL;
    bool            *isswapArray       = NULL;
	Oid disttype;
	int32 disttypmod;

    if (dis_qual)
    {
        *dis_qual = NULL;
    }

    if (sec_quals)
    {
        *sec_quals = NULL;
    }
#endif

    if (!rel_loc_info)
	{
        return NULL;
	}
    /*
     * If the table distributed by value, check if we can reduce the Datanodes
     * by looking at the qualifiers for this relation
     */
	disttype = get_atttype(reloid, rel_loc_info->partAttrNum);
	disttypmod = get_atttypmod(reloid, rel_loc_info->partAttrNum);

    if (IsRelationDistributedByValue(rel_loc_info))
    {
        distcol_expr = pgxc_find_distcol_expr(varno, rel_loc_info->partAttrNum,
                                                    quals);

		/* Remove ArrayCoerceExpr at first */
		if (distcol_expr && IsA(distcol_expr, ArrayCoerceExpr))
		{
			ArrayCoerceExpr *arrayCoerceExpr = castNode(ArrayCoerceExpr, distcol_expr);

			if (arrayCoerceExpr->arg && IsA(arrayCoerceExpr->arg, ArrayExpr))
			{
			distcol_expr = arrayCoerceExpr->arg;
		}
		}

        /*
         * If the type of expression used to find the Datanode, is not same as
         * the distribution column type, try casting it. This is same as what
         * will happen in case of inserting that type of expression value as the
         * distribution column value.
         */
		if (distcol_expr && !IsA(distcol_expr, ArrayExpr))
        {
            distcol_expr = (Expr *)coerce_to_target_type(NULL,
                                                    (Node *)distcol_expr,
                                                    exprType((Node *)distcol_expr),
                                                    disttype, disttypmod,
                                                    COERCION_ASSIGNMENT,
                                                    COERCE_IMPLICIT_CAST, -1);
            /*
             * PGXC_FQS_TODO: We should set the bound parameters here, but we don't have
             * PlannerInfo struct and we don't handle them right now.
             * Even if constant expression mutator changes the expression, it will
             * only simplify it, keeping the semantics same
             */
            distcol_expr = (Expr *)eval_const_expressions(NULL,
                                                            (Node *)distcol_expr);
        }

#ifdef __COLD_HOT__
        if (rel_loc_info->secAttrNum != InvalidAttrNumber)
        {
            Oid        sectype = get_atttype(reloid, rel_loc_info->secAttrNum);
            int32    sectypmod = get_atttypmod(reloid, rel_loc_info->secAttrNum);
            seccol_list = pgxc_find_distcol_exprs(varno, rel_loc_info->secAttrNum,
                                                        quals);

            if (seccol_list)
            {
                ListCell  *qual_cell;
                int len = list_length(seccol_list);
                
                opArray = palloc0(sizeof(Oid) * len);    
                isswapArray = palloc0(sizeof(bool) * len);
                i = 0;

                foreach(qual_cell, seccol_list)
                {
                    Expr *expr = NULL;
                    DisQual *pQual = (DisQual *)lfirst(qual_cell);
                    opArray[i] = pQual->opno;
                    isswapArray[i] = pQual->isswap;
                    expr = (Expr *)pQual->expr;
                    expr = (Expr *)coerce_to_target_type(NULL,
                                            (Node *)expr,
                                            exprType((Node *)expr),
                                            sectype, sectypmod,
                                            COERCION_ASSIGNMENT,
                                            COERCE_IMPLICIT_CAST, -1);

                    expr = (Expr *)eval_const_expressions(NULL,
                                            (Node *)expr);

                    seccol_value_list = lappend(seccol_value_list, (void*)expr);

                    i++;
                }

                if (sec_quals)
                {
                    *sec_quals = (Node *)seccol_list;
                }
            }
        }
        else
        {
            seccol_list = NULL;
        }
#endif
    }

    if (distcol_expr && IsA(distcol_expr, Const))
    {
        Const *const_expr = (Const *)distcol_expr;
        distcol_value = const_expr->constvalue;
        distcol_isnull = const_expr->constisnull;
        distcol_type = const_expr->consttype;

#ifdef __COLD_HOT__
        if (dis_qual)
        {
            *dis_qual = (Node *)distcol_expr;
        }
        
        if (seccol_list && !distcol_isnull)
        {
            bool isalign;
            int rangeOpCnt = 0;
            ListCell  * cell;
            bool  equal_min = false;
            bool  equal_max = false;
            Datum minStamp = 0;
            Datum maxStamp = 0;
            Datum constvalue;

            i = 0;
            foreach(cell, seccol_value_list)
            {
                Expr *sec_distcol_expr = (Expr *)lfirst(cell);

                if (sec_distcol_expr && IsA(sec_distcol_expr, Const) && !TimeStampRange(opArray[i]))
                {
                    Const *const_expr = (Const *)sec_distcol_expr;
                    seccol_value     = const_expr->constvalue;
                    seccol_isnull    = const_expr->constisnull;

                    if (!seccol_isnull)
                    {
                        exec_nodes = GetRelationNodes(rel_loc_info, 
                                                      distcol_value,
                                                      distcol_isnull, 
                                                      seccol_value,
                                                      seccol_isnull,
                                                      relaccess);
                        return exec_nodes;
                    }
                }
                else if (sec_distcol_expr && IsA(sec_distcol_expr, Const) && TimeStampRange(opArray[i]))
                {
                    Const *const_expr = (Const *)sec_distcol_expr;

                    if (2062 == opArray[i]) /* < */
                    {
                        isalign = IsConstAligned(reloid, const_expr->constvalue, rel_loc_info->secAttrNum);

                        if (isswapArray[i])
                        {
                            constvalue = const_expr->constvalue;
                            if (isalign)
                            {
                                /* const < var -> const + 1 <= var */
                                constvalue = constvalue + ONE_SECOND_DATUM;
                            }
                            minStamp = minStamp ? ((constvalue >= minStamp) ? minStamp : constvalue) : constvalue;
                            seccol_type = const_expr->consttype;

                            if (isalign)
                            {
                                equal_min = true;
                            }
                            else
                            {
                                equal_min = false;
                            }
                        }
                        else
                        {
                            constvalue = const_expr->constvalue;
                            if (isalign)
                            {
                                /* var < const -> var <= const -1 */
                                constvalue = constvalue - ONE_SECOND_DATUM;
                            }
                            
                            maxStamp = (constvalue >= maxStamp) ? constvalue : maxStamp;
                            seccol_type = const_expr->consttype;

                            if (isalign)
                            {
                                equal_max = true;
                            }
                            else
                            {
                                equal_max = false;
                            }
                        }
                    }
                    else if (2063 == opArray[i]) /* <= */
                    {
                        if (isswapArray[i])
                        {
                            /* const <= var */
							minStamp = minStamp ? ((const_expr->constvalue >= minStamp) ? minStamp
							                                                            : const_expr->constvalue)
							                    : const_expr->constvalue;
                            seccol_type = const_expr->consttype;
                            equal_min = true;
                        }
                        else
                        {
                            /* var <= const */
                            maxStamp = (const_expr->constvalue >= maxStamp) ? const_expr->constvalue : maxStamp;
                            seccol_type = const_expr->consttype;
                            equal_max = true;
                        }
                    }  
                    else if (2065 == opArray[i]) /* >= */
                    {     
                        if (isswapArray[i])
                        {
                            /* const >= var */
                            maxStamp = (const_expr->constvalue >= maxStamp) ? const_expr->constvalue : maxStamp;
                            seccol_type = const_expr->consttype;
                            equal_max = true;
                        }
                        else
                        {
                            /* var >= const */
							minStamp = minStamp ? ((const_expr->constvalue >= minStamp) ? minStamp
							                                                            : const_expr->constvalue)
							                    : const_expr->constvalue;
                            seccol_type = const_expr->consttype;
                            equal_min = true;
                        }
                    }
                    else if (2064 == opArray[i]) /* > */
                    {
                        isalign = IsConstAligned(reloid, const_expr->constvalue, rel_loc_info->secAttrNum);

                        if (isswapArray[i])
                        {
                            constvalue = const_expr->constvalue;
                            if (isalign)
                            {
                                /* const > var -> const -1 >= var */
                                constvalue = constvalue - ONE_SECOND_DATUM;
                            }
                            maxStamp = (constvalue >= maxStamp) ? constvalue : maxStamp;
                            seccol_type = const_expr->consttype;

                            if (isalign)
                            {
                                equal_max = true;
                            }
                            else
                            {
                                equal_max = false;
                            }
                        }
                        else
                        {
                            constvalue = const_expr->constvalue;
                            if (isalign)
                            {
                                /* var > const -> var  >= const + 1 */
                                constvalue = constvalue + ONE_SECOND_DATUM;
                            }
                            
                            minStamp = minStamp ? ((constvalue >= minStamp) ? minStamp : constvalue) : constvalue;
                            seccol_type = const_expr->consttype;

                            if (isalign)
                            {
                                equal_min = true;
                            }
                            else
                            {
                                equal_min = false;
                            }
                        }
                    }
                    else
                    {
                        seccol_type = InvalidOid;
                    }
                    /* skip range condition */
                    rangeOpCnt++;
                }
                else
                {
                    seccol_value      = (Datum) 0;
                    seccol_isnull    = true;
                }

                i++;
            }

            if (opArray)
            {
                pfree(opArray);
                opArray = NULL;
            }

            if (isswapArray)
            {
                pfree(isswapArray);
                isswapArray = NULL;
            }

            if (rangeOpCnt == list_length(seccol_value_list) || 
                (rangeOpCnt && seccol_isnull))
            {
                exec_nodes = GetRelationTimeStampRangeNodes(rel_loc_info, distcol_value,
                                                            distcol_isnull, distcol_type,
                                                            seccol_type, minStamp, maxStamp, 
                                                            equal_min, equal_max, relaccess);
                if (exec_nodes->nodeList == NULL)
                {
                    seccol_value      = (Datum) 0;
                    seccol_isnull    = true;
                    pfree(exec_nodes);
                    exec_nodes = NULL;
                }
                else
                {
                    return exec_nodes;
                }
            }
        }
#endif

		return GetRelationNodes(rel_loc_info, distcol_value,
		                        distcol_isnull,
		                        seccol_value, seccol_isnull,
		                        relaccess);
	}
	/* Only for shard table without cold hot seperation */
	else if (distcol_expr && IsA(distcol_expr, ArrayExpr) &&
	         rel_loc_info->locatorType == LOCATOR_TYPE_SHARD && !seccol_list)
	{
		ArrayExpr *arrayExpr = (ArrayExpr *) distcol_expr;
		ListCell *lc;
		bool success = true;
		Const *const_expr;
		ExecNodes *temp;

		foreach(lc, arrayExpr->elements)
		{
			Node *expr = (Node *) lfirst(lc);

			/* convert to distribute column type */
			expr = coerce_to_target_type(NULL,
			                             (Node *) expr,
			                             exprType((Node *) expr),
			                             disttype, disttypmod,
			                             COERCION_ASSIGNMENT,
			                             COERCE_IMPLICIT_CAST, -1);
			expr = eval_const_expressions(NULL,
			                              (Node *) expr);
			if (!expr || !IsA(expr, Const))
			{
				success = false;
				break;
			}

			const_expr = castNode(Const, expr);
			temp = GetRelationNodes(rel_loc_info, const_expr->constvalue,
			                        const_expr->constisnull,
			                        seccol_value, seccol_isnull,
			                        relaccess);
			if (!temp)
			{
				success = false;
				break;
			}

			if (exec_nodes)
			{
				Assert(exec_nodes->baselocatortype == temp->baselocatortype);
				exec_nodes->nodeList = list_concat_unique_int(exec_nodes->nodeList,
				                                          temp->nodeList);
    }
    else
    {
				exec_nodes = temp;
			}
		}

		if (success)
		{
			return exec_nodes;
		}
	}

        distcol_value = (Datum) 0;
        distcol_isnull = true;
#ifdef __TBASE__
        if (rel_loc_info->secAttrNum != InvalidAttrNumber && seccol_list &&
            (relaccess == RELATION_ACCESS_READ || relaccess == RELATION_ACCESS_READ_FQS))
        {
            int32     nGroup;
            Oid     *groups;
            List    *newnodelist = NULL;
            Oid      relid = InvalidOid;

            Relation rel = relation_open(reloid, NoLock);

            if (RELATION_IS_CHILD(rel))
            {
                relid = RELATION_GET_PARENT(rel);
            }

            relation_close(rel, NoLock);

            GetRelationSecondGroup(relid, &groups, &nGroup);

            /* do not have key-value */
            if (nGroup == 0)
            {
                ListCell *cell = NULL;
                List *oids = GetRelationGroupsByQuals(reloid, rel_loc_info, (Node *)seccol_list);

                foreach(cell, oids)
                {
                    int j;
                    int32 dn_num;
                    int32 *datanodes;
                    Oid groupoid = lfirst_oid(cell);

                    GetShardNodes(groupoid, &datanodes, &dn_num, NULL);
                    for(j = 0; j < dn_num; j++)
                    {                            
                        newnodelist = list_append_unique_int(newnodelist, datanodes[j]);
                    }
                    pfree(datanodes);
                }

                if (newnodelist)
                {
                    exec_nodes = makeNode(ExecNodes);
                    exec_nodes->baselocatortype = rel_loc_info->locatorType;
                    exec_nodes->accesstype = relaccess;
                    exec_nodes->nodeList = newnodelist;
                    return exec_nodes;
                }
            }
        }
#endif

    exec_nodes = GetRelationNodes(rel_loc_info, distcol_value,
                                                distcol_isnull,
#ifdef __COLD_HOT__
                                                seccol_value, seccol_isnull,
#endif
                                                relaccess);
    return exec_nodes;
}

/*
 * GetRelationDistribColumn
 * Return hash column name for relation or NULL if relation is not distributed.
 */
char *
GetRelationDistribColumn(RelationLocInfo *locInfo)
{
    /* No relation, so simply leave */
    if (!locInfo)
	{
        return NULL;
	}

    /* No distribution column if relation is not distributed with a key */
    if (!IsRelationDistributedByValue(locInfo))
	{
        return NULL;
	}

    /* Return column name */
    return get_attname(locInfo->relid, locInfo->partAttrNum);
}

#ifdef __COLD_HOT__

char *
GetRelationSecDistribColumn(RelationLocInfo *locInfo)
{
    /* No relation, so simply leave */
    if (!locInfo)
	{
        return NULL;
	}

    /* No distribution column if relation is not distributed with a key */
    if (!IsRelationDistributedByValue(locInfo))
	{
        return NULL;
	}

    /* Return column name */
    return get_attname(locInfo->relid, locInfo->secAttrNum);
}

#endif

/*
 * pgxc_find_distcol_expr
 * Search through the quals provided and find out an expression which will give
 * us value of distribution column if exists in the quals. Say for a table
 * tab1 (val int, val2 int) distributed by hash(val), a query "SELECT * FROM
 * tab1 WHERE val = fn(x, y, z) and val2 = 3", fn(x,y,z) is the expression which
 * decides the distribution column value in the rows qualified by this query.
 * Hence return fn(x, y, z). But for a query "SELECT * FROM tab1 WHERE val =
 * fn(x, y, z) || val2 = 3", there is no expression which decides the values
 * distribution column val can take in the qualified rows. So, in such cases
 * this function returns NULL.
 */
static Expr *
pgxc_find_distcol_expr(Index varno,
                       AttrNumber attrNum,
                       Node *quals)
{// #lizard forgives
    List *lquals;
    ListCell *qual_cell;

    /* If no quals, no distribution column expression */
    if (!quals)
	{
        return NULL;
	}

    /* Convert the qualification into List if it's not already so */
    if (!IsA(quals, List))
	{
        lquals = make_ands_implicit((Expr *)quals);
	}
    else
	{
        lquals = (List *)quals;
	}

    /*
     * For every ANDed expression, check if that expression is of the form
     * <distribution_col> = <expr>. If so return expr.
     */
    foreach(qual_cell, lquals)
    {
        Expr *qual_expr = (Expr *)lfirst(qual_cell);
        Expr *lexpr;
        Expr *rexpr;
        Var *var_expr;
        Expr *distcol_expr;
		Oid opno;

		if (IsA(qual_expr, OpExpr))
		{
			OpExpr *op;

        op = (OpExpr *)qual_expr;

        /* If not a binary operator, it can not be '='. */
        if (list_length(op->args) != 2)
			{
            continue;
			}

        lexpr = linitial(op->args);
        rexpr = lsecond(op->args);
			opno = op->opno;
		}
		else if (IsA(qual_expr, ScalarArrayOpExpr))
		{
			ScalarArrayOpExpr *arrayOpExpr = (ScalarArrayOpExpr *) qual_expr;

			if (list_length(arrayOpExpr->args) != 2)
			{
				continue;
			}

			lexpr = linitial(arrayOpExpr->args);
			rexpr = lsecond(arrayOpExpr->args);
			opno = arrayOpExpr->opno;
		}
		else
		{
			continue;
		}

        /*
         * If either of the operands is a RelabelType, extract the Var in the RelabelType.
         * A RelabelType represents a "dummy" type coercion between two binary compatible datatypes.
         * If we do not handle these then our optimization does not work in case of varchar
         * For example if col is of type varchar and is the dist key then
         * select * from vc_tab where col = 'abcdefghijklmnopqrstuvwxyz';
         * should be shipped to one of the nodes only
         */
        if (IsA(lexpr, RelabelType))
		{
            lexpr = ((RelabelType*)lexpr)->arg;
		}
        if (IsA(rexpr, RelabelType))
		{
            rexpr = ((RelabelType*)rexpr)->arg;
		}

        /*
         * If either of the operands is a Var expression, assume the other
         * one is distribution column expression. If none is Var check next
         * qual.
         */
        if (IsA(lexpr, Var))
        {
            var_expr = (Var *)lexpr;
            distcol_expr = rexpr;
        }
        else if (IsA(rexpr, Var))
        {
            var_expr = (Var *)rexpr;
            distcol_expr = lexpr;
        }
        else
		{
            continue;
		}

        /*
         * If Var found is not the distribution column of required relation,
         * check next qual
         */
        if (var_expr->varno != varno || var_expr->varattno != attrNum)
		{
            continue;
		}

        /*
         * If the operator is not an assignment operator, check next
         * constraint. An operator is an assignment operator if it's
         * mergejoinable or hashjoinable. Beware that not every assignment
         * operator is mergejoinable or hashjoinable, so we might leave some
		 * opportunity. But then we have to rely on the opname which may not
         * be something we know to be equality operator as well.
         */
		if (!op_mergejoinable(opno, exprType((Node *) var_expr)) &&
		    !op_hashjoinable(opno, exprType((Node *) var_expr)))
		{
            continue;
		}

        /* Found the distribution column expression return it */
        return distcol_expr;

    }
    /* Exhausted all quals, but no distribution column expression */
    return NULL;
}

#ifdef __COLD_HOT__

static bool IsConstAligned(Oid reloid, Datum constvalue, AttrNumber secAttr)
{// #lizard forgives
    bool    isalign = false;
    int16   partition_strategy = 0;
    fsec_t  current_sec;
    Relation rel;
    struct pg_tm current_time;
    Form_pg_partition_interval routerinfo;
        
    rel        = relation_open(reloid, NoLock);
    routerinfo = rel->rd_partitions_info;
    if (routerinfo)
    {
        if (secAttr == routerinfo->partpartkey)
        {
            partition_strategy = routerinfo->partinterval_type;
            if (partition_strategy == IntervalType_Month &&
                    routerinfo->partinterval_int == 12)
            {
                partition_strategy = IntervalType_Year;
            }
        }
        else
        {
            relation_close(rel, NoLock);
            return false;
        }
    }
    relation_close(rel, NoLock);

    if (timestamp2tm(DatumGetTimestamp(constvalue), NULL, &current_time, &current_sec, NULL, NULL) != 0)
    {
        ereport(ERROR,
                    (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                     errmsg("timestamp out of range")));
    }
    if (current_time.tm_hour == 0 && current_time.tm_min == 0 && current_time.tm_sec == 0 && current_sec == 0)
    {
        isalign = true;
    }

    if(isalign && (partition_strategy == IntervalType_Month))
    {
        isalign = (current_time.tm_mday == 1);
    }

    if (isalign && partition_strategy == IntervalType_Year)
    {
        isalign = (current_time.tm_mday == 1 && current_time.tm_mon == 1);
    }

    return isalign;
}

static bool TimeStampRange(Oid op)
{
    switch (op)
    {
        case 2062:
        case 2063:
        case 2064:
        case 2065:
        {
            return true;
        }

        default:
        {
            return false;
        }        
    }
}

static List *
pgxc_find_distcol_exprs(Index varno,
                                     AttrNumber attrNum,
                                    Node *quals)
{// #lizard forgives
    List *lquals;
    ListCell *qual_cell;
    DisQual  *pQual = NULL;
    List *result  = NULL;
    List *lresult = NULL;

    /* If no quals, no distribution column expression */
    if (!quals)
	{
        return NULL;
	}

    /* Convert the qualification into List if it's not already so */
    if (!IsA(quals, List))
	{
        lquals = make_ands_implicit((Expr *)quals);
	}
    else
	{
        lquals = (List *)quals;
	}

    /*
     * For every ANDed expression, check if that expression is of the form
     * <distribution_col> = <expr>. If so return expr.
     */
    foreach(qual_cell, lquals)
    {
        Expr *qual_expr = (Expr *)lfirst(qual_cell);
        OpExpr *op;
        Expr *lexpr;
        Expr *rexpr;
        Var *var_expr;
        Expr *distcol_expr;
        bool isswap = false;

        /* iterate process nested and */
        if (and_clause((Node *) qual_expr))
        {
            lresult = pgxc_find_distcol_exprs(varno, attrNum, (Node*)(((BoolExpr *) qual_expr)->args));
            result = list_concat(result, lresult);
            continue;
        }

        if (!IsA(qual_expr, OpExpr))
		{
            continue;
		}
        op = (OpExpr *)qual_expr;
        /* If not a binary operator, it can not be '='. */
        if (list_length(op->args) != 2)
		{
            continue;
		}

        lexpr = linitial(op->args);
        rexpr = lsecond(op->args);

        /*
         * If either of the operands is a RelabelType, extract the Var in the RelabelType.
         * A RelabelType represents a "dummy" type coercion between two binary compatible datatypes.
         * If we do not handle these then our optimization does not work in case of varchar
         * For example if col is of type varchar and is the dist key then
         * select * from vc_tab where col = 'abcdefghijklmnopqrstuvwxyz';
         * should be shipped to one of the nodes only
         */
        if (IsA(lexpr, RelabelType))
		{
            lexpr = ((RelabelType*)lexpr)->arg;
		}
        if (IsA(rexpr, RelabelType))
		{
            rexpr = ((RelabelType*)rexpr)->arg;
		}

        /*
         * If either of the operands is a Var expression, assume the other
         * one is distribution column expression. If none is Var check next
         * qual.
         */
        if (IsA(lexpr, Var))
        {
            var_expr = (Var *)lexpr;
            distcol_expr = rexpr;
        }
        else if (IsA(rexpr, Var))
        {
            var_expr = (Var *)rexpr;
            distcol_expr = lexpr;
            isswap = true;
        }
        else
		{
            continue;
		}
        /*
         * If Var found is not the distribution column of required relation,
         * check next qual
         */
        if (var_expr->varno != varno || var_expr->varattno != attrNum)
		{
            continue;
		}
        /*
         * If the operator is not an assignment operator, check next
         * constraint. An operator is an assignment operator if it's
         * mergejoinable or hashjoinable. Beware that not every assignment
         * operator is mergejoinable or hashjoinable, so we might leave some
         * oportunity. But then we have to rely on the opname which may not
         * be something we know to be equality operator as well.
         */
        if (!op_mergejoinable(op->opno, exprType((Node *)lexpr)) &&
            !op_hashjoinable(op->opno, exprType((Node *)lexpr)))
        {
            if (!TimeStampRange(op->opno))
            {
                continue;
            }
        }
        /* Found the distribution column expression return it */
        pQual  = palloc0(sizeof(DisQual));
        pQual->opno = op->opno;
        pQual->expr = distcol_expr;
        pQual->isswap= isswap;
        if (!result)
        {
            result = lcons(pQual, result);
        }
        else
        {
            result = lappend(result, pQual);
        }
    }
    /* Exhausted all quals, but no distribution column expression */
    return result;
}

static ExecNodes *
GetRelationTimeStampRangeNodes(RelationLocInfo *rel_loc_info, 
                                Datum valueForDistCol, 
                                bool isValueNull, 
                                Oid typeOfValueForDistCol,                
                                Oid typeOfValueForSecDistCol,
                                Datum minForSecCol, 
                                Datum maxForSecCol,
                                bool  equalMin,
                                bool  equalMax,
                                RelationAccessType accessType)
{
    ExecNodes    *exec_nodes;

    if (rel_loc_info == NULL)
	{
        return NULL;
	}

    
    switch (rel_loc_info->locatorType)
    {
        case LOCATOR_TYPE_SHARD:
        {             
            exec_nodes = makeNode(ExecNodes);
            exec_nodes->baselocatortype = rel_loc_info->locatorType;
            exec_nodes->accesstype = accessType;

            exec_nodes->nodeList = GetShardMapRangeList(rel_loc_info->groupId, 
                                                        rel_loc_info->coldGroupId,     
                                                        rel_loc_info->relid,
                                                        typeOfValueForDistCol, 
                                                        valueForDistCol,
                                                        rel_loc_info->secAttrNum, 
                                                        typeOfValueForSecDistCol,  
                                                        minForSecCol, 
                                                        maxForSecCol,
                                                        equalMin,
                                                        equalMax,
                                                        accessType);
            return exec_nodes;
        }
        default:
            return NULL;
    }
}

List *
GetRelationGroupsByQuals(Oid reloid, RelationLocInfo *rel_loc_info, Node *sec_quals)
{// #lizard forgives
    int i = 0;
    bool isalign;
    int rangeOpCnt = 0;
    ListCell  *cell;
    Datum minStamp = 0;
    Datum maxStamp = 0;
    Datum constvalue;
    ListCell  *qual_cell;
    Datum seccol_value = 0;
    bool  seccol_isnull = false;
    Oid      seccol_type PG_USED_FOR_ASSERTS_ONLY = InvalidOid;
    Oid    *opArray = NULL;
    bool *isswapArray = NULL;
    List *seccol_value_list = NULL;
    int32 partitionStrategy    = 0;
    Relation rel;
    int32        interval_step            = 0;
    TimestampTz  start_timestamp          = 0;
    Form_pg_partition_interval routerinfo = NULL;
    List *seccol_list = (List *)sec_quals;
    int len = list_length(seccol_list);
    Oid        sectype = get_atttype(reloid, rel_loc_info->secAttrNum);
    int32    sectypmod = get_atttypmod(reloid, rel_loc_info->secAttrNum);
    
    opArray = palloc0(sizeof(Oid) * len);    
    isswapArray = palloc0(sizeof(bool) * len);
    i = 0;
    foreach(qual_cell, seccol_list)
    {
        Expr *expr = NULL;
        DisQual *pQual = (DisQual *)lfirst(qual_cell);
        opArray[i] = pQual->opno;
        isswapArray[i] = pQual->isswap;
        expr = (Expr *)pQual->expr;
        expr = (Expr *)coerce_to_target_type(NULL,
                                (Node *)expr,
                                exprType((Node *)expr),
                                sectype, sectypmod,
                                COERCION_ASSIGNMENT,
                                COERCE_IMPLICIT_CAST, -1);

        expr = (Expr *)eval_const_expressions(NULL,
                                (Node *)expr);

        seccol_value_list = lappend(seccol_value_list, (void*)expr);

        i++;
    }

    rel          = relation_open(reloid, NoLock);
    routerinfo     = rel->rd_partitions_info;
    if (routerinfo)
    {
        partitionStrategy = routerinfo->partinterval_type;

        if (partitionStrategy == IntervalType_Month &&
            routerinfo->partinterval_int == 12)
        {
            partitionStrategy = IntervalType_Year;
        }

        interval_step = routerinfo->partinterval_int;
        start_timestamp = routerinfo->partstartvalue_ts;
    }
    relation_close(rel, NoLock);

    i = 0;
    foreach(cell, seccol_value_list)
    {
        Expr *sec_distcol_expr = (Expr *)lfirst(cell);

        if (sec_distcol_expr && IsA(sec_distcol_expr, Const) && !TimeStampRange(opArray[i]))
        {
            Const *const_expr = (Const *)sec_distcol_expr;
            seccol_value     = const_expr->constvalue;
            seccol_isnull    = const_expr->constisnull;

            if (!seccol_isnull)
            {
                List *oids = NULL;
                if (IsHotData(seccol_value, RELATION_ACCESS_READ, partitionStrategy, interval_step, start_timestamp))
                {
                    oids = lappend_oid(oids, rel_loc_info->groupId);
                }
                else
                {
                    oids = lappend_oid(oids, rel_loc_info->coldGroupId);
                }

                return oids;
            }
        }
        else if (sec_distcol_expr && IsA(sec_distcol_expr, Const) && TimeStampRange(opArray[i]))
        {
            Const *const_expr = (Const *)sec_distcol_expr;

            if (2062 == opArray[i]) /* < */
            {
                isalign = IsConstAligned(reloid, const_expr->constvalue, rel_loc_info->secAttrNum);

                if (isswapArray[i])
                {
                    constvalue = const_expr->constvalue;
                    if (isalign)
                    {
                        /* const < var -> const + 1 <= var */
                        constvalue = constvalue + ONE_SECOND_DATUM;
                    }
                    minStamp = minStamp ? ((constvalue >= minStamp) ? minStamp : constvalue) : constvalue;
                    seccol_type = const_expr->consttype;
                }
                else
                {
                    constvalue = const_expr->constvalue;
                    if (isalign)
                    {
                        /* var < const -> var <= const -1 */
                        constvalue = constvalue - ONE_SECOND_DATUM;
                    }
                    
                    maxStamp = (constvalue >= maxStamp) ? constvalue : maxStamp;
                    seccol_type = const_expr->consttype;
                }
            }
            else if (2063 == opArray[i]) /* <= */
            {
                if (isswapArray[i])
                {
                    /* const <= var */
					minStamp = minStamp ? ((const_expr->constvalue >= minStamp) ? minStamp : const_expr->constvalue)
					                    : const_expr->constvalue;
                    seccol_type = const_expr->consttype;
                }
                else
                {
                    /* var <= const */
                    maxStamp = (const_expr->constvalue >= maxStamp) ? const_expr->constvalue : maxStamp;
                    seccol_type = const_expr->consttype;
                }
            }  
            else if (2065 == opArray[i]) /* >= */
            {     
                if (isswapArray[i])
                {
                    /* const >= var */
                    maxStamp = (const_expr->constvalue >= maxStamp) ? const_expr->constvalue : maxStamp;
                    seccol_type = const_expr->consttype;
                }
                else
                {
                    /* var >= const */
					minStamp = minStamp ? ((const_expr->constvalue >= minStamp) ? minStamp : const_expr->constvalue)
					                    : const_expr->constvalue;
                    seccol_type = const_expr->consttype;
                }
            }
            else if (2064 == opArray[i]) /* > */
            {
                isalign = IsConstAligned(reloid, const_expr->constvalue, rel_loc_info->secAttrNum);

                if (isswapArray[i])
                {
                    constvalue = const_expr->constvalue;
                    if (isalign)
                    {
                        /* const > var -> const -1 >= var */
                        constvalue = constvalue - ONE_SECOND_DATUM;
                    }
                    maxStamp = (constvalue >= maxStamp) ? constvalue : maxStamp;
                    seccol_type = const_expr->consttype;
                }
                else
                {
                    constvalue = const_expr->constvalue;
                    if (isalign)
                    {
                        /* var > const -> var  >= const + 1 */
                        constvalue = constvalue + ONE_SECOND_DATUM;
                    }
                    
                    minStamp = minStamp ? ((constvalue >= minStamp) ? minStamp : constvalue) : constvalue;
                    seccol_type = const_expr->consttype;
                }
            }
            else
            {
                seccol_type = InvalidOid;
            }
            /* skip range condition */
            rangeOpCnt++;
        }
        else
        {
            seccol_value      = (Datum) 0;
            seccol_isnull    = true;
        }

        i++;
    }

    if (opArray)
    {
        pfree(opArray);
        opArray = NULL;
    }

    if (isswapArray)
    {
        pfree(isswapArray);
        isswapArray = NULL;
    }
    
    if (rangeOpCnt == list_length(seccol_value_list))
    {
        List *oids = NULL;
        if (minStamp && maxStamp)
        {            
			if (IsHotData(minStamp, RELATION_ACCESS_READ, partitionStrategy, interval_step, start_timestamp) &&
			    IsHotData(maxStamp, RELATION_ACCESS_READ, partitionStrategy, interval_step, start_timestamp))
            {    /* all hot data */
                oids = lappend_oid(oids, rel_loc_info->groupId);                                
            }
			else if (!IsHotData(minStamp, RELATION_ACCESS_READ, partitionStrategy, interval_step, start_timestamp) &&
			         !IsHotData(maxStamp, RELATION_ACCESS_READ, partitionStrategy, interval_step, start_timestamp))
            {
                /* all cold data */
                oids = lappend_oid(oids, rel_loc_info->coldGroupId);
            }
			else if (!IsHotData(minStamp, RELATION_ACCESS_READ, partitionStrategy, interval_step, start_timestamp) &&
			         IsHotData(maxStamp, RELATION_ACCESS_READ, partitionStrategy, interval_step, start_timestamp))
            {
                /* range across cold and hot group */
                oids = lappend_oid(oids, rel_loc_info->groupId);
                oids = lappend_oid(oids, rel_loc_info->coldGroupId);                
            }
        }
        else if (minStamp)
        {
            /* timestamp >=, all query in hot node */
            if (IsHotData(minStamp, RELATION_ACCESS_READ, partitionStrategy, interval_step, start_timestamp))
            {
                oids = lappend_oid(oids, rel_loc_info->groupId);
            }
            else
            {
                oids = lappend_oid(oids, rel_loc_info->groupId);
                oids = lappend_oid(oids, rel_loc_info->coldGroupId);    
            }
        }
        else if (maxStamp)
        {
            /* <= timestamp, both hot and cold need query */
            if (IsHotData(maxStamp, RELATION_ACCESS_READ, partitionStrategy, interval_step, start_timestamp))
            {
                oids = lappend_oid(oids, rel_loc_info->groupId);
                oids = lappend_oid(oids, rel_loc_info->coldGroupId);
            }
            else
            {
                /* only cold group */
                oids = lappend_oid(oids, rel_loc_info->coldGroupId);
            }
        }
        else
        {
            oids = lappend_oid(oids, rel_loc_info->groupId);
            oids = lappend_oid(oids, rel_loc_info->coldGroupId);
        }

        return oids;
    }
    else
    {
        List *oids = NULL;
        oids = lappend_oid(oids, rel_loc_info->groupId);
        oids = lappend_oid(oids, rel_loc_info->coldGroupId);
        return oids;
    }
}

#endif

#ifdef _MLS_
extern char* g_default_locator_type;

char get_default_locator_type(void)
{
    if (strlen(g_default_locator_type) == 0)
    {
#ifdef ENABLE_ALL_TABLE_TYPE
        return LOCATOR_TYPE_HASH;
#else
        return LOCATOR_TYPE_SHARD;
#endif
    }

    if (strcmp(g_default_locator_type, "shard") == 0)
    {
        return LOCATOR_TYPE_SHARD;
    }
    else if (strcmp(g_default_locator_type, "hash") == 0)
    {
        return LOCATOR_TYPE_HASH;
    }
    else if (strcmp(g_default_locator_type, "replication") == 0)
    {
        return LOCATOR_TYPE_REPLICATED; 
    }

    elog(ERROR, "unknown locator type:%s", g_default_locator_type);
    /* keep compiler slience */
    return LOCATOR_TYPE_HASH;

}

int get_default_distype(void)
{
    if (strlen(g_default_locator_type) == 0)
    {
#ifdef ENABLE_ALL_TABLE_TYPE
        return DISTTYPE_HASH;
#else
        return DISTTYPE_SHARD;
#endif
    }

    if (strcmp(g_default_locator_type, "shard") == 0)
    {
        return DISTTYPE_SHARD;
    }
    else if (strcmp(g_default_locator_type, "hash") == 0)
    {
        return DISTTYPE_HASH;
    }
    else if (strcmp(g_default_locator_type, "replication") == 0)
    {
        return DISTTYPE_REPLICATION; 
    }

    elog(ERROR, "unknown locator type:%s", g_default_locator_type);
    /* keep compiler slience */
    return DISTTYPE_HASH;
}

#endif



