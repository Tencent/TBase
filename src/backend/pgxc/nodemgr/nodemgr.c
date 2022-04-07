/*-------------------------------------------------------------------------
 *
 * nodemgr.c
 *      Routines to support manipulation of the pgxc_node catalog
 *      Support concerns CREATE/ALTER/DROP on NODE object.
 *
 * Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "access/hash.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/pgxc_node.h"
#include "commands/defrem.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/tqual.h"
#include "pgxc/locator.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#ifdef _MIGRATE_
#include "utils/builtins.h"
#include "pgxc/groupmgr.h"
#include "catalog/pgxc_shard_map.h"
#include "utils/fmgroids.h"
#include "catalog/pgxc_class.h"
#include "access/xact.h"
#endif

#ifdef __TBASE__
#include "access/xact.h"
#include "libpq/libpq.h"
#endif
bool enable_multi_cluster = true;
bool enable_multi_cluster_print = false;

/*
 * How many times should we try to find a unique indetifier
 * in case hash of the node name comes out to be duplicate
 */

#define MAX_TRIES_FOR_NID    200

static Datum generate_node_id(const char *node_name);
static void count_coords_datanodes(Relation rel, int *num_coord, int *num_dns);
#ifdef __TBASE__
static void  PgxcCheckNodeValid(char *name, char *clustername, char type);
static void  ValidateCreateGtmNode(void);
static void  ValidateAlterGtmNode(void);
#endif

/*
 * GUC parameters.
 * Shared memory block can not be resized dynamically, so we should have some
 * limits set at startup time to calculate amount of shared memory to store
 * node table. Nodes can be added to running cluster until that limit is reached
 * if cluster needs grow beyond the configuration value should be changed and
 * cluster restarted.
 */
/* Global number of nodes. Point to a shared memory block */
static int       *shmemNumCoords;
static int       *shmemNumDataNodes;
static int       *shmemNumSlaveDataNodes;

/* Shared memory tables of node definitions */
NodeDefinition *coDefs;
NodeDefinition *dnDefs;
NodeDefinition *sdnDefs;

#ifdef __TBASE__
char *PGXCNodeHost;
static char *g_TbasePlane = NULL;
#endif

/* HashTable key: nodeoid  value: position of coDefs/dnDefs */
static HTAB *g_NodeDefHashTab = NULL;

typedef struct
{
    Oid            nodeoid;
} NodeDefLookupTag;

typedef struct
{    
    NodeDefLookupTag  tag;
    int32                 nodeDefIndex;    /* Associated into  coDefs or dnDefs index */
} NodeDefLookupEnt;



/*
 * NodeTablesInit
 *    Initializes shared memory tables of Coordinators and Datanodes.
 */
void
NodeTablesShmemInit(void)
{// #lizard forgives
    bool found;
    int i;

    /*
     * Initialize the table of Coordinators: first sizeof(int) bytes are to
     * store actual number of Coordinators, remaining data in the structure is
     * array of NodeDefinition that can contain up to TBASE_MAX_COORDINATOR_NUMBER entries.
     * That is a bit weird and probably it would be better have these in
     * separate structures, but I am unsure about cost of having shmem structure
     * containing just single integer.
     */
    shmemNumCoords = ShmemInitStruct("Coordinator Table",
                                sizeof(int) +
                                    sizeof(NodeDefinition) * TBASE_MAX_COORDINATOR_NUMBER,
                                &found);

    /* Have coDefs pointing right behind shmemNumCoords */
    coDefs = (NodeDefinition *) (shmemNumCoords + 1);

    /* Mark it empty upon creation */
    if (!found)
    {
        *shmemNumCoords = 0;
        /* Mark nodeishealthy true at init time for all */
        for (i = 0; i < TBASE_MAX_COORDINATOR_NUMBER; i++)
            coDefs[i].nodeishealthy = true;
    }

    /* Same for Datanodes */
    shmemNumDataNodes = ShmemInitStruct("Datanode Table",
                                   sizeof(int) +
                                       sizeof(NodeDefinition) * TBASE_MAX_DATANODE_NUMBER,
                                   &found);

    /* Have dnDefs pointing right behind shmemNumDataNodes */
    dnDefs = (NodeDefinition *) (shmemNumDataNodes + 1);

    /* Mark it empty upon creation */
    if (!found)
    {
        *shmemNumDataNodes = 0;
        /* Mark nodeishealthy true at init time for all */
        for (i = 0; i < TBASE_MAX_DATANODE_NUMBER; i++)
            dnDefs[i].nodeishealthy = true;
    }

    
    /* Same for Datanodes */
    shmemNumSlaveDataNodes = ShmemInitStruct("Slave Datanode Table",
                                   sizeof(int) +
                                       sizeof(NodeDefinition) * TBASE_MAX_DATANODE_NUMBER,
                                   &found);

    /* Have dnDefs pointing right behind shmemNumSlaveDataNodes */
    sdnDefs = (NodeDefinition *) (shmemNumSlaveDataNodes + 1);

    /* Mark it empty upon creation */
    if (!found)
    {
        *shmemNumSlaveDataNodes = 0;
        /* Mark nodeishealthy true at init time for all */
        for (i = 0; i < TBASE_MAX_DATANODE_NUMBER; i++)
            sdnDefs[i].nodeishealthy = true;
    }

#ifdef __TBASE__
    PGXCNodeHost = (char *)ShmemInitStruct("PGXC Node Host",
                                            NAMEDATALEN,
                                           &found);
    if (!found)
    {
        PGXCNodeHost[0] = '\0';
    }

	g_TbasePlane = (char *)ShmemInitStruct("TBase Plane",
											NAMEDATALEN,
								            &found);
	if (!found)
	{
		g_TbasePlane[0] = '\0';
	}
    
    NodeDefHashTabShmemInit();
#endif
    
}


#ifdef __TBASE__
void
NodeDefHashTabShmemInit(void)
{
    HASHCTL        info;
    
    /* Init hash table for nodeoid to dnDefs/coDefs lookup */
    info.keysize   = sizeof(NodeDefLookupTag);
    info.entrysize = sizeof(NodeDefLookupEnt);
    info.hash        = tag_hash;
    g_NodeDefHashTab = ShmemInitHash("NodeDef info look up",
                                  TBASE_MAX_COORDINATOR_NUMBER + 2 * TBASE_MAX_DATANODE_NUMBER, 
                                  TBASE_MAX_COORDINATOR_NUMBER + 2 * TBASE_MAX_DATANODE_NUMBER,
                                  &info,
                                  HASH_ELEM | HASH_FUNCTION | HASH_FIXED_SIZE);    

    if (!g_NodeDefHashTab)
    {
        elog(FATAL, "invalid shmem status when creating node def hash ");
    }
}

#endif

/*
 * NodeHashTableShmemSize
 *    Get the size of Node Definition hash table
 */
Size 
NodeHashTableShmemSize(void)
{
    Size size;

    /* hash table, here just double the element size, in case of memory corruption */
    size = mul_size((TBASE_MAX_DATANODE_NUMBER+TBASE_MAX_COORDINATOR_NUMBER) * 2 , MAXALIGN64(sizeof(NodeDefLookupEnt)));

    return size;
}


/*
 * NodeTablesShmemSize
 *    Get the size of shared memory dedicated to node definitions
 */
Size
NodeTablesShmemSize(void)
{
    Size co_size;
    Size dn_size;
    Size dn_slave_size;
    Size total_size;

    co_size = mul_size(sizeof(NodeDefinition), TBASE_MAX_COORDINATOR_NUMBER);
    co_size = add_size(co_size, sizeof(int));
    dn_size = mul_size(sizeof(NodeDefinition), TBASE_MAX_DATANODE_NUMBER);
    dn_size = add_size(dn_size, sizeof(int));
    dn_slave_size = mul_size(sizeof(NodeDefinition), TBASE_MAX_DATANODE_NUMBER);
    dn_slave_size = add_size(dn_slave_size, sizeof(int));

    total_size = add_size(co_size, dn_size);
    total_size = add_size(total_size, dn_slave_size);
    total_size = add_size(total_size, NAMEDATALEN);
	total_size = add_size(total_size, NAMEDATALEN);
    return total_size;
}

/*
 * Check list of options and return things filled.
 * This includes check on option values.
 */
static void
check_node_options(const char *node_name, List *options, char **node_host,
            int *node_port, char *node_type,
            bool *is_primary, bool *is_preferred
#ifdef __TBASE__
            ,char **node_group,
            char **node_cluster_name,
            bool *alter
#endif
            )
{// #lizard forgives
    ListCell   *option;
#ifdef __TBASE__    
    bool        set_node_primary_option = false;
    bool        set_node_host_option = false;
    bool        set_node_port_option = false;
#endif

    if (!options)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("No options specified")));

    /* Filter options */
    foreach(option, options)
    {
        DefElem    *defel = (DefElem *) lfirst(option);

        if (strcmp(defel->defname, "port") == 0)
        {
            *node_port = defGetTypeLength(defel);
#ifdef __TBASE__            
            set_node_host_option = true;
#endif
            if (*node_port < 1 || *node_port > 65535)
                ereport(ERROR,
                        (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                         errmsg("port value is out of range")));
        }
        else if (strcmp(defel->defname, "host") == 0)
        {
            *node_host = defGetString(defel);
#ifdef __TBASE__            
            set_node_port_option = true;
#endif
        }
        else if (strcmp(defel->defname, "type") == 0)
        {
            char *type_loc;

            type_loc = defGetString(defel);

            if (strcmp(type_loc, "coordinator") != 0 &&
                strcmp(type_loc, "datanode") != 0     &&
                strcmp(type_loc, "gtm") != 0)
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("type value is incorrect, specify 'coordinator or 'datanode' or 'gtm'")));

            if (strcmp(type_loc, "coordinator") == 0)
                *node_type = PGXC_NODE_COORDINATOR;
            else if (strcmp(type_loc, "datanode") == 0)
                *node_type = PGXC_NODE_DATANODE;
            else if (strcmp(type_loc, "gtm") == 0)
                *node_type = PGXC_NODE_GTM;
        }
        else if (strcmp(defel->defname, "primary") == 0)
        {
            *is_primary = defGetBoolean(defel);
#ifdef __TBASE__            
            set_node_primary_option = true;
#endif
        }
        else if (strcmp(defel->defname, "preferred") == 0)
        {
            *is_preferred = defGetBoolean(defel);
        }
#ifdef __TBASE__
        else if (strcmp(defel->defname, "group") == 0)
        {
            *node_group = defGetString(defel);
        }
        else if (strcmp(defel->defname, "cluster") == 0)
        {
            *node_cluster_name = defGetString(defel);
        }
        else if (strcmp(defel->defname, "alter") == 0)
        {
            *alter = defGetBoolean(defel);
        }
#endif
        else
        {
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("incorrect option: %s", defel->defname)));
        }
    }

    /* A primary node has to be a Datanode or Gtm */
    if (*is_primary && *node_type != PGXC_NODE_DATANODE && *node_type != PGXC_NODE_GTM)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("PGXC node %s: cannot be a primary node, it has to be a Datanode or GTM",
                        node_name)));

    /* A preferred node has to be a Datanode */
    if (*is_preferred && *node_type != PGXC_NODE_DATANODE)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("PGXC node %s: cannot be a preferred node, it has to be a Datanode",
                        node_name)));

    /* Node type check */
    if (*node_type == PGXC_NODE_NONE)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("PGXC node %s: Node type not specified",
                        node_name)));

#ifdef XCP
    if (*node_type == PGXC_NODE_DATANODE && NumDataNodes >= TBASE_MAX_DATANODE_NUMBER)
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("Too many datanodes, current value of max_datanodes is %d",
                        TBASE_MAX_DATANODE_NUMBER)));

#endif

#ifdef __TBASE__
    if (PGXC_NODE_GTM == *node_type)
    {
        /* Currently only accept master gtm, so set to  */
        if (set_node_primary_option && (false == *is_primary))
        {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("PGXC node %s: only master gtm accepted",
                        node_name)));
        }

        /* set gtm as primary by default */
        if (!set_node_primary_option)
        {
            *is_primary = true;
        }
        
        if (NULL == *node_host || 0 == *node_port || !set_node_host_option || !set_node_port_option)
        {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("PGXC node %s: host, port should be specified",
                        node_name)));
        }
        if (NULL != *node_group)
        {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("PGXC node %s: gtm should not specify group",
                        node_name)));
        }
    }
#endif

}

static void
get_node_cluster_name(List *options, char **node_cluster_name)
{
    ListCell   *option;

    if (!options)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("No options specified")));

    /* Filter options */
    foreach(option, options)
    {
        DefElem    *defel = (DefElem *) lfirst(option);
        
        if (strcmp(defel->defname, "cluster") == 0)
        {
            *node_cluster_name = defGetString(defel);
        }
    }
}


/*
 * generate_node_id
 *
 * Given a node name compute its hash to generate the identifier
 * If the hash comes out to be duplicate , try some other values
 * Give up after a few tries
 */
static Datum
generate_node_id(const char *node_name)
{
    Datum        node_id;
    uint32        n;
    bool        inc;
    int        i;

    /* Compute node identifier by computing hash of node name */
    node_id = hash_any((unsigned char *)node_name, strlen(node_name));

    /*
     * Check if the hash is near the overflow limit, then we will
     * decrement it , otherwise we will increment
     */
    inc = true;
    n = DatumGetUInt32(node_id);
    if (n >= UINT_MAX - MAX_TRIES_FOR_NID)
        inc = false;

    /*
     * Check if the identifier is clashing with an existing one,
     * and if it is try some other
     */
    for (i = 0; i < MAX_TRIES_FOR_NID; i++)
    {
        HeapTuple    tup;

        tup = SearchSysCache1(PGXCNODEIDENTIFIER, node_id);
        if (tup == NULL)
            break;

        ReleaseSysCache(tup);

        n = DatumGetUInt32(node_id);
        if (inc)
            n++;
        else
            n--;

        node_id = UInt32GetDatum(n);
    }

    /*
     * This has really few chances to happen, but inform backend that node
     * has not been registered correctly in this case.
     */
    if (i >= MAX_TRIES_FOR_NID)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                 errmsg("Please choose different node name."),
                 errdetail("Name \"%s\" produces a duplicate identifier node_name",
                           node_name)));

    return node_id;
}

/* --------------------------------
 *  cmp_nodes
 *
 *  Compare the Oids of two XC nodes
 *  to sort them in ascending order by their names
 * --------------------------------
 */
static int
cmp_nodes(const void *p1, const void *p2)
{
    Oid n1 = *((Oid *)p1);
    Oid n2 = *((Oid *)p2);

    if (strcmp(get_pgxc_nodename(n1), get_pgxc_nodename(n2)) < 0)
        return -1;

    if (strcmp(get_pgxc_nodename(n1), get_pgxc_nodename(n2)) == 0)
        return 0;

    return 1;
}

/*
 * Count the number of coordinators and datanodes configured so far.
 */
static void
count_coords_datanodes(Relation rel, int *num_coord, int *num_dns)
{
    int            coordCount = 0, dnCount = 0;
    HeapScanDesc scan;
    HeapTuple   tuple;

    scan = heap_beginscan(rel, SnapshotSelf, 0, NULL);
    while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
    {
        Form_pgxc_node  nodeForm = (Form_pgxc_node) GETSTRUCT(tuple);

        /* Take definition for given node type */
        switch (nodeForm->node_type)
        {
            case PGXC_NODE_COORDINATOR:
                coordCount++;
                break;
            case PGXC_NODE_DATANODE:
                dnCount++;
                break;
            default:
                break;
        }
    }
    heap_endscan(scan);

    *num_coord = coordCount;
    *num_dns = dnCount;
}

/*
 * Whether node changes happened
 */
bool
PrimaryNodeNumberChanged(void)
{
	return (*shmemNumCoords + *shmemNumDataNodes != NumCoords + NumDataNodes);
}

/*
 * PgxcNodeListAndCount
 *
 * Update node definitions in the shared memory tables from the catalog
 */
static void
PgxcNodeListAndCount(void)
{// #lizard forgives
    Relation rel;
    HeapScanDesc scan;
    HeapTuple   tuple;
    NodeDefinition *nodes = NULL;
    int    numNodes;
#ifdef __TBASE__    
    int loop = 0;
    NodeDefLookupTag tag;
    NodeDefLookupEnt *ent;
    bool found;
#endif    

    LWLockAcquire(NodeTableLock, LW_EXCLUSIVE);

    numNodes = *shmemNumCoords + *shmemNumDataNodes + *shmemNumSlaveDataNodes;

    Assert((*shmemNumCoords >= 0) && (*shmemNumDataNodes >= 0) && (*shmemNumSlaveDataNodes >= 0));

    /*
     * Save the existing health status values because nodes
     * might get added or deleted here. We will save
     * nodeoid, status. No need to differentiate between
     * coords and datanodes since oids will be unique anyways
     */
    if (numNodes > 0)
    {
        nodes = (NodeDefinition*)palloc(numNodes * sizeof(NodeDefinition));

        /* XXX It's possible to call memcpy with */
        if (*shmemNumCoords > 0)
            memcpy(nodes, coDefs, *shmemNumCoords * sizeof(NodeDefinition));

        if (*shmemNumDataNodes > 0)
            memcpy(nodes + *shmemNumCoords, dnDefs,
                   *shmemNumDataNodes * sizeof(NodeDefinition));

        if (*shmemNumSlaveDataNodes > 0)
            memcpy(nodes + *shmemNumCoords + *shmemNumDataNodes, sdnDefs,
               *shmemNumSlaveDataNodes * sizeof(NodeDefinition));
    }

    *shmemNumCoords = 0;
    *shmemNumDataNodes = 0;
    *shmemNumSlaveDataNodes = 0;

    /*
     * Node information initialization is made in one scan:
     * 1) Scan pgxc_node catalog to find the number of nodes for
     *    each node type and make proper allocations
     * 2) Then extract the node Oid
     * 3) Complete primary/preferred node information
     */
    rel = heap_open(PgxcNodeRelationId, AccessShareLock);
    scan = heap_beginscan(rel, SnapshotSelf, 0, NULL);
    while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
    {
        Form_pgxc_node  nodeForm = (Form_pgxc_node) GETSTRUCT(tuple);
        NodeDefinition *node = NULL;
        int i;

#ifdef __TBASE__
        if(enable_multi_cluster && strcmp(NameStr(nodeForm->node_cluster_name), PGXCClusterName))
            continue;
#endif
        if (PGXC_NODE_GTM == nodeForm->node_type)
            continue;
        
        /* Take definition for given node type */
        switch (nodeForm->node_type)
        {
            case PGXC_NODE_COORDINATOR:
                node = &coDefs[(*shmemNumCoords)++];
                break;
            case PGXC_NODE_DATANODE:
                node = &dnDefs[(*shmemNumDataNodes)++];
                break;
            case PGXC_NODE_SLAVEDATANODE:
            default:
                /*
                 * compile warning for node uninitialized, we hope nodetype were in PGXC_NODE enum.
                 */
                node = &sdnDefs[(*shmemNumSlaveDataNodes)++];
                break;
        }

#ifdef __TBASE__
        if (PGXCNodeHost[0] == '\0' && IsUnderPostmaster)
        {
            char *ip = get_local_address(MyProcPort);

            if (ip && strcmp(ip, "localhost") && strcmp(ip, "127.0.0.1"))
            {
                snprintf(PGXCNodeHost, NAMEDATALEN, "%s", ip);
            }
        }
#endif

        /* Populate the definition */
        node->nodeoid = HeapTupleGetOid(tuple);
        memcpy(&node->nodename, &nodeForm->node_name, NAMEDATALEN);
        memcpy(&node->nodehost, &nodeForm->node_host, NAMEDATALEN);
        node->nodeport = nodeForm->node_port;
        node->nodeisprimary = nodeForm->nodeis_primary;
        node->nodeispreferred = nodeForm->nodeis_preferred;
        if(enable_multi_cluster_print)
            elog(LOG, "nodename %s nodehost %s nodeport %d Oid %d", 
                    node->nodename.data, node->nodehost.data, node->nodeport, node->nodeoid);
        /*
         * Copy over the health status from above for nodes that
         * existed before and after the refresh. If we do not find
         * entry for a nodeoid, we mark it as healthy
         */
        node->nodeishealthy = true;
        for (i = 0; i < numNodes; i++)
        {
            if (nodes[i].nodeoid == node->nodeoid)
            {
                node->nodeishealthy = nodes[i].nodeishealthy;
                break;
            }
        }
    }
    heap_endscan(scan);
    heap_close(rel, AccessShareLock);

    elog(DEBUG1, "Done pgxc_nodes scan: %d coordinators and %d datanodes and %d slavedatanodes",
            *shmemNumCoords, *shmemNumDataNodes, *shmemNumSlaveDataNodes);

    if (numNodes)
        pfree(nodes);

    /* Finally sort the lists */
    if (*shmemNumCoords > 1)
        qsort(coDefs, *shmemNumCoords, sizeof(NodeDefinition), cmp_nodes);
    if (*shmemNumDataNodes > 1)
        qsort(dnDefs, *shmemNumDataNodes, sizeof(NodeDefinition), cmp_nodes);
    
    if (*shmemNumSlaveDataNodes > 1)
        qsort(sdnDefs, *shmemNumSlaveDataNodes, sizeof(NodeDefinition), cmp_nodes);

#ifdef __TBASE__
	/* set plane type */
	if (g_TbasePlane[0] == '\0')
	{
		if (PGXCClusterName && strlen(PGXCClusterName) > 0)
		{
			snprintf(g_TbasePlane, NAMEDATALEN, "%s", PGXCClusterName);
		}
	}
	else
	{
		bool plane_changed = false;

		if (PGXCClusterName && strcmp(g_TbasePlane, PGXCClusterName))
		{
			plane_changed = true;
		}

		/* reset hashtab */
		if (plane_changed)
		{
			HASH_SEQ_STATUS scan_status;
			NodeDefLookupEnt  *item;

			hash_seq_init(&scan_status, g_NodeDefHashTab);
			while ((item = (NodeDefLookupEnt *) hash_seq_search(&scan_status)) != NULL)
			{
				if (hash_search(g_NodeDefHashTab, (const void *) &item->tag,
								HASH_REMOVE, NULL) == NULL)
				{
					LWLockRelease(NodeTableLock);
					elog(ERROR, "node definition hash table corrupted");
				}
			}

			snprintf(g_TbasePlane, NAMEDATALEN, "%s", PGXCClusterName);
		}
	}
    /* Add to hash table */
    for (loop = 0; loop < *shmemNumCoords; loop++)
    {
        tag.nodeoid = coDefs[loop].nodeoid;
        ent = (NodeDefLookupEnt*)hash_search(g_NodeDefHashTab, (void *) &tag, HASH_ENTER, &found);
        if (!ent)
        {
            LWLockRelease(NodeTableLock);
            elog(ERROR, "corrupted node definition hash table");
        }
        ent->nodeDefIndex = loop;
    }

    for (loop = 0; loop < *shmemNumDataNodes; loop++)
    {
        tag.nodeoid = dnDefs[loop].nodeoid;
        ent = (NodeDefLookupEnt*)hash_search(g_NodeDefHashTab, (void *) &tag, HASH_ENTER, &found);
        if (!ent)
        {
            LWLockRelease(NodeTableLock);
            elog(ERROR, "corrupted node definition hash table");
        }
        ent->nodeDefIndex = loop;
    }

    
    for (loop = 0; loop < *shmemNumSlaveDataNodes; loop++)
    {
        tag.nodeoid = sdnDefs[loop].nodeoid;
        ent = (NodeDefLookupEnt*)hash_search(g_NodeDefHashTab, (void *) &tag, HASH_ENTER, &found);
        if (!ent)
        {
            LWLockRelease(NodeTableLock);
            elog(ERROR, "corrupted node definition hash table");
        }
        ent->nodeDefIndex = loop;
    }
#endif

    LWLockRelease(NodeTableLock);
}

/*
 * PgxcNodeListAndCountWrapTransaction
 *
 * Update node definitions in the shared memory tables from the catalog wrap the transaction
 */
void
PgxcNodeListAndCountWrapTransaction(void)
{
	bool need_abort = false;
	
	if (!IsTransactionOrTransactionBlock())
	{
		StartTransactionCommand();
		need_abort = true;
	}

	PgxcNodeListAndCount();
	
	if (need_abort)
	{
		AbortCurrentTransaction();
	}
}

/*
 * PgxcNodeGetIds
 *
 * List into palloc'ed arrays Oids of Coordinators and Datanodes currently
 * presented in the node table, as well as number of Coordinators and Datanodes.
 * Any parameter may be NULL if caller is not interested in receiving
 * appropriate results. Preferred and primary node information can be updated
 * in session if requested.
 */
void
PgxcNodeGetOidsExtend(Oid **coOids, Oid **dnOids, Oid **sdnOids,
                int *num_coords, int *num_dns, int *num_sdns, bool update_preferred)
{// #lizard forgives
    LWLockAcquire(NodeTableLock, LW_SHARED);

    elog(DEBUG1, "Get OIDs from table: %d coordinators and %d datanodes",
            *shmemNumCoords, *shmemNumDataNodes);

    if (num_coords)
        *num_coords = *shmemNumCoords;
    if (num_dns)
        *num_dns = *shmemNumDataNodes;
    if (num_sdns)
        *num_sdns = *shmemNumSlaveDataNodes;

    if (coOids)
    {
        int i;

        *coOids = (Oid *) palloc(*shmemNumCoords * sizeof(Oid));
        for (i = 0; i < *shmemNumCoords; i++){
            (*coOids)[i] = coDefs[i].nodeoid;
            elog(DEBUG1, "i %d coOid %d",i, (*coOids)[i]); 
        }
    }

    if (dnOids)
    {
        int i;

        *dnOids = (Oid *) palloc(*shmemNumDataNodes * sizeof(Oid));
        for (i = 0; i < *shmemNumDataNodes; i++)
        {
            (*dnOids)[i] = dnDefs[i].nodeoid;
            elog(DEBUG1, "i %d dnOid %d",i, (*dnOids)[i]); 
        }
    }

    
    if (sdnOids)
    {
        int i;

        *sdnOids = (Oid *) palloc(*shmemNumSlaveDataNodes * sizeof(Oid));
        for (i = 0; i < *shmemNumSlaveDataNodes; i++)
        {
            (*sdnOids)[i] = sdnDefs[i].nodeoid;
            elog(DEBUG1, "i %d sdnOid %d",i, (*sdnOids)[i]); 
        }
    }

    /* Update also preferred and primary node informations if requested */
    if (update_preferred)
    {
        int i;

        /* Initialize primary and preferred node information */
        primary_data_node = InvalidOid;
        num_preferred_data_nodes = 0;

        for (i = 0; i < *shmemNumDataNodes; i++)
        {
            if (dnDefs[i].nodeisprimary)
                primary_data_node = dnDefs[i].nodeoid;

            if (dnDefs[i].nodeispreferred && num_preferred_data_nodes < MAX_PREFERRED_NODES)
            {
                preferred_data_node[num_preferred_data_nodes] = dnDefs[i].nodeoid;
                num_preferred_data_nodes++;
            }
        }
    }

    LWLockRelease(NodeTableLock);
}

/*
 * PgxcNodeGetHealthMap
 *
 * List into palloc'ed arrays Oids of Coordinators and Datanodes currently
 * presented in the node table, as well as number of Coordinators and Datanodes.
 * Any parameter may be NULL if caller is not interested in receiving
 * appropriate results for either the Coordinators or Datanodes.
 */
void
PgxcNodeGetHealthMapExtend(Oid *coOids, Oid *dnOids, Oid *sdnOids,
                int *num_coords, int *num_dns, int *num_sdns, bool *coHealthMap,
                bool *dnHealthMap, bool *sdnHealthMap)
{// #lizard forgives
    elog(DEBUG1, "Get HealthMap from table: %d coordinators and %d datanodes",
            *shmemNumCoords, *shmemNumDataNodes);

    LWLockAcquire(NodeTableLock, LW_SHARED);

    if (num_coords)
        *num_coords = *shmemNumCoords;
    if (num_dns)
        *num_dns = *shmemNumDataNodes;
    if (num_sdns)
        *num_sdns = *shmemNumSlaveDataNodes;

    if (coOids)
    {
        int i;
        for (i = 0; i < *shmemNumCoords; i++)
        {
            coOids[i] = coDefs[i].nodeoid;
            if (coHealthMap)
                coHealthMap[i] = coDefs[i].nodeishealthy;
        }
    }

    if (dnOids)
    {
        int i;

        for (i = 0; i < *shmemNumDataNodes; i++)
        {
            dnOids[i] = dnDefs[i].nodeoid;
            if (dnHealthMap)
                dnHealthMap[i] = dnDefs[i].nodeishealthy;
        }
    }

    
    if (sdnOids)
    {
        int i;

        for (i = 0; i < *shmemNumSlaveDataNodes; i++)
        {
            sdnOids[i] = sdnDefs[i].nodeoid;
            if (sdnHealthMap)
                sdnHealthMap[i] = sdnDefs[i].nodeishealthy;
        }
    }

    LWLockRelease(NodeTableLock);
}

/*
 * Consult the shared memory NodeDefinition structures and
 * fetch the nodeishealthy value and return it back
 *
 * We will probably need a similar function for coordinators
 * in the future..
 */
void
PgxcNodeDnListHealth(List *nodeList, bool *healthmap)
{
    ListCell *lc;
    int index = 0;

    elog(DEBUG1, "Get healthmap from datanodeList");

    if (!nodeList || !list_length(nodeList))
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("NIL or empty nodeList passed")));

    LWLockAcquire(NodeTableLock, LW_SHARED);
    foreach(lc, nodeList)
    {
        int node = lfirst_int(lc);

        if (node >= *shmemNumDataNodes)
        {
            LWLockRelease(NodeTableLock);
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                     errmsg("PGXC health status not found for datanode with oid (%d)",
                         node)));
        }
        healthmap[index++] = dnDefs[node].nodeishealthy;
    }
    LWLockRelease(NodeTableLock);
}

/*
 * Find node definition in the shared memory node table.
 * The structure is a copy palloc'ed in current memory context.
 */
NodeDefinition *
PgxcNodeGetDefinition(Oid node)
{
    NodeDefinition *result = NULL;
    bool             found;
    NodeDefLookupTag tag;
    NodeDefLookupEnt *ent;
    
    tag.nodeoid =   node;

    LWLockAcquire(NodeTableLock, LW_SHARED);

    ent = (NodeDefLookupEnt*)hash_search(g_NodeDefHashTab, (void *) &tag, HASH_FIND, &found);    
    if(found)
    {
        if (dnDefs[ent->nodeDefIndex].nodeoid == node)
        {
            result = (NodeDefinition *) palloc(sizeof(NodeDefinition));

            memcpy(result, dnDefs + ent->nodeDefIndex, sizeof(NodeDefinition));

            LWLockRelease(NodeTableLock);

            return result;
        }
        else if (coDefs[ent->nodeDefIndex].nodeoid == node)
        {
            result = (NodeDefinition *) palloc(sizeof(NodeDefinition));

            memcpy(result, coDefs + ent->nodeDefIndex, sizeof(NodeDefinition));

            LWLockRelease(NodeTableLock);

            return result;
        }
        else if (sdnDefs[ent->nodeDefIndex].nodeoid == node)
        {
            result = (NodeDefinition *) palloc(sizeof(NodeDefinition));

            memcpy(result, sdnDefs + ent->nodeDefIndex, sizeof(NodeDefinition));

            LWLockRelease(NodeTableLock);

            return result;
        }
            
    }

    /* not found, return NULL */
    LWLockRelease(NodeTableLock);
    return NULL;
}

/*
 * Update health status of a node in the shared memory node table.
 *
 * We could try to optimize this by checking if the ishealthy value
 * is already the same as the passed in one.. but if the cluster is
 * impaired, dunno how much such optimizations are worth. So keeping
 * it simple for now
 */
bool
PgxcNodeUpdateHealth(Oid node, bool status)
{
    bool             found;
    NodeDefLookupTag tag;
    NodeDefLookupEnt *ent;
    
    tag.nodeoid =   node;
    
    LWLockAcquire(NodeTableLock, LW_EXCLUSIVE);

    ent = (NodeDefLookupEnt*)hash_search(g_NodeDefHashTab, (void *) &tag, HASH_FIND, &found);    
    if(found)
    {
        if (dnDefs[ent->nodeDefIndex].nodeoid == node)
        {
            dnDefs[ent->nodeDefIndex].nodeishealthy = status;

            LWLockRelease(NodeTableLock);

            return true;
        }
        else if (coDefs[ent->nodeDefIndex].nodeoid == node)
        {
            coDefs[ent->nodeDefIndex].nodeishealthy = status;

            LWLockRelease(NodeTableLock);

            return true;
        }
        else if (sdnDefs[ent->nodeDefIndex].nodeoid == node)
        {
            sdnDefs[ent->nodeDefIndex].nodeishealthy = status;

            LWLockRelease(NodeTableLock);

            return true;
        }
            
    }
    
    /* not found, return false */
    LWLockRelease(NodeTableLock);
    return false;
}

/*
 * PgxcNodeCreate
 *
 * Add a PGXC node
 */
void
PgxcNodeCreate(CreateNodeStmt *stmt)
{// #lizard forgives
    Relation    pgxcnodesrel;
    HeapTuple    htup;
    bool        nulls[Natts_pgxc_node];
    Datum        values[Natts_pgxc_node];
    const char *node_name = stmt->node_name;
    int        i;
    /* Options with default values */
    char       *node_host = NULL;
    char        node_type = PGXC_NODE_NONE;
    int            node_port = 0;
    bool        is_primary = false;
    bool        is_preferred = false;
    Datum        node_id;
    int            coordCount = 0;
    int            dnCount = 0;
#ifdef _MIGRATE_
    Oid         nodeOid;
#endif
#ifdef __TBASE__
    char           *node_group = NULL;
    Oid            groupoid = InvalidOid;
    char        *node_cluster_name = NULL;
    bool        alter = false;
#endif
    

    /* Only a DB administrator can add nodes */
    if (!superuser())
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("must be superuser to create cluster nodes")));

#ifdef __TBASE__
    /* 
     * Support create node format like this: 
     * create gtm node gtm1 with (host='xxx.xxx.xxx.xxx',port=2999);
     * type='gtm' can be ignored.
     */
    if (PGXC_NODE_GTM == stmt->node_type)
    {
        node_type = PGXC_NODE_GTM;
    }
#endif

    /* Filter options */
    check_node_options(node_name, stmt->options, &node_host,
                &node_port, &node_type,
                &is_primary, &is_preferred 
#ifdef __TBASE__
                ,&node_group
                ,&node_cluster_name
                ,&alter
#endif                
                );
    
    if(!node_cluster_name)
    {
        node_cluster_name = strdup(PGXCDefaultClusterName);
        elog(DEBUG1, "PGXC node %s: Applying default cluster value: %s",
             node_name, node_cluster_name);

    }

#ifdef __TBASE__
    if (alter)
    {
        if(node_group)
        {
            groupoid = get_pgxc_groupoid(node_group);
            if(!OidIsValid(groupoid))
            {
                elog(ERROR, "node group [%s] is not exist", node_group);
            }
        }

        if (node_name)
        {
            nodeOid = get_pgxc_nodeoid(node_name);
            if (!OidIsValid(nodeOid))
            {
                elog(ERROR, "node [%s] is not exist", node_name);
            }
        }

        //if (OidIsValid(groupoid))
        {
            PgxcClassModifyData data;

            data.group = groupoid;
            data.node  = nodeOid;
            
            ModifyPgxcClass(PGXC_CLASS_ADD_NODE, &data);
        }

        return;
    }
#endif

    /* Check that node name is node in use */
    if (OidIsValid(get_pgxc_nodeoid_extend(node_name, node_cluster_name)))
        ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_OBJECT),
                 errmsg("PGXC Node %s Cluster %s: object already defined",
                        node_name,
                        node_cluster_name)));

    /* Check length of node name */
    if (strlen(node_name) > PGXC_NODENAME_LENGTH)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                 errmsg("Node name \"%s\" is too long",
                        node_name)));
#ifdef __TBASE__
    PgxcCheckNodeValid((char*)node_name, (char*)node_cluster_name, node_type);
#endif

#ifdef __TBASE__
    /* check if group is exist */
    if(node_group)
    {
        groupoid = get_pgxc_groupoid(node_group);
        if(!OidIsValid(groupoid))
        {
            elog(ERROR,"node group [%s] is not exist", node_group);
        }
    }
#endif
    
    /* Compute node identifier */
    node_id = generate_node_id(node_name);

    /*
     * Check that this node is not created as a primary if one already
     * exists.
     */
    if (is_primary && OidIsValid(primary_data_node) && node_type == PGXC_NODE_DATANODE)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("PGXC node %s: two nodes cannot be primary",
                        node_name)));

#ifdef __TBASE__
    /*
     * Check gtm only has one primary node
     */
    if (PGXC_NODE_GTM == node_type)
    {
        if (!is_primary)
        {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("PGXC node %s: only master gtm accepted", node_name)));
        }
        
        ValidateCreateGtmNode();
    }
#endif

    /*
     * Then assign default values if necessary
     * First for port.
     */
    if (node_port == 0)
    {
        node_port = 5432;
        elog(DEBUG1, "PGXC node %s: Applying default port value: %d",
             node_name, node_port);
    }

    /* Then apply default value for host */
    if (!node_host)
    {
        node_host = strdup("localhost");
        elog(DEBUG1, "PGXC node %s: Applying default host value: %s",
             node_name, node_host);
    }


    /* Iterate through all attributes initializing nulls and values */
    for (i = 0; i < Natts_pgxc_node; i++)
    {
        nulls[i]  = false;
        values[i] = (Datum) 0;
    }

    /*
     * Open the relation for insertion
     * This is necessary to generate a unique Oid for the new node
     * There could be a relation race here if a similar Oid
     * being created before the heap is inserted.
     */
    pgxcnodesrel = heap_open(PgxcNodeRelationId, AccessExclusiveLock);

    /*
     * Get the count of datanodes and coordinators added so far and make sure
     * we're not exceeding the configured limits
     *
     * XXX This is not full proof because someone may first set
     * max_coordinators or max_datanodes to a high value, add nodes and then
     * lower the value again.
     */
    count_coords_datanodes(pgxcnodesrel, &coordCount, &dnCount);

    if ((node_type == PGXC_NODE_DATANODE && dnCount >= TBASE_MAX_DATANODE_NUMBER) ||
        (node_type == PGXC_NODE_COORDINATOR && coordCount >= TBASE_MAX_COORDINATOR_NUMBER))
    {
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                 errmsg("cannot add more than %d %s",
                     node_type == PGXC_NODE_COORDINATOR ?
                     TBASE_MAX_COORDINATOR_NUMBER : TBASE_MAX_DATANODE_NUMBER,
                     node_type == PGXC_NODE_COORDINATOR ?
                     "coordinators" : "datanodes"),
                 errhint("increase the value of %s GUC and restart the cluster",
                     node_type == PGXC_NODE_COORDINATOR ?
                     "max_coordinators" : "max_datanodes"
                     )));

    }

    /* Build entry tuple */
    
    values[Anum_pgxc_node_name - 1] = DirectFunctionCall1(namein, CStringGetDatum(node_name));
    values[Anum_pgxc_node_type - 1] = CharGetDatum(node_type);
    values[Anum_pgxc_node_port - 1] = Int32GetDatum(node_port);
    values[Anum_pgxc_node_host - 1] = DirectFunctionCall1(namein, CStringGetDatum(node_host));
    values[Anum_pgxc_node_is_primary - 1] = BoolGetDatum(is_primary);
    values[Anum_pgxc_node_is_preferred - 1] = BoolGetDatum(is_preferred);
    values[Anum_pgxc_node_id - 1] = node_id;
    values[Anum_pgxc_node_cluster_name - 1] = DirectFunctionCall1(namein, CStringGetDatum(node_cluster_name));
    

    htup = heap_form_tuple(pgxcnodesrel->rd_att, values, nulls);
    
    nodeOid = CatalogTupleInsert(pgxcnodesrel, htup);
    
    heap_close(pgxcnodesrel, AccessExclusiveLock);

#ifdef _MIGRATE_
    /* add this node to group*/
    if(OidIsValid(groupoid))
        AddNodeToGroup(nodeOid, groupoid);

    /* add this node to all shard tables in group */
    if (OidIsValid(groupoid))
    {
        PgxcClassModifyData data;

        data.group = groupoid;
        data.node  = nodeOid;
        
        ModifyPgxcClass(PGXC_CLASS_ADD_NODE, &data);
    }

    if (is_primary && node_type == PGXC_NODE_DATANODE)
        primary_data_node = nodeOid;
#endif
}

/*
 * PgxcNodeAlter
 *
 * Alter a PGXC node
 */
void
PgxcNodeAlter(AlterNodeStmt *stmt)
{// #lizard forgives
    const char *node_name = stmt->node_name;
    char       *node_host = NULL;
    char        node_type = PGXC_NODE_NONE;
    int            node_port = 0;
    bool        is_preferred = false;
    bool        is_primary = false;
    HeapTuple    oldtup, newtup;
    Oid            nodeOid;
    Relation    rel;
    Datum        new_record[Natts_pgxc_node];
    bool        new_record_nulls[Natts_pgxc_node];
    bool        new_record_repl[Natts_pgxc_node];
    uint32        node_id;
    int            coordCount = 0;
    int            dnCount = 0;
#ifdef __TBASE__
    char        *node_group = NULL;
    char         *node_cluster = NULL;
    Oid            groupoid    = InvalidOid;
    bool        alter = false;
#endif


    /* Only a DB administrator can alter cluster nodes */
    if (!superuser())
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("must be superuser to change cluster nodes")));


    /* Filter options */
    get_node_cluster_name(stmt->options, &node_cluster);
    if(NULL != node_cluster)
    {
        elog(ERROR, "CLUSTER could not be modified.");
    }

    if (NULL == stmt->cluster_name)
    {
        node_cluster = strdup(PGXCMainClusterName);
        elog(DEBUG1, "PGXC node %s: Applying default cluster value: %s",
             node_name, node_cluster);
    }
    else
    {
        node_cluster = stmt->cluster_name;
    }
    
    nodeOid = get_pgxc_nodeoid_extend(node_name, node_cluster);
    /* Check that node exists */
    if (!OidIsValid(nodeOid))
    {
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                     errmsg("PGXC Node %s: object not defined",
                            node_name)));
    }
    
    /* Open new tuple, checks are performed on it and new values */
    oldtup = SearchSysCacheCopy1(PGXCNODEOID, ObjectIdGetDatum(nodeOid));
    if (!HeapTupleIsValid(oldtup))
        elog(ERROR, "cache lookup failed for object %u", nodeOid);


    /*
     * check_options performs some internal checks on option values
     * so set up values.
     */
    node_host = get_pgxc_nodehost(nodeOid);
    node_port = get_pgxc_nodeport(nodeOid);
    is_preferred = is_pgxc_nodepreferred(nodeOid);
    is_primary = is_pgxc_nodeprimary(nodeOid);
    node_type = get_pgxc_nodetype(nodeOid);
    node_id = get_pgxc_node_id(nodeOid);

    /* Filter options */
    check_node_options(node_name, stmt->options, &node_host,
                &node_port, &node_type,
                &is_primary, &is_preferred
#ifdef __TBASE__
                , &node_group,
                &node_cluster,
                &alter
#endif
                );

    stmt->node_type = node_type;
    
    /*
     * Two nodes cannot be primary at the same time. If the primary
     * node is this node itself, well there is no point in having an
     * error.
     */
    if (is_primary &&
        OidIsValid(primary_data_node) &&
        nodeOid != primary_data_node  &&
        PGXC_NODE_DATANODE == node_type)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("PGXC node %s: two nodes cannot be primary",
                        node_name)));

#ifdef __TBASE__
    /*
     * Check only one gtm node can be primary
     */
    if (PGXC_NODE_GTM == node_type)
    {
        if (!is_primary)
        {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("PGXC node %s: only master gtm accepted", node_name)));
        }

        ValidateAlterGtmNode();
    }
#endif

    /* Look at the node tuple, and take exclusive lock on it */
    rel = heap_open(PgxcNodeRelationId, AccessExclusiveLock);

    /*
     * Get the count of datanodes and coordinators added so far and make sure
     * we're not exceeding the configured limits
     */
    count_coords_datanodes(rel, &coordCount, &dnCount);

    if ((node_type == PGXC_NODE_DATANODE && dnCount >= TBASE_MAX_DATANODE_NUMBER) ||
        (node_type == PGXC_NODE_COORDINATOR && coordCount >= TBASE_MAX_COORDINATOR_NUMBER))
    {
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                 errmsg("cannot add more than %d %s",
                     node_type == PGXC_NODE_COORDINATOR ?
                     TBASE_MAX_COORDINATOR_NUMBER : TBASE_MAX_DATANODE_NUMBER,
                     node_type == PGXC_NODE_COORDINATOR ?
                     "coordinators" : "datanodes"),
                 errhint("increase the value of %s GUC and restart the cluster",
                     node_type == PGXC_NODE_COORDINATOR ?
                     "max_coordinators" : "max_datanodes"
                     )));

    }

    /* Update values for catalog entry */
    MemSet(new_record, 0, sizeof(new_record));
    MemSet(new_record_nulls, false, sizeof(new_record_nulls));
    MemSet(new_record_repl, false, sizeof(new_record_repl));
    new_record[Anum_pgxc_node_port - 1] = Int32GetDatum(node_port);
    new_record_repl[Anum_pgxc_node_port - 1] = true;
    new_record[Anum_pgxc_node_host - 1] =
        DirectFunctionCall1(namein, CStringGetDatum(node_host));
    new_record_repl[Anum_pgxc_node_host - 1] = true;
    new_record[Anum_pgxc_node_type - 1] = CharGetDatum(node_type);
    new_record_repl[Anum_pgxc_node_type - 1] = true;
    new_record[Anum_pgxc_node_is_primary - 1] = BoolGetDatum(is_primary);
    new_record_repl[Anum_pgxc_node_is_primary - 1] = true;
    new_record[Anum_pgxc_node_is_preferred - 1] = BoolGetDatum(is_preferred);
    new_record_repl[Anum_pgxc_node_is_preferred - 1] = true;
    new_record[Anum_pgxc_node_id - 1] = UInt32GetDatum(node_id);
    new_record_repl[Anum_pgxc_node_id - 1] = true;
    new_record[Anum_pgxc_node_cluster_name - 1] = 
        DirectFunctionCall1(namein, CStringGetDatum(node_cluster));
    new_record_repl[Anum_pgxc_node_cluster_name - 1] = true;
    

    /* Update relation */
    newtup = heap_modify_tuple(oldtup, RelationGetDescr(rel),
                               new_record,
                               new_record_nulls, new_record_repl);
    CatalogTupleUpdate(rel, &oldtup->t_self, newtup);

    /* Release lock at Commit */
    heap_close(rel, AccessExclusiveLock);


    /* 
     * 1. add node to group_members in pgxc_group 
     * 2. add node to pgxc_class
     */
    if (isRestoreMode)
    {
        if (!node_group)
            return ;
    
        /* check node does not belong to any group */
        if (InvalidOid != GetGroupOidByNode(nodeOid))
        {
            elog(ERROR,"node %s already in group %s, can not move to group %s", 
                        node_name,
                        GetGroupNameByNode(nodeOid),
                        node_group);
        }
    
        if(node_group)
        {
            groupoid = get_pgxc_groupoid(node_group);
            if(!OidIsValid(groupoid))
            {
                elog(ERROR,"node group [%s] is not exist", node_group);
            }
        }
        
        /* add this node to pgxc_group*/
        /* add this node to all shard tables in group */
        if(OidIsValid(groupoid))
        {
            PgxcClassModifyData data;
            data.group = groupoid;
            data.node  = nodeOid;

            AddNodeToGroup(nodeOid, groupoid);

            ModifyPgxcClass(PGXC_CLASS_ADD_NODE, &data);
        }
    }
}


/*
 * PgxcNodeRemove
 *
 * Remove a PGXC node
 */
void
PgxcNodeRemove(DropNodeStmt *stmt)
{// #lizard forgives
    Relation    relation;
    HeapTuple    tup;
    const char    *node_name = stmt->node_name;
    Oid        noid;
#ifdef _MIGRATE_
    bool         is_primary;
    ScanKeyData skey;
    SysScanDesc scan;
    Oid groupoid;
#endif

#ifdef __TBASE__
    char        node_type = PGXC_NODE_NONE;
    Form_pgxc_node    nodeForm;
    char       *node_cluster;
    char           *node_group = NULL;
    char        *node_cluster_name = NULL;
    bool        alter = false;
    bool        is_preferred = false;
    char       *node_host = NULL;
    int            node_port = 0;
#endif

    /* Only a DB administrator can remove cluster nodes */
    if (!superuser())
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("must be superuser to remove cluster nodes")));

    if (NULL == stmt->cluster_name)
    {
        node_cluster = strdup(PGXCMainClusterName);
        elog(DEBUG1, "PGXC node %s: Applying default cluster value: %s",
             node_name, node_cluster);
    }
    else
    {
        node_cluster = stmt->cluster_name;
    }

#ifdef __TBASE__
    /* Filter options */
    if (stmt->options)
    {
        is_primary = false;
        is_preferred = false;
        node_type = PGXC_NODE_DATANODE;
        check_node_options(node_name, stmt->options, &node_host,
                            &node_port, &node_type,
                            &is_primary, &is_preferred 
                            ,&node_group
                            ,&node_cluster_name
                            ,&alter            
                            );
    }

    if (alter)
    {
        if (strcmp(node_name, PGXCNodeName) == 0 && strcmp(node_cluster, PGXCMainClusterName) == 0)
        {
            return;
        }
        
        noid = get_pgxc_nodeoid_extend(node_name, node_cluster);

        if (OidIsValid(noid))
        {
            groupoid = GetGroupOidByNode(noid);

            //if (OidIsValid(groupoid))
            {
                PgxcClassModifyData data;

                data.group = groupoid;
                data.node  = noid;
                
                ModifyPgxcClass(PGXC_CLASS_DROP_NODE, &data);
            }
        }

        return;
    }
    
#endif

    noid = get_pgxc_nodeoid_extend(node_name, node_cluster);
    /* Check if node is defined */
    if (!OidIsValid(noid))
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("PGXC Node %s: object not defined",
                        node_name)));

    if (strcmp(node_name, PGXCNodeName) == 0 && strcmp(node_cluster, PGXCMainClusterName) == 0)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("PGXC Node %s: cannot drop local node",
                        node_name)));

    /* Check if node has any shard */
    relation = heap_open(PgxcShardMapRelationId, AccessShareLock);
    ScanKeyInit(&skey,
                Anum_pgxc_shard_map_primarycopy,
                BTEqualStrategyNumber, 
                F_OIDEQ,
                ObjectIdGetDatum(noid));
    
    scan    = systable_beginscan(relation,
                                  PgxcShardMapGroupIndexId, 
                                  true,
                                  NULL, 
                                  1, 
                                  &skey);                              

    tup  = systable_getnext(scan);

    if (HeapTupleIsValid(tup))
    {
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("shard info left on node"),
                 errhint("maybe you should drop shard first")));
    }

    systable_endscan(scan);
    heap_close(relation, AccessShareLock);


    /* PGXCTODO:
     * Is there any group which has this node as member
     * XC Tables will also have this as a member in their array
     * Do this search in the local data structure.
     * If a node is removed, it is necessary to check if there is a distributed
     * table on it. If there are only replicated table it is OK.
     * However, we have to be sure that there are no pooler agents in the cluster pointing to it.
     */

    /* Delete the pgxc_node tuple */
    relation = heap_open(PgxcNodeRelationId, RowExclusiveLock);
    tup = SearchSysCache1(PGXCNODEOID, ObjectIdGetDatum(noid));
    if (!HeapTupleIsValid(tup)) /* should not happen */
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("PGXC Node %s: object not defined",
                        node_name)));
#ifdef __TBASE__    
    nodeForm = (Form_pgxc_node) GETSTRUCT(tup);
    node_type = nodeForm->node_type;
#endif

    simple_heap_delete(relation, &tup->t_self);

    ReleaseSysCache(tup);

#ifdef _MIGRATE_
    if (PGXC_NODE_GTM != node_type)
    {
        groupoid = RemoveNodeFromGroup(noid);

        /* remove this node from all shard tables in group */
        if (OidIsValid(groupoid))
        {
            PgxcClassModifyData data;

            data.group = groupoid;
            data.node  = noid;
            
            ModifyPgxcClass(PGXC_CLASS_DROP_NODE, &data);
        }

        is_primary = is_pgxc_nodeprimary(noid);
        if (is_primary)
            primary_data_node = InvalidOid;
    }
#endif

    heap_close(relation, RowExclusiveLock);
}

#ifdef __TBASE__
void PgxcCheckNodeValid(char *name, char *clustername, char type)
{// #lizard forgives
    bool         valid  = true;
    int32        rownum = 0;
    Relation      rel;
    HeapScanDesc scan;
    HeapTuple    tuple;

    elog(DEBUG1, "check node cluster name %s name %s localname %s", clustername, name, PGXCNodeName);

    if (isRestoreMode)
        return;

    if (PGXC_NODE_DATANODE == type)
    {
        LWLockAcquire(NodeTableLock, LW_SHARED);
        
        rel = heap_open(PgxcNodeRelationId, AccessShareLock);
        scan = heap_beginscan(rel, SnapshotSelf, 0, NULL);
        while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
        {
            Form_pgxc_node  nodeForm = (Form_pgxc_node) GETSTRUCT(tuple);
            if(strcmp(clustername, NameStr(nodeForm->node_cluster_name)) == 0)
            {
                if (strcmp( NameStr(nodeForm->node_name), PGXCNodeName) 
                        && strncmp(name, NameStr(nodeForm->node_name), NAMEDATALEN) <= 0 
                        && (PGXC_NODE_GTM != nodeForm->node_type))
                {
                    valid = false;
                    ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                     errmsg("PGXC Node name %s cluster %s must be defined in ascii order existing node name %s cluster %s", 
                                 name, clustername,  NameStr(nodeForm->node_name), NameStr(nodeForm->node_cluster_name))));
                    break;
                }    
            }
            rownum++;
            
        }
        heap_endscan(scan);
        heap_close(rel, AccessShareLock);
        
        LWLockRelease(NodeTableLock);
        if (!valid)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                     errmsg("PGXC Node name %s must be defined in ascii order", 
                                 name)));
        }

        if (rownum > MAX_NODES_NUMBER)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                     errmsg("PGXC Node too many nodes already, limit is %d", MAX_NODES_NUMBER)));
        }
    }
}
#endif


#ifdef __TBASE__
/* When create master gtm node, supposed to be no gtm info in pgxc_node. If found, report ERROR. */
static void 
ValidateCreateGtmNode(void)
{
    Relation    rel;
    HeapScanDesc scan;
    HeapTuple    gtmtup;
    Form_pgxc_node    nodeForm;
    bool         found = false;

    rel = heap_open(PgxcNodeRelationId, AccessShareLock);
    scan = heap_beginscan_catalog(rel, 0, NULL);

    while (HeapTupleIsValid(gtmtup = heap_getnext(scan, ForwardScanDirection)))
    {
        nodeForm = (Form_pgxc_node) GETSTRUCT(gtmtup);
        if (PGXC_NODE_GTM == nodeForm->node_type)
        {
            found = true;
            break;
        }
    }

    heap_endscan(scan);
    heap_close(rel, AccessShareLock);
    
    if (found)
    {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
             errmsg("Master gtm node already exists")));
    }
}

/* When alter master gtm node, supposed to be exactly one gtm info entry in pgxc_node. If not, report ERROR. */
static void 
ValidateAlterGtmNode(void)
{
    Relation    rel;
    HeapScanDesc scan;
    HeapTuple    tup;
    int            tuple_cnt = 0;
    Form_pgxc_node    nodeForm;
    
    rel = heap_open(PgxcNodeRelationId, AccessShareLock);
    scan = heap_beginscan_catalog(rel, 0, NULL);

    while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
    {
        nodeForm = (Form_pgxc_node) GETSTRUCT(tup);
        if (PGXC_NODE_GTM == nodeForm->node_type)
        {
            tuple_cnt++;
        }
    }

    heap_endscan(scan);
    heap_close(rel, AccessShareLock);

    /* In alter gtm node command, only one gtm tuple will be found */
    if (1 != tuple_cnt)
    {
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("%d gtm nodes are found, shoule be only one", tuple_cnt)));
    }
}
#endif
