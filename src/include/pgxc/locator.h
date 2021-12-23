/*-------------------------------------------------------------------------
 *
 * locator.h
 *		Externally declared locator functions
 *
 *
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/locator.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOCATOR_H
#define LOCATOR_H

#include "fmgr.h"
#define LOCATOR_TYPE_REPLICATED 'R'
#define LOCATOR_TYPE_HASH 'H'
#define LOCATOR_TYPE_RANGE 'G'
#define LOCATOR_TYPE_SINGLE 'S'
#define LOCATOR_TYPE_RROBIN 'N'
#define LOCATOR_TYPE_CUSTOM 'C'
#define LOCATOR_TYPE_MODULO 'M'
#define LOCATOR_TYPE_NONE 'O'
#define LOCATOR_TYPE_DISTRIBUTED 'D'	/* for distributed table without specific
										 * scheme, e.g. result of JOIN of
										 * replicated and distributed table */

#ifdef _MIGRATE_
#define LOCATOR_TYPE_SHARD 'S'
#endif


/* Maximum number of preferred Datanodes that can be defined in cluster */
#define MAX_PREFERRED_NODES 64

#define HASH_SIZE 4096
#define HASH_MASK 0x00000FFF;

#define IsLocatorNone(x) (x == LOCATOR_TYPE_NONE)
#define IsLocatorReplicated(x) (x == LOCATOR_TYPE_REPLICATED)
#define IsLocatorColumnDistributed(x) (x == LOCATOR_TYPE_HASH || \
									   x == LOCATOR_TYPE_RROBIN || \
									   x == LOCATOR_TYPE_MODULO || \
									   x == LOCATOR_TYPE_DISTRIBUTED || \
									   x == LOCATOR_TYPE_SHARD)
#define IsLocatorDistributedByValue(x) (x == LOCATOR_TYPE_HASH || \
										x == LOCATOR_TYPE_MODULO || \
										x == LOCATOR_TYPE_RANGE || \
									    x == LOCATOR_TYPE_SHARD)

#include "nodes/primnodes.h"
#include "utils/relcache.h"

typedef int PartAttrNumber;

/*
 * How relation is accessed in the query
 */
typedef enum
{
	RELATION_ACCESS_READ,				/* SELECT */
	RELATION_ACCESS_READ_FQS,				/* SELECT for FQS */
	RELATION_ACCESS_READ_FOR_UPDATE,	/* SELECT FOR UPDATE */
	RELATION_ACCESS_UPDATE,				/* UPDATE OR DELETE */
	RELATION_ACCESS_INSERT				/* INSERT */
} RelationAccessType;

typedef struct
{
	Oid		relid;
	char		locatorType;
	PartAttrNumber	partAttrNum;	/* if partitioned */
	char		*partAttrName;		/* if partitioned */
#ifdef _MIGRATE_
	Oid         groupId;			/* distribute group */
#endif
#ifdef __COLD_HOT__
	/* used for table in cold-hot group */
	Oid         coldGroupId;        /* cold group oid if exist */
	AttrNumber  secAttrNum;         /* second distributed column's attribute number */
	char       *secAttrName;        /* second distributed column's name */
#endif
	List		*rl_nodeList;		/* Node Indices */
	ListCell	*roundRobinNode;	/* index of the next one to use */
} RelationLocInfo;

#define IsRelationReplicated(rel_loc)			IsLocatorReplicated((rel_loc)->locatorType)
#define IsRelationColumnDistributed(rel_loc) 	IsLocatorColumnDistributed((rel_loc)->locatorType)
#define IsRelationDistributedByValue(rel_loc)	IsLocatorDistributedByValue((rel_loc)->locatorType)
/*
 * Nodes to execute on
 * primarynodelist is for replicated table writes, where to execute first.
 * If it succeeds, only then should it be executed on nodelist.
 * primarynodelist should be set to NULL if not doing replicated write operations
 */
typedef struct
{
	NodeTag		type;
	List		*primarynodelist;
	List		*nodeList;
	char		baselocatortype;
	Expr		*en_expr;		/* expression to evaluate at execution time if planner
						 	 	 * can not determine execution nodes */
#ifdef __COLD_HOT__
	Expr		*sec_en_expr;	/* Sec Expression to evaluate at execution time
								 * if planner can not determine execution
								 * nodes */
#endif
	Oid			en_relid;			/* Relation to determine execution nodes */
	RelationAccessType accesstype;	/* Access type to determine execution nodes */
#ifdef __TBASE__
	bool    	restrict_shippable; /* The ExecNode is choose by join qual on distribute column */
	bool		const_subquery; 	/* The subquery rte only got constant values */
#endif
	bool		need_rewrite;		/* exists func, need to be rewritted when execute plan */
	Datum		rewrite_value;	/* function evaluate result */
	bool		isnull;
	bool		rewrite_done;		/* function rewritted */
	bool        missing_ok;
} ExecNodes;


#define IsExecNodesReplicated(en) IsLocatorReplicated((en)->baselocatortype)
#define IsExecNodesColumnDistributed(en) IsLocatorColumnDistributed((en)->baselocatortype)
#define IsExecNodesDistributedByValue(en) IsLocatorDistributedByValue((en)->baselocatortype)

typedef enum
{
	LOCATOR_LIST_NONE,	/* locator returns integers in range 0..NodeCount-1,
						 * value of nodeList ignored and can be NULL */
	LOCATOR_LIST_INT,	/* nodeList is an integer array (int *), value from
						 * the array is returned */
	LOCATOR_LIST_OID,	/* node list is an array of Oids (Oid *), value from
						 * the array is returned */
	LOCATOR_LIST_POINTER,	/* node list is an array of pointers (void **),
							 * value from the array is returned */
	LOCATOR_LIST_LIST,	/* node list is a list, item type is determined by
						 * list type (integer, oid or pointer). NodeCount
						 * is ignored */
} LocatorListType;

typedef Datum (*LocatorHashFunc) (PG_FUNCTION_ARGS);

typedef struct _Locator Locator;


/*
 * Creates a structure holding necessary info to effectively determine nodes
 * where a tuple should be stored.
 * Locator does not allocate memory while working, all allocations are made at
 * the creation time.
 *
 * Parameters:
 *
 *  locatorType - see LOCATOR_TYPE_* constants
 *  accessType - see RelationAccessType enum
 *  dataType - actual data type of values provided to determine nodes
 *  listType - defines how nodeList parameter is interpreted, see
 *			   LocatorListType enum for more details
 *  nodeCount - number of nodes to distribute
 *	nodeList - detailed info about relation nodes. Either List or array or NULL
 *	result - returned address of the array where locator will output node
 * 			 references. Type of array items (int, Oid or pointer (void *))
 * 			 depends on listType.
 *	primary - set to true if caller ever wants to determine primary node.
 *            Primary node will be returned as the first element of the
 *			  result array
 */
#ifdef _MIGRATE_
extern Locator *createLocator(char locatorType, RelationAccessType accessType,
			  Oid dataType, LocatorListType listType, int nodeCount,
			  void *nodeList, void **result, bool primary, Oid groupid,
			  Oid coldGroupId, Oid secDataType, AttrNumber secAttrNum,
			  Oid relid);
#else
extern Locator *createLocator(char locatorType, RelationAccessType accessType,
			  Oid dataType, LocatorListType listType, int nodeCount,
			  void *nodeList, void **result, bool primary);
#endif

extern void freeLocator(Locator *locator);

extern int GET_NODES(Locator *self, Datum value, bool isnull,
#ifdef __COLD_HOT__
					       Datum secValue, bool secIsNull,
#endif
	                       bool *hasprimary);
extern void *getLocatorResults(Locator *self);
extern void *getLocatorNodeMap(Locator *self);
extern int getLocatorNodeCount(Locator *self);

/* Extern variables related to locations */
extern Oid primary_data_node;
extern Oid preferred_data_node[MAX_PREFERRED_NODES];
extern int num_preferred_data_nodes;

extern void InitRelationLocInfo(void);
extern char GetLocatorType(Oid relid);
extern char ConvertToLocatorType(int disttype);

extern char *GetRelationHashColumn(RelationLocInfo *rel_loc_info);
extern RelationLocInfo *GetRelationLocInfo(Oid relid);
extern RelationLocInfo *CopyRelationLocInfo(RelationLocInfo *src_info);
extern char GetRelationLocType(Oid relid);
extern bool IsTableDistOnPrimary(RelationLocInfo *rel_loc_info);
extern bool IsLocatorInfoEqual(RelationLocInfo *rel_loc_info1, RelationLocInfo *rel_loc_info2);
extern int	GetRoundRobinNode(Oid relid);
extern ExecNodes *GetRelationNodes(RelationLocInfo *rel_loc_info, Datum valueForDistCol,
										bool isValueNull,
#ifdef __COLD_HOT__
										Datum valueForSecDistCol, bool isSecValueNull,
#endif
										RelationAccessType accessType);
extern ExecNodes *GetRelationNodesForExplain(RelationLocInfo *rel_loc_info,
											 RelationAccessType accessType);
extern ExecNodes *GetRelationNodesByQuals(Oid reloid,
										  RelationLocInfo *rel_loc_info,
										  Index varno,
										  Node *quals,
										  RelationAccessType relaccess,
										  Node **dis_qual,
										  Node **sec_quals);

extern bool IsTypeHashDistributable(Oid col_type);
extern List *GetAllDataNodes(void);
extern List *GetAllCoordNodes(void);
extern List *GetEntireCoordNodes(void);
extern int GetAnyDataNode(Bitmapset *nodes);
extern void RelationBuildLocator(Relation rel);
extern void FreeRelationLocInfo(RelationLocInfo *relationLocInfo);

extern bool IsTypeModuloDistributable(Oid col_type);
extern char *GetRelationModuloColumn(RelationLocInfo *rel_loc_info);
extern char *GetRelationDistColumn(RelationLocInfo *rel_loc_info);
extern bool IsDistColumnForRelId(Oid relid, char *part_col_name);
extern void FreeExecNodes(ExecNodes **exec_nodes);
extern List *GetPreferredReplicationNode(List *relNodes);
extern char *GetRelationDistribColumn(RelationLocInfo *locInfo);
#ifdef __COLD_HOT__
extern char *GetRelationSecDistribColumn(RelationLocInfo *locInfo);
extern List *GetRelationGroupsByQuals(Oid reloid, RelationLocInfo *rel_loc_info, Node *sec_quals);
#endif
extern LocatorHashFunc hash_func_ptr(Oid dataType);

#ifdef _MIGRATE_
extern bool IsTypeDistributable(Oid col_type);
#endif

#ifdef __TBASE__
extern char getLocatorDisType(Locator *self);
extern bool prefer_olap;
extern bool IsDistributedColumn(AttrNumber attr, RelationLocInfo *relation_loc_info);
extern int calcDistReplications(char distributionType, Bitmapset *nodes);
#endif

#ifdef _MLS_
extern char get_default_locator_type(void);
extern int get_default_distype(void);
#endif

#endif   /* LOCATOR_H */
