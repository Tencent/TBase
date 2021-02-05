/*-------------------------------------------------------------------------
 *
 * rel.h
 *	  POSTGRES relation descriptor (a/k/a relcache entry) definitions.
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/utils/rel.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef REL_H
#define REL_H

#include "access/tupdesc.h"
#include "access/xlog.h"
#include "access/transam.h"
#include "catalog/pg_class.h"
#include "catalog/pg_index.h"
#include "catalog/pg_publication.h"
#include "fmgr.h"
#include "nodes/bitmapset.h"
#include "pgxc/locator.h"
#include "rewrite/prs2lock.h"
#include "storage/block.h"
#include "storage/relfilenode.h"
#include "utils/relcache.h"
#include "utils/reltrigger.h"
#ifdef __TBASE__
#include "catalog/pg_partition_interval.h"
#include "nodes/parsenodes.h"
#endif
/*
 * LockRelId and LockInfo really belong to lmgr.h, but it's more convenient
 * to declare them here so we can have a LockInfoData field in a Relation.
 */

typedef struct LockRelId
{
	Oid			relId;			/* a relation identifier */
	Oid			dbId;			/* a database identifier */
} LockRelId;

typedef struct LockInfoData
{
	LockRelId	lockRelId;
} LockInfoData;

typedef LockInfoData *LockInfo;

/*
 * Information about the partition key of a relation
 */
typedef struct PartitionKeyData
{
	char		strategy;		/* partitioning strategy */
	int16		partnatts;		/* number of columns in the partition key */
	AttrNumber *partattrs;		/* attribute numbers of columns in the
								 * partition key */
	List	   *partexprs;		/* list of expressions in the partitioning
								 * key, or NIL */

	Oid		   *partopfamily;	/* OIDs of operator families */
	Oid		   *partopcintype;	/* OIDs of opclass declared input data types */
	FmgrInfo   *partsupfunc;	/* lookup info for support funcs */

	/* Partitioning collation per attribute */
	Oid		   *partcollation;

	/* Type information per attribute */
	Oid		   *parttypid;
	int32	   *parttypmod;
	int16	   *parttyplen;
	bool	   *parttypbyval;
	char	   *parttypalign;
	Oid		   *parttypcoll;
}			PartitionKeyData;

typedef struct PartitionKeyData *PartitionKey;

#ifdef _MLS_
typedef struct tagClsExprStruct
{
    MemoryContext mctx;
    Node *        rd_cls_expr;
    int16         attnum;
}ClsExprStruct;
#endif

/*
 * Here are the contents of a relation cache entry.
 */

typedef struct RelationData
{
	RelFileNode rd_node;		/* relation physical identifier */
	/* use "struct" here to avoid needing to include smgr.h: */
	struct SMgrRelationData *rd_smgr;	/* cached file handle, or NULL */
	int			rd_refcnt;		/* reference count */
	BackendId	rd_backend;		/* owning backend id, if temporary relation */
	bool		rd_islocaltemp; /* rel is a temp rel of this session */
	bool		rd_isnailed;	/* rel is nailed in cache */
	bool		rd_isvalid;		/* relcache entry is valid */
	char		rd_indexvalid;	/* state of rd_indexlist: 0 = not valid, 1 =
								 * valid, 2 = temporarily forced */
	bool		rd_statvalid;	/* is rd_statlist valid? */

	/*
	 * rd_createSubid is the ID of the highest subtransaction the rel has
	 * survived into; or zero if the rel was not created in the current top
	 * transaction.  This can be now be relied on, whereas previously it could
	 * be "forgotten" in earlier releases. Likewise, rd_newRelfilenodeSubid is
	 * the ID of the highest subtransaction the relfilenode change has
	 * survived into, or zero if not changed in the current transaction (or we
	 * have forgotten changing it). rd_newRelfilenodeSubid can be forgotten
	 * when a relation has multiple new relfilenodes within a single
	 * transaction, with one of them occurring in a subsequently aborted
	 * subtransaction, e.g. BEGIN; TRUNCATE t; SAVEPOINT save; TRUNCATE t;
	 * ROLLBACK TO save; -- rd_newRelfilenode is now forgotten
	 */
	SubTransactionId rd_createSubid;	/* rel was created in current xact */
	SubTransactionId rd_newRelfilenodeSubid;	/* new relfilenode assigned in
												 * current xact */

	Form_pg_class rd_rel;		/* RELATION tuple */
	TupleDesc	rd_att;			/* tuple descriptor */
	Oid			rd_id;			/* relation's object id */
	LockInfoData rd_lockInfo;	/* lock mgr's info for locking relation */
	RuleLock   *rd_rules;		/* rewrite rules */
	MemoryContext rd_rulescxt;	/* private memory cxt for rd_rules, if any */
	TriggerDesc *trigdesc;		/* Trigger info, or NULL if rel has none */
	/* use "struct" here to avoid needing to include rowsecurity.h: */
	struct RowSecurityDesc *rd_rsdesc;	/* row security policies, or NULL */
#ifdef _MLS_
    ClsExprStruct * rd_cls_struct;/* pg_cls_check function call expr */
#endif

	/* data managed by RelationGetFKeyList: */
	List	   *rd_fkeylist;	/* list of ForeignKeyCacheInfo (see below) */
	bool		rd_fkeyvalid;	/* true if list has been computed */

	MemoryContext rd_partkeycxt;	/* private memory cxt for the below */
	struct PartitionKeyData *rd_partkey;	/* partition key, or NULL */
	MemoryContext rd_pdcxt;		/* private context for partdesc */
	struct PartitionDescData *rd_partdesc;	/* partitions, or NULL */
	List	   *rd_partcheck;	/* partition CHECK quals */

	/* data managed by RelationGetIndexList: */
	List	   *rd_indexlist;	/* list of OIDs of indexes on relation */
	Oid			rd_oidindex;	/* OID of unique index on OID, if any */
	Oid			rd_pkindex;		/* OID of primary key, if any */
	Oid			rd_replidindex; /* OID of replica identity index, if any */

	/* data managed by RelationGetStatExtList: */
	List	   *rd_statlist;	/* list of OIDs of extended stats */

	/* data managed by RelationGetIndexAttrBitmap: */
	Bitmapset  *rd_indexattr;	/* identifies columns used in indexes */
	Bitmapset  *rd_keyattr;		/* cols that can be ref'd by foreign keys */
	Bitmapset  *rd_pkattr;		/* cols included in primary key */
	Bitmapset  *rd_idattr;		/* included in replica identity index */

	PublicationActions *rd_pubactions;	/* publication actions */

	/*
	 * rd_options is set whenever rd_rel is loaded into the relcache entry.
	 * Note that you can NOT look into rd_rel for this data.  NULL means "use
	 * defaults".
	 */
	bytea	   *rd_options;		/* parsed pg_class.reloptions */

	/* These are non-NULL only for an index relation: */
	Form_pg_index rd_index;		/* pg_index tuple describing this index */
	/* use "struct" here to avoid needing to include htup.h: */
	struct HeapTupleData *rd_indextuple;	/* all of pg_index tuple */

	/*
	 * index access support info (used only for an index relation)
	 *
	 * Note: only default support procs for each opclass are cached, namely
	 * those with lefttype and righttype equal to the opclass's opcintype. The
	 * arrays are indexed by support function number, which is a sufficient
	 * identifier given that restriction.
	 *
	 * Note: rd_amcache is available for index AMs to cache private data about
	 * an index.  This must be just a cache since it may get reset at any time
	 * (in particular, it will get reset by a relcache inval message for the
	 * index).  If used, it must point to a single memory chunk palloc'd in
	 * rd_indexcxt.  A relcache reset will include freeing that chunk and
	 * setting rd_amcache = NULL.
	 */
	Oid			rd_amhandler;	/* OID of index AM's handler function */
	MemoryContext rd_indexcxt;	/* private memory cxt for this stuff */
	/* use "struct" here to avoid needing to include amapi.h: */
	struct IndexAmRoutine *rd_amroutine;	/* index AM's API struct */
	Oid		   *rd_opfamily;	/* OIDs of op families for each index col */
	Oid		   *rd_opcintype;	/* OIDs of opclass declared input data types */
	RegProcedure *rd_support;	/* OIDs of support procedures */
	FmgrInfo   *rd_supportinfo; /* lookup info for support procedures */
	int16	   *rd_indoption;	/* per-column AM-specific flags */
	List	   *rd_indexprs;	/* index expression trees, if any */
	List	   *rd_indpred;		/* index predicate tree, if any */
	Oid		   *rd_exclops;		/* OIDs of exclusion operators, if any */
	Oid		   *rd_exclprocs;	/* OIDs of exclusion ops' procs, if any */
	uint16	   *rd_exclstrats;	/* exclusion ops' strategy numbers, if any */
	void	   *rd_amcache;		/* available for use by index AM */
	Oid		   *rd_indcollation;	/* OIDs of index collations */

	/*
	 * foreign-table support
	 *
	 * rd_fdwroutine must point to a single memory chunk palloc'd in
	 * CacheMemoryContext.  It will be freed and reset to NULL on a relcache
	 * reset.
	 */

	/* use "struct" here to avoid needing to include fdwapi.h: */
	struct FdwRoutine *rd_fdwroutine;	/* cached function pointers, or NULL */

	/*
	 * Hack for CLUSTER, rewriting ALTER TABLE, etc: when writing a new
	 * version of a table, we need to make any toast pointers inserted into it
	 * have the existing toast table's OID, not the OID of the transient toast
	 * table.  If rd_toastoid isn't InvalidOid, it is the OID to place in
	 * toast pointers inserted into this rel.  (Note it's set on the new
	 * version of the main heap, not the toast table itself.)  This also
	 * causes toast_save_datum() to try to preserve toast value OIDs.
	 */
	Oid			rd_toastoid;	/* Real TOAST table's OID, or InvalidOid */

	/* use "struct" here to avoid needing to include pgstat.h: */
	struct PgStat_TableStatus *pgstat_info; /* statistics collection area */
#ifdef PGXC
	RelationLocInfo *rd_locator_info;
#endif
#ifdef __TBASE__
	Form_pg_partition_interval  rd_partitions_info;
	dlist_node		rd_lru_list_elem;	/* list member of LRU list */
#endif
} RelationData;


/*
 * ForeignKeyCacheInfo
 *		Information the relcache can cache about foreign key constraints
 *
 * This is basically just an image of relevant columns from pg_constraint.
 * We make it a subclass of Node so that copyObject() can be used on a list
 * of these, but we also ensure it is a "flat" object without substructure,
 * so that list_free_deep() is sufficient to free such a list.
 * The per-FK-column arrays can be fixed-size because we allow at most
 * INDEX_MAX_KEYS columns in a foreign key constraint.
 *
 * Currently, we only cache fields of interest to the planner, but the
 * set of fields could be expanded in future.
 */
typedef struct ForeignKeyCacheInfo
{
	NodeTag		type;
	Oid			conrelid;		/* relation constrained by the foreign key */
	Oid			confrelid;		/* relation referenced by the foreign key */
	int			nkeys;			/* number of columns in the foreign key */
	/* these arrays each have nkeys valid entries: */
	AttrNumber	conkey[INDEX_MAX_KEYS]; /* cols in referencing table */
	AttrNumber	confkey[INDEX_MAX_KEYS];	/* cols in referenced table */
	Oid			conpfeqop[INDEX_MAX_KEYS];	/* PK = FK operator OIDs */
} ForeignKeyCacheInfo;


/*
 * StdRdOptions
 *		Standard contents of rd_options for heaps and generic indexes.
 *
 * RelationGetFillFactor() and RelationGetTargetPageFreeSpace() can only
 * be applied to relations that use this format or a superset for
 * private options data.
 */
 /* autovacuum-related reloptions. */
typedef struct AutoVacOpts
{
	bool		enabled;
	int			vacuum_threshold;
	int			analyze_threshold;
	int			vacuum_cost_delay;
	int			vacuum_cost_limit;
	int			freeze_min_age;
	int			freeze_max_age;
	int			freeze_table_age;
	int			multixact_freeze_min_age;
	int			multixact_freeze_max_age;
	int			multixact_freeze_table_age;
	int			log_min_duration;
	float8		vacuum_scale_factor;
	float8		analyze_scale_factor;
} AutoVacOpts;

typedef struct StdRdOptions
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int			fillfactor;		/* page fill factor in percent (0..100) */
	AutoVacOpts autovacuum;		/* autovacuum-related options */
	bool		user_catalog_table; /* use as an additional catalog relation */
	int			parallel_workers;	/* max number of parallel workers */
} StdRdOptions;

#define HEAP_MIN_FILLFACTOR			10
#define HEAP_DEFAULT_FILLFACTOR		100

/*
 * RelationGetFillFactor
 *		Returns the relation's fillfactor.  Note multiple eval of argument!
 */
#define RelationGetFillFactor(relation, defaultff) \
	((relation)->rd_options ? \
	 ((StdRdOptions *) (relation)->rd_options)->fillfactor : (defaultff))

/*
 * RelationGetTargetPageUsage
 *		Returns the relation's desired space usage per page in bytes.
 */
#define RelationGetTargetPageUsage(relation, defaultff) \
	(BLCKSZ * RelationGetFillFactor(relation, defaultff) / 100)

/*
 * RelationGetTargetPageFreeSpace
 *		Returns the relation's desired freespace per page in bytes.
 */
#define RelationGetTargetPageFreeSpace(relation, defaultff) \
	(BLCKSZ * (100 - RelationGetFillFactor(relation, defaultff)) / 100)

/*
 * RelationIsUsedAsCatalogTable
 *		Returns whether the relation should be treated as a catalog table
 *		from the pov of logical decoding.  Note multiple eval of argument!
 */
#define RelationIsUsedAsCatalogTable(relation)	\
	((relation)->rd_options && \
	 ((relation)->rd_rel->relkind == RELKIND_RELATION || \
	  (relation)->rd_rel->relkind == RELKIND_MATVIEW) ? \
	 ((StdRdOptions *) (relation)->rd_options)->user_catalog_table : false)

/*
 * RelationGetParallelWorkers
 *		Returns the relation's parallel_workers reloption setting.
 *		Note multiple eval of argument!
 */
#define RelationGetParallelWorkers(relation, defaultpw) \
	((relation)->rd_options ? \
	 ((StdRdOptions *) (relation)->rd_options)->parallel_workers : (defaultpw))


/*
 * ViewOptions
 *		Contents of rd_options for views
 */
typedef struct ViewOptions
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	bool		security_barrier;
	int			check_option_offset;
} ViewOptions;

/*
 * RelationIsSecurityView
 *		Returns whether the relation is security view, or not.  Note multiple
 *		eval of argument!
 */
#define RelationIsSecurityView(relation)	\
	((relation)->rd_options ?				\
	 ((ViewOptions *) (relation)->rd_options)->security_barrier : false)

/*
 * RelationHasCheckOption
 *		Returns true if the relation is a view defined with either the local
 *		or the cascaded check option.  Note multiple eval of argument!
 */
#define RelationHasCheckOption(relation)									\
	((relation)->rd_options &&												\
	 ((ViewOptions *) (relation)->rd_options)->check_option_offset != 0)

/*
 * RelationHasLocalCheckOption
 *		Returns true if the relation is a view defined with the local check
 *		option.  Note multiple eval of argument!
 */
#define RelationHasLocalCheckOption(relation)								\
	((relation)->rd_options &&												\
	 ((ViewOptions *) (relation)->rd_options)->check_option_offset != 0 ?	\
	 strcmp((char *) (relation)->rd_options +								\
			((ViewOptions *) (relation)->rd_options)->check_option_offset,	\
			"local") == 0 : false)

/*
 * RelationHasCascadedCheckOption
 *		Returns true if the relation is a view defined with the cascaded check
 *		option.  Note multiple eval of argument!
 */
#define RelationHasCascadedCheckOption(relation)							\
	((relation)->rd_options &&												\
	 ((ViewOptions *) (relation)->rd_options)->check_option_offset != 0 ?	\
	 strcmp((char *) (relation)->rd_options +								\
			((ViewOptions *) (relation)->rd_options)->check_option_offset,	\
			"cascaded") == 0 : false)


/*
 * RelationIsValid
 *		True iff relation descriptor is valid.
 */
#define RelationIsValid(relation) PointerIsValid(relation)

#define InvalidRelation ((Relation) NULL)

/*
 * RelationHasReferenceCountZero
 *		True iff relation reference count is zero.
 *
 * Note:
 *		Assumes relation descriptor is valid.
 */
#define RelationHasReferenceCountZero(relation) \
		((bool)((relation)->rd_refcnt == 0))

/*
 * RelationGetForm
 *		Returns pg_class tuple for a relation.
 *
 * Note:
 *		Assumes relation descriptor is valid.
 */
#define RelationGetForm(relation) ((relation)->rd_rel)

/*
 * RelationGetRelid
 *		Returns the OID of the relation
 */
#define RelationGetRelid(relation) ((relation)->rd_id)

/*
 * RelationGetNumberOfAttributes
 *		Returns the number of attributes in a relation.
 */
#define RelationGetNumberOfAttributes(relation) ((relation)->rd_rel->relnatts)

/*
 * RelationGetDescr
 *		Returns tuple descriptor for a relation.
 */
#define RelationGetDescr(relation) ((relation)->rd_att)

/*
 * RelationGetRelationName
 *		Returns the rel's name.
 *
 * Note that the name is only unique within the containing namespace.
 */
#define RelationGetRelationName(relation) \
	(NameStr((relation)->rd_rel->relname))

/*
 * RelationGetNamespace
 *		Returns the rel's namespace OID.
 */
#define RelationGetNamespace(relation) \
	((relation)->rd_rel->relnamespace)

/*
 * RelationIsMapped
 *		True if the relation uses the relfilenode map.
 *
 * NB: this is only meaningful for relkinds that have storage, else it
 * will misleadingly say "true".
 */
#define RelationIsMapped(relation) \
	((relation)->rd_rel->relfilenode == InvalidOid)

/*
 * RelationOpenSmgr
 *		Open the relation at the smgr level, if not already done.
 */
#define RelationOpenSmgr(relation) \
	do { \
		if ((relation)->rd_smgr == NULL) \
			smgrsetowner(&((relation)->rd_smgr), smgropen((relation)->rd_node, (relation)->rd_backend)); \
			(relation)->rd_smgr->smgr_hasextent = RelationHasExtent(relation); \
	} while (0)

/*
 * RelationCloseSmgr
 *		Close the relation at the smgr level, if not already done.
 *
 * Note: smgrclose should unhook from owner pointer, hence the Assert.
 */
#define RelationCloseSmgr(relation) \
	do { \
		if ((relation)->rd_smgr != NULL) \
		{ \
			smgrclose((relation)->rd_smgr); \
			Assert((relation)->rd_smgr == NULL); \
		} \
	} while (0)

/*
 * RelationGetTargetBlock
 *		Fetch relation's current insertion target block.
 *
 * Returns InvalidBlockNumber if there is no current target block.  Note
 * that the target block status is discarded on any smgr-level invalidation.
 */
#define RelationGetTargetBlock(relation) \
	( (relation)->rd_smgr != NULL ? (relation)->rd_smgr->smgr_targblock : InvalidBlockNumber )

#ifdef _SHARDING_
#define RelationGetTargetBlock_Shard(relation, shardid) \
	( (relation)->rd_smgr != NULL ? smgr_get_target_block((relation)->rd_smgr, shardid) : InvalidBlockNumber )
#endif
/*
 * RelationSetTargetBlock
 *		Set relation's current insertion target block.
 */
#define RelationSetTargetBlock(relation, targblock) \
	do { \
		RelationOpenSmgr(relation); \
		(relation)->rd_smgr->smgr_targblock = (targblock); \
	} while (0)

#ifdef _SHARDING_
#define RelationSetTargetBlock_Shard(relation, targblock, shardid) \
	do { \
		RelationOpenSmgr(relation); \
		smgr_set_target_block((relation)->rd_smgr, shardid, targblock); \
	} while (0)
#endif
/*
 * RelationNeedsWAL
 *		True if relation needs WAL.
 */
#define RelationNeedsWAL(relation) \
	((relation)->rd_rel->relpersistence == RELPERSISTENCE_PERMANENT)
	
#ifdef _SHARDING_
#if 0
#define RelationHasExtent(relation) \
	(IS_PGXC_DATANODE \
	&& (relation)->rd_rel->relpersistence == 'p' \
	&& ((relation)->rd_rel->relkind == RELKIND_RELATION || (relation)->rd_rel->relkind == RELKIND_TOASTVALUE)\
	&& (relation)->rd_locator_info ? ((relation)->rd_locator_info->locatorType == LOCATOR_TYPE_HASH ? true : false) : false\
	&& RelationGetRelid(relation) >= FirstNormalObjectId)
#endif
#define RelationHasExtent(relation) \
	((relation)->rd_rel->relhasextent)
#define RelationGetDisKey(relation) \
	((relation)->rd_locator_info ? (relation)->rd_locator_info->partAttrNum : InvalidAttrNumber)

#define RelationGetSecDisKey(relation) \
	((relation)->rd_locator_info ? (relation)->rd_locator_info->secAttrNum : InvalidAttrNumber)

#define RelationIsSharded(relation) \
	((relation)->rd_locator_info ? (relation)->rd_locator_info->locatorType == LOCATOR_TYPE_SHARD : false)

#define RelationHasToast(relation) \
	OidIsValid((relation)->rd_toastoid)
#endif

/*
 * RelationUsesLocalBuffers
 *		True if relation's pages are stored in local buffers.
 */
#ifdef XCP
#define RelationUsesLocalBuffers(relation) \
	(!OidIsValid(MyCoordId) && \
		((relation)->rd_rel->relpersistence == RELPERSISTENCE_TEMP))
#else
#define RelationUsesLocalBuffers(relation) \
	((relation)->rd_rel->relpersistence == RELPERSISTENCE_TEMP)
#endif

#ifdef PGXC
/*
 * RelationGetLocInfo
 *		Return the location info of relation
 */
#define RelationGetLocInfo(relation) ((relation)->rd_locator_info)
#endif

/*
 * RELATION_IS_LOCAL
 *		If a rel is either temp or newly created in the current transaction,
 *		it can be assumed to be accessible only to the current backend.
 *		This is typically used to decide that we can skip acquiring locks.
 *
 * Beware of multiple eval of argument
 */
#ifdef XCP
#define RELATION_IS_LOCAL(relation) \
	((!OidIsValid(MyCoordId) && (relation)->rd_backend == MyBackendId) || \
	 (OidIsValid(MyCoordId) && (relation)->rd_backend == MyFirstBackendId) || \
	 ((relation)->rd_backend == MyBackendId || \
	 (relation)->rd_createSubid != InvalidSubTransactionId))
#else
#define RELATION_IS_LOCAL(relation) \
	((relation)->rd_islocaltemp || \
	 (relation)->rd_createSubid != InvalidSubTransactionId)
#endif

#ifdef XCP
/*
 * RelationGetLocatorType
 *		Returns the rel's locator type.
 */
#define RelationGetLocatorType(relation) \
	((relation)->rd_locator_info->locatorType)

#endif

/*
 * RELATION_IS_OTHER_TEMP
 *		Test for a temporary relation that belongs to some other session.
 *
 * Beware of multiple eval of argument
 */
#ifdef XCP
#define RELATION_IS_OTHER_TEMP(relation) \
	(((relation)->rd_rel->relpersistence == RELPERSISTENCE_TEMP && \
	 (relation)->rd_backend != MyBackendId) && \
	 ((!OidIsValid(MyCoordId) && (relation)->rd_backend != MyBackendId) || \
	  (OidIsValid(MyCoordId) && (relation)->rd_backend != MyFirstBackendId)))
#else
#define RELATION_IS_OTHER_TEMP(relation) \
	((relation)->rd_rel->relpersistence == RELPERSISTENCE_TEMP && \
	 !(relation)->rd_islocaltemp)
#endif

#ifdef XCP
/*
 * RELATION_IS_COORDINATOR_LOCAL
 * 	Test for a coordinator only relation such as LOCAL TEMP table or a MATVIEW
 */
#define RELATION_IS_COORDINATOR_LOCAL(relation) \
	((RELATION_IS_LOCAL(relation) && !RelationGetLocInfo(relation)))
#endif

/*
 * RelationIsScannable
 *		Currently can only be false for a materialized view which has not been
 *		populated by its query.  This is likely to get more complicated later,
 *		so use a macro which looks like a function.
 */
#define RelationIsScannable(relation) ((relation)->rd_rel->relispopulated)

/*
 * RelationIsPopulated
 *		Currently, we don't physically distinguish the "populated" and
 *		"scannable" properties of matviews, but that may change later.
 *		Hence, use the appropriate one of these macros in code tests.
 */
#define RelationIsPopulated(relation) ((relation)->rd_rel->relispopulated)

/*
 * RelationIsAccessibleInLogicalDecoding
 *		True if we need to log enough information to have access via
 *		decoding snapshot.
 */
#define RelationIsAccessibleInLogicalDecoding(relation) \
	(XLogLogicalInfoActive() && \
	 RelationNeedsWAL(relation) && \
	 (IsCatalogRelation(relation) || RelationIsUsedAsCatalogTable(relation)))

/*
 * RelationIsLogicallyLogged
 *		True if we need to log enough information to extract the data from the
 *		WAL stream.
 *
 * We don't log information for unlogged tables (since they don't WAL log
 * anyway) and for system tables (their content is hard to make sense of, and
 * it would complicate decoding slightly for little gain). Note that we *do*
 * log information for user defined catalog tables since they presumably are
 * interesting to the user...
 */
#define RelationIsLogicallyLogged(relation) \
	(XLogLogicalInfoActive() && \
	 RelationNeedsWAL(relation) && \
	 !IsCatalogRelation(relation))

/*
 * RelationGetPartitionKey
 *		Returns the PartitionKey of a relation
 */
#define RelationGetPartitionKey(relation) ((relation)->rd_partkey)

#ifdef __TBASE__
#define RelationGetNParts(relation) \
	((relation)->rd_partitions_info ? (relation)->rd_partitions_info->partnparts : 0)

#define RelationGetPartitionColumnIndex(relation) \
	((relation)->rd_partitions_info ? (relation)->rd_partitions_info->partpartkey : InvalidAttrNumber)

#define RELATION_IS_INTERVAL(relation) \
	((relation)->rd_rel->relpartkind == RELPARTKIND_PARENT)
	//((relation)->rd_partkey && (relation)->rd_partkey->strategy == PARTITION_STRATEGY_INTERVAL)

#define RELATION_IS_CHILD(relation) \
	((relation)->rd_rel->relpartkind == RELPARTKIND_CHILD)

#define RELATION_GET_PARENT(relation) \
	((relation)->rd_rel->relparent)


#define RELATION_IS_REGULAR(relation) \
	((relation)->rd_rel->relpartkind == RELPARTKIND_NONE)


#define IndexGetRelationId(relation) \
	(	\
		(relation)->rd_rel->relkind == RELKIND_INDEX ? \
				(relation)->rd_index->indrelid : InvalidOid	\
	)

#define PARTITION_KEY_IS_TIMESTAMP(partoid) \
	((partoid) == 1114 || (partoid) == 1184)

extern int64 get_total_relation_size(Relation rel);
#endif

/*
 * PartitionKey inquiry functions
 */
static inline int
get_partition_strategy(PartitionKey key)
{
	return key->strategy;
}

static inline int
get_partition_natts(PartitionKey key)
{
	return key->partnatts;
}

static inline List *
get_partition_exprs(PartitionKey key)
{
	return key->partexprs;
}

/*
 * PartitionKey inquiry functions - one column
 */
static inline int16
get_partition_col_attnum(PartitionKey key, int col)
{
	return key->partattrs[col];
}

static inline Oid
get_partition_col_typid(PartitionKey key, int col)
{
	return key->parttypid[col];
}

static inline int32
get_partition_col_typmod(PartitionKey key, int col)
{
	return key->parttypmod[col];
}

/*
 * RelationGetPartitionDesc
 *		Returns partition descriptor for a relation.
 */
#define RelationGetPartitionDesc(relation) ((relation)->rd_partdesc)

/* routines in utils/cache/relcache.c */
extern void RelationIncrementReferenceCount(Relation rel);
extern void RelationDecrementReferenceCount(Relation rel);
extern bool RelationHasUnloggedIndex(Relation rel);
extern List *RelationGetRepsetList(Relation rel);

#endif							/* REL_H */
