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
/*-------------------------------------------------------------------------
 *
 * pg_publication.c
 *        publication C API manipulation
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *        pg_publication.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"

#include "access/genam.h"
#include "access/hash.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"

#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_type.h"
#include "catalog/pg_publication.h"
#include "catalog/pg_publication_rel.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_extension.h"

#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#ifdef __STORAGE_SCALABLE__
#include "catalog/pg_publication_shard.h"
#endif

#ifdef __SUBSCRIPTION__
#include "replication/logicalrelation.h"
#endif

typedef struct
{
	Oid tableoid;
	Oid oid;
} CatalogId;

/* This is an array of object identities. */
static CatalogId *extmembers;
static int numextmembers;

#define oidcmp(x,y) ( ((x) < (y) ? -1 : ((x) > (y)) ?  1 : 0) )

/*
 * qsort comparator for CatalogId.
 */
static int
CatalogIdCompare(const void *p1, const void *p2)
{
	const CatalogId *obj1 = (const CatalogId *) p1;
	const CatalogId *obj2 = (const CatalogId *) p2;
	int cmpval;
	
	/*
	 * Compare OID first since it's usually unique, whereas there will only be
	 * a few distinct values of tableoid.
	 */
	cmpval = oidcmp(obj1->oid, obj2->oid);
	if (cmpval == 0)
		cmpval = oidcmp(obj1->tableoid, obj2->tableoid);
	return cmpval;
}

/*
 * setExtensionMembership
 *	  accept and save data about which objects belong to extensions
 */
static void
setExtensionMembership(CatalogId *extmems, int nextmems)
{
	/* Sort array in preparation for binary searches */
	if (nextmems > 1)
		qsort((void *) extmems, nextmems, sizeof(CatalogId),
		      CatalogIdCompare);
	/* And save */
	extmembers = extmems;
	numextmembers = nextmems;
}

/*
 * getExtensionMembership --- obtain extension membership data
 *
 * We need to identify objects that are extension members as soon as they're
 * loaded, so that we can correctly determine whether they need to be dentified as publishable.
 * Generally speaking, extension member objects will get marked as *not* to be publishable.
 */
static void
getExtensionMembership()
{
	CatalogId *extmembers;
	Relation depRel;
	SysScanDesc depScan;
	HeapTuple depTup;
	int maxObjs = 32;
	int nextmembers = 0;
	
	extmembers = (CatalogId *) palloc0(maxObjs * sizeof(CatalogId));
	
	depRel = heap_open(DependRelationId, AccessShareLock);
	depScan = systable_beginscan(depRel, DependReferenceIndexId, true, NULL, 0, NULL);
	while (HeapTupleIsValid(depTup = systable_getnext(depScan)))
	{
		/*
		 * We scan pg_depend to find those relations(RelationRelationId)
		 * that depend on the given extension type.
		 * (We assume we can ignore refobjsubid for a type.)
		 */
		Form_pg_depend pg_depend = (Form_pg_depend) GETSTRUCT(depTup);
		if (pg_depend->refclassid != ExtensionRelationId
		    || pg_depend->deptype != DEPENDENCY_EXTENSION
		    || pg_depend->classid != RelationRelationId)
			continue;
		
		if (nextmembers >= maxObjs)
		{
			maxObjs *= 2;
			extmembers = (CatalogId *) repalloc(extmembers, maxObjs * sizeof(CatalogId));
		}
		extmembers[nextmembers].tableoid = pg_depend->classid;
		extmembers[nextmembers].oid = pg_depend->objid;
		nextmembers++;
	}
	
	systable_endscan(depScan);
	relation_close(depRel, AccessShareLock);
	
	/* Remember the data for use later */
	setExtensionMembership(extmembers, nextmembers);
}

/*
 * IsCatalogIdExtensionMember
 *	  return If the specified catalog ID depends on some extension.
 */
static bool
IsCatalogIdExtensionMember(CatalogId catalogId)
{
	CatalogId *low;
	CatalogId *high;
	
	/*
	 * We could use bsearch() here, but the notational cruft of calling
	 * bsearch is nearly as bad as doing it ourselves; and the generalized
	 * bsearch function is noticeably slower as well.
	 */
	if (numextmembers <= 0)
		return false;
	
	low = extmembers;
	high = extmembers + (numextmembers - 1);
	while (low <= high)
	{
		CatalogId *middle;
		int difference;
		
		middle = low + (high - low) / 2;
		/* comparison must match ExtensionMemberIdCompare, below */
		difference = oidcmp(middle->oid, catalogId.oid);
		if (difference == 0)
			difference = oidcmp(middle->tableoid, catalogId.tableoid);
		if (difference == 0)
			return true;
		else if (difference < 0)
			low = middle + 1;
		else
			high = middle - 1;
	}
	return false;
}

/*
 * Check if relation can be in given publication and throws appropriate
 * error if not.
 */
static void
check_publication_add_relation(Relation targetrel)
{// #lizard forgives
    /* Give more specific error for partitioned tables */
    if (RelationGetForm(targetrel)->relkind == RELKIND_PARTITIONED_TABLE)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("\"%s\" is a partitioned table",
                        RelationGetRelationName(targetrel)),
                 errdetail("Adding partitioned tables to publications is not supported."),
                 errhint("You can add the table partitions individually.")));

    /* Must be table */
    if (RelationGetForm(targetrel)->relkind != RELKIND_RELATION)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("\"%s\" is not a table",
                        RelationGetRelationName(targetrel)),
                 errdetail("Only tables can be added to publications.")));

    /* Can't be system table */
    if (IsCatalogRelation(targetrel))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("\"%s\" is a system table",
                        RelationGetRelationName(targetrel)),
                 errdetail("System tables cannot be added to publications.")));

    /* UNLOGGED and TEMP relations cannot be part of publication. */
    if (!RelationNeedsWAL(targetrel))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("table \"%s\" cannot be replicated",
                        RelationGetRelationName(targetrel)),
                 errdetail("Temporary and unlogged relations cannot be replicated.")));

#ifdef __SUBSCRIPTION__
    do 
    {
        Oid     identityOid = InvalidOid;
        char     replident = REPLICA_IDENTITY_NOTHING;

        identityOid = GetRelationIdentityOrPK(targetrel);
        replident = targetrel->rd_rel->relreplident;

        if (OidIsValid(identityOid))
        {
            /* valid relation, do nothing */
        }
        else if (replident == REPLICA_IDENTITY_FULL)
        {
            ereport(WARNING,
                 (errmsg("For data consistency considerations, "
                          "it is recommended to use a REPLICA IDENTITY index or PRIMARY KEY, "
                         "and not a REPLICA IDENTITY FULL")));
        }
        else
        {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("table \"%s\" cannot be replicated, because it has "
                        "neither REPLICA IDENTITY index nor PRIMARY "
                        "KEY, nor a REPLICA IDENTITY FULL",
                        RelationGetRelationName(targetrel))));
        }
    } while (0);
#endif
}

/*
 * Returns if relation represented by oid and Form_pg_class entry
 * is publishable.
 *
 * Does same checks as the above, but does not need relation to be opened
 * and also does not throw errors.
 *
 * Note this also excludes all tables with relid < FirstNormalObjectId,
 * ie all tables created during initdb.  This mainly affects the preinstalled
 * information_schema.  (IsCatalogClass() only checks for these inside
 * pg_catalog and toast schemas.)
 */
static bool
is_publishable_class(Oid relid, Form_pg_class reltuple)
{
    return reltuple->relkind == RELKIND_RELATION &&
        !IsCatalogClass(relid, reltuple) &&
        reltuple->relpersistence == RELPERSISTENCE_PERMANENT &&
        relid >= FirstNormalObjectId;
}


/*
 * SQL-callable variant of the above
 *
 * This returns null when the relation does not exist.  This is intended to be
 * used for example in psql to avoid gratuitous errors when there are
 * concurrent catalog changes.
 */
Datum
pg_relation_is_publishable(PG_FUNCTION_ARGS)
{
    Oid            relid = PG_GETARG_OID(0);
    HeapTuple    tuple;
    bool        result;

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!tuple)
        PG_RETURN_NULL();
    result = is_publishable_class(relid, (Form_pg_class) GETSTRUCT(tuple));
    ReleaseSysCache(tuple);
    PG_RETURN_BOOL(result);
}


/*
 * Insert new publication / relation mapping.
 */
ObjectAddress
publication_add_relation(Oid pubid, Relation targetrel,
                         bool if_not_exists)
{
    Relation    rel;
    HeapTuple    tup;
    Datum        values[Natts_pg_publication_rel];
    bool        nulls[Natts_pg_publication_rel];
    Oid            relid = RelationGetRelid(targetrel);
    Oid            prrelid;
    Publication *pub = GetPublication(pubid);
    ObjectAddress myself,
                referenced;

    rel = heap_open(PublicationRelRelationId, RowExclusiveLock);

    /*
     * Check for duplicates. Note that this does not really prevent
     * duplicates, it's here just to provide nicer error message in common
     * case. The real protection is the unique key on the catalog.
     */
    if (SearchSysCacheExists2(PUBLICATIONRELMAP, ObjectIdGetDatum(relid),
                              ObjectIdGetDatum(pubid)))
    {
        heap_close(rel, RowExclusiveLock);

        if (if_not_exists)
            return InvalidObjectAddress;

        ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_OBJECT),
                 errmsg("relation \"%s\" is already member of publication \"%s\"",
                        RelationGetRelationName(targetrel), pub->name)));
    }

    check_publication_add_relation(targetrel);

    /* Form a tuple. */
    memset(values, 0, sizeof(values));
    memset(nulls, false, sizeof(nulls));

    values[Anum_pg_publication_rel_prpubid - 1] =
        ObjectIdGetDatum(pubid);
    values[Anum_pg_publication_rel_prrelid - 1] =
        ObjectIdGetDatum(relid);

    tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);

    /* Insert tuple into catalog. */
    prrelid = CatalogTupleInsert(rel, tup);
    heap_freetuple(tup);

    ObjectAddressSet(myself, PublicationRelRelationId, prrelid);

    /* Add dependency on the publication */
    ObjectAddressSet(referenced, PublicationRelationId, pubid);
    recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);

    /* Add dependency on the relation */
    ObjectAddressSet(referenced, RelationRelationId, relid);
    recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);

    /* Close the table. */
    heap_close(rel, RowExclusiveLock);

    /* Invalidate relcache so that publication info is rebuilt. */
    CacheInvalidateRelcache(targetrel);

    return myself;
}


/*
 * Gets list of publication oids for a relation oid.
 */
List *
GetRelationPublications(Oid relid)
{
    List       *result = NIL;
    CatCList   *pubrellist;
    int            i;

    /* Find all publications associated with the relation. */
    pubrellist = SearchSysCacheList1(PUBLICATIONRELMAP,
                                     ObjectIdGetDatum(relid));
    for (i = 0; i < pubrellist->n_members; i++)
    {
        HeapTuple    tup = &pubrellist->members[i]->tuple;
        Oid            pubid = ((Form_pg_publication_rel) GETSTRUCT(tup))->prpubid;

        result = lappend_oid(result, pubid);
    }

    ReleaseSysCacheList(pubrellist);

    return result;
}

/*
 * Gets list of relation oids for a publication.
 *
 * This should only be used for normal publications, the FOR ALL TABLES
 * should use GetAllTablesPublicationRelations().
 */
List *
GetPublicationRelations(Oid pubid)
{
    List       *result;
    Relation    pubrelsrel;
    ScanKeyData scankey;
    SysScanDesc scan;
    HeapTuple    tup;

    /* Find all publications associated with the relation. */
    pubrelsrel = heap_open(PublicationRelRelationId, AccessShareLock);

    ScanKeyInit(&scankey,
                Anum_pg_publication_rel_prpubid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(pubid));

    scan = systable_beginscan(pubrelsrel, PublicationRelPrrelidPrpubidIndexId,
                              true, NULL, 1, &scankey);

    result = NIL;
    while (HeapTupleIsValid(tup = systable_getnext(scan)))
    {
        Form_pg_publication_rel pubrel;

        pubrel = (Form_pg_publication_rel) GETSTRUCT(tup);

        result = lappend_oid(result, pubrel->prrelid);
    }

    systable_endscan(scan);
    heap_close(pubrelsrel, AccessShareLock);

    return result;
}

/*
 * Gets list of publication oids for publications marked as FOR ALL TABLES.
 */
List *
GetAllTablesPublications(void)
{
    List       *result;
    Relation    rel;
    ScanKeyData scankey;
    SysScanDesc scan;
    HeapTuple    tup;

    /* Find all publications that are marked as for all tables. */
    rel = heap_open(PublicationRelationId, AccessShareLock);

    ScanKeyInit(&scankey,
                Anum_pg_publication_puballtables,
                BTEqualStrategyNumber, F_BOOLEQ,
                BoolGetDatum(true));

    scan = systable_beginscan(rel, InvalidOid, false,
                              NULL, 1, &scankey);

    result = NIL;
    while (HeapTupleIsValid(tup = systable_getnext(scan)))
        result = lappend_oid(result, HeapTupleGetOid(tup));

    systable_endscan(scan);
    heap_close(rel, AccessShareLock);

    return result;
}

/*
 * Gets list of all relation published by FOR ALL TABLES publication(s).
 */
List *
GetAllTablesPublicationRelations(void)
{
    Relation    classRel;
    ScanKeyData key[1];
    HeapScanDesc scan;
    HeapTuple    tuple;
    List       *result = NIL;

	getExtensionMembership();

    classRel = heap_open(RelationRelationId, AccessShareLock);

    ScanKeyInit(&key[0],
                Anum_pg_class_relkind,
                BTEqualStrategyNumber, F_CHAREQ,
                CharGetDatum(RELKIND_RELATION));

    scan = heap_beginscan_catalog(classRel, 1, key);

    while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
    {
		CatalogId   pub_rel;
        Oid            relid = HeapTupleGetOid(tuple);
        Form_pg_class relForm = (Form_pg_class) GETSTRUCT(tuple);

		pub_rel.tableoid = RelationRelationId;
		pub_rel.oid = relid;
		
		if (is_publishable_class(relid, relForm)
		    && !IsCatalogIdExtensionMember(pub_rel))
            result = lappend_oid(result, relid);
    }

    heap_endscan(scan);
    heap_close(classRel, AccessShareLock);

	if (extmembers)
		pfree(extmembers);

    return result;
}

/*
 * Get publication using oid
 *
 * The Publication struct and its data are palloc'ed here.
 */
Publication *
GetPublication(Oid pubid)
{
    HeapTuple    tup;
    Publication *pub;
    Form_pg_publication pubform;

    tup = SearchSysCache1(PUBLICATIONOID, ObjectIdGetDatum(pubid));

    if (!HeapTupleIsValid(tup))
        elog(ERROR, "cache lookup failed for publication %u", pubid);

    pubform = (Form_pg_publication) GETSTRUCT(tup);

    pub = (Publication *) palloc(sizeof(Publication));
    pub->oid = pubid;
    pub->name = pstrdup(NameStr(pubform->pubname));
    pub->alltables = pubform->puballtables;
    pub->pubactions.pubinsert = pubform->pubinsert;
    pub->pubactions.pubupdate = pubform->pubupdate;
    pub->pubactions.pubdelete = pubform->pubdelete;

    ReleaseSysCache(tup);

    return pub;
}


/*
 * Get Publication using name.
 */
Publication *
GetPublicationByName(const char *pubname, bool missing_ok)
{
    Oid            oid;

    oid = GetSysCacheOid1(PUBLICATIONNAME, CStringGetDatum(pubname));
    if (!OidIsValid(oid))
    {
        if (missing_ok)
            return NULL;

        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("publication \"%s\" does not exist", pubname)));
    }

    return GetPublication(oid);
}

/*
 * get_publication_oid - given a publication name, look up the OID
 *
 * If missing_ok is false, throw an error if name not found.  If true, just
 * return InvalidOid.
 */
Oid
get_publication_oid(const char *pubname, bool missing_ok)
{
    Oid            oid;

    oid = GetSysCacheOid1(PUBLICATIONNAME, CStringGetDatum(pubname));
    if (!OidIsValid(oid) && !missing_ok)
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("publication \"%s\" does not exist", pubname)));
    return oid;
}

/*
 * get_publication_name - given a publication Oid, look up the name
 */
char *
get_publication_name(Oid pubid)
{
    HeapTuple    tup;
    char       *pubname;
    Form_pg_publication pubform;

    tup = SearchSysCache1(PUBLICATIONOID, ObjectIdGetDatum(pubid));

    if (!HeapTupleIsValid(tup))
        elog(ERROR, "cache lookup failed for publication %u", pubid);

    pubform = (Form_pg_publication) GETSTRUCT(tup);
    pubname = pstrdup(NameStr(pubform->pubname));

    ReleaseSysCache(tup);

    return pubname;
}

/*
 * Returns Oids of tables in a publication.
 */
Datum
pg_get_publication_tables(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    char       *pubname = text_to_cstring(PG_GETARG_TEXT_PP(0));
    Publication *publication;
    List       *tables;
    ListCell  **lcp;

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL())
    {
        MemoryContext oldcontext;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /* switch to memory context appropriate for multiple function calls */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        publication = GetPublicationByName(pubname, false);
        if (publication->alltables)
            tables = GetAllTablesPublicationRelations();
        else
            tables = GetPublicationRelations(publication->oid);
        lcp = (ListCell **) palloc(sizeof(ListCell *));
        *lcp = list_head(tables);
        funcctx->user_fctx = (void *) lcp;

        MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    lcp = (ListCell **) funcctx->user_fctx;

    while (*lcp != NULL)
    {
        Oid            relid = lfirst_oid(*lcp);

        *lcp = lnext(*lcp);
        SRF_RETURN_NEXT(funcctx, ObjectIdGetDatum(relid));
    }

    SRF_RETURN_DONE(funcctx);
}
#ifdef __STORAGE_SCALABLE__
/*
 * Insert new publication / shard mapping.
 */
ObjectAddress
publication_add_shard(Oid pubid, int32 shardid,
                         bool if_not_exists)
{
    Relation    rel;
    HeapTuple    tup;
    Datum        values[Natts_pg_publication_shard];
    bool        nulls[Natts_pg_publication_shard];
    Oid            prrelid;
    Publication *pub = GetPublication(pubid);
    ObjectAddress myself,
                referenced;

    rel = heap_open(PublicationShardRelationId, RowExclusiveLock);

    /*
     * Check for duplicates. Note that this does not really prevent
     * duplicates, it's here just to provide nicer error message in common
     * case. The real protection is the unique key on the catalog.
     */
    if (SearchSysCacheExists2(PUBLICATIONSHARDMAP, Int32GetDatum(shardid),
                              ObjectIdGetDatum(pubid)))
    {
        heap_close(rel, RowExclusiveLock);

        if (if_not_exists)
            return InvalidObjectAddress;

        ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_OBJECT),
                 errmsg("shard \"%d\" is already member of publication \"%s\"",
                        shardid, pub->name)));
    }

    /* check shardid */
    if (!ShardIDIsValid(shardid))
    {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
             errmsg("\"%d\" is a invalid shardid",
                    shardid),
             errdetail("Invalid shardid cannot be added to publications.")));
    }

    /* Form a tuple. */
    memset(values, 0, sizeof(values));
    memset(nulls, false, sizeof(nulls));

    values[Anum_pg_publication_shard_prpubid - 1] =
        ObjectIdGetDatum(pubid);
    values[Anum_pg_publication_shard_prshardid - 1] =
        Int32GetDatum(shardid);

    tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);

    /* Insert tuple into catalog. */
    prrelid = CatalogTupleInsert(rel, tup);
    heap_freetuple(tup);

    ObjectAddressSet(myself, PublicationShardRelationId, prrelid);

    /* Add dependency on the publication */
    ObjectAddressSet(referenced, PublicationRelationId, pubid);
    recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);

    /* Close the table. */
    heap_close(rel, RowExclusiveLock);

    return myself;
}

/*
 * Gets list of shard ids for a publication.
 */
List *
GetPublicationShards(Oid pubid)
{
    List       *result;
    Relation    pubrelsrel;
    ScanKeyData scankey;
    SysScanDesc scan;
    HeapTuple    tup;

    pubrelsrel = heap_open(PublicationShardRelationId, AccessShareLock);

    ScanKeyInit(&scankey,
                Anum_pg_publication_shard_prpubid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(pubid));

    scan = systable_beginscan(pubrelsrel, PublicationShardPrshardidPrpubidIndexId,
                              true, NULL, 1, &scankey);

    result = NIL;
    while (HeapTupleIsValid(tup = systable_getnext(scan)))
    {
        Form_pg_publication_shard pubrel;

        pubrel = (Form_pg_publication_shard) GETSTRUCT(tup);

        result = lappend_int(result, pubrel->prshardid);
    }

    systable_endscan(scan);
    heap_close(pubrelsrel, AccessShareLock);

    return result;
}
#endif
