/*-------------------------------------------------------------------------
 *
 * pg_subscription.c
 *        replication subscriptions
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *        src/backend/catalog/pg_subscription.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"

#include "catalog/indexing.h"
#include "catalog/pg_type.h"
#include "catalog/pg_subscription.h"
#include "catalog/pg_subscription_rel.h"

#include "nodes/makefuncs.h"

#include "storage/lmgr.h"

#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/pg_lsn.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#ifdef __STORAGE_SCALABLE__
#include "catalog/pg_subscription_shard.h"
#include "catalog/dependency.h"
#include "catalog/pg_subscription_table.h"
#include "utils/catcache.h"
#include "utils/lsyscache.h"
#endif

#ifdef __SUBSCRIPTION__
#include "commands/subscriptioncmds.h"
#include "utils/tqual.h"
#include "access/sysattr.h"
#include "replication/worker_internal.h"
#include "utils/snapmgr.h"
#endif

static List *textarray_to_stringlist(ArrayType *textarray);

/*
 * Fetch the subscription from the syscache.
 */
Subscription *
GetSubscription(Oid subid, bool missing_ok)
{// #lizard forgives
    HeapTuple    tup;
    Subscription *sub;
    Form_pg_subscription subform;
    Datum        datum;
    bool        isnull;

    tup = SearchSysCache1(SUBSCRIPTIONOID, ObjectIdGetDatum(subid));

    if (!HeapTupleIsValid(tup))
    {
        if (missing_ok)
            return NULL;

        elog(ERROR, "cache lookup failed for subscription %u", subid);
    }

    subform = (Form_pg_subscription) GETSTRUCT(tup);

    sub = (Subscription *) palloc0(sizeof(Subscription));
    sub->oid = subid;
    sub->dbid = subform->subdbid;
    sub->name = pstrdup(NameStr(subform->subname));
    sub->owner = subform->subowner;
    sub->enabled = subform->subenabled;

    /* Get conninfo */
    datum = SysCacheGetAttr(SUBSCRIPTIONOID,
                            tup,
                            Anum_pg_subscription_subconninfo,
                            &isnull);
    Assert(!isnull);
    sub->conninfo = TextDatumGetCString(datum);

    /* Get slotname */
    datum = SysCacheGetAttr(SUBSCRIPTIONOID,
                            tup,
                            Anum_pg_subscription_subslotname,
                            &isnull);
    if (!isnull)
        sub->slotname = pstrdup(NameStr(*DatumGetName(datum)));
    else
        sub->slotname = NULL;

    /* Get synccommit */
    datum = SysCacheGetAttr(SUBSCRIPTIONOID,
                            tup,
                            Anum_pg_subscription_subsynccommit,
                            &isnull);
    Assert(!isnull);
    sub->synccommit = TextDatumGetCString(datum);

    /* Get publications */
    datum = SysCacheGetAttr(SUBSCRIPTIONOID,
                            tup,
                            Anum_pg_subscription_subpublications,
                            &isnull);
    Assert(!isnull);
    sub->publications = textarray_to_stringlist(DatumGetArrayTypeP(datum));

    ReleaseSysCache(tup);

#ifdef __SUBSCRIPTION__
    if (IS_PGXC_COORDINATOR)
    {
        Oid     sub_parent_oid = InvalidOid;
        int32    sub_index = -1;

        check_tbase_subscription_extension();
        PushActiveSnapshot(GetLocalTransactionSnapshot());

        /* get sub_parent/sub_index in tbase_subscription_parallel */
        do
        {
            Relation         tbase_sub_parallel_rel = NULL;
            HeapScanDesc    scan = NULL;
            HeapTuple       tuple = NULL;
            TupleDesc        desc = NULL;
            int                nkeys = 0;
            ScanKeyData     skey[1];

            bool            found = false;

            ScanKeyInit(&skey[nkeys++],
                        Anum_tbase_subscription_parallel_sub_child,
                        BTEqualStrategyNumber, F_OIDEQ,
                        ObjectIdGetDatum(subid));

            tbase_sub_parallel_rel = relation_openrv(makeRangeVar("public",
                                                        (char *)g_tbase_subscription_parallel_relname, -1),
                                                        AccessShareLock);
            desc = RelationGetDescr(tbase_sub_parallel_rel);
            scan = heap_beginscan(tbase_sub_parallel_rel, GetActiveSnapshot(), nkeys, skey);

            while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
            {
                Oid        sub_child = InvalidOid;

                sub_child = DatumGetObjectId(fastgetattr(tuple, Anum_tbase_subscription_parallel_sub_child, desc, &isnull));
                if (false == isnull && subid == sub_child)
                {
                    found = true;
                    sub_parent_oid = DatumGetObjectId(fastgetattr(tuple, Anum_tbase_subscription_parallel_sub_parent, desc, &isnull));
                    sub_index = DatumGetInt32(fastgetattr(tuple, Anum_tbase_subscription_parallel_sub_index, desc, &isnull));
                    sub->active_state = DatumGetBool(fastgetattr(tuple, Anum_tbase_subscription_parallel_sub_active_state, desc, &isnull));
                    sub->active_lsn = DatumGetLSN(fastgetattr(tuple, Anum_tbase_subscription_parallel_sub_active_lsn, desc, &isnull));
                    break;
                }
            }

            heap_endscan(scan);
            relation_close(tbase_sub_parallel_rel, AccessShareLock);

            Assert(found == true && sub_parent_oid != InvalidOid && sub_index >= 0);
            if (!(found == true && sub_parent_oid != InvalidOid && sub_index >= 0))
            {
                abort();
            }
        } while (0);

        /* get others options in tbase_subscription by sub_parent */
        do
        {
            Relation         tbase_sub_rel = NULL;
            HeapScanDesc    scan = NULL;
            HeapTuple       tuple = NULL;
            TupleDesc        desc = NULL;
            int                nkeys = 0;
            ScanKeyData     skey[1];

            bool            found = false;

            ScanKeyInit(&skey[nkeys++],
                        ObjectIdAttributeNumber,
                        BTEqualStrategyNumber, F_OIDEQ,
                        ObjectIdGetDatum(sub_parent_oid));

            tbase_sub_rel = relation_openrv(makeRangeVar("public", 
                                            (char *)g_tbase_subscription_relname, -1), 
                                            AccessShareLock);
            desc = RelationGetDescr(tbase_sub_rel);
            scan = heap_beginscan(tbase_sub_rel, GetActiveSnapshot(), nkeys, skey);

            while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
            {
                Oid     tupleoid = InvalidOid;

                tupleoid = HeapTupleGetOid(tuple);

                if (tupleoid == sub_parent_oid)
                {
                
                    bool    nulls[Natts_tbase_subscription] = { false };
                    Datum    values[Natts_tbase_subscription] = { 0 };

                    Name subname = NULL;

                    heap_deform_tuple(tuple, desc, values, nulls);

                    Assert(false == nulls[Anum_tbase_subscription_sub_name - 1]);
                    subname = DatumGetName(values[Anum_tbase_subscription_sub_name - 1]);
                    sub->parent_name = pstrdup(NameStr(*subname));

                    Assert(false == nulls[Anum_tbase_subscription_sub_parallel_number - 1]);
                    sub->parallel_number = DatumGetInt32(values[Anum_tbase_subscription_sub_parallel_number - 1]);

                    sub->parallel_index = sub_index;

                    Assert(false == nulls[Anum_tbase_subscription_sub_ignore_pk_conflict - 1]);
                    sub->ignore_pk_conflict = DatumGetBool(values[Anum_tbase_subscription_sub_ignore_pk_conflict - 1]);

                    if (false == nulls[Anum_tbase_subscription_sub_manual_hot_date - 1])
                        sub->manual_hot_date = TextDatumGetCString(values[Anum_tbase_subscription_sub_manual_hot_date - 1]);
                    else
                        sub->manual_hot_date = NULL;

                    if (false == nulls[Anum_tbase_subscription_sub_temp_hot_date - 1])
                        sub->temp_hot_date = TextDatumGetCString(values[Anum_tbase_subscription_sub_temp_hot_date - 1]);
                    else
                        sub->temp_hot_date = NULL;

                    if (false == nulls[Anum_tbase_subscription_sub_temp_cold_date - 1])
                        sub->temp_cold_date = TextDatumGetCString(values[Anum_tbase_subscription_sub_temp_cold_date - 1]);
                    else
                        sub->temp_cold_date = NULL;

                    Assert (false == nulls[Anum_tbase_subscription_sub_is_all_actived - 1]);
                    sub->is_all_actived = DatumGetBool(values[Anum_tbase_subscription_sub_is_all_actived - 1]);

                    found = true;
                    break;
                }
            }

            heap_endscan(scan);
            relation_close(tbase_sub_rel, AccessShareLock);

            Assert(found == true && sub->parallel_number >= 1 && sub->parallel_number > sub_index);
            if (!(found == true && sub->parallel_number >= 1 && sub->parallel_number > sub_index))
            {
                abort();
            }
        } while (0);

        /* add parallel info into conninfo */
        do
        {
            StringInfoData conninfo;
            initStringInfo(&conninfo);
            appendStringInfo(&conninfo, 
                                "%s sub_parallel_number=%d sub_parallel_index=%d",
                                sub->conninfo,
                                sub->parallel_number,
                                sub->parallel_index);
            pfree(sub->conninfo);
            sub->conninfo = conninfo.data;
        } while (0);

        PopActiveSnapshot();
        CommandCounterIncrement();
    }
    else
    {
        sub->parent_name = NULL;
        sub->parallel_number = 0;
        sub->parallel_index = 0;
        sub->ignore_pk_conflict = false;
        sub->manual_hot_date = NULL;
        sub->temp_hot_date = NULL;
        sub->temp_cold_date = NULL;
        sub->is_all_actived = true;
        sub->active_state = true;
        sub->active_lsn = InvalidXLogRecPtr;
    }
#endif

    return sub;
}

/*
 * Return number of subscriptions defined in given database.
 * Used by dropdb() to check if database can indeed be dropped.
 */
int
CountDBSubscriptions(Oid dbid)
{
    int            nsubs = 0;
    Relation    rel;
    ScanKeyData scankey;
    SysScanDesc scan;
    HeapTuple    tup;

    rel = heap_open(SubscriptionRelationId, RowExclusiveLock);

    ScanKeyInit(&scankey,
                Anum_pg_subscription_subdbid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(dbid));

    scan = systable_beginscan(rel, InvalidOid, false,
                              NULL, 1, &scankey);

    while (HeapTupleIsValid(tup = systable_getnext(scan)))
        nsubs++;

    systable_endscan(scan);

    heap_close(rel, NoLock);

    return nsubs;
}

/*
 * Free memory allocated by subscription struct.
 */
void
FreeSubscription(Subscription *sub)
{
    pfree(sub->name);
    pfree(sub->conninfo);
    if (sub->slotname)
        pfree(sub->slotname);
    list_free_deep(sub->publications);
    pfree(sub);

#ifdef __SUBSCRIPTION__
    if (sub->parent_name)
        pfree(sub->parent_name);

    if (sub->manual_hot_date)
        pfree(sub->manual_hot_date);

    if (sub->temp_hot_date)
        pfree(sub->temp_hot_date);

    if (sub->temp_cold_date)
        pfree(sub->temp_cold_date);
#endif
}

/*
 * get_subscription_oid - given a subscription name, look up the OID
 *
 * If missing_ok is false, throw an error if name not found.  If true, just
 * return InvalidOid.
 */
Oid
get_subscription_oid(const char *subname, bool missing_ok)
{
    Oid            oid;

    oid = GetSysCacheOid2(SUBSCRIPTIONNAME, MyDatabaseId,
                          CStringGetDatum(subname));
    if (!OidIsValid(oid) && !missing_ok)
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("subscription \"%s\" does not exist", subname)));
    return oid;
}

/*
 * get_subscription_name - given a subscription OID, look up the name
 */
char *
get_subscription_name(Oid subid)
{
    HeapTuple    tup;
    char       *subname;
    Form_pg_subscription subform;

    tup = SearchSysCache1(SUBSCRIPTIONOID, ObjectIdGetDatum(subid));

    if (!HeapTupleIsValid(tup))
        elog(ERROR, "cache lookup failed for subscription %u", subid);

    subform = (Form_pg_subscription) GETSTRUCT(tup);
    subname = pstrdup(NameStr(subform->subname));

    ReleaseSysCache(tup);

    return subname;
}

/*
 * Convert text array to list of strings.
 *
 * Note: the resulting list of strings is pallocated here.
 */
static List *
textarray_to_stringlist(ArrayType *textarray)
{
    Datum       *elems;
    int            nelems,
                i;
    List       *res = NIL;

    deconstruct_array(textarray,
                      TEXTOID, -1, false, 'i',
                      &elems, NULL, &nelems);

    if (nelems == 0)
        return NIL;

    for (i = 0; i < nelems; i++)
        res = lappend(res, makeString(TextDatumGetCString(elems[i])));

    return res;
}

/*
 * Set the state of a subscription table.
 *
 * If update_only is true and the record for given table doesn't exist, do
 * nothing.  This can be used to avoid inserting a new record that was deleted
 * by someone else.  Generally, subscription DDL commands should use false,
 * workers should use true.
 *
 * The insert-or-update logic in this function is not concurrency safe so it
 * might raise an error in rare circumstances.  But if we took a stronger lock
 * such as ShareRowExclusiveLock, we would risk more deadlocks.
 */
Oid
SetSubscriptionRelState(Oid subid, Oid relid, char state,
                        XLogRecPtr sublsn, bool update_only, bool skip_is_exist)
{
    Relation    rel;
    HeapTuple    tup;
    Oid            subrelid = InvalidOid;
    bool        nulls[Natts_pg_subscription_rel];
    Datum        values[Natts_pg_subscription_rel];

    LockSharedObject(SubscriptionRelationId, subid, 0, AccessShareLock);

    rel = heap_open(SubscriptionRelRelationId, RowExclusiveLock);

    /* Try finding existing mapping. */
    tup = SearchSysCacheCopy2(SUBSCRIPTIONRELMAP,
                              ObjectIdGetDatum(relid),
                              ObjectIdGetDatum(subid));

    /* duplicated entey found, skip if needed */
    if (skip_is_exist)
    {
        if (HeapTupleIsValid(tup))
        {
            /* Cleanup. */
            ReleaseSysCache(tup);
            heap_close(rel, NoLock);

            return subrelid;
        }
    }

    /*
     * If the record for given table does not exist yet create new record,
     * otherwise update the existing one.
     */
    if (!HeapTupleIsValid(tup) && !update_only)
    {
        /* Form the tuple. */
        memset(values, 0, sizeof(values));
        memset(nulls, false, sizeof(nulls));
        values[Anum_pg_subscription_rel_srsubid - 1] = ObjectIdGetDatum(subid);
        values[Anum_pg_subscription_rel_srrelid - 1] = ObjectIdGetDatum(relid);
        values[Anum_pg_subscription_rel_srsubstate - 1] = CharGetDatum(state);
        if (sublsn != InvalidXLogRecPtr)
            values[Anum_pg_subscription_rel_srsublsn - 1] = LSNGetDatum(sublsn);
        else
            nulls[Anum_pg_subscription_rel_srsublsn - 1] = true;

        tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);

        /* Insert tuple into catalog. */
        subrelid = CatalogTupleInsert(rel, tup);

        heap_freetuple(tup);
    }
    else if (HeapTupleIsValid(tup))
    {
        bool        replaces[Natts_pg_subscription_rel];

        /* Update the tuple. */
        memset(values, 0, sizeof(values));
        memset(nulls, false, sizeof(nulls));
        memset(replaces, false, sizeof(replaces));

        replaces[Anum_pg_subscription_rel_srsubstate - 1] = true;
        values[Anum_pg_subscription_rel_srsubstate - 1] = CharGetDatum(state);

        replaces[Anum_pg_subscription_rel_srsublsn - 1] = true;
        if (sublsn != InvalidXLogRecPtr)
            values[Anum_pg_subscription_rel_srsublsn - 1] = LSNGetDatum(sublsn);
        else
            nulls[Anum_pg_subscription_rel_srsublsn - 1] = true;

        tup = heap_modify_tuple(tup, RelationGetDescr(rel), values, nulls,
                                replaces);

        /* Update the catalog. */
        CatalogTupleUpdate(rel, &tup->t_self, tup);

        subrelid = HeapTupleGetOid(tup);
    }

    /* Cleanup. */
    heap_close(rel, NoLock);

    return subrelid;
}

/*
 * Get state of subscription table.
 *
 * Returns SUBREL_STATE_UNKNOWN when not found and missing_ok is true.
 */
char
GetSubscriptionRelState(Oid subid, Oid relid, XLogRecPtr *sublsn,
                        bool missing_ok)
{
    Relation    rel;
    HeapTuple    tup;
    char        substate;
    bool        isnull;
    Datum        d;

    rel = heap_open(SubscriptionRelRelationId, AccessShareLock);

    /* Try finding the mapping. */
    tup = SearchSysCache2(SUBSCRIPTIONRELMAP,
                          ObjectIdGetDatum(relid),
                          ObjectIdGetDatum(subid));

    if (!HeapTupleIsValid(tup))
    {
        if (missing_ok)
        {
            heap_close(rel, AccessShareLock);
            *sublsn = InvalidXLogRecPtr;
            return SUBREL_STATE_UNKNOWN;
        }

        elog(ERROR, "subscription table %u in subscription %u does not exist",
             relid, subid);
    }

    /* Get the state. */
    d = SysCacheGetAttr(SUBSCRIPTIONRELMAP, tup,
                        Anum_pg_subscription_rel_srsubstate, &isnull);
    Assert(!isnull);
    substate = DatumGetChar(d);
    d = SysCacheGetAttr(SUBSCRIPTIONRELMAP, tup,
                        Anum_pg_subscription_rel_srsublsn, &isnull);
    if (isnull)
        *sublsn = InvalidXLogRecPtr;
    else
        *sublsn = DatumGetLSN(d);

    /* Cleanup */
    ReleaseSysCache(tup);
    heap_close(rel, AccessShareLock);

    return substate;
}

/*
 * Drop subscription relation mapping. These can be for a particular
 * subscription, or for a particular relation, or both.
 */
void
RemoveSubscriptionRel(Oid subid, Oid relid)
{
    Relation    rel;
    HeapScanDesc scan;
    ScanKeyData skey[2];
    HeapTuple    tup;
    int            nkeys = 0;

    rel = heap_open(SubscriptionRelRelationId, RowExclusiveLock);

    if (OidIsValid(subid))
    {
        ScanKeyInit(&skey[nkeys++],
                    Anum_pg_subscription_rel_srsubid,
                    BTEqualStrategyNumber,
                    F_OIDEQ,
                    ObjectIdGetDatum(subid));
    }

    if (OidIsValid(relid))
    {
        ScanKeyInit(&skey[nkeys++],
                    Anum_pg_subscription_rel_srrelid,
                    BTEqualStrategyNumber,
                    F_OIDEQ,
                    ObjectIdGetDatum(relid));
    }

    /* Do the search and delete what we found. */
    scan = heap_beginscan_catalog(rel, nkeys, skey);
    while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
    {
        CatalogTupleDelete(rel, &tup->t_self);
    }
    heap_endscan(scan);

    heap_close(rel, RowExclusiveLock);
}


/*
 * Get all relations for subscription.
 *
 * Returned list is palloc'ed in current memory context.
 */
List *
GetSubscriptionRelations(Oid subid)
{
    List       *res = NIL;
    Relation    rel;
    HeapTuple    tup;
    int            nkeys = 0;
    ScanKeyData skey[2];
    SysScanDesc scan;

    rel = heap_open(SubscriptionRelRelationId, AccessShareLock);

    ScanKeyInit(&skey[nkeys++],
                Anum_pg_subscription_rel_srsubid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(subid));

    scan = systable_beginscan(rel, InvalidOid, false,
                              NULL, nkeys, skey);

    while (HeapTupleIsValid(tup = systable_getnext(scan)))
    {
        Form_pg_subscription_rel subrel;
        SubscriptionRelState *relstate;

        subrel = (Form_pg_subscription_rel) GETSTRUCT(tup);

        relstate = (SubscriptionRelState *) palloc(sizeof(SubscriptionRelState));
        relstate->relid = subrel->srrelid;
        relstate->state = subrel->srsubstate;
        relstate->lsn = subrel->srsublsn;

        res = lappend(res, relstate);
    }

    /* Cleanup */
    systable_endscan(scan);
    heap_close(rel, AccessShareLock);

    return res;
}

/*
 * Get all relations for subscription that are not in a ready state.
 *
 * Returned list is palloc'ed in current memory context.
 */
List *
GetSubscriptionNotReadyRelations(Oid subid)
{
    List       *res = NIL;
    Relation    rel;
    HeapTuple    tup;
    int            nkeys = 0;
    ScanKeyData skey[2];
    SysScanDesc scan;

    rel = heap_open(SubscriptionRelRelationId, AccessShareLock);

    ScanKeyInit(&skey[nkeys++],
                Anum_pg_subscription_rel_srsubid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(subid));

    ScanKeyInit(&skey[nkeys++],
                Anum_pg_subscription_rel_srsubstate,
                BTEqualStrategyNumber, F_CHARNE,
                CharGetDatum(SUBREL_STATE_READY));

    scan = systable_beginscan(rel, InvalidOid, false,
                              NULL, nkeys, skey);

    while (HeapTupleIsValid(tup = systable_getnext(scan)))
    {
        Form_pg_subscription_rel subrel;
        SubscriptionRelState *relstate;

        subrel = (Form_pg_subscription_rel) GETSTRUCT(tup);

        relstate = (SubscriptionRelState *) palloc(sizeof(SubscriptionRelState));
        relstate->relid = subrel->srrelid;
        relstate->state = subrel->srsubstate;
        relstate->lsn = subrel->srsublsn;

        res = lappend(res, relstate);
    }

    /* Cleanup */
    systable_endscan(scan);
    heap_close(rel, AccessShareLock);

    return res;
}
#ifdef __STORAGE_SCALABLE__
/*
 * Insert new subscription / shard mapping.
 */
ObjectAddress
subscription_add_shard(char * subname, Oid subid, int32 shardid, char *pubname,
                         bool if_not_exists)
{
    Relation    rel;
    HeapTuple    tup;
    Datum        values[Natts_pg_subscription_shard];
    bool        nulls[Natts_pg_subscription_shard];
    Oid            srshardid;
    ObjectAddress myself,
                referenced;

    rel = heap_open(SubscriptionShardRelationId, RowExclusiveLock);

    /*
     * Check for duplicates. Note that this does not really prevent
     * duplicates, it's here just to provide nicer error message in common
     * case. The real protection is the unique key on the catalog.
     */
    if (SearchSysCacheExists3(SUBSCRIPTIONSHARDMAP, ObjectIdGetDatum(subid),
                              Int32GetDatum(shardid), CStringGetDatum(pubname)))
    {
        heap_close(rel, RowExclusiveLock);

        if (if_not_exists)
            return InvalidObjectAddress;

        ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_OBJECT),
                 errmsg("shard \"%d\" is already member of subscription \"%s\" for publication \"%s\" ",
                        shardid, subname, pubname)));
    }

    /* check shardid */
    if (!ShardIDIsValid(shardid))
    {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
             errmsg("\"%d\" is a invalid shardid",
                    shardid),
             errdetail("Invalid shardid cannot be added to subscriptions.")));
    }

    /* Form a tuple. */
    memset(values, 0, sizeof(values));
    memset(nulls, false, sizeof(nulls));

    values[Anum_pg_subscription_shard_srsubid - 1] =
        ObjectIdGetDatum(subid);
    values[Anum_pg_subscription_shard_srshardid - 1] =
        Int32GetDatum(shardid);
    values[Anum_pg_subscription_shard_pubname - 1] =
        DirectFunctionCall1(namein, CStringGetDatum(pubname));

    tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);

    /* Insert tuple into catalog. */
    srshardid = CatalogTupleInsert(rel, tup);
    heap_freetuple(tup);

    ObjectAddressSet(myself, SubscriptionShardRelationId, srshardid);

    /* Add dependency on the subscription */
    ObjectAddressSet(referenced, SubscriptionRelationId, subid);
    recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);

    /* Close the table. */
    heap_close(rel, RowExclusiveLock);

    return myself;
}

/*
 * Insert new subscription / table mapping.
 */
ObjectAddress
subscription_add_table(char * subname, Oid subid, Oid relid, char *pubname,
                         bool if_not_exists)
{
    Relation    rel;
    HeapTuple    tup;
    Datum        values[Natts_pg_subscription_table];
    bool        nulls[Natts_pg_subscription_table];
    Oid            srshardid;
    ObjectAddress myself,
                referenced;

    rel = heap_open(SubscriptionTableRelationId, RowExclusiveLock);

    /*
     * Check for duplicates. Note that this does not really prevent
     * duplicates, it's here just to provide nicer error message in common
     * case. The real protection is the unique key on the catalog.
     */
    if (SearchSysCacheExists3(SUBSCRIPTIONTABLEMAP, ObjectIdGetDatum(subid),
                              ObjectIdGetDatum(relid), CStringGetDatum(pubname)))
    {
        heap_close(rel, RowExclusiveLock);

        if (if_not_exists)
            return InvalidObjectAddress;

        ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_OBJECT),
                 errmsg("table \"%d\" is already member of subscription \"%s\" for publication \"%s\" ",
                        relid, subname, pubname)));
    }

    /* Form a tuple. */
    memset(values, 0, sizeof(values));
    memset(nulls, false, sizeof(nulls));

    values[Anum_pg_subscription_table_srsubid - 1] =
        ObjectIdGetDatum(subid);
    values[Anum_pg_subscription_table_srrelid - 1] =
        ObjectIdGetDatum(relid);
    values[Anum_pg_subscription_table_pubname - 1] =
        DirectFunctionCall1(namein, CStringGetDatum(pubname));

    tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);

    /* Insert tuple into catalog. */
    srshardid = CatalogTupleInsert(rel, tup);
    heap_freetuple(tup);

    ObjectAddressSet(myself, SubscriptionTableRelationId, srshardid);

    /* Add dependency on the subscription */
    ObjectAddressSet(referenced, SubscriptionRelationId, subid);
    recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);

    /* Close the table. */
    heap_close(rel, RowExclusiveLock);

    return myself;
}
/*
 * Get all shards for subscription.
 *
 * Returned list is palloc'ed in current memory context.
 */
List *
GetSubscriptionShards(Oid subid, char *pubname)
{
    int         i = 0;
    List       *res = NIL;
    HeapTuple    tuple;
    Form_pg_subscription_shard shard;
    CatCList   *catlist;

    catlist = SearchSysCacheList2(SUBSCRIPTIONSHARDPUBNAME,
                                  ObjectIdGetDatum(subid),
                                  PointerGetDatum(pubname));

    for (i = 0; i < catlist->n_members; i++)
    {
        tuple = &catlist->members[i]->tuple;
        shard = (Form_pg_subscription_shard) GETSTRUCT(tuple);

        res = lappend_int(res, shard->srshardid);
    }
    
    ReleaseSysCacheList(catlist);
    
    return res;
}

/*
 * get shard info of relation with subscription
 * subscription can subscribe to more than one publication,
 * each publication has different shards for each table,
 * we merge shards from different publication for table.
 */
List *
GetSubscriptionRelShards(Oid subid, Oid relid)    
{
    bool  first  = false;
    List *shards = NULL;
    CatCList   *catlist;
    HeapTuple    tuple;
    Form_pg_subscription_table table;
    int            i;

    catlist = SearchSysCacheList2(SUBSCRIPTIONTABLE,
                                  ObjectIdGetDatum(subid),
                                  ObjectIdGetDatum(relid));

    first = false;
    for (i = 0; i < catlist->n_members; i++)
    {
        char *pubname = NULL;
        List *shard   = NULL;
        
        tuple = &catlist->members[i]->tuple;
        table = (Form_pg_subscription_table) GETSTRUCT(tuple);

        pubname = NameStr(table->pubname);

        shard = GetSubscriptionShards(subid, pubname);

        if (!first)
        {
            first = true;
            shards = copyObject(shard);
        }
        else
        {
            if (shards && shard)
            {
                shards = list_concat_unique_int(shards, shard);
            }
            else
            {
                list_free(shards);
                shards = NULL;
            }
        }

        list_free(shard);
    }

    
    ReleaseSysCacheList(catlist);

    return shards;
}

/*
 * Remove shard from subscription by mapping OID.
 */
void
RemoveSubscriptionShardById(Oid proid)
{
    Relation    rel;
    HeapTuple    tup;

    rel = heap_open(SubscriptionShardRelationId, RowExclusiveLock);

    tup = SearchSysCache1(SUBSCRIPTIONSHARD, ObjectIdGetDatum(proid));

    if (!HeapTupleIsValid(tup))
        elog(ERROR, "cache lookup failed for subscription shard %u",
             proid);

    CatalogTupleDelete(rel, &tup->t_self);

    ReleaseSysCache(tup);

    heap_close(rel, RowExclusiveLock);
}

/*
 * Remove table from subscription by mapping OID.
 */
void
RemoveSubscriptionTableById(Oid proid)
{
    Relation    rel;
    HeapTuple    tup;

    rel = heap_open(SubscriptionTableRelationId, RowExclusiveLock);

    tup = SearchSysCache1(SUBSCRIPTIONTABLEOID, ObjectIdGetDatum(proid));

    if (!HeapTupleIsValid(tup))
        elog(ERROR, "cache lookup failed for subscription table %u",
             proid);

    CatalogTupleDelete(rel, &tup->t_self);

    ReleaseSysCache(tup);

    heap_close(rel, RowExclusiveLock);
}

char *
GetSubscriptionShardDesc(Oid proid)
{
    Relation    rel;
    HeapTuple    tup;
    Form_pg_subscription_shard subshard;
    StringInfoData str_desc;

    initStringInfo(&str_desc);

    rel = heap_open(SubscriptionShardRelationId, RowExclusiveLock);

    tup = SearchSysCache1(SUBSCRIPTIONSHARD, ObjectIdGetDatum(proid));

    if (!HeapTupleIsValid(tup))
    {
        appendStringInfoString(&str_desc, "invalid tuple oid in pg_subscription_shard ");
    }
    else
    {
        subshard = (Form_pg_subscription_shard) GETSTRUCT(tup);

        appendStringInfo(&str_desc, "shard %d in subscription %s", subshard->srshardid, get_subscription_name(subshard->srsubid));

        ReleaseSysCache(tup);
    }

    heap_close(rel, RowExclusiveLock);

    return str_desc.data;
}

char *
GetSubscriptionTableDesc(Oid proid)
{
    Relation    rel;
    HeapTuple    tup;
    Form_pg_subscription_table subtable;
    StringInfoData str_desc;

    initStringInfo(&str_desc);

    rel = heap_open(SubscriptionTableRelationId, RowExclusiveLock);

    tup = SearchSysCache1(SUBSCRIPTIONTABLEOID, ObjectIdGetDatum(proid));

    if (!HeapTupleIsValid(tup))
    {
        appendStringInfoString(&str_desc, "invalid tuple oid in pg_subscription_table ");
    }
    else
    {
        subtable = (Form_pg_subscription_table) GETSTRUCT(tup);

        appendStringInfo(&str_desc, "table %s in subscription %s", get_rel_name(subtable->srrelid), get_subscription_name(subtable->srsubid));

        ReleaseSysCache(tup);
    }

    heap_close(rel, RowExclusiveLock);

    return str_desc.data;
}

/*
 * Drop subscription shard mapping. These can be for a particular
 * subscription, or for a particular shard, or both.
 */
void
RemoveSubscriptionShard(Oid subid, int32 shardid)
{
    Relation    rel;
    HeapScanDesc scan;
    ScanKeyData skey[2];
    HeapTuple    tup;
    int            nkeys = 0;

    rel = heap_open(SubscriptionShardRelationId, RowExclusiveLock);

    if (OidIsValid(subid))
    {
        ScanKeyInit(&skey[nkeys++],
                    Anum_pg_subscription_shard_srsubid,
                    BTEqualStrategyNumber,
                    F_OIDEQ,
                    ObjectIdGetDatum(subid));
    }

    if (ShardIDIsValid(shardid))
    {
        ScanKeyInit(&skey[nkeys++],
                    Anum_pg_subscription_shard_srshardid,
                    BTEqualStrategyNumber,
                    F_INT4EQ,
                    Int32GetDatum(shardid));
    }

    /* Do the search and delete what we found. */
    scan = heap_beginscan_catalog(rel, nkeys, skey);
    while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
    {
        CatalogTupleDelete(rel, &tup->t_self);
    }
    heap_endscan(scan);

    heap_close(rel, RowExclusiveLock);
}

/*
 * Drop subscription table mapping. These can be for a particular
 * subscription, or for a particular table, or both.
 */
void
RemoveSubscriptionTable(Oid subid, Oid relid)
{
    Relation    rel;
    HeapScanDesc scan;
    ScanKeyData skey[2];
    HeapTuple    tup;
    int            nkeys = 0;

    rel = heap_open(SubscriptionTableRelationId, RowExclusiveLock);

    if (OidIsValid(subid))
    {
        ScanKeyInit(&skey[nkeys++],
                    Anum_pg_subscription_table_srsubid,
                    BTEqualStrategyNumber,
                    F_OIDEQ,
                    ObjectIdGetDatum(subid));
    }

    if (OidIsValid(relid))
    {
        ScanKeyInit(&skey[nkeys++],
                    Anum_pg_subscription_table_srrelid,
                    BTEqualStrategyNumber,
                    F_OIDEQ,
                    ObjectIdGetDatum(relid));
    }

    /* Do the search and delete what we found. */
    scan = heap_beginscan_catalog(rel, nkeys, skey);
    while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
    {
        CatalogTupleDelete(rel, &tup->t_self);
    }
    heap_endscan(scan);

    heap_close(rel, RowExclusiveLock);
}
#endif

#ifdef __SUBSCRIPTION__
/*
 * When the first tbase-sub-subscription completes the data COPY, 
 * it will activate the other tbase-sub-subscriptions.
 */
void ActiveAllParallelTbaseSubscriptions(XLogRecPtr active_lsn)
{// #lizard forgives
    Subscription * first_sub = MySubscription;
    Oid parent_sub_oid = InvalidOid;
    List * child_sub_oid_list = NIL;

    /* check if i am the first tbase-sub-subscription */
    Assert(MySubscription != NULL);
    if (!(MySubscription != NULL))
    {
        abort();
    }

    Assert(first_sub->parallel_number > 1 && first_sub->parallel_index == 0);
    if (!(first_sub->parallel_number > 1 && first_sub->parallel_index == 0))
    {
        abort();
    }

    PushActiveSnapshot(GetLocalTransactionSnapshot());

    /* Update tbase_subscription, set sub_is_all_actived to true */
    do
    {
        Relation         tbase_sub_rel = NULL;
        HeapScanDesc    scan = NULL;
        HeapTuple       tuple = NULL;
        TupleDesc        desc = NULL;
        int                nkeys = 0;
        ScanKeyData     skey[1];

        bool            found = false;

        ScanKeyInit(&skey[nkeys++],
                    Anum_tbase_subscription_sub_name,
                    BTEqualStrategyNumber, F_NAMEEQ,
                    CStringGetDatum(first_sub->parent_name));

        tbase_sub_rel = relation_openrv(makeRangeVar("public", 
                                        (char *)g_tbase_subscription_relname, -1), 
                                        RowExclusiveLock);
        desc = RelationGetDescr(tbase_sub_rel);
        scan = heap_beginscan(tbase_sub_rel, GetActiveSnapshot(), nkeys, skey);

        while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
        {
            bool    nulls[Natts_tbase_subscription] = { 0 };
            Datum    values[Natts_tbase_subscription] = { 0 };
            HeapTuple newtup = NULL;

            parent_sub_oid = HeapTupleGetOid(tuple);
            heap_deform_tuple(tuple, desc, values, nulls);

            values[Anum_tbase_subscription_sub_is_all_actived - 1] = BoolGetDatum(true);
            newtup = heap_form_tuple(desc, values, nulls);

            simple_heap_update(tbase_sub_rel, &tuple->t_self, newtup);
            heap_freetuple(newtup);

            found = true;
            break;
        }

        heap_endscan(scan);
        relation_close(tbase_sub_rel, RowExclusiveLock);

        Assert(found == true && OidIsValid(parent_sub_oid));
        if (!(found == true && OidIsValid(parent_sub_oid)))
        {
            abort();
        }
    } while (0);

    /* scan tbase_subscription_parallel, and set sub_active_state and sub_active_lsn */
    do
    {
        Relation         tbase_sub_parallel_rel = NULL;
        HeapScanDesc    scan = NULL;
        HeapTuple       tuple = NULL;
        TupleDesc        desc = NULL;
        int                nkeys = 0;
        ScanKeyData     skey[1];

        int32            sub_number = 0;

        ScanKeyInit(&skey[nkeys++],
                    Anum_tbase_subscription_parallel_sub_parent,
                    BTEqualStrategyNumber, F_OIDEQ,
                    ObjectIdGetDatum(parent_sub_oid));

        tbase_sub_parallel_rel = relation_openrv(makeRangeVar("public",
                                                    (char *)g_tbase_subscription_parallel_relname, -1),
                                                    RowExclusiveLock);
        desc = RelationGetDescr(tbase_sub_parallel_rel);
        scan = heap_beginscan(tbase_sub_parallel_rel, GetActiveSnapshot(), nkeys, skey);

        while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
        {
            Oid        sub_parent = InvalidOid;
            Oid        sub_child = InvalidOid;

            bool    nulls[Natts_tbase_subscription_parallel] = { 0 };
            Datum    values[Natts_tbase_subscription_parallel] = { 0 };
            HeapTuple newtup = NULL;

            heap_deform_tuple(tuple, desc, values, nulls);

            sub_parent = DatumGetObjectId(values[Anum_tbase_subscription_parallel_sub_parent - 1]);
            sub_child= DatumGetObjectId(values[Anum_tbase_subscription_parallel_sub_child - 1]);

            Assert(OidIsValid(sub_parent) && OidIsValid(sub_child) && sub_parent == parent_sub_oid);
            if (!(OidIsValid(sub_parent) && OidIsValid(sub_child) && sub_parent == parent_sub_oid))
            {
                abort();
            }

            values[Anum_tbase_subscription_parallel_sub_active_state - 1] = BoolGetDatum(true);
            values[Anum_tbase_subscription_parallel_sub_active_lsn - 1] = LSNGetDatum(active_lsn);
            newtup = heap_form_tuple(desc, values, nulls);

            simple_heap_update(tbase_sub_parallel_rel, &tuple->t_self, newtup);
            heap_freetuple(newtup);

            sub_number++;
            child_sub_oid_list = lappend_oid(child_sub_oid_list, sub_child);
            if (sub_number == first_sub->parallel_number)
            {
                break;
            }
        }

        heap_endscan(scan);
        relation_close(tbase_sub_parallel_rel, RowExclusiveLock);

        Assert(sub_number == first_sub->parallel_number && child_sub_oid_list != NIL && list_length(child_sub_oid_list) == sub_number);
        if (!(sub_number == first_sub->parallel_number && child_sub_oid_list != NIL && list_length(child_sub_oid_list) == sub_number))
        {
            abort();
        }
    } while (0);

    /* scan pg_subscitption, set subenabled to true */
    do
    {
        Relation    rel = NULL;
        ListCell    *lc = NULL;

        rel = heap_open(SubscriptionRelationId, RowExclusiveLock);

        foreach(lc, child_sub_oid_list)
        {
            bool        nulls[Natts_pg_subscription] = { false };
            bool        replaces[Natts_pg_subscription] = { false };
            Datum        values[Natts_pg_subscription] = { 0 };
            Oid            subid = lfirst_oid(lc);
            HeapTuple    tup = NULL;
            Form_pg_subscription subform = NULL;

            /* Fetch the existing tuple. */
            tup = SearchSysCacheCopy1(SUBSCRIPTIONOID, ObjectIdGetDatum(subid));

            Assert(HeapTupleIsValid(tup));
            if (!HeapTupleIsValid(tup)) 
            {
                abort();
            }
            
            subform = (Form_pg_subscription) GETSTRUCT(tup);

            /* must be owner */
            if (!pg_subscription_ownercheck(HeapTupleGetOid(tup), GetUserId()))
                aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_SUBSCRIPTION,
                               NameStr(subform->subname));

            /* Lock the subscription so nobody else can do anything with it. */
            LockSharedObject(SubscriptionRelationId, subid, 0, AccessExclusiveLock);

            values[Anum_pg_subscription_subenabled - 1] = BoolGetDatum(true);
            replaces[Anum_pg_subscription_subenabled - 1] = true;

            tup = heap_modify_tuple(tup, RelationGetDescr(rel), values, nulls, replaces);
            CatalogTupleUpdate(rel, &tup->t_self, tup);
            heap_freetuple(tup);

            UnlockSharedObject(SubscriptionRelationId, subid, 0, AccessExclusiveLock);
        }

        heap_close(rel, RowExclusiveLock);
    } while (0);

    /* update MySubscription data */
    do
    {
        first_sub->is_all_actived = true;
        first_sub->active_state = true;
        first_sub->active_lsn = active_lsn;

        PopActiveSnapshot();
        CommandCounterIncrement();
    } while (0);
}


/*
 * Get all parallel sub_child_ids with the same parent ID as the specified subid.
 *
 * Returned list is palloc'ed in current memory context.
 */
List *
GetTbaseSubscriptnParallelChild(Oid subid)
{
	List	   *res = NIL;
	int				nkeys = 0;
	ScanKeyData 	skey[1];
	Relation 		tbase_sub_parallel_rel = NULL;
	HeapScanDesc	scan = NULL;
	HeapTuple   	tuple = NULL;
	TupleDesc		desc = NULL;

	bool			found = false;
	bool		    isnull = false;

	Oid 	sub_parent_oid = InvalidOid;
	int32	sub_index = -1;

	PushActiveSnapshot(GetLocalTransactionSnapshot());

	/* get sub_parent_oid in tbase_subscription_parallel */
	ScanKeyInit(&skey[nkeys++],
				Anum_tbase_subscription_parallel_sub_child,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(subid));

	tbase_sub_parallel_rel = relation_openrv(makeRangeVar("public",
											(char *)g_tbase_subscription_parallel_relname, -1),
											AccessShareLock);
	desc = RelationGetDescr(tbase_sub_parallel_rel);
	scan = heap_beginscan(tbase_sub_parallel_rel, GetActiveSnapshot(), nkeys, skey);

	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Oid		sub_child = InvalidOid;

		sub_child = DatumGetObjectId(fastgetattr(tuple, Anum_tbase_subscription_parallel_sub_child, desc, &isnull));
		if (false == isnull && subid == sub_child)
		{
			found = true;
			sub_parent_oid = DatumGetObjectId(fastgetattr(tuple, Anum_tbase_subscription_parallel_sub_parent, desc, &isnull));
			sub_index = DatumGetInt32(fastgetattr(tuple, Anum_tbase_subscription_parallel_sub_index, desc, &isnull));
			break;
		}
	}
	heap_endscan(scan);

	Assert(found == true && sub_parent_oid != InvalidOid && sub_index >= 0);
	if (!(found == true && sub_parent_oid != InvalidOid && sub_index >= 0))
	{
		abort();
	}

	/* get sub_child_oid in tbase_subscription_parallel with sub_parent_oid */
	nkeys = 0;
	ScanKeyInit(&skey[nkeys++],
				Anum_tbase_subscription_parallel_sub_parent,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(sub_parent_oid));

	scan = heap_beginscan(tbase_sub_parallel_rel, GetActiveSnapshot(), nkeys, skey);

	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Oid		parent_oid = InvalidOid;

		parent_oid = DatumGetObjectId(fastgetattr(tuple, Anum_tbase_subscription_parallel_sub_parent, desc, &isnull));
		if (false == isnull && sub_parent_oid == parent_oid)
		{
			Oid sub_child_oid;

			sub_child_oid = DatumGetObjectId(fastgetattr(tuple, Anum_tbase_subscription_parallel_sub_child, desc, &isnull));

			res = lappend_oid(res, sub_child_oid);
		}
	}
	/* Cleanup */
	heap_endscan(scan);
	relation_close(tbase_sub_parallel_rel, AccessShareLock);

	PopActiveSnapshot();

	return res;
}

/*
 * Get all parallel LogicalRepWorker with the same parent ID as the specified subid.
 *
 */
List *
GetTbaseSubscriptnParallelWorker(Oid subid)
{
	List *res = NIL;
	ListCell   *lc_simple;
	LogicalRepWorker *syncworker;

	List *parallel_childids_list = GetTbaseSubscriptnParallelChild(subid);

	/*
	 * Look for a sync worker for this relation.
	 */
	LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);

	foreach (lc_simple, parallel_childids_list)
	{
		Oid sub_oid = lfirst_oid(lc_simple);
		if (sub_oid != subid)
		{
			syncworker = logicalrep_worker_find(sub_oid, InvalidOid, false);

			if (syncworker)
			{
				res = lappend(res, syncworker);
			}
		}

	}
	LWLockRelease(LogicalRepWorkerLock);

	list_free(parallel_childids_list);

	return res;
}

#endif
