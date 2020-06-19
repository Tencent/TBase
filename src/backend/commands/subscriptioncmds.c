/*-------------------------------------------------------------------------
 *
 * subscriptioncmds.c
 *        subscription catalog manipulation functions
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *        subscriptioncmds.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"

#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_type.h"
#include "catalog/pg_subscription.h"
#include "catalog/pg_subscription_rel.h"

#include "commands/defrem.h"
#include "commands/event_trigger.h"
#include "commands/subscriptioncmds.h"

#include "executor/executor.h"

#include "nodes/makefuncs.h"

#include "replication/logicallauncher.h"
#include "replication/origin.h"
#include "replication/walreceiver.h"
#include "replication/walsender.h"
#include "replication/worker_internal.h"

#include "storage/lmgr.h"

#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

#ifdef __STORAGE_SCALABLE__
#include "replication/logical_statistic.h"
#include "fmgr.h"
#endif

#ifdef __SUBSCRIPTION__
#include "commands/extension.h"
#include "utils/tqual.h"
#include "utils/pg_lsn.h"
#include "utils/snapmgr.h"
#endif

#ifdef __SUBSCRIPTION__
const char * g_tbase_subscription_extension = "tbase_subscription";
const char * g_tbase_subscription_relname = "tbase_subscription";
const char * g_tbase_subscription_parallel_relname = "tbase_subscription_parallel";
#endif

#ifdef __STORAGE_SCALABLE__
typedef struct
{
    int32 shardid;
    char  *pubname;
} PubShard;
#endif

static List *fetch_table_list(WalReceiverConn *wrconn, List *publications);
#ifdef __STORAGE_SCALABLE__
static List *fetch_shard_list(WalReceiverConn *wrconn, List *publications);
#endif

#ifdef __SUBSCRIPTION__
static void
parse_tbase_subscription_options(List *options,
                                 bool is_create_stmt,
                                 bool *ignore_pk_conflict,
                                 char **manual_hot_date,
                                 char **temp_hot_date,
                                 char **temp_cold_date,
                                 int *parallel_number,
								 bool *copy_data,
								 char **slot_name,
								 bool *slot_name_given);
static bool check_tbase_subscription_ifexists(Relation tbase_sub_rel, char * check_subname);
static List * tbase_subscription_parallelization(Node * stmt, int parallel_number, bool slot_name_given);
#endif

/*
 * Common option parsing function for CREATE and ALTER SUBSCRIPTION commands.
 *
 * Since not all options can be specified in both commands, this function
 * will report an error on options if the target output pointer is NULL to
 * accommodate that.
 */
static void
parse_subscription_options(List *options, bool *connect, bool *enabled_given,
                           bool *enabled, bool *create_slot,
                           bool *slot_name_given, char **slot_name,
                           bool *copy_data, char **synchronous_commit,
                           bool *refresh)
{// #lizard forgives
    ListCell   *lc;
    bool        connect_given = false;
    bool        create_slot_given = false;
    bool        copy_data_given = false;
    bool        refresh_given = false;

    /* If connect is specified, the others also need to be. */
    Assert(!connect || (enabled && create_slot && copy_data));

    if (connect)
        *connect = true;
    if (enabled)
    {
        *enabled_given = false;
        *enabled = true;
    }
    if (create_slot)
        *create_slot = true;
    if (slot_name)
    {
        *slot_name_given = false;
        *slot_name = NULL;
    }
    if (copy_data)
        *copy_data = true;
    if (synchronous_commit)
        *synchronous_commit = NULL;
    if (refresh)
        *refresh = true;

    /* Parse options */
    foreach(lc, options)
    {
        DefElem    *defel = (DefElem *) lfirst(lc);

        if (strcmp(defel->defname, "connect") == 0 && connect)
        {
            if (connect_given)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("conflicting or redundant options")));

            connect_given = true;
            *connect = defGetBoolean(defel);
        }
        else if (strcmp(defel->defname, "enabled") == 0 && enabled)
        {
            if (*enabled_given)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("conflicting or redundant options")));

            *enabled_given = true;
            *enabled = defGetBoolean(defel);
        }
        else if (strcmp(defel->defname, "create_slot") == 0 && create_slot)
        {
            if (create_slot_given)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("conflicting or redundant options")));

            create_slot_given = true;
            *create_slot = defGetBoolean(defel);
        }
        else if (strcmp(defel->defname, "slot_name") == 0 && slot_name)
        {
            if (*slot_name_given)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("conflicting or redundant options")));

            *slot_name_given = true;
            *slot_name = defGetString(defel);

            /* Setting slot_name = NONE is treated as no slot name. */
            if (strcmp(*slot_name, "none") == 0)
                *slot_name = NULL;
        }
        else if (strcmp(defel->defname, "copy_data") == 0 && copy_data)
        {
            if (copy_data_given)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("conflicting or redundant options")));

            copy_data_given = true;
            *copy_data = defGetBoolean(defel);
        }
        else if (strcmp(defel->defname, "synchronous_commit") == 0 &&
                 synchronous_commit)
        {
            if (*synchronous_commit)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("conflicting or redundant options")));

            *synchronous_commit = defGetString(defel);

            /* Test if the given value is valid for synchronous_commit GUC. */
            (void) set_config_option("synchronous_commit", *synchronous_commit,
                                     PGC_BACKEND, PGC_S_TEST, GUC_ACTION_SET,
                                     false, 0, false);
        }
        else if (strcmp(defel->defname, "refresh") == 0 && refresh)
        {
            if (refresh_given)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("conflicting or redundant options")));

            refresh_given = true;
            *refresh = defGetBoolean(defel);
        }
#ifdef __SUBSCRIPTION__
        else if (strcmp(defel->defname, "ignore_pk_conflict") == 0 ||
                 strcmp(defel->defname, "manual_hot_date") == 0 ||
                 strcmp(defel->defname, "temp_hot_date") == 0 ||
                 strcmp(defel->defname, "temp_cold_date") == 0 ||
                 strcmp(defel->defname, "parallel_number") == 0)
        {
            /* parse in parse_tbase_subscription_options */
        }
#endif
        else
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("unrecognized subscription parameter: %s", defel->defname)));
    }

    /*
     * We've been explicitly asked to not connect, that requires some
     * additional processing.
     */
    if (connect && !*connect)
    {
        /* Check for incompatible options from the user. */
        if (enabled && *enabled_given && *enabled)
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("connect = false and enabled = true are mutually exclusive options")));

        if (create_slot && create_slot_given && *create_slot)
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("connect = false and create_slot = true are mutually exclusive options")));

        if (copy_data && copy_data_given && *copy_data)
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("connect = false and copy_data = true are mutually exclusive options")));

        /* Change the defaults of other options. */
        *enabled = false;
        *create_slot = false;
        *copy_data = false;
    }

    /*
     * Do additional checking for disallowed combination when slot_name = NONE
     * was used.
     */
    if (slot_name && *slot_name_given && !*slot_name)
    {
        if (enabled && *enabled_given && *enabled)
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("slot_name = NONE and enabled = true are mutually exclusive options")));

        if (create_slot && create_slot_given && *create_slot)
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("slot_name = NONE and create_slot = true are mutually exclusive options")));

        if (enabled && !*enabled_given && *enabled)
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("subscription with slot_name = NONE must also set enabled = false")));

        if (create_slot && !create_slot_given && *create_slot)
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("subscription with slot_name = NONE must also set create_slot = false")));
    }
}

/*
 * Auxiliary function to return a text array out of a list of String nodes.
 */
static Datum
publicationListToArray(List *publist)
{
    ArrayType  *arr;
    Datum       *datums;
    int            j = 0;
    ListCell   *cell;
    MemoryContext memcxt;
    MemoryContext oldcxt;

    /* Create memory context for temporary allocations. */
    memcxt = AllocSetContextCreate(CurrentMemoryContext,
                                   "publicationListToArray to array",
                                   ALLOCSET_DEFAULT_MINSIZE,
                                   ALLOCSET_DEFAULT_INITSIZE,
                                   ALLOCSET_DEFAULT_MAXSIZE);
    oldcxt = MemoryContextSwitchTo(memcxt);

    datums = palloc(sizeof(text *) * list_length(publist));
    foreach(cell, publist)
    {
        char       *name = strVal(lfirst(cell));
        ListCell   *pcell;

        /* Check for duplicates. */
        foreach(pcell, publist)
        {
            char       *pname = strVal(lfirst(pcell));

            if (name == pname)
                break;

            if (strcmp(name, pname) == 0)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("publication name \"%s\" used more than once",
                                pname)));
        }

        datums[j++] = CStringGetTextDatum(name);
    }

    MemoryContextSwitchTo(oldcxt);

    arr = construct_array(datums, list_length(publist),
                          TEXTOID, -1, false, 'i');
    MemoryContextDelete(memcxt);

    return PointerGetDatum(arr);
}

/*
 * Create new subscription.
 */
#ifdef __SUBSCRIPTION__
ObjectAddress
CreateSubscription(CreateSubscriptionStmt *stmt, bool isTopLevel, bool force_to_disable)
#else
ObjectAddress
CreateSubscription(CreateSubscriptionStmt *stmt, bool isTopLevel)
#endif
{// #lizard forgives
    Relation    rel;
    ObjectAddress myself;
    Oid            subid;
    bool        nulls[Natts_pg_subscription];
    Datum        values[Natts_pg_subscription];
    Oid            owner = GetUserId();
    HeapTuple    tup;
    bool        connect;
    bool        enabled_given;
    bool        enabled;
    bool        copy_data;
    char       *synchronous_commit;
    char       *conninfo;
    char       *slotname;
    bool        slotname_given;
    char        originname[NAMEDATALEN];
    bool        create_slot;
    List       *publications;

    /*
     * Parse and check options.
     *
     * Connection and publication should not be specified here.
     */
    parse_subscription_options(stmt->options, &connect, &enabled_given,
                               &enabled, &create_slot, &slotname_given,
                               &slotname, &copy_data, &synchronous_commit,
                               NULL);

    /*
     * Since creating a replication slot is not transactional, rolling back
     * the transaction leaves the created replication slot.  So we cannot run
     * CREATE SUBSCRIPTION inside a transaction block if creating a
     * replication slot.
     */
    if (create_slot)
        PreventTransactionChain(isTopLevel, "CREATE SUBSCRIPTION ... WITH (create_slot = true)");

    if (!superuser())
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 (errmsg("must be superuser to create subscriptions"))));

    rel = heap_open(SubscriptionRelationId, RowExclusiveLock);

    /* Check if name is used */
    subid = GetSysCacheOid2(SUBSCRIPTIONNAME, MyDatabaseId,
                            CStringGetDatum(stmt->subname));
    if (OidIsValid(subid))
    {
        ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_OBJECT),
                 errmsg("subscription \"%s\" already exists",
                        stmt->subname)));
    }

    if (!slotname_given && slotname == NULL)
        slotname = stmt->subname;

    /* The default for synchronous_commit of subscriptions is off. */
    if (synchronous_commit == NULL)
        synchronous_commit = "off";

    conninfo = stmt->conninfo;
    publications = stmt->publication;

#ifdef __SUBSCRIPTION__
    /* add parallel info into conninfo */
    if (conninfo != NULL && IsTbaseSubscription((Node *)stmt))
    {
        StringInfoData conn_str;
        initStringInfo(&conn_str);
        appendStringInfo(&conn_str, 
            "%s sub_parallel_number=%d sub_parallel_index=%d",
            conninfo, stmt->sub_parallel_number, stmt->sub_parallel_index);
        conninfo = conn_str.data;

        Assert(stmt->sub_parallel_number >= 1 && stmt->sub_parallel_index >= 0);
        if (!((stmt->sub_parallel_number >= 1 && stmt->sub_parallel_index >= 0))) abort();

        Assert(stmt->sub_parallel_number > stmt->sub_parallel_index);
        if (!((stmt->sub_parallel_number > stmt->sub_parallel_index))) abort();
    }
#endif

    /* Load the library providing us libpq calls. */
    load_file("libpqwalreceiver", false);

    /* Check the connection info string. */
    walrcv_check_conninfo(conninfo);

    /* Everything ok, form a new tuple. */
    memset(values, 0, sizeof(values));
    memset(nulls, false, sizeof(nulls));

    values[Anum_pg_subscription_subdbid - 1] = ObjectIdGetDatum(MyDatabaseId);
    values[Anum_pg_subscription_subname - 1] =
        DirectFunctionCall1(namein, CStringGetDatum(stmt->subname));
    values[Anum_pg_subscription_subowner - 1] = ObjectIdGetDatum(owner);
    values[Anum_pg_subscription_subenabled - 1] = BoolGetDatum(enabled);

#ifdef __SUBSCRIPTION__
    if (force_to_disable == true)
    {
        values[Anum_pg_subscription_subenabled - 1] = BoolGetDatum(false);
    }

    values[Anum_pg_subscription_subconninfo - 1] = CStringGetTextDatum(stmt->conninfo);
#else
    values[Anum_pg_subscription_subconninfo - 1] =
        CStringGetTextDatum(conninfo);
#endif

    if (slotname)
        values[Anum_pg_subscription_subslotname - 1] =
            DirectFunctionCall1(namein, CStringGetDatum(slotname));
    else
        nulls[Anum_pg_subscription_subslotname - 1] = true;
    values[Anum_pg_subscription_subsynccommit - 1] =
        CStringGetTextDatum(synchronous_commit);
    values[Anum_pg_subscription_subpublications - 1] =
        publicationListToArray(publications);

    tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);

    /* Insert tuple into catalog. */
    subid = CatalogTupleInsert(rel, tup);
    heap_freetuple(tup);

    recordDependencyOnOwner(SubscriptionRelationId, subid, owner);

    snprintf(originname, sizeof(originname), "pg_%u", subid);
    replorigin_create(originname);

    /*
     * Connect to remote side to execute requested commands and fetch table
     * info.
     */
    if (connect)
    {
        XLogRecPtr    lsn;
        char       *err;
        WalReceiverConn *wrconn;
        List       *tables;
#ifdef __STORAGE_SCALABLE__
        List       *shards;
#endif
        ListCell   *lc;
        char        table_state;

        /* Try to connect to the publisher. */
        wrconn = walrcv_connect(conninfo, true, stmt->subname, &err);
        if (!wrconn)
            ereport(ERROR,
                    (errmsg("could not connect to the publisher: %s", err)));

        PG_TRY();
        {
            /*
             * Set sync state based on if we were asked to do data copy or
             * not.
             */
            table_state = copy_data ? SUBREL_STATE_INIT : SUBREL_STATE_READY;

            /*
             * Get the table list from publisher and build local table status
             * info.
             */
            tables = fetch_table_list(wrconn, publications);
#ifdef __STORAGE_SCALABLE__
            shards = fetch_shard_list(wrconn, publications);
#endif
            foreach(lc, tables)
            {
                RangeVar   *rv = (RangeVar *) lfirst(lc);
                Oid            relid;
                Relation    relation;

                relid = RangeVarGetRelid(rv, AccessShareLock, false);

#ifdef __STORAGE_SCALABLE__
                relation = heap_open(relid, NoLock);
                if (shards && !stmt->istbase)
                {
                    if (!RelationIsSharded(relation))
                    {
                        heap_close(relation, NoLock);

                        continue;
                    }
                }

                if (RELATION_IS_INTERVAL(relation))
                {
                    heap_close(relation, NoLock);

                    continue;
                }

#ifdef __SUBSCRIPTION__
                if (stmt->istbase)
                {
                    /* only support shard table */
                    if (relation->rd_locator_info == NULL ||
                        relation->rd_locator_info->locatorType != LOCATOR_TYPE_SHARD)
                    {
                        heap_close(relation, NoLock);
                        ereport(LOG,
                            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                             errmsg("TBase Subscripton currently only supports subscribing into the SHARD table, "
                                     "but \"%s\" is not a SHARD table, skip it", RangeVarGetName(rv))));
                        continue;
                    }
                }
#endif
                heap_close(relation, NoLock);
#endif

                /* Check for supported relkind. */
                CheckSubscriptionRelkind(get_rel_relkind(relid),
                                         rv->schemaname, rv->relname);

                SetSubscriptionRelState(subid, relid, table_state,
                                        InvalidXLogRecPtr, false, true);
#ifdef __STORAGE_SCALABLE__
                /* add subscription / table mapping with publication */
                subscription_add_table(stmt->subname, subid, relid, rv->pubname, true);
#endif
            }

#ifdef __STORAGE_SCALABLE__
            /*
             * Get the shard list from publisher and build local shard status
             * info.
             */
            foreach(lc, shards)
            {
                PubShard   *ps = (PubShard *) lfirst(lc);

                if (!ShardIDIsValid(ps->shardid))
                    ereport(ERROR,
                            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                             errmsg("logical replication target shard \"%d\" is invalid",
                                    ps->shardid)));

                subscription_add_shard(stmt->subname, subid, ps->shardid, ps->pubname, true);
            }
#endif

            /*
             * If requested, create permanent slot for the subscription. We
             * won't use the initial snapshot for anything, so no need to
             * export it.
             */
            if (create_slot)
            {
                Assert(slotname);

#ifdef __STORAGE_SCALABLE__
                /* send subscription's name and oid addition additionally */
				if (shards)
				{
					walrcv_create_slot(wrconn, slotname, false,
									   CRS_NOEXPORT_SNAPSHOT, &lsn, slotname, subid, NULL, NULL);
				}
				else
				{
					walrcv_create_slot(wrconn, slotname, false,
									   CRS_NOEXPORT_SNAPSHOT, &lsn, NULL, InvalidOid, NULL, NULL);
				}
#else
                walrcv_create_slot(wrconn, slotname, false,
                                   CRS_NOEXPORT_SNAPSHOT, &lsn);
#endif
                ereport(NOTICE,
                        (errmsg("created replication slot \"%s\" on publisher",
                                slotname)));
            }
        }
        PG_CATCH();
        {
            /* Close the connection in case of failure. */
            walrcv_disconnect(wrconn);
            PG_RE_THROW();
        }
        PG_END_TRY();

        /* And we are done with the remote side. */
        walrcv_disconnect(wrconn);
    }
    else
        ereport(WARNING,
                (errmsg("tables were not subscribed, you will have to run "
                        "ALTER SUBSCRIPTION ... REFRESH PUBLICATION to "
                        "subscribe the tables")));

    heap_close(rel, RowExclusiveLock);

    if (enabled)
        ApplyLauncherWakeupAtCommit();

    ObjectAddressSet(myself, SubscriptionRelationId, subid);

    InvokeObjectPostCreateHook(SubscriptionRelationId, subid, 0);

    return myself;
}

static void
AlterSubscription_refresh(Subscription *sub, bool copy_data)
{// #lizard forgives
    char       *err;
    List       *pubrel_names;
    List       *subrel_states;
    Oid           *subrel_local_oids;
    Oid           *pubrel_local_oids;
    ListCell   *lc;
    int            off;

    /* Load the library providing us libpq calls. */
    load_file("libpqwalreceiver", false);

    /* Try to connect to the publisher. */
    wrconn = walrcv_connect(sub->conninfo, true, sub->name, &err);
    if (!wrconn)
        ereport(ERROR,
                (errmsg("could not connect to the publisher: %s", err)));

    /* Get the table list from publisher. */
    pubrel_names = fetch_table_list(wrconn, sub->publications);

    /* We are done with the remote side, close connection. */
    walrcv_disconnect(wrconn);

#ifdef __STORAGE_SCALABLE__
    if (IS_PGXC_COORDINATOR)
    {
        List *shard_pubrel_names = NIL;

        foreach(lc, pubrel_names)
        {
            RangeVar   *rv = (RangeVar *) lfirst(lc);
            Oid            relid = InvalidOid;
            Relation    relation = NULL;

			relid = RangeVarGetRelid(rv, AccessShareLock, true);
			if (!OidIsValid(relid))
			{
				elog(LOG, "no local relation for remote relation %s", RangeVarGetName(rv));
				continue;
			}

            relation = heap_open(relid, NoLock);

            /* only support shard table */
            if (!RelationIsSharded(relation))
            {
                heap_close(relation, NoLock);
                ereport(LOG,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg("TBase Subscripton on COORDINATOR currently only supports subscribing into the SHARD table, "
                            "but \"%s\" is not a SHARD table, skip it", RangeVarGetName(rv))));
                continue;
            }

            if (RELATION_IS_INTERVAL(relation))
            {
                heap_close(relation, NoLock);
                continue;
            }

            heap_close(relation, NoLock);
            shard_pubrel_names = lappend(shard_pubrel_names, rv);
        }

        pubrel_names = shard_pubrel_names;
    }
#endif

    /* Get local table list. */
    subrel_states = GetSubscriptionRelations(sub->oid);

    /*
     * Build qsorted array of local table oids for faster lookup. This can
     * potentially contain all tables in the database so speed of lookup is
     * important.
     */
    subrel_local_oids = palloc(list_length(subrel_states) * sizeof(Oid));
    off = 0;
    foreach(lc, subrel_states)
    {
        SubscriptionRelState *relstate = (SubscriptionRelState *) lfirst(lc);

        subrel_local_oids[off++] = relstate->relid;
    }
    qsort(subrel_local_oids, list_length(subrel_states),
          sizeof(Oid), oid_cmp);

    /*
     * Walk over the remote tables and try to match them to locally known
     * tables. If the table is not known locally create a new state for it.
     *
     * Also builds array of local oids of remote tables for the next step.
     */
    off = 0;
    pubrel_local_oids = palloc(list_length(pubrel_names) * sizeof(Oid));

    foreach(lc, pubrel_names)
    {
        RangeVar   *rv = (RangeVar *) lfirst(lc);
        Oid            relid;

        relid = RangeVarGetRelid(rv, AccessShareLock, false);

        /* Check for supported relkind. */
        CheckSubscriptionRelkind(get_rel_relkind(relid),
                                 rv->schemaname, rv->relname);

        pubrel_local_oids[off++] = relid;

        if (!bsearch(&relid, subrel_local_oids,
                     list_length(subrel_states), sizeof(Oid), oid_cmp))
        {
            SetSubscriptionRelState(sub->oid, relid,
                                    copy_data ? SUBREL_STATE_INIT : SUBREL_STATE_READY,
                                    InvalidXLogRecPtr, false, true);
#ifdef __STORAGE_SCALABLE__
            /* add subscription / table mapping with publication */
            subscription_add_table(sub->name, sub->oid, relid, rv->pubname, true);
#endif
            ereport(DEBUG1,
                    (errmsg("table \"%s.%s\" added to subscription \"%s\"",
                            rv->schemaname, rv->relname, sub->name)));
        }
    }

    /*
     * Next remove state for tables we should not care about anymore using the
     * data we collected above
     */
    qsort(pubrel_local_oids, list_length(pubrel_names),
          sizeof(Oid), oid_cmp);

    for (off = 0; off < list_length(subrel_states); off++)
    {
        Oid            relid = subrel_local_oids[off];

        if (!bsearch(&relid, pubrel_local_oids,
                     list_length(pubrel_names), sizeof(Oid), oid_cmp))
        {
            RemoveSubscriptionRel(sub->oid, relid);
#ifdef __STORAGE_SCALABLE__
            /* remove subscription / table mapping with publication */
            RemoveSubscriptionTable(sub->oid, relid);
#endif
            logicalrep_worker_stop_at_commit(sub->oid, relid);

            ereport(DEBUG1,
                    (errmsg("table \"%s.%s\" removed from subscription \"%s\"",
                            get_namespace_name(get_rel_namespace(relid)),
                            get_rel_name(relid),
                            sub->name)));
        }
    }
}

/*
 * Alter the existing subscription.
 */
ObjectAddress
AlterSubscription(AlterSubscriptionStmt *stmt)
{// #lizard forgives
    Relation    rel;
    ObjectAddress myself;
    bool        nulls[Natts_pg_subscription];
    bool        replaces[Natts_pg_subscription];
    Datum        values[Natts_pg_subscription];
    HeapTuple    tup;
    Oid            subid;
    bool        update_tuple = false;
    Subscription *sub;

    rel = heap_open(SubscriptionRelationId, RowExclusiveLock);

    /* Fetch the existing tuple. */
    tup = SearchSysCacheCopy2(SUBSCRIPTIONNAME, MyDatabaseId,
                              CStringGetDatum(stmt->subname));

    if (!HeapTupleIsValid(tup))
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("subscription \"%s\" does not exist",
                        stmt->subname)));

    /* must be owner */
    if (!pg_subscription_ownercheck(HeapTupleGetOid(tup), GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_SUBSCRIPTION,
                       stmt->subname);

    subid = HeapTupleGetOid(tup);
    sub = GetSubscription(subid, false);

    /* Lock the subscription so nobody else can do anything with it. */
    LockSharedObject(SubscriptionRelationId, subid, 0, AccessExclusiveLock);

    /* Form a new tuple. */
    memset(values, 0, sizeof(values));
    memset(nulls, false, sizeof(nulls));
    memset(replaces, false, sizeof(replaces));

    switch (stmt->kind)
    {
        case ALTER_SUBSCRIPTION_OPTIONS:
            {
                char       *slotname;
                bool        slotname_given;
                char       *synchronous_commit;

                parse_subscription_options(stmt->options, NULL, NULL, NULL,
                                           NULL, &slotname_given, &slotname,
                                           NULL, &synchronous_commit, NULL);

                if (slotname_given)
                {
                    if (sub->enabled && !slotname)
                        ereport(ERROR,
                                (errcode(ERRCODE_SYNTAX_ERROR),
                                 errmsg("cannot set slot_name = NONE for enabled subscription")));

                    if (slotname)
                        values[Anum_pg_subscription_subslotname - 1] =
                            DirectFunctionCall1(namein, CStringGetDatum(slotname));
                    else
                        nulls[Anum_pg_subscription_subslotname - 1] = true;
                    replaces[Anum_pg_subscription_subslotname - 1] = true;
                }

                if (synchronous_commit)
                {
                    values[Anum_pg_subscription_subsynccommit - 1] =
                        CStringGetTextDatum(synchronous_commit);
                    replaces[Anum_pg_subscription_subsynccommit - 1] = true;
                }

                update_tuple = true;
                break;
            }

        case ALTER_SUBSCRIPTION_ENABLED:
            {
                bool        enabled,
                            enabled_given;

                parse_subscription_options(stmt->options, NULL,
                                           &enabled_given, &enabled, NULL,
                                           NULL, NULL, NULL, NULL, NULL);
                Assert(enabled_given);

                if (!sub->slotname && enabled)
                    ereport(ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("cannot enable subscription that does not have a slot name")));

                values[Anum_pg_subscription_subenabled - 1] =
                    BoolGetDatum(enabled);
                replaces[Anum_pg_subscription_subenabled - 1] = true;

                if (enabled)
                    ApplyLauncherWakeupAtCommit();

                update_tuple = true;
                break;
            }

        case ALTER_SUBSCRIPTION_CONNECTION:
            /* Load the library providing us libpq calls. */
            load_file("libpqwalreceiver", false);
            /* Check the connection info string. */
            walrcv_check_conninfo(stmt->conninfo);

            values[Anum_pg_subscription_subconninfo - 1] =
                CStringGetTextDatum(stmt->conninfo);
            replaces[Anum_pg_subscription_subconninfo - 1] = true;
            update_tuple = true;
            break;

        case ALTER_SUBSCRIPTION_PUBLICATION:
            {
                bool        copy_data;
                bool        refresh;

                parse_subscription_options(stmt->options, NULL, NULL, NULL,
                                           NULL, NULL, NULL, &copy_data,
                                           NULL, &refresh);

                values[Anum_pg_subscription_subpublications - 1] =
                    publicationListToArray(stmt->publication);
                replaces[Anum_pg_subscription_subpublications - 1] = true;

                update_tuple = true;

                /* Refresh if user asked us to. */
                if (refresh)
                {
                    if (!sub->enabled)
                        ereport(ERROR,
                                (errcode(ERRCODE_SYNTAX_ERROR),
                                 errmsg("ALTER SUBSCRIPTION with refresh is not allowed for disabled subscriptions"),
                                 errhint("Use ALTER SUBSCRIPTION ... SET PUBLICATION ... WITH (refresh = false).")));

                    /* Make sure refresh sees the new list of publications. */
                    sub->publications = stmt->publication;

                    AlterSubscription_refresh(sub, copy_data);
                }

                break;
            }

        case ALTER_SUBSCRIPTION_REFRESH:
            {
                bool        copy_data;

                if (!sub->enabled)
                    ereport(ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("ALTER SUBSCRIPTION ... REFRESH is not allowed for disabled subscriptions")));

                parse_subscription_options(stmt->options, NULL, NULL, NULL,
                                           NULL, NULL, NULL, &copy_data,
                                           NULL, NULL);

                AlterSubscription_refresh(sub, copy_data);

                break;
            }

        default:
            elog(ERROR, "unrecognized ALTER SUBSCRIPTION kind %d",
                 stmt->kind);
    }

    /* Update the catalog if needed. */
    if (update_tuple)
    {
        tup = heap_modify_tuple(tup, RelationGetDescr(rel), values, nulls,
                                replaces);

        CatalogTupleUpdate(rel, &tup->t_self, tup);

        heap_freetuple(tup);
    }

    heap_close(rel, RowExclusiveLock);

    ObjectAddressSet(myself, SubscriptionRelationId, subid);

    InvokeObjectPostAlterHook(SubscriptionRelationId, subid, 0);

    return myself;
}

/*
 * Drop a subscription
 */
void
DropSubscription(DropSubscriptionStmt *stmt, bool isTopLevel)
{// #lizard forgives
    Relation    rel;
    ObjectAddress myself;
    HeapTuple    tup;
    Oid            subid;
    Datum        datum;
    bool        isnull;
    char       *subname;
    char       *conninfo;
    char       *slotname;
    List       *subworkers;
    ListCell   *lc;
    char        originname[NAMEDATALEN];
    char       *err = NULL;
    RepOriginId originid;
    WalReceiverConn *wrconn = NULL;
    StringInfoData cmd;

    /*
     * Lock pg_subscription with AccessExclusiveLock to ensure that the
     * launcher doesn't restart new worker during dropping the subscription
     */
    rel = heap_open(SubscriptionRelationId, AccessExclusiveLock);

    tup = SearchSysCache2(SUBSCRIPTIONNAME, MyDatabaseId,
                          CStringGetDatum(stmt->subname));

    if (!HeapTupleIsValid(tup))
    {
        heap_close(rel, NoLock);

        if (!stmt->missing_ok)
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                     errmsg("subscription \"%s\" does not exist",
                            stmt->subname)));
        else
            ereport(NOTICE,
                    (errmsg("subscription \"%s\" does not exist, skipping",
                            stmt->subname)));

        return;
    }

    subid = HeapTupleGetOid(tup);

    /* must be owner */
    if (!pg_subscription_ownercheck(subid, GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_SUBSCRIPTION,
                       stmt->subname);

    /* DROP hook for the subscription being removed */
    InvokeObjectDropHook(SubscriptionRelationId, subid, 0);

    /*
     * Lock the subscription so nobody else can do anything with it (including
     * the replication workers).
     */
    LockSharedObject(SubscriptionRelationId, subid, 0, AccessExclusiveLock);

    /* Get subname */
    datum = SysCacheGetAttr(SUBSCRIPTIONOID, tup,
                            Anum_pg_subscription_subname, &isnull);
    Assert(!isnull);
    subname = pstrdup(NameStr(*DatumGetName(datum)));

    /* Get conninfo */
    datum = SysCacheGetAttr(SUBSCRIPTIONOID, tup,
                            Anum_pg_subscription_subconninfo, &isnull);
    Assert(!isnull);
    conninfo = TextDatumGetCString(datum);

    /* Get slotname */
    datum = SysCacheGetAttr(SUBSCRIPTIONOID, tup,
                            Anum_pg_subscription_subslotname, &isnull);
    if (!isnull)
        slotname = pstrdup(NameStr(*DatumGetName(datum)));
    else
        slotname = NULL;

    /*
     * Since dropping a replication slot is not transactional, the replication
     * slot stays dropped even if the transaction rolls back.  So we cannot
     * run DROP SUBSCRIPTION inside a transaction block if dropping the
     * replication slot.
     *
     * XXX The command name should really be something like "DROP SUBSCRIPTION
     * of a subscription that is associated with a replication slot", but we
     * don't have the proper facilities for that.
     */
    if (slotname)
        PreventTransactionChain(isTopLevel, "DROP SUBSCRIPTION");


    ObjectAddressSet(myself, SubscriptionRelationId, subid);
    EventTriggerSQLDropAddObject(&myself, true, true);

    /* Remove the tuple from catalog. */
    CatalogTupleDelete(rel, &tup->t_self);

    ReleaseSysCache(tup);

    /*
     * If we are dropping the replication slot, stop all the subscription
     * workers immediately, so that the slot becomes accessible.  Otherwise
     * just schedule the stopping for the end of the transaction.
     *
     * New workers won't be started because we hold an exclusive lock on the
     * subscription till the end of the transaction.
     */
    LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);
    subworkers = logicalrep_workers_find(subid, false);
    LWLockRelease(LogicalRepWorkerLock);
    foreach(lc, subworkers)
    {
        LogicalRepWorker *w = (LogicalRepWorker *) lfirst(lc);

        if (slotname)
            logicalrep_worker_stop(w->subid, w->relid);
        else
            logicalrep_worker_stop_at_commit(w->subid, w->relid);
    }
    list_free(subworkers);

    /* Clean up dependencies */
    deleteSharedDependencyRecordsFor(SubscriptionRelationId, subid, 0);

    /* Remove any associated relation synchronization states. */
    RemoveSubscriptionRel(subid, InvalidOid);

#ifdef __STORAGE_SCALABLE__
    /* Remove any associated shards/tables */
    RemoveSubscriptionShard(subid, InvalidShardID);
    RemoveSubscriptionTable(subid, InvalidOid);
    DirectFunctionCall1Coll(tbase_remove_subtable_stat, InvalidOid,
                            UInt32GetDatum(subid));
    DirectFunctionCall1Coll(tbase_remove_sub_stat, InvalidOid,
                            PointerGetDatum(cstring_to_text(stmt->subname)));
#endif

    /* Remove the origin tracking if exists. */
    snprintf(originname, sizeof(originname), "pg_%u", subid);
    originid = replorigin_by_name(originname, true);
    if (originid != InvalidRepOriginId)
        replorigin_drop(originid, false);

    /*
     * If there is no slot associated with the subscription, we can finish
     * here.
     */
    if (!slotname)
    {
        heap_close(rel, NoLock);
        return;
    }

    /*
     * Otherwise drop the replication slot at the publisher node using the
     * replication connection.
     */
    load_file("libpqwalreceiver", false);

    initStringInfo(&cmd);
    appendStringInfo(&cmd, "DROP_REPLICATION_SLOT %s", quote_identifier(slotname));

    wrconn = walrcv_connect(conninfo, true, subname, &err);
    if (wrconn == NULL)
    {
        heap_close(rel, NoLock);
        ereport(ERROR,
                (errmsg("could not connect to publisher when attempting to "
                        "drop the replication slot \"%s\"", slotname),
                 errdetail("The error was: %s", err),
                 errhint("Use ALTER SUBSCRIPTION ... SET (slot_name = NONE) "
                         "to disassociate the subscription from the slot.")));
    }
    PG_TRY();
    {
        WalRcvExecResult *res;

        res = walrcv_exec(wrconn, cmd.data, 0, NULL);

        if (res->status != WALRCV_OK_COMMAND)
            ereport(ERROR,
                    (errmsg("could not drop the replication slot \"%s\" on publisher",
                            slotname),
                     errdetail("The error was: %s", res->err)));
        else
            ereport(NOTICE,
                    (errmsg("dropped replication slot \"%s\" on publisher",
                            slotname)));

        walrcv_clear_result(res);
    }
    PG_CATCH();
    {
        /* Close the connection in case of failure */
        walrcv_disconnect(wrconn);
        PG_RE_THROW();
    }
    PG_END_TRY();

    walrcv_disconnect(wrconn);

    pfree(cmd.data);

    heap_close(rel, NoLock);
}

/*
 * Internal workhorse for changing a subscription owner
 */
static void
AlterSubscriptionOwner_internal(Relation rel, HeapTuple tup, Oid newOwnerId)
{
    Form_pg_subscription form;

    form = (Form_pg_subscription) GETSTRUCT(tup);

    if (form->subowner == newOwnerId)
        return;

    if (!pg_subscription_ownercheck(HeapTupleGetOid(tup), GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_SUBSCRIPTION,
                       NameStr(form->subname));

    /* New owner must be a superuser */
    if (!superuser_arg(newOwnerId))
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("permission denied to change owner of subscription \"%s\"",
                        NameStr(form->subname)),
                 errhint("The owner of a subscription must be a superuser.")));

    form->subowner = newOwnerId;
    CatalogTupleUpdate(rel, &tup->t_self, tup);

    /* Update owner dependency reference */
    changeDependencyOnOwner(SubscriptionRelationId,
                            HeapTupleGetOid(tup),
                            newOwnerId);

    InvokeObjectPostAlterHook(SubscriptionRelationId,
                              HeapTupleGetOid(tup), 0);
}

/*
 * Change subscription owner -- by name
 */
ObjectAddress
AlterSubscriptionOwner(const char *name, Oid newOwnerId)
{
    Oid            subid;
    HeapTuple    tup;
    Relation    rel;
    ObjectAddress address;

    rel = heap_open(SubscriptionRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy2(SUBSCRIPTIONNAME, MyDatabaseId,
                              CStringGetDatum(name));

    if (!HeapTupleIsValid(tup))
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("subscription \"%s\" does not exist", name)));

    subid = HeapTupleGetOid(tup);

    AlterSubscriptionOwner_internal(rel, tup, newOwnerId);

    ObjectAddressSet(address, SubscriptionRelationId, subid);

    heap_freetuple(tup);

    heap_close(rel, RowExclusiveLock);

    return address;
}

/*
 * Change subscription owner -- by OID
 */
void
AlterSubscriptionOwner_oid(Oid subid, Oid newOwnerId)
{
    HeapTuple    tup;
    Relation    rel;

    rel = heap_open(SubscriptionRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy1(SUBSCRIPTIONOID, ObjectIdGetDatum(subid));

    if (!HeapTupleIsValid(tup))
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("subscription with OID %u does not exist", subid)));

    AlterSubscriptionOwner_internal(rel, tup, newOwnerId);

    heap_freetuple(tup);

    heap_close(rel, RowExclusiveLock);
}

/*
 * Get the list of tables which belong to specified publications on the
 * publisher connection.
 */
static List *
fetch_table_list(WalReceiverConn *wrconn, List *publications)
{
    WalRcvExecResult *res;
    StringInfoData cmd;
    TupleTableSlot *slot;
    Oid            tableRow[3] = {TEXTOID, TEXTOID, TEXTOID};
    ListCell   *lc;
    bool        first;
    List       *tablelist = NIL;

    Assert(list_length(publications) > 0);

    initStringInfo(&cmd);
    appendStringInfo(&cmd, "SELECT DISTINCT t.schemaname, t.tablename, t.pubname \n"
                     "  FROM pg_catalog.pg_publication_tables t\n"
                     " WHERE t.pubname IN (");
    first = true;
    foreach(lc, publications)
    {
        char       *pubname = strVal(lfirst(lc));

        if (first)
            first = false;
        else
            appendStringInfoString(&cmd, ", ");

        appendStringInfo(&cmd, "%s", quote_literal_cstr(pubname));
    }
    appendStringInfoString(&cmd, ")");

    res = walrcv_exec(wrconn, cmd.data, 3, tableRow);
    pfree(cmd.data);

    if (res->status != WALRCV_OK_TUPLES)
        ereport(ERROR,
                (errmsg("could not receive list of replicated tables from the publisher: %s",
                        res->err)));

    /* Process tables. */
    slot = MakeSingleTupleTableSlot(res->tupledesc);
    while (tuplestore_gettupleslot(res->tuplestore, true, false, slot))
    {
        char       *nspname;
        char       *relname;
        char       *pubname;
        bool        isnull;
        RangeVar   *rv;

        nspname = TextDatumGetCString(slot_getattr(slot, 1, &isnull));
        Assert(!isnull);
        relname = TextDatumGetCString(slot_getattr(slot, 2, &isnull));
        Assert(!isnull);
        pubname = TextDatumGetCString(slot_getattr(slot, 3, &isnull));
        Assert(!isnull);

        rv = makeRangeVar(pstrdup(nspname), pstrdup(relname), -1);
        rv->pubname = pstrdup(pubname);
        tablelist = lappend(tablelist, rv);

        ExecClearTuple(slot);
    }
    ExecDropSingleTupleTableSlot(slot);

    walrcv_clear_result(res);

    return tablelist;
}

#ifdef __STORAGE_SCALABLE__
/*
 * Get the list of shards which belong to specified publications on the
 * publisher connection.
 */
static List *
fetch_shard_list(WalReceiverConn *wrconn, List *publications)
{
    WalRcvExecResult *res;
    StringInfoData cmd;
    TupleTableSlot *slot;
    Oid            shardrow[2] = {INT4OID, TEXTOID};
    ListCell   *lc;
    bool        first;
    List       *shardlist = NIL;

    Assert(list_length(publications) > 0);

    initStringInfo(&cmd);
    appendStringInfo(&cmd, "SELECT DISTINCT t.prshardid, p.pubname \n"
                     " FROM pg_catalog.pg_publication_shard t, pg_catalog.pg_publication p \n"
                     " WHERE t.prpubid = p.oid and p.pubname IN (");
    first = true;
    foreach(lc, publications)
    {
        char       *pubname = strVal(lfirst(lc));

        if (first)
            first = false;
        else
            appendStringInfoString(&cmd, ", ");

        appendStringInfo(&cmd, "%s", quote_literal_cstr(pubname));
    }
    appendStringInfoString(&cmd, ")");

    res = walrcv_exec(wrconn, cmd.data, 2, shardrow);
    pfree(cmd.data);

    if (res->status != WALRCV_OK_TUPLES)
        ereport(ERROR,
                (errmsg("could not receive list of shards from the publisher: %s",
                        res->err)));

    /* Process tables. */
    slot = MakeSingleTupleTableSlot(res->tupledesc);
    while (tuplestore_gettupleslot(res->tuplestore, true, false, slot))
    {
        int32       shardid;
        char       *pubname;
        bool        isnull;
        PubShard   *ps;

        shardid = DatumGetInt32(slot_getattr(slot, 1, &isnull));
        Assert(!isnull);
        pubname = TextDatumGetCString(slot_getattr(slot, 2, &isnull));
        Assert(!isnull);

        ps = (PubShard *)palloc0(sizeof(PubShard));
        ps->shardid = shardid;
        ps->pubname = pstrdup(pubname);
        shardlist = lappend(shardlist, ps);

        ExecClearTuple(slot);
    }
    ExecDropSingleTupleTableSlot(slot);

    walrcv_clear_result(res);

    return shardlist;
}
#endif

#ifdef __SUBSCRIPTION__
/*
 * Common option parsing function for CREATE and ALTER TBASE SUBSCRIPTION commands.
 *
 * Since not all options can be specified in both commands, this function
 * will report an error on options if the target output pointer is NULL to
 * accommodate that.
 */
static void
parse_tbase_subscription_options(List *options,
                                 bool is_create_stmt,
                                 bool *ignore_pk_conflict,
                                 char **manual_hot_date,
                                 char **temp_hot_date,
                                 char **temp_cold_date,
                                 int *parallel_number,
								 bool *copy_data,
								 char **slot_name,
								 bool *slot_name_given)
{// #lizard forgives
    ListCell *lc = NULL;
    bool copy_data_given = false;
    *slot_name_given = false;

    /* This operation can only be performed on Coordinator */
    if (!IS_PGXC_COORDINATOR || !IsConnFromApp())
    {
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("This operation can only be performed on Coordinator.")));
    }

    if (copy_data)
        *copy_data = true;

    /* Parse options */
    foreach(lc, options)
    {
        DefElem    *defel = (DefElem *) lfirst(lc);

        if (strcmp(defel->defname, "ignore_pk_conflict") == 0 && ignore_pk_conflict)
        {
            *ignore_pk_conflict = defGetBoolean(defel);
        }
        else if (strcmp(defel->defname, "manual_hot_date") == 0 && manual_hot_date)
        {
            *manual_hot_date = defGetString(defel);

            /* Test if the given value is valid for manual_hot_date GUC. */
            (void) set_config_option("manual_hot_date", *manual_hot_date,
                                     PGC_BACKEND, PGC_S_TEST, GUC_ACTION_SET,
                                     false, 0, false);
        }
        else if (strcmp(defel->defname, "temp_hot_date") == 0 && temp_hot_date)
        {
            *temp_hot_date = defGetString(defel);

            /* Test if the given value is valid for temp_hot_date GUC. */
            (void) set_config_option("temp_hot_date", *temp_hot_date,
                                     PGC_BACKEND, PGC_S_TEST, GUC_ACTION_SET,
                                     false, 0, false);
        }
        else if (strcmp(defel->defname, "temp_cold_date") == 0 && temp_cold_date)
        {
            *temp_cold_date = defGetString(defel);

            /* Test if the given value is valid for temp_cold_date GUC. */
            (void) set_config_option("temp_cold_date", *temp_cold_date,
                                     PGC_BACKEND, PGC_S_TEST, GUC_ACTION_SET,
                                     false, 0, false);
        }
        else if (strcmp(defel->defname, "parallel_number") == 0 && parallel_number)
        {
            *parallel_number = defGetInt32(defel);

            if (*parallel_number <= 0)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("Invalid value %d of \"parallel_number\"", *parallel_number)));
            }
        }
        else if (strcmp(defel->defname, "slot_name") == 0)
        {
		    if (*slot_name_given)
		        ereport(ERROR,
		                (errcode(ERRCODE_SYNTAX_ERROR),
		                        errmsg("conflicting or redundant options")));

		    *slot_name_given = true;
		    *slot_name       = defGetString(defel);

		    /* Setting slot_name = NONE is treated as no slot name. */
		    if (strcmp(*slot_name, "none") == 0)
		        *slot_name = NULL;
        }
        else if (strcmp(defel->defname, "copy_data") == 0)
        {
            if (is_create_stmt)
            {
                if (copy_data_given)
                    ereport(ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("conflicting or redundant options")));

                copy_data_given = true;
                if (copy_data)
                {
                    *copy_data = defGetBoolean(defel);
                }
            }
            else
            {
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("The \"copy_data\" option can not be specified in ALTER TBASE SUBSCRIPTION.")));
            }
        }
    }
}

/*
 * check if tbase_subscription extension is installed
 */
void check_tbase_subscription_extension(void)
{
    Oid extOid = InvalidOid;

    extOid = get_extension_oid(g_tbase_subscription_extension, true);

    if (!OidIsValid(extOid))
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("This operation is not allowed until the extension \"%s\" is installed.",
                        g_tbase_subscription_extension)));
}

/* check if already exists a same name */
static bool check_tbase_subscription_ifexists(Relation tbase_sub_rel, char * check_subname)
{
    HeapScanDesc    scan = NULL;
    HeapTuple       tuple = NULL;
    TupleDesc        desc = NULL;

    bool            exists = false;

    desc = RelationGetDescr(tbase_sub_rel);
    scan = heap_beginscan(tbase_sub_rel, GetActiveSnapshot(), 0, NULL);
    while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
    {
        Name subname = NULL;
        bool isnull = true;

        subname = DatumGetName(fastgetattr(tuple, Anum_tbase_subscription_sub_name, desc, &isnull));

        if (false == isnull && subname != NULL && 0 == pg_strcasecmp(check_subname, NameStr(*subname)))
        {
            exists = true;
            break;
        }        
    }
    heap_endscan(scan);

    return exists;
}

/*
 * transform tbase subscription into parallel sub-subscriptions list
 */
static List * tbase_subscription_parallelization(Node * stmt, int parallel_number, bool slot_name_given)
{// #lizard forgives
    int i = 0;
    List * lstmt = NIL;

    Assert(parallel_number >= 1);
    if (!(parallel_number >= 1))
    {
        abort();
    }

    for (i = 0; i < parallel_number; i++)
    {
        Node * stmt_parallel = copyObject(stmt);

        lstmt = lappend(lstmt, stmt_parallel);
        
        switch (nodeTag(stmt_parallel))
        {
            case T_CreateSubscriptionStmt:
            {
                CreateSubscriptionStmt * stmt_create = (CreateSubscriptionStmt *) stmt_parallel;
                char * new_subname = palloc0(NAMEDATALEN);
				char * new_slot_name = palloc0(NAMEDATALEN);

                /* rename subname a parallel one */
                snprintf(new_subname, NAMEDATALEN - 1, "%s_%d_%d", stmt_create->subname, parallel_number, i);
                stmt_create->subname = new_subname;

				/* construct slotname for a parallel one */
				if (slot_name_given)
				{
				    ListCell * 	lc = NULL;

				    foreach(lc, stmt_create->options)
				    {
				        DefElem * defel = (DefElem *) lfirst(lc);
				        if (strcmp(defel->defname, "slot_name") == 0)
				        {
				            char * slot_name_pre = defGetString(defel);
				            if (strcmp(slot_name_pre, "none") == 0)
				                snprintf(new_slot_name, NAMEDATALEN - 1, "%s", slot_name_pre);
				            else
				                snprintf(new_slot_name, NAMEDATALEN - 1, "%s_%d_%d", slot_name_pre, parallel_number, i);

				            defel->arg = (Node *)makeString(new_slot_name);
				        }
				    }
				}

                /*
                 * Only the first sub-subscription of all parallel sub-subscriptions is allowed
                 * to perform the copy_data operation. Other sub-subscriptions must wait until the
                 * first sub-subscription completes the copy_data before starting the incremental
                 * subscription, so only the first sub-subscription's copy_data is retained,
                 * while the copy_data option of other subsubscriptions will be emptied here.
                 */
                if (i != 0)
                {
                    ListCell *     lc = NULL;
                    bool        copy_data_given = false;

                    foreach(lc, stmt_create->options)
                    {
                        DefElem * defel = (DefElem *) lfirst(lc);

                        if (strcmp(defel->defname, "copy_data") == 0)
                        {
                            bool bool_value = defGetBoolean(defel);

                            if (bool_value)
                                defel->arg = (Node *)makeString("false");

                            copy_data_given = true;
                            break;
                        }
                    }

                    if (false == copy_data_given)
                    {
                        DefElem * copy_data = makeDefElem("copy_data", (Node *)makeString("false"), -1);
                        stmt_create->options = lappend(stmt_create->options, copy_data);
                    }
                }

                do
                {
                    stmt_create->sub_parallel_number = parallel_number;
                    stmt_create->sub_parallel_index = i;
                } while (0);
                break;
            }
            case T_AlterSubscriptionStmt:
            {
                AlterSubscriptionStmt * stmt_alter = (AlterSubscriptionStmt *) stmt_parallel;
                char * new_subname = palloc0(NAMEDATALEN);
				char * new_slot_name = palloc0(NAMEDATALEN);

                /* rename subname a parallel one */
                snprintf(new_subname, NAMEDATALEN - 1, "%s_%d_%d", stmt_alter->subname, parallel_number, i);
                stmt_alter->subname = new_subname;

				/*construct a new parallel slot_name */
				if (slot_name_given)
				{
				    ListCell * 	lc = NULL;

				    foreach(lc, stmt_alter->options)
				    {
				        DefElem * defel = (DefElem *) lfirst(lc);
				        if (strcmp(defel->defname, "slot_name") == 0)
				        {
				            char * slot_name_pre = defGetString(defel);
				            if (strcmp(slot_name_pre, "none") == 0)
				                snprintf(new_slot_name, NAMEDATALEN - 1, "%s", slot_name_pre);
				            else
				                snprintf(new_slot_name, NAMEDATALEN - 1, "%s_%d_%d", slot_name_pre, parallel_number, i);
				            defel->arg = (Node *)makeString(new_slot_name);
				        }
				    }
				}
                break;
            }
            case T_DropSubscriptionStmt:
            {
                DropSubscriptionStmt * stmt_drop = (DropSubscriptionStmt *) stmt_parallel;
                char * new_subname = palloc0(NAMEDATALEN);

                /* rename subname a parallel one */
                snprintf(new_subname, NAMEDATALEN - 1, "%s_%d_%d", stmt_drop->subname, parallel_number, i);
                stmt_drop->subname = new_subname;
                break;
            }
            default:
            {
                break;
            }
        }
    }

    return lstmt;
}

/*
 * Create new Tbase subscription.
 */
ObjectAddress
CreateTbaseSubscription(CreateSubscriptionStmt *stmt, bool isTopLevel)
{// #lizard forgives
    bool ignore_pk_conflict = false;
    char *manual_hot_date = NULL;
    char *temp_hot_date = NULL;
    char *temp_cold_date = NULL;
    int parallel_number = 1;
    bool copy_data = false;
	char *slot_name = NULL;
	bool slot_name_given = false;

    Relation tbase_sub_rel = NULL;
    Oid    tbase_sub_parent_oid = InvalidOid;

    List * stmt_create_list = NULL;

    ObjectAddress myself = InvalidObjectAddress;

    /* check if tbase_subscription is installed */
    check_tbase_subscription_extension();

    /* parse options */
    parse_tbase_subscription_options(stmt->options, true,
                                        &ignore_pk_conflict,
                                        &manual_hot_date,
                                        &temp_hot_date,
                                        &temp_cold_date,
                                        &parallel_number,
										&copy_data,
										&slot_name,
										&slot_name_given);

	/* check if parallel_number is not greater than max_logical_replication_workers parameter */
	if (parallel_number > max_logical_replication_workers)
	{
		ereport(ERROR,
                (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
                 errmsg("options parallel_number %d is greater than max_logical_replication_workers %d",
                     parallel_number, max_logical_replication_workers),
                 errhint("You might need to increase max_logical_replication_workers or decrease parallel_number.")));
	}

    PushActiveSnapshot(GetLocalTransactionSnapshot());

    /* check if already exists a same name */
    tbase_sub_rel = relation_openrv(makeRangeVar("public", (char *)g_tbase_subscription_relname, -1), RowExclusiveLock);
    if (check_tbase_subscription_ifexists(tbase_sub_rel,  stmt->subname))
    {
        relation_close(tbase_sub_rel, RowExclusiveLock);
        ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_OBJECT),
                 errmsg("tbase subscription \"%s\" already exists",
                        stmt->subname)));
    }

    /* create an item in tbase_subscription */
    do
    {
        TupleDesc    tup_desc = NULL;
        HeapTuple    tuple = NULL;
        bool        nulls[Natts_tbase_subscription] = { false };
        Datum        values[Natts_tbase_subscription] = { 0 };

        tup_desc = RelationGetDescr(tbase_sub_rel);

        values[Anum_tbase_subscription_sub_name - 1] = DirectFunctionCall1(namein, CStringGetDatum(stmt->subname));
        values[Anum_tbase_subscription_sub_ignore_pk_conflict - 1] = BoolGetDatum(ignore_pk_conflict);

        if (manual_hot_date)
        {
            values[Anum_tbase_subscription_sub_manual_hot_date - 1] = CStringGetTextDatum(manual_hot_date);
        }
        else
        {
            nulls[Anum_tbase_subscription_sub_manual_hot_date - 1] = true;
        }

        if (temp_hot_date)
        {
            values[Anum_tbase_subscription_sub_temp_hot_date - 1] = CStringGetTextDatum(temp_hot_date);
        }
        else
        {
            nulls[Anum_tbase_subscription_sub_temp_hot_date - 1] = true;
        }

        if (temp_cold_date)
        {
            values[Anum_tbase_subscription_sub_temp_cold_date - 1] = CStringGetTextDatum(temp_cold_date);
        }
        else
        {
            nulls[Anum_tbase_subscription_sub_temp_cold_date - 1] = true;
        }

        values[Anum_tbase_subscription_sub_parallel_number - 1] = Int32GetDatum(parallel_number);

        /*
         * If there are some parallel tbase-sub-subscriptions, 
         * other tbase-sub-subscriptions can be activated only after 
         * the first tbase-sub-subscription has completed the data COPY.
         * And other tbase-sub-subscriptions can only be activated by 
         * the first tbase-sub-subscription.
         */
        if (parallel_number > 1 && copy_data == true)
        {
            values[Anum_tbase_subscription_sub_is_all_actived - 1] = BoolGetDatum(false);
        }
        else
        {
            values[Anum_tbase_subscription_sub_is_all_actived - 1] = BoolGetDatum(true);
        }

        tuple = heap_form_tuple(tup_desc, values, nulls);
        tbase_sub_parent_oid = simple_heap_insert(tbase_sub_rel, tuple);
        heap_freetuple(tuple);
    } while (0);

    /* transform to CreateSubscriptionStmt list, and rename each item */
	stmt_create_list = tbase_subscription_parallelization((Node *)stmt, parallel_number, slot_name_given);

    /* call CreateSubscription for each item */
    do
    {
        Relation tbase_sub_parallel_rel = NULL;        
        
        int i = 0;
        ListCell * lc = NULL;

        tbase_sub_parallel_rel = relation_openrv(makeRangeVar("public", (char *)g_tbase_subscription_parallel_relname, -1), RowExclusiveLock);

        foreach(lc, stmt_create_list)
        {
            ObjectAddress pg_sub_tup_addr = InvalidObjectAddress;
            CreateSubscriptionStmt * stmt_create = (CreateSubscriptionStmt *) lfirst(lc);

            TupleDesc    tup_desc = NULL;
            HeapTuple    tuple = NULL;
            bool        nulls[Natts_tbase_subscription_parallel] = { false };
            Datum        values[Natts_tbase_subscription_parallel] = { 0 };

            bool        force_to_disable = false;

            if (parallel_number > 1 && copy_data == true)
            {
                if (i == 0)
                {
                    /*
                     * Here we only activate the first tbase-sub-subscription. 
                     * When the first tbase-sub-subscription completes the data COPY, 
                     * it will activate the other tbase-sub-subscriptions.
                     */
                    values[Anum_tbase_subscription_parallel_sub_active_state - 1] = BoolGetDatum(true);
                }
                else
                {
                    force_to_disable = true;
                    values[Anum_tbase_subscription_parallel_sub_active_state - 1] = BoolGetDatum(false);
                }
            }
            else
            {
                values[Anum_tbase_subscription_parallel_sub_active_state - 1] = BoolGetDatum(true);
            }

            values[Anum_tbase_subscription_parallel_sub_active_lsn - 1] = LSNGetDatum(InvalidXLogRecPtr);

            /* create each sub-subscription by CREATE SUBSCRIPTION */
            pg_sub_tup_addr = CreateSubscription(stmt_create, isTopLevel, force_to_disable);

            /* record each sub-subscription into tbase_subscription_parallel */
            values[Anum_tbase_subscription_parallel_sub_parent - 1] = ObjectIdGetDatum(tbase_sub_parent_oid);
            values[Anum_tbase_subscription_parallel_sub_child - 1] = ObjectIdGetDatum(pg_sub_tup_addr.objectId);
            values[Anum_tbase_subscription_parallel_sub_index - 1] = Int32GetDatum(i);

            tup_desc = RelationGetDescr(tbase_sub_parallel_rel);
            tuple = heap_form_tuple(tup_desc, values, nulls);
            simple_heap_insert(tbase_sub_parallel_rel, tuple);
            heap_freetuple(tuple);

            Assert(i <= parallel_number && i == stmt_create->sub_parallel_index);
            if (!(i <= parallel_number && i == stmt_create->sub_parallel_index))
            {
                abort();
            }

            i++; 
        }

        relation_close(tbase_sub_parallel_rel, RowExclusiveLock);
    } while (0);

    ObjectAddressSet(myself, RelationGetRelid(tbase_sub_rel), tbase_sub_parent_oid);
    relation_close(tbase_sub_rel, RowExclusiveLock);

    PopActiveSnapshot();
    CommandCounterIncrement();

    return myself;
}

/*
 * Alter the existing Tbase subscription.
 */
void AlterTbaseSubscription(AlterSubscriptionStmt *stmt)
{// #lizard forgives
    int parallel_number = 0;
    bool is_all_actived = false;
    bool slot_name_given = false;
    char * slot_name = NULL;

    check_tbase_subscription_extension();

    /* first check TBase subscprition exists and get parallel_number */
    do
    {
        Relation        tbase_sub_rel = NULL;
        HeapScanDesc    scan = NULL;
        HeapTuple       tuple = NULL;
        TupleDesc        desc = NULL;
        bool            exists = false;

        PushActiveSnapshot(GetLocalTransactionSnapshot());

        tbase_sub_rel = relation_openrv(makeRangeVar("public", 
                                        (char *)g_tbase_subscription_relname, -1), 
                                        RowExclusiveLock);
        desc = RelationGetDescr(tbase_sub_rel);
        scan = heap_beginscan(tbase_sub_rel, GetActiveSnapshot(), 0, NULL);

        exists = false;
        while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
        {
            Name subname;
            bool isnull;

            subname = DatumGetName(fastgetattr(tuple, Anum_tbase_subscription_sub_name, desc, &isnull));

            if (false == isnull && subname != NULL && 0 == pg_strcasecmp(stmt->subname, NameStr(*subname)))
            {
                exists = true;
                parallel_number = DatumGetInt32(fastgetattr(tuple, Anum_tbase_subscription_sub_parallel_number, desc, &isnull));
                is_all_actived = DatumGetBool(fastgetattr(tuple, Anum_tbase_subscription_sub_is_all_actived, desc, &isnull));
                break;
            }        
        }

        heap_endscan(scan);
        relation_close(tbase_sub_rel, RowExclusiveLock);

        PopActiveSnapshot();
        CommandCounterIncrement();

        if (false == exists)
        {
            elog(ERROR, "TBase subscription \"%s\" does not exist", stmt->subname);
            return;
        }
    } while(0);

    if (false == is_all_actived && parallel_number > 1 &&
        stmt->kind == ALTER_SUBSCRIPTION_REFRESH)
    {
        elog(ERROR, "TBase Subscription '%s' is not allowed to refresh until all its sub-subscriptions have been activated", stmt->subname);
        return;
    }
	/* check if slot_name is given*/
	if (stmt->kind == ALTER_SUBSCRIPTION_OPTIONS)
	{
	    parse_tbase_subscription_options(stmt->options, false, NULL, NULL, NULL,
	            NULL, NULL, NULL, &slot_name, &slot_name_given);
	}

    do
    {
        List    * stmt_list;
        ListCell* lc;

		stmt_list = tbase_subscription_parallelization((Node *)stmt, parallel_number, slot_name_given);

        foreach(lc, stmt_list)
        {
            AlterSubscriptionStmt * stmt;
            ObjectAddress           address;
            
            stmt    = (AlterSubscriptionStmt *) lfirst(lc);
            address = AlterSubscription(stmt);

            /* do commandCollected inside */
            EventTriggerCollectSimpleCommand(address, InvalidObjectAddress, (Node *)stmt);
        }
    } while (0);

    return;
}

/*
 * Drop a Tbase subscription
 */
void
DropTbaseSubscription(DropSubscriptionStmt *stmt, bool isTopLevel)
{// #lizard forgives
    Oid    sub_parent_oid = InvalidOid;
    int parallel_number = -1;
    
    check_tbase_subscription_extension();

    PushActiveSnapshot(GetLocalTransactionSnapshot());

    /* check if subscription exists */
    do
    {
        Relation         tbase_sub_rel = NULL;
        HeapScanDesc    scan = NULL;
        HeapTuple       tuple = NULL;
        TupleDesc        desc = NULL;

        bool            exists = false;

        tbase_sub_rel = relation_openrv(makeRangeVar("public", 
                                        (char *)g_tbase_subscription_relname, -1), 
                                        RowExclusiveLock);
        desc = RelationGetDescr(tbase_sub_rel);
        scan = heap_beginscan(tbase_sub_rel, GetActiveSnapshot(), 0, NULL);

        while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
        {
            Name subname = NULL;
            bool isnull = true;

            subname = DatumGetName(fastgetattr(tuple, Anum_tbase_subscription_sub_name, desc, &isnull));

            if (false == isnull && subname != NULL && 0 == pg_strcasecmp(stmt->subname, NameStr(*subname)))
            {
                exists = true;
                sub_parent_oid = HeapTupleGetOid(tuple);
                parallel_number = DatumGetInt32(fastgetattr(tuple, Anum_tbase_subscription_sub_parallel_number, desc, &isnull));
                simple_heap_delete(tbase_sub_rel, &tuple->t_self);
                break;
            }        
        }

        heap_endscan(scan);
        relation_close(tbase_sub_rel, RowExclusiveLock);

        if (false == exists)
        {
            if (!stmt->missing_ok)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_UNDEFINED_OBJECT),
                         errmsg("tbase subscription \"%s\" does not exist",
                                stmt->subname)));
            }
            else
            {
                ereport(NOTICE,
                        (errmsg("tbase subscription \"%s\" does not exist, skipping",
                                stmt->subname)));
            }

            return;
        }

        Assert(parallel_number >= 1 && sub_parent_oid != InvalidOid);
        if (!(parallel_number >= 1 && sub_parent_oid != InvalidOid))
        {
            abort();
        }
    } while (0);

    /* scan tbase_subscription_parallel, and delete related tuple by sub_parent */
    do
    {
        Relation         tbase_sub_parallel_rel = NULL;
        HeapScanDesc    scan = NULL;
        HeapTuple       tuple = NULL;
        TupleDesc        desc = NULL;

        int32             i_assert = 0;

        tbase_sub_parallel_rel = relation_openrv(makeRangeVar("public",
                                                    (char *)g_tbase_subscription_parallel_relname, -1),
                                                    RowExclusiveLock);
        desc = RelationGetDescr(tbase_sub_parallel_rel);
        scan = heap_beginscan(tbase_sub_parallel_rel, GetActiveSnapshot(), 0, NULL);

        while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
        {
            Oid     sub_parent = InvalidOid;
            bool isnull = true;

            sub_parent = DatumGetObjectId(fastgetattr(tuple, Anum_tbase_subscription_parallel_sub_parent, desc, &isnull));

            if (false == isnull && sub_parent == sub_parent_oid)
            {
                i_assert++; Assert(i_assert <= parallel_number);
                simple_heap_delete(tbase_sub_parallel_rel, &tuple->t_self);
            }
        }

        heap_endscan(scan);
        relation_close(tbase_sub_parallel_rel, RowExclusiveLock);

        Assert(i_assert == parallel_number);
        if (!(i_assert == parallel_number))
        {
            abort();
        }
    } while (0);

    /* parallelization drop stmt, and drop each parallel sub-subscritions by DropSubscription  */
    do
    {
        List * stmt_drop_list = NULL;
        ListCell * lc = NULL;
        int32 i_assert = 0;

		stmt_drop_list = tbase_subscription_parallelization((Node *)stmt, parallel_number, false);

        foreach(lc, stmt_drop_list)
        {
            DropSubscriptionStmt * stmt_drop = NULL;

            stmt_drop = (DropSubscriptionStmt *) lfirst(lc);
            
			DropSubscription(stmt_drop, isTopLevel);

			i_assert++;
			Assert(i_assert <= parallel_number);

			if (!(i_assert <= parallel_number))
			{
				abort();
			}
        }
    } while (0);

    PopActiveSnapshot();
    CommandCounterIncrement();
}

bool IsTbaseSubscription(Node * stmt)
{
    switch (nodeTag(stmt))
    {
        case T_CreateSubscriptionStmt:
        {
            CreateSubscriptionStmt * stmt_create = (CreateSubscriptionStmt *) stmt;
            return stmt_create->istbase;
            break;
        }
        case T_AlterSubscriptionStmt:
        {
            AlterSubscriptionStmt * stmt_alter = (AlterSubscriptionStmt *) stmt;
            return stmt_alter->istbase;
            break;
        }
        case T_DropSubscriptionStmt:
        {
            DropSubscriptionStmt * stmt_drop = (DropSubscriptionStmt *) stmt;
            return stmt_drop->istbase;
            break;
        }
        default:
        {
            return false;
            break;
        }
    }

    return false;
}

#endif
