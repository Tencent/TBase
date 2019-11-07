#include "postgres.h"

#include <unistd.h>

#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/spin.h"
#include "access/hash.h"

#include "tcop/utility.h"
#include "commands/dbcommands.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#include "libpq/auth.h"
#include "optimizer/planner.h"
#include "nodes/makefuncs.h"
#include "funcapi.h"
#include "stormstats.h"
#include "storage/fd.h"
#include "storage/shmem.h"
#include "storage/lwlock.h"

#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/planner.h"
#include "pgxc/execRemote.h"


/* mark this dynamic library to be compatible with PG */
PG_MODULE_MAGIC;

/* Location of stats file */
#define STORM_DUMP_FILE  "global/storm.stat"

/* This constant defines the magic number in the stats file header */
static const uint32 STORM_FILE_HEADER = 0x20120229;

#define STORM_STATS_COLS 7

typedef struct ssHashKey
{
    int dbname_len;
    const char *dbname_ptr;
} ssHashKey;

typedef struct EventCounters
{
    int64       conn_cnt;
    int64        select_cnt;
    int64        insert_cnt;
    int64        update_cnt;
    int64        delete_cnt;
    int64        ddl_cnt;
} EventCounters;

typedef struct StormStatsEntry
{
    ssHashKey       key;         /* hash key of entry - MUST BE FIRST */
    EventCounters   counters;
    slock_t         mutex;
    char            dbname[1];   /* VARIABLE LENGTH ARRAY - MUST BE LAST */

} StormStatsEntry;

/* Local hash table entry, no mutex needed */
typedef struct LocalStatsEntry
{
    ssHashKey       key;         /* hash key of entry */
    EventCounters   counters;
    char            dbname[NAMEDATALEN];
} LocalStatsEntry;

typedef struct StormSharedState
{
    LWLock *lock;
} StormSharedState;

static bool sp_save;            /* whether to save stats across shutdown */

extern PlannedStmt *planner_callback(Query *parse, int cursorOptions, ParamListInfo boundParams);
extern void auth_check(Port *port, int status);

static void sp_shmem_startup(void);
static void sp_shmem_shutdown(int code, Datum arg);
static Size hash_memsize(void);

static uint32 ss_hash_fn(const void *key, Size keysize);
static int ss_match_fn(const void *key1, const void *key2, Size keysize);
static void stats_store(const char *dbname, CmdType c, bool isConnEvent, bool isUtilEvent);

static StormStatsEntry *alloc_event_entry(ssHashKey *key);

/* Functions */
Datum storm_database_stats(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(storm_database_stats);

/* Shared Memory Objects */
static HTAB *StatsEntryHash = NULL;
static StormSharedState *shared_state = NULL;

/* Session level objects */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static ClientAuthentication_hook_type original_client_auth_hook = NULL;

static ProcessUtility_hook_type prev_ProcessUtility = NULL;

static int max_tracked_dbs;

static void
ProcessUtility_callback(PlannedStmt *pstmt,
                        const char *queryString, ProcessUtilityContext context,
                        ParamListInfo params,
                        QueryEnvironment *queryEnv,
                        DestReceiver *dest,
                        bool sentToRemote,
                        char *completionTag)
{
    Node   *parsetree;

    elog( DEBUG1, "STORMSTATS: using plugin." );

    standard_ProcessUtility(pstmt, queryString, context, params, queryEnv,
                            dest, sentToRemote, completionTag);

    stats_store(get_database_name(MyDatabaseId), CMD_UNKNOWN, false, true);

    parsetree = pstmt->utilityStmt;

    /*
     * Check if it's a CREATE/DROP DATABASE command. Update entries in the
     * shared hash table accordingly.
     */
    switch (nodeTag(parsetree))
    {
        case T_CreatedbStmt:
            {
                ssHashKey        key;
                StormStatsEntry  *entry;
                CreatedbStmt     *stmt = (CreatedbStmt *)parsetree;

                /* Set up key for hashtable search */
                key.dbname_len = strlen(stmt->dbname);
                key.dbname_ptr = stmt->dbname;

                /*
                 * Lookup the hash table entry with exclusive lock. We have to
                 * manipulate the entries immediately anyways..
                 */
                LWLockAcquire(shared_state->lock, LW_EXCLUSIVE);

                entry = (StormStatsEntry *) hash_search(StatsEntryHash, &key, HASH_FIND, NULL);

                /* What do we do if we find an entry already? We WARN for now */
                if (!entry)
                    entry = alloc_event_entry(&key);
                else
                    ereport(WARNING,
                        (errmsg("entry exists already for database %s!",
                        entry->dbname)));
                LWLockRelease(shared_state->lock);
                break;
            }
        case T_DropdbStmt:
            {
                ssHashKey        key;
                StormStatsEntry  *entry;
                DropdbStmt         *stmt = (DropdbStmt *)parsetree;

                /* Set up key for hashtable search */
                key.dbname_len = strlen(stmt->dbname);
                key.dbname_ptr = stmt->dbname;

                /*
                 * Lookup the hash table entry with exclusive lock. We have to
                 * manipulate the entries immediately anyways..
                 */
                LWLockAcquire(shared_state->lock, LW_EXCLUSIVE);

                entry = (StormStatsEntry *) hash_search(StatsEntryHash, &key, HASH_REMOVE, NULL);

                /* What do we do if we do not find an entry? We WARN for now */
                if (!entry && !stmt->missing_ok)
                    ereport(WARNING,
                        (errmsg("entry does not exist for database %s!",
                        stmt->dbname)));
                LWLockRelease(shared_state->lock);
                break;
            }
        default:
            /* Nothing */;
    }
}

void
_PG_init(void)
{
    if (!process_shared_preload_libraries_in_progress)
        return;

    DefineCustomIntVariable("storm_stats.max_tracked_databases",
                            "Sets the maximum number of databases tracked.",
                            NULL,
                            &max_tracked_dbs,
                            1000,
                            1,
                            INT_MAX,
                            PGC_POSTMASTER,
                            0,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomBoolVariable("storm_stats.save",
                             "Save statistics across server shutdowns.",
                             NULL,
                             &sp_save,
                             true,
                             PGC_SIGHUP,
                             0,
                             NULL,
                             NULL,
                             NULL);

    EmitWarningsOnPlaceholders("storm_stats");

    RequestAddinShmemSpace(hash_memsize());
    RequestNamedLWLockTranche("storm_stats", 1);

    prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook = sp_shmem_startup;
    planner_hook = planner_callback;

    original_client_auth_hook = ClientAuthentication_hook;
    ClientAuthentication_hook = auth_check;

    prev_ProcessUtility = ProcessUtility_hook;
    ProcessUtility_hook = ProcessUtility_callback;

    elog( DEBUG1, "STORMSTATS: plugin loaded" );
}

void
_PG_fini(void)
{
    shmem_startup_hook = prev_shmem_startup_hook;
    planner_hook = NULL;
    ProcessUtility_hook = prev_ProcessUtility;

    elog( DEBUG1, "STORMSTATS: plugin unloaded." );
}

static void sp_shmem_startup(void)
{
    HASHCTL        event_ctl;
    bool        found;
    FILE           *file;
    uint32        header;
    int32        num;
    int32        i;
    int        buffer_size;
    char           *buffer = NULL;

    if (prev_shmem_startup_hook)
        prev_shmem_startup_hook();

    /*
     * Create or attach to the shared memory state, including hash table
     */
    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

    shared_state = ShmemInitStruct("storm_stats state", sizeof(StormSharedState), &found);
    if (!shared_state)
        elog(ERROR, "out of shared memory");

    if (!found)
        shared_state->lock = &(GetNamedLWLockTranche("storm_stats"))->lock;

    memset(&event_ctl, 0, sizeof(event_ctl));

    event_ctl.keysize = sizeof(ssHashKey);
    event_ctl.entrysize = sizeof(StormStatsEntry) + NAMEDATALEN;
    event_ctl.hash = ss_hash_fn;
    event_ctl.match = ss_match_fn;

    StatsEntryHash = ShmemInitHash("storm_stats event hash", max_tracked_dbs,
                                   max_tracked_dbs, &event_ctl,
                                   HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);
    if (!StatsEntryHash)
        elog(ERROR, "out of shared memory");

    LWLockRelease(AddinShmemInitLock);

    /*
     * If we're in the postmaster (or a standalone backend...), set up a shmem
     * exit hook to dump the statistics to disk.
     */
    if (!IsUnderPostmaster)
        on_shmem_exit(sp_shmem_shutdown, (Datum) 0);

    /*
     * Attempt to load old statistics from the dump file, if this is the first
     * time through and we weren't told not to.
     */
    if (found || !sp_save)
        return;

    /*
     * Note: we don't bother with locks here, because there should be no other
     * processes running when this code is reached.
     */
    file = AllocateFile(STORM_DUMP_FILE, PG_BINARY_R);
    if (file == NULL)
    {
        if (errno == ENOENT)
            return;                         /* ignore not-found error */
        goto error;
    }

    buffer_size = NAMEDATALEN;
    buffer = (char *) palloc(buffer_size);

    if (fread(&header, sizeof(uint32), 1, file) != 1 ||
        header != STORM_FILE_HEADER ||
        fread(&num, sizeof(int32), 1, file) != 1)
        goto error;

    for (i = 0; i < num; i++)
    {
        StormStatsEntry  temp;
        StormStatsEntry  *entry;

        if (fread(&temp, offsetof(StormStatsEntry, mutex), 1, file) != 1)
            goto error;

        if (temp.key.dbname_len >= buffer_size)
        {
            buffer = (char *) repalloc(buffer, temp.key.dbname_len + 1);
            buffer_size = temp.key.dbname_len + 1;
        }

        if (fread(buffer, 1, temp.key.dbname_len, file) != temp.key.dbname_len)
            goto error;
        buffer[temp.key.dbname_len] = '\0';

        temp.key.dbname_ptr = buffer;

        /* make the hashtable entry (discards old entries if too many) */
        entry = alloc_event_entry(&temp.key);

        /* copy in the actual stats */
        entry->counters = temp.counters;
    }

    pfree(buffer);
    FreeFile(file);
    return;

error:
    ereport(LOG,
            (errcode_for_file_access(),
             errmsg("could not read stormstats file \"%s\": %m",
                    STORM_DUMP_FILE)));
    if (buffer)
        pfree(buffer);
    if (file)
        FreeFile(file);
    /* If possible, throw away the bogus file; ignore any error */
    unlink(STORM_DUMP_FILE);
}

/*
 * shmem_shutdown hook: Dump statistics into file.
 *
 * Note: we don't bother with acquiring lock, because there should be no
 * other processes running when this is called.
 */
static void
sp_shmem_shutdown(int code, Datum arg)
{
    FILE       *file;
    HASH_SEQ_STATUS hash_seq;
    int32        num_entries;
    StormStatsEntry  *entry;

    /* Don't try to dump during a crash. */
    if (code)
        return;

    /* Safety check ... shouldn't get here unless shmem is set up. */
    if (!shared_state || !StatsEntryHash)
        return;

    /* Don't dump if told not to. */
    if (!sp_save)
        return;

    file = AllocateFile(STORM_DUMP_FILE, PG_BINARY_W);
    if (file == NULL)
        goto error;

    if (fwrite(&STORM_FILE_HEADER, sizeof(uint32), 1, file) != 1)
        goto error;
    num_entries = hash_get_num_entries(StatsEntryHash);
    if (fwrite(&num_entries, sizeof(int32), 1, file) != 1)
        goto error;

    hash_seq_init(&hash_seq, StatsEntryHash);
    while ((entry = hash_seq_search(&hash_seq)) != NULL)
    {
        int            len = entry->key.dbname_len;

        if (fwrite(entry, offsetof(StormStatsEntry, mutex), 1, file) != 1 ||
            fwrite(entry->dbname, 1, len, file) != len)
            goto error;
    }

    if (FreeFile(file))
    {
        file = NULL;
        goto error;
    }

    return;

error:
    ereport(LOG,
            (errcode_for_file_access(),
             errmsg("could not write stormstats file \"%s\": %m",
                    STORM_DUMP_FILE)));

    if (file)
        FreeFile(file);
    unlink(STORM_DUMP_FILE);
}

PlannedStmt *planner_callback(Query *parse, int cursorOptions, ParamListInfo boundParams)
{
    PlannedStmt  *plan;

    elog( DEBUG1, "STORMSTATS: using plugin." );

    /* Generate a plan */
    plan = standard_planner(parse, cursorOptions, boundParams);

    stats_store(get_database_name(MyDatabaseId), parse->commandType, false, false);

    return plan;
}

void auth_check(Port *port, int status)
{
    elog( DEBUG1, "STORMSTATS: using plugin." );

    /*
     * Any other plugins which use ClientAuthentication_hook.
     */
    if (original_client_auth_hook)
        original_client_auth_hook(port, status);

    if (status == STATUS_OK)
    {
        stats_store(port->database_name, CMD_UNKNOWN, true, false);
    }
}

static Size hash_memsize(void)
{
    Size        size;
    Size        events_size;
    Size        state_size;

    events_size = hash_estimate_size(max_tracked_dbs, MAXALIGN(sizeof(StormStatsEntry)));
    state_size = MAXALIGN(sizeof(StormSharedState));

    size = add_size(events_size, state_size);

    return size;
}

static StormStatsEntry *alloc_event_entry(ssHashKey *key)
{
    StormStatsEntry *entry;
    bool            found;

    if (hash_get_num_entries(StatsEntryHash) >= max_tracked_dbs)
    {
        elog(ERROR, "STORMSTATS: The maximum number of tracked databases have been reached");
        return NULL;
    }

    /* Find or create an entry with desired hash code */
    entry = (StormStatsEntry *) hash_search(StatsEntryHash, key, HASH_ENTER, &found);

    if (!found)
    {
        entry->key.dbname_ptr = entry->dbname;
        memset(&entry->counters, 0, sizeof(EventCounters));
        SpinLockInit(&entry->mutex);

        memcpy(entry->dbname, key->dbname_ptr, key->dbname_len);
        entry->dbname[key->dbname_len] = '\0';
    }

    return entry;
}

/*
 * Calculate hash value for a key
 */
static uint32
ss_hash_fn(const void *key, Size keysize)
{
    const ssHashKey *k = (const ssHashKey *) key;

    /* we don't bother to include encoding in the hash */
    return DatumGetUInt32(hash_any((const unsigned char *) k->dbname_ptr,
                                k->dbname_len));
}

/*
 * Compare two keys - zero means match
 */
static int
ss_match_fn(const void *key1, const void *key2, Size keysize)
{
    const ssHashKey *k1 = (const ssHashKey *) key1;
    const ssHashKey *k2 = (const ssHashKey *) key2;

    if (k1->dbname_len == k2->dbname_len &&
        memcmp(k1->dbname_ptr, k2->dbname_ptr, k1->dbname_len) == 0)
        return 0;
    else
        return 1;
}

static void
stats_store(const char *dbname, CmdType c, bool isConnEvent, bool isUtilEvent)
{
    ssHashKey        key;
    StormStatsEntry  *entry;

    if (!shared_state || !StatsEntryHash)
        return;

    /* Set up key for hashtable search */
    key.dbname_len = strlen(dbname);
    key.dbname_ptr = dbname;

    /* Lookup the hash table entry with shared lock. */
    LWLockAcquire(shared_state->lock, LW_SHARED);

    entry = (StormStatsEntry *) hash_search(StatsEntryHash, &key, HASH_FIND, NULL);
    if (!entry)
    {
        /* Must acquire exclusive lock to add a new entry. */
        LWLockRelease(shared_state->lock);
        LWLockAcquire(shared_state->lock, LW_EXCLUSIVE);
        entry = alloc_event_entry(&key);
    }

    /* Grab the spinlock while updating the counters. */
    {
        volatile StormStatsEntry *e = (volatile StormStatsEntry *) entry;

        SpinLockAcquire(&e->mutex);

        if (isConnEvent) {
            e->counters.conn_cnt += 1;
        } else if (isUtilEvent) {
            e->counters.ddl_cnt += 1;
        } else {
            switch (c)
            {
                case CMD_SELECT:
                    e->counters.select_cnt += 1;
                    break;
                case CMD_INSERT:
                    e->counters.insert_cnt += 1;
                    break;
                case CMD_UPDATE:
                    e->counters.update_cnt += 1;
                    break;
                case CMD_DELETE:
                    e->counters.delete_cnt += 1;
                    break;
                case CMD_UTILITY:
                case CMD_UNKNOWN:
                case CMD_NOTHING:
                    break;
            }
        }
        SpinLockRelease(&e->mutex);
    }

    LWLockRelease(shared_state->lock);
}

/*
 * Gather statistics from remote coordinators
 */
static HTAB *
storm_gather_remote_coord_info(Oid funcid)
{
    bool        found;
    EState        *estate;
    TupleTableSlot *result;
    RemoteQuery *step;
    RemoteQueryState *node;
    int            i, ncolumns;
    HeapTuple    tp;
    TupleDesc    tupdesc;
    MemoryContext oldcontext;
    HTAB        *LocalStatsHash;
    HASHCTL        event_ctl;

    /*
     * We will sort output by database name, should make adding up info from
     * multiple remote coordinators easier
     */
    char *query = "SELECT * FROM storm_database_stats() ORDER BY datname";

    /* Build up RemoteQuery */
    step = makeNode(RemoteQuery);

    step->combine_type = COMBINE_TYPE_NONE;
    step->exec_nodes = NULL;
    step->sql_statement = query;
    step->force_autocommit = false;
    step->read_only = true;
    step->exec_type = EXEC_ON_COORDS;

    /* Build a local hash table to contain info from remote nodes */
    memset(&event_ctl, 0, sizeof(event_ctl));

    event_ctl.keysize = sizeof(ssHashKey);
    event_ctl.entrysize = sizeof(LocalStatsEntry);
    event_ctl.hash = ss_hash_fn;
    event_ctl.match = ss_match_fn;

    LocalStatsHash = hash_create("storm_stats local hash", max_tracked_dbs,
                                   &event_ctl,
                                   HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);
    if (!LocalStatsHash)
        elog(ERROR, "out of memory");

    /*
     * Add targetlist entries. We use the proc oid to get the tupledesc for
     * this. We could have hardcoded the types of existing set of columns, but
     * if we change the columns later for whatever reasons, this keeps us sane
     */
    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));

    /* Build a tupdesc of all the OUT parameters */
    tupdesc = build_function_result_tupdesc_t(tp);
    ncolumns = tupdesc->natts;

    for (i = 0; i < ncolumns; ++i)
    {
        Var           *var;
        TargetEntry *tle;

        var = makeVar(1,
                      tupdesc->attrs[i]->attnum,
                      tupdesc->attrs[i]->atttypid,
                      tupdesc->attrs[i]->atttypmod,
                      InvalidOid,
                      0);

        tle = makeTargetEntry((Expr *) var, tupdesc->attrs[i]->attnum, NULL, false);
        step->scan.plan.targetlist = lappend(step->scan.plan.targetlist, tle);
    }
    ReleaseSysCache(tp);

    /* Execute query on the data nodes */
    estate = CreateExecutorState();

    oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

    estate->es_snapshot = GetActiveSnapshot();

    node = ExecInitRemoteQuery(step, estate, 0);
    MemoryContextSwitchTo(oldcontext);
    /* get ready to combine results */
    result = ExecRemoteQuery((PlanState *) node);
    while (result != NULL && !TupIsNull(result))
    {
        Datum     value;
        bool    isnull;
        ssHashKey        key;
        LocalStatsEntry  *entry;
        char             *dbname;

        /* Process statistics from the coordinator nodes */
        value = slot_getattr(result, 1, &isnull); /* datname */
        if (isnull)
            ereport(ERROR,
                    (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                     errmsg("database name must not be null")));

        dbname = TextDatumGetCString(value);

        /* Set up key for hashtable search */
        key.dbname_len = strlen(dbname);
        key.dbname_ptr = dbname;

        /* Find or create an entry with desired hash code */
        entry = (LocalStatsEntry *) hash_search(LocalStatsHash, &key, HASH_ENTER, &found);
        if (!found)
        {
            entry->key.dbname_ptr = entry->dbname;
            memset(&entry->counters, 0, sizeof(EventCounters));
            memcpy(entry->dbname, key.dbname_ptr, key.dbname_len);
            entry->dbname[key.dbname_len] = '\0';
        }

        value = slot_getattr(result, 2, &isnull); /* conn_cnt */
        if (!isnull)
            entry->counters.conn_cnt += DatumGetInt64(value);

        value = slot_getattr(result, 3, &isnull); /* select_cnt */
        if (!isnull)
            entry->counters.select_cnt += DatumGetInt64(value);

        value = slot_getattr(result, 4, &isnull); /* insert_cnt */
        if (!isnull)
            entry->counters.insert_cnt += DatumGetInt64(value);

        value = slot_getattr(result, 5, &isnull); /* update_cnt */
        if (!isnull)
            entry->counters.update_cnt += DatumGetInt64(value);

        value = slot_getattr(result, 6, &isnull); /* delete_cnt */
        if (!isnull)
            entry->counters.delete_cnt += DatumGetInt64(value);

        value = slot_getattr(result, 7, &isnull); /* ddl_cnt */
        if (!isnull)
            entry->counters.ddl_cnt += DatumGetInt64(value);

        /* fetch next */
        result = ExecRemoteQuery((PlanState *) node);
    }
    ExecEndRemoteQuery(node);

    return LocalStatsHash;
}

Datum storm_database_stats(PG_FUNCTION_ARGS)
{
    ReturnSetInfo       *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    TupleDesc           tupdesc;
    Tuplestorestate     *tupstore;
    MemoryContext       per_query_ctx;
    MemoryContext       oldcontext;
    HASH_SEQ_STATUS     hash_seq;
    StormStatsEntry        *entry;
    HTAB                *LocalStatsHash = NULL;

    if (IS_PGXC_DATANODE)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("invalid invocation on data node")));

    if (!shared_state || !StatsEntryHash)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("storm_stats must be loaded via shared_preload_libraries")));

    /* check to see if caller supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("set-valued function called in context that cannot accept a set")));
    if (!(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("materialize mode required, but it is not " \
                        "allowed in this context")));

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    /*
     * Query the rest of the coordinators and get their stats. Do this only if
     * you are query originator. Otherwise just provide your local info and
     * return
     */
    if (IsConnFromApp())
        LocalStatsHash = storm_gather_remote_coord_info(fcinfo->flinfo->fn_oid);

    tupdesc = CreateTemplateTupleDesc(STORM_STATS_COLS, false);
    TupleDescInitEntry(tupdesc, (AttrNumber) 1, "dbname",     TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 2, "conn_cnt",   INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 3, "select_cnt", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 4, "insert_cnt", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 5, "update_cnt", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 6, "delete_cnt", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 7, "ddl_cnt",    INT8OID, -1, 0);

    tupstore = tuplestore_begin_heap(true, false, work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;

    LWLockAcquire(shared_state->lock, LW_SHARED);

    hash_seq_init(&hash_seq, StatsEntryHash);
    while ((entry = hash_seq_search(&hash_seq)) != NULL)
    {
        Datum           values[STORM_STATS_COLS];
        bool            nulls[STORM_STATS_COLS];
        int             i = 0;
        EventCounters   tmp, lcl;

        /* generate junk in short-term context */
        MemoryContextSwitchTo(oldcontext);

        memset(values, 0, sizeof(values));
        memset(nulls, 0, sizeof(nulls));
        memset(&lcl, 0, sizeof(lcl));

        values[i++] = CStringGetTextDatum(entry->dbname);

        /* copy counters to a local variable to keep locking time short */
        {
            volatile StormStatsEntry *e = (volatile StormStatsEntry *) entry;

            SpinLockAcquire(&e->mutex);
            tmp = e->counters;
            SpinLockRelease(&e->mutex);
        }

        /* See if LocalStatsHash has additional info to provide */
        if (LocalStatsHash)
        {
            ssHashKey        key;
            LocalStatsEntry    *le;
            bool             found;

            /* Set up key for hashtable search */
            key.dbname_len = strlen(entry->dbname);
            key.dbname_ptr = entry->dbname;

            /* Find an entry with desired hash code */
            le = (LocalStatsEntry *) hash_search(LocalStatsHash, &key, HASH_FIND, &found);

            /*
             * What should we do if entry is not found on the other
             * coordinators? WARN for now..
             */
            if (!found)
            {
                ereport(WARNING,
                    (errmsg("no stats collected from remote coordinators for database %s!",
                     entry->dbname)));
            }
            else
            {
                tmp.ddl_cnt += le->counters.ddl_cnt;
                tmp.conn_cnt += le->counters.conn_cnt;
                tmp.select_cnt += le->counters.select_cnt;
                tmp.insert_cnt += le->counters.insert_cnt;
                tmp.update_cnt += le->counters.update_cnt;
                tmp.delete_cnt += le->counters.delete_cnt;
            }
        }

        values[i++] = Int64GetDatumFast(tmp.conn_cnt);
        values[i++] = Int64GetDatumFast(tmp.select_cnt);
        values[i++] = Int64GetDatumFast(tmp.insert_cnt);
        values[i++] = Int64GetDatumFast(tmp.update_cnt);
        values[i++] = Int64GetDatumFast(tmp.delete_cnt);
        values[i++] = Int64GetDatumFast(tmp.ddl_cnt);

        Assert(i == STORM_STATS_COLS);

        /* switch to appropriate context while storing the tuple */
        MemoryContextSwitchTo(per_query_ctx);
        tuplestore_putvalues(tupstore, tupdesc, values, nulls);
    }

    LWLockRelease(shared_state->lock);

    /* clean up and return the tuplestore */
    tuplestore_donestoring(tupstore);

    /* destroy local hash table */
    if (LocalStatsHash)
        hash_destroy(LocalStatsHash);

    MemoryContextSwitchTo(oldcontext);

    return (Datum) 0;
}
