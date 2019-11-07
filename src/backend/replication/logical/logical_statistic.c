/* -------------------------------------------------------------------------
 *
 * Portions Copyright (c) 2018, Tencent TBase Group
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"

#include "replication/logical_statistic.h"
#include "utils/hsearch.h"
#include "storage/shmem.h"
#include "storage/lwlock.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "access/htup_details.h"

/*
  * check the data consistency between publication and subscription 
  * when reach the consistent point. We provide two ways:
  *
  * 1. tuple count: record the total tuples at publication and subscription,
  *     and check the count.
  * 2. tuple checksum: get hashkey(uint32) by hash(tuple), and sum all tuples'
  *     hashkey, then check the sum values.
  *
  * Both methods provide approximate results, and this can not guarantee the data
  * consistency between publication and subscription.
  *
  * all statistics data stored in shared memory with hashtable.
  * 
  */

/* use subscription name as key in hashtable  */
typedef struct 
{
    char subname[NAMEDATALEN];
} StatTag;

typedef struct
{
    int64     ntups_copy;        /* number of tuples copy out/in */
    int64     ntups_insert;      /* number of tuples inserted during replication */
    int64     ntups_delete;      /* number of tuples deleted during replication */
    int64     checksum_insert;   /* checksum of all tuples inserted during replication */
    int64     checksum_delete;   /* checksum of all tuples deleted during replication */
} StatisticData;

/* statistic data for publication in hashtable */
typedef struct
{
    StatTag    key;
    StatisticData data;
} StatEnt;

/* subscription oid and relid as key */
typedef struct
{
    Oid  subid;
    Oid  relid;
} TableStatTag;

typedef struct
{
    TableStatTag key;
    char state;
    StatisticData data;
} TableStatEnt;

int32 g_PubStatHashSize;
int32 g_PubTableStatHashSize;

/* used for publication level */
static HTAB *PubStatHash;

/* used for publication's table level */
static HTAB *PubTableStatHash;

/* if we need to record number of tuples */
static bool *pubStatCount;

/* if we need to record checksum of tuples */
static bool *pubStatChecksum;

int32 g_SubStatHashSize;
int32 g_SubTableStatHashSize;

/* used for subscription level */
static HTAB *SubStatHash;

/* used for subscription's table level */
static HTAB *SubTableStatHash;

/* if we need to record number of tuples */
static bool *subStatCount;

/* if we need to record checksum of tuples */
static bool *subStatChecksum;


/*
 * Estimate space needed for publication statistic hashtable 
 */
Size
PubStatDataShmemSize(int ssize, int tsize)
{
    Size space = 0;

    space += sizeof(bool);
    space += sizeof(bool);
    space += hash_estimate_size(ssize, sizeof(StatEnt));
    space += hash_estimate_size(tsize, sizeof(TableStatEnt));
    
    return space;
}

/*
 * HashCompareFunc for pubstat keys
 */
static int
pubstat_compare(const void *key1, const void *key2, Size keysize)
{
    Size        s_len = strlen((const char *) key1);
    return strncmp(key1, key2, (int)s_len);
}

/*
 * Initialize shmem hash table for publication statistic hashtable 
 */
void
InitPubStatData(int ssize, int tsize)
{
    bool        found;
    HASHCTL        info;

    info.keysize = sizeof(StatTag);
    info.entrysize = sizeof(StatEnt);
    info.match = pubstat_compare;

    PubStatHash = ShmemInitHash("Publication Statistic Data",
                                  ssize, ssize,
                                  &info,
                                  HASH_ELEM | HASH_COMPARE);

    memset(&info, 0, sizeof(HASHCTL));
    info.keysize = sizeof(TableStatTag);
    info.entrysize = sizeof(TableStatEnt);
    
    PubTableStatHash = ShmemInitHash("Publication's table Statistic Data",
                                  tsize, tsize,
                                  &info,
                                  HASH_ELEM | HASH_BLOBS);


    pubStatCount = (bool *)ShmemInitStruct("Publication Stat Count",
                                             sizeof(bool),
                                             &found);
    if (!found)
        *pubStatCount = false;

    pubStatChecksum = (bool *)ShmemInitStruct("Publication Stat Checksum",
                                             sizeof(bool),
                                             &found);
    if (!found)
        *pubStatChecksum = false;
}

/* update statistic data with subscription */
void
UpdatePubStatistics(char *subname, uint64 ntups_copy, uint64 ntups_insert, uint64 ntups_delete,
                                        uint64 checksum_insert, uint64 checksum_delete, bool init)
{
    bool found;
    StatTag key;
    StatEnt *ent;

    snprintf(key.subname, NAMEDATALEN, "%s", subname);

    LWLockAcquire(PubStatLock, LW_EXCLUSIVE);

    ent = hash_search(PubStatHash, &key, HASH_ENTER, &found);

    /* found entry in hashtable, update statistic data */
    if (found && !init)
    {
        ent->data.ntups_copy += ntups_copy;
        ent->data.ntups_insert += ntups_insert;
        ent->data.ntups_delete += ntups_delete;
        ent->data.checksum_insert += checksum_insert;
        ent->data.checksum_delete += checksum_delete;
    }
    else
    {
        /* not found, init by the data */
        ent->data.ntups_copy = ntups_copy;
        ent->data.ntups_insert = ntups_insert;
        ent->data.ntups_delete = ntups_delete;
        ent->data.checksum_insert = checksum_insert;
        ent->data.checksum_delete = checksum_delete;
    }

    LWLockRelease(PubStatLock);
}

void
UpdatePubTableStatistics(Oid subid, Oid relid, uint64 ntups_copy, uint64 ntups_insert, uint64 ntups_delete,
                                        uint64 checksum_insert, uint64 checksum_delete, bool init)
{
    bool found;
    TableStatTag key;
    TableStatEnt *ent;

    key.subid = subid;
    key.relid = relid;

    LWLockAcquire(PubStatLock, LW_EXCLUSIVE);

    ent = hash_search(PubTableStatHash, &key, HASH_ENTER, &found);

    /* found entry in hashtable, update statistic data */
    if (found && !init)
    {
        ent->data.ntups_copy  +=  ntups_copy;
        ent->data.ntups_insert  +=  ntups_insert;
        ent->data.ntups_delete  +=  ntups_delete;
        ent->data.checksum_insert  +=  checksum_insert;
        ent->data.checksum_delete  +=  checksum_delete;
    }
    else
    {
        /* not found, init by the data */
        ent->data.ntups_copy =  ntups_copy;
        ent->data.ntups_insert =  ntups_insert;
        ent->data.ntups_delete =  ntups_delete;
        ent->data.checksum_insert =  checksum_insert;
        ent->data.checksum_delete =  checksum_delete;
    }

    LWLockRelease(PubStatLock);
}

/* remove statistic data entry in hashtable with subname */
void
RemovePubStatistics(char *subname)
{
    StatTag key;

    snprintf(key.subname, NAMEDATALEN, "%s", subname);

    LWLockAcquire(PubStatLock, LW_EXCLUSIVE);

    hash_search(PubStatHash, &key, HASH_REMOVE, NULL);

    LWLockRelease(PubStatLock);
}

/* set data consistency check when needed */
void
SetPubStatCheck(bool pubstatcount, bool pubstatchecksum)
{
    LWLockAcquire(PubStatLock, LW_EXCLUSIVE);

    *pubStatCount = pubstatcount;
    *pubStatChecksum = pubstatchecksum;

    LWLockRelease(PubStatLock);
}

/*
 * Estimate space needed for subscription statistic hashtable 
 */
Size
SubStatDataShmemSize(int ssize, int tsize)
{
    Size space = 0;

    space += sizeof(bool);
    space += sizeof(bool);
    space += hash_estimate_size(ssize, sizeof(StatEnt));
    space += hash_estimate_size(tsize, sizeof(TableStatEnt));
    
    return space;
}

/*
 * HashCompareFunc for subscription keys
 */
static int
substat_compare(const void *key1, const void *key2, Size keysize)
{
    Size        s_len = strlen((const char *) key1);
    return strncmp(key1, key2, (int)s_len);
}

/*
 * Initialize shmem hash table for subscription statistic hashtable 
 */
void
InitSubStatData(int ssize, int tsize)
{
    bool        found;
    HASHCTL        info;

    info.keysize = sizeof(StatTag);
    info.entrysize = sizeof(StatEnt);
    info.match = substat_compare;

    SubStatHash = ShmemInitHash("Subscription Statistic Data",
                                  ssize, ssize,
                                  &info,
                                  HASH_ELEM | HASH_COMPARE);

    memset(&info, 0, sizeof(HASHCTL));
    info.keysize = sizeof(TableStatTag);
    info.entrysize = sizeof(TableStatEnt);

    SubTableStatHash = ShmemInitHash("Subscription's table Statistic Data",
                                  tsize, tsize,
                                  &info,
                                  HASH_ELEM | HASH_BLOBS);


    subStatCount = (bool *)ShmemInitStruct("Subscription Stat Count",
                                             sizeof(bool),
                                             &found);
    if (!found)
        *subStatCount = false;

    subStatChecksum = (bool *)ShmemInitStruct("Subscription Stat Checksum",
                                             sizeof(bool),
                                             &found);
    if (!found)
        *subStatChecksum = false;
}

/* update statistic data with subscription */
void
UpdateSubStatistics(char *subname, uint64 ntups_copy, uint64 ntups_insert, uint64 ntups_delete,
                                        uint64 checksum_insert, uint64 checksum_delete, bool init)
{
    bool found;
    StatTag key;
    StatEnt *ent;

    snprintf(key.subname, NAMEDATALEN, "%s", subname);

    LWLockAcquire(SubStatLock, LW_EXCLUSIVE);

    ent = hash_search(SubStatHash, &key, HASH_ENTER, &found);

    /* found entry in hashtable, update statistic data */
    if (found && !init)
    {
        ent->data.checksum_insert += checksum_insert;
        ent->data.checksum_delete += checksum_delete;
        ent->data.ntups_copy += ntups_copy;
        ent->data.ntups_insert += ntups_insert;
        ent->data.ntups_delete += ntups_delete;
    }
    else
    {
        /* not found, init by the data */
        ent->data.checksum_insert = checksum_insert;
        ent->data.checksum_delete = checksum_delete;
        ent->data.ntups_copy = ntups_copy;
        ent->data.ntups_insert = ntups_insert;
        ent->data.ntups_delete = ntups_delete;
    }

    LWLockRelease(SubStatLock);
}

void
GetSubTableEntry(Oid subid, Oid relid, void **entry, CmdType cmd)
{
    bool found;
    TableStatTag key;
    TableStatEnt *ent;

    if (*entry == NULL)
    {
        key.subid = subid;
        key.relid = relid;

        LWLockAcquire(SubStatLock, LW_EXCLUSIVE);

        ent = hash_search(SubTableStatHash, &key, HASH_ENTER, &found);

        if (!found)
        {
            ent->data.ntups_insert = 0;
            ent->data.ntups_delete = 0;
            ent->data.checksum_insert = 0;
            ent->data.checksum_delete = 0;
        }

        LWLockRelease(SubStatLock);

        *entry = (void *)ent;
    }
    else
    {
        ent = (TableStatEnt *)(*entry);
    }

    switch(cmd)
    {
        case CMD_INSERT:
            {
                ent->data.ntups_insert++;
            }
            break;
        case CMD_UPDATE:
            {
                ent->data.ntups_insert++;
                ent->data.ntups_delete++;
            }
            break;
        case CMD_DELETE:
            {
                ent->data.ntups_delete++;
            }
            break;
        default:
            break;
    }

}

void
UpdateSubTableStatistics(Oid subid, Oid relid, uint64 ntups_copy, uint64 ntups_insert, uint64 ntups_delete,
                                        uint64 checksum_insert, uint64 checksum_delete, char state, bool init)
{
    bool found;
    TableStatTag key;
    TableStatEnt *ent;

    key.subid = subid;
    key.relid = relid;

    LWLockAcquire(SubStatLock, LW_EXCLUSIVE);

    ent = hash_search(SubTableStatHash, &key, HASH_ENTER, &found);

    /* found entry in hashtable, update statistic data */
    if (found && !init)
    {
        ent->data.ntups_copy += ntups_copy;
        ent->data.ntups_insert += ntups_insert;
        ent->data.ntups_delete += ntups_delete;
        ent->data.checksum_insert += checksum_insert;
        ent->data.checksum_delete += checksum_delete;
        ent->state = state;
    }
    else
    {
        /* not found, init by the data */
        ent->data.ntups_copy = ntups_copy;
        ent->data.ntups_insert = ntups_insert;
        ent->data.ntups_delete = ntups_delete;
        ent->data.checksum_insert = checksum_insert;
        ent->data.checksum_delete = checksum_delete;
        ent->state = state;
    }

    LWLockRelease(SubStatLock);
}


/* remove statistic data entry in hashtable with subname */
void
RemoveSubStatistics(char *subname)
{
    StatTag key;

    snprintf(key.subname, NAMEDATALEN, "%s", subname);

    LWLockAcquire(SubStatLock, LW_EXCLUSIVE);

    hash_search(SubStatHash, &key, HASH_REMOVE, NULL);

    LWLockRelease(SubStatLock);
}

/* set data consistency check when needed */
void
SetSubStatCheck(bool substatcount, bool substatchecksum)
{
    LWLockAcquire(SubStatLock, LW_EXCLUSIVE);

    *subStatCount = substatcount;
    *subStatChecksum = substatchecksum;

    LWLockRelease(SubStatLock);
}

/*********************************************************************/
/*** function defined ****************************************************/
/*********************************************************************/
typedef struct
{
    HASH_SEQ_STATUS status;
}StatInfo;

/* show statistic data of one publication */
Datum tbase_get_pub_stat(PG_FUNCTION_ARGS)
{
#define NCOLUMNS 6
    bool        found;
    StatTag     key;
    StatEnt     *ent;
    Datum        values[NCOLUMNS];
    bool        nulls[NCOLUMNS];
    TupleDesc    tupdesc;
    HeapTuple    htup;
    char        *subname;

    subname = text_to_cstring(PG_GETARG_TEXT_P(0));

    snprintf(key.subname, NAMEDATALEN, "%s", subname);

    tupdesc = CreateTemplateTupleDesc(NCOLUMNS, false);
    TupleDescInitEntry(tupdesc, (AttrNumber)1, "subscription_name",
                       TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)2, "ntups_copyOut",
                       INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)3, "ntups_insert",
                       INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)4, "ntups_delete",
                       INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)5, "checksum_insert",
                       INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)6, "checksum_delete",
                       INT8OID, -1, 0);
    tupdesc = BlessTupleDesc(tupdesc);

    LWLockAcquire(PubStatLock, LW_SHARED);

    ent = hash_search(PubStatHash, &key, HASH_FIND, &found);

    if (!found)
    {
        memset(nulls, true, sizeof(nulls));
    }
    else
    {
        values[0] = PointerGetDatum( cstring_to_text(ent->key.subname));
        nulls[0] = false;

        values[1] = Int64GetDatum( ent->data.ntups_copy);
        nulls[1] = false;

        values[2] = Int64GetDatum( ent->data.ntups_insert);
        nulls[2] = false;

        values[3] = Int64GetDatum( ent->data.ntups_delete);
        nulls[3] = false;

        values[4] = Int64GetDatum( ent->data.checksum_insert);
        nulls[4] = false;

        values[5] = Int64GetDatum( ent->data.checksum_delete);
        nulls[5] = false;
    }

    LWLockRelease(PubStatLock);

    htup = heap_form_tuple(tupdesc, values, nulls);

    PG_RETURN_DATUM(HeapTupleGetDatum(htup));
}

/* show statistic data of all publications with its tables */
Datum tbase_get_all_pub_stat(PG_FUNCTION_ARGS)
{
#define NCOLUMNS 6
    FuncCallContext     *funcctx;
    StatEnt             *ent;
    StatInfo            *info;
    
    if (SRF_IS_FIRSTCALL())
    {        
        MemoryContext oldcontext;
        TupleDesc      tupdesc;
        
        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        
        tupdesc = CreateTemplateTupleDesc(NCOLUMNS, false);
        TupleDescInitEntry(tupdesc, (AttrNumber)1, "subscription_name",
                           TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "ntups_copyOut",
                           INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "ntups_insert",
                           INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)4, "ntups_delete",
                           INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)5, "checksum_insert",
                           INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)6, "checksum_delete",
                           INT8OID, -1, 0);

        funcctx->user_fctx = palloc0(sizeof(StatInfo));
        info = (StatInfo*)funcctx->user_fctx;
        
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        LWLockAcquire(PubStatLock, LW_SHARED);
        hash_seq_init(&info->status, PubStatHash);
        MemoryContextSwitchTo(oldcontext);
    }


    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    info = (StatInfo*) funcctx->user_fctx; 
    if ( ((ent = (StatEnt *) hash_seq_search(&info->status)) != NULL) )
    {
        /* for each row */
        bool        nulls[NCOLUMNS];
        Datum        values[NCOLUMNS];
        HeapTuple    tuple;        

        MemSet(values, 0, sizeof(values));
        MemSet(nulls, 0, sizeof(nulls));

        values[5] = Int64GetDatum(ent->data.checksum_delete);
        
        values[4] = Int64GetDatum(ent->data.checksum_insert);

        values[3] = Int64GetDatum(ent->data.ntups_delete);

        values[2] = Int64GetDatum(ent->data.ntups_insert);

        values[1] = Int64GetDatum(ent->data.ntups_copy);

        values[0] = PointerGetDatum(cstring_to_text(ent->key.subname));

        tuple = heap_form_tuple(funcctx->tuple_desc,  values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    else
    {
        /* nothing left */
        LWLockRelease(PubStatLock);
        SRF_RETURN_DONE(funcctx);
    }
}


/* show statistic data of one subscription */
Datum tbase_get_sub_stat(PG_FUNCTION_ARGS)
{
#define NCOLUMNS 6
    bool        found;
    StatTag     key;
    StatEnt     *ent;
    Datum        values[NCOLUMNS];
    bool        nulls[NCOLUMNS];
    TupleDesc    tupdesc;
    HeapTuple    htup;
    char        *subname;

    subname = text_to_cstring(PG_GETARG_TEXT_P(0));

    snprintf(key.subname, NAMEDATALEN, "%s", subname);

    tupdesc = CreateTemplateTupleDesc(NCOLUMNS,  false);
    TupleDescInitEntry(tupdesc, (AttrNumber) 1,  "subscription_name" ,
                       TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 2,  "ntups_copyIn" ,
                       INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 3,  "ntups_insert" ,
                       INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 4,  "ntups_delete" ,
                       INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 5,  "checksum_insert" ,
                       INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 6,  "checksum_delete" ,
                       INT8OID, -1, 0);
    tupdesc = BlessTupleDesc(tupdesc);

    LWLockAcquire(SubStatLock, LW_SHARED);

    ent = hash_search(SubStatHash, &key, HASH_FIND, &found);

    if (!found)
    {
        memset(nulls, true, sizeof(nulls));
    }
    else
    {
        nulls[0] = false;
        values[0] = PointerGetDatum(cstring_to_text(ent->key.subname));

        nulls[1] = false;
        values[1] = Int64GetDatum(ent->data.ntups_copy);

        nulls[2] = false;
        values[2] = Int64GetDatum(ent->data.ntups_insert);

        nulls[3] = false;
        values[3] = Int64GetDatum(ent->data.ntups_delete);

        nulls[4] = false;
        values[4] = Int64GetDatum(ent->data.checksum_insert);

        nulls[5] = false;
        values[5] = Int64GetDatum(ent->data.checksum_delete);
    }

    LWLockRelease(SubStatLock);

    htup = heap_form_tuple(tupdesc, values, nulls);

    PG_RETURN_DATUM(HeapTupleGetDatum(htup));
}


/* show statistic data of all subscription  */
Datum tbase_get_all_sub_stat(PG_FUNCTION_ARGS)
{
#define NCOLUMNS 6
    FuncCallContext     *funcctx;
    StatEnt             *ent;
    StatInfo            *info;
    
    if (SRF_IS_FIRSTCALL())
    {        
        MemoryContext oldcontext;
        TupleDesc      tupdesc;
        
        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        
        tupdesc = CreateTemplateTupleDesc(NCOLUMNS, false);
        TupleDescInitEntry(tupdesc, (AttrNumber) 1,  "subscription_name",
                           TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 2,  "ntups_copyIn",
                           INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 3,  "ntups_insert",
                           INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 4,  "ntups_delete",
                           INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 5,  "checksum_insert",
                           INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 6,  "checksum_delete",
                           INT8OID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        funcctx->user_fctx = palloc0(sizeof(StatInfo));
        info = (StatInfo*)funcctx->user_fctx;
        
        LWLockAcquire(SubStatLock, LW_SHARED);
        hash_seq_init(&info->status, SubStatHash);
        MemoryContextSwitchTo(oldcontext);
    }


    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    info = (StatInfo*) funcctx->user_fctx; 
    if (((ent = (StatEnt *) hash_seq_search(&info->status)) != NULL))
    {
        /* for each row */
        Datum        values[NCOLUMNS];
        HeapTuple    tuple;        
        bool        nulls[NCOLUMNS];

        MemSet(nulls, 0, sizeof(nulls));
        MemSet(values, 0, sizeof(values));

        values[0] = PointerGetDatum(cstring_to_text(ent->key.subname));

        values[1] = Int64GetDatum(ent->data.ntups_copy);

        values[2] = Int64GetDatum(ent->data.ntups_insert);

        values[3] = Int64GetDatum(ent->data.ntups_delete);

        values[4] = Int64GetDatum(ent->data.checksum_insert);

        values[5] = Int64GetDatum(ent->data.checksum_delete);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    else
    {
        /* nothing left */
        LWLockRelease(SubStatLock);
        SRF_RETURN_DONE(funcctx);
    }
}

/* show statistic data of one publication table */
Datum tbase_get_pubtable_stat(PG_FUNCTION_ARGS)
{
#define ATTR_COLUMNS 7
    bool         found;
    TableStatTag key;
    TableStatEnt *ent;
    Datum        values[ATTR_COLUMNS];
    bool        nulls[ATTR_COLUMNS];
    TupleDesc    tupdesc;
    HeapTuple    htup;

    key.subid = PG_GETARG_OID(0);
    key.relid = PG_GETARG_OID(1);

    tupdesc = CreateTemplateTupleDesc(ATTR_COLUMNS, false);
    TupleDescInitEntry(tupdesc, (AttrNumber) 1, "subscription_id",
                       OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 2, "relation_id",
                       OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 3, "ntups_copyOut",
                       INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 4, "ntups_insert",
                       INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 5, "ntups_delete",
                       INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 6, "checksum_insert",
                       INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 7, "checksum_delete",
                       INT8OID, -1, 0);
    tupdesc = BlessTupleDesc(tupdesc);

    LWLockAcquire(PubStatLock, LW_SHARED);

    ent = hash_search(PubTableStatHash, &key, HASH_FIND, &found);

    if (!found)
    {
        memset(nulls, true, sizeof(nulls));
    }
    else
    {
        values[0] = ObjectIdGetDatum(ent->key.subid);
        nulls[0] = false;

        values[1] = ObjectIdGetDatum(ent->key.relid);
        nulls[1] = false;

        values[2] = Int64GetDatum(ent->data.ntups_copy);
        nulls[2] = false;

        values[3] = Int64GetDatum(ent->data.ntups_insert);
        nulls[3] = false;

        values[4] = Int64GetDatum(ent->data.ntups_delete);
        nulls[4] = false;

        values[5] = Int64GetDatum(ent->data.checksum_insert);
        nulls[5] = false;

        values[6] = Int64GetDatum(ent->data.checksum_delete);
        nulls[6] = false;
    }

    LWLockRelease(PubStatLock);

    htup = heap_form_tuple(tupdesc, values, nulls);

    PG_RETURN_DATUM(HeapTupleGetDatum(htup));
}

/* show statistic data of all publication table */
Datum tbase_get_all_pubtable_stat(PG_FUNCTION_ARGS)
{
#define ATTR_COLUMNS 7
    FuncCallContext     *funcctx;
    TableStatEnt         *ent;
    StatInfo            *info;
    
    if (SRF_IS_FIRSTCALL())
    {        
        MemoryContext oldcontext;
        TupleDesc      tupdesc;
        
        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        
        tupdesc = CreateTemplateTupleDesc(ATTR_COLUMNS, false);
        TupleDescInitEntry(tupdesc, (AttrNumber) 1, "subscription_id",
                         OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 2, "relation_id",
                         OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 3, "ntups_copyOut",
                         INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 4, "ntups_insert",
                         INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 5, "ntups_delete",
                         INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 6, "checksum_insert",
                         INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 7, "checksum_delete",
                         INT8OID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        funcctx->user_fctx = palloc0(sizeof(StatInfo));
        info = (StatInfo*)funcctx->user_fctx;
        
        LWLockAcquire(PubStatLock, LW_SHARED);
        hash_seq_init(&info->status, PubTableStatHash);
        MemoryContextSwitchTo(oldcontext);
    }


    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    info = (StatInfo*)funcctx->user_fctx; 
    if (((ent = (TableStatEnt *) hash_seq_search(&info->status)) != NULL))
    {
        /* for each row */
        Datum        values[ATTR_COLUMNS];
        bool        nulls[ATTR_COLUMNS];
        HeapTuple    tuple;        

        MemSet(values, 0, sizeof(values));
        MemSet(nulls, 0, sizeof(nulls));

        values[0] = ObjectIdGetDatum(ent->key.subid);

        values[1] = ObjectIdGetDatum(ent->key.relid);

        values[2] = Int64GetDatum(ent->data.ntups_copy);

        values[3] = Int64GetDatum(ent->data.ntups_insert);

        values[4] = Int64GetDatum(ent->data.ntups_delete);

        values[5] = Int64GetDatum(ent->data.checksum_insert);

        values[6] = Int64GetDatum(ent->data.checksum_delete);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    else
    {
        /* nothing left */
        LWLockRelease(PubStatLock);
        SRF_RETURN_DONE(funcctx);
    }
}

Datum tbase_get_subtable_stat(PG_FUNCTION_ARGS)
{
#define SUBTABLE_COLUMNS 8
    bool         found;
    TableStatTag key;
    TableStatEnt *ent;
    Datum        values[SUBTABLE_COLUMNS];
    bool        nulls[SUBTABLE_COLUMNS];
    TupleDesc    tupdesc;
    HeapTuple    htup;

    key.subid = PG_GETARG_OID(0);
    key.relid = PG_GETARG_OID(1);

    tupdesc = CreateTemplateTupleDesc(SUBTABLE_COLUMNS, false);
    TupleDescInitEntry(tupdesc, (AttrNumber) 1, "subscription_id",
                       OIDOID, -1, 0 );
    TupleDescInitEntry(tupdesc, (AttrNumber) 2, "relation_id",
                       OIDOID, -1, 0 );
    TupleDescInitEntry(tupdesc, (AttrNumber) 3, "state",
                       CHAROID, -1, 0 );
    TupleDescInitEntry(tupdesc, (AttrNumber) 4, "ntups_copyIn",
                       INT8OID, -1, 0 );
    TupleDescInitEntry(tupdesc, (AttrNumber) 5, "ntups_insert",
                       INT8OID, -1, 0 );
    TupleDescInitEntry(tupdesc, (AttrNumber) 6, "ntups_delete",
                       INT8OID, -1, 0 );
    TupleDescInitEntry(tupdesc, (AttrNumber) 7, "checksum_insert",
                       INT8OID, -1, 0 );
    TupleDescInitEntry(tupdesc, (AttrNumber) 8, "checksum_delete",
                       INT8OID, -1, 0 );

    tupdesc = BlessTupleDesc(tupdesc);

    LWLockAcquire(SubStatLock, LW_SHARED);

    ent = hash_search(SubTableStatHash, &key, HASH_FIND, &found);

    if (!found)
    {
        memset(nulls, true, sizeof(nulls));
    }
    else
    {
        values[0] = ObjectIdGetDatum(ent->key.subid);
        nulls[0] = false;

        values[1] = ObjectIdGetDatum(ent->key.relid);
        nulls[1] = false;

        values[2] = CharGetDatum(ent->state);
        nulls[2] = false;

        values[3] = Int64GetDatum(ent->data.ntups_copy);
        nulls[3] = false;

        values[4] = Int64GetDatum(ent->data.ntups_insert);
        nulls[4] = false;

        values[5] = Int64GetDatum(ent->data.ntups_delete);
        nulls[5] = false;

        values[6] = Int64GetDatum(ent->data.checksum_insert);
        nulls[6] = false;

        values[7] = Int64GetDatum(ent->data.checksum_delete);
        nulls[7] = false;

    }

    LWLockRelease(SubStatLock);

    htup = heap_form_tuple(tupdesc, values, nulls);

    PG_RETURN_DATUM(HeapTupleGetDatum(htup));
}

/* show statistic data of all subscriptions with its tables */
Datum tbase_get_all_subtable_stat(PG_FUNCTION_ARGS)
{
#define SUBTABLE_COLUMNS 8
    FuncCallContext     *funcctx;
    TableStatEnt         *ent;
    StatInfo            *info;
    
    if (SRF_IS_FIRSTCALL())
    {        
        MemoryContext oldcontext;
        TupleDesc      tupdesc;
        
        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        
        tupdesc = CreateTemplateTupleDesc(SUBTABLE_COLUMNS, false);
        TupleDescInitEntry(tupdesc, (AttrNumber) 1, "subscription_id",
                           OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 2, "relation_id",
                           OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 3, "state",
                           CHAROID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 4, "ntups_copyIn",
                           INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 5, "ntups_insert",
                           INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 6, "ntups_delete",
                           INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 7, "checksum_insert",
                           INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 8, "checksum_delete",
                           INT8OID, -1, 0);


        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        funcctx->user_fctx = palloc0(sizeof(StatInfo));
        info = (StatInfo*)funcctx->user_fctx;
        
        LWLockAcquire(SubStatLock, LW_SHARED);
        hash_seq_init(&info->status, SubTableStatHash);
        MemoryContextSwitchTo(oldcontext);
    }


    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    info = (StatInfo*)funcctx->user_fctx; 
    if (((ent = (TableStatEnt *) hash_seq_search(&info->status)) != NULL))
    {
        /* for each row */
        Datum        values[SUBTABLE_COLUMNS];
        bool        nulls[SUBTABLE_COLUMNS];
        HeapTuple    tuple;        

        MemSet(values, 0, sizeof(values));
        MemSet(nulls, 0, sizeof(nulls));

        values[0] = ObjectIdGetDatum(ent->key.subid);

        values[1] = ObjectIdGetDatum(ent->key.relid);

        values[2] = CharGetDatum(ent->state);

        values[3] = Int64GetDatum(ent->data.ntups_copy);

        values[4] = Int64GetDatum(ent->data.ntups_insert);

        values[5] = Int64GetDatum(ent->data.ntups_delete);

        values[6] = Int64GetDatum(ent->data.checksum_insert);

        values[7] = Int64GetDatum(ent->data.checksum_delete);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    else
    {
        /* nothing left */
        LWLockRelease(SubStatLock);
        SRF_RETURN_DONE(funcctx);
    }
}

Datum tbase_set_pub_stat_check(PG_FUNCTION_ARGS)
{
    bool pubstatcount = PG_GETARG_BOOL(0);
    bool pubstatchecksum = PG_GETARG_BOOL(1);

    SetPubStatCheck(pubstatcount, pubstatchecksum);

    PG_RETURN_BOOL(true);
}
Datum tbase_set_sub_stat_check(PG_FUNCTION_ARGS)
{
    bool substatcount = PG_GETARG_BOOL(0);
    bool substatchecksum = PG_GETARG_BOOL(1);

    SetSubStatCheck(substatcount, substatchecksum);

    PG_RETURN_BOOL(true);
}

Datum tbase_get_pub_stat_check(PG_FUNCTION_ARGS)
{
#define NATTRS 2
    Datum        values[NATTRS];
    bool        nulls[NATTRS];
    TupleDesc    tupdesc;
    HeapTuple    htup;

    tupdesc = CreateTemplateTupleDesc(NATTRS, false);
    TupleDescInitEntry(tupdesc, (AttrNumber) 1, "statcount",
                       BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 2, "statchecksum",
                       BOOLOID, -1, 0);

    tupdesc = BlessTupleDesc(tupdesc);

    LWLockAcquire(PubStatLock, LW_SHARED);

    values[0] = BoolGetDatum(*pubStatCount);
    nulls[0] = false;

    values[1] = BoolGetDatum(*pubStatChecksum);
    nulls[1] = false;

    LWLockRelease(PubStatLock);

    htup = heap_form_tuple(tupdesc, values, nulls);

    PG_RETURN_DATUM(HeapTupleGetDatum(htup));
}


Datum tbase_get_sub_stat_check(PG_FUNCTION_ARGS)
{
#define NATTRS 2
    Datum        values[NATTRS];
    bool        nulls[NATTRS];
    TupleDesc    tupdesc;
    HeapTuple    htup;

    tupdesc = CreateTemplateTupleDesc(NATTRS, false);
    TupleDescInitEntry(tupdesc, (AttrNumber) 1, "statcount",
                       BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 2, "statchecksum",
                       BOOLOID, -1, 0);

    tupdesc = BlessTupleDesc(tupdesc);

    LWLockAcquire(SubStatLock, LW_SHARED);

    values[0] = BoolGetDatum(*subStatCount);
    nulls[0] = false;

    values[1] = BoolGetDatum(*subStatChecksum);
    nulls[1] = false;

    LWLockRelease(SubStatLock);

    htup = heap_form_tuple(tupdesc, values, nulls);

    PG_RETURN_DATUM(HeapTupleGetDatum(htup));
}

Datum tbase_remove_pub_stat(PG_FUNCTION_ARGS)
{
    char        *subname;
    HASH_SEQ_STATUS scan_status;
    StatEnt  *item;

    subname = text_to_cstring(PG_GETARG_TEXT_P(0));

    hash_seq_init(&scan_status, PubStatHash);

    LWLockAcquire(PubStatLock, LW_EXCLUSIVE);
    while ((item = (StatEnt *) hash_seq_search(&scan_status)) != NULL)
    {
        if (0 == strcmp(subname, item->key.subname))
        {
            hash_search(PubStatHash, (const void *) &item->key,
                            HASH_REMOVE, NULL);
        }
    }
    LWLockRelease(PubStatLock);

    PG_RETURN_BOOL(true);
}

Datum tbase_remove_pubtable_stat(PG_FUNCTION_ARGS)
{
    Oid subid;
    HASH_SEQ_STATUS scan_status;
    TableStatEnt  *item;

    subid = PG_GETARG_OID(0);

    hash_seq_init(&scan_status, PubTableStatHash);
    
    LWLockAcquire(PubStatLock, LW_EXCLUSIVE);
    while ((item = (TableStatEnt *) hash_seq_search(&scan_status)) != NULL)
    {
        if (item->key.subid == subid)
        {
            hash_search(PubTableStatHash, (const void *) &item->key,
                            HASH_REMOVE, NULL);
        }
    }
    LWLockRelease(PubStatLock);

    PG_RETURN_BOOL(true);
}

Datum tbase_remove_sub_stat(PG_FUNCTION_ARGS)
{
    char        *subname;
    HASH_SEQ_STATUS scan_status;
    StatEnt  *item;

    subname = text_to_cstring(PG_GETARG_TEXT_P(0));

    hash_seq_init(&scan_status, SubStatHash);

    LWLockAcquire(SubStatLock, LW_EXCLUSIVE);
    while ((item = (StatEnt *) hash_seq_search(&scan_status)) != NULL)
    {
        if (0 == strcmp(subname, item->key.subname))
        {
            hash_search(SubStatHash, (const void *) &item->key,
                            HASH_REMOVE, NULL);
        }
    }
    LWLockRelease(SubStatLock);

    PG_RETURN_BOOL(true);
}

Datum tbase_remove_subtable_stat(PG_FUNCTION_ARGS)
{
    Oid subid;
    HASH_SEQ_STATUS scan_status;
    TableStatEnt  *item;

    subid = PG_GETARG_OID(0);

    hash_seq_init(&scan_status, SubTableStatHash);
    
    LWLockAcquire(SubStatLock, LW_EXCLUSIVE);
    while ((item = (TableStatEnt *) hash_seq_search(&scan_status)) != NULL)
    {
        if (item->key.subid == subid)
        {
            hash_search(SubTableStatHash, (const void *) &item->key,
                            HASH_REMOVE, NULL);
        }
    }
    LWLockRelease(SubStatLock);

    PG_RETURN_BOOL(true);
}

