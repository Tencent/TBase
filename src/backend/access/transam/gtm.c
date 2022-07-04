/*-------------------------------------------------------------------------
 *
 * gtm.c
 *
 *      Module interfacing with GTM
 *
 *
 *-------------------------------------------------------------------------
 */

#include <sys/types.h>
#include <unistd.h>

#include "postgres.h"
#include "gtm/libpq-fe.h"
#include "gtm/gtm_client.h"
#include "access/gtm.h"
#include "access/transam.h"
#include "access/xact.h"
#include "utils/elog.h"
#include "miscadmin.h"
#include "pgxc/pgxc.h"
#include "gtm/gtm_c.h"
#include "postmaster/autovacuum.h"
#include "postmaster/clean2pc.h"
#include "postmaster/clustermon.h"
#include "postmaster/postmaster.h"
#include "storage/backendid.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/pg_rusage.h"
#ifdef __TBASE__
#include "catalog/pg_type.h"
#include "utils/memutils.h"
#include "access/htup_details.h"
#include "utils/builtins.h"
#include "fmgr.h"
#include "funcapi.h"
#include "catalog/pgxc_node.h"
#include "utils/syscache.h"
#include "utils/tqual.h"
#include "pgxc/nodemgr.h"
#include "access/xlog.h"
#include "storage/lmgr.h"
#endif

/* To access sequences */
#define GetMyCoordName \
    OidIsValid(MyCoordId) ? get_pgxc_nodename(MyCoordId) : ""
/* Configuration variables */
#ifdef __TBASE__
/*
 * This two value set while accept create/alter gtm node command
 */
char *NewGtmHost = NULL;
int      NewGtmPort = -1;
bool  g_GTM_skip_catalog = false;
char *gtm_unix_socket_directory = DEFAULT_PGSOCKET_DIR;
#endif

int reconnect_gtm_retry_times = 3;
int reconnect_gtm_retry_interval = 500;

char *GtmHost = NULL;
int GtmPort = 0;
static int GtmConnectTimeout = 60;
bool IsXidFromGTM = false;
bool gtm_backup_barrier = false;
extern bool FirstSnapshotSet;
bool GTMDebugPrint = false;
static GTM_Conn *conn;

/* Used to check if needed to commit/abort at datanodes */
GlobalTransactionId currentGxid = InvalidGlobalTransactionId;

#ifdef __TBASE__
typedef struct
{
    int            currIdx;        /* current PROCLOCK index */
    int         totalIdx;
    char        *data;
} PG_Storage_status;

#define    GTM_CHECK_DELTA  (10  * 1000 * 1000)
List *g_CreateSeqList = NULL;
List *g_DropSeqList   = NULL;
List *g_AlterSeqList  = NULL;
/* constant postfix for sequence to avoid same name */
#define GTM_SEQ_POSTFIX "_$TBASE$_sequence_temp_54312678712612"
static void CheckConnection(void);
static void ResetGTMConnection(void);
static int GetGTMStoreStatus(GTMStorageStatus *header);
static int GetGTMStoreSequence(GTM_StoredSeqInfo **store_seq);
static int GetGTMStoreTransaction(GTM_StoredTransactionInfo **store_txn);
static int CheckGTMStoreTransaction(GTMStorageTransactionStatus **store_txn, bool need_fix);
static int CheckGTMStoreSequence(GTMStorageSequneceStatus **store_seq, bool need_fix);
static void ResetGtmInfo(void);
extern GlobalTimestamp GetLatestCommitTS(void);


/* when modify this, had better modify GTMStorageError first*/
void RestoreSeqCreate(CreateInfo *create_info, int32 count)
{
    /* only restore when list is empty */
    if (0 == list_length(g_CreateSeqList) && count > 0)
    {
        int32 i = 0;
        for (i = 0; i < count; i++)
        {
            RegisterSeqCreate(create_info[i].name, create_info[i].gsk_type);
        }
    }
}

void RegisterSeqCreate(char *name, int32 type)
{
    GTM_SequenceKeyData *key      = NULL;
    MemoryContext          old_cxt = NULL;
    
    old_cxt = MemoryContextSwitchTo(TopMemoryContext);
    key = (GTM_SequenceKeyData*)palloc(sizeof(GTM_SequenceKeyData));
	key->gsk_keylen  = strlen(name) + 1;
    key->gsk_key     = pstrdup(name);
    key->gsk_type    = type;    
    g_CreateSeqList  = lappend(g_CreateSeqList, key);
    MemoryContextSwitchTo(old_cxt);
}

void RestoreSeqDrop(DropInfo *drop_info_array, int32 count)
{
    /* only restore when list is empty */
    if (0 == list_length(g_DropSeqList) && count > 0)
    {
        int32 i = 0;
        MemoryContext  old_cxt   = NULL;
        DropInfo      *drop_info = NULL;
        for (i = 0; i < count; i++)
        {
            old_cxt = MemoryContextSwitchTo(TopMemoryContext);
            drop_info = (DropInfo*)palloc(sizeof(DropInfo));
            snprintf(drop_info->new, GTM_NAME_LEN, "%s", drop_info_array[i].new);
            snprintf(drop_info->old, GTM_NAME_LEN, "%s", drop_info_array[i].old);
            drop_info->gsk_type = drop_info_array[i].gsk_type;
            g_DropSeqList       = lappend(g_DropSeqList, drop_info);
            MemoryContextSwitchTo(old_cxt);
        }
    }
}

void RegisterSeqDrop(char *name, int32 type)
{
    DropInfo              *drop_info = NULL;
    MemoryContext         old_cxt   = NULL;
    struct timeval       tp;
    char                 temp[GTM_NAME_LEN];
    
    gettimeofday(&tp, NULL);
    if (GTM_SEQ_FULL_NAME == type)
    {
        /* Here we can only add postfix for the temp sequence, or drop database will fail. */
	    snprintf(temp, GTM_NAME_LEN, "%s"GTM_SEQ_POSTFIX, name);
        if (RenameSequenceGTM((char *)name, temp))
        {
            elog(ERROR, "Deletion of sequences on database %s failed when backup old seq", name);
        }
    }
    else
    {
        snprintf(temp, GTM_NAME_LEN, "%s", name);
    }    
                                
    old_cxt = MemoryContextSwitchTo(TopMemoryContext);
    drop_info = (DropInfo*)palloc(sizeof(DropInfo));
    snprintf(drop_info->new, GTM_NAME_LEN, "%s", temp);
    snprintf(drop_info->old, GTM_NAME_LEN, "%s", name);
    drop_info->gsk_type = type;
    g_DropSeqList       = lappend(g_DropSeqList, drop_info);
    MemoryContextSwitchTo(old_cxt);
}

void RestoreSeqRename(RenameInfo *rename_info_array, int32 count)
{
    /* only restore when list is empty */
    if (0 == list_length(g_AlterSeqList) && count > 0)
    {
        int32 i = 0;
        for (i = 0; i < count; i++)
        {
            RegisterRenameSequence(rename_info_array[i].new, rename_info_array[i].old);
        }
    }
}

void RegisterRenameSequence(char *new, char *old)
{
    MemoryContext old_cxt;
    RenameInfo    *info = NULL;
    ListCell         *cell = NULL;
    RenameInfo    *rename_info = NULL;
    
    /* combine the alter operation of the same sequence in the same transaction .
     * EXAMPLE:
     * RENAME SEQUENCE A TO B --\
     *                           > RENAME SEQUENCE A TO C
     * RENAME SEQUENCE B TO C --/
     */
    foreach(cell, g_AlterSeqList)
    {
        rename_info = (RenameInfo *) lfirst(cell);            
        if (0 == strncmp(rename_info->new, old, GTM_NAME_LEN))
        {            
             elog(LOG, "Combine requence seq:%s ->:%s, %s->%s to old:%s latest "
                                       "new:%s", rename_info->new, rename_info->old, new, old,
                                       rename_info->old, new);
            snprintf(rename_info->new, GTM_NAME_LEN, "%s", new);
            return;
        }        
    }
    
    old_cxt = MemoryContextSwitchTo(TopMemoryContext);
    info = palloc(sizeof(RenameInfo));
    snprintf(info->new, GTM_NAME_LEN, "%s", new);
    snprintf(info->old, GTM_NAME_LEN, "%s", old);
    g_AlterSeqList = lappend(g_AlterSeqList, info);
    MemoryContextSwitchTo(old_cxt);
}

void FinishSeqOp(bool commit)
{// #lizard forgives
    int32                 ret           = 0;
    ListCell               *cell          = NULL;
    GTM_SequenceKeyData *key           = NULL;
    RenameInfo          *rename_info = NULL;
    DropInfo            *drop_info = NULL;
    
    if (!IS_PGXC_LOCAL_COORDINATOR)
    {
        return;
    }
    
    if (commit)
    {
        /* do nothing for the created list */

        /* do the drop for drop list */
        foreach(cell, g_DropSeqList)
        {
            drop_info = (DropInfo *) lfirst(cell);        
            /* No need to differ the gsk_type here. */
            ret = DropSequenceGTM(drop_info->new, drop_info->gsk_type);
            if (ret)
            {
                elog(LOG, "DropSequenceGTM failed for seq:%s type:%d commit:%d", drop_info->new, drop_info->gsk_type, commit);
            }
        }

        /* do the alter for the alter list */
    }
    else
    {
        /* rollback the created list */
        foreach(cell, g_CreateSeqList)
        {
            key = (GTM_SequenceKeyData *) lfirst(cell);            
            ret = DropSequenceGTM(key->gsk_key, key->gsk_type);
            if (ret)
            {
                elog(LOG, "DropSequenceGTM failed for seq:%s type:%d commit:%d", key->gsk_key, key->gsk_type, commit);
            }
        }
        
        /* roll back the drop list */
        foreach(cell, g_DropSeqList)
        {
            drop_info = (DropInfo *) lfirst(cell);            
            /* No need to process GTM_SEQ_DB_NAME here. */
            if (GTM_SEQ_FULL_NAME == drop_info->gsk_type)
            {
                ret = RenameSequenceGTM(drop_info->new, drop_info->old);
                if (ret)
                {
                    elog(LOG, "RenameSequenceGTM seq:%s to:%s failed commit:%d", drop_info->new, drop_info->old, commit);
                }
            }
        }
        
        /* rollback for the alter list */
        foreach(cell, g_AlterSeqList)
        {
            rename_info = (RenameInfo *) lfirst(cell);            
            ret = RenameSequenceGTM(rename_info->new, rename_info->old);
            if (ret)
            {
                elog(LOG, "RenameSequenceGTM seq:%s to:%s failed commit:%d", rename_info->new, rename_info->old, commit);
            }
        }
    }

    /* Free the memory */    
    if (g_CreateSeqList)
    {        
        foreach(cell, g_CreateSeqList)
        {
            key = (GTM_SequenceKeyData *) lfirst(cell);            
            pfree(key->gsk_key);
            pfree(key);
        }
        list_free(g_CreateSeqList);
        g_CreateSeqList = NULL;
    }

    if (g_DropSeqList)
    {
        foreach(cell, g_DropSeqList)
        {
            drop_info = (DropInfo *) lfirst(cell);
            pfree(drop_info);
        }
        list_free(g_DropSeqList);
        g_DropSeqList = NULL;
    }

    if (g_AlterSeqList)
    {
        foreach(cell, g_AlterSeqList)
        {
            rename_info = (RenameInfo *) lfirst(cell);
            pfree(rename_info);        
        }
        list_free(g_AlterSeqList);
        g_AlterSeqList = NULL;
    }
}

int32 GetGTMCreateSeq(char **create_info)
{
    int32                count   = 0;
    ListCell               *cell      = NULL;
    GTM_SequenceKeyData *key       = NULL;    
    CreateInfo             *content = NULL;

    content = (CreateInfo*)palloc0(MAXALIGN(sizeof(CreateInfo) * list_length(g_CreateSeqList)));
    foreach(cell, g_CreateSeqList)
    {
        key = (GTM_SequenceKeyData *) lfirst(cell);            
        strncpy(content[count].name, key->gsk_key, GTM_NAME_LEN);
        content[count].gsk_type = key->gsk_type;
        count++;
    }
    *create_info = (char*)content;
    return count;
}

int32 GetGTMDropSeq(char **drop_info)
{
    int32                 count     = 0;
    ListCell            *cell     = NULL;
    DropInfo             *drop     = NULL;    
    DropInfo            *content = NULL;

    content = (DropInfo*)palloc0(MAXALIGN(sizeof(DropInfo) * list_length(g_DropSeqList)));
    foreach(cell, g_DropSeqList)
    {
        drop = (DropInfo *) lfirst(cell);         
        strncpy(content[count].new, drop->new, GTM_NAME_LEN);
        strncpy(content[count].old, drop->old, GTM_NAME_LEN);
        content[count].gsk_type = drop->gsk_type;
        count++;
    }
    *drop_info = (char*)content;
    return count;
}

int32 GetGTMRenameSeq(char **rename_info)
{
    int32                 count     = 0;
    ListCell            *cell     = NULL;
    RenameInfo            *rename     = NULL;    
    RenameInfo            *content = NULL;

    content = (RenameInfo*)palloc0(MAXALIGN(sizeof(RenameInfo) * list_length(g_AlterSeqList)));
    foreach(cell, g_AlterSeqList)
    {
        rename = (RenameInfo *) lfirst(cell);        
        strncpy(content[count].new, rename->new, GTM_NAME_LEN);
        strncpy(content[count].old, rename->old, GTM_NAME_LEN);
        count++;
    }
    *rename_info = (char*)content;
    return count;
}

/* Finish the transaction with gid. */
int FinishGIDGTM(char *gid)
{
    int ret = 0;

    CheckConnection();
    ret = -1;
    if (conn)
    {
        ret = finish_gid_gtm(conn, gid);
    }

    /*
     * If something went wrong (timeout), try and reset GTM connection.
     * We will abort the transaction locally anyway, and closing GTM will force
     * it to end on GTM.
     */
    if (ret < 0)
    {
        CloseGTM();
        InitGTM();
        if (conn)
        {
            ret = finish_gid_gtm(conn, gid);
        }
    }

    return ret;
}

/* Finish the transaction with gid. */
int GetGTMStoreStatus(GTMStorageStatus *header)
{
    int ret = 0;

    CheckConnection();
    ret = -1;
    if (conn)
    {
        ret = get_gtm_store_status(conn, header);
    }

    /*
     * If something went wrong (timeout), try and reset GTM connection.
     * We will abort the transaction locally anyway, and closing GTM will force
     * it to end on GTM.
     */
    if (ret < 0)
    {
        CloseGTM();
        InitGTM();
        if (conn)
        {
            ret = get_gtm_store_status(conn, header);
        }
    }

    return ret;
}


int GetGTMStoreSequence(GTM_StoredSeqInfo **store_seq)
{
    int ret = 0;

    CheckConnection();
    ret = -1;
    if (conn)
    {
        ret = get_storage_sequence_list(conn, store_seq);
    }

    /*
     * If something went wrong (timeout), try and reset GTM connection.
     * We will abort the transaction locally anyway, and closing GTM will force
     * it to end on GTM.
     */
    if (ret < 0)
    {
        CloseGTM();
        InitGTM();
        if (conn)
        {
            ret = get_storage_sequence_list(conn, store_seq);
        }
    }

    return ret;
}

int CheckGTMStoreTransaction(GTMStorageTransactionStatus **store_txn, bool need_fix)
{
    int ret = 0;

    CheckConnection();
    ret = -1;
    if (conn)
    {
        ret = check_storage_transaction(conn, store_txn, need_fix);
    }

    /*
     * If something went wrong (timeout), try and reset GTM connection.
     * We will abort the transaction locally anyway, and closing GTM will force
     * it to end on GTM.
     */
    if (ret < 0)
    {
        CloseGTM();
        InitGTM();
        if (conn)
        {
            ret = check_storage_transaction(conn, store_txn, need_fix);
        }
    }
    return ret;
}

int CheckGTMStoreSequence(GTMStorageSequneceStatus **store_seq, bool need_fix)
{
    int ret = 0;

    CheckConnection();
    ret = -1;
    if (conn)
    {
        ret = check_storage_sequence(conn, store_seq, need_fix);
    }

    /*
     * If something went wrong (timeout), try and reset GTM connection.
     * We will abort the transaction locally anyway, and closing GTM will force
     * it to end on GTM.
     */
    if (ret < 0)
    {
        CloseGTM();
        InitGTM();
        if (conn)
        {
            ret = check_storage_sequence(conn, store_seq, need_fix);
        }
    }

    return ret;
}

int GetGTMStoreTransaction(GTM_StoredTransactionInfo **store_txn)
{
    int ret = 0;

    CheckConnection();
    ret = -1;
    if (conn)
    {
        ret = get_storage_transaction_list(conn, store_txn);
    }

    /*
     * If something went wrong (timeout), try and reset GTM connection.
     * We will abort the transaction locally anyway, and closing GTM will force
     * it to end on GTM.
     */
    if (ret < 0)
    {
        CloseGTM();
        InitGTM();
        if (conn)
        {
            ret = get_storage_transaction_list(conn, store_txn);
        }
    }
    return ret;
}

/*
 * pg_list_gtm_store - produce a view with gtm store info.
 */
Datum
pg_list_gtm_store(PG_FUNCTION_ARGS)
{
#define  LIST_GTM_STORE_COLUMNS 16
    int32           ret = 0;
    TupleDesc        tupdesc;

    GTMStorageStatus gtm_status;

    Datum        values[LIST_GTM_STORE_COLUMNS];
    bool        nulls[LIST_GTM_STORE_COLUMNS];

    ret = GetGTMStoreStatus(&gtm_status);
    if (ret)
    {
        elog(ERROR, "get gtm info from gtm failed");
    }
   
    /* build tupdesc for result tuples */
    /* this had better match function's declaration in pg_proc.h */
    tupdesc = CreateTemplateTupleDesc(LIST_GTM_STORE_COLUMNS, false);
    

    TupleDescInitEntry(tupdesc, (AttrNumber) 1, "system_identifier",
                       INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 2, "major_version",
                       INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 3, "minor_version",
                       INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 4, "gtm_status",
                       INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 5, "global_time_stamp",
                       INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 6, "global_xmin",
                       INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 7, "next_gxid",
                       INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 8, "seq_total",
                       INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 9, "seq_used",
                       INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 10, "seq_freelist",
                       INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 11, "txn_total",
                       INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 12, "txn_used",
                       INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 13, "txn_freelist",
                       INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 14, "system_lsn",
                       INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 15, "last_update_time",
                       TIMESTAMPTZOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 16, "crc_check_value",
                       INT4OID, -1, 0);        
    BlessTupleDesc(tupdesc);
    MemSet(nulls, false, sizeof(nulls));
    
    /* Fill values and NULLs */
    values[0] = DatumGetInt32(gtm_status.header.m_identifier);
    values[1] = DatumGetInt32(gtm_status.header.m_major_version);
    values[2] = DatumGetInt32(gtm_status.header.m_minor_version);
    values[3] = DatumGetInt32(gtm_status.header.m_gtm_status);
    values[4] = DatumGetInt64(gtm_status.header.m_next_gts);
    values[5] = DatumGetInt32(gtm_status.header.m_global_xmin);;
    values[6] = DatumGetInt32(gtm_status.header.m_next_gxid);
    values[7] = DatumGetInt32(gtm_status.seq_total);
    values[8] = DatumGetInt32(gtm_status.seq_used);    
    values[9] = DatumGetInt32(gtm_status.header.m_seq_freelist);
    values[10] = DatumGetInt32(gtm_status.txn_total);
    values[11] = DatumGetInt32(gtm_status.txn_used);    
    values[12] = DatumGetInt32(gtm_status.header.m_txn_freelist);
    values[13] = DatumGetInt32(gtm_status.header.m_lsn);
    values[14] = TimestampGetDatum(gtm_status.header.m_last_update_time);
    values[15] = DatumGetInt32(gtm_status.header.m_crc);
    
    /* Returns the record as Datum */
    PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}


Datum
pg_list_storage_sequence(PG_FUNCTION_ARGS)
{
#define NUM_SEQ_STATUS_COLUMNS    15
    
    FuncCallContext   *funcctx;
    PG_Storage_status *mystatus;
    GTM_StoredSeqInfo *seqs;

    if (SRF_IS_FIRSTCALL())
    {
        TupleDesc    tupdesc;
        MemoryContext oldcontext;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();
        
        /*
         * switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        
        /* build tupdesc for result tuples */
        /* this had better match function's declaration in pg_proc.h */
        tupdesc = CreateTemplateTupleDesc(NUM_SEQ_STATUS_COLUMNS, false);
        TupleDescInitEntry(tupdesc, (AttrNumber) 1, "gsk_key",
                           TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 2, "gsk_type",
                           INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 3, "gs_value",
                           INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 4, "gs_init_value",
                           INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 5, "gs_increment_by",
                           INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 6, "gs_min_value",
                           INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 7, "gs_max_value",
                           INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 8, "gs_cycle",
                           BOOLOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 9, "gs_called",
                           BOOLOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 10, "gs_reserved",
                           BOOLOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 11, "gs_status",
                           INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 12, "gti_store_handle",
                           INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 13, "last_update_time",
                           TIMESTAMPTZOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 14, "gs_next",
                           INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 15, "gs_crc",
                           INT4OID, -1, 0);
        
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        mystatus = (PG_Storage_status *) palloc0(sizeof(PG_Storage_status));
        funcctx->user_fctx = (void *) mystatus;

        MemoryContextSwitchTo(oldcontext);

        mystatus->totalIdx = GetGTMStoreSequence((GTM_StoredSeqInfo **)&mystatus->data);
        if (mystatus->totalIdx < 0)
        {
            elog(ERROR, "get sequence info from gtm failed");            
        }        
    }

    funcctx  = SRF_PERCALL_SETUP();
    mystatus = (PG_Storage_status *) funcctx->user_fctx;    
    seqs     = (GTM_StoredSeqInfo*)mystatus->data;
    while (mystatus->currIdx < mystatus->totalIdx)
    {
        Datum        values[NUM_SEQ_STATUS_COLUMNS];
        bool        nulls[NUM_SEQ_STATUS_COLUMNS];
        
        HeapTuple    tuple;
        Datum        result;
        GTM_StoredSeqInfo *instance;

        instance = &(seqs[mystatus->currIdx]);        

        /*
         * Form tuple with appropriate data.
         */
        MemSet(values, 0, sizeof(values));
        MemSet(nulls, false, sizeof(nulls));

        values[0] = CStringGetTextDatum(instance->gs_key.gsk_key);
        values[1] = Int32GetDatum(instance->gs_key.gsk_type);
        values[2] = Int64GetDatum(instance->gs_value);
        values[3] = Int64GetDatum(instance->gs_init_value);
        values[4] = Int64GetDatum(instance->gs_increment_by);
        values[5] = Int64GetDatum(instance->gs_min_value);
        values[6] = Int64GetDatum(instance->gs_max_value);
        values[7] = BoolGetDatum(instance->gs_cycle);
        values[8] = BoolGetDatum(instance->gs_called);
        values[9] = BoolGetDatum(instance->gs_reserved);
        values[10] = Int32GetDatum(instance->gs_status);
        values[11] = Int32GetDatum(instance->gti_store_handle);
        values[12] = TimestampTzGetDatum(instance->m_last_update_time);
        values[13] = Int32GetDatum(instance->gs_next);
        values[14] = UInt32GetDatum(instance->gs_crc);                
            

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        
        mystatus->currIdx++;
        SRF_RETURN_NEXT(funcctx, result);
    }    
    
    SRF_RETURN_DONE(funcctx);
}


Datum
pg_list_storage_transaction(PG_FUNCTION_ARGS)
{
#define NUM_TXN_STATUS_COLUMNS    7
    FuncCallContext   *funcctx;
    PG_Storage_status *mystatus;
    GTM_StoredTransactionInfo *txns;

    if (SRF_IS_FIRSTCALL())
    {
        TupleDesc    tupdesc;
        MemoryContext oldcontext;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();
        
        /*
         * switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        
        /* build tupdesc for result tuples */
        /* this had better match function's declaration in pg_proc.h */
        tupdesc = CreateTemplateTupleDesc(NUM_TXN_STATUS_COLUMNS, false);
        TupleDescInitEntry(tupdesc, (AttrNumber) 1, "gti_gid",
                           TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 2, "node_list",
                           TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 3, "gti_state",
                           INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 4, "gti_store_handle",
                           INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 5, "last_update_time",
                           TIMESTAMPTZOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 6, "gs_next",
                           INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 7, "gs_crc",
                           INT4OID, -1, 0);
        
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        mystatus = (PG_Storage_status *) palloc0(sizeof(PG_Storage_status));
        funcctx->user_fctx = (void *) mystatus;

        MemoryContextSwitchTo(oldcontext);

        mystatus->totalIdx = GetGTMStoreTransaction((GTM_StoredTransactionInfo **)&mystatus->data);
        if (mystatus->totalIdx < 0)
        {
            elog(ERROR, "get transaction info from gtm failed");            
        }        
    }

    funcctx  = SRF_PERCALL_SETUP();
    mystatus = (PG_Storage_status*) funcctx->user_fctx;    
    txns     = (GTM_StoredTransactionInfo*)mystatus->data;
    while (mystatus->currIdx < mystatus->totalIdx)
    {
        Datum        values[NUM_TXN_STATUS_COLUMNS];
        bool        nulls[NUM_TXN_STATUS_COLUMNS];
        
        HeapTuple    tuple;
        Datum        result;
        GTM_StoredTransactionInfo *instance;

        instance = &(txns[mystatus->currIdx]);        

        /*
         * Form tuple with appropriate data.
         */
        MemSet(values, 0, sizeof(values));
        MemSet(nulls, false, sizeof(nulls));

        values[0] = CStringGetTextDatum(instance->gti_gid);
        values[1] = CStringGetTextDatum(instance->nodestring);
        values[2] = Int32GetDatum(instance->gti_state);
        values[3] = Int32GetDatum(instance->gti_store_handle);
        values[4] = TimestampTzGetDatum(instance->m_last_update_time);
        values[5] = Int32GetDatum(instance->gs_next);
        values[6] = Int32GetDatum(instance->gti_crc);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        
        mystatus->currIdx++;
        SRF_RETURN_NEXT(funcctx, result);
    }    

    SRF_RETURN_DONE(funcctx);
}

Datum
pg_check_storage_sequence(PG_FUNCTION_ARGS)
{
#define NUM_CHECK_SEQ_STATUS_COLUMNS    17    
    FuncCallContext   *funcctx;
    PG_Storage_status *mystatus;
    GTMStorageSequneceStatus *seqs;

    if (SRF_IS_FIRSTCALL())
    {
        int32          need_fix = false;
        TupleDesc      tupdesc;
        MemoryContext oldcontext;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        need_fix = PG_GETARG_BOOL(0);

        /*
         * switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        
        /* build tupdesc for result tuples */
        /* this had better match function's declaration in pg_proc.h */
        tupdesc = CreateTemplateTupleDesc(NUM_CHECK_SEQ_STATUS_COLUMNS, false);
        TupleDescInitEntry(tupdesc, (AttrNumber) 1, "gsk_key",
                           TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 2, "gsk_type",
                           INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 3, "gs_value",
                           INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 4, "gs_init_value",
                           INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 5, "gs_increment_by",
                           INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 6, "gs_min_value",
                           INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 7, "gs_max_value",
                           INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 8, "gs_cycle",
                           BOOLOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 9, "gs_called",
                           BOOLOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 10, "gs_reserved",
                           BOOLOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 11, "gs_status",
                           INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 12, "gti_store_handle",
                           INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 13, "last_update_time",
                           TIMESTAMPTZOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 14, "gs_next",
                           INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 15, "gs_crc",
                           INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 16, "error_msg",
                           INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 17, "check_status",
                           INT4OID, -1, 0);
        
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        mystatus = (PG_Storage_status *) palloc0(sizeof(PG_Storage_status));
        funcctx->user_fctx = (void *) mystatus;

        MemoryContextSwitchTo(oldcontext);

        mystatus->totalIdx = CheckGTMStoreSequence((GTMStorageSequneceStatus **)&mystatus->data, need_fix);
        if (mystatus->totalIdx < 0)
        {
            elog(ERROR, "get sequence info from gtm failed");            
        }        
    }

    funcctx  = SRF_PERCALL_SETUP();
    mystatus = (PG_Storage_status *) funcctx->user_fctx;    
    seqs     = (GTMStorageSequneceStatus*)mystatus->data;
    while (mystatus->currIdx < mystatus->totalIdx)
    {
        Datum        values[NUM_CHECK_SEQ_STATUS_COLUMNS];
        bool        nulls[NUM_CHECK_SEQ_STATUS_COLUMNS];
        
        HeapTuple    tuple;
        Datum        result;
        GTMStorageSequneceStatus *instance;

        instance = &(seqs[mystatus->currIdx]);        

        /*
         * Form tuple with appropriate data.
         */
        MemSet(values, 0, sizeof(values));
        MemSet(nulls, false, sizeof(nulls));

        values[0] = CStringGetTextDatum(instance->sequence.gs_key.gsk_key);
        values[1] = Int32GetDatum(instance->sequence.gs_key.gsk_type);
        values[2] = Int64GetDatum(instance->sequence.gs_value);
        values[3] = Int64GetDatum(instance->sequence.gs_init_value);
        values[4] = Int64GetDatum(instance->sequence.gs_increment_by);
        values[5] = Int64GetDatum(instance->sequence.gs_min_value);
        values[6] = Int64GetDatum(instance->sequence.gs_max_value);
        values[7] = BoolGetDatum(instance->sequence.gs_cycle);
        values[8] = BoolGetDatum(instance->sequence.gs_called);
        values[9] = BoolGetDatum(instance->sequence.gs_reserved);
        values[10] = Int32GetDatum(instance->sequence.gs_status);
        values[11] = Int32GetDatum(instance->sequence.gti_store_handle);
        values[12] = TimestampTzGetDatum(instance->sequence.m_last_update_time);
        values[13] = Int32GetDatum(instance->sequence.gs_next);
        values[14] = UInt32GetDatum(instance->sequence.gs_crc);                        
        values[15] = Int32GetDatum(instance->error);
        values[16] = Int32GetDatum(instance->status);    

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        
        mystatus->currIdx++;
        SRF_RETURN_NEXT(funcctx, result);
    }    

    SRF_RETURN_DONE(funcctx);
}

Datum
pg_check_storage_transaction(PG_FUNCTION_ARGS)
{
#define NUM_CHECK_TXN_STATUS_COLUMNS    11
    FuncCallContext   *funcctx;
    PG_Storage_status *mystatus;
    GTMStorageTransactionStatus *txns;

    if (SRF_IS_FIRSTCALL())
    {
        TupleDesc      tupdesc;
        MemoryContext oldcontext;
        int32          need_fix = false;

        need_fix = PG_GETARG_BOOL(0);
        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();
        
        /*
         * switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        
        /* build tupdesc for result tuples */
        /* this had better match function's declaration in pg_proc.h */
        tupdesc = CreateTemplateTupleDesc(NUM_CHECK_TXN_STATUS_COLUMNS, false);
        TupleDescInitEntry(tupdesc, (AttrNumber) 1, "gti_gid",
                           TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 2, "node_list",
                           TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 3, "gti_state",
                           INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 4, "gti_store_handle",
                           INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 5, "last_update_time",
                           TIMESTAMPTZOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 6, "gs_next",
                           INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 7, "gs_crc",
                           INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 8, "error_msg",
                           INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 9, "check_status",
                           INT4OID, -1, 0);
        
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        mystatus = (PG_Storage_status *) palloc0(sizeof(PG_Storage_status));
        funcctx->user_fctx = (void *) mystatus;

        MemoryContextSwitchTo(oldcontext);

        mystatus->totalIdx = CheckGTMStoreTransaction((GTMStorageTransactionStatus **)&mystatus->data, need_fix);
        if (mystatus->totalIdx < 0)
        {
            elog(ERROR, "get transaction info from gtm failed");            
        }        
    }

    funcctx  = SRF_PERCALL_SETUP();
    mystatus = (PG_Storage_status*) funcctx->user_fctx;    
    txns     = (GTMStorageTransactionStatus*)mystatus->data;
    while (mystatus->currIdx < mystatus->totalIdx)
    {
        Datum        values[NUM_CHECK_TXN_STATUS_COLUMNS];
        bool        nulls[NUM_CHECK_TXN_STATUS_COLUMNS];
        
        HeapTuple    tuple;
        Datum        result;
        GTMStorageTransactionStatus *instance;

        instance = &(txns[mystatus->currIdx]);        

        /*
         * Form tuple with appropriate data.
         */
        MemSet(values, 0, sizeof(values));
        MemSet(nulls, false, sizeof(nulls));

        values[0] = CStringGetTextDatum(instance->txn.gti_gid);
        values[1] = CStringGetTextDatum(instance->txn.nodestring);
        values[2] = Int32GetDatum(instance->txn.gti_state);
        values[3] = Int32GetDatum(instance->txn.gti_store_handle);
        values[4] = TimestampTzGetDatum(instance->txn.m_last_update_time);
        values[5] = Int32GetDatum(instance->txn.gs_next);
        values[6] = Int32GetDatum(instance->txn.gti_crc);
        values[7] = Int32GetDatum(instance->error);
        values[8] = Int32GetDatum(instance->status);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        
        mystatus->currIdx++;
        SRF_RETURN_NEXT(funcctx, result);
    }    

    SRF_RETURN_DONE(funcctx);
}

#endif

bool
IsGTMConnected()
{
    return conn != NULL;
}

#ifdef __TBASE__
/*
 * Set gtm info with GtmHost and GtmPort.
 *
 * There are three cases:
 * 1.New gtm info from create/alter gtm node command
 * 2.Gtm info from pgxc_node
 * 3.Gtm info from recovery gtm host
 */
static void 
GetMasterGtmInfo(void)
{
	/* Check gtm host and port info */
	Relation	rel;
	HeapScanDesc scan;
	HeapTuple	gtmtup;
	Form_pgxc_node	nodeForm;
	bool		found = false;

	/* reset gtm info */
	ResetGtmInfo();

	/* If NewGtmHost and NewGtmPort, just use it. */
	if (NewGtmHost && NewGtmPort != 0)
	{
		elog(LOG,
			"GetMasterGtmInfo: set master gtm info with NewGtmHost:%s NewGtmPort:%d",
			NewGtmHost, NewGtmPort);

		GtmHost = strdup(NewGtmHost);
		GtmPort = NewGtmPort;

		free(NewGtmHost);
		NewGtmHost = NULL;
		NewGtmPort = 0;

		return;
	}

	/* we have no recovery gtm host info, just read from heap. */
	if (!g_recovery_gtm_host->need_read)
	{
		/*
		 * We must be sure there is no error report, because we may be
		 * in AbortTransaction now.
		 * 1.If we are not in a transaction, we should not open relation.
		 * 2.If we do not get lock, it is ok to try it next time.
		 */
		if (IsTransactionState() &&
			ConditionalLockRelationOid(PgxcNodeRelationId, AccessShareLock))
		{
			rel = relation_open(PgxcNodeRelationId, NoLock);
			scan = heap_beginscan_catalog(rel, 0, NULL);
			/* Only one record will match */
			while (HeapTupleIsValid(gtmtup = heap_getnext(scan, ForwardScanDirection)))
			{
				nodeForm = (Form_pgxc_node) GETSTRUCT(gtmtup);
				if (PGXC_NODE_GTM == nodeForm->node_type && nodeForm->nodeis_primary)
				{
					GtmHost = strdup(NameStr(nodeForm->node_host));
					GtmPort = nodeForm->node_port;
					found = true;
					break;
				}
			}
			heap_endscan(scan);
			relation_close(rel, AccessShareLock);
		}
	}
	else
	{
		/* get the gtm host info  */
		GtmHost = strdup(NameStr(g_recovery_gtm_host->hostdata));
		GtmPort = g_recovery_gtm_host->port;	
		found = true;
	}	

	if (!found)
	{
		elog(LOG,
			"GetMasterGtmInfo: can not get master gtm info from pgxc_node");
	}
}
#endif

static void
CheckConnection(void)
{
	/* Be sure that a backend does not use a postmaster connection */
	if (IsUnderPostmaster && GTMPQispostmaster(conn) == 1)
	{
		CloseGTM();
		InitGTM();
		return;
	}

	if (GTMPQstatus(conn) != CONNECTION_OK)
	{
		CloseGTM();
		InitGTM();
	}
}

static void
ResetGTMConnection(void)
{
	Relation rel;
	HeapScanDesc scan;
	HeapTuple gtmtup;
	Form_pgxc_node nodeForm;
	bool found = false;

	CloseGTM();
	ResetGtmInfo();

	/*
	 * We must be sure there is no error report, because we may be
	 * in AbortTransaction now.
	 * 1.If we are not in a inprogress or commit transaction, we should not open relation.
	 * 2.If we do not get lock, it is ok to try it next time.
	 */
	if ( (IsTransactionState() || IsTransactionCommit()) &&
		ConditionalLockRelationOid(PgxcNodeRelationId, AccessShareLock))
	{
		rel = relation_open(PgxcNodeRelationId, NoLock);
		scan = heap_beginscan_catalog(rel, 0, NULL);
		/* Only one record will match */
		while (HeapTupleIsValid(gtmtup = heap_getnext(scan, ForwardScanDirection)))
		{
			nodeForm = (Form_pgxc_node) GETSTRUCT(gtmtup);
			if (PGXC_NODE_GTM == nodeForm->node_type && nodeForm->nodeis_primary)
			{
				GtmHost = strdup(NameStr(nodeForm->node_host));
				GtmPort = nodeForm->node_port;
				found = true;
				break;
			}
		}
		heap_endscan(scan);
		relation_close(rel, AccessShareLock);

		if (!found)
		{
			elog(LOG, "can not get master gtm info from pgxc_node");
		}
	}

	InitGTM();
}

#ifdef HAVE_UNIX_SOCKETS
/*
 * gtm_unix_socket_file_exists()
 *
 * Checks whether the gtm unix domain socket file exists.
 */
static bool
gtm_unix_socket_file_exists(void)
{
    char		path[MAXGTMPATH];
    char		lockfile[MAXPGPATH];
    int			fd;

    UNIXSOCK_PATH(path, GtmPort, gtm_unix_socket_directory);
    snprintf(lockfile, sizeof(lockfile), "%s.lock", path);

    if ((fd = open(lockfile, O_RDONLY, 0)) < 0)
    {
        /* ENOTDIR means we will throw a more useful error later */
        if (errno != ENOENT && errno != ENOTDIR)
            elog(LOG, "could not open file \"%s\" for reading: %s\n",
                     lockfile, strerror(errno));

        return false;
    }

    close(fd);
    return true;
}
#endif

void
InitGTM(void)
{// #lizard forgives
#define  CONNECT_STR_LEN   256 /* 256 bytes should be enough */
    char conn_str[CONNECT_STR_LEN];
#ifdef __TBASE__
	int  try_cnt = 0;
	const int max_try_cnt = 1;
    bool  same_host = false;

	/*
	 * Only re-set gtm info in two cases:
	 * 1.No gtm info
	 * 2.New gtm info by create/alter gtm node command
	 */
	if ((GtmHost == NULL && GtmPort == 0) ||
		(NewGtmHost != NULL && NewGtmPort != 0))
	{
		GetMasterGtmInfo();
	}
	if (GtmHost == NULL && GtmPort == 0)
	{
		ereport(LOG,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("GtmHost and GtmPort are not set")));
		return;
	}

#ifdef HAVE_UNIX_SOCKETS
    if (GtmHost && (strcmp(PGXCNodeHost, GtmHost) == 0) && gtm_unix_socket_file_exists())
    {
        same_host = true;
    }
#endif
#endif

try_connect_gtm:
	/* If this thread is postmaster itself, it contacts gtm identifying itself */
	if (!IsUnderPostmaster)
	{
		GTM_PGXCNodeType remote_type = GTM_NODE_DEFAULT;

		if (IS_PGXC_COORDINATOR)
			remote_type = GTM_NODE_COORDINATOR;
		else if (IS_PGXC_DATANODE)
			remote_type = GTM_NODE_DATANODE;

#ifdef __TBASE__
        if (same_host)
        {
            /* Use 60s as connection timeout */
            snprintf(conn_str, CONNECT_STR_LEN, "host=%s port=%d node_name=%s remote_type=%d postmaster=1 connect_timeout=%d",
                     gtm_unix_socket_directory, GtmPort, PGXCNodeName, remote_type,
                     tcp_keepalives_idle > 0 ?
                     tcp_keepalives_idle : GtmConnectTimeout);
        }
        else
#endif
        {
		/* Use 60s as connection timeout */
		snprintf(conn_str, CONNECT_STR_LEN, "host=%s port=%d node_name=%s remote_type=%d postmaster=1 connect_timeout=%d",
								GtmHost, GtmPort, PGXCNodeName, remote_type,
								tcp_keepalives_idle > 0 ?
								tcp_keepalives_idle : GtmConnectTimeout);
        }

		/* Log activity of GTM connections */
		if(GTMDebugPrint)
			elog(LOG, "Postmaster: connection established to GTM with string %s", conn_str);
	}
	else
	{
#ifdef __TBASE__
        if (same_host)
        {
            /* Use 60s as connection timeout */
            snprintf(conn_str, CONNECT_STR_LEN, "host=%s port=%d node_name=%s connect_timeout=%d",
                     gtm_unix_socket_directory, GtmPort, PGXCNodeName,
                     tcp_keepalives_idle > 0 ?
                     tcp_keepalives_idle : GtmConnectTimeout);
        }
        else
#endif
        {
		/* Use 60s as connection timeout */
		snprintf(conn_str, CONNECT_STR_LEN, "host=%s port=%d node_name=%s connect_timeout=%d",
				GtmHost, GtmPort, PGXCNodeName,
				tcp_keepalives_idle > 0 ?
				tcp_keepalives_idle : GtmConnectTimeout);
        }

		/* Log activity of GTM connections */
		if (IsAutoVacuumWorkerProcess() && GTMDebugPrint)
			elog(LOG, "Autovacuum worker: connection established to GTM with string %s", conn_str);
		else if (IsAutoVacuumLauncherProcess() && GTMDebugPrint)
			elog(LOG, "Autovacuum launcher: connection established to GTM with string %s", conn_str);
		else if (IsClusterMonitorProcess() && GTMDebugPrint)
			elog(LOG, "Cluster monitor: connection established to GTM with string %s", conn_str);
		else if (IsClean2pcWorker() && GTMDebugPrint)
			elog(LOG, "Clean 2pc worker: connection established to GTM with string %s", conn_str);
		else if (IsClean2pcLauncher() && GTMDebugPrint)
			elog(LOG, "Clean 2pc launcher: connection established to GTM with string %s", conn_str);
		else if(GTMDebugPrint)
			elog(LOG, "Postmaster child: connection established to GTM with string %s", conn_str);
	}

	conn = PQconnectGTM(conn_str);
	if (GTMPQstatus(conn) != CONNECTION_OK)
	{
		int save_errno = errno;
		
#ifdef __TBASE__	
		if (try_cnt < max_try_cnt)
		{
			/* If connect gtm failed, get gtm info from syscache, and try again */
			GetMasterGtmInfo();
			if (GtmHost != NULL && GtmPort)
			{
				elog(DEBUG1, "[InitGTM] Get GtmHost:%s  GtmPort:%d try_cnt:%d max_try_cnt:%d", 
							 GtmHost, GtmPort, try_cnt, max_try_cnt);
			}
			CloseGTM();
			try_cnt++;

			/* if connect with unix domain socket failed */
			if (same_host)
            {
                same_host = false;
            }
			goto try_connect_gtm;
		}
		else
#endif		
		{
			ResetGtmInfo();

			/* Use LOG instead of ERROR to avoid error stack overflow. */
			if(conn)
			{
				ereport(LOG,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("can not connect to GTM: %s %m", GTMPQerrorMessage(conn))));
			}
			else
			{
				ereport(LOG,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("connection is null: %m")));
			}

			errno = save_errno;

			CloseGTM();
		}
		
	}
	else
	{
		if (!GTMSetSockKeepAlive(conn, tcp_keepalives_idle,
							tcp_keepalives_interval, tcp_keepalives_count))
		{
			ereport(LOG,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("GTMSetSockKeepAlive failed: %m")));
		}
		if (IS_PGXC_COORDINATOR)
		{
			register_session(conn, PGXCNodeName, MyProcPid, MyBackendId);
		}
	}
}

void
CloseGTM(void)
{
    if (conn)
    {
        GTMPQfinish(conn);
        conn = NULL;
    }

    /* Log activity of GTM connections */
    if (!IsUnderPostmaster)
        elog(DEBUG1, "Postmaster: connection to GTM closed");
    else if (IsAutoVacuumWorkerProcess())
        elog(DEBUG1, "Autovacuum worker: connection to GTM closed");
    else if (IsAutoVacuumLauncherProcess())
        elog(DEBUG1, "Autovacuum launcher: connection to GTM closed");
    else if (IsClusterMonitorProcess())
        elog(DEBUG1, "Cluster monitor: connection to GTM closed");
	else if (IsClean2pcWorker())
		elog(DEBUG1, "Clean 2pc worker: connection to GTM closed");
	else if (IsClean2pcLauncher())
		elog(DEBUG1, "Clean 2pc launcher: connection to GTM closed");
    else
        elog(DEBUG1, "Postmaster child: connection to GTM closed");
}

#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
GTM_Timestamp 
GetGlobalTimestampGTM(void)
{
	struct rusage start_r;
	struct timeval start_t;
	int  retry_cnt = 0;
	Get_GTS_Result gts_result = {InvalidGlobalTimestamp,false};
	GTM_Timestamp  latest_gts = InvalidGlobalTimestamp;

	if (!g_set_global_snapshot)
	{
		return LocalCommitTimestamp;
	}

    if (log_gtm_stats)
        ResetUsageCommon(&start_r, &start_t);

    CheckConnection();
    // TODO Isolation level
    if (conn)
    {
        gts_result =  get_global_timestamp(conn);
    }
    else if(GTMDebugPrint)
    {
        elog(LOG, "get global timestamp conn is null");
    }

    /* If something went wrong (timeout), try and reset GTM connection
     * and retry. This is safe at the beginning of a transaction.
     */
	while (!GlobalTimestampIsValid(gts_result.gts) &&
		retry_cnt < reconnect_gtm_retry_times)
    {
        if(GTMDebugPrint)
        {
            elog(LOG, "get global timestamp reconnect");
        }

		ResetGTMConnection();
		retry_cnt++;

		elog(DEBUG5, "reset gtm connection %d times", retry_cnt);

        if (conn)
        {
            gts_result = get_global_timestamp(conn);
			if (GlobalTimestampIsValid(gts_result.gts))
			{
				elog(DEBUG5, "retry get global timestamp gts " INT64_FORMAT,
					gts_result.gts);
				break;
			}
        }
        else if(GTMDebugPrint)
        {
			elog(LOG, "get global timestamp conn is null after retry %d times",
				retry_cnt);
		}

		if (retry_cnt < reconnect_gtm_retry_times)
		{
			pg_usleep(reconnect_gtm_retry_interval * 1000);
        }
    }
    elog(DEBUG7, "get global timestamp gts " INT64_FORMAT, gts_result.gts);

	if (!GlobalTimestampIsValid(gts_result.gts))
	{
		elog(WARNING, "retry %d times, get a invalid global timestamp, "
			"ResetGTMConnection", retry_cnt);
		ResetGTMConnection();
	}

    if (log_gtm_stats)
        ShowUsageCommon("BeginTranGTM", &start_r, &start_t);

	latest_gts = GetLatestCommitTS();
	if (gts_result.gts != InvalidGlobalTimestamp && latest_gts > (gts_result.gts + GTM_CHECK_DELTA))
	{
		elog(ERROR, "global gts:%lu is earlier than local gts:%lu, please check GTM status!", gts_result.gts + GTM_CHECK_DELTA, latest_gts);
	}

	/* if we are standby, use timestamp subtracting given interval */
	if (IsStandbyPostgres() && query_delay)
	{
		GTM_Timestamp  interval = query_delay * USECS_PER_SEC;

		gts_result.gts = gts_result.gts - interval;

		if (gts_result.gts < FirstGlobalTimestamp)
		{
			gts_result.gts = FirstGlobalTimestamp;
		}
	}

	GTM_ReadOnly = gts_result.gtm_readonly;
	
	return gts_result.gts;
}
#endif

GlobalTransactionId
BeginTranGTM(GTM_Timestamp *timestamp, const char *globalSession)
{
    GlobalTransactionId  xid = InvalidGlobalTransactionId;
    struct rusage start_r;
    struct timeval start_t;

    if (log_gtm_stats)
        ResetUsageCommon(&start_r, &start_t);

    CheckConnection();
    // TODO Isolation level
    if (conn)
        xid =  begin_transaction(conn, GTM_ISOLATION_RC, globalSession, timestamp);

    /* If something went wrong (timeout), try and reset GTM connection
     * and retry. This is safe at the beginning of a transaction.
     */
    if (!TransactionIdIsValid(xid))
    {
        CloseGTM();
        InitGTM();
        if (conn)
            xid = begin_transaction(conn, GTM_ISOLATION_RC, globalSession, timestamp);
    }
    if (xid)
        IsXidFromGTM = true;
    currentGxid = xid;

    elog(DEBUG2, "BeginTranGTM - session:%s, xid: %d", globalSession, xid);

    if (log_gtm_stats)
        ShowUsageCommon("BeginTranGTM", &start_r, &start_t);
    return xid;
}

GlobalTransactionId
BeginTranAutovacuumGTM(void)
{
    GlobalTransactionId  xid = InvalidGlobalTransactionId;

    CheckConnection();
    // TODO Isolation level
    if (conn)
        xid =  begin_transaction_autovacuum(conn, GTM_ISOLATION_RC);

    /*
     * If something went wrong (timeout), try and reset GTM connection and retry.
     * This is safe at the beginning of a transaction.
     */
    if (!TransactionIdIsValid(xid))
    {
        CloseGTM();
        InitGTM();
        if (conn)
            xid =  begin_transaction_autovacuum(conn, GTM_ISOLATION_RC);
    }
    currentGxid = xid;

    elog(DEBUG3, "BeginTranGTM - %d", xid);
    return xid;
}

int
CommitTranGTM(GlobalTransactionId gxid, int waited_xid_count,
        GlobalTransactionId *waited_xids)
{// #lizard forgives
    int ret;
    struct rusage start_r;
    struct timeval start_t;

    if (!GlobalTransactionIdIsValid(gxid))
        return 0;

    if (log_gtm_stats)
        ResetUsageCommon(&start_r, &start_t);

    elog(DEBUG3, "CommitTranGTM: %d", gxid);

    CheckConnection();
    ret = -1;
    if (conn)
    ret = commit_transaction(conn, gxid, waited_xid_count, waited_xids);

    /*
     * If something went wrong (timeout), try and reset GTM connection.
     * We will close the transaction locally anyway, and closing GTM will force
     * it to be closed on GTM.
     */
    if (ret < 0)
    {
        CloseGTM();
        InitGTM();
        if (conn)
            ret = commit_transaction(conn, gxid, waited_xid_count, waited_xids);
    }

    /* Close connection in case commit is done by autovacuum worker or launcher */
    if (IsAutoVacuumWorkerProcess() || IsAutoVacuumLauncherProcess())
        CloseGTM();

    currentGxid = InvalidGlobalTransactionId;

    if (log_gtm_stats)
        ShowUsageCommon("CommitTranGTM", &start_r, &start_t);
    return ret;
}

/*
 * For a prepared transaction, commit the gxid used for PREPARE TRANSACTION
 * and for COMMIT PREPARED.
 */
int
CommitPreparedTranGTM(GlobalTransactionId gxid,
        GlobalTransactionId prepared_gxid, int waited_xid_count,
        GlobalTransactionId *waited_xids)
{// #lizard forgives
    int ret = 0;
    struct rusage start_r;
    struct timeval start_t;

    if (!GlobalTransactionIdIsValid(gxid) || !GlobalTransactionIdIsValid(prepared_gxid))
        return ret;

    if (log_gtm_stats)
        ResetUsageCommon(&start_r, &start_t);

    elog(DEBUG3, "CommitPreparedTranGTM: %d:%d", gxid, prepared_gxid);

    CheckConnection();
    ret = -1;
    if (conn)
        ret = commit_prepared_transaction(conn, gxid, prepared_gxid,
            waited_xid_count, waited_xids);

    /*
     * If something went wrong (timeout), try and reset GTM connection.
     * We will close the transaction locally anyway, and closing GTM will force
     * it to be closed on GTM.
     */

    if (ret < 0)
    {
        CloseGTM();
        InitGTM();
        if (conn)
            ret = commit_prepared_transaction(conn, gxid, prepared_gxid,
                    waited_xid_count, waited_xids);
    }
    currentGxid = InvalidGlobalTransactionId;

    if (log_gtm_stats)
        ShowUsageCommon("CommitPreparedTranGTM", &start_r, &start_t);

    return ret;
}

int
RollbackTranGTM(GlobalTransactionId gxid)
{
    int ret = -1;

    if (!GlobalTransactionIdIsValid(gxid))
        return 0;
    CheckConnection();

    if (conn)
        ret = abort_transaction(conn, gxid);

    /*
     * If something went wrong (timeout), try and reset GTM connection.
     * We will abort the transaction locally anyway, and closing GTM will force
     * it to end on GTM.
     */
    if (ret < 0)
    {
        CloseGTM();
        InitGTM();
        if (conn)
            ret = abort_transaction(conn, gxid);
    }

    currentGxid = InvalidGlobalTransactionId;
    return ret;
}



/*
 * For CN, these timestamps are global ones issued by GTM
 * and are local timestamps on DNs.
 */
int
LogCommitTranGTM(GlobalTransactionId gxid,
                     const char *gid,
                     const char *nodestring,
                     int node_count,
                     bool isGlobal,
                     bool isCommit,
                     GlobalTimestamp prepare_timestamp,
                     GlobalTimestamp commit_timestamp) 
{
    int ret = 0;

    if (!GlobalTransactionIdIsValid(gxid))
        return 0;
    CheckConnection();

    ret = -1;
    if (conn)
        ret = log_commit_transaction(conn, 
                                     gxid, 
                                     gid, 
                                     nodestring, 
                                     node_count, 
                                     isGlobal, 
                                     isCommit, 
                                     prepare_timestamp, 
                                     commit_timestamp);

    /*
     * If something went wrong (timeout), try and reset GTM connection.
     * We will abort the transaction locally anyway, and closing GTM will force
     * it to end on GTM.
     */
    if (ret < 0)
    {
        CloseGTM();
        InitGTM();
        if (conn)
            ret = log_commit_transaction(conn, 
                                     gxid, 
                                     gid, 
                                     nodestring, 
                                     node_count, 
                                     isGlobal, 
                                     isCommit, 
                                     prepare_timestamp, 
                                     commit_timestamp);
    }

    
    return ret;
}

int
LogScanGTM( GlobalTransactionId gxid, 
                              const char *node_string, 
                              GlobalTimestamp     start_ts,
                              GlobalTimestamp     local_start_ts,
                              GlobalTimestamp     local_complete_ts,
                              int    scan_type,
                              const char *rel_name,
                             int64  scan_number) 
{
    int ret = 0;

    if (!GlobalTransactionIdIsValid(gxid))
        return 0;
    CheckConnection();

    ret = -1;
    if (conn)
        ret = log_scan_transaction(conn, 
                                     gxid, 
                                     node_string,
                                     start_ts,
                                     local_start_ts,
                                     local_complete_ts,
                                     scan_type,
                                     rel_name,
                                     scan_number);

    /*
     * If something went wrong (timeout), try and reset GTM connection.
     * We will abort the transaction locally anyway, and closing GTM will force
     * it to end on GTM.
     */
    if (ret < 0)
    {
        CloseGTM();
        InitGTM();
        if (conn)
            ret = log_scan_transaction(conn, 
                                     gxid, 
                                     node_string,
                                     start_ts,
                                     local_start_ts,
                                     local_complete_ts,
                                     scan_type,
                                     rel_name,
                                     scan_number);
    }

    
    return ret;
}

int
StartPreparedTranGTM(GlobalTransactionId gxid,
                     char *gid,
                     char *nodestring)
{
    int ret = 0;

    if (!GlobalTransactionIdIsValid(gxid))
    {
        return 0;
    }
    CheckConnection();

    ret = -1;
    if (conn)
    {
        ret = start_prepared_transaction(conn, gxid, gid, nodestring);
    }
    /* Here, we should not reconnect, for sometime gtm raise an error, reconnect may skip the error. */
#ifndef __TBASE__
    
    /*
     * If something went wrong (timeout), try and reset GTM connection.
     * We will abort the transaction locally anyway, and closing GTM will force
     * it to end on GTM.
     */
    if (ret < 0)
    {
        CloseGTM();
        InitGTM();
        if (conn)
            ret = start_prepared_transaction(conn, gxid, gid, nodestring);
    }
#endif
    return ret;
}

int
PrepareTranGTM(GlobalTransactionId gxid)
{
    int ret;
    struct rusage start_r;
    struct timeval start_t;

    if (!GlobalTransactionIdIsValid(gxid))
        return 0;
    CheckConnection();

    if (log_gtm_stats)
        ResetUsageCommon(&start_r, &start_t);

    ret = -1;
    if (conn)
        ret = prepare_transaction(conn, gxid);

    /*
     * If something went wrong (timeout), try and reset GTM connection.
     * We will close the transaction locally anyway, and closing GTM will force
     * it to be closed on GTM.
     */
    if (ret < 0)
    {
        CloseGTM();
        InitGTM();
        if (conn)
            ret = prepare_transaction(conn, gxid);
    }
    currentGxid = InvalidGlobalTransactionId;

    if (log_gtm_stats)
        ShowUsageCommon("PrepareTranGTM", &start_r, &start_t);

    return ret;
}


int
GetGIDDataGTM(char *gid,
              GlobalTransactionId *gxid,
              GlobalTransactionId *prepared_gxid,
              char **nodestring)
{
    int ret = 0;

    CheckConnection();
    ret = -1;
    if (conn)
        ret = get_gid_data(conn, GTM_ISOLATION_RC, gid, gxid,
                       prepared_gxid, nodestring);

    /*
     * If something went wrong (timeout), try and reset GTM connection.
     * We will abort the transaction locally anyway, and closing GTM will force
     * it to end on GTM.
     */
    if (ret < 0)
    {
        CloseGTM();
        InitGTM();
        if (conn)
            ret = get_gid_data(conn, GTM_ISOLATION_RC, gid, gxid,
                               prepared_gxid, nodestring);
    }

    return ret;
}

GTM_Snapshot
GetSnapshotGTM(GlobalTransactionId gxid, bool canbe_grouped)
{
    GTM_Snapshot ret_snapshot = NULL;
    struct rusage start_r;
    struct timeval start_t;

    CheckConnection();

    if (log_gtm_stats)
        ResetUsageCommon(&start_r, &start_t);

    if (conn)
        ret_snapshot = get_snapshot(conn, gxid, canbe_grouped);
    if (ret_snapshot == NULL)
    {
        CloseGTM();
        InitGTM();
        if (conn)
            ret_snapshot = get_snapshot(conn, gxid, canbe_grouped);
    }

    if (log_gtm_stats)
        ShowUsageCommon("GetSnapshotGTM", &start_r, &start_t);

    return ret_snapshot;
}


/*
 * Create a sequence on the GTM.
 */
int
CreateSequenceGTM(char *seqname, GTM_Sequence increment, GTM_Sequence minval,
                  GTM_Sequence maxval, GTM_Sequence startval, bool cycle)
{
    GTM_SequenceKeyData seqkey;
    CheckConnection();
    seqkey.gsk_keylen = strlen(seqname) + 1;
    seqkey.gsk_key    = seqname;
    seqkey.gsk_type   = GTM_SEQ_FULL_NAME;

    return conn ? open_sequence(conn, &seqkey, increment, minval, maxval,
            startval, cycle, GetTopTransactionId()) : 0;
}

/*
 * Alter a sequence on the GTM
 */
int
AlterSequenceGTM(char *seqname, GTM_Sequence increment, GTM_Sequence minval,
                 GTM_Sequence maxval, GTM_Sequence startval, GTM_Sequence lastval, bool cycle, bool is_restart)
{
    GTM_SequenceKeyData seqkey;
    CheckConnection();
    seqkey.gsk_keylen = strlen(seqname) + 1;
    seqkey.gsk_key = seqname;

    return conn ? alter_sequence(conn, &seqkey, increment, minval, maxval,
            startval, lastval, cycle, is_restart) : 0;
}

/*
 * get the current sequence value
 */

GTM_Sequence
GetCurrentValGTM(char *seqname)
{
    GTM_Sequence ret = -1;
    GTM_SequenceKeyData seqkey;
    char   *coordName = IS_PGXC_COORDINATOR ? PGXCNodeName : GetMyCoordName;
    int        coordPid = IS_PGXC_COORDINATOR ? MyProcPid : MyCoordPid;
    int        status;

    CheckConnection();
    seqkey.gsk_keylen = strlen(seqname) + 1;
    seqkey.gsk_key = seqname;

    if (conn)
        status = get_current(conn, &seqkey, coordName, coordPid, &ret);
    else
        status = GTM_RESULT_COMM_ERROR;

    /* retry once */
    if (status == GTM_RESULT_COMM_ERROR)
    {
        CloseGTM();
        InitGTM();
        if (conn)
            status = get_current(conn, &seqkey, coordName, coordPid, &ret);
    }
    if (status != GTM_RESULT_OK)
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("%s", GTMPQerrorMessage(conn))));
    return ret;
}

/*
 * Get the next sequence value
 */
GTM_Sequence
GetNextValGTM(char *seqname, GTM_Sequence range, GTM_Sequence *rangemax)
{
    GTM_Sequence ret = -1;
    GTM_SequenceKeyData seqkey;
    char   *coordName = IS_PGXC_COORDINATOR ? PGXCNodeName : GetMyCoordName;
    int        coordPid = IS_PGXC_COORDINATOR ? MyProcPid : MyCoordPid;
    int        status;

    CheckConnection();
    seqkey.gsk_keylen = strlen(seqname) + 1;
    seqkey.gsk_key = seqname;

    if (conn)
        status = get_next(conn, &seqkey, coordName,
                          coordPid, range, &ret, rangemax);
    else
        status = GTM_RESULT_COMM_ERROR;

    /* retry once */
    if (status == GTM_RESULT_COMM_ERROR)
    {
        CloseGTM();
        InitGTM();
        if (conn)
            status = get_next(conn, &seqkey, coordName, coordPid,
                              range, &ret, rangemax);
    }
    if (status != GTM_RESULT_OK)
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("%s", GTMPQerrorMessage(conn))));
    return ret;
}

/*
 * Set values for sequence
 */
int
SetValGTM(char *seqname, GTM_Sequence nextval, bool iscalled)
{
    GTM_SequenceKeyData seqkey;
    char   *coordName = IS_PGXC_COORDINATOR ? PGXCNodeName : GetMyCoordName;
    int        coordPid = IS_PGXC_COORDINATOR ? MyProcPid : MyCoordPid;

    CheckConnection();
    seqkey.gsk_keylen = strlen(seqname) + 1;
    seqkey.gsk_key = seqname;

    return conn ? set_val(conn, &seqkey, coordName, coordPid, nextval, iscalled) : -1;
}

/*
 * Drop the sequence depending the key type
 *
 * Type of Sequence name use in key;
 *        GTM_SEQ_FULL_NAME, full name of sequence
 *        GTM_SEQ_DB_NAME, DB name part of sequence key
 */
int
DropSequenceGTM(char *name, GTM_SequenceKeyType type)
{
    GTM_SequenceKeyData seqkey;
    CheckConnection();
    seqkey.gsk_keylen = strlen(name) + 1;
    seqkey.gsk_key = name;
    seqkey.gsk_type = type;

    return conn ? close_sequence(conn, &seqkey, GetTopTransactionId()) : -1;
}

/*
 * Rename the sequence
 */
int
RenameSequenceGTM(char *seqname, const char *newseqname)
{
    GTM_SequenceKeyData seqkey, newseqkey;
    CheckConnection();
    seqkey.gsk_keylen = strlen(seqname) + 1;
    seqkey.gsk_key = seqname;
    
    newseqkey.gsk_keylen = strlen(newseqname) + 1;
    newseqkey.gsk_key = (char *) newseqname;
    return conn ? rename_sequence(conn, &seqkey, &newseqkey,
            GetTopTransactionId()) : -1;
}

/*
 * Copy the database sequences from src database
 */
int
CopyDataBaseSequenceGTM(char *src_dbname, char *dest_dbname)
{
    GTM_SequenceKeyData src_seqkey, dest_seqkey;
    CheckConnection();
    src_seqkey.gsk_keylen = strlen(src_dbname) + 1;
    src_seqkey.gsk_key = src_dbname;

    dest_seqkey.gsk_keylen = strlen(dest_dbname) + 1;
    dest_seqkey.gsk_key = (char *) dest_dbname;
    return conn ? copy_database_sequence(conn, &src_seqkey, &dest_seqkey,
                                  GetTopTransactionId()) : -1;
}

/*
 * Register Given Node
 * Connection for registering is just used once then closed
 */
int
RegisterGTM(GTM_PGXCNodeType type)
{
    int ret;

    CheckConnection();

    if (!conn)
        return EOF;

    ret = node_register(conn, type, 0, PGXCNodeName, "");
    elog(LOG, "node register %s", PGXCNodeName);
    /* If something went wrong, retry once */
    if (ret < 0)
    {
        CloseGTM();
        InitGTM();
        if (conn)
            ret = node_register(conn, type, 0, PGXCNodeName, "");
    }

    return ret;
}

/*
 * UnRegister Given Node
 * Connection for registering is just used once then closed
 */
int
UnregisterGTM(GTM_PGXCNodeType type)
{
    int ret;

    CheckConnection();

    if (!conn)
        return EOF;

    ret = node_unregister(conn, type, PGXCNodeName);

    /* If something went wrong, retry once */
    if (ret < 0)
    {
        CloseGTM();
        InitGTM();
        if (conn)
            ret = node_unregister(conn, type, PGXCNodeName);
    }

    /*
     * If node is unregistered cleanly, cut the connection.
     * and Node shuts down smoothly.
     */
    CloseGTM();

    return ret;
}

/*
 * Report BARRIER
 */
int
ReportBarrierGTM(const char *barrier_id)
{
    if (!gtm_backup_barrier)
        return EINVAL;

    CheckConnection();

    if (!conn)
        return EOF;

    return(report_barrier(conn, barrier_id));
}

int
ReportGlobalXmin(GlobalTransactionId gxid, GlobalTransactionId *global_xmin,
        GlobalTransactionId *latest_completed_xid)
{
    int errcode = GTM_ERRCODE_UNKNOWN;

    CheckConnection();
    if (!conn)
        return EOF;

    report_global_xmin(conn, PGXCNodeName,
            IS_PGXC_COORDINATOR ?  GTM_NODE_COORDINATOR : GTM_NODE_DATANODE,
            gxid, global_xmin, latest_completed_xid, &errcode);
    return errcode;
}


void 
CleanGTMSeq(void)
{
    char   *coordName = IS_PGXC_COORDINATOR ? PGXCNodeName : GetMyCoordName;
    int        coordPid = IS_PGXC_COORDINATOR ? MyProcPid : MyCoordPid;
    int        status;

    CheckConnection();

    if (conn)
        status = clean_session_sequence(conn, coordName, coordPid);
    else
        status = GTM_RESULT_COMM_ERROR;

    /* retry once */
    if (status == GTM_RESULT_COMM_ERROR)
    {
        CloseGTM();
        InitGTM();
        if (conn)
            status = clean_session_sequence(conn, coordName, coordPid);
    }
    if (status != GTM_RESULT_OK)
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("%s", GTMPQerrorMessage(conn))));

    return;
}

#ifdef __TBASE__
static void ResetGtmInfo(void)
{
    if (GtmHost)
    {
        free(GtmHost);
        GtmHost = NULL;
    }
    GtmPort = 0;
}

void
CheckGTMConnection(void)
{
    CheckConnection();
}

/*
 * Rename the sequence
 */
int
RenameDBSequenceGTM(const char *seqname, const char *newseqname)
{
    GTM_SequenceKeyData seqkey, newseqkey;
    CheckConnection();
    seqkey.gsk_keylen = strlen(seqname) + 1;
    seqkey.gsk_key = (char*)seqname;
    
    newseqkey.gsk_keylen = strlen(newseqname) + 1;
    newseqkey.gsk_key = (char *) newseqname;
    return conn ? rename_db_sequence(conn, &seqkey, &newseqkey,
            GetTopTransactionId()) : -1;
}

#endif

