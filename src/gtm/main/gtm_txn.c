/*-------------------------------------------------------------------------
 *
 * gtm_txn.c
 *    Transaction handling
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *      $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#include "gtm/gtm_txn.h"

#include <unistd.h>
#include <gtm/gtm_xlog.h>
#include <gtm/gtm_xlog_internal.h>
#include "gtm/assert.h"
#include "gtm/elog.h"
#include "gtm/gtm.h"
#include "gtm/gtm_time.h"
#include "gtm/gtm_txn.h"
#include "gtm/gtm_serialize.h"
#include "gtm/gtm_standby.h"
#include "gtm/standby_utils.h"
#include "gtm/libpq.h"
#include "gtm/libpq-int.h"
#include "gtm/pqformat.h"
#include "gtm/gtm_backup.h"
#include "gtm/gtm_store.h"
#include "port/atomics.h"


extern bool Backup_synchronously;

/* Local functions */
static XidStatus GlobalTransactionIdGetStatus(GlobalTransactionId transactionId);
#ifndef __XLOG__
static bool GTM_SetDoVacuum(GTM_TransactionHandle handle);
#else
extern int         max_wal_sender;
extern time_t      g_last_sync_gts; 
extern s_lock_t     g_last_sync_gts_lock;
#endif
extern int GTMStartupGTSDelta;
extern bool GTMClusterReadOnly;
static void init_GTM_TransactionInfo(GTM_TransactionInfo *gtm_txninfo,
                                     GTM_TransactionHandle txn,
                                     GTM_IsolationLevel isolevel,
                                     uint32 client_id,
                                     GTMProxy_ConnID connid,
                                     const char *global_sessionid,
                                     bool readonly);
static GTM_TransactionHandle GTM_GlobalSessionIDToHandle(
                                    const char *global_sessionid);

GlobalTransactionId ControlXid;  /* last one written to control file */
GTM_Transactions GTMTransactions;
#ifdef __TBASE__
#endif
void
GTM_InitTxnManager(void)
{
#ifndef __TBASE__
    int ii;

    memset(&GTMTransactions, 0, sizeof (GTM_Transactions));

    for (ii = 0; ii < GTM_MAX_GLOBAL_TRANSACTIONS; ii++)
    {
        GTM_TransactionInfo *gtm_txninfo = &GTMTransactions.gt_transactions_array[ii];
        gtm_txninfo->gti_in_use = false;
        GTM_RWLockInit(&gtm_txninfo->gti_lock);
    }

    /*
     * XXX When GTM is stopped and restarted, it must start assinging GXIDs
     * greater than the previously assgined values. If it was a clean shutdown,
     * the GTM can store the last assigned value at a known location on
     * permanent storage and read it back when it's restarted. It will get
     * trickier for GTM failures.
     *
     * TODO We skip this part for the prototype.
     */
    GTMTransactions.gt_nextXid = FirstNormalGlobalTransactionId;
    pg_atomic_init_u32(&GTMTransactions.gt_global_xid, FirstNormalGlobalTransactionId);
    pg_atomic_init_u64(&GTMTransactions.gt_access_ts_seq, 0);
    pg_atomic_init_u64(&GTMTransactions.gt_last_access_ts_seq, 0);
    /*
     * XXX The gt_oldestXid is the cluster level oldest Xid
     */
    GTMTransactions.gt_oldestXid = FirstNormalGlobalTransactionId;

    /*
     * XXX Compute various xid limits to avoid wrap-around related database
     * corruptions. Again, this is not implemented for the prototype
     */
    GTMTransactions.gt_xidVacLimit = InvalidGlobalTransactionId;
    GTMTransactions.gt_xidWarnLimit = InvalidGlobalTransactionId;
    GTMTransactions.gt_xidStopLimit = InvalidGlobalTransactionId;
    GTMTransactions.gt_xidWrapLimit = InvalidGlobalTransactionId;

    /*
     * XXX Newest XID that is committed or aborted
     */
    GTMTransactions.gt_latestCompletedXid = FirstNormalGlobalTransactionId;

    /* Initialise gt_recent_global_xmin */
    GTMTransactions.gt_recent_global_xmin = FirstNormalGlobalTransactionId;

    /*
     * Initialize the locks to protect various XID fields as well as the linked
     * list of transactions
     */
    GTM_RWLockInit(&GTMTransactions.gt_XidGenLock);
    GTM_RWLockInit(&GTMTransactions.gt_TransArrayLock);
    
    for(ii = 0; ii < GTM_MAX_THREADS; ii++)
        GTMTransactions.gt_in_locking[ii].lock = 0;
    /*
     * Initialize the list
     */
    GTMTransactions.gt_open_transactions = gtm_NIL;
    GTMTransactions.gt_lastslot = -1;

    GTMTransactions.gt_gtm_state = GTM_STARTING;

    ControlXid = FirstNormalGlobalTransactionId;
#endif
    return;
}

/*
 * Get the status of current or past transaction.
 */
static XidStatus
GlobalTransactionIdGetStatus(GlobalTransactionId transactionId)
{
    XidStatus    xidstatus = TRANSACTION_STATUS_IN_PROGRESS;

    /*
     * Also, check to see if the transaction ID is a permanent one.
     */
    if (!GlobalTransactionIdIsNormal(transactionId))
    {
        if (GlobalTransactionIdEquals(transactionId, BootstrapGlobalTransactionId))
            return TRANSACTION_STATUS_COMMITTED;
        if (GlobalTransactionIdEquals(transactionId, FrozenGlobalTransactionId))
            return TRANSACTION_STATUS_COMMITTED;
        return TRANSACTION_STATUS_ABORTED;
    }

    /*
     * TODO To be implemented
     * This code is not completed yet and the latter code must not be reached.
     */
    Assert(0);
    return xidstatus;
}

/*
 * Given the GXID, find the corresponding transaction handle.
 */
static GTM_TransactionHandle
GTM_GXIDToHandle_Internal(GlobalTransactionId gxid, bool warn)
{
    gtm_ListCell *elem = NULL;
       GTM_TransactionInfo *gtm_txninfo = NULL;

    if (!GlobalTransactionIdIsValid(gxid))
        return InvalidTransactionHandle;

    GTM_RWLockAcquire(&GTMTransactions.gt_TransArrayLock, GTM_LOCKMODE_READ);

    gtm_foreach(elem, GTMTransactions.gt_open_transactions)
    {
        gtm_txninfo = (GTM_TransactionInfo *)gtm_lfirst(elem);
        if (GlobalTransactionIdEquals(gtm_txninfo->gti_gxid, gxid))
            break;
        gtm_txninfo = NULL;
    }

    GTM_RWLockRelease(&GTMTransactions.gt_TransArrayLock);

    if (gtm_txninfo != NULL)
        return gtm_txninfo->gti_handle;
    else
    {
        if (warn)
            ereport(WARNING,
                (ERANGE, errmsg("No transaction handle for gxid: %d",
                                gxid)));
        return InvalidTransactionHandle;
    }
}

GTM_TransactionHandle
GTM_GXIDToHandle(GlobalTransactionId gxid)
{
    return GTM_GXIDToHandle_Internal(gxid, true);
}

static GTM_TransactionHandle
GTM_GlobalSessionIDToHandle(const char *global_sessionid)
{
    gtm_ListCell *elem = NULL;
    GTM_TransactionInfo    *gtm_txninfo = NULL;

    if (global_sessionid == NULL || global_sessionid[0] == '\0')
        return InvalidTransactionHandle;

    gtm_foreach(elem, GTMTransactions.gt_open_transactions)
    {
        gtm_txninfo = (GTM_TransactionInfo *)gtm_lfirst(elem);
        if (strcmp(gtm_txninfo->gti_global_session_id, global_sessionid) == 0)
            break;
        gtm_txninfo = NULL;
    }
    if (gtm_txninfo != NULL)
        return gtm_txninfo->gti_handle;

    return InvalidTransactionHandle;
}

bool
GTM_IsGXIDInProgress(GlobalTransactionId gxid)
{
    return (GTM_GXIDToHandle_Internal(gxid, false) !=
            InvalidTransactionHandle);
}
/*
 * Given the GID (for a prepared transaction), find the corresponding
 * transaction handle.
 */
GTM_TransactionHandle
GTM_GIDToHandle(char *gid)
{
    gtm_ListCell *elem = NULL;
    GTM_TransactionInfo *gtm_txninfo = NULL;

    GTM_RWLockAcquire(&GTMTransactions.gt_TransArrayLock, GTM_LOCKMODE_READ);

    gtm_foreach(elem, GTMTransactions.gt_open_transactions)
    {
        gtm_txninfo = (GTM_TransactionInfo *)gtm_lfirst(elem);
        if (gtm_txninfo->gti_gid && strcmp(gid,gtm_txninfo->gti_gid) == 0)
            break;
        gtm_txninfo = NULL;
    }

    GTM_RWLockRelease(&GTMTransactions.gt_TransArrayLock);

    if (gtm_txninfo != NULL)
        return gtm_txninfo->gti_handle;
    else
        return InvalidTransactionHandle;
}


/*
 * Given the transaction handle, find the corresponding transaction info
 * structure
 *
 * Note: Since a transaction handle is just an index into the global array,
 * this function should be very quick. We should turn into an inline future for
 * fast path.
 */
GTM_TransactionInfo *
GTM_HandleToTransactionInfo(GTM_TransactionHandle handle)
{
    GTM_TransactionInfo *gtm_txninfo = NULL;

    if ((handle < 0) || (handle >= GTM_MAX_GLOBAL_TRANSACTIONS))
    {
        ereport(WARNING,
                (ERANGE, errmsg("Invalid transaction handle: %d", handle)));
        return NULL;
    }

    gtm_txninfo = &GTMTransactions.gt_transactions_array[handle];

    if (!gtm_txninfo->gti_in_use)
    {
        ereport(WARNING,
                (ERANGE, errmsg("Invalid transaction handle (%d), txn_info not in use",
                                handle)));
        return NULL;
    }

    return gtm_txninfo;
}

/*
 * Remove all transaction infos associated with the caller thread and the given
 * backend
 *
 * Also compute the latestCompletedXid.
 */
void
GTM_RemoveAllTransInfos(uint32 client_id, int backend_id)
{
    return;
}

/*
 * Get the latest client identifier issued to the currently open transactions.
 * Remember this may not be the latest identifier issued by the old master, but
 * we won't acknowledge client identifiers larger than what we are about to
 * compute. Any such identifiers will be overwritten the new identifier issued
 * by the new master
 */
uint32
GTMGetLastClientIdentifier(void)
{
    gtm_ListCell *cell;
    uint32 last_client_id = 0;

    /*
     * Scan the global list of open transactions
     */
    GTM_RWLockAcquire(&GTMTransactions.gt_TransArrayLock, GTM_LOCKMODE_WRITE);

    cell = gtm_list_head(GTMTransactions.gt_open_transactions);
    while (cell != NULL)
    {
        GTM_TransactionInfo *gtm_txninfo = gtm_lfirst(cell);

        if (GTM_CLIENT_ID_GT(gtm_txninfo->gti_client_id, last_client_id))
            last_client_id = gtm_txninfo->gti_client_id;
        cell = gtm_lnext(cell);
    }

    GTM_RWLockRelease(&GTMTransactions.gt_TransArrayLock);
    return last_client_id;
}

/*
 * GlobalTransactionIdDidCommit
 *        True iff transaction associated with the identifier did commit.
 *
 * Note:
 *        Assumes transaction identifier is valid.
 */
bool                            /* true if given transaction committed */
GlobalTransactionIdDidCommit(GlobalTransactionId transactionId)
{
    XidStatus    xidstatus;

    xidstatus = GlobalTransactionIdGetStatus(transactionId);

    /*
     * If it's marked committed, it's committed.
     */
    if (xidstatus == TRANSACTION_STATUS_COMMITTED)
        return true;

    /*
     * It's not committed.
     */
    return false;
}

/*
 * GlobalTransactionIdDidAbort
 *        True iff transaction associated with the identifier did abort.
 *
 * Note:
 *        Assumes transaction identifier is valid.
 */
bool                            /* true if given transaction aborted */
GlobalTransactionIdDidAbort(GlobalTransactionId transactionId)
{
    XidStatus    xidstatus;

    xidstatus = GlobalTransactionIdGetStatus(transactionId);

    /*
     * If it's marked aborted, it's aborted.
     */
    if (xidstatus == TRANSACTION_STATUS_ABORTED)
        return true;

    /*
     * It's not aborted.
     */
    return false;
}


#ifndef __XLOG__
/*
 * Set that the transaction is doing vacuum
 *
 */
static bool
GTM_SetDoVacuum(GTM_TransactionHandle handle)
{
    GTM_TransactionInfo *gtm_txninfo = GTM_HandleToTransactionInfo(handle);

    if (gtm_txninfo == NULL)
        ereport(ERROR, (EINVAL, errmsg("Invalid transaction handle")));

    gtm_txninfo->gti_vacuum = true;
    return true;
}
#endif

/*
 * Allocate the next XID for my new transaction
 *
 * The new XID is also stored into the transaction info structure of the given
 * transaction before returning.
 */
bool
GTM_GetGlobalTransactionIdMulti(GTM_TransactionHandle handle[], int txn_count,
        GlobalTransactionId gxid[], GTM_TransactionHandle new_handle[],
        int *new_txn_count)
{// #lizard forgives
    GlobalTransactionId xid = InvalidGlobalTransactionId;
    GTM_TransactionInfo *gtm_txninfo = NULL;
    int ii;

    if (Recovery_IsStandby())
    {
        ereport(ERROR, (EINVAL, errmsg("GTM is running in STANDBY mode -- can not issue new transaction ids")));
        return false;
    }

    GTM_RWLockAcquire(&GTMTransactions.gt_XidGenLock, GTM_LOCKMODE_WRITE);

    if (GTMTransactions.gt_gtm_state == GTM_SHUTTING_DOWN)
    {
        GTM_RWLockRelease(&GTMTransactions.gt_XidGenLock);
        ereport(ERROR, (EINVAL, errmsg("GTM shutting down -- can not issue new transaction ids")));
        return false;
    }

    *new_txn_count = 0;
    /*
     * Now advance the nextXid counter.  This must not happen until after we
     * have successfully completed ExtendCLOG() --- if that routine fails, we
     * want the next incoming transaction to try it again.    We cannot assign
     * more XIDs until there is CLOG space for them.
     */
    for (ii = 0; ii < txn_count; ii++)
    {
        gtm_txninfo = GTM_HandleToTransactionInfo(handle[ii]);
        Assert(gtm_txninfo);

        if (GlobalTransactionIdIsValid(gtm_txninfo->gti_gxid))
        {
            gxid[ii] = gtm_txninfo->gti_gxid;
            elog(DEBUG6, "GTM_TransactionInfo has XID already assgined - %s:%d",
                    gtm_txninfo->gti_global_session_id, gxid[ii]);
            continue;
        }

        xid = GTMTransactions.gt_nextXid;

        /*----------
         * Check to see if it's safe to assign another XID.  This protects against
         * catastrophic data loss due to XID wraparound.  The basic rules are:
         *
         * If we're past xidVacLimit, start trying to force autovacuum cycles.
         * If we're past xidWarnLimit, start issuing warnings.
         * If we're past xidStopLimit, refuse to execute transactions, unless
         * we are running in a standalone backend (which gives an escape hatch
         * to the DBA who somehow got past the earlier defenses).
         *
         * Test is coded to fall out as fast as possible during normal operation,
         * ie, when the vac limit is set and we haven't violated it.
         *----------
         */
        if (GlobalTransactionIdFollowsOrEquals(xid, GTMTransactions.gt_xidVacLimit) &&
            GlobalTransactionIdIsValid(GTMTransactions.gt_xidVacLimit))
        {
            if (GlobalTransactionIdFollowsOrEquals(xid, GTMTransactions.gt_xidStopLimit))
            {
                GTM_RWLockRelease(&GTMTransactions.gt_XidGenLock);
                ereport(ERROR,
                        (ERANGE,
                         errmsg("database is not accepting commands to avoid wraparound data loss in database ")));
            }
            else if (GlobalTransactionIdFollowsOrEquals(xid, GTMTransactions.gt_xidWarnLimit))
                ereport(WARNING,
                (errmsg("database must be vacuumed within %u transactions",
                        GTMTransactions.gt_xidWrapLimit - xid)));
        }

        GlobalTransactionIdAdvance(GTMTransactions.gt_nextXid);

        elog(DEBUG1, "Assigning new transaction ID = %s:%d",
                gtm_txninfo->gti_global_session_id, xid);
        gxid[ii] = gtm_txninfo->gti_gxid = xid;
        new_handle[*new_txn_count] = gtm_txninfo->gti_handle;
        *new_txn_count = *new_txn_count + 1;
    }

    /* Periodically write the xid and sequence info out to the control file.
     * Try and handle wrapping, too.
     */
    if (GlobalTransactionIdIsValid(xid) &&
            (xid - ControlXid > CONTROL_INTERVAL || xid < ControlXid))
    {
#ifdef __TBASE__
        int32 ret;
        ret =  GTM_StoreReserveXid(CONTROL_INTERVAL);
        if (ret)
        {
            GTM_RWLockRelease(&GTMTransactions.gt_XidGenLock);
            elog(LOG, "GTM_GetGlobalTransactionIdMulti reserved gxid failed");
            return false;
        }
#endif
        ControlXid = xid;
    }
    
#ifndef __TBASE__    
    if (GTM_NeedXidRestoreUpdate())
    {
        GTM_SetNeedBackup();
    }
#endif

    GTM_RWLockRelease(&GTMTransactions.gt_XidGenLock);

    return true;
}

/*
 * Allocate the next XID for my new transaction
 *
 * The new XID is also stored into the transaction info structure of the given
 * transaction before returning.
 */
GlobalTransactionId
GTM_GetGlobalTransactionId(GTM_TransactionHandle handle)
{
    GlobalTransactionId gxid;
    GTM_TransactionHandle new_handle;
    int new_count;

    GTM_GetGlobalTransactionIdMulti(&handle, 1, &gxid, &new_handle,
            &new_count);
    return gxid;
}

/*
 * Read nextXid but don't allocate it.
 */
GlobalTransactionId
ReadNewGlobalTransactionId(void)
{
    GlobalTransactionId xid;
#ifdef __TBASE__
    xid = pg_atomic_read_u32(&GTMTransactions.gt_global_xid);
    return xid;    
#else
    GTM_RWLockAcquire(&GTMTransactions.gt_XidGenLock, GTM_LOCKMODE_READ);
    xid = GTMTransactions.gt_nextXid;
    GTM_RWLockRelease(&GTMTransactions.gt_XidGenLock);
    return xid;
#endif    

}

/*
 * Set the nextXid.
 *
 * The GXID is usually read from a control file and set when the GTM is
 * started. When the GTM is finally shutdown, the next to-be-assigned GXID is
 * stroed in the control file.
 *
 * XXX We don't yet handle any crash recovery. So if the GTM is no shutdown normally...
 *
 * This is handled by gtm_backup.c.  Anyway, because this function is to be called by
 * GTM_RestoreTransactionId() and the backup will be performed afterwords,
 * we don't care the new value of GTMTransactions.gt_nextXid here.
 */
void
SetNextGlobalTransactionId(GlobalTransactionId gxid)
{
    GTM_RWLockAcquire(&GTMTransactions.gt_XidGenLock, GTM_LOCKMODE_WRITE);
    GTMTransactions.gt_nextXid = gxid;
    GTMTransactions.gt_gtm_state = GTM_RUNNING;
    pg_atomic_init_u32(&(GTMTransactions.gt_global_xid), gxid);
    GTM_RWLockRelease(&GTMTransactions.gt_XidGenLock);
    return;
}



void
SetControlXid(GlobalTransactionId gxid)
{
    ControlXid = gxid;
}

/* Transaction Control */
int
GTM_BeginTransactionMulti(GTM_IsolationLevel isolevel[],
                     bool readonly[],
                     const char *global_sessionid[],
                     GTMProxy_ConnID connid[],
                     int txn_count,
                     GTM_TransactionHandle txns[])
{
    GTM_TransactionInfo *gtm_txninfo[txn_count];
    MemoryContext oldContext;
    int kk;
    
    memset(gtm_txninfo, 0, sizeof (gtm_txninfo));

    /*
     * XXX We should allocate the transaction info structure in the
     * top-most memory context instead of a thread context. This is
     * necessary because the transaction may outlive the thread which
     * started the transaction. Also, since the structures are stored in
     * the global array, it's dangerous to free the structures themselves
     * without removing the corresponding references from the global array
     */
    oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

    GTM_RWLockAcquire(&GTMTransactions.gt_TransArrayLock, GTM_LOCKMODE_WRITE);

    for (kk = 0; kk < txn_count; kk++)
    {
        int ii, jj, startslot;
        GTM_TransactionHandle txn =
                GTM_GlobalSessionIDToHandle(global_sessionid[kk]);

        if (txn != InvalidTransactionHandle)
        {
            gtm_txninfo[kk] = GTM_HandleToTransactionInfo(txn);
            elog(DEBUG1, "Existing transaction found: %s:%d",
                    gtm_txninfo[kk]->gti_global_session_id,
                    gtm_txninfo[kk]->gti_gxid);
            txns[kk] = txn;
            continue;
        }

        /*
         * We had no cached slots. Now find a free slot in the transation array
         * and store the transaction info structure there
         */
        startslot = GTMTransactions.gt_lastslot + 1;
        if (startslot >= GTM_MAX_GLOBAL_TRANSACTIONS)
            startslot = 0;

        for (ii = startslot, jj = 0;
             jj < GTM_MAX_GLOBAL_TRANSACTIONS;
             ii = (ii + 1) % GTM_MAX_GLOBAL_TRANSACTIONS, jj++)
        {
            if (GTMTransactions.gt_transactions_array[ii].gti_in_use == false)
            {
                gtm_txninfo[kk] = &GTMTransactions.gt_transactions_array[ii];
                break;
            }

            if (ii == GTMTransactions.gt_lastslot)
            {
                GTM_RWLockRelease(&GTMTransactions.gt_TransArrayLock);
                ereport(ERROR,
                        (ERANGE, errmsg("Max transaction limit reached")));
            }
        }

        init_GTM_TransactionInfo(gtm_txninfo[kk], ii, isolevel[kk],
                1, connid[kk],
                global_sessionid[kk],
                readonly[kk]);

        GTMTransactions.gt_lastslot = ii;

        txns[kk] = ii;

        /*
         * Add the structure to the global list of open transactions. We should
         * call add the element to the list in the context of TopMostMemoryContext
         * because the list is global and any memory allocation must outlive the
         * thread context
         */
        GTMTransactions.gt_open_transactions = gtm_lappend(GTMTransactions.gt_open_transactions, gtm_txninfo[kk]);
    }

    GTM_RWLockRelease(&GTMTransactions.gt_TransArrayLock);

    MemoryContextSwitchTo(oldContext);

    return txn_count;
}

/* Transaction Control */
GTM_TransactionHandle
GTM_BeginTransaction(GTM_IsolationLevel isolevel,
                     bool readonly,
                     const char *global_sessionid)
{
    GTM_TransactionHandle txn;
    GTMProxy_ConnID connid = -1;

    GTM_BeginTransactionMulti(&isolevel, &readonly, &global_sessionid, &connid, 1, &txn);
    return txn;
}

static void
init_GTM_TransactionInfo(GTM_TransactionInfo *gtm_txninfo,
                         GTM_TransactionHandle txn,
                         GTM_IsolationLevel isolevel,
                         uint32 client_id,
                         GTMProxy_ConnID connid,
                         const char *global_sessionid,
                         bool readonly)
{
    gtm_txninfo->gti_gxid = InvalidGlobalTransactionId;
    gtm_txninfo->gti_xmin = InvalidGlobalTransactionId;
    gtm_txninfo->gti_state = GTM_TXN_STARTING;

    gtm_txninfo->gti_isolevel = isolevel;
    gtm_txninfo->gti_readonly = readonly;
    gtm_txninfo->gti_in_use = true;

    if (global_sessionid)
        strncpy(gtm_txninfo->gti_global_session_id, global_sessionid,
                GTM_MAX_SESSION_ID_LEN);
    else
        gtm_txninfo->gti_global_session_id[0] = '\0';

    gtm_txninfo->nodestring = NULL;
    gtm_txninfo->gti_gid = NULL;

    gtm_txninfo->gti_handle = txn;
    gtm_txninfo->gti_vacuum = false;

    /*
     * For every new transaction that gets created, we track two important
     * identifiers:
     *
     * gt_client_id: is the identifier assigned to the client connected to
     * GTM. Whenever a connection to GTM is dropped, we must clean up all
     * transactions opened by that client. Since we track all open transactions
     * in a global data structure, this identifier helps us to identify
     * client-specific transactions. Also, the identifier is issued and tracked
     * irrespective of whether the remote client is a GTM proxy or a PG
     * backend.
     *
     * gti_proxy_client_id: is the identifier assigned by the GTM proxy to its
     * client. Proxy sends us this identifier and we track it in the list of
     * open transactions. If a backend disconnects from the proxy, it sends us
     * a MSG_BACKEND_DISCONNECT message, along with the backend identifier. As
     * a response to that message, we clean up all the transactions opened by
     * the backend.
     */ 
    gtm_txninfo->gti_client_id = client_id;
    gtm_txninfo->gti_proxy_client_id = connid;
}

void
GTM_BkupBeginTransactionMulti(GTM_IsolationLevel *isolevel,
                              bool *readonly,
                              const char **global_sessionid,
                              uint32 *client_id,
                              GTMProxy_ConnID *connid,
                              int    txn_count)
{
    GTM_TransactionHandle txn[GTM_MAX_GLOBAL_TRANSACTIONS];
    MemoryContext oldContext;
    int count;

    oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

    count = GTM_BeginTransactionMulti(isolevel, readonly,
                                      global_sessionid, connid,
                                      txn_count, txn);
    if (count != txn_count)
        ereport(ERROR,
                (EINVAL,
                 errmsg("Failed to start %d new transactions", txn_count)));

    MemoryContextSwitchTo(oldContext);
}

void
GTM_BkupBeginTransaction(GTM_IsolationLevel isolevel,
                         bool readonly,
                         const char *global_sessionid,
                         uint32 client_id)
{
    GTMProxy_ConnID connid = -1;

    GTM_BkupBeginTransactionMulti(&isolevel, &readonly,
            &global_sessionid,
            &client_id, &connid, 1);
}
/*
 * Same as GTM_RollbackTransaction, but takes GXID as input
 */
int
GTM_RollbackTransactionGXID(GlobalTransactionId gxid)
{
    GTM_TransactionHandle txn = GTM_GXIDToHandle(gxid);
    return GTM_RollbackTransaction(txn);
}

/*
 * Rollback multiple transactions in one go.
 * In TBase, rollback transaction will not bother GTM.
 */
int
GTM_RollbackTransactionMulti(GTM_TransactionHandle txn[], int txn_count, int status[])
{
    return txn_count;
}

/*
 * Rollback a transaction
 */
int
GTM_RollbackTransaction(GTM_TransactionHandle txn)
{
    int status = 0;
    status = GTM_RollbackTransactionMulti(&txn, 1, &status);
    return status;
}


/*
 * Same as GTM_CommitTransaction but takes GXID as input
 */
int
GTM_CommitTransactionGXID(GlobalTransactionId gxid)
{
    GTM_TransactionHandle txn = GTM_GXIDToHandle(gxid);
    return GTM_CommitTransaction(txn, 0, NULL);
}

/*
 * Commit multiple transactions in one go.
 * In TBase, commit transaction will not bother GTM.
 */
int
GTM_CommitTransactionMulti(GTM_TransactionHandle txn[], int txn_count,
        int waited_xid_count, GlobalTransactionId *waited_xids,
        int status[])
{
    return txn_count;
}

/*
 * Prepare a transaction
 */
int
GTM_PrepareTransaction(GTM_TransactionHandle txn)
{
    GTM_TransactionInfo *gtm_txninfo = NULL;

    gtm_txninfo = GTM_HandleToTransactionInfo(txn);

    if (gtm_txninfo == NULL)
        return STATUS_ERROR;

    /*
     * Mark the transaction as prepared
     */
    GTM_RWLockAcquire(&gtm_txninfo->gti_lock, GTM_LOCKMODE_WRITE);
    gtm_txninfo->gti_state = GTM_TXN_PREPARED;
    GTM_RWLockRelease(&gtm_txninfo->gti_lock);

    return STATUS_OK;
}

/*
 * Commit a transaction
 */
int
GTM_CommitTransaction(GTM_TransactionHandle txn, int waited_xid_count,
        GlobalTransactionId *waited_xids)
{
    int status = 0;
    GTM_CommitTransactionMulti(&txn, 1, waited_xid_count, waited_xids, &status);
    return status;
}

/*
 * Prepare a transaction
 */
int
GTM_StartPreparedTransaction(GTM_TransactionHandle txn,
                             char *gid,
                             char *nodestring)
{
#ifdef __TBASE__
    int32          ret = -1;
    txn = txn;
    
    /* Prepare the TXN in gtm store */    
    ret = GTM_StoreBeginPrepareTxn(gid, nodestring);
    if (ret)
    {
        ereport(ERROR,
                (EPROTO,
                  errmsg("GTM_StartPreparedTransaction Prepare transaction in GTM Store failed")));
    }
#endif
    return STATUS_OK;
}

/*
 * Log a transaction
 */
int
GTM_LogTransaction(    GlobalTransactionId gxid,
                             const char *gid,
                             const char *nodestring,
                             int node_count,
                             int isGlobal,
                             int  isCommit,
                             GlobalTimestamp prepare_ts,
                             GlobalTimestamp commit_ts)
{
    int ret;
        
    ret = GTM_StoreLogTransaction(gxid, gid, nodestring, node_count, isGlobal, isCommit, prepare_ts, commit_ts);
    if (ret)
    {
        elog(LOG, "GTM_LogTransaction failed");
    }            
    

    return STATUS_OK;
}


/*
 * Log a transaction
 */
int
GTM_LogScan(GlobalTransactionId gxid,
                 const char *nodestring,
                 GlobalTimestamp start_ts,
                 GlobalTimestamp local_start_ts,
                 GlobalTimestamp local_complete_ts,
                 int scan_type,
                 const char *rel_name,
                 int64 scan_number)
{
    int ret;
        
    ret = GTM_StoreLogScan(gxid, nodestring, start_ts, local_start_ts, local_complete_ts, scan_type, rel_name, scan_number);
    if (ret)
    {
        elog(LOG, "GTM_LogScanTransaction failed");
    }            
    

    return STATUS_OK;
}


/*
 * Same as GTM_PrepareTransaction but takes GXID as input
 */
int
GTM_StartPreparedTransactionGXID(GlobalTransactionId gxid,
                                 char *gid,
                                 char *nodestring)
{
    GTM_TransactionHandle txn = GTM_GXIDToHandle(gxid);
    return GTM_StartPreparedTransaction(txn, gid, nodestring);
}

int
GTM_GetGIDData(GTM_TransactionHandle prepared_txn,
               GlobalTransactionId *prepared_gxid,
               char **nodestring)
{
    GTM_TransactionInfo    *gtm_txninfo = NULL;
    MemoryContext        oldContext;

    oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

    gtm_txninfo = GTM_HandleToTransactionInfo(prepared_txn);
    if (gtm_txninfo == NULL)
        return STATUS_ERROR;

    /* then get the necessary Data */
    *prepared_gxid = gtm_txninfo->gti_gxid;
    if (gtm_txninfo->nodestring)
    {
        *nodestring = (char *) palloc(strlen(gtm_txninfo->nodestring) + 1);
        memcpy(*nodestring, gtm_txninfo->nodestring, strlen(gtm_txninfo->nodestring) + 1);
        (*nodestring)[strlen(gtm_txninfo->nodestring)] = '\0';
    }
    else
        *nodestring = NULL;

    MemoryContextSwitchTo(oldContext);

    return STATUS_OK;
}

/*
 * Get status of the given transaction
 */
GTM_TransactionStates
GTM_GetStatus(GTM_TransactionHandle txn)
{
    GTM_TransactionInfo *gtm_txninfo = GTM_HandleToTransactionInfo(txn);
    return gtm_txninfo->gti_state;
}

/*
 * Same as GTM_GetStatus but takes GXID as input
 */
GTM_TransactionStates
GTM_GetStatusGXID(GlobalTransactionId gxid)
{
    GTM_TransactionHandle txn = GTM_GXIDToHandle(gxid);
    return GTM_GetStatus(txn);
}

/*
 * Process MSG_TXN_BEGIN message
 */
void
ProcessBeginTransactionCommand(Port *myport, StringInfo message)
{
    GTM_IsolationLevel txn_isolation_level;
    bool txn_read_only;
    StringInfoData buf;
    GTM_TransactionHandle txn;
    GTM_Timestamp timestamp;
    MemoryContext oldContext;
    uint32 global_sessionid_len;
    const char *global_sessionid;

    txn_isolation_level = pq_getmsgint(message, sizeof (GTM_IsolationLevel));
    txn_read_only = pq_getmsgbyte(message);
    global_sessionid_len = pq_getmsgint(message, sizeof (uint32));
    global_sessionid = pq_getmsgbytes(message, global_sessionid_len);

    oldContext = MemoryContextSwitchTo(TopMemoryContext);

    /*
     * Start a new transaction
     */
    txn = GTM_BeginTransaction(txn_isolation_level, txn_read_only,
            global_sessionid);
    if (txn == InvalidTransactionHandle)
        ereport(ERROR,
                (EINVAL,
                 errmsg("Failed to start a new transaction")));

    MemoryContextSwitchTo(oldContext);

    /* GXID has been received, now it's time to get a GTM timestamp */
    timestamp = GTM_TimestampGetCurrent();

#ifndef __XLOG__
    /* Backup first */
    if (GetMyConnection(myport)->standby)
    {
        bkup_begin_transaction(GetMyConnection(myport)->standby,
                txn_isolation_level, txn_read_only,
                global_sessionid,
                GetMyConnection(myport)->con_client_id, timestamp);
        /* Synch. with standby */
        if (Backup_synchronously && (myport->remote_type != GTM_NODE_GTM_PROXY))
            gtm_sync_standby(GetMyConnection(myport)->standby);
    }
#endif
    
    BeforeReplyToClientXLogTrigger();
    
    pq_beginmessage(&buf, 'S');
    pq_sendint(&buf, TXN_BEGIN_RESULT, 4);
    if (myport->remote_type == GTM_NODE_GTM_PROXY)
    {
        GTM_ProxyMsgHeader proxyhdr;
        proxyhdr.ph_conid = myport->conn_id;
        pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
    }
    pq_sendbytes(&buf, (char *)&txn, sizeof(txn));
    pq_sendbytes(&buf, (char *)&timestamp, sizeof (GTM_Timestamp));
    pq_endmessage(myport, &buf);

    if (myport->remote_type != GTM_NODE_GTM_PROXY)
    {
#ifndef __XLOG__
        /* Flush standby first */
        if (GetMyConnection(myport)->standby)
            gtmpqFlush(GetMyConnection(myport)->standby);
#endif
        pq_flush(myport);
    }
    return;
}

#ifdef __TBASE__
#ifndef __XLOG__
void
ProcessBkupGlobalTimestamp(Port *myport, StringInfo message)
{
    GlobalTimestamp timestamp;

    memcpy(&timestamp, pq_getmsgbytes(message, sizeof(GTM_Timestamp)), sizeof(GlobalTimestamp));
    pq_getmsgend(message);
    SetNextGlobalTimestamp(timestamp);
}
#endif
#endif

#ifndef __XLOG__
/*
 * Process MSG_BKUP_TXN_BEGIN message
 */
void
ProcessBkupBeginTransactionCommand(Port *myport, StringInfo message)
{
    GTM_IsolationLevel txn_isolation_level;
    bool txn_read_only;
    GTM_Timestamp timestamp;
    MemoryContext oldContext;
    uint32 client_id;
    uint32 global_sessionid_len;
    const char *global_sessionid;

    txn_isolation_level = pq_getmsgint(message, sizeof(GTM_IsolationLevel));
    txn_read_only = pq_getmsgbyte(message);
    global_sessionid_len = pq_getmsgint(message, sizeof (uint32));
    global_sessionid = pq_getmsgbytes(message, global_sessionid_len);
    client_id = pq_getmsgint(message, sizeof (uint32));
    memcpy(&timestamp, pq_getmsgbytes(message, sizeof(GTM_Timestamp)), sizeof(GTM_Timestamp));
    pq_getmsgend(message);

    oldContext = MemoryContextSwitchTo(TopMemoryContext);

    GTM_BkupBeginTransaction(txn_isolation_level, txn_read_only,
            global_sessionid,
            client_id);

    MemoryContextSwitchTo(oldContext);
}
#endif

GlobalTransactionId
GetNextGlobalTransactionId(void)
{
    GlobalTransactionId gxid;

    for(;;)
    {
        gxid = pg_atomic_fetch_add_u32(&GTMTransactions.gt_global_xid, 1);
        if(gxid >= FirstNormalGlobalTransactionId)
            break;
    }
    return gxid;

}

#ifdef __TBASE__
GlobalTimestamp
GetNextGlobalTimestamp(void)
{
    GlobalTimestamp gts, now, delta, tv_sec, tv_nsec;

    if(enable_gtm_debug)
    {
        AcquireWriteLock();
    }
    else
    {
        AcquireReadLock();
    }
    
    now = GTM_TimestampGetMonotonicRawPrecise(&tv_sec, &tv_nsec);

    if(enable_gtm_debug)
    {
        elog(DEBUG8, "Get MonotonicRaw "INT64_FORMAT" last cycle "INT64_FORMAT " global ts "INT64_FORMAT " last issue "INT64_FORMAT, 
            now,  GTMTransactions.gt_last_cycle, GTMTransactions.gt_global_timestamp, GTMTransactions.gt_last_issue_timestamp);
    }
    
    delta = now - GTMTransactions.gt_last_cycle;
    gts = GTMTransactions.gt_global_timestamp + delta;
    
    if(enable_gtm_debug)
    {
        uint64    last_access_seq =  pg_atomic_read_u64(&GTMTransactions.gt_last_access_ts_seq);
        uint64    access_seq =  pg_atomic_read_u64(&GTMTransactions.gt_access_ts_seq);

        if(gts < GTMTransactions.gt_last_issue_timestamp)
        {

            
            elog(ERROR, "Issued global timestamp turns around last " INT64_FORMAT
                            " last last cycle "INT64_FORMAT
                            " last raw time "INT64_FORMAT
                            " last base global timestamp "INT64_FORMAT
                            " last tv secs "INT64_FORMAT
                            " last tv nsecs "INT64_FORMAT
                            " last access seq "INT64_FORMAT
                            " gts " INT64_FORMAT
                            " last cycle "INT64_FORMAT 
                            " raw ts "INT64_FORMAT
                            " base global timestamp "INT64_FORMAT
                            " tv secs "INT64_FORMAT
                            " tv nsecs "INT64_FORMAT
                            " access seq "INT64_FORMAT,
                            GTMTransactions.gt_last_issue_timestamp,
                            GTMTransactions.gt_last_last_cycle,
                            GTMTransactions.gt_last_raw_timestamp,
                            GTMTransactions.gt_last_global_timestamp,
                            GTMTransactions.gt_last_tv_sec,
                            GTMTransactions.gt_last_tv_nsec,
                            last_access_seq,
                            gts,
                            GTMTransactions.gt_last_cycle, 
                            now,
                            GTMTransactions.gt_global_timestamp,
                            tv_sec,
                            tv_nsec,
                            access_seq);
            ReleaseWriteLock();
            
        }
        GTMTransactions.gt_last_issue_timestamp = gts;
        GTMTransactions.gt_last_last_cycle = GTMTransactions.gt_last_cycle;
        GTMTransactions.gt_last_raw_timestamp = now;
        GTMTransactions.gt_last_global_timestamp = GTMTransactions.gt_global_timestamp;
        GTMTransactions.gt_last_tv_sec = tv_sec;
        GTMTransactions.gt_last_tv_nsec = tv_nsec;
        pg_atomic_init_u64(&GTMTransactions.gt_last_access_ts_seq, access_seq);
        access_seq = pg_atomic_fetch_add_u64(&GTMTransactions.gt_access_ts_seq, 1);
        
    }
    
    if(enable_gtm_debug)
    {
        ReleaseWriteLock();
    }
    else
    {
        ReleaseReadLock();
    }

    if(enable_gtm_debug)
    {
        elog(LOG, "get global timestamp "INT64_FORMAT " last cycle " INT64_FORMAT " now " INT64_FORMAT, 
                            gts, GTMTransactions.gt_last_cycle, now); 
    }
    
    return gts;

}
void AcquireWriteLock(void)
{
    int i;
    
    for(i = 0; i < GTM_MAX_THREADS; i++)
    {
        while(__sync_val_compare_and_swap(&GTMTransactions.gt_in_locking[i].lock, 0, 1) == 1)
            ;
    }

}


void ReleaseWriteLock(void)
{
    int i;
    
    for(i = 0; i < GTM_MAX_THREADS; i++)
    {
        if(__sync_val_compare_and_swap(&GTMTransactions.gt_in_locking[i].lock, 1, 0) != 1)
        {
            elog(ERROR, "thread %d lock should be locked when releasing write lock", i);
        }
    }

}

void AcquireReadLock(void)
{

    while(__sync_val_compare_and_swap(&GTMTransactions.gt_in_locking[ThreadId].lock, 0, 1) == 1)
        ;
}

void ReleaseReadLock(void)
{
    
    if(__sync_val_compare_and_swap(&GTMTransactions.gt_in_locking[ThreadId].lock, 1, 0) != 1)
    {
        elog(ERROR, "thread %d lock should be locked when releasing read lock", ThreadId);
    }

}

void ReleaseReadLockNoCheck(void)
{
    
    __sync_val_compare_and_swap(&GTMTransactions.gt_in_locking[ThreadId].lock, 1, 0);

}


GlobalTimestamp
SyncGlobalTimestamp(void)
{
    GlobalTimestamp gts, now, delta;

    AcquireWriteLock();
    now = GTM_TimestampGetMonotonicRaw();

    if(enable_gtm_debug)
    {
        elog(LOG, "Get MonotonicRaw "INT64_FORMAT" last cycle "INT64_FORMAT " base global ts "INT64_FORMAT 
                    " last issue "INT64_FORMAT 
                    " last last cycle "INT64_FORMAT
                    " last raw time "INT64_FORMAT
                    " last base global timestamp "INT64_FORMAT, 
                now,  
                GTMTransactions.gt_last_cycle, 
                GTMTransactions.gt_global_timestamp, 
                GTMTransactions.gt_last_issue_timestamp,
                GTMTransactions.gt_last_last_cycle,
                GTMTransactions.gt_last_raw_timestamp,
                GTMTransactions.gt_last_global_timestamp);
    }

    delta = now - GTMTransactions.gt_last_cycle;
    GTMTransactions.gt_global_timestamp += delta;
    GTMTransactions.gt_last_cycle = now;
    gts = GTMTransactions.gt_global_timestamp;

    if(enable_gtm_debug)
    {
        elog(LOG, "syncing global timestamp "INT64_FORMAT " last cycle " INT64_FORMAT " now " INT64_FORMAT, 
                            gts, GTMTransactions.gt_last_cycle, now); 
    }
    ReleaseWriteLock();

    
    return gts;

}

void
SetNextGlobalTimestamp(GlobalTimestamp gts)
{
    AcquireWriteLock();
    GTMTransactions.gt_global_timestamp = gts;
    GTMTransactions.gt_last_cycle = GTM_TimestampGetMonotonicRaw();
    GTMTransactions.gt_last_issue_timestamp = gts - 1;
    ReleaseWriteLock();
    
    elog(DEBUG8, "set next global timestamp "INT64_FORMAT " last cycle " INT64_FORMAT, gts, GTMTransactions.gt_last_cycle);
    return;
}

/*
 * Add for global timestamp; Process MSG_GETGTS message
 */
void
ProcessGetGTSCommand(Port *myport, StringInfo message)
{// #lizard forgives
    StringInfoData buf;
    GTM_Timestamp timestamp;
#ifdef __XLOG__
    time_t        now;
#endif
    
    pq_getmsgend(message);    
    
    if (Recovery_IsStandby())
    {
        if (myport->remote_type != GTM_NODE_GTM_CTL && myport->remote_type != GTM_NODE_GTM)
        {
            elog(ERROR, "gtm standby can't provide global timestamp.");
        }
    }

    /* Get a GTM timestamp */
    timestamp = GetNextGlobalTimestamp();
#ifdef __XLOG__
    now       = GTM_TimestampGetMonotonicRaw();

    if(now - GetMyThreadInfo->last_sync_gts > GTM_SYNC_TIME_LIMIT)
    {
        SpinLockAcquire(&g_last_sync_gts_lock);
        GetMyThreadInfo->last_sync_gts = g_last_sync_gts;
        SpinLockRelease(&g_last_sync_gts_lock);

        /* 
         * check local last_sync_gts if updated ,if not ignore.
         */
        if(GetMyThreadInfo->last_sync_gts != 0 && now - GetMyThreadInfo->last_sync_gts > GTM_SYNC_TIME_LIMIT)
            elog(ERROR,"sync time exceeded last:%lu now:%lu",GetMyThreadInfo->last_sync_gts,now);
    }
#endif
        
    /* Respond to the client */
    pq_beginmessage(&buf, 'S');
    pq_sendint(&buf, TXN_BEGIN_GETGTS_RESULT, 4);
    if (myport->remote_type == GTM_NODE_GTM_PROXY)
    {
        GTM_ProxyMsgHeader proxyhdr;
        proxyhdr.ph_conid = myport->conn_id;
        pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
    }
    pq_sendbytes(&buf, (char *) &timestamp, sizeof(GTM_Timestamp));
    if (GTMClusterReadOnly)
    {
        pq_sendbyte(&buf, true);
    }
    pq_endmessage(myport, &buf);

    if (myport->remote_type != GTM_NODE_GTM_PROXY)
    {
        pq_flush(myport);
    }
}

/*
 * Add for global timestamp; Process MSG_GETGTS_MULTI message
 */
void
ProcessGetGTSCommandMulti(Port *myport, StringInfo message)
{
    
    StringInfoData buf;
    GTM_Timestamp timestamp;
    int gts_count;

    if (Recovery_IsStandby())
    {
        if (myport->remote_type != GTM_NODE_GTM_CTL && myport->remote_type != GTM_NODE_GTM)
        {
            elog(ERROR, "gtm standby can't provide global timestamp.");
        }
    }
    
    gts_count = pq_getmsgint(message, sizeof (int));

    if (gts_count <= 0)
        elog(PANIC, "Zero or less transaction count");

    /* Get a GTM timestamp */
    timestamp = GTM_TimestampGetCurrent();

    elog(DEBUG7, "GTM processes timestamp.\n");
    
    BeforeReplyToClientXLogTrigger();
    
    /* Respond to the client */
    pq_beginmessage(&buf, 'S');
    pq_sendint(&buf, TXN_BEGIN_GETGTS_MULTI_RESULT, 4);
    if (myport->remote_type == GTM_NODE_GTM_PROXY)
    {
        GTM_ProxyMsgHeader proxyhdr;
        proxyhdr.ph_conid = myport->conn_id;
        pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
    }
    pq_sendbytes(&buf, (char *)&timestamp, sizeof(GTM_Timestamp));
    pq_endmessage(myport, &buf);

    if (myport->remote_type != GTM_NODE_GTM_PROXY)
    {
    
        pq_flush(myport);
    }

    return;
}

/*
 * Check gtm master and slave status by acquiring gts from both master and slave nodes.
 */
void
ProcessCheckGTMCommand(Port *myport, StringInfo message)
{
    StringInfoData buf;
    int            is_master = 0;
    GTM_Timestamp  master_timestamp  = InvalidGTS;
    int               len       = 0;
    int            i;
    int            standby_count = 0;
    XLogRecPtr     flush_ptr;
    int            is_sync_mode[max_wal_sender];
    char           slave_application_name[max_wal_sender][MAXFNAMELEN];
    XLogRecPtr     slave_xlog_flush_pos[max_wal_sender];

    /* read timeout message */
    pq_getmsgint(message,sizeof(int));
    pq_getmsgend(message);
    
    if (myport->remote_type != GTM_NODE_GTM_CTL)
    {
        /* standby node only handle GTS request from gtm_ctl*/
        elog(ERROR, "check gtm command is supposed to be fired only by gtm or gtm_ctl!!");
    }    
    
    /* Get a GTM timestamp */
    master_timestamp = GetNextGlobalTimestamp();
    
    /* Respond to the client */
    is_master = Recovery_IsStandby();
    
    BeforeReplyToClientXLogTrigger();

    pq_beginmessage(&buf, 'S');
    pq_sendint(&buf, TXN_CHECK_GTM_STATUS_RESULT, 4);

    flush_ptr = GetCurrentXLogwrtResult().Flush;

    pq_sendbytes(&buf, (char *) &is_master, sizeof(is_master));
    pq_sendbytes(&buf, (char *) &master_timestamp, sizeof(GTM_Timestamp));
    pq_sendint64(&buf, flush_ptr);

    standby_count = 0;

    for(i = 0 ; i < max_wal_sender ;i++)
    {
        GTM_MutexLockAcquire(&g_StandbyReplication[i].lock);

        if(g_StandbyReplication[i].is_use == false)
        {
            GTM_MutexLockRelease(&g_StandbyReplication[i].lock);
            continue;
        }

        is_sync_mode[standby_count] = g_StandbyReplication[i].is_sync == true ? 1 : 0;

        memcpy(slave_application_name[standby_count],g_StandbyReplication[i].application_name,MAXFNAMELEN);

        slave_xlog_flush_pos[standby_count] = g_StandbyReplication[i].flush_ptr;

        GTM_MutexLockRelease(&g_StandbyReplication[i].lock);

        standby_count++;
    }

    pq_sendint(&buf, standby_count, sizeof(int));

    for(i = 0; i < standby_count;i++)
    {
        GTM_Timestamp timestamp = master_timestamp + GTM_GTS_ONE_SECOND * GTMStartupGTSDelta;
        
        len = strlen(slave_application_name[i]) + 1;

        pq_sendint(&buf, is_sync_mode[i],sizeof(int));
        pq_sendint(&buf, len , sizeof(int));
        pq_sendbytes(&buf, slave_application_name[i], len);
        pq_sendbytes(&buf, (char *) &timestamp, sizeof(GTM_Timestamp));
        pq_sendint64(&buf, slave_xlog_flush_pos[i]);
    }

    pq_endmessage(myport, &buf);
    pq_flush(myport);
}


/*
 * Check gtm slave status by acquiring gts.
 */
void
ProcessStandbyPreCheckGTMCommand(Port *myport, StringInfo message)
{
    StringInfoData buf;
    int            is_master = 0;
    GTM_Timestamp  master_timestamp  = InvalidGTS;
    int            standby_count = 0;
    XLogRecPtr     flush_ptr;

    /* read timeout message */
    pq_getmsgint(message,sizeof(int));
    pq_getmsgend(message);

    if (myport->remote_type != GTM_NODE_GTM_CTL)
    {
        /* standby node only handle GTS request from gtm_ctl*/
        elog(ERROR, "check gtm command is supposed to be fired only by gtm or gtm_ctl!!");
    }

    /* get static gts from ControlData */
    GTM_RWLockAcquire(&ControlDataLock,GTM_LOCKMODE_WRITE);
    master_timestamp = ControlData->gts;
    GTM_RWLockRelease(&ControlDataLock);

    flush_ptr = GetCurrentXLogwrtResult().Flush;
    is_master = Recovery_IsStandby();

    pq_beginmessage(&buf, 'S');
    pq_sendint(&buf, TXN_CHECK_GTM_STATUS_RESULT, 4);
    pq_sendbytes(&buf, (char *) &is_master, sizeof(is_master));
    pq_sendbytes(&buf, (char *) &master_timestamp, sizeof(GTM_Timestamp));
    pq_sendint64(&buf, flush_ptr);
    pq_sendint(&buf, standby_count, sizeof(int));

    pq_endmessage(myport, &buf);
    pq_flush(myport);
}
#endif 
/*
 * Process MSG_TXN_BEGIN_GETGXID message
 */
void
ProcessBeginTransactionGetGXIDCommand(Port *myport, StringInfo message)
{// #lizard forgives
    StringInfoData buf;
    GlobalTransactionId gxid;
    GTM_Timestamp timestamp;
    MemoryContext oldContext;
    uint32 global_sessionid_len;

#ifndef __XLOG__
    GTM_IsolationLevel txn_isolation_level;
    bool txn_read_only;
    const char *global_sessionid;

    txn_isolation_level = pq_getmsgint(message, sizeof (GTM_IsolationLevel));
    txn_read_only = pq_getmsgbyte(message);
    global_sessionid_len = pq_getmsgint(message, sizeof (uint32));
    global_sessionid = pq_getmsgbytes(message, global_sessionid_len);
#else
    pq_getmsgint(message, sizeof (GTM_IsolationLevel));
    pq_getmsgbyte(message);
    global_sessionid_len = pq_getmsgint(message, sizeof (uint32));
    pq_getmsgbytes(message, global_sessionid_len);
#endif


    oldContext = MemoryContextSwitchTo(TopMemoryContext);

    /* GXID has been received, now it's time to get a GTM timestamp */
    timestamp = GTM_TimestampGetCurrent();

    /*
     * Start a new transaction
     */
#if 0
    txn = GTM_BeginTransaction(txn_isolation_level, txn_read_only,
            global_sessionid);
    if (txn == InvalidTransactionHandle)
        ereport(ERROR,
                (EINVAL,
                 errmsg("Failed to start a new transaction")));

    gxid = GTM_GetGlobalTransactionId(txn);
    if (gxid == InvalidGlobalTransactionId)
        ereport(ERROR,
                (EINVAL,
                 errmsg("Failed to get a new transaction id")));
#endif
    gxid = GetNextGlobalTransactionId();
    MemoryContextSwitchTo(oldContext);

    elog(DEBUG6, "Sending transaction id %u", gxid);

#ifndef __XLOG__
    /* Backup first */
    if (GetMyConnection(myport)->standby)
    {
        GTM_Conn *oldconn = GetMyConnection(myport)->standby;
        int count = 0;

        elog(DEBUG1, "calling begin_transaction() for standby GTM %p.", GetMyConnection(myport)->standby);

retry:
        bkup_begin_transaction_gxid(GetMyConnection(myport)->standby,
                                    gxid, txn_isolation_level,
                                    txn_read_only,
                                    global_sessionid,
                                    GetMyConnection(myport)->con_client_id,
                                    timestamp);

        if (gtm_standby_check_communication_error(myport, &count, oldconn))
            goto retry;

        /* Sync */
        if (Backup_synchronously && (myport->remote_type != GTM_NODE_GTM_PROXY))
            gtm_sync_standby(GetMyConnection(myport)->standby);

    }
#endif
    
    BeforeReplyToClientXLogTrigger();
    
    /* Respond to the client */
    pq_beginmessage(&buf, 'S');
    pq_sendint(&buf, TXN_BEGIN_GETGXID_RESULT, 4);
    if (myport->remote_type == GTM_NODE_GTM_PROXY)
    {
        GTM_ProxyMsgHeader proxyhdr;
        proxyhdr.ph_conid = myport->conn_id;
        pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
    }
    pq_sendbytes(&buf, (char *)&gxid, sizeof(gxid));
    pq_sendbytes(&buf, (char *)&timestamp, sizeof (GTM_Timestamp));
    pq_endmessage(myport, &buf);

    if (myport->remote_type != GTM_NODE_GTM_PROXY)
    {
#ifndef __XLOG__
        /* Flush standby */
        if (GetMyConnection(myport)->standby)
            gtmpqFlush(GetMyConnection(myport)->standby);
#endif
        pq_flush(myport);
    }


    return;
}

#ifndef __XLOG__
static void
GTM_BkupBeginTransactionGetGXIDMulti(GlobalTransactionId *gxid,
                                     GTM_IsolationLevel *isolevel,
                                     bool *readonly,
                                     const char **global_sessionid,
                                     uint32 *client_id,
                                     GTMProxy_ConnID *connid,
                                     int txn_count)
{// #lizard forgives
    GTM_TransactionHandle txn[GTM_MAX_GLOBAL_TRANSACTIONS];
    GTM_TransactionInfo *gtm_txninfo;
    int ii;
    int count;
    MemoryContext oldContext;

    GlobalTransactionId xid = InvalidGlobalTransactionId;

    oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

    count = GTM_BeginTransactionMulti(isolevel, readonly, global_sessionid,
                                      connid, txn_count, txn);
    if (count != txn_count)
        ereport(ERROR,
                (EINVAL,
                 errmsg("Failed to start %d new transactions", txn_count)));

    elog(DEBUG2, "GTM_BkupBeginTransactionGetGXIDMulti - count %d", count);

    //XCPTODO check oldContext = MemoryContextSwitchTo(TopMemoryContext);
    GTM_RWLockAcquire(&GTMTransactions.gt_TransArrayLock, GTM_LOCKMODE_WRITE);

    for (ii = 0; ii < txn_count; ii++)
    {
        gtm_txninfo = GTM_HandleToTransactionInfo(txn[ii]);
        gtm_txninfo->gti_gxid = gxid[ii];
        if (global_sessionid[ii])
            strncpy(gtm_txninfo->gti_global_session_id, global_sessionid[ii],
                    GTM_MAX_SESSION_ID_LEN);

        elog(DEBUG2, "GTM_BkupBeginTransactionGetGXIDMulti: xid(%u), handle(%u)",
                gxid[ii], txn[ii]);

        /*
         * Advance next gxid -- because this is called at slave only, we don't care the restoration point
         * here.  Restoration point will be created at promotion.
         */
        if (GlobalTransactionIdPrecedesOrEquals(GTMTransactions.gt_nextXid, gxid[ii]))
            GTMTransactions.gt_nextXid = gxid[ii] + 1;
        if (!GlobalTransactionIdIsValid(GTMTransactions.gt_nextXid))    /* Handle wrap around too */
            GTMTransactions.gt_nextXid = FirstNormalGlobalTransactionId;
        xid = GTMTransactions.gt_nextXid;
    }

    /* Periodically write the xid and sequence info out to the control file.
     * Try and handle wrapping, too.
     */
    if (GlobalTransactionIdIsValid(xid) &&
            (xid - ControlXid > CONTROL_INTERVAL || xid < ControlXid))
    {
#ifdef __TBASE__
        int32 ret;
        ret =  GTM_StoreReserveXid(CONTROL_INTERVAL);
        if (ret)
        {
            GTM_RWLockRelease(&GTMTransactions.gt_XidGenLock);
            elog(LOG, "GTM_GetGlobalTransactionIdMulti reserved gxid failed");
            return;
        }
#endif
        ControlXid = xid;
    }

    GTM_RWLockRelease(&GTMTransactions.gt_TransArrayLock);

    MemoryContextSwitchTo(oldContext);
}
#endif

#ifndef __XLOG__
static void
GTM_BkupBeginTransactionGetGXID(GlobalTransactionId gxid,
                                GTM_IsolationLevel isolevel,
                                bool readonly,
                                const char *global_sessionid,
                                uint32 client_id)
{
    GTMProxy_ConnID connid = -1;
#ifdef __TBASE__            
    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_BkupBeginTransactionGetGXID gxid:%u.", gxid);
    }
#endif

    GTM_BkupBeginTransactionGetGXIDMulti(&gxid, &isolevel,
            &readonly, &global_sessionid, &client_id, &connid, 1);
}
#endif

#ifndef __XLOG__
/*
 * Process MSG_BKUP_TXN_BEGIN_GETGXID message
 */
void
ProcessBkupBeginTransactionGetGXIDCommand(Port *myport, StringInfo message)
{
    GlobalTransactionId gxid;
    GTM_IsolationLevel txn_isolation_level;
    bool txn_read_only;
    uint32 txn_client_id;
    GTM_Timestamp timestamp;
    uint32 txn_global_sessionid_len;
    const char *txn_global_sessionid;

    gxid = pq_getmsgint(message, sizeof(GlobalTransactionId));
    txn_isolation_level = pq_getmsgint(message, sizeof(GTM_IsolationLevel));
    txn_read_only = pq_getmsgbyte(message);
    txn_global_sessionid_len = pq_getmsgint(message, sizeof (uint32));
    txn_global_sessionid = pq_getmsgbytes(message,
            txn_global_sessionid_len);
    txn_client_id = pq_getmsgint(message, sizeof (uint32));
    memcpy(&timestamp, pq_getmsgbytes(message, sizeof(GTM_Timestamp)), sizeof(GTM_Timestamp));
    pq_getmsgend(message);

    GTM_BkupBeginTransactionGetGXID(gxid, txn_isolation_level,
            txn_read_only, txn_global_sessionid, txn_client_id);
}
#endif

#ifndef __XLOG__
/*
 * Process MSG_BKUP_TXN_BEGIN_GETGXID_AUTOVACUUM message
 */
void
ProcessBkupBeginTransactionGetGXIDAutovacuumCommand(Port *myport, StringInfo message)
{
    GlobalTransactionId gxid;
    GTM_IsolationLevel txn_isolation_level;
    uint32 txn_client_id;

    gxid = pq_getmsgint(message, sizeof(GlobalTransactionId));
    txn_isolation_level = pq_getmsgint(message, sizeof(GTM_IsolationLevel));
    txn_client_id = pq_getmsgint(message, sizeof (uint32));
    pq_getmsgend(message);

    GTM_BkupBeginTransactionGetGXID(gxid, txn_isolation_level,
            false, NULL, txn_client_id);
    GTM_SetDoVacuum(GTM_GXIDToHandle(gxid));
}

#endif

/*
 * Process MSG_TXN_BEGIN_GETGXID_AUTOVACUUM message
 */
void
ProcessBeginTransactionGetGXIDAutovacuumCommand(Port *myport, StringInfo message)
{// #lizard forgives
    bool txn_read_only PG_USED_FOR_ASSERTS_ONLY;
    StringInfoData buf;
    GlobalTransactionId gxid;
    MemoryContext oldContext;

#ifndef __XLOG__
    GTM_IsolationLevel txn_isolation_level;

    txn_isolation_level = pq_getmsgint(message, sizeof (GTM_IsolationLevel));
    txn_read_only = pq_getmsgbyte(message);
#else
    pq_getmsgint(message, sizeof (GTM_IsolationLevel));
    pq_getmsgbyte(message);
#endif


    oldContext = MemoryContextSwitchTo(TopMemoryContext);

    /*
     * Start a new transaction
     */
#if 0
    txn = GTM_BeginTransaction(txn_isolation_level, txn_read_only, NULL);
    if (txn == InvalidTransactionHandle)
        ereport(ERROR,
                (EINVAL,
                 errmsg("Failed to start a new transaction")));

    gxid = GTM_GetGlobalTransactionId(txn);
    if (gxid == InvalidGlobalTransactionId)
        ereport(ERROR,
                (EINVAL,
                 errmsg("Failed to get a new transaction id")));

    /* Indicate that it is for autovacuum */
#endif
    gxid = GetNextGlobalTransactionId();
    //GTM_SetDoVacuum(txn);

    MemoryContextSwitchTo(oldContext);

#ifndef __XLOG__
    /* Backup first */
    if (GetMyConnection(myport)->standby)
    {
        GlobalTransactionId _gxid;
        GTM_Conn *oldconn = GetMyConnection(myport)->standby;
        int count = 0;

        elog(DEBUG1, "calling begin_transaction_autovacuum() for standby GTM %p.",
            GetMyConnection(myport)->standby);

    retry:
        _gxid = bkup_begin_transaction_autovacuum(GetMyConnection(myport)->standby,
                                                  gxid,
                                                  txn_isolation_level,
                                                  GetMyConnection(myport)->con_client_id);

        if (gtm_standby_check_communication_error(myport, &count, oldconn))
            goto retry;

        /* Sync */
        if (Backup_synchronously && (myport->remote_type != GTM_NODE_GTM_PROXY))
            gtm_sync_standby(GetMyConnection(myport)->standby);

        elog(DEBUG1, "begin_transaction_autovacuum() GXID=%d done.", _gxid);
    }

#endif
    
    BeforeReplyToClientXLogTrigger();
    
    /* Respond to the client */
    pq_beginmessage(&buf, 'S');
    pq_sendint(&buf, TXN_BEGIN_GETGXID_AUTOVACUUM_RESULT, 4);
    if (myport->remote_type == GTM_NODE_GTM_PROXY)
    {
        GTM_ProxyMsgHeader proxyhdr;
        proxyhdr.ph_conid = myport->conn_id;
        pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
    }
    pq_sendbytes(&buf, (char *)&gxid, sizeof(gxid));
    pq_endmessage(myport, &buf);

    if (myport->remote_type != GTM_NODE_GTM_PROXY)
    {
#ifndef __XLOG__
        /* Flush standby */
        if (GetMyConnection(myport)->standby)
            gtmpqFlush(GetMyConnection(myport)->standby);
#endif
        pq_flush(myport);
    }

    return;
}

/*
 * Process MSG_TXN_BEGIN_GETGXID_MULTI message
 */
void
ProcessBeginTransactionGetGXIDCommandMulti(Port *myport, StringInfo message)
{// #lizard forgives
    uint32 txn_global_sessionid_len;
    int txn_count;
    GlobalTransactionId txn_gxid[GTM_MAX_GLOBAL_TRANSACTIONS];
    StringInfoData buf;
    GTM_Timestamp timestamp;
    MemoryContext oldContext;
    int ii;

#ifndef __XLOG__
    bool txn_read_only[GTM_MAX_GLOBAL_TRANSACTIONS];
    GTM_IsolationLevel txn_isolation_level[GTM_MAX_GLOBAL_TRANSACTIONS];
    const char *txn_global_sessionid[GTM_MAX_GLOBAL_TRANSACTIONS];
    GTMProxy_ConnID txn_connid[GTM_MAX_GLOBAL_TRANSACTIONS];
    uint32 txn_client_id[GTM_MAX_GLOBAL_TRANSACTIONS];

#endif

    txn_count = pq_getmsgint(message, sizeof (int));

    if (txn_count <= 0)
        elog(PANIC, "Zero or less transaction count");

    for (ii = 0; ii < txn_count; ii++)
    {
#ifndef __XLOG__
        txn_isolation_level[ii] = pq_getmsgint(message, sizeof (GTM_IsolationLevel));
        txn_read_only[ii] = pq_getmsgbyte(message);
        txn_global_sessionid_len = pq_getmsgint(message, sizeof (uint32));
        txn_global_sessionid[ii] = pq_getmsgbytes(message,
                txn_global_sessionid_len);
        txn_connid[ii] = pq_getmsgint(message, sizeof (GTMProxy_ConnID));
        txn_client_id[ii] = GetMyConnection(myport)->con_client_id;
#else
        pq_getmsgint(message, sizeof (GTM_IsolationLevel));
        pq_getmsgbyte(message);
        txn_global_sessionid_len = pq_getmsgint(message, sizeof (uint32));
        pq_getmsgbytes(message, txn_global_sessionid_len);
        pq_getmsgint(message, sizeof (GTMProxy_ConnID));
#endif
    }

    oldContext = MemoryContextSwitchTo(TopMemoryContext);

    /*
     * Start a new transaction
     *
     * XXX Port should contain Coordinator name - replace NULL with that
     */
#if 0
    count = GTM_BeginTransactionMulti(txn_isolation_level, txn_read_only,
                                      txn_global_sessionid, txn_connid,
                                      txn_count, txn);
    if (count != txn_count)
        ereport(ERROR,
                (EINVAL,
                 errmsg("Failed to start %d new transactions", txn_count)));

    if (!GTM_GetGlobalTransactionIdMulti(txn, txn_count, txn_gxid, new_txn,
            &new_txn_count))
        elog(ERROR, "Failed to get global transaction identifiers");
#endif
    
    
    for (ii = 0; ii < txn_count; ii++)
    {
        txn_gxid[ii] = GetNextGlobalTransactionId();
    }
    MemoryContextSwitchTo(oldContext);

    /* GXID has been received, now it's time to get a GTM timestamp */
    timestamp = GTM_TimestampGetCurrent();

#ifndef __XLOG__
    /* Backup first */
    if (GetMyConnection(myport)->standby)
    {
        int _rc;
        GTM_Conn *oldconn = GetMyConnection(myport)->standby;
        int count = 0;

        elog(DEBUG1, "calling begin_transaction_multi() for standby GTM %p.",
             GetMyConnection(myport)->standby);

retry:
        _rc = bkup_begin_transaction_multi(GetMyConnection(myport)->standby,
                                           txn_count,
                                           txn_gxid,
                                           txn_isolation_level,
                                           txn_read_only,
                                           txn_global_sessionid,
                                           txn_client_id,
                                           txn_connid);

        if (gtm_standby_check_communication_error(myport, &count, oldconn))
            goto retry;

        /* Sync */
        if (Backup_synchronously && (myport->remote_type != GTM_NODE_GTM_PROXY))
            gtm_sync_standby(GetMyConnection(myport)->standby);

        elog(DEBUG1, "begin_transaction_multi() rc=%d done.", _rc);
    }
#endif
    
    BeforeReplyToClientXLogTrigger();
    
    /* Respond to the client */
    pq_beginmessage(&buf, 'S');
    pq_sendint(&buf, TXN_BEGIN_GETGXID_MULTI_RESULT, 4);
    if (myport->remote_type == GTM_NODE_GTM_PROXY)
    {
        GTM_ProxyMsgHeader proxyhdr;
        proxyhdr.ph_conid = myport->conn_id;
        pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
    }
    pq_sendbytes(&buf, (char *)&txn_count, sizeof(txn_count));
    pq_sendbytes(&buf, (char *)txn_gxid, sizeof(GlobalTransactionId) * txn_count);
    pq_sendbytes(&buf, (char *)&(timestamp), sizeof (GTM_Timestamp));
    pq_endmessage(myport, &buf);

    if (myport->remote_type != GTM_NODE_GTM_PROXY)
    {
#ifndef __XLOG__
        /* Flush standby */
        if (GetMyConnection(myport)->standby)
            gtmpqFlush(GetMyConnection(myport)->standby);
#endif
        pq_flush(myport);
    }

    return;
}

#ifndef __XLOG__
/*
 * Process MSG_BKUP_BEGIN_TXN_GETGXID_MULTI message
 */
void
ProcessBkupBeginTransactionGetGXIDCommandMulti(Port *myport, StringInfo message)
{
    int txn_count;
    GlobalTransactionId gxid[GTM_MAX_GLOBAL_TRANSACTIONS];
    GTM_IsolationLevel txn_isolation_level[GTM_MAX_GLOBAL_TRANSACTIONS];
    bool txn_read_only[GTM_MAX_GLOBAL_TRANSACTIONS];
    uint32 txn_global_sessionid_len;
    const char *txn_global_sessionid[GTM_MAX_GLOBAL_TRANSACTIONS];
    GTMProxy_ConnID txn_connid[GTM_MAX_GLOBAL_TRANSACTIONS];
    uint32 txn_client_id[GTM_MAX_GLOBAL_TRANSACTIONS];
    int ii;

    txn_count = pq_getmsgint(message, sizeof(int));
    if (txn_count <= 0)
        elog(PANIC, "Zero or less transaction count.");

    for (ii = 0; ii < txn_count; ii++)
    {
        gxid[ii] = pq_getmsgint(message, sizeof(GlobalTransactionId));
        txn_isolation_level[ii] = pq_getmsgint(message, sizeof(GTM_IsolationLevel));
        txn_read_only[ii] = pq_getmsgbyte(message);
        txn_global_sessionid_len = pq_getmsgint(message, sizeof (uint32));
        txn_global_sessionid[ii] = pq_getmsgbytes(message,
                txn_global_sessionid_len);
        txn_client_id[ii] = pq_getmsgint(message, sizeof(uint32));
        txn_connid[ii] = pq_getmsgint(message, sizeof(GTMProxy_ConnID));
    }

    GTM_BkupBeginTransactionGetGXIDMulti(gxid, txn_isolation_level,
            txn_read_only, txn_global_sessionid,
            txn_client_id, txn_connid, txn_count);

}

#endif
/*
 * Process MSG_TXN_COMMIT/MSG_BKUP_TXN_COMMIT message
 *
 * is_backup indicates the message is MSG_BKUP_TXN_COMMIT
 */
void
ProcessCommitTransactionCommand(Port *myport, StringInfo message, bool is_backup)
{// #lizard forgives
    StringInfoData buf;
    GTM_TransactionHandle txn;
    GlobalTransactionId gxid;
    MemoryContext oldContext;
    int status = STATUS_OK;
    int waited_xid_count;
    GlobalTransactionId *waited_xids = NULL;

    const char *data = pq_getmsgbytes(message, sizeof (gxid));

    if (data == NULL)
        ereport(ERROR,
                (EPROTO,
                 errmsg("Message does not contain valid GXID")));
    memcpy(&gxid, data, sizeof (gxid));
    txn = GTM_GXIDToHandle(gxid);

    waited_xid_count = pq_getmsgint(message, sizeof (int));
    if (waited_xid_count > 0)
    {
        waited_xids = (GlobalTransactionId *) pq_getmsgbytes(message,
                waited_xid_count * sizeof (GlobalTransactionId));
    }

    pq_getmsgend(message);

    oldContext = MemoryContextSwitchTo(TopMemoryContext);

    /*
     * Commit the transaction
     */
    status = GTM_CommitTransaction(txn, waited_xid_count, waited_xids);

    MemoryContextSwitchTo(oldContext);

    if(!is_backup)
    {
#ifndef __XLOG__
        /*
         * If the transaction is successfully committed on the GTM master then
         * send a backup message to the GTM slave to redo the action locally
         */
        if ((GetMyConnection(myport)->standby) && (status == STATUS_OK))
        {
            /*
             * Backup first
             */
            int _rc;
            GTM_Conn *oldconn = GetMyConnection(myport)->standby;
            int count = 0;

            elog(DEBUG1, "calling commit_transaction() for standby GTM %p.", GetMyConnection(myport)->standby);

        retry:
            _rc = bkup_commit_transaction(GetMyConnection(myport)->standby, gxid);

            if (gtm_standby_check_communication_error(myport, &count, oldconn))
                goto retry;

            /* Sync */
            if (Backup_synchronously && (myport->remote_type != GTM_NODE_GTM_PROXY))
                gtm_sync_standby(GetMyConnection(myport)->standby);

            elog(DEBUG1, "commit_transaction() rc=%d done.", _rc);
        }
#endif
        
        BeforeReplyToClientXLogTrigger();

        pq_beginmessage(&buf, 'S');
        pq_sendint(&buf, TXN_COMMIT_RESULT, 4);
        if (myport->remote_type == GTM_NODE_GTM_PROXY)
        {
            GTM_ProxyMsgHeader proxyhdr;
            proxyhdr.ph_conid = myport->conn_id;
            pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
        }
        pq_sendbytes(&buf, (char *)&gxid, sizeof(gxid));
        pq_sendbytes(&buf, (char *)&status, sizeof(status));
        pq_endmessage(myport, &buf);

        if (myport->remote_type != GTM_NODE_GTM_PROXY)
        {
#ifndef __XLOG__
            /* Flush standby */
            if (GetMyConnection(myport)->standby)
                gtmpqFlush(GetMyConnection(myport)->standby);
#endif
            pq_flush(myport);
        }

    }
    else 
        BeforeReplyToClientXLogTrigger();
    return;
}

/*
 * Process MSG_TXN_COMMIT_PREPARED/MSG_BKUP_TXN_COMMIT_PREPARED message
 * Commit a prepared transaction
 * Here the GXID used for PREPARE and COMMIT PREPARED are both committed
 *
 * is_backup indicates the message is MSG_BKUP_TXN_COMMIT_PREPARED
 */
void
ProcessCommitPreparedTransactionCommand(Port *myport, StringInfo message, bool is_backup)
{// #lizard forgives
    StringInfoData buf;
    int    txn_count = 2; /* PREPARE and COMMIT PREPARED gxid's */
    GTM_TransactionHandle txn[txn_count];
    GlobalTransactionId gxid[txn_count];
    MemoryContext oldContext;
    int status[txn_count];
    int ii;
    int waited_xid_count;
    GlobalTransactionId *waited_xids PG_USED_FOR_ASSERTS_ONLY = NULL;

    for (ii = 0; ii < txn_count; ii++)
    {
        const char *data = pq_getmsgbytes(message, sizeof (gxid[ii]));
        if (data == NULL)
            ereport(ERROR,
                    (EPROTO,
                     errmsg("Message does not contain valid GXID")));
        memcpy(&gxid[ii], data, sizeof (gxid[ii]));
        txn[ii] = GTM_GXIDToHandle(gxid[ii]);
        elog(DEBUG1, "ProcessCommitTransactionCommandMulti: gxid(%u), handle(%u)", gxid[ii], txn[ii]);
    }

    waited_xid_count = pq_getmsgint(message, sizeof (int));
    if (waited_xid_count > 0)
    {
        waited_xids = (GlobalTransactionId *) pq_getmsgbytes(message,
                waited_xid_count * sizeof (GlobalTransactionId));
    }

    pq_getmsgend(message);

    oldContext = MemoryContextSwitchTo(TopMemoryContext);

    elog(DEBUG1, "Committing: prepared id %u and commit prepared id %u ", gxid[0], gxid[1]);

    /*
     * Commit the prepared transaction.
     */
#if 0
    GTM_CommitTransactionMulti(txn, txn_count, waited_xid_count,
            waited_xids, status);
#endif
    MemoryContextSwitchTo(oldContext);

    if (!is_backup)
    {

#ifndef __XLOG__
        /*
         * If we successfully committed the transaction on the GTM master, then
         * also send a backup message to the GTM slave to redo the action
         * locally
         *
         * GTM_CommitTransactionMulti() above is used to only commit the main
         * and the auxiliary GXID. Since we either commit or delay both of
         * these GXIDs together, its enough to just test for one of the GXIDs.
         * If the transaction commit is delayed, the backup message will be
         * sent when the GTM master receives COMMIT message again and
         * successfully commits the transaction
         */
        if ((GetMyConnection(myport)->standby) && (status[0] == STATUS_OK))
        {
            /* Backup first */
            int _rc;
            GTM_Conn *oldconn = GetMyConnection(myport)->standby;
            int count = 0;

            elog(DEBUG1, "calling commit_prepared_transaction() for standby GTM %p.",
                 GetMyConnection(myport)->standby);

        retry:
            _rc = bkup_commit_prepared_transaction(GetMyConnection(myport)->standby,
                                                   gxid[0], gxid[1] /* prepared GXID */);

            if (gtm_standby_check_communication_error(myport, &count, oldconn))
                goto retry;

            /* Sync */
            if (Backup_synchronously && (myport->remote_type != GTM_NODE_GTM_PROXY))
                gtm_sync_standby(GetMyConnection(myport)->standby);

            elog(DEBUG1, "commit_prepared_transaction() rc=%d done.", _rc);
        }
#endif
        
        BeforeReplyToClientXLogTrigger();
        
        /* Respond to the client */
        pq_beginmessage(&buf, 'S');
        pq_sendint(&buf, TXN_COMMIT_PREPARED_RESULT, 4);
        if (myport->remote_type == GTM_NODE_GTM_PROXY)
        {
            GTM_ProxyMsgHeader proxyhdr;
            proxyhdr.ph_conid = myport->conn_id;
            pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
        }
        pq_sendbytes(&buf, (char *)&gxid[0], sizeof(GlobalTransactionId));
        pq_sendbytes(&buf, (char *)&status[0], 4);
        pq_endmessage(myport, &buf);

        if (myport->remote_type != GTM_NODE_GTM_PROXY)
        {
#ifndef __XLOG__
            /* Flush standby */
            if (GetMyConnection(myport)->standby)
                gtmpqFlush(GetMyConnection(myport)->standby);
#endif
            pq_flush(myport);
        }

    }

    return;
}


/*
 * Process MSG_TXN_GET_GID_DATA
 * This message is used after at the beginning of a COMMIT PREPARED
 * or a ROLLBACK PREPARED.
 * For a given GID the following info is returned:
 * - a fresh GXID,
 * - GXID of the transaction that made the prepare
 * - Datanode and Coordinator node list involved in the prepare
 */
void
ProcessGetGIDDataTransactionCommand(Port *myport, StringInfo message)
{// #lizard forgives
    StringInfoData buf;    
#ifndef __TBASE__
    GTM_TransactionHandle txn, prepared_txn;
#endif
    GlobalTransactionId gxid;
    /* Data to be sent back to client */
    char *gid;
    char *nodestring = NULL;
    GlobalTransactionId prepared_gxid;
    GTMStorageHandle    store_txn_handle;        
    int gidlen;
    bool txn_read_only;

#ifndef __XLOG__
    GTM_IsolationLevel txn_isolation_level;
    /* take the isolation level and read_only instructions */
    txn_isolation_level = pq_getmsgint(message, sizeof (GTM_IsolationLevel));
#else
    pq_getmsgint(message, sizeof (GTM_IsolationLevel));
#endif

    txn_read_only = pq_getmsgbyte(message);

    /* receive GID */
    gidlen = pq_getmsgint(message, sizeof (GTM_StrLen));
    gid = (char *) palloc(gidlen + 1);
    memcpy(gid, (char *)pq_getmsgbytes(message, gidlen), gidlen);
    gid[gidlen] = '\0';

    pq_getmsgend(message);

    /* Get the prepared Transaction for given GID */
#ifdef __TBASE__
    if (Recovery_IsStandby())
    {
        if (myport->remote_type != GTM_NODE_GTM)
        {
            elog(ERROR, "gtm standby can't provide 2PC transaction to datanodes or coordinators.");
        }
    }

    /*We refuse finish gid command when sync commit is on and standby is not connected. */
    if (!Recovery_IsStandby() && enable_sync_commit && !SyncReady)
    {
        elog(ERROR, "synchronous commit is on, synchronous standby is not ready");
    }

    store_txn_handle = GTM_StoreGetPreparedTxnInfo(gid, &prepared_gxid, &nodestring);
    if (INVALID_STORAGE_HANDLE == store_txn_handle)
    {
        ereport(ERROR,
                (EPROTO,
                  errmsg("failed to get prepared transaction '%s' info from GTM store", gid)));
    }    
    txn_read_only = txn_read_only;
#else
    prepared_txn = GTM_GIDToHandle(gid);
    if (prepared_txn == InvalidTransactionHandle)
        ereport(ERROR,
                (EINVAL,
                 errmsg("Failed to get GID Data for prepared transaction")));

    /* First get the GXID for the new transaction */
    txn = GTM_BeginTransaction(txn_isolation_level, txn_read_only, NULL);
    if (txn == InvalidTransactionHandle)
        ereport(ERROR,
            (EINVAL,
             errmsg("Failed to get the information of prepared transaction")));

    gxid = GTM_GetGlobalTransactionId(txn);
    if (gxid == InvalidGlobalTransactionId)
        ereport(ERROR,
                (EINVAL,
                 errmsg("Failed to get a new transaction id")));

    /*
     * Make the internal process, get the prepared information from GID.
     */
    if (GTM_GetGIDData(prepared_txn, &prepared_gxid, &nodestring) != STATUS_OK)
    {
        ereport(ERROR,
                (EINVAL,
                 errmsg("Failed to get the information of prepared transaction")));
    }
#endif

#ifndef __XLOG__
    if (GetMyConnection(myport)->standby)
    {
        GTM_Conn *oldconn = GetMyConnection(myport)->standby;
        int count = 0;
        GTM_Timestamp timestamp = 0;

        elog(DEBUG1, "calling bkup_begin_transaction_gxid() for auxiliary transaction for standby GTM %p.",
            GetMyConnection(myport)->standby);

retry:
        /*
         * The main XID was already backed up on the standby when it was
         * started. Now also backup the new GXID we obtained above for running
         * COMMIT/ROLLBACK PREPARED statements. This is necessary because GTM
         * will later receive a COMMIT/ABORT message for this XID and the
         * standby must be prepared to handle those messages as well
         *
         * Note: We use the same routine used to backup a new transaction
         * instead of writing a routine specific to MSG_TXN_GET_GID_DATA
         * message
         */ 
        bkup_begin_transaction_gxid(GetMyConnection(myport)->standby,
                   gxid,
                   txn_isolation_level,
                   false,
                   NULL,
                   -1,
                   timestamp);

        if (gtm_standby_check_communication_error(myport, &count, oldconn))
            goto retry;

    }
#endif

    BeforeReplyToClientXLogTrigger();
    
    /*
     * Send a SUCCESS message back to the client
     */
    pq_beginmessage(&buf, 'S');
    pq_sendint(&buf, TXN_GET_GID_DATA_RESULT, 4);
    if (myport->remote_type == GTM_NODE_GTM_PROXY)
    {
        GTM_ProxyMsgHeader proxyhdr;
        proxyhdr.ph_conid = myport->conn_id;
        pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
    }

    /* Send the two GXIDs */
    pq_sendbytes(&buf, (char *)&gxid, sizeof(GlobalTransactionId));
    pq_sendbytes(&buf, (char *)&prepared_gxid, sizeof(GlobalTransactionId));

    /* Node string list */
    if (nodestring)
    {
        pq_sendint(&buf, strlen(nodestring), 4);
        pq_sendbytes(&buf, nodestring, strlen(nodestring));
    }
    else
        pq_sendint(&buf, 0, 4);

    /* End of message */
    pq_endmessage(myport, &buf);

    /* No backup to the standby because this does not change internal status */
    if (myport->remote_type != GTM_NODE_GTM_PROXY)
        pq_flush(myport);
    pfree(gid);
    return;
}
/*
 * Process MSG_TXN_GXID_LIST
 */
void
ProcessGXIDListCommand(Port *myport, StringInfo message)
{
    MemoryContext oldContext;
    StringInfoData buf;
    char *data;
    size_t estlen, actlen; /* estimated length and actual length */

    pq_getmsgend(message);

    if (Recovery_IsStandby())
        ereport(ERROR,
            (EPERM,
             errmsg("Operation not permitted under the standby mode.")));

    /*
     * Do something here.
     */
    oldContext = MemoryContextSwitchTo(TopMemoryContext);

    GTM_RWLockAcquire(&GTMTransactions.gt_XidGenLock, GTM_LOCKMODE_WRITE);

    estlen = gtm_get_transactions_size(&GTMTransactions);
    data = malloc(estlen+1);

    actlen = gtm_serialize_transactions(&GTMTransactions, data, estlen);

    elog(DEBUG1, "gtm_serialize_transactions: estlen=%ld, actlen=%ld", estlen, actlen);

    GTM_RWLockRelease(&GTMTransactions.gt_XidGenLock);

    MemoryContextSwitchTo(oldContext);

    BeforeReplyToClientXLogTrigger();
    
    /*
     * Send a SUCCESS message back to the client
     */
    pq_beginmessage(&buf, 'S');
    pq_sendint(&buf, TXN_GXID_LIST_RESULT, 4);
    if (myport->remote_type == GTM_NODE_GTM_PROXY)
    {
        GTM_ProxyMsgHeader proxyhdr;
        proxyhdr.ph_conid = myport->conn_id;
        pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
    }

    pq_sendint(&buf, actlen, sizeof(int32));    /* size of serialized GTM_Transactions */
    pq_sendbytes(&buf, data, actlen);            /* serialized GTM_Transactions */
    pq_endmessage(myport, &buf);

    /* No backup to the standby because this does not change internal state */
    if (myport->remote_type != GTM_NODE_GTM_PROXY)
    {
        pq_flush(myport);
        elog(DEBUG1, "pq_flush()");
    }

    elog(DEBUG1, "ProcessGXIDListCommand() ok. %ld bytes sent. len=%d", actlen, buf.len);
    free(data);

    return;
}


/*
 * Process MSG_TXN_ROLLBACK/MSG_BKUP_TXN_ROLLBACK message
 *
 * is_backup indicates the message is MSG_BKUP_TXN_ROLLBACK
 */
void
ProcessRollbackTransactionCommand(Port *myport, StringInfo message, bool is_backup)
{// #lizard forgives
    StringInfoData buf;
    GTM_TransactionHandle txn;
    GlobalTransactionId gxid;
    MemoryContext oldContext;
    int status = STATUS_OK;
    const char *data = pq_getmsgbytes(message, sizeof (gxid));

    if (data == NULL)
        ereport(ERROR,
                (EPROTO,
                 errmsg("Message does not contain valid GXID")));
    memcpy(&gxid, data, sizeof (gxid));
    txn = GTM_GXIDToHandle(gxid);

    pq_getmsgend(message);

    oldContext = MemoryContextSwitchTo(TopMemoryContext);

    elog(DEBUG1, "Cancelling transaction id %u", gxid);

    /*
     * Commit the transaction
     */
    status = GTM_RollbackTransaction(txn);

    MemoryContextSwitchTo(oldContext);

    if (!is_backup)
    {
#ifndef __XLOG__
        /* Backup first */
        if (GetMyConnection(myport)->standby)
        {
            GTM_Conn *oldconn = GetMyConnection(myport)->standby;
            int count = 0;

            elog(DEBUG1, "calling abort_transaction() for standby GTM %p.", GetMyConnection(myport)->standby);

        retry:
            bkup_abort_transaction(GetMyConnection(myport)->standby, gxid);

            if (gtm_standby_check_communication_error(myport, &count, oldconn))
                goto retry;

            /* Sync */
            if (Backup_synchronously && (myport->remote_type != GTM_NODE_GTM_PROXY))
                gtm_sync_standby(GetMyConnection(myport)->standby);

            elog(DEBUG1, "abort_transaction() GXID=%d done.", gxid);
        }
#endif
        
        BeforeReplyToClientXLogTrigger();
        
        /* Respond to the client */
        pq_beginmessage(&buf, 'S');
        pq_sendint(&buf, TXN_ROLLBACK_RESULT, 4);
        if (myport->remote_type == GTM_NODE_GTM_PROXY)
        {
            GTM_ProxyMsgHeader proxyhdr;
            proxyhdr.ph_conid = myport->conn_id;
            pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
        }
        pq_sendbytes(&buf, (char *)&gxid, sizeof(gxid));
        pq_sendint(&buf, status, sizeof(status));
        pq_endmessage(myport, &buf);

        if (myport->remote_type != GTM_NODE_GTM_PROXY)
        {
#ifndef __XLOG__
            /* Flush standby first */
            if (GetMyConnection(myport)->standby)
                gtmpqFlush(GetMyConnection(myport)->standby);
#endif
            pq_flush(myport);
        }

    }
    else 
        BeforeReplyToClientXLogTrigger();
    return;
}


/*
 * Process MSG_TXN_COMMIT_MULTI/MSG_BKUP_TXN_COMMIT_MULTI message
 *
 * is_backup indicates the message is MSG_BKUP_TXN_COMMIT_MULTI
 */
void
ProcessCommitTransactionCommandMulti(Port *myport, StringInfo message, bool is_backup)
{// #lizard forgives
    StringInfoData buf;
    GTM_TransactionHandle txn[GTM_MAX_GLOBAL_TRANSACTIONS];
    GlobalTransactionId gxid[GTM_MAX_GLOBAL_TRANSACTIONS];
    MemoryContext oldContext;
    int status[GTM_MAX_GLOBAL_TRANSACTIONS];
    int txn_count;
    int ii;

    txn_count = pq_getmsgint(message, sizeof (int));

    for (ii = 0; ii < txn_count; ii++)
    {
        const char *data = pq_getmsgbytes(message, sizeof (gxid[ii]));
        if (data == NULL)
            ereport(ERROR,
                    (EPROTO,
                     errmsg("Message does not contain valid GXID")));
        memcpy(&gxid[ii], data, sizeof (gxid[ii]));
        txn[ii] = GTM_GXIDToHandle(gxid[ii]);
        elog(DEBUG1, "ProcessCommitTransactionCommandMulti: gxid(%u), handle(%u)", gxid[ii], txn[ii]);
    }

    pq_getmsgend(message);

    oldContext = MemoryContextSwitchTo(TopMemoryContext);

    /*
     * Commit the transaction
     */
    GTM_CommitTransactionMulti(txn, txn_count, 0, NULL, status);

    MemoryContextSwitchTo(oldContext);

    if (!is_backup)
    {
#ifndef __XLOG__
        if (GetMyConnection(myport)->standby)
        {
            /* Backup first */
            int _rc;
            GTM_Conn *oldconn = GetMyConnection(myport)->standby;
            int count = 0;

            elog(DEBUG1, "calling commit_transaction_multi() for standby GTM %p.",
                 GetMyConnection(myport)->standby);

        retry:
            _rc =
                bkup_commit_transaction_multi(GetMyConnection(myport)->standby,
                        txn_count, gxid);

            if (gtm_standby_check_communication_error(myport, &count, oldconn))
                goto retry;
            /* Sync */
            if (Backup_synchronously && (myport->remote_type != GTM_NODE_GTM_PROXY))
                gtm_sync_standby(GetMyConnection(myport)->standby);

            elog(DEBUG1, "commit_transaction_multi() rc=%d done.", _rc);
        }
#endif

        BeforeReplyToClientXLogTrigger();
        
        /* Respond to the client */
        pq_beginmessage(&buf, 'S');
        pq_sendint(&buf, TXN_COMMIT_MULTI_RESULT, 4);
        if (myport->remote_type == GTM_NODE_GTM_PROXY)
        {
            GTM_ProxyMsgHeader proxyhdr;
            proxyhdr.ph_conid = myport->conn_id;
            pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
        }
        pq_sendbytes(&buf, (char *)&txn_count, sizeof(txn_count));
        pq_sendbytes(&buf, (char *)status, sizeof(int) * txn_count);
        pq_endmessage(myport, &buf);

        if (myport->remote_type != GTM_NODE_GTM_PROXY)
        {
#ifndef __XLOG__
            /* Flush the standby */
            if (GetMyConnection(myport)->standby)
                gtmpqFlush(GetMyConnection(myport)->standby);
#endif
            pq_flush(myport);
        }
    }
    else 
        BeforeReplyToClientXLogTrigger();
    return;
}

/*
 * Process MSG_TXN_ROLLBACK_MULTI/MSG_BKUP_TXN_ROLLBACK_MULTI message
 *
 * is_backup indicates the message is MSG_BKUP_TXN_ROLLBACK_MULTI
 */
void
ProcessRollbackTransactionCommandMulti(Port *myport, StringInfo message, bool is_backup)
{// #lizard forgives
    StringInfoData buf;
    GTM_TransactionHandle txn[GTM_MAX_GLOBAL_TRANSACTIONS];
    GlobalTransactionId gxid[GTM_MAX_GLOBAL_TRANSACTIONS];
    MemoryContext oldContext;
    int status[GTM_MAX_GLOBAL_TRANSACTIONS];
    int txn_count;
    int ii;

    txn_count = pq_getmsgint(message, sizeof (int));

    for (ii = 0; ii < txn_count; ii++)
    {
        const char *data = pq_getmsgbytes(message, sizeof (gxid[ii]));
        if (data == NULL)
            ereport(ERROR,
                    (EPROTO,
                     errmsg("Message does not contain valid GXID")));
        memcpy(&gxid[ii], data, sizeof (gxid[ii]));
        txn[ii] = GTM_GXIDToHandle(gxid[ii]);
        elog(DEBUG1, "ProcessRollbackTransactionCommandMulti: gxid(%u), handle(%u)", gxid[ii], txn[ii]);
    }

    pq_getmsgend(message);

    oldContext = MemoryContextSwitchTo(TopMemoryContext);

    /*
     * Commit the transaction
     */
    GTM_RollbackTransactionMulti(txn, txn_count, status);

    MemoryContextSwitchTo(oldContext);

    if (!is_backup)
    {
#ifndef __XLOG__
        /* Backup first */
        if (GetMyConnection(myport)->standby)
        {
            int _rc;
            GTM_Conn *oldconn = GetMyConnection(myport)->standby;
            int count = 0;

            elog(DEBUG1, "calling abort_transaction_multi() for standby GTM %p.",
                 GetMyConnection(myport)->standby);

        retry:
            _rc = bkup_abort_transaction_multi(GetMyConnection(myport)->standby, txn_count, gxid);

            if (gtm_standby_check_communication_error(myport, &count, oldconn))
                goto retry;

            /* Sync */
            if (Backup_synchronously &&(myport->remote_type != GTM_NODE_GTM_PROXY))
                gtm_sync_standby(GetMyConnection(myport)->standby);

            elog(DEBUG1, "abort_transaction_multi() rc=%d done.", _rc);
        }
#endif
        
        BeforeReplyToClientXLogTrigger();
        
        /* Respond to the client */
        pq_beginmessage(&buf, 'S');
        pq_sendint(&buf, TXN_ROLLBACK_MULTI_RESULT, 4);
        if (myport->remote_type == GTM_NODE_GTM_PROXY)
        {
            GTM_ProxyMsgHeader proxyhdr;
            proxyhdr.ph_conid = myport->conn_id;
            pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
        }
        pq_sendbytes(&buf, (char *)&txn_count, sizeof(txn_count));
        pq_sendbytes(&buf, (char *)status, sizeof(int) * txn_count);
        pq_endmessage(myport, &buf);

        if (myport->remote_type != GTM_NODE_GTM_PROXY)
        {
#ifndef __XLOG__
            /* Flush the standby */
            if (GetMyConnection(myport)->standby)
                gtmpqFlush(GetMyConnection(myport)->standby);
#endif
            pq_flush(myport);
        }

    }
    else 
        BeforeReplyToClientXLogTrigger();
    return;
}

/*
 * Process MSG_TXN_START_PREPARED/MSG_BKUP_TXN_START_PREPARED message
 *
 */
void
ProcessStartPreparedTransactionCommand(Port *myport, StringInfo message, bool is_backup)
{// #lizard forgives
    StringInfoData buf;
    GTM_TransactionHandle txn = 0;
    GlobalTransactionId gxid;
    GTM_StrLen gidlen, nodelen;
    char nodestring[1024];
    MemoryContext oldContext;
    char *gid;
    const char *data = pq_getmsgbytes(message, sizeof (gxid));

    if (data == NULL)
    {
        ereport(ERROR,
                (EPROTO,
                 errmsg("Message does not contain valid GXID")));
    }
    memcpy(&gxid, data, sizeof (gxid));
    
#ifndef __TBASE__
    txn = GTM_GXIDToHandle(gxid);
#else
    txn = txn;

    /* We refuse prepare transaction command when sync commit is on and standby is not connected. */
    if (!Recovery_IsStandby() && enable_sync_commit && !SyncReady)
    {
        elog(ERROR, "synchronous commit is on, synchronous standby is not ready");
    }

    if (Recovery_IsStandby())
    {
        if (myport->remote_type != GTM_NODE_GTM)
        {
            elog(ERROR, "gtm standby can't provide 2PC transaction to datanodes or coordinators.");
        }
    }
#endif

    /* get GID */
    gidlen = pq_getmsgint(message, sizeof (GTM_StrLen));
    gid = (char *) palloc(gidlen + 1);
    memcpy(gid, (char *)pq_getmsgbytes(message, gidlen), gidlen);
    gid[gidlen] = '\0';

    /* get node string list */
    nodelen = pq_getmsgint(message, sizeof (GTM_StrLen));
    memcpy(nodestring, (char *)pq_getmsgbytes(message, nodelen), nodelen);
    nodestring[nodelen] = '\0';

    pq_getmsgend(message);

    oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

    /*
     * Prepare the transaction
     */
    if (GTM_StartPreparedTransaction(txn, gid, nodestring) != STATUS_OK)
        ereport(ERROR,
                (EINVAL,
                 errmsg("Failed to prepare the transaction")));

    MemoryContextSwitchTo(oldContext);


    if (!is_backup)
    {
#ifndef __XLOG__
        /*
         * Backup first
         */
        if (GetMyConnection(myport)->standby)
        {
            int _rc;
            GTM_Conn *oldconn = GetMyConnection(myport)->standby;
            int count = 0;

            elog(DEBUG1, "calling start_prepared_transaction() for standby GTM %p.",
                GetMyConnection(myport)->standby);

        retry:
            _rc = backup_start_prepared_transaction(GetMyConnection(myport)->standby,
                                                    gxid, gid,
                                                    nodestring);

            if (gtm_standby_check_communication_error(myport, &count, oldconn))
                goto retry;

            /* Sync */
            if (Backup_synchronously && (myport->remote_type != GTM_NODE_GTM_PROXY))
                gtm_sync_standby(GetMyConnection(myport)->standby);

            elog(DEBUG1, "start_prepared_transaction() rc=%d done.", _rc);
        }
#endif     
        BeforeReplyToClientXLogTrigger();
        
        pq_beginmessage(&buf, 'S');
        pq_sendint(&buf, TXN_START_PREPARED_RESULT, 4);
        if (myport->remote_type == GTM_NODE_GTM_PROXY)
        {
            GTM_ProxyMsgHeader proxyhdr;
            proxyhdr.ph_conid = myport->conn_id;
            pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
        }
        pq_sendbytes(&buf, (char *)&gxid, sizeof(GlobalTransactionId));
        pq_endmessage(myport, &buf);

        if (myport->remote_type != GTM_NODE_GTM_PROXY)
        {
#ifndef __XLOG__
            /* Flush the standby */
            if (GetMyConnection(myport)->standby)
                gtmpqFlush(GetMyConnection(myport)->standby);
#endif
            pq_flush(myport);
        }

    }

    pfree(gid);
    return;
}


void
ProcessLogTransactionCommand(Port *myport, StringInfo message, bool is_global, bool is_backup)
{// #lizard forgives
    StringInfoData buf;
    GlobalTransactionId gxid;
    GTM_StrLen gidlen, nodelen;
    char nodestring[1024];
    MemoryContext oldContext;
    char *gid;
    GlobalTimestamp prepare_ts, commit_ts;
    int node_count, isCommit;
    const char *data = pq_getmsgbytes(message, sizeof (gxid));

    if (data == NULL)
    {
        ereport(ERROR,
                (EPROTO,
                 errmsg("Message does not contain valid GXID")));
    }
    memcpy(&gxid, data, sizeof (gxid));
    

    /* get GID */
    gidlen = pq_getmsgint(message, sizeof (GTM_StrLen));
    gid = (char *) palloc(gidlen + 1);
    memcpy(gid, (char *)pq_getmsgbytes(message, gidlen), gidlen);
    gid[gidlen] = '\0';

    /* get node string list */
    nodelen = pq_getmsgint(message, sizeof (GTM_StrLen));
    if(nodelen)
        memcpy(nodestring, (char *)pq_getmsgbytes(message, nodelen), nodelen);
    nodestring[nodelen] = '\0';
    node_count = pq_getmsgint(message, sizeof (int));
    isCommit = pq_getmsgint(message, sizeof (int));
    data = pq_getmsgbytes(message, sizeof(GlobalTimestamp));
    
    if (data == NULL)
    {
        ereport(ERROR,
                (EPROTO,
                 errmsg("Message does not contain valid prepare timestamp")));
    }
    memcpy(&prepare_ts, data, sizeof (prepare_ts));
    data = pq_getmsgbytes(message, sizeof(GlobalTimestamp));
    
    if (data == NULL)
    {
        ereport(ERROR,
                (EPROTO,
                 errmsg("Message does not contain valid commit timestamp")));
    }
    memcpy(&commit_ts, data, sizeof (commit_ts));
    pq_getmsgend(message);

    oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

    /*
     * Prepare the transaction
     */
    if (enable_gtm_debug)
    {
        if(GTM_LogTransaction(gxid, gid, nodestring, node_count, is_global, isCommit, prepare_ts, commit_ts) != STATUS_OK)
        {
            ereport(ERROR,
                (EINVAL,
                 errmsg("Failed to log the transaction")));
        }
    }

    MemoryContextSwitchTo(oldContext);

    if (!is_backup)
    {
    
        BeforeReplyToClientXLogTrigger();
    
        pq_beginmessage(&buf, 'S');
        pq_sendint(&buf, TXN_LOG_TRANSACTION_RESULT, 4);
        if (myport->remote_type == GTM_NODE_GTM_PROXY)
        {
            GTM_ProxyMsgHeader proxyhdr;
            proxyhdr.ph_conid = myport->conn_id;
            pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
        }
        pq_sendbytes(&buf, (char *)&gxid, sizeof(GlobalTransactionId));
        pq_endmessage(myport, &buf);

        if (myport->remote_type != GTM_NODE_GTM_PROXY)
        {
#ifndef __XLOG__
            /* Flush the standby */
            if (GetMyConnection(myport)->standby)
                gtmpqFlush(GetMyConnection(myport)->standby);
#endif
            pq_flush(myport);
        }

    }
    else 
        BeforeReplyToClientXLogTrigger();

    pfree(gid);
    return;
}



void
ProcessLogScanCommand(Port *myport, StringInfo message, bool is_backup)
{// #lizard forgives
    StringInfoData buf;
    GlobalTransactionId gxid;
    GTM_StrLen relnamelen, nodelen;
    char nodestring[1024];
    MemoryContext oldContext;
    char *rel_name;
    GlobalTimestamp start_ts, local_start_ts, local_complete_ts;
    int scan_type;
    int64 scan_number;
    const char *data = pq_getmsgbytes(message, sizeof (gxid));

    if (data == NULL)
    {
        ereport(ERROR,
                (EPROTO,
                 errmsg("Message does not contain valid GXID")));
    }
    memcpy(&gxid, data, sizeof (gxid));
    

    /* get node string */
    nodelen = pq_getmsgint(message, sizeof (GTM_StrLen));
    memcpy(nodestring, (char *)pq_getmsgbytes(message, nodelen), nodelen);
    nodestring[nodelen] = '\0';
    
    data = pq_getmsgbytes(message, sizeof(GlobalTimestamp));
    
    if (data == NULL)
    {
        ereport(ERROR,
                (EPROTO,
                 errmsg("Message does not contain valid start timestamp")));
    }
    memcpy(&start_ts, data, sizeof (start_ts));

    data = pq_getmsgbytes(message, sizeof(GlobalTimestamp));
    
    if (data == NULL)
    {
        ereport(ERROR,
                (EPROTO,
                 errmsg("Message does not contain valid local start timestamp")));
    }
    memcpy(&local_start_ts, data, sizeof (local_start_ts));

    
    data = pq_getmsgbytes(message, sizeof(GlobalTimestamp));
    
    if (data == NULL)
    {
        ereport(ERROR,
                (EPROTO,
                 errmsg("Message does not contain valid local complete timestamp")));
    }
    memcpy(&local_complete_ts, data, sizeof (local_complete_ts));

    scan_type = pq_getmsgint(message, sizeof (scan_type));
    relnamelen = pq_getmsgint(message, sizeof (GTM_StrLen));
    rel_name = (char *) palloc(relnamelen + 1);
    memcpy(rel_name, (char *)pq_getmsgbytes(message, relnamelen), relnamelen);
    rel_name[relnamelen] = '\0';
    
    data = pq_getmsgbytes(message, sizeof(int64));
    
    if (data == NULL)
    {
        ereport(ERROR,
                (EPROTO,
                 errmsg("Message does not contain valid scan number")));
    }
    memcpy(&scan_number, data, sizeof (scan_number));
    pq_getmsgend(message);

    oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

    /*
     * Prepare the transaction
     */
    if (enable_gtm_debug)
    {
        if(GTM_LogScan(gxid, nodestring, start_ts, local_start_ts, local_complete_ts, scan_type, rel_name, scan_number) != STATUS_OK)
        {
            ereport(ERROR,
                (EINVAL,
                 errmsg("Failed to log the scan transaction")));
        }
    }

    MemoryContextSwitchTo(oldContext);

    if (!is_backup)
    {
        
        BeforeReplyToClientXLogTrigger();
    
        pq_beginmessage(&buf, 'S');
        pq_sendint(&buf, TXN_LOG_SCAN_RESULT, 4);
        if (myport->remote_type == GTM_NODE_GTM_PROXY)
        {
            GTM_ProxyMsgHeader proxyhdr;
            proxyhdr.ph_conid = myport->conn_id;
            pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
        }
        pq_sendbytes(&buf, (char *)&gxid, sizeof(GlobalTransactionId));
        pq_endmessage(myport, &buf);

        if (myport->remote_type != GTM_NODE_GTM_PROXY)
        {
#ifndef __XLOG__
            /* Flush the standby */
            if (GetMyConnection(myport)->standby)
                gtmpqFlush(GetMyConnection(myport)->standby);
#endif
            pq_flush(myport);
        }

    }
    else
        BeforeReplyToClientXLogTrigger();

    pfree(rel_name);
    return;
}


/*
 * Process MSG_TXN_PREPARE/MSG_BKUP_TXN_PREPARE message
 *
 * is_backup indicates the message is MSG_BKUP_TXN_PREPARE
 */
void
ProcessPrepareTransactionCommand(Port *myport, StringInfo message, bool is_backup)
{// #lizard forgives
    StringInfoData buf;
    GTM_TransactionHandle txn;
    GlobalTransactionId gxid;
    MemoryContext oldContext;
    const char *data = pq_getmsgbytes(message, sizeof (gxid));

    if (data == NULL)
        ereport(ERROR,
                (EPROTO,
                 errmsg("Message does not contain valid GXID")));
    memcpy(&gxid, data, sizeof (gxid));
    txn = GTM_GXIDToHandle(gxid);

    pq_getmsgend(message);

    oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

    /*
     * Commit the transaction
     */
    GTM_PrepareTransaction(txn);

    MemoryContextSwitchTo(oldContext);

    elog(DEBUG1, "Preparing transaction id %u", gxid);

    if (!is_backup)
    {
#ifndef __XLOG__
        /* Backup first */
        if (GetMyConnection(myport)->standby)
        {
            GTM_Conn *oldconn = GetMyConnection(myport)->standby;
            int count = 0;

            elog(DEBUG1, "calling prepare_transaction() for standby GTM %p.", GetMyConnection(myport)->standby);

        retry:
            bkup_prepare_transaction(GetMyConnection(myport)->standby, gxid);

            if (gtm_standby_check_communication_error(myport, &count, oldconn))
                goto retry;

            /* Sync */
            if (Backup_synchronously && (myport->remote_type != GTM_NODE_GTM_PROXY))
                gtm_sync_standby(GetMyConnection(myport)->standby);

            elog(DEBUG1, "prepare_transaction() GXID=%d done.", gxid);
        }
#endif
        
        BeforeReplyToClientXLogTrigger();
        
        /* Respond to the client */
        pq_beginmessage(&buf, 'S');
        pq_sendint(&buf, TXN_PREPARE_RESULT, 4);
        if (myport->remote_type == GTM_NODE_GTM_PROXY)
        {
            GTM_ProxyMsgHeader proxyhdr;
            proxyhdr.ph_conid = myport->conn_id;
            pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
        }
        pq_sendbytes(&buf, (char *)&gxid, sizeof(gxid));
        pq_endmessage(myport, &buf);

        if (myport->remote_type != GTM_NODE_GTM_PROXY)
        {
#ifndef __XLOG__
            /* Flush the standby */
            if (GetMyConnection(myport)->standby)
                gtmpqFlush(GetMyConnection(myport)->standby);
#endif
            pq_flush(myport);
        }
    }
}


/*
 * Process MSG_TXN_GET_GXID message
 *
 * Notice: we don't have corresponding functions in gtm_client.c which
 * generates a command for this function.
 *
 * Because of this, GTM-standby extension is not included in this function.
 */
void
ProcessGetGXIDTransactionCommand(Port *myport, StringInfo message)
{
    StringInfoData buf;
    GTM_TransactionHandle txn;
    GlobalTransactionId gxid;
    const char *data;
    MemoryContext oldContext;

    elog(DEBUG3, "Inside ProcessGetGXIDTransactionCommand");

    data = pq_getmsgbytes(message, sizeof (txn));
    if (data == NULL)
        ereport(ERROR,
                (EPROTO,
                 errmsg("Message does not contain valid Transaction Handle")));
    memcpy(&txn, data, sizeof (txn));

    pq_getmsgend(message);

    oldContext = MemoryContextSwitchTo(TopMemoryContext);

    /*
     * Get the transaction id for the given global transaction
     */
    gxid = GTM_GetGlobalTransactionId(txn);
    if (GlobalTransactionIdIsValid(gxid))
        ereport(ERROR,
                (EINVAL,
                 errmsg("Failed to get the transaction id")));

    MemoryContextSwitchTo(oldContext);

    elog(DEBUG3, "Sending transaction id %d", gxid);

    BeforeReplyToClientXLogTrigger();

    pq_beginmessage(&buf, 'S');
    pq_sendint(&buf, TXN_GET_GXID_RESULT, 4);
    if (myport->remote_type == GTM_NODE_GTM_PROXY)
    {
        GTM_ProxyMsgHeader proxyhdr;
        proxyhdr.ph_conid = myport->conn_id;
        pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
    }
    pq_sendbytes(&buf, (char *)&txn, sizeof(txn));
    pq_sendbytes(&buf, (char *)&gxid, sizeof(gxid));
    pq_endmessage(myport, &buf);

    if (myport->remote_type != GTM_NODE_GTM_PROXY)
        pq_flush(myport);
    return;
}


/*
 * Process MSG_TXN_GET_NEXT_GXID message
 *
 * This does not need backup to the standby because no internal state changes.
 */
void
ProcessGetNextGXIDTransactionCommand(Port *myport, StringInfo message)
{
    StringInfoData buf;
    GlobalTransactionId next_gxid;
    MemoryContext oldContext;

    elog(DEBUG3, "Inside ProcessGetNextGXIDTransactionCommand");

    pq_getmsgend(message);

    oldContext = MemoryContextSwitchTo(TopMemoryContext);

    /*
     * Get the next gxid.
     */
#if 0
    GTM_RWLockAcquire(&GTMTransactions.gt_XidGenLock, GTM_LOCKMODE_WRITE);
    next_gxid = GTMTransactions.gt_nextXid;

    GTM_RWLockRelease(&GTMTransactions.gt_XidGenLock);
#endif
    next_gxid = pg_atomic_read_u32(&GTMTransactions.gt_global_xid);
    MemoryContextSwitchTo(oldContext);

    elog(DEBUG3, "Sending next gxid %d", next_gxid);
    
    BeforeReplyToClientXLogTrigger();
    
    pq_beginmessage(&buf, 'S');
    pq_sendint(&buf, TXN_GET_NEXT_GXID_RESULT, 4);
    if (myport->remote_type == GTM_NODE_GTM_PROXY)
    {
        GTM_ProxyMsgHeader proxyhdr;
        proxyhdr.ph_conid = myport->conn_id;
        pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
    }
    pq_sendint(&buf, next_gxid, sizeof(GlobalTransactionId));
    pq_endmessage(myport, &buf);

    if (myport->remote_type != GTM_NODE_GTM_PROXY)
        pq_flush(myport);
    return;
}

void
ProcessReportXminCommand(Port *myport, StringInfo message, bool is_backup)
{
    StringInfoData buf;
    GlobalTransactionId gxid;
    GTM_StrLen nodelen;
    char node_name[NI_MAXHOST];
    GTM_PGXCNodeType    type;
    GlobalTransactionId    global_xmin;
    int errcode;

    const char *data = pq_getmsgbytes(message, sizeof (gxid));

    if (data == NULL)
        ereport(ERROR,
                (EPROTO,
                 errmsg("Message does not contain valid GXID")));
    memcpy(&gxid, data, sizeof (gxid));

    /* Read Node Type */
    type = pq_getmsgint(message, sizeof (GTM_PGXCNodeType));

    /* get node name */
    nodelen = pq_getmsgint(message, sizeof (GTM_StrLen));
    memcpy(node_name, (char *)pq_getmsgbytes(message, nodelen), nodelen);
    node_name[nodelen] = '\0';
    pq_getmsgend(message);

    global_xmin = GTM_HandleGlobalXmin(type, node_name, gxid, &errcode);

    {
        
        BeforeReplyToClientXLogTrigger();
    
        /*
         * Send a SUCCESS message back to the client
         */
        pq_beginmessage(&buf, 'S');
        pq_sendint(&buf, REPORT_XMIN_RESULT, 4);
        if (myport->remote_type == GTM_NODE_GTM_PROXY)
        {
            GTM_ProxyMsgHeader proxyhdr;
            proxyhdr.ph_conid = myport->conn_id;
            pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
        }
        pq_sendbytes(&buf, (char *)&GTMTransactions.gt_latestCompletedXid, sizeof (GlobalTransactionId));
        pq_sendbytes(&buf, (char *)&global_xmin, sizeof (GlobalTransactionId));
        pq_sendbytes(&buf, (char *)&errcode, sizeof (errcode));
        pq_endmessage(myport, &buf);
        pq_flush(myport);
    }
}

/*
 * Mark GTM as shutting down. This point onwards no new GXID are issued to
 * ensure that the last GXID recorded in the control file remains sane
 */
void
GTM_SetShuttingDown(void)
{
    GTM_RWLockAcquire(&GTMTransactions.gt_XidGenLock, GTM_LOCKMODE_WRITE);
    GTMTransactions.gt_gtm_state = GTM_SHUTTING_DOWN;
    GTM_RWLockRelease(&GTMTransactions.gt_XidGenLock);
}

bool GTM_NeedXidRestoreUpdate(void)
{
    return(GlobalTransactionIdPrecedesOrEquals(GTMTransactions.gt_backedUpXid, GTMTransactions.gt_nextXid));
}


GlobalTransactionId
GTM_GetLatestCompletedXID(void)
{
    return GTMTransactions.gt_latestCompletedXid;
}

void
GTM_ForgetCreatedSequence(GlobalTransactionId gxid, void *seq)
{
    GTM_TransactionInfo *gtm_txninfo;
    GTM_TransactionHandle txn = GTM_GXIDToHandle(gxid);
   
    if (txn == InvalidTransactionHandle)
        return;

    gtm_txninfo = GTM_HandleToTransactionInfo(txn);
    gtm_txninfo->gti_created_seqs =
        gtm_list_delete(gtm_txninfo->gti_created_seqs, seq);
}

/*
 * Remember sequence created by transaction 'gxid'.
 *
 * This should be removed from the global data structure if the transaction
 * aborts (see GTM_SeqRemoveCreated). If the sequence is later dropped in the
 * same transaction, we remove it from the global structure as well as forget
 * tracking (see GTM_ForgetCreatedSequence). If the transaction commits, just
 * forget about this tracked sequence.
 */
void
GTM_RememberCreatedSequence(GlobalTransactionId gxid, void *seq)
{
    GTM_TransactionInfo *gtm_txninfo;
    GTM_TransactionHandle txn = GTM_GXIDToHandle(gxid);
   
    if (txn == InvalidTransactionHandle)
        return;

    gtm_txninfo = GTM_HandleToTransactionInfo(txn);
    gtm_txninfo->gti_created_seqs =
        gtm_lappend(gtm_txninfo->gti_created_seqs, seq);
}

void
GTM_RememberDroppedSequence(GlobalTransactionId gxid, void *seq)
{
    GTM_TransactionInfo *gtm_txninfo;
    GTM_TransactionHandle txn = GTM_GXIDToHandle(gxid);
   
    if (txn == InvalidTransactionHandle)
        return;

    gtm_txninfo = GTM_HandleToTransactionInfo(txn);
    gtm_txninfo->gti_dropped_seqs =
        gtm_lappend(gtm_txninfo->gti_dropped_seqs, seq);
}

void
GTM_RememberAlteredSequence(GlobalTransactionId gxid, void *seq)
{
    GTM_TransactionInfo *gtm_txninfo;
    GTM_TransactionHandle txn = GTM_GXIDToHandle(gxid);
   
    if (txn == InvalidTransactionHandle)
        return;

    gtm_txninfo = GTM_HandleToTransactionInfo(txn);
    gtm_txninfo->gti_altered_seqs = gtm_lcons(seq,
            gtm_txninfo->gti_altered_seqs);
}


/*
 * TODO
 */
int GTM_GetAllTransactions(GTM_TransactionInfo txninfo[], uint32 txncnt);

/*
 * TODO
 */
uint32 GTM_GetAllPrepared(GlobalTransactionId gxids[], uint32 gxidcnt);

#ifdef __TBASE__

void
ProcessFinishGIDTransactionCommand(Port *myport, StringInfo message)
{// #lizard forgives
    int32 ret;
    StringInfoData buf;
    char *gid;
    int   gidlen;

    /*We refuse finish gid command when sync commit is on and standby is not connected. */
    if (!Recovery_IsStandby() && enable_sync_commit && !SyncReady)
    {
        elog(ERROR, "synchronous commit is on, synchronous standby is not ready");
    }
    
    if (Recovery_IsStandby())
    {
        if (myport->remote_type != GTM_NODE_GTM)
        {
            elog(ERROR, "gtm standby can't provide 2PC transaction to datanodes or coordinators.");
        }
    }
    
    /* receive GID */
    gidlen = pq_getmsgint(message, sizeof (GTM_StrLen));
    gid = (char *) palloc(gidlen + 1);
    memcpy(gid, (char *)pq_getmsgbytes(message, gidlen), gidlen);
    gid[gidlen] = '\0';

    pq_getmsgend(message);

    /* Get the prepared Transaction for given GID */
    ret = GTM_StoreFinishTxn(gid);
    if (!ret)
    {
    
#ifndef __XLOG__
        if (GetMyConnection(myport)->standby)
        {
            GTM_Conn *oldconn = GetMyConnection(myport)->standby;
            int count = 0;

            elog(DEBUG1, "calling bkup_begin_transaction_gxid() for auxiliary transaction for standby GTM %p.",
                GetMyConnection(myport)->standby);

    retry:        
            finish_gid_gtm(GetMyConnection(myport)->standby, gid);
            if (gtm_standby_check_communication_error(myport, &count, oldconn))
                goto retry;

        }
#endif
    }    
    else
    {
        ereport(LOG,
                    (EPROTO,
                      errmsg("failed to finish '%s' info from GTM store", gid)));
    }
    
    BeforeReplyToClientXLogTrigger();
    
    /*
     * Send a SUCCESS message back to the client
     */
    pq_beginmessage(&buf, 'S');
    pq_sendint(&buf, TXN_FINISH_GID_RESULT, 4);
    if (myport->remote_type == GTM_NODE_GTM_PROXY)
    {
        GTM_ProxyMsgHeader proxyhdr;
        proxyhdr.ph_conid = myport->conn_id;
        pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
    }

    /* Send the storage ret */
    pq_sendbytes(&buf, (char *)&ret, sizeof(int32));    

    /* End of message */
    pq_endmessage(myport, &buf);

    /* No backup to the standby because this does not change internal status */
    if (myport->remote_type != GTM_NODE_GTM_PROXY)
    {
        pq_flush(myport);
    }
    pfree(gid);
    return;
}

#endif
