/*-------------------------------------------------------------------------
 *
 * commit_ts.c
 *        PostgreSQL commit timestamp manager
 *
 * This module is a pg_xact-like system that stores the commit timestamp
 * for each transaction.
 *
 * XLOG interactions: this module generates an XLOG record whenever a new
 * CommitTs page is initialized to zeroes.  Also, one XLOG record is
 * generated for setting of values when the caller requests it; this allows
 * us to support values coming from places other than transaction commit.
 * Other writes of CommitTS come from recording of transaction commit in
 * xact.c, which generates its own XLOG records for these events and will
 * re-perform the status update on redo; so we need make no additional XLOG
 * entry here.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/commit_ts.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>
#include <sys/mman.h>

#include "access/commit_ts.h"
#include "access/htup_details.h"
#include "access/lru.h"
#include "access/transam.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

bool enable_committs_print = false;


/*
 * Defines for CommitTs page sizes.  A page is the same BLCKSZ as is used
 * everywhere else in Postgres.
 *
 * Note: because TransactionIds are 32 bits and wrap around at 0xFFFFFFFF,
 * CommitTs page numbering also wraps around at
 * 0xFFFFFFFF/COMMIT_TS_XACTS_PER_PAGE, and CommitTs segment numbering at
 * 0xFFFFFFFF/COMMIT_TS_XACTS_PER_PAGE/SLRU_PAGES_PER_SEGMENT.  We need take no
 * explicit notice of that fact in this module, except when comparing segment
 * and page numbers in TruncateCommitTs (see CommitTsPagePrecedes).
 */

/*
 * We need 8+2 bytes per xact.  Note that enlarging this struct might mean
 * the largest possible file name is more than 5 chars long; see
 * SlruScanDirectory.
 */
typedef struct CommitTimestampEntry
{
    TimestampTz global_timestamp;
    TimestampTz time;
    RepOriginId nodeid;
} CommitTimestampEntry;

#define SizeOfCommitTimestampEntry (offsetof(CommitTimestampEntry, nodeid) + \
                                    sizeof(RepOriginId))

#define COMMIT_TS_XACTS_PER_PAGE \
    (BLCKSZ / SizeOfCommitTimestampEntry)

#define TransactionIdToCTsPage(xid) \
    ((xid) / (TransactionId) COMMIT_TS_XACTS_PER_PAGE)
#define TransactionIdToCTsEntry(xid)    \
    ((xid) % (TransactionId) COMMIT_TS_XACTS_PER_PAGE)

#ifdef __TBASE__
/* We store the latest async LSN for each group of transactions */
#define TLOG_XACTS_PER_LSN_GROUP    32    /* keep this a power of 2 */
#define TLOG_LSNS_PER_PAGE    (COMMIT_TS_XACTS_PER_PAGE / TLOG_XACTS_PER_LSN_GROUP)

#define GetLSNIndex(slotno, xid)	((slotno) * TLOG_LSNS_PER_PAGE + \
	((xid) % (TransactionId) TLOG_LSNS_PER_PAGE))
#endif

/*
 * Link to shared-memory data structures for CommitTs control
 */
static LruCtlData CommitTsCtlData;

#define CommitTsCtl (&CommitTsCtlData)

/*
 * We keep a cache of the last value set in shared memory.
 *
 * This is also good place to keep the activation status.  We keep this
 * separate from the GUC so that the standby can activate the module if the
 * primary has it active independently of the value of the GUC.
 *
 * This is protected by CommitTsLock.  In some places, we use commitTsActive
 * without acquiring the lock; where this happens, a comment explains the
 * rationale for it.
 */
typedef struct CommitTimestampShared
{
    TransactionId xidLastCommit;
    CommitTimestampEntry dataLastCommit;
    bool        commitTsActive;
} CommitTimestampShared;

CommitTimestampShared *commitTsShared;


/* GUC variable */
bool        track_commit_timestamp = true;
bool        track_commit_timestamp_guc;


static void
SetXidCommitTsInPage(TransactionId xid, int nsubxids,
                     TransactionId *subxids, TimestampTz gts, TimestampTz ts,
                     RepOriginId nodeid, int pageno, XLogRecPtr lsn);
static void TransactionIdSetCommitTs(TransactionId xid, TimestampTz gts, TimestampTz ts,
                         RepOriginId nodeid, int partitionno, int slotno, XLogRecPtr lsn);
static void error_commit_ts_disabled(void);
static int    ZeroCommitTsPage(int pageno, int partitionno, bool writeXlog);
static bool CommitTsPagePrecedes(int page1, int page2);
static void ActivateCommitTs(void);
#ifndef __SUPPORT_DISTRIBUTED_TRANSACTION__
static void DeactivateCommitTs(void);
#endif
static void WriteZeroPageXlogRec(int pageno);
static void WriteTruncateXlogRec(int pageno, TransactionId oldestXid);
static void WriteSetTimestampXlogRec(TransactionId mainxid, int nsubxids,
                         TransactionId *subxids, TimestampTz global_timestamp, TimestampTz timestamp,
                         RepOriginId nodeid);


/*
 * TransactionTreeSetCommitTsData
 *
 * Record the final commit timestamp of transaction entries in the commit log
 * for a transaction and its subtransaction tree, as efficiently as possible.
 *
 * xid is the top level transaction id.
 *
 * subxids is an array of xids of length nsubxids, representing subtransactions
 * in the tree of xid. In various cases nsubxids may be zero.
 * The reason why tracking just the parent xid commit timestamp is not enough
 * is that the subtrans SLRU does not stay valid across crashes (it's not
 * permanent) so we need to keep the information about them here. If the
 * subtrans implementation changes in the future, we might want to revisit the
 * decision of storing timestamp info for each subxid.
 *
 * The write_xlog parameter tells us whether to include an XLog record of this
 * or not.  Normally, this is called from transaction commit routines (both
 * normal and prepared) and the information will be stored in the transaction
 * commit XLog record, and so they should pass "false" for this.  The XLog redo
 * code should use "false" here as well.  Other callers probably want to pass
 * true, so that the given values persist in case of crashes.
 */
void
TransactionTreeSetCommitTsData(TransactionId xid, int nsubxids,
                               TransactionId *subxids, TimestampTz global_timestamp, TimestampTz timestamp, 
                               RepOriginId nodeid, bool write_xlog, XLogRecPtr lsn)
{// #lizard forgives
    int            i;
    TransactionId headxid;
#if 0
    TransactionId newestXact;
#endif
    /*
     * No-op if the module is not active.
     *
     * An unlocked read here is fine, because in a standby (the only place
     * where the flag can change in flight) this routine is only called by the
     * recovery process, which is also the only process which can change the
     * flag.
     */
    
    if(InRecovery)
        elog(DEBUG1, "start to set committs data xid %d committs "INT64_FORMAT, xid, global_timestamp);

    if(!GlobalTimestampIsValid(global_timestamp))
    {
        elog(ERROR, "xid %d should not set invalid commit timestamp.", xid);
    }
    
    if (!commitTsShared->commitTsActive)
        return;


    /*
     * Comply with the WAL-before-data rule: if caller specified it wants this
     * value to be recorded in WAL, do so before touching the data.
     */
    if (write_xlog)
        WriteSetTimestampXlogRec(xid, nsubxids, subxids, global_timestamp, timestamp, nodeid);

    /*
     * Figure out the latest Xid in this batch: either the last subxid if
     * there's any, otherwise the parent xid.
     */
#if 0
    if (nsubxids > 0)
        newestXact = subxids[nsubxids - 1];
    else
        newestXact = xid;
#endif
	/*
	 * We split the xids to set the timestamp to in groups belonging to the
	 * same SLRU page; the first element in each such set is its head.  The
	 * first group has the main XID as the head; subsequent sets use the first
	 * subxid not on the previous page as head.  This way, we only have to
	 * lock/modify each SLRU page once.
	 */
	for (i = 0, headxid = xid;;)
	{
		int			pageno = TransactionIdToCTsPage(headxid);
		int			j;

		for (j = i; j < nsubxids; j++)
		{
			if(enable_committs_print)
			{
				elog(LOG, "TransactionTreeSetCommitTsData, subxid xid %d i %d j %d nsubxids %d", subxids[j], i, j, nsubxids);
			}
			
			if (TransactionIdToCTsPage(subxids[j]) != pageno)
			{
				if(enable_committs_print)
				{
					elog(LOG, "break pageno %d subxid xid %d j %d", pageno, subxids[j], j);
				}
				break;
			}
		}
		/* subxids[i..j] are on the same page as the head */
		if(j - i > 0)
		{
			SetXidCommitTsInPage(headxid, j - i, subxids + i, global_timestamp, timestamp, nodeid,
								 pageno, lsn);
		}
		else
		{
			SetXidCommitTsInPage(headxid, 0, NULL, global_timestamp, timestamp, nodeid,
								 pageno, lsn);
		}

		if(enable_committs_print)
		{
			elog(LOG,
				"TransactionTreeSetCommitTsData: set committs data pageno %d xid %d head xid %d j-i %d i %d nsubxids %d committs "INT64_FORMAT,
				pageno, xid, headxid, j - i, i, nsubxids, global_timestamp);
		}

		/* if we wrote out all subxids, we're done. */
		if (j + 1 > nsubxids)
			break;

		/*
		 * Set the new head and skip over it, as well as over the subxids we
		 * just wrote.
		 */
		headxid = subxids[j];
		i = j + 1;
	}
#if 0
    /* update the cached value in shared memory */
    LWLockAcquire(CommitTsLock, LW_EXCLUSIVE);
    commitTsShared->xidLastCommit = xid;
    commitTsShared->dataLastCommit.time = timestamp;
    commitTsShared->dataLastCommit.nodeid = nodeid;

    /* and move forwards our endpoint, if needed */
    if (TransactionIdPrecedes(ShmemVariableCache->newestCommitTsXid, newestXact))
        ShmemVariableCache->newestCommitTsXid = newestXact;
    LWLockRelease(CommitTsLock);
#endif
}

/*
 * Record the commit timestamp of transaction entries in the commit log for all
 * entries on a single page.  Atomic only on this page.
 */
static void
SetXidCommitTsInPage(TransactionId xid, int nsubxids,
                     TransactionId *subxids, TimestampTz gts, TimestampTz ts,
                     RepOriginId nodeid, int pageno, XLogRecPtr lsn)
{
    int            slotno;
    int            i;
    int         partitionno;
    LWLock         *partitionlock;
    
    partitionno = PagenoMappingPartitionno(CommitTsCtl, pageno);
    partitionlock = GetPartitionLock(CommitTsCtl, partitionno);
    LWLockAcquire(partitionlock, LW_EXCLUSIVE);

    slotno = LruReadPage(CommitTsCtl, partitionno, pageno, XLogRecPtrIsInvalid(lsn), xid);

    TransactionIdSetCommitTs(xid, gts, ts, nodeid, partitionno, slotno, lsn);
    for (i = 0; i < nsubxids; i++)
        TransactionIdSetCommitTs(subxids[i], gts, ts, nodeid, partitionno, slotno, lsn);

    CommitTsCtl->shared[partitionno]->page_dirty[slotno] = true;

    LWLockRelease(partitionlock);
}

/*
 * Sets the commit timestamp of a single transaction.
 *
 * Must be called with CommitTsControlLock held
 */
static void
TransactionIdSetCommitTs(TransactionId xid, TimestampTz gts, TimestampTz ts,
                         RepOriginId nodeid, int partitionno, int slotno, XLogRecPtr lsn)
{
	int			entryno = TransactionIdToCTsEntry(xid);
	CommitTimestampEntry entry;

//	Assert(TransactionIdIsNormal(xid));
	if (enable_committs_print)
	{
		elog(LOG, "TransactionIdSetCommitTs: xid %d gts "INT64_FORMAT, xid, gts);
	}
	entry.global_timestamp = gts;
	entry.time = ts;
	entry.nodeid = nodeid;

	LruTlogDisableMemoryProtection(CommitTsCtl->shared[partitionno]->page_buffer[slotno]);
	memcpy(CommitTsCtl->shared[partitionno]->page_buffer[slotno] +
		   SizeOfCommitTimestampEntry * entryno,
		   &entry, SizeOfCommitTimestampEntry);
	LruTlogEnableMemoryProtection(CommitTsCtl->shared[partitionno]->page_buffer[slotno]);

#ifdef __TBASE__
    /*
     * Update the group LSN if the transaction completion LSN is higher.
     *
     * Note: lsn will be invalid when supplied during InRecovery processing,
     * so we don't need to do anything special to avoid LSN updates during
     * recovery. After recovery completes the next clog change will set the
     * LSN correctly.
     */
    if (!XLogRecPtrIsInvalid(lsn))
    {
        int            lsnindex =  GetLSNIndex(slotno,xid);

        if (CommitTsCtl->shared[partitionno]->group_lsn[lsnindex] < lsn)
            CommitTsCtl->shared[partitionno]->group_lsn[lsnindex] = lsn;
    }
    
#endif
}



/*
 * Interrogate the commit timestamp of a transaction.
 *
 * The return value indicates whether a commit timestamp record was found for
 * the given xid.  The timestamp value is returned in *ts (which may not be
 * null), and the origin node for the Xid is returned in *nodeid, if it's not
 * null.
 */
bool
TransactionIdGetCommitTsData(TransactionId xid, TimestampTz *gts, 
                             RepOriginId *nodeid)
{
    int            pageno = TransactionIdToCTsPage(xid);
    int            entryno = TransactionIdToCTsEntry(xid);
    int            slotno;
    int         partitionno;
    LWLock       *partitionLock;    /* buffer partition lock for it */
    CommitTimestampEntry entry;

    
    if (!TransactionIdIsValid(xid))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot retrieve commit timestamp for transaction %u", xid)));
    
    else if (!TransactionIdIsNormal(xid))
    {
        /* frozen and bootstrap xids are always committed far in the past */
        *gts = LocalCommitTimestamp;
        if (nodeid)
            *nodeid = 0;
        return true;
    }

    //elog(DEBUG8, "Get committs xid %d.", xid);
    partitionno = PagenoMappingPartitionno(CommitTsCtl, pageno);

    partitionLock = GetPartitionLock(CommitTsCtl, partitionno);
    
    /* lock is acquired by SimpleLruReadPage_ReadOnly */
    slotno = LruReadPage_ReadOnly(CommitTsCtl, partitionno, pageno, xid);
    memcpy(&entry,
           CommitTsCtl->shared[partitionno]->page_buffer[slotno] +
           SizeOfCommitTimestampEntry * entryno,
           SizeOfCommitTimestampEntry);

    
    *gts = entry.global_timestamp;
    
    if (nodeid)
    {
        *nodeid = entry.nodeid;
    }
    
    //elog(DEBUG8, "Get committs xid %d time " INT64_FORMAT, xid, *ts);
    LWLockRelease(partitionLock);
    return *gts != 0;
}


bool
TransactionIdGetLocalCommitTsData(TransactionId xid, TimestampTz *ts, 
                             RepOriginId *nodeid)
{
    int            pageno = TransactionIdToCTsPage(xid);
    int            entryno = TransactionIdToCTsEntry(xid);
    int            slotno;
    int         partitionno;
    LWLock       *partitionLock;    /* buffer partition lock for it */
    CommitTimestampEntry entry;

    
    if (!TransactionIdIsValid(xid))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot retrieve commit timestamp for transaction %u", xid)));
    
    else if (!TransactionIdIsNormal(xid))
    {
        /* frozen and bootstrap xids are always committed far in the past */
        *ts = LocalCommitTimestamp;
        if (nodeid)
            *nodeid = 0;
        return true;
    }

    //elog(DEBUG8, "Get committs xid %d.", xid);
    partitionno = PagenoMappingPartitionno(CommitTsCtl, pageno);

    partitionLock = GetPartitionLock(CommitTsCtl, partitionno);
    
    /* lock is acquired by SimpleLruReadPage_ReadOnly */
    slotno = LruReadPage_ReadOnly(CommitTsCtl, partitionno, pageno, xid);
    memcpy(&entry,
           CommitTsCtl->shared[partitionno]->page_buffer[slotno] +
           SizeOfCommitTimestampEntry * entryno,
           SizeOfCommitTimestampEntry);

    
    *ts = entry.time;
    
    if (nodeid)
    {
        *nodeid = entry.nodeid;
    }
    
    //elog(DEBUG8, "Get committs xid %d time " INT64_FORMAT, xid, *ts);
    LWLockRelease(partitionLock);
    return *ts != 0;
}


/*
 * Return the Xid of the latest committed transaction.  (As far as this module
 * is concerned, anyway; it's up to the caller to ensure the value is useful
 * for its purposes.)
 *
 * ts and extra are filled with the corresponding data; they can be passed
 * as NULL if not wanted.
 */
TransactionId
GetLatestCommitTsData(TimestampTz *ts, RepOriginId *nodeid)
{
    TransactionId xid;

    LWLockAcquire(CommitTsLock, LW_SHARED);

    /* Error if module not enabled */
    if (!commitTsShared->commitTsActive)
        error_commit_ts_disabled();

    xid = commitTsShared->xidLastCommit;
    if (ts)
        *ts = commitTsShared->dataLastCommit.time;
    if (nodeid)
        *nodeid = commitTsShared->dataLastCommit.nodeid;
    LWLockRelease(CommitTsLock);

    return xid;
}

static void
error_commit_ts_disabled(void)
{
    ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
             errmsg("could not get commit timestamp data"),
             RecoveryInProgress() ?
             errhint("Make sure the configuration parameter \"%s\" is set on the master server.",
                     "track_commit_timestamp") :
             errhint("Make sure the configuration parameter \"%s\" is set.",
                     "track_commit_timestamp")));
}

/*
 * SQL-callable wrapper to obtain commit time of a transaction
 */
Datum
pg_xact_commit_timestamp(PG_FUNCTION_ARGS)
{
    TransactionId xid = PG_GETARG_UINT32(0);
    TimestampTz  ts;
    bool        found;

    found = TransactionIdGetCommitTsData(xid, &ts, NULL);

    if (!found)
        PG_RETURN_NULL();

    PG_RETURN_TIMESTAMPTZ(ts);
}

Datum
pg_xact_local_commit_timestamp(PG_FUNCTION_ARGS)
{
    TransactionId xid = PG_GETARG_UINT32(0);
    TimestampTz ts;
    bool        found;

    found = TransactionIdGetLocalCommitTsData(xid, &ts, NULL);

    if (!found)
        PG_RETURN_NULL();

    PG_RETURN_TIMESTAMPTZ(ts);
}



Datum
pg_last_committed_xact(PG_FUNCTION_ARGS)
{
    TransactionId xid;
    TimestampTz ts;
    Datum        values[2];
    bool        nulls[2];
    TupleDesc    tupdesc;
    HeapTuple    htup;

    /* and construct a tuple with our data */
    xid = GetLatestCommitTsData(&ts, NULL);

    /*
     * Construct a tuple descriptor for the result row.  This must match this
     * function's pg_proc entry!
     */
    tupdesc = CreateTemplateTupleDesc(2, false);
    TupleDescInitEntry(tupdesc, (AttrNumber) 1, "xid",
                       XIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 2, "timestamp",
                       TIMESTAMPTZOID, -1, 0);
    tupdesc = BlessTupleDesc(tupdesc);

    if (!TransactionIdIsNormal(xid))
    {
        memset(nulls, true, sizeof(nulls));
    }
    else
    {
        values[0] = TransactionIdGetDatum(xid);
        nulls[0] = false;

        values[1] = TimestampTzGetDatum(ts);
        nulls[1] = false;
    }

    htup = heap_form_tuple(tupdesc, values, nulls);

    PG_RETURN_DATUM(HeapTupleGetDatum(htup));
}


/*
 * Number of shared CommitTS buffers.
 *
 * We use a very similar logic as for the number of CLOG buffers; see comments
 * in CLOGShmemBuffers.
 */
Size
CommitTsShmemBuffers(void)
{
    return Min(32, Max(4, NBuffers / 32));
}

/*
 * Shared memory sizing for CommitTs
 */
Size
CommitTsShmemSize(void)
{
    int     max_page_no = TransactionIdToCTsPage(MaxTransactionId) + NUM_PARTITIONS;
        
    return NUM_PARTITIONS * (LruShmemSize(CommitTsShmemBuffers(), TLOG_LSNS_PER_PAGE) +
        sizeof(CommitTimestampShared)) + MAXALIGN(sizeof(GlobalLruSharedData)) + LruBufTableShmemSize(max_page_no);
}

/*
 * Initialize CommitTs at system startup (postmaster start or standalone
 * backend)
 */
void
CommitTsShmemInit(void)
{
    bool        found;
    int         max_page_no = TransactionIdToCTsPage(MaxTransactionId);

    elog(LOG, "init committs shmem.");
    CommitTsCtl->PagePrecedes = CommitTsPagePrecedes;
    LruInit(CommitTsCtl, "commit_timestamp", CommitTsShmemBuffers(), TLOG_LSNS_PER_PAGE,  max_page_no, 
                  CommitTsControlLock, "pg_commit_ts",
                  LWTRANCHE_COMMITTS_BUFFERS);

    commitTsShared = ShmemInitStruct("CommitTs shared",
                                     sizeof(CommitTimestampShared),
                                     &found);
    elog(DEBUG10, "init committs mem finish.");
    if (!IsUnderPostmaster)
    {
        Assert(!found);

        commitTsShared->xidLastCommit = InvalidTransactionId;
        TIMESTAMP_NOBEGIN(commitTsShared->dataLastCommit.time);
        commitTsShared->dataLastCommit.nodeid = InvalidRepOriginId;
        commitTsShared->commitTsActive = false;
    }
    else
        Assert(found);
}

/*
 * This function must be called ONCE on system install.
 *
 * (The CommitTs directory is assumed to have been created by initdb, and
 * CommitTsShmemInit must have been called already.)
 */
void
BootStrapCommitTs(void)
{
    /*
     * Nothing to do here at present, unlike most other SLRU modules; segments
     * are created when the server is started with this module enabled. See
     * ActivateCommitTs.
     */
}

/*
 * Initialize (or reinitialize) a page of CommitTs to zeroes.
 * If writeXlog is TRUE, also emit an XLOG record saying we did this.
 *
 * The page is not actually written, just set up in shared memory.
 * The slot number of the new page is returned.
 *
 * Control lock must be held at entry, and will be held at exit.
 */
static int
ZeroCommitTsPage(int pageno, int partitionno, bool writeXlog)
{
    int            slotno;

    
    slotno = LruZeroPage(CommitTsCtl, partitionno, pageno);
    if(InRecovery)
        elog(DEBUG10, "zero commit page pageno %d partition %d slotno %d", pageno, partitionno, slotno);
    elog(DEBUG10, "zero commit page pageno %d partition %d slotno %d", pageno, partitionno, slotno);
    if (writeXlog)
        WriteZeroPageXlogRec(pageno);

    return slotno;
}

/*
 * This must be called ONCE during postmaster or standalone-backend startup,
 * after StartupXLOG has initialized ShmemVariableCache->nextXid.
 */
void
StartupCommitTs(void)
{
    elog(LOG, "startup commitTS.");
    ActivateCommitTs();
}


/*
 * This must be called ONCE during postmaster or standalone-backend startup,
 * after recovery has finished.
 */
void
CompleteCommitTsInitialization(void)
{
    /*
     * If the feature is not enabled, turn it off for good.  This also removes
     * any leftover data.
     *
     * Conversely, we activate the module if the feature is enabled.  This is
     * not necessary in a master system because we already did it earlier, but
     * if we're in a standby server that got promoted which had the feature
     * enabled and was following a master that had the feature disabled, this
     * is where we turn it on locally.
     */
#ifndef __SUPPORT_DISTRIBUTED_TRANSACTION__
    if (!track_commit_timestamp)
        DeactivateCommitTs();
    else
#endif
        ActivateCommitTs();
}

/*
 * Activate or deactivate CommitTs' upon reception of a XLOG_PARAMETER_CHANGE
 * XLog record in a standby.
 */
void
CommitTsParameterChange(bool newvalue, bool oldvalue)
{
    /*
     * If the commit_ts module is disabled in this server and we get word from
     * the master server that it is enabled there, activate it so that we can
     * replay future WAL records involving it; also mark it as active on
     * pg_control.  If the old value was already set, we already did this, so
     * don't do anything.
     *
     * If the module is disabled in the master, disable it here too, unless
     * the module is enabled locally.
     *
     * Note this only runs in the recovery process, so an unlocked read is
     * fine.
     */
#ifndef __SUPPORT_DISTRIBUTED_TRANSACTION__
    if (newvalue)
    {
        if (!commitTsShared->commitTsActive)
            ActivateCommitTs();
    }
    else if (commitTsShared->commitTsActive)
        DeactivateCommitTs();
#endif
}

/*
 * Activate this module whenever necessary.
 *        This must happen during postmaster or standalone-backend startup,
 *        or during WAL replay anytime the track_commit_timestamp setting is
 *        changed in the master.
 *
 * The reason why this SLRU needs separate activation/deactivation functions is
 * that it can be enabled/disabled during start and the activation/deactivation
 * on master is propagated to slave via replay. Other SLRUs don't have this
 * property and they can be just initialized during normal startup.
 *
 * This is in charge of creating the currently active segment, if it's not
 * already there.  The reason for this is that the server might have been
 * running with this module disabled for a while and thus might have skipped
 * the normal creation point.
 */
static void
ActivateCommitTs(void)
{
    TransactionId xid;
    int            pageno;
    int            slotno;
    int            partitionno;
    LWLock        *partitionlock;

    
    /* If we've done this already, there's nothing to do */
    LWLockAcquire(CommitTsLock, LW_EXCLUSIVE);
    if (commitTsShared->commitTsActive)
    {
        LWLockRelease(CommitTsLock);
        return;
    }
    LWLockRelease(CommitTsLock);
    elog(LOG, "activate commit ts.");

    xid = ShmemVariableCache->nextXid;
    pageno = TransactionIdToCTsPage(xid);
    partitionno = PagenoMappingPartitionno(CommitTsCtl, pageno);

    /*
     * Re-Initialize our idea of the latest page number.
     */
    LWLockAcquire(CommitTsControlLock, LW_EXCLUSIVE);
    CommitTsCtl->global_shared->latest_page_number = pageno;
    LWLockRelease(CommitTsControlLock);

    partitionlock = GetPartitionLock(CommitTsCtl, partitionno);
    LWLockAcquire(partitionlock, LW_EXCLUSIVE);
    CommitTsCtl->shared[partitionno]->latest_page_number = pageno;
    LWLockRelease(partitionlock);
    
    elog(LOG, "ActivateCommitTs: committs latest page number %d xid %d partition %d", pageno, xid, partitionno);

    /*
     * If CommitTs is enabled, but it wasn't in the previous server run, we
     * need to set the oldest and newest values to the next Xid; that way, we
     * will not try to read data that might not have been set.
     *
     * XXX does this have a problem if a server is started with commitTs
     * enabled, then started with commitTs disabled, then restarted with it
     * enabled again?  It doesn't look like it does, because there should be a
     * checkpoint that sets the value to InvalidTransactionId at end of
     * recovery; and so any chance of injecting new transactions without
     * CommitTs values would occur after the oldestCommitTsXid has been set to
     * Invalid temporarily.
     */
    LWLockAcquire(CommitTsLock, LW_EXCLUSIVE);
    if (ShmemVariableCache->oldestCommitTsXid == InvalidTransactionId)
    {
        ShmemVariableCache->oldestCommitTsXid =
            ShmemVariableCache->newestCommitTsXid = ReadNewTransactionId();
    }
    LWLockRelease(CommitTsLock);

    /* Create the current segment file, if necessary */
    if (!LruDoesPhysicalPageExist(CommitTsCtl, pageno))
    {

        LWLockAcquire(partitionlock, LW_EXCLUSIVE);
        
        slotno = ZeroCommitTsPage(pageno, partitionno, false);
        LruWritePage(CommitTsCtl, partitionno, slotno);
        Assert(!CommitTsCtl->shared[partitionno]->page_dirty[slotno]);
        
        LWLockRelease(partitionlock);
    }

    /* Change the activation status in shared memory. */
    LWLockAcquire(CommitTsLock, LW_EXCLUSIVE);
    commitTsShared->commitTsActive = true;
    LWLockRelease(CommitTsLock);
}

/*
 * This must be called ONCE at the end of startup/recovery.
 */
void
TrimCommitTs(void)
{
    TransactionId xid = ShmemVariableCache->nextXid;
    int            entryno = TransactionIdToCTsEntry(xid);
    int            pageno = TransactionIdToCTsPage(xid);
    int            partitionno;
    LWLock        *partitionLock;
    

    /*
     * Re-Initialize our idea of the latest page number.
     */
    LWLockAcquire(CommitTsControlLock, LW_EXCLUSIVE);
    CommitTsCtl->global_shared->latest_page_number = pageno;
    LWLockRelease(CommitTsControlLock);
    
	elog(DEBUG10, "Trim committs next xid %d latest page number %d entryno %d", xid, pageno, entryno);
    
    
    /*
     * Zero out the remainder of the current clog page.  Under normal
     * circumstances it should be zeroes already, but it seems at least
     * theoretically possible that XLOG replay will have settled on a nextXID
     * value that is less than the last XID actually used and marked by the
     * previous database lifecycle (since subtransaction commit writes clog
     * but makes no WAL entry).  Let's just be safe. (We need not worry about
     * pages beyond the current one, since those will be zeroed when first
     * used.  For the same reason, there is no need to do anything when
     * nextXid is exactly at a page boundary; and it's likely that the
     * "current" page doesn't exist yet in that case.)
     */
    partitionno = PagenoMappingPartitionno(CommitTsCtl, pageno);
    
    partitionLock = GetPartitionLock(CommitTsCtl, partitionno);

    LWLockAcquire(partitionLock, LW_EXCLUSIVE);
    
    if(entryno < COMMIT_TS_XACTS_PER_PAGE - 1)
    {
        int         slotno;
        int            byteno;
        char       *byteptr;

        /* lock is acquired by SimpleLruReadPage_ReadOnly */
        slotno = LruReadPage(CommitTsCtl, partitionno, pageno, false, xid);
        
        byteno = SizeOfCommitTimestampEntry * entryno + SizeOfCommitTimestampEntry;
        
        byteptr = CommitTsCtl->shared[partitionno]->page_buffer[slotno] + byteno;
        
		LruTlogDisableMemoryProtection(CommitTsCtl->shared[partitionno]->page_buffer[slotno]);
        /* Zero the rest of the page */
        MemSet(byteptr, 0, BLCKSZ - byteno);
		LruTlogEnableMemoryProtection(CommitTsCtl->shared[partitionno]->page_buffer[slotno]);

        elog(DEBUG10, "zero out the remaining page starting from byteno %d len BLCKSZ -byteno %d entryno %d sizeofentry %lu",
            byteno, BLCKSZ - byteno, entryno, SizeOfCommitTimestampEntry);
        CommitTsCtl->shared[partitionno]->page_dirty[slotno] = true;
        
        
    }

    CommitTsCtl->shared[partitionno]->latest_page_number = pageno;
    LWLockRelease(partitionLock);
    
}

#ifndef __SUPPORT_DISTRIBUTED_TRANSACTION__
/*
 * Deactivate this module.
 *
 * This must be called when the track_commit_timestamp parameter is turned off.
 * This happens during postmaster or standalone-backend startup, or during WAL
 * replay.
 *
 * Resets CommitTs into invalid state to make sure we don't hand back
 * possibly-invalid data; also removes segments of old data.
 */
static void
DeactivateCommitTs(void)
{
    /*
     * Cleanup the status in the shared memory.
     *
     * We reset everything in the commitTsShared record to prevent user from
     * getting confusing data about last committed transaction on the standby
     * when the module was activated repeatedly on the primary.
     */
    LWLockAcquire(CommitTsLock, LW_EXCLUSIVE);

    commitTsShared->commitTsActive = false;
    commitTsShared->xidLastCommit = InvalidTransactionId;
    TIMESTAMP_NOBEGIN(commitTsShared->dataLastCommit.time);
    commitTsShared->dataLastCommit.nodeid = InvalidRepOriginId;

    ShmemVariableCache->oldestCommitTsXid = InvalidTransactionId;
    ShmemVariableCache->newestCommitTsXid = InvalidTransactionId;

    LWLockRelease(CommitTsLock);

    /*
     * Remove *all* files.  This is necessary so that there are no leftover
     * files; in the case where this feature is later enabled after running
     * with it disabled for some time there may be a gap in the file sequence.
     * (We can probably tolerate out-of-sequence files, as they are going to
     * be overwritten anyway when we wrap around, but it seems better to be
     * tidy.)
     */
    LWLockAcquire(CommitTsControlLock, LW_EXCLUSIVE);
    (void) LruScanDirectory(CommitTsCtl, LruScanDirCbDeleteAll, NULL);
    LWLockRelease(CommitTsControlLock);
}

#endif
/*
 * This must be called ONCE during postmaster or standalone-backend shutdown
 */
void
ShutdownCommitTs(void)
{

    elog(LOG, "shutdown committs");
    /* Flush dirty CommitTs pages to disk */
    
    LruFlush(CommitTsCtl, false);
    

    /*
     * fsync pg_commit_ts to ensure that any files flushed previously are
     * durably on disk.
     */
    fsync_fname("pg_commit_ts", true);
}

/*
 * Perform a checkpoint --- either during shutdown, or on-the-fly
 */
void
CheckPointCommitTs(void)
{
    
    elog(DEBUG10, "checkpoint committs");
    
    /* Flush dirty CommitTs pages to disk */
    
    LruFlush(CommitTsCtl, true);
    

    /*
     * fsync pg_commit_ts to ensure that any files flushed previously are
     * durably on disk.
     */
    fsync_fname("pg_commit_ts", true);
}

/*
 * Make sure that CommitTs has room for a newly-allocated XID.
 *
 * NB: this is called while holding XidGenLock.  We want it to be very fast
 * most of the time; even when it's not so fast, no actual I/O need happen
 * unless we're forced to write out a dirty CommitTs or xlog page to make room
 * in shared memory.
 *
 * NB: the current implementation relies on track_commit_timestamp being
 * PGC_POSTMASTER.
 */

void
ExtendCommitTs(TransactionId newestXact)
{
    int         pageno;
    int         partitionno;
    LWLock         *partitionlock;

    /*
     * Nothing to do if module not enabled.  Note we do an unlocked read of
     * the flag here, which is okay because this routine is only called from
     * GetNewTransactionId, which is never called in a standby.
     */
    Assert(!InRecovery);
    if (!commitTsShared->commitTsActive)
        return;

    /*
     * No work except at first XID of a page.  But beware: just after
     * wraparound, the first XID of page zero is FirstNormalTransactionId.
     */
    if (TransactionIdToCTsEntry(newestXact) != 0 &&
        !TransactionIdEquals(newestXact, FirstNormalTransactionId))
        return;

    pageno = TransactionIdToCTsPage(newestXact);    

    partitionno = PagenoMappingPartitionno(CommitTsCtl, pageno);

    partitionlock = GetPartitionLock(CommitTsCtl, partitionno);
    
    
    LWLockAcquire(partitionlock, LW_EXCLUSIVE);
    LWLockAcquire(CommitTsCtl->global_shared->ControlLock, LW_EXCLUSIVE);
    
    elog(DEBUG10, "extend committs pageno %d partitionno %d", pageno, partitionno);
    ZeroCommitTsPage(pageno, partitionno, !InRecovery);

    LWLockRelease(CommitTsCtl->global_shared->ControlLock);
    LWLockRelease(partitionlock);

    

}


/*
 * Remove all CommitTs segments before the one holding the passed
 * transaction ID.
 *
 * Note that we don't need to flush XLOG here.
 */
void
TruncateCommitTs(TransactionId oldestXact)
{
    int            cutoffPage;

    /*
     * The cutoff point is the start of the segment containing oldestXact. We
     * pass the *page* containing oldestXact to SimpleLruTruncate.
     */
    cutoffPage = TransactionIdToCTsPage(oldestXact);

    /* Check to see if there's any files that could be removed */
    if (!LruScanDirectory(CommitTsCtl, LruScanDirCbReportPresence,
                           &cutoffPage))
        return;                    /* nothing to remove */

    /* Write XLOG record */
    WriteTruncateXlogRec(cutoffPage, oldestXact);

    
    elog(LOG, "truncate cutoffpage %d", cutoffPage);
    /* Now we can remove the old CommitTs segment(s) */
    LruTruncate(CommitTsCtl, cutoffPage);
}

/*
 * Set the limit values between which commit TS can be consulted.
 */
void
SetCommitTsLimit(TransactionId oldestXact, TransactionId newestXact)
{
    /*
     * Be careful not to overwrite values that are either further into the
     * "future" or signal a disabled committs.
     */
    LWLockAcquire(CommitTsLock, LW_EXCLUSIVE);
    if (ShmemVariableCache->oldestCommitTsXid != InvalidTransactionId)
    {
        if (TransactionIdPrecedes(ShmemVariableCache->oldestCommitTsXid, oldestXact))
            ShmemVariableCache->oldestCommitTsXid = oldestXact;
        if (TransactionIdPrecedes(newestXact, ShmemVariableCache->newestCommitTsXid))
            ShmemVariableCache->newestCommitTsXid = newestXact;
    }
    else
    {
        Assert(ShmemVariableCache->newestCommitTsXid == InvalidTransactionId);
        ShmemVariableCache->oldestCommitTsXid = oldestXact;
        ShmemVariableCache->newestCommitTsXid = newestXact;
    }
    LWLockRelease(CommitTsLock);
}

/*
 * Move forwards the oldest commitTS value that can be consulted
 */
void
AdvanceOldestCommitTsXid(TransactionId oldestXact)
{
    LWLockAcquire(CommitTsLock, LW_EXCLUSIVE);
    if (ShmemVariableCache->oldestCommitTsXid != InvalidTransactionId &&
        TransactionIdPrecedes(ShmemVariableCache->oldestCommitTsXid, oldestXact))
        ShmemVariableCache->oldestCommitTsXid = oldestXact;
    LWLockRelease(CommitTsLock);
}


/*
 * Decide which of two CLOG page numbers is "older" for truncation purposes.
 *
 * We need to use comparison of TransactionIds here in order to do the right
 * thing with wraparound XID arithmetic.  However, if we are asked about
 * page number zero, we don't want to hand InvalidTransactionId to
 * TransactionIdPrecedes: it'll get weird about permanent xact IDs.  So,
 * offset both xids by FirstNormalTransactionId to avoid that.
 */
static bool
CommitTsPagePrecedes(int page1, int page2)
{
    TransactionId xid1;
    TransactionId xid2;

    xid1 = ((TransactionId) page1) * COMMIT_TS_XACTS_PER_PAGE;
    xid1 += FirstNormalTransactionId;
    xid2 = ((TransactionId) page2) * COMMIT_TS_XACTS_PER_PAGE;
    xid2 += FirstNormalTransactionId;

    return TransactionIdPrecedes(xid1, xid2);
}


/*
 * Write a ZEROPAGE xlog record
 */
static void
WriteZeroPageXlogRec(int pageno)
{
    XLogBeginInsert();
    XLogRegisterData((char *) (&pageno), sizeof(int));
    (void) XLogInsert(RM_COMMIT_TS_ID, COMMIT_TS_ZEROPAGE);
}

/*
 * Write a TRUNCATE xlog record
 */
static void
WriteTruncateXlogRec(int pageno, TransactionId oldestXid)
{
    xl_commit_ts_truncate xlrec;

    xlrec.pageno = pageno;
    xlrec.oldestXid = oldestXid;

    XLogBeginInsert();
    XLogRegisterData((char *) (&xlrec), SizeOfCommitTsTruncate);
    (void) XLogInsert(RM_COMMIT_TS_ID, COMMIT_TS_TRUNCATE);
}

/*
 * Write a SETTS xlog record
 */
static void
WriteSetTimestampXlogRec(TransactionId mainxid, int nsubxids,
                         TransactionId *subxids, TimestampTz global_timestamp, TimestampTz timestamp,
                         RepOriginId nodeid)
{
	xl_commit_ts_set record;

	record.global_timestamp = global_timestamp;
	record.timestamp = timestamp;
	record.nodeid = nodeid;
	record.mainxid = mainxid;

	XLogBeginInsert();
	XLogRegisterData((char *) &record,
					 offsetof(xl_commit_ts_set, mainxid) +
					 sizeof(TransactionId));
	XLogRegisterData((char *) subxids, nsubxids * sizeof(TransactionId));
	XLogInsert(RM_COMMIT_TS_ID, COMMIT_TS_SETTS);

	if (enable_committs_print)
	{
		elog(LOG,
			"WriteSetTimestampXlogRec: mainxid %d timestamp "INT64_FORMAT" global_timestamp "INT64_FORMAT,
			mainxid, timestamp, global_timestamp);
	}
}

/*
 * CommitTS resource manager's routines
 */
void
commit_ts_redo(XLogReaderState *record)
{
    uint8        info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    elog(LOG, "redo committs.");
    /* Backup blocks are not used in commit_ts records */
    Assert(!XLogRecHasAnyBlockRefs(record));

    if (info == COMMIT_TS_ZEROPAGE)
    {
        int            pageno;
        int            slotno;
        int            partitionno;
        LWLock        *partitionlock;
        
        memcpy(&pageno, XLogRecGetData(record), sizeof(int));
        partitionno = PagenoMappingPartitionno(CommitTsCtl, pageno);
        
        partitionlock = GetPartitionLock(CommitTsCtl, partitionno);
        
        LWLockAcquire(partitionlock, LW_EXCLUSIVE);
        elog(LOG, "redo committs: zero partitionno %d pageno %d", partitionno, pageno);
        slotno = ZeroCommitTsPage(pageno, partitionno, false);
        LruWritePage(CommitTsCtl, partitionno, slotno);
        Assert(!CommitTsCtl->shared[partitionno]->page_dirty[slotno]);

        LWLockRelease(partitionlock);
    }
    else if (info == COMMIT_TS_TRUNCATE)
    {
        int partitionno;
        
        xl_commit_ts_truncate *trunc = (xl_commit_ts_truncate *) XLogRecGetData(record);

        AdvanceOldestCommitTsXid(trunc->oldestXid);

        /*
         * During XLOG replay, latest_page_number isn't set up yet; insert a
         * suitable value to bypass the sanity test in SimpleLruTruncate.
         */
        partitionno = PagenoMappingPartitionno(CommitTsCtl, trunc->pageno);
        
        CommitTsCtl->shared[partitionno]->latest_page_number = trunc->pageno;
        
        CommitTsCtl->global_shared->latest_page_number = trunc->pageno;
        
        elog(LOG, "redo committs: truncate latest page number %d", trunc->pageno);
        
        LruTruncate(CommitTsCtl, trunc->pageno);
    }
    else if (info == COMMIT_TS_SETTS)
    {
        xl_commit_ts_set *setts = (xl_commit_ts_set *) XLogRecGetData(record);
        int            nsubxids;
        TransactionId *subxids;

        nsubxids = ((XLogRecGetDataLen(record) - SizeOfCommitTsSet) /
                    sizeof(TransactionId));
        if (nsubxids > 0)
        {
            subxids = palloc(sizeof(TransactionId) * nsubxids);
            memcpy(subxids,
                   XLogRecGetData(record) + SizeOfCommitTsSet,
                   sizeof(TransactionId) * nsubxids);
        }
        else
            subxids = NULL;

        TransactionTreeSetCommitTsData(setts->mainxid, nsubxids, subxids,
                                       setts->global_timestamp ,setts->timestamp, setts->nodeid, true, InvalidXLogRecPtr);
        elog(LOG, "commit_ts_redo: set ts xid %d " INT64_FORMAT, setts->mainxid, setts->timestamp);
        if (subxids)
            pfree(subxids);
    }
    else
        elog(PANIC, "commit_ts_redo: unknown op code %u", info);
}
