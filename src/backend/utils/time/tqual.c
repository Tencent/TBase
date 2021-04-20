/*-------------------------------------------------------------------------
 *
 * tqual.c
 *      POSTGRES "time qualification" code, ie, tuple visibility rules.
 *
 * NOTE: all the HeapTupleSatisfies routines will update the tuple's
 * "hint" status bits if we see that the inserting or deleting transaction
 * has now committed or aborted (and it is safe to set the hint bits).
 * If the hint bits are changed, MarkBufferDirtyHint is called on
 * the passed-in buffer.  The caller must hold not only a pin, but at least
 * shared buffer content lock on the buffer containing the tuple.
 *
 * NOTE: When using a non-MVCC snapshot, we must check
 * TransactionIdIsInProgress (which looks in the PGXACT array)
 * before TransactionIdDidCommit/TransactionIdDidAbort (which look in
 * pg_xact).  Otherwise we have a race condition: we might decide that a
 * just-committed transaction crashed, because none of the tests succeed.
 * xact.c is careful to record commit/abort in pg_xact before it unsets
 * MyPgXact->xid in the PGXACT array.  That fixes that problem, but it
 * also means there is a window where TransactionIdIsInProgress and
 * TransactionIdDidCommit will both return true.  If we check only
 * TransactionIdDidCommit, we could consider a tuple committed when a
 * later GetSnapshotData call will still think the originating transaction
 * is in progress, which leads to application-level inconsistency.  The
 * upshot is that we gotta check TransactionIdIsInProgress first in all
 * code paths, except for a few cases where we are looking at
 * subtransactions of our own main transaction and so there can't be any
 * race condition.
 *
 * When using an MVCC snapshot, we rely on XidInMVCCSnapshot rather than
 * TransactionIdIsInProgress, but the logic is otherwise the same: do not
 * check pg_xact until after deciding that the xact is no longer in progress.
 *
 *
 * Summary of visibility functions:
 *
 *     HeapTupleSatisfiesMVCC()
 *          visible to supplied snapshot, excludes current command
 *     HeapTupleSatisfiesUpdate()
 *          visible to instant snapshot, with user-supplied command
 *          counter and more complex result
 *     HeapTupleSatisfiesSelf()
 *          visible to instant snapshot and current command
 *     HeapTupleSatisfiesDirty()
 *          like HeapTupleSatisfiesSelf(), but includes open transactions
 *     HeapTupleSatisfiesVacuum()
 *          visible to any running transaction, used by VACUUM
 *     HeapTupleSatisfiesToast()
 *          visible unless part of interrupted vacuum, used for TOAST
 *     HeapTupleSatisfiesAny()
 *          all tuples are visible
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *      src/backend/utils/time/tqual.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "storage/bufmgr.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/combocid.h"
#include "utils/snapmgr.h"
#include "utils/tqual.h"
#include "access/commit_ts.h"
#include "storage/lmgr.h"
#include "storage/buf_internals.h"
#include "miscadmin.h"
#include "postmaster/autovacuum.h"
#include "access/htup_details.h"


/* Static variables representing various special snapshot semantics */
SnapshotData SnapshotSelfData = {HeapTupleSatisfiesSelf};
SnapshotData SnapshotAnyData = {HeapTupleSatisfiesAny};

#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
static bool XidInMVCCSnapshotDistri(HeapTupleHeader tuple, TransactionId xid,
									Snapshot snapshot, Buffer buffer,
									bool *need_retry, uint16 infomask);
static bool XidInMVCCSnapshot(TransactionId xid, Snapshot snapshot);

/* Debugging.... */

#ifdef DIST_TXN_DEBUG
#define DEBUG_MVCC_XMIN(state, msg) \
	if(enable_distri_visibility_print && \
	   TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple))) \
	{ \
		elog(LOG, "MVCC ts " INT64_FORMAT " %s xmin %d %s.", \
				state? "true":"false", snapshot->start_ts, \
				HeapTupleHeaderGetRawXmin(tuple), msg); \
	}
#else
#define DEBUG_MVCC_XMIN(state, msg) \
	((void) 0)
#endif

#ifdef DIST_TXN_DEBUG
#define DEBUG_MVCC_XMINXMAX(state, xmax, msg) \
	if(enable_distri_visibility_print && \
	   TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple))) \
	{ \
		elog(LOG, "MVCC ts " INT64_FORMAT " %s xmin %d xmax %d %s.", \
				state? "true":"false", snapshot->start_ts, \
				HeapTupleHeaderGetRawXmin(tuple), xmax, msg); \
	}
#else
#define DEBUG_MVCC_XMINXMAX(state, xmax, msg) \
	((void) 0)
#endif

#ifdef DIST_TXN_DEBUG
#define DEBUG_SNAPSHOT(A) \
	do { \
		int	_debug_snapshot_save_errno = errno; \
		if (enable_distri_visibility_print) \
		{ \
		    A; \
		} \
		errno = _debug_snapshot_save_errno; \
	} while (0)
#else
#define DEBUG_SNAPSHOT(A) \
	((void) 0)
#endif

#define DEBUG_INCREASE_VISIBLE_TUPLE \
	if(enable_distri_debug) \
	{ \
		snapshot->number_visible_tuples++; \
	}

#ifdef __SNAPSHOT_CHECK__
static bool SnapshotCheck(TransactionId xid, Snapshot snapshot, int target_res, GlobalTimestamp target_committs);
#else
#define SnapshotCheck(xid, snapshot, target_res, target_committs)
#endif

#endif //  __SUPPORT_DISTRIBUTED_TRANSACTION__

/*
#ifdef _MIGRATE_
SnapshotData SnapshotNowData = {HeapTupleSatisfiesNow};
#endif
*/

#ifdef _MIGRATE_
int g_ShardVisibleMode = SHARD_VISIBLE_MODE_VISIBLE;
#endif


/* local functions */
static bool XidInMVCCSnapshot(TransactionId xid, Snapshot snapshot);


GlobalTimestamp HeapTupleHderGetXminTimestapAtomic(HeapTupleHeader tuple)
{
	if (HEAP_XMIN_TIMESTAMP_IS_UPDATED(tuple->t_infomask2))
	{
		return HeapTupleHeaderGetXminTimestamp(tuple);
	}
	else
	{
		return InvalidGlobalTimestamp;
	}
}

GlobalTimestamp HeapTupleHderGetXmaxTimestapAtomic(HeapTupleHeader tuple)
{
	if (HEAP_XMAX_TIMESTAMP_IS_UPDATED(tuple->t_infomask2))
	{
		return HeapTupleHeaderGetXmaxTimestamp(tuple);
	}
	else
	{
		return InvalidGlobalTimestamp;
	}
}

void HeapTupleHderSetXminTimestapAtomic(HeapTupleHeader tuple, GlobalTimestamp committs)
{
	HeapTupleHeaderSetXminTimestamp(tuple, committs);
	tuple->t_infomask2 |= HEAP_XMIN_TIMESTAMP_UPDATED;
}

void HeapTupleHderSetXmaxTimestapAtomic(HeapTupleHeader tuple, GlobalTimestamp committs)
{
	HeapTupleHeaderSetXmaxTimestamp(tuple, committs);
	tuple->t_infomask2 |= HEAP_XMAX_TIMESTAMP_UPDATED;
}


/*
 * SetHintBits()
 *
 * Set commit/abort hint bits on a tuple, if appropriate at this time.
 *
 * It is only safe to set a transaction-committed hint bit if we know the
 * transaction's commit record is guaranteed to be flushed to disk before the
 * buffer, or if the table is temporary or unlogged and will be obliterated by
 * a crash anyway.  We cannot change the LSN of the page here, because we may
 * hold only a share lock on the buffer, so we can only use the LSN to
 * interlock this if the buffer's LSN already is newer than the commit LSN;
 * otherwise we have to just refrain from setting the hint bit until some
 * future re-examination of the tuple.
 *
 * We can always set hint bits when marking a transaction aborted.  (Some
 * code in heapam.c relies on that!)
 *
 * Also, if we are cleaning up HEAP_MOVED_IN or HEAP_MOVED_OFF entries, then
 * we can always set the hint bits, since pre-9.0 VACUUM FULL always used
 * synchronous commits and didn't move tuples that weren't previously
 * hinted.  (This is not known by this subroutine, but is applied by its
 * callers.)  Note: old-style VACUUM FULL is gone, but we have to keep this
 * module's support for MOVED_OFF/MOVED_IN flag bits for as long as we
 * support in-place update from pre-9.0 databases.
 *
 * Normal commits may be asynchronous, so for those we need to get the LSN
 * of the transaction and then check whether this is flushed.
 *
 * The caller should pass xid as the XID of the transaction to check, or
 * InvalidTransactionId if no check is needed.
 */
static inline void
SetHintBits(HeapTupleHeader tuple, Buffer buffer,
            uint16 infomask, TransactionId xid)
{// #lizard forgives
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
    TransactionId xmin;
    TransactionId xmax;
    GlobalTimestamp global_timestamp;
#endif

	BufferDesc *buf = NULL;
	bool mprotect = false;

    if (TransactionIdIsValid(xid))
    {
        /* NB: xid must be known committed here! */
        XLogRecPtr    commitLSN = TransactionIdGetCommitLSN(xid);

        if (BufferIsPermanent(buffer) && XLogNeedsFlush(commitLSN) &&
            BufferGetLSNAtomic(buffer) < commitLSN)
        {
            /* not flushed and no LSN interlock, so don't set hint */
            return;
        }
    }
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
	/*
	 * BUFFER_LOCK_EXCLUSIVE has made the buffer writable, but BUFFER_LOCK_SHARED
	 * does not, so it has to be set to be writable.
	 *
	 * After setting GTS, it needs to set the memory protection again.
	 */
	buf = GetBufferDescriptor(buffer - 1);
	mprotect = enable_buffer_mprotect &&
		LWLockHeldByMeInMode(BufferDescriptorGetContentLock(buf), LW_SHARED);
	if (mprotect)
	{
		BufDisableMemoryProtection(BufferGetPage(buffer), false);
	}
    if(infomask & HEAP_XMIN_COMMITTED)
    {

        if(!GlobalTimestampIsValid(HeapTupleHderGetXminTimestapAtomic(tuple)))
        {
            xmin = HeapTupleHeaderGetRawXmin(tuple);
            
            if(TransactionIdIsNormal(xmin) && TransactionIdGetCommitTsData(xmin, &global_timestamp, NULL))
            {
            	HeapTupleHderSetXminTimestapAtomic(tuple, global_timestamp);
                if (enable_committs_print)
				{
					BufferDesc *bufHdr = GetBufferDescriptor(buffer - 1);
					RelFileNode *rnode = &bufHdr->tag.rnode;

					elog(LOG,
						"SetHintBits: relfilenode %u pageno %u "
						"CTID %hu/%hu/%hu "
						"infomask %d xmin %u xmin_gts "INT64_FORMAT,
						rnode->relNode, bufHdr->tag.blockNum,
						tuple->t_ctid.ip_blkid.bi_hi,
						tuple->t_ctid.ip_blkid.bi_lo,
						tuple->t_ctid.ip_posid,
						tuple->t_infomask, xmin, global_timestamp);
				}
            }
        }

    }
    else if(infomask & HEAP_XMAX_COMMITTED)
    {
        if(!GlobalTimestampIsValid(HeapTupleHderGetXmaxTimestapAtomic(tuple)))
        {
            xmax = HeapTupleHeaderGetRawXmax(tuple);
            if(TransactionIdIsNormal(xmax) && TransactionIdGetCommitTsData(xmax, &global_timestamp, NULL))
            {
            	HeapTupleHderSetXmaxTimestapAtomic(tuple, global_timestamp);
                if (enable_committs_print)
				{
					BufferDesc *bufHdr = GetBufferDescriptor(buffer - 1);
					RelFileNode *rnode = &bufHdr->tag.rnode;

					elog(LOG,
						"SetHintBits: relfilenode %u pageno %u "
						"CTID %hu/%hu/%hu "
						"infomask %d multixact %d "
						"xid %u xmax %u xmax_gts "INT64_FORMAT,
						rnode->relNode, bufHdr->tag.blockNum,
						tuple->t_ctid.ip_blkid.bi_hi,
						tuple->t_ctid.ip_blkid.bi_lo,
						tuple->t_ctid.ip_posid,
						tuple->t_infomask, tuple->t_infomask & HEAP_XMAX_IS_MULTI,
						HeapTupleHeaderGetUpdateXid(tuple), xmax, global_timestamp);
				}
            }
        }
    }
        
#endif

    tuple->t_infomask |= infomask;
    MarkBufferDirtyHint(buffer, true);

	if (mprotect)
	{
		BufEnableMemoryProtection(BufferGetPage(buffer), false);
	}
}


static inline void SetTimestamp (HeapTupleHeader tuple, TransactionId xid, Buffer buffer,
                            uint16 infomask)
{

    if(infomask & HEAP_XMIN_COMMITTED)
    {
        
        if(!GlobalTimestampIsValid(HeapTupleHderGetXminTimestapAtomic(tuple)))
        {
            SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, xid);
        }
            
    }
    else if(infomask & HEAP_XMAX_COMMITTED)
    {
        if(!GlobalTimestampIsValid(HeapTupleHderGetXmaxTimestapAtomic(tuple)))
        {
            SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED, xid);
        }
    }

}

/*
 * HeapTupleSetHintBits --- exported version of SetHintBits()
 *
 * This must be separate because of C99's brain-dead notions about how to
 * implement inline functions.
 */
void
HeapTupleSetHintBits(HeapTupleHeader tuple, Buffer buffer,
                     uint16 infomask, TransactionId xid)
{
    SetHintBits(tuple, buffer, infomask, xid);
}


/*
 * HeapTupleSatisfiesSelf
 *        True iff heap tuple is valid "for itself".
 *
 *    Here, we consider the effects of:
 *        all committed transactions (as of the current instant)
 *        previous commands of this transaction
 *        changes made by the current command
 *
 * Note:
 *        Assumes heap tuple is valid.
 *
 * The satisfaction of "itself" requires the following:
 *
 * ((Xmin == my-transaction &&                the row was updated by the current transaction, and
 *        (Xmax is null                        it was not deleted
 *         [|| Xmax != my-transaction)])            [or it was deleted by another transaction]
 * ||
 *
 * (Xmin is committed &&                    the row was modified by a committed transaction, and
 *        (Xmax is null ||                    the row has not been deleted, or
 *            (Xmax != my-transaction &&            the row was deleted by another transaction
 *             Xmax is not committed)))            that has not been committed
 */
bool
HeapTupleSatisfiesSelf(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{// #lizard forgives
    HeapTupleHeader tuple = htup->t_data;

#ifdef _MIGRATE_
        
        if(IS_PGXC_DATANODE && ShardIDIsValid(tuple->t_shardid) && SnapshotGetShardTable(snapshot))
        {
            bool shard_is_visible = bms_is_member(tuple->t_shardid/snapshot->groupsize,
                                                    SnapshotGetShardTable(snapshot));
    
            if(!IsConnFromApp())
            {
                if(!shard_is_visible)
                    return false;
            }
            else if(g_ShardVisibleMode != SHARD_VISIBLE_MODE_ALL)
            {
                if((!shard_is_visible && g_ShardVisibleMode == SHARD_VISIBLE_MODE_VISIBLE)
                    || (shard_is_visible && g_ShardVisibleMode == SHARD_VISIBLE_MODE_HIDDEN))
                {
                    return false;
                }
            }
        }
#endif

    Assert(ItemPointerIsValid(&htup->t_self));
    Assert(htup->t_tableOid != InvalidOid);

    if (!HeapTupleHeaderXminCommitted(tuple))
    {
        if (HeapTupleHeaderXminInvalid(tuple))
            return false;

        /* Used by pre-9.0 binary upgrades */
        if (tuple->t_infomask & HEAP_MOVED_OFF)
        {
            TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

            if (TransactionIdIsCurrentTransactionId(xvac))
                return false;
            if (!TransactionIdIsInProgress(xvac))
            {
                if (TransactionIdDidCommit(xvac))
                {
                    SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
                                InvalidTransactionId);
                    return false;
                }
                SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
                            InvalidTransactionId);
            }
        }
        /* Used by pre-9.0 binary upgrades */
        else if (tuple->t_infomask & HEAP_MOVED_IN)
        {
            TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

            if (!TransactionIdIsCurrentTransactionId(xvac))
            {
                if (TransactionIdIsInProgress(xvac))
                    return false;
                if (TransactionIdDidCommit(xvac))
                    SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
                                InvalidTransactionId);
                else
                {
                    SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
                                InvalidTransactionId);
                    return false;
                }
            }
        }
        else if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)))
        {
            if (tuple->t_infomask & HEAP_XMAX_INVALID)    /* xid invalid */
                return true;

            if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))    /* not deleter */
                return true;

            if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
            {
                TransactionId xmax;

                xmax = HeapTupleGetUpdateXid(tuple);

                /* not LOCKED_ONLY, so it has to have an xmax */
                Assert(TransactionIdIsValid(xmax));

                /* updating subtransaction must have aborted */
                if (!TransactionIdIsCurrentTransactionId(xmax))
                    return true;
                else
                    return false;
            }

            if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
            {
                /* deleting subtransaction must have aborted */
                SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
                            InvalidTransactionId);
                return true;
            }

            return false;
        }
        else if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmin(tuple)))
            return false;
        else if (TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple)))
            SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
                        HeapTupleHeaderGetRawXmin(tuple));
        else
        {
            /* it must have aborted or crashed */
            SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
                        InvalidTransactionId);
            return false;
        }
    }

    /* by here, the inserting transaction has committed */

    if (tuple->t_infomask & HEAP_XMAX_INVALID)    /* xid invalid or aborted */
        return true;

    if (tuple->t_infomask & HEAP_XMAX_COMMITTED)
    {
        if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
            return true;
        return false;            /* updated by other */
    }

    if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
    {
        TransactionId xmax;

        if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
            return true;

        xmax = HeapTupleGetUpdateXid(tuple);

        /* not LOCKED_ONLY, so it has to have an xmax */
        Assert(TransactionIdIsValid(xmax));

        if (TransactionIdIsCurrentTransactionId(xmax))
            return false;
        if (TransactionIdIsInProgress(xmax))
            return true;
        if (TransactionIdDidCommit(xmax))
            return false;
        /* it must have aborted or crashed */
        return true;
    }

    if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
    {
        if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
            return true;
        return false;
    }

    if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmax(tuple)))
        return true;

    if (!TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple)))
    {
        /* it must have aborted or crashed */
        SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
                    InvalidTransactionId);
        return true;
    }

    /* xmax transaction committed */

    if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
    {
        SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
                    InvalidTransactionId);
        return true;
    }

    SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED,
                HeapTupleHeaderGetRawXmax(tuple));
    return false;
}

/*
 * HeapTupleSatisfiesAny
 *        Dummy "satisfies" routine: any tuple satisfies SnapshotAny.
 */
bool
HeapTupleSatisfiesAny(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{
    return true;
}

/*
 * HeapTupleSatisfiesToast
 *        True iff heap tuple is valid as a TOAST row.
 *
 * This is a simplified version that only checks for VACUUM moving conditions.
 * It's appropriate for TOAST usage because TOAST really doesn't want to do
 * its own time qual checks; if you can see the main table row that contains
 * a TOAST reference, you should be able to see the TOASTed value.  However,
 * vacuuming a TOAST table is independent of the main table, and in case such
 * a vacuum fails partway through, we'd better do this much checking.
 *
 * Among other things, this means you can't do UPDATEs of rows in a TOAST
 * table.
 */
bool
HeapTupleSatisfiesToast(HeapTuple htup, Snapshot snapshot,
                        Buffer buffer)
{// #lizard forgives
    HeapTupleHeader tuple = htup->t_data;

    Assert(ItemPointerIsValid(&htup->t_self));
    Assert(htup->t_tableOid != InvalidOid);

    if (!HeapTupleHeaderXminCommitted(tuple))
    {
        if (HeapTupleHeaderXminInvalid(tuple))
            return false;

        /* Used by pre-9.0 binary upgrades */
        if (tuple->t_infomask & HEAP_MOVED_OFF)
        {
            TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

            if (TransactionIdIsCurrentTransactionId(xvac))
                return false;
            if (!TransactionIdIsInProgress(xvac))
            {
                if (TransactionIdDidCommit(xvac))
                {
                    SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
                                InvalidTransactionId);
                    return false;
                }
                SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
                            InvalidTransactionId);
            }
        }
        /* Used by pre-9.0 binary upgrades */
        else if (tuple->t_infomask & HEAP_MOVED_IN)
        {
            TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

            if (!TransactionIdIsCurrentTransactionId(xvac))
            {
                if (TransactionIdIsInProgress(xvac))
                    return false;
                if (TransactionIdDidCommit(xvac))
                    SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
                                InvalidTransactionId);
                else
                {
                    SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
                                InvalidTransactionId);
                    return false;
                }
            }
        }

        /*
         * An invalid Xmin can be left behind by a speculative insertion that
         * is canceled by super-deleting the tuple.  This also applies to
         * TOAST tuples created during speculative insertion.
         */
        else if (!TransactionIdIsValid(HeapTupleHeaderGetXmin(tuple)))
            return false;
    }

    /* otherwise assume the tuple is valid for TOAST. */
    return true;
}

/*
 * HeapTupleSatisfiesUpdate
 *
 *    This function returns a more detailed result code than most of the
 *    functions in this file, since UPDATE needs to know more than "is it
 *    visible?".  It also allows for user-supplied CommandId rather than
 *    relying on CurrentCommandId.
 *
 *    The possible return codes are:
 *
 *    HeapTupleInvisible: the tuple didn't exist at all when the scan started,
 *    e.g. it was created by a later CommandId.
 *
 *    HeapTupleMayBeUpdated: The tuple is valid and visible, so it may be
 *    updated.
 *
 *    HeapTupleSelfUpdated: The tuple was updated by the current transaction,
 *    after the current scan started.
 *
 *    HeapTupleUpdated: The tuple was updated by a committed transaction.
 *
 *    HeapTupleBeingUpdated: The tuple is being updated by an in-progress
 *    transaction other than the current transaction.  (Note: this includes
 *    the case where the tuple is share-locked by a MultiXact, even if the
 *    MultiXact includes the current transaction.  Callers that want to
 *    distinguish that case must test for it themselves.)
 */
HTSU_Result
HeapTupleSatisfiesUpdate(HeapTuple htup, CommandId curcid,
                         Buffer buffer)
{// #lizard forgives
    HeapTupleHeader tuple = htup->t_data;

    Assert(ItemPointerIsValid(&htup->t_self));
    Assert(htup->t_tableOid != InvalidOid);

    if (!HeapTupleHeaderXminCommitted(tuple))
    {
        if (HeapTupleHeaderXminInvalid(tuple))
            return HeapTupleInvisible;

        /* Used by pre-9.0 binary upgrades */
        if (tuple->t_infomask & HEAP_MOVED_OFF)
        {
            TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

            if (TransactionIdIsCurrentTransactionId(xvac))
                return HeapTupleInvisible;
            if (!TransactionIdIsInProgress(xvac))
            {
                if (TransactionIdDidCommit(xvac))
                {
                    SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
                                InvalidTransactionId);
                    return HeapTupleInvisible;
                }
                SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
                            InvalidTransactionId);
            }
        }
        /* Used by pre-9.0 binary upgrades */
        else if (tuple->t_infomask & HEAP_MOVED_IN)
        {
            TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

            if (!TransactionIdIsCurrentTransactionId(xvac))
            {
                if (TransactionIdIsInProgress(xvac))
                    return HeapTupleInvisible;
                if (TransactionIdDidCommit(xvac))
                    SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
                                InvalidTransactionId);
                else
                {
                    SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
                                InvalidTransactionId);
                    return HeapTupleInvisible;
                }
            }
        }
        else if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)))
        {
            if (HeapTupleHeaderGetCmin(tuple) >= curcid)
                return HeapTupleInvisible;    /* inserted after scan started */

            if (tuple->t_infomask & HEAP_XMAX_INVALID)    /* xid invalid */
                return HeapTupleMayBeUpdated;

            if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
            {
                TransactionId xmax;

                xmax = HeapTupleHeaderGetRawXmax(tuple);

                /*
                 * Careful here: even though this tuple was created by our own
                 * transaction, it might be locked by other transactions, if
                 * the original version was key-share locked when we updated
                 * it.
                 */

                if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
                {
                    if (MultiXactIdIsRunning(xmax, true))
                        return HeapTupleBeingUpdated;
                    else
                        return HeapTupleMayBeUpdated;
                }

                /*
                 * If the locker is gone, then there is nothing of interest
                 * left in this Xmax; otherwise, report the tuple as
                 * locked/updated.
                 */
                if (!TransactionIdIsInProgress(xmax))
                    return HeapTupleMayBeUpdated;
                return HeapTupleBeingUpdated;
            }

            if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
            {
                TransactionId xmax;

                xmax = HeapTupleGetUpdateXid(tuple);

                /* not LOCKED_ONLY, so it has to have an xmax */
                Assert(TransactionIdIsValid(xmax));

                /* deleting subtransaction must have aborted */
                if (!TransactionIdIsCurrentTransactionId(xmax))
                {
                    if (MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple),
                                             false))
                        return HeapTupleBeingUpdated;
                    return HeapTupleMayBeUpdated;
                }
                else
                {
                    if (HeapTupleHeaderGetCmax(tuple) >= curcid)
                        return HeapTupleSelfUpdated;    /* updated after scan
                                                         * started */
                    else
                        return HeapTupleInvisible;    /* updated before scan
                                                     * started */
                }
            }

            if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
            {
                /* deleting subtransaction must have aborted */
                SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
                            InvalidTransactionId);
                return HeapTupleMayBeUpdated;
            }

            if (HeapTupleHeaderGetCmax(tuple) >= curcid)
                return HeapTupleSelfUpdated;    /* updated after scan started */
            else
                return HeapTupleInvisible;    /* updated before scan started */
        }
        else if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmin(tuple)))
            return HeapTupleInvisible;
        else if (TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple)))
            SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
                        HeapTupleHeaderGetRawXmin(tuple));
        else
        {
            /* it must have aborted or crashed */
            SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
                        InvalidTransactionId);
            return HeapTupleInvisible;
        }
    }

    /* by here, the inserting transaction has committed */

    /*
     * If the committed xmin is a relatively recent transaction, we want to
     * make sure that the GTM sees its commit before it sees our
     * commit since our execution assumes that xmin is committed and hence that
     * ordering must be followed. There is a small race condition which may
     * violate this ordering and hence we record such dependencies and ensure
     * ordering at the commit time
     */
    if (TransactionIdPrecedesOrEquals(RecentXmin, HeapTupleHeaderGetRawXmin(tuple)))
        TransactionRecordXidWait(HeapTupleHeaderGetRawXmin(tuple));

    if (tuple->t_infomask & HEAP_XMAX_INVALID)    /* xid invalid or aborted */
        return HeapTupleMayBeUpdated;

    if (tuple->t_infomask & HEAP_XMAX_COMMITTED)
    {
        if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
            return HeapTupleMayBeUpdated;
        return HeapTupleUpdated;    /* updated by other */
    }

    if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
    {
        TransactionId xmax;

        if (HEAP_LOCKED_UPGRADED(tuple->t_infomask))
            return HeapTupleMayBeUpdated;

        if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
        {
            if (MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), true))
                return HeapTupleBeingUpdated;

            SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
            return HeapTupleMayBeUpdated;
        }

        xmax = HeapTupleGetUpdateXid(tuple);
        if (!TransactionIdIsValid(xmax))
        {
            if (MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), false))
                return HeapTupleBeingUpdated;
        }

        /* not LOCKED_ONLY, so it has to have an xmax */
        Assert(TransactionIdIsValid(xmax));

        if (TransactionIdIsCurrentTransactionId(xmax))
        {
            if (HeapTupleHeaderGetCmax(tuple) >= curcid)
                return HeapTupleSelfUpdated;    /* updated after scan started */
            else
                return HeapTupleInvisible;    /* updated before scan started */
        }

        if (MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), false))
            return HeapTupleBeingUpdated;

        if (TransactionIdDidCommit(xmax))
            return HeapTupleUpdated;

        /*
         * By here, the update in the Xmax is either aborted or crashed, but
         * what about the other members?
         */

        if (!MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), false))
        {
            /*
             * There's no member, even just a locker, alive anymore, so we can
             * mark the Xmax as invalid.
             */
            SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
                        InvalidTransactionId);
            return HeapTupleMayBeUpdated;
        }
        else
        {
            /* There are lockers running */
            return HeapTupleBeingUpdated;
        }
    }

    if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
    {
        if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
            return HeapTupleBeingUpdated;
        if (HeapTupleHeaderGetCmax(tuple) >= curcid)
            return HeapTupleSelfUpdated;    /* updated after scan started */
        else
            return HeapTupleInvisible;    /* updated before scan started */
    }

    if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmax(tuple)))
        return HeapTupleBeingUpdated;

    if (!TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple)))
    {
        /* it must have aborted or crashed */
        SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
                    InvalidTransactionId);
        return HeapTupleMayBeUpdated;
    }

    /* xmax transaction committed */

    if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
    {
        SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
                    InvalidTransactionId);
        return HeapTupleMayBeUpdated;
    }

    SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED,
                HeapTupleHeaderGetRawXmax(tuple));
    return HeapTupleUpdated;    /* updated by other */
}

/*
 * HeapTupleSatisfiesDirty
 *        True iff heap tuple is valid including effects of open transactions.
 *
 *    Here, we consider the effects of:
 *        all committed and in-progress transactions (as of the current instant)
 *        previous commands of this transaction
 *        changes made by the current command
 *
 * This is essentially like HeapTupleSatisfiesSelf as far as effects of
 * the current transaction and committed/aborted xacts are concerned.
 * However, we also include the effects of other xacts still in progress.
 *
 * A special hack is that the passed-in snapshot struct is used as an
 * output argument to return the xids of concurrent xacts that affected the
 * tuple.  snapshot->xmin is set to the tuple's xmin if that is another
 * transaction that's still in progress; or to InvalidTransactionId if the
 * tuple's xmin is committed good, committed dead, or my own xact.
 * Similarly for snapshot->xmax and the tuple's xmax.  If the tuple was
 * inserted speculatively, meaning that the inserter might still back down
 * on the insertion without aborting the whole transaction, the associated
 * token is also returned in snapshot->speculativeToken.
 */
bool
HeapTupleSatisfiesDirty(HeapTuple htup, Snapshot snapshot,
                        Buffer buffer)
{// #lizard forgives
    HeapTupleHeader tuple = htup->t_data;

    Assert(ItemPointerIsValid(&htup->t_self));
    Assert(htup->t_tableOid != InvalidOid);

    snapshot->xmin = snapshot->xmax = InvalidTransactionId;
    snapshot->speculativeToken = 0;

    if (!HeapTupleHeaderXminCommitted(tuple))
    {
        if (HeapTupleHeaderXminInvalid(tuple))
            return false;

        /* Used by pre-9.0 binary upgrades */
        if (tuple->t_infomask & HEAP_MOVED_OFF)
        {
            TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

            if (TransactionIdIsCurrentTransactionId(xvac))
                return false;
            if (!TransactionIdIsInProgress(xvac))
            {
                if (TransactionIdDidCommit(xvac))
                {
                    SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
                                InvalidTransactionId);
                    return false;
                }
                SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
                            InvalidTransactionId);
            }
        }
        /* Used by pre-9.0 binary upgrades */
        else if (tuple->t_infomask & HEAP_MOVED_IN)
        {
            TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

            if (!TransactionIdIsCurrentTransactionId(xvac))
            {
                if (TransactionIdIsInProgress(xvac))
                    return false;
                if (TransactionIdDidCommit(xvac))
                    SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
                                InvalidTransactionId);
                else
                {
                    SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
                                InvalidTransactionId);
                    return false;
                }
            }
        }
        else if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)))
        {
            if (tuple->t_infomask & HEAP_XMAX_INVALID)    /* xid invalid */
                return true;

            if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))    /* not deleter */
                return true;

            if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
            {
                TransactionId xmax;

                xmax = HeapTupleGetUpdateXid(tuple);

                /* not LOCKED_ONLY, so it has to have an xmax */
                Assert(TransactionIdIsValid(xmax));

                /* updating subtransaction must have aborted */
                if (!TransactionIdIsCurrentTransactionId(xmax))
                    return true;
                else
                    return false;
            }

            if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
            {
                /* deleting subtransaction must have aborted */
                SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
                            InvalidTransactionId);
                return true;
            }

            return false;
        }
        else if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmin(tuple)))
        {
            /*
             * Return the speculative token to caller.  Caller can worry about
             * xmax, since it requires a conclusively locked row version, and
             * a concurrent update to this tuple is a conflict of its
             * purposes.
             */
            if (HeapTupleHeaderIsSpeculative(tuple))
            {
                snapshot->speculativeToken =
                    HeapTupleHeaderGetSpeculativeToken(tuple);

                Assert(snapshot->speculativeToken != 0);
            }

            snapshot->xmin = HeapTupleHeaderGetRawXmin(tuple);
            /* XXX shouldn't we fall through to look at xmax? */
            return true;        /* in insertion by other */
        }
        else if (TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple)))
            SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
                        HeapTupleHeaderGetRawXmin(tuple));
        else
        {
            /* it must have aborted or crashed */
            SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
                        InvalidTransactionId);
            return false;
        }
    }

    /* by here, the inserting transaction has committed */

    if (tuple->t_infomask & HEAP_XMAX_INVALID)    /* xid invalid or aborted */
        return true;

    if (tuple->t_infomask & HEAP_XMAX_COMMITTED)
    {
        if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
            return true;
        return false;            /* updated by other */
    }

    if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
    {
        TransactionId xmax;

        if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
            return true;

        xmax = HeapTupleGetUpdateXid(tuple);

        /* not LOCKED_ONLY, so it has to have an xmax */
        Assert(TransactionIdIsValid(xmax));

        if (TransactionIdIsCurrentTransactionId(xmax))
            return false;
        if (TransactionIdIsInProgress(xmax))
        {
            snapshot->xmax = xmax;
            return true;
        }
        if (TransactionIdDidCommit(xmax))
            return false;
        /* it must have aborted or crashed */
        return true;
    }

    if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
    {
        if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
            return true;
        return false;
    }

    if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmax(tuple)))
    {
        if (!HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
            snapshot->xmax = HeapTupleHeaderGetRawXmax(tuple);
        return true;
    }

    if (!TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple)))
    {
        /* it must have aborted or crashed */
        SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
                    InvalidTransactionId);
        return true;
    }

    /* xmax transaction committed */

    if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
    {
        SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
                    InvalidTransactionId);
        return true;
    }

    SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED,
                HeapTupleHeaderGetRawXmax(tuple));
    return false;                /* updated by other */
}

#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
static bool
XminInMVCCSnapshotByTimestamp(HeapTupleHeader tuple, Snapshot snapshot,
							  Buffer buffer, bool *need_retry)
{
	GlobalTimestamp global_committs;
	TransactionId 	xid = HeapTupleHeaderGetRawXmin(tuple);
	bool			res;
	
	global_committs = HeapTupleHderGetXminTimestapAtomic(tuple);
	
	if(!GlobalTimestampIsValid(global_committs))
	{
		DEBUG_SNAPSHOT(elog(LOG, "invalid time xmin snapshot ts " INT64_FORMAT
								" xid %d.", snapshot->start_ts, xid));
		return XidInMVCCSnapshotDistri(tuple, xid, snapshot, buffer,
					need_retry, HEAP_XMIN_COMMITTED);
	}
	else if (snapshot->local || CommitTimestampIsLocal(global_committs))
	{
		res =  XidInMVCCSnapshot(xid, snapshot);

		DEBUG_SNAPSHOT(elog(LOG, "xmin local snapshot ts " INT64_FORMAT
				" res %d xid %d committs " INT64_FORMAT, snapshot->start_ts,
				res, xid, global_committs));
        SnapshotCheck(xid, snapshot, res, 0);
		return res;
	}
	else
	{
		if(enable_distri_debug)
		{
			snapshot->scanned_tuples_after_committed++;
		}
		
		*need_retry = false;
		if(!GlobalTimestampIsValid(snapshot->start_ts))
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("transaction %d does not have valid timestamp. "
							"snapshot start ts " INT64_FORMAT ", autovacuum %d"
							" in recovery %d",
					 xid, snapshot->start_ts,
					 IsAutoVacuumWorkerProcess(),
					 snapshot->takenDuringRecovery)));
		}

		DEBUG_SNAPSHOT(elog(LOG, "outer xmin snapshot ts " INT64_FORMAT " global"
				" committs " INT64_FORMAT " xid %d.", snapshot->start_ts,
				global_committs, xid));
		DEBUG_SNAPSHOT(
			if(!TransactionIdDidCommit(xid))
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("xmin transaction %d should commit but not. "
							   "snapshot start ts " INT64_FORMAT " commit %d "
							   "abort %d in-progress %d active %d recentxmin %d "
							   "start ts " INT64_FORMAT " committs " INT64_FORMAT,
						xid, 
						snapshot->start_ts, 
						TransactionIdDidCommit(xid), 
						TransactionIdDidAbort(xid), 
						TransactionIdIsInProgress(xid),
						TransactionIdIsActive(xid),
						RecentXmin,
						snapshot->start_ts, 
						global_committs)));
			});

		if(snapshot->start_ts > global_committs)
		{
			DEBUG_SNAPSHOT(elog(LOG, "snapshot ts " INT64_FORMAT " false xid %d "
					"committs " INT64_FORMAT " 21.", snapshot->start_ts, xid,
					global_committs));
            SnapshotCheck(xid, snapshot, false, global_committs);
			return false;
		}
		else
        {
			DEBUG_SNAPSHOT(elog(LOG, "snapshot ts " INT64_FORMAT " true xid %d"
					" committs " INT64_FORMAT " 22.", snapshot->start_ts, xid,
					global_committs));

			SnapshotCheck(xid, snapshot, true, global_committs);
			return true;
		}
	}
}

static bool
XmaxInMVCCSnapshotByTimestamp(HeapTupleHeader tuple, Snapshot snapshot,
							  Buffer buffer, bool *need_retry)
{
	GlobalTimestamp global_committs;
	TransactionId xid = HeapTupleHeaderGetRawXmax(tuple);
	bool res;

	global_committs = HeapTupleHderGetXmaxTimestapAtomic(tuple);
	
	if(!GlobalTimestampIsValid(global_committs))
	{
		DEBUG_SNAPSHOT(elog(LOG, "invalid time xmax snapshot ts " INT64_FORMAT
				" xid %d.", snapshot->start_ts, xid));
		return XidInMVCCSnapshotDistri(tuple, xid, snapshot, buffer,
					need_retry, HEAP_XMAX_COMMITTED);
	}
	else if (snapshot->local || CommitTimestampIsLocal(global_committs))
	{
		res = XidInMVCCSnapshot(xid, snapshot);

		DEBUG_SNAPSHOT(elog(LOG, "xmax local snapshot ts " INT64_FORMAT " res "
				"%d xid %d.", snapshot->start_ts, res, xid));

		SnapshotCheck(xid, snapshot, res, 0);
		return res;
	}
	else
	{
		if(enable_distri_debug)
		{
			snapshot->scanned_tuples_after_committed++;
		}
			
		*need_retry = false;
		if(!GlobalTimestampIsValid(snapshot->start_ts))
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("transaction %d does not have valid timestamp. "
							"snapshot start ts " INT64_FORMAT ", autovacuum %d "
							"in recovery %d",
					 xid, snapshot->start_ts, IsAutoVacuumWorkerProcess(),
					 snapshot->takenDuringRecovery)));
		}

		DEBUG_SNAPSHOT(elog(LOG, "outer xmax snapshot ts " INT64_FORMAT "global"
				" committs " INT64_FORMAT "xid %d.", snapshot->start_ts,
				global_committs, xid));
		DEBUG_SNAPSHOT(
			if(!TransactionIdDidCommit(xid))
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("xmax transaction %d should commit but not. "
								"snapshot start ts " INT64_FORMAT " commit %d "
								"abort %d in-progress %d active %d recentxmin %d "
								"start ts " INT64_FORMAT " committs " INT64_FORMAT,
						 xid,
						 snapshot->start_ts,
						 TransactionIdDidCommit(xid),
						 TransactionIdDidAbort(xid),
						 TransactionIdIsInProgress(xid),
						 TransactionIdIsActive(xid),
						 RecentXmin,
						 snapshot->start_ts,
						 global_committs)));
			}
		);

		if(snapshot->start_ts > global_committs)
		{
			DEBUG_SNAPSHOT(elog(LOG, "snapshot ts " INT64_FORMAT "false xid %d"
					" committs" INT64_FORMAT "11.", snapshot->start_ts, xid,
					global_committs));

			SnapshotCheck(xid, snapshot, false, global_committs);
			return false;
		}
		else
		{
			DEBUG_SNAPSHOT(elog(LOG, "snapshot ts " INT64_FORMAT "true xid %d "
					"committs " INT64_FORMAT "12.", snapshot->start_ts, xid,
					global_committs));

			SnapshotCheck(xid, snapshot, true, global_committs);
			return true;
		}
	}
}

/*
 * HeapTupleSatisfiesMVCC
 *        True iff heap tuple is valid for the given MVCC snapshot.
 *
 *    Here, we consider the effects of:
 *        all transactions committed as of the time of the given snapshot
 *        previous commands of this transaction
 *
 *    Does _not_ include:
 *        transactions shown as in-progress by the snapshot
 *        transactions started after the snapshot was taken
 *        changes made by the current command
 *
 * Notice that here, we will not update the tuple status hint bits if the
 * inserting/deleting transaction is still running according to our snapshot,
 * even if in reality it's committed or aborted by now.  This is intentional.
 * Checking the true transaction state would require access to high-traffic
 * shared data structures, creating contention we'd rather do without, and it
 * would not change the result of our visibility check anyway.  The hint bits
 * will be updated by the first visitor that has a snapshot new enough to see
 * the inserting/deleting transaction as done.  In the meantime, the cost of
 * leaving the hint bits unset is basically that each HeapTupleSatisfiesMVCC
 * call will need to run TransactionIdIsCurrentTransactionId in addition to
 * XidInMVCCSnapshot (but it would have to do the latter anyway).  In the old
 * coding where we tried to set the hint bits as soon as possible, we instead
 * did TransactionIdIsInProgress in each call --- to no avail, as long as the
 * inserting/deleting transaction was still running --- which was more cycles
 * and more contention on the PGXACT array.
 */

bool
HeapTupleSatisfiesMVCC(HeapTuple htup, Snapshot snapshot,
                       Buffer buffer)
{// #lizard forgives
    HeapTupleHeader tuple = htup->t_data;
    bool need_retry;

retry:
    need_retry = false;
    Assert(ItemPointerIsValid(&htup->t_self));
    Assert(htup->t_tableOid != InvalidOid);

#ifdef _MIGRATE_
    if(IS_PGXC_DATANODE && ShardIDIsValid(tuple->t_shardid) && SnapshotGetShardTable(snapshot))
    {
        bool shard_is_visible = bms_is_member(tuple->t_shardid/snapshot->groupsize,
                                                SnapshotGetShardTable(snapshot));

        if(!IsConnFromApp())
        {
            if(!shard_is_visible)
                return false;
        }
        else if(g_ShardVisibleMode != SHARD_VISIBLE_MODE_ALL)
        {
            if((!shard_is_visible && g_ShardVisibleMode == SHARD_VISIBLE_MODE_VISIBLE)
                || (shard_is_visible && g_ShardVisibleMode == SHARD_VISIBLE_MODE_HIDDEN))
            {
                return false;
            }
        }
    }
#endif
	if (!HeapTupleHeaderXminCommitted(tuple))
	{
		if (HeapTupleHeaderXminInvalid(tuple))
		{
			DEBUG_MVCC_XMIN(false, "xmin invalid");
			return false;
		}

		/* Used by pre-9.0 binary upgrades */
		if (tuple->t_infomask & HEAP_MOVED_OFF)
		{
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (TransactionIdIsCurrentTransactionId(xvac))
			{
				DEBUG_MVCC_XMIN(false, "move off");
				return false;
			}
			if (!XidInMVCCSnapshotDistri(tuple, xvac, snapshot, buffer, &need_retry, HEAP_XMIN_INVALID))
			{
				if (TransactionIdDidCommit(xvac))
				{
					SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
								InvalidTransactionId);
					DEBUG_MVCC_XMIN(false, "move off 1");
					return false;
				}
				SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
							InvalidTransactionId);
			}
			if(need_retry)
			{
				goto retry;
			}
			
		}
		/* Used by pre-9.0 binary upgrades */
		else if (tuple->t_infomask & HEAP_MOVED_IN)
		{
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (!TransactionIdIsCurrentTransactionId(xvac))
			{
				if (XidInMVCCSnapshotDistri(tuple, xvac, snapshot, buffer, &need_retry, HEAP_XMIN_INVALID))
				{
					if(need_retry)
					{
						goto retry;
					}
					DEBUG_MVCC_XMIN(false, " move in");
					return false;
				}
				if (TransactionIdDidCommit(xvac))
					SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
								InvalidTransactionId);
				else
				{
					SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
								InvalidTransactionId);
					DEBUG_MVCC_XMIN(false, "move in");
					return false;
				}
			}
		}
		else if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)))
		{
			
			if (HeapTupleHeaderGetCmin(tuple) >= snapshot->curcid)
			{
				DEBUG_MVCC_XMIN(false, "current 1");
				return false;	/* inserted after scan started */
			}
			if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid */
			{
				DEBUG_MVCC_XMIN(true, "current 2");
				DEBUG_INCREASE_VISIBLE_TUPLE;
				return true;
			}
			if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))	/* not deleter */
			{
				DEBUG_MVCC_XMIN(true, "current 3");
				DEBUG_INCREASE_VISIBLE_TUPLE;
				return true;
			}
			if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
			{
				TransactionId xmax;

				xmax = HeapTupleGetUpdateXid(tuple);

				/* not LOCKED_ONLY, so it has to have an xmax */
				Assert(TransactionIdIsValid(xmax));

				/* updating subtransaction must have aborted */
				if (!TransactionIdIsCurrentTransactionId(xmax))
				{
					DEBUG_MVCC_XMIN(true, "current 3");
					DEBUG_INCREASE_VISIBLE_TUPLE;
					return true;
				}
				else if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
				{
					DEBUG_MVCC_XMIN(true, "current 4");
					DEBUG_INCREASE_VISIBLE_TUPLE;
					return true;	/* updated after scan started */
				}
				else
				{
					DEBUG_MVCC_XMIN(false, "current 5");
					return false;	/* updated before scan started */
				}
			}

			if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
			{
				/* deleting subtransaction must have aborted */
				SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
							InvalidTransactionId);
				DEBUG_MVCC_XMIN(true, "current 6");
				DEBUG_INCREASE_VISIBLE_TUPLE;
				return true;
			}

			if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
			{
				DEBUG_MVCC_XMIN(true, "current 7");
				DEBUG_INCREASE_VISIBLE_TUPLE;
				return true;	/* deleted after scan started */
			}
			else
			{
				DEBUG_MVCC_XMIN(false, "current 8");
				return false;	/* deleted before scan started */
			}
		}
		else if (XminInMVCCSnapshotByTimestamp(tuple, snapshot, buffer, &need_retry))
		{
			//elog(DEBUG11, "heap xmin in snapshot");
			if(need_retry)
			{
				goto retry;
			}
			DEBUG_MVCC_XMIN(false, "");
			return false;
		}
		else if (TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple)))
			SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
						HeapTupleHeaderGetRawXmin(tuple));
		else
		{
			/* it must have aborted or crashed */
			SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
						InvalidTransactionId);
			DEBUG_MVCC_XMIN(false, "xmin abort");
			return false;
		}
	}
	else
	{
		/* xmin is committed, but maybe not according to our snapshot */
		if (!HeapTupleHeaderXminFrozen(tuple) &&
			XminInMVCCSnapshotByTimestamp(tuple, snapshot, buffer, &need_retry))
		{
			if(need_retry)
			{
				goto retry;
			}
			DEBUG_MVCC_XMIN(false, "according to snapshot");
			return false;		/* treat as still in progress */
		}		
	}

	/* by here, the inserting transaction has committed */

	if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid or aborted */
	{
		DEBUG_MVCC_XMIN(true, "");
		DEBUG_INCREASE_VISIBLE_TUPLE;
		return true;
	}
	if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask)){
		DEBUG_MVCC_XMIN(true, "xmax locked");
		DEBUG_INCREASE_VISIBLE_TUPLE;
		return true;
	}

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
	{
		TransactionId xmax;

		/* already checked above */
		Assert(!HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask));

		xmax = HeapTupleGetUpdateXid(tuple);

		/* not LOCKED_ONLY, so it has to have an xmax */
		Assert(TransactionIdIsValid(xmax));

		if (TransactionIdIsCurrentTransactionId(xmax))
		{
			if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
			{
				DEBUG_MVCC_XMIN(true, "heap multi xmax deleted after scan");
				DEBUG_INCREASE_VISIBLE_TUPLE
				return true;	/* deleted after scan started */
			}
			else
			{
				DEBUG_MVCC_XMINXMAX(true, xmax, "deleted after scan");
				return false;	/* deleted before scan started */
			}
		}
		if (XidInMVCCSnapshotDistri(tuple, xmax, snapshot, buffer, &need_retry, HEAP_XMAX_INVALID))
		{
			if(need_retry)
			{
				goto retry;
			}
			DEBUG_MVCC_XMIN(true, "");
			DEBUG_INCREASE_VISIBLE_TUPLE
			return true;
		}
		if (TransactionIdDidCommit(xmax))
		{
			DEBUG_MVCC_XMINXMAX(false, xmax, "committed");
			return false;		/* updating transaction committed */
		}
		/* it must have aborted or crashed */
		DEBUG_MVCC_XMIN(true, "");
		DEBUG_INCREASE_VISIBLE_TUPLE;
		return true;
	}

	if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED))
	{
		if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
		{
			if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
			{
				DEBUG_MVCC_XMIN(true, "");
				DEBUG_INCREASE_VISIBLE_TUPLE;
				return true;	/* deleted after scan started */
			}
			else
			{
				DEBUG_MVCC_XMIN(false, "xmax deleted before scan");
				return false;	/* deleted before scan started */
			}
		}

		if (XmaxInMVCCSnapshotByTimestamp(tuple, snapshot, buffer, &need_retry))
		{
			if(need_retry)
			{
				goto retry;
			}
			DEBUG_MVCC_XMIN(true, "");
			DEBUG_INCREASE_VISIBLE_TUPLE;
			return true;
		}
		if (!TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple)))
		{
			/* it must have aborted or crashed */
			
			SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
						InvalidTransactionId);
			DEBUG_MVCC_XMIN(true, "heap xmax aborted");
			DEBUG_INCREASE_VISIBLE_TUPLE;
			return true;
		}

		/* xmax transaction committed */
		SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED,
					HeapTupleHeaderGetRawXmax(tuple));
	}
	else
	{	
		/* xmax is committed, but maybe not according to our snapshot */
		if (XmaxInMVCCSnapshotByTimestamp(tuple, snapshot, buffer, &need_retry))
		{	
			if(need_retry)
			{
				goto retry;
			}
			DEBUG_MVCC_XMIN(true, "heap xmax not committed");
			DEBUG_INCREASE_VISIBLE_TUPLE;
			return true;		/* treat as still in progress */
		}
	}

	/* xmax transaction committed */
	DEBUG_MVCC_XMINXMAX(true, HeapTupleHeaderGetRawXmax(tuple), "committed last");
	return false;
}

#ifdef __STORAGE_SCALABLE__
bool
HeapTupleSatisfiesUnshard(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{// #lizard forgives
    HeapTupleHeader tuple = htup->t_data;
    bool need_retry;

retry:
    need_retry = false;
    Assert(ItemPointerIsValid(&htup->t_self));
    Assert(htup->t_tableOid != InvalidOid);

    if(IS_PGXC_DATANODE && tuple->t_shardid < 0)
        return false;

    if(IS_PGXC_DATANODE && tuple->t_shardid >= 0)
    {    
        if(g_DatanodeShardgroupBitmap == NULL)
        {
            elog(ERROR, "shard map in share memory has not been initialized yet.");
        }
		LWLockAcquire(ShardMapLock, LW_SHARED);	
        if(bms_is_member(tuple->t_shardid, g_DatanodeShardgroupBitmap))
        {
			LWLockRelease(ShardMapLock);
            return false;
        }
		LWLockRelease(ShardMapLock);
	}

	if (!HeapTupleHeaderXminCommitted(tuple))
	{
		if (HeapTupleHeaderXminInvalid(tuple))
		{
			DEBUG_MVCC_XMIN(false, "xmin invalid");
			return false;
		}

		/* Used by pre-9.0 binary upgrades */
		if (tuple->t_infomask & HEAP_MOVED_OFF)
		{
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (TransactionIdIsCurrentTransactionId(xvac))
			{
				DEBUG_MVCC_XMIN(false, "move off");
				return false;
			}
			if (!XidInMVCCSnapshotDistri(tuple, xvac, snapshot, buffer, &need_retry, HEAP_XMIN_INVALID))
			{
				if (TransactionIdDidCommit(xvac))
				{
					SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
								InvalidTransactionId);
					DEBUG_MVCC_XMIN(false, "move off 1");
					return false;
				}
				SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
							InvalidTransactionId);
			}
			if(need_retry)
			{
				goto retry;
			}
			
		}
		/* Used by pre-9.0 binary upgrades */
		else if (tuple->t_infomask & HEAP_MOVED_IN)
		{
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (!TransactionIdIsCurrentTransactionId(xvac))
			{
				if (XidInMVCCSnapshotDistri(tuple, xvac, snapshot, buffer, &need_retry, HEAP_XMIN_INVALID))
				{
					if(need_retry)
					{
						goto retry;
					}
					DEBUG_MVCC_XMIN(false, "move in");
					return false;
				}
				if (TransactionIdDidCommit(xvac))
					SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
								InvalidTransactionId);
				else
				{
					SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
								InvalidTransactionId);
					DEBUG_MVCC_XMIN(false, "move in");
					return false;
				}
			}
		}
		else if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)))
		{
			if (HeapTupleHeaderGetCmin(tuple) >= snapshot->curcid)
			{
				DEBUG_MVCC_XMIN(false, "current 1");
				return false;	/* inserted after scan started */
			}
			if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid */
			{
				DEBUG_MVCC_XMIN(true, "current 2");
				DEBUG_INCREASE_VISIBLE_TUPLE;
				return true;
			}
			if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))	/* not deleter */
			{
				DEBUG_MVCC_XMIN(true, "current 3");
				DEBUG_INCREASE_VISIBLE_TUPLE;
				return true;
			}
			if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
			{
				TransactionId xmax;

				xmax = HeapTupleGetUpdateXid(tuple);

				/* not LOCKED_ONLY, so it has to have an xmax */
				Assert(TransactionIdIsValid(xmax));

				/* updating subtransaction must have aborted */
				if (!TransactionIdIsCurrentTransactionId(xmax))
				{
					DEBUG_MVCC_XMIN(true, "current 3");
					DEBUG_INCREASE_VISIBLE_TUPLE;
					return true;
				}
				else if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
				{
					DEBUG_MVCC_XMIN(true, "current 4");
					DEBUG_INCREASE_VISIBLE_TUPLE;
					return true;	/* updated after scan started */
				}
				else
				{
					DEBUG_MVCC_XMIN(false, "current 5");
					return false;	/* updated before scan started */
				}
			}

			if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
			{
				/* deleting subtransaction must have aborted */
				SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
							InvalidTransactionId);
				DEBUG_MVCC_XMIN(true, "current 6");
				DEBUG_INCREASE_VISIBLE_TUPLE;
				return true;
			}

			if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
			{
				DEBUG_MVCC_XMIN(true, "current 7");
				DEBUG_INCREASE_VISIBLE_TUPLE;
				return true;	/* deleted after scan started */
			}
			else
			{
				DEBUG_MVCC_XMIN(false, "current 8");
				return false;	/* deleted before scan started */
			}
		}
		else if (XminInMVCCSnapshotByTimestamp(tuple, snapshot, buffer, &need_retry))
		{
			if(need_retry)
			{
				goto retry;
			}
			DEBUG_MVCC_XMIN(false, "xmin in snapshot");
			return false;
		}
		else if (TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple)))
			SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
						HeapTupleHeaderGetRawXmin(tuple));
		else
		{
			/* it must have aborted or crashed */
			SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
						InvalidTransactionId);
			DEBUG_MVCC_XMIN(false, "xmin aborted");
			return false;
		}
	}
	else
	{
		/* xmin is committed, but maybe not according to our snapshot */
		if (!HeapTupleHeaderXminFrozen(tuple) &&
			XminInMVCCSnapshotByTimestamp(tuple, snapshot, buffer, &need_retry))
		{
			if(need_retry)
			{
				goto retry;
			}
			DEBUG_MVCC_XMIN(false, " xmin not committed according to snapshot");
			return false;		/* treat as still in progress */
		}		
	}

	/* by here, the inserting transaction has committed */

	if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid or aborted */
	{
		DEBUG_MVCC_XMIN(true, "invalid xmax");
		DEBUG_INCREASE_VISIBLE_TUPLE;
		return true;
	}
	if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask)){
		DEBUG_MVCC_XMIN(true, "xmax locked");
		DEBUG_INCREASE_VISIBLE_TUPLE;
		return true;
	}

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
	{
		TransactionId xmax;

		/* already checked above */
		Assert(!HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask));

		xmax = HeapTupleGetUpdateXid(tuple);

		/* not LOCKED_ONLY, so it has to have an xmax */
		Assert(TransactionIdIsValid(xmax));

		if (TransactionIdIsCurrentTransactionId(xmax))
		{
			if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
			{
				DEBUG_MVCC_XMIN(true, "multi xmax deleted after scan");
				DEBUG_INCREASE_VISIBLE_TUPLE;
				return true;	/* deleted after scan started */
			}
			else
			{
				DEBUG_MVCC_XMINXMAX(false, xmax, "deleted before scan");
				return false;	/* deleted before scan started */
			}
		}
		if (XidInMVCCSnapshotDistri(tuple, xmax, snapshot, buffer, &need_retry, HEAP_XMAX_INVALID))
		{
			if(need_retry)
			{
				goto retry;
			}
			DEBUG_MVCC_XMIN(true, "multi xmax in snapshot");
			DEBUG_INCREASE_VISIBLE_TUPLE;
			return true;
		}
		if (TransactionIdDidCommit(xmax))
		{
			DEBUG_MVCC_XMINXMAX(false, xmax, "committed");
			return false;		/* updating transaction committed */
		}
		/* it must have aborted or crashed */
		DEBUG_MVCC_XMIN(true, "xmax aborted");
		DEBUG_INCREASE_VISIBLE_TUPLE;
		return true;
	}

	if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED))
	{
		if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
		{
			if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
			{
				DEBUG_MVCC_XMIN(true, "xmax deleted after scan started");
				DEBUG_INCREASE_VISIBLE_TUPLE;
				return true;	/* deleted after scan started */
			}
			else
			{
				DEBUG_MVCC_XMIN(false, "xmax deleted before scan started");
				return false;	/* deleted before scan started */
			}
		}

		if (XmaxInMVCCSnapshotByTimestamp(tuple, snapshot, buffer, &need_retry))
		{
			if(need_retry)
			{
				goto retry;
			}
			DEBUG_MVCC_XMIN(true, "xmax in mvcc snapshot");
			DEBUG_INCREASE_VISIBLE_TUPLE;
			return true;
		}
		if (!TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple)))
		{
			/* it must have aborted or crashed */
			SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
						InvalidTransactionId);
			DEBUG_MVCC_XMIN(true, "xmax aborted");
			DEBUG_INCREASE_VISIBLE_TUPLE;
			return true;
		}

		/* xmax transaction committed */
		SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED,
					HeapTupleHeaderGetRawXmax(tuple));
	}
	else
	{	
		/* xmax is committed, but maybe not according to our snapshot */
		if (XmaxInMVCCSnapshotByTimestamp(tuple, snapshot, buffer, &need_retry))
		{	
			if(need_retry)
			{
				goto retry;
			}
			DEBUG_MVCC_XMIN(true, "xmax not committed");
			DEBUG_INCREASE_VISIBLE_TUPLE;
			return true;		/* treat as still in progress */
		}
	}

	/* xmax transaction committed */
	DEBUG_MVCC_XMINXMAX(true, HeapTupleHeaderGetRawXmax(tuple), "xmax committed");
	return false;
}

#endif

#else
#ifdef __STORAGE_SCALABLE__
bool
HeapTupleSatisfiesUnshard(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{// #lizard forgives
    HeapTupleHeader tuple = htup->t_data;

    Assert(ItemPointerIsValid(&htup->t_self));
    Assert(htup->t_tableOid != InvalidOid);
    
    if(IS_PGXC_DATANODE && tuple->t_shardid < 0)
        return false;

    if(IS_PGXC_DATANODE && tuple->t_shardid >= 0)
    {    
        if(g_DatanodeShardgroupBitmap == NULL)
        {
            elog(ERROR, "shard map in share memory has not been initialized yet.");
        }
		LWLockAcquire(ShardMapLock, LW_SHARED);	
        if(bms_is_member(tuple->t_shardid, g_DatanodeShardgroupBitmap))
        {
			LWLockRelease(ShardMapLock);
            return false;
        }
		LWLockRelease(ShardMapLock);
    }

    if (!HeapTupleHeaderXminCommitted(tuple))
    {
        if (HeapTupleHeaderXminInvalid(tuple))
            return false;

        /* Used by pre-9.0 binary upgrades */
        if (tuple->t_infomask & HEAP_MOVED_OFF)
        {
            TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

            if (TransactionIdIsCurrentTransactionId(xvac))
                return false;
            if (!XidInMVCCSnapshot(xvac, snapshot))
            {
                if (TransactionIdDidCommit(xvac))
                {
                    SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
                                InvalidTransactionId);
                    return false;
                }
                SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
                            InvalidTransactionId);
            }
        }
        /* Used by pre-9.0 binary upgrades */
        else if (tuple->t_infomask & HEAP_MOVED_IN)
        {
            TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

            if (!TransactionIdIsCurrentTransactionId(xvac))
            {
                if (XidInMVCCSnapshot(xvac, snapshot))
                    return false;
                if (TransactionIdDidCommit(xvac))
                    SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
                                InvalidTransactionId);
                else
                {
                    SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
                                InvalidTransactionId);
                    return false;
                }
            }
        }
        else if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)))
        {
            if (HeapTupleHeaderGetCmin(tuple) >= snapshot->curcid)
                return false;    /* inserted after scan started */

            if (tuple->t_infomask & HEAP_XMAX_INVALID)    /* xid invalid */
                return true;

            if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))    /* not deleter */
                return true;

            if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
            {
                TransactionId xmax;

                xmax = HeapTupleGetUpdateXid(tuple);

                /* not LOCKED_ONLY, so it has to have an xmax */
                Assert(TransactionIdIsValid(xmax));

                /* updating subtransaction must have aborted */
                if (!TransactionIdIsCurrentTransactionId(xmax))
                    return true;
                else if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
                    return true;    /* updated after scan started */
                else
                    return false;    /* updated before scan started */
            }

            if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
            {
                /* deleting subtransaction must have aborted */
                SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
                            InvalidTransactionId);
                return true;
            }

            if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
                return true;    /* deleted after scan started */
            else
                return false;    /* deleted before scan started */
        }
        else if (XidInMVCCSnapshot(HeapTupleHeaderGetRawXmin(tuple), snapshot))
            return false;
        else if (TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple)))
            SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
                        HeapTupleHeaderGetRawXmin(tuple));
        else
        {
            /* it must have aborted or crashed */
            SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
                        InvalidTransactionId);
            return false;
        }
    }
    else
    {
        /* xmin is committed, but maybe not according to our snapshot */
        if (!HeapTupleHeaderXminFrozen(tuple) &&
            XidInMVCCSnapshot(HeapTupleHeaderGetRawXmin(tuple), snapshot))
            return false;        /* treat as still in progress */
    }

    /* by here, the inserting transaction has committed */

    if (tuple->t_infomask & HEAP_XMAX_INVALID)    /* xid invalid or aborted */
        return true;

    if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
        return true;

    if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
    {
        TransactionId xmax;

        /* already checked above */
        Assert(!HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask));

        xmax = HeapTupleGetUpdateXid(tuple);

        /* not LOCKED_ONLY, so it has to have an xmax */
        Assert(TransactionIdIsValid(xmax));

        if (TransactionIdIsCurrentTransactionId(xmax))
        {
            if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
                return true;    /* deleted after scan started */
            else
                return false;    /* deleted before scan started */
        }
        if (XidInMVCCSnapshot(xmax, snapshot))
            return true;
        if (TransactionIdDidCommit(xmax))
            return false;        /* updating transaction committed */
        /* it must have aborted or crashed */
        return true;
    }

    if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED))
    {
        if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
        {
            if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
                return true;    /* deleted after scan started */
            else
                return false;    /* deleted before scan started */
        }

        if (XidInMVCCSnapshot(HeapTupleHeaderGetRawXmax(tuple), snapshot))
            return true;

        if (!TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple)))
        {
            /* it must have aborted or crashed */
            SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
                        InvalidTransactionId);
            return true;
        }

        /* xmax transaction committed */
        SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED,
                    HeapTupleHeaderGetRawXmax(tuple));
    }
    else
    {
        /* xmax is committed, but maybe not according to our snapshot */
        if (XidInMVCCSnapshot(HeapTupleHeaderGetRawXmax(tuple), snapshot))
            return true;        /* treat as still in progress */
    }

    /* xmax transaction committed */

    return false;
}
#endif

bool
HeapTupleSatisfiesMVCC(HeapTuple htup, Snapshot snapshot,
                       Buffer buffer)
{// #lizard forgives
    HeapTupleHeader tuple = htup->t_data;

    Assert(ItemPointerIsValid(&htup->t_self));
    Assert(htup->t_tableOid != InvalidOid);
    
#ifdef _MIGRATE_
    if(IS_PGXC_DATANODE && ShardIDIsValid(tuple->t_shardid) && SnapshotGetShardTable(snapshot))
    {
        bool shard_is_visible = bms_is_member(tuple->t_shardid/snapshot->groupsize,
                                                SnapshotGetShardTable(snapshot));

        if(!IsConnFromApp())
        {
            if(!shard_is_visible)
                return false;
        }
        else if(g_ShardVisibleMode != SHARD_VISIBLE_MODE_ALL)
        {
            if((!shard_is_visible && g_ShardVisibleMode == SHARD_VISIBLE_MODE_VISIBLE)
                || (shard_is_visible && g_ShardVisibleMode == SHARD_VISIBLE_MODE_HIDDEN))
            {
                return false;
            }
        }
    }
#endif

    if (!HeapTupleHeaderXminCommitted(tuple))
    {
        if (HeapTupleHeaderXminInvalid(tuple))
            return false;

        /* Used by pre-9.0 binary upgrades */
        if (tuple->t_infomask & HEAP_MOVED_OFF)
        {
            TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

            if (TransactionIdIsCurrentTransactionId(xvac))
                return false;
            if (!XidInMVCCSnapshot(xvac, snapshot))
            {
                if (TransactionIdDidCommit(xvac))
                {
                    SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
                                InvalidTransactionId);
                    return false;
                }
                SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
                            InvalidTransactionId);
            }
        }
        /* Used by pre-9.0 binary upgrades */
        else if (tuple->t_infomask & HEAP_MOVED_IN)
        {
            TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

            if (!TransactionIdIsCurrentTransactionId(xvac))
            {
                if (XidInMVCCSnapshot(xvac, snapshot))
                    return false;
                if (TransactionIdDidCommit(xvac))
                    SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
                                InvalidTransactionId);
                else
                {
                    SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
                                InvalidTransactionId);
                    return false;
                }
            }
        }
        else if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)))
        {
            if (HeapTupleHeaderGetCmin(tuple) >= snapshot->curcid)
                return false;    /* inserted after scan started */

            if (tuple->t_infomask & HEAP_XMAX_INVALID)    /* xid invalid */
                return true;

            if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))    /* not deleter */
                return true;

            if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
            {
                TransactionId xmax;

                xmax = HeapTupleGetUpdateXid(tuple);

                /* not LOCKED_ONLY, so it has to have an xmax */
                Assert(TransactionIdIsValid(xmax));

                /* updating subtransaction must have aborted */
                if (!TransactionIdIsCurrentTransactionId(xmax))
                    return true;
                else if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
                    return true;    /* updated after scan started */
                else
                    return false;    /* updated before scan started */
            }

            if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
            {
                /* deleting subtransaction must have aborted */
                SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
                            InvalidTransactionId);
                return true;
            }

            if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
                return true;    /* deleted after scan started */
            else
                return false;    /* deleted before scan started */
        }
        else if (XidInMVCCSnapshot(HeapTupleHeaderGetRawXmin(tuple), snapshot))
            return false;
        else if (TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple)))
            SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
                        HeapTupleHeaderGetRawXmin(tuple));
        else
        {
            /* it must have aborted or crashed */
            SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
                        InvalidTransactionId);
            return false;
        }
    }
    else
    {
        /* xmin is committed, but maybe not according to our snapshot */
        if (!HeapTupleHeaderXminFrozen(tuple) &&
            XidInMVCCSnapshot(HeapTupleHeaderGetRawXmin(tuple), snapshot))
            return false;        /* treat as still in progress */
    }

    /* by here, the inserting transaction has committed */

    if (tuple->t_infomask & HEAP_XMAX_INVALID)    /* xid invalid or aborted */
        return true;

    if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
        return true;

    if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
    {
        TransactionId xmax;

        /* already checked above */
        Assert(!HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask));

        xmax = HeapTupleGetUpdateXid(tuple);

        /* not LOCKED_ONLY, so it has to have an xmax */
        Assert(TransactionIdIsValid(xmax));

        if (TransactionIdIsCurrentTransactionId(xmax))
        {
            if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
                return true;    /* deleted after scan started */
            else
                return false;    /* deleted before scan started */
        }
        if (XidInMVCCSnapshot(xmax, snapshot))
            return true;
        if (TransactionIdDidCommit(xmax))
            return false;        /* updating transaction committed */
        /* it must have aborted or crashed */
        return true;
    }

    if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED))
    {
        if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
        {
            if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
                return true;    /* deleted after scan started */
            else
                return false;    /* deleted before scan started */
        }

        if (XidInMVCCSnapshot(HeapTupleHeaderGetRawXmax(tuple), snapshot))
            return true;

        if (!TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple)))
        {
            /* it must have aborted or crashed */
            SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
                        InvalidTransactionId);
            return true;
        }

        /* xmax transaction committed */
        SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED,
                    HeapTupleHeaderGetRawXmax(tuple));
    }
    else
    {
        /* xmax is committed, but maybe not according to our snapshot */
        if (XidInMVCCSnapshot(HeapTupleHeaderGetRawXmax(tuple), snapshot))
            return true;        /* treat as still in progress */
    }

    /* xmax transaction committed */

    return false;
}
#endif

/*
 * HeapTupleSatisfiesVacuum
 *
 *    Determine the status of tuples for VACUUM purposes.  Here, what
 *    we mainly want to know is if a tuple is potentially visible to *any*
 *    running transaction.  If so, it can't be removed yet by VACUUM.
 *
 * OldestXmin is a cutoff XID (obtained from GetOldestXmin()).  Tuples
 * deleted by XIDs >= OldestXmin are deemed "recently dead"; they might
 * still be visible to some open transaction, so we can't remove them,
 * even if we see that the deleting transaction has committed.
 */
HTSV_Result
HeapTupleSatisfiesVacuum(HeapTuple htup, TransactionId OldestXmin,
                         Buffer buffer)
{// #lizard forgives
    HeapTupleHeader tuple = htup->t_data;

    Assert(ItemPointerIsValid(&htup->t_self));
    Assert(htup->t_tableOid != InvalidOid);

    /*
     * Has inserting transaction committed?
     *
     * If the inserting transaction aborted, then the tuple was never visible
     * to any other transaction, so we can delete it immediately.
     */
    if (!HeapTupleHeaderXminCommitted(tuple))
    {
        if (HeapTupleHeaderXminInvalid(tuple))
            return HEAPTUPLE_DEAD;
        /* Used by pre-9.0 binary upgrades */
        else if (tuple->t_infomask & HEAP_MOVED_OFF)
        {
            TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

            if (TransactionIdIsCurrentTransactionId(xvac))
                return HEAPTUPLE_DELETE_IN_PROGRESS;
            if (TransactionIdIsInProgress(xvac))
                return HEAPTUPLE_DELETE_IN_PROGRESS;
            if (TransactionIdDidCommit(xvac))
            {
                SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
                            InvalidTransactionId);
                return HEAPTUPLE_DEAD;
            }
            SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
                        InvalidTransactionId);
        }
        /* Used by pre-9.0 binary upgrades */
        else if (tuple->t_infomask & HEAP_MOVED_IN)
        {
            TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

            if (TransactionIdIsCurrentTransactionId(xvac))
                return HEAPTUPLE_INSERT_IN_PROGRESS;
            if (TransactionIdIsInProgress(xvac))
                return HEAPTUPLE_INSERT_IN_PROGRESS;
            if (TransactionIdDidCommit(xvac))
                SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
                            InvalidTransactionId);
            else
            {
                SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
                            InvalidTransactionId);
                return HEAPTUPLE_DEAD;
            }
        }
        else if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)))
        {
            if (tuple->t_infomask & HEAP_XMAX_INVALID)    /* xid invalid */
                return HEAPTUPLE_INSERT_IN_PROGRESS;
            /* only locked? run infomask-only check first, for performance */
            if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask) ||
                HeapTupleHeaderIsOnlyLocked(tuple))
                return HEAPTUPLE_INSERT_IN_PROGRESS;
            /* inserted and then deleted by same xact */
            if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetUpdateXid(tuple)))
                return HEAPTUPLE_DELETE_IN_PROGRESS;
            /* deleting subtransaction must have aborted */
            return HEAPTUPLE_INSERT_IN_PROGRESS;
        }
        else if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmin(tuple)))
        {
            /*
             * It'd be possible to discern between INSERT/DELETE in progress
             * here by looking at xmax - but that doesn't seem beneficial for
             * the majority of callers and even detrimental for some. We'd
             * rather have callers look at/wait for xmin than xmax. It's
             * always correct to return INSERT_IN_PROGRESS because that's
             * what's happening from the view of other backends.
             */
            return HEAPTUPLE_INSERT_IN_PROGRESS;
        }
        else if (TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple)))
            SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
                        HeapTupleHeaderGetRawXmin(tuple));
        else
        {
            /*
             * Not in Progress, Not Committed, so either Aborted or crashed
             */
            SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
                        InvalidTransactionId);
            return HEAPTUPLE_DEAD;
        }

        /*
         * At this point the xmin is known committed, but we might not have
         * been able to set the hint bit yet; so we can no longer Assert that
         * it's set.
         */
    }

    /*
     * Okay, the inserter committed, so it was good at some point.  Now what
     * about the deleting transaction?
     */
    if (tuple->t_infomask & HEAP_XMAX_INVALID)
        return HEAPTUPLE_LIVE;

    if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
    {
        /*
         * "Deleting" xact really only locked it, so the tuple is live in any
         * case.  However, we should make sure that either XMAX_COMMITTED or
         * XMAX_INVALID gets set once the xact is gone, to reduce the costs of
         * examining the tuple for future xacts.
         */
        if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED))
        {
            if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
            {
                /*
                 * If it's a pre-pg_upgrade tuple, the multixact cannot
                 * possibly be running; otherwise have to check.
                 */
                if (!HEAP_LOCKED_UPGRADED(tuple->t_infomask) &&
                    MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple),
                                         true))
                    return HEAPTUPLE_LIVE;
                SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
            }
            else
            {
                if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmax(tuple)))
                    return HEAPTUPLE_LIVE;
                SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
                            InvalidTransactionId);
            }
        }

        /*
         * We don't really care whether xmax did commit, abort or crash. We
         * know that xmax did lock the tuple, but it did not and will never
         * actually update it.
         */

        return HEAPTUPLE_LIVE;
    }

    if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
    {
        TransactionId xmax;

        if (MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), false))
        {
            /* already checked above */
            Assert(!HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask));

            xmax = HeapTupleGetUpdateXid(tuple);

            /* not LOCKED_ONLY, so it has to have an xmax */
            Assert(TransactionIdIsValid(xmax));

            if (TransactionIdIsInProgress(xmax))
                return HEAPTUPLE_DELETE_IN_PROGRESS;
            else if (TransactionIdDidCommit(xmax))
                /* there are still lockers around -- can't return DEAD here */
                return HEAPTUPLE_RECENTLY_DEAD;
            /* updating transaction aborted */
            return HEAPTUPLE_LIVE;
        }

        Assert(!(tuple->t_infomask & HEAP_XMAX_COMMITTED));

        xmax = HeapTupleGetUpdateXid(tuple);

        /* not LOCKED_ONLY, so it has to have an xmax */
        Assert(TransactionIdIsValid(xmax));

        /* multi is not running -- updating xact cannot be */
        Assert(!TransactionIdIsInProgress(xmax));
        if (TransactionIdDidCommit(xmax))
        {
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
            if (!TransactionIdPrecedes(xmax, OldestXmin))
                return HEAPTUPLE_RECENTLY_DEAD;

            {
                GlobalTimestamp committs;

                /* As xid is removed from procarray (< OldestXmin), the committs should have been written */
                if(false == TransactionIdGetCommitTsData(xmax, &committs, NULL))
                {

                    ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                                errmsg("xmax %d should have commit timestamp committed %d abort %d in-progress %d  Oldestxmin %d at HeapTupleSatisfiesVacuum 1", 
                                        xmax,
                                            TransactionIdDidCommit(xmax),
                                        TransactionIdDidAbort(xmax),
                                        TransactionIdIsInProgress(xmax),
                                        OldestXmin
                                        )));
                    
                }
                    
                if(!TestForOldTimestamp(committs, RecentDataTs))
                {
                    if(vacuum_debug_print)
                        elog(LOG, "vacuum RECENTLY DEAD committs "INT64_FORMAT "RecentDataTs "INT64_FORMAT, committs, RecentDataTs);
                    return HEAPTUPLE_RECENTLY_DEAD;
                }

                if(vacuum_debug_print)
                        elog(LOG, "vacuum DEAD committs "INT64_FORMAT "RecentDataTs "INT64_FORMAT, committs, RecentDataTs);
            }
            
            return HEAPTUPLE_DEAD;
#else
    
            if (!TransactionIdPrecedes(xmax, OldestXmin))
                return HEAPTUPLE_RECENTLY_DEAD;
            else
                return HEAPTUPLE_DEAD;
#endif
        }

        /*
         * Not in Progress, Not Committed, so either Aborted or crashed.
         * Remove the Xmax.
         */
        SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
        return HEAPTUPLE_LIVE;
    }

    if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED))
    {
        if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmax(tuple)))
            return HEAPTUPLE_DELETE_IN_PROGRESS;
        else if (TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple)))
            SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED,
                        HeapTupleHeaderGetRawXmax(tuple));
        else
        {
            /*
             * Not in Progress, Not Committed, so either Aborted or crashed
             */
            SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
                        InvalidTransactionId);
            return HEAPTUPLE_LIVE;
        }

        /*
         * At this point the xmax is known committed, but we might not have
         * been able to set the hint bit yet; so we can no longer Assert that
         * it's set.
         */
    }

    /*
     * Deleter committed, but perhaps it was recent enough that some open
     * transactions could still see the tuple.
     */

#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
    if (!TransactionIdPrecedes(HeapTupleHeaderGetRawXmax(tuple), OldestXmin))
        return HEAPTUPLE_RECENTLY_DEAD;

    {
        GlobalTimestamp committs = HeapTupleHderGetXmaxTimestapAtomic(tuple);

        if(!GlobalTimestampIsValid(committs))
        {
            if(false == TransactionIdGetCommitTsData(HeapTupleHeaderGetRawXmax(tuple), &committs, NULL))
            {
                ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("xmax %d should have commit timestamp committed %d abort %d in-progress %d oldestxmin %d at HeapTupleSatisfiesVacuum 2", 
                                HeapTupleHeaderGetRawXmax(tuple),
                                TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple)),
                                TransactionIdDidAbort(HeapTupleHeaderGetRawXmax(tuple)),
                                TransactionIdIsInProgress(HeapTupleHeaderGetRawXmax(tuple)),
                                OldestXmin
                        )));

                
            }
        }

        if(!TestForOldTimestamp(committs, RecentDataTs))
        {
            if(vacuum_debug_print)
                elog(LOG, "vacuum RECENTLY DEAD committs "INT64_FORMAT "RecentDataTs "INT64_FORMAT, committs, RecentDataTs);
            return HEAPTUPLE_RECENTLY_DEAD;
        }
        if(vacuum_debug_print)
            elog(LOG, "vacuum  DEAD committs "INT64_FORMAT "RecentDataTs "INT64_FORMAT, committs, RecentDataTs);
            
    }
    
    return HEAPTUPLE_DEAD;
#else
    if (!TransactionIdPrecedes(HeapTupleHeaderGetRawXmax(tuple), OldestXmin))
        return HEAPTUPLE_RECENTLY_DEAD;

    /* Otherwise, it's dead and removable */
    return HEAPTUPLE_DEAD;
#endif
}

/*
 * HeapTupleIsSurelyDead
 *
 *    Cheaply determine whether a tuple is surely dead to all onlookers.
 *    We sometimes use this in lieu of HeapTupleSatisfiesVacuum when the
 *    tuple has just been tested by another visibility routine (usually
 *    HeapTupleSatisfiesMVCC) and, therefore, any hint bits that can be set
 *    should already be set.  We assume that if no hint bits are set, the xmin
 *    or xmax transaction is still running.  This is therefore faster than
 *    HeapTupleSatisfiesVacuum, because we don't consult PGXACT nor CLOG.
 *    It's okay to return FALSE when in doubt, but we must return TRUE only
 *    if the tuple is removable.
 */
bool
HeapTupleIsSurelyDead(HeapTuple htup, TransactionId OldestXmin)
{// #lizard forgives
    HeapTupleHeader tuple = htup->t_data;

    Assert(ItemPointerIsValid(&htup->t_self));
    Assert(htup->t_tableOid != InvalidOid);

    /*
     * If the inserting transaction is marked invalid, then it aborted, and
     * the tuple is definitely dead.  If it's marked neither committed nor
     * invalid, then we assume it's still alive (since the presumption is that
     * all relevant hint bits were just set moments ago).
     */
    if (!HeapTupleHeaderXminCommitted(tuple))
        return HeapTupleHeaderXminInvalid(tuple) ? true : false;

    /*
     * If the inserting transaction committed, but any deleting transaction
     * aborted, the tuple is still alive.
     */
    if (tuple->t_infomask & HEAP_XMAX_INVALID)
        return false;

    /*
     * If the XMAX is just a lock, the tuple is still alive.
     */
    if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
        return false;

    /*
     * If the Xmax is a MultiXact, it might be dead or alive, but we cannot
     * know without checking pg_multixact.
     */
    if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
        return false;

    /* If deleter isn't known to have committed, assume it's still running. */
    if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED))
        return false;

    /* Deleter committed, so tuple is dead if the XID is old enough. */
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
    {
        if (!TransactionIdPrecedes(HeapTupleHeaderGetRawXmax(tuple), OldestXmin))
            return false;
        else 
        {
            GlobalTimestamp committs = HeapTupleHderGetXmaxTimestapAtomic(tuple);
            
            if(!GlobalTimestampIsValid(committs))
            {
                if(false == TransactionIdGetCommitTsData(HeapTupleHeaderGetRawXmax(tuple), &committs, NULL))
                {

                    ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                            errmsg("xmax %d should have commit timestamp committed %d abort %d in progress %d oldestxmin %d at HeapTupleIsSurelyDead", 
                                        HeapTupleHeaderGetRawXmax(tuple),
                                        TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple)),
                                        TransactionIdDidAbort(HeapTupleHeaderGetRawXmax(tuple)),
                                        TransactionIdIsInProgress(HeapTupleHeaderGetRawXmax(tuple)),
                                        OldestXmin
                                        )));
                    
                }
            }

            if(TestForOldTimestamp(committs, RecentDataTs))
                return true;

            return false;
        }
    }
#endif
    return TransactionIdPrecedes(HeapTupleHeaderGetRawXmax(tuple), OldestXmin);
}
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__


#ifdef __TBASE_DEBUG__

static HTAB *SharedSnapHash;


typedef struct
{
    GlobalTimestamp start_ts;
    int64             xid;
} SnapTag;

typedef struct
{
    SnapTag            tag;            /* Tag of a snapshot */
    int                res;                /* Snapshot Result */
    GlobalTimestamp     committs;            /* Committs if committed or preparets if prepared */
} SnapLookupEnt;

#define INIT_SNAPTAG(a, ts, xid) \
( \
    (a).start_ts = (ts),\
    (a).xid = (xid)\
)


static uint32
SnapTableHashCode(SnapTag *tagPtr)
{
    return get_hash_value(SharedSnapHash, (void *) tagPtr);
}
int
SnapTableLookup(SnapTag *tagPtr, uint32 hashcode, GlobalTimestamp *committs);
int
SnapTableInsert(SnapTag *tagPtr, uint32 hashcode, int res, GlobalTimestamp committs);
void
SnapTableDelete(SnapTag *tagPtr, uint32 hashcode);

/*
 * BufTableLookup
 *        Lookup the given BufferTag; return buffer ID, or -1 if not found
 *
 * Caller must hold at least share lock on BufMappingLock for tag's partition
 */
int
SnapTableLookup(SnapTag *tagPtr, uint32 hashcode, GlobalTimestamp *committs)
{
    SnapLookupEnt *result;

    result = (SnapLookupEnt *)
        hash_search_with_hash_value(SharedSnapHash,
                                    (void *) tagPtr,
                                    hashcode,
                                    HASH_FIND,
                                    NULL);

    if (!result)
        return -1;

    if(committs)
        *committs = result->committs;
    return result->res;
}

/*
 * BufTableInsert
 *        Insert a hashtable entry for given tag and buffer ID,
 *        unless an entry already exists for that tag
 *
 * Returns -1 on successful insertion.  If a conflicting entry exists
 * already, returns the buffer ID in that entry.
 *
 * Caller must hold exclusive lock on BufMappingLock for tag's partition
 */
int
SnapTableInsert(SnapTag *tagPtr, uint32 hashcode, int res, GlobalTimestamp committs)
{
    SnapLookupEnt *result;
    bool        found;

    Assert(res >= 0);        /* -1 is reserved for not-in-table */

    result = (SnapLookupEnt *)
        hash_search_with_hash_value(SharedSnapHash,
                                    (void *) tagPtr,
                                    hashcode,
                                    HASH_ENTER,
                                    &found);

    if (found)                    /* found something already in the table */
        return result->res;

    result->res = res;
    result->committs = committs; 
    if(enable_distri_print)
    {
        elog(LOG, "snaptable insert xid %u ts "INT64_FORMAT" hashcode %u res %d ts " INT64_FORMAT, 
            tagPtr->xid, tagPtr->start_ts, hashcode, res, committs);
    }
    return -1;
}

/*
 * BufTableDelete
 *        Delete the hashtable entry for given tag (which must exist)
 *
 * Caller must hold exclusive lock on BufMappingLock for tag's partition
 */
void
SnapTableDelete(SnapTag *tagPtr, uint32 hashcode)
{
    SnapLookupEnt *result;

    result = (SnapLookupEnt *)
        hash_search_with_hash_value(SharedSnapHash,
                                    (void *) tagPtr,
                                    hashcode,
                                    HASH_REMOVE,
                                    NULL);
    if(enable_distri_print)
    {
        elog(LOG, "delete xid %u "INT64_FORMAT, tagPtr->xid, tagPtr->start_ts);
    }
    if (!result)                /* shouldn't happen */
    {
        elog(ERROR, "shared snap buffer hash table corrupted xid %u "INT64_FORMAT, tagPtr->xid, tagPtr->start_ts);
    }
}



#define NUM_SNAP_PARTITIONS 128

typedef struct SnapSharedData
{
    /* LWLocks */
    int            lwlock_tranche_id;
    LWLockPadded locks[NUM_SNAP_PARTITIONS];
} SnapSharedData;

typedef SnapSharedData *SnapShared;

SnapShared snapShared;

#define SnapHashPartition(hashcode) \
    ((hashcode) % NUM_SNAP_PARTITIONS)

#define MAX_SIZE MaxTransactionId >> 10
Size
SnapTableShmemSize(void)
{
    int size = MAX_SIZE;
    
    return add_size(hash_estimate_size(size, sizeof(SnapLookupEnt)), MAXALIGN(sizeof(SnapSharedData)));
}

static uint32
snap_hash(const void *key, Size keysize)
{
    const SnapTag *tagPtr = key;
    return tagPtr->xid;
}

static int snap_cmp (const void *key1, const void *key2,
                                Size keysize)
{
    const SnapTag *tagPtr1 = key1, *tagPtr2 = key2;

    if(tagPtr1->start_ts == tagPtr2->start_ts 
        && tagPtr1->xid == tagPtr2->xid)
        return 0;

    return 1;

}


void
InitSnapBufTable(void)
{
    HASHCTL        info;
    long max_size = MAX_SIZE;
    int size = MAX_SIZE;
    bool found;
    int i;
    /* assume no locking is needed yet */

    /* BufferTag maps to Buffer */
    info.keysize = sizeof(SnapTag);
    info.entrysize = sizeof(SnapLookupEnt);
    info.num_partitions = NUM_SNAP_PARTITIONS;
    info.hash         = snap_hash;
    info.match        = snap_cmp;

    SharedSnapHash = ShmemInitHash("Shared Snapshot Lookup Table",
                                  size, size,
                                  &info,
                                  HASH_ELEM  | HASH_COMPARE | HASH_PARTITION | HASH_FUNCTION);

    snapShared = (SnapShared) ShmemInitStruct("Global Snapshot Shared Data",
                                          sizeof(SnapSharedData),
                                          &found);

    if(!found)
    {
        snapShared->lwlock_tranche_id = LWTRANCHE_SNAPSHOT;
        for (i = 0; i < NUM_SNAP_PARTITIONS; i++)
            LWLockInitialize(&snapShared->locks[i].lock,
                                 snapShared->lwlock_tranche_id);
    }
    LWLockRegisterTranche(snapShared->lwlock_tranche_id,
                              "snapshot");    
    
}

bool LookupPreparedXid(TransactionId xid, GlobalTimestamp *prepare_timestamp)
{
    SnapTag             tag;
    uint32                hash;        /* hash value for newTag */
    int                 partitionno;
    LWLock                   *partitionLock;    /* buffer partition lock for it */
    int                 res;
    
    INIT_SNAPTAG(tag, 0, xid);
    hash = SnapTableHashCode(&tag);
    partitionno = SnapHashPartition(hash);
    
    /* Try to find the page while holding only shared lock */
    partitionLock = &snapShared->locks[partitionno].lock;
    LWLockAcquire(partitionLock, LW_SHARED);
    res = SnapTableLookup(&tag, hash, prepare_timestamp);
    LWLockRelease(partitionLock);
    if(res == -1)
        res = 0;

    if(res == 0)
    {
        if(!TransactionIdIsInProgress(xid))
            res = 1;
    }else
    {
        if(enable_distri_print)
        {
            elog(LOG, "xid %d prepared transaction prep ts " INT64_FORMAT " abort %d commit %d", 
                                    xid,  prepare_timestamp, TransactionIdDidAbort(xid), TransactionIdDidCommit(xid));
        }
    }
    return res;

}
void DeletePreparedXid(TransactionId xid)
{
    SnapTag             tag;
    uint32                hash;        /* hash value for newTag */
    int                 partitionno;
    LWLock                   *partitionLock;    /* buffer partition lock for it */
    int                 res;
    GlobalTimestamp        ts;
    
    INIT_SNAPTAG(tag, 0, xid);
    hash = SnapTableHashCode(&tag);
    partitionno = SnapHashPartition(hash);
    /* Try to find the page while holding only shared lock */
    partitionLock = &snapShared->locks[partitionno].lock;
    LWLockAcquire(partitionLock, LW_EXCLUSIVE);

    SnapTableDelete(&tag, hash);
    LWLockRelease(partitionLock);
    return;
}

void InsertPreparedXid(TransactionId xid, GlobalTimestamp prepare_timestamp)
{

    SnapTag             tag;
    uint32                hash;        /* hash value for newTag */
    int                 partitionno;
    LWLock                   *partitionLock;    /* buffer partition lock for it */
    int                 res;
    GlobalTimestamp     ts;
    
    INIT_SNAPTAG(tag, 0, xid);
    hash = SnapTableHashCode(&tag);
    partitionno = SnapHashPartition(hash);
    
    /* Try to find the page while holding only shared lock */
    partitionLock = &snapShared->locks[partitionno].lock;
    LWLockAcquire(partitionLock, LW_EXCLUSIVE);
    if(SnapTableInsert(&tag, hash, 1, prepare_timestamp) != -1)
        elog(ERROR, "insert fails");

    LWLockRelease(partitionLock);


}

#ifdef __SNAPSHOT_CHECK__

static bool SnapshotCheck(TransactionId xid, Snapshot snapshot, int target_res, GlobalTimestamp target_committs)
{// #lizard forgives
    SnapTag             tag;
    uint32                hash;        /* hash value for newTag */
    int                 partitionno;
    LWLock                   *partitionLock;    /* buffer partition lock for it */
    int                 res;
    GlobalTimestamp        res_committs;

    if(snapshot->local)
        return true;
    
    INIT_SNAPTAG(tag, snapshot->start_ts, xid);
    hash = SnapTableHashCode(&tag);
    partitionno = SnapHashPartition(hash);
    
    /* Try to find the page while holding only shared lock */
    partitionLock = &snapShared->locks[partitionno].lock;
    LWLockAcquire(partitionLock, LW_SHARED);
    res = SnapTableLookup(&tag, hash, &res_committs);
    if(res >= 0)
    {
        if(res != target_res)
            ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("snapshot check fails xid %d snapshot start_ts " INT64_FORMAT " autovacuum %d res %d target_res %d res committs " INT64_FORMAT "target committs " INT64_FORMAT, 
                         xid, snapshot->start_ts, IsAutoVacuumWorkerProcess(), res, target_res, res_committs, target_committs)));
        
        if(GlobalTimestampIsValid(res_committs))
        {
            if(res_committs != target_committs)
                ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("snapshot check fails xid %d snapshot start_ts " INT64_FORMAT " autovacuum %d res %d target_res %d res committs " INT64_FORMAT " target committs " INT64_FORMAT, 
                         xid, snapshot->start_ts, IsAutoVacuumWorkerProcess(), res, target_res, res_committs, target_committs)));
            
        }
        else if(GlobalTimestampIsValid(target_committs))
        {
            LWLockRelease(partitionLock);
            LWLockAcquire(partitionLock, LW_EXCLUSIVE);
            SnapTableDelete(&tag, hash);
            if(SnapTableInsert(&tag, hash, res, target_committs) != -1)
                elog(ERROR, "insert fails");        

        }
        LWLockRelease(partitionLock);
        elog(DEBUG12, "snapshot check succeeds xid %d snapshot start_ts " INT64_FORMAT " autovacuum %d res %d target_res %d res committs " INT64_FORMAT " target committs " INT64_FORMAT , 
                         xid, snapshot->start_ts, IsAutoVacuumWorkerProcess(), res, target_res, res_committs, target_committs);
    }
    else
    {
        LWLockRelease(partitionLock);
        LWLockAcquire(partitionLock, LW_EXCLUSIVE);
        if(SnapTableInsert(&tag, hash, target_res, target_committs) != -1)
            elog(ERROR, "insert fails");
        LWLockRelease(partitionLock);
        elog(DEBUG12, "snapshot check inserts xid %d snapshot start_ts " INT64_FORMAT " autovacuum %d res %d target_res %d res committs " INT64_FORMAT " target committs " INT64_FORMAT, 
                         xid, snapshot->start_ts, IsAutoVacuumWorkerProcess(), res, target_res, res_committs, target_committs);
    }

    return true;

}


#endif /* __SNAPSHOT_CHECK__ */

#endif /* __TBASE_DEBUG__ */


static bool
XidIsPrepared(TransactionId xid, Snapshot snapshot, GlobalTimestamp *prepare_ts)
{// #lizard forgives
    uint32        i;

    /*
     * Make a quick range check to eliminate most XIDs without looking at the
     * xip arrays.  Note that this is OK even if we convert a subxact XID to
     * its parent below, because a subxact with XID < xmin has surely also got
     * a parent with XID < xmin, while one with XID >= xmax must belong to a
     * parent that was not yet committed at the time of this snapshot.
     */

    /* Any xid < xmin is not in-progress */
    if (TransactionIdPrecedes(xid, snapshot->prepare_xmin))
        return true;

    /*
     * If the snapshot contains full subxact data, the fastest way to
     * check things is just to compare the given XID against both subxact
     * XIDs and top-level XIDs.  If the snapshot overflowed, we have to
     * use pg_subtrans to convert a subxact XID to its parent XID, but
     * then we need only look at top-level XIDs not subxacts.
     */
    if (!snapshot->suboverflowed)
    {
        /* we have full data, so search subxip */
        int32        j;

        for (j = 0; j < snapshot->prepare_subxcnt; j++)
        {
            if (TransactionIdEquals(xid, snapshot->prepare_subxip[j]))
            {
                *prepare_ts = snapshot->prepare_subxip_ts[j];
                return GlobalTimestampIsValid(*prepare_ts) ? true:false;
            }
        }

        /* not there, fall through to search xip[] */
    }
    else
    {
        /*
         * Snapshot overflowed, so convert xid to top-level.  This is safe
         * because we eliminated too-old XIDs above.
         */
        xid = SubTransGetTopmostTransaction(xid);

        /*
         * If xid was indeed a subxact, we might now have an xid < xmin,
         * so recheck to avoid an array scan.  No point in rechecking
         * xmax.
         */
        if (TransactionIdPrecedes(xid, snapshot->prepare_xmin))
            return true;
    }

    for (i = 0; i < snapshot->prepare_xcnt; i++)
    {
        if (TransactionIdEquals(xid, snapshot->prepare_xip[i]))
        {
            *prepare_ts = snapshot->prepare_xip_ts[i];
            return GlobalTimestampIsValid(*prepare_ts) ? true:false;
        }
    }

    return false;

}



#endif


/*
 * XidInMVCCSnapshot
 *        Is the given XID still-in-progress according to the snapshot?
 *
 * Note: GetSnapshotData never stores either top xid or subxids of our own
 * backend into a snapshot, so these xids will not be reported as "running"
 * by this function.  This is OK for current uses, because we always check
 * TransactionIdIsCurrentTransactionId first, except for known-committed
 * XIDs which could not be ours anyway.
 */
static bool
XidInMVCCSnapshot(TransactionId xid, Snapshot snapshot)
{// #lizard forgives
    uint32        i;

    /*
     * Make a quick range check to eliminate most XIDs without looking at the
     * xip arrays.  Note that this is OK even if we convert a subxact XID to
     * its parent below, because a subxact with XID < xmin has surely also got
     * a parent with XID < xmin, while one with XID >= xmax must belong to a
     * parent that was not yet committed at the time of this snapshot.
     */

    /* Any xid < xmin is not in-progress */
    if (TransactionIdPrecedes(xid, snapshot->xmin))
        return false;
    /* Any xid >= xmax is in-progress */
    if (TransactionIdFollowsOrEquals(xid, snapshot->xmax))
        return true;

    /*
     * Snapshot information is stored slightly differently in snapshots taken
     * during recovery.
     */
    if (!snapshot->takenDuringRecovery)
    {
        /*
         * If the snapshot contains full subxact data, the fastest way to
         * check things is just to compare the given XID against both subxact
         * XIDs and top-level XIDs.  If the snapshot overflowed, we have to
         * use pg_subtrans to convert a subxact XID to its parent XID, but
         * then we need only look at top-level XIDs not subxacts.
         */
        if (!snapshot->suboverflowed)
        {
            /* we have full data, so search subxip */
            int32        j;

            for (j = 0; j < snapshot->subxcnt; j++)
            {
                if (TransactionIdEquals(xid, snapshot->subxip[j]))
                    return true;
            }

            /* not there, fall through to search xip[] */
        }
        else
        {
            /*
             * Snapshot overflowed, so convert xid to top-level.  This is safe
             * because we eliminated too-old XIDs above.
             */
            xid = SubTransGetTopmostTransaction(xid);

            /*
             * If xid was indeed a subxact, we might now have an xid < xmin,
             * so recheck to avoid an array scan.  No point in rechecking
             * xmax.
             */
            if (TransactionIdPrecedes(xid, snapshot->xmin))
                return false;
        }

        for (i = 0; i < snapshot->xcnt; i++)
        {
            if (TransactionIdEquals(xid, snapshot->xip[i]))
                return true;
        }
    }
    else
    {
        int32        j;

        /*
         * In recovery we store all xids in the subxact array because it is by
         * far the bigger array, and we mostly don't know which xids are
         * top-level and which are subxacts. The xip array is empty.
         *
         * We start by searching subtrans, if we overflowed.
         */
        if (snapshot->suboverflowed)
        {
            /*
             * Snapshot overflowed, so convert xid to top-level.  This is safe
             * because we eliminated too-old XIDs above.
             */
            xid = SubTransGetTopmostTransaction(xid);

            /*
             * If xid was indeed a subxact, we might now have an xid < xmin,
             * so recheck to avoid an array scan.  No point in rechecking
             * xmax.
             */
            if (TransactionIdPrecedes(xid, snapshot->xmin))
                return false;
        }

        /*
         * We now have either a top-level xid higher than xmin or an
         * indeterminate xid. We don't know whether it's top level or subxact
         * but it doesn't matter. If it's present, the xid is visible.
         */
        for (j = 0; j < snapshot->subxcnt; j++)
        {
            if (TransactionIdEquals(xid, snapshot->subxip[j]))
                return true;
        }
    }

    return false;
}

#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
static bool
XidInMVCCSnapshotDistri(HeapTupleHeader tuple, TransactionId xid,
						Snapshot snapshot, Buffer buffer,
						bool *need_retry, uint16 infomask)
{
	int res = false;
	GlobalTimestamp prepare_ts;
	GlobalTimestamp global_committs = 0;

	*need_retry = false;
	/* 
	 * For Tbase, we propose a concurrency control mechanism based on global
	 * timestamp to maintain distributed transaction consistency.
	 * 
	 * Rule: T2 can see T1's modification only if T2.start > T1.commit.
	 * For read-committed isolation, T2.start is the executing statement's
	 * start timestmap.
	 */
	if (snapshot->local || !TransactionIdIsNormal(xid))
	{
		res = XidInMVCCSnapshot(xid, snapshot);

		DEBUG_SNAPSHOT(elog(DEBUG12, "local: snapshot ts " INT64_FORMAT "xid %d"
				" res %d.", snapshot->start_ts, xid, res));
		SnapshotCheck(xid, snapshot, res, 0);
		return res;
	}
	
	if(TransactionIdGetCommitTsData(xid, &global_committs, NULL))
	{
		if(!GlobalTimestampIsValid(snapshot->start_ts))
		{
			ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("transaction %d does not have valid timestamp."
								"snapshot start ts " INT64_FORMAT ", autovacuum"
								" %d in recovery %d",
						 xid, snapshot->start_ts,
						 IsAutoVacuumWorkerProcess(),
						 snapshot->takenDuringRecovery)));
		}
		Assert(GlobalTimestampIsValid(snapshot->start_ts));
		
		if(enable_distri_debug)
		{
			snapshot->scanned_tuples_after_committed++;
		}
		
		if(CommitTimestampIsLocal(global_committs))
		{
			res = XidInMVCCSnapshot(xid, snapshot);

			DEBUG_SNAPSHOT(elog(DEBUG12, "local snapshot ts " INT64_FORMAT "res"
					" %d xid %d after wait.", snapshot->start_ts, res, xid));
			SnapshotCheck(xid, snapshot, res, 0);
			return res;
		}

		if(snapshot->start_ts > global_committs)
		{
			DEBUG_SNAPSHOT(elog(LOG, "snapshot ts " INT64_FORMAT "false xid %d"
					" committs " INT64_FORMAT "1.", snapshot->start_ts, xid,
					global_committs));
			SnapshotCheck(xid, snapshot, false, global_committs);
			return false;
		}
		else
		{
			DEBUG_SNAPSHOT(elog(LOG, "snapshot ts " INT64_FORMAT "true xid %d "
					"committs " INT64_FORMAT "2.",
					snapshot->start_ts, xid, global_committs));
			SnapshotCheck(xid, snapshot, true, global_committs);
			SetTimestamp(tuple, xid, buffer, infomask);
			return true;
		}
	}

	prepare_ts = InvalidGlobalTimestamp;
	/* 
	 * If xid has passed the prepare phase, we should wait for it to complete.
	 */
	if(XidIsPrepared(xid, snapshot, &prepare_ts))
	{
		if(enable_distri_debug)
		{
			snapshot->scanned_tuples_after_prepare++;
		}
		
		if(!GlobalTimestampIsValid(snapshot->start_ts))
		{
			ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("transaction %d does not have valid timestamp. "
								"snapshot start ts " INT64_FORMAT ", autovacuum"
								" %d in recovery %d",
								xid, snapshot->start_ts,
								IsAutoVacuumWorkerProcess(),
								snapshot->takenDuringRecovery)));
		}

		if(GlobalTimestampIsValid(prepare_ts) && !GlobalTimestampIsFrozen(prepare_ts) &&
			(snapshot->start_ts < prepare_ts))
		{
			DEBUG_SNAPSHOT(elog(LOG, "snapshot ts " INT64_FORMAT " true xid %d"
					" prep " INT64_FORMAT, snapshot->start_ts, xid, prepare_ts));
			SnapshotCheck(xid, snapshot, true, 0);
			return true;
		}
		
		if(GlobalTimestampIsValid(prepare_ts))
		{
			BufferDesc *buf;
			int lock_type = -1;
			
			buf = GetBufferDescriptor(buffer - 1);
			
			if(LWLockHeldByMeInMode(BufferDescriptorGetContentLock(buf),
										LW_EXCLUSIVE))
			{
				lock_type = BUFFER_LOCK_EXCLUSIVE;
			}
			else if(LWLockHeldByMeInMode(BufferDescriptorGetContentLock(buf),
										LW_SHARED))
			{
				lock_type = BUFFER_LOCK_SHARE;
			}

			XactLockTableWait(xid, NULL, NULL, XLTW_None);
			if(lock_type != -1)
			{
				/* Avoid deadlock */
				if(TransactionIdDidAbort(xid))
				{
					DEBUG_SNAPSHOT(elog(LOG, "abort snapshot ts " INT64_FORMAT
							"false xid %d .", snapshot->start_ts, xid));
					if(enable_distri_debug)
					{
						snapshot->scanned_tuples_after_abort++;
					}
					
					*need_retry = false;
					return false;
				}
				else
				{
					*need_retry = true;
					return true;
				}
			}
		}

		if(TransactionIdGetCommitTsData(xid, &global_committs, NULL))
		{
			if(enable_distri_debug)
			{
				snapshot->scanned_tuples_after_committed++;
			}
			
			if(CommitTimestampIsLocal(global_committs))
			{
				res =  XidInMVCCSnapshot(xid, snapshot);

				DEBUG_SNAPSHOT(elog(DEBUG12, "local snapshot ts " INT64_FORMAT
						"res %d xid %d after wait.",
						snapshot->start_ts, res,xid));
				SnapshotCheck(xid, snapshot, res, 0);
				return res;
			}

			if(snapshot->start_ts > global_committs)
			{
				DEBUG_SNAPSHOT(elog(LOG, "snapshot ts " INT64_FORMAT " false "
						"xid %d commit_ts " INT64_FORMAT " 3.",
						snapshot->start_ts, xid, global_committs));
				SnapshotCheck(xid, snapshot, false, global_committs);
				return false;
			}
			else
			{
				DEBUG_SNAPSHOT(elog(LOG, "snapshot ts " INT64_FORMAT " true xid"
						" %d committs" INT64_FORMAT " 4.", snapshot->start_ts,
						xid, global_committs));
				SnapshotCheck(xid, snapshot, true, global_committs);

				SetTimestamp(tuple, xid, buffer, infomask);
				return true;
			}
		}
		else 
		{/* Abort or crashed */
			if(enable_distri_debug)
			{
				snapshot->scanned_tuples_after_abort++;
			}

			DEBUG_SNAPSHOT(elog(LOG, "abort snapshot ts " INT64_FORMAT " false"
					" xid %d .", snapshot->start_ts, xid));
			SnapshotCheck(xid, snapshot, false, 0);
			return false;
		}
	}

	if(enable_distri_debug)
	{
		snapshot->scanned_tuples_before_prepare++;
	}

	/*
	 * For non-prepared transaction, its commit timestamp must be larger than
	 * the current running transaction/statement's start timestamp. This is
	 * because that as T1's commit timestamp has not yet been aquired on CN,
	 * T2.start < T1.commit is always being held.
	 */
	DEBUG_SNAPSHOT(elog(LOG, "snapshot ts " INT64_FORMAT " true xid %d 5.",
			snapshot->start_ts, xid));
	SnapshotCheck(xid, snapshot, true, 0);

	return true;
}

#endif
/*
 * Is the tuple really only locked?  That is, is it not updated?
 *
 * It's easy to check just infomask bits if the locker is not a multi; but
 * otherwise we need to verify that the updating transaction has not aborted.
 *
 * This function is here because it follows the same time qualification rules
 * laid out at the top of this file.
 */
bool
HeapTupleHeaderIsOnlyLocked(HeapTupleHeader tuple)
{// #lizard forgives
    TransactionId xmax;

    /* if there's no valid Xmax, then there's obviously no update either */
    if (tuple->t_infomask & HEAP_XMAX_INVALID)
        return true;

    if (tuple->t_infomask & HEAP_XMAX_LOCK_ONLY)
        return true;

    /* invalid xmax means no update */
    if (!TransactionIdIsValid(HeapTupleHeaderGetRawXmax(tuple)))
        return true;

    /*
     * if HEAP_XMAX_LOCK_ONLY is not set and not a multi, then this must
     * necessarily have been updated
     */
    if (!(tuple->t_infomask & HEAP_XMAX_IS_MULTI))
        return false;

    /* ... but if it's a multi, then perhaps the updating Xid aborted. */
    xmax = HeapTupleGetUpdateXid(tuple);

    /* not LOCKED_ONLY, so it has to have an xmax */
    Assert(TransactionIdIsValid(xmax));

    if (TransactionIdIsCurrentTransactionId(xmax))
        return false;
    if (TransactionIdIsInProgress(xmax))
        return false;
    if (TransactionIdDidCommit(xmax))
        return false;

    /*
     * not current, not in progress, not committed -- must have aborted or
     * crashed
     */
    return true;
}

/*
 * check whether the transaction id 'xid' is in the pre-sorted array 'xip'.
 */
static bool
TransactionIdInArray(TransactionId xid, TransactionId *xip, Size num)
{
    return bsearch(&xid, xip, num,
                   sizeof(TransactionId), xidComparator) != NULL;
}

/*
 * See the comments for HeapTupleSatisfiesMVCC for the semantics this function
 * obeys.
 *
 * Only usable on tuples from catalog tables!
 *
 * We don't need to support HEAP_MOVED_(IN|OFF) for now because we only support
 * reading catalog pages which couldn't have been created in an older version.
 *
 * We don't set any hint bits in here as it seems unlikely to be beneficial as
 * those should already be set by normal access and it seems to be too
 * dangerous to do so as the semantics of doing so during timetravel are more
 * complicated than when dealing "only" with the present.
 */
bool
HeapTupleSatisfiesHistoricMVCC(HeapTuple htup, Snapshot snapshot,
                               Buffer buffer)
{// #lizard forgives
    HeapTupleHeader tuple = htup->t_data;
    TransactionId xmin = HeapTupleHeaderGetXmin(tuple);
    TransactionId xmax = HeapTupleHeaderGetRawXmax(tuple);

    Assert(ItemPointerIsValid(&htup->t_self));
    Assert(htup->t_tableOid != InvalidOid);

    /* inserting transaction aborted */
    if (HeapTupleHeaderXminInvalid(tuple))
    {
        Assert(!TransactionIdDidCommit(xmin));
        return false;
    }
    /* check if it's one of our txids, toplevel is also in there */
    else if (TransactionIdInArray(xmin, snapshot->subxip, snapshot->subxcnt))
    {
        bool        resolved;
        CommandId    cmin = HeapTupleHeaderGetRawCommandId(tuple);
        CommandId    cmax = InvalidCommandId;

        /*
         * another transaction might have (tried to) delete this tuple or
         * cmin/cmax was stored in a combocid. So we need to lookup the actual
         * values externally.
         */
        resolved = ResolveCminCmaxDuringDecoding(HistoricSnapshotGetTupleCids(), snapshot,
                                                 htup, buffer,
                                                 &cmin, &cmax);

        if (!resolved)
            elog(ERROR, "could not resolve cmin/cmax of catalog tuple");

        Assert(cmin != InvalidCommandId);

        if (cmin >= snapshot->curcid)
            return false;        /* inserted after scan started */
        /* fall through */
    }
    /* committed before our xmin horizon. Do a normal visibility check. */
    else if (TransactionIdPrecedes(xmin, snapshot->xmin))
    {
        Assert(!(HeapTupleHeaderXminCommitted(tuple) &&
                 !TransactionIdDidCommit(xmin)));

        /* check for hint bit first, consult clog afterwards */
        if (!HeapTupleHeaderXminCommitted(tuple) &&
            !TransactionIdDidCommit(xmin))
            return false;
        /* fall through */
    }
    /* beyond our xmax horizon, i.e. invisible */
    else if (TransactionIdFollowsOrEquals(xmin, snapshot->xmax))
    {
        return false;
    }
    /* check if it's a committed transaction in [xmin, xmax) */
    else if (TransactionIdInArray(xmin, snapshot->xip, snapshot->xcnt))
    {
        /* fall through */
    }

    /*
     * none of the above, i.e. between [xmin, xmax) but hasn't committed. I.e.
     * invisible.
     */
    else
    {
        return false;
    }

    /* at this point we know xmin is visible, go on to check xmax */

    /* xid invalid or aborted */
    if (tuple->t_infomask & HEAP_XMAX_INVALID)
        return true;
    /* locked tuples are always visible */
    else if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
        return true;

    /*
     * We can see multis here if we're looking at user tables or if somebody
     * SELECT ... FOR SHARE/UPDATE a system table.
     */
    else if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
    {
        xmax = HeapTupleGetUpdateXid(tuple);
    }

    /* check if it's one of our txids, toplevel is also in there */
    if (TransactionIdInArray(xmax, snapshot->subxip, snapshot->subxcnt))
    {
        bool        resolved;
        CommandId    cmin;
        CommandId    cmax = HeapTupleHeaderGetRawCommandId(tuple);

        /* Lookup actual cmin/cmax values */
        resolved = ResolveCminCmaxDuringDecoding(HistoricSnapshotGetTupleCids(), snapshot,
                                                 htup, buffer,
                                                 &cmin, &cmax);

        if (!resolved)
            elog(ERROR, "could not resolve combocid to cmax");

        Assert(cmax != InvalidCommandId);

        if (cmax >= snapshot->curcid)
            return true;        /* deleted after scan started */
        else
            return false;        /* deleted before scan started */
    }
    /* below xmin horizon, normal transaction state is valid */
    else if (TransactionIdPrecedes(xmax, snapshot->xmin))
    {
        Assert(!(tuple->t_infomask & HEAP_XMAX_COMMITTED &&
                 !TransactionIdDidCommit(xmax)));

        /* check hint bit first */
        if (tuple->t_infomask & HEAP_XMAX_COMMITTED)
            return false;

        /* check clog */
        return !TransactionIdDidCommit(xmax);
    }
    /* above xmax horizon, we cannot possibly see the deleting transaction */
    else if (TransactionIdFollowsOrEquals(xmax, snapshot->xmax))
        return true;
    /* xmax is between [xmin, xmax), check known committed array */
    else if (TransactionIdInArray(xmax, snapshot->xip, snapshot->xcnt))
        return false;
    /* xmax is between [xmin, xmax), but known not to have committed yet */
    else
        return true;
}

#if 0
#ifdef _MIGRATE_
/*
 * HeapTupleSatisfiesNow
 *        True iff heap tuple is valid "now".
 *
 *    Here, we consider the effects of:
 *        all committed transactions (as of the current instant)
 *        previous commands of this transaction
 *
 * Note we do _not_ include changes made by the current command.  This
 * solves the "Halloween problem" wherein an UPDATE might try to re-update
 * its own output tuples, http://en.wikipedia.org/wiki/Halloween_Problem.
 *
 * Note:
 *        Assumes heap tuple is valid.
 *
 * The satisfaction of "now" requires the following:
 *
 * ((Xmin == my-transaction &&                inserted by the current transaction
 *     Cmin < my-command &&                    before this command, and
 *     (Xmax is null ||                        the row has not been deleted, or
 *      (Xmax == my-transaction &&            it was deleted by the current transaction
 *       Cmax >= my-command)))                but not before this command,
 * ||                                        or
 *    (Xmin is committed &&                    the row was inserted by a committed transaction, and
 *        (Xmax is null ||                    the row has not been deleted, or
 *         (Xmax == my-transaction &&            the row is being deleted by this transaction
 *          Cmax >= my-command) ||            but it's not deleted "yet", or
 *         (Xmax != my-transaction &&            the row was deleted by another transaction
 *          Xmax is not committed))))            that has not been committed
 *
 */
bool
HeapTupleSatisfiesNow(HeapTupleHeader tuple, Snapshot snapshot, Buffer buffer)
{// #lizard forgives
//#ifdef _PG_SHARDING_
    if(IS_PGXC_DATANODE && tuple->t_shardid >= 0 && SnapshotGetShardTable(snapshot))
    {
        bool shard_is_visible = bms_is_member(tuple->t_shardid/snapshot->groupsize,
                                                SnapshotGetShardTable(snapshot));

        if(!IsConnFromApp())
        {
            if(!shard_is_visible)
                return false;
        }
        else if(g_ShardVisibleMode != SHARD_VISIBLE_MODE_ALL)
        {
            if((!shard_is_visible && g_ShardVisibleMode == SHARD_VISIBLE_MODE_VISIBLE)
                || (shard_is_visible && g_ShardVisibleMode == SHARD_VISIBLE_MODE_HIDDEN))
            {
                return false;
            }
        }
    }
//#endif

    if (!(tuple->t_infomask & HEAP_XMIN_COMMITTED))
    {
        if (tuple->t_infomask & HEAP_XMIN_INVALID)
            return false;

        /* Used by pre-9.0 binary upgrades */
        if (tuple->t_infomask & HEAP_MOVED_OFF)
        {
            TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

            if (TransactionIdIsCurrentTransactionId(xvac))
                return false;
            if (!TransactionIdIsInProgress(xvac))
            {
                if (TransactionIdDidCommit(xvac))
                {
                    SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
                                InvalidTransactionId);
                    return false;
                }
                SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
                            InvalidTransactionId);
            }
        }
        /* Used by pre-9.0 binary upgrades */
        else if (tuple->t_infomask & HEAP_MOVED_IN)
        {
            TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

            if (!TransactionIdIsCurrentTransactionId(xvac))
            {
                if (TransactionIdIsInProgress(xvac))
                    return false;
                if (TransactionIdDidCommit(xvac))
                    SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
                                InvalidTransactionId);
                else
                {
                    SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
                                InvalidTransactionId);
                    return false;
                }
            }
        }
        else if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmin(tuple)))
        {
            if (HeapTupleHeaderGetCmin(tuple) >= GetCurrentCommandId(false))
                return false;    /* inserted after scan started */

            if (tuple->t_infomask & HEAP_XMAX_INVALID)    /* xid invalid */
                return true;

            if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))    /* not deleter */
                return true;

            if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
            {
                TransactionId xmax;

                xmax = HeapTupleGetUpdateXid(tuple);
                if (!TransactionIdIsValid(xmax))
                    return true;

                /* updating subtransaction must have aborted */
                if (!TransactionIdIsCurrentTransactionId(xmax))
                    return true;
                else
                    return false;
            }

            if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
            {
                /* deleting subtransaction must have aborted */
                SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
                            InvalidTransactionId);
                return true;
            }

            if (HeapTupleHeaderGetCmax(tuple) >= GetCurrentCommandId(false))
                return true;    /* deleted after scan started */
            else
                return false;    /* deleted before scan started */
        }
        else if (TransactionIdIsInProgress(HeapTupleHeaderGetXmin(tuple)))
            return false;
        else if (TransactionIdDidCommit(HeapTupleHeaderGetXmin(tuple)))
            SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
                        HeapTupleHeaderGetXmin(tuple));
        else
        {
            /* it must have aborted or crashed */
            SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
                        InvalidTransactionId);
            return false;
        }
    }

    /* by here, the inserting transaction has committed */

    if (tuple->t_infomask & HEAP_XMAX_INVALID)    /* xid invalid or aborted */
        return true;

    if (tuple->t_infomask & HEAP_XMAX_COMMITTED)
    {
        if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
            return true;
        return false;
    }

    if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
    {
        TransactionId xmax;

        if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
            return true;

        xmax = HeapTupleGetUpdateXid(tuple);
        if (!TransactionIdIsValid(xmax))
            return true;
        if (TransactionIdIsCurrentTransactionId(xmax))
        {
            if (HeapTupleHeaderGetCmax(tuple) >= GetCurrentCommandId(false))
                return true;    /* deleted after scan started */
            else
                return false;    /* deleted before scan started */
        }
        if (TransactionIdIsInProgress(xmax))
            return true;
        if (TransactionIdDidCommit(xmax))
            return false;
        return true;
    }

    if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
    {
        if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
            return true;
        if (HeapTupleHeaderGetCmax(tuple) >= GetCurrentCommandId(false))
            return true;        /* deleted after scan started */
        else
            return false;        /* deleted before scan started */
    }

    if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmax(tuple)))
        return true;

    if (!TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple)))
    {
        /* it must have aborted or crashed */
        SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
                    InvalidTransactionId);
        return true;
    }

    /* xmax transaction committed */

    if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
    {
        SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
                    InvalidTransactionId);
        return true;
    }

    SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED,
                HeapTupleHeaderGetRawXmax(tuple));
    return false;
}
#endif

#endif


