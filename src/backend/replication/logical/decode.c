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
/* -------------------------------------------------------------------------
 *
 * decode.c
 *        This module decodes WAL records read using xlogreader.h's APIs for the
 *        purpose of logical decoding by passing information to the
 *        reorderbuffer module (containing the actual changes) and to the
 *        snapbuild module to build a fitting catalog snapshot (to be able to
 *        properly decode the changes in the reorderbuffer).
 *
 * NOTE:
 *        This basically tries to handle all low level xlog stuff for
 *        reorderbuffer.c and snapbuild.c. There's some minor leakage where a
 *        specific record's struct is used to pass data along, but those just
 *        happen to contain the right amount of data in a convenient
 *        format. There isn't and shouldn't be much intelligence about the
 *        contents of records in here except turning them into a more usable
 *        format.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *      src/backend/replication/logical/decode.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/heapam_xlog.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog_internal.h"
#include "access/xlogutils.h"
#include "access/xlogreader.h"
#include "access/xlogrecord.h"

#include "catalog/pg_control.h"

#include "replication/decode.h"
#include "replication/logical.h"
#include "replication/message.h"
#include "replication/reorderbuffer.h"
#include "replication/origin.h"
#include "replication/snapbuild.h"

#include "storage/standby.h"
#ifdef __STORAGE_SCALABLE__
#include "nodes/bitmapset.h"
#include "utils/relfilenodemap.h"
#include "replication/slot.h"
#include "miscadmin.h"
#endif
#ifdef _PUB_SUB_RELIABLE_
#include "replication/walsender_private.h"
#endif


#ifdef __STORAGE_SCALABLE__
static HTAB *confirmHash = NULL;

#define Confirm_HASH_SIZE 32768
#endif

typedef struct XLogRecordBuffer
{
    XLogRecPtr    origptr;
    XLogRecPtr    endptr;
    XLogReaderState *record;
} XLogRecordBuffer;

/* RMGR Handlers */
static void DecodeXLogOp(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);
static void DecodeHeapOp(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);
static void DecodeHeap2Op(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);
static void DecodeXactOp(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);
static void DecodeStandbyOp(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);
static void DecodeLogicalMsgOp(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);

/* individual record(group)'s handlers */
static void DecodeInsert(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);
static void DecodeUpdate(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);
static void DecodeDelete(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);
static void DecodeMultiInsert(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);
static void DecodeSpecConfirm(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);

static void DecodeCommit(LogicalDecodingContext *ctx, XLogRecordBuffer *buf,
             xl_xact_parsed_commit *parsed, TransactionId xid);
static void DecodeAbort(LogicalDecodingContext *ctx, XLogRecordBuffer *buf,
            xl_xact_parsed_abort *parsed, TransactionId xid);

/* common function to decode tuples */
static void DecodeXLogTuple(char *data, Size len, ReorderBufferTupleBuf *tup);
#ifdef __STORAGE_SCALABLE__
static bool RelationShardIsTarget(Oid relid, int32 shardid);
static void SetSkipSpecConfirm(TransactionId xid);
static bool SkipSpecConfirm(TransactionId xid);
#endif

#ifdef _PUB_SUB_RELIABLE_
static uint8 LogicalDecoding_XLR_INFO_MASK = 0x00;        /* By default, logical decoding is performed on the user WAL stream only . */

bool LogicalDecoding_XLR_InfoMask_Test(uint8 infoMask)
{
    return (bool) ((LogicalDecoding_XLR_INFO_MASK & infoMask) != 0);
}

void LogicalDecoding_XLR_InfoMask_Set(uint8 infoMask)
{
    LogicalDecoding_XLR_INFO_MASK |= infoMask;
}

void LogicalDecoding_XLR_InfoMask_Clear(uint8 infoMask)
{
    LogicalDecoding_XLR_INFO_MASK &= ~infoMask;
}

#endif

/*
 * Take every XLogReadRecord()ed record and perform the actions required to
 * decode it using the output plugin already setup in the logical decoding
 * context.
 *
 * NB: Note that every record's xid needs to be processed by reorderbuffer
 * (xids contained in the content of records are not relevant for this rule).
 * That means that for records which'd otherwise not go through the
 * reorderbuffer ReorderBufferProcessXid() has to be called. We don't want to
 * call ReorderBufferProcessXid for each record type by default, because
 * e.g. empty xacts can be handled more efficiently if there's no previous
 * state for them.
 */
void
LogicalDecodingProcessRecord(LogicalDecodingContext *ctx, XLogReaderState *record)
{// #lizard forgives
    XLogRecordBuffer buf;

#ifdef _PUB_SUB_RELIABLE_
    uint8 info = XLogRecGetInfo(record);

    if (info & XLR_CLUSTER_STREAM)
    {
        if (false == LogicalDecoding_XLR_InfoMask_Test(XLR_CLUSTER_STREAM))
        {
            /* skip record from remote cluster wal stream */
            return;
        }
    }
    else if (info & XLR_INTERNAL_STREAM)
    {
        if (false == LogicalDecoding_XLR_InfoMask_Test(XLR_INTERNAL_STREAM))
        {
            /* skip record from internal wal stream */
            return;
        }
    }
#endif

    buf.origptr = ctx->reader->ReadRecPtr;
    buf.endptr = ctx->reader->EndRecPtr;
    buf.record = record;

    /* cast so we get a warning when new rmgrs are added */
    switch ((RmgrIds) XLogRecGetRmid(record))
    {
            /*
             * Rmgrs we care about for logical decoding. Add new rmgrs in
             * rmgrlist.h's order.
             */
        case RM_XLOG_ID:
            DecodeXLogOp(ctx, &buf);
            break;

        case RM_XACT_ID:
            DecodeXactOp(ctx, &buf);
            break;

        case RM_STANDBY_ID:
            DecodeStandbyOp(ctx, &buf);
            break;

        case RM_HEAP2_ID:
            DecodeHeap2Op(ctx, &buf);
            break;

        case RM_HEAP_ID:
            DecodeHeapOp(ctx, &buf);
            break;

        case RM_LOGICALMSG_ID:
            DecodeLogicalMsgOp(ctx, &buf);
            break;

            /*
             * Rmgrs irrelevant for logical decoding; they describe stuff not
             * represented in logical decoding. Add new rmgrs in rmgrlist.h's
             * order.
             */
        case RM_SMGR_ID:
        case RM_CLOG_ID:
        case RM_DBASE_ID:
        case RM_TBLSPC_ID:
        case RM_MULTIXACT_ID:
        case RM_RELMAP_ID:
        case RM_BTREE_ID:
        case RM_HASH_ID:
        case RM_GIN_ID:
        case RM_GIST_ID:
        case RM_SEQ_ID:
        case RM_SPGIST_ID:
        case RM_BRIN_ID:
        case RM_COMMIT_TS_ID:
#ifdef PGXC 
        case RM_BARRIER_ID:
#endif
        case RM_REPLORIGIN_ID:
        case RM_GENERIC_ID:
#ifdef _SHARDING_
        case RM_EXTENT_ID:
#endif
#ifdef _MLS_
        case RM_REL_CRYPT_ID:
#endif
#ifdef _PUB_SUB_RELIABLE_
        case RM_RELICATION_SLOT_ID:
#endif

            /* just deal with xid, and done */
            ReorderBufferProcessXid(ctx->reorder, XLogRecGetXid(record),
                                    buf.origptr);
            break;
        case RM_NEXT_ID:
            elog(ERROR, "unexpected RM_NEXT_ID rmgr_id: %u", (RmgrIds) XLogRecGetRmid(buf.record));
    }
}

/*
 * Handle rmgr XLOG_ID records for DecodeRecordIntoReorderBuffer().
 */
static void
DecodeXLogOp(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{// #lizard forgives
    SnapBuild  *builder = ctx->snapshot_builder;
    uint8        info = XLogRecGetInfo(buf->record) & ~XLR_INFO_MASK;

    ReorderBufferProcessXid(ctx->reorder, XLogRecGetXid(buf->record),
                            buf->origptr);

    switch (info)
    {
            /* this is also used in END_OF_RECOVERY checkpoints */
        case XLOG_CHECKPOINT_SHUTDOWN:
        case XLOG_END_OF_RECOVERY:
            SnapBuildSerializationPoint(builder, buf->origptr);

            break;
        case XLOG_CHECKPOINT_ONLINE:

            /*
             * a RUNNING_XACTS record will have been logged near to this, we
             * can restart from there.
             */
            break;
        case XLOG_CLEAN_2PC_FILE:
        case XLOG_CREATE_2PC_FILE:
        case XLOG_RECORD_2PC_TIMESTAMP:
            break;
        case XLOG_NOOP:
        case XLOG_NEXTOID:
        case XLOG_SWITCH:
        case XLOG_BACKUP_END:
        case XLOG_PARAMETER_CHANGE:
        case XLOG_RESTORE_POINT:
        case XLOG_FPW_CHANGE:
        case XLOG_FPI_FOR_HINT:
        case XLOG_FPI:
#ifdef __TBASE__
        case XLOG_MVCC:
#endif
            break;
        default:
            elog(ERROR, "unexpected RM_XLOG_ID record type: %u", info);
    }
}

/*
 * Handle rmgr XACT_ID records for DecodeRecordIntoReorderBuffer().
 */
static void
DecodeXactOp(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{// #lizard forgives
    SnapBuild  *builder = ctx->snapshot_builder;
    ReorderBuffer *reorder = ctx->reorder;
    XLogReaderState *r = buf->record;
    uint8        info = XLogRecGetInfo(r) & XLOG_XACT_OPMASK;

    /*
     * No point in doing anything yet, data could not be decoded anyway. It's
     * ok not to call ReorderBufferProcessXid() in that case, except in the
     * assignment case there'll not be any later records with the same xid;
     * and in the assignment case we'll not decode those xacts.
     */
    if (SnapBuildCurrentState(builder) < SNAPBUILD_FULL_SNAPSHOT)
        return;

    switch (info)
    {
        case XLOG_XACT_COMMIT:
        case XLOG_XACT_COMMIT_PREPARED:
            {
                xl_xact_commit *xlrec;
                xl_xact_parsed_commit parsed;
                TransactionId xid;

#ifdef _PUB_SUB_RELIABLE_
				/*
				 * Make sure we sync current transaction to synchronous standbys
				 * before commit to avoid data lost in master-master replication.
				 *
				 * NB: this goes effect under async replication,thus sync replicaton   
				 * is not protected which need to be consider later.
				 */
				if(MyWalSnd && MyWalSnd->sync_standby_priority == 0)
					SyncRepWaitForLSN(buf->endptr, true);
#endif

                xlrec = (xl_xact_commit *) XLogRecGetData(r);
                ParseCommitRecord(XLogRecGetInfo(buf->record), xlrec, &parsed);

                if (!TransactionIdIsValid(parsed.twophase_xid))
                    xid = XLogRecGetXid(r);
                else
                    xid = parsed.twophase_xid;

                DecodeCommit(ctx, buf, &parsed, xid);
                break;
            }
        case XLOG_XACT_ABORT:
        case XLOG_XACT_ABORT_PREPARED:
            {
                xl_xact_abort *xlrec;
                xl_xact_parsed_abort parsed;
                TransactionId xid;

                xlrec = (xl_xact_abort *) XLogRecGetData(r);
                ParseAbortRecord(XLogRecGetInfo(buf->record), xlrec, &parsed);

                if (!TransactionIdIsValid(parsed.twophase_xid))
                    xid = XLogRecGetXid(r);
                else
                    xid = parsed.twophase_xid;

                DecodeAbort(ctx, buf, &parsed, xid);
                break;
            }
        case XLOG_XACT_ASSIGNMENT:
            {
                xl_xact_assignment *xlrec;
                int            i;
                TransactionId *sub_xid;

                xlrec = (xl_xact_assignment *) XLogRecGetData(r);

                sub_xid = &xlrec->xsub[0];

                for (i = 0; i < xlrec->nsubxacts; i++)
                {
                    ReorderBufferAssignChild(reorder, xlrec->xtop,
                                             *(sub_xid++), buf->origptr);
                }
                break;
            }
        case XLOG_XACT_PREPARE:

            /*
             * Currently decoding ignores PREPARE TRANSACTION and will just
             * decode the transaction when the COMMIT PREPARED is sent or
             * throw away the transaction's contents when a ROLLBACK PREPARED
             * is received. In the future we could add code to expose prepared
             * transactions in the changestream allowing for a kind of
             * distributed 2PC.
             */
            ReorderBufferProcessXid(reorder, XLogRecGetXid(r), buf->origptr);
            break;

#ifdef __TBASE__
		case XLOG_XACT_ACQUIRE_GTS:
			{
				/* nothing to do. */
			}
			break;
#endif
        default:
            elog(ERROR, "unexpected RM_XACT_ID record type: %u", info);
    }
}

/*
 * Handle rmgr STANDBY_ID records for DecodeRecordIntoReorderBuffer().
 */
static void
DecodeStandbyOp(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
    SnapBuild  *builder = ctx->snapshot_builder;
    XLogReaderState *r = buf->record;
    uint8        info = XLogRecGetInfo(r) & ~XLR_INFO_MASK;

    ReorderBufferProcessXid(ctx->reorder, XLogRecGetXid(r), buf->origptr);

    switch (info)
    {
        case XLOG_RUNNING_XACTS:
            {
                xl_running_xacts *running = (xl_running_xacts *) XLogRecGetData(r);

                SnapBuildProcessRunningXacts(builder, buf->origptr, running);

                /*
                 * Abort all transactions that we keep track of, that are
                 * older than the record's oldestRunningXid. This is the most
                 * convenient spot for doing so since, in contrast to shutdown
                 * or end-of-recovery checkpoints, we have information about
                 * all running transactions which includes prepared ones,
                 * while shutdown checkpoints just know that no non-prepared
                 * transactions are in progress.
                 */
                ReorderBufferAbortOld(ctx->reorder, running->oldestRunningXid);
            }
            break;
        case XLOG_STANDBY_LOCK:
            break;
        case XLOG_INVALIDATIONS:
            {
                xl_invalidations *invalidations =
                (xl_invalidations *) XLogRecGetData(r);

                ReorderBufferImmediateInvalidation(
                                                   ctx->reorder, invalidations->nmsgs, invalidations->msgs);
            }
            break;
        default:
            elog(ERROR, "unexpected RM_STANDBY_ID record type: %u", info);
    }
}

/*
 * Handle rmgr HEAP2_ID records for DecodeRecordIntoReorderBuffer().
 */
static void
DecodeHeap2Op(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{// #lizard forgives
    uint8        info = XLogRecGetInfo(buf->record) & XLOG_HEAP_OPMASK;
    TransactionId xid = XLogRecGetXid(buf->record);
    SnapBuild  *builder = ctx->snapshot_builder;

    ReorderBufferProcessXid(ctx->reorder, xid, buf->origptr);

    /* no point in doing anything yet */
    if (SnapBuildCurrentState(builder) < SNAPBUILD_FULL_SNAPSHOT)
        return;

    switch (info)
    {
        case XLOG_HEAP2_MULTI_INSERT:
            if (SnapBuildProcessChange(builder, xid, buf->origptr))
                DecodeMultiInsert(ctx, buf);
            break;
        case XLOG_HEAP2_NEW_CID:
            {
                xl_heap_new_cid *xlrec;

                xlrec = (xl_heap_new_cid *) XLogRecGetData(buf->record);
                SnapBuildProcessNewCid(builder, xid, buf->origptr, xlrec);

                break;
            }
        case XLOG_HEAP2_REWRITE:

            /*
             * Although these records only exist to serve the needs of logical
             * decoding, all the work happens as part of crash or archive
             * recovery, so we don't need to do anything here.
             */
            break;

            /*
             * Everything else here is just low level physical stuff we're not
             * interested in.
             */
        case XLOG_HEAP2_FREEZE_PAGE:
        case XLOG_HEAP2_CLEAN:
        case XLOG_HEAP2_CLEANUP_INFO:
        case XLOG_HEAP2_VISIBLE:
        case XLOG_HEAP2_LOCK_UPDATED:
            break;
        default:
            elog(ERROR, "unexpected RM_HEAP2_ID record type: %u", info);
    }
}

/*
 * Handle rmgr HEAP_ID records for DecodeRecordIntoReorderBuffer().
 */
static void
DecodeHeapOp(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{// #lizard forgives
    uint8        info = XLogRecGetInfo(buf->record) & XLOG_HEAP_OPMASK;
    TransactionId xid = XLogRecGetXid(buf->record);
    SnapBuild  *builder = ctx->snapshot_builder;

    ReorderBufferProcessXid(ctx->reorder, xid, buf->origptr);

    /* no point in doing anything yet */
    if (SnapBuildCurrentState(builder) < SNAPBUILD_FULL_SNAPSHOT)
        return;

    switch (info)
    {
        case XLOG_HEAP_INSERT:
            if (SnapBuildProcessChange(builder, xid, buf->origptr))
                DecodeInsert(ctx, buf);
            break;

            /*
             * Treat HOT update as normal updates. There is no useful
             * information in the fact that we could make it a HOT update
             * locally and the WAL layout is compatible.
             */
        case XLOG_HEAP_HOT_UPDATE:
        case XLOG_HEAP_UPDATE:
            if (SnapBuildProcessChange(builder, xid, buf->origptr))
                DecodeUpdate(ctx, buf);
            break;

        case XLOG_HEAP_DELETE:
            if (SnapBuildProcessChange(builder, xid, buf->origptr))
                DecodeDelete(ctx, buf);
            break;

        case XLOG_HEAP_INPLACE:

            /*
             * Inplace updates are only ever performed on catalog tuples and
             * can, per definition, not change tuple visibility.  Since we
             * don't decode catalog tuples, we're not interested in the
             * record's contents.
             *
             * In-place updates can be used either by XID-bearing transactions
             * (e.g.  in CREATE INDEX CONCURRENTLY) or by XID-less
             * transactions (e.g.  VACUUM).  In the former case, the commit
             * record will include cache invalidations, so we mark the
             * transaction as catalog modifying here. Currently that's
             * redundant because the commit will do that as well, but once we
             * support decoding in-progress relations, this will be important.
             */
            if (!TransactionIdIsValid(xid))
                break;

            SnapBuildProcessChange(builder, xid, buf->origptr);
            ReorderBufferXidSetCatalogChanges(ctx->reorder, xid, buf->origptr);
            break;

        case XLOG_HEAP_CONFIRM:
            if (SnapBuildProcessChange(builder, xid, buf->origptr))
                DecodeSpecConfirm(ctx, buf);
            break;

        case XLOG_HEAP_LOCK:
            /* we don't care about row level locks for now */
            break;

        default:
            elog(ERROR, "unexpected RM_HEAP_ID record type: %u", info);
            break;
    }
}

static inline bool
FilterByOrigin(LogicalDecodingContext *ctx, RepOriginId origin_id)
{
    if (ctx->callbacks.filter_by_origin_cb == NULL)
        return false;

    return filter_by_origin_cb_wrapper(ctx, origin_id);
}

/*
 * Handle rmgr LOGICALMSG_ID records for DecodeRecordIntoReorderBuffer().
 */
static void
DecodeLogicalMsgOp(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{// #lizard forgives
    SnapBuild  *builder = ctx->snapshot_builder;
    XLogReaderState *r = buf->record;
    TransactionId xid = XLogRecGetXid(r);
    uint8        info = XLogRecGetInfo(r) & ~XLR_INFO_MASK;
    RepOriginId origin_id = XLogRecGetOrigin(r);
    Snapshot    snapshot;
    xl_logical_message *message;

    if (info != XLOG_LOGICAL_MESSAGE)
        elog(ERROR, "unexpected RM_LOGICALMSG_ID record type: %u", info);

    ReorderBufferProcessXid(ctx->reorder, XLogRecGetXid(r), buf->origptr);

    /* No point in doing anything yet. */
    if (SnapBuildCurrentState(builder) < SNAPBUILD_FULL_SNAPSHOT)
        return;

    message = (xl_logical_message *) XLogRecGetData(r);

    if (message->dbId != ctx->slot->data.database ||
        FilterByOrigin(ctx, origin_id))
        return;

    if (message->transactional &&
        !SnapBuildProcessChange(builder, xid, buf->origptr))
        return;
    else if (!message->transactional &&
             (SnapBuildCurrentState(builder) != SNAPBUILD_CONSISTENT ||
              SnapBuildXactNeedsSkip(builder, buf->origptr)))
        return;

    snapshot = SnapBuildGetOrBuildSnapshot(builder, xid);
    ReorderBufferQueueMessage(ctx->reorder, xid, snapshot, buf->endptr,
                              message->transactional,
                              message->message, /* first part of message is
                                                 * prefix */
                              message->message_size,
                              message->message + message->prefix_size);
}

/*
 * Consolidated commit record handling between the different form of commit
 * records.
 */
static void
DecodeCommit(LogicalDecodingContext *ctx, XLogRecordBuffer *buf,
             xl_xact_parsed_commit *parsed, TransactionId xid)
{// #lizard forgives
    XLogRecPtr    origin_lsn = InvalidXLogRecPtr;
    TimestampTz commit_time = parsed->xact_time;
    RepOriginId origin_id = XLogRecGetOrigin(buf->record);
    int            i;

    if (parsed->xinfo & XACT_XINFO_HAS_ORIGIN)
    {
        origin_lsn = parsed->origin_lsn;
        commit_time = parsed->origin_timestamp;
    }

    /*
     * Process invalidation messages, even if we're not interested in the
     * transaction's contents, since the various caches need to always be
     * consistent.
     */
    if (parsed->nmsgs > 0)
    {
        ReorderBufferAddInvalidations(ctx->reorder, xid, buf->origptr,
                                      parsed->nmsgs, parsed->msgs);
        ReorderBufferXidSetCatalogChanges(ctx->reorder, xid, buf->origptr);
    }

    SnapBuildCommitTxn(ctx->snapshot_builder, buf->origptr, xid,
                       parsed->nsubxacts, parsed->subxacts);

    /* ----
     * Check whether we are interested in this specific transaction, and tell
     * the reorderbuffer to forget the content of the (sub-)transactions
     * if not.
     *
     * There can be several reasons we might not be interested in this
     * transaction:
     * 1) We might not be interested in decoding transactions up to this
     *      LSN. This can happen because we previously decoded it and now just
     *      are restarting or if we haven't assembled a consistent snapshot yet.
     * 2) The transaction happened in another database.
     * 3) The output plugin is not interested in the origin.
     *
     * We can't just use ReorderBufferAbort() here, because we need to execute
     * the transaction's invalidations.  This currently won't be needed if
     * we're just skipping over the transaction because currently we only do
     * so during startup, to get to the first transaction the client needs. As
     * we have reset the catalog caches before starting to read WAL, and we
     * haven't yet touched any catalogs, there can't be anything to invalidate.
     * But if we're "forgetting" this commit because it's it happened in
     * another database, the invalidations might be important, because they
     * could be for shared catalogs and we might have loaded data into the
     * relevant syscaches.
     * ---
     */
    if (SnapBuildXactNeedsSkip(ctx->snapshot_builder, buf->origptr) ||
        (parsed->dbId != InvalidOid && parsed->dbId != ctx->slot->data.database) ||
        FilterByOrigin(ctx, origin_id))
    {
        for (i = 0; i < parsed->nsubxacts; i++)
        {
            ReorderBufferForget(ctx->reorder, parsed->subxacts[i], buf->origptr);
        }
        ReorderBufferForget(ctx->reorder, xid, buf->origptr);

        return;
    }

    /* tell the reorderbuffer about the surviving subtransactions */
    for (i = 0; i < parsed->nsubxacts; i++)
    {
        ReorderBufferCommitChild(ctx->reorder, xid, parsed->subxacts[i],
                                 buf->origptr, buf->endptr);
    }

    /* replay actions of all transaction + subtransactions in order */
    ReorderBufferCommit(ctx->reorder, xid, buf->origptr, buf->endptr,
                        commit_time, origin_id, origin_lsn);
}

/*
 * Get the data from the various forms of abort records and pass it on to
 * snapbuild.c and reorderbuffer.c
 */
static void
DecodeAbort(LogicalDecodingContext *ctx, XLogRecordBuffer *buf,
            xl_xact_parsed_abort *parsed, TransactionId xid)
{
    int            i;

    for (i = 0; i < parsed->nsubxacts; i++)
    {
        ReorderBufferAbort(ctx->reorder, parsed->subxacts[i],
                           buf->record->EndRecPtr);
    }

    ReorderBufferAbort(ctx->reorder, xid, buf->record->EndRecPtr);
}

/*
 * Parse XLOG_HEAP_INSERT (not MULTI_INSERT!) records into tuplebufs.
 *
 * Deletes can contain the new tuple.
 */
static void
DecodeInsert(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{// #lizard forgives
    XLogReaderState *r = buf->record;
    xl_heap_insert *xlrec;
    ReorderBufferChange *change;
    RelFileNode target_node;

    xlrec = (xl_heap_insert *) XLogRecGetData(r);

    /* only interested in our database */
    XLogRecGetBlockTag(r, 0, &target_node, NULL, NULL);
    if (target_node.dbNode != ctx->slot->data.database)
        return;

    /* output plugin doesn't look for this origin, no need to queue */
    if (FilterByOrigin(ctx, XLogRecGetOrigin(r)))
        return;

#ifdef __STORAGE_SCALABLE__
    Assert(MyReplicationSlot);

    if (xlrec->flags & XLH_INSERT_IS_SPECULATIVE)
    {
        SkipSpecConfirm(XLogRecGetXid(r));
    }
    
    /* filter the tuple if not interested in */
    if (OidIsValid(MyReplicationSlot->relid))
    {
        Size        datalen;
        char       *data;
        xl_heap_header xlhdr;
        Oid relationId;
        
        StartTransactionCommand();
        
        relationId = RelidByRelfilenode(target_node.spcNode, target_node.relNode);

        AbortCurrentTransaction();

        /* not our target relation */
        if (relationId != MyReplicationSlot->relid)
        {
            if (xlrec->flags & XLH_INSERT_IS_SPECULATIVE)
            {
                TransactionId xid = XLogRecGetXid(r);
                SetSkipSpecConfirm(xid);
            }
            return;
        }

        data = XLogRecGetBlockData(r, 0, &datalen);
        memcpy((char *) &xlhdr, data, SizeOfHeapHeader);

        if (MyReplicationSlot->shards)
        {
            /* not our target shard */
            if (!bms_is_member(xlhdr.t_shardid, MyReplicationSlot->shards))
            {
                if (xlrec->flags & XLH_INSERT_IS_SPECULATIVE)
                {
                    TransactionId xid = XLogRecGetXid(r);
                    SetSkipSpecConfirm(xid);
                }
                return;
            }
        }

        //MyReplicationSlot->ntups_insert++;
    }
    else if (MyReplicationSlot->npubs)
    {
        bool        found;
        Size        datalen;
        char       *data;
        xl_heap_header xlhdr;
        Oid relationId;

        if (xlrec->flags & XLH_INSERT_CONTAINS_NEW_TUPLE)
        {

            StartTransactionCommand();
            
            relationId = RelidByRelfilenode(target_node.spcNode, target_node.relNode);

            AbortCurrentTransaction();

            data = XLogRecGetBlockData(r, 0, &datalen);
            memcpy((char *) &xlhdr, data, SizeOfHeapHeader);

            found = RelationShardIsTarget(relationId, xlhdr.t_shardid);

            if (!found)
            {
                if (xlrec->flags & XLH_INSERT_IS_SPECULATIVE)
                {
                    TransactionId xid = XLogRecGetXid(r);
                    SetSkipSpecConfirm(xid);
                }
                return;
            }
        }
    }
#endif

    change = ReorderBufferGetChange(ctx->reorder);
    if (!(xlrec->flags & XLH_INSERT_IS_SPECULATIVE))
        change->action = REORDER_BUFFER_CHANGE_INSERT;
    else
        change->action = REORDER_BUFFER_CHANGE_INTERNAL_SPEC_INSERT;
    change->origin_id = XLogRecGetOrigin(r);

    memcpy(&change->data.tp.relnode, &target_node, sizeof(RelFileNode));

    if (xlrec->flags & XLH_INSERT_CONTAINS_NEW_TUPLE)
    {
        Size        datalen;
        char       *tupledata = XLogRecGetBlockData(r, 0, &datalen);
        Size        tuplelen = datalen - SizeOfHeapHeader;

        change->data.tp.newtuple =
            ReorderBufferGetTupleBuf(ctx->reorder, tuplelen);

        DecodeXLogTuple(tupledata, datalen, change->data.tp.newtuple);

#ifdef __STORAGE_SCALABLE__
        {
            Size datalen;
            xl_heap_header *xlhdr;
            char *data = XLogRecGetBlockData(r, 0, &datalen);
            HeapTupleHeader header = change->data.tp.newtuple->tuple.t_data;

            xlhdr = (xl_heap_header *)data;
            header->t_shardid = xlhdr->t_shardid;
        }
#endif
    }

    change->data.tp.clear_toast_afterwards = true;

    ReorderBufferQueueChange(ctx->reorder, XLogRecGetXid(r), buf->origptr, change);
}

/*
 * Parse XLOG_HEAP_UPDATE and XLOG_HEAP_HOT_UPDATE, which have the same layout
 * in the record, from wal into proper tuplebufs.
 *
 * Updates can possibly contain a new tuple and the old primary key.
 */
static void
DecodeUpdate(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{// #lizard forgives
    XLogReaderState *r = buf->record;
    xl_heap_update *xlrec;
    ReorderBufferChange *change;
    char       *data;
    RelFileNode target_node;

    xlrec = (xl_heap_update *) XLogRecGetData(r);

    /* only interested in our database */
    XLogRecGetBlockTag(r, 0, &target_node, NULL, NULL);
    if (target_node.dbNode != ctx->slot->data.database)
        return;

    /* output plugin doesn't look for this origin, no need to queue */
    if (FilterByOrigin(ctx, XLogRecGetOrigin(r)))
        return;

    change = ReorderBufferGetChange(ctx->reorder);
    change->action = REORDER_BUFFER_CHANGE_UPDATE;
    change->origin_id = XLogRecGetOrigin(r);
    memcpy(&change->data.tp.relnode, &target_node, sizeof(RelFileNode));

#ifdef __STORAGE_SCALABLE__
    Assert(MyReplicationSlot);

    /* filter the tuple if not interested in */
    if (OidIsValid(MyReplicationSlot->relid))
    {
        Size        datalen;
        char       *data;
        xl_heap_header xlhdr;
        Oid relationId;

        StartTransactionCommand();
        
        relationId = RelidByRelfilenode(target_node.spcNode, target_node.relNode);

        AbortCurrentTransaction();

        /* not our target relation */
        if (relationId != MyReplicationSlot->relid)
        {
            return;
        }

        memset(&xlhdr, 0, sizeof(xl_heap_header));
        
        if (xlrec->flags & XLH_UPDATE_CONTAINS_NEW_TUPLE)
        {
            data = XLogRecGetBlockData(r, 0, &datalen);
            memcpy((char *) &xlhdr, data, SizeOfHeapHeader);
        }
        else if (xlrec->flags & XLH_UPDATE_CONTAINS_OLD)
        {
            data = XLogRecGetData(r) + SizeOfHeapUpdate;
            memcpy((char *) &xlhdr, data, SizeOfHeapHeader);
        }

        if (MyReplicationSlot->shards)
        {
            /* not our target shard */
            if (!bms_is_member(xlhdr.t_shardid, MyReplicationSlot->shards))
            {
                return;
            }
        }
#if 0
        if (xlrec->flags & XLH_UPDATE_CONTAINS_NEW_TUPLE)
        {
            MyReplicationSlot->ntups_insert++;
        }

        if (xlrec->flags & XLH_UPDATE_CONTAINS_OLD)
        {
            MyReplicationSlot->ntups_delete++;
        }
#endif
    }
    else if (MyReplicationSlot->npubs)
    {
        bool        found;
        Size        datalen;
        char       *data;
        xl_heap_header xlhdr;
        Oid relationId;

        StartTransactionCommand();
        
        relationId = RelidByRelfilenode(target_node.spcNode, target_node.relNode);

        AbortCurrentTransaction();

        memset(&xlhdr, 0, sizeof(xl_heap_header));
        
        if (xlrec->flags & XLH_UPDATE_CONTAINS_NEW_TUPLE)
        {
            data = XLogRecGetBlockData(r, 0, &datalen);
            memcpy((char *) &xlhdr, data, SizeOfHeapHeader);
        }
        else if (xlrec->flags & XLH_UPDATE_CONTAINS_OLD)
        {
            data = XLogRecGetData(r) + SizeOfHeapUpdate;
            memcpy((char *) &xlhdr, data, SizeOfHeapHeader);
        }
        else
        {
            xlhdr.t_shardid = InvalidShardID;
        }

        found = RelationShardIsTarget(relationId, xlhdr.t_shardid);

        if (!found)
        {
            return;
        }
    }
#endif

    if (xlrec->flags & XLH_UPDATE_CONTAINS_NEW_TUPLE)
    {
        Size        datalen;
        Size        tuplelen;

        data = XLogRecGetBlockData(r, 0, &datalen);

        tuplelen = datalen - SizeOfHeapHeader;

        change->data.tp.newtuple =
            ReorderBufferGetTupleBuf(ctx->reorder, tuplelen);

        DecodeXLogTuple(data, datalen, change->data.tp.newtuple);

#ifdef __STORAGE_SCALABLE__
        {
            Size datalen;
            xl_heap_header *xlhdr;
            char *data = XLogRecGetBlockData(r, 0, &datalen);
            HeapTupleHeader header = change->data.tp.newtuple->tuple.t_data;

            xlhdr = (xl_heap_header *)data;
            header->t_shardid = xlhdr->t_shardid;
        }
#endif
    }

    if (xlrec->flags & XLH_UPDATE_CONTAINS_OLD)
    {
        Size        datalen;
        Size        tuplelen;

        /* caution, remaining data in record is not aligned */
        data = XLogRecGetData(r) + SizeOfHeapUpdate;
        datalen = XLogRecGetDataLen(r) - SizeOfHeapUpdate;
        tuplelen = datalen - SizeOfHeapHeader;

        change->data.tp.oldtuple =
            ReorderBufferGetTupleBuf(ctx->reorder, tuplelen);

        DecodeXLogTuple(data, datalen, change->data.tp.oldtuple);
    }

    change->data.tp.clear_toast_afterwards = true;

    ReorderBufferQueueChange(ctx->reorder, XLogRecGetXid(r), buf->origptr, change);
}

/*
 * Parse XLOG_HEAP_DELETE from wal into proper tuplebufs.
 *
 * Deletes can possibly contain the old primary key.
 */
static void
DecodeDelete(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{// #lizard forgives
    XLogReaderState *r = buf->record;
    xl_heap_delete *xlrec;
    ReorderBufferChange *change;
    RelFileNode target_node;

    xlrec = (xl_heap_delete *) XLogRecGetData(r);

    /* only interested in our database */
    XLogRecGetBlockTag(r, 0, &target_node, NULL, NULL);
    if (target_node.dbNode != ctx->slot->data.database)
        return;

    /*
     * Super deletions are irrelevant for logical decoding, it's driven by the
     * confirmation records.
     */
    if (xlrec->flags & XLH_DELETE_IS_SUPER)
        return;

    /* output plugin doesn't look for this origin, no need to queue */
    if (FilterByOrigin(ctx, XLogRecGetOrigin(r)))
        return;

#ifdef __STORAGE_SCALABLE__
    Assert(MyReplicationSlot);

    /* filter the tuple if not interested in */
    if (OidIsValid(MyReplicationSlot->relid))
    {
        xl_heap_header xlhdr;
        Oid relationId;

        StartTransactionCommand();
        
        relationId = RelidByRelfilenode(target_node.spcNode, target_node.relNode);

        AbortCurrentTransaction();

        /* not our target relation */
        if (relationId != MyReplicationSlot->relid)
        {
            return;
        }

        if (xlrec->flags & XLH_DELETE_CONTAINS_OLD)
        {
            memcpy((char *) &xlhdr, (char *) xlrec + SizeOfHeapDelete, SizeOfHeapHeader);

            if (MyReplicationSlot->shards)
            {
                /* not our target shard */
                if (!bms_is_member(xlhdr.t_shardid, MyReplicationSlot->shards))
                {
                    return;
                }
            }

            //MyReplicationSlot->ntups_delete++;
        }
    }
    else if (MyReplicationSlot->npubs)
    {
        bool        found;
        xl_heap_header xlhdr;
        Oid relationId;

        if (xlrec->flags & XLH_DELETE_CONTAINS_OLD)
        {

            StartTransactionCommand();
            
            relationId = RelidByRelfilenode(target_node.spcNode, target_node.relNode);

            AbortCurrentTransaction();


            memcpy((char *) &xlhdr, (char *) xlrec + SizeOfHeapDelete, SizeOfHeapHeader);

            found = RelationShardIsTarget(relationId, xlhdr.t_shardid);

            if (!found)
            {
                return;
            }
        }
    }
#endif

    change = ReorderBufferGetChange(ctx->reorder);
    change->action = REORDER_BUFFER_CHANGE_DELETE;
    change->origin_id = XLogRecGetOrigin(r);

    memcpy(&change->data.tp.relnode, &target_node, sizeof(RelFileNode));

    /* old primary key stored */
    if (xlrec->flags & XLH_DELETE_CONTAINS_OLD)
    {
        Size        datalen = XLogRecGetDataLen(r) - SizeOfHeapDelete;
        Size        tuplelen = datalen - SizeOfHeapHeader;

        Assert(XLogRecGetDataLen(r) > (SizeOfHeapDelete + SizeOfHeapHeader));

        change->data.tp.oldtuple =
            ReorderBufferGetTupleBuf(ctx->reorder, tuplelen);

        DecodeXLogTuple((char *) xlrec + SizeOfHeapDelete,
                        datalen, change->data.tp.oldtuple);
    }

    change->data.tp.clear_toast_afterwards = true;

    ReorderBufferQueueChange(ctx->reorder, XLogRecGetXid(r), buf->origptr, change);
}

/*
 * Decode XLOG_HEAP2_MULTI_INSERT_insert record into multiple tuplebufs.
 *
 * Currently MULTI_INSERT will always contain the full tuples.
 */
static void
DecodeMultiInsert(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{// #lizard forgives
    XLogReaderState *r = buf->record;
    xl_heap_multi_insert *xlrec;
    int            i;
    char       *data;
    char       *tupledata;
    Size        tuplelen;
    RelFileNode rnode;

    xlrec = (xl_heap_multi_insert *) XLogRecGetData(r);

    /* only interested in our database */
    XLogRecGetBlockTag(r, 0, &rnode, NULL, NULL);
    if (rnode.dbNode != ctx->slot->data.database)
        return;

    /* output plugin doesn't look for this origin, no need to queue */
    if (FilterByOrigin(ctx, XLogRecGetOrigin(r)))
        return;

    tupledata = XLogRecGetBlockData(r, 0, &tuplelen);

    data = tupledata;
    for (i = 0; i < xlrec->ntuples; i++)
    {
        ReorderBufferChange *change;
        xl_multi_insert_tuple *xlhdr;
        int            datalen;
        ReorderBufferTupleBuf *tuple;

        change = ReorderBufferGetChange(ctx->reorder);
        change->action = REORDER_BUFFER_CHANGE_INSERT;
        change->origin_id = XLogRecGetOrigin(r);

        memcpy(&change->data.tp.relnode, &rnode, sizeof(RelFileNode));

        /*
         * CONTAINS_NEW_TUPLE will always be set currently as multi_insert
         * isn't used for catalogs, but better be future proof.
         *
         * We decode the tuple in pretty much the same way as DecodeXLogTuple,
         * but since the layout is slightly different, we can't use it here.
         */
        if (xlrec->flags & XLH_INSERT_CONTAINS_NEW_TUPLE)
        {
            HeapTupleHeader header;

            xlhdr = (xl_multi_insert_tuple *) SHORTALIGN(data);
            data = ((char *) xlhdr) + SizeOfMultiInsertTuple;
            datalen = xlhdr->datalen;

#ifdef __STORAGE_SCALABLE__
            Assert(MyReplicationSlot);
        
            /* filter the tuple if not interested in */
            if (OidIsValid(MyReplicationSlot->relid))
            {
                Oid relationId;
                
                StartTransactionCommand();
                
                relationId = RelidByRelfilenode(rnode.spcNode, rnode.relNode);

                AbortCurrentTransaction();
        
                /* not our target relation */
                if (relationId != MyReplicationSlot->relid)
                {
                    data += datalen;
                    ReorderBufferReturnChange(ctx->reorder, change);
                    continue;
                }
        
                if (MyReplicationSlot->shards)
                {
                    /* not our target shard */
                    if (!bms_is_member(xlhdr->t_shardid, MyReplicationSlot->shards))
                    {
                        data += datalen;
                        ReorderBufferReturnChange(ctx->reorder, change);
                        continue;
                    }
                }
        
                //MyReplicationSlot->ntups_insert++;
            }
            else if (MyReplicationSlot->npubs)
            {
                bool found;
                Oid relationId;

                StartTransactionCommand();
                
                relationId = RelidByRelfilenode(rnode.spcNode, rnode.relNode);

                AbortCurrentTransaction();

                found = RelationShardIsTarget(relationId, xlhdr->t_shardid);

                if (!found)
                {
                    data += datalen;
                    ReorderBufferReturnChange(ctx->reorder, change);
                    continue;
                }
            }
#endif
            
            change->data.tp.newtuple =
                ReorderBufferGetTupleBuf(ctx->reorder, datalen);

            tuple = change->data.tp.newtuple;
            header = tuple->tuple.t_data;

            /* not a disk based tuple */
            ItemPointerSetInvalid(&tuple->tuple.t_self);

            /*
             * We can only figure this out after reassembling the
             * transactions.
             */
            tuple->tuple.t_tableOid = InvalidOid;

            tuple->tuple.t_len = datalen + SizeofHeapTupleHeader;

            memset(header, 0, SizeofHeapTupleHeader);

            memcpy((char *) tuple->tuple.t_data + SizeofHeapTupleHeader,
                   (char *) data,
                   datalen);
            data += datalen;

            header->t_infomask = xlhdr->t_infomask;
            header->t_infomask2 = xlhdr->t_infomask2;
            header->t_hoff = xlhdr->t_hoff;
#ifdef __STORAGE_SCALABLE__
			header->t_shardid = xlhdr->t_shardid;
#endif
        }

        /*
         * Reset toast reassembly state only after the last row in the last
         * xl_multi_insert_tuple record emitted by one heap_multi_insert()
         * call.
         */
        if (xlrec->flags & XLH_INSERT_LAST_IN_MULTI &&
            (i + 1) == xlrec->ntuples)
            change->data.tp.clear_toast_afterwards = true;
        else
            change->data.tp.clear_toast_afterwards = false;

        ReorderBufferQueueChange(ctx->reorder, XLogRecGetXid(r),
                                 buf->origptr, change);
    }
    Assert(data == tupledata + tuplelen);
}

/*
 * Parse XLOG_HEAP_CONFIRM from wal into a confirmation change.
 *
 * This is pretty trivial, all the state essentially already setup by the
 * speculative insertion.
 */
static void
DecodeSpecConfirm(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
    XLogReaderState *r = buf->record;
    ReorderBufferChange *change;
    RelFileNode target_node;

    /* only interested in our database */
    XLogRecGetBlockTag(r, 0, &target_node, NULL, NULL);
    if (target_node.dbNode != ctx->slot->data.database)
        return;

    /* output plugin doesn't look for this origin, no need to queue */
    if (FilterByOrigin(ctx, XLogRecGetOrigin(r)))
        return;

#ifdef __STORAGE_SCALABLE__
    if (SkipSpecConfirm(XLogRecGetXid(r)))
    {
        return;
    }
#endif

    change = ReorderBufferGetChange(ctx->reorder);
    change->action = REORDER_BUFFER_CHANGE_INTERNAL_SPEC_CONFIRM;
    change->origin_id = XLogRecGetOrigin(r);

    memcpy(&change->data.tp.relnode, &target_node, sizeof(RelFileNode));

    change->data.tp.clear_toast_afterwards = true;

    ReorderBufferQueueChange(ctx->reorder, XLogRecGetXid(r), buf->origptr, change);
}


/*
 * Read a HeapTuple as WAL logged by heap_insert, heap_update and heap_delete
 * (but not by heap_multi_insert) into a tuplebuf.
 *
 * The size 'len' and the pointer 'data' in the record need to be
 * computed outside as they are record specific.
 */
static void
DecodeXLogTuple(char *data, Size len, ReorderBufferTupleBuf *tuple)
{
    xl_heap_header xlhdr;
    int            datalen = len - SizeOfHeapHeader;
    HeapTupleHeader header;

    Assert(datalen >= 0);

    tuple->tuple.t_len = datalen + SizeofHeapTupleHeader;
    header = tuple->tuple.t_data;

    /* not a disk based tuple */
    ItemPointerSetInvalid(&tuple->tuple.t_self);

    /* we can only figure this out after reassembling the transactions */
    tuple->tuple.t_tableOid = InvalidOid;

    /* data is not stored aligned, copy to aligned storage */
    memcpy((char *) &xlhdr,
           data,
           SizeOfHeapHeader);

    memset(header, 0, SizeofHeapTupleHeader);

    memcpy(((char *) tuple->tuple.t_data) + SizeofHeapTupleHeader,
           data + SizeOfHeapHeader,
           datalen);

    header->t_infomask = xlhdr.t_infomask;
    header->t_infomask2 = xlhdr.t_infomask2;
    header->t_hoff = xlhdr.t_hoff;
}
#ifdef __STORAGE_SCALABLE__
static bool
RelationShardIsTarget(Oid relid, int32 shardid)
{
    bool found = false;
    int i;
    
    for (i = 0; i < MyReplicationSlot->npubs; i++)
    {
        if (MyReplicationSlot->alltables[i] || list_member_oid(MyReplicationSlot->tables[i], relid))
        {
            if (!MyReplicationSlot->pubshards[i] || bms_is_member(shardid, MyReplicationSlot->pubshards[i]))
            {
                found = true;
                break;
            }
        }
    }

    return found;
}
static void
SetSkipSpecConfirm(TransactionId xid)
{
    void *entry = NULL;
    
    if (confirmHash == NULL)
    {
        HASHCTL        hash_ctl;

        memset(&hash_ctl, 0, sizeof(hash_ctl));
        hash_ctl.keysize = sizeof(TransactionId);
        hash_ctl.entrysize = sizeof(TransactionId);

        confirmHash = hash_create("Confirm Xids",
                                Confirm_HASH_SIZE,
                                &hash_ctl,
                                HASH_ELEM | HASH_BLOBS);
    }

    entry = (void *)hash_search(confirmHash, (void *) &xid, HASH_ENTER_NULL, NULL);

    if (entry == NULL)
    {
        elog(ERROR, "out of memory, could not create entry in ConfirmHash");
    }
}
static bool
SkipSpecConfirm(TransactionId xid)
{
    bool result = false;
    void *entry = NULL;
    
    if (confirmHash)
    {
        entry = (void *)hash_search(confirmHash, (void *) &xid, HASH_REMOVE, NULL);

        if (entry)
        {
            result = true;
        }
    }

    return result;
}
#endif
