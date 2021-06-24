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
 * storage.c
 *      code to create and destroy physical storage for relations
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *      src/backend/catalog/storage.c
 *
 * NOTES
 *      Some of this code used to be in storage/smgr/smgr.c, and the
 *      function names still reflect that.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/visibilitymap.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "catalog/catalog.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#ifdef _SHARDING_
#include "commands/vacuum.h"
#include "storage/extentmapping.h"
#include "postmaster/bgwriter.h"
#endif
#include "storage/freespace.h"
#include "storage/smgr.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcryptmap.h"

/*
 * We keep a list of all relations (represented as RelFileNode values)
 * that have been created or deleted in the current transaction.  When
 * a relation is created, we create the physical file immediately, but
 * remember it so that we can delete the file again if the current
 * transaction is aborted.  Conversely, a deletion request is NOT
 * executed immediately, but is just entered in the list.  When and if
 * the transaction commits, we can delete the physical file.
 *
 * To handle subtransactions, every entry is marked with its transaction
 * nesting level.  At subtransaction commit, we reassign the subtransaction's
 * entries to the parent nesting level.  At subtransaction abort, we can
 * immediately execute the abort-time actions for all entries of the current
 * nesting level.
 *
 * NOTE: the list is kept in TopMemoryContext to be sure it won't disappear
 * unbetimes.  It'd probably be OK to keep it in TopTransactionContext,
 * but I'm being paranoid.
 */

typedef struct PendingRelDelete
{
    RelFileNode relnode;        /* relation that may need to be deleted */
    BackendId    backend;        /* InvalidBackendId if not a temp rel */
    bool        atCommit;        /* T=delete at commit; F=delete at abort */
    int            nestLevel;        /* xact nesting level of request */
    struct PendingRelDelete *next;    /* linked-list link */
} PendingRelDelete;

static PendingRelDelete *pendingDeletes = NULL; /* head of linked list */

/*
 * RelationCreateStorage
 *        Create physical storage for a relation.
 *
 * Create the underlying disk file storage for the relation. This only
 * creates the main fork; additional forks are created lazily by the
 * modules that need them.
 *
 * This function is transactional. The creation is WAL-logged, and if the
 * transaction aborts later on, the storage will be destroyed.
 */
void
RelationCreateStorage(RelFileNode rnode, char relpersistence)
{
    PendingRelDelete *pending;
    SMgrRelation srel;
    BackendId    backend;
    bool        needs_wal;

    switch (relpersistence)
    {
        case RELPERSISTENCE_TEMP:
#ifdef XCP
            if (OidIsValid(MyCoordId))
                backend = MyFirstBackendId;
            else
#endif
            backend = BackendIdForTempRelations();
            needs_wal = false;
            break;
        case RELPERSISTENCE_UNLOGGED:
            backend = InvalidBackendId;
            needs_wal = false;
            break;
        case RELPERSISTENCE_PERMANENT:
            backend = InvalidBackendId;
            needs_wal = true;
            break;
        default:
            elog(ERROR, "invalid relpersistence: %c", relpersistence);
            return;                /* placate compiler */
    }

    srel = smgropen(rnode, backend);
    smgrcreate(srel, MAIN_FORKNUM, false);

    if (needs_wal)
        log_smgrcreate(&srel->smgr_rnode.node, MAIN_FORKNUM);

    /* Add the relation to the list of stuff to delete at abort */
    pending = (PendingRelDelete *)
        MemoryContextAlloc(TopMemoryContext, sizeof(PendingRelDelete));
    pending->relnode = rnode;
    pending->backend = backend;
    pending->atCommit = false;    /* delete if abort */
    pending->nestLevel = GetCurrentTransactionNestLevel();
    pending->next = pendingDeletes;
    pendingDeletes = pending;
}

/*
 * Perform XLogInsert of an XLOG_SMGR_CREATE record to WAL.
 */
void
log_smgrcreate(RelFileNode *rnode, ForkNumber forkNum)
{
    xl_smgr_create xlrec;

    /*
     * Make an XLOG entry reporting the file creation.
     */
    xlrec.rnode = *rnode;
    xlrec.forkNum = forkNum;

    XLogBeginInsert();
    XLogRegisterData((char *) &xlrec, sizeof(xlrec));
    XLogInsert(RM_SMGR_ID, XLOG_SMGR_CREATE | XLR_SPECIAL_REL_UPDATE);
}

#ifdef _SHARDING_
void log_smgrdealloc(RelFileNode *rnode, ExtentID eid, int flags)
{
    xl_smgr_dealloc xlrec;
    XLogRecPtr    lsn;

    xlrec.rnode = *rnode;
    xlrec.eid = eid;
    xlrec.flags = flags;

    XLogBeginInsert();
    XLogRegisterData((char *) &xlrec, sizeof(xlrec));
    lsn = XLogInsert(RM_SMGR_ID, XLOG_SMGR_DEALLOC | XLR_SPECIAL_REL_UPDATE);

    XLogFlush(lsn);
}
#endif

/*
 * RelationDropStorage
 *        Schedule unlinking of physical storage at transaction commit.
 */
void
RelationDropStorage(Relation rel)
{
    PendingRelDelete *pending;

    /* Add the relation to the list of stuff to delete at commit */
    pending = (PendingRelDelete *)
        MemoryContextAlloc(TopMemoryContext, sizeof(PendingRelDelete));
    pending->relnode = rel->rd_node;
    pending->backend = rel->rd_backend;
    pending->atCommit = true;    /* delete if commit */
    pending->nestLevel = GetCurrentTransactionNestLevel();
    pending->next = pendingDeletes;
    pendingDeletes = pending;

    /*
     * NOTE: if the relation was created in this transaction, it will now be
     * present in the pending-delete list twice, once with atCommit true and
     * once with atCommit false.  Hence, it will be physically deleted at end
     * of xact in either case (and the other entry will be ignored by
     * smgrDoPendingDeletes, so no error will occur).  We could instead remove
     * the existing list entry and delete the physical file immediately, but
     * for now I'll keep the logic simple.
     */

    RelationCloseSmgr(rel);
}

/*
 * RelationPreserveStorage
 *        Mark a relation as not to be deleted after all.
 *
 * We need this function because relation mapping changes are committed
 * separately from commit of the whole transaction, so it's still possible
 * for the transaction to abort after the mapping update is done.
 * When a new physical relation is installed in the map, it would be
 * scheduled for delete-on-abort, so we'd delete it, and be in trouble.
 * The relation mapper fixes this by telling us to not delete such relations
 * after all as part of its commit.
 *
 * We also use this to reuse an old build of an index during ALTER TABLE, this
 * time removing the delete-at-commit entry.
 *
 * No-op if the relation is not among those scheduled for deletion.
 */
void
RelationPreserveStorage(RelFileNode rnode, bool atCommit)
{
    PendingRelDelete *pending;
    PendingRelDelete *prev;
    PendingRelDelete *next;

    prev = NULL;
    for (pending = pendingDeletes; pending != NULL; pending = next)
    {
        next = pending->next;
        if (RelFileNodeEquals(rnode, pending->relnode)
            && pending->atCommit == atCommit)
        {
            /* unlink and delete list entry */
            if (prev)
                prev->next = next;
            else
                pendingDeletes = next;
            pfree(pending);
            /* prev does not change */
        }
        else
        {
            /* unrelated entry, don't touch it */
            prev = pending;
        }
    }
}

/*
 * RelationTruncate
 *        Physically truncate a relation to the specified number of blocks.
 *
 * This includes getting rid of any buffers for the blocks that are to be
 * dropped.
 */
void
RelationTruncate(Relation rel, BlockNumber nblocks)
{// #lizard forgives
    bool        fsm;
    bool        vm;
#ifdef _SHARDING_
    int         i;
#endif

    /* Open it at the smgr level if not already done */
    RelationOpenSmgr(rel);

    /*
     * Make sure smgr_targblock etc aren't pointing somewhere past new end
     */
    rel->rd_smgr->smgr_targblock = InvalidBlockNumber;
#ifdef _SHARDING_
    for(i=0; i<SMGR_TARGBLOCK_MAX_SHARDS; i++)
    {
        //InitShardTargBlock(&rel->rd_smgr->smgr_shard_targblocks[i]);
        rel->rd_smgr->smgr_shard_targblocks[i] = InvalidBlockNumber;
    }
    //MemSet(rel->rd_smgr->smgr_shard_targblocks, 0, sizeof(rel->rd_smgr->smgr_shard_targblocks));

    if(RelationHasExtent(rel))
        TruncateExtentMap(rel, nblocks);
#endif

    rel->rd_smgr->smgr_fsm_nblocks = InvalidBlockNumber;
    rel->rd_smgr->smgr_vm_nblocks = InvalidBlockNumber;

    /* Truncate the FSM first if it exists */
    fsm = smgrexists(rel->rd_smgr, FSM_FORKNUM);
    if (fsm)
        FreeSpaceMapTruncateRel(rel, nblocks);

    /* Truncate the visibility map too if it exists. */
    vm = smgrexists(rel->rd_smgr, VISIBILITYMAP_FORKNUM);
    if (vm)
        visibilitymap_truncate(rel, nblocks);

    /*
     * We WAL-log the truncation before actually truncating, which means
     * trouble if the truncation fails. If we then crash, the WAL replay
     * likely isn't going to succeed in the truncation either, and cause a
     * PANIC. It's tempting to put a critical section here, but that cure
     * would be worse than the disease. It would turn a usually harmless
     * failure to truncate, that might spell trouble at WAL replay, into a
     * certain PANIC.
     */
    if (RelationNeedsWAL(rel))
    {
        /*
         * Make an XLOG entry reporting the file truncation.
         */
        XLogRecPtr    lsn;
        xl_smgr_truncate xlrec;

        xlrec.blkno = nblocks;
        xlrec.rnode = rel->rd_node;
        xlrec.flags = SMGR_TRUNCATE_ALL;

        XLogBeginInsert();
        XLogRegisterData((char *) &xlrec, sizeof(xlrec));

        lsn = XLogInsert(RM_SMGR_ID,
                         XLOG_SMGR_TRUNCATE | XLR_SPECIAL_REL_UPDATE);

        /*
         * Flush, because otherwise the truncation of the main relation might
         * hit the disk before the WAL record, and the truncation of the FSM
         * or visibility map. If we crashed during that window, we'd be left
         * with a truncated heap, but the FSM or visibility map would still
         * contain entries for the non-existent heap pages.
         */
        if (fsm || vm)
            XLogFlush(lsn);
    }

    /* Do the real work */
    smgrtruncate(rel->rd_smgr, MAIN_FORKNUM, nblocks);
}

/*
 *    smgrDoPendingDeletes() -- Take care of relation deletes at end of xact.
 *
 * This also runs when aborting a subxact; we want to clean up a failed
 * subxact immediately.
 *
 * Note: It's possible that we're being asked to remove a relation that has
 * no physical storage in any fork. In particular, it's possible that we're
 * cleaning up an old temporary relation for which RemovePgTempFiles has
 * already recovered the physical storage.
 */
void
smgrDoPendingDeletes(bool isCommit)
{// #lizard forgives
    int            nestLevel = GetCurrentTransactionNestLevel();
    PendingRelDelete *pending;
    PendingRelDelete *prev;
    PendingRelDelete *next;
    int            nrels = 0,
                i = 0,
                maxrels = 0;
    SMgrRelation *srels = NULL;

    prev = NULL;
    for (pending = pendingDeletes; pending != NULL; pending = next)
    {
        next = pending->next;
        if (pending->nestLevel < nestLevel)
        {
            /* outer-level entries should not be processed yet */
            prev = pending;
        }
        else
        {
            /* unlink list entry first, so we don't retry on failure */
            if (prev)
                prev->next = next;
            else
                pendingDeletes = next;
            /* do deletion if called for */
            if (pending->atCommit == isCommit)
            {
                SMgrRelation srel;

                srel = smgropen(pending->relnode, pending->backend);

                /* allocate the initial array, or extend it, if needed */
                if (maxrels == 0)
                {
                    maxrels = 8;
                    srels = palloc(sizeof(SMgrRelation) * maxrels);
                }
                else if (maxrels <= nrels)
                {
                    maxrels *= 2;
                    srels = repalloc(srels, sizeof(SMgrRelation) * maxrels);
                }

                srels[nrels++] = srel;
            }
            /* must explicitly free the list entry */
            pfree(pending);
            /* prev does not change */
        }
    }

    if (nrels > 0)
    {
        smgrdounlinkall(srels, nrels, false);

        for (i = 0; i < nrels; i++)
		{
#ifdef _MLS_
			/*
			 * clean up the rnode infomation in rel crypt hash table
			 */
			remove_rel_crypt_hash_elem(&(srels[i]->smgr_relcrypt), true);
#endif
            smgrclose(srels[i]);
		}

        pfree(srels);
    }
}

/*
 * smgrGetPendingDeletes() -- Get a list of non-temp relations to be deleted.
 *
 * The return value is the number of relations scheduled for termination.
 * *ptr is set to point to a freshly-palloc'd array of RelFileNodes.
 * If there are no relations to be deleted, *ptr is set to NULL.
 *
 * Only non-temporary relations are included in the returned list.  This is OK
 * because the list is used only in contexts where temporary relations don't
 * matter: we're either writing to the two-phase state file (and transactions
 * that have touched temp tables can't be prepared) or we're writing to xlog
 * (and all temporary files will be zapped if we restart anyway, so no need
 * for redo to do it also).
 *
 * Note that the list does not include anything scheduled for termination
 * by upper-level transactions.
 */
int
smgrGetPendingDeletes(bool forCommit, RelFileNode **ptr)
{// #lizard forgives
    int            nestLevel = GetCurrentTransactionNestLevel();
    int            nrels;
    RelFileNode *rptr;
    PendingRelDelete *pending;

    nrels = 0;
    for (pending = pendingDeletes; pending != NULL; pending = pending->next)
    {
        if (pending->nestLevel >= nestLevel && pending->atCommit == forCommit
            && pending->backend == InvalidBackendId)
            nrels++;
    }
    if (nrels == 0)
    {
        *ptr = NULL;
        return 0;
    }
    rptr = (RelFileNode *) palloc(nrels * sizeof(RelFileNode));
    *ptr = rptr;
    for (pending = pendingDeletes; pending != NULL; pending = pending->next)
    {
        if (pending->nestLevel >= nestLevel && pending->atCommit == forCommit
            && pending->backend == InvalidBackendId)
        {
            *rptr = pending->relnode;
            rptr++;
        }
    }
    return nrels;
}

/*
 *    PostPrepare_smgr -- Clean up after a successful PREPARE
 *
 * What we have to do here is throw away the in-memory state about pending
 * relation deletes.  It's all been recorded in the 2PC state file and
 * it's no longer smgr's job to worry about it.
 */
void
PostPrepare_smgr(void)
{
    PendingRelDelete *pending;
    PendingRelDelete *next;

    for (pending = pendingDeletes; pending != NULL; pending = next)
    {
        next = pending->next;
        pendingDeletes = next;
        /* must explicitly free the list entry */
        pfree(pending);
    }
}


/*
 * AtSubCommit_smgr() --- Take care of subtransaction commit.
 *
 * Reassign all items in the pending-deletes list to the parent transaction.
 */
void
AtSubCommit_smgr(void)
{
    int            nestLevel = GetCurrentTransactionNestLevel();
    PendingRelDelete *pending;

    for (pending = pendingDeletes; pending != NULL; pending = pending->next)
    {
        if (pending->nestLevel >= nestLevel)
            pending->nestLevel = nestLevel - 1;
    }
}

/*
 * AtSubAbort_smgr() --- Take care of subtransaction abort.
 *
 * Delete created relations and forget about deleted relations.
 * We can execute these operations immediately because we know this
 * subtransaction will not commit.
 */
void
AtSubAbort_smgr(void)
{
    smgrDoPendingDeletes(false);
}

void
smgr_redo(XLogReaderState *record)
{// #lizard forgives
    XLogRecPtr    lsn = record->EndRecPtr;
    uint8        info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    /* Backup blocks are not used in smgr records */
    Assert(!XLogRecHasAnyBlockRefs(record));

    if (info == XLOG_SMGR_CREATE)
    {
        xl_smgr_create *xlrec = (xl_smgr_create *) XLogRecGetData(record);
        SMgrRelation reln;

        reln = smgropen(xlrec->rnode, InvalidBackendId);
        smgrcreate(reln, xlrec->forkNum, true);
    }
    else if (info == XLOG_SMGR_TRUNCATE)
    {
        xl_smgr_truncate *xlrec = (xl_smgr_truncate *) XLogRecGetData(record);
        SMgrRelation reln;
        Relation    rel;

        reln = smgropen(xlrec->rnode, InvalidBackendId);

        /*
         * Forcibly create relation if it doesn't exist (which suggests that
         * it was dropped somewhere later in the WAL sequence).  As in
         * XLogReadBufferForRedo, we prefer to recreate the rel and replay the
         * log as best we can until the drop is seen.
         */
        smgrcreate(reln, MAIN_FORKNUM, true);

        /*
         * Before we perform the truncation, update minimum recovery point to
         * cover this WAL record. Once the relation is truncated, there's no
         * going back. The buffer manager enforces the WAL-first rule for
         * normal updates to relation files, so that the minimum recovery
         * point is always updated before the corresponding change in the data
         * file is flushed to disk. We have to do the same manually here.
         *
         * Doing this before the truncation means that if the truncation fails
         * for some reason, you cannot start up the system even after restart,
         * until you fix the underlying situation so that the truncation will
         * succeed. Alternatively, we could update the minimum recovery point
         * after truncation, but that would leave a small window where the
         * WAL-first rule could be violated.
         */
        XLogFlush(lsn);

        if ((xlrec->flags & SMGR_TRUNCATE_HEAP) != 0)
        {
            smgrtruncate(reln, MAIN_FORKNUM, xlrec->blkno);

            /* Also tell xlogutils.c about it */
            XLogTruncateRelation(xlrec->rnode, MAIN_FORKNUM, xlrec->blkno);
        }

        /* Truncate FSM and VM too */
        rel = CreateFakeRelcacheEntry(xlrec->rnode);

        if ((xlrec->flags & SMGR_TRUNCATE_FSM) != 0 &&
            smgrexists(reln, FSM_FORKNUM))
            FreeSpaceMapTruncateRel(rel, xlrec->blkno);
        if ((xlrec->flags & SMGR_TRUNCATE_VM) != 0 &&
            smgrexists(reln, VISIBILITYMAP_FORKNUM))
            visibilitymap_truncate(rel, xlrec->blkno);

        FreeFakeRelcacheEntry(rel);
    }
#ifdef _SHARDING_
    else if (info == XLOG_SMGR_DEALLOC)
    {
        xl_smgr_dealloc *xlrec = (xl_smgr_dealloc *) XLogRecGetData(record);
        SMgrRelation reln;
        BlockNumber nblock;

        reln = smgropen(xlrec->rnode, InvalidBackendId);
        
        smgrcreate(reln, MAIN_FORKNUM, true);

        nblock = smgrnblocks(reln,MAIN_FORKNUM);
        if(nblock < xlrec->eid * PAGES_PER_EXTENTS)
        {
            elog(WARNING, "blocks to dealloc belong to eid %d(%d/%d/%d) is not exist, "
                            "because relation has only %d blocks",
                        xlrec->eid, xlrec->rnode.dbNode, xlrec->rnode.spcNode, xlrec->rnode.relNode,
                        nblock);
            return;
        }

        //RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT);

        if(xlrec->flags & SMGR_DEALLOC_REINIT)
        {
            xlog_reinit_extent_pages(xlrec->rnode, xlrec->eid);
        }
        else if(xlrec->flags & SMGR_DEALLOC_FREESTORAGE)
        {
#ifndef DISABLE_FALLOCATE
            smgrdealloc(reln, MAIN_FORKNUM, xlrec->eid * PAGES_PER_EXTENTS);
#else
            elog(WARNING, "database master dealloc storage space when vacuum sharding, "
                            "but this standby does not support falloc syscall because of version of os or gcc.");
            xlog_reinit_extent_pages(xlrec->rnode, xlrec->eid);
#endif
        }
        DropRelfileNodeExtentBuffers(xlrec->rnode, xlrec->eid);

        smgrclose(reln);
    }
#endif
    else
        elog(PANIC, "smgr_redo: unknown op code %u", info);
}
