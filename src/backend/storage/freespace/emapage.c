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
 * fsmpage.c
 *      routines to search and manipulate one FSM page.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *      src/backend/storage/freespace/fsmpage.c
 *
 * NOTES:
 *
 *    The public functions in this file form an API that hides the internal
 *    structure of a FSM page. This allows freespace.c to treat each FSM page
 *    as a black box with SlotsPerPage "slots". fsm_set_avail() and
 *    fsm_get_avail() let you get/set the value of a slot, and
 *    fsm_search_avail() lets you search for a slot with value >= X.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/xlogutils.h"
#include "common/relpath.h"
#include "catalog/pg_type.h"
#include "catalog/namespace.h"
#include "miscadmin.h"
#include "nodes/bitmapset.h"
#include "utils/rel.h"
#include "storage/bufmgr.h"
#include "storage/extentmapping.h"
#include "storage/freespace.h"
#include "storage/extent_xlog.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "utils/relcryptmap.h"

#define ExtentAssertEMEIsFree(eme) ExtentAssert((eme).is_occupied == 0)
#define ExtentAssertEMEIsOccup(eme) ExtentAssert((eme).is_occupied == 1)

/* common static functions*/

/* for EOB bits*/
#define EOB_NEXT_FREE(eob_pg, search_from) \
    bms_next_member(&((eob_pg)->eob_bits), search_from)

#define EOB_FIRST_FREE(eob_pg)  EOB_NEXT_FREE(eob_pg, -1)
    
#define EOB_FIRSTFREE_IS_FREE(pg)  bms_is_member((pg)->first_empty_extent, &((pg)->eob_bits))

bool    trace_extent = true;

/* for exent mapping logic*/
static ExtentID shard_apply_free_extent(Relation rel, ShardID sid);
static bool     shard_add_extent(Relation rel, ShardID sid, ExtentID eid);
static void        shard_append_extent(Relation rel, ShardID sid, ExtentID eid, uint8 freespace, bool for_rebuild);
static void     shard_append_extent_onlyscan(Relation rel, ShardID sid, ExtentID eid, uint8 freespace);
static void     shard_add_extent_to_alloclist(Relation rel, ExtentID eid, bool ignore_error);
static void     shard_remove_extent_from_alloclist(Relation rel, ExtentID eid, bool ignore_error, bool need_lock_shard);
static void     shard_remove_extent_from_scanlist(Relation rel, ExtentID eid, bool ignore_error, bool need_lock_shard);


/* for system functions */
static Oid         string_to_reloid(char *str);

/*-----------------------------------------------------------------------------
 *
 *         Fork Manager
 *
 *-----------------------------------------------------------------------------
 */
Buffer
extent_readbuffer(Relation rel, BlockNumber blkno, bool extend)
{// #lizard forgives
    Buffer        buf;
    EmaPageType pagetype;
    int32        max_eles = 0;

    ExtentAssert(BlockNumberIsValid(blkno));

    if(blkno < ESAPAGE_OFFSET - 1)
    {
        pagetype = EmaPageType_EOB;
        max_eles = EOBS_PER_PAGE;
    }
    else if(blkno == ESAPAGE_OFFSET - 1)
    {
        pagetype = EmaPageType_EOB;
        max_eles = MAX_EXTENTS % EOBS_PER_PAGE;
    }
    else if(blkno < EMAPAGE_OFFSET - 1)
    {
        pagetype = EmaPageType_ESA;
        max_eles = ESAS_PER_PAGE;
    }
    else if(blkno == EMAPAGE_OFFSET - 1)
    {
        pagetype = EmaPageType_ESA;
        max_eles = MAX_EXTENTS % ESAS_PER_PAGE;
    }
    else if(blkno < EMA_FORK_BLOCKS - 1)
    {
        pagetype = EmaPageType_EMA;
        max_eles = EMES_PER_PAGE;
    }
    else if(blkno == EMA_FORK_BLOCKS - 1)
    {
        pagetype = EmaPageType_EMA;
        max_eles = MAX_EXTENTS % EMES_PER_PAGE;
    }    
    else /*(blkno >= EMA_FORK_BLOCKS) */
    {
        ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("blocknumber too larger"),
                     errdetail("ema page number cannot be greater than %d",
                               EMA_FORK_BLOCKS - 1)));
    }
    
    RelationOpenSmgr(rel);

    /*
     * If we haven't cached the size of the FSM yet, check it first.  Also
     * recheck if the requested block seems to be past end, since our cached
     * value might be stale.  (We send smgr inval messages on truncation, but
     * not on extension.)
     */
    if (rel->rd_smgr->smgr_ema_nblocks == InvalidBlockNumber ||
        blkno >= rel->rd_smgr->smgr_ema_nblocks)
    {
        if (smgrexists(rel->rd_smgr, EXTENT_FORKNUM))
            rel->rd_smgr->smgr_ema_nblocks = smgrnblocks(rel->rd_smgr,
                                                         EXTENT_FORKNUM);
        else
            rel->rd_smgr->smgr_ema_nblocks = 0;
    }

    /* Handle requests beyond EOF */
    if (blkno >= rel->rd_smgr->smgr_ema_nblocks)
    {
        if (extend)
            extent_extend_block(rel, blkno + 1);
        else
            return InvalidBuffer;
    }

    /*
     * Use ZERO_ON_ERROR mode, and initialize the page if necessary. The FSM
     * information is not accurate anyway, so it's better to clear corrupt
     * pages than error out. Since the FSM changes are not WAL-logged, the
     * so-called torn page problem on crash can lead to pages with corrupt
     * headers, for example.
     */
    buf = ReadBufferExtended(rel, EXTENT_FORKNUM, blkno, RBM_ZERO_ON_ERROR, NULL);
    if (PageIsNew(BufferGetPage(buf)))
    {
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        PageInit_shard(BufferGetPage(buf), BLCKSZ, 0, InvalidShardID, true);
        switch(pagetype)
        {
            case EmaPageType_EOB:
                eob_init_page(BufferGetPage(buf), max_eles);
                break;
            case EmaPageType_ESA:
                esa_init_page(BufferGetPage(buf), max_eles);
                break;
            case EmaPageType_EMA:
                ema_init_page(BufferGetPage(buf), max_eles);
                break;
            default:
                elog(PANIC, "page type %d is not supported.", pagetype);
                break;
        }
		LockBuffer(buf, BUFFER_LOCK_UNLOCK);
    }
    return buf;
}

Buffer
extent_readbuffer_for_redo(RelFileNode rnode, BlockNumber blkno, bool extend)
{// #lizard forgives
    SMgrRelation smgr;
    Buffer        buf;
    EmaPageType pagetype;
    int32        max_eles = 0;


    if(blkno < ESAPAGE_OFFSET - 1)
    {
        max_eles = EOBS_PER_PAGE;
        pagetype = EmaPageType_EOB;
    }
    else if(blkno == ESAPAGE_OFFSET - 1)
    {
        max_eles = MAX_EXTENTS % EOBS_PER_PAGE;
        pagetype = EmaPageType_EOB;
    }
    else if(blkno < EMAPAGE_OFFSET - 1)
    {
        max_eles = ESAS_PER_PAGE;
        pagetype = EmaPageType_ESA;
    }
    else if(blkno == EMAPAGE_OFFSET - 1)
    {
        max_eles = MAX_EXTENTS % ESAS_PER_PAGE;
        pagetype = EmaPageType_ESA;
    }
    else if(blkno < EMA_FORK_BLOCKS - 1)
    {
        max_eles = EMES_PER_PAGE;
        pagetype = EmaPageType_EMA;
    }
    else if(blkno == EMA_FORK_BLOCKS - 1)
    {
        max_eles = MAX_EXTENTS % EMES_PER_PAGE;
        pagetype = EmaPageType_EMA;
    }    
    else /*(blkno >= EMA_FORK_BLOCKS) */
    {
        ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("blocknumber too larger "),
                     errdetail("ema page number cannot be greater than %d",
                               EMA_FORK_BLOCKS - 1)));
    }
    
    smgr = smgropen(rnode, InvalidBackendId);

    /*
     * If we haven't cached the size of the FSM yet, check it first.  Also
     * recheck if the requested block seems to be past end, since our cached
     * value might be stale.  (We send smgr inval messages on truncation, but
     * not on extension.)
     */
    if (smgr->smgr_ema_nblocks == InvalidBlockNumber ||
        blkno >= smgr->smgr_ema_nblocks)
    {
        if (smgrexists(smgr, EXTENT_FORKNUM))
            smgr->smgr_ema_nblocks = smgrnblocks(smgr, EXTENT_FORKNUM);
        else
            smgr->smgr_ema_nblocks = 0;
    }

    /* Handle requests beyond EOF */
    if (blkno >= smgr->smgr_ema_nblocks)
    {
        if (extend)
            extent_extend_block_for_redo(rnode, blkno + 1);
        else
            return InvalidBuffer;
    }

    /*
     * Use ZERO_ON_ERROR mode, and initialize the page if necessary. The FSM
     * information is not accurate anyway, so it's better to clear corrupt
     * pages than error out. Since the FSM changes are not WAL-logged, the
     * so-called torn page problem on crash can lead to pages with corrupt
     * headers, for example.
     */
    buf = XLogReadBufferExtended(rnode, EXTENT_FORKNUM, blkno, RBM_ZERO_ON_ERROR);
    if (PageIsNew(BufferGetPage(buf)))
    {
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        PageInit_shard(BufferGetPage(buf), BLCKSZ, 0, InvalidShardID, true);
        switch(pagetype)
        {
            case EmaPageType_EOB:
                eob_init_page(BufferGetPage(buf), max_eles);
                break;
            case EmaPageType_ESA:
                esa_init_page(BufferGetPage(buf), max_eles);
                break;
            case EmaPageType_EMA:
                ema_init_page(BufferGetPage(buf), max_eles);
                break;
            default:
                elog(PANIC, "page type %d is not supported.", pagetype);
                break;
        }
		LockBuffer(buf, BUFFER_LOCK_UNLOCK);
    }
    return buf;
}


void
extent_extend_block(Relation rel, BlockNumber ema_nblocks)
{
    BlockNumber ema_nblocks_now;
    Page        pg;

    pg = (Page) palloc(BLCKSZ);
    //PageInit(pg, BLCKSZ, 0);
    MemSet(pg, 0, BLCKSZ);
    
    /*
     * We use the relation extension lock to lock out other backends trying to
     * extend the FSM at the same time. It also locks out extension of the
     * main fork, unnecessarily, but extending the FSM happens seldom enough
     * that it doesn't seem worthwhile to have a separate lock tag type for
     * it.
     *
     * Note that another backend might have extended or created the relation
     * by the time we get the lock.
     */
    LockRelationForExtension(rel, ExclusiveLock);

    /* Might have to re-open if a cache flush happened */
    RelationOpenSmgr(rel);

    /*
     * Create the FSM file first if it doesn't exist.  If smgr_fsm_nblocks is
     * positive then it must exist, no need for an smgrexists call.
     */
    if ((rel->rd_smgr->smgr_ema_nblocks == 0 ||
         rel->rd_smgr->smgr_ema_nblocks == InvalidBlockNumber) &&
        !smgrexists(rel->rd_smgr, EXTENT_FORKNUM))
        smgrcreate(rel->rd_smgr, EXTENT_FORKNUM, false);

    ema_nblocks_now = smgrnblocks(rel->rd_smgr, EXTENT_FORKNUM);

    while (ema_nblocks_now < ema_nblocks)
    {
        PageSetChecksumInplace(pg, ema_nblocks_now);

        smgrextend(rel->rd_smgr, EXTENT_FORKNUM, ema_nblocks_now,
                   (char *) pg, false);
        ema_nblocks_now++;
    }

    /* Update local cache with the up-to-date size */
    rel->rd_smgr->smgr_ema_nblocks = ema_nblocks_now;

    UnlockRelationForExtension(rel, ExclusiveLock);

    pfree(pg);
}

void
extent_extend_block_for_redo(RelFileNode rnode, BlockNumber ema_nblocks)
{
    SMgrRelation smgr;
    BlockNumber ema_nblocks_now;
    Page        pg;

    pg = (Page) palloc(BLCKSZ);
    //PageInit(pg, BLCKSZ, 0);
    MemSet(pg, 0, BLCKSZ);

    smgr = smgropen(rnode, InvalidBackendId);
    /*
     * Create the FSM file first if it doesn't exist.  If smgr_fsm_nblocks is
     * positive then it must exist, no need for an smgrexists call.
     */
    if ((smgr->smgr_ema_nblocks == 0 ||
         smgr->smgr_ema_nblocks == InvalidBlockNumber) &&
        !smgrexists(smgr, EXTENT_FORKNUM))
        smgrcreate(smgr, EXTENT_FORKNUM, false);

    ema_nblocks_now = smgrnblocks(smgr, EXTENT_FORKNUM);

    while (ema_nblocks_now < ema_nblocks)
    {
        PageSetChecksumInplace(pg, ema_nblocks_now);

        smgrextend(smgr, EXTENT_FORKNUM, ema_nblocks_now,
                   (char *) pg, false);
        ema_nblocks_now++;
    }

    /* Update local cache with the up-to-date size */
    smgr->smgr_ema_nblocks = ema_nblocks_now;

    pfree(pg);
}


/*---------------------------------------------------------------------------
 *
 * Shard List lock manager
 *
 *---------------------------------------------------------------------------
 */
#if 0
void 
LockShardList(Relation rel, ShardID sid, LWLockMode mode)
{
    LWLock * lock = &(LWLockPadded *)MainLWLockArray[SHARDLIST_LOCK_OFFSET + sid].lock;
    LWLockAcquire(lock, mode);
}

void 
UnlockShardList(Relation rel, ShardID sid)
{
    LWLock * lock = &(LWLockPadded *)MainLWLockArray[SHARDLIST_LOCK_OFFSET + sid].lock;
    LWLockRelease(lock);
}
#endif
/*-----------------------------------------------------------------------------
 *
 *         EOB Page
 *
 *-----------------------------------------------------------------------------
 */
void 
eob_init_page(Page page, int32 max_bits)
{
    EOBPage eob_pg = (EOBPage)PageGetContents(page);

    eob_pg->first_empty_extent = EOB_INVALID_FIRST_FREE;
    eob_pg->n_bits = 0;
    eob_pg->eob_bits.nwords = EOBS_PER_PAGE / 32;
    eob_pg->max_bits = max_bits;

    //TODO: write xlog
}

void
eob_clean_page(EOBPage eob_pg)
{
    eob_pg->first_empty_extent = EOB_INVALID_FIRST_FREE;
    eob_pg->n_bits = 0;
    (void)bms_clean_members(&eob_pg->eob_bits);
}

void 
eob_truncate_page(EOBPage eob_pg, int local_bms_offset)
{
    bms_trun_members(&eob_pg->eob_bits, local_bms_offset);
    eob_pg->n_bits = local_bms_offset;
    eob_pg->first_empty_extent = EOB_FIRST_FREE(eob_pg);
}

EOBAddress 
eob_eid_to_address(ExtentID eid)
{
    EOBAddress addr;
    
    if(!ExtentIdIsValid(eid))
    {        
        ereport(PANIC,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("ExtentID is invalid"),
                     errdetail("ExtentID must be less than %d.",
                               MAX_EXTENTS)));
        
    }

    addr.physical_page_number = EOBPAGE_OFFSET + eid / EOBS_PER_PAGE;
    addr.local_bms_offset = eid % EOBS_PER_PAGE;

    return addr;
}

ExtentID 
eob_address_to_eid(EOBAddress addr)
{
    ExtentID eid;
    if(!EOBAddressIsValid(&addr))
    {        
#if 0
        ereport(PANIC,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("EOBAddress is invalid"),
                     errdetail("EOBAddress is (%d,%d).",
                               addr.physical_page_number, addr.local_bms_offset)));
#endif
        return InvalidExtentID;
        
    }

    eid = (addr.physical_page_number - EOBPAGE_OFFSET)*EOBS_PER_PAGE + addr.local_bms_offset;
    return eid;
}

bool 
eob_extent_is_free(Relation rel, ExtentID eid)
{
    EOBAddress addr;
    Buffer buf;
    Page pg;
    EOBPage eob_pg;
    bool    result;

    addr = eob_eid_to_address(eid);
    
    buf = extent_readbuffer(rel, addr.physical_page_number, true);
    if(BufferIsInvalid(buf))
    {
        ereport(PANIC,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("get block from ema file failed."),
                     errdetail("reloid:%d, block number:%d",
                               RelationGetRelid(rel), addr.physical_page_number)));
    }
    
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    pg = BufferGetPage(buf);
    eob_pg = (EOBPage)PageGetContents(pg);

    result = bms_is_member(addr.local_bms_offset, &(eob_pg->eob_bits));
    
    UnlockReleaseBuffer(buf);

    return result;
}


void 
eob_mark_extent(Relation rel, ExtentID eid, bool setfree)
{
    EOBAddress addr;
    Buffer buf;
    Page pg;
    EOBPage eob_pg;

    addr = eob_eid_to_address(eid);
    
    buf = extent_readbuffer(rel, addr.physical_page_number, true);
    if(BufferIsInvalid(buf))
    {
        ereport(PANIC,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("get block from ema file failed."),
                     errdetail("reloid:%d, block number:%d",
                               RelationGetRelid(rel), addr.physical_page_number)));
    }
    
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    pg = BufferGetPage(buf);
    eob_pg = (EOBPage)PageGetContents(pg);

    if(eob_pg->n_bits == 0 || addr.local_bms_offset >= eob_pg->n_bits)
    {
        /* exceed the max eob bit */
        EOBAddress max_eob;
        max_eob.physical_page_number = addr.physical_page_number;
        max_eob.local_bms_offset = eob_pg->n_bits - 1;
        
        elog(ERROR, "set eob failed, setting(eob bit:%d) is exceed the bound of table(max extent: %d).",
                eid, eob_address_to_eid(max_eob));
    }
    
    if(setfree)
    {        
        bms_add_member(&(eob_pg->eob_bits), addr.local_bms_offset);
        if(eob_pg->first_empty_extent > addr.local_bms_offset)
        {
            eob_pg->first_empty_extent = addr.local_bms_offset;
        }
        
    }
    else
    {    
        bms_del_member(&(eob_pg->eob_bits), addr.local_bms_offset);
        if(eob_pg->first_empty_extent == addr.local_bms_offset)
        {
            /*
             * the first empty extent is be occupied, find next empty extent.
             */
            eob_pg->first_empty_extent = EOB_NEXT_FREE(eob_pg, addr.local_bms_offset);
        }
    }

    MarkBufferDirty(buf);
    UnlockReleaseBuffer(buf);
}

void 
eob_page_mark_extent(EOBPage eob_pg, int local_bms_offset, bool setfree)
{
    if(setfree)
    {
        bms_add_member(&(eob_pg->eob_bits), local_bms_offset);
        if(eob_pg->first_empty_extent < 0 || eob_pg->first_empty_extent > local_bms_offset)
        {
            eob_pg->first_empty_extent = local_bms_offset;
        }
    }
    else
    {
        bms_del_member(&(eob_pg->eob_bits), local_bms_offset);
        if(eob_pg->first_empty_extent == local_bms_offset)
        {
            eob_pg->first_empty_extent = EOB_NEXT_FREE(eob_pg, local_bms_offset);
        }
    }
}

ExtentID
eob_get_free_extent(Relation rel)
{// #lizard forgives
    int     blk;
    Buffer     buf;
    EOBPage    pg;
    int        next_free;
    EOBAddress addr;
    
    for(blk = EOBPAGE_OFFSET; blk < ESAPAGE_OFFSET; blk++)
    {
        bool    need_move_first_free = false;
        next_free = -1;        
        
        buf = extent_readbuffer(rel, blk, true);
        if(BufferIsInvalid(buf))
        {
            ereport(PANIC,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("get block from ema file failed."),
                         errdetail("reloid:%d, block number:%d",
                                   RelationGetRelid(rel), blk)));
        }
        
        LockBuffer(buf, BUFFER_LOCK_SHARE);

        pg = (EOBPage)PageGetContents(BufferGetPage(buf));

        if(pg->first_empty_extent >= 0)
        {
            if(!EOB_FIRSTFREE_IS_FREE(pg))
            {
                next_free = EOB_NEXT_FREE(pg, pg->first_empty_extent);
                need_move_first_free = true;
            }
            else
                next_free = pg->first_empty_extent;
        }
        else
        {
            next_free = EOB_FIRST_FREE(pg);
            if(next_free > 0)
                need_move_first_free = true;
        }
        
        UnlockReleaseBuffer(buf);    

        if(need_move_first_free)
        {            
            LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
            pg->first_empty_extent = next_free;
            MarkBufferDirty(buf);
            UnlockReleaseBuffer(buf);
        }

        if(next_free >= 0)
            break;
    }

    if(next_free < 0)
    {
        elog(ERROR, "table is too large, and there is no space to extend.");
    }

    addr.physical_page_number = blk;
    addr.local_bms_offset = next_free;
    return eob_address_to_eid(addr);
}

ExtentID
eob_get_free_extent_and_set_busy(Relation rel)
{// #lizard forgives
    int     blk;
    Buffer     buf;
    EOBPage    pg;
    int        next_free;
    EOBAddress addr;
    
    for(blk = EOBPAGE_OFFSET; blk < ESAPAGE_OFFSET; blk++)
    {
        next_free = -1;        
        
        buf = extent_readbuffer(rel, blk, true);
        if(BufferIsInvalid(buf))
        {
            ereport(PANIC,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("get block from ema file failed."),
                         errdetail("reloid:%d, block number:%d",
                                   RelationGetRelid(rel), blk)));
        }
        
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

        pg = (EOBPage)PageGetContents(BufferGetPage(buf));

        if(pg->n_bits <= 0)
        {
            /* this eob page has not be used.*/
            UnlockReleaseBuffer(buf);
            break;
        }

        if(pg->first_empty_extent >= 0)
        {
            if(!EOB_FIRSTFREE_IS_FREE(pg))
            {
                next_free = EOB_NEXT_FREE(pg, pg->first_empty_extent);
                pg->first_empty_extent = next_free;
                MarkBufferDirty(buf);
            }
            else
                next_free = pg->first_empty_extent;
        }
        else
        {
            next_free = EOB_FIRST_FREE(pg);
            pg->first_empty_extent = next_free;
            MarkBufferDirty(buf);
        }

        if(next_free >= 0)
        {
            eob_page_mark_extent(pg, next_free, false);
            MarkBufferDirty(buf);
        }
        
        UnlockReleaseBuffer(buf);    

        if(next_free >= 0)
            break;
    }

    if(next_free < 0)
    {
        //elog(ERROR, "table is too large, and there is no space to extend.");
        return InvalidExtentID;
    }

    addr.physical_page_number = blk;
    addr.local_bms_offset = next_free;
    return eob_address_to_eid(addr);
}

void eob_truncate(Relation rel, ExtentID new_max_eid, ExtentID old_max_eid)
{// #lizard forgives
    int blkno = 0;
    int old_blks = 0;
    int old_last_offset = 0;
    int new_blks = 0;
    int new_last_offset = 0;
    Buffer buf;

    xl_extent_cleaneob clean_xlrec;
    xl_extent_trunceob trunc_xlrec;
    XLogRecPtr lsn;
    
    if(new_max_eid >= old_max_eid)
        return;

    /* init xlog data struct */
    INIT_EXLOG_CLEANEOB(&clean_xlrec);
    memcpy(&(clean_xlrec.rnode), &(rel->rd_node), sizeof(RelFileNode));    

    INIT_EXLOG_TRUNCEOB(&trunc_xlrec);
    memcpy(&(trunc_xlrec.rnode), &(rel->rd_node), sizeof(RelFileNode));    

    /*
     * clean all of eob pages if new heap is empty.
     */
    if(new_max_eid == 0)
    {
        old_blks = (old_max_eid + EOBS_PER_PAGE - 1)/EOBS_PER_PAGE;
        for(blkno = old_blks - 1; blkno >=0; blkno--)
        {
            buf = extent_readbuffer(rel, blkno + EOBPAGE_OFFSET, true);
            ExtentAssert(BufferIsValid(buf));
            LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

            START_CRIT_SECTION();
            eob_clean_page(BufferGetEOBPage(buf));

            /* write xlog */            
            clean_xlrec.pageno = BufferGetBlockNumber(buf);
            XLogBeginInsert();
            XLogRegisterData((char *) &clean_xlrec, SizeOfCleanEOB);
            lsn = XLogInsert(RM_EXTENT_ID, XLOG_EXTENT_COMMON);

            PageSetLSN(BufferGetPage(buf),lsn);
            MarkBufferDirty(buf);
            
            END_CRIT_SECTION();
            UnlockReleaseBuffer(buf);
            if(trace_extent)
            {
                ereport(LOG,
                    (errmsg("[trace extent]Clean EOB Page:[rel:%d/%d/%d]"
                            "[ema block number:%d, eob block index:%d]",
                            rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
                            clean_xlrec.pageno, blkno)));
            }
        }
        return;
    }

    ExtentAssert(new_max_eid > 0);
    ExtentAssert(old_max_eid > new_max_eid);

    /*
     * computer which pages will be clean or truncate
     */
    old_blks = old_max_eid / EOBS_PER_PAGE;
    old_last_offset = old_max_eid % EOBS_PER_PAGE;
    if (old_last_offset == 0)
    {
        old_blks--;
        old_last_offset = EOBS_PER_PAGE - 1;
    }

    new_blks = new_max_eid / EOBS_PER_PAGE;
    new_last_offset = new_max_eid % EOBS_PER_PAGE;
    if(new_last_offset == 0)
    {
        new_blks--;
        new_last_offset = EOBS_PER_PAGE - 1;
    }

    /*
     * clean pages after the max page of new extent segement
     */
    blkno = old_blks;
    while(blkno > new_blks)
    {
        buf = extent_readbuffer(rel, blkno + EOBPAGE_OFFSET, true);    
        LockBuffer(buf,  BUFFER_LOCK_EXCLUSIVE);

        START_CRIT_SECTION();
        eob_clean_page( BufferGetEOBPage(buf) );

        /* write xlog */
        clean_xlrec.pageno = BufferGetBlockNumber(buf);
        XLogBeginInsert();
        XLogRegisterData((char *) &clean_xlrec,  SizeOfCleanEOB);
        lsn = XLogInsert(RM_EXTENT_ID,  XLOG_EXTENT_COMMON);

        PageSetLSN(BufferGetPage(buf), lsn);
        MarkBufferDirty(buf);
        
        END_CRIT_SECTION();
        UnlockReleaseBuffer(buf);

        if(trace_extent)
        {
            ereport(LOG,
                  (errmsg("[trace extent]Clean EOB Page:[rel:%d/%d/%d]"
                        "[ema block number:%d, eob block index:%d]",
                        rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
                        clean_xlrec.pageno,  blkno)));
        }
        blkno--;
    }

    /*
     * truncate one page
     */
    ExtentAssert(blkno == new_blks);
    buf = extent_readbuffer(rel, blkno + EOBPAGE_OFFSET, true);

    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

    START_CRIT_SECTION();
    
    eob_truncate_page(BufferGetEOBPage(buf), new_last_offset);

    /* write xlog */
    trunc_xlrec.pageno = BufferGetBlockNumber(buf);
    trunc_xlrec.offset = new_last_offset;
    XLogBeginInsert();
    XLogRegisterData((char *) &trunc_xlrec, SizeOfTruncEOB);
    lsn = XLogInsert(RM_EXTENT_ID, XLOG_EXTENT_COMMON);

    PageSetLSN(BufferGetPage(buf),lsn);
    MarkBufferDirty(buf);
    END_CRIT_SECTION();
    UnlockReleaseBuffer(buf);    

    if(trace_extent)
    {
        ereport(LOG,
            (errmsg("[trace extent]Truncate EOB Page:[rel:%d/%d/%d]"
                    "[ema block number:%d, eob block index:%d, new offset:%d]",
                    rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
                    clean_xlrec.pageno, blkno, trunc_xlrec.offset)));
    }
}

/*-----------------------------------------------------------------------------
 *
 *         ESA Page
 *
 *-----------------------------------------------------------------------------
 */
void 
esa_init_page(Page page, int32 max_anchors)
{
    int i=0;
    ESAPage esa_pg = (ESAPage)PageGetContents(page);

    esa_pg->max_anchors = max_anchors;
    for(i=0; i+4 <= max_anchors; i += 4)
    {
        esa_pg->anchors[i].alloc_head = InvalidExtentID;
        esa_pg->anchors[i].alloc_tail = InvalidExtentID;
        esa_pg->anchors[i].scan_head  = InvalidExtentID;
        esa_pg->anchors[i].scan_tail  = InvalidExtentID;

        esa_pg->anchors[i+1].alloc_head = InvalidExtentID;
        esa_pg->anchors[i+1].alloc_tail = InvalidExtentID;
        esa_pg->anchors[i+1].scan_head  = InvalidExtentID;
        esa_pg->anchors[i+1].scan_tail  = InvalidExtentID;

        esa_pg->anchors[i+2].alloc_head = InvalidExtentID;
        esa_pg->anchors[i+2].alloc_tail = InvalidExtentID;
        esa_pg->anchors[i+2].scan_head  = InvalidExtentID;
        esa_pg->anchors[i+2].scan_tail  = InvalidExtentID;

        esa_pg->anchors[i+3].alloc_head = InvalidExtentID;
        esa_pg->anchors[i+3].alloc_tail = InvalidExtentID;
        esa_pg->anchors[i+3].scan_head  = InvalidExtentID;
        esa_pg->anchors[i+3].scan_tail  = InvalidExtentID;
    }

    for(; i < max_anchors; i++)
    {
        esa_pg->anchors[i].alloc_head = InvalidExtentID;
        esa_pg->anchors[i].alloc_tail = InvalidExtentID;
        esa_pg->anchors[i].scan_head  = InvalidExtentID;
        esa_pg->anchors[i].scan_tail  = InvalidExtentID;
    }
}

ESAAddress 
esa_sid_to_address(ShardID sid)
{
    ESAAddress addr;
    int  esa_offset =     ESAPAGE_OFFSET;
    int     esas_per_page = ESAS_PER_PAGE;
    if(!ShardIDIsValid(sid))
    {        
        ereport(PANIC,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("ShardID is invalid"),
                     errdetail("ShardID must be less than %d.",
                               InvalidShardID)));
        
    }

    addr.physical_page_number = esa_offset + sid / esas_per_page;
    addr.local_idx = sid % esas_per_page;

    return addr;
}

ShardID 
esa_address_to_eid(ESAAddress addr)
{
    ExtentID sid;
    if(!ESAAddressIsValid(&addr))
    {        
        ereport(PANIC,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("ESAAddress is invalid"),
                     errdetail("ESAAddress is (%d,%d).",
                               addr.physical_page_number, addr.local_idx)));
        
    }

    sid = (addr.physical_page_number - ESAPAGE_OFFSET)*ESAS_PER_PAGE + addr.local_idx;
    return sid;
}

/* 
 * NOTICE: caller MUST have been acquired the lock on buffer page before calling this method.
 */
void 
esa_page_set_anchor(Page pg, int local_index, int    set_flags,
                    ExtentID scan_head, ExtentID scan_tail,
                    ExtentID alloc_head, ExtentID alloc_tail)
{
    ESAPage esa_pg;

    if(local_index > ESAS_PER_PAGE)
    {
        ereport(PANIC,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("local index of esa page"),
                 errdetail("ExtentID must be less than %d.",
                           InvalidExtentID)));
    }
    esa_pg = (ESAPage)PageGetContents(pg);

    if((set_flags & ESA_SETFLAG_SCANHEAD) != 0)
        esa_pg->anchors[local_index].scan_head = scan_head;
    if((set_flags & ESA_SETFLAG_SCANTAIL) != 0)
        esa_pg->anchors[local_index].scan_tail = scan_tail;
    if((set_flags & ESA_SETFLAG_ALLOCHEAD) != 0)
        esa_pg->anchors[local_index].alloc_head = alloc_head;
    if((set_flags & ESA_SETFLAG_ALLOCTAIL) != 0)
        esa_pg->anchors[local_index].alloc_tail = alloc_tail;
}

void
esa_set_anchor(Relation rel, ShardID sid, int    set_flags,
            ExtentID scan_head, ExtentID scan_tail,
            ExtentID alloc_head, ExtentID alloc_tail)
{
    Buffer buf;    
    ESAAddress addr;
    Page    pg;

    addr = esa_sid_to_address(sid);

    ExtentAssert(ESAAddressIsValid(&addr));

    buf = extent_readbuffer(rel, addr.physical_page_number, true);

    if(BufferIsInvalid(buf))
    {
        ereport(PANIC,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("get block from ema file failed."),
                     errdetail("reloid:%d, block number:%d",
                               RelationGetRelid(rel), addr.physical_page_number)));
    }
    
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    pg = BufferGetPage(buf);

    esa_page_set_anchor(pg, addr.local_idx, set_flags, 
                    scan_head, scan_tail, alloc_head, alloc_tail);
    MarkBufferDirty(buf);
    UnlockReleaseBuffer(buf);
}

EMAShardAnchor 
esa_get_anchor(Relation rel, ShardID sid)
{
    Buffer buf;    
    ESAAddress addr;
    Page    pg;
    ESAPage esa_pg;
    EMAShardAnchor anchor;

    addr = esa_sid_to_address(sid);

    Assert(ESAAddressIsValid(&addr));

    buf = extent_readbuffer(rel, addr.physical_page_number, true);

    if(BufferIsInvalid(buf))
    {
        ereport(PANIC,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("get block from ema file failed."),
                     errdetail("reloid:%d, block number:%d",
                               RelationGetRelid(rel), addr.physical_page_number)));
    }
    
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    pg = BufferGetPage(buf);
    esa_pg = (ESAPage)PageGetContents(pg);
    anchor = esa_pg->anchors[addr.local_idx];
    UnlockReleaseBuffer(buf);

    return anchor;
}

/*-----------------------------------------------------------------------------
 *
 *         EMA Page
 *
 *-----------------------------------------------------------------------------
 */
void 
ema_init_page(Page page, int32 max_emes)
{
    EMAPage ema_pg = (EMAPage)PageGetContents(page);

    ema_pg->max_emes = max_emes;
    ema_pg->n_emes = 0;
}

void
ema_clean_page(EMAPage page)
{
    memset(page->ema, 0, sizeof(ExtentMappingElement) * page->max_emes);
    page->n_emes = 0;
}

void
ema_truncate_page(EMAPage page, int last_offset)
{
    memset(page->ema + last_offset, 0, sizeof(ExtentMappingElement) * (page->max_emes - last_offset));
    page->n_emes = last_offset;
}

void
ema_init_eme(Relation rel, ExtentID eid, ShardID sid)
{
    EMAAddress addr;
    Buffer buf;
    Page pg;

    addr = ema_eid_to_address(eid);
    ExtentAssert(EMAAddressIsValid(&addr));

    buf = extent_readbuffer(rel, addr.physical_page_number, true);

    if(BufferIsInvalid(buf))
    {
        ereport(PANIC,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("get block from ema file failed."),
                     errdetail("reloid:%d, block number:%d",
                               RelationGetRelid(rel), addr.physical_page_number)));
    }
    
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

    pg = BufferGetPage(buf);
    
    ema_page_init_eme(pg,addr.local_idx,sid,MAX_FREESPACE);

    MarkBufferDirty(buf);
    UnlockReleaseBuffer(buf);
}

void
ema_page_init_eme(Page pg, int32 local_index, ShardID sid, uint8 freespace)
{
    EMAPage ema_page;
    
    if(local_index > EMES_PER_PAGE)
    {
        ereport(PANIC,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("local index of EMA page"),
                 errdetail("local_index must be less than %ld.",
                           EMES_PER_PAGE)));
    }
    
    ema_page = (EMAPage)PageGetContents(pg);
    ExtentAssert(ema_page->ema[local_index].is_occupied == 0);
    
    ema_page->ema[local_index].shardid = sid;
    ema_page->ema[local_index].is_occupied = 1;
    ema_page->ema[local_index].max_freespace = freespace;
    ema_page->ema[local_index].hwm = 1;
    ema_page->ema[local_index].alloc_next = InvalidExtentID;
    ema_page->ema[local_index].scan_next = InvalidExtentID;
    ema_page->ema[local_index].scan_prev = InvalidExtentID;
    EMESetAllocPrev(&ema_page->ema[local_index], InvalidExtentID);
}

void
ema_free_eme(Relation rel, ExtentID eid)
{
    EMAAddress addr;
    Buffer buf;
    Page pg;

    addr = ema_eid_to_address(eid);
    ExtentAssert(EMAAddressIsValid(&addr));

    buf = extent_readbuffer(rel, addr.physical_page_number, false);

    if(BufferIsInvalid(buf))
    {
        ereport(PANIC,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("get block from ema file failed."),
                     errdetail("reloid:%d, block number:%d",
                               RelationGetRelid(rel), addr.physical_page_number)));
    }
    
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

    pg = BufferGetPage(buf);
    
    ema_page_free_eme(pg,addr.local_idx);
    MarkBufferDirty(buf);
    UnlockReleaseBuffer(buf);
}

void
ema_page_free_eme(Page pg, int32 local_index)
{
    EMAPage ema_page;
        
    if(local_index > EMES_PER_PAGE)
    {
        ereport(PANIC,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("local index of EMA page"),
                 errdetail("local_index must be less than %ld.",
                           EMES_PER_PAGE)));
    }
    
    ema_page = (EMAPage)PageGetContents(pg);

    ema_page->ema[local_index].shardid = 0;
    ema_page->ema[local_index].is_occupied = false;
    ema_page->ema[local_index].max_freespace = 0;
    ema_page->ema[local_index].hwm = 0;
    ema_page->ema[local_index].alloc_next = InvalidExtentID;
    ema_page->ema[local_index].scan_next = InvalidExtentID;
    ema_page->ema[local_index].scan_prev = InvalidExtentID;
    EMESetAllocPrev(&ema_page->ema[local_index], InvalidExtentID);

}

EMAAddress 
ema_eid_to_address(ExtentID eid)
{
    EMAAddress addr;
    
    if(!ExtentIdIsValid(eid))
    {        
        ereport(PANIC,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("ExtentID is invalid"),
                     errdetail("ExtentID must be less than %d.",
                               InvalidExtentID)));
        
    }

    addr.physical_page_number = EMAPAGE_OFFSET + eid / EMES_PER_PAGE;
    addr.local_idx = eid % EMES_PER_PAGE;

    return addr;
}

ExtentID 
ema_address_to_eid(EMAAddress addr)
{
    ExtentID eid;
    if(!EMAAddressIsValid(&addr))
    {    
#if 0
        ereport(PANIC,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("EMAAddress is invalid"),
                     errdetail("EMAAddress is (%d,%d).",
                               addr.physical_page_number, addr.local_idx)));
#endif
        return InvalidExtentID;
    }

    eid = (addr.physical_page_number - EMAPAGE_OFFSET)*EMES_PER_PAGE + addr.local_idx;
    return eid;
}

ExtentID
ema_next_alloc(Relation rel, ExtentID curr, bool error_if_free, bool *is_occupied, ShardID *sid, int *hwm, uint8 *freespace)
{
    EMAAddress    curr_eme;
    Buffer         buf;
    EMAPage        pg;
    ExtentID    next_extent;

    curr_eme = ema_eid_to_address(curr);
    buf = extent_readbuffer(rel, curr_eme.physical_page_number, false);
    ExtentAssert(BufferIsValid(buf));

    LockBuffer(buf, BUFFER_LOCK_SHARE);

    pg = (EMAPage)PageGetContents(BufferGetPage(buf));

    if(curr_eme.local_idx >= pg->n_emes)
        elog(ERROR, "extent %d is not exist in EMA page.", curr);

    if(error_if_free)
    {
        ExtentAssertEMEIsOccup(pg->ema[curr_eme.local_idx]);
    }
    next_extent = pg->ema[curr_eme.local_idx].alloc_next;

    if(is_occupied)
        *is_occupied = (bool)pg->ema[curr_eme.local_idx].is_occupied;
    
    if(sid)
        *sid = pg->ema[curr_eme.local_idx].shardid;
    
    if(hwm)
        *hwm = pg->ema[curr_eme.local_idx].hwm;
    
    if(freespace)
        *freespace = pg->ema[curr_eme.local_idx].max_freespace;

    UnlockReleaseBuffer(buf);

    return next_extent;
}

ExtentID
ema_prev_alloc(Relation rel, ExtentID curr, uint8 *freespace, ShardID *sid)
{
    EMAAddress    curr_eme;
    Buffer         buf;
    EMAPage        pg;
    ExtentID    prev_extent;

    curr_eme = ema_eid_to_address(curr);
    buf = extent_readbuffer(rel, curr_eme.physical_page_number, false);
    ExtentAssert(BufferIsValid(buf));
    LockBuffer(buf, BUFFER_LOCK_SHARE);

    pg = (EMAPage)PageGetContents(BufferGetPage(buf));

    //ExtentAssertEMEIsOccup(pg->ema[curr_eme.local_idx]);
    prev_extent = EMEGetAllocPrev(&(pg->ema[curr_eme.local_idx]));

    if(freespace)
        *freespace = pg->ema[curr_eme.local_idx].max_freespace;

    if(sid)
        *sid = pg->ema[curr_eme.local_idx].shardid;

    UnlockReleaseBuffer(buf);

    return prev_extent;
}

ExtentID
ema_next_scan(Relation rel, ExtentID curr, bool error_if_free, bool *is_occupied, ShardID *sid, int *hwm, uint8 *freespace)
{
    EMAAddress    curr_eme;
    Buffer         buf;
    EMAPage        pg;
    ExtentID    next_extent;

    curr_eme = ema_eid_to_address(curr);
    buf = extent_readbuffer(rel, curr_eme.physical_page_number, false);
    ExtentAssert(BufferIsValid(buf));
    LockBuffer(buf, BUFFER_LOCK_SHARE);

    pg = (EMAPage)PageGetContents(BufferGetPage(buf));

    if(error_if_free)
    {
        ExtentAssertEMEIsOccup(pg->ema[curr_eme.local_idx]);
    }
    next_extent = pg->ema[curr_eme.local_idx].scan_next;

    if(is_occupied)
        *is_occupied = (bool)pg->ema[curr_eme.local_idx].is_occupied;
    
    if(sid)
        *sid = pg->ema[curr_eme.local_idx].shardid;
    
    if(hwm)
        *hwm = pg->ema[curr_eme.local_idx].hwm;
    
    if(freespace)
        *freespace = pg->ema[curr_eme.local_idx].max_freespace;    

    UnlockReleaseBuffer(buf);

    return next_extent;
}

ExtentID
ema_prev_scan(Relation rel, ExtentID curr, uint8 *freespace, ShardID *sid)
{
    EMAAddress    curr_eme;
    Buffer         buf;
    EMAPage        pg;
    ExtentID    prev_extent;

    curr_eme = ema_eid_to_address(curr);
    buf = extent_readbuffer(rel, curr_eme.physical_page_number, false);
    ExtentAssert(BufferIsValid(buf));
    LockBuffer(buf, BUFFER_LOCK_SHARE);

    pg = (EMAPage)PageGetContents(BufferGetPage(buf));

    //ExtentAssertEMEIsOccup(pg->ema[curr_eme.local_idx]);
    prev_extent = pg->ema[curr_eme.local_idx].scan_prev;

    if(freespace)
        *freespace = pg->ema[curr_eme.local_idx].max_freespace;

    if(sid)
        *sid = pg->ema[curr_eme.local_idx].shardid;

    UnlockReleaseBuffer(buf);

    return prev_extent;

}

ExtentMappingElement *
ema_get_eme(Relation rel, ExtentID eid)
{
    EMAAddress    addr;
    Buffer         buf;
    Page        pg;
    ExtentMappingElement *eme;

    addr = ema_eid_to_address(eid);
    buf = extent_readbuffer(rel, addr.physical_page_number, false);
    ExtentAssert(BufferIsValid(buf));
    LockBuffer(buf, BUFFER_LOCK_SHARE);

    pg = BufferGetPage(buf);

    eme = ema_page_get_eme(pg, addr.local_idx);

    UnlockReleaseBuffer(buf);

    return eme;
}

ExtentMappingElement * 
ema_page_get_eme(Page pg, int32 local_index)
{
    EMAPage ema_page;
    ExtentMappingElement *result = NULL;
    
    if(local_index > EMES_PER_PAGE)
    {
        ereport(PANIC,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("local index of EMA page"),
                 errdetail("local_index must be less than %ld.",
                           EMES_PER_PAGE)));
    }
    
    ema_page = (EMAPage)PageGetContents(pg);

    result = (ExtentMappingElement *)palloc(sizeof(ExtentMappingElement));

    memcpy(result, &ema_page->ema[local_index], sizeof(ExtentMappingElement));

    return result;
}

void 
ema_get_eme_extract(Relation rel, ExtentID eid, 
                    bool *is_occupied, ShardID     *sid, int *hwm, uint8 *freespace)

{
    EMAAddress    addr;
    Buffer         buf;
    Page        pg;

    addr = ema_eid_to_address(eid);
    buf = extent_readbuffer(rel, addr.physical_page_number, false);
    ExtentAssert(BufferIsValid(buf));
    LockBuffer(buf, BUFFER_LOCK_SHARE);

    pg = BufferGetPage(buf);

    ema_page_get_eme_extract(pg, addr.local_idx, is_occupied, sid, hwm, freespace);

    UnlockReleaseBuffer(buf);
}


void 
ema_page_get_eme_extract(Page pg, int32 local_index, 
                                bool *is_occupied, ShardID     *sid, int *hwm, uint8 *freespace)
{
    EMAPage ema_page;
    
    if(local_index > EMES_PER_PAGE)
    {
        ereport(PANIC,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("local index of EMA page"),
                 errdetail("local_index must be less than %ld.",
                           EMES_PER_PAGE)));
    }
    
    ema_page = (EMAPage)PageGetContents(pg);

    if(is_occupied)
        *is_occupied = (bool)ema_page->ema[local_index].is_occupied;

    if(sid)
        *sid = (ShardID)ema_page->ema[local_index].shardid;

    if(hwm)
        *hwm = ema_page->ema[local_index].hwm;

    if(freespace)
        *freespace = (uint8)ema_page->ema[local_index].max_freespace;
}


void
ema_set_eme_hwm(Relation rel, ExtentID eid, int16 hwm)
{
    EMAAddress addr;
    Buffer buf;
    Page pg;

    addr = ema_eid_to_address(eid);
    ExtentAssert(EMAAddressIsValid(&addr));

    buf = extent_readbuffer(rel, addr.physical_page_number, false);

    if(BufferIsInvalid(buf))
    {
        ereport(PANIC,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("get block from ema file failed."),
                     errdetail("reloid:%d, block number:%d",
                               RelationGetRelid(rel), addr.physical_page_number)));
    }
    
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

    pg = BufferGetPage(buf);
    
    ema_page_set_eme_hwm(pg, addr.local_idx, hwm);
    MarkBufferDirty(buf);
    UnlockReleaseBuffer(buf);
}

void
ema_page_set_eme_hwm(Page pg, int32 local_index, int16 hwm)
{
    EMAPage ema_page;
        
    if(local_index > EMES_PER_PAGE)
    {
        ereport(PANIC,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("local index of EMA page"),
                 errdetail("local_index must be less than %ld.",
                           EMES_PER_PAGE)));
    }
    
    ema_page = (EMAPage)PageGetContents(pg);

    if(!ema_page->ema[local_index].is_occupied)
    {
        //TODO: rebuild eme
    }
    
    ema_page->ema[local_index].hwm = 0;
}


void
ema_set_eme_freespace(Relation rel, ExtentID eid, uint8 freespace)
{
    EMAAddress addr;
    Buffer buf;

    addr = ema_eid_to_address(eid);
    ExtentAssert(EMAAddressIsValid(&addr));

    buf = extent_readbuffer(rel, addr.physical_page_number, false);

    if(BufferIsInvalid(buf))
    {
        ereport(PANIC,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("get block from ema file failed."),
                     errdetail("reloid:%d, block number:%d",
                               RelationGetRelid(rel), addr.physical_page_number)));
    }
    
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

    ExtentAssertEMEIsOccup(BufferGetEMAPage(buf)->ema[addr.local_idx]);
    
    START_CRIT_SECTION();
    
    ema_page_set_eme_freespace(BufferGetPage(buf), addr.local_idx, freespace);
    MarkBufferDirty(buf);

    /*
     * write xlog
     */
    {
        xl_extent_seteme xlrec;
        XLogRecPtr    recptr;
        INIT_EXLOG_SETEME(&xlrec);
        
        XLogBeginInsert();
        XLogRegisterBuffer(0, buf, REGBUF_KEEP_DATA);

        xlrec.slot = addr.local_idx;
        xlrec.setflag = EMA_SETFLAG_FREESPACE;
        INIT_EME(&xlrec.eme);
        xlrec.eme.max_freespace = freespace;
        XLogRegisterBufData(0, (char *)&xlrec, SizeOfSetEME);
        recptr = XLogInsert(RM_EXTENT_ID, XLOG_EXTENT_UPDATE_EME);
        PageSetLSN(BufferGetPage(buf), recptr);
    }
    END_CRIT_SECTION();
    UnlockReleaseBuffer(buf);

    if(trace_extent)
    {
        ereport(LOG,
            (errmsg("[trace extent]Update Extent Freespace:[rel:%d/%d/%d]"
                    "[eid:%d, ema block number:%d, offset:%d, freespace:%d]",
                    rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
                    eid, addr.physical_page_number, addr.local_idx, freespace)));
    }
}

void
ema_page_set_eme_freespace(Page pg, int32 local_index, uint8 freespace)
{
    EMAPage ema_page;
        
    if(local_index > EMES_PER_PAGE)
    {
        ereport(PANIC,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("local index of EMA page"),
                 errdetail("local_index must be less than %ld.",
                           EMES_PER_PAGE)));
    }
    
    ema_page = (EMAPage)PageGetContents(pg);

    ExtentAssert(ema_page->ema[local_index].is_occupied == 1);
    
    ema_page->ema[local_index].max_freespace = freespace;

}

void
ema_rnode_set_eme_freespace(RelFileNode rnode, ExtentID eid, uint8 freespace)
{
    EMAAddress addr;
    Buffer     ema_buf;
    Page pg;
    
    addr = ema_eid_to_address(eid);
    
    ema_buf = XLogReadBufferExtended(rnode, EXTENT_FORKNUM, addr.physical_page_number, RBM_ZERO_ON_ERROR);

    if(BufferIsInvalid(ema_buf))
    {
        ereport(PANIC,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("get block from ema file failed."),
                     errdetail("rnode:%d/%d, block number:%d",
                               rnode.dbNode, rnode.relNode, addr.physical_page_number)));
    }

    LockBuffer(ema_buf, BUFFER_LOCK_EXCLUSIVE);

    pg = BufferGetPage(ema_buf);
    
    ema_page_set_eme_freespace(pg, addr.local_idx, freespace);
    MarkBufferDirty(ema_buf);
    UnlockReleaseBuffer(ema_buf);
}

void 
ema_set_eme_link(Relation rel, ExtentID eid, int32 set_flags, 
                    ExtentID scan_prev, ExtentID scan_next, 
                    ExtentID alloc_prev, ExtentID alloc_next)
{
    EMAAddress addr;
    Buffer buf;
    Page pg;

    addr = ema_eid_to_address(eid);
    ExtentAssert(EMAAddressIsValid(&addr));

    buf = extent_readbuffer(rel, addr.physical_page_number, false);

    if(BufferIsInvalid(buf))
    {
        ereport(PANIC,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("get block from ema file failed."),
                     errdetail("reloid:%d, block number:%d",
                               RelationGetRelid(rel), addr.physical_page_number)));
    }
    
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

    pg = BufferGetPage(buf);
    
    ema_page_set_eme_link(pg, addr.local_idx, set_flags,
                        scan_prev, scan_next, alloc_prev, alloc_next);
    MarkBufferDirty(buf);
    UnlockReleaseBuffer(buf);    
}

void 
ema_page_set_eme_link(Page pg, int32 local_index, int32 set_flags, 
                    ExtentID scan_prev, ExtentID scan_next, 
                    ExtentID alloc_prev, ExtentID alloc_next)
{
    EMAPage ema_page;
        
    if(local_index > EMES_PER_PAGE)
    {
        ereport(PANIC,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("local index of EMA page"),
                 errdetail("local_index must be less than %ld.",
                           EMES_PER_PAGE)));
    }
    
    ema_page = (EMAPage)PageGetContents(pg);

    ExtentAssertEMEIsOccup(ema_page->ema[local_index]);
    
    if((set_flags & EMA_SETFLAG_SCANPREV) != 0)
        ema_page->ema[local_index].scan_prev = scan_prev;
    if((set_flags & EMA_SETFLAG_SCANNEXT) != 0)
        ema_page->ema[local_index].scan_next = scan_next;
    if((set_flags & EMA_SETFLAG_ALLOCPREV) != 0)
        EMESetAllocPrev(&ema_page->ema[local_index], alloc_prev);
    if((set_flags & EMA_SETFLAG_ALLOCNEXT) != 0)
        ema_page->ema[local_index].alloc_next = alloc_next;
}

void
ema_page_set_eme(Page pg, int32 local_index, int32 set_flags, ExtentMappingElement *tempEME)
{// #lizard forgives
    EMAPage ema_page;
        
    if(local_index > EMES_PER_PAGE)
    {
        ereport(PANIC,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("local index of EMA page"),
                 errdetail("local_index must be less than %ld.",
                           EMES_PER_PAGE)));
    }
    
    ema_page = (EMAPage)PageGetContents(pg);

    if((set_flags & EMA_SETFLAG_SHARDID) != 0)
        ema_page->ema[local_index].shardid = tempEME->shardid;
    if((set_flags & EMA_SETFLAG_FREESPACE) != 0)
        ema_page->ema[local_index].max_freespace = tempEME->max_freespace;
    if((set_flags & EMA_SETFLAG_HWM) != 0)
        ema_page->ema[local_index].hwm = tempEME->hwm;
    if((set_flags & EMA_SETFLAG_OCCUPIED) != 0)
        ema_page->ema[local_index].is_occupied = tempEME->is_occupied;    
    if((set_flags & EMA_SETFLAG_SCANPREV) != 0)
        ema_page->ema[local_index].scan_prev = tempEME->scan_prev;
    if((set_flags & EMA_SETFLAG_SCANNEXT) != 0)
        ema_page->ema[local_index].scan_next = tempEME->scan_next;
    if((set_flags & EMA_SETFLAG_ALLOCPREV) != 0)
        EMESetAllocPrev(&ema_page->ema[local_index], EMEGetAllocPrev(tempEME));
    if((set_flags & EMA_SETFLAG_ALLOCNEXT) != 0)
        ema_page->ema[local_index].alloc_next = tempEME->alloc_next;

    if(set_flags == EMA_SETFLAG_INIT)
        ema_page_init_eme(pg, local_index, tempEME->shardid, tempEME->max_freespace);

    if(set_flags == EMA_SETFLAG_CLEAN)
        ema_page_free_eme(pg, local_index);
}

void
ema_page_extend_eme(Page pg, int n_emes, bool cleaneme, int clean_start, int clean_end)
{
    EMAPage ema_page;
    
    ema_page = (EMAPage)PageGetContents(pg);

    if(n_emes <= ema_page->n_emes)
        return;
    
    if(cleaneme)
    {
        int i;
        for(i = clean_start; i <= clean_end; i++)
        {
            ema_page->ema[i].scan_next = InvalidExtentID;
            ema_page->ema[i].scan_prev= InvalidExtentID;
            ema_page->ema[i].alloc_next= InvalidExtentID;
            EMESetAllocPrev(&ema_page->ema[i], InvalidExtentID);
        }
    }

    ema_page->n_emes = n_emes;
}

void
ema_truncate(Relation rel, ExtentID new_max_eid, ExtentID old_max_eid)
{// #lizard forgives
    int blkno = 0;
    int old_blks = 0;
    int old_last_offset = 0;
    int new_blks = 0;
    int new_last_offset = 0;
    Buffer buf;

    xl_extent_cleanema clean_xlrec;
    xl_extent_truncema trunc_xlrec;
    XLogRecPtr lsn;
    
    if(new_max_eid >= old_max_eid)
        return;

    /* init xlog data struct */
    INIT_EXLOG_CLEANEMA(&clean_xlrec);
    memcpy(&(clean_xlrec.rnode), &(rel->rd_node), sizeof(RelFileNode));    

    INIT_EXLOG_TRUNCEMA(&trunc_xlrec);
    memcpy(&(trunc_xlrec.rnode), &(rel->rd_node), sizeof(RelFileNode));    

    /*
     * clean all of ema pages if new heap is empty.
     */
    if(new_max_eid == 0)
    {
        old_blks = (old_max_eid + EMES_PER_PAGE - 1)/EMES_PER_PAGE;
        for(blkno = old_blks - 1; blkno >=0; blkno--)
        {
            buf = extent_readbuffer(rel, blkno + EMAPAGE_OFFSET, true);
            ExtentAssert( BufferIsValid(buf) );
            LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

            START_CRIT_SECTION();
            ema_clean_page( BufferGetEMAPage(buf) );

            /* write xlog */            
            clean_xlrec.pageno = BufferGetBlockNumber(buf);
            XLogBeginInsert();
            XLogRegisterData( (char *) &clean_xlrec, SizeOfCleanEMA);
            lsn = XLogInsert(RM_EXTENT_ID, XLOG_EXTENT_COMMON);

            PageSetLSN(BufferGetPage(buf), lsn);
            MarkBufferDirty(buf);
            
            END_CRIT_SECTION();
            UnlockReleaseBuffer(buf);
            if(trace_extent)
            {
                ereport(LOG,
                      (errmsg("[trace extent]Clean EME Page:[rel:%d/%d/%d]"
                              "[ema block number:%d, ema page index:%d]",
                              rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
                              clean_xlrec.pageno, blkno)));
            }
        }
        return;
    }

    ExtentAssert(new_max_eid > 0);
    ExtentAssert(old_max_eid > new_max_eid);

    /*
     * computer which pages will be clean or truncate
     */
    old_blks = old_max_eid / EMES_PER_PAGE;
    old_last_offset = old_max_eid % EMES_PER_PAGE;
    if (old_last_offset == 0)
    {
        old_blks--;
        old_last_offset = EMES_PER_PAGE;
    }

    new_blks = new_max_eid/ EMES_PER_PAGE;
    new_last_offset = new_max_eid % EMES_PER_PAGE;
    if(new_last_offset == 0)
    {
        new_blks--;
        new_last_offset = EMES_PER_PAGE;
    }

    /*
     * clean pages after the max page of new extent segement
     */
    blkno = old_blks;
    while(blkno > new_blks)
    {
        buf = extent_readbuffer(rel, blkno + EMAPAGE_OFFSET, true);    
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

        START_CRIT_SECTION();
        ema_clean_page(BufferGetEMAPage(buf));

        /* write xlog */
        clean_xlrec.pageno = BufferGetBlockNumber(buf);
        XLogBeginInsert();
        XLogRegisterData((char *) &clean_xlrec, SizeOfCleanEMA);
        lsn = XLogInsert(RM_EXTENT_ID, XLOG_EXTENT_COMMON);

        PageSetLSN(BufferGetPage(buf),lsn);
        MarkBufferDirty(buf);
        
        END_CRIT_SECTION();
        UnlockReleaseBuffer(buf);

        if(trace_extent)
        {
            ereport(LOG,
                (errmsg("[trace extent]Clean EME Page:[rel:%d/%d/%d]"
                        "[ema block number:%d, ema page index:%d]",
                        rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
                        clean_xlrec.pageno, blkno)));
        }
        blkno--;
    }

    /*
     * truncate one page
     */
    ExtentAssert(blkno == new_blks);
    buf = extent_readbuffer(rel, blkno + EMAPAGE_OFFSET, true);

    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

    START_CRIT_SECTION();
    
    ema_truncate_page(BufferGetEMAPage(buf), new_last_offset);

    /* write xlog */
    trunc_xlrec.pageno = BufferGetBlockNumber(buf);
    trunc_xlrec.offset = new_last_offset;
    XLogBeginInsert();
    XLogRegisterData((char *) &trunc_xlrec, SizeOfTruncEMA);
    lsn = XLogInsert(RM_EXTENT_ID, XLOG_EXTENT_COMMON);

    PageSetLSN(BufferGetPage(buf),lsn);
    MarkBufferDirty(buf);
    END_CRIT_SECTION();
    UnlockReleaseBuffer(buf);

    if(trace_extent)
    {
        ereport(LOG,
            (errmsg("[trace extent]Truncate EME Page:[rel:%d/%d/%d]"
                    "[ema block number:%d, ema page index:%d, offset:%d]",
                    rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
                    trunc_xlrec.pageno, blkno, trunc_xlrec.offset)));
    }
}

/*-----------------------------------------------------------------------------
 *
 *         list operation
 *
 *-----------------------------------------------------------------------------
 */

bool
shard_add_extent(Relation rel, ShardID sid, ExtentID eid)
{// #lizard forgives
    EMAShardAnchor anchor;
    EMAAddress    targ_eme_addr;
    Buffer        targ_eme_buf;
    EMAPage        targ_eme_pg;
    Page        targ_eme_page;
    
    LockShard(rel, sid, ExclusiveLock);

    /*
     * check EME is free
     */
    targ_eme_addr = ema_eid_to_address(eid);
    targ_eme_buf = extent_readbuffer(rel, targ_eme_addr.physical_page_number, false);
    ExtentAssert(BufferIsValid(targ_eme_buf));
    LockBuffer(targ_eme_buf, BUFFER_LOCK_SHARE);
    targ_eme_page = (Page)BufferGetPage(targ_eme_buf);
    targ_eme_pg = (EMAPage)PageGetContents(targ_eme_page);
    
    if(targ_eme_pg->n_emes <= targ_eme_addr.local_idx)
    {
        elog(ERROR, "internal error: system is trying to add a nonexist extent to shardlist.");
    }

    if(targ_eme_pg->ema[targ_eme_addr.local_idx].is_occupied != 0)
    {
        /* extent is occupied */
        UnlockReleaseBuffer(targ_eme_buf);
        UnlockShard(rel, sid, ExclusiveLock);
        return false;
    }

    LockBuffer(targ_eme_buf, BUFFER_LOCK_UNLOCK);
    
    /* 
     * add this extent to alloc list and scan list 
     * If system crash in this point, the free extent will be leaked.
     */
    anchor = esa_get_anchor(rel, sid);    
    if(!ExtentIdIsValid(anchor.alloc_head) && !ExtentIdIsValid(anchor.scan_head))
    {
        /*
         * both two lists is empty
         */
        Buffer esa_buf;    
        ESAAddress esa_addr;
        ESAPage esa_pg;
        xl_extent_setesa esa_xlrec;
        xl_extent_seteme eme_xlrec;
        XLogRecPtr    recptr;

        INIT_EXLOG_SETESA(&esa_xlrec);
        INIT_EXLOG_SETEME(&eme_xlrec);
        /*
         * get buffer for esa 
         * registered block no: 0
         */
        esa_addr = esa_sid_to_address(sid);
        esa_buf = extent_readbuffer(rel, esa_addr.physical_page_number, false);
        ExtentAssert(BufferIsValid(esa_buf));
        LockBuffer(esa_buf, BUFFER_LOCK_EXCLUSIVE);

        /*
         * lock buffer for target eme.
         * recheck whether the target eme is free.
         * registered block no: 1
         */
        LockBuffer(targ_eme_buf, BUFFER_LOCK_EXCLUSIVE);
        if(targ_eme_pg->ema[targ_eme_addr.local_idx].is_occupied != 0)
        {
            /* extent is occupied */
            UnlockReleaseBuffer(targ_eme_buf);
            UnlockReleaseBuffer(esa_buf);
            UnlockShard(rel, sid, ExclusiveLock);
            return false;
        }
        
        START_CRIT_SECTION();

        /*
         * update anchor
         */
        esa_pg = (ESAPage)PageGetContents(BufferGetPage(esa_buf));
        esa_pg->anchors[esa_addr.local_idx].alloc_head = eid;
        esa_pg->anchors[esa_addr.local_idx].alloc_tail = eid;
        esa_pg->anchors[esa_addr.local_idx].scan_head  = eid;
        esa_pg->anchors[esa_addr.local_idx].scan_tail  = eid;

        MarkBufferDirty(esa_buf);
        
        /*
         * update target EME 
         */
        ExtentAssertEMEIsFree(targ_eme_pg->ema[targ_eme_addr.local_idx]);
        INIT_EME(&targ_eme_pg->ema[targ_eme_addr.local_idx]);
        targ_eme_pg->ema[targ_eme_addr.local_idx].is_occupied = 1;
        targ_eme_pg->ema[targ_eme_addr.local_idx].shardid = sid;
        targ_eme_pg->ema[targ_eme_addr.local_idx].max_freespace = MAX_FREESPACE;
        targ_eme_pg->ema[targ_eme_addr.local_idx].hwm = 1;
        
        /*write xlog*/
        XLogBeginInsert();
        XLogRegisterBuffer(0, esa_buf, REGBUF_KEEP_DATA);
        esa_xlrec.slot = esa_addr.local_idx;
        esa_xlrec.setflag = ESA_SETFLAG_ALL;
        esa_xlrec.anchor.alloc_head = eid;
        esa_xlrec.anchor.alloc_tail = eid;
        esa_xlrec.anchor.scan_head = eid;
        esa_xlrec.anchor.scan_tail = eid;        
        XLogRegisterBufData(0, (char *)&esa_xlrec, SizeOfSetESA);

        XLogRegisterBuffer(1, targ_eme_buf, REGBUF_KEEP_DATA);    
        eme_xlrec.setflag = EMA_SETFLAG_ALL;
        eme_xlrec.slot = targ_eme_addr.local_idx;
        INIT_EME(&eme_xlrec.eme);
        eme_xlrec.eme.is_occupied = 1;
        eme_xlrec.eme.shardid = sid;
        eme_xlrec.eme.max_freespace = MAX_FREESPACE;
        eme_xlrec.eme.hwm = 1;
        XLogRegisterBufData(1, (char *)&eme_xlrec, SizeOfSetEME);
        
        recptr = XLogInsert(RM_EXTENT_ID, XLOG_EXTENT_ATTACH_EXTENT);
        PageSetLSN(BufferGetPage(esa_buf), recptr);    
        PageSetLSN(BufferGetPage(targ_eme_buf), recptr);
        MarkBufferDirty(targ_eme_buf);

        END_CRIT_SECTION();
        UnlockReleaseBuffer(esa_buf);
        UnlockReleaseBuffer(targ_eme_buf);

        if(trace_extent)
        {
            ereport(LOG,
                (errmsg("[trace extent]AttachExtent:[rel:%d/%d/%d][mode:1:scanlist is empty]"
                        "[sid:%d, eid:%d]"
                        "[setesa:blocknum=%d,offset=%d,scanhead=%d,scantail=%d,allochead=%d,alloctail=%d]"
                        "[seteme:blocknum=%d,offset=%d,sid=%d,freespace=MAX_FREESPACE]",
                        rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
                        sid, eid,
                        esa_addr.physical_page_number, esa_addr.local_idx, eid, eid, eid, eid,
                        targ_eme_addr.physical_page_number, targ_eme_addr.local_idx, sid)));
        }
    }
    else if(!ExtentIdIsValid(anchor.alloc_head) && ExtentIdIsValid(anchor.scan_head))
    {
        /* alloc list is emtpy, but scan list is not empty */

        /*
         * if free extentid is precede scan list head, insert this extent to list head.
         */
        if(eid < anchor.scan_head)
        {
            xl_extent_setesa esa_xlrec;
            xl_extent_seteme eme_xlrec;
            XLogRecPtr    recptr;
            Buffer esa_buf; 
            ESAAddress esa_addr;
            ESAPage esa_pg;

            INIT_EXLOG_SETESA(&esa_xlrec);
            INIT_EXLOG_SETEME(&eme_xlrec);
            /*
             * get buffer for esa 
             * registered block no: 0
             */
            esa_addr = esa_sid_to_address(sid);
            esa_buf = extent_readbuffer(rel,  esa_addr.physical_page_number, false);
            ExtentAssert( BufferIsValid(esa_buf));
            LockBuffer(esa_buf,  BUFFER_LOCK_EXCLUSIVE);

            /*
             * lock buffer for target eme.
             * recheck whether the target eme is free.
             * registered block no: 1
             */
            LockBuffer(targ_eme_buf, BUFFER_LOCK_EXCLUSIVE);
            if((targ_eme_pg->ema[targ_eme_addr.local_idx].is_occupied) != 0)
            {
                /* extent is occupied */
                UnlockReleaseBuffer(targ_eme_buf);
                UnlockReleaseBuffer(esa_buf);
                UnlockShard(rel,  sid, ExclusiveLock);
                return false;
            }

            START_CRIT_SECTION();
    
            /*
             * update anchor
             */
            esa_pg = (ESAPage)PageGetContents( BufferGetPage(esa_buf));
            esa_pg->anchors[esa_addr.local_idx].alloc_tail =  eid;
            esa_pg->anchors[esa_addr.local_idx].alloc_head =  eid;
            esa_pg->anchors[esa_addr.local_idx].scan_head  =  eid;
    
            MarkBufferDirty(esa_buf);
            
            /*
             * update target EME 
             */
            ExtentAssertEMEIsFree(targ_eme_pg->ema[targ_eme_addr.local_idx]);
            INIT_EME(&targ_eme_pg->ema[targ_eme_addr.local_idx]);
            targ_eme_pg->ema[targ_eme_addr.local_idx].is_occupied = 1;
            targ_eme_pg->ema[targ_eme_addr.local_idx].shardid = sid;
            targ_eme_pg->ema[targ_eme_addr.local_idx].max_freespace = MAX_FREESPACE;
            targ_eme_pg->ema[targ_eme_addr.local_idx].hwm = 1;
            targ_eme_pg->ema[targ_eme_addr.local_idx].scan_next = anchor.scan_head;

            /*write xlog*/
            XLogBeginInsert();
            XLogRegisterBuffer(0, esa_buf, REGBUF_KEEP_DATA);
            esa_xlrec.slot = esa_addr.local_idx;
            esa_xlrec.setflag = ESA_SETFLAG_ALLOC | ESA_SETFLAG_SCANHEAD;
            esa_xlrec.anchor.alloc_head = eid;
            esa_xlrec.anchor.alloc_tail = eid;
            esa_xlrec.anchor.scan_head = eid;
            esa_xlrec.anchor.scan_tail = InvalidExtentID;        
            XLogRegisterBufData(0, (char *)&esa_xlrec, SizeOfSetESA);

            XLogRegisterBuffer(1, targ_eme_buf, REGBUF_KEEP_DATA);    
            eme_xlrec.setflag = EMA_SETFLAG_ALL;
            eme_xlrec.slot = targ_eme_addr.local_idx;
            INIT_EME(&eme_xlrec.eme);
            eme_xlrec.eme.is_occupied = 1;
            eme_xlrec.eme.shardid = sid;
            eme_xlrec.eme.max_freespace = MAX_FREESPACE;
            eme_xlrec.eme.hwm = 1;
            eme_xlrec.eme.scan_next = anchor.scan_head;
            XLogRegisterBufData(1, (char *)&eme_xlrec, SizeOfSetEME);
            
            recptr = XLogInsert(RM_EXTENT_ID, XLOG_EXTENT_ATTACH_EXTENT);
            PageSetLSN(BufferGetPage(esa_buf), recptr);    
            PageSetLSN(BufferGetPage(targ_eme_buf), recptr);    

            END_CRIT_SECTION();
            UnlockReleaseBuffer(esa_buf);
            UnlockReleaseBuffer(targ_eme_buf);

            if(trace_extent)
            {
                ereport(LOG,
                    (errmsg("[trace extent]AttachExtent:[rel:%d/%d/%d][mode2:insert extent into "
                                                        "the head of scanlist and alloclist is null]"
                            "[sid:%d, eid:%d]"
                            "[setesa:blocknum=%d,offset=%d,scanhead=%d,allochead=%d,alloctail=%d]"
                            "[seteme:blocknum=%d,offset=%d,sid=%d,scannext=%d,freespace=MAX_FREESPACE]",
                            rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
                            sid, eid,
                            esa_addr.physical_page_number, esa_addr.local_idx, eid, eid, eid,
                            targ_eme_addr.physical_page_number, targ_eme_addr.local_idx, sid, anchor.scan_head)));
            }
        }
        else
        {
            Buffer esa_buf; 
            Buffer left_buf;
            ESAAddress esa_addr;
            EMAAddress left_addr;
            ESAPage esa_pg;
            Page    left_page;
            xl_extent_setesa esa_xlrec;
            xl_extent_seteme eme_xlrec;
            xl_extent_seteme left_xlrec;
            XLogRecPtr    recptr;
            ExtentID    e_left, e_right;
            e_left = e_right = anchor.scan_head;

            INIT_EXLOG_SETESA(&esa_xlrec);
            INIT_EXLOG_SETEME(&eme_xlrec);
            INIT_EXLOG_SETEME(&left_xlrec);
            /* search point to insert to scan list */
            while(ExtentIdIsValid(e_left))
            {
                e_right = ema_next_scan(rel, e_left, true, NULL, NULL, NULL, NULL);
                if(e_left == eid)
                {
                    /* error: this free extent has been already added to scan list. */
                }
                else if(EME_IN_SAME_PAGE(e_left, eid)
                    || (e_left < eid && eid < e_right))
                {
                    break;
                }

                if(!ExtentIdIsValid(e_right))
                    break;

                e_left = e_right;
            }            
    
            /*
             * get buffer for esa 
             * registered block no: 0
             */
            esa_addr = esa_sid_to_address(sid);
            esa_buf = extent_readbuffer(rel, esa_addr.physical_page_number, false);
            ExtentAssert(BufferIsValid(esa_buf));
            LockBuffer(esa_buf, BUFFER_LOCK_EXCLUSIVE);

            /*
             * get buffer for left eme
             * registered block no: 1
             */
            left_addr = ema_eid_to_address(e_left);
            if(EME_IN_SAME_PAGE(e_left, eid))
            {                
                left_buf = targ_eme_buf;
                left_page = targ_eme_page;
            }
            else
            {
                left_buf = extent_readbuffer(rel, left_addr.physical_page_number, false);
                ExtentAssert(BufferIsValid(left_buf));
                LockBuffer(left_buf, BUFFER_LOCK_EXCLUSIVE);
                left_page = (Page)BufferGetPage(left_buf);
            }

            /*
             * lock buffer for target eme.
             * recheck whether the target eme is free.
             * registered block no: 2
             */
            LockBuffer(targ_eme_buf, BUFFER_LOCK_EXCLUSIVE);
            if(targ_eme_pg->ema[targ_eme_addr.local_idx].is_occupied != 0)
            {
                /* extent is occupied */
                UnlockReleaseBuffer(targ_eme_buf);
                if(!EME_IN_SAME_PAGE(e_left, eid))
                    UnlockReleaseBuffer(left_buf);
                UnlockReleaseBuffer(esa_buf);
                UnlockShard(rel, sid, ExclusiveLock);
                return false;
            }

            /*
             * if eid is at same page as e_left, targ_eme_buf will be transfered to left_buf for management.
             * we do like this is to ensure e_left is locked before locking buffer of target eme.
             */
            if(EME_IN_SAME_PAGE(e_left, eid))
            {
                targ_eme_buf = InvalidBuffer;
            }
            
            START_CRIT_SECTION();
            
            /*
             * update anchor
             */
            esa_pg = (ESAPage)PageGetContents(BufferGetPage(esa_buf));
            esa_pg->anchors[esa_addr.local_idx].alloc_head = eid;
            esa_pg->anchors[esa_addr.local_idx].alloc_tail = eid;
            if(!ExtentIdIsValid(e_right))
                esa_pg->anchors[esa_addr.local_idx].scan_tail  = eid;
    
            MarkBufferDirty(esa_buf);

            /*
             * update left EME
             */
            PAGE_SET_EME_SCAN_NEXT(left_page, left_addr.local_idx, eid);
            MarkBufferDirty(left_buf);

            /*
             * update target EME 
             */
            ExtentAssertEMEIsFree(targ_eme_pg->ema[targ_eme_addr.local_idx]);
            INIT_EME(&targ_eme_pg->ema[targ_eme_addr.local_idx]);
            targ_eme_pg->ema[targ_eme_addr.local_idx].is_occupied = 1;
            targ_eme_pg->ema[targ_eme_addr.local_idx].shardid = sid;
            targ_eme_pg->ema[targ_eme_addr.local_idx].max_freespace = MAX_FREESPACE;
            targ_eme_pg->ema[targ_eme_addr.local_idx].hwm = 1;
            targ_eme_pg->ema[targ_eme_addr.local_idx].scan_next = e_right;
            if(BufferIsValid(targ_eme_buf))
                MarkBufferDirty(targ_eme_buf);

            /* 
             * write xlog
             */
            XLogBeginInsert();
            /* write xlog of esa */
            XLogRegisterBuffer(0, esa_buf, REGBUF_KEEP_DATA);
            esa_xlrec.slot = esa_addr.local_idx;
            esa_xlrec.setflag = ESA_SETFLAG_ALLOC;
            esa_xlrec.anchor.alloc_head = eid;
            esa_xlrec.anchor.alloc_tail = eid;
            esa_xlrec.anchor.scan_head = InvalidExtentID;    
            esa_xlrec.anchor.scan_tail = InvalidExtentID;
            if(!ExtentIdIsValid(e_right))
            {
                esa_xlrec.setflag |= ESA_SETFLAG_SCANTAIL;
                esa_xlrec.anchor.scan_tail = eid;
            }
            XLogRegisterBufData(0, (char *)&esa_xlrec, SizeOfSetESA);

            /* write xlog for insert point */
            XLogRegisterBuffer(1, left_buf, REGBUF_KEEP_DATA);    
            left_xlrec.setflag = EMA_SETFLAG_SCANNEXT;
            left_xlrec.slot = left_addr.local_idx;
            INIT_EME(&left_xlrec.eme);
            left_xlrec.eme.scan_next = eid;
            XLogRegisterBufData(1, (char *)&left_xlrec, SizeOfSetEME);

            /* write xlog for target eme */
            eme_xlrec.setflag = EMA_SETFLAG_ALL;
            eme_xlrec.slot = targ_eme_addr.local_idx;
            INIT_EME(&eme_xlrec.eme);
            eme_xlrec.eme.is_occupied = 1;
            eme_xlrec.eme.shardid = sid;
            eme_xlrec.eme.max_freespace = MAX_FREESPACE;
            eme_xlrec.eme.hwm = 1;
            eme_xlrec.eme.scan_next = e_right;
            if(BufferIsValid(targ_eme_buf))
            {
                XLogRegisterBuffer(2, targ_eme_buf, REGBUF_KEEP_DATA);
                XLogRegisterBufData(2, (char *)&eme_xlrec, SizeOfSetEME);
            }
            else
            {
                XLogRegisterBufData(1, (char *)&eme_xlrec, SizeOfSetEME);
            }

            recptr = XLogInsert(RM_EXTENT_ID, XLOG_EXTENT_ATTACH_EXTENT);
            PageSetLSN(BufferGetPage(esa_buf), recptr);    
            PageSetLSN(BufferGetPage(left_buf), recptr);    
            if(BufferIsValid(targ_eme_buf))
                PageSetLSN(BufferGetPage(targ_eme_buf), recptr);;

            END_CRIT_SECTION();
            UnlockReleaseBuffer(esa_buf);
            UnlockReleaseBuffer(left_buf);
            if(BufferIsValid(targ_eme_buf))
                UnlockReleaseBuffer(targ_eme_buf);

            if(trace_extent)
            {
                ereport(LOG,
                    (errmsg("[trace extent]AttachExtent:[rel:%d/%d/%d][mode3:insert extent into "
                                                        "internal of scanlist and alloclist is null]"
                            "[sid:%d, eid:%d]"
                            "[setesa:blocknum=%d,offset=%d,allochead=%d,alloctail=%d]"
                            "[seteme:blocknum=%d,offset=%d,scannext=%d]"
                            "[seteme:blocknum=%d,offset=%d,sid=%d,scannext=%d,freespace=MAX_FREESPACE]",
                            rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
                            sid, eid,
                            esa_addr.physical_page_number, esa_addr.local_idx, eid, eid,
                            left_addr.physical_page_number, left_addr.local_idx, eid,
                            targ_eme_addr.physical_page_number, targ_eme_addr.local_idx, sid, e_right)));
            }
        }
    }
    else  /* anchor->alloc_head is valid */
    {
        /*
        * both two lists are not empty.
        */
        ExtentID scan_left = InvalidExtentID;
        ExtentID scan_right = InvalidExtentID;
        ExtentID alloc_left = InvalidExtentID;
        ExtentID alloc_right = InvalidExtentID;
        
        Buffer esa_buf = InvalidBuffer; 
        Buffer scan_left_buf = InvalidBuffer;
        Buffer alloc_left_buf = InvalidBuffer;
        ESAAddress esa_addr;
        EMAAddress scan_left_addr;
        EMAAddress alloc_left_addr;
        ESAPage esa_pg = NULL;
        Page    scan_left_page = NULL;
        Page    alloc_left_page = NULL;
        xl_extent_setesa esa_xlrec;
        xl_extent_seteme scan_left_xlrec;
        xl_extent_seteme alloc_left_xlrec;
        xl_extent_seteme targ_eme_xlrec;
        XLogRecPtr    recptr  = InvalidXLogRecPtr;

        INIT_EXLOG_SETESA(&esa_xlrec);
        INIT_EXLOG_SETEME(&scan_left_xlrec);
        INIT_EXLOG_SETEME(&alloc_left_xlrec);
        INIT_EXLOG_SETEME(&targ_eme_xlrec);

        esa_addr.physical_page_number = InvalidBlockNumber;
        esa_addr.local_idx = 0;
        scan_left_addr.physical_page_number = InvalidBlockNumber;
        scan_left_addr.local_idx = 0;
        alloc_left_addr.physical_page_number = InvalidBlockNumber;
        alloc_left_addr.local_idx = 0;
        
        if(!ExtentIdIsValid(anchor.scan_head))
        {
            /* 
             * alloc list is not empty, and scan list is empty
             * need to rebuild scan list 
             */
            UnlockShard(rel, sid, ExclusiveLock);
            elog(ERROR, "relation %d extent was corrupted.", RelationGetRelid(rel));
        }

        /* 
         * search insert point in scan list
         */
        if(eid > anchor.scan_head)
        {            
            scan_left = scan_right = anchor.scan_head;
            
            while(ExtentIdIsValid(scan_left))
            {
                scan_right = ema_next_scan(rel, scan_left, true, NULL, NULL, NULL, NULL);

                if(!ExtentIdIsValid(scan_right))
                    break;
                
                if(scan_left == eid)
                {
                    /* error: this free extent has been already added to scan list. */
                }
                else if(EME_IN_SAME_PAGE(scan_left, eid)
                    || (scan_left < eid && eid < scan_right))
                {
                    break;
                }                

                scan_left = scan_right;
            }
        }
        /*
         * search insert point in alloc list
         */
        if(eid > anchor.alloc_head)
        {
            alloc_left = alloc_right = anchor.alloc_head;

            while(ExtentIdIsValid(alloc_left))
            {
                alloc_right = ema_next_alloc(rel, alloc_left, true, NULL, NULL, NULL, NULL);

                if(!ExtentIdIsValid(alloc_right))
                    break;
                
                if(alloc_left == eid)
                {
                    /* error: this free extent has been already added to scan list. */
                }
                else if(EME_IN_SAME_PAGE(alloc_left, eid)
                    || (alloc_left < eid && eid < alloc_right))
                {
                    break;
                }                

                alloc_left = alloc_right;
            }
        }

        /*
         * get buffer for esa if need
         */
        if(!ExtentIdIsValid(scan_left) || !ExtentIdIsValid(scan_right) 
                || !ExtentIdIsValid(alloc_left) || !ExtentIdIsValid(alloc_right))
        {
            esa_addr = esa_sid_to_address(sid);
            esa_buf = extent_readbuffer(rel, esa_addr.physical_page_number, true);
            ExtentAssert(BufferIsValid(esa_buf));
            LockBuffer(esa_buf, BUFFER_LOCK_EXCLUSIVE);
            esa_pg = (ESAPage)PageGetContents(BufferGetPage(esa_buf));
        }

        /*
         * get buffer for alloc_left
         * registered block no : 1
         */
        if(!ExtentIdIsValid(alloc_left))
        {
            //only need to alloc head of esa and target eme.
            alloc_left_buf = InvalidBuffer;
            alloc_left_page = NULL;
        }
        else
        {
            alloc_left_addr = ema_eid_to_address(alloc_left);
            if(EME_IN_SAME_PAGE(alloc_left, eid))
            {                
                alloc_left_buf = targ_eme_buf;
                alloc_left_page = targ_eme_page;
            }
            else
            {
                alloc_left_buf = extent_readbuffer(rel, alloc_left_addr.physical_page_number, false);
                ExtentAssert(BufferIsValid(alloc_left_buf));
                LockBuffer(alloc_left_buf, BUFFER_LOCK_EXCLUSIVE);
                alloc_left_page = (Page)BufferGetPage(alloc_left_buf);
            }
        }
        
        /*
         * get buffer for scan_left
         * registered block no : 2
         */        
        if(ExtentIdIsValid(scan_left))
        {
            scan_left_addr = ema_eid_to_address(scan_left);
            if(ExtentIdIsValid(alloc_left) && EME_IN_SAME_PAGE(scan_left, alloc_left))
            {                
                scan_left_buf = InvalidBuffer;
                scan_left_page = alloc_left_page;
            }
            else if(EME_IN_SAME_PAGE(scan_left, eid))
            {        
                /* buffer of target eme is transfered to scan left */    
                scan_left_buf = targ_eme_buf;
                scan_left_page = targ_eme_page;    
            }        
            else
            {
                scan_left_buf = extent_readbuffer(rel, scan_left_addr.physical_page_number, false);
                ExtentAssert(BufferIsValid(scan_left_buf));
                LockBuffer(scan_left_buf, BUFFER_LOCK_EXCLUSIVE);
                scan_left_page = (Page)BufferGetPage(scan_left_buf);
            }
        }
        else
        {
            //only need to alloc head of esa and target eme.
            scan_left_buf = InvalidBuffer;
            scan_left_page = NULL;
        }

        /*
         * lock buffer for target eme.
         * recheck whether the target eme is free.
         * registered block no: 3
         */
        LockBuffer(targ_eme_buf, BUFFER_LOCK_EXCLUSIVE);
        if(targ_eme_pg->ema[targ_eme_addr.local_idx].is_occupied != 0)
        {
            /* extent is occupied */
            if(BufferIsValid(esa_buf))
                UnlockReleaseBuffer(esa_buf);
            if(BufferIsValid(alloc_left_buf))
                UnlockReleaseBuffer(alloc_left_buf);
            if(BufferIsValid(scan_left_buf))
                UnlockReleaseBuffer(scan_left_buf);
            if((ExtentIdIsValid(alloc_left) && EME_IN_SAME_PAGE(eid, alloc_left)) 
                 || (ExtentIdIsValid(scan_left) && EME_IN_SAME_PAGE(eid, scan_left)))
            {
                //nothing to do
            }
            else
            {
                UnlockReleaseBuffer(targ_eme_buf);
            }

            UnlockShard(rel, sid, ExclusiveLock);
            return false;
        }

        if( (ExtentIdIsValid(alloc_left) && EME_IN_SAME_PAGE(eid, alloc_left)) 
             || (ExtentIdIsValid(scan_left) && EME_IN_SAME_PAGE(eid, scan_left)))
        {
            targ_eme_buf = InvalidBuffer;
        }

        START_CRIT_SECTION();
        XLogBeginInsert();
        
        /*
         * update anchor
         */
        if(!ExtentIdIsValid(scan_left) || !ExtentIdIsValid(scan_right) 
                || !ExtentIdIsValid(alloc_left) || !ExtentIdIsValid(alloc_right))
        {
            XLogRegisterBuffer(0, esa_buf, REGBUF_KEEP_DATA);
            esa_xlrec.slot = esa_addr.local_idx;
            esa_xlrec.setflag = 0;
            INIT_ESA(&esa_xlrec.anchor);

            if(!ExtentIdIsValid(alloc_left))
            {
                /*
                 * insert new extent to the head of alloc list
                 */
                esa_xlrec.setflag |= ESA_SETFLAG_ALLOCHEAD;
                esa_xlrec.anchor.alloc_head = eid;
                esa_pg->anchors[esa_addr.local_idx].alloc_head = eid;
            }
            else if(!ExtentIdIsValid(alloc_right))
            {
                esa_xlrec.setflag |= ESA_SETFLAG_ALLOCTAIL;
                esa_xlrec.anchor.alloc_tail = eid;
                esa_pg->anchors[esa_addr.local_idx].alloc_tail = eid;
            }

            if(!ExtentIdIsValid(scan_left))
            {
                /*
                 * insert new extent to the head of scan list
                 */
                esa_xlrec.setflag |= ESA_SETFLAG_SCANHEAD;
                esa_xlrec.anchor.scan_head = eid;
                esa_pg->anchors[esa_addr.local_idx].scan_head = eid;
            }
            else if(!ExtentIdIsValid(scan_right))
            {
                /*
                 * append new extent to the tail of scan list
                 */
                esa_xlrec.setflag |= ESA_SETFLAG_SCANTAIL;
                esa_xlrec.anchor.scan_tail = eid;
                esa_pg->anchors[esa_addr.local_idx].scan_tail = eid;    
            }
            MarkBufferDirty(esa_buf);
            XLogRegisterBufData(0, (char *)&esa_xlrec, SizeOfSetESA);
        }

        /*
         * update alloc left eme
         */
        if(ExtentIdIsValid(alloc_left))
        {
            PAGE_SET_EME_ALLOC_NEXT(alloc_left_page, alloc_left_addr.local_idx, eid);
            alloc_left_xlrec.slot = alloc_left_addr.local_idx;
            alloc_left_xlrec.setflag = EMA_SETFLAG_ALLOCNEXT;
            INIT_EME(&alloc_left_xlrec.eme);
            alloc_left_xlrec.eme.alloc_next = eid;
            MarkBufferDirty(alloc_left_buf);

            XLogRegisterBuffer(1, alloc_left_buf, REGBUF_KEEP_DATA);
            XLogRegisterBufData(1, (char *)&alloc_left_xlrec, SizeOfSetEME);
        }
        
        /*
         * update scan left eme
         */
        if(ExtentIdIsValid(scan_left))
        {
            PAGE_SET_EME_SCAN_NEXT(scan_left_page, scan_left_addr.local_idx, eid);
            scan_left_xlrec.slot = scan_left_addr.local_idx;
            scan_left_xlrec.setflag = EMA_SETFLAG_SCANNEXT;
            INIT_EME(&scan_left_xlrec.eme);
            scan_left_xlrec.eme.scan_next = eid;

            if(ExtentIdIsValid(alloc_left) && EME_IN_SAME_PAGE(scan_left, alloc_left))
            {
                XLogRegisterBufData(1, (char *)&scan_left_xlrec, SizeOfSetEME);    
            }
            else 
            {
                MarkBufferDirty(scan_left_buf);
                XLogRegisterBuffer(2, scan_left_buf, REGBUF_KEEP_DATA);
                XLogRegisterBufData(2, (char *)&scan_left_xlrec, SizeOfSetEME);
            }
        }
        /*
         * update target eme
         */
        ExtentAssertEMEIsFree(targ_eme_pg->ema[targ_eme_addr.local_idx]); 
        INIT_EME(&targ_eme_pg->ema[targ_eme_addr.local_idx]);
        targ_eme_pg->ema[targ_eme_addr.local_idx].is_occupied = 1;
        targ_eme_pg->ema[targ_eme_addr.local_idx].shardid = sid;
        targ_eme_pg->ema[targ_eme_addr.local_idx].max_freespace = MAX_FREESPACE;
        targ_eme_pg->ema[targ_eme_addr.local_idx].hwm = 1;

        targ_eme_pg->ema[targ_eme_addr.local_idx].alloc_next = 
                        ExtentIdIsValid(alloc_left) ? alloc_right : anchor.alloc_head;

        targ_eme_pg->ema[targ_eme_addr.local_idx].scan_next = 
                        ExtentIdIsValid(scan_left) ? scan_right : anchor.scan_head;

        targ_eme_xlrec.setflag = EMA_SETFLAG_ALL;
        targ_eme_xlrec.slot = targ_eme_addr.local_idx;
        INIT_EME(&targ_eme_xlrec.eme);
        targ_eme_xlrec.eme.is_occupied = 1;
        targ_eme_xlrec.eme.shardid = sid;
        targ_eme_xlrec.eme.max_freespace = MAX_FREESPACE;
        targ_eme_xlrec.eme.hwm = 1;
        targ_eme_xlrec.eme.scan_next = ExtentIdIsValid(scan_left) ? scan_right : anchor.scan_head;
        targ_eme_xlrec.eme.alloc_next = ExtentIdIsValid(alloc_left) ? alloc_right : anchor.alloc_head;

        if(ExtentIdIsValid(alloc_left) && EME_IN_SAME_PAGE(eid, alloc_left))
        {
            XLogRegisterBufData(1, (char *)&targ_eme_xlrec, SizeOfSetEME);
        }
        else if(ExtentIdIsValid(scan_left) && EME_IN_SAME_PAGE(eid, scan_left))
        {
            XLogRegisterBufData(2, (char *)&targ_eme_xlrec, SizeOfSetEME);
        }
        else
        {        
            MarkBufferDirty(targ_eme_buf);
            XLogRegisterBuffer(3, targ_eme_buf, REGBUF_KEEP_DATA);
            XLogRegisterBufData(3, (char *)&targ_eme_xlrec, SizeOfSetEME);
        }
        
        /*
         * write xlog
         */
        recptr = XLogInsert(RM_EXTENT_ID, XLOG_EXTENT_ATTACH_EXTENT);
        if(BufferIsValid(esa_buf))
            PageSetLSN(BufferGetPage(esa_buf), recptr);            
        if(BufferIsValid(scan_left_buf))
            PageSetLSN(BufferGetPage(scan_left_buf), recptr);
        if(BufferIsValid(alloc_left_buf))
            PageSetLSN(BufferGetPage(alloc_left_buf), recptr);
        if(BufferIsValid(targ_eme_buf))
            PageSetLSN(BufferGetPage(targ_eme_buf), recptr);

        END_CRIT_SECTION();
        if(BufferIsValid(esa_buf))
            UnlockReleaseBuffer(esa_buf);
        if(BufferIsValid(scan_left_buf))
            UnlockReleaseBuffer(scan_left_buf);
        if(BufferIsValid(alloc_left_buf))
            UnlockReleaseBuffer(alloc_left_buf);
        if(BufferIsValid(targ_eme_buf))
            UnlockReleaseBuffer(targ_eme_buf);

        if(trace_extent)
        {
            StringInfoData esa_log;
            initStringInfo(&esa_log);

            /* 
             * log relation
             */
            appendStringInfo(&esa_log, "[trace extent]AttachExtent:[rel:%d/%d/%d][mode4:both scan list and alloclist are not empty]",
                                    rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode);
            /* 
             * log sid and eid 
             */
            appendStringInfo(&esa_log, "[sid:%d, eid:%d]", sid, eid);

            /* 
             * log esa 
             */
            appendStringInfo(&esa_log, "[setesa:blocknum=%d,offset=%d",
                                esa_addr.physical_page_number, esa_addr.local_idx);
            if(!ExtentIdIsValid(alloc_left))
                appendStringInfo(&esa_log, ",allochead=%d", eid);
            if(!ExtentIdIsValid(alloc_right))
                appendStringInfo(&esa_log, ",alloctail=%d", eid);
            if(!ExtentIdIsValid(scan_left))
                appendStringInfo(&esa_log, ",scanchead=%d", eid);
            if(!ExtentIdIsValid(scan_right))
                appendStringInfo(&esa_log, ",scantail=%d", eid);

            /* 
             *log scan extent 
             */
            if(ExtentIdIsValid(scan_left))
                appendStringInfo(&esa_log, "[setscaneme:blocknum=%d,offset=%d,scannext=%d]",
                                scan_left_addr.physical_page_number, scan_left_addr.local_idx,
                                eid);

            /* 
             * log alloc extent 
             */
            if(ExtentIdIsValid(alloc_left))
                appendStringInfo(&esa_log, "[setalloceme:blocknum=%d,offset=%d,allocnext=%d]",
                                alloc_left_addr.physical_page_number, alloc_left_addr.local_idx,
                                eid);

            /* 
             * log target extent
             */
            appendStringInfo(&esa_log, "[settargeteme:blocknum=%d,offset=%d,sid=%d,freespace=MAX_FREESPACE,"
                                        "scannext=%d, allocnext=%d]",
                                targ_eme_addr.physical_page_number, targ_eme_addr.local_idx, sid,
                                ExtentIdIsValid(scan_left) ? scan_right : anchor.scan_head,
                                ExtentIdIsValid(alloc_left) ? alloc_right : anchor.alloc_head);
            
            ereport(LOG, (errmsg("%s", esa_log.data)));
        }
    }

    UnlockShard(rel, sid, ExclusiveLock);

    return true;
}

void
shard_append_extent(Relation rel, ShardID sid, ExtentID eid, uint8 freespace, bool for_rebuild)
{// #lizard forgives
    EMAShardAnchor anchor;    
    XLogRecPtr    recptr = InvalidXLogRecPtr;

    EOBAddress    eob_addr;
    Buffer        eob_buf;
    
    xl_extent_extendeme xlrec_ex;
    xl_extent_extendeob xlrec_eob;

    INIT_EXLOG_EXTENDEOB(&xlrec_eob);
    INIT_EXLOG_EXTENDEME(&xlrec_ex);

    eob_addr = eob_eid_to_address(eid);
    
    LockShard(rel, sid, ExclusiveLock);
    /* 
     * add this extent to alloc list and scan list 
     * If system crash in this point, the free extent will be leaked.
     */
    anchor = esa_get_anchor(rel, sid);    

    if(!ExtentIdIsValid(anchor.scan_head) && !ExtentIdIsValid(anchor.alloc_head))
    {
        Buffer esa_buf;    
        Buffer eme_buf;
        ESAAddress esa_addr;
        EMAAddress eme_addr;
        xl_extent_setesa esa_xlrec;
        xl_extent_seteme eme_xlrec;

        INIT_EXLOG_SETESA(&esa_xlrec);
        INIT_EXLOG_SETEME(&eme_xlrec);

        /*
        * get buffer for eob
        * registered block no : 0
        */
        eob_buf = extent_readbuffer(rel, eob_addr.physical_page_number, false);
        ExtentAssert(BufferIsValid(eob_buf));
        LockBuffer(eob_buf, BUFFER_LOCK_EXCLUSIVE);
        
        /*
         * get buffer for esa 
         * registered block no : 1
         */
        esa_addr = esa_sid_to_address(sid);
        esa_buf = extent_readbuffer(rel, esa_addr.physical_page_number, false);
        ExtentAssert(BufferIsValid(esa_buf));
        LockBuffer(esa_buf, BUFFER_LOCK_EXCLUSIVE);

        /*
         * get buffer for target EME
         * registered block no : 2
         */
        eme_addr = ema_eid_to_address(eid);
        eme_buf = extent_readbuffer(rel, eme_addr.physical_page_number, false);
        ExtentAssert(BufferIsValid(eme_buf));
        LockBuffer(eme_buf, BUFFER_LOCK_EXCLUSIVE);
        
        START_CRIT_SECTION();

        /*
         * update eob
         */
        ExtentAssert(eob_addr.local_bms_offset == BufferGetEOBPage(eob_buf)->n_bits);
        if(eob_addr.local_bms_offset != BufferGetEOBPage(eob_buf)->n_bits)
        {
            elog(ERROR, "extend eob error, emes in eobpage:%d, extending eob local idx: %d",
                        BufferGetEOBPage(eob_buf)->n_bits,
                        eob_addr.local_bms_offset);
        }
        BufferGetEOBPage(eob_buf)->n_bits++;
        MarkBufferDirty(eob_buf);        
         
        /*
         * update anchor
         */
        esa_page_set_anchor(BufferGetPage(esa_buf),
                            esa_addr.local_idx,
                            ESA_SETFLAG_ALL,
                            eid,
                            eid,
                            eid,
                            eid);
        MarkBufferDirty(esa_buf);

        /*
         * update target EME 
         */
        ema_page_init_eme(BufferGetPage(eme_buf),
                            eme_addr.local_idx,
                            sid,
                            freespace);

        ExtentAssert(eme_addr.local_idx == BufferGetEMAPage(eme_buf)->n_emes);
        if(eme_addr.local_idx != BufferGetEMAPage(eme_buf)->n_emes)
        {
            elog(ERROR, "extend extent error, emes in esapage:%d, extending extent local idx: %d",
                        BufferGetEMAPage(eme_buf)->n_emes,
                        eme_addr.local_idx);
        }
        BufferGetEMAPage(eme_buf)->n_emes++;
        MarkBufferDirty(eme_buf);
        
        /*write xlog*/
        XLogBeginInsert();

        /* write eob xlog */        
        XLogRegisterBuffer(0, eob_buf, REGBUF_KEEP_DATA);
        xlrec_eob.slot = eob_addr.local_bms_offset;
        xlrec_eob.n_eobs = BufferGetEOBPage(eob_buf)->n_bits;
        xlrec_eob.flags = 0;
        xlrec_eob.setfree_start = -1;
        xlrec_eob.setfree_end = -1;
        
        XLogRegisterBufData(0, (char *)&xlrec_eob, SizeOfExtendEOB);

        /* write esa xlog */
        XLogRegisterBuffer(1, esa_buf, REGBUF_KEEP_DATA);
        esa_xlrec.slot = esa_addr.local_idx;
        esa_xlrec.setflag = ESA_SETFLAG_ALL;
        esa_xlrec.anchor.alloc_head = eid;
        esa_xlrec.anchor.alloc_tail = eid;
        esa_xlrec.anchor.scan_head = eid;
        esa_xlrec.anchor.scan_tail = eid;        
        XLogRegisterBufData(1, (char *)&esa_xlrec, SizeOfSetESA);

        /* write eme xlog */
        XLogRegisterBuffer(2, eme_buf, REGBUF_KEEP_DATA);    
        eme_xlrec.setflag = EMA_SETFLAG_ALL;
        if(!for_rebuild)
            eme_xlrec.setflag |= EMA_SETFLAG_EXTENDHEAP;
        eme_xlrec.extentid = eid;
        memcpy(&(eme_xlrec.rnode), &(rel->rd_node), sizeof(RelFileNode));
        eme_xlrec.slot = eme_addr.local_idx;
        INIT_EME(&eme_xlrec.eme);
        eme_xlrec.eme.is_occupied = 1;
        eme_xlrec.eme.shardid = sid;
        eme_xlrec.eme.max_freespace = freespace;
        eme_xlrec.eme.hwm = 1;
        XLogRegisterBufData(2, (char *)&eme_xlrec, SizeOfSetEME);
        
        /* eme page: n_emes */
        xlrec_ex.n_emes = BufferGetEMAPage(eme_buf)->n_emes;
        xlrec_ex.flags = 0;
        xlrec_ex.setfree_start = -1;
        xlrec_ex.setfree_end = -1;
        XLogRegisterBufData(2, (char *)&xlrec_ex, SizeOfExtendEME);
            
        recptr = XLogInsert(RM_EXTENT_ID, XLOG_EXTENT_APPEND_EXTENT);
        PageSetLSN(BufferGetPage(eob_buf),recptr);
        PageSetLSN(BufferGetPage(esa_buf), recptr);    
        PageSetLSN(BufferGetPage(eme_buf), recptr);    

        END_CRIT_SECTION();
        UnlockReleaseBuffer(eob_buf);
        UnlockReleaseBuffer(esa_buf);
        UnlockReleaseBuffer(eme_buf);

        if(trace_extent)
        {
            ereport(LOG,
                    (errmsg("[trace extent]AppendExtent:[rel:%d/%d/%d][mode:1:scanlist is empty]"
                            "[sid:%d, eid:%d, rebuild:%d]"
                            "[seteob:blocknum=%d,offset=%d]"
                            "[setesa:blocknum=%d,offset=%d,scanhead=%d,scantail=%d,allochead=%d,alloctail=%d]"
                            "[seteme:blocknum=%d,offset=%d,sid=%d,freespace=%d]"
                            "[setemepage:blocknum=%d, n_emes=%d]",
                            rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
                            sid, eid, for_rebuild,
                            eob_addr.physical_page_number, eob_addr.local_bms_offset,
                            esa_addr.physical_page_number, esa_addr.local_idx, eid, eid, eid, eid,
                            eme_addr.physical_page_number, eme_addr.local_idx, sid, freespace,
                            eme_addr.physical_page_number, xlrec_ex.n_emes)));
        }
    }
    else if(ExtentIdIsValid(anchor.scan_head) && !ExtentIdIsValid(anchor.alloc_head))
    {
        Buffer esa_buf = InvalidBuffer;
        Buffer ema_buf = InvalidBuffer;
        Buffer targ_eme_buf = InvalidBuffer;
        Buffer targ_eme_buf_idx = InvalidBuffer;
        ESAAddress esa_addr;
        EMAAddress eme_addr;
        EMAAddress targ_eme_addr;
        xl_extent_setesa esa_xlrec;
        xl_extent_seteme eme_xlrec;
        xl_extent_seteme targ_eme_xlrec;

        INIT_EXLOG_SETESA(&esa_xlrec);
        INIT_EXLOG_SETEME(&eme_xlrec);
        INIT_EXLOG_SETEME(&targ_eme_xlrec);

        /*
        * get buffer for eob
        * registered block no : 0
        */
        eob_buf = extent_readbuffer(rel, eob_addr.physical_page_number, false);
        ExtentAssert(BufferIsValid(eob_buf));
        LockBuffer(eob_buf, BUFFER_LOCK_EXCLUSIVE);
        
        /*
         * lock esa page 
         * registered block no: 1
         */
        esa_addr = esa_sid_to_address(sid);
        esa_buf = extent_readbuffer(rel, esa_addr.physical_page_number, false);
        ExtentAssert(BufferIsValid(esa_buf));
        LockBuffer(esa_buf, BUFFER_LOCK_EXCLUSIVE);

        /* 
         * lock eme page for scanlist
         * registered block no: 2
         */
        eme_addr = ema_eid_to_address(anchor.scan_tail);
        ema_buf = extent_readbuffer(rel, eme_addr.physical_page_number, false);
        ExtentAssert(BufferIsValid(esa_buf));
        LockBuffer(ema_buf, BUFFER_LOCK_EXCLUSIVE);

        /*
         * lock eme page for target eme if need
         * registered block no: 3
         */
        targ_eme_addr = ema_eid_to_address(eid);
        if(!EME_IN_SAME_PAGE(anchor.scan_tail, eid))
        {            
            targ_eme_buf = extent_readbuffer(rel, targ_eme_addr.physical_page_number, false);
            targ_eme_buf_idx = targ_eme_buf;
            ExtentAssert(BufferIsValid(targ_eme_buf));
            LockBuffer(targ_eme_buf, BUFFER_LOCK_EXCLUSIVE);
        }
        else
        {
            targ_eme_buf_idx = ema_buf;
        }
        
        START_CRIT_SECTION();

        /*
         * update eob
         */
        ExtentAssert(eob_addr.local_bms_offset == BufferGetEOBPage(eob_buf)->n_bits);
        if(eob_addr.local_bms_offset != BufferGetEOBPage(eob_buf)->n_bits)
        {
            elog(ERROR, "extend eob error, emes in eobpage:%d, extending eob local idx: %d",
                        BufferGetEOBPage(eob_buf)->n_bits,
                        eob_addr.local_bms_offset);
        }
        BufferGetEOBPage(eob_buf)->n_bits++;
        MarkBufferDirty(eob_buf);        
        
        /* 
         * update esa page 
         */
        PAGE_SET_ESA_ALLOC(BufferGetPage(esa_buf),
                            esa_addr.local_idx,
                            eid, eid);
        PAGE_SET_ESA_SCAN_TAIL(BufferGetPage(esa_buf), esa_addr.local_idx, eid);
        MarkBufferDirty(esa_buf);        
        
        /* 
         * update eme page 
         */
        PAGE_SET_EME_SCAN_NEXT(BufferGetPage(ema_buf), eme_addr.local_idx, eid);
        MarkBufferDirty(ema_buf);

        /*
         * update target eme
         */
        ExtentAssertEMEIsFree(BufferGetEMAPage(targ_eme_buf_idx)->ema[targ_eme_addr.local_idx]);
        ema_page_init_eme(BufferGetPage(targ_eme_buf_idx),
                            targ_eme_addr.local_idx,
                            sid,
                            freespace);
        
        ExtentAssert(targ_eme_addr.local_idx == BufferGetEMAPage(targ_eme_buf_idx)->n_emes);
        if(targ_eme_addr.local_idx != BufferGetEMAPage(targ_eme_buf_idx)->n_emes)
        {
            elog(ERROR, "extend extent error, emes in esapage:%d, extending extent local idx: %d",
                        BufferGetEMAPage(targ_eme_buf_idx)->n_emes,
                        targ_eme_addr.local_idx);
        }
        BufferGetEMAPage(targ_eme_buf_idx)->n_emes++;
        MarkBufferDirty(targ_eme_buf_idx);

        /*
        * write xlog for eob
        */    
        XLogRegisterBuffer(0, eob_buf, REGBUF_KEEP_DATA);
        xlrec_eob.slot = eob_addr.local_bms_offset;
        xlrec_eob.n_eobs = BufferGetEOBPage(eob_buf)->n_bits;
        xlrec_eob.flags = 0;
        xlrec_eob.setfree_start = -1;
        xlrec_eob.setfree_end = -1;
        XLogRegisterBufData(0, (char *)&xlrec_eob, SizeOfExtendEOB);
        
        /*
         * write xlog for esa
         */
        XLogBeginInsert();
        XLogRegisterBuffer(1, esa_buf, REGBUF_KEEP_DATA);
        esa_xlrec.slot = esa_addr.local_idx;
        esa_xlrec.setflag = ESA_SETFLAG_ALLOC | ESA_SETFLAG_SCANTAIL;
        esa_xlrec.anchor.alloc_head = eid;
        esa_xlrec.anchor.alloc_tail = eid;
        esa_xlrec.anchor.scan_tail = eid;        
        XLogRegisterBufData(1, (char *)&esa_xlrec, SizeOfSetESA);
        
        /*
         * write xlog for tail eme
         */
        XLogRegisterBuffer(2, ema_buf, REGBUF_KEEP_DATA);
        eme_xlrec.slot = eme_addr.local_idx;
        eme_xlrec.setflag = EMA_SETFLAG_SCANNEXT;
        INIT_EME(&eme_xlrec.eme);
        eme_xlrec.eme.scan_next = eid;
        XLogRegisterBufData(2, (char *)&eme_xlrec, SizeOfSetEME);

        /*
         * write xlog for target eme
         */
        targ_eme_xlrec.setflag = EMA_SETFLAG_ALL;    
        if(!for_rebuild)
            targ_eme_xlrec.setflag |= EMA_SETFLAG_EXTENDHEAP;
        targ_eme_xlrec.extentid = eid;
        memcpy(&(targ_eme_xlrec.rnode), &(rel->rd_node), sizeof(RelFileNode));
        targ_eme_xlrec.slot = targ_eme_addr.local_idx;
        INIT_EME(&targ_eme_xlrec.eme);
        targ_eme_xlrec.eme.is_occupied = 1;
        targ_eme_xlrec.eme.shardid = sid;
        targ_eme_xlrec.eme.max_freespace = freespace;
        targ_eme_xlrec.eme.hwm = 1;

        /* eme page: n_emes */
        xlrec_ex.n_emes = BufferGetEMAPage(targ_eme_buf_idx)->n_emes;
        xlrec_ex.flags = 0;
        xlrec_ex.setfree_start = -1;
        xlrec_ex.setfree_end = -1;
        
        if(!EME_IN_SAME_PAGE(anchor.scan_tail, eid))
        {
            XLogRegisterBuffer(3, targ_eme_buf, REGBUF_KEEP_DATA);
            XLogRegisterBufData(3, (char *)&xlrec_ex, SizeOfExtendEME);
            XLogRegisterBufData(3, (char *)&targ_eme_xlrec, SizeOfSetEME);            
        }            
        else
        {
            XLogRegisterBufData(2, (char *)&xlrec_ex, SizeOfExtendEME);
            XLogRegisterBufData(2, (char *)&targ_eme_xlrec, SizeOfSetEME);            
        }
        
        recptr = XLogInsert(RM_EXTENT_ID, XLOG_EXTENT_APPEND_EXTENT);
        PageSetLSN(BufferGetPage(eob_buf), recptr);    
        PageSetLSN(BufferGetPage(esa_buf), recptr);    
        PageSetLSN(BufferGetPage(ema_buf), recptr);
        if(BufferIsValid(targ_eme_buf))
            PageSetLSN(BufferGetPage(targ_eme_buf), recptr);

        END_CRIT_SECTION();
        UnlockReleaseBuffer(eob_buf);
        UnlockReleaseBuffer(esa_buf);
        UnlockReleaseBuffer(ema_buf);
        if(BufferIsValid(targ_eme_buf))
            UnlockReleaseBuffer(targ_eme_buf);

        if(trace_extent)
        {
            ereport(LOG,
                    (errmsg("[trace extent]AppendExtent:[rel:%d/%d/%d][mode:2:scanlist is not empty, alloclist is empty]"
                            "[sid:%d, eid:%d, rebuild:%d]"
                            "[seteob:blocknum=%d,offset=%d]"
                            "[setesa:blocknum=%d,offset=%d,scantail=%d,allochead=%d,alloctail=%d]"
                            "[seteme:blocknum=%d,offset=%d,eid=%d,scannext=%d]"
                            "[seteme:blocknum=%d,offset=%d,sid=%d,freespace=%d]"
                            "[setemepage:blocknum=%d, n_emes=%d]",
                            rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
                            sid, eid, for_rebuild,
                            eob_addr.physical_page_number, eob_addr.local_bms_offset,
                            esa_addr.physical_page_number, esa_addr.local_idx, eid, eid, eid,
                            eme_addr.physical_page_number, eme_addr.local_idx, anchor.scan_tail, eid,
                            targ_eme_addr.physical_page_number, targ_eme_addr.local_idx, sid, freespace,
                            targ_eme_addr.physical_page_number, xlrec_ex.n_emes)));
        }
    }
    else if(ExtentIdIsValid(anchor.scan_head) && ExtentIdIsValid(anchor.alloc_head))
    {
        Buffer esa_buf = InvalidBuffer;
        Buffer ema1_buf = InvalidBuffer; 
        Buffer ema2_buf = InvalidBuffer;
        Buffer targ_eme_buf = InvalidBuffer;
        Buffer targ_eme_buf_idx = InvalidBuffer;
        ESAAddress esa_addr;
        EMAAddress eme1_addr, eme2_addr, targ_eme_addr;
        xl_extent_setesa esa_xlrec;
        xl_extent_seteme eme1_xlrec;
        xl_extent_seteme eme2_xlrec;
        xl_extent_seteme targ_eme_xlrec;
        int        targ_eme_pg_src = 0; /* 2: same as ema1_buf, 3: same as ema2_buf, 4: independent page */

        XLogEnsureRecordSpace(5, 0);
        
        INIT_EXLOG_SETESA(&esa_xlrec);
        INIT_EXLOG_SETEME(&eme1_xlrec);
        INIT_EXLOG_SETEME(&eme2_xlrec);
        INIT_EXLOG_SETEME(&targ_eme_xlrec);

        /*
        * get buffer for eob
        * registered block no : 0
        */
        eob_buf = extent_readbuffer(rel, eob_addr.physical_page_number, false);
        ExtentAssert(BufferIsValid(eob_buf));
        LockBuffer(eob_buf, BUFFER_LOCK_EXCLUSIVE);
        
        /*
         * lock esa page 
         * registered block no: 1
         */
        esa_addr = esa_sid_to_address(sid);
        esa_buf = extent_readbuffer(rel, esa_addr.physical_page_number, false);
        ExtentAssert(BufferIsValid(esa_buf));
        LockBuffer(esa_buf, BUFFER_LOCK_EXCLUSIVE);

        /* 
         * lock page for alloclist
         * registered block no: 2
         */
        eme1_addr = ema_eid_to_address(anchor.alloc_tail);
        ema1_buf = extent_readbuffer(rel, eme1_addr.physical_page_number, false);
        ExtentAssert(BufferIsValid(ema1_buf));
        LockBuffer(ema1_buf, BUFFER_LOCK_EXCLUSIVE);

        /*
         * lock page for scanlist
         * registered block no: 3
         */
        eme2_addr = ema_eid_to_address(anchor.scan_tail);
        if(!EME_IN_SAME_PAGE(anchor.scan_tail, anchor.alloc_tail))
        {            
            ema2_buf = extent_readbuffer(rel, eme2_addr.physical_page_number, false);
            ExtentAssert(BufferIsValid(ema2_buf));
            LockBuffer(ema2_buf, BUFFER_LOCK_EXCLUSIVE);
        }

        /*
         * lock page for target eme
         * registered block no: 4
         */
        targ_eme_addr = ema_eid_to_address(eid);
        if(!EME_IN_SAME_PAGE(anchor.alloc_tail, eid) && !EME_IN_SAME_PAGE(anchor.scan_tail, eid))
        {
            targ_eme_buf = extent_readbuffer(rel, targ_eme_addr.physical_page_number, false);
            ExtentAssert(BufferIsValid(targ_eme_buf));
            LockBuffer(targ_eme_buf, BUFFER_LOCK_EXCLUSIVE);
        }
        
        START_CRIT_SECTION();
        XLogBeginInsert();

        /*
         * update eob
         */
        ExtentAssert(eob_addr.local_bms_offset == BufferGetEOBPage(eob_buf)->n_bits);
        if(eob_addr.local_bms_offset != BufferGetEOBPage(eob_buf)->n_bits)
        {
            elog(ERROR, "extend eob error, emes in eobpage:%d, extending eob local idx: %d",
                        BufferGetEOBPage(eob_buf)->n_bits,
                        eob_addr.local_bms_offset);
        }
        BufferGetEOBPage(eob_buf)->n_bits++;
        MarkBufferDirty(eob_buf);    
        XLogRegisterBuffer(0, eob_buf, REGBUF_KEEP_DATA);
        xlrec_eob.slot = eob_addr.local_bms_offset;
        xlrec_eob.n_eobs = BufferGetEOBPage(eob_buf)->n_bits;
        xlrec_eob.flags = 0;
        xlrec_eob.setfree_start = -1;
        xlrec_eob.setfree_end = -1;
        XLogRegisterBufData(0, (char *)&xlrec_eob, SizeOfExtendEOB);
        
        /* 
         * update esa page 
         */
        PAGE_SET_ESA_ALLOC_TAIL(BufferGetPage(esa_buf), esa_addr.local_idx, eid);
        PAGE_SET_ESA_SCAN_TAIL(BufferGetPage(esa_buf), esa_addr.local_idx, eid);
        MarkBufferDirty(esa_buf);    
        XLogRegisterBuffer(1, esa_buf, REGBUF_KEEP_DATA);
        esa_xlrec.slot = esa_addr.local_idx;
        esa_xlrec.setflag = ESA_SETFLAG_ALLOCTAIL | ESA_SETFLAG_SCANTAIL;
        INIT_ESA(&esa_xlrec.anchor);
        esa_xlrec.anchor.alloc_tail = eid;
        esa_xlrec.anchor.scan_tail = eid;        
        XLogRegisterBufData(1, (char *)&esa_xlrec, SizeOfSetESA);
        
        /* 
         * update eme page 
         */
        if(anchor.alloc_tail == anchor.scan_tail)
        {
            /*
             * alloc tail is same as scan tail.
             */
            PAGE_SET_EME_SCAN_NEXT(BufferGetPage(ema1_buf), eme1_addr.local_idx, eid);
            PAGE_SET_EME_ALLOC_NEXT(BufferGetPage(ema1_buf), eme1_addr.local_idx, eid);
            MarkBufferDirty(ema1_buf);

            XLogRegisterBuffer(2, ema1_buf, REGBUF_KEEP_DATA);
            eme1_xlrec.slot = eme1_addr.local_idx;
            eme1_xlrec.setflag = EMA_SETFLAG_SCANNEXT | EMA_SETFLAG_ALLOCNEXT;
            INIT_EME(&eme1_xlrec.eme);
            eme1_xlrec.eme.scan_next = eid;
            eme1_xlrec.eme.alloc_next = eid;
            XLogRegisterBufData(2, (char *)&eme1_xlrec, SizeOfSetEME);
        }
        else if(EME_IN_SAME_PAGE(anchor.alloc_tail, anchor.scan_tail))
        {
            /*
             * both alloc tail and scan tail are in same page, but they are not equal.
             */
            PAGE_SET_EME_ALLOC_NEXT(BufferGetPage(ema1_buf), eme1_addr.local_idx, eid);
            PAGE_SET_EME_SCAN_NEXT(BufferGetPage(ema1_buf), eme2_addr.local_idx, eid);
            MarkBufferDirty(ema1_buf);

            XLogRegisterBuffer(2, ema1_buf, REGBUF_KEEP_DATA);
            eme1_xlrec.slot = eme1_addr.local_idx;
            eme1_xlrec.setflag = EMA_SETFLAG_ALLOCNEXT;
            INIT_EME(&eme1_xlrec.eme);
            eme1_xlrec.eme.alloc_next = eid;
            XLogRegisterBufData(2, (char *)&eme1_xlrec, SizeOfSetEME);

            eme2_xlrec.slot = eme2_addr.local_idx;
            eme2_xlrec.setflag = EMA_SETFLAG_SCANNEXT;
            INIT_EME(&eme2_xlrec.eme);
            eme2_xlrec.eme.scan_next = eid;
            XLogRegisterBufData(2, (char *)&eme2_xlrec, SizeOfSetEME);
        }
        else
        {
            /*
             * alloc tail and scan tail are not in same page.
             */
            PAGE_SET_EME_ALLOC_NEXT(BufferGetPage(ema1_buf), eme1_addr.local_idx, eid);
            PAGE_SET_EME_SCAN_NEXT(BufferGetPage(ema2_buf), eme2_addr.local_idx, eid);
            MarkBufferDirty(ema1_buf);
            MarkBufferDirty(ema2_buf);

            XLogRegisterBuffer(2, ema1_buf, REGBUF_KEEP_DATA);
            eme1_xlrec.slot = eme1_addr.local_idx;
            eme1_xlrec.setflag = EMA_SETFLAG_ALLOCNEXT;
            INIT_EME(&eme1_xlrec.eme);
            eme1_xlrec.eme.alloc_next = eid;
            XLogRegisterBufData(2, (char *)&eme1_xlrec, SizeOfSetEME);

            XLogRegisterBuffer(3, ema2_buf, REGBUF_KEEP_DATA);
            eme2_xlrec.slot = eme2_addr.local_idx;
            eme2_xlrec.setflag = EMA_SETFLAG_SCANNEXT;
            INIT_EME(&eme2_xlrec.eme);
            eme2_xlrec.eme.scan_next = eid;
            XLogRegisterBufData(3, (char *)&eme2_xlrec, SizeOfSetEME);
        }

        /* 
         * update target eme 
         */
        if(EME_IN_SAME_PAGE(anchor.alloc_tail, eid))
        {
            targ_eme_pg_src = 2;
            targ_eme_buf_idx = ema1_buf;
        }
        else if(BufferIsValid(ema2_buf) && EME_IN_SAME_PAGE(anchor.scan_tail, eid))
        {
            targ_eme_pg_src = 3;
            targ_eme_buf_idx = ema2_buf;
        }
        else
        {
            XLogRegisterBuffer(4, targ_eme_buf, REGBUF_KEEP_DATA);
            targ_eme_pg_src = 4;
            targ_eme_buf_idx = targ_eme_buf;
        }

        ExtentAssertEMEIsFree(BufferGetEMAPage(targ_eme_buf_idx)->ema[targ_eme_addr.local_idx]);
        ema_page_init_eme(BufferGetPage(targ_eme_buf_idx),
                                targ_eme_addr.local_idx,
                                sid,
                                freespace);

        ExtentAssert(targ_eme_addr.local_idx == BufferGetEMAPage(targ_eme_buf_idx)->n_emes);
        if(targ_eme_addr.local_idx != BufferGetEMAPage(targ_eme_buf_idx)->n_emes)
        {
            elog(ERROR, "extend extent error, emes in esapage:%d, extending extent local idx: %d",
                        BufferGetEMAPage(targ_eme_buf_idx)->n_emes,
                        targ_eme_addr.local_idx);
        }
        BufferGetEMAPage(targ_eme_buf_idx)->n_emes++;
        MarkBufferDirty(targ_eme_buf_idx);

        /* 
         * write xlog for target eme 
         */
        INIT_EME(&targ_eme_xlrec.eme);
        targ_eme_xlrec.setflag = EMA_SETFLAG_ALL;    
        if(!for_rebuild)
            targ_eme_xlrec.setflag |= EMA_SETFLAG_EXTENDHEAP;
        targ_eme_xlrec.extentid = eid;
        memcpy(&(targ_eme_xlrec.rnode), &(rel->rd_node), sizeof(RelFileNode));
        targ_eme_xlrec.slot = targ_eme_addr.local_idx;
        targ_eme_xlrec.eme.is_occupied = 1;
        targ_eme_xlrec.eme.shardid = sid;
        targ_eme_xlrec.eme.max_freespace = freespace;
        targ_eme_xlrec.eme.hwm = 1;
        XLogRegisterBufData(targ_eme_pg_src, (char *)&targ_eme_xlrec, SizeOfSetEME);

        /* eme page: n_emes */
        xlrec_ex.n_emes = targ_eme_addr.local_idx + 1;
        xlrec_ex.flags = 0;
        xlrec_ex.setfree_start = -1;
        xlrec_ex.setfree_end = -1;
        
        XLogRegisterBufData(targ_eme_pg_src, (char *)&xlrec_ex, SizeOfExtendEME);
        
        recptr = XLogInsert(RM_EXTENT_ID, XLOG_EXTENT_APPEND_EXTENT);
        PageSetLSN(BufferGetPage(eob_buf), recptr);    
        PageSetLSN(BufferGetPage(esa_buf), recptr);    
        PageSetLSN(BufferGetPage(ema1_buf), recptr);
        if(BufferIsValid(ema2_buf))
            PageSetLSN(BufferGetPage(ema2_buf), recptr);
        if(BufferIsValid(targ_eme_buf))
            PageSetLSN(BufferGetPage(targ_eme_buf), recptr);
        
        END_CRIT_SECTION();
        UnlockReleaseBuffer(eob_buf);
        UnlockReleaseBuffer(esa_buf);
        UnlockReleaseBuffer(ema1_buf);
        if(BufferIsValid(ema2_buf))
            UnlockReleaseBuffer(ema2_buf);
        if(BufferIsValid(targ_eme_buf))
            UnlockReleaseBuffer(targ_eme_buf);

        if(trace_extent)
        {
            ereport(LOG,
                (errmsg("[trace extent]AppendExtent:[rel:%d/%d/%d][mode:3:both scanlist and alloclist are not empty]"
                        "[sid:%d, eid:%d, rebuild:%d]"
                        "[seteob:blocknum=%d,offset=%d]"
                        "[setesa:blocknum=%d,offset=%d,scantail=%d,alloctail=%d]"
                        "[seteme1:blocknum=%d,offset=%d,eid=%d,allocnext=%d]"
                        "[seteme2:blocknum=%d,offset=%d,eid=%d,scannext=%d]"
                        "[seteme:blocknum=%d,offset=%d,sid=%d,freespace=%d]"
                        "[setemepage:blocknum=%d, n_emes=%d]",
                        rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
                        sid, eid, for_rebuild,
                        eob_addr.physical_page_number, eob_addr.local_bms_offset,
                        esa_addr.physical_page_number, esa_addr.local_idx, eid, eid,
                        eme1_addr.physical_page_number, eme1_addr.local_idx, anchor.alloc_tail, eid,
                        eme2_addr.physical_page_number, eme2_addr.local_idx, anchor.scan_tail, eid,
                        targ_eme_addr.physical_page_number, targ_eme_addr.local_idx, sid, freespace,
                        targ_eme_addr.physical_page_number, xlrec_ex.n_emes)));
        }
    }
    else
    {
        /*
         * can not go here.
         */
    }

    UnlockShard(rel, sid, ExclusiveLock);

    //XLogFlush(recptr);
}

/*
 * Only append a extent to scanlist of one shard.
 * this function is used by RebuildExtent.
 */
void
shard_append_extent_onlyscan(Relation rel, ShardID sid, ExtentID eid, uint8 freespace)
{// #lizard forgives
    EMAShardAnchor anchor;

    EOBAddress    eob_addr;
    Buffer        eob_buf;
    
    xl_extent_extendeme xlrec_ex;
    xl_extent_extendeob xlrec_eob;

    INIT_EXLOG_EXTENDEOB(&xlrec_eob);
    INIT_EXLOG_EXTENDEME(&xlrec_ex);

    eob_addr = eob_eid_to_address(eid);
    
    LockShard(rel, sid, ExclusiveLock);
    /* 
     * add this extent to alloc list and scan list 
     * If system crash in this point, the free extent will be leaked.
     */
    anchor = esa_get_anchor(rel, sid);    

    if(!ExtentIdIsValid(anchor.scan_head))
    {
        Buffer esa_buf; 
        Buffer eme_buf;
        ESAAddress esa_addr;
        EMAAddress eme_addr;
        xl_extent_setesa esa_xlrec;
        xl_extent_seteme eme_xlrec;
        XLogRecPtr    recptr;

        INIT_EXLOG_SETEME(&eme_xlrec);
        INIT_EXLOG_SETESA(&esa_xlrec);

        /*
        * get buffer for eob
        * registered block no : 0
        */
        eob_buf = extent_readbuffer(rel, eob_addr.physical_page_number, false);
        ExtentAssert(BufferIsValid(eob_buf));
        LockBuffer(eob_buf,BUFFER_LOCK_EXCLUSIVE);
        
        /*
         * get buffer for esa 
         * registered block no : 1
         */
        esa_addr = esa_sid_to_address(sid);
        esa_buf = extent_readbuffer(rel,  esa_addr.physical_page_number, false);
        ExtentAssert( BufferIsValid(esa_buf));
        LockBuffer(esa_buf,  BUFFER_LOCK_EXCLUSIVE);

        /*
         * get buffer for target EME
         * registered block no : 2
         */
        eme_addr = ema_eid_to_address(eid);
        eme_buf = extent_readbuffer( rel, eme_addr.physical_page_number, false);
        ExtentAssert( BufferIsValid(eme_buf) );
        LockBuffer(eme_buf, BUFFER_LOCK_EXCLUSIVE);
        
        START_CRIT_SECTION();
        /*
         * update eob
         */
        ExtentAssert(eob_addr.local_bms_offset == BufferGetEOBPage(eob_buf)->n_bits);
        if(eob_addr.local_bms_offset != (BufferGetEOBPage(eob_buf)->n_bits))
        {
            elog(ERROR, "extend eob error, emes in eobpage:%d,  extending eob local idx: %d",
                        BufferGetEOBPage(eob_buf)->n_bits,
                        eob_addr.local_bms_offset);
        }
        BufferGetEOBPage(eob_buf)->n_bits++;
        MarkBufferDirty(eob_buf);                

        /*
         * update anchor
         */
        PAGE_SET_ESA_SCAN(BufferGetPage(esa_buf),esa_addr.local_idx, eid, eid);

        MarkBufferDirty(esa_buf);

        /*
         * update target EME 
         */
        ema_page_init_eme(  BufferGetPage(eme_buf),
                            eme_addr.local_idx,
                            sid,
                            freespace);
        ExtentAssert( eme_addr.local_idx == BufferGetEMAPage(eme_buf)->n_emes );
        if(eme_addr.local_idx != BufferGetEMAPage(eme_buf)->n_emes)
        {
            elog(ERROR, "extend extent error, emes in esapage:%d, extending extent local idx: %d",
                      BufferGetEMAPage(eme_buf)->n_emes,
                      eme_addr.local_idx);
        }
        BufferGetEMAPage(eme_buf)->n_emes++;
        MarkBufferDirty(eme_buf);
        
        /*write xlog*/
        XLogBeginInsert();

        /* write eob xlog */        
        XLogRegisterBuffer(0,eob_buf, REGBUF_KEEP_DATA);
        xlrec_eob.slot = eob_addr.local_bms_offset;
        xlrec_eob.n_eobs = BufferGetEOBPage(eob_buf)->n_bits;
        xlrec_eob.setfree_start = -1;
        xlrec_eob.setfree_end = -1;
        xlrec_eob.flags = 0;
        XLogRegisterBufData(0, (char *)&xlrec_eob, SizeOfExtendEOB);

        /* write esa xlog */
        XLogRegisterBuffer(1, esa_buf, REGBUF_KEEP_DATA);
        esa_xlrec.setflag = ESA_SETFLAG_SCAN;
        esa_xlrec.slot = esa_addr.local_idx;
        esa_xlrec.anchor.alloc_head = InvalidExtentID;
        esa_xlrec.anchor.alloc_tail = InvalidExtentID;
        esa_xlrec.anchor.scan_head = eid;
        esa_xlrec.anchor.scan_tail = eid;        
        XLogRegisterBufData(1, (char *)&esa_xlrec, SizeOfSetESA);

        /* write eme xlog */
        XLogRegisterBuffer(2, eme_buf, REGBUF_KEEP_DATA);    
        eme_xlrec.setflag = EMA_SETFLAG_ALL;
        eme_xlrec.slot = eme_addr.local_idx;
        INIT_EME( &eme_xlrec.eme );
        eme_xlrec.eme.max_freespace = freespace;
        eme_xlrec.eme.hwm  =  1;
        eme_xlrec.eme.is_occupied  =  1;
        eme_xlrec.eme.shardid = sid;
        XLogRegisterBufData(2, (char *)&eme_xlrec, SizeOfSetEME);

        /* eme page: n_emes */
        xlrec_ex.n_emes = BufferGetEMAPage(eme_buf)->n_emes;
        xlrec_ex.setfree_start = -1;
        xlrec_ex.setfree_end = -1;
        xlrec_ex.flags = 0;
        XLogRegisterBufData(2, (char *)&xlrec_ex, SizeOfExtendEME);
        
        recptr = XLogInsert(RM_EXTENT_ID,  XLOG_EXTENT_APPEND_EXTENT);
        PageSetLSN(BufferGetPage(eob_buf), recptr); 
        PageSetLSN(BufferGetPage(esa_buf), recptr); 
        PageSetLSN(BufferGetPage(eme_buf), recptr); 

        END_CRIT_SECTION();
        UnlockReleaseBuffer(eob_buf );
        UnlockReleaseBuffer(esa_buf );
        UnlockReleaseBuffer(eme_buf );

        if(trace_extent)
        {
            ereport(LOG,
              (errmsg("[trace extent]AppendExtentOnlyScan:[rel:%d/%d/%d][mode:1:scanlist is empty]"
                        "[sid:%d, eid:%d]"
                        "[seteob:blocknum=%d,offset=%d]"
                        "[setesa:blocknum=%d,offset=%d,scanhead=%d,scantail=%d]"
                        "[seteme:blocknum=%d,offset=%d,sid=%d,is_occupied=%d,freespace=%d,allpointer set to INVALIDEID]"
                        "[setemepage:blocknum=%d, n_emes=%d]",
                        rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
                        sid, eid,
                        eob_addr.physical_page_number, eob_addr.local_bms_offset,
                        esa_addr.physical_page_number, esa_addr.local_idx, eid, eid,
                        eme_addr.physical_page_number, eme_addr.local_idx, sid, eme_xlrec.eme.is_occupied, freespace,
                        eme_addr.physical_page_number, xlrec_ex.n_emes)));
        }
    }
    else
    {
        Buffer esa_buf, ema_buf, targ_eme_buf;    
        ESAAddress esa_addr;
        EMAAddress eme_addr;
        EMAAddress targ_eme_addr;
        xl_extent_setesa esa_xlrec;
        xl_extent_seteme eme_xlrec;
        xl_extent_seteme targ_eme_xlrec;
        XLogRecPtr    recptr;

        INIT_EXLOG_SETESA(&esa_xlrec);
        INIT_EXLOG_SETEME(&eme_xlrec);
        INIT_EXLOG_SETEME(&targ_eme_xlrec);

        /*
        * get buffer for eob
        * registered block no : 0
        */
        eob_buf = extent_readbuffer(rel, eob_addr.physical_page_number, false);
        ExtentAssert(BufferIsValid(eob_buf));
        LockBuffer(eob_buf, BUFFER_LOCK_EXCLUSIVE);
        
        /*
         * lock esa page 
         * registered block no: 1
         */
        esa_addr = esa_sid_to_address(sid);
        esa_buf = extent_readbuffer(rel, esa_addr.physical_page_number, false);
        ExtentAssert(BufferIsValid(esa_buf));
        LockBuffer(esa_buf, BUFFER_LOCK_EXCLUSIVE);

        /* 
         * lock eme page for scanlist
         * registered block no: 2
         */
        eme_addr = ema_eid_to_address(anchor.scan_tail);
        ema_buf = extent_readbuffer(rel, eme_addr.physical_page_number, false);
        ExtentAssert(BufferIsValid(esa_buf));
        LockBuffer(ema_buf, BUFFER_LOCK_EXCLUSIVE);

        /*
         * lock eme page for target eme if need
         * registered block no: 3
         */
        targ_eme_addr = ema_eid_to_address(eid);
        if(!EME_IN_SAME_PAGE(anchor.scan_tail, eid))
        {            
            targ_eme_buf = extent_readbuffer(rel, targ_eme_addr.physical_page_number, false);
            ExtentAssert(BufferIsValid(targ_eme_buf));
            LockBuffer(targ_eme_buf, BUFFER_LOCK_EXCLUSIVE);
        }
        else
        {
            targ_eme_buf = InvalidBuffer;
        }
        
        START_CRIT_SECTION();

        /*
         * update eob
         */
        ExtentAssert(eob_addr.local_bms_offset == BufferGetEOBPage(eob_buf)->n_bits);
        if(eob_addr.local_bms_offset != BufferGetEOBPage(eob_buf)->n_bits)
        {
            elog(ERROR, "extend eob error, emes in eobpage:%d, extending eob local idx: %d",
                        BufferGetEOBPage(eob_buf)->n_bits,
                        eob_addr.local_bms_offset);
        }
        BufferGetEOBPage(eob_buf)->n_bits++;
        MarkBufferDirty(eob_buf);
        
        /* 
         * update esa page 
         */     
        PAGE_SET_ESA_SCAN_TAIL(BufferGetPage(esa_buf), esa_addr.local_idx, eid);
        MarkBufferDirty(esa_buf);        
        
        /* 
         * update eme page 
         */
        PAGE_SET_EME_SCAN_NEXT(BufferGetPage(ema_buf), eme_addr.local_idx, eid);
        MarkBufferDirty(ema_buf);

        /*
         * update target eme
         */
#define PHYSICAL_TARG_EME_BUF (BufferIsValid(targ_eme_buf) ? targ_eme_buf : ema_buf)
        ExtentAssertEMEIsFree((BufferGetEMAPage(PHYSICAL_TARG_EME_BUF))->ema[targ_eme_addr.local_idx]);
        ema_page_init_eme(BufferGetPage(PHYSICAL_TARG_EME_BUF),
                            targ_eme_addr.local_idx,
                            sid,
                            freespace);
        ExtentAssert(targ_eme_addr.local_idx == BufferGetEMAPage(PHYSICAL_TARG_EME_BUF)->n_emes);
        if(targ_eme_addr.local_idx != BufferGetEMAPage(PHYSICAL_TARG_EME_BUF)->n_emes)
        {
            elog(ERROR, "extend extent error, emes in esapage:%d, extending extent local idx: %d",
                        BufferGetEMAPage(PHYSICAL_TARG_EME_BUF)->n_emes,
                        targ_eme_addr.local_idx);
        }
        BufferGetEMAPage(PHYSICAL_TARG_EME_BUF)->n_emes++;
        MarkBufferDirty(PHYSICAL_TARG_EME_BUF);
        
        /* write eob xlog */        
        XLogRegisterBuffer(0, eob_buf, REGBUF_KEEP_DATA);
        xlrec_eob.slot = eob_addr.local_bms_offset;
        xlrec_eob.n_eobs = BufferGetEOBPage(eob_buf)->n_bits;
        xlrec_eob.flags = 0;
        xlrec_eob.setfree_start = -1;
        xlrec_eob.setfree_end = -1;
        XLogRegisterBufData(0, (char *)&xlrec_eob, SizeOfExtendEOB);

        /*
         * write xlog for esa
         */
        XLogBeginInsert();
        XLogRegisterBuffer(1, esa_buf, REGBUF_KEEP_DATA);
        esa_xlrec.slot = esa_addr.local_idx;
        esa_xlrec.setflag = ESA_SETFLAG_SCANTAIL;
        esa_xlrec.anchor.scan_tail = eid;        
        XLogRegisterBufData(1, (char *)&esa_xlrec, SizeOfSetESA);
        
        /*
         * write xlog for tail eme
         */
        XLogRegisterBuffer(2, ema_buf, REGBUF_KEEP_DATA);
        eme_xlrec.slot = eme_addr.local_idx;
        eme_xlrec.setflag = EMA_SETFLAG_SCANNEXT;
        INIT_EME(&eme_xlrec.eme);
        eme_xlrec.eme.scan_next = eid;
        XLogRegisterBufData(2, (char *)&eme_xlrec, SizeOfSetEME);

        /*
         * write xlog for target eme
         */
        targ_eme_xlrec.setflag = EMA_SETFLAG_ALL;
        targ_eme_xlrec.slot = targ_eme_addr.local_idx;
        INIT_EME(&targ_eme_xlrec.eme);
        targ_eme_xlrec.eme.is_occupied = 1;
        targ_eme_xlrec.eme.shardid = sid;
        targ_eme_xlrec.eme.max_freespace = freespace;
        targ_eme_xlrec.eme.hwm = 1;

        /* eme page: n_emes */
        xlrec_ex.n_emes = BufferGetEMAPage(PHYSICAL_TARG_EME_BUF)->n_emes;
        xlrec_ex.setfree_start = -1;
        xlrec_ex.setfree_end = -1;
        xlrec_ex.flags = 0;

        
        if(!EME_IN_SAME_PAGE(anchor.scan_tail, eid))
        {
            XLogRegisterBuffer(3,  targ_eme_buf, REGBUF_KEEP_DATA);
            XLogRegisterBufData(3, (char *)&xlrec_ex, SizeOfExtendEME);
            XLogRegisterBufData(3, (char *)&targ_eme_xlrec, SizeOfSetEME);
        }            
        else
        {
            XLogRegisterBufData(2,  (char *)&xlrec_ex, SizeOfExtendEME);
            XLogRegisterBufData(2,  (char *)&targ_eme_xlrec, SizeOfSetEME);
        }
        
        recptr = XLogInsert(RM_EXTENT_ID, XLOG_EXTENT_APPEND_EXTENT);
        PageSetLSN(BufferGetPage(eob_buf),  recptr); 
        PageSetLSN(BufferGetPage(esa_buf),  recptr); 
        PageSetLSN(BufferGetPage(ema_buf),  recptr);
        if(BufferIsValid(targ_eme_buf))
        {
            PageSetLSN(BufferGetPage(targ_eme_buf), recptr);
        }

        END_CRIT_SECTION();
        UnlockReleaseBuffer( eob_buf);
        UnlockReleaseBuffer( esa_buf);
        UnlockReleaseBuffer( ema_buf);
        if(BufferIsValid( targ_eme_buf))
        {
            UnlockReleaseBuffer(targ_eme_buf);
        }

        if(trace_extent)
        {
            ereport(LOG,
                  (errmsg("[trace extent]AppendExtentOnlyScan:[rel:%d/%d/%d][mode:1:scanlist is empty]"
                        "[sid:%d, eid:%d]"
                        "[seteob:blocknum=%d,offset=%d]"
                        "[setesa:blocknum=%d,offset=%d,scantail=%d]"
                        "[seteme:blocknum=%d,offset=%d,sid=%d,is_occupied=%d,freespace=%d,allpointer set to INVALIDEID]"
                        "[setemepage:blocknum=%d, n_emes=%d]",
                        rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
                        sid, eid,
                        eob_addr.physical_page_number, eob_addr.local_bms_offset,
                        esa_addr.physical_page_number, esa_addr.local_idx, eid,
                        eme_addr.physical_page_number, eme_addr.local_idx, sid, eme_xlrec.eme.is_occupied, freespace,
                        eme_addr.physical_page_number, xlrec_ex.n_emes)));
        }
    }
    
    UnlockShard(rel, sid, ExclusiveLock);
}


void
shard_add_extent_to_alloclist(Relation rel, ExtentID eid, bool ignore_error)
{// #lizard forgives
    EMAShardAnchor anchor;
    ShardID sid = InvalidShardID;
    ExtentID    next_alloc = InvalidExtentID;
    ExtentID    next_scan = InvalidExtentID;

    /*
     * STEP 0: check if this extent is occupied?
     */
    bool occupied = false;
    
    next_alloc = ema_next_alloc(rel, eid, false, &occupied, &sid, NULL, NULL);

    if(!occupied && !ignore_error)
    {
        elog(ERROR, "this extent %d is not belong to any shard.", eid);
        return;
    }

    LockShard(rel, sid, ExclusiveLock);
    anchor = esa_get_anchor(rel, sid);    
    
    /*
     * STEP 1: check if this extent is in alloc list?
     */
    if(ExtentIdIsValid(next_alloc) || anchor.alloc_tail == eid)
    {
        /* extent is in alloclist now. */
        UnlockShard(rel, sid, ExclusiveLock);
        return;
    }
     
    /*
     * STEP 2: check if this extent is in scan list?
     */
    {
        bool in_scanlist = true;
        if(!ExtentIdIsValid(anchor.scan_head))
        {
            in_scanlist = false;
        }
        else
        {
            next_scan = ema_next_scan(rel, eid, true, NULL, NULL, NULL, NULL);
            if(!ExtentIdIsValid(next_scan) && anchor.scan_tail != eid)
                in_scanlist = false;
        }

        /* extent is not in scan list now. */
        if(!in_scanlist)
        {            
            UnlockShard(rel, sid, ExclusiveLock);
            elog(WARNING, "extentmap is inconsistent. extent %d with shardid %d is not in shard scan list.", eid, sid);
            //shard_add_extent(rel, sid, eid);
            return;
        }
    }

    /*
     * STEP3: add this extent into alloc list?
     */    
    if(!ExtentIdIsValid(anchor.alloc_head))
    {        
        /* only need to set anchor */
        xl_extent_setesa xlrec;
        XLogRecPtr    recptr;
        ESAAddress addr = esa_sid_to_address(sid);
        Buffer buf = extent_readbuffer(rel, addr.physical_page_number, false);
        ExtentAssert(BufferIsValid(buf));
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

        INIT_EXLOG_SETESA(&xlrec);
        
        START_CRIT_SECTION();
        PAGE_SET_ESA_ALLOC(BufferGetPage(buf), addr.local_idx, eid, eid);
        MarkBufferDirty(buf);
        
        XLogBeginInsert();
        XLogRegisterBuffer(0, buf, REGBUF_KEEP_DATA);
        xlrec.slot = addr.local_idx;
        xlrec.setflag = ESA_SETFLAG_ALLOC;
        INIT_ESA(&xlrec.anchor);
        xlrec.anchor.alloc_head = eid;
        xlrec.anchor.alloc_tail = eid;        
        XLogRegisterBufData(0, (char *)&xlrec, SizeOfSetESA);
        
        recptr = XLogInsert(RM_EXTENT_ID, XLOG_EXTENT_MAKE_AVAIL);
        PageSetLSN(BufferGetPage(buf), recptr);        
        END_CRIT_SECTION();

        UnlockReleaseBuffer(buf);

        if(trace_extent)
            ereport(LOG,
                (errmsg("[trace extent]MakeAvailable:[rel:%d/%d/%d][mode1:and a extent to a empty scanlist]"
                        "[sid:%d, eid:%d]"
                        "[setesa:blocknum=%d,offset=%d,allochead=%d,alloctail=%d]",
                        rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
                        sid, eid,
                        addr.physical_page_number, addr.local_idx, eid, eid)));
    }
    else  /* anchor->alloc_head is valid */
    {
        /*
        * STEP 2: add extent to alloc list
        */
        if(eid < anchor.alloc_head)
        {
            /* add this extent to the head of alloclist */
            xl_extent_setesa esa_xlrec;
            xl_extent_seteme ema_xlrec;
            ESAAddress esa_addr;
            EMAAddress ema_addr;
            Buffer esa_buf = InvalidBuffer;
            Buffer ema_buf = InvalidBuffer;
            XLogRecPtr    recptr;

            INIT_EXLOG_SETESA(&esa_xlrec);
            INIT_EXLOG_SETEME(&ema_xlrec);
            /* 
             * get and lock esa buffer 
             */
            esa_addr = esa_sid_to_address(sid);
            esa_buf = extent_readbuffer(rel, esa_addr.physical_page_number, false);
            ExtentAssert(BufferIsValid(esa_buf));
            LockBuffer(esa_buf, BUFFER_LOCK_EXCLUSIVE);

            /* 
             * get and lock ema buffer 
             */
            ema_addr = ema_eid_to_address(eid);
            ema_buf = extent_readbuffer(rel, ema_addr.physical_page_number, false);
            ExtentAssert(BufferIsValid(ema_buf));
            LockBuffer(ema_buf, BUFFER_LOCK_EXCLUSIVE);

            START_CRIT_SECTION();
            /* 
             * set next alloc extent of target extent to first extent in alloclist
             */
            PAGE_SET_EME_ALLOC_NEXT(BufferGetPage(ema_buf), ema_addr.local_idx, anchor.alloc_head);
            MarkBufferDirty(ema_buf);
            /* 
             * set alloclist head to the target extent 
             */
            PAGE_SET_ESA_ALLOC_HEAD(BufferGetPage(esa_buf), esa_addr.local_idx, eid);
            MarkBufferDirty(esa_buf);
            
            /* 
             * write xlog
             */
            XLogBeginInsert();

            /* xlog for esa */
            XLogRegisterBuffer(0, esa_buf, REGBUF_KEEP_DATA);
            esa_xlrec.slot = esa_addr.local_idx;
            esa_xlrec.setflag = ESA_SETFLAG_ALLOCHEAD;
            INIT_ESA(&esa_xlrec.anchor);
            esa_xlrec.anchor.alloc_head = eid;
            XLogRegisterBufData(0, (char *)&esa_xlrec, SizeOfSetESA);

            /* xlog for eme */
            XLogRegisterBuffer(1, ema_buf, REGBUF_KEEP_DATA);
            ema_xlrec.slot = ema_addr.local_idx;
            ema_xlrec.setflag = EMA_SETFLAG_ALLOCNEXT;
            INIT_EME(&ema_xlrec.eme);
            ema_xlrec.eme.alloc_next = anchor.alloc_head;
            XLogRegisterBufData(1, (char *)&ema_xlrec, SizeOfSetEME);            
            
            recptr = XLogInsert(RM_EXTENT_ID, XLOG_EXTENT_MAKE_AVAIL);            
            PageSetLSN(BufferGetPage(esa_buf), recptr);
            PageSetLSN(BufferGetPage(ema_buf), recptr);
            
            END_CRIT_SECTION();

            UnlockReleaseBuffer(ema_buf);
            UnlockReleaseBuffer(esa_buf);    

            if(trace_extent)
                ereport(LOG,
                    (errmsg("[trace extent]MakeAvailable:[rel:%d/%d/%d][mode2:insert a extent to the head of scanlist]"
                            "[sid:%d, eid:%d]"
                            "[setesa:blocknum=%d,offset=%d,allochead=%d]"
                            "[seteme:blocknum=%d,offset=%d,eid=%d,allocnext=%d]",
                            rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
                            sid, eid,
                            esa_addr.physical_page_number, esa_addr.local_idx, eid,
                            ema_addr.physical_page_number, ema_addr.local_idx, eid, anchor.alloc_head)));
        }
        else
        {
            Buffer esa_buf = InvalidBuffer;
            Buffer eme1_buf = InvalidBuffer;
            Buffer eme2_buf = InvalidBuffer;    
            ESAAddress esa_addr;
            EMAAddress eme1_addr, eme2_addr;
            xl_extent_setesa esa_xlrec;
            xl_extent_seteme eme1_xlrec;
            xl_extent_seteme eme2_xlrec;
            XLogRecPtr    recptr;
            ExtentID    e_left, e_right;
            
            INIT_EXLOG_SETESA(&esa_xlrec);
            INIT_EXLOG_SETEME(&eme1_xlrec);
            INIT_EXLOG_SETEME(&eme2_xlrec);

            esa_addr.physical_page_number = InvalidBlockNumber;
            esa_addr.local_idx = 0;
            eme1_addr.physical_page_number = InvalidBlockNumber;
            eme1_addr.local_idx = 0;
            eme2_addr.physical_page_number = InvalidBlockNumber;
            eme2_addr.local_idx = 0;
            /* search point to insert to alloc list */            
            e_left = e_right = anchor.alloc_head;

            while(ExtentIdIsValid(e_left))
            {
                e_right = ema_next_alloc(rel, e_left, true, NULL, NULL, NULL, NULL);

                if(!ExtentIdIsValid(e_right))
                    break;
                
                if(e_left == eid || e_right == eid)
                {
                    /* error: this free extent has been already added to scan list. */
                    elog(WARNING, "system is trying to add a extent in alloc list to its alloc list again."
                                    " shardid:%d, extentid:%d.", sid, eid);
                    UnlockShard(rel, sid, ExclusiveLock);
                    return;
                }
                else if(EME_IN_SAME_PAGE(e_left, eid)
                    || (e_left < eid && eid < e_right))
                {
                    break;
                }                

                e_left = e_right;
            }

            /* 
             * get and lock esa buffer if needed 
             */
            if(!ExtentIdIsValid(e_right))
            {
                esa_addr = esa_sid_to_address(sid);
                esa_buf = extent_readbuffer(rel, esa_addr.physical_page_number, false);
                ExtentAssert(BufferIsValid(esa_buf));            
                LockBuffer(esa_buf, BUFFER_LOCK_EXCLUSIVE);
            }
            else
            {
                esa_buf = InvalidBuffer;
            }

            /* 
             * get and lock insert point buffer 
             */
            eme1_addr = ema_eid_to_address(e_left);
            eme1_buf = extent_readbuffer(rel, eme1_addr.physical_page_number, false);
            ExtentAssert(BufferIsValid(eme1_buf));
            LockBuffer(eme1_buf, BUFFER_LOCK_EXCLUSIVE);

            /* 
             * get and lock buffer which contain target extent if need
             */
            eme2_addr = ema_eid_to_address(eid);
            if(ExtentIdIsValid(e_right) && !EME_IN_SAME_PAGE(e_left, eid))
            {                
                eme2_buf = extent_readbuffer(rel, eme2_addr.physical_page_number, false);
                ExtentAssert(BufferIsValid(eme2_buf));
                LockBuffer(eme2_buf, BUFFER_LOCK_EXCLUSIVE);
            }

            START_CRIT_SECTION();

            XLogBeginInsert();
            /* 
             * set alloc next of insert pointer to target extent 
             */
            PAGE_SET_EME_ALLOC_NEXT(BufferGetPage(eme1_buf),eme1_addr.local_idx, eid);
            
            XLogRegisterBuffer(1, eme1_buf, REGBUF_KEEP_DATA);
            eme1_xlrec.slot = eme1_addr.local_idx;
            eme1_xlrec.setflag = EMA_SETFLAG_ALLOCNEXT;
            INIT_EME(&eme1_xlrec.eme);
            eme1_xlrec.eme.alloc_next = eid;
            XLogRegisterBufData(1, (char *)&eme1_xlrec, SizeOfSetEME);

            /* set next point of target extent if need */
            if(ExtentIdIsValid(e_right))
            {
                eme2_xlrec.slot = eme2_addr.local_idx;
                eme2_xlrec.setflag = EMA_SETFLAG_ALLOCNEXT;
                INIT_EME(&eme2_xlrec.eme);
                eme2_xlrec.eme.alloc_next = e_right;
                
                if(BufferIsValid(eme2_buf))
                {
                    PAGE_SET_EME_ALLOC_NEXT(BufferGetPage(eme2_buf), eme2_addr.local_idx, e_right);
                    XLogRegisterBuffer(2, eme2_buf, REGBUF_KEEP_DATA);
                    XLogRegisterBufData(2, (char *)&eme2_xlrec, SizeOfSetEME);
                }
                else
                {
                    PAGE_SET_EME_ALLOC_NEXT(BufferGetPage(eme1_buf), eme2_addr.local_idx, e_right);
                    XLogRegisterBufData(1, (char *)&eme2_xlrec, SizeOfSetEME);
                }
            }

            MarkBufferDirty(eme1_buf);
            if(BufferIsValid(eme2_buf))
                MarkBufferDirty(eme2_buf);

            /* set alloc tail of esa if need*/
            if(!ExtentIdIsValid(e_right))
            {
                PAGE_SET_ESA_ALLOC_TAIL(BufferGetPage(esa_buf), esa_addr.local_idx, eid);
                MarkBufferDirty(esa_buf);
                
                XLogRegisterBuffer(0, esa_buf, REGBUF_KEEP_DATA);
                esa_xlrec.slot = esa_addr.local_idx;
                esa_xlrec.setflag = ESA_SETFLAG_ALLOCTAIL;
                INIT_ESA(&esa_xlrec.anchor);
                esa_xlrec.anchor.alloc_tail = eid;
                XLogRegisterBufData(0, (char *)&esa_xlrec, SizeOfSetESA);                
            }

            recptr = XLogInsert(RM_EXTENT_ID, XLOG_EXTENT_MAKE_AVAIL);

            if(esa_buf)
                PageSetLSN(BufferGetPage(esa_buf),recptr);
            PageSetLSN(BufferGetPage(eme1_buf),recptr);
            if(eme2_buf)
                PageSetLSN(BufferGetPage(eme2_buf),recptr);

            END_CRIT_SECTION();
            if(eme2_buf)
                UnlockReleaseBuffer(eme2_buf);
            UnlockReleaseBuffer(eme1_buf);
            if(esa_buf)
                UnlockReleaseBuffer(esa_buf);

            if(trace_extent)
            {
                if(ExtentIdIsValid(e_right))
                {
                    ereport(LOG,
                        (errmsg("[trace extent]MakeAvailable:[rel:%d/%d/%d][mode3:insert a extent to scanlist]"
                                "[sid:%d, eid:%d]"
                                "[seteme1:blocknum=%d,offset=%d,eid=%d,allocnext=%d]"
                                "[seteme2:blocknum=%d,offset=%d,eid=%d,allocnext=%d]",
                                rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
                                sid, eid,
                                eme1_addr.physical_page_number, eme1_addr.local_idx, e_left, eid,
                                eme2_addr.physical_page_number, eme2_addr.local_idx, eid, e_right)));
                }
                else
                {
                    ereport(LOG,
                        (errmsg("[trace extent]MakeAvailable:[rel:%d/%d/%d][mode4:insert a extent to the tail of scanlist]"
                                "[sid:%d, eid:%d]"
                                "[setesa:blocknum=%d,offset=%d,alloctail=%d]"
                                "[seteme1:blocknum=%d,offset=%d,eid=%d,allocnext=%d]",
                                rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
                                sid, eid,
                                esa_addr.physical_page_number, esa_addr.local_idx, eid,
                                eme1_addr.physical_page_number, eme1_addr.local_idx, e_left, eid)));
                }
            }
        }
    }

    UnlockShard(rel, sid, ExclusiveLock);
}


void
shard_remove_extent_from_alloclist(Relation rel, ExtentID eid, bool ignore_error, bool need_lock_shard)
{// #lizard forgives
    EMAShardAnchor anchor;
    bool        eid_not_exist = false;
    ShardID        sid = InvalidShardID;
    ShardID     checking_sid = InvalidShardID;
    bool        occupied = false;

    ExtentID    next_alloc = InvalidExtentID;
    next_alloc = ema_next_alloc(rel, eid, !ignore_error, &occupied, &sid, NULL, NULL);
    
    if(!occupied || !ShardIDIsValid(sid))
    {
        if(!ignore_error)
            elog(ERROR, "extend %d is not belong to any shard", eid);
        return;
    }

    if(need_lock_shard)
        LockShard(rel, sid, ExclusiveLock);
    /* 
     * add this extent to alloc list and scan list 
     * If system crash in this point, the free extent will be leaked.
     */
    anchor = esa_get_anchor(rel, sid);    

    if(anchor.alloc_tail != eid && !ExtentIdIsValid(next_alloc))
    {
        if(ignore_error)
        {
            if(need_lock_shard)
                UnlockShard(rel, sid, ExclusiveLock);
            return;
        }
        else
        {
            elog(ERROR, "extend %d is not belong to shard %d alloclist", eid, sid);
        }
    }

    if(!ExtentIdIsValid(anchor.alloc_head))
    {
        eid_not_exist = true;
    }
    else if (anchor.alloc_head == eid)
    {    
        xl_extent_setesa esa_xlrec;
        xl_extent_seteme eme_xlrec;
            
        ExtentID next_eid = ema_next_alloc(rel, eid, true, NULL, &checking_sid, NULL, NULL);
        Buffer esa_buf, eme_buf;    
        ESAAddress esa_addr;
        EMAAddress eme_addr;

        INIT_EXLOG_SETESA(&esa_xlrec);
        INIT_EXLOG_SETEME(&eme_xlrec);
        esa_addr = esa_sid_to_address(sid);
        esa_buf = extent_readbuffer(rel, esa_addr.physical_page_number, false);
        ExtentAssert(BufferIsValid(esa_buf));            
        LockBuffer(esa_buf, BUFFER_LOCK_EXCLUSIVE);

        eme_addr = ema_eid_to_address(eid);
        eme_buf = extent_readbuffer(rel, eme_addr.physical_page_number, false);
        ExtentAssert(BufferIsValid(eme_buf));
        LockBuffer(eme_buf, BUFFER_LOCK_EXCLUSIVE);

        START_CRIT_SECTION();
        
        /* 
         * set anchor alloc head 
         */
        if(ExtentIdIsValid(next_eid))
        {
            PAGE_SET_ESA_ALLOC_HEAD(BufferGetPage(esa_buf), esa_addr.local_idx, next_eid);
            
            esa_xlrec.slot = esa_addr.local_idx;
            esa_xlrec.setflag = ESA_SETFLAG_ALLOCHEAD;
            INIT_ESA(&esa_xlrec.anchor);
            esa_xlrec.anchor.alloc_head = next_eid;
        }
        else
        {
            PAGE_SET_ESA_ALLOC(BufferGetPage(esa_buf), esa_addr.local_idx,
                                    InvalidExtentID, InvalidExtentID);
            esa_xlrec.slot = esa_addr.local_idx;
            esa_xlrec.setflag = ESA_SETFLAG_ALLOC;
            INIT_ESA(&esa_xlrec.anchor);
        }
        MarkBufferDirty(esa_buf);

        /* 
         * set alloc next of eme removed
         */
        PAGE_SET_EME_ALLOC_NEXT(BufferGetPage(eme_buf),eme_addr.local_idx,InvalidExtentID);
        
        eme_xlrec.slot = eme_addr.local_idx;
        eme_xlrec.setflag = EMA_SETFLAG_ALLOCNEXT;
        INIT_EME(&eme_xlrec.eme);
        
        MarkBufferDirty(eme_buf);        

        /*write xlog*/
        {
            XLogRecPtr    recptr;
            /* write xlog */
            XLogBeginInsert();
            XLogRegisterBuffer(0, esa_buf, REGBUF_KEEP_DATA);
            XLogRegisterBuffer(1, eme_buf, REGBUF_KEEP_DATA);
            XLogRegisterBufData(0, (char *)&esa_xlrec, SizeOfSetESA);
            XLogRegisterBufData(1, (char *)&eme_xlrec, SizeOfSetEME);
            recptr = XLogInsert(RM_EXTENT_ID, XLOG_EXTENT_MAKE_FULL);

            PageSetLSN(BufferGetPage(esa_buf),recptr);
            PageSetLSN(BufferGetPage(eme_buf),recptr);
        }

        END_CRIT_SECTION();
        UnlockReleaseBuffer(esa_buf);
        UnlockReleaseBuffer(eme_buf);

        if(trace_extent)
        {
            if(ExtentIdIsValid(next_eid))
                ereport(LOG,
                    (errmsg("[trace extent]RemoveFromAllocList:[rel:%d/%d/%d][mode1.1:remove head of scanlist]"
                            "[sid:%d, eid:%d]"
                            "[setesa:blocknum=%d,offset=%d,allochead=%d]"
                            "[seteme:blocknum=%d,offset=%d,eid=%d,allocnext=InvalidExtentID]",
                            rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
                            sid, eid,
                            esa_addr.physical_page_number, esa_addr.local_idx, next_eid,
                            eme_addr.physical_page_number, eme_addr.local_idx, eid)));
            else
                ereport(LOG,
                    (errmsg("[trace extent]RemoveFromAllocList:[rel:%d/%d/%d][mode1.2:remove head of scanlist]"
                            "[sid:%d, eid:%d]"
                            "[setesa:blocknum=%d,offset=%d,allochead=InvalidExtentID,alloctail=InvalidExtentID]"
                            "[seteme:blocknum=%d,offset=%d,eid=%d,allocnext=InvalidExtentID]",
                            rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
                            sid, eid,
                            esa_addr.physical_page_number, esa_addr.local_idx,
                            eme_addr.physical_page_number, eme_addr.local_idx, eid)));
        }
    }
    else
    {
        xl_extent_setesa esa_xlrec;
        xl_extent_seteme eme1_xlrec;
        xl_extent_seteme eme2_xlrec;
        ExtentID curr, next;
        INIT_EXLOG_SETESA(&esa_xlrec);
        curr = anchor.alloc_head;
        next = anchor.alloc_head;

        INIT_EXLOG_SETEME(&eme1_xlrec);
        INIT_EXLOG_SETEME(&eme2_xlrec);
        while(ExtentIdIsValid(curr))
        {
            next = ema_next_alloc(rel, curr, true, NULL, &checking_sid, NULL, NULL);

            if(next == eid)
            {
                break;
            }
            else
            {
                curr = next;
            }
        }

        if(!ExtentIdIsValid(curr))
            eid_not_exist = true;
        else
        {
            Buffer esa_buf = InvalidBuffer;
            Buffer eme1_buf = InvalidBuffer;
            Buffer eme2_buf = InvalidBuffer;    
            ESAAddress esa_addr;
            EMAAddress eme1_addr, eme2_addr;
            ExtentID    next_next = InvalidExtentID;
            XLogRecPtr    recptr;

            /*
             * lock buffer for esa page if need
             * register block no of xlog : 0
             */
            if(eid == anchor.alloc_tail)
            {
                esa_addr = esa_sid_to_address(sid);
                esa_buf = extent_readbuffer(rel, esa_addr.physical_page_number, false);
                ExtentAssert(BufferIsValid(esa_buf));            
                LockBuffer(esa_buf, BUFFER_LOCK_EXCLUSIVE);
            }

            /*
             * lock buffer for removing point
             * register block no of xlog : 1
             */
            eme1_addr = ema_eid_to_address(curr);
            eme1_buf = extent_readbuffer(rel, eme1_addr.physical_page_number, false);
            ExtentAssert(BufferIsValid(eme1_buf));
            LockBuffer(eme1_buf, BUFFER_LOCK_EXCLUSIVE);

            /*
             * lock buffer for target eme if need.
             * register block no of xlog : 2
             */
            eme2_addr = ema_eid_to_address(next);
            if(ExtentIdIsValid(next) && !EME_IN_SAME_PAGE(curr,next))
            {                
                eme2_buf = extent_readbuffer(rel, eme2_addr.physical_page_number, false);
                ExtentAssert(BufferIsValid(eme2_buf));
                LockBuffer(eme2_buf, BUFFER_LOCK_EXCLUSIVE);
            }

            ExtentAssert(ExtentIdIsValid(next));
            {
                Buffer tmp = BufferIsValid(eme2_buf) ? eme2_buf : eme1_buf;
                EMAPage pg = (EMAPage)PageGetContents(BufferGetPage(tmp));
                next_next = pg->ema[eme2_addr.local_idx].alloc_next;            
            }
            
            START_CRIT_SECTION();

            XLogBeginInsert();
            
            /*
             * update eme of removing point
             * set allocnext of current extent to next alloc of next 
             */
            PAGE_SET_EME_ALLOC_NEXT(BufferGetPage(eme1_buf),eme1_addr.local_idx,next_next);

            XLogRegisterBuffer(1, eme1_buf, REGBUF_KEEP_DATA);
            eme1_xlrec.slot = eme1_addr.local_idx;
            eme1_xlrec.setflag = EMA_SETFLAG_ALLOCNEXT;
            INIT_EME(&eme1_xlrec.eme);
            eme1_xlrec.eme.alloc_next = next_next;
            XLogRegisterBufData(1, (char *)&eme1_xlrec, SizeOfSetEME);

            /* 
             * update target eme to be removed if need
             * set allocnext of next extent
             * it does not need to be updated if the target eme is the tail of alloc list.
             */
            if(ExtentIdIsValid(next_next))
            {
                if(BufferIsValid(eme2_buf))
                {    
                    /*
                     * xlog is registered to buffer no.2 if target eme is not at same page as removing point eme.
                     */
                    PAGE_SET_EME_ALLOC_NEXT(BufferGetPage(eme2_buf),eme2_addr.local_idx,InvalidExtentID);

                    XLogRegisterBuffer(2, eme2_buf, REGBUF_KEEP_DATA);
                    eme2_xlrec.slot = eme2_addr.local_idx;
                    eme2_xlrec.setflag = EMA_SETFLAG_ALLOCNEXT;
                    INIT_EME(&eme2_xlrec.eme);
                    XLogRegisterBufData(2, (char *)&eme2_xlrec, SizeOfSetEME);
                    MarkBufferDirty(eme2_buf);
                }
                else
                {
                    /*
                     * xlog is registered to buffer no.1 if target eme is at same page as removing point eme.
                     */
                    PAGE_SET_EME_ALLOC_NEXT(BufferGetPage(eme1_buf),eme2_addr.local_idx,InvalidExtentID);

                    eme2_xlrec.slot = eme2_addr.local_idx;
                    eme2_xlrec.setflag = EMA_SETFLAG_ALLOCNEXT;
                    INIT_EME(&eme2_xlrec.eme);
                    XLogRegisterBufData(1, (char *)&eme2_xlrec, SizeOfSetEME);
                }
            }

            MarkBufferDirty(eme1_buf);

            /*
             * update alloc tail of esa if target eme is exactly right the tail of alloc list
             */
            if(BufferIsValid(esa_buf))
            {
                ExtentAssert(!ExtentIdIsValid(next_next));
                PAGE_SET_ESA_ALLOC_TAIL(BufferGetPage(esa_buf),
                                        esa_addr.local_idx,
                                        curr);

                XLogRegisterBuffer(0, esa_buf, REGBUF_KEEP_DATA);
                esa_xlrec.slot = esa_addr.local_idx;
                esa_xlrec.setflag = ESA_SETFLAG_ALLOCTAIL;
                INIT_ESA(&esa_xlrec.anchor);
                esa_xlrec.anchor.alloc_tail = curr;
                XLogRegisterBufData(0, (char *)&esa_xlrec, SizeOfSetESA);
                MarkBufferDirty(esa_buf);
            }    

            recptr = XLogInsert(RM_EXTENT_ID, XLOG_EXTENT_MAKE_FULL);

            if(esa_buf)
                PageSetLSN(BufferGetPage(esa_buf),recptr);
            PageSetLSN(BufferGetPage(eme1_buf),recptr);
            if(eme2_buf)
                PageSetLSN(BufferGetPage(eme2_buf),recptr);

            END_CRIT_SECTION();
            if(eme2_buf)
                UnlockReleaseBuffer(eme2_buf);
            UnlockReleaseBuffer(eme1_buf);
            if(esa_buf)
                UnlockReleaseBuffer(esa_buf);

            if(trace_extent)
            {
                if(eid == anchor.alloc_tail)
                {
                    if(ExtentIdIsValid(next_next))
                    {
                        ereport(LOG,
                            (errmsg("[trace extent]RemoveFromAllocList:[rel:%d/%d/%d][mode2.1:remove one extent from scanlist]"
                                    "[sid:%d, eid:%d]"
                                    "[setesa:blocknum=%d,offset=%d,alloctail=%d]"
                                    "[seteme1:blocknum=%d,offset=%d,eid=%d,allocnext=%d]"
                                    "[seteme2:blocknum=%d,offset=%d,eid=%d,allocnext=InvalidExtentID]",
                                    rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
                                    sid, eid,
                                    esa_addr.physical_page_number, esa_addr.local_idx, curr,
                                    eme1_addr.physical_page_number, eme1_addr.local_idx, curr, next_next,
                                    eme2_addr.physical_page_number, eme2_addr.local_idx, eid)));
                    }
                    else
                    {
                        ereport(LOG,
                            (errmsg("[trace extent]RemoveFromAllocList:[rel:%d/%d/%d][mode2.2:remove one extent from scanlist]"
                                    "[sid:%d, eid:%d]"
                                    "[setesa:blocknum=%d,offset=%d,alloctail=%d]"
                                    "[seteme1:blocknum=%d,offset=%d,eid=%d,allocnext=InvalidExtentID]",
                                    rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
                                    sid, eid,
                                    esa_addr.physical_page_number, esa_addr.local_idx, curr,
                                    eme1_addr.physical_page_number, eme1_addr.local_idx, curr)));
                    }
                }
                else
                {
                    if(ExtentIdIsValid(next_next))
                    {
                        ereport(LOG,
                            (errmsg("[trace extent]RemoveFromAllocList:[rel:%d/%d/%d][mode2.3:remove one extent from scanlist]"
                                    "[sid:%d, eid:%d]"
                                    "[seteme1:blocknum=%d,offset=%d,eid=%d,allocnext=%d]"
                                    "[seteme2:blocknum=%d,offset=%d,eid=%d,allocnext=InvalidExtentID]",
                                    rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
                                    sid, eid,
                                    eme1_addr.physical_page_number, eme1_addr.local_idx, curr, next_next,
                                    eme2_addr.physical_page_number, eme2_addr.local_idx, eid)));
                    }
                    else
                    {
                        ereport(LOG,
                            (errmsg("[trace extent]RemoveFromAllocList:[rel:%d/%d/%d][mode2.4:remove one extent from scanlist]"
                                    "[sid:%d, eid:%d]"
                                    "[seteme1:blocknum=%d,offset=%d,eid=%d,allocnext=InvalidExtentID]",
                                    rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
                                    sid, eid,
                                    eme1_addr.physical_page_number, eme1_addr.local_idx, curr)));
                    }
                }
            }
        }
    }
    
    if(need_lock_shard)
        UnlockShard(rel, sid, ExclusiveLock);

    if(eid_not_exist)
    {
        elog(ERROR, "extent %d is not belong to shard %d", eid, sid);
    }
}

void
shard_remove_extent_from_scanlist(Relation rel, ExtentID eid, bool ignore_error, bool need_lock_shard)
{// #lizard forgives
    EMAShardAnchor anchor;
    bool        eid_not_exist = false;
    ShardID        sid = InvalidShardID;
    ShardID     checking_sid = InvalidShardID;
    bool        occupied = false;

    ExtentID    next_scan = InvalidExtentID;
    next_scan = ema_next_scan(rel, eid, !ignore_error, &occupied, &sid, NULL, NULL);
    
    if(!occupied || !ShardIDIsValid(sid))
    {
        if(!ignore_error)
            elog(ERROR, "extend %d is not belong to any shard", eid);
        return;
    }

    if(need_lock_shard)
        LockShard(rel, sid, ExclusiveLock);
    /* 
     *.
     */
    anchor = esa_get_anchor(rel, sid);    

    if(anchor.scan_tail != eid && !ExtentIdIsValid(next_scan))
    {
        if(ignore_error)
        {
            if(need_lock_shard)
                UnlockShard(rel, sid, ExclusiveLock);
            return;
        }
        else
        {
            elog(ERROR, "extend %d is not belong to shard %d alloclist", eid, sid);
        }
    }

    if(!ExtentIdIsValid(anchor.scan_head))
    {
        eid_not_exist = true;
    }
    else if (anchor.scan_head == eid)
    {    
        xl_extent_seteob eob_xlrec;
        xl_extent_setesa esa_xlrec;        
        xl_extent_cleaneme eme_xlrec;
            
        ExtentID next_eid = ema_next_scan(rel, eid, true, NULL, &checking_sid, NULL, NULL);
        Buffer eob_buf, esa_buf, eme_buf;
        EOBAddress eob_addr;
        ESAAddress esa_addr;
        EMAAddress eme_addr;

        INIT_EXLOG_SETEOB(&eob_xlrec);
        INIT_EXLOG_SETESA(&esa_xlrec);
        INIT_EXLOG_CLEANEME(&eme_xlrec);

        /*
         * lock eob page 0
         */
        eob_addr = eob_eid_to_address(eid);
        eob_buf = extent_readbuffer(rel, eob_addr.physical_page_number, false);
        ExtentAssert(BufferIsValid(eob_buf));            
        LockBuffer(eob_buf, BUFFER_LOCK_EXCLUSIVE);

        /*
         * lock esa page 1
         */
        esa_addr = esa_sid_to_address(sid);
        esa_buf = extent_readbuffer(rel, esa_addr.physical_page_number, false);
        ExtentAssert(BufferIsValid(esa_buf));            
        LockBuffer(esa_buf, BUFFER_LOCK_EXCLUSIVE);

        /*
         * lock ema page 2
         */
        eme_addr = ema_eid_to_address(eid);
        eme_buf = extent_readbuffer(rel, eme_addr.physical_page_number, false);
        ExtentAssert(BufferIsValid(eme_buf));
        LockBuffer(eme_buf, BUFFER_LOCK_EXCLUSIVE);

        START_CRIT_SECTION();

        /*
         * set eob page
         */
        eob_page_mark_extent(BufferGetEOBPage(eob_buf), eob_addr.local_bms_offset, true);
        MarkBufferDirty(eob_buf);
        eob_xlrec.slot = eob_addr.local_bms_offset;
        eob_xlrec.setfree  = true;
        
        /* 
         * set anchor scan head 
         */
        if(ExtentIdIsValid(next_eid))
        {
            PAGE_SET_ESA_SCAN_HEAD(BufferGetPage(esa_buf), esa_addr.local_idx, next_eid);
            
            esa_xlrec.slot = esa_addr.local_idx;
            esa_xlrec.setflag = ESA_SETFLAG_SCANHEAD;
            INIT_ESA(&esa_xlrec.anchor);
            esa_xlrec.anchor.scan_head = next_eid;
        }
        else
        {
            PAGE_SET_ESA_SCAN(BufferGetPage(esa_buf), esa_addr.local_idx,
                                    InvalidExtentID, InvalidExtentID);
            esa_xlrec.slot = esa_addr.local_idx;
            esa_xlrec.setflag = ESA_SETFLAG_SCAN;
            INIT_ESA(&esa_xlrec.anchor);
        }
        MarkBufferDirty(esa_buf);

        /* 
         * set scan next of eme removed
         */
        ema_page_free_eme(BufferGetPage(eme_buf), eme_addr.local_idx);
        
        eme_xlrec.slot = eme_addr.local_idx;
        
        MarkBufferDirty(eme_buf);        

        /*write xlog*/
        {
            XLogRecPtr    recptr;
            /* write xlog */
            XLogBeginInsert();

            XLogRegisterBuffer(0, eob_buf, REGBUF_KEEP_DATA);
            XLogRegisterBufData(0, (char *)&eob_xlrec, SizeOfSetEOB);    
        
            XLogRegisterBuffer(1, esa_buf, REGBUF_KEEP_DATA);            
            XLogRegisterBufData(1, (char *)&esa_xlrec, SizeOfSetESA);

            XLogRegisterBuffer(2, eme_buf, REGBUF_KEEP_DATA);
            XLogRegisterBufData(2, (char *)&eme_xlrec, SizeOfCleanEME);
            recptr = XLogInsert(RM_EXTENT_ID, XLOG_EXTENT_DETACH_EXTENT);

            PageSetLSN(BufferGetPage(eob_buf),recptr);
            PageSetLSN(BufferGetPage(esa_buf),recptr);
            PageSetLSN(BufferGetPage(eme_buf),recptr);
        }

        END_CRIT_SECTION();
        UnlockReleaseBuffer(eob_buf);
        UnlockReleaseBuffer(esa_buf);
        UnlockReleaseBuffer(eme_buf);

        if(trace_extent)
        {
            if(ExtentIdIsValid(next_eid))
                ereport(LOG,
                    (errmsg("[trace extent]DeattachExtent:[rel:%d/%d/%d][mode1:scanlist has only one extent]"
                            "[sid:%d, eid:%d]"
                            "[freeob:blocknum=%d,offset=%d]"
                            "[setesa:blocknum=%d,offset=%d,scanhead=%d]"
                            "[freeeme:blocknum=%d,offset=%d]",
                            rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
                            sid, eid,
                            eob_addr.physical_page_number, eob_addr.local_bms_offset,
                            esa_addr.physical_page_number, esa_addr.local_idx, next_eid,
                            eme_addr.physical_page_number, eme_addr.local_idx)));
            else
                ereport(LOG,
                    (errmsg("[trace extent]DeattachExtent:[rel:%d/%d/%d][mode2:remove a internal element of scanlist]"
                            "[sid:%d, eid:%d]"
                            "[freeob:blocknum=%d,offset=%d]"
                            "[setesa:blocknum=%d,offset=%d,scanhead=InvalidExtentID,scantail=InvalidExtentID]"
                            "[freeeme:blocknum=%d,offset=%d]",
                            rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
                            sid, eid,
                            eob_addr.physical_page_number, eob_addr.local_bms_offset,
                            esa_addr.physical_page_number, esa_addr.local_idx,
                            eme_addr.physical_page_number, eme_addr.local_idx)));
        }
    }
    else
    {
        xl_extent_seteob eob_xlrec;
        xl_extent_setesa esa_xlrec;
        xl_extent_seteme eme1_xlrec;
        xl_extent_cleaneme eme2_xlrec;
        ExtentID curr, next;

        INIT_EXLOG_SETEOB(&eob_xlrec);
        INIT_EXLOG_SETESA(&esa_xlrec);        
        INIT_EXLOG_SETEME(&eme1_xlrec);
        INIT_EXLOG_CLEANEME(&eme2_xlrec);

        curr = anchor.scan_head;
        next = anchor.scan_head;
        while(ExtentIdIsValid(curr))
        {
            next = ema_next_scan(rel, curr, true, NULL, &checking_sid, NULL, NULL);

            if(next == eid)
            {
                break;
            }
            else
            {
                curr = next;
            }
        }

        if(!ExtentIdIsValid(curr))
            eid_not_exist = true;
        else
        {
            Buffer eob_buf = InvalidBuffer;
            Buffer esa_buf = InvalidBuffer;
            Buffer eme1_buf = InvalidBuffer;
            Buffer eme2_buf = InvalidBuffer;
            Buffer eme2_buf_alias = InvalidBuffer;
            EOBAddress eob_addr;
            ESAAddress esa_addr;
            EMAAddress eme1_addr, eme2_addr;
            ExtentID    next_next = InvalidExtentID;
            XLogRecPtr    recptr;

            /*
             * lock eob page 0
             */
            eob_addr = eob_eid_to_address(eid);
            eob_buf = extent_readbuffer(rel, eob_addr.physical_page_number, false);
            ExtentAssert(BufferIsValid(eob_buf));            
            LockBuffer(eob_buf, BUFFER_LOCK_EXCLUSIVE);
        
            /*
             * lock buffer for esa page if need
             * register block no of xlog : 1
             */
            if(eid == anchor.scan_tail)
            {
                esa_addr = esa_sid_to_address(sid);
                esa_buf = extent_readbuffer(rel, esa_addr.physical_page_number, false);
                ExtentAssert(BufferIsValid(esa_buf));            
                LockBuffer(esa_buf, BUFFER_LOCK_EXCLUSIVE);
            }

            /*
             * lock buffer for removing point
             * register block no of xlog : 2
             */
            ExtentAssert(ExtentIdIsValid(curr));
            eme1_addr = ema_eid_to_address(curr);
            eme1_buf = extent_readbuffer(rel, eme1_addr.physical_page_number, false);
            ExtentAssert(BufferIsValid(eme1_buf));
            LockBuffer(eme1_buf, BUFFER_LOCK_EXCLUSIVE);

            /*
             * lock buffer for target eme if need.
             * register block no of xlog : 3
             */
            ExtentAssert(ExtentIdIsValid(next));
            eme2_addr = ema_eid_to_address(next);
            if(ExtentIdIsValid(next) && !EME_IN_SAME_PAGE(curr,next))
            {                
                eme2_buf = extent_readbuffer(rel, eme2_addr.physical_page_number, false);
                ExtentAssert(BufferIsValid(eme2_buf));
                LockBuffer(eme2_buf, BUFFER_LOCK_EXCLUSIVE);
            }
            eme2_buf_alias = BufferIsValid(eme2_buf) ? eme2_buf : eme1_buf;        
            next_next = BufferGetEMAPage(eme2_buf_alias)->ema[eme2_addr.local_idx].alloc_next;            
            
            START_CRIT_SECTION();

            XLogBeginInsert();

            /*
             * 0 : set eob page
             */
            eob_page_mark_extent(BufferGetEOBPage(eob_buf), eob_addr.local_bms_offset, true);
            MarkBufferDirty(eob_buf);
            eob_xlrec.slot = eob_addr.local_bms_offset;
            eob_xlrec.setfree  = true;
            XLogRegisterBuffer(0, eob_buf, REGBUF_KEEP_DATA);
            XLogRegisterBufData(0, (char *)&eob_xlrec, SizeOfSetEOB);
            
            /*
             * 1: set esa page
             * update alloc tail of esa if target eme is exactly right the tail of alloc list
             */
            if(BufferIsValid(esa_buf))
            {
                ExtentAssert(!ExtentIdIsValid(next_next));
                PAGE_SET_ESA_SCAN_TAIL(BufferGetPage(esa_buf),
                                        esa_addr.local_idx,
                                        curr);

                esa_xlrec.slot = esa_addr.local_idx;
                esa_xlrec.setflag = ESA_SETFLAG_SCANTAIL;
                INIT_ESA(&esa_xlrec.anchor);
                esa_xlrec.anchor.scan_tail = curr;
                
                XLogRegisterBuffer(1, esa_buf, REGBUF_KEEP_DATA);
                XLogRegisterBufData(1, (char *)&esa_xlrec, SizeOfSetESA);
                MarkBufferDirty(esa_buf);
            }    

            /*
             * 2 : set removing point link
             * update eme of removing point
             * set allocnext of current extent to next alloc of next 
             */
            PAGE_SET_EME_SCAN_NEXT(BufferGetPage(eme1_buf),eme1_addr.local_idx,next_next);

            eme1_xlrec.slot = eme1_addr.local_idx;
            eme1_xlrec.setflag = EMA_SETFLAG_SCANNEXT;
            INIT_EME(&eme1_xlrec.eme);
            eme1_xlrec.eme.scan_next = next_next;
            
            XLogRegisterBuffer(2, eme1_buf, REGBUF_KEEP_DATA);
            XLogRegisterBufData(2, (char *)&eme1_xlrec, SizeOfSetEME);
            MarkBufferDirty(eme1_buf);

            /* 
             * 3: set target eme
             * update target eme to be removed if need
             * set allocnext of next extent
             * it does not need to be updated if the target eme is the tail of alloc list.
             */
            if(ExtentIdIsValid(next_next))
            {
                if(BufferIsValid(eme2_buf))
                {    
                    /*
                     * xlog is registered to buffer no.3 if target eme is not at same page as removing point eme.
                     */
                    ema_page_free_eme(BufferGetPage(eme2_buf), eme2_addr.local_idx);
                    XLogRegisterBuffer(3, eme2_buf, REGBUF_KEEP_DATA);
                    eme2_xlrec.slot = eme2_addr.local_idx;
                    XLogRegisterBufData(3, (char *)&eme2_xlrec, SizeOfCleanEME);
                    MarkBufferDirty(eme2_buf);
                }
                else
                {
                    /*
                     * xlog is registered to buffer no.2 if target eme is at same page as removing point eme.
                     */
                    ema_page_free_eme(BufferGetPage(eme1_buf), eme2_addr.local_idx);
                    
                    eme2_xlrec.slot = eme2_addr.local_idx;
                    XLogRegisterBufData(2, (char *)&eme2_xlrec, SizeOfCleanEME);
                }
            }            
            
            recptr = XLogInsert(RM_EXTENT_ID, XLOG_EXTENT_DETACH_EXTENT);

            PageSetLSN(BufferGetPage(eob_buf),recptr);
            if(esa_buf)
                PageSetLSN(BufferGetPage(esa_buf),recptr);
            PageSetLSN(BufferGetPage(eme1_buf),recptr);
            if(eme2_buf)
                PageSetLSN(BufferGetPage(eme2_buf),recptr);

            END_CRIT_SECTION();
            
            UnlockReleaseBuffer(eob_buf);
            if(esa_buf)
                UnlockReleaseBuffer(esa_buf);
            UnlockReleaseBuffer(eme1_buf);
            if(eme2_buf)
                UnlockReleaseBuffer(eme2_buf);    

            if(trace_extent)
            {
                if(eid == anchor.scan_tail)
                    ereport(LOG,
                        (errmsg("[trace extent]DeattachExtent:[rel:%d/%d/%d][mode3:remove the tail of scanlist]"
                                "[sid:%d, eid:%d]"
                                "[freeeob:blocknum=%d,offset=%d]"
                                "[setesa:blocknum=%d,offset=%d,scantail=%d]"
                                "[seteme:blocknum=%d,offset=%d,scannext=%d]"
                                "[cleaneme:blocknum=%d,offset=%d]",
                                rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
                                sid, eid,
                                eob_addr.physical_page_number, eob_addr.local_bms_offset,
                                esa_addr.physical_page_number, esa_addr.local_idx, esa_xlrec.anchor.scan_tail,
                                eme1_addr.physical_page_number, eme1_addr.local_idx, next_next,
                                eme2_addr.physical_page_number, eme2_addr.local_idx)));
                else
                    ereport(LOG,
                        (errmsg("[trace extent]DeattachExtent:[rel:%d/%d/%d][mode4:remove a internal extent of scanlist]"
                                "[sid:%d, eid:%d]"
                                "[freeeob:blocknum=%d,offset=%d]"
                                "[seteme:blocknum=%d,offset=%d,scannext=%d]"
                                "[cleaneme:blocknum=%d,offset=%d]",
                                rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
                                sid, eid,
                                eob_addr.physical_page_number, eob_addr.local_bms_offset,
                                eme1_addr.physical_page_number, eme1_addr.local_idx, next_next,
                                eme2_addr.physical_page_number, eme2_addr.local_idx)));
            }
        }
    }

    if(need_lock_shard)
        UnlockShard(rel, sid, ExclusiveLock);

    if(eid_not_exist)
    {
        elog(ERROR, "extent %d is not belong to shard %d", eid, sid);
    }
}



ExtentID 
shard_apply_free_extent(Relation rel, ShardID sid)
{
    ExtentID     eid;
reget_eid:
    /* get free extent from eob page and set it */
    eid = eob_get_free_extent_and_set_busy(rel);

    /* check this extentid is indeed free in page
      * TODO: check
      */

    if(!ExtentIdIsValid(eid))
        return InvalidExtentID;
    /* init eme */
    //ema_init_eme(rel, eid, sid);

    if(!shard_add_extent(rel, sid, eid))
        goto reget_eid;
    return eid;
}

void
repair_eme(Relation rel, BlockNumber blk, ShardID sid)
{
    
}

/*
 * add new extent to shard list if there is no available extent
 */
ExtentID
GetExtentWithFreeSpace(Relation rel, ShardID sid, uint8 min_cat)
{// #lizard forgives
    EMAShardAnchor anchor = esa_get_anchor(rel, sid);
    ExtentID    e_idx;
    ExtentID    e_next;
    ExtentID    result = InvalidExtentID;
    uint8        avail;

    LockShard(rel, sid, AccessShareLock);
    if(ExtentIdIsValid(anchor.alloc_head))
    {
        e_idx = anchor.alloc_head;

        while(ExtentIdIsValid(e_idx))
        {
            ShardID e_sid = InvalidShardID;
            bool    occupied = false;
            e_next = ema_next_alloc(rel, e_idx, true, &occupied, &e_sid, NULL, &avail);

            if(!occupied)
            {
                elog(WARNING, "eid %d is in the list of shard %d, but the occupation flag is false.",
                        e_idx, sid);
            }

            if(occupied && e_sid != sid)
            {
                elog(WARNING, "eid %d is in the list of shard %d, but shardid of EME is %d.",
                        e_idx, sid, e_sid);
            }
            
            if(avail >= min_cat)
            {
                result = e_idx;
                break;
            }
            else
                e_idx = e_next;
        }            
    }
    UnlockShard(rel, sid, AccessShareLock);
    
    if(result == InvalidExtentID)
    {
        result = shard_apply_free_extent(rel,sid);
    }

    return result;
}

/*
 * Extend extent for relation if no avaible extents.
 * eid is passed for check consistency extentmapping
 */
void
ExtendExtentForShard(Relation rel, ShardID sid, ExtentID eid, uint8 freespace, bool for_rebuild)
{// #lizard forgives
    //smgrnblocks(rel->rd_smgr, MAIN_FORKNUM);
    Buffer eob_buf;
    Buffer ema_buf;
    EOBPage    eob_pg;
    EMAPage ema_pg;
    //XLogRecPtr    recptr;

    EMAAddress ema_addr;
    EOBAddress eob_addr;

    //xl_extent_extendeob xlrec_eob;
    //xl_extent_initeme xlrec_initeme;
    //xl_extent_extendeme xlrec_ex;
    
    bool    ema_error = false;
    bool    eob_error = false;

    //INIT_EXLOG_EXTENDEOB(&xlrec_eob);
    //INIT_EXLOG_INITEME(&xlrec_initeme);
    //INIT_EXLOG_EXTENDEME(&xlrec_ex);
    
    /*
     * STEP 1: get page of  EOB
     * registred block no: 0
     */
    {
        eob_addr = eob_eid_to_address(eid);
        if(eid > 0 && eob_addr.local_bms_offset == 0)
        {
            eob_buf = extent_readbuffer(rel, eob_addr.physical_page_number - 1, false);
            ExtentAssert(BufferIsValid(eob_buf));
            if(BufferIsInvalid(eob_buf))
            {
                eob_error = true;
                goto extent_mapping_error;
            }

            LockBuffer(eob_buf, BUFFER_LOCK_SHARE);
            eob_pg = (EOBPage)PageGetContents(BufferGetPage(eob_buf));
            ExtentAssert(eob_pg->n_bits == eob_pg->max_bits && eob_pg->max_bits == EOBS_PER_PAGE);
            if(eob_pg->n_bits != eob_pg->max_bits || eob_pg->max_bits != EOBS_PER_PAGE)
            {
                eob_error = true;
                UnlockReleaseBuffer(eob_buf);
                goto extent_mapping_error;
            }
            UnlockReleaseBuffer(eob_buf);
        }        
        
        eob_buf = extent_readbuffer(rel, eob_addr.physical_page_number, true);
        ExtentAssert(BufferIsValid(eob_buf));
        if(BufferIsInvalid(eob_buf))
        {
            eob_error = true;
            goto extent_mapping_error;
        }

        LockBuffer(eob_buf, BUFFER_LOCK_EXCLUSIVE);
        eob_pg = (EOBPage)PageGetContents(BufferGetPage(eob_buf));

        ExtentAssert(eob_pg->n_bits == eob_addr.local_bms_offset);
        if(eob_pg->n_bits != eob_addr.local_bms_offset)
        {
            eob_error = true;
            UnlockReleaseBuffer(eob_buf);
            goto extent_mapping_error;
        }
    }
    
    /*
     * STEP 2: get page of EME
     * registred block no: 1
     */
    {
        ema_addr = ema_eid_to_address(eid);
        
        if(eid > 0 && ema_addr.local_idx == 0)
        {
            ema_buf = extent_readbuffer(rel, ema_addr.physical_page_number - 1, false);
            ExtentAssert(BufferIsValid(ema_buf));
            if(BufferIsInvalid(ema_buf))
            {
                ema_error = true;
                goto extent_mapping_error;
            }

            LockBuffer(ema_buf, BUFFER_LOCK_SHARE);
            ema_pg = (EMAPage)PageGetContents(BufferGetPage(ema_buf));
            ExtentAssert(ema_pg->n_emes == ema_pg->max_emes && ema_pg->max_emes == EMES_PER_PAGE);
            if(ema_pg->n_emes != ema_pg->max_emes || ema_pg->max_emes != EMES_PER_PAGE)
            {
                ema_error = true;
                UnlockReleaseBuffer(ema_buf);
                goto extent_mapping_error;
            }
            UnlockReleaseBuffer(ema_buf);
        }
        
        ema_buf = extent_readbuffer(rel, ema_addr.physical_page_number, true);
        ExtentAssert(BufferIsValid(ema_buf));
        if(BufferIsInvalid(ema_buf))
        {
            ema_error = true;
            goto extent_mapping_error;
        }

        LockBuffer(ema_buf, BUFFER_LOCK_EXCLUSIVE);
        ema_pg = (EMAPage)PageGetContents(BufferGetPage(ema_buf));

        ExtentAssert(ema_pg->n_emes == ema_addr.local_idx);
        if(ema_pg->n_emes != ema_addr.local_idx)
        {
            ema_error = true;
            UnlockReleaseBuffer(ema_buf);
            goto extent_mapping_error;
        }
    }
#if 0
    START_CRIT_SECTION();

    XLogBeginInsert();
    /*
     * set page of EOB 
     */
    {
        eob_pg->n_bits++;
        MarkBufferDirty(eob_buf);
        /* write xlog */        
        XLogRegisterBuffer(0, eob_buf, REGBUF_KEEP_DATA);
        xlrec_eob.slot = eob_addr.local_bms_offset;
        xlrec_eob.n_eobs = eob_pg->n_bits;
        XLogRegisterBufData(0, (char *)&xlrec_eob, SizeOfExtendEOB);    
    }

    /*
     * set page of EME
     */
    {
        //ema_page_occup_eme(BufferGetPage(ema_buf), ema_addr.local_idx);
        ema_pg->n_emes++;
        MarkBufferDirty(ema_buf);

        /* write xlog */
        XLogRegisterBuffer(1, ema_buf, REGBUF_KEEP_DATA);
        //xlrec_initeme.slot = ema_addr.local_idx;
        //xlrec_initeme.freespace = 0;
        //xlrec_initeme.shardid = 0;
        //XLogRegisterBufData(1, (char *)&xlrec_initeme, SizeOfInitEME);
        xlrec_ex.n_emes = ema_pg->n_emes;
        XLogRegisterBufData(1, (char *)&xlrec_ex, SizeOfExtendEME);
    }

    recptr = XLogInsert(RM_EXTENT_ID, XLOG_EXTENT_NEW_EXTENT);
    PageSetLSN(BufferGetPage(eob_buf), recptr);
    PageSetLSN(BufferGetPage(ema_buf), recptr);
    END_CRIT_SECTION();
#endif
    UnlockReleaseBuffer(ema_buf);
    UnlockReleaseBuffer(eob_buf);

    /*
     * STEP 3: link to shard lists(scan list and alloc list)
     */
    if(freespace > EXTENT_SAVED_MINCAT)
        shard_append_extent(rel, sid, eid, freespace, for_rebuild);
    else
        shard_append_extent_onlyscan(rel, sid, eid, freespace);

    return;
extent_mapping_error:
    if(ema_error)
        UnlockReleaseBuffer(eob_buf);
    elog(ERROR, "extentmapping is error. ema_error:%d, eob_error:%d", ema_error, eob_error);
}

/*
 * Only be called during rebuilding extent map.
 */
void 
ExtendExtentForRebuild(Relation rel, ExtentID eid)
{
    ExtentID tmp_eid = InvalidExtentID;
    ExtentID last_eid = InvalidExtentID;

    ExtentAssert(ExtentIdIsValid(eid));
    while(true)
    {
        tmp_eid = extent_extend_for_rebuild(rel, eid);

        /* avoid infinite looping*/
        if(ExtentIdIsValid(last_eid) && last_eid == tmp_eid)
        {
            elog(ERROR, "cannot extend relation %d/%d/%d extent eid from %d to %d",
                    rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
                    last_eid, eid);
        }

        if(tmp_eid > eid)
        {
            elog(ERROR, "extentd relation %d/%d/%d error. "
                        "Caller was supposed to extend up to %d, but actually extend to %d",
                    rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
                    eid, tmp_eid);
        }
        
        if(tmp_eid == eid)
            break;

        last_eid = tmp_eid;
    }
}

ExtentID
extent_extend_for_rebuild(Relation rel, ExtentID eid)
{// #lizard forgives
    XLogRecPtr    recptr;
    EOBAddress eob_old_max_eid = {0, 0};
    EOBAddress eob_new_max_eid;
    ExtentID   eob_old_extent = InvalidExtentID;
    EMAAddress ema_old_max_eid = {EMAPAGE_OFFSET, 0};
    EMAAddress ema_new_max_eid;
    ExtentID   ema_old_extent = InvalidExtentID;
    ExtentID    target_eid = InvalidExtentID;

    xl_extent_extendeob xlrec_eob;
    xl_extent_extendeme xlrec_eme;
    Buffer eob_buf;
    Buffer ema_buf;
    int blkno;
    int    bits;

    /* find the old max eob*/
    for(blkno = EOBPAGE_OFFSET; blkno < EOB_TOPPAGE; blkno++)
    {
        eob_buf = extent_readbuffer(rel, blkno, true);
        if(BufferIsInvalid(eob_buf))
            break;

        if(EOBPage_IS_EMPTY(BufferGetPage(eob_buf)))
        {
            ReleaseBuffer(eob_buf);
            break;
        }
        
        eob_old_max_eid.physical_page_number = blkno;
        eob_old_max_eid.local_bms_offset = BufferGetEOBPage(eob_buf)->n_bits - 1;
        
        if(!EOBPage_IS_FULL(BufferGetPage(eob_buf)))
        {
            ReleaseBuffer(eob_buf);
            break;
        }

        ReleaseBuffer(eob_buf);
    }

    /* find the old max ema*/
    for(blkno = EMAPAGE_OFFSET; blkno < EMA_TOPPAGE; blkno++)
    {
        ema_buf = extent_readbuffer(rel, blkno, true);
        if(BufferIsInvalid(ema_buf))
            break;

        if(EMAPage_IS_EMPTY(BufferGetPage(ema_buf)))
        {
            ReleaseBuffer(ema_buf);
            break;
        }
        
        ema_old_max_eid.physical_page_number = blkno;
        ema_old_max_eid.local_idx = BufferGetEMAPage(ema_buf)->n_emes - 1;
        
        if(!EMAPage_IS_FULL(BufferGetPage(ema_buf)))
        {
            ReleaseBuffer(ema_buf);
            break;
        }

        ReleaseBuffer(ema_buf);
    }

    eob_old_extent = eob_address_to_eid(eob_old_max_eid);
    ema_old_extent = ema_address_to_eid(ema_old_max_eid);
    if(eob_old_extent != ema_old_extent)
    {
        elog(WARNING, "eob max extentid %d isn't as same as ema max extentid %d.",
                    eob_old_extent,
                    ema_old_extent);
    }

    if(eid < eob_old_extent )
    {
        elog(ERROR, "eob page has error. max extent is %d, but heap is less than extent %d.",
                eob_old_extent, eid);
    }

    if(eid < ema_old_extent )
    {
        elog(ERROR, "ema page has error. max extent is %d, but heap is less than extent %d.",
                ema_old_extent, eid);
    }

    /*
     * Only one eob page and one emapage can be updated at one time.
     * Get the max eid which I can extend at this time
     */
    target_eid = eid;
    if(target_eid - eob_old_extent > EOBS_PER_PAGE)
        target_eid = eob_old_extent + EOBS_PER_PAGE;
    if(target_eid - ema_old_extent > EMES_PER_PAGE)
        target_eid = ema_old_extent + EMES_PER_PAGE;

    eob_new_max_eid = eob_eid_to_address(target_eid);
    ema_new_max_eid = ema_eid_to_address(target_eid);
    
    if(eob_new_max_eid.physical_page_number > eob_old_max_eid.physical_page_number
        && eob_old_max_eid.local_bms_offset < EOBS_PER_PAGE - 1)
    {
        eob_new_max_eid.physical_page_number = eob_old_max_eid.physical_page_number;
        eob_new_max_eid.local_bms_offset = EOBS_PER_PAGE - 1;
        target_eid = eob_address_to_eid(eob_new_max_eid);        
    }

    if(ema_new_max_eid.physical_page_number > ema_old_max_eid.physical_page_number
        && ema_old_max_eid.local_idx < EMES_PER_PAGE - 1)
    {
        ema_new_max_eid.physical_page_number = ema_old_max_eid.physical_page_number;
        ema_new_max_eid.local_idx = EMES_PER_PAGE - 1;
        if(target_eid > ema_address_to_eid(ema_new_max_eid))
            target_eid = ema_address_to_eid(ema_new_max_eid);
    }

    eob_new_max_eid = eob_eid_to_address(target_eid);
    ema_new_max_eid = ema_eid_to_address(target_eid);

    INIT_EXLOG_EXTENDEOB(&xlrec_eob);
    INIT_EXLOG_EXTENDEME(&xlrec_eme);
    
    /* lock eob buffer */
    eob_buf = extent_readbuffer(rel, eob_new_max_eid.physical_page_number, true);
    LockBuffer(eob_buf, BUFFER_LOCK_EXCLUSIVE);
    

    /* lock ema buffer */
    ema_buf = extent_readbuffer(rel, ema_new_max_eid.physical_page_number, true);
    LockBuffer(ema_buf, BUFFER_LOCK_EXCLUSIVE);
    
    START_CRIT_SECTION();
    XLogBeginInsert();

    /* update eob page and write xlog */
    xlrec_eob.slot = eob_new_max_eid.local_bms_offset;
    xlrec_eob.n_eobs = eob_new_max_eid.local_bms_offset + 1;
    xlrec_eob.flags = EXTEND_EOB_FLAGS_SETFREE;
    xlrec_eob.setfree_start = BufferGetEOBPage(eob_buf)->n_bits;
    xlrec_eob.setfree_end = eob_new_max_eid.local_bms_offset;
    
    for(bits = BufferGetEOBPage(eob_buf)->n_bits; bits <= eob_new_max_eid.local_bms_offset; bits++)
        bms_add_member(&BufferGetEOBPage(eob_buf)->eob_bits, bits);    
    BufferGetEOBPage(eob_buf)->n_bits = eob_new_max_eid.local_bms_offset + 1;
    
    /* update ema page and write xlog */
    xlrec_eme.n_emes = ema_new_max_eid.local_idx + 1;
    xlrec_eme.flags = EXTEND_EME_FLAGS_SETFREE;
    xlrec_eme.setfree_start = BufferGetEMAPage(ema_buf)->n_emes;
    xlrec_eme.setfree_end = ema_new_max_eid.local_idx;
    ema_page_extend_eme(BufferGetPage(ema_buf), 
                        ema_new_max_eid.local_idx + 1, 
                        true, 
                        BufferGetEMAPage(ema_buf)->n_emes,
                        ema_new_max_eid.local_idx);
    
    /* write xlog */
    XLogRegisterBuffer(0, eob_buf, REGBUF_KEEP_DATA);
    XLogRegisterBufData(0, (char *)&xlrec_eob, SizeOfExtendEOB);

    XLogRegisterBuffer(1, ema_buf, REGBUF_KEEP_DATA);
    XLogRegisterBufData(1, (char *)&xlrec_eme, SizeOfExtendEME);

    recptr = XLogInsert(RM_EXTENT_ID, XLOG_EXTENT_EXTEND);
    PageSetLSN(BufferGetPage(eob_buf), recptr);
    PageSetLSN(BufferGetPage(ema_buf), recptr);    
    MarkBufferDirty(eob_buf);
    MarkBufferDirty(ema_buf);
    END_CRIT_SECTION();

    UnlockReleaseBuffer(eob_buf);
    UnlockReleaseBuffer(ema_buf);

    if(trace_extent)
    {
        ereport(LOG,
            (errmsg("[trace extent]Extend Extents:[rel:%d/%d/%d]"
                    "[new max eid:%d]"
                    "[extend eob: ema block number:%d, n_bits:%d,"
                    " setfree:%d, setfree_start:%d, setfree_end:%d]"
                    "[extend ema: ema block number:%d, n_emes:%d,"
                    " setfree:%d, setfree_start:%d, setfree_end:%d]",
                    rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
                    target_eid,
                    eob_new_max_eid.physical_page_number, xlrec_eob.n_eobs,
                    xlrec_eob.flags & EXTEND_EOB_FLAGS_SETFREE, xlrec_eob.setfree_start, xlrec_eob.setfree_end,
                    ema_new_max_eid.physical_page_number, xlrec_eme.n_emes,
                    xlrec_eme.flags & EXTEND_EME_FLAGS_SETFREE, xlrec_eme.setfree_start, xlrec_eme.setfree_end)));
    }
    return target_eid;
}


/*
 * these two types of extents can be free:
 *     1. extents belong to hidden shards
 *     2. extents which has no tuples.
 * caller have to make sure only these two types of extent will be freed
 */
void
FreeExtent(Relation rel, ExtentID eid)
{
    //Buffer eob_buf;
    //EOBPage eob_pg;
    //EOBAddress eob_addr;
    //XLogRecPtr    recptr;

    bool    occupied = false;
    ShardID    sid = InvalidShardID;
    
    (void)ema_next_scan(rel, eid, false, &occupied, &sid, NULL, NULL);
    
    if(!occupied)
    {
        return;
    }

    ExtentAssert(ShardIDIsValid(sid));
    
    LockShard(rel, sid, ExclusiveLock);
    shard_remove_extent_from_alloclist(rel, eid, true, false);
    shard_remove_extent_from_scanlist(rel, eid, true, false);

#if 0    
    /*
     * STEP 1: get page of    EOB
     * registred block no: 0
     */
    {
        eob_addr = eob_eid_to_address(eid);    
        
        eob_buf = extent_readbuffer(rel, eob_addr.physical_page_number, true);
        ExtentAssert(BufferIsValid(eob_buf));
        LockBuffer(eob_buf, BUFFER_LOCK_EXCLUSIVE);
        eob_pg = (EOBPage)PageGetContents(BufferGetPage(eob_buf));
    }

    
    /*
     * STEP 2: get page of EME
     * registred block no: 1
     */
    {
        ema_addr = ema_eid_to_address(eid);
    
        ema_buf = extent_readbuffer(rel, ema_addr.physical_page_number, true);
        ExtentAssert(BufferIsValid(ema_buf));

        LockBuffer(ema_buf, BUFFER_LOCK_EXCLUSIVE);
    }

    START_CRIT_SECTION();

    XLogBeginInsert();
    /*
     * set page of EOB 
     */
    {        
        xl_extent_seteob xlrec;
        INIT_EXLOG_SETEOB(&xlrec);
        
        eob_page_mark_extent(eob_pg, eob_addr.local_bms_offset, true);
        MarkBufferDirty(eob_buf);
        /* write xlog */        
        XLogRegisterBuffer(0, eob_buf, REGBUF_KEEP_DATA);
        xlrec.slot = eob_addr.local_bms_offset;
        xlrec.setfree  = true;
        XLogRegisterBufData(0, (char *)&xlrec, SizeOfSetEOB);    
    }

    /*
     * set page of EME
     */
    {
        xl_extent_cleaneme xlrec;
        INIT_EXLOG_CLEANEME(&xlrec);
        
        ema_page_free_eme(BufferGetPage(ema_buf), ema_addr.local_idx);
        MarkBufferDirty(ema_buf);

        /* write xlog */
        XLogRegisterBuffer(1, ema_buf, REGBUF_KEEP_DATA);
        xlrec.slot = ema_addr.local_idx;
        XLogRegisterBufData(1, (char *)&xlrec, SizeOfCleanEME);
    }


    recptr = XLogInsert(RM_EXTENT_ID, XLOG_EXTENT_DETACH_EXTENT);
    PageSetLSN(BufferGetPage(eob_buf), recptr);
    END_CRIT_SECTION();
    UnlockReleaseBuffer(eob_buf);
#endif

    UnlockShard(rel, sid, ExclusiveLock);
}

/*
 * Remove extent from alloclist if it has not enough space to put more tuples in.
 * Shorten alloc list can reduce the time of locating extent for inserting rows. 
 * Only called when freespace changing.
 */
void
MarkExtentFull(Relation rel, ExtentID eid)
{
    shard_remove_extent_from_alloclist(rel, eid, false, true);
}

void
MarkExtentAvailable(Relation rel, ExtentID eid)
{
    shard_add_extent_to_alloclist(rel, eid, false);
}

ExtentID
GetShardScanHead(Relation rel, ShardID sid)
{
    EMAShardAnchor anchor;
    LockShard(rel, sid, AccessShareLock);
    anchor = esa_get_anchor(rel, sid);    
    UnlockShard(rel, sid, AccessShareLock);

    return anchor.scan_head;
}


ExtentID
RelOidGetShardScanHead(Oid reloid, ShardID sid)
{
    ExtentID scanhead;
    Relation rel = heap_open(reloid, AccessShareLock);
    scanhead = GetShardScanHead(rel, sid);
    heap_close(rel, AccessShareLock);
    return scanhead;
}

#if 0
static int
next_free_extent(EOBPage eob_pg, int search_from)
{
    int word_idx;
    int bit_offset = -1;
    int word_slot = search_from / EOBWord_BITLEN ;

    for(word_idx = word_slot; word_idx < EOBWORDS_PER_PAGE; word_idx++)
    {
        if (eob_pg->eob_bits[word_idx] != 0xFFFFFFFF)
        {
            bit_offset = find_first0_of_int(eob_pg->eob_bits[word_idx]);            
            break;
        }

        if(word_idx * EOBWord_BITLEN + bit_offset >= eob_pg->n_bits)
        {
            break;
        }
    }

    if(bit_offset < 0)
        return bit_offset;
    else
        return word_idx * EOBWord_BITLEN + bit_offset;
}


static uint32 find_zero_filters[] = {0xFFFF, 0xFF, 0xF, 0x3, 0x1};
static uint16 find_zero_offsets[] = {16, 8, 4, 2, 1};

static int
find_first0_of_int(EOBWord word)
{
    uint32 seg;
    int i=0;
    int     bit_offset = 0;
    int        ites = sizeof(find_zero_filters) / sizeof(int32);
    seg = word;

    if( word == 0xFFFFFFFF)
        return -1;    
    
    while ( i < ites )
    {
        if((seg & find_zero_filters[i]) == find_zero_filters[i])
        {
            bit_offset += find_zero_offsets[i];
            seg = (seg >> find_zero_offsets[i]) & find_zero_filters[i];
        }
        else
        {
            seg = seg & find_zero_filters[i];
        }
        
        i++;
    }

    return bit_offset;
}
#endif


/*-------------------------------------------------------------------------------------------
 *  functions for user to inspect extent info
 *-------------------------------------------------------------------------------------------
 */

typedef struct
{
    int     eid;
    bool     is_occupied;
    int     shardid;
    int        freespace_cat;
    int        hwm;
    int        scan_next;
    int        scan_prev;
    int        alloc_next;
    int        alloc_prev;
}MctxStat;

typedef struct
{
    int    currIdx;
    int length;
    ExtentID last_eid;
    MctxStat    mctxstat[EMES_PER_PAGE];    
} ShmMgr_State;

Datum pg_extent_info_oid(PG_FUNCTION_ARGS)
{// #lizard forgives
#define EXTENT_STAT_COLUMN_NUM 9
    Oid                relOid = PG_GETARG_OID(0);
    FuncCallContext *funcctx = NULL;
    ShmMgr_State    *mctx_status  = NULL;

    if (SRF_IS_FIRSTCALL())
    {        
        TupleDesc   tupdesc;
        MemoryContext oldcontext;
        
        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
                * Switch to memory context appropriate for multiple function calls
                */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples */
        tupdesc = CreateTemplateTupleDesc(EXTENT_STAT_COLUMN_NUM, false);
        TupleDescInitEntry(tupdesc, (AttrNumber) 1, "eid",
                         INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 2, "is_occupied",
                         BOOLOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 3, "shardid",
                         INT4OID, -1, 0);        
        TupleDescInitEntry(tupdesc, (AttrNumber) 4, "freespace_cat",
                         INT4OID, -1, 0);        
        TupleDescInitEntry(tupdesc, (AttrNumber) 5, "hwm",
                         INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 6, "scan_next",
                         INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 7, "scan_prev",
                         INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 8, "alloc_next",
                         INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 9, "alloc_prev",
                         INT4OID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        mctx_status = (ShmMgr_State *) palloc(sizeof(ShmMgr_State));
        funcctx->user_fctx = (void *) mctx_status;

        mctx_status->currIdx = 0;

        MemoryContextSwitchTo(oldcontext);
    }

      funcctx = SRF_PERCALL_SETUP();
    mctx_status  = (ShmMgr_State *) funcctx->user_fctx;


    if(mctx_status->currIdx < MAX_EXTENTS)
    {
           Datum        values[EXTENT_STAT_COLUMN_NUM];
        bool        nulls[EXTENT_STAT_COLUMN_NUM];
        HeapTuple    tuple;
        Datum        result;
        MctxStat    *stat;

        if(mctx_status->currIdx % EMES_PER_PAGE == 0)
        {
            EMAAddress    addr;
            Buffer         buf;
            EMAPage        pg;
            Relation    rel = heap_open(relOid, AccessShareLock);
            int         i;

            if(!RelationHasExtent(rel))
            {
                heap_close(rel, AccessShareLock);
                SRF_RETURN_DONE(funcctx);
            }
            addr = ema_eid_to_address(mctx_status->currIdx);
            buf = extent_readbuffer(rel, addr.physical_page_number, false);
            if(BufferIsInvalid(buf))
            {
                heap_close(rel,AccessShareLock);
                SRF_RETURN_DONE(funcctx);
            }
            
            LockBuffer(buf, BUFFER_LOCK_SHARE);

            pg = (EMAPage)PageGetContents(BufferGetPage(buf));

            if(pg->n_emes <= 0)
            {
                UnlockReleaseBuffer(buf);
                heap_close(rel, AccessShareLock);
                SRF_RETURN_DONE(funcctx);
            }
            
            mctx_status->length = pg->n_emes;

            for(i=0; i<pg->n_emes; i++)
            {
                mctx_status->mctxstat[i].eid = mctx_status->currIdx + i;
                mctx_status->mctxstat[i].is_occupied = (bool)pg->ema[i].is_occupied;
                mctx_status->mctxstat[i].shardid = pg->ema[i].shardid;
                mctx_status->mctxstat[i].freespace_cat = pg->ema[i].max_freespace;
                mctx_status->mctxstat[i].hwm = pg->ema[i].hwm;
                mctx_status->mctxstat[i].scan_next = pg->ema[i].scan_next;
                mctx_status->mctxstat[i].scan_prev = pg->ema[i].scan_prev;
                mctx_status->mctxstat[i].alloc_next = pg->ema[i].alloc_next;
                mctx_status->mctxstat[i].alloc_prev = EMEGetAllocPrev(&pg->ema[i]);
            }

            UnlockReleaseBuffer(buf);
            heap_close(rel, AccessShareLock);
        }

        if(mctx_status->length < EMES_PER_PAGE 
            && (mctx_status->currIdx % EMES_PER_PAGE) >= mctx_status->length)
        {
            SRF_RETURN_DONE(funcctx);
        }

        stat = &mctx_status->mctxstat[mctx_status->currIdx % EMES_PER_PAGE];

        MemSet(values, 0, sizeof(values));
        MemSet(nulls,  0, sizeof(nulls));

        if(!ExtentIdIsValid(stat->scan_next))
            nulls[5] = true;
        if(!ExtentIdIsValid(stat->scan_prev))
            nulls[6] = true;
        if(!ExtentIdIsValid(stat->alloc_next))
            nulls[7] = true;
        if(!ExtentIdIsValid(stat->alloc_prev))
            nulls[8] = true;

        values[0] = Int32GetDatum(stat->eid);
        values[1] = BoolGetDatum(stat->is_occupied);
        values[2] = Int32GetDatum(stat->shardid);        
        values[3] = Int32GetDatum(stat->freespace_cat);
        values[4] = Int32GetDatum(stat->hwm);
        values[5] = Int32GetDatum(stat->scan_next);
        values[6] = Int32GetDatum(stat->scan_prev);
        values[7] = Int32GetDatum(stat->alloc_next);
        values[8] = Int32GetDatum(stat->alloc_prev);

        mctx_status->currIdx++;
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    }

    SRF_RETURN_DONE(funcctx);   
}

Datum pg_extent_info_relname(PG_FUNCTION_ARGS)
{// #lizard forgives
#define EXTENT_STAT_COLUMN_NUM 9
    char            *relname = text_to_cstring(PG_GETARG_TEXT_P(0));
    Oid             relOid = InvalidOid;
    FuncCallContext *funcctx = NULL;
    ShmMgr_State    *mctx_status  = NULL;

    relOid = string_to_reloid(relname);
    
    if (SRF_IS_FIRSTCALL())
    {        
        TupleDesc   tupdesc;
        MemoryContext oldcontext;
        
        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
                * Switch to memory context appropriate for multiple function calls
                */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples */
        tupdesc = CreateTemplateTupleDesc(EXTENT_STAT_COLUMN_NUM, false);
        TupleDescInitEntry(tupdesc, (AttrNumber) 1, "eid",
                         INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 2, "is_occupied",
                         BOOLOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 3, "shardid",
                         INT4OID, -1, 0);        
        TupleDescInitEntry(tupdesc, (AttrNumber) 4, "freespace_cat",
                         INT4OID, -1, 0);        
        TupleDescInitEntry(tupdesc, (AttrNumber) 5, "hwm",
                         INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 6, "scan_next",
                         INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 7, "scan_prev",
                         INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 8, "alloc_next",
                         INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 9, "alloc_prev",
                         INT4OID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        mctx_status = (ShmMgr_State *) palloc(sizeof(ShmMgr_State));
        funcctx->user_fctx = (void *) mctx_status;

        mctx_status->currIdx = 0;

        MemoryContextSwitchTo(oldcontext);
    }

      funcctx = SRF_PERCALL_SETUP();
    mctx_status  = (ShmMgr_State *) funcctx->user_fctx;


    if(mctx_status->currIdx < MAX_EXTENTS)
    {
           Datum        values[EXTENT_STAT_COLUMN_NUM];
        bool        nulls[EXTENT_STAT_COLUMN_NUM];
        HeapTuple    tuple;
        Datum        result;
        MctxStat    *stat;

        if(mctx_status->currIdx % EMES_PER_PAGE == 0)
        {
            EMAAddress    addr;
            Buffer         buf;
            EMAPage        pg;
            Relation    rel = heap_open(relOid, AccessShareLock);
            int         i;

            if(!RelationHasExtent(rel))
            {
                heap_close(rel, AccessShareLock);
                SRF_RETURN_DONE(funcctx);
            }
            addr = ema_eid_to_address(mctx_status->currIdx);
            buf = extent_readbuffer(rel, addr.physical_page_number, true);
            if(BufferIsInvalid(buf))
            {
                heap_close(rel,AccessShareLock);
                SRF_RETURN_DONE(funcctx);
            }
            
            LockBuffer(buf, BUFFER_LOCK_SHARE);

            pg = (EMAPage)PageGetContents(BufferGetPage(buf));

            if(pg->n_emes <= 0)
            {
                UnlockReleaseBuffer(buf);
                heap_close(rel, AccessShareLock);
                SRF_RETURN_DONE(funcctx);
            }
            
            mctx_status->length = pg->n_emes;

            for(i=0; i<pg->n_emes; i++)
            {
                mctx_status->mctxstat[i].eid = mctx_status->currIdx + i;
                mctx_status->mctxstat[i].is_occupied = (bool)pg->ema[i].is_occupied;
                mctx_status->mctxstat[i].shardid = pg->ema[i].shardid;
                mctx_status->mctxstat[i].freespace_cat = pg->ema[i].max_freespace;
                mctx_status->mctxstat[i].hwm = pg->ema[i].hwm;
                mctx_status->mctxstat[i].scan_next = pg->ema[i].scan_next;
                mctx_status->mctxstat[i].scan_prev = pg->ema[i].scan_prev;
                mctx_status->mctxstat[i].alloc_next = pg->ema[i].alloc_next;
                mctx_status->mctxstat[i].alloc_prev = EMEGetAllocPrev(&pg->ema[i]);
            }

            UnlockReleaseBuffer(buf);
            heap_close(rel, AccessShareLock);
        }

        if(mctx_status->length < EMES_PER_PAGE 
            && (mctx_status->currIdx % EMES_PER_PAGE) >= mctx_status->length)
        {
            SRF_RETURN_DONE(funcctx);
        }

        stat = &mctx_status->mctxstat[mctx_status->currIdx % EMES_PER_PAGE];

        MemSet(values, 0, sizeof(values));
        MemSet(nulls,  0, sizeof(nulls));

        if(!ExtentIdIsValid(stat->scan_next))
            nulls[5] = true;
        if(!ExtentIdIsValid(stat->scan_prev))
            nulls[6] = true;
        if(!ExtentIdIsValid(stat->alloc_next))
            nulls[7] = true;
        if(!ExtentIdIsValid(stat->alloc_prev))
            nulls[8] = true;

        values[0] = Int32GetDatum(stat->eid);
        values[1] = BoolGetDatum(stat->is_occupied);
        values[2] = Int32GetDatum(stat->shardid);        
        values[3] = Int32GetDatum(stat->freespace_cat);
        values[4] = Int32GetDatum(stat->hwm);
        values[5] = Int32GetDatum(stat->scan_next);
        values[6] = Int32GetDatum(stat->scan_prev);
        values[7] = Int32GetDatum(stat->alloc_next);
        values[8] = Int32GetDatum(stat->alloc_prev);

        mctx_status->currIdx++;
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    }

    SRF_RETURN_DONE(funcctx);   
}

Datum pg_shard_scan_list_oid(PG_FUNCTION_ARGS)
{// #lizard forgives
#define SHARD_SCAN_COLUMN_NUM 6
    Oid                relOid = PG_GETARG_OID(0);
    ShardID            sid =  PG_GETARG_INT32(1);
    FuncCallContext *funcctx = NULL;
    ShmMgr_State    *mctx_status  = NULL;
    Relation        rel;
    EMAShardAnchor    anchor;

    if(!ShardIDIsValid(sid))
    {
        elog(ERROR, "shardid %d is invalid.", sid);
    }
    
    if (SRF_IS_FIRSTCALL())
    {        
        TupleDesc   tupdesc;
        MemoryContext oldcontext;
        
        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
                * Switch to memory context appropriate for multiple function calls
                */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples */
        tupdesc = CreateTemplateTupleDesc(SHARD_SCAN_COLUMN_NUM, false);
        TupleDescInitEntry(tupdesc, (AttrNumber) 1, "eid",
                         INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 2, "is_occupied",
                         BOOLOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 3, "shardid",
                         INT4OID, -1, 0);        
        TupleDescInitEntry(tupdesc, (AttrNumber) 4, "freespace_cat",
                         INT4OID, -1, 0);        
        TupleDescInitEntry(tupdesc, (AttrNumber) 5, "hwm",
                         INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 6, "scan_next",
                         INT4OID, -1, 0);
        
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        mctx_status = (ShmMgr_State *) palloc(sizeof(ShmMgr_State));
        funcctx->user_fctx = (void *) mctx_status;

        mctx_status->currIdx = 0;
        mctx_status->length = 0;

        MemoryContextSwitchTo(oldcontext);

        rel = heap_open(relOid, AccessShareLock);
        anchor = esa_get_anchor(rel, sid);
        if(!ExtentIdIsValid(anchor.scan_head))
        {
            heap_close(rel, AccessShareLock);
            SRF_RETURN_DONE(funcctx);
        }
        else
        {
            mctx_status->last_eid = anchor.scan_head;
        }

        heap_close(rel, AccessShareLock);
    }

      funcctx = SRF_PERCALL_SETUP();
    mctx_status  = (ShmMgr_State *) funcctx->user_fctx;

    if(mctx_status->currIdx >= mctx_status->length && !ExtentIdIsValid(mctx_status->last_eid))
    {
        SRF_RETURN_DONE(funcctx);
    }
        
    rel = heap_open(relOid, AccessShareLock);
    LockShard(rel, sid, AccessShareLock);    

    if(mctx_status->length == 0)
    {
        int i = 0;
        ExtentID curr = mctx_status->last_eid;
        ExtentID next;
        for(i = 0; i<EMES_PER_PAGE; i++)
        {
            bool     is_occupied;
            ShardID extent_sid;
            int        hwm;
            uint8     freespace;
            
            next = ema_next_scan(rel, curr, false, &is_occupied, &extent_sid, &hwm, &freespace);

            mctx_status->mctxstat[i].eid = curr;
            mctx_status->mctxstat[i].is_occupied = is_occupied;
            mctx_status->mctxstat[i].shardid = extent_sid;
            mctx_status->mctxstat[i].freespace_cat = freespace;
            mctx_status->mctxstat[i].hwm = hwm;
            mctx_status->mctxstat[i].scan_next = next;
            mctx_status->mctxstat[i].scan_prev = InvalidExtentID;
            mctx_status->mctxstat[i].alloc_next = InvalidExtentID;
            mctx_status->mctxstat[i].alloc_prev = InvalidExtentID;

            mctx_status->length++;
            if(!ExtentIdIsValid(next))
            {
                mctx_status->last_eid = InvalidExtentID;
                break;
            }
            curr = next;
        }

        mctx_status->currIdx = 0;
        if(mctx_status->length == EMES_PER_PAGE)
            mctx_status->last_eid = next;
    }

    UnlockShard(rel, sid, AccessShareLock);
    heap_close(rel, AccessShareLock);

    {
           Datum        values[SHARD_SCAN_COLUMN_NUM];
        bool        nulls[SHARD_SCAN_COLUMN_NUM];
        HeapTuple    tuple;
        Datum        result;
        MctxStat    *stat;

        stat = &mctx_status->mctxstat[mctx_status->currIdx];

        MemSet(values, 0, sizeof(values));
        MemSet(nulls,  0, sizeof(nulls));

        if(!ExtentIdIsValid(stat->scan_next))
            nulls[5] = true;

        values[0] = Int32GetDatum(stat->eid);
        values[1] = BoolGetDatum(stat->is_occupied);
        values[2] = Int32GetDatum(stat->shardid);        
        values[3] = Int32GetDatum(stat->freespace_cat);
        values[4] = Int32GetDatum(stat->hwm);
        values[5] = Int32GetDatum(stat->scan_next);

        mctx_status->currIdx++;
        if(mctx_status->currIdx >= mctx_status->length)
        {
            mctx_status->length = 0;
            mctx_status->currIdx = 0;
        }
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    }
}

Datum pg_shard_scan_list_relname(PG_FUNCTION_ARGS)
{// #lizard forgives
#define SHARD_SCAN_COLUMN_NUM 6
    char            *relname = text_to_cstring(PG_GETARG_TEXT_P(0));
    ShardID            sid =  PG_GETARG_INT32(1);
    FuncCallContext *funcctx = NULL;
    ShmMgr_State    *mctx_status  = NULL;
    Relation        rel;
    EMAShardAnchor    anchor;
    Oid                relOid = InvalidOid;

    if(!ShardIDIsValid(sid))
    {
        elog(ERROR, "shardid %d is invalid.", sid);
    }

    relOid = string_to_reloid(relname);
    
    if (SRF_IS_FIRSTCALL())
    {        
        TupleDesc   tupdesc;
        MemoryContext oldcontext;
        
        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
                * Switch to memory context appropriate for multiple function calls
                */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples */
        tupdesc = CreateTemplateTupleDesc(SHARD_SCAN_COLUMN_NUM, false);
        TupleDescInitEntry(tupdesc, (AttrNumber) 1, "eid",
                         INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 2, "is_occupied",
                         BOOLOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 3, "shardid",
                         INT4OID, -1, 0);        
        TupleDescInitEntry(tupdesc, (AttrNumber) 4, "freespace_cat",
                         INT4OID, -1, 0);        
        TupleDescInitEntry(tupdesc, (AttrNumber) 5, "hwm",
                         INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 6, "scan_next",
                         INT4OID, -1, 0);
        
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        mctx_status = (ShmMgr_State *) palloc(sizeof(ShmMgr_State));
        funcctx->user_fctx = (void *) mctx_status;

        mctx_status->currIdx = 0;
        mctx_status->length = 0;

        MemoryContextSwitchTo(oldcontext);

        rel = heap_open(relOid, AccessShareLock);
        anchor = esa_get_anchor(rel, sid);
        if(!ExtentIdIsValid(anchor.scan_head))
        {
            heap_close(rel, AccessShareLock);
            SRF_RETURN_DONE(funcctx);
        }
        else
        {
            mctx_status->last_eid = anchor.scan_head;
        }

        heap_close(rel, AccessShareLock);
    }

      funcctx = SRF_PERCALL_SETUP();
    mctx_status  = (ShmMgr_State *) funcctx->user_fctx;

    if(mctx_status->currIdx >= mctx_status->length && !ExtentIdIsValid(mctx_status->last_eid))
    {
        SRF_RETURN_DONE(funcctx);
    }
        
    rel = heap_open(relOid, AccessShareLock);
    LockShard(rel, sid, AccessShareLock);    

    if(mctx_status->length == 0)
    {
        int i = 0;
        ExtentID curr = mctx_status->last_eid;
        ExtentID next;
        for(i = 0; i<EMES_PER_PAGE; i++)
        {
            bool     is_occupied;
            ShardID extent_sid;
            int        hwm;
            uint8     freespace;
            
            next = ema_next_scan(rel, curr, false, &is_occupied, &extent_sid, &hwm, &freespace);

            mctx_status->mctxstat[i].eid = curr;
            mctx_status->mctxstat[i].is_occupied = is_occupied;
            mctx_status->mctxstat[i].shardid = extent_sid;
            mctx_status->mctxstat[i].freespace_cat = freespace;
            mctx_status->mctxstat[i].hwm = hwm;
            mctx_status->mctxstat[i].scan_next = next;
            mctx_status->mctxstat[i].scan_prev = InvalidExtentID;
            mctx_status->mctxstat[i].alloc_next = InvalidExtentID;
            mctx_status->mctxstat[i].alloc_prev = InvalidExtentID;

            mctx_status->length++;
            if(!ExtentIdIsValid(next))
            {
                mctx_status->last_eid = InvalidExtentID;
                break;
            }
            curr = next;
        }

        mctx_status->currIdx = 0;
        if(mctx_status->length == EMES_PER_PAGE)
            mctx_status->last_eid = next;
    }

    UnlockShard(rel, sid, AccessShareLock);
    heap_close(rel, AccessShareLock);

    {
           Datum        values[SHARD_SCAN_COLUMN_NUM];
        bool        nulls[SHARD_SCAN_COLUMN_NUM];
        HeapTuple    tuple;
        Datum        result;
        MctxStat    *stat;

        stat = &mctx_status->mctxstat[mctx_status->currIdx];

        MemSet(values, 0, sizeof(values));
        MemSet(nulls,  0, sizeof(nulls));

        if(!ExtentIdIsValid(stat->scan_next))
            nulls[5] = true;

        values[0] = Int32GetDatum(stat->eid);
        values[1] = BoolGetDatum(stat->is_occupied);
        values[2] = Int32GetDatum(stat->shardid);        
        values[3] = Int32GetDatum(stat->freespace_cat);
        values[4] = Int32GetDatum(stat->hwm);
        values[5] = Int32GetDatum(stat->scan_next);

        mctx_status->currIdx++;
        if(mctx_status->currIdx >= mctx_status->length)
        {
            mctx_status->length = 0;
            mctx_status->currIdx = 0;
        }
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    }
}

Datum pg_shard_alloc_list_oid(PG_FUNCTION_ARGS)
{// #lizard forgives
#define SHARD_SCAN_COLUMN_NUM 6
    Oid                relOid = PG_GETARG_OID(0);
    ShardID            sid =  PG_GETARG_INT32(1);
    FuncCallContext *funcctx = NULL;
    ShmMgr_State    *mctx_status  = NULL;
    Relation        rel;
    EMAShardAnchor    anchor;

    if(!ShardIDIsValid(sid))
    {
        elog(ERROR, "shardid %d is invalid.", sid);
    }
    
    if (SRF_IS_FIRSTCALL())
    {        
        TupleDesc   tupdesc;
        MemoryContext oldcontext;
        
        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
                * Switch to memory context appropriate for multiple function calls
                */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples */
        tupdesc = CreateTemplateTupleDesc(SHARD_SCAN_COLUMN_NUM, false);
        TupleDescInitEntry(tupdesc, (AttrNumber) 1, "eid",
                         INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 2, "is_occupied",
                         BOOLOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 3, "shardid",
                         INT4OID, -1, 0);        
        TupleDescInitEntry(tupdesc, (AttrNumber) 4, "freespace_cat",
                         INT4OID, -1, 0);        
        TupleDescInitEntry(tupdesc, (AttrNumber) 5, "hwm",
                         INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 6, "alloc_next",
                         INT4OID, -1, 0);
        
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        mctx_status = (ShmMgr_State *) palloc(sizeof(ShmMgr_State));
        funcctx->user_fctx = (void *) mctx_status;

        mctx_status->currIdx = 0;
        mctx_status->length = 0;

        MemoryContextSwitchTo(oldcontext);

        rel = heap_open(relOid, AccessShareLock);
        anchor = esa_get_anchor(rel, sid);
        if(!ExtentIdIsValid(anchor.alloc_head))
        {
            heap_close(rel, AccessShareLock);
            SRF_RETURN_DONE(funcctx);
        }
        else
        {
            mctx_status->last_eid = anchor.alloc_head;
        }

        heap_close(rel, AccessShareLock);
    }

      funcctx = SRF_PERCALL_SETUP();
    mctx_status  = (ShmMgr_State *) funcctx->user_fctx;

    if(mctx_status->currIdx >= mctx_status->length && !ExtentIdIsValid(mctx_status->last_eid))
    {
        SRF_RETURN_DONE(funcctx);
    }
        
    rel = heap_open(relOid, AccessShareLock);
    LockShard(rel, sid, AccessShareLock);    

    if(mctx_status->length == 0)
    {
        int i = 0;
        ExtentID curr = mctx_status->last_eid;
        ExtentID next;
        for(i = 0; i<EMES_PER_PAGE; i++)
        {
            bool     is_occupied;
            ShardID extent_sid;
            int        hwm;
            uint8     freespace;
            
            next = ema_next_alloc(rel, curr, false, &is_occupied, &extent_sid, &hwm, &freespace);

            mctx_status->mctxstat[i].eid = curr;
            mctx_status->mctxstat[i].is_occupied = is_occupied;
            mctx_status->mctxstat[i].shardid = extent_sid;
            mctx_status->mctxstat[i].freespace_cat = freespace;
            mctx_status->mctxstat[i].hwm = hwm;
            mctx_status->mctxstat[i].scan_next = InvalidExtentID;
            mctx_status->mctxstat[i].scan_prev = InvalidExtentID;
            mctx_status->mctxstat[i].alloc_next = next;
            mctx_status->mctxstat[i].alloc_prev = InvalidExtentID;

            mctx_status->length++;
            if(!ExtentIdIsValid(next))
            {
                mctx_status->last_eid = InvalidExtentID;
                break;
            }
            curr = next;
        }

        mctx_status->currIdx = 0;
        if(mctx_status->length == EMES_PER_PAGE)
            mctx_status->last_eid = next;
    }

    UnlockShard(rel, sid, AccessShareLock);
    heap_close(rel, AccessShareLock);

    {
           Datum        values[SHARD_SCAN_COLUMN_NUM];
        bool        nulls[SHARD_SCAN_COLUMN_NUM];
        HeapTuple    tuple;
        Datum        result;
        MctxStat    *stat;

        stat = &mctx_status->mctxstat[mctx_status->currIdx];

        MemSet(values, 0, sizeof(values));
        MemSet(nulls,  0, sizeof(nulls));

        if(!ExtentIdIsValid(stat->alloc_next))
            nulls[5] = true;

        values[0] = Int32GetDatum(stat->eid);
        values[1] = BoolGetDatum(stat->is_occupied);
        values[2] = Int32GetDatum(stat->shardid);        
        values[3] = Int32GetDatum(stat->freespace_cat);
        values[4] = Int32GetDatum(stat->hwm);
        values[5] = Int32GetDatum(stat->alloc_next);

        mctx_status->currIdx++;
        if(mctx_status->currIdx >= mctx_status->length)
        {
            mctx_status->length = 0;
            mctx_status->currIdx = 0;
        }
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    }
}

Datum pg_shard_alloc_list_relname(PG_FUNCTION_ARGS)
{// #lizard forgives
#define SHARD_SCAN_COLUMN_NUM 6
    char            *relname = text_to_cstring(PG_GETARG_TEXT_P(0));
    ShardID            sid =  PG_GETARG_INT32(1);
    FuncCallContext *funcctx = NULL;
    ShmMgr_State    *mctx_status  = NULL;
    Relation        rel;
    EMAShardAnchor    anchor;
    Oid                relOid = InvalidOid;

    if(!ShardIDIsValid(sid))
    {
        elog(ERROR, "shardid %d is invalid.", sid);
    }

    relOid = string_to_reloid(relname);
    
    if (SRF_IS_FIRSTCALL())
    {        
        TupleDesc   tupdesc;
        MemoryContext oldcontext;
        
        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
                * Switch to memory context appropriate for multiple function calls
                */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples */
        tupdesc = CreateTemplateTupleDesc(SHARD_SCAN_COLUMN_NUM, false);
        TupleDescInitEntry(tupdesc, (AttrNumber) 1, "eid",
                         INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 2, "is_occupied",
                         BOOLOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 3, "shardid",
                         INT4OID, -1, 0);        
        TupleDescInitEntry(tupdesc, (AttrNumber) 4, "freespace_cat",
                         INT4OID, -1, 0);        
        TupleDescInitEntry(tupdesc, (AttrNumber) 5, "hwm",
                         INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 6, "alloc_next",
                         INT4OID, -1, 0);
        
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        mctx_status = (ShmMgr_State *) palloc(sizeof(ShmMgr_State));
        funcctx->user_fctx = (void *) mctx_status;

        mctx_status->currIdx = 0;
        mctx_status->length = 0;

        MemoryContextSwitchTo(oldcontext);

        rel = heap_open(relOid, AccessShareLock);
        anchor = esa_get_anchor(rel, sid);
        if(!ExtentIdIsValid(anchor.alloc_head))
        {
            heap_close(rel, AccessShareLock);
            SRF_RETURN_DONE(funcctx);
        }
        else
        {
            mctx_status->last_eid = anchor.alloc_head;
        }

        heap_close(rel, AccessShareLock);
    }

    funcctx = SRF_PERCALL_SETUP();
    mctx_status  = (ShmMgr_State *)funcctx->user_fctx;

    if(mctx_status->currIdx >= mctx_status->length && !ExtentIdIsValid(mctx_status->last_eid))
    {
        SRF_RETURN_DONE(funcctx);
    }
        
    rel = heap_open(relOid,  AccessShareLock);
    LockShard(rel, sid,  AccessShareLock);    

    if(mctx_status->length == 0)
    {
        ExtentID curr = mctx_status->last_eid;
        ExtentID next;
        int i = 0;
        for(i = 0; i<EMES_PER_PAGE; i++)
        {
            bool     is_occupied;
            ShardID extent_sid;
            int        hwm;
            uint8     freespace;
            
            next = ema_next_alloc(rel, curr, false, &is_occupied, &extent_sid, &hwm, &freespace);

            mctx_status->mctxstat[i].eid = curr;
            mctx_status->mctxstat[i].is_occupied = is_occupied;
            mctx_status->mctxstat[i].shardid = extent_sid;
            mctx_status->mctxstat[i].freespace_cat = freespace;
            mctx_status->mctxstat[i].hwm = hwm;
            mctx_status->mctxstat[i].scan_next = InvalidExtentID;
            mctx_status->mctxstat[i].scan_prev = InvalidExtentID;
            mctx_status->mctxstat[i].alloc_prev = InvalidExtentID;
            mctx_status->mctxstat[i].alloc_next = next;

            mctx_status->length++;
            if(!ExtentIdIsValid(next))
            {
                mctx_status->last_eid = InvalidExtentID;
                break;
            }
            curr = next;
        }

        mctx_status->currIdx = 0;
        if(mctx_status->length == EMES_PER_PAGE)
        {
            mctx_status->last_eid = next;
        }
    }

    UnlockShard(rel, sid, AccessShareLock);
    heap_close(rel, AccessShareLock);

    {
        HeapTuple    tuple;
        Datum        result;
        Datum       values[SHARD_SCAN_COLUMN_NUM];
        bool        nulls[SHARD_SCAN_COLUMN_NUM];
        MctxStat    *stat;

        stat = &mctx_status->mctxstat[mctx_status->currIdx];

        MemSet(values, 0, sizeof(values));
        MemSet(nulls,  0, sizeof(nulls));

        if(!ExtentIdIsValid(stat->alloc_next))
            nulls[5] = true;

        values[0] = Int32GetDatum(stat->eid);
        values[1] = BoolGetDatum(stat->is_occupied);
        values[2] = Int32GetDatum(stat->shardid);        
        values[3] = Int32GetDatum(stat->freespace_cat);
        values[4] = Int32GetDatum(stat->hwm);
        values[5] = Int32GetDatum(stat->alloc_next);

        mctx_status->currIdx++;
        if(mctx_status->currIdx >= mctx_status->length)
        {
            mctx_status->length = 0;
            mctx_status->currIdx = 0;
        }
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    }
}

typedef struct
{
    int inited;
    int    currIdx;
    EMAShardAnchor esa[MAX_SHARDS];    
} ESA_State;

#define ESA_STAT_COLUMN_NUM 7
Datum pg_shard_anchor_oid(PG_FUNCTION_ARGS)
{
    Oid                relOid = PG_GETARG_OID(0);
    FuncCallContext *funcctx = NULL;
    ESA_State   *esa_status  = NULL;
    Datum        values[ESA_STAT_COLUMN_NUM];
    bool        nulls[ESA_STAT_COLUMN_NUM];
    HeapTuple    tuple;
    Datum        result;
    ESAAddress     addr;

    if (SRF_IS_FIRSTCALL())
    {        
        TupleDesc   tupdesc;
        MemoryContext oldcontext;
        
        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
                * Switch to memory context appropriate for multiple function calls
                */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples */
        tupdesc = CreateTemplateTupleDesc(ESA_STAT_COLUMN_NUM, false);
        TupleDescInitEntry(tupdesc, (AttrNumber) 1, "shardid",
                         INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 2, "pageno",
                         INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 3, "slotinpage",
                         INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 4, "scanhead",
                         INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 5, "scantail",
                         INT4OID, -1, 0);        
        TupleDescInitEntry(tupdesc, (AttrNumber) 6, "allochead",
                         INT4OID, -1, 0);        
        TupleDescInitEntry(tupdesc, (AttrNumber) 7, "alloctail",
                         INT4OID, -1, 0);


        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        esa_status = (ESA_State *) palloc(sizeof(ESA_State));
        funcctx->user_fctx = (void *) esa_status;

        esa_status->currIdx = 0;
        esa_status->inited = false;

        MemoryContextSwitchTo(oldcontext);
    }

      funcctx = SRF_PERCALL_SETUP();
    esa_status  = (ESA_State *) funcctx->user_fctx;

    if(!esa_status->inited)
    {
        int         shardidx;
        Relation     rel;

        rel = heap_open(relOid, AccessShareLock);

        if( !RelationHasExtent(rel) )
        {
            elog(ERROR, "relation %d is not organized with extent.", RelationGetRelid(rel));
        }
                
        for(shardidx = 0;  shardidx < MAX_SHARDS; shardidx++)
        {
            esa_status->esa[shardidx] = esa_get_anchor(rel, shardidx);
        }        
        heap_close(rel,  AccessShareLock);
    }

    if(esa_status->currIdx >= MAX_SHARDS)
        SRF_RETURN_DONE(funcctx);

    MemSet(nulls,  0, sizeof(nulls));
    MemSet(values, 0, sizeof(values));

    addr = esa_sid_to_address(esa_status->currIdx);
    values[0] = Int32GetDatum(esa_status->currIdx);
    values[1] = Int32GetDatum(addr.physical_page_number);
    values[2] = Int32GetDatum(addr.local_idx);
    values[3] = Int32GetDatum(esa_status->esa[esa_status->currIdx].scan_head);
    values[4] = Int32GetDatum(esa_status->esa[esa_status->currIdx].scan_tail);
    values[5] = Int32GetDatum(esa_status->esa[esa_status->currIdx].alloc_head);
    values[6] = Int32GetDatum(esa_status->esa[esa_status->currIdx].alloc_tail);

    esa_status->currIdx++;
    tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
    result = HeapTupleGetDatum(tuple);
    SRF_RETURN_NEXT(funcctx, result);
}

Datum pg_shard_anchor_relname(PG_FUNCTION_ARGS)
{
    HeapTuple    tuple;
    Datum        result;
    ESAAddress     addr;
    char         *relname = text_to_cstring(PG_GETARG_TEXT_P(0));
    Oid            relOid = string_to_reloid(relname);    
    FuncCallContext *funcctx = NULL;
    ESA_State   *esa_status  = NULL;
    Datum        values[ESA_STAT_COLUMN_NUM];
    bool        nulls[ESA_STAT_COLUMN_NUM];

    if (SRF_IS_FIRSTCALL())
    {        
        MemoryContext oldcontext;
        TupleDesc   tupdesc;
        
        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
                * Switch to memory context appropriate for multiple function calls
                */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples */
        tupdesc = CreateTemplateTupleDesc(ESA_STAT_COLUMN_NUM, false);
        TupleDescInitEntry( tupdesc, (AttrNumber) 1, "shardid",
                         INT4OID, -1, 0);
        TupleDescInitEntry( tupdesc, (AttrNumber) 2, "pageno",
                         INT4OID, -1, 0);
        TupleDescInitEntry( tupdesc, (AttrNumber) 3, "slotinpage",
                         INT4OID, -1, 0);
        TupleDescInitEntry( tupdesc, (AttrNumber) 4, "scanhead",
                         INT4OID, -1, 0);
        TupleDescInitEntry( tupdesc, (AttrNumber) 5, "scantail",
                         INT4OID, -1, 0);        
        TupleDescInitEntry( tupdesc, (AttrNumber) 6, "allochead",
                         INT4OID, -1, 0);        
        TupleDescInitEntry( tupdesc, (AttrNumber) 7, "alloctail",
                         INT4OID, -1, 0);


        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        esa_status = (ESA_State *)palloc(sizeof(ESA_State));
        funcctx->user_fctx = (void *)esa_status;

        esa_status->inited = false;
        esa_status->currIdx = 0;

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    esa_status  = (ESA_State *) funcctx->user_fctx;

    if( !esa_status->inited )
    {
        Relation     rel;
        int         shardidx;

        rel = heap_open(relOid, AccessShareLock);

        if(!RelationHasExtent(rel))
        {
            elog(ERROR,  "relation %s is not organized with extent.", RelationGetRelationName(rel));
        }
        
        for(shardidx = 0;  shardidx < MAX_SHARDS; shardidx++)
        {
            esa_status->esa[shardidx] = esa_get_anchor(rel, shardidx);
        }        
        heap_close(rel,  AccessShareLock);
    }

    if(esa_status->currIdx >= MAX_SHARDS)
    {
        SRF_RETURN_DONE(funcctx);
    }

    MemSet(nulls,  0, sizeof(nulls));
    MemSet(values, 0, sizeof(values));

    addr = esa_sid_to_address(esa_status->currIdx);
    values[0] = Int32GetDatum( esa_status->currIdx);
    values[1] = Int32GetDatum( addr.physical_page_number);
    values[2] = Int32GetDatum( addr.local_idx);
    values[3] = Int32GetDatum( esa_status->esa[esa_status->currIdx].scan_head);
    values[4] = Int32GetDatum( esa_status->esa[esa_status->currIdx].scan_tail);
    values[5] = Int32GetDatum( esa_status->esa[esa_status->currIdx].alloc_head);
    values[6] = Int32GetDatum( esa_status->esa[esa_status->currIdx].alloc_tail);

    esa_status->currIdx++;
    tuple = heap_form_tuple(funcctx->tuple_desc,  values, nulls);
    result = HeapTupleGetDatum(tuple);
    SRF_RETURN_NEXT(funcctx,  result);
    
}

static Oid
string_to_reloid(char *str)
{
    char     *str1 = NULL;
    char     *str2 = NULL;
    char     *schema = NULL;
    char     *relname = NULL;
    Oid        reloid = InvalidOid;


    str1 = strtok(str, ". ");
    if(str1)
        str2 = strtok(NULL, ". ");

    if(str1 && str2)
    {
        schema = str1;
        relname = str2;
    }
    else if(str1)
    {
        relname = str1;
    }
    else
    {
        elog(ERROR, "table name %s is invalid.", str);
    }

    if(schema)
    {
        Oid namespace_oid;
        namespace_oid = get_namespace_oid(schema, false);
        reloid = get_relname_relid(relname, namespace_oid);
    }
    else
    {
        reloid = RelnameGetRelid(relname);
    }

    if(InvalidOid == reloid)            
        elog(ERROR, "table %s is not exist.", str);

    return reloid;
}


void
TruncateExtentMap(Relation rel, BlockNumber nblocks)
{
    ExtentID start_eid;
    ExtentID end_eid;
    ExtentID eid;
    if(!RelationHasExtent(rel))
        return;

    if(nblocks % PAGES_PER_EXTENTS != 0)
        elog(ERROR, "truncation extentmap must begin with start point of a extent.");

    start_eid = nblocks / PAGES_PER_EXTENTS;
    end_eid = RelationGetNumberOfBlocks(rel) / PAGES_PER_EXTENTS;

    if(trace_extent)
    {
        ereport(LOG,
            (errmsg("[trace extent]TruncateExtentMap:[rel:%d/%d/%d]start"
                    "[starteid:%d, endeid:%d]",
                    rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
                    start_eid, end_eid)));
    }
        
    for(eid = end_eid - 1; eid >= start_eid && ExtentIdIsValid(eid); eid--)
    {
        FreeExtent(rel, eid);
    }

    eob_truncate(rel, start_eid, end_eid);
    ema_truncate(rel, start_eid, end_eid);

    if(trace_extent)
    {
        ereport(LOG,
            (errmsg("[trace extent]TruncateExtentMap:[rel:%d/%d/%d]end"
                    "[starteid:%d, endeid:%d]",
                    rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
                    start_eid, end_eid)));
    }
}

void
RebuildExtentMap(Relation rel)
{// #lizard forgives
    
    BlockNumber blk = InvalidBlockNumber;        
    ExtentID    eid = InvalidExtentID;
    ExtentID    begin_skip = InvalidExtentID;
    ExtentID    total_extents = (RelationGetNumberOfBlocks(rel) + PAGES_PER_EXTENTS - 1) / PAGES_PER_EXTENTS;
    xl_extent_truncate xlrec;
    
    if(!RelationHasExtent(rel))
    {
        elog(ERROR, "relation %s storage is not organized by extent.", RelationGetRelationName(rel));
    }

    /*
     * truncate exist extent file 
     */        
    //TODO: write xlog for truncate extent file
    RelationOpenSmgr(rel);
    smgrdounlinkfork(rel->rd_smgr, EXTENT_FORKNUM, false);
#ifdef _MLS_
	/*
     * clean up the rnode infomation in rel crypt hash table
     */
	remove_rel_crypt_hash_elem(&(rel->rd_smgr->smgr_relcrypt), true);
#endif
    RelationCloseSmgr(rel);
    
    INIT_EXLOG_TRUNCATE(&xlrec);
    memcpy(&(xlrec.rnode), &(rel->rd_node), sizeof(RelFileNode));    
    XLogBeginInsert();
    XLogRegisterData((char *) &xlrec, SizeOfTruncateExtentSeg);
    XLogInsert(RM_EXTENT_ID, XLOG_EXTENT_TRUNCATE);
    
    /*
     * rebuild extentmap of main data
     */
    for(eid = 0; eid < total_extents; eid++)
    {
        Buffer        data_buf = InvalidBuffer;
        Page        data_pg = NULL;
        ShardID        page_sid = InvalidShardID;
        uint8        freespace = 0;
        
        blk = eid * PAGES_PER_EXTENTS;

        data_buf = ReadBuffer(rel, blk);
        ExtentAssert(BufferIsValid(data_buf));

        data_pg = BufferGetPage(data_buf);
        page_sid = PageGetShardId(data_pg);
        
        if(PageIsNew(data_pg) || page_sid == InvalidShardID)
        {
            ReleaseBuffer(data_buf);
            if(!ExtentIdIsValid(begin_skip))
                begin_skip = eid;
            continue;
        }

        ReleaseBuffer(data_buf);

        if(ExtentIdIsValid(begin_skip))
        {
            ExtendExtentForRebuild(rel, eid - 1);
            begin_skip = InvalidExtentID;
        }

        if(!ShardIDIsValid(page_sid))
        {
            elog(WARNING, "block %d of relation %d/%d/%d has invalid shardid %d",
                            blk,
                            rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
                            page_sid);
            continue;
        }        

        freespace = GetMaxAvailWithExtent(rel, eid);
        ExtendExtentForShard(rel, page_sid, eid, freespace, true);
    }

    if(ExtentIdIsValid(begin_skip))
    {
        ExtendExtentForRebuild(rel, total_extents - 1);
        begin_skip = InvalidExtentID;
    }
    
    /*
     * rebuild extentmap of toast if any
     */
    if(RelationHasToast(rel))
    {
        /*
         * lock has be hold already at main table.
         */
        Relation toast_rel = heap_open(rel->rd_toastoid, NoLock);

        RebuildExtentMap(toast_rel);

        heap_close(toast_rel, NoLock);
    }
}

/*
 * check extent info
 */
 typedef enum CheckExtentErrCode
{
    CEEC_NO_ERROR,
    CEEC_BLOCK_NOT_EXIST_IN_HEAP,
    CEEC_EXTENT_NOT_OCCUPIED_IN_CATALOG,
    CEEC_SHARDID_NOT_SAME,
    CEEC_CATALOG_CORRUPTED,
    CEEC_MAX_ERRCODE
}CheckExtentErrCode;

typedef struct
{
    int     eid;
    CheckExtentErrCode    errtype;
    int        page_shardid;
    bool     is_occupied;
    int     shardid;
    int        freespace_cat;
    int        hwm;
    int        scan_next;
    int        scan_prev;
    int        alloc_next;
    int        alloc_prev;
}CheckExtent;

const char* CheckExtentError[] =
{
    "NO_ERROR",
    "BLOCK_NOT_EXIST_IN_HEAP",
    "EXTENT_NOT_OCCUPIED_IN_CATALOG",
    "SHARDID_NOT_SAME",
    "CATALOG_CORRUPTED"
};

typedef struct
{
    int    currIdx;
    int length;
    ExtentID last_eid;
    CheckExtent    mctxstat[EMES_PER_PAGE];    
} CheckExtent_State;

static 
void check_extent_catalog(CheckExtent *catalog, bool page_is_new)
{// #lizard forgives
    if(catalog->errtype != CEEC_NO_ERROR)
        return;

    if(page_is_new)
        return;

    if(!catalog->is_occupied)
    {
        if((ShardIDIsValid(catalog->shardid) && catalog->shardid > 0)
            || ExtentIdIsValid(catalog->scan_next)
            || ExtentIdIsValid(catalog->scan_prev)
            || ExtentIdIsValid(catalog->alloc_next)
            || ExtentIdIsValid(catalog->alloc_prev))
        {
            catalog->errtype = CEEC_CATALOG_CORRUPTED;
            return;
        }

        if(ShardIDIsValid(catalog->page_shardid))
        {
            catalog->errtype = CEEC_EXTENT_NOT_OCCUPIED_IN_CATALOG;
            return;
        }
    }
    else
    {
        if(!ShardIDIsValid(catalog->shardid))
        {
            catalog->errtype = CEEC_CATALOG_CORRUPTED;
            return;
        }

        if(catalog->shardid != catalog->page_shardid)
        {
            catalog->errtype = CEEC_SHARDID_NOT_SAME;
            return;
        }
    }
}

Datum pg_check_extent(PG_FUNCTION_ARGS)
{// #lizard forgives
#define CHECK_EXTENT_COLUMN_NUM 12
    Oid                relOid = PG_GETARG_OID(0);
    FuncCallContext *funcctx = NULL;
    CheckExtent_State    *mctx_status  = NULL;

    if (SRF_IS_FIRSTCALL())
    {        
        TupleDesc   tupdesc;
        MemoryContext oldcontext;
        
        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
                * Switch to memory context appropriate for multiple function calls
                */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples */
        tupdesc = CreateTemplateTupleDesc(CHECK_EXTENT_COLUMN_NUM, false);
        TupleDescInitEntry(tupdesc, (AttrNumber) 1, "eid",
                         INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 2, "error_type",
                         INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 3, "error",
                         TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 4, "page_shardid",
                         INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 5, "is_occupied",
                         BOOLOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 6, "extent_shardid",
                         INT4OID, -1, 0);        
        TupleDescInitEntry(tupdesc, (AttrNumber) 7, "freespace_cat",
                         INT4OID, -1, 0);        
        TupleDescInitEntry(tupdesc, (AttrNumber) 8, "hwm",
                         INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 9, "scan_next",
                         INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 10, "scan_prev",
                         INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 11, "alloc_next",
                         INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 12, "alloc_prev",
                         INT4OID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        mctx_status = (CheckExtent_State *) palloc(sizeof(CheckExtent_State));
        funcctx->user_fctx = (void *) mctx_status;

        mctx_status->currIdx = 0;

        MemoryContextSwitchTo(oldcontext);
    }

      funcctx = SRF_PERCALL_SETUP();
    mctx_status  = (CheckExtent_State *) funcctx->user_fctx;

    if(mctx_status->currIdx< MAX_EXTENTS)
    {
           Datum        values[CHECK_EXTENT_COLUMN_NUM];
        bool        nulls[CHECK_EXTENT_COLUMN_NUM];
        HeapTuple    tuple;
        Datum        result;
        CheckExtent    *stat;

        if(mctx_status->currIdx % EMES_PER_PAGE == 0)
        {
            int         i;
            Buffer         buf;
            EMAPage        pg;
            EMAAddress    addr;
            Relation    rel = heap_open(relOid, AccessShareLock);

            if(!RelationHasExtent(rel))
            {
                heap_close(rel, AccessShareLock);
                SRF_RETURN_DONE( funcctx );
            }
            addr = ema_eid_to_address(mctx_status->currIdx);
            buf = extent_readbuffer(rel,  addr.physical_page_number, false);
            if(BufferIsInvalid(buf) )
            {
                heap_close(rel, AccessShareLock);
                SRF_RETURN_DONE( funcctx );
            }
            
            LockBuffer(buf, BUFFER_LOCK_SHARE);
            pg = (EMAPage)PageGetContents(BufferGetPage(buf));

            if(pg->n_emes <= 0)
            {
                UnlockReleaseBuffer(buf);
                heap_close(rel,  AccessShareLock);
                SRF_RETURN_DONE(funcctx);
            }
            
            mctx_status->length = pg->n_emes;

            for(i=0; i<mctx_status->length; i++)
            {
                mctx_status->mctxstat[i].alloc_next = pg->ema[i].alloc_next;
                mctx_status->mctxstat[i].alloc_prev = EMEGetAllocPrev(&pg->ema[i]);
                mctx_status->mctxstat[i].eid = mctx_status->currIdx + i;
                mctx_status->mctxstat[i].is_occupied = (bool)pg->ema[i].is_occupied;
                mctx_status->mctxstat[i].shardid = pg->ema[i].shardid;
                mctx_status->mctxstat[i].freespace_cat = pg->ema[i].max_freespace;
                mctx_status->mctxstat[i].hwm = pg->ema[i].hwm;
                mctx_status->mctxstat[i].scan_next = pg->ema[i].scan_next;
                mctx_status->mctxstat[i].scan_prev = pg->ema[i].scan_prev;
            }

            for(i=0; i<mctx_status->length; i++)
            {
                ExtentID eid = mctx_status->mctxstat[i].eid;
                Buffer    data_buf = InvalidBuffer;

                mctx_status->mctxstat[i].errtype= CEEC_NO_ERROR;
                    
                data_buf = ReadBuffer(rel, eid * PAGES_PER_EXTENTS);

                if(BufferIsInvalid(data_buf))
                {
                    mctx_status->mctxstat[i].page_shardid = InvalidShardID;
                    mctx_status->mctxstat[i].errtype= CEEC_BLOCK_NOT_EXIST_IN_HEAP;
                }
                else if(PageIsNew(BufferGetPage(data_buf)))
                {
                    mctx_status->mctxstat[i].page_shardid = InvalidShardID;
                    check_extent_catalog(&mctx_status->mctxstat[i], true);
                }
                else
                {
                    mctx_status->mctxstat[i].page_shardid = PageGetShardId(BufferGetPage(data_buf));
                    check_extent_catalog(&mctx_status->mctxstat[i], false);
                }
                
                ReleaseBuffer(data_buf);                
            }

            UnlockReleaseBuffer(buf);                        
            heap_close(rel, AccessShareLock);
        }

        if(mctx_status->length < EMES_PER_PAGE 
            && (mctx_status->currIdx % EMES_PER_PAGE) >= mctx_status->length)
        {
            SRF_RETURN_DONE(funcctx);
        }

        stat = &mctx_status->mctxstat[mctx_status->currIdx % EMES_PER_PAGE];

        MemSet(values, 0, sizeof(values));
        MemSet(nulls,  0, sizeof(nulls));

        if(!ExtentIdIsValid(stat->scan_next))
            nulls[8] = true;
        if(!ExtentIdIsValid(stat->scan_prev))
            nulls[9] = true;
        if(!ExtentIdIsValid(stat->alloc_next))
            nulls[10] = true;
        if(!ExtentIdIsValid(stat->alloc_prev))
            nulls[11] = true;

        values[0] = Int32GetDatum(stat->eid);
        values[1] = Int32GetDatum(stat->errtype);
        values[2] = CStringGetTextDatum(CheckExtentError[stat->errtype]);
        values[3] = Int32GetDatum(stat->page_shardid);
        values[4] = BoolGetDatum(stat->is_occupied);
        values[5] = Int32GetDatum(stat->shardid);        
        values[6] = Int32GetDatum(stat->freespace_cat);
        values[7] = Int32GetDatum(stat->hwm);
        values[8] = Int32GetDatum(stat->scan_next);
        values[9] = Int32GetDatum(stat->scan_prev);
        values[10] = Int32GetDatum(stat->alloc_next);
        values[11] = Int32GetDatum(stat->alloc_prev);

        mctx_status->currIdx++;
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    }

    SRF_RETURN_DONE(funcctx);   
}

