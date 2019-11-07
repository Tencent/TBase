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
 * buf.h
 *      Basic buffer manager data types.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/ema_internals.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef EMA_INTERNALS_H
#define EMA_INTERNALS_H

#include "access/htup_details.h"
#include "storage/buf.h"
#include "storage/bufpage.h"
#include "storage/block.h"
#include "storage/relfilenode.h"
#include "utils/relcache.h"

#define EXTENT_SAVED_MINCAT 5
#define MAX_FREESPACE 254

typedef enum EmaPageType
{
    EmaPageType_NONE,
    EmaPageType_EOB,
    EmaPageType_ESA,
    EmaPageType_EMA
}EmaPageType;

/*---------------------------------------------------------------------------
 *
 * Extent Occupation Bitmap (EOB) struct
 * EOB record if every extend has be occupied by one shard
 *
 *---------------------------------------------------------------------------
 */
#define EOBPAGE_OFFSET 0

typedef struct EOBAddress
{
    BlockNumber     physical_page_number;
    int32             local_bms_offset;
}EOBAddress;

#define EOB_INVALID_FIRST_FREE -2
typedef struct EOBPageData
{
    int32        max_bits;
    int32        n_bits;            
    int32        first_empty_extent; /* be inited to -1 */
    Bitmapset    eob_bits;            /* 1 represent empty, 0 represent being used */
}EOBPageData;

typedef struct EOBPageData *EOBPage;
#define BufferGetEOBPage(buf) \
    (BufferIsValid(buf) ? (EOBPage)PageGetContents(BufferGetPage(buf)) : NULL)

#define EOBS_PER_PAGE \
    ((BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - offsetof(EOBPageData, eob_bits) - offsetof(Bitmapset, words))*8)
#define EOB_MAXPAGES \
    (int)((MAX_EXTENTS + EOBS_PER_PAGE - 1)/EOBS_PER_PAGE)
#define EOB_TOPPAGE (EOBPAGE_OFFSET + EOB_MAXPAGES)
#define EOBAddressIsValid(addr) \
    ((addr)->physical_page_number >= EOBPAGE_OFFSET \
        && (addr)->physical_page_number < EOBPAGE_OFFSET + EOB_MAXPAGES \
        && (addr)->local_bms_offset >= 0 \
        && (addr)->local_bms_offset < EOBS_PER_PAGE)
#define EOBPage_IS_FULL(pg) \
    (((EOBPage)PageGetContents(pg))->max_bits <= ((EOBPage)PageGetContents(pg))->n_bits)
#define EOBPage_IS_EMPTY(pg) \
    (((EOBPage)PageGetContents(pg))->n_bits <= 0)

/*--------------------------------------------------------------------------- 
 *
 * EMA Shard Anchor (ESA)  struct
 *
 *---------------------------------------------------------------------------
 */
#define ESAPAGE_OFFSET EOB_TOPPAGE

typedef struct ESAAddress
{
    BlockNumber     physical_page_number;
    int32            local_idx;
}ESAAddress;

typedef struct EMAShardAnchor
{
    ExtentID scan_head;
    ExtentID scan_tail;
    ExtentID alloc_head;
    ExtentID alloc_tail;
}EMAShardAnchor;

typedef struct ESAPageData
{
    uint32     max_anchors;
    EMAShardAnchor anchors[FLEXIBLE_ARRAY_MEMBER];
}ESAPageData;

typedef struct ESAPageData *ESAPage;

#define BufferGetESAPage(buf) \
    (BufferIsValid(buf) ? (ESAPage)PageGetContents(BufferGetPage(buf)) : NULL)

#define ESA_SIZE sizeof(EMAShardAnchor)
#define ESAS_PER_PAGE \
    ((BLCKSZ - MAXALIGN(SizeOfPageHeaderData)  - offsetof(ESAPageData, anchors))/ESA_SIZE)
#define ESA_MAXPAGES \
    (int)((MAX_SHARDS + ESAS_PER_PAGE - 1)/ESAS_PER_PAGE)
#define ESA_TOPPAGE (ESAPAGE_OFFSET + ESA_MAXPAGES)
#define ESAAddressIsValid(addr) ((addr)->physical_page_number >= ESAPAGE_OFFSET \
                                    && (addr)->physical_page_number < ESAPAGE_OFFSET + ESA_MAXPAGES \
                                    && (addr)->local_idx >= 0 \
                                    && (addr)->local_idx < ESAS_PER_PAGE)

#define INIT_ESA(anchor) \
    do{ \
        (anchor)->scan_head = (anchor)->scan_tail = (anchor)->alloc_head = (anchor)->alloc_tail = InvalidExtentID; \
    }while(0)
/*---------------------------------------------------------------------------
 * 
 * Extent Mapping Array(EMA) struct
 * EMA records the mapping from extent to shard.
 * One extent only store data belong to one shard, whereas one shard can have more than one extent.
 *
 *---------------------------------------------------------------------------
 */
#define EMAPAGE_OFFSET ESA_TOPPAGE

typedef struct EMAAddress
{
    BlockNumber     physical_page_number;
    int32            local_idx;
}EMAAddress;

typedef struct ExtentMappingElement
{
    uint32 shardid : 12;
    uint32 max_freespace:8;
    uint32 hwm:11;
    uint32 is_occupied:1;    

    uint32 alloc_prev_high:8;
    ExtentID scan_next:24;
    uint32 alloc_prev_mid:8;
    ExtentID scan_prev:24;
    uint32 alloc_prev_low:8;
    ExtentID alloc_next:24;    
}ExtentMappingElement;

#define EMEGetAllocPrev(eme) \
    (((eme)->alloc_prev_high << 16) | ((eme)->alloc_prev_mid << 8) | ((eme)->alloc_prev_low))
#define EMESetAllocPrev(eme, alloc_prev) \
    do \
    {    \
        (eme)->alloc_prev_low = (alloc_prev) & 0xFF; \
        (eme)->alloc_prev_mid = ((alloc_prev) & 0xFF00) >> 8; \
        (eme)->alloc_prev_high = ((alloc_prev) & 0xFF0000) >> 16; \
    }while(0);

#define INIT_EME(eme) \
    do{ \
        (eme)->shardid = 0; \
        (eme)->max_freespace = 0; \
        (eme)->hwm = 0; \
        (eme)->is_occupied = 0; \
        (eme)->scan_next = (eme)->scan_prev = (eme)->alloc_next = InvalidExtentID; \
        EMESetAllocPrev(eme, InvalidExtentID); \
    }while(0)
    
typedef struct EMAPageData
{
    int32     max_emes;
    int32     n_emes;
    ExtentMappingElement ema[FLEXIBLE_ARRAY_MEMBER];
}EMAPageData;

typedef struct EMAPageData *EMAPage;

#define BufferGetEMAPage(buf) \
    (BufferIsValid(buf) ? (EMAPage)PageGetContents(BufferGetPage(buf)) : NULL)

#define EME_SIZE sizeof(ExtentMappingElement)
#define EMES_PER_PAGE ((BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - offsetof(EMAPageData, ema))/EME_SIZE)
#define EMA_MAXPAGES (int)((MAX_EXTENTS + EMES_PER_PAGE - 1)/EMES_PER_PAGE)
#define EMA_TOPPAGE (EMAPAGE_OFFSET + EMA_MAXPAGES)
#define EMAAddressIsValid(addr) ((addr)->physical_page_number >= EMAPAGE_OFFSET \
                                    && (addr)->physical_page_number < EMAPAGE_OFFSET + EMA_MAXPAGES \
                                    && (addr)->local_idx >= 0 \
                                    && (addr)->local_idx < EMES_PER_PAGE)


#define EMA_FORK_BLOCKS EMAPAGE_OFFSET + EMA_MAXPAGES

#define EMAPage_IS_FULL(pg) \
    (((EMAPage)PageGetContents(pg))->max_emes <= ((EMAPage)PageGetContents(pg))->n_emes)
#define EMAPage_IS_EMPTY(pg) \
    (((EMAPage)PageGetContents(pg))->n_emes <= 0)



/*-------------------------------------------------------------------------
 * 
 * functions and macros for EOB
 *
 *-------------------------------------------------------------------------
 */
extern void            eob_page_mark_extent(EOBPage eob_pg, int local_bms_offset, bool setfree);
extern void         eob_init_page(Page page, int32 max_bits);
extern void         eob_clean_page(EOBPage page);
extern void         eob_truncate_page(EOBPage page, int local_bms_offset);

extern EOBAddress     eob_eid_to_address(ExtentID eid);
extern ExtentID        eob_address_to_eid(EOBAddress addr);
extern bool         eob_extent_is_free(Relation rel, ExtentID eid);
extern void         eob_mark_extent(Relation rel, ExtentID eid, bool setfree);
extern ExtentID     eob_get_free_extent(Relation rel);
extern ExtentID     eob_get_free_extent_and_set_busy(Relation rel);
extern void         eob_truncate(Relation rel, ExtentID new_max_eid, ExtentID old_max_eid);


/*-------------------------------------------------------------------------
 *
 * functions and macros for ESA
 *
 *--------------------------------------------------------------------------
 */

/* flags for set ESA */
#define ESA_SETFLAG_SCANHEAD 0x01
#define ESA_SETFLAG_SCANTAIL 0x02
#define ESA_SETFLAG_ALLOCHEAD 0x04
#define ESA_SETFLAG_ALLOCTAIL 0x08
#define ESA_SETFLAG_ALL  \
        (ESA_SETFLAG_SCANHEAD | ESA_SETFLAG_SCANTAIL | ESA_SETFLAG_ALLOCHEAD | ESA_SETFLAG_ALLOCTAIL)
#define ESA_SETFLAG_SCAN \
        (ESA_SETFLAG_SCANHEAD | ESA_SETFLAG_SCANTAIL)
#define ESA_SETFLAG_ALLOC \
        (ESA_SETFLAG_ALLOCHEAD | ESA_SETFLAG_ALLOCTAIL)

/* macor for setting ESA located by shardid*/
#define SET_ESA_SCAN_HEAD(rel, sid, scan_head) \
            esa_set_anchor(rel, sid, ESA_SETFLAG_SCANHEAD, \
                scan_head, InvalidExtentID, \
                InvalidExtentID, InvalidExtentID)

#define SET_ESA_SCAN_TAIL(rel, sid, scan_tail) \
            esa_set_anchor(rel, sid, ESA_SETFLAG_SCANTAIL, \
                InvalidExtentID, scan_tail, \
                InvalidExtentID, InvalidExtentID)

#define SET_ESA_ALLOC_HEAD(rel, sid, alloc_head) \
            esa_set_anchor(rel, sid, ESA_SETFLAG_ALLOCHEAD, \
                InvalidExtentID, InvalidExtentID, \
                alloc_head, InvalidExtentID)

#define SET_ESA_ALLOC_TAIL(rel, sid, alloc_tail) \
            esa_set_anchor(rel, sid, ESA_SETFLAG_ALLOCTAIL, \
                InvalidExtentID, InvalidExtentID, \
                InvalidExtentID, alloc_tail)

#define SET_ESA_SCAN(rel, sid, scan_head, scan_tail) \
            esa_set_anchor(rel, sid, ESA_SETFLAG_SCAN, \
                scan_head, scan_tail, \
                InvalidExtentID, InvalidExtentID)

#define SET_ESA_ALLOC(rel, sid, alloc_head, alloc_tail) \
            esa_set_anchor(rel, sid, ESA_SETFLAG_ALLOC, \
                InvalidExtentID, InvalidExtentID, \
                alloc_head, alloc_tail)
                
/* macor for setting ESA in page */
#define PAGE_SET_ESA_SCAN_HEAD(pg, local_index, scan_head) \
            esa_page_set_anchor(pg, local_index, ESA_SETFLAG_SCANHEAD, \
                scan_head, InvalidExtentID, \
                InvalidExtentID, InvalidExtentID)

#define PAGE_SET_ESA_SCAN_TAIL(pg, local_index, scan_tail) \
            esa_page_set_anchor(pg, local_index, ESA_SETFLAG_SCANTAIL, \
                InvalidExtentID, scan_tail, \
                InvalidExtentID, InvalidExtentID)

#define PAGE_SET_ESA_ALLOC_HEAD(pg, local_index, alloc_head) \
            esa_page_set_anchor(pg, local_index, ESA_SETFLAG_ALLOCHEAD, \
                InvalidExtentID, InvalidExtentID, \
                alloc_head, InvalidExtentID)

#define PAGE_SET_ESA_ALLOC_TAIL(pg, local_index, alloc_tail) \
            esa_page_set_anchor(pg, local_index, ESA_SETFLAG_ALLOCTAIL, \
                InvalidExtentID, InvalidExtentID, \
                InvalidExtentID, alloc_tail)

#define PAGE_SET_ESA_SCAN(pg, local_index, scan_head, scan_tail) \
            esa_page_set_anchor(pg, local_index, ESA_SETFLAG_SCAN, \
                scan_head, scan_tail, \
                InvalidExtentID, InvalidExtentID)

#define PAGE_SET_ESA_ALLOC(pg, local_index, alloc_head, alloc_tail) \
            esa_page_set_anchor(pg, local_index, ESA_SETFLAG_ALLOC, \
                InvalidExtentID, InvalidExtentID, \
                alloc_head, alloc_tail)

/* functions of ESA */
extern EMAShardAnchor esa_get_anchor(Relation rel, ShardID sid);



extern void         esa_init_page(Page page, int32 max_anchors);
extern ESAAddress     esa_sid_to_address(ShardID sid);
extern ShardID         esa_address_to_eid(ESAAddress addr);
extern void         esa_page_set_anchor(Page pg, int local_index, int    set_flags,
                                                ExtentID scan_head, ExtentID scan_tail,
                                                ExtentID alloc_head, ExtentID alloc_tail);
extern void            esa_set_anchor(Relation rel, ShardID sid, int    set_flags,
                                    ExtentID scan_head, ExtentID scan_tail,
                                    ExtentID alloc_head, ExtentID alloc_tail);
extern EMAShardAnchor    esa_get_anchor(Relation rel, ShardID sid);




/*-------------------------------------------------------------------------
 * 
 * Functions and macros for EMA
 *
 *--------------------------------------------------------------------------
 */
/* flags for setting fields of EMA*/
#define EMA_SETFLAG_SCANPREV     0x0001
#define EMA_SETFLAG_SCANNEXT     0x0002
#define EMA_SETFLAG_ALLOCPREV     0x0004
#define EMA_SETFLAG_ALLOCNEXT     0x0008
#define EMA_SETFLAG_SHARDID         0x0010
#define EMA_SETFLAG_FREESPACE    0x0020
#define EMA_SETFLAG_HWM            0x0040
#define EMA_SETFLAG_OCCUPIED    0x0080
#define EMA_SETFLAG_INIT        0x0100
#define EMA_SETFLAG_CLEAN        0x0200
#define EMA_SETFLAG_EXTENDHEAP    0x0400
#define EMA_SETFLAG_ALLPOINTER    0x000F
#define EMA_SETFLAG_ALL         0x00FF

/* macro for setting EME located by extentid */
#define SET_EME_SCAN_PREV(rel, eid, scan_prev) \
    ema_set_eme_link(rel, eid, EMA_SETFLAG_SCANPREV, \
                    scan_prev, InvalidExtentID, \
                    InvalidExtentID, InvalidExtentID)

#define SET_EME_SCAN_NEXT(rel, eid, scan_next) \
    ema_set_eme_link(rel, eid, EMA_SETFLAG_SCANNEXT, \
                    InvalidExtentID, scan_next, \
                    InvalidExtentID, InvalidExtentID)

#define SET_EME_ALLOC_PREV(rel, eid, alloc_prev) \
    ema_set_eme_link(rel, eid, EMA_SETFLAG_ALLOCPREV, \
                    InvalidExtentID, InvalidExtentID, \
                    alloc_prev, InvalidExtentID)

#define SET_EME_ALLOC_NEXT(rel, eid, alloc_next) \
    ema_set_eme_link(rel, eid, EMA_SETFLAG_ALLOCNEXT, \
                    InvalidExtentID, InvalidExtentID, \
                    InvalidExtentID, alloc_next)


/* macro for setting EME in page */
#define PAGE_SET_EME_SCAN_PREV(pg, local_index, scan_prev) \
    ema_page_set_eme_link(pg, local_index, EMA_SETFLAG_SCANPREV, \
                        scan_prev, InvalidExtentID, \
                        InvalidExtentID, InvalidExtentID)
                        
#define PAGE_SET_EME_SCAN_NEXT(pg, local_index, scan_next) \
    ema_page_set_eme_link(pg, local_index, EMA_SETFLAG_SCANNEXT, \
                        InvalidExtentID, scan_next, \
                        InvalidExtentID, InvalidExtentID)                        

#define PAGE_SET_EME_ALLOC_PREV(pg, local_index, alloc_prev) \
    ema_page_set_eme_link(pg, local_index, EMA_SETFLAG_ALLOCPREV, \
                        InvalidExtentID, InvalidExtentID, \
                        alloc_prev, InvalidExtentID)

#define PAGE_SET_EME_ALLOC_NEXT(pg, local_index, alloc_next) \
    ema_page_set_eme_link(pg, local_index, EMA_SETFLAG_ALLOCNEXT, \
                        InvalidExtentID, InvalidExtentID, \
                        InvalidExtentID, alloc_next)
                        
#define EME_IN_SAME_PAGE(eid1, eid2) \
    ((eid1) / EMES_PER_PAGE == (eid2) / EMES_PER_PAGE)

extern EMAAddress     
                ema_eid_to_address(ExtentID eid);
extern ExtentID ema_address_to_eid(EMAAddress addr);

extern void     ema_init_page(Page page, int32 max_emes);
extern void     ema_clean_page(EMAPage page);
extern void     ema_truncate_page(EMAPage page, int last_offset);

extern void     ema_init_eme(Relation rel, ExtentID eid, ShardID sid);
extern void        ema_free_eme(Relation rel, ExtentID eid);
extern void     repair_eme(Relation rel, BlockNumber blk, ShardID sid);
extern ExtentMappingElement *
                ema_get_eme(Relation rel, ExtentID eid);
extern void     ema_get_eme_extract(Relation rel, 
                                            ExtentID eid, 
                                            bool *is_occupied, 
                                            ShardID     *sid, 
                                            int *hwm, 
                                            uint8 *freespace);
extern void     ema_set_eme_hwm(Relation rel, ExtentID eid, int16 hwm);
extern void     ema_set_eme_link(Relation rel, 
                                        ExtentID eid, 
                                        int32 set_flags, 
                                        ExtentID scan_prev, 
                                        ExtentID scan_next, 
                                        ExtentID alloc_prev, 
                                        ExtentID alloc_next);
extern void     ema_set_eme_freespace(Relation rel, ExtentID eid, uint8 freespace);

extern ExtentID    ema_next_alloc(Relation rel, 
                                    ExtentID curr, 
                                    bool error_if_free, 
                                    bool *is_occupied, 
                                    ShardID *sid, 
                                    int *hwm, 
                                    uint8 *freespace);
extern ExtentID    ema_prev_alloc(Relation rel, 
                                    ExtentID curr, 
                                    uint8 *freespace, 
                                    ShardID *sid);
extern ExtentID ema_next_scan(Relation rel, 
                                    ExtentID curr, 
                                    bool error_if_free, 
                                    bool *is_occupied, 
                                    ShardID *sid, 
                                    int *hwm, 
                                    uint8 *freespace);
extern ExtentID ema_prev_scan(Relation rel, 
                                    ExtentID curr, 
                                    uint8 *freespace, 
                                    ShardID *sid);

extern ExtentMappingElement * 
                ema_page_get_eme(Page pg, int32 local_index);
extern void     ema_page_get_eme_extract(Page pg, 
                                                int32 local_index, 
                                                bool *is_occupied, 
                                                ShardID     *sid, 
                                                int *hwm, 
                                                uint8 *freespace);
extern void     ema_page_set_eme_hwm(Page pg, int32 local_index, int16 hwm);
extern void     ema_page_set_eme_freespace(Page pg, int32 local_index, uint8 freespace);
extern void     ema_page_set_eme_link(Page pg, 
                                                int32 local_index, 
                                                int32 set_flags, 
                                                ExtentID scan_prev, 
                                                ExtentID scan_next, 
                                                ExtentID alloc_prev, 
                                                ExtentID alloc_next);
extern void        ema_page_set_eme(Page pg, int32 local_index,     int32 set_flags, ExtentMappingElement *tempEME);
extern void     ema_page_init_eme(Page pg, int32 local_index, ShardID sid, uint8 freespace);
extern void     ema_page_free_eme(Page pg, int32 local_index);
extern void     ema_page_extend_eme(Page pg, int n_emes, bool cleaneme, int clean_start, int clean_end);
extern void     ema_rnode_set_eme_freespace(RelFileNode rnode, ExtentID eid, uint8 freespace);
extern void     ema_truncate(Relation rel, ExtentID new_max_eid, ExtentID old_max_eid);

extern Buffer     extent_readbuffer(Relation rel, BlockNumber blkno, bool extend);
extern void     extent_extend_block(Relation rel, BlockNumber ema_nblocks);
extern Buffer     extent_readbuffer_for_redo(RelFileNode rnode, BlockNumber blkno, bool extend);
extern void     extent_extend_block_for_redo(RelFileNode rnode, BlockNumber ema_nblocks);
extern ExtentID extent_extend_for_rebuild(Relation rel, ExtentID eid);



/*--------------------------------------------------------------------------- 
 *
 * external functions
 *
 *---------------------------------------------------------------------------
 */
extern ExtentID GetExtentWithFreeSpace(Relation rel, ShardID sid, uint8 min_cat);
extern void        ExtendExtentForShard(Relation rel, ShardID sid, ExtentID eid, uint8 freespace, bool for_rebuild);
extern void        ExtendExtentForRebuild(Relation rel, ExtentID eid);
extern void        FreeExtent(Relation rel, ExtentID eid);
extern void     MarkExtentFull(Relation rel, ExtentID eid);
extern void     MarkExtentAvailable(Relation rel, ExtentID eid);
extern ExtentID    GetShardScanHead(Relation re, ShardID sid);
extern ExtentID RelOidGetShardScanHead(Oid reloid, ShardID sid);
extern void     TruncateExtentMap(Relation rel, BlockNumber nblocks);
extern void       RebuildExtentMap(Relation rel);


#endif                            /* EMA_INTERNALS_H */
