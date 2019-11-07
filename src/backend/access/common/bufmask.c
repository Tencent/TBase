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
 * bufmask.c
 *      Routines for buffer masking. Used to mask certain bits
 *      in a page which can be different when the WAL is generated
 *      and when the WAL is applied.
 *
 * Portions Copyright (c) 2016-2017, PostgreSQL Global Development Group
 *
 * Contains common routines required for masking a page.
 *
 * IDENTIFICATION
 *      src/backend/storage/buffer/bufmask.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/bufmask.h"

/*
 * mask_page_lsn
 *
 * In consistency checks, the LSN of the two pages compared will likely be
 * different because of concurrent operations when the WAL is generated
 * and the state of the page when WAL is applied.
 */
void
mask_page_lsn(Page page)
{
    PageHeader    phdr = (PageHeader) page;

    PageXLogRecPtrSet(phdr->pd_lsn, (uint64) MASK_MARKER);
}

/*
 * mask_page_hint_bits
 *
 * Mask hint bits in PageHeader. We want to ignore differences in hint bits,
 * since they can be set without emitting any WAL.
 */
void
mask_page_hint_bits(Page page)
{
    PageHeader    phdr = (PageHeader) page;

    /* Ignore prune_xid (it's like a hint-bit) */
    phdr->pd_prune_xid = MASK_MARKER;
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
    phdr->pd_prune_ts = MASK_MARKER;
#endif

    /* Ignore PD_PAGE_FULL and PD_HAS_FREE_LINES flags, they are just hints. */
    PageClearFull(page);
    PageClearHasFreeLinePointers(page);

    /*
     * During replay, if the page LSN has advanced past our XLOG record's LSN,
     * we don't mark the page all-visible. See heap_xlog_visible() for
     * details.
     */
    PageClearAllVisible(page);
}

/*
 * mask_unused_space
 *
 * Mask the unused space of a page between pd_lower and pd_upper.
 */
void
mask_unused_space(Page page)
{
    int            pd_lower = ((PageHeader) page)->pd_lower;
    int            pd_upper = ((PageHeader) page)->pd_upper;
    int            pd_special = ((PageHeader) page)->pd_special;

    /* Sanity check */
    if (pd_lower > pd_upper || pd_special < pd_upper ||
        pd_lower < SizeOfPageHeaderData || pd_special > BLCKSZ)
    {
        elog(ERROR, "invalid page pd_lower %u pd_upper %u pd_special %u\n",
             pd_lower, pd_upper, pd_special);
    }

    memset(page + pd_lower, MASK_MARKER, pd_upper - pd_lower);
}

/*
 * mask_lp_flags
 *
 * In some index AMs, line pointer flags can be modified in master without
 * emitting any WAL record.
 */
void
mask_lp_flags(Page page)
{
    OffsetNumber offnum,
                maxoff;

    maxoff = PageGetMaxOffsetNumber(page);
    for (offnum = FirstOffsetNumber;
         offnum <= maxoff;
         offnum = OffsetNumberNext(offnum))
    {
        ItemId        itemId = PageGetItemId(page, offnum);

        if (ItemIdIsUsed(itemId))
            itemId->lp_flags = LP_UNUSED;
    }
}

/*
 * mask_page_content
 *
 * In some index AMs, the contents of deleted pages need to be almost
 * completely ignored.
 */
void
mask_page_content(Page page)
{
    /* Mask Page Content */
    memset(page + SizeOfPageHeaderData, MASK_MARKER,
           BLCKSZ - SizeOfPageHeaderData);

    /* Mask pd_lower and pd_upper */
    memset(&((PageHeader) page)->pd_lower, MASK_MARKER,
           sizeof(uint16));
    memset(&((PageHeader) page)->pd_upper, MASK_MARKER,
           sizeof(uint16));
}
