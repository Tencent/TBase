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
 * xlogreader.h
 *        Definitions for the generic XLog reading facility
 *
 * Portions Copyright (c) 2013-2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *        src/include/access/xlogreader.h
 *
 * NOTES
 *        See the definition of the XLogReaderState struct for instructions on
 *        how to use the XLogReader infrastructure.
 *
 *        The basic idea is to allocate an XLogReaderState via
 *        XLogReaderAllocate(), and call XLogReadRecord() until it returns NULL.
 *
 *        After reading a record with XLogReadRecord(), it's decomposed into
 *        the per-block and main data parts, and the parts can be accessed
 *        with the XLogRec* macros and functions. You can also decode a
 *        record that's already constructed in memory, without reading from
 *        disk, by calling the DecodeXLogRecord() function.
 *-------------------------------------------------------------------------
 */
#ifndef XLOGREADER_H
#define XLOGREADER_H

#include "access/xlogrecord.h"

typedef struct XLogReaderState XLogReaderState;

/* Function type definition for the read_page callback */
typedef int (*XLogPageReadCB) (XLogReaderState *xlogreader,
                               XLogRecPtr targetPagePtr,
                               int reqLen,
                               XLogRecPtr targetRecPtr,
                               char *readBuf,
                               TimeLineID *pageTLI);

typedef struct
{
    /* Is this block ref in use? */
    bool        in_use;

    /* Identify the block this refers to */
    RelFileNode rnode;
    ForkNumber    forknum;
    BlockNumber blkno;

    /*shardid for initiating page*/
#ifdef _SHARDING_
    ShardID        sid;        
#endif

    /* copy of the fork_flags field from the XLogRecordBlockHeader */
    uint8        flags;

    /* Information on full-page image, if any */
    bool        has_image;        /* has image, even for consistency checking */
    bool        apply_image;    /* has image that should be restored */
    char       *bkp_image;
    uint16        hole_offset;
    uint16        hole_length;
    uint16        bimg_len;
    uint8        bimg_info;

    /* Buffer holding the rmgr-specific data associated with this block */
    bool        has_data;
    char       *data;
    uint16        data_len;
    uint16        data_bufsz;
} DecodedBkpBlock;

struct XLogReaderState
{
    /* ----------------------------------------
     * Public parameters
     * ----------------------------------------
     */

    /*
     * Data input callback (mandatory).
     *
     * This callback shall read at least reqLen valid bytes of the xlog page
     * starting at targetPagePtr, and store them in readBuf.  The callback
     * shall return the number of bytes read (never more than XLOG_BLCKSZ), or
     * -1 on failure.  The callback shall sleep, if necessary, to wait for the
     * requested bytes to become available.  The callback will not be invoked
     * again for the same page unless more than the returned number of bytes
     * are needed.
     *
     * targetRecPtr is the position of the WAL record we're reading.  Usually
     * it is equal to targetPagePtr + reqLen, but sometimes xlogreader needs
     * to read and verify the page or segment header, before it reads the
     * actual WAL record it's interested in.  In that case, targetRecPtr can
     * be used to determine which timeline to read the page from.
     *
     * The callback shall set *pageTLI to the TLI of the file the page was
     * read from.  It is currently used only for error reporting purposes, to
     * reconstruct the name of the WAL file where an error occurred.
     */
    XLogPageReadCB read_page;

    /*
     * System identifier of the xlog files we're about to read.  Set to zero
     * (the default value) if unknown or unimportant.
     */
    uint64        system_identifier;

    /*
     * Opaque data for callbacks to use.  Not used by XLogReader.
     */
    void       *private_data;

    /*
     * Start and end point of last record read.  EndRecPtr is also used as the
     * position to read next, if XLogReadRecord receives an invalid recptr.
     */
    XLogRecPtr    ReadRecPtr;        /* start of last record read */
    XLogRecPtr    EndRecPtr;        /* end+1 of last record read */


    /* ----------------------------------------
     * Decoded representation of current record
     *
     * Use XLogRecGet* functions to investigate the record; these fields
     * should not be accessed directly.
     * ----------------------------------------
     */
    XLogRecord *decoded_record; /* currently decoded record */

    char       *main_data;        /* record's main data portion */
    uint32        main_data_len;    /* main data portion's length */
    uint32        main_data_bufsz;    /* allocated size of the buffer */

    RepOriginId record_origin;

    /* information about blocks referenced by the record. */
    DecodedBkpBlock blocks[XLR_MAX_BLOCK_ID + 1];

    int            max_block_id;    /* highest block_id in use (-1 if none) */

    /* ----------------------------------------
     * private/internal state
     * ----------------------------------------
     */

    /*
     * Buffer for currently read page (XLOG_BLCKSZ bytes, valid up to at least
     * readLen bytes)
     */
    char       *readBuf;
    uint32        readLen;

    /* last read segment, segment offset, TLI for data currently in readBuf */
    XLogSegNo    readSegNo;
    uint32        readOff;
    TimeLineID    readPageTLI;

    /*
     * beginning of prior page read, and its TLI.  Doesn't necessarily
     * correspond to what's in readBuf; used for timeline sanity checks.
     */
    XLogRecPtr    latestPagePtr;
    TimeLineID    latestPageTLI;

    /* beginning of the WAL record being read. */
    XLogRecPtr    currRecPtr;
    /* timeline to read it from, 0 if a lookup is required */
    TimeLineID    currTLI;

    /*
     * Safe point to read to in currTLI if current TLI is historical
     * (tliSwitchPoint) or InvalidXLogRecPtr if on current timeline.
     *
     * Actually set to the start of the segment containing the timeline switch
     * that ends currTLI's validity, not the LSN of the switch its self, since
     * we can't assume the old segment will be present.
     */
    XLogRecPtr    currTLIValidUntil;

    /*
     * If currTLI is not the most recent known timeline, the next timeline to
     * read from when currTLIValidUntil is reached.
     */
    TimeLineID    nextTLI;

    /* Buffer for current ReadRecord result (expandable) */
    char       *readRecordBuf;
    uint32        readRecordBufSize;

    /* Buffer to hold error message */
    char       *errormsg_buf;
};

/* Get a new XLogReader */
extern XLogReaderState *XLogReaderAllocate(XLogPageReadCB pagereadfunc,
                   void *private_data);

/* Free an XLogReader */
extern void XLogReaderFree(XLogReaderState *state);

/* Read the next XLog record. Returns NULL on end-of-WAL or failure */
extern struct XLogRecord *XLogReadRecord(XLogReaderState *state,
               XLogRecPtr recptr, char **errormsg);

/* Invalidate read state */
extern void XLogReaderInvalReadState(XLogReaderState *state);

#ifdef FRONTEND
extern XLogRecPtr XLogFindNextRecord(XLogReaderState *state, XLogRecPtr RecPtr);
#endif                            /* FRONTEND */

/* Functions for decoding an XLogRecord */

extern bool DecodeXLogRecord(XLogReaderState *state, XLogRecord *record,
                 char **errmsg);

#define XLogRecGetTotalLen(decoder) ((decoder)->decoded_record->xl_tot_len)
#define XLogRecGetPrev(decoder) ((decoder)->decoded_record->xl_prev)
#define XLogRecGetInfo(decoder) ((decoder)->decoded_record->xl_info)
#define XLogRecGetRmid(decoder) ((decoder)->decoded_record->xl_rmid)
#define XLogRecGetXid(decoder) ((decoder)->decoded_record->xl_xid)
#define XLogRecGetOrigin(decoder) ((decoder)->record_origin)
#define XLogRecGetData(decoder) ((decoder)->main_data)
#define XLogRecGetDataLen(decoder) ((decoder)->main_data_len)
#define XLogRecHasAnyBlockRefs(decoder) ((decoder)->max_block_id >= 0)
#define XLogRecHasBlockRef(decoder, block_id) \
    ((decoder)->blocks[block_id].in_use)
#define XLogRecHasBlockImage(decoder, block_id) \
    ((decoder)->blocks[block_id].has_image)
#define XLogRecBlockImageApply(decoder, block_id) \
    ((decoder)->blocks[block_id].apply_image)

#ifdef __TBASE__
#define XLogRecGetEndLsn(decoder) ((decoder)->EndRecPtr - 1)
#endif
extern bool RestoreBlockImage(XLogReaderState *recoder, uint8 block_id, char *dst);
extern char *XLogRecGetBlockData(XLogReaderState *record, uint8 block_id, Size *len);
extern bool XLogRecGetBlockTag(XLogReaderState *record, uint8 block_id,
                   RelFileNode *rnode, ForkNumber *forknum,
                   BlockNumber *blknum);
extern bool XLogRecGetBlockShardID(XLogReaderState *record, uint8 block_id,
                                   ShardID *sid);

#endif                            /* XLOGREADER_H */
