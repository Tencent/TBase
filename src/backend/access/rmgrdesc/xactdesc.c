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
 * xactdesc.c
 *      rmgr descriptor routines for access/transam/xact.c
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *      src/backend/access/rmgrdesc/xactdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/transam.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "storage/sinval.h"
#include "storage/standbydefs.h"
#include "utils/timestamp.h"

/*
 * Parse the WAL format of an xact commit and abort records into an easier to
 * understand format.
 *
 * This routines are in xactdesc.c because they're accessed in backend (when
 * replaying WAL) and frontend (pg_waldump) code. This file is the only xact
 * specific one shared between both. They're complicated enough that
 * duplication would be bothersome.
 */

void
ParseCommitRecord(uint8 info, xl_xact_commit *xlrec, xl_xact_parsed_commit *parsed)
{// #lizard forgives
    char       *data = ((char *) xlrec) + MinSizeOfXactCommit;

    memset(parsed, 0, sizeof(*parsed));

    parsed->xinfo = 0;            /* default, if no XLOG_XACT_HAS_INFO is
                                 * present */

    parsed->xact_time = xlrec->xact_time;
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
    parsed->global_timestamp = xlrec->global_timestamp;
#endif

    if (info & XLOG_XACT_HAS_INFO)
    {
        xl_xact_xinfo *xl_xinfo = (xl_xact_xinfo *) data;

        parsed->xinfo = xl_xinfo->xinfo;

        data += sizeof(xl_xact_xinfo);
    }

    if (parsed->xinfo & XACT_XINFO_HAS_DBINFO)
    {
        xl_xact_dbinfo *xl_dbinfo = (xl_xact_dbinfo *) data;

        parsed->dbId = xl_dbinfo->dbId;
        parsed->tsId = xl_dbinfo->tsId;

        data += sizeof(xl_xact_dbinfo);
    }

    if (parsed->xinfo & XACT_XINFO_HAS_SUBXACTS)
    {
        xl_xact_subxacts *xl_subxacts = (xl_xact_subxacts *) data;

        parsed->nsubxacts = xl_subxacts->nsubxacts;
        parsed->subxacts = xl_subxacts->subxacts;

        data += MinSizeOfXactSubxacts;
        data += parsed->nsubxacts * sizeof(TransactionId);
    }

    if (parsed->xinfo & XACT_XINFO_HAS_RELFILENODES)
    {
        xl_xact_relfilenodes *xl_relfilenodes = (xl_xact_relfilenodes *) data;

        parsed->nrels = xl_relfilenodes->nrels;
        parsed->xnodes = xl_relfilenodes->xnodes;

        data += MinSizeOfXactRelfilenodes;
        data += xl_relfilenodes->nrels * sizeof(RelFileNode);
    }

    if (parsed->xinfo & XACT_XINFO_HAS_INVALS)
    {
        xl_xact_invals *xl_invals = (xl_xact_invals *) data;

        parsed->nmsgs = xl_invals->nmsgs;
        parsed->msgs = xl_invals->msgs;

        data += MinSizeOfXactInvals;
        data += xl_invals->nmsgs * sizeof(SharedInvalidationMessage);
    }

    if (parsed->xinfo & XACT_XINFO_HAS_TWOPHASE)
    {
        xl_xact_twophase *xl_twophase = (xl_xact_twophase *) data;

        parsed->twophase_xid = xl_twophase->xid;

        data += sizeof(xl_xact_twophase);
    }

    if (parsed->xinfo & XACT_XINFO_HAS_ORIGIN)
    {
        xl_xact_origin xl_origin;

        /* we're only guaranteed 4 byte alignment, so copy onto stack */
        memcpy(&xl_origin, data, sizeof(xl_origin));

        parsed->origin_lsn = xl_origin.origin_lsn;
        parsed->origin_timestamp = xl_origin.origin_timestamp;

        data += sizeof(xl_xact_origin);
    }
}

void
ParseAbortRecord(uint8 info, xl_xact_abort *xlrec, xl_xact_parsed_abort *parsed)
{
    char       *data = ((char *) xlrec) + MinSizeOfXactAbort;

    memset(parsed, 0, sizeof(*parsed));

    parsed->xinfo = 0;            /* default, if no XLOG_XACT_HAS_INFO is
                                 * present */

    parsed->xact_time = xlrec->xact_time;
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
    parsed->global_timestamp = xlrec->global_timestamp;
#endif


    if (info & XLOG_XACT_HAS_INFO)
    {
        xl_xact_xinfo *xl_xinfo = (xl_xact_xinfo *) data;

        parsed->xinfo = xl_xinfo->xinfo;

        data += sizeof(xl_xact_xinfo);
    }

    if (parsed->xinfo & XACT_XINFO_HAS_SUBXACTS)
    {
        xl_xact_subxacts *xl_subxacts = (xl_xact_subxacts *) data;

        parsed->nsubxacts = xl_subxacts->nsubxacts;
        parsed->subxacts = xl_subxacts->subxacts;

        data += MinSizeOfXactSubxacts;
        data += parsed->nsubxacts * sizeof(TransactionId);
    }

    if (parsed->xinfo & XACT_XINFO_HAS_RELFILENODES)
    {
        xl_xact_relfilenodes *xl_relfilenodes = (xl_xact_relfilenodes *) data;

        parsed->nrels = xl_relfilenodes->nrels;
        parsed->xnodes = xl_relfilenodes->xnodes;

        data += MinSizeOfXactRelfilenodes;
        data += xl_relfilenodes->nrels * sizeof(RelFileNode);
    }

    if (parsed->xinfo & XACT_XINFO_HAS_TWOPHASE)
    {
        xl_xact_twophase *xl_twophase = (xl_xact_twophase *) data;

        parsed->twophase_xid = xl_twophase->xid;

        data += sizeof(xl_xact_twophase);
    }
}

static void
xact_desc_commit(StringInfo buf, uint8 info, xl_xact_commit *xlrec, RepOriginId origin_id)
{// #lizard forgives
    xl_xact_parsed_commit parsed;
    int            i;

    ParseCommitRecord(info, xlrec, &parsed);

    /* If this is a prepared xact, show the xid of the original xact */
    if (TransactionIdIsValid(parsed.twophase_xid))
        appendStringInfo(buf, "%u: ", parsed.twophase_xid);

    appendStringInfoString(buf, timestamptz_to_str(xlrec->xact_time));

    if (parsed.nrels > 0)
    {
        appendStringInfoString(buf, "; rels:");
        for (i = 0; i < parsed.nrels; i++)
        {
            char       *path = relpathperm_client(parsed.xnodes[i],
                    MAIN_FORKNUM, "");

            appendStringInfo(buf, " %s", path);
            pfree(path);
        }
    }
    if (parsed.nsubxacts > 0)
    {
        appendStringInfoString(buf, "; subxacts:");
        for (i = 0; i < parsed.nsubxacts; i++)
            appendStringInfo(buf, " %u", parsed.subxacts[i]);
    }
    if (parsed.nmsgs > 0)
    {
        standby_desc_invalidations(
                                   buf, parsed.nmsgs, parsed.msgs, parsed.dbId, parsed.tsId,
                                   XactCompletionRelcacheInitFileInval(parsed.xinfo));
    }

    if (XactCompletionForceSyncCommit(parsed.xinfo))
        appendStringInfoString(buf, "; sync");

    if (parsed.xinfo & XACT_XINFO_HAS_ORIGIN)
    {
        appendStringInfo(buf, "; origin: node %u, lsn %X/%X, at %s",
                         origin_id,
                         (uint32) (parsed.origin_lsn >> 32),
                         (uint32) parsed.origin_lsn,
                         timestamptz_to_str(parsed.origin_timestamp));
    }
}

static void
xact_desc_abort(StringInfo buf, uint8 info, xl_xact_abort *xlrec)
{
    xl_xact_parsed_abort parsed;
    int            i;

    ParseAbortRecord(info, xlrec, &parsed);

    /* If this is a prepared xact, show the xid of the original xact */
    if (TransactionIdIsValid(parsed.twophase_xid))
        appendStringInfo(buf, "%u: ", parsed.twophase_xid);

    appendStringInfoString(buf, timestamptz_to_str(xlrec->xact_time));
    if (parsed.nrels > 0)
    {
        appendStringInfoString(buf, "; rels:");
        for (i = 0; i < parsed.nrels; i++)
        {
            char       *path = relpathperm_client(parsed.xnodes[i],
                    MAIN_FORKNUM, "");

            appendStringInfo(buf, " %s", path);
            pfree(path);
        }
    }

    if (parsed.nsubxacts > 0)
    {
        appendStringInfoString(buf, "; subxacts:");
        for (i = 0; i < parsed.nsubxacts; i++)
            appendStringInfo(buf, " %u", parsed.subxacts[i]);
    }
}

static void
xact_desc_assignment(StringInfo buf, xl_xact_assignment *xlrec)
{
    int            i;

    appendStringInfoString(buf, "subxacts:");

    for (i = 0; i < xlrec->nsubxacts; i++)
        appendStringInfo(buf, " %u", xlrec->xsub[i]);
}

void
xact_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & XLOG_XACT_OPMASK;

	if (info == XLOG_XACT_COMMIT || info == XLOG_XACT_COMMIT_PREPARED)
	{
		xl_xact_commit *xlrec = (xl_xact_commit *) rec;

		xact_desc_commit(buf, XLogRecGetInfo(record), xlrec,
						 XLogRecGetOrigin(record));
	}
	else if (info == XLOG_XACT_ABORT || info == XLOG_XACT_ABORT_PREPARED)
	{
		xl_xact_abort *xlrec = (xl_xact_abort *) rec;

		xact_desc_abort(buf, XLogRecGetInfo(record), xlrec);
	}
	else if (info == XLOG_XACT_ASSIGNMENT)
	{
		xl_xact_assignment *xlrec = (xl_xact_assignment *) rec;

		/*
		 * Note that we ignore the WAL record's xid, since we're more
		 * interested in the top-level xid that issued the record and which
		 * xids are being reported here.
		 */
		appendStringInfo(buf, "xtop %u: ", xlrec->xtop);
		xact_desc_assignment(buf, xlrec);
	}
#ifdef __TBASE__
	else if (info == XLOG_XACT_ACQUIRE_GTS)
	{
		xl_xact_acquire_gts *xlrec = (xl_xact_acquire_gts *) rec;
		appendStringInfo(buf, "acquire global timestamp "INT64_FORMAT" ", xlrec->global_timestamp);
	}
#endif
}

const char *
xact_identify(uint8 info)
{// #lizard forgives
    const char *id = NULL;

    switch (info & XLOG_XACT_OPMASK)
    {
        case XLOG_XACT_COMMIT:
            id = "COMMIT";
            break;
        case XLOG_XACT_PREPARE:
            id = "PREPARE";
            break;
        case XLOG_XACT_ABORT:
            id = "ABORT";
            break;
        case XLOG_XACT_COMMIT_PREPARED:
            id = "COMMIT_PREPARED";
            break;
        case XLOG_XACT_ABORT_PREPARED:
            id = "ABORT_PREPARED";
            break;
        case XLOG_XACT_ASSIGNMENT:
            id = "ASSIGNMENT";
            break;
#ifdef __TBASE__
        case XLOG_XACT_ACQUIRE_GTS:
            id = "ACQUIRE_GTS";
            break;
#endif            
    }

    return id;
}
