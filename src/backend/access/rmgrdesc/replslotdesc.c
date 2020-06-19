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
 * replslotdesc.c
 *    rmgr descriptor routines for replication/slot.c
 *
 * Portions Copyright (c) 2019, TBase Development Group
 *
 *
 * IDENTIFICATION
 *    src/backend/access/rmgrdesc/replslotdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/replslotdesc.h"

void replication_slot_desc(StringInfo buf, XLogReaderState *record)
{
    char       *rec = XLogRecGetData(record);
    uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    switch (info)
    {
        case XLOG_REPLORIGIN_SLOT_LSN_REPLICA:
        {
            xl_replication_slot_lsn_replica *xlrec;

            xlrec = (xl_replication_slot_lsn_replica *) rec;

            appendStringInfo(buf, "info in xlrec, id:%d, xmin:%u, catalog_xmin:%u, restart_lsn %X/%X, confirmed_flush %X/%X",
                             xlrec->slotid,
                             xlrec->xmin,
                             xlrec->catalog_xmin,
                             (uint32) (xlrec->restart_lsn >> 32),
                             (uint32) xlrec->restart_lsn,
                             (uint32) (xlrec->confirmed_flush >> 32),
                             (uint32) xlrec->confirmed_flush);
            break;
        }
        case XLOG_REPLORIGIN_SLOT_CREATE:
        {
            xl_replication_slot_create *xlrec;

            xlrec = (xl_replication_slot_create *) rec;

            appendStringInfo(buf, "info in xlrec, id:%d, name:%s, database:%u, persistency:%d, xmin:%u, catalog_xmin:%u, "
                            "restart_lsn:%X/%X, confirmed_flush:%X/%X, effective_xmin:%d, effective_catalog_xmin:%d, "
                            "pgoutput:%d, subid:%u, subname:%s, relid:%u", 
                            xlrec->slotid,
                            NameStr(xlrec->slotname),
                            xlrec->database,
                            xlrec->persistency, xlrec->xmin, xlrec->catalog_xmin,
                            (uint32) (xlrec->restart_lsn >> 32), (uint32) xlrec->restart_lsn,
                            (uint32) (xlrec->confirmed_flush >> 32), (uint32) xlrec->confirmed_flush,
                            xlrec->effective_xmin, xlrec->effective_catalog_xmin,
                            xlrec->pgoutput, xlrec->subid, NameStr(xlrec->subname), xlrec->relid);
            break;
        }
        case XLOG_REPLORIGIN_SLOT_DROP:
        {
            xl_replication_slot_drop *xlrec;

            xlrec = (xl_replication_slot_drop *) rec;

            appendStringInfo(buf, "info in xlrec, id:%d, name:%s", 
                            xlrec->slotid,
                            NameStr(xlrec->slotname));
            break;
        }
        case XLOG_REPLORIGIN_SLOT_RENAME:
        {
            xl_replication_slot_rename *xlrec;

            xlrec = (xl_replication_slot_rename *) rec;

            appendStringInfo(buf, "info in xlrec, id:%d, old_name:%s, new_name:%s",
                             xlrec->slotid,
                             NameStr(xlrec->old_slotname),
                             NameStr(xlrec->new_slotname));
            break;
        }
        default:
            break;
    }
    return;
}

const char * replication_slot_identify(uint8 info)
{
    switch (info)
    {
        case XLOG_REPLORIGIN_SLOT_LSN_REPLICA:
            return "REPLORIGIN_SLOT_LSN_REPLICA";
        case XLOG_REPLORIGIN_SLOT_CREATE:
            return "REPLORIGIN_SLOT_CREATE";
        case XLOG_REPLORIGIN_SLOT_DROP:
            return "REPLORIGIN_SLOT_DROP";
        case XLOG_REPLORIGIN_SLOT_RENAME:
            return "REPLORIGIN_SLOT_RENAME";
        default:
            break;
    }
    return NULL;
}
