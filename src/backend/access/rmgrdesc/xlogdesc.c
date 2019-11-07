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
 * xlogdesc.c
 *      rmgr descriptor routines for access/transam/xlog.c
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *      src/backend/access/rmgrdesc/xlogdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "catalog/pg_control.h"
#include "utils/guc.h"
#include "utils/timestamp.h"

/*
 * GUC support
 */
const struct config_enum_entry wal_level_options[] = {
    {"minimal", WAL_LEVEL_MINIMAL, false},
    {"replica", WAL_LEVEL_REPLICA, false},
    {"archive", WAL_LEVEL_REPLICA, true},    /* deprecated */
    {"hot_standby", WAL_LEVEL_REPLICA, true},    /* deprecated */
    {"logical", WAL_LEVEL_LOGICAL, false},
    {NULL, 0, false}
};

void
xlog_desc(StringInfo buf, XLogReaderState *record)
{// #lizard forgives
    char       *rec = XLogRecGetData(record);
    uint8        info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    if (info == XLOG_CHECKPOINT_SHUTDOWN ||
        info == XLOG_CHECKPOINT_ONLINE)
    {
        CheckPoint *checkpoint = (CheckPoint *) rec;
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
            appendStringInfo(buf, "redo %X/%X; "
                             "tli %u; prev tli %u; fpw %s; xid %u:%u; latest commit timetamp " INT64_FORMAT "; latest global timetamp " INT64_FORMAT ";oid %u; multi %u; offset %u; "
                             "oldest xid %u in DB %u; oldest multi %u in DB %u; "
                             "oldest/newest commit timestamp xid: %u/%u; "
                             "oldest running xid %u; %s",
                             (uint32) (checkpoint->redo >> 32), (uint32) checkpoint->redo,
                             checkpoint->ThisTimeLineID,
                             checkpoint->PrevTimeLineID,
                             checkpoint->fullPageWrites ? "true" : "false",
                             checkpoint->nextXidEpoch, checkpoint->nextXid,
                             checkpoint->latestCommitTs,
                             checkpoint->latestGTS,
                             checkpoint->nextOid,
                             checkpoint->nextMulti,
                             checkpoint->nextMultiOffset,
                             checkpoint->oldestXid,
                             checkpoint->oldestXidDB,
                             checkpoint->oldestMulti,
                             checkpoint->oldestMultiDB,
                             checkpoint->oldestCommitTsXid,
                             checkpoint->newestCommitTsXid,
                             checkpoint->oldestActiveXid,
                             (info == XLOG_CHECKPOINT_SHUTDOWN) ? "shutdown" : "online");
#else

        appendStringInfo(buf, "redo %X/%X; "
                         "tli %u; prev tli %u; fpw %s; xid %u:%u; oid %u; multi %u; offset %u; "
                         "oldest xid %u in DB %u; oldest multi %u in DB %u; "
                         "oldest/newest commit timestamp xid: %u/%u; "
                         "oldest running xid %u; %s",
                         (uint32) (checkpoint->redo >> 32), (uint32) checkpoint->redo,
                         checkpoint->ThisTimeLineID,
                         checkpoint->PrevTimeLineID,
                         checkpoint->fullPageWrites ? "true" : "false",
                         checkpoint->nextXidEpoch, checkpoint->nextXid,
                         checkpoint->nextOid,
                         checkpoint->nextMulti,
                         checkpoint->nextMultiOffset,
                         checkpoint->oldestXid,
                         checkpoint->oldestXidDB,
                         checkpoint->oldestMulti,
                         checkpoint->oldestMultiDB,
                         checkpoint->oldestCommitTsXid,
                         checkpoint->newestCommitTsXid,
                         checkpoint->oldestActiveXid,
                         (info == XLOG_CHECKPOINT_SHUTDOWN) ? "shutdown" : "online");
#endif
    }
    else if (info == XLOG_CLEAN_2PC_FILE)
    {
        appendStringInfo(buf, "clean 2pc file %s", rec);
    }
    else if (info == XLOG_CREATE_2PC_FILE)
    {
        appendStringInfo(buf, "create 2pc file %s", rec);
    }
    else if (info == XLOG_RECORD_2PC_TIMESTAMP)
    {
        appendStringInfo(buf, "record global commit timestamp in 2pc file %s", rec);
    }
    else if (info == XLOG_NEXTOID)
    {
        Oid            nextOid;

        memcpy(&nextOid, rec, sizeof(Oid));
        appendStringInfo(buf, "%u", nextOid);
    }
    else if (info == XLOG_RESTORE_POINT)
    {
        xl_restore_point *xlrec = (xl_restore_point *) rec;

        appendStringInfoString(buf, xlrec->rp_name);
    }
    else if (info == XLOG_FPI || info == XLOG_FPI_FOR_HINT)
    {
        /* no further information to print */
    }
    else if (info == XLOG_BACKUP_END)
    {
        XLogRecPtr    startpoint;

        memcpy(&startpoint, rec, sizeof(XLogRecPtr));
        appendStringInfo(buf, "%X/%X",
                         (uint32) (startpoint >> 32), (uint32) startpoint);
    }
    else if (info == XLOG_PARAMETER_CHANGE)
    {
        xl_parameter_change xlrec;
        const char *wal_level_str;
        const struct config_enum_entry *entry;

        memcpy(&xlrec, rec, sizeof(xl_parameter_change));

        /* Find a string representation for wal_level */
        wal_level_str = "?";
        for (entry = wal_level_options; entry->name; entry++)
        {
            if (entry->val == xlrec.wal_level)
            {
                wal_level_str = entry->name;
                break;
            }
        }

        appendStringInfo(buf, "max_connections=%d max_worker_processes=%d "
                         "max_prepared_xacts=%d max_locks_per_xact=%d "
                         "wal_level=%s wal_log_hints=%s "
                         "track_commit_timestamp=%s",
                         xlrec.MaxConnections,
                         xlrec.max_worker_processes,
                         xlrec.max_prepared_xacts,
                         xlrec.max_locks_per_xact,
                         wal_level_str,
                         xlrec.wal_log_hints ? "on" : "off",
                         xlrec.track_commit_timestamp ? "on" : "off");
    }
    else if (info == XLOG_FPW_CHANGE)
    {
        bool        fpw;

        memcpy(&fpw, rec, sizeof(bool));
        appendStringInfoString(buf, fpw ? "true" : "false");
    }
    else if (info == XLOG_END_OF_RECOVERY)
    {
        xl_end_of_recovery xlrec;

        memcpy(&xlrec, rec, sizeof(xl_end_of_recovery));
        appendStringInfo(buf, "tli %u; prev tli %u; time %s",
                         xlrec.ThisTimeLineID, xlrec.PrevTimeLineID,
                         timestamptz_to_str(xlrec.end_time));
    }
#ifdef __TBASE__
    else if (info == XLOG_MVCC)
    {
        int32    need_mvcc;

        memcpy(&need_mvcc, rec, sizeof(int32));
        appendStringInfo(buf, "%d", need_mvcc);
    }
#endif
}

const char *
xlog_identify(uint8 info)
{// #lizard forgives
    const char *id = NULL;

    switch (info & ~XLR_INFO_MASK)
    {
        case XLOG_CHECKPOINT_SHUTDOWN:
            id = "CHECKPOINT_SHUTDOWN";
            break;
        case XLOG_CHECKPOINT_ONLINE:
            id = "CHECKPOINT_ONLINE";
            break;
        case XLOG_NOOP:
            id = "NOOP";
            break;
        case XLOG_NEXTOID:
            id = "NEXTOID";
            break;
        case XLOG_SWITCH:
            id = "SWITCH";
            break;
        case XLOG_BACKUP_END:
            id = "BACKUP_END";
            break;
        case XLOG_PARAMETER_CHANGE:
            id = "PARAMETER_CHANGE";
            break;
        case XLOG_RESTORE_POINT:
            id = "RESTORE_POINT";
            break;
        case XLOG_FPW_CHANGE:
            id = "FPW_CHANGE";
            break;
        case XLOG_END_OF_RECOVERY:
            id = "END_OF_RECOVERY";
            break;
        case XLOG_FPI:
            id = "FPI";
            break;
        case XLOG_FPI_FOR_HINT:
            id = "FPI_FOR_HINT";
            break;
        case XLOG_CLEAN_2PC_FILE:
            id = "CLEAN_2PC_FILE";
            break;
        case XLOG_CREATE_2PC_FILE:
            id = "CREATE_2PC_FILE";
            break;
        case XLOG_RECORD_2PC_TIMESTAMP:
            id = "XLOG_RECORD_2PC_TIMESTAMP";
            break;
#ifdef __TBASE__
        case XLOG_MVCC:
            id = "NEED_MVCC";
            break;
#endif
    }

    return id;
}
