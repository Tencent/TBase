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
#ifndef GTM_XLOG_INTERNAL_H
#define GTM_XLOG_INTERNAL_H

#include <signal.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h> 
#include <sys/types.h>

#include "gtm/gtm_c.h"
#include "gtm/gtm_lock.h"
#include "gtm/gtm_checkpoint.h"
#include "utils/pg_crc.h"
#include "libpq-be.h"

#define GTM_XLOG_PAGE_MAGIC     0xFFFF
#define GTM_XLOG_SEG_SIZE       (1024 * 1024 * 2) /* 2M */
#define GTM_XLOG_BLCKSZ         (4 * 1024) /* 4k */

#define GTM_MAX_COMMAND_SIZE 1024

#define FIRST_TIMELINE_ID   0x01

#define FIRST_XLOG_REC      GTM_XLOG_BLCKSZ
#define FIRST_USABLE_BYTE   UsableBytesInPage

#define GTMXLogSegmentsPerXLogId    (UINT64CONST(0x100000000) / GTM_XLOG_SEG_SIZE)

#define GTMArchiverCheckInterval (1000 * 200) /* 200ms */

#define MAXFNAMELEN        64

#define MAX_COMMAND_LEN 2048

#define GTMXLogFileName(fname, tli, logSegNo)    \
    snprintf(fname, MAXFNAMELEN, "gtm_xlog/%08X%08X%08X", tli,        \
            (uint32) ((logSegNo) / GTMXLogSegmentsPerXLogId), \
            (uint32) ((logSegNo) % GTMXLogSegmentsPerXLogId))

#define GTMXLogFileNameWithoutGtmDir(fname, tli, logSegNo)    \
    snprintf(fname, MAXFNAMELEN, "%08X%08X%08X", tli,        \
            (uint32) ((logSegNo) / GTMXLogSegmentsPerXLogId), \
            (uint32) ((logSegNo) % GTMXLogSegmentsPerXLogId))

#define GTMXLogFileStatusReadyName(fname, tli, logSegNo)    \
    snprintf(fname, MAXFNAMELEN, "gtm_xlog/archive_status/%08X%08X%08X.ready", tli,        \
            (uint32) ((logSegNo) / GTMXLogSegmentsPerXLogId), \
            (uint32) ((logSegNo) % GTMXLogSegmentsPerXLogId))

#define GTMXLogFileStatusDoneName(fname, tli, logSegNo)    \
    snprintf(fname, MAXFNAMELEN, "gtm_xlog/archive_status/%08X%08X%08X.done", tli,        \
            (uint32) ((logSegNo) / GTMXLogSegmentsPerXLogId), \
            (uint32) ((logSegNo) % GTMXLogSegmentsPerXLogId))

#define GTMXLogFileGtsName(fname, tli, logSegNo)    \
    snprintf(fname, MAXFNAMELEN, "gtm_xlog/archive_status/%08X%08X%08X.gts", tli,        \
            (uint32) ((logSegNo) / GTMXLogSegmentsPerXLogId), \
            (uint32) ((logSegNo) % GTMXLogSegmentsPerXLogId))

#define RECOVERY_CONF_NAME "recovery.conf"
#define RECOVERY_CONF_NAME_DONE "recovery.done"

#define GTM_XLOG_RECOVERY_GTS_VALIDATE_NUM 5

typedef enum  
{ 
    XLOG_CMD_RANGE_OVERWRITE,   /* OverWrite range for map file */ 
    XLOG_CMD_CHECK_POINT,       /* CheckPoint record,Redo will find this record and start */
    XLOG_REC_GTS                /* Record gts in case of gts overflip */
} XlogRecType;

/*
 * The overall layout of an XLOG record is:
 *        Fixed-size header (XLogRecord struct)
 *        XLogCommand struct
 *        XLogCommand struct
 *        ...
 */
typedef struct XLogRecord
{
    uint32          xl_tot_len;     /* total len of entire record */
    XLogRecPtr      xl_prev;         /* ptr to previous record in log */
    uint8           xl_info;         /* flag bits, for further use,useless for now */
    pg_time_t       xl_time;
    GlobalTimestamp xl_timestamp;
    /* 2 bytes of padding here, initialize to zero */
    pg_crc32c     xl_crc;         /* CRC for this record */
} XLogRecord; 

typedef struct XLogPageHeaderData
{
    uint16        xlp_magic;            /* magic value for correctness checks */
    uint16        xlp_info;             /* flag bits, see below */
    XLogRecPtr    xlp_pageaddr;         /* XLOG address of this page */
} XLogPageHeaderData;

#define UsableBytesInPage    (GTM_XLOG_BLCKSZ - sizeof(XLogPageHeaderData))
#define UsableBytesInSegment ((GTM_XLOG_SEG_SIZE/GTM_XLOG_BLCKSZ) * UsableBytesInPage)

/*
 * Data structure stored in xlog files.
 * structure in xlog file arrage looks like this exclude page header
 * XLogRecord XLogCommand XLogCommand XLogCommand
 */
typedef struct
{
    int32_t type;     /* indicate XlogRecType */
    int32_t len;      /* len of xlog cmd data */
    char data[0];
} XLogCommand;

#define XLogCommadLength(cmd) (sizeof(XLogCommand) + cmd->len)

typedef struct GTM_CheckStandbyStatusResult
{
    char                ip[MAXFNAMELEN];
    char                port[MAXFNAMELEN];

    XLogRecPtr          flush_ptr;
    XLogRecPtr          write_ptr;
    XLogRecPtr          replay_ptr;

    GTM_Timestamp timestamp;

} GTM_CheckStandbyStatusResult;

/*
 * The functions in gtm_xlog.c construct a chain of XLogRecData structs
 * to represent the final WAL record.
 */
typedef struct XLogRecData
{
    struct XLogRecData *next;    /* next struct in chain, or NULL */
    char      *data;             /* start of data to include */
    uint32    len;               /* length of data to include */
} XLogRecData;

/*
 * Record xlog data chain list.
 */
typedef struct
{
    uint32        rdata_len;        /* total length of data in rdata chain */
    XLogRecData   *rdata_head;      /* head of the chain of data registered with this block */
    XLogRecData   *rdata_rear;      /* end of the chain of data registered with this block */
} XLogRegisterBuff;
/*
 * Command header for all xlog command types.
 */
typedef struct
{
    uint32 type;
} XLogCmdHeader;

typedef struct
{
    XLogCmdHeader hdr;
    int32_t offset;     /* offset of the map file */
    int32_t bytes;
    char data[0];
} XLogCmdRangerOverWrite;

typedef struct
{
    XLogCmdHeader hdr;
    GlobalTimestamp gts;
    pg_time_t  time;
    TimeLineID timeline;
} XLogCmdCheckPoint;

typedef struct
{
    XLogCmdHeader hdr;
    GlobalTimestamp gts;
} XLogRecGts;

typedef struct
{
    uint64  segment_no;
    int     total_length;    /* indicate the total length of xlog ,-1 mean not read */
    char    buff[GTM_XLOG_SEG_SIZE];
} GTM_XLogSegmentBuff;

typedef struct GTM_ThreadInfo GTM_ThreadInfo ;

typedef struct
{
     GTM_MutexLock       lock;
     bool                is_use;
     char                application_name[MAXFNAMELEN];
     char                node_name[MAXFNAMELEN];

     /* protects flush_ptr,write_ptr,replay_ptr,time_line */
     GTM_MutexLock       pos_status_lck;
     GTM_CV              pos_update_cv;
     XLogRecPtr          flush_ptr;
     XLogRecPtr          write_ptr;
     XLogRecPtr          replay_ptr;
     TimeLineID          time_line;


     XLogRecPtr          send_ptr;

     GTM_CV              xlog_to_send;
     GTM_MutexLock       send_request_lck;
     XLogRecPtr          send_request;

     GTM_ThreadInfo      *walsender_thread;
     Port                *port;

     GTM_XLogSegmentBuff xlog_read_buff;

     bool                is_sync;

     XLogRecPtr          next_sync_pos;
     bool                sync_hint;

} GTM_StandbyReplication;

typedef struct XLogWaiter {
    XLogRecPtr pos;
    GTM_CV  cv;
    GTM_MutexLock lock;
    bool  finished;
} XLogWaiter;
#endif
