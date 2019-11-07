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
 * walreceiver.h
 *      Exports from replication/walreceiverfuncs.c.
 *
 * Portions Copyright (c) 2010-2017, PostgreSQL Global Development Group
 *
 * src/include/replication/walreceiver.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _WALRECEIVER_H
#define _WALRECEIVER_H

#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "fmgr.h"
#include "replication/logicalproto.h"
#include "replication/walsender.h"
#include "storage/latch.h"
#include "storage/spin.h"
#include "pgtime.h"
#include "utils/tuplestore.h"

/* user-settable parameters */
extern int    wal_receiver_status_interval;
extern int    wal_receiver_timeout;
extern bool hot_standby_feedback;

/*
 * MAXCONNINFO: maximum size of a connection string.
 *
 * XXX: Should this move to pg_config_manual.h?
 */
#define MAXCONNINFO        1024

/* Can we allow the standby to accept replication connection from another standby? */
#define AllowCascadeReplication() (EnableHotStandby && max_wal_senders > 0)

/*
 * Values for WalRcv->walRcvState.
 */
typedef enum
{
    WALRCV_STOPPED,                /* stopped and mustn't start up again */
    WALRCV_STARTING,            /* launched, but the process hasn't
                                 * initialized yet */
    WALRCV_STREAMING,            /* walreceiver is streaming */
    WALRCV_WAITING,                /* stopped streaming, waiting for orders */
    WALRCV_RESTARTING,            /* asked to restart streaming */
    WALRCV_STOPPING                /* requested to stop, but still running */
} WalRcvState;

/* Shared memory area for management of walreceiver process */
typedef struct
{
    /*
     * PID of currently active walreceiver process, its current state and
     * start time (actually, the time at which it was requested to be
     * started).
     */
    pid_t        pid;
    WalRcvState walRcvState;
    pg_time_t    startTime;

    /*
     * receiveStart and receiveStartTLI indicate the first byte position and
     * timeline that will be received. When startup process starts the
     * walreceiver, it sets these to the point where it wants the streaming to
     * begin.
     */
    XLogRecPtr    receiveStart;
    TimeLineID    receiveStartTLI;

    /*
     * receivedUpto-1 is the last byte position that has already been
     * received, and receivedTLI is the timeline it came from.  At the first
     * startup of walreceiver, these are set to receiveStart and
     * receiveStartTLI. After that, walreceiver updates these whenever it
     * flushes the received WAL to disk.
     */
    XLogRecPtr    receivedUpto;
    TimeLineID    receivedTLI;

    /*
     * latestChunkStart is the starting byte position of the current "batch"
     * of received WAL.  It's actually the same as the previous value of
     * receivedUpto before the last flush to disk.  Startup process can use
     * this to detect whether it's keeping up or not.
     */
    XLogRecPtr    latestChunkStart;

    /*
     * Time of send and receive of any message received.
     */
    TimestampTz lastMsgSendTime;
    TimestampTz lastMsgReceiptTime;

    /*
     * Latest reported end of WAL on the sender
     */
    XLogRecPtr    latestWalEnd;
    TimestampTz latestWalEndTime;

    /*
     * connection string; initially set to connect to the primary, and later
     * clobbered to hide security-sensitive fields.
     */
    char        conninfo[MAXCONNINFO];

    /*
     * replication slot name; is also used for walreceiver to connect with the
     * primary
     */
    char        slotname[NAMEDATALEN];

    /* set true once conninfo is ready to display (obfuscated pwds etc) */
    bool        ready_to_display;

    slock_t        mutex;            /* locks shared variables shown above */

    /*
     * force walreceiver reply?  This doesn't need to be locked; memory
     * barriers for ordering are sufficient.
     */
    bool        force_reply;

    /*
     * Latch used by startup process to wake up walreceiver after telling it
     * where to start streaming (after setting receiveStart and
     * receiveStartTLI), and also to tell it to send apply feedback to the
     * primary whenever specially marked commit records are applied. This is
     * normally mapped to procLatch when walreceiver is running.
     */
    Latch       *latch;
} WalRcvData;

extern WalRcvData *WalRcv;

typedef struct
{
    bool        logical;        /* True if this is logical replication stream,
                                 * false if physical stream.  */
    char       *slotname;        /* Name of the replication slot or NULL. */
    XLogRecPtr    startpoint;        /* LSN of starting point. */

    union
    {
        struct
        {
            TimeLineID    startpointTLI;    /* Starting timeline */
        }            physical;
        struct
        {
            uint32        proto_version;    /* Logical protocol version */
            List       *publication_names;    /* String list of publications */
        }            logical;
    }            proto;
} WalRcvStreamOptions;

struct WalReceiverConn;
typedef struct WalReceiverConn WalReceiverConn;

/*
 * Status of walreceiver query execution.
 *
 * We only define statuses that are currently used.
 */
typedef enum
{
    WALRCV_ERROR,                /* There was error when executing the query. */
    WALRCV_OK_COMMAND,            /* Query executed utility or replication
                                 * command. */
    WALRCV_OK_TUPLES,            /* Query returned tuples. */
    WALRCV_OK_COPY_IN,            /* Query started COPY FROM. */
    WALRCV_OK_COPY_OUT,            /* Query started COPY TO. */
    WALRCV_OK_COPY_BOTH            /* Query started COPY BOTH replication
                                 * protocol. */
} WalRcvExecStatus;

/*
 * Return value for walrcv_query, returns the status of the execution and
 * tuples if any.
 */
typedef struct WalRcvExecResult
{
    WalRcvExecStatus status;
    char       *err;
    Tuplestorestate *tuplestore;
    TupleDesc    tupledesc;
} WalRcvExecResult;

/* libpqwalreceiver hooks */
typedef WalReceiverConn *(*walrcv_connect_fn) (const char *conninfo, bool logical,
                                               const char *appname,
                                               char **err);
typedef void (*walrcv_check_conninfo_fn) (const char *conninfo);
typedef char *(*walrcv_get_conninfo_fn) (WalReceiverConn *conn);
typedef char *(*walrcv_identify_system_fn) (WalReceiverConn *conn,
                                            TimeLineID *primary_tli,
                                            int *server_version);
typedef void (*walrcv_readtimelinehistoryfile_fn) (WalReceiverConn *conn,
                                                   TimeLineID tli,
                                                   char **filename,
                                                   char **content, int *size);
typedef bool (*walrcv_startstreaming_fn) (WalReceiverConn *conn,
                                          const WalRcvStreamOptions *options);
typedef void (*walrcv_endstreaming_fn) (WalReceiverConn *conn,
                                        TimeLineID *next_tli);
typedef int (*walrcv_receive_fn) (WalReceiverConn *conn, char **buffer,
                                  pgsocket *wait_fd);
typedef void (*walrcv_send_fn) (WalReceiverConn *conn, const char *buffer,
                                int nbytes);
#ifdef __STORAGE_SCALABLE__
typedef char *(*walrcv_create_slot_fn) (WalReceiverConn *conn,
                                        const char *slotname, bool temporary,
                                        CRSSnapshotAction snapshot_action,
                                        XLogRecPtr *lsn, char *subname, Oid subid, char *namespace, char *relname);
#else
typedef char *(*walrcv_create_slot_fn) (WalReceiverConn *conn,
                                        const char *slotname, bool temporary,
                                        CRSSnapshotAction snapshot_action,
                                        XLogRecPtr *lsn);
#endif
typedef WalRcvExecResult *(*walrcv_exec_fn) (WalReceiverConn *conn,
                                             const char *query,
                                             const int nRetTypes,
                                             const Oid *retTypes);
typedef void (*walrcv_disconnect_fn) (WalReceiverConn *conn);

typedef struct WalReceiverFunctionsType
{
    walrcv_connect_fn walrcv_connect;
    walrcv_check_conninfo_fn walrcv_check_conninfo;
    walrcv_get_conninfo_fn walrcv_get_conninfo;
    walrcv_identify_system_fn walrcv_identify_system;
    walrcv_readtimelinehistoryfile_fn walrcv_readtimelinehistoryfile;
    walrcv_startstreaming_fn walrcv_startstreaming;
    walrcv_endstreaming_fn walrcv_endstreaming;
    walrcv_receive_fn walrcv_receive;
    walrcv_send_fn walrcv_send;
    walrcv_create_slot_fn walrcv_create_slot;
    walrcv_exec_fn walrcv_exec;
    walrcv_disconnect_fn walrcv_disconnect;
} WalReceiverFunctionsType;

extern PGDLLIMPORT WalReceiverFunctionsType *WalReceiverFunctions;

#define walrcv_connect(conninfo, logical, appname, err) \
    WalReceiverFunctions->walrcv_connect(conninfo, logical, appname, err)
#define walrcv_check_conninfo(conninfo) \
    WalReceiverFunctions->walrcv_check_conninfo(conninfo)
#define walrcv_get_conninfo(conn) \
    WalReceiverFunctions->walrcv_get_conninfo(conn)
#define walrcv_identify_system(conn, primary_tli, server_version) \
    WalReceiverFunctions->walrcv_identify_system(conn, primary_tli, server_version)
#define walrcv_readtimelinehistoryfile(conn, tli, filename, content, size) \
    WalReceiverFunctions->walrcv_readtimelinehistoryfile(conn, tli, filename, content, size)
#define walrcv_startstreaming(conn, options) \
    WalReceiverFunctions->walrcv_startstreaming(conn, options)
#define walrcv_endstreaming(conn, next_tli) \
    WalReceiverFunctions->walrcv_endstreaming(conn, next_tli)
#define walrcv_receive(conn, buffer, wait_fd) \
    WalReceiverFunctions->walrcv_receive(conn, buffer, wait_fd)
#define walrcv_send(conn, buffer, nbytes) \
    WalReceiverFunctions->walrcv_send(conn, buffer, nbytes)
#ifdef __STORAGE_SCALABLE__
#define walrcv_create_slot(conn, slotname, temporary, snapshot_action, lsn, subname, subid, relspacename, relname) \
    WalReceiverFunctions->walrcv_create_slot(conn, slotname, temporary, snapshot_action, lsn, subname, subid, relspacename, relname)
#else
#define walrcv_create_slot(conn, slotname, temporary, snapshot_action, lsn) \
    WalReceiverFunctions->walrcv_create_slot(conn, slotname, temporary, snapshot_action, lsn)
#endif
#define walrcv_exec(conn, exec, nRetTypes, retTypes) \
    WalReceiverFunctions->walrcv_exec(conn, exec, nRetTypes, retTypes)
#define walrcv_disconnect(conn) \
    WalReceiverFunctions->walrcv_disconnect(conn)

static inline void
walrcv_clear_result(WalRcvExecResult *walres)
{
    if (!walres)
        return;

    if (walres->err)
        pfree(walres->err);

    if (walres->tuplestore)
        tuplestore_end(walres->tuplestore);

    if (walres->tupledesc)
        FreeTupleDesc(walres->tupledesc);

    pfree(walres);
}

/* prototypes for functions in walreceiver.c */
extern void WalReceiverMain(void) pg_attribute_noreturn();

/* prototypes for functions in walreceiverfuncs.c */
extern Size WalRcvShmemSize(void);
extern void WalRcvShmemInit(void);
extern void ShutdownWalRcv(void);
extern bool WalRcvStreaming(void);
extern bool WalRcvRunning(void);
extern void RequestXLogStreaming(TimeLineID tli, XLogRecPtr recptr,
                     const char *conninfo, const char *slotname);
extern XLogRecPtr GetWalRcvWriteRecPtr(XLogRecPtr *latestChunkStart, TimeLineID *receiveTLI);
extern int    GetReplicationApplyDelay(void);
extern int    GetReplicationTransferLatency(void);
extern void WalRcvForceReply(void);

#endif                            /* _WALRECEIVER_H */
