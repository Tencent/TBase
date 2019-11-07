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
 * dest.h
 *      support for communication destinations
 *
 * Whenever the backend executes a query that returns tuples, the results
 * have to go someplace.  For example:
 *
 *      - stdout is the destination only when we are running a
 *        standalone backend (no postmaster) and are returning results
 *        back to an interactive user.
 *
 *      - a remote process is the destination when we are
 *        running a backend with a frontend and the frontend executes
 *        PQexec() or PQfn().  In this case, the results are sent
 *        to the frontend via the functions in backend/libpq.
 *
 *      - DestNone is the destination when the system executes
 *        a query internally.  The results are discarded.
 *
 * dest.c defines three functions that implement destination management:
 *
 * BeginCommand: initialize the destination at start of command.
 * CreateDestReceiver: return a pointer to a struct of destination-specific
 * receiver functions.
 * EndCommand: clean up the destination at end of command.
 *
 * BeginCommand/EndCommand are executed once per received SQL query.
 *
 * CreateDestReceiver returns a receiver object appropriate to the specified
 * destination.  The executor, as well as utility statements that can return
 * tuples, are passed the resulting DestReceiver* pointer.  Each executor run
 * or utility execution calls the receiver's rStartup method, then the
 * receiveSlot method (zero or more times), then the rShutdown method.
 * The same receiver object may be re-used multiple times; eventually it is
 * destroyed by calling its rDestroy method.
 *
 * In some cases, receiver objects require additional parameters that must
 * be passed to them after calling CreateDestReceiver.  Since the set of
 * parameters varies for different receiver types, this is not handled by
 * this module, but by direct calls from the calling code to receiver type
 * specific functions.
 *
 * The DestReceiver object returned by CreateDestReceiver may be a statically
 * allocated object (for destination types that require no local state),
 * in which case rDestroy is a no-op.  Alternatively it can be a palloc'd
 * object that has DestReceiver as its first field and contains additional
 * fields (see printtup.c for an example).  These additional fields are then
 * accessible to the DestReceiver functions by casting the DestReceiver*
 * pointer passed to them.  The palloc'd object is pfree'd by the rDestroy
 * method.  Note that the caller of CreateDestReceiver should take care to
 * do so in a memory context that is long-lived enough for the receiver
 * object not to disappear while still needed.
 *
 * Special provision: None_Receiver is a permanently available receiver
 * object for the DestNone destination.  This avoids useless creation/destroy
 * calls in portal and cursor manipulations.
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/tcop/dest.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DEST_H
#define DEST_H

#include "executor/tuptable.h"


/* buffer size to use for command completion tags */
#define COMPLETION_TAG_BUFSIZE    64


/* ----------------
 *        CommandDest is a simplistic means of identifying the desired
 *        destination.  Someday this will probably need to be improved.
 *
 * Note: only the values DestNone, DestDebug, DestRemote are legal for the
 * global variable whereToSendOutput.   The other values may be used
 * as the destination for individual commands.
 * ----------------
 */
typedef enum
{
    DestNone,                    /* results are discarded */
    DestDebug,                    /* results go to debugging output */
    DestRemote,                    /* results sent to frontend process */
    DestRemoteExecute,            /* sent to frontend, in Execute command */
    DestRemoteSimple,            /* sent to frontend, w/no catalog access */
    DestSPI,                    /* results sent to SPI manager */
    DestTuplestore,                /* results sent to Tuplestore */
    DestIntoRel,                /* results sent to relation (SELECT INTO) */
    DestCopyOut,                /* results sent to COPY TO code */
    DestSQLFunction,            /* results sent to SQL-language func mgr */
#ifdef XCP
    DestProducer,                /* results sent to a SharedQueue */
#ifdef __TBASE__
    DestParallelSend,           /* results send to data buffers */
#endif
#endif
    DestTransientRel,            /* results sent to transient relation */
    DestTupleQueue                /* results sent to tuple queue */
} CommandDest;

/* ----------------
 *        DestReceiver is a base type for destination-specific local state.
 *        In the simplest cases, there is no state info, just the function
 *        pointers that the executor must call.
 *
 * Note: the receiveSlot routine must be passed a slot containing a TupleDesc
 * identical to the one given to the rStartup routine.  It returns bool where
 * a "true" value means "continue processing" and a "false" value means
 * "stop early, just as if we'd reached the end of the scan".
 * ----------------
 */
typedef struct _DestReceiver DestReceiver;

struct _DestReceiver
{
    /* Called for each tuple to be output: */
    bool        (*receiveSlot) (TupleTableSlot *slot,
                                DestReceiver *self);
    /* Per-executor-run initialization and shutdown: */
    void        (*rStartup) (DestReceiver *self,
                             int operation,
                             TupleDesc typeinfo);
    void        (*rShutdown) (DestReceiver *self);
    /* Destroy the receiver object itself (if dynamically allocated) */
    void        (*rDestroy) (DestReceiver *self);
    /* CommandDest code for this receiver */
    CommandDest mydest;
    /* Private fields might appear beyond this point... */
};

extern DestReceiver *None_Receiver; /* permanent receiver for DestNone */

/* The primary destination management functions */

extern void BeginCommand(const char *commandTag, CommandDest dest);
extern DestReceiver *CreateDestReceiver(CommandDest dest);
extern void EndCommand(const char *commandTag, CommandDest dest);

/* Additional functions that go with destination management, more or less. */

extern void NullCommand(CommandDest dest);
extern void ReadyForQuery(CommandDest dest);
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
extern void
ReadyForCommit(CommandDest dest);
#endif

#endif                            /* DEST_H */
