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
 * timeout.h
 *      Routines to multiplex SIGALRM interrupts for multiple timeout reasons.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/timeout.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TIMEOUT_H
#define TIMEOUT_H

#include "datatype/timestamp.h"

/*
 * Identifiers for timeout reasons.  Note that in case multiple timeouts
 * trigger at the same time, they are serviced in the order of this enum.
 */
typedef enum TimeoutId
{
    /* Predefined timeout reasons */
    STARTUP_PACKET_TIMEOUT,
    DEADLOCK_TIMEOUT,
    LOCK_TIMEOUT,
    STATEMENT_TIMEOUT,
    STANDBY_DEADLOCK_TIMEOUT,
    STANDBY_TIMEOUT,
    STANDBY_LOCK_TIMEOUT,
    IDLE_IN_TRANSACTION_SESSION_TIMEOUT,
    /* First user-definable timeout reason */
    USER_TIMEOUT,
    /* Maximum number of timeout reasons */
    MAX_TIMEOUTS = 16
} TimeoutId;

/* callback function signature */
typedef void (*timeout_handler_proc) (void);

/*
 * Parameter structure for setting multiple timeouts at once
 */
typedef enum TimeoutType
{
    TMPARAM_AFTER,
    TMPARAM_AT
} TimeoutType;

typedef struct
{
    TimeoutId    id;                /* timeout to set */
    TimeoutType type;            /* TMPARAM_AFTER or TMPARAM_AT */
    int            delay_ms;        /* only used for TMPARAM_AFTER */
    TimestampTz fin_time;        /* only used for TMPARAM_AT */
} EnableTimeoutParams;

/*
 * Parameter structure for clearing multiple timeouts at once
 */
typedef struct
{
    TimeoutId    id;                /* timeout to clear */
    bool        keep_indicator; /* keep the indicator flag? */
} DisableTimeoutParams;

/* timeout setup */
extern void InitializeTimeouts(void);
extern TimeoutId RegisterTimeout(TimeoutId id, timeout_handler_proc handler);
extern void reschedule_timeouts(void);

/* timeout operation */
extern void enable_timeout_after(TimeoutId id, int delay_ms);
extern void enable_timeout_at(TimeoutId id, TimestampTz fin_time);
extern void enable_timeouts(const EnableTimeoutParams *timeouts, int count);
extern void disable_timeout(TimeoutId id, bool keep_indicator);
extern void disable_timeouts(const DisableTimeoutParams *timeouts, int count);
extern void disable_all_timeouts(bool keep_indicators);

/* accessors */
extern bool get_timeout_indicator(TimeoutId id, bool reset_indicator);
extern TimestampTz get_timeout_start_time(TimeoutId id);
extern TimestampTz get_timeout_finish_time(TimeoutId id);
#ifdef __TBASE__
extern void disable_timeout_safely(void);
#endif

#endif                            /* TIMEOUT_H */
