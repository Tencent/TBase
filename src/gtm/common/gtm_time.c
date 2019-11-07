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
 * gtm_time.c
 *            Timestamp handling on GTM
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *            $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */

#include "gtm/gtm.h"
#include "gtm/gtm_c.h"
#include "gtm/gtm_time.h"
#include <time.h>
#include <sys/time.h>

GTM_Timestamp
GTM_TimestampGetCurrent(void)
{
    struct timeval    tp;
    GTM_Timestamp    result;

    gettimeofday(&tp, NULL);

    result = (GTM_Timestamp) tp.tv_sec -
    ((GTM_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY);

#ifdef HAVE_INT64_TIMESTAMP
    result = (result * USECS_PER_SEC) + tp.tv_usec;
#else
    result = result + (tp.tv_usec / 1000000.0);
#endif

    return result;
}

GlobalTimestamp
GTM_TimestampGetMonotonicRaw(void)
{
    struct timespec     tp;
    GlobalTimestamp  result;    

    clock_gettime(CLOCK_MONOTONIC_RAW, &tp);

    result = tp.tv_sec * USECS_PER_SEC + tp.tv_nsec / 1000;
    elog(DEBUG8, "clock gettime sec "INT64_FORMAT " nsec " INT64_FORMAT " res " INT64_FORMAT, tp.tv_sec, tp.tv_nsec, result);
    
    return result;
}


GlobalTimestamp
GTM_TimestampGetMonotonicRawPrecise(GlobalTimestamp *tv_sec, GlobalTimestamp *tv_nsec)
{
    struct timespec     tp;
    GlobalTimestamp  result;

    clock_gettime(CLOCK_MONOTONIC_RAW, &tp);

    result = tp.tv_sec * USECS_PER_SEC + tp.tv_nsec / 1000;
    elog(DEBUG8, "clock gettime sec "INT64_FORMAT " nsec " INT64_FORMAT " res " INT64_FORMAT, tp.tv_sec, tp.tv_nsec, result);
    *tv_sec = tp.tv_sec;
    *tv_nsec = tp.tv_nsec;
    
    return result;
}

/*
 * TimestampDifference -- convert the difference between two timestamps
 *        into integer seconds and microseconds
 *
 * Both inputs must be ordinary finite timestamps (in current usage,
 * they'll be results from GTM_TimestampGetCurrent()).
 *
 * We expect start_time <= stop_time.  If not, we return zeroes; for current
 * callers there is no need to be tense about which way division rounds on
 * negative inputs.
 */
void
GTM_TimestampDifference(GTM_Timestamp start_time, GTM_Timestamp stop_time,
                    long *secs, int *microsecs)
{
    GTM_Timestamp diff = stop_time - start_time;

    if (diff <= 0)
    {
        *secs = 0;
        *microsecs = 0;
    }
    else
    {
#ifdef HAVE_INT64_TIMESTAMP
        *secs = (long) (diff / USECS_PER_SEC);
        *microsecs = (int) (diff % USECS_PER_SEC);
#else
        *secs = (long) diff;
        *microsecs = (int) ((diff - *secs) * 1000000.0);
#endif
    }
}

/*
 * GTM_TimestampDifferenceExceeds -- report whether the difference between two
 *        timestamps is >= a threshold (expressed in milliseconds)
 *
 * Both inputs must be ordinary finite timestamps (in current usage,
 * they'll be results from GTM_TimestampDifferenceExceeds()).
 */
bool
GTM_TimestampDifferenceExceeds(GTM_Timestamp start_time,
                           GTM_Timestamp stop_time,
                           int msec)
{
    GTM_Timestamp diff = stop_time - start_time;

#ifdef HAVE_INT64_TIMESTAMP
    return (diff >= msec * INT64CONST(1000));
#else
    return (diff * 1000.0 >= msec);
#endif
}
