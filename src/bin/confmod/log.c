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
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <sys/types.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <fcntl.h>

#include "log.h"
#include "util.h"

static FILE *logFile = NULL;
static int logMsgLevel = INFO;

static char *fname = NULL;
static char *funcname = NULL;
static int lineno = -1;

static char timebuf[MAXTOKEN+1] = { 0 };
static char *elog_time(void)
{
    struct tm *tm_s;
    time_t now;

    now = time(NULL);
    tm_s = localtime(&now);

    snprintf(timebuf, MAXTOKEN, "%02d-%02d-%02d %02d:%02d:%02d", 
             ((tm_s->tm_year+1900) >= 2000) ? (tm_s->tm_year + (1900 - 2000)) : tm_s->tm_year, 
             tm_s->tm_mon+1, tm_s->tm_mday, tm_s->tm_hour, tm_s->tm_min, tm_s->tm_sec);
    return timebuf;
}

void
elog_start(const char *file, const char *func, int line)
{
    fname = Strdup(file);
    funcname = Strdup(func);
    lineno = line;

    if (!logFile)
        logFile = stderr;
}

static void
clean_location(void)
{
    freeAndReset(fname);
    freeAndReset(funcname);
    lineno = -1;
}

static const char * 
elog_level(int level)
{// #lizard forgives
    const char * ret = NULL;
    switch(level)
    {
        case DEBUG3:
            ret = "DEBUG3";
            break;
        case DEBUG2:
            ret = "DEBUG2";
            break;
        case DEBUG1:
            ret = "DEBUG1";
            break;
        case INFO:
            ret = "INFO";
            break;
        case NOTICE2:
            ret = "NOTICE2";
            break;
        case NOTICE:
            ret = "NOTICE";
            break;
        case WARNING:
            ret = "WARNING";
            break;
        case ERROR:
            ret = "ERROR";
            break;
        case PANIC:
            ret = "PANIC";
            break;
        case MANDATORY:
            ret = "MANDATORY";
            break;
        default:
            ret = "UNKNOWN";
            break;
    }

    return ret;
}

static void
elogMsgRaw(int level, const char *msg)
{
    if (logFile && level >= logMsgLevel)
    {
        fprintf(logFile, "[%s] [%s:%s:%d] [%s]: %s", 
            elog_time(), fname, funcname, lineno, elog_level(level), msg);
        fflush(logFile);
    }
    clean_location();
}

void
elogFinish(int level, const char *fmt, ...)
{
    char msg[MAXLINE+1];
    va_list arg;

    va_start(arg, fmt);
    vsnprintf(msg, MAXLINE, fmt, arg);
    va_end(arg);
    elogMsgRaw(level, msg);
}
