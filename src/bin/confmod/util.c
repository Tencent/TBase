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
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#include <stdio.h>
#include <time.h>
#include <stdio.h>

#include "util.h"
#include "log.h"

static int Malloc_ed = 0;
static int Strdup_ed = 0;
static int Freed = 0;

void
*Malloc(size_t size)
{
    void *rv = malloc(size);

    Malloc_ed++;
    if (rv == NULL)
    {
        elog(PANIC, "No more memory.  See core file for details.\n");
        abort();
        exit(1);
    }
    return(rv);
}

void *
Malloc0(size_t size)
{
    void *rv = malloc(size);

    Malloc_ed++;
    if (rv == NULL)
    {
        elog(PANIC, "No more memory.  See core file for details. \n");
        abort();
        exit(1);
    }
    memset(rv, 0, size);
    return(rv);
}

void *
Realloc(void *ptr, size_t size)
{
    void *rv = realloc(ptr, size);

    if (rv == NULL)
    {
        elog(PANIC, "No more memory.  See core file for details. \n");
        abort();
        exit(1);
    }
    return(rv);
}

void
Free(void *ptr)
{
    Freed++;
    if (ptr)
        free(ptr);
}

/*
 * If flag is TRUE and chdir fails, then exit(1)
 */
int
Chdir(char *path, int flag)
{
    if (chdir(path))
    {
        elog(ERROR, "Could not change work directory to \"%s\". %s%s \n", 
             path, flag == TRUE ? "Exiting. " : "", strerror(errno));
        if (flag == TRUE)
            exit(1);
        else
            return -1;
    }

    return 0;
}

FILE *
Fopen(char *path, char *mode)
{
    FILE *rv;

    if ((rv = fopen(path, mode)) == NULL)
        elog(ERROR, "Could not open the file \"%s\" in \"%s\", %s\n", path, mode, strerror(errno));
    return(rv);
}

char *
Strdup(const char *s)
{
    char *rv;

    Strdup_ed++;
    rv = strdup(s);
    if (rv == NULL)
    {
        elog(PANIC, "No more memory.  See core file for details.\n");
        abort();
        exit(1);
    }
    return(rv);
}

void
TrimNl(char *s)
{
    for (;*s && *s != '\n'; s++);
    *s = 0;
}

void
MyUsleep(long microsec)
{
    struct timeval delay;

    if (microsec <= 0)
        return;

    delay.tv_sec = microsec / 1000000L;
    delay.tv_usec = microsec % 1000000L;
    (void) select(0, NULL, NULL, NULL, &delay);
}
