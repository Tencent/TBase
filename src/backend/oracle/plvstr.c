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
/*
  This code implements one part of functonality of
  free available library PL/Vision. Please look www.quest.com

  Original author: Steven Feuerstein, 1996 - 2002
  PostgreSQL implementation author: Pavel Stehule, 2006

  This module is under BSD Licence

  History:
    1.0. first public version 13. March 2006
*/


#include "postgres.h"
#include "utils/builtins.h"
#include "utils/numeric.h"
#include "string.h"
#include "stdlib.h"
#include "utils/pg_locale.h"
#include "mb/pg_wchar.h"
#include "nodes/execnodes.h"

#include "catalog/pg_type.h"
#include "libpq/pqformat.h"
#include "oracle/oracle.h"

#define PARAMETER_ERROR(detail) \
    ereport(ERROR, \
        (errcode(ERRCODE_INVALID_PARAMETER_VALUE), \
         errmsg("invalid parameter"), \
         errdetail(detail)));

#ifndef _pg_mblen
#define _pg_mblen    pg_mblen
#endif

typedef enum
{
    POSITION,
    FIRST,
    LAST
}  position_mode;

/*
 * Make substring, can handle negative start
 *
 */
static int
orcl_mb_strlen(text *str, char **sizes, int **positions)
{
    int r_len;
    int cur_size = 0;
    int sz;
    char *p;
    int cur = 0;

    p = VARDATA_ANY(str);
    r_len = VARSIZE_ANY_EXHDR(str);

    if (NULL != sizes)
        *sizes = palloc(r_len * sizeof(char));
    if (NULL != positions)
        *positions = palloc(r_len * sizeof(int));

    while (cur < r_len)
    {
        sz = _pg_mblen(p);
        if (sizes)
            (*sizes)[cur_size] = sz;
        if (positions)
            (*positions)[cur_size] = cur;
        cur += sz;
        p += sz;
        cur_size += 1;
    }

    return cur_size;
}

/* simply search algorhitm - can be better */

static int
orcl_instr_mb(text *txt, text *pattern, int start, int nth)
{
    int            c_len_txt, c_len_pat;
    int            b_len_pat;
    int           *pos_txt;
    const char *str_txt, *str_pat;
    int            beg, end, i, dx;

    str_txt = VARDATA_ANY(txt);
    c_len_txt = orcl_mb_strlen(txt, NULL, &pos_txt);
    str_pat = VARDATA_ANY(pattern);
    b_len_pat = VARSIZE_ANY_EXHDR(pattern);
    c_len_pat = pg_mbstrlen_with_len(str_pat, b_len_pat);

    if (start > 0)
    {
        dx = 1;
        beg = start - 1;
        end = c_len_txt - c_len_pat + 1;
        if (beg >= end)
            return 0;    /* out of range */
    }
    else
    {
        dx = -1;
        beg = Min(c_len_txt + start, c_len_txt - c_len_pat);
        end = -1;
        if (beg <= end)
            return 0;    /* out of range */
    }

    for (i = beg; i != end; i += dx)
    {
        if (memcmp(str_txt + pos_txt[i], str_pat, b_len_pat) == 0)
        {
            if (--nth == 0)
                return i + 1;
        }
    }

    return 0;
}

static int
orcl_instr(text *txt, text *pattern, int start, int nth)
{// #lizard forgives
    int            len_txt, len_pat;
    const char *str_txt, *str_pat;
    int            beg, end, i, dx;

    if (nth <= 0)
        PARAMETER_ERROR("Four parameter isn't positive.");

    /* Forward for multibyte strings */
    if (pg_database_encoding_max_length() > 1)
        return orcl_instr_mb(txt, pattern, start, nth);

    str_txt = VARDATA_ANY(txt);
    len_txt = VARSIZE_ANY_EXHDR(txt);
    str_pat = VARDATA_ANY(pattern);
    len_pat = VARSIZE_ANY_EXHDR(pattern);

    if (start > 0)
    {
        dx = 1;
        beg = start - 1;
        end = len_txt - len_pat + 1;
        if (beg >= end)
            return 0;    /* out of range */
    }
    else
    {
        dx = -1;
        beg = Min(len_txt + start, len_txt - len_pat);
        end = -1;
        if (beg <= end)
            return 0;    /* out of range */
    }

    for (i = beg; i != end; i += dx)
    {
        if (memcmp(str_txt + i, str_pat, len_pat) == 0)
        {
            if (--nth == 0)
                return i + 1;
        }
    }

    return 0;
}

/****************************************************************
 * oracle.instr
 *
 * Syntax:
 *   FUNCTION oracle.instr (string_in VARCHAR, pattern VARCHAR)
 *   FUNCTION oracle.instr (string_in VARCHAR, pattern VARCHAR,
 *            start_in INTEGER)
 *   FUNCTION oracle.instr (string_in VARCHAR, pattern VARCHAR,
 *            start_in INTEGER, nth INTEGER)
 *            RETURN INT;
 *
 * Purpouse:
 *   Search pattern in string.
 *
 ****************************************************************/
Datum
orcl_instr2(PG_FUNCTION_ARGS)
{
    text *arg1 = PG_GETARG_TEXT_PP(0);
    text *arg2 = PG_GETARG_TEXT_PP(1);

    PG_RETURN_NULL_IF_EMPTY_TEXT(arg1);
    PG_RETURN_NULL_IF_EMPTY_TEXT(arg2);

    PG_RETURN_INT32(orcl_instr(arg1, arg2, 1, 1));
}

Datum
orcl_instr3(PG_FUNCTION_ARGS)
{
    text *arg1 = PG_GETARG_TEXT_PP(0);
    text *arg2 = PG_GETARG_TEXT_PP(1);
    int arg3 = PG_GETARG_INT32(2);

    PG_RETURN_NULL_IF_EMPTY_TEXT(arg1);
    PG_RETURN_NULL_IF_EMPTY_TEXT(arg2);

    PG_RETURN_INT32(orcl_instr(arg1, arg2, arg3, 1));
}

Datum
orcl_instr4(PG_FUNCTION_ARGS)
{
    text *arg1 = PG_GETARG_TEXT_PP(0);
    text *arg2 = PG_GETARG_TEXT_PP(1);
    int arg3 = PG_GETARG_INT32(2);
    int arg4 = PG_GETARG_INT32(3);

    PG_RETURN_NULL_IF_EMPTY_TEXT(arg1);
    PG_RETURN_NULL_IF_EMPTY_TEXT(arg2);

    PG_RETURN_INT32(orcl_instr(arg1, arg2, arg3, arg4));
}


