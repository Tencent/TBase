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
#include "postgres.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "mb/pg_wchar.h"
#include "utils/builtins.h"
#include "utils/int8.h"
#include "utils/numeric.h"
#include "utils/pg_locale.h"
#include "utils/formatting.h"
#include "oracle/oracle.h"

Datum
orcl_int4_tochar(PG_FUNCTION_ARGS)
{
    text        *fmt = PG_GETARG_TEXT_PP_IF_NULL(1);

    if (!fmt)
    {
        Datum result = DirectFunctionCall1(int4out, PG_GETARG_DATUM(0));
        PG_RETURN_TEXT_P(cstring_to_text(DatumGetCString(result)));
    }

    PG_RETURN_NULL_IF_EMPTY_TEXT(fmt);

    return DirectFunctionCall2(int4_to_char,
                                PG_GETARG_DATUM(0),
                                PG_GETARG_DATUM(1));
}

Datum
orcl_int8_tochar(PG_FUNCTION_ARGS)
{
    text        *fmt = PG_GETARG_TEXT_PP_IF_NULL(1);

    if (!fmt)
    {
        Datum result = DirectFunctionCall1(int8out, PG_GETARG_DATUM(0));
        PG_RETURN_TEXT_P(cstring_to_text(DatumGetCString(result)));
    }

    PG_RETURN_NULL_IF_EMPTY_TEXT(fmt);

    return DirectFunctionCall2(int8_to_char,
                                PG_GETARG_DATUM(0),
                                PG_GETARG_DATUM(1));
}

Datum
orcl_float4_tochar(PG_FUNCTION_ARGS)
{
    text        *fmt = PG_GETARG_TEXT_PP_IF_NULL(1);

    if (!fmt)
    {
        Datum result = DirectFunctionCall1(float4out, PG_GETARG_DATUM(0));
        PG_RETURN_TEXT_P(cstring_to_text(DatumGetCString(result)));
    }

    PG_RETURN_NULL_IF_EMPTY_TEXT(fmt);

    return DirectFunctionCall2(float4_to_char,
                                PG_GETARG_DATUM(0),
                                PG_GETARG_DATUM(1));
}

Datum
orcl_float8_tochar(PG_FUNCTION_ARGS)
{
    text       *fmt = PG_GETARG_TEXT_PP_IF_NULL(1);

    if (!fmt)
    {
        Datum result = DirectFunctionCall1(float8out, PG_GETARG_DATUM(0));
        PG_RETURN_TEXT_P(cstring_to_text(DatumGetCString(result)));
    }

    PG_RETURN_NULL_IF_EMPTY_TEXT(fmt);

    return DirectFunctionCall2(float8_to_char,
                                PG_GETARG_DATUM(0),
                                PG_GETARG_DATUM(1));
}

Datum
orcl_numeric_tochar(PG_FUNCTION_ARGS)
{
    text       *fmt = PG_GETARG_TEXT_PP_IF_NULL(1);

    if (!fmt)
    {
        Datum result = DirectFunctionCall1(numeric_out, PG_GETARG_DATUM(0));
        PG_RETURN_TEXT_P(cstring_to_text(DatumGetCString(result)));
    }

    PG_RETURN_NULL_IF_EMPTY_TEXT(fmt);

    return DirectFunctionCall2(numeric_to_char,
                                PG_GETARG_DATUM(0),
                                PG_GETARG_DATUM(1));
}

Datum
orcl_text_tochar(PG_FUNCTION_ARGS)
{
    return PG_GETARG_DATUM(0);
}

Datum
orcl_timestamp_tochar(PG_FUNCTION_ARGS)
{
    text *fmt = PG_GETARG_TEXT_PP_IF_NULL(1);

    PG_RETURN_NULL_IF_EMPTY_TEXT(fmt);

    if (!fmt)
    {
        if (nls_timestamp_format && strlen(nls_timestamp_format))
            fmt = cstring_to_text(nls_timestamp_format);
        else
            PG_RETURN_NULL();
    }

    return DirectFunctionCall2(timestamp_to_char,
                                PG_GETARG_DATUM(0),
                                PointerGetDatum(fmt));
}

Datum
orcl_timestamptz_tochar(PG_FUNCTION_ARGS)
{
    text *fmt = PG_GETARG_TEXT_PP_IF_NULL(1);

    PG_RETURN_NULL_IF_EMPTY_TEXT(fmt);

    if (!fmt)
    {
        if (nls_timestamp_tz_format && strlen(nls_timestamp_tz_format))
            fmt = cstring_to_text(nls_timestamp_tz_format);
        else
            PG_RETURN_NULL();
    }

    return DirectFunctionCall2(timestamptz_to_char,
                                PG_GETARG_DATUM(0),
                                PointerGetDatum(fmt));
}

Datum
orcl_interval_tochar(PG_FUNCTION_ARGS)
{
    text *fmt = PG_GETARG_TEXT_PP_IF_NULL(1);

    PG_RETURN_NULL_IF_EMPTY_TEXT(fmt);

    if (!fmt)
    {
        if (nls_timestamp_format && strlen(nls_timestamp_format))
            fmt = cstring_to_text(nls_timestamp_format);
        else
            PG_RETURN_NULL();
    }

    return DirectFunctionCall2(interval_to_char,
                                PG_GETARG_DATUM(0),
                                PointerGetDatum(fmt));
}

/*
 * oracle function to_number(text, text)
 *         convert string to numeric
 */
Datum
orcl_text_tonumber(PG_FUNCTION_ARGS)
{
    text *txt = PG_GETARG_TEXT_PP_IF_NULL(0);
    text *fmt = PG_GETARG_TEXT_PP_IF_NULL(1);

    if (!txt)
        PG_RETURN_NULL();

    PG_RETURN_NULL_IF_EMPTY_TEXT(txt);
    PG_RETURN_NULL_IF_EMPTY_TEXT(fmt);

    if (!fmt)
    {
        char *txtstr = text_to_cstring(txt);
        Datum result = DirectFunctionCall3(numeric_in,
                                           CStringGetDatum(txtstr),
                                           0,
                                           -1);
        pfree(txtstr);

        return result;
    } 
    else
    {
        return DirectFunctionCall2(numeric_to_number,
                            PG_GETARG_DATUM(0),
                            PG_GETARG_DATUM(1));
    }
}

Datum
orcl_float4_tonumber(PG_FUNCTION_ARGS)
{
    text *fmt = PG_GETARG_TEXT_PP_IF_NULL(1);

    PG_RETURN_NULL_IF_EMPTY_TEXT(fmt);

    if (!fmt)
    {
        return DirectFunctionCall1(float4_numeric, PG_GETARG_DATUM(0));
    }
    else
    {
        Datum val = DirectFunctionCall1(float4out, PG_GETARG_DATUM(0));
        text *valtxt = cstring_to_text(DatumGetCString(val));

        return DirectFunctionCall2(numeric_to_number,
                                    PointerGetDatum(valtxt),
                                    PG_GETARG_DATUM(1));
    }
}

Datum
orcl_float8_tonumber(PG_FUNCTION_ARGS)
{
    text *fmt = PG_GETARG_TEXT_PP_IF_NULL(1);

    PG_RETURN_NULL_IF_EMPTY_TEXT(fmt);

    if (!fmt)
    {
        return DirectFunctionCall1(float8_numeric, PG_GETARG_DATUM(0));
    } 
    else
    {
        Datum val = DirectFunctionCall1(float8out, PG_GETARG_DATUM(0));
        text *valtxt = cstring_to_text(DatumGetCString(val));

        return DirectFunctionCall2(numeric_to_number,
                                    PointerGetDatum(valtxt),
                                    PG_GETARG_DATUM(1));
    }
}

/* 3 is enough, but it is defined as 4 in backend code. */
#ifndef MAX_CONVERSION_GROWTH
#define MAX_CONVERSION_GROWTH  4
#endif

/*
 * Convert a tilde (~) to ...
 *    1: a full width tilde. (same as JA16EUCTILDE in oracle)
 *    0: a full width overline. (same as JA16EUC in oracle)
 */
#define JA_TO_FULL_WIDTH_TILDE    1

static const char *
TO_MULTI_BYTE_UTF8[95] =
{
    "\343\200\200",
    "\357\274\201",
    "\342\200\235",
    "\357\274\203",
    "\357\274\204",
    "\357\274\205",
    "\357\274\206",
    "\342\200\231",
    "\357\274\210",
    "\357\274\211",
    "\357\274\212",
    "\357\274\213",
    "\357\274\214",
    "\357\274\215",
    "\357\274\216",
    "\357\274\217",
    "\357\274\220",
    "\357\274\221",
    "\357\274\222",
    "\357\274\223",
    "\357\274\224",
    "\357\274\225",
    "\357\274\226",
    "\357\274\227",
    "\357\274\230",
    "\357\274\231",
    "\357\274\232",
    "\357\274\233",
    "\357\274\234",
    "\357\274\235",
    "\357\274\236",
    "\357\274\237",
    "\357\274\240",
    "\357\274\241",
    "\357\274\242",
    "\357\274\243",
    "\357\274\244",
    "\357\274\245",
    "\357\274\246",
    "\357\274\247",
    "\357\274\250",
    "\357\274\251",
    "\357\274\252",
    "\357\274\253",
    "\357\274\254",
    "\357\274\255",
    "\357\274\256",
    "\357\274\257",
    "\357\274\260",
    "\357\274\261",
    "\357\274\262",
    "\357\274\263",
    "\357\274\264",
    "\357\274\265",
    "\357\274\266",
    "\357\274\267",
    "\357\274\270",
    "\357\274\271",
    "\357\274\272",
    "\357\274\273",
    "\357\277\245",
    "\357\274\275",
    "\357\274\276",
    "\357\274\277",
    "\342\200\230",
    "\357\275\201",
    "\357\275\202",
    "\357\275\203",
    "\357\275\204",
    "\357\275\205",
    "\357\275\206",
    "\357\275\207",
    "\357\275\210",
    "\357\275\211",
    "\357\275\212",
    "\357\275\213",
    "\357\275\214",
    "\357\275\215",
    "\357\275\216",
    "\357\275\217",
    "\357\275\220",
    "\357\275\221",
    "\357\275\222",
    "\357\275\223",
    "\357\275\224",
    "\357\275\225",
    "\357\275\226",
    "\357\275\227",
    "\357\275\230",
    "\357\275\231",
    "\357\275\232",
    "\357\275\233",
    "\357\275\234",
    "\357\275\235",
#if JA_TO_FULL_WIDTH_TILDE
    "\357\275\236"
#else
    "\357\277\243"
#endif
};

static const char *
TO_MULTI_BYTE_EUCJP[95] =
{
    "\241\241",
    "\241\252",
    "\241\311",
    "\241\364",
    "\241\360",
    "\241\363",
    "\241\365",
    "\241\307",
    "\241\312",
    "\241\313",
    "\241\366",
    "\241\334",
    "\241\244",
    "\241\335",
    "\241\245",
    "\241\277",
    "\243\260",
    "\243\261",
    "\243\262",
    "\243\263",
    "\243\264",
    "\243\265",
    "\243\266",
    "\243\267",
    "\243\270",
    "\243\271",
    "\241\247",
    "\241\250",
    "\241\343",
    "\241\341",
    "\241\344",
    "\241\251",
    "\241\367",
    "\243\301",
    "\243\302",
    "\243\303",
    "\243\304",
    "\243\305",
    "\243\306",
    "\243\307",
    "\243\310",
    "\243\311",
    "\243\312",
    "\243\313",
    "\243\314",
    "\243\315",
    "\243\316",
    "\243\317",
    "\243\320",
    "\243\321",
    "\243\322",
    "\243\323",
    "\243\324",
    "\243\325",
    "\243\326",
    "\243\327",
    "\243\330",
    "\243\331",
    "\243\332",
    "\241\316",
    "\241\357",
    "\241\317",
    "\241\260",
    "\241\262",
    "\241\306",
    "\243\341",
    "\243\342",
    "\243\343",
    "\243\344",
    "\243\345",
    "\243\346",
    "\243\347",
    "\243\350",
    "\243\351",
    "\243\352",
    "\243\353",
    "\243\354",
    "\243\355",
    "\243\356",
    "\243\357",
    "\243\360",
    "\243\361",
    "\243\362",
    "\243\363",
    "\243\364",
    "\243\365",
    "\243\366",
    "\243\367",
    "\243\370",
    "\243\371",
    "\243\372",
    "\241\320",
    "\241\303",
    "\241\321",
#if JA_TO_FULL_WIDTH_TILDE
    "\241\301"
#else
    "\241\261"
#endif
};

Datum
orcl_to_multi_byte(PG_FUNCTION_ARGS)
{// #lizard forgives
    text       *src;
    text       *dst;
    const char *s;
    char       *d;
    int            srclen;
    int            dstlen;
    int            i;
    const char **map;

    switch (GetDatabaseEncoding())
    {
        case PG_UTF8:
            map = TO_MULTI_BYTE_UTF8;
            break;
        case PG_EUC_JP:
        case PG_EUC_JIS_2004:
            map = TO_MULTI_BYTE_EUCJP;
            break;
        /*
         * TODO: Add converter for encodings.
         */
        default:    /* no need to convert */
            PG_RETURN_DATUM(PG_GETARG_DATUM(0));
    }

    src = PG_GETARG_TEXT_PP(0);
    s = VARDATA_ANY(src);
    srclen = VARSIZE_ANY_EXHDR(src);
    dst = (text *) palloc(VARHDRSZ + srclen * MAX_CONVERSION_GROWTH);
    d = VARDATA(dst);

    for (i = 0; i < srclen; i++)
    {
        unsigned char    u = (unsigned char) s[i];
        if (0x20 <= u && u <= 0x7e)
        {
            const char *m = map[u - 0x20];
            while (*m)
            {
                *d++ = *m++;
            }
        }
        else
        {
            *d++ = s[i];
        }
    }

    dstlen = d - VARDATA(dst);
    SET_VARSIZE(dst, VARHDRSZ + dstlen);

    PG_RETURN_TEXT_P(dst);
}

static int
getindex(const char **map, char *mbchar, int mblen)
{
    int        i;

    for (i = 0; i < 95; i++)
    {
        if (!memcmp(map[i], mbchar, mblen))
            return i;
    }

    return -1;
}

Datum
orcl_to_single_byte(PG_FUNCTION_ARGS)
{// #lizard forgives
    text       *src;
    text       *dst;
    char       *s;
    char       *d;
    int            srclen;
    int            dstlen;
    const char **map;

    switch (GetDatabaseEncoding())
    {
        case PG_UTF8:
            map = TO_MULTI_BYTE_UTF8;
            break;
        case PG_EUC_JP:
        case PG_EUC_JIS_2004:
            map = TO_MULTI_BYTE_EUCJP;
            break;
        /*
         * TODO: Add converter for encodings.
         */
        default:    /* no need to convert */
            PG_RETURN_DATUM(PG_GETARG_DATUM(0));
    }

    src = PG_GETARG_TEXT_PP(0);
    s = VARDATA_ANY(src);
    srclen = VARSIZE_ANY_EXHDR(src);

    /* XXX - The output length should be <= input length */
    dst = (text *) palloc0(VARHDRSZ + srclen);
    d = VARDATA(dst);

    while (*s && (s - VARDATA_ANY(src) < srclen))
    {
        char   *u = s;
        int        clen;
        int        mapindex;

        clen = pg_mblen(u);
        s += clen;

        if (clen == 1)
            *d++ = *u;
        else if ((mapindex = getindex(map, u, clen)) >= 0)
        {
            const char m = 0x20 + mapindex;
            *d++ = m;
        }
        else
        {
            memcpy(d, u, clen);
            d += clen;
        }
    }

    dstlen = d - VARDATA(dst);
    SET_VARSIZE(dst, VARHDRSZ + dstlen);

    PG_RETURN_TEXT_P(dst);
}

