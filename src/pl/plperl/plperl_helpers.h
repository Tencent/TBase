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
#ifndef PL_PERL_HELPERS_H
#define PL_PERL_HELPERS_H

#include "mb/pg_wchar.h"

/*
 * convert from utf8 to database encoding
 *
 * Returns a palloc'ed copy of the original string
 */
static inline char *
utf_u2e(char *utf8_str, size_t len)
{
    char       *ret;

    ret = pg_any_to_server(utf8_str, len, PG_UTF8);

    /* ensure we have a copy even if no conversion happened */
    if (ret == utf8_str)
        ret = pstrdup(ret);

    return ret;
}

/*
 * convert from database encoding to utf8
 *
 * Returns a palloc'ed copy of the original string
 */
static inline char *
utf_e2u(const char *str)
{
    char       *ret;

    ret = pg_server_to_any(str, strlen(str), PG_UTF8);

    /* ensure we have a copy even if no conversion happened */
    if (ret == str)
        ret = pstrdup(ret);

    return ret;
}


/*
 * Convert an SV to a char * in the current database encoding
 *
 * Returns a palloc'ed copy of the original string
 */
static inline char *
sv2cstr(SV *sv)
{
    dTHX;
    char       *val,
               *res;
    STRLEN        len;

    /*
     * get a utf8 encoded char * out of perl. *note* it may not be valid utf8!
     */

    /*
     * SvPVutf8() croaks nastily on certain things, like typeglobs and
     * readonly objects such as $^V. That's a perl bug - it's not supposed to
     * happen. To avoid crashing the backend, we make a copy of the sv before
     * passing it to SvPVutf8(). The copy is garbage collected when we're done
     * with it.
     */
    if (SvREADONLY(sv) ||
        isGV_with_GP(sv) ||
        (SvTYPE(sv) > SVt_PVLV && SvTYPE(sv) != SVt_PVFM))
        sv = newSVsv(sv);
    else
    {
        /*
         * increase the reference count so we can just SvREFCNT_dec() it when
         * we are done
         */
        SvREFCNT_inc_simple_void(sv);
    }

    /*
     * Request the string from Perl, in UTF-8 encoding; but if we're in a
     * SQL_ASCII database, just request the byte soup without trying to make
     * it UTF8, because that might fail.
     */
    if (GetDatabaseEncoding() == PG_SQL_ASCII)
        val = SvPV(sv, len);
    else
        val = SvPVutf8(sv, len);

    /*
     * Now convert to database encoding.  We use perl's length in the event we
     * had an embedded null byte to ensure we error out properly.
     */
    res = utf_u2e(val, len);

    /* safe now to garbage collect the new SV */
    SvREFCNT_dec(sv);

    return res;
}

/*
 * Create a new SV from a string assumed to be in the current database's
 * encoding.
 */
static inline SV *
cstr2sv(const char *str)
{
    dTHX;
    SV           *sv;
    char       *utf8_str;

    /* no conversion when SQL_ASCII */
    if (GetDatabaseEncoding() == PG_SQL_ASCII)
        return newSVpv(str, 0);

    utf8_str = utf_e2u(str);

    sv = newSVpv(utf8_str, 0);
    SvUTF8_on(sv);
    pfree(utf8_str);

    return sv;
}

/*
 * croak() with specified message, which is given in the database encoding.
 *
 * Ideally we'd just write croak("%s", str), but plain croak() does not play
 * nice with non-ASCII data.  In modern Perl versions we can call cstr2sv()
 * and pass the result to croak_sv(); in versions that don't have croak_sv(),
 * we have to work harder.
 */
static inline void
croak_cstr(const char *str)
{
    dTHX;

#ifdef croak_sv
    /* Use sv_2mortal() to be sure the transient SV gets freed */
    croak_sv(sv_2mortal(cstr2sv(str)));
#else

    /*
     * The older way to do this is to assign a UTF8-marked value to ERRSV and
     * then call croak(NULL).  But if we leave it to croak() to append the
     * error location, it does so too late (only after popping the stack) in
     * some Perl versions.  Hence, use mess() to create an SV with the error
     * location info already appended.
     */
    SV           *errsv = get_sv("@", GV_ADD);
    char       *utf8_str = utf_e2u(str);
    SV           *ssv;

    ssv = mess("%s", utf8_str);
    SvUTF8_on(ssv);

    pfree(utf8_str);

    sv_setsv(errsv, ssv);

    croak(NULL);
#endif                            /* croak_sv */
}

#endif                            /* PL_PERL_HELPERS_H */
