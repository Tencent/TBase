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
 * varchar.c
 *      Functions for the built-in types char(n) and varchar(n).
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *      src/backend/utils/adt/varchar.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"


#include "access/hash.h"
#include "access/tuptoaster.h"
#include "catalog/pg_collation.h"
#include "libpq/pqformat.h"
#include "nodes/nodeFuncs.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/varlena.h"
#include "mb/pg_wchar.h"


/* common code for bpchartypmodin and varchartypmodin */
static int32
anychar_typmodin(ArrayType *ta, const char *typename)
{
    int32        typmod;
    int32       *tl;
    int            n;

    tl = ArrayGetIntegerTypmods(ta, &n);

    /*
     * we're not too tense about good error message here because grammar
     * shouldn't allow wrong number of modifiers for CHAR
     */
    if (n != 1)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("invalid type modifier")));

    if (*tl < 1)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("length for type %s must be at least 1", typename)));
    if (*tl > MaxAttrSize)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("length for type %s cannot exceed %d",
                        typename, MaxAttrSize)));

    /*
     * For largely historical reasons, the typmod is VARHDRSZ plus the number
     * of characters; there is enough client-side code that knows about that
     * that we'd better not change it.
     */
    typmod = VARHDRSZ + *tl;

    return typmod;
}

/* common code for bpchartypmodout and varchartypmodout */
static char *
anychar_typmodout(int32 typmod)
{
    char       *res = (char *) palloc(64);

    if (typmod > VARHDRSZ)
        snprintf(res, 64, "(%d)", (int) (typmod - VARHDRSZ));
    else
        *res = '\0';

    return res;
}


/*
 * CHAR() and VARCHAR() types are part of the SQL standard. CHAR()
 * is for blank-padded string whose length is specified in CREATE TABLE.
 * VARCHAR is for storing string whose length is at most the length specified
 * at CREATE TABLE time.
 *
 * It's hard to implement these types because we cannot figure out
 * the length of the type from the type itself. I changed (hopefully all) the
 * fmgr calls that invoke input functions of a data type to supply the
 * length also. (eg. in INSERTs, we have the tupleDescriptor which contains
 * the length of the attributes and hence the exact length of the char() or
 * varchar(). We pass this to bpcharin() or varcharin().) In the case where
 * we cannot determine the length, we pass in -1 instead and the input
 * converter does not enforce any length check.
 *
 * We actually implement this as a varlena so that we don't have to pass in
 * the length for the comparison functions. (The difference between these
 * types and "text" is that we truncate and possibly blank-pad the string
 * at insertion time.)
 *
 *                                                              - ay 6/95
 */


/*****************************************************************************
 *     bpchar - char()                                                         *
 *****************************************************************************/

/*
 * bpchar_input -- common guts of bpcharin and bpcharrecv
 *
 * s is the input text of length len (may not be null-terminated)
 * atttypmod is the typmod value to apply
 *
 * Note that atttypmod is measured in characters, which
 * is not necessarily the same as the number of bytes.
 *
 * If the input string is too long, raise an error, unless the extra
 * characters are spaces, in which case they're truncated.  (per SQL)
 */
static BpChar *
bpchar_input(const char *s, size_t len, int32 atttypmod)
{
    BpChar       *result;
    char       *r;
    size_t        maxlen;

    /* If typmod is -1 (or invalid), use the actual string length */
    if (atttypmod < (int32) VARHDRSZ)
        maxlen = len;
    else
    {
        size_t        charlen;    /* number of CHARACTERS in the input */

        maxlen = atttypmod - VARHDRSZ;
        charlen = pg_mbstrlen_with_len(s, len);
        if (charlen > maxlen)
        {
            /* Verify that extra characters are spaces, and clip them off */
            size_t        mbmaxlen = pg_mbcharcliplen(s, len, maxlen);
            size_t        j;

            /*
             * at this point, len is the actual BYTE length of the input
             * string, maxlen is the max number of CHARACTERS allowed for this
             * bpchar type, mbmaxlen is the length in BYTES of those chars.
             */
            for (j = mbmaxlen; j < len; j++)
            {
                if (s[j] != ' ')
                    ereport(ERROR,
                            (errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
                             errmsg("value too long for type character(%d)",
                                    (int) maxlen)));
            }

            /*
             * Now we set maxlen to the necessary byte length, not the number
             * of CHARACTERS!
             */
            maxlen = len = mbmaxlen;
        }
        else
        {
            /*
             * Now we set maxlen to the necessary byte length, not the number
             * of CHARACTERS!
             */
            maxlen = len + (maxlen - charlen);
        }
    }

    result = (BpChar *) palloc(maxlen + VARHDRSZ);
    SET_VARSIZE(result, maxlen + VARHDRSZ);
    r = VARDATA(result);
    memcpy(r, s, len);

    /* blank pad the string if necessary */
    if (maxlen > len)
        memset(r + len, ' ', maxlen - len);

    return result;
}

/*
 * Convert a C string to CHARACTER internal representation.  atttypmod
 * is the declared length of the type plus VARHDRSZ.
 */
Datum
bpcharin(PG_FUNCTION_ARGS)
{
    char       *s = PG_GETARG_CSTRING(0);

#ifdef NOT_USED
    Oid            typelem = PG_GETARG_OID(1);
#endif
    int32        atttypmod = PG_GETARG_INT32(2);
    BpChar       *result;

    result = bpchar_input(s, strlen(s), atttypmod);
    PG_RETURN_BPCHAR_P(result);
}


/*
 * Convert a CHARACTER value to a C string.
 *
 * Uses the text conversion functions, which is only appropriate if BpChar
 * and text are equivalent types.
 */
Datum
bpcharout(PG_FUNCTION_ARGS)
{
    Datum        txt = PG_GETARG_DATUM(0);

    PG_RETURN_CSTRING(TextDatumGetCString(txt));
}

/*
 *        bpcharrecv            - converts external binary format to bpchar
 */
Datum
bpcharrecv(PG_FUNCTION_ARGS)
{
    StringInfo    buf = (StringInfo) PG_GETARG_POINTER(0);

#ifdef NOT_USED
    Oid            typelem = PG_GETARG_OID(1);
#endif
    int32        atttypmod = PG_GETARG_INT32(2);
    BpChar       *result;
    char       *str;
    int            nbytes;

    str = pq_getmsgtext(buf, buf->len - buf->cursor, &nbytes);
    result = bpchar_input(str, nbytes, atttypmod);
    pfree(str);
    PG_RETURN_BPCHAR_P(result);
}

/*
 *        bpcharsend            - converts bpchar to binary format
 */
Datum
bpcharsend(PG_FUNCTION_ARGS)
{
    /* Exactly the same as textsend, so share code */
    return textsend(fcinfo);
}


/*
 * Converts a CHARACTER type to the specified size.
 *
 * maxlen is the typmod, ie, declared length plus VARHDRSZ bytes.
 * isExplicit is true if this is for an explicit cast to char(N).
 *
 * Truncation rules: for an explicit cast, silently truncate to the given
 * length; for an implicit cast, raise error unless extra characters are
 * all spaces.  (This is sort-of per SQL: the spec would actually have us
 * raise a "completion condition" for the explicit cast case, but Postgres
 * hasn't got such a concept.)
 */
Datum
bpchar(PG_FUNCTION_ARGS)
{// #lizard forgives
    BpChar       *source = PG_GETARG_BPCHAR_PP(0);
    int32        maxlen = PG_GETARG_INT32(1);
    bool        isExplicit = PG_GETARG_BOOL(2);
    BpChar       *result;
    int32        len;
    char       *r;
    char       *s;
    int            i;
    int            charlen;        /* number of characters in the input string +
                                 * VARHDRSZ */

    /* No work if typmod is invalid */
    if (maxlen < (int32) VARHDRSZ)
        PG_RETURN_BPCHAR_P(source);

    maxlen -= VARHDRSZ;

    len = VARSIZE_ANY_EXHDR(source);
    s = VARDATA_ANY(source);

    charlen = pg_mbstrlen_with_len(s, len);

    /* No work if supplied data matches typmod already */
    if (charlen == maxlen)
        PG_RETURN_BPCHAR_P(source);

    if (charlen > maxlen)
    {
        /* Verify that extra characters are spaces, and clip them off */
        size_t        maxmblen;

        maxmblen = pg_mbcharcliplen(s, len, maxlen);

        if (!isExplicit)
        {
            for (i = maxmblen; i < len; i++)
                if (s[i] != ' ')
                    ereport(ERROR,
                            (errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
                             errmsg("value too long for type character(%d)",
                                    maxlen)));
        }

        len = maxmblen;

        /*
         * At this point, maxlen is the necessary byte length, not the number
         * of CHARACTERS!
         */
        maxlen = len;
    }
    else
    {
        /*
         * At this point, maxlen is the necessary byte length, not the number
         * of CHARACTERS!
         */
        maxlen = len + (maxlen - charlen);
    }

    Assert(maxlen >= len);

    result = palloc(maxlen + VARHDRSZ);
    SET_VARSIZE(result, maxlen + VARHDRSZ);
    r = VARDATA(result);

    memcpy(r, s, len);

    /* blank pad the string if necessary */
    if (maxlen > len)
        memset(r + len, ' ', maxlen - len);

    PG_RETURN_BPCHAR_P(result);
}


/* char_bpchar()
 * Convert char to bpchar(1).
 */
Datum
char_bpchar(PG_FUNCTION_ARGS)
{
    char        c = PG_GETARG_CHAR(0);
    BpChar       *result;

    result = (BpChar *) palloc(VARHDRSZ + 1);

    SET_VARSIZE(result, VARHDRSZ + 1);
    *(VARDATA(result)) = c;

    PG_RETURN_BPCHAR_P(result);
}


/* bpchar_name()
 * Converts a bpchar() type to a NameData type.
 */
Datum
bpchar_name(PG_FUNCTION_ARGS)
{
    BpChar       *s = PG_GETARG_BPCHAR_PP(0);
    char       *s_data;
    Name        result;
    int            len;

    len = VARSIZE_ANY_EXHDR(s);
    s_data = VARDATA_ANY(s);

    /* Truncate oversize input */
    if (len >= NAMEDATALEN)
        len = pg_mbcliplen(s_data, len, NAMEDATALEN - 1);

    /* Remove trailing blanks */
    while (len > 0)
    {
        if (s_data[len - 1] != ' ')
            break;
        len--;
    }

    /* We use palloc0 here to ensure result is zero-padded */
    result = (Name) palloc0(NAMEDATALEN);
    memcpy(NameStr(*result), s_data, len);

    PG_RETURN_NAME(result);
}

/* name_bpchar()
 * Converts a NameData type to a bpchar type.
 *
 * Uses the text conversion functions, which is only appropriate if BpChar
 * and text are equivalent types.
 */
Datum
name_bpchar(PG_FUNCTION_ARGS)
{
    Name        s = PG_GETARG_NAME(0);
    BpChar       *result;

    result = (BpChar *) cstring_to_text(NameStr(*s));
    PG_RETURN_BPCHAR_P(result);
}

Datum
bpchartypmodin(PG_FUNCTION_ARGS)
{
    ArrayType  *ta = PG_GETARG_ARRAYTYPE_P(0);

    PG_RETURN_INT32(anychar_typmodin(ta, "char"));
}

Datum
bpchartypmodout(PG_FUNCTION_ARGS)
{
    int32        typmod = PG_GETARG_INT32(0);

    PG_RETURN_CSTRING(anychar_typmodout(typmod));
}


/*****************************************************************************
 *     varchar - varchar(n)
 *
 * Note: varchar piggybacks on type text for most operations, and so has no
 * C-coded functions except for I/O and typmod checking.
 *****************************************************************************/

/*
 * varchar_input -- common guts of varcharin and varcharrecv
 *
 * s is the input text of length len (may not be null-terminated)
 * atttypmod is the typmod value to apply
 *
 * Note that atttypmod is measured in characters, which
 * is not necessarily the same as the number of bytes.
 *
 * If the input string is too long, raise an error, unless the extra
 * characters are spaces, in which case they're truncated.  (per SQL)
 *
 * Uses the C string to text conversion function, which is only appropriate
 * if VarChar and text are equivalent types.
 */
static VarChar *
varchar_input(const char *s, size_t len, int32 atttypmod)
{
    VarChar    *result;
    size_t        maxlen;

    maxlen = atttypmod - VARHDRSZ;

    if (atttypmod >= (int32) VARHDRSZ && len > maxlen)
    {
        /* Verify that extra characters are spaces, and clip them off */
        size_t        mbmaxlen = pg_mbcharcliplen(s, len, maxlen);
        size_t        j;

        for (j = mbmaxlen; j < len; j++)
        {
            if (s[j] != ' ')
                ereport(ERROR,
                        (errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
                         errmsg("value too long for type character varying(%d)",
                                (int) maxlen)));
        }

        len = mbmaxlen;
    }

    result = (VarChar *) cstring_to_text_with_len(s, len);
    return result;
}

/*
 * Convert a C string to VARCHAR internal representation.  atttypmod
 * is the declared length of the type plus VARHDRSZ.
 */
Datum
varcharin(PG_FUNCTION_ARGS)
{
    char       *s = PG_GETARG_CSTRING(0);

#ifdef NOT_USED
    Oid            typelem = PG_GETARG_OID(1);
#endif
    int32        atttypmod = PG_GETARG_INT32(2);
    VarChar    *result;

    result = varchar_input(s, strlen(s), atttypmod);
    PG_RETURN_VARCHAR_P(result);
}


/*
 * Convert a VARCHAR value to a C string.
 *
 * Uses the text to C string conversion function, which is only appropriate
 * if VarChar and text are equivalent types.
 */
Datum
varcharout(PG_FUNCTION_ARGS)
{
    Datum        txt = PG_GETARG_DATUM(0);

    PG_RETURN_CSTRING(TextDatumGetCString(txt));
}

/*
 *        varcharrecv            - converts external binary format to varchar
 */
Datum
varcharrecv(PG_FUNCTION_ARGS)
{
    StringInfo    buf = (StringInfo) PG_GETARG_POINTER(0);

#ifdef NOT_USED
    Oid            typelem = PG_GETARG_OID(1);
#endif
    int32        atttypmod = PG_GETARG_INT32(2);
    VarChar    *result;
    char       *str;
    int            nbytes;

    str = pq_getmsgtext(buf, buf->len - buf->cursor, &nbytes);
    result = varchar_input(str, nbytes, atttypmod);
    pfree(str);
    PG_RETURN_VARCHAR_P(result);
}

/*
 *        varcharsend            - converts varchar to binary format
 */
Datum
varcharsend(PG_FUNCTION_ARGS)
{
    /* Exactly the same as textsend, so share code */
    return textsend(fcinfo);
}


/*
 * varchar_transform()
 * Flatten calls to varchar's length coercion function that set the new maximum
 * length >= the previous maximum length.  We can ignore the isExplicit
 * argument, since that only affects truncation cases.
 */
Datum
varchar_transform(PG_FUNCTION_ARGS)
{
    FuncExpr   *expr = castNode(FuncExpr, PG_GETARG_POINTER(0));
    Node       *ret = NULL;
    Node       *typmod;

    Assert(list_length(expr->args) >= 2);

    typmod = (Node *) lsecond(expr->args);

    if (IsA(typmod, Const) &&!((Const *) typmod)->constisnull)
    {
        Node       *source = (Node *) linitial(expr->args);
        int32        old_typmod = exprTypmod(source);
        int32        new_typmod = DatumGetInt32(((Const *) typmod)->constvalue);
        int32        old_max = old_typmod - VARHDRSZ;
        int32        new_max = new_typmod - VARHDRSZ;

        if (new_typmod < 0 || (old_typmod >= 0 && old_max <= new_max))
            ret = relabel_to_typmod(source, new_typmod);
    }

    PG_RETURN_POINTER(ret);
}

/*
 * Converts a VARCHAR type to the specified size.
 *
 * maxlen is the typmod, ie, declared length plus VARHDRSZ bytes.
 * isExplicit is true if this is for an explicit cast to varchar(N).
 *
 * Truncation rules: for an explicit cast, silently truncate to the given
 * length; for an implicit cast, raise error unless extra characters are
 * all spaces.  (This is sort-of per SQL: the spec would actually have us
 * raise a "completion condition" for the explicit cast case, but Postgres
 * hasn't got such a concept.)
 */
Datum
varchar(PG_FUNCTION_ARGS)
{
    VarChar    *source = PG_GETARG_VARCHAR_PP(0);
    int32        typmod = PG_GETARG_INT32(1);
    bool        isExplicit = PG_GETARG_BOOL(2);
    int32        len,
                maxlen;
    size_t        maxmblen;
    int            i;
    char       *s_data;

    len = VARSIZE_ANY_EXHDR(source);
    s_data = VARDATA_ANY(source);
    maxlen = typmod - VARHDRSZ;

    /* No work if typmod is invalid or supplied data fits it already */
    if (maxlen < 0 || len <= maxlen)
        PG_RETURN_VARCHAR_P(source);

    /* only reach here if string is too long... */

    /* truncate multibyte string preserving multibyte boundary */
    maxmblen = pg_mbcharcliplen(s_data, len, maxlen);

    if (!isExplicit)
    {
        for (i = maxmblen; i < len; i++)
            if (s_data[i] != ' ')
                ereport(ERROR,
                        (errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
                         errmsg("value too long for type character varying(%d)",
                                maxlen)));
    }

    PG_RETURN_VARCHAR_P((VarChar *) cstring_to_text_with_len(s_data,
                                                             maxmblen));
}

Datum
varchartypmodin(PG_FUNCTION_ARGS)
{
    ArrayType  *ta = PG_GETARG_ARRAYTYPE_P(0);

    PG_RETURN_INT32(anychar_typmodin(ta, "varchar"));
}

Datum
varchartypmodout(PG_FUNCTION_ARGS)
{
    int32        typmod = PG_GETARG_INT32(0);

    PG_RETURN_CSTRING(anychar_typmodout(typmod));
}


/*****************************************************************************
 * Exported functions
 *****************************************************************************/

/* "True" length (not counting trailing blanks) of a BpChar */
static inline int
bcTruelen(BpChar *arg)
{
    return bpchartruelen(VARDATA_ANY(arg), VARSIZE_ANY_EXHDR(arg));
}

int
bpchartruelen(char *s, int len)
{
    int            i;

    /*
     * Note that we rely on the assumption that ' ' is a singleton unit on
     * every supported multibyte server encoding.
     */
    for (i = len - 1; i >= 0; i--)
    {
        if (s[i] != ' ')
            break;
    }
    return i + 1;
}

Datum
bpcharlen(PG_FUNCTION_ARGS)
{
    BpChar       *arg = PG_GETARG_BPCHAR_PP(0);
    int            len;

    /* get number of bytes, ignoring trailing spaces */
    len = bcTruelen(arg);

    /* in multibyte encoding, convert to number of characters */
    if (pg_database_encoding_max_length() != 1)
        len = pg_mbstrlen_with_len(VARDATA_ANY(arg), len);

    PG_RETURN_INT32(len);
}

Datum
bpcharoctetlen(PG_FUNCTION_ARGS)
{
    Datum        arg = PG_GETARG_DATUM(0);

    /* We need not detoast the input at all */
    PG_RETURN_INT32(toast_raw_datum_size(arg) - VARHDRSZ);
}


/*****************************************************************************
 *    Comparison Functions used for bpchar
 *
 * Note: btree indexes need these routines not to leak memory; therefore,
 * be careful to free working copies of toasted datums.  Most places don't
 * need to be so careful.
 *****************************************************************************/

Datum
bpchareq(PG_FUNCTION_ARGS)
{
    BpChar       *arg1 = PG_GETARG_BPCHAR_PP(0);
    BpChar       *arg2 = PG_GETARG_BPCHAR_PP(1);
    int            len1,
                len2;
    bool        result;

    len1 = bcTruelen(arg1);
    len2 = bcTruelen(arg2);

    /*
     * Since we only care about equality or not-equality, we can avoid all the
     * expense of strcoll() here, and just do bitwise comparison.
     */
    if (len1 != len2)
        result = false;
    else
        result = (memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), len1) == 0);

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL(result);
}

Datum
bpcharne(PG_FUNCTION_ARGS)
{
    BpChar       *arg1 = PG_GETARG_BPCHAR_PP(0);
    BpChar       *arg2 = PG_GETARG_BPCHAR_PP(1);
    int            len1,
                len2;
    bool        result;

    len1 = bcTruelen(arg1);
    len2 = bcTruelen(arg2);

    /*
     * Since we only care about equality or not-equality, we can avoid all the
     * expense of strcoll() here, and just do bitwise comparison.
     */
    if (len1 != len2)
        result = true;
    else
        result = (memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), len1) != 0);

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL(result);
}

Datum
bpcharlt(PG_FUNCTION_ARGS)
{
    BpChar       *arg1 = PG_GETARG_BPCHAR_PP(0);
    BpChar       *arg2 = PG_GETARG_BPCHAR_PP(1);
    int            len1,
                len2;
    int            cmp;

    len1 = bcTruelen(arg1);
    len2 = bcTruelen(arg2);

    cmp = varstr_cmp(VARDATA_ANY(arg1), len1, VARDATA_ANY(arg2), len2,
                     PG_GET_COLLATION());

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL(cmp < 0);
}

Datum
bpcharle(PG_FUNCTION_ARGS)
{
    BpChar       *arg1 = PG_GETARG_BPCHAR_PP(0);
    BpChar       *arg2 = PG_GETARG_BPCHAR_PP(1);
    int            len1,
                len2;
    int            cmp;

    len1 = bcTruelen(arg1);
    len2 = bcTruelen(arg2);

    cmp = varstr_cmp(VARDATA_ANY(arg1), len1, VARDATA_ANY(arg2), len2,
                     PG_GET_COLLATION());

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL(cmp <= 0);
}

Datum
bpchargt(PG_FUNCTION_ARGS)
{
    BpChar       *arg1 = PG_GETARG_BPCHAR_PP(0);
    BpChar       *arg2 = PG_GETARG_BPCHAR_PP(1);
    int            len1,
                len2;
    int            cmp;

    len1 = bcTruelen(arg1);
    len2 = bcTruelen(arg2);

    cmp = varstr_cmp(VARDATA_ANY(arg1), len1, VARDATA_ANY(arg2), len2,
                     PG_GET_COLLATION());

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL(cmp > 0);
}

Datum
bpcharge(PG_FUNCTION_ARGS)
{
    BpChar       *arg1 = PG_GETARG_BPCHAR_PP(0);
    BpChar       *arg2 = PG_GETARG_BPCHAR_PP(1);
    int            len1,
                len2;
    int            cmp;

    len1 = bcTruelen(arg1);
    len2 = bcTruelen(arg2);

    cmp = varstr_cmp(VARDATA_ANY(arg1), len1, VARDATA_ANY(arg2), len2,
                     PG_GET_COLLATION());

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL(cmp >= 0);
}

Datum
bpcharcmp(PG_FUNCTION_ARGS)
{
    BpChar       *arg1 = PG_GETARG_BPCHAR_PP(0);
    BpChar       *arg2 = PG_GETARG_BPCHAR_PP(1);
    int            len1,
                len2;
    int            cmp;

    len1 = bcTruelen(arg1);
    len2 = bcTruelen(arg2);

    cmp = varstr_cmp(VARDATA_ANY(arg1), len1, VARDATA_ANY(arg2), len2,
                     PG_GET_COLLATION());

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_INT32(cmp);
}

Datum
bpchar_sortsupport(PG_FUNCTION_ARGS)
{
    SortSupport ssup = (SortSupport) PG_GETARG_POINTER(0);
    Oid            collid = ssup->ssup_collation;
    MemoryContext oldcontext;

    oldcontext = MemoryContextSwitchTo(ssup->ssup_cxt);

    /* Use generic string SortSupport */
    varstr_sortsupport(ssup, collid, true);

    MemoryContextSwitchTo(oldcontext);

    PG_RETURN_VOID();
}

Datum
bpchar_larger(PG_FUNCTION_ARGS)
{
    BpChar       *arg1 = PG_GETARG_BPCHAR_PP(0);
    BpChar       *arg2 = PG_GETARG_BPCHAR_PP(1);
    int            len1,
                len2;
    int            cmp;

    len1 = bcTruelen(arg1);
    len2 = bcTruelen(arg2);

    cmp = varstr_cmp(VARDATA_ANY(arg1), len1, VARDATA_ANY(arg2), len2,
                     PG_GET_COLLATION());

    PG_RETURN_BPCHAR_P((cmp >= 0) ? arg1 : arg2);
}

Datum
bpchar_smaller(PG_FUNCTION_ARGS)
{
    BpChar       *arg1 = PG_GETARG_BPCHAR_PP(0);
    BpChar       *arg2 = PG_GETARG_BPCHAR_PP(1);
    int            len1,
                len2;
    int            cmp;

    len1 = bcTruelen(arg1);
    len2 = bcTruelen(arg2);

    cmp = varstr_cmp(VARDATA_ANY(arg1), len1, VARDATA_ANY(arg2), len2,
                     PG_GET_COLLATION());

    PG_RETURN_BPCHAR_P((cmp <= 0) ? arg1 : arg2);
}


/*
 * bpchar needs a specialized hash function because we want to ignore
 * trailing blanks in comparisons.
 *
 * Note: currently there is no need for locale-specific behavior here,
 * but if we ever change the semantics of bpchar comparison to trust
 * strcoll() completely, we'd need to do something different in non-C locales.
 */
Datum
hashbpchar(PG_FUNCTION_ARGS)
{
    BpChar       *key = PG_GETARG_BPCHAR_PP(0);
    char       *keydata;
    int            keylen;
    Datum        result;

    keydata = VARDATA_ANY(key);
    keylen = bcTruelen(key);

    result = hash_any((unsigned char *) keydata, keylen);

    /* Avoid leaking memory for toasted inputs */
    PG_FREE_IF_COPY(key, 0);

    return result;
}

Datum
hashbpcharextended(PG_FUNCTION_ARGS)
{
	BpChar	   *key = PG_GETARG_BPCHAR_PP(0);
	char	   *keydata;
	int			keylen;
	Datum		result;

	keydata = VARDATA_ANY(key);
	keylen = bcTruelen(key);

	result = hash_any_extended((unsigned char *) keydata, keylen,
							   PG_GETARG_INT64(1));

	PG_FREE_IF_COPY(key, 0);

	return result;
}

/*
 * The following operators support character-by-character comparison
 * of bpchar datums, to allow building indexes suitable for LIKE clauses.
 * Note that the regular bpchareq/bpcharne comparison operators, and
 * regular support functions 1 and 2 with "C" collation are assumed to be
 * compatible with these!
 */

static int
internal_bpchar_pattern_compare(BpChar *arg1, BpChar *arg2)
{
    int            result;
    int            len1,
                len2;

    len1 = bcTruelen(arg1);
    len2 = bcTruelen(arg2);

    result = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));
    if (result != 0)
        return result;
    else if (len1 < len2)
        return -1;
    else if (len1 > len2)
        return 1;
    else
        return 0;
}


Datum
bpchar_pattern_lt(PG_FUNCTION_ARGS)
{
    BpChar       *arg1 = PG_GETARG_BPCHAR_PP(0);
    BpChar       *arg2 = PG_GETARG_BPCHAR_PP(1);
    int            result;

    result = internal_bpchar_pattern_compare(arg1, arg2);

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL(result < 0);
}


Datum
bpchar_pattern_le(PG_FUNCTION_ARGS)
{
    BpChar       *arg1 = PG_GETARG_BPCHAR_PP(0);
    BpChar       *arg2 = PG_GETARG_BPCHAR_PP(1);
    int            result;

    result = internal_bpchar_pattern_compare(arg1, arg2);

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL(result <= 0);
}


Datum
bpchar_pattern_ge(PG_FUNCTION_ARGS)
{
    BpChar       *arg1 = PG_GETARG_BPCHAR_PP(0);
    BpChar       *arg2 = PG_GETARG_BPCHAR_PP(1);
    int            result;

    result = internal_bpchar_pattern_compare(arg1, arg2);

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL(result >= 0);
}


Datum
bpchar_pattern_gt(PG_FUNCTION_ARGS)
{
    BpChar       *arg1 = PG_GETARG_BPCHAR_PP(0);
    BpChar       *arg2 = PG_GETARG_BPCHAR_PP(1);
    int            result;

    result = internal_bpchar_pattern_compare(arg1, arg2);

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL(result > 0);
}


Datum
btbpchar_pattern_cmp(PG_FUNCTION_ARGS)
{
    BpChar       *arg1 = PG_GETARG_BPCHAR_PP(0);
    BpChar       *arg2 = PG_GETARG_BPCHAR_PP(1);
    int            result;

    result = internal_bpchar_pattern_compare(arg1, arg2);

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_INT32(result);
}


Datum
btbpchar_pattern_sortsupport(PG_FUNCTION_ARGS)
{
    SortSupport ssup = (SortSupport) PG_GETARG_POINTER(0);
    MemoryContext oldcontext;

    oldcontext = MemoryContextSwitchTo(ssup->ssup_cxt);

    /* Use generic string SortSupport, forcing "C" collation */
    varstr_sortsupport(ssup, C_COLLATION_OID, true);

    MemoryContextSwitchTo(oldcontext);

    PG_RETURN_VOID();
}

#ifdef _PG_ORCL_

/*
 * varchar2_input -- common guts of varchar2in and varchar2recv
 *
 * s is the input text of length len (may not be null-terminated)
 * atttypmod is the typmod value to apply
 *
 * If the input string is too long, raise an error
 *
 * Uses the C string to text conversion function, which is only appropriate
 * if VarChar and text are equivalent types.
 */

static VarChar *
varchar2_input(const char *s, size_t len, int32 atttypmod)
{
    VarChar        *result;        /* input data */
    size_t        maxlen;

    maxlen = atttypmod - VARHDRSZ;

    /*
     * Perform the typmod check; error out if value too long for VARCHAR2
     */
    if (atttypmod >= (int32) VARHDRSZ && len > maxlen)
        if (len > maxlen)
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("input value length is %zd; too long for type varchar2(%zd)", len , maxlen)));

    result = (VarChar *) cstring_to_text_with_len(s, len);
    return  result;
}

/*
 * Converts a C string to VARCHAR2 internal representation.  atttypmod
 * is the declared length of the type plus VARHDRSZ.
 */
Datum
varchar2in(PG_FUNCTION_ARGS)
{
    char    *s = PG_GETARG_CSTRING(0);
#ifdef NOT_USED
    Oid        typelem = PG_GETARG_OID(1);
#endif
    int32    atttypmod = PG_GETARG_INT32(2);
    VarChar    *result;

    result = varchar2_input(s, strlen(s), atttypmod);
    PG_RETURN_VARCHAR_P(result);
}


/*
 * converts a VARCHAR2 value to a C string.
 *
 * Uses the text to C string conversion function, which is only appropriate
 * if VarChar and text are equivalent types.
 */
Datum
varchar2out(PG_FUNCTION_ARGS)
{
    Datum   txt = PG_GETARG_DATUM(0);

    PG_RETURN_CSTRING(TextDatumGetCString(txt));
}

/*
 * varchar2typmodin -- type modifier input function
 *
 * just like varchartypmodin()
 */
Datum
varchar2typmodin(PG_FUNCTION_ARGS)
{
    ArrayType  *ta = PG_GETARG_ARRAYTYPE_P(0);

    PG_RETURN_INT32(anychar_typmodin(ta, "varchar2"));
}

/*
 * varchar2typmodout -- type modifier output function
 *
 * just like varchartypmodout()
 */
Datum
varchar2typmodout(PG_FUNCTION_ARGS)
{
    int32        typmod = PG_GETARG_INT32(0);

    PG_RETURN_CSTRING(anychar_typmodout(typmod));
}

/*
 * converts external binary format to varchar
 */
Datum
varchar2recv(PG_FUNCTION_ARGS)
{
    StringInfo    buf = (StringInfo) PG_GETARG_POINTER(0);

#ifdef NOT_USED
    Oid            typelem = PG_GETARG_OID(1);
#endif
    int32        atttypmod = PG_GETARG_INT32(2);    /* typmod of the receiving column */
    VarChar        *result;
    char        *str;                            /* received data */
    int            nbytes;                            /* length in bytes of recived data */

    str = pq_getmsgtext(buf, buf->len - buf->cursor, &nbytes);
    result = varchar2_input(str, nbytes, atttypmod);
    pfree(str);
    PG_RETURN_VARCHAR_P(result);
}

/*
 * varchar2send -- convert varchar2 to binary value
 *
 * just use varcharsend()
 */
Datum
varchar2send(PG_FUNCTION_ARGS)
{
    return varcharsend(fcinfo);
}

/*
 * varchar2_transform()
 * Flatten calls to varchar's length coercion function that set the new maximum
 * length >= the previous maximum length.  We can ignore the isExplicit
 * argument, since that only affects truncation cases.
 *
 * just use varchar_transform()
 */

/*
 * Converts a VARCHAR2 type to the specified size.
 *
 * maxlen is the typmod, ie, declared length plus VARHDRSZ bytes.
 * isExplicit is true if this is for an explicit cast to varchar2(N).
 *
 * Truncation rules: for an explicit cast, silently truncate to the given
 * length; for an implicit cast, raise error if length limit is exceeded
 */
Datum
varchar2(PG_FUNCTION_ARGS)
{
    VarChar        *source = PG_GETARG_VARCHAR_PP(0);
    int32        typmod = PG_GETARG_INT32(1);
    bool        isExplicit = PG_GETARG_BOOL(2);
    int32        len,
                maxlen;
    char        *s_data;

    len = VARSIZE_ANY_EXHDR(source);
    s_data = VARDATA_ANY(source);
    maxlen = typmod - VARHDRSZ;

    /* No work if typmod is invalid or supplied data fits it already */
    if (maxlen < 0 || len <= maxlen)
        PG_RETURN_VARCHAR_P(source);

    /* error out if value too long unless it's an explicit cast */
    if (!isExplicit)
    {
        if (len > maxlen)
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("input value length is %d; too long for type varchar2(%d)",len ,maxlen)));
    }

    PG_RETURN_VARCHAR_P((VarChar *) cstring_to_text_with_len(s_data,maxlen));
}

/*
 * nvarchar2_input -- common guts of nvarchar2in and nvarchar2recv
 *
 * s is the input text of length len (may not be null-terminated)
 * atttypmod is the typmod value to apply
 *
 * If the input string is too long, raise an error
 *
 * Uses the C string to text conversion function, which is only appropriate
 * if VarChar and text are equivalent types.
 */

static VarChar *
nvarchar2_input(const char *s, size_t len, int32 atttypmod)
{
    VarChar        *result;        /* input data */
    size_t        maxlen;

    maxlen = atttypmod - VARHDRSZ;

    /*
     * Perform the typmod check; error out if value too long for NVARCHAR2
     */
    if (atttypmod >= (int32) VARHDRSZ && len > maxlen)
    {
        /* Verify that input length is within typmod limit.
         *
         * NOTE: blankspace is not truncated
         */
        size_t        mbmaxlen = pg_mbstrlen(s);

        if (mbmaxlen > maxlen)
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("input value length is %zd; too long for type nvarchar2(%zd)", mbmaxlen , maxlen)));
    }

    result = (VarChar *) cstring_to_text_with_len(s, len);
    return  result;
}

/*
 * Converts a C string to NVARCHAR2 internal representation.  atttypmod
 * is the declared length of the type plus VARHDRSZ.
 */
Datum
nvarchar2in(PG_FUNCTION_ARGS)
{
    char    *s = PG_GETARG_CSTRING(0);
#ifdef NOT_USED
    Oid        typelem = PG_GETARG_OID(1);
#endif
    int32    atttypmod = PG_GETARG_INT32(2);
    VarChar    *result;

    result = nvarchar2_input(s, strlen(s), atttypmod);
    PG_RETURN_VARCHAR_P(result);
}


/*
 * converts a NVARCHAR2 value to a C string.
 *
 * Uses the text to C string conversion function, which is only appropriate
 * if VarChar and text are equivalent types.
 */
Datum
nvarchar2out(PG_FUNCTION_ARGS)
{
    Datum   txt = PG_GETARG_DATUM(0);

    PG_RETURN_CSTRING(TextDatumGetCString(txt));
}

/*
 * nvarchar2typmodin -- type modifier input function
 *
 * just like varchartypmodin()
 */
Datum
nvarchar2typmodin(PG_FUNCTION_ARGS)
{
    ArrayType  *ta = PG_GETARG_ARRAYTYPE_P(0);

    PG_RETURN_INT32(anychar_typmodin(ta, "nvarchar2"));
}

/*
 * nvarchar2typmodout -- type modifier output function
 *
 * just like varchartypmodout()
 */
Datum
nvarchar2typmodout(PG_FUNCTION_ARGS)
{
    int32        typmod = PG_GETARG_INT32(0);

    PG_RETURN_CSTRING(anychar_typmodout(typmod));
}

/*
 * converts external binary format to nvarchar
 */
Datum
nvarchar2recv(PG_FUNCTION_ARGS)
{
    StringInfo    buf = (StringInfo) PG_GETARG_POINTER(0);

#ifdef NOT_USED
    Oid            typelem = PG_GETARG_OID(1);
#endif
    int32        atttypmod = PG_GETARG_INT32(2);    /* typmod of the receiving column */
    VarChar        *result;
    char        *str;                            /* received data */
    int            nbytes;                            /* length in bytes of recived data */

    str = pq_getmsgtext(buf, buf->len - buf->cursor, &nbytes);
    result = nvarchar2_input(str, nbytes, atttypmod);
    pfree(str);
    PG_RETURN_VARCHAR_P(result);
}

/*
 * nvarchar2send -- convert nvarchar2 to binary value
 *
 * just use varcharsend()
 */
Datum
nvarchar2send(PG_FUNCTION_ARGS)
{
    return varcharsend(fcinfo);
}

/*
 * nvarchar2_transform()
 * Flatten calls to varchar's length coercion function that set the new maximum
 * length >= the previous maximum length.  We can ignore the isExplicit
 * argument, since that only affects truncation cases.
 *
 * just use varchar_transform()
 */

/*
 * Converts a NVARCHAR2 type to the specified size.
 *
 * maxlen is the typmod, ie, declared length plus VARHDRSZ bytes.
 * isExplicit is true if this is for an explicit cast to nvarchar2(N).
 *
 * Truncation rules: for an explicit cast, silently truncate to the given
 * length; for an implicit cast, raise error if length limit is exceeded
 */
Datum
nvarchar2(PG_FUNCTION_ARGS)
{
    VarChar        *source = PG_GETARG_VARCHAR_PP(0);
    int32        typmod = PG_GETARG_INT32(1);
    bool        isExplicit = PG_GETARG_BOOL(2);
    int32        len,
                maxlen;
    size_t        maxmblen;
    char        *s_data;

    len = VARSIZE_ANY_EXHDR(source);
    s_data = VARDATA_ANY(source);
    maxlen = typmod - VARHDRSZ;

    /* No work if typmod is invalid or supplied data fits it already */
    if (maxlen < 0 || len <= maxlen)
        PG_RETURN_VARCHAR_P(source);

    /* only reach here if string is too long... */

    /* truncate multibyte string preserving multibyte boundary */
    maxmblen = pg_mbcharcliplen(s_data, len, maxlen);

    /* error out if value too long unless it's an explicit cast */
    if (!isExplicit)
    {
        /* if there is still data beyond maxmblen, error out
         *
         * Remember - no blankspace truncation on implicit cast
         */
        if (len > maxmblen)
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("input value too long for type nvarchar2(%d)", maxlen)));
    }

    PG_RETURN_VARCHAR_P((VarChar *) cstring_to_text_with_len(s_data, maxmblen));
}

#endif

