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
 * oid.c
 *      Functions for the built-in type Oid ... also oidvector.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *      src/backend/utils/adt/oid.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>
#include <limits.h>

#include "catalog/pg_type.h"
#include "libpq/pqformat.h"
#include "nodes/value.h"
#include "utils/array.h"
#include "utils/builtins.h"


#define OidVectorSize(n)    (offsetof(oidvector, values) + (n) * sizeof(Oid))


/*****************************************************************************
 *     USER I/O ROUTINES                                                         *
 *****************************************************************************/

static Oid
oidin_subr(const char *s, char **endloc)
{// #lizard forgives
    unsigned long cvt;
    char       *endptr;
    Oid            result;

    if (*s == '\0')
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("invalid input syntax for type %s: \"%s\"",
                        "oid", s)));

    errno = 0;
    cvt = strtoul(s, &endptr, 10);

    /*
     * strtoul() normally only sets ERANGE.  On some systems it also may set
     * EINVAL, which simply means it couldn't parse the input string. This is
     * handled by the second "if" consistent across platforms.
     */
    if (errno && errno != ERANGE && errno != EINVAL)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("invalid input syntax for type %s: \"%s\"",
                        "oid", s)));

    if (endptr == s && *s != '\0')
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("invalid input syntax for type %s: \"%s\"",
                        "oid", s)));

    if (errno == ERANGE)
        ereport(ERROR,
                (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                 errmsg("value \"%s\" is out of range for type %s",
                        s, "oid")));

    if (endloc)
    {
        /* caller wants to deal with rest of string */
        *endloc = endptr;
    }
    else
    {
        /* allow only whitespace after number */
        while (*endptr && isspace((unsigned char) *endptr))
            endptr++;
        if (*endptr)
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                     errmsg("invalid input syntax for type %s: \"%s\"",
                            "oid", s)));
    }

    result = (Oid) cvt;

    /*
     * Cope with possibility that unsigned long is wider than Oid, in which
     * case strtoul will not raise an error for some values that are out of
     * the range of Oid.
     *
     * For backwards compatibility, we want to accept inputs that are given
     * with a minus sign, so allow the input value if it matches after either
     * signed or unsigned extension to long.
     *
     * To ensure consistent results on 32-bit and 64-bit platforms, make sure
     * the error message is the same as if strtoul() had returned ERANGE.
     */
#if OID_MAX != ULONG_MAX
    if (cvt != (unsigned long) result &&
        cvt != (unsigned long) ((int) result))
        ereport(ERROR,
                (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                 errmsg("value \"%s\" is out of range for type %s",
                        s, "oid")));
#endif

    return result;
}

Datum
oidin(PG_FUNCTION_ARGS)
{
    char       *s = PG_GETARG_CSTRING(0);
    Oid            result;

    result = oidin_subr(s, NULL);
    PG_RETURN_OID(result);
}

Datum
oidout(PG_FUNCTION_ARGS)
{
    Oid            o = PG_GETARG_OID(0);
    char       *result = (char *) palloc(12);

    snprintf(result, 12, "%u", o);
    PG_RETURN_CSTRING(result);
}

/*
 *        oidrecv            - converts external binary format to oid
 */
Datum
oidrecv(PG_FUNCTION_ARGS)
{
    StringInfo    buf = (StringInfo) PG_GETARG_POINTER(0);

    PG_RETURN_OID((Oid) pq_getmsgint(buf, sizeof(Oid)));
}

/*
 *        oidsend            - converts oid to binary format
 */
Datum
oidsend(PG_FUNCTION_ARGS)
{
    Oid            arg1 = PG_GETARG_OID(0);
    StringInfoData buf;

    pq_begintypsend(&buf);
    pq_sendint(&buf, arg1, sizeof(Oid));
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 * construct oidvector given a raw array of Oids
 *
 * If oids is NULL then caller must fill values[] afterward
 */
oidvector *
buildoidvector(const Oid *oids, int n)
{
    oidvector  *result;

    result = (oidvector *) palloc0(OidVectorSize(n));

    if (n > 0 && oids)
        memcpy(result->values, oids, n * sizeof(Oid));

    /*
     * Attach standard array header.  For historical reasons, we set the index
     * lower bound to 0 not 1.
     */
    SET_VARSIZE(result, OidVectorSize(n));
    result->ndim = 1;
    result->dataoffset = 0;        /* never any nulls */
    result->elemtype = OIDOID;
    result->dim1 = n;
    result->lbound1 = 0;

    return result;
}

/*
 *        oidvectorin            - converts "num num ..." to internal form
 */
Datum
oidvectorin(PG_FUNCTION_ARGS)
{// #lizard forgives
    char       *oidString = PG_GETARG_CSTRING(0);
    oidvector  *result;
    int            n;

    result = (oidvector *) palloc0(OidVectorSize(FUNC_MAX_ARGS));

    for (n = 0; n < FUNC_MAX_ARGS; n++)
    {
        while (*oidString && isspace((unsigned char) *oidString))
            oidString++;
        if (*oidString == '\0')
            break;
        result->values[n] = oidin_subr(oidString, &oidString);
    }
    while (*oidString && isspace((unsigned char) *oidString))
        oidString++;
    if (*oidString)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("oidvector has too many elements")));

    SET_VARSIZE(result, OidVectorSize(n));
    result->ndim = 1;
    result->dataoffset = 0;        /* never any nulls */
    result->elemtype = OIDOID;
    result->dim1 = n;
    result->lbound1 = 0;

    PG_RETURN_POINTER(result);
}

/*
 *        oidvectorout - converts internal form to "num num ..."
 */
Datum
oidvectorout(PG_FUNCTION_ARGS)
{
    oidvector  *oidArray = (oidvector *) PG_GETARG_POINTER(0);
    int            num,
                nnums = oidArray->dim1;
    char       *rp;
    char       *result;

    /* assumes sign, 10 digits, ' ' */
    rp = result = (char *) palloc(nnums * 12 + 1);
    for (num = 0; num < nnums; num++)
    {
        if (num != 0)
            *rp++ = ' ';
        sprintf(rp, "%u", oidArray->values[num]);
        while (*++rp != '\0')
            ;
    }
    *rp = '\0';
    PG_RETURN_CSTRING(result);
}

/*
 *        oidvectorrecv            - converts external binary format to oidvector
 */
Datum
oidvectorrecv(PG_FUNCTION_ARGS)
{
    StringInfo    buf = (StringInfo) PG_GETARG_POINTER(0);
    FunctionCallInfoData locfcinfo;
    oidvector  *result;

    /*
     * Normally one would call array_recv() using DirectFunctionCall3, but
     * that does not work since array_recv wants to cache some data using
     * fcinfo->flinfo->fn_extra.  So we need to pass it our own flinfo
     * parameter.
     */
    InitFunctionCallInfoData(locfcinfo, fcinfo->flinfo, 3,
                             InvalidOid, NULL, NULL);

    locfcinfo.arg[0] = PointerGetDatum(buf);
    locfcinfo.arg[1] = ObjectIdGetDatum(OIDOID);
    locfcinfo.arg[2] = Int32GetDatum(-1);
    locfcinfo.argnull[0] = false;
    locfcinfo.argnull[1] = false;
    locfcinfo.argnull[2] = false;

    result = (oidvector *) DatumGetPointer(array_recv(&locfcinfo));

    Assert(!locfcinfo.isnull);

    /* sanity checks: oidvector must be 1-D, 0-based, no nulls */
    if (ARR_NDIM(result) != 1 ||
        ARR_HASNULL(result) ||
        ARR_ELEMTYPE(result) != OIDOID ||
        ARR_LBOUND(result)[0] != 0)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
                 errmsg("invalid oidvector data")));

    /* check length for consistency with oidvectorin() */
    if (ARR_DIMS(result)[0] > FUNC_MAX_ARGS)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("oidvector has too many elements")));

    PG_RETURN_POINTER(result);
}

/*
 *        oidvectorsend            - converts oidvector to binary format
 */
Datum
oidvectorsend(PG_FUNCTION_ARGS)
{
    return array_send(fcinfo);
}

/*
 *        oidparse                - get OID from IConst/FConst node
 */
Oid
oidparse(Node *node)
{
    switch (nodeTag(node))
    {
        case T_Integer:
            return intVal(node);
        case T_Float:

            /*
             * Values too large for int4 will be represented as Float
             * constants by the lexer.  Accept these if they are valid OID
             * strings.
             */
            return oidin_subr(strVal(node), NULL);
        default:
            elog(ERROR, "unrecognized node type: %d", (int) nodeTag(node));
    }
    return InvalidOid;            /* keep compiler quiet */
}

/* qsort comparison function for Oids */
int
oid_cmp(const void *p1, const void *p2)
{
    Oid            v1 = *((const Oid *) p1);
    Oid            v2 = *((const Oid *) p2);

    if (v1 < v2)
        return -1;
    if (v1 > v2)
        return 1;
    return 0;
}


/*****************************************************************************
 *     PUBLIC ROUTINES                                                         *
 *****************************************************************************/

Datum
oideq(PG_FUNCTION_ARGS)
{
    Oid            arg1 = PG_GETARG_OID(0);
    Oid            arg2 = PG_GETARG_OID(1);

    PG_RETURN_BOOL(arg1 == arg2);
}

Datum
oidne(PG_FUNCTION_ARGS)
{
    Oid            arg1 = PG_GETARG_OID(0);
    Oid            arg2 = PG_GETARG_OID(1);

    PG_RETURN_BOOL(arg1 != arg2);
}

Datum
oidlt(PG_FUNCTION_ARGS)
{
    Oid            arg1 = PG_GETARG_OID(0);
    Oid            arg2 = PG_GETARG_OID(1);

    PG_RETURN_BOOL(arg1 < arg2);
}

Datum
oidle(PG_FUNCTION_ARGS)
{
    Oid            arg1 = PG_GETARG_OID(0);
    Oid            arg2 = PG_GETARG_OID(1);

    PG_RETURN_BOOL(arg1 <= arg2);
}

Datum
oidge(PG_FUNCTION_ARGS)
{
    Oid            arg1 = PG_GETARG_OID(0);
    Oid            arg2 = PG_GETARG_OID(1);

    PG_RETURN_BOOL(arg1 >= arg2);
}

Datum
oidgt(PG_FUNCTION_ARGS)
{
    Oid            arg1 = PG_GETARG_OID(0);
    Oid            arg2 = PG_GETARG_OID(1);

    PG_RETURN_BOOL(arg1 > arg2);
}

Datum
oidlarger(PG_FUNCTION_ARGS)
{
    Oid            arg1 = PG_GETARG_OID(0);
    Oid            arg2 = PG_GETARG_OID(1);

    PG_RETURN_OID((arg1 > arg2) ? arg1 : arg2);
}

Datum
oidsmaller(PG_FUNCTION_ARGS)
{
    Oid            arg1 = PG_GETARG_OID(0);
    Oid            arg2 = PG_GETARG_OID(1);

    PG_RETURN_OID((arg1 < arg2) ? arg1 : arg2);
}

Datum
oidvectoreq(PG_FUNCTION_ARGS)
{
    int32        cmp = DatumGetInt32(btoidvectorcmp(fcinfo));

    PG_RETURN_BOOL(cmp == 0);
}

Datum
oidvectorne(PG_FUNCTION_ARGS)
{
    int32        cmp = DatumGetInt32(btoidvectorcmp(fcinfo));

    PG_RETURN_BOOL(cmp != 0);
}

Datum
oidvectorlt(PG_FUNCTION_ARGS)
{
    int32        cmp = DatumGetInt32(btoidvectorcmp(fcinfo));

    PG_RETURN_BOOL(cmp < 0);
}

Datum
oidvectorle(PG_FUNCTION_ARGS)
{
    int32        cmp = DatumGetInt32(btoidvectorcmp(fcinfo));

    PG_RETURN_BOOL(cmp <= 0);
}

Datum
oidvectorge(PG_FUNCTION_ARGS)
{
    int32        cmp = DatumGetInt32(btoidvectorcmp(fcinfo));

    PG_RETURN_BOOL(cmp >= 0);
}

Datum
oidvectorgt(PG_FUNCTION_ARGS)
{
    int32        cmp = DatumGetInt32(btoidvectorcmp(fcinfo));

    PG_RETURN_BOOL(cmp > 0);
}

#ifdef _MIGRATE_
oidvector *
oidvector_append(oidvector *oldoids, Oid newOid)
{
    oidvector *result;
    int     oldlen = 0;
    
    if(!oldoids)
        oldlen = 0;
    else
        oldlen = oldoids->dim1;

    result = (oidvector *)palloc(OidVectorSize(oldlen + 1));

    result->dataoffset = 0;
    result->dim1 = oldlen + 1;
    result->elemtype = OIDOID;
    result->lbound1 = 0;
    result->ndim = 1;
    SET_VARSIZE(result, OidVectorSize(oldlen + 1));

	if (oldoids && oldoids->dim1 > 0)
        memcpy(result->values, oldoids->values, oldlen * sizeof(Oid));

    result->values[result->dim1-1] = newOid;

    return result;
}

oidvector *
oidvector_remove(oidvector *oldoids, Oid toremove)
{
    oidvector *result;
    int     oldlen = 0;
    int     newlen = 0;
    int        i;
    
    if(!oldoids || oldoids->dim1 == 1)
        return NULL;

    oldlen = oldoids->dim1;
    
    result = (oidvector *)palloc(OidVectorSize(oldlen - 1));

    result->dataoffset = 0;
    result->dim1 = oldlen - 1;
    result->elemtype = OIDOID;
    result->lbound1 = 0;
    result->ndim = 1;
    SET_VARSIZE(result, OidVectorSize(oldlen - 1));

    for(i = 0; i < oldlen; i++)
    {
        if(oldoids->values[i] != toremove)
            result->values[newlen++] = oldoids->values[i];
    }

    if (newlen != oldlen - 1)
    {
        elog(ERROR, "[oidvector_remove] remove oid failed.");
    }
    
    return result;
}

bool 
oidvector_member(oidvector *oids, Oid oid)
{
    int i;
    
    if(!oids)
        return false;

    for (i = 0; i < oids->dim1; i++)
    {
        if (oids->values[i] == oid)
            return true;
    }

    return false;
}
#endif
