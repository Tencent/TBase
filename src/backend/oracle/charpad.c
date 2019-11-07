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
/*----------------------------------------------------------------------------
 *
 *  charpad.c
 *  LPAD and RPAD SQL functions for PostgreSQL.
 *
 *----------------------------------------------------------------------------
 */

#include "postgres.h"
#include "utils/builtins.h"
#include "utils/formatting.h"
#include "mb/pg_wchar.h"
#include "fmgr.h"
#include "oracle/oracle.h"

/* flags */
#define ON    true
#define OFF    false

/* Upper limit on total width of the padded output of *pad functions */
#define PAD_MAX 4000

/*
 * orcl_lpad(string text, length int32 [, fill text])
 *
 * Fill up the string to length 'length' by prepending
 * the characters fill (a half-width space by default)
 */
Datum
orcl_lpad(PG_FUNCTION_ARGS)
{// #lizard forgives
    text    *string1 = PG_GETARG_TEXT_PP(0);
    int32    output_width = PG_GETARG_INT32(1);
    text    *string2 = PG_GETARG_TEXT_PP(2);
    text    *ret;
    char    *ptr1,
            *ptr2 = NULL,
            *ptr2start = NULL,
            *ptr2end = NULL,
            *ptr_ret,
            *spc = " ";
    int        mlen,
            dsplen,
            s1blen,
            s2blen,
            hslen,
            total_blen = 0,
            s1_width = 0,
            s2_add_width = 0,
            s1_add_blen = 0,
            s2_add_blen = 0;
    bool    s2_operate = ON,
            half_space = OFF,
            init_ptr = ON;

    /* validate output width (the 2nd argument) */
    if (output_width < 0)
        output_width = 0;
    if (output_width > PAD_MAX)
        output_width = PAD_MAX;

    /* get byte-length of the 1st and 3rd argument strings */
    s1blen = VARSIZE_ANY_EXHDR(string1);
    s2blen = VARSIZE_ANY_EXHDR(string2);

    /* validate the lengths */
    if (s1blen < 0)
        s1blen = 0;
    if (s2blen < 0)
        s2blen = 0;

    /* if the filler length is zero disable filling */
    if (s2blen == 0)
    {
        s2_operate = OFF;    /* turn off string2 processing flag */
        output_width = 0;    /* same behavior as Oracle database */
    }

    /* byte-length of half-width space */
    hslen = pg_mblen(spc);

    /*
     * Calculate the length of the portion of string1 to include in
     * the final output
     */
    ptr1 = VARDATA_ANY(string1);
    while (s1blen > 0)
    {
        /* byte-length and display length per character of string1 */
        mlen = pg_mblen(ptr1);
        dsplen = pg_dsplen(ptr1);

        /* accumulate display length of string1 */
        s1_width += dsplen;

        /*
         * if string1 is longer/wider than the requested output_width,
         * discard this character and prepend a half-width space instead
         */
        if(s1_width >= output_width)
        {
            if(s1_width != output_width)
            {
                /* secure bytes for a half-width space in the final output */
                if (output_width != 0)
                {
                    s1_add_blen += hslen;
                    half_space = ON;
                }
            }
            else /* exactly fits, so include this chracter */
            {
                s1_add_blen += mlen;
            }

            /*
             * turn off string2 processing because string1 already
             * consumed output_width
             */
            s2_operate = OFF;

            /* done with string1 */
            break;
        }

        /* accumulate string1's portion of byte-length of the output */
        s1_add_blen += mlen;

        /* advance one character within string1 */
        ptr1 += mlen;

        /* loop counter */
        s1blen -= mlen;
    }

    /* Calculate the length of the portion composed of string2 to use for padding */
    if (s2_operate)
    {
        /* remaining part of output_width is composed of string2 */
        s2_add_width = output_width - s1_width;

        ptr2 = ptr2start = VARDATA_ANY(string2);
        ptr2end = ptr2 + s2blen;

        while (s2_add_width > 0)
        {
            /*  byte-length and display length per character of string2 */
            mlen = pg_mblen(ptr2);
            dsplen = pg_dsplen(ptr2);

            /*
             * output_width can not fit this character of string2, so discard it and
             * prepend a half-width space instead
             */
            if(dsplen > s2_add_width)
            {
                s2_add_blen += hslen;
                half_space = ON;

                /* done with string2 */
                break;
            }

            /* accumulate string2's portion of byte-length of the output */
            s2_add_blen += mlen;

            /* loop counter */
            s2_add_width -= dsplen;

            /* advance one character within string2 */
            ptr2 += mlen;

            /* when get to the end of string2, reset ptr2 to the start */
            if (ptr2 == ptr2end)
                ptr2 = ptr2start;
        }
    }

    /* allocate enough space to contain output_width worth of characters */
    total_blen = s1_add_blen + s2_add_blen;
    ret = (text *) palloc(VARHDRSZ + total_blen);
    ptr_ret = VARDATA(ret);

    /*
     * add a half-width space as a padding necessary to satisfy the required
     * output_width
     *
     * (memory already allocated as reserved by either s1_add_blen
     *  or s2_add_blen)
     */
    if (half_space)
    {
        memcpy(ptr_ret, spc, hslen);
        ptr_ret += hslen;
    }

    /* prepend string2 padding */
    while(s2_add_blen > 0)
    {
        /* reset ptr2 to the string2 start */
        if(init_ptr)
        {
            init_ptr = OFF;
            ptr2 = ptr2start;
        }

        mlen = pg_mblen(ptr2);
        if ( s2_add_blen < mlen )
            break;

        memcpy(ptr_ret, ptr2, mlen);
        ptr_ret += mlen;
        ptr2 += mlen;

        /* loop counter */
        s2_add_blen -= mlen;

        /* when get to the end of string2, reset ptr2 back to the start */
        if (ptr2 == ptr2end)
            ptr2 = ptr2start;
    }

    init_ptr = ON;

    /* string1 */
    while(s1_add_blen > 0)
    {
        /* reset ptr1 back to the start of string1 */
        if(init_ptr)
        {
            init_ptr = OFF;
            ptr1 = VARDATA_ANY(string1);
        }

        mlen = pg_mblen(ptr1);

        if( s1_add_blen < mlen )
            break;

        memcpy(ptr_ret, ptr1, mlen);
        ptr_ret += mlen;
        ptr1 += mlen;

        /* loop counter */
        s1_add_blen -= mlen;
    }

    SET_VARSIZE(ret, ptr_ret - (char *) ret);

    PG_RETURN_TEXT_P(ret);
}

/*
 * orcl_rpad(string text, length int32 [, fill text])
 *
 * Fill up the string to length 'length' by appending
 * the characters fill (a half-width space by default)
 */
Datum
orcl_rpad(PG_FUNCTION_ARGS)
{// #lizard forgives
    /*length related variables*/
    int        mlen,
            dsplen,
            s1blen,
            s2blen,
            hslen,
            total_blen = 0,
            s1_width = 0,
            s2_add_width = 0,
            s1_add_blen = 0,
            s2_add_blen = 0;

    text    *string1 = PG_GETARG_TEXT_PP(0);
    int32    output_width = PG_GETARG_INT32(1);
    text    *string2 = PG_GETARG_TEXT_PP(2);
    text    *ret;
    char    *ptr1,
            *ptr2 = NULL,
            *ptr2start = NULL,
            *ptr2end = NULL,
            *ptr_ret,
            *spc = " ";

    
    bool    s2_operate = ON,
            half_space = OFF,
            init_ptr = ON;

    /*    
    *validate output width (the 2nd argument) 
    */
    if (output_width < 0)
        output_width = 0;
    if (output_width > PAD_MAX)
        output_width = PAD_MAX;

    /* get byte-length of the 1st and 3rd argument strings */
    s2blen = VARSIZE_ANY_EXHDR(string2);
    s1blen = VARSIZE_ANY_EXHDR(string1);

    /* validate the lengths */
    if (s2blen < 0)
        s2blen = 0;
    if (s1blen < 0)
        s1blen = 0;

    /* if the filler length is zero disable filling */
    if (s2blen == 0)
    {
        s2_operate = OFF;    /* turn off string2 processing flag */
        output_width = 0;    /* same behavior as Oracle database */
    }

    /* byte-length of half-width space */
    hslen = pg_mblen(spc);

    /*
     * Calculate the length of the portion of string1 to include in
     * the final output
     */
    ptr1 = VARDATA_ANY(string1);
    while (s1blen > 0)
    {
        /* byte-length and display length per character of string1 */
        dsplen = pg_dsplen(ptr1);
        mlen = pg_mblen(ptr1);

        /* accumulate display length of string1 */
        s1_width += dsplen;

        /*
         * if string1 is longer/wider than the requested output_width,
         * discard this character and prepend a half-width space instead
         */
        if(s1_width >= output_width)
        {
            if(s1_width != output_width)
            {
                /* secure bytes for a half-width space in the final output */
                if (output_width != 0)
                {
                    half_space = ON;
                    s1_add_blen += hslen;
                }
            }
            else /* exactly fits, so include this chracter */
            {
                s1_add_blen += mlen;
            }

            /*
             * turn off string2 processing because string1 already
             * consumed output_width
             */
            s2_operate = OFF;

            /* done with string1 */
            break;
        }
        
        /* advance one character within string1 */
        ptr1 += mlen;

        /* accumulate string1's portion of byte-length of the output */
        s1_add_blen += mlen;


        /* loop counter */
        s1blen -= mlen;
    }

    /* Calculate the length of the portion composed of string2 to use for padding */
    if (s2_operate)
    {
        /* remaining part of output_width is composed of string2 */
        s2_add_width = output_width - s1_width;

        ptr2 = ptr2start = VARDATA_ANY(string2);
        ptr2end = ptr2 + s2blen;

        while (s2_add_width > 0)
        {
            /*  byte-length and display length per character of string2 */
            dsplen = pg_dsplen(ptr2);
            mlen = pg_mblen(ptr2);

            /*
             * output_width can not fit this character of string2, so discard it and
             * prepend a half-width space instead
             */
            if(dsplen > s2_add_width)
            {
                half_space = ON;
                s2_add_blen += hslen;

                /* done with string2 */
                break;
            }

            /* loop counter */
            s2_add_width -= dsplen;

            /* accumulate string2's portion of byte-length of the output */
            s2_add_blen += mlen;


            /* advance one character within string2 */
            ptr2 += mlen;

            /* when get to the end of string2, reset ptr2 to the start */
            if (ptr2 == ptr2end)
                ptr2 = ptr2start;
        }
    }

    /* allocate enough space to contain output_width worth of characters */
    total_blen = s1_add_blen + s2_add_blen;
    ret = (text *) palloc(VARHDRSZ + total_blen);
    ptr_ret = VARDATA(ret);

    /* string1 */
    while(s1_add_blen > 0)
    {
        /* reset ptr1 back to the start of string1 */
        if(init_ptr)
        {
            init_ptr = OFF;
            ptr1 = VARDATA_ANY(string1);
        }

        mlen = pg_mblen(ptr1);

        if( s1_add_blen < mlen )
            break;

        memcpy(ptr_ret, ptr1, mlen);
        ptr_ret += mlen;
        ptr1 += mlen;

        /* loop counter */
        s1_add_blen -= mlen;
    }

    init_ptr = ON;

    /* append string2 padding */
    while(s2_add_blen > 0)
    {
        /* reset ptr2 to the string2 start */
        if(init_ptr)
        {
            init_ptr = OFF;
            ptr2 = ptr2start;
        }

        mlen = pg_mblen(ptr2);
        if ( s2_add_blen < mlen )
            break;

        memcpy(ptr_ret, ptr2, mlen);
        ptr_ret += mlen;
        ptr2 += mlen;

        /* loop counter */
        s2_add_blen -= mlen;

        /* when get to the end of string2, reset ptr2 back to the start */
        if (ptr2 == ptr2end)
            ptr2 = ptr2start;
    }

    /*
     * add a half-width space as a padding necessary to satisfy the required
     * output_width
     *
     * (memory already allocated as reserved by either s1_add_blen
     *  or s2_add_blen)
     */
    if (half_space)
    {
        memcpy(ptr_ret, spc, hslen);
        ptr_ret += hslen;
    }

    SET_VARSIZE(ret, ptr_ret - (char *) ret);

    PG_RETURN_TEXT_P(ret);
}
