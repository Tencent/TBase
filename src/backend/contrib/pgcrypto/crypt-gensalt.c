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
 * Written by Solar Designer and placed in the public domain.
 * See crypt_blowfish.c for more information.
 *
 * contrib/pgcrypto/crypt-gensalt.c
 *
 * This file contains salt generation functions for the traditional and
 * other common crypt(3) algorithms, except for bcrypt which is defined
 * entirely in crypt_blowfish.c.
 *
 * Put bcrypt generator also here as crypt-blowfish.c
 * may not be compiled always.          -- marko
 */

#include "postgres.h"

#include "contrib/pgcrypto/px-crypt.h"

typedef unsigned int BF_word;

static unsigned char _crypt_itoa64[64 + 1] =
"./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

char *
_crypt_gensalt_traditional_rn(unsigned long count,
                  const char *input, int size, char *output, int output_size)
{
    if (size < 2 || output_size < 2 + 1 || (count && count != 25))
    {
        if (output_size > 0)
            output[0] = '\0';
        return NULL;
    }

    output[0] = _crypt_itoa64[(unsigned int) input[0] & 0x3f];
    output[1] = _crypt_itoa64[(unsigned int) input[1] & 0x3f];
    output[2] = '\0';

    return output;
}

char *
_crypt_gensalt_extended_rn(unsigned long count,
                  const char *input, int size, char *output, int output_size)
{
    unsigned long value;

/* Even iteration counts make it easier to detect weak DES keys from a look
 * at the hash, so they should be avoided */
    if (size < 3 || output_size < 1 + 4 + 4 + 1 ||
        (count && (count > 0xffffff || !(count & 1))))
    {
        if (output_size > 0)
            output[0] = '\0';
        return NULL;
    }

    if (!count)
        count = 725;

    output[0] = '_';
    output[1] = _crypt_itoa64[count & 0x3f];
    output[2] = _crypt_itoa64[(count >> 6) & 0x3f];
    output[3] = _crypt_itoa64[(count >> 12) & 0x3f];
    output[4] = _crypt_itoa64[(count >> 18) & 0x3f];
    value = (unsigned long) (unsigned char) input[0] |
        ((unsigned long) (unsigned char) input[1] << 8) |
        ((unsigned long) (unsigned char) input[2] << 16);
    output[5] = _crypt_itoa64[value & 0x3f];
    output[6] = _crypt_itoa64[(value >> 6) & 0x3f];
    output[7] = _crypt_itoa64[(value >> 12) & 0x3f];
    output[8] = _crypt_itoa64[(value >> 18) & 0x3f];
    output[9] = '\0';

    return output;
}

char *
_crypt_gensalt_md5_rn(unsigned long count,
                  const char *input, int size, char *output, int output_size)
{
    unsigned long value;

    if (size < 3 || output_size < 3 + 4 + 1 || (count && count != 1000))
    {
        if (output_size > 0)
            output[0] = '\0';
        return NULL;
    }

    output[0] = '$';
    output[1] = '1';
    output[2] = '$';
    value = (unsigned long) (unsigned char) input[0] |
        ((unsigned long) (unsigned char) input[1] << 8) |
        ((unsigned long) (unsigned char) input[2] << 16);
    output[3] = _crypt_itoa64[value & 0x3f];
    output[4] = _crypt_itoa64[(value >> 6) & 0x3f];
    output[5] = _crypt_itoa64[(value >> 12) & 0x3f];
    output[6] = _crypt_itoa64[(value >> 18) & 0x3f];
    output[7] = '\0';

    if (size >= 6 && output_size >= 3 + 4 + 4 + 1)
    {
        value = (unsigned long) (unsigned char) input[3] |
            ((unsigned long) (unsigned char) input[4] << 8) |
            ((unsigned long) (unsigned char) input[5] << 16);
        output[7] = _crypt_itoa64[value & 0x3f];
        output[8] = _crypt_itoa64[(value >> 6) & 0x3f];
        output[9] = _crypt_itoa64[(value >> 12) & 0x3f];
        output[10] = _crypt_itoa64[(value >> 18) & 0x3f];
        output[11] = '\0';
    }

    return output;
}



static unsigned char BF_itoa64[64 + 1] =
"./ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

static void
BF_encode(char *dst, const BF_word *src, int size)
{
    const unsigned char *sptr = (const unsigned char *) src;
    const unsigned char *end = sptr + size;
    unsigned char *dptr = (unsigned char *) dst;
    unsigned int c1,
                c2;

    do
    {
        c1 = *sptr++;
        *dptr++ = BF_itoa64[c1 >> 2];
        c1 = (c1 & 0x03) << 4;
        if (sptr >= end)
        {
            *dptr++ = BF_itoa64[c1];
            break;
        }

        c2 = *sptr++;
        c1 |= c2 >> 4;
        *dptr++ = BF_itoa64[c1];
        c1 = (c2 & 0x0f) << 2;
        if (sptr >= end)
        {
            *dptr++ = BF_itoa64[c1];
            break;
        }

        c2 = *sptr++;
        c1 |= c2 >> 6;
        *dptr++ = BF_itoa64[c1];
        *dptr++ = BF_itoa64[c2 & 0x3f];
    } while (sptr < end);
}

char *
_crypt_gensalt_blowfish_rn(unsigned long count,
                  const char *input, int size, char *output, int output_size)
{
    if (size < 16 || output_size < 7 + 22 + 1 ||
        (count && (count < 4 || count > 31)))
    {
        if (output_size > 0)
            output[0] = '\0';
        return NULL;
    }

    if (!count)
        count = 5;

    output[0] = '$';
    output[1] = '2';
    output[2] = 'a';
    output[3] = '$';
    output[4] = '0' + count / 10;
    output[5] = '0' + count % 10;
    output[6] = '$';

    BF_encode(&output[7], (const BF_word *) input, 16);
    output[7 + 22] = '\0';

    return output;
}
