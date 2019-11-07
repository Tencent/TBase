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
 * File imported from FreeBSD, original by Poul-Henning Kamp.
 *
 * $FreeBSD: src/lib/libcrypt/crypt-md5.c,v 1.5 1999/12/17 20:21:45 peter Exp $
 *
 * contrib/pgcrypto/crypt-md5.c
 */

#include "postgres.h"

#include "contrib/pgcrypto/px.h"
#include "contrib/pgcrypto/px-crypt.h"

#define MD5_SIZE 16

static const char _crypt_a64[] =
"./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

static void
_crypt_to64(char *s, unsigned long v, int n)
{
    while (--n >= 0)
    {
        *s++ = _crypt_a64[v & 0x3f];
        v >>= 6;
    }
}

/*
 * UNIX password
 */

char *
px_crypt_md5(const char *pw, const char *salt, char *passwd, unsigned dstlen)
{
    static char *magic = "$1$"; /* This string is magic for this algorithm.
                                 * Having it this way, we can get better later
                                 * on */
    static char *p;
    static const char *sp,
               *ep;
    unsigned char final[MD5_SIZE];
    int            sl,
                pl,
                i;
    PX_MD       *ctx,
               *ctx1;
    int            err;
    unsigned long l;

    if (!passwd || dstlen < 120)
        return NULL;

    /* Refine the Salt first */
    sp = salt;

    /* If it starts with the magic string, then skip that */
    if (strncmp(sp, magic, strlen(magic)) == 0)
        sp += strlen(magic);

    /* It stops at the first '$', max 8 chars */
    for (ep = sp; *ep && *ep != '$' && ep < (sp + 8); ep++)
        continue;

    /* get the length of the true salt */
    sl = ep - sp;

    /* */
    err = px_find_digest("md5", &ctx);
    if (err)
        return NULL;
    err = px_find_digest("md5", &ctx1);

    /* The password first, since that is what is most unknown */
    px_md_update(ctx, (const uint8 *) pw, strlen(pw));

    /* Then our magic string */
    px_md_update(ctx, (uint8 *) magic, strlen(magic));

    /* Then the raw salt */
    px_md_update(ctx, (const uint8 *) sp, sl);

    /* Then just as many characters of the MD5(pw,salt,pw) */
    px_md_update(ctx1, (const uint8 *) pw, strlen(pw));
    px_md_update(ctx1, (const uint8 *) sp, sl);
    px_md_update(ctx1, (const uint8 *) pw, strlen(pw));
    px_md_finish(ctx1, final);
    for (pl = strlen(pw); pl > 0; pl -= MD5_SIZE)
        px_md_update(ctx, final, pl > MD5_SIZE ? MD5_SIZE : pl);

    /* Don't leave anything around in vm they could use. */
    crypt_memset(final, 0, sizeof final);

    /* Then something really weird... */
    for (i = strlen(pw); i; i >>= 1)
        if (i & 1)
            px_md_update(ctx, final, 1);
        else
            px_md_update(ctx, (const uint8 *) pw, 1);

    /* Now make the output string */
    strcpy(passwd, magic);
    strncat(passwd, sp, sl);
    strcat(passwd, "$");

    px_md_finish(ctx, final);

    /*
     * and now, just to make sure things don't run too fast On a 60 Mhz
     * Pentium this takes 34 msec, so you would need 30 seconds to build a
     * 1000 entry dictionary...
     */
    for (i = 0; i < 1000; i++)
    {
        px_md_reset(ctx1);
        if (i & 1)
            px_md_update(ctx1, (const uint8 *) pw, strlen(pw));
        else
            px_md_update(ctx1, final, MD5_SIZE);

        if (i % 3)
            px_md_update(ctx1, (const uint8 *) sp, sl);

        if (i % 7)
            px_md_update(ctx1, (const uint8 *) pw, strlen(pw));

        if (i & 1)
            px_md_update(ctx1, final, MD5_SIZE);
        else
            px_md_update(ctx1, (const uint8 *) pw, strlen(pw));
        px_md_finish(ctx1, final);
    }

    p = passwd + strlen(passwd);

    l = (final[0] << 16) | (final[6] << 8) | final[12];
    _crypt_to64(p, l, 4);
    p += 4;
    l = (final[1] << 16) | (final[7] << 8) | final[13];
    _crypt_to64(p, l, 4);
    p += 4;
    l = (final[2] << 16) | (final[8] << 8) | final[14];
    _crypt_to64(p, l, 4);
    p += 4;
    l = (final[3] << 16) | (final[9] << 8) | final[15];
    _crypt_to64(p, l, 4);
    p += 4;
    l = (final[4] << 16) | (final[10] << 8) | final[5];
    _crypt_to64(p, l, 4);
    p += 4;
    l = final[11];
    _crypt_to64(p, l, 2);
    p += 2;
    *p = '\0';

    /* Don't leave anything around in vm they could use. */
    crypt_memset(final, 0, sizeof final);

    px_md_free(ctx1);
    px_md_free(ctx);

    return passwd;
}
