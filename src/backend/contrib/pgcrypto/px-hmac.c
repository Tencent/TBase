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
 * px-hmac.c
 *        HMAC implementation.
 *
 * Copyright (c) 2001 Marko Kreen
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *      notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *      notice, this list of conditions and the following disclaimer in the
 *      documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 * contrib/pgcrypto/px-hmac.c
 */

#include "postgres.h"

#include "contrib/pgcrypto/px.h"

#define HMAC_IPAD 0x36
#define HMAC_OPAD 0x5C

static unsigned
hmac_result_size(PX_HMAC *h)
{
    return px_md_result_size(h->md);
}

static unsigned
hmac_block_size(PX_HMAC *h)
{
    return px_md_block_size(h->md);
}

static void
hmac_init(PX_HMAC *h, const uint8 *key, unsigned klen)
{
    unsigned    bs,
                i;
    uint8       *keybuf;
    PX_MD       *md = h->md;

    bs = px_md_block_size(md);
    keybuf = crypt_alloc(bs);
    memset(keybuf, 0, bs);

    if (klen > bs)
    {
        px_md_update(md, key, klen);
        px_md_finish(md, keybuf);
        px_md_reset(md);
    }
    else
        memcpy(keybuf, key, klen);

    for (i = 0; i < bs; i++)
    {
        h->p.ipad[i] = keybuf[i] ^ HMAC_IPAD;
        h->p.opad[i] = keybuf[i] ^ HMAC_OPAD;
    }

    crypt_memset(keybuf, 0, bs);
    crypt_free(keybuf);

    px_md_update(md, h->p.ipad, bs);
}

static void
hmac_reset(PX_HMAC *h)
{
    PX_MD       *md = h->md;
    unsigned    bs = px_md_block_size(md);

    px_md_reset(md);
    px_md_update(md, h->p.ipad, bs);
}

static void
hmac_update(PX_HMAC *h, const uint8 *data, unsigned dlen)
{
    px_md_update(h->md, data, dlen);
}

static void
hmac_finish(PX_HMAC *h, uint8 *dst)
{
    PX_MD       *md = h->md;
    unsigned    bs,
                hlen;
    uint8       *buf;

    bs = px_md_block_size(md);
    hlen = px_md_result_size(md);

    buf = crypt_alloc(hlen);

    px_md_finish(md, buf);

    px_md_reset(md);
    px_md_update(md, h->p.opad, bs);
    px_md_update(md, buf, hlen);
    px_md_finish(md, dst);

    crypt_memset(buf, 0, hlen);
    crypt_free(buf);
}

static void
hmac_free(PX_HMAC *h)
{
    unsigned    bs;

    bs = px_md_block_size(h->md);
    px_md_free(h->md);

    crypt_memset(h->p.ipad, 0, bs);
    crypt_memset(h->p.opad, 0, bs);
    crypt_free(h->p.ipad);
    crypt_free(h->p.opad);
    crypt_free(h);
}


/* PUBLIC FUNCTIONS */

int
px_find_hmac(const char *name, PX_HMAC **res)
{
    int            err;
    PX_MD       *md;
    PX_HMAC    *h;
    unsigned    bs;

    err = px_find_digest(name, &md);
    if (err)
        return err;

    bs = px_md_block_size(md);
    if (bs < 2)
    {
        px_md_free(md);
        return PXE_HASH_UNUSABLE_FOR_HMAC;
    }

    h = crypt_alloc(sizeof(*h));
    h->p.ipad = crypt_alloc(bs);
    h->p.opad = crypt_alloc(bs);
    h->md = md;

    h->result_size = hmac_result_size;
    h->block_size = hmac_block_size;
    h->reset = hmac_reset;
    h->update = hmac_update;
    h->finish = hmac_finish;
    h->free = hmac_free;
    h->init = hmac_init;

    *res = h;

    return 0;
}
