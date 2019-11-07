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
 * mbuf.h
 *        Memory buffer operations.
 *
 * Copyright (c) 2005 Marko Kreen
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
 * contrib/pgcrypto/mbuf.h
 */

#ifndef __PX_MBUF_H
#define __PX_MBUF_H

typedef struct MBuf MBuf;
typedef struct PushFilter PushFilter;
typedef struct PullFilter PullFilter;
typedef struct PushFilterOps PushFilterOps;
typedef struct PullFilterOps PullFilterOps;

struct PushFilterOps
{
    /*
     * should return needed buffer size, 0- no buffering, <0 on error if NULL,
     * no buffering, and priv=init_arg
     */
    int            (*init) (PushFilter *next, void *init_arg, void **priv_p);

    /*
     * send data to next.  should consume all? if null, it will be simply
     * copied (in-place) returns 0 on error
     */
    int            (*push) (PushFilter *next, void *priv,
                                     const uint8 *src, int len);
    int            (*flush) (PushFilter *next, void *priv);
    void        (*free) (void *priv);
};

struct PullFilterOps
{
    /*
     * should return needed buffer size, 0- no buffering, <0 on error if NULL,
     * no buffering, and priv=init_arg
     */
    int            (*init) (void **priv_p, void *init_arg, PullFilter *src);

    /*
     * request data from src, put result ptr to data_p can use ptr from src or
     * use buf as work area if NULL in-place copy
     */
    int            (*pull) (void *priv, PullFilter *src, int len,
                                     uint8 **data_p, uint8 *buf, int buflen);
    void        (*free) (void *priv);
};

/*
 * Memory buffer
 */
MBuf       *mbuf_create(int len);
MBuf       *mbuf_create_from_data(uint8 *data, int len);
int            mbuf_tell(MBuf *mbuf);
int            mbuf_avail(MBuf *mbuf);
int            mbuf_size(MBuf *mbuf);
int            mbuf_grab(MBuf *mbuf, int len, uint8 **data_p);
int            mbuf_steal_data(MBuf *mbuf, uint8 **data_p);
int            mbuf_append(MBuf *dst, const uint8 *buf, int cnt);
int            mbuf_rewind(MBuf *mbuf);
int            mbuf_free(MBuf *mbuf);

/*
 * Push filter
 */
int pushf_create(PushFilter **res, const PushFilterOps *ops, void *init_arg,
             PushFilter *next);
int            pushf_write(PushFilter *mp, const uint8 *data, int len);
void        pushf_free_all(PushFilter *mp);
void        pushf_free(PushFilter *mp);
int            pushf_flush(PushFilter *mp);

int            pushf_create_mbuf_writer(PushFilter **mp_p, MBuf *mbuf);

/*
 * Pull filter
 */
int pullf_create(PullFilter **res, const PullFilterOps *ops,
             void *init_arg, PullFilter *src);
int            pullf_read(PullFilter *mp, int len, uint8 **data_p);
int pullf_read_max(PullFilter *mp, int len,
               uint8 **data_p, uint8 *tmpbuf);
void        pullf_free(PullFilter *mp);
int            pullf_read_fixed(PullFilter *src, int len, uint8 *dst);

int            pullf_create_mbuf_reader(PullFilter **pf_p, MBuf *mbuf);

#define GETBYTE(pf, dst) \
    do { \
        uint8 __b; \
        int __res = pullf_read_fixed(pf, 1, &__b); \
        if (__res < 0) \
            return __res; \
        (dst) = __b; \
    } while (0)

#endif   /* __PX_MBUF_H */
