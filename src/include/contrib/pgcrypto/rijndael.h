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
 * contrib/pgcrypto/rijndael.h
 *
 *    $OpenBSD: rijndael.h,v 1.3 2001/05/09 23:01:32 markus Exp $ */

/* This is an independent implementation of the encryption algorithm:    */
/*                                                                        */
/*           RIJNDAEL by Joan Daemen and Vincent Rijmen                    */
/*                                                                        */
/* which is a candidate algorithm in the Advanced Encryption Standard    */
/* programme of the US National Institute of Standards and Technology.  */
/*                                                                        */
/* Copyright in this implementation is held by Dr B R Gladman but I        */
/* hereby give permission for its free direct or derivative use subject */
/* to acknowledgment of its origin and compliance with any conditions    */
/* that the originators of the algorithm place on its exploitation.     */
/*                                                                        */
/* Dr Brian Gladman (gladman@seven77.demon.co.uk) 14th January 1999        */

#ifndef _RIJNDAEL_H_
#define _RIJNDAEL_H_

/* 1. Standard types for AES cryptography source code                */

typedef uint8 u1byte;            /* an 8 bit unsigned character type */
typedef uint16 u2byte;            /* a 16 bit unsigned integer type    */
typedef uint32 u4byte;            /* a 32 bit unsigned integer type    */

typedef int8 s1byte;            /* an 8 bit signed character type    */
typedef int16 s2byte;            /* a 16 bit signed integer type        */
typedef int32 s4byte;            /* a 32 bit signed integer type        */

typedef struct _rijndael_ctx
{
    u4byte        k_len;
    int            decrypt;
    u4byte        e_key[64];
    u4byte        d_key[64];
} rijndael_ctx;


/* 2. Standard interface for AES cryptographic routines                */

/* These are all based on 32 bit unsigned values and will therefore */
/* require endian conversions for big-endian architectures            */

rijndael_ctx *
            rijndael_set_key(rijndael_ctx *, const u4byte *, const u4byte, int);
void        rijndael_encrypt(rijndael_ctx *, const u4byte *, u4byte *);
void        rijndael_decrypt(rijndael_ctx *, const u4byte *, u4byte *);

/* conventional interface */

void        aes_set_key(rijndael_ctx *ctx, const uint8 *key, unsigned keybits, int enc);
void        aes_ecb_encrypt(rijndael_ctx *ctx, uint8 *data, unsigned len);
void        aes_ecb_decrypt(rijndael_ctx *ctx, uint8 *data, unsigned len);
void        aes_cbc_encrypt(rijndael_ctx *ctx, uint8 *iva, uint8 *data, unsigned len);
void        aes_cbc_decrypt(rijndael_ctx *ctx, uint8 *iva, uint8 *data, unsigned len);

#endif   /* _RIJNDAEL_H_ */
