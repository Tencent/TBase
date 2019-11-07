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
 * compress_io.h
 *     Interface to compress_io.c routines
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *       src/bin/pg_dump/compress_io.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef __COMPRESS_IO__
#define __COMPRESS_IO__

#include "pg_backup_archiver.h"

/* Initial buffer sizes used in zlib compression. */
#define ZLIB_OUT_SIZE    4096
#define ZLIB_IN_SIZE    4096

typedef enum
{
    COMPR_ALG_NONE,
    COMPR_ALG_LIBZ
} CompressionAlgorithm;

/* Prototype for callback function to WriteDataToArchive() */
typedef void (*WriteFunc) (ArchiveHandle *AH, const char *buf, size_t len);

/*
 * Prototype for callback function to ReadDataFromArchive()
 *
 * ReadDataFromArchive will call the read function repeatedly, until it
 * returns 0 to signal EOF. ReadDataFromArchive passes a buffer to read the
 * data into in *buf, of length *buflen. If that's not big enough for the
 * callback function, it can free() it and malloc() a new one, returning the
 * new buffer and its size in *buf and *buflen.
 *
 * Returns the number of bytes read into *buf, or 0 on EOF.
 */
typedef size_t (*ReadFunc) (ArchiveHandle *AH, char **buf, size_t *buflen);

/* struct definition appears in compress_io.c */
typedef struct CompressorState CompressorState;

extern CompressorState *AllocateCompressor(int compression, WriteFunc writeF);
extern void ReadDataFromArchive(ArchiveHandle *AH, int compression,
                    ReadFunc readF);
extern void WriteDataToArchive(ArchiveHandle *AH, CompressorState *cs,
                   const void *data, size_t dLen);
extern void EndCompressor(ArchiveHandle *AH, CompressorState *cs);


typedef struct cfp cfp;

extern cfp *cfopen(const char *path, const char *mode, int compression);
extern cfp *cfopen_read(const char *path, const char *mode);
extern cfp *cfopen_write(const char *path, const char *mode, int compression);
extern int    cfread(void *ptr, int size, cfp *fp);
extern int    cfwrite(const void *ptr, int size, cfp *fp);
extern int    cfgetc(cfp *fp);
extern char *cfgets(cfp *fp, char *buf, int len);
extern int    cfclose(cfp *fp);
extern int    cfeof(cfp *fp);
extern const char *get_cfp_error(cfp *fp);

#endif
