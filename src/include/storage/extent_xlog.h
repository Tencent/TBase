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
 * extentmapping_xlog.h
 *      Basic buffer manager data types.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/extentmapping_xlog.h
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef _EXTENTMAPPING_XLOG_H_
#define _EXTENTMAPPING_XLOG_H_

#include "access/xlogreader.h"
#include "lib/stringinfo.h"
/*
 * related macro define 
 */
#define XLOG_EXTENT_NEW_EXTENT            0x00
#define XLOG_EXTENT_UPDATE_EME            0x10
#define XLOG_EXTENT_APPEND_EXTENT        0x20
#define XLOG_EXTENT_ATTACH_EXTENT        0x30
#define XLOG_EXTENT_DETACH_EXTENT        0x40
#define XLOG_EXTENT_MAKE_FULL            0x50
#define XLOG_EXTENT_MAKE_AVAIL            0x60
#define XLOG_EXTENT_FREE_DISK            0x70
#define XLOG_EXTENT_TRUNCATE            0x80
#define XLOG_EXTENT_COMMON                0x90
#define XLOG_EXTENT_EXTEND                0xA0

#define XLOG_EXTENT_OPMASK                0xF0

#define FragTag_EXTENT_XLOG_SETEOB        0x01
#define FragTag_EXTENT_XLOG_EXTENDEOB    0x02
#define FragTag_EXTENT_XLOG_SETEME        0x03
#define FragTag_EXTENT_XLOG_EXTENDEME    0x04
#define FragTag_EXTENT_XLOG_INITEME        0x05
#define FragTag_EXTENT_XLOG_CLEANEME    0x06
#define FragTag_EXTENT_XLOG_SETESA        0x07
#define FragTag_EXTENT_XLOG_TRUNCATE    0x08
#define FragTag_EXTENT_XLOG_TRUNCEOB    0x09
#define FragTag_EXTENT_XLOG_CLEANEOB    0x0a
#define FragTag_EXTENT_XLOG_TRUNCEMA    0x0b
#define FragTag_EXTENT_XLOG_CLEANEMA    0x0c



#define INIT_EXLOG_SETEOB(xlrecptr) ((xlrecptr)->tag = FragTag_EXTENT_XLOG_SETEOB)
#define INIT_EXLOG_EXTENDEOB(xlrecptr) ((xlrecptr)->tag = FragTag_EXTENT_XLOG_EXTENDEOB)
#define INIT_EXLOG_SETEME(xlrecptr) ((xlrecptr)->tag = FragTag_EXTENT_XLOG_SETEME)
#define INIT_EXLOG_EXTENDEME(xlrecptr) ((xlrecptr)->tag = FragTag_EXTENT_XLOG_EXTENDEME)
#define INIT_EXLOG_INITEME(xlrecptr) ((xlrecptr)->tag = FragTag_EXTENT_XLOG_INITEME)
#define INIT_EXLOG_CLEANEME(xlrecptr) ((xlrecptr)->tag = FragTag_EXTENT_XLOG_CLEANEME)
#define INIT_EXLOG_SETESA(xlrecptr) ((xlrecptr)->tag = FragTag_EXTENT_XLOG_SETESA)
#define INIT_EXLOG_TRUNCATE(xlrecptr) ((xlrecptr)->tag = FragTag_EXTENT_XLOG_TRUNCATE)
#define INIT_EXLOG_TRUNCEOB(xlrecptr) ((xlrecptr)->tag = FragTag_EXTENT_XLOG_TRUNCEOB)
#define INIT_EXLOG_CLEANEOB(xlrecptr) ((xlrecptr)->tag = FragTag_EXTENT_XLOG_CLEANEOB)
#define INIT_EXLOG_TRUNCEMA(xlrecptr) ((xlrecptr)->tag = FragTag_EXTENT_XLOG_TRUNCEMA)
#define INIT_EXLOG_CLEANEMA(xlrecptr) ((xlrecptr)->tag = FragTag_EXTENT_XLOG_CLEANEMA)

typedef struct xl_extent_seteob
{
    int8    tag;
    int16    slot;
    bool     setfree;
}xl_extent_seteob;

#define SizeOfSetEOB     sizeof(xl_extent_seteob)


typedef struct xl_extent_extendeob
{
    int8    tag;
    int8    flags;
    int16    slot;
    int16    n_eobs;
    int32     setfree_start;
    int32    setfree_end;
}xl_extent_extendeob;

#define SizeOfExtendEOB     sizeof(xl_extent_extendeob)

#define EXTEND_EOB_FLAGS_SETFREE     0x01

typedef struct xl_extent_seteme
{
    int8    tag;
    int16     slot;
    int16    setflag;
    ExtentID     extentid;
    ExtentMappingElement eme;
    RelFileNode    rnode;
}xl_extent_seteme;

#define SizeOfSetEME     sizeof(xl_extent_seteme)

typedef struct xl_extent_extendeme
{
    int8    tag;
    int8    flags;
    int16    n_emes;
    int32    setfree_start;
    int32    setfree_end;
}xl_extent_extendeme;

#define SizeOfExtendEME     sizeof(xl_extent_extendeme)
#define EXTEND_EME_FLAGS_SETFREE     0x01

typedef struct xl_extent_initeme
{
    int8    tag;
    int16     slot;
    int8    freespace;
    ShardID    shardid;
}xl_extent_initeme;

#define SizeOfInitEME     sizeof(xl_extent_initeme)

typedef struct xl_extent_cleaneme
{
    int8    tag;
    int16     slot;
}xl_extent_cleaneme;

#define SizeOfCleanEME    sizeof(xl_extent_cleaneme)

typedef struct xl_extent_setesa
{
    int8    tag;
    int16     slot;
    int16    setflag;    
    EMAShardAnchor    anchor;
}xl_extent_setesa;

#define SizeOfSetESA    sizeof(xl_extent_setesa)

typedef struct xl_extent_truncate
{
    int8    tag;
    RelFileNode rnode;
}xl_extent_truncate;

#define SizeOfTruncateExtentSeg    sizeof(xl_extent_truncate)

typedef struct xl_extent_trunceob
{
    int8    tag;
    int16    pageno;
    int32    offset;
    RelFileNode rnode;    
}xl_extent_trunceob;

#define SizeOfTruncEOB sizeof(xl_extent_trunceob)

typedef struct xl_extent_cleaneob
{
    int8    tag;
    int16    pageno;
    RelFileNode rnode;
}xl_extent_cleaneob;

#define SizeOfCleanEOB sizeof(xl_extent_cleaneob)

typedef struct xl_extent_truncema
{
    int8    tag;
    int16    pageno;
    int32    offset;
    RelFileNode rnode;    
}xl_extent_truncema;

#define SizeOfTruncEMA sizeof(xl_extent_truncema)

typedef struct xl_extent_cleanema
{
    int8    tag;
    int16    pageno;
    RelFileNode rnode;
}xl_extent_cleanema;

#define SizeOfCleanEMA sizeof(xl_extent_cleanema)

/* 
 *related function declaration 
 */
extern void extent_redo(XLogReaderState *record);
extern void extent_desc(StringInfo buf, XLogReaderState *record);
extern const char *extent_identify(uint8 info);

#endif                            /* _EXTENTMAPPING_XLOG_H_ */
