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
 * relcryptdesc.c
 *    rmgr descriptor routines for utils/cache/relcrypt.c
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/backend/access/rmgrdesc/relcryptdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/relcrypt.h"
#include "utils/relcryptmap.h"
#include "access/xlogrecord.h"

void rel_crypt_desc(StringInfo buf, XLogReaderState *record)
{
    uint8                info;

    info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    switch(info & XLR_RMGR_INFO_MASK)
    {
        case XLOG_CRYPT_KEY_INSERT:
            {
                CryptKeyInfo xlrec;
                xlrec = (CryptKeyInfo ) XLogRecGetData(record);
                appendStringInfo(buf, "crypt key insert, algo_id:%d, option:%d, keysize:%d", 
                    xlrec->algo_id, xlrec->option, xlrec->keysize);
                break;
            }
        case XLOG_CRYPT_KEY_DELETE:
            appendStringInfo(buf, "xlog type is comming, info:%u", XLOG_CRYPT_KEY_DELETE);
            break;
        case XLOG_REL_CRYPT_INSERT:
            {
                xl_rel_crypt_insert *xlrec;
                xlrec = (xl_rel_crypt_insert *) XLogRecGetData(record);
                appendStringInfo(buf, "rel crypt insert, database:%u tablespace:%u relnode:%u, algo_id:%d",
                                 xlrec->rnode.dbNode, xlrec->rnode.spcNode, xlrec->rnode.relNode, xlrec->algo_id);
                break;
            }
        case XLOG_REL_CRYPT_DELETE:
            {
            	xl_rel_crypt_delete *xlrec;
				xlrec = (xl_rel_crypt_delete *) XLogRecGetData(record);
				appendStringInfo(buf, "rel crypt delete, database:%u tablespace:%u relnode:%u, algo_id:%d",
						xlrec->rnode.dbNode, xlrec->rnode.spcNode, xlrec->rnode.relNode, xlrec->algo_id);
            break;
            }
        default:
            Assert(0);
            break;
    }
    
    return;
}

const char * rel_crypt_identify(uint8 info)
{
    const char *id = NULL;

    switch(info & XLR_RMGR_INFO_MASK)
    {
        case XLOG_CRYPT_KEY_INSERT:
            id = "CRYPT KEY INSERT";
            break;
        case XLOG_CRYPT_KEY_DELETE:
            id = "CRYPT KEY DELETE";
            break;
        case XLOG_REL_CRYPT_INSERT:
            id = "REL CRYPT INSERT";
            break;
        case XLOG_REL_CRYPT_DELETE:
            id = "REL CRYPT DELETE";
            break;
        default:
            Assert(0);
            break;
    }

    return id;
}

