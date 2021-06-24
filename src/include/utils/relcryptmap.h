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
 * relcryptmap.h
 *    relation file crypt map
 *
 * Portions Copyright (c) 2018, Tbase Global Development Group
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/relcryptmap.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RELCRYPT_MAP_H
#define RELCRYPT_MAP_H

#include "access/xlogreader.h"
#include "lib/stringinfo.h"
#include "storage/relfilenode.h"
#include "utils/relcrypt.h"

/* ----------------
 *      relcrypt-related XLOG entries
 * ----------------
 */

#define XLOG_CRYPT_KEY_INSERT               0x10
#define XLOG_CRYPT_KEY_DELETE               0x20
#define XLOG_REL_CRYPT_INSERT               0x40
#define XLOG_REL_CRYPT_DELETE               0x80

typedef struct xl_rel_crypt_insert
{
    RelFileNode rnode;
    int         algo_id;
} xl_rel_crypt_insert;

typedef xl_rel_crypt_insert xl_rel_crypt_delete;

extern void rel_crypt_redo(XLogReaderState *record);
extern void rel_crypt_desc(StringInfo buf, XLogReaderState *record);
extern const char * rel_crypt_identify(uint8 info);
extern void cyprt_key_info_hash_init(void);
extern void crypt_key_info_load_mapfile(void);
extern void rel_cyprt_hash_init(void);
extern void rel_crypt_load_mapfile(void);
extern void CheckPointRelCrypt(void);
extern void StartupReachConsistentState(void);
extern Size crypt_key_info_hash_shmem_size(void);
extern Size rel_crypt_hash_shmem_size(void);
extern bool crypt_key_info_hash_lookup(AlgoId algo_id, CryptKeyInfo * relcrypt_ret);
extern void cryt_ky_inf_fil_stru_for_deflt_alg(AlgoId algo_id, CryptKeyInfo cryptkey_out);
extern void crypt_key_info_load_default_key(void);
extern void crypt_key_info_free(CryptKeyInfo cryptkey);
extern CryptKeyInfo crypt_key_info_alloc(int option);
extern void rel_crypt_hash_insert(RelFileNode * rnode, AlgoId algo_id, bool write_wal, bool in_building_procedure);
extern void remove_rel_crypt_hash_elem(RelCrypt relCrypt, bool write_wal);
extern void rel_crypt_hash_delete(RelFileNode * rnode, bool write_wal);
extern void crypt_key_info_hash_insert(CryptKeyInfo cryptkey_input, bool write_wal, bool in_building_procedure);
extern int crypt_key_info_cal_key_size(CryptKeyInfo cryptkey);
extern bool CheckRelFileNodeExists(RelFileNode *rnode);
extern List* MarkRelCryptInvalid(void);
extern void rel_crypt_write_mapfile(bool is_backup);
#endif                          /* RELCRYPT_MAP_H */
