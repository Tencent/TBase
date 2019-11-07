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
#ifndef _MLS_H_
#define _MLS_H_

#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "utils/relcache.h"
#include "access/transam.h"
#include "access/heapam.h"
#include "executor/executor.h"
#include "postmaster/postmaster.h"
#include "storage/relfilenode.h"
#include "utils/cls.h"
/*Special purpose*/
#define MARK(X) 1

/*
 * this macro seems redundant, cause IsSystemRelation has already done a more elegant work,
 * while, we just keep a sample priciple, that is tables created in initdb procedure, we just skip them.
 */
#define IS_SYSTEM_REL(_relid)       ((_relid) < FirstNormalObjectId)

extern bool g_enable_crypt_check;
extern bool g_is_mls_user;

/*
* related function declaration
*/
extern List * FetchAllParitionList(Oid relid);
extern void MlsExecCheck(ScanState *node, TupleTableSlot *slot);
extern Size MlsShmemSize(void);
extern bool mls_check_relation_permission(Oid relid, bool * schema_bound);
extern bool mls_check_schema_permission(Oid schemaoid);
extern bool mls_check_column_permission(Oid relid, int attnum);
extern bool mls_check_role_permission(Oid roleid);
extern int transfer_rol_kind_ext(Oid rolid);
extern int transfer_rel_kind_ext(Oid relid);
extern bool mls_user(void);
extern Oid mls_get_parent_oid(Relation rel);
extern void CacheInvalidateRelcacheAllPatition(Oid databaseid, Oid relid);
extern bool is_mls_or_audit_user(void);
extern void MlsShmemInit(void);
extern bool mls_support_data_type(Oid typid);
extern Oid mls_get_parent_oid_by_relid(Oid relid);
extern const char * mls_query_string_prune(const char * querystring);
extern bool is_mls_user(void);
extern bool userid_is_mls_user(Oid userid);
extern bool is_mls_root_user(void);
extern bool check_is_mls_user(void);

extern void mls_start_encrypt_parellel_workers(void);
extern List* mls_encrypt_buf_parellel(List * buf_id_list, int16 algo_id, int buf_id, int status);
extern bool mls_encrypt_queue_is_empty(void);
extern List * mls_get_crypted_buflist(List * buf_id_list);
extern void mls_crypt_worker_free_slot(int worker_id, int slot_id);
extern List * SyncBufidListAppend(List * buf_id_list, int buf_id, int status, int slot_id, int worker_id, char* bufToWrite);
extern void mls_crypt_parellel_main_exit(void);
extern uint32 mls_crypt_parle_get_queue_capacity(void);
extern void mls_log_crypt_worker_detail(void);
extern void mls_check_datamask_need_passby(ScanState * scanstate, Oid relid);
extern bool mls_check_inner_conn(const char * cmd_option);
extern int mls_check_schema_crypted(Oid schemaoid);
extern int mls_check_tablespc_crypted(Oid tablespcoid);
extern void InsertTrsprtCryptPolicyMapTuple(Relation pg_transp_crypt_map_desc,
                                                    Oid relnamespace,
                                                    Oid    reltablespace);
extern void init_extension_table_oids(void);
extern void check_tbase_mls_extension(void);
extern void CheckMlsTableUserAcl(ResultRelInfo *resultRelInfo, HeapTuple tuple);
extern bool check_user_has_acl_for_namespace(Oid target_namespace);
extern bool check_user_has_acl_for_relation(Oid target_relid);

#endif /*_MLS_H_*/
