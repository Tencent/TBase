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
#ifndef __PGXC_AUDIT_FGA__H
#define __PGXC_AUDIT_FGA__H

#include "postgres.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/execnodes.h"

#define AUDIT_FGA_SQL_LEN   4096
#define AUDIT_TRIGGER_FEEDBACK_LEN  256

extern bool enable_fga;
extern const char *g_commandTag;


/* simple list of strings */
typedef struct _stringlist
{
    char       *str;
    struct _stringlist *next;
} _stringlist;

typedef struct AuditFgaPolicy
{
    NodeTag     type;
    char       *policy_name;    /* Name of the policy */
    //Expr       *qual;            /* Expression to audit condition */
    List       *qual;            /* Expression to audit condition */
    char       *query_string;
} AuditFgaPolicy;

typedef struct audit_fga_policy_state
{
    char       *policy_name;    /* Name of the policy */
    ExprState  *qual;            /* Expression to audit condition */
    char       *query_string;
} audit_fga_policy_state;

typedef enum exec_status
{
    FGA_STATUS_INIT = 0,
    FGA_STATUS_OK,
    FGA_STATUS_DOING,
    FGA_STATUS_FAIL
} exec_status;


typedef struct audit_fga_tigger_info
{
    int     backend_pid;
    char    user_name[NAMEDATALEN];
    char    db_name[NAMEDATALEN];
    char    host[32];
    char    port[32];    
    Oid     handler_module;
    char    func_name[NAMEDATALEN];
    int     status;
    char    exec_feedback[AUDIT_TRIGGER_FEEDBACK_LEN];
} audit_fga_tigger_info;

/*related function declaration*/
extern Oid schema_name_2_oid(text *in_string);
extern Oid object_name_2_oid(text *in_string, Oid schema_oid);
extern Oid function_name_2_oid(text *in_string, Oid schema_oid);
extern Datum text_2_namedata_datum(text *in_string);
extern void exec_policy_funct_on_other_node(char *query_string);

extern bool has_policy_matched_cmd(char * cmd_type, Datum statement_types_datum, bool is_null);
extern bool has_policy_matched_columns(List * tlist, oidvector *audit_column_oids, bool audit_column_opts);
extern bool get_audit_fga_quals(Oid rel, char * cmd_type, List *tlist, List **audit_fga_policy_list);
extern void audit_fga_log_policy_info(AuditFgaPolicy *policy_s, char * cmd_type);
extern void audit_fga_log_policy_info_2(audit_fga_policy_state *policy_s, char * cmd_type);

extern void audit_fga_log_prefix(StringInfoData *buf);
extern void audit_fga_log_prefix_json(StringInfoData *buf);

extern void ApplyAuditFgaMain(Datum main_arg);
extern void ApplyAuditFgaRegister(void);
extern Size AuditFgaShmemSize(void);
extern void AuditFgaShmemInit(void);
extern void write_trigger_handle_to_shmem(Oid func);


#endif  /*AUDIT_FGA_H*/

