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
#ifndef PGXC_AUDIT_FGA_H
#define PGXC_AUDIT_FGA_H

#include "catalog/genbki.h"

#define PgAuditFgaConfRelationId  5050

CATALOG(pg_audit_fga_conf,5050) BKI_WITHOUT_OIDS
{
    Oid                auditor_id;            /* who write this conf */
    Oid             object_schema;      /* object schema oid */
    Oid                object_id;            /* which object to be audited, include view and table */
    NameData        policy_name;        /* the unique name of the policy */
    oidvector       audit_column_ids;   /* the column oids to be checked for access */
    text            audit_columns;      /*  the columns to be checked for access */
#ifdef CATALOG_VARLEN    
    pg_node_tree    audit_condition;    /*  a condition in a row that indicates a monitoring condition */
#endif
    text            audit_condition_str;/*  a condition in a row that indicates a monitoring condition */
    Oid             handler_schema;     /* the schema that contains the event handler */
    Oid             handler_module;     /* the function name of the event handler */
    bool            audit_enable;       /* enables the policy if TRUE, which is the default */
    NameData        statement_types;    /* the SQL statement types to which this policy is applicable: INSERT, UPDATE, DELETE, or SELECT only */
    bool            audit_column_opts;  /* audited when the query references any column specified in the audit_column parameter or only when all such columns are referenced. 0: any column, 1: all column */
}FormData_audit_fga_conf;

typedef FormData_audit_fga_conf *Form_audit_fga_conf;

#define Natts_pg_audit_fga                    13
#define Natts_audit_fga_conf                Natts_pg_audit_fga

#define Anum_audit_fga_conf_auditor_id                1
#define Anum_audit_fga_conf_object_schema            2
#define Anum_audit_fga_conf_object_id                3
#define Anum_audit_fga_conf_policy_name                4
#define Anum_audit_fga_conf_audit_column_ids        5
#define Anum_audit_fga_conf_audit_columns            6
#define Anum_audit_fga_conf_audit_condition            7
#define Anum_audit_fga_conf_audit_condition_str        8
#define Anum_audit_fga_conf_handler_schema            9
#define Anum_audit_fga_conf_handler_module            10
#define Anum_audit_fga_conf_audit_enable            11
#define Anum_audit_fga_conf_statement_types            12
#define Anum_audit_fga_conf_audit_column_opts        13

#endif     /* PGXC_AUDIT_FGA_H */


