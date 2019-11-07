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
 * pg_authid.h
 *      definition of the system "authorization identifier" relation (pg_authid)
 *      along with the relation's initial contents.
 *
 *      pg_shadow and pg_group are now publicly accessible views on pg_authid.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_authid.h
 *
 * NOTES
 *      the genbki.pl script reads this file and generates .bki
 *      information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_AUTHID_H
#define PG_AUTHID_H

#include "catalog/genbki.h"

/*
 * The CATALOG definition has to refer to the type of rolvaliduntil as
 * "timestamptz" (lower case) so that bootstrap mode recognizes it.  But
 * the C header files define this type as TimestampTz.  Since the field is
 * potentially-null and therefore can't be accessed directly from C code,
 * there is no particular need for the C struct definition to show the
 * field type as TimestampTz --- instead we just make it int.
 */
#define timestamptz int


/* ----------------
 *        pg_authid definition.  cpp turns this into
 *        typedef struct FormData_pg_authid
 * ----------------
 */
#define AuthIdRelationId    1260
#define AuthIdRelation_Rowtype_Id    2842

CATALOG(pg_authid,1260) BKI_SHARED_RELATION BKI_ROWTYPE_OID(2842) BKI_SCHEMA_MACRO
{
    NameData    rolname;        /* name of role */
    bool        rolsuper;        /* read this field via superuser() only! */
    bool        rolinherit;        /* inherit privileges from other roles? */
    bool        rolcreaterole;    /* allowed to create more roles? */
    bool        rolcreatedb;    /* allowed to create databases? */
    bool        rolcanlogin;    /* allowed to log in as session user? */
    bool        rolreplication; /* role used for streaming replication */
    bool        rolbypassrls;    /* bypasses row level security? */
    int32        rolconnlimit;    /* max connections allowed (-1=no limit) */

    /* remaining fields may be null; use heap_getattr to read them! */
#ifdef CATALOG_VARLEN            /* variable-length fields start here */
    text        rolpassword;    /* password, if any */
    timestamptz rolvaliduntil;    /* password expiration time, if any */
#endif
} FormData_pg_authid;

#undef timestamptz


/* ----------------
 *        Form_pg_authid corresponds to a pointer to a tuple with
 *        the format of pg_authid relation.
 * ----------------
 */
typedef FormData_pg_authid *Form_pg_authid;

/* ----------------
 *        compiler constants for pg_authid
 * ----------------
 */
#define Natts_pg_authid                    11
#define Anum_pg_authid_rolname            1
#define Anum_pg_authid_rolsuper            2
#define Anum_pg_authid_rolinherit        3
#define Anum_pg_authid_rolcreaterole    4
#define Anum_pg_authid_rolcreatedb        5
#define Anum_pg_authid_rolcanlogin        6
#define Anum_pg_authid_rolreplication    7
#define Anum_pg_authid_rolbypassrls        8
#define Anum_pg_authid_rolconnlimit        9
#define Anum_pg_authid_rolpassword        10
#define Anum_pg_authid_rolvaliduntil    11

/* ----------------
 *        initial contents of pg_authid
 *
 * The uppercase quantities will be replaced at initdb time with
 * user choices.
 *
 * The C code typically refers to these roles using the #define symbols,
 * so be sure to keep those in sync with the DATA lines.
 * ----------------
 */
DATA(insert OID = 10 ( "POSTGRES" t t t t t t t -1 _null_ _null_));
#define BOOTSTRAP_SUPERUSERID            10
DATA(insert OID = 3373 ( "pg_monitor" f t f f f f f -1 _null_ _null_));
#define DEFAULT_ROLE_MONITOR        3373
DATA(insert OID = 3374 ( "pg_read_all_settings" f t f f f f f -1 _null_ _null_));
#define DEFAULT_ROLE_READ_ALL_SETTINGS    3374
DATA(insert OID = 3375 ( "pg_read_all_stats" f t f f f f f -1 _null_ _null_));
#define DEFAULT_ROLE_READ_ALL_STATS 3375
DATA(insert OID = 3377 ( "pg_stat_scan_tables" f t f f f f f -1 _null_ _null_));
#define DEFAULT_ROLE_STAT_SCAN_TABLES    3377
DATA(insert OID = 4200 ( "pg_signal_backend" f t f f f f f -1 _null_ _null_));
#define DEFAULT_ROLE_SIGNAL_BACKENDID    4200
#ifdef _MLS_
DATA(insert OID = 4565 ( "mls_admin" f f t f t f f -1 md5c9952291f36f276ac60ab76218973bbd _null_));
#define DEFAULT_ROLE_MLS_SYS_USERID 4565
DATA(insert OID = 6116 ( "audit_admin" f f f f t f f -1 md535964be9ee19e31738175cf766ff7ca4 _null_));
#define DEFAULT_ROLE_AUDIT_SYS_USERID 6116

#define MLS_USER_DEFAULT_PASSWD     "SecurityAdmin@TBasev2"
#define AUDIT_USER_DEFAULT_PASSWD   "AuditAdmin@TBasev2"

#define MLS_USER        "mls_admin"
#define AUDIT_USER      "audit_admin"
#define MLS_CONN_OPTION "-c inner_conn=no"

#define ROLE_NORMAL_USER    'n'
#define ROLE_MLS_USER       's'
#define ROLE_AUDIT_USER     'a'
#endif

#endif                            /* PG_AUTHID_H */
