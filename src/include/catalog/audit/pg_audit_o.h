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
#ifndef PGXC_AUDIT_OBJ_H
#define PGXC_AUDIT_OBJ_H

#include "catalog/genbki.h"

#define PgAuditObjConfRelationId  5018

CATALOG(pg_audit_obj_conf,5018)
{
    Oid            auditor_id;            /* who write this conf */
    Oid            class_id;            /* classId of ObjectAddress */
    Oid            object_id;            /* objectId of ObjectAddress */
    int32        object_sub_id;        /* objectSubId of ObjectAddress */
    int32        action_id;            /* which action to be audited */   
    char        action_mode;        /* when to audit: successful, not successful or all */     
    bool        action_ison;        /* turn on or power off this audit configure */
}FormData_audit_obj_conf;

typedef FormData_audit_obj_conf *Form_audit_obj_conf;

#define Natts_pg_audit_o                    7
#define Natts_audit_obj_conf                Natts_pg_audit_o

#define Anum_audit_obj_conf_auditor_id        1
#define Anum_audit_obj_conf_class_id        2
#define Anum_audit_obj_conf_object_id        3
#define Anum_audit_obj_conf_object_sub_id    4
#define Anum_audit_obj_conf_action_id        5
#define Anum_audit_obj_conf_action_mode        6
#define Anum_audit_obj_conf_action_ison        7

/* DATA(insert OID = 5105 ( 10         1259    11610     3    1    a    t)); */
/* DATA(insert OID = 5106 ( 3373     1259    11610    0    1    f     f)); */
/* DATA(insert OID = 5107 ( 3373     1259    11632    2    3    f     f)); */
/* DATA(insert OID = 5108 ( 3374     1247    114        0    3     s     t)); */
/* DATA(insert OID = 5109 ( 3375     1247    3361    0    4     n     f)); */
/* DATA(insert OID = 5110 ( 3377     1255    1242    0    5     n     t)); */
/* DATA(insert OID = 5111 ( 4200     2617    95        0    6     n     f)); */

#endif     /* PGXC_AUDIT_OBJ_H */


