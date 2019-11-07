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
#ifndef __PGXC_AUDIT__H
#define __PGXC_AUDIT__H

#include "nodes/nodes.h"
#include "nodes/parsenodes.h"

extern bool enable_audit;
extern bool enable_audit_warning;
extern bool enable_audit_depend;

typedef enum AuditSQL
{
    AuditSql_All                = 0,        /* AUDIT ALL [ {BY xxx} | {ON xxx} ]*/

    AuditSql_ShortcutBegin        = 1,        /* SQL Statement Shortcuts for Auditing    */
                                            /* Used for AUDIT xxx [BY xxx] */
    AuditSql_AlterSystem,

    AuditSql_Database,
    AuditSql_Database_Alter,
    AuditSql_Database_Create,
    AuditSql_Database_Drop,

    AuditSql_Extension            = 100,
    AuditSql_Extension_Alter,
    AuditSql_Extension_Create,
    AuditSql_Extension_Drop,

    AuditSql_Function            = 200,
    AuditSql_Function_Alter,
    AuditSql_Function_Create,
    AuditSql_Function_Drop,

    AuditSql_Group                = 300,
    AuditSql_Group_CreateNodeGroup,
    AuditSql_Group_CreateShardingGroup,
    AuditSql_Group_DropNodeGroup,
    AuditSql_Group_DropShardingInGroup,

    AuditSql_Index                = 400,
    AuditSql_Index_Alter,
    AuditSql_Index_Create,
    AuditSql_Index_Drop,

    AuditSql_MaterializedView    = 500,
    AuditSql_MaterializedView_Alter,
    AuditSql_MaterializedView_Create,
    AuditSql_MaterializedView_Drop,

    AuditSql_Node                = 600,
    AuditSql_Node_Alter,
    AuditSql_Node_Create,
    AuditSql_Node_Drop,

    AuditSql_Partition            = 700,
    AuditSql_Partition_AddPartitions,

    AuditSql_Role                = 800,
    AuditSql_Role_Alter,
    AuditSql_Role_Create,
    AuditSql_Role_Drop,
    AuditSql_Role_Set,

    AuditSql_Schema,
    AuditSql_Schema_Alter,
    AuditSql_Schema_Create,
    AuditSql_Schema_Drop,

    AuditSql_Sequence            = 900,
    AuditSql_Sequence_Create,
    AuditSql_Sequence_Drop,

    AuditSql_Table                = 1000,
    AuditSql_Table_Create,
    AuditSql_Table_Drop,
    AuditSql_Table_Truncate,

    AuditSql_Tablespace            = 1100,
    AuditSql_Tablespace_Alter,
    AuditSql_Tablespace_Create,
    AuditSql_Tablespace_Drop,

    AuditSql_Trigger            = 1200,
    AuditSql_Trigger_Alter,
    AuditSql_Trigger_Create,
    AuditSql_Trigger_Drop,
    AuditSql_Trigger_Disable,
    AuditSql_Trigger_Enable,

    AuditSql_Type                = 1300,
    AuditSql_Type_Alter,
    AuditSql_Type_Create,
    AuditSql_Type_Drop,

    AuditSql_User                = 1400,
    AuditSql_User_Alter,
    AuditSql_User_Create,
    AuditSql_User_Drop,

    AuditSql_View                = 1500,
    AuditSql_View_Alter,
    AuditSql_View_Create,
    AuditSql_View_Drop,

    AuditSql_ShortcutEnd         = 30000,    /* SQL Statement Shortcuts for Auditing    */

    AuditSql_AdditionalBegin     = 30001,    /* Additional SQL Statement Shortcuts for Auditing */
                                            /* Used for AUDIT xxx [BY xxx] */

    AuditSql_AlterSequence,                    /* ALTER ON SEQUENCE */
    AuditSql_AlterTable,                    /* ALTER ON TABLE */
    AuditSql_CommentTable,                    /* COMMENT ON TABLE[.COLUMN] */
                                            /* COMMENT ON VIEW[.COLUMN] */
                                            /* COMMENT ON MATERIALIZED VIEW[.COLUMN] */
    AuditSql_DeleteTable,                    /* DELETE FROM TABLE */
                                            /* DELETE FROM VIEW */
    AuditSql_GrantFunction,                    /* GRANT ON FUNCTION */
                                            /* REOVKE ON FUNCTION */
    AuditSql_GrantSequence,                    /* GRANT ON SEQUENCE */
                                            /* REVOKE ON SEQUENCE */
    AuditSql_GrantTable,                    /* GRANT ON TABLE/VIEW/MVIEW */
                                            /* REVOKE ON TABLE/VIEW/MVIEW */
    AuditSql_GrantType,                        /* GRANT ON TYPE */
                                            /* REVOKE ON TYPE */
    AuditSql_InsertTable,                    /* INSERT INTO TABLE */
                                            /* INSERT INTO VIEW */
    AuditSql_LockTable,                        /* LOCK TABLE */
    AuditSql_SelectSequence,                /* SELECT SEQUENCE.nextval */
                                            /* SELECT SEQUENCE.currval */
    AuditSql_SelectTable,                    /* SELECT FROM table */
                                            /* SELECT FROM view */
                                            /* SELECT FROM materialized view */
    AuditSql_SystemAudit,                    /* AUDIT */
                                            /* NOAUDIT */
    AuditSql_SystemGrant,                    /* GRANT */
                                            /* REVOKE */
    AuditSql_UpdateTable,                    /* UPDATE TABLE */
                                            /* UPDATE VIEW */

    AuditSql_AdditionalEnd        = 60000,    /* Additional SQL Statement Shortcuts for Auditing */

    AuditSql_SchemaObjectBegin    = 60001,    /* Schema Object Auditing Options */
                                            /* Used for AUDIT xxx ON xxx */

    AuditSql_Alter,                            /* ALTER TABLE */
                                            /* ALTER VIEW */
                                            /* ALTER MATERIALIZED VIEW */
                                            /* ALTER SEQUENCE */
    AuditSql_Audit,                            /* AUDIT ON TABLE */
                                            /* NOAUDIT ON TABLE */
                                            /* AUDIT ON VIEW */
                                            /* NOAUDIT ON VIEW */
                                            /* AUDIT ON MATERIALIZED VIEW */
                                            /* NOAUDIT ON MATERIALIZED VIEW */
                                            /* AUDIT ON SEQUENCE */
                                            /* NOAUDIT ON SEQUENCE */
                                            /* AUDIT ON FUNCTION */
                                            /* NOAUDIT ON FUNCTION */
    AuditSql_Comment,                        /* COMMENT ON TABLE */
                                            /* COMMENT ON VIEW */
                                            /* COMMENT ON MATERIALIZED VIEW */
    AuditSql_Delete,                        /* DELETE FROM TABLE */
                                            /* DELETE FROM VIEW */
    AuditSql_Grant,                            /* GRANT ON TABLE */
                                            /* REVOKE ON TABLE */
                                            /* GRANT ON VIEW */
                                            /* REVOKE ON VIEW */
                                            /* GRANT ON MATERIALIZED VIEW */
                                            /* REVOKE ON MATERIALIZED VIEW */
                                            /* GRANT ON SEQUENCE */
                                            /* REVOKE ON SEQUENCE */
                                            /* GRANT ON FUNCTION */
                                            /* REVOKE ON FUNCTION */
    /* AuditSql_Index, */                    /* Audit/NoAudit Index On Table/MView is not supported in current version */
                                            /* CREATE INDEX ON TABLE */
                                            /* DROP INDEX ON TABLE */
                                            /* ALTER INDEX ON TABLE */
                                            /* CREATE INDEX ON MATERIALIZED VIEW */
                                            /* DROP INDEX ON MATERIALIZED VIEW */
                                            /* ALTER INDEX ON MATERIALIZED VIEW */
    AuditSql_Insert,                        /* INSERT INTO TABLE */
                                            /* INSERT INTO VIEW */
    AuditSql_Lock,                            /* LOCK ON TABLE */
    AuditSql_Rename,                        /* RENAME ON TABLE */
                                            /* RENAME ON VIEW */
                                            /* RENAME ON MATERIALIZED VIEW */
    AuditSql_Select,                        /* SELECT FROM TABLE */
                                            /* SELECT FROM VIEW */
                                            /* SELECT FROM MATERIALIZED VIEW */
                                            /* SELECT ON SEQUENCE */
    AuditSql_Update,                        /* UPDATE TABLE */
                                            /* UPDATE VIEW */
                                            /* UPDATE MATERIALIZED VIEW */

    AuditSql_SchemaObjectEnd    = 90000,    /* Schema Object Auditing Options */

    AuditSQL_Ivalid                = 99999,    /* Ivalid AuditSQL */
} AuditSQL;

extern void AuditInitEnv(void);
extern void AuditDefine(AuditStmt * stmt, const char * queryString, char ** auditString);
extern void AuditClean(CleanAuditStmt * stmt, const char * queryString, char ** cleanString);
extern void AuditClearResultInfo(void);
extern void AuditReadQueryList(const char * query_sring, List * l_parsetree);
extern void AuditProcessResultInfo(bool is_success);
extern void AuditCheckPerms(Oid table_oid, Oid roleid, AclMode mask);

extern void RemoveStmtAuditById(Oid audit_id);
extern void RemoveUserAuditById(Oid audit_id);
extern void RemoveObjectAuditById(Oid audit_id);
extern void RemoveObjectDefaultAuditById(Oid audit_id);

extern char * get_stmt_audit_desc(Oid audit_id, bool just_identity);
extern char * get_user_audit_desc(Oid audit_id, bool just_identity);
extern char * get_obj_audit_desc(Oid audit_id, bool just_identity);
extern char * get_obj_default_audit_desc(Oid audit_id, bool just_identity);

extern bool audituser(void);
extern bool audituser_arg(Oid roleid);

#endif

