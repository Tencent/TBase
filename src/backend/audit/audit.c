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
#include <sys/types.h>
#include <unistd.h>

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "port.h"

#include "audit/audit.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/skey.h"
#include "access/sysattr.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/objectaddress.h"
#include "catalog/pgxc_node.h"
#include "catalog/pgxc_group.h"
#include "catalog/pgxc_shard_map.h"
#include "catalog/pg_audit.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_class.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_database.h"
#include "catalog/pg_event_trigger.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_language.h"
#include "catalog/pg_largeobject.h"
#include "catalog/pg_largeobject_metadata.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_policy.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_rewrite.h"
#include "catalog/pg_subscription.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_type.h"
#include "common/username.h"
#include "commands/dbcommands.h"
#include "commands/event_trigger.h"
#include "commands/extension.h"
#include "commands/policy.h"
#include "commands/proclang.h"
#include "commands/tablespace.h"
#include "executor/spi.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "libpq/libpq-be.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_func.h"
#include "parser/parse_type.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "postmaster/auditlogger.h"
#include "postmaster/postmaster.h"
#include "storage/lockdefs.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/formatting.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/rel.h"
#include "utils/syscache.h"

bool enable_audit = false;
bool enable_audit_warning = false;
bool enable_audit_depend = false;

static bool use_object_missok = true;

// #define Use_Audit_Assert 0

#define Audit_001_For_Input         0        /* audit first part for Audit Input */
#define Audit_002_For_Clean         0        /* audit second part for Clean Audit */
#define Audit_003_For_Hit             0        /* audit third part for Audit Hit */
#define Audit_004_For_Log             0        /* audit forth part for Audit Log */

#define Audit_MaxStmtPerShortcut    64        /* max number of AuditSQL that belongs to one shortcut sql
                                             *
                                             * Audit Alter Type, Audit Create Type and Audit Drop type 
                                             * are all belong to Audit Type
                                             */

#define Audit_MaxObjectTypePerStmt     64        /* max number of object type that one AuditSQL can record
                                             *
                                             * Audit Rename can used on table, view,
                                             * or materialized view and all their columns.
                                             */
                                            
#define Audit_MaxNodeTagPerStmt        64        /* max number of stmt that one AuditSQL can record 
                                             *
                                             * Audit Index, not only audit Create Index, but also
                                             * audit Drop Index, Alter Index
                                             *
                                             * Audit Grand, not only audit Grant, but alst audit Revoke.
                                             */

#define Audit_InvalidEnum             -99999    /* put into the end of AuditStmtMap.sub_id/.obj_type/.stmt_tag */

#define Audit_Object_Default        InvalidOid            /* Object ID used when Audit xxx ON DEFAULT */

#define InvalidSysCacheID             (SysCacheSize + SysCacheSize)    /* Ivalid ID for SysCacheIdentifier */

#define Audit_nSysScanKeys            8

typedef enum AuditModeMius
{
    AuditModeMi_All            = AuditMode_All,            /* just a place holder*/
    AuditModeMi_Success        = AuditMode_Success,        /* AuditMode_All - AuditMode_Fail = AuditModeMi_Success */
    AuditModeMi_Fail        = AuditMode_Fail,            /* AuditModeMi_All - AuditMode_Success = AuditModeMi_Fail */
    AuditModeMi_Mutex        = 'm',                        /* AuditModeMi_Success - AuditMode_Fail = AuditModeMi_Mutex 
                                                         * AuditMode_Fail - AuditModeMi_Success = AuditModeMi_Mutex
                                                         */
    AuditModeMi_Overtop        = 'o',                        /* xxx - AuditMode_All = AuditModeMi_Overtop */
    AuditModeMi_Equal        = 'e',                        /* if (ModeA == ModeB) then ModeA - ModeB = AuditModeMi_Equal */
    AuditModeMi_Ivalid        = 'i',                        /* All others are Ivalid AuditMode minus operation */
} AuditModeMi;

typedef struct AuditStmtMap
{
    AuditSQL    id;                                        /* refer to AuditSQL */                        
    char *        name;                                    /* Create Table, Drop Index defined in gram.y */
    AuditSQL    sub_id[Audit_MaxStmtPerShortcut];        /* refer to AuditSQL */
    ObjectType    obj_type[Audit_MaxObjectTypePerStmt];    /* see OBJECT_VIEW, OBJECT_INDEX in parsenodes.h */
    NodeTag        stmt_tag[Audit_MaxNodeTagPerStmt];        /* see definitions in NodeTag: T_CommentStmt, T_SelectStmt */
} AuditStmtMap;

typedef struct AuditStmtContext
{
    StringInfo         action_str;                            /* transform AuditStmt to SQL text string */
    List             *l_action_items;                    /* action items expanded from AuditStmt->action_list */
    List            *l_user_oids;                        /* user ids expanded from AuditStmt->user_list */
    ObjectAddress    object_address;                        /* object address of AuditStmt->object_name */
} AuditStmtContext;

typedef struct AuditStmtRecord
{
    AuditStmtMap    *action_item;                        /* action items expanded from AuditStmt->action_list */
    ObjectAddress    obj_address;                        /* if this record is type of AuditType_Object,
                                                         *         then refer to object address of AuditStmt->object_name 
                                                         * if this recore is type of AuditType_User
                                                         *        then refer to user address of AuditStmt->user_list
                                                         * else refer to InvalidObjectAddress
                                                         */
} AuditStmtRecord;

typedef enum SysCacheIdentifier SysCacheIdentifier;
typedef struct AuditRecordWriter AuditRecordWriter;
typedef struct AuditRecordWriter
{
    Oid                    sysrel_oid;                        /* where to store those audit records,
                                                         * PgAuditStmtConfRelationId: Audit xxx
                                                         * PgAuditUserConfRelationId: Audit xxx By xxx
                                                         * PgAuditObjConfRelationId: Audit xxx ON xxx
                                                         * PgAuditObjDefOptsRelationId: Audit xxx ON DEFAULT
                                                         */
    SysCacheIdentifier    syscache_id;                    /* SysCache id for sysrel_oid,
                                                         * PgAuditStmtConfRelationId: AUDITSTMTCONF
                                                         * PgAuditUserConfRelationId: AUDITUSERCONF
                                                         * PgAuditObjConfRelationId: AUDITOBJCONF
                                                         * PgAuditObjDefOptsRelationId: AUDITOBJDEFAULT
                                                         */
    void                 (*writer_fn)(AuditStmt *,
                                     AuditStmtContext *,
                                      AuditRecordWriter *,
                                     AuditStmtRecord *,
                                     Relation);            /* which function to insert or update this a record
                                                         * into sysrel,
                                                         * PgAuditStmtConfRelationId: audit_write_sysrel_stmt_conf
                                                         * PgAuditUserConfRelationId: audit_write_sysrel_user_conf
                                                         * PgAuditObjConfRelationId: audit_write_sysrel_obj_conf
                                                         * PgAuditObjDefOptsRelationId: audit_write_sysrel_obj_def_opts
                                                         */
} AuditRecordWriter;

typedef struct CleanAuditStmtContext
{
    StringInfo         clean_str;                            /* transform CleanAuditStmt to SQL text string */
    List            *l_user_oids;                        /* user ids expanded from CleanAuditStmt->user_list */
    ObjectAddress    object_address;                        /* object address of CleanAuditStmt->object_name */
} CleanAuditStmtContext;

typedef struct AuditHitInfo
{
    NodeTag            stmt_tag;                            /* statement tag read from Query */
    ObjectAddress    obj_addr;                            /* object address read from Query */
    ObjectType        obj_type;                            /* object type read from Query */
    char            *obj_name;                            /* name of obj read from Query */
    List            *l_hit_index;                        /* index found in gAuditStmtActionMap by stmt_type and obj_type */ 
    List            *l_hit_action;                        /* action items found in gAuditStmtActionMap by stmt_type and obj_type */
    List            *l_hit_match;                        /* is this hit match AuditConf in pg catalog tables */
    List            *l_hit_audit;                        /* Object Audit/Object Default Audit/User Audit/Statement Audit */
    bool            is_success;                            /* this sql exec successfull ?*/
} AuditHitInfo;

typedef struct AuditResultInfo
{
    char            *qry_string;                        /* query_string from exec_simple_query */
    char            *cmd_tag;                            /* tag of query created by CreateCommandTag */
    List            *l_hit_info;                        /* list of AuditHitInfo read from Query after pg_parse_query */
    Oid                db_id;                                /* where to execute this query */
    char            *db_name;                            /* where to execute this query */
    Oid                db_user_id;                            /* who execute this query */
    char            *db_user_name;                        /* who execute this query */
    char            *node_name;                            /* node name */
    Oid                node_oid;                            /* node oid in pgxc_node */
    char            node_type;                            /* coord or datanode ? */
    char            *node_osuser;                        /* current os user name */
    char            *node_host;                            /* node host address */
    int                node_port;                            /* node port */
    pid_t            proc_pid;                            /* postgres pid */
    pid_t            proc_ppid;                            /* postmaster pid */
    pg_time_t        proc_start_time;                    /* postgres start time */
    pg_time_t         qry_begin_time;                        /* gegin time of QueryDesc */
    pg_time_t         qry_end_time;                        /* end time of QueryDesc*/
    bool            is_success;                            /* is this xact success ? */
    char            *error_msg;                            /* why this xact failed ? */
    char            *client_host;                        /* client ip of conn */
    char            *client_hostname;                    /* client ip of conn */
    char            *client_port;                        /* client port of conn */
    char            *app_name;                            /* client app name */
} AuditResultInfo;

typedef struct AuditHitFindObjectContext
{
    NodeTag        stmt_tag;
    List *         l_hit_info;
} AuditHitFindObjectContext;

static AuditResultInfo * gAuditResultInfo = NULL;

#ifdef Use_Audit_Assert
    #ifdef Trap
        #undef Trap
        #define Trap(condition, errorType) \
            do { \
                if (condition) \
                    ExceptionalCondition(CppAsString(condition), (errorType), \
                                         __FILE__, __LINE__); \
            } while (0)
    #endif

    #ifdef Assert
        #undef Assert
        #define Assert(condition) \
            Trap(!(condition), "FailedAssertion")
    #endif
#endif

#ifdef Audit_001_For_Input

static bool isa_valid_enum(int32 input);

static bool isa_shortcut_audit_stmt(AuditStmtMap * item);

static bool isa_statement_action_id(AuditSQL action_id);
static bool isa_user_action_id(AuditSQL action_id);
static bool isa_object_action_id(AuditSQL action_id);

static bool isa_statement_audit_type(AuditType type);
static bool isa_user_audit_type(AuditType type);
static bool isa_object_audit_type(AuditType type);

static bool is_audit_environment(void);
static bool is_audit_enable(void);

static void audit_get_cacheid_pg_audit_o(int32 * rel_cacheid, 
                                           int32 * oid_cacheid);
static void audit_get_cacheid_pg_audit_d(int32 * rel_cacheid, 
                                           int32 * oid_cacheid);
static void audit_get_cacheid_pg_audit_u(int32 * rel_cacheid, 
                                           int32 * oid_cacheid);
static void audit_get_cacheid_pg_audit_s(int32 * rel_cacheid, 
                                           int32 * oid_cacheid);

static char * audit_get_type_string(AuditType type);
static char * audit_get_mode_string(AuditMode mode);

static Size audit_get_stmt_map_size(void);
static AuditStmtMap * audit_get_stmt_map_by_action(AuditSQL action_id);
static AuditStmtMap * audit_get_stmt_map_by_index(int32 idx);

static void audit_build_idx_for_stmt_action_map(void);

static bool audit_check_action_type(AuditType audit_type, 
                                    AuditSQL action_id);

static void audit_expand_AuditSql_All(AuditStmt * stmt,
                                      AuditStmtContext * ctxt);
static bool audit_check_relkind(Relation relation,
                                List * object_name,
                                ObjectType object_type);
static void audit_check_connection(void);
static void audit_free_stmt_context(AuditStmtContext * ctxt);
static void audit_clean_free_stmt_context(CleanAuditStmtContext * ctxt);
static AuditModeMi audit_mode_minus(AuditMode first,
                                    AuditMode second);

static void audit_append_unique_action_item(AuditStmtContext * ctxt,
                                            AuditStmtMap * action_item);
static void audit_process_action_list(AuditStmt * stmt,
                                      AuditStmtContext * ctxt);
static void audit_process_user_list_internal(List * user_list,
                                             List ** l_user_oids);
static void audit_process_user_list(AuditStmt * stmt,
                                    AuditStmtContext * ctxt);
static void audit_clean_process_user_list(CleanAuditStmt * stmt,
                                          CleanAuditStmtContext * ctxt);
static void audit_process_object_internal(List * object_name, 
                                           ObjectType object_type,
                                           ObjectAddress * object_address);
static void audit_process_object(AuditStmt * stmt, 
                                 AuditStmtContext * ctxt);
static void audit_clean_process_object(CleanAuditStmt * stmt, 
                                        CleanAuditStmtContext * ctxt);
static char * audit_object_type_string(ObjectType object_type, bool report_error);
static void audit_final_to_string(AuditStmt * stmt, 
                                  AuditStmtContext * ctxt);
static void audit_generate_one_record(AuditStmt * stmt,
                                      AuditStmtContext * ctxt,
                                      AuditStmtMap * item,
                                      Oid user_id,
                                      AuditStmtRecord ** record);
static void audit_generate_record_list(AuditStmt * stmt,
                                       AuditStmtContext * ctxt,
                                       List ** lrecord);
static void audit_write_sysrel_stmt_conf(AuditStmt * stmt,
                                         AuditStmtContext * ctxt,
                                         AuditRecordWriter * writer,
                                         AuditStmtRecord * record,
                                         Relation pg_sysrel);
static void audit_write_sysrel_user_conf(AuditStmt * stmt,
                                         AuditStmtContext * ctxt,
                                         AuditRecordWriter * writer,
                                         AuditStmtRecord * record,
                                         Relation pg_sysrel);
static void audit_write_sysrel_obj_conf(AuditStmt * stmt,
                                        AuditStmtContext * ctxt,
                                        AuditRecordWriter * writer,
                                        AuditStmtRecord * record,
                                        Relation pg_sysrel);
static void audit_write_sysrel_obj_def_opts(AuditStmt * stmt,
                                            AuditStmtContext * ctxt,
                                            AuditRecordWriter * writer,
                                            AuditStmtRecord * record,
                                            Relation pg_sysrel);
static void audit_get_record_writer(AuditStmt * stmt,
                                    AuditRecordWriter * writer);
static void audit_write_recod_list(AuditStmt * stmt,
                                   AuditStmtContext * ctxt,
                                   List * lrecord);
#endif

#ifdef Audit_002_For_Clean

static void audit_remove_tuple_by_oid(int32 cacheid,
                                      Oid audit_id);
static void audit_clean_for_statement(CleanAuditStmt * stmt, 
                                        CleanAuditStmtContext * ctxt);
static void audit_clean_for_user_by_oids(List * l_user_oids);
static void audit_clean_for_user(CleanAuditStmt * stmt, 
                                 CleanAuditStmtContext * ctxt);
static void audit_clean_for_object(CleanAuditStmt * stmt, 
                                      CleanAuditStmtContext * ctxt);
static void audit_clean_for_object_default(CleanAuditStmt * stmt, 
                                                CleanAuditStmtContext * ctxt);
static void audit_clean_for_unknown(CleanAuditStmt * stmt, 
                                      CleanAuditStmtContext * ctxt);
static void audit_clean_process_delete(CleanAuditStmt * stmt, 
                                         CleanAuditStmtContext * ctxt);
#endif

#ifdef Audit_003_For_Hit

static AuditResultInfo * audit_hit_init_result_info(void);
static void audit_hit_free_result_info(AuditResultInfo * audit_ret);

static AuditResultInfo * audit_hit_get_result_info(void);
static void audit_hit_set_result_info(AuditResultInfo * audit_ret);

static void audit_hit_find_action_index(AuditHitInfo * hit_info);
static void audit_hit_free_hit_info(AuditHitInfo * hit_info);
static AuditHitInfo * audit_hit_make_hit_info(NodeTag stmt_tag,
                                              Oid class_id,
                                              Oid object_id,
                                              int32 object_sub_id,
                                              ObjectType obj_type,
                                              char * obj_name);
static AuditHitInfo * audit_hit_make_hit_info_2(AuditSQL id,
                                                NodeTag stmt_tag,
                                                Oid class_id,
                                                Oid object_id,
                                                int32 object_sub_id,
                                                ObjectType obj_type,
                                                char * obj_name);
static bool audit_hit_find_seq_walker(Node * node, void * context);
static List * audit_hit_find_object_info(Query * audit_query, 
                                         NodeTag stmt_tag);

static bool    audit_hit_object_missing_ok(bool src_missing_ok);
static List * audit_hit_read_query_utility(Query * audit_query);
static List * audit_hit_read_query_tree(Query * audit_query);
static void audit_hit_remove_dup_object(AuditResultInfo * audit_ret);
static void audit_hit_read_query_list(Port * port,
                                      const char * query_sring,
                                      List * l_parsetree);

static bool audit_hit_match_in_pg_audit_o(AuditHitInfo * audit_hit,
                                             AuditSQL action_id,
                                             AuditMode reverse_mode);
static bool audit_hit_match_in_pg_audit_d(AuditHitInfo * audit_hit,
                                             AuditSQL action_id,
                                             AuditMode reverse_mode);
static bool audit_hit_match_in_pg_audit_u(AuditHitInfo * audit_hit,
                                             AuditSQL action_id,
                                             AuditMode reverse_mode);
static bool audit_hit_match_in_pg_audit_s(AuditHitInfo * audit_hit,
                                             AuditSQL action_id,
                                             AuditMode reverse_mode);
static void audit_hit_rebuild_hit_info(AuditHitInfo * hit_info,
                                       int hit_index,
                                       AuditStmtMap * hit_action,
                                       int hit_match,
                                       char * hit_audit,
                                       bool is_success);
static void audit_hit_match_in_catalog(AuditHitInfo * audit_hit,
                                       bool is_success);
static void audit_hit_print_result_log(void);
static void audit_hit_process_result_info(bool is_success);

#endif

#ifdef Audit_004_For_Log

#endif

static AuditStmtMap gAuditStmtActionMap[] =
{
    {
        AuditSql_All,
        "ALL",
        {Audit_InvalidEnum},
        {Audit_InvalidEnum},
        {Audit_InvalidEnum},
    },

    /* SQL Statement Shortcuts for Auditing */
    {
        AuditSql_ShortcutBegin,
        "ShortcutBegin",
        {Audit_InvalidEnum},
        {Audit_InvalidEnum},
        {Audit_InvalidEnum},
    },
    {
        AuditSql_AlterSystem,
        "Alter System",
        {Audit_InvalidEnum},
        {Audit_InvalidEnum},
        {
            T_AlterSystemStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Database,
        "Database",
        {
            AuditSql_Database_Alter, 
            AuditSql_Database_Create,
            AuditSql_Database_Drop,
            Audit_InvalidEnum
        },
        {
            OBJECT_DATABASE,
            Audit_InvalidEnum
        },
        {Audit_InvalidEnum},
    },
    {
        AuditSql_Database_Alter,
        "Alter Database",
        {Audit_InvalidEnum},
        {
            OBJECT_DATABASE,
            Audit_InvalidEnum
        },
        {
            T_AlterDatabaseSetStmt,
            T_AlterDatabaseStmt,
            T_AlterOwnerStmt,
            T_RenameStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Database_Create,
        "Create Database",
        {Audit_InvalidEnum},
        {
            OBJECT_DATABASE,
            Audit_InvalidEnum
        },
        {
            T_CreatedbStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Database_Drop,
        "Drop Database",
        {Audit_InvalidEnum},
        {
            OBJECT_DATABASE,
            Audit_InvalidEnum
        },
        {
            T_DropdbStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Extension,
        "Extension",
        {
            AuditSql_Extension_Alter, 
            AuditSql_Extension_Create,
            AuditSql_Extension_Drop,
            Audit_InvalidEnum
        },
        {
            OBJECT_EXTENSION,
            Audit_InvalidEnum
        },
        {Audit_InvalidEnum},
    },
    {
        AuditSql_Extension_Alter,
        "Alter Extension",
        {Audit_InvalidEnum},
        {
            OBJECT_EXTENSION,
            Audit_InvalidEnum
        },
        {
            T_AlterExtensionContentsStmt,
            T_AlterExtensionStmt,
            T_AlterObjectSchemaStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Extension_Create,
        "Create Extension",
        {Audit_InvalidEnum},
        {
            OBJECT_EXTENSION,
            Audit_InvalidEnum
        },
        {
            T_CreateExtensionStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Extension_Drop,
        "Drop Extension",
        {Audit_InvalidEnum},
        {
            OBJECT_EXTENSION,
            Audit_InvalidEnum
        },
        {
            T_DropStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Function,
        "Function",
        {
            AuditSql_Function_Alter,
            AuditSql_Function_Create,
            AuditSql_Function_Drop,
            Audit_InvalidEnum
        },
        {
            OBJECT_FUNCTION,
            Audit_InvalidEnum
        },
        {Audit_InvalidEnum},
    },
    {
        AuditSql_Function_Alter,
        "Alter Function",
        {Audit_InvalidEnum},
        {
            OBJECT_FUNCTION,
            Audit_InvalidEnum
        },
        {
            T_AlterFunctionStmt,
            T_AlterObjectDependsStmt,
            T_AlterObjectSchemaStmt,
            T_AlterOwnerStmt,
            T_RenameStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Function_Create,
        "Create Function",
        {Audit_InvalidEnum},
        {
            OBJECT_FUNCTION,
            Audit_InvalidEnum
        },
        {
            T_CreateFunctionStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Function_Drop,
        "Drop Function",
        {Audit_InvalidEnum},
        {
            OBJECT_FUNCTION,
            Audit_InvalidEnum
        },
        {
            T_AlterExtensionContentsStmt,
            T_DropStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Group,
        "Group",
        {
            AuditSql_Group_CreateNodeGroup,
            AuditSql_Group_CreateShardingGroup,
            AuditSql_Group_DropNodeGroup,
            AuditSql_Group_DropShardingInGroup,
            Audit_InvalidEnum
        },
        {Audit_InvalidEnum},
        {Audit_InvalidEnum},
    },
    {
        AuditSql_Group_CreateNodeGroup,
        "Create Node Group",
        {Audit_InvalidEnum},
        {Audit_InvalidEnum},
        {
            T_CreateGroupStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Group_CreateShardingGroup,
        "Create Sharding Group",
        {Audit_InvalidEnum},
        {Audit_InvalidEnum},
        {
            T_CreateShardStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Group_DropNodeGroup,
        "Drop Node Group",
        {Audit_InvalidEnum},
        {Audit_InvalidEnum},
        {
            T_DropGroupStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Group_DropShardingInGroup,
        "Drop Sharding in Group",
        {Audit_InvalidEnum},
        {Audit_InvalidEnum},
        {
            T_DropShardStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Index,
        "Index",
        {
            AuditSql_Index_Alter,
            AuditSql_Index_Create,
            AuditSql_Index_Drop,
            Audit_InvalidEnum
        },
        {
            OBJECT_INDEX,
            Audit_InvalidEnum
        },
        {Audit_InvalidEnum},
    },
    {
        AuditSql_Index_Alter,
        "Alter Index",
        {Audit_InvalidEnum},
        {
            OBJECT_INDEX,
            Audit_InvalidEnum
        },
        {
            T_AlterTableStmt,
            T_AlterTableMoveAllStmt,
            T_RenameStmt,
            T_AlterObjectDependsStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Index_Create,
        "Create Index",
        {Audit_InvalidEnum},
        {
            OBJECT_INDEX,
            Audit_InvalidEnum
        },
        {
            T_IndexStmt,
            T_ReindexStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Index_Drop,
        "Drop Index",
        {Audit_InvalidEnum},
        {
            OBJECT_INDEX,
            Audit_InvalidEnum
        },
        {
            T_DropStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_MaterializedView,
        "Materialized View",
        {
            AuditSql_MaterializedView_Alter,
            AuditSql_MaterializedView_Create,
            AuditSql_MaterializedView_Drop,
            Audit_InvalidEnum
        },
        {
            OBJECT_MATVIEW,
            Audit_InvalidEnum
        },
        {Audit_InvalidEnum},
    },
    {
        AuditSql_MaterializedView_Alter,
        "Alter Materialized View",
        {Audit_InvalidEnum},
        {
            OBJECT_MATVIEW,
            Audit_InvalidEnum
        },
        {
            T_AlterTableStmt,
            T_AlterTableMoveAllStmt,
            T_RenameStmt,
            T_AlterObjectDependsStmt,
            T_AlterObjectSchemaStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_MaterializedView_Create,
        "Create Materialized View",
        {Audit_InvalidEnum},
        {
            OBJECT_MATVIEW,
            Audit_InvalidEnum
        },
        {
            T_CreateTableAsStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_MaterializedView_Drop,
        "Drop Materialized View",
        {Audit_InvalidEnum},
        {
            OBJECT_MATVIEW,
            Audit_InvalidEnum
        },
        {
            T_AlterExtensionContentsStmt,
            T_DropStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Node,
        "Node",
        {
            AuditSql_Node_Alter,
            AuditSql_Node_Create,
            AuditSql_Node_Drop,
            Audit_InvalidEnum
        },
        {Audit_InvalidEnum},
        {Audit_InvalidEnum},
    },
    {
        AuditSql_Node_Alter,
        "Alter Node",
        {Audit_InvalidEnum},
        {Audit_InvalidEnum},
        {
            T_AlterNodeStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Node_Create,
        "Create Node",
        {Audit_InvalidEnum},
        {Audit_InvalidEnum},
        {
            T_CreateNodeStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Node_Drop,
        "Drop Node",
        {Audit_InvalidEnum},
        {Audit_InvalidEnum},
        {
            T_DropNodeStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Partition,
        "Partition",
        {
            AuditSql_Partition_AddPartitions,
            Audit_InvalidEnum
        },
        {Audit_InvalidEnum},
        {Audit_InvalidEnum},
    },
    {
        AuditSql_Partition_AddPartitions,
        "Add Partitions",
        {Audit_InvalidEnum},
        {Audit_InvalidEnum},
        {
            T_AlterTableCmd,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Role,
        "Role",
        {
            AuditSql_Role_Alter,
            AuditSql_Role_Create,
            AuditSql_Role_Drop,
            AuditSql_Role_Set,
            Audit_InvalidEnum
        },
        {
            OBJECT_ROLE,            /* ROLESTMT_ROLE */
            Audit_InvalidEnum
        },
        {Audit_InvalidEnum},
    },
    {
        AuditSql_Role_Alter,
        "Alter Role",
        {Audit_InvalidEnum},
        {
            OBJECT_ROLE,            /* ROLESTMT_ROLE */
            Audit_InvalidEnum
        },
        {
            T_AlterRoleStmt,
            T_RenameStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Role_Create,
        "Create Role",
        {Audit_InvalidEnum},
        {
            OBJECT_ROLE,            /* ROLESTMT_ROLE */
            Audit_InvalidEnum
        },
        {
            T_CreateRoleStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Role_Drop,
        "Drop Role",
        {Audit_InvalidEnum},
        {
            OBJECT_ROLE,            /* ROLESTMT_ROLE */
            Audit_InvalidEnum
        },
        {
            T_DropRoleStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Role_Set,
        "Set Role",
        {Audit_InvalidEnum},
        {
            OBJECT_ROLE,            /* ROLESTMT_ROLE */
            Audit_InvalidEnum
        },
        {
            T_AlterRoleSetStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Schema,
        "Schema",
        {
            AuditSql_Schema_Alter, 
            AuditSql_Schema_Create,
            AuditSql_Schema_Drop,
            Audit_InvalidEnum
        },
        {
            OBJECT_SCHEMA,
            Audit_InvalidEnum
        },
        {Audit_InvalidEnum},
    },
    {
        AuditSql_Schema_Alter,
        "Alter Schema",
        {Audit_InvalidEnum},
        {
            OBJECT_SCHEMA,
            Audit_InvalidEnum
        },
        {
            T_AlterOwnerStmt,
            T_RenameStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Schema_Create,
        "Create Schema",
        {Audit_InvalidEnum},
        {
            OBJECT_SCHEMA,
            Audit_InvalidEnum
        },
        {
            T_CreateSchemaStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Schema_Drop,
        "Drop Schema",
        {Audit_InvalidEnum},
        {
            OBJECT_SCHEMA,
            Audit_InvalidEnum
        },
        {
            T_DropStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Sequence,
        "Sequence",
        {
            AuditSql_Sequence_Create,
            AuditSql_Sequence_Drop,
            Audit_InvalidEnum
        },
        {
            OBJECT_SEQUENCE,
            Audit_InvalidEnum
        },
        {Audit_InvalidEnum},
    },
    {
        AuditSql_Sequence_Create,
        "Create Sequence",
        {Audit_InvalidEnum},
        {
            OBJECT_SEQUENCE,
            Audit_InvalidEnum
        },
        {
            T_CreateSeqStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Sequence_Drop,
        "Drop Sequence",
        {Audit_InvalidEnum},
        {
            OBJECT_SEQUENCE,
            Audit_InvalidEnum
        },
        {
            T_AlterExtensionContentsStmt,
            T_DropStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Table,
        "Table",
        {
            AuditSql_Table_Create,
            AuditSql_Table_Drop,
            AuditSql_Table_Truncate,
            Audit_InvalidEnum
        },
        {
            OBJECT_TABLE,
            Audit_InvalidEnum
        },
        {Audit_InvalidEnum},
    },
    {
        AuditSql_Table_Create,
        "Create Table",
        {Audit_InvalidEnum},
        {
            OBJECT_TABLE,
            Audit_InvalidEnum
        },
        {
            T_CreateStmt,
            T_CreateTableAsStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Table_Drop,
        "Drop Table",
        {Audit_InvalidEnum},
        {
            OBJECT_TABLE,
            Audit_InvalidEnum
        },
        {
            T_AlterExtensionContentsStmt,
            T_DropStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Table_Truncate,
        "Truncate Table",
        {Audit_InvalidEnum},
        {
            OBJECT_TABLE,
            Audit_InvalidEnum
        },
        {
            T_TruncateStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Tablespace,
        "Tablespace",
        {
            AuditSql_Tablespace_Alter,
            AuditSql_Tablespace_Create,
            AuditSql_Tablespace_Drop,
            Audit_InvalidEnum
        },
        {
            OBJECT_TABLESPACE,
            Audit_InvalidEnum
        },
        {Audit_InvalidEnum},
    },
    {
        AuditSql_Tablespace_Alter,
        "Alter Tablespace",
        {Audit_InvalidEnum},
        {
            OBJECT_TABLESPACE,
            Audit_InvalidEnum
        },
        {
            T_AlterOwnerStmt,
            T_AlterTableSpaceOptionsStmt,
            T_RenameStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Tablespace_Create,
        "Create Tablespace",
        {Audit_InvalidEnum},
        {
            OBJECT_TABLESPACE,
            Audit_InvalidEnum
        },
        {
            T_CreateTableSpaceStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Tablespace_Drop,
        "Drop Tablespace",
        {Audit_InvalidEnum},
        {
            OBJECT_TABLESPACE,
            Audit_InvalidEnum
        },
        {
            T_DropTableSpaceStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Trigger,
        "Trigger",
        {
            AuditSql_Trigger_Alter,
            AuditSql_Trigger_Create,
            AuditSql_Trigger_Drop,
            AuditSql_Trigger_Disable,
            AuditSql_Trigger_Enable,
            Audit_InvalidEnum
        },
        {
            OBJECT_TRIGGER,
            Audit_InvalidEnum
        },
        {Audit_InvalidEnum},
    },
    {
        AuditSql_Trigger_Alter,
        "Alter Trigger",
        {Audit_InvalidEnum},
        {
            OBJECT_TRIGGER,
            Audit_InvalidEnum
        },
        {
            T_RenameStmt,
            T_AlterObjectDependsStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Trigger_Create,
        "Create Trigger",
        {Audit_InvalidEnum},
        {
            OBJECT_TRIGGER,
            Audit_InvalidEnum
        },
        {
            T_CreateTrigStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Trigger_Drop,
        "Drop Trigger",
        {Audit_InvalidEnum},
        {
            OBJECT_TRIGGER,
            Audit_InvalidEnum
        },
        {
            T_DropStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Trigger_Disable,
        "Disable Trigger",
        {Audit_InvalidEnum},
        {
            OBJECT_TRIGGER,
            Audit_InvalidEnum
        },
        {
            T_AlterTableCmd,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Trigger_Enable,
        "Enable Trigger",
        {Audit_InvalidEnum},
        {
            OBJECT_TRIGGER,
            Audit_InvalidEnum
        },
        {
            T_AlterTableCmd,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Type,
        "Type",
        {
            AuditSql_Type_Alter,
            AuditSql_Type_Create,
            AuditSql_Type_Drop,
            Audit_InvalidEnum
        },
        {
            OBJECT_TYPE,
            Audit_InvalidEnum
        },
        {Audit_InvalidEnum},
    },
    {
        AuditSql_Type_Alter,
        "Alter Type",
        {Audit_InvalidEnum},
        {
            OBJECT_TYPE,
            Audit_InvalidEnum
        },
        {
            T_AlterObjectSchemaStmt,
            T_AlterOwnerStmt,
            T_AlterTableStmt,
            T_RenameStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Type_Create,
        "Create Type",
        {Audit_InvalidEnum},
        {
            OBJECT_TYPE,
            Audit_InvalidEnum
        },
        {
            T_DefineStmt,
            T_CompositeTypeStmt,
            T_CreateEnumStmt,
            T_CreateRangeStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Type_Drop,
        "Drop Type",
        {Audit_InvalidEnum},
        {
            OBJECT_TYPE,
            Audit_InvalidEnum
        },
        {
            T_AlterExtensionContentsStmt,
            T_DropStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_User,
        "User",
        {
            AuditSql_User_Alter,
            AuditSql_User_Create,
            AuditSql_User_Drop,
            Audit_InvalidEnum
        },
        {
            OBJECT_ROLE,            /* ROLESTMT_USER */
            Audit_InvalidEnum
        },
        {Audit_InvalidEnum},
    },
    {
        AuditSql_User_Alter,
        "Alter User",
        {Audit_InvalidEnum},
        {
            OBJECT_ROLE,            /* ROLESTMT_USER */
            Audit_InvalidEnum
        },
        {
            T_AlterRoleSetStmt,
            T_AlterRoleStmt,
            T_RenameStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_User_Create,
        "Create User",
        {Audit_InvalidEnum},
        {
            OBJECT_ROLE,            /* ROLESTMT_USER */
            Audit_InvalidEnum
        },
        {
            T_CreateRoleStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_User_Drop,
        "Drop User",
        {Audit_InvalidEnum},
        {
            OBJECT_ROLE,            /* ROLESTMT_USER */
            Audit_InvalidEnum
        },
        {
            T_AlterRoleStmt,
            T_DropRoleStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_View,
        "View",
        {
            AuditSql_View_Alter,
            AuditSql_View_Create,
            AuditSql_View_Drop,
            Audit_InvalidEnum
        },
        {
            OBJECT_VIEW,
            Audit_InvalidEnum
        },
        {Audit_InvalidEnum},
    },
    {
        AuditSql_View_Alter,
        "Alter View",
        {Audit_InvalidEnum},
        {
            OBJECT_VIEW,
            Audit_InvalidEnum
        },
        {
            T_RenameStmt,
            T_AlterTableStmt,
            T_AlterObjectSchemaStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_View_Create,
        "Create View",
        {Audit_InvalidEnum},
        {
            OBJECT_VIEW,
            Audit_InvalidEnum
        },
        {
            T_ViewStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_View_Drop,
        "Drop View",
        {Audit_InvalidEnum},
        {
            OBJECT_VIEW,
            Audit_InvalidEnum
        },
        {
            T_AlterExtensionContentsStmt,
            T_DropStmt,
            Audit_InvalidEnum
        },
    },

    /* Add SQL Statement Shortcuts for Auditing    before AuditSql_ShortcutEnd */
    {
        AuditSql_ShortcutEnd,
        "ShortcutEnd",
        {Audit_InvalidEnum},
        {Audit_InvalidEnum},
        {Audit_InvalidEnum},
    },

    /* Additional SQL Statement Shortcuts for Auditing */
    {
        AuditSql_AdditionalBegin,
        "AdditionalBegin",
        {Audit_InvalidEnum},
        {Audit_InvalidEnum},
        {Audit_InvalidEnum},
    },
    {
        AuditSql_AlterSequence,
        "Alter Sequence",
        {Audit_InvalidEnum},
        {
            OBJECT_SEQUENCE,
            Audit_InvalidEnum
        },
        {
            T_AlterTableStmt,
            T_AlterSeqStmt,
            T_RenameStmt,
            T_AlterObjectSchemaStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_AlterTable,
        "Alter Table",
        {Audit_InvalidEnum},
        {
            OBJECT_TABLE,
            Audit_InvalidEnum
        },
        {
            T_AlterTableStmt,
            T_AlterTableMoveAllStmt,
            T_RenameStmt,
            T_AlterObjectSchemaStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_CommentTable,
        "Comment Table",
        {Audit_InvalidEnum},
        {
            OBJECT_TABLE,
            OBJECT_VIEW,
            OBJECT_MATVIEW,
            OBJECT_COLUMN,
            Audit_InvalidEnum
        },
        {
            T_CommentStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_DeleteTable,
        "Delete Table",
        {Audit_InvalidEnum},
        {
            OBJECT_TABLE,
            OBJECT_VIEW,            /* cannot change (perform delete on) materialized view */
            Audit_InvalidEnum
        },
        {
            T_DeleteStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_GrantFunction,
        "Grant Function",
        {Audit_InvalidEnum},
        {
            OBJECT_FUNCTION,        /* ACL_OBJECT_FUNCTION */
            Audit_InvalidEnum
        },
        {
            T_GrantStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_GrantSequence,
        "Grant Sequence",
        {Audit_InvalidEnum},
        {
            OBJECT_SEQUENCE,        /* ACL_OBJECT_SEQUENCE */
            Audit_InvalidEnum
        },
        {
            T_GrantStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_GrantTable,
        "Grant Table",
        {Audit_InvalidEnum},
        {
            OBJECT_TABLE,            /* ACL_OBJECT_RELATION */
            OBJECT_VIEW,
            OBJECT_MATVIEW,
            Audit_InvalidEnum
        },
        {
            T_GrantStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_GrantType,
        "Grant Type",
        {Audit_InvalidEnum},
        {
            OBJECT_TYPE,            /* ACL_OBJECT_TYPE */
            Audit_InvalidEnum
        },
        {
            T_GrantStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_InsertTable,
        "Insert Table",
        {Audit_InvalidEnum},
        {
            OBJECT_TABLE,
            OBJECT_VIEW,            /* cannot change (perform insert into) materialized view */            
            Audit_InvalidEnum
        },
        {
            T_InsertStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_LockTable,
        "Lock Table",
        {Audit_InvalidEnum},
        {
            OBJECT_TABLE,            /* can not lock on view and materialized view */            
            Audit_InvalidEnum
        },
        {
            T_LockStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_SelectSequence,
        "Select Sequence",
        {Audit_InvalidEnum},
        {
            OBJECT_SEQUENCE,
            Audit_InvalidEnum
        },
        {
            T_SelectStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_SelectTable,
        "Select Table",
        {Audit_InvalidEnum},
        {
            OBJECT_TABLE,
            OBJECT_VIEW,
            OBJECT_MATVIEW,
            Audit_InvalidEnum
        },
        {
            T_SelectStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_SystemAudit,
        "System Audit",
        {Audit_InvalidEnum},
        {Audit_InvalidEnum},
        {
            T_AuditStmt,
            T_CleanAuditStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_SystemGrant,
        "System Grant",
        {Audit_InvalidEnum},
        {Audit_InvalidEnum},
        {
            T_GrantStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_UpdateTable,
        "Update Table",
        {Audit_InvalidEnum},
        {
            OBJECT_TABLE,
            OBJECT_VIEW,            /* cannot change (perform update on) materialized view */
            Audit_InvalidEnum
        },
        {
            T_UpdateStmt,
            Audit_InvalidEnum
        },
    },

    /* Add Additional SQL Statement Shortcuts for Auditing before AuditSql_AdditionalEnd */
    {
        AuditSql_AdditionalEnd,
        "AdditionalEnd",
        {Audit_InvalidEnum},
        {Audit_InvalidEnum},
        {Audit_InvalidEnum},
    },

    /* Schema Object Auditing Options */
    {
        AuditSql_SchemaObjectBegin,
        "SchemaObjectBegin",
        {Audit_InvalidEnum},
        {Audit_InvalidEnum},
        {Audit_InvalidEnum},
    },
    {
        AuditSql_Alter,
        "Alter",
        {Audit_InvalidEnum},
        {
            OBJECT_TABLE,
            OBJECT_VIEW,
            OBJECT_MATVIEW,
            OBJECT_SEQUENCE,
            /* OBJECT_INDEX, */
            Audit_InvalidEnum
        },
        {
            T_AlterObjectDependsStmt,
            T_AlterObjectSchemaStmt,
            T_AlterSeqStmt,
            T_AlterTableStmt,
            T_AlterTableMoveAllStmt,
            T_RenameStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Audit,
        "Audit",
        {Audit_InvalidEnum},
        {
            OBJECT_TABLE,
            OBJECT_VIEW,
            OBJECT_MATVIEW,
            OBJECT_SEQUENCE,
            OBJECT_FUNCTION,
            Audit_InvalidEnum
        },
        {
            T_AuditStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Comment,
        "Comment",
        {Audit_InvalidEnum},
        {
            OBJECT_TABLE,
            OBJECT_VIEW,
            OBJECT_MATVIEW,
            OBJECT_TYPE,
            OBJECT_FUNCTION,
            Audit_InvalidEnum
        },
        {
            T_CommentStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Delete,
        "Delete",
        {Audit_InvalidEnum},
        {
            OBJECT_TABLE,
            OBJECT_VIEW,            /* cannot change (perform delete on) materialized view */
            Audit_InvalidEnum
        },
        {
            T_DeleteStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Grant,
        "Grant",
        {Audit_InvalidEnum},
        {
            OBJECT_TABLE,            /* ACL_OBJECT_RELATION */
            OBJECT_VIEW,            /* ACL_OBJECT_RELATION */
            OBJECT_MATVIEW,            /* ACL_OBJECT_RELATION */
            OBJECT_SEQUENCE,        /* ACL_OBJECT_SEQUENCE */
            OBJECT_FUNCTION,        /* ACL_OBJECT_FUNCTION */
            Audit_InvalidEnum
        },
        {
            T_GrantStmt,
            Audit_InvalidEnum
        },
    },
/*    {
 *        AuditSql_Index,                // Audit/NoAudit Index On Table/MView is not supported in current version
 *        "Index",
 *        {Audit_InvalidEnum},
 *        {
 *            OBJECT_TABLE,
 *            OBJECT_MATVIEW,
 *            Audit_InvalidEnum
 *        },
 *        {
 *            T_IndexStmt,            // Create Index on Table
 *            T_DropStmt,                // Drop Index
 *            Audit_InvalidEnum
 *        },
 *    },
 */ {
        AuditSql_Insert,
        "Insert",
        {Audit_InvalidEnum},
        {
            OBJECT_TABLE,
            OBJECT_VIEW,            /* can perform insert on materialized view */        
            Audit_InvalidEnum
        },
        {
            T_InsertStmt,
            Audit_InvalidEnum
        },
    },    
     {
        AuditSql_Lock,
        "Lock",
        {Audit_InvalidEnum},
        {
            OBJECT_TABLE,            /* can not lock on view and materialized view */
            Audit_InvalidEnum
        },
        {
            T_LockStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Rename,
        "Rename",
        {Audit_InvalidEnum},
        {
            /* OBJECT_INDEX, */
            OBJECT_SEQUENCE,
            OBJECT_TABLE,
            OBJECT_VIEW,
            OBJECT_MATVIEW,
            /* OBJECT_COLUMN, */
            OBJECT_TYPE,
            Audit_InvalidEnum
        },
        {
            T_RenameStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Select,
        "Select",
        {Audit_InvalidEnum},
        {
            OBJECT_TABLE,
            OBJECT_VIEW,
            OBJECT_MATVIEW,
            OBJECT_SEQUENCE,
            Audit_InvalidEnum
        },
        {
            T_SelectStmt,
            Audit_InvalidEnum
        },
    },
    {
        AuditSql_Update,
        "Update",
        {Audit_InvalidEnum},
        {
            OBJECT_TABLE,
            OBJECT_VIEW,
            OBJECT_MATVIEW,
            Audit_InvalidEnum
        },
        {
            T_UpdateStmt,
            Audit_InvalidEnum
        },
    },

    /* Add Schema Object Auditing Options before AuditSql_SchemaObjectEnd */
    {
        AuditSql_SchemaObjectEnd,
        "SchemaObjectEnd",
        {Audit_InvalidEnum},
        {Audit_InvalidEnum},
        {Audit_InvalidEnum},
    },

    {
        AuditSQL_Ivalid,
        "InvalidAuditSQL",
        {Audit_InvalidEnum},
        {Audit_InvalidEnum},
        {Audit_InvalidEnum},
    }
};

struct 
{
    AuditSQL    action_id;
    int32        action_index;
} gAuditStmtActionIndexMap [(sizeof(gAuditStmtActionMap) / sizeof(gAuditStmtActionMap[0]))] = { {-1, -1} };

#ifdef Audit_001_For_Input 

static void audit_build_idx_for_stmt_action_map(void)
{
    Size i = 0;

    Assert(((sizeof(gAuditStmtActionIndexMap) / sizeof(gAuditStmtActionIndexMap[0]))) == (sizeof(gAuditStmtActionMap) / sizeof(gAuditStmtActionMap[0])));
    
    for (i = 0; i < audit_get_stmt_map_size(); i++)
    {
        gAuditStmtActionIndexMap[i].action_id = gAuditStmtActionMap[i].id;
        gAuditStmtActionIndexMap[i].action_index = i;
    }
}

char * audit_get_type_string(AuditType type)
{
    char * type_str = "Invalid Audit Type";

    switch (type)
    {
        case AuditType_Statement:
            type_str = "Statement Audit";
            break;
        case AuditType_User:
            type_str = "User Audit";
            break;
        case AuditType_Object:
            type_str = "Object Audit";
            break;
        default:
            type_str = "Invalid Audit Type";
            break;
    }

    return type_str;
}

char * audit_get_mode_string(AuditMode mode)
{
    char * mode_str = "Invalid Audit Mode";

    switch (mode)
    {
        case AuditMode_All:
            mode_str = "Audit Always";
            break;
        case AuditMode_Success:
            mode_str = "Audit When Success";
            break;
        case AuditMode_Fail:
            mode_str = "Audit When Not Success";
            break;
        case AuditMode_None:
            mode_str = "Audit Never";
            break;
        default:
            mode_str = "Invalid Audit Mode";
            break;
    }

    return mode_str;
}

Size audit_get_stmt_map_size(void)
{
    return (sizeof(gAuditStmtActionMap) / sizeof(gAuditStmtActionMap[0]));
}

AuditStmtMap * audit_get_stmt_map_by_action(AuditSQL action_id)
{
    Size i = 0;
    int32 idx = -1;
    AuditStmtMap * stmt = NULL;

    Assert(((sizeof(gAuditStmtActionIndexMap) / sizeof(gAuditStmtActionIndexMap[0]))) == (sizeof(gAuditStmtActionMap) / sizeof(gAuditStmtActionMap[0])));

    /* first get index from gAuditStmtActionIndexMap */
    for (i = 0; i < audit_get_stmt_map_size(); i++)
    {
        if (gAuditStmtActionIndexMap[i].action_id == action_id)
        {
            idx = gAuditStmtActionIndexMap[i].action_index;
            break;
        }
    }

    /* not find index in gAuditStmtActionIndexMap */
    if (idx < 0 || idx >= (int32)audit_get_stmt_map_size())
    {
        return NULL;
    }

    stmt = audit_get_stmt_map_by_index(idx);
    Assert(stmt != NULL && stmt->id == action_id);

    return stmt;
}

AuditStmtMap * audit_get_stmt_map_by_index(int32 idx)
{
    if (idx >= 0 && idx < (int32)audit_get_stmt_map_size())
    {
        return (&(gAuditStmtActionMap[idx]));
    }

    return NULL;
}

bool audituser(void)
{
    return audituser_arg(GetUserId());
}

bool audituser_arg(Oid roleid)
{
    return (bool)(roleid == DEFAULT_ROLE_AUDIT_SYS_USERID);
}

Datum
pg_get_audit_action_name(PG_FUNCTION_ARGS)
{
    int32 action_id = PG_GETARG_INT32(0);
    char * action_name = "InvalidStmt";
    AuditStmtMap * item = audit_get_stmt_map_by_action((AuditSQL)action_id);

    if (item != NULL)
    {
        action_name = item->name;
    }

    PG_RETURN_TEXT_P(cstring_to_text(action_name));
}

Datum
pg_get_audit_action_mode(PG_FUNCTION_ARGS)
{
    char action_mode = PG_GETARG_CHAR(0);
    char * mode_str = audit_get_mode_string((AuditMode) action_mode);

    PG_RETURN_TEXT_P(cstring_to_text(mode_str));
}

static bool isa_valid_enum(int32 input)
{
    if (input == Audit_InvalidEnum)
    {
        return false;
    }

    return true;
}

/* 
 * check whether an AuditStmtMap item is a shortcut AuditSQL,
 * such as: AuditSql_Extension, AuditSql_Function, and so on ...
 */
static bool isa_shortcut_audit_stmt(AuditStmtMap * item)
{
    AuditSQL action_id = AuditSQL_Ivalid;

    if (item == NULL)
    {
        return false;
    }

    action_id = item->id;

    /* shortcut is only defined between AuditSql_ShortcutBegin and AuditSql_ShortcutEnd */    
    if (action_id > AuditSql_ShortcutBegin &&
        action_id < AuditSql_ShortcutEnd)
    {
        if (isa_valid_enum((int32)(item->sub_id[0])))
        {
            return true;
        }
    }

    return false;
}

/* Audit xxx */
static bool isa_statement_action_id(AuditSQL action_id)
{
    if (action_id > AuditSql_ShortcutBegin &&
        action_id < AuditSql_ShortcutEnd)
    {
        return true;
    }

    if (action_id > AuditSql_AdditionalBegin &&
        action_id < AuditSql_AdditionalEnd)
    {
        return true;
    }

    if (action_id == AuditSql_All)
    {
        return true;
    }

    return false;
}

/* Audit xxx BY xxx */
static bool isa_user_action_id(AuditSQL action_id)
{
    if (action_id > AuditSql_ShortcutBegin &&
        action_id < AuditSql_ShortcutEnd)
    {
        return true;
    }

    if (action_id > AuditSql_AdditionalBegin &&
        action_id < AuditSql_AdditionalEnd)
    {
        return true;
    }

    if (action_id == AuditSql_All)
    {
        return true;
    }

    return false;
}

/* Audit xxx ON xxx */
static bool isa_object_action_id(AuditSQL action_id)
{
    if (action_id > AuditSql_SchemaObjectBegin &&
        action_id < AuditSql_SchemaObjectEnd)
    {
        return true;
    }

    if (action_id == AuditSql_All)
    {
        return true;
    }

    return false;
}

static bool isa_statement_audit_type(AuditType type)
{
    if (type == AuditType_Statement)
    {
        return true;
    }

    return false;
}

static bool isa_user_audit_type(AuditType type)
{
    if (type == AuditType_User)
    {
        return true;
    }

    return false;
}

static bool isa_object_audit_type(AuditType type)
{
    if (type == AuditType_Object)
    {
        return true;
    }

    return false;
}

static bool is_audit_environment(void)
{
    if (IsNormalProcessingMode() &&
        IsPostmasterEnvironment &&
        IsUnderPostmaster &&
        IsBackendPostgres)
    {
        return true;
    }

    return false;
}

static bool is_audit_enable(void)
{
    return enable_audit;
}

static void audit_get_cacheid_pg_audit_o(int32 * rel_cacheid, 
                                           int32 * oid_cacheid)
{
    if (rel_cacheid != NULL)
    {
        *rel_cacheid = AUDITOBJCONF;
    }

    if (oid_cacheid != NULL)
    {
        *oid_cacheid = AUDITOBJCONFOID;
    }
}

static void audit_get_cacheid_pg_audit_d(int32 * rel_cacheid, 
                                           int32 * oid_cacheid)
{
    if (rel_cacheid != NULL)
    {
        *rel_cacheid = AUDITOBJDEFAULT;
    }

    if (oid_cacheid != NULL)
    {
        *oid_cacheid = AUDITOBJDEFAULTOID;
    }
}

static void audit_get_cacheid_pg_audit_u(int32 * rel_cacheid, 
                                           int32 * oid_cacheid)
{
    if (rel_cacheid != NULL)
    {
        *rel_cacheid = AUDITUSERCONF;
    }

    if (oid_cacheid != NULL)
    {
        *oid_cacheid = AUDITUSERCONFOID;
    }
}

static void audit_get_cacheid_pg_audit_s(int32 * rel_cacheid, 
                                           int32 * oid_cacheid)
{
    if (rel_cacheid != NULL)
    {
        *rel_cacheid = AUDITSTMTCONF;
    }

    if (oid_cacheid != NULL)
    {
        *oid_cacheid = AUDITSTMTCONFOID;
    }
}

/*
 * Check whether AuditSQL is valid for AuditType
 */
static bool audit_check_action_type(AuditType audit_type, 
                                      AuditSQL action_id)
{
    switch (audit_type)
    {
        case AuditType_Statement:
            return isa_statement_action_id(action_id);
            break;
        case AuditType_User:
            return isa_user_action_id(action_id);
            break;
        case AuditType_Object:
            return isa_object_action_id(action_id);
            break;
        default:
            return false;
            break;
    }

    return false;
}

/*
 * get all action_id list for AuditSql_All
 */
static void audit_expand_AuditSql_All(AuditStmt * stmt,
                                      AuditStmtContext * ctxt)
{// #lizard forgives
    Size i = 0;
    AuditSQL action_id = AuditSQL_Ivalid;

    switch (stmt->audit_type)
    {
        case AuditType_Statement:
        case AuditType_User:
            for (i = 0; i < audit_get_stmt_map_size(); i++)
            {
                AuditStmtMap * item = audit_get_stmt_map_by_index(i);
                action_id = item->id;

                /* process SQL Statement Shortcuts */
                if (action_id > AuditSql_ShortcutBegin &&
                    action_id < AuditSql_ShortcutEnd)
                {
                    if (!isa_shortcut_audit_stmt(item))
                    {
                        audit_append_unique_action_item(ctxt, item);
                    }
                }
                else if (action_id > AuditSql_AdditionalBegin &&
                         action_id < AuditSql_AdditionalEnd)
                {
                    /* process for Additional SQL Statement */
                    audit_append_unique_action_item(ctxt, item);
                }
                else if (action_id >= AuditSql_SchemaObjectBegin)
                {
                    /* stop loop at AuditSql_SchemaObjectBegin */
                    break;
                }
            }
            return;
            break;
        case AuditType_Object:
            for (action_id = AuditSql_SchemaObjectBegin + 1; 
                 action_id < AuditSql_SchemaObjectEnd;
                 action_id++)
            {
                AuditStmtMap * item = audit_get_stmt_map_by_action(action_id);
                if (item == NULL)
                {
                    /* iterate to the end of gAuditStmtActionMap, so just break */
                    break;
                }

                Assert(item->id == action_id);

                /* Audit ALL on DEFAULT */
                if (stmt->object_name == NIL)
                {
                    /* Assert(ctxt->object_address == InvalidObjectAddress); */
                    Assert(!IsValidObjectAddress(&(ctxt->object_address)));
                    audit_append_unique_action_item(ctxt, item);
                }
                else
                {
                    /* Audit ALL on TABLE TEST */
                    for (i = 0; i < Audit_MaxObjectTypePerStmt; i++)
                    {
                        ObjectType obj_type = item->obj_type[i];

                        if (!isa_valid_enum((int32)obj_type))
                        {
                            break;
                        }
                        else if (obj_type == stmt->object_type)
                        {
                            audit_append_unique_action_item(ctxt, item);
                            break;
                        }
                    }
                }
            }
            return;
            break;
        default:
            return;
            break;
    }
}

static bool audit_check_relkind(Relation relation,
                                List * object_name,
                                ObjectType object_type)
{// #lizard forgives
    Form_pg_class classForm = relation->rd_rel;
    char relkind = classForm->relkind;

    switch (object_type)
    {
        case OBJECT_INDEX:
            if (relkind != RELKIND_INDEX)
            {
                ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg(" \"%s\" is no an index",
                             NameListToString(object_name))));
                return false;
            }
            break;
        case OBJECT_SEQUENCE:
            if (relkind != RELKIND_SEQUENCE)
            {
                ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg(" \"%s\" is no a sequence",
                             NameListToString(object_name))));
                return false;
            }
            break;
        case OBJECT_TABLE:
            if (relkind != RELKIND_RELATION)
            {
                ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg(" \"%s\" is no a table",
                             NameListToString(object_name))));
                return false;
            }
            break;
        case OBJECT_VIEW:
            if (relkind != RELKIND_VIEW)
            {
                ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg(" \"%s\" is no a view",
                             NameListToString(object_name))));
                return false;
            }
            break;
        case OBJECT_MATVIEW:
            if (relkind != RELKIND_MATVIEW)
            {
                ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg(" \"%s\" is no a materialized view",
                             NameListToString(object_name))));
                return false;
            }
            break;
        case OBJECT_COLUMN:
            if (relkind != RELKIND_RELATION &&
                relkind != RELKIND_VIEW &&
                relkind != RELKIND_MATVIEW)
            {
                ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg(" \"%s\" is no a relation's column",
                             NameListToString(object_name))));
                return false;
            }
            break;
        default:
        {
            ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg("unsupported relkind of \"%s\" to audit",
                             NameListToString(object_name))));
            return false;
            break;
        }
    }

    return true;
}

static void audit_process_object_internal(List * object_name, 
                                           ObjectType object_type,
                                           ObjectAddress * object_address)

{// #lizard forgives
    ObjectAddress address = InvalidObjectAddress;

    if (object_name != NIL)
    {
        Relation relation = NULL;
        Oid obj_nspoid = InvalidOid;

        switch (object_type)
        {
            case OBJECT_INDEX:
            case OBJECT_SEQUENCE:
            case OBJECT_TABLE:
            case OBJECT_VIEW:
            case OBJECT_MATVIEW:
            case OBJECT_COLUMN:
            {
                address = get_object_address(object_type,
                                             (Node *)object_name,
                                             &relation,
                                             AccessShareLock,
                                             false);
                if (relation != NULL)
                {
                    audit_check_relkind(relation,
                                    object_name,
                                    object_type);
                    relation_close(relation, NoLock);
                }

                break;
            }
            case OBJECT_FUNCTION:
            {
                List * lfoid = FunctionGetOidsByName(object_name);

                /* noly supported the first function in current version */
                ObjectAddressSubSet(address, ProcedureRelationId, linitial_oid(lfoid), 0);

                list_free(lfoid);
                break;
            }
            case OBJECT_TYPE:
            {
                TypeName * type_name = makeTypeNameFromNameList(object_name);
                address = get_object_address(object_type,
                                             (Node *)type_name,
                                             &relation,
                                             AccessShareLock,
                                             false);
                if (relation != NULL)
                {
                    relation_close(relation, NoLock);
                }

                pfree(type_name);
                break;
            }
            default:
            {
                ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg("unsupported object \"%s\" to audit",
                             NameListToString(object_name))));
                break;
            }
        }

        obj_nspoid = get_object_namespace(&address);
        if (IsSystemNamespace(obj_nspoid) ||
            IsToastNamespace(obj_nspoid))
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("can not perform audit on system object \"%s\"",
                             NameListToString(object_name))));
        }
    }

    if (object_address != NULL)
    {
        *object_address = address;
    }
}


static void audit_process_object(AuditStmt * stmt, 
                                 AuditStmtContext * ctxt)

{
    audit_process_object_internal(stmt->object_name,
                                  stmt->object_type,
                                  &(ctxt->object_address));

    if (stmt->object_name != NIL)
    {
        Assert(isa_object_audit_type(stmt->audit_type));
    }
}

static void audit_clean_process_object(CleanAuditStmt * stmt, 
                                        CleanAuditStmtContext * ctxt)
{
    audit_process_object_internal(stmt->object_name,
                                  stmt->object_type,
                                  &(ctxt->object_address));
}

static void audit_check_connection(void)
{
    if (IS_PGXC_DATANODE)
    {
        if (!IsConnFromCoord())
        {
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 (errmsg("can not execute audit statement directly on datanode"))));
        }
    }

    if (IS_PGXC_COORDINATOR)
    {
        if (!IsConnFromCoord() &&
            !IsConnFromApp())
        {
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 (errmsg("invalid connection to execute audit statement on coordinator"))));
        }
    }
}

static void audit_free_stmt_context(AuditStmtContext * ctxt)
{
    if (ctxt == NULL)
    {
        return;
    }

    if (ctxt->action_str != NULL)
    {
        pfree(ctxt->action_str->data);
        pfree(ctxt->action_str);
        ctxt->action_str = NULL;
    }

    if (ctxt->l_action_items != NIL)
    {
        list_free(ctxt->l_action_items);
        ctxt->l_action_items = NIL;
    }

    if (ctxt->l_user_oids != NIL)
    {
        list_free(ctxt->l_user_oids);
        ctxt->l_user_oids = NIL;
    }
}

static void audit_clean_free_stmt_context(CleanAuditStmtContext * ctxt)
{
    if (ctxt == NULL)
    {
        return;
    }

    if (ctxt->clean_str != NULL)
    {
        pfree(ctxt->clean_str->data);
        pfree(ctxt->clean_str);
        ctxt->clean_str = NULL;
    }

    if (ctxt->l_user_oids != NIL)
    {
        list_free(ctxt->l_user_oids);
        ctxt->l_user_oids = NIL;
    }
}

static AuditModeMi audit_mode_minus(AuditMode first, AuditMode second)
{// #lizard forgives
    if (first == second)
    {
        return AuditModeMi_Equal;
    }
    else if (second == AuditMode_All)
    {
        return AuditModeMi_Overtop;
    }
    else if (first == AuditMode_All && second == AuditMode_Fail)
    {
        return AuditModeMi_Success;
    }
    else if (first == AuditMode_All && second == AuditMode_Success)
    {
        return AuditModeMi_Fail;
    }
    else if (first == AuditMode_Success && second == AuditMode_Fail)
    {
        return AuditModeMi_Mutex;
    }
    else if (first == AuditMode_Fail && second == AuditMode_Success)
    {
        return AuditModeMi_Mutex;
    }
    else
    {
        return AuditModeMi_Ivalid;
    }

    return AuditModeMi_Ivalid;    
}

static void audit_append_unique_action_item(AuditStmtContext * ctxt,
                                            AuditStmtMap * action_item)
{
    Assert(action_item != NULL);
    ctxt->l_action_items = list_append_unique_ptr(ctxt->l_action_items, (void *)action_item);
}

static void audit_process_action_list(AuditStmt * stmt,
                                      AuditStmtContext * ctxt)
{// #lizard forgives
    ListCell *l = NULL;

    if (stmt->action_list == NIL)
    {
        Assert(stmt->action_list != NIL);
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("none statement to audit")));
        return;
    }

    foreach(l, stmt->action_list)
    {
        AuditSQL action_id = (AuditSQL) lfirst_int(l);
        AuditStmtMap * action_item = audit_get_stmt_map_by_action(action_id);

        if (action_item == NULL)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("can not found \"%d\" in action stmt map", action_id)));
            Assert(0);
        }

        Assert(action_id == action_item->id);

        if (l == list_tail(stmt->action_list))
        {
            appendStringInfo(ctxt->action_str, "%s ", action_item->name);
        }
        else
        {
            appendStringInfo(ctxt->action_str, "%s, ", action_item->name);
        }

        /*
         *    isa_statement_action_id ?
         *    isa_user_action_id ?
         *    isa_object_action_id ?
         */
        if (!audit_check_action_type(stmt->audit_type, action_id))
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("\"%s\" can not be used for \"%s\"",
                            action_item->name,
                            audit_get_type_string(stmt->audit_type))));
            return;
        }

        /* check if this object can be audit by this action */
        /* Audit ALL on TABLE TEST */
        if (isa_object_audit_type(stmt->audit_type) &&
            stmt->object_name != NIL &&
            action_id != AuditSql_All)
        {
            int32 i = 0;
            bool action_can_audit_on_object = false;

            for (i = 0; i < Audit_MaxObjectTypePerStmt; i++)
            {
                ObjectType obj_type = action_item->obj_type[i];

                if (!isa_valid_enum((int32)obj_type))
                {
                    break;
                }
                else if (obj_type == stmt->object_type)
                {
                    action_can_audit_on_object = true;
                    break;
                }
            }

            if (!action_can_audit_on_object)
            {
                ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("can not audit \"%s\" on \"%s\"",
                             action_item->name,
                             NameListToString(stmt->object_name))));
                return;
            }
        }

        /* add sub_id of shortcut into lid */
        if (isa_shortcut_audit_stmt(action_item))
        {
            int32 i = 0;

            for (i = 0; i < Audit_MaxStmtPerShortcut; i++)
            {
                AuditSQL sub_action_id = action_item->sub_id[i];

                if (isa_valid_enum((int32)sub_action_id))
                {
                    AuditStmtMap * sub_action_item = audit_get_stmt_map_by_action(sub_action_id);

                    Assert(sub_action_id == sub_action_item->id);
                    audit_append_unique_action_item(ctxt, sub_action_item);
                }
                else
                {
                    break;
                }
            }
        }
        else if (action_id == AuditSql_All)
        {
            audit_expand_AuditSql_All(stmt, ctxt);
        }
        else
        {
            Assert(action_id == action_item->id);
            audit_append_unique_action_item(ctxt, action_item);
        }
    }
}

static void audit_process_user_list_internal(List * user_list,
                                             List ** l_user_oids)
{
    List * luid = NIL;
    ListCell *l = NULL;

    if (user_list == NIL)
    {
        return;
    }

    foreach(l, user_list)
    {
        Oid user_id = InvalidOid;
        char * user_name = NULL;

        Assert(IsA(lfirst(l), String));

        user_name = strVal(lfirst(l));
        user_id = get_role_oid(user_name, false);

        if (!OidIsValid(user_id))
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_ROLE_SPECIFICATION),
                     errmsg("user \"%s\" does not exist", user_name)));
            return;
        }

        luid = list_append_unique_oid(luid, user_id);
    }

    Assert(luid != NIL);

    if (l_user_oids != NULL)
    {
        *l_user_oids = luid;
    }
}

static void audit_process_user_list(AuditStmt * stmt,
                                    AuditStmtContext * ctxt)
{
    if (isa_user_audit_type(stmt->audit_type))
    {
        Assert(stmt->user_list != NIL);
        audit_process_user_list_internal(stmt->user_list,
                                         &(ctxt->l_user_oids));
    }
}

static void audit_clean_process_user_list(CleanAuditStmt * stmt,
                                          CleanAuditStmtContext * ctxt)

{
    if (stmt->user_list != NIL)
    {
        audit_process_user_list_internal(stmt->user_list,
                                         &(ctxt->l_user_oids));
    }
}

static char * audit_object_type_string(ObjectType object_type, bool report_error)
{// #lizard forgives
    switch (object_type)
    {
        case OBJECT_INDEX:
            return "Index";
            break;
        case OBJECT_SEQUENCE:
            return "Sequence";
            break;
        case OBJECT_TABLE:
            return "Table";
            break;
        case OBJECT_VIEW:
            return "View";
            break;
        case OBJECT_MATVIEW:
            return "Materialized View";
            break;
        case OBJECT_COLUMN:
            return "Column";
            break;
        case OBJECT_FUNCTION:
            return "Function";
            break;
        case OBJECT_TYPE:
            return "Type";
            break;
        default:
            if (report_error == true)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                         errmsg("unsupported object type %d ", object_type)));
            }
            break;
    }

    return "UnknownObjectType";
}

static void audit_final_to_string(AuditStmt * stmt, 
                                  AuditStmtContext * ctxt)
{// #lizard forgives
    switch (stmt->audit_type)
    {
        case AuditType_Statement:
        {
            /*Audit xxx, so do nothing here */
            if (isa_statement_audit_type(stmt->audit_type))
            {
                /* do nothing */
            }
            break;
        }
        case AuditType_User:
        {
            ListCell * l = NULL;
            Assert(stmt->user_list != NIL);

            appendStringInfoString(ctxt->action_str, "BY ");

            foreach(l, stmt->user_list)
            {    
                Assert(IsA(lfirst(l), String));
                if (l == list_tail(stmt->user_list))
                {
                    appendStringInfo(ctxt->action_str, "%s ", strVal(lfirst(l)));
                }
                else
                {
                    appendStringInfo(ctxt->action_str, "%s, ", strVal(lfirst(l)));
                }
            }
            break;
        }
        case AuditType_Object:
        {
            appendStringInfoString(ctxt->action_str, "ON ");

            /* Audit xxx ON DEFAULT */
            if (stmt->object_name == NIL)
            {
                appendStringInfoString(ctxt->action_str, "DEFAULT ");
            }
            else
            {
                appendStringInfo(ctxt->action_str, "%s %s ",
                                 audit_object_type_string(stmt->object_type, true),
                                 NameListToString(stmt->object_name));
            }

            break;
        }
        default:
        {
            ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("unsurpported audit type")));
            break;
        }
    }

    switch (stmt->audit_mode)
    {
        case AuditMode_All:
            appendStringInfoString(ctxt->action_str, "; ");
            break;
        case AuditMode_Success:
            appendStringInfoString(ctxt->action_str, "WHENEVER SUCCESSFUL; ");
            break;
        case AuditMode_Fail:
            appendStringInfoString(ctxt->action_str, "WHENEVER NOT SUCCESSFUL; ");
            break;
        case AuditMode_None:
            /* do nothing */
            break;
        default:
        {
            ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("unsurpported audit mode")));
            break;
        }
    }
}

static void audit_generate_one_record(AuditStmt * stmt,
                                      AuditStmtContext * ctxt,
                                      AuditStmtMap * item,
                                      Oid user_id,
                                      AuditStmtRecord ** record)
{
    AuditStmtRecord * rcd = palloc0(sizeof(AuditStmtRecord));

    Assert(rcd != NULL);
    rcd->action_item = item;

    switch (stmt->audit_type)
    {
        case AuditType_Statement:
        {
            rcd->obj_address = InvalidObjectAddress;
            break;
        }
        case AuditType_User:
        {
            ObjectAddressSet(rcd->obj_address, AuthIdRelationId, user_id);
            break;
        }
        case AuditType_Object:
        {
            if (stmt->object_name != NULL)
            {
                rcd->obj_address = ctxt->object_address;
                /* Assert(rcd->obj_address != InvalidObjectAddress); */
                Assert(IsValidObjectAddress(&(rcd->obj_address)));
            }
            else
            {
                rcd->obj_address = InvalidObjectAddress;
            }
            break;
        }
        default:
        {
            ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("unsurpported audit type")));
            break;
        }
    }

    if (record != NULL)
    {
        *record = rcd;
    }
}

static void audit_generate_record_list(AuditStmt * stmt,
                                       AuditStmtContext * ctxt,
                                       List ** lrecord)
{
    ListCell * l = NULL;
    List * lresult = NIL;

    foreach(l, ctxt->l_action_items)
    {
        AuditStmtMap * item = (AuditStmtMap *) lfirst(l);

        if (ctxt->l_user_oids != NIL)
        {
            ListCell * lu = NULL;
            foreach(lu, ctxt->l_user_oids)
            {
                Oid user_id = lfirst_oid(lu);
                AuditStmtRecord * record = NULL;

                Assert(isa_user_audit_type(stmt->audit_type));
                Assert(isa_user_action_id(item->id));
                audit_generate_one_record(stmt, ctxt, item, user_id, &record);
                Assert(record != NULL);

                lresult = lappend(lresult, record);
            }
        }
        else
        {
            AuditStmtRecord * record = NULL;

            Assert(isa_object_audit_type(stmt->audit_type) || 
                   isa_statement_audit_type(stmt->audit_type));
            Assert(isa_object_action_id(item->id) ||
                   isa_statement_action_id(item->id));
            audit_generate_one_record(stmt, ctxt, item, InvalidOid, &record);
            Assert(record != NULL);

            lresult = lappend(lresult, record);
        }
    }

    if (lrecord != NULL)
    {
        *lrecord = lresult;
    }
}

static void audit_write_sysrel_stmt_conf(AuditStmt * stmt,
                                         AuditStmtContext * ctxt,
                                         AuditRecordWriter * writer,
                                         AuditStmtRecord * record,
                                         Relation pg_sysrel)
{// #lizard forgives
    bool nulls[Natts_pg_audit_s] = { false };
    bool replaces[Natts_pg_audit_s] = { false };
    Datum values[Natts_pg_audit_s] = { 0 };

    HeapTuple oldtup = NULL;
    HeapTuple newtup = NULL;

    AuditStmtMap * item = NULL;
    AuditSQL action_id = AuditSQL_Ivalid;
    char * action_name = NULL;
    AuditMode action_mode = stmt->audit_mode;
    bool action_ison = stmt->audit_ison;
    Oid auditor_id = GetUserId();

    int    i = 0;

    /*
     * initialize arrays needed for heap_form_tuple or heap_modify_tuple
     */
    for (i = 0; i < Natts_pg_audit_s; ++i)
    {
        nulls[i] = false;
        replaces[i] = false;
        values[i] = (Datum) 0;
    }

    item = record->action_item;
    Assert(item != NULL);

    action_id = item->id;
    action_name = item->name;

    oldtup = SearchSysCache1(writer->syscache_id, Int32GetDatum((int32)action_id));
    if (HeapTupleIsValid(oldtup))
    {
        /* already exists this action record, maybe update, 
         * maybe ignoe when the same case 
         */
        Form_audit_stmt_conf pg_struct = (Form_audit_stmt_conf)(GETSTRUCT(oldtup));
        bool should_update = false;

        /* action_mode and action_ison are both differ */
        if (pg_struct->action_mode != (char) action_mode &&
            pg_struct->action_ison != action_ison)
        {
            if (action_ison)
            {
                replaces[Anum_audit_stmt_conf_auditor_id - 1] = true;
                replaces[Anum_audit_stmt_conf_action_mode - 1] = true;
                replaces[Anum_audit_stmt_conf_action_ison - 1] = true;
                values[Anum_audit_stmt_conf_auditor_id - 1] = ObjectIdGetDatum(auditor_id);
                values[Anum_audit_stmt_conf_action_mode - 1] = CharGetDatum((char)action_mode);
                values[Anum_audit_stmt_conf_action_ison - 1] = BoolGetDatum(action_ison);
                should_update = true;
            }
            else
            {
                AuditModeMi mode_mi = audit_mode_minus((AuditMode)pg_struct->action_mode, action_mode);
                if (mode_mi == AuditModeMi_Ivalid || mode_mi == AuditModeMi_Mutex)
                {
                    if (enable_audit_warning)
                    {
                        ereport(WARNING,
                            (errcode(ERRCODE_WARNING),
                             errmsg("001. ignore to close a non-existent audit configuration \"%s\" ", action_name)));
                    }
                }
                else if (mode_mi == AuditModeMi_Overtop)
                {
                    replaces[Anum_audit_stmt_conf_auditor_id - 1] = true;
                    replaces[Anum_audit_stmt_conf_action_ison - 1] = true;
                    values[Anum_audit_stmt_conf_auditor_id - 1] = ObjectIdGetDatum(auditor_id);
                    values[Anum_audit_stmt_conf_action_ison - 1] = BoolGetDatum(action_ison);
                    should_update = true;

                    if (enable_audit_warning)
                    {
                        ereport(WARNING,
                            (errcode(ERRCODE_WARNING),
                             errmsg("002. attempt to close audit configuration \"%s\" more than exists ", action_name)));
                    }
                }
                else
                {
                    /* attempt to noaudit a subset of tup */
                    replaces[Anum_audit_stmt_conf_auditor_id - 1] = true;
                    replaces[Anum_audit_stmt_conf_action_mode - 1] = true;
                    values[Anum_audit_stmt_conf_auditor_id - 1] = ObjectIdGetDatum(auditor_id);
                    values[Anum_audit_stmt_conf_action_mode - 1] = CharGetDatum((char)mode_mi);
                    should_update = true;
                }
            }
        }
        else if (pg_struct->action_mode != (char) action_mode && 
                 pg_struct->action_ison == action_ison)
        {
            /* action_ison equal but action_mode differs */
            if (action_ison)
            {
                AuditModeMi mode_mi = audit_mode_minus((AuditMode)pg_struct->action_mode, action_mode);
                if (mode_mi == AuditModeMi_Overtop || mode_mi == AuditModeMi_Mutex)
                {
                    mode_mi = AuditModeMi_All;
                    replaces[Anum_audit_stmt_conf_auditor_id - 1] = true;
                    replaces[Anum_audit_stmt_conf_action_mode - 1] = true;
                    values[Anum_audit_stmt_conf_auditor_id - 1] = ObjectIdGetDatum(auditor_id);
                    values[Anum_audit_stmt_conf_action_mode - 1] = CharGetDatum((char)mode_mi);
                    should_update = true;
                }
                else
                {
                    if (enable_audit_warning)
                    {
                        ereport(WARNING,
                            (errcode(ERRCODE_WARNING),
                             errmsg("003. ignore to redefine an existent audit configuration \"%s\" ", action_name)));
                    }
                }
            }
            else
            {
                if (enable_audit_warning)
                {
                    ereport(WARNING,
                            (errcode(ERRCODE_WARNING),
                             errmsg("004. ignore to close a non-existent audit configuration \"%s\" ", action_name)));
                }
            }
        }
        else if (pg_struct->action_mode == (char) action_mode && 
                 pg_struct->action_ison != action_ison)
        {
            /* action_mode equal while action_ison differs */
            replaces[Anum_audit_stmt_conf_auditor_id - 1] = true;
            replaces[Anum_audit_stmt_conf_action_ison - 1] = true;
            values[Anum_audit_stmt_conf_auditor_id - 1] = ObjectIdGetDatum(auditor_id);
            values[Anum_audit_stmt_conf_action_ison - 1] = BoolGetDatum(action_ison);
            should_update = true;
        }
        else
        {
            if (action_ison)
            {
                if (enable_audit_warning)
                {
                    ereport(WARNING,
                        (errcode(ERRCODE_WARNING),
                         errmsg("005. ignore to redefine an existent audit configuration \"%s\" ", action_name)));
                }
            }
            else
            {
                if (enable_audit_warning)
                {
                    ereport(WARNING,
                            (errcode(ERRCODE_WARNING),
                             errmsg("006. ignore to reclose a canceled audit configuration \"%s\" ", action_name)));
                }
            }
        }

        /* do update catalog systable */
        if (should_update == true)
        {
            newtup = heap_modify_tuple(oldtup,
                                       RelationGetDescr(pg_sysrel),
                                       values,
                                       nulls,
                                       replaces);
            CatalogTupleUpdate(pg_sysrel, &newtup->t_self, newtup);
        }

        ReleaseSysCache(oldtup);
    }
    else
    {
        /* not exists this action record */
        if (stmt->audit_ison == false)
        {
            /* noaudit a none action record */
            if (enable_audit_warning)
            {
                ereport(WARNING,
                    (errcode(ERRCODE_WARNING),
                     errmsg("007. ignore to close a non-existent audit configuration \"%s\" ", action_name)));
            }

            return;
        }

        /* should insert new audit configuration */
        values[Anum_audit_stmt_conf_auditor_id - 1] = ObjectIdGetDatum(auditor_id);
        values[Anum_audit_stmt_conf_action_id - 1] = Int32GetDatum((int32)action_id);
        values[Anum_audit_stmt_conf_action_mode - 1] = CharGetDatum((char)action_mode);
        values[Anum_audit_stmt_conf_action_ison - 1] = BoolGetDatum(action_ison);

        newtup = heap_form_tuple(RelationGetDescr(pg_sysrel),
                                 values,
                                 nulls);
        CatalogTupleInsert(pg_sysrel, newtup);
    }

    if (newtup != NULL)
    {
        heap_freetuple(newtup);
    }
}

static void audit_write_sysrel_user_conf(AuditStmt * stmt,
                                         AuditStmtContext * ctxt,
                                         AuditRecordWriter * writer,
                                         AuditStmtRecord * record,
                                         Relation pg_sysrel)
{// #lizard forgives
    bool nulls[Natts_pg_audit_u] = { false };
    bool replaces[Natts_pg_audit_u] = { false };
    Datum values[Natts_pg_audit_u] = { 0 };

    HeapTuple oldtup = NULL;
    HeapTuple newtup = NULL;

    AuditStmtMap * item = NULL;
    AuditSQL action_id = AuditSQL_Ivalid;
    char * action_name = NULL;
    AuditMode action_mode = stmt->audit_mode;
    bool action_ison = stmt->audit_ison;
    Oid auditor_id = GetUserId();
    ObjectAddress user_addr = record->obj_address;

    int    i = 0;

    /*
     * initialize arrays needed for heap_form_tuple or heap_modify_tuple
     */
    for (i = 0; i < Natts_pg_audit_u; ++i)
    {
        nulls[i] = false;
        replaces[i] = false;
        values[i] = (Datum) 0;
    }

    item = record->action_item;
    Assert(item != NULL);

    action_id = item->id;
    action_name = item->name;

    oldtup = SearchSysCache2(writer->syscache_id, 
                             ObjectIdGetDatum(user_addr.objectId), 
                             Int32GetDatum((int32)action_id));
    if (HeapTupleIsValid(oldtup))
    {
        /* already exists this action record, maybe update, 
         * maybe ignoe when the same case 
         */
        Form_audit_user_conf pg_struct = (Form_audit_user_conf)(GETSTRUCT(oldtup));
        bool should_update = false;

        /* action_mode and action_ison are both differ */
        if (pg_struct->action_mode != (char) action_mode &&
            pg_struct->action_ison != action_ison)
        {
            if (action_ison)
            {
                replaces[Anum_audit_user_conf_auditor_id - 1] = true;
                replaces[Anum_audit_user_conf_action_mode - 1] = true;
                replaces[Anum_audit_user_conf_action_ison - 1] = true;
                values[Anum_audit_user_conf_auditor_id - 1] = ObjectIdGetDatum(auditor_id);
                values[Anum_audit_user_conf_action_mode - 1] = CharGetDatum((char)action_mode);
                values[Anum_audit_user_conf_action_ison - 1] = BoolGetDatum(action_ison);
                should_update = true;
            }
            else
            {
                AuditModeMi mode_mi = audit_mode_minus((AuditMode)pg_struct->action_mode, action_mode);
                if (mode_mi == AuditModeMi_Ivalid || mode_mi == AuditModeMi_Mutex)
                {
                    if (enable_audit_warning)
                    {
                        ereport(WARNING,
                            (errcode(ERRCODE_WARNING),
                             errmsg("001. ignore to close a non-existent audit configuration \"%s\" ", action_name)));
                    }
                }
                else if (mode_mi == AuditModeMi_Overtop)
                {
                    replaces[Anum_audit_user_conf_auditor_id - 1] = true;
                    replaces[Anum_audit_user_conf_action_ison - 1] = true;
                    values[Anum_audit_user_conf_auditor_id - 1] = ObjectIdGetDatum(auditor_id);
                    values[Anum_audit_user_conf_action_ison - 1] = BoolGetDatum(action_ison);
                    should_update = true;

                    if (enable_audit_warning)
                    {
                        ereport(WARNING,
                            (errcode(ERRCODE_WARNING),
                             errmsg("002. attempt to close audit configuration \"%s\" more than exists ", action_name)));
                    }
                }
                else
                {
                    /* attempt to noaudit a subset of tup */
                    replaces[Anum_audit_user_conf_auditor_id - 1] = true;
                    replaces[Anum_audit_user_conf_action_mode - 1] = true;
                    values[Anum_audit_user_conf_auditor_id - 1] = ObjectIdGetDatum(auditor_id);
                    values[Anum_audit_user_conf_action_mode - 1] = CharGetDatum((char)mode_mi);
                    should_update = true;
                }
            }
        }
        else if (pg_struct->action_mode != (char) action_mode && 
                 pg_struct->action_ison == action_ison)
        {
            /* action_ison equal but action_mode differs */
            if (action_ison)
            {
                AuditModeMi mode_mi = audit_mode_minus((AuditMode)pg_struct->action_mode, action_mode);
                if (mode_mi == AuditModeMi_Overtop || mode_mi == AuditModeMi_Mutex)
                {
                    mode_mi = AuditModeMi_All;
                    replaces[Anum_audit_user_conf_auditor_id - 1] = true;
                    replaces[Anum_audit_user_conf_action_mode - 1] = true;
                    values[Anum_audit_user_conf_auditor_id - 1] = ObjectIdGetDatum(auditor_id);
                    values[Anum_audit_user_conf_action_mode - 1] = CharGetDatum((char)mode_mi);
                    should_update = true;
                }
                else
                {
                    if (enable_audit_warning)
                    {
                        ereport(WARNING,
                            (errcode(ERRCODE_WARNING),
                             errmsg("003. ignore to redefine an existent audit configuration \"%s\" ", action_name)));
                    }
                }
            }
            else
            {
                if (enable_audit_warning)
                {
                    ereport(WARNING,
                            (errcode(ERRCODE_WARNING),
                             errmsg("004. ignore to close a non-existent audit configuration \"%s\" ", action_name)));
                }
            }
        }
        else if (pg_struct->action_mode == (char) action_mode && 
                 pg_struct->action_ison != action_ison)
        {
            /* action_mode equal while action_ison differs */
            replaces[Anum_audit_user_conf_auditor_id - 1] = true;
            replaces[Anum_audit_user_conf_action_ison - 1] = true;
            values[Anum_audit_user_conf_auditor_id - 1] = ObjectIdGetDatum(auditor_id);
            values[Anum_audit_user_conf_action_ison - 1] = BoolGetDatum(action_ison);
            should_update = true;
        }
        else
        {
            if (action_ison)
            {
                if (enable_audit_warning)
                {
                    ereport(WARNING,
                        (errcode(ERRCODE_WARNING),
                         errmsg("005. ignore to redefine an existent audit configuration \"%s\" ", action_name)));
                }
            }
            else
            {
                if (enable_audit_warning)
                {
                    ereport(WARNING,
                            (errcode(ERRCODE_WARNING),
                             errmsg("006. ignore to reclose a canceled audit configuration \"%s\" ", action_name)));
                }
            }
        }

        /* do update catalog systable */
        if (should_update == true)
        {
            newtup = heap_modify_tuple(oldtup,
                                       RelationGetDescr(pg_sysrel),
                                       values,
                                       nulls,
                                       replaces);
            CatalogTupleUpdate(pg_sysrel, &newtup->t_self, newtup);
        }

        ReleaseSysCache(oldtup);
    }
    else
    {
        ObjectAddress myself = InvalidObjectAddress;

        /* not exists this action record */
        if (stmt->audit_ison == false)
        {
            /* noaudit a none action record */
            if (enable_audit_warning)
            {
                ereport(WARNING,
                    (errcode(ERRCODE_WARNING),
                     errmsg("007. ignore to close a non-existent audit configuration \"%s\" ", action_name)));
            }

            return;
        }

        /* should insert new audit configuration */
        values[Anum_audit_user_conf_auditor_id - 1] = ObjectIdGetDatum(auditor_id);
        values[Anum_audit_user_conf_user_id - 1] = ObjectIdGetDatum(user_addr.objectId);
        values[Anum_audit_user_conf_action_id - 1] = Int32GetDatum((int32)action_id);
        values[Anum_audit_user_conf_action_mode - 1] = CharGetDatum((char)action_mode);
        values[Anum_audit_user_conf_action_ison - 1] = BoolGetDatum(action_ison);

        newtup = heap_form_tuple(RelationGetDescr(pg_sysrel),
                                 values,
                                 nulls);
        CatalogTupleInsert(pg_sysrel, newtup);

        myself.classId = writer->sysrel_oid;
        myself.objectId = HeapTupleGetOid(newtup);
        myself.objectSubId = 0;

        if (enable_audit_depend)
        {
            recordSharedDependencyOn(&myself, &user_addr, SHARED_DEPENDENCY_POLICY);
        }
    }

    if (newtup != NULL)
    {
        heap_freetuple(newtup);
    }
}

static void audit_write_sysrel_obj_conf(AuditStmt * stmt,
                                        AuditStmtContext * ctxt,
                                        AuditRecordWriter * writer,
                                        AuditStmtRecord * record,
                                        Relation pg_sysrel)
{// #lizard forgives
    bool nulls[Natts_pg_audit_o] = { false };
    bool replaces[Natts_pg_audit_o] = { false };
    Datum values[Natts_pg_audit_o] = { 0 };

    HeapTuple oldtup = NULL;
    HeapTuple newtup = NULL;

    AuditStmtMap * item = NULL;
    AuditSQL action_id = AuditSQL_Ivalid;
    char * action_name = NULL;
    AuditMode action_mode = stmt->audit_mode;
    bool action_ison = stmt->audit_ison;
    Oid auditor_id = GetUserId();
    ObjectAddress obj_addr = record->obj_address;

    int    i = 0;

    /*
     * initialize arrays needed for heap_form_tuple or heap_modify_tuple
     */
    for (i = 0; i < Natts_pg_audit_o; ++i)
    {
        nulls[i] = false;
        replaces[i] = false;
        values[i] = (Datum) 0;
    }

    item = record->action_item;
    Assert(item != NULL);

    action_id = item->id;
    action_name = item->name;

    oldtup = SearchSysCache4(writer->syscache_id,
                             ObjectIdGetDatum(obj_addr.classId),
                             ObjectIdGetDatum(obj_addr.objectId),
                             Int32GetDatum(obj_addr.objectSubId),
                             Int32GetDatum((int32)action_id));
    if (HeapTupleIsValid(oldtup))
    {
        /* already exists this action record, maybe update, 
         * maybe ignoe when the same case 
         */
        Form_audit_obj_conf pg_struct = (Form_audit_obj_conf)(GETSTRUCT(oldtup));
        bool should_update = false;

        /* action_mode and action_ison are both differ */
        if (pg_struct->action_mode != (char) action_mode &&
            pg_struct->action_ison != action_ison)
        {
            if (action_ison)
            {
                replaces[Anum_audit_obj_conf_auditor_id - 1] = true;
                replaces[Anum_audit_obj_conf_action_mode - 1] = true;
                replaces[Anum_audit_obj_conf_action_ison - 1] = true;
                values[Anum_audit_obj_conf_auditor_id - 1] = ObjectIdGetDatum(auditor_id);
                values[Anum_audit_obj_conf_action_mode - 1] = CharGetDatum((char)action_mode);
                values[Anum_audit_obj_conf_action_ison - 1] = BoolGetDatum(action_ison);
                should_update = true;
            }
            else
            {
                AuditModeMi mode_mi = audit_mode_minus((AuditMode)pg_struct->action_mode, action_mode);
                if (mode_mi == AuditModeMi_Ivalid || mode_mi == AuditModeMi_Mutex)
                {
                    if (enable_audit_warning)
                    {
                        ereport(WARNING,
                            (errcode(ERRCODE_WARNING),
                             errmsg("001. ignore to close a non-existent audit configuration \"%s\" ", action_name)));
                    }
                }
                else if (mode_mi == AuditModeMi_Overtop)
                {
                    replaces[Anum_audit_obj_conf_auditor_id - 1] = true;
                    replaces[Anum_audit_obj_conf_action_ison - 1] = true;
                    values[Anum_audit_obj_conf_auditor_id - 1] = ObjectIdGetDatum(auditor_id);
                    values[Anum_audit_obj_conf_action_ison - 1] = BoolGetDatum(action_ison);
                    should_update = true;

                    if (enable_audit_warning)
                    {
                        ereport(WARNING,
                            (errcode(ERRCODE_WARNING),
                             errmsg("002. attempt to close audit configuration \"%s\" more than exists ", action_name)));
                    }
                }
                else
                {
                    /* attempt to noaudit a subset of tup */
                    replaces[Anum_audit_obj_conf_auditor_id - 1] = true;
                    replaces[Anum_audit_obj_conf_action_mode - 1] = true;
                    values[Anum_audit_obj_conf_auditor_id - 1] = ObjectIdGetDatum(auditor_id);
                    values[Anum_audit_obj_conf_action_mode - 1] = CharGetDatum((char)mode_mi);
                    should_update = true;
                }
            }
        }
        else if (pg_struct->action_mode != (char) action_mode && 
                 pg_struct->action_ison == action_ison)
        {
            /* action_ison equal but action_mode differs */
            if (action_ison)
            {
                AuditModeMi mode_mi = audit_mode_minus((AuditMode)pg_struct->action_mode, action_mode);
                if (mode_mi == AuditModeMi_Overtop || mode_mi == AuditModeMi_Mutex)
                {
                    mode_mi = AuditModeMi_All;
                    replaces[Anum_audit_obj_conf_auditor_id - 1] = true;
                    replaces[Anum_audit_obj_conf_action_mode - 1] = true;
                    values[Anum_audit_obj_conf_auditor_id - 1] = ObjectIdGetDatum(auditor_id);
                    values[Anum_audit_obj_conf_action_mode - 1] = CharGetDatum((char)mode_mi);
                    should_update = true;
                }
                else
                {
                    if (enable_audit_warning)
                    {
                        ereport(WARNING,
                            (errcode(ERRCODE_WARNING),
                             errmsg("003. ignore to redefine an existent audit configuration \"%s\" ", action_name)));
                    }
                }
            }
            else
            {
                if (enable_audit_warning)
                {
                    ereport(WARNING,
                            (errcode(ERRCODE_WARNING),
                             errmsg("004. ignore to close a non-existent audit configuration \"%s\" ", action_name)));
                }
            }
        }
        else if (pg_struct->action_mode == (char) action_mode && 
                 pg_struct->action_ison != action_ison)
        {
            /* action_mode equal while action_ison differs */
            replaces[Anum_audit_obj_conf_auditor_id - 1] = true;
            replaces[Anum_audit_obj_conf_action_ison - 1] = true;
            values[Anum_audit_obj_conf_auditor_id - 1] = ObjectIdGetDatum(auditor_id);
            values[Anum_audit_obj_conf_action_ison - 1] = BoolGetDatum(action_ison);
            should_update = true;
        }
        else
        {
            if (action_ison)
            {
                if (enable_audit_warning)
                {
                    ereport(WARNING,
                        (errcode(ERRCODE_WARNING),
                         errmsg("005. ignore to redefine an existent audit configuration \"%s\" ", action_name)));
                }
            }
            else
            {
                if (enable_audit_warning)
                {
                    ereport(WARNING,
                            (errcode(ERRCODE_WARNING),
                             errmsg("006. ignore to reclose a canceled audit configuration \"%s\" ", action_name)));
                }
            }
        }

        /* do update catalog systable */
        if (should_update == true)
        {
            newtup = heap_modify_tuple(oldtup,
                                       RelationGetDescr(pg_sysrel),
                                       values,
                                       nulls,
                                       replaces);
            CatalogTupleUpdate(pg_sysrel, &newtup->t_self, newtup);
        }

        ReleaseSysCache(oldtup);
    }
    else
    {
        ObjectAddress myself = InvalidObjectAddress;

        /* not exists this action record */
        if (stmt->audit_ison == false)
        {
            /* noaudit a none action record */
            if (enable_audit_warning)
            {
                ereport(WARNING,
                    (errcode(ERRCODE_WARNING),
                     errmsg("007. ignore to close a non-existent audit configuration \"%s\" ", action_name)));
            }

            return;
        }

        /* should insert new audit configuration */
        values[Anum_audit_obj_conf_auditor_id - 1] = ObjectIdGetDatum(auditor_id);
        values[Anum_audit_obj_conf_class_id - 1] = ObjectIdGetDatum(obj_addr.classId);
        values[Anum_audit_obj_conf_object_id - 1] = ObjectIdGetDatum(obj_addr.objectId);
        values[Anum_audit_obj_conf_object_sub_id - 1] = Int32GetDatum(obj_addr.objectSubId);
        values[Anum_audit_obj_conf_action_id - 1] = Int32GetDatum((int32)action_id);
        values[Anum_audit_obj_conf_action_mode - 1] = CharGetDatum((char)action_mode);
        values[Anum_audit_obj_conf_action_ison - 1] = BoolGetDatum(action_ison);

        newtup = heap_form_tuple(RelationGetDescr(pg_sysrel),
                                 values,
                                 nulls);
        CatalogTupleInsert(pg_sysrel, newtup);

        myself.classId = writer->sysrel_oid;
        myself.objectId = HeapTupleGetOid(newtup);
        myself.objectSubId = 0;

        if (enable_audit_depend)
        {
            recordDependencyOn(&myself, &obj_addr, DEPENDENCY_AUTO);
        }
    }

    if (newtup != NULL)
    {
        heap_freetuple(newtup);
    }
}

static void audit_write_sysrel_obj_def_opts(AuditStmt * stmt,
                                             AuditStmtContext * ctxt,
                                             AuditRecordWriter * writer,
                                             AuditStmtRecord * record,
                                             Relation pg_sysrel)
{// #lizard forgives
    bool nulls[Natts_pg_audit_d] = { false };
    bool replaces[Natts_pg_audit_d] = { false };
    Datum values[Natts_pg_audit_d] = { 0 };

    HeapTuple oldtup = NULL;
    HeapTuple newtup = NULL;

    AuditStmtMap * item = NULL;
    AuditSQL action_id = AuditSQL_Ivalid;
    char * action_name = NULL;
    AuditMode action_mode = stmt->audit_mode;
    bool action_ison = stmt->audit_ison;
    Oid auditor_id = GetUserId();

    int    i = 0;

    /*
     * initialize arrays needed for heap_form_tuple or heap_modify_tuple
     */
    for (i = 0; i < Natts_pg_audit_d; ++i)
    {
        nulls[i] = false;
        replaces[i] = false;
        values[i] = (Datum) 0;
    }

    item = record->action_item;
    Assert(item != NULL);

    action_id = item->id;
    action_name = item->name;

    oldtup = SearchSysCache1(writer->syscache_id, Int32GetDatum((int32)action_id));
    if (HeapTupleIsValid(oldtup))
    {
        /* already exists this action record, maybe update, 
         * maybe ignoe when the same case 
         */
        Form_audit_obj_def_opts pg_struct = (Form_audit_obj_def_opts)(GETSTRUCT(oldtup));
        bool should_update = false;

        /* action_mode and action_ison are both differ */
        if (pg_struct->action_mode != (char) action_mode &&
            pg_struct->action_ison != action_ison)
        {
            if (action_ison)
            {
                replaces[Anum_audit_obj_def_opts_auditor_id - 1] = true;
                replaces[Anum_audit_obj_def_opts_action_mode - 1] = true;
                replaces[Anum_audit_obj_def_opts_action_ison - 1] = true;
                values[Anum_audit_obj_def_opts_auditor_id - 1] = ObjectIdGetDatum(auditor_id);
                values[Anum_audit_obj_def_opts_action_mode - 1] = CharGetDatum((char)action_mode);
                values[Anum_audit_obj_def_opts_action_ison - 1] = BoolGetDatum(action_ison);
                should_update = true;
            }
            else
            {
                AuditModeMi mode_mi = audit_mode_minus((AuditMode)pg_struct->action_mode, action_mode);
                if (mode_mi == AuditModeMi_Ivalid || mode_mi == AuditModeMi_Mutex)
                {
                    if (enable_audit_warning)
                    {
                        ereport(WARNING,
                            (errcode(ERRCODE_WARNING),
                             errmsg("001. ignore to close a non-existent audit configuration \"%s\" ", action_name)));
                    }
                }
                else if (mode_mi == AuditModeMi_Overtop)
                {
                    replaces[Anum_audit_obj_def_opts_auditor_id - 1] = true;
                    replaces[Anum_audit_obj_def_opts_action_ison - 1] = true;
                    values[Anum_audit_obj_def_opts_auditor_id - 1] = ObjectIdGetDatum(auditor_id);
                    values[Anum_audit_obj_def_opts_action_ison - 1] = BoolGetDatum(action_ison);
                    should_update = true;

                    if (enable_audit_warning)
                    {
                        ereport(WARNING,
                            (errcode(ERRCODE_WARNING),
                             errmsg("002. attempt to close audit configuration \"%s\" more than exists ", action_name)));
                    }
                }
                else
                {
                    /* attempt to noaudit a subset of tup */
                    replaces[Anum_audit_obj_def_opts_auditor_id - 1] = true;
                    replaces[Anum_audit_obj_def_opts_action_mode - 1] = true;
                    values[Anum_audit_obj_def_opts_auditor_id - 1] = ObjectIdGetDatum(auditor_id);
                    values[Anum_audit_obj_def_opts_action_mode - 1] = CharGetDatum((char)mode_mi);
                    should_update = true;
                }
            }
        }
        else if (pg_struct->action_mode != (char) action_mode && 
                 pg_struct->action_ison == action_ison)
        {
            /* action_ison equal but action_mode differs */
            if (action_ison)
            {
                AuditModeMi mode_mi = audit_mode_minus((AuditMode)pg_struct->action_mode, action_mode);
                if (mode_mi == AuditModeMi_Overtop || mode_mi == AuditModeMi_Mutex)
                {
                    mode_mi = AuditModeMi_All;
                    replaces[Anum_audit_obj_def_opts_auditor_id - 1] = true;
                    replaces[Anum_audit_obj_def_opts_action_mode - 1] = true;
                    values[Anum_audit_obj_def_opts_auditor_id - 1] = ObjectIdGetDatum(auditor_id);
                    values[Anum_audit_obj_def_opts_action_mode - 1] = CharGetDatum((char)mode_mi);
                    should_update = true;
                }
                else
                {
                    if (enable_audit_warning)
                    {
                        ereport(WARNING,
                            (errcode(ERRCODE_WARNING),
                             errmsg("003. ignore to redefine an existent audit configuration \"%s\" ", action_name)));
                    }
                }
            }
            else
            {
                if (enable_audit_warning)
                {
                    ereport(WARNING,
                            (errcode(ERRCODE_WARNING),
                             errmsg("004. ignore to close a non-existent audit configuration \"%s\" ", action_name)));
                }
            }
        }
        else if (pg_struct->action_mode == (char) action_mode && 
                 pg_struct->action_ison != action_ison)
        {
            /* action_mode equal while action_ison differs */
            replaces[Anum_audit_obj_def_opts_auditor_id - 1] = true;
            replaces[Anum_audit_obj_def_opts_action_ison - 1] = true;
            values[Anum_audit_obj_def_opts_auditor_id - 1] = ObjectIdGetDatum(auditor_id);
            values[Anum_audit_obj_def_opts_action_ison - 1] = BoolGetDatum(action_ison);
            should_update = true;
        }
        else
        {
            if (action_ison)
            {
                if (enable_audit_warning)
                {
                    ereport(WARNING,
                        (errcode(ERRCODE_WARNING),
                         errmsg("005. ignore to redefine an existent audit configuration \"%s\" ", action_name)));
                }
            }
            else
            {
                if (enable_audit_warning)
                {
                    ereport(WARNING,
                            (errcode(ERRCODE_WARNING),
                             errmsg("006. ignore to reclose a canceled audit configuration \"%s\" ", action_name)));
                }
            }
        }

        /* do update catalog systable */
        if (should_update == true)
        {
            newtup = heap_modify_tuple(oldtup,
                                       RelationGetDescr(pg_sysrel),
                                       values,
                                       nulls,
                                       replaces);
            CatalogTupleUpdate(pg_sysrel, &newtup->t_self, newtup);
        }

        ReleaseSysCache(oldtup);
    }
    else
    {
        /* not exists this action record */
        if (stmt->audit_ison == false)
        {
            /* noaudit a none action record */
            if (enable_audit_warning)
            {
                ereport(WARNING,
                    (errcode(ERRCODE_WARNING),
                     errmsg("007. ignore to close a non-existent audit configuration \"%s\" ", action_name)));
            }

            return;
        }

        /* should insert new audit configuration */
        values[Anum_audit_obj_def_opts_auditor_id - 1] = ObjectIdGetDatum(auditor_id);
        values[Anum_audit_obj_def_opts_action_id - 1] = Int32GetDatum((int32)action_id);
        values[Anum_audit_obj_def_opts_action_mode - 1] = CharGetDatum((char)action_mode);
        values[Anum_audit_obj_def_opts_action_ison - 1] = BoolGetDatum(action_ison);

        newtup = heap_form_tuple(RelationGetDescr(pg_sysrel),
                                 values,
                                 nulls);
        CatalogTupleInsert(pg_sysrel, newtup);
    }

    if (newtup != NULL)
    {
        heap_freetuple(newtup);
    }
}

static void audit_get_record_writer(AuditStmt * stmt,
                                    AuditRecordWriter * writer)
{
    AuditRecordWriter wr = {InvalidOid, InvalidSysCacheID, NULL};

    switch (stmt->audit_type)
    {
        case AuditType_Statement:
        {
            audit_get_cacheid_pg_audit_s((int32 *)&(wr.syscache_id), NULL);        /* Audit xxx */
            wr.writer_fn = audit_write_sysrel_stmt_conf;
            GetSysCacheInfo(wr.syscache_id, &(wr.sysrel_oid), NULL, NULL);
            break;
        }
        case AuditType_User:
        {
            audit_get_cacheid_pg_audit_u((int32 *)&(wr.syscache_id), NULL);        /* Audit xxx By xxx */
            wr.writer_fn = audit_write_sysrel_user_conf;
            GetSysCacheInfo(wr.syscache_id, &(wr.sysrel_oid), NULL, NULL);
            break;
        }
        case AuditType_Object:
        {
            if (stmt->object_name != NULL)
            {
                audit_get_cacheid_pg_audit_o((int32 *)&(wr.syscache_id), NULL);    /* Audit xxx ON xxx */
                wr.writer_fn = audit_write_sysrel_obj_conf;
                GetSysCacheInfo(wr.syscache_id, &(wr.sysrel_oid), NULL, NULL);
            }
            else
            {
                audit_get_cacheid_pg_audit_d((int32 *)&(wr.syscache_id), NULL); /* Audit xxx ON DEFAULT */
                wr.writer_fn = audit_write_sysrel_obj_def_opts;
                GetSysCacheInfo(wr.syscache_id, &(wr.sysrel_oid), NULL, NULL);
            }
            break;
        }
        default:
        {
            ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("unsurpported audit type")));
            return;
            break;
        }
    }

    if (writer != NULL)
    {
        *writer = wr;
    }
}

static void audit_write_recod_list(AuditStmt * stmt,
                                   AuditStmtContext * ctxt,
                                   List * lrecord)
{
    AuditRecordWriter writer = {InvalidOid, InvalidSysCacheID, NULL};
    Relation pg_sysrel = NULL;
    ListCell * l = NULL;

    audit_get_record_writer(stmt, &writer);
    Assert(OidIsValid(writer.sysrel_oid));
    Assert(writer.syscache_id != InvalidSysCacheID);
    Assert(writer.writer_fn != NULL);

    pg_sysrel = heap_open(writer.sysrel_oid, RowExclusiveLock);

    foreach(l, lrecord)
    {
        AuditStmtRecord * record = (AuditStmtRecord *)lfirst(l);
        (*(writer.writer_fn))(stmt, 
                              ctxt,
                              &writer,
                              record,
                              pg_sysrel);
    }

    CacheInvalidateRelcache(pg_sysrel);
    heap_close(pg_sysrel, RowExclusiveLock);
}

#endif

#ifdef Audit_002_For_Clean

static void audit_remove_tuple_by_oid(int32 cacheid, Oid audit_id)
{
    int32 sys_cacheid = cacheid;
    Oid sys_reloid = InvalidOid;
    Oid sys_indoid = InvalidOid;

    Relation sys_rel = NULL;
    LOCKMODE lockmode = NoLock;

    int sys_nkeys = 0;
    ScanKeyData skey[Audit_nSysScanKeys];
    SysScanDesc sd = NULL;
    HeapTuple    tup = NULL;

    GetSysCacheInfo(sys_cacheid, 
                    &sys_reloid,
                    &sys_indoid,
                    NULL);

    MemSet(skey, 0, sizeof(skey));

    if (OidIsValid(audit_id))
    {
        lockmode = RowExclusiveLock;
        sys_rel = heap_open(sys_reloid, lockmode);

        ScanKeyInit(&(skey[sys_nkeys]),
                    ObjectIdAttributeNumber,
                    BTEqualStrategyNumber, F_OIDEQ,
                    ObjectIdGetDatum(audit_id)); sys_nkeys++;

        /* index scan */
        sd = systable_beginscan(sys_rel, 
                                sys_indoid,
                                true,
                                NULL, 
                                sys_nkeys,
                                skey);
    }
    else
    {
        lockmode = AccessExclusiveLock;
        sys_rel = heap_open(sys_reloid, lockmode);

        ScanKeyInit(&(skey[sys_nkeys]),
                    ObjectIdAttributeNumber,
                    BTGreaterStrategyNumber, F_OIDGT,
                    ObjectIdGetDatum(InvalidOid)); sys_nkeys++;

        /* seq scan */
        sd = systable_beginscan(sys_rel, 
                                sys_indoid,
                                false,
                                NULL, 
                                sys_nkeys,
                                skey);
    }

    while ((tup = systable_getnext(sd)) != NULL)
    {
        CatalogTupleDelete(sys_rel, &tup->t_self);
    }

    systable_endscan(sd);

    CacheInvalidateRelcache(sys_rel);
    heap_close(sys_rel, lockmode);
}

static void audit_clean_for_statement(CleanAuditStmt * stmt, 
                                        CleanAuditStmtContext * ctxt)
{
    Assert(stmt->clean_type == CleanAuditType_Statement||
           stmt->clean_type == CleanAuditType_All);
    RemoveStmtAuditById(InvalidOid);
}

static void audit_clean_for_user_by_oids(List * l_user_oids)
{
    int32 sys_cacheid = InvalidSysCacheID;
    Oid sys_reloid = InvalidOid;
    Oid sys_indoid = InvalidOid;

    Relation sys_rel = NULL;

    ObjectAddresses * objects = NULL;
    ListCell * l = NULL;

    audit_get_cacheid_pg_audit_u((int32 *)&(sys_cacheid), NULL);
    GetSysCacheInfo(sys_cacheid, 
                    &sys_reloid,
                    &sys_indoid,
                    NULL);

    objects = new_object_addresses();
    sys_rel = heap_open(sys_reloid, AccessExclusiveLock);

    foreach(l, l_user_oids)
    {
        Oid user_id = lfirst_oid(l);

        SysScanDesc sd = NULL;
        HeapTuple tup = NULL;
        ScanKeyData skey[Audit_nSysScanKeys];
        int sys_nkeys  = 0;

        MemSet(skey, 0, sizeof(skey));

        if (OidIsValid(user_id))
        {
            ScanKeyInit(&(skey[sys_nkeys]),
                        Anum_audit_user_conf_user_id,
                        BTEqualStrategyNumber, F_OIDEQ,
                        ObjectIdGetDatum(user_id)); sys_nkeys++;

            /* index scan */
            sd = systable_beginscan(sys_rel, 
                                    sys_indoid,
                                    true,
                                    NULL, 
                                    sys_nkeys,
                                    skey);
        }
        else
        {
            ScanKeyInit(&(skey[sys_nkeys]),
                        ObjectIdAttributeNumber,
                        BTGreaterStrategyNumber, F_OIDGT,
                        ObjectIdGetDatum(InvalidOid)); sys_nkeys++;

            /* seq scan */
            sd = systable_beginscan(sys_rel, 
                                    sys_indoid,
                                    false,
                                    NULL, 
                                    sys_nkeys,
                                    skey);
        }

        while ((tup = systable_getnext(sd)) != NULL)
        {
            Oid tupoid = HeapTupleGetOid(tup);

            if (enable_audit_depend)
            {
                ObjectAddress obj = InvalidObjectAddress;
                ObjectAddressSet(obj, sys_reloid, tupoid);
                add_exact_object_address(&obj, objects);
            }
            else
            {
                CatalogTupleDelete(sys_rel, &tup->t_self);
            }
        }

        systable_endscan(sd);
    }

    CacheInvalidateRelcache(sys_rel);
    heap_close(sys_rel, AccessExclusiveLock);

    /* perform delete depend tupe */
    performMultipleDeletions(objects, DROP_RESTRICT, 0 /* PERFORM_DELETION_INTERNAL */);
    free_object_addresses(objects);
}

static void audit_clean_for_user(CleanAuditStmt * stmt, 
                                 CleanAuditStmtContext * ctxt)
{
    if (ctxt->l_user_oids != NIL)
    {
        Assert(stmt->user_list != NIL);
        Assert(stmt->clean_type == CleanAuditType_ByUser ||
               stmt->clean_type == CleanAuditType_All);
        audit_clean_for_user_by_oids(ctxt->l_user_oids);
    }
    else
    {
        Assert(stmt->user_list == NIL);
        Assert(stmt->clean_type == CleanAuditType_User ||
               stmt->clean_type == CleanAuditType_All);
        audit_clean_for_user_by_oids(list_make1_oid(InvalidOid));
    }
}

static void audit_clean_for_object(CleanAuditStmt * stmt, 
                                      CleanAuditStmtContext * ctxt)
{
    int32 sys_cacheid = InvalidSysCacheID;
    Oid sys_reloid = InvalidOid;
    Oid sys_indoid = InvalidOid;

    LOCKMODE lockmode = NoLock;
    Relation sys_rel = NULL;
    SysScanDesc sd = NULL;
    HeapTuple tup = NULL;

    int sys_nkeys = 0;
    ScanKeyData skey[Audit_nSysScanKeys];

    ObjectAddresses * objects = NULL;

    Assert(stmt->clean_type == CleanAuditType_All ||
           stmt->clean_type == CleanAuditType_Object ||
           stmt->clean_type == CleanAuditType_OnObject);

    audit_get_cacheid_pg_audit_o((int32 *)&(sys_cacheid), NULL);
    GetSysCacheInfo(sys_cacheid, 
                    &sys_reloid,
                    &sys_indoid,
                    NULL);

    MemSet(skey, 0, sizeof(skey));

    objects = new_object_addresses();

    /* if (ctxt->object_address == InvalidObjectAddress) */
    if (!IsValidObjectAddress(&(ctxt->object_address)))
    {
        lockmode = AccessExclusiveLock;
        sys_rel = heap_open(sys_reloid, lockmode);

        ScanKeyInit(&(skey[sys_nkeys]),
                    ObjectIdAttributeNumber,
                    BTGreaterStrategyNumber, F_OIDGT,
                    ObjectIdGetDatum(InvalidOid)); sys_nkeys++;

        /* seq scan */
        sd = systable_beginscan(sys_rel, 
                                sys_indoid,
                                false,
                                NULL, 
                                sys_nkeys,
                                skey);
    }
    else
    {
        lockmode = RowExclusiveLock;
        sys_rel = heap_open(sys_reloid, lockmode);

        ScanKeyInit(&(skey[sys_nkeys]),
                    Anum_audit_obj_conf_class_id,
                    BTEqualStrategyNumber, F_OIDEQ,
                    ObjectIdGetDatum(ctxt->object_address.classId)); sys_nkeys++;
        ScanKeyInit(&(skey[sys_nkeys]),
                    Anum_audit_obj_conf_object_id,
                    BTEqualStrategyNumber, F_OIDEQ,
                    ObjectIdGetDatum(ctxt->object_address.objectId)); sys_nkeys++;
        ScanKeyInit(&(skey[sys_nkeys]),
                    Anum_audit_obj_conf_object_sub_id,
                    BTEqualStrategyNumber, F_INT4EQ,
                    Int32GetDatum(ctxt->object_address.objectSubId)); sys_nkeys++;

        /* index scan */
        sd = systable_beginscan(sys_rel, 
                                sys_indoid,
                                true,
                                NULL, 
                                sys_nkeys,
                                skey);
    }

    while ((tup = systable_getnext(sd)) != NULL)
    {
        Oid tupoid = HeapTupleGetOid(tup);

        if (enable_audit_depend)
        {
            ObjectAddress obj = InvalidObjectAddress;
            ObjectAddressSet(obj, sys_reloid, tupoid);
            add_exact_object_address(&obj, objects);
        }
        else
        {
            CatalogTupleDelete(sys_rel, &tup->t_self);
        }
    }

    systable_endscan(sd);

    CacheInvalidateRelcache(sys_rel);
    heap_close(sys_rel, lockmode);

    /* perform delete depend tupe */
    performMultipleDeletions(objects, DROP_RESTRICT, 0 /* PERFORM_DELETION_INTERNAL */);
    free_object_addresses(objects);
}

static void audit_clean_for_object_default(CleanAuditStmt * stmt, 
                                                CleanAuditStmtContext * ctxt)
{
    Assert(stmt->clean_type == CleanAuditType_All ||
           stmt->clean_type == CleanAuditType_OnDefault);
    RemoveObjectDefaultAuditById(InvalidOid);
}

static void audit_clean_for_unknown(CleanAuditStmt * stmt, 
                                    CleanAuditStmtContext * ctxt)
{// #lizard forgives
    StringInfo clean_str = NULL;
    int ret = SPI_OK_DELETE;

    Assert(stmt->clean_type == CleanAuditType_Unknown);

    ret = SPI_connect();
    if (ret < 0)
    {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("SPI connect failure on node '%s', returned %d",
                        PGXCNodeName, ret)));
    }

    /* delete unknown user audit, user mabye droped */
    clean_str = makeStringInfo();
    appendStringInfoString(clean_str, "delete from pg_catalog.pg_audit_user_conf"
                                             " where pg_catalog.pg_audit_user_conf.user_id not in (select pg_catalog.pg_authid.oid from pg_catalog.pg_authid); ");
    ret = SPI_exec(clean_str->data, 0);
    if (ret != SPI_OK_DELETE)
    {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("failed to execute '%s' on node '%s'",
                        clean_str->data, PGXCNodeName)));
    }

    /* delete unknown sequence/index/table/view/matview, rel mabye droped  */
    resetStringInfo(clean_str);
    appendStringInfo(clean_str, "delete from pg_catalog.pg_audit_obj_conf"
                                      " where pg_catalog.pg_audit_obj_conf.class_id = %d"
                                            " and pg_catalog.pg_audit_obj_conf.object_id not in (select pg_catalog.pg_class.oid from pg_catalog.pg_class); ",
                              RelationRelationId);
    ret = SPI_exec(clean_str->data, 0);
    if (ret != SPI_OK_DELETE)
    {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("failed to execute '%s' on node '%s'",
                        clean_str->data, PGXCNodeName)));
    }

    /* delete unknown Column, column mabye droped */
    resetStringInfo(clean_str);
    appendStringInfo(clean_str, "delete from pg_catalog.pg_audit_obj_conf"
                                       " where pg_catalog.pg_audit_obj_conf.class_id = %d"
                                             " and pg_catalog.pg_audit_obj_conf.object_id in (select pg_catalog.pg_class.oid from pg_catalog.pg_class)"
                                             " and pg_catalog.pg_audit_obj_conf.object_sub_id != 0"
                                             " and pg_catalog.pg_audit_obj_conf.object_sub_id not in (select pg_catalog.pg_attribute.attnum"
                                                                                                           " from pg_catalog.pg_attribute"
                                                                                                           " where pg_catalog.pg_attribute.attrelid = pg_catalog.pg_audit_obj_conf.object_id"
                                                                                                    "); ",
                                 RelationRelationId);
    ret = SPI_exec(clean_str->data, 0);
    if (ret != SPI_OK_DELETE)
    {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("failed to execute '%s' on node '%s'",
                        clean_str->data, PGXCNodeName)));
    }

    /* delete unknown Function, func mabye droped */
    resetStringInfo(clean_str);
    appendStringInfo(clean_str, "delete from pg_catalog.pg_audit_obj_conf"
                                       " where pg_catalog.pg_audit_obj_conf.class_id = %d"
                                             " and pg_catalog.pg_audit_obj_conf.object_id not in (select pg_catalog.pg_proc.oid from pg_catalog.pg_proc)"
                                             " and pg_catalog.pg_audit_obj_conf.object_sub_id = 0; ",
                                 ProcedureRelationId);
    ret = SPI_exec(clean_str->data, 0);
    if (ret != SPI_OK_DELETE)
    {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("failed to execute '%s' on node '%s'",
                        clean_str->data, PGXCNodeName)));
    }

    /* delete unknown type, type mabye droped */
    resetStringInfo(clean_str);
    appendStringInfo(clean_str, "delete from pg_catalog.pg_audit_obj_conf"
                                       " where pg_catalog.pg_audit_obj_conf.class_id = %d"
                                             " and pg_catalog.pg_audit_obj_conf.object_id not in (select pg_catalog.pg_type.oid from pg_catalog.pg_type)"
                                             " and pg_catalog.pg_audit_obj_conf.object_sub_id = 0; ",
                                TypeRelationId);
    ret = SPI_exec(clean_str->data, 0);
    if (ret != SPI_OK_DELETE)
    {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("failed to execute '%s' on node '%s'",
                        clean_str->data, PGXCNodeName)));
    }

    pfree(clean_str->data);
    pfree(clean_str);

    if (SPI_finish() != SPI_OK_FINISH)
    {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("SPI_finish() failed on node '%s'",
                        PGXCNodeName)));
    }
}

static void audit_clean_process_delete(CleanAuditStmt * stmt, 
                                         CleanAuditStmtContext * ctxt)
{// #lizard forgives
    switch (stmt->clean_type)
    {
        case CleanAuditType_All:
        {
            audit_clean_for_statement(stmt, ctxt);
            audit_clean_for_user(stmt, ctxt);
            audit_clean_for_object(stmt, ctxt);
            audit_clean_for_object_default(stmt, ctxt);
            appendStringInfoString(ctxt->clean_str, "Clean All Audit ; ");
            break;
        }
        case CleanAuditType_Statement:
        {
            audit_clean_for_statement(stmt, ctxt);
            appendStringInfoString(ctxt->clean_str, "Clean Statement Audit ; ");
            break;
        }
        case CleanAuditType_User:
        {
            audit_clean_for_user(stmt, ctxt);
            appendStringInfoString(ctxt->clean_str, "Clean User Audit ; ");
            break;
        }
        case CleanAuditType_ByUser:
        {
            ListCell * l = NULL;
            Assert(stmt->user_list != NIL);

            audit_clean_for_user(stmt, ctxt);
            appendStringInfoString(ctxt->clean_str, "Clean User Audit By ");

            foreach(l, stmt->user_list)
            {    
                Assert(IsA(lfirst(l), String));
                if (l == list_tail(stmt->user_list))
                {
                    appendStringInfo(ctxt->clean_str, "%s ; ", strVal(lfirst(l)));
                }
                else
                {
                    appendStringInfo(ctxt->clean_str, "%s, ", strVal(lfirst(l)));
                }
            }

            break;
        }
        case CleanAuditType_Object:
        {
            audit_clean_for_object(stmt, ctxt);
            appendStringInfoString(ctxt->clean_str, "Clean Object Audit ; ");
            break;
        }
        case CleanAuditType_OnObject:
        {
            Assert(stmt->object_name != NIL);

            audit_clean_for_object(stmt, ctxt);
            appendStringInfo(ctxt->clean_str, "Clean Object Audit On %s %s ; ",
                             audit_object_type_string(stmt->object_type, true),
                             NameListToString(stmt->object_name));
            break;
        }
        case CleanAuditType_OnDefault:
        {
            audit_clean_for_object_default(stmt, ctxt);
            appendStringInfoString(ctxt->clean_str, "Clean Object Audit On Default ; ");
            break;
        }
        case CleanAuditType_Unknown:
        {
            audit_clean_for_unknown(stmt, ctxt);
            appendStringInfoString(ctxt->clean_str, "Clean Unknown Audit ; ");
            break;
        }
        default:
        {
            ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("unsurpported clean audit type")));
            break;
        }
    }
}

void RemoveStmtAuditById(Oid audit_id)
{
    int32 sys_cacheid = InvalidSysCacheID;
    audit_get_cacheid_pg_audit_s(NULL, (int32 *)&(sys_cacheid));
    audit_remove_tuple_by_oid(sys_cacheid, audit_id);
}

void RemoveUserAuditById(Oid audit_id)
{
    int32 sys_cacheid = InvalidSysCacheID;
    audit_get_cacheid_pg_audit_u(NULL, (int32 *)&(sys_cacheid));
    audit_remove_tuple_by_oid(sys_cacheid, audit_id);
}

void RemoveObjectAuditById(Oid audit_id)
{
    int32 sys_cacheid = InvalidSysCacheID;
    audit_get_cacheid_pg_audit_o(NULL, (int32 *)&(sys_cacheid));
    audit_remove_tuple_by_oid(sys_cacheid, audit_id);
}

void RemoveObjectDefaultAuditById(Oid audit_id)
{
    int32 sys_cacheid = InvalidSysCacheID;
    audit_get_cacheid_pg_audit_d(NULL, &(sys_cacheid));
    audit_remove_tuple_by_oid(sys_cacheid, audit_id);
}

/* "statement audit %s" */
char * get_stmt_audit_desc(Oid audit_id, bool just_identity)
{
    int32 sys_cacheid = InvalidSysCacheID;
    HeapTuple tup = NULL;
    StringInfoData str_desc;

    initStringInfo(&str_desc);

    if (!just_identity)
    {
        appendStringInfoString(&str_desc, "statement audit ");
    }

    audit_get_cacheid_pg_audit_s(NULL, (int32 *)&(sys_cacheid));
    tup = SearchSysCache1(sys_cacheid, ObjectIdGetDatum(audit_id));
    if (HeapTupleIsValid(tup))
    {
        Form_audit_stmt_conf pg_struct = (Form_audit_stmt_conf)(GETSTRUCT(tup));
        int32 action_id = pg_struct->action_id;
        AuditStmtMap * action_item = audit_get_stmt_map_by_action((AuditSQL)action_id);
        
        if (action_item != NULL)
        {
            appendStringInfo(&str_desc, "%s", action_item->name);
        }
        else
        {
            appendStringInfo(&str_desc, "%s", "InvaldStmt");
        }

        ReleaseSysCache(tup);
    }
    else
    {
        appendStringInfo(&str_desc, "%s", "InvaldStmt");
    }

    return str_desc.data;
}

/* "user audit %s by %s" */
char * get_user_audit_desc(Oid audit_id, bool just_identity)
{
    int32 sys_cacheid = InvalidSysCacheID;
    HeapTuple tup = NULL;
    StringInfoData str_desc;

    initStringInfo(&str_desc);

    if (!just_identity)
    {
        appendStringInfoString(&str_desc, "user audit ");
    }

    audit_get_cacheid_pg_audit_u(NULL, &(sys_cacheid));
    tup = SearchSysCache1(sys_cacheid, ObjectIdGetDatum(audit_id));
    if (HeapTupleIsValid(tup))
    {
        Form_audit_user_conf pg_struct = (Form_audit_user_conf)(GETSTRUCT(tup));
        int32 action_id = pg_struct->action_id;
        ObjectAddress obj_address = InvalidObjectAddress;
        AuditStmtMap * action_item = audit_get_stmt_map_by_action((AuditSQL)action_id);

        ObjectAddressSet(obj_address, 
                         AuthIdRelationId,
                         pg_struct->user_id);

        if (action_item != NULL)
        {
            appendStringInfo(&str_desc, "%s by %s",
                             action_item->name,
                             getObjectDescription(&obj_address));
        }
        else
        {
            appendStringInfo(&str_desc, "%s by %s", 
                             "InvaldStmt", 
                             getObjectDescription(&obj_address));
        }

        ReleaseSysCache(tup);
    }
    else
    {
        appendStringInfo(&str_desc, "%s by %s", "InvaldStmt", "InvalidUser");
    }

    return str_desc.data;    
}

/* "object audit %s on %s" */
char * get_obj_audit_desc(Oid audit_id, bool just_identity)
{
    int32 sys_cacheid = InvalidSysCacheID;
    HeapTuple tup = NULL;
    StringInfoData str_desc;

    initStringInfo(&str_desc);

    if (!just_identity)
    {
        appendStringInfoString(&str_desc, "object audit ");
    }

    audit_get_cacheid_pg_audit_o(NULL, (int32 *)&(sys_cacheid));
    tup = SearchSysCache1(sys_cacheid, ObjectIdGetDatum(audit_id));
    if (HeapTupleIsValid(tup))
    {
        Form_audit_obj_conf pg_struct = (Form_audit_obj_conf)(GETSTRUCT(tup));
        int32 action_id = pg_struct->action_id;
        ObjectAddress obj_address = InvalidObjectAddress;
        AuditStmtMap * action_item = audit_get_stmt_map_by_action((AuditSQL)action_id);

        ObjectAddressSubSet(obj_address,
                            pg_struct->class_id,
                            pg_struct->object_id,
                            pg_struct->object_sub_id);

        if (action_item != NULL)
        {
            appendStringInfo(&str_desc, "%s on %s",
                             action_item->name,
                             getObjectDescription(&obj_address));
        }
        else
        {
            appendStringInfo(&str_desc, "%s on %s", 
                             "InvaldStmt", 
                             getObjectDescription(&obj_address));
        }

        ReleaseSysCache(tup);
    }
    else
    {
        appendStringInfo(&str_desc, "%s on %s", "InvaldStmt", "InvalidObject");
    }

    return str_desc.data;
}

/* object default audit %s */
char * get_obj_default_audit_desc(Oid audit_id, bool just_identity)
{
    int32 sys_cacheid = InvalidSysCacheID;
    HeapTuple tup = NULL;
    StringInfoData str_desc;

    initStringInfo(&str_desc);

    if (!just_identity)
    {
        appendStringInfoString(&str_desc, "object default audit ");
    }

    audit_get_cacheid_pg_audit_d(NULL, (int32 *)&(sys_cacheid));
    tup = SearchSysCache1(sys_cacheid, ObjectIdGetDatum(audit_id));
    if (HeapTupleIsValid(tup))
    {
        Form_audit_obj_def_opts pg_struct = (Form_audit_obj_def_opts)(GETSTRUCT(tup));
        int32 action_id = pg_struct->action_id;
        AuditStmtMap * action_item = audit_get_stmt_map_by_action((AuditSQL)action_id);
        
        if (action_item != NULL)
        {
            appendStringInfo(&str_desc, "%s", action_item->name);
        }
        else
        {
            appendStringInfo(&str_desc, "%s", "InvaldStmt");
        }

        ReleaseSysCache(tup);
    }
    else
    {
        appendStringInfo(&str_desc, "%s", "InvaldStmt");
    }

    return str_desc.data;
}

#endif

void AuditInitEnv(void)
{
    /* first build index for gAuditStmtActionMap 
     * to make it faster when call audit_get_stmt_map_by_action
     */
    audit_build_idx_for_stmt_action_map();
}

void AuditDefine(AuditStmt * stmt, const char * queryString, char ** auditString)
{
    AuditStmtContext stmt_ctx = { NULL, NIL, NIL, InvalidObjectAddress };
    List * laudit_record = NIL;

    if (!audituser())
    {
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 (errmsg("must be audituser to define audit configuration"))));
        return;
    }

    if (!is_audit_environment())
    {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 (errmsg("must in NormalProcessing to define audit configuration"))));
        return;
    }

    audit_check_connection();
    stmt_ctx.action_str = makeStringInfo();

    if (stmt->audit_ison)
    {
        appendStringInfoString(stmt_ctx.action_str, "AUDIT ");
    }
    else
    {
        appendStringInfoString(stmt_ctx.action_str, "NOAUDIT ");
    }

    audit_process_object(stmt, &stmt_ctx);
    audit_process_user_list(stmt, &stmt_ctx);
    audit_process_action_list(stmt, &stmt_ctx);
    audit_final_to_string(stmt, &stmt_ctx);
    audit_generate_record_list(stmt, &stmt_ctx, &laudit_record);
    audit_write_recod_list(stmt, &stmt_ctx, laudit_record);

    if (enable_audit_warning)
    {
        ereport(WARNING,
                (errmsg("AuditDefine Rcv: %s ", queryString)));
        ereport(WARNING,
                (errmsg("AuditDefine Snd: %s ", stmt_ctx.action_str->data)));
    }

    if (auditString != NULL)
    {
        *auditString = pstrdup(stmt_ctx.action_str->data);
    }

    audit_free_stmt_context(&stmt_ctx);
    if (laudit_record != NIL)
    {
        list_free_deep(laudit_record);
    }
}

void AuditClean(CleanAuditStmt * stmt, const char * queryString, char ** cleanString)
{
    CleanAuditStmtContext stmt_ctx = { NULL, NIL, InvalidObjectAddress };

    if (!audituser())
    {
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 (errmsg("must be audituser to clean audit configuration"))));
        return;
    }

    if (!is_audit_environment())
    {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 (errmsg("must in NormalProcessing to clean audit configuration"))));
        return;
    }

    audit_check_connection();
    stmt_ctx.clean_str = makeStringInfo();

    audit_clean_process_object(stmt, &stmt_ctx);
    audit_clean_process_user_list(stmt, &stmt_ctx);
    audit_clean_process_delete(stmt, &stmt_ctx);

    if (enable_audit_warning)
    {
        ereport(WARNING,
                (errmsg("CleanAudit Rcv: %s ", queryString)));
        ereport(WARNING,
                (errmsg("CleanAudit Snd: %s ", stmt_ctx.clean_str->data)));
    }

    if (cleanString != NULL)
    {
        *cleanString = pstrdup(stmt_ctx.clean_str->data);
    }

    audit_clean_free_stmt_context(&stmt_ctx);
}

void AuditClearResultInfo(void)
{
    if (is_audit_environment())
    {
        audit_hit_set_result_info(NULL);
    }
}

void AuditReadQueryList(const char * query_sring, List * l_parsetree)
{
    /*
     * Only audit sql from outside
     */
    if (!IsConnFromApp())
    {
        return;
    }

    if (is_audit_enable() && 
        is_audit_environment() &&
        l_parsetree != NULL)
    {
        MemoryContext oldcontext = MemoryContextSwitchTo(AuditContext);
        audit_hit_read_query_list(MyProcPort, query_sring, l_parsetree);
        MemoryContextSwitchTo(oldcontext);
    }
}

void AuditProcessResultInfo(bool is_success)
{
    if (is_audit_enable() && 
        is_audit_environment())
    {
        MemoryContext oldcontext = MemoryContextSwitchTo(AuditContext);
        audit_hit_process_result_info(is_success);
        audit_hit_set_result_info(NULL);
        MemoryContextSwitchTo(oldcontext);
    }
}

void AuditCheckPerms(Oid table_oid, Oid roleid, AclMode mask)
{
    if ((mask & (ACL_INSERT | ACL_UPDATE | ACL_TRUNCATE)) &&
        IsAuditClass(table_oid))
    {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_CLASS, get_rel_name(table_oid));
    }
}

#ifdef Audit_003_For_Hit

static AuditResultInfo * audit_hit_init_result_info(void)
{// #lizard forgives
    AuditResultInfo * audit_ret = NULL;

    audit_ret = palloc0(sizeof(AuditResultInfo));
    audit_ret->qry_string = NULL;
    audit_ret->cmd_tag = NULL;
    audit_ret->l_hit_info = NIL;
    audit_ret->db_id = MyDatabaseId;

    {
        char * db_name = get_database_name(MyDatabaseId);
        if (db_name == NULL)
        {
            db_name = "unknown";
        }
        audit_ret->db_name = pstrdup(db_name);
    }

    audit_ret->db_user_id = GetUserId();

    {
        char * db_user_name = GetUserNameFromId(audit_ret->db_user_id, true);
        if (db_user_name == NULL)
        {
            db_user_name = "unknown";
        }
        audit_ret->db_user_name = pstrdup(db_user_name);
    }

    audit_ret->node_name = pstrdup((PGXCNodeName != NULL && PGXCNodeName[0] != '\0') ? PGXCNodeName : "unknown");
    audit_ret->node_oid = get_pgxc_nodeoid(audit_ret->node_name);
    if (OidIsValid(audit_ret->node_oid))
    {
        audit_ret->node_type = get_pgxc_nodetype(audit_ret->node_oid);
    }
    else
    {
        audit_ret->node_type = PGXC_NODE_NONE;
        if (IS_PGXC_DATANODE)
        {
            audit_ret->node_type = PGXC_NODE_DATANODE;
        }
        else if (IS_PGXC_COORDINATOR)
        {
            audit_ret->node_type = PGXC_NODE_COORDINATOR;
        }
    }

    {
        char * errstr = NULL;
        const char * node_osuser = get_user_name(&errstr);
        if (node_osuser == NULL)
        {
            node_osuser = "unknown";
        }
        audit_ret->node_osuser = pstrdup(node_osuser);
    }

    if (OidIsValid(audit_ret->node_oid))
    {
        audit_ret->node_host = pstrdup(get_pgxc_nodehost(audit_ret->node_oid));
        audit_ret->node_port = get_pgxc_nodeport(audit_ret->node_oid);
    }
    else
    {
        audit_ret->node_host = pstrdup("localhost");
        audit_ret->node_port = PostPortNumber;
    }

    audit_ret->proc_pid = getpid();
    audit_ret->proc_ppid = getppid();
    audit_ret->proc_start_time = MyStartTime;
    audit_ret->qry_begin_time = (pg_time_t) time(NULL);
    audit_ret->qry_end_time = (pg_time_t) time(NULL);
    audit_ret->is_success = false;
    audit_ret->error_msg = NULL;
    audit_ret->client_host = pstrdup((MyProcPort != NULL && MyProcPort->remote_host != NULL && MyProcPort->remote_host[0] != '\0') ? MyProcPort->remote_host : "unknown");
    audit_ret->client_hostname = pstrdup((MyProcPort != NULL && MyProcPort->remote_hostname != NULL && MyProcPort->remote_hostname[0] != '\0') ? MyProcPort->remote_hostname : "unknown");
    audit_ret->client_port = pstrdup((MyProcPort != NULL && MyProcPort->remote_port != NULL && MyProcPort->remote_port[0] != '\0') ? MyProcPort->remote_port : "unknown");
    audit_ret->app_name = pstrdup((application_name != NULL && application_name[0] != '\0') ? application_name : "unknown");

    return audit_ret;
}

static void audit_hit_free_result_info(AuditResultInfo * audit_ret)
{// #lizard forgives
    if (audit_ret == NULL)
    {
        return;
    }
    
    if (audit_ret->qry_string != NULL)
    {
        pfree(audit_ret->qry_string);
    }

    if (audit_ret->cmd_tag != NULL)
    {
        pfree(audit_ret->cmd_tag);
    }

    if (audit_ret->l_hit_info != NIL)
    {
        ListCell * l = NULL;
        foreach(l, audit_ret->l_hit_info)
        {
            AuditHitInfo * hit_info = (AuditHitInfo *) lfirst(l);
            audit_hit_free_hit_info(hit_info);
        }

        list_free(audit_ret->l_hit_info);
    }

    if (audit_ret->db_name != NULL)
    {
        pfree(audit_ret->db_name);
    }

    if (audit_ret->db_user_name != NULL)
    {
        pfree(audit_ret->db_user_name);
    }

    if (audit_ret->node_name != NULL)
    {
        pfree(audit_ret->node_name);
    }

    if (audit_ret->node_osuser != NULL)
    {
        pfree(audit_ret->node_osuser);
    }

    if (audit_ret->node_host != NULL)
    {
        pfree(audit_ret->node_host);
    }

    if (audit_ret->error_msg != NULL)
    {
        pfree(audit_ret->error_msg);
    }

    if (audit_ret->client_host != NULL)
    {
        pfree(audit_ret->client_host);
    }

    if (audit_ret->client_hostname != NULL)
    {
        pfree(audit_ret->client_hostname);
    }

    if (audit_ret->client_port != NULL)
    {
        pfree(audit_ret->client_port);
    }

    if (audit_ret->app_name != NULL)
    {
        pfree(audit_ret->app_name);
    }

    pfree(audit_ret);
}

static AuditResultInfo * audit_hit_get_result_info(void)
{
    return gAuditResultInfo;
}

static void audit_hit_set_result_info(AuditResultInfo * audit_ret)
{
    gAuditResultInfo = audit_ret;
}

static void audit_hit_find_action_index(AuditHitInfo * hit_info)
{// #lizard forgives
    int32 i = 0;

    if (hit_info == NULL)
    {
        return;
    }

    for (i = 0; i < audit_get_stmt_map_size(); i++)
    {
        AuditStmtMap * item = audit_get_stmt_map_by_index(i);
        bool is_object_type_hit = false;
        bool is_node_tag_hit = false;
        int32 j = 0;

        for (j = 0; j < Audit_MaxObjectTypePerStmt; j++)
        {
            ObjectType obj_type = item->obj_type[j];
            if (obj_type == hit_info->obj_type)
            {
                is_object_type_hit = true;
                break;
            }
            else if ((int)obj_type == Audit_InvalidEnum)
            {
                break;
            }
        }

        if (is_object_type_hit == false)
        {
            continue;
        }

        for (j = 0; j < Audit_MaxNodeTagPerStmt; j++)
        {
            NodeTag stmt_tag = item->stmt_tag[j];
            if (stmt_tag == hit_info->stmt_tag)
            {
                is_node_tag_hit = true;
                break;
            }
            else if ((int)stmt_tag == Audit_InvalidEnum)
            {
                break;
            }
        }

        if (is_object_type_hit &&
            is_node_tag_hit)
        {
            hit_info->l_hit_index = list_append_unique_int(hit_info->l_hit_index, (int)i);
            hit_info->l_hit_action = list_append_unique_ptr(hit_info->l_hit_action, (void *)item);
        }
    }

    Assert(list_length(hit_info->l_hit_index) == list_length(hit_info->l_hit_action));
}

static void audit_hit_free_hit_info(AuditHitInfo * hit_info)
{
    if (hit_info == NULL)
    {
        return;
    }

    if (hit_info->obj_name != NULL)
    {
        pfree(hit_info->obj_name);
    }

    if (hit_info->l_hit_index != NIL)
    {
        list_free(hit_info->l_hit_index);
    }

    if (hit_info->l_hit_action != NIL)
    {
        list_free(hit_info->l_hit_action);
    }

    if (hit_info->l_hit_match != NIL)
    {
        list_free(hit_info->l_hit_match);
    }

    if (hit_info->l_hit_audit != NIL)
    {
        list_free_deep(hit_info->l_hit_audit);
    }

    pfree(hit_info);
}

static AuditHitInfo * audit_hit_make_hit_info(NodeTag stmt_tag,
                                              Oid class_id,
                                              Oid object_id,
                                              int32 object_sub_id,
                                              ObjectType obj_type,
                                              char * obj_name)
{
    AuditHitInfo * hit_info = NULL;

    hit_info = palloc0(sizeof(AuditHitInfo));
    Assert(hit_info != NULL);

    hit_info->stmt_tag = stmt_tag;
    ObjectAddressSubSet(hit_info->obj_addr, 
                        class_id,
                        object_id,
                        object_sub_id);
    hit_info->obj_type = obj_type;
    hit_info->obj_name = pstrdup(obj_name != NULL ? obj_name : "unknown");
    hit_info->l_hit_index = NIL;
    hit_info->l_hit_action = NIL;
    hit_info->l_hit_match = NIL;
    hit_info->l_hit_audit = NIL;
    hit_info->is_success = false;

    audit_hit_find_action_index(hit_info);

    if (hit_info->l_hit_index == NIL)
    {
        audit_hit_free_hit_info(hit_info);
        return NULL;
    }

    return hit_info;
}

static AuditHitInfo * audit_hit_make_hit_info_2(AuditSQL id,
                                                NodeTag stmt_tag,
                                                Oid class_id,
                                                Oid object_id,
                                                int32 object_sub_id,
                                                ObjectType obj_type,
                                                char * obj_name)
{
    AuditHitInfo * hit_info = NULL;
    int32 i = 0;

    hit_info = palloc0(sizeof(AuditHitInfo));
    Assert(hit_info != NULL);

    hit_info->stmt_tag = stmt_tag;
    ObjectAddressSubSet(hit_info->obj_addr, 
                        class_id,
                        object_id,
                        object_sub_id);
    hit_info->obj_type = obj_type;
    hit_info->obj_name = pstrdup(obj_name != NULL ? obj_name : "unknown");
    hit_info->l_hit_index = NIL;
    hit_info->l_hit_action = NIL;
    hit_info->l_hit_match = NIL;
    hit_info->l_hit_audit = NIL;
    hit_info->is_success = false;

    for (i = 0; i < audit_get_stmt_map_size(); i++)
    {
        AuditStmtMap * item = audit_get_stmt_map_by_index(i);

        if (item->id == id)
        {
            hit_info->l_hit_index = list_append_unique_int(hit_info->l_hit_index, (int)i);
            hit_info->l_hit_action = list_append_unique_ptr(hit_info->l_hit_action, (void *)item);
        }
    }

    if (hit_info->l_hit_index == NIL)
    {
        audit_hit_free_hit_info(hit_info);
        return NULL;
    }

    return hit_info;
}


static bool audit_hit_find_seq_walker(Node * node, void * context)
{// #lizard forgives
    AuditHitFindObjectContext * hit_ctxt = (AuditHitFindObjectContext *) (context);

    if (node == NULL)
    {
        return false;
    }

    if (IsA(node, FuncExpr))
    {
        FuncExpr * func_expr = (FuncExpr *) node;
        Oid func_oid = func_expr->funcid;

        if (func_oid == F_NEXTVAL_OID ||
            func_oid == F_CURRVAL_OID ||
            func_oid == F_SETVAL_OID)
        {
            Const * func_arg = (Const *) linitial(func_expr->args);

            if (IsA(func_arg, Const) && 
                func_arg->consttype == REGCLASSOID)
            {
                Oid seqrel_oid = DatumGetObjectId(func_arg->constvalue);
                HeapTuple tp = SearchSysCache1(RELOID, ObjectIdGetDatum(seqrel_oid));

                if (HeapTupleIsValid(tp))
                {
                    Form_pg_class reltup = (Form_pg_class) GETSTRUCT(tp);

                    if (reltup->relkind != RELKIND_SEQUENCE)
                    {
                        /* do nothing */
                        Assert(reltup->relkind == RELKIND_SEQUENCE);
                    }
                    else
                    {
                        AuditHitInfo * hit_info = audit_hit_make_hit_info(hit_ctxt->stmt_tag,
                                                                          RelationRelationId,
                                                                          seqrel_oid,
                                                                          0,
                                                                          OBJECT_SEQUENCE,
                                                                          NameStr(reltup->relname));
                        if (hit_info != NULL)
                        {
                            hit_ctxt->l_hit_info = lappend(hit_ctxt->l_hit_info, (void *)hit_info);
                        }
                    }

                    ReleaseSysCache(tp);
                }
                else
                {
                    /* do nothing*/
                    Assert(HeapTupleIsValid(tp));
                }
            }
        }
    }

    return expression_tree_walker(node, audit_hit_find_seq_walker, context);
}

static List * audit_hit_find_object_info(Query * audit_query, 
                                         NodeTag stmt_tag)
{// #lizard forgives
    ListCell * l = NULL;
    AuditHitFindObjectContext hit_ctxt = { T_Invalid, NIL };

    hit_ctxt.stmt_tag = stmt_tag;
    hit_ctxt.l_hit_info = NIL;

    foreach(l, audit_query->rtable)
    {
        RangeTblEntry * rte = (RangeTblEntry *)(lfirst(l));
        RTEKind rtekind = rte->rtekind;

        switch (rtekind)
        {
            case RTE_RELATION:
            {
                Oid rel_oid = rte->relid;
                char rel_kind = rte->relkind;
                char * rel_name = get_rel_name(rel_oid);
                int32 rel_type = -1;

                if (rel_name == NULL)
                {
                    ereport(ERROR,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             (errmsg("cache lookup failed for relation %u", rel_oid))));
                }

                if (rel_kind == RELKIND_RELATION)
                {
                    rel_type = OBJECT_TABLE;
                }
                else if (rel_kind == RELKIND_VIEW)
                {
                    rel_type = OBJECT_VIEW;
                }
                else if (rel_kind == RELKIND_MATVIEW)
                {
                    rel_type = OBJECT_MATVIEW;
                }

                if (rel_type != -1)
                {
                    AuditHitInfo * hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                                                      RelationRelationId,
                                                                      rel_oid,
                                                                      0,
                                                                      rel_type,
                                                                      rel_name);
                    if (hit_info != NULL)
                    {
                        hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
                    }
                }
                break;
            }
            case RTE_SUBQUERY:
            {
                Query * subquery = rte->subquery;
                List * l_hit_info = audit_hit_read_query_tree(subquery);
                if (l_hit_info != NIL)
                {
                    hit_ctxt.l_hit_info = list_concat(hit_ctxt.l_hit_info, l_hit_info);
                }
                break;
            }
            case RTE_JOIN:
            {
                break;
            }
            case RTE_FUNCTION:
            {
                break;
            }
            case RTE_CTE:
            {
                if (rte->ctename != NULL)
                {
                    ListCell * lc = NULL;

                    foreach(lc, audit_query->cteList)
                    {
                        CommonTableExpr * cte = (CommonTableExpr *) lfirst(lc);

                        Assert(IsA(cte, CommonTableExpr));
                        if (pg_strcasecmp(cte->ctename, rte->ctename) == 0)
                        {
                            Query * ctequery = (Query *) (cte->ctequery);
                            List * l_hit_info = audit_hit_read_query_tree(ctequery);
                            if (l_hit_info != NIL)
                            {
                                hit_ctxt.l_hit_info = list_concat(hit_ctxt.l_hit_info, l_hit_info);
                            }
                            break;
                        }
                    }
                }
                break;
            }
            default:
            {
                break;
            }
        }
    }

    query_tree_walker(audit_query, audit_hit_find_seq_walker, &hit_ctxt, QTW_IGNORE_RANGE_TABLE);

    return hit_ctxt.l_hit_info;
}

static bool    audit_hit_object_missing_ok(bool src_missing_ok)
{
    if (use_object_missok)
    {
        return true;
    }

    return src_missing_ok;
}

static List * audit_hit_read_query_utility(Query * audit_query)
{// #lizard forgives
    Node * audit_stmt = (Node *) audit_query->utilityStmt;
    AuditHitFindObjectContext hit_ctxt = { T_Invalid, NIL };

    if (audit_query->utilityStmt == NULL)
    {
        return NIL;
    }

    hit_ctxt.stmt_tag = nodeTag(audit_stmt);
    hit_ctxt.l_hit_info = NIL;

    switch (nodeTag(audit_stmt))
    {
        case T_AlterDatabaseSetStmt:
        {
            /* AlterDatabaseSet */
            /*
             * ALTER DATABASE database_name SetResetClause
             */
            AlterDatabaseSetStmt * stmt = (AlterDatabaseSetStmt *) audit_stmt;

            char * object_name = NULL;
            ObjectAddress address = InvalidObjectAddress;

            AuditHitInfo * hit_info = NULL;

            object_name = stmt->dbname;
            address.classId = DatabaseRelationId;
            address.objectId = get_database_oid(object_name, audit_hit_object_missing_ok(false));
            address.objectSubId = InvalidOid;

            // audit for ALTER DATABASE
            hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                               address.classId,
                                               address.objectId,
                                               address.objectSubId,
                                               OBJECT_DATABASE,
                                               object_name);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        case T_AlterDatabaseStmt:
        {
            /* AlterDatabase */
            /*
             * ALTER DATABASE database_name WITH createdb_opt_list
             * ALTER DATABASE database_name createdb_opt_list
             * ALTER DATABASE database_name SET TABLESPACE name
             */
            AlterDatabaseStmt * stmt = (AlterDatabaseStmt *) audit_stmt;

            char * object_name = NULL;
            ObjectAddress address = InvalidObjectAddress;

            AuditHitInfo * hit_info = NULL;

            object_name = stmt->dbname;
            address.classId = DatabaseRelationId;
            address.objectId = get_database_oid(object_name, audit_hit_object_missing_ok(false));
            address.objectSubId = InvalidOid;

            // audit for ALTER DATABASE
            hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                               address.classId,
                                               address.objectId,
                                               address.objectSubId,
                                               OBJECT_DATABASE,
                                               object_name);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        case T_AlterExtensionContentsStmt:
        {
            /* ExecAlterExtensionContentsStmt */
            /*
             * ALTER EXTENSION name add_drop FUNCTION function_with_argtypes
             * ALTER EXTENSION name add_drop TABLE any_name
             * ALTER EXTENSION name add_drop SEQUENCE any_name
             * ALTER EXTENSION name add_drop VIEW any_name
             * ALTER EXTENSION name add_drop MATERIALIZED VIEW any_name
             * ALTER EXTENSION name add_drop TYPE_P Typename
             */
            AlterExtensionContentsStmt * stmt = (AlterExtensionContentsStmt *) audit_stmt;

            ObjectAddress extension = InvalidObjectAddress;
            ObjectAddress object = InvalidObjectAddress;
            Relation relation = NULL;

            AuditHitInfo * hit_info = NULL;

            extension.classId = ExtensionRelationId;
            extension.objectId = get_extension_oid(stmt->extname, audit_hit_object_missing_ok(false));
            extension.objectSubId = 0;

            object = get_object_address(stmt->objtype, 
                                        stmt->object,
                                        &relation,
                                        AccessShareLock, 
                                        false);
            if (relation != NULL)
            {
                relation_close(relation, NoLock);
            }

            if (false && stmt->action < 0)
            {
                // audit for DROP xxx
                char * obj_name = NULL;

                obj_name = get_object_name(stmt->objtype, stmt->object);
                hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                                    object.classId,
                                                    object.objectId,
                                                    object.objectSubId,
                                                    stmt->objtype,
                                                    obj_name);
                if (hit_info != NULL)
                {
                    hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
                }
            }

            // audit for ALTER EXTENSION
            hit_info = NULL;
            hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                               extension.classId,
                                               extension.objectId,
                                               extension.objectSubId,
                                               OBJECT_EXTENSION,
                                               stmt->extname);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        case T_AlterExtensionStmt:
        {
            /* ExecAlterExtensionStmt */
            /*
             * ALTER EXTENSION name UPDATE alter_extension_opt_list
             */
            AlterExtensionStmt * stmt = (AlterExtensionStmt *) audit_stmt;

            ObjectAddress extension = InvalidObjectAddress;

            AuditHitInfo * hit_info = NULL;

            extension.classId = ExtensionRelationId;
            extension.objectId = get_extension_oid(stmt->extname, audit_hit_object_missing_ok(false));
            extension.objectSubId = 0;

            // audit for ALTER EXTENSION
            hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                               extension.classId,
                                               extension.objectId,
                                               extension.objectSubId,
                                               OBJECT_EXTENSION,
                                               stmt->extname);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        case T_AlterFunctionStmt:
        {
            /* AlterFunction */
            /*
             * ALTER FUNCTION function_with_argtypes alterfunc_opt_list opt_restrict
             */
            AlterFunctionStmt * stmt = (AlterFunctionStmt *) audit_stmt;

            Oid funcOid = InvalidOid;
            char * funcName = NULL;

            AuditHitInfo * hit_info = NULL;

            funcOid = LookupFuncWithArgs(stmt->func, audit_hit_object_missing_ok(false));
            funcName = get_object_name(OBJECT_FUNCTION, (Node *)stmt->func);

            // audit for ALTER FUNCTION
            hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                               ProcedureRelationId,
                                               funcOid,
                                               0,
                                               OBJECT_FUNCTION,
                                               funcName);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        case T_AlterNodeStmt:
        {
            /* PgxcNodeAlter */
            /*
             * ALTER CLUSTER NODE pgxcnode_name OptWith
             * ALTER NODE pgxcnode_name OptWith
             */
            AlterNodeStmt * stmt = (AlterNodeStmt *) audit_stmt;

            AuditHitInfo * hit_info = NULL;

            // audit for ALTER NODE
            hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                               PgxcNodeRelationId,
                                               InvalidOid,
                                               InvalidOid,
                                               Audit_InvalidEnum,
                                               stmt->node_name);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        case T_AlterObjectDependsStmt:
        {
            /* ExecAlterObjectDependsStmt */
            /* 
             * ALTER FUNCTION function_with_argtypes DEPENDS ON EXTENSION name
             * ALTER TRIGGER name ON qualified_name DEPENDS ON EXTENSION name
             * ALTER MATERIALIZED VIEW qualified_name DEPENDS ON EXTENSION name
             * ALTER INDEX qualified_name DEPENDS ON EXTENSION name
             */
            AlterObjectDependsStmt * stmt = (AlterObjectDependsStmt *) audit_stmt;

            char * object_name = NULL;
            ObjectAddress address = InvalidObjectAddress;
            Relation rel = NULL;

            AuditHitInfo * hit_info = NULL;

            if (stmt->objectType == OBJECT_FUNCTION ||
                stmt->objectType == OBJECT_TRIGGER)
            {
                object_name = get_object_name(stmt->objectType, stmt->object);
            }
            else if (stmt->objectType == OBJECT_MATVIEW ||
                     stmt->objectType == OBJECT_INDEX)
            {
                object_name = RangeVarGetName(stmt->relation);
            }

            address = get_object_address_rv(stmt->objectType, stmt->relation, (List *) stmt->object,
                                            &rel, AccessShareLock, audit_hit_object_missing_ok(false));
            if (rel)
            {
                heap_close(rel, NoLock);
            }

            // audit for ALTER xxx 
            hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                               address.classId,
                                               address.objectId,
                                               address.objectSubId,
                                               stmt->objectType,
                                               object_name);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        case T_AlterObjectSchemaStmt:
        {
            /* ExecAlterObjectSchemaStmt */
            /*
             * ALTER EXTENSION name SET SCHEMA name
             * ALTER FUNCTION function_with_argtypes SET SCHEMA name
             * ALTER TABLE relation_expr SET SCHEMA name
             * ALTER TABLE IF_P EXISTS relation_expr SET SCHEMA name
             * ALTER SEQUENCE qualified_name SET SCHEMA name
             * ALTER SEQUENCE IF_P EXISTS qualified_name SET SCHEMA name
             * ALTER VIEW qualified_name SET SCHEMA name
             * ALTER VIEW IF_P EXISTS qualified_name SET SCHEMA name
             * ALTER MATERIALIZED VIEW qualified_name SET SCHEMA name
             * ALTER MATERIALIZED VIEW IF_P EXISTS qualified_name SET SCHEMA name
             * ALTER TYPE_P any_name SET SCHEMA name
             */
            AlterObjectSchemaStmt * stmt = (AlterObjectSchemaStmt *) audit_stmt;

            char * object_name = NULL;
            ObjectAddress address = InvalidObjectAddress;

            AuditHitInfo * hit_info = NULL;

            switch (stmt->objectType)
            {
                case OBJECT_EXTENSION:
                {
                    object_name = get_object_name(stmt->objectType, stmt->object);
                    address.classId = ExtensionRelationId;
                    address.objectId = get_extension_oid(object_name, audit_hit_object_missing_ok(false));
                    address.objectSubId = InvalidOid;
                    break;
                }
                case OBJECT_FOREIGN_TABLE:
                case OBJECT_SEQUENCE:
                case OBJECT_TABLE:
                case OBJECT_VIEW:
                case OBJECT_MATVIEW:
                {
                    object_name = RangeVarGetName(stmt->relation);
                    address.classId = RelationRelationId;
                    address.objectId = RangeVarGetRelid(stmt->relation, NoLock, audit_hit_object_missing_ok(stmt->missing_ok));
                    address.objectSubId = InvalidOid;
                    break;
                }
                case OBJECT_DOMAIN:
                case OBJECT_TYPE:
                {
                    List * names = castNode(List, stmt->object);
                    TypeName * typename = makeTypeNameFromNameList(names);

                    object_name = NameListToString(names);
                    address.classId = TypeRelationId;
                    address.objectId = LookupTypeNameOid(NULL, typename, audit_hit_object_missing_ok(false));
                    address.objectSubId = InvalidOid;
                    break;
                }
                /* generic code path */
                case OBJECT_AGGREGATE:
                case OBJECT_COLLATION:
                case OBJECT_CONVERSION:
                case OBJECT_FUNCTION:
                case OBJECT_OPERATOR:
                case OBJECT_OPCLASS:
                case OBJECT_OPFAMILY:
                case OBJECT_STATISTIC_EXT:
                case OBJECT_TSCONFIGURATION:
                case OBJECT_TSDICTIONARY:
                case OBJECT_TSPARSER:
                case OBJECT_TSTEMPLATE:
                {
                    Relation rel = NULL;

                    object_name = get_object_name(stmt->objectType, stmt->object);
                    address = get_object_address(stmt->objectType,
                                                  stmt->object,
                                                  &rel,
                                                 AccessShareLock,
                                                 audit_hit_object_missing_ok(false));
                    if (rel != NULL)
                    {
                        heap_close(rel, NoLock);
                    }
                    break;
                }
                default:
                {
                    elog(ERROR, "unrecognized AlterObjectSchemaStmt type: %d",
                             (int) stmt->objectType);
                    break;
                }
            }

            // audit for ALTER xxx 
            hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                               address.classId,
                                               address.objectId,
                                               address.objectSubId,
                                               stmt->objectType,
                                               object_name);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        case T_AlterOwnerStmt:
        {
            /* ExecAlterOwnerStmt */
            /*
             * ALTER AGGREGATE aggregate_with_argtypes OWNER TO RoleSpec
               * ALTER COLLATION any_name OWNER TO RoleSpec
               * ALTER CONVERSION_P any_name OWNER TO RoleSpec
               * ALTER DATABASE database_name OWNER TO RoleSpec
               * ALTER DOMAIN_P any_name OWNER TO RoleSpec
               * ALTER FUNCTION function_with_argtypes OWNER TO RoleSpec
               * ALTER opt_procedural LANGUAGE name OWNER TO RoleSpec
               * ALTER LARGE_P OBJECT_P NumericOnly OWNER TO RoleSpec
               * ALTER OPERATOR operator_with_argtypes OWNER TO RoleSpec
               * ALTER OPERATOR CLASS any_name USING access_method OWNER TO RoleSpec
               * ALTER OPERATOR FAMILY any_name USING access_method OWNER TO RoleSpec
               * ALTER SCHEMA name OWNER TO RoleSpec
               * ALTER TYPE_P any_name OWNER TO RoleSpec
               * ALTER TABLESPACE name OWNER TO RoleSpec
               * ALTER STATISTICS any_name OWNER TO RoleSpec
               * ALTER TEXT_P SEARCH DICTIONARY any_name OWNER TO RoleSpec
               * ALTER TEXT_P SEARCH CONFIGURATION any_name OWNER TO RoleSpec
               * ALTER FOREIGN DATA_P WRAPPER name OWNER TO RoleSpec
               * ALTER SERVER name OWNER TO RoleSpec
               * ALTER EVENT TRIGGER name OWNER TO RoleSpec
               * ALTER PUBLICATION name OWNER TO RoleSpec
               * ALTER SUBSCRIPTION name OWNER TO RoleSpec
             */
            AlterOwnerStmt * stmt = (AlterOwnerStmt *) audit_stmt;

            char * object_name = NULL;
            ObjectAddress address = InvalidObjectAddress;

            AuditHitInfo * hit_info = NULL;

            switch (stmt->objectType)
            {
                case OBJECT_DATABASE:
                {
                    object_name = get_object_name(stmt->objectType, stmt->object);
                    address.classId = DatabaseRelationId;
                    address.objectId = get_database_oid(object_name, audit_hit_object_missing_ok(false));
                    address.objectSubId = InvalidOid;
                    break;
                }
                case OBJECT_SCHEMA:
                {
                    object_name = get_object_name(stmt->objectType, stmt->object);
                    address.classId = NamespaceRelationId;
                    address.objectId = get_namespace_oid(object_name, audit_hit_object_missing_ok(false));
                    address.objectSubId = InvalidOid;
                    break;
                }
                case OBJECT_TYPE:
                case OBJECT_DOMAIN:        /* same as TYPE */
                {
                    List * names = castNode(List, stmt->object);
                    TypeName * typename = makeTypeNameFromNameList(names);

                    object_name = NameListToString(names);
                    address.classId = TypeRelationId;
                    address.objectId = LookupTypeNameOid(NULL, typename, audit_hit_object_missing_ok(false));
                    address.objectSubId = InvalidOid;
                    break;
                }
                case OBJECT_FDW:
                {
                    object_name = get_object_name(stmt->objectType, stmt->object);
                    address.classId = ForeignDataWrapperRelationId;
                    address.objectId = get_foreign_data_wrapper_oid(object_name, audit_hit_object_missing_ok(false));
                    address.objectSubId = InvalidOid;
                    break;
                }
                case OBJECT_FOREIGN_SERVER:
                {
                    object_name = get_object_name(stmt->objectType, stmt->object);
                    address.classId = ForeignServerRelationId;
                    address.objectId = get_foreign_server_oid(object_name, audit_hit_object_missing_ok(false));
                    address.objectSubId = InvalidOid;
                    break;
                }
                case OBJECT_EVENT_TRIGGER:
                {
                    object_name = get_object_name(stmt->objectType, stmt->object);
                    address.classId = EventTriggerRelationId;
                    address.objectId = get_event_trigger_oid(object_name, audit_hit_object_missing_ok(false));
                    address.objectSubId = InvalidOid;
                    break;
                }
                case OBJECT_PUBLICATION:
                {
                    object_name = get_object_name(stmt->objectType, stmt->object);
                    address.classId = PublicationRelationId;
                    address.objectId = get_publication_oid(object_name, audit_hit_object_missing_ok(false));
                    address.objectSubId = InvalidOid;
                    break;
                }
                case OBJECT_SUBSCRIPTION:
                {
                    object_name = get_object_name(stmt->objectType, stmt->object);
                    address.classId = SubscriptionRelationId;
                    address.objectId = get_subscription_oid(object_name, audit_hit_object_missing_ok(false));
                    address.objectSubId = InvalidOid;
                    break;
                }
                /* Generic cases */
                case OBJECT_AGGREGATE:
                case OBJECT_COLLATION:
                case OBJECT_CONVERSION:
                case OBJECT_FUNCTION:
                case OBJECT_LANGUAGE:
                case OBJECT_LARGEOBJECT:
                case OBJECT_OPERATOR:
                case OBJECT_OPCLASS:
                case OBJECT_OPFAMILY:
                case OBJECT_STATISTIC_EXT:
                case OBJECT_TABLESPACE:
                case OBJECT_TSDICTIONARY:
                case OBJECT_TSCONFIGURATION:
                {
                    Relation rel = NULL;

                    object_name = get_object_name(stmt->objectType, stmt->object);
                    address = get_object_address(stmt->objectType,
                                                  stmt->object,
                                                  &rel,
                                                 AccessShareLock,
                                                 audit_hit_object_missing_ok(false));
                    if (rel != NULL)
                    {
                        heap_close(rel, NoLock);
                    }

                    if (address.classId == LargeObjectRelationId)
                    {
                        address.classId = LargeObjectMetadataRelationId;
                    }
                    break;
                }
                default:
                {
                    elog(ERROR, "unrecognized AlterOwnerStmt type: %d",
                         (int) stmt->objectType);
                    break;
                }
            }

            // audit for ALTER xxx 
            hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                               address.classId,
                                               address.objectId,
                                               address.objectSubId,
                                               stmt->objectType,
                                               object_name);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        case T_AlterRoleSetStmt:
        {
            /* AlterRoleSet */
            /*
             * ALTER ROLE RoleSpec opt_in_database SetResetClause
             * ALTER ROLE ALL opt_in_database SetResetClause
             * ALTER USER RoleSpec opt_in_database SetResetClause
             * ALTER USER ALL opt_in_database SetResetClause
             */
            AlterRoleSetStmt * stmt = (AlterRoleSetStmt *) audit_stmt;

            char * role_name = NULL;

            AuditHitInfo * hit_info = NULL;

            if (stmt->role != NULL)
            {
                role_name = stmt->role->rolename;
            }

            if (stmt->stmt_type == ROLESTMT_ROLE)
            {
                // audit for ALTER ROLE
                hit_info = audit_hit_make_hit_info_2(AuditSql_Role_Alter,
                                                     hit_ctxt.stmt_tag,
                                                        AuthIdRelationId,
                                                        InvalidOid,
                                                        InvalidOid,
                                                        OBJECT_ROLE,
                                                        role_name);
            }
            else if (stmt->stmt_type == ROLESTMT_USER)
            {
                // audit for ALTER USER
                hit_info = audit_hit_make_hit_info_2(AuditSql_User_Alter,
                                                     hit_ctxt.stmt_tag,
                                                        AuthIdRelationId,
                                                        InvalidOid,
                                                        InvalidOid,
                                                        OBJECT_ROLE,
                                                        role_name);
            }

            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        case T_AlterRoleStmt:
        {
            /* AlterRole */
            /*
             * ALTER ROLE RoleSpec opt_with AlterOptRoleList
             * ALTER USER RoleSpec opt_with AlterOptRoleList
             * ALTER GROUP_P RoleSpec add_drop USER role_list
             */
            AlterRoleStmt * stmt = (AlterRoleStmt *) audit_stmt;

            char * role_name = NULL;

            if (stmt->role != NULL)
            {
                role_name = stmt->role->rolename;
            }

            if (stmt->stmt_type == ROLESTMT_ROLE)
            {
                AuditHitInfo * hit_info = NULL;

                // audit for ALTER ROLE
                hit_info = audit_hit_make_hit_info_2(AuditSql_Role_Alter,
                                                     hit_ctxt.stmt_tag,
                                                        AuthIdRelationId,
                                                        InvalidOid,
                                                        InvalidOid,
                                                        OBJECT_ROLE,
                                                        role_name);

                if (hit_info != NULL)
                {
                    hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
                }
            }
            else if (stmt->stmt_type == ROLESTMT_USER)
            {
                AuditHitInfo * hit_info = NULL;

                // audit for ALTER USER
                hit_info = audit_hit_make_hit_info_2(AuditSql_User_Alter,
                                                     hit_ctxt.stmt_tag,
                                                        AuthIdRelationId,
                                                        InvalidOid,
                                                        InvalidOid,
                                                        OBJECT_ROLE,
                                                        role_name);

                if (hit_info != NULL)
                {
                    hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
                }
            }
            else if (stmt->stmt_type == ROLESTMT_GROUP &&
                     stmt->action < 0)
            {
                // audit for ALTER GROUP_P RoleSpec DROP USER role_list
                DefElem  * defel = (DefElem *) linitial(stmt->options);
                
                if (IsA(defel->arg, List))
                {
                    List * roles = (List *)defel->arg;
                    ListCell * cell = NULL;

                    foreach(cell, roles)
                    {
                        RoleSpec * rolespec = (RoleSpec *)lfirst(cell);
                        AuditHitInfo * hit_info = NULL;

                        // audit for ALTER GROUP_P RoleSpec DROP USER role_list
                        hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                                           AuthIdRelationId,
                                                           InvalidOid,
                                                           InvalidOid,
                                                           OBJECT_ROLE,
                                                           rolespec->rolename);
                        if (hit_info != NULL)
                        {
                            hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
                        }
                    }
                }
            }

            break;
        }
        case T_AlterSeqStmt:
        {
            /* AlterSequence */
            /* 
             * ALTER SEQUENCE qualified_name SeqOptList
             * ALTER SEQUENCE IF_P EXISTS qualified_name SeqOptList
             */
            AlterSeqStmt * stmt = (AlterSeqStmt *) audit_stmt;

            Oid seq_oid = InvalidOid;
            char * seq_name = NULL;

            AuditHitInfo * hit_info = NULL;

            seq_oid = RangeVarGetRelid(stmt->sequence, NoLock, audit_hit_object_missing_ok(stmt->missing_ok));
            seq_name = RangeVarGetName(stmt->sequence);

            // audit for ALTER SEQUENCE
            hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                               RelationRelationId,
                                               seq_oid,
                                               0,
                                               OBJECT_SEQUENCE,
                                               seq_name);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        case T_AlterSystemStmt:
        {
            /* AlterSystemSetConfigFile */
            /*
             * ALTER SYSTEM_P SET generic_set
             * ALTER SYSTEM_P RESET generic_reset
             */
            AlterSystemStmt * stmt = (AlterSystemStmt *) audit_stmt;

            AuditHitInfo * hit_info = NULL;

            // audit for ALTER SYSTEM
            hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                               InvalidOid,
                                               InvalidOid,
                                               InvalidOid,
                                               Audit_InvalidEnum,
                                               stmt->setstmt->name);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        case T_AlterTableMoveAllStmt:
        {
            /* AlterTableMoveAll */
            /*
             * ALTER TABLE ALL IN_P TABLESPACE name SET TABLESPACE name opt_nowait
             * ALTER TABLE ALL IN_P TABLESPACE name OWNED BY role_list SET TABLESPACE name opt_nowait
             * ALTER INDEX ALL IN_P TABLESPACE name SET TABLESPACE name opt_nowait
             * ALTER INDEX ALL IN_P TABLESPACE name OWNED BY role_list SET TABLESPACE name opt_nowait
             * ALTER MATERIALIZED VIEW ALL IN_P TABLESPACE name SET TABLESPACE name opt_nowait
             * ALTER MATERIALIZED VIEW ALL IN_P TABLESPACE name OWNED BY role_list SET TABLESPACE name opt_nowait
             */
            AlterTableMoveAllStmt * stmt = (AlterTableMoveAllStmt *) audit_stmt;

            Oid nsp_oid = InvalidOid;
            StringInfo nsp_name = NULL;

            AuditHitInfo * hit_info = NULL;

            nsp_oid = get_tablespace_oid(stmt->orig_tablespacename, audit_hit_object_missing_ok(false));

            nsp_name = makeStringInfo();
            appendStringInfo(nsp_name, "from %s -> to %s", stmt->orig_tablespacename, stmt->new_tablespacename);

            // audit for ALTER xxx
            hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                               NamespaceRelationId,
                                               nsp_oid,
                                               InvalidOid,
                                               stmt->objtype,
                                               nsp_name->data);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        case T_AlterTableSpaceOptionsStmt:
        {
            /* AlterTableSpaceOptions */
            /*
             * ALTER TABLESPACE name SET reloptions
             * ALTER TABLESPACE name RESET reloptions
             */
            AlterTableSpaceOptionsStmt * stmt = (AlterTableSpaceOptionsStmt *) audit_stmt;

            Oid tablespace_oid = InvalidOid;

            AuditHitInfo * hit_info = NULL;

            tablespace_oid = get_tablespace_oid(stmt->tablespacename, audit_hit_object_missing_ok(false));

            // audit for ALTER TABLESPACE
            hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                               TableSpaceRelationId,
                                               tablespace_oid,
                                               0,
                                               OBJECT_TABLESPACE,
                                               stmt->tablespacename);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        case T_AlterTableStmt:
        {
            /* AlterTable */
            /*
             * ALTER TABLE relation_expr alter_table_cmds
             * ALTER TABLE IF_P EXISTS relation_expr alter_table_cmds
             * ALTER TABLE relation_expr partition_cmd
             * ALTER TABLE IF_P EXISTS relation_expr partition_cmd
             * ALTER INDEX qualified_name alter_table_cmds
             * ALTER INDEX IF_P EXISTS qualified_name alter_table_cmds
             * ALTER SEQUENCE qualified_name alter_table_cmds
             * ALTER SEQUENCE IF_P EXISTS qualified_name alter_table_cmds
             * ALTER VIEW qualified_name alter_table_cmds
             * ALTER VIEW IF_P EXISTS qualified_name alter_table_cmds
             * ALTER MATERIALIZED VIEW qualified_name alter_table_cmds
             * ALTER MATERIALIZED VIEW IF_P EXISTS qualified_name alter_table_cmds
             * ALTER TYPE_P any_name alter_type_cmds
             * ALTER FOREIGN TABLE relation_expr alter_table_cmds
             * ALTER FOREIGN TABLE IF_P EXISTS relation_expr alter_table_cmds
             */
            AlterTableStmt * stmt = (AlterTableStmt *) audit_stmt;

            Oid object_oid = InvalidOid;
            char * object_name = NULL;

            AuditHitInfo * hit_info = NULL;
            ListCell * lcmd = NULL;

            object_oid = RangeVarGetRelid(stmt->relation, AccessShareLock, audit_hit_object_missing_ok(stmt->missing_ok));
            object_name = RangeVarGetName(stmt->relation);

            // audit for ALTER xxx
            hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                               RelationRelationId,
                                               object_oid,
                                               0,
                                               stmt->relkind,
                                               object_name);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            /* 
             * process for T_AlterTableCmd
             * Add Partitions
             * Enable Trigger
             * Disable Trigger
             */
            foreach(lcmd, stmt->cmds)
            {
                AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lcmd);

                hit_info = NULL;
                switch (cmd->subtype)
                {
                    case AT_AddPartitions:    /* Partitions variants */
                    {
                        // audit for Add Partitions
                        hit_info = audit_hit_make_hit_info_2(AuditSql_Partition_AddPartitions,
                                                             cmd->type,
                                                                InvalidOid,
                                                                InvalidOid,
                                                                InvalidOid,
                                                                stmt->relkind,
                                                                object_name);
                        break;
                    }
                    case AT_EnableTrig:        /* ENABLE TRIGGER variants */
                    case AT_EnableAlwaysTrig:
                    case AT_EnableReplicaTrig:
                    case AT_EnableTrigAll:
                    case AT_EnableTrigUser:
                    {
                        // audit for ENABLE TRIGGER
                        hit_info = audit_hit_make_hit_info_2(AuditSql_Trigger_Enable,
                                                             cmd->type,
                                                                InvalidOid,
                                                                InvalidOid,
                                                                InvalidOid,
                                                                stmt->relkind,
                                                                object_name);
                        break;
                    }
                    case AT_DisableTrig:    /* DISABLE TRIGGER variants */
                    case AT_DisableTrigAll:
                    case AT_DisableTrigUser:
                    {
                        // audit for DISABLE TRIGGER
                        hit_info = audit_hit_make_hit_info_2(AuditSql_Trigger_Disable,
                                                             cmd->type,
                                                                InvalidOid,
                                                                InvalidOid,
                                                                InvalidOid,
                                                                stmt->relkind,
                                                                object_name);
                        break;
                    }
                    default:
                    {
                        break;
                    }
                }

                if (hit_info != NULL)
                {
                    hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
                }
            }

            break;
        }
        case T_AuditStmt:
        {
            /* AuditDefine */
            /*
             * audit_or_noaudit audit_stmt_list opt_when_success_or_not
             * audit_or_noaudit audit_stmt_list BY audit_user_list opt_when_success_or_not
             * audit_or_noaudit audit_stmt_list ON audit_obj_type audit_obj opt_when_success_or_not
             * audit_or_noaudit audit_stmt_list ON audit_obj opt_when_success_or_not
             */
            AuditStmt * stmt = (AuditStmt *) audit_stmt;

            AuditHitInfo * hit_info = NULL;

            if (isa_object_audit_type(stmt->audit_type) &&
                stmt->object_name != NULL)
            {
                char * object_name = NameListToString(stmt->object_name);
                ObjectAddress object_address = InvalidObjectAddress;
                ObjectType object_type = stmt->object_type;
                Relation relation = NULL;

                switch (object_type)
                {
                    case OBJECT_INDEX:
                    case OBJECT_SEQUENCE:
                    case OBJECT_TABLE:
                    case OBJECT_VIEW:
                    case OBJECT_MATVIEW:
                    case OBJECT_COLUMN:
                    {
                        object_address = get_object_address(object_type,
                                                             (Node *)stmt->object_name,
                                                             &relation,
                                                             AccessShareLock,
                                                             audit_hit_object_missing_ok(false));
                        if (relation != NULL)
                        {
                            relation_close(relation, NoLock);
                        }
                        break;
                    }
                    case OBJECT_FUNCTION:
                    {
                        List * lfoid = FunctionGetOidsByName(stmt->object_name);

                        /* noly supported the first function in current version */
                        ObjectAddressSubSet(object_address, ProcedureRelationId, linitial_oid(lfoid), 0);

                        list_free(lfoid);
                        break;
                    }
                    case OBJECT_TYPE:
                    {
                        TypeName * type_name = makeTypeNameFromNameList(stmt->object_name);
                        object_address = get_object_address(object_type,
                                                             (Node *)type_name,
                                                             &relation,
                                                             AccessShareLock,
                                                             audit_hit_object_missing_ok(false));
                        if (relation != NULL)
                        {
                            relation_close(relation, NoLock);
                        }

                        pfree(type_name);
                        break;
                    }
                    default:
                    {
                        ereport(ERROR,
                            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                             errmsg("unsupported object \"%s\" to audit",
                                     NameListToString(stmt->object_name))));
                        break;
                    }
                }

                // audit for Audit/NoAudit xxx
                hit_info = audit_hit_make_hit_info_2(AuditSql_Audit,
                                                     hit_ctxt.stmt_tag,
                                                        object_address.classId,
                                                        object_address.objectId,
                                                        object_address.objectSubId,
                                                        object_type,
                                                        object_name);
                if (hit_info != NULL)
                {
                    hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
                }

                // audit for Audit/NoAudit xxx
                hit_info = NULL;
                hit_info = audit_hit_make_hit_info_2(AuditSql_SystemAudit,
                                                     hit_ctxt.stmt_tag,
                                                        object_address.classId,
                                                        object_address.objectId,
                                                        object_address.objectSubId,
                                                        Audit_InvalidEnum,
                                                        object_name);
                if (hit_info != NULL)
                {
                    hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
                }
            }
            else
            {
                // audit for Audit/NoAudit xxx
                hit_info = audit_hit_make_hit_info_2(AuditSql_SystemAudit,
                                                     hit_ctxt.stmt_tag,
                                                        InvalidOid,
                                                        InvalidOid,
                                                        InvalidOid,
                                                        Audit_InvalidEnum,
                                                        NULL);
                if (hit_info != NULL)
                {
                    hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
                }
            }

            break;
        }
        case T_CleanAuditStmt:
        {
            /* AuditClean */
            /*
             * CLEAN ALL AUDIT
             * CLEAN STATEMENT AUDIT
             * CLEAN USER AUDIT
             * CLEAN USER AUDIT BY audit_user_list
             * CLEAN OBJECT_P AUDIT
             * CLEAN OBJECT_P AUDIT ON audit_obj_type audit_obj
             * CLEAN OBJECT_P AUDIT ON audit_obj
             * CLEAN UNKNOWN AUDIT
             */
            // CleanAuditStmt * stmt = (CleanAuditStmt *) audit_stmt;

            AuditHitInfo * hit_info = NULL;

            // audit for CLEAN xxx
            hit_info = audit_hit_make_hit_info_2(AuditSql_SystemAudit,
                                                 hit_ctxt.stmt_tag,
                                                    InvalidOid,
                                                    InvalidOid,
                                                    InvalidOid,
                                                    Audit_InvalidEnum,
                                                    NULL);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        case T_CommentStmt:
        {
            /* CommentObject */
            /*
             * COMMENT ON comment_type_any_name any_name IS comment_text
             * COMMENT ON comment_type_name name IS comment_text
             * COMMENT ON TYPE_P Typename IS comment_text
             * COMMENT ON FUNCTION function_with_argtypes IS comment_text
             * COMMENT ON TRIGGER name ON any_name IS comment_text
             */
            CommentStmt * stmt = (CommentStmt *) audit_stmt;

            char * object_name = NULL;
            ObjectType objtype = stmt->objtype;
            Relation relation = NULL;
            ObjectAddress address = InvalidObjectAddress;

            AuditHitInfo * hit_info = NULL;

            object_name = get_object_name(stmt->objtype, stmt->object);
            address = get_object_address(stmt->objtype, stmt->object,
                                          &relation, AccessShareLock, 
                                          audit_hit_object_missing_ok(false));

            if (stmt->objtype == OBJECT_COLUMN)
            {
                List * list_name = castNode(List, stmt->object);

                /* column not exists */
                if (relation == NULL && list_length(list_name) >= 2)
                {
                    List * relname = list_truncate(list_copy(list_name), list_length(list_name) - 1);
                    relation = relation_openrv_extended(makeRangeVarFromNameList(relname),
                                                        AccessShareLock, 
                                                        audit_hit_object_missing_ok(false));
                    if (relation != NULL)
                    {
                        ObjectAddressSet(address, RelationRelationId, RelationGetRelid(relation));
                        switch (relation->rd_rel->relkind)
                        {
                            case RELKIND_RELATION:
                            {
                                objtype = OBJECT_TABLE;
                                break;
                            }
                            case RELKIND_VIEW:
                            {
                                objtype = OBJECT_VIEW;
                                break;
                            }
                            case RELKIND_MATVIEW:
                            {
                                objtype = OBJECT_MATVIEW;
                                break;
                            }
                            default:
                            {
                                break;
                            }
                        }
                    }
                }
            }

            if (relation != NULL)
            {
                relation_close(relation, NoLock);
            }

            // audit for COMMENT ON xxx
            hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                               address.classId,
                                               address.objectId,
                                               address.objectSubId,
                                               objtype,
                                               object_name);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        case T_CompositeTypeStmt:
        {
            /* DefineCompositeType */
            /*
             * CREATE TYPE_P any_name AS '(' OptTableFuncElementList ')'
             */
            CompositeTypeStmt * stmt = (CompositeTypeStmt *) audit_stmt;

            char * type_name = NULL;

            AuditHitInfo * hit_info = NULL;

            type_name = RangeVarGetName(stmt->typevar);

            // audit for CREATE TYPE_P
            hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                               TypeRelationId,
                                               InvalidOid,
                                               InvalidOid,
                                               OBJECT_TYPE,
                                               type_name);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        case T_CreatedbStmt:
        {
            /* createdb */
            /*
             * CREATE DATABASE database_name opt_with createdb_opt_list
             */
            CreatedbStmt * stmt = (CreatedbStmt *) audit_stmt;

            char * object_name = NULL;
            ObjectAddress address = InvalidObjectAddress;

            AuditHitInfo * hit_info = NULL;

            object_name = stmt->dbname;
            address.classId = DatabaseRelationId;
            address.objectId = InvalidOid;
            address.objectSubId = InvalidOid;

            // audit for CREATE DATABASE
            hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                               address.classId,
                                               address.objectId,
                                               address.objectSubId,
                                               OBJECT_DATABASE,
                                               object_name);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }
            break;
        }
        case T_CreateEnumStmt:
        {
            /* DefineEnum */
            /*
             * CREATE TYPE_P any_name AS ENUM_P '(' opt_enum_val_list ')'
             */
            CreateEnumStmt * stmt = (CreateEnumStmt *) audit_stmt;

            char * enum_name = NULL;

            AuditHitInfo * hit_info = NULL;

            enum_name = NameListToString(stmt->typeName);

            // audit for CREATE TYPE_P
            hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                               TypeRelationId,
                                               InvalidOid,
                                               InvalidOid,
                                               OBJECT_TYPE,
                                               enum_name);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        case T_CreateExtensionStmt:
        {
            /* CreateExtension */
            /*
             * CREATE EXTENSION name opt_with create_extension_opt_list
             * CREATE EXTENSION IF_P NOT EXISTS name opt_with create_extension_opt_list
             */
            CreateExtensionStmt * stmt = (CreateExtensionStmt *) audit_stmt;

            AuditHitInfo * hit_info = NULL;

            // audit for CREATE EXTENSION
            hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                               ExtensionRelationId,
                                               InvalidOid,
                                               InvalidOid,
                                               OBJECT_EXTENSION,
                                               stmt->extname);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        case T_CreateFunctionStmt:
        {
            /* CreateFunction */
            /*
             * CREATE opt_or_replace FUNCTION func_name func_args_with_defaults            RETURNS func_return createfunc_opt_list opt_definition
             * CREATE opt_or_replace FUNCTION func_name func_args_with_defaults              RETURNS TABLE '(' table_func_column_list ')' createfunc_opt_list opt_definition
             * CREATE opt_or_replace FUNCTION func_name func_args_with_defaults              createfunc_opt_list opt_definition
             */
            CreateFunctionStmt * stmt = (CreateFunctionStmt *) audit_stmt;

            char * func_name = NULL;

            AuditHitInfo * hit_info = NULL;

            func_name = NameListToString(stmt->funcname);

            // audit for CREATE FUNCTION
            hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                               ProcedureRelationId,
                                               InvalidOid,
                                               InvalidOid,
                                               OBJECT_FUNCTION,
                                               func_name);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        case T_CreateGroupStmt:
        {
            /* PgxcGroupCreate */
            /*
             * CREATE NODE GROUP_P pgxcgroup_name WITH pgxcnodes
             * CREATE DEFAULT NODE GROUP_P pgxcgroup_name WITH pgxcnodes
             */
            CreateGroupStmt * stmt = (CreateGroupStmt *) audit_stmt;

            AuditHitInfo * hit_info = NULL;

            // audit for CREATE NODE GROUP
            hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                               PgxcGroupRelationId,
                                               InvalidOid,
                                               InvalidOid,
                                               Audit_InvalidEnum,
                                               stmt->group_name);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        case T_CreateNodeStmt:
        {
            /* PgxcNodeCreate */
            /*
             * CREATE NODE pgxcnode_name OptWith
             */
            CreateNodeStmt * stmt = (CreateNodeStmt *) audit_stmt;

            AuditHitInfo * hit_info = NULL;

            // audit for CREATE NODE
            hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                               PgxcNodeRelationId,
                                               InvalidOid,
                                               InvalidOid,
                                               Audit_InvalidEnum,
                                               stmt->node_name);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        case T_CreateRangeStmt:
        {
            /* DefineRange */
            /*
             * CREATE TYPE_P any_name AS RANGE definition
             */
            CreateRangeStmt * stmt = (CreateRangeStmt *) audit_stmt;

            char * range_name = NULL;

            AuditHitInfo * hit_info = NULL;

            range_name = NameListToString(stmt->typeName);

            // audit for CREATE TYPE_P
            hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                               TypeRelationId,
                                               InvalidOid,
                                               InvalidOid,
                                               OBJECT_TYPE,
                                               range_name);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        case T_CreateRoleStmt:
        {
            /* CreateRole */
            /*
             * CREATE ROLE RoleId opt_with OptRoleList
             * CREATE USER RoleId opt_with OptRoleList
             * CREATE GROUP_P RoleId opt_with OptRoleList
             */
            CreateRoleStmt * stmt = (CreateRoleStmt *) audit_stmt;

            AuditHitInfo * hit_info = NULL;

            if (stmt->stmt_type == ROLESTMT_ROLE)
            {
                // audit for CREATE ROLE
                hit_info = audit_hit_make_hit_info_2(AuditSql_Role_Create,
                                                     hit_ctxt.stmt_tag,
                                                        AuthIdRelationId,
                                                        InvalidOid,
                                                        InvalidOid,
                                                        OBJECT_ROLE,
                                                        stmt->role);
            }
            else if (stmt->stmt_type == ROLESTMT_USER)
            {
                // audit for CREATE USER
                hit_info = audit_hit_make_hit_info_2(AuditSql_User_Create,
                                                     hit_ctxt.stmt_tag,
                                                        AuthIdRelationId,
                                                        InvalidOid,
                                                        InvalidOid,
                                                        OBJECT_ROLE,
                                                        stmt->role);
            }
            else
            {
                // audit for CREATE GROUP
            }

            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        case T_CreateSchemaStmt:
        {
            /* CreateSchemaCommand */
            /*
             * CREATE SCHEMA OptSchemaName AUTHORIZATION RoleSpec OptSchemaEltList
             * CREATE SCHEMA ColId OptSchemaEltList
             * CREATE SCHEMA IF_P NOT EXISTS OptSchemaName AUTHORIZATION RoleSpec OptSchemaEltList
             * CREATE SCHEMA IF_P NOT EXISTS ColId OptSchemaEltList
             */
            CreateSchemaStmt * stmt = (CreateSchemaStmt *) audit_stmt;

            char * object_name = NULL;
            ObjectAddress address = InvalidObjectAddress;

            AuditHitInfo * hit_info = NULL;

            object_name = stmt->schemaname;
            address.classId = NamespaceRelationId;
            address.objectId = InvalidOid;
            address.objectSubId = InvalidOid;

            // audit for CREATE SCHEMA
            hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                               address.classId,
                                               address.objectId,
                                               address.objectSubId,
                                               OBJECT_SCHEMA,
                                               object_name);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }
            break;
        }
        case T_CreateSeqStmt:
        {
            /* DefineSequence */
            /*
             * CREATE OptTemp SEQUENCE qualified_name OptSeqOptList
             * CREATE OptTemp SEQUENCE IF_P NOT EXISTS qualified_name OptSeqOptList
             */
            CreateSeqStmt * stmt = (CreateSeqStmt *) audit_stmt;

            char * seq_name = NULL;

            AuditHitInfo * hit_info = NULL;

            seq_name = RangeVarGetName(stmt->sequence);

            // audit for CREATE SEQUENCE
            hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                               RelationRelationId,
                                               InvalidOid,
                                               InvalidOid,
                                               OBJECT_SEQUENCE,
                                               seq_name);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        case T_CreateShardStmt:
        {
            /* InitShardMap */
            /*
             * CREATE SHARDING GROUP_P Iconst IN_P Iconst
             * CREATE EXTENSION SHARDING GROUP_P Iconst IN_P Iconst
             * CREATE SHARDING  GROUP_P TO GROUP_P pgxcgroup_name
             * CREATE EXTENSION SHARDING GROUP_P TO GROUP_P pgxcgroup_name
             */
            CreateShardStmt * stmt = (CreateShardStmt *) audit_stmt;

            char * shard_name = NULL;

            AuditHitInfo * hit_info = NULL;

            if (stmt->members != NIL)
            {
                shard_name = NameListToString(stmt->members);
            }

            // audit for CREATE SHARDING GROUP
            hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                               PgxcShardMapRelationId,
                                               InvalidOid,
                                               InvalidOid,
                                               Audit_InvalidEnum,
                                               shard_name);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        case T_CreateStmt:
        {
            /* DefineRelation */
            /*
             * CREATE OptTemp TABLE qualified_name '(' OptTableElementList ')'            OptInherit OptPartitionSpec OptWith OnCommitOption OptTableSpace OptDistributeBy OptSubCluster
             * CREATE OptTemp TABLE IF_P NOT EXISTS qualified_name '('            OptTableElementList ')' OptInherit OptPartitionSpec OptWith            OnCommitOption OptTableSpace OptDistributeBy OptSubCluster
             * CREATE OptTemp TABLE qualified_name OF any_name            OptTypedTableElementList OptPartitionSpec OptWith OnCommitOption            OptTableSpace OptDistributeBy OptSubCluster
             * CREATE OptTemp TABLE IF_P NOT EXISTS qualified_name OF any_name            OptTypedTableElementList OptPartitionSpec OptWith OnCommitOption            OptTableSpace OptDistributeBy OptSubCluster
             * CREATE OptTemp TABLE qualified_name PARTITION OF qualified_name            OptTypedTableElementList ForValues OptPartitionSpec OptWith            OnCommitOption OptTableSpace
             * CREATE OptTemp TABLE IF_P NOT EXISTS qualified_name PARTITION OF            qualified_name OptTypedTableElementList ForValues OptPartitionSpec            OptWith OnCommitOption OptTableSpace
             */
            CreateStmt * stmt = (CreateStmt *) audit_stmt;

            char * tbl_name = NULL;

            AuditHitInfo * hit_info = NULL;

            tbl_name = RangeVarGetName(stmt->relation);

            // audit for CREATE TABLE
            hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                               RelationRelationId,
                                               InvalidOid,
                                               InvalidOid,
                                               OBJECT_TABLE,
                                               tbl_name);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        case T_CreateTableAsStmt:
        {
            /* ExecCreateTableAs */
            /*
             * CREATE OptTemp TABLE create_as_target AS SelectStmt opt_with_data
             * CREATE OptTemp TABLE IF_P NOT EXISTS create_as_target AS SelectStmt opt_with_data
             * CREATE OptNoLog MATERIALIZED VIEW create_mv_target AS SelectStmt opt_with_data
             * CREATE OptNoLog MATERIALIZED VIEW IF_P NOT EXISTS create_mv_target AS SelectStmt opt_with_data
             * CREATE OptTemp TABLE create_as_target AS                EXECUTE name execute_param_clause opt_with_data
             */
            CreateTableAsStmt * stmt = (CreateTableAsStmt *) audit_stmt;

            char * rel_name = NULL;

            AuditHitInfo * hit_info = NULL;

            rel_name = RangeVarGetName(stmt->into->rel);

            // audit for CREATE AS
            hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                               RelationRelationId,
                                               InvalidOid,
                                               InvalidOid,
                                               stmt->relkind,
                                               rel_name);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        case T_CreateTableSpaceStmt:
        {
            /* CreateTableSpace */
            /*
             * CREATE TABLESPACE name OptTableSpaceOwner LOCATION Sconst opt_reloptions
             */
            CreateTableSpaceStmt * stmt = (CreateTableSpaceStmt *) audit_stmt;

            AuditHitInfo * hit_info = NULL;

            // audit for CREATE TABLESPACE
            hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                               TableSpaceRelationId,
                                               InvalidOid,
                                               0,
                                               OBJECT_TABLESPACE,
                                               stmt->tablespacename);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        case T_CreateTrigStmt:
        {
            /* CreateTrigger */
            /*
             * CREATE TRIGGER name TriggerActionTime TriggerEvents ON            qualified_name TriggerReferencing TriggerForSpec TriggerWhen            EXECUTE PROCEDURE func_name '(' TriggerFuncArgs ')'
             * CREATE CONSTRAINT TRIGGER name AFTER TriggerEvents ON            qualified_name OptConstrFromTable ConstraintAttributeSpec            FOR EACH ROW TriggerWhen            EXECUTE PROCEDURE func_name '(' TriggerFuncArgs ')'
             * CREATE ASSERTION name CHECK '(' a_expr ')'            ConstraintAttributeSpec
             */
            CreateTrigStmt * stmt = (CreateTrigStmt *) audit_stmt;

            AuditHitInfo * hit_info = NULL;

            // audit for CREATE TRIGGER
            hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                               TriggerRelationId,
                                               InvalidOid,
                                               InvalidOid,
                                               OBJECT_TRIGGER,
                                               stmt->trigname);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        case T_DefineStmt:
        {
            /* DefineAggregate/DefineOperator/DefineType/DefineTSParser/DefineTSDictionary/DefineTSTemplate/DefineTSConfiguration/DefineCollation */
            /*
             * CREATE TYPE_P any_name definition
             * CREATE TYPE_P any_name
             * CREATE TYPE_P any_name AS '(' OptTableFuncElementList ')'
             * CREATE TYPE_P any_name AS ENUM_P '(' opt_enum_val_list ')'
             * CREATE TYPE_P any_name AS RANGE definition
             */
            DefineStmt *stmt = (DefineStmt *) audit_stmt;

            switch (stmt->kind)
            {
                case OBJECT_AGGREGATE:
                    break;
                case OBJECT_OPERATOR:
                    break;
                case OBJECT_TYPE:
                {
                    char * type_name = NameListToString(stmt->defnames);

                    AuditHitInfo * hit_info = NULL;

                    // audit for CREATE TYPE_P any_name
                    hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                                       TypeRelationId,
                                                       InvalidOid,
                                                       InvalidOid,
                                                       OBJECT_TYPE,
                                                       type_name);
                    if (hit_info != NULL)
                    {
                        hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
                    }

                    break;
                }
                case OBJECT_TSPARSER:
                    break;
                case OBJECT_TSDICTIONARY:
                    break;
                case OBJECT_TSTEMPLATE:
                    break;
                case OBJECT_TSCONFIGURATION:
                    break;
                case OBJECT_COLLATION:
                    break;
                default:
                    elog(ERROR, "unrecognized define stmt type: %d",
                         (int) stmt->kind);
                    break;
            }

            break;
        }
        case T_DropdbStmt:
        {
            /* dropdb */
            /*
             * DROP DATABASE database_name
             * DROP DATABASE IF_P EXISTS database_name
             */
            DropdbStmt * stmt = (DropdbStmt *) audit_stmt;

            char * object_name = NULL;
            ObjectAddress address = InvalidObjectAddress;

            AuditHitInfo * hit_info = NULL;

            object_name = stmt->dbname;
            address.classId = DatabaseRelationId;
            address.objectId = get_database_oid(object_name, audit_hit_object_missing_ok(stmt->missing_ok));
            address.objectSubId = InvalidOid;

            // audit for DROP DATABASE
            hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                               address.classId,
                                               address.objectId,
                                               address.objectSubId,
                                               OBJECT_DATABASE,
                                               object_name);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        case T_DropGroupStmt:
        {
            /* PgxcGroupRemove */
            /*
             * DROP NODE GROUP_P pgxcgroup_name
             */
            DropGroupStmt * stmt = (DropGroupStmt *) audit_stmt;

            AuditHitInfo * hit_info = NULL;

            // audit for DROP NODE GROUP
            hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                               PgxcGroupRelationId,
                                               InvalidOid,
                                               InvalidOid,
                                               Audit_InvalidEnum,
                                               stmt->group_name);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        case T_DropNodeStmt:
        {
            /* PgxcNodeRemove */
            /*
             * DROP NODE pgxcnode_name
             */
            DropNodeStmt * stmt = (DropNodeStmt *) audit_stmt;

            AuditHitInfo * hit_info = NULL;

            // audit for DROP NODE
            hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                               PgxcNodeRelationId,
                                               InvalidOid,
                                               InvalidOid,
                                               Audit_InvalidEnum,
                                               stmt->node_name);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        case T_DropRoleStmt:
        {
            /* DropRole */
            /*
             * DROP ROLE role_list
             * DROP ROLE IF_P EXISTS role_list
             * DROP USER role_list
             * DROP USER IF_P EXISTS role_list
             * DROP GROUP_P role_list
             * DROP GROUP_P IF_P EXISTS role_list
             */
            DropRoleStmt * stmt = (DropRoleStmt *) audit_stmt;

            ListCell * item = NULL;

            foreach(item, stmt->roles)
            {
                RoleSpec * rolspec = lfirst(item);
                AuditHitInfo * hit_info = NULL;

                if (rolspec->roletype != ROLESPEC_CSTRING)
                {
                    continue;
                }

                if (stmt->stmt_type == ROLESTMT_ROLE)
                {
                    // audit for DROP ROLE
                    hit_info = audit_hit_make_hit_info_2(AuditSql_Role_Drop,
                                                         hit_ctxt.stmt_tag,
                                                            AuthIdRelationId,
                                                            InvalidOid,
                                                            InvalidOid,
                                                            OBJECT_ROLE,
                                                            rolspec->rolename);
                }
                else if (stmt->stmt_type == ROLESTMT_USER)
                {
                    // audit for DROP USER
                    hit_info = audit_hit_make_hit_info_2(AuditSql_User_Drop,
                                                         hit_ctxt.stmt_tag,
                                                            AuthIdRelationId,
                                                            InvalidOid,
                                                            InvalidOid,
                                                            OBJECT_ROLE,
                                                            rolspec->rolename);
                }
                else
                {
                    // audit for DROP GROUP
                    continue;
                }

                if (hit_info != NULL)
                {
                    hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
                }
            }

            break;
        }
        case T_DropShardStmt:
        {
            /* DropShardMap_Node */
            /*
             * DROP SHARDING IN_P GROUP_P pgxcgroup_name
             */
            DropShardStmt * stmt = (DropShardStmt *) audit_stmt;

            AuditHitInfo * hit_info = NULL;

            // audit for DROP SHARDING IN GROUP
            hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                               PgxcShardMapRelationId,
                                               InvalidOid,
                                               InvalidOid,
                                               Audit_InvalidEnum,
                                               stmt->group_name);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        case T_DropStmt:
        {
            /* ExecDropStmt */
            /*
             * DROP ASSERTION name opt_drop_behavior
             * DROP drop_type_any_name IF_P EXISTS any_name_list opt_drop_behavior
             * DROP drop_type_any_name any_name_list opt_drop_behavior
             * DROP drop_type_name IF_P EXISTS name_list opt_drop_behavior
             * DROP drop_type_name name_list opt_drop_behavior
             * DROP drop_type_name_on_any_name name ON any_name opt_drop_behavior
             * DROP drop_type_name_on_any_name IF_P EXISTS name ON any_name opt_drop_behavior
             * DROP TYPE_P type_name_list opt_drop_behavior
             * DROP TYPE_P IF_P EXISTS type_name_list opt_drop_behavior
             * DROP INDEX CONCURRENTLY any_name_list opt_drop_behavior
             * DROP INDEX CONCURRENTLY IF_P EXISTS any_name_list opt_drop_behavior
             * DROP FUNCTION function_with_argtypes_list opt_drop_behavior
             * DROP FUNCTION IF_P EXISTS function_with_argtypes_list opt_drop_behavior
             */
            DropStmt * stmt = (DropStmt *) audit_stmt;

            ListCell * cell = NULL;

            foreach(cell, stmt->objects)
            {
                Node * object = lfirst(cell);
                char * object_name = NULL;
                ObjectAddress object_address = InvalidObjectAddress;
                Relation relation = NULL;

                AuditHitInfo * hit_info = NULL;

                object_name = get_object_name(stmt->removeType, object);
                object_address = get_object_address(stmt->removeType,
                                                    object,
                                                    &relation, 
                                                    AccessShareLock,
                                                    audit_hit_object_missing_ok(stmt->missing_ok));
                if (relation != NULL)
                {
                    relation_close(relation, NoLock);
                }

                hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                                   object_address.classId,
                                                   object_address.objectId,
                                                   object_address.objectSubId,
                                                   stmt->removeType,
                                                   object_name);
                if (hit_info != NULL)
                {
                    hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
                }
            }

            break;
        }
        case T_DropTableSpaceStmt:
        {
            /* DropTableSpace */
            /*
             * DROP TABLESPACE name
             * DROP TABLESPACE IF_P EXISTS name
             */
            DropTableSpaceStmt * stmt = (DropTableSpaceStmt *) audit_stmt;

            Oid tablespace_oid = InvalidOid;

            AuditHitInfo * hit_info = NULL;

            tablespace_oid = get_tablespace_oid(stmt->tablespacename, audit_hit_object_missing_ok(stmt->missing_ok));

            // audit for DROP TABLESPACE
            hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                               TableSpaceRelationId,
                                               tablespace_oid,
                                               0,
                                               OBJECT_TABLESPACE,
                                               stmt->tablespacename);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        case T_GrantStmt:
        {
            /* ExecuteGrantStmt */
            /*
             * GRANT privileges ON privilege_target TO grantee_list            opt_grant_grant_option
             * REVOKE privileges ON privilege_target            FROM grantee_list opt_drop_behavior
             * REVOKE GRANT OPTION FOR privileges ON privilege_target            FROM grantee_list opt_drop_behavior
             * ALTER DEFAULT PRIVILEGES DefACLOptionList DefACLAction
             * GRANT privileges ON defacl_privilege_target TO grantee_list            opt_grant_grant_option
             * REVOKE privileges ON defacl_privilege_target            FROM grantee_list opt_drop_behavior
             * REVOKE GRANT OPTION FOR privileges ON defacl_privilege_target            FROM grantee_list opt_drop_behavior
             */
            GrantStmt * stmt = (GrantStmt *) audit_stmt;

            switch (stmt->targtype)
            {
                case ACL_TARGET_OBJECT:
                {
                    // istmt.objects = objectNamesToOids(stmt->objtype, stmt->objects);
                    GrantObjectType objtype = stmt->objtype;
                    List * objnames = stmt->objects;
                    ListCell * cell = NULL;

                    Assert(objnames != NIL);

                    switch (objtype)
                    {
                        case ACL_OBJECT_RELATION:
                        {
                            foreach(cell, objnames)
                            {
                                RangeVar     * relvar = (RangeVar *) lfirst(cell);
                                char          * relname = RangeVarGetName(relvar);
                                Oid               relOid = RangeVarGetRelid(relvar, NoLock, audit_hit_object_missing_ok(false));
                                char           relkind = get_rel_relkind(relOid);
                                ObjectType     reltype = 0;

                                AuditHitInfo * hit_info = NULL;

                                switch (relkind)
                                {
                                    case RELKIND_RELATION:
                                    {
                                        reltype = OBJECT_TABLE;
                                        break;
                                    }
                                    case RELKIND_SEQUENCE:
                                    {
                                        reltype = OBJECT_SEQUENCE;
                                        break;
                                    }
                                    case RELKIND_VIEW:
                                    {
                                        reltype = OBJECT_VIEW;
                                        break;
                                    }
                                    case RELKIND_MATVIEW:
                                    {
                                        reltype = OBJECT_MATVIEW;
                                        break;
                                    }
                                    case RELKIND_PARTITIONED_TABLE:
                                    {
                                        reltype = OBJECT_TABLE;
                                        break;
                                    }
                                    default:
                                    {
                                        reltype = 0;
                                        break;
                                    }
                                }

                                if (reltype != 0)
                                {
                                    hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                                                       RelationRelationId,
                                                                       relOid,
                                                                       0,
                                                                       reltype,
                                                                       relname);
                                }
                                else
                                {
                                    hit_info = audit_hit_make_hit_info_2(AuditSql_SystemGrant,
                                                                         hit_ctxt.stmt_tag,
                                                                         RelationRelationId,
                                                                         relOid,
                                                                         0,
                                                                         Audit_InvalidEnum,
                                                                         relname);
                                }

                                if (hit_info != NULL)
                                {
                                    hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
                                }
                            }
                            break;
                        }
                        case ACL_OBJECT_SEQUENCE:
                        {
                            foreach(cell, objnames)
                            {
                                RangeVar   *relvar = (RangeVar *) lfirst(cell);
                                char        *relname = RangeVarGetName(relvar);
                                Oid            relOid = RangeVarGetRelid(relvar, NoLock, audit_hit_object_missing_ok(false));

                                AuditHitInfo * hit_info = NULL;

                                hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                                                   RelationRelationId,
                                                                   relOid,
                                                                   0,
                                                                   OBJECT_SEQUENCE,
                                                                   relname);
                                if (hit_info != NULL)
                                {
                                    hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
                                }
                            }
                            break;
                        }
                        case ACL_OBJECT_DATABASE:
                        {
                            foreach(cell, objnames)
                            {
                                char       *dbname = strVal(lfirst(cell));
                                Oid            dbid = get_database_oid(dbname, audit_hit_object_missing_ok(false));

                                AuditHitInfo * hit_info = NULL;

                                hit_info = audit_hit_make_hit_info_2(AuditSql_SystemGrant,
                                                                     hit_ctxt.stmt_tag,
                                                                     DatabaseRelationId,
                                                                     dbid,
                                                                     0,
                                                                     Audit_InvalidEnum,
                                                                     dbname);
                                if (hit_info != NULL)
                                {
                                    hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
                                }
                            }
                            break;
                        }
                        case ACL_OBJECT_DOMAIN:
                        case ACL_OBJECT_TYPE:
                        {
                            foreach(cell, objnames)
                            {
                                List       *typname = (List *) lfirst(cell);
                                Oid            oid = LookupTypeNameOid(NULL, makeTypeNameFromNameList(typname), 
                                                                    audit_hit_object_missing_ok(false));

                                AuditHitInfo * hit_info = NULL;

                                hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                                                   TypeRelationId,
                                                                   oid,
                                                                   0,
                                                                   OBJECT_TYPE,
                                                                   NameListToString(typname));
                                if (hit_info != NULL)
                                {
                                    hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
                                }
                            }
                            break;
                        }
                        case ACL_OBJECT_FUNCTION:
                        {
                            foreach(cell, objnames)
                            {
                                ObjectWithArgs     * func = (ObjectWithArgs *) lfirst(cell);
                                char             * funcname = get_object_name(OBJECT_FUNCTION, (Node *) func);
                                Oid                  funcid = LookupFuncWithArgs(func, audit_hit_object_missing_ok(false));

                                AuditHitInfo * hit_info = NULL;

                                hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                                                   ProcedureRelationId,
                                                                   funcid,
                                                                   0,
                                                                   OBJECT_FUNCTION,
                                                                   funcname);
                                if (hit_info != NULL)
                                {
                                    hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
                                }
                            }
                            break;
                        }
                        case ACL_OBJECT_LANGUAGE:
                        {
                            foreach(cell, objnames)
                            {
                                char       *langname = strVal(lfirst(cell));
                                Oid            oid = get_language_oid(langname, audit_hit_object_missing_ok(false));

                                AuditHitInfo * hit_info = NULL;

                                hit_info = audit_hit_make_hit_info_2(AuditSql_SystemGrant,
                                                                     hit_ctxt.stmt_tag,
                                                                     LanguageRelationId,
                                                                     oid,
                                                                     0,
                                                                     Audit_InvalidEnum,
                                                                     langname);
                                if (hit_info != NULL)
                                {
                                    hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
                                }
                            }
                            break;
                        }
                        case ACL_OBJECT_LARGEOBJECT:
                        {
                            foreach(cell, objnames)
                            {
                                Oid            lobjOid = oidparse(lfirst(cell));
                                char        lobName[64 + 1] = { 0 };

                                AuditHitInfo * hit_info = NULL;

                                if (false && !LargeObjectExists(lobjOid))
                                    ereport(ERROR,
                                            (errcode(ERRCODE_UNDEFINED_OBJECT),
                                             errmsg("large object %u does not exist",
                                                    lobjOid)));

                                snprintf(lobName, 64, "%u", lobjOid);
                                hit_info = audit_hit_make_hit_info_2(AuditSql_SystemGrant,
                                                                     hit_ctxt.stmt_tag,
                                                                     LargeObjectMetadataRelationId,
                                                                     lobjOid,
                                                                     0,
                                                                     Audit_InvalidEnum,
                                                                     lobName);
                                if (hit_info != NULL)
                                {
                                    hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
                                }
                            }
                            break;
                        }
                        case ACL_OBJECT_NAMESPACE:
                        {
                            foreach(cell, objnames)
                            {
                                char       *nspname = strVal(lfirst(cell));
                                Oid            oid = get_namespace_oid(nspname, audit_hit_object_missing_ok(false));

                                AuditHitInfo * hit_info = NULL;

                                hit_info = audit_hit_make_hit_info_2(AuditSql_SystemGrant,
                                                                     hit_ctxt.stmt_tag,
                                                                     NamespaceRelationId,
                                                                     oid,
                                                                     0,
                                                                     Audit_InvalidEnum,
                                                                     nspname);
                                if (hit_info != NULL)
                                {
                                    hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
                                }
                            }
                            break;
                        }
                        case ACL_OBJECT_TABLESPACE:
                        {
                            foreach(cell, objnames)
                            {
                                char       *spcname = strVal(lfirst(cell));
                                Oid            spcoid = get_tablespace_oid(spcname, audit_hit_object_missing_ok(false));

                                AuditHitInfo * hit_info = NULL;

                                hit_info = audit_hit_make_hit_info_2(AuditSql_SystemGrant,
                                                                     hit_ctxt.stmt_tag,
                                                                     TableSpaceRelationId,
                                                                     spcoid,
                                                                     0,
                                                                     Audit_InvalidEnum,
                                                                     spcname);
                                if (hit_info != NULL)
                                {
                                    hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
                                }
                            }
                            break;
                        }
                        case ACL_OBJECT_FDW:
                        {
                            foreach(cell, objnames)
                            {
                                char       *fdwname = strVal(lfirst(cell));
                                Oid            fdwid = get_foreign_data_wrapper_oid(fdwname, audit_hit_object_missing_ok(false));

                                AuditHitInfo * hit_info = NULL;

                                hit_info = audit_hit_make_hit_info_2(AuditSql_SystemGrant,
                                                                     hit_ctxt.stmt_tag,
                                                                     ForeignDataWrapperRelationId,
                                                                     fdwid,
                                                                     0,
                                                                     Audit_InvalidEnum,
                                                                     fdwname);
                                if (hit_info != NULL)
                                {
                                    hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
                                }
                            }
                            break;
                        }
                        case ACL_OBJECT_FOREIGN_SERVER:
                        {
                            foreach(cell, objnames)
                            {
                                char       *srvname = strVal(lfirst(cell));
                                Oid            srvid = get_foreign_server_oid(srvname, audit_hit_object_missing_ok(false));

                                AuditHitInfo * hit_info = NULL;

                                hit_info = audit_hit_make_hit_info_2(AuditSql_SystemGrant,
                                                                     hit_ctxt.stmt_tag,
                                                                     ForeignServerRelationId,
                                                                     srvid,
                                                                     0,
                                                                     Audit_InvalidEnum,
                                                                     srvname);
                                if (hit_info != NULL)
                                {
                                    hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
                                }
                            }
                            break;
                        }
                        default:
                        {
                            elog(ERROR, "unrecognized GrantStmt.objtype: %d",
                                 (int) objtype);
                            break;
                        }
                    }
                    break;
                }
                case ACL_TARGET_ALL_IN_SCHEMA:
                {
                    // istmt.objects = objectsInSchemaToOids(stmt->objtype, stmt->objects);
                    AuditHitInfo * hit_info = NULL;

                    hit_info = audit_hit_make_hit_info_2(AuditSql_SystemGrant,
                                                         hit_ctxt.stmt_tag,
                                                         InvalidOid,
                                                         InvalidOid,
                                                         InvalidOid,
                                                         Audit_InvalidEnum,
                                                         NULL);
                    // audit for GRANT/REVOKE xxx ON ALL xxx IN SCHEMA xxx TO ...
                    if (hit_info != NULL)
                    {
                        hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
                    }
                    break;
                }
                /* ACL_TARGET_DEFAULTS should not be seen here */
                default:
                {
                    elog(ERROR, "unrecognized GrantStmt.targtype: %d",
                         (int) stmt->targtype);
                    break;
                }
            }

            break;
        }
        case T_IndexStmt:
        {
            /* DefineIndex */
            /*
             * CREATE opt_unique INDEX opt_concurrently opt_index_name            ON qualified_name access_method_clause '(' index_params ')'            opt_reloptions OptTableSpace where_clause
             * CREATE opt_unique INDEX opt_concurrently IF_P NOT EXISTS index_name            ON qualified_name access_method_clause '(' index_params ')'            opt_reloptions OptTableSpace where_clause
             */
            IndexStmt * stmt = (IndexStmt *) audit_stmt;

            char * rel_name = NULL;
            Oid rel_oid = InvalidOid;

            AuditHitInfo * hit_info = NULL;

            rel_name = RangeVarGetName(stmt->relation);
            rel_oid = RangeVarGetRelid(stmt->relation, NoLock, audit_hit_object_missing_ok(false));

            // audit for CREATE INDEX xxx ON xxx
            hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                               RelationRelationId,
                                               rel_oid,
                                               0,
                                               Audit_InvalidEnum,
                                               rel_name);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        case T_LockStmt:
        {
            /* LockTableCommand */
            /*
             * LOCK_P opt_table relation_expr_list opt_lock opt_nowait
             */
            LockStmt * stmt = (LockStmt *) audit_stmt;
            ListCell * p = NULL;

            foreach(p, stmt->relations)
            {
                RangeVar * rv = (RangeVar *) lfirst(p);

                char * relname = NULL;     
                Oid reloid = InvalidOid;

                AuditHitInfo * hit_info = NULL;

                relname = RangeVarGetName(rv);
                reloid = RangeVarGetRelid(rv, NoLock, audit_hit_object_missing_ok(false));

                // audit for LOCK
                hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                                   RelationRelationId,
                                                   reloid,
                                                   0,
                                                   OBJECT_TABLE,
                                                   relname);
                if (hit_info != NULL)
                {
                    hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
                }
            }

            break;
        }
        case T_ReindexStmt:
        {
            /* ReindexIndex/ReindexTable/ReindexMultipleTables */
            /*
             * REINDEX reindex_target_type qualified_name
             * REINDEX reindex_target_multitable name
             * REINDEX '(' reindex_option_list ')' reindex_target_type qualified_name
             * REINDEX '(' reindex_option_list ')' reindex_target_multitable name
             */
            ReindexStmt *stmt = (ReindexStmt *) audit_stmt;

            StringInfo object_name = NULL;
            ObjectAddress address = InvalidObjectAddress;

            AuditHitInfo * hit_info = NULL;

            object_name = makeStringInfo();

            switch (stmt->kind)
            {
                case REINDEX_OBJECT_INDEX:
                {
                    Oid objectOid = RangeVarGetRelid(stmt->relation, NoLock, audit_hit_object_missing_ok(false));
                    appendStringInfo(object_name, "Index -> %s", RangeVarGetName(stmt->relation));
                    ObjectAddressSet(address, RelationRelationId, objectOid);
                    break;
                }
                case REINDEX_OBJECT_TABLE:
                {
                    Oid objectOid = RangeVarGetRelid(stmt->relation, NoLock, audit_hit_object_missing_ok(false));
                    appendStringInfo(object_name, "Table -> %s", RangeVarGetName(stmt->relation));
                    ObjectAddressSet(address, RelationRelationId, objectOid);
                    break;
                }
                case REINDEX_OBJECT_SCHEMA:
                {
                    Oid objectOid = get_namespace_oid(stmt->name, audit_hit_object_missing_ok(false));
                    appendStringInfo(object_name, "Schema -> %s", stmt->name);
                    ObjectAddressSet(address, NamespaceRelationId, objectOid);
                    break;
                }
                case REINDEX_OBJECT_SYSTEM:
                {
                    Oid objectOid = MyDatabaseId;
                    appendStringInfo(object_name, "System -> %s", stmt->name);
                    ObjectAddressSet(address, DatabaseRelationId, objectOid);
                    break;
                }
                case REINDEX_OBJECT_DATABASE:
                {
                    Oid objectOid = MyDatabaseId;
                    appendStringInfo(object_name, "Database -> %s", stmt->name);
                    ObjectAddressSet(address, DatabaseRelationId, objectOid);
                    break;
                }
                default:
                {
                    elog(ERROR, "unrecognized object type: %d",
                         (int) stmt->kind);
                    break;
                }
            }

            // audit for REINDEX
            hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                               address.classId,
                                               address.objectId,
                                               address.objectSubId,
                                               Audit_InvalidEnum,
                                               object_name->data);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        case T_RenameStmt:
        {
            /* ExecRenameStmt */
            /*
             * ALTER DATABASE database_name RENAME TO database_name
             * ALTER FUNCTION function_with_argtypes RENAME TO name
             * ALTER GROUP_P RoleId RENAME TO RoleId
             * ALTER SCHEMA name RENAME TO name
             * ALTER TABLE relation_expr RENAME TO name
             * ALTER TABLE IF_P EXISTS relation_expr RENAME TO name
             * ALTER SEQUENCE qualified_name RENAME TO name
             * ALTER SEQUENCE IF_P EXISTS qualified_name RENAME TO name
             * ALTER VIEW qualified_name RENAME TO name
             * ALTER VIEW IF_P EXISTS qualified_name RENAME TO name
             * ALTER MATERIALIZED VIEW qualified_name RENAME TO name
             * ALTER MATERIALIZED VIEW IF_P EXISTS qualified_name RENAME TO name
             * ALTER INDEX qualified_name RENAME TO name
             * ALTER INDEX IF_P EXISTS qualified_name RENAME TO name
             * ALTER TABLE relation_expr RENAME opt_column name TO name
             * ALTER TABLE IF_P EXISTS relation_expr RENAME opt_column name TO name
             * ALTER MATERIALIZED VIEW qualified_name RENAME opt_column name TO name
             * ALTER MATERIALIZED VIEW IF_P EXISTS qualified_name RENAME opt_column name TO name
             * ALTER TRIGGER name ON qualified_name RENAME TO name
             * ALTER ROLE RoleId RENAME TO RoleId
             * ALTER USER RoleId RENAME TO RoleId
             * ALTER TABLESPACE name RENAME TO name
             * ALTER TYPE_P any_name RENAME TO name
             * ALTER TYPE_P any_name RENAME ATTRIBUTE name TO name opt_drop_behavior
             */
            RenameStmt * stmt = (RenameStmt *) audit_stmt;

            char * object_name = NULL;
            ObjectAddress address = InvalidObjectAddress;

            AuditHitInfo * hit_info = NULL;

            switch (stmt->renameType)
            {
                case OBJECT_TABCONSTRAINT:
                {
                    object_name = RangeVarGetName(stmt->relation);
                    address.classId = RelationRelationId;
                    address.objectId = RangeVarGetRelid(stmt->relation, NoLock, audit_hit_object_missing_ok(stmt->missing_ok));
                    address.objectSubId = 0;                
                    break;
                }
                case OBJECT_DOMCONSTRAINT:
                {
                    TypeName * typename = makeTypeNameFromNameList(castNode(List, stmt->object));
                    
                    object_name = NameListToString(castNode(List, stmt->object));
                    address.classId = TypeRelationId;
                    address.objectId = LookupTypeNameOid(NULL, typename, audit_hit_object_missing_ok(false));
                    address.objectSubId = 0;
                    break;
                }
                case OBJECT_DATABASE:
                {
                    object_name = stmt->subname;
                    address.classId = DatabaseRelationId;
                    address.objectId = get_database_oid(stmt->subname, audit_hit_object_missing_ok(stmt->missing_ok));
                    address.objectSubId = InvalidOid;
                    break;
                }
                case OBJECT_ROLE:
                {
                    object_name = stmt->subname;
                    address.classId = AuthIdRelationId;
                    address.objectId = get_role_oid(stmt->subname, audit_hit_object_missing_ok(stmt->missing_ok));
                    address.objectSubId = InvalidOid;
                    break;
                }
                case OBJECT_SCHEMA:
                {
                    object_name = stmt->subname;
                    address.classId = NamespaceRelationId;
                    address.objectId = get_namespace_oid(stmt->subname, audit_hit_object_missing_ok(stmt->missing_ok));
                    address.objectSubId = InvalidOid;
                    break;
                }
                case OBJECT_TABLESPACE:
                {
                    object_name = stmt->subname;
                    address.classId = TableSpaceRelationId;
                    address.objectId = get_tablespace_oid(stmt->subname, audit_hit_object_missing_ok(stmt->missing_ok));
                    address.objectSubId = InvalidOid;
                    break;
                }
                case OBJECT_TABLE:
                case OBJECT_SEQUENCE:
                case OBJECT_VIEW:
                case OBJECT_MATVIEW:
                case OBJECT_INDEX:
                case OBJECT_FOREIGN_TABLE:
                {
                    object_name = RangeVarGetName(stmt->relation);
                    address.classId = RelationRelationId;
                    address.objectId = RangeVarGetRelid(stmt->relation, NoLock, audit_hit_object_missing_ok(stmt->missing_ok));
                    address.objectSubId = InvalidOid;
                    break;
                }
                case OBJECT_COLUMN:
                case OBJECT_ATTRIBUTE:
                {
                    StringInfo col_name =  makeStringInfo();
                    Oid relid = RangeVarGetRelid(stmt->relation, NoLock, audit_hit_object_missing_ok(stmt->missing_ok));
                    AttrNumber attnum = get_attnum(relid, stmt->subname);
                    
                    appendStringInfo(col_name, "%s.%s", RangeVarGetName(stmt->relation), stmt->subname);
                    object_name = col_name->data;
                    ObjectAddressSubSet(address, RelationRelationId, relid, attnum);
                    break;
                }
                case OBJECT_RULE:
                {
                    object_name = stmt->subname;
                    ObjectAddressSubSet(address, RewriteRelationId, InvalidOid, 0);
                    break;
                }
                case OBJECT_TRIGGER:
                {
                    Oid relid = RangeVarGetRelid(stmt->relation, NoLock, audit_hit_object_missing_ok(stmt->missing_ok));
                    Oid    tgoid = get_trigger_oid(relid, stmt->subname, audit_hit_object_missing_ok(stmt->missing_ok));

                    object_name = stmt->subname;
                    ObjectAddressSubSet(address, TriggerRelationId, tgoid, 0);
                    break;
                }
                case OBJECT_POLICY:
                {
                    Oid relid = RangeVarGetRelid(stmt->relation, NoLock, audit_hit_object_missing_ok(stmt->missing_ok));
                    Oid    pooid = get_relation_policy_oid(relid, stmt->subname, audit_hit_object_missing_ok(stmt->missing_ok));

                    object_name = stmt->subname;
                    ObjectAddressSubSet(address, PolicyRelationId, pooid, 0);
                    break;
                }
                case OBJECT_DOMAIN:
                case OBJECT_TYPE:
                {
                    List * names = castNode(List, stmt->object);
                    TypeName * typename = makeTypeNameFromNameList(names);
                    Oid typeOid = LookupTypeNameOid(NULL, typename, audit_hit_object_missing_ok(false));

                    object_name = NameListToString(names);
                    ObjectAddressSubSet(address, TypeRelationId, typeOid, 0);
                    break;
                }
                case OBJECT_AGGREGATE:
                case OBJECT_COLLATION:
                case OBJECT_CONVERSION:
                case OBJECT_EVENT_TRIGGER:
                case OBJECT_FDW:
                case OBJECT_FOREIGN_SERVER:
                case OBJECT_FUNCTION:
                case OBJECT_OPCLASS:
                case OBJECT_OPFAMILY:
                case OBJECT_LANGUAGE:
                case OBJECT_STATISTIC_EXT:
                case OBJECT_TSCONFIGURATION:
                case OBJECT_TSDICTIONARY:
                case OBJECT_TSPARSER:
                case OBJECT_TSTEMPLATE:
                case OBJECT_PUBLICATION:
                case OBJECT_SUBSCRIPTION:
                {
                    Relation relation = NULL;
    
                    object_name = get_object_name(stmt->renameType, stmt->object);
                    address = get_object_address(stmt->renameType,
                                                 stmt->object,
                                                 &relation,
                                                 AccessShareLock,
                                                 audit_hit_object_missing_ok(stmt->missing_ok));
                    if (relation != NULL)
                    {
                        relation_close(relation, NoLock);
                    }
                    break;
                }
                default:
                {
                    elog(ERROR, "unrecognized rename stmt type: %d",
                             (int) stmt->renameType);
                    break;
                }
            }

            if (stmt->renameType == OBJECT_ROLE)
            {
                if (stmt->stmt_type == ROLESTMT_ROLE)
                {
                    // audit for ALTER ROLE
                    hit_info = audit_hit_make_hit_info_2(AuditSql_Role_Alter,
                                                         hit_ctxt.stmt_tag,
                                                         address.classId,
                                                         address.objectId,
                                                         address.objectSubId,
                                                         stmt->renameType,
                                                         object_name);
                }
                else if (stmt->stmt_type == ROLESTMT_USER)
                {
                    // audit for ALTER USER
                    hit_info = audit_hit_make_hit_info_2(AuditSql_User_Alter,
                                                         hit_ctxt.stmt_tag,
                                                         address.classId,
                                                         address.objectId,
                                                         address.objectSubId,
                                                         stmt->renameType,
                                                         object_name);
                }
                else
                {
                    // audit for ALTER GROUP
                }
            }
            else
            {
                // audit for ALTER xxx RENAME
                ObjectType obj_type = stmt->renameType;
                if (obj_type == OBJECT_COLUMN)
                {
                    obj_type = stmt->relationType;
                    address.objectSubId = 0;
                }

                hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                                   address.classId,
                                                   address.objectId,
                                                   address.objectSubId,
                                                   obj_type,
                                                   object_name);
            }

            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        case T_TruncateStmt:
        {
            /* ExecuteTruncate */
            /*
             * TRUNCATE opt_table relation_expr_list opt_restart_seqs opt_drop_behavior
             */
            TruncateStmt * stmt = (TruncateStmt *) audit_stmt;

            ListCell * cell = NULL;

            foreach(cell, stmt->relations)
            {
                RangeVar * rv = lfirst(cell);
                char * rname = RangeVarGetName(rv);
                Oid roid = RangeVarGetRelid(rv, NoLock, audit_hit_object_missing_ok(false));

                AuditHitInfo * hit_info = NULL;
                
                // audit for TRUNCATE TABLE
                hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                                   RelationRelationId,
                                                   roid,
                                                   0,
                                                   OBJECT_TABLE,
                                                   rname);
                if (hit_info != NULL)
                {
                    hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
                }
            }

            break;
        }
        case T_ViewStmt:
        {
            /* DefineView */
            /*
             * CREATE OptTemp VIEW qualified_name opt_column_list opt_reloptions                AS SelectStmt opt_check_option
             * CREATE OR REPLACE OptTemp VIEW qualified_name opt_column_list opt_reloptions                AS SelectStmt opt_check_option
             * CREATE OptTemp RECURSIVE VIEW qualified_name '(' columnList ')' opt_reloptions                AS SelectStmt opt_check_option
             * CREATE OR REPLACE OptTemp RECURSIVE VIEW qualified_name '(' columnList ')' opt_reloptions                AS SelectStmt opt_check_option
             */
            ViewStmt * stmt = (ViewStmt *) audit_stmt;

            char * view_name = NULL;

            AuditHitInfo * hit_info = NULL;

            view_name = RangeVarGetName(stmt->view);

            // audit for CREATE VIEW
            hit_info = audit_hit_make_hit_info(hit_ctxt.stmt_tag,
                                               RelationRelationId,
                                               InvalidOid,
                                               InvalidOid,
                                               OBJECT_VIEW,
                                               view_name);
            if (hit_info != NULL)
            {
                hit_ctxt.l_hit_info = lappend(hit_ctxt.l_hit_info, (void *)hit_info);
            }

            break;
        }
        default:
        {
            break;
        }
    }

    return hit_ctxt.l_hit_info;
}

static List * audit_hit_read_query_tree(Query * audit_query)
{// #lizard forgives
    List * l_hit_info = NIL;

    if (audit_query == NULL ||
        !IsA(audit_query, Query))
    {
        return NIL;
    }

    switch (audit_query->commandType)
    {
        case CMD_SELECT:
        {
            l_hit_info = audit_hit_find_object_info(audit_query, T_SelectStmt);
            break;
        }
        case CMD_UPDATE:
        {
            l_hit_info = audit_hit_find_object_info(audit_query, T_UpdateStmt);
            break;
        }
        case CMD_INSERT:
        {
            l_hit_info = audit_hit_find_object_info(audit_query, T_InsertStmt);
            break;
        }
        case CMD_DELETE:
        {
            l_hit_info = audit_hit_find_object_info(audit_query, T_DeleteStmt);
            break;
        }
        case CMD_UTILITY:
        {
            l_hit_info = audit_hit_read_query_utility(audit_query);
            break;
        }
        case CMD_UNKNOWN:
        case CMD_NOTHING:
        {
            return NIL;
            break;
        }
    }

    return l_hit_info;
}

static void audit_hit_remove_dup_object(AuditResultInfo * audit_ret)
{// #lizard forgives
    List * l_src = NIL;
    List * l_des = NIL;

    ListCell * lc_src = NULL;
    ListCell * lc_des = NULL;

    if (audit_ret == NULL ||
        audit_ret->l_hit_info == NIL)
    {
        return;
    }

    l_src = audit_ret->l_hit_info;

    foreach(lc_src, l_src)
    {
        AuditHitInfo * hit_info_src = (AuditHitInfo *)lfirst(lc_src);
        bool should_remove = false;

        foreach(lc_des, l_des)
        {
            AuditHitInfo * hit_info_des = (AuditHitInfo *)lfirst(lc_des);

            if (hit_info_src->stmt_tag == hit_info_des->stmt_tag &&
                IsSameObjectAddress(&(hit_info_src->obj_addr),
                                    &(hit_info_des->obj_addr)) &&
                hit_info_src->obj_type == hit_info_des->obj_type)
            {
                if (hit_info_src->obj_name != NULL &&
                    hit_info_des->obj_name != NULL &&
                    pg_strcasecmp(hit_info_src->obj_name,
                                  hit_info_des->obj_name) == 0)
                {
                    should_remove = true;
                    break;
                }
                else if (hit_info_src->obj_name == NULL &&
                         hit_info_des->obj_name == NULL)
                {
                    should_remove = true;
                    break;
                }
            }
        }

        if (should_remove == false)
        {
            l_des = lappend(l_des, (void *) hit_info_src);
        }
        else
        {
            audit_hit_free_hit_info(hit_info_src);
        }
    }

    list_free(l_src);
    audit_ret->l_hit_info = l_des;
}

static void audit_hit_read_query_list(Port * port,
                                      const char * query_sring,
                                      List * l_parsetree)
{
    if (audit_hit_get_result_info() == NULL)
    {
        AuditResultInfo * audit_ret = audit_hit_init_result_info();
        Query * audit_query =  (Query *)linitial(l_parsetree);                 /* only audit first query tree */    

        audit_ret->qry_string = pstrdup(query_sring);
        audit_ret->cmd_tag = pstrdup(CreateCommandTag((Node *)audit_query));
        audit_ret->l_hit_info = audit_hit_read_query_tree(audit_query);

        audit_hit_remove_dup_object(audit_ret);

        if (audit_ret->l_hit_info == NIL)
        {
            audit_hit_free_result_info(audit_ret);
        }
        else
        {
            audit_hit_set_result_info(audit_ret);
        }
    }
}

static AuditMode audit_hit_get_reverse_mode(bool is_success)
{
    if (is_success)
    {
        return AuditMode_Fail;
    }

    return AuditMode_Success;
}

static bool audit_hit_match_in_pg_audit_o(AuditHitInfo * audit_hit,
                                             AuditSQL action_id,
                                             AuditMode reverse_mode)
{
    int32 sys_cacheid = InvalidSysCacheID;
    Oid sys_reloid = InvalidOid;
    Oid sys_indoid = InvalidOid;

    int sys_nkeys = 0;
    ScanKeyData skey[Audit_nSysScanKeys];

    Relation sys_rel = NULL;
    LOCKMODE lockmode = AccessShareLock;
    
    SysScanDesc sd = NULL;
    HeapTuple    tup = NULL;

    bool is_match = false;

    audit_get_cacheid_pg_audit_o(&(sys_cacheid), NULL);
    GetSysCacheInfo(sys_cacheid, 
                    &sys_reloid,
                    &sys_indoid,
                    NULL);

    MemSet(skey, 0, sizeof(skey));

    ScanKeyInit(&(skey[sys_nkeys]),
                Anum_audit_obj_conf_class_id,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(audit_hit->obj_addr.classId)); sys_nkeys++;
    ScanKeyInit(&(skey[sys_nkeys]),
                Anum_audit_obj_conf_object_id,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(audit_hit->obj_addr.objectId)); sys_nkeys++;
    ScanKeyInit(&(skey[sys_nkeys]),
                Anum_audit_obj_conf_object_sub_id,
                BTEqualStrategyNumber, F_INT4EQ,
                Int32GetDatum(audit_hit->obj_addr.objectSubId)); sys_nkeys++;
    ScanKeyInit(&(skey[sys_nkeys]),
                Anum_audit_obj_conf_action_id,
                BTEqualStrategyNumber, F_INT4EQ,
                Int32GetDatum(action_id)); sys_nkeys++;

    sys_rel = heap_open(sys_reloid, lockmode);
    sd = systable_beginscan(sys_rel, 
                            sys_indoid,
                            true,
                            NULL, 
                            sys_nkeys,
                            skey);

    while ((tup = systable_getnext(sd)) != NULL)
    {
        Form_audit_obj_conf pg_struct = (Form_audit_obj_conf)(GETSTRUCT(tup));
        AuditMode audit_mode = (AuditMode)pg_struct->action_mode;
        bool action_ison = pg_struct->action_ison;
        
        if (action_ison == true && 
            audit_mode != reverse_mode)
        {
            is_match = true;
            break;
        }
    }

    systable_endscan(sd);
    heap_close(sys_rel, lockmode);

    return is_match;
}

static bool audit_hit_match_in_pg_audit_d(AuditHitInfo * audit_hit,
                                             AuditSQL action_id,
                                             AuditMode reverse_mode)
{
    int32 sys_cacheid = InvalidSysCacheID;
    Oid sys_reloid = InvalidOid;
    Oid sys_indoid = InvalidOid;

    int sys_nkeys = 0;
    ScanKeyData skey[Audit_nSysScanKeys];

    Relation sys_rel = NULL;
    LOCKMODE lockmode = AccessShareLock;
    
    SysScanDesc sd = NULL;
    HeapTuple    tup = NULL;

    bool is_match = false;

    audit_get_cacheid_pg_audit_d((int32 *)&(sys_cacheid), NULL);
    GetSysCacheInfo(sys_cacheid, 
                    &sys_reloid,
                    &sys_indoid,
                    NULL);

    MemSet(skey, 0, sizeof(skey));

    ScanKeyInit(&(skey[sys_nkeys]),
                Anum_audit_obj_def_opts_action_id,
                BTEqualStrategyNumber, F_INT4EQ,
                Int32GetDatum(action_id)); sys_nkeys++;

    sys_rel = heap_open(sys_reloid, lockmode);
    sd = systable_beginscan(sys_rel, 
                            sys_indoid,
                            true,
                            NULL, 
                            sys_nkeys,
                            skey);

    while ((tup = systable_getnext(sd)) != NULL)
    {
        Form_audit_obj_def_opts pg_struct = (Form_audit_obj_def_opts)(GETSTRUCT(tup));
        AuditMode audit_mode = (AuditMode)pg_struct->action_mode;
        bool action_ison = pg_struct->action_ison;
        
        if (action_ison == true && 
            audit_mode != reverse_mode)
        {
            is_match = true;
            break;
        }
    }

    systable_endscan(sd);
    heap_close(sys_rel, lockmode);

    return is_match;
}

static bool audit_hit_match_in_pg_audit_u(AuditHitInfo * audit_hit,
                                             AuditSQL action_id,
                                             AuditMode reverse_mode)
{
    int32 sys_cacheid = InvalidSysCacheID;
    Oid sys_reloid = InvalidOid;
    Oid sys_indoid = InvalidOid;

    int sys_nkeys = 0;
    ScanKeyData skey[Audit_nSysScanKeys];

    Relation sys_rel = NULL;
    LOCKMODE lockmode = AccessShareLock;
    
    SysScanDesc sd = NULL;
    HeapTuple    tup = NULL;

    bool is_match = false;

    audit_get_cacheid_pg_audit_u((int32 *)&(sys_cacheid), NULL);
    GetSysCacheInfo(sys_cacheid, 
                    &sys_reloid,
                    &sys_indoid,
                    NULL);

    MemSet(skey, 0, sizeof(skey));

    ScanKeyInit(&(skey[sys_nkeys]),
                Anum_audit_user_conf_user_id,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(GetUserId())); sys_nkeys++;
    ScanKeyInit(&(skey[sys_nkeys]),
                Anum_audit_user_conf_action_id,
                BTEqualStrategyNumber, F_INT4EQ,
                Int32GetDatum(action_id)); sys_nkeys++;

    sys_rel = heap_open(sys_reloid, lockmode);
    sd = systable_beginscan(sys_rel, 
                            sys_indoid,
                            true,
                            NULL, 
                            sys_nkeys,
                            skey);

    while ((tup = systable_getnext(sd)) != NULL)
    {
        Form_audit_user_conf pg_struct = (Form_audit_user_conf)(GETSTRUCT(tup));
        AuditMode audit_mode = (AuditMode)pg_struct->action_mode;
        bool action_ison = pg_struct->action_ison;
        
        if (action_ison == true && 
            audit_mode != reverse_mode)
        {
            is_match = true;
            break;
        }
    }

    systable_endscan(sd);
    heap_close(sys_rel, lockmode);

    return is_match;
}

static bool audit_hit_match_in_pg_audit_s(AuditHitInfo * audit_hit,
                                             AuditSQL action_id,
                                             AuditMode reverse_mode)
{
    int32 sys_cacheid = InvalidSysCacheID;
    Oid sys_reloid = InvalidOid;
    Oid sys_indoid = InvalidOid;

    int sys_nkeys = 0;
    ScanKeyData skey[Audit_nSysScanKeys];

    Relation sys_rel = NULL;
    LOCKMODE lockmode = AccessShareLock;
    
    SysScanDesc sd = NULL;
    HeapTuple    tup = NULL;

    bool is_match = false;

    audit_get_cacheid_pg_audit_s((int32 *)&(sys_cacheid), NULL);
    GetSysCacheInfo(sys_cacheid, 
                    &sys_reloid,
                    &sys_indoid,
                    NULL);

    MemSet(skey, 0, sizeof(skey));

    ScanKeyInit(&(skey[sys_nkeys]),
                Anum_audit_stmt_conf_action_id,
                BTEqualStrategyNumber, F_INT4EQ,
                Int32GetDatum(action_id)); sys_nkeys++;

    sys_rel = heap_open(sys_reloid, lockmode);
    sd = systable_beginscan(sys_rel, 
                            sys_indoid,
                            true,
                            NULL, 
                            sys_nkeys,
                            skey);

    while ((tup = systable_getnext(sd)) != NULL)
    {
        Form_audit_stmt_conf pg_struct = (Form_audit_stmt_conf)(GETSTRUCT(tup));
        AuditMode audit_mode = (AuditMode)pg_struct->action_mode;
        bool action_ison = pg_struct->action_ison;
        
        if (action_ison == true && 
            audit_mode != reverse_mode)
        {
            is_match = true;
            break;
        }
    }

    systable_endscan(sd);
    heap_close(sys_rel, lockmode);

    return is_match;
}

static void audit_hit_rebuild_hit_info(AuditHitInfo * hit_info,
                                       int hit_index,
                                       AuditStmtMap * hit_action,
                                       int hit_match,
                                       char * hit_audit,
                                       bool is_success)
{// #lizard forgives
    if (hit_info == NULL ||
        hit_action == NULL ||
        hit_audit == NULL)
    {
        return;
    }

    if (hit_info->l_hit_index != NIL)
    {
        list_free(hit_info->l_hit_index);
        hit_info->l_hit_index = NIL;
    }

    if (hit_info->l_hit_action != NIL)
    {
        list_free(hit_info->l_hit_action);
        hit_info->l_hit_action = NIL;
    }

    if (hit_info->l_hit_match != NIL)
    {
        list_free(hit_info->l_hit_match);
        hit_info->l_hit_match = NIL;
    }

    if (hit_info->l_hit_audit != NIL)
    {
        list_free_deep(hit_info->l_hit_audit);
        hit_info->l_hit_audit = NIL;
    }

    hit_info->l_hit_index = lappend_int(hit_info->l_hit_index, hit_index);
    hit_info->l_hit_action = lappend(hit_info->l_hit_action, (void *)hit_action);
    hit_info->l_hit_match = lappend_int(hit_info->l_hit_match, hit_match);
    hit_info->l_hit_audit = lappend(hit_info->l_hit_audit, (void *)pstrdup(hit_audit));
    hit_info->is_success = is_success;

    Assert(list_length(hit_info->l_hit_index) == list_length(hit_info->l_hit_action));
    Assert(list_length(hit_info->l_hit_index) == list_length(hit_info->l_hit_match));
    Assert(list_length(hit_info->l_hit_index) == list_length(hit_info->l_hit_audit));
}

static void audit_hit_match_in_catalog(AuditHitInfo * audit_hit,
                                       bool is_success)
{// #lizard forgives
    ListCell * lc1 = NULL;
    ListCell * lc2 = NULL;

    AuditMode reverse_mode = audit_hit_get_reverse_mode(is_success);

    Assert(list_length(audit_hit->l_hit_index) == list_length(audit_hit->l_hit_action));

    forboth(lc1, audit_hit->l_hit_index, lc2, audit_hit->l_hit_action)
    {
        int    action_index = lfirst_int(lc1);
        AuditStmtMap * action_item = (AuditStmtMap *) lfirst(lc2);
        AuditSQL action_id = action_item->id;
        char * audit_type = "Unkown Audit";
        int is_match = 0;

        if (isa_object_action_id(action_id))
        {
            if (audit_hit_match_in_pg_audit_o(audit_hit, action_id, reverse_mode))
            {
                is_match = 1;
                audit_type = "Object Audit";
                audit_hit_rebuild_hit_info(audit_hit, action_index, action_item, is_match, audit_type, is_success);
                return;
                
            }
        }

        Assert(is_match == 0);
    }

    forboth(lc1, audit_hit->l_hit_index, lc2, audit_hit->l_hit_action)
    {
        int    action_index = lfirst_int(lc1);
        AuditStmtMap * action_item = (AuditStmtMap *) lfirst(lc2);
        AuditSQL action_id = action_item->id;
        char * audit_type = "Unkown Audit";
        int is_match = 0;

        if (isa_object_action_id(action_id))
        {
            if (audit_hit_match_in_pg_audit_d(audit_hit, action_id, reverse_mode))
            {
                is_match = 1;
                audit_type = "Object Default Audit";
                audit_hit_rebuild_hit_info(audit_hit, action_index, action_item, is_match, audit_type, is_success);
                return;
            }
        }

        Assert(is_match == 0);
    }

    forboth(lc1, audit_hit->l_hit_index, lc2, audit_hit->l_hit_action)
    {
        int    action_index = lfirst_int(lc1);
        AuditStmtMap * action_item = (AuditStmtMap *) lfirst(lc2);
        AuditSQL action_id = action_item->id;
        char * audit_type = "Unkown Audit";
        int is_match = 0;

        if (audituser() || /* audit all sql executed by audit_admin */
            audit_hit_match_in_pg_audit_u(audit_hit, action_id, reverse_mode))
        {
            is_match = 1;
            audit_type = "User Audit";
            audit_hit_rebuild_hit_info(audit_hit, action_index, action_item, is_match, audit_type, is_success);
            return;
        }

        Assert(is_match == 0);
    }

    forboth(lc1, audit_hit->l_hit_index, lc2, audit_hit->l_hit_action)
    {
        int    action_index = lfirst_int(lc1);
        AuditStmtMap * action_item = (AuditStmtMap *) lfirst(lc2);
        AuditSQL action_id = action_item->id;
        char * audit_type = "Unkown Audit";
        int is_match = 0;

        if (audit_hit_match_in_pg_audit_s(audit_hit, action_id, reverse_mode))
        {
            is_match = 1;
            audit_type = "Statement Audit";
            audit_hit_rebuild_hit_info(audit_hit, action_index, action_item, is_match, audit_type, is_success);
            return;
        }

        Assert(is_match == 0);
        audit_hit->l_hit_match = lappend_int(audit_hit->l_hit_match, is_match);
        audit_hit->l_hit_audit = lappend(audit_hit->l_hit_audit, (void *)pstrdup(audit_type));
    }

    Assert(list_length(audit_hit->l_hit_index) == list_length(audit_hit->l_hit_action));
    Assert(list_length(audit_hit->l_hit_index) == list_length(audit_hit->l_hit_match));
    Assert(list_length(audit_hit->l_hit_index) == list_length(audit_hit->l_hit_audit));
}

static void audit_hit_print_result_log(void)
{
    ListCell * l = NULL;
    AuditResultInfo * audit_ret = audit_hit_get_result_info();

    char start_time[128] = { 0 };
    char begin_time[128] = { 0 };
    char end_time[128] = { 0 };

    bool ignore_others = false;

    pg_strftime(start_time, sizeof(start_time),
                "%Y-%m-%d %H:%M:%S %Z",
                pg_localtime(&(audit_ret->proc_start_time), log_timezone));

    pg_strftime(begin_time, sizeof(begin_time),
                "%Y-%m-%d %H:%M:%S %Z",
                pg_localtime(&(audit_ret->qry_begin_time), log_timezone));

    pg_strftime(end_time, sizeof(end_time),
                "%Y-%m-%d %H:%M:%S %Z",
                pg_localtime(&(audit_ret->qry_end_time), log_timezone));

    foreach(l, audit_ret->l_hit_info)
    {
        AuditHitInfo * audit_hit = (AuditHitInfo *) lfirst(l);
        ListCell * lc1 = NULL;
        ListCell * lc2 = NULL;
        ListCell * lc3 = NULL;

        Assert(list_length(audit_hit->l_hit_index) == list_length(audit_hit->l_hit_action));
        Assert(list_length(audit_hit->l_hit_index) == list_length(audit_hit->l_hit_match));
        Assert(list_length(audit_hit->l_hit_index) == list_length(audit_hit->l_hit_audit));

        forthree(lc1, audit_hit->l_hit_action, lc2, audit_hit->l_hit_match, lc3, audit_hit->l_hit_audit)
        {
            AuditStmtMap * hit_aciton = (AuditStmtMap *) lfirst(lc1);
            int hit_match = lfirst_int(lc2);
            char * hit_audit = (char *) lfirst(lc3);
            char * exec_status = (audit_hit->is_success) ? "Exec Successfull" : "Exec Not Successfull";

            if (!hit_match)
            {
                continue;
            }

            /*
             * when hit both user audit and statement audit,
             * only print the first audit log, and ignore others
             */
            if (strcmp(hit_audit, "User Audit") == 0 ||
                strcmp(hit_audit, "Statement Audit") == 0)
            {
                if (ignore_others == true)
                {
                    continue;
                }
                else
                {
                    /* print the first, ignore others behind */
                    ignore_others = true;
                }
            }

            audit_log(
                "AuditOutMessage: "
                "AuditType: \"%s\", "                        // audit type
                "QueryString: \"%s\", "                        // qry_string
                "TopCommandTag: \"%s\", "                    // cmd_tag
                "DatabaseID: %u, "                            // db_id
                "DatabaseName: \"%s\", "                    // db_name
                "DatabaseUserID: %u, "                        // db_user_id
                "DatabaseUserName: \"%s\", "                // db_user_name
                "NodeOid: %u, "                                // node_oid
                "NodeName: \"%s\", "                        // node_name
                "NodeType: \"%s\", "                        // node_type
                "NodeHost: \"%s\", "                        // node_host
                "NodePort: %d, "                            // node_port
                "NodeOSUser: \"%s\", "                        // node_osuser
                "PostgresPID: %u, "                            // proc_pid
                "PostmasterPID: %u, "                        // proc_ppid,
                "BackendStartTime: \"%s\", "                // proc_start_time
                "QueryBeginTime: \"%s\", "                    // qry_start_time
                "QueryEndTime: \"%s\", "                    // qry_end_time
                "QueryIsSuccess: %d, "                        // is_success
                "ClientHost: \"%s\", "                        // client_host
                "ClientHostname: \"%s\", "                    // client_hostname
                "ClientPort: \"%s\", "                        // client_port
                "AppName: \"%s\", "                            // app_name,
                "ObjectClassID: %u, "                        // obj_addr.classId,
                "ObjectId: %u, "                            // obj_addr.objectId
                "ObjectSubId: %d, "                            // obj_addr.objectSubId
                "ObjectType: \"%s\", "                        // obj_type
                "ObjectName: \"%s\", "                        // obj_name
                "ActionID: %d, "                            // action_id
                "ActionName: \"%s\", "                        // action_name
                "ExecStatus: \"%s\" ",                        // exec_status

                hit_audit,                                    // audit type
                audit_ret->qry_string,                        // qry_string
                audit_ret->cmd_tag,                            // cmd_tag
                audit_ret->db_id,                            // db_id
                audit_ret->db_name,                            // db_name
                audit_ret->db_user_id,                        // db_user_id
                audit_ret->db_user_name,                    // db_user_name
                audit_ret->node_oid,                        // node_oid
                audit_ret->node_name,                        // node_name
                PGXCNodeTypeString(audit_ret->node_type),    // node_type
                audit_ret->node_host,                        // node_host
                audit_ret->node_port,                        // node_port
                audit_ret->node_osuser,                        // node_osuser
                audit_ret->proc_pid,                        // proc_pid
                audit_ret->proc_ppid,                        // proc_ppid,
                start_time,                                    // proc_start_time
                begin_time,                                    // qry_start_time
                end_time,                                    // qry_end_time
                audit_ret->is_success,                        // is_success
                audit_ret->client_host,                        // client_host
                audit_ret->client_hostname,                    // client_hostname
                audit_ret->client_port,                        // client_port
                audit_ret->app_name,                        // app_name,
                audit_hit->obj_addr.classId,                // obj_addr.classId,
                audit_hit->obj_addr.objectId,                // obj_addr.objectId
                audit_hit->obj_addr.objectSubId,            // obj_addr.objectSubId
                audit_object_type_string(audit_hit->obj_type,
                                         false),             // obj_type
                audit_hit->obj_name,                        // obj_name
                hit_aciton->id,                                // action_id
                hit_aciton->name,                            // action_name
                exec_status                                    // exec_status
                );
        }
    }
}

static void audit_hit_process_result_info(bool is_success)
{
    ListCell * l = NULL;
    AuditResultInfo * audit_ret = audit_hit_get_result_info();
    
    if (audit_ret == NULL)
    {
        return;
    }

    foreach(l, audit_ret->l_hit_info)
    {
        AuditHitInfo * audit_hit = (AuditHitInfo *) lfirst(l);
        audit_hit_match_in_catalog(audit_hit, is_success);
    }

    audit_ret->is_success = is_success;
    audit_ret->qry_end_time = (pg_time_t) time(NULL);
    audit_hit_print_result_log();
}

#endif

#ifdef Audit_004_For_Log

#endif


