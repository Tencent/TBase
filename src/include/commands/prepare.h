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
 * prepare.h
 *      PREPARE, EXECUTE and DEALLOCATE commands, and prepared-stmt storage
 *
 *
 * Copyright (c) 2002-2017, PostgreSQL Global Development Group
 *
 * src/include/commands/prepare.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PREPARE_H
#define PREPARE_H

#include "commands/explain.h"
#include "datatype/timestamp.h"
#include "utils/plancache.h"

/*
 * The data structure representing a prepared statement.  This is now just
 * a thin veneer over a plancache entry --- the main addition is that of
 * a name.
 *
 * Note: all subsidiary storage lives in the referenced plancache entry.
 */
typedef struct
{
    /* dynahash.c requires key to be first field */
    char        stmt_name[NAMEDATALEN];
    CachedPlanSource *plansource;    /* the actual cached plan */
    bool        from_sql;        /* prepared via SQL, not FE/BE protocol? */
#ifdef XCP    
    bool        use_resowner;    /* does it use resowner for tracking? */
#endif    
    TimestampTz prepare_time;    /* the time when the stmt was prepared */
} PreparedStatement;

#ifdef PGXC
typedef struct
{
    /* dynahash.c requires key to be first field */
    char        stmt_name[NAMEDATALEN];
    int        number_of_nodes;    /* number of nodes where statement is active */
    int         dns_node_indices[0];        /* node ids where statement is active */
} DatanodeStatement;
#endif

/* Utility statements PREPARE, EXECUTE, DEALLOCATE, EXPLAIN EXECUTE */
extern void PrepareQuery(PrepareStmt *stmt, const char *queryString,
             int stmt_location, int stmt_len);
extern void ExecuteQuery(ExecuteStmt *stmt, IntoClause *intoClause,
             const char *queryString, ParamListInfo params,
             DestReceiver *dest, char *completionTag);
extern void DeallocateQuery(DeallocateStmt *stmt);
extern void ExplainExecuteQuery(ExecuteStmt *execstmt, IntoClause *into,
                    ExplainState *es, const char *queryString,
                    ParamListInfo params, QueryEnvironment *queryEnv);

/* Low-level access to stored prepared statements */
extern void StorePreparedStatement(const char *stmt_name,
                       CachedPlanSource *plansource,
                       bool from_sql,
					   bool use_resowner,
					   const char need_rewrite);
extern PreparedStatement *FetchPreparedStatement(const char *stmt_name,
                       bool throwError);
extern void DropPreparedStatement(const char *stmt_name, bool showError);
extern TupleDesc FetchPreparedStatementResultDesc(PreparedStatement *stmt);
extern List *FetchPreparedStatementTargetList(PreparedStatement *stmt);

extern void DropAllPreparedStatements(void);

#ifdef PGXC
extern DatanodeStatement *FetchDatanodeStatement(const char *stmt_name, bool throwError);
extern bool ActivateDatanodeStatementOnNode(const char *stmt_name, int nodeidx);
extern bool HaveActiveDatanodeStatements(void);
extern void DropDatanodeStatement(const char *stmt_name);
extern void InactivateDatanodeStatementOnNode(int nodeidx);
extern int SetRemoteStatementName(Plan *plan, const char *stmt_name, int num_params,
                        Oid *param_types, int n);
#endif

#ifdef __TBASE__
extern void PrepareRemoteDMLStatement(bool upsert, char *stmt, 
                                    char *select_stmt, char *update_stmt);

extern void DropRemoteDMLStatement(char *stmt, char *update_stmt);
extern void RebuildDatanodeQueryHashTable(void);
#endif

#endif                            /* PREPARE_H */
