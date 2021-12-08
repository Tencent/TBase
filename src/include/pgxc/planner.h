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
 * planner.h
 *        Externally declared locator functions
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/planner.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGXCPLANNER_H
#define PGXCPLANNER_H

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "pgxc/locator.h"
#include "tcop/dest.h"
#include "nodes/relation.h"


typedef enum
{
    COMBINE_TYPE_NONE,            /* it is known that no row count, do not parse */
    COMBINE_TYPE_SUM,            /* sum row counts (partitioned, round robin) */
    COMBINE_TYPE_SAME            /* expect all row counts to be the same (replicated write) */
}    CombineType;

/* For sorting within RemoteQuery handling */
/*
 * It is pretty much like Sort, but without Plan. We may use Sort later.
 */
typedef struct
{
    NodeTag        type;
    int            numCols;        /* number of sort-key columns */
    AttrNumber *sortColIdx;        /* their indexes in the target list */
    Oid           *sortOperators;    /* OIDs of operators to sort them by */
    Oid           *sortCollations;
    bool       *nullsFirst;        /* NULLS FIRST/LAST directions */
} SimpleSort;

/*
 * Determines if query has to be launched
 * on Coordinators only (SEQUENCE DDL),
 * on Datanodes (normal Remote Queries),
 * or on all Postgres-XC nodes (Utilities and DDL).
 */
typedef enum
{
    EXEC_ON_CURRENT,
    EXEC_ON_DATANODES,
    EXEC_ON_COORDS,
    EXEC_ON_ALL_NODES,
    EXEC_ON_NONE
} RemoteQueryExecType;

typedef enum
{
    EXEC_DIRECT_NONE,
    EXEC_DIRECT_LOCAL,
    EXEC_DIRECT_LOCAL_UTILITY,
    EXEC_DIRECT_UTILITY,
    EXEC_DIRECT_SELECT,
    EXEC_DIRECT_INSERT,
    EXEC_DIRECT_UPDATE,
    EXEC_DIRECT_DELETE
} ExecDirectType;

#ifdef __TBASE__
typedef enum UPSERT_ACTION
{
    UPSERT_NONE,
    UPSERT_SELECT,
    UPSERT_INSERT,
    UPSERT_UPDATE
} UPSERT_ACTION;
#endif
/*
 * Contains instructions on processing a step of a query.
 * In the prototype this will be simple, but it will eventually
 * evolve into a GridSQL-style QueryStep.
 */
typedef struct
{
    Scan            scan;
    ExecDirectType        exec_direct_type;    /* track if remote query is execute direct and what type it is */
    char            *sql_statement;
    ExecNodes        *exec_nodes;        /* List of Datanodes where to launch query */
    CombineType        combine_type;
    SimpleSort        *sort;
    bool            read_only;        /* do not use 2PC when committing read only steps */
    bool            force_autocommit;    /* some commands like VACUUM require autocommit mode */
    char            *statement;        /* if specified use it as a PreparedStatement name on Datanodes */
    char            *cursor;        /* if specified use it as a Portal name on Datanodes */
    int             rq_num_params;      /* number of parameters present in
                                           remote statement */
    Oid             *rq_param_types;    /* parameter types for the remote
                                           statement */
    RemoteQueryExecType    exec_type;
    int            reduce_level;        /* in case of reduced JOIN, it's level    */
    char            *outer_alias;
    char            *inner_alias;
    int            outer_reduce_level;
    int            inner_reduce_level;
    Relids            outer_relids;
    Relids            inner_relids;
    char            *inner_statement;
    char            *outer_statement;
    char            *join_condition;
    bool            has_row_marks;        /* Did SELECT had FOR UPDATE/SHARE? */
    bool            has_ins_child_sel_parent;    /* This node is part of an INSERT SELECT that
                                 * inserts into child by selecting from its parent */

    bool            rq_finalise_aggs;   /* Aggregates should be finalised at
                                           the 
                                         * Datanode */
    bool            rq_sortgroup_colno; /* Use resno for sort group references
                                         * instead of expressions */
    Query           *remote_query;  /* Query structure representing the query
                                       to be
                                     * sent to the datanodes */
    List            *base_tlist;    /* the targetlist representing the result
                                       of 
                                     * the query to be sent to the datanode */

    /*
     * Reference targetlist of Vars to match the Vars in the plan nodes on
     * coordinator to the corresponding Vars in the remote_query.  These
     * targetlists are used to while replacing/adding targetlist and quals in
     * the remote_query.
     */ 
    List            *coord_var_tlist;
    List            *query_var_tlist;
    bool            is_temp;
#ifdef __TBASE__
    /*
      * This part is used for 'insert...on onconflict do update' while the target
      * relation has unshippable triggers, we have to do the UPSERT on coordinator with
      * triggers. In order to make triggers work, we separate UPSERT into INSERT and
      * UPDATE.
         */
	Query           *forDeparse;      /* function statement */
    char            *sql_select;      /* select statement */
    char            *sql_select_base;
    bool            forUpadte;
    int             ss_num_params;
    Oid             *ss_param_types;
    char            *select_cursor;
    char            *sql_update;      /* update statement */
    int             su_num_params;
    Oid             *su_param_types;
    char            *update_cursor;
    UPSERT_ACTION   action;
    bool            dml_on_coordinator;
    AttrNumber        jf_ctid;
    AttrNumber        jf_xc_node_id;
    AttrNumber        jf_xc_wholerow;
    Bitmapset       *conflict_cols;

	Node			*parsetree;  /* to recognize subtxn cmds (savepoint, rollback to, release savepoint) */
	bool            is_set;      /* is SET statement ? */
	bool            ignore_tuple_desc; /* should ignore received tuple slot desc ? */
#endif
} RemoteQuery;

/*
 * Going to be a RemoteQuery replacement.
 * Submit left subplan to the nodes defined by the Distribution and combine
 * results.
 */
typedef struct
{
    Scan        scan;
    char         distributionType;
    AttrNumber    distributionKey;
    List        *distributionNodes;
    List        *distributionRestrict;
    List        *nodeList;
    bool         execOnAll;
    SimpleSort *sort;
    char       *cursor;
    int64       unique;
#ifdef __TBASE__
    /*
      * if gather is under remotesubplan, parallel worker can send tuples 
      * directly without gather node?
     */
    bool        parallelWorkerSendTuple; 
	/* params that generated by initplan */
	Bitmapset  *initPlanParams;
#endif

} RemoteSubplan;

/*
 * FQS_context
 * This context structure is used by the Fast Query Shipping walker, to gather
 * information during analysing query for Fast Query Shipping.
 */
typedef struct
{
    bool        sc_for_expr;        /* if false, the we are checking shippability
                                     * of the Query, otherwise, we are checking
                                     * shippability of a stand-alone expression.
                                     */
    Bitmapset    *sc_shippability;    /* The conditions for (un)shippability of the
                                     * query.
                                     */
    Query        *sc_query;            /* the query being analysed for FQS */
    int            sc_query_level;        /* level of the query */
    int            sc_max_varlevelsup;    /* maximum upper level referred to by any
                                     * variable reference in the query. If this
                                     * value is greater than 0, the query is not
                                     * shippable, if shipped alone.
                                     */
    ExecNodes    *sc_exec_nodes;        /* nodes where the query should be executed */
    ExecNodes    *sc_subquery_en;    /* ExecNodes produced by merging the ExecNodes
                                     * for individual subqueries. This gets
                                     * ultimately merged with sc_exec_nodes.
                                     */
} Shippability_context;

extern PlannedStmt *pgxc_direct_planner(Query *query, int cursorOptions,
                                        ParamListInfo boundParams);
extern List *AddRemoteQueryNode(List *stmts, const char *queryString,
                                RemoteQueryExecType remoteExecType);
extern PlannedStmt *pgxc_planner(Query *query, int cursorOptions,
                                         ParamListInfo boundParams);
extern ExecNodes *pgxc_is_query_shippable(Query *query, int query_level);

#ifdef __TBASE__
extern RangeTblEntry *make_dummy_remote_rte(char *relname, Alias *alias);
extern void pgxc_add_returning_list(RemoteQuery *rq, List *ret_list, int rel_index);

extern void pgxc_build_dml_statement(PlannerInfo *root, CmdType cmdtype,
                        Index resultRelationIndex, RemoteQuery *rqplan,
                        List *sourceTargetList, bool interval);
extern Expr *pgxc_set_en_expr(Oid tableoid, Index resultRelationIndex);

extern Expr *pgxc_set_sec_en_expr(Oid tableoid, Index resultRelationIndex);
#endif

#endif   /* PGXCPLANNER_H */
