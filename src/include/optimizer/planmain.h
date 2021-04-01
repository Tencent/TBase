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
 * planmain.h
 *      prototypes for various files in optimizer/plan
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/planmain.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PLANMAIN_H
#define PLANMAIN_H

#include "nodes/plannodes.h"
#include "nodes/relation.h"
#ifdef XCP
#include "pgxc/planner.h"
#endif

/* possible values for force_parallel_mode */
typedef enum
{
    FORCE_PARALLEL_OFF,
    FORCE_PARALLEL_ON,
    FORCE_PARALLEL_REGRESS
}            ForceParallelMode;

/* GUC parameters */
#define DEFAULT_CURSOR_TUPLE_FRACTION 0.1
extern double cursor_tuple_fraction;
extern int    force_parallel_mode;

#ifdef __TBASE__
extern int remote_subplan_depth;
extern List *groupOids;
extern bool enable_distributed_unique_plan;
extern bool has_cold_hot_table;
extern int min_workers_of_hashjon_gather;

#define INSERT_TRIGGER "tt_dn_in_"
#define UPDATE_TRIGGER "tt_dn_up_"
#define DELETE_TRIGGER "tt_dn_de_"
#endif

/* query_planner callback to compute query_pathkeys */
typedef void (*query_pathkeys_callback) (PlannerInfo *root, void *extra);

/*
 * prototypes for plan/planmain.c
 */
extern RelOptInfo *query_planner(PlannerInfo *root, List *tlist,
              query_pathkeys_callback qp_callback, void *qp_extra);

/*
 * prototypes for plan/planagg.c
 */
extern void preprocess_minmax_aggregates(PlannerInfo *root, List *tlist);

/*
 * prototypes for plan/createplan.c
 */
extern Plan *create_plan(PlannerInfo *root, Path *best_path);
extern ForeignScan *make_foreignscan(List *qptlist, List *qpqual,
                 Index scanrelid, List *fdw_exprs, List *fdw_private,
                 List *fdw_scan_tlist, List *fdw_recheck_quals,
                 Plan *outer_plan);
extern Plan *materialize_finished_plan(Plan *subplan);
extern bool is_projection_capable_path(Path *path);
extern bool is_projection_capable_plan(Plan *plan);

/* External use of these functions is deprecated: */
extern Sort *make_sort_from_sortclauses(List *sortcls, Plan *lefttree);
extern Agg *make_agg(List *tlist, List *qual,
         AggStrategy aggstrategy, AggSplit aggsplit,
         int numGroupCols, AttrNumber *grpColIdx, Oid *grpOperators,
         List *groupingSets, List *chain,
         double dNumGroups, Plan *lefttree);
extern Limit *make_limit(Plan *lefttree, Node *limitOffset, Node *limitCount,
						 int64 offset_est, int64 count_est, bool skipEarlyFinish);
extern RemoteSubplan *make_remotesubplan(PlannerInfo *root,
                   Plan *lefttree,
                   Distribution *resultDistribution,
                   Distribution *execDistribution,
                   List *pathkeys);

/*
 * prototypes for plan/initsplan.c
 */
extern int    from_collapse_limit;
extern int    join_collapse_limit;

extern void add_base_rels_to_query(PlannerInfo *root, Node *jtnode);
extern void build_base_rel_tlists(PlannerInfo *root, List *final_tlist);
extern void add_vars_to_targetlist(PlannerInfo *root, List *vars,
                       Relids where_needed, bool create_new_ph);
extern void find_lateral_references(PlannerInfo *root);
extern void create_lateral_join_info(PlannerInfo *root);
extern List *deconstruct_jointree(PlannerInfo *root);
extern void distribute_restrictinfo_to_rels(PlannerInfo *root,
                                RestrictInfo *restrictinfo);
extern void process_implied_equality(PlannerInfo *root,
                         Oid opno,
                         Oid collation,
                         Expr *item1,
                         Expr *item2,
                         Relids qualscope,
                         Relids nullable_relids,
                         Index security_level,
                         bool below_outer_join,
                         bool both_const);
extern RestrictInfo *build_implied_join_equality(Oid opno,
                            Oid collation,
                            Expr *item1,
                            Expr *item2,
                            Relids qualscope,
                            Relids nullable_relids,
                            Index security_level);
extern void match_foreign_keys_to_quals(PlannerInfo *root);

/*
 * prototypes for plan/analyzejoins.c
 */
extern List *remove_useless_joins(PlannerInfo *root, List *joinlist);
extern void reduce_unique_semijoins(PlannerInfo *root);
extern bool query_supports_distinctness(Query *query);
extern bool query_is_distinct_for(Query *query, List *colnos, List *opids);
extern bool innerrel_is_unique(PlannerInfo *root,
                   Relids outerrelids, RelOptInfo *innerrel,
                   JoinType jointype, List *restrictlist, bool force_cache);

/*
 * prototypes for plan/setrefs.c
 */
extern Plan *set_plan_references(PlannerInfo *root, Plan *plan);
extern void record_plan_function_dependency(PlannerInfo *root, Oid funcid);
extern void extract_query_dependencies(Node *query,
                           List **relationOids,
                           List **invalItems,
                           bool *hasRowSecurity);
#ifdef __TBASE__
extern bool contain_remote_subplan_walker(Node *node, void *context, bool include_cte);
extern Plan *create_remotedml_plan(PlannerInfo *root, Plan *topplan, CmdType cmdtyp);
extern bool partkey_match_index(Oid indexoid, AttrNumber partkey);
extern List *build_physical_tlist_with_sysattr(List *relation_tlist, List *physical_tlist);
extern char *get_internal_cursor(void);
#endif

#endif                            /* PLANMAIN_H */
