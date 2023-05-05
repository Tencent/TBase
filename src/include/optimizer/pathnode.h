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
 * pathnode.h
 *      prototypes for pathnode.c, relnode.c.
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/pathnode.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PATHNODE_H
#define PATHNODE_H

#include "nodes/relation.h"


/*
 * prototypes for pathnode.c
 */
extern int compare_path_costs(Path *path1, Path *path2,
                CostSelector criterion);
extern int compare_fractional_path_costs(Path *path1, Path *path2,
                              double fraction);
extern void set_cheapest(RelOptInfo *parent_rel);
extern void add_path(RelOptInfo *parent_rel, Path *new_path);
extern bool add_path_precheck(RelOptInfo *parent_rel,
                  Cost startup_cost, Cost total_cost,
                  List *pathkeys, Relids required_outer);
extern void add_partial_path(RelOptInfo *parent_rel, Path *new_path);
extern bool add_partial_path_precheck(RelOptInfo *parent_rel,
                          Cost total_cost, List *pathkeys);

extern Path *create_seqscan_path(PlannerInfo *root, RelOptInfo *rel,
                    Relids required_outer, int parallel_workers);
extern Path *create_samplescan_path(PlannerInfo *root, RelOptInfo *rel,
                        Relids required_outer);
extern IndexPath *create_index_path(PlannerInfo *root,
                  IndexOptInfo *index,
                  List *indexclauses,
                  List *indexclausecols,
                  List *indexorderbys,
                  List *indexorderbycols,
                  List *pathkeys,
                  ScanDirection indexscandir,
                  bool indexonly,
                  Relids required_outer,
                  double loop_count,
                  bool partial_path);
extern BitmapHeapPath *create_bitmap_heap_path(PlannerInfo *root,
                        RelOptInfo *rel,
                        Path *bitmapqual,
                        Relids required_outer,
                        double loop_count,
                        int parallel_degree);
extern BitmapAndPath *create_bitmap_and_path(PlannerInfo *root,
                        RelOptInfo *rel,
                        List *bitmapquals);
extern BitmapOrPath *create_bitmap_or_path(PlannerInfo *root,
                      RelOptInfo *rel,
                      List *bitmapquals);
extern TidPath *create_tidscan_path(PlannerInfo *root, RelOptInfo *rel,
                    List *tidquals, Relids required_outer);
extern AppendPath *create_append_path(RelOptInfo *rel, List *subpaths,
                    Relids required_outer, int parallel_workers,
                    List *partitioned_rels);
extern MergeAppendPath *create_merge_append_path(PlannerInfo *root,
                          RelOptInfo *rel,
                          List *subpaths,
                          List *pathkeys,
                          Relids required_outer,
                          List *partitioned_rels);
extern QualPath *create_qual_path(PlannerInfo *root, Path *subpath, List *quals);
extern ResultPath *create_result_path(PlannerInfo *root, RelOptInfo *rel,
                    PathTarget *target, List *resconstantqual);
extern MaterialPath *create_material_path(RelOptInfo *rel, Path *subpath);
extern UniquePath *create_unique_path(PlannerInfo *root, RelOptInfo *rel,
                    Path *subpath, SpecialJoinInfo *sjinfo);
extern GatherPath *create_gather_path(PlannerInfo *root,
                    RelOptInfo *rel, Path *subpath, PathTarget *target,
                    Relids required_outer, double *rows);
extern GatherMergePath *create_gather_merge_path(PlannerInfo *root,
                          RelOptInfo *rel,
                          Path *subpath,
                          PathTarget *target,
                          List *pathkeys,
                          Relids required_outer,
                          double *rows);
extern SubqueryScanPath *create_subqueryscan_path(PlannerInfo *root,
                          RelOptInfo *rel, Path *subpath, List *pathkeys,
                          Relids required_outer, Distribution *distribution);
extern Path *create_functionscan_path(PlannerInfo *root, RelOptInfo *rel,
                          List *pathkeys, Relids required_outer);
extern Path *create_tablexprscan_path(PlannerInfo *root, RelOptInfo *rel,
                          List *pathkeys, Relids required_outer);
extern Path *create_valuesscan_path(PlannerInfo *root, RelOptInfo *rel,
                        Relids required_outer);
extern Path *create_tablefuncscan_path(PlannerInfo *root, RelOptInfo *rel,
                          Relids required_outer);
extern Path *create_ctescan_path(PlannerInfo *root, RelOptInfo *rel,
                    Relids required_outer);
extern Path *create_namedtuplestorescan_path(PlannerInfo *root, RelOptInfo *rel,
                                Relids required_outer);
extern Path *create_worktablescan_path(PlannerInfo *root, RelOptInfo *rel,
                          Relids required_outer);
extern ForeignPath *create_foreignscan_path(PlannerInfo *root, RelOptInfo *rel,
                        PathTarget *target,
                        double rows, Cost startup_cost, Cost total_cost,
                        List *pathkeys,
                        Relids required_outer,
                        Path *fdw_outerpath,
                        List *fdw_private);

extern Relids calc_nestloop_required_outer(Relids outerrelids,
							 Relids outer_paramrels,
							 Relids innerrelids,
							 Relids inner_paramrels);
extern Relids calc_non_nestloop_required_outer(Path *outer_path, Path *inner_path);

extern NestPath *create_nestloop_path(PlannerInfo *root,
                      RelOptInfo *joinrel,
                      JoinType jointype,
                      JoinCostWorkspace *workspace,
                      JoinPathExtraData *extra,
                      Path *outer_path,
                      Path *inner_path,
                      List *restrict_clauses,
                      List *pathkeys,
                      Relids required_outer);

extern MergePath *create_mergejoin_path(PlannerInfo *root,
                      RelOptInfo *joinrel,
                      JoinType jointype,
                      JoinCostWorkspace *workspace,
                      JoinPathExtraData *extra,
                      Path *outer_path,
                      Path *inner_path,
                      List *restrict_clauses,
                      List *pathkeys,
                      Relids required_outer,
                      List *mergeclauses,
                      List *outersortkeys,
                      List *innersortkeys);

extern HashPath *create_hashjoin_path(PlannerInfo *root,
                      RelOptInfo *joinrel,
                      JoinType jointype,
                      JoinCostWorkspace *workspace,
                      JoinPathExtraData *extra,
                      Path *outer_path,
                      Path *inner_path,
                      List *restrict_clauses,
                      Relids required_outer,
                      List *hashclauses);

extern ProjectionPath *create_projection_path(PlannerInfo *root,
                        RelOptInfo *rel,
                        Path *subpath,
                        PathTarget *target);
extern Path *apply_projection_to_path(PlannerInfo *root,
                          RelOptInfo *rel,
                          Path *path,
                          PathTarget *target);
extern ProjectSetPath *create_set_projection_path(PlannerInfo *root,
                            RelOptInfo *rel,
                            Path *subpath,
                            PathTarget *target);
extern SortPath *create_sort_path(PlannerInfo *root,
                  RelOptInfo *rel,
                  Path *subpath,
                  List *pathkeys,
                  double limit_tuples);
extern GroupPath *create_group_path(PlannerInfo *root,
                  RelOptInfo *rel,
                  Path *subpath,
                  PathTarget *target,
                  List *groupClause,
                  List *qual,
                  double numGroups);
extern UpperUniquePath *create_upper_unique_path(PlannerInfo *root,
                          RelOptInfo *rel,
                          Path *subpath,
                          int numCols,
                          double numGroups);
extern AggPath *create_agg_path(PlannerInfo *root,
                RelOptInfo *rel,
                Path *subpath,
                PathTarget *target,
                AggStrategy aggstrategy,
                AggSplit aggsplit,
                List *groupClause,
                List *qual,
                const AggClauseCosts *aggcosts,
                double numGroups);
extern GroupingSetsPath *create_groupingsets_path(PlannerInfo *root,
                          RelOptInfo *rel,
                          Path *subpath,
                          PathTarget *target,
                          List *having_qual,
                          AggStrategy aggstrategy,
                          List *rollups,
                          const AggClauseCosts *agg_costs,
                          double numGroups);
extern MinMaxAggPath *create_minmaxagg_path(PlannerInfo *root,
                      RelOptInfo *rel,
                      PathTarget *target,
                      List *mmaggregates,
                      List *quals);
extern WindowAggPath *create_windowagg_path(PlannerInfo *root,
                      RelOptInfo *rel,
                      Path *subpath,
                      PathTarget *target,
                      List *windowFuncs,
                      WindowClause *winclause,
                      List *winpathkeys);
extern SetOpPath *create_setop_path(PlannerInfo *root,
                  RelOptInfo *rel,
                  Path *subpath,
                  SetOpCmd cmd,
                  SetOpStrategy strategy,
                  List *distinctList,
                  AttrNumber flagColIdx,
                  int firstFlag,
                  double numGroups,
                  double outputRows);
extern RecursiveUnionPath *create_recursiveunion_path(PlannerInfo *root,
                            RelOptInfo *rel,
                            Path *leftpath,
                            Path *rightpath,
                            PathTarget *target,
                            List *distinctList,
                            int wtParam,
                            double numGroups);
extern LockRowsPath *create_lockrows_path(PlannerInfo *root, RelOptInfo *rel,
                      Path *subpath, List *rowMarks, int epqParam);
extern ModifyTablePath *create_modifytable_path(PlannerInfo *root,
                        RelOptInfo *rel,
                        CmdType operation, bool canSetTag,
                        Index nominalRelation, List *partitioned_rels,
						bool partColsUpdated,
                        List *resultRelations, List *subpaths,
                        List *subroots,
                        List *withCheckOptionLists, List *returningLists,
                        List *rowMarks, OnConflictExpr *onconflict,
                        int epqParam);
extern LimitPath *create_limit_path(PlannerInfo *root, RelOptInfo *rel,
                  Path *subpath,
                  Node *limitOffset, Node *limitCount,
				  int64 offset_est, int64 count_est,
				  bool earlyFinish);

extern Path *reparameterize_path(PlannerInfo *root, Path *path,
                    Relids required_outer,
                    double loop_count);
extern Path *reparameterize_path_by_child(PlannerInfo *root, Path *path,
                                                        RelOptInfo *child_rel);

extern Path *create_remotesubplan_path(PlannerInfo *root, Path *subpath,
                                        Distribution *distribution);

/*
 * prototypes for relnode.c
 */
extern void setup_simple_rel_arrays(PlannerInfo *root);
extern RelOptInfo *build_simple_rel(PlannerInfo *root, int relid,
                  RelOptInfo *parent);
extern RelOptInfo *find_base_rel(PlannerInfo *root, int relid);
extern RelOptInfo *find_join_rel(PlannerInfo *root, Relids relids);
extern RelOptInfo *build_join_rel(PlannerInfo *root,
                Relids joinrelids,
                RelOptInfo *outer_rel,
                RelOptInfo *inner_rel,
                SpecialJoinInfo *sjinfo,
                List **restrictlist_ptr);
extern Relids min_join_parameterization(PlannerInfo *root,
                          Relids joinrelids,
                          RelOptInfo *outer_rel,
                          RelOptInfo *inner_rel);
extern RelOptInfo *build_empty_join_rel(PlannerInfo *root);
extern RelOptInfo *fetch_upper_rel(PlannerInfo *root, UpperRelationKind kind,
                Relids relids);
extern AppendRelInfo *find_childrel_appendrelinfo(PlannerInfo *root,
                            RelOptInfo *rel);
extern Relids find_childrel_parents(PlannerInfo *root, RelOptInfo *rel);
extern ParamPathInfo *get_baserel_parampathinfo(PlannerInfo *root,
                          RelOptInfo *baserel,
                          Relids required_outer);
extern ParamPathInfo *get_joinrel_parampathinfo(PlannerInfo *root,
                          RelOptInfo *joinrel,
                          Path *outer_path,
                          Path *inner_path,
                          SpecialJoinInfo *sjinfo,
                          Relids required_outer,
                          List **restrict_clauses);
extern ParamPathInfo *get_appendrel_parampathinfo(RelOptInfo *appendrel,
                            Relids required_outer);
extern ParamPathInfo *find_param_path_info(RelOptInfo *rel,
                                        Relids required_outer);
extern RelOptInfo *build_child_join_rel(PlannerInfo *root,
                                        RelOptInfo *outer_rel, RelOptInfo *inner_rel,
                                        RelOptInfo *parent_joinrel, List *restrictlist,
                                        SpecialJoinInfo *sjinfo, JoinType jointype);

#ifdef __TBASE__
extern Path *create_redistribute_grouping_path(PlannerInfo *root, 
                                                Query *parse, Path *path);
extern Path *create_redistribute_distinct_agg_path(PlannerInfo *root,
												   Query *parse, Path *path,
												   Aggref *agg);
extern void contains_remotesubplan(Path *path, int *number, bool *redistribute);

extern void assign_constrain_nodes(List *node_list);

extern int replication_level;

extern bool restrict_query;
extern bool enable_subquery_shipping;
extern char *g_constrain_group;
#endif

#endif                            /* PATHNODE_H */
