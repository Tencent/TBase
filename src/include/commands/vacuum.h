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
 * vacuum.h
 *      header file for postgres vacuum cleaner and statistics analyzer
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/vacuum.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VACUUM_H
#define VACUUM_H

#include "access/htup.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "nodes/parsenodes.h"
#include "storage/buf.h"
#include "storage/lock.h"
#include "storage/relfilenode.h"
#include "utils/relcache.h"
#include "pgxc/planner.h"

/*----------
 * ANALYZE builds one of these structs for each attribute (column) that is
 * to be analyzed.  The struct and subsidiary data are in anl_context,
 * so they live until the end of the ANALYZE operation.
 *
 * The type-specific typanalyze function is passed a pointer to this struct
 * and must return TRUE to continue analysis, FALSE to skip analysis of this
 * column.  In the TRUE case it must set the compute_stats and minrows fields,
 * and can optionally set extra_data to pass additional info to compute_stats.
 * minrows is its request for the minimum number of sample rows to be gathered
 * (but note this request might not be honored, eg if there are fewer rows
 * than that in the table).
 *
 * The compute_stats routine will be called after sample rows have been
 * gathered.  Aside from this struct, it is passed:
 *        fetchfunc: a function for accessing the column values from the
 *                   sample rows
 *        samplerows: the number of sample tuples
 *        totalrows: estimated total number of rows in relation
 * The fetchfunc may be called with rownum running from 0 to samplerows-1.
 * It returns a Datum and an isNull flag.
 *
 * compute_stats should set stats_valid TRUE if it is able to compute
 * any useful statistics.  If it does, the remainder of the struct holds
 * the information to be stored in a pg_statistic row for the column.  Be
 * careful to allocate any pointed-to data in anl_context, which will NOT
 * be CurrentMemoryContext when compute_stats is called.
 *
 * Note: for the moment, all comparisons done for statistical purposes
 * should use the database's default collation (DEFAULT_COLLATION_OID).
 * This might change in some future release.
 *----------
 */
typedef struct VacAttrStats *VacAttrStatsP;

typedef Datum (*AnalyzeAttrFetchFunc) (VacAttrStatsP stats, int rownum,
                                       bool *isNull);

typedef void (*AnalyzeAttrComputeStatsFunc) (VacAttrStatsP stats,
                                             AnalyzeAttrFetchFunc fetchfunc,
                                             int samplerows,
                                             double totalrows);

typedef struct VacAttrStats
{
    /*
     * These fields are set up by the main ANALYZE code before invoking the
     * type-specific typanalyze function.
     *
     * Note: do not assume that the data being analyzed has the same datatype
     * shown in attr, ie do not trust attr->atttypid, attlen, etc.  This is
     * because some index opclasses store a different type than the underlying
     * column/expression.  Instead use attrtypid, attrtypmod, and attrtype for
     * information about the datatype being fed to the typanalyze function.
     */
    Form_pg_attribute attr;        /* copy of pg_attribute row for column */
    Oid            attrtypid;        /* type of data being analyzed */
    int32        attrtypmod;        /* typmod of data being analyzed */
    Form_pg_type attrtype;        /* copy of pg_type row for attrtypid */
    MemoryContext anl_context;    /* where to save long-lived data */

    /*
     * These fields must be filled in by the typanalyze routine, unless it
     * returns FALSE.
     */
    AnalyzeAttrComputeStatsFunc compute_stats;    /* function pointer */
    int            minrows;        /* Minimum # of rows wanted for stats */
    void       *extra_data;        /* for extra type-specific data */

    /*
     * These fields are to be filled in by the compute_stats routine. (They
     * are initialized to zero when the struct is created.)
     */
    bool        stats_valid;
    float4        stanullfrac;    /* fraction of entries that are NULL */
    int32        stawidth;        /* average width of column values */
    float4        stadistinct;    /* # distinct values */
    int16        stakind[STATISTIC_NUM_SLOTS];
    Oid            staop[STATISTIC_NUM_SLOTS];
    int            numnumbers[STATISTIC_NUM_SLOTS];
    float4       *stanumbers[STATISTIC_NUM_SLOTS];
    int            numvalues[STATISTIC_NUM_SLOTS];
    Datum       *stavalues[STATISTIC_NUM_SLOTS];

    /*
     * These fields describe the stavalues[n] element types. They will be
     * initialized to match attrtypid, but a custom typanalyze function might
     * want to store an array of something other than the analyzed column's
     * elements. It should then overwrite these fields.
     */
    Oid            statypid[STATISTIC_NUM_SLOTS];
    int16        statyplen[STATISTIC_NUM_SLOTS];
    bool        statypbyval[STATISTIC_NUM_SLOTS];
    char        statypalign[STATISTIC_NUM_SLOTS];

    /*
     * These fields are private to the main ANALYZE code and should not be
     * looked at by type-specific functions.
     */
    int            tupattnum;        /* attribute number within tuples */
    HeapTuple  *rows;            /* access info for std fetch function */
    TupleDesc    tupDesc;
    Datum       *exprvals;        /* access info for index fetch function */
    bool       *exprnulls;
    int            rowstride;
} VacAttrStats;

/*
 * Parameters customizing behavior of VACUUM and ANALYZE.
 */
typedef struct VacuumParams
{
    int            freeze_min_age; /* min freeze age, -1 to use default */
    int            freeze_table_age;    /* age at which to scan whole table */
    int            multixact_freeze_min_age;    /* min multixact freeze age, -1 to
                                             * use default */
    int            multixact_freeze_table_age; /* multixact age at which to scan
                                             * whole table */
    bool        is_wraparound;    /* force a for-wraparound vacuum */
    int            log_min_duration;    /* minimum execution threshold in ms at
                                     * which  verbose logs are activated, -1
                                     * to use default */
} VacuumParams;

/* GUC parameters */
extern PGDLLIMPORT int default_statistics_target;	/* PGDLLIMPORT for PostGIS */
extern int	vacuum_freeze_min_age;
extern int	vacuum_defer_freeze_min_age;
extern int	vacuum_freeze_table_age;
extern int	vacuum_multixact_freeze_min_age;
extern int	vacuum_multixact_freeze_table_age;

#ifdef __TBASE__
extern bool	enable_sampling_analyze;
extern bool distributed_query_analyze;
extern bool explain_query_analyze;

/* max number of queries collected */
#define MAX_DISTRIBUTED_QUERIES 512

#define    QUERY_SIZE 4096

#define PLAN_SIZE  131072

typedef struct
{      
    int  pid;                     /* backend pid for query */
    /* 
     * analyze info to be added, such as runtime for particular plan,
     * tuples produced by particular plan.
     */
}    AnalyzeInfo;

/*
 * query info on coordinator
 */
typedef struct
{
    char  query[QUERY_SIZE];     /* query string */
    char  plan[PLAN_SIZE];      /* corresponding query plan(serialized) */
    AnalyzeInfo info;             /* collected info for query */
}    QueryInfo;

typedef struct
{
    QueryInfo list[MAX_DISTRIBUTED_QUERIES];
    int       freelist[MAX_DISTRIBUTED_QUERIES];
    int       freeindex;
    slock_t   lock;
}    QueryInfoList;

/*
 * query analyze info on datanode, stored in hashtable for searching
 */
typedef struct
{
    /*
     * search key for analyze info, usually cursor or query string.
     * allocate large space for search key here to support query string
     * as key, little expensive..
     */
    char key[QUERY_SIZE];
    char plan[PLAN_SIZE];
    AnalyzeInfo info;
}    AnalyzeInfoEntry;

typedef struct 
{
	double samplenum;
	double totalnum;
	double deadnum;
	int64 totalpages;
	int64 visiblepages;
	HeapTuple *rows;
}SampleRowsContext;



#endif

/* in commands/vacuum.c */
extern void ExecVacuum(VacuumStmt *vacstmt, bool isTopLevel);
extern void	  vacuum(int				  options,
					 RangeVar			  *relation,
					 Oid				  relid,
					 VacuumParams		  *params,
					 List				  *va_cols,
					 BufferAccessStrategy bstrategy,
					 bool				  isTopLevel,
					 StatSyncOpt *syncOpt);
extern void vac_open_indexes(Relation relation, LOCKMODE lockmode,
                 int *nindexes, Relation **Irel);
extern void vac_close_indexes(int nindexes, Relation *Irel, LOCKMODE lockmode);
extern double vac_estimate_reltuples(Relation relation, bool is_analyze,
                       BlockNumber total_pages,
                       BlockNumber scanned_pages,
                       double scanned_tuples);
extern void vac_update_relstats(Relation relation,
                    BlockNumber num_pages,
                    double num_tuples,
                    BlockNumber num_all_visible_pages,
                    bool hasindex,
                    TransactionId frozenxid,
                    MultiXactId minmulti,
                    bool in_outer_xact);
extern void vacuum_set_xid_limits(Relation rel,
                      int freeze_min_age, int freeze_table_age,
                      int multixact_freeze_min_age,
                      int multixact_freeze_table_age,
                      TransactionId *oldestXmin,
                      TransactionId *freezeLimit,
                      TransactionId *xidFullScanLimit,
                      MultiXactId *multiXactCutoff,
                      MultiXactId *mxactFullScanLimit);
extern void vac_update_datfrozenxid(void);
extern void vacuum_delay_point(void);
#ifdef XCP
extern void vacuum_rel_coordinator(Relation onerel, bool is_outer, VacuumParams *params, StatSyncOpt *syncOpt);
TargetEntry *make_relation_tle(Oid reloid, const char *relname, const char *column);
#endif

/* in commands/vacuumlazy.c */
extern void lazy_vacuum_rel(Relation onerel, int options,
                VacuumParams *params, BufferAccessStrategy bstrategy);
#ifdef _SHARDING_
extern void truncate_extent_tuples(Relation onerel, 
                            BlockNumber     from_blk, 
                            BlockNumber to_blk, 
                            bool cleanpage, 
                            int *deleted_tuples);
extern void reinit_extent_pages(Relation rel, ExtentID eid);
extern void xlog_reinit_extent_pages(RelFileNode rnode, ExtentID eid);
extern void ExecVacuumShard(VacuumShardStmt *stmt);

#endif

/* in commands/analyze.c */
extern void	  analyze_rel(Oid				   relid,
						  RangeVar			   *relation,
						  int				   options,
						  VacuumParams		   *params,
						  List				   *va_cols,
						  bool				   in_outer_xact,
						  BufferAccessStrategy bstrategy,
						  StatSyncOpt		 *syncOpt);
extern bool std_typanalyze(VacAttrStats *stats);

/* in utils/misc/sampling.c --- duplicate of declarations in utils/sampling.h */
extern double anl_random_fract(void);
extern double anl_init_selection_state(int n);
extern double anl_get_next_S(double t, int n, double *stateptr);
extern RemoteQuery *init_sync_remotequery(StatSyncOpt *syncOpt, char **cnname);

#ifdef __TBASE__
extern Size QueryAnalyzeInfoShmemSize(void);

extern void QueryAnalyzeInfoInit(void);

extern void StoreQueryAnalyzeInfo(const char *key, void *ptr);

extern void DropQueryAnalyzeInfo(const char *key);

extern bool FetchQueryAnalyzeInfo(int index, char *key, void *ptr);

extern AnalyzeInfoEntry *FetchAllQueryAnalyzeInfo(HASH_SEQ_STATUS *status, bool init);

extern void ClearQueryAnalyzeInfo(void);

extern char *GetAnalyzeInfo(int nodeid, char *key);

extern void ExecSample(SampleStmt *stmt, DestReceiver *dest);

extern int     gts_maintain_option;
typedef enum
{
	GTS_MAINTAIN_NOTHING			= 0, /* do nothing */
	GTS_MAINTAIN_VACUUM_CHECK		= 1, /* check correctness of GTS while
										  *	doing vacuum. */
	GTS_MAINTAIN_VACUUM_RESET		= 2, /* check correctness of GTS and reset
										  * it according to tlog if it is wrong
										  * while doing vacuum. */
} GTSMaintainOption;
#endif

#endif                            /* VACUUM_H */
