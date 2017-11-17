/*-------------------------------------------------------------------------
 *
 * nodeAgg.h
 *      prototypes for nodeAgg.c
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeAgg.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEAGG_H
#define NODEAGG_H

#include "nodes/execnodes.h"
#ifdef __TBASE__
#include "executor/execParallel.h"
#endif

#ifdef __TBASE__
extern bool g_hybrid_hash_agg;
extern bool g_hybrid_hash_agg_debug;
extern int  g_default_hashagg_nbatches;
#endif

extern AggState *ExecInitAgg(Agg *node, EState *estate, int eflags);
extern void ExecEndAgg(AggState *node);
extern void ExecReScanAgg(AggState *node);

extern Size hash_agg_entry_size(int numAggs);

extern Datum aggregate_dummy(PG_FUNCTION_ARGS);

#ifdef __TBASE__
extern void ReDistributeEstimate(PlanState *node, ParallelContext *pcxt);

extern void ReDistributeInitializeDSM(PlanState *node, ParallelContext *pcxt);

extern void ReDistributeInitializeWorker(PlanState *node, ParallelWorkerContext *pwcxt);

extern void InitializeReDistribute(ReDistributeState *state, BufFile ***file);

extern bool ReDistributeData(ReDistributeState *state, BufFile **file, AttrNumber varattno, 
                 TupleTableSlot *slot, LocatorHashFunc    hashfunc, Oid type, MemoryContext tmpcxt);

extern void ReDistributeShareBufFile(ReDistributeState *state, BufFile **file);

extern BufFile *GetReDistributeBufFile(ReDistributeState *state, ReDistributeDataType *dataType);

extern bool GetReDistributeData(ReDistributeState *state, BufFile *file, TupleTableSlot **slot, 
                         ReDistributeDataType dataType, BufFile **aggfile, int *index);

extern int ReDistributeHash(Oid dataType, int numWorkers, Datum value, LocatorHashFunc hashfunc);

extern void ReDistributeBufferCheck(ReDistributeState *state);

extern void ReDistributeEreport(void);

#endif

#endif                            /* NODEAGG_H */
