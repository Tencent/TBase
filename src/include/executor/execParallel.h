/*--------------------------------------------------------------------
 * execParallel.h
 *		POSTGRES parallel execution interface
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/include/executor/execParallel.h
 *--------------------------------------------------------------------
 */

#ifndef EXECPARALLEL_H
#define EXECPARALLEL_H

#include "access/parallel.h"
#include "nodes/execnodes.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "utils/dsa.h"

typedef struct SharedExecutorInstrumentation SharedExecutorInstrumentation;

typedef struct ParallelExecutorInfo
{
	PlanState  *planstate;
	ParallelContext *pcxt;
	BufferUsage *buffer_usage;
	SharedExecutorInstrumentation *instrumentation;
	shm_mq_handle **tqueue;
	dsa_area   *area;
	bool		finished;
#ifdef __TBASE__
	bool        *executor_done;
#endif
} ParallelExecutorInfo;

#ifdef __TBASE__

extern bool *parallelExecutionError;

extern ParallelExecutorInfo *ExecInitParallelPlan(PlanState *planstate, 
                       EState *estate, int nworkers, Gather *gather);
#else
extern ParallelExecutorInfo *ExecInitParallelPlan(PlanState *planstate, 
                       EState *estate, int nworkers);
#endif

extern void ExecParallelFinish(ParallelExecutorInfo *pei);
extern void ExecParallelCleanup(ParallelExecutorInfo *pei);
extern void ExecParallelReinitialize(PlanState *planstate,
						 ParallelExecutorInfo *pei);

extern void ParallelQueryMain(dsm_segment *seg, shm_toc *toc);
#ifdef __TBASE__
extern ParallelWorkerStatus *GetParallelWorkerStatusInfo(shm_toc *toc);
extern int32 ExecGetForWorkerNumber(ParallelWorkerStatus *worker_status);

extern void *ParallelWorkerShmAlloc(Size size, bool zero);

extern dsa_area *GetNumWorkerDsa(int workerNumber);

extern bool ParallelError(void);

extern void HandleParallelExecutionError(void);
#endif
#endif							/* EXECPARALLEL_H */
