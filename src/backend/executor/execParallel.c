/*-------------------------------------------------------------------------
 *
 * execParallel.c
 *      Support routines for parallel execution.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * This file contains routines that are intended to support setting up,
 * using, and tearing down a ParallelContext from within the PostgreSQL
 * executor.  The ParallelContext machinery will handle starting the
 * workers and ensuring that their state generally matches that of the
 * leader; see src/backend/access/transam/README.parallel for details.
 * However, we must save and restore relevant executor state, such as
 * any ParamListInfo associated with the query, buffer usage info, and
 * the actual plan to be passed down to the worker.
 *
 * IDENTIFICATION
 *      src/backend/executor/execParallel.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/execParallel.h"
#include "executor/executor.h"
#include "executor/nodeBitmapHeapscan.h"
#include "executor/nodeCustom.h"
#include "executor/nodeForeignscan.h"
#include "executor/nodeHash.h"
#include "executor/nodeIndexscan.h"
#include "executor/nodeIndexonlyscan.h"
#include "executor/nodeSeqscan.h"
#include "executor/nodeSort.h"
#include "executor/tqueue.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "storage/spin.h"
#include "tcop/tcopprot.h"
#include "utils/dsa.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "pgstat.h"
#ifdef __TBASE__
#include "pgxc/execRemote.h"
#include "executor/nodeHashjoin.h"
#include "executor/nodeAgg.h"
#include "utils/lsyscache.h"
#include "pgxc/squeue.h"
#endif
#ifdef _MLS_
#include "utils/mls.h"
#endif
/*
 * Magic numbers for parallel executor communication.  We use constants
 * greater than any 32-bit integer here so that values < 2^32 can be used
 * by individual parallel nodes to store their own state.
 */
#define PARALLEL_KEY_PLANNEDSTMT        UINT64CONST(0xE000000000000001)
#define PARALLEL_KEY_PARAMS                UINT64CONST(0xE000000000000002)
#define PARALLEL_KEY_BUFFER_USAGE        UINT64CONST(0xE000000000000003)
#define PARALLEL_KEY_TUPLE_QUEUE        UINT64CONST(0xE000000000000004)
#define PARALLEL_KEY_INSTRUMENTATION    UINT64CONST(0xE000000000000005)
#define PARALLEL_KEY_DSA                UINT64CONST(0xE000000000000006)
#define PARALLEL_KEY_QUERY_TEXT        UINT64CONST(0xE000000000000007)
#ifdef __TBASE__
#define PARALLEL_KEY_WORKERS_TOTAL_NUM  UINT64CONST(0xE000000000000008)
#define PARALLEL_KEY_WORKERS_SEND       UINT64CONST(0xE000000000000009)
#define PARALLEL_KEY_WORKERS_SEG_HANDLE UINT64CONST(0xE00000000000000A)

/* 
  *Used as key to create dsa for each parallel worker, we assume max launched parallel
  * workers' num is 128. 
  */
#define PARALLEL_KEY_FIRST_WORKER_DSA   UINT64CONST(0xE000000000000010)
#define PARALLEL_KEY_MAX_WORKER_DSA     UINT64CONST(0xE000000000000090)

#define PARALLEL_KEY_EXEC_PARAM_LIST         UINT64CONST(0xE0000000000000A0)
#define PARALLEL_KEY_EXEC_PARAM_VALUE        UINT64CONST(0xE0000000000000A1)

#define PARALLEL_KEY_EXEC_ERROR        UINT64CONST(0xE0000000000000B1)
#define PARALLEL_KEY_EXEC_DONE         UINT64CONST(0xE0000000000000B2)
#define PARALLEL_REMOTEINSTR_OFFSET    UINT64CONST(0xEC00000000000000)
#endif

#define PARALLEL_TUPLE_QUEUE_SIZE        65536

#ifdef __TBASE__
/* dsa for parallel worker, used in execution for exchange data between workers */
static dsa_area   **currentParallelWorkerArea = NULL;

bool *parallelExecutionError = NULL;
#endif

/*
 * DSM structure for accumulating per-PlanState instrumentation.
 *
 * instrument_options: Same meaning here as in instrument.c.
 *
 * instrument_offset: Offset, relative to the start of this structure,
 * of the first Instrumentation object.  This will depend on the length of
 * the plan_node_id array.
 *
 * num_workers: Number of workers.
 *
 * num_plan_nodes: Number of plan nodes.
 *
 * plan_node_id: Array of plan nodes for which we are gathering instrumentation
 * from parallel workers.  The length of this array is given by num_plan_nodes.
 */
struct SharedExecutorInstrumentation
{
    int            instrument_options;
    int            instrument_offset;
    int            num_workers;
    int            num_plan_nodes;
    int            plan_node_id[FLEXIBLE_ARRAY_MEMBER];
    /* array of num_plan_nodes * num_workers Instrumentation objects follows */
};
#define GetInstrumentationArray(sei) \
    (AssertVariableIsOfTypeMacro(sei, SharedExecutorInstrumentation *), \
     (Instrumentation *) (((char *) sei) + sei->instrument_offset))

/* Context object for ExecParallelEstimate. */
typedef struct ExecParallelEstimateContext
{
    ParallelContext *pcxt;
    int            nnodes;
} ExecParallelEstimateContext;

/* Context object for ExecParallelInitializeDSM. */
typedef struct ExecParallelInitializeDSMContext
{
    ParallelContext *pcxt;
    SharedExecutorInstrumentation *instrumentation;
    int            nnodes;
} ExecParallelInitializeDSMContext;

#ifdef __TBASE__
/* Context object for ExecParallelInitializeRemoteInstr. */
typedef struct ExecParallelRemoteInstrContext
{
	ParallelContext *pcxt;
	int			ndatanode;
} ExecParallelRemoteInstrContext;
#endif

/* Helper functions that run in the parallel leader. */
static char *ExecSerializePlan(Plan *plan, EState *estate);
static bool ExecParallelEstimate(PlanState *node,
                     ExecParallelEstimateContext *e);
static bool ExecParallelInitializeDSM(PlanState *node,
                          ExecParallelInitializeDSMContext *d);
static shm_mq_handle **ExecParallelSetupTupleQueues(ParallelContext *pcxt,
                             bool reinitialize);
static bool ExecParallelReInitializeDSM(PlanState *planstate,
							ParallelContext *pcxt);
static bool ExecParallelRetrieveInstrumentation(PlanState *planstate,
                                    SharedExecutorInstrumentation *instrumentation);
#ifdef __TBASE__
static bool ExecParallelEstimateRemoteInstr(PlanState *planstate,
                                            ExecParallelRemoteInstrContext *ri);
static bool ExecParallelInitRemoteInstrDSM(PlanState *planstate,
                                           ExecParallelRemoteInstrContext *ri);
static bool ExecInitializeWorkerRemoteInstr(PlanState *planstate, ParallelWorkerContext *pcxt);
#endif
/* Helper function that runs in the parallel worker. */
static DestReceiver *ExecParallelGetReceiver(dsm_segment *seg, shm_toc *toc);

/*
 * Create a serialized representation of the plan to be sent to each worker.
 */
static char *
ExecSerializePlan(Plan *plan, EState *estate)
{
    PlannedStmt *pstmt;
    ListCell   *lc;

    /* We can't scribble on the original plan, so make a copy. */
    plan = copyObject(plan);

    /*
     * The worker will start its own copy of the executor, and that copy will
     * insert a junk filter if the toplevel node has any resjunk entries. We
     * don't want that to happen, because while resjunk columns shouldn't be
     * sent back to the user, here the tuples are coming back to another
     * backend which may very well need them.  So mutate the target list
     * accordingly.  This is sort of a hack; there might be better ways to do
     * this...
     */
    foreach(lc, plan->targetlist)
    {
        TargetEntry *tle = lfirst_node(TargetEntry, lc);

        tle->resjunk = false;
    }

    /*
     * Create a dummy PlannedStmt.  Most of the fields don't need to be valid
     * for our purposes, but the worker will need at least a minimal
     * PlannedStmt to start the executor.
     */
    pstmt = makeNode(PlannedStmt);
    pstmt->commandType = CMD_SELECT;
    pstmt->queryId = 0;
    pstmt->hasReturning = false;
    pstmt->hasModifyingCTE = false;
    pstmt->canSetTag = true;
    pstmt->transientPlan = false;
    pstmt->dependsOnRole = false;
    pstmt->parallelModeNeeded = false;
    pstmt->planTree = plan;
    pstmt->rtable = estate->es_range_table;
    pstmt->resultRelations = NIL;
    pstmt->nonleafResultRelations = NIL;

    /*
     * Transfer only parallel-safe subplans, leaving a NULL "hole" in the list
     * for unsafe ones (so that the list indexes of the safe ones are
     * preserved).  This positively ensures that the worker won't try to run,
     * or even do ExecInitNode on, an unsafe subplan.  That's important to
     * protect, eg, non-parallel-aware FDWs from getting into trouble.
     */
    pstmt->subplans = NIL;
    foreach(lc, estate->es_plannedstmt->subplans)
    {
        Plan       *subplan = (Plan *) lfirst(lc);

        if (subplan && !subplan->parallel_safe)
            subplan = NULL;
        pstmt->subplans = lappend(pstmt->subplans, subplan);
    }

    pstmt->rewindPlanIDs = NULL;
    pstmt->rowMarks = NIL;
    pstmt->relationOids = NIL;
    pstmt->invalItems = NIL;    /* workers can't replan anyway... */
    pstmt->nParamExec = estate->es_plannedstmt->nParamExec;
    pstmt->utilityStmt = NULL;
    pstmt->stmt_location = -1;
    pstmt->stmt_len = -1;

    /* Return serialized copy of our dummy PlannedStmt. */
    return nodeToString(pstmt);
}

/*
 * Parallel-aware plan nodes (and occasionally others) may need some state
 * which is shared across all parallel workers.  Before we size the DSM, give
 * them a chance to call shm_toc_estimate_chunk or shm_toc_estimate_keys on
 * &pcxt->estimator.
 *
 * While we're at it, count the number of PlanState nodes in the tree, so
 * we know how many SharedPlanStateInstrumentation structures we need.
 */
static bool
ExecParallelEstimate(PlanState *planstate, ExecParallelEstimateContext *e)
{
#ifdef __TBASE__
	int previous_nworkers;
#endif
    if (planstate == NULL)
        return false;

    /* Count this node. */
    e->nnodes++;

        switch (nodeTag(planstate))
        {
            case T_SeqScanState:
			if (planstate->plan->parallel_aware)
                ExecSeqScanEstimate((SeqScanState *) planstate,
                                    e->pcxt);
                break;
            case T_IndexScanState:
			if (planstate->plan->parallel_aware)
                ExecIndexScanEstimate((IndexScanState *) planstate,
                                      e->pcxt);
                break;
            case T_IndexOnlyScanState:
			if (planstate->plan->parallel_aware)
                ExecIndexOnlyScanEstimate((IndexOnlyScanState *) planstate,
                                          e->pcxt);
                break;
            case T_ForeignScanState:
			if (planstate->plan->parallel_aware)
                ExecForeignScanEstimate((ForeignScanState *) planstate,
                                        e->pcxt);
                break;
            case T_CustomScanState:
			if (planstate->plan->parallel_aware)
                ExecCustomScanEstimate((CustomScanState *) planstate,
                                       e->pcxt);
                break;
            case T_BitmapHeapScanState:
			if (planstate->plan->parallel_aware)
                ExecBitmapHeapEstimate((BitmapHeapScanState *) planstate,
                                       e->pcxt);
                break;
		case T_HashState:
			/* even when not parallel-aware, for EXPLAIN ANALYZE */
			ExecHashEstimate((HashState *) planstate, e->pcxt);
			break;
		case T_SortState:
			/* even when not parallel-aware, for EXPLAIN ANALYZE */
			ExecSortEstimate((SortState *) planstate, e->pcxt);
#ifdef __TBASE__
			if (planstate->plan->parallel_aware)
				ReDistributeEstimate(planstate, e->pcxt);
			break;
            /* For remote query and remote subplan, there is no need for shared storage. */
            case T_RemoteQueryState:                
			break;
            case T_RemoteSubplanState:
			/*
             * If we are running with instrument option, must init full plantree here,
             * to ensure e->nnodes correct. Further, we estimate per node instrument
             * for remote instrumentation.
             */
			if (planstate->instrument && NULL == planstate->lefttree)
			{
				ExecParallelRemoteInstrContext ri;
				RemoteSubplanState *node = (RemoteSubplanState *) planstate;
				
				ri.ndatanode = list_length(((RemoteSubplan *)planstate->plan)->nodeList);
				ri.pcxt = e->pcxt;
				
				planstate->lefttree = ExecInitNode(planstate->plan->lefttree,
				                                   planstate->state,
				                                   EXEC_FLAG_EXPLAIN_ONLY);
				planstate_tree_walker(planstate, ExecParallelEstimateRemoteInstr, &ri);
				node->combiner.remote_parallel_estimated = true;
			}
                break;
            case T_HashJoinState:
			if (planstate->plan->parallel_aware)
                ExecParallelHashJoinEstimate((HashJoinState*) planstate,
                                             e->pcxt);
                break;
            case T_AggState:
			if (planstate->plan->parallel_aware)
                {
                    AggState *aggstate = (AggState *)planstate;

                    if (aggstate->aggstrategy == AGG_HASHED)
                        ReDistributeEstimate(planstate, e->pcxt);
                }
                break;
		case T_GatherState:
			previous_nworkers = e->pcxt->nworkers;
			e->pcxt->nworkers = ((Gather *) planstate->plan)->num_workers;
#endif
            default:
                break;
        }

#ifdef __TBASE__
	planstate_tree_walker(planstate, ExecParallelEstimate, e);
	
	if (IsA(planstate, GatherState))
		e->pcxt->nworkers = previous_nworkers;
	
	return false;
#else
    return planstate_tree_walker(planstate, ExecParallelEstimate, e);
#endif
}

/*
 * Initialize the dynamic shared memory segment that will be used to control
 * parallel execution.
 */
static bool
ExecParallelInitializeDSM(PlanState *planstate,
                          ExecParallelInitializeDSMContext *d)
{
#ifdef __TBASE__
	int previous_nworkers;
#endif
    if (planstate == NULL)
        return false;

    /* If instrumentation is enabled, initialize slot for this node. */
    if (d->instrumentation != NULL)
        d->instrumentation->plan_node_id[d->nnodes] =
            planstate->plan->plan_node_id;

    /* Count this node. */
    d->nnodes++;

    /*
	 * Call initializers for DSM-using plan nodes.
     *
	 * Most plan nodes won't do anything here, but plan nodes that allocated
	 * DSM may need to initialize shared state in the DSM before parallel
	 * workers are launched.  They can allocate the space they previously
     * estimated using shm_toc_allocate, and add the keys they previously
     * estimated using shm_toc_insert, in each case targeting pcxt->toc.
     */
        switch (nodeTag(planstate))
        {
            case T_SeqScanState:
			if (planstate->plan->parallel_aware)
                ExecSeqScanInitializeDSM((SeqScanState *) planstate,
                                         d->pcxt);
                break;
            case T_IndexScanState:
			if (planstate->plan->parallel_aware)
                ExecIndexScanInitializeDSM((IndexScanState *) planstate,
                                           d->pcxt);
                break;
            case T_IndexOnlyScanState:
			if (planstate->plan->parallel_aware)
                ExecIndexOnlyScanInitializeDSM((IndexOnlyScanState *) planstate,
                                               d->pcxt);
                break;
            case T_ForeignScanState:
			if (planstate->plan->parallel_aware)
                ExecForeignScanInitializeDSM((ForeignScanState *) planstate,
                                             d->pcxt);
                break;
            case T_CustomScanState:
			if (planstate->plan->parallel_aware)
                ExecCustomScanInitializeDSM((CustomScanState *) planstate,
                                            d->pcxt);
                break;
            case T_BitmapHeapScanState:
			if (planstate->plan->parallel_aware)
                ExecBitmapHeapInitializeDSM((BitmapHeapScanState *) planstate,
                                            d->pcxt);
                break;
		case T_HashState:
			/* even when not parallel-aware, for EXPLAIN ANALYZE */
			ExecHashInitializeDSM((HashState *) planstate, d->pcxt);
			break;
		case T_SortState:
			/* even when not parallel-aware, for EXPLAIN ANALYZE */
			ExecSortInitializeDSM((SortState *) planstate, d->pcxt);
#ifdef __TBASE__
			if (planstate->plan->parallel_aware)
				ReDistributeInitializeDSM(planstate, d->pcxt);
			break;
            case T_RemoteQueryState:
			if (planstate->plan->parallel_aware)
                ExecRemoteQueryInitializeDSM((RemoteQueryState *)planstate,
                                                  d->pcxt);
                break;
            case T_RemoteSubplanState:
			{
				RemoteSubplanState *node = (RemoteSubplanState *) planstate;
				if (node->combiner.remote_parallel_estimated)
				{
					ExecParallelRemoteInstrContext ri;
					
					ri.ndatanode = list_length(((RemoteSubplan *)planstate->plan)->nodeList);
					ri.pcxt = d->pcxt;
					
					planstate_tree_walker(planstate, ExecParallelInitRemoteInstrDSM, &ri);
				}
			if (planstate->plan->parallel_aware)
                ExecRemoteSubPlanInitializeDSM((RemoteSubplanState *)planstate,
                                                  d->pcxt);
			}
                break;                
            case T_HashJoinState:
			if (planstate->plan->parallel_aware)
                ExecParallelHashJoinInitializeDSM((HashJoinState *) planstate,
                                                  d->pcxt);
                break;
            case T_AggState:
			if (planstate->plan->parallel_aware)
                {
                    AggState *aggstate = (AggState *)planstate;
                    
                    if (aggstate->aggstrategy == AGG_HASHED)
                        ReDistributeInitializeDSM(planstate, d->pcxt);
                }
                break;
		case T_GatherState:
			previous_nworkers = d->pcxt->nworkers;
			d->pcxt->nworkers = ((Gather *) planstate->plan)->num_workers;
#endif
            default:
                break;
        }

#ifdef __TBASE__
	planstate_tree_walker(planstate, ExecParallelInitializeDSM, d);
	
	if (IsA(planstate, GatherState))
		d->pcxt->nworkers = previous_nworkers;
	
	return false;
#else
    return planstate_tree_walker(planstate, ExecParallelInitializeDSM, d);
#endif
}

/*
 * It sets up the response queues for backend workers to return tuples
 * to the main backend and start the workers.
 */
static shm_mq_handle **
ExecParallelSetupTupleQueues(ParallelContext *pcxt, bool reinitialize)
{
    shm_mq_handle **responseq;
    char       *tqueuespace;
    int            i;

    /* Skip this if no workers. */
    if (pcxt->nworkers == 0)
        return NULL;

    /* Allocate memory for shared memory queue handles. */
    responseq = (shm_mq_handle **)
        palloc(pcxt->nworkers * sizeof(shm_mq_handle *));

    /*
     * If not reinitializing, allocate space from the DSM for the queues;
     * otherwise, find the already allocated space.
     */
    if (!reinitialize)
        tqueuespace =
            shm_toc_allocate(pcxt->toc,
                             mul_size(PARALLEL_TUPLE_QUEUE_SIZE,
                                      pcxt->nworkers));
    else
        tqueuespace = shm_toc_lookup(pcxt->toc, PARALLEL_KEY_TUPLE_QUEUE, false);

    /* Create the queues, and become the receiver for each. */
    for (i = 0; i < pcxt->nworkers; ++i)
    {
        shm_mq       *mq;

        mq = shm_mq_create(tqueuespace +
                           ((Size) i) * PARALLEL_TUPLE_QUEUE_SIZE,
                           (Size) PARALLEL_TUPLE_QUEUE_SIZE);

        shm_mq_set_receiver(mq, MyProc);
        responseq[i] = shm_mq_attach(mq, pcxt->seg, NULL);
    }

    /* Add array of queues to shm_toc, so others can find it. */
    if (!reinitialize)
        shm_toc_insert(pcxt->toc, PARALLEL_KEY_TUPLE_QUEUE, tqueuespace);

    /* Return array of handles. */
    return responseq;
}

/*
 * Sets up the required infrastructure for backend workers to perform
 * execution and return results to the main backend.
 */
#ifdef __TBASE__
ParallelExecutorInfo *
ExecInitParallelPlan(PlanState *planstate, EState *estate, int nworkers, Gather *gather)
#else
ParallelExecutorInfo *
ExecInitParallelPlan(PlanState *planstate, EState *estate, int nworkers)
#endif
{// #lizard forgives
    ParallelExecutorInfo *pei;
    ParallelContext *pcxt;
    ExecParallelEstimateContext e;
    ExecParallelInitializeDSMContext d;
    char       *pstmt_data;
    char       *pstmt_space;
    char       *param_space;
    BufferUsage *bufusage_space;
    SharedExecutorInstrumentation *instrumentation = NULL;
    int            pstmt_len;
    int            param_len;
    int            instrumentation_len = 0;
    int            instrument_offset = 0;
    Size        dsa_minsize = dsa_minimum_size();
    char       *query_string;
    int            query_len;
    ParallelWorkerStatus *num_parallel_workers = NULL;
    bool        *parallel_send                 = NULL;
    dsm_handle  *handle                        = NULL;
#ifdef __TBASE__
    StringInfoData    buf;
#endif
    /* Allocate object for return value. */
    pei = palloc0(sizeof(ParallelExecutorInfo));
    pei->finished = false;
    pei->planstate = planstate;
#ifdef __TBASE__
    pei->executor_done = NULL;
#endif

    /* Fix up and serialize plan to be sent to workers. */
    pstmt_data = ExecSerializePlan(planstate->plan, estate);

    /* Create a parallel context. */
    pcxt = CreateParallelContext("postgres", "ParallelQueryMain", nworkers);
    pei->pcxt = pcxt;

    /*
     * Before telling the parallel context to create a dynamic shared memory
     * segment, we need to figure out how big it should be.  Estimate space
     * for the various things we need to store.
     */

    /* Estimate space for query text. */
    query_len = strlen(estate->es_sourceText);
    shm_toc_estimate_chunk(&pcxt->estimator, query_len);
    shm_toc_estimate_keys(&pcxt->estimator, 1);

    /* Estimate space for serialized PlannedStmt. */
    pstmt_len = strlen(pstmt_data) + 1;
    shm_toc_estimate_chunk(&pcxt->estimator, pstmt_len);
    shm_toc_estimate_keys(&pcxt->estimator, 1);

    /* Estimate space for serialized ParamListInfo. */
    param_len = EstimateParamListSpace(estate->es_param_list_info);
    shm_toc_estimate_chunk(&pcxt->estimator, param_len);
    shm_toc_estimate_keys(&pcxt->estimator, 1);

#ifdef __TBASE__
    /* Estimate space for parallel workers' total number. */
    shm_toc_estimate_chunk(&pcxt->estimator, sizeof(ParallelWorkerStatus));
    shm_toc_estimate_keys(&pcxt->estimator, 1);

    shm_toc_estimate_chunk(&pcxt->estimator, sizeof(bool));
    shm_toc_estimate_keys(&pcxt->estimator, 1);

    shm_toc_estimate_chunk(&pcxt->estimator, sizeof(bool));
    shm_toc_estimate_keys(&pcxt->estimator, 1);

    shm_toc_estimate_chunk(&pcxt->estimator, sizeof(bool));
    shm_toc_estimate_keys(&pcxt->estimator, 1);

    if (gather && gather->parallelWorker_sendTuple)
    {
        shm_toc_estimate_chunk(&pcxt->estimator, sizeof(dsm_handle));
        shm_toc_estimate_keys(&pcxt->estimator, 1);
    }

    /* send param to workers */
    if (estate->es_plannedstmt->nParamExec)
    {
        int i = 0;
        int nParamExec = estate->es_plannedstmt->nParamExec;
        ParamExecData *es_param_exec_vals = estate->es_param_exec_vals;
        shm_toc_estimate_chunk(&pcxt->estimator, nParamExec * sizeof(ParamExecData));
        shm_toc_estimate_keys(&pcxt->estimator, 1);

        initStringInfo(&buf);

        for (i = 0; i < nParamExec; i++)
        { 
            if (!es_param_exec_vals[i].isnull && OidIsValid(es_param_exec_vals[i].ptype))
            {
                Oid        typOutput;
                bool    typIsVarlena;
                Datum    pval;
                char   *pstring;
                int        len;
                Oid ptype = es_param_exec_vals[i].ptype;
                Datum value = es_param_exec_vals[i].value;

                /* Get info needed to output the value */
                getTypeOutputInfo(ptype, &typOutput, &typIsVarlena);

                /*
                 * If we have a toasted datum, forcibly detoast it here to avoid
                 * memory leakage inside the type's output routine.
                 */
                if (typIsVarlena)
                    pval = PointerGetDatum(PG_DETOAST_DATUM(value));
                else
                    pval = value;

                /* Convert Datum to string */
                pstring = OidOutputFunctionCall(typOutput, pval);

                /* copy data to the buffer */
                len = strlen(pstring);
                appendBinaryStringInfo(&buf, (char *) &len, 4);
                appendBinaryStringInfo(&buf, pstring, len);
            }
        }

        if (buf.len)
        {
            shm_toc_estimate_chunk(&pcxt->estimator, buf.len);
            shm_toc_estimate_keys(&pcxt->estimator, 1);
        }
    }
#endif

    /*
     * Estimate space for BufferUsage.
     *
     * If EXPLAIN is not in use and there are no extensions loaded that care,
     * we could skip this.  But we have no way of knowing whether anyone's
     * looking at pgBufferUsage, so do it unconditionally.
     */
    shm_toc_estimate_chunk(&pcxt->estimator,
                           mul_size(sizeof(BufferUsage), pcxt->nworkers));
    shm_toc_estimate_keys(&pcxt->estimator, 1);

    if (!gather || !gather->parallelWorker_sendTuple)
    {
        /* Estimate space for tuple queues. */
        shm_toc_estimate_chunk(&pcxt->estimator,
                               mul_size(PARALLEL_TUPLE_QUEUE_SIZE, pcxt->nworkers));
        shm_toc_estimate_keys(&pcxt->estimator, 1);
    }

    /*
     * Give parallel-aware nodes a chance to add to the estimates, and get a
     * count of how many PlanState nodes there are.
     */
    e.pcxt = pcxt;
    e.nnodes = 0;
    ExecParallelEstimate(planstate, &e);

    /* Estimate space for instrumentation, if required. */
    if (estate->es_instrument)
    {
        instrumentation_len =
            offsetof(SharedExecutorInstrumentation, plan_node_id) +
            sizeof(int) * e.nnodes;
        instrumentation_len = MAXALIGN(instrumentation_len);
        instrument_offset = instrumentation_len;
        instrumentation_len +=
            mul_size(sizeof(Instrumentation),
                     mul_size(e.nnodes, nworkers));
        shm_toc_estimate_chunk(&pcxt->estimator, instrumentation_len);
        shm_toc_estimate_keys(&pcxt->estimator, 1);
    }

    /* Estimate space for DSA area. */
    shm_toc_estimate_chunk(&pcxt->estimator, dsa_minsize);
    shm_toc_estimate_keys(&pcxt->estimator, 1);

#ifdef __TBASE__
    /* Estimate space for DSA area for each parallel worker. */
    if(nworkers)
    {
        int i = 0;
        for(i = 0; i < nworkers; i++)
        {
            shm_toc_estimate_chunk(&pcxt->estimator, dsa_minsize);
            shm_toc_estimate_keys(&pcxt->estimator, 1);
        }
    }
#endif

#ifdef __TBASE__
	/* set snapshot as needed */
	if (!g_set_global_snapshot && !ActiveSnapshotSet())
	{
		SetSnapshot(estate);
	}
#endif


    /* Everyone's had a chance to ask for space, so now create the DSM. */
    InitializeParallelDSM(pcxt);

    /*
     * OK, now we have a dynamic shared memory segment, and it should be big
     * enough to store all of the data we estimated we would want to put into
     * it, plus whatever general stuff (not specifically executor-related) the
     * ParallelContext itself needs to store there.  None of the space we
     * asked for has been allocated or initialized yet, though, so do that.
     */

    /* Store query string */
    query_string = shm_toc_allocate(pcxt->toc, query_len);
    memcpy(query_string, estate->es_sourceText, query_len);
    shm_toc_insert(pcxt->toc, PARALLEL_KEY_QUERY_TEXT, query_string);

    /* Store serialized PlannedStmt. */
    pstmt_space = shm_toc_allocate(pcxt->toc, pstmt_len);
    memcpy(pstmt_space, pstmt_data, pstmt_len);
    shm_toc_insert(pcxt->toc, PARALLEL_KEY_PLANNEDSTMT, pstmt_space);

    /* Store serialized ParamListInfo. */
    param_space = shm_toc_allocate(pcxt->toc, param_len);
    shm_toc_insert(pcxt->toc, PARALLEL_KEY_PARAMS, param_space);
    SerializeParamList(estate->es_param_list_info, &param_space);

#ifdef __TBASE__
    /* Store parallel workers total number. */
    num_parallel_workers = shm_toc_allocate(pcxt->toc, sizeof(ParallelWorkerStatus));
    num_parallel_workers->parallelWorkersSetupDone = false;
    num_parallel_workers->numLaunchedWorkers       = 0;
    num_parallel_workers->numExpectedWorkers       = nworkers;
    shm_toc_insert(pcxt->toc, PARALLEL_KEY_WORKERS_TOTAL_NUM, num_parallel_workers);

    parallel_send        = shm_toc_allocate(pcxt->toc, sizeof(bool));

    if (gather)
    {
        *parallel_send   = gather->parallelWorker_sendTuple;
    }
    else
    {
        *parallel_send   = false;
    }

    /* parallel executor done flag */
    pei->executor_done = shm_toc_allocate(pcxt->toc, sizeof(bool));

    *pei->executor_done = false;

    shm_toc_insert(pcxt->toc, PARALLEL_KEY_EXEC_DONE, pei->executor_done);

    parallelExecutionError = shm_toc_allocate(pcxt->toc, sizeof(bool));

    *parallelExecutionError = false;

    shm_toc_insert(pcxt->toc, PARALLEL_KEY_WORKERS_SEND, parallel_send);

    shm_toc_insert(pcxt->toc, PARALLEL_KEY_EXEC_ERROR, parallelExecutionError);

    if (gather && gather->parallelWorker_sendTuple)
    {
        handle  = shm_toc_allocate(pcxt->toc, sizeof(dsm_handle));

        *handle = GetParallelSendSegHandle();

        if (*handle == DSM_HANDLE_INVALID)
        {
            elog(ERROR, " invalid parallel send segment handle.");
        }

        shm_toc_insert(pcxt->toc, PARALLEL_KEY_WORKERS_SEG_HANDLE, handle);
    }

    /* param */
    if (estate->es_plannedstmt->nParamExec)
    {
        int nParamExec = estate->es_plannedstmt->nParamExec;

        ParamExecData *es_param_exec_vals = shm_toc_allocate(pcxt->toc, nParamExec * sizeof(ParamExecData));

        memcpy(es_param_exec_vals, estate->es_param_exec_vals, nParamExec * sizeof(ParamExecData));

        shm_toc_insert(pcxt->toc, PARALLEL_KEY_EXEC_PARAM_LIST, es_param_exec_vals);

        if (buf.len)
        {
            char *paramsExec = shm_toc_allocate(pcxt->toc, buf.len);

            memcpy(paramsExec, buf.data, buf.len);

            shm_toc_insert(pcxt->toc, PARALLEL_KEY_EXEC_PARAM_VALUE, paramsExec);

            pfree(buf.data);
        }
    }
#endif

    /* Allocate space for each worker's BufferUsage; no need to initialize. */
    bufusage_space = shm_toc_allocate(pcxt->toc,
                                      mul_size(sizeof(BufferUsage), pcxt->nworkers));
    shm_toc_insert(pcxt->toc, PARALLEL_KEY_BUFFER_USAGE, bufusage_space);
    pei->buffer_usage = bufusage_space;

    if (gather && gather->parallelWorker_sendTuple)
    {
        pei->tqueue = NULL;
    }
    else
    {
        /* Set up tuple queues. */
        pei->tqueue = ExecParallelSetupTupleQueues(pcxt, false);
    }

    /*
     * If instrumentation options were supplied, allocate space for the data.
     * It only gets partially initialized here; the rest happens during
     * ExecParallelInitializeDSM.
     */
    if (estate->es_instrument)
    {
        Instrumentation *instrument;
        int            i;

        instrumentation = shm_toc_allocate(pcxt->toc, instrumentation_len);
        instrumentation->instrument_options = estate->es_instrument;
        instrumentation->instrument_offset = instrument_offset;
        instrumentation->num_workers = nworkers;
        instrumentation->num_plan_nodes = e.nnodes;
        instrument = GetInstrumentationArray(instrumentation);
        for (i = 0; i < nworkers * e.nnodes; ++i)
            InstrInit(&instrument[i], estate->es_instrument);
        shm_toc_insert(pcxt->toc, PARALLEL_KEY_INSTRUMENTATION,
                       instrumentation);
        pei->instrumentation = instrumentation;
    }

    /*
     * Create a DSA area that can be used by the leader and all workers.
     * (However, if we failed to create a DSM and are using private memory
     * instead, then skip this.)
     */
    if (pcxt->seg != NULL)
    {
        char       *area_space;

        area_space = shm_toc_allocate(pcxt->toc, dsa_minsize);
        shm_toc_insert(pcxt->toc, PARALLEL_KEY_DSA, area_space);
        pei->area = dsa_create_in_place(area_space, dsa_minsize,
                                        LWTRANCHE_PARALLEL_QUERY_DSA,
                                        pcxt->seg);
#ifdef __TBASE__
        /* create dsa for each parallel worker. */
        if(nworkers)
        {
            int i = 0;
            for(i = 0; i < nworkers; i++)
            {
                area_space = shm_toc_allocate(pcxt->toc, dsa_minsize);
                shm_toc_insert(pcxt->toc, (PARALLEL_KEY_FIRST_WORKER_DSA + i),
                               area_space);
                dsa_create_in_place(area_space, dsa_minsize,
                                        LWTRANCHE_PARALLEL_WORKER_DSA,
                                        pcxt->seg);
            }
        }
#endif
    }

    /*
     * Make the area available to executor nodes running in the leader.  See
     * also ParallelQueryMain which makes it available to workers.
     */
    estate->es_query_dsa = pei->area;

    /*
     * Give parallel-aware nodes a chance to initialize their shared data.
     * This also initializes the elements of instrumentation->ps_instrument,
     * if it exists.
     */
    d.pcxt = pcxt;
    d.instrumentation = instrumentation;
    d.nnodes = 0;
    ExecParallelInitializeDSM(planstate, &d);

    /*
	 * Make sure that the world hasn't shifted under our feet.  This could
     * probably just be an Assert(), but let's be conservative for now.
     */
    if (e.nnodes != d.nnodes)
        elog(ERROR, "inconsistent count of PlanState nodes");

    /* OK, we're ready to rock and roll. */
    return pei;
}

/*
 * Re-initialize the parallel executor shared memory state before launching
 * a fresh batch of workers.
 */
void
ExecParallelReinitialize(PlanState *planstate,
						 ParallelExecutorInfo *pei)
{
	/* Old workers must already be shut down */
	Assert(pei->finished);

	ReinitializeParallelDSM(pei->pcxt);
	pei->tqueue = ExecParallelSetupTupleQueues(pei->pcxt, true);
	pei->finished = false;

	/* Traverse plan tree and let each child node reset associated state. */
	ExecParallelReInitializeDSM(planstate, pei->pcxt);
}

/*
 * Traverse plan tree to reinitialize per-node dynamic shared memory state
 */
static bool
ExecParallelReInitializeDSM(PlanState *planstate,
							ParallelContext *pcxt)
{
	if (planstate == NULL)
		return false;

	/*
	 * Call reinitializers for DSM-using plan nodes.
	 */
	switch (nodeTag(planstate))
	{
		case T_SeqScanState:
			if (planstate->plan->parallel_aware)
				ExecSeqScanReInitializeDSM((SeqScanState *) planstate,
										   pcxt);
			break;
		case T_IndexScanState:
			if (planstate->plan->parallel_aware)
				ExecIndexScanReInitializeDSM((IndexScanState *) planstate,
											 pcxt);
			break;
		case T_IndexOnlyScanState:
			if (planstate->plan->parallel_aware)
				ExecIndexOnlyScanReInitializeDSM((IndexOnlyScanState *) planstate,
												 pcxt);
			break;
		case T_ForeignScanState:
			if (planstate->plan->parallel_aware)
				ExecForeignScanReInitializeDSM((ForeignScanState *) planstate,
											   pcxt);
			break;
		case T_CustomScanState:
			if (planstate->plan->parallel_aware)
				ExecCustomScanReInitializeDSM((CustomScanState *) planstate,
											  pcxt);
			break;
		case T_BitmapHeapScanState:
			if (planstate->plan->parallel_aware)
				ExecBitmapHeapReInitializeDSM((BitmapHeapScanState *) planstate,
											  pcxt);
			break;
		case T_HashState:
			/* even when not parallel-aware, for EXPLAIN ANALYZE */
			ExecHashReInitializeDSM((HashState *) planstate, pcxt);
			break;
		case T_SortState:
			/* even when not parallel-aware, for EXPLAIN ANALYZE */
			ExecSortReInitializeDSM((SortState *) planstate, pcxt);
			break;

		default:
			break;
	}

	return planstate_tree_walker(planstate, ExecParallelReInitializeDSM, pcxt);
}

/*
 * Copy instrumentation information about this node and its descendants from
 * dynamic shared memory.
 */
static bool
ExecParallelRetrieveInstrumentation(PlanState *planstate,
                                    SharedExecutorInstrumentation *instrumentation)
{
    Instrumentation *instrument;
    int            i;
    int            n;
    int            ibytes;
    int            plan_node_id = planstate->plan->plan_node_id;
    MemoryContext oldcontext;

    /* Find the instrumentation for this node. */
    for (i = 0; i < instrumentation->num_plan_nodes; ++i)
        if (instrumentation->plan_node_id[i] == plan_node_id)
            break;
    if (i >= instrumentation->num_plan_nodes)
        elog(ERROR, "plan node %d not found", plan_node_id);

    /* Accumulate the statistics from all workers. */
    instrument = GetInstrumentationArray(instrumentation);
    instrument += i * instrumentation->num_workers;
    for (n = 0; n < instrumentation->num_workers; ++n)
        InstrAggNode(planstate->instrument, &instrument[n]);

    /*
     * Also store the per-worker detail.
     *
     * Worker instrumentation should be allocated in the same context as the
     * regular instrumentation information, which is the per-query context.
     * Switch into per-query memory context.
     */
    oldcontext = MemoryContextSwitchTo(planstate->state->es_query_cxt);
    ibytes = mul_size(instrumentation->num_workers, sizeof(Instrumentation));
    planstate->worker_instrument =
        palloc(ibytes + offsetof(WorkerInstrumentation, instrument));
#ifndef __TBASE__
    MemoryContextSwitchTo(oldcontext);
#endif

    planstate->worker_instrument->num_workers = instrumentation->num_workers;
    memcpy(&planstate->worker_instrument->instrument, instrument, ibytes);

	/* Perform any node-type-specific work that needs to be done. */
	switch (nodeTag(planstate))
	{
		case T_SortState:
		ExecSortRetrieveInstrumentation((SortState *) planstate);
			break;
		case T_HashState:
			ExecHashRetrieveInstrumentation((HashState *) planstate);
			break;
		default:
			break;
	}
#ifdef __TBASE__
	/* also retrieve instrumentation from remote */
	if (planstate->dn_instrument != NULL)
	{
		DatanodeInstrumentation *tmp_instrument = planstate->dn_instrument;
		int     nnode = planstate->dn_instrument->nnode;
		Size    size = offsetof(DatanodeInstrumentation, instrument) +
		               mul_size(nnode, sizeof(RemoteInstrumentation));
		
		elog(DEBUG1, "retrieve downstream instrumentation, plan_node_id %d nnode %d", plan_node_id, nnode);
		
		planstate->dn_instrument = palloc0(size);
		memcpy(planstate->dn_instrument, tmp_instrument, size);
	}
	/*
	 * TBase switch memory context later to keep retrieved instrumentation live until 
	 * sending them back to upstream.
	 */
	MemoryContextSwitchTo(oldcontext);
#endif

    return planstate_tree_walker(planstate, ExecParallelRetrieveInstrumentation,
                                 instrumentation);
}

/*
 * Finish parallel execution.  We wait for parallel workers to finish, and
 * accumulate their buffer usage and instrumentation.
 */
void
ExecParallelFinish(ParallelExecutorInfo *pei)
{
    int            i;

    if (pei->finished)
        return;

    /* First, wait for the workers to finish. */
    WaitForParallelWorkersToFinish(pei->pcxt);

    /* Next, accumulate buffer usage. */
    if (pei->pcxt->toc != NULL)
    {
        for (i = 0; i < pei->pcxt->nworkers_launched; ++i)
            InstrAccumParallelQuery(&pei->buffer_usage[i]);
    }

    /* Finally, accumulate instrumentation, if any. */
    if (pei->instrumentation)
        ExecParallelRetrieveInstrumentation(pei->planstate,
                                            pei->instrumentation);

    pei->finished = true;
}

/*
 * Clean up whatever ParallelExecutorInfo resources still exist after
 * ExecParallelFinish.  We separate these routines because someone might
 * want to examine the contents of the DSM after ExecParallelFinish and
 * before calling this routine.
 */
void
ExecParallelCleanup(ParallelExecutorInfo *pei)
{
    if (pei->area != NULL)
    {
        dsa_detach(pei->area);
        pei->area = NULL;
    }
    if (pei->pcxt != NULL)
    {
        DestroyParallelContext(pei->pcxt);
        pei->pcxt = NULL;
    }
    pfree(pei);
}

/*
 * Create a DestReceiver to write tuples we produce to the shm_mq designated
 * for that purpose.
 */
static DestReceiver *
ExecParallelGetReceiver(dsm_segment *seg, shm_toc *toc)
{
    char       *mqspace;
    shm_mq       *mq;

    mqspace = shm_toc_lookup(toc, PARALLEL_KEY_TUPLE_QUEUE, false);
    mqspace += ParallelWorkerNumber * PARALLEL_TUPLE_QUEUE_SIZE;
    mq = (shm_mq *) mqspace;
    shm_mq_set_sender(mq, MyProc);
    return CreateTupleQueueDestReceiver(shm_mq_attach(mq, seg, NULL));
}

/*
 * Create a QueryDesc for the PlannedStmt we are to execute, and return it.
 */
static QueryDesc *
ExecParallelGetQueryDesc(shm_toc *toc, DestReceiver *receiver,
                         int instrument_options)
{
    char       *pstmtspace;
    char       *paramspace;
    PlannedStmt *pstmt;
    ParamListInfo paramLI;
    char       *queryString;

    /* Get the query string from shared memory */
    queryString = shm_toc_lookup(toc, PARALLEL_KEY_QUERY_TEXT, false);

    /* Reconstruct leader-supplied PlannedStmt. */
    pstmtspace = shm_toc_lookup(toc, PARALLEL_KEY_PLANNEDSTMT, false);
    pstmt = (PlannedStmt *) stringToNode(pstmtspace);

    /* Reconstruct ParamListInfo. */
    paramspace = shm_toc_lookup(toc, PARALLEL_KEY_PARAMS, false);
    paramLI = RestoreParamList(&paramspace);

    /*
     * Create a QueryDesc for the query.
     *
     * It's not obvious how to obtain the query string from here; and even if
     * we could copying it would take more cycles than not copying it. But
     * it's a bit unsatisfying to just use a dummy string here, so consider
     * revising this someday.
     */
    return CreateQueryDesc(pstmt,
                           queryString,
                           GetActiveSnapshot(), InvalidSnapshot,
                           receiver, paramLI, NULL, instrument_options);
}

/*
 * Copy instrumentation information from this node and its descendants into
 * dynamic shared memory, so that the parallel leader can retrieve it.
 */
static bool
ExecParallelReportInstrumentation(PlanState *planstate,
                                  SharedExecutorInstrumentation *instrumentation)
{
    int            i;
    int            plan_node_id = planstate->plan->plan_node_id;
    Instrumentation *instrument;

    InstrEndLoop(planstate->instrument);

    /*
     * If we shuffled the plan_node_id values in ps_instrument into sorted
     * order, we could use binary search here.  This might matter someday if
     * we're pushing down sufficiently large plan trees.  For now, do it the
     * slow, dumb way.
     */
    for (i = 0; i < instrumentation->num_plan_nodes; ++i)
        if (instrumentation->plan_node_id[i] == plan_node_id)
            break;
    if (i >= instrumentation->num_plan_nodes)
        elog(ERROR, "plan node %d not found", plan_node_id);

    /*
     * Add our statistics to the per-node, per-worker totals.  It's possible
     * that this could happen more than once if we relaunched workers.
     */
    instrument = GetInstrumentationArray(instrumentation);
    instrument += i * instrumentation->num_workers;
    Assert(IsParallelWorker());
    Assert(ParallelWorkerNumber < instrumentation->num_workers);
    InstrAggNode(&instrument[ParallelWorkerNumber], planstate->instrument);

    return planstate_tree_walker(planstate, ExecParallelReportInstrumentation,
                                 instrumentation);
}

/*
 * Initialize the PlanState and its descendants with the information
 * retrieved from shared memory.  This has to be done once the PlanState
 * is allocated and initialized by executor; that is, after ExecutorStart().
 */
static bool
ExecParallelInitializeWorker(PlanState *planstate, ParallelWorkerContext *pwcxt)
{
    if (planstate == NULL)
        return false;

        switch (nodeTag(planstate))
        {
            case T_SeqScanState:
			if (planstate->plan->parallel_aware)
				ExecSeqScanInitializeWorker((SeqScanState *) planstate, pwcxt);
                break;
            case T_IndexScanState:
			if (planstate->plan->parallel_aware)
				ExecIndexScanInitializeWorker((IndexScanState *) planstate,
											  pwcxt);
                break;
            case T_IndexOnlyScanState:
			if (planstate->plan->parallel_aware)
				ExecIndexOnlyScanInitializeWorker((IndexOnlyScanState *) planstate,
												  pwcxt);
                break;
            case T_ForeignScanState:
			if (planstate->plan->parallel_aware)
                ExecForeignScanInitializeWorker((ForeignScanState *) planstate,
												pwcxt);
                break;
            case T_CustomScanState:
			if (planstate->plan->parallel_aware)
                ExecCustomScanInitializeWorker((CustomScanState *) planstate,
				                               pwcxt);
                break;
            case T_BitmapHeapScanState:
			if (planstate->plan->parallel_aware)
				ExecBitmapHeapInitializeWorker((BitmapHeapScanState *) planstate, pwcxt);
                break;
		case T_HashState:
			/* even when not parallel-aware, for EXPLAIN ANALYZE */
			ExecHashInitializeWorker((HashState *) planstate, pwcxt);
			break;
		case T_SortState:
			/* even when not parallel-aware, for EXPLAIN ANALYZE */
			ExecSortInitializeWorker((SortState *) planstate, pwcxt);
#ifdef __TBASE__
			if (planstate->plan->parallel_aware)
				ReDistributeInitializeWorker(planstate, pwcxt);
			break;
            case T_RemoteQueryState:
			if (planstate->plan->parallel_aware)
				ExecRemoteQueryInitializeDSMWorker((RemoteQueryState *)planstate, pwcxt);
                break;
            case T_RemoteSubplanState: 
			if (planstate->instrument && NULL == planstate->lefttree)
			{
				/* if instrument needed, init full plantree in worker */
				planstate->lefttree = ExecInitNode(planstate->plan->lefttree,
				                                   planstate->state,
				                                   EXEC_FLAG_EXPLAIN_ONLY);
				/* attach share memory for it's child */
				planstate_tree_walker(planstate, ExecInitializeWorkerRemoteInstr, pwcxt);
			}
            if (planstate->plan->parallel_aware)        
				ExecRemoteSubPlanInitDSMWorker((RemoteSubplanState *)planstate, pwcxt);
			break;
		case T_HashJoinState:
			if (planstate->plan->parallel_aware)
				ExecParallelHashJoinInitWorker((HashJoinState *) planstate, pwcxt);
                break;
            case T_AggState:
			    if (planstate->plan->parallel_aware)
                {
                    AggState *aggstate = (AggState *)planstate;

                    if (aggstate->aggstrategy == AGG_HASHED)
					ReDistributeInitializeWorker(planstate, pwcxt);
                }
                break;
#endif
            default:
                break;
        }

	return planstate_tree_walker(planstate, ExecParallelInitializeWorker,
								 pwcxt);
}

#ifdef __TBASE__
/*
 * Estimate share memory space for plan nodes executed by remote, they contain instruments
 * from all datanodes involved, and only leader worker receive these instruments.
 */
static bool
ExecParallelEstimateRemoteInstr(PlanState *node, ExecParallelRemoteInstrContext *ri)
{
	ParallelContext *pcxt = ri->pcxt;
	Size size = mul_size(ri->ndatanode, sizeof(RemoteInstrumentation));
	size = add_size(size, offsetof(DatanodeInstrumentation, instrument));
	
	if (node == NULL)
		return false;
	
	/*
	 * only remote plan node could be here, we need disable parallel for these nodes
	 * to prevent them from initializing other share memory for execution, they don't
	 * need that, only init share memory for instrument collecting.
	 */
	node->plan->parallel_aware = false;
	
	shm_toc_estimate_chunk(&pcxt->estimator, size);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	
	/* for sub-plan */
	if (IsA(node, RemoteSubplanState) && node->lefttree == NULL)
	{
		node->lefttree = ExecInitNode(node->plan->lefttree,
		                              node->state,
		                              EXEC_FLAG_EXPLAIN_ONLY);
	}
	
	elog(DEBUG1, "parallel estimate shm remote instrument for plan node %d", node->plan->plan_node_id);
	
	return planstate_tree_walker(node, ExecParallelEstimateRemoteInstr,
	                             ri);
}

/*
 * Allocate share memory space for plan nodes executed by remote, they contain instruments
 * from all datanodes involved, and only leader worker receive these instruments. use
 * plan_node_id + offset as a unique key.
 */
static bool
ExecParallelInitRemoteInstrDSM(PlanState *node, ExecParallelRemoteInstrContext *ri)
{
	ParallelContext *pcxt = ri->pcxt;
	Size size = mul_size(ri->ndatanode, sizeof(RemoteInstrumentation));
	size = add_size(size, offsetof(DatanodeInstrumentation, instrument));
	
	if (node == NULL)
		return false;
	
	node->dn_instrument = shm_toc_allocate(pcxt->toc, size);
	memset(node->dn_instrument, 0, size);
	node->dn_instrument->nnode = ri->ndatanode;
	shm_toc_insert(pcxt->toc, node->plan->plan_node_id + PARALLEL_REMOTEINSTR_OFFSET,
	               node->dn_instrument);
	
	elog(DEBUG1, "parallel allocate shm remote instrument for plan node %d", node->plan->plan_node_id);
	
	return planstate_tree_walker(node, ExecParallelInitRemoteInstrDSM,
	                             ri);
}

/*
 * Fetch the share memory for plan nodes executed by remote, they will be fulfilled
 * with instruments during RemoteSubplan node's execution. use plan_node_id + offset
 * as the unique key.
 */
static bool
ExecInitializeWorkerRemoteInstr(PlanState *planstate, ParallelWorkerContext *pwcxt)
{
	/*
	 * only remote plan node could be here, we need disable parallel for these nodes
	 * to prevent them from initializing other share memory for execution, they don't
	 * need that, only init share memory for instrument collecting.
	 */
	planstate->plan->parallel_aware = false;
	planstate->dn_instrument = shm_toc_lookup(pwcxt->toc,
	                                          planstate->plan->plan_node_id + PARALLEL_REMOTEINSTR_OFFSET,
	                                          false);
	
	/* for sub-plan */
	if (IsA(planstate, RemoteSubplanState) && planstate->lefttree == NULL)
	{
		planstate->lefttree = ExecInitNode(planstate->plan->lefttree,
		                                   planstate->state,
		                                   EXEC_FLAG_EXPLAIN_ONLY);
	}
	
	elog(DEBUG1, "parallel init worker remote instrument for plan node %d", planstate->plan->plan_node_id);
	
	return planstate_tree_walker(planstate, ExecInitializeWorkerRemoteInstr,
	                             pwcxt);
}
#endif

/*
 * Main entrypoint for parallel query worker processes.
 *
 * We reach this function from ParallelWorkerMain, so the setup necessary to
 * create a sensible parallel environment has already been done;
 * ParallelWorkerMain worries about stuff like the transaction state, combo
 * CID mappings, and GUC values, so we don't need to deal with any of that
 * here.
 *
 * Our job is to deal with concerns specific to the executor.  The parallel
 * group leader will have stored a serialized PlannedStmt, and it's our job
 * to execute that plan and write the resulting tuples to the appropriate
 * tuple queue.  Various bits of supporting information that we need in order
 * to do this are also stored in the dsm_segment and can be accessed through
 * the shm_toc.
 */
void
ParallelQueryMain(dsm_segment *seg, shm_toc *toc)
{// #lizard forgives
    BufferUsage *buffer_usage;
    DestReceiver *receiver;
    QueryDesc  *queryDesc;
    SharedExecutorInstrumentation *instrumentation;
    int            instrument_options = 0;
    void       *area_space;
    dsa_area   *area;
	ParallelWorkerContext pwcxt;
#ifdef __TBASE__
    int i                               = 0;
    int nWorkers                        = 0;
    ParallelWorkerStatus *worker_status = NULL;
    bool *parallel_send                 = NULL;
    dsm_handle *handle                  = NULL;
#endif

#ifdef __TBASE__
    parallel_send = shm_toc_lookup(toc, PARALLEL_KEY_WORKERS_SEND, false);

    parallelExecutionError = shm_toc_lookup(toc, PARALLEL_KEY_EXEC_ERROR, false);

    if (*parallel_send)
    {
        handle = shm_toc_lookup(toc, PARALLEL_KEY_WORKERS_SEG_HANDLE, false);

        receiver = GetParallelSendReceiver(*handle);
    }
    else
    {
#endif
    /* Set up DestReceiver, SharedExecutorInstrumentation, and QueryDesc. */
    receiver = ExecParallelGetReceiver(seg, toc);
#ifdef __TBASE__
        {
            bool *execute_done = shm_toc_lookup(toc, PARALLEL_KEY_EXEC_DONE, false);

            ParallelReceiverSetExecuteDone(receiver, execute_done);
        }
    }
#endif

    instrumentation = shm_toc_lookup(toc, PARALLEL_KEY_INSTRUMENTATION, true);
    if (instrumentation != NULL)
        instrument_options = instrumentation->instrument_options;
    queryDesc = ExecParallelGetQueryDesc(toc, receiver, instrument_options);

    /* Setting debug_query_string for individual workers */
    debug_query_string = queryDesc->sourceText;
    
#ifdef _MLS_
    if (is_mls_user())
    {
        debug_query_string = mls_query_string_prune(debug_query_string);
    }
    /* Report workers' query for monitoring purposes */
    pgstat_report_activity(STATE_RUNNING, debug_query_string);
#endif
    
    /* Prepare to track buffer usage during query execution. */
    InstrStartParallelQuery();

    /* Attach to the dynamic shared memory area. */
    area_space = shm_toc_lookup(toc, PARALLEL_KEY_DSA, false);
    area = dsa_attach_in_place(area_space, seg);

#ifdef __TBASE__
    /* Attach to each worker's dynamic shared memory area */
    worker_status = GetParallelWorkerStatusInfo(toc);
    nWorkers      = worker_status->numExpectedWorkers;
    currentParallelWorkerArea = (dsa_area **)palloc(sizeof(dsa_area *) * nWorkers);

    for(i = 0; i < nWorkers; i++)
    {
        area_space = shm_toc_lookup(toc, (PARALLEL_KEY_FIRST_WORKER_DSA + i),
                                    false);
        currentParallelWorkerArea[i] = dsa_attach_in_place(area_space, seg);
    }

    if (queryDesc->plannedstmt->nParamExec)
    {
        int i = 0;

        int offset = 0;

        char *paramValue = NULL;

        int len = 0;
        
        int nParamExec = queryDesc->plannedstmt->nParamExec;

        ParamExecData *es_param_exec_vals = NULL;
        
        es_param_exec_vals = shm_toc_lookup(toc, PARALLEL_KEY_EXEC_PARAM_LIST, false);

        queryDesc->es_param_exec_vals = (ParamExecData *)palloc0(nParamExec * sizeof(ParamExecData));

        memcpy(queryDesc->es_param_exec_vals, es_param_exec_vals, nParamExec * sizeof(ParamExecData));

        for (i = 0; i < nParamExec; i++)
        {
            if (!queryDesc->es_param_exec_vals[i].isnull && OidIsValid(queryDesc->es_param_exec_vals[i].ptype))
            {
                Oid            typinput;
                Oid            typioparam;
                char       *pstring;
                Oid         ptype = queryDesc->es_param_exec_vals[i].ptype;
                
                if (!paramValue)
                {
                    paramValue = shm_toc_lookup(toc, PARALLEL_KEY_EXEC_PARAM_VALUE,
                                                false);
                }

                memcpy((char *)&len, paramValue + offset, 4);
                offset += 4;   /* data len + '\0' */

                pstring = (char *)palloc(len + 1);

                memcpy(pstring, paramValue + offset, len);

                pstring[len] = '\0';

                getTypeInputInfo(ptype, &typinput, &typioparam);

                queryDesc->es_param_exec_vals[i].value = OidInputFunctionCall(typinput, pstring, typioparam, -1);

                offset += len;

                pfree(pstring);
            }
            else
            {
                queryDesc->es_param_exec_vals[i].isnull = true;
                queryDesc->es_param_exec_vals[i].execPlan = NULL;
                queryDesc->es_param_exec_vals[i].value = 0;
            }
        }
    }
#endif
	pwcxt.toc = toc;
	pwcxt.seg = seg;
    /* Start up the executor */
    ExecutorStart(queryDesc, 0);

    /* Special executor initialization steps for parallel workers */
    queryDesc->planstate->state->es_query_dsa = area;
	ExecParallelInitializeWorker(queryDesc->planstate, &pwcxt);

    /* Run the plan */
    ExecutorRun(queryDesc, ForwardScanDirection, 0L, true);

    /* Shut down the executor */
    ExecutorFinish(queryDesc);

    /* Report buffer usage during parallel execution. */
    buffer_usage = shm_toc_lookup(toc, PARALLEL_KEY_BUFFER_USAGE, false);
    InstrEndParallelQuery(&buffer_usage[ParallelWorkerNumber]);

    /* Report instrumentation data if any instrumentation options are set. */
    if (instrumentation != NULL)
        ExecParallelReportInstrumentation(queryDesc->planstate,
                                          instrumentation);

    /* Must do this after capturing instrumentation. */
    ExecutorEnd(queryDesc);

    /* Cleanup. */
    dsa_detach(area);
#ifdef __TBASE__
    for(i = 0; i < nWorkers; i++)
    {
        dsa_detach(currentParallelWorkerArea[i]);
    }
    pfree(currentParallelWorkerArea);
#endif
    FreeQueryDesc(queryDesc);
    (*receiver->rDestroy) (receiver);
}

#ifdef __TBASE__
ParallelWorkerStatus *GetParallelWorkerStatusInfo(shm_toc *toc)
{
    ParallelWorkerStatus *worker_status = NULL;
    worker_status = (ParallelWorkerStatus*)shm_toc_lookup(toc, PARALLEL_KEY_WORKERS_TOTAL_NUM, false);
    return worker_status;
}

int32 ExecGetForWorkerNumber(ParallelWorkerStatus *worker_status)
{
    if (worker_status)
    {
        /* Get worker number, loop until we get the number. */
        do
        {
            if (worker_status->parallelWorkersSetupDone)
            {
                return worker_status->numLaunchedWorkers;
            }
            
            pg_usleep(1000L);
        }while(!worker_status->parallelWorkersSetupDone);
    }
    return 0;
}

/* allocate share memory for parallel worker from dsa */
void *
ParallelWorkerShmAlloc(Size size, bool zero)
{
    dsa_pointer dp;
    void *shm_ptr = NULL;

    if(!currentParallelWorkerArea[0])
    {
        elog(ERROR, "parallel worker dsa does not initialize.");
    }

    if(zero)
    {
        dp = dsa_allocate0(currentParallelWorkerArea[0], size);
    }
    else
    {
        dp = dsa_allocate(currentParallelWorkerArea[0], size);
    }
    shm_ptr = dsa_get_address(currentParallelWorkerArea[0], dp);

    return shm_ptr;
}
dsa_area *
GetNumWorkerDsa(int workerNumber)
{
    return currentParallelWorkerArea[workerNumber];
}

bool
ParallelError(void)
{    
    if (parallelExecutionError)
    {
        return (*parallelExecutionError);
    }

    return false;
}

void 
HandleParallelExecutionError(void)
{
    if (parallelExecutionError)
    {
        *parallelExecutionError = true;

        SetParallelSendError();

        parallelExecutionError = NULL;
    }
}
#endif
