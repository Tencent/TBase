/*-------------------------------------------------------------------------
 *
 * nodeHashjoin.c
 *      Routines to handle hash join nodes
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *      src/backend/executor/nodeHashjoin.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "executor/executor.h"
#include "executor/hashjoin.h"
#include "executor/nodeHash.h"
#include "executor/nodeHashjoin.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#ifdef __TBASE__
#include "access/xact.h"
#include "executor/execParallel.h"
#endif

/*
 * States of the ExecHashJoin state machine
 */
#define HJ_BUILD_HASHTABLE        1
#define HJ_NEED_NEW_OUTER        2
#define HJ_SCAN_BUCKET            3
#define HJ_FILL_OUTER_TUPLE        4
#define HJ_FILL_INNER_TUPLES    5
#define HJ_NEED_NEW_BATCH        6

/* Returns true if doing null-fill on outer relation */
#define HJ_FILL_OUTER(hjstate)    ((hjstate)->hj_NullInnerTupleSlot != NULL)
/* Returns true if doing null-fill on inner relation */
#define HJ_FILL_INNER(hjstate)    ((hjstate)->hj_NullOuterTupleSlot != NULL)

#ifdef __TBASE__
volatile ParallelHashJoinStatus *statusParallelWorker = NULL;
#endif
static TupleTableSlot *ExecHashJoinOuterGetTuple(PlanState *outerNode,
                          HashJoinState *hjstate,
                          uint32 *hashvalue);
static TupleTableSlot *ExecHashJoinGetSavedTuple(HashJoinState *hjstate,
                          BufFile *file,
                          uint32 *hashvalue,
                          TupleTableSlot *tupleSlot);
static bool ExecHashJoinNewBatch(HashJoinState *hjstate);

#ifdef __TBASE__
static void ExecShareBufFileName(volatile ParallelHashJoinState *parallelState, HashJoinTable hashtable, bool inner);
static HashJoinTable ExecMergeShmHashTable(HashJoinState * hjstate, volatile ParallelHashJoinState *parallelState, 
                                Hash *node, List *hashOperators, bool keepNulls);
static void ExecFormNewOuterBufFile(HashJoinState * hjstate, volatile ParallelHashJoinState *parallelState, 
                                 Hash *node);

#endif
/* ----------------------------------------------------------------
 *        ExecHashJoin
 *
 *        This function implements the Hybrid Hashjoin algorithm.
 *
 *        Note: the relation we build hash table on is the "inner"
 *              the other one is "outer".
 * ----------------------------------------------------------------
 */
static TupleTableSlot *            /* return: a tuple or NULL */
ExecHashJoin(PlanState *pstate)
{// #lizard forgives
    HashJoinState *node = castNode(HashJoinState, pstate);
    PlanState  *outerNode;
    HashState  *hashNode;
    HashJoin   *hashJoin;
    ExprState  *joinqual;
    ExprState  *otherqual;
    ExprContext *econtext;
    HashJoinTable hashtable;
    TupleTableSlot *outerTupleSlot;
    uint32        hashvalue;
    int            batchno;
#ifdef __TBASE__
    volatile ParallelHashJoinState *parallelState;
#endif
    /*
     * get information from HashJoin node
     */
    joinqual = node->js.joinqual;
    otherqual = node->js.ps.qual;
    hashNode = (HashState *) innerPlanState(node);
    outerNode = outerPlanState(node);
    hashtable = node->hj_HashTable;
    econtext = node->js.ps.ps_ExprContext;
#ifdef __TBASE__
    parallelState = node->hj_parallelState;
#endif
    /*
     * Reset per-tuple memory context to free any expression evaluation
     * storage allocated in the previous tuple cycle.
     */
    ResetExprContext(econtext);

    /*
     * run the hash join state machine
     */
    for (;;)
    {
        /*
         * It's possible to iterate this loop many times before returning a
         * tuple, in some pathological cases such as needing to move much of
         * the current batch to a later batch.  So let's check for interrupts
         * each time through.
         */
        CHECK_FOR_INTERRUPTS();

        switch (node->hj_JoinState)
        {
            case HJ_BUILD_HASHTABLE:

                /*
                 * First time through: build hash table for inner relation.
                 */
                Assert(hashtable == NULL);

                /*
                 * If the outer relation is completely empty, and it's not
                 * right/full join, we can quit without building the hash
                 * table.  However, for an inner join it is only a win to
                 * check this when the outer relation's startup cost is less
                 * than the projected cost of building the hash table.
                 * Otherwise it's best to build the hash table first and see
                 * if the inner relation is empty.  (When it's a left join, we
                 * should always make this check, since we aren't going to be
                 * able to skip the join on the strength of an empty inner
                 * relation anyway.)
                 *
                 * If we are rescanning the join, we make use of information
                 * gained on the previous scan: don't bother to try the
                 * prefetch if the previous scan found the outer relation
                 * nonempty. This is not 100% reliable since with new
                 * parameters the outer relation might yield different
                 * results, but it's a good heuristic.
                 *
                 * The only way to make the check is to try to fetch a tuple
                 * from the outer plan node.  If we succeed, we have to stash
                 * it away for later consumption by ExecHashJoinOuterGetTuple.
                 */
                hashJoin = (HashJoin*)node->js.ps.plan;
                if (HJ_FILL_INNER(node))
                {
                    /* no chance to not build the hash table */
                    node->hj_FirstOuterTupleSlot = NULL;
                }
                else if (HJ_FILL_OUTER(node) ||
                         (outerNode->plan->startup_cost < hashNode->ps.plan->total_cost &&
                          !node->hj_OuterNotEmpty))
                {
#ifdef __TBASE__
                    /* When we need to prefetch inner, we just assume there is at lease one row from outer plan */
                    if (!hashJoin->join.prefetch_inner)
                    {
                        node->hj_OuterInited = true;
#endif
                        node->hj_FirstOuterTupleSlot = ExecProcNode(outerNode);
                        if (TupIsNull(node->hj_FirstOuterTupleSlot))
                        {
                            node->hj_OuterNotEmpty = false;

#ifdef __TBASE__
                            if(!IsParallelWorker() || !parallelState)
                            {
                                if (!node->hj_InnerInited && IS_PGXC_DATANODE)
                                {
                                    ExecDisconnectNode(&hashNode->ps);
                                }
                                return NULL;
                            }
                            else
                                parallelState->statusParallelWorker[ParallelWorkerNumber] = ParallelHashJoin_EmptyOuter;
#else
                            return NULL;
#endif
                        }
                        else
                            node->hj_OuterNotEmpty = true;
#ifdef __TBASE__
                    }
                    else
                    {
                        node->hj_FirstOuterTupleSlot = NULL;
                    }
#endif
                }
                else
                {
                    node->hj_FirstOuterTupleSlot = NULL;
                }

#ifdef __TBASE__
                if(IsParallelWorker() && parallelState)
                {
#if 1
                    /* no tuples from outer, check other workers to see if we can stop here. */
                    if (parallelState->statusParallelWorker[ParallelWorkerNumber] == ParallelHashJoin_EmptyOuter)
                    {
                        int  i           = 0;
                        int  nEmptyOuter = 1;
                        int  nWorkers    = parallelState->numLaunchedParallelWorkers;
                        bool *checked    = (bool *)palloc0(sizeof(bool) * nWorkers);
                        bool  exit       = false;

                        while (nEmptyOuter < nWorkers)
                        {
                            if (i != ParallelWorkerNumber && !checked[i])
                            {
                                if (parallelState->statusParallelWorker[i] >= ParallelHashJoin_BuildShmHashTable &&
                                    parallelState->statusParallelWorker[i] <= ParallelHashJoin_MergeShmHashTableDone)
                                {
                                    break;
                                }
                                else if (parallelState->statusParallelWorker[i] == ParallelHashJoin_EmptyOuter)
                                {
                                    nEmptyOuter++;
                                    checked[i] = true;
                                }
                                else if (!hashNode->ps.plan->parallel_aware)
                                {
                                    exit = true;
                                    break;
                                }
                                else if (parallelState->statusParallelWorker[i] == ParallelHashJoin_Error || ParallelError())
                                {
                                    elog(ERROR, "[%s:%d]some other workers exit with errors, and we need to exit because"
                                                " of data corrupted.", __FILE__, __LINE__);
                                }
                                    
                            }

                            i = (i + 1) % nWorkers;
                            pg_usleep(10L);
                        }

                        pfree(checked);

                        if (nEmptyOuter == nWorkers || exit)
                        {
                            elog(LOG, "worker %d ExecHashJoin no tuples from outer!", ParallelWorkerNumber);
                            ExecDisconnectNode(&hashNode->ps);
                            return NULL;
                        }
                    }
#endif        
                    if (hashNode->ps.plan->parallel_aware)
                    {
                        node->hj_InnerInited = true;
                        /* 
                          * create hashtable in share-memory
                          */
                        parallelState->statusParallelWorker[ParallelWorkerNumber] = ParallelHashJoin_BuildShmHashTable;
                        hashtable = ExecShmHashTableCreate((Hash *) hashNode->ps.plan,
                                                            node->hj_HashOperators,
                                                            HJ_FILL_INNER(node));

                        hashNode->hashtable = hashtable;

                        /* 
                          * build hashtable in share-memory, and set the worker status to tell other worker our hashtable
                          * has been finished.
                          */
                        (void)MultiExecShmHash((HashState *) hashNode);

                        /*
                          * put hashtable bufile's file name into shm, so other workers can access
                          */
                        parallelState->hashTableParallelWorker[ParallelWorkerNumber] = (dsa_pointer)hashtable;
                        ExecShareBufFileName(parallelState, hashtable, true);
                        parallelState->statusParallelWorker[ParallelWorkerNumber] = ParallelHashJoin_BuildShmHashTableDone;

                        /* merge shared hashtable, copy into local hashtable */
                        hashtable = ExecMergeShmHashTable(node, parallelState, (Hash *) hashNode->ps.plan,
                                                            node->hj_HashOperators,
                                                            HJ_FILL_INNER(node));
                        node->hj_HashTable = hashtable;
						/* copy into hashNode too, for instrumentation */
						hashNode->hashtable = hashtable;
                        parallelState->statusParallelWorker[ParallelWorkerNumber] = ParallelHashJoin_MergeShmHashTableDone;
                    }
                    else
                    {
                        node->hj_InnerInited = true;
                        /*
                         * create the hash table
                         */
                        hashtable = ExecHashTableCreate((Hash *) hashNode->ps.plan,
                                                        node->hj_HashOperators,
                                                        HJ_FILL_INNER(node));
                        node->hj_HashTable = hashtable;

                        /*
                         * execute the Hash node, to build the hash table
                         */
                        hashNode->hashtable = hashtable;
                        (void) MultiExecProcNode((PlanState *) hashNode);
                    }
                }
                else
                {
#endif
                node->hj_InnerInited = true;
                /*
                 * create the hash table
                 */
                hashtable = ExecHashTableCreate((Hash *) hashNode->ps.plan,
                                                node->hj_HashOperators,
                                                HJ_FILL_INNER(node));
                node->hj_HashTable = hashtable;

                /*
                 * execute the Hash node, to build the hash table
                 */
                hashNode->hashtable = hashtable;
                (void) MultiExecProcNode((PlanState *) hashNode);
#ifdef __TBASE__
                }
#endif
                /*
                 * If the inner relation is completely empty, and we're not
                 * doing a left outer join, we can quit without scanning the
                 * outer relation.
                 */
                if (hashtable->totalTuples == 0 && !HJ_FILL_OUTER(node))
                {
                    if (!node->hj_OuterInited && IS_PGXC_DATANODE)
                    {
                        ExecDisconnectNode(outerNode);
                    }
                    return NULL;
                }

                /*
                 * need to remember whether nbatch has increased since we
                 * began scanning the outer relation
                 */
                hashtable->nbatch_outstart = hashtable->nbatch;

                /*
                 * Reset OuterNotEmpty for scan.  (It's OK if we fetched a
                 * tuple above, because ExecHashJoinOuterGetTuple will
                 * immediately set it again.)
                 */
                node->hj_OuterNotEmpty = false;

                node->hj_JoinState = HJ_NEED_NEW_OUTER;

                /* FALL THRU */

            case HJ_NEED_NEW_OUTER:

                /*
                 * We don't have an outer tuple, try to get the next one
                 */
                outerTupleSlot = ExecHashJoinOuterGetTuple(outerNode,
                                                           node,
                                                           &hashvalue);
                if (TupIsNull(outerTupleSlot))
                {
                    /* end of batch, or maybe whole join */
                    if (HJ_FILL_INNER(node))
                    {
#ifdef __TBASE__
                        /*
                          * In parallel mode, we can also do right or full join
                          * since tuples are hashed into different corresponding
                          * batches. Each worker join some batches, and we can get
                          * the results.
                          * First, we should share each workers' outer batch buffiles to
                          * others.
                          * Second, form new outer batches.
                          * Third, join some batches.
                            */
                          if (IsParallelWorker() && parallelState && 
                            hashNode->ps.plan->parallel_aware)
                          {
                              /* only do it once */
                              if (!node->share_outerBufFile)
                              {
                                  HashJoinTable hashtable = node->hj_HashTable;
                                
                                  if (hashtable->curbatch != 0)
                                  {
                                      elog(ERROR, "only share outer bufFile at end of batch 0,"
                                        "current batch %d, worker %d.", hashtable->curbatch, ParallelWorkerNumber);    
                                  }

                                parallelState->statusParallelWorker[ParallelWorkerNumber] = ParallelHashJoin_ShareOuterBufFile;
                                
                                /* share outer buffiles to other workers */
                                ExecShareBufFileName(parallelState, hashtable, false);
                                
                                parallelState->statusParallelWorker[ParallelWorkerNumber] = ParallelHashJoin_ShareOuterBufFileDone;

                                /* form new outer buffiles */
                                ExecFormNewOuterBufFile(node, parallelState, (Hash *) hashNode->ps.plan);
                                
                                    node->share_outerBufFile = true;
                              }
                          }
#endif
						/* set up to scan for unmatched inner tuples */
						ExecPrepHashTableForUnmatched(node);
						node->hj_JoinState = HJ_FILL_INNER_TUPLES;
					}
					else
						node->hj_JoinState = HJ_NEED_NEW_BATCH;
					continue;
				}
				
				econtext->ecxt_outertuple = outerTupleSlot;
				node->hj_MatchedOuter = false;

				/*
				 * Find the corresponding bucket for this tuple in the main
				 * hash table or skew hash table.
				 */
				node->hj_CurHashValue = hashvalue;
				ExecHashGetBucketAndBatch(hashtable, hashvalue,
										  &node->hj_CurBucketNo, &batchno);
				node->hj_CurSkewBucketNo = ExecHashGetSkewBucket(hashtable,
																 hashvalue);
				node->hj_CurTuple = NULL;

				/*
				 * The tuple might not belong to the current batch (where
				 * "current batch" includes the skew buckets if any).
				 */
				if (batchno != hashtable->curbatch &&
					node->hj_CurSkewBucketNo == INVALID_SKEW_BUCKET_NO)
				{
					/*
					 * Need to postpone this outer tuple to a later batch.
					 * Save it in the corresponding outer-batch file.
					 */
					Assert(batchno > hashtable->curbatch);
					ExecHashJoinSaveTuple(ExecFetchSlotMinimalTuple(outerTupleSlot),
										  hashvalue,
										  &hashtable->outerBatchFile[batchno]);
					/* Loop around, staying in HJ_NEED_NEW_OUTER state */
					continue;
				}

				/* OK, let's scan the bucket for matches */
				node->hj_JoinState = HJ_SCAN_BUCKET;

				/* FALL THRU */

			case HJ_SCAN_BUCKET:

				/*
				 * Scan the selected hash bucket for matches to current outer
				 */
				if (!ExecScanHashBucket(node, econtext))
				{
					/* out of matches; check for possible outer-join fill */
					node->hj_JoinState = HJ_FILL_OUTER_TUPLE;
					continue;
				}

				/*
				 * We've got a match, but still need to test non-hashed quals.
				 * ExecScanHashBucket already set up all the state needed to
				 * call ExecQual.
				 *
				 * If we pass the qual, then save state for next call and have
				 * ExecProject form the projection, store it in the tuple
				 * table, and return the slot.
				 *
				 * Only the joinquals determine tuple match status, but all
				 * quals must pass to actually return the tuple.
				 */
				if (joinqual == NULL || ExecQual(joinqual, econtext))
				{
#ifdef __TBASE__
                    if (node->js.jointype == JOIN_LEFT_SCALAR && node->hj_MatchedOuter)
                        ereport(ERROR,
                                (errcode(ERRCODE_CARDINALITY_VIOLATION),
                                        errmsg("more than one row returned by a subquery used as an expression")));
#endif
					node->hj_MatchedOuter = true;
					HeapTupleHeaderSetMatch(HJTUPLE_MINTUPLE(node->hj_CurTuple));

					/* In an antijoin, we never return a matched tuple */
					if (node->js.jointype == JOIN_ANTI)
					{
						node->hj_JoinState = HJ_NEED_NEW_OUTER;
						continue;
					}

					/*
					 * If we only need to join to the first matching inner
					 * tuple, then consider returning this one, but after that
					 * continue with next outer tuple.
					 */
					if (node->js.single_match)
						node->hj_JoinState = HJ_NEED_NEW_OUTER;

					if (otherqual == NULL || ExecQual(otherqual, econtext))
#ifdef __TBASE__
                    {
                        node->matched_tuples++;
#endif
                        return ExecProject(node->js.ps.ps_ProjInfo);
#ifdef __TBASE__
                    }
#endif
                    else
                        InstrCountFiltered2(node, 1);
                }
                else
                    InstrCountFiltered1(node, 1);
                break;

            case HJ_FILL_OUTER_TUPLE:

                /*
                 * The current outer tuple has run out of matches, so check
                 * whether to emit a dummy outer-join tuple.  Whether we emit
                 * one or not, the next state is NEED_NEW_OUTER.
                 */
                node->hj_JoinState = HJ_NEED_NEW_OUTER;

                if (!node->hj_MatchedOuter &&
                    HJ_FILL_OUTER(node))
                {
                    /*
                     * Generate a fake join tuple with nulls for the inner
                     * tuple, and return it if it passes the non-join quals.
                     */
                    econtext->ecxt_innertuple = node->hj_NullInnerTupleSlot;

                    if (otherqual == NULL || ExecQual(otherqual, econtext))
                        return ExecProject(node->js.ps.ps_ProjInfo);
                    else
                        InstrCountFiltered2(node, 1);
                }
                break;

            case HJ_FILL_INNER_TUPLES:

                /*
                 * We have finished a batch, but we are doing right/full join,
                 * so any unmatched inner tuples in the hashtable have to be
                 * emitted before we continue to the next batch.
                 */
                if (!ExecScanHashTableForUnmatched(node, econtext))
                {
                    /* no more unmatched tuples */
                    node->hj_JoinState = HJ_NEED_NEW_BATCH;
                    continue;
                }

                /*
                 * Generate a fake join tuple with nulls for the outer tuple,
                 * and return it if it passes the non-join quals.
                 */
                econtext->ecxt_outertuple = node->hj_NullOuterTupleSlot;

                if (otherqual == NULL || ExecQual(otherqual, econtext))
                    return ExecProject(node->js.ps.ps_ProjInfo);
                else
                    InstrCountFiltered2(node, 1);
                break;

            case HJ_NEED_NEW_BATCH:

                /*
                 * Try to advance to next batch.  Done if there are no more.
                 */
                if (!ExecHashJoinNewBatch(node))
                    return NULL;    /* end of join */
                node->hj_JoinState = HJ_NEED_NEW_OUTER;
                break;

            default:
                elog(ERROR, "unrecognized hashjoin state: %d",
                     (int) node->hj_JoinState);
        }
    }
}

/* ----------------------------------------------------------------
 *        ExecInitHashJoin
 *
 *        Init routine for HashJoin node.
 * ----------------------------------------------------------------
 */
HashJoinState *
ExecInitHashJoin(HashJoin *node, EState *estate, int eflags)
{
	HashJoinState *hjstate;
	Plan	   *outerNode;
	Hash	   *hashNode;
	List	   *lclauses;
	List	   *rclauses;
	List	   *hoperators;
	ListCell   *l;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * create state structure
	 */
	hjstate = makeNode(HashJoinState);
	hjstate->js.ps.plan = (Plan *) node;
	hjstate->js.ps.state = estate;
	hjstate->js.ps.ExecProcNode = ExecHashJoin;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &hjstate->js.ps);

	/*
	 * initialize child expressions
	 */
	hjstate->js.ps.qual =
		ExecInitQual(node->join.plan.qual, (PlanState *) hjstate);
	hjstate->js.jointype = node->join.jointype;
	hjstate->js.joinqual =
		ExecInitQual(node->join.joinqual, (PlanState *) hjstate);
	hjstate->hashclauses =
		ExecInitQual(node->hashclauses, (PlanState *) hjstate);

	/*
	 * initialize child nodes
	 *
	 * Note: we could suppress the REWIND flag for the inner input, which
	 * would amount to betting that the hash will be a single batch.  Not
	 * clear if this would be a win or not.
	 */
	outerNode = outerPlan(node);
	hashNode = (Hash *) innerPlan(node);

	outerPlanState(hjstate) = ExecInitNode(outerNode, estate, eflags);
	innerPlanState(hjstate) = ExecInitNode((Plan *) hashNode, estate, eflags);

	/*
	 * tuple table initialization
	 */
	ExecInitResultTupleSlot(estate, &hjstate->js.ps);
	hjstate->hj_OuterTupleSlot = ExecInitExtraTupleSlot(estate);

	/*
	 * detect whether we need only consider the first matching inner tuple
	 */
	hjstate->js.single_match = (node->join.inner_unique ||
								node->join.jointype == JOIN_SEMI ||
								node->join.jointype == JOIN_LEFT_SEMI);

	/* set up null tuples for outer joins, if needed */
	switch (node->join.jointype)
	{
		case JOIN_INNER:
        case JOIN_SEMI:
		    break;
#ifdef __TBASE__
		case JOIN_LEFT_SCALAR:
		case JOIN_LEFT_SEMI:
#endif
		case JOIN_LEFT:
		case JOIN_ANTI:
			hjstate->hj_NullInnerTupleSlot =
				ExecInitNullTupleSlot(estate,
									  ExecGetResultType(innerPlanState(hjstate)));
			break;
		case JOIN_RIGHT:
			hjstate->hj_NullOuterTupleSlot =
				ExecInitNullTupleSlot(estate,
									  ExecGetResultType(outerPlanState(hjstate)));
			break;
		case JOIN_FULL:
			hjstate->hj_NullOuterTupleSlot =
				ExecInitNullTupleSlot(estate,
									  ExecGetResultType(outerPlanState(hjstate)));
			hjstate->hj_NullInnerTupleSlot =
				ExecInitNullTupleSlot(estate,
									  ExecGetResultType(innerPlanState(hjstate)));
			break;
		default:
			elog(ERROR, "unrecognized join type: %d",
				 (int) node->join.jointype);
	}

	/*
	 * now for some voodoo.  our temporary tuple slot is actually the result
	 * tuple slot of the Hash node (which is our inner plan).  we can do this
	 * because Hash nodes don't return tuples via ExecProcNode() -- instead
	 * the hash join node uses ExecScanHashBucket() to get at the contents of
	 * the hash table.  -cim 6/9/91
	 */
	{
		HashState  *hashstate = (HashState *) innerPlanState(hjstate);
		TupleTableSlot *slot = hashstate->ps.ps_ResultTupleSlot;

		hjstate->hj_HashTupleSlot = slot;
	}

	/*
	 * initialize tuple type and projection info
	 */
	ExecAssignResultTypeFromTL(&hjstate->js.ps);
	ExecAssignProjectionInfo(&hjstate->js.ps, NULL);

	ExecSetSlotDescriptor(hjstate->hj_OuterTupleSlot,
						  ExecGetResultType(outerPlanState(hjstate)));

	/*
	 * initialize hash-specific info
	 */
	hjstate->hj_HashTable = NULL;
	hjstate->hj_FirstOuterTupleSlot = NULL;

	hjstate->hj_CurHashValue = 0;
	hjstate->hj_CurBucketNo = 0;
	hjstate->hj_CurSkewBucketNo = INVALID_SKEW_BUCKET_NO;
	hjstate->hj_CurTuple = NULL;

	/*
	 * Deconstruct the hash clauses into outer and inner argument values, so
	 * that we can evaluate those subexpressions separately.  Also make a list
	 * of the hash operator OIDs, in preparation for looking up the hash
	 * functions to use.
	 */
	lclauses = NIL;
	rclauses = NIL;
	hoperators = NIL;
	foreach(l, node->hashclauses)
	{
		OpExpr	   *hclause = lfirst_node(OpExpr, l);

		lclauses = lappend(lclauses, ExecInitExpr(linitial(hclause->args),
												  (PlanState *) hjstate));
		rclauses = lappend(rclauses, ExecInitExpr(lsecond(hclause->args),
												  (PlanState *) hjstate));
		hoperators = lappend_oid(hoperators, hclause->opno);
	}
	hjstate->hj_OuterHashKeys = lclauses;
	hjstate->hj_InnerHashKeys = rclauses;
	hjstate->hj_HashOperators = hoperators;
	/* child Hash node needs to evaluate inner hash keys, too */
	((HashState *) innerPlanState(hjstate))->hashkeys = rclauses;

	hjstate->hj_JoinState = HJ_BUILD_HASHTABLE;
	hjstate->hj_MatchedOuter = false;
	hjstate->hj_OuterNotEmpty = false;
#ifdef __TBASE__
    hjstate->hj_OuterInited = false;
    hjstate->hj_InnerInited = false;
#endif

    return hjstate;
}

/* ----------------------------------------------------------------
 *        ExecEndHashJoin
 *
 *        clean up routine for HashJoin node
 * ----------------------------------------------------------------
 */
void
ExecEndHashJoin(HashJoinState *node)
{// #lizard forgives
#ifdef __TBASE__
    HashState *hashNode = (HashState *) innerPlanState(node);
#endif

#ifdef __TBASE__
    if (IsParallelWorker() && node->hj_parallelState && HJ_FILL_INNER(node) &&
        hashNode->ps.plan->parallel_aware)
    {
        int nWorkers = 0;
        int nDone    = 0;
        ParallelHashJoinState *parallelState = node->hj_parallelState;
        volatile ParallelHashJoinStatus *statusParallelWorker = parallelState->statusParallelWorker;

        if (statusParallelWorker[ParallelWorkerNumber] == ParallelHashJoin_EmptyOuter)
        {
            elog(ERROR, "worker %d status abnormal %d in end of hash right/full join.",
                         ParallelWorkerNumber, statusParallelWorker[ParallelWorkerNumber]);
        }
        
        statusParallelWorker[ParallelWorkerNumber] = ParallelHashJoin_ExecJoinDone;

        /* wait for other workers to finish */
        nWorkers = parallelState->numLaunchedParallelWorkers;
        while(nDone < nWorkers)
        {
            if (statusParallelWorker[nDone] == ParallelHashJoin_Error || ParallelError())
            {
                elog(ERROR, "[%s:%d]some other workers exit with errors, and we need to exit because"
                            " of data corrupted.", __FILE__, __LINE__);
            }
            else if(statusParallelWorker[nDone] < ParallelHashJoin_ExecJoinDone &&
                       statusParallelWorker[nDone] > ParallelHashJoin_None)
            {
                pg_usleep(1000L);
            }
            
            else
            {
                nDone++;
            }
        }
    }
#endif

    /*
     * Free hash table
     */
    if (node->hj_HashTable)
    {
        ExecHashTableDestroy(node->hj_HashTable);
        node->hj_HashTable = NULL;
    }
#ifdef __TBASE__
    if(IsParallelWorker() && node->hj_parallelState && hashNode->ps.plan->parallel_aware)
    {
        int nWorkers = 0;
        int nDone    = 0;
        dsa_area *dsa = GetNumWorkerDsa(ParallelWorkerNumber);
        ParallelHashJoinState *parallelState = node->hj_parallelState;
        volatile ParallelHashJoinStatus *statusParallelWorker = parallelState->statusParallelWorker;
        HashJoinTable hashtable = (HashJoinTable)dsa_get_address(dsa, parallelState->hashTableParallelWorker[ParallelWorkerNumber]);

        if (statusParallelWorker[ParallelWorkerNumber] != ParallelHashJoin_EmptyOuter)
        {
            statusParallelWorker[ParallelWorkerNumber] = ParallelHashJoin_ExecJoinDone;
        }

        /* wait for other workers to finish */
        nWorkers = parallelState->numLaunchedParallelWorkers;
        while(nDone < nWorkers)
        {
            if (statusParallelWorker[nDone] == ParallelHashJoin_Error || ParallelError())
            {
                elog(ERROR, "[%s:%d]some other workers exit with errors, and we need to exit because"
                            " of data corrupted.", __FILE__, __LINE__);
            }
            else if(statusParallelWorker[nDone] < ParallelHashJoin_ExecJoinDone &&
                       statusParallelWorker[nDone] > ParallelHashJoin_None &&
                       statusParallelWorker[nDone] != ParallelHashJoin_EmptyOuter)
            {
                pg_usleep(1000L);
            }
            else
            {
                nDone++;
            }
        }

        /* remove bufFile in share hashtable */
        if(hashtable)
        {
            int            i;
            
            for (i = 1; i < hashtable->nbatch; i++)
            {
                if (hashtable->innerBatchFile[i])
                    BufFileClose(hashtable->innerBatchFile[i]);
            }
        }

        elog(DEBUG1, "worker %d ExecHashjoin matched tuples %zu", ParallelWorkerNumber,
                                                               node->matched_tuples);
    }
    else
    {
        elog(DEBUG1, "ExecHashjoin matched tuples %zu", node->matched_tuples);
    }
#endif
    /*
     * Free the exprcontext
     */
    ExecFreeExprContext(&node->js.ps);

    /*
     * clean out the tuple table
     */
    ExecClearTuple(node->js.ps.ps_ResultTupleSlot);
    ExecClearTuple(node->hj_OuterTupleSlot);
    ExecClearTuple(node->hj_HashTupleSlot);

    /*
     * clean up subtrees
     */
    ExecEndNode(outerPlanState(node));
    ExecEndNode(innerPlanState(node));
}

/*
 * ExecHashJoinOuterGetTuple
 *
 *        get the next outer tuple for hashjoin: either by
 *        executing the outer plan node in the first pass, or from
 *        the temp files for the hashjoin batches.
 *
 * Returns a null slot if no more outer tuples (within the current batch).
 *
 * On success, the tuple's hash value is stored at *hashvalue --- this is
 * either originally computed, or re-read from the temp file.
 */
static TupleTableSlot *
ExecHashJoinOuterGetTuple(PlanState *outerNode,
                          HashJoinState *hjstate,
                          uint32 *hashvalue)
{// #lizard forgives
    HashJoinTable hashtable = hjstate->hj_HashTable;
    int            curbatch = hashtable->curbatch;
    TupleTableSlot *slot;

    if (curbatch == 0)            /* if it is the first pass */
    {
        /*
         * Check to see if first outer tuple was already fetched by
         * ExecHashJoin() and not used yet.
         */
        slot = hjstate->hj_FirstOuterTupleSlot;
        if (!TupIsNull(slot))
            hjstate->hj_FirstOuterTupleSlot = NULL;
        else
            slot = ExecProcNode(outerNode);

        while (!TupIsNull(slot))
        {
            /*
             * We have to compute the tuple's hash value.
             */
            ExprContext *econtext = hjstate->js.ps.ps_ExprContext;

            econtext->ecxt_outertuple = slot;
            if (ExecHashGetHashValue(hashtable, econtext,
                                     hjstate->hj_OuterHashKeys,
                                     true,    /* outer tuple */
                                     HJ_FILL_OUTER(hjstate),
                                     hashvalue))
            {
                /* remember outer relation is not empty for possible rescan */
                hjstate->hj_OuterNotEmpty = true;

                return slot;
            }

            /*
             * That tuple couldn't match because of a NULL, so discard it and
             * continue with the next one.
             */
            slot = ExecProcNode(outerNode);
        }
    }
    else if (curbatch < hashtable->nbatch)
    {
        BufFile    *file = hashtable->outerBatchFile[curbatch];

        /*
         * In outer-join cases, we could get here even though the batch file
         * is empty.
         */
        if (file == NULL)
            return NULL;

        slot = ExecHashJoinGetSavedTuple(hjstate,
                                         file,
                                         hashvalue,
                                         hjstate->hj_OuterTupleSlot);
        if (!TupIsNull(slot))
            return slot;
    }

    /* End of this batch */
    return NULL;
}

/*
 * ExecHashJoinNewBatch
 *        switch to a new hashjoin batch
 *
 * Returns true if successful, false if there are no more batches.
 */
static bool
ExecHashJoinNewBatch(HashJoinState *hjstate)
{// #lizard forgives
    HashJoinTable hashtable = hjstate->hj_HashTable;
    int            nbatch;
    int            curbatch;
    BufFile    *innerFile;
    TupleTableSlot *slot;
    uint32        hashvalue;
#ifdef __TBASE__
    HashState  *hashNode = (HashState *) innerPlanState(hjstate);
#endif

    nbatch = hashtable->nbatch;
    curbatch = hashtable->curbatch;

    if (curbatch > 0)
    {
        /*
         * We no longer need the previous outer batch file; close it right
         * away to free disk space.
         */
        if (hashtable->outerBatchFile[curbatch])
            BufFileClose(hashtable->outerBatchFile[curbatch]);
        hashtable->outerBatchFile[curbatch] = NULL;
    }
    else                        /* we just finished the first batch */
    {
        /*
         * Reset some of the skew optimization state variables, since we no
         * longer need to consider skew tuples after the first batch. The
         * memory context reset we are about to do will release the skew
         * hashtable itself.
         */
        hashtable->skewEnabled = false;
        hashtable->skewBucket = NULL;
        hashtable->skewBucketNums = NULL;
        hashtable->nSkewBuckets = 0;
        hashtable->spaceUsedSkew = 0;
    }

    /*
     * We can always skip over any batches that are completely empty on both
     * sides.  We can sometimes skip over batches that are empty on only one
     * side, but there are exceptions:
     *
     * 1. In a left/full outer join, we have to process outer batches even if
     * the inner batch is empty.  Similarly, in a right/full outer join, we
     * have to process inner batches even if the outer batch is empty.
     *
     * 2. If we have increased nbatch since the initial estimate, we have to
     * scan inner batches since they might contain tuples that need to be
     * reassigned to later inner batches.
     *
     * 3. Similarly, if we have increased nbatch since starting the outer
     * scan, we have to rescan outer batches in case they contain tuples that
     * need to be reassigned.
     */
#ifdef __TBASE__
    if (IsParallelWorker() && hjstate->hj_parallelState && HJ_FILL_INNER(hjstate) &&
        hashNode->ps.plan->parallel_aware)
    {
        if (curbatch == 0)
        {
            if (ParallelWorkerNumber == 0)
            {
                curbatch = curbatch + hjstate->hj_parallelState->numLaunchedParallelWorkers;
            }
            else
            {
                curbatch = curbatch + ParallelWorkerNumber;
            }
        }
        else
        {
            curbatch = curbatch + hjstate->hj_parallelState->numLaunchedParallelWorkers;
        }
    }
    else
    {
        curbatch++;
    }
#else
    curbatch++;
#endif
    while (curbatch < nbatch &&
           (hashtable->outerBatchFile[curbatch] == NULL ||
            hashtable->innerBatchFile[curbatch] == NULL))
    {
        if (hashtable->outerBatchFile[curbatch] &&
            HJ_FILL_OUTER(hjstate))
            break;                /* must process due to rule 1 */
        if (hashtable->innerBatchFile[curbatch] &&
            HJ_FILL_INNER(hjstate))
            break;                /* must process due to rule 1 */
        if (hashtable->innerBatchFile[curbatch] &&
            nbatch != hashtable->nbatch_original)
            break;                /* must process due to rule 2 */
        if (hashtable->outerBatchFile[curbatch] &&
            nbatch != hashtable->nbatch_outstart)
            break;                /* must process due to rule 3 */
        /* We can ignore this batch. */
        /* Release associated temp files right away. */
        if (hashtable->innerBatchFile[curbatch])
            BufFileClose(hashtable->innerBatchFile[curbatch]);
        hashtable->innerBatchFile[curbatch] = NULL;
        if (hashtable->outerBatchFile[curbatch])
            BufFileClose(hashtable->outerBatchFile[curbatch]);
        hashtable->outerBatchFile[curbatch] = NULL;
#ifdef __TBASE__
        if (IsParallelWorker() && hjstate->hj_parallelState && HJ_FILL_INNER(hjstate) &&
            hashNode->ps.plan->parallel_aware)
        {
            curbatch = curbatch + hjstate->hj_parallelState->numLaunchedParallelWorkers;
        }
        else
        {
            curbatch++;
        }
#else
        curbatch++;
#endif

    }

    if (curbatch >= nbatch)
        return false;            /* no more batches */

    hashtable->curbatch = curbatch;

    /*
     * Reload the hash table with the new inner batch (which could be empty)
     */
    ExecHashTableReset(hashtable);

    innerFile = hashtable->innerBatchFile[curbatch];

    if (innerFile != NULL)
    {
        if (BufFileSeek(innerFile, 0, 0L, SEEK_SET))
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("could not rewind hash-join temporary file: %m")));

        while ((slot = ExecHashJoinGetSavedTuple(hjstate,
                                                 innerFile,
                                                 &hashvalue,
                                                 hjstate->hj_HashTupleSlot)))
        {
            /*
             * NOTE: some tuples may be sent to future batches.  Also, it is
             * possible for hashtable->nbatch to be increased here!
             */
            ExecHashTableInsert(hashtable, slot, hashvalue);
        }

        /*
         * after we build the hash table, the inner batch file is no longer
         * needed
         */
        BufFileClose(innerFile);
        hashtable->innerBatchFile[curbatch] = NULL;
    }

    /*
     * Rewind outer batch file (if present), so that we can start reading it.
     */
    if (hashtable->outerBatchFile[curbatch] != NULL)
    {
        if (BufFileSeek(hashtable->outerBatchFile[curbatch], 0, 0L, SEEK_SET))
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("could not rewind hash-join temporary file: %m")));
    }

    return true;
}

/*
 * ExecHashJoinSaveTuple
 *        save a tuple to a batch file.
 *
 * The data recorded in the file for each tuple is its hash value,
 * then the tuple in MinimalTuple format.
 *
 * Note: it is important always to call this in the regular executor
 * context, not in a shorter-lived context; else the temp file buffers
 * will get messed up.
 */
void
ExecHashJoinSaveTuple(MinimalTuple tuple, uint32 hashvalue,
                      BufFile **fileptr)
{
    BufFile    *file = *fileptr;
    size_t        written;

    if (file == NULL)
    {
        /* First write to this batch file, so open it. */
        file = BufFileCreateTemp(false);
        *fileptr = file;
    }

    written = BufFileWrite(file, (void *) &hashvalue, sizeof(uint32));
    if (written != sizeof(uint32))
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("could not write to hash-join temporary file: %m")));

    written = BufFileWrite(file, (void *) tuple, tuple->t_len);
    if (written != tuple->t_len)
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("could not write to hash-join temporary file: %m")));
}

/*
 * ExecHashJoinGetSavedTuple
 *        read the next tuple from a batch file.  Return NULL if no more.
 *
 * On success, *hashvalue is set to the tuple's hash value, and the tuple
 * itself is stored in the given slot.
 */
static TupleTableSlot *
ExecHashJoinGetSavedTuple(HashJoinState *hjstate,
                          BufFile *file,
                          uint32 *hashvalue,
                          TupleTableSlot *tupleSlot)
{// #lizard forgives
    uint32        header[2];
    size_t        nread;
    MinimalTuple tuple;

    /*
     * We check for interrupts here because this is typically taken as an
     * alternative code path to an ExecProcNode() call, which would include
     * such a check.
     */
    CHECK_FOR_INTERRUPTS();
#ifdef __TBASE__
RETRY:
#endif
    /*
     * Since both the hash value and the MinimalTuple length word are uint32,
     * we can read them both in one BufFileRead() call without any type
     * cheating.
     */
    nread = BufFileRead(file, (void *) header, sizeof(header));
#ifdef __TBASE__
    /*
      * each bufFile of worker may have more than one file, and some of
      * those files do not exceed the MAX_PHYSICAL_FILESIZE, those files
      * should also be read.
         */
    if(IsParallelWorker())
    {
        if(nread == 0)
        {
            if(BufFileReadDone(file))
            {
                ExecClearTuple(tupleSlot);
                return NULL;
            }
            else
            {
                goto RETRY;
            }
        }
    }
    else
    {
#endif
    if (nread == 0)                /* end of file */
    {
        ExecClearTuple(tupleSlot);
        return NULL;
    }
#ifdef __TBASE__
    }
#endif
    if (nread != sizeof(header))
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("could not read from hash-join temporary file: %m")));
    *hashvalue = header[0];
    tuple = (MinimalTuple) palloc(header[1]);
    tuple->t_len = header[1];
    nread = BufFileRead(file,
                        (void *) ((char *) tuple + sizeof(uint32)),
                        header[1] - sizeof(uint32));
    if (nread != header[1] - sizeof(uint32))
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("could not read from hash-join temporary file: %m")));
    return ExecStoreMinimalTuple(tuple, tupleSlot, true);
}


void
ExecReScanHashJoin(HashJoinState *node)
{
    /*
     * In a multi-batch join, we currently have to do rescans the hard way,
     * primarily because batch temp files may have already been released. But
     * if it's a single-batch join, and there is no parameter change for the
     * inner subnode, then we can just re-use the existing hash table without
     * rebuilding it.
     */
    if (node->hj_HashTable != NULL)
    {
        if (node->hj_HashTable->nbatch == 1 &&
            node->js.ps.righttree->chgParam == NULL)
        {
            /*
             * Okay to reuse the hash table; needn't rescan inner, either.
             *
             * However, if it's a right/full join, we'd better reset the
             * inner-tuple match flags contained in the table.
             */
            if (HJ_FILL_INNER(node))
                ExecHashTableResetMatchFlags(node->hj_HashTable);

            /*
             * Also, we need to reset our state about the emptiness of the
             * outer relation, so that the new scan of the outer will update
             * it correctly if it turns out to be empty this time. (There's no
             * harm in clearing it now because ExecHashJoin won't need the
             * info.  In the other cases, where the hash table doesn't exist
             * or we are destroying it, we leave this state alone because
             * ExecHashJoin will need it the first time through.)
             */
            node->hj_OuterNotEmpty = false;

            /* ExecHashJoin can skip the BUILD_HASHTABLE step */
            node->hj_JoinState = HJ_NEED_NEW_OUTER;
        }
        else
        {
            /* must destroy and rebuild hash table */
            ExecHashTableDestroy(node->hj_HashTable);
            node->hj_HashTable = NULL;
            node->hj_JoinState = HJ_BUILD_HASHTABLE;

            /*
             * if chgParam of subnode is not null then plan will be re-scanned
             * by first ExecProcNode.
             */
            if (node->js.ps.righttree->chgParam == NULL)
                ExecReScan(node->js.ps.righttree);
        }
    }

    /* Always reset intra-tuple state */
    node->hj_CurHashValue = 0;
    node->hj_CurBucketNo = 0;
    node->hj_CurSkewBucketNo = INVALID_SKEW_BUCKET_NO;
    node->hj_CurTuple = NULL;

    node->hj_MatchedOuter = false;
    node->hj_FirstOuterTupleSlot = NULL;

    /*
     * if chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.
     */
    if (node->js.ps.lefttree->chgParam == NULL)
        ExecReScan(node->js.ps.lefttree);
}
#ifdef __TBASE__
/* ----------------------------------------------------------------
 *        ExecParallelHashJoinEstimate
 *
 *        estimates the space required to serialize hashjoin parallel state.
 * ----------------------------------------------------------------
 */
void
ExecParallelHashJoinEstimate(HashJoinState *node, ParallelContext *pcxt)
{
    node->hj_parallelStateLen = ParallelHashJoinState_Size(pcxt->nworkers);
    shm_toc_estimate_chunk(&pcxt->estimator, node->hj_parallelStateLen);
    shm_toc_estimate_keys(&pcxt->estimator, 1);
}


/* ----------------------------------------------------------------
 *        ExecParallelHashJoinInitializeDSM
 *
 *        Set up a parallel hashjoin state for parallel workers
 * ----------------------------------------------------------------
 */
void
ExecParallelHashJoinInitializeDSM(HashJoinState *node,
                                          ParallelContext *pcxt)
{
    int i = 0;
    int offset = 0;
    ParallelHashJoinState *parallelState = NULL;

    parallelState = shm_toc_allocate(pcxt->toc, node->hj_parallelStateLen);

    /* orginize memory allocated */
    offset += sizeof(ParallelHashJoinState);
    parallelState->statusParallelWorker = (ParallelHashJoinStatus *)((char *)parallelState + offset);

    offset += sizeof(ParallelHashJoinStatus) * pcxt->nworkers;
    parallelState->hashTableParallelWorker = (dsa_pointer *)((char *)parallelState + offset);

    offset += sizeof(dsa_pointer) * pcxt->nworkers;
    parallelState->bufFileNames = (dsa_pointer *)((char *)parallelState + offset);

    offset += sizeof(dsa_pointer) * pcxt->nworkers;
    parallelState->outerBufFileNames = (dsa_pointer *)((char *)parallelState + offset);
    
    parallelState->numExpectedParallelWorkers = pcxt->nworkers;
    for(i = 0;i < pcxt->nworkers; i++)
    {
        parallelState->statusParallelWorker[i] = ParallelHashJoin_None;
    }
    
    shm_toc_insert(pcxt->toc, node->js.ps.plan->plan_node_id, parallelState);
    node->hj_parallelState = parallelState;
}

/* ----------------------------------------------------------------
 *        ExecParallelHashJoinInitializeWorker
 *
 *        Copy relevant information from TOC into planstate.
 * ----------------------------------------------------------------
 */
void
ExecParallelHashJoinInitWorker(HashJoinState *node, ParallelWorkerContext *pwcxt)
{
    int offset = 0;
    ParallelHashJoinState *parallelState = NULL;
    volatile ParallelWorkerStatus *numParallelWorkers = NULL;

	parallelState = shm_toc_lookup(pwcxt->toc, node->js.ps.plan->plan_node_id, false);
	numParallelWorkers = GetParallelWorkerStatusInfo(pwcxt->toc);

    node->hj_parallelState = (ParallelHashJoinState *)palloc0(sizeof(ParallelHashJoinState));

    node->hj_parallelState->numExpectedParallelWorkers = parallelState->numExpectedParallelWorkers;
    
    /* orginize memory allocated */
    offset += sizeof(ParallelHashJoinState);
    node->hj_parallelState->statusParallelWorker = (ParallelHashJoinStatus *)((char *)parallelState + offset);

    offset += sizeof(ParallelHashJoinStatus) * numParallelWorkers->numExpectedWorkers;
    node->hj_parallelState->hashTableParallelWorker = (dsa_pointer *)((char *)parallelState + offset);

    offset += sizeof(dsa_pointer) * numParallelWorkers->numExpectedWorkers;
    node->hj_parallelState->bufFileNames = (dsa_pointer *)((char *)parallelState + offset);

    offset += sizeof(dsa_pointer) * numParallelWorkers->numExpectedWorkers;
    node->hj_parallelState->outerBufFileNames = (dsa_pointer *)((char *)parallelState + offset);
    
    /*
      * get total number of launched parallel workers.
      * this number is set by session after launching all parallel workers,
      * so we may need to wait for the setup
      */
    while(!numParallelWorkers->parallelWorkersSetupDone)
    {
        pg_usleep(1000L); 
    }
    node->hj_parallelState->numLaunchedParallelWorkers = numParallelWorkers->numLaunchedWorkers;

    if(node->hj_parallelState->numLaunchedParallelWorkers > 
       node->hj_parallelState->numExpectedParallelWorkers)
    {
        elog(ERROR, "launched parallel workers' total number:%d"
                    "is greater than the expected:%d",
                    node->hj_parallelState->numLaunchedParallelWorkers,
                    node->hj_parallelState->numExpectedParallelWorkers);
    }

    if(ParallelWorkerNumber >= node->hj_parallelState->numLaunchedParallelWorkers)
    {
        elog(ERROR, "parallel worker's number:%d is greater than"
                    "launched parallel workers' total number:%d",
                    ParallelWorkerNumber, node->hj_parallelState->numLaunchedParallelWorkers);
    }
    
    node->hj_parallelState->statusParallelWorker[ParallelWorkerNumber] = ParallelHashJoin_Init;

    statusParallelWorker = node->hj_parallelState->statusParallelWorker;
}

/* 
  * share bufFiles with other parallel workers
  */
static void
ExecShareBufFileName(volatile ParallelHashJoinState *parallelState, HashJoinTable hashtable, bool inner)
{// #lizard forgives
    int i = 0;
    int j = 0;
    int numFiles = 0;
    dsa_pointer dp;
    HashJoinTable ht;
    
    /* get hashtable */
    dsa_area * dsa = GetNumWorkerDsa(ParallelWorkerNumber);

    if (inner)
    {
        ht = (HashJoinTable)dsa_get_address(dsa, (dsa_pointer)hashtable);
    }
    else
    {
        ht = hashtable;
    }
    
    /*
      * allocate space for bufFile's file name in shm.
      * store num_batch in hashtable, num_files in each batch,
      * then file names
      */
    if(ht->nbatch > 1)
    {
        HashTableBufFileName *htbufFileName = NULL;
        int                  *nFiles        = NULL;
        dsa_pointer          *names         = NULL;
            
        dp            = dsa_allocate0(dsa, sizeof(HashTableBufFileName));

        if (inner)
        {
            parallelState->bufFileNames[ParallelWorkerNumber] = dp;
        }
        else
        {
            parallelState->outerBufFileNames[ParallelWorkerNumber] = dp;
        }
        
        htbufFileName = (HashTableBufFileName *)dsa_get_address(dsa, dp);

        htbufFileName->nBatch = ht->nbatch;

        dp            = dsa_allocate0(dsa, sizeof(int) * ht->nbatch);
        htbufFileName->nFiles = dp;
        nFiles        = (int *)dsa_get_address(dsa, dp);

        dp            = dsa_allocate0(dsa, sizeof(dsa_pointer) * ht->nbatch);
        htbufFileName->name   = dp;
        names         = (dsa_pointer *)dsa_get_address(dsa, dp);
        
        for(i = 0; i < ht->nbatch; i++)
        {
            BufFile *file;
            
            if (inner)
            {
                file = ht->innerBatchFile[i];
            }
            else
            {
                int ret = 0;

                if (ht->outerBatchFile[i])
                {
                    /* flush outer bufFile until flush successfully  */
                    do
                    {
                        ret = FlushBufFile(ht->outerBatchFile[i]);
                    } while(ret == EOF);
                }
                
                file = ht->outerBatchFile[i];
            }

            numFiles  = NumFilesBufFile(file);

            nFiles[i] = numFiles;
            
            if(numFiles)
            {
                dsa_pointer *bufFileName = NULL;
                dp                       = dsa_allocate0(dsa, sizeof(dsa_pointer) * numFiles);
                names[i]                 = dp;
                bufFileName              = (dsa_pointer *)dsa_get_address(dsa, dp);

                for(j = 0; j < numFiles; j++)
                {
                    char *fileName = NULL;
                    dp             = dsa_allocate0(dsa, MAXPGPATH);
                    bufFileName[j] = dp;
                    fileName = (char *)dsa_get_address(dsa, dp);
                    snprintf(fileName, MAXPGPATH, "%s", getBufFileName(file, j));
                }
            }
        }
    }
}

/*
  * merge all parallel workers' shared hashtables into local hashtable
  */
static HashJoinTable
ExecMergeShmHashTable(HashJoinState * hjstate, volatile ParallelHashJoinState *parallelState, 
                                Hash *node, List *hashOperators, bool keepNulls)
{// #lizard forgives
    int i            = 0;
    int nMerged      = 0;
    int workerLeader = 0;
    int indexbucket  = 0;
    int indexbatch   = 0;
    int nWorkers     = parallelState->numLaunchedParallelWorkers;
    bool *merged     = (bool *)palloc0(sizeof(bool) * nWorkers);
    dsa_area * leaderDsa = GetNumWorkerDsa(workerLeader);
    volatile ParallelHashJoinStatus *statusParallelWorker    = parallelState->statusParallelWorker;
    volatile dsa_pointer            *hashTableParallelWorker = parallelState->hashTableParallelWorker;
    HashJoinTable ht = (HashJoinTable)dsa_get_address(leaderDsa, hashTableParallelWorker[workerLeader]);
    
    /* build local hashtable */
    HashJoinTable hashtable = ExecHashTableCreate(node,
                                                  hashOperators,
                                                  keepNulls);
    i = 0;
    nMerged = 0;
    while(nMerged < nWorkers)
    {
        if(!merged[i] && statusParallelWorker[i] >= ParallelHashJoin_BuildShmHashTableDone)
        {
            dsa_area *dsa = GetNumWorkerDsa(i);
            HashJoinTable mergeHashtable = (HashJoinTable)dsa_get_address(dsa, hashTableParallelWorker[i]);
            
            merged[i] = true;
            nMerged++;

            /* 
              *   merge hashtable buckets 
              *   worker leader do the merging, other workers just copy the merged hashtable
              *   into local memory
              * 
              *   the main work of merging is just linking the hashtable's buckets from different workers.
              */
            if(workerLeader == ParallelWorkerNumber)
            {
                if(i != workerLeader)
                {            
                    HashJoinTupleData **ht_buckets          = (HashJoinTuple *)dsa_get_address(leaderDsa, 
                                                                          (dsa_pointer)ht->buckets);
                    HashJoinTupleData **ht_buckets_tail     = (HashJoinTuple *)dsa_get_address(leaderDsa, 
                                                                          (dsa_pointer)ht->buckets_tail);
                    HashJoinTupleData **merged_buckets      = (HashJoinTuple *)dsa_get_address(dsa, 
                                                                          (dsa_pointer)mergeHashtable->buckets);
                    HashJoinTupleData **merged_buckets_tail = (HashJoinTuple *)dsa_get_address(dsa, 
                                                                          (dsa_pointer)mergeHashtable->buckets_tail);
                    int *head_workNum                       = (int *)dsa_get_address(leaderDsa, 
                                                                          (dsa_pointer)ht->bucket_wNum);
                    int *tail_workNum                       = (int *)dsa_get_address(leaderDsa, 
                                                                          (dsa_pointer)ht->bucket_tail_wNum);
                    
                    if(ht->nbuckets != mergeHashtable->nbuckets)
                    {
                        elog(ERROR, "number of buckets is different in parallel workers' hashtables.");
                    }

                    ht->totalTuples = ht->totalTuples + mergeHashtable->totalTuples;
					ht->spacePeak = Max(ht->spacePeak, mergeHashtable->spacePeak);
                    
                    /* merge hashtable */
                    for(indexbucket = 0; indexbucket < ht->nbuckets; indexbucket++)
                    {
                        if(ht_buckets[indexbucket])
                        {
                            int workerNumber = tail_workNum[indexbucket];
                            HashJoinTupleData *tail = (HashJoinTupleData *)dsa_get_address(GetNumWorkerDsa(workerNumber), 
                                                                                       (dsa_pointer)ht_buckets_tail[indexbucket]);
                            tail->next              = merged_buckets[indexbucket];
                            tail->workerNumber      = i;

                            if(merged_buckets[indexbucket])
                            {
                                ht_buckets_tail[indexbucket] = merged_buckets_tail[indexbucket];
                                tail_workNum[indexbucket] = i;
                            }
                        }
                        else
                        {
                            if(merged_buckets[indexbucket])
                            {
                                ht_buckets[indexbucket] = merged_buckets[indexbucket];
                                ht_buckets_tail[indexbucket] = merged_buckets_tail[indexbucket];
                                tail_workNum[indexbucket] = i;
                                head_workNum[indexbucket] = i;
                            }
                        }
                    }
                }
            }

            /* merge hashtable inner batch file */
            if(hashtable->nbatch > 1)
            {
                HashTableBufFileName *bufFileNames = (HashTableBufFileName *)dsa_get_address(dsa, 
                                                                         parallelState->bufFileNames[i]);
                int *nFiles                        = (int *)dsa_get_address(dsa, bufFileNames->nFiles);
                
                dsa_pointer *names                 = (dsa_pointer *)dsa_get_address(dsa, bufFileNames->name);
                
                if(hashtable->nbatch != mergeHashtable->nbatch)
                {
                    elog(ERROR, "number of batch is different in parallel workers' hashtables.");
                }
                
                for(indexbatch = 0; indexbatch < hashtable->nbatch; indexbatch++)
                {
                    int fileNum   = 0;
                    dsa_pointer *fileName = NULL;

                    fileNum     = nFiles[indexbatch];

                    if(fileNum > 0)
                    {
                        fileName = (dsa_pointer *)dsa_get_address(dsa, names[indexbatch]);
                        CreateBufFile(dsa, fileNum, fileName, &hashtable->innerBatchFile[indexbatch]);
                    }
                }
            }
        }
        else if (statusParallelWorker[i] == ParallelHashJoin_Error || ParallelError())
        {
            elog(ERROR, "[%s:%d]some other workers exit with errors, and we need to exit because"
                        " of data corrupted.", __FILE__, __LINE__);
        }
#if 0
        else if (!merged[i] && statusParallelWorker[i] == ParallelHashJoin_None)
        {
            elog(LOG, "worker %d not launched while worker %d ExecMergeShmHashTable.", i, ParallelWorkerNumber);
            merged[i] = true;
            nMerged++;
        }
#endif
        pg_usleep(1000L);
        i = (i + 1) % nWorkers;
    }

    if(workerLeader == ParallelWorkerNumber)
    {
#if 0
        /* debug */
        int i = 0;
        size_t tuple_num = 0;
        HashJoinTuple hashTuple;
        dsa_area *dsa = NULL;
        int work_num = 0;

        HashJoinTupleData **ht_buckets  = (HashJoinTuple *)dsa_get_address(leaderDsa, 
                                              (dsa_pointer)ht->buckets);
        int *head_wnum = (int *)dsa_get_address(leaderDsa, 
                                              (dsa_pointer)ht->bucket_wNum);
        for(i = 0; i < ht->nbuckets; i++)
        {
            HashJoinTupleData *temp = NULL;
            
            if(ht_buckets[i])
            {
                tuple_num++;

                dsa = GetNumWorkerDsa(head_wnum[i]);
                hashTuple = (HashJoinTuple)dsa_get_address(dsa, (dsa_pointer)ht_buckets[i]);

                temp = hashTuple->next;
                work_num = hashTuple->workerNumber;
            }

            while(temp)
            {
                tuple_num++;

                dsa = GetNumWorkerDsa(work_num);
                hashTuple = (HashJoinTuple)dsa_get_address(dsa, (dsa_pointer)temp);

                temp = hashTuple->next;
                work_num = hashTuple->workerNumber;
            }
        }

        if(tuple_num != ht->totalTuples)
        {
            elog(PANIC, "merged hashtable's totaltuple is mismatch, tuple_num %zu, totalTuples %lf.",
                         tuple_num, ht->totalTuples);
        }
#endif
        statusParallelWorker[workerLeader] = ParallelHashJoin_MergeShmHashTableDone;
    }

    /* 
      * copy merged hashtable into local hashtable 
      * should wait for the leader finish merging hashtable
      */
    while(statusParallelWorker[workerLeader] < ParallelHashJoin_MergeShmHashTableDone)
    {
        if (statusParallelWorker[workerLeader] == ParallelHashJoin_Error || ParallelError())
        {
            elog(ERROR, "[%s:%d]some other workers exit with errors, and we need to exit because"
                        " of data corrupted.", __FILE__, __LINE__);
        }
        pg_usleep(1000L);
    }

    {
        HashJoinTable ht = (HashJoinTable)dsa_get_address(leaderDsa, hashTableParallelWorker[workerLeader]);
        HashJoinTupleData **ht_buckets  = (HashJoinTuple *)dsa_get_address(leaderDsa, 
                                                      (dsa_pointer)ht->buckets);
        int *bucket_wNum = (int *)dsa_get_address(leaderDsa, (dsa_pointer)ht->bucket_wNum);
        memcpy(hashtable->buckets, ht_buckets, sizeof(HashJoinTupleData *) * hashtable->nbuckets);
        memcpy(hashtable->bucket_wNum, bucket_wNum, sizeof(int) * hashtable->nbuckets);
        hashtable->totalTuples = ht->totalTuples;
        hashtable->skewEnabled = false;
        hashtable->growEnabled = false;
		/* copy instrumentation too */
		hashtable->nbuckets = ht->nbuckets;
		hashtable->nbuckets_original = ht->nbuckets_original;
		hashtable->nbatch = ht->nbatch;
		hashtable->nbatch_original = ht->nbatch_original;
		hashtable->spacePeak = ht->spacePeak;
    }

#if 0
    /* debug */
    {
        int i = 0;
        size_t tuple_num = 0;
        HashJoinTuple hashTuple;
        dsa_area *dsa = NULL;
        int work_num = 0;

        HashJoinTupleData **ht_buckets  = hashtable->buckets;
        for(i = 0; i < hashtable->nbuckets; i++)
        {
            HashJoinTupleData *temp = NULL;
            
            if(ht_buckets[i])
            {
                tuple_num++;

                dsa = GetNumWorkerDsa(hashtable->bucket_wNum[i]);
                hashTuple = (HashJoinTuple)dsa_get_address(dsa, (dsa_pointer)ht_buckets[i]);

                temp = hashTuple->next;
                work_num = hashTuple->workerNumber;
            }

            while(temp)
            {
                tuple_num++;

                dsa = GetNumWorkerDsa(work_num);
                hashTuple = (HashJoinTuple)dsa_get_address(dsa, (dsa_pointer)temp);

                temp = hashTuple->next;
                work_num = hashTuple->workerNumber;
            }
        }

        if (hashtable->nbatch > 1)
        {
            uint32 hashvalue = 0;
            
            for(i = 1; i < hashtable->nbatch; i++)
            {
                while (ExecHashJoinGetSavedTuple(hjstate, hashtable->innerBatchFile[i], &hashvalue, hjstate->hj_HashTupleSlot))
                {
                    tuple_num++;
                    ExecClearTuple(hjstate->hj_HashTupleSlot);
                }

                ReSetBufFile(hashtable->innerBatchFile[i]);
            }
        }

        elog(LOG, "worker_num:%d, tuple_num:%zu, totaltuple:%lf", ParallelWorkerNumber, tuple_num, hashtable->totalTuples);
    }
#endif
    pfree(merged);

    return hashtable;
}

static void
ExecFormNewOuterBufFile(HashJoinState * hjstate, volatile ParallelHashJoinState *parallelState, 
                                 Hash *node)
{// #lizard forgives
    int i = 0;
    int nMerged = 0;
    int indexbatch   = 0;
    int nWorkers     = parallelState->numLaunchedParallelWorkers;
    bool *merged     = (bool *)palloc0(sizeof(bool) * nWorkers);
    HashJoinTable hashtable = hjstate->hj_HashTable;
    volatile ParallelHashJoinStatus *statusParallelWorker     = parallelState->statusParallelWorker;

    i = 0;
    nMerged = 1;
    while(nMerged < nWorkers)
    {
        if (i != ParallelWorkerNumber && !merged[i] && statusParallelWorker[i] >= ParallelHashJoin_ShareOuterBufFileDone)
        {
            dsa_area *dsa = GetNumWorkerDsa(i);
            
            merged[i] = true;
            nMerged++;

            /* merge hashtable outer batch files */
            if (hashtable->nbatch > 1)
            {
                HashTableBufFileName *bufFileNames = (HashTableBufFileName *)dsa_get_address(dsa, 
                                                                         parallelState->outerBufFileNames[i]);
                
                int *nFiles                        = (int *)dsa_get_address(dsa, bufFileNames->nFiles);
                
                dsa_pointer *names                 = (dsa_pointer *)dsa_get_address(dsa, bufFileNames->name);
                
                if (hashtable->nbatch != bufFileNames->nBatch)
                {
                    elog(ERROR, "number of outer batch is different in parallel workers' hashtables."
                                "worker %d nbatch %d, worker %d nbatch %d.", ParallelWorkerNumber, hashtable->nbatch,
                                i, bufFileNames->nBatch);
                }
                
                for (indexbatch = 0; indexbatch < hashtable->nbatch; indexbatch++)
                {
                    int fileNum   = 0;
                    dsa_pointer *fileName = NULL;

                    fileNum     = nFiles[indexbatch];

                    if (fileNum > 0)
                    {
                        fileName = (dsa_pointer *)dsa_get_address(dsa, names[indexbatch]);
                        CreateBufFile(dsa, fileNum, fileName, &hashtable->outerBatchFile[indexbatch]);
                    }
                }
            }
            
        }
        else if (statusParallelWorker[i] == ParallelHashJoin_Error || ParallelError())
        {
            elog(ERROR, "[%s:%d]some other workers exit with errors, and we need to exit because"
                        " of data corrupted.", __FILE__, __LINE__);
        }

        pg_usleep(1000L);
        i = (i + 1) % nWorkers;
    }
}

void
ParallelHashJoinEreport(void)
{
    if (statusParallelWorker)
    {
        statusParallelWorker[ParallelWorkerNumber] = ParallelHashJoin_Error;
        statusParallelWorker = NULL;
    }
}
#endif
