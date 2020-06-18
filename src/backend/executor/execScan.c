/*-------------------------------------------------------------------------
 *
 * execScan.c
 *      This code provides support for generalized relation scans. ExecScan
 *      is passed a node and a pointer to a function to "do the right thing"
 *      and return a tuple from the relation. ExecScan then does the tedious
 *      stuff - checking the qualification and projecting the tuple
 *      appropriately.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *      src/backend/executor/execScan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "executor/executor.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#ifdef _MLS_
#include "utils/mls.h"
#include "utils/datamask.h"
#endif

#ifdef __AUDIT_FGA__
#include "audit/audit_fga.h"
#endif

#ifdef __TBASE__
#include "pgxc/shardmap.h"
#include "access/htup_details.h"
#include "utils/guc.h"
#include "storage/nodelock.h"
#endif

static bool tlist_matches_tupdesc(PlanState *ps, List *tlist, Index varno, TupleDesc tupdesc);


/*
 * ExecScanFetch -- fetch next potential tuple
 *
 * This routine is concerned with substituting a test tuple if we are
 * inside an EvalPlanQual recheck.  If we aren't, just execute
 * the access method's next-tuple routine.
 */
static inline TupleTableSlot *
ExecScanFetch(ScanState *node,
              ExecScanAccessMtd accessMtd,
              ExecScanRecheckMtd recheckMtd)
{// #lizard forgives
    EState       *estate = node->ps.state;

    if (estate->es_epqTuple != NULL)
    {
        /*
         * We are inside an EvalPlanQual recheck.  Return the test tuple if
         * one is available, after rechecking any access-method-specific
         * conditions.
         */
        Index        scanrelid = ((Scan *) node->ps.plan)->scanrelid;

        if (scanrelid == 0)
        {
            TupleTableSlot *slot = node->ss_ScanTupleSlot;

            /*
             * This is a ForeignScan or CustomScan which has pushed down a
             * join to the remote side.  The recheck method is responsible not
             * only for rechecking the scan/join quals but also for storing
             * the correct tuple in the slot.
             */
            if (!(*recheckMtd) (node, slot))
                ExecClearTuple(slot);    /* would not be returned by scan */
            return slot;
        }
        else if (estate->es_epqTupleSet[scanrelid - 1])
        {
            TupleTableSlot *slot = node->ss_ScanTupleSlot;

            /* Return empty slot if we already returned a tuple */
            if (estate->es_epqScanDone[scanrelid - 1])
                return ExecClearTuple(slot);
            /* Else mark to remember that we shouldn't return more */
            estate->es_epqScanDone[scanrelid - 1] = true;

            /* Return empty slot if we haven't got a test tuple */
            if (estate->es_epqTuple[scanrelid - 1] == NULL)
                return ExecClearTuple(slot);

            /* Store test tuple in the plan node's scan slot */
            ExecStoreTuple(estate->es_epqTuple[scanrelid - 1],
                           slot, InvalidBuffer, false);

            /* Check if it meets the access-method conditions */
            if (!(*recheckMtd) (node, slot))
                ExecClearTuple(slot);    /* would not be returned by scan */

            return slot;
        }
    }

    /*
     * Run the node-type-specific access method function to get the next tuple
     */
    return (*accessMtd) (node);
}

/* ----------------------------------------------------------------
 *        ExecScan
 *
 *        Scans the relation using the 'access method' indicated and
 *        returns the next qualifying tuple in the direction specified
 *        in the global variable ExecDirection.
 *        The access method returns the next tuple and ExecScan() is
 *        responsible for checking the tuple returned against the qual-clause.
 *
 *        A 'recheck method' must also be provided that can check an
 *        arbitrary tuple of the relation against any qual conditions
 *        that are implemented internal to the access method.
 *
 *        Conditions:
 *          -- the "cursor" maintained by the AMI is positioned at the tuple
 *             returned previously.
 *
 *        Initial States:
 *          -- the relation indicated is opened for scanning so that the
 *             "cursor" is positioned before the first qualifying tuple.
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecScan(ScanState *node,
         ExecScanAccessMtd accessMtd,    /* function returning a tuple */
         ExecScanRecheckMtd recheckMtd)
{// #lizard forgives
    ExprContext *econtext;
    ExprState  *qual;
    ProjectionInfo *projInfo;
    
#ifdef __AUDIT_FGA__
	ShardID 	shardid = InvalidShardID;
    ListCell      *item;

    char *cmd_type = "SELECT";
    CmdType    commandType = CMD_SELECT;

    if (node->ps.state && node->ps.state->es_plannedstmt)
    {
        commandType = node->ps.state->es_plannedstmt->commandType;
    }
#endif

    /*
     * Fetch data from node
     */
    qual = node->ps.qual;
    projInfo = node->ps.ps_ProjInfo;
    econtext = node->ps.ps_ExprContext;

#ifdef __COLD_HOT__
    /* prune hot data only if select */
    if (commandType == CMD_SELECT && !node->inited)
    {
        node->inited = true;

        if (!g_EnableColdHotVisible && !ScanNeedExecute(node->ss_currentRelation))
        {
            return NULL;
        }
    }
#endif

    /*
     * If we have neither a qual to check nor a projection to do, just skip
     * all the overhead and return the raw scan tuple.
     */
#ifdef __AUDIT_FGA__
    if (!qual && !projInfo && !node->ps.audit_fga_qual && enable_fga)
#else
    if (!qual && !projInfo)
#endif
    {

next_record:
        
        ResetExprContext(econtext);
#ifdef _MLS_
        {
            TupleTableSlot *slot;

            slot = ExecScanFetch(node, accessMtd, recheckMtd);

            if (!TupIsNull(slot))
            {
                if (g_enable_user_authority_force_check)
                {
                    if (SKIP_TUPLE == ExecCheckPgclassAuthority(node, slot))
                    {
                        ExecClearTuple(slot);
                        goto next_record;
                    }
                }

                MlsExecCheck(node, slot);

				if(node && datamask_scan_key_contain_mask(node))
				{
					ExecClearTuple(slot);
                    return NULL;
				}
            }

#ifdef __TBASE__
            /* update shard statistic info about select if needed */
            if (g_StatShardInfo && !TupIsNull(slot) && IS_PGXC_DATANODE)
            {
                if (IsA(node, SeqScanState) || IsA(node, SampleScanState) ||
                    IsA(node, IndexScanState) || IsA(node, BitmapHeapScanState) ||
                    IsA(node, TidScanState))
                {
                    HeapTuple tup = slot->tts_tuple;

                    UpdateShardStatistic(CMD_SELECT, HeapTupleGetShardId(tup), 0, 0);

					LightLockCheck(commandType, InvalidOid, HeapTupleGetShardId(tup));
                }
            }
#endif
            
            return slot;
        }
#else
        return ExecScanFetch(node, accessMtd, recheckMtd);
#endif
        
    }

    /*
     * Reset per-tuple memory context to free any expression evaluation
     * storage allocated in the previous tuple cycle.
     */
    ResetExprContext(econtext);

    /*
     * get a tuple from the access method.  Loop until we obtain a tuple that
     * passes the qualification.
     */
    for (;;)
    {
        TupleTableSlot *slot;

        CHECK_FOR_INTERRUPTS();

        slot = ExecScanFetch(node, accessMtd, recheckMtd);

        /*
         * if the slot returned by the accessMtd contains NULL, then it means
         * there is nothing more to scan so we just return an empty slot,
         * being careful to use the projection result slot so it has correct
         * tupleDesc.
         */
        if (TupIsNull(slot))
        {
            if (projInfo)
                return ExecClearTuple(projInfo->pi_state.resultslot);
            else
                return slot;
        }

#ifdef __TBASE__
		shardid = InvalidShardID;
        /* update shard statistic info about select if needed */
        if (g_StatShardInfo && IS_PGXC_DATANODE)
        {
            if (IsA(node, SeqScanState) || IsA(node, SampleScanState) ||
                IsA(node, IndexScanState) || IsA(node, BitmapHeapScanState) ||
                IsA(node, TidScanState))
            {
                HeapTuple tup = slot->tts_tuple;

                UpdateShardStatistic(CMD_SELECT, HeapTupleGetShardId(tup), 0, 0);

				shardid = HeapTupleGetShardId(tup);
            }
        }
#endif

        /*
         * place the current tuple into the expr context
         */
        econtext->ecxt_scantuple = slot;

#ifdef _MLS_
        if (g_enable_user_authority_force_check )
        {
            if (SKIP_TUPLE == ExecCheckPgclassAuthority(node, slot))
            {
                ExecClearTuple(slot);
                continue;
            }
        }

        MlsExecCheck(node, slot);
#endif

        /*
         * check that the current tuple satisfies the qual-clause
         *
         * check for non-null qual here to avoid a function call to ExecQual()
         * when the qual is null ... saves only a few cycles, but they add up
         * ...
         */
        if (qual == NULL || ExecQual(qual, econtext))
        {
#ifdef __AUDIT_FGA__
            if (enable_fga && g_commandTag && (strcmp(g_commandTag, "SELECT") == 0))
            {
                foreach (item, node->ps.audit_fga_qual)
                {
                    audit_fga_policy_state *audit_fga_qual = (audit_fga_policy_state *) lfirst(item);
                    if (audit_fga_qual != NULL)
                    {
                        if(ExecQual(audit_fga_qual->qual, econtext))
                        {
                            audit_fga_log_policy_info_2(audit_fga_qual, cmd_type);

                            node->ps.audit_fga_qual = list_delete(node->ps.audit_fga_qual, audit_fga_qual);
                        }
                    }   
                }
            }
#endif
#ifdef __TBASE__
			if (IS_PGXC_DATANODE)
			{
				LightLockCheck(commandType, InvalidOid, shardid);
			}
#endif
            /*             * Found a satisfactory scan tuple.
             */
            if (projInfo)
            {
                /*
                 * Form a projection tuple, store it in the result tuple slot
                 * and return it.
                 */
                return ExecProject(projInfo);
            }
            else
            {
                /*
                 * Here, we aren't projecting, so just return scan tuple.
                 */
                return slot;
            }
        }
        else
            InstrCountFiltered1(node, 1);

        /*
         * Tuple fails qual, so free per-tuple memory and try again.
         */
        ResetExprContext(econtext);
    }
}

/*
 * ExecAssignScanProjectionInfo
 *        Set up projection info for a scan node, if necessary.
 *
 * We can avoid a projection step if the requested tlist exactly matches
 * the underlying tuple type.  If so, we just set ps_ProjInfo to NULL.
 * Note that this case occurs not only for simple "SELECT * FROM ...", but
 * also in most cases where there are joins or other processing nodes above
 * the scan node, because the planner will preferentially generate a matching
 * tlist.
 *
 * ExecAssignScanType must have been called already.
 */
void
ExecAssignScanProjectionInfo(ScanState *node)
{
    Scan       *scan = (Scan *) node->ps.plan;

    ExecAssignScanProjectionInfoWithVarno(node, scan->scanrelid);
}

/*
 * ExecAssignScanProjectionInfoWithVarno
 *        As above, but caller can specify varno expected in Vars in the tlist.
 */
void
ExecAssignScanProjectionInfoWithVarno(ScanState *node, Index varno)
{
    Scan       *scan = (Scan *) node->ps.plan;

    if (tlist_matches_tupdesc(&node->ps,
                              scan->plan.targetlist,
                              varno,
                              node->ss_ScanTupleSlot->tts_tupleDescriptor))
        node->ps.ps_ProjInfo = NULL;
    else
        ExecAssignProjectionInfo(&node->ps,
                                 node->ss_ScanTupleSlot->tts_tupleDescriptor);
}

static bool
tlist_matches_tupdesc(PlanState *ps, List *tlist, Index varno, TupleDesc tupdesc)
{// #lizard forgives
    int            numattrs = tupdesc->natts;
    int            attrno;
    bool        hasoid;
    ListCell   *tlist_item = list_head(tlist);

    /* Check the tlist attributes */
    for (attrno = 1; attrno <= numattrs; attrno++)
    {
        Form_pg_attribute att_tup = tupdesc->attrs[attrno - 1];
        Var           *var;

        if (tlist_item == NULL)
            return false;        /* tlist too short */
        var = (Var *) ((TargetEntry *) lfirst(tlist_item))->expr;
        if (!var || !IsA(var, Var))
            return false;        /* tlist item not a Var */
        /* if these Asserts fail, planner messed up */
        Assert(var->varno == varno);
        Assert(var->varlevelsup == 0);
        if (var->varattno != attrno)
            return false;        /* out of order */
        if (att_tup->attisdropped)
            return false;        /* table contains dropped columns */
#ifdef _MLS_
        if (att_tup->atthasmissing)
            return false;       /* table contains cols with missing values */
#endif
        /*
         * Note: usually the Var's type should match the tupdesc exactly, but
         * in situations involving unions of columns that have different
         * typmods, the Var may have come from above the union and hence have
         * typmod -1.  This is a legitimate situation since the Var still
         * describes the column, just not as exactly as the tupdesc does. We
         * could change the planner to prevent it, but it'd then insert
         * projection steps just to convert from specific typmod to typmod -1,
         * which is pretty silly.
         */
        if (var->vartype != att_tup->atttypid ||
            (var->vartypmod != att_tup->atttypmod &&
             var->vartypmod != -1))
            return false;        /* type mismatch */

        tlist_item = lnext(tlist_item);
    }

    if (tlist_item)
        return false;            /* tlist too long */

    /*
     * If the plan context requires a particular hasoid setting, then that has
     * to match, too.
     */
    if (ExecContextForcesOids(ps, &hasoid) &&
        hasoid != tupdesc->tdhasoid)
        return false;

    return true;
}

/*
 * ExecScanReScan
 *
 * This must be called within the ReScan function of any plan node type
 * that uses ExecScan().
 */
void
ExecScanReScan(ScanState *node)
{
    EState       *estate = node->ps.state;

    /* Rescan EvalPlanQual tuple if we're inside an EvalPlanQual recheck */
    if (estate->es_epqScanDone != NULL)
    {
        Index        scanrelid = ((Scan *) node->ps.plan)->scanrelid;

        if (scanrelid > 0)
            estate->es_epqScanDone[scanrelid - 1] = false;
        else
        {
            Bitmapset  *relids;
            int            rtindex = -1;

            /*
             * If an FDW or custom scan provider has replaced the join with a
             * scan, there are multiple RTIs; reset the epqScanDone flag for
             * all of them.
             */
            if (IsA(node->ps.plan, ForeignScan))
                relids = ((ForeignScan *) node->ps.plan)->fs_relids;
            else if (IsA(node->ps.plan, CustomScan))
                relids = ((CustomScan *) node->ps.plan)->custom_relids;
            else
                elog(ERROR, "unexpected scan node: %d",
                     (int) nodeTag(node->ps.plan));

            while ((rtindex = bms_next_member(relids, rtindex)) >= 0)
            {
                Assert(rtindex > 0);
                estate->es_epqScanDone[rtindex - 1] = false;
            }
        }
    }
}
