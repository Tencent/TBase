/*-------------------------------------------------------------------------
 *
 * pquery.c
 *      POSTGRES process query command code
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *      src/backend/tcop/pquery.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <limits.h>

#include "access/xact.h"
#include "commands/prepare.h"
#include "executor/tstoreReceiver.h"
#include "miscadmin.h"
#include "pg_trace.h"
#ifdef XCP
#include "catalog/pgxc_node.h"
#include "executor/producerReceiver.h"
#include "pgxc/nodemgr.h"
#endif
#ifdef PGXC
#include "pgstat.h"
#include "pgxc/pgxc.h"
#include "pgxc/planner.h"
#include "pgxc/execRemote.h"
#include "access/relscan.h"
#endif
#include "tcop/pquery.h"
#include "tcop/utility.h"
#include "utils/memutils.h"
#include "utils/pg_rusage.h"
#include "utils/snapmgr.h"
#ifdef __TBASE__
#include "pgxc/squeue.h"
#include "optimizer/planner.h"
#include "executor/execParallel.h"
#include "commands/defrem.h"
#include "commands/explain_dist.h"
#include "commands/vacuum.h"
#include "postmaster/postmaster.h"
#include "optimizer/planmain.h"
#endif

#ifdef __TBASE__
bool     paramPassDown = false;
#endif

/* Hooks for plugins to get control in PortalStart */
PortalStart_hook_type PortalStart_hook = NULL;

/*
 * ActivePortal is the currently executing Portal (the most closely nested,
 * if there are several).
 */
Portal        ActivePortal = NULL;


static void ProcessQuery(PlannedStmt *plan,
             const char *sourceText,
             ParamListInfo params,
             QueryEnvironment *queryEnv,
             DestReceiver *dest,
			 char *completionTag,
			 int instrument);
static void FillPortalStore(Portal portal, bool isTopLevel);
static uint64 RunFromStore(Portal portal, ScanDirection direction, uint64 count,
             DestReceiver *dest);
static uint64 PortalRunSelect(Portal portal, bool forward, long count,
                DestReceiver *dest);
static void PortalRunUtility(Portal portal, PlannedStmt *pstmt,
                 bool isTopLevel, bool setHoldSnapshot,
                 DestReceiver *dest, char *completionTag);
static void PortalRunMulti(Portal portal,
               bool isTopLevel, bool setHoldSnapshot,
               DestReceiver *dest, DestReceiver *altdest,
               char *completionTag);
static uint64 DoPortalRunFetch(Portal portal,
                 FetchDirection fdirection,
                 long count,
                 DestReceiver *dest);
static void DoPortalRewind(Portal portal);

#ifdef __TBASE__
static void GetGtmInfoFromUserCmd(Node* stmt);
static bool NeedSnapshot(PlannedStmt *plannedstmt);
#endif

/*
 * CreateQueryDesc
 */
QueryDesc *
CreateQueryDesc(PlannedStmt *plannedstmt,
                const char *sourceText,
                Snapshot snapshot,
                Snapshot crosscheck_snapshot,
                DestReceiver *dest,
                ParamListInfo params,
                QueryEnvironment *queryEnv,
                int instrument_options)
{
    QueryDesc  *qd = (QueryDesc *) palloc(sizeof(QueryDesc));

    qd->operation = plannedstmt->commandType;    /* operation */
    qd->plannedstmt = plannedstmt;    /* plan */
    qd->sourceText = sourceText;    /* query text */
    qd->snapshot = RegisterSnapshot(snapshot);    /* snapshot */
    /* RI check snapshot */
    qd->crosscheck_snapshot = RegisterSnapshot(crosscheck_snapshot);
    qd->dest = dest;            /* output dest */
    qd->params = params;        /* parameter values passed into query */
    qd->queryEnv = queryEnv;
    qd->instrument_options = instrument_options;    /* instrumentation wanted? */

    /* null these fields until set by ExecutorStart */
    qd->tupDesc = NULL;
    qd->estate = NULL;
    qd->planstate = NULL;
    qd->totaltime = NULL;

#ifdef XCP
    qd->squeue = NULL;
    qd->myindex = -1;
#endif

#ifdef __TBASE__
    qd->sender = NULL;
    qd->es_param_exec_vals = NULL;
	qd->epqContext = NULL;
#endif

    /* not yet executed */
    qd->already_executed = false;

    return qd;
}

/*
 * FreeQueryDesc
 */
void
FreeQueryDesc(QueryDesc *qdesc)
{
    /* Can't be a live query */
    Assert(qdesc->estate == NULL);

    /* forget our snapshots */
    UnregisterSnapshot(qdesc->snapshot);
    UnregisterSnapshot(qdesc->crosscheck_snapshot);

    /* Only the QueryDesc itself need be freed */
    pfree(qdesc);
}


/*
 * ProcessQuery
 *        Execute a single plannable query within a PORTAL_MULTI_QUERY,
 *        PORTAL_ONE_RETURNING, or PORTAL_ONE_MOD_WITH portal
 *
 *    plan: the plan tree for the query
 *    sourceText: the source text of the query
 *    params: any parameters needed
 *    dest: where to send results
 *    completionTag: points to a buffer of size COMPLETION_TAG_BUFSIZE
 *        in which to store a command completion status string.
 *
 * completionTag may be NULL if caller doesn't want a status string.
 *
 * Must be called in a memory context that will be reset or deleted on
 * error; otherwise the executor's memory usage will be leaked.
 */
static void
ProcessQuery(PlannedStmt *plan,
             const char *sourceText,
             ParamListInfo params,
             QueryEnvironment *queryEnv,
             DestReceiver *dest,
			 char *completionTag,
			 int instrument)
{
    int         eflags = 0;
    QueryDesc  *queryDesc;

    /*
     * Create the QueryDesc object
     */
#ifdef __TBASE__
    if (plan->need_snapshot && !g_set_global_snapshot)
    {
        queryDesc = CreateQueryDesc(plan, sourceText,
                            InvalidSnapshot, InvalidSnapshot,
							dest, params, queryEnv, instrument);
    }
    else
#endif
    queryDesc = CreateQueryDesc(plan, sourceText,
                                GetActiveSnapshot(), InvalidSnapshot,
								dest, params, queryEnv, instrument);

	if (plan->hasReturning)
	{
	    eflags |= EXEC_FLAG_RETURNING;
	}

    /*
     * Call ExecutorStart to prepare the plan for execution
     */
	ExecutorStart(queryDesc, eflags);

    /*
     * Run the plan to completion.
     */
    ExecutorRun(queryDesc, ForwardScanDirection, 0L, true);

    /*
     * Build command completion status string, if caller wants one.
     */
    if (completionTag)
    {
        Oid            lastOid;

        switch (queryDesc->operation)
        {
            case CMD_SELECT:
                snprintf(completionTag, COMPLETION_TAG_BUFSIZE,
                         "SELECT " UINT64_FORMAT,
                         queryDesc->estate->es_processed);
                break;
            case CMD_INSERT:
                if (queryDesc->estate->es_processed == 1)
                    lastOid = queryDesc->estate->es_lastoid;
                else
                    lastOid = InvalidOid;
                snprintf(completionTag, COMPLETION_TAG_BUFSIZE,
                         "INSERT %u " UINT64_FORMAT,
                         lastOid, queryDesc->estate->es_processed);
                break;
            case CMD_UPDATE:
                snprintf(completionTag, COMPLETION_TAG_BUFSIZE,
                         "UPDATE " UINT64_FORMAT,
                         queryDesc->estate->es_processed);
                break;
            case CMD_DELETE:
                snprintf(completionTag, COMPLETION_TAG_BUFSIZE,
                         "DELETE " UINT64_FORMAT,
                         queryDesc->estate->es_processed);
                break;
            default:
                strcpy(completionTag, "???");
                break;
        }
    }

#ifdef __TBASE__
	if (instrument && queryDesc->planstate)
	{
		SendLocalInstr(queryDesc->planstate);
	}
#endif

    /*
     * Now, we close down all the scans and free allocated resources.
     */
    ExecutorFinish(queryDesc);

    if (IsA(plan->planTree, RemoteQuery))
    {
        /*
         * Update coordinator statistics
         */
        if (IS_PGXC_COORDINATOR)
        {
            EState *estate = queryDesc->estate;

            if (estate->es_num_result_relations > 0 && estate->es_processed > 0)
            {
                switch (estate->es_plannedstmt->commandType)
                {
                    case CMD_INSERT:
                        /* One statement can insert into only one relation */
                        pgstat_count_remote_insert(
                                estate->es_result_relations[0].ri_RelationDesc,
                                estate->es_processed);
                        break;
                    case CMD_UPDATE:
                    case CMD_DELETE:
                        {
                            /*
                             * We can not determine here how many row were updated
                             * or delete in each table, so assume same number of
                             * affected row in each table.
                             * If resulting number of rows is 0 because of rounding,
                             * increment each counter at least on 1.
                             */
                            int        i;
                            int     n;
                            bool     update;

                            update = (estate->es_plannedstmt->commandType == CMD_UPDATE);
                            n = estate->es_processed / estate->es_num_result_relations;
                            if (n == 0)
                                n = 1;
                            for (i = 0; i < estate->es_num_result_relations; i++)
                            {
                                Relation r;
                                r = estate->es_result_relations[i].ri_RelationDesc;
                                if (update)
                                    pgstat_count_remote_update(r, n);
                                else
                                    pgstat_count_remote_delete(r, n);
                            }
                        }
                        break;
                    default:
                        /* nothing to count */
                        break;
                }
            }
        }

    }
    ExecutorEnd(queryDesc);
    FreeQueryDesc(queryDesc);
}

/*
 * ChoosePortalStrategy
 *        Select portal execution strategy given the intended statement list.
 *
 * The list elements can be Querys or PlannedStmts.
 * That's more general than portals need, but plancache.c uses this too.
 *
 * See the comments in portal.h.
 */
PortalStrategy
ChoosePortalStrategy(List *stmts)
{// #lizard forgives
    int            nSetTag;
    ListCell   *lc;

    /*
     * PORTAL_ONE_SELECT and PORTAL_UTIL_SELECT need only consider the
     * single-statement case, since there are no rewrite rules that can add
     * auxiliary queries to a SELECT or a utility command. PORTAL_ONE_MOD_WITH
     * likewise allows only one top-level statement.
     */
    if (list_length(stmts) == 1)
    {
        Node       *stmt = (Node *) linitial(stmts);

        if (IsA(stmt, Query))
        {
            Query       *query = (Query *) stmt;

            if (query->canSetTag)
            {
                if (query->commandType == CMD_SELECT)
                {
                    if (query->hasModifyingCTE)
                        return PORTAL_ONE_MOD_WITH;
                    else
                        return PORTAL_ONE_SELECT;
                }
                if (query->commandType == CMD_UTILITY)
                {
                    if (UtilityReturnsTuples(query->utilityStmt))
                        return PORTAL_UTIL_SELECT;
                    /* it can't be ONE_RETURNING, so give up */
                    return PORTAL_MULTI_QUERY;
                }
#ifdef PGXC
                /*
                 * This is possible with an EXECUTE DIRECT in a SPI.
                 * PGXCTODO: there might be a better way to manage the
                 * cases with EXECUTE DIRECT here like using a special
                 * utility command and redirect it to a correct portal
                 * strategy.
                 * Something like PORTAL_UTIL_SELECT might be far better.
                 */
                if (query->commandType == CMD_SELECT &&
                    query->utilityStmt != NULL &&
                    IsA(query->utilityStmt, RemoteQuery))
                {
                    RemoteQuery *step = (RemoteQuery *) stmt;
                    /*
                     * Let's choose PORTAL_ONE_SELECT for now
                     * After adding more PGXC functionality we may have more
                     * sophisticated algorithm of determining portal strategy
                     *
                     * EXECUTE DIRECT is a utility but depending on its inner query
                     * it can return tuples or not depending on the query used.
                     */
                    if (step->exec_direct_type == EXEC_DIRECT_SELECT
                        || step->exec_direct_type == EXEC_DIRECT_UPDATE
                        || step->exec_direct_type == EXEC_DIRECT_DELETE
                        || step->exec_direct_type == EXEC_DIRECT_INSERT
                        || step->exec_direct_type == EXEC_DIRECT_LOCAL)
                        return PORTAL_ONE_SELECT;
                    else if (step->exec_direct_type == EXEC_DIRECT_UTILITY
                             || step->exec_direct_type == EXEC_DIRECT_LOCAL_UTILITY)
                        return PORTAL_MULTI_QUERY;
                    else
                        return PORTAL_ONE_SELECT;
                }
#endif
            }
        }
#ifdef PGXC
        else if (IsA(stmt, RemoteQuery))
        {
            RemoteQuery *step = (RemoteQuery *) stmt;
            /*
             * Let's choose PORTAL_ONE_SELECT for now
             * After adding more PGXC functionality we may have more
             * sophisticated algorithm of determining portal strategy.
             *
             * EXECUTE DIRECT is a utility but depending on its inner query
             * it can return tuples or not depending on the query used.
             */
            if (step->exec_direct_type == EXEC_DIRECT_SELECT
                || step->exec_direct_type == EXEC_DIRECT_UPDATE
                || step->exec_direct_type == EXEC_DIRECT_DELETE
                || step->exec_direct_type == EXEC_DIRECT_INSERT
                || step->exec_direct_type == EXEC_DIRECT_LOCAL)
                return PORTAL_ONE_SELECT;
            else if (step->exec_direct_type == EXEC_DIRECT_UTILITY
                     || step->exec_direct_type == EXEC_DIRECT_LOCAL_UTILITY)
                return PORTAL_MULTI_QUERY;
            else
                return PORTAL_ONE_SELECT;
        }
#endif
        else if (IsA(stmt, PlannedStmt))
        {
            PlannedStmt *pstmt = (PlannedStmt *) stmt;

#ifdef XCP
            if (list_length(pstmt->distributionRestrict) > 1)
                return PORTAL_DISTRIBUTED;
#endif

            if (pstmt->canSetTag)
            {
                if (pstmt->commandType == CMD_SELECT)
                {
                    if (pstmt->hasModifyingCTE)
                        return PORTAL_ONE_MOD_WITH;
                    else
                        return PORTAL_ONE_SELECT;
                }
                if (pstmt->commandType == CMD_UTILITY)
                {
                    if (UtilityReturnsTuples(pstmt->utilityStmt))
                        return PORTAL_UTIL_SELECT;
                    /* it can't be ONE_RETURNING, so give up */
                    return PORTAL_MULTI_QUERY;
                }
            }
        }
        else
            elog(ERROR, "unrecognized node type: %d", (int) nodeTag(stmt));
    }

    /*
     * PORTAL_ONE_RETURNING has to allow auxiliary queries added by rewrite.
     * Choose PORTAL_ONE_RETURNING if there is exactly one canSetTag query and
     * it has a RETURNING list.
     */
    nSetTag = 0;
    foreach(lc, stmts)
    {
        Node       *stmt = (Node *) lfirst(lc);

        if (IsA(stmt, Query))
        {
            Query       *query = (Query *) stmt;

            if (query->canSetTag)
            {
                if (++nSetTag > 1)
                    return PORTAL_MULTI_QUERY;    /* no need to look further */
                if (query->commandType == CMD_UTILITY ||
                    query->returningList == NIL)
                    return PORTAL_MULTI_QUERY;    /* no need to look further */
            }
        }
        else if (IsA(stmt, PlannedStmt))
        {
            PlannedStmt *pstmt = (PlannedStmt *) stmt;

            if (pstmt->canSetTag)
            {
                if (++nSetTag > 1)
                    return PORTAL_MULTI_QUERY;    /* no need to look further */
                if (pstmt->commandType == CMD_UTILITY ||
                    !pstmt->hasReturning)
                    return PORTAL_MULTI_QUERY;    /* no need to look further */
            }
        }
        else
            elog(ERROR, "unrecognized node type: %d", (int) nodeTag(stmt));
    }
    if (nSetTag == 1)
        return PORTAL_ONE_RETURNING;

    /* Else, it's the general case... */
    return PORTAL_MULTI_QUERY;
}

/*
 * FetchPortalTargetList
 *        Given a portal that returns tuples, extract the query targetlist.
 *        Returns NIL if the portal doesn't have a determinable targetlist.
 *
 * Note: do not modify the result.
 */
List *
FetchPortalTargetList(Portal portal)
{
    /* no point in looking if we determined it doesn't return tuples */
    if (portal->strategy == PORTAL_MULTI_QUERY)
        return NIL;
    /* get the primary statement and find out what it returns */
    return FetchStatementTargetList((Node *) PortalGetPrimaryStmt(portal));
}

/*
 * FetchStatementTargetList
 *        Given a statement that returns tuples, extract the query targetlist.
 *        Returns NIL if the statement doesn't have a determinable targetlist.
 *
 * This can be applied to a Query or a PlannedStmt.
 * That's more general than portals need, but plancache.c uses this too.
 *
 * Note: do not modify the result.
 *
 * XXX be careful to keep this in sync with UtilityReturnsTuples.
 */
List *
FetchStatementTargetList(Node *stmt)
{// #lizard forgives
    if (stmt == NULL)
        return NIL;
    if (IsA(stmt, Query))
    {
        Query       *query = (Query *) stmt;

        if (query->commandType == CMD_UTILITY)
        {
            /* transfer attention to utility statement */
            stmt = query->utilityStmt;
        }
        else
        {
            if (query->commandType == CMD_SELECT)
                return query->targetList;
            if (query->returningList)
                return query->returningList;
            return NIL;
        }
    }
    if (IsA(stmt, PlannedStmt))
    {
        PlannedStmt *pstmt = (PlannedStmt *) stmt;

        if (pstmt->commandType == CMD_UTILITY)
        {
            /* transfer attention to utility statement */
            stmt = pstmt->utilityStmt;
        }
        else
        {
            if (pstmt->commandType == CMD_SELECT)
                return pstmt->planTree->targetlist;
            if (pstmt->hasReturning)
                return pstmt->planTree->targetlist;
            return NIL;
        }
    }
    if (IsA(stmt, FetchStmt))
    {
        FetchStmt  *fstmt = (FetchStmt *) stmt;
        Portal        subportal;

        Assert(!fstmt->ismove);
        subportal = GetPortalByName(fstmt->portalname);
        Assert(PortalIsValid(subportal));
        return FetchPortalTargetList(subportal);
    }
    if (IsA(stmt, ExecuteStmt))
    {
        ExecuteStmt *estmt = (ExecuteStmt *) stmt;
        PreparedStatement *entry;

        entry = FetchPreparedStatement(estmt->name, true);
        return FetchPreparedStatementTargetList(entry);
    }
    return NIL;
}

/*
 * PortalStart
 *        Prepare a portal for execution.
 *
 * Caller must already have created the portal, done PortalDefineQuery(),
 * and adjusted portal options if needed.
 *
 * If parameters are needed by the query, they must be passed in "params"
 * (caller is responsible for giving them appropriate lifetime).
 *
 * The caller can also provide an initial set of "eflags" to be passed to
 * ExecutorStart (but note these can be modified internally, and they are
 * currently only honored for PORTAL_ONE_SELECT portals).  Most callers
 * should simply pass zero.
 *
 * The caller can optionally pass a snapshot to be used; pass InvalidSnapshot
 * for the normal behavior of setting a new snapshot.  This parameter is
 * presently ignored for non-PORTAL_ONE_SELECT portals (it's only intended
 * to be used for cursors).
 *
 * On return, portal is ready to accept PortalRun() calls, and the result
 * tupdesc (if any) is known.
 */
void
PortalStart(Portal portal, ParamListInfo params,
            int eflags, Snapshot snapshot)
{// #lizard forgives
    Portal        saveActivePortal;
    ResourceOwner saveResourceOwner;
    MemoryContext savePortalContext;
    MemoryContext oldContext;
#ifdef XCP
    QueryDesc  *queryDesc = NULL;
#else
    QueryDesc  *queryDesc;
#endif
    int            myeflags;
#ifdef __TBASE__
    bool snapshot_set = true;
#endif

    AssertArg(PortalIsValid(portal));
    AssertState(portal->status == PORTAL_DEFINED);

    /*
     * Set up global portal context pointers.
     */
    saveActivePortal = ActivePortal;
    saveResourceOwner = CurrentResourceOwner;
    savePortalContext = PortalContext;
    PG_TRY();
    {
        ActivePortal = portal;
        if (portal->resowner)
            CurrentResourceOwner = portal->resowner;
        PortalContext = PortalGetHeapMemory(portal);

        oldContext = MemoryContextSwitchTo(PortalGetHeapMemory(portal));

        /* Must remember portal param list, if any */
        portal->portalParams = params;

        /*
         * Determine the portal execution strategy
         */
        portal->strategy = ChoosePortalStrategy(portal->stmts);

        /*
         * Fire her up according to the strategy
         */
        switch (portal->strategy)
        {
#ifdef XCP
            case PORTAL_DISTRIBUTED:
			{
				int  i;
				bool paramNeedPassDown = false;
				
                /* No special ability is needed */
                eflags = 0;
                /* Must set snapshot before starting executor. */
                if (snapshot)
                    PushActiveSnapshot(GetActiveSnapshot());
                else
                    PushActiveSnapshot(GetTransactionSnapshot());

                /*
                 * Create QueryDesc in portal's context; for the moment, set
                 * the destination to DestNone.
                 */
                queryDesc = CreateQueryDesc((PlannedStmt *) linitial(portal->stmts),
                                            portal->sourceText,
                                            GetActiveSnapshot(),
                                            InvalidSnapshot,
                                            None_Receiver,
                                            params,
                                            NULL,
#ifdef __TBASE__
                                            portal->up_instrument);
#else
                                            0);
#endif
				/*
				 * set information about EvalPlanQual if any, they will be fill in
				 * estate later after it been created.
				 */
				queryDesc->epqContext = portal->epqContext;
				
                /*
                 * If parent node have sent down parameters, and at least one
                 * of them is PARAM_EXEC we should avoid "single execution"
                 * model. All parent nodes deliver the same values for
                 * PARAM_EXTERN since these values are provided by client and
                 * they are not changed during the query execution.
                 * On the contrary, values of PARAM_EXEC are results of execution
                 * on the parent node and in general diferent parents send to
                 * this node different values and executions are not equivalent.
                 * Since PARAM_EXECs are always at the end of the list we just
                 * need to check last item to figure out if there are any
                 * PARAM_EXECs.
                 * NB: Check queryDesc->plannedstmt->nParamExec > 0 is incorrect
                 * here since queryDesc->plannedstmt->nParamExec may be used
                 * just to allocate space for them and no actual values passed.
				 *
				 * Also, if we are doing EvalPlanQual, we will be rescan soon, which
				 * is not supported in SharedQueue mode. Force to do it traditionally.
                 */
#ifdef __TBASE__
                for (i = 0; i < queryDesc->plannedstmt->nParamRemote; i++)
                {
                        RemoteParam *rparam = &queryDesc->plannedstmt->remoteparams[i];
                        if (rparam->paramkind == PARAM_EXEC &&
                            rparam->paramused != REMOTE_PARAM_INITPLAN) /* if it's from initplan, still work with shared queue */
                        {
                                paramNeedPassDown = true;
                                break;
                        }
                }

                if ((!paramPassDown && paramNeedPassDown) || queryDesc->epqContext != NULL)   
#else
                if (queryDesc->plannedstmt->nParamRemote > 0 &&
                        queryDesc->plannedstmt->remoteparams[queryDesc->plannedstmt->nParamRemote-1].paramkind == PARAM_EXEC)
#endif
                {
                    int        *consMap;
                    int         len;
                    ListCell   *lc;
                    Locator       *locator;
                    Oid            keytype;
                    DestReceiver *dest;
#ifdef __TBASE__
                    int16 nodeMap[MAX_NODES_NUMBER];
#endif

                    len = list_length(queryDesc->plannedstmt->distributionNodes);
                    consMap = (int *) palloc0(len * sizeof(int));
                    queryDesc->squeue = NULL;
                    queryDesc->myindex = -1;
                    PGXC_PARENT_NODE_ID = PGXCNodeGetNodeIdFromName(PGXC_PARENT_NODE,
                                                       &PGXC_PARENT_NODE_TYPE);

#ifdef __TBASE__
                    /* init nodemap */
                    for (i = 0; i < MAX_NODES_NUMBER; i++)
                    {
                        nodeMap[i] = SQ_CONS_INIT;
                    }
#endif
                    i = 0;
                    foreach(lc, queryDesc->plannedstmt->distributionNodes)
                    {
#ifdef __TBASE__
                        int nodeid = lfirst_int(lc);

                        if (PGXC_PARENT_NODE_ID == nodeid)
                        {
                            consMap[i] = SQ_CONS_SELF;

                            nodeMap[nodeid] = SQ_CONS_SELF;
                        }
                        else
                        {
                            consMap[i] = SQ_CONS_NONE;

                            nodeMap[nodeid] = SQ_CONS_NONE;
                        }
                        i++;
#else
                        if (PGXC_PARENT_NODE_ID == lfirst_int(lc))
                            consMap[i] = SQ_CONS_SELF;
                        else
                            consMap[i] = SQ_CONS_NONE;
                        i++;
#endif
                    }
                    /*
                     * Multiple executions of the RemoteSubplan may lead to name
                     * conflict of SharedQueue, if the subplan has more
                     * RemoteSubplan nodes in the execution plan tree.
                     * We need to make them unique.
                     */
                    RemoteSubplanMakeUnique(
                            (Node *) queryDesc->plannedstmt->planTree,
							PGXC_PARENT_NODE_ID, parentPGXCPid);

                    elog(DEBUG3, "RemoteSubplanMakeUnique for PARAM_EXEC unique: %d, portal: %s",
                         PGXC_PARENT_NODE_ID, portal->name);
                    /*
                     * Call ExecutorStart to prepare the plan for execution
                     */
                    ExecutorStart(queryDesc, eflags);

                    /*
                     * Set up locator if result distribution is requested
                     */
                    keytype = queryDesc->plannedstmt->distributionKey == InvalidAttrNumber ?
                            InvalidOid :
                            queryDesc->tupDesc->attrs[queryDesc->plannedstmt->distributionKey-1]->atttypid;
#ifdef _MIGRATE_
                    locator = createLocator(
                            queryDesc->plannedstmt->distributionType,
                            RELATION_ACCESS_INSERT,
                            keytype,
                            LOCATOR_LIST_INT,
                            len,
                            consMap,
                            NULL,
                            false,
                            InvalidOid, InvalidOid, InvalidOid, InvalidAttrNumber, InvalidOid);
#else        
                    locator = createLocator(
                            queryDesc->plannedstmt->distributionType,
                            RELATION_ACCESS_INSERT,
                            keytype,
                            LOCATOR_LIST_INT,
                            len,
                            consMap,
                            NULL,
                            false);
#endif
                    dest = CreateDestReceiver(DestProducer);
                    SetProducerDestReceiverParams(dest,
                            queryDesc->plannedstmt->distributionKey,
                            locator, queryDesc->squeue
#ifdef __TBASE__
                            ,
                            queryDesc->sender
#endif
                            );
#ifdef __TBASE__
                    /* set nodemap for shard distribution */
                    if (LOCATOR_TYPE_SHARD == queryDesc->plannedstmt->distributionType)
                    {
                        SetProducerNodeMap(dest, nodeMap);
                    }
#endif
                    queryDesc->dest = dest;
                }
                else
                {
                    int        *consMap;
                    int         len;

                    /* Distributed data requested, bind shared queue for data exchange */
                    len = list_length(queryDesc->plannedstmt->distributionNodes);
                    consMap = (int *) palloc(len * sizeof(int));
                    queryDesc->squeue = SharedQueueBind(portal->name,
                                queryDesc->plannedstmt->distributionRestrict,
                                queryDesc->plannedstmt->distributionNodes,
                                &queryDesc->myindex, consMap
#ifdef __TBASE__
                                ,&queryDesc->sender
#endif
                                );
                    if (queryDesc->myindex == -1)
                    {
                        /* producer */
                        Locator       *locator;
                        Oid            keytype;
                        DestReceiver *dest;

#ifdef __TBASE__
                        if (distributed_query_analyze)
                        {
                            StoreQueryAnalyzeInfo(portal->name, queryDesc->plannedstmt);
                        }
#endif

                        PG_TRY();
                        {
                            /*
                             * Call ExecutorStart to prepare the plan for execution
                             */
                            ExecutorStart(queryDesc, eflags);
                        }
                        PG_CATCH();
                        {
                            /* Ensure SharedQueue is released */
                            SharedQueueUnBind(queryDesc->squeue, true);
                            queryDesc->squeue = NULL;
                            PG_RE_THROW();
                        }
                        PG_END_TRY();

                        /*
                         * This tells PortalCleanup to shut down the executor
                         */
                        portal->queryDesc = queryDesc;

                        /*
                         * Some basic sanity checking against invalid remote plans.
                         */
                        Assert((queryDesc->plannedstmt->distributionKey == InvalidAttrNumber) ||
                               (queryDesc->plannedstmt->distributionKey <= queryDesc->tupDesc->natts));

                        /*
                         * Set up locator if result distribution is requested
                         */
                        keytype = queryDesc->plannedstmt->distributionKey == InvalidAttrNumber ?
                                InvalidOid :
                                queryDesc->tupDesc->attrs[queryDesc->plannedstmt->distributionKey-1]->atttypid;
#ifdef _MIGRATE_
                        locator = createLocator(
                                queryDesc->plannedstmt->distributionType,
                                RELATION_ACCESS_INSERT,
                                keytype,
                                LOCATOR_LIST_INT,
                                len,
                                consMap,
                                NULL,
                                false,
                                InvalidOid, InvalidOid, InvalidOid, InvalidAttrNumber, InvalidOid);
#else
                        locator = createLocator(
                                queryDesc->plannedstmt->distributionType,
                                RELATION_ACCESS_INSERT,
                                keytype,
                                LOCATOR_LIST_INT,
                                len,
                                consMap,
                                NULL,
                                false);
#endif

#ifdef __TBASE__
                        if (needParallelSend(queryDesc->squeue))
                        {
                            SetLocatorInfo(queryDesc->squeue, consMap, len, 
                                           queryDesc->plannedstmt->distributionType, keytype, queryDesc->plannedstmt->distributionKey);
                        }
#endif
                        dest = CreateDestReceiver(DestProducer);
                        SetProducerDestReceiverParams(dest,
                                queryDesc->plannedstmt->distributionKey,
                                locator, queryDesc->squeue
#ifdef __TBASE__
                              ,
                            queryDesc->sender
#endif
                                );
                        queryDesc->dest = dest;

                        addProducingPortal(portal);
                    }
                    else
                    {
                        /*
                         * We do not need to initialize executor, but need
                         * a tuple descriptor
                         */
                        queryDesc->tupDesc = ExecCleanTypeFromTL(
                                queryDesc->plannedstmt->planTree->targetlist,
                                false);
                    }
                    pfree(consMap);
                }

                portal->queryDesc = queryDesc;

                /*
                 * Remember tuple descriptor (computed by ExecutorStart)
                 */
                portal->tupDesc = queryDesc->tupDesc;

                /*
                 * Reset cursor position data to "start of query"
                 */
                portal->atStart = true;
                portal->atEnd = false;    /* allow fetches */
                portal->portalPos = 0;

                PopActiveSnapshot();
			}
                break;
#endif

            case PORTAL_ONE_SELECT:
                /* Must set snapshot before starting executor. */
                if (snapshot)
                    PushActiveSnapshot(snapshot);
                else
#ifdef __TBASE__
                {
                    /*
                     * set snapshot if needed
                     */
                    if (NeedSnapshot(linitial_node(PlannedStmt, portal->stmts)))
                     {
                         PushActiveSnapshot(GetTransactionSnapshot());
                     }
                    else
                    {
                        snapshot_set = false;
                    }
                }
#else
                    PushActiveSnapshot(GetTransactionSnapshot());
#endif

                /*
                 * Create QueryDesc in portal's context; for the moment, set
                 * the destination to DestNone.
                 */
#ifdef __TBASE__
                if (!snapshot_set)
                {
                    queryDesc = CreateQueryDesc(linitial_node(PlannedStmt, portal->stmts),
                                                portal->sourceText,
                                                InvalidSnapshot,
                                                InvalidSnapshot,
                                                None_Receiver,
                                                params,
                                                portal->queryEnv,
                                                portal->up_instrument);
                }
                else
#endif
                queryDesc = CreateQueryDesc(linitial_node(PlannedStmt, portal->stmts),
                                            portal->sourceText,
                                            GetActiveSnapshot(),
                                            InvalidSnapshot,
                                            None_Receiver,
                                            params,
                                            portal->queryEnv,
#ifdef __TBASE__
                                            portal->up_instrument);
#else
                                            0);
#endif
                /*
				 * set information about EvalPlanQual if any, they will be fill in
				 * estate later after it been created.
				 */
				queryDesc->epqContext = portal->epqContext;
				
				/*
                 * If it's a scrollable cursor, executor needs to support
                 * REWIND and backwards scan, as well as whatever the caller
                 * might've asked for.
                 */
                if (portal->cursorOptions & CURSOR_OPT_SCROLL)
                    myeflags = eflags | EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD;
                else
                    myeflags = eflags;

                /*
                 * Call ExecutorStart to prepare the plan for execution
                 */
                ExecutorStart(queryDesc, myeflags);

                /*
                 * This tells PortalCleanup to shut down the executor
                 */
                portal->queryDesc = queryDesc;

                /*
                 * Remember tuple descriptor (computed by ExecutorStart)
                 */
                portal->tupDesc = queryDesc->tupDesc;

                /*
                 * Reset cursor position data to "start of query"
                 */
                portal->atStart = true;
                portal->atEnd = false;    /* allow fetches */
                portal->portalPos = 0;

#ifdef __TBASE__
                if (snapshot_set)
                {
                    PopActiveSnapshot();
                }
#else
                PopActiveSnapshot();
#endif
                break;

            case PORTAL_ONE_RETURNING:
            case PORTAL_ONE_MOD_WITH:

                /*
                 * We don't start the executor until we are told to run the
                 * portal.  We do need to set up the result tupdesc.
                 */
                {
                    PlannedStmt *pstmt;
					List *list = NIL;

                    pstmt = PortalGetPrimaryStmt(portal);
					if (portal->strategy == PORTAL_ONE_RETURNING &&
						pstmt->parseTree && pstmt->parseTree->returningList)
						list = pstmt->parseTree->returningList;
					else
						list = pstmt->planTree->targetlist;
                    portal->tupDesc =
						ExecCleanTypeFromTL(list, false);
                }

                /*
                 * Reset cursor position data to "start of query"
                 */
                portal->atStart = true;
                portal->atEnd = false;    /* allow fetches */
                portal->portalPos = 0;
                break;

            case PORTAL_UTIL_SELECT:

                /*
                 * We don't set snapshot here, because PortalRunUtility will
                 * take care of it if needed.
                 */
                {
                    PlannedStmt *pstmt = PortalGetPrimaryStmt(portal);

                    Assert(pstmt->commandType == CMD_UTILITY);
                    portal->tupDesc = UtilityTupleDescriptor(pstmt->utilityStmt);
                }

                /*
                 * Reset cursor position data to "start of query"
                 */
                portal->atStart = true;
                portal->atEnd = false;    /* allow fetches */
                portal->portalPos = 0;
                break;

            case PORTAL_MULTI_QUERY:
                /* Need do nothing now */
                portal->tupDesc = NULL;
                break;
        }
		
		if (PortalStart_hook)
			PortalStart_hook(portal);
    }
    PG_CATCH();
    {
        /* Uncaught error while executing portal: mark it dead */
        MarkPortalFailed(portal);

#ifdef XCP
        if (queryDesc && queryDesc->squeue)
        {
            /*
             * Associate the query desc with the portal so it is unbound upon
             * transaction end.
             */
            portal->queryDesc = queryDesc;
        }
#endif

        /* Restore global vars and propagate error */
        ActivePortal = saveActivePortal;
        CurrentResourceOwner = saveResourceOwner;
        PortalContext = savePortalContext;

        PG_RE_THROW();
    }
    PG_END_TRY();

    MemoryContextSwitchTo(oldContext);

    ActivePortal = saveActivePortal;
    CurrentResourceOwner = saveResourceOwner;
    PortalContext = savePortalContext;

    portal->status = PORTAL_READY;
}

/*
 * PortalSetResultFormat
 *        Select the format codes for a portal's output.
 *
 * This must be run after PortalStart for a portal that will be read by
 * a DestRemote or DestRemoteExecute destination.  It is not presently needed
 * for other destination types.
 *
 * formats[] is the client format request, as per Bind message conventions.
 */
void
PortalSetResultFormat(Portal portal, int nFormats, int16 *formats)
{
    int            natts;
    int            i;

    /* Do nothing if portal won't return tuples */
    if (portal->tupDesc == NULL)
        return;
    natts = portal->tupDesc->natts;
    portal->formats = (int16 *)
        MemoryContextAlloc(PortalGetHeapMemory(portal),
                           natts * sizeof(int16));
    if (nFormats > 1)
    {
        /* format specified for each column */
        if (nFormats != natts)
            ereport(ERROR,
                    (errcode(ERRCODE_PROTOCOL_VIOLATION),
                     errmsg("bind message has %d result formats but query has %d columns",
                            nFormats, natts)));
        memcpy(portal->formats, formats, natts * sizeof(int16));
    }
    else if (nFormats > 0)
    {
        /* single format specified, use for all columns */
        int16        format1 = formats[0];

        for (i = 0; i < natts; i++)
            portal->formats[i] = format1;
    }
    else
    {
        /* use default format for all columns */
        for (i = 0; i < natts; i++)
            portal->formats[i] = 0;
    }
}

/*
 * PortalRun
 *        Run a portal's query or queries.
 *
 * count <= 0 is interpreted as a no-op: the destination gets started up
 * and shut down, but nothing else happens.  Also, count == FETCH_ALL is
 * interpreted as "all rows".  Note that count is ignored in multi-query
 * situations, where we always run the portal to completion.
 *
 * isTopLevel: true if query is being executed at backend "top level"
 * (that is, directly from a client command message)
 *
 * dest: where to send output of primary (canSetTag) query
 *
 * altdest: where to send output of non-primary queries
 *
 * completionTag: points to a buffer of size COMPLETION_TAG_BUFSIZE
 *        in which to store a command completion status string.
 *        May be NULL if caller doesn't want a status string.
 *
 * Returns TRUE if the portal's execution is complete, FALSE if it was
 * suspended due to exhaustion of the count parameter.
 */
bool
PortalRun(Portal portal, long count, bool isTopLevel, bool run_once,
          DestReceiver *dest, DestReceiver *altdest,
          char *completionTag)
{// #lizard forgives
    bool        result;
    uint64        nprocessed;
    ResourceOwner saveTopTransactionResourceOwner;
    MemoryContext saveTopTransactionContext;
    Portal        saveActivePortal;
    ResourceOwner saveResourceOwner;
    MemoryContext savePortalContext;
    MemoryContext saveMemoryContext;

    AssertArg(PortalIsValid(portal));

    TRACE_POSTGRESQL_QUERY_EXECUTE_START();

    /* Initialize completion tag to empty string */
    if (completionTag)
        completionTag[0] = '\0';

    if (log_executor_stats && portal->strategy != PORTAL_MULTI_QUERY)
    {
        elog(DEBUG3, "PortalRun");
        /* PORTAL_MULTI_QUERY logs its own stats per query */
        ResetUsage();
    }

    /*
     * Check for improper portal use, and mark portal active.
     */
    MarkPortalActive(portal);

    /* Set run_once flag.  Shouldn't be clear if previously set. */
    Assert(!portal->run_once || run_once);
    portal->run_once = run_once;

    /*
     * Set up global portal context pointers.
     *
     * We have to play a special game here to support utility commands like
     * VACUUM and CLUSTER, which internally start and commit transactions.
     * When we are called to execute such a command, CurrentResourceOwner will
     * be pointing to the TopTransactionResourceOwner --- which will be
     * destroyed and replaced in the course of the internal commit and
     * restart.  So we need to be prepared to restore it as pointing to the
     * exit-time TopTransactionResourceOwner.  (Ain't that ugly?  This idea of
     * internally starting whole new transactions is not good.)
     * CurrentMemoryContext has a similar problem, but the other pointers we
     * save here will be NULL or pointing to longer-lived objects.
     */
    saveTopTransactionResourceOwner = TopTransactionResourceOwner;
    saveTopTransactionContext = TopTransactionContext;
    saveActivePortal = ActivePortal;
    saveResourceOwner = CurrentResourceOwner;
    savePortalContext = PortalContext;
    saveMemoryContext = CurrentMemoryContext;
#ifdef __TBASE__
    parallelExecutionError = NULL;
#endif
    PG_TRY();
    {
        ActivePortal = portal;
        if (portal->resowner)
            CurrentResourceOwner = portal->resowner;
        PortalContext = PortalGetHeapMemory(portal);

        MemoryContextSwitchTo(PortalContext);

        switch (portal->strategy)
        {
            case PORTAL_ONE_SELECT:
            case PORTAL_ONE_RETURNING:
            case PORTAL_ONE_MOD_WITH:
            case PORTAL_UTIL_SELECT:

                /*
                 * If we have not yet run the command, do so, storing its
                 * results in the portal's tuplestore.  But we don't do that
                 * for the PORTAL_ONE_SELECT case.
                 */
                if (portal->strategy != PORTAL_ONE_SELECT && !portal->holdStore)
                    FillPortalStore(portal, isTopLevel);

                /*
                 * Now fetch desired portion of results.
                 */
                nprocessed = PortalRunSelect(portal, true, count, dest);

                /*
                 * If the portal result contains a command tag and the caller
                 * gave us a pointer to store it, copy it. Patch the "SELECT"
                 * tag to also provide the rowcount.
                 */
                if (completionTag && portal->commandTag)
                {
                    if (strcmp(portal->commandTag, "SELECT") == 0)
                        snprintf(completionTag, COMPLETION_TAG_BUFSIZE,
                                 "SELECT " UINT64_FORMAT, nprocessed);
                    else
                        strcpy(completionTag, portal->commandTag);
                }

                /* Mark portal not active */
                portal->status = PORTAL_READY;

                /*
                 * Since it's a forward fetch, say DONE iff atEnd is now true.
                 */
                result = portal->atEnd;
                break;

            case PORTAL_MULTI_QUERY:
                PortalRunMulti(portal, isTopLevel, false,
                               dest, altdest, completionTag);

                /* Prevent portal's commands from being re-executed */
                MarkPortalDone(portal);

                /* Always complete at end of RunMulti */
                result = true;
                break;

#ifdef XCP
            case PORTAL_DISTRIBUTED:
                if (count == FETCH_ALL)
                    count = 0;
                nprocessed = 0;

                if (portal->queryDesc->myindex == -1)
                {
                    if (portal->queryDesc->squeue)
                    {
                        /* Make sure the producer is advancing */
                        while (count == 0 || nprocessed < count)
                        {
                            if (!portal->queryDesc->estate->es_finished)
                                AdvanceProducingPortal(portal, false);
                            /* make read pointer active */
                            tuplestore_select_read_pointer(portal->holdStore, 1);
                            /* perform reads */
                            nprocessed += RunFromStore(portal,
                                                       ForwardScanDirection,
                                               count ? count - nprocessed : 0,
                                                       dest);
                            /*
                             * Switch back to the write pointer
                             * We do not want to seek if the tuplestore operates
                             * with a file, so copy pointer before.
                             * Also advancing write pointer would allow to free some
                             * memory.
                             */
                            tuplestore_copy_read_pointer(portal->holdStore, 1, 0);
                            tuplestore_select_read_pointer(portal->holdStore, 0);
                            /* try to release occupied memory */
                            tuplestore_trim(portal->holdStore);
                            /* Break if we can not get more rows */
                            if (portal->queryDesc->estate->es_finished)
                                break;
                        }
                        if (nprocessed > 0)
                            portal->atStart = false; /* OK to go backward now */
                        portal->atEnd = portal->queryDesc->estate->es_finished &&
                            tuplestore_ateof(portal->holdStore);
                        portal->portalPos += nprocessed;

#ifdef __TBASE__
                        if (portal->atEnd)
                        {
                            removeProducingPortal(portal);
                        }
#endif
                    }
                    else
                    {
                        DestReceiver *olddest;

                        count = FETCH_ALL;

                        Assert(portal->queryDesc->dest->mydest == DestProducer);
                        olddest = SetSelfConsumerDestReceiver(
                                portal->queryDesc->dest, dest);
                        /*
                         * Now fetch desired portion of results.
                         */
                        nprocessed = PortalRunSelect(portal, true, count,
                                                     portal->queryDesc->dest);
                        SetSelfConsumerDestReceiver(
                                portal->queryDesc->dest, olddest);
                    }
                }
                else
                {
                    QueryDesc       *queryDesc = portal->queryDesc;
                    SharedQueue        squeue = queryDesc->squeue;
                    int             myindex = queryDesc->myindex;
                    TupleTableSlot *slot;

                    if (squeue == NULL)
                    {
                        elog(ERROR, "squeue: %s is null, myindex: %d, atStart: %d, atEnd: %d", portal->name, myindex, portal->atStart, portal->atEnd);
                    }

                    /*
                     * We are the consumer.
                     * We have skipped plan initialization, hence we do not have
                     * a tuple table to get a slot to receive tuples, so prepare
                     * standalone slot.
                     */
                    slot = MakeSingleTupleTableSlot(queryDesc->tupDesc);

                    (*dest->rStartup) (dest, CMD_SELECT, queryDesc->tupDesc);

                    /*
                     * Loop until we've processed the proper number of tuples
                     * from the plan.
                     */
                    for (;;)
                    {
                        List *producing = getProducingPortals();
                        bool done;

                        /*
                         * Obtain a tuple from the queue.
                         * If the session is running producing cursors it is
                         * not safe to wait for available tuple. Two sessions
                         * may deadlock each other. So if session is producing
                         * it should keep advancing producing cursors.
                         */

                        done = SharedQueueRead(squeue, myindex, slot,
                                               list_length(producing) == 0);
                        /*
                         * if the tuple is null, then we assume there is nothing
                         * more to process so we end the loop...
                         * Also if null tuple is returned the squeue is reset
                         * already, we want to prevent resetting it again
                         */
                        if (TupIsNull(slot))
                        {
                            if (!done && producing)
                            {
                                /* No data to read, advance producing portals */
                                ListCell   *lc = list_head(producing);
                                while (lc)
                                {
                                    Portal p = (Portal) lfirst(lc);
                                    /* Get reference to next entry before
                                     * advancing current portal, because the
                                     * function may remove current entry from
                                     * the list.
                                     */
                                    lc = lnext(lc);

                                    AdvanceProducingPortal(p, false);
                                }
                                continue;
                            }
                            else
                            {
                                queryDesc->squeue = NULL;
                                break;
                            }
                        }
                        /*
                         * Send the tuple
                         */
                        (*dest->receiveSlot) (slot, dest);

                        /*
                         * increment the number of processed tuples and check count.
                         * If we've processed the proper number then quit, else
                         * loop again and process more tuples. Zero count means
                         * no limit.
                         */
                        if (count && count == ++nprocessed)
                            break;
                    }
                    (*dest->rShutdown) (dest);

                    ExecDropSingleTupleTableSlot(slot);

                    if (nprocessed > 0)
                        portal->atStart = false;        /* OK to go backward now */
                    if (count == 0 ||
                        (unsigned long) nprocessed < (unsigned long) count)
                        portal->atEnd = true;    /* we retrieved 'em all */
                    portal->portalPos += nprocessed;
                }
                /* Mark portal not active */
                portal->status = PORTAL_READY;
                result = portal->atEnd;
                break;
#endif

            default:
                elog(ERROR, "unrecognized portal strategy: %d",
                     (int) portal->strategy);
                result = false; /* keep compiler quiet */
                break;
        }
    }
    PG_CATCH();
    {
#ifdef __TBASE__
        HandleParallelExecutionError();
#endif
        /* Uncaught error while executing portal: mark it dead */
        MarkPortalFailed(portal);

        /* Restore global vars and propagate error */
        if (saveMemoryContext == saveTopTransactionContext)
            MemoryContextSwitchTo(TopTransactionContext);
        else
            MemoryContextSwitchTo(saveMemoryContext);
        ActivePortal = saveActivePortal;
        if (saveResourceOwner == saveTopTransactionResourceOwner)
            CurrentResourceOwner = TopTransactionResourceOwner;
        else
            CurrentResourceOwner = saveResourceOwner;
        PortalContext = savePortalContext;

        PG_RE_THROW();
    }
    PG_END_TRY();

    if (saveMemoryContext == saveTopTransactionContext)
        MemoryContextSwitchTo(TopTransactionContext);
#ifdef __TBASE__
    /*
     * saveMemoryContext points to subtransaction's memorycontext, but ROLLBACK SUBTXN
     * has already released the resource, so we need to switch to current transaction context.
     */
    else if (IS_PGXC_DATANODE && portal->commandTag && (strcmp(portal->commandTag, "ROLLBACK SUBTXN") == 0))
    {
        MemoryContext curTransactionContext = GetCurrentTransactionContext();
        MemoryContextSwitchTo(curTransactionContext);
    }
#endif
    else
        MemoryContextSwitchTo(saveMemoryContext);
    ActivePortal = saveActivePortal;
    if (saveResourceOwner == saveTopTransactionResourceOwner)
        CurrentResourceOwner = TopTransactionResourceOwner;
#ifdef __TBASE__
    /*
     * saveResourceOwner points to subtransaction's resourceOwner, but ROLLBACK SUBTXN
     * has already released the resource, so we need to switch to current transaction owner.
     */
	else if (IS_PGXC_DATANODE && portal->commandTag && (strcmp(portal->commandTag, "ROLLBACK SUBTXN") == 0))
    {
        CurrentResourceOwner = GetCurrentTransactionResourceOwner();
    }
#endif
    else
        CurrentResourceOwner = saveResourceOwner;
    PortalContext = savePortalContext;

    if (log_executor_stats && portal->strategy != PORTAL_MULTI_QUERY)
        ShowUsage("EXECUTOR STATISTICS");

    TRACE_POSTGRESQL_QUERY_EXECUTE_DONE();

    return result;
}

/*
 * PortalRunSelect
 *        Execute a portal's query in PORTAL_ONE_SELECT mode, and also
 *        when fetching from a completed holdStore in PORTAL_ONE_RETURNING,
 *        PORTAL_ONE_MOD_WITH, and PORTAL_UTIL_SELECT cases.
 *
 * This handles simple N-rows-forward-or-backward cases.  For more complex
 * nonsequential access to a portal, see PortalRunFetch.
 *
 * count <= 0 is interpreted as a no-op: the destination gets started up
 * and shut down, but nothing else happens.  Also, count == FETCH_ALL is
 * interpreted as "all rows".  (cf FetchStmt.howMany)
 *
 * Caller must already have validated the Portal and done appropriate
 * setup (cf. PortalRun).
 *
 * Returns number of rows processed (suitable for use in result tag)
 */
static uint64
PortalRunSelect(Portal portal,
                bool forward,
                long count,
                DestReceiver *dest)
{// #lizard forgives
    QueryDesc  *queryDesc;
    ScanDirection direction;
    uint64        nprocessed;
    struct        rusage start_r;
    struct        timeval start_t;

    if (log_executor_stats)
        ResetUsageCommon(&start_r, &start_t);
    /*
     * NB: queryDesc will be NULL if we are fetching from a held cursor or a
     * completed utility query; can't use it in that path.
     */
    queryDesc = PortalGetQueryDesc(portal);

    /* Caller messed up if we have neither a ready query nor held data. */
    Assert(queryDesc || portal->holdStore);

    /*
     * Force the queryDesc destination to the right thing.  This supports
     * MOVE, for example, which will pass in dest = DestNone.  This is okay to
     * change as long as we do it on every fetch.  (The Executor must not
     * assume that dest never changes.)
     */
    if (queryDesc)
        queryDesc->dest = dest;

    /*
     * Determine which direction to go in, and check to see if we're already
     * at the end of the available tuples in that direction.  If so, set the
     * direction to NoMovement to avoid trying to fetch any tuples.  (This
     * check exists because not all plan node types are robust about being
     * called again if they've already returned NULL once.)  Then call the
     * executor (we must not skip this, because the destination needs to see a
     * setup and shutdown even if no tuples are available).  Finally, update
     * the portal position state depending on the number of tuples that were
     * retrieved.
     */
    if (forward)
    {
        if (portal->atEnd || count <= 0)
        {
            direction = NoMovementScanDirection;
            count = 0;            /* don't pass negative count to executor */
        }
        else
            direction = ForwardScanDirection;

        /* In the executor, zero count processes all rows */
        if (count == FETCH_ALL)
            count = 0;

        if (portal->holdStore)
            nprocessed = RunFromStore(portal, direction, (uint64) count, dest);
        else
        {
            PushActiveSnapshot(queryDesc->snapshot);
            ExecutorRun(queryDesc, direction, (uint64) count,
                        portal->run_once);
            nprocessed = queryDesc->estate->es_processed;
            PopActiveSnapshot();
        }

        if (!ScanDirectionIsNoMovement(direction))
        {
            if (nprocessed > 0)
                portal->atStart = false;    /* OK to go backward now */
            if (count == 0 || nprocessed < (uint64) count)
                portal->atEnd = true;    /* we retrieved 'em all */
            portal->portalPos += nprocessed;
        }
    }
    else
    {
        if (portal->cursorOptions & CURSOR_OPT_NO_SCROLL)
            ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                     errmsg("cursor can only scan forward"),
                     errhint("Declare it with SCROLL option to enable backward scan.")));

        if (portal->atStart || count <= 0)
        {
            direction = NoMovementScanDirection;
            count = 0;            /* don't pass negative count to executor */
        }
        else
            direction = BackwardScanDirection;

        /* In the executor, zero count processes all rows */
        if (count == FETCH_ALL)
            count = 0;

        if (portal->holdStore)
            nprocessed = RunFromStore(portal, direction, (uint64) count, dest);
        else
        {
            PushActiveSnapshot(queryDesc->snapshot);
            ExecutorRun(queryDesc, direction, (uint64) count,
                        portal->run_once);
            nprocessed = queryDesc->estate->es_processed;
            PopActiveSnapshot();
        }

        if (!ScanDirectionIsNoMovement(direction))
        {
            if (nprocessed > 0 && portal->atEnd)
            {
                portal->atEnd = false;    /* OK to go forward now */
                portal->portalPos++;    /* adjust for endpoint case */
            }
            if (count == 0 || nprocessed < (uint64) count)
            {
                portal->atStart = true; /* we retrieved 'em all */
                portal->portalPos = 0;
            }
            else
            {
                portal->portalPos -= nprocessed;
            }
        }
    }

    if (log_executor_stats)
        ShowUsageCommon("PortalRunSelect", &start_r, &start_t);
    return nprocessed;
}

/*
 * FillPortalStore
 *        Run the query and load result tuples into the portal's tuple store.
 *
 * This is used for PORTAL_ONE_RETURNING, PORTAL_ONE_MOD_WITH, and
 * PORTAL_UTIL_SELECT cases only.
 */
static void
FillPortalStore(Portal portal, bool isTopLevel)
{
    DestReceiver *treceiver;
    char        completionTag[COMPLETION_TAG_BUFSIZE];

    PortalCreateHoldStore(portal);
    treceiver = CreateDestReceiver(DestTuplestore);
    SetTuplestoreDestReceiverParams(treceiver,
                                    portal->holdStore,
                                    portal->holdContext,
                                    false);

    completionTag[0] = '\0';

    switch (portal->strategy)
    {
        case PORTAL_ONE_RETURNING:
        case PORTAL_ONE_MOD_WITH:

            /*
             * Run the portal to completion just as for the default
             * MULTI_QUERY case, but send the primary query's output to the
             * tuplestore.  Auxiliary query outputs are discarded.  Set the
             * portal's holdSnapshot to the snapshot used (or a copy of it).
             */
            PortalRunMulti(portal, isTopLevel, true,
                           treceiver, None_Receiver, completionTag);
            break;

        case PORTAL_UTIL_SELECT:
            PortalRunUtility(portal, linitial_node(PlannedStmt, portal->stmts),
                             isTopLevel, true, treceiver, completionTag);
            break;

        default:
            elog(ERROR, "unsupported portal strategy: %d",
                 (int) portal->strategy);
            break;
    }

    /* Override default completion tag with actual command result */
    if (completionTag[0] != '\0')
        portal->commandTag = pstrdup(completionTag);

    (*treceiver->rDestroy) (treceiver);
}

/*
 * RunFromStore
 *        Fetch tuples from the portal's tuple store.
 *
 * Calling conventions are similar to ExecutorRun, except that we
 * do not depend on having a queryDesc or estate.  Therefore we return the
 * number of tuples processed as the result, not in estate->es_processed.
 *
 * One difference from ExecutorRun is that the destination receiver functions
 * are run in the caller's memory context (since we have no estate).  Watch
 * out for memory leaks.
 */
static uint64
RunFromStore(Portal portal, ScanDirection direction, uint64 count,
             DestReceiver *dest)
{
    uint64        current_tuple_count = 0;
    TupleTableSlot *slot;

    slot = MakeSingleTupleTableSlot(portal->tupDesc);

    (*dest->rStartup) (dest, CMD_SELECT, portal->tupDesc);

    if (ScanDirectionIsNoMovement(direction))
    {
        /* do nothing except start/stop the destination */
    }
    else
    {
        bool        forward = ScanDirectionIsForward(direction);

        for (;;)
        {
            MemoryContext oldcontext;
            bool        ok;

            oldcontext = MemoryContextSwitchTo(portal->holdContext);

            ok = tuplestore_gettupleslot(portal->holdStore, forward, false,
                                         slot);

            MemoryContextSwitchTo(oldcontext);

            if (!ok)
                break;

            /*
             * If we are not able to send the tuple, we assume the destination
             * has closed and no more tuples can be sent. If that's the case,
             * end the loop.
             */
            if (!((*dest->receiveSlot) (slot, dest)))
                break;

            ExecClearTuple(slot);

            /*
             * check our tuple count.. if we've processed the proper number
             * then quit, else loop again and process more tuples. Zero count
             * means no limit.
             */
            current_tuple_count++;
            if (count && count == current_tuple_count)
                break;
        }
    }

    (*dest->rShutdown) (dest);

    ExecDropSingleTupleTableSlot(slot);

    return current_tuple_count;
}

/*
 * PortalRunUtility
 *        Execute a utility statement inside a portal.
 */
static void
PortalRunUtility(Portal portal, PlannedStmt *pstmt,
                 bool isTopLevel, bool setHoldSnapshot,
                 DestReceiver *dest, char *completionTag)
{// #lizard forgives
    Node       *utilityStmt = pstmt->utilityStmt;
    Snapshot    snapshot;

#ifdef __TBASE__
    /* 
     * If process alter gtm node OR create gtm node command, we set NewGtmHost and NewGtmPort here.
     * Fucntion GetTransactionSnapshot will try to connect gtm using this gtm info.
     */
    GetGtmInfoFromUserCmd(utilityStmt);
#endif

	/*
	 * Set snapshot if utility stmt needs one.  Most reliable way to do this
	 * seems to be to enumerate those that do not need one; this is a short
	 * list.  Transaction control, LOCK, and SET must *not* set a snapshot
	 * since they need to be executable at the start of a transaction-snapshot
	 * mode transaction without freezing a snapshot.  By extension we allow
	 * SHOW not to set a snapshot.  The other stmts listed are just efficiency
	 * hacks.  Beware of listing anything that can modify the database --- if,
	 * say, it has to update an index with expressions that invoke
	 * user-defined functions, then it had better have a snapshot.
	 */
	if (!(IsA(utilityStmt, TransactionStmt) ||
		  IsA(utilityStmt, LockStmt) ||
		  IsA(utilityStmt, VariableSetStmt) ||
		  IsA(utilityStmt, VariableShowStmt) ||
		  IsA(utilityStmt, ConstraintsSetStmt) ||
	/* efficiency hacks from here down */
		  IsA(utilityStmt, FetchStmt) ||
		  IsA(utilityStmt, ListenStmt) ||
		  IsA(utilityStmt, NotifyStmt) ||
		  IsA(utilityStmt, UnlistenStmt) ||
#ifdef __TBASE__
		  /* Node Lock/Unlock do not modify any data */
		  IsA(utilityStmt, LockNodeStmt) ||
#endif
#ifdef PGXC
          IsA(utilityStmt, PauseClusterStmt) ||
          IsA(utilityStmt, BarrierStmt) ||
          (IsA(utilityStmt, CheckPointStmt) && IS_PGXC_DATANODE)))
#else
          IsA(utilityStmt, CheckPointStmt)))
#endif
    {
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
		/* Avoid the start timestamp to be too old to execute on DNs */
		if(IsA(utilityStmt, VacuumStmt) || IsA(utilityStmt, AlterNodeStmt))
		{
			snapshot = GetLocalTransactionSnapshot();
		}
		else
        {
            snapshot = GetTransactionSnapshot();
        }
#else
        snapshot = GetTransactionSnapshot();
#endif
        /* If told to, register the snapshot we're using and save in portal */
        if (setHoldSnapshot)
        {
            snapshot = RegisterSnapshot(snapshot);
            portal->holdSnapshot = snapshot;
        }
        PushActiveSnapshot(snapshot);
        /* PushActiveSnapshot might have copied the snapshot */
        snapshot = GetActiveSnapshot();
    }
    else
        snapshot = NULL;

    ProcessUtility(pstmt,
                   portal->sourceText,
                   isTopLevel ? PROCESS_UTILITY_TOPLEVEL : PROCESS_UTILITY_QUERY,
                   portal->portalParams,
                   portal->queryEnv,
                   dest,
#ifdef PGXC
                   false,
#endif /* PGXC */
                   completionTag);

    /* Some utility statements may change context on us */
    MemoryContextSwitchTo(PortalGetHeapMemory(portal));

    /*
     * Some utility commands may pop the ActiveSnapshot stack from under us,
     * so be careful to only pop the stack if our snapshot is still at the
     * top.
     */
    if (snapshot != NULL && ActiveSnapshotSet() &&
        snapshot == GetActiveSnapshot())
        PopActiveSnapshot();
	else
	{
		/* Clear snapshots created in process QueryRewriteCTAS */
		while (ActiveSnapshotSet())
		{
			if (S_FOR_CTAS == GetActiveSnapshotStatus() ||
				snapshot == GetActiveSnapshot())
			{
				PopActiveSnapshot();
				continue;
			}
			break;
		}
	}
}

/*
 * PortalRunMulti
 *        Execute a portal's queries in the general case (multi queries
 *        or non-SELECT-like queries)
 */
static void
PortalRunMulti(Portal portal,
               bool isTopLevel, bool setHoldSnapshot,
               DestReceiver *dest, DestReceiver *altdest,
               char *completionTag)
{// #lizard forgives
    bool        active_snapshot_set = false;
    ListCell   *stmtlist_item;
#ifdef PGXC
    CombineTag    combine;

    combine.cmdType = CMD_UNKNOWN;
    combine.data[0] = '\0';
#endif

    /*
     * If the destination is DestRemoteExecute, change to DestNone.  The
     * reason is that the client won't be expecting any tuples, and indeed has
     * no way to know what they are, since there is no provision for Describe
     * to send a RowDescription message when this portal execution strategy is
     * in effect.  This presently will only affect SELECT commands added to
     * non-SELECT queries by rewrite rules: such commands will be executed,
     * but the results will be discarded unless you use "simple Query"
     * protocol.
     */
    if (dest->mydest == DestRemoteExecute)
        dest = None_Receiver;
    if (altdest->mydest == DestRemoteExecute)
        altdest = None_Receiver;

    /*
     * Loop to handle the individual queries generated from a single parsetree
     * by analysis and rewrite.
     */
    foreach(stmtlist_item, portal->stmts)
    {
        PlannedStmt *pstmt = lfirst_node(PlannedStmt, stmtlist_item);

        /*
         * If we got a cancel signal in prior command, quit
         */
        CHECK_FOR_INTERRUPTS();

        if (pstmt->utilityStmt == NULL)
        {
            /*
             * process a plannable query.
             */
            TRACE_POSTGRESQL_QUERY_EXECUTE_START();

            if (log_executor_stats)
                ResetUsage();

            /*
             * Must always have a snapshot for plannable queries.  First time
             * through, take a new snapshot; for subsequent queries in the
             * same portal, just update the snapshot's copy of the command
             * counter.
             */
            if (!active_snapshot_set)
            {
#ifdef __TBASE__
                if (!setHoldSnapshot && !NeedSnapshot(pstmt))
                {
                    /* will set snapshot when execute */
                    active_snapshot_set = true;
                }
                else
                {
#endif
                Snapshot    snapshot = GetTransactionSnapshot();

                /* If told to, register the snapshot and save in portal */
                if (setHoldSnapshot)
                {
                    snapshot = RegisterSnapshot(snapshot);
                    portal->holdSnapshot = snapshot;
                }

                /*
                 * We can't have the holdSnapshot also be the active one,
                 * because UpdateActiveSnapshotCommandId would complain.  So
                 * force an extra snapshot copy.  Plain PushActiveSnapshot
                 * would have copied the transaction snapshot anyway, so this
                 * only adds a copy step when setHoldSnapshot is true.  (It's
                 * okay for the command ID of the active snapshot to diverge
                 * from what holdSnapshot has.)
                 */
                PushCopiedSnapshot(snapshot);
                active_snapshot_set = true;
#ifdef __TBASE__
                }
                if (IS_PGXC_LOCAL_COORDINATOR)
                    SetSendCommandId(true);
#endif
            }
            else
                UpdateActiveSnapshotCommandId();

            if (pstmt->canSetTag)
            {
                /* statement can set tag string */
                ProcessQuery(pstmt,
                             portal->sourceText,
                             portal->portalParams,
                             portal->queryEnv,
							 dest, completionTag,
							 portal->up_instrument);
#ifdef PGXC
                /* it's special for INSERT */
                if (IS_PGXC_COORDINATOR &&
                    pstmt->commandType == CMD_INSERT)
                    HandleCmdComplete(pstmt->commandType, &combine,
                            completionTag, strlen(completionTag));
#endif
            }
            else
            {
                /* stmt added by rewrite cannot set tag */
                ProcessQuery(pstmt,
                             portal->sourceText,
                             portal->portalParams,
                             portal->queryEnv,
							 altdest, NULL,
							 portal->up_instrument);
            }

            if (log_executor_stats)
                ShowUsage("EXECUTOR STATISTICS");

            TRACE_POSTGRESQL_QUERY_EXECUTE_DONE();
        }
        else
        {
            /*
             * process utility functions (create, destroy, etc..)
             *
             * We must not set a snapshot here for utility commands (if one is
             * needed, PortalRunUtility will do it).  If a utility command is
             * alone in a portal then everything's fine.  The only case where
             * a utility command can be part of a longer list is that rules
             * are allowed to include NotifyStmt.  NotifyStmt doesn't care
             * whether it has a snapshot or not, so we just leave the current
             * snapshot alone if we have one.
             */
            if (pstmt->canSetTag)
            {
                Assert(!active_snapshot_set);
                /* statement can set tag string */
                PortalRunUtility(portal, pstmt, isTopLevel, false,
                                 dest, completionTag);
            }
            else
            {
                Assert(IsA(pstmt->utilityStmt, NotifyStmt));
                /* stmt added by rewrite cannot set tag */
                PortalRunUtility(portal, pstmt, isTopLevel, false,
                                 altdest, NULL);
            }
        }

        /*
         * Increment command counter between queries, but not after the last
         * one.
         */
        if (lnext(stmtlist_item) != NULL)
            CommandCounterIncrement();

        /*
         * Clear subsidiary contexts to recover temporary memory.
         */
        Assert(PortalGetHeapMemory(portal) == CurrentMemoryContext);

        MemoryContextDeleteChildren(PortalGetHeapMemory(portal));
    }

    /* Pop the snapshot if we pushed one. */
    if (active_snapshot_set)
        PopActiveSnapshot();

    /*
     * If a command completion tag was supplied, use it.  Otherwise use the
     * portal's commandTag as the default completion tag.
     *
     * Exception: Clients expect INSERT/UPDATE/DELETE tags to have counts, so
     * fake them with zeros.  This can happen with DO INSTEAD rules if there
     * is no replacement query of the same type as the original.  We print "0
     * 0" here because technically there is no query of the matching tag type,
     * and printing a non-zero count for a different query type seems wrong,
     * e.g.  an INSERT that does an UPDATE instead should not print "0 1" if
     * one row was updated.  See QueryRewrite(), step 3, for details.
     */

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && combine.data[0] != '\0')
        strcpy(completionTag, combine.data);
#endif

    if (completionTag && completionTag[0] == '\0')
    {
        if (portal->commandTag)
            strcpy(completionTag, portal->commandTag);
        if (strcmp(completionTag, "SELECT") == 0)
            sprintf(completionTag, "SELECT 0 0");
        else if (strcmp(completionTag, "INSERT") == 0)
            strcpy(completionTag, "INSERT 0 0");
        else if (strcmp(completionTag, "UPDATE") == 0)
            strcpy(completionTag, "UPDATE 0");
        else if (strcmp(completionTag, "DELETE") == 0)
            strcpy(completionTag, "DELETE 0");
    }
}

/*
 * PortalRunFetch
 *        Variant form of PortalRun that supports SQL FETCH directions.
 *
 * Note: we presently assume that no callers of this want isTopLevel = true.
 *
 * count <= 0 is interpreted as a no-op: the destination gets started up
 * and shut down, but nothing else happens.  Also, count == FETCH_ALL is
 * interpreted as "all rows".  (cf FetchStmt.howMany)
 *
 * Returns number of rows processed (suitable for use in result tag)
 */
uint64
PortalRunFetch(Portal portal,
               FetchDirection fdirection,
               long count,
               DestReceiver *dest)
{
    uint64        result;
    Portal        saveActivePortal;
    ResourceOwner saveResourceOwner;
    MemoryContext savePortalContext;
    MemoryContext oldContext;

    AssertArg(PortalIsValid(portal));

    /*
     * Check for improper portal use, and mark portal active.
     */
    MarkPortalActive(portal);

    /* If supporting FETCH, portal can't be run-once. */
    Assert(!portal->run_once);

    /*
     * Set up global portal context pointers.
     */
    saveActivePortal = ActivePortal;
    saveResourceOwner = CurrentResourceOwner;
    savePortalContext = PortalContext;
    PG_TRY();
    {
        ActivePortal = portal;
        if (portal->resowner)
            CurrentResourceOwner = portal->resowner;
        PortalContext = PortalGetHeapMemory(portal);

        oldContext = MemoryContextSwitchTo(PortalContext);

        switch (portal->strategy)
        {
            case PORTAL_ONE_SELECT:
                result = DoPortalRunFetch(portal, fdirection, count, dest);
                break;

            case PORTAL_ONE_RETURNING:
            case PORTAL_ONE_MOD_WITH:
            case PORTAL_UTIL_SELECT:

                /*
                 * If we have not yet run the command, do so, storing its
                 * results in the portal's tuplestore.
                 */
                if (!portal->holdStore)
                    FillPortalStore(portal, false /* isTopLevel */ );

                /*
                 * Now fetch desired portion of results.
                 */
                result = DoPortalRunFetch(portal, fdirection, count, dest);
                break;

            default:
                elog(ERROR, "unsupported portal strategy");
                result = 0;        /* keep compiler quiet */
                break;
        }
    }
    PG_CATCH();
    {
        /* Uncaught error while executing portal: mark it dead */
        MarkPortalFailed(portal);

        /* Restore global vars and propagate error */
        ActivePortal = saveActivePortal;
        CurrentResourceOwner = saveResourceOwner;
        PortalContext = savePortalContext;

        PG_RE_THROW();
    }
    PG_END_TRY();

    MemoryContextSwitchTo(oldContext);

    /* Mark portal not active */
    portal->status = PORTAL_READY;

    ActivePortal = saveActivePortal;
    CurrentResourceOwner = saveResourceOwner;
    PortalContext = savePortalContext;

    return result;
}

/*
 * DoPortalRunFetch
 *        Guts of PortalRunFetch --- the portal context is already set up
 *
 * count <= 0 is interpreted as a no-op: the destination gets started up
 * and shut down, but nothing else happens.  Also, count == FETCH_ALL is
 * interpreted as "all rows".  (cf FetchStmt.howMany)
 *
 * Returns number of rows processed (suitable for use in result tag)
 */
static uint64
DoPortalRunFetch(Portal portal,
                 FetchDirection fdirection,
                 long count,
                 DestReceiver *dest)
{// #lizard forgives
    bool        forward;

    Assert(portal->strategy == PORTAL_ONE_SELECT ||
           portal->strategy == PORTAL_ONE_RETURNING ||
           portal->strategy == PORTAL_ONE_MOD_WITH ||
           portal->strategy == PORTAL_UTIL_SELECT);

    switch (fdirection)
    {
        case FETCH_FORWARD:
            if (count < 0)
            {
                fdirection = FETCH_BACKWARD;
                count = -count;
            }
            /* fall out of switch to share code with FETCH_BACKWARD */
            break;
        case FETCH_BACKWARD:
            if (count < 0)
            {
                fdirection = FETCH_FORWARD;
                count = -count;
            }
            /* fall out of switch to share code with FETCH_FORWARD */
            break;
        case FETCH_ABSOLUTE:
            if (count > 0)
            {
                /*
                 * Definition: Rewind to start, advance count-1 rows, return
                 * next row (if any).
                 *
                 * In practice, if the goal is less than halfway back to the
                 * start, it's better to scan from where we are.
                 *
                 * Also, if current portalPos is outside the range of "long",
                 * do it the hard way to avoid possible overflow of the count
                 * argument to PortalRunSelect.  We must exclude exactly
                 * LONG_MAX, as well, lest the count look like FETCH_ALL.
                 *
                 * In any case, we arrange to fetch the target row going
                 * forwards.
                 */
                if ((uint64) (count - 1) <= portal->portalPos / 2 ||
                    portal->portalPos >= (uint64) LONG_MAX)
                {
                    DoPortalRewind(portal);
                    if (count > 1)
                        PortalRunSelect(portal, true, count - 1,
                                        None_Receiver);
                }
                else
                {
                    long        pos = (long) portal->portalPos;

                    if (portal->atEnd)
                        pos++;    /* need one extra fetch if off end */
                    if (count <= pos)
                        PortalRunSelect(portal, false, pos - count + 1,
                                        None_Receiver);
                    else if (count > pos + 1)
                        PortalRunSelect(portal, true, count - pos - 1,
                                        None_Receiver);
                }
                return PortalRunSelect(portal, true, 1L, dest);
            }
            else if (count < 0)
            {
                /*
                 * Definition: Advance to end, back up abs(count)-1 rows,
                 * return prior row (if any).  We could optimize this if we
                 * knew in advance where the end was, but typically we won't.
                 * (Is it worth considering case where count > half of size of
                 * query?  We could rewind once we know the size ...)
                 */
                PortalRunSelect(portal, true, FETCH_ALL, None_Receiver);
                if (count < -1)
                    PortalRunSelect(portal, false, -count - 1, None_Receiver);
                return PortalRunSelect(portal, false, 1L, dest);
            }
            else
            {
                /* count == 0 */
                /* Rewind to start, return zero rows */
                DoPortalRewind(portal);
                return PortalRunSelect(portal, true, 0L, dest);
            }
            break;
        case FETCH_RELATIVE:
            if (count > 0)
            {
                /*
                 * Definition: advance count-1 rows, return next row (if any).
                 */
                if (count > 1)
                    PortalRunSelect(portal, true, count - 1, None_Receiver);
                return PortalRunSelect(portal, true, 1L, dest);
            }
            else if (count < 0)
            {
                /*
                 * Definition: back up abs(count)-1 rows, return prior row (if
                 * any).
                 */
                if (count < -1)
                    PortalRunSelect(portal, false, -count - 1, None_Receiver);
                return PortalRunSelect(portal, false, 1L, dest);
            }
            else
            {
                /* count == 0 */
                /* Same as FETCH FORWARD 0, so fall out of switch */
                fdirection = FETCH_FORWARD;
            }
            break;
        default:
            elog(ERROR, "bogus direction");
            break;
    }

    /*
     * Get here with fdirection == FETCH_FORWARD or FETCH_BACKWARD, and count
     * >= 0.
     */
    forward = (fdirection == FETCH_FORWARD);

    /*
     * Zero count means to re-fetch the current row, if any (per SQL)
     */
    if (count == 0)
    {
        bool        on_row;

        /* Are we sitting on a row? */
        on_row = (!portal->atStart && !portal->atEnd);

        if (dest->mydest == DestNone)
        {
            /* MOVE 0 returns 0/1 based on if FETCH 0 would return a row */
            return on_row ? 1 : 0;
        }
        else
        {
            /*
             * If we are sitting on a row, back up one so we can re-fetch it.
             * If we are not sitting on a row, we still have to start up and
             * shut down the executor so that the destination is initialized
             * and shut down correctly; so keep going.  To PortalRunSelect,
             * count == 0 means we will retrieve no row.
             */
            if (on_row)
            {
                PortalRunSelect(portal, false, 1L, None_Receiver);
                /* Set up to fetch one row forward */
                count = 1;
                forward = true;
            }
        }
    }

    /*
     * Optimize MOVE BACKWARD ALL into a Rewind.
     */
    if (!forward && count == FETCH_ALL && dest->mydest == DestNone)
    {
        uint64        result = portal->portalPos;

        if (result > 0 && !portal->atEnd)
            result--;
        DoPortalRewind(portal);
        return result;
    }

    return PortalRunSelect(portal, forward, count, dest);
}

/*
 * DoPortalRewind - rewind a Portal to starting point
 */
static void
DoPortalRewind(Portal portal)
{
    QueryDesc  *queryDesc;

    /* Rewind holdStore, if we have one */
    if (portal->holdStore)
    {
        MemoryContext oldcontext;

        oldcontext = MemoryContextSwitchTo(portal->holdContext);
        tuplestore_rescan(portal->holdStore);
        MemoryContextSwitchTo(oldcontext);
    }

    /* Rewind executor, if active */
    queryDesc = PortalGetQueryDesc(portal);
    if (queryDesc)
    {
        PushActiveSnapshot(queryDesc->snapshot);
        ExecutorRewind(queryDesc);
        PopActiveSnapshot();
    }

    portal->atStart = true;
    portal->atEnd = false;
    portal->portalPos = 0;
}

#ifdef XCP
/*
 * Execute the specified portal's query and distribute tuples to consumers.
 * Returs 1 if portal should keep producing, 0 if all consumers have enough
 * rows in the buffers to pause producing temporarily, -1 if the query is
 * completed.
 */
int
AdvanceProducingPortal(Portal portal, bool can_wait)
{// #lizard forgives
    Portal        saveActivePortal;
    ResourceOwner saveResourceOwner;
    MemoryContext savePortalContext;
    MemoryContext oldContext;
    QueryDesc  *queryDesc;
    SharedQueue squeue;
    DestReceiver *treceiver;
    int            result;

    queryDesc = PortalGetQueryDesc(portal);
    squeue = queryDesc->squeue;

    Assert(queryDesc);
    /* Make sure the portal is producing */
    Assert(squeue && queryDesc->myindex == -1);
    /* Make sure there is proper receiver */
    Assert(queryDesc->dest && queryDesc->dest->mydest == DestProducer);

    /*
     * Set up global portal context pointers.
     */
    saveActivePortal = ActivePortal;
    saveResourceOwner = CurrentResourceOwner;
    savePortalContext = PortalContext;
    PG_TRY();
    {
        ActivePortal = portal;
        CurrentResourceOwner = portal->resowner;
        PortalContext = PortalGetHeapMemory(portal);

        oldContext = MemoryContextSwitchTo(PortalGetHeapMemory(portal));

        /*
         * That is the first pass thru if the hold store is not initialized yet,
         * Need to initialize stuff.
         */
        if (portal->holdStore == NULL && portal->status != PORTAL_FAILED)
        {
            int idx PG_USED_FOR_ASSERTS_ONLY;
            char storename[64];

            PortalCreateProducerStore(portal);
            treceiver = CreateDestReceiver(DestTuplestore);
            SetTuplestoreDestReceiverParams(treceiver,
                                            portal->holdStore,
                                            portal->holdContext,
                                            false);
            SetSelfConsumerDestReceiver(queryDesc->dest, treceiver);
            SetProducerTempMemory(queryDesc->dest, portal->tmpContext);
            snprintf(storename, 64, "%s producer store", portal->name);
            tuplestore_collect_stat(portal->holdStore, storename);
            /*
             * Tuplestore does not clear eof flag on the active read pointer,
             * causing the store is always in EOF state once reached when
             * there is a single read pointer. We do not want behavior like this
             * and workaround by using secondary read pointer.
             * Primary read pointer (0) is active when we are writing to
             * the tuple store, secondary read pointer is for reading, and its
             * eof flag is cleared if a tuple is written to the store.
             * We know the extra read pointer has index 1, so do not store it.
             */
            idx = tuplestore_alloc_read_pointer(portal->holdStore, 0);
            Assert(idx == 1);
        }

        if (queryDesc->estate && !queryDesc->estate->es_finished &&
                portal->status != PORTAL_FAILED)
        {
            /*
             * If the portal's hold store has tuples available for read and
             * all consumer queues are not empty we skip advancing the portal
             * (pause it) to prevent buffering too many rows at the producer.
             * NB just created portal store would not be in EOF state, but in
             * this case consumer queues will be empty and do not allow
             * erroneous pause. After the first call to AdvanceProducingPortal
             * portal will try to read the hold store and EOF flag will be set
             * correctly.
             */
            tuplestore_select_read_pointer(portal->holdStore, 1);
            if (!tuplestore_ateof(portal->holdStore) &&
                    SharedQueueCanPause(squeue))
                result = 0;
            else
                result = 1;
            tuplestore_select_read_pointer(portal->holdStore, 0);

            if (result)
            {
                /* Execute query and dispatch tuples via dest receiver */
#define PRODUCE_TUPLES 100
                PushActiveSnapshot(queryDesc->snapshot);

                if (g_UseDataPump)
                {
                    /* get all tuples */
                    ExecutorRun(queryDesc, ForwardScanDirection, 0,
                            portal->run_once);
                }
                else
                {
                    ExecutorRun(queryDesc, ForwardScanDirection, PRODUCE_TUPLES,
                            portal->run_once);
                }
                PopActiveSnapshot();

                if (g_UseDataPump)
                {
                    ExecutorFinish(queryDesc);
                }
                else
                {
                    if (queryDesc->estate->es_processed < PRODUCE_TUPLES)
                    {
                        /*
                         * Finish the executor, but we may still have some tuples
                         * in the local storages.
                         * We should keep trying pushing them into the squeue, so do not
                         * remove the portal from the list of producers.
                         */
                        ExecutorFinish(queryDesc);
                    }
                }
            }
        }

        /* Try to dump local tuplestores */
        if ((queryDesc->estate == NULL || queryDesc->estate->es_finished) &&
                ProducerReceiverPushBuffers(queryDesc->dest))
        {
            if (can_wait && queryDesc->estate == NULL)
            {
                (*queryDesc->dest->rDestroy) (queryDesc->dest);
                queryDesc->dest = NULL;
                portal->queryDesc = NULL;
                squeue = NULL;

                removeProducingPortal(portal);
                FreeQueryDesc(queryDesc);

                /*
                 * Current context is the portal context, which is going
                 * to be deleted
                 */
                MemoryContextSwitchTo(TopTransactionContext);

                ActivePortal = saveActivePortal;
                CurrentResourceOwner = saveResourceOwner;
                PortalContext = savePortalContext;

                if (portal->resowner)
                {
                    bool        isCommit = (portal->status != PORTAL_FAILED);

                    ResourceOwnerRelease(portal->resowner,
                                         RESOURCE_RELEASE_BEFORE_LOCKS,
                                         isCommit, false);
                    ResourceOwnerRelease(portal->resowner,
                                         RESOURCE_RELEASE_LOCKS,
                                         isCommit, false);
                    ResourceOwnerRelease(portal->resowner,
                                         RESOURCE_RELEASE_AFTER_LOCKS,
                                         isCommit, false);
                    ResourceOwnerDelete(portal->resowner);
                }
                portal->resowner = NULL;

                /*
                 * Delete tuplestore if present.  We should do this even under error
                 * conditions; since the tuplestore would have been using cross-
                 * transaction storage, its temp files need to be explicitly deleted.
                 */
                if (portal->holdStore)
                {
                    MemoryContext oldcontext;

                    oldcontext = MemoryContextSwitchTo(portal->holdContext);
                    tuplestore_end(portal->holdStore);
                    MemoryContextSwitchTo(oldcontext);
                    portal->holdStore = NULL;
                }

                /* delete tuplestore storage, if any */
                if (portal->holdContext)
                    MemoryContextDelete(portal->holdContext);

                /* release subsidiary storage */
                MemoryContextDelete(PortalGetHeapMemory(portal));

                /* release portal struct (it's in PortalMemory) */
                pfree(portal);
            }
            /* report portal is not producing */
            result = -1;
        }
        else
        {
            result = SharedQueueCanPause(queryDesc->squeue) ? 0 : 1;
        }
    }
    PG_CATCH();
    {
        /* Uncaught error while executing portal: mark it dead */
        portal->status = PORTAL_FAILED;
        /*
         * Reset producer to allow consumers to finish, so receiving node will
         * handle the error.
         */
        if (squeue)
            SharedQueueReset(squeue, -1);

        /* Restore global vars and propagate error */
        ActivePortal = saveActivePortal;
        CurrentResourceOwner = saveResourceOwner;
        PortalContext = savePortalContext;

        PG_RE_THROW();
    }
    PG_END_TRY();

    MemoryContextSwitchTo(oldContext);

    ActivePortal = saveActivePortal;
    CurrentResourceOwner = saveResourceOwner;
    PortalContext = savePortalContext;

    return result;
}


/*
 * Iterate over producing portal, determine already closed, and clean them up,
 * waiting while consumers finish their work. Closed producers should be
 * cleaned up and resources are released before proceeding with handling of
 * next request.
 */
void
cleanupClosedProducers(void)
{
    ListCell   *lc = list_head(getProducingPortals());
    while (lc)
    {
        Portal p = (Portal) lfirst(lc);
        QueryDesc  *queryDesc = PortalGetQueryDesc(p);
        SharedQueue squeue = queryDesc->squeue;

        /*
         * Get next already, because next call may remove cell from
         * the list and invalidate next reference
         */
        lc = lnext(lc);

        /* When portal is closed executor state is not set */
        if (queryDesc->estate == NULL)
        {
            /*
             * Set up global portal context pointers.
             */
            Portal        saveActivePortal = ActivePortal;
            ResourceOwner saveResourceOwner = CurrentResourceOwner;
            MemoryContext savePortalContext = PortalContext;

            PG_TRY();
            {
                MemoryContext oldContext;
                ActivePortal = p;
                CurrentResourceOwner = p->resowner;
                PortalContext = PortalGetHeapMemory(p);

                oldContext = MemoryContextSwitchTo(PortalGetHeapMemory(p));

                (*queryDesc->dest->rDestroy) (queryDesc->dest);
                queryDesc->dest = NULL;
                p->queryDesc = NULL;
                squeue = NULL;

                removeProducingPortal(p);
                FreeQueryDesc(queryDesc);

                /*
                 * Current context is the portal context, which is going
                 * to be deleted
                 */
                MemoryContextSwitchTo(TopTransactionContext);

                ActivePortal = saveActivePortal;
                CurrentResourceOwner = saveResourceOwner;
                PortalContext = savePortalContext;

                if (p->resowner)
                {
                    bool        isCommit = (p->status != PORTAL_FAILED);

                    ResourceOwnerRelease(p->resowner,
                                         RESOURCE_RELEASE_BEFORE_LOCKS,
                                         isCommit, false);
                    ResourceOwnerRelease(p->resowner,
                                         RESOURCE_RELEASE_LOCKS,
                                         isCommit, false);
                    ResourceOwnerRelease(p->resowner,
                                         RESOURCE_RELEASE_AFTER_LOCKS,
                                         isCommit, false);
                    ResourceOwnerDelete(p->resowner);
                }
                p->resowner = NULL;

                /*
                 * Delete tuplestore if present.  We should do this even under error
                 * conditions; since the tuplestore would have been using cross-
                 * transaction storage, its temp files need to be explicitly deleted.
                 */
                if (p->holdStore)
                {
                    MemoryContext oldcontext;

                    oldcontext = MemoryContextSwitchTo(p->holdContext);
                    tuplestore_end(p->holdStore);
                    MemoryContextSwitchTo(oldcontext);
                    p->holdStore = NULL;
                }

                /* delete tuplestore storage, if any */
                if (p->holdContext)
                    MemoryContextDelete(p->holdContext);

                /* release subsidiary storage */
                MemoryContextDelete(PortalGetHeapMemory(p));

                /* release portal struct (it's in PortalMemory) */
                pfree(p);

                MemoryContextSwitchTo(oldContext);
            }
            PG_CATCH();
            {
                /* Uncaught error while executing portal: mark it dead */
                p->status = PORTAL_FAILED;
                /*
                 * Reset producer to allow consumers to finish, so receiving node will
                 * handle the error.
                 */
                if (squeue)
                    SharedQueueReset(squeue, -1);

                /* Restore global vars and propagate error */
                ActivePortal = saveActivePortal;
                CurrentResourceOwner = saveResourceOwner;
                PortalContext = savePortalContext;

                PG_RE_THROW();
            }
            PG_END_TRY();

            ActivePortal = saveActivePortal;
            CurrentResourceOwner = saveResourceOwner;
            PortalContext = savePortalContext;
        }
    }
}
#endif

#ifdef __AUDIT__
bool PortalGetQueryInfo(Portal portal, Query ** parseTree, const char ** queryString)
{// #lizard forgives
    ListCell * l = NULL;

    if (portal == NULL ||
        parseTree == NULL ||
        queryString == NULL)
    {
        return false;
    }

    /* 00. try get QueryInfo from PlannedStmt only */
    foreach(l, portal->stmts)
    {
        PlannedStmt * pstmt = (PlannedStmt *) lfirst(l);

        if (IsA(pstmt, PlannedStmt) && 
            pstmt->parseTree != NULL &&
            pstmt->queryString != NULL)
        {
            *parseTree = pstmt->parseTree;
            *queryString = pstmt->queryString;
            return true;
        }
    }

    /* 01. try get QueryInfo from PlannedStmt and QueryString from Portal*/
    if (portal->sourceText != NULL)
    {
        *queryString = portal->sourceText;

        foreach(l, portal->stmts)
        {
            PlannedStmt * pstmt = (PlannedStmt *) lfirst(l);

            if (IsA(pstmt, PlannedStmt) && 
                pstmt->parseTree != NULL)
            {
                *parseTree = pstmt->parseTree;
                return true;
            }
        }
    }

    return false;
}
#endif

#ifdef __TBASE__
static void
GetGtmInfoFromUserCmd(Node* stmt)
{// #lizard forgives
    ListCell   *option  = NULL;
    List       *options = NULL;

    /* 
     * If process alter gtm node OR create gtm node command, we set NewGtmHost and NewGtmPort here.
     * Fucntion GetTransactionSnapshot later will try to connect gtm using this gtm info. 
     */
    if ((IsA(stmt, AlterNodeStmt) && (PGXC_NODE_GTM == ((AlterNodeStmt*)stmt)->node_type)) ||
         (IsA(stmt, CreateNodeStmt) && (PGXC_NODE_GTM == ((CreateNodeStmt*)stmt)->node_type)))
    {
        if (NewGtmHost)
        {
            free(NewGtmHost);
            NewGtmHost = NULL;
        }

        switch (nodeTag(stmt))
        {
            case T_CreateNodeStmt:
                options = ((CreateNodeStmt*)stmt)->options;
                break;
            case T_AlterNodeStmt:
                options = ((AlterNodeStmt*)stmt)->options;
                break;
            default:
                elog(ERROR, "unsupported node type: %d", (int) nodeTag(stmt));
                break;
        }

        foreach(option, options)
        {
            DefElem    *defel = (DefElem *) lfirst(option);
            if (strcmp(defel->defname, "host") == 0)
            {
                NewGtmHost = strdup(defGetString(defel));
            }
            else if (strcmp(defel->defname, "port") == 0)
            {
                NewGtmPort = defGetTypeLength(defel);

                if (NewGtmPort < 1 || NewGtmPort > 65535)
                    ereport(ERROR,
                            (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                             errmsg("port value is out of range")));
            }
        }
    }
}
static bool
contain_remote_query(Node *node, bool *remote_subplan)
{// #lizard forgives
    Node *plan = node;

    if (!plan)
    {
        return false;
    }
    
    if (IsA(node, RemoteQuery))
    {
        return true;
    }
    else if (IsA(node, RemoteSubplan))
    {
        RemoteSubplan *subplan = (RemoteSubplan *)node;

        if (remote_subplan && list_length(subplan->nodeList) > 0)
        {
            *remote_subplan = true;
        }
        
        if (!need_global_snapshot)
        {
            int node_count = list_length(subplan->nodeList);
            
            if (node_count > 1)
            {
                need_global_snapshot = true;
            }
            else if (node_count == 1)
            {
                MemoryContext old = MemoryContextSwitchTo(TopTransactionContext);
                executed_node_list = list_append_unique_int(executed_node_list, linitial_int(subplan->nodeList));
                MemoryContextSwitchTo(old);

                if (list_length(executed_node_list) > 1)
                {
                    need_global_snapshot = true;
                }
            }
        }
    }

    if (IsA(node, SubqueryScan))
    {
        SubqueryScan *subquery = (SubqueryScan *)node;
        plan = (Node *)subquery->subplan;
    }

    if (IsA(plan, Append))
    {
        ListCell *lc;
        Append *append = (Append *)plan;

        foreach(lc, append->appendplans)
        {
            Plan *appendplan = (Plan *)lfirst(lc);

            if (appendplan && contain_remote_query((Node*)appendplan, remote_subplan))
            {
                return true;
            }
        }

        return false;
    }
    else if (IsA(plan, MergeAppend))
    {
        ListCell *lc;
        MergeAppend *mergeappend = (MergeAppend *)plan;

        foreach(lc, mergeappend->mergeplans)
        {
            Plan *mergeappendplan = (Plan *)lfirst(lc);

            if (mergeappendplan && contain_remote_query((Node*)mergeappendplan, remote_subplan))
            {
                return true;
            }
        }

        return false;
    }

    if (outerPlan(plan))
    {
        if (contain_remote_query((Node*)outerPlan(plan), remote_subplan))
        {
            return true;
        }
    }

    if (innerPlan(plan))
    {
        if (contain_remote_query((Node*)innerPlan(plan), remote_subplan))
        {
            return true;
        }
    }
    return false;
}

/*
 * If query meets those conditions:
 *    coordinator
 *    read committed(isolation)
 *    execute RemoteQuery or plan with remotesubplan
 *    no subplans
 *    DML without triggers/returning/cte
 *    SELECT
 * do not get snapshot until execution
 */
static bool
NeedSnapshot(PlannedStmt *plannedstmt)
{// #lizard forgives
    bool result = true;

    if (plannedstmt && IS_PGXC_LOCAL_COORDINATOR && !IsolationUsesXactSnapshot() && !g_set_global_snapshot && !RecoveryInProgress())
    {
        bool remote_subplan = false;
        if (!plannedstmt->subplans && (contain_remote_query((Node*)plannedstmt->planTree, &remote_subplan) || remote_subplan))
        {
            switch(plannedstmt->commandType)
            {
                case CMD_SELECT:
                {
                    result = false;
                    plannedstmt->need_snapshot = true;
                    break;
                }
                case CMD_UPDATE:
                case CMD_INSERT:
                case CMD_DELETE:
                    if (!IsA(plannedstmt->planTree, ModifyTable) && !plannedstmt->hasReturning
                        && !plannedstmt->hasModifyingCTE)
                    {
                        result = false;
                        plannedstmt->need_snapshot = true;
                    }
                    break;
                default:
                    break;
            }
        }
    }

    return result;
}
#endif
