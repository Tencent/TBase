/*-------------------------------------------------------------------------
 *
 * planner.c
 *
 *      Functions for generating a PGXC style plan.
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *      $$
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "access/transam.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_class.h"
#include "catalog/pg_inherits_fn.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "catalog/pgxc_node.h"
#include "commands/prepare.h"
#include "executor/executor.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/tlist.h"
#include "parser/parse_agg.h"
#include "parser/parse_func.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "parser/parse_oper.h"
#include "pgxc/execRemote.h"
#include "pgxc/pgxc.h"
#include "pgxc/locator.h"
#include "pgxc/nodemgr.h"
#include "pgxc/planner.h"
#include "tcop/pquery.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/portal.h"
#include "utils/syscache.h"
#include "utils/numeric.h"
#include "utils/memutils.h"
#include "access/hash.h"
#include "commands/tablecmds.h"
#include "utils/timestamp.h"
#include "utils/date.h"
#ifdef __TBASE__
#include "access/sysattr.h"
#include "catalog/pg_attribute.h"
#include "optimizer/var.h"
#include "access/htup_details.h"
#include "optimizer/planner.h"
#include "optimizer/prep.h"

#endif

static bool contains_temp_tables(List *rtable);
static PlannedStmt *pgxc_FQS_planner(Query *query, int cursorOptions,
                                     ParamListInfo boundParams);
static RemoteQuery *pgxc_FQS_create_remote_plan(Query *query,
                                                ExecNodes *exec_nodes,
                                                bool is_exec_direct);
static CombineType get_plan_combine_type(CmdType commandType, char baselocatortype);

#ifdef XCP
/*
 * AddRemoteQueryNode
 *
 * Add a Remote Query node to launch on Datanodes.
 * This can only be done for a query a Top Level to avoid
 * duplicated queries on Datanodes.
 */
List *
AddRemoteQueryNode(List *stmts, const char *queryString, RemoteQueryExecType remoteExecType)
{
    List *result = stmts;

    /* If node is appplied on EXEC_ON_NONE, simply return the list unchanged */
    if (remoteExecType == EXEC_ON_NONE)
        return result;

    /* Only a remote Coordinator is allowed to send a query to backend nodes */
    if (remoteExecType == EXEC_ON_CURRENT ||
            (IS_PGXC_LOCAL_COORDINATOR))
    {
        RemoteQuery *step = makeNode(RemoteQuery);
        step->combine_type = COMBINE_TYPE_SAME;
        step->sql_statement = (char *) queryString;
        step->exec_type = remoteExecType;
        result = lappend(result, step);
    }

    return result;
}
#endif


/*
 * pgxc_direct_planner
 * The routine tries to see if the statement can be completely evaluated on the
 * datanodes. In such cases coordinator is not needed to evaluate the statement,
 * and just acts as a proxy. A statement can be completely shipped to the remote
 * node if every row of the result can be evaluated on a single datanode.
 * For example:
 *
 * Only EXECUTE DIRECT statements are sent directly as of now
 */
PlannedStmt *
pgxc_direct_planner(Query *query, int cursorOptions, ParamListInfo boundParams)
{
    PlannedStmt *result;
    RemoteQuery *query_step = NULL;

    /* build the PlannedStmt result */
    result = makeNode(PlannedStmt);

    /* Try and set what we can */
    result->commandType = query->commandType;
    result->canSetTag = query->canSetTag;
    result->utilityStmt = query->utilityStmt;
    result->rtable = query->rtable;

    /* EXECUTE DIRECT statements have their RemoteQuery node already built when analyzing */
    if (query->utilityStmt
        && IsA(query->utilityStmt, RemoteQuery))
    {
        RemoteQuery *stmt = (RemoteQuery *) query->utilityStmt;
        if (stmt->exec_direct_type != EXEC_DIRECT_NONE)
        {
            query_step = stmt;
            query->utilityStmt = NULL;
            result->utilityStmt = NULL;
        }
    }

    Assert(query_step);
    /* Optimize multi-node handling */
    query_step->read_only = query->commandType == CMD_SELECT;

    result->planTree = (Plan *) query_step;

    query_step->scan.plan.targetlist = query->targetList;

    return result;
}

/*
 * Returns true if at least one temporary table is in use
 * in query (and its subqueries)
 */
static bool
contains_temp_tables(List *rtable)
{
    ListCell *item;

    foreach(item, rtable)
    {
        RangeTblEntry *rte = (RangeTblEntry *) lfirst(item);

        if (rte->rtekind == RTE_RELATION)
        {
            if (IsTempTable(rte->relid))
                return true;
        }
        else if (rte->rtekind == RTE_SUBQUERY &&
                 contains_temp_tables(rte->subquery->rtable))
            return true;
    }

    return false;
}

/*
 * get_plan_combine_type - determine combine type
 *
 * COMBINE_TYPE_SAME - for replicated updates
 * COMBINE_TYPE_SUM - for hash and round robin updates
 * COMBINE_TYPE_NONE - for operations where row_count is not applicable
 *
 * return NULL if it is not safe to be done in a single step.
 */
static CombineType
get_plan_combine_type(CmdType commandType, char baselocatortype)
{

    switch (commandType)
    {
        case CMD_INSERT:
        case CMD_UPDATE:
        case CMD_DELETE:
            return baselocatortype == LOCATOR_TYPE_REPLICATED ?
                    COMBINE_TYPE_SAME : COMBINE_TYPE_SUM;

        default:
            return COMBINE_TYPE_NONE;
    }
    /* quiet compiler warning */
    return COMBINE_TYPE_NONE;
}


/*
 * Build up a QueryPlan to execute on.
 *
 * This functions tries to find out whether
 * 1. The statement can be shipped to the Datanode and Coordinator is needed
 *    only as a proxy - in which case, it creates a single node plan.
 * 2. The statement can be evaluated on the Coordinator completely - thus no
 *    query shipping is involved and standard_planner() is invoked to plan the
 *    statement
 * 3. The statement needs Coordinator as well as Datanode for evaluation -
 *    again we use standard_planner() to plan the statement.
 *
 * The plan generated in either of the above cases is returned.
 */
PlannedStmt *
pgxc_planner(Query *query, int cursorOptions, ParamListInfo boundParams)
{
    PlannedStmt *result;

    /* see if can ship the query completely */
    result = pgxc_FQS_planner(query, cursorOptions, boundParams);
    if (result)
        return result;

    /* we need Coordinator for evaluation, invoke standard planner */
    result = standard_planner(query, cursorOptions, boundParams);
    return result;
}

/*
 * pgxc_FQS_planner
 * The routine tries to see if the statement can be completely evaluated on the
 * Datanodes. In such cases Coordinator is not needed to evaluate the statement,
 * and just acts as a proxy. A statement can be completely shipped to the remote
 * node if every row of the result can be evaluated on a single Datanode.
 * For example:
 *
 * 1. SELECT * FROM tab1; where tab1 is a distributed table - Every row of the
 * result set can be evaluated at a single Datanode. Hence this statement is
 * completely shippable even though many Datanodes are involved in evaluating
 * complete result set. In such case Coordinator will be able to gather rows
 * arisign from individual Datanodes and proxy the result to the client.
 *
 * 2. SELECT count(*) FROM tab1; where tab1 is a distributed table - there is
 * only one row in the result but it needs input from all the Datanodes. Hence
 * this is not completely shippable.
 *
 * 3. SELECT count(*) FROM tab1; where tab1 is replicated table - since result
 * can be obtained from a single Datanode, this is a completely shippable
 * statement.
 *
 * fqs in the name of function is acronym for fast query shipping.
 */
static PlannedStmt *
pgxc_FQS_planner(Query *query, int cursorOptions, ParamListInfo boundParams)
{// #lizard forgives
    PlannedStmt        *result;
    PlannerGlobal    *glob;
    PlannerInfo        *root;
    ExecNodes        *exec_nodes;
    Plan            *top_plan;
    List            *tlist = query->targetList;

#ifdef __TBASE__
    groupOids = NULL;
#endif

    /* Try by-passing standard planner, if fast query shipping is enabled */
    if (!enable_fast_query_shipping)
        return NULL;

    /* Do not FQS cursor statements that require backward scrolling */
    if (cursorOptions & CURSOR_OPT_SCROLL)
        return NULL;

    /* Do not FQS EXEC DIRECT statements */
    if (query->utilityStmt && IsA(query->utilityStmt, RemoteQuery))
    {
        RemoteQuery *stmt = (RemoteQuery *) query->utilityStmt;
        if (stmt->exec_direct_type != EXEC_DIRECT_NONE)
            return NULL;
    }

    /*
     * If the query can not be or need not be shipped to the Datanodes, don't
     * create any plan here. standard_planner() will take care of it.
     */
    exec_nodes = pgxc_is_query_shippable(query, 0);
    if (exec_nodes == NULL)
        return NULL;

    glob = makeNode(PlannerGlobal);
    glob->boundParams = boundParams;
    /* Create a PlannerInfo data structure, usually it is done for a subquery */
    root = makeNode(PlannerInfo);
    root->parse = query;
    root->glob = glob;
    root->query_level = 1;
    root->planner_cxt = CurrentMemoryContext;
    preprocess_rowmarks(root);
    tlist = preprocess_targetlist(root, tlist);

    /*
     * We decided to ship the query to the Datanode/s, create a RemoteQuery node
     * for the same.
     */
    top_plan = (Plan *)pgxc_FQS_create_remote_plan(query, exec_nodes, false);
    top_plan->targetlist = tlist;
    /*
     * Just before creating the PlannedStmt, do some final cleanup
     * We need to save plan dependencies, so that dropping objects will
     * invalidate the cached plan if it depends on those objects. Table
     * dependencies are available in glob->relationOids and all other
     * dependencies are in glob->invalItems. These fields can be retrieved
     * through set_plan_references().
     */
    top_plan = set_plan_references(root, top_plan);

    /* build the PlannedStmt result */
    result = makeNode(PlannedStmt);
    /* Try and set what we can, rest must have been zeroed out by makeNode() */
    result->commandType = query->commandType;
    result->canSetTag = query->canSetTag;
    result->utilityStmt = query->utilityStmt;

    /* Set result relations */
    if (query->commandType != CMD_SELECT)
        result->resultRelations = list_make1_int(query->resultRelation);
    result->planTree = top_plan;
    result->rtable = query->rtable;
    result->queryId = query->queryId;
    result->relationOids = glob->relationOids;
    result->invalItems = glob->invalItems;
    result->rowMarks = glob->finalrowmarks;
	result->hasReturning = (query->returningList != NULL);

    return result;
}

static RemoteQuery *
pgxc_FQS_create_remote_plan(Query *query, ExecNodes *exec_nodes, bool is_exec_direct)
{
    RemoteQuery *query_step;
    StringInfoData buf;
    RangeTblEntry    *dummy_rte;

    /* EXECUTE DIRECT statements have their RemoteQuery node already built when analyzing */
    if (is_exec_direct)
    {
        Assert(IsA(query->utilityStmt, RemoteQuery));
        query_step = (RemoteQuery *)query->utilityStmt;
        query->utilityStmt = NULL;
    }
    else
    {
        query_step = makeNode(RemoteQuery);
        query_step->combine_type = COMBINE_TYPE_NONE;
        query_step->exec_type = EXEC_ON_DATANODES;
        query_step->exec_direct_type = EXEC_DIRECT_NONE;
        query_step->exec_nodes = exec_nodes;
    }

    Assert(query_step->exec_nodes);

    /* Deparse query tree to get step query. */
    if (query_step->sql_statement == NULL)
    {
        initStringInfo(&buf);
        /*
         * We always finalise aggregates on datanodes for FQS.
         * Use the expressions for ORDER BY or GROUP BY clauses.
         */
        deparse_query(query, &buf, NIL, true, false);
        query_step->sql_statement = pstrdup(buf.data);
        pfree(buf.data);
    }

	if (query_step->exec_nodes &&
		query_step->exec_nodes->need_rewrite &&
		query->commandType == CMD_INSERT)
	{
		query_step->forDeparse = copyObject(query);
	}

    /* Optimize multi-node handling */
    query_step->read_only = (query->commandType == CMD_SELECT && !query->hasForUpdate);
    query_step->has_row_marks = query->hasForUpdate;

    /* Check if temporary tables are in use in query */
    /* PGXC_FQS_TODO: scanning the rtable again for the queries should not be
     * needed. We should be able to find out if the query has a temporary object
     * while finding nodes for the objects. But there is no way we can convey
     * that information here. Till such a connection is available, this is it.
     */
    if (contains_temp_tables(query->rtable))
        query_step->is_temp = true;

    /*
     * We need to evaluate some expressions like the ExecNodes->en_expr at
     * Coordinator, prepare those for evaluation. Ideally we should call
     * preprocess_expression, but it needs PlannerInfo structure for the same
     */
    fix_opfuncids((Node *)(query_step->exec_nodes->en_expr));
    /*
     * PGXCTODO
     * When Postgres runs insert into t (a) values (1); against table
     * defined as create table t (a int, b int); the plan is looking
     * like insert into t (a,b) values (1,null);
     * Later executor is verifying plan, to make sure table has not
     * been altered since plan has been created and comparing table
     * definition with plan target list and output error if they do
     * not match.
     * I could not find better way to generate targetList for pgxc plan
     * then call standard planner and take targetList from the plan
     * generated by Postgres.
     */
    query_step->combine_type = get_plan_combine_type(
                query->commandType, query_step->exec_nodes->baselocatortype);

    /*
     * Create a dummy RTE for the remote query being created. Append the dummy
     * range table entry to the range table. Note that this modifies the master
     * copy the caller passed us, otherwise e.g EXPLAIN VERBOSE will fail to
     * find the rte the Vars built below refer to. Also create the tuple
     * descriptor for the result of this query from the base_tlist (targetlist
     * we used to generate the remote node query).
     */
    dummy_rte = makeNode(RangeTblEntry);
    dummy_rte->rtekind = RTE_REMOTE_DUMMY;
    /* Use a dummy relname... */
    if (is_exec_direct)
        dummy_rte->relname = "__EXECUTE_DIRECT__";
    else
        dummy_rte->relname       = "__REMOTE_FQS_QUERY__";
    dummy_rte->eref           = makeAlias("__REMOTE_FQS_QUERY__", NIL);
    /* Rest will be zeroed out in makeNode() */

    query->rtable = lappend(query->rtable, dummy_rte);
    query_step->scan.scanrelid     = list_length(query->rtable);
    query_step->scan.plan.targetlist = query->targetList;
    query_step->base_tlist = query->targetList;

    return query_step;
}
#ifdef __TBASE__
RangeTblEntry *
make_dummy_remote_rte(char *relname, Alias *alias)
{
    RangeTblEntry *dummy_rte = makeNode(RangeTblEntry);
    dummy_rte->rtekind = RTE_REMOTE_DUMMY;

    /* use a dummy relname... */
    dummy_rte->relname         = relname;
    dummy_rte->eref             = alias;

    return dummy_rte;
}

/*
 * pgxc_make_param
 *
 * Helper function to make a parameter
 */
static Param *
pgxc_make_param(int param_num, Oid param_type)
{
    Param    *param;

    param = makeNode(Param);
    /* Parameter values are supplied from outside the plan */
    param->paramkind = PARAM_EXTERN;
    /* Parameters are numbered from 1 to n */
    param->paramid = param_num;
    param->paramtype = param_type;
    /* The following members are not required for deparsing */
    param->paramtypmod = -1;
    param->paramcollid = InvalidOid;
    param->location = -1;

    return param;
}
/*
 * pgxc_add_param_as_tle
 *
 * Helper function to add a parameter to the target list of the query
 */
static void
pgxc_add_param_as_tle(Query *query, int param_num, Oid param_type,
                        char *resname)
{
    Param        *param;
    TargetEntry    *res_tle;

    param = pgxc_make_param(param_num, param_type);
    res_tle = makeTargetEntry((Expr *)param, param_num, resname, false);
    query->targetList = lappend(query->targetList, res_tle);
}

/*
 * pgxc_dml_add_qual_to_query
 *
 * This function adds a qual of the form sys_col_name = $? to a query
 * It is required while adding quals like ctid = $2 or xc_node_id = $3 to DMLs
 *
 * Parameters Description
 * query         : The qual will be added to this query
 * param_num     : The parameter number to use while adding the qual
 * sys_col_attno : Which system column to use for LHS of the = operator
 *               : SelfItemPointerAttributeNumber for ctid
 *               : XC_NodeIdAttributeNumber for xc_node_id
 * varno         : Index of this system column's relation in range table
 */
static void
pgxc_dml_add_qual_to_query(Query *query, int param_num,
                            AttrNumber sys_col_attno, Index varno, Oid param_type, bool explicit_cast)
{
    Var            *lhs_var;
    Expr        *qual;
    Param        *rhs_param;

    /* Make a parameter expr for RHS of the = operator */
    rhs_param = pgxc_make_param(param_num, param_type);

    rhs_param->explicit_cast = explicit_cast;

    /* Make a system column ref expr for LHS of the = operator */
    lhs_var = makeVar(varno, sys_col_attno, param_type, -1, InvalidOid, 0);

    /* Make the new qual sys_column_name = $? */
    qual = make_op(NULL, list_make1(makeString("=")), (Node *)lhs_var,
                                    (Node *)rhs_param, NULL, -1);

    /* Add the qual to the qual list */
    query->jointree->quals = (Node *)lappend((List *)query->jointree->quals,
                                        (Node *)qual);
}

/*
 * pgxc_rqplan_build_statement
 * Given a RemoteQuery plan generate the SQL statement from Query structure
 * inside it.
 */
static void
pgxc_rqplan_build_statement(RemoteQuery *rqplan)
{
    StringInfo sql = makeStringInfo();
    deparse_query(rqplan->remote_query, sql, NULL, rqplan->rq_finalise_aggs,
                    rqplan->rq_sortgroup_colno);
    if (rqplan->sql_statement)
        pfree(rqplan->sql_statement);
    rqplan->sql_statement = sql->data;
    return;
}

#if 0
/*
 * pgxc_find_unique_index finds either primary key or unique index
 * defined for the passed relation.
 * Returns the number of columns in the primary key or unique index
 * ZERO means no primary key or unique index is defined.
 * The column attributes of the primary key or unique index are returned
 * in the passed indexed_col_numbers.
 * The function allocates space for indexed_col_numbers, the caller is
 * supposed to free it after use.
 */
static int
pgxc_find_unique_index(Oid relid, int16 **indexed_col_numbers)
{
    HeapTuple        indexTuple = NULL;
    HeapTuple        indexUnique = NULL;
    Form_pg_index    indexStruct;
    ListCell        *item;
    int                i;

    /* Get necessary information about relation */
    Relation rel = relation_open(relid, AccessShareLock);

    foreach(item, RelationGetIndexList(rel))
    {
        Oid            indexoid = lfirst_oid(item);

        indexTuple = SearchSysCache1(INDEXRELID,
                                    ObjectIdGetDatum(indexoid));
        if (!HeapTupleIsValid(indexTuple))
            elog(ERROR, "cache lookup failed for index %u", indexoid);

        indexStruct = (Form_pg_index) GETSTRUCT(indexTuple);

        if (indexStruct->indisprimary)
        {
            indexUnique = indexTuple;
            ReleaseSysCache(indexTuple);
            break;
        }

        /* In case we do not have a primary key, use a unique index */
        if (indexStruct->indisunique)
        {
            indexUnique = indexTuple;
        }

        ReleaseSysCache(indexTuple);
    }
    relation_close(rel, AccessShareLock);

    if (!indexUnique)
        return 0;

    indexStruct = (Form_pg_index) GETSTRUCT(indexUnique);

    *indexed_col_numbers = palloc0(indexStruct->indnatts * sizeof(int16));

    /*
     * Now get the list of PK attributes from the indkey definition (we
     * assume a primary key cannot have expressional elements)
     */
    for (i = 0; i < indexStruct->indnatts; i++)
    {
        (*indexed_col_numbers)[i] = indexStruct->indkey.values[i];
    }
    return indexStruct->indnatts;
}

/*
 * is_pk_being_changed determines whether the query is changing primary key
 * or unique index.
 * The attributes of the primary key / unique index and their count is
 * passed to the function along with the query
 * Returns true if the query is changing the primary key / unique index
 * The function takes care of the fact that just having the primary key
 * in set caluse does not mean that it is being changed unless the RHS
 * is different that the LHS of the set caluse i.e. set pk = pk
 * is taken as no change to the column
 */
static bool
is_pk_being_changed(const Query *query, int16 *indexed_col_numbers, int count)
{// #lizard forgives
    ListCell *lc;
    int i;

    if (query == NULL || query->rtable == NULL || indexed_col_numbers == NULL)
        return false;

    if (query->commandType != CMD_UPDATE)
        return false;

    for (i = 0; i < count; i++)
    {
        foreach(lc, query->targetList)
        {
            TargetEntry *tle = (TargetEntry *) lfirst(lc);

            /* Nothing to do for a junk entry */
            if (tle->resjunk)
                continue;

            /*
             * The TargetEntry::resno is the same as the attribute number
             * of the column being updated, if the attribute number of the
             * column being updated and the attribute of the primary key of
             * the table is same means this set clause entry is updating the
             * primary key column of the target table.
             */
            if (indexed_col_numbers[i] == tle->resno)
            {
                Var *v;
                /*
                 * Although the set caluse contains pk column, but if it is
                 * not being modified, we can use pk for updating the row
                 */
                if (!IsA(tle->expr, Var))
                    return true;

                v = (Var *)tle->expr;
                if (v->varno == query->resultRelation &&
                    v->varattno == tle->resno)
                {
                    return false;
                }
                else
                {
                    return true;
                }
            }
        }
    }
    return false;
}
#endif
/*
 * pgxc_add_returning_list
 *
 * This function adds RETURNING var list to the passed remote query node
 * It first pulls all vars from the returning list.
 * It then iterates over all the vars and picks all belonging
 * to the remote relation. The refined vars list is then copied in plan target
 * list as well as base_tlist of remote query.
 *
 * Parameters:
 * rq             : The remote query node to whom the returning
 *                  list is to be added
 * ret_list       : The returning list
 * rel_index      : The index of the concerned relation in RTE list
 */
void
pgxc_add_returning_list(RemoteQuery *rq, List *ret_list, int rel_index)
{
    List        *shipableReturningList = NULL;
    List        *varlist;
    ListCell    *lc;

    /* Do we have to add a returning clause or not? */
    if (ret_list == NULL)
        return;

    /*
     * Returning lists cannot contain aggregates and
     * we are not supporting place holders for now
     */
    varlist = pull_var_clause((Node *)ret_list, 0);

    /*
     * For every entry in the returning list if the entry belongs to the
     * same table as the one whose index is passed then add it to the
     * shippable returning list
     */
    foreach (lc, varlist)
    {
        Var *var = lfirst(lc);

        if (var->varno == rel_index)
            shipableReturningList = add_to_flat_tlist(shipableReturningList,
                                                        list_make1(var));
    }

    /*
     * If the user query had RETURNING clause and here we find that
     * none of the items in the returning list are shippable
     * we intend to send RETURNING NULL to the datanodes
     * Otherwise no rows will be returned from the datanodes
     * and no rows will be projected to the upper nodes in the
     * execution tree.
     */
    if ((shipableReturningList == NIL ||
        list_length(shipableReturningList) <= 0) &&
        list_length(ret_list) > 0)
    {
        Expr *null_const = (Expr *)makeNullConst(INT4OID, -1, InvalidOid);

        shipableReturningList = add_to_flat_tlist(shipableReturningList,
                                                list_make1(null_const));
    }

    /*
     * Copy the refined var list in plan target list as well as
     * base_tlist of the remote query node
     */
    rq->scan.plan.targetlist = list_copy(shipableReturningList);
    rq->base_tlist = list_copy(shipableReturningList);
}

/*
  * pgxc_build_upsert_statement
  *
  *  Construct subquery statement for UPSERT executed on coordinator with unshippable triggers.
  *  We separate UPSERT into SELECT, INSERT and UPDATE, INSERT has already been generated
  *  before, now we have to build the SELECT and UPDATE statement.
  */
static void
pgxc_build_upsert_statement(PlannerInfo *root, CmdType cmdtype,
                                    Index resultRelationIndex, RemoteQuery *rqplan,
                                    List *sourceTargetList)
{// #lizard forgives
    ListCell *cell;
    Query *query_to_deparse;
    RangeTblEntry    *res_rel;
    int    col_att = 0;
    Bitmapset  *keyCols;
    Bitmapset  *updatedCols;
    StringInfo sql_select = makeStringInfo();
    StringInfo sql_update = makeStringInfo();
    Relation relation;
    Oid         type;
    int         natts;
    int         attnum;
    Var         *var;
    TargetEntry    *tle;
    const char *attrname;
    RangeTblRef        *target_table_ref;

    /* First construct a reference to an entry in the query's rangetable */
    target_table_ref = makeNode(RangeTblRef);

    /* RangeTblRef::rtindex will be the same as indicated by the caller */
    target_table_ref->rtindex = resultRelationIndex;
    
    /* init query structure */
    query_to_deparse = makeNode(Query);
    query_to_deparse->resultRelation = 0;
    query_to_deparse->rtable = root->parse->rtable;
    query_to_deparse->jointree = makeNode(FromExpr);
    query_to_deparse->jointree->fromlist = lappend(query_to_deparse->jointree->fromlist,
                                                   target_table_ref);

    res_rel = rt_fetch(resultRelationIndex, query_to_deparse->rtable);
    
    /* construct select statement */
    query_to_deparse->commandType = CMD_SELECT;

    rqplan->ss_param_types = (Oid *)palloc(list_length(sourceTargetList) * sizeof(Oid));
    
    /* construct select targetlist and whereclause */
    foreach(cell, sourceTargetList)
    {
        Oid type PG_USED_FOR_ASSERTS_ONLY; 
        TargetEntry    *select_tle;
        
        tle = lfirst(cell);

        col_att++;
        
        /*
         * Make sure the entry in the source target list belongs to the
         * target table of the DML
         */
        if (tle->resorigtbl != 0 && tle->resorigtbl != res_rel->relid)
            continue;

        /* Make sure the column has not been dropped */
        if (get_rte_attribute_is_dropped(res_rel, col_att))
		{
			rqplan->rq_param_types[rqplan->rq_num_params++] = InvalidOid;
            continue;
		}

        type = exprType((Node *) tle->expr);

        var = makeVarFromTargetEntry(resultRelationIndex, tle);

        select_tle = makeTargetEntry((Expr *) var,
                                      tle->resno,
                                     get_attname(res_rel->relid, var->varoattno),
                                      false);

        query_to_deparse->targetList = lappend(query_to_deparse->targetList, select_tle);

        /* keep param type */
        //rqplan->ss_param_types[rqplan->ss_num_params++] = type;
        
        /*
         * Create the param to be used for where caluse (a = $1 and b = $2 ...)
         * and add it to the quals
         */
        //pgxc_dml_add_qual_to_query(query_to_deparse, rqplan->ss_num_params,
        //                           col_att, resultRelationIndex, type, true);
    }

    /* make 'and' whereclause */
    //query_to_deparse->jointree->quals = (Node *)make_andclause(
    //                            (List *)query_to_deparse->jointree->quals);

    /* put ctid and xc_node_id into targetlist */
    var = makeVar(resultRelationIndex,
                  XC_NodeIdAttributeNumber,
                  INT4OID,
                  -1,
                  InvalidOid,
                  0);

    attrname = "xc_node_id";

    rqplan->jf_xc_node_id = list_length(query_to_deparse->targetList) + 1;

    tle = makeTargetEntry((Expr *) var,
                          list_length(query_to_deparse->targetList) + 1,
                          pstrdup(attrname),
                          false);

    query_to_deparse->targetList = lappend(query_to_deparse->targetList, tle);


    var = makeVar(resultRelationIndex,
                  SelfItemPointerAttributeNumber,
                  TIDOID,
                  -1,
                  InvalidOid,
                  0);

    attrname = "ctid";

    rqplan->jf_ctid = list_length(query_to_deparse->targetList) + 1;;

    tle = makeTargetEntry((Expr *) var,
                          list_length(query_to_deparse->targetList) + 1,
                          pstrdup(attrname),
                          false);

    query_to_deparse->targetList = lappend(query_to_deparse->targetList, tle);

    deparse_query(query_to_deparse, sql_select, NULL, rqplan->rq_finalise_aggs,
                    rqplan->rq_sortgroup_colno);
    
    /* add lock tuple for select */
    /*
     * Compute lock mode to use.  If columns that are part of the key have not
     * been modified, then we can use a weaker lock, allowing for better
     * concurrency.
     */
    updatedCols = res_rel->updatedCols;

    relation = heap_open(res_rel->relid, AccessShareLock);
    
    keyCols = RelationGetIndexAttrBitmap(relation,
                                         INDEX_ATTR_BITMAP_KEY);

    heap_close(relation, AccessShareLock);

    if (bms_overlap(keyCols, updatedCols))
        rqplan->forUpadte = true;
        //appendStringInfoString(sql_select, " FOR UPDATE");
    else
        rqplan->forUpadte = false;
        //appendStringInfoString(sql_select, " FOR NO KEY UPDATE");
    
    if (rqplan->sql_select_base)
        pfree(rqplan->sql_select_base);
    rqplan->sql_select_base = sql_select->data;

    
    /* construct update statement */
    query_to_deparse->resultRelation = resultRelationIndex;
    query_to_deparse->targetList = NULL;
    query_to_deparse->commandType = CMD_UPDATE;
    query_to_deparse->jointree = makeNode(FromExpr);
    
    natts = get_relnatts(res_rel->relid);

	/* natts + 1(xc_node_id) + 1(ctid) */
	rqplan->su_param_types = (Oid *)palloc((natts + 2) * sizeof(Oid));

    for (attnum = 1; attnum <= natts; attnum++)
    {
        /* Make sure the column has not been dropped */
        if (get_rte_attribute_is_dropped(res_rel, attnum))
		{
			rqplan->rq_param_types[rqplan->rq_num_params++] = InvalidOid;
            continue;
		}

        type = get_atttype(res_rel->relid, attnum);
        pgxc_add_param_as_tle(query_to_deparse, attnum,
                            type,
                            get_attname(res_rel->relid, attnum));
        /* keep param type */
        rqplan->su_param_types[rqplan->su_num_params++] = type;
    }

    pgxc_dml_add_qual_to_query(query_to_deparse, rqplan->su_num_params + 1,
                SelfItemPointerAttributeNumber, resultRelationIndex, TIDOID, false);

    rqplan->su_param_types[rqplan->su_num_params++] = TIDOID;

    pgxc_dml_add_qual_to_query(query_to_deparse, rqplan->su_num_params + 1,
                    XC_NodeIdAttributeNumber, resultRelationIndex, INT4OID, false);
    
    rqplan->su_param_types[rqplan->su_num_params++] = INT4OID;

    /* make 'and' whereclause */
    query_to_deparse->jointree->quals = (Node *)make_andclause(
                                (List *)query_to_deparse->jointree->quals);

    /* pgxc_add_returning_list copied returning list in base_tlist */
    if (rqplan->base_tlist)
        query_to_deparse->returningList = list_copy(rqplan->base_tlist);

    deparse_query(query_to_deparse, sql_update, NULL, rqplan->rq_finalise_aggs,
                rqplan->rq_sortgroup_colno);

    if (rqplan->sql_update)
        pfree(rqplan->sql_update);
    rqplan->sql_update = sql_update->data;

    rqplan->conflict_cols = bms_copy(root->parse->conflict_cols);
}

/*
 * pgxc_build_dml_statement
 *
 * Construct a Query structure for the query to be fired on the datanodes
 * and deparse it. Fields not set remain memzero'ed as set by makeNode.
 * Following is a description of all members of Query structure
 * when used for deparsing of non FQSed DMLs in XC.
 *
 * querySource        : Can be set to QSRC_ORIGINAL i.e. 0
 * queryId            : Not used in deparsing, can be 0
 * canSetTag        : Not used in deparsing, can be false
 * utilityStmt        : A DML is not a utility statement, keep it NULL
 * resultRelation    : Index of the target relation will be sent by the caller
 * hasAggs            : Our DML won't contain any aggregates in tlist, so false
 * hasWindowFuncs    : Our DML won't contain any window funcs in tlist, so false
 * hasSubLinks        : RemoteQuery does not support subquery, so false
 * hasDistinctOn    : Our DML wont contain any DISTINCT clause, so false
 * hasRecursive        : WITH RECURSIVE wont be specified in our DML, so false
 * hasModifyingCTE    : Our DML will not be in WITH, so false
 * hasForUpdate        : FOR UPDATE/SHARE can be there but not untill we support it
 * cteList            : WITH list will be NULL in our case
 * rtable            : We can set the rtable as being the same as the original query
 * jointree            : In XC we plan non FQSed DML's in such a maner that the
 *                    : DML's to be sent to the datanodes do not contain joins,
 *                    : so the join tree will not contain any thing in fromlist,
 *                    : It will however contain quals and the number of quals
 *                    : will always be fixed to two in case of UPDATE/DELETE &
 *                    : zero in case of an INSERT. The quals will be of the
 *                    : form ctid = $4 or xc_node_id = $5
 * targetList        : For DELETEs it will be NULL
 *                    : For INSERTs it will be a list of params. The number of
 *                    :             params will be the same as the number of
 *                    :             enteries in the source data plan target list
 *                    :             The targetList specifies the VALUES caluse
 *                    :             e.g. INSERT INTO TAB VALUES ($1, $2, ...)
 *                    : For UPDATEs it will be a list of parameters, the number
 *                    :             of parameters will be the same as the number
 *                    :             entries in the original query, however the
 *                    :             parameter numbers will be the one where
 *                    :             the target entry of the original query occurs
 *                    :             in the source data plan target list
 *                    :             The targetList specified the SET clause
 *                    :             e.g. UPDATE tab SET c1 = $3, c2 = $5 ....
 * returningList    : will be provided by pgxc_add_returning_list
 * groupClause        : Our DML won't contin any so NULL.
 * havingQual        : Our DML won't contin any so NULL.
 * windowClause        : Our DML won't contin any so NULL.
 * distinctClause    : Our DML won't contin any so NULL.
 * sortClause        : Our DML won't contin any so NULL.
 * limitOffset        : Our DML won't contin any so NULL.
 * limitCount        : Our DML won't contin any so NULL.
 * rowMarks            : Will be NULL for now, may be used when we provide support
 *                    : for WHERE CURRENT OF.
 * setOperations    : Our DML won't contin any so NULL.
 * constraintDeps    : Our DML won't contin any so NULL.
 * sql_statement    : Original query is not required for deparsing
 * is_local            : Not required for deparsing, keep 0
 * has_to_save_cmd_id    : Not required for deparsing, keep 0
 */
void
pgxc_build_dml_statement(PlannerInfo *root, CmdType cmdtype,
                        Index resultRelationIndex, RemoteQuery *rqplan,
                        List *sourceTargetList, bool interval)
{// #lizard forgives
    Query            *query_to_deparse;
    RangeTblRef        *target_table_ref;
    RangeTblEntry    *res_rel;
    ListCell        *elt;
    bool            ctid_found = false;
    bool            node_id_found = false;
    int                col_att = 0;
    int                ctid_param_num PG_USED_FOR_ASSERTS_ONLY = 0;
    ListCell        *lc;
    bool            can_use_pk_for_rep_change = false;
    int16            *indexed_col_numbers = NULL;
#if 0
    int                index_col_count = 0;
#endif

    /* Make sure we are dealing with DMLs */
    if (cmdtype != CMD_UPDATE &&
        cmdtype != CMD_INSERT &&
        cmdtype != CMD_DELETE)
        return;

    rqplan->rq_num_params = 0;
    rqplan->ss_num_params = 0;
    rqplan->su_num_params = 0;

    /* First construct a reference to an entry in the query's rangetable */
    target_table_ref = makeNode(RangeTblRef);

    /* RangeTblRef::rtindex will be the same as indicated by the caller */
    target_table_ref->rtindex = resultRelationIndex;

    query_to_deparse = makeNode(Query);
    query_to_deparse->commandType = cmdtype;
    query_to_deparse->resultRelation = resultRelationIndex;
    query_to_deparse->hasUnshippableTriggers = true;
    /*
     * While copying the range table to the query to deparse make sure we do
     * not copy RTE's of type RTE_JOIN because set_deparse_for_query
     * function expects that each RTE_JOIN is accompanied by a JoinExpr in
     * Query's jointree, which is not true in case of XC's DML planning.
     * We therefore fill the RTE's of type RTE_JOIN with dummy RTE entries.
     * If each RTE of type RTE_JOIN is not accompanied by a corresponding
     * JoinExpr in Query's jointree then set_deparse_for_query crashes
     * when trying to set_join_column_names, because set_using_names did not
     * call identify_join_columns to put valid values in
     * deparse_columns::leftrti & deparse_columns::rightrti
     * Instead of putting a check in set_join_column_names to return in case
     * of invalid values in leftrti or rightrti, it is preferable to change
     * code here and skip RTE's of type RTE_JOIN while copying
     */
    foreach(lc, root->parse->rtable)
    {
        RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

        if (rte->rtekind == RTE_JOIN)
        {
            RangeTblEntry    *dummy_rte;
            char            *rte_name;

            rte_name = "_DUMMY_RTE_";
            dummy_rte = make_dummy_remote_rte(rte_name,
                                    makeAlias("_DUMMY_RTE_", NIL));

            query_to_deparse->rtable = lappend(query_to_deparse->rtable, dummy_rte);
        }
        else
        {
            query_to_deparse->rtable = lappend(query_to_deparse->rtable, rte);
        }
    }

    res_rel = rt_fetch(resultRelationIndex, query_to_deparse->rtable);
    Assert(res_rel->rtekind == RTE_RELATION);

    /* This RTE should appear in FROM clause of the SQL statement constructed */
    res_rel->inFromCl = true;

    query_to_deparse->jointree = makeNode(FromExpr);

#if 0
    can_use_pk_for_rep_change = (cmdtype == CMD_UPDATE || cmdtype == CMD_DELETE) &&
                IsRelationReplicated(GetRelationLocInfo(res_rel->relid));

    if (can_use_pk_for_rep_change)
    {
        index_col_count = pgxc_find_unique_index(res_rel->relid,
                                                &indexed_col_numbers);
        if (index_col_count <= 0)
            can_use_pk_for_rep_change = false;

        if (can_use_pk_for_rep_change)
        {
            if (is_pk_being_changed(root->parse, indexed_col_numbers,
                                    index_col_count))
            {
                can_use_pk_for_rep_change = false;
            }
        }
    }

    rqplan->rq_use_pk_for_rep_change = can_use_pk_for_rep_change;
#endif
    /*
     * Prepare a param list for INSERT queries
     * While doing so note the position of ctid, xc_node_id in source data
     * plan's target list provided by the caller.
     */
    if (cmdtype == CMD_INSERT)
    {
        rqplan->rq_param_types = (Oid *)palloc(list_length(sourceTargetList) * sizeof(Oid));
    }
    
    foreach(elt, sourceTargetList)
    {
        Oid type;
        TargetEntry    *tle = lfirst(elt);

        col_att++;

        /* The position of ctid/xc_node_id is not required for INSERT */
        if (!can_use_pk_for_rep_change && tle->resjunk &&
            (cmdtype == CMD_UPDATE || cmdtype == CMD_DELETE))
        {
            Var *v = (Var *)tle->expr;

            if (v->varno == resultRelationIndex || interval)
            {
                if (v->varattno == XC_NodeIdAttributeNumber)
                {
                    if (node_id_found)
                        elog(ERROR, "Duplicate node_ids not expected in source target list");
                    node_id_found = true;
                }
                else if (v->varattno == SelfItemPointerAttributeNumber)
                {
                    if (ctid_found)
                        elog(ERROR, "Duplicate ctids not expected in source target list");
                    ctid_found = true;
                }

            }

            continue;
        }

        /*
         * Make sure the entry in the source target list belongs to the
         * target table of the DML
         */
        if (tle->resorigtbl != 0 && tle->resorigtbl != res_rel->relid)
            continue;

        if (cmdtype == CMD_INSERT)
        {
            /* Make sure the column has not been dropped */
            if (get_rte_attribute_is_dropped(res_rel, col_att))
			{
				rqplan->rq_param_types[rqplan->rq_num_params++] = InvalidOid;
                continue;
			}

            /*
             * Create the param to be used for VALUES caluse ($1, $2 ...)
             * and add it to the query target list
             */
            type = exprType((Node *) tle->expr);
            pgxc_add_param_as_tle(query_to_deparse, tle->resno,
                                    type, NULL);

            /* keep param type */
            rqplan->rq_param_types[rqplan->rq_num_params++] = type;
        }
    }

    if(root->parse->onConflict)
        query_to_deparse->onConflict = copyObject(root->parse->onConflict);

    /*
     * In XC we will update *all* the table attributes to reduce code
     * complexity in finding the columns being updated, it works whether
     * we have before row triggers defined on the table or not.
     * The code complexity arises for the case of a table with child tables,
     * where columns are added to parent using ALTER TABLE. The attribute
     * number of the added column is different in parnet and child table.
     * In this case we first have to use TargetEntry::resno to find the name
     * of the column being updated in parent table, and then find the attribute
     * number of that particular column in the child. This makes code complex.
     * In comaprison if we choose to update all the columns of the table
     * irrespective of the columns being updated, the code becomes simple
     * and easy to read.
     * Performance comparison between the two approaches (updating all columns
     * and updating only the columns that were in the target list) shows that
     * both the approaches give similar TPS in hour long runs of DBT1.
     * In XC UPDATE will look like :
     * UPDATE ... SET att1 = $1, att1 = $2, .... attn = $n WHERE ctid = $(n+1)
     */
    if (cmdtype == CMD_UPDATE)
    {
        Oid         type;
        int            natts = get_relnatts(res_rel->relid);
        int            attnum;
		int         appendix = 0;

		/* count origin attrs and ctid, nodeid */
		appendix += node_id_found ? 1 : 0;
		appendix += ctid_found ? 1 : 0;
		rqplan->rq_param_types = (Oid *)palloc((natts + appendix) * sizeof(Oid));

        for (attnum = 1; attnum <= natts; attnum++)
        {
            /* Make sure the column has not been dropped */
            if (get_rte_attribute_is_dropped(res_rel, attnum))
			{
				rqplan->rq_param_types[rqplan->rq_num_params++] = InvalidOid;
                continue;
			}

            type = get_atttype(res_rel->relid, attnum);
            pgxc_add_param_as_tle(query_to_deparse, attnum,
                                type,
                                get_attname(res_rel->relid, attnum));
            /* keep param type */
            rqplan->rq_param_types[rqplan->rq_num_params++] = type;
        }

        /*
         * The data row generated for BIND has all required values, plus NULL
         * values for attributes that are not SET. The first n parameters are
         * the n table attributes, followed by ctid and optionally node_id. So
         * we know that the ctid has to be n + 1.
         */
        if (!can_use_pk_for_rep_change)
        {
            ctid_param_num = natts + 1;
        }
    }
    if (cmdtype == CMD_DELETE)
    {
        if (!can_use_pk_for_rep_change)
        {
            /*
             * Since there is no data to update, the first param is going to be
             * ctid.
             */
            ctid_param_num = 1;
        }
    }

    /* Add quals like ctid = $4 AND xc_node_id = $6 to the UPDATE/DELETE query */
    if (cmdtype == CMD_UPDATE || cmdtype == CMD_DELETE)
    {
        /*
         * If it is not replicated, we can use CTID, otherwise we need
         * to use a defined primary key
         */
        if (!can_use_pk_for_rep_change)
        {
            if (!ctid_found)
                elog(ERROR, "Source data plan's target list does not contain ctid colum");

            /* delete just need ctid and xc_node_id */
            if (cmdtype == CMD_DELETE)
            {
                rqplan->rq_param_types = (Oid *)palloc(2 * sizeof(Oid));
            }
            /*
             * Beware, the ordering of ctid and node_id is important ! ctid should
             * be followed by node_id, not vice-versa, so as to be consistent with
             * the data row to be generated while binding the parameters for the
             * update statement.
             */
            pgxc_dml_add_qual_to_query(query_to_deparse, rqplan->rq_num_params + 1,
                            SelfItemPointerAttributeNumber, resultRelationIndex, TIDOID, false);

            rqplan->rq_param_types[rqplan->rq_num_params++] = TIDOID;

            if (node_id_found)
            {
                pgxc_dml_add_qual_to_query(query_to_deparse, rqplan->rq_num_params + 1,
                                XC_NodeIdAttributeNumber, resultRelationIndex, INT4OID, false);

                rqplan->rq_param_types[rqplan->rq_num_params++] = INT4OID;
            }
        }
#if 0
        else
        {
            /*
             * Add all the columns of the primary key or unique index
             * in the where clause of update / delete on the replicated table
             */
            int i;
            for (i = 0; i < index_col_count; i++)
            {
                int            pkattno = indexed_col_numbers[i];

                col_att = 0;
                foreach(elt, sourceTargetList)
                {
                    TargetEntry    *tle = lfirst(elt);
                    Var *v;
            
                    col_att++;
            
                    v = (Var *)tle->expr;
        
                    if (v->varno == resultRelationIndex &&
                        v->varattno == pkattno)
                    {
                        break;
                    }
                }

                pgxc_dml_add_qual_to_query(query_to_deparse, col_att,
                            pkattno, resultRelationIndex, INT4OID, false);
            }
        }
#endif
        query_to_deparse->jointree->quals = (Node *)make_andclause(
                        (List *)query_to_deparse->jointree->quals);
    }

    if (indexed_col_numbers != NULL)
        pfree(indexed_col_numbers);

    /* pgxc_add_returning_list copied returning list in base_tlist */
    if (rqplan->base_tlist)
        query_to_deparse->returningList = list_copy(rqplan->base_tlist);

    rqplan->remote_query = query_to_deparse;

    pgxc_rqplan_build_statement(rqplan);

    /*
      * At last, we have to check if it is insert... on conflict do update.
      * If so, we need to generate select and update statement for coordinator
      * to exec UPSERT.
      */
    if (cmdtype == CMD_INSERT && root->parse->onConflict &&
        root->parse->onConflict->action == ONCONFLICT_UPDATE)
    {
        pgxc_build_upsert_statement(root, cmdtype, resultRelationIndex, 
                                    rqplan, sourceTargetList);
    }
}

/*
 * pgxc_set_en_expr
 * Try to find the expression of distribution column to calculate node at plan execution
 */
Expr *
pgxc_set_en_expr(Oid tableoid, Index resultRelationIndex)
{
    HeapTuple tp;
    Form_pg_attribute partAttrTup;
    Var    *var;
    RelationLocInfo *rel_loc_info;

    /* Get location info of the target table */
    rel_loc_info = GetRelationLocInfo(tableoid);
    if (rel_loc_info == NULL)
         return NULL;

    /*
     * For hash/modulo distributed tables, the target node must be selected
     * at the execution time based on the partition column value.
     *
     * For round robin distributed tables, tuples must be divided equally
     * between the nodes.
     *
     * For replicated tables, tuple must be inserted in all the Datanodes
     *
     * XXX Need further testing for replicated and round-robin tables
     */
    if (rel_loc_info->locatorType != LOCATOR_TYPE_HASH &&
        rel_loc_info->locatorType != LOCATOR_TYPE_MODULO
        && rel_loc_info->locatorType != LOCATOR_TYPE_SHARD
        )
        return NULL;

    tp = SearchSysCache(ATTNUM,
                        ObjectIdGetDatum(tableoid),
                        Int16GetDatum(rel_loc_info->partAttrNum),
                        0, 0);
    partAttrTup = (Form_pg_attribute) GETSTRUCT(tp);

    /*
     * Create a Var for the distribution column and set it for
     * execution time evaluation of target node. ExecEvalVar() picks
     * up values from ecxt_scantuple if Var does not refer either OUTER
     * or INNER varno. We utilize that mechanism to pick up values from
     * the tuple returned by the current plan node
     */
    var = makeVar(resultRelationIndex,
                  rel_loc_info->partAttrNum,
                  partAttrTup->atttypid,
                  partAttrTup->atttypmod,
                  partAttrTup->attcollation,
                  0);
    ReleaseSysCache(tp);

    return (Expr *) var;
}

#if 0
Expr *
pgxc_set_sec_en_expr(Oid tableoid, Index resultRelationIndex)
{
    HeapTuple tp;
    Form_pg_attribute partAttrTup;
    Var    *var;
    RelationLocInfo *rel_loc_info;

    /* Do not have distribute column now */
    return NULL;

    /* Get location info of the target table */
    rel_loc_info = GetRelationLocInfo(tableoid);
    if (rel_loc_info == NULL)
         return NULL;

    /*
     * For hash/modulo distributed tables, the target node must be selected
     * at the execution time based on the partition column value.
     *
     * For round robin distributed tables, tuples must be divided equally
     * between the nodes.
     *
     * For replicated tables, tuple must be inserted in all the Datanodes
     *
     * XXX Need further testing for replicated and round-robin tables
     */
    if (rel_loc_info->locatorType != LOCATOR_TYPE_HASH &&
        rel_loc_info->locatorType != LOCATOR_TYPE_MODULO
        && rel_loc_info->locatorType != LOCATOR_TYPE_SHARD
        )
        return NULL;

    if (InvalidAttrNumber == rel_loc_info->secAttrNum)
    {
        return NULL;
    }
    tp = SearchSysCache(ATTNUM,
                        ObjectIdGetDatum(tableoid),
                        Int16GetDatum(rel_loc_info->secAttrNum),
                        0, 0);
    partAttrTup = (Form_pg_attribute) GETSTRUCT(tp);

    /*
     * Create a Var for the distribution column and set it for
     * execution time evaluation of target node. ExecEvalVar() picks
     * up values from ecxt_scantuple if Var does not refer either OUTER
     * or INNER varno. We utilize that mechanism to pick up values from
     * the tuple returned by the current plan node
     */
    var = makeVar(resultRelationIndex,
                  rel_loc_info->secAttrNum,
                  partAttrTup->atttypid,
                  partAttrTup->atttypmod,
                  partAttrTup->attcollation,
                  0);
    ReleaseSysCache(tp);

    return (Expr *) var;
}
#endif
#endif
