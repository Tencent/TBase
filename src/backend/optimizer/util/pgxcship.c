/*-------------------------------------------------------------------------
 *
 * pgxcship.c
 *        Routines to evaluate expression shippability to remote nodes
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012, Postgres-XC Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *      src/backend/optimizer/util/pgxcship.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "catalog/pg_class.h"
#include "catalog/pg_inherits_fn.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#ifdef PGXC
#include "catalog/pg_trigger.h"
#endif
#include "catalog/pg_type.h"
#include "catalog/pgxc_node.h"
#include "commands/trigger.h"
#include "nodes/nodeFuncs.h"
#include "nodes/relation.h"
#include "optimizer/clauses.h"
#include "optimizer/pgxcship.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#include "parser/parse_coerce.h"
#include "parser/parse_type.h"
#include "pgxc/locator.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/nodemgr.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#ifdef __COLD_HOT__
#include "catalog/pgxc_key_values.h"
#endif
#ifdef __TBASE__
#include "catalog/pg_constraint.h"
#include "access/htup_details.h"
#include "optimizer/pathnode.h"
#endif
/*
 * Shippability_context
 * This context structure is used by the Fast Query Shipping walker, to gather
 * information during analysing query for Fast Query Shipping.
 */
typedef struct
{
    bool        sc_for_expr;        /* if false, the we are checking shippability
                                     * of the Query, otherwise, we are checking
                                     * shippability of a stand-alone expression.
                                     */
    Bitmapset    *sc_shippability;    /* The conditions for (un)shippability of the
                                     * query.
                                     */
    Query        *sc_query;            /* the query being analysed for FQS */
    int            sc_query_level;        /* level of the query */
    int            sc_max_varlevelsup;    /* maximum upper level referred to by any
                                     * variable reference in the query. If this
                                     * value is greater than 0, the query is not
                                     * shippable, if shipped alone.
                                     */
    ExecNodes    *sc_exec_nodes;        /* nodes where the query should be executed */
    ExecNodes    *sc_subquery_en;    /* ExecNodes produced by merging the ExecNodes
                                     * for individual subqueries. This gets
                                     * ultimately merged with sc_exec_nodes.
                                     */
    bool        sc_groupby_has_distcol;    /* GROUP BY clause has distribution column */
} Shippability_context;

/*
 * ShippabilityStat
 * List of reasons why a query/expression is not shippable to remote nodes.
 */
typedef enum
{
    SS_UNSHIPPABLE_EXPR = 0,    /* it has unshippable expression */
    SS_NEED_SINGLENODE,            /* Has expressions which can be evaluated when
                                 * there is only a single node involved.
                                 * Athought aggregates too fit in this class, we
                                 * have a separate status to report aggregates,
                                 * see below.
                                 */
    SS_NEEDS_COORD,                /* the query needs Coordinator */
    SS_VARLEVEL,                /* one of its subqueries has a VAR
                                 * referencing an upper level query
                                 * relation
                                 */
    SS_NO_NODES,                /* no suitable nodes can be found to ship
                                 * the query
                                 */
    SS_UNSUPPORTED_EXPR,        /* it has expressions currently unsupported
                                 * by FQS, but such expressions might be
                                 * supported by FQS in future
                                 */
    SS_HAS_AGG_EXPR,            /* it has aggregate expressions */
    SS_UNSHIPPABLE_TYPE,        /* the type of expression is unshippable */
    SS_UNSHIPPABLE_TRIGGER,        /* the type of trigger is unshippable */
	SS_UPDATES_DISTRIBUTION_COLUMN,	/* query updates the distribution column */
	SS_NEED_FUNC_REWRITE		/* exist func expression of distribution column,
								 * we should rewrite the expr for FQS 
								 */
} ShippabilityStat;

extern void PoolPingNodes(void);


/* Determine if given function is shippable */
static bool pgxc_is_func_shippable(Oid funcid);
/* Check equijoin conditions on given relations */
static Expr *pgxc_find_dist_equijoin_qual(Relids varnos_1, Relids varnos_2,
                                Oid distcol_type, Node *quals, List *rtable);
/* Merge given execution nodes based on join shippability conditions */
static ExecNodes *pgxc_merge_exec_nodes(ExecNodes *en1, ExecNodes *en2);
/* Check if given Query includes distribution column */
static bool pgxc_query_has_distcolgrouping(Query *query);

/* Manipulation of shippability reason */
static bool pgxc_test_shippability_reason(Shippability_context *context,
                                          ShippabilityStat reason);
static void pgxc_set_shippability_reason(Shippability_context *context,
                                         ShippabilityStat reason);
static void pgxc_reset_shippability_reason(Shippability_context *context,
                                           ShippabilityStat reason);

/* Evaluation of shippability */
static bool pgxc_shippability_walker(Node *node, Shippability_context *sc_context);
static void pgxc_set_exprtype_shippability(Oid exprtype, Shippability_context *sc_context);

static ExecNodes *pgxc_is_join_shippable(ExecNodes *inner_en, ExecNodes *outer_en,
                        Relids in_relids, Relids out_relids, JoinType jointype,
                        List *join_quals, Query *query, List *rtables);

/* Fast-query shipping (FQS) functions */
static ExecNodes *pgxc_FQS_get_relation_nodes(RangeTblEntry *rte,
                                              Index varno,
                                              Query *query);
static ExecNodes *pgxc_FQS_find_datanodes(Query *query);
static bool pgxc_query_needs_coord(Query *query);
static bool pgxc_query_contains_only_pg_catalog(List *rtable);
static bool pgxc_is_var_distrib_column(Var *var, List *rtable);
static bool pgxc_distinct_has_distcol(Query *query);
static bool pgxc_targetlist_has_distcol(Query *query);
static ExecNodes *pgxc_FQS_find_datanodes_recurse(Node *node, Query *query,
                                            Bitmapset **relids);
static ExecNodes *pgxc_FQS_datanodes_for_rtr(Index varno, Query *query);


#ifdef __TBASE__
static ExecNodes* pgxc_is_group_subquery_shippable(Query *query, Shippability_context *sc_context);
static void pgxc_is_rte_subquery_shippable(Node *node, Shippability_context *sc_context);
static bool pgxc_is_simple_subquery(Query *subquery);
static bool pgxc_FQS_check_subquery_const(Query *query);
static ExecNodes *make_FQS_single_node();
#endif
/*
 * Set the given reason in Shippability_context indicating why the query can not be
 * shipped directly to remote nodes.
 */
static void
pgxc_set_shippability_reason(Shippability_context *context, ShippabilityStat reason)
{
    context->sc_shippability = bms_add_member(context->sc_shippability, reason);
}

/*
 * pgxc_reset_shippability_reason
 * Reset reason why the query cannot be shipped to remote nodes
 */
static void
pgxc_reset_shippability_reason(Shippability_context *context, ShippabilityStat reason)
{
    context->sc_shippability = bms_del_member(context->sc_shippability, reason);
    return;
}


/*
 * See if a given reason is why the query can not be shipped directly
 * to the remote nodes.
 */
static bool
pgxc_test_shippability_reason(Shippability_context *context, ShippabilityStat reason)
{
    return bms_is_member(reason, context->sc_shippability);
}


/*
 * pgxc_set_exprtype_shippability
 * Set the expression type shippability. For now composite types
 * derived from view definitions are not shippable.
 */
static void
pgxc_set_exprtype_shippability(Oid exprtype, Shippability_context *sc_context)
{
    char    typerelkind;

    typerelkind = get_rel_relkind(typeidTypeRelid(exprtype));

    if (typerelkind == RELKIND_SEQUENCE ||
        typerelkind == RELKIND_VIEW        ||
        typerelkind == RELKIND_FOREIGN_TABLE)
        pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_TYPE);
}

#ifdef __TBASE__
/*
 * pgxc_FQS_check_const_recurse
 * Recursively check the query node to see if it only contains constant values.
 * We only support all constant values in same leaf nodes, correlated cases are
 * not supported.
 */
static bool
pgxc_FQS_check_const_recurse(Node *node, Query *query)
{
	if (!node)
		return false;

	switch(nodeTag(node))
	{
		case T_FromExpr:
		{
			FromExpr	*from_expr = (FromExpr *)node;
			ListCell	*lcell;
			bool		 result = true;

			/*
			 * Only support SELECT for now
			 */
			if (query->commandType != CMD_SELECT)
				return false;

			/*
			 * Check the SetOperation to cover the case of
			 * '(const subquery) UNION (const subquery)...'
			 */
			if (!from_expr->fromlist)
			{
				if (query->setOperations &&
					IsA(query->setOperations, SetOperationStmt))
				{
					return pgxc_FQS_check_const_recurse(query->setOperations, query);
				}
				return false;
			}

			/* Check if all RTEs contains only constant values */
			foreach (lcell, from_expr->fromlist)
			{
				Node	*fromlist_entry = lfirst(lcell);

				if (!pgxc_FQS_check_const_recurse(fromlist_entry, query))
				{
					result = false;
				}
			}
			return result;
		}
		case T_RangeTblRef:
		{
			RangeTblRef *rtr = (RangeTblRef *)node;
			RangeTblEntry *rte = rt_fetch(rtr->rtindex, query->rtable);

			if (rte->rtekind == RTE_SUBQUERY)
			{
				return pgxc_FQS_check_subquery_const(rte->subquery);
			}
			return false;
		}
		case T_JoinExpr:
		{
			/* TODO: Not supported yet */
			return false;
		}
		case T_SetOperationStmt:
		{
			SetOperationStmt *setOp = (SetOperationStmt *)node;

			/* Only handle UNION cases */
			if (setOp->op == SETOP_UNION &&
				pgxc_FQS_check_const_recurse(setOp->larg, query) &&
				pgxc_FQS_check_const_recurse(setOp->rarg, query))
			{
				return true;
			}
			return false;
		}
		default:
			return false;
	}
	/* Keep compiler happy */
	return false;
}

/*
 * pgxc_FQS_check_subquery_const
 * Check the query node to see if it only contains constant values, we could
 * provide more shipping optimizations based on this hint.
 */
static bool
pgxc_FQS_check_subquery_const(Query *query)
{
	ListCell *lc;
	bool result = true;

	/* If all target list entries are T_Const, then we are done. */
	foreach(lc, query->targetList)
	{
		TargetEntry *tle = lfirst(lc);
		if (!IsA(tle->expr, Const))
		{
			result = false;
		}
	}

	if (result == true)
		return true;

	/* Otherwise, check if all RTEs are const */
	return pgxc_FQS_check_const_recurse((Node *)query->jointree, query);
}
#endif

/*
 * pgxc_FQS_datanodes_for_rtr
 * For a given RangeTblRef find the datanodes where corresponding data is
 * located.
 */
static ExecNodes *
pgxc_FQS_datanodes_for_rtr(Index varno, Query *query)
{// #lizard forgives
    RangeTblEntry *rte = rt_fetch(varno, query->rtable);
    switch (rte->rtekind)
    {
        case RTE_RELATION:
        {
            /* For anything, other than a table, we can't find the datanodes */
#ifdef __TBASE__
			if (rte->relkind != RELKIND_RELATION &&
				rte->relkind != RELKIND_PARTITIONED_TABLE)
			{
				return NULL;
			}
#else
            if (rte->relkind != RELKIND_RELATION)
                return NULL;
#endif
            /*
             * In case of inheritance, child tables can have completely different
             * Datanode distribution than parent. To handle inheritance we need
             * to merge the Datanodes of the children table as well. The inheritance
             * is resolved during planning, so we may not have the RTEs of the
             * children here. Also, the exact method of merging Datanodes of the
             * children is not known yet. So, when inheritance is requested, query
             * can not be shipped.
             * See prologue of has_subclass, we might miss on the optimization
             * because has_subclass can return true even if there aren't any
             * subclasses, but it's ok.
             */
#ifdef __TBASE__
			/* 
			 * all partitioned tables should have the same distribution, try to 
			 * get execution datanodes
			 */
			if (rte->inh && has_subclass(rte->relid) &&
				rte->relkind != RELKIND_PARTITIONED_TABLE)
			{
				return NULL;
			}
#else
            if (rte->inh && has_subclass(rte->relid))
                return NULL;
#endif

			return pgxc_FQS_get_relation_nodes(rte, varno, query);
		}
		break;
		case RTE_SUBQUERY:
#ifdef __TBASE__
		{
			Query 		*subquery = rte->subquery;
			ExecNodes 	*exec_nodes = NULL;

			/*
			 * Consider the case if subquery only contains constant values.
			 * If so, we can treat them as replicated RTE.
			 */
			if (enable_subquery_shipping &&
				pgxc_FQS_check_subquery_const(subquery))
			{
				exec_nodes = makeNode(ExecNodes);
				exec_nodes->baselocatortype = LOCATOR_TYPE_REPLICATED;
				/*
				 * No locate info stored for such subquery RTEs, we use this
				 * flag to force using the other hand locate info.
				 */
				exec_nodes->const_subquery = true;

				return exec_nodes;
			}

			/* Try to process exec_nodes for simple Subquery */
			if (enable_subquery_shipping &&
				pgxc_is_simple_subquery(subquery))
			{
				Bitmapset	*relids = NULL;

				/* Recurse into the subquery to find executable datanodes. */
				exec_nodes = pgxc_FQS_find_datanodes_recurse((Node *)subquery->jointree,
															 subquery, &relids);

				/* Clean up the relids used in recursion function */
				bms_free(relids);
				relids = NULL;

				/*
				 * Currently we only support Subquery push down to single DN.
				 * Multiple DN pushdown will have cross-phase issues between
				 * main query and subquery, it needs more complicate
				 * calculation. So we just skip the case by now.
				 */
				if (exec_nodes && exec_nodes->nodeList &&
					(list_length(exec_nodes->nodeList) == 1))
					return exec_nodes;
				else
					return NULL;
			}

			return NULL;
		}
#endif
		/* For any other type of RTE, we return NULL for now */
		case RTE_JOIN:
		case RTE_CTE:
		case RTE_FUNCTION:
		case RTE_VALUES:
		default:
			return NULL;
	}
}

/*
 * pgxc_FQS_find_datanodes_recurse
 * Recursively find whether the sub-tree of From Expr rooted under given node is
 * pushable and if yes where.
 */
static ExecNodes *
pgxc_FQS_find_datanodes_recurse(Node *node, Query *query, Bitmapset **relids)
{
	List		*query_rtable = query->rtable;

	if (!node)
		return NULL;

	switch(nodeTag(node))
	{
		case T_FromExpr:
		{
			FromExpr	*from_expr = (FromExpr *)node;
			ListCell	*lcell;
			bool		first;
			Bitmapset	*from_relids;
			ExecNodes	*result_en;

			/*
			 * For INSERT commands, we won't have any entries in the from list.
			 * Get the datanodes using the resultRelation index.
			 */
			if (query->commandType != CMD_SELECT && !from_expr->fromlist)
			{
				*relids = bms_make_singleton(query->resultRelation);
				return pgxc_FQS_datanodes_for_rtr(query->resultRelation,
														query);
			}

			/*
			 * All the entries in the From list are considered to be INNER
			 * joined with the quals as the JOIN condition. Get the datanodes
			 * for the first entry in the From list. For every subsequent entry
			 * determine whether the join between the relation in that entry and
			 * the cumulative JOIN of previous entries can be pushed down to the
			 * datanodes and the corresponding set of datanodes where the join
			 * can be pushed down.
			 */
			first = true;
			result_en = NULL;
			from_relids = NULL;
			foreach (lcell, from_expr->fromlist)
			{
				Node *fromlist_entry = lfirst(lcell);
				Bitmapset *fle_relids = NULL;
				ExecNodes *tmp_en;
				ExecNodes *en = pgxc_FQS_find_datanodes_recurse(fromlist_entry,
																query, &fle_relids);
				/*
				 * If any entry in fromlist is not shippable, jointree is not
				 * shippable
				 */
				if (!en)
				{
					FreeExecNodes(&result_en);
					return NULL;
				}

				/* FQS does't ship a DML with more than one relation involved */
				if (!first && query->commandType != CMD_SELECT)
				{
					FreeExecNodes(&result_en);
					return NULL;
				}

				if (first)
				{
					first = false;
					result_en = en;
					from_relids = fle_relids;
					continue;
				}

				tmp_en = result_en;
				/*
				 * Check whether the JOIN is pushable to the datanodes and
				 * find the datanodes where the JOIN can be pushed to
				 */
				result_en = pgxc_is_join_shippable(result_en, en, from_relids,
										fle_relids, JOIN_INNER,
										make_ands_implicit((Expr *)from_expr->quals),
#ifdef __TBASE__
                                        query,
#endif
                                        query_rtable);
                from_relids = bms_join(from_relids, fle_relids);
                FreeExecNodes(&tmp_en);
            }

            *relids = from_relids;
            return result_en;
        }
            break;

        case T_RangeTblRef:
        {
            RangeTblRef *rtr = (RangeTblRef *)node;
            *relids = bms_make_singleton(rtr->rtindex);
            return pgxc_FQS_datanodes_for_rtr(rtr->rtindex, query);
        }
            break;

        case T_JoinExpr:
        {
            JoinExpr *join_expr = (JoinExpr *)node;
            Bitmapset *l_relids = NULL;
            Bitmapset *r_relids = NULL;
            ExecNodes *len;
            ExecNodes *ren;
            ExecNodes *result_en;

            /* FQS does't ship a DML with more than one relation involved */
            if (query->commandType != CMD_SELECT)
                return NULL;

            len = pgxc_FQS_find_datanodes_recurse(join_expr->larg, query,
                                                                &l_relids);
            ren = pgxc_FQS_find_datanodes_recurse(join_expr->rarg, query,
                                                                &r_relids);
            /* If either side of JOIN is unshippable, JOIN is unshippable */
            if (!len || !ren)
            {
                FreeExecNodes(&len);
                FreeExecNodes(&ren);
                return NULL;
            }
            /*
             * Check whether the JOIN is pushable or not, and find the datanodes
             * where the JOIN can be pushed to.
             */
            result_en = pgxc_is_join_shippable(ren, len, r_relids, l_relids,
                                                join_expr->jointype,
                                                make_ands_implicit((Expr *)join_expr->quals),
#ifdef __TBASE__
                                                query,
#endif
                                                query_rtable);
            FreeExecNodes(&len);
            FreeExecNodes(&ren);
            *relids = bms_join(l_relids, r_relids);
            return result_en;
        }
            break;

        default:
            *relids = NULL;
            return NULL;
            break;
    }
    /* Keep compiler happy */
    return NULL;
}

/*
 * pgxc_FQS_find_datanodes
 * Find the list of nodes where to ship query.
 */
static ExecNodes *
pgxc_FQS_find_datanodes(Query *query)
{// #lizard forgives
    Bitmapset    *relids = NULL;
    ExecNodes    *exec_nodes;

    /*
     * For SELECT, the datanodes required to execute the query is obtained from
     * the join tree of the query
     */
    exec_nodes = pgxc_FQS_find_datanodes_recurse((Node *)query->jointree,
                                                        query, &relids);
    bms_free(relids);
    relids = NULL;

    /* If we found the datanodes to ship, use them */
    if (exec_nodes && exec_nodes->nodeList)
    {
        /*
         * If relations involved in the query are such that ultimate JOIN is
         * replicated JOIN, choose only one of them. If one of them is a
         * preferred node choose that one, otherwise choose the first one.
         */
        if (IsLocatorReplicated(exec_nodes->baselocatortype) &&
            (exec_nodes->accesstype == RELATION_ACCESS_READ ||
            exec_nodes->accesstype == RELATION_ACCESS_READ_FQS ||
            exec_nodes->accesstype == RELATION_ACCESS_READ_FOR_UPDATE))
        {
            List *tmp_list = exec_nodes->nodeList;
            exec_nodes->nodeList = GetPreferredReplicationNode(exec_nodes->nodeList);
            list_free(tmp_list);
        }
        return exec_nodes;
    }
    /*
     * If we found the expression which can decide which can be used to decide
     * where to ship the query, use that
     */
    else if (exec_nodes && exec_nodes->en_expr)
        return exec_nodes;
    /* No way to figure out datanodes to ship the query to */
    return NULL;
}


static Expr *
create_dis_col_eval(Node *quals, AttrNumber discol)
{// #lizard forgives
    Expr *result = NULL;

    if(!quals)
        return result;
    
    if(IsA(quals, OpExpr))
    {
        Expr *lexpr;
        Expr *rexpr;
        OpExpr * op = (OpExpr *)quals;

        /* could not be '=' */
        if (list_length(op->args) != 2)
            return result;
                
        lexpr = linitial(op->args);
        rexpr = lsecond(op->args);

        if (IsA(lexpr, RelabelType))
            lexpr = ((RelabelType*)lexpr)->arg;
        if (IsA(rexpr, RelabelType))
            rexpr = ((RelabelType*)rexpr)->arg;

        if (IsA(lexpr, Var) && IsA(rexpr, Param))
        {
            if(discol == ((Var *)lexpr)->varattno)
            {
                /* must be '=' */
                if (!op_mergejoinable(op->opno, exprType((Node *)lexpr)) &&
                    !op_hashjoinable(op->opno, exprType((Node *)lexpr)))
                    return result;
                else
                    result = (Expr *)copyObject(rexpr);
            }
        }
        else if (IsA(rexpr, Var) && IsA(lexpr, Param))
        {
            if(discol == ((Var *)rexpr)->varattno)
            {
                /* must be '=' */
                if (!op_mergejoinable(op->opno, exprType((Node *)lexpr)) &&
                    !op_hashjoinable(op->opno, exprType((Node *)lexpr)))
                    return result;
                else
                    result = (Expr *)copyObject(lexpr);
            }
        }
    }
    else if(IsA(quals, BoolExpr) && ((BoolExpr *)quals)->boolop == AND_EXPR)
    {
        ListCell *lc;
        BoolExpr * bexpr = (BoolExpr *)quals;

        foreach(lc, bexpr->args)
        {
            Expr *lexpr = lfirst(lc);

            result = create_dis_col_eval((Node *)lexpr, discol);

            if(result)
                break;
        }
    }


    return result;
}


/*
 * pgxc_FQS_get_relation_nodes
 * Return ExecNodes structure so as to decide which node the query should
 * execute on. If it is possible to set the node list directly, set it.
 * Otherwise set the appropriate distribution column expression or relid in
 * ExecNodes structure.
 */
static ExecNodes *
pgxc_FQS_get_relation_nodes(RangeTblEntry *rte, Index varno, Query *query)
{// #lizard forgives
    CmdType command_type = query->commandType;
    bool for_update = query->rowMarks ? true : false;
    ExecNodes    *rel_exec_nodes;
    RelationAccessType rel_access = RELATION_ACCESS_READ;
    RelationLocInfo *rel_loc_info;
    bool retry = true;

    Assert(rte == rt_fetch(varno, (query->rtable)));

    switch (command_type)
    {
        case CMD_SELECT:
            if (for_update)
                rel_access = RELATION_ACCESS_READ_FOR_UPDATE;
            else
                rel_access = RELATION_ACCESS_READ_FQS;
            break;

        case CMD_UPDATE:
        case CMD_DELETE:
            rel_access = RELATION_ACCESS_UPDATE;
            break;

        case CMD_INSERT:
            rel_access = RELATION_ACCESS_INSERT;
            break;

        default:
            /* should not happen, but */
            elog(ERROR, "Unrecognised command type %d", command_type);
            break;
    }

    rel_loc_info = GetRelationLocInfo(rte->relid);
    /* If we don't know about the distribution of relation, bail out */
    if (!rel_loc_info)
        return NULL;

    /*
     * Find out the datanodes to execute this query on.
     * But first if it's a replicated table, identify and remove
     * unhealthy nodes from the rel_loc_info. Only for SELECTs!
     *
     * PGXC_FQS_TODO: for now, we apply node reduction only when there is only
     * one relation involved in the query. If there are multiple distributed
     * tables in the query and we apply node reduction here, we may fail to ship
     * the entire join. We should apply node reduction transitively.
     */
retry_pools:
    if (command_type == CMD_SELECT &&
            rel_loc_info->locatorType == LOCATOR_TYPE_REPLICATED)
    {
        int i;
        List *newlist = NIL;
        ListCell *lc;
        bool *healthmap = NULL;
        healthmap = (bool*)palloc(sizeof(bool) * TBASE_MAX_DATANODE_NUMBER);
        if (healthmap == NULL)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_OUT_OF_MEMORY),
                     errmsg("out of memory for healthmap")));
        }
        
        PgxcNodeDnListHealth(rel_loc_info->rl_nodeList, healthmap);

        i = 0;
        foreach(lc, rel_loc_info->rl_nodeList)
        {
            if (!healthmap[i++])
                newlist = lappend_int(newlist, lfirst_int(lc));
        }

        if (healthmap)
        {
            pfree(healthmap);
            healthmap = NULL;
        }

        if (newlist != NIL)
            rel_loc_info->rl_nodeList = list_difference_int(rel_loc_info->rl_nodeList,
                                                     newlist);
        /*
         * If all nodes are down, cannot do much, just return NULL here
         */
        if (rel_loc_info->rl_nodeList == NIL)
        {
            /*
             * Try an on-demand pool maintenance just to see if some nodes
             * have come back.
             *
             * Try once and error out if datanodes are still down
             */
            if (retry)
            {
                rel_loc_info->rl_nodeList = newlist;
                newlist = NIL;
                PoolPingNodes();
                retry = false;
                goto retry_pools;
            }
            else
                elog(ERROR,
                 "Could not find healthy datanodes for replicated table. Exiting!");
            return NULL;
        }
    }
    if (list_length(query->rtable) == 1)
        rel_exec_nodes = GetRelationNodesByQuals(rte->relid, rel_loc_info, varno,
                                                 query->jointree->quals, rel_access, NULL, NULL);
    else
    {
#ifdef __COLD_HOT__
        int32    nGroup;
        Oid     *groups;

        if (!((command_type == CMD_INSERT && query->onConflict) || command_type == CMD_SELECT))
        {
            GetRelationSecondGroup(rte->relid, &groups, &nGroup);
            if (nGroup)
            {
                pfree(groups);
            }
            if (OidIsValid(rel_loc_info->coldGroupId) || nGroup)
            {
                elog(ERROR, "table in cold-hot group or key-value group could not join with other tables.");
            }
        }
        
        rel_exec_nodes = GetRelationNodes(rel_loc_info, (Datum) 0,
                                  true, 0, true, rel_access);
#else
        rel_exec_nodes = GetRelationNodes(rel_loc_info, (Datum) 0,
                                          true, rel_access);
#endif
    }

    if (!rel_exec_nodes)
        return NULL;

    if ((rel_access == RELATION_ACCESS_INSERT || list_length(query->rtable) == 1)&&
             IsRelationDistributedByValue(rel_loc_info))
    {
        ListCell *lc;
        TargetEntry *tle;
        /*
         * If the INSERT is happening on a table distributed by value of a
         * column, find out the
         * expression for distribution column in the targetlist, and stick in
         * in ExecNodes, and clear the nodelist. Execution will find
         * out where to insert the row.
         */
        /* It is a partitioned table, get value by looking in targetList */
        if(rel_access == RELATION_ACCESS_INSERT)
        {
            foreach(lc, query->targetList)
            {
                tle = (TargetEntry *) lfirst(lc);

                if (tle->resjunk)
                    continue;
                if (strcmp(tle->resname, GetRelationDistribColumn(rel_loc_info)) == 0)
                {
#ifdef __TBASE__
                    if (expression_returns_set((Node *)tle->expr))
                        rel_exec_nodes->en_expr = NULL;
                    else
#endif
                        rel_exec_nodes->en_expr = tle->expr;
#ifndef __COLD_HOT__
                    break;
#endif
                }

#ifdef __COLD_HOT__
                if (rel_loc_info->secAttrNum != InvalidAttrNumber)
                {
                    if (strcmp(tle->resname, GetRelationSecDistribColumn(rel_loc_info)) == 0)
                    {
                        if (expression_returns_set((Node *)tle->expr))
                            rel_exec_nodes->sec_en_expr = NULL;
                        else
                            rel_exec_nodes->sec_en_expr = tle->expr;
                    }
                }

                /* got both distribute key, done */
                if (rel_exec_nodes->en_expr && rel_exec_nodes->sec_en_expr)
                {
                    break;
                }
#endif

            }
        }
        else
        {
            if(rel_loc_info->locatorType == LOCATOR_TYPE_HASH || rel_loc_info->locatorType == LOCATOR_TYPE_SHARD)
            {
                Node *quals = query->jointree->quals;
                rel_exec_nodes->en_expr = create_dis_col_eval(quals, rel_loc_info->partAttrNum);
#ifdef __COLD_HOT__
                if (rel_loc_info->secAttrNum != InvalidAttrNumber)
                {
                    rel_exec_nodes->sec_en_expr = create_dis_col_eval(quals, rel_loc_info->secAttrNum);
                }
#endif
            }
        }
        /* Not found, bail out */
        if (!rel_exec_nodes->en_expr)        
        {
            if(rel_access != RELATION_ACCESS_INSERT)                
                return rel_exec_nodes;        
            else                
                return NULL;
        }
        
        /* We found the TargetEntry for the partition column */
        list_free(rel_exec_nodes->primarynodelist);
        rel_exec_nodes->primarynodelist = NULL;
        list_free(rel_exec_nodes->nodeList);
        rel_exec_nodes->nodeList = NULL;
        rel_exec_nodes->en_relid = rel_loc_info->relid;
    }
    return rel_exec_nodes;
}

static bool
pgxc_query_has_distcolgrouping(Query *query)
{
    ListCell    *lcell;
    foreach (lcell, query->groupClause)
    {
        SortGroupClause     *sgc = lfirst(lcell);
        Node                *sgc_expr;
        if (!IsA(sgc, SortGroupClause))
            continue;
        sgc_expr = get_sortgroupclause_expr(sgc, query->targetList);
		if (IsA(sgc_expr, Var) && (((Var *)sgc_expr)->varlevelsup == 0) &&
            pgxc_is_var_distrib_column((Var *)sgc_expr, query->rtable))
            return true;
    }
    return false;
}

static bool
pgxc_distinct_has_distcol(Query *query)
{
    ListCell    *lcell;
    foreach (lcell, query->distinctClause)
    {
        SortGroupClause     *sgc = lfirst(lcell);
        Node                *sgc_expr;
        if (!IsA(sgc, SortGroupClause))
            continue;
        sgc_expr = get_sortgroupclause_expr(sgc, query->targetList);
		if (IsA(sgc_expr, Var) && (((Var *)sgc_expr)->varlevelsup == 0) &&
            pgxc_is_var_distrib_column((Var *)sgc_expr, query->rtable))
            return true;
    }
    return false;
}

/*
 * pgxc_shippability_walker
 * walks the query/expression tree routed at the node passed in, gathering
 * information which will help decide whether the query to which this node
 * belongs is shippable to the Datanodes.
 *
 * The function should try to walk the entire tree analysing each subquery for
 * shippability. If a subquery is shippable but not the whole query, we would be
 * able to create a RemoteQuery node for that subquery, shipping it to the
 * Datanode.
 *
 * Return value of this function is governed by the same rules as
 * expression_tree_walker(), see prologue of that function for details.
 */
static bool
pgxc_shippability_walker(Node *node, Shippability_context *sc_context)
{// #lizard forgives
    if (node == NULL)
        return false;

    /* Below is the list of nodes that can appear in a query, examine each
     * kind of node and find out under what conditions query with this node can
     * be shippable. For each node, update the context (add fields if
     * necessary) so that decision whether to FQS the query or not can be made.
     * Every node which has a result is checked to see if the result type of that
     * expression is shippable.
     */
    switch(nodeTag(node))
    {
        /* Constants are always shippable */
        case T_Const:
            pgxc_set_exprtype_shippability(exprType(node), sc_context);
            break;

            /*
             * For placeholder nodes the shippability of the node, depends upon the
             * expression which they refer to. It will be checked separately, when
             * that expression is encountered.
             */
        case T_CaseTestExpr:
            pgxc_set_exprtype_shippability(exprType(node), sc_context);
            break;

            /*
             * record_in() function throws error, thus requesting a result in the
             * form of anonymous record from datanode gets into error. Hence, if the
             * top expression of a target entry is ROW(), it's not shippable.
             */
        case T_TargetEntry:
        {
            TargetEntry *tle = (TargetEntry *)node;
            if (tle->expr)
            {
                char typtype = get_typtype(exprType((Node *)tle->expr));
                if (!typtype || typtype == TYPTYPE_PSEUDO)
                    pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);
            }
        }
        break;

        case T_SortGroupClause:
            if (sc_context->sc_for_expr)
                pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);
            break;

        case T_CoerceViaIO:
        {
            CoerceViaIO        *cvio = (CoerceViaIO *)node;
            Oid                input_type = exprType((Node *)cvio->arg);
            Oid                output_type = cvio->resulttype;
            CoercionContext    cc;

            cc = cvio->coerceformat == COERCE_IMPLICIT_CAST ? COERCION_IMPLICIT :
                COERCION_EXPLICIT;

            /* now we decide to push down the const value type cast */
            if (COERCION_IMPLICIT == cc && IsA(cvio->arg, Const))
            {

            }
            else if (!can_coerce_type(1, &input_type, &output_type, cc))
            {
                /*
                 * Internally we use IO coercion for types which do not have casting
                 * defined for them e.g. cstring::date. If such casts are sent to
                 * the datanode, those won't be accepted. Hence such casts are
                 * unshippable. Since it will be shown as an explicit cast.
                 */
                pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);
            }
            pgxc_set_exprtype_shippability(exprType(node), sc_context);
        }
        break;
        /*
         * Nodes, which are shippable if the tree rooted under these nodes is
         * shippable
         */
        case T_CoerceToDomainValue:
            /*
             * PGXCTODO: mostly, CoerceToDomainValue node appears in DDLs,
             * do we handle DDLs here?
             */
        case T_FieldSelect:
        case T_NamedArgExpr:
        case T_RelabelType:
        case T_BoolExpr:
            /*
             * PGXCTODO: we might need to take into account the kind of boolean
             * operator we have in the quals and see if the corresponding
             * function is immutable.
             */
        case T_ArrayCoerceExpr:
        case T_ConvertRowtypeExpr:
        case T_CaseExpr:
        case T_ArrayExpr:
        case T_RowExpr:
        case T_CollateExpr:
        case T_CoalesceExpr:
        case T_XmlExpr:
        case T_NullTest:
        case T_BooleanTest:
        case T_CoerceToDomain:
            pgxc_set_exprtype_shippability(exprType(node), sc_context);
            break;

        case T_List:
            break;
            
        case T_RangeTblRef:
            {
                /* Check whether we are in a subquery, if we are check the shippable of the subquery. */
                pgxc_is_rte_subquery_shippable(node, sc_context);
            }
            break;

        case T_ArrayRef:
            /*
             * When multiple values of of an array are updated at once
             * FQS planner cannot yet handle SQL representation correctly.
             * So disable FQS in this case and let standard planner manage it.
             */
        case T_FieldStore:
            /*
             * PostgreSQL deparsing logic does not handle the FieldStore
             * for more than one fields (see processIndirection()). So, let's
             * handle it through standard planner, where whole row will be
             * constructed.
             */
        case T_SetToDefault:
            /*
             * PGXCTODO: we should actually check whether the default value to
             * be substituted is shippable to the Datanode. Some cases like
             * nextval() of a sequence can not be shipped to the Datanode, hence
             * for now default values can not be shipped to the Datanodes
             */
            pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);
            pgxc_set_exprtype_shippability(exprType(node), sc_context);
            break;

        case T_Var:
        {
            Var    *var = (Var *)node;
            /*
             * if a subquery references an upper level variable, that query is
             * not shippable, if shipped alone.
             */
            if (var->varlevelsup > sc_context->sc_max_varlevelsup)
                sc_context->sc_max_varlevelsup = var->varlevelsup;
            pgxc_set_exprtype_shippability(exprType(node), sc_context);
        }
        break;

        case T_Param:
        {
            Param *param = (Param *)node;
            /* PGXCTODO: Can we handle internally generated parameters? */
            if (param->paramkind != PARAM_EXTERN)
                pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);
            pgxc_set_exprtype_shippability(exprType(node), sc_context);
        }
        break;

        case T_CurrentOfExpr:
        {
            /*
             * Ideally we should not see CurrentOf expression here, it
             * should have been replaced by the CTID = ? expression. But
             * still, no harm in shipping it as is.
             */
            pgxc_set_exprtype_shippability(exprType(node), sc_context);
        }
        break;

        case T_SQLValueFunction:
            /*
             * XXX PG10MERGE: Do we really need to do any checks here?
             * Shouldn't all SQLValueFunctions be shippable?
             */
            pgxc_set_exprtype_shippability(exprType(node), sc_context);
            break;

        case T_NextValueExpr:
            /*
             * We must not FQS NextValueExpr since it could be used for
             * distribution key and it should get mapped to the correct
             * datanode.
             */
            pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);
            break;

        case T_Aggref:
        {
            Aggref *aggref = (Aggref *)node;
            /*
             * An aggregate is completely shippable to the Datanode, if the
             * whole group resides on that Datanode. This will be clear when
             * we see the GROUP BY clause.
             * agglevelsup is minimum of variable's varlevelsup, so we will
             * set the sc_max_varlevelsup when we reach the appropriate
             * VARs in the tree.
             */
            pgxc_set_shippability_reason(sc_context, SS_HAS_AGG_EXPR);
            /*
             * If a stand-alone expression to be shipped, is an
             * 1. aggregate with ORDER BY, DISTINCT directives, it needs all
             * the qualifying rows
             * 2. aggregate without collection function
             * 3. (PGXCTODO:)aggregate with polymorphic transition type, the
             *    the transition type needs to be resolved to correctly interpret
             *    the transition results from Datanodes.
             * Hence, such an expression can not be shipped to the datanodes.
             */
            if (aggref->aggorder ||
                aggref->aggdistinct ||
                aggref->agglevelsup ||
                !aggref->agghas_collectfn ||
                IsPolymorphicType(aggref->aggtrantype))
                pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);

            pgxc_set_exprtype_shippability(exprType(node), sc_context);
        }
        break;

        case T_FuncExpr:
        {
            FuncExpr    *funcexpr = (FuncExpr *)node;
            /*
             * PGXC_FQS_TODO: it's too restrictive not to ship non-immutable
             * functions to the Datanode. We need a better way to see what
             * can be shipped to the Datanode and what can not be.
             */
            if (!pgxc_is_func_shippable(funcexpr->funcid))
			{
				/* Ship insert if function doesn't return set */
				if (sc_context->sc_query &&
					sc_context->sc_query->commandType == CMD_INSERT &&
					!(funcexpr->funcretset && sc_context->sc_for_expr))
				{
					pgxc_set_shippability_reason(sc_context, SS_NEED_FUNC_REWRITE);
				}
				else
                pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);
			}

            if (funcexpr->funcretset && sc_context->sc_for_expr)
                pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);

            pgxc_set_exprtype_shippability(exprType(node), sc_context);
        }
        break;

        case T_OpExpr:
        case T_DistinctExpr:    /* struct-equivalent to OpExpr */
        case T_NullIfExpr:        /* struct-equivalent to OpExpr */
        {
            /*
             * All of these three are structurally equivalent to OpExpr, so
             * cast the node to OpExpr and check if the operator function is
             * immutable. See PGXC_FQS_TODO item for FuncExpr.
             */
            OpExpr *op_expr = (OpExpr *)node;
            Oid        opfuncid = OidIsValid(op_expr->opfuncid) ?
                op_expr->opfuncid : get_opcode(op_expr->opno);
            if (!OidIsValid(opfuncid) ||
                !pgxc_is_func_shippable(opfuncid))
                pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);

            pgxc_set_exprtype_shippability(exprType(node), sc_context);
        }
        break;

        case T_ScalarArrayOpExpr:
        {
            /*
             * Check if the operator function is shippable to the Datanode
             * PGXC_FQS_TODO: see immutability note for FuncExpr above
             */
            ScalarArrayOpExpr *sao_expr = (ScalarArrayOpExpr *)node;
            Oid        opfuncid = OidIsValid(sao_expr->opfuncid) ?
                sao_expr->opfuncid : get_opcode(sao_expr->opno);
            if (!OidIsValid(opfuncid) ||
                !pgxc_is_func_shippable(opfuncid))
                pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);
        }
        break;

        case T_RowCompareExpr:
        case T_MinMaxExpr:
        {
            /*
             * PGXCTODO should we be checking the comparision operator
             * functions as well, as we did for OpExpr OR that check is
             * unnecessary. Operator functions are always shippable?
             * Otherwise this node should be treated similar to other
             * "shell" nodes.
             */
            pgxc_set_exprtype_shippability(exprType(node), sc_context);
        }
        break;

        case T_Query:
        {
            bool   tree_examine_ret = false;
            Query *query = (Query *)node;
            ExecNodes *exec_nodes = NULL;

            /* A stand-alone expression containing Query is not shippable */
            if (sc_context->sc_for_expr)
            {
                pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);
                break;
            }
            /*
             * We are checking shippability of whole query, go ahead. The query
             * in the context should be same as the query being checked
             */
            Assert(query == sc_context->sc_query);

            /* CREATE TABLE AS is not supported in FQS */
            if (query->commandType == CMD_UTILITY &&
                IsA(query->utilityStmt, CreateTableAsStmt))
                pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);

            if (query->hasRecursive)
                pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);

            /* Queries with FOR UPDATE/SHARE can't be shipped */
        //    if (query->hasForUpdate || query->rowMarks)
            //    pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);

            /*
             * If the query needs Coordinator for evaluation or the query can be
             * completed on Coordinator itself, we don't ship it to the Datanode
             */
            if (pgxc_query_needs_coord(query))
                pgxc_set_shippability_reason(sc_context, SS_NEEDS_COORD);

            /* PGXCTODO: It should be possible to look at the Query and find out
             * whether it can be completely evaluated on the Datanode just like SELECT
             * queries. But we need to be careful while finding out the Datanodes to
             * execute the query on, esp. for the result relations. If one happens to
             * remove/change this restriction, make sure you change
             * pgxc_FQS_get_relation_nodes appropriately.
             * For now DMLs with single rtable entry are candidates for FQS
             */
            if (query->commandType != CMD_SELECT && list_length(query->rtable) > 1)
            {
                /*
				 * Try to shipping insert with multiple rtables. Skip FQS if it
				 * contains subquery.
				 */
                if(query->commandType == CMD_INSERT && query->onConflict)
                {
                    ListCell *cell;
                    
                    foreach(cell, query->rtable)
                    {
                        RangeTblEntry *tbl = lfirst(cell);

                        if(tbl->rtekind == RTE_SUBQUERY)
                        {
                            pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);
                            break;
                        }
                    }

                    if (query->hasSubLinks)
                    {
                        pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);
                    }
                }
                else
                {
                    pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);
                }
            }

            /*
             * In following conditions query is shippable when there is only one
             * Datanode involved
             * 1. the query has aggregagtes without grouping by distribution
             *    column
             * 2. the query has window functions
             * 3. the query has ORDER BY clause
             * 4. the query has Distinct clause without distribution column in
             *    distinct clause
             * 5. the query has limit and offset clause
             */
            if (query->hasWindowFuncs || query->sortClause ||
                query->limitOffset || query->limitCount)
                pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);

            /*
             * Presence of aggregates or having clause, implies grouping. In
             * such cases, the query won't be shippable unless 1. there is only
             * a single node involved 2. GROUP BY clause has distribution column
             * in it. In the later case aggregates for a given group are entirely
             * computable on a single datanode, because all the rows
             * participating in particular group reside on that datanode.
             * The distribution column can be of any relation
             * participating in the query. All the rows of that relation with
             * the same value of distribution column reside on same node.
             */
            if ((query->hasAggs || query->havingQual || query->groupClause))
            {    
                /* Check whether the sub query is shippable */
                exec_nodes = pgxc_is_group_subquery_shippable(query, sc_context);
                if (exec_nodes)
                {
                    
                }
                else if(!pgxc_query_has_distcolgrouping(query))
                {
                    pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);
                }
            }
            

            /*
             * If distribution column of any relation is present in the distinct
             * clause, values for that column across nodes will differ, thus two
             * nodes won't be able to produce same result row. Hence in such
             * case, we can execute the queries on many nodes managing to have
             * distinct result.
             */
            if (query->distinctClause && !pgxc_distinct_has_distcol(query))
                pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);

            
            if ((query->commandType == CMD_UPDATE) &&
                    pgxc_targetlist_has_distcol(query))
                pgxc_set_shippability_reason(sc_context, SS_UPDATES_DISTRIBUTION_COLUMN);

#ifdef __TBASE__
            /*
             * Check shippability of triggers on this query. Don't consider
             * TRUNCATE triggers; it's a utility statement and triggers are
             * handled explicitly in ExecuteTruncate()
             */
            if (query->commandType == CMD_UPDATE ||
                query->commandType == CMD_INSERT ||
                query->commandType == CMD_DELETE)
            {
                RangeTblEntry *rte = (RangeTblEntry *)
                    list_nth(query->rtable, query->resultRelation - 1);

                if (!pgxc_check_triggers_shippability(rte->relid,
                                                      query->commandType))
                {
                    query->hasUnshippableTriggers = true;
                    pgxc_set_shippability_reason(sc_context,
                                                 SS_UNSHIPPABLE_TRIGGER);
                }

                /* We have to check update triggers if insert...on conflict do update... */
                if (query->onConflict && query->onConflict->action == ONCONFLICT_UPDATE)
                {
                    if (!pgxc_check_triggers_shippability(rte->relid,
                                      CMD_UPDATE))
                    {
                        query->hasUnshippableTriggers = true;
                        pgxc_set_shippability_reason(sc_context,
                                                     SS_UNSHIPPABLE_TRIGGER);
                    }
                }

                /*
                 * PGXCTODO: For the time being Postgres-XC does not support
                 * global constraints, but once it does it will be necessary
                 * to add here evaluation of the shippability of indexes and
                 * constraints of the relation used for INSERT/UPDATE/DELETE.
                 */
            }
#endif

            /*
             * walk the entire query tree to analyse the query. We will walk the
             * range table, when examining the FROM clause. No need to do it
             * here
             */
            tree_examine_ret = query_tree_walker(query, pgxc_shippability_walker,
                                    sc_context, QTW_IGNORE_RANGE_TABLE );
            if (tree_examine_ret)
            {
                return true;
            }
            else
            {
                exec_nodes = sc_context->sc_exec_nodes;
            }

            /*
             * PGXC_FQS_TODO:
             * There is a subquery in this query, which references Vars in the upper
             * query. For now stop shipping such queries. We should get rid of this
             * condition.
             */
            if (sc_context->sc_max_varlevelsup != 0)
                pgxc_set_shippability_reason(sc_context, SS_VARLEVEL);

            /*
             * Walk the join tree of the query and find the
             * Datanodes needed for evaluating this query
             */
            sc_context->sc_exec_nodes = pgxc_FQS_find_datanodes(query);
            #if 0
            sc_context->sc_exec_nodes = pgxc_merge_exec_nodes(exec_nodes, sc_context->sc_exec_nodes);            
            #endif
        }
        break;

        case T_FromExpr:
        {
            /* We don't expect FromExpr in a stand-alone expression */
            if (sc_context->sc_for_expr)
                pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);

            /*
             * We will examine the jointree of query separately to determine the
             * set of datanodes where to execute the query.
             * If this is an INSERT query with quals, resulting from say
             * conditional rule, we can not handle those in FQS, since there is
             * not SQL representation for such quals.
             */
            if (sc_context->sc_query->commandType == CMD_INSERT &&
                ((FromExpr *)node)->quals)
                pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);

        }
        break;

        case T_WindowFunc:
        {
            WindowFunc *winf = (WindowFunc *)node;
            /*
             * A window function can be evaluated on a Datanode if there is
             * only one Datanode involved.
             */
            pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);

            /*
             * A window function is not shippable as part of a stand-alone
             * expression. If the window function is non-immutable, it can not
             * be shipped to the datanodes.
             */
            if (sc_context->sc_for_expr ||
                !pgxc_is_func_shippable(winf->winfnoid))
                pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);

            pgxc_set_exprtype_shippability(exprType(node), sc_context);
        }
        break;

        case T_WindowClause:
        {
            /*
             * A window function can be evaluated on a Datanode if there is
             * only one Datanode involved.
             */
            pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);

            /*
             * A window function is not shippable as part of a stand-alone
             * expression
             */
            if (sc_context->sc_for_expr)
                pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);
        }
        break;

        case T_JoinExpr:
            /* We don't expect JoinExpr in a stand-alone expression */
            if (sc_context->sc_for_expr)
                pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);

            /*
             * The shippability of join will be deduced while
             * examining the jointree of the query. Nothing to do here
             */
            break;

        case T_SubLink:
        {
            /*
             * We need to walk the tree in sublink to check for its
             * shippability. We need to call pgxc_is_query_shippable() on Query
             * instead of this function so that every subquery gets a different
             * context for itself. We should avoid the default expression walker
             * getting called on the subquery. At the same time we don't want to
             * miss any other member (current or future) of this structure, from
             * being scanned. So, copy the SubLink structure with subselect
             * being NULL and call expression_tree_walker on the copied
             * structure.
             */
            SubLink        sublink = *(SubLink *)node;
            ExecNodes    *sublink_en;
            /*
             * Walk the query and find the nodes where the query should be
             * executed and node distribution. Merge this with the existing
             * node list obtained for other subqueries. If merging fails, we
             * can not ship the whole query.
             */
            if (IsA(sublink.subselect, Query))
                sublink_en = pgxc_is_query_shippable((Query *)(sublink.subselect),
                                                     sc_context->sc_query_level);
            else
                sublink_en = NULL;

            /* PGXCTODO free the old sc_subquery_en. */
            /* If we already know that this query does not have a set of nodes
             * to evaluate on, don't bother to merge again.
             */
            if (!pgxc_test_shippability_reason(sc_context, SS_NO_NODES))
            {
                /*
                 * If this is the first time we are finding out the nodes for
                 * SubLink, we don't have anything to merge, just assign.
                 */
                if (!sc_context->sc_subquery_en)
                    sc_context->sc_subquery_en = sublink_en;
                /*
                 * Merge if only the accumulated SubLink ExecNodes and the
                 * ExecNodes for this subquery are both replicated.
                 */
                else if (sublink_en && IsExecNodesReplicated(sublink_en) &&
                            IsExecNodesReplicated(sc_context->sc_subquery_en))
                {
                    sc_context->sc_subquery_en = pgxc_merge_exec_nodes(sublink_en,
                                                                   sc_context->sc_subquery_en);
                }
                else
                    sc_context->sc_subquery_en = NULL;

                /*
                 * If we didn't find a cumulative ExecNodes, set shippability
                 * reason, so that we don't bother merging future sublinks.
                 */
                if (!sc_context->sc_subquery_en)
                    pgxc_set_shippability_reason(sc_context, SS_NO_NODES);
            }
            else
                Assert(!sc_context->sc_subquery_en);

            /* Check if the type of sublink result is shippable */
            pgxc_set_exprtype_shippability(exprType(node), sc_context);

            /* Wipe out subselect as explained above and walk the copied tree */
            sublink.subselect = NULL;
            return expression_tree_walker((Node *)&sublink, pgxc_shippability_walker,
                                            sc_context);
        }
        break;

        case T_OnConflictExpr:
        {
            return false;
        }

        case T_SubPlan:
        case T_AlternativeSubPlan:
        case T_CommonTableExpr:
        case T_SetOperationStmt:
        case T_PlaceHolderVar:
        case T_AppendRelInfo:
        case T_PlaceHolderInfo:
        case T_WithCheckOption:
        {
            /* PGXCTODO: till we exhaust this list */
            pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);
            /*
             * These expressions are not supported for shippability entirely, so
             * there is no need to walk trees underneath those. If we do so, we
             * might walk the trees with wrong context there.
             */
            return false;
        }
        break;

        case T_GroupingFunc:
            /*
             * Let expression tree walker inspect the arguments. Not sure if
             * that's necessary, as those are just references to grouping
             * expressions of the query (and thus likely examined as part
             * of another node).
             */
            return expression_tree_walker(node, pgxc_shippability_walker,
                                          sc_context);

        default:
            elog(ERROR, "unrecognized node type: %d",
                 (int) nodeTag(node));
            break;
    }

    return expression_tree_walker(node, pgxc_shippability_walker, (void *)sc_context);
}


/*
 * pgxc_query_needs_coord
 * Check if the query needs Coordinator for evaluation or it can be completely
 * evaluated on Coordinator. Return true if so, otherwise return false.
 */
static bool
pgxc_query_needs_coord(Query *query)
{
    /*
     * If the query involves just the catalog tables, and is not an EXEC DIRECT
     * statement, it can be evaluated completely on the Coordinator. No need to
     * involve Datanodes.
     */
    if (pgxc_query_contains_only_pg_catalog(query->rtable))
        return true;

    return false;
}


/*
 * pgxc_is_var_distrib_column
 * Check if given var is a distribution key.
 */
static
bool pgxc_is_var_distrib_column(Var *var, List *rtable)
{// #lizard forgives
    RangeTblEntry   *rte = rt_fetch(var->varno, rtable);
    RelationLocInfo    *rel_loc_info;

    /* distribution column only applies to the relations */
    if (rte->rtekind != RTE_RELATION ||
        rte->relkind != RELKIND_RELATION)
        return false;
    rel_loc_info = GetRelationLocInfo(rte->relid);
    if (!rel_loc_info)
        return false;
#ifdef __COLD_HOT__
    if (var->varattno == rel_loc_info->partAttrNum)
    {
        if (AttributeNumberIsValid(rel_loc_info->secAttrNum))
        {
            return false;
        }
        
        return true;
    }
#else
    if (var->varattno == rel_loc_info->partAttrNum)
        return true;
#endif
    return false;
}

#ifdef __TBASE__
static bool
pgxc_is_shard_in_same_group(Var *var1, Var *var2, List *rtable)
{// #lizard forgives
    bool result = true;
    RangeTblEntry   *rte1 = rt_fetch(var1->varno, rtable);
    RelationLocInfo    *rel_loc_info1 = GetRelationLocInfo(rte1->relid);
    RangeTblEntry   *rte2 = rt_fetch(var2->varno, rtable);
    RelationLocInfo    *rel_loc_info2 = GetRelationLocInfo(rte2->relid);

    if (rel_loc_info1->locatorType == LOCATOR_TYPE_SHARD &&
        rel_loc_info2->locatorType == LOCATOR_TYPE_SHARD)
    {
        if (rel_loc_info1->groupId != rel_loc_info2->groupId ||
            AttributeNumberIsValid(rel_loc_info1->secAttrNum) ||
            AttributeNumberIsValid(rel_loc_info2->secAttrNum))
        {
            result = false;
        }
    }
    else if (rel_loc_info1->locatorType == LOCATOR_TYPE_SHARD &&
             rel_loc_info2->locatorType != LOCATOR_TYPE_SHARD)
    {
        result = false;
    }
    else if (rel_loc_info1->locatorType != LOCATOR_TYPE_SHARD &&
             rel_loc_info2->locatorType == LOCATOR_TYPE_SHARD)
    {
        result = false;
    }

    return result;
}

/*
 * Check is the subquery is simple enough to pushdown to DN
 */
static bool
pgxc_is_simple_subquery(Query *query)
{
	/*
	 * Let's just make sure it's a valid select ...
	 */
	if (!IsA(query, Query) || query->commandType != CMD_SELECT)
		return false;

	/*
	 * Can't currently pushdown a query with setops (unless it's simple UNION
	 * ALL, which is handled by a different code path).
	 */
	if (query->setOperations)
		return false;

	/*
	 * Can't pushdown a subquery involving grouping, aggregation, SRFs,
	 * sorting, limiting, or WITH.
	 */
	if (query->hasWindowFuncs ||
		query->hasTargetSRFs ||
		query->groupClause ||
		query->groupingSets ||
		query->distinctClause ||
		query->hasForUpdate ||
		query->cteList)
		return false;

	/*
	 * Don't pushdown a subquery that has any volatile functions in its
	 * targetlist.  Otherwise we might introduce multiple evaluations of these
	 * functions, if they get copied to multiple places in the upper query,
	 * leading to surprising results.  (Note: the PlaceHolderVar mechanism
	 * doesn't quite guarantee single evaluation; else we could pull up anyway
	 * and just wrap such items in PlaceHolderVars ...)
	 */
	if (contain_volatile_functions((Node *) query->targetList))
		return false;

	return true;
}
#endif

/*
 * Returns whether or not the rtable (and its subqueries)
 * only contain pg_catalog entries.
 */
static bool
pgxc_query_contains_only_pg_catalog(List *rtable)
{
    ListCell *item;

    /* May be complicated. Before giving up, just check for pg_catalog usage */
    foreach(item, rtable)
    {
        RangeTblEntry *rte = (RangeTblEntry *) lfirst(item);

        if (rte->rtekind == RTE_RELATION)
        {
            if (get_rel_namespace(rte->relid) != PG_CATALOG_NAMESPACE)
                return false;
        }
        else if (rte->rtekind == RTE_SUBQUERY &&
                 !pgxc_query_contains_only_pg_catalog(rte->subquery->rtable))
            return false;
    }
    return true;
}

/*
 * Construct ExecNodes for single datanode to fqs
 */
static ExecNodes *
make_FQS_single_node()
{
	ExecNodes       *exec_nodes;
	exec_nodes = makeNode(ExecNodes);
	exec_nodes->accesstype = RELATION_ACCESS_READ_FQS;
	exec_nodes->nodeList = lappend_int(exec_nodes->nodeList, 0);
	return exec_nodes;
}

/*
 * pgxc_is_query_shippable
 * This function calls the query walker to analyse the query to gather
 * information like  Constraints under which the query can be shippable, nodes
 * on which the query is going to be executed etc.
 * Based on the information gathered, it decides whether the query can be
 * executed on Datanodes directly without involving Coordinator.
 * If the query is shippable this routine also returns the nodes where the query
 * should be shipped. If the query is not shippable, it returns NULL.
 */
ExecNodes *
pgxc_is_query_shippable(Query *query, int query_level)
{
	Shippability_context sc_context;
	ExecNodes	*exec_nodes;
	bool		canShip = true;
	Bitmapset	*shippability;

	memset(&sc_context, 0, sizeof(sc_context));
	/* let's assume that by default query is shippable */
	sc_context.sc_query = query;
	sc_context.sc_query_level = query_level;
	sc_context.sc_for_expr = false;

	/*
	 * We might have already decided not to ship the query to the Datanodes, but
	 * still walk it anyway to find out if there are any subqueries which can be
	 * shipped.
	 */
	pgxc_shippability_walker((Node *)query, &sc_context);

	exec_nodes = sc_context.sc_exec_nodes;

	/* For single datanode and select command, if we don't need coord
	 * and exec_nodes exists, return it directly. But if exec_nodes is
	 * NULL we make exec_nodes for FQS;
	 */
	if (!bms_is_member(SS_NEEDS_COORD, sc_context.sc_shippability))
	{
		if (NumDataNodes == 1 && query->commandType == CMD_SELECT)
		{
			if (exec_nodes)
		return exec_nodes;
			return make_FQS_single_node();
		}
	}
		
	/*
	 * The shippability context contains two ExecNodes, one for the subLinks
	 * involved in the Query and other for the relation involved in FromClause.
	 * They are computed at different times while scanning the query. Merge both
	 * of them if they are both replicated. If query doesn't have SubLinks, we
	 * don't need to consider corresponding ExecNodes.
	 * PGXC_FQS_TODO:
	 * Merge the subquery ExecNodes if both of them are replicated.
	 * The logic to merge node lists with other distribution
	 * strategy is not clear yet.
	 */
	if (query->hasSubLinks)
	{

#ifdef __TBASE__
		int num_fromclause_nodes = 0;
		int num_sublink_nodes = 0;

		/* Get number of DN nodes for Main Query result */
		if (exec_nodes && exec_nodes->nodeList)
		{
			num_fromclause_nodes = list_length(exec_nodes->nodeList);
		}

		/* Get number of DN nodes for Sublink result */
		if (sc_context.sc_subquery_en && sc_context.sc_subquery_en->nodeList)
		{
			num_sublink_nodes = list_length(sc_context.sc_subquery_en->nodeList);
		}

		/*
		 * Try to merge sublink nodelist only if:
		 * XXX Only cover CMD_SELECT
		 * XXX Both main query and sublink results got single DN node
		 * XXX With same column distributed type
		 */
		if (enable_subquery_shipping &&
			exec_nodes && sc_context.sc_subquery_en &&
			query->commandType == CMD_SELECT &&
			IsExecNodesColumnDistributed(exec_nodes) &&
			IsExecNodesColumnDistributed(sc_context.sc_subquery_en) &&
			exec_nodes->baselocatortype == sc_context.sc_subquery_en->baselocatortype &&
			(num_fromclause_nodes == 1) && (num_sublink_nodes == 1))
		{
			exec_nodes = pgxc_merge_exec_nodes(exec_nodes, sc_context.sc_subquery_en);
		}
		/* Fall back to PGXC logic that only try with replicated type */
#endif
		else if (exec_nodes && IsExecNodesReplicated(exec_nodes) &&
				 sc_context.sc_subquery_en &&
				 IsExecNodesReplicated(sc_context.sc_subquery_en))
		{
			exec_nodes = pgxc_merge_exec_nodes(exec_nodes, sc_context.sc_subquery_en);
		}
		else
		{
			exec_nodes = NULL;
		}
	}

	/*
	 * Look at the information gathered by the walker in Shippability_context and that
	 * in the Query structure to decide whether we should ship this query
	 * directly to the Datanode or not
	 */

	/*
	 * If the planner was not able to find the Datanodes to the execute the
	 * query, the query is not completely shippable. So, return NULL
	 */
	if (!exec_nodes)
		return NULL;

    /* Copy the shippability reasons. We modify the copy for easier handling.
     * The original can be saved away */
    shippability = bms_copy(sc_context.sc_shippability);

    /*
     * If the query has an expression which renders the shippability to single
     * node, and query needs to be shipped to more than one node, it can not be
     * shipped
     */
    if (bms_is_member(SS_NEED_SINGLENODE, shippability))
    {
        /*
         * if nodeList has no nodes, it ExecNodes will have other means to know
         * the nodes where to execute like distribution column expression. We
         * can't tell how many nodes the query will be executed on, hence treat
         * that as multiple nodes.
         */
        if (list_length(exec_nodes->nodeList) > 1 ||
            !((exec_nodes->baselocatortype == LOCATOR_TYPE_HASH
                || exec_nodes->baselocatortype == LOCATOR_TYPE_SHARD)
                && (exec_nodes->en_expr || list_length(exec_nodes->nodeList) == 1)))
            canShip = false;

        /* We handled the reason here, reset it */
        shippability = bms_del_member(shippability, SS_NEED_SINGLENODE);
    }

    /*
     * If HAS_AGG_EXPR is set but NEED_SINGLENODE is not set, it means the
     * aggregates are entirely shippable, so don't worry about it.
     */
    shippability = bms_del_member(shippability, SS_HAS_AGG_EXPR);

	/*
	 * If an insert sql command whose distribute key's value is a
	 * function, we allow it to be shipped to datanode. But we must
	 * must know the function's result before real execute. So set
	 * the flag to identify rewrite in ExecutePlan.
	 */
	if (bms_is_member(SS_NEED_FUNC_REWRITE, shippability) &&
		(IsLocatorColumnDistributed(exec_nodes->baselocatortype) ||
		IsLocatorDistributedByValue(exec_nodes->baselocatortype)))
	{
		exec_nodes->need_rewrite = true;
		shippability = bms_del_member(shippability, SS_NEED_FUNC_REWRITE);
	}
	else
		exec_nodes->need_rewrite = false;

    /* Can not ship the query for some reason */
    if (!bms_is_empty(shippability))
        canShip = false;

    /* Always keep this at the end before checking canShip and return */
    if (!canShip && exec_nodes)
        FreeExecNodes(&exec_nodes);
    /* If query is to be shipped, we should know where to execute the query */
    Assert (!canShip || exec_nodes);

    bms_free(shippability);
    shippability = NULL;

    return exec_nodes;
}


/*
 * pgxc_is_expr_shippable
 * Check whether the given expression can be shipped to datanodes.
 *
 * Note on has_aggs
 * The aggregate expressions are not shippable if they can not be completely
 * evaluated on a single datanode. But this function does not have enough
 * context to determine the set of datanodes where the expression will be
 * evaluated. Hence, the caller of this function can handle aggregate
 * expressions, it passes a non-NULL value for has_aggs. This function returns
 * whether the expression has any aggregates or not through this argument. If a
 * caller passes NULL value for has_aggs, this function assumes that the caller
 * can not handle the aggregates and deems the expression has unshippable.
 */
bool
pgxc_is_expr_shippable(Expr *node, bool *has_aggs)
{
    Shippability_context sc_context;

    /* Create the FQS context */
    memset(&sc_context, 0, sizeof(sc_context));
    sc_context.sc_query = NULL;
    sc_context.sc_query_level = 0;
    sc_context.sc_for_expr = true;

    /* Walk the expression to check its shippability */
    pgxc_shippability_walker((Node *)node, &sc_context);

    /*
     * If caller is interested in knowing, whether the expression has aggregates
     * let the caller know about it. The caller is capable of handling such
     * expressions. Otherwise assume such an expression as not shippable.
     */
    if (has_aggs)
        *has_aggs = pgxc_test_shippability_reason(&sc_context, SS_HAS_AGG_EXPR);
    else if (pgxc_test_shippability_reason(&sc_context, SS_HAS_AGG_EXPR))
        return false;
    /* Done with aggregate expression shippability. Delete the status */
    pgxc_reset_shippability_reason(&sc_context, SS_HAS_AGG_EXPR);

    /* If there are reasons why the expression is unshippable, return false */
    if (!bms_is_empty(sc_context.sc_shippability))
        return false;

    /* If nothing wrong found, the expression is shippable */
    return true;
}


/*
 * pgxc_is_func_shippable
 * Determine if a function is shippable
 */
static bool
pgxc_is_func_shippable(Oid funcid)
{// #lizard forgives    
#ifdef __TBASE__
    bool result = false;
    
    switch(funcid)
    {
        /* convert function */
        case  395 : /* array_to_string */
        case  384 : /* array_to_string */
        case 1171 : /* date_part       */
        case 1717 : /* convert_to       */
        case 1770 : /* to_char           */
        case 1772 : /* to_char           */
        case 1773 : /* to_char           */
        case 1774 : /* to_char           */
        case 1775 : /* to_char           */
        case 1776 : /* to_char           */
        case 1777 : /* to_number       */
        case 1778 : /* to_timestamp    */
        case 1780 : /* to_date           */
        case 1768 : /* to_char           */
        case 2049 : /* to_char           */
        case 3153 : /* array_to_json   */
        case 3154 : /* array_to_json   */
        case 3155 : /* row_to_json       */
        case 3156 : /* row_to_json       */
        case 3176 : /* to_json           */
        case 3714 : /* ts_token_type   */
        case 3749 : /* to_tsvector       */
        case 3750 : /* to_tsquery       */
        case 3751 : /* plainto_tsquery */

        case 1299 : /* now() */
        case 1174 : /* convert date to timestamp with time zone */
        case 2024 : /* convert date to timestamp */
        case 2019 : /* convert timestamp with time zone to time */
        case 1178 : /* convert timestamp with time zone to date */
        case 2027 : /* convert timestamp with time zone to timestamp */
        case 2029 : /* convert timestamp to date */
        case 2028 : /* convert timestamp to timestamp with time zone */
        case 1316 : /* convert timestamp to time */

        case 3438 : /* convert text to timestamp with out time zone */
        case 3449 : /* convert text to date compatible with oracle */
        case 3437 : /* convert text to timestamp with time zone */
        case 3434 : /* convert text to timestamp with time zone */

        /* timestamp compare function */        
        case 2338 : /* date_lt_timestamp    */
        case 2339 : /* date_le_timestamp    */
        case 2340 : /* date_eq_timestamp    */
        case 2341 : /* date_gt_timestamp    */
        case 2342 : /* date_ge_timestamp    */
        case 2343 : /* date_ne_timestamp    */
        case 2344 : /* date_cmp_timestamp   */
        case 2351 : /* date_lt_timestamptz  */
        case 2352 : /* date_le_timestamptz  */
        case 2353 : /* date_eq_timestamptz  */
        case 2354 : /* date_gt_timestamptz  */
        case 2355 : /* date_ge_timestamptz  */
        case 2356 : /* date_ne_timestamptz  */
        case 2357 : /* date_cmp_timestamptz */
        case 1189 : /* timestamptz_pl_interval   */
        case 1190 : /* timestamptz_mi_interval   */
        case 2377 : /* timestamptz_lt_date       */
        case 2378 : /* timestamptz_le_date       */
        case 2379 : /* timestamptz_eq_date       */
        case 2380 : /* timestamptz_gt_date       */
        case 2381 : /* timestamptz_ge_date       */
        case 2382 : /* timestamptz_ne_date       */
        case 2383 : /* timestamptz_cmp_date      */
        case 2520 : /* timestamp_lt_timestamptz  */
        case 2521 : /* timestamp_le_timestamptz  */
        case 2522 : /* timestamp_eq_timestamptz  */
        case 2523 : /* timestamp_gt_timestamptz  */
        case 2524 : /* timestamp_ge_timestamptz  */
        case 2525 : /* timestamp_ne_timestamptz  */
        case 2526 : /* timestamp_cmp_timestamptz */
        case 2527 : /* timestamptz_lt_timestamp  */
        case 2528 : /* timestamptz_le_timestamp  */
        case 2529 : /* timestamptz_eq_timestamp  */
        case 2530 : /* timestamptz_gt_timestamp  */
        case 2531 : /* timestamptz_ge_timestamp  */
        case 2532 : /* timestamptz_ne_timestamp  */
        case 2533 : /* timestamptz_cmp_timestamp */
        {
            result = true;
            break;
        }
            
        default:
        {
			result = (func_volatile(funcid) != PROVOLATILE_VOLATILE);
            break;
        }
    }
    return result;
#else
    /*
     * For the time being a function is thought as shippable
     * only if it is immutable.
     */
    return (func_volatile(funcid) == PROVOLATILE_IMMUTABLE);    
#endif    
}


/*
 * pgxc_find_dist_equijoin_qual
 * Check equijoin conditions on given relations
 */
static Expr *
pgxc_find_dist_equijoin_qual(Relids varnos_1,
        Relids varnos_2, Oid distcol_type, Node *quals, List *rtable)
{// #lizard forgives
    List        *lquals;
    ListCell    *qcell;

    /* If no quals, no equijoin */
    if (!quals)
        return NULL;
    /*
     * Make a copy of the argument bitmaps, it will be modified by
     * bms_first_member().
     */
    varnos_1 = bms_copy(varnos_1);
    varnos_2 = bms_copy(varnos_2);

    if (!IsA(quals, List))
        lquals = make_ands_implicit((Expr *)quals);
    else
        lquals = (List *)quals;

    foreach(qcell, lquals)
    {
        Expr *qual_expr = (Expr *)lfirst(qcell);
        OpExpr *op;
        Var *lvar;
        Var *rvar;

        if (!IsA(qual_expr, OpExpr))
            continue;
        op = (OpExpr *)qual_expr;
        /* If not a binary operator, it can not be '='. */
        if (list_length(op->args) != 2)
            continue;

        /*
         * Check if both operands are Vars, if not check next expression */
        if (IsA(linitial(op->args), Var) && IsA(lsecond(op->args), Var))
        {
            lvar = (Var *)linitial(op->args);
            rvar = (Var *)lsecond(op->args);
        }
        else
#ifndef _PG_REGRESS_
        /*
         * handle IMPLICIT_CAST case here
         */
        {
            Node *left_arg = (Node *)linitial(op->args);
            Node *right_arg = (Node *)lsecond(op->args);
            
            lvar = (Var *)get_var_from_arg(left_arg);
            rvar = (Var *)get_var_from_arg(right_arg);

            if (!lvar || !rvar)
            {
                continue;
            }
        }
#else
            continue;
#endif

        /*
         * If the data types of both the columns are not same, continue. Hash
         * and Modulo of a the same bytes will be same if the data types are
         * same. So, only when the data types of the columns are same, we can
         * ship a distributed JOIN to the Datanodes
         */
        if (exprType((Node *)lvar) != exprType((Node *)rvar))
            continue;

        /* if the vars do not correspond to the required varnos, continue. */
        if ((bms_is_member(lvar->varno, varnos_1) && bms_is_member(rvar->varno, varnos_2)) ||
            (bms_is_member(lvar->varno, varnos_2) && bms_is_member(rvar->varno, varnos_1)))
        {
            if (!pgxc_is_var_distrib_column(lvar, rtable) ||
                !pgxc_is_var_distrib_column(rvar, rtable))
                continue;
#ifdef __TBASE__
            /* join shard tables should in same group */
            if (!pgxc_is_shard_in_same_group(lvar, rvar, rtable))
            {
                continue;
            }
#endif
        }
        else
            continue;
        /*
         * If the operator is not an assignment operator, check next
         * constraint. An operator is an assignment operator if it's
         * mergejoinable or hashjoinable. Beware that not every assignment
         * operator is mergejoinable or hashjoinable, so we might leave some
         * oportunity. But then we have to rely on the opname which may not
         * be something we know to be equality operator as well.
         */
        if (!op_mergejoinable(op->opno, exprType((Node *)lvar)) &&
            !op_hashjoinable(op->opno, exprType((Node *)lvar)))
            continue;
        /* Found equi-join condition on distribution columns */
        return qual_expr;
    }
    return NULL;
}

#ifdef __TBASE__
static List*
pgxc_find_dist_equi_nodes(Relids varnos_1,
        Relids varnos_2, Oid distcol_type, Node *quals, List *rtable)
{// #lizard forgives
    List        *lquals;
    ListCell    *qcell;

    /* If no quals, no equijoin */
    if (!quals)
        return NULL;
    /*
     * Make a copy of the argument bitmaps, it will be modified by
     * bms_first_member().
     */
    varnos_1 = bms_copy(varnos_1);
    varnos_2 = bms_copy(varnos_2);

    if (!IsA(quals, List))
        lquals = make_ands_implicit((Expr *)quals);
    else
        lquals = (List *)quals;

    foreach(qcell, lquals)
    {
        Expr *qual_expr = (Expr *)lfirst(qcell);
        OpExpr *op;
        Var *var;
        Expr *const_expr;

        if (!IsA(qual_expr, OpExpr))
            continue;
        op = (OpExpr *)qual_expr;
        /* If not a binary operator, it can not be '='. */
        if (list_length(op->args) != 2)
            continue;

        {
            Node *left_arg = (Node *)linitial(op->args);
            Node *right_arg = (Node *)lsecond(op->args);

            if (IsA(left_arg, RelabelType))
            {
                RelabelType *rt = (RelabelType *)left_arg;
                if (rt->relabelformat == COERCE_IMPLICIT_CAST)
                {
                    left_arg = (Node *)rt->arg;
                }
            }

            if (IsA(right_arg, RelabelType))
            {
                RelabelType *rt = (RelabelType *)right_arg;
                if (rt->relabelformat == COERCE_IMPLICIT_CAST)
                {
                    right_arg = (Node *)rt->arg;
                }
            }

            if (IsA(left_arg, Var) && IsA(right_arg, Const))
            {
                var = (Var *)left_arg;
                const_expr = (Expr *)right_arg;
            }
            else if (IsA(left_arg, Const) && IsA(right_arg, Var))
            {
                var = (Var *)right_arg;
                const_expr = (Expr *)left_arg;
            }
            else
            {
                continue;
            }
        }

        /* if the vars do not correspond to the required varnos, continue. */
        if ((bms_is_member(var->varno, varnos_1) || bms_is_member(var->varno, varnos_2)))
        {
            if (!pgxc_is_var_distrib_column(var, rtable))
                continue;
        }
        else
            continue;
        /*
         * If the operator is not an assignment operator, check next
         * constraint. An operator is an assignment operator if it's
         * mergejoinable or hashjoinable. Beware that not every assignment
         * operator is mergejoinable or hashjoinable, so we might leave some
         * oportunity. But then we have to rely on the opname which may not
         * be something we know to be equality operator as well.
         */
        if (!op_mergejoinable(op->opno, exprType((Node *)var)) &&
            !op_hashjoinable(op->opno, exprType((Node *)var)))
            continue;
        /* Found equi-qual condition on distribution columns, get executed nodelist */
        if (const_expr)
        {
            RangeTblEntry   *rte = rt_fetch(var->varno, rtable);
            RelationLocInfo    *rel_loc_info = GetRelationLocInfo(rte->relid);
            Oid        disttype = get_atttype(rte->relid, rel_loc_info->partAttrNum);
            int32    disttypmod = get_atttypmod(rte->relid, rel_loc_info->partAttrNum);

            const_expr = (Expr *)coerce_to_target_type(NULL,
                                                    (Node *)const_expr,
                                                    exprType((Node *)const_expr),
                                                    disttype, disttypmod,
                                                    COERCION_ASSIGNMENT,
                                                    COERCE_IMPLICIT_CAST, -1);

            const_expr = (Expr *)eval_const_expressions(NULL,(Node *)const_expr);

            if (const_expr && IsA(const_expr, Const))
            {
                Const *con = (Const *)const_expr;
                
                ExecNodes *exec_nodes = GetRelationNodes(rel_loc_info, con->constvalue,
                                            con->constisnull,
                                            0, true,
                                            RELATION_ACCESS_READ);
                return exec_nodes->nodeList;
            }
        }
    }
    return NULL;
}

#endif

/*
 * pgxc_merge_exec_nodes
 * The routine combines the two exec_nodes passed such that the resultant
 * exec_node corresponds to the JOIN of respective relations.
 * If both exec_nodes can not be merged, it returns NULL.
 */
static ExecNodes *
pgxc_merge_exec_nodes(ExecNodes *en1, ExecNodes *en2)
{// #lizard forgives
    ExecNodes    *merged_en = makeNode(ExecNodes);
    ExecNodes    *tmp_en;

    /* If either of exec_nodes are NULL, return the copy of other one */
    if (!en1)
    {
        tmp_en = copyObject(en2);
        return tmp_en;
    }
    if (!en2)
    {
        tmp_en = copyObject(en1);
        return tmp_en;
    }

    /* Following cases are not handled in this routine */
    /* PGXC_FQS_TODO how should we handle table usage type? */
    if (en1->primarynodelist || en2->primarynodelist ||
        en1->en_expr || en2->en_expr ||
        OidIsValid(en1->en_relid) || OidIsValid(en2->en_relid) ||
        (en1->accesstype != RELATION_ACCESS_READ && en1->accesstype != RELATION_ACCESS_READ_FQS) ||
        (en2->accesstype != RELATION_ACCESS_READ && en2->accesstype != RELATION_ACCESS_READ_FQS))
        return NULL;

    if (IsExecNodesReplicated(en1) &&
        IsExecNodesReplicated(en2))
    {
        /*
         * Replicated/replicated join case
         * Check that replicated relation is not disjoint
         * with initial relation which is also replicated.
         * If there is a common portion of the node list between
         * the two relations, other rtables have to be checked on
         * this restricted list.
         */
        merged_en->nodeList = list_intersection_int(en1->nodeList,
                                                    en2->nodeList);
        merged_en->baselocatortype = LOCATOR_TYPE_REPLICATED;
        if (!merged_en->nodeList)
            FreeExecNodes(&merged_en);
        return merged_en;
    }

    if (IsExecNodesReplicated(en1) &&
        IsExecNodesColumnDistributed(en2))
    {
        List    *diff_nodelist = NULL;
        /*
         * Replicated/distributed join case.
         * Node list of distributed table has to be included
         * in node list of replicated table.
         */
        diff_nodelist = list_difference_int(en2->nodeList, en1->nodeList);
        /*
         * If the difference list is not empty, this means that node list of
         * distributed table is not completely mapped by node list of replicated
         * table, so go through standard planner.
         */
        if (diff_nodelist)
            FreeExecNodes(&merged_en);
        else
        {
            merged_en->nodeList = list_copy(en2->nodeList);
            merged_en->baselocatortype = LOCATOR_TYPE_DISTRIBUTED;
        }
        return merged_en;
    }

    if (IsExecNodesColumnDistributed(en1) &&
        IsExecNodesReplicated(en2))
    {
        List *diff_nodelist = NULL;
        /*
         * Distributed/replicated join case.
         * Node list of distributed table has to be included
         * in node list of replicated table.
         */
        diff_nodelist = list_difference_int(en1->nodeList, en2->nodeList);

        /*
         * If the difference list is not empty, this means that node list of
         * distributed table is not completely mapped by node list of replicated
             * table, so go through standard planner.
         */
        if (diff_nodelist)
            FreeExecNodes(&merged_en);
        else
        {
            merged_en->nodeList = list_copy(en1->nodeList);
            merged_en->baselocatortype = LOCATOR_TYPE_DISTRIBUTED;
        }
        return merged_en;
    }

    if (IsExecNodesColumnDistributed(en1) &&
        IsExecNodesColumnDistributed(en2))
    {
        /*
         * Distributed/distributed case
         * If the caller has suggested that this is an equi-join between two
         * distributed results, check that they have the same nodes in the distribution
         * node list. The caller is expected to fully decide whether to merge
         * the nodes or not.
         */
        if (!list_difference_int(en1->nodeList, en2->nodeList) &&
            !list_difference_int(en2->nodeList, en1->nodeList))
        {
            merged_en->nodeList = list_copy(en1->nodeList);
            if (en1->baselocatortype == en2->baselocatortype)
                merged_en->baselocatortype = en1->baselocatortype;
            else
                merged_en->baselocatortype = LOCATOR_TYPE_DISTRIBUTED;
        }
        else
            FreeExecNodes(&merged_en);
        return merged_en;
    }

    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("Postgres-XC does not support this distribution type yet"),
             errdetail("The feature is not currently supported")));

    /* Keep compiler happy */
    return NULL;
}


/*
 * pgxc_is_join_reducible
 * The shippability of JOIN is decided in following steps
 * 1. Are the JOIN conditions shippable?
 *     For INNER JOIN it's possible to apply some of the conditions at the
 *     Datanodes and others at coordinator. But for other JOINs, JOIN conditions
 *     decide which tuples on the OUTER side are appended with NULL columns from
 *     INNER side, we need all the join conditions to be shippable for the join to
 *     be shippable.
 * 2. Do the JOIN conditions have quals that will make it shippable?
 *     When both sides of JOIN are replicated, irrespective of the quals the JOIN
 *     is shippable.
 *     INNER joins between replicated and distributed relation are shippable
 *     irrespective of the quals. OUTER join between replicated and distributed
 *     relation is shippable if distributed relation is the outer relation.
 *     All joins between hash/modulo distributed relations are shippable if they
 *     have equi-join on the distributed column, such that distribution columns
 *     have same datatype and same distribution strategy.
 * 3. Are datanodes where the joining relations exist, compatible?
 *     Joins between replicated relations are shippable if both relations share a
 *     datanode. Joins between distributed relations are shippable if both
 *     relations are distributed on same set of Datanodes. Join between replicated
 *     and distributed relations is shippable is replicated relation is replicated
 *     on all nodes where distributed relation is distributed.
 *
 * The first step is to be applied by the caller of this function.
 */
static ExecNodes *
pgxc_is_join_shippable(ExecNodes *inner_en, ExecNodes *outer_en, Relids in_relids,
                        Relids out_relids, JoinType jointype, List *join_quals,
#ifdef __TBASE__
                        Query *query,
#endif
						List *rtables)
{
	bool	merge_nodes = false;

	/*
	 * If either of inner_en or outer_en is NULL, return NULL. We can't ship the
	 * join when either of the sides do not have datanodes to ship to.
	 */
	if (!outer_en || !inner_en)
		return NULL;
	/*
	 * We only support reduction of INNER, LEFT [OUTER] and FULL [OUTER] joins.
	 * RIGHT [OUTER] join is converted to LEFT [OUTER] join during join tree
	 * deconstruction.
	 */
	if (jointype != JOIN_INNER && jointype != JOIN_LEFT && jointype != JOIN_FULL)
		return NULL;

	/*
	 * If both sides are replicated or have single node each, we ship any kind
	 * of JOIN
	 */
	if ((IsExecNodesReplicated(inner_en) && IsExecNodesReplicated(outer_en) &&
		 !inner_en->const_subquery && !outer_en->const_subquery) ||
		(list_length(inner_en->nodeList) == 1 &&
		 list_length(outer_en->nodeList) == 1))
		merge_nodes = true;

	/* If both sides are distributed, ... */
	else if (IsExecNodesColumnDistributed(inner_en) &&
			 IsExecNodesColumnDistributed(outer_en))
	{
		/*
		 * If two sides are distributed in the same manner by a value, with an
		 * equi-join on the distribution column and that condition
		 * is shippable, ship the join if node lists from both sides can be
		 * merged.
		 */
		if (inner_en->baselocatortype == outer_en->baselocatortype &&
			IsExecNodesDistributedByValue(inner_en))
		{
			Expr *equi_join_expr = pgxc_find_dist_equijoin_qual(in_relids,
													out_relids, InvalidOid,
													(Node *)join_quals, rtables);
			if (equi_join_expr && pgxc_is_expr_shippable(equi_join_expr, NULL))
				merge_nodes = true;
#ifdef __TBASE__
            if (merge_nodes && restrict_query && query->commandType == CMD_SELECT)
            {
                if (outer_en->restrict_shippable || inner_en->restrict_shippable)
                {
                    ExecNodes *merged_en = makeNode(ExecNodes);
                    switch (jointype)
                    {
                        case JOIN_INNER:
                        {
                            if (outer_en->restrict_shippable)
                                merged_en->nodeList = list_copy(outer_en->nodeList);
                            if (inner_en->restrict_shippable)
                                merged_en->nodeList = list_copy(inner_en->nodeList);
                            merged_en->restrict_shippable = true;
                            break;
                        }
                        case JOIN_LEFT:
                        {
                            merged_en->nodeList = list_copy(outer_en->nodeList);
                            if (outer_en->restrict_shippable)
                            {
                                merged_en->restrict_shippable = true;
                            }
                            break;
                        }
                        case JOIN_FULL:
                        {
                            merged_en->nodeList = list_union_int(outer_en->nodeList, inner_en->nodeList);
                            break;
                        }
                        default:
                            break;
                    }
                    merged_en->baselocatortype = inner_en->baselocatortype;
                    return merged_en;
                }
                else
                {
                    if (jointype == JOIN_INNER)
                    {
                        List *nodelist = NULL;
                        
                        nodelist = pgxc_find_dist_equi_nodes(in_relids,
                                                    out_relids, InvalidOid,
                                                    (Node *)join_quals, rtables);
                        if (nodelist)
                        {
                            ExecNodes *merged_en = makeNode(ExecNodes);
                            merged_en->nodeList = nodelist;
                            merged_en->baselocatortype = inner_en->baselocatortype;
                            merged_en->restrict_shippable = true;
                            return merged_en;
                        }

                        nodelist = pgxc_find_dist_equi_nodes(in_relids,
                                                    out_relids, InvalidOid,
                                                    (Node *)make_ands_implicit((Expr *)query->jointree->quals), rtables);
                        if (nodelist)
                        {
                            ExecNodes *merged_en = makeNode(ExecNodes);
                            merged_en->nodeList = nodelist;
                            merged_en->baselocatortype = inner_en->baselocatortype;
                            merged_en->restrict_shippable = true;
                            return merged_en;
                        }

                        return pgxc_merge_exec_nodes(inner_en, outer_en);
                    }
                    else if (jointype == JOIN_LEFT)
                    {
                        List *nodelist = NULL;

                        nodelist = pgxc_find_dist_equi_nodes(in_relids,
                                                    out_relids, InvalidOid,
                                                    (Node *)make_ands_implicit((Expr *)query->jointree->quals), rtables);
                        if (nodelist)
                        {
                            ExecNodes *merged_en = makeNode(ExecNodes);
                            merged_en->nodeList = nodelist;
                            merged_en->baselocatortype = inner_en->baselocatortype;
                            merged_en->restrict_shippable = true;
                            return merged_en;
                        }

                        return pgxc_merge_exec_nodes(inner_en, outer_en);
                    }
                    else
                    {
                        return pgxc_merge_exec_nodes(inner_en, outer_en);
                    }
                }
            }
#endif
		}
	}
	/*
	 * If outer side is distributed and inner side is replicated, we can ship
	 * LEFT OUTER and INNER join.
	 */
	else if (IsExecNodesColumnDistributed(outer_en) &&
			 IsExecNodesReplicated(inner_en) &&
			 (jointype == JOIN_INNER || jointype == JOIN_LEFT))
	{
		merge_nodes = true;
#ifdef __TBASE__
		/*
		 * Push down to restrict datanodes based if join is on distributed
		 * column or related qual
		 */
		if (restrict_query &&
			query->commandType == CMD_SELECT &&
			!outer_en->restrict_shippable)
		{
			List *nodelist = NULL;

			if (jointype == JOIN_INNER)
			{
				nodelist = pgxc_find_dist_equi_nodes(in_relids,
											out_relids, InvalidOid,
											(Node *)join_quals, rtables);
				if (nodelist && !list_difference_int(nodelist, inner_en->nodeList))
				{
					ExecNodes *merged_en = makeNode(ExecNodes);
					merged_en->nodeList = nodelist;
					merged_en->baselocatortype = outer_en->baselocatortype;
					merged_en->restrict_shippable = true;
					return merged_en;
				}
			}

			if (jointype == JOIN_INNER || jointype == JOIN_LEFT)
			{
				nodelist = pgxc_find_dist_equi_nodes(in_relids,
											out_relids, InvalidOid,
											(Node *)make_ands_implicit((Expr *)query->jointree->quals), rtables);
				if (nodelist && !list_difference_int(nodelist, inner_en->nodeList))
				{
					ExecNodes *merged_en = makeNode(ExecNodes);
					merged_en->nodeList = nodelist;
					merged_en->baselocatortype = outer_en->baselocatortype;
					merged_en->restrict_shippable = true;
					return merged_en;
				}
			}
		}

		/* Inner side is constant subquery */
		if (enable_subquery_shipping && inner_en->const_subquery)
		{
			ExecNodes *merged_en = makeNode(ExecNodes);
			merged_en->nodeList = list_copy(outer_en->nodeList);
			merged_en->baselocatortype = outer_en->baselocatortype;
			return merged_en;
		}
#endif
	}
	/*
	 * If outer side is replicated and inner side is distributed, we can ship
	 * only for INNER join.
	 */
	else if (IsExecNodesReplicated(outer_en) &&
			 IsExecNodesColumnDistributed(inner_en) &&
			 jointype == JOIN_INNER)
	{
		merge_nodes = true;
#ifdef __TBASE__
		/*
		 * Push down to restrict datanodes based if join is on distributed
		 * column or related qual
		 */
		if (restrict_query &&
			query->commandType == CMD_SELECT &&
			!inner_en->restrict_shippable)
		{
			List *nodelist = NULL;

			nodelist = pgxc_find_dist_equi_nodes(in_relids,
										out_relids, InvalidOid,
										(Node *)join_quals, rtables);
			if (nodelist && !list_difference_int(nodelist, outer_en->nodeList))
			{
				ExecNodes *merged_en = makeNode(ExecNodes);
				merged_en->nodeList = nodelist;
				merged_en->baselocatortype = inner_en->baselocatortype;
				merged_en->restrict_shippable = true;
				return merged_en;
			}

			nodelist = pgxc_find_dist_equi_nodes(in_relids,
										out_relids, InvalidOid,
										(Node *)make_ands_implicit((Expr *)query->jointree->quals), rtables);
			if (nodelist && !list_difference_int(nodelist, outer_en->nodeList))
			{
				ExecNodes *merged_en = makeNode(ExecNodes);
				merged_en->nodeList = nodelist;
				merged_en->baselocatortype = inner_en->baselocatortype;
				merged_en->restrict_shippable = true;
				return merged_en;
			}
		}

		/* Outer side is constant subquery */
		if (enable_subquery_shipping && outer_en->const_subquery)
		{
			ExecNodes *merged_en = makeNode(ExecNodes);
			merged_en->nodeList = list_copy(inner_en->nodeList);
			merged_en->baselocatortype = inner_en->baselocatortype;
			return merged_en;
		}
#endif
    }
    /*
     * If the ExecNodes of inner and outer nodes can be merged, the JOIN is
     * shippable
     */
    if (merge_nodes)
        return pgxc_merge_exec_nodes(inner_en, outer_en);
    else
        return NULL;
}

static
bool pgxc_targetlist_has_distcol(Query *query)
{
    RangeTblEntry   *rte = rt_fetch(query->resultRelation, query->rtable);
    RelationLocInfo    *rel_loc_info;
    ListCell   *lc;
    const char *distcol;

    /* distribution column only applies to the relations */
    if (rte->rtekind != RTE_RELATION ||
        rte->relkind != RELKIND_RELATION)
        return false;
    rel_loc_info = GetRelationLocInfo(rte->relid);
    if (!rel_loc_info)
        return false;

    distcol = GetRelationDistribColumn(rel_loc_info);
    if (!distcol)
        return false;

    foreach(lc, query->targetList)
    {
        TargetEntry *tle = (TargetEntry *) lfirst(lc);

        if (tle->resjunk)
            continue;
        if (strcmp(tle->resname, distcol) == 0)
            return true;
    }
    return false;
}
#ifdef __TBASE__
/*
 * pgxc_check_triggers_shippability:
 * Return true if none of the triggers prevents the query from being FQSed.
 */
bool
pgxc_check_triggers_shippability(Oid relid, int commandType)
{
    bool  has_unshippable_trigger;
    int16 trigevent = pgxc_get_trigevent(commandType);
    Relation    rel = relation_open(relid, AccessShareLock);
    RelationLocInfo *rd_locator_info = rel->rd_locator_info;

    /* only process hash and shard table */
    if (rd_locator_info && 
       (rd_locator_info->locatorType != LOCATOR_TYPE_SHARD))
    {
        relation_close(rel, AccessShareLock);

        return true;
    }
    /*
     * If we don't find unshippable row trigger, then the statement is
     * shippable as far as triggers are concerned. For FQSed query, statement
     * triggers are separately invoked on coordinator.
     */
    has_unshippable_trigger = pgxc_find_unshippable_triggers(rel->trigdesc, trigevent, 0, true);

    relation_close(rel, AccessShareLock);
    return !has_unshippable_trigger;
}

/*
  * find unshippable triggers if any 
  *
  * If ignore_timing is true, just the trig_event is used to find a match, so
  * once the event matches, the search returns true regardless of whether it is a
  * before or after row trigger.
  *
  * If ignore_timing is false, return true if we find one or more unshippable
  * triggers that match the exact combination of event and timing.
  */
bool
pgxc_find_unshippable_triggers(TriggerDesc *trigdesc, int16 trig_event, 
                                       int16 trig_timing, bool ignore_timing)
{// #lizard forgives
    int i = 0;

    /* no triggers */
    if (!trigdesc)
        return false;

    /*
     * Quick check by just scanning the trigger descriptor, before
     * actually peeking into each of the individual triggers.
     */
    if (!pgxc_has_trigger_for_event(trig_event, trigdesc))
        return false;

    for (i = 0; i < trigdesc->numtriggers; i++)
    {
        Trigger    *trigger = &trigdesc->triggers[i];
        int16        tgtype = trigger->tgtype;

        /*
         * If we are asked to find triggers of *any* level or timing, just match
         * the event type to determine whether we should ignore this trigger.
         */
        if (ignore_timing)
        {
            if ((TRIGGER_FOR_INSERT(trig_event) && !TRIGGER_FOR_INSERT(tgtype)) ||
                (TRIGGER_FOR_UPDATE(trig_event) && !TRIGGER_FOR_UPDATE(tgtype)) ||
                (TRIGGER_FOR_DELETE(trig_event) && !TRIGGER_FOR_DELETE(tgtype)))
                continue;
        }
        else
        {
            /*
             * Otherwise, do an exact match with the given combination of event
             * and timing.
             */
            if (!((tgtype & TRIGGER_TYPE_TIMING_MASK) == trig_timing &&
                (tgtype & trig_event)))
                continue;
        }

        /*
         * We now know that we cannot ignore this trigger, so check its
         * shippability.
         */
        if (!pgxc_is_trigger_shippable(trigger))
            return true;
    }

    return false;
}

static bool
constraint_is_foreign_key(Oid constroid)
{
    Relation    constrRel;
    HeapTuple    constrTup;
    Form_pg_constraint constrForm;
    bool result = false;

    constrRel = heap_open(ConstraintRelationId, AccessShareLock);
    constrTup = get_catalog_object_by_oid(constrRel, constroid);
    if (!HeapTupleIsValid(constrTup))
        elog(ERROR, "cache lookup failed for constraint %u", constroid);

    constrForm = (Form_pg_constraint) GETSTRUCT(constrTup);

    if (constrForm->contype == CONSTRAINT_FOREIGN)
    {
        result = true;
    }
    else
    {
        result = false;
    }

    heap_close(constrRel, AccessShareLock);

    return result;
}

static bool
pgxc_is_shard_table(Oid relid)
{
    bool is_shard_table = false;
    Relation rel;

    if (OidIsValid(relid))
    {
        rel = heap_open(relid, NoLock);

        is_shard_table = RelationIsSharded(rel);

        heap_close(rel, NoLock);
    }

    return is_shard_table;
}
/*
 * pgxc_is_trigger_shippable:
 * Check if trigger is shippable to a remote node. This function would be
 * called both on coordinator as well as datanode. We want this function
 * to be workable on datanode because we want to skip non-shippable triggers
 * on datanode.
 */
bool
pgxc_is_trigger_shippable(Trigger *trigger)
{
    bool        res = true;

    /*
     * If trigger is based on a constraint or is internal, enforce its launch
     * whatever the node type where we are for the time being.
     * PGXCTODO: we need to remove this condition once constraints are better
     * implemented within Postgres-XC as a constraint can be locally
     * evaluated on remote nodes depending on the distribution type of the table
     * on which it is defined or on its parent/child distribution types.
     */
    if (trigger->tgisinternal)
    {
        if (!OidIsValid(trigger->tgconstraint) || 
            !constraint_is_foreign_key(trigger->tgconstraint) ||
            !pgxc_is_shard_table(trigger->tgconstrrelid))
        return true;
    }

    /*
     * INSTEAD OF triggers can only be defined on views, which are defined
     * only on Coordinators, so they cannot be shipped.
     */
    if (TRIGGER_FOR_INSTEAD(trigger->tgtype))
        res = false;

    /* Finally check if function called is shippable */
    if (!pgxc_is_func_shippable(trigger->tgfoid))
        res = false;

    /*
      * For now, we regard a function as shippable only if it is immutable, but
      * do not look inside the function. 
      *  In TBase, we need more than this. We have to know what exactly the function do, 
      *  and if function called will involve more than one datanodes.
      * TODO: check the function called
      */
    res = false; /* treat all triggers as unshippable now.... */

    return res;
}

static ExecNodes*
pgxc_is_group_subquery_shippable(Query *query, Shippability_context *sc_context)
{// #lizard forgives
    ListCell    *lcell;
    ExecNodes   *exec_nodes = NULL;      
    
    foreach (lcell, query->groupClause)
    {
        SortGroupClause     *sgc = lfirst(lcell);
        Node                *sgc_expr;
        if (!IsA(sgc, SortGroupClause))
        {
            continue;
        }
        
        sgc_expr = get_sortgroupclause_expr(sgc, query->targetList);
        if (IsA(sgc_expr, Var))
        {
            Var             *var       = NULL;
            RangeTblEntry   *rte       = NULL;            
            var             = (Var *)sgc_expr;

			if (var->varlevelsup != 0)
			{
				continue;
			}
            rte = rt_fetch(var->varno, query->rtable);    
            if (RTE_SUBQUERY == rte->rtekind)
            {
                Shippability_context local_sc;  
                ExecNodes   *local_exec_nodes_0 = NULL;  
                ExecNodes   *local_exec_nodes_1 = NULL;  

                /* just assume we are a standalone query. */
                memset((char*)&local_sc, 0X00, sizeof(Shippability_context));

                local_sc.sc_query = rte->subquery;
                local_sc.sc_query_level = 0;
                local_sc.sc_for_expr  = false;
                
                pgxc_shippability_walker((Node*)rte->subquery, &local_sc);
                if (local_sc.sc_exec_nodes && 1 == list_length(local_sc.sc_exec_nodes->nodeList))
                {                    
                    /* try to merge the exec node to check whether the subquery has the same exec node as the local one. */
                    local_exec_nodes_0 = pgxc_merge_exec_nodes(local_sc.sc_exec_nodes, sc_context->sc_exec_nodes);
                    local_exec_nodes_1 = exec_nodes;
                    exec_nodes = pgxc_merge_exec_nodes(local_exec_nodes_0, local_exec_nodes_1);

                    if (local_exec_nodes_0)
                    {
                        FreeExecNodes(&local_exec_nodes_0);
                    }

                    if (local_exec_nodes_1)
                    {
                        FreeExecNodes(&local_exec_nodes_1);
                    }
                    
                    if (!exec_nodes)
                    {    
                        /* Can't be push down. */
                        pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);

                        /* Free the structs if needed. */
                        if (local_sc.sc_exec_nodes)
                        {
                            FreeExecNodes(&local_sc.sc_exec_nodes);
                        }

                        if (local_sc.sc_subquery_en)
                        {
                            FreeExecNodes(&local_sc.sc_subquery_en);
                        }
                        return NULL;
                    }    

                    /* Free the structs if needed. */
                    if (local_sc.sc_exec_nodes)
                    {
                        FreeExecNodes(&local_sc.sc_exec_nodes);
                    }

                    if (local_sc.sc_subquery_en)
                    {
                        FreeExecNodes(&local_sc.sc_subquery_en);
                    }
                
                }
                else
                {        
                    if (exec_nodes)
                    {
                        FreeExecNodes(&exec_nodes);
                    }
                    return NULL;
                }
                
            }
            continue;
        }
    }
    return exec_nodes;
}

static void
pgxc_is_rte_subquery_shippable(Node *node, Shippability_context *sc_context)
{// #lizard forgives
    ExecNodes   *exec_nodes = NULL;     
    
    if (IsA(node, RangeTblRef))
    {
        RangeTblRef     *ref = NULL;
        RangeTblEntry   *rte = NULL;
        
        ref = (RangeTblRef*)node;
        rte = rt_fetch(ref->rtindex, sc_context->sc_query->rtable);    
        if (RTE_SUBQUERY == rte->rtekind)
        {
            Shippability_context local_sc;  

            /* just assume we are a standalone query. */
            memset((char*)&local_sc, 0X00, sizeof(Shippability_context));

            local_sc.sc_query       = rte->subquery;
            local_sc.sc_query_level = 0;
            local_sc.sc_for_expr    = false;
            pgxc_shippability_walker((Node*)rte->subquery, &local_sc);
            if (local_sc.sc_exec_nodes && 1 == list_length(local_sc.sc_exec_nodes->nodeList))
            {                    
                /* try to merge the exec node to check whether the subquery has the same exec node as the local one. */
                exec_nodes = pgxc_merge_exec_nodes(local_sc.sc_exec_nodes, sc_context->sc_exec_nodes);                
                if (!exec_nodes)
                {    
                    /* Can't be push down. */
                    pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);

                    /* Free all exec node to tell the caller we are not shippable. */
                    if (sc_context->sc_exec_nodes)
                    {
                        FreeExecNodes(&sc_context->sc_exec_nodes);
                        sc_context->sc_exec_nodes = NULL;
                    }
                    
                }
                else
                {
                    /* shippable , just set the exec nodes */
                    if (sc_context->sc_exec_nodes)
                    {
                        FreeExecNodes(&sc_context->sc_exec_nodes);
                    }
                    sc_context->sc_exec_nodes = exec_nodes;
                }

                /* Free the structs if needed. */
                if (local_sc.sc_exec_nodes)
                {
                    FreeExecNodes(&local_sc.sc_exec_nodes);
                }

                if (local_sc.sc_subquery_en)
                {
                    FreeExecNodes(&local_sc.sc_subquery_en);
                }
            }            
        }
    }
}

Node *
get_var_from_arg(Node *arg)
{
    Var *var = NULL;

    if (!arg)
    {
        return NULL;
    }
    
    switch(nodeTag(arg))
    {
        case T_Var:
            var = (Var *)arg;
            break;
        case T_RelabelType:
            {
                RelabelType *rt = (RelabelType *)arg;

                if (rt->relabelformat == COERCE_IMPLICIT_CAST && 
                    IsA(rt->arg, Var))
                {
                    var = (Var *)rt->arg;
                }
                break;
            }
        default:
            var = NULL;
            break;
    }

    return (Node *)var;
}

bool
is_var_distribute_column(Var *var, List *rtable)
{
    if (!rtable || !var)
    {
        return false;
    }

    if (var->varno > rtable->length)
    {
        return false;
    }

    return pgxc_is_var_distrib_column(var, rtable);
}
#endif
