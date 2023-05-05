/*-------------------------------------------------------------------------
 *
 * pathnode.c
 *      Routines to manipulate pathlists and create path nodes
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *      src/backend/optimizer/util/pathnode.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "nodes/extensible.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/prep.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "foreign/fdwapi.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/selfuncs.h"
#ifdef XCP
#include "access/heapam.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "pgxc/locator.h"
#include "pgxc/nodemgr.h"
#include "utils/rel.h"
#ifdef __TBASE__
#include "catalog/pgxc_key_values.h"
#include "executor/nodeAgg.h"
#include "optimizer/distribution.h"
#include "optimizer/tlist.h"
#include "optimizer/planner.h"
#include "optimizer/pgxcship.h"
#include "pgxc/groupmgr.h"
#include "pgxc/pgxcnode.h"
#include "utils/memutils.h"
#endif

#ifdef _MIGRATE_
#include "catalog/pgxc_class.h"
#endif

#endif

#ifdef __TBASE__
/*GUC parameter */
bool prefer_olap;
/* Max replication level on join to make Query more efficient */
int replication_level;
/* Restrict query to involved node as possible */
bool restrict_query = false;
/* Support fast query shipping for subquery */
bool enable_subquery_shipping = false;

/* join will happen in these nodes forcibly */
char  *g_constrain_group; /* the GUC variable */
static Bitmapset *constrainNodes = NULL;
#define BMS_EQUAL_CONSTRAINT(bms) (bms_is_empty(constrainNodes) || bms_equal(constrainNodes, (bms)))

#define  REPLICATION_FACTOR 0.8
#endif

typedef enum
{
    COSTS_EQUAL,                /* path costs are fuzzily equal */
    COSTS_BETTER1,                /* first path is cheaper than second */
    COSTS_BETTER2,                /* second path is cheaper than first */
    COSTS_DIFFERENT                /* neither path dominates the other on cost */
} PathCostComparison;

/*
 * STD_FUZZ_FACTOR is the normal fuzz factor for compare_path_costs_fuzzily.
 * XXX is it worth making this user-controllable?  It provides a tradeoff
 * between planner runtime and the accuracy of path cost comparisons.
 */
#define STD_FUZZ_FACTOR 1.01

static List *translate_sub_tlist(List *tlist, int relid);
static List *reparameterize_pathlist_by_child(PlannerInfo *root,
                                                                List *pathlist,
                                                                RelOptInfo *child_rel);

#ifdef XCP
static void restrict_distribution(PlannerInfo *root, RestrictInfo *ri,
                                  Path *pathnode);
static Path *redistribute_path(PlannerInfo *root, Path *subpath, List *pathkeys,
                  char distributionType, Node* distributionExpr,
                  Bitmapset *nodes, Bitmapset *restrictNodes);
static void set_scanpath_distribution(PlannerInfo *root, RelOptInfo *rel, Path *pathnode);
static List *set_joinpath_distribution(PlannerInfo *root, JoinPath *pathnode);
extern void PoolPingNodes(void);
#endif

#ifdef __TBASE__
static int get_num_connections(int numnodes, int nRemotePlans);
#endif

/*****************************************************************************
 *        MISC. PATH UTILITIES
 *****************************************************************************/

/*
 * compare_path_costs
 *      Return -1, 0, or +1 according as path1 is cheaper, the same cost,
 *      or more expensive than path2 for the specified criterion.
 */
int
compare_path_costs(Path *path1, Path *path2, CostSelector criterion)
{// #lizard forgives
    if (criterion == STARTUP_COST)
    {
        if (path1->startup_cost < path2->startup_cost)
            return -1;
        if (path1->startup_cost > path2->startup_cost)
            return +1;

        /*
         * If paths have the same startup cost (not at all unlikely), order
         * them by total cost.
         */
        if (path1->total_cost < path2->total_cost)
            return -1;
        if (path1->total_cost > path2->total_cost)
            return +1;
    }
    else
    {
        if (path1->total_cost < path2->total_cost)
            return -1;
        if (path1->total_cost > path2->total_cost)
            return +1;

        /*
         * If paths have the same total cost, order them by startup cost.
         */
        if (path1->startup_cost < path2->startup_cost)
            return -1;
        if (path1->startup_cost > path2->startup_cost)
            return +1;
    }
    return 0;
}

/*
 * compare_path_fractional_costs
 *      Return -1, 0, or +1 according as path1 is cheaper, the same cost,
 *      or more expensive than path2 for fetching the specified fraction
 *      of the total tuples.
 *
 * If fraction is <= 0 or > 1, we interpret it as 1, ie, we select the
 * path with the cheaper total_cost.
 */
int
compare_fractional_path_costs(Path *path1, Path *path2,
                              double fraction)
{
    Cost        cost1,
                cost2;

    if (fraction <= 0.0 || fraction >= 1.0)
        return compare_path_costs(path1, path2, TOTAL_COST);
    cost1 = path1->startup_cost +
        fraction * (path1->total_cost - path1->startup_cost);
    cost2 = path2->startup_cost +
        fraction * (path2->total_cost - path2->startup_cost);
    if (cost1 < cost2)
        return -1;
    if (cost1 > cost2)
        return +1;
    return 0;
}

/*
 * compare_path_costs_fuzzily
 *      Compare the costs of two paths to see if either can be said to
 *      dominate the other.
 *
 * We use fuzzy comparisons so that add_path() can avoid keeping both of
 * a pair of paths that really have insignificantly different cost.
 *
 * The fuzz_factor argument must be 1.0 plus delta, where delta is the
 * fraction of the smaller cost that is considered to be a significant
 * difference.  For example, fuzz_factor = 1.01 makes the fuzziness limit
 * be 1% of the smaller cost.
 *
 * The two paths are said to have "equal" costs if both startup and total
 * costs are fuzzily the same.  Path1 is said to be better than path2 if
 * it has fuzzily better startup cost and fuzzily no worse total cost,
 * or if it has fuzzily better total cost and fuzzily no worse startup cost.
 * Path2 is better than path1 if the reverse holds.  Finally, if one path
 * is fuzzily better than the other on startup cost and fuzzily worse on
 * total cost, we just say that their costs are "different", since neither
 * dominates the other across the whole performance spectrum.
 *
 * This function also enforces a policy rule that paths for which the relevant
 * one of parent->consider_startup and parent->consider_param_startup is false
 * cannot survive comparisons solely on the grounds of good startup cost, so
 * we never return COSTS_DIFFERENT when that is true for the total-cost loser.
 * (But if total costs are fuzzily equal, we compare startup costs anyway,
 * in hopes of eliminating one path or the other.)
 */
static PathCostComparison
compare_path_costs_fuzzily(Path *path1, Path *path2, double fuzz_factor)
{// #lizard forgives
#define CONSIDER_PATH_STARTUP_COST(p)  \
    ((p)->param_info == NULL ? (p)->parent->consider_startup : (p)->parent->consider_param_startup)

    /*
     * Check total cost first since it's more likely to be different; many
     * paths have zero startup cost.
     */
    if (path1->total_cost > path2->total_cost * fuzz_factor)
    {
        /* path1 fuzzily worse on total cost */
        if (CONSIDER_PATH_STARTUP_COST(path1) &&
            path2->startup_cost > path1->startup_cost * fuzz_factor)
        {
            /* ... but path2 fuzzily worse on startup, so DIFFERENT */
            return COSTS_DIFFERENT;
        }
        /* else path2 dominates */
        return COSTS_BETTER2;
    }
    if (path2->total_cost > path1->total_cost * fuzz_factor)
    {
        /* path2 fuzzily worse on total cost */
        if (CONSIDER_PATH_STARTUP_COST(path2) &&
            path1->startup_cost > path2->startup_cost * fuzz_factor)
        {
            /* ... but path1 fuzzily worse on startup, so DIFFERENT */
            return COSTS_DIFFERENT;
        }
        /* else path1 dominates */
        return COSTS_BETTER1;
    }
    /* fuzzily the same on total cost ... */
    if (path1->startup_cost > path2->startup_cost * fuzz_factor)
    {
        /* ... but path1 fuzzily worse on startup, so path2 wins */
        return COSTS_BETTER2;
    }
    if (path2->startup_cost > path1->startup_cost * fuzz_factor)
    {
        /* ... but path2 fuzzily worse on startup, so path1 wins */
        return COSTS_BETTER1;
    }
    /* fuzzily the same on both costs */
    return COSTS_EQUAL;

#undef CONSIDER_PATH_STARTUP_COST
}

/*
 * set_cheapest
 *      Find the minimum-cost paths from among a relation's paths,
 *      and save them in the rel's cheapest-path fields.
 *
 * cheapest_total_path is normally the cheapest-total-cost unparameterized
 * path; but if there are no unparameterized paths, we assign it to be the
 * best (cheapest least-parameterized) parameterized path.  However, only
 * unparameterized paths are considered candidates for cheapest_startup_path,
 * so that will be NULL if there are no unparameterized paths.
 *
 * The cheapest_parameterized_paths list collects all parameterized paths
 * that have survived the add_path() tournament for this relation.  (Since
 * add_path ignores pathkeys for a parameterized path, these will be paths
 * that have best cost or best row count for their parameterization.  We
 * may also have both a parallel-safe and a non-parallel-safe path in some
 * cases for the same parameterization in some cases, but this should be
 * relatively rare since, most typically, all paths for the same relation
 * will be parallel-safe or none of them will.)
 *
 * cheapest_parameterized_paths always includes the cheapest-total
 * unparameterized path, too, if there is one; the users of that list find
 * it more convenient if that's included.
 *
 * This is normally called only after we've finished constructing the path
 * list for the rel node.
 */
void
set_cheapest(RelOptInfo *parent_rel)
{// #lizard forgives
    Path       *cheapest_startup_path;
    Path       *cheapest_total_path;
    Path       *best_param_path;
    List       *parameterized_paths;
    ListCell   *p;

    Assert(IsA(parent_rel, RelOptInfo));

#ifdef __TBASE__
	/*
	 * When set_joinpath_distribution() adjusted the strategy for complex
	 * UPDATE/DELETE, the original paths could be give up caused by no proper
	 * distribution found. Which lead to an early error pop up here, thus
	 * we need to provide more accurate error message here. (Before the
	 * complex delete enhancement, this will pop up in group_planner at
	 * final stage.)
	 */
	if (parent_rel->pathlist == NIL &&
		parent_rel->resultRelLoc != RESULT_REL_NONE)
	{
#ifdef _PG_REGRESS_
			ereport(ERROR,
					(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
					 errmsg("could not plan this distributed UPDATE/DELETE"),
					 errdetail("correlated or complex UPDATE/DELETE is currently not supported in Postgres-XL.")));
#else
			ereport(ERROR,
					(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
					 errmsg("could not plan this distributed UPDATE/DELETE"),
					 errdetail("correlated or complex UPDATE/DELETE is currently not supported in TBase.")));
#endif
	}
#endif

    if (parent_rel->pathlist == NIL)
        elog(ERROR, "could not devise a query plan for the given query");

    cheapest_startup_path = cheapest_total_path = best_param_path = NULL;
    parameterized_paths = NIL;

    foreach(p, parent_rel->pathlist)
    {
        Path       *path = (Path *) lfirst(p);
        int            cmp;

        if (path->param_info)
        {
            /* Parameterized path, so add it to parameterized_paths */
            parameterized_paths = lappend(parameterized_paths, path);

            /*
             * If we have an unparameterized cheapest-total, we no longer care
             * about finding the best parameterized path, so move on.
             */
            if (cheapest_total_path)
                continue;

            /*
             * Otherwise, track the best parameterized path, which is the one
             * with least total cost among those of the minimum
             * parameterization.
             */
            if (best_param_path == NULL)
                best_param_path = path;
            else
            {
                switch (bms_subset_compare(PATH_REQ_OUTER(path),
                                           PATH_REQ_OUTER(best_param_path)))
                {
                    case BMS_EQUAL:
                        /* keep the cheaper one */
                        if (compare_path_costs(path, best_param_path,
                                               TOTAL_COST) < 0)
                            best_param_path = path;
                        break;
                    case BMS_SUBSET1:
                        /* new path is less-parameterized */
                        best_param_path = path;
                        break;
                    case BMS_SUBSET2:
                        /* old path is less-parameterized, keep it */
                        break;
                    case BMS_DIFFERENT:

                        /*
                         * This means that neither path has the least possible
                         * parameterization for the rel.  We'll sit on the old
                         * path until something better comes along.
                         */
                        break;
                }
            }
        }
        else
        {
            /* Unparameterized path, so consider it for cheapest slots */
            if (cheapest_total_path == NULL)
            {
                cheapest_startup_path = cheapest_total_path = path;
                continue;
            }

            /*
             * If we find two paths of identical costs, try to keep the
             * better-sorted one.  The paths might have unrelated sort
             * orderings, in which case we can only guess which might be
             * better to keep, but if one is superior then we definitely
             * should keep that one.
             */
            cmp = compare_path_costs(cheapest_startup_path, path, STARTUP_COST);
            if (cmp > 0 ||
                (cmp == 0 &&
                 compare_pathkeys(cheapest_startup_path->pathkeys,
                                  path->pathkeys) == PATHKEYS_BETTER2))
                cheapest_startup_path = path;

            cmp = compare_path_costs(cheapest_total_path, path, TOTAL_COST);
            if (cmp > 0 ||
                (cmp == 0 &&
                 compare_pathkeys(cheapest_total_path->pathkeys,
                                  path->pathkeys) == PATHKEYS_BETTER2))
                cheapest_total_path = path;
        }
    }

    /* Add cheapest unparameterized path, if any, to parameterized_paths */
    if (cheapest_total_path)
        parameterized_paths = lcons(cheapest_total_path, parameterized_paths);

    /*
     * If there is no unparameterized path, use the best parameterized path as
     * cheapest_total_path (but not as cheapest_startup_path).
     */
    if (cheapest_total_path == NULL)
        cheapest_total_path = best_param_path;
    Assert(cheapest_total_path != NULL);

    parent_rel->cheapest_startup_path = cheapest_startup_path;
    parent_rel->cheapest_total_path = cheapest_total_path;
    parent_rel->cheapest_unique_path = NULL;    /* computed only if needed */
    parent_rel->cheapest_parameterized_paths = parameterized_paths;
}

/*
 * add_path
 *      Consider a potential implementation path for the specified parent rel,
 *      and add it to the rel's pathlist if it is worthy of consideration.
 *      A path is worthy if it has a better sort order (better pathkeys) or
 *      cheaper cost (on either dimension), or generates fewer rows, than any
 *      existing path that has the same or superset parameterization rels.
 *      We also consider parallel-safe paths more worthy than others.
 *
 *      We also remove from the rel's pathlist any old paths that are dominated
 *      by new_path --- that is, new_path is cheaper, at least as well ordered,
 *      generates no more rows, requires no outer rels not required by the old
 *      path, and is no less parallel-safe.
 *
 *      In most cases, a path with a superset parameterization will generate
 *      fewer rows (since it has more join clauses to apply), so that those two
 *      figures of merit move in opposite directions; this means that a path of
 *      one parameterization can seldom dominate a path of another.  But such
 *      cases do arise, so we make the full set of checks anyway.
 *
 *      There are two policy decisions embedded in this function, along with
 *      its sibling add_path_precheck.  First, we treat all parameterized paths
 *      as having NIL pathkeys, so that they cannot win comparisons on the
 *      basis of sort order.  This is to reduce the number of parameterized
 *      paths that are kept; see discussion in src/backend/optimizer/README.
 *
 *      Second, we only consider cheap startup cost to be interesting if
 *      parent_rel->consider_startup is true for an unparameterized path, or
 *      parent_rel->consider_param_startup is true for a parameterized one.
 *      Again, this allows discarding useless paths sooner.
 *
 *      The pathlist is kept sorted by total_cost, with cheaper paths
 *      at the front.  Within this routine, that's simply a speed hack:
 *      doing it that way makes it more likely that we will reject an inferior
 *      path after a few comparisons, rather than many comparisons.
 *      However, add_path_precheck relies on this ordering to exit early
 *      when possible.
 *
 *      NOTE: discarded Path objects are immediately pfree'd to reduce planner
 *      memory consumption.  We dare not try to free the substructure of a Path,
 *      since much of it may be shared with other Paths or the query tree itself;
 *      but just recycling discarded Path nodes is a very useful savings in
 *      a large join tree.  We can recycle the List nodes of pathlist, too.
 *
 *      As noted in optimizer/README, deleting a previously-accepted Path is
 *      safe because we know that Paths of this rel cannot yet be referenced
 *      from any other rel, such as a higher-level join.  However, in some cases
 *      it is possible that a Path is referenced by another Path for its own
 *      rel; we must not delete such a Path, even if it is dominated by the new
 *      Path.  Currently this occurs only for IndexPath objects, which may be
 *      referenced as children of BitmapHeapPaths as well as being paths in
 *      their own right.  Hence, we don't pfree IndexPaths when rejecting them.
 *
 * 'parent_rel' is the relation entry to which the path corresponds.
 * 'new_path' is a potential path for parent_rel.
 *
 * Returns nothing, but modifies parent_rel->pathlist.
 */
void
add_path(RelOptInfo *parent_rel, Path *new_path)
{// #lizard forgives
    bool        accept_new = true;    /* unless we find a superior old path */
    ListCell   *insert_after = NULL;    /* where to insert new item */
    List       *new_path_pathkeys;
    ListCell   *p1;
    ListCell   *p1_prev;
    ListCell   *p1_next;

    /*
     * This is a convenient place to check for query cancel --- no part of the
     * planner goes very long without calling add_path().
     */
    CHECK_FOR_INTERRUPTS();

#ifdef __TBASE__
	/*
	 * In case we skipped the join paths caused by invalid result rel
	 * distribution.
	 */
	if (!new_path)
		return;
#endif

    /* Pretend parameterized paths have no pathkeys, per comment above */
    new_path_pathkeys = new_path->param_info ? NIL : new_path->pathkeys;

    /*
     * Loop to check proposed new path against old paths.  Note it is possible
     * for more than one old path to be tossed out because new_path dominates
     * it.
     *
     * We can't use foreach here because the loop body may delete the current
     * list cell.
     */
    p1_prev = NULL;
    for (p1 = list_head(parent_rel->pathlist); p1 != NULL; p1 = p1_next)
    {
        Path       *old_path = (Path *) lfirst(p1);
        bool        remove_old = false; /* unless new proves superior */
        PathCostComparison costcmp;
        PathKeysComparison keyscmp;
        BMS_Comparison outercmp;

        p1_next = lnext(p1);

        /*
         * Do a fuzzy cost comparison with standard fuzziness limit.
         */
        costcmp = compare_path_costs_fuzzily(new_path, old_path,
                                             STD_FUZZ_FACTOR);

        /*
         * If the two paths compare differently for startup and total cost,
         * then we want to keep both, and we can skip comparing pathkeys and
         * required_outer rels.  If they compare the same, proceed with the
         * other comparisons.  Row count is checked last.  (We make the tests
         * in this order because the cost comparison is most likely to turn
         * out "different", and the pathkeys comparison next most likely.  As
         * explained above, row count very seldom makes a difference, so even
         * though it's cheap to compare there's not much point in checking it
         * earlier.)
         */
        if (costcmp != COSTS_DIFFERENT)
        {
            /* Similarly check to see if either dominates on pathkeys */
            List       *old_path_pathkeys;

            old_path_pathkeys = old_path->param_info ? NIL : old_path->pathkeys;
            keyscmp = compare_pathkeys(new_path_pathkeys,
                                       old_path_pathkeys);
            if (keyscmp != PATHKEYS_DIFFERENT)
            {
                switch (costcmp)
                {
                    case COSTS_EQUAL:
                        outercmp = bms_subset_compare(PATH_REQ_OUTER(new_path),
                                                      PATH_REQ_OUTER(old_path));
                        if (keyscmp == PATHKEYS_BETTER1)
                        {
                            if ((outercmp == BMS_EQUAL ||
                                 outercmp == BMS_SUBSET1) &&
                                new_path->rows <= old_path->rows &&
                                new_path->parallel_safe >= old_path->parallel_safe)
                                remove_old = true;    /* new dominates old */
                        }
                        else if (keyscmp == PATHKEYS_BETTER2)
                        {
                            if ((outercmp == BMS_EQUAL ||
                                 outercmp == BMS_SUBSET2) &&
                                new_path->rows >= old_path->rows &&
                                new_path->parallel_safe <= old_path->parallel_safe)
                                accept_new = false; /* old dominates new */
                        }
                        else    /* keyscmp == PATHKEYS_EQUAL */
                        {
                            if (outercmp == BMS_EQUAL)
                            {
                                /*
                                 * Same pathkeys and outer rels, and fuzzily
                                 * the same cost, so keep just one; to decide
                                 * which, first check parallel-safety, then
                                 * rows, then do a fuzzy cost comparison with
                                 * very small fuzz limit.  (We used to do an
                                 * exact cost comparison, but that results in
                                 * annoying platform-specific plan variations
                                 * due to roundoff in the cost estimates.)    If
                                 * things are still tied, arbitrarily keep
                                 * only the old path.  Notice that we will
                                 * keep only the old path even if the
                                 * less-fuzzy comparison decides the startup
                                 * and total costs compare differently.
                                 */
                                if (new_path->parallel_safe >
                                    old_path->parallel_safe)
                                    remove_old = true;    /* new dominates old */
                                else if (new_path->parallel_safe <
                                         old_path->parallel_safe)
                                    accept_new = false; /* old dominates new */
                                else if (new_path->rows < old_path->rows)
                                    remove_old = true;    /* new dominates old */
                                else if (new_path->rows > old_path->rows)
                                    accept_new = false; /* old dominates new */
                                else if (compare_path_costs_fuzzily(new_path,
                                                                    old_path,
                                                                    1.0000000001) == COSTS_BETTER1)
                                    remove_old = true;    /* new dominates old */
                                else
                                    accept_new = false; /* old equals or
                                                         * dominates new */
                            }
                            else if (outercmp == BMS_SUBSET1 &&
                                     new_path->rows <= old_path->rows &&
                                     new_path->parallel_safe >= old_path->parallel_safe)
                                remove_old = true;    /* new dominates old */
                            else if (outercmp == BMS_SUBSET2 &&
                                     new_path->rows >= old_path->rows &&
                                     new_path->parallel_safe <= old_path->parallel_safe)
                                accept_new = false; /* old dominates new */
                            /* else different parameterizations, keep both */
                        }
                        break;
                    case COSTS_BETTER1:
                        if (keyscmp != PATHKEYS_BETTER2)
                        {
                            outercmp = bms_subset_compare(PATH_REQ_OUTER(new_path),
                                                          PATH_REQ_OUTER(old_path));
                            if ((outercmp == BMS_EQUAL ||
                                 outercmp == BMS_SUBSET1) &&
                                new_path->rows <= old_path->rows &&
                                new_path->parallel_safe >= old_path->parallel_safe)
                                remove_old = true;    /* new dominates old */
                        }
                        break;
                    case COSTS_BETTER2:
                        if (keyscmp != PATHKEYS_BETTER1)
                        {
                            outercmp = bms_subset_compare(PATH_REQ_OUTER(new_path),
                                                          PATH_REQ_OUTER(old_path));
                            if ((outercmp == BMS_EQUAL ||
                                 outercmp == BMS_SUBSET2) &&
                                new_path->rows >= old_path->rows &&
                                new_path->parallel_safe <= old_path->parallel_safe)
                                accept_new = false; /* old dominates new */
                        }
                        break;
                    case COSTS_DIFFERENT:

                        /*
                         * can't get here, but keep this case to keep compiler
                         * quiet
                         */
                        break;
                }
            }
        }

        /*
         * Remove current element from pathlist if dominated by new.
         */
        if (remove_old)
        {
            parent_rel->pathlist = list_delete_cell(parent_rel->pathlist,
                                                    p1, p1_prev);

            /*
             * Delete the data pointed-to by the deleted cell, if possible
             */
            if (!IsA(old_path, IndexPath))
                pfree(old_path);
            /* p1_prev does not advance */
        }
        else
        {
            /* new belongs after this old path if it has cost >= old's */
            if (new_path->total_cost >= old_path->total_cost)
                insert_after = p1;
            /* p1_prev advances */
            p1_prev = p1;
        }

        /*
         * If we found an old path that dominates new_path, we can quit
         * scanning the pathlist; we will not add new_path, and we assume
         * new_path cannot dominate any other elements of the pathlist.
         */
        if (!accept_new)
            break;
    }

    if (accept_new)
    {
        /* Accept the new path: insert it at proper place in pathlist */
        if (insert_after)
            lappend_cell(parent_rel->pathlist, insert_after, new_path);
        else
            parent_rel->pathlist = lcons(new_path, parent_rel->pathlist);
    }
    else
    {
        /* Reject and recycle the new path */
        if (!IsA(new_path, IndexPath))
            pfree(new_path);
    }
}

/*
 * add_path_precheck
 *      Check whether a proposed new path could possibly get accepted.
 *      We assume we know the path's pathkeys and parameterization accurately,
 *      and have lower bounds for its costs.
 *
 * Note that we do not know the path's rowcount, since getting an estimate for
 * that is too expensive to do before prechecking.  We assume here that paths
 * of a superset parameterization will generate fewer rows; if that holds,
 * then paths with different parameterizations cannot dominate each other
 * and so we can simply ignore existing paths of another parameterization.
 * (In the infrequent cases where that rule of thumb fails, add_path will
 * get rid of the inferior path.)
 *
 * At the time this is called, we haven't actually built a Path structure,
 * so the required information has to be passed piecemeal.
 */
bool
add_path_precheck(RelOptInfo *parent_rel,
                  Cost startup_cost, Cost total_cost,
                  List *pathkeys, Relids required_outer)
{// #lizard forgives
    List       *new_path_pathkeys;
    bool        consider_startup;
    ListCell   *p1;

    /* Pretend parameterized paths have no pathkeys, per add_path policy */
    new_path_pathkeys = required_outer ? NIL : pathkeys;

    /* Decide whether new path's startup cost is interesting */
    consider_startup = required_outer ? parent_rel->consider_param_startup : parent_rel->consider_startup;

    foreach(p1, parent_rel->pathlist)
    {
        Path       *old_path = (Path *) lfirst(p1);
        PathKeysComparison keyscmp;

        /*
         * We are looking for an old_path with the same parameterization (and
         * by assumption the same rowcount) that dominates the new path on
         * pathkeys as well as both cost metrics.  If we find one, we can
         * reject the new path.
         *
         * Cost comparisons here should match compare_path_costs_fuzzily.
         */
        if (total_cost > old_path->total_cost * STD_FUZZ_FACTOR)
        {
            /* new path can win on startup cost only if consider_startup */
            if (startup_cost > old_path->startup_cost * STD_FUZZ_FACTOR ||
                !consider_startup)
            {
                /* new path loses on cost, so check pathkeys... */
                List       *old_path_pathkeys;

                old_path_pathkeys = old_path->param_info ? NIL : old_path->pathkeys;
                keyscmp = compare_pathkeys(new_path_pathkeys,
                                           old_path_pathkeys);
                if (keyscmp == PATHKEYS_EQUAL ||
                    keyscmp == PATHKEYS_BETTER2)
                {
                    /* new path does not win on pathkeys... */
                    if (bms_equal(required_outer, PATH_REQ_OUTER(old_path)))
                    {
                        /* Found an old path that dominates the new one */
                        return false;
                    }
                }
            }
        }
        else
        {
            /*
             * Since the pathlist is sorted by total_cost, we can stop looking
             * once we reach a path with a total_cost larger than the new
             * path's.
             */
            break;
        }
    }

    return true;
}

/*
 * add_partial_path
 *      Like add_path, our goal here is to consider whether a path is worthy
 *      of being kept around, but the considerations here are a bit different.
 *      A partial path is one which can be executed in any number of workers in
 *      parallel such that each worker will generate a subset of the path's
 *      overall result.
 *
 *      As in add_path, the partial_pathlist is kept sorted with the cheapest
 *      total path in front.  This is depended on by multiple places, which
 *      just take the front entry as the cheapest path without searching.
 *
 *      We don't generate parameterized partial paths for several reasons.  Most
 *      importantly, they're not safe to execute, because there's nothing to
 *      make sure that a parallel scan within the parameterized portion of the
 *      plan is running with the same value in every worker at the same time.
 *      Fortunately, it seems unlikely to be worthwhile anyway, because having
 *      each worker scan the entire outer relation and a subset of the inner
 *      relation will generally be a terrible plan.  The inner (parameterized)
 *      side of the plan will be small anyway.  There could be rare cases where
 *      this wins big - e.g. if join order constraints put a 1-row relation on
 *      the outer side of the topmost join with a parameterized plan on the inner
 *      side - but we'll have to be content not to handle such cases until
 *      somebody builds an executor infrastructure that can cope with them.
 *
 *      Because we don't consider parameterized paths here, we also don't
 *      need to consider the row counts as a measure of quality: every path will
 *      produce the same number of rows.  Neither do we need to consider startup
 *      costs: parallelism is only used for plans that will be run to completion.
 *      Therefore, this routine is much simpler than add_path: it needs to
 *      consider only pathkeys and total cost.
 *
 *      As with add_path, we pfree paths that are found to be dominated by
 *      another partial path; this requires that there be no other references to
 *      such paths yet.  Hence, GatherPaths must not be created for a rel until
 *      we're done creating all partial paths for it.  Unlike add_path, we don't
 *      take an exception for IndexPaths as partial index paths won't be
 *      referenced by partial BitmapHeapPaths.
 */
void
add_partial_path(RelOptInfo *parent_rel, Path *new_path)
{// #lizard forgives
    bool        accept_new = true;    /* unless we find a superior old path */
    ListCell   *insert_after = NULL;    /* where to insert new item */
    ListCell   *p1;
    ListCell   *p1_prev;
    ListCell   *p1_next;

    /* Check for query cancel. */
    CHECK_FOR_INTERRUPTS();

    /*
     * As in add_path, throw out any paths which are dominated by the new
     * path, but throw out the new path if some existing path dominates it.
     */
    p1_prev = NULL;
    for (p1 = list_head(parent_rel->partial_pathlist); p1 != NULL;
         p1 = p1_next)
    {
        Path       *old_path = (Path *) lfirst(p1);
        bool        remove_old = false; /* unless new proves superior */
        PathKeysComparison keyscmp;

        p1_next = lnext(p1);

        /* Compare pathkeys. */
        keyscmp = compare_pathkeys(new_path->pathkeys, old_path->pathkeys);

        /* Unless pathkeys are incompable, keep just one of the two paths. */
        if (keyscmp != PATHKEYS_DIFFERENT)
        {
            if (new_path->total_cost > old_path->total_cost * STD_FUZZ_FACTOR)
            {
                /* New path costs more; keep it only if pathkeys are better. */
                if (keyscmp != PATHKEYS_BETTER1)
                    accept_new = false;
            }
            else if (old_path->total_cost > new_path->total_cost
                     * STD_FUZZ_FACTOR)
            {
                /* Old path costs more; keep it only if pathkeys are better. */
                if (keyscmp != PATHKEYS_BETTER2)
                    remove_old = true;
            }
            else if (keyscmp == PATHKEYS_BETTER1)
            {
                /* Costs are about the same, new path has better pathkeys. */
                remove_old = true;
            }
            else if (keyscmp == PATHKEYS_BETTER2)
            {
                /* Costs are about the same, old path has better pathkeys. */
                accept_new = false;
            }
            else if (old_path->total_cost > new_path->total_cost * 1.0000000001)
            {
                /* Pathkeys are the same, and the old path costs more. */
                remove_old = true;
            }
            else
            {
                /*
                 * Pathkeys are the same, and new path isn't materially
                 * cheaper.
                 */
                accept_new = false;
            }
        }

        /*
         * Remove current element from partial_pathlist if dominated by new.
         */
        if (remove_old)
        {
            parent_rel->partial_pathlist =
                list_delete_cell(parent_rel->partial_pathlist, p1, p1_prev);
            pfree(old_path);
            /* p1_prev does not advance */
        }
        else
        {
            /* new belongs after this old path if it has cost >= old's */
            if (new_path->total_cost >= old_path->total_cost)
                insert_after = p1;
            /* p1_prev advances */
            p1_prev = p1;
        }

        /*
         * If we found an old path that dominates new_path, we can quit
         * scanning the partial_pathlist; we will not add new_path, and we
         * assume new_path cannot dominate any later path.
         */
        if (!accept_new)
            break;
    }

    if (accept_new)
    {
        /* Accept the new path: insert it at proper place */
        if (insert_after)
            lappend_cell(parent_rel->partial_pathlist, insert_after, new_path);
        else
            parent_rel->partial_pathlist =
                lcons(new_path, parent_rel->partial_pathlist);
    }
    else
    {
        /* Reject and recycle the new path */
        pfree(new_path);
    }
}

/*
 * add_partial_path_precheck
 *      Check whether a proposed new partial path could possibly get accepted.
 *
 * Unlike add_path_precheck, we can ignore startup cost and parameterization,
 * since they don't matter for partial paths (see add_partial_path).  But
 * we do want to make sure we don't add a partial path if there's already
 * a complete path that dominates it, since in that case the proposed path
 * is surely a loser.
 */
bool
add_partial_path_precheck(RelOptInfo *parent_rel, Cost total_cost,
                          List *pathkeys)
{
    ListCell   *p1;

    /*
     * Our goal here is twofold.  First, we want to find out whether this path
     * is clearly inferior to some existing partial path.  If so, we want to
     * reject it immediately.  Second, we want to find out whether this path
     * is clearly superior to some existing partial path -- at least, modulo
     * final cost computations.  If so, we definitely want to consider it.
     *
     * Unlike add_path(), we always compare pathkeys here.  This is because we
     * expect partial_pathlist to be very short, and getting a definitive
     * answer at this stage avoids the need to call add_path_precheck.
     */
    foreach(p1, parent_rel->partial_pathlist)
    {
        Path       *old_path = (Path *) lfirst(p1);
        PathKeysComparison keyscmp;

        keyscmp = compare_pathkeys(pathkeys, old_path->pathkeys);
        if (keyscmp != PATHKEYS_DIFFERENT)
        {
            if (total_cost > old_path->total_cost * STD_FUZZ_FACTOR &&
                keyscmp != PATHKEYS_BETTER1)
                return false;
            if (old_path->total_cost > total_cost * STD_FUZZ_FACTOR &&
                keyscmp != PATHKEYS_BETTER2)
                return true;
        }
    }

    /*
     * This path is neither clearly inferior to an existing partial path nor
     * clearly good enough that it might replace one.  Compare it to
     * non-parallel plans.  If it loses even before accounting for the cost of
     * the Gather node, we should definitely reject it.
     *
     * Note that we pass the total_cost to add_path_precheck twice.  This is
     * because it's never advantageous to consider the startup cost of a
     * partial path; the resulting plans, if run in parallel, will be run to
     * completion.
     */
    if (!add_path_precheck(parent_rel, total_cost, total_cost, pathkeys,
                           NULL))
        return false;

    return true;
}


/*****************************************************************************
 *        PATH NODE CREATION ROUTINES
 *****************************************************************************/
#ifdef XCP
/*
 * restrict_distribution
 *    Analyze the RestrictInfo and decide if it is possible to restrict
 *    distribution nodes
 */
static void
restrict_distribution(PlannerInfo *root, RestrictInfo *ri,
                                  Path *pathnode)
{// #lizard forgives
    Distribution   *distribution = pathnode->distribution;
    Oid                keytype;
    Const           *constExpr = NULL;
    bool            found_key = false;
#ifdef _MIGRATE_
    Oid             reloid  = InvalidOid;
    Oid             groupid = InvalidOid;
#endif

    /*
     * Can not restrict - not distributed or key is not defined
     */
    if (distribution == NULL ||
            distribution->distributionExpr == NULL)
        return;

    /*
     * We do not support OR'ed conditions yet
     */
    if (ri->orclause)
        return;

    /*
     * Check if the operator is hash joinable. Currently we only support hash
     * joinable operator for arriving at restricted nodes. This allows us
     * correctly deduce clauses which include a mix of int2/int4/int8 or
     * float4/float8 or clauses which have same type arguments and have a hash
     * joinable operator.
     *
     * Note: This stuff is mostly copied from check_hashjoinable
     */
    {
        Expr       *clause = ri->clause;
        Oid            opno;
        Node       *leftarg;

        if (ri->pseudoconstant)
            return;
        if (!is_opclause(clause))
            return;
        if (list_length(((OpExpr *) clause)->args) != 2)
            return;

        opno = ((OpExpr *) clause)->opno;
        leftarg = linitial(((OpExpr *) clause)->args);

        if (!op_hashjoinable(opno, exprType(leftarg)) ||
                contain_volatile_functions((Node *) clause))
            return;
    }


    keytype = exprType(distribution->distributionExpr);
    if (ri->left_ec)
    {
        EquivalenceClass *ec = ri->left_ec;
        ListCell *lc;
        foreach(lc, ec->ec_members)
        {
            EquivalenceMember *em = (EquivalenceMember *) lfirst(lc);
            if (equal(em->em_expr, distribution->distributionExpr))
                found_key = true;
            else if (bms_is_empty(em->em_relids))
            {
                Expr *cexpr = (Expr *) eval_const_expressions(root,
                                                       (Node *) em->em_expr);
                if (IsA(cexpr, Const))
                    constExpr = (Const *) cexpr;
            }
        }
    }
    if (ri->right_ec)
    {
        EquivalenceClass *ec = ri->right_ec;
        ListCell *lc;
        foreach(lc, ec->ec_members)
        {
            EquivalenceMember *em = (EquivalenceMember *) lfirst(lc);
            if (equal(em->em_expr, distribution->distributionExpr))
                found_key = true;
            else if (bms_is_empty(em->em_relids))
            {
                Expr *cexpr = (Expr *) eval_const_expressions(root,
                                                       (Node *) em->em_expr);
                if (IsA(cexpr, Const))
                    constExpr = (Const *) cexpr;
            }
        }
    }
    if (IsA(ri->clause, OpExpr))
    {
        OpExpr *opexpr = (OpExpr *) ri->clause;
        if (opexpr->args->length == 2 &&
                op_mergejoinable(opexpr->opno, exprType(linitial(opexpr->args))))
        {
            Expr *arg1 = (Expr *) linitial(opexpr->args);
            Expr *arg2 = (Expr *) lsecond(opexpr->args);
            Expr *other = NULL;
#ifndef _PG_REGRESS_
            Var *var1 = (Var *)get_var_from_arg((Node *)arg1);
            Var *var2 = (Var *)get_var_from_arg((Node *)arg2);

            if (var1)
            {
                arg1 = (Expr *)var1;
            }

            if (var2)
            {
                arg2 = (Expr *)var2;
            }
#endif
            if (equal(arg1, distribution->distributionExpr))
                other = arg2;
            else if (equal(arg2, distribution->distributionExpr))
                other = arg1;
            if (other)
            {
                found_key = true;
                other = (Expr *) eval_const_expressions(root, (Node *) other);
                if (IsA(other, Const))
                    constExpr = (Const *) other;
            }
        }
    }
    if (found_key && constExpr)
    {
        List        *nodeList = NIL;
        Bitmapset  *tmpset = bms_copy(distribution->nodes);
        Bitmapset  *restrictinfo = NULL;
        Locator    *locator;
        int           *nodenums;
        int         i, count;
#ifdef __COLD_HOT__
        AttrNumber  secAttrNum = InvalidAttrNumber;
#endif

        while((i = bms_first_member(tmpset)) >= 0)
            nodeList = lappend_int(nodeList, i);
        bms_free(tmpset);
        
#ifdef _MIGRATE_
        if (!nodeList)
        {
            elog(DEBUG5, "[restrict_distribution]nodeList is empty");
            return ;
        }

        //for (i = 1; i < root->simple_rel_array_size; i++)
        if (pathnode->parent->relid < root->simple_rel_array_size)
        {
            i = pathnode->parent->relid;
            if (root->simple_rte_array[i]->rtekind == RTE_RELATION
                && root->simple_rte_array[i]->relkind == 'r')
            {
                reloid = root->simple_rte_array[i]->relid;
                elog(DEBUG5, "[restrict_distribution]reloid=%d", reloid);
            }
        }
        groupid = GetRelGroup(reloid);
        elog(DEBUG5, "[restrict_distribution]groupid=%d", groupid);

        if (!OidIsValid(groupid) && distribution->distributionType == LOCATOR_TYPE_SHARD)
        {
            elog(ERROR, "could not get group info for shard table %u", reloid);
        }

#ifdef __COLD_HOT__
        if (OidIsValid(reloid))
        {
            Relation rel = heap_open(reloid, NoLock);

            if (rel->rd_locator_info && AttributeNumberIsValid(rel->rd_locator_info->secAttrNum))
            {
                secAttrNum = rel->rd_locator_info->secAttrNum;
            }

            heap_close(rel, NoLock);
        }
#endif

        locator = createLocator(distribution->distributionType,
                                RELATION_ACCESS_READ,
                                keytype,
                                LOCATOR_LIST_LIST,
                                0,
                                (void *) nodeList,
                                (void **) &nodenums,
                                false,
                                groupid, GetRelColdGroup(reloid), InvalidOid, secAttrNum,
                                reloid);
#else
        locator = createLocator(distribution->distributionType,
                                RELATION_ACCESS_READ,
                                keytype,
                                LOCATOR_LIST_LIST,
                                0,
                                (void *) nodeList,
                                (void **) &nodenums,
                                false);
#endif
#ifdef __COLD_HOT__
        count = GET_NODES(locator, constExpr->constvalue,
                          constExpr->constisnull, 0, true, NULL);
#else
        count = GET_NODES(locator, constExpr->constvalue,
                          constExpr->constisnull, NULL);
#endif

        for (i = 0; i < count; i++)
            restrictinfo = bms_add_member(restrictinfo, nodenums[i]);
        if (distribution->restrictNodes)
            distribution->restrictNodes = bms_intersect(distribution->restrictNodes,
                                                        restrictinfo);
        else
            distribution->restrictNodes = restrictinfo;
        list_free(nodeList);
        freeLocator(locator);
    }
}

/*
 * set_scanpath_distribution
 *      Assign distribution to the path which is a base relation scan.
 */
static void
set_scanpath_distribution(PlannerInfo *root, RelOptInfo *rel, Path *pathnode)
{// #lizard forgives
    RangeTblEntry   *rte;
    RelationLocInfo *rel_loc_info;

    rte = planner_rt_fetch(rel->relid, root);
    rel_loc_info = GetRelationLocInfo(rte->relid);
    
#ifdef __TBASE__
    /* 
     * get group oid which base rel belongs to, and used later at end of planner.
     * local tables not included.
     */
    if (IS_PGXC_COORDINATOR)
    {
        if (rel_loc_info)
        {
            Oid group = InvalidOid;

            if (rel_loc_info->locatorType == LOCATOR_TYPE_SHARD ||
                rel_loc_info->locatorType == LOCATOR_TYPE_REPLICATED)
                group = GetRelGroup(rte->relid);
            else
                group = InvalidOid;

#ifdef __COLD_HOT__
            if (AttributeNumberIsValid(rel_loc_info->secAttrNum) 
                || OidIsValid(rel_loc_info->coldGroupId))
            {
                has_cold_hot_table = true;
            }
            else
            {
                if (rel_loc_info->locatorType == LOCATOR_TYPE_REPLICATED
                    && !OidIsValid(group))
                {
                    /* do nothing */
                }
                else
                {
                    groupOids = list_append_unique_oid(groupOids, group);
                }
            }
#endif
        }
    }
#endif

    if (IS_PGXC_COORDINATOR && rel_loc_info)
    {
        ListCell *lc;
        bool retry = true;
        Distribution *distribution = makeNode(Distribution);
        distribution->distributionType = rel_loc_info->locatorType;
        /*
         * for LOCATOR_TYPE_REPLICATED distribution, check if
         * all of the mentioned nodes are hale and hearty. Remove
         * those which are not. Do this only for SELECT queries!
         */
retry_pools:
        if (root->parse->commandType == CMD_SELECT &&
                distribution->distributionType == LOCATOR_TYPE_REPLICATED)
        {
            int i;
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
                if (healthmap[i++] == true)
                    distribution->nodes = bms_add_member(distribution->nodes,
                                                         lfirst_int(lc));
            }

            if (healthmap)
            {
                pfree(healthmap);
                healthmap = NULL;
            }

            if (bms_is_empty(distribution->nodes))
            {
                /*
                 * Try an on-demand pool maintenance just to see if some nodes
                 * have come back.
                 *
                 * Try once and error out if datanodes are still down
                 */
                if (retry)
                {
                    PoolPingNodes();
                    retry = false;
                    goto retry_pools;
                }
                else
                    elog(ERROR,
                         "Could not find healthy nodes for replicated table. Exiting!");
            }
        }
        else
        {
            foreach(lc, rel_loc_info->rl_nodeList)
                distribution->nodes = bms_add_member(distribution->nodes,
                                                     lfirst_int(lc));
        }

        distribution->restrictNodes = NULL;
        /*
         * Distribution expression of the base relation is Var representing
         * respective attribute.
         */
        distribution->distributionExpr = NULL;
        if (rel_loc_info->partAttrNum)
        {
            Var        *var = NULL;
            ListCell   *lc;

            /* Look if the Var is already in the target list */
            foreach (lc, rel->reltarget->exprs)
            {
                var = (Var *) lfirst(lc);
                if (IsA(var, Var) && var->varno == rel->relid &&
                        var->varattno == rel_loc_info->partAttrNum)
                    break;
            }
            /* If not found we should look up the attribute and make the Var */
            if (!lc)
            {
                Relation     relation = heap_open(rte->relid, NoLock);
                TupleDesc    tdesc = RelationGetDescr(relation);
                Form_pg_attribute att_tup;

                att_tup = tdesc->attrs[rel_loc_info->partAttrNum - 1];
                var = makeVar(rel->relid, rel_loc_info->partAttrNum,
                              att_tup->atttypid, att_tup->atttypmod,
                              att_tup->attcollation, 0);


                heap_close(relation, NoLock);
            }

            distribution->distributionExpr = (Node *) var;
        }
        pathnode->distribution = distribution;
    }
}

/*
 * create_remotesubplan_path
 *    Redistribute the data to match the distribution.
 *
 * Creates a RemoteSubPath on top of the path, redistributing the data
 * according to the specified distribution.
 */
Path *
create_remotesubplan_path(PlannerInfo *root, Path *subpath,
                          Distribution *distribution)
{
    RelOptInfo       *rel = subpath->parent;
    RemoteSubPath  *pathnode;
	Distribution   *subDist = subpath->distribution;

    pathnode = makeNode(RemoteSubPath);
    pathnode->path.pathtype = T_RemoteSubplan;
    pathnode->path.parent = rel;
    pathnode->path.param_info = subpath->param_info;
    pathnode->path.pathkeys = subpath->pathkeys;
    pathnode->subpath = subpath;
    pathnode->path.distribution = (Distribution *) copyObject(distribution);

    /* We don't want to run subplains in parallel workers */
    pathnode->path.parallel_aware = false;
    pathnode->path.parallel_safe = false;

    pathnode->path.pathtarget = subpath->pathtarget;

    cost_remote_subplan((Path *) pathnode, subpath->startup_cost,
                        subpath->total_cost, subpath->rows, rel->reltarget->width,
						subDist ? calcDistReplications(subDist->distributionType, subDist->nodes) : 1);

    return (Path *) pathnode;
}

/*
 * redistribute_path
 *     Redistributes the path to match desired distribution parameters.
 *
 * It's also possible to specify desired sort order using pathkeys. If the
 * subpath does not match the order, a Sort node will be added automatically.
 * This is similar to how create_merge_append_path() injects Sort nodes.
 */
static Path *
redistribute_path(PlannerInfo *root, Path *subpath, List *pathkeys,
                  char distributionType, Node* distributionExpr,
                  Bitmapset *nodes, Bitmapset *restrictNodes)
{
    Distribution   *distribution = NULL;
    RelOptInfo       *rel = subpath->parent;
    RemoteSubPath  *pathnode;

     if (distributionType != LOCATOR_TYPE_NONE)
    {
        distribution = makeNode(Distribution);
        distribution->distributionType = distributionType;
        distribution->nodes = nodes;
        distribution->restrictNodes = restrictNodes;
        distribution->distributionExpr = distributionExpr;
    }

    /*
     * If inner path node is a MaterialPath pull it up to store tuples on
     * the destination nodes and avoid sending them over the network.
     */
    if (IsA(subpath, MaterialPath))
    {
        MaterialPath *mpath = (MaterialPath *) subpath;
        /* If subpath is already a RemoteSubPath, just replace distribution */
        if (IsA(mpath->subpath, RemoteSubPath))
        {
            pathnode = (RemoteSubPath *) mpath->subpath;
        }
        else
        {
            pathnode = makeNode(RemoteSubPath);
            pathnode->path.pathtype = T_RemoteSubplan;
            pathnode->path.parent = rel;
            pathnode->path.pathtarget = rel->reltarget;
            pathnode->path.param_info = subpath->param_info;
            pathnode->path.pathkeys = subpath->pathkeys;
            pathnode->subpath = mpath->subpath;

            /* We don't want to run subplains in parallel workers */
            pathnode->path.parallel_aware = false;
            pathnode->path.parallel_safe = false;

            mpath->subpath = (Path *) pathnode;
        }

        subpath = pathnode->subpath;
        pathnode->path.distribution = distribution;
        /* (re)calculate costs */
		cost_remote_subplan((Path *) pathnode,
							subpath->startup_cost,
							subpath->total_cost,
							subpath->rows,
							rel->reltarget->width,
							calcDistReplications(distributionType, nodes));

		mpath->path.distribution = (Distribution *) copyObject(distribution);
        mpath->subpath = (Path *) pathnode;
        cost_material(&mpath->path,
                      pathnode->path.startup_cost,
                      pathnode->path.total_cost,
                      pathnode->path.rows,
                      rel->reltarget->width);
        return (Path *) mpath;
    }
    else
    {
        Cost    input_startup_cost = 0;
        Cost    input_total_cost = 0;

        pathnode = makeNode(RemoteSubPath);
        pathnode->path.pathtype = T_RemoteSubplan;
        pathnode->path.parent = rel;
        pathnode->path.pathtarget = rel->reltarget;
        pathnode->path.param_info = subpath->param_info;
        pathnode->path.pathkeys = pathkeys ? pathkeys : subpath->pathkeys;
        pathnode->path.distribution = distribution;

        /*
         * If we need to insert a Sort node, add it here, so that it gets
         * pushed down to the remote node.
         *
         * This works just like create_merge_append_path, i.e. we only do the
         * costing here and only actually construct the Sort node later in
         * create_remotescan_plan.
         */
        if (pathkeys_contained_in(pathkeys, subpath->pathkeys))
        {
            /* Subpath is adequately ordered, we won't need to sort it */
            input_startup_cost += subpath->startup_cost;
            input_total_cost += subpath->total_cost;
        }
        else
        {
            /* We'll need to insert a Sort node, so include cost for that */
            Path        sort_path;        /* dummy for result of cost_sort */

            cost_sort(&sort_path,
                      root,
                      pathkeys,
                      subpath->total_cost,
                      subpath->parent->tuples,
                      subpath->pathtarget->width,
                      0.0,
                      work_mem,
                      -1.0);

            input_startup_cost += sort_path.startup_cost;
            input_total_cost += sort_path.total_cost;
        }
        pathnode->subpath = subpath;

        /* We don't want to run subplains in parallel workers */
        pathnode->path.parallel_aware = false;
        pathnode->path.parallel_safe = false;

        cost_remote_subplan((Path *) pathnode,
							input_startup_cost,
							input_total_cost,
							subpath->rows,
							rel->reltarget->width,
							calcDistReplications(distributionType, nodes));
        return (Path *) pathnode;
    }
}


/*
 * Analyze join parameters and set distribution of the join node.
 * If there are possible alternate distributions the respective pathes are
 * returned as a list so caller can cost all of them and choose cheapest to
 * continue.
 */
static List *
set_joinpath_distribution(PlannerInfo *root, JoinPath *pathnode)
{// #lizard forgives
    Distribution   *innerd = pathnode->innerjoinpath->distribution;
    Distribution   *outerd = pathnode->outerjoinpath->distribution;
    Distribution   *targetd;
    List           *alternate = NIL;
    List           *restrictClauses = NIL;

    List           *innerpathkeys = pathnode->innerjoinpath->pathkeys;
    List           *outerpathkeys = pathnode->outerjoinpath->pathkeys;
#ifdef __TBASE__
	bool           dml = false;
	bool		   keepResultRelLoc = false;
	PlannerInfo    *top_root = root;
	ResultRelLocation resultRelLoc = RESULT_REL_NONE;

	while(top_root->parent_root)
	{
		top_root = top_root->parent_root;
	}

	if (top_root->parse->commandType == CMD_UPDATE ||
		top_root->parse->commandType == CMD_DELETE)
	{
		dml = true;
	}

	/*
	 * Only top root will consider more restrict rules to make sure
	 * UPDATE/DELETE result relation does not redistributed.
	 */
	if (top_root->parse->commandType == CMD_UPDATE ||
		top_root->parse->commandType == CMD_DELETE)
	{
		/* Set the result relation location */
		resultRelLoc = getResultRelLocation(top_root->parse->resultRelation,
											pathnode->innerjoinpath->parent->relids,
											pathnode->outerjoinpath->parent->relids);

		pathnode->path.parent->resultRelLoc = resultRelLoc;

		if (resultRelLoc != RESULT_REL_NONE)
		{
			keepResultRelLoc = true;
		}
	}
#endif


    /* for mergejoins, override with outersortkeys, if needed */
    if (IsA(pathnode, MergePath))
    {
        MergePath *mpath = (MergePath*)pathnode;

        if (mpath->innersortkeys)
            innerpathkeys = mpath->innersortkeys;

        if (mpath->outersortkeys)
            outerpathkeys = mpath->outersortkeys;
    }

    /* Catalog join */
    if (innerd == NULL && outerd == NULL)
        return NIL;
#ifdef __TBASE__
	/*
	 * DML may need to push down to datanodes, for example:
	 *   DELETE FROM
	 *   	geocode_settings as gc
	 *   USING geocode_settings_default AS gf
	 *   WHERE
	 *   	gf.name = gc.name and gf.setting = gc.setting;
	 * prefer_olap means pulling query up to coordinator node, in case data
	 * re-distribute in TPC-C test case.
	 *
	 * TODO: We need to automatically determine whether we need to pull it up,
	* but not using GUC.
	*/
	if(!dml &&
	   (!prefer_olap ||
	    (root->parse &&
	     root->parse->hasCoordFuncs)))
	{
		goto pull_up;
	}

	/*
	 * the join of cold-hot tables must be pulled up to CN until we find a way 
	 * to determine whether this join occurs in a specific group.
	 */
#ifdef __COLD_HOT__
    if (has_cold_hot_table)
    {
        if (list_length(groupOids) > 1)
        {
            goto pull_up;
        }
        else if (list_length(groupOids) < 1)
        {
            has_cold_hot_table = false;
			elog(ERROR, "cold-hot table joins without groups");
        }
    }
#endif
#endif
    /*
     * If both subpaths are distributed by replication, the resulting
     * distribution will be replicated on smallest common set of nodes.
     * Catalog tables are the same on all nodes, so treat them as replicated
     * on all nodes.
     */
    if ((innerd && IsLocatorReplicated(innerd->distributionType)) &&
        (outerd && IsLocatorReplicated(outerd->distributionType)))
    {
        /* Determine common nodes */
        Bitmapset *common;

        common = bms_intersect(innerd->nodes, outerd->nodes);
        if (bms_is_empty(common))
            goto not_allowed_join;

        /*
         * Join result is replicated on common nodes. Running query on any
         * of them produce correct result.
         */
        targetd = makeNode(Distribution);
        targetd->distributionType = LOCATOR_TYPE_REPLICATED;
        targetd->nodes = common;
        targetd->restrictNodes = NULL;
        pathnode->path.distribution = targetd;
        return alternate;
    }

	/*
	 * Check if we have inner replicated
	 * The "both replicated" case is already checked, so if innerd
	 * is replicated, then outerd is not replicated and it is not NULL.
	 * This case is not acceptable for some join types. If outer relation is
	 * nullable data nodes will produce joined rows with NULLs for cases when
	 * matching row exists, but on other data node.
	 */

	if ((innerd && IsLocatorReplicated(innerd->distributionType)) &&
			(pathnode->jointype == JOIN_INNER ||
			 pathnode->jointype == JOIN_LEFT ||
			 pathnode->jointype == JOIN_SEMI ||
#ifdef __TBASE__
             pathnode->jointype == JOIN_LEFT_SCALAR ||
			 pathnode->jointype == JOIN_LEFT_SEMI ||
#endif
			 pathnode->jointype == JOIN_ANTI))
	{
		/* We need inner relation is defined on all nodes where outer is */
		if (!outerd || !bms_is_subset(outerd->nodes, innerd->nodes))
			goto not_allowed_join;

		targetd = makeNode(Distribution);
		targetd->distributionType = outerd->distributionType;
		targetd->nodes = bms_copy(outerd->nodes);
		targetd->restrictNodes = bms_copy(outerd->restrictNodes);
		targetd->distributionExpr = outerd->distributionExpr;
		pathnode->path.distribution = targetd;
		return alternate;
	}


    /*
     * Check if we have outer replicated
     * The "both replicated" case is already checked, so if outerd
     * is replicated, then innerd is not replicated and it is not NULL.
     * This case is not acceptable for some join types. If inner relation is
     * nullable data nodes will produce joined rows with NULLs for cases when
     * matching row exists, but on other data node.
     */
    if ((outerd && IsLocatorReplicated(outerd->distributionType)) &&
            (pathnode->jointype == JOIN_INNER ||
             pathnode->jointype == JOIN_RIGHT))
    {
        /* We need outer relation is defined on all nodes where inner is */
        if (!innerd || !bms_is_subset(innerd->nodes, outerd->nodes))
            goto not_allowed_join;

        targetd = makeNode(Distribution);
        targetd->distributionType = innerd->distributionType;
        targetd->nodes = bms_copy(innerd->nodes);
        targetd->restrictNodes = bms_copy(innerd->restrictNodes);
        targetd->distributionExpr = innerd->distributionExpr;
        pathnode->path.distribution = targetd;
        return alternate;
    }

    restrictClauses = list_copy(pathnode->joinrestrictinfo);
    restrictClauses = list_concat(restrictClauses,
            pathnode->movedrestrictinfo);

	/*
	 * This join is still allowed if inner and outer paths have equivalent
	 * distribution and joined along the distribution keys. Make sure
	 * distribution functions are the same, for now they depend on data type.
	 */
	if (innerd && outerd &&
		innerd->distributionType == outerd->distributionType &&
		innerd->distributionExpr &&
		outerd->distributionExpr &&
	    bms_equal(innerd->nodes, outerd->nodes) &&
	    BMS_EQUAL_CONSTRAINT(innerd->nodes))
	{
		ListCell   *lc;

        /*
         * Planner already did necessary work and if there is a join
         * condition like left.key=right.key the key expressions
         * will be members of the same equivalence class, and both
         * sides of the corresponding RestrictInfo will refer that
         * Equivalence Class.
         * Try to figure out if such restriction exists.
         */
        foreach(lc, restrictClauses)
        {
			RestrictInfo *ri = (RestrictInfo *) lfirst(lc);
			ListCell	 *emc = NULL;
			bool		  found_outer = false;
			bool		  found_inner = false;

            /*
             * Restriction operator is not equality operator ?
             */
            if (ri->left_ec == NULL || ri->right_ec == NULL)
                continue;

            /*
             * A restriction with OR may be compatible if all OR'ed
             * conditions are compatible. For the moment we do not
             * check this and skip restriction. The case if multiple
             * OR'ed conditions are compatible is rare and probably
             * do not worth doing at all.
             */
            if (ri->orclause)
                continue;

            if (!OidIsValid(ri->hashjoinoperator))
                continue;

            /*
             * If parts belong to the same equivalence member check
             * if both distribution keys are members of the class.
             */
            if (ri->left_ec == ri->right_ec)
            {
                foreach(emc, ri->left_ec->ec_members)
                {
					EquivalenceMember *em 	= (EquivalenceMember *) lfirst(emc);
					Expr			  *var 	= (Expr *)em->em_expr;

                    if (IsA(var, RelabelType))
                        var = ((RelabelType *) var)->arg;
                    if (!found_outer)
                        found_outer = equal(var, outerd->distributionExpr);

                    if (!found_inner)
                        found_inner = equal(var, innerd->distributionExpr);
                }
                if (found_outer && found_inner)
                {
                    ListCell *tlc, *emc;

                    targetd = makeNode(Distribution);
                    targetd->distributionType = innerd->distributionType;
                    targetd->nodes = bms_copy(innerd->nodes);
                    targetd->restrictNodes = bms_copy(innerd->restrictNodes);
                    targetd->distributionExpr = NULL;
                    pathnode->path.distribution = targetd;
                    /*
                     * Each member of the equivalence class may be a
                     * distribution expression, but we prefer some from the
                     * target list.
                     */
                    foreach(tlc, pathnode->path.parent->reltarget->exprs)
                    {
                        Expr *var = (Expr *) lfirst(tlc);
                        foreach(emc, ri->left_ec->ec_members)
                        {
                            EquivalenceMember *em;
                            Expr *emvar;

                            em = (EquivalenceMember *) lfirst(emc);
                            emvar = (Expr *)em->em_expr;
                            if (IsA(emvar, RelabelType))
                                emvar = ((RelabelType *) emvar)->arg;
                            if (equal(var, emvar))
                            {
                                targetd->distributionExpr = (Node *) var;
#ifdef __TBASE__
								/*
								 * For UPDATE/DELETE, make sure we are distributing by
								 * the result relation.
								 */
								if (keepResultRelLoc &&
									!equal_distributions(top_root,
														 top_root->distribution,
														 targetd))
								{
									continue;
								}
#endif
                                return alternate;
                            }
                        }
                    }
                    /* Not found, take any */
                    targetd->distributionExpr = innerd->distributionExpr;

#ifdef __TBASE__
					/*
					 * For UPDATE/DELETE, make sure we are distributing by
					 * the result relation.
					 */
					if (keepResultRelLoc &&
						!equal_distributions(top_root,
											 top_root->distribution,
											 targetd))
					{
						pfree(targetd);
						targetd = NULL;
						continue;
					}
#endif
                    return alternate;
                }
            }
            /*
             * Check clause, if both arguments are distribution keys and
             * operator is an equality operator
             */
            else
            {
                OpExpr *op_exp;
                Expr   *arg1,
                       *arg2;

                op_exp = (OpExpr *) ri->clause;
                if (!IsA(op_exp, OpExpr) || list_length(op_exp->args) != 2)
                    continue;

                arg1 = (Expr *) linitial(op_exp->args);
                arg2 = (Expr *) lsecond(op_exp->args);
#ifndef _PG_REGRESS_
                {        
                    Var *var1 = (Var *)get_var_from_arg((Node *)arg1);
                    Var *var2 = (Var *)get_var_from_arg((Node *)arg2);
        
                    if (var1)
                    {
                        arg1 = (Expr *)var1;
                    }
        
                    if (var2)
                    {
                        arg2 = (Expr *)var2;
                    }
                }
#endif
                found_outer = equal(arg1, outerd->distributionExpr) || equal(arg2, outerd->distributionExpr);
                found_inner = equal(arg1, innerd->distributionExpr) || equal(arg2, innerd->distributionExpr);

                if (found_outer && found_inner)
                {
                    targetd = makeNode(Distribution);
                    targetd->distributionType = innerd->distributionType;
                    targetd->nodes = bms_copy(innerd->nodes);
                    targetd->restrictNodes = bms_copy(innerd->restrictNodes);
                    pathnode->path.distribution = targetd;
#ifndef _PG_REGRESS_
                    if (pathnode->jointype == JOIN_LEFT)
                    {
                        targetd->restrictNodes = bms_copy(outerd->restrictNodes);
                    }
                    else if (pathnode->jointype == JOIN_FULL)
                    {
                        targetd->restrictNodes = bms_union(outerd->restrictNodes, innerd->restrictNodes);
                    }
#endif
					/*
					 * In case of outer join distribution key should not refer
					 * distribution key of nullable part.
					 */
					if (pathnode->jointype == JOIN_FULL)
						/* both parts are nullable */
						targetd->distributionExpr = NULL;
					else if (pathnode->jointype == JOIN_RIGHT)
						targetd->distributionExpr = innerd->distributionExpr;
					else
						targetd->distributionExpr = outerd->distributionExpr;

#ifdef __TBASE__
					/*
					 * For UPDATE/DELETE, make sure we are distributing by
					 * the result relation.
					 */
					if (keepResultRelLoc &&
						!equal_distributions(top_root,
											 top_root->distribution,
											 targetd))
					{
						pfree(targetd);
						targetd = NULL;
						continue;
					}
#endif
					return alternate;
				}
			}
		}
#ifndef _PG_REGRESS_
		if (bms_equal(innerd->restrictNodes, outerd->restrictNodes) &&
			bms_num_members(innerd->restrictNodes) == 1 && restrict_query &&
			pathnode->jointype != JOIN_FULL)
		{
			targetd = makeNode(Distribution);
			targetd->distributionType = innerd->distributionType;
			targetd->nodes = bms_copy(innerd->nodes);
			targetd->restrictNodes = bms_copy(innerd->restrictNodes);
			pathnode->path.distribution = targetd;

			/*
			 * In case of outer join distribution key should not refer
			 * distribution key of nullable part.
			 */
			if (pathnode->jointype == JOIN_FULL)
				/* both parts are nullable */
				targetd->distributionExpr = NULL;
			else if (pathnode->jointype == JOIN_RIGHT)
				targetd->distributionExpr = innerd->distributionExpr;
			else
				targetd->distributionExpr = outerd->distributionExpr;

#ifdef __TBASE__
			/*
			 * For UPDATE/DELETE, make sure we are distributing by
			 * the result relation.
			 */
			if (!keepResultRelLoc || equal_distributions(top_root,
														 top_root->distribution,
														 targetd))
			{
				return alternate;
			}
			else
			{
				pfree(targetd);
				targetd = NULL;
			}
#else
			return alternate;
#endif
		}
#endif
    }

    /*
     * If we could not determine the distribution redistribute the subpathes.
     */
not_allowed_join:
	/*
	 * If redistribution is required, sometimes the cheapest path would be if
	 * one of the subplan is replicated. If replication of any or all subplans
	 * is possible, return resulting plans as alternates. Try to distribute all
	 * by has as main variant.
	 */

#ifdef NOT_USED	
	/* These join types allow replicated inner */
#ifdef __TBASE__
	if (outerd &&
			(pathnode->jointype == JOIN_INNER ||
			 pathnode->jointype == JOIN_LEFT ||
			 pathnode->jointype == JOIN_SEMI ||
			 pathnode->jointype == JOIN_LEFT_SCALAR ||
			 pathnode->jointype == JOIN_ANTI))
#else
	if (outerd &&
			(pathnode->jointype == JOIN_INNER ||
			 pathnode->jointype == JOIN_LEFT ||
			 pathnode->jointype == JOIN_SEMI ||
			 pathnode->jointype == JOIN_ANTI))
#endif
	{
		/*
		 * Since we discard all alternate pathes except one it is OK if all they
		 * reference the same objects
		 */
		JoinPath *altpath = flatCopyJoinPath(pathnode);
		/* Redistribute inner subquery */
		altpath->innerjoinpath = redistribute_path(
				root,
				altpath->innerjoinpath,
				innerpathkeys,
				LOCATOR_TYPE_REPLICATED,
				NULL,
				bms_copy(outerd->nodes),
				bms_copy(outerd->restrictNodes));
		targetd = makeNode(Distribution);
		targetd->distributionType = outerd->distributionType;
		targetd->nodes = bms_copy(outerd->nodes);
		targetd->restrictNodes = bms_copy(outerd->restrictNodes);
		targetd->distributionExpr = outerd->distributionExpr;
		altpath->path.distribution = targetd;
		alternate = lappend(alternate, altpath);
	}

    /* These join types allow replicated outer */
    if (innerd &&
            (pathnode->jointype == JOIN_INNER ||
             pathnode->jointype == JOIN_RIGHT))
    {
        /*
         * Since we discard all alternate pathes except one it is OK if all they
         * reference the same objects
         */
        JoinPath *altpath = flatCopyJoinPath(pathnode);
        /* Redistribute inner subquery */
        altpath->outerjoinpath = redistribute_path(
                root,
                altpath->outerjoinpath,
                outerpathkeys,
                LOCATOR_TYPE_REPLICATED,
                NULL,
                bms_copy(innerd->nodes),
                bms_copy(innerd->restrictNodes));
        targetd = makeNode(Distribution);
        targetd->distributionType = innerd->distributionType;
        targetd->nodes = bms_copy(innerd->nodes);
        targetd->restrictNodes = bms_copy(innerd->restrictNodes);
        targetd->distributionExpr = innerd->distributionExpr;
        altpath->path.distribution = targetd;
        alternate = lappend(alternate, altpath);
    }
#endif

    /*
     * Redistribute subplans to make them compatible.
     * If any of the subplans is a coordinator subplan skip this stuff and do
     * coordinator join.
     */
    if (innerd && outerd)
    {
        RestrictInfo   *preferred = NULL;
        Expr           *new_inner_key = NULL;
        Expr           *new_outer_key = NULL;
        char            distType = LOCATOR_TYPE_NONE;
        ListCell        *lc;
#ifdef __TBASE__
		Oid				group;
		int 			nRemotePlans_outer = 0;
		int 			nRemotePlans_inner = 0;
		bool 			redistribute_outer = false;
		bool 			redistribute_inner = false;
#endif

        /*
         * Look through the join restrictions to find one that is a hashable
         * operator on two arguments. Choose best restriction acoording to
         * following criteria:
         * 1. one argument is already a partitioning key of one subplan.
         * 2. restriction is cheaper to calculate
         */
        foreach(lc, restrictClauses)
        {
            RestrictInfo   *ri = (RestrictInfo *) lfirst(lc);

            /* can not handle ORed conditions */
            if (ri->orclause)
                continue;

            if (IsA(ri->clause, OpExpr))
            {
                OpExpr *expr = (OpExpr *) ri->clause;
                if (list_length(expr->args) == 2 &&
                        op_hashjoinable(expr->opno, exprType(linitial(expr->args))))
                {
                    Expr *left = (Expr *) linitial(expr->args);
                    Expr *right = (Expr *) lsecond(expr->args);
#ifdef __TBASE__
                    Expr *left_expr = left;
                    Expr *right_expr = right;
#endif
					Oid leftType PG_USED_FOR_ASSERTS_ONLY = exprType((Node *) left);
#ifndef __TBASE__
					Oid rightType PG_USED_FOR_ASSERTS_ONLY = exprType((Node *) right);
#endif
					Relids inner_rels = pathnode->innerjoinpath->parent->relids;
					Relids outer_rels = pathnode->outerjoinpath->parent->relids;
					QualCost cost;

#ifndef	__TBASE__
					/*
					 * Check if both parts are of the same data type and choose
					 * distribution type to redistribute.
					 * XXX We may want more sophisticated algorithm to choose
					 * the best condition to redistribute parts along.
					 * For now use simple but reliable approach.
					 */
					if (leftType != rightType)
						continue;
#endif
#ifndef _PG_REGRESS_
                    {        
                        Expr *expr1 = (Expr *)get_var_from_arg((Node *)left);
                        Expr *expr2 = (Expr *)get_var_from_arg((Node *)right);
            
                        if (expr1)
                        {
                            left_expr = expr1;
                        }
            
                        if (expr2)
                        {
                            right_expr = expr2;
                        }

                        if (IsA(left_expr, Const) || IsA(right_expr, Const))
                        {
                            continue;
                        }
                    }
#endif
					/*
					 * Evaluation cost will be needed to choose preferred
					 * distribution
					 */
					cost_qual_eval_node(&cost, (Node *) ri, root);

					if (outerd->distributionExpr && BMS_EQUAL_CONSTRAINT(outerd->nodes))
					{
#ifdef __TBASE__
						/*
						 * For UPDATE/DELETE, make sure outer rel does not need
						 * to distribute
						 */
						if (keepResultRelLoc && resultRelLoc == RESULT_REL_INNER)
							continue;
#endif
						/*
						 * If left side is distribution key of outer subquery
						 * and right expression refers only inner subquery
						 */
						if (equal(outerd->distributionExpr, left_expr) &&
								bms_is_subset(ri->right_relids, inner_rels))
						{
							if (!preferred || /* no preferred restriction yet found */
								(new_inner_key && new_outer_key) || /* preferred restriction require redistribution of both parts */
								(cost.per_tuple < preferred->eval_cost.per_tuple)) /* current restriction is cheaper */
							{
								/* set new preferred restriction */
								preferred = ri;
								new_inner_key = right;
								new_outer_key = NULL; /* no need to change */
								distType = outerd->distributionType;
							}
							continue;
						}
						/*
						 * If right side is distribution key of outer subquery
						 * and left expression refers only inner subquery
						 */
						if (equal(outerd->distributionExpr, right_expr) &&
								bms_is_subset(ri->left_relids, inner_rels))
						{
							if (!preferred || /* no preferred restriction yet found */
								(new_inner_key && new_outer_key) || /* preferred restriction require redistribution of both parts */
								(cost.per_tuple < preferred->eval_cost.per_tuple)) /* current restriction is cheaper */
							{
								/* set new preferred restriction */
								preferred = ri;
								new_inner_key = left;
								new_outer_key = NULL; /* no need to change */
								distType = outerd->distributionType;
							}
							continue;
						}
					}
					if (innerd->distributionExpr && BMS_EQUAL_CONSTRAINT(innerd->nodes))
					{
#ifdef __TBASE__
						/* For UPDATE/DELETE, make sure inner rel does not need to distribute */
						if (keepResultRelLoc && resultRelLoc == RESULT_REL_OUTER)
							continue;
#endif
						/*
						 * If left side is distribution key of inner subquery
						 * and right expression refers only outer subquery
						 */
						if (equal(innerd->distributionExpr, left_expr) &&
								bms_is_subset(ri->right_relids, outer_rels))
						{
							if (!preferred || /* no preferred restriction yet found */
									(new_inner_key && new_outer_key) || /* preferred restriction require redistribution of both parts */
									(cost.per_tuple < preferred->eval_cost.per_tuple)) /* current restriction is cheaper */
							{
								/* set new preferred restriction */
								preferred = ri;
								new_inner_key = NULL; /* no need to change */
								new_outer_key = right;
								distType = innerd->distributionType;
							}
							continue;
						}
						/*
						 * If right side is distribution key of inner subquery
						 * and left expression refers only outer subquery
						 */
						if (equal(innerd->distributionExpr, right_expr) &&
								bms_is_subset(ri->left_relids, outer_rels))
						{
							if (!preferred || /* no preferred restriction yet found */
									(new_inner_key && new_outer_key) || /* preferred restriction require redistribution of both parts */
									(cost.per_tuple < preferred->eval_cost.per_tuple)) /* current restriction is cheaper */
							{
								/* set new preferred restriction */
								preferred = ri;
								new_inner_key = NULL; /* no need to change */
								new_outer_key = left;
								distType = innerd->distributionType;
							}
							continue;
						}
					}
					/*
					 * Current restriction recuire redistribution of both parts.
					 * If preferred restriction require redistribution of one,
					 * keep it.
					 */
					if (preferred &&
							(new_inner_key == NULL || new_outer_key == NULL))
						continue;

					/*
					 * Skip this condition if the data type of the expressions
					 * does not allow either HASH or MODULO distribution.
					 * HASH distribution is preferrable.
					 */
#ifdef __TBASE__
                    if (groupOids)
                    {
                        group = linitial_oid(groupOids);
                    }
                    else
                    {
                        group = InvalidOid;
                    }

                    if (OidIsValid(group))
                    {
                        distType = LOCATOR_TYPE_SHARD;
                    }
                    else
                    {
#endif
                    if (IsTypeHashDistributable(leftType))
                        distType = LOCATOR_TYPE_HASH;
                    else if (IsTypeModuloDistributable(leftType))
                        distType = LOCATOR_TYPE_MODULO;
                    else
                        continue;
#ifdef __TBASE__
					}

					/*
					 * Skip redistribute both side, which will redistribute the
					 * result relation
					 */
					if (keepResultRelLoc)
						continue;
#endif
					/*
					 * If this restriction the first or easier to calculate
					 * then preferred, try to store it as new preferred
					 * restriction to redistribute along it.
					 */
					if (preferred == NULL ||
						(cost.per_tuple < preferred->eval_cost.per_tuple))
					{
						/*
						 * Left expression depends only on outer subpath and
						 * right expression depends only on inner subpath, so
						 * we can redistribute both and make left expression the
						 * distribution key of outer subplan and right
						 * expression the distribution key of inner subplan
						 */
						if (bms_is_subset(ri->left_relids, outer_rels) &&
								bms_is_subset(ri->right_relids, inner_rels))
						{
							preferred = ri;
							new_outer_key = left;
							new_inner_key = right;
						}
						/*
						 * Left expression depends only on inner subpath and
						 * right expression depends only on outer subpath, so
						 * we can redistribute both and make left expression the
						 * distribution key of inner subplan and right
						 * expression the distribution key of outer subplan
						 */
						if (bms_is_subset(ri->left_relids, inner_rels) &&
								bms_is_subset(ri->right_relids, outer_rels))
						{
							preferred = ri;
							new_inner_key = left;
							new_outer_key = right;
						}
					}
				}
			}
		}

#ifdef __TBASE__
		contains_remotesubplan(pathnode->outerjoinpath, &nRemotePlans_outer, &redistribute_outer);
		contains_remotesubplan(pathnode->innerjoinpath, &nRemotePlans_inner, &redistribute_inner);
#endif

		/* If we have suitable restriction we can repartition accordingly */
		if (preferred)
		{
			Bitmapset *nodes = NULL;
			Bitmapset *restrictNodes = NULL;
#ifdef __TBASE__
			/* consider the outer/inner size when make the redistribute plan */
			bool replicate_inner = false;
			bool replicate_outer = false;
			RelOptInfo *outer_rel = pathnode->outerjoinpath->parent;
			RelOptInfo *inner_rel = pathnode->innerjoinpath->parent;
			double outer_size = outer_rel->rows * outer_rel->reltarget->width;
			double inner_size = inner_rel->rows * inner_rel->reltarget->width;
			int outer_nodes = bms_num_members(outerd->nodes);
			int inner_nodes = bms_num_members(innerd->nodes);
#endif

            /* If we redistribute both parts do join on all nodes ... */
            if (new_inner_key && new_outer_key)
            {
				if (bms_is_empty(constrainNodes))
				{
                int i;
                for (i = 0; i < NumDataNodes; i++)
                    nodes = bms_add_member(nodes, i);

#ifdef __TBASE__
					if (OidIsValid(group))
				{
					int      node_index;
					int32	 dn_num;
					int32   *datanodes;

					GetShardNodes(group, &datanodes, &dn_num, NULL);

					bms_free(nodes);
					nodes = NULL;
					
					for(node_index = 0; node_index < dn_num; node_index++)
					{						
						nodes = bms_add_member(nodes, datanodes[node_index]);
					}
				}

				/*
				 * We should not get both new_inner_key & new_outer_key for
				 * UPDATE/DELETE
				 */
				Assert(!keepResultRelLoc);

				/*
				 * if any side is smaller enough, replicate the smaller one
				 * instead of redistribute both of them.
                 */
                if(inner_size * outer_nodes < inner_size + outer_size &&
                    (pathnode->jointype != JOIN_RIGHT && pathnode->jointype != JOIN_FULL) &&
                    outerd->distributionType != LOCATOR_TYPE_REPLICATED && !redistribute_inner &&
                    get_num_connections(outer_nodes, nRemotePlans_inner + 1) < MaxConnections * REPLICATION_FACTOR &&
					!dml && nRemotePlans_inner < replication_level && !pathnode->inner_unique)
                {
                    replicate_inner = true;

                    nodes = bms_copy(outerd->nodes);
                }

				if(outer_size * inner_nodes < inner_size + outer_size &&
					(pathnode->jointype != JOIN_LEFT &&
					 pathnode->jointype != JOIN_FULL &&
					 pathnode->jointype != JOIN_SEMI &&
					 pathnode->jointype != JOIN_LEFT_SCALAR &&
					 pathnode->jointype != JOIN_LEFT_SEMI &&
					 pathnode->jointype != JOIN_ANTI) &&
					 innerd->distributionType != LOCATOR_TYPE_REPLICATED && !redistribute_outer &&
					 get_num_connections(inner_nodes, nRemotePlans_outer + 1) < MaxConnections * REPLICATION_FACTOR &&
					 !replicate_inner && !dml && nRemotePlans_outer < replication_level && !pathnode->inner_unique)
				{
					replicate_outer = true;

					nodes = bms_copy(innerd->nodes);
				}
#endif
            }
				else
				{
					nodes = bms_copy(constrainNodes);
					replicate_inner = false;
					replicate_outer = false;
				}
			}
            /*
             * ... if we do only one of them redistribute it on the same nodes
             * as other.
             */
            else if (new_inner_key)
            {
#ifdef __TBASE__
				/*
				 * If inner is smaller than outer, redistribute inner as the
				 * preferred key we picked.
				 * If inner is bigger than outer (inner > inner->nodes * outer),
				 * replicate outer as an optimization to save network costs.
                 */
				if(inner_size > outer_size * inner_nodes &&
					(pathnode->jointype != JOIN_LEFT &&
					 pathnode->jointype != JOIN_FULL &&
					 pathnode->jointype != JOIN_SEMI &&
					 pathnode->jointype != JOIN_LEFT_SCALAR &&
					 pathnode->jointype != JOIN_LEFT_SEMI &&
					 pathnode->jointype != JOIN_ANTI) &&
					 innerd->distributionType != LOCATOR_TYPE_REPLICATED && !redistribute_outer &&
					 get_num_connections(inner_nodes, nRemotePlans_outer + 1) < MaxConnections * REPLICATION_FACTOR &&
					 !dml && nRemotePlans_outer < replication_level && !pathnode->inner_unique)
				{
					replicate_outer = true;

					/* replicate outer to all inner nodes */
					nodes = bms_copy(innerd->nodes);
					restrictNodes = bms_copy(innerd->restrictNodes);
				}
				else
				{
					Assert(!keepResultRelLoc || resultRelLoc != RESULT_REL_INNER);
#endif
					nodes = bms_copy(outerd->nodes);
					restrictNodes = bms_copy(outerd->restrictNodes);
#ifdef __TBASE__
                }
#endif
            }
            else /*if (new_outer_key)*/
            {
#ifdef __TBASE__
				/*
				 * If outer is smaller than inner, redistribute outer as the
				 * preferred key we picked.
				 * If outer is bigger than inner (outer > outer->nodes * inner),
				 * replicate inner as an optimization to save network costs.
				 */
				if (outer_size > inner_size * outer_nodes &&
					(pathnode->jointype != JOIN_RIGHT && pathnode->jointype != JOIN_FULL) &&
					outerd->distributionType != LOCATOR_TYPE_REPLICATED && !redistribute_inner &&
					get_num_connections(outer_nodes, nRemotePlans_inner + 1) < MaxConnections * REPLICATION_FACTOR &&
					!dml && nRemotePlans_inner < replication_level && !pathnode->inner_unique)
				{
					replicate_inner = true;

					/* replicate inner to all outer nodes */
					nodes = bms_copy(outerd->nodes);
					restrictNodes = bms_copy(outerd->restrictNodes);
				}
				else
				{
					Assert(!keepResultRelLoc || resultRelLoc != RESULT_REL_OUTER);
#endif
					nodes = bms_copy(innerd->nodes);
					restrictNodes = bms_copy(innerd->restrictNodes);
#ifdef __TBASE__
                }
#endif
            }

            /*
             * Redistribute join by hash, and, if jointype allows, create
             * alternate path where inner subplan is distributed by replication
             */
            if (new_inner_key)
            {
#ifdef __TBASE__
				/*
				 * replicate outer rel, just set LOCATOR_TYPE_NONE to remove
				 * the path distribution.
				 */
                if(replicate_outer)
                {
                    pathnode->outerjoinpath = redistribute_path(
                                                root,
                                                pathnode->outerjoinpath,
                                                outerpathkeys,
                                                LOCATOR_TYPE_NONE,
                                                NULL,
                                                innerd->nodes,
                                                NULL);

                    if (IsA(pathnode, MergePath))
                        ((MergePath*)pathnode)->outersortkeys = NIL;
                }
                else if(!replicate_inner)
                {
#endif
                /* Redistribute inner subquery */
                pathnode->innerjoinpath = redistribute_path(
                        root,
                        pathnode->innerjoinpath,
                        innerpathkeys,
                        distType,
                        (Node *) new_inner_key,
                        nodes,
                        restrictNodes);

                if (IsA(pathnode, MergePath))
                    ((MergePath*)pathnode)->innersortkeys = NIL;
#ifdef __TBASE__
                }
#endif
            }
            /*
             * Redistribute join by hash, and, if jointype allows, create
             * alternate path where outer subplan is distributed by replication
             */
            if (new_outer_key)
            {
#ifdef __TBASE__
                /*
				 * replicate inner rel, just set LOCATOR_TYPE_NONE to remove
				 * the path distribution.
				 */
                if(replicate_inner)
                {
                    pathnode->innerjoinpath = redistribute_path(
                                                root,
                                                pathnode->innerjoinpath,
                                                innerpathkeys,
                                                LOCATOR_TYPE_NONE,
                                                NULL,
                                                outerd->nodes,
                                                NULL);

                    if (IsA(pathnode, MergePath))
                        ((MergePath*)pathnode)->innersortkeys = NIL;
                }
                else if(!replicate_outer)
                {
#endif
                /* Redistribute outer subquery */
                pathnode->outerjoinpath = redistribute_path(
                        root,
                        pathnode->outerjoinpath,
                        outerpathkeys,
                        distType,
                        (Node *) new_outer_key,
                        nodes,
                        restrictNodes);

                if (IsA(pathnode, MergePath))
                    ((MergePath*)pathnode)->outersortkeys = NIL;
#ifdef __TBASE__
                }
#endif
            }
            targetd = makeNode(Distribution);
            targetd->distributionType = distType;
            targetd->nodes = nodes;
            targetd->restrictNodes = NULL;
            pathnode->path.distribution = targetd;

            /*
             * For mergejoins we can also reset the sortkeys, because
             * redistribute_path will take care of that by creating a nested
             * Sort if needed. Otherwise create_mergejoin_plan would add
             * another sort node, but we want to push it down.
             */
            if (IsA(pathnode, MergePath))
            {
                // ((MergePath*)pathnode)->innersortkeys = NIL;
                // ((MergePath*)pathnode)->outersortkeys = NIL;
            }

            /*
             * In case of outer join distribution key should not refer
             * distribution key of nullable part.
             * NB: we should not refer innerd and outerd here, subpathes are
             * redistributed already
             */
            if (pathnode->jointype == JOIN_FULL)
                /* both parts are nullable */
                targetd->distributionExpr = NULL;
            else if (pathnode->jointype == JOIN_RIGHT)
                targetd->distributionExpr =
                        pathnode->innerjoinpath->distribution->distributionExpr;
            else
#ifdef __TBASE__
                if(replicate_outer)
                    targetd->distributionExpr =
                            pathnode->innerjoinpath->distribution->distributionExpr;
                else
#endif
                targetd->distributionExpr =
                        pathnode->outerjoinpath->distribution->distributionExpr;

			return alternate;
		}

#ifdef __TBASE__
		if (keepResultRelLoc)
		{
			/*
			 * We didn't got the preferred redistribution plan for UPDATE/DELETE.
			 * Thus, to keeping result relation not redistributed, we replicate
			 * the other subpath.
			 */
			if (resultRelLoc == RESULT_REL_INNER &&
				pathnode->jointype != JOIN_LEFT && pathnode->jointype != JOIN_FULL &&
				pathnode->jointype != JOIN_SEMI && pathnode->jointype != JOIN_ANTI &&
				pathnode->jointype != JOIN_LEFT_SCALAR &&
				pathnode->jointype != JOIN_LEFT_SEMI && !pathnode->inner_unique)
			{
				/* Replicate outer */
				pathnode->outerjoinpath = redistribute_path(
											root,
											pathnode->outerjoinpath,
											outerpathkeys,
											LOCATOR_TYPE_NONE,
											NULL,
											innerd->nodes,
											NULL);
				pathnode->path.distribution = innerd;

				if (IsA(pathnode, MergePath))
					((MergePath*)pathnode)->outersortkeys = NIL;
			}
			else if (resultRelLoc == RESULT_REL_OUTER &&
					 pathnode->jointype != JOIN_RIGHT && pathnode->jointype != JOIN_FULL &&
					 !pathnode->inner_unique)
			{
				/* Replicate inner */
				pathnode->innerjoinpath = redistribute_path(
											root,
											pathnode->innerjoinpath,
											innerpathkeys,
											LOCATOR_TYPE_NONE,
											NULL,
											outerd->nodes,
											NULL);
				pathnode->path.distribution = outerd;

				if (IsA(pathnode, MergePath))
					((MergePath*)pathnode)->innersortkeys = NIL;
			}

			return alternate;
		}
	}

	/* 
	 * For DELETE/UPDATE, If the other side already been replicated, we directly
	 * inherit the resultRelLoc side distribution.
	 */
	if (keepResultRelLoc)
	{
		if (innerd &&resultRelLoc == RESULT_REL_INNER &&
			pathnode->jointype != JOIN_LEFT && pathnode->jointype != JOIN_FULL &&
			pathnode->jointype != JOIN_SEMI && pathnode->jointype != JOIN_ANTI &&
			pathnode->jointype != JOIN_LEFT_SCALAR &&
			pathnode->jointype != JOIN_LEFT_SEMI && !pathnode->inner_unique)
		{
			pathnode->path.distribution = innerd;
			return alternate;
		}
		else if (outerd && resultRelLoc == RESULT_REL_OUTER &&
				 pathnode->jointype != JOIN_RIGHT && pathnode->jointype != JOIN_FULL &&
				 !pathnode->inner_unique)
		{
			pathnode->path.distribution = outerd;
			return alternate;
		}
#endif
	}

    /*
     * Build cartesian product, if no hasheable restrictions is found.
     * Perform coordinator join in such cases. If this join would be a part of
     * larger join, it will be handled as replicated.
     * To do that leave join distribution NULL and place a RemoteSubPath node on
     * top of each subpath to provide access to joined result sets.
     * Do not redistribute pathes that already have NULL distribution, this is
     * possible if performing outer join on a coordinator and a datanode
     * relations.
     */
#if 0
    if (innerd && !outerd)
    {
        if (IsLocatorDistributedByValue(innerd->distributionType) &&
            !pathnode->innerjoinpath->param_info &&
            !pathnode->outerjoinpath->param_info &&
            pathnode->jointype == JOIN_INNER)
        {
            pathnode->path.distribution = copyObject(pathnode->innerjoinpath->distribution);

            return alternate;
        }
    }
    
    if (!innerd && outerd)
    {
        if (IsLocatorDistributedByValue(outerd->distributionType) &&
            !pathnode->innerjoinpath->param_info &&
            !pathnode->outerjoinpath->param_info &&
            pathnode->jointype == JOIN_INNER)
        {
            pathnode->path.distribution = copyObject(pathnode->outerjoinpath->distribution);

            return alternate;
        }
    }
#endif
pull_up:
    if (innerd)
    {
        pathnode->innerjoinpath = redistribute_path(root,
                                                    pathnode->innerjoinpath,
                                                    innerpathkeys,
                                                    LOCATOR_TYPE_NONE,
                                                    NULL,
                                                    NULL,
                                                    NULL);

        if (IsA(pathnode, MergePath))
            ((MergePath*)pathnode)->innersortkeys = NIL;
    }

    if (outerd)
    {
        pathnode->outerjoinpath = redistribute_path(root,
                                                    pathnode->outerjoinpath,
                                                    outerpathkeys,
                                                    LOCATOR_TYPE_NONE,
                                                    NULL,
                                                    NULL,
                                                    NULL);
                                                    

        if (IsA(pathnode, MergePath))
            ((MergePath*)pathnode)->outersortkeys = NIL;
    }

    return alternate;
}

#ifdef __TBASE__
/* count remotesubplans in path */
void
contains_remotesubplan(Path *path, int *number, bool *redistribute)
{// #lizard forgives
    if (!number)    
        return;

    if (!path)
        return;

    switch(path->pathtype)
    {
        case T_RemoteSubplan:
            {
                RemoteSubPath *pathnode = (RemoteSubPath *)path;
                
                Distribution   *subdistribution = path->distribution;
                
                if (!subdistribution || subdistribution->distributionType == LOCATOR_TYPE_NONE)
                {
                    (*number)++;
                }

                if (subdistribution && (subdistribution->distributionType == LOCATOR_TYPE_HASH ||
                    subdistribution->distributionType == LOCATOR_TYPE_SHARD))
                    *redistribute = true;

                contains_remotesubplan(pathnode->subpath, number, redistribute);
            }
            break;
        case T_Gather:
            {
                GatherPath *pathnode = (GatherPath *)path;

                contains_remotesubplan(pathnode->subpath, number, redistribute);
            }
            break;
		case T_GatherMerge:
			{
				GatherMergePath *pathnode = (GatherMergePath *)path;
				
				contains_remotesubplan(pathnode->subpath, number, redistribute);
			}
			break;
        case T_Agg:
            {
                if (IsA(path, AggPath))
                {
                    AggPath *pathnode = (AggPath *)path;

                    contains_remotesubplan(pathnode->subpath, number, redistribute);
                }
                else
                {
                    GroupingSetsPath *pathnode = (GroupingSetsPath *)path;

                    contains_remotesubplan(pathnode->subpath, number, redistribute);
                }
            }
            break;
        case T_HashJoin:
            {
                HashPath *pathnode = (HashPath *)path;

                contains_remotesubplan(pathnode->jpath.innerjoinpath, number, redistribute);

                contains_remotesubplan(pathnode->jpath.outerjoinpath, number, redistribute);
            }
            break;
        case T_MergeJoin:
            {
                MergePath *pathnode = (MergePath *)path;

                contains_remotesubplan(pathnode->jpath.innerjoinpath, number, redistribute);

                contains_remotesubplan(pathnode->jpath.outerjoinpath, number, redistribute);
            }
            break;
        case T_NestLoop:
            {
                NestPath *pathnode = (NestPath *)path;

                contains_remotesubplan(pathnode->innerjoinpath, number, redistribute);

                contains_remotesubplan(pathnode->outerjoinpath, number, redistribute);
            }
            break;
        case T_Material:
            {
                MaterialPath *pathnode = (MaterialPath *)path;

                contains_remotesubplan(pathnode->subpath, number, redistribute);
            }
            break;
        case T_Unique:
            {
                UniquePath *pathnode = (UniquePath *)path;

                contains_remotesubplan(pathnode->subpath, number, redistribute);
            }
            break;
        case T_SubqueryScan:
            {
                SubqueryScanPath *pathnode = (SubqueryScanPath *)path;

                contains_remotesubplan(pathnode->subpath, number, redistribute);
            }
            break;
        case T_Result:
            {
                if (IsA(path, ProjectionPath))
                {
                    ProjectionPath *pathnode = (ProjectionPath *)path;

                    contains_remotesubplan(pathnode->subpath, number, redistribute);
                }
            }
            break;
        case T_ProjectSet:
            {
                ProjectSetPath *pathnode = (ProjectSetPath *)path;

                contains_remotesubplan(pathnode->subpath, number, redistribute);
            }
            break;
        case T_Sort:
            {
                SortPath *pathnode = (SortPath *)path;

                contains_remotesubplan(pathnode->subpath, number, redistribute);
            }
            break;
        case T_Group:
            {
                GroupPath *pathnode = (GroupPath *)path;

                contains_remotesubplan(pathnode->subpath, number, redistribute);
            }
            break;
        case T_Append:
            {
                ListCell *cell;
                AppendPath *pathnode = (AppendPath *)path;

                foreach(cell, pathnode->subpaths)
                {
                    Path *p = (Path *)lfirst(cell);

                    contains_remotesubplan(p, number, redistribute);
                }
            }
            break;
        case T_MergeAppend:
            {
                ListCell *cell;
                MergeAppendPath *pathnode = (MergeAppendPath *)path;

                foreach(cell, pathnode->subpaths)
                {
                    Path *p = (Path *)lfirst(cell);

                    contains_remotesubplan(p, number, redistribute);
                }
            }
            break;
        default:
            break;
    }
}

static int
get_num_connections(int numnodes, int nRemotePlans)
{
    int i;
    int num_connections = 1;

    for (i = 0; i < nRemotePlans; i++)
    {
        num_connections = num_connections * numnodes;
    }

    return num_connections;
}

/*
 * redistribute local grouping results among datanodes for 
 * distinct aggs like count(distinct a) or avg(distinct a)...
 *
 * Tips: we do not check the agg column's type, directly use that
 * as hash column, but some data types are not supported as hash column now,
 * maybe some errors.
 */
Path *
create_redistribute_distinct_agg_path(PlannerInfo *root, Query *parse, Path *path, Aggref *agg)
{
	PathTarget *pathtarget = path->pathtarget;
	TargetEntry *te    = NULL;
	Bitmapset   *nodes = NULL;
	Oid group;
	int i;

	te = get_sortgroupclause_tle((SortGroupClause *)linitial(agg->aggdistinct),
								 agg->args);

	if(te == NULL)
	{
		elog(ERROR, "Distinct aggref not found in pathtarget.");
	}

	if (list_length(groupOids) > 1)
	{
		groupOids = NULL;
		elog(ERROR, "Tables from different groups should not be invloved in one Query.");
	}

	if (groupOids)
	{
		group = linitial_oid(groupOids);
	}
	else
	{
		group = InvalidOid;
	}

	if (group == InvalidOid)
	{
		for (i = 0; i < NumDataNodes; i++)
			nodes = bms_add_member(nodes, i);

		/*
		 * FIXING ME! check hash column's data type to satisfity hash locator func
		 */
		path = redistribute_path(root,
								 path,
								 NULL,
								 LOCATOR_TYPE_HASH,
								 (Node *)te->expr,
								 nodes,
								 NULL);
	}
	else
	{
		ListCell *cell;
		List *nodelist = GetGroupNodeList(group);

		foreach (cell, nodelist)
		{
			int nodeid = lfirst_int(cell);

			nodes = bms_add_member(nodes, nodeid);
		}
		/*
		 * FIXING ME! check hash column's data type to satisfity hash locator func
		 */
		path = redistribute_path(root,
								 path,
								 NULL,
								 LOCATOR_TYPE_SHARD,
								 (Node *)te->expr,
								 nodes,
								 NULL);
	}

	path->pathkeys = NULL;
	path->pathtarget = pathtarget;

	return path;
}

/*
  * redistribute local grouping results among datanodes, then
  * get the final grouping results. seems more efficient...
  *
  * Tips: we do not check the grouping column's type, directly use that
  * as hash column, but some data types are not supported as hash column now,
  * maybe some errors.
  */
Path *
create_redistribute_grouping_path(PlannerInfo *root, Query *parse, Path *path)
{// #lizard forgives
    if(parse->groupingSets)
    {
        /* not implement now */
        elog(ERROR, "grouping sets not supported now!");
    }

    if(parse->groupClause)
    {
        /*
          * current implemention is quite rough, need more work.
          */
        int i;
        Bitmapset *nodes = NULL;
        TargetEntry *te = NULL;
        Oid group = InvalidOid;
        double    rows = 0;
        double    num_groups = 0;
        int       colIdx = -1;
                        
        AttrNumber *groupColIdx = extract_grouping_cols(parse->groupClause,
                                                        parse->targetList);

        List *groupExprs = get_sortgrouplist_exprs(parse->groupClause,
                                                 parse->targetList);

        if (IsA(path, AggPath))
        {
            AggPath *agg = (AggPath *)path;

            rows = agg->subpath->rows;
        }
        else
        {
            rows = path->rows;
        }

        /* choose group key which get max group numbers as distributed key */
        for ( i = 0; i < list_length(parse->groupClause); i++)
        {
            List *expr = NULL;
            double dNumGroups;
            void *groupExpr = list_nth(groupExprs, i);

            expr = lappend(expr, groupExpr);
            
            dNumGroups = estimate_num_groups(root, expr, rows, NULL);

            if (dNumGroups > num_groups)
            {
                num_groups = dNumGroups;
                colIdx = i;
            }

            list_free(expr);
        }

        list_free(groupExprs);

        if (colIdx < 0)
        {
            elog(ERROR, "Could not get group-by redistributed column");
        }

        te = (TargetEntry *)list_nth(parse->targetList,
                                     groupColIdx[colIdx]-1);

        if (groupOids)
        {
            group = linitial_oid(groupOids);
        }
        else
        {
            group = InvalidOid;
        }

        if (group == InvalidOid)
        {
            for (i = 0; i < NumDataNodes; i++)
                nodes = bms_add_member(nodes, i);

            /*
              * FIXING ME! check hash column's data type to satisfity hash locator func
              */
            path = redistribute_path(root,
                                     path,
                                     NULL,
                                     LOCATOR_TYPE_HASH,
                                     (Node *)te->expr,
                                     nodes,
                                     NULL);
        }
        else
        {
            ListCell *cell;
            List *nodelist = GetGroupNodeList(group);

            foreach(cell, nodelist)
            {
                int nodeid = lfirst_int(cell);

                nodes = bms_add_member(nodes, nodeid);
            }
            /*
              * FIXING ME! check hash column's data type to satisfity hash locator func
              */
            path = redistribute_path(root,
                                     path,
                                     NULL,
                                     LOCATOR_TYPE_SHARD,
                                     (Node *)te->expr,
                                     nodes,
                                     NULL);
        }

        path->pathkeys = NULL;

        return path;
    }

    if(parse->hasAggs)
    {
        /*
          * FIXING ME! check hash column's data type to satisfity hash locator func
          */
        path = redistribute_path(root,
                                 path,
                                 NULL,
                                 LOCATOR_TYPE_NONE,
                                 NULL,
                                 NULL,
                                 NULL);

        return path;
    }

    return NULL; /* keep compiler quiet */
}

static List *
add_groups_to_list(bool has_baserestrictinfo, Oid          relid, RelationLocInfo *rel_loc_info, 
                          Node *dis_qual, List    *nodeList, Node *sec_quals)
{// #lizard forgives
    int32     nGroup;
    Oid     *groups;
    int      i;
    List    *newnodelist = NULL;

    Relation rel = relation_open(relid, NoLock);

    if (RELATION_IS_CHILD(rel))
    {
        relid = RELATION_GET_PARENT(rel);
    }

    relation_close(rel, NoLock);

    /* has base AND-quals with relations */
    if (has_baserestrictinfo)
    {
        /* meet distributed column quals */
        if (dis_qual)
        {
            int16         typlen;
            bool         typbyval;
            char         typalign;
            char         typdelim;
            Oid          typioparam;
            Oid          typiofunc;
            Oid          keyValueGroup;
            Oid          keyValueColdGroup;
            char        *value;
            Const *const_expr = (Const *)dis_qual;
            
            /* check whether the value is key value */
            get_type_io_data(const_expr->consttype, IOFunc_output,
                             &typlen, &typbyval,
                             &typalign, &typdelim,
                             &typioparam, &typiofunc);
            value = OidOutputFunctionCall(typiofunc, const_expr->constvalue);        
            
            keyValueGroup = GetKeyValuesGroup(MyDatabaseId, relid, value, &keyValueColdGroup);
            pfree(value);
            value = NULL;

            /* relation in key-value */
            if (OidIsValid(keyValueGroup))
            {
                groupOids = list_append_unique_oid(groupOids, keyValueGroup);
                groupOids = list_append_unique_oid(groupOids, keyValueColdGroup);
            }
            else
            {
                ListCell *cell;

                foreach(cell, nodeList)
                {
                    int nodeidx = lfirst_int(cell);

                    Oid nodeoid = PGXCNodeGetNodeOid(nodeidx, PGXC_NODE_DATANODE);

                    Oid groupoid = GetGroupOidByNode(nodeoid);

                    groupOids = list_append_unique_oid(groupOids, groupoid);
                }
            }
        }
        else
        {
            GetRelationSecondGroup(relid, &groups, &nGroup);

            /* relation in key-value */
            if (nGroup)
            {
                for (i = 0; i < nGroup; i++)
                {
                    groupOids = list_append_unique_oid(groupOids, groups[i]);
                }

                pfree(groups);

                if (OidIsValid(rel_loc_info->groupId))
                {
                    groupOids = list_append_unique_oid(groupOids, rel_loc_info->groupId);
                }

                if (OidIsValid(rel_loc_info->coldGroupId))
                {
                    groupOids = list_append_unique_oid(groupOids, rel_loc_info->coldGroupId);
                }
            }
            else
            {
                /* has second distributed column qual */
                if (sec_quals)
                {
                    ListCell *cell;
                    
                    List *oids = GetRelationGroupsByQuals(relid, rel_loc_info, sec_quals);

                    groupOids = list_union_oid(groupOids, oids);

                    foreach(cell, oids)
                    {
                        int j;
                        int32     dn_num;
                        int32  *datanodes;
                        Oid groupoid = lfirst_oid(cell);

                        GetShardNodes(groupoid, &datanodes, &dn_num, NULL);
                        for(j = 0; j < dn_num; j++)
                        {                            
                            newnodelist = list_append_unique_int(newnodelist, datanodes[j]);
                        }
                        pfree(datanodes);
                    }
                }
                else
                {
                    if (OidIsValid(rel_loc_info->groupId))
                    {
                        groupOids = list_append_unique_oid(groupOids, rel_loc_info->groupId);
                    }

                    if (OidIsValid(rel_loc_info->coldGroupId))
                    {
                        groupOids = list_append_unique_oid(groupOids, rel_loc_info->coldGroupId);
                    }
                }
            }
        }
    }
    else
    {
        if (OidIsValid(rel_loc_info->groupId))
        {
            groupOids = list_append_unique_oid(groupOids, rel_loc_info->groupId);
        }

        if (OidIsValid(rel_loc_info->coldGroupId))
        {
            groupOids = list_append_unique_oid(groupOids, rel_loc_info->coldGroupId);
        }

        /* secondary group */
        GetRelationSecondGroup(relid, &groups, &nGroup);
        for (i = 0; i < nGroup; i++)
        {
            groupOids = list_append_unique_oid(groupOids, groups[i]);
        }
        
        if (nGroup)
        {
            pfree(groups);
        }
    }

    return newnodelist;
}
#endif

#endif

#ifdef __COLD_HOT__
static void
adjust_distribution_nodes(PlannerInfo *root, RelOptInfo *rel, 
                                    RelationLocInfo *rel_loc_info, Distribution *distribution)
{// #lizard forgives
    if (rel_loc_info && AttributeNumberIsValid(rel_loc_info->secAttrNum))
    {
        int nodeidx;
        List *cold_hot_group = NULL;
        CmdType cmd = root->parse->commandType;
        Bitmapset *nodelist = distribution->restrictNodes ? distribution->restrictNodes : distribution->nodes;

        nodeidx = -1;
        while ((nodeidx = bms_next_member(nodelist, nodeidx)) >= 0)
        {
            Oid nodeoid = PGXCNodeGetNodeOid(nodeidx, PGXC_NODE_DATANODE);

            Oid groupoid = GetGroupOidByNode(nodeoid);

            groupOids = list_append_unique_oid(groupOids, groupoid);

            cold_hot_group = list_append_unique_oid(cold_hot_group, groupoid);
        }

        if (distribution->restrictNodes && list_length(cold_hot_group) == 1)
        {
            if (cmd == CMD_SELECT || 
                ((cmd == CMD_UPDATE || cmd == CMD_DELETE || cmd == CMD_INSERT) && root->parse->resultRelation != rel->relid))
            {
                int j;
                int32 dn_num;
                int32 *datanodes;
                Bitmapset *newnodelist = NULL;
                Oid groupoid = linitial_oid(cold_hot_group);

                GetShardNodes(groupoid, &datanodes, &dn_num, NULL);
                for(j = 0; j < dn_num; j++)
                {                            
                    newnodelist = bms_add_member(newnodelist, datanodes[j]);
                }

                if (newnodelist)
                {
                    distribution->nodes = newnodelist;
                }
            }
        }
    }
}

#endif

/*
 * create_seqscan_path
 *      Creates a path corresponding to a sequential scan, returning the
 *      pathnode.
 */
Path *
create_seqscan_path(PlannerInfo *root, RelOptInfo *rel,
                    Relids required_outer, int parallel_workers)
{// #lizard forgives
    Path       *pathnode = makeNode(Path);
#ifdef __COLD_HOT__
    RangeTblEntry    *rte = planner_rt_fetch(rel->relid, root);
    RelationLocInfo *rel_loc_info = GetRelationLocInfo(rte->relid);
#endif
    pathnode->pathtype = T_SeqScan;
    pathnode->parent = rel;
    pathnode->pathtarget = rel->reltarget;
    pathnode->param_info = get_baserel_parampathinfo(root, rel,
                                                     required_outer);
    pathnode->parallel_aware = parallel_workers > 0 ? true : false;
    pathnode->parallel_safe = rel->consider_parallel;
    pathnode->parallel_workers = parallel_workers;
    pathnode->pathkeys = NIL;    /* seqscan has unordered result */
    
#ifdef XCP
    set_scanpath_distribution(root, rel, pathnode);
    if (rel->baserestrictinfo)
    {
        ListCell *lc;
#ifdef __COLD_HOT__
        bool or_clause = false;
        List *quals = NULL;
#endif

        foreach (lc, rel->baserestrictinfo)
        {
            RestrictInfo *ri = (RestrictInfo *) lfirst(lc);
            restrict_distribution(root, ri, pathnode);

#ifdef __COLD_HOT__
            if (!ri->orclause)
            {
                quals = lappend(quals, ri->clause);
            }
            else
            {
                or_clause = true;
            }
#endif
        }

#ifdef __COLD_HOT__
        if (IS_PGXC_COORDINATOR && !or_clause)
        {
            int count = 0;
            Distribution   *distribution = pathnode->distribution;
            RelationAccessType rel_access = RELATION_ACCESS_READ;
            bool for_update = root->parse->rowMarks ? true : false;

            switch (root->parse->commandType)
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
                    elog(ERROR, "Unrecognised command type %d", root->parse->commandType);
                    break;
            }

            if (rel_loc_info && AttributeNumberIsValid(rel_loc_info->secAttrNum))
            {
                Node *dis_qual = NULL;
                Node *sec_quals = NULL;
                ExecNodes *nodes = NULL;

                if (root->parse->commandType == CMD_UPDATE || 
                    root->parse->commandType == CMD_DELETE ||
                    root->parse->commandType == CMD_INSERT)
                {
                    if (root->parse->resultRelation != rel->relid)
                    {
                        rel_access = RELATION_ACCESS_READ;
                    }
                }
                nodes = GetRelationNodesByQuals(rte->relid, rel_loc_info, rel->relid, 
                                                    (Node *)quals, rel_access, &dis_qual, &sec_quals);
                if (root->parse->commandType == CMD_SELECT)
                {
                    List *newnodelist = add_groups_to_list(true, rte->relid, rel_loc_info, 
                                                           dis_qual, nodes->nodeList, sec_quals);
                    if (newnodelist)
                    {
                        nodes->nodeList = newnodelist;
                    }
                }

                count = list_length(nodes->nodeList);

                if (count)
                {
                    ListCell *cell;
                    Bitmapset  *restrictinfo = NULL;
                    
                    foreach(cell, nodes->nodeList)
                    {
                        restrictinfo = bms_add_member(restrictinfo, lfirst_int(cell));
                    }

                    distribution->restrictNodes = restrictinfo;
                }
            }
        }
        else if (IS_PGXC_COORDINATOR && or_clause && root->parse->commandType == CMD_SELECT)
        {
            if (rel_loc_info && AttributeNumberIsValid(rel_loc_info->secAttrNum))
            {
                add_groups_to_list(false, rte->relid, rel_loc_info, NULL, NULL, NULL);
            }
        }
#endif
    }
    else
    {
        if (IS_PGXC_COORDINATOR && root->parse->commandType == CMD_SELECT)
        {
            if (rel_loc_info && AttributeNumberIsValid(rel_loc_info->secAttrNum))
            {
                add_groups_to_list(false, rte->relid, rel_loc_info, NULL, NULL, NULL);
            }
        }
    }
#ifdef __COLD_HOT__
    if (IS_PGXC_COORDINATOR)
    {
        Distribution   *distribution = ((Path *)pathnode)->distribution;

        adjust_distribution_nodes(root, rel, rel_loc_info, distribution);
    }
#endif
#endif

    cost_seqscan(pathnode, root, rel, pathnode->param_info);

    return pathnode;
}

/*
 * create_samplescan_path
 *      Creates a path node for a sampled table scan.
 */
Path *
create_samplescan_path(PlannerInfo *root, RelOptInfo *rel, Relids required_outer)
{// #lizard forgives
    Path       *pathnode = makeNode(Path);
#ifdef __COLD_HOT__
    RangeTblEntry    *rte = planner_rt_fetch(rel->relid, root);
    RelationLocInfo *rel_loc_info = GetRelationLocInfo(rte->relid);
#endif
    pathnode->pathtype = T_SampleScan;
    pathnode->parent = rel;
    pathnode->pathtarget = rel->reltarget;
    pathnode->param_info = get_baserel_parampathinfo(root, rel,
                                                     required_outer);
    pathnode->parallel_aware = false;
    pathnode->parallel_safe = rel->consider_parallel;
    pathnode->parallel_workers = 0;
    pathnode->pathkeys = NIL;    /* samplescan has unordered result */

#ifdef XCP
    set_scanpath_distribution(root, rel, pathnode);
    if (rel->baserestrictinfo)
    {
        ListCell *lc;
#ifdef __COLD_HOT__
        bool or_clause = false;
        List *quals = NULL;
#endif
        foreach (lc, rel->baserestrictinfo)
        {
            RestrictInfo *ri = (RestrictInfo *) lfirst(lc);
            restrict_distribution(root, ri, pathnode);
#ifdef __COLD_HOT__
            if (!ri->orclause)
            {
                quals = lappend(quals, ri->clause);
            }
            else
            {
                or_clause = true;
            }
#endif
        }
#ifdef __COLD_HOT__
        if (IS_PGXC_COORDINATOR && !or_clause)
        {
            int count = 0;
            Distribution   *distribution = pathnode->distribution;
            RelationAccessType rel_access = RELATION_ACCESS_READ;
            bool for_update = root->parse->rowMarks ? true : false;

            switch (root->parse->commandType)
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
                    elog(ERROR, "Unrecognised command type %d", root->parse->commandType);
                    break;
            }

            if (rel_loc_info && AttributeNumberIsValid(rel_loc_info->secAttrNum))
            {
                Node *dis_qual = NULL;
                Node *sec_quals = NULL;
                ExecNodes *nodes = NULL;
                
                if (root->parse->commandType == CMD_UPDATE || 
                    root->parse->commandType == CMD_DELETE ||
                    root->parse->commandType == CMD_INSERT)
                {
                    if (root->parse->resultRelation != rel->relid)
                    {
                        rel_access = RELATION_ACCESS_READ;
                    }
                }
                nodes = GetRelationNodesByQuals(rte->relid, rel_loc_info, rel->relid, 
                                                    (Node *)quals, rel_access, &dis_qual, &sec_quals);
                if (root->parse->commandType == CMD_SELECT)
                {
                    List *newnodelist = add_groups_to_list(true, rte->relid, rel_loc_info, 
                                                           dis_qual, nodes->nodeList, sec_quals);
                    if (newnodelist)
                    {
                        nodes->nodeList = newnodelist;
                    }
                }

                count = list_length(nodes->nodeList);

                if (count)
                {
                    ListCell *cell;
                    Bitmapset  *restrictinfo = NULL;
                    
                    foreach(cell, nodes->nodeList)
                    {
                        restrictinfo = bms_add_member(restrictinfo, lfirst_int(cell));
                    }

                    distribution->restrictNodes = restrictinfo;
                }
            }
        }
        else if (IS_PGXC_COORDINATOR && or_clause && root->parse->commandType == CMD_SELECT)
        {
            if (rel_loc_info && AttributeNumberIsValid(rel_loc_info->secAttrNum))
            {
                add_groups_to_list(false, rte->relid, rel_loc_info, NULL, NULL, NULL);
            }
        }
#endif
    }
    else
    {
        if (IS_PGXC_COORDINATOR && root->parse->commandType == CMD_SELECT)
        {
            if (rel_loc_info && AttributeNumberIsValid(rel_loc_info->secAttrNum))
            {
                add_groups_to_list(false, rte->relid, rel_loc_info, NULL, NULL, NULL);
            }
        }
    }
#ifdef __COLD_HOT__
    if (IS_PGXC_COORDINATOR)
    {
        Distribution   *distribution = ((Path *)pathnode)->distribution;

        adjust_distribution_nodes(root, rel, rel_loc_info, distribution);
    }
#endif
#endif

    cost_samplescan(pathnode, root, rel, pathnode->param_info);

    return pathnode;
}

/*
 * create_index_path
 *      Creates a path node for an index scan.
 *
 * 'index' is a usable index.
 * 'indexclauses' is a list of RestrictInfo nodes representing clauses
 *            to be used as index qual conditions in the scan.
 * 'indexclausecols' is an integer list of index column numbers (zero based)
 *            the indexclauses can be used with.
 * 'indexorderbys' is a list of bare expressions (no RestrictInfos)
 *            to be used as index ordering operators in the scan.
 * 'indexorderbycols' is an integer list of index column numbers (zero based)
 *            the ordering operators can be used with.
 * 'pathkeys' describes the ordering of the path.
 * 'indexscandir' is ForwardScanDirection or BackwardScanDirection
 *            for an ordered index, or NoMovementScanDirection for
 *            an unordered index.
 * 'indexonly' is true if an index-only scan is wanted.
 * 'required_outer' is the set of outer relids for a parameterized path.
 * 'loop_count' is the number of repetitions of the indexscan to factor into
 *        estimates of caching behavior.
 * 'partial_path' is true if constructing a parallel index scan path.
 *
 * Returns the new path node.
 */
IndexPath *
create_index_path(PlannerInfo *root,
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
                  bool partial_path)
{// #lizard forgives
    IndexPath  *pathnode = makeNode(IndexPath);
    RelOptInfo *rel = index->rel;
    List       *indexquals,
               *indexqualcols;
#ifdef __COLD_HOT__
    List *quals = NULL;
    RangeTblEntry    *rte = planner_rt_fetch(rel->relid, root);
    RelationLocInfo *rel_loc_info = GetRelationLocInfo(rte->relid);
#endif


    pathnode->path.pathtype = indexonly ? T_IndexOnlyScan : T_IndexScan;
    pathnode->path.parent = rel;
    pathnode->path.pathtarget = rel->reltarget;
    pathnode->path.param_info = get_baserel_parampathinfo(root, rel,
                                                          required_outer);
    pathnode->path.parallel_aware = false;
    pathnode->path.parallel_safe = rel->consider_parallel;
    pathnode->path.parallel_workers = 0;
    pathnode->path.pathkeys = pathkeys;

    /* Convert clauses to indexquals the executor can handle */
    expand_indexqual_conditions(index, indexclauses, indexclausecols,
                                &indexquals, &indexqualcols);

    /* Fill in the pathnode */
    pathnode->indexinfo = index;
    pathnode->indexclauses = indexclauses;
    pathnode->indexquals = indexquals;
    pathnode->indexqualcols = indexqualcols;
    pathnode->indexorderbys = indexorderbys;
    pathnode->indexorderbycols = indexorderbycols;
    pathnode->indexscandir = indexscandir;

#ifdef XCP
    set_scanpath_distribution(root, rel, (Path *) pathnode);
    if (indexclauses)
    {
        ListCell *lc;
        foreach (lc, indexclauses)
        {
            RestrictInfo *ri = (RestrictInfo *) lfirst(lc);
            restrict_distribution(root, ri, (Path *) pathnode);
        }
    }
    if (rel->baserestrictinfo && restrict_query)
    {
        ListCell *lc;
        foreach (lc, rel->baserestrictinfo)
        {
            RestrictInfo *ri = (RestrictInfo *) lfirst(lc);
            restrict_distribution(root, ri, (Path *) pathnode);
        }
    }
#endif
#ifdef __COLD_HOT__
    if (IS_PGXC_COORDINATOR)
    {
        ListCell *lc;
        foreach (lc, rel->baserestrictinfo)
        {
            RestrictInfo *ri = (RestrictInfo *) lfirst(lc);

            if (!ri->orclause)
            {
                quals = lappend(quals, ri->clause);
            }
            else
            {
                quals = NULL;
                break;
            }
        }
    }
    
	if (IS_PGXC_COORDINATOR)
    {
        int count = 0;
        Distribution   *distribution = ((Path *)pathnode)->distribution;
        RelationAccessType rel_access = RELATION_ACCESS_READ;
        bool for_update = root->parse->rowMarks ? true : false;

        switch (root->parse->commandType)
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
                elog(ERROR, "Unrecognised command type %d", root->parse->commandType);
                break;
        }

        if (rel_loc_info && AttributeNumberIsValid(rel_loc_info->secAttrNum))
        {
            Node *dis_qual = NULL;
            Node *sec_quals = NULL;
            ExecNodes *nodes = NULL;

            if (root->parse->commandType == CMD_UPDATE || 
                root->parse->commandType == CMD_DELETE ||
                root->parse->commandType == CMD_INSERT)
            {
                if (root->parse->resultRelation != rel->relid)
                {
                    rel_access = RELATION_ACCESS_READ;
                }
            }
            nodes = GetRelationNodesByQuals(rte->relid, rel_loc_info, rel->relid, 
                                                (Node *)quals, rel_access, &dis_qual, &sec_quals);
            if (root->parse->commandType == CMD_SELECT)
            {
                List *newnodelist = add_groups_to_list(true, rte->relid, rel_loc_info, 
                                                       dis_qual, nodes->nodeList, sec_quals);
                if (newnodelist)
                {
                    nodes->nodeList = newnodelist;
                }
            }

            count = list_length(nodes->nodeList);

            if (count)
            {
                ListCell *cell;
                Bitmapset  *restrictinfo = NULL;
                
                foreach(cell, nodes->nodeList)
                {
                    restrictinfo = bms_add_member(restrictinfo, lfirst_int(cell));
                }

                distribution->restrictNodes = restrictinfo;
            }
        }
    }
#ifdef __COLD_HOT__
    if (IS_PGXC_COORDINATOR)
    {
        Distribution   *distribution = ((Path *)pathnode)->distribution;

        adjust_distribution_nodes(root, rel, rel_loc_info, distribution);
    }
#endif

#endif

    cost_index(pathnode, root, loop_count, partial_path);

    return pathnode;
}

/*
 * create_bitmap_heap_path
 *      Creates a path node for a bitmap scan.
 *
 * 'bitmapqual' is a tree of IndexPath, BitmapAndPath, and BitmapOrPath nodes.
 * 'required_outer' is the set of outer relids for a parameterized path.
 * 'loop_count' is the number of repetitions of the indexscan to factor into
 *        estimates of caching behavior.
 *
 * loop_count should match the value used when creating the component
 * IndexPaths.
 */
BitmapHeapPath *
create_bitmap_heap_path(PlannerInfo *root,
                        RelOptInfo *rel,
                        Path *bitmapqual,
                        Relids required_outer,
                        double loop_count,
                        int parallel_degree)
{// #lizard forgives
    BitmapHeapPath *pathnode = makeNode(BitmapHeapPath);
#ifdef __COLD_HOT__
    RangeTblEntry    *rte = planner_rt_fetch(rel->relid, root);
    RelationLocInfo *rel_loc_info = GetRelationLocInfo(rte->relid);
#endif

    pathnode->path.pathtype = T_BitmapHeapScan;
    pathnode->path.parent = rel;
    pathnode->path.pathtarget = rel->reltarget;
    pathnode->path.param_info = get_baserel_parampathinfo(root, rel,
                                                          required_outer);
    pathnode->path.parallel_aware = parallel_degree > 0 ? true : false;
    pathnode->path.parallel_safe = rel->consider_parallel;
    pathnode->path.parallel_workers = parallel_degree;
    pathnode->path.pathkeys = NIL;    /* always unordered */

    pathnode->bitmapqual = bitmapqual;

#ifdef XCP
    set_scanpath_distribution(root, rel, (Path *) pathnode);
    if (rel->baserestrictinfo)
    {
        ListCell *lc;
#ifdef __COLD_HOT__
        bool or_clause = false;
        List *quals = NULL;
#endif
        foreach (lc, rel->baserestrictinfo)
        {
            RestrictInfo *ri = (RestrictInfo *) lfirst(lc);
            restrict_distribution(root, ri, (Path *) pathnode);
#ifdef __COLD_HOT__
            if (!ri->orclause)
            {
                quals = lappend(quals, ri->clause);
            }
            else
            {
                or_clause = true;
            }
#endif
        }
#ifdef __COLD_HOT__
        if (IS_PGXC_COORDINATOR && !or_clause)
        {
            int count = 0;
            Distribution   *distribution = ((Path *)pathnode)->distribution;
            RelationAccessType rel_access = RELATION_ACCESS_READ;
            bool for_update = root->parse->rowMarks ? true : false;

            switch (root->parse->commandType)
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
                    elog(ERROR, "Unrecognised command type %d", root->parse->commandType);
                    break;
            }

            if (rel_loc_info && AttributeNumberIsValid(rel_loc_info->secAttrNum))
            {
                Node *dis_qual = NULL;
                Node *sec_quals = NULL;
                ExecNodes *nodes = NULL;

                if (root->parse->commandType == CMD_UPDATE || 
                    root->parse->commandType == CMD_DELETE ||
                    root->parse->commandType == CMD_INSERT)
                {
                    if (root->parse->resultRelation != rel->relid)
                    {
                        rel_access = RELATION_ACCESS_READ;
                    }
                }
                nodes = GetRelationNodesByQuals(rte->relid, rel_loc_info, rel->relid, 
                                                    (Node *)quals, rel_access, &dis_qual, &sec_quals);
                if (root->parse->commandType == CMD_SELECT)
                {
                    List *newnodelist = add_groups_to_list(true, rte->relid, rel_loc_info, 
                                                           dis_qual, nodes->nodeList, sec_quals);
                    if (newnodelist)
                    {
                        nodes->nodeList = newnodelist;
                    }
                }

                count = list_length(nodes->nodeList);

                if (count)
                {
                    ListCell *cell;
                    Bitmapset  *restrictinfo = NULL;
                    
                    foreach(cell, nodes->nodeList)
                    {
                        restrictinfo = bms_add_member(restrictinfo, lfirst_int(cell));
                    }

                    distribution->restrictNodes = restrictinfo;
                }
            }
        }
        else if (IS_PGXC_COORDINATOR && or_clause && root->parse->commandType == CMD_SELECT)
        {
            if (rel_loc_info && AttributeNumberIsValid(rel_loc_info->secAttrNum))
            {
                add_groups_to_list(false, rte->relid, rel_loc_info, NULL, NULL, NULL);
            }
        }
#endif

    }
    else
    {
        if (IS_PGXC_COORDINATOR && root->parse->commandType == CMD_SELECT)
        {
            if (rel_loc_info && AttributeNumberIsValid(rel_loc_info->secAttrNum))
            {
                add_groups_to_list(false, rte->relid, rel_loc_info, NULL, NULL, NULL);
            }
        }
    }
#ifdef __COLD_HOT__
    if (IS_PGXC_COORDINATOR)
    {
        Distribution   *distribution = ((Path *)pathnode)->distribution;

        adjust_distribution_nodes(root, rel, rel_loc_info, distribution);
    }
#endif
#endif

    cost_bitmap_heap_scan(&pathnode->path, root, rel,
                          pathnode->path.param_info,
                          bitmapqual, loop_count);

    return pathnode;
}

/*
 * create_bitmap_and_path
 *      Creates a path node representing a BitmapAnd.
 */
BitmapAndPath *
create_bitmap_and_path(PlannerInfo *root,
                       RelOptInfo *rel,
                       List *bitmapquals)
{
    BitmapAndPath *pathnode = makeNode(BitmapAndPath);

    pathnode->path.pathtype = T_BitmapAnd;
    pathnode->path.parent = rel;
    pathnode->path.pathtarget = rel->reltarget;
    pathnode->path.param_info = NULL;    /* not used in bitmap trees */

    /*
     * Currently, a BitmapHeapPath, BitmapAndPath, or BitmapOrPath will be
     * parallel-safe if and only if rel->consider_parallel is set.  So, we can
     * set the flag for this path based only on the relation-level flag,
     * without actually iterating over the list of children.
     */
    pathnode->path.parallel_aware = false;
    pathnode->path.parallel_safe = rel->consider_parallel;
    pathnode->path.parallel_workers = 0;

    pathnode->path.pathkeys = NIL;    /* always unordered */

    pathnode->bitmapquals = bitmapquals;

#ifdef XCP
    set_scanpath_distribution(root, rel, (Path *) pathnode);
#endif

    /* this sets bitmapselectivity as well as the regular cost fields: */
    cost_bitmap_and_node(pathnode, root);

    return pathnode;
}

/*
 * create_bitmap_or_path
 *      Creates a path node representing a BitmapOr.
 */
BitmapOrPath *
create_bitmap_or_path(PlannerInfo *root,
                      RelOptInfo *rel,
                      List *bitmapquals)
{
    BitmapOrPath *pathnode = makeNode(BitmapOrPath);

    pathnode->path.pathtype = T_BitmapOr;
    pathnode->path.parent = rel;
    pathnode->path.pathtarget = rel->reltarget;
    pathnode->path.param_info = NULL;    /* not used in bitmap trees */

    /*
     * Currently, a BitmapHeapPath, BitmapAndPath, or BitmapOrPath will be
     * parallel-safe if and only if rel->consider_parallel is set.  So, we can
     * set the flag for this path based only on the relation-level flag,
     * without actually iterating over the list of children.
     */
    pathnode->path.parallel_aware = false;
    pathnode->path.parallel_safe = rel->consider_parallel;
    pathnode->path.parallel_workers = 0;

    pathnode->path.pathkeys = NIL;    /* always unordered */

    pathnode->bitmapquals = bitmapquals;

#ifdef XCP
    set_scanpath_distribution(root, rel, (Path *) pathnode);
#endif

    /* this sets bitmapselectivity as well as the regular cost fields: */
    cost_bitmap_or_node(pathnode, root);

    return pathnode;
}

/*
 * create_tidscan_path
 *      Creates a path corresponding to a scan by TID, returning the pathnode.
 */
TidPath *
create_tidscan_path(PlannerInfo *root, RelOptInfo *rel, List *tidquals,
                    Relids required_outer)
{
    TidPath    *pathnode = makeNode(TidPath);

    pathnode->path.pathtype = T_TidScan;
    pathnode->path.parent = rel;
    pathnode->path.pathtarget = rel->reltarget;
    pathnode->path.param_info = get_baserel_parampathinfo(root, rel,
                                                          required_outer);
    pathnode->path.parallel_aware = false;
    pathnode->path.parallel_safe = rel->consider_parallel;
    pathnode->path.parallel_workers = 0;
    pathnode->path.pathkeys = NIL;    /* always unordered */

    pathnode->tidquals = tidquals;

#ifdef XCP
    set_scanpath_distribution(root, rel, (Path *) pathnode);
    /* We may need to pass info about target node to support */
    if (pathnode->path.distribution)
        elog(ERROR, "could not perform TID scan on remote relation");
#endif

    cost_tidscan(&pathnode->path, root, rel, tidquals,
                 pathnode->path.param_info);

    return pathnode;
}

/*
 * create_append_path
 *      Creates a path corresponding to an Append plan, returning the
 *      pathnode.
 *
 * Note that we must handle subpaths = NIL, representing a dummy access path.
 */
AppendPath *
create_append_path(RelOptInfo *rel, List *subpaths, Relids required_outer,
                   int parallel_workers, List *partitioned_rels)
{// #lizard forgives
    AppendPath *pathnode = makeNode(AppendPath);
    ListCell   *l;
#ifdef XCP
    Distribution *distribution;
    Path       *subpath;
#endif

    pathnode->path.pathtype = T_Append;
    pathnode->path.parent = rel;
    pathnode->path.pathtarget = rel->reltarget;
    pathnode->path.param_info = get_appendrel_parampathinfo(rel,
                                                            required_outer);
    pathnode->path.parallel_aware = false;
    pathnode->path.parallel_safe = rel->consider_parallel;
    pathnode->path.parallel_workers = parallel_workers;
    pathnode->path.pathkeys = NIL;    /* result is always considered unsorted */
#ifdef XCP
    /*
     * Append path is used to implement scans of partitioned tables, inherited
     * tables and some "set" operations, like UNION ALL. While all partitioned
     * and inherited tables should have the same distribution, UNION'ed queries
     * may have different.  When paths being appended have the same
     * distribution it is OK to push Append down to the data nodes. If not,
     * perform "coordinator" Append.
     *
     * Since we ensure that all partitions of a partitioned table are always
     * distributed by the same strategy on the same set of nodes, we can push
     * down MergeAppend of partitions of the table.
     */
    if (partitioned_rels && subpaths)
    {
        /* Take distribution of the first node */
        l = list_head(subpaths);
        subpath = (Path *) lfirst(l);
        distribution = copyObject(subpath->distribution);
        pathnode->path.distribution = distribution;
    }
    /* Special case of the dummy relation, if the subpaths list is empty */
    else if (subpaths)
    {
        /* Take distribution of the first node */
        l = list_head(subpaths);
        subpath = (Path *) lfirst(l);
        distribution = copyObject(subpath->distribution);
        /*
         * Check remaining subpaths, if all distributions equal to the first set
         * it as a distribution of the Append path; otherwise make up coordinator
         * Append
         */
        while ((l = lnext(l)))
        {
            subpath = (Path *) lfirst(l);

            /*
             * For Append and MergeAppend paths, we are most often dealing with
             * different relations, appended together. So its very likely that
             * the distribution for each relation will have a different varno.
             * But we should be able to push down Append and MergeAppend as
             * long as rest of the distribution information matches.
             *
             * equalDistribution() compares everything except the varnos
             */
            if (equalDistribution(distribution, subpath->distribution))
            {
                /*
                 * Both distribution and subpath->distribution may be NULL at
                 * this point, or they both are not null.
                 */
#ifdef __TBASE__
				if (distribution)
				{
					if (distribution->restrictNodes && subpath->distribution->restrictNodes)
					{
						distribution->restrictNodes = bms_union(
							distribution->restrictNodes,
							subpath->distribution->restrictNodes);
					}
					else
					{
						distribution->restrictNodes = NULL;
					}
				}
#else
                if (distribution && subpath->distribution->restrictNodes)
                    distribution->restrictNodes = bms_union(
                            distribution->restrictNodes,
                            subpath->distribution->restrictNodes);
#endif
            }
            else
            {
                break;
            }
        }
        if (l)
        {
            List *newsubpaths = NIL;
            foreach(l, subpaths)
            {
                subpath = (Path *) lfirst(l);
                if (subpath->distribution)
                    subpath = redistribute_path(NULL, subpath, NIL,
                                                LOCATOR_TYPE_NONE, NULL,
                                                NULL, NULL);
                newsubpaths = lappend(newsubpaths, subpath);
            }
            subpaths = newsubpaths;
            pathnode->path.distribution = NULL;
        }
        else
            pathnode->path.distribution = distribution;
    }
#endif
    pathnode->partitioned_rels = list_copy(partitioned_rels);
    pathnode->subpaths = subpaths;

    /*
     * We don't bother with inventing a cost_append(), but just do it here.
     *
     * Compute rows and costs as sums of subplan rows and costs.  We charge
     * nothing extra for the Append itself, which perhaps is too optimistic,
     * but since it doesn't do any selection or projection, it is a pretty
     * cheap node.
     */
    pathnode->path.rows = 0;
    pathnode->path.startup_cost = 0;
    pathnode->path.total_cost = 0;
    foreach(l, subpaths)
    {
        Path       *subpath = (Path *) lfirst(l);

        pathnode->path.rows += subpath->rows;

        if (l == list_head(subpaths))    /* first node? */
            pathnode->path.startup_cost = subpath->startup_cost;
        pathnode->path.total_cost += subpath->total_cost;
        pathnode->path.parallel_safe = pathnode->path.parallel_safe &&
            subpath->parallel_safe;

        /* All child paths must have same parameterization */
        Assert(bms_equal(PATH_REQ_OUTER(subpath), required_outer));
    }

    return pathnode;
}

/*
 * create_merge_append_path
 *      Creates a path corresponding to a MergeAppend plan, returning the
 *      pathnode.
 */
MergeAppendPath *
create_merge_append_path(PlannerInfo *root,
                         RelOptInfo *rel,
                         List *subpaths,
                         List *pathkeys,
                         Relids required_outer,
                         List *partitioned_rels)
{// #lizard forgives
    MergeAppendPath *pathnode = makeNode(MergeAppendPath);
    Cost        input_startup_cost;
    Cost        input_total_cost;
    ListCell   *l;
#ifdef XCP
    Distribution *distribution = NULL;
    Path       *subpath;
#endif

    pathnode->path.pathtype = T_MergeAppend;
    pathnode->path.parent = rel;
#ifdef XCP
    /*
     * Since we ensure that all partitions of a partitioned table are always
     * distributed by the same strategy on the same set of nodes, we can push
     * down MergeAppend of partitions of the table.
     *
     * For MergeAppend of non-partitions, it is safe to push down MergeAppend
     * if all subpath distributions are the same and these distributions are
     * Replicated or distribution key is the expression of the first pathkey.
     */
    l = list_head(subpaths);
    subpath = (Path *) lfirst(l);
    distribution = copyObject(subpath->distribution);

    if (partitioned_rels)
    {
        pathnode->path.distribution = distribution;
    }
    else
    {
        /*
         * Verify if it is safe to push down MergeAppend with this distribution.
         * TODO implement check of the second condition (distribution key is the
         * first pathkey)
         */
        if (distribution == NULL || IsLocatorReplicated(distribution->distributionType))
        {
            /*
             * Check remaining subpaths, if all distributions equal to the first set
             * it as a distribution of the Append path; otherwise make up coordinator
             * Append
             */
            while ((l = lnext(l)))
            {
                subpath = (Path *) lfirst(l);

                /*
                 * See comments in Append path
                 */
                if (distribution && equalDistribution(distribution, subpath->distribution))
                {
                    if (subpath->distribution->restrictNodes)
                        distribution->restrictNodes = bms_union(
                                distribution->restrictNodes,
                                subpath->distribution->restrictNodes);
                }
                else
                {
                    break;
                }
            }
        }
        if (l)
        {
            List *newsubpaths = NIL;
            foreach(l, subpaths)
            {
                subpath = (Path *) lfirst(l);
                if (subpath->distribution)
                {
                    /*
                     * If an explicit sort is necessary, make sure it's pushed
                     * down to the remote node (i.e. add it before the remote
                     * subplan).
                     */
                    subpath = redistribute_path(root, subpath, pathkeys,
                                                LOCATOR_TYPE_NONE, NULL,
                                                NULL, NULL);
                }
                newsubpaths = lappend(newsubpaths, subpath);
            }
            subpaths = newsubpaths;
            pathnode->path.distribution = NULL;
        }
        else
            pathnode->path.distribution = distribution;
    }
#endif

    pathnode->path.pathtarget = rel->reltarget;
    pathnode->path.param_info = get_appendrel_parampathinfo(rel,
                                                            required_outer);
    pathnode->path.parallel_aware = false;
    pathnode->path.parallel_safe = rel->consider_parallel;
    pathnode->path.parallel_workers = 0;
    pathnode->path.pathkeys = pathkeys;
    pathnode->partitioned_rels = list_copy(partitioned_rels);
    pathnode->subpaths = subpaths;

    /*
     * Apply query-wide LIMIT if known and path is for sole base relation.
     * (Handling this at this low level is a bit klugy.)
     */
    if (bms_equal(rel->relids, root->all_baserels))
        pathnode->limit_tuples = root->limit_tuples;
    else
        pathnode->limit_tuples = -1.0;

    /*
     * Add up the sizes and costs of the input paths.
     */
    pathnode->path.rows = 0;
    input_startup_cost = 0;
    input_total_cost = 0;
    foreach(l, subpaths)
    {
        Path       *subpath = (Path *) lfirst(l);

        pathnode->path.rows += subpath->rows;
        pathnode->path.parallel_safe = pathnode->path.parallel_safe &&
            subpath->parallel_safe;

        if (pathkeys_contained_in(pathkeys, subpath->pathkeys))
        {
            /* Subpath is adequately ordered, we won't need to sort it */
            input_startup_cost += subpath->startup_cost;
            input_total_cost += subpath->total_cost;
        }
        else
        {
            /* We'll need to insert a Sort node, so include cost for that */
            Path        sort_path;    /* dummy for result of cost_sort */

            cost_sort(&sort_path,
                      root,
                      pathkeys,
                      subpath->total_cost,
                      subpath->parent->tuples,
                      subpath->pathtarget->width,
                      0.0,
                      work_mem,
                      pathnode->limit_tuples);
            input_startup_cost += sort_path.startup_cost;
            input_total_cost += sort_path.total_cost;
        }

        /* All child paths must have same parameterization */
        Assert(bms_equal(PATH_REQ_OUTER(subpath), required_outer));
    }

    /* Now we can compute total costs of the MergeAppend */
    cost_merge_append(&pathnode->path, root,
                      pathkeys, list_length(subpaths),
                      input_startup_cost, input_total_cost,
                      pathnode->path.rows);

    return pathnode;
}

QualPath *
create_qual_path(PlannerInfo *root, Path *subpath, List *quals)
{
	QualPath   *pathnode = makeNode(QualPath);
	RelOptInfo *rel = subpath->parent;
	QualCost    qual_cost;
	Cost        run_cost;
	
	cost_qual_eval(&qual_cost, quals, root);
	
	pathnode->path.pathtype = T_Result;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = subpath->pathtarget;
	pathnode->path.parallel_safe = rel->consider_parallel;
	
	pathnode->quals = quals;
	pathnode->subpath = subpath;
	
	pathnode->path.rows = subpath->rows;
	run_cost = subpath->total_cost - subpath->startup_cost;
	run_cost += (cpu_operator_cost + qual_cost.per_tuple) * pathnode->path.rows;
	
	pathnode->path.startup_cost = subpath->startup_cost + qual_cost.startup;
	pathnode->path.total_cost = subpath->total_cost + run_cost;
	
	return pathnode;
}

/*
 * create_result_path
 *      Creates a path representing a Result-and-nothing-else plan.
 *
 * This is only used for degenerate cases, such as a query with an empty
 * jointree.
 */
ResultPath *
create_result_path(PlannerInfo *root, RelOptInfo *rel,
                   PathTarget *target, List *resconstantqual)
{
    ResultPath *pathnode = makeNode(ResultPath);

    pathnode->path.pathtype = T_Result;
    pathnode->path.parent = rel;
    pathnode->path.pathtarget = target;
    pathnode->path.param_info = NULL;    /* there are no other rels... */
    pathnode->path.parallel_aware = false;
    pathnode->path.parallel_safe = rel->consider_parallel;
    pathnode->path.parallel_workers = 0;
    pathnode->path.pathkeys = NIL;
    pathnode->quals = resconstantqual;

    /* Hardly worth defining a cost_result() function ... just do it */
    pathnode->path.rows = 1;
    pathnode->path.startup_cost = target->cost.startup;
    pathnode->path.total_cost = target->cost.startup +
        cpu_tuple_cost + target->cost.per_tuple;
    if (resconstantqual)
    {
        QualCost    qual_cost;

        cost_qual_eval(&qual_cost, resconstantqual, root);
        /* resconstantqual is evaluated once at startup */
        pathnode->path.startup_cost += qual_cost.startup + qual_cost.per_tuple;
        pathnode->path.total_cost += qual_cost.startup + qual_cost.per_tuple;
    }

    return pathnode;
}

/*
 * create_material_path
 *      Creates a path corresponding to a Material plan, returning the
 *      pathnode.
 */
MaterialPath *
create_material_path(RelOptInfo *rel, Path *subpath)
{
    MaterialPath *pathnode = makeNode(MaterialPath);

    Assert(subpath->parent == rel);

    pathnode->path.pathtype = T_Material;
    pathnode->path.parent = rel;
    pathnode->path.pathtarget = rel->reltarget;
    pathnode->path.param_info = subpath->param_info;
    pathnode->path.parallel_aware = false;
    pathnode->path.parallel_safe = rel->consider_parallel &&
        subpath->parallel_safe;
    pathnode->path.parallel_workers = subpath->parallel_workers;
    pathnode->path.pathkeys = subpath->pathkeys;

    pathnode->subpath = subpath;

#ifdef XCP
    pathnode->path.distribution = (Distribution *) copyObject(subpath->distribution);
#endif

    cost_material(&pathnode->path,
                  subpath->startup_cost,
                  subpath->total_cost,
                  subpath->rows,
                  subpath->pathtarget->width);

    return pathnode;
}

/*
 * create_unique_path
 *      Creates a path representing elimination of distinct rows from the
 *      input data.  Distinct-ness is defined according to the needs of the
 *      semijoin represented by sjinfo.  If it is not possible to identify
 *      how to make the data unique, NULL is returned.
 *
 * If used at all, this is likely to be called repeatedly on the same rel;
 * and the input subpath should always be the same (the cheapest_total path
 * for the rel).  So we cache the result.
 */
UniquePath *
create_unique_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath,
                   SpecialJoinInfo *sjinfo)
{// #lizard forgives
    UniquePath *pathnode;
    Path        sort_path;        /* dummy for result of cost_sort */
    Path        agg_path;        /* dummy for result of cost_agg */
    MemoryContext oldcontext;
    int            numCols;

    /* Caller made a mistake if subpath isn't cheapest_total ... */
    Assert(subpath == rel->cheapest_total_path);
    Assert(subpath->parent == rel);
    /* ... or if SpecialJoinInfo is the wrong one */
    Assert(sjinfo->jointype == JOIN_SEMI);
    Assert(bms_equal(rel->relids, sjinfo->syn_righthand));

    /* If result already cached, return it */
    if (rel->cheapest_unique_path)
        return (UniquePath *) rel->cheapest_unique_path;

    /* If it's not possible to unique-ify, return NULL */
    if (!(sjinfo->semi_can_btree || sjinfo->semi_can_hash))
        return NULL;

    /*
     * We must ensure path struct and subsidiary data are allocated in main
     * planning context; otherwise GEQO memory management causes trouble.
     */
    oldcontext = MemoryContextSwitchTo(root->planner_cxt);

    pathnode = makeNode(UniquePath);

    pathnode->path.pathtype = T_Unique;
    pathnode->path.parent = rel;
    pathnode->path.pathtarget = rel->reltarget;
    pathnode->path.param_info = subpath->param_info;
    pathnode->path.parallel_aware = false;
    pathnode->path.parallel_safe = rel->consider_parallel &&
        subpath->parallel_safe;
    pathnode->path.parallel_workers = subpath->parallel_workers;

    /*
     * Assume the output is unsorted, since we don't necessarily have pathkeys
     * to represent it.  (This might get overridden below.)
     */
    pathnode->path.pathkeys = NIL;

    pathnode->subpath = subpath;
    pathnode->in_operators = sjinfo->semi_operators;
    pathnode->uniq_exprs = sjinfo->semi_rhs_exprs;

#ifdef XCP
    /* distribution is the same as in the subpath */
    pathnode->path.distribution = (Distribution *) copyObject(subpath->distribution);
#endif

    /*
     * If the input is a relation and it has a unique index that proves the
     * semi_rhs_exprs are unique, then we don't need to do anything.  Note
     * that relation_has_unique_index_for automatically considers restriction
     * clauses for the rel, as well.
     */
    if (rel->rtekind == RTE_RELATION && sjinfo->semi_can_btree &&
        relation_has_unique_index_for(root, rel, NIL,
                                      sjinfo->semi_rhs_exprs,
                                      sjinfo->semi_operators))
    {
        pathnode->umethod = UNIQUE_PATH_NOOP;
        pathnode->path.rows = rel->rows;
        pathnode->path.startup_cost = subpath->startup_cost;
        pathnode->path.total_cost = subpath->total_cost;
        pathnode->path.pathkeys = subpath->pathkeys;

        rel->cheapest_unique_path = (Path *) pathnode;

        MemoryContextSwitchTo(oldcontext);

        return pathnode;
    }

    /*
     * If the input is a subquery whose output must be unique already, then we
     * don't need to do anything.  The test for uniqueness has to consider
     * exactly which columns we are extracting; for example "SELECT DISTINCT
     * x,y" doesn't guarantee that x alone is distinct. So we cannot check for
     * this optimization unless semi_rhs_exprs consists only of simple Vars
     * referencing subquery outputs.  (Possibly we could do something with
     * expressions in the subquery outputs, too, but for now keep it simple.)
     */
    if (rel->rtekind == RTE_SUBQUERY)
    {
        RangeTblEntry *rte = planner_rt_fetch(rel->relid, root);

        if (query_supports_distinctness(rte->subquery))
        {
            List       *sub_tlist_colnos;

            sub_tlist_colnos = translate_sub_tlist(sjinfo->semi_rhs_exprs,
                                                   rel->relid);

            if (sub_tlist_colnos &&
                query_is_distinct_for(rte->subquery,
                                      sub_tlist_colnos,
                                      sjinfo->semi_operators))
            {
                pathnode->umethod = UNIQUE_PATH_NOOP;
                pathnode->path.rows = rel->rows;
                pathnode->path.startup_cost = subpath->startup_cost;
                pathnode->path.total_cost = subpath->total_cost;
                pathnode->path.pathkeys = subpath->pathkeys;

                rel->cheapest_unique_path = (Path *) pathnode;

                MemoryContextSwitchTo(oldcontext);

                return pathnode;
            }
        }
    }

    /* Estimate number of output rows */
    pathnode->path.rows = estimate_num_groups(root,
                                              sjinfo->semi_rhs_exprs,
                                              rel->rows,
                                              NULL);
    numCols = list_length(sjinfo->semi_rhs_exprs);

    if (sjinfo->semi_can_btree)
    {
        /*
         * Estimate cost for sort+unique implementation
         */
        cost_sort(&sort_path, root, NIL,
                  subpath->total_cost,
                  rel->rows,
                  subpath->pathtarget->width,
                  0.0,
                  work_mem,
                  -1.0);

        /*
         * Charge one cpu_operator_cost per comparison per input tuple. We
         * assume all columns get compared at most of the tuples. (XXX
         * probably this is an overestimate.)  This should agree with
         * create_upper_unique_path.
         */
        sort_path.total_cost += cpu_operator_cost * rel->rows * numCols;
    }

    if (sjinfo->semi_can_hash)
    {
        /*
         * Estimate the overhead per hashtable entry at 64 bytes (same as in
         * planner.c).
         */
        int            hashentrysize = subpath->pathtarget->width + 64;

        if (hashentrysize * pathnode->path.rows > work_mem * 1024L)
        {
            /*
             * We should not try to hash.  Hack the SpecialJoinInfo to
             * remember this, in case we come through here again.
             */
            sjinfo->semi_can_hash = false;
        }
        else
            cost_agg(&agg_path, root,
                     AGG_HASHED, NULL,
                     numCols, pathnode->path.rows,
                     subpath->startup_cost,
                     subpath->total_cost,
                     rel->rows);
    }

    if (sjinfo->semi_can_btree && sjinfo->semi_can_hash)
    {
        if (agg_path.total_cost < sort_path.total_cost)
            pathnode->umethod = UNIQUE_PATH_HASH;
        else
            pathnode->umethod = UNIQUE_PATH_SORT;
    }
    else if (sjinfo->semi_can_btree)
        pathnode->umethod = UNIQUE_PATH_SORT;
    else if (sjinfo->semi_can_hash)
        pathnode->umethod = UNIQUE_PATH_HASH;
    else
    {
        /* we can get here only if we abandoned hashing above */
        MemoryContextSwitchTo(oldcontext);
        return NULL;
    }

    if (pathnode->umethod == UNIQUE_PATH_HASH)
    {
        pathnode->path.startup_cost = agg_path.startup_cost;
        pathnode->path.total_cost = agg_path.total_cost;
    }
    else
    {
        pathnode->path.startup_cost = sort_path.startup_cost;
        pathnode->path.total_cost = sort_path.total_cost;
    }

    rel->cheapest_unique_path = (Path *) pathnode;

    MemoryContextSwitchTo(oldcontext);

    return pathnode;
}

/*
 * create_gather_merge_path
 *
 *      Creates a path corresponding to a gather merge scan, returning
 *      the pathnode.
 */
GatherMergePath *
create_gather_merge_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath,
                         PathTarget *target, List *pathkeys,
                         Relids required_outer, double *rows)
{
    GatherMergePath *pathnode = makeNode(GatherMergePath);
    Cost        input_startup_cost = 0;
    Cost        input_total_cost = 0;

    Assert(subpath->parallel_safe);
    Assert(pathkeys);

    pathnode->path.pathtype = T_GatherMerge;
    pathnode->path.parent = rel;
    pathnode->path.param_info = get_baserel_parampathinfo(root, rel,
                                                          required_outer);
    pathnode->path.parallel_aware = false;

    /* distribution is the same as in the subpath */
    pathnode->path.distribution = (Distribution *) copyObject(subpath->distribution);

    pathnode->subpath = subpath;
    pathnode->num_workers = subpath->parallel_workers;
    pathnode->path.pathkeys = pathkeys;
    pathnode->path.pathtarget = target ? target : rel->reltarget;
    pathnode->path.rows += subpath->rows;

    if (pathkeys_contained_in(pathkeys, subpath->pathkeys))
    {
        /* Subpath is adequately ordered, we won't need to sort it */
        input_startup_cost += subpath->startup_cost;
        input_total_cost += subpath->total_cost;
    }
    else
    {
        /* We'll need to insert a Sort node, so include cost for that */
        Path        sort_path;    /* dummy for result of cost_sort */

        cost_sort(&sort_path,
                  root,
                  pathkeys,
                  subpath->total_cost,
                  subpath->rows,
                  subpath->pathtarget->width,
                  0.0,
                  work_mem,
                  -1);
        input_startup_cost += sort_path.startup_cost;
        input_total_cost += sort_path.total_cost;
    }

    cost_gather_merge(pathnode, root, rel, pathnode->path.param_info,
                      input_startup_cost, input_total_cost, rows);

    return pathnode;
}

/*
 * translate_sub_tlist - get subquery column numbers represented by tlist
 *
 * The given targetlist usually contains only Vars referencing the given relid.
 * Extract their varattnos (ie, the column numbers of the subquery) and return
 * as an integer List.
 *
 * If any of the tlist items is not a simple Var, we cannot determine whether
 * the subquery's uniqueness condition (if any) matches ours, so punt and
 * return NIL.
 */
static List *
translate_sub_tlist(List *tlist, int relid)
{
    List       *result = NIL;
    ListCell   *l;

    foreach(l, tlist)
    {
        Var           *var = (Var *) lfirst(l);

        if (!var || !IsA(var, Var) ||
            var->varno != relid)
            return NIL;            /* punt */

        result = lappend_int(result, var->varattno);
    }
    return result;
}

/*
 * create_gather_path
 *      Creates a path corresponding to a gather scan, returning the
 *      pathnode.
 *
 * 'rows' may optionally be set to override row estimates from other sources.
 */
GatherPath *
create_gather_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath,
                   PathTarget *target, Relids required_outer, double *rows)
{
    GatherPath *pathnode = makeNode(GatherPath);

    Assert(subpath->parallel_safe);

    pathnode->path.pathtype = T_Gather;
    pathnode->path.parent = rel;
    pathnode->path.pathtarget = target;
    pathnode->path.param_info = get_baserel_parampathinfo(root, rel,
                                                          required_outer);
    pathnode->path.parallel_aware = false;
    pathnode->path.parallel_safe = false;
    pathnode->path.parallel_workers = 0;
    pathnode->path.pathkeys = NIL;    /* Gather has unordered result */

    /* distribution is the same as in the subpath */
    pathnode->path.distribution = (Distribution *) copyObject(subpath->distribution);

    pathnode->subpath = subpath;
    pathnode->num_workers = subpath->parallel_workers;
    pathnode->single_copy = false;

    if (pathnode->num_workers == 0)
    {
        pathnode->path.pathkeys = subpath->pathkeys;
        pathnode->num_workers = 1;
        pathnode->single_copy = true;
    }

    cost_gather(pathnode, root, rel, pathnode->path.param_info, rows);

    return pathnode;
}

/*
 * create_subqueryscan_path
 *      Creates a path corresponding to a scan of a subquery,
 *      returning the pathnode.
 */
SubqueryScanPath *
#ifdef XCP
create_subqueryscan_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath,
                         List *pathkeys, Relids required_outer,
                         Distribution *distribution)
#else
create_subqueryscan_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath,
                         List *pathkeys, Relids required_outer)
#endif
{
    SubqueryScanPath *pathnode = makeNode(SubqueryScanPath);

#ifdef XCP
    pathnode->path.distribution = distribution;
#endif
    pathnode->path.pathtype = T_SubqueryScan;
    pathnode->path.parent = rel;
    pathnode->path.pathtarget = rel->reltarget;
    pathnode->path.param_info = get_baserel_parampathinfo(root, rel,
                                                          required_outer);
    pathnode->path.parallel_aware = false;
    pathnode->path.parallel_safe = rel->consider_parallel &&
        subpath->parallel_safe;
    pathnode->path.parallel_workers = subpath->parallel_workers;
    pathnode->path.pathkeys = pathkeys;
    pathnode->subpath = subpath;

    cost_subqueryscan(pathnode, root, rel, pathnode->path.param_info);

    return pathnode;
}

/*
 * create_functionscan_path
 *      Creates a path corresponding to a sequential scan of a function,
 *      returning the pathnode.
 */
Path *
create_functionscan_path(PlannerInfo *root, RelOptInfo *rel,
                         List *pathkeys, Relids required_outer)
{
    Path       *pathnode = makeNode(Path);

    pathnode->pathtype = T_FunctionScan;
    pathnode->parent = rel;
    pathnode->pathtarget = rel->reltarget;
    pathnode->param_info = get_baserel_parampathinfo(root, rel,
                                                     required_outer);
    pathnode->parallel_aware = false;
    pathnode->parallel_safe = rel->consider_parallel;
    pathnode->parallel_workers = 0;
    pathnode->pathkeys = pathkeys;

    cost_functionscan(pathnode, root, rel, pathnode->param_info);

    return pathnode;
}

/*
 * create_tablefuncscan_path
 *      Creates a path corresponding to a sequential scan of a table function,
 *      returning the pathnode.
 */
Path *
create_tablefuncscan_path(PlannerInfo *root, RelOptInfo *rel,
                          Relids required_outer)
{
    Path       *pathnode = makeNode(Path);

    pathnode->pathtype = T_TableFuncScan;
    pathnode->parent = rel;
    pathnode->pathtarget = rel->reltarget;
    pathnode->param_info = get_baserel_parampathinfo(root, rel,
                                                     required_outer);
    pathnode->parallel_aware = false;
    pathnode->parallel_safe = rel->consider_parallel;
    pathnode->parallel_workers = 0;
    pathnode->pathkeys = NIL;    /* result is always unordered */

    cost_tablefuncscan(pathnode, root, rel, pathnode->param_info);

    return pathnode;
}

/*
 * create_valuesscan_path
 *      Creates a path corresponding to a scan of a VALUES list,
 *      returning the pathnode.
 */
Path *
create_valuesscan_path(PlannerInfo *root, RelOptInfo *rel,
                       Relids required_outer)
{
    Path       *pathnode = makeNode(Path);

    pathnode->pathtype = T_ValuesScan;
    pathnode->parent = rel;
    pathnode->pathtarget = rel->reltarget;
    pathnode->param_info = get_baserel_parampathinfo(root, rel,
                                                     required_outer);
    pathnode->parallel_aware = false;
    pathnode->parallel_safe = rel->consider_parallel;
    pathnode->parallel_workers = 0;
    pathnode->pathkeys = NIL;    /* result is always unordered */

    cost_valuesscan(pathnode, root, rel, pathnode->param_info);

    return pathnode;
}

/*
 * create_ctescan_path
 *      Creates a path corresponding to a scan of a non-self-reference CTE,
 *      returning the pathnode.
 */
Path *
create_ctescan_path(PlannerInfo *root, RelOptInfo *rel, Relids required_outer)
{
    Path       *pathnode = makeNode(Path);

    pathnode->pathtype = T_CteScan;
    pathnode->parent = rel;
    pathnode->pathtarget = rel->reltarget;
    pathnode->param_info = get_baserel_parampathinfo(root, rel,
                                                     required_outer);
    pathnode->parallel_aware = false;
    pathnode->parallel_safe = rel->consider_parallel;
    pathnode->parallel_workers = 0;
    pathnode->pathkeys = NIL;    /* XXX for now, result is always unordered */

    cost_ctescan(pathnode, root, rel, pathnode->param_info);

    return pathnode;
}

/*
 * create_namedtuplestorescan_path
 *      Creates a path corresponding to a scan of a named tuplestore, returning
 *      the pathnode.
 */
Path *
create_namedtuplestorescan_path(PlannerInfo *root, RelOptInfo *rel,
                                Relids required_outer)
{
    Path       *pathnode = makeNode(Path);

    pathnode->pathtype = T_NamedTuplestoreScan;
    pathnode->parent = rel;
    pathnode->pathtarget = rel->reltarget;
    pathnode->param_info = get_baserel_parampathinfo(root, rel,
                                                     required_outer);
    pathnode->parallel_aware = false;
    pathnode->parallel_safe = rel->consider_parallel;
    pathnode->parallel_workers = 0;
    pathnode->pathkeys = NIL;    /* result is always unordered */

    cost_namedtuplestorescan(pathnode, root, rel, pathnode->param_info);

    return pathnode;
}

/*
 * create_worktablescan_path
 *      Creates a path corresponding to a scan of a self-reference CTE,
 *      returning the pathnode.
 */
Path *
create_worktablescan_path(PlannerInfo *root, RelOptInfo *rel,
                          Relids required_outer)
{
    Path       *pathnode = makeNode(Path);

    pathnode->pathtype = T_WorkTableScan;
    pathnode->parent = rel;
    pathnode->pathtarget = rel->reltarget;
    pathnode->param_info = get_baserel_parampathinfo(root, rel,
                                                     required_outer);
    pathnode->parallel_aware = false;
    pathnode->parallel_safe = rel->consider_parallel;
    pathnode->parallel_workers = 0;
    pathnode->pathkeys = NIL;    /* result is always unordered */

    /* Cost is the same as for a regular CTE scan */
    cost_ctescan(pathnode, root, rel, pathnode->param_info);

    return pathnode;
}

/*
 * create_foreignscan_path
 *      Creates a path corresponding to a scan of a foreign table, foreign join,
 *      or foreign upper-relation processing, returning the pathnode.
 *
 * This function is never called from core Postgres; rather, it's expected
 * to be called by the GetForeignPaths, GetForeignJoinPaths, or
 * GetForeignUpperPaths function of a foreign data wrapper.  We make the FDW
 * supply all fields of the path, since we do not have any way to calculate
 * them in core.  However, there is a usually-sane default for the pathtarget
 * (rel->reltarget), so we let a NULL for "target" select that.
 */
ForeignPath *
create_foreignscan_path(PlannerInfo *root, RelOptInfo *rel,
                        PathTarget *target,
                        double rows, Cost startup_cost, Cost total_cost,
                        List *pathkeys,
                        Relids required_outer,
                        Path *fdw_outerpath,
                        List *fdw_private)
{
    ForeignPath *pathnode = makeNode(ForeignPath);

    pathnode->path.pathtype = T_ForeignScan;
    pathnode->path.parent = rel;
    pathnode->path.pathtarget = target ? target : rel->reltarget;
    pathnode->path.param_info = get_baserel_parampathinfo(root, rel,
                                                          required_outer);
    pathnode->path.parallel_aware = false;
    pathnode->path.parallel_safe = rel->consider_parallel;
    pathnode->path.parallel_workers = 0;
    pathnode->path.rows = rows;
    pathnode->path.startup_cost = startup_cost;
    pathnode->path.total_cost = total_cost;
    pathnode->path.pathkeys = pathkeys;

    pathnode->fdw_outerpath = fdw_outerpath;
    pathnode->fdw_private = fdw_private;

    return pathnode;
}

/*
 * calc_nestloop_required_outer
 *      Compute the required_outer set for a nestloop join path
 *
 * Note: result must not share storage with either input
 */
Relids
calc_nestloop_required_outer(Relids outerrelids,
							 Relids outer_paramrels,
							 Relids innerrelids,
							 Relids inner_paramrels)
{
    Relids        required_outer;

    /* inner_path can require rels from outer path, but not vice versa */
	Assert(!bms_overlap(outer_paramrels, innerrelids));
    /* easy case if inner path is not parameterized */
    if (!inner_paramrels)
        return bms_copy(outer_paramrels);
    /* else, form the union ... */
    required_outer = bms_union(outer_paramrels, inner_paramrels);
    /* ... and remove any mention of now-satisfied outer rels */
    required_outer = bms_del_members(required_outer,
									 outerrelids);
    /* maintain invariant that required_outer is exactly NULL if empty */
    if (bms_is_empty(required_outer))
    {
        bms_free(required_outer);
        required_outer = NULL;
    }
    return required_outer;
}

/*
 * calc_non_nestloop_required_outer
 *      Compute the required_outer set for a merge or hash join path
 *
 * Note: result must not share storage with either input
 */
Relids
calc_non_nestloop_required_outer(Path *outer_path, Path *inner_path)
{
    Relids        outer_paramrels = PATH_REQ_OUTER(outer_path);
    Relids        inner_paramrels = PATH_REQ_OUTER(inner_path);
    Relids        required_outer;

    /* neither path can require rels from the other */
    Assert(!bms_overlap(outer_paramrels, inner_path->parent->relids));
    Assert(!bms_overlap(inner_paramrels, outer_path->parent->relids));
    /* form the union ... */
    required_outer = bms_union(outer_paramrels, inner_paramrels);
    /* we do not need an explicit test for empty; bms_union gets it right */
    return required_outer;
}

/*
 * create_nestloop_path
 *      Creates a pathnode corresponding to a nestloop join between two
 *      relations.
 *
 * 'joinrel' is the join relation.
 * 'jointype' is the type of join required
 * 'workspace' is the result from initial_cost_nestloop
 * 'extra' contains various information about the join
 * 'outer_path' is the outer path
 * 'inner_path' is the inner path
 * 'restrict_clauses' are the RestrictInfo nodes to apply at the join
 * 'pathkeys' are the path keys of the new join path
 * 'required_outer' is the set of required outer rels
 *
 * Returns the resulting path node.
 */
NestPath *
create_nestloop_path(PlannerInfo *root,
                     RelOptInfo *joinrel,
                     JoinType jointype,
                     JoinCostWorkspace *workspace,
                     JoinPathExtraData *extra,
                     Path *outer_path,
                     Path *inner_path,
                     List *restrict_clauses,
                     List *pathkeys,
                     Relids required_outer)
{// #lizard forgives
    NestPath   *pathnode = makeNode(NestPath);
#ifdef XCP
    List       *alternate;
    ListCell   *lc;
    List       *mclauses = NIL;
#endif
    Relids        inner_req_outer = PATH_REQ_OUTER(inner_path);

    /*
     * If the inner path is parameterized by the outer, we must drop any
     * restrict_clauses that are due to be moved into the inner path.  We have
     * to do this now, rather than postpone the work till createplan time,
     * because the restrict_clauses list can affect the size and cost
     * estimates for this path.
     */
    if (bms_overlap(inner_req_outer, outer_path->parent->relids))
    {
        Relids        inner_and_outer = bms_union(inner_path->parent->relids,
                                                inner_req_outer);
        List       *jclauses = NIL;
        ListCell   *lc;

        foreach(lc, restrict_clauses)
        {
            RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

            if (!join_clause_is_movable_into(rinfo,
                                             inner_path->parent->relids,
                                             inner_and_outer))
                jclauses = lappend(jclauses, rinfo);
#ifdef XCP
            else
                mclauses = lappend(mclauses, rinfo);
#endif
        }
        restrict_clauses = jclauses;
    }

    pathnode->path.pathtype = T_NestLoop;
    pathnode->path.parent = joinrel;
    pathnode->path.pathtarget = joinrel->reltarget;
    pathnode->path.param_info =
        get_joinrel_parampathinfo(root,
                                  joinrel,
                                  outer_path,
                                  inner_path,
                                  extra->sjinfo,
                                  required_outer,
                                  &restrict_clauses);
    pathnode->path.parallel_aware = false;
    pathnode->path.parallel_safe = joinrel->consider_parallel &&
        outer_path->parallel_safe && inner_path->parallel_safe;
    /* This is a foolish way to estimate parallel_workers, but for now... */
    pathnode->path.parallel_workers = outer_path->parallel_workers;
    pathnode->path.pathkeys = pathkeys;
    pathnode->jointype = jointype;
    pathnode->inner_unique = extra->inner_unique;
    pathnode->outerjoinpath = outer_path;
    pathnode->innerjoinpath = inner_path;
    pathnode->joinrestrictinfo = restrict_clauses;
#ifdef __TBASE__
    pathnode->path.parallel_aware = outer_path->parallel_workers > 0 ? true : false;
#endif
#ifdef XCP
    pathnode->movedrestrictinfo = mclauses;

    alternate = set_joinpath_distribution(root, pathnode);
#endif

#ifdef __TBASE__
	/*
	 * Since set_joinpath_distribution() could add additional pathnode such as
	 * RemoteSubplan, the result of initial_cost_nestloop() needs to be
	 * recalculated.
	 */
	initial_cost_nestloop(root, workspace, jointype,
						  pathnode->outerjoinpath,
						  pathnode->innerjoinpath,
						  extra);
#endif

    final_cost_nestloop(root, pathnode, workspace, extra);

#ifdef XCP
    /*
     * Also calculate costs of all alternates and return cheapest path
     */
    foreach(lc, alternate)
    {
        NestPath *altpath = (NestPath *) lfirst(lc);

#ifdef __TBASE__
		/*
		 * Recalculate the initial cost of alternate path
		 */
		initial_cost_nestloop(root, workspace, jointype,
							  altpath->outerjoinpath,
							  altpath->innerjoinpath,
							  extra);
#endif

        final_cost_nestloop(root, altpath, workspace, extra);
        if (altpath->path.total_cost < pathnode->path.total_cost)
            pathnode = altpath;
    }
#endif

	/* For DELETE, check if the path distribution satisfy resultRel distribution */
	if (!SatisfyResultRelDist(root, &pathnode->path))
		return NULL;

	return pathnode;
}

/*
 * create_mergejoin_path
 *      Creates a pathnode corresponding to a mergejoin join between
 *      two relations
 *
 * 'joinrel' is the join relation
 * 'jointype' is the type of join required
 * 'workspace' is the result from initial_cost_mergejoin
 * 'extra' contains various information about the join
 * 'outer_path' is the outer path
 * 'inner_path' is the inner path
 * 'restrict_clauses' are the RestrictInfo nodes to apply at the join
 * 'pathkeys' are the path keys of the new join path
 * 'required_outer' is the set of required outer rels
 * 'mergeclauses' are the RestrictInfo nodes to use as merge clauses
 *        (this should be a subset of the restrict_clauses list)
 * 'outersortkeys' are the sort varkeys for the outer relation
 * 'innersortkeys' are the sort varkeys for the inner relation
 */
MergePath *
create_mergejoin_path(PlannerInfo *root,
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
                      List *innersortkeys)
{
    MergePath  *pathnode = makeNode(MergePath);
#ifdef XCP
    List       *alternate;
    ListCell   *lc;
#endif

    pathnode->jpath.path.pathtype = T_MergeJoin;
    pathnode->jpath.path.parent = joinrel;
    pathnode->jpath.path.pathtarget = joinrel->reltarget;
    pathnode->jpath.path.param_info =
        get_joinrel_parampathinfo(root,
                                  joinrel,
                                  outer_path,
                                  inner_path,
                                  extra->sjinfo,
                                  required_outer,
                                  &restrict_clauses);
    pathnode->jpath.path.parallel_aware = false;
    pathnode->jpath.path.parallel_safe = joinrel->consider_parallel &&
        outer_path->parallel_safe && inner_path->parallel_safe;
    /* This is a foolish way to estimate parallel_workers, but for now... */
    pathnode->jpath.path.parallel_workers = outer_path->parallel_workers;
    pathnode->jpath.path.pathkeys = pathkeys;
    pathnode->jpath.jointype = jointype;
    pathnode->jpath.inner_unique = extra->inner_unique;
    pathnode->jpath.outerjoinpath = outer_path;
    pathnode->jpath.innerjoinpath = inner_path;
    pathnode->jpath.joinrestrictinfo = restrict_clauses;
    pathnode->path_mergeclauses = mergeclauses;
    pathnode->outersortkeys = outersortkeys;
    pathnode->innersortkeys = innersortkeys;
#ifdef __TBASE__
    pathnode->jpath.path.parallel_aware = outer_path->parallel_workers > 0 ? true : false;
#endif
#ifdef XCP
    alternate = set_joinpath_distribution(root, (JoinPath *) pathnode);
#endif
    /* pathnode->skip_mark_restore will be set by final_cost_mergejoin */
    /* pathnode->materialize_inner will be set by final_cost_mergejoin */

#ifdef __TBASE__
	/*
	 * Since set_joinpath_distribution() could add additional pathnode such as
	 * RemoteSubplan, the result of initial_cost_mergejoin() needs to be
	 * recalculated.
	 */
	initial_cost_mergejoin(root, workspace, jointype, mergeclauses,
						   pathnode->jpath.outerjoinpath,
						   pathnode->jpath.innerjoinpath,
						   outersortkeys, innersortkeys,
						   extra);
#endif

    final_cost_mergejoin(root, pathnode, workspace, extra);

#ifdef XCP
    /*
     * Also calculate costs of all alternates and return cheapest path
     */
    foreach(lc, alternate)
    {
        MergePath *altpath = (MergePath *) lfirst(lc);

#ifdef __TBASE__
		/*
		 * Recalculate the initial cost of alternate path
		 */
		initial_cost_mergejoin(root, workspace, jointype, mergeclauses,
							   altpath->jpath.outerjoinpath,
							   altpath->jpath.innerjoinpath,
							   outersortkeys, innersortkeys,
							   extra);
#endif

        final_cost_mergejoin(root, altpath, workspace, extra);
        if (altpath->jpath.path.total_cost < pathnode->jpath.path.total_cost)
            pathnode = altpath;
    }
#endif

	/* For DELETE, check if the path distribution satisfy resultRel distribution */
	if (!SatisfyResultRelDist(root, &pathnode->jpath.path))
		return NULL;

	return pathnode;
}

/*
 * create_hashjoin_path
 *      Creates a pathnode corresponding to a hash join between two relations.
 *
 * 'joinrel' is the join relation
 * 'jointype' is the type of join required
 * 'workspace' is the result from initial_cost_hashjoin
 * 'extra' contains various information about the join
 * 'outer_path' is the cheapest outer path
 * 'inner_path' is the cheapest inner path
 * 'restrict_clauses' are the RestrictInfo nodes to apply at the join
 * 'required_outer' is the set of required outer rels
 * 'hashclauses' are the RestrictInfo nodes to use as hash clauses
 *        (this should be a subset of the restrict_clauses list)
 */
HashPath *
create_hashjoin_path(PlannerInfo *root,
                     RelOptInfo *joinrel,
                     JoinType jointype,
                     JoinCostWorkspace *workspace,
                     JoinPathExtraData *extra,
                     Path *outer_path,
                     Path *inner_path,
                     List *restrict_clauses,
                     Relids required_outer,
                     List *hashclauses)
{// #lizard forgives
    HashPath   *pathnode = makeNode(HashPath);
#ifdef XCP
    List       *alternate;
    ListCell   *lc;
#endif

    pathnode->jpath.path.pathtype = T_HashJoin;
    pathnode->jpath.path.parent = joinrel;
    pathnode->jpath.path.pathtarget = joinrel->reltarget;
    pathnode->jpath.path.param_info =
        get_joinrel_parampathinfo(root,
                                  joinrel,
                                  outer_path,
                                  inner_path,
                                  extra->sjinfo,
                                  required_outer,
                                  &restrict_clauses);
#ifdef __TBASE__
    if (olap_optimizer)
    {
        pathnode->jpath.path.parallel_aware = outer_path->parallel_aware;
    }
    else
#endif
    pathnode->jpath.path.parallel_aware = false;

    pathnode->jpath.path.parallel_safe = joinrel->consider_parallel &&
        outer_path->parallel_safe && inner_path->parallel_safe;
    /* This is a foolish way to estimate parallel_workers, but for now... */
    pathnode->jpath.path.parallel_workers = outer_path->parallel_workers;

    /*
     * A hashjoin never has pathkeys, since its output ordering is
     * unpredictable due to possible batching.  XXX If the inner relation is
     * small enough, we could instruct the executor that it must not batch,
     * and then we could assume that the output inherits the outer relation's
     * ordering, which might save a sort step.  However there is considerable
     * downside if our estimate of the inner relation size is badly off. For
     * the moment we don't risk it.  (Note also that if we wanted to take this
     * seriously, joinpath.c would have to consider many more paths for the
     * outer rel than it does now.)
     */
    pathnode->jpath.path.pathkeys = NIL;
    pathnode->jpath.jointype = jointype;
    pathnode->jpath.inner_unique = extra->inner_unique;
    pathnode->jpath.outerjoinpath = outer_path;
    pathnode->jpath.innerjoinpath = inner_path;
    pathnode->jpath.joinrestrictinfo = restrict_clauses;
    pathnode->path_hashclauses = hashclauses;
#ifdef XCP
    alternate = set_joinpath_distribution(root, (JoinPath *) pathnode);
#endif

#ifdef __TBASE__
	/*
	 * Since set_joinpath_distribution() could add additional pathnode such as
	 * RemoteSubplan, the result of initial_cost_hashjoin() needs to be
	 * recalculated.
	 */
	initial_cost_hashjoin(root,
						  workspace,
						  jointype,
						  hashclauses,
						  pathnode->jpath.outerjoinpath,
						  pathnode->jpath.innerjoinpath,
						  extra);
#endif

	/* final_cost_hashjoin will fill in pathnode->num_batches */
    final_cost_hashjoin(root, pathnode, workspace, extra);

#ifdef XCP
    /*
     * Calculate costs of all alternates and return cheapest path
     */
    foreach(lc, alternate)
    {
        HashPath *altpath = (HashPath *) lfirst(lc);

#ifdef __TBASE__
		/*
		 * Recalculate the initial cost of alternate path
		 */
		initial_cost_hashjoin(root,
							  workspace,
							  jointype,
							  hashclauses,
							  altpath->jpath.outerjoinpath,
							  altpath->jpath.innerjoinpath,
							  extra);
#endif

        final_cost_hashjoin(root, altpath, workspace, extra);
        if (altpath->jpath.path.total_cost < pathnode->jpath.path.total_cost)
            pathnode = altpath;
    }
#endif

	/* For DELETE, check if the path distribution satisfy resultRel distribution */
	if (!SatisfyResultRelDist(root, &pathnode->jpath.path))
		return NULL;

	return pathnode;
}

/*
 * create_projection_path
 *      Creates a pathnode that represents performing a projection.
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'target' is the PathTarget to be computed
 */
ProjectionPath *
create_projection_path(PlannerInfo *root,
                       RelOptInfo *rel,
                       Path *subpath,
                       PathTarget *target)
{
    ProjectionPath *pathnode = makeNode(ProjectionPath);
    PathTarget *oldtarget = subpath->pathtarget;

    pathnode->path.pathtype = T_Result;
    pathnode->path.parent = rel;
    pathnode->path.pathtarget = target;
    /* For now, assume we are above any joins, so no parameterization */
    pathnode->path.param_info = NULL;
    pathnode->path.parallel_aware = false;
    pathnode->path.parallel_safe = rel->consider_parallel &&
        subpath->parallel_safe &&
        is_parallel_safe(root, (Node *) target->exprs);
    pathnode->path.parallel_workers = subpath->parallel_workers;
    /* Projection does not change the sort order */
    pathnode->path.pathkeys = subpath->pathkeys;

    pathnode->path.distribution = (Distribution *) copyObject(subpath->distribution);

    pathnode->subpath = subpath;

    /*
     * We might not need a separate Result node.  If the input plan node type
     * can project, we can just tell it to project something else.  Or, if it
     * can't project but the desired target has the same expression list as
     * what the input will produce anyway, we can still give it the desired
     * tlist (possibly changing its ressortgroupref labels, but nothing else).
     * Note: in the latter case, create_projection_plan has to recheck our
     * conclusion; see comments therein.
     */
    if (is_projection_capable_path(subpath) ||
        equal(oldtarget->exprs, target->exprs))
    {
        /* No separate Result node needed */
        pathnode->dummypp = true;

        /*
         * Set cost of plan as subpath's cost, adjusted for tlist replacement.
         */
        pathnode->path.rows = subpath->rows;
        pathnode->path.startup_cost = subpath->startup_cost +
            (target->cost.startup - oldtarget->cost.startup);
        pathnode->path.total_cost = subpath->total_cost +
            (target->cost.startup - oldtarget->cost.startup) +
            (target->cost.per_tuple - oldtarget->cost.per_tuple) * subpath->rows;
    }
    else
    {
        /* We really do need the Result node */
        pathnode->dummypp = false;

        /*
         * The Result node's cost is cpu_tuple_cost per row, plus the cost of
         * evaluating the tlist.  There is no qual to worry about.
         */
        pathnode->path.rows = subpath->rows;
        pathnode->path.startup_cost = subpath->startup_cost +
            target->cost.startup;
        pathnode->path.total_cost = subpath->total_cost +
            target->cost.startup +
            (cpu_tuple_cost + target->cost.per_tuple) * subpath->rows;
    }

    return pathnode;
}

/*
 * apply_projection_to_path
 *      Add a projection step, or just apply the target directly to given path.
 *
 * This has the same net effect as create_projection_path(), except that if
 * a separate Result plan node isn't needed, we just replace the given path's
 * pathtarget with the desired one.  This must be used only when the caller
 * knows that the given path isn't referenced elsewhere and so can be modified
 * in-place.
 *
 * If the input path is a GatherPath, we try to push the new target down to
 * its input as well; this is a yet more invasive modification of the input
 * path, which create_projection_path() can't do.
 *
 * Note that we mustn't change the source path's parent link; so when it is
 * add_path'd to "rel" things will be a bit inconsistent.  So far that has
 * not caused any trouble.
 *
 * 'rel' is the parent relation associated with the result
 * 'path' is the path representing the source of data
 * 'target' is the PathTarget to be computed
 */
Path *
apply_projection_to_path(PlannerInfo *root,
                         RelOptInfo *rel,
                         Path *path,
                         PathTarget *target)
{
    QualCost    oldcost;

    /*
     * If given path can't project, we might need a Result node, so make a
     * separate ProjectionPath.
     */
    if (!is_projection_capable_path(path))
        return (Path *) create_projection_path(root, rel, path, target);

    /*
     * We can just jam the desired tlist into the existing path, being sure to
     * update its cost estimates appropriately.
     */
    oldcost = path->pathtarget->cost;
    path->pathtarget = target;

    path->startup_cost += target->cost.startup - oldcost.startup;
    path->total_cost += target->cost.startup - oldcost.startup +
        (target->cost.per_tuple - oldcost.per_tuple) * path->rows;

    /*
     * If the path happens to be a Gather path, we'd like to arrange for the
     * subpath to return the required target list so that workers can help
     * project.  But if there is something that is not parallel-safe in the
     * target expressions, then we can't.
     */
    if (IsA(path, GatherPath) &&
        is_parallel_safe(root, (Node *) target->exprs))
    {
        GatherPath *gpath = (GatherPath *) path;

        /*
         * We always use create_projection_path here, even if the subpath is
         * projection-capable, so as to avoid modifying the subpath in place.
         * It seems unlikely at present that there could be any other
         * references to the subpath, but better safe than sorry.
         *
         * Note that we don't change the GatherPath's cost estimates; it might
         * be appropriate to do so, to reflect the fact that the bulk of the
         * target evaluation will happen in workers.
         */
        gpath->subpath = (Path *)
            create_projection_path(root,
                                   gpath->subpath->parent,
                                   gpath->subpath,
                                   target);
    }
    else if (path->parallel_safe &&
             !is_parallel_safe(root, (Node *) target->exprs))
    {
        /*
         * We're inserting a parallel-restricted target list into a path
         * currently marked parallel-safe, so we have to mark it as no longer
         * safe.
         */
        path->parallel_safe = false;
    }

    return path;
}

/*
 * create_set_projection_path
 *      Creates a pathnode that represents performing a projection that
 *      includes set-returning functions.
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'target' is the PathTarget to be computed
 */
ProjectSetPath *
create_set_projection_path(PlannerInfo *root,
                           RelOptInfo *rel,
                           Path *subpath,
                           PathTarget *target)
{
    ProjectSetPath *pathnode = makeNode(ProjectSetPath);
    double        tlist_rows;
    ListCell   *lc;

    pathnode->path.pathtype = T_ProjectSet;
    pathnode->path.parent = rel;
    pathnode->path.pathtarget = target;
    /* For now, assume we are above any joins, so no parameterization */
    pathnode->path.param_info = NULL;
    pathnode->path.parallel_aware = false;
    pathnode->path.parallel_safe = rel->consider_parallel &&
        subpath->parallel_safe &&
        is_parallel_safe(root, (Node *) target->exprs);
    pathnode->path.parallel_workers = subpath->parallel_workers;
    /* Projection does not change the sort order XXX? */
    pathnode->path.pathkeys = subpath->pathkeys;

    /* distribution is the same as in the subpath */
    pathnode->path.distribution = copyObject(subpath->distribution);

    pathnode->subpath = subpath;

    /*
     * Estimate number of rows produced by SRFs for each row of input; if
     * there's more than one in this node, use the maximum.
     */
    tlist_rows = 1;
    foreach(lc, target->exprs)
    {
        Node       *node = (Node *) lfirst(lc);
        double        itemrows;

        itemrows = expression_returns_set_rows(node);
        if (tlist_rows < itemrows)
            tlist_rows = itemrows;
    }

    /*
     * In addition to the cost of evaluating the tlist, charge cpu_tuple_cost
     * per input row, and half of cpu_tuple_cost for each added output row.
     * This is slightly bizarre maybe, but it's what 9.6 did; we may revisit
     * this estimate later.
     */
    pathnode->path.rows = subpath->rows * tlist_rows;
    pathnode->path.startup_cost = subpath->startup_cost +
        target->cost.startup;
    pathnode->path.total_cost = subpath->total_cost +
        target->cost.startup +
        (cpu_tuple_cost + target->cost.per_tuple) * subpath->rows +
        (pathnode->path.rows - subpath->rows) * cpu_tuple_cost / 2;

    return pathnode;
}

/*
 * create_sort_path
 *      Creates a pathnode that represents performing an explicit sort.
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'pathkeys' represents the desired sort order
 * 'limit_tuples' is the estimated bound on the number of output tuples,
 *        or -1 if no LIMIT or couldn't estimate
 */
SortPath *
create_sort_path(PlannerInfo *root,
                 RelOptInfo *rel,
                 Path *subpath,
                 List *pathkeys,
                 double limit_tuples)
{
    SortPath   *pathnode = makeNode(SortPath);

    pathnode->path.pathtype = T_Sort;
    pathnode->path.parent = rel;
    /* Sort doesn't project, so use source path's pathtarget */
    pathnode->path.pathtarget = subpath->pathtarget;
    /* For now, assume we are above any joins, so no parameterization */
    pathnode->path.param_info = NULL;
    pathnode->path.parallel_aware = false;
    pathnode->path.parallel_safe = rel->consider_parallel &&
        subpath->parallel_safe;
    pathnode->path.parallel_workers = subpath->parallel_workers;
    pathnode->path.pathkeys = pathkeys;

    /* distribution is the same as in the subpath */
    pathnode->path.distribution = copyObject(subpath->distribution);

    pathnode->subpath = subpath;

    cost_sort(&pathnode->path, root, pathkeys,
              subpath->total_cost,
              subpath->rows,
              subpath->pathtarget->width,
              0.0,                /* XXX comparison_cost shouldn't be 0? */
              work_mem, limit_tuples);

    return pathnode;
}

/*
 * create_group_path
 *      Creates a pathnode that represents performing grouping of presorted input
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'target' is the PathTarget to be computed
 * 'groupClause' is a list of SortGroupClause's representing the grouping
 * 'qual' is the HAVING quals if any
 * 'numGroups' is the estimated number of groups
 */
GroupPath *
create_group_path(PlannerInfo *root,
                  RelOptInfo *rel,
                  Path *subpath,
                  PathTarget *target,
                  List *groupClause,
                  List *qual,
                  double numGroups)
{
    GroupPath  *pathnode = makeNode(GroupPath);

    pathnode->path.pathtype = T_Group;
    pathnode->path.parent = rel;
    pathnode->path.pathtarget = target;
    /* For now, assume we are above any joins, so no parameterization */
    pathnode->path.param_info = NULL;
    pathnode->path.parallel_aware = false;
    pathnode->path.parallel_safe = rel->consider_parallel &&
        subpath->parallel_safe;
    pathnode->path.parallel_workers = subpath->parallel_workers;
    /* Group doesn't change sort ordering */
    pathnode->path.pathkeys = subpath->pathkeys;

    /* distribution is the same as in the subpath */
    pathnode->path.distribution = (Distribution *) copyObject(subpath->distribution);

    pathnode->subpath = subpath;

    pathnode->groupClause = groupClause;
    pathnode->qual = qual;

    cost_group(&pathnode->path, root,
               list_length(groupClause),
               numGroups,
               subpath->startup_cost, subpath->total_cost,
               subpath->rows);

    /* add tlist eval cost for each output row */
    pathnode->path.startup_cost += target->cost.startup;
    pathnode->path.total_cost += target->cost.startup +
        target->cost.per_tuple * pathnode->path.rows;

    return pathnode;
}

/*
 * create_upper_unique_path
 *      Creates a pathnode that represents performing an explicit Unique step
 *      on presorted input.
 *
 * This produces a Unique plan node, but the use-case is so different from
 * create_unique_path that it doesn't seem worth trying to merge the two.
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'numCols' is the number of grouping columns
 * 'numGroups' is the estimated number of groups
 *
 * The input path must be sorted on the grouping columns, plus possibly
 * additional columns; so the first numCols pathkeys are the grouping columns
 */
UpperUniquePath *
create_upper_unique_path(PlannerInfo *root,
                         RelOptInfo *rel,
                         Path *subpath,
                         int numCols,
                         double numGroups)
{
    UpperUniquePath *pathnode = makeNode(UpperUniquePath);

    pathnode->path.pathtype = T_Unique;
    pathnode->path.parent = rel;
    /* Unique doesn't project, so use source path's pathtarget */
    pathnode->path.pathtarget = subpath->pathtarget;
    /* For now, assume we are above any joins, so no parameterization */
    pathnode->path.param_info = NULL;
    pathnode->path.parallel_aware = false;
    pathnode->path.parallel_safe = rel->consider_parallel &&
        subpath->parallel_safe;
    pathnode->path.parallel_workers = subpath->parallel_workers;
    /* Unique doesn't change the input ordering */
    pathnode->path.pathkeys = subpath->pathkeys;

    pathnode->path.distribution = (Distribution *) copyObject(subpath->distribution);

    pathnode->subpath = subpath;
    pathnode->numkeys = numCols;

    /*
     * Charge one cpu_operator_cost per comparison per input tuple. We assume
     * all columns get compared at most of the tuples.  (XXX probably this is
     * an overestimate.)
     */
    pathnode->path.startup_cost = subpath->startup_cost;
    pathnode->path.total_cost = subpath->total_cost +
        cpu_operator_cost * subpath->rows * numCols;
    pathnode->path.rows = numGroups;

    return pathnode;
}

/*
 * create_agg_path
 *      Creates a pathnode that represents performing aggregation/grouping
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'target' is the PathTarget to be computed
 * 'aggstrategy' is the Agg node's basic implementation strategy
 * 'aggsplit' is the Agg node's aggregate-splitting mode
 * 'groupClause' is a list of SortGroupClause's representing the grouping
 * 'qual' is the HAVING quals if any
 * 'aggcosts' contains cost info about the aggregate functions to be computed
 * 'numGroups' is the estimated number of groups (1 if not grouping)
 */
AggPath *
create_agg_path(PlannerInfo *root,
                RelOptInfo *rel,
                Path *subpath,
                PathTarget *target,
                AggStrategy aggstrategy,
                AggSplit aggsplit,
                List *groupClause,
                List *qual,
                const AggClauseCosts *aggcosts,
                double numGroups)
{
    AggPath    *pathnode = makeNode(AggPath);

    pathnode->path.pathtype = T_Agg;
    pathnode->path.parent = rel;
    pathnode->path.pathtarget = target;
    /* For now, assume we are above any joins, so no parameterization */
    pathnode->path.param_info = NULL;
    pathnode->path.parallel_aware = false;
    pathnode->path.parallel_safe = rel->consider_parallel &&
        subpath->parallel_safe;
    pathnode->path.parallel_workers = subpath->parallel_workers;
    if (aggstrategy == AGG_SORTED)
        pathnode->path.pathkeys = subpath->pathkeys;    /* preserves order */
    else
        pathnode->path.pathkeys = NIL;    /* output is unordered */
    pathnode->subpath = subpath;

    /* distribution is the same as in the subpath */
    pathnode->path.distribution = (Distribution *) copyObject(subpath->distribution);

    pathnode->aggstrategy = aggstrategy;
    pathnode->aggsplit = aggsplit;
    pathnode->numGroups = numGroups;
    pathnode->groupClause = groupClause;
    pathnode->qual = qual;
#ifdef __TBASE__
	pathnode->hybrid = false;
	pathnode->entrySize = 0;
#endif

    cost_agg(&pathnode->path, root,
             aggstrategy, aggcosts,
             list_length(groupClause), numGroups,
             subpath->startup_cost, subpath->total_cost,
             subpath->rows);

    /* add tlist eval cost for each output row */
    pathnode->path.startup_cost += target->cost.startup;
    pathnode->path.total_cost += target->cost.startup +
        target->cost.per_tuple * pathnode->path.rows;

#ifdef __TBASE__
	/* estimate entry size for hashtable used by hashagg */
	if (g_hybrid_hash_agg)
	{
		if (aggstrategy == AGG_HASHED)
		{
			pathnode->entrySize = estimate_hashagg_entrysize(subpath, aggcosts, numGroups);
		}
	}
#endif

    return pathnode;
}

/*
 * create_groupingsets_path
 *      Creates a pathnode that represents performing GROUPING SETS aggregation
 *
 * GroupingSetsPath represents sorted grouping with one or more grouping sets.
 * The input path's result must be sorted to match the last entry in
 * rollup_groupclauses.
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'target' is the PathTarget to be computed
 * 'having_qual' is the HAVING quals if any
 * 'rollups' is a list of RollupData nodes
 * 'agg_costs' contains cost info about the aggregate functions to be computed
 * 'numGroups' is the estimated total number of groups
 */
GroupingSetsPath *
create_groupingsets_path(PlannerInfo *root,
                         RelOptInfo *rel,
                         Path *subpath,
                         PathTarget *target,
                         List *having_qual,
                         AggStrategy aggstrategy,
                         List *rollups,
                         const AggClauseCosts *agg_costs,
                         double numGroups)
{// #lizard forgives
    GroupingSetsPath *pathnode = makeNode(GroupingSetsPath);
    ListCell   *lc;
    bool        is_first = true;
    bool        is_first_sort = true;

    /* The topmost generated Plan node will be an Agg */
    pathnode->path.pathtype = T_Agg;
    pathnode->path.parent = rel;
    pathnode->path.pathtarget = target;
    pathnode->path.param_info = subpath->param_info;
    pathnode->path.parallel_aware = false;
    pathnode->path.parallel_safe = rel->consider_parallel &&
        subpath->parallel_safe;
    pathnode->path.parallel_workers = subpath->parallel_workers;
    pathnode->subpath = subpath;

    /* distribution is the same as in the subpath */
    pathnode->path.distribution = (Distribution *) copyObject(subpath->distribution);

    /*
     * Simplify callers by downgrading AGG_SORTED to AGG_PLAIN, and AGG_MIXED
     * to AGG_HASHED, here if possible.
     */
    if (aggstrategy == AGG_SORTED &&
        list_length(rollups) == 1 &&
        ((RollupData *) linitial(rollups))->groupClause == NIL)
        aggstrategy = AGG_PLAIN;

    if (aggstrategy == AGG_MIXED &&
        list_length(rollups) == 1)
        aggstrategy = AGG_HASHED;

    /*
     * Output will be in sorted order by group_pathkeys if, and only if, there
     * is a single rollup operation on a non-empty list of grouping
     * expressions.
     */
    if (aggstrategy == AGG_SORTED && list_length(rollups) == 1)
        pathnode->path.pathkeys = root->group_pathkeys;
    else
        pathnode->path.pathkeys = NIL;

    pathnode->aggstrategy = aggstrategy;
    pathnode->rollups = rollups;
    pathnode->qual = having_qual;

    Assert(rollups != NIL);
    Assert(aggstrategy != AGG_PLAIN || list_length(rollups) == 1);
    Assert(aggstrategy != AGG_MIXED || list_length(rollups) > 1);

    foreach(lc, rollups)
    {
        RollupData *rollup = lfirst(lc);
        List       *gsets = rollup->gsets;
        int            numGroupCols = list_length(linitial(gsets));

        /*
         * In AGG_SORTED or AGG_PLAIN mode, the first rollup takes the
         * (already-sorted) input, and following ones do their own sort.
         *
         * In AGG_HASHED mode, there is one rollup for each grouping set.
         *
         * In AGG_MIXED mode, the first rollups are hashed, the first
         * non-hashed one takes the (already-sorted) input, and following ones
         * do their own sort.
         */
        if (is_first)
        {
            cost_agg(&pathnode->path, root,
                     aggstrategy,
                     agg_costs,
                     numGroupCols,
                     rollup->numGroups,
                     subpath->startup_cost,
                     subpath->total_cost,
                     subpath->rows);
            is_first = false;
            if (!rollup->is_hashed)
                is_first_sort = false;
        }
        else
        {
            Path        sort_path;    /* dummy for result of cost_sort */
            Path        agg_path;    /* dummy for result of cost_agg */

            if (rollup->is_hashed || is_first_sort)
            {
                /*
                 * Account for cost of aggregation, but don't charge input
                 * cost again
                 */
                cost_agg(&agg_path, root,
                         rollup->is_hashed ? AGG_HASHED : AGG_SORTED,
                         agg_costs,
                         numGroupCols,
                         rollup->numGroups,
                         0.0, 0.0,
                         subpath->rows);
                if (!rollup->is_hashed)
                    is_first_sort = false;
            }
            else
            {
                /* Account for cost of sort, but don't charge input cost again */
                cost_sort(&sort_path, root, NIL,
                          0.0,
                          subpath->rows,
                          subpath->pathtarget->width,
                          0.0,
                          work_mem,
                          -1.0);

                /* Account for cost of aggregation */

                cost_agg(&agg_path, root,
                         AGG_SORTED,
                         agg_costs,
                         numGroupCols,
                         rollup->numGroups,
                         sort_path.startup_cost,
                         sort_path.total_cost,
                         sort_path.rows);
            }

            pathnode->path.total_cost += agg_path.total_cost;
            pathnode->path.rows += agg_path.rows;
        }

#ifdef __TBASE__
		if (g_hybrid_hash_agg)
		{
			if (rollup->is_hashed)
			{
				rollup->entrySize = estimate_hashagg_entrysize(subpath, agg_costs, numGroups);
			}
		}
#endif
    }

    /* add tlist eval cost for each output row */
    pathnode->path.startup_cost += target->cost.startup;
    pathnode->path.total_cost += target->cost.startup +
        target->cost.per_tuple * pathnode->path.rows;

    return pathnode;
}

/*
 * create_minmaxagg_path
 *      Creates a pathnode that represents computation of MIN/MAX aggregates
 *
 * 'rel' is the parent relation associated with the result
 * 'target' is the PathTarget to be computed
 * 'mmaggregates' is a list of MinMaxAggInfo structs
 * 'quals' is the HAVING quals if any
 */
MinMaxAggPath *
create_minmaxagg_path(PlannerInfo *root,
                      RelOptInfo *rel,
                      PathTarget *target,
                      List *mmaggregates,
                      List *quals)
{
    MinMaxAggPath *pathnode = makeNode(MinMaxAggPath);
    Cost        initplan_cost;
    ListCell   *lc;

    /* The topmost generated Plan node will be a Result */
    pathnode->path.pathtype = T_Result;
    pathnode->path.parent = rel;
    pathnode->path.pathtarget = target;
    /* For now, assume we are above any joins, so no parameterization */
    pathnode->path.param_info = NULL;
    pathnode->path.parallel_aware = false;
    /* A MinMaxAggPath implies use of subplans, so cannot be parallel-safe */
    pathnode->path.parallel_safe = false;
    pathnode->path.parallel_workers = 0;
    /* Result is one unordered row */
    pathnode->path.rows = 1;
    pathnode->path.pathkeys = NIL;

    pathnode->mmaggregates = mmaggregates;
    pathnode->quals = quals;

    /* Calculate cost of all the initplans ... */
    initplan_cost = 0;
    foreach(lc, mmaggregates)
    {
        MinMaxAggInfo *mminfo = (MinMaxAggInfo *) lfirst(lc);

        initplan_cost += mminfo->pathcost;
    }

    /* add tlist eval cost for each output row, plus cpu_tuple_cost */
    pathnode->path.startup_cost = initplan_cost + target->cost.startup;
    pathnode->path.total_cost = initplan_cost + target->cost.startup +
        target->cost.per_tuple + cpu_tuple_cost;

    return pathnode;
}

/*
 * create_windowagg_path
 *      Creates a pathnode that represents computation of window functions
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'target' is the PathTarget to be computed
 * 'windowFuncs' is a list of WindowFunc structs
 * 'winclause' is a WindowClause that is common to all the WindowFuncs
 * 'winpathkeys' is the pathkeys for the PARTITION keys + ORDER keys
 *
 * The actual sort order of the input must match winpathkeys, but might
 * have additional keys after those.
 */
WindowAggPath *
create_windowagg_path(PlannerInfo *root,
                      RelOptInfo *rel,
                      Path *subpath,
                      PathTarget *target,
                      List *windowFuncs,
                      WindowClause *winclause,
                      List *winpathkeys)
{
    WindowAggPath *pathnode = makeNode(WindowAggPath);

    pathnode->path.pathtype = T_WindowAgg;
    pathnode->path.parent = rel;
    pathnode->path.pathtarget = target;
    /* For now, assume we are above any joins, so no parameterization */
    pathnode->path.param_info = NULL;
    pathnode->path.parallel_aware = false;
    pathnode->path.parallel_safe = rel->consider_parallel &&
        subpath->parallel_safe;
    pathnode->path.parallel_workers = subpath->parallel_workers;
    /* WindowAgg preserves the input sort order */
    pathnode->path.pathkeys = subpath->pathkeys;

    pathnode->subpath = subpath;
    pathnode->winclause = winclause;
    pathnode->winpathkeys = winpathkeys;

    pathnode->path.distribution = copyObject(subpath->distribution);

    /*
     * For costing purposes, assume that there are no redundant partitioning
     * or ordering columns; it's not worth the trouble to deal with that
     * corner case here.  So we just pass the unmodified list lengths to
     * cost_windowagg.
     */
    cost_windowagg(&pathnode->path, root,
                   windowFuncs,
                   list_length(winclause->partitionClause),
                   list_length(winclause->orderClause),
                   subpath->startup_cost,
                   subpath->total_cost,
                   subpath->rows);

    /* add tlist eval cost for each output row */
    pathnode->path.startup_cost += target->cost.startup;
    pathnode->path.total_cost += target->cost.startup +
        target->cost.per_tuple * pathnode->path.rows;

    return pathnode;
}

/*
 * create_setop_path
 *      Creates a pathnode that represents computation of INTERSECT or EXCEPT
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'cmd' is the specific semantics (INTERSECT or EXCEPT, with/without ALL)
 * 'strategy' is the implementation strategy (sorted or hashed)
 * 'distinctList' is a list of SortGroupClause's representing the grouping
 * 'flagColIdx' is the column number where the flag column will be, if any
 * 'firstFlag' is the flag value for the first input relation when hashing;
 *        or -1 when sorting
 * 'numGroups' is the estimated number of distinct groups
 * 'outputRows' is the estimated number of output rows
 */
SetOpPath *
create_setop_path(PlannerInfo *root,
                  RelOptInfo *rel,
                  Path *subpath,
                  SetOpCmd cmd,
                  SetOpStrategy strategy,
                  List *distinctList,
                  AttrNumber flagColIdx,
                  int firstFlag,
                  double numGroups,
                  double outputRows)
{
    SetOpPath  *pathnode = makeNode(SetOpPath);

    pathnode->path.pathtype = T_SetOp;
    pathnode->path.parent = rel;
    /* SetOp doesn't project, so use source path's pathtarget */
    pathnode->path.pathtarget = subpath->pathtarget;
    /* For now, assume we are above any joins, so no parameterization */
    pathnode->path.param_info = NULL;
    pathnode->path.parallel_aware = false;
    pathnode->path.parallel_safe = rel->consider_parallel &&
        subpath->parallel_safe;
    pathnode->path.parallel_workers = subpath->parallel_workers;
    /* SetOp preserves the input sort order if in sort mode */
    pathnode->path.pathkeys =
        (strategy == SETOP_SORTED) ? subpath->pathkeys : NIL;

    pathnode->path.distribution = copyObject(subpath->distribution);

    pathnode->subpath = subpath;
    pathnode->cmd = cmd;
    pathnode->strategy = strategy;
    pathnode->distinctList = distinctList;
    pathnode->flagColIdx = flagColIdx;
    pathnode->firstFlag = firstFlag;
    pathnode->numGroups = numGroups;

    /*
     * Charge one cpu_operator_cost per comparison per input tuple. We assume
     * all columns get compared at most of the tuples.
     */
    pathnode->path.startup_cost = subpath->startup_cost;
    pathnode->path.total_cost = subpath->total_cost +
        cpu_operator_cost * subpath->rows * list_length(distinctList);
    pathnode->path.rows = outputRows;

    return pathnode;
}

/*
 * create_recursiveunion_path
 *      Creates a pathnode that represents a recursive UNION node
 *
 * 'rel' is the parent relation associated with the result
 * 'leftpath' is the source of data for the non-recursive term
 * 'rightpath' is the source of data for the recursive term
 * 'target' is the PathTarget to be computed
 * 'distinctList' is a list of SortGroupClause's representing the grouping
 * 'wtParam' is the ID of Param representing work table
 * 'numGroups' is the estimated number of groups
 *
 * For recursive UNION ALL, distinctList is empty and numGroups is zero
 */
RecursiveUnionPath *
create_recursiveunion_path(PlannerInfo *root,
                           RelOptInfo *rel,
                           Path *leftpath,
                           Path *rightpath,
                           PathTarget *target,
                           List *distinctList,
                           int wtParam,
                           double numGroups)
{
    RecursiveUnionPath *pathnode = makeNode(RecursiveUnionPath);

    pathnode->path.pathtype = T_RecursiveUnion;
    pathnode->path.parent = rel;
    pathnode->path.pathtarget = target;
    /* For now, assume we are above any joins, so no parameterization */
    pathnode->path.param_info = NULL;
    pathnode->path.parallel_aware = false;
    pathnode->path.parallel_safe = rel->consider_parallel &&
        leftpath->parallel_safe && rightpath->parallel_safe;
    /* Foolish, but we'll do it like joins for now: */
    pathnode->path.parallel_workers = leftpath->parallel_workers;
    /* RecursiveUnion result is always unsorted */
    pathnode->path.pathkeys = NIL;

    /*
     * FIXME This assumes left/right path have the same distribution, or one
     * of them is NULL. This is related to the subquery_planner() assuming all
     * tables are replicated on the same group of nodes, which may or may not
     * be the case, and we need to be more careful about it.
     */
    if (leftpath->distribution)
        pathnode->path.distribution = copyObject(leftpath->distribution);
    else
        pathnode->path.distribution = copyObject(rightpath->distribution);

    pathnode->leftpath = leftpath;
    pathnode->rightpath = rightpath;
    pathnode->distinctList = distinctList;
    pathnode->wtParam = wtParam;
    pathnode->numGroups = numGroups;

    cost_recursive_union(&pathnode->path, leftpath, rightpath);

    return pathnode;
}

/*
 * create_lockrows_path
 *      Creates a pathnode that represents acquiring row locks
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'rowMarks' is a list of PlanRowMark's
 * 'epqParam' is the ID of Param for EvalPlanQual re-eval
 */
LockRowsPath *
create_lockrows_path(PlannerInfo *root, RelOptInfo *rel,
                     Path *subpath, List *rowMarks, int epqParam)
{
    LockRowsPath *pathnode = makeNode(LockRowsPath);

    pathnode->path.pathtype = T_LockRows;
    pathnode->path.parent = rel;
    /* LockRows doesn't project, so use source path's pathtarget */
    pathnode->path.pathtarget = subpath->pathtarget;
    /* For now, assume we are above any joins, so no parameterization */
    pathnode->path.param_info = NULL;
    pathnode->path.parallel_aware = false;
    pathnode->path.parallel_safe = false;
    pathnode->path.parallel_workers = 0;
    pathnode->path.rows = subpath->rows;

    pathnode->path.distribution = copyObject(subpath->distribution);

    /*
     * The result cannot be assumed sorted, since locking might cause the sort
     * key columns to be replaced with new values.
     */
    pathnode->path.pathkeys = NIL;

    pathnode->subpath = subpath;
    pathnode->rowMarks = rowMarks;
    pathnode->epqParam = epqParam;

    /*
     * We should charge something extra for the costs of row locking and
     * possible refetches, but it's hard to say how much.  For now, use
     * cpu_tuple_cost per row.
     */
    pathnode->path.startup_cost = subpath->startup_cost;
    pathnode->path.total_cost = subpath->total_cost +
        cpu_tuple_cost * subpath->rows;

    return pathnode;
}

/*
 * create_modifytable_path
 *      Creates a pathnode that represents performing INSERT/UPDATE/DELETE mods
 *
 * 'rel' is the parent relation associated with the result
 * 'operation' is the operation type
 * 'canSetTag' is true if we set the command tag/es_processed
 * 'nominalRelation' is the parent RT index for use of EXPLAIN
 * 'partitioned_rels' is an integer list of RT indexes of non-leaf tables in
 *        the partition tree, if this is an UPDATE/DELETE to a partitioned table.
 *        Otherwise NIL.
 * 'partColsUpdated' is true if any partitioning columns are being updated,
 *		either from the target relation or a descendent partitioned table.
 * 'resultRelations' is an integer list of actual RT indexes of target rel(s)
 * 'subpaths' is a list of Path(s) producing source data (one per rel)
 * 'subroots' is a list of PlannerInfo structs (one per rel)
 * 'withCheckOptionLists' is a list of WCO lists (one per rel)
 * 'returningLists' is a list of RETURNING tlists (one per rel)
 * 'rowMarks' is a list of PlanRowMarks (non-locking only)
 * 'onconflict' is the ON CONFLICT clause, or NULL
 * 'epqParam' is the ID of Param for EvalPlanQual re-eval
 */
ModifyTablePath *
create_modifytable_path(PlannerInfo *root, RelOptInfo *rel,
                        CmdType operation, bool canSetTag,
                        Index nominalRelation, List *partitioned_rels,
						bool partColsUpdated,
                        List *resultRelations, List *subpaths,
                        List *subroots,
                        List *withCheckOptionLists, List *returningLists,
                        List *rowMarks, OnConflictExpr *onconflict,
                        int epqParam)
{
    ModifyTablePath *pathnode = makeNode(ModifyTablePath);
    double        total_size;
    ListCell   *lc;

    Assert(list_length(resultRelations) == list_length(subpaths));
    Assert(list_length(resultRelations) == list_length(subroots));
    Assert(withCheckOptionLists == NIL ||
           list_length(resultRelations) == list_length(withCheckOptionLists));
    Assert(returningLists == NIL ||
           list_length(resultRelations) == list_length(returningLists));

    pathnode->path.pathtype = T_ModifyTable;
    pathnode->path.parent = rel;
    /* pathtarget is not interesting, just make it minimally valid */
    pathnode->path.pathtarget = rel->reltarget;
    /* For now, assume we are above any joins, so no parameterization */
    pathnode->path.param_info = NULL;
    pathnode->path.parallel_aware = false;
    pathnode->path.parallel_safe = false;
    pathnode->path.parallel_workers = 0;
    pathnode->path.pathkeys = NIL;

    /*
     * Compute cost & rowcount as sum of subpath costs & rowcounts.
     *
     * Currently, we don't charge anything extra for the actual table
     * modification work, nor for the WITH CHECK OPTIONS or RETURNING
     * expressions if any.  It would only be window dressing, since
     * ModifyTable is always a top-level node and there is no way for the
     * costs to change any higher-level planning choices.  But we might want
     * to make it look better sometime.
     */
    pathnode->path.startup_cost = 0;
    pathnode->path.total_cost = 0;
    pathnode->path.rows = 0;
    total_size = 0;
    foreach(lc, subpaths)
    {
        Path       *subpath = (Path *) lfirst(lc);

        if (lc == list_head(subpaths))    /* first node? */
            pathnode->path.startup_cost = subpath->startup_cost;
        pathnode->path.total_cost += subpath->total_cost;
        pathnode->path.rows += subpath->rows;
        total_size += subpath->pathtarget->width * subpath->rows;
    }

    /*
     * Set width to the average width of the subpath outputs.  XXX this is
     * totally wrong: we should report zero if no RETURNING, else an average
     * of the RETURNING tlist widths.  But it's what happened historically,
     * and improving it is a task for another day.
     */
    if (pathnode->path.rows > 0)
        total_size /= pathnode->path.rows;
    pathnode->path.pathtarget->width = rint(total_size);

    pathnode->operation = operation;
    pathnode->canSetTag = canSetTag;
    pathnode->nominalRelation = nominalRelation;
    pathnode->partitioned_rels = list_copy(partitioned_rels);
	pathnode->partColsUpdated = partColsUpdated;
    pathnode->resultRelations = resultRelations;
    pathnode->subpaths = subpaths;
    pathnode->subroots = subroots;
    pathnode->withCheckOptionLists = withCheckOptionLists;
    pathnode->returningLists = returningLists;
    pathnode->rowMarks = rowMarks;
    pathnode->onconflict = onconflict;
    pathnode->epqParam = epqParam;

    return pathnode;
}

/*
 * create_limit_path
 *      Creates a pathnode that represents performing LIMIT/OFFSET
 *
 * In addition to providing the actual OFFSET and LIMIT expressions,
 * the caller must provide estimates of their values for costing purposes.
 * The estimates are as computed by preprocess_limit(), ie, 0 represents
 * the clause not being present, and -1 means it's present but we could
 * not estimate its value.
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'limitOffset' is the actual OFFSET expression, or NULL
 * 'limitCount' is the actual LIMIT expression, or NULL
 * 'offset_est' is the estimated value of the OFFSET expression
 * 'count_est' is the estimated value of the LIMIT expression
 */
LimitPath *
create_limit_path(PlannerInfo *root, RelOptInfo *rel,
                  Path *subpath,
                  Node *limitOffset, Node *limitCount,
                  int64 offset_est, int64 count_est,
                  bool skipEarlyFinish)
{// #lizard forgives
    LimitPath  *pathnode = makeNode(LimitPath);

    pathnode->path.pathtype = T_Limit;
    pathnode->path.parent = rel;
    /* Limit doesn't project, so use source path's pathtarget */
    pathnode->path.pathtarget = subpath->pathtarget;
    /* For now, assume we are above any joins, so no parameterization */
    pathnode->path.param_info = NULL;
    pathnode->path.parallel_aware = false;
    pathnode->path.parallel_safe = rel->consider_parallel &&
        subpath->parallel_safe;
    pathnode->path.parallel_workers = subpath->parallel_workers;
    pathnode->path.rows = subpath->rows;
    pathnode->path.startup_cost = subpath->startup_cost;
    pathnode->path.total_cost = subpath->total_cost;
    pathnode->path.pathkeys = subpath->pathkeys;
    pathnode->subpath = subpath;
    pathnode->limitOffset = limitOffset;
    pathnode->limitCount = limitCount;
	pathnode->skipEarlyFinish = skipEarlyFinish;

    pathnode->path.distribution = copyObject(subpath->distribution);

    /*
     * Adjust the output rows count and costs according to the offset/limit.
     * This is only a cosmetic issue if we are at top level, but if we are
     * building a subquery then it's important to report correct info to the
     * outer planner.
     *
     * When the offset or count couldn't be estimated, use 10% of the
     * estimated number of rows emitted from the subpath.
     *
     * XXX we don't bother to add eval costs of the offset/limit expressions
     * themselves to the path costs.  In theory we should, but in most cases
     * those expressions are trivial and it's just not worth the trouble.
     */
    if (offset_est != 0)
    {
        double        offset_rows;

        if (offset_est > 0)
            offset_rows = (double) offset_est;
        else
            offset_rows = clamp_row_est(subpath->rows * 0.10);
        if (offset_rows > pathnode->path.rows)
            offset_rows = pathnode->path.rows;
        if (subpath->rows > 0)
            pathnode->path.startup_cost +=
                (subpath->total_cost - subpath->startup_cost)
                * offset_rows / subpath->rows;
        pathnode->path.rows -= offset_rows;
        if (pathnode->path.rows < 1)
            pathnode->path.rows = 1;
    }

    if (count_est != 0)
    {
        double        count_rows;

        if (count_est > 0)
            count_rows = (double) count_est;
        else
            count_rows = clamp_row_est(subpath->rows * 0.10);
        if (count_rows > pathnode->path.rows)
            count_rows = pathnode->path.rows;
        if (subpath->rows > 0)
            pathnode->path.total_cost = pathnode->path.startup_cost +
                (subpath->total_cost - subpath->startup_cost)
                * count_rows / subpath->rows;
        pathnode->path.rows = count_rows;
        if (pathnode->path.rows < 1)
            pathnode->path.rows = 1;
    }

    return pathnode;
}


/*
 * reparameterize_path
 *        Attempt to modify a Path to have greater parameterization
 *
 * We use this to attempt to bring all child paths of an appendrel to the
 * same parameterization level, ensuring that they all enforce the same set
 * of join quals (and thus that that parameterization can be attributed to
 * an append path built from such paths).  Currently, only a few path types
 * are supported here, though more could be added at need.  We return NULL
 * if we can't reparameterize the given path.
 *
 * Note: we intentionally do not pass created paths to add_path(); it would
 * possibly try to delete them on the grounds of being cost-inferior to the
 * paths they were made from, and we don't want that.  Paths made here are
 * not necessarily of general-purpose usefulness, but they can be useful
 * as members of an append path.
 */
Path *
reparameterize_path(PlannerInfo *root, Path *path,
                    Relids required_outer,
                    double loop_count)
{// #lizard forgives
    RelOptInfo *rel = path->parent;

    /* Can only increase, not decrease, path's parameterization */
    if (!bms_is_subset(PATH_REQ_OUTER(path), required_outer))
        return NULL;
    switch (path->pathtype)
    {
        case T_SeqScan:
            return create_seqscan_path(root, rel, required_outer, 0);
        case T_SampleScan:
            return (Path *) create_samplescan_path(root, rel, required_outer);
        case T_IndexScan:
        case T_IndexOnlyScan:
            {
                IndexPath  *ipath = (IndexPath *) path;
                IndexPath  *newpath = makeNode(IndexPath);

                /*
                 * We can't use create_index_path directly, and would not want
                 * to because it would re-compute the indexqual conditions
                 * which is wasted effort.  Instead we hack things a bit:
                 * flat-copy the path node, revise its param_info, and redo
                 * the cost estimate.
                 */
                memcpy(newpath, ipath, sizeof(IndexPath));
                newpath->path.param_info =
                    get_baserel_parampathinfo(root, rel, required_outer);
                cost_index(newpath, root, loop_count, false);
                return (Path *) newpath;
            }
        case T_BitmapHeapScan:
            {
                BitmapHeapPath *bpath = (BitmapHeapPath *) path;

                return (Path *) create_bitmap_heap_path(root,
                                                        rel,
                                                        bpath->bitmapqual,
                                                        required_outer,
                                                        loop_count, 0);
            }
        case T_SubqueryScan:
#ifdef XCP
            {
                SubqueryScanPath *spath = (SubqueryScanPath *) path;

                return (Path *) create_subqueryscan_path(root,
                                                         rel,
                                                         spath->subpath,
                                                         spath->path.pathkeys,
                                                         required_outer,
                                                         path->distribution);
            }
#else
            {
                SubqueryScanPath *spath = (SubqueryScanPath *) path;

                return (Path *) create_subqueryscan_path(root,
                                                         rel,
                                                         spath->subpath,
                                                         spath->path.pathkeys,
                                                         required_outer);
            }
#endif
        default:
            break;
    }
    return NULL;
}

/*
 * reparameterize_path_by_child
 * 		Given a path parameterized by the parent of the given child relation,
 * 		translate the path to be parameterized by the given child relation.
 *
 * The function creates a new path of the same type as the given path, but
 * parameterized by the given child relation.  Most fields from the original
 * path can simply be flat-copied, but any expressions must be adjusted to
 * refer to the correct varnos, and any paths must be recursively
 * reparameterized.  Other fields that refer to specific relids also need
 * adjustment.
 *
 * The cost, number of rows, width and parallel path properties depend upon
 * path->parent, which does not change during the translation. Hence those
 * members are copied as they are.
 *
 * If the given path can not be reparameterized, the function returns NULL.
 */
Path *
reparameterize_path_by_child(PlannerInfo *root, Path *path,
							 RelOptInfo *child_rel)
{

#define FLAT_COPY_PATH(newnode, node, nodetype)  \
	( (newnode) = makeNode(nodetype), \
	  memcpy((newnode), (node), sizeof(nodetype)) )

#define ADJUST_CHILD_ATTRS(node) \
	((node) = \
	 (List *) adjust_appendrel_attrs_multilevel(root, (Node *) (node), \
												child_rel->relids, \
												child_rel->top_parent_relids))

#define REPARAMETERIZE_CHILD_PATH(path) \
do { \
	(path) = reparameterize_path_by_child(root, (path), child_rel); \
	if ((path) == NULL) \
		return NULL; \
} while(0);

#define REPARAMETERIZE_CHILD_PATH_LIST(pathlist) \
do { \
	if ((pathlist) != NIL) \
	{ \
		(pathlist) = reparameterize_pathlist_by_child(root, (pathlist), \
													  child_rel); \
		if ((pathlist) == NIL) \
			return NULL; \
	} \
} while(0);

	Path	   *new_path;
	ParamPathInfo *new_ppi;
	ParamPathInfo *old_ppi;
	Relids		required_outer;

	/*
	 * If the path is not parameterized by parent of the given relation, it
	 * doesn't need reparameterization.
	 */
	if (!path->param_info ||
		!bms_overlap(PATH_REQ_OUTER(path), child_rel->top_parent_relids))
		return path;

	/* Reparameterize a copy of given path. */
	switch (nodeTag(path))
	{
		case T_Path:
			FLAT_COPY_PATH(new_path, path, Path);
			break;

		case T_IndexPath:
			{
				IndexPath  *ipath;

				FLAT_COPY_PATH(ipath, path, IndexPath);
				ADJUST_CHILD_ATTRS(ipath->indexclauses);
				ADJUST_CHILD_ATTRS(ipath->indexquals);
				new_path = (Path *) ipath;
			}
			break;

		case T_BitmapHeapPath:
			{
				BitmapHeapPath *bhpath;

				FLAT_COPY_PATH(bhpath, path, BitmapHeapPath);
				REPARAMETERIZE_CHILD_PATH(bhpath->bitmapqual);
				new_path = (Path *) bhpath;
			}
			break;

		case T_BitmapAndPath:
			{
				BitmapAndPath *bapath;

				FLAT_COPY_PATH(bapath, path, BitmapAndPath);
				REPARAMETERIZE_CHILD_PATH_LIST(bapath->bitmapquals);
				new_path = (Path *) bapath;
			}
			break;

		case T_BitmapOrPath:
			{
				BitmapOrPath *bopath;

				FLAT_COPY_PATH(bopath, path, BitmapOrPath);
				REPARAMETERIZE_CHILD_PATH_LIST(bopath->bitmapquals);
				new_path = (Path *) bopath;
			}
			break;

		case T_TidPath:
			{
				TidPath    *tpath;

				/*
				 * TidPath contains tidquals, which do not contain any
				 * external parameters per create_tidscan_path(). So don't
				 * bother to translate those.
				 */
				FLAT_COPY_PATH(tpath, path, TidPath);
				new_path = (Path *) tpath;
			}
			break;

		case T_ForeignPath:
			{
				ForeignPath *fpath;
				ReparameterizeForeignPathByChild_function rfpc_func;

				FLAT_COPY_PATH(fpath, path, ForeignPath);
				if (fpath->fdw_outerpath)
					REPARAMETERIZE_CHILD_PATH(fpath->fdw_outerpath);

				/* Hand over to FDW if needed. */
				rfpc_func =
					path->parent->fdwroutine->ReparameterizeForeignPathByChild;
				if (rfpc_func)
					fpath->fdw_private = rfpc_func(root, fpath->fdw_private,
												   child_rel);
				new_path = (Path *) fpath;
			}
			break;

		case T_CustomPath:
			{
				CustomPath *cpath;

				FLAT_COPY_PATH(cpath, path, CustomPath);
				REPARAMETERIZE_CHILD_PATH_LIST(cpath->custom_paths);
				if (cpath->methods &&
					cpath->methods->ReparameterizeCustomPathByChild)
					cpath->custom_private =
						cpath->methods->ReparameterizeCustomPathByChild(root,
																		cpath->custom_private,
																		child_rel);
				new_path = (Path *) cpath;
			}
			break;

		case T_NestPath:
			{
				JoinPath   *jpath;

				FLAT_COPY_PATH(jpath, path, NestPath);

				REPARAMETERIZE_CHILD_PATH(jpath->outerjoinpath);
				REPARAMETERIZE_CHILD_PATH(jpath->innerjoinpath);
				ADJUST_CHILD_ATTRS(jpath->joinrestrictinfo);
				new_path = (Path *) jpath;
			}
			break;

		case T_MergePath:
			{
				JoinPath   *jpath;
				MergePath  *mpath;

				FLAT_COPY_PATH(mpath, path, MergePath);

				jpath = (JoinPath *) mpath;
				REPARAMETERIZE_CHILD_PATH(jpath->outerjoinpath);
				REPARAMETERIZE_CHILD_PATH(jpath->innerjoinpath);
				ADJUST_CHILD_ATTRS(jpath->joinrestrictinfo);
				ADJUST_CHILD_ATTRS(mpath->path_mergeclauses);
				new_path = (Path *) mpath;
			}
			break;

		case T_HashPath:
			{
				JoinPath   *jpath;
				HashPath   *hpath;

				FLAT_COPY_PATH(hpath, path, HashPath);

				jpath = (JoinPath *) hpath;
				REPARAMETERIZE_CHILD_PATH(jpath->outerjoinpath);
				REPARAMETERIZE_CHILD_PATH(jpath->innerjoinpath);
				ADJUST_CHILD_ATTRS(jpath->joinrestrictinfo);
				ADJUST_CHILD_ATTRS(hpath->path_hashclauses);
				new_path = (Path *) hpath;
			}
			break;

		case T_AppendPath:
			{
				AppendPath *apath;

				FLAT_COPY_PATH(apath, path, AppendPath);
				REPARAMETERIZE_CHILD_PATH_LIST(apath->subpaths);
				new_path = (Path *) apath;
			}
			break;

		case T_MergeAppendPath:
			{
				MergeAppendPath *mapath;

				FLAT_COPY_PATH(mapath, path, MergeAppendPath);
				REPARAMETERIZE_CHILD_PATH_LIST(mapath->subpaths);
				new_path = (Path *) mapath;
			}
			break;

		case T_MaterialPath:
			{
				MaterialPath *mpath;

				FLAT_COPY_PATH(mpath, path, MaterialPath);
				REPARAMETERIZE_CHILD_PATH(mpath->subpath);
				new_path = (Path *) mpath;
			}
			break;

		case T_UniquePath:
			{
				UniquePath *upath;

				FLAT_COPY_PATH(upath, path, UniquePath);
				REPARAMETERIZE_CHILD_PATH(upath->subpath);
				ADJUST_CHILD_ATTRS(upath->uniq_exprs);
				new_path = (Path *) upath;
			}
			break;

		case T_GatherPath:
			{
				GatherPath *gpath;

				FLAT_COPY_PATH(gpath, path, GatherPath);
				REPARAMETERIZE_CHILD_PATH(gpath->subpath);
				new_path = (Path *) gpath;
			}
			break;

		case T_GatherMergePath:
			{
				GatherMergePath *gmpath;

				FLAT_COPY_PATH(gmpath, path, GatherMergePath);
				REPARAMETERIZE_CHILD_PATH(gmpath->subpath);
				new_path = (Path *) gmpath;
			}
			break;

		default:

			/* We don't know how to reparameterize this path. */
			return NULL;
	}

	/*
	 * Adjust the parameterization information, which refers to the topmost
	 * parent. The topmost parent can be multiple levels away from the given
	 * child, hence use multi-level expression adjustment routines.
	 */
	old_ppi = new_path->param_info;
	required_outer =
		adjust_child_relids_multilevel(root, old_ppi->ppi_req_outer,
									   child_rel->relids,
									   child_rel->top_parent_relids);

	/* If we already have a PPI for this parameterization, just return it */
	new_ppi = find_param_path_info(new_path->parent, required_outer);

	/*
	 * If not, build a new one and link it to the list of PPIs. For the same
	 * reason as explained in mark_dummy_rel(), allocate new PPI in the same
	 * context the given RelOptInfo is in.
	 */
	if (new_ppi == NULL)
	{
		MemoryContext oldcontext;
		RelOptInfo *rel = path->parent;

		oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(rel));

		new_ppi = makeNode(ParamPathInfo);
		new_ppi->ppi_req_outer = bms_copy(required_outer);
		new_ppi->ppi_rows = old_ppi->ppi_rows;
		new_ppi->ppi_clauses = old_ppi->ppi_clauses;
		ADJUST_CHILD_ATTRS(new_ppi->ppi_clauses);
		rel->ppilist = lappend(rel->ppilist, new_ppi);

		MemoryContextSwitchTo(oldcontext);
	}
	bms_free(required_outer);

	new_path->param_info = new_ppi;

	/*
	 * Adjust the path target if the parent of the outer relation is
	 * referenced in the targetlist. This can happen when only the parent of
	 * outer relation is laterally referenced in this relation.
	 */
	if (bms_overlap(path->parent->lateral_relids,
					child_rel->top_parent_relids))
	{
		new_path->pathtarget = copy_pathtarget(new_path->pathtarget);
		ADJUST_CHILD_ATTRS(new_path->pathtarget->exprs);
	}

	return new_path;
}

/*
 * reparameterize_pathlist_by_child
 * 		Helper function to reparameterize a list of paths by given child rel.
 */
static List *
reparameterize_pathlist_by_child(PlannerInfo *root,
								 List *pathlist,
								 RelOptInfo *child_rel)
{
	ListCell   *lc;
	List	   *result = NIL;

	foreach(lc, pathlist)
	{
		Path	   *path = reparameterize_path_by_child(root, lfirst(lc),
														child_rel);
		if (path == NULL)
		{
			list_free(result);
			return NIL;
		}

		result = lappend(result, path);
	}

	return result;
}

#ifdef __TBASE__
/*
 * Count datanode number for given path, consider replication table as 1
 * because we use this function to figure out how many parts that data
 * had been separated into, when we estimating costs of a plan. Therefore
 * to get more accurate estimating result as in a distributed system.
 */
double
path_count_datanodes(Path *path)
{
	if (path->distribution && IsA(path->distribution, Distribution) &&
	    (path->distribution->distributionType == LOCATOR_TYPE_SHARD ||
	     path->distribution->distributionType == LOCATOR_TYPE_HASH))
	{
		double nodes;
		
		nodes = bms_num_members(path->distribution->restrictNodes);
		if (nodes > 0)
			return nodes;
		
		nodes = bms_num_members(path->distribution->nodes);
		if (nodes > 0)
			return nodes;
	}
	
	return 1;
}

void
assign_constrain_nodes(List *node_list)
{
	MemoryContext oldctx = MemoryContextSwitchTo(TopMemoryContext);
	ListCell *lc;
	
	bms_free(constrainNodes);
	constrainNodes = NULL;
	
	foreach(lc, node_list)
	{
		constrainNodes = bms_add_member(constrainNodes, lfirst_int(lc));
	}
	
	MemoryContextSwitchTo(oldctx);
}
#endif
