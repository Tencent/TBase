/*-------------------------------------------------------------------------
 *
 * subset.c
 *	  POSTGRES user defined column correlationship
 *
 * Portions Copyright (c) 2020-Present, TBase Development Team, Tencent
 *
 * IDENTIFICATION
 *	  src/backend/statistics/knowledge.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_statistic_ext.h"
#include "nodes/relation.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "statistics/extended_stats_internal.h"
#include "statistics/statistics.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

static bool subset_is_compatible_clause(Node *clause, Index relid,
						AttrNumber *attnum);
static bool subset_implies_attribute(MVDependency *dependency,
						AttrNumber attnum);

/*
 * Build subset dependencies between groups of columns
 */
MVDependencies *
statext_subset_build(int numrows, List *columns)
{
	int			i;
	int			k;

	/* result */
	MVDependencies *dependencies = NULL;
	MVDependency   *d;

	/* Currently, we only support subset defined with 2 columns */
	Assert(list_length(columns) == 2);
	k = list_length(columns);

	/* initialize the list of dependencies */
	dependencies = (MVDependencies *) palloc0(sizeof(MVDependencies));

	dependencies->magic = STATS_DEPS_MAGIC;
	dependencies->type = STATS_DEPS_TYPE_BASIC;
	dependencies->ndeps = 1;

	dependencies = (MVDependencies *) repalloc(dependencies,
											   offsetof(MVDependencies, deps)
											   + dependencies->ndeps * sizeof(MVDependency));

	d = (MVDependency *) palloc0(offsetof(MVDependency, attributes)
								 + k * sizeof(AttrNumber));
	d->degree = 1.0;
	d->nattributes = k;
	for (i = 0; i < k; i++)
	{
		d->attributes[i] = list_nth_int(columns, i);
	}

	dependencies->deps[0] = d;

	return dependencies;
}

/*
 * statext_subset_load
 *		Load the subset dependency for the indicated pg_statistic_ext tuple
 */
MVDependencies *
statext_subset_load(Oid mvoid)
{
	bool		isnull;
	Datum		deps;
	HeapTuple	htup = SearchSysCache1(STATEXTOID, ObjectIdGetDatum(mvoid));

	if (!HeapTupleIsValid(htup))
		elog(ERROR, "cache lookup failed for statistics object %u", mvoid);

	deps = SysCacheGetAttr(STATEXTOID, htup,
						   Anum_pg_statistic_ext_stxsubset, &isnull);
	Assert(!isnull);

	ReleaseSysCache(htup);

	/* Reuse the functional dependencies deserialize function */
	return statext_dependencies_deserialize(DatumGetByteaP(deps));
}

/*
 * subset_is_compatible_clause
 *		Determines if the clause is compatible with subset dependencies
 *
 * When returning True attnum is set to the attribute number of the Var within
 * the supported clause. Comparing to dependencies compatibility check, subset
 * is less restrictive.
 */
static bool
subset_is_compatible_clause(Node *clause, Index relid, AttrNumber *attnum)
{
	RestrictInfo *rinfo = (RestrictInfo *) clause;

	if (!IsA(rinfo, RestrictInfo))
		return false;

	/* Pseudoconstants are not really interesting here. */
	if (rinfo->pseudoconstant)
		return false;

	/* clauses referencing multiple varnos are incompatible */
	if (bms_membership(rinfo->clause_relids) != BMS_SINGLETON)
		return false;

	if (is_opclause(rinfo->clause))
	{
		OpExpr	   *expr = (OpExpr *) rinfo->clause;
		Var		   *var;
		bool		varonleft = true;
		bool		ok;

		/* Only expressions with two arguments are considered compatible. */
		if (list_length(expr->args) != 2)
			return false;

		/* see if it actually has the right */
		ok = (NumRelids((Node *) expr) == 1) &&
			(is_pseudo_constant_clause(lsecond(expr->args)) ||
			 (varonleft = false,
			  is_pseudo_constant_clause(linitial(expr->args))));

		/* unsupported structure (two variables or so) */
		if (!ok)
			return false;

		var = (varonleft) ? linitial(expr->args) : lsecond(expr->args);

		/* in case it's a T_RelableType */
		if (IsA(var, RelabelType))
			var = (Var *) ((RelabelType *) var)->arg;

		/* We only support plain Vars for now */
		if (!IsA(var, Var))
			return false;

		/* Ensure var is from the correct relation */
		if (var->varno != relid)
			return false;

		/* we also better ensure the Var is from the current level */
		if (var->varlevelsup > 0)
			return false;

		/* Also skip system attributes (we don't allow stats on those). */
		if (!AttrNumberIsForUserDefinedAttr(var->varattno))
			return false;

		*attnum = var->varattno;
		return true;
	}

	return false;
}

/*
 * subset_eliminate_attribute
 *		check that the attnum matches is implied by the subset dependency
 */
static bool
subset_implies_attribute(MVDependency *dependency, AttrNumber attnum)
{
	if (attnum == dependency->attributes[dependency->nattributes - 1])
		return true;

	return false;
}

/*
 * subset_clauselist_selectivity
 *		Return the estimated selectivity of the given clauses using
 *		functional dependency statistics, or 1.0 if no useful functional
 *		dependency statistic exists.
 *
 * 'estimatedclauses' is an output argument that gets a bit set corresponding
 * to the (zero-based) list index of clauses that are included in the
 * estimated selectivity.
 *
 * Given equality clauses on attributes (a,b) we find the strongest dependency
 * between them, i.e. either (a=>b) or (b=>a). Assuming (a=>b) is the selected
 * dependency, we then combine the per-clause selectivities using the formula
 */
Selectivity
subset_clauselist_selectivity(PlannerInfo *root,
							  List *clauses,
							  int varRelid,
							  JoinType jointype,
							  SpecialJoinInfo *sjinfo,
							  RelOptInfo *rel,
							  Bitmapset **estimatedclauses)
{
	Selectivity s1 = 1.0;
	ListCell   *l;
	Bitmapset  *clauses_attnums = NULL;
	StatisticExtInfo *stat;
	MVDependencies *dependencies;
	AttrNumber *list_attnums;
	int			listidx;

	/* check if there's any stats that might be useful for us. */
	if (!has_stats_of_kind(rel->statlist, STATS_EXT_SUBSET))
		return 1.0;

	list_attnums = (AttrNumber *) palloc(sizeof(AttrNumber) *
										 list_length(clauses));

	/*
	 * Pre-process the clauses list to extract the attnums seen in each item.
	 * We need to determine if there's any clauses which will be useful for
	 * subset selectivity elimination. Along the way we'll record all of
	 * the attnums for each clause in a list which we'll reference later so we
	 * don't need to repeat the same work again. We'll also keep track of all
	 * attnums seen.
	 */
	listidx = 0;
	foreach(l, clauses)
	{
		Node	   *clause = (Node *) lfirst(l);
		AttrNumber	attnum;

		if (subset_is_compatible_clause(clause, rel->relid, &attnum))
		{
			list_attnums[listidx] = attnum;
			clauses_attnums = bms_add_member(clauses_attnums, attnum);
		}
		else
			list_attnums[listidx] = InvalidAttrNumber;

		listidx++;
	}

	/*
	 * If there's not at least two distinct attnums then reject the whole list
	 * of clauses. We must return 1.0 so the calling function's selectivity is
	 * unaffected.
	 */
	if (bms_num_members(clauses_attnums) < 2)
	{
		pfree(list_attnums);
		return 1.0;
	}

	/* find the best suited statistics object for these attnums */
	stat = choose_best_statistics(rel->statlist, clauses_attnums,
								  STATS_EXT_SUBSET);

	/* if no matching stats could be found then we've nothing to do */
	if (!stat)
	{
		pfree(list_attnums);
		return 1.0;
	}

	/*
	 * Load the dependency items stored in the statistics object.
	 */
	dependencies = statext_subset_load(stat->statOid);

	/*
	 * Apply the dependencies recursively, starting with the widest/strongest
	 * ones, and proceeding to the smaller/weaker ones. At the end of each
	 * round we factor in the selectivity of clauses on the implied attribute,
	 * and remove the clauses from the list.
	 *
	 * Actually, for subset dependency, there should be only one dependency
	 * entry. But we still keep the while loop style align with normal
	 * dependency selectivity calculation does, to get better support for
	 * possible future enhancements.
	 */
	do
	{
		Selectivity s2 = 1.0;
		MVDependency *dependency;

		/* There is only one dependency to indicate the subset relation */
		Assert(dependencies->ndeps == 1);
		dependency = dependencies->deps[0];

		/*
		 * We found an applicable dependency, so find all the clauses on the
		 * implied attribute - with dependency (a,b => c) we look for clauses
		 * on 'c'.
		 */
		listidx = -1;
		foreach(l, clauses)
		{
			Node	   *clause;

			listidx++;

			/*
			 * Skip incompatible clauses, and ones we've already estimated on.
			 */
			if (list_attnums[listidx] == InvalidAttrNumber ||
				bms_is_member(listidx, *estimatedclauses))
				continue;

			/*
			 * Technically we could find more than one clause for a given
			 * attnum. Since these clauses must be equality clauses, we choose
			 * to only take the selectivity estimate from the final clause in
			 * the list for this attnum. If the attnum happens to be compared
			 * to a different Const in another clause then no rows will match
			 * anyway. If it happens to be compared to the same Const, then
			 * ignoring the additional clause is just the thing to do.
			 */
			if (subset_implies_attribute(dependency, list_attnums[listidx]))
			{
				clause = (Node *) lfirst(l);

				s2 = clause_selectivity(root, clause, varRelid, jointype,
										sjinfo);

				/* mark this one as done, so we don't touch it again. */
				*estimatedclauses = bms_add_member(*estimatedclauses, listidx);

				/*
				 * Mark that we've got and used the dependency on this clause.
				 * We'll want to ignore this when looking for the next
				 * strongest dependency above.
				 */
				clauses_attnums = bms_del_member(clauses_attnums,
												 list_attnums[listidx]);
			}
		}

		/*
		 * Now factor in the selectivity for all the "implied" clauses into
		 * the final one, using this formula:
		 *
		 * P(a,b) = P(a) * (f + (1-f) * P(b))
		 *
		 * where 'f' is the degree of validity of the dependency.
		 *
		 * Currently, the subset statistic can only eliminate the implied
		 * clause by forcing dependency degree to 1.0.
		 */
		Assert(dependency->degree == 1.0);
		s1 *= (dependency->degree + (1 - dependency->degree) * s2);
	} while(0);

	pfree(dependencies);
	pfree(list_attnums);

	return s1;
}
