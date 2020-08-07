/*-------------------------------------------------------------------------
 *
 * distribution.c
 *	  Routines related to adjust path distribution
 *
 * Copyright (c) 2020-Present TBase development team, Tencent
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/util/distribution.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "nodes/bitmapset.h"
#include "nodes/nodes.h"
#include "optimizer/distribution.h"
#include "optimizer/paths.h"

/*
 * equal_distributions
 * 	Check that two distributions are equal.
 *
 * Distributions are considered equal if they are of the same type, on the
 * same set of nodes, and if the distribution expressions are known to be equal
 * (either the same expressions or members of the same equivalence class).
 */
bool
equal_distributions(PlannerInfo *root, Distribution *dst1,
					Distribution *dst2)
{
	/* fast path */
	if (dst1 == dst2)
		return true;

	if (dst1 == NULL || dst2 == NULL)
		return false;

	/* conditions easier to check go first */
	if (dst1->distributionType != dst2->distributionType)
		return false;

	if (!bms_equal(dst1->nodes, dst2->nodes))
		return false;

	if (equal(dst1->distributionExpr, dst2->distributionExpr))
		return true;

	/*
	 * For more thorough expression check we need to ensure they both are
	 * defined
	 */
	if (dst1->distributionExpr == NULL || dst2->distributionExpr == NULL)
		return false;

	/*
	 * More thorough check, but allows some important cases, like if
	 * distribution column is not updated (implicit set distcol=distcol) or
	 * set distcol = CONST, ... WHERE distcol = CONST - pattern used by many
	 * applications.
	 */
	if (exprs_known_equal(root, dst1->distributionExpr, dst2->distributionExpr))
		return true;

	/* The restrictNodes field does not matter for distribution equality */
	return false;
}

/*
 * Get the location of DML result relation if it appears in either subpath
 */
ResultRelLocation
getResultRelLocation(int resultRel, Relids inner, Relids outer)
{
	ResultRelLocation location = RESULT_REL_NONE;

	if (bms_is_member(resultRel, inner))
	{
		location = RESULT_REL_INNER;
	}
	else if (bms_is_member(resultRel, outer))
	{
		location = RESULT_REL_OUTER;
	}

	return location;
}

/*
 * Check if the path distribution satisfy the result relation distribution.
 */
bool
SatisfyResultRelDist(PlannerInfo *root, Path *path)
{
	PlannerInfo    *top_root = root;
	bool			equal = false;

	/* Get top root */
	while(top_root->parent_root)
	{
		top_root = top_root->parent_root;
	}

	/*
	 * Check the UPDATE/DELETE command, make sure the path distribution equals the
	 * result relation distribution.
	 * We only invalidate the check if the result relation appears in one of
	 * the left/right subpath.
	 */
	if ((top_root->parse->commandType == CMD_UPDATE ||
		 top_root->parse->commandType == CMD_DELETE) &&
		path->parent->resultRelLoc != RESULT_REL_NONE)
	{
		equal = equal_distributions(top_root,
									top_root->distribution,
									path->distribution);

		if (!equal)
			return false;
	}

	return true;
}
