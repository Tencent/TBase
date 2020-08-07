/*-------------------------------------------------------------------------
 *
 * distribution.h
 *	  Routines related to adjust distribution
 *
 * Copyright (c) 2020-Present TBase development team, Tencent
 *
 *
 * IDENTIFICATION
 *	  src/include/optimizer/distribution.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DISTRIBUTION_H
#define DISTRIBUTION_H

#include "postgres.h"

#include "nodes/relation.h"

/* TODO(TBase): Move all plan/path distribution routines to this file */

extern bool equal_distributions(PlannerInfo *root, Distribution *dst1,
					Distribution *dst2);
extern ResultRelLocation getResultRelLocation(int resultRel, Relids inner,
					Relids outer);
extern bool SatisfyResultRelDist(PlannerInfo *root, Path *path);
#endif  /* DISTRIBUTION_H */
