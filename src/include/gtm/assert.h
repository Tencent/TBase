/*-------------------------------------------------------------------------
 *
 * assert.h
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#ifndef GTM_ASSERT_H
#define GTM_ASSERT_H

#include "c.h"

extern bool assert_enabled;

void ExceptionalCondition(const char *conditionName,
                         const char *errorType,
                         const char *fileName,
                         int lineNumber);

#endif

