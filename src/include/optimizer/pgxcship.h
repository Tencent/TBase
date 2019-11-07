/*-------------------------------------------------------------------------
 *
 * pgxcship.h
 *        Functionalities for the evaluation of expression shippability
 *        to remote nodes
 *
 *
 * Portions Copyright (c) 1996-2012 PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/optimizer/pgxcship.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PGXCSHIP_H
#define PGXCSHIP_H

#include "nodes/parsenodes.h"
#include "nodes/relation.h"
#include "pgxc/locator.h"
#ifdef __TBASE__
#include "utils/reltrigger.h"
#endif

/* Determine if query is shippable */
extern ExecNodes *pgxc_is_query_shippable(Query *query, int query_level);
/* Determine if an expression is shippable */
extern bool pgxc_is_expr_shippable(Expr *node, bool *has_aggs);

#ifdef __TBASE__
extern bool pgxc_check_triggers_shippability(Oid relid, int commandType);

extern bool pgxc_find_unshippable_triggers(TriggerDesc *trigdesc, int16 trig_event, 
                                       int16 trig_timing, bool ignore_timing);
extern bool pgxc_is_trigger_shippable(Trigger *trigger);

extern Node *get_var_from_arg(Node *arg);

extern bool is_var_distribute_column(Var *var, List *rtable);
#endif
#endif
