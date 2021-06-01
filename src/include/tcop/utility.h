/*-------------------------------------------------------------------------
 *
 * utility.h
 *	  prototypes for utility.c.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/tcop/utility.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef UTILITY_H
#define UTILITY_H

#include "tcop/tcopprot.h"
#ifdef __TBASE__
#include "lib/stringinfo.h"
#endif
typedef enum
{
	PROCESS_UTILITY_TOPLEVEL,	/* toplevel interactive command */
	PROCESS_UTILITY_QUERY,		/* a complete query, but not toplevel */
	PROCESS_UTILITY_SUBCOMMAND	/* a portion of a query */
} ProcessUtilityContext;

/* Hook for plugins to get control in ProcessUtility() */
typedef void (*ProcessUtility_hook_type) (PlannedStmt *pstmt,
										  const char *queryString, ProcessUtilityContext context,
										  ParamListInfo params,
										  QueryEnvironment *queryEnv,
										  DestReceiver *dest,
										  bool sentToRemote,
										  char *completionTag);
extern PGDLLIMPORT ProcessUtility_hook_type ProcessUtility_hook;

extern void ProcessUtility(PlannedStmt *pstmt, const char *queryString,
			   ProcessUtilityContext context, ParamListInfo params,
			   QueryEnvironment *queryEnv,
			   DestReceiver *dest,
			   bool sentToRemote,
			   char *completionTag);
extern void standard_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
						ProcessUtilityContext context, ParamListInfo params,
						QueryEnvironment *queryEnv,
						DestReceiver *dest,
						bool sentToRemote,
						char *completionTag);

extern bool UtilityReturnsTuples(Node *parsetree);

extern TupleDesc UtilityTupleDescriptor(Node *parsetree);

extern Query *UtilityContainsQuery(Node *parsetree);

extern const char *CreateCommandTag(Node *parsetree);

extern LogStmtLevel GetCommandLogLevel(Node *parsetree);

extern bool CommandIsReadOnly(PlannedStmt *pstmt);

#ifdef PGXC
extern bool pgxc_lock_for_utility_stmt(Node *parsetree);
#endif
#ifdef __TBASE__
typedef void (*ErrcodeHookType) (ErrorData *edata, StringInfo buff);
extern PGDLLIMPORT ErrcodeHookType g_pfErrcodeHook;

/* Does txn include parallel DDLs */
extern bool is_txn_has_parallel_ddl;
/* Parallel DDL switch */
extern bool enable_parallel_ddl;

#define LOCAL_PARALLEL_DDL	\
	(IS_PGXC_LOCAL_COORDINATOR && is_txn_has_parallel_ddl)
extern void CheckAndDropRole(Node *parsetree, bool sentToRemote,
								const char *queryString);
extern void CheckAndSendLeaderCNReindex(bool sentToRemote, ReindexStmt *stmt,
											const char *queryString);

/* Has leader CN executed ddl */
extern bool leader_cn_executed_ddl;
extern void SendLeaderCNUtility(const char *queryString, bool temp);
extern void SendLeaderCNUtilityWithContext(const char *queryString, bool temp);
#endif
#endif							/* UTILITY_H */
