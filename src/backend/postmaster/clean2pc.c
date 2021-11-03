/*-------------------------------------------------------------------------
 *
 * clean2pc.c
 *
 * The background clean 2pc processes are added by whalesong.
 * They attempt to clean the abnormal 2pc.
 *
 * Portions Copyright (c) 1996-2021, TDSQL-PG Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/clean2pc.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_database.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "executor/executor.h"
#include "libpq/pqsignal.h"
#include "nodes/makefuncs.h"
#include "postmaster/clean2pc.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "pgxc/execRemote.h"
#include "storage/buf_internals.h"
#include "storage/ipc.h"
#include "storage/pmsignal.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/timeout.h"

#define MAX_GID           64

#define SQL_CMD_LEN       1024
#define MAX_DB_SIZE       100

#define DB_TEMPLATE0     "template0"
#define DB_TEMPLATE1     "template1"
#define DB_DEFAULT       "postgres"

typedef enum
{
	Query2pcAttr_gid             = 0,
	Query2pcAttr_database        = 1,
	Query2pcAttr_global_status   = 2,
	Query2pcAttr_status_on_nodes = 3,
	Query2pcAttr_butty
} Query2pcAttrEnum;

bool enable_clean_2pc_launcher = true;

int auto_clean_2pc_interval        = 60;
int auto_clean_2pc_delay           = 300;
int auto_clean_2pc_timeout         = 1200;
int auto_clean_2pc_max_check_time  = 1200;

static volatile sig_atomic_t got_SIGTERM = false;
static volatile sig_atomic_t got_SIGHUP  = false;
static volatile sig_atomic_t got_SIGUSR2 = false;

/* Flags to tell if we are in an clean 2pc process */
static bool am_clean_2pc_launcher = false;
static bool am_clean_2pc_worker   = false;

static StringInfo result_str = NULL;

#ifdef EXEC_BACKEND
static pid_t clean_2pc_launcher_forkexec(void);
static pid_t clean_2pc_worker_forkexec(void);
#endif

NON_EXEC_STATIC void
Clean2pcLauncherMain(int argc, char *argv[]) pg_attribute_noreturn();
NON_EXEC_STATIC void
Clean2pcWorkerMain(int argc, char *argv[]) pg_attribute_noreturn();

static void	start_query_worker(TimestampTz clean_time);
static void	start_clean_worker(int count);

static void do_query_2pc(TimestampTz clean_time);
static void do_clean_2pc(TimestampTz clean_time);

static void clean_2pc_sigterm_handler(SIGNAL_ARGS);
static void clean_2pc_sighup_handler(SIGNAL_ARGS);
static void clean_2pc_sigusr2_handler(SIGNAL_ARGS);

static List *get_database_list(void);
static Oid   get_default_database(void);

static void ExitCleanRunning(int status, Datum arg);

/* struct to keep track of databases in worker */
typedef struct Clean2pcDBInfo
{
	Oid   db_oid;
	char *db_name;
} Clean2pcDBInfo;

typedef struct
{
	TimestampTz clean_time;

	bool worker_running;
	Oid  worker_db;

	int  db_count;
	Oid  db_list[MAX_DB_SIZE];
} Clean2pcShmemStruct;

static Clean2pcShmemStruct *Clean2pcShmem = NULL;

/*
 * Main entry point for 2pc clean launcher, to be called from the
 * postmaster.
 */
int
StartClean2pcLauncher(void)
{
	pid_t clean_2pc_pid = 0;

#ifdef EXEC_BACKEND
	switch ((clean_2pc_pid = clean_2pc_launcher_forkexec()))
#else
	switch ((clean_2pc_pid = fork_process()))
#endif
	{
		case -1:
			ereport(LOG,
					(errmsg("could not fork 2pc clean launcher: %m")));
			return 0;

#ifndef EXEC_BACKEND
		case 0:
			/* in postmaster child ... */
			InitPostmasterChild();

			/* Close the postmaster's sockets */
			ClosePostmasterPorts(false);

			Clean2pcLauncherMain(0, NULL);
			break;
#endif
		default:
			return (int) clean_2pc_pid;
	}

	return 0;
}

/*
 * Main loop for the 2pc clean launcher.
 */
NON_EXEC_STATIC void
Clean2pcLauncherMain(int argc, char *argv[])
{
	int wait_time = 0;
	TimestampTz clean_time = GetCurrentTimestamp();

	am_clean_2pc_launcher = true;

	/* Identify myself via ps */
	init_ps_display("2pc clean launcher", "", "", "");

	elog(LOG, "2pc clean launcher start");

	SetProcessingMode(InitProcessing);

	/*
	 * Set up signal handlers.  We operate on databases much like a regular
	 * backend, so we use the same signal handling.  See equivalent code in
	 * tcop/postgres.c.
	 */
	pqsignal(SIGHUP, clean_2pc_sighup_handler);
	pqsignal(SIGTERM, clean_2pc_sigterm_handler);
	pqsignal(SIGINT, StatementCancelHandler);
	pqsignal(SIGQUIT, quickdie);
	InitializeTimeouts(); /* establishes SIGALRM handler */

	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGUSR2, clean_2pc_sigusr2_handler);
	pqsignal(SIGFPE, FloatExceptionHandler);
	pqsignal(SIGCHLD, SIG_DFL);

	PG_SETMASK(&UnBlockSig);

	/* Early initialization */
	BaseInit();

	/*
	 * Create a per-backend PGPROC struct in shared memory, except in the
	 * EXEC_BACKEND case where this was done in SubPostmasterMain. We must do
	 * this before we can use LWLocks (and in the EXEC_BACKEND case we already
	 * had to do some stuff with LWLocks).
	 */
#ifndef EXEC_BACKEND
	InitProcess();
#endif

	InitPostgres(NULL, InvalidOid, NULL, InvalidOid, NULL);

	SetProcessingMode(NormalProcessing);

	LWLockAcquire(Clean2pcLock, LW_EXCLUSIVE);
	Clean2pcShmem->worker_running = false;
	Clean2pcShmem->db_count = 0;
	Clean2pcShmem->worker_db = InvalidOid;
	LWLockRelease(Clean2pcLock);

	if (result_str == NULL)
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(TopMemoryContext);
		result_str = makeStringInfo();
		MemoryContextSwitchTo(oldcontext);
	}

	wait_time = auto_clean_2pc_delay;
	for (;;)
	{
		pg_usleep(1000000L * wait_time);

		if (got_SIGTERM)
		{
			elog(LOG, "2pc clean launcher got SIGTERM");
			got_SIGTERM = false;
			proc_exit(0);
		}

		if (got_SIGHUP)
		{
			elog(LOG, "2pc clean launcher got SIGHUP");
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
			wait_time = auto_clean_2pc_delay;
			continue;
		}

		if (got_SIGUSR2)
		{
			got_SIGUSR2 = false;
			clean_time = GetCurrentTimestamp();
			wait_time = auto_clean_2pc_delay;
			elog(LOG, "2pc clean launcher got SIGUSR2, clean_time: "
				INT64_FORMAT, clean_time);
			continue;
		}

		start_query_worker(clean_time);

		if (got_SIGTERM || got_SIGHUP || got_SIGUSR2)
		{
			wait_time = 0;
		}
		else
		{
			wait_time = auto_clean_2pc_interval;
		}
	}
}

/*
 * Main entry point for 2pc clean worker, to be called from the
 * postmaster.
 */
int
StartClean2pcWorker(void)
{
	pid_t clean_2pc_pid = 0;

#ifdef EXEC_BACKEND
	switch ((clean_2pc_pid = clean_2pc_worker_forkexec()))
#else
	switch ((clean_2pc_pid = fork_process()))
#endif
	{
		case -1:
			ereport(LOG,
					(errmsg("could not fork 2pc clean worker: %m")));
			return 0;

#ifndef EXEC_BACKEND
		case 0:
			/* in postmaster child ... */
			InitPostmasterChild();

			/* Close the postmaster's sockets */
			ClosePostmasterPorts(false);

			Clean2pcWorkerMain(0, NULL);
			break;
#endif
		default:
			return (int) clean_2pc_pid;
	}

	return 0;
}

/*
 * Main for the 2pc clean worker.
 */
NON_EXEC_STATIC void
Clean2pcWorkerMain(int argc, char *argv[])
{
	char db_name[NAMEDATALEN];
	Oid  db_oid = InvalidOid;
	int  clean_db_count = 0;

	am_clean_2pc_worker = true;

	on_proc_exit(ExitCleanRunning, 0);

	/* Identify myself via ps */
	init_ps_display("2pc clean worker", "", "", "");

	elog(LOG, "2pc clean worker start");

	SetProcessingMode(InitProcessing);

	/*
	 * Set up signal handlers.  We operate on databases much like a regular
	 * backend, so we use the same signal handling.  See equivalent code in
	 * tcop/postgres.c.
	 */
	pqsignal(SIGHUP, clean_2pc_sighup_handler);
	pqsignal(SIGTERM, clean_2pc_sigterm_handler);
	pqsignal(SIGINT, StatementCancelHandler);
	pqsignal(SIGQUIT, quickdie);
	InitializeTimeouts(); /* establishes SIGALRM handler */

	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGUSR2, clean_2pc_sigusr2_handler);
	pqsignal(SIGFPE, FloatExceptionHandler);
	pqsignal(SIGCHLD, SIG_DFL);

	PG_SETMASK(&UnBlockSig);

	/* Early initialization */
	BaseInit();

	/*
	 * Create a per-backend PGPROC struct in shared memory, except in the
	 * EXEC_BACKEND case where this was done in SubPostmasterMain. We must do
	 * this before we can use LWLocks (and in the EXEC_BACKEND case we already
	 * had to do some stuff with LWLocks).
	 */
#ifndef EXEC_BACKEND
	InitProcess();
#endif

	LWLockAcquire(Clean2pcLock, LW_EXCLUSIVE);

	Clean2pcShmem->worker_running = true;

	db_oid = Clean2pcShmem->worker_db;

	Assert(OidIsValid(db_oid));

	InitPostgres(NULL, db_oid, NULL, InvalidOid, db_name);

	SetProcessingMode(NormalProcessing);

	if (result_str == NULL)
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(TopMemoryContext);
		result_str = makeStringInfo();
		MemoryContextSwitchTo(oldcontext);
	}

	if (Clean2pcShmem->db_count == 0)
	{
		elog(DEBUG5, "query 2pc from db: %s", db_name);
		do_query_2pc(Clean2pcShmem->clean_time);
		clean_db_count = Clean2pcShmem->db_count;
	}
	else
	{
		elog(LOG, "clean 2pc for db: %s", db_name);
		do_clean_2pc(Clean2pcShmem->clean_time);
	}

	Clean2pcShmem->worker_running = false;

	LWLockRelease(Clean2pcLock);

	if (clean_db_count != 0)
	{
		start_clean_worker(clean_db_count);
	}

	/* All done, go away */
	proc_exit(0);
}

static void
do_query_2pc(TimestampTz clean_time)
{
	int                  i = 0;
	int                  count_db = 0;
	int                  count_2pc = 0;
	MemoryContext        oldcontext = NULL;
	char                 query[SQL_CMD_LEN];
	char                 gid[MAX_GID];
	char                *startnode = NULL;
	bool                 is_start_from = true;
	Oid                  db_oid = InvalidOid;
	Oid                  last_db_oid = InvalidOid;
	EState              *estate = NULL;
	RemoteQuery         *plan = NULL;
	RemoteQueryState    *pstate = NULL;
	TupleTableSlot      *result = NULL;
	Var                 *dummy = NULL;
	int                  attr_num = 4;
	int64                check_time = 0;
	TimestampTz          curr_time = GetCurrentTimestamp();
	Oid                  node_oid = 0;
	char                 node_type = PGXC_NODE_COORDINATOR;
	int                  node_index = 0;
	static const char   *attr_name[] = {"gid", "database",
							"global_transaction_status",
							"transaction_status_on_allnodes"};

	Assert(result_str != NULL);
	resetStringInfo(result_str);

	check_time = (curr_time - clean_time)/USECS_PER_SEC;

	if (check_time < 0)
	{
		elog(WARNING, "Invalid check_time: " INT64_FORMAT
			", curr_time: " INT64_FORMAT ", clean_time: " INT64_FORMAT,
			check_time, curr_time, clean_time);
		return;
	}

	if (check_time > INT32_MAX)
	{
		check_time = INT32_MAX;
	}

	if (auto_clean_2pc_max_check_time != 0)
	{
		if (check_time > auto_clean_2pc_max_check_time)
		{
			check_time = auto_clean_2pc_max_check_time;
		}
	}

	snprintf(query, SQL_CMD_LEN, "select * FROM pg_clean_check_txn("
		INT64_FORMAT ") order by database limit 1000;", check_time);

	StartTransactionCommand();

	InitMultinodeExecutor(false);

	node_oid = get_pgxc_nodeoid(PGXCNodeName);
	if (!OidIsValid(node_oid))
	{
		elog(ERROR, "get node(%s) oid failed", PGXCNodeName);
		return;
	}
	node_index = PGXCNodeGetNodeId(node_oid, &node_type);

	elog(DEBUG1, "node(%d) query: %s", node_index, query);

	plan = makeNode(RemoteQuery);
	plan->combine_type = COMBINE_TYPE_NONE;
	plan->exec_nodes = makeNode(ExecNodes);
	plan->exec_type = EXEC_ON_COORDS;

	plan->exec_nodes->nodeList = lappend_int(plan->exec_nodes->nodeList, node_index);

	plan->sql_statement = (char*)query;
	plan->force_autocommit = false;

	/*
	* We only need the target entry to determine result data type.
	* So create dummy even if real expression is a function.
	*/
	for (i = 1; i <= attr_num; i++)
	{
		dummy = makeVar(1, i, TEXTOID, 0, InvalidOid, 0);
		plan->scan.plan.targetlist = lappend(plan->scan.plan.targetlist,
										makeTargetEntry((Expr *) dummy, i, NULL, false));
	}

	/* prepare to execute */
	estate = CreateExecutorState();
	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);
	estate->es_snapshot = GetActiveSnapshot();
	pstate = ExecInitRemoteQuery(plan, estate, 0);
	MemoryContextSwitchTo(oldcontext);

	Clean2pcShmem->db_count = 0;

	result = ExecRemoteQuery((PlanState *) pstate);

	while (result != NULL && !TupIsNull(result))
	{
		slot_getallattrs(result);

		is_start_from = true;
		count_2pc++;

		for (i = 0; i < attr_num; i++)
		{
			char *value = text_to_cstring(DatumGetTextP(result->tts_values[i]));
			appendStringInfo(result_str, "\t%s: %s", attr_name[i], value);
			switch (i)
			{
			case Query2pcAttr_gid: /* value is gid */
				if (IsXidImplicit(value))
				{
					/* get start node from gid */
					startnode = NULL;

					strcpy(gid, value);
					startnode = strtok(gid, ":");
					if (NULL == startnode)
					{
						elog(WARNING, "get startnode(%s) from gid(%s) failed",
							startnode, gid);
						break;
					}

					startnode = strtok(NULL, ":");
					if (NULL == startnode)
					{
						elog(WARNING, "get startnode(%s) from gid(%s) failed",
							startnode, gid);
						break;
					}

					if (strcmp(startnode, PGXCNodeName) != 0)
					{
						is_start_from = false;
					}
				}
				break;
			case Query2pcAttr_database: /* value is database */
				if (is_start_from)
				{
					db_oid = get_database_oid(value, true);
					if (!OidIsValid(db_oid))
					{
						elog(WARNING, "get database(%s) oid failed", value);
					}
					else if (db_oid != last_db_oid)
					{
						if (Clean2pcShmem->db_count < MAX_DB_SIZE)
						{
							Clean2pcShmem->db_list[Clean2pcShmem->db_count++] = db_oid;
						}
						last_db_oid = db_oid;
						count_db++;
					}
				}
				break;
			default:
				break;
			}
		}

		appendStringInfo(result_str, "\n");

		result = ExecRemoteQuery((PlanState *) pstate);
	}

	ExecEndRemoteQuery(pstate);

	CommitTransactionCommand();

	if (count_2pc > 0)
	{
		Assert(result_str->data != NULL);
		elog(LOG, "query remain 2pc count(%d), db count(%d), sql: %s",
			count_2pc, count_db, query);
		elog(DEBUG1, "remain 2pc:\n%s", result_str->data);
	}
}

static void
do_clean_2pc(TimestampTz clean_time)
{
	int                  i = 0;
	int                  count = 0;
	MemoryContext        oldcontext = NULL;
	char                 query[SQL_CMD_LEN];
	EState              *estate = NULL;
	RemoteQuery         *plan = NULL;
	RemoteQueryState    *pstate = NULL;
	TupleTableSlot      *result = NULL;
	Var                 *dummy = NULL;
	int                  attr_num = 4;
	Oid                  node_oid = 0;
	char                 node_type = PGXC_NODE_COORDINATOR;
	int                  node_index = 0;
	static const char   *attr_name[] = {"gid", "global_transaction_status",
								"operation", "operation_status"};

	Assert(result_str != NULL);
	resetStringInfo(result_str);

	snprintf(query, SQL_CMD_LEN, "select * FROM pg_clean_execute_on_node('%s', %ld)"
			" limit 1000;", PGXCNodeName, clean_time);

	StartTransactionCommand();

	InitMultinodeExecutor(false);

	node_oid = get_pgxc_nodeoid(PGXCNodeName);
	if (!OidIsValid(node_oid))
	{
		elog(ERROR, "get node(%s) oid failed", PGXCNodeName);
		return;
	}
	node_index = PGXCNodeGetNodeId(node_oid, &node_type);

	elog(DEBUG1, "node(%d) query: %s", node_index, query);

	plan = makeNode(RemoteQuery);
	plan->combine_type = COMBINE_TYPE_NONE;
	plan->exec_nodes = makeNode(ExecNodes);
	plan->exec_type = EXEC_ON_COORDS;

	plan->exec_nodes->nodeList = lappend_int(plan->exec_nodes->nodeList, node_index);

	plan->sql_statement = (char*)query;
	plan->force_autocommit = false;

	/*
	* We only need the target entry to determine result data type.
	* So create dummy even if real expression is a function.
	*/
	for (i = 1; i <= attr_num; i++)
	{
		dummy = makeVar(1, i, TEXTOID, 0, InvalidOid, 0);
		plan->scan.plan.targetlist = lappend(plan->scan.plan.targetlist,
										makeTargetEntry((Expr *) dummy, i, NULL, false));
	}

	/* prepare to execute */
	estate = CreateExecutorState();
	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);
	estate->es_snapshot = GetActiveSnapshot();
	pstate = ExecInitRemoteQuery(plan, estate, 0);
	MemoryContextSwitchTo(oldcontext);

	result = ExecRemoteQuery((PlanState *) pstate);

	while (result != NULL && !TupIsNull(result))
	{
		slot_getallattrs(result); 

		count++;

		for (i = 0; i < attr_num; i++)
		{
			char *value = text_to_cstring(DatumGetTextP(result->tts_values[i]));
			appendStringInfo(result_str, "\t%s: %s", attr_name[i], value);
		}

		appendStringInfo(result_str, "\n");

		result = ExecRemoteQuery((PlanState *) pstate);
	}

	ExecEndRemoteQuery(pstate);

	CommitTransactionCommand();

	if (count > 0)
	{
		Assert(NULL != result_str->data);
		elog(LOG, "clean 2pc count(%d), sql: %s", count, query);
		elog(LOG, "clean 2pc:\n%s", result_str->data);
	}
}

/* SIGTERM: set flag to exit normally */
static void
clean_2pc_sigterm_handler(SIGNAL_ARGS)
{
	elog(LOG, "SIGTERM: %d", postgres_signal_arg);
	got_SIGTERM = true;
}


/* SIGHUP: set flag to re-read config file at next convenient time */
static void
clean_2pc_sighup_handler(SIGNAL_ARGS)
{
	elog(LOG, "SIGHUP: %d", postgres_signal_arg);
	got_SIGHUP = true;
}

/* SIGUSR2: used for notify 2pc abnormal */
static void
clean_2pc_sigusr2_handler(SIGNAL_ARGS)
{
	elog(LOG, "SIGUSR2: %d", postgres_signal_arg);
	got_SIGUSR2 = true;
}

/*
 * IsClean2pcLauncher functions
 *		Return whether this is a 2pc clean launcher.
 */
bool
IsClean2pcLauncher(void)
{
	return am_clean_2pc_launcher;
}

/*
 * IsClean2pcWorker functions
 *		Return whether this is a 2pc clean worker.
 */
bool
IsClean2pcWorker(void)
{
	return am_clean_2pc_worker;
}

/*
 * get_database_list
 *		Return a list of all databases found in pg_database.
 *
 * The list and associated data is allocated in the caller's memory context,
 * which is in charge of ensuring that it's properly cleaned up afterwards.
 *
 * Note: this is the only function in which the autovacuum launcher uses a
 * transaction.  Although we aren't attached to any particular database and
 * therefore can't access most catalogs, we do have enough infrastructure
 * to do a seqscan on pg_database.
 */
static List *
get_database_list(void)
{
	List	   *dblist = NIL;
	Relation	rel;
	HeapScanDesc scan;
	HeapTuple	tup;
	MemoryContext resultcxt;

	/* This is the context that we will allocate our output data in */
	resultcxt = CurrentMemoryContext;

	StartTransactionCommand();

	rel = heap_open(DatabaseRelationId, AccessShareLock);
	scan = heap_beginscan_catalog(rel, 0, NULL);

	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
	{
		Form_pg_database pgdatabase = (Form_pg_database) GETSTRUCT(tup);
		Clean2pcDBInfo  *db_info;
		MemoryContext oldcxt;

		oldcxt = MemoryContextSwitchTo(resultcxt);

		db_info = (Clean2pcDBInfo *) palloc(sizeof(Clean2pcDBInfo));

		db_info->db_oid = HeapTupleGetOid(tup);
		db_info->db_name = pstrdup(NameStr(pgdatabase->datname));

		dblist = lappend(dblist, db_info);
		MemoryContextSwitchTo(oldcxt);
	}

	heap_endscan(scan);
	heap_close(rel, AccessShareLock);

	CommitTransactionCommand();

	return dblist;
}

static Oid
get_default_database(void)
{
	Oid default_db = InvalidOid;
	Oid template0_db = InvalidOid;
	Oid template1_db = InvalidOid;
	List *dblist = NULL;
	ListCell *cell = NULL;
	Clean2pcDBInfo *db_info = NULL;
	char *default_db_name = NULL;

	/* Get a list of databases */
	dblist = get_database_list();
	foreach(cell, dblist)
	{
		db_info = lfirst(cell);

		if (strcmp(db_info->db_name, DB_DEFAULT) == 0)
		{
			default_db = db_info->db_oid;
			default_db_name = db_info->db_name;
			break;
		}

		if (strcmp(db_info->db_name, DB_TEMPLATE0) == 0)
		{
			template0_db = db_info->db_oid;
			continue;
		}

		if (strcmp(db_info->db_name, DB_TEMPLATE1) == 0)
		{
			template1_db = db_info->db_oid;
			continue;
		}

		if (!OidIsValid(default_db))
		{
			default_db = db_info->db_oid;
			default_db_name = db_info->db_name;
		}
	}

	if (!OidIsValid(default_db))
	{
		if (OidIsValid(template1_db))
		{
			default_db = template1_db;
			default_db_name = DB_TEMPLATE1;
		} else if (OidIsValid(template0_db))
		{
			default_db = template0_db;
			default_db_name = DB_TEMPLATE0;
		}
	}

	Assert(OidIsValid(default_db));

	elog(DEBUG2, "get default db: oid(%d), name(%s)", default_db, default_db_name);

	return default_db;
}

/*
 * start query worker to query 2pc
 */
static void
start_query_worker(TimestampTz clean_time)
{
	Oid db_oid = get_default_database();
	if (!OidIsValid(db_oid))
	{
		elog(WARNING, "get default database failed");
		return;
	}

	Assert(OidIsValid(db_oid));

	if (auto_clean_2pc_timeout != 0)
	{
		TimestampTz curr_time = GetCurrentTimestamp();
		if (curr_time - clean_time > auto_clean_2pc_timeout * USECS_PER_SEC)
		{
			clean_time = curr_time - auto_clean_2pc_timeout * USECS_PER_SEC;
		}
	}

	LWLockAcquire(Clean2pcLock, LW_EXCLUSIVE);

	Clean2pcShmem->clean_time = clean_time;

	while (Clean2pcShmem->worker_running)
	{
		LWLockRelease(Clean2pcLock);

		if (got_SIGTERM)
		{
			proc_exit(0);
		}

		pg_usleep(1000000L); /* wait 1s */

		elog(LOG, "waiting to db(%d)", Clean2pcShmem->worker_db);

		LWLockAcquire(Clean2pcLock, LW_EXCLUSIVE);
	}

	Clean2pcShmem->worker_running = true;
	Clean2pcShmem->db_count = 0;
	Clean2pcShmem->worker_db = db_oid;

	LWLockRelease(Clean2pcLock);

	SendPostmasterSignal(PMSIGNAL_START_CLEAN_2PC_WORKER);

	pg_usleep(1000000L); /* wait 1s */
}

/*
 * start clean worker to clean 2pc
 */
static void
start_clean_worker(int count)
{
	int i = 0;

	for (i = 0; i < count; i++)
	{
		LWLockAcquire(Clean2pcLock, LW_EXCLUSIVE);

		while (Clean2pcShmem->worker_running)
		{
			LWLockRelease(Clean2pcLock);

			if (got_SIGTERM)
			{
				proc_exit(0);
			}

			pg_usleep(1000000L); /* wait 1s */

			elog(LOG, "waiting to db(%d)", Clean2pcShmem->worker_db);

			LWLockAcquire(Clean2pcLock, LW_EXCLUSIVE);
		}

		Clean2pcShmem->worker_db = Clean2pcShmem->db_list[i];

		if (Clean2pcShmem->db_count != count)
		{
			elog(WARNING, "db_count(%d)!=count(%d)", Clean2pcShmem->db_count, count);
			LWLockRelease(Clean2pcLock);
			break;
		}

		if (!OidIsValid(Clean2pcShmem->worker_db))
		{
			elog(WARNING, "get invalid oid, count: %d, i: %d", count, i);
			LWLockRelease(Clean2pcLock);
			continue;
		}

		Clean2pcShmem->worker_running = true;
		SendPostmasterSignal(PMSIGNAL_START_CLEAN_2PC_WORKER);

		LWLockRelease(Clean2pcLock);

		pg_usleep(1000000L); /* wait 1s */
	}
}

/*
 * on_proc_exit callback to set worker_running to false
 */
static void
ExitCleanRunning(int status, Datum arg)
{
	if (Clean2pcShmem->worker_running)
	{
		Clean2pcShmem->worker_running = false;
		elog(LOG, "2pc clean worker exit abnormally");
	}
	else
	{
		elog(DEBUG5, "2pc clean worker exit normally");
	}
}

/*
 * Clean2pcShmemSize
 *		Compute space needed for clean 2pc related shared memory
 */
Size
Clean2pcShmemSize(void)
{
	Size		size;

	/*
	 * Need the fixed struct and the array of WorkerInfoData.
	 */
	size = sizeof(Clean2pcShmemStruct);
	size = MAXALIGN(size);

	return size;
}

/*
 * Clean2pcShmemInit
 *		Allocate and initialize clean 2pc related  shared memory
 */
void
Clean2pcShmemInit(void)
{
	bool		found;
	Clean2pcShmem = (Clean2pcShmemStruct *) ShmemInitStruct("Clean 2pc Data",
															Clean2pcShmemSize(),
															&found);
}

#ifdef EXEC_BACKEND
/*
 * forkexec routine for the 2pc clean launcher process.
 *
 * Format up the arglist, then fork and exec.
 */
static pid_t
clean_2pc_launcher_forkexec(void)
{
	char	   *av[10];
	int			ac = 0;

	av[ac++] = "postgres";
	av[ac++] = "--forkclean2pclauncher";
	av[ac++] = NULL; /* filled in by postmaster_forkexec */
	av[ac] = NULL;

	Assert(ac < lengthof(av));

	return postmaster_forkexec(ac, av);
}

/*
 * forkexec routine for the 2pc clean worker process.
 *
 * Format up the arglist, then fork and exec.
 */
static pid_t
clean_2pc_worker_forkexec(void)
{
	char	   *av[10];
	int			ac = 0;

	av[ac++] = "postgres";
	av[ac++] = "--forkclean2pcworker";
	av[ac++] = NULL; /* filled in by postmaster_forkexec */
	av[ac] = NULL;

	Assert(ac < lengthof(av));

	return postmaster_forkexec(ac, av);
}

/*
 * We need this set from the outside, before InitProcess is called
 */
void
Clean2pcLauncherIAm(void)
{
	am_clean_2pc_launcher = true;
}

/*
 * We need this set from the outside, before InitProcess is called
 */
void
Clean2pcWorkerIAm(void)
{
	am_clean_2pc_worker = true;
}
#endif
