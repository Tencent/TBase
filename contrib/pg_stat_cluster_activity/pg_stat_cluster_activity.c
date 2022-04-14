#include "postgres.h"

#include "catalog/pg_authid.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "commands/explain.h"
#include "common/ip.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "pgstat.h"
#include "pgxc/execRemote.h"
#include "pgxc/pgxc.h"
#include "pgxc/squeue.h"
#include "port/atomics.h"
#include "storage/ipc.h"
#include "storage/procarray.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/portal.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC;

#define PG_STAT_GET_ClUSTER_ACTIVITY_COLS 22

/* ----------
 * Total number of backends including auxiliary
 *
 * We reserve a slot for each possible BackendId, plus one for each
 * possible auxiliary process type.  (This scheme assumes there is not
 * more than one of any auxiliary process type at a time.) MaxBackends
 * includes autovacuum workers and background workers as well.
 * ----------
 */
#define NumBackendStatSlots (MaxBackends + NUM_AUXPROCTYPES)

#define UINT32_ACCESS_ONCE(var)		 ((uint32)(*((volatile uint32 *)&(var))))

/*
 * PgClusterStatus is something like PgBackendStatus (see pgstat.c) but it
 * contains information that a query executed in a cluster database system.
 * Each PgClusterStatus stands for a backend process forked by postmaster,
 * the same way PgBackendStatus does, like extended fields of PgBackendStatus.
 * We show it in view pg_stat_cluster_activity, still, one tuple for an entry.
 */
typedef struct PgClusterStatus
{
	/*
	 * To avoid locking overhead, we use the following protocol: a backend
	 * increments changecount before modifying its entry, and again after
	 * finishing a modification.  A would-be reader should note the value of
	 * changecount, copy the entry into private memory, then check
	 * changecount again.  If the value hasn't changed, and if it's even,
	 * the copy is valid; otherwise start over.  This makes updates cheap
	 * while reads are potentially expensive, but that's the tradeoff we want.
	 *
	 * The above protocol needs the memory barriers to ensure that the
	 * apparent order of execution is as it desires. Otherwise, for example,
	 * the CPU might rearrange the code so that changecount is incremented
	 * twice before the modification on a machine with weak memory ordering.
	 * This surprising result can lead to bugs.
	 */
	int changecount;
	
	bool valid;                     /* don't show this entry if false */
	
	/* fields that will be shown in pg_stat_cluster_activity */
	char sessionid[NAMEDATALEN];    /* global session id in a cluster, one for a session */
	char nodename[NAMEDATALEN];     /* nodename, determined after process started */
	char role[NAMEDATALEN];         /* coord, datanode, producer or consumer */
	
	/* portal_name or portal_name_unique */
	char sqname[NAMEDATALEN];
	/* true if sharequeue end, but currently change when query ends in this backend */
	bool sqdone;
	/* part of plantree this backend is processing, OR last processed if backend is idle */
	char planstate[4096];
	
	/*
	 * portal name: the name of current portal, given by upper node of processing query 
	 * cursor name: contained in planstate this backend is querying, which would be
	 *              portal name of next layer of nodes bellow this backend
	 *              
	 * Note: with these two fields plus nodename, we can build a backend tree of executing query
	 *       in whole distributed system.
	 */
	char portal[NAMEDATALEN];
	char cursors[NAMEDATALEN * 64];
} PgClusterStatus;

static PgClusterStatus *ClusterStatusArray = NULL;
static PgClusterStatus *MyCSEntry = NULL;

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static pgstat_report_hook_type prev_pgstat_report_hook = NULL;
static PortalStart_hook_type prev_PortalStart = NULL;
static PortalDrop_hook_type prev_PortalDrop = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;

static bool pgcs_enable_planstate; /* whether to show planstate in result sets */

/*
 * Macros to load and store st_changecount with the memory barriers.
 *
 * increment_changecount_before() and
 * increment_changecount_after() need to be called before and after
 * entries are modified, respectively. This makes sure that st_changecount
 * is incremented around the modification.
 *
 * Also save_changecount_before() and save_changecount_after()
 * need to be called before and after entries are copied into private memory
 * respectively.
 */
#define increment_changecount_before(status)	\
	do {	\
		status->changecount++;	\
		pg_write_barrier(); \
	} while (0)

#define increment_changecount_after(status) \
	do {	\
		pg_write_barrier(); \
		status->changecount++;	\
		Assert((status->changecount & 1) == 0); \
	} while (0)

#define save_changecount_before(status, save_changecount)	\
	do {	\
		save_changecount = status->changecount; \
		pg_read_barrier();	\
	} while (0)

#define save_changecount_after(status, save_changecount)	\
	do {	\
		pg_read_barrier();	\
		save_changecount = status->changecount; \
	} while (0)

Datum pg_stat_get_cluster_activity(PG_FUNCTION_ARGS);
Datum pg_signal_session(PG_FUNCTION_ARGS);
Datum pg_terminate_session(PG_FUNCTION_ARGS);
Datum pg_cancel_session(PG_FUNCTION_ARGS);

void _PG_init(void);
void _PG_fini(void);

PG_FUNCTION_INFO_V1(pg_stat_get_cluster_activity);
PG_FUNCTION_INFO_V1(pg_signal_session);
PG_FUNCTION_INFO_V1(pg_terminate_session);
PG_FUNCTION_INFO_V1(pg_cancel_session);


static ParamListInfo
EvaluateSessionIDParam(const char *sessionid)
{
	int num_params = 1;
	ParamListInfo paramLI = (ParamListInfo)
		palloc0(offsetof(ParamListInfoData, params) +
		        num_params * sizeof(ParamExternData));
	
	ParamExternData *prm;
	
	/* we have static list of params, so no hooks needed */
	paramLI->paramFetch = NULL;
	paramLI->paramFetchArg = NULL;
	paramLI->parserSetup = NULL;
	paramLI->parserSetupArg = NULL;
	paramLI->numParams = num_params;
	paramLI->paramMask = NULL;
	
	prm = &paramLI->params[0];
	prm->ptype = TEXTOID;
	prm->pflags = PARAM_FLAG_CONST;
	if (sessionid != NULL)
	{
		prm->value = CStringGetTextDatum(sessionid);
		prm->isnull = false;
	}
	else
	{
		prm->isnull = true;
	}
	
	return paramLI;
}

/*
 * walk through planstate tree and gets cursors it contains in
 * RemoteSubplan node, formed as a single string delimited each
 * cursor by a space (one cursor stands for a RemoteSubplan node).
 */
static bool
cursorCollectWalker(PlanState *planstate, StringInfo str)
{
	if (IsA(planstate, RemoteSubplanState))
	{
		RemoteSubplan *plan = (RemoteSubplan *) planstate->plan;
		if (plan->cursor != NULL)
		{
			appendStringInfoString(str, plan->cursor);
			if (plan->unique)
				appendStringInfo(str, "_"INT64_FORMAT, plan->unique);
			/* add a space as delimiter */
			appendStringInfoString(str, " ");
		}
	}
	
	return planstate_tree_walker(planstate, cursorCollectWalker, str);
}

/*
 * Initialize the shared status array and several string buffers
 * during postmaster startup.
 */
static void
CreateSharedClusterStatus(void)
{
	Size		size;
	bool        found;
	
	/* Create or attach to the shared array */
	size = mul_size(sizeof(PgClusterStatus), NumBackendStatSlots);
	ClusterStatusArray = (PgClusterStatus *)
		ShmemInitStruct("Cluster Status Array", size, &found);
	
	if (!found)
	{
		/*
		 * We're the first - initialize.
		 */
		MemSet(ClusterStatusArray, 0, size);
	}
}

/*
 * Shut down a single backend's statistics reporting at process exit.
 *
 * Flush any remaining statistics counts out to the collector.
 * Without this, operations triggered during backend exit (such as
 * temp table deletions) won't be counted.
 *
 * Lastly, clear out our entry in the PgBackendStatus array.
 */
static void
pgcs_shutdown_hook(int code, Datum arg)
{
	volatile PgClusterStatus *entry = MyCSEntry;
	
	/*
	 * Clear my status entry, following the protocol of bumping st_changecount
	 * before and after.  We use a volatile pointer here to ensure the
	 * compiler doesn't try to get cute.
	 */
	increment_changecount_before(entry);
	
	entry->valid = false;	/* mark invalid to hide this entry */
	
	increment_changecount_after(entry);
}

/* ----------
 * pgcs_entry_initialize() -
 *
 *	Initialize my cluster status entry, and set up our on-proc-exit hook.
 *	as an extension but we don't have hook during process startup, so called
 *	each time the backend try to report something.
 * ----------
 */
static void
pgcs_entry_initialize(void)
{
	/* already initialized */
	if (MyCSEntry != NULL)
		return;

	if (ClusterStatusArray == NULL)
	{
		ereport(ERROR,
		        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			        errmsg("shared memory for pg_stat_cluster_activity is not prepared"),
			        errhint("maybe you need to set shared_preload_libraries in postgresql.conf file")));
		return;
	}
	
	/* Initialize MyCSEntry */
	if (MyBackendId != InvalidBackendId)
	{
		Assert(MyBackendId >= 1 && MyBackendId <= MaxBackends);
		MyCSEntry = &ClusterStatusArray[MyBackendId - 1];
	}
	else
	{
		/* Must be an auxiliary process */
		Assert(MyAuxProcType != NotAnAuxProcess);
		
		/*
		 * Assign the MyCSEntry for an auxiliary process. Since it doesn't
		 * have a BackendId, the slot is statically allocated based on the
		 * auxiliary process type (MyAuxProcType).  Backends use slots indexed
		 * in the range from 1 to MaxBackends (inclusive), so we use
		 * MaxBackends + AuxBackendType + 1 as the index of the slot for an
		 * auxiliary process.
		 */
		MyCSEntry = &ClusterStatusArray[MaxBackends + MyAuxProcType];
	}
	
	/* also set nodename here, it won't change anyway */
	memcpy(MyCSEntry->nodename, PGXCNodeName, strlen(PGXCNodeName) + 1);
	
	/* Set up a process-exit hook to clean up */
	on_shmem_exit(pgcs_shutdown_hook, 0);
}

/* ----------
 * pgcs_report_common
 * 
 *  Report common fileds of cluster backend status activity,
 *  called by pgcs_report_query_activity and pgcs_report_activity.
 * ----------
 */
static void
pgcs_report_common(PgClusterStatus *entry)
{
	strncpy((char *) entry->sessionid, PGXCSessionId, NAMEDATALEN);
	
	entry->sqdone = false;
	entry->valid = true;
}

/* ----------
 * pgcs_report_role
 * 
 *  Report role, sqname, also if this backend become consumer, remove
 *  previous planstate and cursor.
 * ----------
 */
static void
pgcs_report_role(PgClusterStatus *entry, QueryDesc *desc)
{
	/* fields need queryDesc */
	if (IS_PGXC_DATANODE)
	{
		if (desc != NULL && desc->squeue)
		{
			strncpy((char *) entry->sqname, SqueueName(desc->squeue), NAMEDATALEN);
			if (IsSqueueProducer())
			{
				strncpy((char *) entry->role, "producer", NAMEDATALEN);
			}
			else if (IsSqueueConsumer())
			{
				strncpy((char *) entry->role, "consumer", NAMEDATALEN);
				/* consumer does not know of planstate */
				entry->planstate[0] = '\0';
				entry->cursors[0] = '\0';
			}
			else
			{
				/* do not support */
				entry->role[0] = '\0';
			}
		}
		else if (IsParallelWorker())
		{
			strncpy((char *) entry->role, "parallel worker", NAMEDATALEN);
		}
		else
		{
			strncpy((char *) entry->role, "datanode", NAMEDATALEN);
		}
	}
	else if (IS_PGXC_COORDINATOR)
	{
		strncpy((char *) entry->role, "coordinator", NAMEDATALEN);
	}
	else
	{
		/* do not support */
		entry->role[0] = '\0';
	}
}

/* ----------
 * pgcs_report_query_activity
 *
 *  Do nothing but set common field, just enable this cluster entry
 *  to make it visible in the same time as pg_stat_activity. Hooked
 *  in pgstat_report_activity, args are redundant.
 */
static void
pgcs_report_query_activity(BackendState state, const char *cmd_str)
{
	volatile PgClusterStatus *entry;
	
	pgcs_entry_initialize();
	entry = MyCSEntry;
	
	pgcs_report_common((PgClusterStatus *) entry);
	
	if (prev_pgstat_report_hook)
		prev_pgstat_report_hook(state, cmd_str);
}

/* ----------
 * pgcs_report_executor_activity
 * 
 *  Report fileds of per-query referred, hooked as ExecutorStart_hook
 *  report planstate, cursors and common fields.
 * ----------
 */
static void
pgcs_report_executor_activity(QueryDesc *desc, int eflags)
{
	volatile PgClusterStatus *entry;
	StringInfo planstate_str = NULL;
	StringInfo cursors = NULL;
	MemoryContext oldcxt;
	
	if (prev_ExecutorStart)
		prev_ExecutorStart(desc, eflags);
	else
		standard_ExecutorStart(desc, eflags);
	
	pgcs_entry_initialize();
	entry = MyCSEntry;
	
	if (!desc)
		return;
	
	/* if query already done, just report sqdone and return */
	if (desc->already_executed)
	{
		increment_changecount_before(entry);
		entry->sqdone = true;
		increment_changecount_after(entry);
		return;
	}
	
	/*
	 * Make sure we operate in the per-query context, so any cruft will be
	 * discarded later during ExecutorEnd. estate should be set by standard_ExecutorStart.
	 */
	oldcxt = MemoryContextSwitchTo(desc->estate->es_query_cxt);
	
	if (desc->planstate != NULL)
	{
		/* make planstate text tree if enabled */
		if (pgcs_enable_planstate)
		{
			ExplainState *es = NewExplainState();
			
			es->costs = false;
			/* we don't want plan->targetlist been changed */
			es->skip_remote_query = true;
			
			ExplainBeginOutput(es);
			ExplainPrintPlan(es, desc);
			ExplainEndOutput(es);
			/* remove last '\n' */
			if (es->str->len > 1)
				es->str->data[--es->str->len] = '\0';
			planstate_str = es->str;
		}
		else
		{
			planstate_str = makeStringInfo();
			appendStringInfoString(planstate_str, "disabled");
		}
		
		/* find name of RemoteSubplan to show as cursors */
		cursors = makeStringInfo();
		cursorCollectWalker(desc->planstate, cursors);
	}
	
	increment_changecount_before(entry);
	
	if (planstate_str != NULL && planstate_str->len > 0)
		memcpy((char *) entry->planstate, planstate_str->data, Min(planstate_str->len + 1, 4096));
	if (cursors != NULL && cursors->len > 0)
		memcpy((char *) entry->cursors, cursors->data, Min(cursors->len + 1, NAMEDATALEN * 64));
	
	pgcs_report_common((PgClusterStatus *) entry);
	pgcs_report_role((PgClusterStatus *) entry, desc);
	
	increment_changecount_after(entry);
	
	MemoryContextSwitchTo(oldcxt);
}

/* ----------
 * pgcs_report_activity
 * 
 *  Report fileds of per-portal referred, hooked as PortalStart_hook
 *  report portal name and common fields.
 * ----------
 */
static void
pgcs_report_activity(Portal portal)
{
	volatile PgClusterStatus *entry;
	QueryDesc *desc = portal->queryDesc;
	
	pgcs_entry_initialize();
	entry = MyCSEntry;
	
	/* if query already done, just report sqdone and return */
	if (desc != NULL && desc->already_executed)
	{
		increment_changecount_before(entry);
		entry->sqdone = true;
		increment_changecount_after(entry);
		return;
	}
	
	increment_changecount_before(entry);
	
	strncpy((char *) entry->portal, portal->name, NAMEDATALEN);
	pgcs_report_common((PgClusterStatus *) entry);
	pgcs_report_role((PgClusterStatus *) entry, desc);
	
	increment_changecount_after(entry);
}

/* ----------
 * pgstat_fetch_stat_local_csentry
 * 
 *  Given a backend id, find particular cluster status entry, copy valid
 *  entry into local memory, loop around changecount to ensure concurrency.
 * ----------
 */
static PgClusterStatus *
pgstat_fetch_stat_local_csentry(int beid)
{
	PgClusterStatus *csentry;
	PgClusterStatus *local = palloc(sizeof(PgClusterStatus));
	local->valid = false;
	
	if (ClusterStatusArray == NULL)
	{
		ereport(ERROR,
		        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			        errmsg("shared memory for pg_stat_cluster_activity is not prepared"),
			        errhint("maybe you need to set shared_preload_libraries in postgresql.conf")));
		return NULL;
	}
	
	if (beid < 1)
		return NULL;
	
	csentry = &ClusterStatusArray[beid - 1];
	
	for (;;)
	{
		int			before_changecount;
		int			after_changecount;
		
		save_changecount_before(csentry, before_changecount);
		if (csentry->valid)
		{
			memcpy(local, csentry, sizeof(PgClusterStatus));
		}
		save_changecount_after(csentry, after_changecount);
		if (before_changecount == after_changecount &&
		    (before_changecount & 1) == 0)
			break;
		
		/* Make sure we can break out of loop if stuck... */
		CHECK_FOR_INTERRUPTS();
	}
	
	return local;
}

/* ----------
 * pg_stat_get_remote_activity
 * 
 *  Execute pg_stat_get_cluster_activity query remotely and save
 *  results in tuplestore.
 * ----------
 */
static void
pg_stat_get_remote_activity(const char *sessionid, bool coordonly, Tuplestorestate *tupstore, TupleDesc tupdesc)
{
#define QUERY_LEN 1024
	char    query[QUERY_LEN];
	EState              *estate;
	MemoryContext		oldcontext;
	RemoteQuery 		*plan;
	RemoteQueryState    *pstate;
	TupleTableSlot		*result = NULL;
	
	/*
	 * Here we call pg_stat_get_cluster_activity in remote with args:
	 * coordonly = false, localonly = true, to prevent recursive calls in remote nodes.
	 */
	snprintf(query, QUERY_LEN, "select * from pg_stat_get_cluster_activity($1, false, true)");
	
	plan = makeNode(RemoteQuery);
	plan->combine_type = COMBINE_TYPE_NONE;
	/*
	 * set exec_nodes to NULL makes ExecRemoteQuery send query to all nodes
	 * (local CN nodes won't recieved query again).
	 */
	plan->exec_nodes = NULL;
	plan->exec_type = EXEC_ON_ALL_NODES;
	plan->sql_statement = (char *) query;
	plan->force_autocommit = false;
	plan->exec_nodes = makeNode(ExecNodes);
	plan->exec_nodes->missing_ok = true;
	
	if (coordonly)
	{
		plan->exec_nodes->nodeList = GetAllCoordNodes();
		plan->exec_type = EXEC_ON_COORDS;
	}
	
	/* prepare to execute */
	estate = CreateExecutorState();
	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);
	estate->es_snapshot = GetActiveSnapshot();
	estate->es_param_list_info = EvaluateSessionIDParam(sessionid);
	pstate = ExecInitRemoteQuery(plan, estate, 0);
	ExecAssignResultType((PlanState *) pstate, tupdesc);
	MemoryContextSwitchTo(oldcontext);
	
	result = ExecRemoteQuery((PlanState *) pstate);
	
	while (result != NULL && !TupIsNull(result))
	{
		slot_getallattrs(result);
		
		tuplestore_puttupleslot(tupstore, result);
		result = ExecRemoteQuery((PlanState *) pstate);
	}
	
	ExecEndRemoteQuery(pstate);
	FreeExecutorState(estate);
}

/* ----------
 * pg_stat_get_cluster_activity
 * 
 *  Internal SRF function of this extension, access sharememory to find
 *  every live backend which executed or executing query. copy to local
 *  and show status. also we collect some fields from PGBackendStatus
 *
 *  arguments:  sessionid -- global unique id for a session, generated by CN
 *              coordonly -- only dispatch to other cn if true.
 *              localonly -- collect local entries status if true.
 *              
 *  Note: since we also collect PGBackendStatus, get them first and use
 *  backend id to access particular cluster status entry to narrow down
 *  loop search range from all backend slots to localNumBackends (see pgstat.c)
 * ----------
 */
Datum
pg_stat_get_cluster_activity(PG_FUNCTION_ARGS)
{
	int              num_backends = pgstat_fetch_stat_numbackends();
	int			     curr_backend;
	bool             with_sessionid = !PG_ARGISNULL(0);
	bool             coordonly = PG_ARGISNULL(1) ? false : PG_GETARG_BOOL(1);
	bool             localonly = PG_ARGISNULL(2) ? false : PG_GETARG_BOOL(2);
	const char      *sessionid = with_sessionid ? text_to_cstring(PG_GETARG_TEXT_P(0)) : NULL;
	ReturnSetInfo   *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	     tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext    per_query_ctx;
	MemoryContext    oldcontext;
	
	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
		        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			        errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
		        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			        errmsg("materialize mode required, but it is not " \
						"allowed in this context")));
	
	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	
	/* switch to query's memory context to save results during execution */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);
	
	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;
	
	MemoryContextSwitchTo(oldcontext);
	
	/* dispatch query to remote if needed */
	if (!localonly && IS_PGXC_COORDINATOR)
		pg_stat_get_remote_activity(sessionid, coordonly, tupstore, tupdesc);
	
	/* 1-based index */
	for (curr_backend = 1; curr_backend <= num_backends; curr_backend++)
	{
		/* for each row */
		Datum		values[PG_STAT_GET_ClUSTER_ACTIVITY_COLS];
		bool		nulls[PG_STAT_GET_ClUSTER_ACTIVITY_COLS];
		
		/* same as pg_stat_get_activity */
		LocalPgBackendStatus *local_beentry;
		PgBackendStatus *beentry;
		PGPROC	   *proc;
		const char *wait_event_type = NULL;
		const char *wait_event = NULL;
		
		/* cluster information */
		PgClusterStatus *local_csentry;
		
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));
		
		/* Get the next one in the list */
		local_beentry = pgstat_fetch_stat_local_beentry(curr_backend);
		local_csentry = pgstat_fetch_stat_local_csentry(local_beentry->backend_id);
		if (!local_beentry || !local_csentry)
		{
			int			i;
			
			/* Ignore missing entries if looking for specific sessionid */
			if (with_sessionid)
				continue;
			
			for (i = 0; i < lengthof(nulls); i++)
				nulls[i] = true;
			
			nulls[13] = false;
			values[13] = CStringGetTextDatum("<backend information not available>");
			
			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
			continue;
		}
		
		if (!local_csentry->valid)
			continue;
		
		beentry = &local_beentry->backendStatus;
		/* If looking for specific sessionid, ignore all the others */
		if (with_sessionid && strcmp(sessionid, local_csentry->sessionid) != 0)
			continue;
		
		/* Values available to all callers */
		values[0] = CStringGetTextDatum(local_csentry->sessionid);
		values[1] = Int32GetDatum(beentry->st_procpid);
		
		if (beentry->st_databaseid != InvalidOid)
		{
			char *dbname = get_database_name(beentry->st_databaseid);
			if (dbname != NULL)
				values[7] = CStringGetTextDatum(dbname);
			else
				nulls[7] = true;
		}
		else
			nulls[7] = true;
		
		if (beentry->st_userid != InvalidOid)
		{
			char *usename = GetUserNameFromId(beentry->st_userid, true);
			if (usename != NULL)
				values[8] = CStringGetTextDatum(usename);
			else
				nulls[8] = true;
		}
		else
			nulls[8] = true;
		
		/* Values only available to owner or superuser or pg_read_all_stats */
		if (has_privs_of_role(GetUserId(), beentry->st_userid) ||
		    is_member_of_role(GetUserId(), DEFAULT_ROLE_READ_ALL_STATS))
		{
			SockAddr	zero_clientaddr;
			
			/* A zeroed client addr means we don't know */
			memset(&zero_clientaddr, 0, sizeof(zero_clientaddr));
			if (memcmp(&(beentry->st_clientaddr), &zero_clientaddr,
			           sizeof(zero_clientaddr)) == 0)
			{
				nulls[2] = true;
				nulls[3] = true;
				nulls[4] = true;
			}
			else
			{
				if (beentry->st_clientaddr.addr.ss_family == AF_INET
#ifdef HAVE_IPV6
				    || beentry->st_clientaddr.addr.ss_family == AF_INET6
#endif
					)
				{
					char		remote_host[NI_MAXHOST];
					char		remote_port[NI_MAXSERV];
					int			ret;
					
					remote_host[0] = '\0';
					remote_port[0] = '\0';
					ret = pg_getnameinfo_all(&beentry->st_clientaddr.addr,
					                         beentry->st_clientaddr.salen,
					                         remote_host, sizeof(remote_host),
					                         remote_port, sizeof(remote_port),
					                         NI_NUMERICHOST | NI_NUMERICSERV);
					if (ret == 0)
					{
						clean_ipv6_addr(beentry->st_clientaddr.addr.ss_family, remote_host);
						values[2] = DirectFunctionCall1(inet_in,
						                                CStringGetDatum(remote_host));
						if (beentry->st_clienthostname &&
						    beentry->st_clienthostname[0])
							values[3] = CStringGetTextDatum(beentry->st_clienthostname);
						else
							nulls[3] = true;
						values[4] = Int32GetDatum(atoi(remote_port));
					}
					else
					{
						nulls[2] = true;
						nulls[3] = true;
						nulls[4] = true;
					}
				}
				else if (beentry->st_clientaddr.addr.ss_family == AF_UNIX)
				{
					/*
					 * Unix sockets always reports NULL for host and -1 for
					 * port, so it's possible to tell the difference to
					 * connections we have no permissions to view, or with
					 * errors.
					 */
					nulls[2] = true;
					nulls[3] = true;
					values[4] = DatumGetInt32(-1);
				}
				else
				{
					/* Unknown address type, should never happen */
					nulls[2] = true;
					nulls[3] = true;
					nulls[4] = true;
				}
			}
			
			values[5] = CStringGetTextDatum(local_csentry->nodename);
			values[6] = CStringGetTextDatum(local_csentry->role);
			
			proc = BackendPidGetProc(beentry->st_procpid);
			if (proc != NULL)
			{
				uint32		raw_wait_event;
				
				raw_wait_event = UINT32_ACCESS_ONCE(proc->wait_event_info);
				wait_event_type = pgstat_get_wait_event_type(raw_wait_event);
				wait_event = pgstat_get_wait_event(raw_wait_event);
			}
			else if (beentry->st_backendType != B_BACKEND)
			{
				/*
				 * For an auxiliary process, retrieve process info from
				 * AuxiliaryProcs stored in shared-memory.
				 */
				proc = AuxiliaryPidGetProc(beentry->st_procpid);
				
				if (proc != NULL)
				{
					uint32		raw_wait_event;
					
					raw_wait_event =
						UINT32_ACCESS_ONCE(proc->wait_event_info);
					wait_event_type =
						pgstat_get_wait_event_type(raw_wait_event);
					wait_event = pgstat_get_wait_event(raw_wait_event);
				}
			}
			
			if (wait_event_type)
				values[9] = CStringGetTextDatum(wait_event_type);
			else
				nulls[9] = true;
			
			if (wait_event)
				values[10] = CStringGetTextDatum(wait_event);
			else
				nulls[10] = true;
			
			switch (beentry->st_state)
			{
				case STATE_IDLE:
					values[11] = CStringGetTextDatum("idle");
					break;
				case STATE_RUNNING:
					values[11] = CStringGetTextDatum("active");
					break;
				case STATE_IDLEINTRANSACTION:
					values[11] = CStringGetTextDatum("idle in transaction");
					break;
				case STATE_FASTPATH:
					values[11] = CStringGetTextDatum("fastpath function call");
					break;
				case STATE_IDLEINTRANSACTION_ABORTED:
					values[11] = CStringGetTextDatum("idle in transaction (aborted)");
					break;
				case STATE_DISABLED:
					values[11] = CStringGetTextDatum("disabled");
					break;
				case STATE_UNDEFINED:
					nulls[11] = true;
					break;
			}
			
			values[12] = CStringGetTextDatum(local_csentry->sqname);
			values[13] = BoolGetDatum(local_csentry->sqdone);
			values[14] = CStringGetTextDatum(beentry->st_activity);
			values[15] = CStringGetTextDatum(local_csentry->planstate);
			values[16] = CStringGetTextDatum(local_csentry->portal);
			values[17] = CStringGetTextDatum(local_csentry->cursors);
			
			if (beentry->st_proc_start_timestamp != 0)
				values[18] = TimestampTzGetDatum(beentry->st_proc_start_timestamp);
			else
				nulls[18] = true;
			
			if (beentry->st_xact_start_timestamp != 0)
				values[19] = TimestampTzGetDatum(beentry->st_xact_start_timestamp);
			else
				nulls[19] = true;
			
			if (beentry->st_activity_start_timestamp != 0)
				values[20] = TimestampTzGetDatum(beentry->st_activity_start_timestamp);
			else
				nulls[20] = true;
			
			if (beentry->st_state_start_timestamp != 0)
				values[21] = TimestampTzGetDatum(beentry->st_state_start_timestamp);
			else
				nulls[21] = true;
		}
		else
		{
			values[14] = CStringGetTextDatum("<insufficient privilege>");
			nulls[2] = true;
			nulls[3] = true;
			nulls[4] = true;
			nulls[5] = true;
			nulls[6] = true;
			nulls[9] = true;
			nulls[10] = true;
			nulls[11] = true;
			nulls[12] = true;
			nulls[13] = true;
			nulls[15] = true;
			nulls[16] = true;
			nulls[17] = true;
			nulls[18] = true;
			nulls[19] = true;
			nulls[20] = true;
			nulls[21] = true;
		}
		
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}
	
	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);
	
	return (Datum) 0;
}

static bool
pgcs_signal_session_remote(const char *sessionid, int signal)
{
#define QUERY_LEN 1024
	char    query[QUERY_LEN];
	EState              *estate;
	MemoryContext		oldcontext;
	RemoteQuery 		*plan;
	RemoteQueryState    *pstate;
	Var 				*dummy;
	TupleTableSlot		*result = NULL;
	
	snprintf(query, QUERY_LEN, "select pg_signal_session($1, %d, true)", signal);
	
	plan = makeNode(RemoteQuery);
	plan->combine_type = COMBINE_TYPE_NONE;
	/*
	 * set exec_nodes to NULL makes ExecRemoteQuery send query to all nodes
	 * (local CN nodes won't recieved query again).
	 */
	plan->exec_nodes = NULL;
	plan->exec_type = EXEC_ON_ALL_NODES;
	plan->sql_statement = (char *) query;
	plan->force_autocommit = false;
	
	/*
	 * We only need the target entry to determine result data type.
	 * So create dummy even if real expression is a function.
	 */
	dummy = makeVar(1, 1, TEXTOID, 0, InvalidOid, 0);
	plan->scan.plan.targetlist = lappend(plan->scan.plan.targetlist,
	                                     makeTargetEntry((Expr *) dummy, 1, NULL, false));
	
	/* prepare to execute */
	estate = CreateExecutorState();
	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);
	estate->es_snapshot = GetActiveSnapshot();
	estate->es_param_list_info = EvaluateSessionIDParam(sessionid);
	pstate = ExecInitRemoteQuery(plan, estate, 0);
	MemoryContextSwitchTo(oldcontext);
	
	result = ExecRemoteQuery((PlanState *) pstate);
	ExecEndRemoteQuery(pstate);
	if (TupIsNull(result))
	{
		elog(ERROR, "result of pg_signal_session executed remotely is NULL");
		return false;
	}
	
	FreeExecutorState(estate);
	return true;
}

static bool
pgcs_signal_session(const char *sessionid, int signal)
{
	int         num_backends = pgstat_fetch_stat_numbackends();
	int         curr_backend;
	const char *funcname;
	LocalPgBackendStatus *local_beentry;
	PgClusterStatus      *local_csentry;
	PgBackendStatus      *beentry;
	
	if (signal == SIGTERM)
		funcname = "pg_terminate_backend";
	else if (signal == SIGINT)
		funcname = "pg_cancel_backend";
	else
		elog(ERROR, "pgcs_signal_session only support SIGTERM and SIGINT, not %d", signal);
	
	/* 1-based index */
	for (curr_backend = 1; curr_backend <= num_backends; curr_backend++)
	{
		/* Get the next one in the list */
		local_beentry = pgstat_fetch_stat_local_beentry(curr_backend);
		local_csentry = pgstat_fetch_stat_local_csentry(local_beentry->backend_id);
		
		if (local_csentry->valid && strcmp(local_csentry->sessionid, sessionid) == 0)
		{
			beentry = &local_beentry->backendStatus;
			OidFunctionCall1(fmgr_internal_function(funcname),
			                 Int32GetDatum(beentry->st_procpid));
		}
	}
	
	return true;
}

Datum
pg_signal_session(PG_FUNCTION_ARGS)
{
	const char *sessionid = text_to_cstring(PG_GETARG_TEXT_P(0));
	int         signal = PG_GETARG_INT32(1);
	bool        localonly = PG_ARGISNULL(2) ? false : PG_GETARG_BOOL(2);
	bool        result;
	
	result = pgcs_signal_session(sessionid, signal);
	if (result && !localonly)
		result = pgcs_signal_session_remote(sessionid, signal);
	
	return BoolGetDatum(result);
}

Datum
pg_terminate_session(PG_FUNCTION_ARGS)
{
	return DirectFunctionCall3(pg_signal_session,
	                           PG_GETARG_DATUM(0),
	                           Int32GetDatum(SIGTERM),
	                           BoolGetDatum(false));
}

Datum
pg_cancel_session(PG_FUNCTION_ARGS)
{
	return DirectFunctionCall3(pg_signal_session,
	                           PG_GETARG_DATUM(0),
	                           Int32GetDatum(SIGINT),
	                           BoolGetDatum(false));
}

/*
 * Hooked as shmem_startup_hook
 */
static void
pgcs_shmem_startup(void)
{
	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();
	
	CreateSharedClusterStatus();
}

/*
 * Estimate shared memory space needed.
 */
static Size
pgcs_memsize(void)
{
	return mul_size(sizeof(PgClusterStatus), NumBackendStatSlots);
}

/*
 * Module load callback
 */
void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;
	
	/*
	 * Define (or redefine) custom GUC variables.
	 */
	DefineCustomBoolVariable("pg_stat_cluster_activity.enable_planstate",
	                         "whether to show planstate in result sets.",
	                         NULL,
	                         &pgcs_enable_planstate,
	                         true,
	                         PGC_SUSET,
	                         0,
	                         NULL,
	                         NULL,
	                         NULL);
	
	/*
	 * Request additional shared resources.  (These are no-ops if we're not in
	 * the postmaster process.)  We'll allocate or attach to the shared
	 * resources in pgcs_shmem_startup().
	 */
	RequestAddinShmemSpace(pgcs_memsize());
	
	/*
	 * Install hooks.
	 */
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pgcs_shmem_startup;
	prev_pgstat_report_hook = pgstat_report_hook;
	pgstat_report_hook = pgcs_report_query_activity;
	prev_PortalStart = PortalStart_hook;
	PortalStart_hook = pgcs_report_activity;
	prev_PortalDrop = PortalDrop_hook;
	PortalDrop_hook = pgcs_report_activity;
	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = pgcs_report_executor_activity;
}

/*
 * Module unload callback
 */
void
_PG_fini(void)
{
	/* Uninstall hooks. */
	shmem_startup_hook = prev_shmem_startup_hook;
	pgstat_report_hook = prev_pgstat_report_hook;
	PortalStart_hook = prev_PortalStart;
	PortalDrop_hook = prev_PortalDrop;
	ExecutorStart_hook = prev_ExecutorStart;
}
