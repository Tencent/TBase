#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "catalog/namespace.h"
#include "utils/timestamp.h"
#include "utils/varlena.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/memutils.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#define LINUX_PAGE_SIZE 4096
#define	MAX_MEMORY_DETAIL	2048

typedef struct
{
	char 	*memory_context_name;
	int		level;
	char 	*parent_name;
	int		parent_index;
	long	self_total_space;
	long	self_free_space;
	long	all_total_space;
	long	all_free_space;
} MemoryContextDetail;

typedef struct
{
	int	current;
	int length;
	MemoryContextDetail	details[MAX_MEMORY_DETAIL];
} SessionMemoryContexts;

int get_memory_detail(MemoryContext mctx,
	MemoryContext parent,
	int level,
	int ind_on_parent,
	const int ind_on_stat,
	SessionMemoryContexts *contexts);


/*
 * pg_node_memory_detail
 *
 * node  memory detail
 */
PG_FUNCTION_INFO_V1(pg_node_memory_detail);

Datum
pg_node_memory_detail(PG_FUNCTION_ARGS)
{
	FuncCallContext *fctx;

	if (!superuser())
	{
		ereport(ERROR,
			(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
			(errmsg("must be superuser to use memory functions"))));
	}

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext mctx;
		TupleDesc tupdesc;

		fctx = SRF_FIRSTCALL_INIT();
		mctx = MemoryContextSwitchTo(fctx->multi_call_memory_ctx);

		/* Build a tuple descriptor for our result type */
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		{
			elog(ERROR, "return type must be a row type");
		}

		fctx->max_calls = 1;
		fctx->tuple_desc = tupdesc;

		MemoryContextSwitchTo(mctx);
	}

	fctx = SRF_PERCALL_SETUP();

	if (fctx->call_cntr < fctx->max_calls)
	{
		HeapTuple	resultTuple;
		Datum	result;
		Datum	values[4];
		bool	nulls[4];
		int64	size		= 0;
		Size	totalPages	= 0;
		Size	rssPages	= 0;
		Size	sharePages	= 0;
		char	file[MAXPGPATH]   = {0};
		char	buf[MAXPGPATH] = {0};
		FILE	*handle       = NULL;

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		values[0] = CStringGetTextDatum(PGXCNodeName);
		values[1] = Int64GetDatum(MyProcPid);
		values[2] = CStringGetTextDatum("process_used_memory");

		snprintf(file, MAXPGPATH, "/proc/%d/statm", MyProcPid);
		handle = fopen(file, "r");
		if (handle != NULL && fgets(buf, MAXPGPATH, handle) > 0)
		{
			if (3 == sscanf(buf, "%lu %lu %lu", &totalPages, &rssPages, &sharePages))
			{
				size = ((rssPages - sharePages) * LINUX_PAGE_SIZE) / 1024;
			}
		}
		values[3] = Int64GetDatum(size);

		/* Build and return the result tuple. */
		resultTuple = heap_form_tuple(fctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(resultTuple);

		SRF_RETURN_NEXT(fctx, result);
	}
	else
	{
		SRF_RETURN_DONE(fctx);
	}
}

/*
 * pg_session_memory_detail
 *
 * session memory detail
 */
PG_FUNCTION_INFO_V1(pg_session_memory_detail);

Datum
pg_session_memory_detail(PG_FUNCTION_ARGS)
{
	FuncCallContext *fctx;

	if (!superuser())
	{
		ereport(ERROR,
			(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
			(errmsg("must be superuser to use memory functions"))));
	}

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext mctx;
		TupleDesc tupdesc;
		SessionMemoryContexts *contexts;

		fctx = SRF_FIRSTCALL_INIT();
		mctx = MemoryContextSwitchTo(fctx->multi_call_memory_ctx);

		/* Build a tuple descriptor for our result type */
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		{
			elog(ERROR, "return type must be a row type");
		}
		fctx->tuple_desc = tupdesc;

		contexts = (SessionMemoryContexts *) palloc0(sizeof(SessionMemoryContexts));
		contexts->current = 0;
		contexts->length = 0;
		contexts->details[0].memory_context_name = pstrdup("TopMemoryContext");
		contexts->details[0].level = 0;
		contexts->details[0].parent_name = NULL;
		contexts->details[0].parent_index = 0;
		(void) get_memory_detail(TopMemoryContext, NULL, 0, 0, 0, contexts);

		fctx->user_fctx = contexts;
		fctx->max_calls = contexts->length;

		MemoryContextSwitchTo(mctx);
	}

	fctx = SRF_PERCALL_SETUP();

	if (fctx->call_cntr < fctx->max_calls)
	{
		HeapTuple	resultTuple;
		Datum	result;
		Datum	values[5];
		bool	nulls[5];
		SessionMemoryContexts *contexts = fctx->user_fctx;
		MemoryContextDetail *detail = &contexts->details[fctx->call_cntr];

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		values[0] = CStringGetTextDatum(detail->memory_context_name);
		values[1] = Int64GetDatum(detail->level);
		if (detail->parent_name == NULL)
		{
			nulls[2] = true;
		}
		else
		{
			values[2] = CStringGetTextDatum(detail->parent_name);
		}
		values[3] = Int64GetDatum(detail->all_total_space);
		values[3] = Int64GetDatum(detail->all_free_space);

		/* Build and return the result tuple. */
		resultTuple = heap_form_tuple(fctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(resultTuple);

		SRF_RETURN_NEXT(fctx, result);
	}
	else
	{
		SRF_RETURN_DONE(fctx);
	}
}

/*
 * get memory details of self and children.
 */
int
get_memory_detail(MemoryContext mctx,
	MemoryContext parent,
	int level,
	int ind_on_parent,
	const int ind_on_stat,
	SessionMemoryContexts *contexts)
{
	MemoryContext	iter;
	int				child_index = 0;
	int				itr_indx_on_stat = 0;
	int 			next_ind_on_stat = 0;
	MemoryContextDetail *stat = NULL;

	if (ind_on_stat >= MAX_MEMORY_DETAIL)
	{
		elog(WARNING, "too many memory contexts!");
		return ind_on_stat;
	}

	stat = &contexts->details[ind_on_stat];
	stat->memory_context_name = pstrdup(mctx->name);
	stat->parent_name = parent ? pstrdup(parent->name) : NULL;
	stat->parent_index = ind_on_parent;
	stat->level = level;
	stat->self_free_space = -1;
	stat->self_total_space = -1;
	if (IsA(mctx,AllocSetContext))
	{
		AllocSetStats_Output(mctx, &stat->self_total_space, &stat->self_free_space);
		stat->all_free_space = stat->self_free_space;
		stat->all_total_space = stat->self_total_space;
	}

	itr_indx_on_stat = ind_on_stat + 1;
	contexts->length += 1;
	child_index = 0;
	iter = mctx->firstchild;
	while (iter)
	{
		next_ind_on_stat = get_memory_detail(iter, mctx, level+1, child_index, itr_indx_on_stat, contexts);
		iter = iter->nextchild;

		stat->all_free_space += contexts->details[itr_indx_on_stat].all_free_space;
		stat->all_total_space += contexts->details[itr_indx_on_stat].all_total_space;

		itr_indx_on_stat = next_ind_on_stat;

		child_index++;
	}

	return itr_indx_on_stat;
}
