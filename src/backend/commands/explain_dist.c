/*-------------------------------------------------------------------------
 *
 * explain_dist.c
 *    This code provides support for distributed explain analyze.
 *
 * Portions Copyright (c) 2020, Tencent TBase-C Group
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *        src/backend/commands/explain_dist.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "commands/explain_dist.h"
#include "executor/hashjoin.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "nodes/nodeFuncs.h"
#include "utils/lsyscache.h"
#include "utils/tuplesort.h"

/* Read instrument field */
#define INSTR_READ_FIELD(fldname)                \
do {                                             \
    instr->fldname = strtod(tmp_head, &tmp_pos); \
    tmp_head = tmp_pos + 1;                      \
} while(0)

/* Set max instrument */
#define INSTR_MAX_FIELD(fldname)                          \
do {                                                      \
    target->fldname = Max(src->fldname, target->fldname); \
} while(0)

/* Tools for max/min */
#define SET_MIN_MAX(min, max, tmp) \
do {                               \
    if (min > tmp)                 \
        min = tmp;                 \
    if (max < tmp)                 \
        max = tmp;                 \
} while(0)

/* Serialize state */
typedef struct
{
	/* ids of plan nodes we've handled */
	Bitmapset  *printed_nodes;
	/* send str buf */
	StringInfoData buf;
} SerializeState;

/*
 * InstrOut
 *
 * Serialize Instrumentation structure with the format
 * "nodetype-plan_node_id-node_oid{val,val,...,val}".
 *
 * NOTE: The function should be modified if the structure of Instrumentation
 * or its relevant members has been changed.
 */
static void
InstrOut(StringInfo buf, Plan *plan, Instrumentation *instr, int current_node_id)
{
	/* nodeTag for varify */
	appendStringInfo(buf, "%hd-%d-%d{", nodeTag(plan), plan->plan_node_id, current_node_id);
	
	/* bool */
	/* running should be false after InstrEndLoop */
	appendStringInfo(buf, "%hd,", instr->need_timer);
	appendStringInfo(buf, "%hd,", instr->need_bufusage);
	appendStringInfo(buf, "%hd,", instr->running);
	/* instr_time */
	/* starttime and counter should be 0 after InstrEndLoop */
	appendStringInfo(buf, "%ld,", instr->starttime.tv_sec);
	appendStringInfo(buf, "%ld,", instr->starttime.tv_nsec);
	appendStringInfo(buf, "%ld,", instr->counter.tv_sec);
	appendStringInfo(buf, "%ld,", instr->counter.tv_nsec);
	/* double */
	/* firsttuple and tuplecount should be 0 after InstrEndLoop */
	appendStringInfo(buf, "%.0f,", instr->firsttuple);
	appendStringInfo(buf, "%.0f,", instr->tuplecount);
	/* BufferUsage */
	appendStringInfo(buf, "%ld,", instr->bufusage_start.shared_blks_hit);
	appendStringInfo(buf, "%ld,", instr->bufusage_start.shared_blks_read);
	appendStringInfo(buf, "%ld,", instr->bufusage_start.shared_blks_dirtied);
	appendStringInfo(buf, "%ld,", instr->bufusage_start.shared_blks_written);
	appendStringInfo(buf, "%ld,", instr->bufusage_start.local_blks_hit);
	appendStringInfo(buf, "%ld,", instr->bufusage_start.local_blks_read);
	appendStringInfo(buf, "%ld,", instr->bufusage_start.local_blks_dirtied);
	appendStringInfo(buf, "%ld,", instr->bufusage_start.local_blks_written);
	appendStringInfo(buf, "%ld,", instr->bufusage_start.temp_blks_read);
	appendStringInfo(buf, "%ld,", instr->bufusage_start.temp_blks_written);
	appendStringInfo(buf, "%ld,", instr->bufusage_start.blk_read_time.tv_sec);
	appendStringInfo(buf, "%ld,", instr->bufusage_start.blk_read_time.tv_nsec);
	appendStringInfo(buf, "%ld,", instr->bufusage_start.blk_write_time.tv_sec);
	appendStringInfo(buf, "%ld,", instr->bufusage_start.blk_write_time.tv_nsec);
	/* double */
	appendStringInfo(buf, "%.10f,", instr->startup);
	appendStringInfo(buf, "%.10f,", instr->total);
	appendStringInfo(buf, "%.0f,", instr->ntuples);
	appendStringInfo(buf, "%.0f,", instr->nloops);
	appendStringInfo(buf, "%.0f,", instr->nfiltered1);
	appendStringInfo(buf, "%.0f,", instr->nfiltered2);
	/* BufferUsage */
	appendStringInfo(buf, "%ld,", instr->bufusage.shared_blks_hit);
	appendStringInfo(buf, "%ld,", instr->bufusage.shared_blks_read);
	appendStringInfo(buf, "%ld,", instr->bufusage.shared_blks_dirtied);
	appendStringInfo(buf, "%ld,", instr->bufusage.shared_blks_written);
	appendStringInfo(buf, "%ld,", instr->bufusage.local_blks_hit);
	appendStringInfo(buf, "%ld,", instr->bufusage.local_blks_read);
	appendStringInfo(buf, "%ld,", instr->bufusage.local_blks_dirtied);
	appendStringInfo(buf, "%ld,", instr->bufusage.local_blks_written);
	appendStringInfo(buf, "%ld,", instr->bufusage.temp_blks_read);
	appendStringInfo(buf, "%ld,", instr->bufusage.temp_blks_written);
	appendStringInfo(buf, "%ld,", instr->bufusage.blk_read_time.tv_sec);
	appendStringInfo(buf, "%ld,", instr->bufusage.blk_read_time.tv_nsec);
	appendStringInfo(buf, "%ld,", instr->bufusage.blk_write_time.tv_sec);
	appendStringInfo(buf, "%ld}", instr->bufusage.blk_write_time.tv_nsec);
	
	elog(DEBUG1, "InstrOut: plan_node_id %d, node %d, nloops %.0f", plan->plan_node_id, current_node_id, instr->nloops);
}

#if 0
/*
 * WorkerInstrOut
 *
 * Serialize worker instrumentation with the format
 * "n|val,val,..,val|...|val,val,..,val|". n indicates the worker num,
 * and | separates each worker instrumentation.
 */
static void
WorkerInstrOut(StringInfo buf, WorkerInstrumentation *worker_instr)
{
	int n;
	
	if (worker_instr == NULL)
	{
		appendStringInfo(buf, "0|");
		return;
	}
	
	appendStringInfo(buf, "%d|", worker_instr->num_workers);
	for (n = 0; n < worker_instr->num_workers; n++)
	{
		Instrumentation *instr = &worker_instr->instrument[n];
		
		if (instr->nloops <= 0)
			appendStringInfo(buf, "0,0,0,0|");
		else
			/* send startup, total, ntuples, loops for now */
			appendStringInfo(buf, "%.10f,%.10f,%.0f,%.0f|",
			                 instr->startup, instr->total, instr->ntuples, instr->nloops);
	}
}
#endif

/*
 * SpecInstrOut
 *
 * Serialize specific information in planstate with the format
 * "1/0<val,val,...,val>", and 1/0 indicates if values are valid or not.
 *
 * NOTE: The function should be modified if the corresponding data structure
 * has been changed.
 * The function is VERY related to show_sort_info, show_hash_info.
 */
static void
SpecInstrOut(StringInfo buf, NodeTag plantag, PlanState *planstate)
{
	switch(plantag)
	{
		case T_Gather:
		{
			appendStringInfo(buf, "%d>",
			                 ((GatherState *) planstate)->nworkers_launched);
		}
			break;
		
		case T_GatherMerge:
		{
			appendStringInfo(buf, "%d>",
			                 ((GatherMergeState *) planstate)->nworkers_launched);
		}
			break;
		case T_Sort:
		{
			/* according to RemoteSortState and show_sort_info */
			SortState *sortstate = castNode(SortState, planstate);
			
			if (sortstate->sort_Done && sortstate->tuplesortstate)
			{
				Tuplesortstate *state = (Tuplesortstate *) sortstate->tuplesortstate;
				TuplesortInstrumentation stats;
				tuplesort_get_stats(state, &stats);
				Assert(stats.sortMethod != SORT_TYPE_STILL_IN_PROGRESS);
				appendStringInfo(buf, "1<%hd,%hd,%ld>",
				                 stats.sortMethod, stats.spaceType, stats.spaceUsed);
			}
			else if (sortstate->instrument.sortMethod != -1)
			{
				Assert(sortstate->instrument.sortMethod != SORT_TYPE_STILL_IN_PROGRESS);
				Assert(sortstate->instrument.spaceType != -1);
				appendStringInfo(buf, "1<%hd,%hd,%ld>",
				                 sortstate->instrument.sortMethod,
				                 sortstate->instrument.spaceType,
				                 sortstate->instrument.spaceUsed);
			}
			else
			{
				appendStringInfo(buf, "0>");
			}
			
			if (sortstate->shared_info)
			{
				int n;
				appendStringInfo(buf, "%d>", sortstate->shared_info->num_workers);
				for (n = 0; n < sortstate->shared_info->num_workers; n++)
				{
					TuplesortInstrumentation *w_stats;
					w_stats = &sortstate->shared_info->sinstrument[n];
					if (w_stats->sortMethod == SORT_TYPE_STILL_IN_PROGRESS)
					{
						appendStringInfo(buf, "0>");
					}
					else
						appendStringInfo(buf, "%hd,%hd,%ld>",
						                 w_stats->sortMethod,
						                 w_stats->spaceType, w_stats->spaceUsed);
					elog(DEBUG1, "send out parallel sort %d info: %d %d %ld",
					     planstate->plan->plan_node_id,
					     w_stats->sortMethod,
					     w_stats->spaceType,
					     w_stats->spaceUsed);
				}
			}
			else
				appendStringInfo(buf, "0>");
		}
			break;
		case T_Hash:
		{
			/* according to show_hash_info */
			HashState *hashstate = castNode(HashState, planstate);
			HashJoinTable hashtable = hashstate->hashtable;
			
			int     nbuckets = 0;
			int     nbuckets_original = 0;
			int     nbatch = 0;
			int     nbatch_original = 0;
			Size    spacePeak = 0;
			bool    valid = true;
			
			if (hashtable)
			{
				nbuckets = hashtable->nbuckets;
				nbuckets_original = hashtable->nbuckets_original;
				nbatch = hashtable->nbatch;
				nbatch_original = hashtable->nbatch_original;
				spacePeak = hashtable->spacePeak;
			}
			else if (hashstate->shared_info)
			{
				int n;
				for (n = 0; n < hashstate->shared_info->num_workers; n++)
				{
					HashInstrumentation *w_stats = &hashstate->shared_info->hinstrument[n];
					/* Find the first worker that built a hash table. same logic in show_hash_info */
					if (w_stats->nbatch > 0)
					{
						nbuckets = w_stats->nbuckets;
						nbuckets_original = w_stats->nbuckets_original;
						nbatch = w_stats->nbatch;
						nbatch_original = w_stats->nbatch_original;
						spacePeak = w_stats->space_peak;
						break;
					}
				}
			}
			else
			{
				Assert(hashstate->hinstrument == NULL);
				valid = false;
			}
			
			if (valid)
			{
				elog(DEBUG1, "send out hash %d peak %zu", planstate->plan->plan_node_id,
				     spacePeak);
				appendStringInfo(buf, "1<%d,%d,%d,%d,%ld>",
				                 nbuckets, nbuckets_original,
				                 nbatch, nbatch_original,
				                 spacePeak);
			}
			else
				appendStringInfo(buf, "0>");
		}
			break;
		
		default:
			break;
	}
}

/*
 * InstrIn
 *
 * DeSerialize of one Instrumentation.
 */
static void
InstrIn(StringInfo str, RemoteInstr *rinstr)
{
	char *tmp_pos;
	char *tmp_head = &str->data[str->cursor];
	Instrumentation *instr = &rinstr->instr;
	
	if (str->len <= 0)
		return;
	
	/* verify nodetype and plan_node_id */
	rinstr->nodeTag = strtol(tmp_head, &tmp_pos, 0);
	tmp_head = tmp_pos + 1;
	rinstr->key.plan_node_id = (int) strtol(tmp_head, &tmp_pos, 0);
	tmp_head = tmp_pos + 1;
	rinstr->key.node_id = strtol(tmp_head, &tmp_pos, 0);
	tmp_head = tmp_pos + 1;
	
	/* read values */
	INSTR_READ_FIELD(need_timer);
	INSTR_READ_FIELD(need_bufusage);
	INSTR_READ_FIELD(running);
	
	INSTR_READ_FIELD(starttime.tv_sec);
	INSTR_READ_FIELD(starttime.tv_nsec);
	INSTR_READ_FIELD(counter.tv_sec);
	INSTR_READ_FIELD(counter.tv_nsec);
	
	INSTR_READ_FIELD(firsttuple);
	INSTR_READ_FIELD(tuplecount);
	
	INSTR_READ_FIELD(bufusage_start.shared_blks_hit);
	INSTR_READ_FIELD(bufusage_start.shared_blks_read);
	INSTR_READ_FIELD(bufusage_start.shared_blks_dirtied);
	INSTR_READ_FIELD(bufusage_start.shared_blks_written);
	INSTR_READ_FIELD(bufusage_start.local_blks_hit);
	INSTR_READ_FIELD(bufusage_start.local_blks_read);
	INSTR_READ_FIELD(bufusage_start.local_blks_dirtied);
	INSTR_READ_FIELD(bufusage_start.local_blks_written);
	INSTR_READ_FIELD(bufusage_start.temp_blks_read);
	INSTR_READ_FIELD(bufusage_start.temp_blks_written);
	INSTR_READ_FIELD(bufusage_start.blk_read_time.tv_sec);
	INSTR_READ_FIELD(bufusage_start.blk_read_time.tv_nsec);
	INSTR_READ_FIELD(bufusage_start.blk_write_time.tv_sec);
	INSTR_READ_FIELD(bufusage_start.blk_write_time.tv_nsec);
	
	INSTR_READ_FIELD(startup);
	INSTR_READ_FIELD(total);
	INSTR_READ_FIELD(ntuples);
	INSTR_READ_FIELD(nloops);
	INSTR_READ_FIELD(nfiltered1);
	INSTR_READ_FIELD(nfiltered2);
	
	INSTR_READ_FIELD(bufusage.shared_blks_hit);
	INSTR_READ_FIELD(bufusage.shared_blks_read);
	INSTR_READ_FIELD(bufusage.shared_blks_dirtied);
	INSTR_READ_FIELD(bufusage.shared_blks_written);
	INSTR_READ_FIELD(bufusage.local_blks_hit);
	INSTR_READ_FIELD(bufusage.local_blks_read);
	INSTR_READ_FIELD(bufusage.local_blks_dirtied);
	INSTR_READ_FIELD(bufusage.local_blks_written);
	INSTR_READ_FIELD(bufusage.temp_blks_read);
	INSTR_READ_FIELD(bufusage.temp_blks_written);
	INSTR_READ_FIELD(bufusage.blk_read_time.tv_sec);
	INSTR_READ_FIELD(bufusage.blk_read_time.tv_nsec);
	INSTR_READ_FIELD(bufusage.blk_write_time.tv_sec);
	INSTR_READ_FIELD(bufusage.blk_write_time.tv_nsec);
	
	elog(DEBUG1, "InstrIn: plan_node_id %d, node %d, nloops %.0f", rinstr->key.plan_node_id, rinstr->key.node_id, instr->nloops);
	
	/* tmp_head points to next instrument's nodetype or '\0' already */
	str->cursor = tmp_head - &str->data[0];
}

/*
 * SpecInstrIn
 *
 * DeSerialize of specific instrument info of current node.
 */
static void
SpecInstrIn(StringInfo str, RemoteInstr *instr)
{
	char    *tmp_pos;
	char    *tmp_head = &str->data[str->cursor];
	
	switch(instr->nodeTag)
	{
		case T_Gather:
		case T_GatherMerge:
		{
			INSTR_READ_FIELD(nworkers_launched);
		}
			break;
		case T_Sort:
		{
			/* either stat or w_stat is valid */
			bool isvalid = (bool) strtod(tmp_head, &tmp_pos);
			tmp_head = tmp_pos + 1;
			
			if (isvalid)
			{
				INSTR_READ_FIELD(sort_stat.sortMethod);
				INSTR_READ_FIELD(sort_stat.spaceType);
				INSTR_READ_FIELD(sort_stat.spaceUsed);
				Assert(instr->sort_stat.sortMethod != SORT_TYPE_STILL_IN_PROGRESS);
			}
			
			INSTR_READ_FIELD(nworkers_launched);
			if (instr->nworkers_launched > 0)
			{
				int n;
				instr->w_sort_stats = (TuplesortInstrumentation *) palloc0(instr->nworkers_launched * sizeof(TuplesortInstrumentation));
				
				for (n = 0; n < instr->nworkers_launched; n++)
				{
					INSTR_READ_FIELD(w_sort_stats[n].sortMethod);
					if (instr->w_sort_stats[n].sortMethod != SORT_TYPE_STILL_IN_PROGRESS)
					{
						INSTR_READ_FIELD(w_sort_stats[n].spaceType);
						INSTR_READ_FIELD(w_sort_stats[n].spaceUsed);
					}
				}
			}
		}
			break;
		case T_Hash:
		{
			bool isvalid = (bool) strtod(tmp_head, &tmp_pos);
			tmp_head = tmp_pos + 1;
			
			if (isvalid)
			{
				INSTR_READ_FIELD(hash_stat.nbuckets);
				INSTR_READ_FIELD(hash_stat.nbuckets_original);
				INSTR_READ_FIELD(hash_stat.nbatch);
				INSTR_READ_FIELD(hash_stat.nbatch_original);
				INSTR_READ_FIELD(hash_stat.space_peak);
			}
		}
			break;
		
		default:
			break;
	}
	
	str->cursor = tmp_head - &str->data[0];
}

/*
 * SerializeLocalInstr
 *
 * Serialize local instruments in the planstate tree for sending.
 */
static bool
SerializeLocalInstr(PlanState *planstate, SerializeState *ss)
{
	/*
	 * We should handle InitPlan/SubPlan the same as in ExplainSubPlans.
	 * But we do not want another planstate_tree_walker,
	 * it is ok to use plan_node_id in place of plan_id.
	 */
	int plan_node_id = planstate->plan->plan_node_id;
	if (bms_is_member(plan_node_id, ss->printed_nodes))
		return false;
	else
		ss->printed_nodes = bms_add_member(ss->printed_nodes, plan_node_id);
	
	/* For CteScan producer, deal with its child directly */
	if (IsA(planstate, CteScanState))
		planstate = ((CteScanState *)planstate)->cteplanstate;
	
	if (planstate->instrument)
	{
		/* clean up the instrumentation state as in ExplainNode */
		InstrEndLoop(planstate->instrument);
		if (planstate->dn_instrument)
		{
			/* re-send our received remote instr to upstream. */
			int n;
			for (n = 0; n < planstate->dn_instrument->nnode; n++)
			{
				Instrumentation *instrument = &(planstate->dn_instrument->instrument[n].instr);
				int              node_id = planstate->dn_instrument->instrument[n].nodeid;
				
				/* instrument valid only if node_oid set */
				if (node_id != 0)
				{
					InstrOut(&ss->buf, planstate->plan, instrument, node_id);
					SpecInstrOut(&ss->buf, nodeTag(planstate->plan), planstate);
				}
				else
				{
					elog(DEBUG1, "can't send instr out plan_node_id %d not attached", plan_node_id);
				}
			}
		}
		else
		{
			/* send our own instr */
			InstrOut(&ss->buf, planstate->plan, planstate->instrument, 0);
			SpecInstrOut(&ss->buf, nodeTag(planstate->plan), planstate);
		}
	}
	else
	{
		/* should not be NULL */
		elog(ERROR, "SerializeLocalInstr: instrument is NULL, %d",
		     nodeTag(planstate));
	}

	return planstate_tree_walker(planstate, SerializeLocalInstr, ss);
}

/*
 * SendLocalInstr
 *
 * Serialize local instrument of the given planstate and send it to upper node.
 */
void
SendLocalInstr(PlanState *planstate)
{
	SerializeState ss;
	
	/* Construct str with the same logic in ExplainNode */
	ss.printed_nodes = NULL;
	pq_beginmessage(&ss.buf, 'i');
	SerializeLocalInstr(planstate, &ss);
	pq_endmessage(&ss.buf);
	bms_free(ss.printed_nodes);
	pq_flush();
}

static void
combineSpecRemoteInstr(RemoteInstr *rtarget, RemoteInstr *rsrc)
{
	int i;
	/* specific instrument */
	switch (rsrc->nodeTag)
	{
		case T_Gather:
		case T_GatherMerge:
		{
			rtarget->nworkers_launched = Max(rtarget->nworkers_launched, rsrc->nworkers_launched);
		}
			break;
		case T_Sort:
		{
			if (rsrc->sort_stat.sortMethod != SORT_TYPE_STILL_IN_PROGRESS &&
			    rsrc->sort_stat.sortMethod != -1)
			{
				/* TODO: figure out which sortMethod is worse */
				rtarget->sort_stat.sortMethod = rsrc->sort_stat.sortMethod;
				if (rtarget->sort_stat.spaceType == rsrc->sort_stat.spaceType)
				{
					/* same space type, just compare space used */
					rtarget->sort_stat.spaceUsed = Max(rtarget->sort_stat.spaceUsed, rsrc->sort_stat.spaceUsed);
				}
				else if (rtarget->sort_stat.spaceType > rsrc->sort_stat.spaceType)
				{
					/* invalid > memory > disk */
					rtarget->sort_stat.spaceType = rsrc->sort_stat.spaceType;
					rtarget->sort_stat.spaceUsed = rsrc->sort_stat.spaceUsed;
				}
			}
			
			rtarget->nworkers_launched = Max(rtarget->nworkers_launched, rsrc->nworkers_launched);
			if (rtarget->w_sort_stats == NULL)
			{
				rtarget->w_sort_stats = palloc0(rtarget->nworkers_launched * sizeof(TuplesortInstrumentation));
				for (i = 0; i < rtarget->nworkers_launched; i++)
					rtarget->w_sort_stats[i].spaceType = -1;
			}
			for (i = 0; i < rtarget->nworkers_launched; i++)
			{
				if (rsrc->w_sort_stats[i].sortMethod == SORT_TYPE_STILL_IN_PROGRESS ||
				    rsrc->w_sort_stats[i].sortMethod == -1)
					continue;
				
				/* same logic above */
				/* TODO: figure out which sortMethod is worse */
				rtarget->w_sort_stats[i].sortMethod = rsrc->w_sort_stats[i].sortMethod;
				if (rtarget->w_sort_stats[i].spaceType == rsrc->w_sort_stats[i].spaceType)
				{
					/* same space type, just compare space used */
					rtarget->w_sort_stats[i].spaceUsed = Max(rtarget->w_sort_stats[i].spaceUsed, rsrc->w_sort_stats[i].spaceUsed);
				}
				else if (rtarget->w_sort_stats[i].spaceType > rsrc->w_sort_stats[i].spaceType)
				{
					/* invalid > memory > disk */
					rtarget->w_sort_stats[i].spaceType = rsrc->w_sort_stats[i].spaceType;
					rtarget->w_sort_stats[i].spaceUsed = rsrc->w_sort_stats[i].spaceUsed;
				}
				
				elog(DEBUG1, "combine parallel plan %d sort state %d %d %ld",
				     rtarget->key.plan_node_id,
				     rtarget->w_sort_stats[i].sortMethod,
				     rtarget->w_sort_stats[i].spaceType,
				     rtarget->w_sort_stats[i].spaceUsed);
			}
		}
			break;
		case T_Hash:
		{
			rtarget->hash_stat.nbuckets = Max(rtarget->hash_stat.nbuckets, rsrc->hash_stat.nbuckets);
			rtarget->hash_stat.nbuckets_original = Max(rtarget->hash_stat.nbuckets_original, rsrc->hash_stat.nbuckets_original);
			rtarget->hash_stat.nbatch = Max(rtarget->hash_stat.nbatch, rsrc->hash_stat.nbatch);
			rtarget->hash_stat.nbatch_original = Max(rtarget->hash_stat.nbatch_original, rsrc->hash_stat.nbatch_original);
			rtarget->hash_stat.space_peak = Max(rtarget->hash_stat.space_peak, rsrc->hash_stat.space_peak);
		}
			break;
		default:
			break;
	}
}

/*
 * combineRemoteInstr
 *
 * tool function to combine received instrumentation of all nodes,
 * currently it choose max value.
 */
static void
combineRemoteInstr(RemoteInstr *rtarget, RemoteInstr *rsrc)
{
	Instrumentation *target = &rtarget->instr;
	Instrumentation *src = &rsrc->instr;
	
	Assert(rtarget->key.node_id == rsrc->key.node_id);
	Assert(rtarget->key.plan_node_id == rsrc->key.plan_node_id);
	Assert(rtarget->nodeTag == rsrc->nodeTag);
	
	/* regular instrument */
	INSTR_MAX_FIELD(need_timer);
	INSTR_MAX_FIELD(need_bufusage);
	INSTR_MAX_FIELD(running);
	
	INSTR_MAX_FIELD(starttime.tv_sec);
	INSTR_MAX_FIELD(starttime.tv_nsec);
	INSTR_MAX_FIELD(counter.tv_sec);
	INSTR_MAX_FIELD(counter.tv_nsec);
	
	INSTR_MAX_FIELD(firsttuple);
	INSTR_MAX_FIELD(tuplecount);
	
	INSTR_MAX_FIELD(bufusage_start.shared_blks_hit);
	INSTR_MAX_FIELD(bufusage_start.shared_blks_read);
	INSTR_MAX_FIELD(bufusage_start.shared_blks_dirtied);
	INSTR_MAX_FIELD(bufusage_start.shared_blks_written);
	INSTR_MAX_FIELD(bufusage_start.local_blks_hit);
	INSTR_MAX_FIELD(bufusage_start.local_blks_read);
	INSTR_MAX_FIELD(bufusage_start.local_blks_dirtied);
	INSTR_MAX_FIELD(bufusage_start.local_blks_written);
	INSTR_MAX_FIELD(bufusage_start.temp_blks_read);
	INSTR_MAX_FIELD(bufusage_start.temp_blks_written);
	INSTR_MAX_FIELD(bufusage_start.blk_read_time.tv_sec);
	INSTR_MAX_FIELD(bufusage_start.blk_read_time.tv_nsec);
	INSTR_MAX_FIELD(bufusage_start.blk_write_time.tv_sec);
	INSTR_MAX_FIELD(bufusage_start.blk_write_time.tv_nsec);
	
	INSTR_MAX_FIELD(startup);
	INSTR_MAX_FIELD(total);
	INSTR_MAX_FIELD(ntuples);
	INSTR_MAX_FIELD(nloops);
	INSTR_MAX_FIELD(nfiltered1);
	INSTR_MAX_FIELD(nfiltered2);
	
	INSTR_MAX_FIELD(bufusage.shared_blks_hit);
	INSTR_MAX_FIELD(bufusage.shared_blks_read);
	INSTR_MAX_FIELD(bufusage.shared_blks_dirtied);
	INSTR_MAX_FIELD(bufusage.shared_blks_written);
	INSTR_MAX_FIELD(bufusage.local_blks_hit);
	INSTR_MAX_FIELD(bufusage.local_blks_read);
	INSTR_MAX_FIELD(bufusage.local_blks_dirtied);
	INSTR_MAX_FIELD(bufusage.local_blks_written);
	INSTR_MAX_FIELD(bufusage.temp_blks_read);
	INSTR_MAX_FIELD(bufusage.temp_blks_written);
	INSTR_MAX_FIELD(bufusage.blk_read_time.tv_sec);
	INSTR_MAX_FIELD(bufusage.blk_read_time.tv_nsec);
	INSTR_MAX_FIELD(bufusage.blk_write_time.tv_sec);
	INSTR_MAX_FIELD(bufusage.blk_write_time.tv_nsec);
	
	combineSpecRemoteInstr(rtarget, rsrc);
}

/*
 * HandleRemoteInstr
 *
 * Handle remote instrument message and save it by plan_node_id.
 */
void
HandleRemoteInstr(char *msg_body, size_t len, int nodeid, ResponseCombiner *combiner)
{
	RemoteInstr recv_instr;
	StringInfo  recv_str;
	bool        found;
	RemoteInstr *cur_instr;
	MemoryContext oldcontext;
	
	if (combiner->recv_instr_htbl == NULL)
	{
		elog(WARNING, "combiner is not prepared for instrumentation");
		return;
	}
	elog(DEBUG1, "Handle remote instrument: nodeid %d", nodeid);
	
	/* must doing this under per query context */
	oldcontext = MemoryContextSwitchTo(combiner->ss.ps.state->es_query_cxt);
	
	recv_str = makeStringInfo();
	appendBinaryStringInfo(recv_str, msg_body, len);
	
	while(recv_str->cursor < recv_str->len)
	{
		memset(&recv_instr, 0, sizeof(RemoteInstr));
		recv_instr.sort_stat.sortMethod = -1;
		recv_instr.sort_stat.spaceType = -1;
		InstrIn(recv_str, &recv_instr);
		SpecInstrIn(recv_str, &recv_instr);
		
		if (recv_instr.key.node_id == 0)
			recv_instr.key.node_id = nodeid;
		
		cur_instr = (RemoteInstr *) hash_search(combiner->recv_instr_htbl,
		                                        (void *) &recv_instr.key,
		                                        HASH_ENTER, &found);
		if (found)
		{
			combineRemoteInstr(cur_instr, &recv_instr);
		}
		else
		{
			elog(DEBUG1, "remote instr hashtable enter plan_node_id %d node %d",
			     recv_instr.key.plan_node_id, recv_instr.key.node_id);
			
			memcpy(cur_instr, &recv_instr, sizeof(RemoteInstr));
			if (recv_instr.nodeTag == T_Sort && recv_instr.nworkers_launched > 0)
			{
				Size size = sizeof(TuplesortInstrumentation) * recv_instr.nworkers_launched;
				
				cur_instr->w_sort_stats = palloc(size);
				memcpy(cur_instr->w_sort_stats, recv_instr.w_sort_stats, size);
			}
		}
	}
	
	MemoryContextSwitchTo(oldcontext);
}

/*
 * attachRemoteSpecialInstr
 *
 * Attach specific information in planstate.
 */
static void
attachRemoteSpecificInstr(PlanState *planstate, RemoteInstr *rinstr)
{
	int nodeTag = nodeTag(planstate->plan);
	int nworkers = rinstr->nworkers_launched;
	
	switch(nodeTag)
	{
		case T_Gather:
		{
			GatherState *gs = (GatherState *) planstate;
			gs->nworkers_launched = nworkers;
		}
			break;
		case T_GatherMerge:
		{
			GatherMergeState *gms = (GatherMergeState *) planstate;
			gms->nworkers_launched = nworkers;
		}
			break;
		case T_Sort:
		{
			SortState *ss = (SortState *) planstate;
			ss->instrument.sortMethod = rinstr->sort_stat.sortMethod;
			ss->instrument.spaceType = rinstr->sort_stat.spaceType;
			ss->instrument.spaceUsed = rinstr->sort_stat.spaceUsed;
			elog(DEBUG1, "attach sort nworkers %d", nworkers);
			
			if (nworkers > 0)
			{
				int  i;
				if (ss->shared_info == NULL)
				{
					Size size = offsetof(SharedSortInfo, sinstrument)
					            + nworkers * sizeof(TuplesortInstrumentation);
					ss->shared_info = palloc0(size);
				}
				
				ss->shared_info->num_workers = nworkers;
				for (i = 0; i < nworkers; i++)
				{
					ss->shared_info->sinstrument[i].sortMethod = rinstr->w_sort_stats[i].sortMethod;
					ss->shared_info->sinstrument[i].spaceType = rinstr->w_sort_stats[i].spaceType;
					ss->shared_info->sinstrument[i].spaceUsed = rinstr->w_sort_stats[i].spaceUsed;
					elog(DEBUG1, "attach parallel sort %d, info: %d %d %ld",
					     planstate->plan->plan_node_id,
					     ss->shared_info->sinstrument[i].sortMethod,
					     ss->shared_info->sinstrument[i].spaceType,
					     ss->shared_info->sinstrument[i].spaceUsed);
				}
			}
		}
			break;
		case T_Hash:
		{
			HashState *hs = (HashState *) planstate;
			if (IsParallelWorker())
			{
				Assert(hs->hinstrument != NULL);
				Assert(hs->shared_info != NULL);
				Assert(hs->hashtable == NULL);
				/* copy into first instrument */
				memcpy(&hs->shared_info->hinstrument[0], &rinstr->hash_stat, sizeof(HashInstrumentation));
				elog(DEBUG1, "parallel worker attach hash state plan %d peak %zu",
				     planstate->plan->plan_node_id, hs->hinstrument->space_peak);
			}
			else
			{
				if (hs->hashtable == NULL)
					hs->hashtable = palloc(sizeof(HashJoinTableData));
				
				hs->hashtable->nbuckets = rinstr->hash_stat.nbuckets;
				hs->hashtable->nbuckets_original = rinstr->hash_stat.nbuckets_original;
				hs->hashtable->nbatch = rinstr->hash_stat.nbatch;
				hs->hashtable->nbatch_original = rinstr->hash_stat.nbatch_original;
				hs->hashtable->spacePeak = rinstr->hash_stat.space_peak;
			}
		}
			break;
		default:
			break;
	}
}

/*
 * AttachRemoteInstr
 *
 * Attach instrument information in planstate from saved info in combiner.
 */
bool
AttachRemoteInstr(PlanState *planstate, AttachRemoteInstrContext *ctx)
{
	int plan_node_id = planstate->plan->plan_node_id;
	
	if (bms_is_member(plan_node_id, ctx->printed_nodes))
		return false;
	else
		ctx->printed_nodes = bms_add_member(ctx->printed_nodes, plan_node_id);
	
	if (IsA(planstate, RemoteSubplanState) && planstate->lefttree == NULL)
	{
		/* subplan could be here, init it's child too */
		planstate->lefttree = ExecInitNode(planstate->plan->lefttree,
		                                   planstate->state,
		                                   EXEC_FLAG_EXPLAIN_ONLY);
	}
	
	if (planstate->instrument)
	{
		RemoteInstrKey  key;
		bool            found;
		RemoteInstr    *rinstr;
		RemoteInstr     rinstr_final; /* for specific instrument */
		bool            spec_need_attach = false;
		ListCell       *lc;
		
		int n = 0;
		int nnode = list_length(ctx->node_idx_List);
		
		key.plan_node_id = plan_node_id;
		memset(&rinstr_final, 0, sizeof(RemoteInstr));
		rinstr_final.sort_stat.sortMethod = -1;
		rinstr_final.sort_stat.spaceType = -1;
		
		/* This is for non-parallel case. If parallel, we init dn_instrument in dsm. */
		if (planstate->dn_instrument == NULL)
		{
			Size size = offsetof(DatanodeInstrumentation, instrument) +
			            mul_size(nnode, sizeof(RemoteInstrumentation));
			Assert(!IsParallelWorker());
			planstate->dn_instrument = palloc0(size);
			planstate->dn_instrument->nnode = nnode;
		}
		
		foreach(lc, ctx->node_idx_List)
		{
			key.node_id = get_pgxc_node_id(get_nodeoid_from_nodeid(lfirst_int(lc), PGXC_NODE_DATANODE));
			elog(DEBUG1, "attach node %d, plan_node_id %d", key.node_id, key.plan_node_id);
			rinstr = (RemoteInstr *) hash_search(ctx->htab,
			                                     (void *) &key,
			                                     HASH_FIND, &found);
			
			if (found)
			{
				Assert(rinstr->nodeTag == nodeTag(planstate->plan));
				Assert(rinstr->key.plan_node_id == plan_node_id);
				
				elog(DEBUG1, "instr attach plan_node_id %d node %d index %d", plan_node_id, key.node_id, n);
				planstate->dn_instrument->instrument[n].nodeid = key.node_id;
				memcpy(&planstate->dn_instrument->instrument[n].instr, &rinstr->instr, sizeof(Instrumentation));
				/* TODO attach all nodes' remote specific instr */
				rinstr_final.nodeTag = rinstr->nodeTag;
				rinstr_final.key = rinstr->key;
				combineSpecRemoteInstr(&rinstr_final, rinstr);
				spec_need_attach = true;
			}
			else
			{
				elog(DEBUG1, "failed to find remote instr of plan_node_id %d node %d", plan_node_id, key.node_id);
			}
			n++;
		}
		/* TODO attach all nodes' remote specific instr */
		if (spec_need_attach)
			attachRemoteSpecificInstr(planstate, &rinstr_final);
	}
	else
	{
		/* should not be NULL */
		elog(ERROR, "AttachRemoteInstr: instrument is NULL, tag %d id %d",
		     nodeTag(planstate), plan_node_id);
	}

	return planstate_tree_walker(planstate, AttachRemoteInstr, ctx);
}

/*
 * ExplainCommonRemoteInstr
 *
 * Explain remote instruments for common info of current node.
 */
void
ExplainCommonRemoteInstr(PlanState *planstate, ExplainState *es)
{
	int     i;
	int     nnode = planstate->dn_instrument->nnode;
	
	RemoteInstrumentation *rinstr = planstate->dn_instrument->instrument;
	/* for min/max display */
	double nloops_min, nloops_max, nloops;
	double startup_sec_min, startup_sec_max, startup_sec;
	double total_sec_min, total_sec_max, total_sec;
	double rows_min, rows_max, rows;
	/* for verbose */
	StringInfoData buf;
	
	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		appendStringInfoChar(es->str, '\n');
		appendStringInfoSpaces(es->str, es->indent * 2);
	}
	
	/* give min max a startup value */
	for (i = 0; i < nnode; i++)
	{
		Instrumentation *instr = &rinstr[i].instr;
		if (instr->nloops != 0)
		{
			nloops_min = nloops_max = instr->nloops;
			startup_sec_min = startup_sec_max = 1000.0 * instr->startup / nloops_min;
			total_sec_min = total_sec_max = 1000.0 * instr->total / nloops_min;
			rows_min = rows_max = instr->ntuples / nloops_min;
			break;
		}
	}
	if (i == nnode)
	{
		appendStringInfo(es->str, "DN (never executed)");
		return;
	}
	
	if (es->verbose)
		initStringInfo(&buf);
	
	for (i = 0; i < nnode; i++)
	{
		Instrumentation *instr = &rinstr[i].instr;
		int              node_id = rinstr[i].nodeid;
		char            *dnname;
		
		if (node_id == 0)
			continue;
		
		dnname = get_pgxc_nodename_from_identifier(node_id);
		nloops = instr->nloops;
		startup_sec = 1000.0 * instr->startup / nloops;
		total_sec = 1000.0 * instr->total / nloops;
		rows = instr->ntuples / nloops;
		
		SET_MIN_MAX(nloops_min, nloops_max, nloops);
		SET_MIN_MAX(startup_sec_min, startup_sec_max, startup_sec);
		SET_MIN_MAX(total_sec_min, total_sec_max, total_sec);
		SET_MIN_MAX(rows_min, rows_max, rows);
		
		/* one line for each dn if verbose */
		if (es->verbose)
		{
			if (es->format == EXPLAIN_FORMAT_TEXT)
			{
				appendStringInfoChar(&buf, '\n');
				appendStringInfoSpaces(&buf, es->indent * 2);
				if (nloops <= 0)
				{
					appendStringInfo(&buf, "- %s (never executed)", dnname);
				}
				else
				{
					if (es->timing)
						appendStringInfo(&buf,
						                 "- %s (actual time=%.3f..%.3f rows=%.0f loops=%.0f)",
						                 dnname, startup_sec, total_sec, rows, nloops);
					else
						appendStringInfo(&buf,
						                 "- %s (actual rows=%.0f loops=%.0f)",
						                 dnname, rows, nloops);
				}
			}
			else
			{
				ExplainPropertyText("Data Node", dnname, es);
				if (es->timing)
				{
					ExplainPropertyFloat("Actual Startup Time", startup_sec, 3, es);
					ExplainPropertyFloat("Actual Total Time", total_sec, 3, es);
				}
				ExplainPropertyFloat("Actual Rows", rows, 0, es);
				ExplainPropertyFloat("Actual Loops", nloops, 0, es);
			}
		}
	}
	
	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		bool show_verbose = true;
		
		if (nloops_max <= 0)
		{
			show_verbose = false;
			appendStringInfo(es->str, "DN (never executed)");
		}
		else
		{
			if (es->timing)
				appendStringInfo(es->str,
				                 "DN (actual startup time=%.3f..%.3f total time=%.3f..%.3f rows=%.0f..%.0f loops=%.0f..%.0f)",
				                 startup_sec_min, startup_sec_max,
				                 total_sec_min, total_sec_max, rows_min, rows_max,
				                 nloops_min, nloops_max);
			else
				appendStringInfo(es->str,
				                 "DN (actual rows=%.0f..%.0f loops=%.0f..%.0f)",
				                 rows_min, rows_max, nloops_min, nloops_max);
		}
		
		if (es->verbose)
		{
			if (show_verbose)
				appendStringInfo(es->str, "%s", buf.data);
			pfree(buf.data);
		}
	}
	else
	{
		ExplainPropertyText("Data Node", "ALL", es);
		if (es->timing)
		{
			ExplainPropertyFloat("Actual Min Startup Time", startup_sec_min, 3, es);
			ExplainPropertyFloat("Actual Max Startup Time", startup_sec_max, 3, es);
			ExplainPropertyFloat("Actual Min Total Time", total_sec_min, 3, es);
			ExplainPropertyFloat("Actual Max Total Time", total_sec_max, 3, es);
		}
		ExplainPropertyFloat("Actual Min Rows", rows_min, 0, es);
		ExplainPropertyFloat("Actual Max Rows", rows_max, 0, es);
		ExplainPropertyFloat("Actual Min Loops", nloops_min, 0, es);
		ExplainPropertyFloat("Actual Max Loops", nloops_max, 0, es);
	}
}
