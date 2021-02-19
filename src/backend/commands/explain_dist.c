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
 * "nodetype-plan_node_id{val,val,...,val}".
 *
 * NOTE: The function should be modified if the structure of Instrumentation
 * or its relevant members has been changed.
 */
static void
InstrOut(StringInfo buf, Plan *plan, Instrumentation *instr)
{
	/* nodeTag for varify */
	appendStringInfo(buf, "%hd-%d{", nodeTag(plan), plan->plan_node_id);
	
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
	
	elog(DEBUG1, "InstrOut: plan_node_id %d, nloops %.0f", plan->plan_node_id, instr->nloops);
}

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
#if 0
		case T_Sort:
		{
			/* according to RemoteSortState and show_sort_info */
			SortState *sortstate = castNode(SortState, planstate);
			
			if (sortstate->sort_Done && sortstate->tuplesortstate)
			{
				Tuplesortstate  *state = (Tuplesortstate *) sortstate->tuplesortstate;
				char            *sortMethod;
				char            *spaceType;
				long            spaceUsed;
				
				tuplesort_get_stats(state, (const char **) &sortMethod, (const char **) &spaceType, &spaceUsed);
				appendStringInfo(buf, "1<%s,%s,%ld>",
				                 sortMethod, spaceType, spaceUsed);
			}
			else
				appendStringInfo(buf, "0>");
		}
			break;
		
		case T_Hash:
		{
			/* according to RemoteHashState and show_hash_info */
			HashState *hashstate = castNode(HashState, planstate);
			HashJoinTable hashtable = hashstate->hashtable;
			
			if (hashtable)
			{
				hashtable->nbuckets = 0;
				appendStringInfo(buf, "1<%d,%d,%d,%d,%ld>",
				                 hashtable->nbuckets, hashtable->nbuckets_original,
				                 hashtable->nbatch, hashtable->nbatch_original,
				                 (hashtable->spacePeak + 1023) / 1024);
			}
			else
				appendStringInfo(buf, "0>");
		}
			break;
#endif
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
	rinstr->id = (int) strtol(tmp_head, &tmp_pos, 0);
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
	
	elog(DEBUG1, "InstrIn: plan_node_id %d, nloops %.0f", rinstr->id, instr->nloops);
	
	/* tmp_head points to next instrument's nodetype or '\0' already */
	str->cursor = tmp_head - &str->data[0];
}

/*
 * SpecInstrIn
 *
 * DeSerialize of specific instrument info of current node.
 */
static void
SpecInstrIn(StringInfo str, RemoteInstr *rinstr)
{
	char    *tmp_pos;
	char    *tmp_head = &str->data[str->cursor];
	
	switch(rinstr->nodeTag)
	{
		case T_Gather:
		case T_GatherMerge:
		{
			rinstr->nworkers_launched = (int) strtod(tmp_head, &tmp_pos);
			tmp_head = tmp_pos + 1;
		}
			break;
#if 0
		case T_Sort:
		{
			RemoteSortState *instr = (RemoteSortState *)palloc0(
				sizeof(RemoteSortState));
			/* either stat or w_stat is valid */
			INSTR_READ_FIELD(rs.isvalid);
			if (instr->rs.isvalid)
			{
				INSTR_READ_FIELD(stat.sortMethod);
				INSTR_READ_FIELD(stat.spaceType);
				INSTR_READ_FIELD(stat.spaceUsed);
			}
			
			INSTR_READ_FIELD(rs.num_workers);
			if (instr->rs.num_workers > 0)
			{
				int n;
				Size size;
				
				size = mul_size(sizeof(TuplesortInstrumentation),
				                instr->rs.num_workers);
				instr->w_stats = (TuplesortInstrumentation *)palloc0(size);
				
				for (n = 0; n < instr->rs.num_workers; n++)
				{
					INSTR_READ_FIELD(w_stats[n].sortMethod);
					if (instr->w_stats[n].sortMethod != SORT_TYPE_STILL_IN_PROGRESS)
					{
						INSTR_READ_FIELD(w_stats[n].spaceType);
						INSTR_READ_FIELD(w_stats[n].spaceUsed);
					}
				}
			}
			remote_instr->state = (RemoteState *) instr;
		}
			break;
		
		case T_Hash:
		{
			RemoteHashState *instr = (RemoteHashState *)palloc0(
				sizeof(RemoteHashState));
			INSTR_READ_FIELD(rs.isvalid);
			if (instr->rs.isvalid)
			{
				INSTR_READ_FIELD(nbuckets);
				INSTR_READ_FIELD(nbuckets_original);
				INSTR_READ_FIELD(nbatch);
				INSTR_READ_FIELD(nbatch_original);
				INSTR_READ_FIELD(spacePeakKb);
			}
			remote_instr->state = (RemoteState *) instr;
		}
			break;
#endif
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
		InstrOut(&ss->buf, planstate->plan, planstate->instrument);
		//WorkerInstrOut(&ss->buf, planstate->worker_instrument);
		SpecInstrOut(&ss->buf, nodeTag(planstate->plan), planstate);
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
	
	Assert(rtarget->id == rsrc->id);
	Assert(rtarget->nodeTag == rsrc->nodeTag);
	
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
	
	rtarget->nworkers_launched = Max(rtarget->nworkers_launched, rsrc->nworkers_launched);
}

/*
 * HandleRemoteInstr
 *
 * Handle remote instrument message and save it by plan_node_id.
 */
void
HandleRemoteInstr(char *msg_body, size_t len, int nodeoid, ResponseCombiner *combiner)
{
	RemoteInstr recv_instr;
	StringInfo  recv_str;
	bool        found;
	RemoteInstr *cur_instr;
	
	if (combiner->recv_instr_htbl == NULL)
	{
		elog(ERROR, "combiner is not prepared for instrumentation");
	}
	elog(DEBUG1, "Handle remote instrument: nodeoid %d", nodeoid);
	
	recv_str = makeStringInfo();
	appendBinaryStringInfo(recv_str, msg_body, len);
	
	while(recv_str->cursor < recv_str->len)
	{
		InstrIn(recv_str, &recv_instr);
		SpecInstrIn(recv_str, &recv_instr);
		cur_instr = (RemoteInstr *) hash_search(combiner->recv_instr_htbl,
		                                        (void *) &recv_instr.id,
		                                        HASH_ENTER, &found);
		if (found)
		{
			combineRemoteInstr(cur_instr, &recv_instr);
		}
		else
		{
			memcpy(cur_instr, &recv_instr, sizeof(RemoteInstr));
		}
	}
}

/*
 * attachRemoteSpecialInstr
 *
 * Attach specific information in planstate.
 */
static void
attachRemoteSpecialInstr(PlanState *planstate, RemoteInstr *rinstr)
{
	int nodeTag = nodeTag(planstate->plan);
	
	switch(nodeTag)
	{
		case T_Gather:
			{
				GatherState *gs = (GatherState *) planstate;
				gs->nworkers_launched = rinstr->nworkers_launched;
			}
			break;
		case T_GatherMerge:
			{
				GatherMergeState *gms = (GatherMergeState *) planstate;
				gms->nworkers_launched = rinstr->nworkers_launched;
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
AttachRemoteInstr(PlanState *planstate, ResponseCombiner *combiner)
{
	int plan_node_id = planstate->plan->plan_node_id;
	if (bms_is_member(plan_node_id, combiner->printed_nodes))
		return false;
	else
		combiner->printed_nodes = bms_add_member(combiner->printed_nodes, plan_node_id);
	
	if (IsA(planstate, RemoteSubplanState) && NULL == planstate->lefttree)
	{
		Plan        *plan = planstate->plan;
		PlanState   *remote_ps;
		EState      *estate = planstate->state;

		remote_ps = ExecInitNode(plan->lefttree, estate, EXEC_FLAG_EXPLAIN_ONLY);
		planstate->lefttree = remote_ps;
	}
	
	if (planstate->instrument)
	{
		bool        found;
		RemoteInstr *rinstr= (RemoteInstr *) hash_search(combiner->recv_instr_htbl,
		                                                 (void *) &plan_node_id,
		                                                 HASH_FIND, &found);
		if (!found)
		{
			elog(DEBUG1, "AttachRemoteInstr: remote instrumentation not found, tag %d id %d",
			     nodeTag(planstate->plan), plan_node_id);
		}
		else
		{
			Assert(rinstr->nodeTag == nodeTag(planstate->plan));
			Assert(rinstr->id == plan_node_id);
			
			memcpy(planstate->instrument, &rinstr->instr, sizeof(Instrumentation));
			attachRemoteSpecialInstr(planstate, rinstr);
		}
	}
	else
	{
		/* should not be NULL */
		elog(ERROR, "AttachRemoteInstr: instrument is NULL, tag %d id %d",
		     nodeTag(planstate), plan_node_id);
	}

	return planstate_tree_walker(planstate, AttachRemoteInstr, combiner);
}
