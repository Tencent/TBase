/*-------------------------------------------------------------------------
 *
 * explain_dist.h
 *
 * Portions Copyright (c) 2018, Tencent TBase-C Group.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/explain_dist.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef EXPLAINDIST_H
#define EXPLAINDIST_H

#include "commands/explain.h"
#include "pgxc/execRemote.h"

/* Key of hash table entry */
typedef struct RemoteInstrKey
{
	int plan_node_id;   /* unique id of current plan node */
	int node_id;        /* node id */
} RemoteInstrKey;

/* Hash table entry */
typedef struct RemoteInstr
{
	RemoteInstrKey key;
	
	int nodeTag;            /* type of current plan node */
	Instrumentation instr;  /* instrument of current plan node */
	
	/* for Gather and Sort */
	int nworkers_launched;  /* worker num of gather or sort */
	
	/* for Sort */
	TuplesortInstrumentation sort_stat;      /* instrument if no parallel */
	TuplesortInstrumentation *w_sort_stats;  /* instrument of parallel workers */
	
	/* for Hash */
	HashInstrumentation hash_stat;
} RemoteInstr;

typedef struct AttachRemoteInstrContext
{
	List        *node_idx_List;     /* list of node index in dn_handles */
	HTAB        *htab;              /* htab from combiner, stored remote instr */
	Bitmapset   *printed_nodes;     /* ids of plan nodes we've handled */
} AttachRemoteInstrContext;

extern void SendLocalInstr(PlanState *planstate);
extern void HandleRemoteInstr(char *msg_body, size_t len, int nodeid, ResponseCombiner *combiner);
extern bool AttachRemoteInstr(PlanState *planstate, AttachRemoteInstrContext *ctx);
extern void ExplainCommonRemoteInstr(PlanState *planstate, ExplainState *es);

#endif  /* EXPLAINDIST_H  */