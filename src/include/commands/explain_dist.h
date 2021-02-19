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

/* Hash table entry */
typedef struct
{
	int id;                 /* unique id of current plan node */
	int nodeTag;            /* type of current plan node */
	Instrumentation instr;  /* instrument of current plan node */
	
	/* for Gather */
	int nworkers_launched;  /* worker num of gather */
	
	/* for Hash: */
} RemoteInstr;

extern void SendLocalInstr(PlanState *planstate);
extern void HandleRemoteInstr(char *msg_body, size_t len, int nodeoid, ResponseCombiner *combiner);
extern bool AttachRemoteInstr(PlanState *planstate, ResponseCombiner *combiner);

#endif  /* EXPLAINDIST_H  */