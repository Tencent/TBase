/*-------------------------------------------------------------------------
 *
 * gtm_seq.h
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#ifndef GTM_SEQ_H
#define GTM_SEQ_H

#include "gtm/stringinfo.h"
#include "gtm/gtm_lock.h"
#include "gtm/libpq-be.h"

/* Global sequence  related structures */
#ifdef __TBASE__
#define  SEQ_RESERVE_COUNT     5000
#define  SEQ_RESERVE_MIN_GAP   10
#endif
/* constant postfix for sequence to avoid same name */
#define GTM_SEQ_POSTFIX "_$TBASE$_sequence_temp_54312678712612"
#define SEQ_KEY_LEN 256

typedef struct GTM_SeqLastVal
{
	char			gs_coord_name[SP_NODE_NAME];
	int32			gs_coord_procid;
	GTM_Sequence	gs_last_value;
} GTM_SeqLastVal;

typedef struct GTM_SeqInfo
{
	GTM_SequenceKey	gs_key;
	GTM_SequenceKey	gs_oldkey;
	GTM_Sequence	gs_value;
	GTM_Sequence	gs_backedUpValue;
	GTM_Sequence	gs_init_value;
	int32			gs_max_lastvals;
	int32			gs_lastval_count;
	GTM_SeqLastVal *gs_last_values;
	GTM_Sequence	gs_increment_by; /* increase step */
	GTM_Sequence	gs_min_value;    /* min value of the seq */
	GTM_Sequence	gs_max_value;    /* max value of the seq */
	bool			gs_cycle;        /* whether we are cycled */
	bool			gs_called;       
	GlobalTransactionId	gs_created_gxid;

	int32			gs_ref_count;
	int32			gs_state;
	GTM_RWLock		gs_lock;
#ifdef __TBASE__
	bool			 gs_reserved;	 /* whether we have reserve value*/
	GTMStorageHandle gs_store_handle;
	int32            gs_left_reserve_seq_number;
#endif
} GTM_SeqInfo;

#define SEQ_STATE_ACTIVE	1
#define SEQ_STATE_DELETED	2

#define SEQ_IS_ASCENDING(s)		((s)->gs_increment_by > 0)
#define SEQ_IS_CYCLE(s)		((s)->gs_cycle)
#define SEQ_IS_CALLED(s)	((s)->gs_called)

#define SEQ_DEF_MAX_SEQVAL_ASCEND			0x7ffffffffffffffeLL
#define SEQ_DEF_MIN_SEQVAL_ASCEND			0x1

#define SEQ_DEF_MAX_SEQVAL_DESCEND			-0x1
#define SEQ_DEF_MIN_SEQVAL_DESCEND			-0x7ffffffffffffffeLL

#define SEQ_MAX_REFCOUNT		1024

/* SEQUENCE Management */
void GTM_InitSeqManager(void);
int GTM_SeqOpen(GTM_SequenceKey seqkey,
			GTM_Sequence increment_by,
			GTM_Sequence minval,
			GTM_Sequence maxval,
			GTM_Sequence startval,
			bool cycle,
			GlobalTransactionId gxid
			);
int GTM_SeqAlter(GTM_SequenceKey seqkey,
				 GTM_Sequence increment_by,
				 GTM_Sequence minval,
				 GTM_Sequence maxval,
				 GTM_Sequence startval,
				 GTM_Sequence lastval,
				 bool cycle,
				 bool is_restart);
int GTM_SeqClose(GTM_SequenceKey seqkey, GlobalTransactionId gxid);
int GTM_SeqRename(GTM_SequenceKey seqkey, GTM_SequenceKey newseqkey,
				  GlobalTransactionId gxid);
int GTM_SeqGetNext(GTM_SequenceKey seqkey, char *coord_name,
			   int coord_procid, GTM_Sequence range,
			   GTM_Sequence *result, GTM_Sequence *rangemax);
void GTM_SeqGetCurrent(GTM_SequenceKey seqkey, char *coord_name,
				  int coord_procid, GTM_Sequence *result);
int GTM_SeqSetVal(GTM_SequenceKey seqkey, char *coord_name,
			  int coord_procid, GTM_Sequence nextval, bool iscalled);
int GTM_SeqReset(GTM_SequenceKey seqkey);

void ProcessSequenceInitCommand(Port *myport, StringInfo message, bool is_backup);
void ProcessSequenceGetCurrentCommand(Port *myport, StringInfo message);
void ProcessSequenceGetNextCommand(Port *myport, StringInfo message, bool is_backup);
void ProcessSequenceSetValCommand(Port *myport, StringInfo message, bool is_backup);
void ProcessSequenceResetCommand(Port *myport, StringInfo message, bool is_backup);
void ProcessSequenceCloseCommand(Port *myport, StringInfo message, bool is_backup);
void ProcessSequenceRenameCommand(Port *myport, StringInfo message, bool is_backup);
void ProcessSequenceAlterCommand(Port *myport, StringInfo message, bool is_backup);

void ProcessSequenceListCommand(Port *myport, StringInfo message);
void ProcessSequenceCleanCommand(Port *myport, StringInfo message, bool is_backup);
void ProcessDBSequenceRenameCommand(Port *myport, StringInfo message, bool is_backup);
void ProcessCopyDataBaseSequenceCommand(Port *myport, StringInfo message);

void decode_seq_key(char* value, GTM_SequenceKey seqkey);
void GTM_SaveSeqInfo(FILE *ctlf);
int GTM_SeqRestore(GTM_SequenceKey seqkey,
			   GTM_Sequence increment_by,
			   GTM_Sequence minval,
			   GTM_Sequence maxval,
			   GTM_Sequence startval,
			   GTM_Sequence curval,
			   int32 state,
			   bool cycle,
			   bool called);

void GTM_CleanupSeqSession(char *coord_name, int coord_procid);

bool GTM_NeedSeqRestoreUpdate(GTM_SequenceKey seqkey);
void GTM_WriteRestorePointSeq(FILE *f);
void GTM_SeqRemoveCreated(void *seqinfo);
void GTM_SeqRestoreDropped(void *seqinfo);
void GTM_SeqRemoveDropped(void *seqinfo);
void GTM_SeqRestoreAltered(void *ptr);
void GTM_SeqRemoveAltered(void *seqinfo);

#ifdef __TBASE__
extern void  GTM_SeqInvalidateHandle(GTM_SequenceKey seqkey);
extern void  GTM_SeqInvalidateAlteredSeq(void *ptr);
#endif
#endif
