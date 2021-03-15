/*-------------------------------------------------------------------------
 *
 * gtm_txn.h
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#ifndef _GTM_TXN_H
#define _GTM_TXN_H

#include "gtm/libpq-be.h"
#include "gtm/gtm_c.h"
#include "gtm/gtm_gxid.h"
#include "gtm/gtm_lock.h"
#include "gtm/gtm_list.h"
#include "gtm/stringinfo.h"
#include "port/atomics.h"



typedef int XidStatus;

#define TRANSACTION_STATUS_IN_PROGRESS      0x00
#define TRANSACTION_STATUS_COMMITTED        0x01
#define TRANSACTION_STATUS_ABORTED          0x02

struct GTM_RestoreContext;
/*
 * prototypes for functions in transam/transam.c
 */
extern bool GlobalTransactionIdDidCommit(GlobalTransactionId transactionId);
extern bool GlobalTransactionIdDidAbort(GlobalTransactionId transactionId);
extern void GlobalTransactionIdAbort(GlobalTransactionId transactionId);

/* in transam/varsup.c */
extern GlobalTransactionId GTM_GetGlobalTransactionId(GTM_TransactionHandle handle);
extern bool GTM_GetGlobalTransactionIdMulti(
		GTM_TransactionHandle handle[],
		int txn_count,
		GlobalTransactionId gxids[],
		GTM_TransactionHandle new_handle[],
		int *new_txn_count);
extern GlobalTransactionId ReadNewGlobalTransactionId(void);
extern GlobalTransactionId GTM_GetLatestCompletedXID(void);
extern void SetGlobalTransactionIdLimit(GlobalTransactionId oldest_datfrozenxid);
extern void SetNextGlobalTransactionId(GlobalTransactionId gxid);
#ifdef __TBASE__
extern GlobalTimestamp GetNextGlobalTimestamp(void);
extern void SetNextGlobalTimestamp(GlobalTimestamp gts);
extern GlobalTimestamp SyncGlobalTimestamp(void);
extern void AcquireWriteLock(void);
extern void ReleaseWriteLock(void);
extern void AcquireReadLock(void);

extern void ReleaseReadLock(void);

extern void ReleaseReadLockNoCheck(void);

#endif
extern void SetControlXid(GlobalTransactionId gxid);
extern void GTM_SetShuttingDown(void);

/* For restoration point backup */
extern bool GTM_NeedXidRestoreUpdate(void);
extern void GTM_WriteRestorePointXid(FILE *f);

typedef enum GTM_States
{
	GTM_STARTING,
	GTM_RUNNING,
	GTM_SHUTTING_DOWN
} GTM_States;

/* Global transaction states at the GTM */
typedef enum GTM_TransactionStates
{
	GTM_TXN_INIT,
	GTM_TXN_STARTING,
	GTM_TXN_IN_PROGRESS,
	GTM_TXN_PREPARE_IN_PROGRESS,
	GTM_TXN_PREPARED,
	GTM_TXN_COMMIT_IN_PROGRESS,
	GTM_TXN_COMMITTED,
	GTM_TXN_ABORT_IN_PROGRESS,
	GTM_TXN_ABORTED,
	GTM_TXN_IMPLICATE_PREPARED
} GTM_TransactionStates;

typedef struct GTM_TransactionInfo
{
	GTM_TransactionHandle	gti_handle;
	uint32					gti_client_id;
	char					gti_global_session_id[GTM_MAX_SESSION_ID_LEN];
	bool					gti_in_use;
	GlobalTransactionId		gti_gxid;
	GTM_TransactionStates	gti_state;
	GlobalTransactionId		gti_xmin;
	GTM_IsolationLevel		gti_isolevel;
	bool					gti_readonly;
	GTMProxy_ConnID			gti_proxy_client_id;
	char					*nodestring; /* List of nodes prepared */
	char					*gti_gid;

	GTM_SnapshotData		gti_current_snapshot;
	bool					gti_snapshot_set;

	GTM_RWLock				gti_lock;
	bool					gti_vacuum;
	gtm_List				*gti_created_seqs;
	gtm_List				*gti_dropped_seqs;
	gtm_List				*gti_altered_seqs;
} GTM_TransactionInfo;

#define GTM_MAX_2PC_NODES				16
/* By default a GID length is limited to 256 bits in PostgreSQL */
#define GTM_MAX_GID_LEN					256
#define GTM_MAX_NODESTRING_LEN			1024
#define GTM_CheckTransactionHandle(x)	((x) >= 0 && (x) < GTM_MAX_GLOBAL_TRANSACTIONS)
#define GTM_IsTransSerializable(x)		((x)->gti_isolevel == GTM_ISOLATION_SERIALIZABLE)

#define GTM_MAX_THREADS 512
#define CACHE_LINE_SIZE 64
typedef union rw_lock {
	int lock;
	char padding[CACHE_LINE_SIZE];
} RW_lock;

typedef struct GTM_Transactions
{
	uint32				gt_txn_count;
	GTM_States			gt_gtm_state;

	GTM_RWLock			gt_XidGenLock;

	/*
	 * These fields are protected by XidGenLock
	 */
	GlobalTransactionId gt_nextXid;		/* next XID to assign */
	GlobalTransactionId gt_backedUpXid;	/* backed up, restoration point */

	GlobalTransactionId gt_oldestXid;	/* cluster-wide minimum datfrozenxid */
	GlobalTransactionId gt_xidVacLimit;	/* start forcing autovacuums here */
	GlobalTransactionId gt_xidWarnLimit; /* start complaining here */
	GlobalTransactionId gt_xidStopLimit; /* refuse to advance nextXid beyond here */
	GlobalTransactionId gt_xidWrapLimit; /* where the world ends */

	/*
	 * These fields are protected by TransArrayLock.
	 */
	GlobalTransactionId gt_latestCompletedXid;	/* newest XID that has committed or
										 		 * aborted */

	GlobalTransactionId	gt_recent_global_xmin;

	int32				gt_lastslot;
	GTM_TransactionInfo	gt_transactions_array[GTM_MAX_GLOBAL_TRANSACTIONS];
	gtm_List			*gt_open_transactions;

	GTM_RWLock			gt_TransArrayLock;
	pg_atomic_uint32	gt_global_xid;

	GlobalTimestamp		gt_last_cycle;
	GlobalTimestamp 	gt_global_timestamp;
	/* For debug purpose */
	GlobalTimestamp		gt_last_issue_timestamp;
	GlobalTimestamp 	gt_last_raw_timestamp;
	GlobalTimestamp		gt_last_last_cycle;
	GlobalTimestamp		gt_last_global_timestamp;
	GlobalTimestamp		gt_last_tv_sec;
	GlobalTimestamp		gt_last_tv_nsec;
	pg_atomic_uint64	gt_access_ts_seq;
	pg_atomic_uint64	gt_last_access_ts_seq;
	
	RW_lock				gt_in_locking[GTM_MAX_THREADS];
} GTM_Transactions;

extern GTM_Transactions	GTMTransactions;

/* NOTE: This macro should be used with READ lock held on gt_TransArrayLock! */
#define GTM_CountOpenTransactions()	(gtm_list_length(GTMTransactions.gt_open_transactions))

/*
 * Two hash tables will be maintained to quickly find the
 * GTM_TransactionInfo block given either the GXID or the GTM_TransactionHandle.
 */

GTM_TransactionInfo *GTM_HandleToTransactionInfo(GTM_TransactionHandle handle);
GTM_TransactionHandle GTM_GXIDToHandle(GlobalTransactionId gxid);
GTM_TransactionHandle GTM_GIDToHandle(char *gid);
bool GTM_IsGXIDInProgress(GlobalTransactionId gxid);

/* Transaction Control */
void GTM_InitTxnManager(void);
GTM_TransactionHandle GTM_BeginTransaction(GTM_IsolationLevel isolevel,
										   bool readonly,
										   const char *global_sessionid);
int GTM_BeginTransactionMulti(GTM_IsolationLevel isolevel[],
										   bool readonly[],
										   const char *global_sessionid[],
										   GTMProxy_ConnID connid[],
										   int txn_count,
										   GTM_TransactionHandle txns[]);
int GTM_RollbackTransaction(GTM_TransactionHandle txn);
int GTM_RollbackTransactionMulti(GTM_TransactionHandle txn[], int txn_count, int status[]);
int GTM_RollbackTransactionGXID(GlobalTransactionId gxid);
int GTM_CommitTransaction(GTM_TransactionHandle txn,
		int waited_xid_count, GlobalTransactionId *waited_xids);
int GTM_CommitTransactionMulti(GTM_TransactionHandle txn[], int txn_count,
		int waited_xid_count, GlobalTransactionId *waited_xids,
		int status[]);
int GTM_CommitTransactionGXID(GlobalTransactionId gxid);
int GTM_PrepareTransaction(GTM_TransactionHandle txn);
int GTM_StartPreparedTransaction(GTM_TransactionHandle txn,
								 char *gid,
								 char *nodestring);
int
GTM_LogTransaction(	GlobalTransactionId gxid,
							 const char *gid,
							 const char *nodestring,
							 int node_count,
							 int isGlobal,
							 int isCommit,
							 GlobalTimestamp prepare_ts,
							 GlobalTimestamp commit_ts);
int
GTM_LogScan(GlobalTransactionId gxid,
				 const char *nodestring,
			     GlobalTimestamp start_ts,
			     GlobalTimestamp local_start_ts,
			     GlobalTimestamp local_complete_ts,
			     int scan_type,
			     const char *rel_name,
			     int64 scan_number);


int GTM_StartPreparedTransactionGXID(GlobalTransactionId gxid,
									 char *gid,
									 char *nodestring);
int GTM_GetGIDData(GTM_TransactionHandle prepared_txn,
				   GlobalTransactionId *prepared_gxid,
				   char **nodestring);
uint32 GTM_GetAllPrepared(GlobalTransactionId gxids[], uint32 gxidcnt);
GTM_TransactionStates GTM_GetStatus(GTM_TransactionHandle txn);
GTM_TransactionStates GTM_GetStatusGXID(GlobalTransactionId gxid);
int GTM_GetAllTransactions(GTM_TransactionInfo txninfo[], uint32 txncnt);
void GTM_RemoveAllTransInfos(uint32 client_id, int backend_id);
uint32 GTMGetFirstClientIdentifier(void);
uint32 GTMGetLastClientIdentifier(void);

GTM_Snapshot GTM_GetSnapshotData(GTM_TransactionInfo *my_txninfo,
								 GTM_Snapshot snapshot);
GTM_Snapshot GTM_GetTransactionSnapshot(GTM_TransactionHandle handle[],
		int txn_count, int *status);
void GTM_FreeCachedTransInfo(void);

void ProcessBeginTransactionCommand(Port *myport, StringInfo message);
#ifdef __TBASE__
void
ProcessBkupGlobalTimestamp(Port *myport, StringInfo message);
#endif

void ProcessBkupBeginTransactionCommand(Port *myport, StringInfo message);
void GTM_BkupBeginTransactionMulti(GTM_IsolationLevel *isolevel,
								   bool *readonly,
								   const char **global_sessionid,
								   uint32 *client_id,
								   GTMProxy_ConnID *connid,
								   int	txn_count);

void ProcessBeginTransactionCommandMulti(Port *myport, StringInfo message);
void ProcessBeginTransactionGetGXIDCommand(Port *myport, StringInfo message);
void ProcessCommitTransactionCommand(Port *myport, StringInfo message, bool is_backup);
void ProcessCommitPreparedTransactionCommand(Port *myport, StringInfo message, bool is_backup);
void ProcessRollbackTransactionCommand(Port *myport, StringInfo message, bool is_backup);
void ProcessStartPreparedTransactionCommand(Port *myport, StringInfo message, bool is_backup);
void ProcessLogTransactionCommand(Port *myport, StringInfo message, bool is_global, bool is_backup);
void ProcessLogScanCommand(Port *myport, StringInfo message, bool is_backup);
void ProcessPrepareTransactionCommand(Port *myport, StringInfo message, bool is_backup);
void ProcessGetGIDDataTransactionCommand(Port *myport, StringInfo message);
void ProcessGetGXIDTransactionCommand(Port *myport, StringInfo message);
void ProcessGXIDListCommand(Port *myport, StringInfo message);
void ProcessGetNextGXIDTransactionCommand(Port *myport, StringInfo message);
void ProcessReportXminCommand(Port *myport, StringInfo message, bool is_backup);

void ProcessBeginTransactionGetGXIDAutovacuumCommand(Port *myport, StringInfo message);
void ProcessBkupBeginTransactionGetGXIDAutovacuumCommand(Port *myport, StringInfo message);

void ProcessBeginTransactionGetGXIDCommandMulti(Port *myport, StringInfo message);
void ProcessCommitTransactionCommandMulti(Port *myport, StringInfo message, bool is_backup);
void ProcessRollbackTransactionCommandMulti(Port *myport, StringInfo message, bool is_backup) ;

void GTM_WriteRestorePointVersion(FILE *f);
void GTM_RestoreStart(FILE *ctlf, struct GTM_RestoreContext *context);
void GTM_SaveTxnInfo(FILE *ctlf);
void GTM_RestoreTxnInfo(FILE *ctlf, GlobalTransactionId next_gxid,
		struct GTM_RestoreContext *context, bool force_xid);
void GTM_BkupBeginTransaction(GTM_IsolationLevel isolevel,
							  bool readonly,
							  const char *global_sessionid,
							  uint32 client_id);
void ProcessBkupBeginTransactionGetGXIDCommand(Port *myport, StringInfo message);
void ProcessBkupBeginTransactionGetGXIDCommandMulti(Port *myport, StringInfo message);

GlobalTransactionId
GetNextGlobalTransactionId(void);

/*
 * In gtm_snap.c
 */
void ProcessGetSnapshotCommand(Port *myport, StringInfo message, bool get_gxid);
void ProcessGetSnapshotCommandMulti(Port *myport, StringInfo message);
void GTM_FreeSnapshotData(GTM_Snapshot snapshot);
void GTM_RememberDroppedSequence(GlobalTransactionId gxid, void *seq);
void GTM_ForgetCreatedSequence(GlobalTransactionId gxid, void *seq);
void GTM_RememberCreatedSequence(GlobalTransactionId gxid, void *seq);
void GTM_RememberAlteredSequence(GlobalTransactionId gxid, void *seq);
#ifdef __TBASE__
extern void GTM_RestoreStoreInfo(GlobalTransactionId next_gxid, bool force_xid);
extern void ProcessFinishGIDTransactionCommand(Port *myport, StringInfo message);
void ProcessGetGTSCommand(Port *myport, StringInfo message);
void ProcessGetGTSCommandMulti(Port *myport, StringInfo message);
void ProcessCheckGTMCommand(Port *myport, StringInfo message);
void ProcessStandbyPreCheckGTMCommand(Port *myport, StringInfo message);
#endif

#endif
