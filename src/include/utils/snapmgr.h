/*-------------------------------------------------------------------------
 *
 * snapmgr.h
 *	  POSTGRES snapshot manager
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/snapmgr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SNAPMGR_H
#define SNAPMGR_H

#include "fmgr.h"
#include "utils/relcache.h"
#include "utils/resowner.h"
#include "utils/snapshot.h"
#include "miscadmin.h"
#include "utils/timestamp.h"



/*
 * The structure used to map times to TransactionId values for the "snapshot
 * too old" feature must have a few entries at the tail to hold old values;
 * otherwise the lookup will often fail and the expected early pruning or
 * vacuum will not usually occur.  It is best if this padding is for a number
 * of minutes greater than a thread would normally be stalled, but it's OK if
 * early vacuum opportunities are occasionally missed, so there's no need to
 * use an extreme value or get too fancy.  10 minutes seems plenty.
 */
#define OLD_SNAPSHOT_PADDING_ENTRIES 10
#define OLD_SNAPSHOT_TIME_MAP_ENTRIES (old_snapshot_threshold + OLD_SNAPSHOT_PADDING_ENTRIES)

typedef enum SnapshotStatus
{
	S_DEFAULT,
	S_FOR_CTAS			/* After creating a table, obtain a new snapshot in the QueryRewriteCTAS process */
} SnapshotStatus;

/*
 * Common definition of relation properties that allow early pruning/vacuuming
 * when old_snapshot_threshold >= 0.
 */
#define RelationAllowsEarlyPruning(rel) \
( \
	 RelationNeedsWAL(rel) \
  && !IsCatalogRelation(rel) \
  && !RelationIsAccessibleInLogicalDecoding(rel) \
  && !RelationHasUnloggedIndex(rel) \
)

#define EarlyPruningEnabled(rel) (old_snapshot_threshold >= 0 && RelationAllowsEarlyPruning(rel))

/* GUC variables */
extern PGDLLIMPORT int old_snapshot_threshold;


extern Size SnapMgrShmemSize(void);
extern void SnapMgrInit(void);
extern TimestampTz GetSnapshotCurrentTimestamp(void);
extern TimestampTz GetOldSnapshotThresholdTimestamp(void);

extern bool FirstSnapshotSet;

extern TransactionId TransactionXmin;
extern TransactionId RecentXmin;
extern PGDLLIMPORT TransactionId RecentGlobalXmin;
extern TransactionId RecentGlobalDataXmin;

extern GlobalTimestamp RecentCommitTs;
extern GlobalTimestamp RecentDataTs;
extern int	vacuum_delta;
extern bool vacuum_debug_print;


#ifdef _SHARDING_
extern Snapshot GetTransactionSnapshot_shard(bool need_shardmap, bool latest);
#define GetTransactionSnapshot() GetTransactionSnapshot_shard(true, false)
#define GetLocalTransactionSnapshot() GetTransactionSnapshot_shard(true, true)
#define GetTransactionSnapshot_without_shard() GetTransactionSnapshot_shard(false, false)
#endif
extern Snapshot GetLatestSnapshot(void);
extern void SnapshotSetCommandId(CommandId curcid);
extern Snapshot GetOldestSnapshot(void);

extern Snapshot GetCatalogSnapshot(Oid relid);
extern Snapshot GetNonHistoricCatalogSnapshot(Oid relid);
extern void InvalidateCatalogSnapshot(void);
extern void InvalidateCatalogSnapshotConditionally(void);

extern void PushActiveSnapshot(Snapshot snapshot);
extern void PushCopiedSnapshot(Snapshot snapshot);
extern void UpdateActiveSnapshotCommandId(void);
void UpdateActiveSnapshotStatus(SnapshotStatus new_status);
SnapshotStatus GetActiveSnapshotStatus(void);
extern int GetActiveSnapshotLevel(void);
extern void SetActiveSnapshotLevel(int level);
extern void PopActiveSnapshot(void);
extern Snapshot GetActiveSnapshot(void);
extern bool ActiveSnapshotSet(void);

extern Snapshot RegisterSnapshot(Snapshot snapshot);
extern void UnregisterSnapshot(Snapshot snapshot);
extern Snapshot RegisterSnapshotOnOwner(Snapshot snapshot, ResourceOwner owner);
extern void UnregisterSnapshotFromOwner(Snapshot snapshot, ResourceOwner owner);

extern void AtSubCommit_Snapshot(int level);
extern void AtSubAbort_Snapshot(int level);
extern void AtEOXact_Snapshot(bool isCommit, bool resetXmin);

extern void ImportSnapshot(const char *idstr);
extern bool XactHasExportedSnapshots(void);
extern void DeleteAllExportedSnapshotFiles(void);
extern bool ThereAreNoPriorRegisteredSnapshots(void);
extern TransactionId TransactionIdLimitedForOldSnapshots(TransactionId recentXmin,
									Relation relation);
extern void MaintainOldSnapshotTimeMapping(TimestampTz whenTaken,
							   TransactionId xmin);

extern char *ExportSnapshot(Snapshot snapshot);

/* Support for catalog timetravel for logical decoding */
struct HTAB;
extern struct HTAB *HistoricSnapshotGetTupleCids(void);
extern void SetupHistoricSnapshot(Snapshot snapshot_now, struct HTAB *tuplecids);
extern void TeardownHistoricSnapshot(bool is_error);
extern bool HistoricSnapshotActive(void);

extern Size EstimateSnapshotSpace(Snapshot snapshot);
extern void SerializeSnapshot(Snapshot snapshot, char *start_address);
extern Snapshot RestoreSnapshot(char *start_address);
extern void RestoreTransactionSnapshot(Snapshot snapshot, void *master_pgproc);

/* Support for snapshot checking */
#ifdef __TBASE_DEBUG__

extern Size SnapTableShmemSize(void);
extern void InitSnapBufTable(void);
extern void InsertPreparedXid(TransactionId xid, GlobalTimestamp prepare_timestamp);
extern void DeletePreparedXid(TransactionId xid);
extern bool LookupPreparedXid(TransactionId xid, GlobalTimestamp *prepare_timestamp);



#endif



static inline bool
TestForOldTimestamp(GlobalTimestamp currentTimestamp, GlobalTimestamp oldestTimestamp)
{
	
	if(IsInitProcessingMode())
	{
		return true;
	}

	if(CommitTimestampIsLocal(currentTimestamp))
	{
		return true;
	}
	
	if(currentTimestamp < oldestTimestamp)
	{
		elog(DEBUG12, "test for old time true ts " INT64_FORMAT " recent " INT64_FORMAT, currentTimestamp, oldestTimestamp);
		return true;
	}
	else
	{
		elog(DEBUG12, "test for old time false ts " INT64_FORMAT " recent " INT64_FORMAT, currentTimestamp, oldestTimestamp);
		return false;
	}
}


#endif							/* SNAPMGR_H */
