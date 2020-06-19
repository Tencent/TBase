/*-------------------------------------------------------------------------
 * slot.h
 *       Replication slot management.
 *
 * Copyright (c) 2012-2017, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef SLOT_H
#define SLOT_H

#include "fmgr.h"
#include "access/xlog.h"
#include "access/xlogreader.h"
#include "storage/condition_variable.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/spin.h"

/*
 * Behaviour of replication slots, upon release or crash.
 *
 * Slots marked as PERSISTENT are crash-safe and will not be dropped when
 * released. Slots marked as EPHEMERAL will be dropped when released or after
 * restarts.
 *
 * EPHEMERAL slots can be made PERSISTENT by calling ReplicationSlotPersist().
 */
typedef enum ReplicationSlotPersistency
{
    RS_PERSISTENT,
    RS_EPHEMERAL,
    RS_TEMPORARY
} ReplicationSlotPersistency;

/*
 * On-Disk data of a replication slot, preserved across restarts.
 */
typedef struct ReplicationSlotPersistentData
{
    /* The slot's identifier */
    NameData    name;

    /* database the slot is active on */
    Oid            database;

    /*
     * The slot's behaviour when being dropped (or restored after a crash).
     */
    ReplicationSlotPersistency persistency;

    /*
     * xmin horizon for data
     *
     * NB: This may represent a value that hasn't been written to disk yet;
     * see notes for effective_xmin, below.
     */
    TransactionId xmin;

    /*
     * xmin horizon for catalog tuples
     *
     * NB: This may represent a value that hasn't been written to disk yet;
     * see notes for effective_xmin, below.
     */
    TransactionId catalog_xmin;

    /* oldest LSN that might be required by this replication slot */
    XLogRecPtr    restart_lsn;

    /*
     * Oldest LSN that the client has acked receipt for.  This is used as the
     * start_lsn point in case the client doesn't specify one, and also as a
     * safety measure to jump forwards in case the client specifies a
     * start_lsn that's further in the past than this value.
     */
    XLogRecPtr    confirmed_flush;

    /* plugin name */
    NameData    plugin;
#ifdef _PUB_SUB_RELIABLE_
    int         slotid;
#endif 
} ReplicationSlotPersistentData;

/*
 * Shared memory state of a single replication slot.
 */
typedef struct ReplicationSlot
{
    /* lock, on same cacheline as effective_xmin */
    slock_t        mutex;

    /* is this slot defined */
    bool        in_use;

    /* Who is streaming out changes for this slot? 0 in unused slots. */
    pid_t        active_pid;

    /* any outstanding modifications? */
    bool        just_dirtied;
    bool        dirty;

    /*
     * For logical decoding, it's extremely important that we never remove any
     * data that's still needed for decoding purposes, even after a crash;
     * otherwise, decoding will produce wrong answers.  Ordinary streaming
     * replication also needs to prevent old row versions from being removed
     * too soon, but the worst consequence we might encounter there is
     * unwanted query cancellations on the standby.  Thus, for logical
     * decoding, this value represents the latest xmin that has actually been
     * written to disk, whereas for streaming replication, it's just the same
     * as the persistent value (data.xmin).
     */
    TransactionId effective_xmin;
    TransactionId effective_catalog_xmin;

    /* data surviving shutdowns and crashes */
    ReplicationSlotPersistentData data;

    /* is somebody performing io on this slot? */
    LWLock        io_in_progress_lock;

    /* Condition variable signalled when active_pid changes */
    ConditionVariable active_cv;

    /* all the remaining data is only used for logical slots */

    /*
     * When the client has confirmed flushes(data.confirmed_flush) >= candidate_xmin_lsn we can
     * advance the candidate_catalog_xmin.  When candidate_restart_valid has been passed,
     * candidate_restart_lsn can be increased.
     */
    TransactionId candidate_catalog_xmin;
    XLogRecPtr    candidate_xmin_lsn;
    XLogRecPtr    candidate_restart_valid;
    XLogRecPtr    candidate_restart_lsn;
#ifdef __STORAGE_SCALABLE__
    bool        pgoutput;
    /*
     * subscription we serve, we can serve only one at same moment
     */
    Oid         subid;      /* oid of subscription at remote */
    NameData    subname;    /* name of subscription at remote */

    /*
     * we only decode this relation within the subscription
     */
    Oid         relid;
    Bitmapset   *shards;  /* shards we interested in */

    /*
     * we decode all tables with shards in publications
     */
    int         npubs;
    bool        *alltables;
    List        **tables;
    Bitmapset   **pubshards;

    /* statistic data */
    uint64     ntups_insert;
    uint64     ntups_delete;
    uint64     checksum_insert;
    uint64     checksum_delete;
#endif
} ReplicationSlot;

#define SlotIsPhysical(slot) (slot->data.database == InvalidOid)
#define SlotIsLogical(slot) (slot->data.database != InvalidOid)

/*
 * Shared memory control area for all of replication slots.
 */
typedef struct ReplicationSlotCtlData
{
    /*
     * This array should be declared [FLEXIBLE_ARRAY_MEMBER], but for some
     * reason you can't do that in an otherwise-empty struct.
     */
    ReplicationSlot replication_slots[1];
} ReplicationSlotCtlData;

/*
 * Pointers to shared memory
 */
extern ReplicationSlotCtlData *ReplicationSlotCtl;
extern PGDLLIMPORT ReplicationSlot *MyReplicationSlot;

/* GUCs */
extern PGDLLIMPORT int max_replication_slots;

/* shmem initialization functions */
extern Size ReplicationSlotsShmemSize(void);
extern void ReplicationSlotsShmemInit(void);

/* management of individual slots */
extern void ReplicationSlotCreate(const char *name, bool db_specific,
                      ReplicationSlotPersistency p);
extern void ReplicationSlotPersist(void);
extern void ReplicationSlotDrop(const char *name, bool nowait);
extern void ReplicationSlotModify(const char *old_name, const char *new_name, bool nowait);

extern void ReplicationSlotAcquire(const char *name, bool nowait);
extern void ReplicationSlotRelease(void);
extern void ReplicationSlotCleanup(void);
extern void ReplicationSlotSave(void);
extern void ReplicationSlotMarkDirty(void);

/* misc stuff */
extern bool ReplicationSlotValidateName(const char *name, int elevel);
extern void ReplicationSlotReserveWal(void);
extern void ReplicationSlotsComputeRequiredXmin(bool already_locked);
extern void ReplicationSlotsComputeRequiredLSN(void);
extern XLogRecPtr ReplicaSlotsComputLogicalRstartLSN(void);
extern bool ReplicationSlotsCountDBSlots(Oid dboid, int *nslots, int *nactive);
extern void ReplicationSlotsDropDBSlots(Oid dboid);

extern void StartupReplicationSlots(void);
extern void CheckPointReplicationSlots(void);

extern void CheckSlotRequirements(void);

#ifdef _PUB_SUB_RELIABLE_
extern void replica_slot_wal_record_lsn_replic(void);
extern void replica_slot_wal_record_creat_slot(void);
extern void replication_slot_wal_record_drop(void);
extern void replication_slot_wal_record_rename_slot(const char * new_name);
#endif

#endif                            /* SLOT_H */
