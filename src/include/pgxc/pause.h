/*-------------------------------------------------------------------------
 *
 * pause.h
 *
 *      Definitions for the Pause/Unpause Cluster handling
 *
 * IDENTIFICATION
 *      $$
 *
 *-------------------------------------------------------------------------
 */

#ifndef PAUSE_H
#define PAUSE_H

#include "storage/s_lock.h"

/* Shared memory area for management of cluster pause/unpause */
typedef struct {
    int        cl_holder_pid; /* pid of the process issuing CLUSTER PAUSE */
    int        cl_process_count; /* Number of processes undergoing txns */

    slock_t    cl_mutex; /* locks shared variables mentioned above */
} ClusterLockInfo;

extern ClusterLockInfo *ClustLinfo;

extern bool cluster_lock_held;
extern bool cluster_ex_lock_held;

extern void ClusterLockShmemInit(void);
extern Size ClusterLockShmemSize(void);
extern void AcquireClusterLock(bool exclusive);
extern void ReleaseClusterLock(bool exclusive);

extern void RequestClusterPause(bool pause, char *completionTag);
extern void PGXCCleanClusterLock(int code, Datum arg);
#endif
