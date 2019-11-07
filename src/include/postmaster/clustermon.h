/*-------------------------------------------------------------------------
 *
 * clustermon.h
 *      header file for cluster monitor process
 *
 *
 * Portions Copyright (c) 2015, 2ndQuadrant Ltd
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/postmaster/autovacuum.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTERMON_H
#define CLUSTERMON_H

#include "storage/s_lock.h"
#include "gtm/gtm_c.h"

typedef struct
{
    slock_t                mutex;
    GlobalTransactionId    reported_recent_global_xmin;
    GlobalTransactionId    reporting_recent_global_xmin;
    GlobalTransactionId    gtm_recent_global_xmin;
} ClusterMonitorCtlData;

extern void ClusterMonitorShmemInit(void);
extern Size ClusterMonitorShmemSize(void);

/* Status inquiry functions */
extern bool IsClusterMonitorProcess(void);

/* Functions to start cluster monitor process, called from postmaster */
int ClusterMonitorInit(void);
extern int    StartClusterMonitor(void);
extern GlobalTransactionId ClusterMonitorGetGlobalXmin(void);
extern void ClusterMonitorSetGlobalXmin(GlobalTransactionId xmin);
extern GlobalTransactionId ClusterMonitorGetReportingGlobalXmin(void);

#ifdef EXEC_BACKEND
extern void ClusterMonitorIAm(void);
#endif

extern int ClusterMonitorInit(void);

#endif   /* CLUSTERMON_H */
