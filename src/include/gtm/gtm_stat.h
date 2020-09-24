/*-------------------------------------------------------------------------
 *
 * gtm_stat.h
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * Portions Copyright (c) 2012-2018 TBase Development Group
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#ifndef _GTM_STAT_H
#define _GTM_STAT_H

#include "gtm/gtm_c.h"
#include "gtm/gtm_lock.h"
#include "gtm/gtm_msg.h"
#include "gtm/libpq-be.h"
#include "gtm/stringinfo.h"
#include "port/atomics.h"

typedef int64 pg_time_t;
#define CACHE_LINE_SIZE 64
#define CACHE_LINE_ALIGN __attribute__((aligned(CACHE_LINE_SIZE)))

typedef enum GTM_Statistic_Cmd
{
    CMD_GETGTS,
    CMD_SEQUENCE_GET_NEXT,
    CMD_TXN_START_PREPARED,
    CMD_STATISTICS_TYPE_COUNT
} GTM_StatisticsCmd;

typedef struct
{
    pg_atomic_uint32 total_request_times;
    pg_atomic_uint32 total_costtime;
    pg_atomic_uint32 max_costtime;
    pg_atomic_uint32 min_costtime;
} CACHE_LINE_ALIGN GTM_StatisticsInfo;

typedef struct
{
    GTM_StatisticsInfo cmd_statistics[CMD_STATISTICS_TYPE_COUNT];
} GTM_WorkerStatistics;

typedef struct
{
    uint32     total_request_times;
    union
    {
        uint32 total_costtime;
        uint32 avg_costtime;
    };
    uint32     max_costtime;
    uint32     min_costtime;
} GTM_StatisticsItem;

typedef struct
{
    pg_time_t          start_time;                            /* statistics info start time */
    pg_time_t          end_time;                              /* statistics info end time */
    int32              sequences_remained;                    /* sequence remained num */
    int32              txn_remained;                          /* txn remained num */
    GTM_StatisticsItem stat_info[CMD_STATISTICS_TYPE_COUNT];  /* specific cmd statistics info */
} GTM_StatisticsResult;

typedef struct
{
    pg_time_t stat_start_time;      /* statistics info start time */
    s_lock_t  lock;                 /* lock to avoid multi client */
} GTM_Statistics;

extern GTM_Statistics GTMStatistics;

void GTM_InitGtmStatistics(void);

void GTM_InitStatisticsHandle(void);

void GTM_UpdateStatistics(GTM_WorkerStatistics* stat_handle, GTM_MessageType mtype, uint32 costtime);

void ProcessGetStatisticsCommand(Port *myport, StringInfo message);
#endif
