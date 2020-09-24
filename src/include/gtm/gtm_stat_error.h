/*-------------------------------------------------------------------------
 *
 * gtm_stat_error.h

 *	  collect error logs of gtm
 *
 * Copyright (c) 2020-Present TBase development team, Tencent
 *
 * IDENTIFICATION
 *	  src/gtm/main/gtm_stat_error.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _GTM_STAT_ERROR_H
#define _GTM_STAT_ERROR_H

#include "gtm/gtm_c.h"
#include "gtm/gtm_lock.h"
#include "gtm/datapump.h"
#include "gtm/bloom.h"

#define GTM_MAX_ERRMSG_SIZE (1024) 	/* max size of each error msg to track */
#define GTM_BLOOM_FILTER_SIZE (1 * 1024 * 1024)
#define GTM_GLOBAL_ERRLOG_DATAPUMP_SIZE (10 * 1024) /* k */
#define GTM_THREAD_ERRLOG_DATAPUMP_SIZE (16) /* k */

typedef int64 pg_time_t;

typedef struct
{
    int			proc_id;		/* process id */
    int			error_no;		/* errno  */
    pg_time_t   log_time;		/* log time */
    int		 	err_level;		/* error level */
    int			errmsg_len;		/* length of valid bytes in error message */
    char		errmsg[0];		/* variable length array - must be last */
} GTM_ErrLog;

typedef struct
{
    s_lock_t         lock;               /* lock to avoid multi client */
    pg_atomic_uint32 full;               /* datapump is full */
    char             *tmp_buff;          /* a buff use to read tuple data */
    BLOOM            *bloom_filter;      /* bloom filter use to exclude duplicates */
    DataPumpBuf      *datapump_buff;     /* circular queue buffer */
} GTM_LogCollector;

extern GTM_LogCollector GlobalLogCollector;

DataPumpBuf *GTM_BuildDataPumpBuf(uint32 size);
void GTM_DestroyDataPumpBuf(DataPumpBuf *buff);
int GTM_InitLogCollector(void);
void GTM_DeInitLogCollector(void);
void GTM_ProcessLogCollection(void);
void ProcessGetErrorlogCommand(Port *myport, StringInfo message);
#endif
