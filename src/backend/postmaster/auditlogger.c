/*
 * Tencent is pleased to support the open source community by making TBase available.  
 * 
 * Copyright (C) 2019 THL A29 Limited, a Tencent company.  All rights reserved.
 * 
 * TBase is licensed under the BSD 3-Clause License, except for the third-party component listed below. 
 * 
 * A copy of the BSD 3-Clause License is included in this file.
 * 
 * Other dependencies and licenses:
 * 
 * Open Source Software Licensed Under the PostgreSQL License: 
 * --------------------------------------------------------------------
 * 1. Postgres-XL XL9_5_STABLE
 * Portions Copyright (c) 2015-2016, 2ndQuadrant Ltd
 * Portions Copyright (c) 2012-2015, TransLattice, Inc.
 * Portions Copyright (c) 2010-2017, Postgres-XC Development Group
 * Portions Copyright (c) 1996-2015, The PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, The Regents of the University of California
 * 
 * Terms of the PostgreSQL License: 
 * --------------------------------------------------------------------
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written agreement
 * is hereby granted, provided that the above copyright notice and this
 * paragraph and the following two paragraphs appear in all copies.
 * 
 * IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
 * DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * 
 * THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
 * ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 * 
 * 
 * Terms of the BSD 3-Clause License:
 * --------------------------------------------------------------------
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 * 
 * 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation 
 * and/or other materials provided with the distribution.
 * 
 * 3. Neither the name of THL A29 Limited nor the names of its contributors may be used to endorse or promote products derived from this software without 
 * specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, 
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS 
 * BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE 
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT 
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH 
 * DAMAGE.
 * 
 */
/*-------------------------------------------------------------------------
 *
 * auditlogger.c
 *
 * Copyright (c) 2004-2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *      src/backend/postmaster/auditlogger.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include "lib/stringinfo.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "pgstat.h"
#include "pgtime.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "postmaster/auditlogger.h"
#include "postmaster/syslogger.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/pg_shmem.h"
#include "utils/guc.h"
#include "utils/ps_status.h"
#include "utils/timestamp.h"
#include "storage/pmsignal.h"
#include "storage/spin.h"
#include "storage/fd.h"
#include "pgxc/squeue.h"
#include "utils/resowner.h"
#include "utils/palloc.h"
#include "utils/memutils.h"
#include "storage/procarray.h"
#include "pgxc/squeue.h"
#include "pgxc/shardmap.h"

#define        BYTES_PER_KB            1024
#define        AUDIT_BITMAP_WORD        (WORDNUM(MaxBackends) + 1)
#define        AUDIT_BITMAP_SIZE        (BITMAPSET_SIZE(AUDIT_BITMAP_WORD))

#define        AUDIT_SLEEP_MICROSEC    100000L
#define        AUDIT_LATCH_MICROSEC    10000000L

// #define     Use_Audit_Assert         0

#define     AuditLog_001_For_Main                0
#define        AuditLog_002_For_ShareMemoryQueue    0
#define        AuditLog_003_For_LogFile            0
#define        AuditLog_004_For_QueueReadWrite        0
#define        AuditLog_005_For_ThreadWorker        0
#define        AuditLog_006_For_Elog                0
#define        AuditLog_007_For_ShardStatistics    0

#ifdef Use_Audit_Assert
    #ifdef Trap
        #undef Trap
        #define Trap(condition, errorType) \
            do { \
                if (condition) \
                    ExceptionalCondition(CppAsString(condition), (errorType), \
                                         __FILE__, __LINE__); \
            } while (0)
    #endif

    #ifdef Assert
        #undef Assert
        #define Assert(condition) \
            Trap(!(condition), "FailedAssertion")
    #endif
#endif

typedef struct AuditLogQueue
{
	pid_t					q_pid;
	int						q_size;
	slock_t					q_lock;
	volatile int			q_head;
	volatile int			q_tail;
	char					q_area[FLEXIBLE_ARRAY_MEMBER];
} AlogQueue;

typedef struct AuditLogQueueArray
{
    int                        a_count;
    Bitmapset              *    a_bitmap;
    AlogQueue              *    a_queue[FLEXIBLE_ARRAY_MEMBER];
} AlogQueueArray;

typedef struct AuditLogQueueCache
{
	/* local ThreadSema for CommonLogWriter, FGALogWriter and TraceLogWriter. */
	ThreadSema				q_sema;
	int						q_count;
	AlogQueue			  * q_cache[FLEXIBLE_ARRAY_MEMBER];
} AlogQueueCache;

/*
 * shared memory queue array
 *
 * store common audit logs, each elem for a backend
 */
static AlogQueueArray	  * AuditCommonLogQueueArray = NULL;
/* store fga audit logs, each elem for a backend */
static AlogQueueArray	  * AuditFGALogQueueArray = NULL;
/* store trace audit logs, each elem for a backend */
static AlogQueueArray	  * AuditTraceLogQueueArray = NULL;

/*
 * shared memory bitmap to notify consumers to read audit log from AlogQueueArray above
 * each element for one consumer
 */
static int                  *    AuditConsumerNotifyBitmap = NULL;

/*
 * Postgres backend state, used in postgres backend only
 *
 * Postgres backend write common audit log into AuditCommonLogQueueArray->a_queue[idx]
 * and write fga audit log info AuditFGALogQueueArray->a_queue[idx]
 * and write trace audit log info AuditTraceLogQueueArray->a_queue[idx]
 *
 * Postgres backend acqurie free index by AuditLoggerQueueAcquire
 *
 */
static int                    AuditPostgresAlogQueueIndex = 0;

/*
 * Consumer local queue cache for AuditLog_max_worker_number consumers, used in
 * audit logger process only.
 *
 * store common audit logs, each elem for a thread Consumer
 */
static AlogQueueCache	  * AuditCommonLogLocalCache = NULL;
/* store fga audit logs, each elem for a thread Consumer */
static AlogQueueCache	  * AuditFGALogLocalCache = NULL;
/* store trace audit logs, each elem for a thread Consumer */
static AlogQueueCache	  * AuditTraceLogLocalCache = NULL;

/*
 * local ThreadSema array for AuditLog_max_worker_number consumers, used in audit
 * logger process only.
 *
 * each elem for a thread Consumer.
 */
static ThreadSema		  * AuditConsumerNotifySemas = NULL;

/*
 * GUC parameters.    can change at SIGHUP.
 */
int							AuditLog_RotationAge = HOURS_PER_DAY * MINS_PER_HOUR;
int							AuditLog_RotationSize = 10 * 1024;
char					  * AuditLog_filename = NULL;
static char				  * TraceLog_filename = "maintain-%A-%H.log";
bool						AuditLog_truncate_on_rotation = false;
int							AuditLog_file_mode = S_IRUSR | S_IWUSR;

/* max number of worker thead to read audit log */
int							AuditLog_max_worker_number = 16;
/* size of AlogQueue->q_area for each backend to store common audit log, KB */
int							AuditLog_common_log_queue_size_kb = 64;
/* size of AlogQueue->q_area for each backend to store fga audit log, KB */
int							AuditLog_fga_log_queue_size_kb = 64;
/* size of AlogQueue->q_area for each backend to store trace audit log, KB */
int							Maintain_trace_log_queue_size_kb = 64;
/* size of common audit log local buffer for each worker */
int							AuditLog_common_log_cache_size_kb = 64;
/* size of fga audit log local buffer for each worker */
int							AuditLog_fga_log_cacae_size_kb = 64;
/* size of trace audit log local buffer for each worker */
int							Maintain_trace_log_cache_size_kb = 64;

/*
 * Globally visible state
 */
bool                        am_auditlogger = false;
bool                        enable_auditlogger_warning = false;

/*
 * Logger Private state
 */
static pg_time_t			audit_next_rotation_time = 0;
static bool					audit_rotation_disabled = false;
static FILE				  * audit_comm_log_file = NULL;
static FILE				  * audit_fga_log_file = NULL;
static FILE				  * audit_trace_log_file = NULL;
static slock_t				audit_comm_log_file_lock;
static slock_t				audit_fga_log_file_lock;
static slock_t				audit_trace_log_file_lock;
NON_EXEC_STATIC pg_time_t	audit_first_log_file_time = 0;
static char				  * audit_last_comm_log_file_name = NULL;
static char				  * audit_last_fga_log_file_name = NULL;
static char				  * audit_last_trace_log_file_name = NULL;
static char				  * audit_log_directory = NULL;
static char				  * trace_log_directory = NULL;
static char				  * audit_curr_log_dir = NULL;
static char				  * trace_curr_log_dir = NULL;
static char				  * audit_curr_log_file_name = NULL;
static int					audit_curr_log_rotation_age = 0;

/*
 * Flags set by interrupt handlers for later service in the main loop.
 */
static volatile sig_atomic_t audit_gotSIGHUP = false;            /* pg_ctl reload */
static volatile sig_atomic_t audit_gotSIGQUIT = false;            /* pg_ctl stop -m immediate */
static volatile sig_atomic_t audit_gotSIGTERM = false;            /* pg_ctl stop -m smart */
static volatile sig_atomic_t audit_gotSIGINT= false;            /* pg_ctl stop -m fast */
static volatile sig_atomic_t audit_rotation_requested = false;    /* SIGUSR1, killed by pg_rotate_audit_logfile() */
static volatile sig_atomic_t audit_consume_requested = false;    /* SIGUSR2, killed by alog() */

#ifdef AuditLog_001_For_Main

/* Local subroutines */
#ifdef EXEC_BACKEND
static pid_t                auditlogger_forkexec(void);
#endif
NON_EXEC_STATIC void        AuditLoggerMain(int argc, char *argv[]) pg_attribute_noreturn();

static void        audit_logger_MainLoop(void);

static void        audit_sigHupHandler(SIGNAL_ARGS);
static void        audit_sigUsr1Handler(SIGNAL_ARGS);
static void        audit_sigUsr2Handler(SIGNAL_ARGS);
static void        audit_sigQuitHandler(SIGNAL_ARGS);
static void        audit_sigTermHandler(SIGNAL_ARGS);
static void        audit_sigIntHandler(SIGNAL_ARGS);

static void        audit_process_sighup(void);
static void        audit_process_sigusr1(void);
static void        audit_process_sigusr2(void);
static void        audit_process_sigterm(void);
static void        audit_process_sigint(void);
static void        audit_process_sigquit(void);
static void        audit_process_rotate(void);
static void        audit_process_wakeup(bool timeout);

static void        audit_assign_log_dir(void);
#endif

#ifdef AuditLog_002_For_ShareMemoryQueue
static Size 	audit_queue_elem_size(int queue_size_kb);

static Size		audit_shared_queue_array_bitmap_offset(void);
static Size		audit_shared_queue_array_header_size(void);
static Size		audit_shared_common_queue_elem_size(void);
static Size		audit_shared_common_queue_array_size(void);
static Size		audit_shared_fga_queue_elem_size(void);
static Size		audit_shared_fga_queue_array_size(void);
static Size		audit_shared_trace_queue_elem_size(void);
static Size		audit_shared_trace_queue_array_size(void);
static Size 	audit_shared_consumer_bitmap_size(void);
static int 		audit_shared_consumer_bitmap_get_value(int consumer_id);
static void 	audit_shared_consumer_bitmap_set_value(int consumer_id, int value);
#endif

#ifdef AuditLog_003_For_LogFile
static int		audit_write_log_file(const char *buffer, int count, int destination);
static FILE *	audit_open_log_file(const char *filename, const char *mode, bool allow_errors);
static void		audit_open_fga_log_file(void);
static void		audit_open_trace_log_file(void);
static void		audit_rotate_log_file(bool time_based_rotation, int size_rotation_for);
static char *	audit_log_file_getname(pg_time_t timestamp, const char *suffix);
static char *	trace_log_file_getname(pg_time_t timestamp, const char *suffix);
static void		audit_set_next_rotation_time(void);
#endif

#ifdef AuditLog_004_For_QueueReadWrite
static void     alog_just_caller(void * var);
static void     alog_queue_init(AlogQueue * queue, int queue_size_kb);
static char *     alog_queue_offset_to(AlogQueue * queue, int offset);
static bool     alog_queue_is_full(int q_size, int q_head, int q_tail);
static bool     alog_queue_is_empty(int q_size, int q_head, int q_tail);
static bool     alog_queue_is_empty2(AlogQueue * queue);
static bool     alog_queue_is_enough(int q_size, int q_head, int q_tail, int N);
static int         alog_queue_used(int q_size, int q_head, int q_tail);
static int         alog_queue_remain(int q_size, int q_head, int q_tail);
static bool     alog_queue_push(AlogQueue * queue, char * buff, int len);
static bool     alog_queue_push2(AlogQueue * queue, char * buff1, int len1, char * buff2, int len2);
static bool     alog_queue_pushn(AlogQueue * queue, char * buff[], int len[], int n);
static int         alog_queue_get_str_len(AlogQueue * queue, int offset);
static bool     alog_queue_pop_to_queue(AlogQueue * from, AlogQueue * to);
static bool     alog_queue_pop_to_file(AlogQueue * from, int destination);
#endif

#ifdef AuditLog_005_For_ThreadWorker
static AlogQueue *		alog_get_shared_common_queue(int idx);
static AlogQueue * 		alog_get_shared_fga_queue(int idx);
static AlogQueue * 		alog_get_shared_trace_queue(int idx);
static AlogQueue *		alog_get_local_common_cache(int consumer_id);
static AlogQueue *		alog_get_local_fga_cache(int consumer_id);
static AlogQueue *		alog_get_local_trace_cache(int consumer_id);
static AlogQueueCache * alog_make_local_cache(int cache_number, int queue_size_kb);
static ThreadSema *        alog_make_consumer_semas(int consumer_count);
static void             alog_consumer_wakeup(int consumer_id);
static void             alog_consumer_sleep(int consumer_id);
static void *            alog_consumer_main(void * arg);
static void             alog_writer_wakeup(int writer_destination);
static void             alog_writer_sleep(int writer_destination);
static void *            alog_writer_main(void * arg);
static void                alog_start_writer(int writer_destination);
static void                alog_start_consumer(int consumer_id);
static void                alog_start_all_worker(void);
#endif

#ifdef AuditLog_006_For_Elog
#endif

#ifdef AuditLog_007_For_ShardStatistics
static void *            alog_shard_stat_main(void * arg);
static void                alog_start_shard_stat_worker(void);
#endif

#ifdef AuditLog_001_For_Main
/*
 * Postmaster subroutine to start a auditlogger subprocess.
 */
int
AuditLogger_Start(void)
{
    pid_t        auditLoggerPid = 0;

    if (AuditLog_max_worker_number > MaxConnections)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                     errmsg("alog_max_worker_number can't be bigger than max_connections")));
        exit(1);
    }

#ifdef EXEC_BACKEND
    switch ((auditLoggerPid = auditlogger_forkexec()))
#else
    switch ((auditLoggerPid = fork_process()))
#endif
    {
        case -1:
            ereport(LOG,
                    (errmsg("could not fork audit logger: %m")));
            return 0;

#ifndef EXEC_BACKEND
        case 0:
            /* in postmaster child ... */
            InitPostmasterChild();

            /* Close the postmaster's sockets */
            ClosePostmasterPorts(false);

            /* Drop our connection to postmaster's shared memory, as well */
            dsm_detach_all();

            /* do the work */
            AuditLoggerMain(0, NULL);
            break;
#endif

        default:
            /* success, in postmaster */
            return (int) auditLoggerPid;
    }

    /* we should never reach here */
    return 0;
}


#ifdef EXEC_BACKEND

/*
 * auditlogger_forkexec() -
 *
 * Format up the arglist for, then fork and exec, a auditlogger process
 */
static pid_t
auditlogger_forkexec(void)
{
    char       *av[10] = { 0 };
    int            ac = 0;

    av[ac++] = "postgres";
    av[ac++] = "--forkalog";
    av[ac++] = NULL;            /* filled in by postmaster_forkexec */

    av[ac] = NULL;
    Assert(ac < lengthof(av));

    return postmaster_forkexec(ac, av);
}

#endif                            /* EXEC_BACKEND */

/*
 * Main entry point for auditlogger process
 * argc/argv parameters are valid only in EXEC_BACKEND case.
 */
NON_EXEC_STATIC void
AuditLoggerMain(int argc, char *argv[])
{
    am_auditlogger = true;
    
    /*
     * Properly accept or ignore signals the postmaster might send us
     *
     * Note: we ignore all termination signals, and instead exit only when all
     * upstream processes are gone, to ensure we don't miss any dying gasps of
     * broken backends...
     */

    pqsignal(SIGHUP, audit_sigHupHandler);    /* set flag to read config file */
    pqsignal(SIGINT, audit_sigIntHandler);
    pqsignal(SIGTERM, audit_sigTermHandler);
    pqsignal(SIGQUIT, audit_sigQuitHandler);
    pqsignal(SIGALRM, SIG_IGN);
    pqsignal(SIGPIPE, SIG_IGN);
    pqsignal(SIGUSR1, audit_sigUsr1Handler);    /* request log rotation */
    pqsignal(SIGUSR2, audit_sigUsr2Handler);    /* request consumer to read audit log */

    /*
     * Reset some signals that are accepted by postmaster but not here
     */
    pqsignal(SIGCHLD, SIG_DFL);
    pqsignal(SIGTTIN, SIG_DFL);
    pqsignal(SIGTTOU, SIG_DFL);
    pqsignal(SIGCONT, SIG_DFL);
    pqsignal(SIGWINCH, SIG_DFL);

    CurrentResourceOwner = ResourceOwnerCreate(NULL, "Audit Logger");
    CurrentMemoryContext = AllocSetContextCreate(TopMemoryContext,
                                                 "Audit Logger",
                                                 ALLOCSET_DEFAULT_SIZES);

    /* We allow SIGQUIT (quickdie) at all times */
    sigdelset(&BlockSig, SIGQUIT);

    /*
     * Unblock signals (they were blocked when the postmaster forked us)
     */
    PG_SETMASK(&UnBlockSig);

    /*
     * Identify myself via ps
     */
    init_ps_display("audit logger process", "", "", "");
    
    audit_logger_MainLoop();
    
    exit(0);
}

/*
 * audit_logger_MainLoop
 *
 * Main loop for audit logger process
 */
static void
audit_logger_MainLoop(void)
{
	/*
	 * Create log directory if not present; ignore errors
	 */
	audit_assign_log_dir();
	mkdir(audit_log_directory, S_IRWXU);
	mkdir(trace_log_directory, S_IRWXU);

	/*
	 * Remember active logfile's name.	We recompute this from the reference
	 * time because passing down just the pg_time_t is a lot cheaper than
	 * passing a whole file path in the EXEC_BACKEND case.
	 */
	audit_first_log_file_time = time(NULL);
	audit_last_comm_log_file_name = audit_log_file_getname(audit_first_log_file_time, NULL);
	audit_comm_log_file = audit_open_log_file(audit_last_comm_log_file_name, "a", false);
	audit_open_fga_log_file();
	audit_open_trace_log_file();

	/* remember active logfile parameters */
	audit_curr_log_dir = pstrdup(audit_log_directory);
	trace_curr_log_dir= pstrdup(trace_log_directory);
	audit_curr_log_file_name = pstrdup(AuditLog_filename);
	audit_curr_log_rotation_age = AuditLog_RotationAge;

	SpinLockInit(&(audit_comm_log_file_lock));
	SpinLockInit(&(audit_fga_log_file_lock));
	SpinLockInit(&(audit_trace_log_file_lock));

	/* set next planned rotation time */
	audit_set_next_rotation_time();

	/* start consumer and writer thread*/
	alog_start_all_worker();

	/* main worker loop */
	while (PostmasterIsAlive())
	{
		int	rc = 0;

		/* Clear any already-pending wakeups */
		ResetLatch(MyLatch);

		audit_process_sighup();
		audit_process_sigusr1();
		audit_process_sigusr2();
		audit_process_sigterm();
		audit_process_sigint();
		audit_process_sigquit();
		audit_process_rotate();
		audit_process_wakeup(false);

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   AUDIT_LATCH_MICROSEC,
					   WAIT_EVENT_AUDIT_LOGGER_MAIN);

		if (rc & WL_POSTMASTER_DEATH)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("audit logger exit after postmaster die")));
			exit(2);
		}
		else if (rc & WL_TIMEOUT)
		{
			audit_consume_requested = true;
			audit_process_wakeup(true);
		}
	}
}

/* --------------------------------
 *        signal handler routines
 * --------------------------------
 */

/* SIGHUP: set flag to reload config file */
static void
audit_sigHupHandler(SIGNAL_ARGS)
{
    int            save_errno = errno;

    if (enable_auditlogger_warning)
    {
        ereport(LOG,
                (errmsg("audit logger %d got SIGHUP in audit_sigHupHandler: %m", getpid())));
    }

    /* select pg_reload_conf(); */
    audit_gotSIGHUP = true;
    SetLatch(MyLatch);

    errno = save_errno;
}

/* SIGUSR1: set flag to rotate logfile */
static void
audit_sigUsr1Handler(SIGNAL_ARGS)
{
    int            save_errno = errno;

    if (enable_auditlogger_warning)
    {
        ereport(LOG,
                (errmsg("audit logger %d got SIGUSR1 in audit_sigUsr1Handler: %m", getpid())));
    }

    /* select pg_rotate_audit_logfile(); */
    audit_rotation_requested = true;
    SetLatch(MyLatch);

    errno = save_errno;
}

/* SIGUSR2: set flag to wakeup consumers to read audit log */
static void
audit_sigUsr2Handler(SIGNAL_ARGS)
{
    int            save_errno = errno;

    if (enable_auditlogger_warning)
    {
        ereport(LOG,
                (errmsg("audit logger %d got SIGUSR2 in audit_sigUsr2Handler: %m", getpid())));
    }

    audit_consume_requested = true;
    SetLatch(MyLatch);

    errno = save_errno;
}

/* SIGQUIT signal handler for audit logger process */
static void
audit_sigQuitHandler(SIGNAL_ARGS)
{
    int            save_errno = errno;

    if (enable_auditlogger_warning)
    {
        ereport(LOG,
                (errmsg("audit logger %d got SIGQUIT in audit_sigQuitHandler: %m", getpid())));
    }

    /* pg_ctl stop -m immediate */
    audit_gotSIGQUIT = true;
    SetLatch(MyLatch);

    errno = save_errno;
}

/* SIGTERM signal handler for audit logger process */
static void
audit_sigTermHandler(SIGNAL_ARGS)
{
    int            save_errno = errno;

    if (enable_auditlogger_warning)
    {
        ereport(LOG,
                (errmsg("audit logger %d got SIGTERM in audit_sigTermHandler: %m", getpid())));
    }

    /* pg_ctl stop */
    audit_gotSIGTERM = true;
    SetLatch(MyLatch);

    errno = save_errno;
}

/* SIGINT signal handler for audit logger process */
static void
audit_sigIntHandler(SIGNAL_ARGS)
{
    int            save_errno = errno;

    if (enable_auditlogger_warning)
    {
        ereport(LOG,
                (errmsg("audit logger %d got SIGINT in audit_sigIntHandler: %m", getpid())));
    }

    /* pg_ctl stop -m fast */
    audit_gotSIGINT = true;
    SetLatch(MyLatch);

    errno = save_errno;
}

static void audit_process_sighup(void)
{
    /*
     * Process any requests or signals received recently.
     */
    if (audit_gotSIGHUP)
    {
        audit_gotSIGHUP = false;
        ProcessConfigFile(PGC_SIGHUP);

        audit_assign_log_dir();

        /*
         * Check if the log directory or filename pattern changed in
         * postgresql.conf. If so, force rotation to make sure we're
         * writing the logfiles in the right place.
         */
        if (strcmp(audit_log_directory, audit_curr_log_dir) != 0)
        {
            pfree(audit_curr_log_dir);
            audit_curr_log_dir = pstrdup(audit_log_directory);
            audit_rotation_requested = true;

            /*
             * Also, create new directory if not present; ignore errors
             */
            mkdir(audit_log_directory, S_IRWXU);
        }

        if (strcmp(AuditLog_filename, audit_curr_log_file_name) != 0)
        {
            pfree(audit_curr_log_file_name);
            audit_curr_log_file_name = pstrdup(AuditLog_filename);
            audit_rotation_requested = true;
        }

        /*
         * If rotation time parameter changed, reset next rotation time,
         * but don't immediately force a rotation.
         */
        if (audit_curr_log_rotation_age != AuditLog_RotationAge)
        {
            audit_curr_log_rotation_age = AuditLog_RotationAge;
            audit_set_next_rotation_time();
        }

        /*
         * If we had a rotation-disabling failure, re-enable rotation
         * attempts after SIGHUP, and force one immediately.
         */
        if (audit_rotation_disabled)
        {
            audit_rotation_disabled = false;
            audit_rotation_requested = true;
        }
    }
}

static void audit_process_sigusr1(void)
{
    /* sigusr1 was processed by audit_process_rotate */
}

static void audit_process_sigusr2(void)
{
    /* sigusr2 was processed by audit_process_wakeup */
}

static void audit_process_sigterm(void)
{
    if (audit_gotSIGTERM)
    {
        audit_gotSIGTERM = false;
        proc_exit(3);
    }
}

static void audit_process_sigint(void)
{
    if (audit_gotSIGINT)
    {
        audit_gotSIGINT = false;
        proc_exit(4);
    }
}

static void audit_process_sigquit(void)
{
    if (audit_gotSIGQUIT)
    {
        audit_gotSIGQUIT = false;
        proc_exit(5);
    }
}

static void audit_process_rotate(void)
{
	bool		time_based_rotation = false;
	int			size_rotation_for = 0;
	pg_time_t	now = MyStartTime;

	if (AuditLog_RotationAge > 0 && !audit_rotation_disabled)
	{
		/* Do a logfile rotation if it's time */
		now = (pg_time_t) time(NULL);
		if (now >= audit_next_rotation_time)
			audit_rotation_requested = time_based_rotation = true;
	}

	if (!audit_rotation_requested && AuditLog_RotationSize > 0 && !audit_rotation_disabled)
	{
		/* Do a rotation if file is too big */
		if (ftell(audit_comm_log_file) >= AuditLog_RotationSize * 1024L)
		{
			audit_rotation_requested = true;
			size_rotation_for |= AUDIT_COMMON_LOG;
		}

		if (audit_fga_log_file != NULL &&
			ftell(audit_fga_log_file) >= AuditLog_RotationSize * 1024L)
		{
			audit_rotation_requested = true;
			size_rotation_for |= AUDIT_FGA_LOG;
		}

		if (audit_trace_log_file != NULL &&
			ftell(audit_trace_log_file) >= AuditLog_RotationSize * 1024L)
		{
			audit_rotation_requested = true;
			size_rotation_for |= MAINTAIN_TRACE_LOG;
		}
	}

	if (audit_rotation_requested)
	{
		/*
		 * Force rotation when both values are zero. It means the request
		 * was sent by pg_rotate_log_file.
		 */
		if (!time_based_rotation && size_rotation_for == 0)
		{
			size_rotation_for = AUDIT_COMMON_LOG | AUDIT_FGA_LOG | MAINTAIN_TRACE_LOG;
		}
		audit_rotate_log_file(time_based_rotation, size_rotation_for);
	}
}

/*
 * if any audit log was coming,
 * wakeup a consumer to read
 */
static void	audit_process_wakeup(bool timeout)
{
	if (audit_consume_requested)
	{
		int i = 0;

		if (timeout)
		{
			for (i = 0; i < MaxBackends; i++)
			{
				int sharedIdx = i;
				int consumer_id = sharedIdx % AuditLog_max_worker_number;

				AlogQueue * shared_common_queue = alog_get_shared_common_queue(sharedIdx);
				AlogQueue * shared_fga_queue = alog_get_shared_fga_queue(sharedIdx);
				AlogQueue * shared_trace_queue = alog_get_shared_trace_queue(sharedIdx);

				bool b_common_is_empty = alog_queue_is_empty2(shared_common_queue);
				bool b_fga_is_empty = alog_queue_is_empty2(shared_fga_queue);
				bool b_trace_is_empty = alog_queue_is_empty2(shared_trace_queue);

				pg_memory_barrier();

				if (!b_common_is_empty || !b_fga_is_empty || !b_trace_is_empty)
				{
					if (!audit_shared_consumer_bitmap_get_value(consumer_id))
					{
						audit_shared_consumer_bitmap_set_value(consumer_id, 1);
						alog_consumer_wakeup(consumer_id);
					}
				}
			}
		}
		else
		{
			for (i = 0; i < AuditLog_max_worker_number; i++)
			{
				int consumer_id = i;
				int bitmap_value = audit_shared_consumer_bitmap_get_value(consumer_id);

				pg_memory_barrier();

				if (bitmap_value)
				{
					alog_consumer_wakeup(consumer_id);
				}
			}
		}

		audit_consume_requested = false;
	}
}

#endif

#ifdef AuditLog_002_For_ShareMemoryQueue

/* --------------------------------
 *        shmem routines
 * --------------------------------
 */

/* --------------------------------
 *
 * 01. AlogQueueArray  as follows
 *
 *                    __________________________________
 *                   |                                  |
 * | a_count | a_bitmap pointer | a_queue [MaxBackends] |   a_bitmap content    | AlogQueue0 | AlogQueue1 | AlogQueue2 | ... | AlogQueue[MaxBackends - 1] |
 * |                               |___|___|___|___|____________________________|            |            |            |     |                            |
 * |                                   |___|___|___|_________________________________________|            |            |     |                            |
 * |                                       |___|___|______________________________________________________|            |     |                            |
 * |                                           |___|___________________________________________________________________|     |                            |
 * |                                               |_________________________________________________________________________|                            |
 * |                                                                                                                                                      |
 * |         |                  |                       |                       |                                                                         |
 * |         |                  |                       |                       |<-------- audit_shared_common_queue_elem_size() * MaxBackends ---------->|
 * |<- 4B -> |<------ 4B ------>|<- 4 * MaxBackends B ->|<- AUDIT_BITMAP_SIZE ->|<--------- audit_shared_fga_queue_elem_size() * MaxBackends ------------>|
 * |         |                  |                       |                       |<-------- audit_shared_trace_queue_elem_size() * MaxBackends ----------->|
 * |                                                                            |                                                                         |
 * |                                                                            |                                                                         |
 * |<------------------------- Shared Log Queue Array Header ------------------>|<------------------- AlogQueue Array [MaxBackends] --------------------->|
 *
 *
 * 02. AlogQueue as follows
 *
 *                                             | q_area -> char[AuditLog_common_log_queue_size_kb * BYTES_PER_KB] |
 * | q_pid | q_size | q_lock | q_head | q_tail |                              OR                                  |
 *                                             | q_area ->  char[AuditLog_fga_log_queue_size_kb * BYTES_PER_KB]   |
 *                                             |                              OR                                  |
 *                                             | q_area ->  char[Maintain_trace_log_queue_size_kb * BYTES_PER_KB] |
 *
 * --------------------------------
 */

static Size audit_shared_queue_array_bitmap_offset(void)
{
    Size alogQueueArrayBitmapOffset = 0;

	/* store AlogQueueArray->a_count, a_bitmap */
	alogQueueArrayBitmapOffset = add_size(alogQueueArrayBitmapOffset,
										offsetof(AlogQueueArray, a_queue));

	/* store AlogQueueArray->a_queue */
	alogQueueArrayBitmapOffset = add_size(alogQueueArrayBitmapOffset,
										  mul_size(sizeof(AlogQueue *), MaxBackends));

    return alogQueueArrayBitmapOffset;
}

static Size audit_shared_queue_array_header_size(void)
{
    Size alogQueueArrayHeaderSize = 0;

	/* store AlogQueueArray->a_count, a_bitmap, a_queue */
	alogQueueArrayHeaderSize = add_size(alogQueueArrayHeaderSize,
										audit_shared_queue_array_bitmap_offset());

	/* store content of AlogQueueArray->a_bitmap */
	alogQueueArrayHeaderSize = add_size(alogQueueArrayHeaderSize,
										AUDIT_BITMAP_SIZE);

    return alogQueueArrayHeaderSize;
}

static Size audit_queue_elem_size(int queue_size_kb)
{
    Size elemQueueSize = 0;

    elemQueueSize = offsetof(AlogQueue, q_area);
    elemQueueSize = add_size(elemQueueSize,
                             mul_size(queue_size_kb, BYTES_PER_KB));
    return elemQueueSize;
}

static Size audit_shared_common_queue_elem_size(void)
{
    Size        alogCommonQueueItemSize = 0;

    /* store content of common audit log */
    alogCommonQueueItemSize = audit_queue_elem_size(AuditLog_common_log_queue_size_kb);

    return alogCommonQueueItemSize;
}

static Size audit_shared_common_queue_array_size(void)
{
    Size        alogCommonQueueSize = 0;

    /* store content of common audit log */
    alogCommonQueueSize = audit_shared_common_queue_elem_size();
    alogCommonQueueSize = mul_size(alogCommonQueueSize, MaxBackends);

    alogCommonQueueSize = add_size(alogCommonQueueSize,
                                   audit_shared_queue_array_header_size());

    return alogCommonQueueSize;
}

static Size audit_shared_fga_queue_elem_size(void)
{
    Size        alogFgaQueueItemSize = 0;

    /* store content of fga audit log */
    alogFgaQueueItemSize = audit_queue_elem_size(AuditLog_fga_log_queue_size_kb);

    return alogFgaQueueItemSize;
}

static Size audit_shared_fga_queue_array_size(void)
{
    Size        alogFgaQueueSize = 0;

    /* store content of fga audit log */
    alogFgaQueueSize = audit_shared_fga_queue_elem_size();
    alogFgaQueueSize = mul_size(alogFgaQueueSize, MaxBackends);

    alogFgaQueueSize = add_size(alogFgaQueueSize,
                                audit_shared_queue_array_header_size());

    return alogFgaQueueSize;
}

static Size audit_shared_trace_queue_elem_size(void)
{
	Size		alogTraceQueueItemSize = 0;

	/* store content of trace audit log */
	alogTraceQueueItemSize = audit_queue_elem_size(Maintain_trace_log_queue_size_kb);

	return alogTraceQueueItemSize;
}

static Size audit_shared_trace_queue_array_size(void)
{
	Size		alogTraceQueueSize = 0;

	/* store content of trace audit log */
	alogTraceQueueSize = audit_shared_trace_queue_elem_size();
	alogTraceQueueSize = mul_size(alogTraceQueueSize, MaxBackends);

	alogTraceQueueSize = add_size(alogTraceQueueSize,
								audit_shared_queue_array_header_size());

	return alogTraceQueueSize;
}

static Size audit_shared_consumer_bitmap_size(void)
{
    Size alogConsumerBitmapSize = 0;

    alogConsumerBitmapSize = mul_size(AuditLog_max_worker_number, sizeof(int));

    return alogConsumerBitmapSize;
}

static int audit_shared_consumer_bitmap_get_value(int consumer_id)
{
    Assert(consumer_id >= 0 && consumer_id < AuditLog_max_worker_number);
    return AuditConsumerNotifyBitmap[consumer_id];
}

static void audit_shared_consumer_bitmap_set_value(int consumer_id, int value)
{
    Assert(consumer_id >= 0 && consumer_id < AuditLog_max_worker_number);
    AuditConsumerNotifyBitmap[consumer_id] = value;
}

Size AuditLoggerShmemSize(void)
{
	Size		size = 0;
	Size		alogCommonQueueSize = 0;
	Size		alogFgaQueueSize = 0;
	Size		alogTraceQueueSize = 0;
	Size		alogConsumerBmpSize = 0;

    /* for common audit log */
    alogCommonQueueSize = audit_shared_common_queue_array_size();

    /* for fga audit log*/
    alogFgaQueueSize = audit_shared_fga_queue_array_size();

	/* for trace audit log */
	alogTraceQueueSize = audit_shared_trace_queue_array_size();

	/* for consumer notify bitmap */
	alogConsumerBmpSize = audit_shared_consumer_bitmap_size();

	/* for total size */
	size = add_size(alogCommonQueueSize, alogFgaQueueSize);
	size = add_size(size, alogTraceQueueSize);
	size = add_size(size, alogConsumerBmpSize);

    return size;
}

void AuditLoggerShmemInit(void)
{
	Size		alogBmpOffset = 0;
	Size		alogHeaderSize = 0;
	Size		alogItemSize = 0;
	Size		alogArraySize = 0;
	Size		alogConsumerBmpSize = 0;

	bool		found = false;
	int			i = 0;

	alogBmpOffset = audit_shared_queue_array_bitmap_offset();
	alogHeaderSize = audit_shared_queue_array_header_size();
	alogItemSize = audit_shared_common_queue_elem_size();
	alogArraySize = audit_shared_common_queue_array_size();

	AuditCommonLogQueueArray = ShmemInitStruct("Audit Common Log Queue",
												alogArraySize,
												&found);
	/* Mark it empty upon creation */
	if (!found)
	{
		AlogQueueArray * alogQueueArray = AuditCommonLogQueueArray;
		int falogQueueArray = 0;
		Size sharedMemSize = 0;

		if (enable_auditlogger_warning)
		{
			sharedMemSize += alogHeaderSize;
			MemSet(alogQueueArray, 'a', alogHeaderSize);

			for (i = 0; i < MaxBackends; i++)
			{
				AlogQueue * alogQueueItem = NULL;

				alogQueueItem = (AlogQueue *)(((char *) alogQueueArray) + alogHeaderSize + i * alogItemSize);
				sharedMemSize += audit_shared_common_queue_elem_size();
				MemSet(alogQueueItem, 'b', audit_shared_common_queue_elem_size());
			}

			falogQueueArray = BasicOpenFile("AuditCommonLogQueueArray.txt", O_RDWR | O_TRUNC | O_CREAT, S_IRUSR | S_IWUSR);
			if (falogQueueArray != -1)
			{
				write(falogQueueArray, alogQueueArray, alogArraySize);
				write(falogQueueArray, "\nNew Line\n", strlen("\nNew Line\n"));
			}

			Assert(sharedMemSize == alogArraySize);
		}

		MemSet(alogQueueArray, 0, alogArraySize);

		alogQueueArray->a_count = MaxBackends;
		alogQueueArray->a_bitmap = bms_make(((char *) alogQueueArray) + alogBmpOffset, 
											MaxBackends);
		for (i = 0; i < MaxBackends; i++)
		{
			AlogQueue * alogQueueItem = NULL;

			alogQueueItem = (AlogQueue *)(((char *) alogQueueArray) + alogHeaderSize + i * alogItemSize);

			alog_queue_init(alogQueueItem, AuditLog_common_log_queue_size_kb);

			alogQueueArray->a_queue[i] = alogQueueItem;
		}

		if (enable_auditlogger_warning)
		{
			if (falogQueueArray != -1)
			{
				write(falogQueueArray, alogQueueArray, alogArraySize);
				close(falogQueueArray);
			}
		}
	}

	found = false;
	i = 0;

	alogBmpOffset = audit_shared_queue_array_bitmap_offset();
	alogHeaderSize = audit_shared_queue_array_header_size();
	alogItemSize = audit_shared_fga_queue_elem_size();
	alogArraySize = audit_shared_fga_queue_array_size();

	AuditFGALogQueueArray = ShmemInitStruct("Audit FGA Log Queue",
											alogArraySize,
											&found);
	/* Mark it empty upon creation */
	if (!found)
	{
		AlogQueueArray * alogQueueArray = AuditFGALogQueueArray;
		int falogQueueArray = 0;
		Size sharedMemSize = 0;

		if (enable_auditlogger_warning)
		{
			sharedMemSize += alogHeaderSize;
			MemSet(alogQueueArray, 'c', alogHeaderSize);

			for (i = 0; i < MaxBackends; i++)
			{
				AlogQueue * alogQueueItem = NULL;

				alogQueueItem = (AlogQueue *)(((char *) alogQueueArray) + alogHeaderSize + i * alogItemSize);
				sharedMemSize += audit_shared_fga_queue_elem_size();
				MemSet(alogQueueItem, 'd', audit_shared_fga_queue_elem_size());
			}

			falogQueueArray = BasicOpenFile("AuditFGALogQueueArray.txt", O_RDWR | O_TRUNC | O_CREAT, S_IRUSR | S_IWUSR);
			if (falogQueueArray != -1)
			{
				write(falogQueueArray, alogQueueArray, alogArraySize);
				write(falogQueueArray, "\nNew Line\n", strlen("\nNew Line\n"));
			}

			Assert(sharedMemSize == alogArraySize);
		}

		MemSet(alogQueueArray, 0, alogArraySize);

		alogQueueArray->a_count = MaxBackends;
		alogQueueArray->a_bitmap = bms_make(((char *) alogQueueArray) + alogBmpOffset, 
										MaxBackends);
		for (i = 0; i < MaxBackends; i++)
		{
			AlogQueue * alogQueueItem = NULL;

			alogQueueItem = (AlogQueue *)(((char *) alogQueueArray) + alogHeaderSize + i * alogItemSize);

			alog_queue_init(alogQueueItem, AuditLog_fga_log_queue_size_kb);

			alogQueueArray->a_queue[i] = alogQueueItem;
		}

		if (enable_auditlogger_warning)
		{
			if (falogQueueArray != -1)
			{
				write(falogQueueArray, alogQueueArray, alogArraySize);
				close(falogQueueArray);
			}
		}
	}

	found = false;
	i = 0;

	alogBmpOffset = audit_shared_queue_array_bitmap_offset();
	alogHeaderSize = audit_shared_queue_array_header_size();
	alogItemSize = audit_shared_trace_queue_elem_size();
	alogArraySize = audit_shared_trace_queue_array_size();

	AuditTraceLogQueueArray = ShmemInitStruct("Audit Trace Log Queue",
											alogArraySize,
											&found);
	/* Mark it empty upon creation */
	if (!found)
	{
		AlogQueueArray * alogQueueArray = AuditTraceLogQueueArray;
		int falogQueueArray = 0;
		Size sharedMemSize = 0;

		if (enable_auditlogger_warning)
		{
			sharedMemSize += alogHeaderSize;
			MemSet(alogQueueArray, 'e', alogHeaderSize);

			for (i = 0; i < MaxBackends; i++)
			{
				AlogQueue * alogQueueItem = NULL;

				alogQueueItem = (AlogQueue *)(((char *) alogQueueArray) + alogHeaderSize + i * alogItemSize);
				sharedMemSize += audit_shared_trace_queue_elem_size();
				MemSet(alogQueueItem, 'f', audit_shared_trace_queue_elem_size());
			}

			falogQueueArray = BasicOpenFile("AuditTraceLogQueueArray.txt", O_RDWR | O_TRUNC | O_CREAT, S_IRUSR | S_IWUSR);
			if (falogQueueArray != -1)
			{
				write(falogQueueArray, alogQueueArray, alogArraySize);
				write(falogQueueArray, "\nNew Line\n", strlen("\nNew Line\n"));
			}

			Assert(sharedMemSize == alogArraySize);
		}

		MemSet(alogQueueArray, 0, alogArraySize);

		alogQueueArray->a_count = MaxBackends;
		alogQueueArray->a_bitmap = bms_make(((char *) alogQueueArray) + alogBmpOffset,
											MaxBackends);
		for (i = 0; i < MaxBackends; i++)
		{
			AlogQueue * alogQueueItem = NULL;

			alogQueueItem = (AlogQueue *)(((char *) alogQueueArray) + alogHeaderSize + i * alogItemSize);

			alog_queue_init(alogQueueItem, Maintain_trace_log_queue_size_kb);

			alogQueueArray->a_queue[i] = alogQueueItem;
		}

		if (enable_auditlogger_warning)
		{
			if (falogQueueArray != -1)
			{
				write(falogQueueArray, alogQueueArray, alogArraySize);
				close(falogQueueArray);
			}
		}
	}

	found = false;
	i = 0;

	alogConsumerBmpSize = audit_shared_consumer_bitmap_size();

	AuditConsumerNotifyBitmap = ShmemInitStruct("Audit Consumer Bitmap",
										  alogConsumerBmpSize,
										  &found);
	/* Mark it empty upon creation */
	if (!found)
	{
		MemSet(AuditConsumerNotifyBitmap, 0, alogConsumerBmpSize);
	}
}

#endif

#ifdef AuditLog_003_For_LogFile

/* --------------------------------
 *        logfile routines
 * --------------------------------
 */

/*
 * Make audit_log_directory from Log_directory
 */
static void
audit_assign_log_dir(void)
{
    if (audit_log_directory != NULL)
    {
        pfree(audit_log_directory);
        audit_log_directory = NULL;
    }

    if (Log_directory == NULL)
    {
        audit_log_directory = pstrdup("pg_log/audit");
    }
    else
    {
        StringInfoData alog_dir;

		memset(&alog_dir, 0, sizeof(alog_dir));
		initStringInfo(&alog_dir);
		appendStringInfo(&alog_dir, "%s/audit", Log_directory);

		audit_log_directory = alog_dir.data;
	}

	if (trace_log_directory != NULL)
	{
		pfree(trace_log_directory);
		trace_log_directory = NULL;
	}
	trace_log_directory = pstrdup("pg_log/maintain");
}

/*
 * Write text to the currently open logfile
 *
 * This is exported so that elog.c can call it when am_auditlogger is true.
 * This allows the auditlogger process to record elog messages of its own,
 * even though its stderr does not point at the syslog pipe.
 */
static int
audit_write_log_file(const char *buffer, int count, int destination)
{
    int            rc = 0;

	if (destination == AUDIT_FGA_LOG)
	{
		rc = fwrite(buffer, 1, count, audit_fga_log_file);
	}
	else if (destination == MAINTAIN_TRACE_LOG)
	{
		rc = fwrite(buffer, 1, count, audit_trace_log_file);
	}
	else
	{
		Assert(destination == AUDIT_COMMON_LOG);
		rc = fwrite(buffer, 1, count, audit_comm_log_file);
	}

    /* can't use ereport here because of possible recursion */
    if (rc != count)
    {
        write_stderr("could not write to audit log file: %s\n", strerror(errno));
        return -1;
    }

    return 0;
}

static void
audit_open_fga_log_file(void)
{
    char       *filename = NULL;

    filename = audit_log_file_getname(time(NULL), ".fga");

    audit_fga_log_file = audit_open_log_file(filename, "a", false);

    if (audit_last_fga_log_file_name != NULL) /* probably shouldn't happen */
        pfree(audit_last_fga_log_file_name);

    audit_last_fga_log_file_name = filename;
}


static void
audit_open_trace_log_file(void)
{
	char	   *filename = NULL;

	filename = trace_log_file_getname(time(NULL), ".trace");

	audit_trace_log_file = audit_open_log_file(filename, "a", false);

	if (audit_last_trace_log_file_name != NULL)
	{
		/* probably shouldn't happen */
		pfree(audit_last_trace_log_file_name);
	}

	audit_last_trace_log_file_name = filename;
}

/*
 * Open a new logfile with proper permissions and buffering options.
 *
 * If allow_errors is true, we just log any open failure and return NULL
 * (with errno still correct for the fopen failure).
 * Otherwise, errors are treated as fatal.
 */
static FILE *
audit_open_log_file(const char *filename, const char *mode, bool allow_errors)
{
    FILE       *fh = NULL;
    mode_t        oumask = 0;

    /*
     * Note we do not let AuditLog_file_mode disable IWUSR, since we certainly want
     * to be able to write the files ourselves.
     */
    oumask = umask((mode_t) ((~(AuditLog_file_mode | S_IWUSR)) & (S_IRWXU | S_IRWXG | S_IRWXO)));
    fh = fopen(filename, mode);
    umask(oumask);

    if (fh)
    {
        setvbuf(fh, NULL, PG_IOLBF, 0);

#ifdef WIN32
        /* use CRLF line endings on Windows */
        _setmode(_fileno(fh), _O_TEXT);
#endif
    }
    else
    {
        int            save_errno = errno;

        ereport(allow_errors ? LOG : FATAL,
                (errcode_for_file_access(),
                 errmsg("could not open audit log file \"%s\": %m",
                        filename)));
        errno = save_errno;
    }

    return fh;
}

/*
 * perform logfile rotation
 */
static void
audit_rotate_log_file(bool time_based_rotation, int size_rotation_for)
{
	char	   *filename = NULL;
	char	   *fgafilename = NULL;
	char	   *tracefilename = NULL;
	pg_time_t	fntime = 0;
	FILE	   *fh = NULL;

	audit_rotation_requested = false;

	/*
	 * When doing a time-based rotation, invent the new logfile name based on
	 * the planned rotation time, not current time, to avoid "slippage" in the
	 * file name when we don't do the rotation immediately.
	 */
	if (time_based_rotation)
		fntime = audit_next_rotation_time;
	else
		fntime = time(NULL);
	filename = audit_log_file_getname(fntime, NULL);
	if (audit_fga_log_file != NULL)
		fgafilename = audit_log_file_getname(fntime, ".fga");
	if (audit_trace_log_file != NULL)
		tracefilename = trace_log_file_getname(fntime, ".trace");

	/*
	 * Decide whether to overwrite or append.  We can overwrite if (a)
	 * AuditLog_truncate_on_rotation is set, (b) the rotation was triggered by
	 * elapsed time and not something else, and (c) the computed file name is
	 * different from what we were previously logging into.
	 *
	 * Note: audit_last_comm_log_file_name should never be NULL here, but if it is, append.
	 */
	if (time_based_rotation || (size_rotation_for & AUDIT_COMMON_LOG))
	{
		if (AuditLog_truncate_on_rotation && time_based_rotation &&
			audit_last_comm_log_file_name != NULL &&
			strcmp(filename, audit_last_comm_log_file_name) != 0)
			fh = audit_open_log_file(filename, "w", true);
		else
			fh = audit_open_log_file(filename, "a", true);

		if (!fh)
		{
			/*
			 * ENFILE/EMFILE are not too surprising on a busy system; just
			 * keep using the old file till we manage to get a new one.
			 * Otherwise, assume something's wrong with audit_log_directory and stop
			 * trying to create files.
			 */
			if (errno != ENFILE && errno != EMFILE)
			{
				ereport(LOG,
						(errmsg("disabling automatic rotation audit log file (use SIGHUP to re-enable)")));
				audit_rotation_disabled = true;
			}

			if (filename)
				pfree(filename);
			if (fgafilename)
				pfree(fgafilename);
			if (tracefilename)
				pfree(tracefilename);
			return;
		}

		SpinLockAcquire(&(audit_comm_log_file_lock));
		fclose(audit_comm_log_file);
		audit_comm_log_file = fh;
		SpinLockRelease(&(audit_comm_log_file_lock));

		/* instead of pfree'ing filename, remember it for next time */
		if (audit_last_comm_log_file_name != NULL)
			pfree(audit_last_comm_log_file_name);
		audit_last_comm_log_file_name = filename;
		filename = NULL;
	}

	/* Same as above, but for fga audit log file. */
	if (audit_fga_log_file != NULL &&
		(time_based_rotation || (size_rotation_for & AUDIT_FGA_LOG)))
	{
		if (AuditLog_truncate_on_rotation && time_based_rotation &&
			audit_last_fga_log_file_name != NULL &&
			strcmp(fgafilename, audit_last_fga_log_file_name) != 0)
			fh = audit_open_log_file(fgafilename, "w", true);
		else
			fh = audit_open_log_file(fgafilename, "a", true);

		if (!fh)
		{
			/*
			 * ENFILE/EMFILE are not too surprising on a busy system; just
			 * keep using the old file till we manage to get a new one.
			 * Otherwise, assume something's wrong with audit_log_directory and stop
			 * trying to create files.
			 */
			if (errno != ENFILE && errno != EMFILE)
			{
				ereport(LOG,
						(errmsg("disabling automatic rotation audit log file (use SIGHUP to re-enable)")));
				audit_rotation_disabled = true;
			}

			if (filename)
				pfree(filename);
			if (fgafilename)
				pfree(fgafilename);
			if (tracefilename)
				pfree(tracefilename);
			return;
		}

		SpinLockAcquire(&(audit_fga_log_file_lock));
		fclose(audit_fga_log_file);
		audit_fga_log_file = fh;
		SpinLockRelease(&(audit_fga_log_file_lock));

		/* instead of pfree'ing filename, remember it for next time */
		if (audit_last_fga_log_file_name != NULL)
			pfree(audit_last_fga_log_file_name);
		audit_last_fga_log_file_name = fgafilename;
		fgafilename = NULL;
	}

	/* Same as above, but for trace audit log file. */
	if (audit_trace_log_file != NULL &&
		(time_based_rotation || (size_rotation_for & MAINTAIN_TRACE_LOG)))
	{
		/*
		 * Only append write，do not consider overwrite for maintain trace log.
		 * That is different from audit common log and fga log.
		 *
		if (AuditLog_truncate_on_rotation && time_based_rotation &&
			audit_last_trace_log_file_name != NULL &&
			strcmp(tracefilename, audit_last_trace_log_file_name) != 0)
			fh = audit_open_log_file(tracefilename, "w", true);
		else
		*/
		{
			fh = audit_open_log_file(tracefilename, "a", true);
		}

		if (!fh)
		{
			/*
			 * ENFILE/EMFILE are not too surprising on a busy system; just
			 * keep using the old file till we manage to get a new one.
			 * Otherwise, assume something's wrong with audit_log_directory and stop
			 * trying to create files.
			 */
			if (errno != ENFILE && errno != EMFILE)
			{
				ereport(LOG,
						(errmsg("disabling automatic rotation audit log file (use SIGHUP to re-enable)")));
				audit_rotation_disabled = true;
			}

			if (filename)
				pfree(filename);
			if (fgafilename)
				pfree(fgafilename);
			if (tracefilename)
				pfree(tracefilename);
			return;
		}

		SpinLockAcquire(&(audit_trace_log_file_lock));
		fclose(audit_trace_log_file);
		audit_trace_log_file = fh;
		SpinLockRelease(&(audit_trace_log_file_lock));

		/* instead of pfree'ing filename, remember it for next time */
		if (audit_last_trace_log_file_name != NULL)
			pfree(audit_last_trace_log_file_name);
		audit_last_trace_log_file_name = tracefilename;
		tracefilename = NULL;
	}

	if (filename)
		pfree(filename);
	if (fgafilename)
		pfree(fgafilename);
	if (tracefilename)
		pfree(tracefilename);

	audit_set_next_rotation_time();
}

/*
 * construct logfile name using timestamp information
 *
 * If suffix isn't NULL, append it to the name, replacing any ".log"
 * that may be in the pattern.
 *
 * Result is palloc'd.
 */
static char *
audit_log_file_getname(pg_time_t timestamp, const char *suffix)
{
    char       *filename = NULL;
    int            len = 0;

    filename = palloc(MAXPGPATH);

    snprintf(filename, MAXPGPATH, "%s/", audit_log_directory);

    len = strlen(filename);

    /* treat AuditLog_filename as a strftime pattern */
    pg_strftime(filename + len, MAXPGPATH - len, AuditLog_filename,
                pg_localtime(&timestamp, log_timezone));

    if (suffix != NULL)
    {
        len = strlen(filename);
        if (len > 4 && (strcmp(filename + (len - 4), ".log") == 0))
            len -= 4;
        strlcpy(filename + len, suffix, MAXPGPATH - len);
    }

    return filename;
}

/*
 * construct logfile name using timestamp information.
 * acoording to audit_log_file_getname().
 *
 * If suffix isn't NULL, append it to the name, replacing any ".log"
 * that may be in the pattern.
 *
 * Result is palloc'd.
 */
static char *
trace_log_file_getname(pg_time_t timestamp, const char *suffix)
{
	char	   *filename = NULL;
	int			len = 0;

	filename = palloc(MAXPGPATH);

	snprintf(filename, MAXPGPATH, "%s/", trace_log_directory);

	len = strlen(filename);

	/* treat AuditLog_filename as a strftime pattern */
	pg_strftime(filename + len, MAXPGPATH - len, TraceLog_filename,
				pg_localtime(&timestamp, log_timezone));

	if (suffix != NULL)
	{
		len = strlen(filename);
		if (len > 4 && (strcmp(filename + (len - 4), ".log") == 0))
			len -= 4;
		strlcpy(filename + len, suffix, MAXPGPATH - len);
	}

	return filename;
}

/*
 * Determine the next planned rotation time, and store in audit_next_rotation_time.
 */
static void
audit_set_next_rotation_time(void)
{
    pg_time_t    now = 0;
    struct pg_tm *tm = NULL;
    int            rotinterval = 0;

    /* nothing to do if time-based rotation is disabled */
    if (AuditLog_RotationAge <= 0)
        return;

    /*
     * The requirements here are to choose the next time > now that is a
     * "multiple" of the log rotation interval.     "Multiple" can be interpreted
     * fairly loosely.    In this version we align to log_timezone rather than
     * GMT.
     */
    rotinterval = AuditLog_RotationAge * SECS_PER_MINUTE;    /* convert to seconds */
    now = (pg_time_t) time(NULL);
    tm = pg_localtime(&now, log_timezone);
    now += tm->tm_gmtoff;
    now -= now % rotinterval;
    now += rotinterval;
    now -= tm->tm_gmtoff;
    audit_next_rotation_time = now;
}
#endif

#ifdef AuditLog_004_For_QueueReadWrite

/* --------------------------------
 *        AlogQueue routines
 * --------------------------------
 */

/*
 * just for compile warning: unused-but-set-variable
 */
static void alog_just_caller(void * var)
{
}

/*
 * init queue area
 */
static void alog_queue_init(AlogQueue * queue, int queue_size_kb)
{
    queue->q_pid = 0;
    queue->q_size = mul_size(queue_size_kb, BYTES_PER_KB);
    SpinLockInit(&(queue->q_lock));
    queue->q_head = 0;
    queue->q_tail = 0;
    MemSet(queue->q_area, 0, queue->q_size);
}

/*
 * Get a write pointer in queue
 */
static char * alog_queue_offset_to(AlogQueue * queue, int offset)
{
    char * start = (char *) queue;

    Assert(offset >= 0 && offset < queue->q_size);

    start += offsetof(AlogQueue, q_area);
    start += offset;

    return start;
}

static bool alog_queue_is_full(int q_size, int q_head, int q_tail)
{
    Assert(q_size > 0 && q_head >= 0 && q_tail >= 0);
    Assert(q_head < q_size && q_tail < q_size);

    if ((q_tail + 1) % q_size == q_head)
    {
        return true;
    }
    else
    {
        return false;
    }
}

static bool alog_queue_is_empty(int q_size, int q_head, int q_tail)
{
    Assert(q_size > 0 && q_head >= 0 && q_tail >= 0);
    Assert(q_head < q_size && q_tail < q_size);

    if (q_tail == q_head)
    {
        return true;
    }
    else
    {
        return false;
    }
}

static bool alog_queue_is_empty2(AlogQueue * queue)
{
	volatile int q_head = queue->q_head;
	volatile int q_tail = queue->q_tail;
	volatile int q_size = queue->q_size;

    pg_memory_barrier();

    return alog_queue_is_empty(q_size, q_head, q_tail);
}

/*
 * how many bytes already in used
 */
static int alog_queue_used(int q_size, int q_head, int q_tail)
{
	int used = (q_tail - q_head + q_size) % q_size;

    Assert(q_size > 0 && q_head >= 0 && q_tail >= 0);
    Assert(q_head < q_size && q_tail < q_size);

	return used;
}

/*
 * how many bytes remain in Queue
 */
static int alog_queue_remain(int q_size, int q_head, int q_tail)
{
    int remain = (q_head - q_tail + q_size - 1) % q_size;

    Assert(q_size > 0 && q_head >= 0 && q_tail >= 0);
    Assert(q_head < q_size && q_tail < q_size);
    Assert(remain == (q_size - 1) - ((q_tail - q_head + q_size) % q_size));
    Assert(remain == (q_size - 1) - alog_queue_used(q_size, q_head, q_tail));

    return remain;
}

/*
 * whether queue has enough space for N bytes ?
 */
static bool alog_queue_is_enough(int q_size, int q_head, int q_tail, int N)
{
    int remain = alog_queue_remain(q_size, q_head, q_tail);

    Assert(q_size > 0 && q_head >= 0 && q_tail >= 0 && N > 0);
    Assert(q_head < q_size && q_tail < q_size);

    if (remain > N)
    {
        return true;
    }

    return false;
}

/*
 * write one buff to queue
 *
 * len = size(int) + strlen(str)
 *
 */
static bool alog_queue_push(AlogQueue * queue, char * buff, int len)
{
    char * buff_array [] = { buff };
    int len_array [] = { len };

    return alog_queue_pushn(queue, buff_array, len_array, sizeof(len_array)/sizeof(len_array[0]));
}

/*
 * write buff1 and buff2 to queue
 */
static bool alog_queue_push2(AlogQueue * queue, char * buff1, int len1, char * buff2, int len2)
{
	char * buff_array[] = {buff1, buff2};
	int len_array[] = {len1, len2};

    return alog_queue_pushn(queue, buff_array, len_array, sizeof(len_array)/sizeof(len_array[0]));
}

/*
 * write n buffs to queue
 */
static bool alog_queue_pushn(AlogQueue * queue, char * buff[], int len[], int n)
{
	volatile int q_head = queue->q_head;
	volatile int q_tail = queue->q_tail;
	volatile int q_size = queue->q_size;

	int q_head_before = q_head;
	int q_tail_before = q_tail;
	int q_size_before = q_size;

	int q_used_before = 0;
	int q_used_after = 0;

	int total_len = 0;
	int i = 0;

	for (i = 0; i < n; i++)
	{
		total_len += len[i];
	}

    pg_memory_barrier();
    alog_just_caller(&q_used_before);
    alog_just_caller(&q_used_after);

    Assert(q_size > 0 && q_head >= 0 && q_tail >= 0);
    Assert(q_head < q_size && q_tail < q_size);
    Assert(buff != NULL && len != 0 && n > 0 && total_len > 0);

    q_used_before = alog_queue_used(q_size_before, q_head_before, q_tail_before);

    if (alog_queue_is_full(q_size, q_head, q_tail))
    {
        return false;
    }

    if (!alog_queue_is_enough(q_size, q_head, q_tail, total_len))
    {
        return false;
    }

    for (i = 0; i < n; i++)
    {
        char * curr_buff = buff[i];
        int curr_len = len[i];

        /* has enough space, write directly */
        if (q_size - q_tail >= curr_len)
        {
            char * p_start = alog_queue_offset_to(queue, q_tail);
            memcpy(p_start, curr_buff, curr_len);
        }
        else
        {
            /* must write as two parts */
            int first_len = q_size - q_tail;
            int second_len = curr_len - first_len;

            char * first_buf = curr_buff + 0;
            char * second_buf = curr_buff + first_len;

            char * p_start = NULL;

            pg_memory_barrier();

            Assert(first_len > 0 && first_len < q_size);
            Assert(second_len > 0 && second_len < q_size);

            /* 01. write the first parts into the tail of queue->q_area */
            p_start = alog_queue_offset_to(queue, q_tail);
            memcpy(p_start, first_buf, first_len);

            Assert((q_tail + first_len) % q_size == 0);

            /* 02. write the remain parts into the head of queue->q_area */
            p_start = alog_queue_offset_to(queue, 0);
            memcpy(p_start, second_buf, second_len);
        }

        q_tail = (q_tail + curr_len) % q_size;
    }

    q_used_after = alog_queue_used(q_size, q_head, q_tail);
    Assert(q_used_before + total_len == q_used_after);

    queue->q_tail = q_tail;

    return true;
}

/*
 * |<- strlen value ->|<- string message content ->|
 * |											   |
 * |											   |
 * |<------------------ buff --------------------->|
 *
 * len = size(int) + strlen(str)
 *
 */
static int alog_queue_get_str_len(AlogQueue * queue, int offset)
{
    volatile int q_size = queue->q_size;
    char buff[sizeof(int)] = { '\0' };
    int len = 0;

    pg_memory_barrier();

    Assert(offset >= 0 && offset < q_size);

    /* read len directly */
    if (q_size - offset >= sizeof(int))
    {
        char * q_start = alog_queue_offset_to(queue, offset);
        memcpy(buff, q_start, sizeof(int));
    }
    else
    {
        /* must read as two parts */
        int first_len = q_size - offset;
        int second_len = sizeof(int) - first_len;

        char * p_start = NULL;

        pg_memory_barrier();

        Assert(first_len > 0 && first_len < q_size);
        Assert(second_len > 0 && second_len < sizeof(int));

        /* 01. copy the first parts */
        p_start = alog_queue_offset_to(queue, offset);
        memcpy(buff, p_start, first_len);

        /* 02. copy the remain parts */
        p_start = alog_queue_offset_to(queue, 0);
        memcpy(buff + first_len, p_start, second_len);
    }

    memcpy((char *)(&len), buff, sizeof(int));

    Assert(len > 0 && len < q_size);

    return len;
}

/*
 * copy message from queue to another as much as possible
 *
 * |<- strlen value ->|<- string message content ->|
 * |											   |
 * |											   |
 * |<------------------ buff --------------------->|
 *
 * len = size(int) + strlen(str)
 *
 */
static bool alog_queue_pop_to_queue(AlogQueue * from, AlogQueue * to)
{// #lizard forgives
    volatile int q_from_head = from->q_head;      
    volatile int q_from_tail = from->q_tail;
    volatile int q_from_size = from->q_size;

    volatile int q_to_head = to->q_head;      
    volatile int q_to_tail = to->q_tail;
    volatile int q_to_size = to->q_size;

    int from_head = q_from_head;      
    int from_tail = q_from_tail;
    int from_size = q_from_size;

    int to_head = q_to_head;
    int to_tail = q_to_tail;
    int to_size = q_to_size;

    int from_total = 0;

    int from_used = 0;
    int from_copyed = 0;

    int to_used = 0;
    int to_copyed = 0;

    pg_memory_barrier();
    alog_just_caller(&from_total);
    alog_just_caller(&to_used);

    from_total = from_used = alog_queue_used(from_size, from_head, from_tail);
    to_used = alog_queue_used(to_size, to_head, to_tail);

    Assert(from_size > 0 && from_head >= 0 && from_tail >= 0);
    Assert(from_head < from_size && from_tail < from_size && from_used <= from_size);

    Assert(to_size > 0 && to_head >= 0 && to_tail >= 0);
    Assert(to_head < to_size && to_tail < to_size && to_used <= to_size);

    /* from is empty, ignore */
    if (alog_queue_is_empty(from_size, from_head, from_tail))
    {
        return false;
    }

    /* to is full, can not write */
    if (alog_queue_is_full(to_size, to_head, to_tail))
    {
        return false;
    }

    /* copy message into queue until to is full or from is empty */
    do
    {
        int string_len = alog_queue_get_str_len(from, from_head);
        int copy_len = sizeof(int) + string_len;

        pg_memory_barrier();

        Assert(string_len > 0 && string_len < from_size);
        Assert(copy_len > 0 && copy_len < from_size);

        if (!alog_queue_is_enough(to_size, to_head, to_tail, copy_len))
        {
            break;
        }

        /* just copy dierctly */
        if (from_size - from_head >= copy_len)
        {
            char * p_start = alog_queue_offset_to(from, from_head);
            if (!alog_queue_push(to, p_start, copy_len))
            {
                break;
            }
        }
        else
        {
            /* must copy as two parts */
            int first_len = from_size - from_head;
            int second_len = copy_len - first_len;
            char * p_first_start = NULL;
            char * p_second_start = NULL;

            Assert(first_len > 0 && first_len < from_size);
            Assert(second_len > 0 && second_len < from_size);

            p_first_start = alog_queue_offset_to(from, from_head);
            p_second_start = alog_queue_offset_to(from, 0);

            /* 01. copy the content parts into the tail of to->q_area */
            if (!alog_queue_push2(to, p_first_start, first_len, p_second_start, second_len))
            {
                break;
            }
        }

        from_head = (from_head + copy_len) % from_size;
        to_tail = (to_tail + copy_len) % to_size;

        from_copyed += copy_len;
        to_copyed += copy_len;

        Assert(from_copyed <= from_total);
        Assert(from_used - copy_len >= 0);
        Assert(to_used + copy_len <= to_size);
        Assert(from_used - copy_len == alog_queue_used(from_size, from_head, from_tail));
        Assert(to_used + copy_len == alog_queue_used(to_size, to_head, to_tail));

        from_used = alog_queue_used(from_size, from_head, from_tail);
        to_used = alog_queue_used(to_size, to_head, to_tail);
    } while (!alog_queue_is_empty(from_size, from_head, from_tail));

    from->q_head = from_head;

    return true;
}

/*
 * copy message from queue to file as much as possible
 */
static bool alog_queue_pop_to_file(AlogQueue * from, int destination)
{
	volatile int q_from_head = from->q_head;
	volatile int q_from_tail = from->q_tail;
	volatile int q_from_size = from->q_size;

	int from_head = q_from_head;
	int from_tail = q_from_tail;
	int from_size = q_from_size;

	int from_total = 0;

	int from_used = 0;
	int from_copyed = 0;

	volatile slock_t * file_lock = NULL;

	pg_memory_barrier();
	alog_just_caller(&from_total);

	from_total = from_used = alog_queue_used(from_size, from_head, from_tail);

	Assert(from_size > 0 && from_head >= 0 && from_tail >= 0);
	Assert(from_head < from_size && from_tail < from_size && from_used <= from_size);
	Assert(destination == AUDIT_COMMON_LOG ||
		destination == AUDIT_FGA_LOG ||
		destination == MAINTAIN_TRACE_LOG);

	if (destination == AUDIT_COMMON_LOG)
	{
		file_lock = &audit_comm_log_file_lock;
	}
	else if (destination == AUDIT_FGA_LOG)
	{
		file_lock = &audit_fga_log_file_lock;
	}
	else
	{
		Assert(destination == MAINTAIN_TRACE_LOG);
		file_lock = &audit_trace_log_file_lock;
	}

	/* from is empty, ignore */
	if (alog_queue_is_empty(from_size, from_head, from_tail))
	{
		return false;
	}

	/* copy message into file until from is empty */
	do
	{
		int string_len = alog_queue_get_str_len(from, from_head);
		int copy_len = sizeof(int) + string_len;

		pg_memory_barrier();

		/* just copy dierctly */
		if (from_size - from_head >= copy_len)
		{
			char * p_start = alog_queue_offset_to(from, from_head + sizeof(int));

			/* only copy message content, not write message len */
			SpinLockAcquire(file_lock);
			audit_write_log_file(p_start, string_len, destination);
			SpinLockRelease(file_lock);
		}
		else if (from_size - from_head > sizeof(int))
		{
			/* must copy as two parts */
			int first_len = from_size - from_head - sizeof(int);
			int second_len = string_len - first_len;
			char * p_start = NULL;

			Assert(first_len > 0 && first_len < from_size);
			Assert(second_len > 0 && second_len < from_size);

			SpinLockAcquire(file_lock);
			p_start = alog_queue_offset_to(from, from_head + sizeof(int));
			audit_write_log_file(p_start, first_len, destination);

			p_start = alog_queue_offset_to(from, 0);
			audit_write_log_file(p_start, second_len, destination);
			SpinLockRelease(file_lock);
		}
		else
		{
			/* just copy content only */
			int cpy_offset = (from_head + sizeof(int)) % from_size;
			char * p_start = alog_queue_offset_to(from, cpy_offset);

			Assert(from_size - from_head <= sizeof(int));
			SpinLockAcquire(file_lock);
			audit_write_log_file(p_start, string_len, destination);
			SpinLockRelease(file_lock);
		}

		from_head = (from_head + copy_len) % from_size;
		from_copyed += copy_len;

		Assert(from_copyed <= from_total);
		Assert(from_used - copy_len >= 0);
		Assert(from_used - copy_len == alog_queue_used(from_size, from_head, from_tail));

		from_used = alog_queue_used(from_size, from_head, from_tail);
	} while (!alog_queue_is_empty(from_size, from_head, from_tail));

	from->q_head = from_head;

	return true;
}

#endif

#ifdef AuditLog_005_For_ThreadWorker

/*
 * find an unused shard entry id in AuditCommonLogQueueArray, AuditFGALogQueueArray
 * and AuditTraceLogQueueArray.
 *
 * called by postgres backend to init AuditPostgresAlogQueueIndex
 */
int AuditLoggerQueueAcquire(void)
{
    int alogIdx = -1;

	AlogQueue * common_queue = NULL;
	AlogQueue * fga_queue = NULL;
	AlogQueue * trace_queue = NULL;

    if (!IsBackendPostgres)
    {
        ereport(FATAL,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("only postgres backend can acqurie shared queue to write audit log")));
        return -1;
    }

    alogIdx = MyProc->pgprocno;
    Assert(alogIdx >= 0 && alogIdx < MaxBackends);

	common_queue = alog_get_shared_common_queue(alogIdx);
	fga_queue = alog_get_shared_fga_queue(alogIdx);
	trace_queue = alog_get_shared_trace_queue(alogIdx);

	Assert(common_queue->q_pid == fga_queue->q_pid);
	Assert(common_queue->q_pid == trace_queue->q_pid);

	AuditPostgresAlogQueueIndex = alogIdx;
	common_queue->q_pid = MyProcPid;
	fga_queue->q_pid = MyProcPid;
	trace_queue->q_pid = MyProcPid;

    if (enable_auditlogger_warning)
    {
        Assert(alogIdx < MaxBackends);
        ereport(LOG, (errmsg("acquire alog queue index %d for backend %d ", alogIdx, MyProcPid)));
    }

    return alogIdx;
}

static AlogQueue * alog_get_shared_common_queue(int idx)
{
    AlogQueue * queue = NULL;

    Assert(idx >= 0 && idx < MaxBackends);
    queue = AuditCommonLogQueueArray->a_queue[idx];

    return queue;
}

static AlogQueue * alog_get_shared_fga_queue(int idx)
{
    AlogQueue * queue = NULL;

    Assert(idx >= 0 && idx < MaxBackends);
    queue = AuditFGALogQueueArray->a_queue[idx];

    return queue;
}

static AlogQueue * alog_get_shared_trace_queue(int idx)
{
	AlogQueue * queue = NULL;

	Assert(idx >= 0 && idx < MaxBackends);
	queue = AuditTraceLogQueueArray->a_queue[idx];

	return queue;
}

static AlogQueue * alog_get_local_common_cache(int consumer_id)
{
    AlogQueue * queue = NULL;

    Assert(consumer_id >= 0 && consumer_id < AuditLog_max_worker_number);
    queue = AuditCommonLogLocalCache->q_cache[consumer_id];

    return queue;
}

static AlogQueue * alog_get_local_fga_cache(int consumer_id)
{
    AlogQueue * queue = NULL;

    Assert(consumer_id >= 0 && consumer_id < AuditLog_max_worker_number);
    queue = AuditFGALogLocalCache->q_cache[consumer_id];

    return queue;
}

static AlogQueue * alog_get_local_trace_cache(int consumer_id)
{
	AlogQueue * queue = NULL;

	Assert(consumer_id >= 0 && consumer_id < AuditLog_max_worker_number);
	queue = AuditTraceLogLocalCache->q_cache[consumer_id];

	return queue;
}

/*
 * local cache for log Consumer
 *
 * AuditCommonLogLocalCache = alog_make_local_cache(AuditLog_max_worker_number,
 * 		AuditLog_common_log_cache_size_kb);
 *
 * AuditFGALogLocalCache = alog_make_local_cache(AuditLog_max_worker_number,
 * 		AuditLog_fga_log_cacae_size_kb);
 *
 * AuditTraceLogLocalCache = alog_make_local_cache(AuditLog_max_worker_number,
 * 		Maintain_trace_log_cache_size_kb);
 */
static AlogQueueCache * alog_make_local_cache(int cache_number, int queue_size_kb)
{
    Size headerSize = 0;
    Size cacheSize = 0;
    Size queueSize = 0;

    AlogQueueCache * cache = NULL;
    ThreadSema * sema = NULL;

    int i = 0;

    headerSize = offsetof(AlogQueueCache, q_cache);
    headerSize = add_size(headerSize, cache_number * sizeof(AlogQueue *));

    queueSize = audit_queue_elem_size(queue_size_kb);

    cacheSize = queueSize * cache_number;
    cacheSize = add_size(cacheSize, headerSize);

    cache = palloc0(cacheSize);
    sema = &(cache->q_sema);

    for (i = 0; i < cache_number; i++)
    {
        AlogQueue * queue = NULL;

        queue = (AlogQueue *)(((char *) cache) + headerSize + i * queueSize);

        alog_queue_init(queue, queue_size_kb);

        cache->q_cache[i] = queue;
    }

    ThreadSemaInit(sema, 0);
    cache->q_count = cache_number;

    return cache;
}

/*
 * make local ThreadSema array for AuditLog_max_worker_number consumers
 */
static ThreadSema *    alog_make_consumer_semas(int consumer_count)
{
    ThreadSema * sema_array = NULL;
    int i = 0;

    Assert(consumer_count == AuditLog_max_worker_number);

    sema_array = palloc0(consumer_count * sizeof(ThreadSema));
    for (i = 0; i < consumer_count; i++)
    {
        ThreadSema * sema = (&(sema_array[i]));
        ThreadSemaInit(sema, 0);
    }

    return sema_array;
}

/*
 * Wakeup a consumer thread to read audit log
 * from shared audit log queue
 */
static void alog_consumer_wakeup(int consumer_id)
{
    ThreadSema * sema = NULL;

    Assert(consumer_id >= 0 && consumer_id < AuditLog_max_worker_number);

    sema = (&(AuditConsumerNotifySemas[consumer_id]));
    ThreadSemaUp(sema);
}

/*
 * Sleep if there is no log to read in
 * shared audit log queue
 */
static void alog_consumer_sleep(int consumer_id)
{
    ThreadSema * sema = NULL;

    Assert(consumer_id >= 0 && consumer_id < AuditLog_max_worker_number);

    sema = (&(AuditConsumerNotifySemas[consumer_id]));
    ThreadSemaDown(sema);
}

/*
 * AuditLog_max_worker_number consumers
 *
 * read log from part of AuditCommonLogQueueArray, AuditFGALogQueueArray and
 * AuditTraceLogQueueArray; write to one cache in AuditCommonLogLocalCache,
 * AuditFgaLogQueueCache and AuditTraceLogQueueCache.
 *
 */
static void * alog_consumer_main(void * arg)
{
	int consumer_id = *((int *) arg);
	int i = 0;

	AlogQueue * local_common_cache = NULL;
	AlogQueue * local_fga_cache = NULL;
	AlogQueue * local_trace_cache = NULL;

	Assert(consumer_id >= 0 && consumer_id < AuditLog_max_worker_number);

	/* get local common queue cache entry from AuditCommonLogLocalCache */
	local_common_cache = alog_get_local_common_cache(consumer_id);

	/* get local fga queue cache entry from AuditFgaLogQueueCache */
	local_fga_cache = alog_get_local_fga_cache(consumer_id);

	/* get local trace queue cache entry from AuditTraceLogQueueCache */
	local_trace_cache = alog_get_local_trace_cache(consumer_id);

	while (true)
	{
		bool shared_is_empty = true;

		for (i = 0; i < ((MaxBackends / AuditLog_max_worker_number) + 1); i++)
		{
			int sharedIdx = consumer_id +  i * AuditLog_max_worker_number;
			AlogQueue * shared_common_queue = NULL;
			AlogQueue * shared_fga_queue = NULL;
			AlogQueue * shared_trace_queue = NULL;

			Assert(consumer_id == (sharedIdx % AuditLog_max_worker_number));

			if (sharedIdx < MaxBackends)
			{
				bool local_is_empty = false;

				/* get shared common queue entry from AuditCommonLogQueueArray */
				shared_common_queue = alog_get_shared_common_queue(sharedIdx);

				/* get shared fga queue entry from AuditFGALogQueueArray */
				shared_fga_queue = alog_get_shared_fga_queue(sharedIdx);

				/* get shared trace queue entry from AuditTraceLogQueueArray */
				shared_trace_queue = alog_get_shared_trace_queue(sharedIdx);

				local_is_empty = false;
				if (alog_queue_is_empty2(local_common_cache))
				{
					local_is_empty = true;
				}

				/* read from shared queue, and write to local cache queue */
				if (alog_queue_pop_to_queue(shared_common_queue, local_common_cache))
				{
					if (local_is_empty)
					{
						alog_writer_wakeup(AUDIT_COMMON_LOG);
					}
				}

				local_is_empty = false;
				if (alog_queue_is_empty2(local_fga_cache))
				{
					local_is_empty = true;
				}

				if (alog_queue_pop_to_queue(shared_fga_queue, local_fga_cache))
				{
					if (local_is_empty)
					{
						alog_writer_wakeup(AUDIT_FGA_LOG);
					}
				}

				local_is_empty = false;
				if (alog_queue_is_empty2(local_trace_cache))
				{
					local_is_empty = true;
				}

				if (alog_queue_pop_to_queue(shared_trace_queue, local_trace_cache))
				{
					if (local_is_empty)
					{
						alog_writer_wakeup(MAINTAIN_TRACE_LOG);
					}
				}

				if (!alog_queue_is_empty2(shared_common_queue) ||
					!alog_queue_is_empty2(shared_fga_queue) ||
					!alog_queue_is_empty2(shared_trace_queue))
				{
					shared_is_empty = false;
				}
			}
		}

		if (shared_is_empty)
		{
			/*
			 * maybe shared input is empty,
			 * local output is full,
			 * so wait a moment and retry
			 */
			audit_shared_consumer_bitmap_set_value(consumer_id, 0);
			alog_consumer_sleep(consumer_id);
		}
	}

	return NULL;
}

/*
 * Wakeup a writer thread to read audit log
 * from local audit log cache
 */
static void alog_writer_wakeup(int writer_destination)
{
    ThreadSema * sema = NULL;
    AlogQueueCache * local_cache = NULL;

	Assert(writer_destination == AUDIT_COMMON_LOG ||
		   writer_destination == AUDIT_FGA_LOG ||
		   writer_destination == MAINTAIN_TRACE_LOG);

	if (writer_destination == AUDIT_COMMON_LOG)
	{
		local_cache = AuditCommonLogLocalCache;
	}
	else if (writer_destination == AUDIT_FGA_LOG)
	{
		local_cache = AuditFGALogLocalCache;
	}
	else
	{
		Assert(writer_destination == MAINTAIN_TRACE_LOG);
		local_cache = AuditTraceLogLocalCache;
	}

    sema = (&(local_cache->q_sema));
    ThreadSemaUp(sema);
}

/*
 * Sleep if there is no log to read in
 * local audit log cache
 */
static void alog_writer_sleep(int writer_destination)
{
    ThreadSema * sema = NULL;
    AlogQueueCache * local_cache = NULL;

	Assert(writer_destination == AUDIT_COMMON_LOG ||
		   writer_destination == AUDIT_FGA_LOG ||
		   writer_destination == MAINTAIN_TRACE_LOG);

	if (writer_destination == AUDIT_COMMON_LOG)
	{
		local_cache = AuditCommonLogLocalCache;
	}
	else if (writer_destination == AUDIT_FGA_LOG)
	{
		local_cache = AuditFGALogLocalCache;
	}
	else
	{
		Assert(writer_destination == MAINTAIN_TRACE_LOG);
		local_cache = AuditTraceLogLocalCache;
	}

    sema = (&(local_cache->q_sema));
    ThreadSemaDown(sema);
}

/*
 * three writer, write log to logfile
 *
 * one for AuditCommonLogLocalCache
 * one for AuditFgaLogQueueCache
 * one for AuditTraceLogQueueCache
 */
static void * alog_writer_main(void * arg)
{
	int writer_destination = *((int *) arg);
	AlogQueueCache * local_cache = NULL;

	Assert(writer_destination == AUDIT_COMMON_LOG ||
		writer_destination == AUDIT_FGA_LOG ||
		writer_destination == MAINTAIN_TRACE_LOG);

	if (writer_destination == AUDIT_COMMON_LOG)
	{
		/* read from AuditCommonLogLocalCache, and write to common log file */
		local_cache = AuditCommonLogLocalCache;
	}
	else if (writer_destination == AUDIT_FGA_LOG)
	{
		/* read from AuditFgaLogQueueCache, and write to fga log file */
		local_cache = AuditFGALogLocalCache;
	}
	else
	{
		/* read from AuditTraceLogQueueCache, and write to trace log file */
		Assert(writer_destination == MAINTAIN_TRACE_LOG);
		local_cache = AuditTraceLogLocalCache;
	}

	while (true)
	{
		int i = 0;
		bool copy_nothing = true;

		for (i = 0; i < AuditLog_max_worker_number; i++)
		{
			int consumer_id = i;
			AlogQueue * local_queue = local_cache->q_cache[i];

			if (alog_queue_pop_to_file(local_queue, writer_destination))
			{
				copy_nothing = false;
			}

			if (alog_queue_is_empty2(local_queue))
			{
				alog_consumer_wakeup(consumer_id);
			}
		}

		if (copy_nothing)
		{
			/*
			 * maybe local input is empty,
			 * so wait a moment and retry
			 */
			alog_writer_sleep(writer_destination);
		}
	}

	return NULL;
}

static void alog_start_writer(int writer_destination)
{
    int * des = NULL;
    int ret = 0;

	Assert(writer_destination == AUDIT_COMMON_LOG ||
		writer_destination == AUDIT_FGA_LOG ||
		writer_destination == MAINTAIN_TRACE_LOG);

    des = palloc0(sizeof(int));
    *des = writer_destination;

	ret = CreateThread(alog_writer_main, (void *)des, MT_THR_DETACHED);
	if (ret != 0)
	{
		/* failed to create thread, exit */
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not start audit log write worker")));
		exit(6);
	}
}

static void alog_start_consumer(int consumer_id)
{
    int * id = NULL;
    int ret = 0;

    Assert(consumer_id >= 0 && consumer_id < AuditLog_max_worker_number);

    id = palloc0(sizeof(int));
    *id = consumer_id;

    ret = CreateThread(alog_consumer_main, (void *)id, MT_THR_DETACHED);
    if (ret != 0)
    {
        /* failed to create thread, exit */
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("could not start audit log consumer worker")));
        exit(7);
    }
}

static void alog_start_all_worker(void)
{
    int i = 0;

	AuditCommonLogLocalCache = alog_make_local_cache(AuditLog_max_worker_number,
											AuditLog_common_log_cache_size_kb);
	AuditFGALogLocalCache = alog_make_local_cache(AuditLog_max_worker_number,
											AuditLog_fga_log_cacae_size_kb);
	AuditTraceLogLocalCache = alog_make_local_cache(AuditLog_max_worker_number,
											Maintain_trace_log_cache_size_kb);
	AuditConsumerNotifySemas = alog_make_consumer_semas(AuditLog_max_worker_number);

	/* 00, start writer worker, one for common log, one for fga log, one for trace log. */
	alog_start_writer(AUDIT_COMMON_LOG);
	alog_start_writer(AUDIT_FGA_LOG);
	alog_start_writer(MAINTAIN_TRACE_LOG);

    /* 001, start AuditLog_max_worker_number consumer worker */
    for (i = 0; i < AuditLog_max_worker_number; i++)
    {
        alog_start_consumer(i);
    }

    /* 002, start a worker for shard statistics */
    alog_start_shard_stat_worker();
}

#endif

#ifdef AuditLog_006_For_Elog

void alog(int destination, const char *fmt,...)
{
	StringInfoData buf;
	AlogQueue * queue = NULL;

	int len = 0;
	int idx = 0;
	int consumer_id = 0;

	Assert(AuditPostgresAlogQueueIndex >= 0 &&
		   AuditPostgresAlogQueueIndex < MaxBackends);

	idx = AuditPostgresAlogQueueIndex;
	consumer_id = (idx % AuditLog_max_worker_number);

	if(destination != AUDIT_COMMON_LOG &&
		destination != AUDIT_FGA_LOG &&
		destination != MAINTAIN_TRACE_LOG)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("only common/fga/trace audit log can be processed")));
		return;
	}

	if (!IsBackendPostgres ||
		!IsUnderPostmaster)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("only postgres backend can write audit log")));
		return;
	}

	if (destination == AUDIT_COMMON_LOG)
	{
		queue = alog_get_shared_common_queue(idx);
	}
	else if (destination == AUDIT_FGA_LOG)
	{
		queue = alog_get_shared_fga_queue(idx);
	}
	else
	{
		Assert(destination == MAINTAIN_TRACE_LOG);
		queue = alog_get_shared_trace_queue(idx);
	}

	Assert(queue->q_pid == getpid());

	initStringInfo(&buf);
	appendBinaryStringInfo(&buf, (const char *)(&len), sizeof(len));

	for (;;)
	{
		va_list		args;
		int			needed;
		va_start(args, fmt);
		needed = appendStringInfoVA(&buf, fmt, args);
		va_end(args);
		if (needed == 0)
		{
			break;
		}
		enlargeStringInfo(&buf, needed);
	}

	appendStringInfoChar(&buf, '\n');

	/* push string len to header */
	len = buf.len - sizeof(len);
	memcpy(buf.data, (char *)(&len), sizeof(len));

	/* push total buff into queue */
	len = buf.len;
	while (false == alog_queue_push(queue, buf.data, len))
	{
		if (!audit_shared_consumer_bitmap_get_value(consumer_id))
		{
			/*
			 * set shared consumer bitmap value to 1 to
			 * notify consumer to read log
			 */
			audit_shared_consumer_bitmap_set_value(consumer_id, 1);

			/* Notify logger process that it's got something to do */
			SendPostmasterSignal(PMSIGNAL_WAKEN_AUDIT_LOGGER);
		}

		pg_usleep(AUDIT_SLEEP_MICROSEC);
	}

	pfree(buf.data);

	if (!audit_shared_consumer_bitmap_get_value(consumer_id))
	{
		/*
		 * set shared consumer bitmap value to 1 to
		 * notify consumer to read audit log
		 */
		audit_shared_consumer_bitmap_set_value(consumer_id, 1);

		/* Notify audit logger process that it's got something to do */
		SendPostmasterSignal(PMSIGNAL_WAKEN_AUDIT_LOGGER);
	}
}

#endif

#ifdef AuditLog_007_For_ShardStatistics
static void * alog_shard_stat_main(void * arg)
{
	atexit(FlushShardStatistic);

	while (true)
	{
		long shard_stat_interval = g_ShardInfoFlushInterval * 1000000L;

		FlushShardStatistic();

		pg_usleep(shard_stat_interval);
	}

    return NULL;
}

static void    alog_start_shard_stat_worker(void)
{
    int ret = 0;

    ret = CreateThread(alog_shard_stat_main, (void *) NULL, MT_THR_DETACHED);
    if (ret != 0)
    {
        /* failed to create thread, exit */
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("could not start shard stat worker")));
        exit(8);
    }
}

#endif
