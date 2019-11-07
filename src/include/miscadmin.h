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
 * miscadmin.h
 *      This file contains general postgres administration and initialization
 *      stuff that used to be spread out between the following files:
 *        globals.h                        global variables
 *        pdir.h                            directory path crud
 *        pinit.h                            postgres initialization
 *        pmod.h                            processing modes
 *      Over time, this has also become the preferred place for widely known
 *      resource-limitation stuff, such as work_mem and check_stack_depth().
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/miscadmin.h
 *
 * NOTES
 *      some of the information in this file should be moved to other files.
 *
 *-------------------------------------------------------------------------
 */
#ifndef MISCADMIN_H
#define MISCADMIN_H

#include <signal.h>

#include "pgtime.h"                /* for pg_time_t */


#define InvalidPid                (-1)


/*****************************************************************************
 *      System interrupt and critical section handling
 *
 * There are two types of interrupts that a running backend needs to accept
 * without messing up its state: QueryCancel (SIGINT) and ProcDie (SIGTERM).
 * In both cases, we need to be able to clean up the current transaction
 * gracefully, so we can't respond to the interrupt instantaneously ---
 * there's no guarantee that internal data structures would be self-consistent
 * if the code is interrupted at an arbitrary instant.  Instead, the signal
 * handlers set flags that are checked periodically during execution.
 *
 * The CHECK_FOR_INTERRUPTS() macro is called at strategically located spots
 * where it is normally safe to accept a cancel or die interrupt.  In some
 * cases, we invoke CHECK_FOR_INTERRUPTS() inside low-level subroutines that
 * might sometimes be called in contexts that do *not* want to allow a cancel
 * or die interrupt.  The HOLD_INTERRUPTS() and RESUME_INTERRUPTS() macros
 * allow code to ensure that no cancel or die interrupt will be accepted,
 * even if CHECK_FOR_INTERRUPTS() gets called in a subroutine.  The interrupt
 * will be held off until CHECK_FOR_INTERRUPTS() is done outside any
 * HOLD_INTERRUPTS() ... RESUME_INTERRUPTS() section.
 *
 * There is also a mechanism to prevent query cancel interrupts, while still
 * allowing die interrupts: HOLD_CANCEL_INTERRUPTS() and
 * RESUME_CANCEL_INTERRUPTS().
 *
 * Special mechanisms are used to let an interrupt be accepted when we are
 * waiting for a lock or when we are waiting for command input (but, of
 * course, only if the interrupt holdoff counter is zero).  See the
 * related code for details.
 *
 * A lost connection is handled similarly, although the loss of connection
 * does not raise a signal, but is detected when we fail to write to the
 * socket. If there was a signal for a broken connection, we could make use of
 * it by setting ClientConnectionLost in the signal handler.
 *
 * A related, but conceptually distinct, mechanism is the "critical section"
 * mechanism.  A critical section not only holds off cancel/die interrupts,
 * but causes any ereport(ERROR) or ereport(FATAL) to become ereport(PANIC)
 * --- that is, a system-wide reset is forced.  Needless to say, only really
 * *critical* code should be marked as a critical section!    Currently, this
 * mechanism is only used for XLOG-related code.
 *
 *****************************************************************************/

/* in globals.c */
/* these are marked volatile because they are set by signal handlers: */
extern PGDLLIMPORT volatile bool InterruptPending;
extern PGDLLIMPORT volatile bool QueryCancelPending;
extern PGDLLIMPORT volatile bool ProcDiePending;
extern PGDLLIMPORT volatile bool IdleInTransactionSessionTimeoutPending;
extern PGDLLIMPORT volatile sig_atomic_t ConfigReloadPending;

extern volatile bool ClientConnectionLost;

/* these are marked volatile because they are examined by signal handlers: */
extern PGDLLIMPORT volatile uint32 InterruptHoldoffCount;
extern PGDLLIMPORT volatile uint32 QueryCancelHoldoffCount;
extern PGDLLIMPORT volatile uint32 CritSectionCount;

#ifdef __TBASE__
extern PGDLLIMPORT volatile int PoolerReloadHoldoffCount;
extern PGDLLIMPORT volatile int PoolerReloadPending;
#endif

/* in tcop/postgres.c */
extern void ProcessInterrupts(void);
extern bool IsQueryCancelPending(void);


#ifndef WIN32

#define CHECK_FOR_INTERRUPTS() \
do { \
    if (InterruptPending) \
        ProcessInterrupts(); \
} while(0)
#else                            /* WIN32 */

#define CHECK_FOR_INTERRUPTS() \
do { \
    if (UNBLOCKED_SIGNAL_QUEUE()) \
        pgwin32_dispatch_queued_signals(); \
    if (InterruptPending) \
        ProcessInterrupts(); \
} while(0)
#endif                            /* WIN32 */


#define HOLD_INTERRUPTS()  (InterruptHoldoffCount++)

#define RESUME_INTERRUPTS() \
do { \
    Assert(InterruptHoldoffCount > 0); \
    InterruptHoldoffCount--; \
} while(0)

#define HOLD_CANCEL_INTERRUPTS()  (QueryCancelHoldoffCount++)

#define RESUME_CANCEL_INTERRUPTS() \
do { \
    Assert(QueryCancelHoldoffCount > 0); \
    QueryCancelHoldoffCount--; \
} while(0)

#define START_CRIT_SECTION()  (CritSectionCount++)

#define END_CRIT_SECTION() \
do { \
    Assert(CritSectionCount > 0); \
    CritSectionCount--; \
} while(0)


#ifdef __TBASE__
#define HOLD_POOLER_RELOAD()  (PoolerReloadHoldoffCount++)

#define RESUME_POOLER_RELOAD() \
do { \
    Assert(PoolerReloadHoldoffCount > 0); \
    if(PoolerReloadHoldoffCount > 0)    \
        PoolerReloadHoldoffCount--; \
} while(0)

#define CHECK_FOR_POOLER_RELOAD() \
do { \
    if (PoolerReloadPending) \
        HandlePoolerReload(); \
} while(0)
#endif


/*****************************************************************************
 *      globals.h --                                                             *
 *****************************************************************************/

/*
 * from utils/init/globals.c
 */
extern PGDLLIMPORT pid_t PostmasterPid;
extern PGDLLIMPORT bool IsPostmasterEnvironment;
extern PGDLLIMPORT bool IsUnderPostmaster;
extern PGDLLIMPORT bool IsBackgroundWorker;
extern PGDLLIMPORT bool IsBinaryUpgrade;

#ifdef __AUDIT__
extern PGDLLIMPORT bool IsBackendPostgres;
#endif

extern bool ExitOnAnyError;

extern PGDLLIMPORT char *DataDir;

extern PGDLLIMPORT int NBuffers;
extern int    MaxBackends;
extern int    MaxConnections;
extern int    max_worker_processes;
extern int    max_parallel_workers;

extern PGDLLIMPORT int MyProcPid;
extern PGDLLIMPORT pg_time_t MyStartTime;
extern PGDLLIMPORT struct Port *MyProcPort;
extern PGDLLIMPORT struct Latch *MyLatch;
extern int32 MyCancelKey;
extern int    MyPMChildSlot;

extern char OutputFileName[];
extern PGDLLIMPORT char my_exec_path[];
extern char pkglib_path[];

#ifdef EXEC_BACKEND
extern char postgres_exec_path[];
#endif

/*
 * done in storage/backendid.h for now.
 *
 * extern BackendId    MyBackendId;
 */
extern PGDLLIMPORT Oid MyDatabaseId;

extern PGDLLIMPORT Oid MyDatabaseTableSpace;

/*
 * Date/Time Configuration
 *
 * DateStyle defines the output formatting choice for date/time types:
 *    USE_POSTGRES_DATES specifies traditional Postgres format
 *    USE_ISO_DATES specifies ISO-compliant format
 *    USE_SQL_DATES specifies Oracle/Ingres-compliant format
 *    USE_GERMAN_DATES specifies German-style dd.mm/yyyy
 *
 * DateOrder defines the field order to be assumed when reading an
 * ambiguous date (anything not in YYYY-MM-DD format, with a four-digit
 * year field first, is taken to be ambiguous):
 *    DATEORDER_YMD specifies field order yy-mm-dd
 *    DATEORDER_DMY specifies field order dd-mm-yy ("European" convention)
 *    DATEORDER_MDY specifies field order mm-dd-yy ("US" convention)
 *
 * In the Postgres and SQL DateStyles, DateOrder also selects output field
 * order: day comes before month in DMY style, else month comes before day.
 *
 * The user-visible "DateStyle" run-time parameter subsumes both of these.
 */

/* valid DateStyle values */
#define USE_POSTGRES_DATES        0
#define USE_ISO_DATES            1
#define USE_SQL_DATES            2
#define USE_GERMAN_DATES        3
#define USE_XSD_DATES            4

/* valid DateOrder values */
#define DATEORDER_YMD            0
#define DATEORDER_DMY            1
#define DATEORDER_MDY            2

extern PGDLLIMPORT int DateStyle;
extern PGDLLIMPORT int DateOrder;

/*
 * IntervalStyles
 *     INTSTYLE_POSTGRES               Like Postgres < 8.4 when DateStyle = 'iso'
 *     INTSTYLE_POSTGRES_VERBOSE       Like Postgres < 8.4 when DateStyle != 'iso'
 *     INTSTYLE_SQL_STANDARD           SQL standard interval literals
 *     INTSTYLE_ISO_8601               ISO-8601-basic formatted intervals
 */
#define INTSTYLE_POSTGRES            0
#define INTSTYLE_POSTGRES_VERBOSE    1
#define INTSTYLE_SQL_STANDARD        2
#define INTSTYLE_ISO_8601            3

extern PGDLLIMPORT int IntervalStyle;

#define MAXTZLEN        10        /* max TZ name len, not counting tr. null */

extern bool enableFsync;
extern bool allowSystemTableMods;
extern PGDLLIMPORT int work_mem;
extern PGDLLIMPORT int maintenance_work_mem;
extern PGDLLIMPORT int replacement_sort_tuples;

extern int    VacuumCostPageHit;
extern int    VacuumCostPageMiss;
extern int    VacuumCostPageDirty;
extern int    VacuumCostLimit;
extern int    VacuumCostDelay;

extern int    VacuumPageHit;
extern int    VacuumPageMiss;
extern int    VacuumPageDirty;

extern int    VacuumCostBalance;
extern bool VacuumCostActive;

#ifdef PGXC
extern bool useLocalXid;
#endif


/* in tcop/postgres.c */

#if defined(__ia64__) || defined(__ia64)
typedef struct
{
    char       *stack_base_ptr;
    char       *register_stack_base_ptr;
} pg_stack_base_t;
#else
typedef char *pg_stack_base_t;
#endif

extern pg_stack_base_t set_stack_base(void);
extern void restore_stack_base(pg_stack_base_t base);
extern void check_stack_depth(void);
extern bool stack_is_too_deep(void);

extern void PostgresSigHupHandler(SIGNAL_ARGS);

/* in tcop/utility.c */
extern void PreventCommandIfReadOnly(const char *cmdname);
extern void PreventCommandIfParallelMode(const char *cmdname);
extern void PreventCommandDuringRecovery(const char *cmdname);

/* in utils/misc/guc.c */
extern int    trace_recovery_messages;
extern int    trace_recovery(int trace_level);

/*****************************************************************************
 *      pdir.h --                                                                 *
 *            POSTGRES directory path definitions.                             *
 *****************************************************************************/

/* flags to be OR'd to form sec_context */
#define SECURITY_LOCAL_USERID_CHANGE    0x0001
#define SECURITY_RESTRICTED_OPERATION    0x0002
#define SECURITY_NOFORCE_RLS            0x0004

extern char *DatabasePath;

/* now in utils/init/miscinit.c */
extern void InitPostmasterChild(void);
extern void InitStandaloneProcess(const char *argv0);

extern void SetDatabasePath(const char *path);

extern char *GetUserNameFromId(Oid roleid, bool noerr);
extern Oid    GetUserId(void);
extern Oid    GetOuterUserId(void);
extern Oid    GetSessionUserId(void);
extern Oid    GetAuthenticatedUserId(void);
extern void GetUserIdAndSecContext(Oid *userid, int *sec_context);
extern void SetUserIdAndSecContext(Oid userid, int sec_context);
extern bool InLocalUserIdChange(void);
extern bool InSecurityRestrictedOperation(void);
extern bool InNoForceRLSOperation(void);
extern void GetUserIdAndContext(Oid *userid, bool *sec_def_context);
extern void SetUserIdAndContext(Oid userid, bool sec_def_context);
extern void InitializeSessionUserId(const char *rolename, Oid useroid);
extern void InitializeSessionUserIdStandalone(void);
extern void SetSessionAuthorization(Oid userid, bool is_superuser);
#ifdef XCP
extern void SetGlobalSession(Oid coordid, int coordpid);
extern char *GetClusterUserName(void);
#endif
extern Oid    GetCurrentRoleId(void);
extern void SetCurrentRoleId(Oid roleid, bool is_superuser);

extern void SetDataDir(const char *dir);
extern void ChangeToDataDir(void);

extern void SwitchToSharedLatch(void);
extern void SwitchBackToLocalLatch(void);

/* in utils/misc/superuser.c */
extern bool superuser(void);    /* current user is superuser */
extern bool superuser_arg(Oid roleid);    /* given user is superuser */


/*****************************************************************************
 *      pmod.h --                                                                 *
 *            POSTGRES processing mode definitions.                            *
 *****************************************************************************/

/*
 * Description:
 *        There are three processing modes in POSTGRES.  They are
 * BootstrapProcessing or "bootstrap," InitProcessing or
 * "initialization," and NormalProcessing or "normal."
 *
 * The first two processing modes are used during special times. When the
 * system state indicates bootstrap processing, transactions are all given
 * transaction id "one" and are consequently guaranteed to commit. This mode
 * is used during the initial generation of template databases.
 *
 * Initialization mode: used while starting a backend, until all normal
 * initialization is complete.  Some code behaves differently when executed
 * in this mode to enable system bootstrapping.
 *
 * If a POSTGRES backend process is in normal mode, then all code may be
 * executed normally.
 */

typedef enum ProcessingMode
{
    BootstrapProcessing,        /* bootstrap creation of template database */
    InitProcessing,                /* initializing system */
    NormalProcessing            /* normal processing */
} ProcessingMode;

extern ProcessingMode Mode;

#define IsBootstrapProcessingMode() (Mode == BootstrapProcessing)
#define IsInitProcessingMode()        (Mode == InitProcessing)
#define IsNormalProcessingMode()    (Mode == NormalProcessing)

#define GetProcessingMode() Mode

#define SetProcessingMode(mode) \
    do { \
        AssertArg((mode) == BootstrapProcessing || \
                  (mode) == InitProcessing || \
                  (mode) == NormalProcessing); \
        Mode = (mode); \
    } while(0)


/*
 * Auxiliary-process type identifiers.  These used to be in bootstrap.h
 * but it seems saner to have them here, with the ProcessingMode stuff.
 * The MyAuxProcType global is defined and set in bootstrap.c.
 */

typedef enum
{
    NotAnAuxProcess = -1,
    CheckerProcess = 0,
    BootstrapProcess,
    StartupProcess,
    BgWriterProcess,
    CheckpointerProcess,
    WalWriterProcess,
    WalReceiverProcess,
#ifdef PGXC
    PoolerProcess,
    ClusterMonitorProcess,
#endif   

    NUM_AUXPROCTYPES            /* Must be last! */
} AuxProcType;

extern AuxProcType MyAuxProcType;

#define AmBootstrapProcess()        (MyAuxProcType == BootstrapProcess)
#define AmStartupProcess()            (MyAuxProcType == StartupProcess)
#define AmBackgroundWriterProcess() (MyAuxProcType == BgWriterProcess)
#define AmCheckpointerProcess()        (MyAuxProcType == CheckpointerProcess)
#define AmWalWriterProcess()        (MyAuxProcType == WalWriterProcess)
#define AmWalReceiverProcess()        (MyAuxProcType == WalReceiverProcess)
#define AmClusterMonitorProcess()    (MyAuxProcType == ClusterMonitorProcess)

#ifdef __SUBSCRIPTION__
extern bool AmTbaseSubscriptionWalSender(void);
extern bool AmTbaseSubscriptionApplyWorker(void);
#endif

/*****************************************************************************
 *      pinit.h --                                                             *
 *            POSTGRES initialization and cleanup definitions.                 *
 *****************************************************************************/

/* in utils/init/postinit.c */
extern void pg_split_opts(char **argv, int *argcp, const char *optstr);
extern void InitializeMaxBackends(void);
extern void InitPostgres(const char *in_dbname, Oid dboid, const char *username,
             Oid useroid, char *out_dbname);
extern void BaseInit(void);

/* in utils/init/miscinit.c */
extern bool IgnoreSystemIndexes;
extern PGDLLIMPORT bool process_shared_preload_libraries_in_progress;
extern char *session_preload_libraries_string;
extern char *shared_preload_libraries_string;
extern char *local_preload_libraries_string;

extern void CreateDataDirLockFile(bool amPostmaster);
extern void ForgetLockFiles(void);
extern void CreateSocketLockFile(const char *socketfile, bool amPostmaster,
                     const char *socketDir);
extern void TouchSocketLockFiles(void);
extern void AddToDataDirLockFile(int target_line, const char *str);
extern bool RecheckDataDirLockFile(void);
extern void ValidatePgVersion(const char *path);
extern void process_shared_preload_libraries(void);
extern void process_session_preload_libraries(void);
extern void pg_bindtextdomain(const char *domain);
extern bool has_rolreplication(Oid roleid);

/* in access/transam/xlog.c */
extern bool BackupInProgress(void);
extern void CancelBackup(void);

#endif                            /* MISCADMIN_H */
