/*-------------------------------------------------------------------------
 *
 * clustermon.c
 *
 * Postgres-XL Cluster Monitor
 *
 * Portions Copyright (c) 2015, 2ndQuadrant Ltd
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *      src/backend/postmaster/clustermon.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <sys/types.h>
#include <sys/time.h>
#include <unistd.h>

#include "access/gtm.h"
#include "access/transam.h"
#include "access/xact.h"
#include "gtm/gtm_c.h"
#include "gtm/gtm_gxid.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgxc/pgxc.h"
#include "postmaster/clustermon.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/spin.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/timeout.h"
#include "utils/timestamp.h"
#include "pgstat.h"

/* Flags to tell if we are in a clustermon process */
static bool am_clustermon = false;

/* Flags set by signal handlers */
static volatile sig_atomic_t got_SIGHUP = false;
static volatile sig_atomic_t got_SIGTERM = false;

/* Memory context for long-lived data */
static MemoryContext ClusterMonitorMemCxt;
static ClusterMonitorCtlData *ClusterMonitorCtl = NULL; 

static void cm_sighup_handler(SIGNAL_ARGS);
static void cm_sigterm_handler(SIGNAL_ARGS);
#ifdef __USE_GLOBAL_SNAPSHOT__
static void ClusterMonitorSetReportedGlobalXmin(GlobalTransactionId xmin);
static void ClusterMonitorSetReportingGlobalXmin(GlobalTransactionId xmin);
#endif
/* PID of clustser monitoring process */
int            ClusterMonitorPid = 0;

#define CLUSTER_MONITOR_NAPTIME    5

/*
 * Main loop for the cluster monitor process.
 */
int
ClusterMonitorInit(void)
{// #lizard forgives
    sigjmp_buf    local_sigjmp_buf;
    GTM_PGXCNodeType nodetype = IS_PGXC_DATANODE ?
                                    GTM_NODE_DATANODE :
                                    GTM_NODE_COORDINATOR;
#ifdef __USE_GLOBAL_SNAPSHOT__
    GlobalTransactionId oldestXmin;
    GlobalTransactionId newOldestXmin;
    GlobalTransactionId lastGlobalXmin;
    GlobalTransactionId latestCompletedXid;
    int status;
#endif
    am_clustermon = true;

    /* Identify myself via ps */
    init_ps_display("cluster monitor process", "", "", "");

    ereport(LOG,
            (errmsg("cluster monitor started")));

    if (PostAuthDelay)
        pg_usleep(PostAuthDelay * 1000000L);

    /*
     * Set up signal handlers.  We operate on databases much like a regular
     * backend, so we use the same signal handling.  See equivalent code in
     * tcop/postgres.c.
     */
    pqsignal(SIGHUP, cm_sighup_handler);
    pqsignal(SIGINT, StatementCancelHandler);
    pqsignal(SIGTERM, cm_sigterm_handler);

    pqsignal(SIGQUIT, quickdie);
    InitializeTimeouts();        /* establishes SIGALRM handler */

    pqsignal(SIGPIPE, SIG_IGN);
    pqsignal(SIGUSR1, procsignal_sigusr1_handler);
    pqsignal(SIGFPE, FloatExceptionHandler);
    pqsignal(SIGCHLD, SIG_DFL);

    /*
     * Create a memory context that we will do all our work in.  We do this so
     * that we can reset the context during error recovery and thereby avoid
     * possible memory leaks.
     */
    ClusterMonitorMemCxt = AllocSetContextCreate(TopMemoryContext,
                                          "Cluster Monitor",
                                          ALLOCSET_DEFAULT_MINSIZE,
                                          ALLOCSET_DEFAULT_INITSIZE,
                                          ALLOCSET_DEFAULT_MAXSIZE);
    MemoryContextSwitchTo(ClusterMonitorMemCxt);

    SetProcessingMode(NormalProcessing);

    if (RegisterGTM(nodetype) < 0)
    {
        UnregisterGTM(nodetype);
        if (RegisterGTM(nodetype) < 0)
        {
            ereport(LOG,
                    (errcode(ERRCODE_IO_ERROR),
                     errmsg("Can not register node on GTM")));
        }
    }

    /*
     * If an exception is encountered, processing resumes here.
     *
     * This code is a stripped down version of PostgresMain error recovery.
     */
    if (sigsetjmp(local_sigjmp_buf, 1) != 0)
    {
        /* since not using PG_TRY, must reset error stack by hand */
        error_context_stack = NULL;

        /* Prevents interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /* Forget any pending QueryCancel or timeout request */
        disable_all_timeouts(false);
        QueryCancelPending = false;        /* second to avoid race condition */

        /* Report the error to the server log */
        EmitErrorReport();

        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        MemoryContextSwitchTo(ClusterMonitorMemCxt);
        FlushErrorState();

        /* Flush any leaked data in the top-level context */
        MemoryContextResetAndDeleteChildren(ClusterMonitorMemCxt);

        /* Now we can allow interrupts again */
        RESUME_INTERRUPTS();

        /* if in shutdown mode, no need for anything further; just go away */
        if (got_SIGTERM)
            goto shutdown;

        /*
         * Sleep at least 1 second after any error.  We don't want to be
         * filling the error logs as fast as we can.
         */
        pg_usleep(1000000L);
    }

    /* We can now handle ereport(ERROR) */
    PG_exception_stack = &local_sigjmp_buf;

    /* must unblock signals before calling rebuild_database_list */
    PG_SETMASK(&UnBlockSig);

    /*
     * Force statement_timeout and lock_timeout to zero to avoid letting these
     * settings prevent regular maintenance from being executed.
     */
    SetConfigOption("statement_timeout", "0", PGC_SUSET, PGC_S_OVERRIDE);
    SetConfigOption("lock_timeout", "0", PGC_SUSET, PGC_S_OVERRIDE);

    /* loop until shutdown request */
    while (!got_SIGTERM)
    {
        struct timeval nap;
        int            rc;

        /*
         * Repeat at CLUSTER_MONITOR_NAPTIME seconds interval
         */
        nap.tv_sec = CLUSTER_MONITOR_NAPTIME;
        nap.tv_usec = 0;

        /*
         * Wait until naptime expires or we get some type of signal (all the
         * signal handlers will wake us by calling SetLatch).
         */
        rc = WaitLatch(MyLatch,
                       WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                       (nap.tv_sec * 1000L) + (nap.tv_usec / 1000L),
                       WAIT_EVENT_CLUSTER_MONITOR_MAIN);

        ResetLatch(MyLatch);

        /* Process sinval catchup interrupts that happened while sleeping */
        ProcessCatchupInterrupt();

        /*
         * Emergency bailout if postmaster has died.  This is to avoid the
         * necessity for manual cleanup of all postmaster children.
         */
        if (rc & WL_POSTMASTER_DEATH)
            proc_exit(1);

        /* the normal shutdown case */
        if (got_SIGTERM)
            break;

        if (got_SIGHUP)
        {
            got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
        }
#ifdef __USE_GLOBAL_SNAPSHOT__

        /*
         * Compute RecentGlobalXmin, report it to the GTM and sleep for the set
         * interval. Keep doing this forever
         */
        lastGlobalXmin = ClusterMonitorGetGlobalXmin();
         LWLockAcquire(ClusterMonitorLock, LW_EXCLUSIVE);
        oldestXmin = GetOldestXminInternal(NULL, 0, true, lastGlobalXmin);
        ClusterMonitorSetReportingGlobalXmin(oldestXmin);
        LWLockRelease(ClusterMonitorLock);

        if ((status = ReportGlobalXmin(oldestXmin, &newOldestXmin,
                        &latestCompletedXid)))
        {
            elog(DEBUG1, "Failed (status %d) to report RecentGlobalXmin "
                    "- reported RecentGlobalXmin %d, received "
                    "RecentGlobalXmin %d, " "received latestCompletedXid %d",
                    status, oldestXmin, newOldestXmin,
                    latestCompletedXid);
            if (status == GTM_ERRCODE_TOO_OLD_XMIN ||
                status == GTM_ERRCODE_NODE_EXCLUDED)
            {
                /*
                 * If we haven't seen a new transaction for a very long time or
                 * were disconncted for a while or excluded from the xmin
                 * computation for any reason, our xmin calculation could be
                 * well in the past, especially because its capped by the
                 * latestCompletedXid which may not advance on an idle server.
                 * In such cases, use the value of latestCompletedXid as
                 * returned by GTM and then recompute local xmin.
                 *
                 * If the GTM's global xmin advances even further while we are
                 * ready with a new xmin, just repeat the entire exercise as
                 * long as GTM keeps returning us a more current value of
                 * latestCompletedXid and thus pushing forward our local xmin
                 * calculation
                 */
                if (GlobalTransactionIdIsValid(latestCompletedXid) &&
                        TransactionIdPrecedes(oldestXmin, latestCompletedXid))
                {
                    SetLatestCompletedXid(latestCompletedXid);
                    continue;
                }
            }
        }
        else
        {
            elog(DEBUG1, "Successfully reported xmin to GTM - reported_xmin %d,"
                    "received RecentGlobalXmin %d, "
                    "received latestCompletedXid %d", oldestXmin,
                    newOldestXmin, latestCompletedXid);

            SetLatestCompletedXid(latestCompletedXid);
            ClusterMonitorSetReportedGlobalXmin(oldestXmin);
            if (GlobalTransactionIdIsValid(newOldestXmin))
                ClusterMonitorSetGlobalXmin(newOldestXmin);
        }

        ClusterMonitorSetReportingGlobalXmin(InvalidGlobalTransactionId);
#endif

    }

    /* Normal exit from the cluster monitor is here */
shutdown:
    UnregisterGTM(nodetype);
    ereport(LOG,
            (errmsg("cluster monitor shutting down")));

    proc_exit(0);                /* done */
}

/* SIGHUP: set flag to re-read config file at next convenient time */
static void
cm_sighup_handler(SIGNAL_ARGS)
{
    int            save_errno = errno;

    got_SIGHUP = true;
    SetLatch(MyLatch);

    errno = save_errno;
}

/* SIGTERM: time to die */
static void
cm_sigterm_handler(SIGNAL_ARGS)
{
    int            save_errno = errno;

    got_SIGTERM = true;
    SetLatch(MyLatch);

    errno = save_errno;
}


/*
 * IsClusterMonitor functions
 *        Return whether this is either a cluster monitor process or a worker
 *        process.
 */
bool
IsClusterMonitorProcess(void)
{
    return am_clustermon;
}

/* Report shared-memory space needed by ClusterMonitor */
Size
ClusterMonitorShmemSize(void)
{
    return sizeof (ClusterMonitorCtlData);
}

void
ClusterMonitorShmemInit(void)
{
    bool        found;

    ClusterMonitorCtl = (ClusterMonitorCtlData *)
        ShmemInitStruct("Cluster Monitor Ctl", ClusterMonitorShmemSize(), &found);

    if (!found)
    {
        /* First time through, so initialize */
        MemSet(ClusterMonitorCtl, 0, ClusterMonitorShmemSize());
        SpinLockInit(&ClusterMonitorCtl->mutex);
    }
}

GlobalTransactionId
ClusterMonitorGetGlobalXmin(void)
{
    GlobalTransactionId xmin;

    SpinLockAcquire(&ClusterMonitorCtl->mutex);
    xmin = ClusterMonitorCtl->gtm_recent_global_xmin;
    SpinLockRelease(&ClusterMonitorCtl->mutex);

    return xmin;
}

void
ClusterMonitorSetGlobalXmin(GlobalTransactionId xmin)
{
    /*
     * First extend the commit logs. Even though we may not have actually
     * started any transactions in the new range, we must still extend the logs
     * so that later operations which rely on the RecentGlobalXmin to truncate
     * the logs work correctly.
     */
    ExtendLogs(xmin);

    LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

    /*
     * Do a consistency check to ensure that we NEVER have running transactions
     * with xmin less than what the GTM has already computed. While during
     * normal execution, this should never happen, if we ever been excluded
     * from the xmin calculation by the GTM while we are still running old
     * transactions, PANIC is our best bet to avoid corruption
     */ 
    ProcArrayCheckXminConsistency(xmin);

    SpinLockAcquire(&ClusterMonitorCtl->mutex);
    ClusterMonitorCtl->gtm_recent_global_xmin = xmin;
    SpinLockRelease(&ClusterMonitorCtl->mutex);

    LWLockRelease(ProcArrayLock);
}
#ifdef __USE_GLOBAL_SNAPSHOT__
static void
ClusterMonitorSetReportedGlobalXmin(GlobalTransactionId xmin)
{
    elog(DEBUG2, "ClusterMonitorSetReportedGlobalXmin - old %d, new %d",
            ClusterMonitorCtl->reported_recent_global_xmin,
            xmin);
    SpinLockAcquire(&ClusterMonitorCtl->mutex);
    ClusterMonitorCtl->reported_recent_global_xmin = xmin;
    SpinLockRelease(&ClusterMonitorCtl->mutex);
}

static void
ClusterMonitorSetReportingGlobalXmin(GlobalTransactionId xmin)
{
    elog(DEBUG2, "ClusterMonitorSetReportingGlobalXmin - old %d, new %d",
            ClusterMonitorCtl->reporting_recent_global_xmin,
            xmin);
    SpinLockAcquire(&ClusterMonitorCtl->mutex);
    ClusterMonitorCtl->reporting_recent_global_xmin = xmin;
    SpinLockRelease(&ClusterMonitorCtl->mutex);
}
#endif
GlobalTransactionId
ClusterMonitorGetReportingGlobalXmin(void)
{
    GlobalTransactionId reporting_xmin;

    SpinLockAcquire(&ClusterMonitorCtl->mutex);
    reporting_xmin = ClusterMonitorCtl->reporting_recent_global_xmin;
    SpinLockRelease(&ClusterMonitorCtl->mutex);

    return reporting_xmin;
}
