/*-------------------------------------------------------------------------
 *
 * main.c
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *      $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#include <time.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/timeb.h>
#include <sys/select.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/sysinfo.h>
#include <sys/epoll.h>
#include <getopt.h>
#include <stdio.h>
#include <dirent.h>
#include <sys/timeb.h>

#include "gtm/gtm_c.h"
#include "gtm/path.h"
#include "gtm/gtm.h"
#include "gtm/elog.h"
#include "gtm/memutils.h"
#include "gtm/gtm_list.h"
#include "gtm/gtm_seq.h"
#include "gtm/standby_utils.h"
#include "gtm/gtm_standby.h"
#include "gtm/gtm_xlog_internal.h"
#include "gtm/gtm_client.h"
#include "gtm/gtm_xlog.h"
#include "gtm/libpq.h"
#include "gtm/libpq-fe.h"
#include "gtm/libpq-be.h"
#include "gtm/pqsignal.h"
#include "gtm/pqformat.h"
#include "gtm/assert.h"
#include "gtm/register.h"
#include "gtm/replication.h"
#include "gtm/gtm_txn.h"
#include "gtm/gtm_seq.h"
#include "gtm/gtm_msg.h"
#include "gtm/gtm_opt.h"
#include "gtm/gtm_utils.h"
#include "gtm/gtm_backup.h"
#include "gtm/gtm_time.h"
#include "gtm/gtm_stat.h"
#include "gtm/gtm_stat_error.h"
#include "gtm/syslogger.h"

#ifdef __TBASE__
#include "gtm/gtm_store.h"
#endif

#ifdef __XLOG__
#include "gtm/gtm_xlog.h"
#endif

extern int    optind;
extern char *optarg;

#define GTM_MAX_PATH            1024
#define GTM_DEFAULT_HOSTNAME    "*"
#define GTM_DEFAULT_PORT        6666
#define GTM_PID_FILE            "gtm.pid"
#define GTM_LOG_FILE            "gtm.log"
#define GTM_LOG_FILE_DIR		"gtm_log"

#define LOOPS_UNTIL_HIBERNATE        50
#define HIBERNATE_FACTOR        25

#define GTM_STARTUP_CONNECT_ACTIVE_TIMEOUT	(2)

static char *progname = "gtm";
char       *ListenAddresses;
int            GTMPortNumber;
char        *GTMDataDir;
char        *NodeName;
int            GTM_Standby_Connetion_Timeout = 30;
bool        Backup_synchronously = false;
char         *active_addr;
int         active_port;
int            tcp_keepalives_idle;
int            tcp_keepalives_interval;
int            tcp_keepalives_count;
char        *error_reporter;
char        *status_reader;
bool        isStartUp;
int            scale_factor_threads = 1;
int            worker_thread_number = 2;
#ifdef __TBASE__
int         wal_writer_delay;
int         checkpoint_interval;
char        *archive_command;
bool        archive_mode;
int         max_reserved_wal_number;
int         max_wal_sender;
char        *synchronous_standby_names;
char        *application_name;
bool        first_init;
char        *recovery_file_name;
char        *recovery_command;
GlobalTimestamp recovery_timestamp;
char        *recovery_target_timestamp;
bool        recovery_pitr_mode;
bool        GTMClusterReadOnly;
char        *GTMStartupGTSSet;
int         GTMGTSFreezeLimit;
int         GTMStartupGTSDelta;
char	    *unix_socket_directory = NULL;

#endif
GTM_MutexLock   control_lock;

#ifdef __TBASE__
bool        enable_gtm_sequence_debug = false;
bool        enalbe_gtm_xlog_debug = false;
bool        enable_gtm_debug   = false;
bool        enable_sync_commit = false;
int         warnning_time_cost = 0;


GTM_TimerEntry *g_timer_entry;
int32              g_used_entry = 0;
GTM_RWLock      g_timer_lock; /* We don't expect too much concurrency, so use only a big lock. */
int                g_max_lock_number = 6000; /* max lock per thread can hold. init value is for main thread. it will be changed after initlize.*/
int             g_max_thread_number = 512; /* max thread number of gtm. */
GTM_ThreadInfo  *g_timekeeper_thread = NULL;
GTM_ThreadInfo    *g_timebackup_thread = NULL;
GTM_ThreadInfo  *g_timer_thread = NULL;
GTM_ThreadInfo  *g_logcollector_thread = NULL;
GTM_ThreadInfo  *g_syslogger_thread = NULL;

void *GTM_ThreadLogCollector(void *argp);
extern void GTM_ErrorLogCollector(ErrorData *edata, StringInfo buff);
GTM_ThreadInfo  *g_standby_pre_server_thread = NULL;

#ifdef __XLOG__
GTM_ThreadInfo  *g_basebackup_thread   = NULL;
GTM_ThreadInfo  *g_xlog_writer_thread   = NULL;
GTM_ThreadInfo  *g_checkpoint_thread    = NULL;
GTM_ThreadInfo  *g_walreceiver_thread   = NULL;
GTM_ThreadInfo  *g_redoer_thread        = NULL;
GTM_ThreadInfo  *g_archiver_thread      = NULL;

GTM_MutexLock    g_checkpointer_blocker;


uint32           *g_checkpointDirtyStart;
uint32           *g_checkpointDirtySize;
char             *g_checkpointMapperBuff;

time_t           g_last_sync_gts;       /* record when last gts last sync to disk */
s_lock_t          g_last_sync_gts_lock;

#endif

static long long getSystemTime();
static void  *GTM_TimerThread(void *argp);
static void   GTM_TimerRun(void);
static int    GTM_TimerInit(void);
#ifndef __XLOG__
static void   CheckStandbyConnect(GTM_ThreadInfo *my_threadinfo, GTM_ConnectionInfo *conn);
#endif

extern size_t g_GTMStoreSize;
#endif
/* If this is GTM or not */
/*
 * Used to determine if given Port is in GTM or in GT_Proxy.
 * If it is in GTM, we should consider to flush GTM_Conn before
 * writing anything to Port.
 */
bool        isGTM = true;

GTM_ThreadID    TopMostThreadID;

enum GTM_PromoteStatus promote_status = GTM_PRPMOTE_INIT;
s_lock_t promote_status_lck;

/* The socket(s) we're listening to. */
#define MAXLISTEN    64
static int    ListenSocket[MAXLISTEN];

pthread_key_t    threadinfo_key;
static bool        GTMAbortPending = false;

static void GTM_SaveVersion(FILE *ctlf);

static Port *ConnCreate(int serverFd);
static int ServerLoop(void);
static int initMasks(fd_set *rmask);
#ifdef __XLOG__
void GTM_PortCleanup(Port *con_port);
#endif
void *GTM_ThreadMain(void *argp);
void *GTM_ThreadTimeKeeper(void *argp);
static void *GTM_ThreadSysLogger(void *argp);
static bool GTM_SysLoggerStart(void);

#ifdef __XLOG__
void *GTM_ThreadCheckPointer(void *argp);
void *GTM_ThreadWalSender(void *argp);
void *GTM_ThreadWalReceiver(void *argp);
void *GTM_ThreadWalRedoer(void *argp);
void *GTM_ThreadArchiver(void *argp);
void *GTM_ThreadBasebackup(void *argp);
#else
void *GTM_ThreadTimeBackup(void *argp);
#endif

void bind_thread_to_cores (cpu_set_t cpuset) ;
void bind_timekeeper_thread(void);
void bind_service_threads(void);

static int GTMAddConnection(Port *port, GTM_Conn *standby);
static int ReadCommand(Port *myport, StringInfo inBuf);

static void ProcessCommand(Port *myport, StringInfo input_message);
static void ProcessBasebackupCommand(Port *myport, StringInfo input_message);
static void ProcessPGXCNodeCommand(Port *myport, GTM_MessageType mtype, StringInfo message);
static void ProcessTransactionCommand(Port *myport, GTM_MessageType mtype, StringInfo message);
static void ProcessSnapshotCommand(Port *myport, GTM_MessageType mtype, StringInfo message);
static void ProcessSequenceCommand(Port *myport, GTM_MessageType mtype, StringInfo message);
static void ProcessQueryCommand(Port *myport, GTM_MessageType mtype, StringInfo message);

static void GTM_RegisterPGXCNode(Port *myport, char *PGXCNodeName);

static bool CreateOptsFile(int argc, char *argv[]);
static void CreateDataDirLockFile(void);
void CreateLockFile(const char *filename, const char *refName);
static void SetDataDir(void);
static void ChangeToDataDir(void);
static void checkDataDir(void);
void DeleteLockFile(const char *filename);
extern void RemoveSocketFile(void);
static void PromoteToActive(void);
#ifndef __XLOG__
static void ProcessSyncStandbyCommand(Port *myport, GTM_MessageType mtype, StringInfo message);
#endif
static void ProcessBarrierCommand(Port *myport, GTM_MessageType mtype, StringInfo message);
static int 
GTMInitConnection(GTM_ConnectionInfo *conninfo);
static void SetNonBlockConnection(GTM_ConnectionInfo *conninfo);
static void gtm_standby_pre_server_loop(char *data_dir);

#ifdef __XLOG__
static void thread_replication_clean(GTM_StandbyReplication *replication);
void SendXLogSyncStatus(GTM_Conn *conn);
static void WaitRedoertoExit(void);
static void GTMSigHupHandler(void);
#endif

void GTM_Exit(void);

/*
 * One-time initialization. It's called immediately after the main process
 * starts
 */
static GTM_ThreadInfo *
MainThreadInit()
{
    GTM_ThreadInfo *thrinfo;

    pthread_key_create(&threadinfo_key, NULL);

    /*
     * Initialize the lock protecting the global threads info and backup lock info.
     */
    GTM_RWLockInit(&GTMThreads->gt_lock);
    GTM_RWLockInit(&gtm_bkup_lock);

    /*
     * Set the next client identifier to be issued after connection
     * establishment
     */
    GTMThreads->gt_starting_client_id = 0;
    GTMThreads->gt_next_client_id = 1;

    GTMThreads->gt_block_new_connection = false;

    /*
     * We are called even before memory context management is setup. We must
     * use malloc
     */
    thrinfo = (GTM_ThreadInfo *)malloc(sizeof (GTM_ThreadInfo));

    if (thrinfo == NULL)
    {
        fprintf(stderr, "malloc failed: %d", errno);
        fflush(stdout);
        fflush(stderr);
        exit(1);
    }

    memset(thrinfo, 0, sizeof(GTM_ThreadInfo));

    thrinfo->is_main_thread = true;
    if (SetMyThreadInfo(thrinfo))
    {
        fprintf(stderr, "SetMyThreadInfo failed: %d", errno);
        fflush(stdout);
        fflush(stderr);
        exit(1);
    }

    
#ifdef __TBASE__
    /* use init value of g_max_lock_number for main thread. */
    if (!g_max_lock_number)
    {
        abort();
    }
    thrinfo->max_lock_number     = g_max_lock_number;
    thrinfo->backup_timer_handle = INVALID_TIMER_HANDLE;
    thrinfo->locks_hold = (GTM_RWLock**)malloc(sizeof(void*) * g_max_lock_number);
#ifdef __XLOG__
    thrinfo->write_locks_hold = (GTM_RWLock**)malloc(sizeof(void*) * g_max_lock_number);
    thrinfo->current_write_number = 0;
    thrinfo->xlog_inserting = false;

    thrinfo->xlog_waiter.pos      = InvalidXLogRecPtr;
    thrinfo->xlog_waiter.finished = false;
    GTM_MutexLockInit(&thrinfo->xlog_waiter.lock);
    GTM_CVInit(&thrinfo->xlog_waiter.cv);
#endif
    if (NULL == thrinfo->locks_hold)
    {
        fprintf(stderr, "out of memory when init main thread");
        fflush(stdout);
        fflush(stderr);
        exit(1);
    }
    memset(thrinfo->locks_hold, 0x00, sizeof(void*) * g_max_lock_number);
#endif

	/* thread main is syslogger before syslogger thread create */
    thrinfo->am_syslogger = true;
    GTM_RWLockInit(&thrinfo->thr_lock);
    GTM_RWLockAcquire(&thrinfo->thr_lock, GTM_LOCKMODE_WRITE);    

    TopMostThreadID = pthread_self();

    return thrinfo;
}

/*
 * Bare minimum supporting infrastructure. Must be called at the very beginning
 * so that further initilization can have it ready
 */
static void
InitGTMProcess()
{
    GTM_ThreadInfo *thrinfo PG_USED_FOR_ASSERTS_ONLY = MainThreadInit();
    MyThreadID = pthread_self();
    MemoryContextInit();

    /*
     * The memory context is now set up.
     * Add the thrinfo structure in the global array
     */
    GTM_MutexLockInit(&control_lock);
}

static void
BaseInit(char *data_dir)
{
    /* Initialize standby lock before doing anything else */
    Recovery_InitStandbyLock();
    
    checkDataDir();
    SetDataDir();
    ChangeToDataDir();
    CreateDataDirLockFile();
    
#ifdef __TBASE__    
    if(GTM_ControlDataInit() != GTM_STORE_OK)
        exit(1);    
#endif

#ifdef __XLOG__
    GTM_StoreSizeInit();
    GTM_StandbyBaseinit();
    GTM_XLogCtlDataInit();

    SpinLockInit(&g_last_sync_gts_lock);
#endif

    if (Log_directory == NULL)
    {
        Log_directory = (char *) malloc(GTM_MAX_PATH);
        sprintf(Log_directory, "%s/%s", GTMDataDir, GTM_LOG_FILE_DIR);
    }

    if (GTMLogFile == NULL)
    {
        GTMLogFile = (char *) malloc(GTM_MAX_PATH);
        sprintf(GTMLogFile, "%s/%s", Log_directory, GTM_LOG_FILE);
    }

    /* Save Node Register File in register.c */
    Recovery_SaveRegisterFileName(GTMDataDir);

	GTM_LogFileInit();

    GTM_InitTxnManager();
    GTM_InitSeqManager();
    GTM_InitNodeManager();
}

/*
 * SIGHUP: set flag to re-read config file at next convenient time.
 *
 * Sets the ConfigReloadPending flag, which should be checked at convenient
 * places
 */
void
GTMSigHupHandler(void)
{
       int                     save_errno = errno;

       ConfigReloadPending = true;

       errno = save_errno;
}

static void
GTM_SigleHandler(int signal)
{// #lizard forgives
    fprintf(stderr, "Received signal %d\n", signal);

    switch (signal)
    {
        case SIGKILL:
        case SIGTERM:
        case SIGQUIT:
        case SIGINT:
            break;
		
		case SIGHUP:
			GTMSigHupHandler();
			return ;

        case SIGUSR1:
            if (Recovery_IsStandby())
                PromoteToActive();
            return;

        default:
            fprintf(stderr, "Unknown signal %d\n", signal);
            return;
    }

    /*
     * XXX We should do a clean shutdown here.
     */

    /* Rewrite Register Information (clean up unregister records) */
    Recovery_SaveRegisterInfo();

#ifndef __XLOG__
    /* Delete pid file before shutting down */
    DeleteLockFile(GTM_PID_FILE);
#endif

    PG_SETMASK(&BlockSig);
    GTMAbortPending = true;

    return;
}

static void
GTM_ThreadSigHandler(int signal)
{
    switch (signal)
    {    
        case SIGQUIT:
            pthread_exit(NULL);        

        default:
            fprintf(stderr, "Unknown signal %d\n", signal);
            return;
    }
    return;
}


/*
 * Help display should match
 */
static void
help(const char *progname)
{
    printf(_("This is the GTM server.\n\n"));
    printf(_("Usage:\n  %s [OPTION]...\n\n"), progname);
    printf(_("Options:\n"));
    printf(_("  -h hostname     GTM server hostname/IP to listen.\n"));
    printf(_("  -p port         GTM server port number to listen.\n"));
    printf(_("  -n nodename     Node name for GTM server.\n"));
    printf(_("  -x xid          Starting GXID \n"));
    printf(_("  -D directory    GTM working directory\n"));
    printf(_("  -l filename     GTM server log file name \n"));
    printf(_("  -c              Show server status, then exit\n"));
    printf(_("  -f              Force start GTM with starting XID specified by -x option\n"));
    printf(_("  -g              Force start GTM with starting GTS specified by -g option\n"));
    printf(_("  -r              Nodes connected with GTM will be readonly\n"));
    printf(_("  -T              Refuse to start GTM when GTS has no more than -T days left,100 years by default\n"));
    printf(_("  -d              Add -d seconds to GTS when started\n"));
    printf(_("  --help          Show this help, then exit\n"));
    printf(_("\n"));
    printf(_("Options for Standby mode:\n"));
    printf(_("  -s              Start as a GTM standby server.\n"));
    printf(_("  -i hostname     Active GTM server hostname/IP to connect.\n"));
    printf(_("  -q port         Active GTM server port number to connect.\n"));
    printf(_("\n"));
}

static void
gtm_status()
{
    fprintf(stderr, "gtm_status(): must be implemented to scan the shmem.\n");
    exit(0);
}

#ifndef __TBASE__
/*
 * Save control file info
 */
void
SaveControlInfo(void)
{
    FILE       *ctlf;

    GTM_MutexLockAcquire(&control_lock);

    ctlf = fopen(GTMControlFileTmp, "w");

    if (ctlf == NULL)
    {
        fprintf(stderr, "Failed to create/open the control file\n");
        GTM_MutexLockRelease(&control_lock);
        return;
    }

    GTM_SaveVersion(ctlf);
    GTM_SaveTxnInfo(ctlf);
    GTM_SaveSeqInfo(ctlf);
    fclose(ctlf);

    remove(GTMControlFile);
    rename(GTMControlFileTmp, GTMControlFile);

    GTM_MutexLockRelease(&control_lock);
}

#endif

#ifdef __TBASE__
static int CheckTscFeatures(char *cmd);

static bool CheckClockSource(void);

static int CheckTscFeatures(char *cmd)
{
    FILE *file = popen(cmd, "r");
    int count;
    
    if (file == NULL)
        return false;

	if (fscanf(file, "%d", &count) == EOF)
    {
	    count = 0;
    }
    pclose(file);
    
    return count;
}

static bool CheckClockSource(void)
{
    int count1, count2, count3;

    count1 = CheckTscFeatures("grep 'constant_tsc' /proc/cpuinfo |wc -l");
    count2 = CheckTscFeatures("grep 'nonstop_tsc' /proc/cpuinfo |wc -l");
    count3 = CheckTscFeatures("grep 'processor' /proc/cpuinfo |wc -l");

    if(0 == count1)
    {
        elog(LOG, "Processors do not support constant tsc which may cause undefined behavior of distributed transactions");
        return false;
    }
    
    if(0 == count2)
    {
        elog(LOG, "Processors do not support nonstop tsc which may cause undefined behavior of distributed transactions");
        return false;
    }

    if(count1 != count3 || count2 != count3)
    {
        elog(LOG, "Not all processors support constant and nonstop tsc which may cause undefined behavior of distributed transactions");
        return false;
    }

    count1 = CheckTscFeatures("grep 'tsc' /sys/devices/system/clocksource/clocksource0/current_clocksource |wc -l");
    if(count1 != 1)
    {
        elog(LOG, "OS does not choose TSC as clocksource which may cause undefined behavier of distributed transactions");
        return false;
    }
    
    return true;
}

#endif

static void GTM_XLogRecoveryIfNeed(char *data_dir)
{
    Assert(ControlData != NULL);

    switch(ControlData->state)
    {
        case DB_SHUTDOWNED_IN_RECOVERY:
        case DB_SHUTDOWNING:
        case DB_STARTUP:
        case DB_IN_CRASH_RECOVERY:
        case DB_IN_ARCHIVE_RECOVERY:
        case DB_IN_PRODUCTION:
            elog(LOG, "Detect GTM server crash.");
            GTM_XLogRecovery(ControlData->checkPoint,data_dir);
            break;
        case DB_SHUTDOWNED:
            break;
    }
}

int
main(int argc, char *argv[])
{// #lizard forgives
    int            opt;
    int            status;
    int            i;
    GlobalTransactionId next_gxid = InvalidGlobalTransactionId;
#ifndef __TBASE__
    FILE       *ctlf = NULL;    
#else
    int            ret;
#endif
    bool        force_xid = false;

	int			process_thread_num = 0;
    bool        do_basebackup = false;
    /*
     * Local variable to hold command line options.
     *
     * if -c option is specified, then only -D option will be taken to locate
     * GTM data directory.   All the other options are ignored.   GTM status
     * will be printed out based on the specified data directory and GTM will
     * simply exit.   If -D option is not specified in this case, current directory
     * will be used.
     *
     * In other case, all the command line options are analyzed.
     *
     * They're first analyzed and then -D option are used to locate configuration file.
     * If -D option is not specified, then the default value will be used.
     *
     * Please note that configuration specified in the configuration file (gtm.conf)
     * will be analyzed first and then will be overridden by the value specified
     * in command line options.   -D and -C options are handled separately and used
     * to determine configuration file location.
     *
     * Please also note that -x option (startup GXID) will be handled in this section.
     * It has no corresponding configuration from the configuration file.
     */

    bool     is_gtm_status = false;        /* "false" means no -c option was found */
    char     *listen_addresses = NULL;
    char    *node_name = NULL;
    char    *port_number = NULL;
    char    *data_dir = NULL;
    char    *log_file = NULL;
    char    *is_standby_mode = NULL;
    char    *dest_addr = NULL;
    char    *dest_port = NULL;
    char    *gtm_freeze_time_limit = NULL;
    char    *gtm_startup_gts_delta = NULL;
    char    *gtm_startup_gts_set   = NULL;
    char    *gtm_cluster_read_only = NULL;
    int     util_thread_cnt = 0;

    isStartUp = true;
    SpinLockInit(&promote_status_lck);
    promote_status = GTM_PRPMOTE_INIT;

    /*
     * At first, initialize options.  Also moved something from BaseInit() here.
     */
    InitializeGTMOptions();

    /* 
     * Also initialize bare minimum supporting infrastructure such as memory
     * context and thread control structure
     */
    InitGTMProcess();

    /*
     * Catch standard options before doing much else
     */
    if (argc > 1)
    {
        if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
        {
            help(argv[0]);
            exit(0);
        }
    }

    ListenAddresses = strdup(GTM_DEFAULT_HOSTNAME);
    GTMPortNumber = GTM_DEFAULT_PORT;

    /*
     * Parse the command like options and set variables
     */
    while ((opt = getopt(argc, argv, "ch:n:p:x:D:l:si:q:f:bT:g:r")) != -1)
    {
        switch (opt)
        {
            case 'c':
                gtm_status();
                break; /* never reach here. */

            case 'h':
                if (listen_addresses)
                    free(listen_addresses);
                listen_addresses = strdup(optarg);
                break;

            case 'n':
                if (NodeName)
                    free(NodeName);
                NodeName = strdup(optarg);
                break;

            case 'p':
                if (port_number)
                    free(port_number);
                port_number = strdup(optarg);
                break;

            case 'x':
                next_gxid = (GlobalTransactionId)atoll(optarg);
                break;

            case 'D':
                if (data_dir)
                    free(data_dir);
                data_dir = strdup(optarg);
                canonicalize_path(data_dir);
                break;

            case 'l':
                if (log_file)
                    free(log_file);
                log_file = strdup(optarg);
                break;

            case 's':
                if (is_standby_mode)
                    free(is_standby_mode);
                is_standby_mode = strdup("standby");
                break;

            case 'i':
                if (dest_addr)
                    free(dest_addr);
                dest_addr = strdup(optarg);
                break;

            case 'q':
                if (dest_port)
                    free(dest_port);
                dest_port = strdup(optarg);
                break;
            case 'f':
                force_xid = true;
                break;
            case 'b':
                do_basebackup = true;
                break;
            case 'r':
                if (gtm_cluster_read_only)
                    free(gtm_cluster_read_only);
                gtm_cluster_read_only = strdup(optarg);
                break;
            case 'T':
                if (gtm_freeze_time_limit)
                    free(gtm_freeze_time_limit);
                gtm_freeze_time_limit = strdup(optarg);
                break;
            case 'd':
                if (gtm_startup_gts_delta)
                    free(gtm_startup_gts_delta);
                gtm_startup_gts_delta = strdup(optarg);
                break;
            case 'g':
                if (gtm_startup_gts_set)
                    free(gtm_startup_gts_set);
                gtm_startup_gts_set = strdup(optarg);
                break;

            default:
                write_stderr("Try \"%s --help\" for more information.\n",
                             progname);
        }
    }

    /*
     * Handle status, if any
     */
    if (is_gtm_status)
        gtm_status();
    /* Never reach beyond here */

    /*
     * Setup work directory
     */
    if (data_dir)
        SetConfigOption(GTM_OPTNAME_DATA_DIR, data_dir, GTMC_STARTUP, GTMC_S_OVERRIDE);

    /*
     * Setup configuration file
     */
    if (!SelectConfigFiles(data_dir, progname))
        exit(1);

    /*
     * Parse config file
     */
    if(ProcessConfigFile(GTMC_STARTUP) == false)
    {
        elog(LOG,"configure file gtm.conf parse fails");
        exit(2);
    }
    /*
     * Override with command line options
     */
    if (log_file)
    {
        SetConfigOption(GTM_OPTNAME_LOG_FILE, log_file, GTMC_STARTUP, GTMC_S_OVERRIDE);
        free(log_file);
        log_file = NULL;
    }
    if (listen_addresses)
    {
        SetConfigOption(GTM_OPTNAME_LISTEN_ADDRESSES,
                        listen_addresses, GTMC_STARTUP, GTMC_S_OVERRIDE);
        free(listen_addresses);
        listen_addresses = NULL;
    }
    if (node_name)
    {
        SetConfigOption(GTM_OPTNAME_NODENAME, node_name, GTMC_STARTUP, GTMC_S_OVERRIDE);
        free(node_name);
        node_name = NULL;
    }
    if (port_number)
    {
        SetConfigOption(GTM_OPTNAME_PORT, port_number, GTMC_STARTUP, GTMC_S_OVERRIDE);
        free(port_number);
        port_number = NULL;
    }
    if (is_standby_mode)
    {
        SetConfigOption(GTM_OPTNAME_STARTUP, is_standby_mode, GTMC_STARTUP, GTMC_S_OVERRIDE);
        free(is_standby_mode);
        is_standby_mode = NULL;
    }
    if (dest_addr)
    {
        SetConfigOption(GTM_OPTNAME_ACTIVE_HOST, dest_addr, GTMC_STARTUP, GTMC_S_OVERRIDE);
        free(dest_addr);
        dest_addr = NULL;
    }
    if (dest_port)
    {
        SetConfigOption(GTM_OPTNAME_ACTIVE_PORT, dest_port, GTMC_STARTUP, GTMC_S_OVERRIDE);
        free(dest_port);
        dest_port = NULL;
    }
    if (gtm_freeze_time_limit)
    {
        SetConfigOption(GTM_OPTNAME_GTS_FREEZE_TIME_LIMIT, gtm_freeze_time_limit, GTMC_STARTUP, GTMC_S_OVERRIDE);
        free(gtm_freeze_time_limit);
        dest_port = NULL;
    }
    if (gtm_startup_gts_delta)
    {
        SetConfigOption(GTM_OPTNAME_STARTUP_GTS_DELTA, gtm_startup_gts_delta, GTMC_STARTUP, GTMC_S_OVERRIDE);
        free(gtm_startup_gts_delta);
        dest_port = NULL;
    }
    if (gtm_startup_gts_set)
    {
        SetConfigOption(GTM_OPTNAME_STARTUP_GTS_SET, gtm_startup_gts_set, GTMC_STARTUP, GTMC_S_OVERRIDE);
        free(gtm_startup_gts_set);
        dest_port = NULL;
    }
    if (gtm_cluster_read_only)
    {
        SetConfigOption(GTM_OPTNAME_CLUSTER_READ_ONLY, gtm_cluster_read_only, GTMC_STARTUP, GTMC_S_OVERRIDE);
        free(gtm_cluster_read_only);
        dest_port = NULL;
    }

    if (GTMDataDir == NULL)
    {
        write_stderr("GTM data directory must be specified\n");
        write_stderr("Try \"%s --help\" for more information.\n",
                     progname);
        exit(1);
    }

    if (NodeName == NULL || NodeName[0] == 0)
    {
        write_stderr("Nodename must be specified\n");
        write_stderr("Try \"%s --help\" for more information.\n",
                     progname);
        exit(1);
    }

    if (ListenAddresses == NULL || ListenAddresses[0] == 0)
    {
        write_stderr("Listen_addresses must be specified\n");
        write_stderr("Try \"%s --help\" for more information.\n",
                     progname);
        exit(1);
    }

    if(enable_sync_commit == true  && synchronous_standby_names == NULL)
    {
        write_stderr("You have to provide synchronous_standby_names to enable sync_commit");
        write_stderr("Try \"%s --help\" for more information.\n",
                     progname);
        exit(1);
    }

	if( (synchronous_standby_names != NULL && strlen(synchronous_standby_names) > 0) && enable_sync_commit == false)
    {
        write_stderr("synchronous_standby_names is not allow to set in async commit mode");
        write_stderr("Try \"%s --help\" for more information.\n",
                     progname);
    }

    /*
     * GTM accepts no non-option switch arguments.
     */
    if (optind < argc)
    {
        write_stderr("%s: invalid argument: \"%s\"\n",
                     progname, argv[optind]);
        write_stderr("Try \"%s --help\" for more information.\n",
                     progname);
        exit(1);
    }

    /*
     * Establish input sockets.
     */
    for (i = 0; i < MAXLISTEN; i++)
        ListenSocket[i] = -1;

    if (ListenAddresses)
    {
        if (strcmp(ListenAddresses, "*") == 0)
            status = StreamServerPort(AF_UNSPEC, NULL,
                                      (unsigned short) GTMPortNumber,
                                      NULL,
                                      ListenSocket, MAXLISTEN);
        else
            status = StreamServerPort(AF_UNSPEC, ListenAddresses,
                                      (unsigned short) GTMPortNumber,
                                      NULL,
                                      ListenSocket, MAXLISTEN);

        if (status != STATUS_OK)
            ereport(FATAL,
                    (errmsg("could not create listen socket for \"%s\"",
                            ListenAddresses)));
    }

#ifdef __TBASE__
#ifdef HAVE_UNIX_SOCKETS
    if (unix_socket_directory)
	{
        status = StreamServerPort(AF_UNIX, NULL,
                                  (unsigned short) GTMPortNumber,
                                  unix_socket_directory,
                                  ListenSocket, MAXLISTEN);

        if (status != STATUS_OK)
        {
            ereport(FATAL,
                    (errmsg("could not create Unix-domain socket in directory \"%s\"",
                            unix_socket_directory)));
        }
	}
#endif
#endif

    /*
     * check that we have some socket to listen on
     */
    if (ListenSocket[0] == -1)
        ereport(FATAL,
                (errmsg("no socket created for listening")));

    /*
     * Some basic initialization must happen before we do anything
     * useful
     */
    BaseInit(data_dir);

    /*
     * Check options for the standby mode. Do it after StandbyLock has been
     * initialised in BaseInit()
     */
    if (Recovery_IsStandby())
    {
        if (active_addr == NULL || active_port < 1)
        {
            help(argv[0]);
            exit(1);
        }
        Recovery_StandbySetConnInfo(active_addr, active_port);
    }

#ifdef __XLOG__

    if(access(RECOVERY_CONF_NAME,F_OK) == 0)
    {
        recovery_pitr_mode = true;

        ValidXLogRecoveryCondition();
        GTM_XLogRecovery(ControlData->checkPoint,data_dir);
        elog(LOG,"recovery finish,please start gtm");

        ControlData->state = DB_SHUTDOWNED;
        ControlDataSync(false);

        rename(RECOVERY_CONF_NAME,RECOVERY_CONF_NAME_DONE);
        exit(1);
    }
    else
    {
        recovery_pitr_mode = false;
        recovery_timestamp = InvalidGTS;
    }

    if(Recovery_IsStandby() == false)
    {
        GTM_XLogRecoveryIfNeed(data_dir);
	}

#endif

#ifdef __TBASE__
    elog(LOG, "Starting GTM server at (%s:%d) with syn storage", ListenAddresses, GTMPortNumber);
#else
    elog(LOG, "Starting GTM server at (%s:%d) -- control file %s", ListenAddresses, GTMPortNumber, GTMControlFile);
#endif

    g_max_lock_number = 6000;
    pqsignal(SIGHUP, GTM_SigleHandler);
    pqsignal(SIGKILL, GTM_SigleHandler);
    pqsignal(SIGQUIT, GTM_SigleHandler);
    pqsignal(SIGTERM, GTM_SigleHandler);
    pqsignal(SIGINT, GTM_SigleHandler);
    pqsignal(SIGUSR1, GTM_SigleHandler);
    pqsignal(SIGPIPE, SIG_IGN);

    pqinitmask();

    /*
     * Establish a connection between the active and standby.
     */
    if (Recovery_IsStandby())
    {
        if (!gtm_standby_start_startup(GTM_STARTUP_CONNECT_ACTIVE_TIMEOUT))
        {
#ifdef __TBASE__
            elog(LOG, "Failed to establish a connection to active-GTM.");

            /*
             * if failed to establish a connection to active-GTM, just
             * retry, but support the check status command.
             */
            gtm_standby_pre_server_loop(data_dir);
#else
            elog(ERROR, "Failed to establish a connection to active-GTM.");
#endif
        }
        else
        {
            elog(LOG, "Standby GTM Startup connection established with active-GTM.");
        }
    }

    SpinLockAcquire(&promote_status_lck);
    if (promote_status == GTM_PRPMOTE_INIT)
    {
        promote_status = GTM_PRPMOTE_CONNED;
    }
    SpinLockRelease(&promote_status_lck);

#ifdef __XLOG__
    GTM_XLogFileInit(data_dir);

    GTM_RWLockAcquire(&ControlDataLock,GTM_LOCKMODE_WRITE);
    
    ControlData->state = DB_IN_PRODUCTION;
    ControlDataSync(false);
    
    GTM_RWLockRelease(&ControlDataLock);

#endif
    
    /*
     * Read the last GXID and start from there
     */
    if (Recovery_IsStandby())
    {
#ifdef __TBASE__
        bool  bret = false;
        int32 ret;
        int64 identifier = 0;
        int64 lsn = 0;
        GlobalTimestamp gts = 0;
        int  max_retry_times = 10;
        
        system("rm -rf gtm_xlog/*");

        bret = GTM_StoreGetSysInfo(&identifier, &lsn, &gts);
        if (!bret)
        {
            elog(LOG, "GTM get storage identifier and lsn failed.");
            identifier = 0;
            lsn = 0;
        }

        if (!gtm_standby_begin_backup(identifier, lsn, gts))
        {
            elog(LOG, "Failed to set begin_backup status to the active-GTM.");
            exit(1);
        }

        do
        {
            ret = GTM_StoreStandbyInitFromMaster(GTMDataDir);
            if (ret)
            {
                elog(LOG, "GTM_StoreMasterInit failed for %s.retry", strerror(errno));
                max_retry_times--;
                sleep(3);
            }
            else 
            {
                break;
            }
        } while(max_retry_times > 0);

		if(ret)
		{
    		elog(FATAL, "GTM_StoreMasterInit failed too many times \"%s\", exit", strerror(errno));
		}
		
		if (!gtm_standby_restore_next_gxid())
		{
			elog(FATAL, "Failed to restore next/last gxid from the active-GTM.");
		}
		elog(LOG, "Restoring next/last gxid from the active-GTM succeeded.");

		if (!gtm_standby_restore_gxid())
		{
			elog(FATAL, "Failed to restore all of gxid(s) from the active-GTM.");
		}
		elog(LOG, "Restoring all of gxid(s) from the active-GTM succeeded.");

		if (!gtm_standby_restore_sequence())
		{
			elog(FATAL, "Failed to restore sequences from the active-GTM.");
		}
		elog(LOG, "Restoring sequences from the active-GTM succeeded.");


#else
        if (!gtm_standby_begin_backup(0, 0, 0))
        {
            elog(ERROR, "Failed to set begin_backup satatus to the active-GTM.");
            exit(1);
        }
        if (!gtm_standby_restore_next_gxid())
        {
            elog(ERROR, "Failed to restore next/last gxid from the active-GTM.");
            exit(1);
        }
        elog(LOG, "Restoring next/last gxid from the active-GTM succeeded.");

        if (!gtm_standby_restore_gxid())
        {
            elog(ERROR, "Failed to restore all of gxid(s) from the active-GTM.");
            exit(1);
        }
        elog(LOG, "Restoring all of gxid(s) from the active-GTM succeeded.");

        if (!gtm_standby_restore_sequence())
        {
            elog(ERROR, "Failed to restore sequences from the active-GTM.");
            exit(1);
        }
        elog(LOG, "Restoring sequences from the active-GTM succeeded.");

        
    
#endif
    }    
#ifdef __TBASE__
    else
    {    
        int32 ret = 0;
        ret = GTM_StoreMasterInit(GTMDataDir);
        if (ret)
        {
            elog(ERROR, "GTM_StoreMasterInit failed for %s.", strerror(errno));
            exit(1);
        }
        GTM_RestoreStoreInfo(next_gxid, force_xid);
    }
#else
    else
    {
        GTM_RestoreContext restoreContext;

        GTM_MutexLockAcquire(&control_lock);

        ctlf = fopen(GTMControlFile, "r");

        /*
         * When the GTMControlFile file is updated, we first write the updated
         * contents to a GTMControlFileTmp, delete the original GTMControlFile
         * and then rename the GTMControlFileTmp file to GTMControlFile
         *
         * In a rare situation, the GTMControlFile may get deleted, but the
         * GTMControlFileTmp may not get renamed. If we don't find the
         * GTMControlFile file, then look for the GTMControlFileTmp file. If
         * none exists, then its an error condition and we must not start.
         */
        if (ctlf == NULL)
        {
            switch (errno)
            {
                case ENOENT:
                    elog(WARNING, "%s not found, now looking for %s",
                            GTMControlFile, GTMControlFileTmp);
                    break;
                default:
                    elog(ERROR, "Could not open %s, errno %d - aborting GTM start",
                            GTMControlFile, errno);
            }
            ctlf = fopen(GTMControlFileTmp, "r");
            if (ctlf == NULL)
                elog(ERROR, "Could not open %s, errno %d - aborting GTM start",
                        GTMControlFileTmp, errno);

            /*
             * Ok, so the GTMControlFileTmp exists. Just rename it to the
             * GTMControlFile and open again with the new name
             */
            elog(WARNING, "Renaming %s to %s", GTMControlFileTmp,
                    GTMControlFile);
            fclose(ctlf);
            rename(GTMControlFileTmp, GTMControlFile);
            ctlf = fopen(GTMControlFile, "r");
            if (ctlf == NULL)
                elog(ERROR, "Could not open %s, errno %d - aborting GTM start",
                        GTMControlFile, errno);
        }

        GTM_RestoreStart(ctlf, &restoreContext);
        GTM_RestoreTxnInfo(ctlf, next_gxid, &restoreContext, force_xid);
        GTM_RestoreSeqInfo(ctlf, &restoreContext);
        if (ctlf)
            fclose(ctlf);

        GTM_MutexLockRelease(&control_lock);
    }
#endif

#ifndef __TBASE__
    /* Backup the restore point */
    GTM_SetNeedBackup();
    GTM_WriteRestorePoint();
#endif

    if (Recovery_IsStandby())
    {
        elog(DEBUG8, "register node name %s port %d", NodeName, GTMPortNumber);
        if (!gtm_standby_register_self(NodeName, GTMPortNumber, GTMDataDir))
        {
            elog(ERROR, "Failed to register myself on the active-GTM as a GTM node.");
            exit(1);
        }
        elog(LOG, "Registering myself to the active-GTM as a GTM node succeeded.");
    }

    /* Recover Data of Registered nodes. */
    if (Recovery_IsStandby())
    {
        if (!gtm_standby_restore_node())
        {
            elog(ERROR, "Failed to restore node information from the active-GTM.");
            exit(1);
        }
        elog(LOG, "Restoring node information from the active-GTM succeeded.");
    }
    else
        elog(LOG, "Started to run as GTM-Active.");

    /*
     * Record gtm options.  We delay this till now to avoid recording
     * bogus options
     */
    if (!CreateOptsFile(argc, argv))
        exit(1);

    SpinLockAcquire(&promote_status_lck);
    /*
     * GTM_PRPMOTE_IN_STARTUP is setting in PromoteToActive,
     * do CurrentTimeLineID++ here.
     */
    if (promote_status == GTM_PRPMOTE_IN_STARTUP)
    {
        SetCurrentTimeLineID(GetCurrentTimeLineID() + 1);
    }

    /*
     * set promote_status to GTM_PRPMOTE_NORMAL finally
     */
    promote_status = GTM_PRPMOTE_NORMAL;
    SpinLockRelease(&promote_status_lck);

    /*
     * Now, activating a standby GTM...
     */
    if (Recovery_IsStandby())
    {
        /*
         * Before ending the backup, inform the GTM master that we are now
         * ready to accept connections and mark ourselves as CONNECTED. All GTM
         * threads are still blocked at this point and when they are unlocked,
         * we will be ready to accept new connections
         */
        if (!gtm_standby_activate_self())
        {
            elog(ERROR, "Failed to update the standby-GTM status as \"CONNECTED\".");
            exit(1);
        }
        elog(LOG, "Updating the standby-GTM status as \"CONNECTED\" succeeded.");

        /*
         * GTM master can now start serving incoming requests. Before it serves
         * any request, it will open a connection with us and start copying all
         * those messages. So we are guaranteed to see each operation, either
         * in the backup we took or as GTM master copies those messages
         */
        if (!gtm_standby_end_backup())
        {
            elog(ERROR, "Failed to setup normal standby mode to the active-GTM.");
            exit(1);
        }
        elog(LOG, "Started to run as GTM-Standby.");

        if (!gtm_standby_finish_startup())
        {
            elog(ERROR, "Failed to close the initial connection to the active-GTM.");
            exit(1);
        }

        elog(DEBUG1, "Startup connection with the active-GTM closed.");
    }

#ifdef __XLOG__
    if(Recovery_IsStandby() && do_basebackup)
    {
        elog(LOG,"Do basebackup success");

        ControlData->state = DB_SHUTDOWNED;
        ControlDataSync(false);
        exit(0);
    }

    if(!Recovery_IsStandby() && ControlData->checkPoint == FIRST_XLOG_REC)
    {
        DoCheckPoint(false);
    }
#endif

#ifdef __TBASE__
    if(false == CheckClockSource())
    {
        elog(LOG, "constant and nonstop TSC is needed.\n");
    }
    else
    {
        elog(LOG, "constant and nonstop TSC is available.\n");
    }
#endif

    bind_service_threads();
#ifdef __TBASE__

    if(GTMStartupGTSSet != NULL)
    {
    
        GTM_Timestamp gts = atoll(GTMStartupGTSSet);
        GTM_Timestamp current_gts = GetNextGlobalTimestamp();
        if (gts == 0)
        {
            elog(ERROR,"%s is a invalid gts",GTMStartupGTSSet);
            exit(1);
        }

        if(current_gts > gts)
        {
            elog(ERROR,"Can't set gts to %s which is lower than current gts "INT64_FORMAT"",GTMStartupGTSSet,current_gts);
            exit(1);
        }
        
        SetNextGlobalTimestamp(gts);
    }

    if(GTMGTSFreezeLimit * GTM_GTS_ONE_SECOND * 24 * 3600 + GTMTransactions.gt_global_timestamp < 0 )
    {
        elog(LOG,"Global timestamp has less then %d days left,refuse to start.",GTMGTSFreezeLimit);
        exit(1);
    }

    /* calculate worker thread number. */
    if (scale_factor_threads > 0)
    {
        process_thread_num = (get_nprocs() - 1) * scale_factor_threads;
        
    }
    else if (worker_thread_number > 0)
    {
        process_thread_num = worker_thread_number;
    }

    if(0 == process_thread_num)
    {
        process_thread_num = 2;
    }
    else
    {
        process_thread_num = g_max_thread_number < process_thread_num ? g_max_thread_number : process_thread_num;
    }

	/* start syslogger thread to handle log */
    if (!GTM_SysLoggerStart())
    {
        elog(ERROR, "Failed to create syslogger thread.");
        exit(1);
    }
    util_thread_cnt++;

    /* Create GTM threads handling requests */
    g_timekeeper_thread = GTM_ThreadCreate(GTM_ThreadTimeKeeper, g_max_lock_number);
    if (NULL == g_timekeeper_thread)
    {
        elog(ERROR, "Failed to create timekeeper thread.");
        exit(1);
    }
    util_thread_cnt++;

#ifdef __XLOG__
    g_basebackup_thread = GTM_ThreadCreate(GTM_ThreadBasebackup, -1);
    if (NULL == g_basebackup_thread)
    {
        elog(ERROR, "Failed to create basebackup thread");
        exit(1);
    }
    util_thread_cnt++;
#endif

#ifndef __XLOG__
    g_timebackup_thread = GTM_ThreadCreate(GTM_ThreadTimeBackup, g_max_lock_number);
    if (NULL == g_timebackup_thread)
    {
        elog(ERROR, "Failed to create timebackup thread.");
        exit(1);
    }
    util_thread_cnt++;
#endif

    ret = GTM_TimerInit();
    if (ret)
    {
        elog(ERROR, "Failed to init timer.");
    }

    g_timer_thread = GTM_ThreadCreate(GTM_TimerThread, g_max_lock_number);
    if (NULL == g_timer_thread)
    {
        elog(ERROR, "Failed to create gtm timer thread.");
        exit(1);
    }
    util_thread_cnt++;

    g_archiver_thread = GTM_ThreadCreate(GTM_ThreadArchiver, g_max_lock_number);
    if (NULL == g_archiver_thread)
    {
        elog(ERROR, "Failed to create archiver thread");
        exit(1);
    }
    util_thread_cnt++;

    GTM_MutexLockInit(&g_checkpointer_blocker)  ;
    if(Recovery_IsStandby())
    {
        GTM_MutexLockAcquire(&g_checkpointer_blocker);
    }
    
    g_checkpoint_thread  = GTM_ThreadCreate(GTM_ThreadCheckPointer,g_max_lock_number);
    if (NULL == g_checkpoint_thread)
    {
        elog(ERROR, "Failed to create gtm checkpointer thread.");
        exit(1);
    }
    util_thread_cnt++;
    
    g_walreceiver_thread = NULL;
    if(Recovery_IsStandby())
    {
        g_walreceiver_thread  = GTM_ThreadCreate(GTM_ThreadWalReceiver,g_max_lock_number);
        if (NULL == g_walreceiver_thread)
        {
            elog(ERROR, "Failed to create gtm walreceiver thread.");
            exit(1);
        }
        util_thread_cnt++;

        g_redoer_thread  = GTM_ThreadCreate(GTM_ThreadWalRedoer,g_max_lock_number);
        if (NULL == g_redoer_thread)
        {
            elog(ERROR, "Failed to create gtm walreceiver thread.");
            exit(1);
        }
        util_thread_cnt++;
    }
    
    /*
     * maybe one GTM_ThreadMain create as g_standby_pre_server_thread before
     */
    i = (g_standby_pre_server_thread == NULL) ? 0 : 1;
	for(; i < process_thread_num; i++)
    {
        elog(DEBUG8, "Create thread %d.\n", i);
        if (NULL == GTM_ThreadCreate(GTM_ThreadMain, g_max_lock_number))
        {
            elog(ERROR, "Failed to create gtm thread.");
            exit(1);
        }
    }

    for(i = 0; i < max_wal_sender; i++)
	{
		{
    		GTM_ThreadInfo *thr = GTM_ThreadCreate(GTM_ThreadWalSender, g_max_lock_number);
    		if (NULL == thr)
    		{
    			elog(ERROR, "Failed to create wal sender thread.");
    			exit(1);
    		}
		}
	}

    g_logcollector_thread = GTM_ThreadCreate(GTM_ThreadLogCollector, g_max_lock_number);
    if (NULL == g_logcollector_thread)
    {
        elog(ERROR, "Failed to create gtm log collector thread.");
        exit(1);
    }
    util_thread_cnt++;

	fprintf(stdout, "TBase create %d worker thread.\n", process_thread_num); 
	
	/* Processing threads + Timer + Timekeeper + Timebackup threads + Walwrite + CheckPointer*/
	GTMThreads->gt_start_thread_count = process_thread_num + max_wal_sender + util_thread_cnt;
	fprintf(stdout, "Start sever loop start thread count %d running thread count %d.\n",
		GTMThreads->gt_start_thread_count, GTMThreads->gt_thread_count); 
	
	elog(LOG, "Start sever loop start thread count %d running thread count %d.\n",
			GTMThreads->gt_start_thread_count, GTMThreads->gt_thread_count);

	/* init statistic time */
    GTM_InitGtmStatistics();

    /* init log hook */
    errlog_collection_func = GTM_ErrorLogCollector;

#endif
    fprintf(stdout, "TBase GTM is ready to go!!\n");
    /*
     * Accept any new connections. Fork a new thread for each incoming
     * connection
     */
    status = ServerLoop();

    /*
     * ServerLoop probably shouldn't ever return, but if it does, close down.
     */
    exit(status != STATUS_OK);

    return 0;                    /* not reached */
}

/*
 * ConnCreate -- create a local connection data structure
 */
static Port *
ConnCreate(int serverFd)
{
    Port       *port;

    if (!(port = (Port *) calloc(1, sizeof(Port))))
    {
        ereport(LOG,
                (ENOMEM,
                 errmsg("out of memory")));
        exit(1);
    }

    if (StreamConnection(serverFd, port) != STATUS_OK)
    {
        if (port->sock >= 0)
            StreamClose(port->sock);
        ConnFree(port);
        return NULL;
    }

    port->conn_id = InvalidGTMProxyConnID;
    return port;
}

/*
 * ConnFree -- free a local connection data structure
 */
void
ConnFree(Port *conn)
{
    free(conn);
}

/*
 * Main idle loop of postmaster
 */
static int
ServerLoop(void)
{// #lizard forgives
    fd_set        readmask;
    int            nSockets;
    sigjmp_buf  local_sigjmp_buf;
    GTM_ThreadInfo *my_threadinfo = GetMyThreadInfo;

    /* Release the lock we acquire in MainThreadInit. */    
    GTM_RWLockRelease(&my_threadinfo->thr_lock);
    if (sigsetjmp(local_sigjmp_buf, 1) != 0)
    {
        RWLockCleanUp();
        /* Report the error to the server log */
        EmitErrorReport(NULL);

        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */        
        FlushErrorState();
    }

    
    
    /* We can now handle ereport(ERROR) */
    PG_exception_stack = &local_sigjmp_buf;

    nSockets = initMasks(&readmask);
    for (;;)
    {
        fd_set        rmask;
        int            selres;

        //MemoryContextStats(TopMostMemoryContext);

        /*
         * Wait for a connection request to arrive.
         *
         * We wait at most one minute, to ensure that the other background
         * tasks handled below get done even when no requests are arriving.
         */
        memcpy((char *) &rmask, (char *) &readmask, sizeof(fd_set));

        PG_SETMASK(&UnBlockSig);

        /* if timekeeper thread exit, main thread should prepare to exit. */
        if (GTMAbortPending || NULL == g_timekeeper_thread || NULL == g_checkpoint_thread)
        {
            /*
             * XXX We should do a clean shutdown here. For the time being, just
             * write the next GXID to be issued in the control file and exit
             * gracefully
             */

            elog(LOG, "GTM shutting down.");
            
            /*
             * Tell GTM that we are shutting down so that no new GXIDs are
             * issued this point onwards
             */
            GTM_SetShuttingDown();            

            elog(LOG, "GTM timer thread exit.");
#ifdef __TBASE__

#ifdef __XLOG__
            if(Recovery_IsStandby())
                WaitRedoertoExit();
            else
                DoCheckPoint(true);

            GTM_RWLockAcquire(&ControlDataLock,GTM_LOCKMODE_WRITE);
            ControlData->state = DB_SHUTDOWNED;
            XLogCtlShutDown();
            ControlDataSync(true);
#else
            GTM_StoreShutDown();
#endif

#else            
            SaveControlInfo();
#endif

#ifdef __XLOG__
            /* Delete pid file */
            DeleteLockFile(GTM_PID_FILE);
#endif

#ifdef __TBASE__
#ifdef HAVE_UNIX_SOCKETS
            RemoveSocketFile();
#endif
#endif
            elog(LOG, "GTM exits");
            exit(1);
        }

		if(ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(GTMC_SIGHUP);
			load_sync_structures();
		}


        {
            /* must set timeout each time; some OSes change it! */
            struct timeval timeout;        


            timeout.tv_sec = 60;
            timeout.tv_usec = 0;
            /* No need to take the lock now. */
#if 0
            /*
             * Now GTM-Standby can backup current status during this region
             */
            GTM_RWLockRelease(&my_threadinfo->thr_lock);
#endif

            selres = select(nSockets, &rmask, NULL, NULL, &timeout);
#if 0

            /*
             * Prohibit GTM-Standby backup from here.
             */
            GTM_RWLockAcquire(&my_threadinfo->thr_lock, GTM_LOCKMODE_WRITE);
#endif
        }

        /*
         * Block all signals until we wait again.  (This makes it safe for our
         * signal handlers to do nontrivial work.)
         */
        PG_SETMASK(&BlockSig);


        /* Now check the select() result */
        if (selres < 0)
        {
            if (errno != EINTR && errno != EWOULDBLOCK)
            {
                ereport(LOG,
                        (EACCES,
                         errmsg("select() failed in main thread: %m")));
                return STATUS_ERROR;
            }
        }
        
        /*
         * New connection pending on any of our sockets? If so, fork a child
         * process to deal with it.
         */
        if (selres > 0)
        {
            int            i;

            for (i = 0; i < MAXLISTEN; i++)
            {
                if (ListenSocket[i] == -1)
                {
                    break;
                }
                
                if (FD_ISSET(ListenSocket[i], &rmask))
                {
                    Port       *port;

                    port = ConnCreate(ListenSocket[i]);
                    if (port)
                    {
                        if (GTMAddConnection(port, NULL) != STATUS_OK)
                        {
                            StreamClose(port->sock);
                            ConnFree(port);
                        }
                    }
                }
            }
        }
    }
}

/*
 * add connection into g_standby_pre_server_thread
 */
static int
gtm_add_connection_standby_pre_server(Port *port)
{
    GTM_ConnectionInfo *conninfo = NULL;
    struct epoll_event event;

    if (!g_standby_pre_server_thread->thr_epoll_ok)
    {
        elog(LOG, "g_standby_pre_server_thread epoll not ready.");
        return STATUS_ERROR;
    }

    conninfo = (GTM_ConnectionInfo *)palloc0(sizeof (GTM_ConnectionInfo));
    conninfo->con_port = port;
    conninfo->con_init = false;
    port->conn = conninfo;

    /* Set conn to non-blocking mode for epoll wait */
    SetNonBlockConnection(conninfo);

    conninfo->con_thrinfo = g_standby_pre_server_thread;
    event.data.ptr = conninfo;
    event.events = EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP;
    if(-1 == epoll_ctl (g_standby_pre_server_thread->thr_efd, EPOLL_CTL_ADD, conninfo->con_port->sock, &event))
    {
        elog(LOG, "failed to add socket to epoll");
        return STATUS_ERROR;
    }

    return STATUS_OK;
}

/*
 * handle loop before establish a connection to active-GTM
 */
static void
gtm_standby_pre_server_loop(char *data_dir)
{
    fd_set		readmask;
    int			nSockets;
    sigjmp_buf  local_sigjmp_buf;

    /*
     * recovery here first
     */
    GTM_XLogRecoveryIfNeed(data_dir);

    /*
     * start GTM_ThreadMain to support get gtm status command
     */
    g_standby_pre_server_thread = GTM_ThreadCreate(GTM_ThreadMain, g_max_lock_number);
    if (NULL == g_standby_pre_server_thread)
    {
        elog(LOG, "Failed to create standby_pre_server_thread thread");
        exit(1);
    }

    if (sigsetjmp(local_sigjmp_buf, 1) != 0)
    {
        RWLockCleanUp();
        /* Report the error to the server log */
        EmitErrorReport(NULL);

        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        FlushErrorState();
    }

    /* We can now handle ereport(ERROR) */
    PG_exception_stack = &local_sigjmp_buf;

    nSockets = initMasks(&readmask);
    while (Recovery_IsStandby())
    {
        fd_set		rmask;
        int			selres;

        /*
         * Wait for a connection request to arrive.
         *
         * We wait at most one minute, to ensure that the other background
         * tasks handled below get done even when no requests are arriving.
         */
        memcpy((char *) &rmask, (char *) &readmask, sizeof(fd_set));

        PG_SETMASK(&UnBlockSig);

        /* if timekeeper thread exit, main thread should prepare to exit. */
        if (GTMAbortPending)
        {
            /*
             * XXX We should do a clean shutdown here. For the time being, just
             * write the next GXID to be issued in the control file and exit
             * gracefully
             */

            elog(LOG, "GTM shutting down.");

            /*
             * Tell GTM that we are shutting down so that no new GXIDs are
             * issued this point onwards
             */
            GTM_SetShuttingDown();

            GTM_RWLockAcquire(&ControlDataLock,GTM_LOCKMODE_WRITE);
            ControlData->state = DB_SHUTDOWNED;
            ControlDataSync(false);
            GTM_RWLockRelease(&ControlDataLock);

            /* Delete pid file */
            DeleteLockFile(GTM_PID_FILE);
#ifdef HAVE_UNIX_SOCKETS
            RemoveSocketFile();
#endif
            elog(LOG, "GTM exits");
            exit(1);
        }

        {
            /* must set timeout each time; some OSes change it! */
            struct timeval timeout;

            timeout.tv_sec = 5;
            timeout.tv_usec = 0;

            selres = select(nSockets, &rmask, NULL, NULL, &timeout);
        }

        /*
         * Block all signals until we wait again.  (This makes it safe for our
         * signal handlers to do nontrivial work.)
         */
        PG_SETMASK(&BlockSig);

        /* Now check the select() result */
        if (selres < 0)
        {
            if (errno != EINTR && errno != EWOULDBLOCK)
            {
                ereport(LOG,
                        (EACCES,
                                errmsg("select() failed in main thread: %m")));
                exit(1);
            }
        }

        /*
         * New connection pending on any of our sockets? If so, fork a child
         * process to deal with it.
         */
        if (selres > 0)
        {
            int			i;

            for (i = 0; i < MAXLISTEN; i++)
            {
                if (ListenSocket[i] == -1)
                {
                    break;
                }

                if (FD_ISSET(ListenSocket[i], &rmask))
                {
                    Port	   *port;

                    port = ConnCreate(ListenSocket[i]);
                    if (port)
                    {
                        if (gtm_add_connection_standby_pre_server(port) != STATUS_OK)
                        {
                            StreamClose(port->sock);
                            ConnFree(port);
                        }
                    }
                }
            }
        }

        /*
         * retry establish a connection between the active and standby,
         * controlling frequency with select timeout
         */
        if (gtm_standby_start_startup(GTM_STARTUP_CONNECT_ACTIVE_TIMEOUT))
        {
            elog(LOG, "Standby GTM Startup connection established with active-GTM.");
            break;
        }
        elog(LOG, "Failed to establish a connection to active-GTM.");
    }

    /*
     * clear exception stack here
     */
    PG_exception_stack = NULL;
}

/*
 * Initialise the masks for select() for the ports we are listening on.
 * Return the number of sockets to listen on.
 */
static int
initMasks(fd_set *rmask)
{
    int            maxsock = -1;
    int            i;

    FD_ZERO(rmask);

    for (i = 0; i < MAXLISTEN; i++)
    {
        int            fd = ListenSocket[i];

        if (fd == -1)
            break;
        FD_SET(fd, rmask);
        if (fd > maxsock)
            maxsock = fd;
    }

    return maxsock + 1;
}

void
GTM_PortCleanup(Port *con_port)
{
    if (!con_port)
    {
        return;
    }
    
    StreamClose(con_port->sock);

    /* Free the node_name in the port */
    if (con_port->node_name != NULL)
        /*
         * We don't have to reset pointer to NULL her because ConnFree()
         * frees this structure next.
         */
        pfree(con_port->node_name);

#ifndef __TBASE__
    if(con_port->remote_host)
    {
        free(con_port->remote_host);
    }

    if(con_port->remote_port)
    {
        free(con_port->remote_port);
    }
#endif

    /* Free the port */
    ConnFree(con_port);
}

void
GTM_ConnCleanup(GTM_ConnectionInfo *conn)
{
    MemoryContext oldContext;
    
    if(NULL == conn)
    {
        return;
    }
    /*
     * Close a connection to GTM standby.
     */
    oldContext = MemoryContextSwitchTo(TopMemoryContext);

#ifndef __XLOG__
    if (conn->standby)
    {
        elog(DEBUG1, "Closing a connection to the GTM standby.");
    
        GTMPQfinish(conn->standby);
        conn->standby = NULL;
    }
#endif

    if (conn->con_port)
    {
        GTM_PortCleanup(conn->con_port);
        conn->con_port = NULL;
    }
    /* Free the connection info structure */
    pfree(conn);
    
    MemoryContextSwitchTo(oldContext);
}

void
GTM_RemoveConnection(GTM_ConnectionInfo *conn)
{
    epoll_ctl(GetMyThreadInfo->thr_efd,EPOLL_CTL_DEL,conn->con_port->sock,NULL);

    Recovery_PGXCNodeDisconnect(conn->con_port);
    GTM_ConnCleanup(conn);
}

void bind_thread_to_cores (cpu_set_t cpuset) 
{

   pthread_t current_thread = pthread_self(); 
   int r;

   if((r = pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset)))
   {
        elog(ERROR, "binding threads failed for %d", r);
   }
}

void bind_timekeeper_thread(void)
{
    cpu_set_t cpuset;
    
    
    CPU_ZERO(&cpuset);
    CPU_SET(0, &cpuset); /* dedicate cpu 0 to the timekeeper thread */
    
    bind_thread_to_cores(cpuset);
}

void bind_service_threads(void)
{
    int cpu_num = get_nprocs();
    int cpu;
    cpu_set_t cpuset;
    
    
    CPU_ZERO(&cpuset);
    for(cpu = 1; cpu < cpu_num; cpu++)
    {
        CPU_SET(cpu, &cpuset);
    }
    
    bind_thread_to_cores(cpuset);
}

/* time keeper thread will not handle any signal, any signal will cause the thread exit. */
void 
*
GTM_ThreadTimeKeeper(void *argp)
{// #lizard forgives
    GTM_ThreadInfo *my_threadinfo = (GTM_ThreadInfo *)argp;
    sigjmp_buf  local_sigjmp_buf;
    time_t      last;
    time_t        now;
    int ret;
    struct sigaction    action;  
       
    action.sa_flags = 0;  
    action.sa_handler = GTM_ThreadSigHandler;  
	sigemptyset(&action.sa_mask);
         
    ret = sigaction(SIGQUIT, &action, NULL);  
    if (ret)
    {
        elog(LOG, "register thread quit handler failed");
    }

    elog(DEBUG8, "Starting the time keeper thread");
    bind_timekeeper_thread();

    MessageContext = AllocSetContextCreate(TopMemoryContext,
                                       "MessageContext",
                                       ALLOCSET_DEFAULT_MINSIZE,
                                       ALLOCSET_DEFAULT_INITSIZE,
                                       ALLOCSET_DEFAULT_MAXSIZE,
                                       false);

    /*
     * POSTGRES main processing loop begins here
     *
     * If an exception is encountered, processing resumes here so we abort the
     * current transaction and start a new one.
     *
     * You might wonder why this isn't coded as an infinite loop around a
     * PG_TRY construct.  The reason is that this is the bottom of the
     * exception stack, and so with PG_TRY there would be no exception handler
     * in force at all during the CATCH part.  By leaving the outermost setjmp
     * always active, we have at least some chance of recovering from an error
     * during error recovery.  (If we get into an infinite loop thereby, it
     * will soon be stopped by overflow of elog.c's internal state stack.)
     */

    if (sigsetjmp(local_sigjmp_buf, 1) != 0)
    {
#ifdef __TBASE__
        RWLockCleanUp();
#endif
        EmitErrorReport(NULL);
        
        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        MemoryContextSwitchTo(TopMemoryContext);
        FlushErrorState();
    }

    /* We can now handle ereport(ERROR) */
    PG_exception_stack = &local_sigjmp_buf;

    for(;;)
    {
        GlobalTimestamp latestGlobalTimestamp;            
        
        MemoryContextSwitchTo(MessageContext);
        MemoryContextResetAndDeleteChildren(MessageContext);
        
        /*
         * Sync timestamp to standby.
         * Use lock to protect standby connection, in case gtm standby reconnect.
         * Also timestamp to be flushed to disk.
         */        

        /* no need to lock here. */
        if(GTM_SHUTTING_DOWN == GTMTransactions.gt_gtm_state)
        {
            goto shutdown;
        }

#ifdef __XLOG__
        if(Recovery_IsStandby())
        {
            /* we save g_last_sync_gts here because get gts command is still used when role is standby*/
            SpinLockAcquire(&g_last_sync_gts_lock);
            g_last_sync_gts = GTM_TimestampGetMonotonicRaw();
            SpinLockRelease(&g_last_sync_gts_lock);

            usleep(GTM_SYNC_CYCLE);
            continue;
        }
#endif

        latestGlobalTimestamp = SyncGlobalTimestamp();
        last = GTM_TimestampGetMonotonicRaw();

        if(GTM_StoreGlobalTimestamp(latestGlobalTimestamp))
        {
            elog(LOG, "storing global timestamp failed, going to exit!!");
            exit(1);
        }

#ifdef __XLOG__
        SpinLockAcquire(&g_last_sync_gts_lock);
        g_last_sync_gts = last;
        SpinLockRelease(&g_last_sync_gts_lock);
#endif

        usleep(GTM_SYNC_CYCLE);

        now = GTM_TimestampGetMonotonicRaw();    
        if((now - last) > GTM_SYNC_TIME_LIMIT)
        {
            elog(LOG, "The timekeeper thread takes too long "INT64_FORMAT " seconds to complete", (now - last)/1000000);
        }
    }
shutdown:
    g_timekeeper_thread = NULL;
    elog(LOG, "GTM is shutting down, timekeeper exits!");
    return my_threadinfo;    
}

#ifndef __XLOG__
/* time keeper thread will not handle any signal, any signal will cause the thread exit. */
void 
*
GTM_ThreadTimeBackup(void *argp)
{// #lizard forgives
    GTM_ThreadInfo *my_threadinfo = (GTM_ThreadInfo *)argp;
    sigjmp_buf  local_sigjmp_buf;
    time_t      last;
    time_t        now;    
    int ret;
    struct sigaction    action;  
#ifdef __TBASE__
    GTM_ConnectionInfo  fake_conn;
    GTM_ConnectionInfo *conn;
    memset(&fake_conn, 0X00, sizeof(GTM_ConnectionInfo));
    conn = &fake_conn;     
#endif

       
    action.sa_flags = 0;  
    action.sa_handler = GTM_ThreadSigHandler;  
         
    ret = sigaction(SIGQUIT, &action, NULL);  
    if (ret)
    {
        elog(LOG, "register thread quit handler failed");
    }


    elog(DEBUG8, "Starting the time backup thread");
    bind_timekeeper_thread();

    MessageContext = AllocSetContextCreate(TopMemoryContext,
                                       "MessageContext",
                                       ALLOCSET_DEFAULT_MINSIZE,
                                       ALLOCSET_DEFAULT_INITSIZE,
                                       ALLOCSET_DEFAULT_MAXSIZE,
                                       false);

    /*
     * POSTGRES main processing loop begins here
     *
     * If an exception is encountered, processing resumes here so we abort the
     * current transaction and start a new one.
     *
     * You might wonder why this isn't coded as an infinite loop around a
     * PG_TRY construct.  The reason is that this is the bottom of the
     * exception stack, and so with PG_TRY there would be no exception handler
     * in force at all during the CATCH part.  By leaving the outermost setjmp
     * always active, we have at least some chance of recovering from an error
     * during error recovery.  (If we get into an infinite loop thereby, it
     * will soon be stopped by overflow of elog.c's internal state stack.)
     */

    if (sigsetjmp(local_sigjmp_buf, 1) != 0)
    {
#ifdef __TBASE__
        RWLockCleanUp();
#endif
        EmitErrorReport(NULL);
        
        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        MemoryContextSwitchTo(TopMemoryContext);
        FlushErrorState();
    }

    /* We can now handle ereport(ERROR) */
    PG_exception_stack = &local_sigjmp_buf;

    for(;;)
    {
        bool            lock_result = false;
        GlobalTimestamp latestGlobalTimestamp;            
        
        MemoryContextSwitchTo(MessageContext);
        MemoryContextResetAndDeleteChildren(MessageContext);
        
        /*
         * Sync timestamp to standby.
         * Use lock to protect standby connection, in case gtm standby reconnect.
         * Also timestamp to be flushed to disk.
         */        
        latestGlobalTimestamp = GetNextGlobalTimestamp();
        last = GTM_TimestampGetMonotonicRaw();                
        
        /* no need to lock here. */
        if(GTM_SHUTTING_DOWN == GTMTransactions.gt_gtm_state)
        {
            goto shutdown;
        }

        /* check standby connection before we get lock. */
        CheckStandbyConnect(my_threadinfo, conn);
        lock_result = GTM_RWLockConditionalAcquire(&my_threadinfo->thr_lock, GTM_LOCKMODE_WRITE);            
        if (lock_result)
        {
            if (conn->standby)
            {
                if(bkup_global_timestamp(conn->standby, latestGlobalTimestamp + GTM_GLOBAL_TIME_DELTA))
                {
                    /* close standby connection. */
                    gtm_standby_disconnect_from_standby(conn->standby);
                    conn->standby = NULL;
                    GTM_RWLockRelease(&my_threadinfo->thr_lock);
                    elog(LOG, "bkup timestamp to standby failed!!");
                    continue;
                }
                /* Sync with standby */
                gtm_sync_standby(conn->standby);
            }

            /*
             * Now GTM-Standby can backup current status during this region
             */
            GTM_RWLockRelease(&my_threadinfo->thr_lock);
        }
        else
        {
            elog(LOG, "Time backup lock thread failed!!");
        }        
        usleep(GTM_SYNC_CYCLE);        

        now = GTM_TimestampGetMonotonicRaw();        
        if((now - last) > GTM_SYNC_TIME_LIMIT)
        {
            elog(LOG, "The timebackup thread takes too long "INT64_FORMAT " seconds to complete.", now - last);
        }
    }
shutdown:
    g_timebackup_thread = NULL;
    elog(LOG, "GTM is shuting down, timebackup thread exits!");
    return my_threadinfo;    
}
#endif

#ifdef __XLOG__
void *
GTM_ThreadCheckPointer(void *argp)
{
    GTM_ThreadInfo *my_threadinfo = (GTM_ThreadInfo *)argp;
    sigjmp_buf  local_sigjmp_buf;
    int ret;
    struct sigaction    action;  
       
    action.sa_flags = 0;  
    action.sa_handler = GTM_ThreadSigHandler;  
	sigemptyset(&action.sa_mask);
    ret = sigaction(SIGQUIT, &action, NULL);  
    if (ret)
    {
        elog(LOG, "register thread quit handler failed");
    }

    MessageContext = AllocSetContextCreate(TopMemoryContext,
                                       "MessageContext",
                                       ALLOCSET_DEFAULT_MINSIZE,
                                       ALLOCSET_DEFAULT_INITSIZE,
                                       ALLOCSET_DEFAULT_MAXSIZE,
                                       false);

    /*
     * POSTGRES main processing loop begins here
     *
     * If an exception is encountered, processing resumes here so we abort the
     * current transaction and start a new one.
     *
     * You might wonder why this isn't coded as an infinite loop around a
     * PG_TRY construct.  The reason is that this is the bottom of the
     * exception stack, and so with PG_TRY there would be no exception handler
     * in force at all during the CATCH part.  By leaving the outermost setjmp
     * always active, we have at least some chance of recovering from an error
     * during error recovery.  (If we get into an infinite loop thereby, it
     * will soon be stopped by overflow of elog.c's internal state stack.)
     */

    elog(LOG, "checkpointer start.");

    if (sigsetjmp(local_sigjmp_buf, 1) != 0)
    {
#ifdef __TBASE__
        RWLockCleanUp();
#endif
        EmitErrorReport(NULL);
        
        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        MemoryContextSwitchTo(TopMemoryContext);
        FlushErrorState();
    }

    /* We can now handle ereport(ERROR) */
    PG_exception_stack = &local_sigjmp_buf;

    GTM_MutexLockAcquire(&g_checkpointer_blocker);

    for(;;)
    {        
        MemoryContextSwitchTo(MessageContext);
        MemoryContextResetAndDeleteChildren(MessageContext);

        {
            pg_time_t start_time = time(NULL);
            pg_time_t end_time   = start_time + checkpoint_interval * 60 ; /* minute to seconds */

            while(start_time < end_time)
            {
                sleep(end_time - start_time);
                start_time = time(NULL);
            }
        }

        /* no need to lock here. */
        if(GTM_SHUTTING_DOWN == GTMTransactions.gt_gtm_state)
        {
            goto shutdown;
        }

        DoCheckPoint(false);

    }
shutdown:
    g_checkpoint_thread = NULL;
    elog(LOG, "GTM is shutting down, checkpoint exits!");
    return my_threadinfo;    
}

static void thread_replication_clean(GTM_StandbyReplication *replication)
{
    elog(LOG,"Replication exits %s",replication->application_name);

    if(replication->is_sync)
    {
    	GTM_MutexLockAcquire(&XLogSync->check_mutex);
        RemoveSyncStandby(replication);
		GTM_MutexLockRelease(&XLogSync->check_mutex);
		 
        elog(LOG,"sync standby disconnected");
    }

    if(replication->port != NULL)
        epoll_ctl(GetMyThreadInfo->thr_efd,EPOLL_CTL_DEL,replication->port->sock,NULL);

    gtm_close_replication(replication);
    GTM_PortCleanup(replication->port);
}

void *
GTM_ThreadWalSender(void *argp)
{// #lizard forgives
    GTM_ThreadInfo *       my_threadinfo = (GTM_ThreadInfo *)argp;
    sigjmp_buf             local_sigjmp_buf;
    struct sigaction       action;
    
    GTM_StandbyReplication *replication = NULL;
    Port                   *standby;
    int                     efd,ret;
    struct epoll_event ev;

    action.sa_flags = 0;
    action.sa_handler = GTM_ThreadSigHandler;  
	sigemptyset(&action.sa_mask);
    ret = sigaction(SIGQUIT, &action, NULL);  
    
    if (ret)
    {
        elog(LOG, "register thread quit handler failed");
    }

    efd  = epoll_create1(0);
    if(efd == -1)
    {
        elog(LOG,"epoll create fail %s",strerror(errno));
        exit(1);
    }

    my_threadinfo->thr_efd      = efd;

    replication = register_self_to_standby_replication();
    Assert(replication);

    MessageContext = AllocSetContextCreate(TopMemoryContext,
                                       "MessageContext",
                                       ALLOCSET_DEFAULT_MINSIZE,
                                       ALLOCSET_DEFAULT_INITSIZE,
                                       ALLOCSET_DEFAULT_MAXSIZE,
                                       false);
                                       
    if (sigsetjmp(local_sigjmp_buf, 1) != 0)
    {
        RWLockCleanUp();
        EmitErrorReport(NULL);
        MemoryContextSwitchTo(TopMemoryContext);
        FlushErrorState();
        thread_replication_clean(replication);
    }
    
    PG_exception_stack = &local_sigjmp_buf;
    for(;;)
    {
        /* wait for communication */
        for(;;)
        {
            GTM_MutexLockAcquire(&replication->lock);
            if(replication->is_use)
            {
                elog(LOG,"Acquire one standby %s",replication->application_name);
                GTM_MutexLockRelease(&replication->lock);
                break;
            }
            GTM_MutexLockRelease(&replication->lock);

            pg_usleep(GTMArchiverCheckInterval);
        }

        standby = replication->port;

        ev.data.fd = standby->sock;
        ev.events  = EPOLLIN | EPOLLERR | EPOLLHUP;

        if(epoll_ctl(efd,EPOLL_CTL_ADD,standby->sock,&ev) == -1)
            elog(ERROR,"epoll fails %s",strerror(errno));

        for(;;)
        {        
            MemoryContextSwitchTo(MessageContext);
            MemoryContextResetAndDeleteChildren(MessageContext);
            
            if(GTM_SHUTTING_DOWN == GTMTransactions.gt_gtm_state)
                goto shutdown;

            while((ret = GTM_GetReplicationResultIfAny(replication,standby)) == 1)
                continue;

            if(ret == EOF)
                elog(ERROR, "Replication connection fault");

            if(GTM_HasXLogToSend(replication))
            {
                if(SendXLogContext(replication,standby) == false)
                    elog(ERROR,"Replication connection fault");
            }
        }
    }

shutdown:
    elog(LOG, "GTM is shutting down, walsender exits!");
    return my_threadinfo;    
}

/*
 * Log collection thread, responsible for summarizing
 * the log data of each thread to global datapump
 */
void *
GTM_ThreadLogCollector(void *argp)
{
    GTM_ThreadInfo *my_threadinfo = (GTM_ThreadInfo *)argp;
    sigjmp_buf  local_sigjmp_buf;
    struct sigaction action;
    int ret = 0;
    action.sa_flags = 0;
    action.sa_handler = GTM_ThreadSigHandler;

    ret = sigaction(SIGQUIT, &action, NULL);
    if (ret)
    {
        elog(LOG, "register thread quit handler failed");
    }

    elog(DEBUG8, "Starting the log collector thread");
    MessageContext = AllocSetContextCreate(TopMemoryContext,
                                           "MessageContext",
                                           ALLOCSET_DEFAULT_MINSIZE,
                                           ALLOCSET_DEFAULT_INITSIZE,
                                           ALLOCSET_DEFAULT_MAXSIZE,
                                           false);

    /*
     * POSTGRES main processing loop begins here
     *
     * If an exception is encountered, processing resumes here so we abort the
     * current transaction and start a new one.
     *
     * You might wonder why this isn't coded as an infinite loop around a
     * PG_TRY construct.  The reason is that this is the bottom of the
     * exception stack, and so with PG_TRY there would be no exception handler
     * in force at all during the CATCH part.  By leaving the outermost setjmp
     * always active, we have at least some chance of recovering from an error
     * during error recovery.  (If we get into an infinite loop thereby, it
     * will soon be stopped by overflow of elog.c's internal state stack.)
     */

    if (sigsetjmp(local_sigjmp_buf, 1) != 0)
    {
#ifdef __TBASE__
        RWLockCleanUp();
#endif
        EmitErrorReport(NULL);

        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        MemoryContextSwitchTo(TopMemoryContext);
        FlushErrorState();
    }

    /* We can now handle ereport(ERROR) */
    PG_exception_stack = &local_sigjmp_buf;

    if (GTM_InitLogCollector() != 0)
    {
        elog(ERROR, "Failed to Init LogCollector.");
        exit(1);
    }

    for(;;)
    {
        /* no need to lock here. */
        if(GTM_SHUTTING_DOWN == GTMTransactions.gt_gtm_state)
        {
           break;
        }

        /* sleep GTM_LOG_COLLECT_CYCLE */
        usleep(GTM_LOG_COLLECT_CYCLE);

        GTM_ProcessLogCollection();
    }

    elog(LOG, "GTM is shutting down, log collector exits!");
    return my_threadinfo;
}

void
SendXLogSyncStatus(GTM_Conn *conn)
{// #lizard forgives
    XLogwrtResult result;
    XLogRecPtr    apply;
    
    if (gtmpqPutMsgStart('C', true, conn) ||
        gtmpqPutInt(MSG_GET_REPLICATION_STATUS, sizeof (GTM_MessageType), conn))
        goto send_failed;

    /* we acquire locks once at a time to avoid dead lock */
    SpinLockAcquire(&XLogCtl->walwirte_info_lck);
    result = XLogCtl->LogwrtResult;
    SpinLockRelease(&XLogCtl->walwirte_info_lck);

    GTM_RWLockAcquire(&XLogCtl->standby_info_lck,GTM_LOCKMODE_READ);
    apply  = XLogCtl->apply;
    GTM_RWLockRelease(&XLogCtl->standby_info_lck);

    if(gtmpqPutInt64(result.Write,conn))
        goto send_failed;
    if(gtmpqPutInt64(result.Flush,conn))
        goto send_failed;
    if(gtmpqPutInt64(apply,conn))
        goto send_failed;
    if(gtmpqPutInt(GetCurrentTimeLineID(),sizeof(TimeLineID),conn))
        goto send_failed;
                
    if (gtmpqPutMsgEnd(conn))
        goto send_failed;
    if (gtmpqFlush(conn))
        goto send_failed;

    if(enalbe_gtm_xlog_debug)
        elog(LOG,"send xlog sync staus write : %X/%X,flush : %X/%X, apply: %X/%X timeline: %d",
             (uint32_t)(result.Write >> 32),
             (uint32_t)(result.Write),
             (uint32_t)(result.Flush >> 32),
             (uint32_t)(result.Flush),
             (uint32_t)(apply>> 32),
             (uint32_t)(apply),GetCurrentTimeLineID());
        
    return ;
send_failed:
    elog(LOG,"send xlog status to gtm master fails");
}

static bool StringEndWith(const char *str,const char *pattern)
{
    int str_len ,pattern_len;

    str_len = strlen(str);
    pattern_len = strlen(pattern);

    if(str_len < pattern_len)
        return false;

    if(strncmp(str + str_len - pattern_len,pattern,pattern_len) == 0)
        return true;
    return false;
}

static bool IsValidXLogStatusFile(const char *str,XLogSegNo *no,TimeLineID *timeline)
{
    int tmp = 0;
    char data[9] = {0};
    int i;
    
#define XLOG_NAME_LENGTH 24

    if(strlen(str) < XLOG_NAME_LENGTH)
        return false;

    for (i = 0 ; i < XLOG_NAME_LENGTH ; i++)
    {
        if(!('0' <= str[i]  && str[i] <= '9') && !('A' <= str[i]  && str[i] <= 'F'))
            return false;
    }

    memcpy(data,str,8);
    sscanf(data,"%X",timeline);

    *no = 0;
    memcpy(data,str + 8 ,8);
    sscanf(data,"%X",&tmp);

    *no = tmp * GTMXLogSegmentsPerXLogId;
    memcpy(data,str + 8 + 8 ,8);
    sscanf(data,"%X",&tmp);

    *no += tmp;

    elog(LOG,"IsValidXLogStatusFile %s %ld",str,*no);

    return true;
}

void *
GTM_ThreadArchiver(void *argp)
{// #lizard forgives
    GTM_ThreadInfo *       my_threadinfo = (GTM_ThreadInfo *)argp;
    sigjmp_buf  local_sigjmp_buf;
    int ret;
    struct sigaction    action;
    char command[MAX_COMMAND_LEN];
    char file_name[MAXFNAMELEN];
    char file_name_no_dirname[MAXFNAMELEN];
    char file_xlog_status[MAXFNAMELEN];
    XLogSegNo   segment_no = 0;
    TimeLineID  timeLine   = FIRST_TIMELINE_ID;

    XLogSegNo   delete_segment_no = 0;
    TimeLineID  delete_timeLine   = FIRST_TIMELINE_ID;

    XLogRecPtr  min_replication_pos = InvalidXLogRecPtr;

    action.sa_flags = 0;
    action.sa_handler = GTM_ThreadSigHandler;
	sigemptyset(&action.sa_mask);

    ret = sigaction(SIGQUIT, &action, NULL);

    if (ret)
    {
        elog(LOG, "register thread quit handler failed");
    }

    MessageContext = AllocSetContextCreate(TopMemoryContext,
                                           "MessageContext",
                                           ALLOCSET_DEFAULT_MINSIZE,
                                           ALLOCSET_DEFAULT_INITSIZE,
                                           ALLOCSET_DEFAULT_MAXSIZE,
                                           false);

    if (sigsetjmp(local_sigjmp_buf, 1) != 0)
    {
        RWLockCleanUp();
        EmitErrorReport(NULL);
        MemoryContextSwitchTo(TopMemoryContext);
        FlushErrorState();
    }

    PG_exception_stack = &local_sigjmp_buf;

    system("mkdir -p gtm_xlog/archive_status");

    /* 
     * sleep 60 seconds before backend replication reconnects 
     * use while to avoid signal interrupt
    */
    
    {
        time_t sleep_to = time(NULL) + 60;
        while(sleep_to >= time(NULL))
        {
            sleep(1);
        }
    }
    {
        DIR *dp                = NULL;
        struct dirent* ep      = NULL;
        bool ready_found = false;
        bool done_found  = false;
        XLogSegNo  c_no;
        TimeLineID c_timeline;

        dp = opendir("gtm_xlog/archive_status/");
        if(dp != NULL)
        {

            while((ep = readdir(dp)))
            {
                if(StringEndWith(ep->d_name,".ready") && IsValidXLogStatusFile(ep->d_name,&c_no,&c_timeline))
                {
                    if(c_no < segment_no || ready_found == false)
                    {
                        segment_no = c_no ;
                        timeLine   = c_timeline;
                        ready_found = true;
                    }
                }
                else if(StringEndWith(ep->d_name,".done") && IsValidXLogStatusFile(ep->d_name,&c_no,&c_timeline))
                {
                    if(c_no < delete_segment_no || done_found == false)
                    {
                        delete_segment_no = c_no ;
                        delete_timeLine   = c_timeline;
                        done_found = true;
                    }
                }
            }
            closedir(dp);
        }
        else
        {
            elog(LOG,"couldn't open xlog dir");
            exit(1);
        }

        if(!ready_found)
        {
            dp = opendir("gtm_xlog");
            if(dp == NULL)
            {
                elog(LOG,"couldn't open xlog dir");
                exit(1);
            }

            while ((ep = readdir(dp)))
            {
                if (IsValidXLogStatusFile(ep->d_name, &c_no, &c_timeline))
                {
                    if (c_no < segment_no || ready_found == false)
                    {
                        segment_no = c_no;
                        timeLine = c_timeline;
                        ready_found = true;
                    }
                }
            }
            closedir(dp);
        }

        if(!done_found)
        {
            delete_segment_no = segment_no;
            delete_timeLine   = timeLine;
        }
    }

    if(enalbe_gtm_xlog_debug)
        elog(LOG,"start archive segment no :%ld timeline: %d delete no:%ld timeline: %d"
                ,segment_no,timeLine
                ,delete_segment_no,delete_timeLine);

    if(archive_command == NULL)
        archive_mode = false;

    for(;;)
    {
        if(archive_mode == false || delete_segment_no < segment_no)
        {
            XLogSegNo min_delete_segment_no;
            char      gts_file[MAXFNAMELEN];

            GTM_RWLockAcquire(&ControlDataLock,GTM_LOCKMODE_READ);
            min_delete_segment_no = GetSegmentNo(ControlData->checkPoint);
            GTM_RWLockRelease(&ControlDataLock);

            min_replication_pos = GetMinReplicationRequiredLocation();

            if(min_replication_pos != InvalidXLogRecPtr && GetSegmentNo(min_replication_pos) < min_delete_segment_no)
                min_delete_segment_no = GetSegmentNo(min_replication_pos);

            if(min_delete_segment_no > delete_segment_no)
            {
                GTMXLogFileName(file_name,delete_timeLine,delete_segment_no);
                if(IsXLogFileExist(file_name) == false)
                {
                    pg_usleep(GTMArchiverCheckInterval);
                    delete_timeLine++;
                    continue;
                }

                GTMXLogFileGtsName(gts_file,delete_timeLine,delete_segment_no);
                if(archive_command)
                    GTMXLogFileStatusDoneName(file_xlog_status,delete_timeLine,delete_segment_no);
                else
                    GTMXLogFileStatusReadyName(file_xlog_status,delete_timeLine,delete_segment_no);

                snprintf(command,MAX_COMMAND_LEN,"rm -rf %s %s %s",file_name,gts_file,file_xlog_status);

                if(enalbe_gtm_xlog_debug)
                    elog(LOG,"delete %s",command);

                system(command);
                delete_segment_no++;
            }
        }

        /* archive xlog file */
        if(archive_mode == false || Recovery_IsStandby() == true)
        {
            pg_usleep(GTMArchiverCheckInterval);
            continue;
        }

        if(GetCurrentTimeLineID() == timeLine && segment_no == GetCurrentSegmentNo())
        {
            pg_usleep(GTMArchiverCheckInterval);
            continue;
        }

        GTMXLogFileName(file_name,timeLine,segment_no);
        GTMXLogFileNameWithoutGtmDir(file_name_no_dirname,timeLine,segment_no);
        GTMXLogFileStatusReadyName(file_xlog_status,timeLine,segment_no);

        if(IsXLogFileExist(file_xlog_status) == false)
        {
            segment_no++;
            continue;
        }

        GetFormatedCommandLine(command,MAX_COMMAND_LEN,archive_command,file_name_no_dirname,file_name);

        ret = system(command);

        if(enalbe_gtm_xlog_debug)
            elog(LOG,"%s",command);

        if(ret != -1 && WIFEXITED(ret) && WEXITSTATUS(ret) == 0)
        {
            char done_file_name[MAXFNAMELEN];

            GTMXLogFileStatusDoneName(done_file_name,timeLine,segment_no);
            rename(file_xlog_status,done_file_name);
            segment_no++;
        }
        else
        {
            elog(LOG, "archive command %s fails,retry", command);
            sleep(20);
        }

        pg_usleep(GTMArchiverCheckInterval);
    }

    g_archiver_thread = NULL;
    elog(LOG, "GTM is shutting down, archiver exits!");
    return my_threadinfo;
}

void *
GTM_ThreadWalRedoer(void *argp)
{
    GTM_ThreadInfo *my_threadinfo = (GTM_ThreadInfo *)argp;
    sigjmp_buf  local_sigjmp_buf;
    int ret;
    struct sigaction    action;  

    action.sa_flags = 0;
    action.sa_handler = GTM_ThreadSigHandler;  
	sigemptyset(&action.sa_mask);
    ret = sigaction(SIGQUIT, &action, NULL);  
    
    if (ret)
    {
        elog(LOG, "register thread quit handler failed");
    }
    
    MessageContext = AllocSetContextCreate(TopMemoryContext,
                                       "MessageContext",
                                       ALLOCSET_DEFAULT_MINSIZE,
                                       ALLOCSET_DEFAULT_INITSIZE,
                                       ALLOCSET_DEFAULT_MAXSIZE,
                                       false);
                                       
    if (sigsetjmp(local_sigjmp_buf, 1) != 0)
    {
        RWLockCleanUp();
        EmitErrorReport(NULL);
        MemoryContextSwitchTo(TopMemoryContext);
        FlushErrorState();
    }
    
    PG_exception_stack = &local_sigjmp_buf;

    GTM_ThreadWalRedoer_Internal();

    g_redoer_thread = NULL;
    elog(LOG, "GTM is shutting down, redoer exits!");
    return my_threadinfo;    
}

static void WaitRedoertoExit(void)
{
    while(g_redoer_thread)
    {
        pg_usleep(1000);
    }
}

static long long getSystemTime() 
{
    struct timeb t;
    ftime(&t);
    return 1000 * t.time + t.millitm;
}

void *
GTM_ThreadWalReceiver(void *argp)
{// #lizard forgives
    GTM_ThreadInfo *my_threadinfo = (GTM_ThreadInfo *)argp;
    sigjmp_buf  local_sigjmp_buf;
    int ret;
    struct sigaction    action;  
    GTM_Result     *res = NULL;

    int                efd  = -1;
    struct epoll_event event;

    XLogRecPtr    start_pos = InvalidXLogRecPtr;
    XLogRecPtr    end_pos   = InvalidXLogRecPtr;
    XLogRecPtr    flush_pos = InvalidXLogRecPtr;
    int           size      = 0;
    int           n         = 0;
    time_t        next_send_time = 0;
    long long     t_start,t_end;
    
    action.sa_flags = 0;  
    action.sa_handler = GTM_ThreadSigHandler;  
	sigemptyset(&action.sa_mask);

    ret = sigaction(SIGQUIT, &action, NULL);

    if (ret)
    {
        elog(LOG, "register thread quit handler failed");
    }

    MessageContext = AllocSetContextCreate(TopMemoryContext,
                                       "MessageContext",
                                       ALLOCSET_DEFAULT_MINSIZE,
                                       ALLOCSET_DEFAULT_INITSIZE,
                                       ALLOCSET_DEFAULT_MAXSIZE,
                                       false);


    efd  = epoll_create1(0);
    if(efd == -1)
    {
        elog(LOG,"epoll create fail %s",strerror(errno));
        exit(1);
    }

    my_threadinfo->thr_efd      = efd;

    if (sigsetjmp(local_sigjmp_buf, 1) != 0)
    {
        RWLockCleanUp();
        EmitErrorReport(NULL);
        MemoryContextSwitchTo(TopMemoryContext);
        FlushErrorState();
    }
    
    PG_exception_stack = &local_sigjmp_buf;

reconnect:

    if(GTM_SHUTTING_DOWN == GTMTransactions.gt_gtm_state)
        goto shutdown;

    if(!Recovery_IsStandby())
        goto promote;

    if(GTM_ActiveConn)
    {
        GTMPQfinish(GTM_ActiveConn);
        GTM_ActiveConn = NULL;
    }

    sleep(1);

	if (!gtm_standby_start_startup(0))
    {
        elog(ERROR, "Failed to establish a connection to active-GTM.");
    }

    if (GTM_ActiveConn == NULL || GTMPQstatus(GTM_ActiveConn) != CONNECTION_OK ||
        gtm_standby_register_self(NULL,0,NULL)  == 0 ||
        gtm_standby_start_replication(application_name) != 0)
    {
        elog(LOG,"gtm connection fails");
        goto reconnect;
    }

    event.data.fd  = GTM_ActiveConn->sock;
    event.events   = EPOLLIN | EPOLLHUP | EPOLLERR;

    if(epoll_ctl(efd,EPOLL_CTL_ADD,GTM_ActiveConn->sock,&event) < 0)
    {
        elog(LOG, "epoll fails %s", strerror(errno));
        goto reconnect;
    }

    next_send_time = time(NULL);

    for(;;)
    {
        t_start = getSystemTime();
        MemoryContextSwitchTo(MessageContext);
        MemoryContextResetAndDeleteChildren(MessageContext);

        if(time(NULL) >= next_send_time)
        {
            SendXLogSyncStatus(GTM_ActiveConn);
            next_send_time = time(NULL) + XLOG_KEEP_ALIVE_TIME;
        }

        if(!Recovery_IsStandby())
            goto promote;

        if(gtmpqHasDataLeft(GTM_ActiveConn) == false)
        {
            n = epoll_wait(efd,&event,1,100);

            if(GTM_SHUTTING_DOWN == GTMTransactions.gt_gtm_state || Recovery_IsStandby() == false)
                break;

            if(n == 0)
            {
                if(enalbe_gtm_xlog_debug)
                    elog(LOG,"no data to read");
                continue;
            }

            if(!(event.events & EPOLLIN))
            {
                elog(LOG,"replication terminated.");
                goto reconnect;
            }

            if(gtmpqReadData(GTM_ActiveConn) < 0)
            {
                elog(LOG,"replication terminated.");
                goto reconnect;
            }
        }

        if((res = GTMPQgetResult(GTM_ActiveConn)) == NULL)
        {
            GTM_ActiveConn->result->gr_status = GTM_RESULT_COMM_ERROR;
            elog(LOG,"replication terminated.");
            goto reconnect;
        }

        Assert(res->gr_status == GTM_RESULT_OK);
        Assert(res->gr_type   == MSG_REPLICATION_CONTENT);
            
        if (res->gr_resdata.grd_xlog_data.status != Send_OK)
        {
            Assert(res->gr_resdata.grd_xlog_data.status == Send_XlogFile_Not_Found);
            elog(LOG,"xlog file not found in master, exit now");
            exit(1);
        }

        Assert(res->gr_resdata.grd_xlog_data.status == Send_OK);
        size      = res->gr_resdata.grd_xlog_data.length;
        start_pos = res->gr_resdata.grd_xlog_data.pos;
        end_pos   = start_pos + size;

        if(enalbe_gtm_xlog_debug)
            elog(LOG,"receive xlog from %X/%X to %X/%X %d",
                 (uint32)(start_pos>>32),
                 (uint32)start_pos,
                 (uint32)(end_pos>>32),
                 (uint32)end_pos,
                 size);

        /* write data to xlog buff */
        if(size != 0)
        {
            if(XLogInCurrentSegment(start_pos) == false)
            {
                if(start_pos % GTM_XLOG_SEG_SIZE != 0)
                    elog(LOG,"invalid switch from remote");

                XLogFlush(GetStandbyWriteBuffPos());
                SwitchXLogFile();
            }

            CopyXLogRecordToBuff(res->gr_resdata.grd_xlog_data.xlog_data,start_pos,end_pos,(uint64)size);
            NotifyReplication(end_pos);
            UpdateStandbyWriteBuffPos(end_pos);
			XLogFlush(end_pos);
        }

        if(res->gr_resdata.grd_xlog_data.reply)
        {
            WaitSyncComplete(flush_pos);
            SendXLogSyncStatus(GTM_ActiveConn);
        }

        t_end = getSystemTime();

        if(enalbe_gtm_xlog_debug)
            elog(LOG,"cost time %lld ms",t_end - t_start);
    }

promote:
    if(Recovery_IsStandby() == false)
    {
        gtm_standby_finish_startup();
        elog(LOG, "Promoting slave to master,walreceiver exits");
        return my_threadinfo;
    }

shutdown:
    g_walreceiver_thread = NULL;
    elog(LOG, "GTM is shutting down, walreceiver exits!");
    return my_threadinfo; 
}

#endif

/* main thread handle SIGQUIT as the thread exit signal. */
void *
GTM_ThreadMain(void *argp)
{// #lizard forgives
    GTM_ThreadInfo *thrinfo = (GTM_ThreadInfo *)argp;
    int        ret  = 0;
    int     qtype;
    StringInfoData input_message;
    sigjmp_buf  local_sigjmp_buf;
    int         efd;
     struct epoll_event events[GTM_MAX_CONNECTIONS_PER_THREAD];
    struct sigaction    action;  
       
    action.sa_flags = 0;  
    action.sa_handler = GTM_ThreadSigHandler;  
	sigemptyset(&action.sa_mask);
         
    ret = sigaction(SIGQUIT, &action, NULL);  
	if (ret)
	{
		elog(LOG, "register thread quit handler failed");
	}

	elog(DEBUG8, "Starting the connection helper thread");
	bind_service_threads();

    GTM_InitStatisticsHandle();

    /*
     * Create the memory context we will use in the main loop.
     *
     * MessageContext is reset once per iteration of the main loop, ie, upon
     * completion of processing of each command message from the client.
     *
     * This context is thread-specific
     */
	MessageContext = AllocSetContextCreate(TopMemoryContext,
										   "MessageContext",
										   ALLOCSET_DEFAULT_MINSIZE,
										   ALLOCSET_DEFAULT_INITSIZE,
										   ALLOCSET_DEFAULT_MAXSIZE,
										   false);
	
	efd = epoll_create1(0);
	if(efd == -1)
	{
		elog(ERROR, "failed to create epoll");
	}
	thrinfo->thr_efd = efd;
	thrinfo->thr_epoll_ok = true;
	
	/*
	 * Acquire the thread lock to prevent connection from GTM-Standby to update
	 * GTM-Standby registration.
	 */

	/*
	 * Get the input_message in the TopMemoryContext so that we don't need to
	 * free/palloc it for every incoming message. Unlike Postgres, we don't
	 * expect the incoming messages to be of arbitrary sizes
	 */

	initStringInfo(&input_message);

	/*
	 * POSTGRES main processing loop begins here
	 *
	 * If an exception is encountered, processing resumes here so we abort the
	 * current transaction and start a new one.
	 *
	 * You might wonder why this isn't coded as an infinite loop around a
	 * PG_TRY construct.  The reason is that this is the bottom of the
	 * exception stack, and so with PG_TRY there would be no exception handler
	 * in force at all during the CATCH part.  By leaving the outermost setjmp
	 * always active, we have at least some chance of recovering from an error
	 * during error recovery.  (If we get into an infinite loop thereby, it
	 * will soon be stopped by overflow of elog.c's internal state stack.)
	 */

	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		bool	report = false;
#ifdef __TBASE__
        RWLockCleanUp();
#endif
        /*
         * NOTE: if you are tempted to add more code in this if-block,
         * consider the high probability that it should be in
         * AbortTransaction() instead.    The only stuff done directly here
         * should be stuff that is guaranteed to apply *only* for outer-level
         * error recovery, such as adjusting the FE/BE protocol status.
         */

        /* Report the error to the client and/or server log */
        if(!report)
        {
            report = true;
            if(thrinfo->thr_conn)
            {
                EmitErrorReport(thrinfo->thr_conn->con_port);
            }
            else
            {
                EmitErrorReport(NULL);
            }
        }

        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        MemoryContextSwitchTo(TopMemoryContext);
        FlushErrorState();
    }

    /* We can now handle ereport(ERROR) */
    PG_exception_stack = &local_sigjmp_buf;

    for (;;)
    {
        int         i, n;

        elog(DEBUG8, "for loop");        
        
        /* Put all queued connections to local connection array */
        elog(DEBUG8, "get new conns");

        /* Wait for available event */
        n = epoll_wait (efd, events, GTM_MAX_CONNECTIONS_PER_THREAD, -1);

        elog(DEBUG8, "epoll_wait wakeup %d", n);
        
        for(i = 0; i < n; i++)
        {
            GTM_ConnectionInfo *conn;

            thrinfo->thr_conn = NULL;
            /*
             * Just reset the input buffer to avoid repeated palloc/pfrees
             *
             * XXX We should consider resetting the MessageContext periodically to
             * handle any memory leaks
             */
            MemoryContextSwitchTo(MessageContext);
            MemoryContextResetAndDeleteChildren(MessageContext);
            resetStringInfo(&input_message);
            
            if(!(events[i].events & EPOLLIN))
            {
                elog(DEBUG8, "no read data");
                continue;
            }
            
            conn = events[i].data.ptr;
            elog(DEBUG8, "read command");
            thrinfo->thr_conn = conn;

            if(conn->con_port == NULL)
                continue;

            if(false == conn->con_init)
            {
                if(GTMInitConnection(conn) != STATUS_OK)
                {
                    elog(LOG, "initiating connection failed");
                    GTM_RemoveConnection(conn);    
                    thrinfo->thr_conn = NULL;
                    continue;
                }

                continue;
            }

            /*
             * (3) read a command (loop blocks here)
             */
            qtype = ReadCommand(conn->con_port, &input_message);
            elog(DEBUG8, "read command qtype %c", qtype);
            
            /*
             * Check if GTM Standby info is upadted
             * Maybe the following lines can be a separate function.   At present, this is done only here so
             * I'll leave them here.   K.Suzuki, Nov.29, 2011
             * Please note that we don't check if it is not in the standby mode to allow cascased standby.
             *
             * Also ensure that we don't try to connect just yet if we are
             * responsible for serving the BACKUP request from the standby.
             * Otherwise, this will lead to a deadlock
             */
            elog(DEBUG8, "standby checked %c", qtype);
            switch(qtype)
            {
                case 'C':
                    ProcessCommand(conn->con_port, &input_message);
                    elog(DEBUG8, "complete command %c", qtype);
                    break;

                case 'X':
                    elog(DEBUG8, "Removing all transaction infos - qtype:X");
                    
                case EOF:
                    /*
                     * Connection termination request
                     * Remove all transactions opened within the thread. Note that
                     * we don't remove transaction infos if we are a standby and
                     * the transaction infos actually correspond to in-progress
                     * transactions on the master
                     */
                    elog(DEBUG8, "Removing all transaction infos - qtype:EOF");
                    if (!Recovery_IsStandby())
                        GTM_RemoveAllTransInfos(conn->con_client_id, -1);

                    /* Disconnect node if necessary */                    
                    GTM_RemoveConnection(conn);
                    break;

                case 'F':
                    elog(DEBUG8, "Flush");
                    /*
                     * Flush all the outgoing data on the wire. Consume the message
                     * type field for sanity
                     */
                    /* Sync with standby first */
#ifndef __XLOG__
                    if (conn->standby)
                    {
                        if (Backup_synchronously)
                            gtm_sync_standby(conn->standby);
                        else
                            gtmpqFlush(conn->standby);
                    }
                    pq_getmsgint(&input_message, sizeof (GTM_MessageType));
                    pq_getmsgend(&input_message);
                    pq_flush(conn->con_port);
#endif
                    break;

                default:
                    elog(DEBUG8, "Remove transactions");
                    /*
                     * Remove all transactions opened by the client
                     */
                    GTM_RemoveAllTransInfos(conn->con_client_id, -1);

                    /* Disconnect node if necessary */
                    GTM_RemoveConnection(conn);
                    ereport(FATAL,
                            (EPROTO,
                             errmsg("invalid frontend message type %d",
                                    qtype)));
                    break;
            }

            /* no need to lock here. */
            if(GTM_SHUTTING_DOWN == GTMTransactions.gt_gtm_state)
            {
                break;
            }
        }
    }

    return thrinfo;
}


void *
GTM_ThreadBasebackup(void *argp)
{// #lizard forgives
       GTM_ThreadInfo *thrinfo = (GTM_ThreadInfo *)argp;
    int        ret  = 0;
    int     qtype;
    StringInfoData input_message;
    sigjmp_buf  local_sigjmp_buf;
    int         efd;
     struct epoll_event events[GTM_MAX_CONNECTIONS_PER_THREAD];
    struct sigaction    action;

    action.sa_flags = 0;
    action.sa_handler = GTM_ThreadSigHandler;
	sigemptyset(&action.sa_mask);

    ret = sigaction(SIGQUIT, &action, NULL);
    if (ret)
    {
        elog(LOG, "register thread quit handler failed");
    }

    /*
     * Create the memory context we will use in the main loop.
     *
     * MessageContext is reset once per iteration of the main loop, ie, upon
     * completion of processing of each command message from the client.
     *
     * This context is thread-specific
     */
    MessageContext = AllocSetContextCreate(TopMemoryContext,
                                           "MessageContext",
                                           ALLOCSET_DEFAULT_MINSIZE,
                                           ALLOCSET_DEFAULT_INITSIZE,
                                           ALLOCSET_DEFAULT_MAXSIZE,
                                           false);

    efd = epoll_create1(0);
    if(efd == -1)
    {
        elog(ERROR, "failed to create epoll");
    }
    thrinfo->thr_efd = efd;
    /* not allow outer connections */
    thrinfo->thr_epoll_ok = false;

    initStringInfo(&input_message);

    /*
     * If an exception is encountered, processing resumes here so we abort the
     * current transaction and start a new one.
     *
     * You might wonder why this isn't coded as an infinite loop around a
     * PG_TRY construct.  The reason is that this is the bottom of the
     * exception stack, and so with PG_TRY there would be no exception handler
     * in force at all during the CATCH part.  By leaving the outermost setjmp
     * always active, we have at least some chance of recovering from an error
     * during error recovery.  (If we get into an infinite loop thereby, it
     * will soon be stopped by overflow of elog.c's internal state stack.)
     */

    if (sigsetjmp(local_sigjmp_buf, 1) != 0)
    {
        bool    report = false;
#ifdef __TBASE__
        RWLockCleanUp();
#endif
        /*
         * NOTE: if you are tempted to add more code in this if-block,
         * consider the high probability that it should be in
         * AbortTransaction() instead.    The only stuff done directly here
         * should be stuff that is guaranteed to apply *only* for outer-level
         * error recovery, such as adjusting the FE/BE protocol status.
         */

        /* Report the error to the client and/or server log */
        if(!report)
        {
            report = true;
            if(thrinfo->thr_conn)
            {
                EmitErrorReport(thrinfo->thr_conn->con_port);
            }
            else
            {
                EmitErrorReport(NULL);
            }
        }

        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        MemoryContextSwitchTo(TopMemoryContext);
        FlushErrorState();
    }

    /* We can now handle ereport(ERROR) */
    PG_exception_stack = &local_sigjmp_buf;

    for (;;)
    {
        int         i, n;

        /* Put all queued connections to local connection array */

        /* Wait for available event */
        n = epoll_wait (efd, events, GTM_MAX_CONNECTIONS_PER_THREAD, -1);

        for(i = 0; i < n; i++)
        {
            GTM_ConnectionInfo *conn;

            thrinfo->thr_conn = NULL;
            /*
             * Just reset the input buffer to avoid repeated palloc/pfrees
             *
             * XXX We should consider resetting the MessageContext periodically to
             * handle any memory leaks
             */
            MemoryContextSwitchTo(MessageContext);
            MemoryContextResetAndDeleteChildren(MessageContext);
            resetStringInfo(&input_message);

            if(!(events[i].events & EPOLLIN))
            {
                elog(DEBUG8, "no read data");
                continue;
            }

            conn = events[i].data.ptr;
            elog(DEBUG8, "read command");
            thrinfo->thr_conn = conn;

            if(conn->con_port == NULL)
                continue;

            /*
             * (3) read a command (loop blocks here)
             */
            qtype = ReadCommand(conn->con_port, &input_message);
            elog(DEBUG8, "read command qtype %c", qtype);

            /*
             * Check if GTM Standby info is upadted
             * Maybe the following lines can be a separate function.   At present, this is done only here so
             * I'll leave them here.   K.Suzuki, Nov.29, 2011
             * Please note that we don't check if it is not in the standby mode to allow cascased standby.
             *
             * Also ensure that we don't try to connect just yet if we are
             * responsible for serving the BACKUP request from the standby.
             * Otherwise, this will lead to a deadlock
             */
            elog(DEBUG8, "standby checked %c", qtype);
            switch(qtype)
            {
                case 'C':
                    ProcessBasebackupCommand(conn->con_port, &input_message);
                    elog(DEBUG8, "complete command %c", qtype);
                    break;

                case EOF:
                    /* Disconnect node if necessary */
                    GTM_RemoveConnection(conn);
                    break;

                default:
                    /* Disconnect node if necessary */
                    GTM_RemoveConnection(conn);
                    elog(LOG,"invalid message");
            }
            /* no need to lock here. */
            if(GTM_SHUTTING_DOWN == GTMTransactions.gt_gtm_state)
            {
                break;
            }
        }
    }

    return thrinfo;
}

void
ProcessBasebackupCommand(Port *myport, StringInfo input_message)
{
    GTM_MessageType    mtype;

    myport->conn_id = InvalidGTMProxyConnID;
    mtype = pq_getmsgint(input_message, sizeof (GTM_MessageType));

    elog(LOG, "mtype = %s (%d).", gtm_util_message_name(mtype), (int)mtype);

    switch (mtype)
    {
        case MSG_END_BACKUP:
            ProcessGTMEndBackup(myport, input_message);
            break;
        case MSG_GET_STORAGE:
            ProcessStorageTransferCommand(myport, input_message);
            break;
        case MSG_NODE_REGISTER:
        case MSG_NODE_UNREGISTER:
        case MSG_NODE_LIST:
        case MSG_REGISTER_SESSION:
            ProcessPGXCNodeCommand(myport, mtype, input_message);
            break;
        case MSG_TXN_GET_NEXT_GXID:
            ProcessGetNextGXIDTransactionCommand(myport, input_message);
            break;
        default:
            ereport(ERROR,
                    (EPROTO,
                            errmsg("invalid frontend message type %d",
                                   mtype)));
    }

}
void
ProcessCommand(Port *myport, StringInfo input_message)
{// #lizard forgives
    GTM_MessageType    mtype;
    GTM_ProxyMsgHeader proxyhdr;

#ifdef __TBASE__
    GTM_ThreadInfo *my_threadinfo = NULL;
    long long  start_time;
    long long  cost_time;
    my_threadinfo = GetMyThreadInfo;
#ifndef __XLOG__
    GTM_ConnectionInfo *conn;
    conn = my_threadinfo->thr_conn;
#endif
#endif

    if (myport->remote_type == GTM_NODE_GTM_PROXY)
    {
        pq_copymsgbytes(input_message, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
    }
    else
    {
        proxyhdr.ph_conid = InvalidGTMProxyConnID;
    }

    myport->conn_id = proxyhdr.ph_conid;
    mtype = pq_getmsgint(input_message, sizeof (GTM_MessageType));

    /*
     * The next line will have some overhead.  Better to be in
     * compile option.
     */
    elog(DEBUG1, "mtype = %s (%d).", gtm_util_message_name(mtype), (int)mtype);

#ifdef __TBASE__
    if (promote_status != GTM_PRPMOTE_NORMAL)
    {
        if (mtype != MSG_CHECK_GTM_STATUS)
        {
            elog(ERROR, "standby gtm only support get gtm status command before establish a connection to active-GTM or promote, mtype = %s (%d).", gtm_util_message_name(mtype), (int)mtype);
        }

        return ProcessStandbyPreCheckGTMCommand(myport, input_message);
    }

    start_time = getSystemTime();
    /*
     * Get Timestamp does not need to sync with standby
     */
    GetMyThreadInfo->handle_standby = (mtype != MSG_GETGTS &&
                      mtype != MSG_GETGTS_MULTI &&
                      mtype != MSG_BEGIN_BACKUP &&
#ifndef __XLOG__
                      mtype != MSG_END_BACKUP &&
#endif
                      mtype != MSG_LIST_GTM_STORE &&
                      mtype != MSG_LIST_GTM_STORE_SEQ &&
                      mtype != MSG_LIST_GTM_STORE_TXN &&
                      mtype != MSG_CHECK_GTM_STORE_SEQ &&
                      mtype != MSG_CHECK_GTM_STORE_TXN &&
                      mtype != MSG_CHECK_GTM_STATUS
    );

    if(my_threadinfo->handle_standby)
    {
#ifndef __XLOG__
        /* Handle standby connecion staff. */
        CheckStandbyConnect(my_threadinfo, conn);
#endif
        /* Hold the lock, in case reset all standby connections. */
        GTM_RWLockAcquire(&my_threadinfo->thr_lock, GTM_LOCKMODE_WRITE);
    }

    XLogBeginInsert();
    my_threadinfo->xlog_inserting = true;
    my_threadinfo->current_write_number = 0;

#endif
    switch (mtype)
    {
#ifndef __XLOG__
        case MSG_SYNC_STANDBY:
            ProcessSyncStandbyCommand(myport, mtype, input_message);
            break;
#endif
        case MSG_NODE_REGISTER:
        case MSG_BKUP_NODE_REGISTER:
        case MSG_NODE_UNREGISTER:
        case MSG_BKUP_NODE_UNREGISTER:
        case MSG_NODE_LIST:
        case MSG_REGISTER_SESSION:
            ProcessPGXCNodeCommand(myport, mtype, input_message);
            break;
        case MSG_BEGIN_BACKUP:
            ProcessGTMBeginBackup(myport, input_message);
            break;
#ifndef __XLOG__
        case MSG_END_BACKUP:
            ProcessGTMEndBackup(myport, input_message);
            break;
#endif
        case MSG_NODE_BEGIN_REPLICATION_INIT:
        case MSG_NODE_END_REPLICATION_INIT:
        case MSG_TXN_BEGIN:
        case MSG_BKUP_TXN_BEGIN:
        case MSG_BKUP_GLOBAL_TIMESTAMP:
        case MSG_TXN_BEGIN_GETGXID:
        case MSG_BKUP_TXN_BEGIN_GETGXID:
        case MSG_TXN_BEGIN_GETGXID_AUTOVACUUM:
        case MSG_BKUP_TXN_BEGIN_GETGXID_AUTOVACUUM:
        case MSG_TXN_PREPARE:
        case MSG_BKUP_TXN_PREPARE:
        case MSG_TXN_START_PREPARED:
        case MSG_BKUP_TXN_START_PREPARED:
        case MSG_TXN_COMMIT:
        case MSG_BKUP_TXN_COMMIT:
        case MSG_TXN_COMMIT_PREPARED:
        case MSG_BKUP_TXN_COMMIT_PREPARED:
        case MSG_TXN_ROLLBACK:
        case MSG_BKUP_TXN_ROLLBACK:
        case MSG_TXN_GET_GXID:
        case MSG_BKUP_TXN_GET_GXID:
        case MSG_TXN_BEGIN_GETGXID_MULTI:
        case MSG_BKUP_TXN_BEGIN_GETGXID_MULTI:
        case MSG_TXN_COMMIT_MULTI:
        case MSG_BKUP_TXN_COMMIT_MULTI:
        case MSG_TXN_ROLLBACK_MULTI:
        case MSG_BKUP_TXN_ROLLBACK_MULTI:
        case MSG_TXN_GET_GID_DATA:
        case MSG_TXN_GET_NEXT_GXID:
        case MSG_TXN_GXID_LIST:
#ifdef XCP
            case MSG_REPORT_XMIN:
        case MSG_BKUP_REPORT_XMIN:
#endif
#ifdef __TBASE__
        case MSG_TXN_FINISH_GID:
        case MSG_TXN_LOG_COMMIT:
        case MSG_TXN_LOG_GLOBAL_COMMIT:
        case MSG_TXN_LOG_SCAN:
        case MSG_TXN_LOG_GLOBAL_SCAN:
        case MSG_GETGTS:
        case MSG_GETGTS_MULTI:
        case MSG_CHECK_GTM_STATUS:
#endif

            ProcessTransactionCommand(myport, mtype, input_message);
            break;

        case MSG_SNAPSHOT_GET:
        case MSG_SNAPSHOT_GXID_GET:
        case MSG_SNAPSHOT_GET_MULTI:
            ProcessSnapshotCommand(myport, mtype, input_message);
            break;

        case MSG_SEQUENCE_INIT:
        case MSG_BKUP_SEQUENCE_INIT:
        case MSG_SEQUENCE_GET_CURRENT:
        case MSG_SEQUENCE_GET_NEXT:
        case MSG_BKUP_SEQUENCE_GET_NEXT:
        case MSG_SEQUENCE_GET_LAST:
        case MSG_SEQUENCE_SET_VAL:
        case MSG_BKUP_SEQUENCE_SET_VAL:
        case MSG_SEQUENCE_RESET:
        case MSG_BKUP_SEQUENCE_RESET:
        case MSG_SEQUENCE_CLOSE:
        case MSG_BKUP_SEQUENCE_CLOSE:
        case MSG_SEQUENCE_RENAME:
        case MSG_BKUP_SEQUENCE_RENAME:
        case MSG_SEQUENCE_ALTER:
        case MSG_BKUP_SEQUENCE_ALTER:
        case MSG_SEQUENCE_LIST:
        case MSG_CLEAN_SESSION_SEQ:
#ifdef __TBASE__
        case MSG_DB_SEQUENCE_RENAME:
        case MSG_BKUP_DB_SEQUENCE_RENAME:    
		case MSG_SEQUENCE_COPY:
#endif
            ProcessSequenceCommand(myport, mtype, input_message);
            break;

        case MSG_TXN_GET_STATUS:
        case MSG_TXN_GET_ALL_PREPARED:
            ProcessQueryCommand(myport, mtype, input_message);
            break;

        case MSG_BARRIER:
        case MSG_BKUP_BARRIER:
            ProcessBarrierCommand(myport, mtype, input_message);
            break;

        case MSG_BACKEND_DISCONNECT:
            elog(DEBUG1, "MSG_BACKEND_DISCONNECT received - removing all txn infos");
            GTM_RemoveAllTransInfos(GetMyConnection(myport)->con_client_id, proxyhdr.ph_conid);
            /* Mark PGXC Node as disconnected if backend disconnected is postmaster */
            ProcessPGXCNodeBackendDisconnect(myport, input_message);
            break;
#ifdef __TBASE__
#ifndef __XLOG__
        case MSG_GET_STORAGE:
        {
            /* process storage file transfer request */
            ProcessStorageTransferCommand(myport, input_message);
            break;
        }
#endif

        case MSG_LIST_GTM_STORE:
        {
            ProcessGetGTMHeaderCommand(myport, input_message);
            break;
        }
        case MSG_LIST_GTM_STORE_SEQ:            /* List  gtm running sequence info */
        {
            ProcessListStorageSequenceCommand(myport, input_message);
            break;
        }
        case MSG_LIST_GTM_STORE_TXN:            /* List  gtm running transaction info */
        {
            ProcessListStorageTransactionCommand(myport, input_message);
            break;
        }
        case MSG_CHECK_GTM_STORE_SEQ:            /* Check gtm sequence usage info */
        {
            ProcessCheckStorageSequenceCommand(myport, input_message);
            break;
        }
        case MSG_CHECK_GTM_STORE_TXN:            /* Check gtm transaction usage info */
        {
            ProcessCheckStorageTransactionCommand(myport, input_message);
            break;
        }
#ifdef __XLOG__
        case MSG_START_REPLICATION:
        {
            ProcessStartReplicationCommand(myport,input_message);
            break;
        }
#endif
        case MSG_GET_STATISTICS:
        {
            ProcessGetStatisticsCommand(myport,input_message);
            break;
        }
        case MSG_GET_ERRORLOG:
        {
            ProcessGetErrorlogCommand(myport,input_message);
            break;
        }
#endif
        default:
            ereport(FATAL,
                    (EPROTO,
                            errmsg("invalid frontend message type %d",
                                   mtype)));
    }

    BeforeReplyToClientXLogTrigger();

#ifdef __TBASE__
    cost_time = getSystemTime() - start_time;
    if(enable_gtm_debug || cost_time > warnning_time_cost)
	    elog(LOG, "cost mtype = %s (%d) %lld ms.", gtm_util_message_name(mtype), (int)mtype,cost_time);

    GTM_UpdateStatistics(my_threadinfo->stat_handle, mtype, cost_time);

    if (my_threadinfo->handle_standby)
    {
        GTM_RWLockRelease(&my_threadinfo->thr_lock);
    }
#endif

#ifndef __TBASE__
    if (GTM_NeedBackup())
    {
        GTM_WriteRestorePoint();
    }
#endif
}

static int
GTMInitConnection(GTM_ConnectionInfo *conninfo)
{// #lizard forgives
    int ret = STATUS_OK;

    conninfo->con_init = true;

    elog(DEBUG8, "Init connection");
    {
        /*
         * We expect a startup message at the very start. The message type is
         * REGISTER_COORD, followed by the 4 byte Coordinator ID
         */
        char startup_type;
        GTM_StartupPacket sp;
        StringInfoData inBuf;

        startup_type = pq_getbyte(conninfo->con_port);

        if (startup_type != 'A')
        {
            elog(LOG, "Expecting a startup message, but received %c, %d",
                 startup_type, (int)startup_type);
            return STATUS_ERROR;
        }
        initStringInfo(&inBuf);

        /*
         * All frontend messages have a length word next
         * after the type code; we can read the message contents independently of
         * the type.
         */
        if (pq_getmessage(conninfo->con_port, &inBuf, 0))
        {
            elog(LOG, "Expecting coordinator ID, but received EOF");
            ret = STATUS_ERROR;

        }
        memcpy(&sp,
               pq_getmsgbytes(&inBuf, sizeof (GTM_StartupPacket)),
               sizeof (GTM_StartupPacket));
        pq_getmsgend(&inBuf);

        GTM_RegisterPGXCNode(conninfo->con_port, sp.sp_node_name);

        conninfo->con_port->remote_type = sp.sp_remotetype;
        conninfo->con_port->is_postmaster = sp.sp_ispostmaster;
        conninfo->con_port->remote_backend_pid = sp.sp_backend_pid;
        conninfo->con_client_id = 0;
        /*
         * If the client has resent the identifier assigned to it previously
         * (by GTM master), use that identifier.
         *
         * We only accept identifiers which are lesser or equal to the last
         * identifier we had seen when we were promoted. All other identifiers
         * will be overwritten by what we have assigned
         */
        if ((sp.sp_client_id != 0) &&
            (sp.sp_client_id <= GTMThreads->gt_starting_client_id))
        {
            conninfo->con_client_id = sp.sp_client_id;
        }
        pfree(inBuf.data);
    }

    if(Recovery_IsStandby() && 
        conninfo->con_port->remote_type != GTM_NODE_GTM && 
        conninfo->con_port->remote_type != GTM_NODE_GTM_CTL)
    {
        elog(LOG, "Standby is not allowed to connect");
        ret = STATUS_ERROR; 
    }
    else
    {
        /*
         * Send a dummy authentication request message 'R' as the client
         * expects that in the current protocol. Also send the client
         * identifier issued by us (or sent by the client in the startup packet
         * if we concluded to use the same)
         */
        StringInfoData buf;
        pq_beginmessage(&buf, 'R');
        pq_sendint(&buf, conninfo->con_client_id, 4);
        pq_endmessage(conninfo->con_port, &buf);
        pq_flush(conninfo->con_port);

        elog(DEBUG8, "Sent connection authentication message to the client");
    }


    return ret;

}

static void
SetNonBlockConnection(GTM_ConnectionInfo *conninfo)
{
    int flags;

    if((flags = fcntl(conninfo->con_port->sock, F_GETFL, 0))== -1)
    {
        ereport(LOG,
                (EPROTO,
                        errmsg("fcntl get errors %s sock %d", strerror(errno),
                               conninfo->con_port->sock)));
        return;
    }

    if(fcntl(conninfo->con_port->sock, F_SETFL, flags | O_NONBLOCK) == -1)
    {
        ereport(LOG,
                (EPROTO,
                        errmsg("fcntl set errors %s", strerror(errno))));
        return;
    }

    conninfo->con_port->is_nonblocking = true;
}

static int
GTMAddConnection(Port *port, GTM_Conn *standby)
{// #lizard forgives
    GTM_ConnectionInfo *conninfo = NULL;
    int             i;
    GTM_ThreadInfo        *thrinfo;
    struct epoll_event event;


    conninfo = (GTM_ConnectionInfo *)palloc0(sizeof (GTM_ConnectionInfo));
    elog(DEBUG8, "Started new connection");
    conninfo->con_port = port;
    conninfo->con_init = false;
    port->conn = conninfo;

#ifndef __XLOG__
    /*
     * Add a connection to the standby.
     */
    if (standby != NULL)
        conninfo->standby = standby;
#endif

    /* Set conn to non-blocking mode for epoll wait */
    SetNonBlockConnection(conninfo);

    elog(DEBUG8, "gt thread count %d", GTMThreads->gt_thread_count);
    for(;;)
    {
        /*
         * Check whether there are enough process threads to handle connections.
         * If not, just create new threads.
         */
        GTM_RWLockAcquire(&GTMThreads->gt_lock, GTM_LOCKMODE_READ);

        if(GTMThreads->gt_block_new_connection == false)
        {
            while(GTMThreads->gt_thread_count < GTMThreads->gt_start_thread_count)
            {
                elog(LOG, "create threads on demand thread count %d start thread count %d",
                     GTMThreads->gt_thread_count,GTMThreads->gt_start_thread_count);

                GTM_RWLockRelease(&GTMThreads->gt_lock);

                if (NULL == GTM_ThreadCreate(GTM_ThreadMain, g_max_lock_number))
                {
                    elog(WARNING, "Failed to create gtm thread.");
	                GTM_RWLockAcquire(&GTMThreads->gt_lock, GTM_LOCKMODE_READ);
                    break;
                }
                GTM_RWLockAcquire(&GTMThreads->gt_lock, GTM_LOCKMODE_READ);
            }
        }
        GTM_RWLockRelease(&GTMThreads->gt_lock);

        i = (GTMThreads->gt_next_thread++) % GTMThreads->gt_start_thread_count;

        thrinfo = GTMThreads->gt_threads[i];
        if(NULL == thrinfo)
        {
            elog(DEBUG1, "thread %d exits.", i);
            continue;
        }

        if(false == thrinfo->thr_epoll_ok)
        {
            continue;
        }

        if(NULL == g_timekeeper_thread || NULL == g_checkpoint_thread)
        {
            elog(LOG, "timekeeper or checkpoint thread exited, should not add new connections.");

            return STATUS_ERROR;
        }

        conninfo->con_thrinfo = thrinfo;
        event.data.ptr = conninfo;
        event.events = EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP;
        if(-1 == epoll_ctl (thrinfo->thr_efd, EPOLL_CTL_ADD, conninfo->con_port->sock, &event))
        {
            elog(LOG, "failed to add socket to epoll");
            return STATUS_ERROR;
        }
        break;
    }
    return STATUS_OK;
}

/* ----------------
 *        ReadCommand reads a command from either the frontend or
 *        standard input, places it in inBuf, and returns the
 *        message type code (first byte of the message).
 *        EOF is returned if end of file.
 * ----------------
 */
static int
ReadCommand(Port *myport, StringInfo inBuf)
{
    int             qtype;

    /*
     * Get message type code from the frontend.
     */
    qtype = pq_getbyte(myport);

    if (qtype == EOF)            /* frontend disconnected */
    {
        /* don't fill up the proxy log with client disconnect messages */
        ereport(DEBUG1,
                (EPROTO,
                        errmsg("unexpected EOF on client connection")));
        return EOF;
    }

    /*
     * Validate message type code before trying to read body; if we have lost
     * sync, better to say "command unknown" than to run out of memory because
     * we used garbage as a length word.
     *
     * This also gives us a place to set the doing_extended_query_message flag
     * as soon as possible.
     */
    switch (qtype)
    {
        case 'C':
            break;

        case 'X':
            break;

        case 'F':
            break;

        default:

            /*
             * Otherwise we got garbage from the frontend.    We treat this as
             * fatal because we have probably lost message boundary sync, and
             * there's no good way to recover.
             */
            ereport(ERROR,
                    (EPROTO,
                            errmsg("invalid frontend message type %d", qtype)));

            break;
    }

    /*
     * In protocol version 3, all frontend messages have a length word next
     * after the type code; we can read the message contents independently of
     * the type.
     */
    if (pq_getmessage(myport, inBuf, 0))
        return EOF;            /* suitable message already logged */

    return qtype;
}

#ifndef __XLOG__
/*
 * Process MSG_SYNC_STANDBY message
 */
static void
ProcessSyncStandbyCommand(Port *myport, GTM_MessageType mtype, StringInfo message)
{
    StringInfoData    buf;

    pq_getmsgend(message);

    pq_beginmessage(&buf, 'S');
    pq_sendint(&buf, SYNC_STANDBY_RESULT, 4);
    pq_endmessage(myport, &buf);
    /* Sync standby first */
    if (GetMyConnection(myport)->standby)
        gtm_sync_standby(GetMyConnection(myport)->standby);
    pq_flush(myport);
}
#endif


static void
ProcessPGXCNodeCommand(Port *myport, GTM_MessageType mtype, StringInfo message)
{
    switch (mtype)
    {
        case MSG_NODE_REGISTER:
            ProcessPGXCNodeRegister(myport, message, false);
            break;
#ifndef __XLOG__
        case MSG_BKUP_NODE_REGISTER:
            ProcessPGXCNodeRegister(myport, message, true);
            break;
#endif

        case MSG_NODE_UNREGISTER:
            ProcessPGXCNodeUnregister(myport, message, false);
            break;

#ifndef __XLOG__
        case MSG_BKUP_NODE_UNREGISTER:
            ProcessPGXCNodeUnregister(myport, message, true);
            break;
#endif

        case MSG_NODE_LIST:
            ProcessPGXCNodeList(myport, message);
            break;

        case MSG_REGISTER_SESSION:
            ProcessPGXCRegisterSession(myport, message);
            break;

        default:
            Assert(0);            /* Shouldn't come here.. keep compiler quite */
    }
}

static void
ProcessTransactionCommand(Port *myport, GTM_MessageType mtype, StringInfo message)
{// #lizard forgives
    elog(DEBUG1, "ProcessTransactionCommand: mtype:%d", mtype);

    switch (mtype)
    {
        case MSG_NODE_BEGIN_REPLICATION_INIT:
            ProcessBeginReplicaInitSyncRequest(myport, message);
            break;

        case MSG_NODE_END_REPLICATION_INIT:
            ProcessEndReplicaInitSyncRequest(myport, message);
            break;

        case MSG_TXN_BEGIN:
            ProcessBeginTransactionCommand(myport, message);
            break;

#ifndef __XLOG__
        case MSG_BKUP_TXN_BEGIN:
            ProcessBkupBeginTransactionCommand(myport, message);
            break;
#endif

#ifdef __TBASE__
#ifndef __XLOG__
        case MSG_BKUP_GLOBAL_TIMESTAMP:
            ProcessBkupGlobalTimestamp(myport, message);
            break;
#endif

        case MSG_GETGTS:
            ProcessGetGTSCommand(myport, message);
            break;

        case MSG_GETGTS_MULTI:
            ProcessGetGTSCommandMulti(myport, message);
            break;

        case MSG_CHECK_GTM_STATUS:
            ProcessCheckGTMCommand(myport, message);
            break;
#endif
        case MSG_TXN_BEGIN_GETGXID:
            ProcessBeginTransactionGetGXIDCommand(myport, message);
            break;

#ifndef __XLOG__
        case MSG_BKUP_TXN_BEGIN_GETGXID:
            ProcessBkupBeginTransactionGetGXIDCommand(myport, message);
            break;
#endif

        case MSG_TXN_BEGIN_GETGXID_AUTOVACUUM:
            ProcessBeginTransactionGetGXIDAutovacuumCommand(myport, message);
            break;

#ifndef __XLOG__
        case MSG_BKUP_TXN_BEGIN_GETGXID_AUTOVACUUM:
            ProcessBkupBeginTransactionGetGXIDAutovacuumCommand(myport, message);
            break;
#endif

        case MSG_TXN_BEGIN_GETGXID_MULTI:
            ProcessBeginTransactionGetGXIDCommandMulti(myport, message);
            break;
#ifndef __XLOG__
        case MSG_BKUP_TXN_BEGIN_GETGXID_MULTI:
            ProcessBkupBeginTransactionGetGXIDCommandMulti(myport, message);
            break;
#endif

        case MSG_TXN_START_PREPARED:
            ProcessStartPreparedTransactionCommand(myport, message, false);
            break;

#ifndef __XLOG__
        case MSG_BKUP_TXN_START_PREPARED:
            ProcessStartPreparedTransactionCommand(myport, message, true);
            break;
#endif

        case MSG_TXN_PREPARE:
            ProcessPrepareTransactionCommand(myport, message, false);
            break;

#ifndef __XLOG__
        case MSG_BKUP_TXN_PREPARE:
            ProcessPrepareTransactionCommand(myport, message, true);
            break;
#endif

        case MSG_TXN_COMMIT:
            ProcessCommitTransactionCommand(myport, message, false);
            break;

#ifndef __XLOG__
        case MSG_BKUP_TXN_COMMIT:
            ProcessCommitTransactionCommand(myport, message, true);
            break;
#endif

        case MSG_TXN_COMMIT_PREPARED:
            ProcessCommitPreparedTransactionCommand(myport, message, false);
            break;

#ifndef __XLOG__
        case MSG_BKUP_TXN_COMMIT_PREPARED:
            ProcessCommitPreparedTransactionCommand(myport, message, true);
            break;
#endif

        case MSG_TXN_ROLLBACK:
            ProcessRollbackTransactionCommand(myport, message, false);
            break;

#ifndef __XLOG__
        case MSG_BKUP_TXN_ROLLBACK:
            ProcessRollbackTransactionCommand(myport, message, true);
            break;
#endif

        case MSG_TXN_COMMIT_MULTI:
            ProcessCommitTransactionCommandMulti(myport, message, false);
            break;

#ifndef __XLOG__
        case MSG_BKUP_TXN_COMMIT_MULTI:
            ProcessCommitTransactionCommandMulti(myport, message, true);
            break;
#endif

        case MSG_TXN_ROLLBACK_MULTI:
            ProcessRollbackTransactionCommandMulti(myport, message, false);
            break;

#ifndef __XLOG__
        case MSG_BKUP_TXN_ROLLBACK_MULTI:
            ProcessRollbackTransactionCommandMulti(myport, message, true);
            break;
#endif

        case MSG_TXN_GET_GXID:
            /*
             * Notice: we don't have corresponding functions in gtm_client.c
             *
             * Because this function is not used, GTM-standby extension is not
             * included in this function.
             */
            ProcessGetGXIDTransactionCommand(myport, message);
            break;

        case MSG_TXN_GET_GID_DATA:
            ProcessGetGIDDataTransactionCommand(myport, message);
            break;


#ifdef __TBASE__
        case MSG_TXN_FINISH_GID:
        {
            ProcessFinishGIDTransactionCommand(myport, message);
            break;
        }
        case MSG_TXN_LOG_COMMIT:
            ProcessLogTransactionCommand(myport, message, false, false);
            break;
        case MSG_TXN_LOG_GLOBAL_COMMIT:
            ProcessLogTransactionCommand(myport, message, true, false);
            break;
        case MSG_TXN_LOG_SCAN:
        case MSG_TXN_LOG_GLOBAL_SCAN:
            ProcessLogScanCommand(myport, message, false);
            break;
#endif

        case MSG_TXN_GET_NEXT_GXID:
            ProcessGetNextGXIDTransactionCommand(myport, message);
            break;

        case MSG_TXN_GXID_LIST:
            ProcessGXIDListCommand(myport, message);
            break;

        case MSG_REPORT_XMIN:
            ProcessReportXminCommand(myport, message, false);
            break;

        case MSG_BKUP_REPORT_XMIN:
            ProcessReportXminCommand(myport, message, true);
            break;

        default:
            Assert(0);            /* Shouldn't come here.. keep compiler quite */
    }
}

static void
ProcessSnapshotCommand(Port *myport, GTM_MessageType mtype, StringInfo message)
{
    switch (mtype)
    {
        case MSG_SNAPSHOT_GET:
            ProcessGetSnapshotCommand(myport, message, false);
            break;

        case MSG_SNAPSHOT_GET_MULTI:
            ProcessGetSnapshotCommandMulti(myport, message);
            break;

        case MSG_SNAPSHOT_GXID_GET:
            ProcessGetSnapshotCommand(myport, message, true);
            break;

        default:
            Assert(0);            /* Shouldn't come here.. keep compiler quite */
    }

}

static void
ProcessSequenceCommand(Port *myport, GTM_MessageType mtype, StringInfo message)
{// #lizard forgives
    /* We refuse all sequence command when sync commit is on and standby is not connected. */
    if (!Recovery_IsStandby() && enable_sync_commit && !SyncReady)
    {
        elog(ERROR, "synchronous commit is on, synchronous standby is not ready");
    }

    switch (mtype)
    {
        case MSG_SEQUENCE_INIT:
            ProcessSequenceInitCommand(myport, message, false);
            break;

#ifndef __XLOG__
        case MSG_BKUP_SEQUENCE_INIT:
            ProcessSequenceInitCommand(myport, message, true);
            break;
#endif

        case MSG_SEQUENCE_ALTER:
            ProcessSequenceAlterCommand(myport, message, false);
            break;

#ifndef __XLOG__
        case MSG_BKUP_SEQUENCE_ALTER:
            ProcessSequenceAlterCommand(myport, message, true);
            break;
#endif

        case MSG_SEQUENCE_GET_CURRENT:
            ProcessSequenceGetCurrentCommand(myport, message);
            break;

        case MSG_SEQUENCE_GET_NEXT:
            ProcessSequenceGetNextCommand(myport, message, false);
            break;

#ifndef __XLOG__
        case MSG_BKUP_SEQUENCE_GET_NEXT:
            ProcessSequenceGetNextCommand(myport, message, true);
            break;
#endif

        case MSG_SEQUENCE_SET_VAL:
            ProcessSequenceSetValCommand(myport, message, false);
            break;

#ifndef __XLOG__
        case MSG_BKUP_SEQUENCE_SET_VAL:
            ProcessSequenceSetValCommand(myport, message, true);
            break;
#endif

        case MSG_SEQUENCE_RESET:
            ProcessSequenceResetCommand(myport, message, false);
            break;

#ifndef __XLOG__
        case MSG_BKUP_SEQUENCE_RESET:
            ProcessSequenceResetCommand(myport, message, true);
            break;
#endif

        case MSG_SEQUENCE_CLOSE:
            ProcessSequenceCloseCommand(myport, message, false);
            break;

#ifndef __XLOG__
        case MSG_BKUP_SEQUENCE_CLOSE:
            ProcessSequenceCloseCommand(myport, message, true);
            break;
#endif

        case MSG_SEQUENCE_RENAME:
            ProcessSequenceRenameCommand(myport, message, false);
            break;
#ifdef __TBASE__        
        case MSG_DB_SEQUENCE_RENAME:
            ProcessDBSequenceRenameCommand(myport, message, false);
            break;

         case MSG_BKUP_DB_SEQUENCE_RENAME:
            ProcessDBSequenceRenameCommand(myport, message, true);
            break;
#endif

#ifndef __XLOG__
        case MSG_BKUP_SEQUENCE_RENAME:
            ProcessSequenceRenameCommand(myport, message, true);
            break;
#endif

        case MSG_SEQUENCE_LIST:
            ProcessSequenceListCommand(myport, message);
            break;

        case MSG_CLEAN_SESSION_SEQ:
            ProcessSequenceCleanCommand(myport, message, false);
            break;

        case MSG_SEQUENCE_COPY:
            ProcessCopyDataBaseSequenceCommand(myport, message);
            break;
        default:
            Assert(0);            /* Shouldn't come here.. keep compiler quite */
    }
}

static void
ProcessQueryCommand(Port *myport, GTM_MessageType mtype, StringInfo message)
{
    switch (mtype)
    {
        case MSG_TXN_GET_STATUS:
        case MSG_TXN_GET_ALL_PREPARED:
            break;

        default:
            Assert(0);            /* Shouldn't come here.. keep compiler quite */
    }

}


static void
GTM_RegisterPGXCNode(Port *myport, char *PGXCNodeName)
{
    MemoryContext oldContext;

    oldContext = MemoryContextSwitchTo(TopMemoryContext);
    elog(DEBUG3, "Registering coordinator with name %s", PGXCNodeName);
    myport->node_name = pstrdup(PGXCNodeName);
    MemoryContextSwitchTo(oldContext);
}


/*
 * Validate the proposed data directory
 */
static void
checkDataDir(void)
{
    struct stat stat_buf;

    Assert(GTMDataDir);

    retry:
    if (stat(GTMDataDir, &stat_buf) != 0)
    {
        if (errno == ENOENT)
        {
            if (mkdir(GTMDataDir, 0700) != 0)
            {
                ereport(FATAL,
                        (errno,
                                errmsg("failed to create the directory \"%s\"",
                                       GTMDataDir)));
            }
            goto retry;
        }
        else
            ereport(FATAL,
                    (EPERM,
                            errmsg("could not read permissions of directory \"%s\": %m",
                                   GTMDataDir)));
    }

    /* eventual chdir would fail anyway, but let's test ... */
    if (!S_ISDIR(stat_buf.st_mode))
        ereport(FATAL,
                (EINVAL,
                        errmsg("specified data directory \"%s\" is not a directory",
                               GTMDataDir)));

    /*
     * Check that the directory belongs to my userid; if not, reject.
     *
     * This check is an essential part of the interlock that prevents two
     * postmasters from starting in the same directory (see CreateLockFile()).
     * Do not remove or weaken it.
     *
     * XXX can we safely enable this check on Windows?
     */
#if !defined(WIN32) && !defined(__CYGWIN__)
    if (stat_buf.st_uid != geteuid())
        ereport(FATAL,
                (EINVAL,
                        errmsg("data directory \"%s\" has wrong ownership",
                               GTMDataDir),
                        errhint("The server must be started by the user that owns the data directory.")));
#endif
}

/*
 * Set data directory, but make sure it's an absolute path.  Use this,
 * never set DataDir directly.
 */
void
SetDataDir()
{
    char   *new;

    /* If presented path is relative, convert to absolute */
    new = make_absolute_path(GTMDataDir);
    if (!new)
        ereport(FATAL,
                (errno,
                        errmsg("failed to set the data directory \"%s\"",
                               GTMDataDir)));

    if (GTMDataDir)
        free(GTMDataDir);

    GTMDataDir = new;
}

/*
 * Change working directory to DataDir.  Most of the postmaster and backend
 * code assumes that we are in DataDir so it can use relative paths to access
 * stuff in and under the data directory.  For convenience during path
 * setup, however, we don't force the chdir to occur during SetDataDir.
 */
static void
ChangeToDataDir(void)
{
    if (chdir(GTMDataDir) < 0)
        ereport(FATAL,
                (EINVAL,
                        errmsg("could not change directory to \"%s\": %m",
                               GTMDataDir)));
}

/*
 * Create the data directory lockfile.
 *
 * When this is called, we must have already switched the working
 * directory to DataDir, so we can just use a relative path.  This
 * helps ensure that we are locking the directory we should be.
 */
static void
CreateDataDirLockFile()
{
    CreateLockFile(GTM_PID_FILE, GTMDataDir);
}

/*
 * Create a lockfile.
 *
 * filename is the name of the lockfile to create.
 * amPostmaster is used to determine how to encode the output PID.
 * isDDLock and refName are used to determine what error message to produce.
 */
void
CreateLockFile(const char *filename, const char *refName)
{// #lizard forgives
    int            fd;
    char        buffer[MAXPGPATH + 100];
    int            ntries;
    int            len;
    int            encoded_pid;
    pid_t        other_pid;
    pid_t        my_pid = getpid();

    /*
     * We need a loop here because of race conditions.    But don't loop forever
     * (for example, a non-writable $PGDATA directory might cause a failure
     * that won't go away).  100 tries seems like plenty.
     */
    for (ntries = 0;; ntries++)
    {
        /*
         * Try to create the lock file --- O_EXCL makes this atomic.
         *
         * Think not to make the file protection weaker than 0600.    See
         * comments below.
         */
        fd = open(filename, O_RDWR | O_CREAT | O_EXCL, 0600);
        if (fd >= 0)
            break;                /* Success; exit the retry loop */

        /*
         * Couldn't create the pid file. Probably it already exists.
         */
        if ((errno != EEXIST && errno != EACCES) || ntries > 100)
            ereport(FATAL,
                    (EINVAL,
                            errmsg("could not create lock file \"%s\": %m",
                                   filename)));

        /*
         * Read the file to get the old owner's PID.  Note race condition
         * here: file might have been deleted since we tried to create it.
         */
        fd = open(filename, O_RDONLY, 0600);
        if (fd < 0)
        {
            if (errno == ENOENT)
                continue;        /* race condition; try again */
            ereport(FATAL,
                    (EINVAL,
                            errmsg("could not open lock file \"%s\": %m",
                                   filename)));
        }
        if ((len = read(fd, buffer, sizeof(buffer) - 1)) < 0)
            ereport(FATAL,
                    (EINVAL,
                            errmsg("could not read lock file \"%s\": %m",
                                   filename)));
        close(fd);

        buffer[len] = '\0';
        encoded_pid = atoi(buffer);
        other_pid = (pid_t) encoded_pid;

        if (other_pid <= 0)
            elog(FATAL, "bogus data in lock file \"%s\": \"%s\"",
                 filename, buffer);

        /*
         * Check to see if the other process still exists
         *
         * If the PID in the lockfile is our own PID or our parent's PID, then
         * the file must be stale (probably left over from a previous system
         * boot cycle).  We need this test because of the likelihood that a
         * reboot will assign exactly the same PID as we had in the previous
         * reboot.    Also, if there is just one more process launch in this
         * reboot than in the previous one, the lockfile might mention our
         * parent's PID.  We can reject that since we'd never be launched
         * directly by a competing postmaster.    We can't detect grandparent
         * processes unfortunately, but if the init script is written
         * carefully then all but the immediate parent shell will be
         * root-owned processes and so the kill test will fail with EPERM.
         *
         * We can treat the EPERM-error case as okay because that error
         * implies that the existing process has a different userid than we
         * do, which means it cannot be a competing postmaster.  A postmaster
         * cannot successfully attach to a data directory owned by a userid
         * other than its own.    (This is now checked directly in
         * checkDataDir(), but has been true for a long time because of the
         * restriction that the data directory isn't group- or
         * world-accessible.)  Also, since we create the lockfiles mode 600,
         * we'd have failed above if the lockfile belonged to another userid
         * --- which means that whatever process kill() is reporting about
         * isn't the one that made the lockfile.  (NOTE: this last
         * consideration is the only one that keeps us from blowing away a
         * Unix socket file belonging to an instance of Postgres being run by
         * someone else, at least on machines where /tmp hasn't got a
         * stickybit.)
         *
         * Windows hasn't got getppid(), but doesn't need it since it's not
         * using real kill() either...
         *
         * Normally kill() will fail with ESRCH if the given PID doesn't
         * exist.
         */
        if (other_pid != my_pid
            #ifndef WIN32
            && other_pid != getppid()
#endif
                )
        {
            if (kill(other_pid, 0) == 0 ||
                (errno != ESRCH && errno != EPERM))
            {
                /* lockfile belongs to a live process */
                ereport(FATAL,
                        (EINVAL,
                                errmsg("lock file \"%s\" already exists",
                                       filename),
                                errhint("Is another GTM (PID %d) running in data directory \"%s\"?",
                                        (int) other_pid, refName)));
            }
        }

        /*
         * Looks like nobody's home.  Unlink the file and try again to create
         * it.    Need a loop because of possible race condition against other
         * would-be creators.
         */
        if (unlink(filename) < 0)
            ereport(FATAL,
                    (EACCES,
                            errmsg("could not remove old lock file \"%s\": %m",
                                   filename),
                            errhint("The file seems accidentally left over, but "
                                            "it could not be removed. Please remove the file "
                                            "by hand and try again.")));
    }

    /*
     * Successfully created the file, now fill it.
     */
    snprintf(buffer, sizeof(buffer), "%d\n%s\n%d\n",
             (int) my_pid, GTMDataDir, (Recovery_IsStandby() ? 0 : 1));
    errno = 0;
    if (write(fd, buffer, strlen(buffer)) != strlen(buffer))
    {
        int            save_errno = errno;

        close(fd);
        unlink(filename);
        /* if write didn't set errno, assume problem is no disk space */
        errno = save_errno ? save_errno : ENOSPC;
        ereport(FATAL,
                (EACCES,
                        errmsg("could not write lock file \"%s\": %m", filename)));
    }
    if (close(fd))
    {
        int            save_errno = errno;

        unlink(filename);
        errno = save_errno;
        ereport(FATAL,
                (EACCES,
                        errmsg("could not write lock file \"%s\": %m", filename)));
    }
}

/*
 * Create the opts file
 */
static bool
CreateOptsFile(int argc, char *argv[])
{
    FILE       *fp;
    int            i;

#define OPTS_FILE    "gtm.opts"

    if ((fp = fopen(OPTS_FILE, "w")) == NULL)
    {
        elog(LOG, "could not create file \"%s\": %m", OPTS_FILE);
        return false;
    }


    /* skip -D data args ,in case of rebooting will result in repeated parameters*/
    for (i = 1; i < argc; i++)
    {
        if(strcmp(argv[i],"-D") == 0)
            i++;
        if(strcmp(argv[i],"-g") == 0)
            i++;
        else
            fprintf(fp, " \"%s\"", argv[i]);
    }
    fputs("\n", fp);

    if (fclose(fp))
    {
        elog(LOG, "could not write file \"%s\": %m", OPTS_FILE);
        return false;
    }

    return true;
}

/* delete pid file */
void
DeleteLockFile(const char *filename)
{
    if (unlink(filename) < 0)
        ereport(FATAL,
                (EACCES,
                        errmsg("could not remove old lock file \"%s\": %m",
                               filename),
                        errhint("The file seems accidentally left over, but "
                                        "it could not be removed. Please remove the file "
                                        "by hand and try again.")));
}

static void
StartupThreadAfterPromote()
{
    GTM_MutexLockRelease(&g_checkpointer_blocker);
}


static void
PromoteToActive(void)
{
    const char *conf_file;
    FILE       *fp;

    elog(LOG, "Promote signal received. Becoming an active...");

    /*
     * Set starting and next client idendifier before promotion is complete
     */
//    GTM_SetInitialAndNextClientIdentifierAtPromote();

    SpinLockAcquire(&promote_status_lck);
    if (promote_status != GTM_PRPMOTE_INIT && promote_status != GTM_PRPMOTE_NORMAL)
    {
        elog(LOG, "Promote signal received. But not allow to promote, promote status %d", promote_status);
        SpinLockRelease(&promote_status_lck);
        return;
    }
	
    /*
     * Do promoting things here.
     * if promote_status is GTM_PRPMOTE_INIT, should use the CurrentTimeLineID to Recovery or OpenXLogFile,
     * keep it's value here
     */
    if (promote_status == GTM_PRPMOTE_NORMAL)
    {
    SetCurrentTimeLineID(GetCurrentTimeLineID() + 1);
    }
    else
    {
        promote_status = GTM_PRPMOTE_IN_STARTUP;
    }
    SpinLockRelease(&promote_status_lck);

    Recovery_StandbySetStandby(false);
    StartupThreadAfterPromote();
    CreateDataDirLockFile();

    /*
     * Update the GTM config file for the next restart..
     */
    conf_file = GetConfigOption("config_file", true);
    elog(LOG, "Config file is %s...", conf_file);
    if ((fp = fopen(conf_file, PG_BINARY_A)) == NULL)
    {
        ereport(FATAL,
                (EINVAL,
                        errmsg("could not open GTM configuration file \"%s\": %m",
                               conf_file)));

    }
    else
    {
        time_t        stamp_time = (time_t) time(NULL);
        char        strfbuf[128];
        struct tm   timeinfo;

        localtime_r(&stamp_time,&timeinfo);
        
        strftime(strfbuf, sizeof(strfbuf),
                 "%Y-%m-%d %H:%M:%S %Z",
                 &timeinfo);

        fprintf(fp,
                "#===================================================\n"
                        "# Updated due to GTM promote request\n"
                        "# %s\nstartup = ACT\n"
                        "#===================================================\n", strfbuf);
        if (fclose(fp))
            ereport(FATAL,
                    (EINVAL,
                            errmsg("could not close GTM configuration file \"%s\": %m",
                                   conf_file)));
    }

#ifndef __TBASE__
    GTM_SetNeedBackup();
    GTM_WriteRestorePoint();
#endif
    return;
}

static void ProcessBarrierCommand(Port *myport, GTM_MessageType mtype, StringInfo message)
{// #lizard forgives
    int barrier_id_len;
    char *barrier_id;
#ifndef __XLOG__
    int count = 0;
    GTM_Conn *oldconn = GetMyConnection(myport)->standby;
#endif
    StringInfoData buf;

    if (Recovery_IsStandby())
    {
        if (myport->remote_type != GTM_NODE_GTM)
        {
            elog(ERROR, "gtm standby can't provide barrier to datanodes or coordinators.");
        }
    }

    barrier_id_len = pq_getmsgint(message, sizeof(int));
    barrier_id = (char *)pq_getmsgbytes(message, barrier_id_len);
    pq_getmsgend(message);

    elog(INFO, "Processing BARRIER %s", barrier_id);

#ifndef __XLOG__
    if ((mtype == MSG_BARRIER) && GetMyConnection(myport)->standby)
    {
    retry:
        bkup_report_barrier(GetMyConnection(myport)->standby, barrier_id);
        if (gtm_standby_check_communication_error(myport, &count, oldconn))
            goto retry;

        if (Backup_synchronously && (myport->remote_type != GTM_NODE_GTM_PROXY))
            gtm_sync_standby(GetMyConnection(myport)->standby);
    }
#endif

    GTM_WriteBarrierBackup(barrier_id);

    if (mtype == MSG_BARRIER)
    {
        /*
         * Send a SUCCESS message back to the client
         */
        pq_beginmessage(&buf, 'S');
        pq_sendint(&buf, BARRIER_RESULT, 4);
        if (myport->remote_type == GTM_NODE_GTM_PROXY)
        {
            GTM_ProxyMsgHeader proxyhdr;
            proxyhdr.ph_conid = myport->conn_id;
            pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
        }
        pq_endmessage(myport, &buf);

        if (myport->remote_type != GTM_NODE_GTM_PROXY)
        {
#ifndef __XLOG__
            /* Flush standby first */
            if (GetMyConnection(myport)->standby)
                gtmpqFlush(GetMyConnection(myport)->standby);
#endif
            pq_flush(myport);
        }
    }
}

void
GTM_RestoreStart(FILE *ctlf, struct GTM_RestoreContext *context)
{
    int version;

    Assert(ctlf);

    if (fscanf(ctlf, "version: %d\n", &version) == 1)
    {
        elog(LOG, "Read control file version %d", version);
        context->version = version;
    }
    else
    {
        elog(LOG, "Failed to read file version");
        context->version = -1;
    }
}

void
GTM_RestoreTxnInfo(FILE *ctlf, GlobalTransactionId next_gxid,
                   struct GTM_RestoreContext *context, bool force_xid)
{
    GlobalTransactionId saved_gxid = InvalidGlobalTransactionId;
    GlobalTimestamp  saved_gts = 0;
    if (ctlf)
    {
        /*
         * If the control file version is 20160302, then we expect to see
         * next_xid and global_xmin saved as first two lines. For older
         * versions, just the next_xid is stored
         */
        if (context && context->version == 20160302)
        {
            if (fscanf(ctlf, "next_xid: %u\n", &saved_gxid) != 1)
                saved_gxid = InvalidGlobalTransactionId;

            if (fscanf(ctlf, "next_timestamp: " INT64_FORMAT "\n", &saved_gts) != 1)
                saved_gts = InvalidGlobalTransactionId;
        }
        else
        {
            if (fscanf(ctlf, "%u\n", &saved_gxid) != 1)
                saved_gxid = InvalidGlobalTransactionId;
        }
    }

    /*
     * If the caller has supplied an explicit XID to restore, just use that.
     * This is typically only be used during initdb and in some exception
     * circumstances to recover from failures. But otherwise we must start with
     * the XIDs saved in the control file
     *
     * If the global_xmin was saved (which should be unless we are dealing with
     * an old control file), use that. Otherwise set it saved_gxid/next_xid
     * whatever is available. If we don't used the value incremented by
     * CONTROL_INTERVAL because its better to start with a conservative value
     * for the GlobalXmin
     */


    SetNextGlobalTimestamp(saved_gts + GTM_GTS_ONE_SECOND * GTMStartupGTSDelta);
    SetNextGlobalTransactionId(next_gxid);
    elog(LOG, "Restoring last GXID to %u\n", next_gxid);
    elog(LOG, "Restoring global xmin to %u\n",
         GTMTransactions.gt_recent_global_xmin);
    elog(LOG, "Restoring gts to " INT64_FORMAT "\n",
         saved_gts + GTM_GTS_ONE_SECOND * GTMStartupGTSDelta);

    /* Set this otherwise a strange snapshot might be returned for the first one */
    GTMTransactions.gt_latestCompletedXid = next_gxid - 1;
    return;
}
static void
GTM_SaveVersion(FILE *ctlf)
{
    fprintf(ctlf, "version: %d\n", GTM_CONTROL_VERSION);
}

void
GTM_SaveTxnInfo(FILE *ctlf)
{
    GlobalTransactionId next_gxid;
    GlobalTransactionId global_xmin = GTMTransactions.gt_recent_global_xmin;
    GlobalTimestamp     next_gts;
    next_gts = GetNextGlobalTimestamp();
    next_gxid = ReadNewGlobalTransactionId();

    elog(DEBUG1, "Saving transaction info - next_gxid: %u, global_xmin: %u",
         next_gxid, global_xmin);

    fprintf(ctlf, "next_xid: %u\n", next_gxid);
    fprintf(ctlf, "next_timestamp: " INT64_FORMAT "\n", next_gts);
}

void
GTM_WriteRestorePointXid(FILE *f)
{
    if ((MaxGlobalTransactionId - GTMTransactions.gt_nextXid) <= RestoreDuration)
        GTMTransactions.gt_backedUpXid = GTMTransactions.gt_nextXid + RestoreDuration;
    else
        GTMTransactions.gt_backedUpXid = FirstNormalGlobalTransactionId + (RestoreDuration - (MaxGlobalTransactionId - GTMTransactions.gt_nextXid));

    elog(DEBUG1, "Saving transaction restoration info, backed-up gxid: %u", GTMTransactions.gt_backedUpXid);
    fprintf(f, "next_xid: %u\n", GTMTransactions.gt_backedUpXid);
    fprintf(f, "global_xmin: %u\n", GTMTransactions.gt_backedUpXid);
}

void
GTM_WriteRestorePointVersion(FILE *f)
{
    GTM_SaveVersion(f);
}

void
GTM_RestoreSeqInfo(FILE *ctlf, struct GTM_RestoreContext *context)
{// #lizard forgives
    char seqname[1024];

    if (ctlf == NULL)
        return;

    while (fscanf(ctlf, "%s", seqname) == 1)
    {
        GTM_SequenceKeyData seqkey;
        GTM_Sequence increment_by;
        GTM_Sequence minval;
        GTM_Sequence maxval;
        GTM_Sequence startval;
        GTM_Sequence curval;
        int32 state;
        bool cycle;
        bool called;
        char boolval[16];

        decode_seq_key(seqname, &seqkey);

        if (fscanf(ctlf, "%ld", &curval) != 1)
        {
            elog(WARNING, "Corrupted control file");
            return;
        }
        if (fscanf(ctlf, "%ld", &startval) != 1)
        {
            elog(WARNING, "Corrupted control file");
            return;
        }
        if (fscanf(ctlf, "%ld", &increment_by) != 1)
        {
            elog(WARNING, "Corrupted control file");
            return;
        }
        if (fscanf(ctlf, "%ld", &minval) != 1)
        {
            elog(WARNING, "Corrupted control file");
            return;
        }
        if (fscanf(ctlf, "%ld", &maxval) != 1)
        {
            elog(WARNING, "Corrupted control file");
            return;
        }
        if (fscanf(ctlf, "%s", boolval) == 1)
        {
            cycle = (*boolval == 't');
        }
        else
        {
            elog(WARNING, "Corrupted control file");
            return;
        }
        if (fscanf(ctlf, "%s", boolval) == 1)
        {
            called = (*boolval == 't');
        }
        else
        {
            elog(WARNING, "Corrupted control file");
            return;
        }
        if (fscanf(ctlf, "%x", &state) != 1)
        {
            elog(WARNING, "Corrupted control file");
            return;
        }
        GTM_SeqRestore(&seqkey, increment_by, minval, maxval, startval, curval,
                       state, cycle, called);
    }
}

#ifdef __TBASE__
void
GTM_RestoreStoreInfo(GlobalTransactionId next_gxid, bool force_xid)
{// #lizard forgives
    int32 ret = 0;
    GlobalTransactionId saved_gxid            = InvalidGlobalTransactionId;
    GlobalTransactionId saved_global_xmin    = InvalidGlobalTransactionId;
    GlobalTimestamp        saved_gts            = 0;

    ret = GTM_StoreRestore(&saved_gts, &saved_gxid, &saved_global_xmin);
    if (ret)
    {
        elog(FATAL, "GTM_RestoreStoreInfo restore data file failed");
        return;
    }
#ifndef __TBASE__
    /*
     * If the caller has supplied an explicit XID to restore, just use that.
     * This is typically only be used during initdb and in some exception
     * circumstances to recover from failures. But otherwise we must start with
     * the XIDs saved in the control file
     *
     * If the global_xmin was saved (which should be unless we are dealing with
     * an old control file), use that. Otherwise set it saved_gxid/next_xid
     * whatever is available. If we don't used the value incremented by
     * CONTROL_INTERVAL because its better to start with a conservative value
     * for the GlobalXmin
     */
    if (!GlobalTransactionIdIsValid(next_gxid))
    {
        if (GlobalTransactionIdIsValid(saved_gxid))
        {
            /* 
             * Add in extra amount in case we had not gracefully stopped
             */
            next_gxid = saved_gxid + CONTROL_INTERVAL;
            ret =  GTM_StoreReserveXid(CONTROL_INTERVAL);
            if (ret)
            {
                elog(FATAL, "GTM_RestoreStoreInfo reserved gxid failed");
            }
            SetControlXid(next_gxid);
        }
        else
        {
            saved_gxid = next_gxid = InitialGXIDValue_Default;
            ret =  GTM_StoreReserveXid(CONTROL_INTERVAL + InitialGXIDValue_Default);
            if (ret)
            {
                elog(FATAL, "GTM_RestoreStoreInfo reserved gxid failed");
            }
        }

        if (GlobalTransactionIdIsValid(saved_global_xmin))
        {
            GTMTransactions.gt_recent_global_xmin = saved_global_xmin;
        }
        else
        {
            GTMTransactions.gt_recent_global_xmin = saved_gxid;
        }
    }
    else
    {
        if (GlobalTransactionIdIsValid(saved_gxid) &&
            GlobalTransactionIdPrecedes(next_gxid, saved_gxid) && !force_xid)
            ereport(FATAL,
                    (EINVAL,
                     errmsg("Requested to start GTM with starting xid %d, "
                         "which is lower than gxid saved in control file %d. Refusing to start",
                         next_gxid, saved_gxid),
                     errhint("If you must force start GTM with a lower xid, please"
                         " use -f option")));
        GTMTransactions.gt_recent_global_xmin = next_gxid;
    }
    
    SetNextGlobalTransactionId(next_gxid);
#endif

    {
        GlobalTimestamp recovered_gts = saved_gts + GTMStartupGTSDelta * GTM_GTS_ONE_SECOND;
           SetNextGlobalTimestamp(recovered_gts);
        XLogCtl->segment_max_gts = recovered_gts;
        XLogCtl->segment_max_timestamp = time(NULL);
    }
        
    elog(LOG, "Restoring gts to " INT64_FORMAT "\n",
         saved_gts + GTMStartupGTSDelta * GTM_GTS_ONE_SECOND);
#ifndef __TBASE__
    elog(LOG, "Restoring last GXID to %u\n", next_gxid);
    elog(LOG, "Restoring global xmin to %u\n",
            GTMTransactions.gt_recent_global_xmin);

    /* Set this otherwise a strange snapshot might be returned for the first one */
    GTMTransactions.gt_latestCompletedXid = next_gxid - 1;
#endif
    return;
}

int GTM_TimerInit(void)
{
    int ret = 0;
    g_timer_entry = (GTM_TimerEntry*)malloc(sizeof(GTM_TimerEntry) * GTM_MAX_TIMER_ENTRY_NUMBER);
    if (NULL == g_timer_entry)
    {
        elog(LOG, "Failed to create timers, out of memory.");
        return -1;
    }
    memset(g_timer_entry, 0X00, sizeof(GTM_TimerEntry) * GTM_MAX_TIMER_ENTRY_NUMBER);

    ret = GTM_RWLockInit(&g_timer_lock);
    if (ret)
    {
        free(g_timer_entry);
        elog(LOG, "Failed to create timers, init lock failed for %s.", strerror(errno));
        return -1;
    }
    return 0;
}

/* Add a timer into the timer entries. */
GTM_TimerHandle GTM_AddTimer(void *(* func)(void*), GTM_TIMER_TYPE type, time_t interval, void *para)
{// #lizard forgives
    bool bret  = false;
    bool found = false;
    int  i    = 0;

    if (type != GTM_TIMER_TYPE_ONCE && type !=  GTM_TIMER_TYPE_LOOP)
    {
        elog(LOG, "Failed to acquire lock invalid timer type:%d.", type);
        return INVALID_TIMER_HANDLE;
    }

    bret = GTM_RWLockAcquire(&g_timer_lock, GTM_LOCKMODE_WRITE);
    if (!bret)
    {
        elog(LOG, "Failed to acquire lock when alloc timer. reason: %s.", strerror(errno));
        return INVALID_TIMER_HANDLE;
    }

    /* loop to find an empty entry */
    found = false;
    for (i = 0; i < GTM_MAX_TIMER_ENTRY_NUMBER; i++)
    {
        if (!g_timer_entry[i].balloced)
        {
            found = true;
            g_timer_entry[i].timer_routine = func;
            g_timer_entry[i].param = para;
            g_timer_entry[i].interval = interval;
            g_timer_entry[i].start_time = time(NULL);
            g_timer_entry[i].bactive  = true;
            g_timer_entry[i].balloced = true;
            g_used_entry++;
            break;
        }
    }

    bret = GTM_RWLockRelease(&g_timer_lock);
    if (!bret)
    {
        elog(LOG, "Failed to release lock when alloc timer. reason: %s.", strerror(errno));
        return INVALID_TIMER_HANDLE;
    }
    return found ? i : INVALID_TIMER_HANDLE;
}

/* Remove a timer from the timer entries. */
int GTM_RemoveTimer(GTM_TimerHandle handle)
{
    bool bret  = false;
    if (handle < 0 || handle >= GTM_MAX_TIMER_ENTRY_NUMBER )
    {
        elog(LOG, "Failed to remove timer for invalid handle:%d.", handle);
        return -1;
    }

    bret = GTM_RWLockAcquire(&g_timer_lock, GTM_LOCKMODE_WRITE);
    if (!bret)
    {
        elog(LOG, "Failed to acquire lock when remove timer. reason: %s.", strerror(errno));
        return -1;
    }

    g_timer_entry[handle].timer_routine = NULL;
    g_timer_entry[handle].param = NULL;
    g_timer_entry[handle].interval = 0;
    g_timer_entry[handle].start_time = 0;
    g_timer_entry[handle].bactive  = false;
    g_timer_entry[handle].balloced = false;
    g_used_entry--;
    bret = GTM_RWLockRelease(&g_timer_lock);
    if (!bret)
    {
        elog(LOG, "Failed to release lock when remove timer. reason: %s.", strerror(errno));
        return -1;
    }
    return 0;
}

/* Enable a timer. */
int GTM_ActiveTimer(GTM_TimerHandle handle)
{
    bool bret  = false;
    if (handle < 0 || handle >= GTM_MAX_TIMER_ENTRY_NUMBER )
    {
        elog(ERROR, "Failed to active timer for invalid handle:%d.", handle);
        return -1;
    }

    bret = GTM_RWLockAcquire(&g_timer_lock, GTM_LOCKMODE_WRITE);
    if (!bret)
    {
        elog(ERROR, "Failed to acquire lock when active timer. reason: %s.", strerror(errno));
        return -1;
    }

    g_timer_entry[handle].start_time = time(NULL);
    g_timer_entry[handle].bactive    = true;

    bret = GTM_RWLockRelease(&g_timer_lock);
    if (!bret)
    {
        elog(ERROR, "Failed to release lock when active timer. reason: %s.", strerror(errno));
        return -1;
    }
    return 0;
}

/* Disable a timer. */
int GTM_DeactiveTimer(GTM_TimerHandle handle)
{
    bool bret  = false;
    if (handle < 0 || handle >= GTM_MAX_TIMER_ENTRY_NUMBER )
    {
        elog(ERROR, "Failed to deactive timer for invalid handle:%d.", handle);
        return -1;
    }

    bret = GTM_RWLockAcquire(&g_timer_lock, GTM_LOCKMODE_WRITE);
    if (!bret)
    {
        elog(ERROR, "Failed to acquire lock when deactive timer. reason: %s.", strerror(errno));
        return -1;
    }

    g_timer_entry[handle].bactive  = false;
    g_timer_entry[handle].start_time = 0;
    bret = GTM_RWLockRelease(&g_timer_lock);
    if (!bret)
    {
        elog(ERROR, "Failed to release lock when deactive timer. reason: %s.", strerror(errno));
        return -1;
    }
    return 0;
}

/* Run the timer entries. */
void GTM_TimerRun(void)
{// #lizard forgives
    bool bret  = false;
    int  i    = 0;
    time_t now = 0;

    if (0 == g_used_entry)
    {
        return;
    }

    bret = GTM_RWLockAcquire(&g_timer_lock, GTM_LOCKMODE_WRITE);
    if (!bret)
    {
        elog(ERROR, "Failed to acquire lock when timer run. reason: %s.", strerror(errno));
        return ;
    }

    /* loop to find an empty entry */
    for (i = 0; i < GTM_MAX_TIMER_ENTRY_NUMBER; i++)
    {
        if (g_timer_entry[i].balloced && g_timer_entry[i].bactive)
        {
            now = time(NULL);
            switch (g_timer_entry[i].time_type)
            {
                case GTM_TIMER_TYPE_ONCE:
                {
                    if (now > g_timer_entry[i].start_time)
                    {
                        if (now - g_timer_entry[i].start_time >= g_timer_entry[i].interval)
                        {
                            g_timer_entry[i].timer_routine(g_timer_entry[i].param);
                            g_timer_entry[i].bactive  = false;/* Once timer will be disabled when triggered. */
                            g_timer_entry[i].start_time = 0;
                        }
                    }
                    else
                    {
                        /* just in case. */
                        g_timer_entry[i].start_time = now;
                    }
                    break;
                }

                case GTM_TIMER_TYPE_LOOP:
                {
                    if (now > g_timer_entry[i].start_time)
                    {
                        if (now - g_timer_entry[i].start_time >= g_timer_entry[i].interval)
                        {
                            g_timer_entry[i].timer_routine(g_timer_entry[i].param);
                            g_timer_entry[i].start_time = time(NULL); /* Once timer will run again when timeout. */
                        }
                    }
                    else
                    {
                        /* just in case. */
                        g_timer_entry[i].start_time = now;
                    }
                    break;
                }
                case GTM_TIMER_TYPE_BUTTY:
                default:
                {
                    GTM_RWLockRelease(&g_timer_lock);
                    elog(ERROR, "invalid timer type: %d.", g_timer_entry[i].time_type);
                }
            }
        }
    }

    bret = GTM_RWLockRelease(&g_timer_lock);
    if (!bret)
    {
        elog(ERROR, "Failed to release lock when timer run. reason: %s.", strerror(errno));
        return ;
    }
}

void
*
GTM_TimerThread(void *argp)
{
    GTM_ThreadInfo *thrinfo = (GTM_ThreadInfo *)argp;
    sigjmp_buf  local_sigjmp_buf;

    MessageContext = AllocSetContextCreate(TopMemoryContext,
                                           "MessageContext",
                                           ALLOCSET_DEFAULT_MINSIZE,
                                           ALLOCSET_DEFAULT_INITSIZE,
                                           ALLOCSET_DEFAULT_MAXSIZE,
                                           false);

    /*
     * POSTGRES main processing loop begins here
     *
     * If an exception is encountered, processing resumes here so we abort the
     * current transaction and start a new one.
     *
     * You might wonder why this isn't coded as an infinite loop around a
     * PG_TRY construct.  The reason is that this is the bottom of the
     * exception stack, and so with PG_TRY there would be no exception handler
     * in force at all during the CATCH part.  By leaving the outermost setjmp
     * always active, we have at least some chance of recovering from an error
     * during error recovery.  (If we get into an infinite loop thereby, it
     * will soon be stopped by overflow of elog.c's internal state stack.)
     */

    if (sigsetjmp(local_sigjmp_buf, 1) != 0)
    {
#ifdef __TBASE__
        RWLockCleanUp();
#endif
        EmitErrorReport(NULL);

        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        MemoryContextSwitchTo(TopMemoryContext);
        FlushErrorState();
    }

    /* We can now handle ereport(ERROR) */
    PG_exception_stack = &local_sigjmp_buf;

    for(;;)
    {
        MemoryContextSwitchTo(MessageContext);
        MemoryContextResetAndDeleteChildren(MessageContext);
        /* no need to lock here. */
        if(GTM_SHUTTING_DOWN == GTMTransactions.gt_gtm_state)
        {
            break;
        }

        GTM_TimerRun();

        sleep(GTM_TIMER_NAP);
    }
    elog(LOG, "GTM timer thread exit.");
    g_timer_thread = NULL;
    return thrinfo;
}

#ifndef __XLOG__
/*
 * Check whether the standby connection is avaliable.
 */
void CheckStandbyConnect(GTM_ThreadInfo *my_threadinfo, GTM_ConnectionInfo *conn)
{
    if (GTMThreads->gt_standby_ready     &&
            NULL == conn->standby        &&
            my_threadinfo->thr_status != GTM_THREAD_BACKUP)
    {
        /* Connect to GTM-Standby */
        conn->standby = gtm_standby_connect_to_standby();
        if (NULL == conn->standby)
        {    
            elog(LOG, "Connect standby node failed!!\n"); 
            GTMThreads->gt_standby_ready = false;    /* This will make other threads to disconnect from
                                                     * the standby, if needed.*/
        }
    }
    else if (!GTMThreads->gt_standby_ready && conn->standby)
    {
        /* Disconnect from GTM-Standby */
        gtm_standby_disconnect_from_standby(conn->standby);
        conn->standby = NULL;
    }    
}

#endif


/*
 * syslogger thread, handle log rotation and write log to logfile.
 */
static void*
GTM_ThreadSysLogger(void *argp)
{
#define GTM_SYSLOGGER_WAIT_EVENTS 2
    GTM_ThreadInfo *my_threadinfo = (GTM_ThreadInfo *)argp;
    sigjmp_buf      local_sigjmp_buf;
    pg_time_t		now = 0;
    int             efd = -1;
    int             n = 0;
    char		    logbuffer[READ_BUF_SIZE];
    int			    bytes_in_logbuffer = 0;
    char	        *currentLogFilename = NULL;
    int			    currentLogRotationAge = 0;
    bool            got_SIGHUP = false;
    bool            pipe_eof_seen = false;
    sigset_t        mask;
    struct epoll_event events[GTM_SYSLOGGER_WAIT_EVENTS];

    my_threadinfo->am_syslogger = true;

    /* ignore signal */
    sigfillset(&mask);
    pthread_sigmask(SIG_BLOCK, &mask, NULL);

    elog(DEBUG8, "Starting the syslogger thread");

    bind_service_threads();

    efd = GTM_InitSysloggerEpoll();
    if (efd == -1)
    {
        elog(LOG, "failed to init syslogger epoll");
        exit(1);
    }

    MessageContext = AllocSetContextCreate(TopMemoryContext,
                                           "MessageContext",
                                           ALLOCSET_DEFAULT_MINSIZE,
                                           ALLOCSET_DEFAULT_INITSIZE,
                                           ALLOCSET_DEFAULT_MAXSIZE,
                                           false);

    /*
     * POSTGRES main processing loop begins here
     *
     * If an exception is encountered, processing resumes here so we abort the
     * current transaction and start a new one.
     *
     * You might wonder why this isn't coded as an infinite loop around a
     * PG_TRY construct.  The reason is that this is the bottom of the
     * exception stack, and so with PG_TRY there would be no exception handler
     * in force at all during the CATCH part.  By leaving the outermost setjmp
     * always active, we have at least some chance of recovering from an error
     * during error recovery.  (If we get into an infinite loop thereby, it
     * will soon be stopped by overflow of elog.c's internal state stack.)
     */
    if (sigsetjmp(local_sigjmp_buf, 1) != 0)
    {
#ifdef __TBASE__
        RWLockCleanUp();
#endif
        EmitErrorReport(NULL);

        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        MemoryContextSwitchTo(TopMemoryContext);
        FlushErrorState();
    }

    /* We can now handle ereport(ERROR) */
    PG_exception_stack = &local_sigjmp_buf;

    MemoryContextSwitchTo(MessageContext);
    MemoryContextResetAndDeleteChildren(MessageContext);

    /* remember active logfile parameters */
    currentLogFilename = pstrdup(Log_filename);
    currentLogRotationAge = Log_RotationAge;

    /* set next planned rotation time */
    set_next_rotation_time();

    for(;;)
    {
        bool		time_based_rotation = false;
        int			size_rotation_for = 0;
        long		cur_timeout;
        int         i = 0;

        /*
		 * Process any requests or signals received recently.
		 */
        if (got_SIGHUP)
        {
            got_SIGHUP = false;

            if (strcmp(Log_filename, currentLogFilename) != 0)
            {
                pfree(currentLogFilename);
                currentLogFilename = pstrdup(Log_filename);
                rotation_requested = true;
            }

            /*
             * If rotation time parameter changed, reset next rotation time,
             * but don't immediately force a rotation.
             */
            if (currentLogRotationAge != Log_RotationAge)
            {
                currentLogRotationAge = Log_RotationAge;
                set_next_rotation_time();
            }

            /*
             * If we had a rotation-disabling failure, re-enable rotation
             * attempts after SIGHUP, and force one immediately.
             */
            if (rotation_disabled)
            {
                rotation_disabled = false;
                rotation_requested = true;
            }
        }

        if (Log_RotationAge > 0 && !rotation_disabled)
        {
            /* Do a logfile rotation if it's time */
            now = (pg_time_t) time(NULL);
            if (now >= next_rotation_time)
                rotation_requested = time_based_rotation = true;
        }

        if (!rotation_requested && Log_RotationSize > 0 && !rotation_disabled)
        {
            /* Do a rotation if file is too big */
            if (ftell(gtmlogFile) >= Log_RotationSize * 1024L)
            {
                rotation_requested = true;
                size_rotation_for |= LOG_DESTINATION_STDERR;
            }
        }

        if (rotation_requested)
        {
            /*
             * Force rotation when both values are zero. It means the request
             * was sent by pg_rotate_logfile.
             */
            if (!time_based_rotation && size_rotation_for == 0)
                size_rotation_for = LOG_DESTINATION_STDERR;
            logfile_rotate(time_based_rotation, size_rotation_for);
        }

        /*
         * Calculate time till next time-based rotation, so that we don't
         * sleep longer than that.  We assume the value of "now" obtained
         * above is still close enough.  Note we can't make this calculation
         * until after calling logfile_rotate(), since it will advance
         * next_rotation_time.
         *
         * Also note that we need to beware of overflow in calculation of the
         * timeout: with large settings of Log_RotationAge, next_rotation_time
         * could be more than INT_MAX msec in the future.  In that case we'll
         * wait no more than INT_MAX msec, and try again.
         */
        if (Log_RotationAge > 0 && !rotation_disabled)
        {
            pg_time_t	delay;

            delay = next_rotation_time - now;
            if (delay > 0)
            {
                if (delay > INT_MAX / 1000)
                    delay = INT_MAX / 1000;
                cur_timeout = delay * 1000L;	/* msec */
            }
            else
                cur_timeout = 0;
        }
        else
        {
            cur_timeout = -1L;
        }

        /*
         * Sleep until there's something to do
         */
        n = epoll_wait (efd, events, GTM_SYSLOGGER_WAIT_EVENTS, cur_timeout);
        for(i = 0; i < n; i++)
        {
            if(events[i].events & EPOLLIN)
            {
                if (events[i].data.fd == signalPipe[0])
                {
                    got_SIGHUP = true;
                    GTM_drainNotifyBytes();
                    elog(LOG, "Configuration update message received in syslogger thread.");
                }
                else
                {
                    int			bytesRead;

                    bytesRead = read(syslogPipe[0],
                                     logbuffer + bytes_in_logbuffer,
                                     sizeof(logbuffer) - bytes_in_logbuffer);
                    if (bytesRead < 0)
                    {
                        if (errno != EINTR)
                            ereport(LOG,
                                    (errmsg("could not read from logger pipe: %m")));
                    }
                    else if (bytesRead > 0)
                    {
                        bytes_in_logbuffer += bytesRead;
                        process_pipe_input(logbuffer, &bytes_in_logbuffer, &pipe_eof_seen);
                        continue;
                    }
                    else
                    {
                        /*
                         * Zero bytes read when select() is saying read-ready means
                         * EOF on the pipe: that is, there are no longer any processes
                         * with the pipe write end open.  Therefore, the postmaster
                         * and all backends are shut down, and we are done.
                         */
                        pipe_eof_seen = true;
                    }
                }
            }
        }

        if (pipe_eof_seen)
        {
            elog(LOG, "GTM syslogger exit(%d)", exit_flag);
            /* if there's any data left then force it out now */
            flush_pipe_input(logbuffer, &bytes_in_logbuffer);

            exit(exit_flag);
        }
    }

    return my_threadinfo;
}

static bool
GTM_SysLoggerStart(void)
{
    if (syslogPipe[0] < 0)
    {
        if (pipe(syslogPipe) < 0)
            ereport(FATAL,
                    (errmsg("could not create pipe for syslog: %m")));
    }

    if (signalPipe[0] < 0)
    {
        if (pipe(signalPipe) < 0)
            ereport(FATAL,
                    (errmsg("could not create pipe for signal: %m")));
    }

    /* Create GTM threads handling requests */
    g_syslogger_thread = GTM_ThreadCreate(GTM_ThreadSysLogger, g_max_lock_number);
    if (NULL == g_syslogger_thread)
    {
        return false;
    }

    fflush(stdout);
    if (dup2(syslogPipe[1], fileno(stdout)) < 0)
        ereport(FATAL,
                (errmsg("could not redirect stdout: %m")));
    fflush(stderr);
    if (dup2(syslogPipe[1], fileno(stderr)) < 0)
        ereport(FATAL,
                (errmsg("could not redirect stderr: %m")));
    /* Now we are done with the write end of the pipe. */
    close(syslogPipe[1]);
    syslogPipe[1] = -1;

    GetMyThreadInfo->am_syslogger = false;
    atexit(GTM_Exit);
    return true;
}

/*
 * let exit in syslogger
 */
void
GTM_Exit(void)
{
    if (g_syslogger_thread != NULL && !GetMyThreadInfo->am_syslogger)
    {
        Assert(exit_flag != GTM_DEFAULT_EXIT_FLAG);

        /* notify syslogger to do the exit */
        elog(LOG, "notify syslogger to exit(%d).", exit_flag);
        sleep(-1);
    }
}

#endif
