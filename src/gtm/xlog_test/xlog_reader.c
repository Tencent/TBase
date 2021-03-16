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
#include <sys/select.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/sysinfo.h>
#include <sys/epoll.h>
#include <getopt.h>
#include <stdio.h>

#include "gtm/gtm_c.h"
#include "gtm/path.h"
#include "gtm/gtm.h"
#include "gtm/elog.h"
#include "gtm/memutils.h"
#include "gtm/gtm_list.h"
#include "gtm/gtm_seq.h"
#include "gtm/standby_utils.h"
#include "gtm/gtm_standby.h"
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

#ifdef __TBASE__
#include "gtm/gtm_store.h"
#endif

#ifdef _ _XLOG__
#include "gtm/gtm_xlog.h"
#endif

extern int    optind;
extern char *optarg;

#define GTM_MAX_PATH            1024
#define GTM_DEFAULT_HOSTNAME    "*"
#define GTM_DEFAULT_PORT        6666
#define GTM_PID_FILE            "gtm.pid"
#define GTM_LOG_FILE            "gtm.log"

#define LOOPS_UNTIL_HIBERNATE        50
#define HIBERNATE_FACTOR        25

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
#endif
GTM_MutexLock   control_lock;

#ifdef __TBASE__
bool        enable_gtm_sequence_debug = false;
bool        enalbe_gtm_xlog_debug = true;
bool        enalbe_gtm_xlog_replay_debug = true;
bool        enable_gtm_debug   = false;
bool        enable_sync_commit = false;


GTM_TimerEntry *g_timer_entry;
int32              g_used_entry = 0;
GTM_RWLock      g_timer_lock; /* We don't expect too much concurrency, so use only a big lock. */
int                g_max_lock_number = 1024; /* max lock per thread can hold. init value is for main thread. it will be changed after initlize.*/
int             g_max_thread_number = 512; /* max thread number of gtm. */
GTM_ThreadInfo  *g_timekeeper_thread = NULL;
GTM_ThreadInfo    *g_timebackup_thread = NULL;
GTM_ThreadInfo  *g_timer_thread = NULL;
GTM_ThreadInfo  *g_xlog_writer_thread = NULL;
GTM_ThreadInfo  *g_checkpoint_thread = NULL;

uint32           *g_checkpointDirtyStart;
uint32           *g_checkpointDirtySize;
char             *g_checkpointMapperBuff;

static void  *GTM_TimerThread(void *argp);
static void   GTM_TimerRun(void);
static int    GTM_TimerInit(void);
static void   CheckStandbyConnect(GTM_ThreadInfo *my_threadinfo, GTM_ConnectionInfo *conn);


    static uint32 
ReaderPrintRangeOverwrite(XLogCmdRangerOverWrite *cmd);

    static uint32 
ReaderPrintCheckPoint(XLogCmdCheckPoint *cmd);

    static uint32 
ReaderPrintTimestamp(XLogRecGts *cmd);

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


/* The socket(s) we're listening to. */
#define MAXLISTEN    64
static int    ListenSocket[MAXLISTEN];

pthread_key_t    threadinfo_key;
static bool        GTMAbortPending = false;

static void GTM_SaveVersion(FILE *ctlf);

static Port *ConnCreate(int serverFd);
static int ServerLoop(void);
static int initMasks(fd_set *rmask);
void *GTM_XLogTestThread(void *argp);
void *GTM_ThreadTimeKeeper(void *argp);
void *GTM_ThreadTimeBackup(void *argp);
void *GTM_ThreadCheckPointer(void *argp);
void *GTM_ThreadXLogWriter(void *argp);
void *GTM_XLogTestThread(void *argp);

void bind_thread_to_cores (cpu_set_t cpuset) ;
void bind_timekeeper_thread(void);
void bind_service_threads(void);

static int GTMAddConnection(Port *port, GTM_Conn *standby);
static int ReadCommand(Port *myport, StringInfo inBuf);

static void ProcessCommand(Port *myport, StringInfo input_message);
static void ProcessPGXCNodeCommand(Port *myport, GTM_MessageType mtype, StringInfo message);
static void ProcessTransactionCommand(Port *myport, GTM_MessageType mtype, StringInfo message);
static void ProcessSnapshotCommand(Port *myport, GTM_MessageType mtype, StringInfo message);
static void ProcessSequenceCommand(Port *myport, GTM_MessageType mtype, StringInfo message);
static void ProcessQueryCommand(Port *myport, GTM_MessageType mtype, StringInfo message);

static void GTM_RegisterPGXCNode(Port *myport, char *PGXCNodeName);

static bool CreateOptsFile(int argc, char *argv[]);
static void CreateDataDirLockFile(void);
static void CreateLockFile(const char *filename, const char *refName);
static void SetDataDir(void);
static void ChangeToDataDir(void);
static void checkDataDir(void);
static void DeleteLockFile(const char *filename);
static void PromoteToActive(void);
static void ProcessSyncStandbyCommand(Port *myport, GTM_MessageType mtype, StringInfo message);
static void ProcessBarrierCommand(Port *myport, GTM_MessageType mtype, StringInfo message);
static int 
GTMInitConnection(GTM_ConnectionInfo *conninfo);


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
    if (NULL == thrinfo->locks_hold)
    {
        fprintf(stderr, "out of memory when init main thread");
        fflush(stdout);
        fflush(stderr);
        exit(1);
    }
    memset(thrinfo->locks_hold, 0x00, sizeof(void*) * g_max_lock_number);
#endif

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
    GTM_StoreSizeInit();
    if(GTM_ControlDataInit() != GTM_STORE_OK)
        exit(1);

    GTM_XLogCtlDataInit();
#endif

    if (GTMLogFile == NULL)
    {
        GTMLogFile = (char *) malloc(GTM_MAX_PATH);
        sprintf(GTMLogFile, "%s/%s", GTMDataDir, GTM_LOG_FILE);
    }

    /* Save Node Register File in register.c */
    Recovery_SaveRegisterFileName(GTMDataDir);

    DebugFileOpen();

    GTM_InitTxnManager();
    GTM_InitSeqManager();
    GTM_InitNodeManager();
}

static void
GTM_SigleHandler(int signal)
{
    fprintf(stderr, "Received signal %d\n", signal);

    switch (signal)
    {
        case SIGKILL:
        case SIGTERM:
        case SIGQUIT:
        case SIGINT:
        case SIGHUP:
            break;

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

    /* Delete pid file before shutting down */
    DeleteLockFile(GTM_PID_FILE);

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
    printf(_("  -c              show server status, then exit\n"));
    printf(_("  -f              force start GTM with starting XID specified by -x option\n"));
    printf(_("  --help          show this help, then exit\n"));
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

char buff[GTM_XLOG_SEG_SIZE];

    static void
ReadRedoXLogRecord(XLogRecord *rec)
{
    uint32   len;
    char    *data;
    uint32   cmd_size;
    XLogCmdHeader *header;

    len = rec->xl_tot_len;

    data = (char *)rec + sizeof(XLogRecord);

    while(len > 0)
    {
        header = (XLogCmdHeader *)data;

        switch (header->type)
        {
            case XLOG_CMD_RANGE_OVERWRITE:
                cmd_size = ReaderPrintRangeOverwrite((XLogCmdRangerOverWrite *)data);
                break;
            case XLOG_CMD_CHECK_POINT:
                cmd_size = ReaderPrintCheckPoint((XLogCmdCheckPoint *)data);
                break;
            case XLOG_REC_GTS:
                cmd_size = ReaderPrintTimestamp((XLogRecGts *)data);
                break;
            default:
                printf( "unrecognize xlog command type %d",header->type);
                exit(1);
        }

        len  -= cmd_size;
        data += cmd_size;
    }
}


    static int64 
ReadXLogToBuff(char *xlog_path)
{
    ssize_t nbytes;
    int     fd;
    struct stat statbuf;

    fd = open(xlog_path,O_RDWR, S_IRUSR | S_IWUSR);

    if (fstat(fd, &statbuf) < 0)
    {
        close(fd);
        printf( "ReadXLog stat file:%s failed for:%s.", xlog_path, strerror(errno));
        return -1;
    }

    if(statbuf.st_size > GTM_XLOG_SEG_SIZE)
    {
        close(fd);
        printf( "ReadXLog file %s size larger than %d",xlog_path,GTM_XLOG_SEG_SIZE);
        return -1;
    }

    nbytes = read(fd, buff, statbuf.st_size);

    if(nbytes != statbuf.st_size)
    {
        close(fd);
        printf( "ReadXLog read file %s failed for:%s.", xlog_path, strerror(errno));
        return -1;
    }

    return nbytes;
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
{
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
        if (GTMAbortPending || NULL == g_timekeeper_thread)
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
            GTM_StoreShutDown(); //TODO xlog shutdown or not?

            /* Save control data */
            GTM_RWLockAcquire(&ControlDataLock,GTM_LOCKMODE_WRITE);
            ControlData->state = DB_SHUTDOWNED;
            ControlDataSync();
            GTM_RWLockRelease(&ControlDataLock);

#else            
            SaveControlInfo();
#endif
            elog(LOG, "GTM is going to exit...");
            exit(1);
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

    if (conn->standby)
    {
        elog(DEBUG1, "Closing a connection to the GTM standby.");
    
        GTMPQfinish(conn->standby);
        conn->standby = NULL;
    }

    if (conn->con_port)
    {
        StreamClose(conn->con_port->sock);

        /* Free the node_name in the port */
        if (conn->con_port->node_name != NULL)
            /* 
             * We don't have to reset pointer to NULL her because ConnFree() 
             * frees this structure next.
             */
            pfree(conn->con_port->node_name);

        if(conn->con_port->remote_host)
        {
            free(conn->con_port->remote_host);
        }
        
        if(conn->con_port->remote_port)
        {
            free(conn->con_port->remote_port);
        }
        
        /* Free the port */
        ConnFree(conn->con_port);
        conn->con_port = NULL;
    }
    /* Free the connection info structure */
    pfree(conn);
    
    MemoryContextSwitchTo(oldContext);
}

void
GTM_RemoveConnection(GTM_ConnectionInfo *conn)
{
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
{
    GTM_ThreadInfo *my_threadinfo = (GTM_ThreadInfo *)argp;
    sigjmp_buf  local_sigjmp_buf;
    time_t      last;
    time_t        now;
    int ret;
    struct sigaction    action;  
       
    action.sa_flags = 0;  
    action.sa_handler = GTM_ThreadSigHandler;  
         
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
        
        latestGlobalTimestamp = SyncGlobalTimestamp();
        last = GTM_TimestampGetMonotonicRaw();                

        //XLogBeginInsert(); 

        //if(GTM_StoreGlobalTimestamp(latestGlobalTimestamp))
        //{
        //    elog(LOG, "storing global timestamp failed, going to exit!!");
        //    GTM_SetShuttingDown();
        //    break;
        //}        

        //XLogFlush(XLogInsert());
        
        usleep(GTM_SYNC_CYCLE);        

        now = GTM_TimestampGetMonotonicRaw();    
        if((now - last) > GTM_SYNC_TIME_LIMIT)
        {
            elog(LOG, "The timekeeper thread takes too long "INT64_FORMAT " seconds to complete, going to exit", now - last);
            GTM_SetShuttingDown();
            break;
        }
    }
shutdown:
    g_timekeeper_thread = NULL;
    elog(LOG, "GTM is shuting down, timekeeper exits!");
    return my_threadinfo;    
}


/* time keeper thread will not handle any signal, any signal will cause the thread exit. */
void 
*
GTM_ThreadTimeBackup(void *argp)
{
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
	sigemptyset(&action.sa_mask);
         
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
            goto shutdown;
        }

        DoCheckPoint(false);

    }
shutdown:
    g_checkpoint_thread = NULL;
    elog(LOG, "GTM is shuting down, checkpoint exits!");
    return my_threadinfo;    
}

void *
GTM_ThreadXLogWriter(void *argp)
{
    GTM_ThreadInfo *my_threadinfo = (GTM_ThreadInfo *)argp;
    struct sigaction    action;  
    sigjmp_buf     local_sigjmp_buf;
    int            left_till_hibernate;
    int            cur_timeout;
    int ret;
       
    action.sa_flags = 0;  
    action.sa_handler = GTM_ThreadSigHandler;  
	sigemptyset(&action.sa_mask);
    ret = sigaction(SIGQUIT, &action, NULL);  
    if (ret)
    {
        elog(LOG, "register thread quit handler failed");
    }

    elog(DEBUG8, "Starting the walwriter thread");

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

    left_till_hibernate = LOOPS_UNTIL_HIBERNATE;
    for(;;)
    {        
        MemoryContextSwitchTo(MessageContext);
        MemoryContextResetAndDeleteChildren(MessageContext);

        /* no need to lock here. */
        if(GTM_SHUTTING_DOWN == GTMTransactions.gt_gtm_state)
            goto shutdown;

        if(XLogBackgroundFlush())
            left_till_hibernate = LOOPS_UNTIL_HIBERNATE;
        else if (left_till_hibernate > 0)
            left_till_hibernate--;

        /*
         * Sleep until we are signaled or WalWriterDelay has elapsed.  If we
         * haven't done anything useful for quite some time, lengthen the
         * sleep time so as to reduce the server's idle power consumption.
         */
        if (left_till_hibernate > 0)
            cur_timeout = wal_writer_delay;    /* in ms */
        else
            cur_timeout = wal_writer_delay * HIBERNATE_FACTOR;    

        usleep(cur_timeout);
    }
shutdown:
    g_xlog_writer_thread = NULL;
    elog(LOG, "GTM is shuting down, walwriter exits!");
    return my_threadinfo;    
}

static int GetRandomInt(int max)
{
    return rand() % max;
}

void *
GTM_XLogTestThread(void *argp)
{
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
    char *data;
    int  i, n;
    int num;
         
    ret = sigaction(SIGQUIT, &action, NULL);  
    if (ret)
    {
        elog(LOG, "register thread quit handler failed");
    }

    elog(DEBUG8, "Starting the connection helper thread");
    bind_service_threads();

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
    
    n = 0;

    srand(time(NULL));
    
    for (;;)
    {

        MemoryContextSwitchTo(MessageContext);
        MemoryContextResetAndDeleteChildren(MessageContext);
        resetStringInfo(&input_message);

        GTM_RWLockAcquire(&thrinfo->thr_lock, GTM_LOCKMODE_WRITE); 

        XLogBeginInsert();
       
        num = GetRandomInt(20480); 

        data = palloc(num);

        for(i = 0; i < num;i++)
            data[i] = i % 128;

        XLogRegisterRangeOverwrite(num,num,data);
        n = num;

        XLogFlush(XLogInsert());

        GTM_RWLockRelease(&thrinfo->thr_lock); 

        if(GTM_SHUTTING_DOWN == GTMTransactions.gt_gtm_state)
        {
            break;
        }
    }

    return thrinfo;
}

void
ProcessCommand(Port *myport, StringInfo input_message)
{
    bool                handle_standby = false;
    GTM_MessageType    mtype;
    GTM_ProxyMsgHeader proxyhdr;

#ifdef __TBASE__
    GTM_ThreadInfo *my_threadinfo = NULL;    
    GTM_ConnectionInfo *conn;
    my_threadinfo = GetMyThreadInfo;
    conn = my_threadinfo->thr_conn;        
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
    /*
     * Get Timestamp does not need to sync with standby
     */
    handle_standby = (mtype != MSG_GETGTS && 
            mtype != MSG_GETGTS_MULTI && 
            mtype != MSG_BEGIN_BACKUP && 
            mtype != MSG_END_BACKUP &&
            mtype != MSG_LIST_GTM_STORE &&
            mtype != MSG_LIST_GTM_STORE_SEQ &&
            mtype != MSG_LIST_GTM_STORE_TXN &&
            mtype != MSG_CHECK_GTM_STORE_SEQ &&
            mtype != MSG_CHECK_GTM_STORE_TXN 
            );

    if(handle_standby)
    {
        /* Handle standby connecion staff. */
        CheckStandbyConnect(my_threadinfo, conn);

        /* Hold the lock, in case reset all standby connections. */
        GTM_RWLockAcquire(&my_threadinfo->thr_lock, GTM_LOCKMODE_WRITE);        
    }

    XLogBeginInsert();

#endif
    switch (mtype)
    {
        case MSG_SYNC_STANDBY:
            ProcessSyncStandbyCommand(myport, mtype, input_message);
            break;
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
        case MSG_END_BACKUP:
            ProcessGTMEndBackup(myport, input_message);
            break;
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
        case MSG_GET_STORAGE:
            {
                /* process storage file transfer request */
                ProcessStorageTransferCommand(myport, input_message);
                break;
            }

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
#endif
        default:
            ereport(FATAL,
                    (EPROTO,
                     errmsg("invalid frontend message type %d",
                         mtype)));
    }

#ifdef __TBASE__    
    if (handle_standby)
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
{
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
            elog(LOG, "Expecting a startup message, but received %c",
                    startup_type); 
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
{
    GTM_ConnectionInfo *conninfo = NULL;
    int             i;
    GTM_ThreadInfo        *thrinfo;    
    struct epoll_event event;


    conninfo = (GTM_ConnectionInfo *)palloc0(sizeof (GTM_ConnectionInfo));
    elog(DEBUG8, "Started new connection");
    conninfo->con_port = port;
    port->conn = conninfo;

    /*
     * Add a connection to the standby.
     */
    if (standby != NULL)
        conninfo->standby = standby;

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
        while(GTMThreads->gt_thread_count < GTMThreads->gt_start_thread_count)
        {
            elog(LOG, "create threads on demand thread count %d start thread count %d", 
                    GTMThreads->gt_thread_count,GTMThreads->gt_start_thread_count);

            GTM_RWLockRelease(&GTMThreads->gt_lock);

            if (NULL == GTM_ThreadCreate(GTM_XLogTestThread, g_max_lock_number))
            {
                elog(WARNING, "Failed to create gtm thread.");
                break;
            }
            GTM_RWLockAcquire(&GTMThreads->gt_lock, GTM_LOCKMODE_READ);

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

        if(NULL == g_timekeeper_thread)
        {
            elog(LOG, "timekeeper thread exited, should not add new connections.");

            return STATUS_ERROR;
        }

        conninfo->con_thrinfo = thrinfo;
        event.data.ptr = conninfo;
        event.events = EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP;
        if(-1 == epoll_ctl (thrinfo->thr_efd, EPOLL_CTL_ADD, conninfo->con_port->sock, &event))
        {
            elog(LOG, "failed to add socket to epoll");
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


    static void
ProcessPGXCNodeCommand(Port *myport, GTM_MessageType mtype, StringInfo message)
{
    switch (mtype)
    {
        case MSG_NODE_REGISTER:
            ProcessPGXCNodeRegister(myport, message, false);
            break;

        case MSG_BKUP_NODE_REGISTER:
            ProcessPGXCNodeRegister(myport, message, true);
            break;

        case MSG_NODE_UNREGISTER:
            ProcessPGXCNodeUnregister(myport, message, false);
            break;

        case MSG_BKUP_NODE_UNREGISTER:
            ProcessPGXCNodeUnregister(myport, message, true);
            break;

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
{
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

        case MSG_BKUP_TXN_BEGIN:
            ProcessBkupBeginTransactionCommand(myport, message);
            break;

#ifdef __TBASE__
        case MSG_BKUP_GLOBAL_TIMESTAMP:
            ProcessBkupGlobalTimestamp(myport, message);
            break;

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

        case MSG_BKUP_TXN_BEGIN_GETGXID:
            ProcessBkupBeginTransactionGetGXIDCommand(myport, message);
            break;

        case MSG_TXN_BEGIN_GETGXID_AUTOVACUUM:
            ProcessBeginTransactionGetGXIDAutovacuumCommand(myport, message);
            break;

        case MSG_BKUP_TXN_BEGIN_GETGXID_AUTOVACUUM:
            ProcessBkupBeginTransactionGetGXIDAutovacuumCommand(myport, message);
            break;

        case MSG_TXN_BEGIN_GETGXID_MULTI:
            ProcessBeginTransactionGetGXIDCommandMulti(myport, message);
            break;

        case MSG_BKUP_TXN_BEGIN_GETGXID_MULTI:
            ProcessBkupBeginTransactionGetGXIDCommandMulti(myport, message);
            break;

        case MSG_TXN_START_PREPARED:
            ProcessStartPreparedTransactionCommand(myport, message, false);
            break;

        case MSG_BKUP_TXN_START_PREPARED:
            ProcessStartPreparedTransactionCommand(myport, message, true);
            break;

        case MSG_TXN_PREPARE:
            ProcessPrepareTransactionCommand(myport, message, false);
            break;

        case MSG_BKUP_TXN_PREPARE:
            ProcessPrepareTransactionCommand(myport, message, true);
            break;

        case MSG_TXN_COMMIT:
            ProcessCommitTransactionCommand(myport, message, false);
            break;

        case MSG_BKUP_TXN_COMMIT:
            ProcessCommitTransactionCommand(myport, message, true);
            break;

        case MSG_TXN_COMMIT_PREPARED:
            ProcessCommitPreparedTransactionCommand(myport, message, false);
            break;

        case MSG_BKUP_TXN_COMMIT_PREPARED:
            ProcessCommitPreparedTransactionCommand(myport, message, true);
            break;

        case MSG_TXN_ROLLBACK:
            ProcessRollbackTransactionCommand(myport, message, false);
            break;

        case MSG_BKUP_TXN_ROLLBACK:
            ProcessRollbackTransactionCommand(myport, message, true);
            break;

        case MSG_TXN_COMMIT_MULTI:
            ProcessCommitTransactionCommandMulti(myport, message, false);
            break;

        case MSG_BKUP_TXN_COMMIT_MULTI:
            ProcessCommitTransactionCommandMulti(myport, message, true);
            break;

        case MSG_TXN_ROLLBACK_MULTI:
            ProcessRollbackTransactionCommandMulti(myport, message, false);
            break;

        case MSG_BKUP_TXN_ROLLBACK_MULTI:
            ProcessRollbackTransactionCommandMulti(myport, message, true);
            break;

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
{
    /* We refuse all sequence command when sync commit is on and standby is not connected. */
    if (enable_sync_commit)
    {
        if (!Recovery_IsStandby())
        {
            if (NULL == GetMyConnection(myport)->standby)
            {
                elog(ERROR, "synchronus commit is on, standby must be connected.");
            }
        }
    }

    switch (mtype)
    {
        case MSG_SEQUENCE_INIT:
            ProcessSequenceInitCommand(myport, message, false);
            break;

        case MSG_BKUP_SEQUENCE_INIT:
            ProcessSequenceInitCommand(myport, message, true);
            break;

        case MSG_SEQUENCE_ALTER:
            ProcessSequenceAlterCommand(myport, message, false);
            break;

        case MSG_BKUP_SEQUENCE_ALTER:
            ProcessSequenceAlterCommand(myport, message, true);
            break;

        case MSG_SEQUENCE_GET_CURRENT:
            ProcessSequenceGetCurrentCommand(myport, message);
            break;

        case MSG_SEQUENCE_GET_NEXT:
            ProcessSequenceGetNextCommand(myport, message, false);
            break;

        case MSG_BKUP_SEQUENCE_GET_NEXT:
            ProcessSequenceGetNextCommand(myport, message, true);
            break;

        case MSG_SEQUENCE_SET_VAL:
            ProcessSequenceSetValCommand(myport, message, false);
            break;

        case MSG_BKUP_SEQUENCE_SET_VAL:
            ProcessSequenceSetValCommand(myport, message, true);
            break;

        case MSG_SEQUENCE_RESET:
            ProcessSequenceResetCommand(myport, message, false);
            break;

        case MSG_BKUP_SEQUENCE_RESET:
            ProcessSequenceResetCommand(myport, message, true);
            break;

        case MSG_SEQUENCE_CLOSE:
            ProcessSequenceCloseCommand(myport, message, false);
            break;

        case MSG_BKUP_SEQUENCE_CLOSE:
            ProcessSequenceCloseCommand(myport, message, true);
            break;

        case MSG_SEQUENCE_RENAME:
            ProcessSequenceRenameCommand(myport, message, false);
            break;

        case MSG_BKUP_SEQUENCE_RENAME:
            ProcessSequenceRenameCommand(myport, message, true);
            break;

        case MSG_SEQUENCE_LIST:
            ProcessSequenceListCommand(myport, message);
            break;

        case MSG_CLEAN_SESSION_SEQ:
            ProcessSequenceCleanCommand(myport, message, false);
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
    static void
CreateLockFile(const char *filename, const char *refName)
{
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

    for (i = 1; i < argc; i++)
        fprintf(fp, " \"%s\"", argv[i]);
    fputs("\n", fp);

    if (fclose(fp))
    {
        elog(LOG, "could not write file \"%s\": %m", OPTS_FILE);
        return false;
    }

    return true;
}

/* delete pid file */
    static void
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
PromoteToActive(void)
{
    const char *conf_file;
    FILE       *fp;

    elog(LOG, "Promote signal received. Becoming an active...");

    /*
     * Set starting and next client idendifier before promotion is complete
     */
    //    GTM_SetInitialAndNextClientIdentifierAtPromote();

    /*
     * Do promoting things here.
     */
    Recovery_StandbySetStandby(false);
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

        strftime(strfbuf, sizeof(strfbuf),
                "%Y-%m-%d %H:%M:%S %Z",
                localtime(&stamp_time));

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
{
    int barrier_id_len;
    char *barrier_id;
    int count = 0;
    GTM_Conn *oldconn = GetMyConnection(myport)->standby;
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

    if ((mtype == MSG_BARRIER) && GetMyConnection(myport)->standby)
    {
retry:
        bkup_report_barrier(GetMyConnection(myport)->standby, barrier_id);
        if (gtm_standby_check_communication_error(myport, &count, oldconn))
            goto retry;

        if (Backup_synchronously && (myport->remote_type != GTM_NODE_GTM_PROXY))
            gtm_sync_standby(GetMyConnection(myport)->standby);
    }

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
            /* Flush standby first */
            if (GetMyConnection(myport)->standby)
                gtmpqFlush(GetMyConnection(myport)->standby);
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


    SetNextGlobalTimestamp(saved_gts + GTM_GLOBAL_TIME_DELTA);
    SetNextGlobalTransactionId(next_gxid);
    elog(LOG, "Restoring last GXID to %u\n", next_gxid);
    elog(LOG, "Restoring global xmin to %u\n",
            GTMTransactions.gt_recent_global_xmin);
    elog(LOG, "Restoring gts to " INT64_FORMAT "\n",
            saved_gts + GTM_GLOBAL_TIME_DELTA);

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
{
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
{
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

    SetNextGlobalTimestamp(saved_gts + GTM_GLOBAL_TIME_DELTA);
    elog(LOG, "Restoring gts to " INT64_FORMAT "\n",
            saved_gts + GTM_GLOBAL_TIME_DELTA);
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
{
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
{
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

    void *
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

XLogRecPtr PrintPageHeader(XLogPageHeaderData *header)
{
    if(header->xlp_magic != GTM_XLOG_PAGE_MAGIC)
    {
        printf("xlog header magic validate fails %x\n",header->xlp_magic);
        return InvalidXLogRecPtr;
    }
    printf("XLogPageHeaderData ptr:%lld\n",header->xlp_pageaddr);
    return header->xlp_pageaddr;
}

void PrintMemory(char *data,int size)
{
    while(size--)
    {
        printf("%d ",*data);
        data++;
    }
    printf("\n");
}

    static uint32 
ReaderPrintRangeOverwrite(XLogCmdRangerOverWrite *cmd)
{
    printf("RangeOverwrite %d %d",cmd->offset,cmd->bytes);

    return cmd->bytes + sizeof(XLogCmdRangerOverWrite);
}

    static uint32 
ReaderPrintCheckPoint(XLogCmdCheckPoint *cmd)
{
    printf("ReaderPrintCheckPoint %lld ",cmd->gts);
    return sizeof(XLogCmdCheckPoint);
}

    static uint32 
ReaderPrintTimestamp(XLogRecGts *cmd)
{
    printf("ReaderPrintCheckPoint %lld",cmd->gts);
    return sizeof(XLogCmdCheckPoint);
}

void
Read_XLogRecovery(char *xlog_path,uint64 segment_no,uint64 offset)
{
    char       *xlog_buff;
    char       *xlog_rec;
    uint64      idx;
    uint64      page_offset;
    ssize_t     bytes_read;
    ssize_t     desired_bytes;
    ssize_t     read_size;
    ssize_t     cur_xlog_size;
    XLogRecPtr  redo_pos;
    XLogRecPtr  redo_end_pos;
    XLogRecPtr  preXLogRecord;
    XLogRecord  *record_header;
    bool        read_header;
    pg_crc32    crc;
    XLogPageHeaderData header;
    int rec_offset = 0;

    xlog_buff = buff;

    /* One record must not larger then UsableBytesInSegment */
    xlog_rec = malloc(UsableBytesInSegment); 

    record_header = (XLogRecord  *)xlog_rec;

    cur_xlog_size = ReadXLogToBuff(xlog_path);
    Assert(cur_xlog_size >= 0);

    desired_bytes = sizeof(XLogRecord);
    bytes_read    = 0;
    read_header   = false;
    preXLogRecord = InvalidXLogRecPtr;
    redo_pos      = InvalidXLogRecPtr;
    redo_end_pos  = InvalidXLogRecPtr;
    idx           = offset % GTM_XLOG_SEG_SIZE;

    xlog_buff += offset;


    while(idx < cur_xlog_size)
    {
        read_size   = desired_bytes;

        page_offset = idx % GTM_XLOG_BLCKSZ;

        /* if we are at the head of a page,skip the page header */
        if(page_offset == 0)
        {
            memcpy(&header,xlog_buff,sizeof(XLogPageHeaderData));

            PrintPageHeader(&header);
            
            xlog_buff += sizeof(XLogPageHeaderData);
            idx       += sizeof(XLogPageHeaderData);

            read_size = MIN(read_size,UsableBytesInPage);
        }
        else
        {
            read_size = MIN(read_size,GTM_XLOG_BLCKSZ - page_offset);
        }

        if(redo_pos == InvalidXLogRecPtr)
            redo_pos = segment_no * GTM_XLOG_SEG_SIZE + idx;

        memcpy(xlog_rec + rec_offset,xlog_buff,read_size);

        idx           += read_size;
        xlog_buff     += read_size;
        desired_bytes -= read_size;
        bytes_read    += read_size;
        rec_offset    += read_size;

        if(!read_header && bytes_read >= sizeof(XLogRecord))
        {
            read_header   = true;
            desired_bytes = record_header->xl_tot_len;

            printf("Read header %X/%X wiht bytes %lld \n",(uint32)(redo_pos >> 32),(uint32)redo_pos,desired_bytes);
        }

        if(desired_bytes == 0)
        {
            printf("Get one record at %lld length %lld rec_crc: %u\n",redo_pos,record_header->xl_tot_len,record_header->xl_crc);

            INIT_CRC32C(crc);
            COMP_CRC32C(crc,xlog_rec + sizeof(XLogRecord),record_header->xl_tot_len);
            COMP_CRC32C(crc,record_header,offsetof(XLogRecord,xl_crc));
            FIN_CRC32C(crc);

            //PrintMemory(xlog_rec + sizeof(XLogRecord),record_header->xl_tot_len);
            //PrintMemory((char *)xlog_rec,offsetof(XLogRecord,xl_crc));

            Assert(redo_pos != InvalidXLogRecPtr);

            printf("caled crc %lld\n",crc);
            if(crc != record_header->xl_crc)
            {
                printf("Xlog %X/%X crc validation fails\n",(uint32)(redo_pos >> 32),(uint32)redo_pos);
                break;
            }

            if(preXLogRecord != InvalidXLogRecPtr && preXLogRecord != record_header->xl_prev)
            {
                printf("Xlog %X/%X validation prelink fails recorded : %X/%X calculated: %X/%X\n",(uint32)(redo_pos >> 32),(uint32)redo_pos,
                        (uint32)(record_header->xl_prev >> 32),(uint32)record_header->xl_prev,(uint32)(preXLogRecord >> 32),(uint32)preXLogRecord);
                break;
            }

            ReadRedoXLogRecord(record_header);

            printf("\n##############################\n");

            redo_end_pos  = segment_no * GTM_XLOG_SEG_SIZE + idx;

            desired_bytes = sizeof(XLogRecord);
            bytes_read    = 0;
            read_header   = false;
            preXLogRecord = redo_pos;
            redo_pos      = InvalidXLogRecPtr;
            rec_offset    = 0;
        }
    }

    printf("exit %d\n",cur_xlog_size);
}

    int
main(int argc, char *argv[])
{
    uint64 seg = atoi(argv[2]);
    if(seg == 0)
        Read_XLogRecovery(argv[1],seg,4096);
    else
        Read_XLogRecovery(argv[1],seg,0);
    return 0;
}

