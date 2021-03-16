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
 * proxy_main.c
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
#include <getopt.h>

#include "gtm/gtm_c.h"
#include "gtm/path.h"
#include "gtm/gtm_proxy.h"
#include "gtm/register.h"
#include "gtm/elog.h"
#include "gtm/memutils.h"
#include "gtm/gtm_list.h"
#include "gtm/libpq.h"
#include "gtm/libpq-be.h"
#include "gtm/libpq-fe.h"
#include "gtm/pqsignal.h"
#include "gtm/pqformat.h"
#include "gtm/assert.h"
#include "gtm/gtm_txn.h"
#include "gtm/gtm_seq.h"
#include "gtm/gtm_msg.h"
#include "gtm/libpq-int.h"
#include "gtm/gtm_ip.h"
#include "gtm/gtm_standby.h"
/* For reconnect control lock */
#include "gtm/gtm_lock.h"
#include "gtm/gtm_opt.h"

extern int    optind;
extern char *optarg;

#define GTM_MAX_PATH            1024
#define GTM_PROXY_DEFAULT_HOSTNAME    "*"
#define GTM_PROXY_DEFAULT_PORT        6666
#define GTM_PROXY_DEFAULT_WORKERS    2
#define GTM_PID_FILE            "gtm_proxy.pid"
#define GTM_LOG_FILE            "gtm_proxy.log"
#ifndef PROXY_CLIENT_TIMEOUT
#ifdef  GTM_DEBUG
#define PROXY_CLIENT_TIMEOUT    3600
#else
#define PROXY_CLIENT_TIMEOUT    20
#endif
#endif

static int poll_timeout_ms = 10;
static char *progname = "gtm_proxy";
char       *ListenAddresses;
int            GTMProxyPortNumber;
int            GTMProxyWorkerThreads;
char        *GTMProxyDataDir;
char        *GTMProxyConfigFileName;
char        *GTMConfigFileName;

char        *GTMServerHost;
int            GTMServerPortNumber;

int            GTMConnectRetryInterval = 60;

#ifdef __XLOG__
char *recovery_file_name;
#endif

/*
 * Keepalives setup for the connection with GTM server
 */
int    tcp_keepalives_idle = 0;
int    tcp_keepalives_interval = 0;
int tcp_keepalives_count = 0;

char *GTMProxyNodeName = NULL;
GTM_ThreadID    TopMostThreadID;

/* Communication area with SIGUSR2 signal handler */
GTMProxy_ThreadInfo **Proxy_ThreadInfo;
short    ReadyToReconnect = FALSE;
char    *NewGTMServerHost;
int        NewGTMServerPortNumber;

/* Status reader/reporter */
char    *error_reporter;
char    *status_reader;

/* Mode */
bool    isStartUp = false;

/* Reconnect Control Lock */
GTM_RWLock     ReconnectControlLock;
jmp_buf     mainThreadSIGUSR1_buf;
int            SIGUSR1Accepted = FALSE;

/* If this is GTM or not */
/*
 * Used to determine if given Port is in GTM or in GT_Proxy.
 * If it is in GTM, we should consider to flush GTM_Conn before
 * writing anything to Port.
 */
bool isGTM = false;

/* The socket(s) we're listening to. */
#define MAXLISTEN    64
static int    ListenSocket[MAXLISTEN];

pthread_key_t    threadinfo_key;
static bool        GTMProxyAbortPending = false;
static GTM_Conn *master_conn;


/*
 * External Routines
 */
extern void InitializeGTMOptions(void);


/*
 * Internal Routines
 */
static Port *ConnCreate(int serverFd);
static void ConnFree(Port *conn);
static int ServerLoop(void);
static int initMasks(fd_set *rmask);
void *GTMProxy_ThreadMain(void *argp);
static int GTMProxyAddConnection(Port *port);
static int ReadCommand(GTMProxy_ConnectionInfo *conninfo, StringInfo inBuf);
static void GTMProxy_HandshakeConnection(GTMProxy_ConnectionInfo *conninfo);
static void GTMProxy_HandleDisconnect(GTMProxy_ConnectionInfo *conninfo, GTM_Conn *gtm_conn);

static void GTMProxy_ProxyCommand(GTMProxy_ConnectionInfo *conninfo,
        GTM_Conn *gtm_conn, GTM_MessageType mtype, StringInfo message);

static void ProcessCommand(GTMProxy_ConnectionInfo *conninfo,
        GTM_Conn *gtm_conn, StringInfo input_message);
static GTM_Conn *HandleGTMError(GTM_Conn *gtm_conn);
static GTM_Conn *HandlePostCommand(GTMProxy_ConnectionInfo *conninfo, GTM_Conn *gtm_conn);
static void ProcessTransactionCommand(GTMProxy_ConnectionInfo *conninfo,
        GTM_Conn *gtm_conn, GTM_MessageType mtype, StringInfo message);
static void ProcessSnapshotCommand(GTMProxy_ConnectionInfo *conninfo,
        GTM_Conn *gtm_conn, GTM_MessageType mtype, StringInfo message);

static void GTMProxy_RegisterPGXCNode(GTMProxy_ConnectionInfo *conninfo,
                                      char *node_name,
                                      GTM_PGXCNodeType remote_type,
                                      bool is_postmaster);

static void ProcessResponse(GTMProxy_ThreadInfo *thrinfo,
        GTMProxy_CommandInfo *cmdinfo, GTM_Result *res);

static void GTMProxy_ProcessPendingCommands(GTMProxy_ThreadInfo *thrinfo);
static void GTMProxy_CommandPending(GTMProxy_ConnectionInfo *conninfo,
        GTM_MessageType mtype, GTMProxy_CommandData cmd_data);

static bool CreateOptsFile(int argc, char *argv[]);
static void CreateDataDirLockFile(void);
void CreateLockFile(const char *filename, const char *refName);
static void SetDataDir(void);
static void ChangeToDataDir(void);
static void checkDataDir(void);
void DeleteLockFile(const char *filename);
static void RegisterProxy(bool is_reconnect);
static void UnregisterProxy(void);
static GTM_Conn *ConnectGTM(void);
static void ReleaseCmdBackup(GTMProxy_CommandInfo *cmdinfo);
static void workerThreadReconnectToGTM(void);
#ifdef USE_ASSERT_CHECKING
static bool IsProxiedMessage(GTM_MessageType mtype);
#endif

/*
 * One-time initialization. It's called immediately after the main process
 * starts
 */
static GTMProxy_ThreadInfo *
MainThreadInit()
{
    GTMProxy_ThreadInfo *thrinfo;

    pthread_key_create(&threadinfo_key, NULL);

    /*
     * Initialize the lock protecting the global threads info
     */
    GTM_RWLockInit(&GTMProxyThreads->gt_lock);

    /*
     * We are called even before memory context management is setup. We must
     * use malloc
     */
    thrinfo = (GTMProxy_ThreadInfo *)malloc(sizeof (GTMProxy_ThreadInfo));

    if (thrinfo == NULL)
    {
        fprintf(stderr, "malloc failed: %d", errno);
        fflush(stdout);
        fflush(stderr);
        exit(1);
    }

    memset((char *)thrinfo, 0, sizeof(GTMProxy_ThreadInfo));

    if (SetMyThreadInfo(thrinfo))
    {
        fprintf(stderr, "SetMyThreadInfo failed: %d", errno);
        fflush(stdout);
        fflush(stderr);
        exit(1);
    }

    TopMostThreadID = pthread_self();

    return thrinfo;
}

/*
 * Bare minimum supporting infrastructure. Must be called at the very beginning
 * so that further initilization can have it ready
 */
static void
InitGTMProxyProcess()
{
    GTMProxy_ThreadInfo *thrinfo = MainThreadInit();
    MyThreadID = pthread_self();
    MemoryContextInit();

    /*
     * The memory context is now set up.
     * Add the thrinfo structure in the global array
     */
    if (GTMProxy_ThreadAdd(thrinfo) == -1)
    {
        fprintf(stderr, "GTMProxy_ThreadAdd for main thread failed: %d", errno);
        fflush(stdout);
        fflush(stderr);
    }
}

static void
BaseInit()
{
    checkDataDir();
    SetDataDir();
    ChangeToDataDir();
    CreateDataDirLockFile();

    if (GTMLogFile == NULL)
    {
        GTMLogFile = (char *) malloc(GTM_MAX_PATH);
        sprintf(GTMLogFile, "%s/%s", GTMProxyDataDir, GTM_LOG_FILE);
    }

    /* Initialize reconnect control lock */

    GTM_RWLockInit(&ReconnectControlLock);

    /* Register Proxy on GTM */
    RegisterProxy(false);

    DebugFileOpen();
}

static char *
read_token(char *line, char **next)
{// #lizard forgives
    char *tok;
    char *next_token;

    if (line == NULL)
    {
        *next = NULL;
        return(NULL);
    }
    for (tok = line;; tok++)
    {
        if (*tok == 0 || *tok == '\n')
            return(NULL);
        if (*tok == ' ' || *tok == '\t')
            continue;
        else
            break;
    }
    for (next_token = tok;; next_token++)
    {
        if (*next_token == 0 || *next_token == '\n')
        {
            *next_token = 0;
            *next = NULL;
            return(tok);
        }
        if (*next_token == ' ' || *next_token == '\t')

        {
            *next_token = 0;
            *next = next_token + 1;
            return(tok);
        }
        else
            continue;
    }
    Assert(0);        /* Never comes here.  Keep compiler quiet. */
}

/*
 * Returns non-zero if failed.
 * We assume that current working directory is that specified by -D option.
 */
#define MAXLINE 1024
#define INVALID_RECONNECT_OPTION_MSG() \
    do{ \
        ereport(ERROR, (0, errmsg("Invalid Reconnect Option"))); \
    } while(0)

static int
GTMProxy_ReadReconnectInfo(void)
{// #lizard forgives

    char optstr[MAXLINE];
    char *line;
    FILE *optarg_file;
    char *optValue;
    char *option;
    char *next_token;

    optarg_file = fopen("newgtm", "r");
    if (optarg_file == NULL)
    {
        INVALID_RECONNECT_OPTION_MSG();
        return(-1);
    }
    line = fgets(optstr, MAXLINE, optarg_file);
    if (line == NULL)
    {
        INVALID_RECONNECT_OPTION_MSG();
        return(-1);
    }
    fclose(optarg_file);

    elog(LOG, "reconnect option = \"%s\"\n", optstr);

    next_token = optstr;
    while ((option = read_token(next_token, &next_token)))
    {
        if (strcmp(option, "-t") == 0)    /* New GTM port */
        {
            optValue = read_token(next_token, &next_token);
            if (optValue == NULL)
            {
                INVALID_RECONNECT_OPTION_MSG();
                return(-1);
            }
            NewGTMServerPortNumber = atoi(optValue);
            continue;
        }
        else if (strcmp(option, "-s") == 0)
        {
            optValue = read_token(next_token, &next_token);
            if (optValue == NULL)
            {
                INVALID_RECONNECT_OPTION_MSG();
                return(-1);
            }
            if (NewGTMServerHost)
                free(NewGTMServerHost);
            NewGTMServerHost = strdup(optValue);
            continue;
        }
        else
        {
            INVALID_RECONNECT_OPTION_MSG();
            return(-1);
        }
    }
    return(0);
}

static void
GTMProxy_SigleHandler(int signal)
{// #lizard forgives
    int ii;

    elog(DEBUG1, "Received signal %d\n", signal);

    switch (signal)
    {
        case SIGKILL:
        case SIGTERM:
        case SIGQUIT:
        case SIGINT:
        case SIGHUP:
            break;
        case SIGUSR1:    /* Reconnect from gtm_ctl */
            /*
             * Only the main thread can distribute SIGUSR2 to avoid lock contention
             * of the thread info. If an other thread receives SIGUSR1, it will proxy
             * SIGUSR1 to the main thread.
             *
             * The mask is set to block signals.  They're blocked until all the
             * threads reconnect to the new GTM.
             */
            elog(DEBUG1, "Accepted SIGUSR1\n");
            if (MyThreadID != TopMostThreadID)
            {

                elog(DEBUG1, "Not on main thread, proxy the signal to the main thread.");

                pthread_kill(TopMostThreadID, SIGUSR1);
                return;
            }
            /*
             * Then this is the main thread.
             */
            PG_SETMASK(&BlockSig);

            elog(DEBUG1, "I'm the main thread. Accepted SIGUSR1.");

            /*
             * Set Reconnect Info
             */
            if (!ReadyToReconnect)
            {
                elog(DEBUG1, "SIGUSR1 detected, but not ready to handle this. Ignored");
                PG_SETMASK(&UnBlockSig);
                return;
            }
            elog(DEBUG1, "SIGUSR1 detected. Set reconnect info for each worker thread");
            if (GTMProxy_ReadReconnectInfo() != 0)
            {
                /* Failed to read reconnect information from reconnect data file */
                PG_SETMASK(&UnBlockSig);
                return;
            }
            /*
             * Send SIGUSR2 to all worker threads.
             * Check if all the worker threads can accept SIGUSR2
             */
            for (ii = 0; ii < GTMProxyWorkerThreads; ii++)
            {
                if ((Proxy_ThreadInfo[ii] == NULL) ||
                    (Proxy_ThreadInfo[ii]->can_accept_SIGUSR2 == FALSE))
                {
                    elog(NOTICE, "Some worker thread is not ready to handle this. Retry reconnection later.\n");
                    PG_SETMASK(&UnBlockSig);
                    return;
                }
            }
            /*
             * Before send SIGUSR2 to worker threads, acquire reconnect control lock in write mode
             * so that worker threads wait until main thread reconnects to new GTM and register
             * itself.
             */
            GTM_RWLockAcquire(&ReconnectControlLock, GTM_LOCKMODE_WRITE);

            /* We cannot accept the next SIGUSR1 until all the reconnect is finished. */
            ReadyToReconnect = false;

            /*
             * Issue SIGUSR2 to all the worker threads.
             * It will not be issued to the main thread.
             */
            for (ii = 0; ii < GTMProxyWorkerThreads; ii++)
                pthread_kill(Proxy_ThreadInfo[ii]->thr_id, SIGUSR2);

            elog(DEBUG1, "SIGUSR2 issued to all the worker threads.");
            PG_SETMASK(&UnBlockSig);

            /*
             * Note that during connection handling with backends, signals are blocked
             * so it is safe to longjump here.
             */
            siglongjmp(mainThreadSIGUSR1_buf, 1);

        case SIGUSR2:  /* Reconnect from the main thread */
            /* Main thread has nothing to do twith this signal and should not receive this. */
            PG_SETMASK(&BlockSig);

            elog(DEBUG1, "Detected SIGUSR2, thread:%ld", (long) MyThreadID);

            if (MyThreadID == TopMostThreadID)
            {
                /* This should not be reached. Just in case. */

                elog(DEBUG1, "SIGUSR2 received by the main thread.  Ignoring.");

                PG_SETMASK(&UnBlockSig);
                return;
            }
            GetMyThreadInfo->reconnect_issued = TRUE;
            if (GetMyThreadInfo->can_longjmp)
            {
                siglongjmp(GetMyThreadInfo->longjmp_env, 1);
            }
            PG_SETMASK(&UnBlockSig);
            return;

        default:
            fprintf(stderr, "Unknown signal %d\n", signal);
            return;
    }

    /* Unregister Proxy on GTM */
    UnregisterProxy();

    /*
     * XXX We should do a clean shutdown here.
     */
    /* Delete pid file before shutting down */
    DeleteLockFile(GTM_PID_FILE);

    PG_SETMASK(&BlockSig);
    GTMProxyAbortPending = true;

    return;
}

/*
 * Help display should match
 */
static void
help(const char *progname)
{
    printf(_("This is the GTM proxy.\n\n"));
    printf(_("Usage:\n  %s [OPTION]...\n\n"), progname);
    printf(_("Options:\n"));
    printf(_("  -h hostname        GTM proxy hostname/IP\n"));
    printf(_("  -p port            GTM proxy port number\n"));
    printf(_("  -s hostname        GTM server hostname/IP \n"));
    printf(_("  -t port            GTM server port number\n"));
    printf(_("  -i nodename     GTM proxy nodename\n"));
    printf(_("  -n count        Number of worker threads\n"));
    printf(_("  -D directory    GTM proxy working directory\n"));
    printf(_("  -l filename        GTM proxy log file name \n"));
    printf(_("  --help          show this help, then exit\n"));
}


int
main(int argc, char *argv[])
{// #lizard forgives
    int            opt;
    int            status;
    int            i;

    /*
     * Variable to store option parameters
     */
    char    *listen_addresses = NULL;
    char    *node_name = NULL;
    char       *proxy_port_number = NULL;
    char    *proxy_worker_threads = NULL;
    char    *data_dir = NULL;
    char    *log_file = NULL;
    char    *gtm_host = NULL;
    char    *gtm_port = NULL;

    isStartUp = true;

    /*
     * At first, initialize options.
     */
    InitializeGTMOptions();

    /* 
     * Also initialize bare minimum supporting infrastructure such as memory
     * context and thread control structure
     */
    InitGTMProxyProcess();

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

    ListenAddresses = strdup(GTM_PROXY_DEFAULT_HOSTNAME);
    GTMProxyPortNumber = GTM_PROXY_DEFAULT_PORT;
    GTMProxyWorkerThreads = GTM_PROXY_DEFAULT_WORKERS;

    NewGTMServerHost = NULL;

    /*
     * Parse the command like options and set variables
     */
    while ((opt = getopt(argc, argv, "h:i:p:n:D:l:s:t:")) != -1)
    {
        switch (opt)
        {
            case 'h':
                /* Listen address of the proxy */
                if (listen_addresses)
                    free(listen_addresses);
                listen_addresses = strdup(optarg);
                break;

            case 'i':
                /* GTM Proxy identification name */
                if (node_name)
                    free(node_name);
                node_name = strdup(optarg);
                break;

            case 'p':
                /* Port number for the proxy to listen on */
                if (proxy_port_number)
                    free(proxy_port_number);
                proxy_port_number = strdup(optarg);
                break;

            case 'n':
                /* Number of worker threads */
                if (proxy_worker_threads)
                    free(proxy_worker_threads);
                proxy_worker_threads = strdup(optarg);
                break;

            case 'D':
                if (data_dir)
                    free(data_dir);
                data_dir = strdup(optarg);
                canonicalize_path(data_dir);
                break;

            case 'l':
                /* The log file */
                if (log_file)
                    free(log_file);
                log_file = strdup(optarg);
                break;

            case 's':
                /* GTM server host name */
                if (gtm_host)
                    free(gtm_host);
                gtm_host = strdup(optarg);
                break;

            case 't':
                /* GTM server port number */
                if (gtm_port)
                    free(gtm_port);
                gtm_port = strdup(optarg);
                break;

            default:
                write_stderr("Try \"%s --help\" for more information.\n",
                             progname);
        }
    }

    /*
     * Setup working directory
     */
    if (data_dir)
        SetConfigOption("data_dir", data_dir, GTMC_STARTUP, GTMC_S_OVERRIDE);

    /*
     * Setup configuration file
     */
    if (!SelectConfigFiles(data_dir, progname))
        exit(1);

    /*
     * Parse config file
     */
    ProcessConfigFile(GTMC_STARTUP);

    /*
     * Override with command line options.   "data_dir" was handled in the privious line.
     */
    if (listen_addresses)
    {
        SetConfigOption(GTM_OPTNAME_LISTEN_ADDRESSES, listen_addresses, GTMC_STARTUP, GTMC_S_OVERRIDE);
        free(listen_addresses);
        listen_addresses = NULL;
    }
    if (node_name)
    {
        SetConfigOption(GTM_OPTNAME_NODENAME, node_name, GTMC_STARTUP, GTMC_S_OVERRIDE);
        free(node_name);
        node_name = NULL;
    }
    if (proxy_port_number)
    {
        SetConfigOption(GTM_OPTNAME_PORT, proxy_port_number, GTMC_STARTUP, GTMC_S_OVERRIDE);
        free(proxy_port_number);
        proxy_port_number = NULL;
    }
    if (proxy_worker_threads)
    {
        SetConfigOption(GTM_OPTNAME_WORKER_THREADS, proxy_worker_threads, GTMC_STARTUP, GTMC_S_OVERRIDE);
        free(proxy_worker_threads);
        proxy_worker_threads = NULL;
    }
    if (log_file)
    {
        SetConfigOption(GTM_OPTNAME_LOG_FILE, log_file, GTMC_STARTUP, GTMC_S_OVERRIDE);
        free(log_file);
        log_file = NULL;
    }
    if (gtm_host)
    {
        SetConfigOption(GTM_OPTNAME_GTM_HOST, gtm_host, GTMC_STARTUP, GTMC_S_OVERRIDE);
        free(gtm_host);
        gtm_host = NULL;
    }
    if (gtm_port)
    {
        SetConfigOption(GTM_OPTNAME_GTM_PORT, gtm_port, GTMC_STARTUP, GTMC_S_OVERRIDE);
        free(gtm_port);
        gtm_port = NULL;
    }


    /*
     * Check Options
     */
    if (GTMProxyDataDir == NULL)
    {
        write_stderr("GTM Proxy data directory must be specified\n");
        write_stderr("Try \"%s --help\" for more information.\n",
                     progname);
        exit(1);
    }
    if (GTMProxyNodeName == NULL)
    {
        write_stderr("GTM Proxy Node name must be specified\n");
        write_stderr("Try \"%s --help\" for more information.\n",
                     progname);
        exit(1);
    }
    if (ListenAddresses == NULL || *ListenAddresses == '\0')
    {
        write_stderr("GTM Proxy listen addresses must be specified\n");
        write_stderr("Try \"%s --help\" for more information.\n",
                     progname);
        exit(1);
    }
    if (GTMProxyPortNumber == 0)
    {
        write_stderr("GTM Proxy port number must be specified\n");
        write_stderr("Try \"%s --help\" for more information.\n",
                     progname);
        exit(1);

    }
    if (GTMServerHost == NULL || *GTMServerHost == '\0')
    {
        write_stderr("GTM server listen address must be specified\n");
        write_stderr("Try \"%s --help\" for more information.\n",
                     progname);
        exit(1);

    }
    if (GTMServerPortNumber == 0)
    {
        write_stderr("GTM server port number must be specified\n");
        write_stderr("Try \"%s --help\" for more information.\n",
                     progname);
        exit(1);

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
     * Some basic initialization must happen before we do anything
     * useful
     */
    BaseInit();

    elog(LOG, "Starting GTM proxy at (%s:%d)", ListenAddresses, GTMProxyPortNumber);

    /*
     * Establish input sockets.
     */
    for (i = 0; i < MAXLISTEN; i++)
        ListenSocket[i] = -1;

    if (ListenAddresses)
    {
        int            success = 0;

        if (strcmp(ListenAddresses, "*") == 0)
            status = StreamServerPort(AF_UNSPEC, NULL,
                                      (unsigned short) GTMProxyPortNumber,
									  NULL,
                                      ListenSocket, MAXLISTEN);
        else
            status = StreamServerPort(AF_UNSPEC, ListenAddresses,
                                      (unsigned short) GTMProxyPortNumber,
                                      NULL,
                                      ListenSocket, MAXLISTEN);

        if (status == STATUS_OK)
            success++;
        else
            ereport(FATAL,
                    (errmsg("could not create listen socket for \"%s\"",
                            ListenAddresses)));
    }

    /*
     * check that we have some socket to listen on
     */
    if (ListenSocket[0] == -1)
        ereport(FATAL,
                (errmsg("no socket created for listening")));

    /*
     * Record gtm proxy options.  We delay this till now to avoid recording
     * bogus options
     */
    if (!CreateOptsFile(argc, argv))
        exit(1);

    pqsignal(SIGHUP, GTMProxy_SigleHandler);
    pqsignal(SIGKILL, GTMProxy_SigleHandler);
    pqsignal(SIGQUIT, GTMProxy_SigleHandler);
    pqsignal(SIGTERM, GTMProxy_SigleHandler);
    pqsignal(SIGINT, GTMProxy_SigleHandler);
    pqsignal(SIGUSR1, GTMProxy_SigleHandler);
    pqsignal(SIGUSR2, GTMProxy_SigleHandler);
    pqsignal(SIGPIPE, SIG_IGN);

    pqinitmask();

    /*
     * Initialize SIGUSR2 interface area (Thread info)
     */
    Proxy_ThreadInfo = palloc0(sizeof(GTMProxy_ThreadInfo *) * GTMProxyWorkerThreads);

    /*
     * Pre-fork so many worker threads
     */
    for (i = 0; i < GTMProxyWorkerThreads; i++)
    {
        /*
         * XXX Start the worker thread
         */
        if (GTMProxy_ThreadCreate(GTMProxy_ThreadMain, i) == NULL)
        {
            elog(ERROR, "failed to create a new thread");
            return STATUS_ERROR;
        }
    }

    /*
     * Accept any new connections. Add for each incoming connection to one of
     * the pre-forked threads.
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
        return NULL;
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
static void
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

    nSockets = initMasks(&readmask);

    for (;;)
    {
        fd_set        rmask;
        int            selres;

        if (sigsetjmp(mainThreadSIGUSR1_buf, 1) != 0)
        {
            /*
             * Reconnect!
             * Use RegisterProxy() call. Before this, change connection information
             * of GTM to the new one.
             * Because this is done while ReconnectControlLock is acquired,
             * worker threads can use this change and they don't have to worry about
             * new connection point.
             *
             * Because we leave the old socket as is, there could be some waste of
             * the resource but this may not happen so many times.
             */

            elog(DEBUG1, "Main Thread reconnecting to new GTM.");
            RegisterProxy(TRUE);
            elog(DEBUG1, "Reconnected.");

            /* If it is done, then release the lock for worker threads. */
            GTM_RWLockRelease(&ReconnectControlLock);
        }
        /*
         * Delay the point to accept reconnect until here because
         * longjmp buffer has not been prepared.
         */
        ReadyToReconnect = TRUE;

        /*
         * Wait for a connection request to arrive.
         *
         * Wait at most one minute, to ensure that the other background
         * tasks handled below get done even when no requests are arriving.
         */
        memcpy((char *) &rmask, (char *) &readmask, sizeof(fd_set));

        PG_SETMASK(&UnBlockSig);

        if (GTMProxyAbortPending)
        {
            /*
             * Tell everybody that we are shutting down
             *
             * !! TODO
             */
            exit(1);
        }

        {
            /* must set timeout each time; some OSes change it! */
            struct timeval timeout;

            timeout.tv_sec = 60;
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
                ereport(DEBUG1,
                        (EACCES,
                         errmsg("select() failed in main thread: %m")));
                return STATUS_ERROR;
            }
        }

        /*
         * New connection pending on any of our sockets? If so, accept the
         * connection and add it to one of the worker threads.
         */
        if (selres > 0)
        {
            int            i;

            for (i = 0; i < MAXLISTEN; i++)
            {
                if (ListenSocket[i] == -1)
                    break;
                if (FD_ISSET(ListenSocket[i], &rmask))
                {
                    Port       *port;

                    port = ConnCreate(ListenSocket[i]);
                    if (port)
                    {
                        if (GTMProxyAddConnection(port) != STATUS_OK)
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

/*
 * The main worker thread routine
 */
void *
GTMProxy_ThreadMain(void *argp)
{// #lizard forgives
    GTMProxy_ThreadInfo *thrinfo = (GTMProxy_ThreadInfo *)argp;
    int qtype;
    StringInfoData input_message;
    sigjmp_buf  local_sigjmp_buf;
    int32 saved_seqno = -1;
    int ii, nrfds;
    char gtm_connect_string[1024];
    int    first_turn = TRUE;    /* Used only to set longjmp target at the first turn of thread loop */
    GTMProxy_CommandData cmd_data = {};

    elog(DEBUG3, "Starting the connection helper thread");

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

    /*
     * Set up connection with the GTM server
     */
    sprintf(gtm_connect_string, "host=%s port=%d node_name=%s remote_type=%d",
            GTMServerHost, GTMServerPortNumber, GTMProxyNodeName, GTM_NODE_GTM_PROXY);

    thrinfo->thr_gtm_conn = PQconnectGTM(gtm_connect_string);

    if (thrinfo->thr_gtm_conn == NULL)
        elog(FATAL, "GTM connection failed");

    /*
     * Get the input_message in the TopMemoryContext so that we don't need to
     * free/palloc it for every incoming message. Unlike Postgres, we don't
     * expect the incoming messages to be of arbitrary sizes
     */

    initStringInfo(&input_message);

    thrinfo->reconnect_issued = FALSE;

    /*
     * Initialize comand backup area
     */
    for (ii = 0; ii < GTM_PROXY_MAX_CONNECTIONS; ii++)
    {
        thrinfo->thr_any_backup[ii] = FALSE;
        thrinfo->thr_qtype[ii] = 0;
        initStringInfo(&(thrinfo->thr_inBufData[ii]));
    }

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
        /*
         * NOTE: if you are tempted to add more code in this if-block,
         * consider the high probability that it should be in
         * AbortTransaction() instead.    The only stuff done directly here
         * should be stuff that is guaranteed to apply *only* for outer-level
         * error recovery, such as adjusting the FE/BE protocol status.
         */

        /* Report the error to the client and/or server log */
        if (thrinfo->thr_conn_count > 0)
        {
            for (ii = 0; ii < thrinfo->thr_conn_count; ii++)
            {
                int connIndx = thrinfo->thr_conn_map[ii];
                GTMProxy_ConnectionInfo *conninfo = thrinfo->thr_all_conns[connIndx];
                
                /*
                 * Now clean up disconnected connections
                 */
                if (conninfo->con_disconnected)
                {
                    GTMProxy_ThreadRemoveConnection(thrinfo, conninfo);
                    pfree(conninfo);
                    ii--;
                }
                else
                {
                    /*
                     * Consume all the pending data on this connection and send
                     * error report
                     */
                    if (conninfo->con_pending_msg != MSG_TYPE_INVALID)
                    {
                        conninfo->con_port->PqRecvPointer = conninfo->con_port->PqRecvLength = 0;
                        conninfo->con_pending_msg = MSG_TYPE_INVALID;
                        EmitErrorReport(conninfo->con_port);
                    }
                }
            }
        }
        else
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

    /*
     * Now we're entering thread loop. The last work is to initialize SIGUSR2 control.
     */
    Disable_Longjmp();
    GetMyThreadInfo->can_accept_SIGUSR2 = TRUE;
    GetMyThreadInfo->reconnect_issued = FALSE;
    GetMyThreadInfo->can_longjmp = FALSE;

    /*--------------------------------------------------------------
     * Thread Loop
     *-------------------------------------------------------------
     */
    for (;;)
    {
        gtm_ListCell *elem = NULL;
        GTM_Result *res = NULL;

        /*
         * Release storage left over from prior query cycle, and create a new
         * query input buffer in the cleared MessageContext.
         */
        MemoryContextSwitchTo(MessageContext);
        MemoryContextResetAndDeleteChildren(MessageContext);

        /*
         * The following block should be skipped at the first turn.
         */
        if (!first_turn)
        {
            /*
             * Check if there are any changes to the connection array assigned to
             * this thread. If so, we need to rebuild the fd array.
             */
            GTM_MutexLockAcquire(&thrinfo->thr_lock);
            if (saved_seqno != thrinfo->thr_seqno)
            {
                saved_seqno = thrinfo->thr_seqno;

                while (thrinfo->thr_conn_count <= 0)
                {
                    /*
                     * No connections assigned to the thread. Wait for at least one
                     * connection to be assigned to us
                     */
                    if (sigsetjmp(GetMyThreadInfo->longjmp_env, 1) == 0)
                    {
                        Enable_Longjmp();
                        GTM_CVWait(&thrinfo->thr_cv, &thrinfo->thr_lock);
                        Disable_Longjmp();
                    }
                    else
                    {
                        /* SIGUSR2 here */
                        workerThreadReconnectToGTM();
                    }
                }

                memset(thrinfo->thr_poll_fds, 0, sizeof (thrinfo->thr_poll_fds));

                /*
                 * Now grab all the open connections. A lock is being hold so no
                 * new connections can be added.
                 */
                for (ii = 0; ii < thrinfo->thr_conn_count; ii++)
                {
                    int connIndx = thrinfo->thr_conn_map[ii];
                    GTMProxy_ConnectionInfo *conninfo = thrinfo->thr_all_conns[connIndx];

                    /*
                     * Detect if the connection has been dropped to avoid
                     * a segmentation fault.
                     */
                    if (conninfo->con_port == NULL)
                    {
                        conninfo->con_disconnected = true;
                        continue;
                    }

                    /*
                     * If this is a newly added connection, complete the handshake
                     */
                    if (!conninfo->con_authenticated)
                        GTMProxy_HandshakeConnection(conninfo);

                    thrinfo->thr_poll_fds[ii].fd = conninfo->con_port->sock;
                    thrinfo->thr_poll_fds[ii].events = POLLIN;
                    thrinfo->thr_poll_fds[ii].revents = 0;
                }
            }
            GTM_MutexLockRelease(&thrinfo->thr_lock);

            while (true)
            {
                Enable_Longjmp();
                nrfds = poll(thrinfo->thr_poll_fds, thrinfo->thr_conn_count,
                            poll_timeout_ms);
                Disable_Longjmp();

                if (nrfds < 0)
                {
                    if (errno == EINTR)
                        continue;
                    elog(FATAL, "poll returned with error %d", nrfds);
                }
                else
                    break;
            }

            if (nrfds == 0)
                continue;

            /*
             * Initialize the lists
             */
            thrinfo->thr_processed_commands = gtm_NIL;
            memset(thrinfo->thr_pending_commands, 0, sizeof (thrinfo->thr_pending_commands));
        }

        /*
         * Each SIGUSR2 should return here and please note that from the beginning
         * of the outer loop, longjmp is disabled and signal handler will simply return
         * so that we don't have to be botherd with the memory context. We should be
         * sure to be in MemoryContext where siglongjmp() is issued.
         */
setjmp_again:
        if (sigsetjmp(thrinfo->longjmp_env, 1) == 0)
        {
            Disable_Longjmp();
        }
        else
        {
            /*
             * SIGUSR2 is detected and jumped here
             * Reconnection phase
             */
            workerThreadReconnectToGTM();

            /*
             * Correction of pending works.
             */
            for (ii = 0; ii < MSG_TYPE_COUNT; ii++)
            {
                thrinfo->thr_pending_commands[ii] = gtm_NIL;
            }
            gtm_list_free_deep(thrinfo->thr_processed_commands);
            thrinfo->thr_processed_commands = gtm_NIL;
            goto setjmp_again;    /* Get ready for another SIGUSR2 */
        }
        if (first_turn)
        {
            first_turn = FALSE;
            continue;
        }

        /*
         * Just reset the input buffer to avoid repeated palloc/pfrees
         *
         * XXX We should consider resetting the MessageContext periodically to
         * handle any memory leaks
         */
        resetStringInfo(&input_message);

        /*
         * Now, read command from each of the connections that has some data to
         * be read.
         */
        for (ii = 0; ii < thrinfo->thr_conn_count; ii++)
        {
            int connIndx = thrinfo->thr_conn_map[ii];
            GTMProxy_ConnectionInfo *conninfo = thrinfo->thr_all_conns[connIndx];
            thrinfo->thr_conn = conninfo;

            if (thrinfo->thr_poll_fds[ii].revents & POLLHUP)
            {
                /*
                 * The fd has become invalid. The connection is broken. Add it
                 * to the remove_list and cleanup at the end of this round of
                 * cleanup.
                 */
                GTMProxy_CommandPending(thrinfo->thr_conn,
                            MSG_BACKEND_DISCONNECT, cmd_data);
                continue;
            }

            if ((thrinfo->thr_any_backup[connIndx]) ||
                (thrinfo->thr_poll_fds[ii].revents & POLLIN))
            {
                /*
                 * (3) read a command (loop blocks here)
                 */
                qtype = ReadCommand(thrinfo->thr_conn, &input_message);

                thrinfo->thr_poll_fds[ii].revents = 0;

                switch(qtype)
                {
                    case 'C':
                        ProcessCommand(thrinfo->thr_conn, thrinfo->thr_gtm_conn,
                                &input_message);
                        HandlePostCommand(thrinfo->thr_conn, thrinfo->thr_gtm_conn);
                        break;

                    case 'X':
                    case EOF:
                        /*
                         * Connection termination request
                         *
                         * Close the socket and remember the connection
                         * as disconnected. All such connections will be
                         * removed after the command processing is over. We
                         * can't remove it just yet because we pass the slot id
                         * to the server to quickly find the backend connection
                         * while processing proxied messages.
                         */
                        GTMProxy_CommandPending(thrinfo->thr_conn,
                                                MSG_BACKEND_DISCONNECT, cmd_data);
                        break;
                    default:
                        /*
                         * Also disconnect if protocol error
                         */
                        GTMProxy_HandleDisconnect(thrinfo->thr_conn, thrinfo->thr_gtm_conn);
                        elog(ERROR, "Unexpected message, or client disconnected abruptly.");
                        break;
                }

            }
        }

        /*
         * Ok. All the commands are processed. Commands which can be proxied
         * directly have been already sent to the GTM server. Now, group the
         * remaining commands, send them to the server and flush the data.
         */
        GTMProxy_ProcessPendingCommands(thrinfo);

        /*
         * Add a special marker to tell the GTM server that we are done with
         * one round of messages and the GTM server should flush all the
         * pending responses after seeing this message.
         */
        if (gtmpqPutMsgStart('F', true, thrinfo->thr_gtm_conn) ||
            gtmpqPutInt(MSG_DATA_FLUSH, sizeof (GTM_MessageType), thrinfo->thr_gtm_conn) ||
            gtmpqPutMsgEnd(thrinfo->thr_gtm_conn))
            elog(ERROR, "Error sending flush message");

        /*
         * Make sure everything is on wire now
         */
        Enable_Longjmp();
		if (gtmpqFlush(thrinfo->thr_gtm_conn))
        {
            elog(ERROR, "Error sending flush message");
        }
        Disable_Longjmp();

        /*
         * Read back the responses and put them on to the right backend
         * connection.
         */
        gtm_foreach(elem, thrinfo->thr_processed_commands)
        {
            GTMProxy_CommandInfo *cmdinfo = (GTMProxy_CommandInfo *)gtm_lfirst(elem);

            /*
             * If this is a continuation of a multi-part command response, we
             * don't need to read another result from the stream. The previous
             * result contains our response and we should just read from it.
             */
            if (cmdinfo->ci_res_index == 0)
            {
                Enable_Longjmp();
                if ((res = GTMPQgetResult(thrinfo->thr_gtm_conn)) == NULL)
                {
                    /*
                     * Here's another place to check GTM communication error.
                     * In this case, backup of each command will be taken care of
                     * by ProcessResponse() so if socket read/write error is recorded,
                     * disconnect GTM connection, retry connection and then if it faile,
                     * wait for reconnect from gtm_ctl.
                     */
                    if ((thrinfo->thr_gtm_conn->last_errno != 0) || (thrinfo->thr_gtm_conn->status == CONNECTION_BAD))
                    {
                        /*
                         * Please note that error handling can end up with longjmp() and
                         * may not return here.
                         */
                        HandleGTMError(thrinfo->thr_gtm_conn);
                    }
                    elog(ERROR, "GTMPQgetResult failed");
                }
                Disable_Longjmp();
            }

            ProcessResponse(thrinfo, cmdinfo, res);
        }

        gtm_list_free_deep(thrinfo->thr_processed_commands);
        thrinfo->thr_processed_commands = gtm_NIL;

        /*
         * Now clean up disconnected connections
         */
        for (ii = 0; ii < thrinfo->thr_conn_count; ii++)
        {
            int connIndx = thrinfo->thr_conn_map[ii];
            GTMProxy_ConnectionInfo *conninfo = thrinfo->thr_all_conns[connIndx];
            if (conninfo->con_disconnected)
            {
                GTMProxy_ThreadRemoveConnection(thrinfo, conninfo);
                pfree(conninfo);
                ii--;
            }
        }
    }

    /* can't get here because the above loop never exits */
    Assert(false);

    return thrinfo;
}

/*
 * Add the accepted connection to the pool
 */
static int
GTMProxyAddConnection(Port *port)
{
    GTMProxy_ConnectionInfo *conninfo = NULL;

    conninfo = (GTMProxy_ConnectionInfo *)palloc0(sizeof (GTMProxy_ConnectionInfo));

    if (conninfo == NULL)
    {
        ereport(LOG,
                (ENOMEM,
                    errmsg("Out of memory")));
        return STATUS_ERROR;
    }

    elog(DEBUG3, "Started new connection");
    conninfo->con_port = port;

    /*
     * Add the conninfo struct to the next worker thread in round-robin manner
     */
    if (!GTMProxy_ThreadAddConnection(conninfo))
        return STATUS_ERROR;

    return STATUS_OK;
}

void
ProcessCommand(GTMProxy_ConnectionInfo *conninfo, GTM_Conn *gtm_conn,
        StringInfo input_message)
{// #lizard forgives
    GTM_MessageType mtype;

    mtype = pq_getmsgint(input_message, sizeof (GTM_MessageType));

    switch (mtype)
    {
        case MSG_TXN_BEGIN_GETGXID_AUTOVACUUM:
        case MSG_TXN_PREPARE:
        case MSG_TXN_START_PREPARED:
        case MSG_TXN_GET_GID_DATA:
        case MSG_TXN_COMMIT_PREPARED:
        case MSG_SNAPSHOT_GET:
        case MSG_SEQUENCE_INIT:
        case MSG_SEQUENCE_GET_CURRENT:
        case MSG_SEQUENCE_GET_NEXT:
        case MSG_SEQUENCE_GET_LAST:
        case MSG_SEQUENCE_SET_VAL:
        case MSG_SEQUENCE_RESET:
        case MSG_SEQUENCE_CLOSE:
        case MSG_SEQUENCE_RENAME:
        case MSG_SEQUENCE_ALTER:
        case MSG_BARRIER:
        case MSG_TXN_COMMIT:
        case MSG_REGISTER_SESSION:
        case MSG_REPORT_XMIN:
        case MSG_NODE_REGISTER:
        case MSG_NODE_UNREGISTER:
#ifdef __TBASE__
        case MSG_TXN_FINISH_GID:
        case MSG_LIST_GTM_STORE:
        case MSG_LIST_GTM_STORE_SEQ:            /* List  gtm running sequence info */            
        case MSG_LIST_GTM_STORE_TXN:            /* List  gtm running transaction info */            
        case MSG_CHECK_GTM_STORE_SEQ:            /* Check gtm sequence usage info */            
        case MSG_CHECK_GTM_STORE_TXN:            /* Check gtm transaction usage info */
        case MSG_CLEAN_SESSION_SEQ:
            {
                break;
            }
#endif
            GTMProxy_ProxyCommand(conninfo, gtm_conn, mtype, input_message);
            break;

        case MSG_TXN_BEGIN:
        case MSG_TXN_BEGIN_GETGXID:
        case MSG_TXN_COMMIT_MULTI:
        case MSG_TXN_ROLLBACK:
        case MSG_TXN_GET_GXID:
            ProcessTransactionCommand(conninfo, gtm_conn, mtype, input_message);
            break;

        case MSG_SNAPSHOT_GET_MULTI:
        case MSG_SNAPSHOT_GXID_GET:
            ProcessSnapshotCommand(conninfo, gtm_conn, mtype, input_message);
            break;

        default:
            ereport(FATAL,
                    (EPROTO,
                     errmsg("invalid frontend message type %d",
                            mtype)));
    }

    conninfo->con_pending_msg = mtype;

}

/*
 * This funciton mainly takes care of GTM communcation error.
 *
 * Communication error was stored in last_errno chaned from GTMProxy_ConnectionInfo.
 * Note that it is set to zero if the last send/receive/read/write succeeds.
 *
 * If error is detected, then it tries to connect to GTM again, if it is
 * specified by configuration parameters.
 *
 * Relevant configuration parameters are: gtm_connect_retry_idle, gtm_connect_retry_count
 * and gtm_connect_retry_interval.
 *
 * If it is not successfull or configuration parameter does not specify it,
 * then, according to another confugration parameters, it waits "reconnect"
 * command from gtm_proxy.
 *
 * Relevant configuration parameters are: err_wait_idle, err_wait_count, and
 * err_wait_interval.
 */

static GTM_Conn *
HandleGTMError(GTM_Conn *gtm_conn)
{

    elog(NOTICE,
         "GTM communication error was detected.  Retrying connection, interval = %d.",
         GTMConnectRetryInterval);
    for (;;)
    {
        char gtm_connect_string[1024];

        /* Wait and retry reconnect */
        elog(DEBUG1, "Waiting %d secs.", GTMConnectRetryInterval);
        pg_usleep((long)GTMConnectRetryInterval * 1000000L);

        /*
         * Connect retry
         * Because this proxy has been registered to current
         * GTM, we don't re-register it.
         *
         * Please note that GTM-Proxy accepts "reconnect" from gtm_ctl
         * even while it is retrying to connect to GTM.
         */
        elog(DEBUG1, "Try to reconnect to GTM");
        /* Make sure RECONNECT command would not come while we reconnecting */
        Disable_Longjmp();
        /* Close and free previous connection object if still active */
        GTMPQfinish(gtm_conn);
        /* Reconnect */
        sprintf(gtm_connect_string, "host=%s port=%d node_name=%s remote_type=%d",
                GTMServerHost, GTMServerPortNumber, GTMProxyNodeName, GTM_NODE_GTM_PROXY);
        gtm_conn = PQconnectGTM(gtm_connect_string);
        /*
         * If reconnect succeeded the connection will be ready to use out of
         * there, otherwise thr_gtm_conn will be set to NULL preventing double
         * free.
         */
        GetMyThreadInfo->thr_gtm_conn = gtm_conn;
        Enable_Longjmp();
        if (gtm_conn)
        {
            /* Success, update thread info and return new connection */
            elog(NOTICE, "GTM connection retry succeeded.");
            return gtm_conn;
        }
        elog(NOTICE, "GTM connection retry failed.");
    }
}

static GTM_Conn *
HandlePostCommand(GTMProxy_ConnectionInfo *conninfo, GTM_Conn *gtm_conn)
{
    int        connIdx = conninfo->con_id;

    Assert(conninfo && gtm_conn);
    /*
     * Check if the response was handled without error.
     * In this case, use last_errno to detect the error
     * because system call error is only one case to detect GTM error.
     */
    if (gtm_conn->last_errno != 0)
    {
        return(HandleGTMError(gtm_conn));
    }
    else
    {
        /*
         * Command handled without error.  Clear the backup.
         */
        resetStringInfo(&GetMyThreadInfo->thr_inBufData[connIdx]);
        GetMyThreadInfo->thr_any_backup[connIdx] = FALSE;
        return(gtm_conn);
    }

}

#ifdef USE_ASSERT_CHECKING
static bool
IsProxiedMessage(GTM_MessageType mtype)
{// #lizard forgives
    switch (mtype)
    {
        case MSG_TXN_BEGIN:
        case MSG_TXN_BEGIN_GETGXID_AUTOVACUUM:
        case MSG_TXN_PREPARE:
        case MSG_TXN_START_PREPARED:
        /* There are not so many 2PC from application messages, so just proxy it. */
        case MSG_TXN_COMMIT_PREPARED:
        case MSG_TXN_GET_GXID:
        case MSG_TXN_GET_GID_DATA:
        case MSG_NODE_REGISTER:
        case MSG_NODE_UNREGISTER:
        case MSG_REGISTER_SESSION:
        case MSG_REPORT_XMIN:
        case MSG_SNAPSHOT_GXID_GET:
        case MSG_SEQUENCE_INIT:
        case MSG_SEQUENCE_GET_CURRENT:
        case MSG_SEQUENCE_GET_NEXT:
        case MSG_SEQUENCE_GET_LAST:
        case MSG_SEQUENCE_SET_VAL:
        case MSG_SEQUENCE_RESET:
        case MSG_SEQUENCE_CLOSE:
        case MSG_SEQUENCE_RENAME:
        case MSG_SEQUENCE_ALTER:
        case MSG_SNAPSHOT_GET:
        case MSG_TXN_COMMIT:
        case MSG_BARRIER:
#ifdef __TBASE__
        case MSG_TXN_FINISH_GID:
        case MSG_LIST_GTM_STORE:
        case MSG_LIST_GTM_STORE_SEQ:            /* List  gtm running sequence info */            
        case MSG_LIST_GTM_STORE_TXN:            /* List  gtm running transaction info */            
        case MSG_CHECK_GTM_STORE_SEQ:            /* Check gtm sequence usage info */            
        case MSG_CHECK_GTM_STORE_TXN:            /* Check gtm transaction usage info */
        case MSG_CLEAN_SESSION_SEQ:
#endif
            return true;

        default:
            return false;
    }
}
#endif

static void
ProcessResponse(GTMProxy_ThreadInfo *thrinfo, GTMProxy_CommandInfo *cmdinfo,
        GTM_Result *res)
{// #lizard forgives
    StringInfoData buf;
    GlobalTransactionId gxid;
    GTM_Timestamp timestamp;
    int    status;

    switch (cmdinfo->ci_mtype)
    {
        case MSG_TXN_BEGIN_GETGXID:
            /*
             * This is a grouped command. We send just the transaction count to
             * the GTM server which responds back with the start GXID. We
             * derive our GXID from the start GXID and the our position in the
             * command queue
             */
            if (res->gr_status == GTM_RESULT_OK)
            {
                if (res->gr_type != TXN_BEGIN_GETGXID_MULTI_RESULT)
                {
                    ReleaseCmdBackup(cmdinfo);
                    elog(ERROR, "Wrong result");
                }
                if (cmdinfo->ci_res_index >= res->gr_resdata.grd_txn_get_multi.txn_count)
                {
                    ReleaseCmdBackup(cmdinfo);
                    elog(ERROR, "Too few GXIDs");
                }

                gxid = res->gr_resdata.grd_txn_get_multi.txn_gxid[cmdinfo->ci_res_index];

                /* Send back to each client the same timestamp value asked in this message */
                timestamp = res->gr_resdata.grd_txn_get_multi.timestamp;

                pq_beginmessage(&buf, 'S');
                pq_sendint(&buf, TXN_BEGIN_GETGXID_RESULT, 4);
                pq_sendbytes(&buf, (char *)&gxid, sizeof (GlobalTransactionId));
                pq_sendbytes(&buf, (char *)&timestamp, sizeof (GTM_Timestamp));
                pq_endmessage(cmdinfo->ci_conn->con_port, &buf);
                pq_flush(cmdinfo->ci_conn->con_port);
            }
            else
            {
                pq_beginmessage(&buf, 'E');
                pq_sendbytes(&buf, res->gr_proxy_data, res->gr_msglen);
                pq_endmessage(cmdinfo->ci_conn->con_port, &buf);
                pq_flush(cmdinfo->ci_conn->con_port);
            }
            cmdinfo->ci_conn->con_pending_msg = MSG_TYPE_INVALID;
            ReleaseCmdBackup(cmdinfo);
            break;

        case MSG_TXN_COMMIT_MULTI:
            if (res->gr_type != TXN_COMMIT_MULTI_RESULT)
            {
                ReleaseCmdBackup(cmdinfo);
                elog(ERROR, "Wrong result");
            }
            /*
             * These are grouped messages. We send an array of GXIDs to commit
             * or rollback and the server sends us back an array of status
             * codes.
             */
            if (cmdinfo->ci_res_index >= res->gr_resdata.grd_txn_rc_multi.txn_count)
            {
                ReleaseCmdBackup(cmdinfo);
                elog(ERROR, "Too few GXIDs");
            }

            if (res->gr_resdata.grd_txn_rc_multi.status[cmdinfo->ci_res_index] == STATUS_OK)
            {
                int txn_count = 1;
                int status = STATUS_OK;

                pq_beginmessage(&buf, 'S');
                pq_sendint(&buf, TXN_COMMIT_MULTI_RESULT, 4);
                pq_sendbytes(&buf, (const char *)&txn_count, sizeof (int));
                pq_sendbytes(&buf, (const char *)&status, sizeof (int));
                pq_endmessage(cmdinfo->ci_conn->con_port, &buf);
                pq_flush(cmdinfo->ci_conn->con_port);
            }
            else
            {
                ReleaseCmdBackup(cmdinfo);
                ereport(ERROR2, (EINVAL, errmsg("Transaction commit failed")));
            }
            cmdinfo->ci_conn->con_pending_msg = MSG_TYPE_INVALID;
            ReleaseCmdBackup(cmdinfo);
            break;

        case MSG_TXN_ROLLBACK:
            if (res->gr_type != TXN_ROLLBACK_MULTI_RESULT)
            {
                ReleaseCmdBackup(cmdinfo);
                elog(ERROR, "Wrong result");
            }
            /*
             * These are grouped messages. We send an array of GXIDs to commit
             * or rollback and the server sends us back an array of status
             * codes.
             */
            if (cmdinfo->ci_res_index >= res->gr_resdata.grd_txn_rc_multi.txn_count)
            {
                ReleaseCmdBackup(cmdinfo);
                elog(ERROR, "Too few GXIDs");
            }

            if (res->gr_resdata.grd_txn_rc_multi.status[cmdinfo->ci_res_index] == STATUS_OK)
            {
                pq_beginmessage(&buf, 'S');
                pq_sendint(&buf, TXN_ROLLBACK_RESULT, 4);
                pq_sendbytes(&buf, (char *)&cmdinfo->ci_data.cd_rc.gxid, sizeof (GlobalTransactionId));
                pq_endmessage(cmdinfo->ci_conn->con_port, &buf);
                pq_flush(cmdinfo->ci_conn->con_port);
            }
            else
            {
                ReleaseCmdBackup(cmdinfo);
                ereport(ERROR2, (EINVAL, errmsg("Transaction commit failed")));
            }
            cmdinfo->ci_conn->con_pending_msg = MSG_TYPE_INVALID;
            ReleaseCmdBackup(cmdinfo);
            break;

        case MSG_SNAPSHOT_GET_MULTI:
            if ((res->gr_type != SNAPSHOT_GET_RESULT) &&
                (res->gr_type != SNAPSHOT_GET_MULTI_RESULT))
            {
                ReleaseCmdBackup(cmdinfo);
                elog(ERROR, "Wrong result");
            }

            if (cmdinfo->ci_res_index >= res->gr_resdata.grd_txn_snap_multi.txn_count)
            {
                ReleaseCmdBackup(cmdinfo);
                elog(ERROR, "Too few GXIDs - %d:%d", cmdinfo->ci_res_index,
                        res->gr_resdata.grd_txn_snap_multi.txn_count);
            }

            status = res->gr_resdata.grd_txn_snap_multi.status[cmdinfo->ci_res_index];
               if ((status == STATUS_OK) || (status == STATUS_NOT_FOUND))
            {
                int txn_count = 1;
                int status = STATUS_OK;

                pq_beginmessage(&buf, 'S');
                pq_sendint(&buf, SNAPSHOT_GET_MULTI_RESULT, 4);
                pq_sendbytes(&buf, (char *)&txn_count, sizeof (txn_count));
                pq_sendbytes(&buf, (char *)&status, sizeof (status));
                pq_sendbytes(&buf, (char *)&res->gr_snapshot.sn_xmin, sizeof (GlobalTransactionId));
                pq_sendbytes(&buf, (char *)&res->gr_snapshot.sn_xmax, sizeof (GlobalTransactionId));
                pq_sendint(&buf, res->gr_snapshot.sn_xcnt, sizeof (int));
                pq_sendbytes(&buf, (char *)res->gr_snapshot.sn_xip,
                             sizeof(GlobalTransactionId) * res->gr_snapshot.sn_xcnt);
                pq_endmessage(cmdinfo->ci_conn->con_port, &buf);
                pq_flush(cmdinfo->ci_conn->con_port);
            }
            else
            {
                ReleaseCmdBackup(cmdinfo);
                ereport(ERROR2, (EINVAL, errmsg("snapshot request failed")));
            }
            cmdinfo->ci_conn->con_pending_msg = MSG_TYPE_INVALID;
            ReleaseCmdBackup(cmdinfo);
            break;

        case MSG_TXN_BEGIN:
        case MSG_TXN_BEGIN_GETGXID_AUTOVACUUM:
        case MSG_TXN_PREPARE:
        case MSG_TXN_START_PREPARED:
        /* There are not so many 2PC from application messages, so just proxy it. */
        case MSG_TXN_COMMIT_PREPARED:
        case MSG_TXN_GET_GXID:
        case MSG_TXN_GET_GID_DATA:
        case MSG_NODE_REGISTER:
        case MSG_NODE_UNREGISTER:
        case MSG_REGISTER_SESSION:
        case MSG_REPORT_XMIN:
        case MSG_SNAPSHOT_GXID_GET:
        case MSG_SEQUENCE_INIT:
        case MSG_SEQUENCE_GET_CURRENT:
        case MSG_SEQUENCE_GET_NEXT:
        case MSG_SEQUENCE_GET_LAST:
        case MSG_SEQUENCE_SET_VAL:
        case MSG_SEQUENCE_RESET:
        case MSG_SEQUENCE_CLOSE:
        case MSG_SEQUENCE_RENAME:
        case MSG_SEQUENCE_ALTER:
        case MSG_SNAPSHOT_GET:
        case MSG_TXN_COMMIT:
#ifdef __TBASE__
        case MSG_TXN_FINISH_GID:
        case MSG_LIST_GTM_STORE:
        case MSG_LIST_GTM_STORE_SEQ:            /* List  gtm running sequence info */            
        case MSG_LIST_GTM_STORE_TXN:            /* List  gtm running transaction info */            
        case MSG_CHECK_GTM_STORE_SEQ:            /* Check gtm sequence usage info */            
        case MSG_CHECK_GTM_STORE_TXN:            /* Check gtm transaction usage info */
        case MSG_CLEAN_SESSION_SEQ:
#endif
            Assert(IsProxiedMessage(cmdinfo->ci_mtype));
            if ((res->gr_proxyhdr.ph_conid == InvalidGTMProxyConnID) ||
                (res->gr_proxyhdr.ph_conid >= GTM_PROXY_MAX_CONNECTIONS) ||
                (thrinfo->thr_all_conns[res->gr_proxyhdr.ph_conid] != cmdinfo->ci_conn))
            {
                ReleaseCmdBackup(cmdinfo);
                elog(PANIC, "Invalid response or synchronization loss");
            }

            /*
             * These are just proxied messages.. so just forward the response
             * back after stripping the conid part.
             *
             * !!TODO As we start adding support for message grouping for
             * messages, those message types would be removed from the above
             * and handled separately.
             */
            switch (res->gr_status)
            {
                case GTM_RESULT_OK:
                    pq_beginmessage(&buf, 'S');
                    pq_sendint(&buf, res->gr_type, 4);
                    pq_sendbytes(&buf, res->gr_proxy_data, res->gr_msglen);
                    pq_endmessage(cmdinfo->ci_conn->con_port, &buf);
                    pq_flush(cmdinfo->ci_conn->con_port);
                    break;

                default:
                    pq_beginmessage(&buf, 'E');
                    pq_sendbytes(&buf, res->gr_proxy_data, res->gr_msglen);
                    pq_endmessage(cmdinfo->ci_conn->con_port, &buf);
                    pq_flush(cmdinfo->ci_conn->con_port);
                    break;
            }
            cmdinfo->ci_conn->con_pending_msg = MSG_TYPE_INVALID;
            ReleaseCmdBackup(cmdinfo);
            break;

        case MSG_BARRIER:
            switch (res->gr_status)
            {
                case GTM_RESULT_OK:
                    pq_beginmessage(&buf, 'S');
                    pq_sendint(&buf, res->gr_type, 4);
                    pq_sendbytes(&buf, res->gr_proxy_data, res->gr_msglen);
                    pq_endmessage(cmdinfo->ci_conn->con_port, &buf);
                    pq_flush(cmdinfo->ci_conn->con_port);
                    break;

                default:
                    pq_beginmessage(&buf, 'E');
                    pq_sendbytes(&buf, res->gr_proxy_data, res->gr_msglen);
                    pq_endmessage(cmdinfo->ci_conn->con_port, &buf);
                    pq_flush(cmdinfo->ci_conn->con_port);
                    break;
            }
            cmdinfo->ci_conn->con_pending_msg = MSG_TYPE_INVALID;
            ReleaseCmdBackup(cmdinfo);
            break;
            
        default:
            ReleaseCmdBackup(cmdinfo);
            ereport(FATAL,
                    (EPROTO,
                     errmsg("invalid frontend message type %d",
                            cmdinfo->ci_mtype)));
    }
}

/* ----------------
 *        ReadCommand reads a command from either the frontend or
 *        standard input, places it in inBuf, and returns the
 *        message type code (first byte of the message).
 *        EOF is returned if end of file.
 * ----------------
 */
static int
ReadCommand(GTMProxy_ConnectionInfo *conninfo, StringInfo inBuf)
{
    int             qtype;
    int                connIdx = conninfo->con_id;
    int                anyBackup;

    anyBackup = (GetMyThreadInfo->thr_any_backup[connIdx] ? TRUE : FALSE);

    /*
     * Get message type code from the frontend.
     */
    if (!anyBackup)
    {
        qtype = pq_getbyte(conninfo->con_port);
        GetMyThreadInfo->thr_qtype[connIdx] = qtype;
        /*
         * We should not update thr_any_backup here.  This should be
         * updated when the backup is consumed or command processing
         * is done.
         */
    }
    else
    {
        qtype = GetMyThreadInfo->thr_qtype[connIdx];
    }

    if (qtype == EOF)            /* frontend disconnected */
    {
        /* don't fill up the proxy log with client disconnect messages */
        ereport(DEBUG1,
                (EPROTO,
                 errmsg("unexpected EOF on client connection")));
        return qtype;
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
    if (!anyBackup)
    {
        if (pq_getmessage(conninfo->con_port, inBuf, 0))
            return EOF;            /* suitable message already logged */

        copyStringInfo(&(GetMyThreadInfo->thr_inBufData[connIdx]), inBuf);

        /*
         * The next line is added because we added the code to clear backup
         * when the response is processed.
         */
        GetMyThreadInfo->thr_any_backup[connIdx] = TRUE;
    }
    else
    {
        copyStringInfo(inBuf, &(GetMyThreadInfo->thr_inBufData[connIdx]));
    }
    return qtype;
}

static void
ProcessTransactionCommand(GTMProxy_ConnectionInfo *conninfo, GTM_Conn *gtm_conn,
        GTM_MessageType mtype, StringInfo message)
{
    GTMProxy_CommandData cmd_data;
    uint32 global_sessionid_len;

    switch (mtype)
    {
        case MSG_TXN_BEGIN_GETGXID:
            cmd_data.cd_beg.iso_level = pq_getmsgint(message, sizeof (GTM_IsolationLevel));
            cmd_data.cd_beg.rdonly = pq_getmsgbyte(message);
            global_sessionid_len = pq_getmsgint(message, sizeof (uint32));
            memcpy(cmd_data.cd_beg.global_sessionid, pq_getmsgbytes(message,
                    global_sessionid_len), global_sessionid_len);
            GTMProxy_CommandPending(conninfo, mtype, cmd_data);
            break;

        case MSG_TXN_COMMIT_MULTI:
            {
                (void) pq_getmsgint(message, sizeof (int));
            }
            /* fall through */
        case MSG_TXN_ROLLBACK:
            {
                const char *data = pq_getmsgbytes(message,
                        sizeof (GlobalTransactionId));
                if (data == NULL)
                    ereport(ERROR,
                            (EPROTO,
                             errmsg("Message does not contain valid GXID")));
                memcpy(&cmd_data.cd_rc.gxid, data, sizeof (GlobalTransactionId));
            }
            pq_getmsgend(message);
            GTMProxy_CommandPending(conninfo, mtype, cmd_data);
            break;

        case MSG_TXN_BEGIN:
        case MSG_TXN_GET_GXID:
            elog(FATAL, "Support not yet added for these message types");
            break;

        default:
            Assert(0);            /* Shouldn't come here.. keep compiler quiet */
    }
}

static void
ProcessSnapshotCommand(GTMProxy_ConnectionInfo *conninfo, GTM_Conn *gtm_conn,
        GTM_MessageType mtype, StringInfo message)
{
    GTMProxy_CommandData cmd_data;

    switch (mtype)
    {
        case MSG_SNAPSHOT_GET_MULTI:
            {
                {
                    const char *data;

                    (void) pq_getmsgint(message, sizeof (int));
                    data = pq_getmsgbytes(message,
                            sizeof (GlobalTransactionId));
                    if (data == NULL)
                        ereport(ERROR,
                                (EPROTO,
                                 errmsg("Message does not contain valid GXID")));
                    memcpy(&cmd_data.cd_snap.gxid, data, sizeof (GlobalTransactionId));
                }
                pq_getmsgend(message);
                GTMProxy_CommandPending(conninfo, mtype, cmd_data);
            }
            break;

        case MSG_SNAPSHOT_GXID_GET:
            elog(ERROR, "Message not yet support");
            break;

        default:
            Assert(0);            /* Shouldn't come here.. keep compiler quiet */
    }

}

/*
 * Proxy the incoming message to the GTM server after adding our own identifier
 * to it. The rest of the message is forwarded as it is without even reading
 * its contents.
 */
static void
GTMProxy_ProxyCommand(GTMProxy_ConnectionInfo *conninfo, GTM_Conn *gtm_conn,
        GTM_MessageType mtype, StringInfo message)
{
    GTMProxy_CommandInfo *cmdinfo;
    GTMProxy_ThreadInfo *thrinfo = GetMyThreadInfo;
    GTM_ProxyMsgHeader proxyhdr;
    const char *unreadmsg;
    int unreadmsglen;

    Assert(IsProxiedMessage(mtype));

    proxyhdr.ph_conid = conninfo->con_id;

    unreadmsglen = pq_getmsgunreadlen(message);
    unreadmsg = pq_getmsgbytes(message, unreadmsglen);

     /* Start the message. */
    if (gtmpqPutMsgStart('C', true, gtm_conn) ||
        gtmpqPutnchar((char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader), gtm_conn) ||
        gtmpqPutInt(mtype, sizeof (GTM_MessageType), gtm_conn) ||
        gtmpqPutnchar(unreadmsg, unreadmsglen, gtm_conn))
        elog(ERROR, "Error sending proxied message");

    /*
     * Add the message to the pending command list
     */
    cmdinfo = palloc0(sizeof (GTMProxy_CommandInfo));
    cmdinfo->ci_mtype = mtype;
    cmdinfo->ci_conn = conninfo;
    cmdinfo->ci_res_index = 0;
    thrinfo->thr_processed_commands = gtm_lappend(thrinfo->thr_processed_commands, cmdinfo);

    /* Finish the message. */
    Enable_Longjmp();
    if (gtmpqPutMsgEnd(gtm_conn))
        elog(ERROR, "Error finishing the message");
    Disable_Longjmp();

    return;
}


/*
 * Record the incoming message as per its type. After all messages of this type
 * are collected, they will be sent in a single message to the GTM server.
 */
static void
GTMProxy_CommandPending(GTMProxy_ConnectionInfo *conninfo, GTM_MessageType mtype,
        GTMProxy_CommandData cmd_data)
{
    GTMProxy_CommandInfo *cmdinfo;
    GTMProxy_ThreadInfo *thrinfo = GetMyThreadInfo;

    MemoryContext oldContext = MemoryContextSwitchTo(TopMemoryContext);

    /*
     * Add the message to the pending command list
     */
    cmdinfo = palloc0(sizeof (GTMProxy_CommandInfo));
    cmdinfo->ci_mtype = mtype;
    cmdinfo->ci_conn = conninfo;
    cmdinfo->ci_res_index = 0;
    cmdinfo->ci_data = cmd_data;
    thrinfo->thr_pending_commands[mtype] = gtm_lappend(thrinfo->thr_pending_commands[mtype], cmdinfo);

    MemoryContextSwitchTo(oldContext);

    return;
}

/*
 * Register PGXC Node Connection in Proxy
 * Registery on GTM is made with MSG_NODE_REGISTER message type when node is launched.
 */
static void
GTMProxy_RegisterPGXCNode(GTMProxy_ConnectionInfo *conninfo,
                          char *node_name,
                          GTM_PGXCNodeType remote_type,
                          bool is_postmaster)
{
    elog(DEBUG3, "Registering PGXC Node with name %s", node_name);
    conninfo->con_port->node_name = strdup(node_name);
    conninfo->con_port->remote_type = remote_type;
    conninfo->con_port->is_postmaster = is_postmaster;
}

static void
GTMProxy_HandshakeConnection(GTMProxy_ConnectionInfo *conninfo)
{
    /*
     * We expect a startup message at the very start. The message type is
     * REGISTER_COORD, followed by the 4 byte PGXC node ID
     */
    char startup_type;
    GTM_StartupPacket sp;
    StringInfoData inBuf;
    StringInfoData buf;

    startup_type = pq_getbyte(conninfo->con_port);

    if (startup_type != 'A')
        ereport(ERROR,
                (EPROTO,
                 errmsg("Expecting a startup message, but received %c",
                     startup_type)));

    initStringInfo(&inBuf);

    /*
     * All frontend messages have a length word next
     * after the type code; we can read the message contents independently of
     * the type.
     */
    if (pq_getmessage(conninfo->con_port, &inBuf, 0))
        ereport(ERROR,
                (EPROTO,
                 errmsg("Expecting PGXC Node ID, but received EOF")));

    memcpy(&sp,
           pq_getmsgbytes(&inBuf, sizeof (GTM_StartupPacket)),
           sizeof (GTM_StartupPacket));
    pq_getmsgend(&inBuf);

    GTMProxy_RegisterPGXCNode(conninfo, sp.sp_node_name, sp.sp_remotetype, sp.sp_ispostmaster);

    /*
     * Send a dummy authentication request message 'R' as the client
     * expects that in the current protocol
     */
    pq_beginmessage(&buf, 'R');
    pq_sendint(&buf, 0, 4);
    pq_endmessage(conninfo->con_port, &buf);
    pq_flush(conninfo->con_port);

    conninfo->con_authenticated = true;

    elog(DEBUG3, "Sent connection authentication message to the client");
}

static void
GTMProxy_HandleDisconnect(GTMProxy_ConnectionInfo *conninfo, GTM_Conn *gtm_conn)
{// #lizard forgives
    GTM_ProxyMsgHeader proxyhdr;
    int namelen;

    proxyhdr.ph_conid = conninfo->con_id;
    /* Start the message. */
    if (gtmpqPutMsgStart('C', true, gtm_conn) ||
        gtmpqPutnchar((char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader), gtm_conn) ||
        gtmpqPutInt(MSG_BACKEND_DISCONNECT, sizeof (GTM_MessageType), gtm_conn) ||
        gtmpqPutc(conninfo->con_port->is_postmaster, gtm_conn))
        elog(ERROR, "Error proxing data");

    /*
     * Then send node type and node number if backend is a postmaster to
     * disconnect the correct node.
     */
    if (conninfo->con_port->is_postmaster)
    {
        namelen = strlen(conninfo->con_port->node_name);
        if (gtmpqPutnchar((char *)&conninfo->con_port->remote_type, sizeof(GTM_PGXCNodeType), gtm_conn) ||
            gtmpqPutInt(namelen, sizeof (int), gtm_conn) ||
            gtmpqPutnchar(conninfo->con_port->node_name, namelen, gtm_conn) )
            elog(ERROR, "Error proxing data");
    }

    /* Finish the message. */
    if (gtmpqPutMsgEnd(gtm_conn))
        elog(ERROR, "Error finishing the message");

    conninfo->con_disconnected = true;
    if (conninfo->con_port->sock > 0)
        StreamClose(conninfo->con_port->sock);
    ConnFree(conninfo->con_port);
    conninfo->con_port = NULL;

    return;
}

/*
 * Process all the pending messages now.
 */
static void
GTMProxy_ProcessPendingCommands(GTMProxy_ThreadInfo *thrinfo)
{// #lizard forgives
    int ii;
    GTMProxy_CommandInfo *cmdinfo = NULL;
    GTM_ProxyMsgHeader proxyhdr;
    GTM_Conn *gtm_conn = thrinfo->thr_gtm_conn;
    gtm_ListCell *elem = NULL;

    for (ii = 0; ii < MSG_TYPE_COUNT; ii++)
    {
        int res_index = 0;

        /* We process backend disconnects last! */
        if (ii == MSG_BACKEND_DISCONNECT ||
                gtm_list_length(thrinfo->thr_pending_commands[ii]) == 0)
            continue;

        /*
         * Start a new group message and fill in the headers
         */
        proxyhdr.ph_conid = InvalidGTMProxyConnID;

        if (gtmpqPutMsgStart('C', true, gtm_conn) ||
            gtmpqPutnchar((char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader), gtm_conn))
            elog(ERROR, "Error proxing data");

        switch (ii)
        {
            case MSG_TXN_BEGIN_GETGXID:
                if (gtm_list_length(thrinfo->thr_pending_commands[ii]) <=0 )
                    elog(PANIC, "No pending commands of type %d", ii);

                if (gtmpqPutInt(MSG_TXN_BEGIN_GETGXID_MULTI, sizeof (GTM_MessageType), gtm_conn) ||
                    gtmpqPutInt(gtm_list_length(thrinfo->thr_pending_commands[ii]), sizeof(int), gtm_conn))
                    elog(ERROR, "Error sending data");
                gtm_foreach (elem, thrinfo->thr_pending_commands[ii])
                {
                    cmdinfo = (GTMProxy_CommandInfo *)gtm_lfirst(elem);
                    Assert(cmdinfo->ci_mtype == ii);
                    cmdinfo->ci_res_index = res_index++;
                    if (gtmpqPutInt(cmdinfo->ci_data.cd_beg.iso_level,
                                sizeof (GTM_IsolationLevel), gtm_conn) ||
                        gtmpqPutc(cmdinfo->ci_data.cd_beg.rdonly, gtm_conn) ||
                        gtmpqPutInt(strlen(cmdinfo->ci_data.cd_beg.global_sessionid) + 1,
                            sizeof (uint32), gtm_conn) ||
                        gtmpqPutnchar(cmdinfo->ci_data.cd_beg.global_sessionid,
                            strlen(cmdinfo->ci_data.cd_beg.global_sessionid) + 1, gtm_conn) ||
                        gtmpqPutInt(cmdinfo->ci_conn->con_id, sizeof (GTMProxy_ConnID), gtm_conn))
                        elog(ERROR, "Error sending data");

                }

                /* Finish the message. */
                Enable_Longjmp();
                if (gtmpqPutMsgEnd(gtm_conn))
                    elog(ERROR, "Error finishing the message");
                Disable_Longjmp();

                /*
                 * Move the entire list to the processed command
                 */
                thrinfo->thr_processed_commands = gtm_list_concat(thrinfo->thr_processed_commands,
                        thrinfo->thr_pending_commands[ii]);
                /*
                 * Free the list header of the second list, unless
                 * gtm_list_concat actually returned the second list as-is
                 * because the first list was empty
                 */
                if ((thrinfo->thr_processed_commands != thrinfo->thr_pending_commands[ii]) &&
                    (thrinfo->thr_pending_commands[ii] != gtm_NIL))
                    pfree(thrinfo->thr_pending_commands[ii]);
                thrinfo->thr_pending_commands[ii] = gtm_NIL;
                break;

            case MSG_TXN_COMMIT_MULTI:
                if (gtmpqPutInt(MSG_TXN_COMMIT_MULTI, sizeof (GTM_MessageType), gtm_conn) ||
                    gtmpqPutInt(gtm_list_length(thrinfo->thr_pending_commands[ii]), sizeof(int), gtm_conn))
                    elog(ERROR, "Error sending data");

                gtm_foreach (elem, thrinfo->thr_pending_commands[ii])
                {
                    cmdinfo = (GTMProxy_CommandInfo *)gtm_lfirst(elem);
                    Assert(cmdinfo->ci_mtype == ii);
                    cmdinfo->ci_res_index = res_index++;
                    {
                        if (gtmpqPutnchar((char *)&cmdinfo->ci_data.cd_rc.gxid,
                                sizeof (GlobalTransactionId), gtm_conn))
                            elog(ERROR, "Error sending data");
                    }
                }

                /* Finish the message. */
                Enable_Longjmp();
                if (gtmpqPutMsgEnd(gtm_conn))
                    elog(ERROR, "Error finishing the message");
                Disable_Longjmp();

                /*
                 * Move the entire list to the processed command
                 */
                thrinfo->thr_processed_commands = gtm_list_concat(thrinfo->thr_processed_commands,
                        thrinfo->thr_pending_commands[ii]);
                /*
                 * Free the list header of the second list, unless
                 * gtm_list_concat actually returned the second list as-is
                 * because the first list was empty
                 */
                if ((thrinfo->thr_processed_commands != thrinfo->thr_pending_commands[ii]) &&
                    (thrinfo->thr_pending_commands[ii] != gtm_NIL))
                    pfree(thrinfo->thr_pending_commands[ii]);
                thrinfo->thr_pending_commands[ii] = gtm_NIL;
                break;

                break;

            case MSG_TXN_ROLLBACK:
                if (gtmpqPutInt(MSG_TXN_ROLLBACK_MULTI, sizeof (GTM_MessageType), gtm_conn) ||
                    gtmpqPutInt(gtm_list_length(thrinfo->thr_pending_commands[ii]), sizeof(int), gtm_conn))
                    elog(ERROR, "Error sending data");

                gtm_foreach (elem, thrinfo->thr_pending_commands[ii])
                {
                    cmdinfo = (GTMProxy_CommandInfo *)gtm_lfirst(elem);
                    Assert(cmdinfo->ci_mtype == ii);
                    cmdinfo->ci_res_index = res_index++;
                    {
                        if (gtmpqPutnchar((char *)&cmdinfo->ci_data.cd_rc.gxid,
                                sizeof (GlobalTransactionId), gtm_conn))
                            elog(ERROR, "Error sending data");
                    }
                }

                /* Finish the message. */
                Enable_Longjmp();
                if (gtmpqPutMsgEnd(gtm_conn))
                    elog(ERROR, "Error finishing the message");
                Disable_Longjmp();


                /*
                 * Move the entire list to the processed command
                 */
                thrinfo->thr_processed_commands = gtm_list_concat(thrinfo->thr_processed_commands,
                        thrinfo->thr_pending_commands[ii]);
                /*
                 * Free the list header of the second list, unless
                 * gtm_list_concat actually returned the second list as-is
                 * because the first list was empty
                 */
                if ((thrinfo->thr_processed_commands != thrinfo->thr_pending_commands[ii]) &&
                    (thrinfo->thr_pending_commands[ii] != gtm_NIL))
                    pfree(thrinfo->thr_pending_commands[ii]);
                thrinfo->thr_pending_commands[ii] = gtm_NIL;
                break;

            case MSG_SNAPSHOT_GET_MULTI:
                if (gtmpqPutInt(MSG_SNAPSHOT_GET_MULTI, sizeof (GTM_MessageType), gtm_conn) ||
                    gtmpqPutInt(gtm_list_length(thrinfo->thr_pending_commands[ii]), sizeof(int), gtm_conn))
                    elog(ERROR, "Error sending data");

                gtm_foreach (elem, thrinfo->thr_pending_commands[ii])
                {
                    cmdinfo = (GTMProxy_CommandInfo *)gtm_lfirst(elem);
                    Assert(cmdinfo->ci_mtype == ii);
                    cmdinfo->ci_res_index = res_index++;
                    {
                        if (gtmpqPutnchar((char *)&cmdinfo->ci_data.cd_rc.gxid,
                                sizeof (GlobalTransactionId), gtm_conn))
                            elog(ERROR, "Error sending data");
                    }
                }

                /* Finish the message. */
                Enable_Longjmp();
                if (gtmpqPutMsgEnd(gtm_conn))
                    elog(ERROR, "Error finishing the message");
                Disable_Longjmp();

                /*
                 * Move the entire list to the processed command
                 */
                thrinfo->thr_processed_commands = gtm_list_concat(thrinfo->thr_processed_commands,
                        thrinfo->thr_pending_commands[ii]);
                /*
                 * Free the list header of the second list, unless
                 * gtm_list_concat actually returned the second list as-is
                 * because the first list was empty
                 */
                if ((thrinfo->thr_processed_commands != thrinfo->thr_pending_commands[ii]) &&
                    (thrinfo->thr_pending_commands[ii] != gtm_NIL))
                    pfree(thrinfo->thr_pending_commands[ii]);
                thrinfo->thr_pending_commands[ii] = gtm_NIL;
                break;


            default:
                elog(ERROR, "This message type (%d) can not be grouped together", ii);
        }
    }
    /* Process backend disconnect messages now */
    gtm_foreach (elem, thrinfo->thr_pending_commands[MSG_BACKEND_DISCONNECT])
    {
        ereport(DEBUG1,
                (EPROTO,
                 errmsg("cleaning up client disconnection")));
        cmdinfo = (GTMProxy_CommandInfo *)gtm_lfirst(elem);
        GTMProxy_HandleDisconnect(cmdinfo->ci_conn, gtm_conn);
    }
}

/*
 * Validate the proposed data directory
 */
static void
checkDataDir(void)
{
    struct stat stat_buf;

    Assert(GTMProxyDataDir);

retry:
    if (stat(GTMProxyDataDir, &stat_buf) != 0)
    {
        if (errno == ENOENT)
        {
            if (mkdir(GTMProxyDataDir, 0700) != 0)
            {
                ereport(FATAL,
                        (errno,
                         errmsg("failed to create the directory \"%s\"",
                             GTMProxyDataDir)));
            }
            goto retry;
        }
        else
            ereport(FATAL,
                    (EPERM,
                 errmsg("could not read permissions of directory \"%s\": %m",
                        GTMProxyDataDir)));
    }

    /* eventual chdir would fail anyway, but let's test ... */
    if (!S_ISDIR(stat_buf.st_mode))
        ereport(FATAL,
                (EINVAL,
                 errmsg("specified data directory \"%s\" is not a directory",
                        GTMProxyDataDir)));

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
                        GTMProxyDataDir),
                 errhint("The server must be started by the user that owns the data directory.")));
#endif
}

/*
 * Set data directory, but make sure it's an absolute path.  Use this,
 * never set DataDir directly.
 */
void
SetDataDir(void)
{
    char   *new;

    /* If presented path is relative, convert to absolute */
    new = make_absolute_path(GTMProxyDataDir);
    if (!new)
        ereport(FATAL,
                (errno,
                 errmsg("failed to set the data directory \"%s\"",
                        GTMProxyDataDir)));

    if (GTMProxyDataDir)
        free(GTMProxyDataDir);

    GTMProxyDataDir = new;
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
    if (chdir(GTMProxyDataDir) < 0)
        ereport(FATAL,
                (EINVAL,
                 errmsg("could not change directory to \"%s\": %m",
                        GTMProxyDataDir)));
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
    CreateLockFile(GTM_PID_FILE, GTMProxyDataDir);
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
                          errhint("Is another GTM proxy (PID %d) running in data directory \"%s\"?",
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
    snprintf(buffer, sizeof(buffer), "%d\n%s\n",
             (int) my_pid, GTMProxyDataDir);
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

#define OPTS_FILE    "gtm_proxy.opts"

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

/*
 * Unregister Proxy on GTM
 */
static void
UnregisterProxy(void)
{// #lizard forgives
    GTM_PGXCNodeType type = GTM_NODE_GTM_PROXY;
    GTM_Result *res = NULL;
    time_t finish_time;

    if (!master_conn || GTMPQstatus(master_conn) != CONNECTION_OK)
        master_conn = ConnectGTM();
    if (!master_conn || GTMProxyNodeName == NULL)
        goto failed;

    if (gtmpqPutMsgStart('C', true, master_conn) ||
        gtmpqPutInt(MSG_NODE_UNREGISTER, sizeof (GTM_MessageType), master_conn) ||
        gtmpqPutnchar((char *)&type, sizeof(GTM_PGXCNodeType), master_conn) ||
        /* Node name length */
        gtmpqPutInt(strlen(GTMProxyNodeName), sizeof (GTM_StrLen), master_conn) ||
        /* Node name (var-len) */
        gtmpqPutnchar(GTMProxyNodeName, strlen(GTMProxyNodeName), master_conn) )
        goto failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(master_conn))
        goto failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(master_conn))
        goto failed;

    finish_time = time(NULL) + PROXY_CLIENT_TIMEOUT;
    if (gtmpqWaitTimed(true, false, master_conn, finish_time) ||
        gtmpqReadData(master_conn) < 0)
        goto failed;

    if ((res = GTMPQgetResult(master_conn)) == NULL)
        goto failed;

    /* Check on node type and node name */
    if (res->gr_status == GTM_RESULT_OK)
    {
        Assert(res->gr_resdata.grd_node.type == type);
        Assert( strcmp(res->gr_resdata.grd_node.node_name, GTMProxyNodeName) == 0 );
    }

    /* Disconnect cleanly as Proxy is shutting down */
    GTMPQfinish(master_conn);

    return;

failed:
    /*
     * We don't deliberately write an ERROR here to ensure that proxy shutdown
     * proceeds to the end. Without that we have a danger of leaving behind a
     * stale PID file, thus causing gtm_ctl stop to wait forever for the proxy
     * to shutdown
     *
     * XXX This can happen when GTM restarts, clearing existing registration
     * information. See if this needs to fixed
     */
    return elog(LOG, "can not Unregister Proxy on GTM");
}

/*
 * Register Proxy on GTM
 *
 * If reconnect is specified, then existing connection is closed
 * and the target GTM is taken from NewGTMServerHost and
 * NewGTMServerPortNumber.
 */
static void
RegisterProxy(bool is_reconnect)
{// #lizard forgives
    GTM_PGXCNodeType type = GTM_NODE_GTM_PROXY;
    GTM_PGXCNodePort port = (GTM_PGXCNodePort) GTMProxyPortNumber;
    GTM_Result *res = NULL;
    char proxyname[] = "";
    time_t finish_time;
    MemoryContext old_mcxt = NULL;

    if (is_reconnect)
    {
        elog(NOTICE,
             "Reconnect to new GTM, hostname=%s, port=%d",
             NewGTMServerHost, NewGTMServerPortNumber);
        /*
         * Now reconnect.   Close the exising connection
         * and update the target host and port.
         * First, change the memory context to TopMemoryContext
         */
        old_mcxt = MemoryContextSwitchTo(TopMemoryContext);

        /* Change the target to new GTM */
        GTMPQfinish(master_conn);
        GTMServerHost = NewGTMServerHost;
        GTMServerPortNumber = NewGTMServerPortNumber;
    }

    master_conn = ConnectGTM();
    if (!master_conn || GTMProxyNodeName == NULL)
        goto failed;

    /*
     * As this node is itself a Proxy it registers 0 as Proxy ID on GTM
     * as it doesn't go through any other proxy.
     */
    if (gtmpqPutMsgStart('C', true, master_conn) ||
        gtmpqPutInt(MSG_NODE_REGISTER, sizeof (GTM_MessageType), master_conn) ||
        gtmpqPutnchar((char *)&type, sizeof(GTM_PGXCNodeType), master_conn) ||
        gtmpqPutInt((int)strlen(GTMProxyNodeName), sizeof(int), master_conn) ||
        gtmpqPutnchar(GTMProxyNodeName, (int)strlen(GTMProxyNodeName), master_conn) ||
        gtmpqPutInt((int)strlen(ListenAddresses), sizeof(int), master_conn) ||
        gtmpqPutnchar(ListenAddresses, (int)strlen(ListenAddresses), master_conn) ||
        gtmpqPutnchar((char *)&port, sizeof(GTM_PGXCNodePort), master_conn) ||
        /* PGXCTODO : previously ZERO was used if the node was itself proxy, now its name is empty. */
        gtmpqPutInt((int)strlen(proxyname), sizeof(int), master_conn) ||
        gtmpqPutnchar(proxyname, (int)strlen(proxyname), master_conn) ||
        gtmpqPutInt((int)strlen(GTMProxyDataDir), 4, master_conn) ||
        gtmpqPutnchar(GTMProxyDataDir, strlen(GTMProxyDataDir), master_conn)||
        gtmpqPutInt(NODE_CONNECTED, sizeof(GTM_PGXCNodeStatus), master_conn))
        goto failed;

    /* Finish the message. */
    if (gtmpqPutMsgEnd(master_conn))
        goto failed;

    /* Flush to ensure backend gets it. */
    if (gtmpqFlush(master_conn))
        goto failed;

    finish_time = time(NULL) + PROXY_CLIENT_TIMEOUT;
    if (gtmpqWaitTimed(true, false, master_conn, finish_time) ||
        gtmpqReadData(master_conn) < 0)
    {
        elog(ERROR, "Cannot read data.");
        goto failed;
    }

    if ((res = GTMPQgetResult(master_conn)) == NULL)
    {
        elog(ERROR, "Cannot get result.");
        goto failed;
    }

    if (res->gr_status == GTM_RESULT_OK)
    {
        Assert(res->gr_resdata.grd_node.type == type);
        Assert( strcmp(res->gr_resdata.grd_node.node_name, GTMProxyNodeName) == 0 );
    }

    /* If reconnect, restore the old memory context */
    if (is_reconnect)
        MemoryContextSwitchTo(old_mcxt);
    return;

failed:
    elog(ERROR, "can not register Proxy on GTM");
}

static GTM_Conn*
ConnectGTM(void)
{
    char conn_str[256];
    GTM_Conn *conn;

    sprintf(conn_str, "host=%s port=%d node_name=%s remote_type=%d postmaster=1",
            GTMServerHost, GTMServerPortNumber, GTMProxyNodeName, GTM_NODE_GTM_PROXY_POSTMASTER);

    conn = PQconnectGTM(conn_str);
    if (GTMPQstatus(conn) != CONNECTION_OK)
    {
        int save_errno = errno;

        elog(ERROR, "can not connect to GTM");

        errno = save_errno;

        GTMPQfinish(conn);
        conn = NULL;
    }

    return conn;
}

/*
 * Release backup command data
 */
static void ReleaseCmdBackup(GTMProxy_CommandInfo *cmdinfo)
{
    GTMProxy_ConnID connIdx = cmdinfo->ci_conn->con_id;

    GetMyThreadInfo->thr_any_backup[connIdx] = FALSE;
    GetMyThreadInfo->thr_qtype[connIdx] = 0;
    resetStringInfo(&(GetMyThreadInfo->thr_inBufData[connIdx]));
}


static void
workerThreadReconnectToGTM(void)
{
    char gtm_connect_string[1024];
    MemoryContext   oldContext;
    uint32 saveMyClientId = 0;

    /*
     * First of all, we should acquire reconnect control lock in READ mode
     * to wait for the main thread to finish reconnect.
     */
    GTM_RWLockAcquire(&ReconnectControlLock, GTM_LOCKMODE_READ);
    GTM_RWLockRelease(&ReconnectControlLock);    /* The lock not needed any longer */
    PG_SETMASK(&UnBlockSig);

    /* Disconnect the current connection and re-connect to the new GTM */
    oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

    /*
     * Before cleaning the old connection, remember the client identifier
     * issued to us by the old GTM master. We send that identifier back to the
     * new master so that it can re-establish our association with any
     * transactions currently open by us
     */
    if (GetMyThreadInfo->thr_gtm_conn)
    {
        saveMyClientId = GetMyThreadInfo->thr_gtm_conn->my_id;
        GTMPQfinish(GetMyThreadInfo->thr_gtm_conn);
    }

    sprintf(gtm_connect_string, "host=%s port=%d node_name=%s remote_type=%d client_id=%u",
            GTMServerHost, GTMServerPortNumber, GTMProxyNodeName,
            GTM_NODE_GTM_PROXY, saveMyClientId);
    elog(DEBUG1, "Worker thread connecting to %s", gtm_connect_string);
    GetMyThreadInfo->thr_gtm_conn = PQconnectGTM(gtm_connect_string);

    if (GetMyThreadInfo->thr_gtm_conn == NULL)
        elog(FATAL, "Worker thread GTM connection failed.");
    elog(DEBUG1, "Worker thread connection done.");

    MemoryContextSwitchTo(oldContext);

    /* Initialize the command processing */
    GetMyThreadInfo->reconnect_issued = FALSE;
}
