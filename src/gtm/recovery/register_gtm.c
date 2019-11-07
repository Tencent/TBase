/*-------------------------------------------------------------------------
 *
 * register.c
 *  PGXC Node Register on GTM and GTM Proxy, node registering functions
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

#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/epoll.h>

#include "gtm/elog.h"
#include "gtm/gtm.h"
#include "gtm/gtm_client.h"
#include "gtm/gtm_serialize.h"
#include "gtm/gtm_standby.h"
#include "gtm/libpq.h"
#include "gtm/libpq-int.h"
#include "gtm/pqformat.h"
#include "gtm/stringinfo.h"
#include "gtm/register.h"

#include "gtm/gtm_ip.h"

#ifdef XCP
#include "storage/backendid.h"
#endif

#ifdef __TBASE__
#include "gtm/gtm_store.h"
#endif

#ifndef __XLOG__
static void finishStandbyConn(GTM_ThreadInfo *thrinfo);
#endif
extern bool Backup_synchronously;
extern GTM_ThreadInfo  *g_basebackup_thread ;
extern GTM_TimerHandle  g_GTM_Backup_Timer;
extern GTM_RWLock        g_GTM_Backup_Timer_Lock;

/*
 * Process MSG_NODE_REGISTER/MSG_BKUP_NODE_REGISTER message.
 *
 * is_backup indicates the message is MSG_BKUP_NODE_REGISTER.
 */
void
ProcessPGXCNodeRegister(Port *myport, StringInfo message, bool is_backup)
{// #lizard forgives
    GTM_PGXCNodeType    type;
    GTM_PGXCNodePort    port;
    int                ret = 0;
    char            remote_host[NI_MAXHOST];
    char            remote_port[NI_MAXHOST];
    char            datafolder[NI_MAXHOST];
    char            node_name[NI_MAXHOST];
    char            proxyname[NI_MAXHOST];
    char            *ipaddress;
    MemoryContext        oldContext;
    int            len;
    StringInfoData        buf;
    GTM_PGXCNodeStatus    status;

    /* Read Node Type */
    memcpy(&type, pq_getmsgbytes(message, sizeof (GTM_PGXCNodeType)),
            sizeof (GTM_PGXCNodeType));

     /* Read Node name */
    len = pq_getmsgint(message, sizeof (int));
    if (len >= NI_MAXHOST)
        ereport(ERROR,
                (EINVAL,
                 errmsg("Invalid name length.")));

    memcpy(node_name, (char *)pq_getmsgbytes(message, len), len);
    node_name[len] = '\0';

     /* Read Host name */
    len = pq_getmsgint(message, sizeof (int));
    memcpy(remote_host, (char *)pq_getmsgbytes(message, len), len);
    remote_host[len] = '\0';
    ipaddress = remote_host;

    /* Read Port Number */
    memcpy(&port, pq_getmsgbytes(message, sizeof (GTM_PGXCNodePort)),
            sizeof (GTM_PGXCNodePort));

    /* Read Proxy name (empty string if no proxy used) */
    len = pq_getmsgint(message, sizeof (GTM_StrLen));
    if (len >= NI_MAXHOST)
        ereport(ERROR,
                (EINVAL,
                 errmsg("Invalid proxy name length.")));
    memcpy(proxyname, (char *)pq_getmsgbytes(message, len), len);
    proxyname[len] = '\0';

    /*
     * Finish by reading Data Folder (length and then string)
     */
    len = pq_getmsgint(message, sizeof (GTM_StrLen));

    memcpy(datafolder, (char *)pq_getmsgbytes(message, len), len);
    datafolder[len] = '\0';

    status = pq_getmsgint(message, sizeof (GTM_PGXCNodeStatus));
    
    gtm_getnameinfo_all(&myport->raddr.addr, myport->raddr.salen,
                       remote_host, sizeof(remote_host),
                       remote_port, sizeof(remote_port),
                       NI_NUMERICHOST | NI_NUMERICSERV);
    ipaddress = remote_host;
    elog(DEBUG8,
         "ProcessPGXCNodeRegister: ipaddress = \"%s\", node name = \"%s\", proxy name = \"%s\", "
         "datafolder \"%s\", status = %d",
         ipaddress, node_name, proxyname, datafolder, status);

    if ((type!=GTM_NODE_GTM_PROXY) &&
        (type!=GTM_NODE_GTM_PROXY_POSTMASTER) &&
        (type!=GTM_NODE_COORDINATOR) &&
        (type!=GTM_NODE_DATANODE) &&
        (type!=GTM_NODE_GTM) &&
        (type!=GTM_NODE_DEFAULT))
        ereport(ERROR,
                (EINVAL,
                 errmsg("Unknown node type.")));

    elog(DEBUG1, "Node type = %d", type);

    /*
     * We must use the TopMostMemoryContext because the Node ID information is
     * not bound to a thread and can outlive any of the thread specific
     * contextes.
     */
    oldContext = MemoryContextSwitchTo(TopMostMemoryContext);
    
#ifndef __XLOG__
    /*
     * We don't check if the this is not in standby mode to allow
     * cascaded standby.
     */
    if (type == GTM_NODE_GTM)
    {

        elog(LOG, "Registering GTM Standby.  Try to unregister the current one.");
        /*
         * There's another standby.   May be failed one.
         * Clean this up.  This means that we allow
         * only one standby at the same time.
         *
         * This helps to give up failed standby and connect
         * new one, regardless how they stopped.
         *
         * Be sure that all ther threads are locked by other
         * means, typically by receiving MSG_BEGIN_BACKUP.
         *
         * First try to unregister GTM which is now connected.  We don't care
         * if it failed.
         */
        ret = Recovery_PGXCNodeUnregister(type, node_name, false, -1);
        if (ret)
        {
            elog(LOG, "No GTM standby exist, unregister failed.");
        }
        /*
         * Then disconnect the connections to the standby from each thread.
         * Please note that we assume only one standby is allowed at the same time.
         * Cascade standby may be allowed.
         */
        elog(LOG, "Try to disconect off the gtm standby.");
        GTM_DoForAllOtherThreads(finishStandbyConn);
    }
#endif

#ifdef __XLOG__
    /* Add node to node type store and hash. */
    ret = Recovery_PGXCNodeRegister(type, node_name, port,
                                  proxyname, status,
                                  ipaddress, datafolder, false, myport->sock,
                                  false);
    if(ret == EEXIST && type == GTM_NODE_GTM)
    {
        elog(LOG,"gtm slave reconnected");
    }
    else if (ret)
    {
        ereport(ERROR,
                (EINVAL,
                 errmsg("Failed to Register node")));
    }
#else
    /* Add node to node type store and hash. */
    if (Recovery_PGXCNodeRegister(type, node_name, port,
                                  proxyname, status,
                                  ipaddress, datafolder, false, myport->sock,
                                  false))
    {
        ereport(ERROR,
                (EINVAL,
                 errmsg("Failed to Register node")));
    }
#endif

#ifndef __XLOG__
    /*
     * We don't check if the this is not in standby mode to allow
     * cascaded standby.
     */
    if ((type == GTM_NODE_GTM) && (status == NODE_CONNECTED))
    {
        GTMThreads->gt_standby_ready = true;
    }
#endif

    MemoryContextSwitchTo(oldContext);

    pq_getmsgend(message);

    if (!is_backup)
    {
#ifndef __XLOG__
        /*
         * Backup first
         */
        if (GetMyConnection(myport)->standby)
        {
            int _rc;
            GTM_Conn *oldconn = GetMyConnection(myport)->standby;
            int count = 0;
            GTM_PGXCNodeInfo *standbynode;

            elog(DEBUG1, "calling node_register_internal() for standby GTM %p.",
                 GetMyConnection(myport)->standby);

        retry:
            _rc = bkup_node_register_internal(GetMyConnection(myport)->standby,
                                              type,
                                              ipaddress,
                                              port,
                                              node_name,
                                              datafolder,
                                              status);

            elog(DEBUG8, "node_register_internal() returns rc %d.", _rc);

            if (gtm_standby_check_communication_error(myport, &count, oldconn))
                goto retry;

            /* Now check if there're other standby registered. */
            standbynode = find_standby_node_info();
            if (!standbynode)
                GTMThreads->gt_standby_ready = false;

            if (Backup_synchronously && (myport->remote_type != GTM_NODE_GTM_PROXY))
                gtm_sync_standby(GetMyConnection(myport)->standby);

        }
#endif

        /*
         * Then, send a SUCCESS message back to the client
         */
        pq_beginmessage(&buf, 'S');
        pq_sendint(&buf, NODE_REGISTER_RESULT, 4);
        if (myport->remote_type == GTM_NODE_GTM_PROXY)
        {
            GTM_ProxyMsgHeader proxyhdr;
            proxyhdr.ph_conid = myport->conn_id;
            pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
        }
        pq_sendbytes(&buf, (char *)&type, sizeof(GTM_PGXCNodeType));
        /* Node name length */
        pq_sendint(&buf, strlen(node_name), 4);
        /* Node name (var-len) */
        pq_sendbytes(&buf, node_name, strlen(node_name));
        pq_endmessage(myport, &buf);

        if (myport->remote_type != GTM_NODE_GTM_PROXY)
        {
#ifndef __XLOG__
            if (GetMyConnection(myport)->standby)
                gtmpqFlush(GetMyConnection(myport)->standby);
#endif
            pq_flush(myport);
        }
    }

}


/*
 * Process MSG_NODE_UNREGISTER/MSG_BKUP_NODE_UNREGISTER
 *
 * is_backup indiccates MSG_BKUP_NODE_UNREGISTER
 */
void
ProcessPGXCNodeUnregister(Port *myport, StringInfo message, bool is_backup)
{// #lizard forgives
    GTM_PGXCNodeType    type;
    MemoryContext        oldContext;
    StringInfoData        buf;
    int            len;
    char            node_name[NI_MAXHOST];

    /* Read Node Type and number */
    memcpy(&type, pq_getmsgbytes(message, sizeof (GTM_PGXCNodeType)),
            sizeof (GTM_PGXCNodeType));

     /* Read Node name */
    len = pq_getmsgint(message, sizeof (int));
    if (len >= NI_MAXHOST)
        ereport(ERROR,
                (EINVAL,
                 errmsg("Invalid node name length")));
    memcpy(node_name, (char *)pq_getmsgbytes(message, len), len);
    node_name[len] = '\0';

    /*
     * We must use the TopMostMemoryContext because the Node ID information is
     * not bound to a thread and can outlive any of the thread specific
     * contextes.
     */
    oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

    if (Recovery_PGXCNodeUnregister(type, node_name, false, myport->sock))
    {
        ereport(ERROR,
                (EINVAL,
                 errmsg("Failed to Unregister node")));
    }

    MemoryContextSwitchTo(oldContext);

    pq_getmsgend(message);

    if (!is_backup)
    {
#ifndef __XLOG__
        /*
         * Backup first
         */
        if (GetMyConnection(myport)->standby)
        {
            int _rc;
            GTM_Conn *oldconn = GetMyConnection(myport)->standby;
            int count = 0;

            elog(DEBUG1, "calling node_unregister() for standby GTM %p.",
                 GetMyConnection(myport)->standby);

        retry:
            _rc = bkup_node_unregister(GetMyConnection(myport)->standby,
                                       type,
                                       node_name);


            if (gtm_standby_check_communication_error(myport, &count, oldconn))
                goto retry;

            if (Backup_synchronously && (myport->remote_type != GTM_NODE_GTM_PROXY))
                gtm_sync_standby(GetMyConnection(myport)->standby);

            elog(DEBUG1, "node_unregister() returns rc %d.", _rc);
        }
#endif

        /*
         * Send a SUCCESS message back to the client
         */
        pq_beginmessage(&buf, 'S');
        pq_sendint(&buf, NODE_UNREGISTER_RESULT, 4);
        if (myport->remote_type == GTM_NODE_GTM_PROXY)
        {
            GTM_ProxyMsgHeader proxyhdr;
            proxyhdr.ph_conid = myport->conn_id;
            pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
        }
        pq_sendbytes(&buf, (char *)&type, sizeof(GTM_PGXCNodeType));
        /* Node name length */
        pq_sendint(&buf, strlen(node_name), 4);
        /* Node name (var-len) */
        pq_sendbytes(&buf, node_name, strlen(node_name));

        pq_endmessage(myport, &buf);

        /* Flush standby before flush to the client */
        if (myport->remote_type != GTM_NODE_GTM_PROXY)
        {
#ifndef __XLOG__
            if (GetMyConnection(myport)->standby)
                gtmpqFlush(GetMyConnection(myport)->standby);
#endif
            pq_flush(myport);
        }
    }
}

/*
 * Process MSG_NODE_LIST
 */
void
ProcessPGXCNodeList(Port *myport, StringInfo message)
{
    MemoryContext        oldContext;
    StringInfoData        buf;
    int            num_node = 13;
    int i;

    GTM_PGXCNodeInfo *data[MAX_NODES];
    char *s_data[MAX_NODES];
    size_t s_datalen[MAX_NODES];

    /*
     * We must use the TopMostMemoryContext because the Node ID information is
     * not bound to a thread and can outlive any of the thread specific
     * contextes.
     */
    oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

    memset(data, 0, sizeof(GTM_PGXCNodeInfo *) * MAX_NODES);
    memset(s_data, 0, sizeof(char *) * MAX_NODES);

    num_node = pgxcnode_get_all(data, MAX_NODES, false);

    for (i = 0; i < num_node; i++)
    {
        size_t s_len;

        s_len = gtm_get_pgxcnodeinfo_size(data[i]);

        /*
         * Allocate memory blocks for serialized GTM_PGXCNodeInfo data.
         */
        s_data[i] = (char *)malloc(s_len+1);
        memset(s_data[i], 0, s_len+1);

        s_datalen[i] = gtm_serialize_pgxcnodeinfo(data[i], s_data[i], s_len+1);

        elog(DEBUG1, "gtm_get_pgxcnodeinfo_size: s_len=%ld, s_datalen=%ld", s_len, s_datalen[i]);
    }

    MemoryContextSwitchTo(oldContext);

    pq_getmsgend(message);

    /*
     * Send a SUCCESS message back to the client
     */
    pq_beginmessage(&buf, 'S');
    pq_sendint(&buf, NODE_LIST_RESULT, 4);
    if (myport->remote_type == GTM_NODE_GTM_PROXY)
    {
        GTM_ProxyMsgHeader proxyhdr;
        proxyhdr.ph_conid = myport->conn_id;
        pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
    }
    pq_sendint(&buf, num_node, sizeof(int));   /* number of nodes */

    /*
     * Send pairs of GTM_PGXCNodeInfo size and serialized GTM_PGXCNodeInfo body.
     */
    for (i = 0; i < num_node; i++)
    {
        pq_sendint(&buf, s_datalen[i], sizeof(int));
        pq_sendbytes(&buf, s_data[i], s_datalen[i]);
    }

    pq_endmessage(myport, &buf);

    if (myport->remote_type != GTM_NODE_GTM_PROXY)
        pq_flush(myport);

    /*
     * Release memory blocks for the serialized data.
     */
    for (i = 0; i < num_node; i++)
    {
        free(s_data[i]);
    }

    elog(DEBUG1, "ProcessPGXCNodeList() ok.");
}

void
ProcessGTMBeginBackup(Port *myport, StringInfo message)
{// #lizard forgives
#ifndef __TBASE__
    int ii;
#else
    bool  failed = false;
    bool  bret   = false;
    int64 local_sysid  = 0;
    int64 local_syslsn = 0;
    GlobalTimestamp local_sysgts = 0;
    int64 remote_sysid  = 0;
    int64 remote_syslsn = 0;
    GlobalTimestamp remote_sysgts = 0;
    StringInfoData buf;
    GTM_ThreadInfo *my_threadinfo;
#endif

#ifdef __XLOG__
    struct epoll_event event;
#endif

    pq_copymsgbytes(message, (char*)&remote_sysid,  sizeof(remote_sysid));
    pq_copymsgbytes(message, (char*)&remote_syslsn, sizeof(remote_syslsn));    
    pq_copymsgbytes(message, (char*)&remote_sysgts, sizeof(GlobalTimestamp));
    pq_getmsgend(message);

    bret = GTM_StoreGetSysInfo(&local_sysid, &local_syslsn, &local_sysgts);
    if (!bret)
    {
        elog(LOG, "GTM start backup get system info failed");
        failed = true;
    }
    else if (remote_sysid && remote_syslsn && remote_sysgts)
    {
        if ((remote_sysid != local_sysid) || (remote_syslsn > local_syslsn) || (remote_sysgts > local_sysgts))
        {
            elog(LOG, "GTM start backup consistancy check failed, local identifier:%ld local lsn:%ld local gts:%ld remote identifier:%ld, remote lsn:%ld, remote gts:%ld",
                    local_sysid, local_syslsn, local_sysgts, remote_sysid, remote_syslsn, remote_sysgts);
            failed = true;
        }
    }
    
    my_threadinfo = GetMyThreadInfo;
    elog(LOG, "GTM start backup local identifier:%ld local lsn:%ld local gts:%ld remote identifier:%ld, remote lsn:%ld, remote gts:%ld",
                    local_sysid, local_syslsn, local_sysgts, remote_sysid, remote_syslsn, remote_sysgts);
#ifndef __TBASE__
    for (ii = 0; ii < GTMThreads->gt_array_size; ii++)
    {
        if (GTMThreads->gt_threads[ii] && GTMThreads->gt_threads[ii] != my_threadinfo)
            GTM_RWLockAcquire(&GTMThreads->gt_threads[ii]->thr_lock, GTM_LOCKMODE_WRITE);
    }
#else
    /* disconnect from current thread */
    if(epoll_ctl(GetMyThreadInfo->thr_efd,EPOLL_CTL_DEL,myport->sock,NULL) != 0)
    {
        elog(LOG,"epoll delete fails %s",strerror(errno));
        failed = true;
    }

    event.data.ptr = GetMyThreadInfo->thr_conn;
    event.events = EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP;

    if(epoll_ctl(g_basebackup_thread->thr_efd,EPOLL_CTL_ADD,myport->sock,&event) != 0)
    {
        elog(LOG,"epoll add fails %s",strerror(errno));
        failed = true;
    }
#endif

    elog(LOG, "GTM start backup %s", failed ? "failed" : "succeed");
    my_threadinfo->thr_status = GTM_THREAD_BACKUP;
    pq_beginmessage(&buf, 'S');
    pq_sendint(&buf, failed ? BEGIN_BACKUP_FAIL_RESULT : BEGIN_BACKUP_SUCCEED_RESULT, 4);
    pq_endmessage(myport, &buf);
    pq_flush(myport);
}

void
ProcessGTMEndBackup(Port *myport, StringInfo message)
{
#ifndef __TBASE__
    int  ii;
#else
    bool bret = false;
    bool do_unlock = false;
#endif

    GTM_ThreadInfo *my_threadinfo;
    StringInfoData buf;

    pq_getmsgend(message);
    my_threadinfo = GetMyThreadInfo;

    elog(LOG, "GTM end backup");
    
#ifndef __TBASE__
    for (ii = 0; ii < GTMThreads->gt_array_size; ii++)
    {
        if (GTMThreads->gt_threads[ii] && GTMThreads->gt_threads[ii] != my_threadinfo)
            GTM_RWLockRelease(&GTMThreads->gt_threads[ii]->thr_lock);
    }
#else
    GTM_RWLockAcquire(&g_GTM_Backup_Timer_Lock,GTM_LOCKMODE_WRITE);
    if (g_GTM_Backup_Timer != INVALID_TIMER_HANDLE)
    {
        GTM_RemoveTimer(g_GTM_Backup_Timer);
        g_GTM_Backup_Timer = INVALID_TIMER_HANDLE;
        do_unlock = true;
    }
    GTM_RWLockRelease(&g_GTM_Backup_Timer_Lock);

    if(do_unlock)
    {
        bret = GTM_StoreUnLock();
        if (!bret)
        {
            elog(LOG, "ProcessGTMEndBackup GTM_StoreUnLock failed");
        }
    }
#endif

    elog(LOG, "GTM end backup complete");
    my_threadinfo->thr_status = GTM_THREAD_RUNNING;
    pq_beginmessage(&buf, 'S');
    pq_sendint(&buf, END_BACKUP_RESULT, 4);
    pq_endmessage(myport, &buf);
    pq_flush(myport);
}


#ifndef __XLOG__
static void
finishStandbyConn(GTM_ThreadInfo *thrinfo)
{
    if (thrinfo->standby)
    {
        GTMPQfinish(thrinfo->standby);
        thrinfo->standby = NULL;
    }
}
#endif

/*
 * Process MSG_REGISTER_SESSION message
 */
void
ProcessPGXCRegisterSession(Port *myport, StringInfo message)
{// #lizard forgives
    char            coord_name[SP_NODE_NAME];
    int32            coord_procid;
    int32            coord_backendid;
    int32            len;
    MemoryContext    oldContext;
    int             old_procid;
    StringInfoData    buf;

    len = pq_getmsgint(message, sizeof(len));
    if (len >= SP_NODE_NAME)
        ereport(ERROR,
                (EINVAL,
                 errmsg("Invalid name length.")));

    memcpy(coord_name, (char *)pq_getmsgbytes(message, len), len);
    coord_name[len] = '\0';

    coord_procid = pq_getmsgint(message, sizeof(coord_procid));

    coord_backendid = pq_getmsgint(message, sizeof(coord_backendid));

    /*
     * Check if all required data are supplied
     */
    if (len > 0 || coord_procid > 0 || coord_backendid != InvalidBackendId)
    {
        oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

        /*
         * Register the session
         */
        old_procid = Recovery_PGXCNodeRegisterCoordProcess(coord_name, coord_procid,
                                                           coord_backendid);
        MemoryContextSwitchTo(oldContext);

        /*
         * If there was a session with same backend id clean it up.
         */
        if (old_procid)
            GTM_CleanupSeqSession(coord_name, old_procid);
    }

#ifndef __XLOG__
    /*
     * If there is a standby forward the info to it
     */
    if (GetMyConnection(myport)->standby)
    {
        int _rc;
        GTM_Conn *oldconn = GetMyConnection(myport)->standby;
        int count = 0;
        GTM_PGXCNodeInfo *standbynode;

        elog(DEBUG1, "calling register_session() for standby GTM %p.",
             GetMyConnection(myport)->standby);

        do
        {
            _rc = register_session(GetMyConnection(myport)->standby,
                                   coord_name, coord_procid, coord_backendid);

            elog(DEBUG1, "register_session() returns rc %d.", _rc);
        }
        while (gtm_standby_check_communication_error(myport, &count, oldconn));

        /* Now check if there're other standby registered. */
        standbynode = find_standby_node_info();
        if (!standbynode)
            GTMThreads->gt_standby_ready = false;

        if (Backup_synchronously && (myport->remote_type != GTM_NODE_GTM_PROXY))
            gtm_sync_standby(GetMyConnection(myport)->standby);

    }
#endif

    /* Make up response */
    pq_beginmessage(&buf, 'S');
    pq_sendint(&buf, REGISTER_SESSION_RESULT, 4);
    /* For proxy write out header */
    if (myport->remote_type == GTM_NODE_GTM_PROXY)
    {
        GTM_ProxyMsgHeader proxyhdr;
        proxyhdr.ph_conid = myport->conn_id;
        pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
    }
    pq_endmessage(myport, &buf);
    /* Flush connections */
    if (myport->remote_type != GTM_NODE_GTM_PROXY)
    {
#ifndef __XLOG__
        if (GetMyConnection(myport)->standby)
            gtmpqFlush(GetMyConnection(myport)->standby);
#endif
        pq_flush(myport);
    }
}
