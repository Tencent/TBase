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

#include "gtm/elog.h"
#include "gtm/gtm.h"
#include "gtm/gtm_client.h"
#include "gtm/gtm_serialize.h"
#include "gtm/gtm_standby.h"
#include "gtm/gtm_time.h"
#include "gtm/gtm_txn.h"
#include "gtm/libpq.h"
#include "gtm/libpq-int.h"
#include "gtm/pqformat.h"
#include "gtm/stringinfo.h"
#include "gtm/register.h"

#include "gtm/gtm_ip.h"
#include "storage/backendid.h"

#define GTM_NODE_FILE            "register.node"
#define NODE_HASH_TABLE_SIZE    16
#define GTM_NODE_FILE_MAX_PATH    1024

typedef struct GTM_NodeInfoHashBucket
{
    gtm_List        *nhb_list;
} GTM_PGXCNodeInfoHashBucket;

static char GTMPGXCNodeFile[GTM_NODE_FILE_MAX_PATH];

/* Lock access of record file when necessary */
static GTM_RWLock RegisterFileLock;

/* Lock to control registration/unregistration of nodes */
static GTM_RWLock PGXCNodesLock;

static int NodeRegisterMagic = 0xeaeaeaea;
static int NodeUnregisterMagic = 0xebebebeb;
static int NodeEndMagic = 0xefefefef;

#define GTM_GlobalXmin GTMTransactions.gt_recent_global_xmin

static GTM_PGXCNodeInfoHashBucket GTM_PGXCNodes[NODE_HASH_TABLE_SIZE];
static GTM_Timestamp GTM_GlobalXminComputedTime;

static uint32 pgxcnode_gethash(char *nodename);
static int pgxcnode_remove_info(GTM_PGXCNodeInfo *node);
static int pgxcnode_add_info(GTM_PGXCNodeInfo *node);
static char *pgxcnode_copy_char(const char *str);

#define pgxcnode_type_equal(type1,type2) (type1 == type2)
#define pgxcnode_port_equal(port1,port2) (port1 == port2)

size_t
pgxcnode_get_all(GTM_PGXCNodeInfo **data, size_t maxlen, bool locked)
{
    GTM_PGXCNodeInfoHashBucket *bucket;
    gtm_ListCell *elem;
    int node = 0;
    int i;

    if (!locked)
        GTM_RWLockAcquire(&PGXCNodesLock, GTM_LOCKMODE_READ);

    for (i = 0; i < NODE_HASH_TABLE_SIZE; i++)
    {
        bucket = &GTM_PGXCNodes[i];
        gtm_foreach(elem, bucket->nhb_list)
        {
            GTM_PGXCNodeInfo *curr_nodeinfo = NULL;

            curr_nodeinfo = (GTM_PGXCNodeInfo *) gtm_lfirst(elem);
            if (curr_nodeinfo != NULL)
            {
                data[node] = curr_nodeinfo;
                node++;
            }

            if (node == maxlen)
                break;
        }
    }
    
    if (!locked)
        GTM_RWLockRelease(&PGXCNodesLock);

    return node;
}

size_t
pgxcnode_find_by_type(GTM_PGXCNodeType type, GTM_PGXCNodeInfo **data, size_t maxlen)
{
    GTM_PGXCNodeInfoHashBucket *bucket;
    gtm_ListCell *elem;
    int node = 0;
    int i;

    GTM_RWLockAcquire(&PGXCNodesLock, GTM_LOCKMODE_READ);
    for (i = 0; i < NODE_HASH_TABLE_SIZE; i++)
    {
        bucket = &GTM_PGXCNodes[i];
        gtm_foreach(elem, bucket->nhb_list)
        {
            GTM_PGXCNodeInfo *cur = NULL;

            cur = (GTM_PGXCNodeInfo *) gtm_lfirst(elem);

            if (cur != NULL && cur->type == type)
            {
                data[node] = cur;
                elog(DEBUG1, "pgxcnode_find_by_type: cur=%p, ipaddress=%s", cur, cur->ipaddress);
                node++;
            }

            if (node == maxlen)
                break;
        }
    }
    GTM_RWLockRelease(&PGXCNodesLock);

    return node;
}

/*
 * Find the pgxcnode info structure for the given node type and number key.
 */
GTM_PGXCNodeInfo *
pgxcnode_find_info(GTM_PGXCNodeType type, char *node_name)
{
    uint32 hash = pgxcnode_gethash(node_name);
    GTM_PGXCNodeInfoHashBucket *bucket;
    gtm_ListCell *elem;
    GTM_PGXCNodeInfo *curr_nodeinfo = NULL;

    bucket = &GTM_PGXCNodes[hash];

    GTM_RWLockAcquire(&PGXCNodesLock, GTM_LOCKMODE_READ);
    gtm_foreach(elem, bucket->nhb_list)
    {
        curr_nodeinfo = (GTM_PGXCNodeInfo *) gtm_lfirst(elem);
        if (pgxcnode_type_equal(curr_nodeinfo->type, type) &&
            (strcmp(curr_nodeinfo->nodename, node_name) == 0))
            break;
        curr_nodeinfo = NULL;
    }

    GTM_RWLockRelease(&PGXCNodesLock);

    return curr_nodeinfo;
}

/*
 * Get the Hash Key depending on the node name
 * We do not except to have hundreds of nodes yet,
 * This function could be replaced by a better one
 * such as a double hash function indexed on type and Node Name
 */
static uint32
pgxcnode_gethash(char *nodename)
{
    int            i;
    int            length;
    int            value;
    uint32            hash = 0;

    if (nodename == NULL || nodename == '\0')
    {
        return 0;
    }

    length = strlen(nodename);

    value = 0x238F13AF * length;

    for (i = 0; i < length; i++)
    {
        value = value + ((nodename[i] << i * 5 % 24) & 0x7fffffff);
    }

    hash = (1103515243 * value + 12345) % 65537 & 0x00000FFF;

    return (hash % NODE_HASH_TABLE_SIZE);
}

/*
 * Remove a PGXC Node Info structure from the global hash table
 */
static int
pgxcnode_remove_info(GTM_PGXCNodeInfo *nodeinfo)
{
    uint32 hash = pgxcnode_gethash(nodeinfo->nodename);
    GTM_PGXCNodeInfoHashBucket   *bucket;

    bucket = &GTM_PGXCNodes[hash];

    GTM_RWLockAcquire(&PGXCNodesLock, GTM_LOCKMODE_WRITE);
    GTM_RWLockAcquire(&nodeinfo->node_lock, GTM_LOCKMODE_WRITE);

    bucket->nhb_list = gtm_list_delete(bucket->nhb_list, nodeinfo);

    GTM_RWLockRelease(&nodeinfo->node_lock);
    GTM_RWLockRelease(&PGXCNodesLock);

    return 0;
}

/*
 * Add a PGXC Node info structure to the global hash table
 */
static int
pgxcnode_add_info(GTM_PGXCNodeInfo *nodeinfo)
{// #lizard forgives
    uint32 hash = pgxcnode_gethash(nodeinfo->nodename);
    GTM_PGXCNodeInfoHashBucket   *bucket;
    gtm_ListCell *elem;

    bucket = &GTM_PGXCNodes[hash];

    GTM_RWLockAcquire(&PGXCNodesLock, GTM_LOCKMODE_WRITE);

    if (!GlobalTransactionIdIsValid(nodeinfo->reported_xmin))
        nodeinfo->reported_xmin = GTM_GlobalXmin;

    gtm_foreach(elem, bucket->nhb_list)
    {
        GTM_PGXCNodeInfo *curr_nodeinfo = NULL;
        curr_nodeinfo = (GTM_PGXCNodeInfo *) gtm_lfirst(elem);

        /* GTM Proxy are always registered as they do not have Identification numbers yet */
        if (pgxcnode_type_equal(curr_nodeinfo->type, nodeinfo->type) &&
            (strcmp(curr_nodeinfo->nodename, nodeinfo->nodename) == 0))
        {
            if (curr_nodeinfo->status == NODE_CONNECTED)
            {
                /*
                 * There are two ways nodes get registered on the GTM, either
                 * via ClusterMonitor or when a distribution session is
                 * started. We differentiate between them. While its okay to
                 * see duplicate messages for session registration or a session
                 * registration message when the node is already registered by
                 * the ClusterMonitor. Otherwise, return an error.
                 *
                 * The session registration is also converted into node
                 * registration if the second message comes late
                 */
                if (!curr_nodeinfo->is_session && !nodeinfo->is_session)
                    ereport(LOG,
                        (EEXIST,
                         errmsg("Node with the given ID number already exists - %s %d:%d",
                            nodeinfo->nodename, nodeinfo->status,
                            nodeinfo->type )));
                else if (curr_nodeinfo->is_session && !nodeinfo->is_session)
                {
                    curr_nodeinfo->is_session = false;
                    GTM_RWLockRelease(&PGXCNodesLock);
                    return 0;
                }
                else if (!curr_nodeinfo->is_session && nodeinfo->is_session)
                {
                    GTM_RWLockRelease(&PGXCNodesLock);
                    return 0;
                }
                GTM_RWLockRelease(&PGXCNodesLock);
                return EEXIST;
            }
            else
            {
                /*
                 * Node has been disconnected abruptly.
                 * And we are sure that disconnections are not done by other node
                 * trying to use the same ID.
                 * So check if its data (port, datafolder and remote IP) has changed
                 * and modify it.
                 */
                if (!pgxcnode_port_equal(curr_nodeinfo->port, nodeinfo->port))
                    curr_nodeinfo->port = nodeinfo->port;

                if (strlen(curr_nodeinfo->datafolder) == strlen(nodeinfo->datafolder))
                {
                    if (memcpy(curr_nodeinfo->datafolder,
                               nodeinfo->datafolder,
                               strlen(nodeinfo->datafolder)) != 0)
                    {
                        pfree(curr_nodeinfo->ipaddress);
                        curr_nodeinfo->ipaddress = nodeinfo->ipaddress;
                    }
                }

                if (strlen(curr_nodeinfo->ipaddress) == strlen(nodeinfo->ipaddress))
                {
                    if (memcpy(curr_nodeinfo->datafolder,
                               nodeinfo->datafolder,
                               strlen(nodeinfo->datafolder)) != 0)
                    {
                        pfree(curr_nodeinfo->datafolder);
                        curr_nodeinfo->datafolder = nodeinfo->datafolder;
                    }
                }

                /* Reconnect a disconnected node */
                curr_nodeinfo->status = NODE_CONNECTED;

                /* Set socket number with the new one */
                curr_nodeinfo->socket = nodeinfo->socket;
                GTM_RWLockRelease(&PGXCNodesLock);
                return 0;
            }
        }
    }

    /*
     * Safe to add the structure to the list
     */
    bucket->nhb_list = gtm_lappend(bucket->nhb_list, nodeinfo);
    GTM_RWLockRelease(&PGXCNodesLock);

    return 0;
}

/*
 * Makes a copy of given string in TopMostMemoryContext
 */
static char *
pgxcnode_copy_char(const char *str)
{
    char *retstr = NULL;

    /*
     * We must use the TopMostMemoryContext because the node information is
     * not bound to a thread and can outlive any of the thread specific
     * contextes.
     */
    retstr = (char *) MemoryContextAlloc(TopMostMemoryContext,
                                         strlen(str) + 1);

    if (retstr == NULL)
        ereport(ERROR, (ENOMEM, errmsg("Out of memory")));

    memcpy(retstr, str, strlen(str));
    retstr[strlen(str)] = '\0';

    return retstr;
}

/*
 * Unregister the given node
 */
int
Recovery_PGXCNodeUnregister(GTM_PGXCNodeType type, char *node_name, bool in_recovery, int socket)
{
    GTM_PGXCNodeInfo *nodeinfo = pgxcnode_find_info(type, node_name);

    if (nodeinfo != NULL)
    {
        /*
         * Unregistration has to be made by the same connection as the one used for registration
         * or the one that reconnected the node.
         */
        pgxcnode_remove_info(nodeinfo);

        /* Add a record to file on disk saying that this node has been unregistered correctly */
        if (!in_recovery)
        {
            Recovery_RecordRegisterInfo(nodeinfo, false);
        }

        elog(LOG, "Node:%s host:%s port:%d forder:%s unregistered.", node_name, 
                                                                     nodeinfo->ipaddress ? nodeinfo->ipaddress : "NULL", 
                                                                     nodeinfo->port, 
                                                                     nodeinfo->datafolder ? nodeinfo->datafolder : "NULL");
        
        pfree(nodeinfo->nodename);
        if (nodeinfo->ipaddress)
            pfree(nodeinfo->ipaddress);

        if (nodeinfo->datafolder)
            pfree(nodeinfo->datafolder);

        if (nodeinfo->sessions)
            pfree(nodeinfo->sessions);

        pfree(nodeinfo);        
        
    }
    else
    {
        return EINVAL;
    }
    return 0;
}

int
Recovery_PGXCNodeRegister(GTM_PGXCNodeType    type,
                          char            *nodename,
                          GTM_PGXCNodePort    port,
                          char            *proxyname,
                          GTM_PGXCNodeStatus    status,
                          char            *ipaddress,
                          char            *datafolder,
                          bool            in_recovery,
                          int            socket,
                          bool            is_session)
{
    GTM_PGXCNodeInfo *nodeinfo = NULL;
    int errcode = 0;

    nodeinfo = (GTM_PGXCNodeInfo *) palloc0(sizeof(GTM_PGXCNodeInfo));

    if (nodeinfo == NULL)
        ereport(ERROR, (ENOMEM, errmsg("Out of memory")));

    GTM_RWLockInit(&nodeinfo->node_lock);

    /* Fill in structure */
    nodeinfo->type = type;
    if (nodename)
        nodeinfo->nodename = pgxcnode_copy_char(nodename);
    nodeinfo->port = port;
    if (proxyname)
        nodeinfo->proxyname = pgxcnode_copy_char(proxyname);
    if (datafolder)
        nodeinfo->datafolder = pgxcnode_copy_char(datafolder);
    if (ipaddress)
        nodeinfo->ipaddress = pgxcnode_copy_char(ipaddress);
    nodeinfo->status = status;
    nodeinfo->socket = socket;
    nodeinfo->reported_xmin = InvalidGlobalTransactionId;
    nodeinfo->reported_xmin_time = 0;
    nodeinfo->is_session = is_session;

    elog(DEBUG8, "Recovery_PGXCNodeRegister Request info: type=%d, nodename=%s, port=%d," \
              "datafolder=%s, ipaddress=%s, status=%d",
              type, nodename, port, datafolder, ipaddress, status);
    elog(LOG, "Recovery_PGXCNodeRegister Node info: type=%d, nodename=%s, port=%d, "\
              "datafolder=%s, ipaddress=%s, status=%d",
              nodeinfo->type, nodeinfo->nodename, nodeinfo->port,
              nodeinfo->datafolder, nodeinfo->ipaddress, nodeinfo->status);

    /* Add PGXC Node Info to the global hash table */
    errcode = pgxcnode_add_info(nodeinfo);

    /*
     * Add a Record to file disk saying that this node
     * with given data has been correctly registered
     */
    if (!in_recovery && errcode == 0)
    {
        Recovery_RecordRegisterInfo(nodeinfo, true);
    }

    return errcode;
}




/*
 * Called at GTM shutdown, rewrite on disk register information
 * and write only data of nodes currently registered.
 */
void
Recovery_SaveRegisterInfo(void)
{// #lizard forgives
    GTM_PGXCNodeInfoHashBucket *bucket;
    gtm_ListCell *elem;
    GTM_PGXCNodeInfo *nodeinfo = NULL;
    int hash, ctlfd;
    char filebkp[GTM_NODE_FILE_MAX_PATH];

    GTM_RWLockAcquire(&RegisterFileLock, GTM_LOCKMODE_WRITE);

    /* Create a backup file in case their is a problem during file writing */
    sprintf(filebkp, "%s.bkp", GTMPGXCNodeFile);

    ctlfd = open(filebkp, O_WRONLY | O_CREAT | O_TRUNC,
                 S_IRUSR | S_IWUSR);

    if (ctlfd < 0)
    {
        GTM_RWLockRelease(&RegisterFileLock);
        return;
    }

    GTM_RWLockAcquire(&PGXCNodesLock, GTM_LOCKMODE_READ);
    for (hash = 0; hash < NODE_HASH_TABLE_SIZE; hash++)
    {
        bucket = &GTM_PGXCNodes[hash];

        /* Write one by one information about registered nodes */
        gtm_foreach(elem, bucket->nhb_list)
        {
            int len;

            nodeinfo = (GTM_PGXCNodeInfo *) gtm_lfirst(elem);
            if (nodeinfo == NULL)
                break;

            GTM_RWLockAcquire(&nodeinfo->node_lock, GTM_LOCKMODE_READ);

            write(ctlfd, &NodeRegisterMagic, sizeof (NodeRegisterMagic));

            write(ctlfd, &nodeinfo->type, sizeof (GTM_PGXCNodeType));
            if (nodeinfo->nodename)
            {
                len = strlen(nodeinfo->nodename);
                write(ctlfd, &len, sizeof(uint32));
                write(ctlfd, nodeinfo->nodename, len);
            }
            else
            {
                len = 0;
                write(ctlfd, &len, sizeof(uint32));
            }

            write(ctlfd, &nodeinfo->port, sizeof (GTM_PGXCNodePort));

            if (nodeinfo->proxyname)
            {
                len = strlen(nodeinfo->proxyname);
                write(ctlfd, &len, sizeof(uint32));
                write(ctlfd, nodeinfo->proxyname, len);
            }
            else
            {
                len = 0;
                write(ctlfd, &len, sizeof(uint32));
            }

            write(ctlfd, &nodeinfo->status, sizeof (GTM_PGXCNodeStatus));

            if (nodeinfo->ipaddress)
            {
                len = strlen(nodeinfo->ipaddress);
                write(ctlfd, &len, sizeof(uint32));
                write(ctlfd, nodeinfo->ipaddress, len);
            }
            else
            {
                len = 0;
                write(ctlfd, &len, sizeof(uint32));
            }

            if (nodeinfo->datafolder)
            {
                len = strlen(nodeinfo->datafolder);
                write(ctlfd, &len, sizeof(uint32));
                write(ctlfd, nodeinfo->datafolder, len);
            }
            else
            {
                len = 0;
                write(ctlfd, &len, sizeof(uint32));
            }

            write(ctlfd, &NodeEndMagic, sizeof(NodeEndMagic));

            GTM_RWLockRelease(&nodeinfo->node_lock);
        }
    }
    GTM_RWLockRelease(&PGXCNodesLock);

    close(ctlfd);

    /* Replace former file by backup file */
    if (rename(filebkp, GTMPGXCNodeFile) < 0)
    {
        elog(LOG, "Cannot save register file");
    }

    GTM_RWLockRelease(&RegisterFileLock);
}

/*
 * Add a Register or Unregister record on PGXC Node file on disk.
 */
void
Recovery_RecordRegisterInfo(GTM_PGXCNodeInfo *nodeinfo, bool is_register)
{// #lizard forgives
    int ctlfd;
    int len;

	if (nodeinfo == NULL)
	{
		return;
	}

    GTM_RWLockAcquire(&RegisterFileLock, GTM_LOCKMODE_WRITE);

    ctlfd = open(GTMPGXCNodeFile, O_WRONLY | O_CREAT | O_APPEND,
                 S_IRUSR | S_IWUSR);

	if (ctlfd == -1)
    {
        GTM_RWLockRelease(&RegisterFileLock);
        return;
    }

    GTM_RWLockAcquire(&nodeinfo->node_lock, GTM_LOCKMODE_READ);

    if (is_register)
        write(ctlfd, &NodeRegisterMagic, sizeof (NodeRegisterMagic));
    else
        write(ctlfd, &NodeUnregisterMagic, sizeof (NodeUnregisterMagic));

    write(ctlfd, &nodeinfo->type, sizeof (GTM_PGXCNodeType));

    if (nodeinfo->nodename)
    {
        len = strlen(nodeinfo->nodename);
        write(ctlfd, &len, sizeof(uint32));
        write(ctlfd, nodeinfo->nodename, len);
    }
    else
    {
        len = 0;
        write(ctlfd, &len, sizeof(uint32));
    }

    if (is_register)
    {
        int len;

        write(ctlfd, &nodeinfo->port, sizeof (GTM_PGXCNodePort));

        if (nodeinfo->proxyname)
        {
            len = strlen(nodeinfo->proxyname);
            write(ctlfd, &len, sizeof(uint32));
            write(ctlfd, nodeinfo->proxyname, len);
        }
        else
        {
            len = 0;
            write(ctlfd, &len, sizeof(uint32));
        }

        write(ctlfd, &nodeinfo->status, sizeof (GTM_PGXCNodeStatus));

        if (nodeinfo->ipaddress)
        {
            len = strlen(nodeinfo->ipaddress);
            write(ctlfd, &len, sizeof(uint32));
            write(ctlfd, nodeinfo->ipaddress, len);
        }
        else
        {
            len = 0;
            write(ctlfd, &len, sizeof(uint32));
        }

        if (nodeinfo->datafolder)
        {
            len = strlen(nodeinfo->datafolder);
            write(ctlfd, &len, sizeof(uint32));
            write(ctlfd, nodeinfo->datafolder, len);
        }
        else
        {
            len = 0;
            write(ctlfd, &len, sizeof(uint32));
        }
    }

    write(ctlfd, &NodeEndMagic, sizeof(NodeEndMagic));

    GTM_RWLockRelease(&nodeinfo->node_lock);

    close(ctlfd);
    GTM_RWLockRelease(&RegisterFileLock);
}

void
Recovery_SaveRegisterFileName(char *dir)
{
    if (!dir)
        return;

    sprintf(GTMPGXCNodeFile, "%s/%s", dir, GTM_NODE_FILE);
}

/*
 * Disconnect node whose master connection has been cut with GTM
 */
void
Recovery_PGXCNodeDisconnect(Port *myport)
{
    GTM_PGXCNodeType    type = myport->remote_type;
    char            *nodename = myport->node_name;
    GTM_PGXCNodeInfo    *nodeinfo = NULL;
    MemoryContext        oldContext;

    /* Only a master connection can disconnect a node */
    if (!myport->is_postmaster)
        return;

    /*
     * We must use the TopMostMemoryContext because the Node ID information is
     * not bound to a thread and can outlive any of the thread specific
     * contextes.
     */
    oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

    nodeinfo = pgxcnode_find_info(type, nodename);

    if (nodeinfo != NULL)
    {
        /*
         * Disconnection cannot be made with another socket than the one used for registration.
         * socket may have a dummy value (-1) under GTM standby node.
         */
        if (nodeinfo->socket >= 0 && myport->sock != nodeinfo->socket)
            return;

        GTM_RWLockAcquire(&nodeinfo->node_lock, GTM_LOCKMODE_WRITE);

        nodeinfo->status = NODE_DISCONNECTED;
        nodeinfo->socket = 0;

        GTM_RWLockRelease(&nodeinfo->node_lock);
    }

    MemoryContextSwitchTo(oldContext);
}

int
Recovery_PGXCNodeBackendDisconnect(GTM_PGXCNodeType type, char *nodename, int socket)
{
    GTM_PGXCNodeInfo *nodeinfo = pgxcnode_find_info(type, nodename);
    int errcode = 0;


    if (nodeinfo != NULL)
    {
        /*
         * A node can be only disconnected by the same connection as the one used for registration
         * or reconnection.
         */
        if (socket != nodeinfo->socket)
            return -1;

        GTM_RWLockAcquire(&nodeinfo->node_lock, GTM_LOCKMODE_WRITE);

        nodeinfo->status = NODE_DISCONNECTED;
        nodeinfo->socket = 0;

        GTM_RWLockRelease(&nodeinfo->node_lock);
    }
    else
        errcode = -1;

    return errcode;
}

/*
 * Register active distributed session. If another session with specified
 * BackendId already exists return the PID of the session, so caller could clean
 * it up. Otherwise return 0.
 */
int
Recovery_PGXCNodeRegisterCoordProcess(char *coord_node, int coord_procid,
                                      int coord_backendid)
{// #lizard forgives
    GTM_PGXCNodeInfo   *nodeinfo;
    int                    i;

    /*
     * Get the registration record for the coordinator node. If not specified,
     * register it now.
     */
    nodeinfo = pgxcnode_find_info(GTM_NODE_COORDINATOR, coord_node);

    while (nodeinfo == NULL)
    {
        int errcode = Recovery_PGXCNodeRegister(GTM_NODE_COORDINATOR, coord_node, 0, NULL,
                                      NODE_CONNECTED,
                                      NULL, NULL, false, 0, true);

        /*
         * If another thread registers before we get a chance, just look for
         * the nodeinfo again
         */
        if (errcode != 0 && errcode != EEXIST)
            return 0;

        nodeinfo = pgxcnode_find_info(GTM_NODE_COORDINATOR, coord_node);
    }

    /* Iterate over the existing sessions */
    GTM_RWLockAcquire(&nodeinfo->node_lock, GTM_LOCKMODE_WRITE);
    for (i = 0; i < nodeinfo->num_sessions; i++)
    {
        if (nodeinfo->sessions[i].gps_coord_proc_id == coord_procid)
        {
            /*
             * Already registered, nothing todo.
             * May be session lost the GTM connection and now is reconnecting.
             */
            GTM_RWLockRelease(&nodeinfo->node_lock);
            return 0;
        }
        if (nodeinfo->sessions[i].gps_coord_backend_id == coord_backendid)
        {
            /*
             * Reuse the entry and return PID of the previous session.
             */
            int result = nodeinfo->sessions[i].gps_coord_proc_id;
            elog(DEBUG1, "New session %s:%d with existing BackendId %d",
                 coord_node, coord_procid, coord_backendid);
            nodeinfo->sessions[i].gps_coord_proc_id = coord_procid;
            GTM_RWLockRelease(&nodeinfo->node_lock);
            return result;
        }
    }
    /* Session not found, populate new entry */
    elog(DEBUG1, "New session %s:%d with BackendId %d",
         coord_node, coord_procid, coord_backendid);
    if (nodeinfo->num_sessions == nodeinfo->max_sessions)
    {
        /* need to extend array */
#define INIT_SESSIONS 256
        if (nodeinfo->max_sessions == 0)
        {
            nodeinfo->sessions = (GTM_PGXCSession *)
                    palloc(INIT_SESSIONS * sizeof(GTM_PGXCSession));
            nodeinfo->max_sessions = INIT_SESSIONS;
        }
        else
        {
            int newsize = nodeinfo->max_sessions * 2;
            nodeinfo->sessions = (GTM_PGXCSession *)
                    repalloc(nodeinfo->sessions,
                             newsize * sizeof(GTM_PGXCSession));
            nodeinfo->max_sessions = newsize;
        }
    }
    nodeinfo->sessions[nodeinfo->num_sessions].gps_coord_proc_id = coord_procid;
    nodeinfo->sessions[nodeinfo->num_sessions].gps_coord_backend_id = coord_backendid;
    nodeinfo->num_sessions++;
    GTM_RWLockRelease(&nodeinfo->node_lock);

    return 0;
}

/*
 * Process MSG_BACKEND_DISCONNECT
 *
 * A Backend has disconnected on a Proxy.
 * If this backend is postmaster, mark the referenced node as disconnected.
 */
void
ProcessPGXCNodeBackendDisconnect(Port *myport, StringInfo message)
{
    MemoryContext        oldContext;
    GTM_PGXCNodeType    type;
    bool            is_postmaster;
    char            node_name[NI_MAXHOST];
    int         len;

    is_postmaster = pq_getmsgbyte(message);

    if (is_postmaster)
    {
        /* Read Node Type and name */
        memcpy(&type, pq_getmsgbytes(message, sizeof (GTM_PGXCNodeType)), sizeof (GTM_PGXCNodeType));

        /* Read Node name */
        len = pq_getmsgint(message, sizeof (int));
        if (len >= NI_MAXHOST)
        {
            elog(LOG, "Invalid node name length %d", len);
            return;
        }
        memcpy(node_name, (char *)pq_getmsgbytes(message, len), len);
        node_name[len] = '\0';
    }

    pq_getmsgend(message);

    if (!is_postmaster)
        return; /* Nothing to do */

    /*
     * We must use the TopMostMemoryContext because the Node ID information is
     * not bound to a thread and can outlive any of the thread specific
     * contextes.
     */
    oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

    if (Recovery_PGXCNodeBackendDisconnect(type, node_name, myport->sock) < 0)
    {
        elog(LOG, "Cannot disconnect Unregistered node");
    }

    MemoryContextSwitchTo(oldContext);

#ifndef __XLOG__
    /*
     * Forwarding MSG_BACKEND_DISCONNECT message to GTM standby.
     * No need to wait any response.
     */
    if (GetMyConnection(myport)->standby)
    {
        int _rc;
        GTM_Conn *oldconn = GetMyConnection(myport)->standby;
        int count = 0;

        elog(DEBUG1, "forwarding MSG_BACKEND_DISCONNECT to standby GTM %p.",
                  GetMyConnection(myport)->standby);

retry:
        _rc = backend_disconnect(GetMyConnection(myport)->standby,
                     is_postmaster,
                     type,
                     node_name);

        if (gtm_standby_check_communication_error(myport, &count, oldconn))
            goto retry;

        elog(DEBUG1, "MSG_BACKEND_DISCONNECT rc=%d done.", _rc);
    }
#endif
}

void
GTM_InitNodeManager(void)
{
    int ii;

    for (ii = 0; ii < NODE_HASH_TABLE_SIZE; ii++)
    {
        GTM_PGXCNodes[ii].nhb_list = gtm_NIL;
    }

    GTM_RWLockInit(&RegisterFileLock);
    GTM_RWLockInit(&PGXCNodesLock);
}

/* 
 * Set to 600 seconds, but should be a few multiple for cluster monitor naptime
 */ 
#define GTM_REPORT_XMIN_DELAY_THRESHOLD (600 * 1000)

GlobalTransactionId
GTM_HandleGlobalXmin(GTM_PGXCNodeType type, char *node_name,
        GlobalTransactionId reported_xmin, int *errcode)
{// #lizard forgives
    GTM_PGXCNodeInfo *all_nodes[MAX_NODES];
    int num_nodes;
    GTM_Timestamp current_time;
    GTM_PGXCNodeInfo *mynodeinfo;
    int ii;
    GlobalTransactionId global_xmin;
    gtm_ListCell *elem;

    *errcode = 0;

    elog(DEBUG1, "node_name: %s, reported_xmin: %d, global_xmin: %d",
            node_name, reported_xmin,
            GTM_GlobalXmin);
    
    mynodeinfo = pgxcnode_find_info(type, node_name);
    if (mynodeinfo == NULL)
    {
        *errcode = GTM_ERRCODE_NODE_NOT_REGISTERED;
        elog(LOG, "GTM_ERRCODE_NODE_NOT_REGISTERED - node_name %s", node_name);
        return InvalidGlobalTransactionId;
    }

    GTM_RWLockAcquire(&mynodeinfo->node_lock, GTM_LOCKMODE_WRITE);

    /*
     * If we were excluded from the GlobalXmin calculation because we failed to
     * report our status for GTM_REPORT_XMIN_DELAY_THRESHOLD seconds, we can
     * only join the cluster back iff the GlobalXmin hasn't advanced beyond
     * what we'd last reported. Otherwise its possible that some nodes are way
     * ahead of us. So we must give up and restart all over again (this is done
     * via PANIC in Cluster Monitor process on the remote side
     *
     * The GTM_REPORT_XMIN_DELAY_THRESHOLD is of many order higher than the
     * naptime used by Cluster Monitor. So unless there was a network outage or
     * the remote node got serious busy such that the Cluster Monitor did not
     * get opportunity to report xmin in a timely fashion, we shouldn't get
     * into this situation often.
     *
     */
    if ((mynodeinfo->excluded) &&
            GlobalTransactionIdPrecedes(reported_xmin, GTM_GlobalXmin))
    {
        *errcode = GTM_ERRCODE_NODE_EXCLUDED;

        /*
         * This node is joining back the cluster after being excluded from the
         * GTM_GlobalXmin calculation because of timeout, disconnection or node
         * failure. In such cases, we send appropriate error back to the node
         * and let it handle the situation. To ensure that our GTM_GlobalXmin
         * does not keep advancing while the node is trying to join back the
         * cluster, we temporarily set reported_xmin to the current
         * GTM_GlobalXmin and wait to see if the node finally catches up.
         *
         * Note: If the node had old transaction running while it was excluded
         * by the GTM, it will fail the consistency checks and restart itself.
         */
        mynodeinfo->joining = true;
        mynodeinfo->reported_xmin_time = GTM_TimestampGetCurrent();
        mynodeinfo->reported_xmin = GTM_GlobalXmin;

        GTM_RWLockRelease(&mynodeinfo->node_lock);
        elog(LOG, "GTM_ERRCODE_NODE_EXCLUDED - node_name %s, reported_xmin %d "
                "previously reported_xmin %d, GTM_GlobalXmin %d", node_name,
                reported_xmin,
                mynodeinfo->reported_xmin,
                GTM_GlobalXmin);
        return InvalidGlobalTransactionId;
    }

    /*
     * The remote node must not report a xmin which precedes the GTM_GlobalXmin
     * we have already computed. If it ever happens, send an error back and let
     * the remote node handle it, possibly restarting itself
     */
    if (GlobalTransactionIdPrecedes(reported_xmin, GTM_GlobalXmin))
    {
        *errcode = GTM_ERRCODE_TOO_OLD_XMIN;

        mynodeinfo->joining = true;

        /*
         * When node registers from the first time, the reported_xmin is set
         * to GTM_GlobalXmin and what we receive from the node would most
         * likely precedes that, especially because nodes' latestCompletedXid
         * could precede our GTM_GlobalXmin. The node is prepared to handle
         * that case, but we should avoid logging an useless and often
         * confusing log message.
         */
        if (mynodeinfo->reported_xmin_time)
            elog(LOG, "GTM_ERRCODE_TOO_OLD_XMIN - node_name %s, reported_xmin %d, "
                    "previously reported_xmin %d, GTM_GlobalXmin %d", node_name,
                    reported_xmin, mynodeinfo->reported_xmin, GTM_GlobalXmin);

        mynodeinfo->reported_xmin_time = GTM_TimestampGetCurrent();
        mynodeinfo->reported_xmin = GTM_GlobalXmin;

        GTM_RWLockRelease(&mynodeinfo->node_lock);
        return InvalidGlobalTransactionId;
    }

    elog(DEBUG1, "node_name: %s, reported_xmin: %d, nodeinfo->reported_xmin: %d",
            mynodeinfo->nodename, reported_xmin,
            mynodeinfo->reported_xmin);

    mynodeinfo->reported_xmin = reported_xmin;

    /*
     * Node joined back, set both excluded and joining to false
     */
    if (mynodeinfo->excluded)
    {
        mynodeinfo->excluded = false;
        mynodeinfo->joining = false;
    }
    mynodeinfo->reported_xmin_time = current_time = GTM_TimestampGetCurrent();

    GTM_RWLockRelease(&mynodeinfo->node_lock);
    
    /* Compute global xmin */
    global_xmin = InvalidGlobalTransactionId;

    /*
     * Hold the PGXCNodesLock in READ mode until we are done with the
     * GlobalXmin calculation. We don't want any new node to join the cluster,
     * but its OK for other nodes to report and do these computation in
     * parallel. If the other guy beats us and advances the GlobalXmin beyond
     * what we compute, we accept that calculation
     */
    GTM_RWLockAcquire(&PGXCNodesLock, GTM_LOCKMODE_READ);
    num_nodes = pgxcnode_get_all(all_nodes, MAX_NODES, true);

    for (ii = 0; ii < num_nodes; ii++)
    {
        GTM_PGXCNodeInfo *nodeinfo = all_nodes[ii];

        elog(DEBUG1, "nodeinfo %p, type: %d, exclude %c, xmin %d, time %ld",
                nodeinfo, nodeinfo->type, nodeinfo->excluded ? 'T' : 'F',
                nodeinfo->reported_xmin, nodeinfo->reported_xmin_time);

        /*
         * If a node has not reported its status for
         * GTM_REPORT_XMIN_DELAY_THRESHOLD and neither in the process of
         * rejoining the cluster, don't include it in the GTM_GlobalXmin
         * calculation
         */
        if (nodeinfo->excluded && !nodeinfo->joining)
            continue;

        /*
         * Care only for datanodes and coordinators
         */
        if (nodeinfo->type != GTM_NODE_COORDINATOR && nodeinfo->type !=
                GTM_NODE_DATANODE)
            continue;

        if (nodeinfo->reported_xmin_time &&
            GTM_TimestampDifferenceExceeds(nodeinfo->reported_xmin_time,
                    current_time, GTM_REPORT_XMIN_DELAY_THRESHOLD))
        {
            elog(LOG, "Timediff exceeds threshold - last reporting time %ld, "
                    "current time %ld, last reported_xmin %d, reported_xmin %d,"
                    " - excluding the node %s from GlobalXmin calculation",
                    nodeinfo->reported_xmin_time, current_time,
                    reported_xmin, nodeinfo->reported_xmin,
                    nodeinfo->nodename);

            GTM_RWLockAcquire(&nodeinfo->node_lock, GTM_LOCKMODE_WRITE);
            if (GTM_TimestampDifferenceExceeds(nodeinfo->reported_xmin_time,
                        current_time, GTM_REPORT_XMIN_DELAY_THRESHOLD))
            {
                nodeinfo->excluded = true;
                nodeinfo->joining = false;
                GTM_RWLockRelease(&nodeinfo->node_lock);
                continue;
            }
            GTM_RWLockRelease(&nodeinfo->node_lock);
        }

        /*
         * Now grab the lock on the nodeinfo so that no further changes are
         * possible to its state.
         */
        GTM_RWLockAcquire(&nodeinfo->node_lock, GTM_LOCKMODE_READ);

        /* 
         * Just check again if the excluded state hasn't changed. Shouldn't
         * happen too often anyways
         */
        if (nodeinfo->excluded)
        {
            GTM_RWLockRelease(&nodeinfo->node_lock);
            continue;
        }

        GTM_RWLockRelease(&nodeinfo->node_lock);
    }

    /*
     * Now all nodes that must be excluded from GlobalXmin computation have
     * been marked correctly and xmin computed and set for an idle remote node,
     * if so. Lets compute the GlobalXmin
     */

    /*
     * GlobalXmin is capped by the latestCompletedXid. Since any future
     * additions/changes can't cross this horizon, it seems appropriate to use
     * this as upper bound for GlobalXmin computation
     */
    global_xmin = GTMTransactions.gt_latestCompletedXid;
    if (!GlobalTransactionIdIsValid(global_xmin))
        global_xmin = FirstNormalGlobalTransactionId;
    else
        GlobalTransactionIdAdvance(global_xmin);

    GTM_RWLockAcquire(&GTMTransactions.gt_TransArrayLock, GTM_LOCKMODE_READ);
    gtm_foreach(elem, GTMTransactions.gt_open_transactions)
    {
        volatile GTM_TransactionInfo *gtm_txninfo = (GTM_TransactionInfo *)gtm_lfirst(elem);
        GlobalTransactionId xid;

        /* Update globalxmin to be the smallest valid xmin */
        xid = gtm_txninfo->gti_xmin;        /* fetch just once */
        if (GlobalTransactionIdIsNormal(xid) &&
            GlobalTransactionIdPrecedes(xid, global_xmin))
            global_xmin = xid;

        /* Fetch xid just once - see GetNewTransactionId */
        xid = gtm_txninfo->gti_gxid;
        if (GlobalTransactionIdIsNormal(xid) &&
            GlobalTransactionIdPrecedes(xid, global_xmin))
            global_xmin = xid;
    }
    GTM_RWLockRelease(&GTMTransactions.gt_TransArrayLock);
    
    for (ii = 0; ii < num_nodes; ii++)
    {
        GlobalTransactionId xid;
        GTM_PGXCNodeInfo *nodeinfo = all_nodes[ii];

        if (nodeinfo->excluded)
        {
            elog(DEBUG1, "Node %s is excluded (last reported time %ld, current"
                " time %ld, last reported_xmin %d. This node won't be included in "
                "GTM_GlobalXmin computation",
                nodeinfo->nodename, nodeinfo->reported_xmin_time,
                GTM_TimestampGetCurrent(), nodeinfo->reported_xmin);
            continue;
        }

        if (nodeinfo->type != GTM_NODE_COORDINATOR && nodeinfo->type !=
                GTM_NODE_DATANODE)
            continue;

        /* Fetch once */
        xid = nodeinfo->reported_xmin;
        if (!GlobalTransactionIdIsValid(global_xmin))
            global_xmin = xid;
        else if (GlobalTransactionIdPrecedes(xid, global_xmin))
            global_xmin = xid;
    }
    GTM_RWLockRelease(&PGXCNodesLock);

    /*
     * Now update the GTM_GlobalXmin and also record the time when its updated
     * but iff someone else has not beaten us in the calculation already, which
     * is possible because we did the calculation holding only a READ lock on
     * PGXCNodesLock
     */
    if (GlobalTransactionIdIsValid(global_xmin))
    {
        GTM_RWLockAcquire(&PGXCNodesLock, GTM_LOCKMODE_WRITE);
        if (GlobalTransactionIdPrecedes(GTM_GlobalXmin, global_xmin))
        {
            elog(DEBUG1, "Computed new GTM_GlobalXmin %d, old value was %d",
                    global_xmin, GTM_GlobalXmin);
            GTM_GlobalXmin = global_xmin;
            GTM_GlobalXminComputedTime = current_time;
        }
        else if (GlobalTransactionIdFollows(GTM_GlobalXmin, global_xmin))
        {
            elog(LOG, "The current (old) GTM_GlobalXmin %d is newer than what "
                    "we just computed %d - keeping the current value",
                    GTM_GlobalXmin, global_xmin);
            global_xmin = GTM_GlobalXmin;
        }
        GTM_RWLockRelease(&PGXCNodesLock);
    }


    return global_xmin;
}
