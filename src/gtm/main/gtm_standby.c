/*-------------------------------------------------------------------------
 *
 * gtm_standby.c
 *		Functionalities of GTM Standby
 *
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *		src/gtm/common/gtm_standby.c
 *
 *-------------------------------------------------------------------------
 */
#include <gtm/gtm_xlog.h>
#include <gtm/gtm_c.h>
#include "gtm/gtm_standby.h"

#include "gtm/elog.h"
#include "gtm/gtm.h"
#include "gtm/gtm_c.h"
#include "gtm/standby_utils.h"
#include "gtm/gtm_client.h"
#include "gtm/gtm_seq.h"
#include "gtm/gtm_serialize.h"
#include "gtm/gtm_utils.h"
#include "gtm/register.h"
#ifdef __TBASE__
#include "gtm/gtm_store.h"
#endif

#ifdef __XLOG__

GTM_StandbyReplication *g_StandbyReplication;

GTM_RWLock              g_SyncReplicatioLck;
GTM_StandbyReplication *g_SyncReplication;

extern GTMControlHeader  *g_GTM_Store_Header;

#endif

GTM_Conn *GTM_ActiveConn = NULL;
static char standbyHostName[NI_MAXHOST];
static char standbyNodeName[NI_MAXHOST];
static int  standbyPortNumber;
static char *standbyDataDir;
extern char  *NodeName;
extern int	 GTMPortNumber;

#ifndef __XLOG__
static GTM_Conn *gtm_standby_connect_to_standby_int(int *report_needed);
#endif
static GTM_Conn *gtm_standby_connectToActiveGTM(int timeout);
static void AddBackupLabel(uint64 segment_no);

/* Defined in main.c */
extern char *NodeName;
extern int   max_wal_sender;
extern int   GTM_Standby_Connetion_Timeout;


int
gtm_standby_start_startup(int timeout)
{
	GTM_ActiveConn = gtm_standby_connectToActiveGTM(timeout);
	if (GTM_ActiveConn == NULL || GTMPQstatus(GTM_ActiveConn) != CONNECTION_OK)
	{
		int save_errno = errno;
		if(GTM_ActiveConn)
			elog(LOG, "can not connect to GTM: %s %m", GTMPQerrorMessage(GTM_ActiveConn));
		else
			elog(LOG, "connection is null: %m");

		errno = save_errno;
		if(GTM_ActiveConn)
			GTMPQfinish(GTM_ActiveConn);
		GTM_ActiveConn = NULL;
		return 0;
	}

	elog(LOG, "Connection established to the GTM active.");

	return 1;
}

int
gtm_standby_finish_startup(void)
{
	elog(DEBUG1, "Closing a startup connection...");

	GTMPQfinish(GTM_ActiveConn);
	GTM_ActiveConn = NULL;

	elog(DEBUG1, "A startup connection closed.");
	return 1;
}

int
gtm_standby_restore_next_gxid(void)
{
	GlobalTransactionId next_gxid = InvalidGlobalTransactionId;
#ifdef __TBASE__	
	next_gxid = get_next_gxid(GTM_ActiveConn);
	GTM_RestoreStoreInfo(next_gxid, true);
#else
	next_gxid = get_next_gxid(GTM_ActiveConn);
	GTM_RestoreTxnInfo(NULL, next_gxid, NULL, true);
#endif
	elog(DEBUG1, "Restoring the next GXID done.");
	return 1;
}

int
gtm_standby_restore_sequence(void)
{
#ifndef __TBASE__
	GTM_SeqInfo *seq_list;
	int num_seq;
	int i;
	
	/*
	 * Restore sequence data.
	 */
	num_seq = get_sequence_list(GTM_ActiveConn, &seq_list);

	for (i = 0; i < num_seq; i++)
	{
		GTM_SeqRestore(seq_list[i].gs_key,
					   seq_list[i].gs_increment_by,
					   seq_list[i].gs_min_value,
					   seq_list[i].gs_max_value,
					   seq_list[i].gs_init_value,
					   seq_list[i].gs_value,
					   seq_list[i].gs_state,
					   seq_list[i].gs_cycle,
					   seq_list[i].gs_called);
	}	

	elog(DEBUG1, "Restoring sequences done.");
#endif
	return 1;
}

int
gtm_standby_restore_gxid(void)
{
#ifndef __TBASE__
	int num_txn;
	GTM_Transactions txn;
	int i;
	/*
	 * Restore gxid data.
	 */
	num_txn = get_txn_gxid_list(GTM_ActiveConn, &txn);

	GTM_RWLockAcquire(&GTMTransactions.gt_XidGenLock, GTM_LOCKMODE_WRITE);
	GTM_RWLockAcquire(&GTMTransactions.gt_TransArrayLock, GTM_LOCKMODE_WRITE);

	GTMTransactions.gt_txn_count = txn.gt_txn_count;
	GTMTransactions.gt_gtm_state = txn.gt_gtm_state;
	GTMTransactions.gt_nextXid = txn.gt_nextXid;
	GTMTransactions.gt_oldestXid = txn.gt_oldestXid;
	GTMTransactions.gt_xidVacLimit = txn.gt_xidVacLimit;
	GTMTransactions.gt_xidWarnLimit = txn.gt_xidWarnLimit;
	GTMTransactions.gt_xidStopLimit = txn.gt_xidStopLimit;
	GTMTransactions.gt_xidWrapLimit = txn.gt_xidWrapLimit;
	GTMTransactions.gt_latestCompletedXid = txn.gt_latestCompletedXid;
	GTMTransactions.gt_recent_global_xmin = txn.gt_recent_global_xmin;
	GTMTransactions.gt_lastslot = txn.gt_lastslot;

	for (i = 0; i < num_txn; i++)
	{
		int handle = txn.gt_transactions_array[i].gti_handle;

		GTMTransactions.gt_transactions_array[handle].gti_handle = txn.gt_transactions_array[i].gti_handle;

		GTMTransactions.gt_transactions_array[handle].gti_client_id = txn.gt_transactions_array[i].gti_client_id;
		GTMTransactions.gt_transactions_array[handle].gti_in_use = txn.gt_transactions_array[i].gti_in_use;
		GTMTransactions.gt_transactions_array[handle].gti_gxid = txn.gt_transactions_array[i].gti_gxid;
		GTMTransactions.gt_transactions_array[handle].gti_state = txn.gt_transactions_array[i].gti_state;
		GTMTransactions.gt_transactions_array[handle].gti_xmin = txn.gt_transactions_array[i].gti_xmin;
		GTMTransactions.gt_transactions_array[handle].gti_isolevel = txn.gt_transactions_array[i].gti_isolevel;
		GTMTransactions.gt_transactions_array[handle].gti_readonly = txn.gt_transactions_array[i].gti_readonly;
		GTMTransactions.gt_transactions_array[handle].gti_proxy_client_id = txn.gt_transactions_array[i].gti_proxy_client_id;

		if (txn.gt_transactions_array[i].nodestring == NULL )
			GTMTransactions.gt_transactions_array[handle].nodestring = NULL;
		else
			GTMTransactions.gt_transactions_array[handle].nodestring = txn.gt_transactions_array[i].nodestring;

		/* GID */
		if (txn.gt_transactions_array[i].gti_gid == NULL )
			GTMTransactions.gt_transactions_array[handle].gti_gid = NULL;
		else
			GTMTransactions.gt_transactions_array[handle].gti_gid = txn.gt_transactions_array[i].gti_gid;

		/* copy GTM_SnapshotData */
		GTMTransactions.gt_transactions_array[handle].gti_current_snapshot.sn_xmin =
						txn.gt_transactions_array[i].gti_current_snapshot.sn_xmin;
		GTMTransactions.gt_transactions_array[handle].gti_current_snapshot.sn_xmax =
						txn.gt_transactions_array[i].gti_current_snapshot.sn_xmax;
		GTMTransactions.gt_transactions_array[handle].gti_current_snapshot.sn_xcnt =
						txn.gt_transactions_array[i].gti_current_snapshot.sn_xcnt;
		GTMTransactions.gt_transactions_array[handle].gti_current_snapshot.sn_xip =
						txn.gt_transactions_array[i].gti_current_snapshot.sn_xip;
		/* end of copying GTM_SnapshotData */

		GTMTransactions.gt_transactions_array[handle].gti_snapshot_set =
						txn.gt_transactions_array[i].gti_snapshot_set;
		GTMTransactions.gt_transactions_array[handle].gti_vacuum =
						txn.gt_transactions_array[i].gti_vacuum;

		/*
		 * Is this correct? Is GTM_TXN_COMMITTED transaction categorized as "open"?
		 */
		if (GTMTransactions.gt_transactions_array[handle].gti_state != GTM_TXN_ABORTED)
		{
			GTMTransactions.gt_open_transactions =
					gtm_lappend(GTMTransactions.gt_open_transactions,
							&GTMTransactions.gt_transactions_array[handle]);
		}
	}

	dump_transactions_elog(&GTMTransactions, num_txn);

	GTM_RWLockRelease(&GTMTransactions.gt_TransArrayLock);
	GTM_RWLockRelease(&GTMTransactions.gt_XidGenLock);
	
	elog(DEBUG1, "Restoring %d gxid(s) done.", num_txn);
#endif
	return 1;
}

int
gtm_standby_restore_node(void)
{
	GTM_PGXCNodeInfo *data;
	int rc, i;
	int num_node;

	elog(LOG, "Copying node information from the GTM active...");

	data = (GTM_PGXCNodeInfo *) malloc(sizeof(GTM_PGXCNodeInfo) * 128);
	memset(data, 0, sizeof(GTM_PGXCNodeInfo) * 128);
	
	rc = get_node_list(GTM_ActiveConn, data, 128);
	if (rc < 0)
	{
		elog(DEBUG3, "get_node_list() failed.");
		rc = 0;
		goto finished;
	}

	num_node = rc;

	for (i = 0; i < num_node; i++)
	{
		elog(DEBUG1, "get_node_list: nodetype=%d, nodename=%s, datafolder=%s",
			 data[i].type, data[i].nodename, data[i].datafolder);
		if (Recovery_PGXCNodeRegister(data[i].type, data[i].nodename, data[i].port,
					 data[i].proxyname, data[i].status,
					 data[i].ipaddress, data[i].datafolder, true,
					 -1 /* dummy socket */, false) != 0)
		{
			rc = 0;
			goto finished;
		}
	}

	elog(LOG, "Copying node information from GTM active done.");

finished:	
	free(data);
	return rc;
}

/*
 * Register myself to the GTM (active) as a "disconnected" node.
 *
 * This status would be updated later after restoring completion.
 * See gtm_standby_update_self().
 *
 * Returns 1 on success, 0 on failure.
 */
int
gtm_standby_register_self(const char *node_name, int port, const char *datadir)
{
	int rc;
#ifdef __XLOG__
    static char *s_node_name = NULL;
	static int   s_port      = 0;
	static char *s_datadir   = NULL;

	static bool init         = false;

	if(init == false)
	{
		s_node_name = strdup(node_name);
		s_datadir     = strdup(datadir);
		s_port        = port;

		init = true;
	}
	else
	{
		node_name = s_node_name;
		port      = s_port;
		datadir   = s_datadir;
	}
#endif

	elog(DEBUG8, "Registering standby-GTM status...");


	node_get_local_addr(GTM_ActiveConn, standbyHostName, sizeof(standbyNodeName), &rc);

	memset(standbyNodeName, 0, NI_MAXHOST);
	strncpy(standbyNodeName, node_name, NI_MAXHOST - 1);
	standbyPortNumber = port;
	standbyDataDir= (char *)datadir;
	elog(LOG, "register standbyhostname %s, port number %d node name %s datadir %s", 
					standbyHostName, standbyPortNumber, standbyNodeName, standbyDataDir);
	rc = node_register_internal(GTM_ActiveConn, GTM_NODE_GTM, standbyHostName, standbyPortNumber,
								standbyNodeName, standbyDataDir,
								NODE_DISCONNECTED);
	if (rc < 0)
	{
		elog(DEBUG1, "Failed to register a standby-GTM status.");

		return 0;
	}

	elog(DEBUG1, "Registering standby-GTM done.");
	
	return 1;
}

/*
 * Update my node status from "disconnected" to "connected" in GTM by myself.
 *
 * Returns 1 on success, 0 on failure.
 */
int
gtm_standby_activate_self(void)
{
	int rc;

	elog(DEBUG1, "Updating the standby-GTM status to \"CONNECTED\"...");

	rc = node_unregister(GTM_ActiveConn, GTM_NODE_GTM, standbyNodeName);
	if (rc < 0)
	{
		elog(DEBUG1, "Failed to unregister old standby-GTM status.");
		return 0;
	}
	elog(LOG, "register standbyhostname %s, port number %d node name %s datadir %s", 
					standbyHostName, standbyPortNumber, standbyNodeName, standbyDataDir);
	rc = node_register_internal(GTM_ActiveConn, GTM_NODE_GTM, standbyHostName, standbyPortNumber,
								standbyNodeName, standbyDataDir,
								NODE_CONNECTED);

	if (rc < 0)
	{
		elog(DEBUG1, "Failed to register a new standby-GTM status.");
		return 0;
	}	

	elog(DEBUG1, "Updating the standby-GTM status done.");

	return 1;
}


/*
 * Find "one" GTM standby node info.
 *
 * Returns a pointer to GTM_PGXCNodeInfo on success,
 * or returns NULL on failure.
 */
GTM_PGXCNodeInfo *
find_standby_node_info(void)
{
	GTM_PGXCNodeInfo *node[1024];
	size_t n;
	int i;

	n = pgxcnode_find_by_type(GTM_NODE_GTM, node, 1024);

	for (i = 0 ; i < n ; i++)
	{
		elog(DEBUG8, "pgxcnode_find_by_type: nodename=%s, type=%d, ipaddress=%s, port=%d, status=%d",
			 node[i]->nodename,
			 node[i]->type,
			 node[i]->ipaddress,
			 node[i]->port,
			 node[i]->status);

		/* 
		 * Must not try and connect to ourself. That will lead to a deadlock
		 *
		 * !!TODO Ideally we should not be registered on the GTM, but when a
		 * failover happens, the standby may carry forward the node
		 * registration information previously sent by the original master as a
		 * backup. This needs to be studied further
		 */
		if (strcmp(node[i]->nodename, NodeName) &&
			node[i]->status == NODE_CONNECTED)
			return node[i];
	}

	return NULL;
}


#ifndef __XLOG__

/*
 * Make a connection to the GTM standby node when getting connected
 * from the client.
 *
 * Returns a pointer to a GTM_Conn object on success, or NULL on failure.
 */
GTM_Conn *
gtm_standby_connect_to_standby(void)
{
	GTM_Conn *conn;
	int report;

	conn = gtm_standby_connect_to_standby_int(&report);

	return conn;
}
#endif

GTM_Conn *
gtm_connect_to_standby(GTM_PGXCNodeInfo *n,int timeout)
{
	GTM_Conn *standby = NULL;
	char conn_string[1024];

	elog(DEBUG8, "GTM standby is active. Going to connect.");

	snprintf(conn_string, sizeof(conn_string),
		 "host=%s port=%d node_name=%s remote_type=%d connect_timeout=%d",
			 n->ipaddress, n->port, NodeName, GTM_NODE_GTM, timeout);

	standby = PQconnectGTM(conn_string);
	if (standby == NULL || GTMPQstatus(standby) != CONNECTION_OK)
	{
		int save_errno = errno;
		if(standby)
		{
			elog(LOG, "can not connect to GTM standby: %s %m", GTMPQerrorMessage(standby));
		}
		else
		{
			elog(LOG, "connection is null: %m");
		}

		errno = save_errno;
		if(standby)
		{
			GTMPQfinish(standby);
		}
		return NULL;
	}

	return standby;
}

#ifndef __XLOG__
static GTM_Conn *
gtm_standby_connect_to_standby_int(int *report_needed)
{
	GTM_Conn *standby = NULL;
	GTM_PGXCNodeInfo *n;
	char conn_string[1024];
	
	*report_needed = 0;

	n = find_standby_node_info();
	if (!n)
	{
		elog(LOG, "Any GTM standby node not found in registered node(s).");
		return NULL;
	}

	elog(DEBUG8, "GTM standby is active. Going to connect.");
	*report_needed = 1;

	
	snprintf(conn_string, sizeof(conn_string),
		 "host=%s port=%d node_name=%s remote_type=%d connect_timeout=%d",
			 n->ipaddress, n->port, NodeName, GTM_NODE_GTM, GTM_Standby_Connetion_Timeout);

	standby = PQconnectGTM(conn_string);
	if (standby == NULL || GTMPQstatus(standby) != CONNECTION_OK)
	{
		int save_errno = errno;
		if(standby)
		{
			elog(LOG, "can not connect to GTM standby: %s %m", GTMPQerrorMessage(standby));
		}
		else
		{
			elog(LOG, "connection is null: %m");
		}

		errno = save_errno;
		if(standby)
		{
			GTMPQfinish(standby);
		}
		return NULL;
	}

	elog(DEBUG8, "Connection established with GTM standby. - %p conn %s socket %d", n, conn_string, standby->sock);

	return standby;
}
#endif

#ifndef __XLOG__
void
gtm_standby_disconnect_from_standby(GTM_Conn *conn)
{
	if (Recovery_IsStandby())
		return;

	GTMPQfinish(conn);
}



GTM_Conn *
gtm_standby_reconnect_to_standby(GTM_Conn *old_conn, int retry_max)
{
	GTM_Conn *newconn = NULL;
	int report;
	int i;

	if (Recovery_IsStandby())
		return NULL;

	if (old_conn != NULL)
		gtm_standby_disconnect_from_standby(old_conn);

	for (i = 0; i < retry_max; i++)
	{
		elog(DEBUG1, "gtm_standby_reconnect_to_standby(): going to re-connect. retry=%d", i);

		newconn = gtm_standby_connect_to_standby_int(&report);
		if (newconn != NULL)
			break;

		elog(DEBUG1, "gtm_standby_reconnect_to_standby(): re-connect failed. retry=%d", i);
	}

	return newconn;
}
#endif


#define GTM_STANDBY_RETRY_MAX 3

#ifndef  __XLOG__
bool
gtm_standby_check_communication_error(Port *myport, int *retry_count, GTM_Conn *oldconn)
{
	
	/*
	 * This function may be called without result from standby.
	 */
	if (GetMyConnection(myport)->standby->result
		&& GetMyConnection(myport)->standby->result->gr_status == GTM_RESULT_COMM_ERROR)
	{
		if (*retry_count == 0)
		{
			(*retry_count)++;

			GetMyConnection(myport)->standby =
					gtm_standby_reconnect_to_standby(GetMyConnection(myport)->standby,
													 GTM_STANDBY_RETRY_MAX);

			if (GetMyConnection(myport)->standby)
				return true;
		}

		elog(DEBUG1, "communication error with standby.");
	}
	return false;
}
#endif

int
gtm_standby_begin_backup(int64 identifier, int64 lsn, GlobalTimestamp gts)
{
	int rc = set_begin_backup(GTM_ActiveConn, identifier, lsn, gts);
	return (rc ? 0 : 1);
}

int
gtm_standby_end_backup(void)
{

	int rc = set_end_backup(GTM_ActiveConn, false);
	
	return (rc ? 0 : 1);
}

int
gtm_standby_start_replication(const char *application_name)
{
	char ip_port[NI_MAXHOST];
    int  rc = 0;
	int  i  = 0;
	int len = 0;

	if(strlen(application_name) == 0)
	{
		node_get_local_addr(GTM_ActiveConn, ip_port, NI_MAXHOST, &rc);

		len = strlen(ip_port);

		snprintf(ip_port + len,NI_MAXHOST - len,":%d",GTMPortNumber);

		for(i = 0; i < len ; i++)
		{
			if(ip_port[i] == '_')
				ip_port[i] = '.';
		}

		return set_begin_replication(GTM_ActiveConn,ip_port,NodeName);
	}

    return set_begin_replication(GTM_ActiveConn,application_name,NodeName);
}

extern char *NodeName;		/* Defined in main.c */

void
gtm_standby_finishActiveConn(void)
{
	
	GTM_ActiveConn = gtm_standby_connectToActiveGTM(0);
	if (GTM_ActiveConn == NULL)
	{
		elog(DEBUG3, "Error in connection");
		return;
	}
	elog(DEBUG1, "Connection established to the GTM active.");

	/* Unregister self from Active-GTM */
	node_unregister(GTM_ActiveConn, GTM_NODE_GTM, NodeName);
	/* Disconnect form Active */
	GTMPQfinish(GTM_ActiveConn);
	
#ifdef __TBASE__
	GTM_ActiveConn = NULL;
#endif

}

static GTM_Conn *
gtm_standby_connectToActiveGTM(int timeout)
{
	char connect_string[1024];
	int active_port = Recovery_StandbyGetActivePort();
	char *active_address = Recovery_StandbyGetActiveAddress();

	/* Need to connect to Active-GTM again here */
	elog(LOG, "Connecting the GTM active on %s:%d...", active_address, active_port);

	if (timeout != 0)
    {
        sprintf(connect_string, "host=%s port=%d node_name=%s remote_type=%d connect_timeout=%d",
                active_address, active_port, NodeName, GTM_NODE_GTM, timeout);
    }
    else
    {
        sprintf(connect_string, "host=%s port=%d node_name=%s remote_type=%d",
                active_address, active_port, NodeName, GTM_NODE_GTM);
    }

	return PQconnectGTM(connect_string);
}
#ifdef __TBASE__
/*
 * Init standby storage.
 */
int32 GTM_StoreStandbyInitFromMaster(char *data_dir)
{
	int32  ret   = 0;
	size_t size  = 0;
	char   *data = NULL;

	if (NULL == data_dir)
	{
		elog(LOG, "GTM_StoreStandbyInitFromMaster invalid null parameter");
		return GTM_STORE_ERROR;
	}
	
	if (enable_gtm_sequence_debug)
	{
		elog(LOG, "GTM_StoreStandbyInitFromMaster begin");
	}
	
	size = (uint32)get_storage_file(GTM_ActiveConn, &data,&XLogCtl->apply,&XLogCtl->thisTimeLineID);
	if (-1 == size)
	{
		elog(LOG, "GTM_StoreStandbyInitFromMaster get_storage_file failed");
		return GTM_STORE_ERROR;
	}

	ret = GTM_StoreStandbyInit(data_dir, data, (uint32)size);
	if (ret)
	{
		elog(LOG, "GTM_StoreStandbyInitFromMaster GTM_StoreStandbyInit failed");
		return GTM_STORE_ERROR;
	}

    /* we transfer data from the beginning of xlog */
	XLogCtl->LogwrtResult.Write = XLogCtl->LogwrtResult.Flush = XLogCtl->apply - (XLogCtl->apply % GTM_XLOG_SEG_SIZE);
	NewXLogFile(GetSegmentNo(XLogCtl->LogwrtResult.Flush));

    ControlData->checkPoint     = XLogCtl->apply;
    ControlData->prevCheckPoint = InvalidXLogRecPtr;
	ControlData->thisTimeLineID = XLogCtl->thisTimeLineID;
	ControlData->gts            = g_GTM_Store_Header->m_next_gts;
	ControlData->time           = time(NULL);

	ControlDataSync(false);
    AddBackupLabel(GetSegmentNo(XLogCtl->LogwrtResult.Flush));

	elog(LOG,"Get start replication at %X/%X,timeLine: %d",
		 (uint32_t)(XLogCtl->LogwrtResult.Flush>>32),
		 (uint32_t)(XLogCtl->LogwrtResult.Flush),
		 XLogCtl->thisTimeLineID);


	if (enable_gtm_sequence_debug)
	{
		elog(LOG, "GTM_StoreStandbyInitFromMaster done");
	}
	return GTM_STORE_OK;
}

static void
AddBackupLabel(uint64 segment_no)
{
    char xlog_filename[MAXFNAMELEN];
    TimeLineID timeline  = FIRST_TIMELINE_ID;
    FILE *fp             = NULL;

    timeline = GetCurrentTimeLineID();

    fp = fopen("backup_label","w");

    if(fp == NULL)
    {
        elog(LOG,"could not create backup_label file \"backup_label\": %s",strerror(errno));
        return;
    }

    GTMXLogFileNameWithoutGtmDir(xlog_filename,timeline,segment_no);

    fprintf(fp,"%s\n",xlog_filename);

    if(fclose(fp) != 0)
    {
        elog(LOG,"could not write backup_label file \"backup_label\": %s",strerror(errno));
        exit(1);
    }
}
#endif
