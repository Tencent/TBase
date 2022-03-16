/*-------------------------------------------------------------------------
 *
 * poolmgr.h
 *
 *	  Definitions for the Datanode connection pool.
 *
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/poolmgr.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef POOLMGR_H
#define POOLMGR_H
#include <sys/time.h>
#include "nodes/nodes.h"
#include "pgxcnode.h"
#include "poolcomm.h"
#include "storage/pmsignal.h"
#include "utils/hsearch.h"
#include "squeue.h"
#include "utils/guc.h"


#define MAX_IDLE_TIME 60

/*
 * List of flags related to pooler connection clean up when disconnecting
 * a session or relaeasing handles.
 * When Local SET commands (POOL_CMD_LOCAL_SET) are used, local parameter
 * string is cleaned by the node commit itself.
 * When global SET commands (POOL_CMD_GLOBAL_SET) are used, "RESET ALL"
 * command is sent down to activated nodes to at session end. At the end
 * of a transaction, connections using global SET commands are not sent
 * back to pool.
 * When temporary object commands are used (POOL_CMD_TEMP), "DISCARD ALL"
 * query is sent down to nodes whose connection is activated at the end of
 * a session.
 * At the end of a transaction, a session using either temporary objects
 * or global session parameters has its connections not sent back to pool.
 *
 * Local parameters are used to change within current transaction block.
 * They are sent to remote nodes invloved in the transaction after sending
 * BEGIN TRANSACTION using a special firing protocol.
 * They cannot be sent when connections are obtained, making them having no
 * effect as BEGIN is sent by backend after connections are obtained and
 * obtention confirmation has been sent back to backend.
 * SET CONSTRAINT, SET LOCAL commands are in this category.
 *
 * Global parmeters are used to change the behavior of current session.
 * They are sent to the nodes when the connections are obtained.
 * SET GLOBAL, general SET commands are in this category.
 */
typedef enum
{
	POOL_CMD_TEMP,		/* Temporary object flag */
	POOL_CMD_LOCAL_SET,	/* Local SET flag, current transaction block only */
	POOL_CMD_GLOBAL_SET	/* Global SET flag */
} PoolCommandType;

#ifdef __TBASE__
typedef enum
{
	SIGNAL_SIGINT   = 0,
	SIGNAL_SIGUSR2  = 1
}   SignalType;

/* 
 * pooler error code 
 * corresponding with err_msg in poolmgr.c
 */
typedef enum
{
	POOL_ERR_NONE,
	POOL_ERR_GET_CONNECTIONS_POOLER_LOCKED,
	POOL_ERR_GET_CONNECTIONS_TASK_NOT_DONE,
	POOL_ERR_GET_CONNECTIONS_DISPATCH_FAILED,
	POOL_ERR_GET_CONNECTIONS_INVALID_ARGUMENT,
	POOL_ERR_GET_CONNECTIONS_OOM,
	POOL_ERR_GET_CONNECTIONS_CONNECTION_BAD,
	POOL_ERR_CANCEL_TASK_NOT_DONE,
	POOL_ERR_CANCEL_DISPATCH_FAILED,
	POOL_ERR_CANCEL_SEND_FAILED,
	NUMBER_POOL_ERRS
}   PoolErrorCode;

#define PoolErrIsValid(err)    ((bool) (err > POOL_ERR_NONE && err < NUMBER_POOL_ERRS))
#endif

/* Connection pool entry */
typedef struct
{
	/* stamp elements */
	time_t released;/* timestamp when the connection last time release */
	time_t checked; /* timestamp when the connection last time check */
	time_t created; /* timestamp when the connection created */
	bool   bwarmed;
	
	int32  usecount;
	NODE_CONNECTION *conn;
	NODE_CANCEL	*xc_cancelConn;

	/* trace info */	
	int32  refcount;   /* reference count */
    int64 m_version;  /* version of node slot */
	int32  pid;		   /* agent pid that contains the slot */
	int32  seqnum;	   /* slot seqnum for the slot, unique for one slot */
	bool   bdestoryed; /* used to show whether we are destoryed */
	char   *file;      /* file where destroy the slot */
	int32  lineno;	   /* lineno where destroy the slot */
	char   *node_name; /* connection node name , pointer to datanode_pool node_name, no memory allocated*/
	int32  backend_pid;/* backend pid of remote connection */
} PGXCNodePoolSlot;

/* Pool of connections to specified pgxc node */
typedef struct
{
	Oid			nodeoid;	/* Node Oid related to this pool */
	bool        coord;      /* whether am I coordinator */
	bool        asyncInProgress;/* whether am in asyn building */
	char	   *connstr;
	int         nwarming;   /* connection number warming in progress */
	int         nquery;     /* connection number query memory size in progress */
	int			freeSize;	/* available connections */
	int			size;  		/* total pool size */

	char		node_name[NAMEDATALEN]; /* name of the node.*/
    int64		m_version;	/* version of node pool */
	PGXCNodePoolSlot **slot;
} PGXCNodePool;

/* All pools for specified database */
typedef struct databasepool
{
	char	   *database;
	char	   *user_name;
	char	   *pgoptions;		/* Connection options */
	HTAB	   *nodePools; 		/* Hashtable of PGXCNodePool, one entry for each
								 * Coordinator or DataNode */
	time_t		oldest_idle;
 	bool        bneed_warm;
	bool        bneed_precreate;
	bool        bneed_pool;		/* check whether need  connect pool */
    int64		version;        /* used to generate node_pool's version */
	MemoryContext mcxt;
	struct databasepool *next; 	/* Reference to next to organize linked list */
} DatabasePool;
#define       PGXC_POOL_ERROR_MSG_LEN  512
typedef struct PGXCASyncTaskCtl
{
	slock_t              m_lock;		 /* common lock */
	int32                m_status; 		 /* PoolAyncCtlStaus */
	int32 			     m_mumber_total;
	int32  				 m_number_done;	

	/* acquire connections */
	int32             	*m_result;       /* fd array */
	int32				*m_pidresult;	 /* pid array */
	List 				*m_datanodelist;
	List 				*m_coordlist;
	int32  				 m_number_succeed;

	/* set local command */
	int32                m_res;

	/* set command */
	char                 *m_command;
	int32                 m_total;
	int32                 m_succeed;

	/* last command for 'g' and 's' */
	CommandId             m_max_command_id;

	/* errmsg and error status. */
	bool                  m_missing_ok;
	int32				  m_error_offset;
	char                  m_error_msg[PGXC_POOL_ERROR_MSG_LEN];
}PGXCASyncTaskCtl;


/*
 * Agent of client session (Pool Manager side)
 * Acts as a session manager, grouping connections together
 * and managing session parameters
 */
typedef struct
{
	/* Process ID of postmaster child process associated to pool agent */
	int				pid;
	/* communication channel */
	PoolPort		port;
	DatabasePool   *pool;
	MemoryContext	mcxt;
	int				num_dn_connections;
	int				num_coord_connections;
	Oid		   	   *dn_conn_oids;		/* one for each Datanode */
	Oid		   	   *coord_conn_oids;	/* one for each Coordinator */
	PGXCNodePoolSlot **dn_connections; /* one for each Datanode */
	PGXCNodePoolSlot **coord_connections; /* one for each Coordinator */
	
	char		   *session_params;
	char		   *local_params;
	List            *session_params_list; /* session param list */
	List 			*local_params_list;   /* local param list */
	
	bool			is_temp; /* Temporary objects used for this pool session? */

	int             query_count;   /* query count, if exceed, need to reconnect database */
	bool            breconnecting; /* whether we are reconnecting */
	int             agentindex;

	
	bool            destory_pending; /* whether we have been ordered to destory */
	int32			ref_count;		 /* reference count */
	PGXCASyncTaskCtl *task_control;  /* in error situation, we need to free the task control */

    pg_time_t cmd_start_time;        /* command start time */
} PoolAgent;

/* Handle to the pool manager (Session's side) */
typedef struct
{
	/* communication channel */
	PoolPort	port;
} PoolHandle;

typedef struct PoolerCmdStatistics
{
    uint64 total_request_times;     /* command total request times */
    union
    {
        uint64 total_costtime;      /* total time spent processing commands */
        uint64 avg_costtime;        /* avg time spent processing command */
    };
    uint64 max_costtime;            /* max time spent processing command */
    uint64 min_costtime;            /* min time spent processing command */
} PoolerCmdStatistics;


#define POOLER_CMD_COUNT (18)



#define     POOLER_ERROR_MSG_LEN  256

extern int	MinPoolSize;
extern int	MaxPoolSize;
extern int	InitPoolSize;
extern int	MinFreeSize;

extern int	PoolerPort;
extern int	PoolConnKeepAlive;
extern int	PoolMaintenanceTimeout;
extern bool PersistentConnections;

extern char *g_PoolerWarmBufferInfo;
extern char *g_unpooled_database;
extern char *g_unpooled_user;

extern int	PoolSizeCheckGap; 
extern int	PoolConnMaxLifetime; 
extern int	PoolMaxMemoryLimit;
extern int	PoolConnectTimeOut;
extern int  PoolScaleFactor;
extern int  PoolDNSetTimeout;
extern int  PoolCheckSlotTimeout;
extern int  PoolPrintStatTimeout;
extern bool PoolConnectDebugPrint;
extern bool PoolSubThreadLogPrint;
/* Status inquiry functions */
extern void PGXCPoolerProcessIam(void);
extern bool IsPGXCPoolerProcess(void);

/* Initialize internal structures */
extern int	PoolManagerInit(void);

/* Destroy internal structures */
extern int	PoolManagerDestroy(void);

/*
 * Get handle to pool manager. This function should be called just before
 * forking off new session. It creates PoolHandle, PoolAgent and a pipe between
 * them. PoolAgent is stored within Postmaster's memory context and Session
 * closes it later. PoolHandle is returned and should be store in a local
 * variable. After forking off it can be stored in global memory, so it will
 * only be accessible by the process running the session.
 */
extern PoolHandle *GetPoolManagerHandle(void);

/*
 * Called from Postmaster(Coordinator) after fork. Close one end of the pipe and
 * free memory occupied by PoolHandler
 */
extern void PoolManagerCloseHandle(PoolHandle *handle);

/*
 * Gracefully close connection to the PoolManager
 */
extern void PoolManagerDisconnect(void);

extern char *session_options(void);

/*
 * Called from Session process after fork(). Associate handle with session
 * for subsequent calls. Associate session with specified database and
 * initialize respective connection pool
 */
extern void PoolManagerConnect(PoolHandle *handle,
	                           const char *database, const char *user_name,
	                           char *pgoptions);

/*
 * Reconnect to pool manager
 * This simply does a disconnection followed by a reconnection.
 */
extern void PoolManagerReconnect(void);

/*
 * Save a SET command in Pooler.
 * This command is run on existent agent connections
 * and stored in pooler agent to be replayed when new connections
 * are requested.
 */
#define POOL_SET_COMMAND_ALL  -1
#define POOL_SET_COMMAND_NONE 0

extern int PoolManagerSetCommand(PGXCNodeHandle **connections, int32 count, PoolCommandType command_type, 
				      			const char *set_command);

/* Get pooled connections */
extern int *PoolManagerGetConnections(List *datanodelist, List *coordlist, bool raise_error, int **pids);

/* Clean pool connections */
extern void PoolManagerCleanConnection(List *datanodelist, List *coordlist, char *dbname, char *username);

/* Check consistency of connection information cached in pooler with catalogs */
extern bool PoolManagerCheckConnectionInfo(void);

/* Reload connection data in pooler and drop all the existing connections of pooler */
extern void PoolManagerReloadConnectionInfo(void);

/* Send Abort signal to transactions being run */
extern int	PoolManagerAbortTransactions(char *dbname, char *username, int **proc_pids);

/* Return connections back to the pool, for both Coordinator and Datanode connections */
extern void PoolManagerReleaseConnections(bool force);

/* Cancel a running query on Datanodes as well as on other Coordinators */
extern bool PoolManagerCancelQuery(int dn_count, int* dn_list, int co_count, int* co_list, int signal);

/* Lock/unlock pool manager */
extern void PoolManagerLock(bool is_lock);

/* Check if pool has a handle */
extern bool IsPoolHandle(void);

/* Send commands to alter the behavior of current transaction */
extern int   PoolManagerSendLocalCommand(int dn_count, int* dn_list, int co_count, int* co_list);

/* Do pool health check activity */
extern void PoolAsyncPingNodes(void);
extern void PoolPingNodes(void);
extern void PoolPingNodeRecheck(Oid nodeoid);
extern bool check_persistent_connections(bool *newval, void **extra,
		GucSource source);

/* Refresh connection data in pooler and drop connections of altered nodes in pooler */
extern int PoolManagerRefreshConnectionInfo(void);
extern int PoolManagerClosePooledConnections(const char *dbname, const char *username);

extern int PoolManagerGetCmdStatistics(char *s, int size);
extern void PoolManagerResetCmdStatistics(void);
extern int PoolManagerGetConnStatistics(StringInfo s);

#endif
