/*-------------------------------------------------------------------------
 *
 * poolmgr.c
 *
 *      Connection pool manager handles connections to Datanodes
 *
 * The pooler runs as a separate process and is forked off from a
 * Coordinator postmaster. If the Coordinator needs a connection from a
 * Datanode, it asks for one from the pooler, which maintains separate
 * pools for each Datanode. A group of connections can be requested in
 * a single request, and the pooler returns a list of file descriptors
 * to use for the connections.
 *
 * Note the current implementation does not yet shrink the pool over time
 * as connections are idle.  Also, it does not queue requests; if a
 * connection is unavailable, it will simply fail. This should be implemented
 * one day, although there is a chance for deadlocks. For now, limiting
 * connections should be done between the application and Coordinator.
 * Still, this is useful to avoid having to re-establish connections to the
 * Datanodes all the time for multiple Coordinator backend sessions.
 *
 * The term "agent" here refers to a session manager, one for each backend
 * Coordinator connection to the pooler. It will contain a list of connections
 * allocated to a session, at most one per Datanode.
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * IDENTIFICATION
 *      $$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include <poll.h>
#include <signal.h>
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "access/xact.h"
#include "catalog/pgxc_node.h"
#include "commands/dbcommands.h"
#include "nodes/nodes.h"
#include "pgxc/poolmgr.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/resowner.h"
#include "lib/stringinfo.h"
#include "libpq/libpq-be.h"
#include "libpq/pqformat.h"
#include "common/username.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "pgxc/nodemgr.h"
#include "pgxc/poolutils.h"
#include "../interfaces/libpq/libpq-fe.h"
#include "../interfaces/libpq/libpq-int.h"
#include "postmaster/postmaster.h"        /* For Unix_socket_directories */
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include "utils/varlena.h"
#include "port.h"
#include <math.h>
#include <sys/timeb.h>
#ifdef __TBASE__
#include "access/xlog.h"
#endif

/* the mini use conut of a connection */
#define  MINI_USE_COUNT    10

/* Configuration options */
int            InitPoolSize = 10;
int            MinPoolSize  = 50;
int            MaxPoolSize  = 100;
int            MinFreeSize  = 10;

int            PoolerPort             = 6667;
int            PoolConnKeepAlive      = 600;
int            PoolMaintenanceTimeout = 30;
int            PoolSizeCheckGap       = 120;  /* max check memory size gap, in seconds */
int            PoolConnMaxLifetime    = 600;  /* max lifetime of a pooled connection, in seconds */
int            PoolMaxMemoryLimit     = 10;
int            PoolConnectTimeOut     = 10;
int            PoolScaleFactor        = 2;
int         PoolDNSetTimeout       = 10;
int         PoolCheckSlotTimeout   = -1;   /* Pooler check slot. One slot can only in nodepool or agent at one time. */
int         PoolPrintStatTimeout   = -1;
    
bool        PersistentConnections    = false;
char        *g_PoolerWarmBufferInfo  = "postgres:postgres";

char        *g_unpooled_database     = "template1";
char        *g_unpooled_user         = "mls_admin";


bool         PoolConnectDebugPrint  = false; /* Pooler connect debug print */
bool         PoolerStuckExit         = true;  /* Pooler exit when stucked */
bool         PoolSubThreadLogPrint  = true;  /* Pooler sub thread log print */

#define      POOL_ASYN_WARM_PIPE_LEN      32   /* length of asyn warm pipe */
#define      POOL_ASYN_WARN_NUM           1      /* how many connections to warm once maintaince per node pool */
#define      POOL_SYN_CONNECTION_NUM      512   /* original was 128. To avoid can't get pooled connection */     

PGPipe      *g_AsynUtilityPipeSender = NULL; /* filled with connections */
PGPipe      *g_AsynUtilityPipeRcver  = NULL; /* used for return warmed connections */
ThreadSema   g_AsnyUtilitysem;                  /* used for async warm thread */

#define      IS_ASYNC_PIPE_FULL()   (PipeIsFull(g_AsynUtilityPipeSender))
#define      MAX_FREE_CONNECTION_NUM 100


#define      POOL_SYN_REQ_CONNECTION_NUM   32

/*
 * Flags set by interrupt handlers for later service in the main loop.
 */
static volatile sig_atomic_t shutdown_requested = false;
static volatile sig_atomic_t got_SIGHUP = false;

/* used to track connection slot info */
static int32    g_Slot_Seqnum                = 0;  
static int32    g_Connection_Acquire_Count = 0; 


typedef struct PoolerStatistics
{
    int32 client_request_conn_total;            /* total acquire connection num by client */
    int32 client_request_from_hashtab;            /* client get all conn from hashtab */
    int32 client_request_from_thread;            /* client get at least part of conn from thread */
    int32 acquire_conn_from_hashtab;    /* immediate get conn from hashtab */
    int32 acquire_conn_from_hashtab_and_set; /* get conn from hashtab, but need to set by sync thread */
    int32 acquire_conn_from_thread;        /* can't get conn from hashtab, need to conn by sync thread */
    unsigned long acquire_conn_time;    /* time cost for all conn process by sync thread */
}PoolerStatistics;

PoolerStatistics g_pooler_stat;

/* global command statistics handle */
PoolerCmdStatistics* g_pooler_cmd_stat = NULL;

unsigned char g_pooler_cmd[POOLER_CMD_COUNT] =
{
    'a',                    /* ABORT */
    'b',                    /* Fire transaction-block commands on given nodes */
    'c',                    /* CONNECT */
    'd',                    /* DISCONNECT */
    'f',                    /* CLEAN CONNECTION */
    'g',                    /* GET CONNECTIONS */
    'h',                    /* Cancel SQL Command in progress on specified connections */
    'o',                    /* Lock/unlock pooler */
    'p',                    /* Reload connection info */
    'P',                    /* Ping connection info */
    'q',                    /* Check connection info consistency */
    'r',                    /* RELEASE CONNECTIONS */
    'R',                    /* Refresh connection info */
    's',                    /* Session-related COMMAND */
    't',                    /* Close pooler connections*/
    'x',                    /* Get command statistics */
    'y',                    /* Reset command statistics */
    'z'                     /* Get connection statistics */
};

/* a map used to change msgtype to id */
uint8 g_qtype2id[256];

/* Flag to tell if we are Postgres-XC pooler process */
static bool am_pgxc_pooler = false;

typedef enum
{
    COMMAND_CONNECTION_WARM                   = 0, /* warm connection */
    COMMAND_JUDGE_CONNECTION_MEMSIZE          = 1,    
    COMMAND_CONNECTION_NEED_CLOSE               = 2, /* we should close the connection */    
    COMMAND_CONNECTION_BUILD                  = 3, /* async connection build */
    COMMAND_CONNECTION_CLOSE                  = 4, /* async connection close */
    COMMAND_PING_NODE                         = 5, /* ping node */
    COMMAND_BUYYT
}PoolAsyncCmd;

/* used for async warm a connection */
typedef struct
{
    int32              cmd;        /* PoolAsyncCmd */
    int32              nodeindex;
    Oid                node;
    DatabasePool      *dbPool;
    PGXCNodePoolSlot  *slot;    
    int32              size;       /* session memory context size */
    SendSetQueryStatus set_query_status;    /* send set query status */ 
    NameData           nodename;   /* used by async ping node */
    NameData           nodehost;   /* used by async ping node */
    int                   nodeport;   /* used by async ping node */
    int                nodestatus; /* used by async ping node, 0 means ok */
}PGXCAsyncWarmInfo;

/* Concurrently connection build info */
typedef struct
{
    int32             cmd;        /* PoolAsyncCmd */
    bool              bCoord;
    DatabasePool     *dbPool;
    int32             nodeindex;
    Oid                  nodeoid;      /* Node Oid related to this pool */
    char                *connstr;    /* palloc memory, need free */

	int64             m_version;  /* version of node pool */
    int32             size;        /* total pool size */
    int32               validSize;  /* valid data element number */    
    bool              failed;
    PGXCNodePoolSlot  slot[1];    /* var length array */
} PGXCPoolConnectReq;

typedef enum
{
    PoolAsyncStatus_idle = 0,
    PoolAsyncStatus_busy = 1,    
    PoolAsyncStatus_butty
}PoolAsyncStatus;
#define      MAX_SYNC_NETWORK_PIPE_LEN      1024    /* length of SYNC network pipe */
#define      MAX_SYNC_NETWORK_THREAD        PoolScaleFactor

typedef struct 
{
    PGPipe     **request;          /* request pipe */
    PGPipe     **response;         /* response pipe */
    ThreadSema *sem;               /* response sem */
    
    int32      *nodeindex;        /* nodeindex we are processing */
    int32      *status;            /* worker thread status, busy or not */
    pg_time_t  *start_stamp;       /* job start stamp */
    
    int32       *remote_backend_pid;        /* dn's backend pid */
    Oid           *remote_nodeoid;            /* dn's node oid */
    char       **remote_ip; /* dn's ip */
    int32       *remote_port;            /* dn's port */
    char       **message;                /* you can put some note to print */
    int               *cmdtype;                /* cmdtype current processing */
}PGXCPoolSyncNetWorkControl;

typedef struct
{
    int32   threadIndex;
}PGXCPoolConnThreadParam;

static PGXCPoolSyncNetWorkControl g_PoolConnControl;
static PGXCPoolSyncNetWorkControl g_PoolSyncNetworkControl;

/* Connection information cached */
typedef struct
{
    Oid         nodeoid;
    char    *host;
    int         port;
} PGXCNodeConnectionInfo;

/* Pooler set command desc */
typedef struct PoolerSetDesc
{
    int   tag_len;
    int   total_len;
    char  *command;    
}PoolerSetDesc;

/* The root memory context */
static MemoryContext PoolerMemoryContext = NULL;
/*
 * Allocations of core objects: Datanode connections, upper level structures,
 * connection strings, etc.
 */
static MemoryContext PoolerCoreContext = NULL;
/*
 * Memory to store Agents
 */
static MemoryContext PoolerAgentContext = NULL;

/* Pool to all the databases (linked list) */
static DatabasePool *databasePools = NULL;

/* PoolAgents */
#define INIT_VERSION    0
typedef struct
{
    uint32  m_nobject;   
    uint64  *m_fsm;      
    uint32  m_fsmlen;    
    uint32  m_version;
}BitmapMgr;

static int                agentCount         = 0;
static int              poolAgentSize     = 0;
static uint32           mgrVersion          = INIT_VERSION;
static int32            *agentIndexes     = NULL;
static int              usedAgentSize     = 0;

static BitmapMgr        *poolAgentMgr      = NULL;
static PoolAgent        **poolAgents        = NULL;

static PoolHandle        *poolHandle           = NULL;

typedef struct PGXCMapNode
{
    Oid            nodeoid;    /* Node Oid */
    int32       nodeidx;    /* Node index*/
    char        node_type;
    char        node_name[NAMEDATALEN];
} PGXCMapNode;

static void  create_node_map(void);
static void  refresh_node_map(void);
static int32 get_node_index_by_nodeoid(Oid node);
static int32 get_node_info_by_nodeoid(Oid node, char *type);
static char* get_node_name_by_nodeoid(Oid node);
static bool  connection_need_pool(char *filter, const char *name);


/* used to descibe sync task info */
typedef enum
{
    PoolAyncCtlStaus_init       = 0,    
    PoolAyncCtlStaus_dispatched = 1,
    PoolAyncCtlStaus_done         = 2,
    PoolAyncCtlStaus_error         = 3,
    PoolAyncCtlStaus_butty
}PoolAyncCtlStaus;


/* status used to control parallel managment */
typedef enum
{
    PoolResetStatus_reset           = 0,    
    PoolResetStatus_destory       = 1,
    PoolResetStatus_error           = 2,

    PoolLocalSetStatus_reset      = 3,    
    PoolLocalSetStatus_destory   = 4,
    PoolLocalSetStatus_error      = 5,

    PoolCancelStatus_cancel       = 6,
    PoolCancelStatus_end_query   = 7,
    PoolCancelStatus_destory      = 8,
    PoolCancelStatus_error           = 9,
    
    PoolSetCommandStatus_set      = 10,    
    PoolSetCommandStatus_destory = 11,
    PoolSetCommandStatus_error      = 12,

    PoolConnectStaus_init           = 13,    
    PoolConnectStaus_connected      = 14,
    PoolConnectStaus_set_param      = 15,
    PoolConnectStaus_done             = 16,    
    PoolConnectStaus_destory        = 17,
    PoolConnectStaus_error             = 18,
    PoolSetCommandStatus_butty
}PoolStatusEnum;

#ifdef __TBASE__
char *poolErrorMsg[] = {"No Error",
                        "Fail to get connections, pooler is locked",
                        "Fail to get connections, last request not finished yet",
                        "Fail to get connections, server overload",
                        "Fail to get connections, invalid arguments",
                        "Fail to get connections, out of memory when allocate memory for the conn structure",
                        "Fail to get connections, remote server maybe down or connection info is not correct",
                        "Fail to cancel query, last request not finished yet",
                        "Fail to cancel query, server overload",
                        "Fail to cancel query, fail to send cancel request to remote server",
                        "Number of pooler errors"
                        };
#endif

typedef struct
{
	int32             cmd;  	      /* refer to handle_agent_input command tag */
	bool              bCoord;		  /* coordinator or datanode*/
	PGXCASyncTaskCtl  *taskControl;	
	PoolAgent         *agent;
	PGXCNodePool      *nodepool;	  /* node pool for current node */
	PGXCNodePoolSlot  *slot;		  /* connection slot , no need to free */
	int32             current_status; /* currrent connect status*/
	int32             final_status;   /* final status we are going to get to*/
	int32             nodeindex;      /* node index of the remote peer */
	bool			  needfree;       /* whether need to free taskControl, last thread set the flag */

	int32			  req_seq;		  /* req sequence number */
	int32             pid;			  /* pid that acquires the connection */
	bool              needConnect;	  /* check whether we need to build a new connection , we acquire new connections */	
	bool			  error_flag;	  /* set when error */
	SendSetQueryStatus setquery_status;    /* send set query status */ 
	struct  timeval   start_time;		/* when acquire conn by sync thread, the time begin request */
	struct  timeval   end_time;			/* when acquire conn by sync thread, the time finish request */
	char              errmsg[POOLER_ERROR_MSG_LEN];
	pg_time_t         cmd_start_time;   /* command start time, including the processing time in the main process */
    pg_time_t         cmd_end_time;     /* command end time */
}PGXCPoolAsyncReq;

static void pooler_subthread_write_log(int elevel, int lineno, const char *filename, const char *funcname, const char *fmt, ...)__attribute__((format(printf, 5, 6)));

/* Use this macro when a sub thread needs to print logs */
#define pooler_thread_logger(elevel, ...) \
    do { \
        pooler_subthread_write_log(elevel, __LINE__, __FILE__, PG_FUNCNAME_MACRO, __VA_ARGS__); \
    } while(0)

#define FORMATTED_TS_LEN                (128)                                          /* format timestamp buf length */
#define POOLER_WRITE_LOG_ONCE_LIMIT     (5)                                            /* number of logs written at a time */
#define MAX_THREAD_LOG_PIPE_LEN         (2 * 1024)                                     /* length of thread log pipe */
#define DEFAULT_LOG_BUF_LEN             (1024)                                         /* length of thread log length */
PGPipe  *g_ThreadLogQueue = NULL;
#ifdef __TBASE__
bool g_allow_distri_query_on_standby_node = false;
#endif

static inline void RebuildAgentIndex(void);

static inline PGXCASyncTaskCtl* create_task_control(List *datanodelist,    List *coordlist, int32 *fd_result, int32 *pid_result);
static inline bool dispatch_connection_request(PGXCASyncTaskCtl  *taskControl,
                                                bool               bCoord,
                                                PoolAgent         *agent,
                                                PGXCNodePool      *nodepool,
                                                int32              status,
                                                int32              finStatus,
                                                int32              nodeindex,
                                                int32              reqseq,
                                                bool               dispatched);

static inline bool dispatch_reset_request(PGXCASyncTaskCtl  *taskControl,
                                            bool               bCoord,
                                            PoolAgent         *agent,
                                            int32              status,    /* PoolResetStatus */
                                            int32              nodeindex,
                                            bool               dispatched);

static inline bool dispatch_local_set_request(PGXCASyncTaskCtl  *taskControl,
                                                bool               bCoord,
                                                PoolAgent         *agent,
                                                int32              nodeindex,
                                                bool               dispatched);
static inline bool dispatch_cancle_request(PGXCASyncTaskCtl  *taskControl,
                                                bool               bCoord,
                                                PoolAgent         *agent,
                                                int32              nodeindex,
                                                bool               dispatched,
                                                int                signal);
static inline bool dispatch_set_command_request(PGXCASyncTaskCtl  *taskControl,
                                                bool               bCoord,
                                                PoolAgent         *agent,
                                                int32              nodeindex,
                                                bool               dispatched);
static inline void  finish_task_request(PGXCASyncTaskCtl  *taskControl);
static inline bool  check_is_task_done(PGXCASyncTaskCtl  *taskControl);
static inline void  set_task_status(PGXCASyncTaskCtl  *taskControl, int32 status);
static inline int32 get_task_status(PGXCASyncTaskCtl  *taskControl);
static inline void  add_task_result(PGXCASyncTaskCtl  *taskControl, int32 res);
static inline int32 get_task_result(PGXCASyncTaskCtl  *taskControl);
static inline void  set_command_total(PGXCASyncTaskCtl  *taskControl, int32 number);    
static inline void  set_command_increase_succeed(PGXCASyncTaskCtl  *taskControl);
static inline bool  get_command_success_status(PGXCASyncTaskCtl  *taskControl);
static inline void  acquire_command_increase_succeed(PGXCASyncTaskCtl  *taskControl);
static inline bool  get_acquire_success_status(PGXCASyncTaskCtl  *taskControl);
static inline void  set_task_max_command_id(PGXCASyncTaskCtl  *taskControl, CommandId id);
static inline CommandId get_task_max_commandID(PGXCASyncTaskCtl  *taskControl);
static inline void  set_task_error_msg(PGXCASyncTaskCtl  *taskControl, char *error);

static inline bool  pooler_is_async_task_done(void);
static inline bool  pooler_wait_for_async_task_done(void);
static inline void  pooler_async_task_start(PGXCPoolSyncNetWorkControl *control, int32 thread, int32 nodeindex, PGXCNodePoolSlot *slot, Oid nodeoid, int32 cmdtype);
static inline void     pooler_async_task_done(PGXCPoolSyncNetWorkControl *control, int32 thread);
static inline int32 pooler_async_task_pick_thread(PGXCPoolSyncNetWorkControl *control, int32 nodeindex);


static HTAB     *g_nodemap = NULL; /* used to map nodeOid to nodeindex */

static int    is_pool_locked = false;
static int    server_fd = -1;

static volatile int32 is_pool_release = 0;

static int    node_info_check(PoolAgent *agent);
static void agent_init(PoolAgent *agent, const char *database, const char *user_name,
                       const char *pgoptions);
static void agent_destroy(PoolAgent *agent);
static void agent_create(int new_fd);
static void agent_handle_input(PoolAgent *agent, StringInfo s);
static int  agent_session_command(PoolAgent *agent,
                                    const char *set_command,                                    
                                  Oid  *oids,
                                  int32 oid_num,
                                  PoolCommandType command_type);
static int  agent_set_command(PoolAgent *agent,
                                const char *set_command,                                    
                              Oid  *oids,
                              int32 oid_num,
                              PoolCommandType command_type);

static int  agent_temp_command(PoolAgent *agent);
static PoolerSetDesc *agent_compress_command(char *set_command, PoolerSetDesc *set_desc, bool *need_free,int *len);
static void agent_handle_set_command(PoolAgent *agent, char *set_command, PoolCommandType command_type);

static DatabasePool *create_database_pool(const char *database, const char *user_name, const char *pgoptions);
static void insert_database_pool(DatabasePool *pool);

static void reload_database_pools(PoolAgent *agent);
static DatabasePool *find_database_pool(const char *database, const char *user_name, const char *pgoptions);

static int agent_acquire_connections(PoolAgent *agent, List *datanodelist, List *coordlist,
									 bool raise_error, int32 *num, int **fd_result, int **pid_result);
static int send_local_commands(PoolAgent *agent, List *datanodelist, List *coordlist);
static int cancel_query_on_connections(PoolAgent *agent, List *datanodelist, List *coordlist, int signal);
static PGXCNodePoolSlot *acquire_connection(DatabasePool *dbPool, PGXCNodePool **pool,int32 nodeidx, Oid node, bool bCoord);
static void agent_release_connections(PoolAgent *agent, bool force_destroy, bool sync);
static void agent_return_connections(PoolAgent *agent);

static bool agent_reset_session(PoolAgent *agent);
static void release_connection(DatabasePool *dbPool, PGXCNodePoolSlot *slot,
							   int32 nodeidx, Oid node, bool force_destroy, bool bCoord, bool sync);
static void destroy_slot_ex(int32 nodeidx, Oid node, PGXCNodePoolSlot *slot, bool sync, char *file, int32 line);
#define  destroy_slot(nodeidx, node, slot, sync) destroy_slot_ex(nodeidx, node, slot, sync, __FILE__, __LINE__)

static void close_slot(int32 nodeidx, Oid node, PGXCNodePoolSlot *slot);

static PGXCNodePool *grow_pool(DatabasePool *dbPool, int32 nodeidx, Oid node, bool bCoord);
static void destroy_node_pool(PGXCNodePool *node_pool);
static void destroy_node_pool_free_slots(PGXCNodePool *node_pool);

static bool preconnect_and_warm(DatabasePool *dbPool);
static void connect_pools(void);
#ifdef _POOLER_CHECK_
static void  check_pooler_slot(void);
static void  do_check_pooler_slot(void);
static void  check_single_slot(PGXCNodePoolSlot *slot);
static void  print_pooler_slot(PGXCNodePoolSlot  *slot);
static void  check_duplicate_allocated_conn(void);
static void  check_agent_duplicate_conn(int32 agent_index, bool coord, int32 node_index, int32 pid);
static void  check_hashtab_slots(void);
static void  hashtab_check_single_slot(PGXCNodePoolSlot *slot, HTAB  *nodeHtb, PGXCNodePool *outerPool);

#endif
static void reset_pooler_statistics(void);
static void print_pooler_statistics(void);
static void record_task_message(PGXCPoolSyncNetWorkControl* control, int32 thread, char* message);
static void record_time(struct timeval start_time, struct timeval end_time);

static void PoolerLoop(void);
static int clean_connection(List *node_discard,
                            const char *database,
                            const char *user_name);
static int *abort_pids(int *count,
                       int pid,
                       const char *database,
                       const char *user_name);
static char *build_node_conn_str(Oid node, DatabasePool *dbPool);

/* Signal handlers */
static void pooler_die(SIGNAL_ARGS);
static void pooler_quickdie(SIGNAL_ARGS);
static void pools_maintenance(void);
static bool shrink_pool(DatabasePool *pool);
static bool pooler_pools_warm(void);
static void pooler_async_warm_database_pool(DatabasePool   *pool);
static void pooler_sync_connections_to_nodepool(void);
static void pooler_handle_sync_response_queue(void);
static void pooler_async_warm_connection(DatabasePool *pool, PGXCNodePoolSlot *slot, PGXCNodePool *nodePool, Oid node);
static void pooler_async_query_connection(DatabasePool *pool, PGXCNodePoolSlot *slot, int32 nodeidx, Oid node);

static void  *pooler_async_utility_thread(void *arg);
static void  *pooler_async_connection_management_thread(void *arg);
static void  *pooler_sync_remote_operator_thread(void *arg);

static bool   pooler_async_build_connection(DatabasePool *pool, int64 pool_version, int32 nodeidx, Oid node, 
                                            int32 size, char *connStr, bool bCoord);
static BitmapMgr *BmpMgrCreate(uint32 objnum);
static int        BmpMgrAlloc(BitmapMgr *mgr);
static void       BmpMgrFree(BitmapMgr *mgr, int index);
static uint32       BmpMgrGetVersion(BitmapMgr *mgr);
static int           BmpMgrGetUsed(BitmapMgr *mgr, int32 *indexes, int32 indexlen);
static void       pooler_sig_hup_handler(SIGNAL_ARGS);
static bool       BmpMgrHasIndexAndClear(BitmapMgr *mgr, int index);
static inline  void  agent_increase_ref_count(PoolAgent *agent);
static inline  void  agent_decrease_ref_count(PoolAgent *agent);
static inline  bool  agent_can_destory(PoolAgent *agent);
static inline  void  agent_pend_destory(PoolAgent *agent);
static inline  void  agent_set_destory(PoolAgent *agent);
static inline  bool  agent_pending(PoolAgent *agent);
static inline  void  agent_handle_pending_agent(PoolAgent *agent);
static inline  bool  agent_destory_task_control(PoolAgent *agent);
static inline  void  pooler_init_sync_control(PGXCPoolSyncNetWorkControl *control);
static inline  int32 pooler_get_slot_seq_num(void);
static inline  int32 pooler_get_connection_acquire_num(void);
static void  record_slot_info(PGXCPoolSyncNetWorkControl *control, int32 thread, PGXCNodePoolSlot *slot, Oid nodeoid);
static bool  is_slot_avail(PGXCPoolAsyncReq* request);
static void TryPingUnhealthyNode(Oid nodeoid);
static void handle_abort(PoolAgent * agent, StringInfo s);
static void handle_command_to_nodes(PoolAgent * agent, StringInfo s);
static void handle_connect(PoolAgent * agent, StringInfo s);
static void handle_clean_connection(PoolAgent * agent, StringInfo s);
static void handle_get_connections(PoolAgent * agent, StringInfo s);
static void handle_query_cancel(PoolAgent * agent, StringInfo s);
static void handle_session_command(PoolAgent * agent, StringInfo s);
static int  refresh_database_pools(PoolAgent *agent);

static void pooler_async_ping_node(Oid node);
static bool match_databasepool(DatabasePool *databasePool, const char* user_name, const char* database);
static int handle_close_pooled_connections(PoolAgent * agent, StringInfo s);
#ifdef __TBASE__
static void ConnectPoolManager(void);
#endif

static void init_pooler_cmd_statistics(void);
static void reset_pooler_cmd_statistics(void);
static void update_pooler_cmd_statistics(unsigned char qtype, uint64 costtime);
static void handle_get_cmd_statistics(PoolAgent *agent);
static void handle_get_conn_statistics(PoolAgent *agent);

#define IncreaseSlotRefCount(slot,filename,linenumber)\
do\
{\
    if (slot)\
    {\
        (slot->refcount)++;\
        if (slot->refcount > 1)\
        {\
            elog(PANIC, POOL_MGR_PREFIX"[%s:%d] invalid slot reference count:%d", filename, linenumber, slot->refcount);\
        }\
        slot->file     =    filename;\
        slot->lineno =  linenumber;\
    }\
}while(0)

#define DecreaseSlotRefCount(slot,filename,linenumber)\
do\
{\
    if (slot)\
    {\
        (slot->refcount)--;\
        if (slot->refcount < 0)\
        {\
            elog(PANIC, POOL_MGR_PREFIX"[%s:%d] invalid slot reference count:%d", filename, linenumber, slot->refcount);\
        }\
        slot->file     = filename;\
        slot->lineno = linenumber;\
    }\
}while(0)

    

#define DecreasePoolerSize(nodepool,file,lineno) \
do\
{\
    (nodepool->size)--;\
    if (nodepool->size < 0)\
    {\
        elog(PANIC, POOL_MGR_PREFIX"[%s:%d] invalid node pool size: %d, freesize:%d, nwarming:%d, node:%u, connstr:%s", file, lineno, nodepool->size, nodepool->freeSize, nodepool->nwarming, nodepool->nodeoid, nodepool->connstr);\
    }\
    if (PoolConnectDebugPrint)\
    {\
        elog(LOG, POOL_MGR_PREFIX"[%s:%d] decrease node pool size: %d, freesize:%d, nwarming:%d, node:%u, connstr:%s", file, lineno, nodepool->size, nodepool->freeSize, nodepool->nwarming, nodepool->nodeoid, nodepool->connstr);\
    }\
    if (nodepool->freeSize > nodepool->size && PoolConnectDebugPrint)\
    {\
        elog(PANIC, POOL_MGR_PREFIX"[%s:%d] invalid node pool size: %d, freesize:%d, nwarming:%d, node:%u, connstr:%s", file, lineno, nodepool->size, nodepool->freeSize, nodepool->nwarming, nodepool->nodeoid, nodepool->connstr);\
    }\
}while(0)


#define DecreasePoolerSizeAsync(nodepool, seqnum, file,lineno) \
    do\
    {\
        (nodepool->size)--;\
        if (nodepool->size < 0)\
        {\
            elog(PANIC, POOL_MGR_PREFIX"[%s:%d] invalid node pool size: %d, freesize:%d, nwarming:%d, node:%u, seq_num:%d", file, lineno, nodepool->size, nodepool->freeSize, nodepool->nwarming, nodepool->nodeoid, seqnum);\
        }\
        if (PoolConnectDebugPrint)\
        {\
            elog(LOG, POOL_MGR_PREFIX"[%s:%d] decrease node pool size: %d, freesize:%d, nwarming:%d, node:%u, seq_num:%d", file, lineno, nodepool->size, nodepool->freeSize, nodepool->nwarming, nodepool->nodeoid, seqnum);\
        }\
        if (nodepool->freeSize > nodepool->size && PoolConnectDebugPrint)\
        {\
            elog(PANIC, POOL_MGR_PREFIX"[%s:%d] invalid node pool size: %d, freesize:%d, nwarming:%d, node:%u, seq_num:%d", file, lineno, nodepool->size, nodepool->freeSize, nodepool->nwarming, nodepool->nodeoid, seqnum);\
        }\
    }while(0)


#define IncreasePoolerSize(nodepool,file,lineno) \
    do\
    {\
        (nodepool->size)++;\
        if (PoolConnectDebugPrint)\
        {\
            elog(LOG, POOL_MGR_PREFIX"[%s:%d] increase node pool size: %d, freesize:%d, nwarming:%d, node:%u, connstr:%s", file, lineno, nodepool->size, nodepool->freeSize, nodepool->nwarming, nodepool->nodeoid, nodepool->connstr);\
        }\
        if (nodepool->freeSize > nodepool->size && PoolConnectDebugPrint)\
        {\
            elog(PANIC, POOL_MGR_PREFIX"[%s:%d] invalid node pool size: %d, freesize:%d, nwarming:%d, node:%u, connstr:%s", file, lineno, nodepool->size, nodepool->freeSize, nodepool->nwarming, nodepool->nodeoid, nodepool->connstr);\
        }\
    }while(0)


#define ValidatePoolerSize(nodepool,file,lineno) \
do\
{\
    if (nodepool->size < 0)\
    {\
        elog(PANIC, POOL_MGR_PREFIX"[%s:%d] invalid node pool size: %d, freesize:%d, nwarming:%d, node:%u, connstr:%s", file, lineno, nodepool->size, nodepool->freeSize, nodepool->nwarming, nodepool->nodeoid, nodepool->connstr);\
    }\
    if (nodepool->freeSize > nodepool->size && PoolConnectDebugPrint)\
    {\
        elog(PANIC, POOL_MGR_PREFIX"[%s:%d] invalid node pool size: %d, freesize:%d, nwarming:%d, node:%u, connstr:%s", file, lineno, nodepool->size, nodepool->freeSize, nodepool->nwarming, nodepool->nodeoid, nodepool->connstr);\
    }\
}while(0)

#define DecreasePoolerFreesize(nodepool,file,lineno) \
do\
{\
    (nodepool->freeSize)--;\
    if (nodepool->freeSize < 0)\
    {\
        elog(PANIC, POOL_MGR_PREFIX"[%s:%d] invalid node pool size: %d, freesize:%d, nwarming:%d, node:%u, connstr:%s", file, lineno, nodepool->size, nodepool->freeSize, nodepool->nwarming, nodepool->nodeoid, nodepool->connstr);\
    }\
    if (PoolConnectDebugPrint)\
    {\
        elog(LOG, POOL_MGR_PREFIX"[%s:%d] decrease freesize node pool size: %d, freesize:%d, nwarming:%d, node:%u, connstr:%s", file, lineno, nodepool->size, nodepool->freeSize, nodepool->nwarming, nodepool->nodeoid, nodepool->connstr);\
    }\
    if (nodepool->freeSize > nodepool->size && PoolConnectDebugPrint)\
    {\
        elog(PANIC, POOL_MGR_PREFIX"[%s:%d] invalid node pool size: %d, freesize:%d, nwarming:%d, node:%u, connstr:%s", file, lineno, nodepool->size, nodepool->freeSize, nodepool->nwarming, nodepool->nodeoid, nodepool->connstr);\
    }\
}while(0)

#define IncreasePoolerFreesize(nodepool,file,lineno) \
do\
{\
    (nodepool->freeSize)++;\
    if (PoolConnectDebugPrint)\
    {\
        elog(LOG, POOL_MGR_PREFIX"[%s:%d] increase freesize node pool size: %d, freesize:%d, nwarming:%d, node:%u, connstr:%s", file, lineno, nodepool->size, nodepool->freeSize, nodepool->nwarming, nodepool->nodeoid, nodepool->connstr);\
    }\
    if (nodepool->freeSize > nodepool->size && PoolConnectDebugPrint)\
    {\
        elog(PANIC, POOL_MGR_PREFIX"[%s:%d] invalid node pool size: %d, freesize:%d, nwarming:%d, node:%u, connstr:%s", file, lineno, nodepool->size, nodepool->freeSize, nodepool->nwarming, nodepool->nodeoid, nodepool->connstr);\
    }\
}while(0)

void
PGXCPoolerProcessIam(void)
{
    am_pgxc_pooler = true;
}

bool
IsPGXCPoolerProcess(void)
{
    return am_pgxc_pooler;
}

/*
 * Initialize internal structures
 */
int
PoolManagerInit()
{
    elog(DEBUG1, POOL_MGR_PREFIX"Pooler process is started: %d", getpid());

    /*
     * Set up memory contexts for the pooler objects
     */
    PoolerMemoryContext = AllocSetContextCreate(TopMemoryContext,
                                                "PoolerMemoryContext",
                                                ALLOCSET_DEFAULT_MINSIZE,
                                                ALLOCSET_DEFAULT_INITSIZE,
                                                ALLOCSET_DEFAULT_MAXSIZE);
    PoolerCoreContext = AllocSetContextCreate(PoolerMemoryContext,
                                              "PoolerCoreContext",
                                              ALLOCSET_DEFAULT_MINSIZE,
                                              ALLOCSET_DEFAULT_INITSIZE,
                                              ALLOCSET_DEFAULT_MAXSIZE);
    PoolerAgentContext = AllocSetContextCreate(PoolerMemoryContext,
                                               "PoolerAgentContext",
                                               ALLOCSET_DEFAULT_MINSIZE,
                                               ALLOCSET_DEFAULT_INITSIZE,
                                               ALLOCSET_DEFAULT_MAXSIZE);

    ForgetLockFiles();    

    /*
     * If possible, make this process a group leader, so that the postmaster
     * can signal any child processes too.    (pool manager probably never has any
     * child processes, but for consistency we make all postmaster child
     * processes do this.)
     */
#ifdef HAVE_SETSID
	if (setsid() < 0)
		elog(DEBUG1, POOL_MGR_PREFIX"setsid() failed: %m");
		//elog(FATAL, POOL_MGR_PREFIX"setsid() failed: %m");
#endif
    /*
     * Properly accept or ignore signals the postmaster might send us
     */
    pqsignal(SIGINT,  pooler_die);
    pqsignal(SIGTERM, pooler_die);
    pqsignal(SIGQUIT, pooler_quickdie);
    pqsignal(SIGHUP,  pooler_sig_hup_handler);

    /* TODO other signal handlers */

    /* We allow SIGQUIT (quickdie) at all times */
    sigdelset(&BlockSig, SIGQUIT);

    /*
     * Unblock signals (they were blocked when the postmaster forked us)
     */
    PG_SETMASK(&UnBlockSig);

    /* Allocate pooler structures in the Pooler context */
    MemoryContextSwitchTo(PoolerMemoryContext);
    poolAgentSize = ALIGN_UP(MaxConnections, BITS_IN_LONGLONG);
    poolAgents = (PoolAgent **) palloc0(poolAgentSize * sizeof(PoolAgent *));
    if (poolAgents == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg(POOL_MGR_PREFIX"out of memory")));
    }

    poolAgentMgr = BmpMgrCreate(poolAgentSize);
    if (poolAgentMgr == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg(POOL_MGR_PREFIX"out of memory")));
    }

    agentIndexes = (int32*) palloc0(poolAgentSize * sizeof(int32));
    if (agentIndexes == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg(POOL_MGR_PREFIX"out of memory")));
    }
    
    if (MaxConnections > MaxPoolSize)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                     errmsg(POOL_MGR_PREFIX"max_pool_size can't be smaller than max_connections")));
    }
    PoolerLoop();
    return 0;
}


/*
 * Check connection info consistency with system catalogs
 */
static int
node_info_check(PoolAgent *agent)
{// #lizard forgives
    DatabasePool   *dbPool = databasePools;
    List            *checked = NIL;
    int             res = POOL_CHECK_SUCCESS;
    Oid               *coOids;
    Oid               *dnOids;
    int                numCo;
    int                numDn;

    /*
     * First check if agent's node information matches to current content of the
     * shared memory table.
     */
    PgxcNodeGetOids(&coOids, &dnOids, &numCo, &numDn, false);

    if (agent->num_coord_connections != numCo ||
            agent->num_dn_connections != numDn ||
            memcmp(agent->coord_conn_oids, coOids, numCo * sizeof(Oid)) ||
            memcmp(agent->dn_conn_oids, dnOids, numDn * sizeof(Oid)))
    {
        res = POOL_CHECK_FAILED;
    }

    /* Release palloc'ed memory */
    pfree(coOids);
    pfree(dnOids);

    /*
     * Iterate over all dbnode pools and check if connection strings
     * are matching node definitions.
     */
    while (res == POOL_CHECK_SUCCESS && dbPool)
    {
        HASH_SEQ_STATUS hseq_status;
        PGXCNodePool   *nodePool;

        hash_seq_init(&hseq_status, dbPool->nodePools);
        while ((nodePool = (PGXCNodePool *) hash_seq_search(&hseq_status)))
        {
            char            *connstr_chk;

            /* No need to check same Datanode twice */
            if (list_member_oid(checked, nodePool->nodeoid))
                continue;
            checked = lappend_oid(checked, nodePool->nodeoid);

            connstr_chk = build_node_conn_str(nodePool->nodeoid, dbPool);
            if (connstr_chk == NULL)
            {
                /* Problem of constructing connection string */
                hash_seq_term(&hseq_status);
                res = POOL_CHECK_FAILED;
                break;
            }
            /* return error if there is difference */
            if (strcmp(connstr_chk, nodePool->connstr))
            {
                pfree(connstr_chk);
                hash_seq_term(&hseq_status);
                res = POOL_CHECK_FAILED;
                break;
            }

            pfree(connstr_chk);
        }
        dbPool = dbPool->next;
    }
    list_free(checked);
    return res;
}

/*
 * Destroy internal structures
 */
int
PoolManagerDestroy(void)
{
    int            status = 0;

    if (PoolerMemoryContext)
    {
        MemoryContextDelete(PoolerMemoryContext);
        PoolerMemoryContext = NULL;
    }

    return status;
}


/*
 * Get handle to pool manager
 * Invoked from Postmaster's main loop just before forking off new session
 * Returned PoolHandle structure will be inherited by session process
 */
PoolHandle *
GetPoolManagerHandle(void)
{
    PoolHandle *handle;
    int            fdsock;

    /* Connect to the pooler */
    fdsock = pool_connect(PoolerPort, Unix_socket_directories);
    if (fdsock < 0)
    {
        int            saved_errno = errno;

        ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_FAILURE),
                 errmsg(POOL_MGR_PREFIX"failed to connect to pool manager: %m")));
        errno = saved_errno;
        return NULL;
    }

    /* Allocate handle */
    /*
     * XXX we may change malloc here to palloc but first ensure
     * the CurrentMemoryContext is properly set.
     * The handle allocated just before new session is forked off and
     * inherited by the session process. It should remain valid for all
     * the session lifetime.
     */
    handle = (PoolHandle *) malloc(sizeof(PoolHandle));
    if (!handle)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg(POOL_MGR_PREFIX"out of memory")));
        return NULL;
    }

    handle->port.fdsock = fdsock;
    handle->port.RecvLength = 0;
    handle->port.RecvPointer = 0;
    handle->port.SendPointer = 0;

    return handle;
}


/*
 * Close handle
 */
void
PoolManagerCloseHandle(PoolHandle *handle)
{
    close(Socket(handle->port));
    free(handle);
}


/*
 * Create agent
 */
static void
agent_create(int new_fd)
{
    int32         agentindex = 0;
    MemoryContext oldcontext;
    PoolAgent  *agent;    

    agentindex = BmpMgrAlloc(poolAgentMgr);    
    if (-1 == agentindex)
    {
        ereport(PANIC,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                     errmsg(POOL_MGR_PREFIX"out of agent index")));
    }
    
    oldcontext = MemoryContextSwitchTo(PoolerAgentContext);

    /* Allocate agent */
    agent = (PoolAgent *) palloc0(sizeof(PoolAgent));
    if (!agent)
    {
        close(new_fd);
        BmpMgrFree(poolAgentMgr, agentindex);
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg(POOL_MGR_PREFIX"out of memory")));
        return;
    }

    agent->port.fdsock = new_fd;
    agent->port.RecvLength = 0;
    agent->port.RecvPointer = 0;
    agent->port.SendPointer = 0;
#ifdef __TBASE__
    agent->port.error_code  = 0;
    SpinLockInit(&agent->port.lock);
#endif
    agent->pool = NULL;
    agent->mcxt = AllocSetContextCreate(CurrentMemoryContext,
                                        "Agent",
                                        ALLOCSET_DEFAULT_MINSIZE,
                                        ALLOCSET_DEFAULT_INITSIZE,
                                        ALLOCSET_DEFAULT_MAXSIZE);
    agent->num_dn_connections = 0;
    agent->num_coord_connections = 0;
    agent->dn_conn_oids = NULL;
    agent->coord_conn_oids = NULL;
    agent->dn_connections = NULL;
    agent->coord_connections = NULL;
    agent->session_params = NULL;
    agent->local_params = NULL;
    agent->is_temp = false;
    agent->pid = 0;
    agent->agentindex = agentindex;

    /* Append new agent to the list */    
    poolAgents[agentindex] = agent;
    
    agentCount++;    
    
    MemoryContextSwitchTo(oldcontext);
    if (PoolConnectDebugPrint)
    {
        elog(LOG, POOL_MGR_PREFIX"agent_create end, agentCount:%d, fd:%d", agentCount, new_fd);
    }
}

/*
 * session_options
 * Returns the pgoptions string generated using a particular
 * list of parameters that are required to be propagated to Datanodes.
 * These parameters then become default values for the pooler sessions.
 * For e.g., a psql user sets PGDATESTYLE. This value should be set
 * as the default connection parameter in the pooler session that is
 * connected to the Datanodes. There are various parameters which need to
 * be analysed individually to determine whether these should be set on
 * Datanodes.
 *
 * Note: These parameters values are the default values of the particular
 * Coordinator backend session, and not the new values set by SET command.
 *
 */

char *session_options(void)
{
    int                 i;
    char            *pgoptions[] = {"DateStyle", "timezone", "geqo", "intervalstyle", "lc_monetary"};
    StringInfoData     options;
    List            *value_list;
    ListCell        *l;

    initStringInfo(&options);

    for (i = 0; i < sizeof(pgoptions)/sizeof(char*); i++)
    {
        const char        *value;

        appendStringInfo(&options, " -c %s=", pgoptions[i]);

        value = GetConfigOptionResetString(pgoptions[i]);

        /* lc_monetary does not accept lower case values */
        if (strcmp(pgoptions[i], "lc_monetary") == 0)
        {
            appendStringInfoString(&options, value);
            continue;
        }

		if (SplitIdentifierString(strdup(value), ',', &value_list) < 0)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("parse session options failed.")));
        }

        foreach(l, value_list)
        {
            char *value = (char *) lfirst(l);
            appendStringInfoString(&options, value);
            if (lnext(l))
                appendStringInfoChar(&options, ',');
        }
    }

    return options.data;
}

/*
 * Associate session with specified database and respective connection pool
 * Invoked from Session process
 */
void
PoolManagerConnect(PoolHandle *handle,
                   const char *database, const char *user_name,
                   char *pgoptions)
{
    int n32;
    char msgtype = 'c';

    Assert(handle);
    Assert(database);
    Assert(user_name);

    /* Save the handle */
    poolHandle = handle;

    /* Message type */
    pool_putbytes(&handle->port, &msgtype, 1);

    /* Message length */
    n32 = htonl(strlen(database) + strlen(user_name) + strlen(pgoptions) + 23);
    pool_putbytes(&handle->port, (char *) &n32, 4);

    /* PID number */
    n32 = htonl(MyProcPid);
    pool_putbytes(&handle->port, (char *) &n32, 4);

    /* Length of Database string */
    n32 = htonl(strlen(database) + 1);
    pool_putbytes(&handle->port, (char *) &n32, 4);

    /* Send database name followed by \0 terminator */
    pool_putbytes(&handle->port, database, strlen(database) + 1);
    pool_flush(&handle->port);

    /* Length of user name string */
    n32 = htonl(strlen(user_name) + 1);
    pool_putbytes(&handle->port, (char *) &n32, 4);

    /* Send user name followed by \0 terminator */
    pool_putbytes(&handle->port, user_name, strlen(user_name) + 1);
    pool_flush(&handle->port);

    /* Length of pgoptions string */
    n32 = htonl(strlen(pgoptions) + 1);
    pool_putbytes(&handle->port, (char *) &n32, 4);

    /* Send pgoptions followed by \0 terminator */
    pool_putbytes(&handle->port, pgoptions, strlen(pgoptions) + 1);
    pool_flush(&handle->port);

}

/*
 * Reconnect to pool manager
 * It simply does a disconnection and a reconnection.
 */
void
PoolManagerReconnect(void)
{
    HOLD_POOLER_RELOAD();

    if (poolHandle)
    {
        PoolManagerDisconnect();
    }
    
	ConnectPoolManager();

    RESUME_POOLER_RELOAD();
}

int
PoolManagerSetCommand(PGXCNodeHandle **connections, int32 count, PoolCommandType command_type, 
                      const char *set_command)
{// #lizard forgives
    int32 i   = 0;
    int32 oid_length = 0;
    int32 n32 = 0;
    int32 res = 0;
    CommandId cmdID = InvalidCommandId;
    char msgtype = 's';
    char *p   = NULL;
    char *sep = NULL;
    
    if (PoolConnectDebugPrint)
    {
        elog(LOG, "[PoolManagerSetCommand]recv command_type=%d, count=%d set_command=%s", command_type, count, set_command);
    }
    
    if (set_command)
    {
        /* validate set command format */
        p = (char*)set_command;
        while ( *p != '\0')
        {
            sep = strstr(p, ";");
            if (sep)
            {
                sep++;
                /* skip tab and space */
                while (*sep != '\0' && !isalnum (*sep))
                {
                    sep++;
                }

                if (*sep == '\0')
                {
                    break;
                }

                /* skip set command */
                if (strlen(sep) > 3)
                {
                    if ((sep[0] == 's' || sep[0] == 'S') &&
                        (sep[1] == 'e' || sep[1] == 'E') &&
                        (sep[2] == 't' || sep[2] == 'T'))
                    {
                        sep = strstr(sep, ";");    
                        if (NULL == sep)
                        {
                            break;
                        }
                        p = sep + 1;                    
                        continue;
                    }

                    if ((sep[0] == 'r' || sep[0] == 'R') &&
                        (sep[1] == 'e' || sep[1] == 'E') &&
                        (sep[2] == 's' || sep[2] == 'S') &&
                        (sep[3] == 'e' || sep[3] == 'E') &&
                        (sep[4] == 't' || sep[4] == 'T'))
                    {
                        sep = strstr(sep, ";");    
                        if (NULL == sep)
                        {
                            break;
                        }
                        p = sep + 1;                    
                        continue;
                    }

                    /* skip select */
                    if (strlen(sep) > 6)
                    {
                        if ((sep[0] == 's' || sep[0] == 'S') &&
                            (sep[1] == 'e' || sep[1] == 'E') &&
                            (sep[2] == 'l' || sep[2] == 'L') &&
                            (sep[3] == 'e' || sep[3] == 'E') &&
                            (sep[4] == 'c' || sep[4] == 'C') &&
                            (sep[5] == 't' || sep[5] == 'T'))
                        {
                            sep = strstr(sep, ";");
                            if (NULL == sep)
                            {
                                break;
                            }
                            p = sep + 1;                    
                            continue;
                        }    
                    }
                    elog(ERROR, POOL_MGR_PREFIX"ERROR SET query, set can not be followed by other query %s", set_command);            
                }
                else
                {
                    break;
                }
            }
            else
            {
                break;
            }
        }
    }

    if (NULL == poolHandle)
    {
		ConnectPoolManager();
    }

    if (poolHandle)
    {    
        Assert(poolHandle);

        /*
         * If SET LOCAL is in use, flag current transaction as using
         * transaction-block related parameters with pooler agent.
         */
        if (command_type == POOL_CMD_LOCAL_SET)
            SetCurrentLocalParamStatus(true);

        /* Message type */
        pool_putbytes(&poolHandle->port, &msgtype, 1);

        /* total_Len = msg_len(4)+ oid_count(4)  + oid_length(oid_count * 4) + set_command */
        if (POOL_SET_COMMAND_ALL == count)
        {
            oid_length = sizeof(count);
        }
        else
        {
            oid_length = sizeof(count) + sizeof(Oid) * count;
        }
        
        /* Message length */
        if (set_command)
        {
            n32 =  htonl(strlen(set_command) + 13 + oid_length); /*  length + oid_length + type + string_length + '\0' = 4 + 4 + 4 + 1*/
        }
        else
        {
            n32 = htonl(12 + oid_length);  /*  length + oid_length+ type + string_length(0) = 4 + 4 + 4*/
        }

        pool_putbytes(&poolHandle->port, (char *) &n32, 4); /* msg_len */

        n32 = htonl(count);
        pool_putbytes(&poolHandle->port, (char *) &n32, 4); /* oid_count */

        if (count != POOL_SET_COMMAND_ALL && count != POOL_SET_COMMAND_NONE)
        {
            for (i = 0; i < count; i++)
            {            
                elog(LOG, "PoolManagerSetCommand send %s to node %s, pid:%d", set_command, connections[i]->nodename, connections[i]->backend_pid);
                if ('E' == connections[i]->transaction_status)
                {
                    abort();
                }            
                n32 = htonl(connections[i]->nodeoid);
                pool_putbytes(&poolHandle->port, (char *) &n32, 4); /* oids */
            }
        }

        /* LOCAL or SESSION parameter ? */
        n32 = htonl(command_type);
        pool_putbytes(&poolHandle->port, (char *) &n32, 4);

        if (set_command)
        {
            /* Length of SET command string */
            n32 = htonl(strlen(set_command) + 1);
            pool_putbytes(&poolHandle->port, (char *) &n32, 4);

            /* Send command string followed by \0 terminator */
            pool_putbytes(&poolHandle->port, set_command, strlen(set_command) + 1);
        }
        else
        {
            /* Send empty command */
            n32 = htonl(0);
            pool_putbytes(&poolHandle->port, (char *) &n32, 4);
        }

        pool_flush(&poolHandle->port);
        
        /* Get result */
        res = pool_recvres_with_commandID(&poolHandle->port, &cmdID, set_command);
        if (PoolConnectDebugPrint)
        {
            elog(LOG, "[PoolManagerSetCommand] receive reponse of set command to pooler: %s, command_type=%d, res=%d, cmdID=%u",
                    set_command, command_type, res, cmdID);
        }

        if (cmdID != InvalidCommandId)
        {
            if (cmdID > GetReceivedCommandId())
            {
                SetReceivedCommandId(cmdID);
                if (PoolConnectDebugPrint)
                {
                    elog(LOG, "[PoolManagerSetCommand] set_local_command_id pooler: %s, command_type=%d, res=%d, cmdID=%u",
                            set_command, command_type, res, cmdID);
                }
            }
        }
    }
    else
    {
        elog(LOG, "[PoolManagerSetCommand] poolhandle is empty");
        abort();
    }
    return res;
}

/*
 * Send commands to alter the behavior of current transaction and update begin sent status
 */

int
PoolManagerSendLocalCommand(int dn_count, int* dn_list, int co_count, int* co_list)
{// #lizard forgives
    uint32        n32;
    /*
     * Buffer contains the list of both Coordinator and Datanodes, as well
     * as the number of connections
     */
    uint32         buf[2 + dn_count + co_count];
    int         i;

    if (poolHandle == NULL)
        return EOF;

    if (dn_count == 0 && co_count == 0)
        return EOF;

    if (dn_count != 0 && dn_list == NULL)
        return EOF;

    if (co_count != 0 && co_list == NULL)
        return EOF;

    /* Insert the list of Datanodes in buffer */
    n32 = htonl((uint32) dn_count);
    buf[0] = n32;

    for (i = 0; i < dn_count;)
    {
        n32 = htonl((uint32) dn_list[i++]);
        buf[i] = n32;
    }

    /* Insert the list of Coordinators in buffer */
    n32 = htonl((uint32) co_count);
    buf[dn_count + 1] = n32;

    /* Not necessary to send to pooler a request if there is no Coordinator */
    if (co_count != 0)
    {
        for (i = dn_count + 1; i < (dn_count + co_count + 1);)
        {
            n32 = htonl((uint32) co_list[i - (dn_count + 1)]);
            buf[++i] = n32;
        }
    }
    pool_putmessage(&poolHandle->port, 'b', (char *) buf, (2 + dn_count + co_count) * sizeof(uint32));
    pool_flush(&poolHandle->port);

	/* Get result */
	return pool_recvres(&poolHandle->port, true);
}

/*
 * Lock/unlock pool manager
 * During locking, the only operations not permitted are abort, connection and
 * connection obtention.
 */
void
PoolManagerLock(bool is_lock)
{
    char msgtype = 'o';
    int n32;
    int msglen = 8;

    HOLD_POOLER_RELOAD();

    if (poolHandle == NULL)
    {
		ConnectPoolManager();
    }

    /* Message type */
    pool_putbytes(&poolHandle->port, &msgtype, 1);

    /* Message length */
    n32 = htonl(msglen);
    pool_putbytes(&poolHandle->port, (char *) &n32, 4);

    /* Lock information */
    n32 = htonl((int) is_lock);
    pool_putbytes(&poolHandle->port, (char *) &n32, 4);
    pool_flush(&poolHandle->port);

    RESUME_POOLER_RELOAD();
}

/*
 * get pooler command statistics
 */
int
PoolManagerGetCmdStatistics(char *s, int size)
{
    int qtype = 0;
    char msgtype = 'x';
    HOLD_POOLER_RELOAD();

    if (poolHandle == NULL)
    {
        ConnectPoolManager();
    }

    /* Message type */
    pool_putbytes(&poolHandle->port, &msgtype, 1);
    pool_flush(&poolHandle->port);

    qtype = pool_getbyte(&poolHandle->port);
    if (qtype == EOF || (unsigned char)qtype != msgtype)
    {
        elog(ERROR, POOL_MGR_PREFIX"get command statistics error, qtype:%d", qtype);
        RESUME_POOLER_RELOAD();
        return -1;
    }

    /* get all command statistics messages */
    pool_getbytes(&poolHandle->port, s, size);

    RESUME_POOLER_RELOAD();
    return 0;
}

/*
 * reset command statistics
 */
void
PoolManagerResetCmdStatistics(void)
{
    char msgtype = 'y';
    HOLD_POOLER_RELOAD();

    if (poolHandle == NULL)
    {
        ConnectPoolManager();
    }

    /* Message type */
    pool_putbytes(&poolHandle->port, &msgtype, 1);
    pool_flush(&poolHandle->port);

    RESUME_POOLER_RELOAD();
}

/*
 * get pooler connections statistics
 */
int
PoolManagerGetConnStatistics(StringInfo s)
{
    int qtype = 0;
    char msgtype = 'z';
    HOLD_POOLER_RELOAD();

    if (poolHandle == NULL)
    {
        ConnectPoolManager();
    }

    /* Message type */
    pool_putbytes(&poolHandle->port, &msgtype, 1);
    pool_flush(&poolHandle->port);

    qtype = pool_getbyte(&poolHandle->port);
    if (qtype == EOF || (unsigned char)qtype != msgtype)
    {
        elog(ERROR, POOL_MGR_PREFIX"get conn statistics error, qtype:%d", qtype);
        RESUME_POOLER_RELOAD();
        return -1;
    }

    /* get all the messages left */
    pool_getmessage(&poolHandle->port, s, 0);

    RESUME_POOLER_RELOAD();
    return 0;
}

/*
 * Init PoolAgent
 */
static void
agent_init(PoolAgent *agent, const char *database, const char *user_name,
           const char *pgoptions)
{
    MemoryContext oldcontext;

    Assert(agent);
    Assert(database);
    Assert(user_name);
        
    /* disconnect if we are still connected */
    if (agent->pool)
    {
		agent_release_connections(agent, false, false);
    }

    oldcontext = MemoryContextSwitchTo(agent->mcxt);

    /* Get needed info and allocate memory */
    PgxcNodeGetOids(&agent->coord_conn_oids, &agent->dn_conn_oids,
                    &agent->num_coord_connections, &agent->num_dn_connections, false);

    agent->coord_connections = (PGXCNodePoolSlot **)
            palloc0(agent->num_coord_connections * sizeof(PGXCNodePoolSlot *));
    agent->dn_connections = (PGXCNodePoolSlot **)
            palloc0(agent->num_dn_connections * sizeof(PGXCNodePoolSlot *));
    /* find database */
    agent->pool = find_database_pool(database, user_name, pgoptions);

    /* create if not found */
    if (agent->pool == NULL)
    {
        agent->pool = create_database_pool(database, user_name, pgoptions);
    }

    MemoryContextSwitchTo(oldcontext);

    agent->query_count     = 0;
    agent->destory_pending = false;
    agent->task_control    = NULL;
    agent->breconnecting   = false;
    return;
}

/*
 * Destroy PoolAgent
 */
static void
agent_destroy(PoolAgent *agent)
{// #lizard forgives
    bool   bsync = true;
    int32  agentindex;
    int32  fd;
    Assert(agent);
    
    agentindex = agent->agentindex;
    fd         = Socket(agent->port);
    close(fd);
    
    if (PoolConnectDebugPrint)
    {
        elog(LOG, POOL_MGR_PREFIX"agent_destroy close fd:%d, pid:%d, num_dn_connections:%d, num_coord_connections:%d, "
                                "session_params:%s, local_params:%s, is_temp:%d", 
            fd, agent->pid, agent->num_dn_connections, agent->num_coord_connections, 
            agent->session_params, agent->local_params,
            agent->is_temp);
    }

    /* check whether we can destory the agent */
    if (agent_can_destory(agent))
    {
        /* Discard connections if any remaining */
        if (agent->pool)
        {
            /*
             * Agent is being destroyed, so reset session parameters
             * before putting back connections to pool.
             */
            //bsync = agent_reset_session(agent);
            
            /*
             * Release them all.
             * Force disconnection if there are temporary objects on agent.
             */
            
            if (bsync)
            {
			    /*
			     * if temporary objects used for this pool session, release using synchronization
			     */
				agent_release_connections(agent, true, agent->is_temp);
            }                    
        }

        /* if async, we leave the following resource for the async thread */
         if (bsync)
        {
            agent_destory_task_control(agent);
            MemoryContextDelete(agent->mcxt);
            pfree(agent);
        }
    }
    else
    {
        agent_pend_destory(agent);        
    }

    /* Destory shadow */
    if (BmpMgrHasIndexAndClear(poolAgentMgr, agentindex))
    {
        --agentCount;
    }

    poolAgents[agentindex] = NULL; 
    
    if (PoolConnectDebugPrint)
    {
        elog(LOG, POOL_MGR_PREFIX"agent_destroy end, agentCount:%d", agentCount);
    }
}


static void
destroy_pend_agent(PoolAgent *agent)
{
    bool   bsync = true;
    
    
    if (PoolConnectDebugPrint)
    {
        elog(LOG, POOL_MGR_PREFIX"destroy_pend_agent enter");
    }

    /* check whether we can destory the agent */
    if (agent_can_destory(agent))
    {
        /* Discard connections if any remaining */
        if (agent->pool)
        {
            /*
             * Agent is being destroyed, so reset session parameters
             * before putting back connections to pool.
             */
            bsync = agent_reset_session(agent);
            
            /*
             * Release them all.
             * Force disconnection if there are temporary objects on agent.
             */
            if (bsync)
            {
				agent_release_connections(agent, true, false);
            }                    
        }

        /* if async, we leave the following resource for the async thread */
         if (bsync)
        {            
            agent_destory_task_control(agent);
            MemoryContextDelete(agent->mcxt);
            pfree(agent);
        }
        agent_set_destory(agent);
    }
}

static inline bool agent_destory_task_control(PoolAgent *agent)
{
    bool done = true;
    if (agent->task_control)
    {
        if (check_is_task_done(agent->task_control))
        {
            if (PoolConnectDebugPrint)
            {
                elog(LOG, POOL_MGR_PREFIX"agent_destory_task_control last time job is done!!pid:%d", agent->pid);
            }
            pfree(agent->task_control);
            agent->task_control = NULL;
            done = true;
        }
        else
        {
            elog(LOG, POOL_MGR_PREFIX"agent_destory_task_control last time job is not done yet!!pid:%d", agent->pid);
            done = false;
        }
    }
    return done;
}
/*
 * Release handle to pool manager
 */
void
PoolManagerDisconnect(void)
{
    HOLD_POOLER_RELOAD();
    if (poolHandle)
    {
        Assert(poolHandle);

        pool_putmessage(&poolHandle->port, 'd', NULL, 0);
        pool_flush(&poolHandle->port);

        PoolManagerCloseHandle(poolHandle);
        poolHandle = NULL;
    }

    RESUME_POOLER_RELOAD();
}


/*
 * Get pooled connections
 */
int *
PoolManagerGetConnections(List *datanodelist, List *coordlist, bool raise_error, int **pids)
{
    int            i;
    ListCell   *nodelist_item;
    int           *fds;
    int            totlen = list_length(datanodelist) + list_length(coordlist);
	int         totsize = sizeof(int) * (totlen + 2) + 1; /* sizeof nodes list + raise_error flag */
    int            nodes[totlen + 2];
	char       *msg;
    int         pool_recvpids_num;
    int         pool_recvfds_ret;

    int         j = 0;

#ifdef __TBASE__
	/*
	 * if it is the standby node of the main plane, the distributed query will be connected to
	 * the main data node, and the standby cn may generate the same global xid as the main cn,
	 * so disable the distributed query of the standby node on the main plane
	 */
	if (g_allow_distri_query_on_standby_node == false && IS_PGXC_MAINCLUSTER_SLAVENODE)
    {
        elog(ERROR, "can't do distributed query because it is the main plane standby node.");
    }
#endif

    HOLD_POOLER_RELOAD();
    
    if (poolHandle == NULL)
    {
		ConnectPoolManager();
    }
    
    /*
     * Prepare end send message to pool manager.
     * First with Datanode list.
     * This list can be NULL for a query that does not need
     * Datanode Connections (Sequence DDLs)
     */
    nodes[0] = htonl(list_length(datanodelist));
    i = 1;
    if (list_length(datanodelist) != 0)
    {
        foreach(nodelist_item, datanodelist)
        {
            nodes[i++] = htonl(lfirst_int(nodelist_item));
        }
    }
    /* Then with Coordinator list (can be nul) */
    nodes[i++] = htonl(list_length(coordlist));
    if (list_length(coordlist) != 0)
    {
        foreach(nodelist_item, coordlist)
        {
            nodes[i++] = htonl(lfirst_int(nodelist_item));
        }
    }

    /* Receive response */
    fds = (int *) palloc(sizeof(int) * totlen);
    if (fds == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg(POOL_MGR_PREFIX"out of memory")));
    }

	msg = palloc(totsize);
	memcpy(msg, (char *) nodes, totsize - 1);
	msg[totsize - 1] = (char) raise_error;
	pool_putmessage(&poolHandle->port, 'g', msg, totsize);
	pfree(msg);

    if (PoolConnectDebugPrint)
    {
        elog(LOG, POOL_MGR_PREFIX"backend required %d connections, cn_count:%d, dn_count:%d pooler_fd %d, MyProcPid:%d", 
                                totlen, list_length(coordlist), list_length(datanodelist), poolHandle->port.fdsock, MyProcPid);
    }
    
    pool_flush(&poolHandle->port);
    pool_recvfds_ret = pool_recvfds(&poolHandle->port, fds, totlen);
    if (pool_recvfds_ret)
    {
        pfree(fds);
        if (PoolConnectDebugPrint)
        {
            elog(LOG, "[PoolManagerGetConnections]pool_recvfds_ret=%d, failed to pool_recvfds. return NULL.", pool_recvfds_ret);
        }
        /* Disconnect off pooler. */
        PoolManagerDisconnect();
        RESUME_POOLER_RELOAD();
        return NULL;
    }

    *pids = NULL;
    pool_recvpids_num = pool_recvpids(&poolHandle->port, pids);
    if (pool_recvpids_num != totlen)
    {
        if(*pids)
        {
            pfree(*pids);
            *pids = NULL;
        }
        /* Disconnect off pooler. */
        PoolManagerDisconnect();
        elog(LOG, "[PoolManagerGetConnections]pool_recvpids_num=%d, totlen=%d. failed to pool_recvpids. return NULL.", pool_recvpids_num, totlen);
        RESUME_POOLER_RELOAD();
        return NULL;
    }

    if (PoolConnectDebugPrint)
    {
        for (j = 0; j < pool_recvpids_num; j++)
        {
            elog(LOG, "[PoolManagerGetConnections] PoolManagerGetConnections cnt:%d Proc:%d get conns pid:%d", j+1, MyProcPid, (*pids)[j]);
        }
    }

    RESUME_POOLER_RELOAD();
    return fds;
}

/*
 * Abort active transactions using pooler.
 * Take a lock forbidding access to Pooler for new transactions.
 */
int
PoolManagerAbortTransactions(char *dbname, char *username, int **proc_pids)
{
    int        num_proc_ids = 0;
    int        n32, msglen;
    char        msgtype = 'a';
    int        dblen = dbname ? strlen(dbname) + 1 : 0;
    int        userlen = username ? strlen(username) + 1 : 0;
    
    if (poolHandle == NULL)
    {
		ConnectPoolManager();
    }

    /* Message type */
    pool_putbytes(&poolHandle->port, &msgtype, 1);

    /* Message length */
    msglen = dblen + userlen + 12;
    n32 = htonl(msglen);
    pool_putbytes(&poolHandle->port, (char *) &n32, 4);

    /* Length of Database string */
    n32 = htonl(dblen);
    pool_putbytes(&poolHandle->port, (char *) &n32, 4);

    /* Send database name, followed by \0 terminator if necessary */
    if (dbname)
        pool_putbytes(&poolHandle->port, dbname, dblen);

    /* Length of Username string */
    n32 = htonl(userlen);
    pool_putbytes(&poolHandle->port, (char *) &n32, 4);

    /* Send user name, followed by \0 terminator if necessary */
    if (username)
        pool_putbytes(&poolHandle->port, username, userlen);

    pool_flush(&poolHandle->port);

    /* Then Get back Pids from Pooler */
    num_proc_ids = pool_recvpids(&poolHandle->port, proc_pids);

    return num_proc_ids;
}


/*
 * Clean up Pooled connections
 */
void
PoolManagerCleanConnection(List *datanodelist, List *coordlist, char *dbname, char *username)
{// #lizard forgives
    int            totlen = list_length(datanodelist) + list_length(coordlist);
    int            nodes[totlen + 2];
    ListCell        *nodelist_item;
    int            i, n32, msglen;
    char            msgtype = 'f';
    int            userlen = username ? strlen(username) + 1 : 0;
    int            dblen = dbname ? strlen(dbname) + 1 : 0;

    HOLD_POOLER_RELOAD();

    if (poolHandle == NULL)
    {
		ConnectPoolManager();
    }

    nodes[0] = htonl(list_length(datanodelist));
    i = 1;
    if (list_length(datanodelist) != 0)
    {
        foreach(nodelist_item, datanodelist)
        {
            nodes[i++] = htonl(lfirst_int(nodelist_item));
        }
    }
    /* Then with Coordinator list (can be nul) */
    nodes[i++] = htonl(list_length(coordlist));
    if (list_length(coordlist) != 0)
    {
        foreach(nodelist_item, coordlist)
        {
            nodes[i++] = htonl(lfirst_int(nodelist_item));
        }
    }

    /* Message type */
    pool_putbytes(&poolHandle->port, &msgtype, 1);

    /* Message length */
    msglen = sizeof(int) * (totlen + 2) + dblen + userlen + 12;
    n32 = htonl(msglen);
    pool_putbytes(&poolHandle->port, (char *) &n32, 4);

    /* Send list of nodes */
    pool_putbytes(&poolHandle->port, (char *) nodes, sizeof(int) * (totlen + 2));

    /* Length of Database string */
    n32 = htonl(dblen);
    pool_putbytes(&poolHandle->port, (char *) &n32, 4);

    /* Send database name, followed by \0 terminator if necessary */
    if (dbname)
        pool_putbytes(&poolHandle->port, dbname, dblen);

    /* Length of Username string */
    n32 = htonl(userlen);
    pool_putbytes(&poolHandle->port, (char *) &n32, 4);

    /* Send user name, followed by \0 terminator if necessary */
    if (username)
        pool_putbytes(&poolHandle->port, username, userlen);

    pool_flush(&poolHandle->port);

    RESUME_POOLER_RELOAD();

	/* Receive result message */
	if (pool_recvres(&poolHandle->port, true) != CLEAN_CONNECTION_COMPLETED)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg(POOL_MGR_PREFIX"Clean connections not completed. HINT: cannot drop the currently open database")));
	}
}


/*
 * Check connection information consistency cached in pooler with catalog information
 */
bool
PoolManagerCheckConnectionInfo(void)
{
    int res;

    /*
     * New connection may be established to clean connections to
     * specified nodes and databases.
     */
    if (poolHandle == NULL)
    {
		ConnectPoolManager();
    }
    
	PgxcNodeListAndCountWrapTransaction();
    pool_putmessage(&poolHandle->port, 'q', NULL, 0);
    pool_flush(&poolHandle->port);

	res = pool_recvres(&poolHandle->port, true);

    if (res == POOL_CHECK_SUCCESS)
        return true;

    return false;
}


/*
 * Reload connection data in pooler and drop all the existing connections of pooler
 */
void
PoolManagerReloadConnectionInfo(void)
{
    Assert(poolHandle);
	PgxcNodeListAndCountWrapTransaction();
    pool_putmessage(&poolHandle->port, 'p', NULL, 0);
    pool_flush(&poolHandle->port);
}

/*
 * get systime time, ms
 */
static pg_time_t
get_system_time()
{
    struct timeb t;
    ftime(&t);
    return 1000 * t.time + t.millitm;
}

/*
 * Handle messages to agent
 */
static void
agent_handle_input(PoolAgent * agent, StringInfo s)
{// #lizard forgives
    int            qtype;

    qtype = pool_getbyte(&agent->port);
    /*
     * We can have multiple messages, so handle them all
     */
    for (;;)
    {
        int            res;

        agent->cmd_start_time = get_system_time();

        /*
         * During a pool cleaning, Abort, Connect and Get Connections messages
         * are not allowed on pooler side.
         * It avoids to have new backends taking connections
         * while remaining transactions are aborted during FORCE and then
         * Pools are being shrinked.
         */
        if (is_pool_locked && (qtype == 'a' || qtype == 'c' || qtype == 'g'))
        {
            elog(WARNING,POOL_MGR_PREFIX"Pool operation cannot run during pool lock");
        }

        if (PoolConnectDebugPrint)
        {
            elog(LOG, POOL_MGR_PREFIX"get qtype=%c from backend pid:%d", qtype, agent->pid);
        }

        switch (qtype)
        {
            case 'a':            /* ABORT */
                handle_abort(agent, s);
                break;
            case 'b':            /* Fire transaction-block commands on given nodes */
                handle_command_to_nodes(agent, s);
                break;
            case 'c':            /* CONNECT */
                handle_connect(agent, s);
                break;
            case 'd':            /* DISCONNECT */
                pool_getmessage(&agent->port, s, 4);
                agent_destroy(agent);
                pq_getmsgend(s);
                break;
            case 'f':            /* CLEAN CONNECTION */
                handle_clean_connection(agent, s);
                break;
            case 'g':            /* GET CONNECTIONS */
                handle_get_connections(agent, s);
                break;

            case 'h':            /* Cancel SQL Command in progress on specified connections */
                handle_query_cancel(agent, s);
                break;
            case 'o':            /* Lock/unlock pooler */
                pool_getmessage(&agent->port, s, 8);
                is_pool_locked = pq_getmsgint(s, 4);
                pq_getmsgend(s);
                break;
            case 'p':            /* Reload connection info */
                /*
                 * Connection information reloaded concerns all the database pools.
                 * A database pool is reloaded as follows for each remote node:
                 * - node pool is deleted if the node has been deleted from catalog.
                 *   Subsequently all its connections are dropped.
                 * - node pool is deleted if its port or host information is changed.
                 *   Subsequently all its connections are dropped.
                 * - node pool is kept unchanged with existing connection information
                 *   is not changed. However its index position in node pool is changed
                 *   according to the alphabetical order of the node name in new
                 *   cluster configuration.
                 * Backend sessions are responsible to reconnect to the pooler to update
                 * their agent with newest connection information.
                 * The session invocating connection information reload is reconnected
                 * and uploaded automatically after database pool reload.
                 * Other server sessions are signaled to reconnect to pooler and update
                 * their connection information separately.
                 * During reload process done internally on pooler, pooler is locked
                 * to forbid new connection requests.
                 */
                pool_getmessage(&agent->port, s, 4);
                pq_getmsgend(s);

                /* First update all the pools */
                reload_database_pools(agent);
                break;
            case 'P':            /* Ping connection info */
                /*
                 * Ping unhealthy nodes in the pools. If any of the
                 * nodes come up, update SHARED memory to
                 * indicate the same.
                 */
                pool_getmessage(&agent->port, s, 4);
                pq_getmsgend(s);

                /* Ping all the pools */
                // TODO: task should done in thread.
                PoolPingNodes();
                break;
                
            case 'q':            /* Check connection info consistency */
                pool_getmessage(&agent->port, s, 4);
                pq_getmsgend(s);

                /* Check cached info consistency */
                res = node_info_check(agent);

                /* Send result */
                pool_sendres(&agent->port, res, NULL, 0, true);
                break;
            case 'r':            /* RELEASE CONNECTIONS */
                {
                    bool destroy;

                    pool_getmessage(&agent->port, s, 8);
                    destroy = (bool) pq_getmsgint(s, 4);
                    pq_getmsgend(s);
                    if (PoolConnectDebugPrint)
                    {
                        elog(LOG, POOL_MGR_PREFIX"receive command %c from agent:%d. destory=%d", qtype, agent->pid, destroy);
                    }
					agent_release_connections(agent, destroy, false);
                }
                break;
                
            case 'R':            /* Refresh connection info */
                pool_getmessage(&agent->port, s, 4);
                pq_getmsgend(s);
                res = refresh_database_pools(agent);
                pool_sendres(&agent->port, res, NULL, 0, true);
                break;    
                
            case 's':            /* Session-related COMMAND */
                handle_session_command(agent, s);
                break;

            case 't':
                res = handle_close_pooled_connections(agent ,s);
                /* Send result */
                pool_sendres(&agent->port, res, NULL, 0, true);
                break;

            case 'x':          /* get command statistics */
		        handle_get_cmd_statistics(agent);
		        break;

            case 'y':          /* reset command statistics */
                reset_pooler_cmd_statistics();
                break;

            case 'z':          /* get connections statistics */
                handle_get_conn_statistics(agent);
                break;

            case EOF:            /* EOF */
                agent_destroy(agent);
                return;    
            default:            /* EOF or protocol violation */
                elog(WARNING, POOL_MGR_PREFIX"invalid request tag:%c", qtype);
                agent_destroy(agent);
                return;
        }

        /* if cmd_start_time is not 0, means cmd handle in main loop sync, statistic here */
        if (agent->cmd_start_time != 0)
        {
            update_pooler_cmd_statistics(qtype, get_system_time() - agent->cmd_start_time);
        }

        /* avoid reading from connection */
        if ((qtype = pool_pollbyte(&agent->port)) == EOF)
            break;
    }
}

/*
 * Manage a session command for pooler
 */
static int
agent_session_command(PoolAgent *agent,
                        const char *set_command,                                    
                      Oid  *oids,
                      int32 oid_num,
                      PoolCommandType command_type)
{
    int res;
    if (PoolConnectDebugPrint)
    {
        elog(LOG, POOL_MGR_PREFIX"agent_session_command ENTER, pid:%d  async set command oid_num:%d command_type:%d command:%s", agent->pid, oid_num, command_type, set_command);
    }

    
    switch (command_type)
    {
        case POOL_CMD_LOCAL_SET:
        case POOL_CMD_GLOBAL_SET:
        {
            res = agent_set_command(agent, set_command, oids, oid_num, command_type);
            break;
        }

        case POOL_CMD_TEMP:
        {
            res = agent_temp_command(agent);
            break;
        }
        
        default:
        {
            res = -1;
            break;
        }
    }

    if (PoolConnectDebugPrint)
    {
        elog(LOG, POOL_MGR_PREFIX"agent_session_command EXIT pid:%d  async set command oid_num:%d command_type:%d res:%d command:%s", agent->pid, oid_num, command_type, res, set_command);
    }
    return res;
}

/*
 * Set agent flag that a temporary object is in use.
 */
static int
agent_temp_command(PoolAgent *agent)
{
    agent->is_temp = true;
    return 0;
}
/*
 * compress set command
 */
PoolerSetDesc *
agent_compress_command(char *set_command, PoolerSetDesc *set_desc, bool *need_free, int *len)
{// #lizard forgives    
#define POINTER_ARRRY_LEN  8
    int   count    = 0;
    int   max_len  = 0;
    PoolerSetDesc *pointer = NULL;
    char *begin    = NULL;
    if (set_command)
    {
        max_len    = *len;
        pointer    = set_desc;
        *need_free = false;        
        /* set command format: set XXX to XXX;set XXX to XXX;set XXX to XXX; */
        begin  = set_command;
        while (*begin != '\0')
        {
            char *p             = NULL;
            char *q              = NULL;
            char *value_end      = NULL;
            int   tag_len         = 0;
            int   total_len      = 0;
            
            /* skip head space */
            while (isspace(*begin))
            {
                begin++;
            }

            /* skip "set" */                    
            p = q = begin;
            while (!isspace(*p))
            {
                tag_len++;
                total_len++;
                p++;
            }
            /* skip a space */
            p += 1;
            tag_len++;
            total_len++;
            q = p;
            
            /* compress out the space */
            while (isspace(*q))
            {
                q++;
            }
            
            while (!isspace(*q) && *q != ';' && *q != '=' && *q != '\0')
            {
                *p = *q;
                p++;
                total_len++;
                tag_len++;
                q++;
            }

            /* tag only includes the first two tokens */
            if (*q == ';')
            {            
                *p = '\0';
            }    
            else if (*q == '\0')
            {
                *p = '\0';
            }
            else
            {                    
                /* copy the value */
                value_end   = q;
                while (*value_end != ';' && *value_end != '\0')
                {
                    value_end++;
                }

                do
                {
                    value_end--;
                }while(isspace(*value_end));
                
                while (q <= value_end)
                {
                    *p = *q;
                    p++;
                    total_len++;
                    q++;
                }

                /* skip space */
                while (isspace(*q))
                {
                    q++;
                }
                /* q must be ";" */
                *p = '\0';
            }

            if (count >= max_len)
            {
                if (set_desc == pointer)
                {
                    int i = 0;
                    pointer = (PoolerSetDesc*)palloc0(sizeof(PoolerSetDesc) * (max_len + POINTER_ARRRY_LEN));                    
                    for (i = 0; i < max_len; i++)
                    {
                        pointer[i] = set_desc[i];
                    }
                    *need_free = true;
                }
                else
                {
                    pointer = (PoolerSetDesc*)repalloc(pointer,
                                               sizeof(PoolerSetDesc) * (max_len + POINTER_ARRRY_LEN));                      
                }
                max_len = max_len + POINTER_ARRRY_LEN;
            }
            pointer[count].command     = begin;    
            pointer[count].tag_len     = tag_len;
            pointer[count].total_len = total_len;
            count++;
            if (*q == '\0')
            {
                break;
            }
            /*begin next statement */
            begin = q + 1;
        }
    }
    *len = count;
    return pointer;
}

void 
agent_handle_set_command(PoolAgent *agent, char *set_command, PoolCommandType command_type)
{// #lizard forgives
#define DEFAULT_GUC_ARRAY_LEN 8
    bool    need_new_set = false;
    bool    found        = false;
    bool    need_free    = false;
    bool    need_reform  = false;

    int     i       = 0;
    int     guc_num = 0;    
    ListCell          *guc_list_item = NULL;
    List*             guc_list         = NULL;
    char*              guc_str         = NULL;
    MemoryContext     oldcontext     = NULL;
    PoolerSetDesc     *guc_desc      = NULL;
    PoolerSetDesc     *guc_pointer   = NULL;
    PoolerSetDesc     guc_array[DEFAULT_GUC_ARRAY_LEN];
    /*
     * special case handled here
     * if receive 'RESET SESSION AUTHORIZATION' command, we need to add this to guc_list,
     * and remove all 'SET SESSION AUTHORIZATION' commands from guc_list
     */
    bool  reset_command = false;
    const char *reset_auth_command = "RESET SESSION AUTHORIZATION";
    const char *set_auth_command1 = "SET SESSION AUTHORIZATION";
    const char *set_auth_command2 = "SET SESSION_AUTHORIZATION";
    
    /* to optimize the performance we use pre_allocated  array */
    guc_num = DEFAULT_GUC_ARRAY_LEN;
    guc_pointer = agent_compress_command(set_command, guc_array, &need_free, &guc_num);
    
    if (POOL_CMD_GLOBAL_SET == command_type)
    {        
        guc_list = agent->session_params_list;
    }
    else if (POOL_CMD_LOCAL_SET == command_type)
    {
        guc_list = agent->local_params_list;
    }
    else
    {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg(POOL_MGR_PREFIX"Set command process failed, pid;%d", agent->pid)));
    }
    
    need_reform = false;
    for (i = 0; i < guc_num; i++)
    {
        need_new_set = false;
        found        = false;
        reset_command = false;

        if (0 == strncasecmp(reset_auth_command, guc_pointer[i].command, strlen(reset_auth_command)))
        {
            /* receive 'RESET SESSION AUTHORIZATION' command */
            reset_command = true;
        }

RECHECK:
        foreach(guc_list_item, guc_list)
        {
            guc_desc = (PoolerSetDesc*)lfirst(guc_list_item);

            if (reset_command)
            {
                if (0 == strncasecmp(guc_desc->command, set_auth_command1, strlen(set_auth_command1)) ||
                    0 == strncasecmp(guc_desc->command, set_auth_command2, strlen(set_auth_command2)))
                {
                    /* remove 'SET SESSION AUTHORIZATION' command */
                    pfree(guc_desc->command);
                    guc_list = list_delete_ptr(guc_list, guc_desc);
                    goto RECHECK;
                }
            }
            else
            {
                if (guc_desc->tag_len != guc_pointer[i].tag_len)
                {
                    continue;
                }
                else if (0 == strncmp(guc_desc->command, guc_pointer[i].command, guc_desc->tag_len))
                {
                    /* we are the same command , then to check whether the command value are identical */
                    if (guc_desc->total_len !=  guc_pointer[i].total_len)
                    {
                        need_new_set = true;
                    }
                    else if (0 == strcmp(guc_desc->command, guc_pointer[i].command))
                    {
                        /* identical set command , nothing to do */
                        need_new_set = false;
                    }
                    else
                    {
                        need_new_set = true;
                    }
                    found = true;
                    break;
                }
                else
                {
                    continue;
                }
            }
        }

        /* we got an exist guc command, remove it and replace it with the new one */
        if (found)
        {
            if (need_new_set)
            {
                /* free the old one */
                pfree(guc_desc->command);
                guc_list            = list_delete_ptr(guc_list, guc_desc);
                oldcontext          = MemoryContextSwitchTo(agent->mcxt);
                guc_desc->command   = pstrdup(guc_pointer[i].command);
                guc_desc->total_len = guc_pointer[i].total_len;
                guc_list            = lappend(guc_list, guc_desc);
                MemoryContextSwitchTo(oldcontext);
                need_reform =  true;
            }
        }
        else
        {
            /* add a new guc item */
            PoolerSetDesc *new_item = NULL;
            oldcontext = MemoryContextSwitchTo(agent->mcxt);
            new_item = palloc0(sizeof(PoolerSetDesc));
            new_item->tag_len = guc_pointer[i].tag_len;
            new_item->total_len = guc_pointer[i].total_len;
            new_item->command = pstrdup(guc_pointer[i].command);
            guc_list = lappend(guc_list, new_item);
            MemoryContextSwitchTo(oldcontext);
            need_reform =  true;
        }
    }        

    if (need_reform)
    {
        int32 total_len = 0;
        int32 offset    = 0;
        foreach(guc_list_item, guc_list)
        {
            guc_desc = (PoolerSetDesc*)lfirst(guc_list_item);
            total_len += guc_desc->total_len;
        }
        total_len += list_length(guc_list) * 2; /* ";" number */
        
        /* free the old memory */
        oldcontext = MemoryContextSwitchTo(agent->mcxt);
        
        /* form the new sql */
        guc_str = palloc0(total_len);
        foreach(guc_list_item, guc_list)
        {
            guc_desc = (PoolerSetDesc*)lfirst(guc_list_item);
            offset += snprintf(guc_str + offset, total_len - offset, "%s;", guc_desc->command);
        }
        MemoryContextSwitchTo(oldcontext);
        
        if (POOL_CMD_GLOBAL_SET == command_type)
        {        
            agent->session_params_list = guc_list;
            if (agent->session_params)
            {
                pfree(agent->session_params);
            }
            agent->session_params = guc_str;
        }
        else if (POOL_CMD_LOCAL_SET == command_type)
        {
            agent->local_params_list = guc_list;
            if (agent->local_params)
            {
                pfree(agent->local_params);
            }
            agent->local_params      = guc_str;
        }
        else
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg(POOL_MGR_PREFIX"Set command process failed, pid;%d", agent->pid)));
        }
    }
    pfree(set_command);
    /* free the memory */
    if (need_free)
    {
        pfree(guc_pointer);
    }
}
    

/*
 * Save a SET command and distribute it to the agent connections
 * already in use. Return the number of set command that have been sent out.
 */
static int
agent_set_command(PoolAgent *agent,
                  const char *set_command,                                    
                  Oid  *oids,
                  int32 oid_num,
                  PoolCommandType command_type)
{// #lizard forgives
    bool    ret = 0;
    int32   node_index = 0;
    int        i   = 0;
    int        res = 0;    
    char    node_type = '\0';
    MemoryContext     oldcontext     = NULL;
    char             *temp_command   = NULL;
    PGXCASyncTaskCtl *asyncTaskCtl   = NULL;    

    Assert(agent);
    Assert(set_command);
    Assert(command_type == POOL_CMD_LOCAL_SET || command_type == POOL_CMD_GLOBAL_SET);

    /* we can get here is because we have error last time, so here just check whether we have done the job */
    if (!agent_destory_task_control(agent))
    {
        elog(LOG, POOL_MGR_PREFIX"agent_set_command last request not finish yet pid:%d", agent->pid);
        goto FATAL_ERROR;
    }
    
    temp_command = pstrdup(set_command);
    agent_handle_set_command(agent, temp_command, command_type);    

    if (PoolConnectDebugPrint)
    {
        elog(LOG, "agent_set_command: session_params %s, local_params %s", agent->session_params, agent->local_params);
    }
    
    /* just remember the sql command , no need to do real set */
    if (POOL_SET_COMMAND_NONE == oid_num)
    {
        if (PoolConnectDebugPrint)
        {
            elog(LOG, POOL_MGR_PREFIX"handle_session_command pid:%d  async set command oid_num:%d , nothing to do!", agent->pid, oid_num);
        }
        return 0;
    }


    if (POOL_SET_COMMAND_ALL == oid_num)
    {
        if (PoolConnectDebugPrint)
        {
            elog(LOG, POOL_MGR_PREFIX"handle_session_command pid:%d  async set command oid_num:%d , set all nodes!", agent->pid, oid_num);
        }
        
        /*
         * Launch the new command to all the connections already hold by the agent
         * It does not matter if command is local or global as this has explicitely been sent
         * by client. PostgreSQL backend also cannot send to its pooler agent SET LOCAL if current
         * transaction is not in a transaction block. This has also no effect on local Coordinator
         * session.
         */
        for (i = 0; i < agent->num_dn_connections; i++)
        {
            if (agent->dn_connections[i])
            {
                if (!asyncTaskCtl)
                {
                    asyncTaskCtl = create_task_control(NULL, NULL, NULL, NULL);
                    oldcontext = MemoryContextSwitchTo(agent->mcxt);
                    asyncTaskCtl->m_command = pstrdup(set_command);
                    MemoryContextSwitchTo(oldcontext);
                }                
                
                ret = dispatch_set_command_request(asyncTaskCtl,
                                                   false,
                                                   agent,
                                                   i,
                                                   false);
                if (!ret)
                {
                    goto FATAL_ERROR;
                }
                res++;
            }
            else
            {
                if (PoolConnectDebugPrint)
                {
                    elog(LOG, POOL_MGR_PREFIX"handle_session_command pid:%d oid_num:%d datanode node index:%d command:%s null connection nothing to do!", agent->pid, oid_num, node_index, set_command);
                }
            }
        }
    
        for (i = 0; i < agent->num_coord_connections; i++)
        {
            if (agent->coord_connections[i])
            {
                if (!asyncTaskCtl)
                {
                    asyncTaskCtl = create_task_control(NULL, NULL, NULL, NULL);
                    oldcontext = MemoryContextSwitchTo(agent->mcxt);
                    asyncTaskCtl->m_command = pstrdup(set_command);
                    MemoryContextSwitchTo(oldcontext);
                }                
                
                ret = dispatch_set_command_request(asyncTaskCtl,
                                                   true,
                                                   agent,
                                                   i,
                                                   false);
                if (!ret)
                {
                    goto FATAL_ERROR;
                }
                res++;
            }            
            else
            {
                if (PoolConnectDebugPrint)
                {
                    elog(LOG, POOL_MGR_PREFIX"handle_session_command pid:%d oid_num:%d coordinator node index:%d command:%s null connection nothing to do!", agent->pid, oid_num, node_index, set_command);
                }
            }
        }    
    
        if (asyncTaskCtl)
        {
            set_command_total(asyncTaskCtl, res);
            ret = dispatch_set_command_request(asyncTaskCtl,
                                               false,
                                               agent,
                                               0,
                                               true);
            if (!ret)
            {
                goto FATAL_ERROR;
            }
        }
        return res;
    }
    else
    {
        for (i = 0; i < oid_num; i++)
        {
            node_index = get_node_info_by_nodeoid(oids[i], &node_type);
            switch (node_type)
            {
                case PGXC_NODE_DATANODE:
                    {
                        if (agent->dn_connections[node_index])
                        {
                            if (!asyncTaskCtl)
                            {
                                asyncTaskCtl = create_task_control(NULL, NULL, NULL, NULL);
                                oldcontext = MemoryContextSwitchTo(agent->mcxt);
                                asyncTaskCtl->m_command = pstrdup(set_command);
                                MemoryContextSwitchTo(oldcontext);
                            }                
                            
                            ret = dispatch_set_command_request(asyncTaskCtl,
                                                               false,
                                                               agent,
                                                               node_index,
                                                               false);
                            if (!ret)
                            {
                                goto FATAL_ERROR;
                            }
                            res++;
                        }                        
                        else
                        {
                            if (PoolConnectDebugPrint)
                            {
                                elog(LOG, POOL_MGR_PREFIX"handle_session_command pid:%d oid_num:%d datanode node index:%d command:%s null connection nothing to do!", agent->pid, oid_num, node_index, set_command);
                            }
                        }
                    }
                    break;
                    
                case PGXC_NODE_COORDINATOR:                
                    {
                        if (agent->coord_connections[i])
                        {
                            if (!asyncTaskCtl)
                            {
                                asyncTaskCtl = create_task_control(NULL, NULL, NULL, NULL);
                                oldcontext = MemoryContextSwitchTo(agent->mcxt);
                                asyncTaskCtl->m_command = pstrdup(set_command);
                                MemoryContextSwitchTo(oldcontext);
                            }                
                            
                            ret = dispatch_set_command_request(asyncTaskCtl,
                                                               true,
                                                               agent,
                                                               node_index,
                                                               false);
                            if (!ret)
                            {
                                goto FATAL_ERROR;
                            }
                            res++;
                        }
                        else
                        {
                            if (PoolConnectDebugPrint)
                            {
                                elog(LOG, POOL_MGR_PREFIX"handle_session_command pid:%d oid_num:%d coordinator node index:%d command:%s null connection nothing to do!", agent->pid, oid_num, node_index, set_command);
                            }
                        }
                    }
                    break;
                    
                    break;
                default:
                {
                    elog(PANIC, POOL_MGR_PREFIX"agent_set_command invalid node type:%c", node_type);
                }
                    
            }
        }

        if (asyncTaskCtl)
        {
            set_command_total(asyncTaskCtl, res);
            ret = dispatch_set_command_request(asyncTaskCtl,
                                               false,
                                               agent,
                                               0,
                                               true);
            if (!ret)
            {
                goto FATAL_ERROR;
            }
        }
        return res;
    }    
    
FATAL_ERROR:
    /* record the task control, in case of memory leak */
    if (asyncTaskCtl)
    {
        agent->task_control = asyncTaskCtl;
    }
    elog(LOG, POOL_MGR_PREFIX"agent_set_command failed pid:%d, command:%s", agent->pid, set_command);
    return -1;
}

/*
 * acquire connection
 * return -1: error happen
 * return 0 : when fd_result and pid_result set to NULL, async acquire connection will be done in parallel threads
 * return 0 : when fd_result and pid_result is not NULL, acquire connection is done(acquire from freeslot in pool).
 */
static int 
agent_acquire_connections(PoolAgent *agent, List *datanodelist, List *coordlist,
						  bool raise_error, int32 *num, int **fd_result, int **pid_result)
{
    int32              i    = 0;
    int32             acquire_seq = 0;
    int                  node = 0;
    int32             acquire_succeed_num = 0;
    int32             acquire_failed_num  = 0;
    int32             set_request_num     = 0;
    int32              connect_num  = 0;
    bool              succeed      = false;
    PGXCNodePool     *nodePool;
    PGXCNodePoolSlot *slot         = NULL;
    ListCell         *nodelist_item;
    MemoryContext     oldcontext;
    PGXCASyncTaskCtl *asyncTaskCtl = NULL;

    Assert(agent);

    acquire_seq = pooler_get_connection_acquire_num();
    if (PoolPrintStatTimeout > 0)
    {
        g_pooler_stat.client_request_conn_total++;
    }
    
    if (PoolConnectDebugPrint)
    {
        elog(LOG, POOL_MGR_PREFIX"[agent_acquire_connections]agent pid=%d, NumCoords=%d, NumDataNodes=%d, "
                                 "num_coord_connections=%d, num_dn_connections=%d, "
                                 "datanodelist len=%d, coordlist len=%d",
                                agent->pid, NumCoords, NumDataNodes, 
                                agent->num_coord_connections, agent->num_dn_connections,
                                list_length(datanodelist), list_length(coordlist));
    }
    
    /* we have scaled out the nodes, fresh node info */
    if (agent->num_coord_connections < NumCoords || agent->num_dn_connections < NumDataNodes)
    {
        int32  orig_cn_number = agent->num_coord_connections;
        int32  orig_dn_number = agent->num_dn_connections;
        PGXCNodePoolSlot **orig_cn_connections = agent->coord_connections;
        PGXCNodePoolSlot **orig_dn_connections = agent->dn_connections;
        
        elog(LOG, POOL_MGR_PREFIX"[agent_acquire_connections]Pooler found node number extension pid:%d, acquire_seq:%d, refresh node info now", agent->pid, acquire_seq);
        pfree((void*)(agent->dn_conn_oids));        
        agent->dn_conn_oids = NULL;
        pfree((void*)(agent->coord_conn_oids));        
        agent->coord_conn_oids = NULL;

        /* fix memleak */
        oldcontext = MemoryContextSwitchTo(agent->mcxt);
        
        PgxcNodeGetOids(&agent->coord_conn_oids, &agent->dn_conn_oids,
                        &agent->num_coord_connections, &agent->num_dn_connections, false);

        agent->coord_connections = (PGXCNodePoolSlot **)
                palloc0(agent->num_coord_connections * sizeof(PGXCNodePoolSlot *));
        agent->dn_connections = (PGXCNodePoolSlot **)
                palloc0(agent->num_dn_connections * sizeof(PGXCNodePoolSlot *));

        /* fix memleak */
        MemoryContextSwitchTo(oldcontext);

        /* index of newly added nodes must be biggger, so memory copy can hanle node extension */
        memcpy(agent->coord_connections, orig_cn_connections, sizeof(sizeof(PGXCNodePoolSlot *)) * orig_cn_number);
        memcpy(agent->dn_connections, orig_dn_connections, sizeof(sizeof(PGXCNodePoolSlot *)) * orig_dn_number);
        pfree((void*)orig_cn_connections);
        pfree((void*)orig_dn_connections);
    }
    
    /* Check if pooler can accept those requests */
    if (list_length(datanodelist) > agent->num_dn_connections ||
            list_length(coordlist) > agent->num_coord_connections)
    {
        elog(LOG, "[agent_acquire_connections]agent_acquire_connections called with invalid arguments -"
                "list_length(datanodelist) %d, num_dn_connections %d,"
                "list_length(coordlist) %d, num_coord_connections %d",
                list_length(datanodelist), agent->num_dn_connections,
                list_length(coordlist), agent->num_coord_connections);
#ifdef __TBASE__
        SpinLockAcquire(&agent->port.lock);
        agent->port.error_code = POOL_ERR_GET_CONNECTIONS_INVALID_ARGUMENT;
        snprintf(agent->port.err_msg, POOL_ERR_MSG_LEN, "agent_acquire_connections called with invalid arguments -"
                "list_length(datanodelist) %d, num_dn_connections %d,"
                "list_length(coordlist) %d, num_coord_connections %d",
                list_length(datanodelist), agent->num_dn_connections,
                list_length(coordlist), agent->num_coord_connections);
        SpinLockRelease(&agent->port.lock);
#endif
        return -1;
    }

    /*
     * Allocate memory
     * File descriptors of Datanodes and Coordinators are saved in the same array,
     * This array will be sent back to the postmaster.
     * It has a length equal to the length of the Datanode list
     * plus the length of the Coordinator list.
     * Datanode fds are saved first, then Coordinator fds are saved.
     */
    *fd_result = (int *) palloc((list_length(datanodelist) + list_length(coordlist)) * sizeof(int));
    if (*fd_result == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory")));
    }

    *pid_result = (int *) palloc((list_length(datanodelist) + list_length(coordlist)) * sizeof(int));
    if (*pid_result == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory")));
    }

    /* we can get here is because we have error last time, so here just check whether we have done the job */
    if (!agent_destory_task_control(agent))
    {
#ifdef __TBASE__
        SpinLockAcquire(&agent->port.lock);
        agent->port.error_code = POOL_ERR_GET_CONNECTIONS_TASK_NOT_DONE;
        snprintf(agent->port.err_msg, POOL_ERR_MSG_LEN, "%s", poolErrorMsg[agent->port.error_code]);
        SpinLockRelease(&agent->port.lock);
#endif
        elog(LOG, POOL_MGR_PREFIX"[agent_acquire_connections] pid:%d last request not finish yet", agent->pid);
        goto FATAL_ERROR;
    }
    
    /*
     * There are possible memory allocations in the core pooler, we want
     * these allocations in the contect of the database pool
     */
    oldcontext = MemoryContextSwitchTo(agent->pool->mcxt);

    /* Initialize fd_result */
    i = 0;
    /* Save in array fds of Datanodes first */
    foreach(nodelist_item, datanodelist)
    {
        node = lfirst_int(nodelist_item);

        /* valid check */
        if (node >= agent->num_dn_connections)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_OUT_OF_MEMORY),
                             errmsg("invalid node index:%d, num_dn_connections is %d, pid:%d", node, agent->num_dn_connections, agent->pid)));
        }

        slot = NULL;
        /* Acquire from the pool if none */
        if (NULL == agent->dn_connections[node])
        {
            slot = acquire_connection(agent->pool, &nodePool, node,
                                      agent->dn_conn_oids[node], false);

            /* Handle failure */
            if (slot == NULL)
            {
                if (PoolConnectDebugPrint)
                {
                    elog(LOG, POOL_MGR_PREFIX"[agent_acquire_connections]acquire_connection can't get conn of node:%s from free slots", nodePool->node_name);
                }

                /* we have task control pending, can not proceed, wait for the pending job done */
                if (agent->task_control)
                {
#ifdef __TBASE__
                    SpinLockAcquire(&agent->port.lock);
                    agent->port.error_code = POOL_ERR_GET_CONNECTIONS_TASK_NOT_DONE;
                    snprintf(agent->port.err_msg, POOL_ERR_MSG_LEN, "%s", poolErrorMsg[agent->port.error_code]);
                    SpinLockRelease(&agent->port.lock);
#endif
                    elog(LOG, POOL_MGR_PREFIX"[agent_acquire_connections] datanode node:%s nodeindex:%d, pid:%d , acquire_seq:%d last request still in progres", nodePool->node_name, node, agent->pid, acquire_seq);
                    goto FATAL_ERROR;
                }

                acquire_failed_num++;
                if (!asyncTaskCtl)
                {
                    asyncTaskCtl = create_task_control(datanodelist, coordlist, *fd_result, *pid_result);
                }

                if (PoolConnectDebugPrint)
                {
                    elog(LOG, POOL_MGR_PREFIX"[agent_acquire_connections]going to acquire conn by sync thread for node:%s.", nodePool->node_name);
                }
                
				asyncTaskCtl->m_missing_ok = !raise_error;
                /* dispatch build connection request */
                succeed = dispatch_connection_request(asyncTaskCtl,
                                                        false,
                                                        agent,
                                                        nodePool,
                                                        PoolConnectStaus_init, /* we need to build a new connection*/
                                                        agent->session_params ? PoolConnectStaus_set_param : PoolConnectStaus_connected,
                                                        node,
                                                        acquire_seq,
                                                        false/* whether we are the last request */
                                                        ); 
                if (!succeed)
                {
                    goto FATAL_ERROR;
                }

                if (PoolPrintStatTimeout > 0)
                {
                    g_pooler_stat.acquire_conn_from_thread++;
                }
                continue;
            }
            else
            {
                acquire_succeed_num++;    
				if (!slot->bdestoryed && difftime(time(NULL), slot->created) > PoolConnMaxLifetime)
				{
					elog(WARNING,
						POOL_MGR_PREFIX"agent_acquire_connections acquired connection to "
						"database:%s user:%s "
						"node:%s backend_pid:%d nodeidx:%d "
						"nodepool size:%d freeSize:%d that should be destoried.",
						agent->pool->database, agent->pool->user_name,
						nodePool->node_name, slot->backend_pid, node,
						nodePool->size, nodePool->freeSize);
				}
                if (PoolConnectDebugPrint)
                {
                    /* double check, to ensure no double destory and multiple agents for one slot */
                    elog(LOG, POOL_MGR_PREFIX"[agent_acquire_connections] pid:%d get datanode connection nodeindex:%d nodename:%s backend_pid:%d slot_seq:%d from hash table", agent->pid, node, slot->node_name, slot->backend_pid, slot->seqnum);
                    if (slot->bdestoryed || slot->pid != -1)
                    {
                        abort();
                    }
                }
                            
                /* Store in the descriptor */
                slot->pid = agent->pid;
                agent->dn_connections[node] = slot;
                if (agent->session_params)
                {                    
                    if (agent->task_control)
                    {
                        elog(LOG, POOL_MGR_PREFIX"[agent_acquire_connections] datanode node:%u nodeindex:%d pid:%d last request still in progres", agent->dn_conn_oids[node], node, agent->pid);                
                        goto FATAL_ERROR;
                    }
                    
                    set_request_num++;
                    if (!asyncTaskCtl)
                    {
                        asyncTaskCtl = create_task_control(datanodelist, coordlist, *fd_result, *pid_result);
                    }
                    
					asyncTaskCtl->m_missing_ok = !raise_error;
                    /* dispatch set param request */
                    succeed = dispatch_connection_request(asyncTaskCtl,
                                                            false,
                                                            agent,
                                                            nodePool,
                                                            PoolConnectStaus_connected, /* we already had a connection*/
                                                            PoolConnectStaus_set_param,
                                                            node,
                                                            acquire_seq,
                                                            false/* whether we are the last request */
                                                            );
                    if (!succeed)
                    {
                        goto FATAL_ERROR;
                    }
                    
                    if (PoolPrintStatTimeout > 0)
                    {
                        g_pooler_stat.acquire_conn_from_hashtab_and_set++;
                    }
                }
                else
                {
                    if (PoolPrintStatTimeout > 0)
                    {
                        g_pooler_stat.acquire_conn_from_hashtab++;
                    }
                }
            }            
        }
        else
        {
            if (PoolConnectDebugPrint)
            {
                slot = agent->dn_connections[node];
                elog(LOG, POOL_MGR_PREFIX"[agent_acquire_connections] datanode node:%s nodeindex:%d pid:%d already got a slot_seq:%d backend_pid:%d in agent", slot->node_name, node, agent->pid, slot->seqnum, slot->backend_pid);
            }
        }
    }

    /* Save then in the array fds for Coordinators */
    foreach(nodelist_item, coordlist)
    {
        node = lfirst_int(nodelist_item);

        /* valid check */
        if (node >= agent->num_coord_connections)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_OUT_OF_MEMORY),
                             errmsg(POOL_MGR_PREFIX"invalid node index:%d, num_coord_connections is %d, pid:%d", node, agent->num_coord_connections, agent->pid)));
        }
        
        /* Acquire from the pool if none */
        if (NULL == agent->coord_connections[node])
        {
            PGXCNodePoolSlot *slot = acquire_connection(agent->pool, &nodePool, node, agent->coord_conn_oids[node], true);

            /* Handle failure */
            if (slot == NULL)
            {
                acquire_failed_num++;
                /* we have task control pending, can not proceed, wait for the pending job done */
                if (agent->task_control)
                {
#ifdef __TBASE__
                    SpinLockAcquire(&agent->port.lock);
                    agent->port.error_code = POOL_ERR_GET_CONNECTIONS_TASK_NOT_DONE;
                    snprintf(agent->port.err_msg, POOL_ERR_MSG_LEN, "%s", poolErrorMsg[agent->port.error_code]);
                    SpinLockRelease(&agent->port.lock);
#endif
                    elog(LOG, POOL_MGR_PREFIX"[agent_acquire_connections] coord node:%s nodeindex:%d pid:%d last request still in progres", nodePool->node_name, node, agent->pid);                
                    goto FATAL_ERROR;
                }
                
                if (!asyncTaskCtl)
                {
                    asyncTaskCtl = create_task_control(datanodelist, coordlist, *fd_result, *pid_result);
                }

				asyncTaskCtl->m_missing_ok = !raise_error;
                /* dispatch build connection request */
                succeed = dispatch_connection_request(asyncTaskCtl,
                                                        true,
                                                        agent,
                                                        nodePool,
                                                        PoolConnectStaus_init, /* we need to build a new connection*/
                                                        agent->session_params ? PoolConnectStaus_set_param : PoolConnectStaus_connected,
                                                        node,
                                                        acquire_seq,
                                                        false/* whether we are the last request */
                                                        );
                if (!succeed)
                {
                    goto FATAL_ERROR;
                }

                if (PoolPrintStatTimeout > 0)
                {
                    g_pooler_stat.acquire_conn_from_thread++;
                }
            }
            else
            {                
                acquire_succeed_num++;
                if (PoolConnectDebugPrint)
                {
                    elog(LOG, POOL_MGR_PREFIX"[agent_acquire_connections] pid:%d get coord connection nodeindex:%d nodename:%s backend_pid:%d slot_seq:%d from hash table", agent->pid, node, slot->node_name, slot->backend_pid, slot->seqnum);
                    /* double check, to ensure no double destory and multiple agents for one slot */
                    if (slot->bdestoryed || slot->pid != -1)
                    {
                        abort();
                    }
                }
                /*
                  * Update newly-acquired slot with session parameters.
                 * Local parameters are fired only once BEGIN has been launched on
                 * remote nodes.
                */
                slot->pid = agent->pid;
                agent->coord_connections[node] = slot;
                if (agent->session_params)
                {
                    set_request_num++;
                    /* we have task control pending, can not proceed, wait for the pending job done */
                    if (agent->task_control)
                    {                
                        elog(LOG, POOL_MGR_PREFIX"[agent_acquire_connections] coord node:%u nodeindex:%d pid:%d last request still in progres", agent->coord_conn_oids[node], node, agent->pid);                
                        goto FATAL_ERROR;
                    }
                    
                    if (!asyncTaskCtl)
                    {
                        asyncTaskCtl = create_task_control(datanodelist, coordlist, *fd_result, *pid_result);
                    }
					
					asyncTaskCtl->m_missing_ok = !raise_error;
                    /* dispatch set param request */
                    succeed = dispatch_connection_request(asyncTaskCtl,
                                                            true,
                                                            agent,
                                                            nodePool,
                                                            PoolConnectStaus_connected, /* we already had a connection*/
                                                            PoolConnectStaus_set_param,
                                                            node,
                                                            acquire_seq,
                                                            false/* whether we are the last request */
                                                            );
                    if (!succeed)
                    {
                        goto FATAL_ERROR;
                    }

                    if (PoolPrintStatTimeout > 0)
                    {
                        g_pooler_stat.acquire_conn_from_hashtab_and_set++;
                    }
                }
                else
                {
                    if (PoolPrintStatTimeout > 0)
                    {
                        g_pooler_stat.acquire_conn_from_hashtab++;
                    }
                }
            }
        }        
        else
        {
            if (PoolConnectDebugPrint)
            {
                slot = agent->coord_connections[node];
                elog(LOG, POOL_MGR_PREFIX"[agent_acquire_connections] coordinator node:%s nodeindex:%d pid:%d already got a slot_seq:%d backend_pid:%d in agent", slot->node_name, node, agent->pid, slot->seqnum, slot->backend_pid);
            }
        }
    }
    MemoryContextSwitchTo(oldcontext);

    if (NULL == asyncTaskCtl)
    {    
#ifdef _POOLER_CHECK_    
        char **hostip = NULL;
        char **hostport = NULL;
        
        hostip = (char**)palloc0((list_length(datanodelist) + list_length(coordlist)) * sizeof(char *));
        if (hostip == NULL)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_OUT_OF_MEMORY),
                     errmsg("out of memory")));
        }

        hostport = (char**)palloc0((list_length(datanodelist) + list_length(coordlist)) * sizeof(char *));
        if (hostport == NULL)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_OUT_OF_MEMORY),
                     errmsg("out of memory")));
        }
#endif        
    
        if (PoolConnectDebugPrint)
        {
            elog(LOG, POOL_MGR_PREFIX"[agent_acquire_connections] pid:%d get all connections from hashtab", agent->pid);
        }
        
#ifdef _POOLER_CHECK_
        check_pooler_slot();
        check_hashtab_slots();
        check_duplicate_allocated_conn();
#endif
        i = 0;
        /* Save in array fds of Datanodes first */
        foreach(nodelist_item, datanodelist)
        {
            node = lfirst_int(nodelist_item);
            (*fd_result)[i] = PQsocket((PGconn *) agent->dn_connections[node]->conn);
            (*pid_result)[i] = ((PGconn *) agent->dn_connections[node]->conn)->be_pid;
#ifdef     _POOLER_CHECK_    
            hostip[i] = pstrdup(((PGconn *) agent->dn_connections[node]->conn)->pghost);
            hostport[i] = pstrdup(((PGconn *) agent->dn_connections[node]->conn)->pgport);
#endif
            connect_num++;
            i++;
        }

        /* Save then in the array fds for Coordinators */
        foreach(nodelist_item, coordlist)
        {
            node = lfirst_int(nodelist_item);
            (*fd_result)[i] = PQsocket((PGconn *) agent->coord_connections[node]->conn);
            (*pid_result)[i] = ((PGconn *) agent->coord_connections[node]->conn)->be_pid;
#ifdef     _POOLER_CHECK_    
            hostip[i] = pstrdup(((PGconn *) agent->coord_connections[node]->conn)->pghost);
            hostport[i] = pstrdup(((PGconn *) agent->coord_connections[node]->conn)->pgport);
#endif
            connect_num++;
            i++;
        }
#ifdef _POOLER_CHECK_        
        {
            int j = 0;
            for (i = 0; i < connect_num; i ++)
            {                
                for (j = 0; j < connect_num; j ++)
                {
                    if ((*pid_result)[i] == (*pid_result)[j] && 
                        i != j                                  &&
                        strcmp(hostip[i], hostip[j]) == 0     &&
                        strcmp(hostport[i], hostport[j]) == 0) 
                    {
                        abort();
                    }
                }
            }

            for (i = 0; i < connect_num; i ++)
            {
                if (hostip[i])
                {
                    pfree(hostip[i]);
                    hostip[i] = NULL;
                }
                if (hostport[i])
                {
                    pfree(hostport[i]);
                    hostport[i] = NULL;
                }
            }
            if (hostip)
            {
                pfree(hostip);
                hostip = NULL;
            }
            if (hostport)
            {
                pfree(hostport);
                hostport = NULL;
            }
        }
#endif        
        /* set the number */
        *num = connect_num;
    }
    else
    {
#ifdef _POOLER_CHECK_
        check_pooler_slot();
        check_hashtab_slots();
        check_duplicate_allocated_conn();
#endif
        /* use async thread to process the request, dispatch last request */
        if (set_request_num || acquire_failed_num)
        {
            if (PoolConnectDebugPrint)
            {
                elog(LOG, POOL_MGR_PREFIX"[agent_acquire_connections]send dispatch package. set_request_num=%d, acquire_failed_num=%d", 
                                        set_request_num, acquire_failed_num);
            }
        
            /* dispatch set param request */
            succeed = dispatch_connection_request(asyncTaskCtl,
                                                    true,
                                                    agent,
                                                    nodePool,
                                                    PoolConnectStaus_destory, /* we already had a connection*/
                                                    PoolConnectStaus_destory,
                                                    node,
                                                    acquire_seq,
                                                    true/* we are the last request */
                                                    );
        }

        
        if (!succeed)
        {
            goto FATAL_ERROR;
        }

        
        /* just return NULL*/
        if (PoolConnectDebugPrint)
        {                
            if (set_request_num)
            {
                elog(LOG, POOL_MGR_PREFIX"[agent_acquire_connections] pid:%d acquire_succeed_num:%d acquire_failed_num:%d set_request_num:%d use parallel thread to process set request", agent->pid, acquire_succeed_num, acquire_failed_num, set_request_num);
            }
            else
            {
                elog(LOG, POOL_MGR_PREFIX"[agent_acquire_connections] pid:%d can't get all connections from hashtab, acquire_succeed_num:%d acquire_failed_num:%d set_request_num:%d use parallel thread to process", agent->pid, acquire_succeed_num, acquire_failed_num, set_request_num);
            }
        }
        
        *num = 0;
        *fd_result = NULL;
        *pid_result = NULL;
    }        

    if (PoolConnectDebugPrint)
    {
        if (*fd_result == NULL)
        {
            elog(LOG, POOL_MGR_PREFIX"[agent_acquire_connections]return fd_result = NULL");
        }
        else
        {
            elog(LOG, POOL_MGR_PREFIX"[agent_acquire_connections]return fd_result NOT NULL");
        }
    }
    
    return 0;
    
FATAL_ERROR:
    /* error, just set the number */
    /* record the task control, in case of memory leak */
    if (asyncTaskCtl)
    {        
        agent->task_control = asyncTaskCtl;
    }
    elog(LOG, POOL_MGR_PREFIX"agent_acquire_connections pid:%d failed to get connections, acquire_succeed_num:%d acquire_failed_num:%d set_request_num:%d", agent->pid, acquire_succeed_num, acquire_failed_num, set_request_num);
    *num = 0;
    return -1;
}

/*
 * send transaction local commands if any, set the begin sent status in any case
 */
static int
send_local_commands(PoolAgent *agent, List *datanodelist, List *coordlist)
{// #lizard forgives
    bool                ret;
    int                    node = 0;
    int                    res  = 0;
    ListCell            *nodelist_item;
    PGXCNodePoolSlot    *slot;
    PGXCASyncTaskCtl    *asyncTaskCtl = NULL;

    /* we can get here is because we have error last time, so here just check whether we have done the job */
    if (!agent_destory_task_control(agent))
    {
        elog(LOG, POOL_MGR_PREFIX"send_local_commands last request not finish yet, pid:%d", agent->pid);
        goto FATAL_ERROR;
    }
    
    Assert(agent);    
    if (datanodelist != NULL)
    {
        if (list_length(datanodelist) > 0 && agent->dn_connections)
        {
            foreach(nodelist_item, datanodelist)
            {
                node = lfirst_int(nodelist_item);

                if(node < 0 || node >= agent->num_dn_connections)
                {
                    continue;
                }

                slot = agent->dn_connections[node];

                if (slot == NULL)
                {
                    continue;
                }

                if (agent->local_params != NULL)
                {
                    if (!asyncTaskCtl)
                    {
                        asyncTaskCtl = create_task_control(NULL, NULL, NULL, NULL);
                    }                
                    
                    ret = dispatch_local_set_request(asyncTaskCtl,
                                                       false,
                                                       agent,
                                                       node,
                                                       false);
                    if (!ret)
                    {
                        goto FATAL_ERROR;
                    }
                    res++;
                }
            }
        }
    }

    if (coordlist != NULL)
    {
        if (list_length(coordlist) > 0 && agent->coord_connections)
        {
            foreach(nodelist_item, coordlist)
            {
                node = lfirst_int(nodelist_item);

                if(node < 0 || node >= agent->num_coord_connections)
                    continue;

                slot = agent->coord_connections[node];

                if (slot == NULL)
                    continue;

                if (agent->local_params != NULL)
                {
                    if (!asyncTaskCtl)
                    {
                        asyncTaskCtl = create_task_control(NULL, NULL, NULL, NULL);
                    }
                    ret = dispatch_local_set_request(asyncTaskCtl,
                                                       true,
                                                       agent,
                                                       node,
                                                       false);
                    if (!ret)
                    {
                        goto FATAL_ERROR;
                    }
                    res++;
                }
            }
        }
    }

    /* dispatch last request */
    if (res)
    {
        ret = dispatch_local_set_request(asyncTaskCtl,
                                           true,
                                           agent,
                                           node,
                                           true);
        if (!ret)
        {
            goto FATAL_ERROR;
        }
    }
    return res;

FATAL_ERROR:
    /* record the task control, in case of memory leak */
    if (asyncTaskCtl)
    {
        agent->task_control = asyncTaskCtl;
    }
    elog(LOG, POOL_MGR_PREFIX"send_local_commands failed, pid:%d", agent->pid);
    return -1;
}

/*
 * Cancel query
 */
static int
cancel_query_on_connections(PoolAgent *agent, List *datanodelist, List *coordlist, int signal)
{// #lizard forgives
    bool               ret;
    int                  node = 0;
    ListCell          *nodelist_item;
    int                   nCount;
    PGXCASyncTaskCtl* asyncTaskCtl = NULL;
    
    nCount = 0;

    if (agent == NULL)
    {
        return nCount;
    }

    /* we can get here is because we have error last time, so here just check whether we have done the job */
    if (!agent_destory_task_control(agent))
    {
#ifdef __TBASE__
        SpinLockAcquire(&agent->port.lock);
        agent->port.error_code = POOL_ERR_CANCEL_TASK_NOT_DONE;
        snprintf(agent->port.err_msg, POOL_ERR_MSG_LEN, "%s", poolErrorMsg[agent->port.error_code]);
        SpinLockRelease(&agent->port.lock);
#endif
        elog(LOG, POOL_MGR_PREFIX"cancel_query_on_connections pid:%d last request not finish yet", agent->pid);
        goto FATAL_ERROR;
    }
    
    /* Send cancel on Datanodes first */
    foreach(nodelist_item, datanodelist)
    {
        node = lfirst_int(nodelist_item);

        if(node < 0 || node >= agent->num_dn_connections)
        {
            continue;
        }

        if (agent->dn_connections == NULL)
        {
            break;
        }

        if(agent->dn_connections[node])
        {            
            nCount++;
            if (!asyncTaskCtl)
            {
                asyncTaskCtl = create_task_control(NULL, NULL, NULL, NULL);
            }
            ret = dispatch_cancle_request(asyncTaskCtl,
                                            false,
                                            agent,
                                            node,
                                            false,
                                            signal);
            if (!ret)
            {
                goto FATAL_ERROR;
            }

        }
        else
        {
            
#ifdef _POOLER_CHECK_    
            elog(LOG, POOL_MGR_PREFIX"pid:%d dispatch async CANCLE_QUERY datanode nodeindex:%d connection failed, no such connection", 
                                                                                                            agent->pid, node);
#endif
        }
    }

    /* Send cancel to Coordinators too, e.g. if DDL was in progress */
    foreach(nodelist_item, coordlist)
    {
        node = lfirst_int(nodelist_item);

        if(node < 0 || node >= agent->num_coord_connections)
        {
            continue;
        }

        if (agent->coord_connections == NULL)
        {
            break;
        }

        if(agent->coord_connections[node])
        {
            nCount++;
            if (!asyncTaskCtl)
            {
                asyncTaskCtl = create_task_control(NULL, NULL, NULL, NULL);
            }
            ret = dispatch_cancle_request(asyncTaskCtl,
                                            true,
                                            agent,
                                            node,
                                            false,
                                            signal);
            if (!ret)
            {
                goto FATAL_ERROR;
            }
        }
        else
        {
            
#ifdef _POOLER_CHECK_    
            elog(LOG, POOL_MGR_PREFIX"pid:%d dispatch async CANCLE_QUERY coordinator nodeindex:%d connection failed, no such connection", 
                                                                                                            agent->pid, node);
#endif
        }
    }

    if (nCount)
    {
        ret = dispatch_cancle_request(asyncTaskCtl,
                                        true,
                                        agent,
                                        0,
                                        true,
                                        signal);
        if (!ret)
        {
            goto FATAL_ERROR;
        }
    }
    return nCount;
    
FATAL_ERROR:
    /* record the task control, in case of memory leak */
    if (asyncTaskCtl)
    {
        agent->task_control = asyncTaskCtl;
    }
    elog(LOG, POOL_MGR_PREFIX"cancel_query_on_connections failed pid:%d ", agent->pid);
    return -1;
}

/*
 * Return connections back to the pool
 */
void
PoolManagerReleaseConnections(bool force)
{
    char msgtype = 'r';
    int n32;
    int msglen = 8;

    /* If disconnected from pooler all the connections already released */
    if (!poolHandle)
    {
        return;
    }

    elog(DEBUG1, "Returning connections back to the pool");

    /* Message type */
    pool_putbytes(&poolHandle->port, &msgtype, 1);

    /* Message length */
    n32 = htonl(msglen);
    pool_putbytes(&poolHandle->port, (char *) &n32, 4);

    /* Lock information */
    n32 = htonl((int) force);
    pool_putbytes(&poolHandle->port, (char *) &n32, 4);
    pool_flush(&poolHandle->port);
}

/*
 * Cancel Query
 */
bool
PoolManagerCancelQuery(int dn_count, int* dn_list, int co_count, int* co_list, int signal)
{// #lizard forgives
    uint32        n32 = 0;
    int32       res = 0;
    /*
     * Buffer contains the list of both Coordinator and Datanodes, as well
     * as the number of connections
     */
    uint32         buf[2 + dn_count + co_count + 1];
    int         i;

    if (NULL == poolHandle)    
    {    
        PoolManagerReconnect();    
        /* After reconnect, recheck the handle. */
        if (NULL == poolHandle)
        {
            elog(LOG, POOL_MGR_PREFIX"PoolManagerCancelQuery poolHandle is NULL");
            return false;
        }
    }

    if (dn_count == 0 && co_count == 0)
    {
        elog(LOG, POOL_MGR_PREFIX"PoolManagerCancelQuery no node speicfied");
        return true;
    }

    if (dn_count != 0 && dn_list == NULL)
    {
        elog(LOG, POOL_MGR_PREFIX"PoolManagerCancelQuery invalid dn_count:%d, null dn_list", dn_count);
        return false;
    }

    if (co_count != 0 && co_list == NULL)
    {
        elog(LOG, POOL_MGR_PREFIX"PoolManagerCancelQuery invalid co_count:%d, null co_list", co_count);
        return false;
    }

    if (signal > SIGNAL_SIGUSR2)
    {
        elog(LOG, POOL_MGR_PREFIX"PoolManagerCancelQuery invalid signal:%d", signal);
        return false;
    }

    /* Insert the list of Datanodes in buffer */
    n32 = htonl((uint32) dn_count);
    buf[0] = n32;

    for (i = 0; i < dn_count;)
    {
        n32 = htonl((uint32) dn_list[i++]);
        buf[i] = n32;
    }

    /* Insert the list of Coordinators in buffer */
    n32 = htonl((uint32) co_count);
    buf[dn_count + 1] = n32;

    /* Not necessary to send to pooler a request if there is no Coordinator */
    if (co_count != 0)
    {
        for (i = dn_count + 1; i < (dn_count + co_count + 1);)
        {
            n32 = htonl((uint32) co_list[i - (dn_count + 1)]);
            buf[++i] = n32;
        }
    }

    n32 = htonl((uint32) signal);
    buf[2 + dn_count + co_count] = n32;
    
    pool_putmessage(&poolHandle->port, 'h', (char *) buf, (2 + dn_count + co_count + 1) * sizeof(uint32));
    pool_flush(&poolHandle->port);

	res = pool_recvres(&poolHandle->port, false);

    if (res != (dn_count + co_count))
    {
        /* Set to LOG to avoid regress test failure. */
        elog(LOG, POOL_MGR_PREFIX"cancel query on remote nodes required:%d return:%d", dn_count + co_count, res);
    }
    return res == (dn_count + co_count);
}

/*
 * Release connections for Datanodes and Coordinators
 */
static void
agent_release_connections(PoolAgent *agent, bool force_destroy, bool sync)
{
    MemoryContext oldcontext;
    int              i;
    
    /* increase query count */
    agent->query_count++;
    if (!agent->dn_connections && !agent->coord_connections)
    {
        return;
    }

    /*
     * If there are some session parameters or temporary objects,
     * do not put back connections to pool.
     * Disconnection will be made when session is cut for this user.
     * Local parameters are reset when transaction block is finished,
     * so don't do anything for them, but just reset their list.
     */
    if (agent->local_params)
    {
        pfree(agent->local_params);
        agent->local_params = NULL;
    }

    if (((agent->session_params) || agent->is_temp) && !force_destroy)
    {
        if (PoolConnectDebugPrint)
        {
            elog(LOG, POOL_MGR_PREFIX"++++agent_release_connections agent will hold conn. pid:%d session_params:%s is_temp:%d ++++",
                agent->pid, agent->session_params, agent->is_temp);
        }
        return;
    }    

    agent_destory_task_control(agent);
    
    if (PoolConnectDebugPrint)
    {
        elog(LOG, POOL_MGR_PREFIX"++++agent_release_connections pid:%d begin++++", agent->pid);
    }
    /*
     * There are possible memory allocations in the core pooler, we want
     * these allocations in the content of the database pool
     */
    oldcontext = MemoryContextSwitchTo(agent->pool->mcxt);

    /*
     * Remaining connections are assumed to be clean.
     * First clean up for Datanodes
     */
    for (i = 0; i < agent->num_dn_connections; i++)
    {
        PGXCNodePoolSlot *slot = agent->dn_connections[i];

        /*
         * Release connection.
         * If connection has temporary objects on it, destroy connection slot.
         */
        if (slot)
        {
            if (PoolConnectDebugPrint)
            {
                elog(LOG, POOL_MGR_PREFIX"++++agent_release_connections pid:%d release slot_seq:%d nodename:%s backend_pid:%d++++", agent->pid, slot->seqnum, slot->node_name, slot->backend_pid);
            }
			release_connection(agent->pool, slot, i, agent->dn_conn_oids[i], force_destroy, false, sync);
        }
        agent->dn_connections[i] = NULL;
    }
    
    /* Then clean up for Coordinator connections */
    for (i = 0; i < agent->num_coord_connections; i++)
    {
        PGXCNodePoolSlot *slot = agent->coord_connections[i];

        /*
         * Release connection.
         * If connection has temporary objects on it, destroy connection slot.
         */
        if (slot)
        {
            if (PoolConnectDebugPrint)
            {
                elog(LOG, POOL_MGR_PREFIX"++++agent_release_connections pid:%d release slot_seq:%d nodename:%s backend_pid:%d++++", agent->pid, slot->seqnum, slot->node_name, slot->backend_pid);
            }
			release_connection(agent->pool, slot, i, agent->coord_conn_oids[i], force_destroy, true, sync);
        }
        agent->coord_connections[i] = NULL;
    }

    if (!force_destroy && agent->pool->oldest_idle == (time_t) 0)
    {
        agent->pool->oldest_idle = time(NULL);
    }
    
    MemoryContextSwitchTo(oldcontext);
    
#ifdef _POOLER_CHECK_
    check_hashtab_slots();
    check_pooler_slot();
    check_duplicate_allocated_conn();
#endif
    
    if (PoolConnectDebugPrint)
    {
        elog(LOG, POOL_MGR_PREFIX"++++agent_release_connections done++++, pid:%d", agent->pid);
    }
}


/*
 * Return connections for Datanodes and Coordinators to node pool.
 * Here we have refreshed the connections, it is OK to reuse them.
 */
static void
agent_return_connections(PoolAgent *agent)
{// #lizard forgives
    MemoryContext oldcontext;
    int              i;
    
    /* increase query count */
    agent->query_count++;
    
    if (!agent->dn_connections && !agent->coord_connections)
    {
        return;
    }

    agent_destory_task_control(agent);
    
    if (PoolConnectDebugPrint)
    {
        elog(LOG, POOL_MGR_PREFIX"++++agent_return_connections pid:%d begin++++", agent->pid);
    }
    /*
     * There are possible memory allocations in the core pooler, we want
     * these allocations in the content of the database pool
     */
    oldcontext = MemoryContextSwitchTo(agent->pool->mcxt);

    /*
     * Remaining connections are assumed to be clean.
     * First clean up for Datanodes
     */
    for (i = 0; i < agent->num_dn_connections; i++)
    {
        PGXCNodePoolSlot *slot = agent->dn_connections[i];

        /*
         * Release connection.
         * If connection has temporary objects on it, destroy connection slot.
         */
        if (slot)
        {
            if (PoolConnectDebugPrint)
            {
                elog(LOG, POOL_MGR_PREFIX"++++agent_return_connections pid:%d release slot_seq:%d++++", agent->pid, slot->seqnum);
            }
			release_connection(agent->pool, slot, i, agent->dn_conn_oids[i], false, false, false);
        }
        agent->dn_connections[i] = NULL;
    }
    
    /* Then clean up for Coordinator connections */
    for (i = 0; i < agent->num_coord_connections; i++)
    {
        PGXCNodePoolSlot *slot = agent->coord_connections[i];

        /*
         * Release connection.
         * If connection has temporary objects on it, destroy connection slot.
         */
        if (slot)
        {
            if (PoolConnectDebugPrint)
            {
                elog(LOG, POOL_MGR_PREFIX"++++agent_return_connections pid:%d release slot_seq:%d++++", agent->pid, slot->seqnum);
            }
			release_connection(agent->pool, slot, i, agent->coord_conn_oids[i], false, true, false);
        }
        agent->coord_connections[i] = NULL;
    }

    if (agent->pool->oldest_idle == (time_t) 0)
    {
        agent->pool->oldest_idle = time(NULL);
    }
    
    MemoryContextSwitchTo(oldcontext);
    
    if (PoolConnectDebugPrint)
    {
        elog(LOG, POOL_MGR_PREFIX"++++agent_return_connections done++++, pid:%d", agent->pid);
    }
}

/*
 * Reset session parameters for given connections in the agent.
 * This is done before putting back to pool connections that have been
 * modified by session parameters.
 */
static bool
agent_reset_session(PoolAgent *agent)
{// #lizard forgives
    int                  i = 0;
    bool              release;
    bool              ret;
    bool              bsync       = true;
    int32             count          = 0;
    PGXCASyncTaskCtl *taskControl = NULL;

    
    if (!agent->session_params && !agent->local_params)
    {
        return bsync;
    }

    bsync = false;
    
    if (PoolConnectDebugPrint)
    {
        elog(LOG, POOL_MGR_PREFIX"++++agent_reset_session begin to async reset session pid:%d++++", agent->pid);
    }

    /* if we have task control and failed to free it, we just release the connections rather than reset them */
    release = !agent_destory_task_control(agent);
    if (!release)
    {
        taskControl = create_task_control(NULL, NULL, NULL, NULL);
    }
    
    /* Reset connection params */
    /* Check agent slot for each Datanode */
    if (agent->dn_connections)
    {
        for (i = 0; i < agent->num_dn_connections; i++)
        {
            PGXCNodePoolSlot *slot = agent->dn_connections[i];

            /* Reset given slot with parameters */
            if (slot)
            {
                if (release)
                {
                    if (PoolConnectDebugPrint)
                    {
                        elog(LOG, POOL_MGR_PREFIX"++++agent_reset_session pid:%d release slot_seq:%d++++", agent->pid, slot->seqnum);
                    }
					release_connection(agent->pool, slot, i, agent->dn_conn_oids[i], false, false, false);
                    agent->dn_connections[i] = NULL;

                }
                else
                {
                    if (PoolConnectDebugPrint)
                    {
                        elog(LOG, POOL_MGR_PREFIX"++++agent_reset_session async reset pid:%d slot_seq:%d++++", agent->pid, slot->seqnum);
                    }
                    ret = dispatch_reset_request(taskControl,
                                                false,
                                                agent,
                                                PoolResetStatus_reset, /* PoolResetStatus */
                                                i,
                                                false);        
                    if (ret)
                    {
                        count++;
                    }
                }
            }
        }
    }

    if (agent->coord_connections)
    {
        /* Check agent slot for each Coordinator */
        for (i = 0; i < agent->num_coord_connections; i++)
        {
            PGXCNodePoolSlot *slot = agent->coord_connections[i];

            /* Reset given slot with parameters */
            if (slot)
            {
                if (release)
                {
                    if (PoolConnectDebugPrint)
                    {
                        elog(LOG, POOL_MGR_PREFIX"++++agent_reset_session pid:%d release slot_seq:%d++++", agent->pid, slot->seqnum);
                    }
                    agent->coord_connections[i] = NULL;
					release_connection(agent->pool, slot, i, agent->coord_conn_oids[i], false, false, false);

                }
                else
                {
                    if (PoolConnectDebugPrint)
                    {
                        elog(LOG, POOL_MGR_PREFIX"++++agent_reset_session async reset pid:%d slot_seq:%d++++", agent->pid, slot->seqnum);
                    }
                    ret = dispatch_reset_request(taskControl,
                                                    true,
                                                    agent,
                                                    PoolResetStatus_reset, /* PoolResetStatus */
                                                    i,
                                                    false);
                    if (ret)
                    {
                        count++;
                    }
                }
            }
        }
    }

    if (count)
    {
        if (!release)
        {
            /* we have async reset connections, so we have to handle them */
            if (count)
            {
                /* dispatch a fake request to finish all reset */
                ret = dispatch_reset_request(taskControl,
                                                true,
                                                agent,
                                                PoolResetStatus_destory,    
                                                i,
                                                true);
                /* here we failed to send the last request, we have to make sure agent and task control will be properly handled */
                if (!ret)
                {
                    bsync = false;
                    agent->task_control = taskControl;
                    
                    /* set pend destory flag */
                    agent_pend_destory(agent);
                }
                else
                {
                    bsync = false;
                }
            }
            else
            {
                bsync = true;
            }
        }
    }
    else if (taskControl)
    {
        pfree((void*)taskControl);
        bsync = true;
    }
    
    if (PoolConnectDebugPrint)
    {
        elog(LOG, POOL_MGR_PREFIX"++++agent_reset_session finish async reset session pid:%d dispatch count:%d bsync:%d++++", agent->pid, count, bsync);
    }
    
    return bsync;
}
/*
 * Create new empty pool for a database.
 * By default Database Pools have a size null so as to avoid interactions
 * between PGXC nodes in the cluster (Co/Co, Dn/Dn and Co/Dn).
 * Pool is increased at the first GET_CONNECTION message received.
 * Returns POOL_OK if operation succeed POOL_FAIL in case of OutOfMemory
 * error and POOL_WEXIST if poll for this database already exist.
 */
static DatabasePool *
create_database_pool(const char *database, const char *user_name, const char *pgoptions)
{
#define     NODE_POOL_NAME_LEN    256
    bool             need_pool = true;
    MemoryContext    oldcontext;
    MemoryContext    dbcontext;
    DatabasePool   *databasePool;
    HASHCTL            hinfo;
    int                hflags;
    char            hash_name[NODE_POOL_NAME_LEN];

    dbcontext = AllocSetContextCreate(PoolerCoreContext,
                                      "DB Context",
                                      ALLOCSET_DEFAULT_MINSIZE,
                                      ALLOCSET_DEFAULT_INITSIZE,
                                      ALLOCSET_DEFAULT_MAXSIZE);
    oldcontext = MemoryContextSwitchTo(dbcontext);
    /* Allocate memory */
    databasePool = (DatabasePool *) palloc(sizeof(DatabasePool));
    if (!databasePool)
    {
        /* out of memory */
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory")));
        return NULL;
    }

    databasePool->mcxt = dbcontext;
     /* Copy the database name */
    databasePool->database = pstrdup(database);
     /* Copy the user name */
    databasePool->user_name = pstrdup(user_name);
     /* Copy the pgoptions */
    databasePool->pgoptions = pstrdup(pgoptions);

    /* Reset the oldest_idle value */
    databasePool->oldest_idle = (time_t) 0;
    
    if (!databasePool->database)
    {
        /* out of memory */
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory")));
        pfree(databasePool);
        return NULL;
    }

    /* Init next reference */
    databasePool->next = NULL;

    /* Init node hashtable */
    MemSet(&hinfo, 0, sizeof(hinfo));
    hflags = 0;

    hinfo.keysize = sizeof(Oid);
    hinfo.entrysize = sizeof(PGXCNodePool);
    hflags |= HASH_ELEM;

    hinfo.hcxt = dbcontext;
    hflags |= HASH_CONTEXT;    

    hinfo.hash = oid_hash;
    hflags |= HASH_FUNCTION;

    snprintf(hash_name, NODE_POOL_NAME_LEN, "%s_%s_Node_Pool", database, user_name);
    databasePool->nodePools = hash_create(hash_name, TBASE_MAX_DATANODE_NUMBER + TBASE_MAX_COORDINATOR_NUMBER,
                                          &hinfo, hflags);

    MemoryContextSwitchTo(oldcontext);

    /* Insert into the list */
    insert_database_pool(databasePool);
    need_pool = connection_need_pool(g_unpooled_database, database);
    if (need_pool)
    {
        need_pool =  connection_need_pool(g_unpooled_user, user_name);
    }
    
    /* Init no need for warm */
    databasePool->bneed_warm      = false;
    databasePool->bneed_precreate = false;
    databasePool->bneed_pool       = need_pool;
    databasePool->version         = 0;
    return databasePool;
}


/*
 * Insert new database pool to the list
 */
static void
insert_database_pool(DatabasePool *databasePool)
{
    Assert(databasePool);

    /* Reference existing list or null the tail */
    if (databasePools)
        databasePool->next = databasePools;
    else
        databasePool->next = NULL;

    /* Update head pointer */
    databasePools = databasePool;
}

/*
 * Rebuild information of database pools
 */
static void
reload_database_pools(PoolAgent *agent)
{// #lizard forgives
    bool          bsucceed = false;
    DatabasePool *databasePool;
    
    /*
     * Release node connections if any held. It is not guaranteed client session
     * does the same so don't ever try to return them to pool and reuse
     */
	agent_release_connections(agent, true, false);

    /* before destory nodepool, just wait for all async task is done */
    bsucceed = pooler_wait_for_async_task_done();
    if (!bsucceed)
    {
        elog(WARNING, POOL_MGR_PREFIX"async task not finish before reload all database pools");
    }
    
    /* Forget previously allocated node info */
    MemoryContextReset(agent->mcxt);

    /* and allocate new */
    PgxcNodeGetOids(&agent->coord_conn_oids, &agent->dn_conn_oids,
                    &agent->num_coord_connections, &agent->num_dn_connections, false);

    agent->coord_connections = (PGXCNodePoolSlot **)
            palloc0(agent->num_coord_connections * sizeof(PGXCNodePoolSlot *));
    agent->dn_connections = (PGXCNodePoolSlot **)
            palloc0(agent->num_dn_connections * sizeof(PGXCNodePoolSlot *));

    /*
     * Scan the list and destroy any altered pool. They will be recreated
     * upon subsequent connection acquisition.
     */
    databasePool = databasePools;
    while (databasePool)
    {
        /* Update each database pool slot with new connection information */
        HASH_SEQ_STATUS hseq_status;
        PGXCNodePool   *nodePool;

        hash_seq_init(&hseq_status, databasePool->nodePools);
        while ((nodePool = (PGXCNodePool *) hash_seq_search(&hseq_status)))
        {
            char *connstr_chk = build_node_conn_str(nodePool->nodeoid, databasePool);

            if (connstr_chk == NULL || strcmp(connstr_chk, nodePool->connstr))
            {
                /* Node has been removed or altered */
#ifdef __TBASE__
				if (nodePool->size == nodePool->freeSize || connstr_chk == NULL)
#else
                if (nodePool->size == nodePool->freeSize)
#endif
                {
					elog(LOG, POOL_MGR_PREFIX"nodePool:%s has been changed, "
						"size:%d, freeSize:%d, reload_database_pools: nodePools "
						"of node (%u, %s) is removed.",
						nodePool->connstr,
						nodePool->size, nodePool->freeSize,
						nodePool->nodeoid, nodePool->node_name);
                    destroy_node_pool(nodePool);
                    hash_search(databasePool->nodePools, &nodePool->nodeoid,
                                HASH_REMOVE, NULL);
                }
                else
                {
                    destroy_node_pool_free_slots(nodePool);

                    /* increase the node pool version */
					nodePool->m_version = databasePool->version++;
					elog(LOG, POOL_MGR_PREFIX"nodePool:%s has been changed, "
						"size:%d, freeSize:%d, reload_database_pools: nodePools "
						"of node (%u, %s) has increased version "INT64_FORMAT,
						nodePool->connstr,
						nodePool->size, nodePool->freeSize,
						nodePool->nodeoid, nodePool->node_name, nodePool->m_version);

                    /* fresh the connect string so that new coming connection will connect to the new node  */
                    if (connstr_chk)
                    {
                        if (nodePool->connstr)
                        {
                            pfree(nodePool->connstr);
                        }
                        nodePool->connstr = pstrdup(connstr_chk);
                    }
                }
            }

            if (connstr_chk)
            {
                pfree(connstr_chk);
            }
        }

        databasePool = databasePool->next;
    }

    refresh_node_map();
}


/*
 * Find pool for specified database and username in the list
 */
static DatabasePool *
find_database_pool(const char *database, const char *user_name, const char *pgoptions)
{
    DatabasePool *databasePool;

    /* Scan the list */
    databasePool = databasePools;
    while (databasePool)
    {
        if (strcmp(database, databasePool->database) == 0 &&
            strcmp(user_name, databasePool->user_name) == 0 &&
            strcmp(pgoptions, databasePool->pgoptions) == 0)
            break;

        databasePool = databasePool->next;
    }
    return databasePool;
}

/*
 * Acquire connection
 */
static PGXCNodePoolSlot *
acquire_connection(DatabasePool *dbPool, PGXCNodePool **pool,int32 nodeidx, Oid node, bool bCoord)
{// #lizard forgives
    int32              fd;
    int32              loop = 0;
    PGXCNodePool       *nodePool;
    PGXCNodePoolSlot   *slot;

    Assert(dbPool);

    nodePool = (PGXCNodePool *) hash_search(dbPool->nodePools, &node, HASH_FIND,
                                            NULL);

    /*
     * When a Coordinator pool is initialized by a Coordinator Postmaster,
     * it has a NULL size and is below minimum size that is 1
     * This is to avoid problems of connections between Coordinators
     * when creating or dropping Databases.
     */
    if (nodePool == NULL || nodePool->freeSize == 0)
    {
        if (PoolConnectDebugPrint)
        {        
            if (nodePool)
            {
                elog(LOG, POOL_MGR_PREFIX"node:%u no free connection, nodeindex:%d, size:%d, freeSize:%d begin grow in async mode", nodePool->nodeoid, nodeidx, nodePool->size, nodePool->freeSize);
            }
        }
        /* here, we try to build entry of the hash table */
        nodePool = grow_pool(dbPool, nodeidx, node, bCoord);        
    }


    if (PoolConnectDebugPrint)
    {    
        elog(LOG, POOL_MGR_PREFIX"node:%u nodeidx:%d size:%d, freeSize:%d", nodePool->nodeoid, nodeidx, nodePool->size, nodePool->freeSize);
    }
    /* get the nodepool */
    *pool = nodePool;
         
    slot = NULL;
    /* Check available connections */
    while (nodePool && nodePool->freeSize > 0)
    {
        int            poll_result;

        loop++;
        DecreasePoolerFreesize(nodePool,__FILE__,__LINE__);
        slot = nodePool->slot[nodePool->freeSize];
        nodePool->slot[nodePool->freeSize] = NULL;

        if (PoolConnectDebugPrint)
        {
            elog(LOG, POOL_MGR_PREFIX"acquire_connection alloc a connection to node:%s backend_pid:%d nodeidx:%d nodepool size:%d freeSize:%d", nodePool->node_name, slot->backend_pid, nodeidx, nodePool->size, nodePool->freeSize);
        }
        
retry:
        /* only pick up the connections that matches the latest version */
        if (slot->m_version == nodePool->m_version)
        {
            fd = PQsocket((PGconn *) slot->conn);
            if (fd > 0)
            {
                /*
                 * Make sure connection is ok, destroy connection slot if there is a
                 * problem.
                 */
                poll_result = pqReadReady((PGconn *) slot->conn);

                if (poll_result == 0)
                {
                    /* increase use count */
                    slot->usecount++;
                    break;         /* ok, no data */
                }
                else if (poll_result < 0)
                {
                    if (errno == EAGAIN || errno == EINTR)
                    {
                        errno = 0;
                        goto retry;
                     }


                    elog(WARNING, POOL_MGR_PREFIX"Error in checking connection, errno = %d", errno);
                }
                else
                {
                    elog(WARNING, POOL_MGR_PREFIX"Unexpected data on connection, cleaning.");
                }
            }
            else
            {
                elog(WARNING, POOL_MGR_PREFIX"connection to node %u contains invalid fd:%d", node, fd);
            }
        }
		destroy_slot(nodeidx, node, slot, false);
        slot = NULL;

        /* Decrement current max pool size */
        DecreasePoolerSize(nodePool,__FILE__, __LINE__);
        
        /* Ensure we are not below minimum size */
        if (0 == nodePool->freeSize)
        {
            break;
        }
    }

    if (PoolConnectDebugPrint)
    {
        if (NULL == slot)
        {
            if (0 == loop)
            {            
                ereport(WARNING,
                        (errcode(ERRCODE_CONNECTION_FAILURE),
                         errmsg(POOL_MGR_PREFIX"no more free connection to node:%u nodeidx:%d", node, nodeidx)));
            }
            else
            {
                ereport(WARNING,
                        (errcode(ERRCODE_CONNECTION_FAILURE),
                         errmsg(POOL_MGR_PREFIX"no valid free connection to node:%u nodeidx:%d", node, nodeidx)));
            }
        }
    }

    if (slot)
    {
        PgxcNodeUpdateHealth(node, true);
    }
    
    /* prebuild connection before next acquire */
    nodePool = grow_pool(dbPool, nodeidx, node, bCoord);
    IncreaseSlotRefCount(slot,__FILE__,__LINE__);
    return slot;
}


/*
 * release connection from specified pool and slot
 */
static void
release_connection(DatabasePool *dbPool, PGXCNodePoolSlot *slot,
				   int32 nodeidx, Oid node, bool force_destroy, bool bCoord, bool sync)
{
    PGXCNodePool *nodePool;
    time_t        now;

    Assert(dbPool);
    Assert(slot);

    nodePool = (PGXCNodePool *) hash_search(dbPool->nodePools, &node, HASH_FIND,
                                            NULL);
	/*
	 * The node pool of connections may has been created just now and the size is
	 * initialized to 0. This situation needs to be excluded.
	 */
	if ((nodePool == NULL) || (nodePool->size == 0))
    {
        /*
         * The node may be altered or dropped.
         * In any case the slot is no longer valid.
         */
        if (PoolConnectDebugPrint)
        {
			elog(LOG,
				POOL_MGR_PREFIX"release_connection connection to "
				"database:%s user:%s "
				"node:%s backend_pid:%d nodeidx:%d "
				"size:%d freeSize:%d can not find nodepool, just destory it",
				dbPool->database, dbPool->user_name,
				nodePool->node_name, slot->backend_pid, nodeidx,
				nodePool->size, nodePool->freeSize);
        }
		destroy_slot(nodeidx, node, slot, sync);
        return;
    }

    if (PoolConnectDebugPrint)
    {
		elog(LOG,
			POOL_MGR_PREFIX"release_connection connection to "
			"database:%s user:%s "
			"nodename:%s backend_pid:%d nodeidx:%d "
			"size:%d freeSize:%d begin to release",
			dbPool->database, dbPool->user_name,
			nodePool->node_name, slot->backend_pid, nodeidx,
			nodePool->size, nodePool->freeSize);
    }

    /* force destroy the connection when pool not enabled */
    if (!force_destroy && !dbPool->bneed_pool)
    {
        force_destroy = true;
    }
    
    /* destory the slot of former nodePool */
    if (slot->m_version != nodePool->m_version)
    {
        if (PoolConnectDebugPrint)
        {
			elog(LOG,
				POOL_MGR_PREFIX"release_connection connection to "
				"database:%s user:%s "
				"node:%s backend_pid:%d nodeidx:%d agentCount:%d "
				"size:%d freeSize:%d node version:%lu slot version:%lu not match",
				dbPool->database, dbPool->user_name,
				nodePool->node_name, slot->backend_pid, nodeidx, agentCount,
				nodePool->size, nodePool->freeSize, nodePool->m_version, slot->m_version);
        }
		destroy_slot(nodeidx, node, slot, sync);
		return;
    }
    
    if (!force_destroy)
    {
        now = time(NULL);
        if (dbPool->bneed_warm)
        {
            /* warm a connection is a hard job, when release them, we need make sure it has worked long enough. */
            if (slot->bwarmed)
            {
				if (nodePool->freeSize > MinFreeSize || difftime(now, slot->created) > PoolConnMaxLifetime)
                {
                    force_destroy = true;
                    if (PoolConnectDebugPrint)
                    {
						elog(LOG,
							POOL_MGR_PREFIX"warmed connection to "
							"database:%s user:%s "
							"node:%s backend_pid:%d nodeidx:%d lifetime expired, "
							"closed it, size:%d freeSize:%d",
							dbPool->database, dbPool->user_name,
							nodePool->node_name, slot->backend_pid, nodeidx,
							nodePool->size, nodePool->freeSize);
                    }
                }                
            }
            else 
            {
                if (((nodePool->freeSize > 0) && (nodePool->nwarming + nodePool->nquery) > MinFreeSize) ||                                         
					(difftime(now, slot->created) >= PoolConnMaxLifetime))
                {
                    force_destroy = true;
                    if (PoolConnectDebugPrint)
                    {
						elog(LOG,
							POOL_MGR_PREFIX"unwarmed connection to "
							"database:%s user:%s "
							"node:%s backend_pid:%d nodeidx:%d lifetime expired, "
							"closed it, size:%d freeSize:%d",
							dbPool->database, dbPool->user_name,
							nodePool->node_name, slot->backend_pid, nodeidx,
							nodePool->size, nodePool->freeSize);
                    }
                }
            }
        }    
        else if (((nodePool->freeSize > 0) && (nodePool->nwarming + nodePool->nquery) > MinFreeSize) ||                                         
			(difftime(now, slot->created) >= PoolConnMaxLifetime))
        {
            force_destroy = true;
            if (PoolConnectDebugPrint)
            {
				elog(LOG,
					POOL_MGR_PREFIX"connection to "
					"database:%s user:%s "
					"node:%s backend_pid:%d nodeidx:%d lifetime expired, "
					"closed it, size:%d freeSize:%d",
					dbPool->database, dbPool->user_name,
					nodePool->node_name, slot->backend_pid, nodeidx,
					nodePool->size, nodePool->freeSize);
            }
        }
    }
    
    /* return or discard */
    if (!force_destroy)
    {
        /* add the unwarmed slot to async thread */
        if (dbPool->bneed_warm && !nodePool->coord && !slot->bwarmed && !IS_ASYNC_PIPE_FULL() && 0 == nodePool->nwarming)
        {
            DecreaseSlotRefCount(slot,__FILE__,__LINE__);
            slot->released = now;
            pooler_async_warm_connection(dbPool, slot, nodePool, node);
            grow_pool(dbPool, nodeidx, node, bCoord);
        }
        else
        {        
            if ((difftime(now, slot->checked) >=  PoolSizeCheckGap) && !IS_ASYNC_PIPE_FULL())
            {                
                /* increase the warm connection count */
                DecreaseSlotRefCount(slot,__FILE__,__LINE__);
                nodePool->nquery++;
                pooler_async_query_connection(dbPool, slot, nodeidx, node);
                grow_pool(dbPool, nodeidx, node, bCoord);
            }
            else
            {
                /* Insert the slot into the array and increase pool free size */
                DecreaseSlotRefCount(slot,__FILE__,__LINE__);
                slot->pid = -1;
                nodePool->slot[nodePool->freeSize] = slot;    
                IncreasePoolerFreesize(nodePool,__FILE__,__LINE__);
                slot->released = now;
                if (PoolConnectDebugPrint)
                {
					elog(LOG,
						POOL_MGR_PREFIX"release_connection return connection to "
						"database:%s user:%s "
						"node:%s backend_pid:%d nodeidx:%d "
						"nodepool size:%d freeSize:%d",
						dbPool->database, dbPool->user_name,
						nodePool->node_name, slot->backend_pid, nodeidx,
						nodePool->size, nodePool->freeSize);
                }
            }
        }    
    }
    else
    {
        elog(DEBUG1, POOL_MGR_PREFIX"Cleaning up connection from pool %s, closing", nodePool->connstr);
        if (PoolConnectDebugPrint)
        {
			elog(LOG,
				POOL_MGR_PREFIX"release_connection destory connection to "
				"database:%s user:%s "
				"node:%s backend_pid:%d nodeidx:%d "
				"nodepool size:%d freeSize:%d",
				dbPool->database, dbPool->user_name,
				nodePool->node_name, slot->backend_pid, nodeidx,
				nodePool->size, nodePool->freeSize);
        }
		destroy_slot(nodeidx, node, slot, sync);
        
        /* Decrease pool size */
        DecreasePoolerSize(nodePool,__FILE__, __LINE__);
        
        /* Ensure we are not below minimum size, here we don't need sync build */            
        /* only grow pool when pool needed. */
        if (dbPool->bneed_pool)
        {
            grow_pool(dbPool, nodeidx, node, bCoord);
        }        
    }

	if (!slot->bdestoryed && difftime(time(NULL), slot->created) > PoolConnMaxLifetime)
	{
		elog(WARNING,
			POOL_MGR_PREFIX"release_connection has not destoried connection to "
			"database:%s user:%s "
			"node:%s backend_pid:%d nodeidx:%d "
			"nodepool size:%d freeSize:%d",
			dbPool->database, dbPool->user_name,
			nodePool->node_name, slot->backend_pid, nodeidx,
			nodePool->size, nodePool->freeSize);
	}
}

/*
 * Increase database pool size, create new if does not exist
 */
static PGXCNodePool *
grow_pool(DatabasePool *dbPool, int32 nodeidx, Oid node, bool bCoord)
{// #lizard forgives
    PGXCNodePool   *nodePool;
    bool            found;    

    Assert(dbPool);
    
    nodePool = (PGXCNodePool *) hash_search(dbPool->nodePools, &node,
                                            HASH_ENTER, &found);    
    
    if (!found)
    {
        char              *name_str = NULL;
        MemoryContext     oldcontext;
        oldcontext = MemoryContextSwitchTo(PoolerMemoryContext);
        
        nodePool->connstr = build_node_conn_str(node, dbPool);
        if (!nodePool->connstr)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg(POOL_MGR_PREFIX"could not build connection string for node %u", node)));
        }

        
        nodePool->slot = (PGXCNodePoolSlot **) palloc0(MaxPoolSize * sizeof(PGXCNodePoolSlot *));
        if (!nodePool->slot)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_OUT_OF_MEMORY),
                     errmsg(POOL_MGR_PREFIX"out of memory")));
        }
        nodePool->freeSize   = 0;
        nodePool->size       = 0;
        nodePool->coord      = bCoord;        
        nodePool->nwarming   = 0;
        nodePool->nquery     = 0;

        name_str = get_node_name_by_nodeoid(node);
        if (NULL == name_str)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg(POOL_MGR_PREFIX"get node %u name failed", node)));
        }
        snprintf(nodePool->node_name, NAMEDATALEN, "%s", name_str);
        MemoryContextSwitchTo(oldcontext);

		nodePool->m_version = dbPool->version++;
		elog(LOG,
			"grow_pool: nodePools of node (%u, %s) is created.",
			nodePool->nodeoid, nodePool->node_name);
    }    

    /* here, we move the connection build work to async threads */
    if (!nodePool->asyncInProgress && dbPool->bneed_pool)
    {
        /* async build connection to other nodes, at least keep 10 free connection in the pool */
        if (nodePool->size < InitPoolSize || (nodePool->freeSize < MinFreeSize && nodePool->size < MaxPoolSize))
        {
            /* total pool size CAN NOT be larger than agentCount too much, to avoid occupying idle connection slot of datanode */
            if (nodePool->size < agentCount + MinFreeSize)
            {
                int32 size        = 0;
                int32 initSize    = 0;
                int32 minFreeSize = 0;
                
                initSize    = nodePool->size < InitPoolSize ? InitPoolSize - nodePool->size : 0;
                minFreeSize = nodePool->freeSize < MinFreeSize ? MinFreeSize - nodePool->freeSize : 0;
                size        = minFreeSize > initSize ? minFreeSize : initSize;
                
                if (size)
                {
    				if (pooler_async_build_connection(dbPool, nodePool->m_version, nodeidx, node, size, nodePool->connstr, bCoord))
					{
    					nodePool->asyncInProgress = true;
					}
                }
            }
        }
    }
    
    if (nodePool->size >= MaxPoolSize)
    {
        elog(LOG, POOL_MGR_PREFIX"pool %s Size:%d exceed MaxPoolSize:%d",
             nodePool->connstr,
             nodePool->size,
             MaxPoolSize);
    }
    return nodePool;
}


/*
 * Destroy pool slot, including slot itself.
 */
static void
destroy_slot_ex(int32 nodeidx, Oid node, PGXCNodePoolSlot *slot, bool sync, char *file, int32 line)
{
    int32                  threadid = 0;
    uint64              pipeput_loops = 0;
    PGXCPoolConnectReq *connReq;
    MemoryContext        oldcontext;

    struct pg_conn*  tmp_conn = NULL;
    
    if (!slot)
    {
        return;
    }    

    if (PoolConnectDebugPrint)
    {
        /* should never happened */
        if (slot->bdestoryed)
        {
            abort();
        }
    }
    
    /* record last time destory position */
    slot->file   = file;
    slot->lineno = line;
    
    if (!slot->conn || !slot->xc_cancelConn)
    {
        elog(LOG, POOL_MGR_PREFIX"destroy_slot invalid slot status, null pointer conn:%p xc_cancelConn:%p", slot->conn, slot->xc_cancelConn);
    }

	if (sync)
    {
	    /* release now if sync */
        PQfreeCancel((PGcancel *)slot->xc_cancelConn);
        PGXCNodeClose(slot->conn);
        slot->bdestoryed = true;
        pfree(slot);
        return;
    }

    /* if no free pipe line avaliable, just do it sync */
    threadid = pooler_async_task_pick_thread(&g_PoolConnControl, nodeidx);
    if (-1 == threadid)
    {
        elog(LOG, POOL_MGR_PREFIX"destroy_slot_ex no pipeline avaliable, sync close connection node:%u nodeidx:%d usecount:%d slot_seq:%d", node, nodeidx, slot->usecount, slot->seqnum);
        PQfreeCancel((PGcancel *)slot->xc_cancelConn);    
        PGXCNodeClose(slot->conn);
        slot->bdestoryed = true;
        pfree(slot);
        return;
    }

    oldcontext = MemoryContextSwitchTo(PoolerMemoryContext);    
    connReq            = (PGXCPoolConnectReq*)palloc0(sizeof(PGXCPoolConnectReq));
    MemoryContextSwitchTo(oldcontext);
    
    connReq->cmd       = COMMAND_CONNECTION_CLOSE;
    connReq->nodeoid   = node;
    connReq->validSize = 0;
    connReq->nodeindex = nodeidx;
    connReq->slot[0].xc_cancelConn = slot->xc_cancelConn;
    connReq->slot[0].conn          = slot->conn;
    connReq->slot[0].seqnum        = slot->seqnum;
    connReq->slot[0].bdestoryed    = slot->bdestoryed;
    connReq->slot[0].file          = slot->file;
    connReq->slot[0].lineno        = slot->lineno;

    if (PoolConnectDebugPrint)
    {
        if (slot->conn)
        {
            tmp_conn = (struct pg_conn*)slot->conn;
            elog(LOG, POOL_MGR_PREFIX"destroy_slot_ex close conn remote backendid:%d remoteip:%s %s pgport:%s dbname:%s dbuser:%s", 
                    tmp_conn->be_pid, tmp_conn->pghost, tmp_conn->pghostaddr, tmp_conn->pgport, 
                    tmp_conn->dbName,
                    tmp_conn->pguser);
        }
    }

    while (-1 == PipePut(g_PoolConnControl.request[threadid % MAX_SYNC_NETWORK_THREAD], (void*)connReq))
    {
		pipeput_loops++;
    }

	if (pipeput_loops > 0)
	{
		elog(LOG, POOL_MGR_PREFIX"destroy_slot_ex fail to async close connection node:%u for loops %lu", node, pipeput_loops);
	}
    
    /* signal thread to start build job */
    ThreadSemaUp(&g_PoolConnControl.sem[threadid % MAX_SYNC_NETWORK_THREAD]);
    if (PoolConnectDebugPrint)
    {
         elog(LOG, POOL_MGR_PREFIX"destroy_slot_ex async close connection node:%u nodeidx:%d threadid:%d usecount:%d slot_seq:%d", node, nodeidx, threadid, slot->usecount, slot->seqnum);    
    }

    /* set destroy flag */
    slot->bdestoryed = true;
    pfree(slot);
}


/*
 * Close pool slot, don't free the slot itself.
 */
static void
close_slot(int32 nodeidx, Oid node, PGXCNodePoolSlot *slot)
{// #lizard forgives
    int32 threadid;
    uint64 pipeput_loops = 0;
    PGXCPoolConnectReq *connReq;
    if (!slot)
    {
        return;
    }

    if (PoolConnectDebugPrint)
    {
        if (slot->bdestoryed)
        {
            abort();
        }
    }
    
    if (!slot->conn || !slot->xc_cancelConn)
    {
        elog(LOG, POOL_MGR_PREFIX"close_slot invalid slot status, null pointer conn:%p xc_cancelConn:%p", slot->conn, slot->xc_cancelConn);
    }

    /* if no free pipe line avaliable, just do it sync */
    threadid = pooler_async_task_pick_thread(&g_PoolConnControl, nodeidx);
    if (-1 == threadid)
    {
        elog(LOG, POOL_MGR_PREFIX"no pipeline avaliable, sync close connection node:%u nodeidx:%d usecount:%d slot_seq:%d", node, nodeidx, slot->usecount, slot->seqnum);    
        PQfreeCancel((PGcancel *)slot->xc_cancelConn);    
        PGXCNodeClose(slot->conn);
        return;
    }

    
    connReq            = (PGXCPoolConnectReq*)palloc0(sizeof(PGXCPoolConnectReq));
    connReq->cmd       = COMMAND_CONNECTION_CLOSE;
    connReq->nodeoid   = node;
    connReq->validSize = 0;
    connReq->slot[0].xc_cancelConn = slot->xc_cancelConn;
    connReq->slot[0].conn          = slot->conn;
    while (-1 == PipePut(g_PoolConnControl.request[threadid % MAX_SYNC_NETWORK_THREAD], (void*)connReq))
    {
		pipeput_loops++;	
    }

	if (pipeput_loops > 0)
	{
		elog(LOG, POOL_MGR_PREFIX"fail to async close connection node:%u for loops %lu", node, pipeput_loops);
	}
    
    /* signal thread to start build job */
    ThreadSemaUp(&g_PoolConnControl.sem[threadid % MAX_SYNC_NETWORK_THREAD]);
    if (PoolConnectDebugPrint)
    {
        elog(LOG, POOL_MGR_PREFIX"async close connection node:%u nodeidx:%d threadid:%d usecount:%d slot_seq:%d", node, nodeidx, threadid, slot->usecount, slot->seqnum);    
    }
    /* set destroy flag */
    slot->bdestoryed = true;
}


/*
 * Destroy node pool
 */
static void
destroy_node_pool(PGXCNodePool *node_pool)
{
    int            i;

    if (!node_pool)
    {
        return;
    }

    /*
     * At this point all agents using connections from this pool should be already closed
     * If this not the connections to the Datanodes assigned to them remain open, this will
     * consume Datanode resources.
     */
    elog(DEBUG1, POOL_MGR_PREFIX"About to destroy node pool %s, current size is %d, %d connections are in use",
         node_pool->connstr, node_pool->freeSize, node_pool->size - node_pool->freeSize);
    if (node_pool->connstr)
    {
        pfree(node_pool->connstr);
    }

    if (node_pool->slot)
    {
        int32 nodeidx;
        nodeidx = get_node_index_by_nodeoid(node_pool->nodeoid);
        for (i = 0; i < node_pool->freeSize; i++)
        {
			destroy_slot(nodeidx, node_pool->nodeoid, node_pool->slot[i], false);
        }
        pfree(node_pool->slot);
		node_pool->size -= node_pool->freeSize;
		node_pool->freeSize = 0;
    }
}

/*
 * Destroy free slot of the node pool
 */
static void
destroy_node_pool_free_slots(PGXCNodePool *node_pool)
{
    int            i;

    if (!node_pool)
    {
        return;
    }

    if (PoolConnectDebugPrint)
    {
		elog(LOG,
			POOL_MGR_PREFIX"About to destroy slots of node pool %s, node_pool version:"INT64_FORMAT
			"agentCount is %d current size is %d, freeSize is %d, %d connections are in use",
			node_pool->connstr, node_pool->m_version,
			agentCount, node_pool->size, node_pool->freeSize, node_pool->size - node_pool->freeSize);
    }

    if (node_pool->slot)
    {
        int32 nodeidx;
        nodeidx = get_node_index_by_nodeoid(node_pool->nodeoid);
        for (i = 0; i < node_pool->freeSize; i++)
        {            
			destroy_slot(nodeidx, node_pool->nodeoid, node_pool->slot[i], false);
            node_pool->slot[i] = NULL;
        }
        node_pool->size     -= node_pool->freeSize;        
		node_pool->freeSize = 0;
    }
}

/*
 * setup current log time
 */
static void
setup_formatted_current_log_time(char* formatted_current_log_time)
{
    pg_time_t	stamp_time;
    char		msbuf[13];
    struct timeval timeval;

    gettimeofday(&timeval, NULL);
    stamp_time = (pg_time_t) timeval.tv_sec;

    /*
     * Note: we expect that guc.c will ensure that log_timezone is set up (at
     * least with a minimal GMT value) before Log_line_prefix can become
     * nonempty or CSV mode can be selected.
     */
    pg_strftime(formatted_current_log_time, FORMATTED_TS_LEN,
            /* leave room for milliseconds... */
                "%Y-%m-%d %H:%M:%S     %Z",
                pg_localtime(&stamp_time, log_timezone));

    /* 'paste' milliseconds into place... */
    sprintf(msbuf, ".%03d", (int) (timeval.tv_usec / 1000));
    memcpy(formatted_current_log_time + 19, msbuf, 4);
}

/*
 * write pooler's subthread log into thread log queue
 * only call by pooler's subthread in elog
 */
static void
pooler_subthread_write_log(int elevel, int lineno, const char *filename, const char *funcname, const char *fmt, ...)
{
    char *buf = NULL;
    int buf_len = 0;
    int offset = 0;
    char formatted_current_log_time[FORMATTED_TS_LEN];

    if (!PoolSubThreadLogPrint)
    {
        /* not enable sun thread log print, return */
        return;
    }

    if (PipeIsFull(g_ThreadLogQueue))
    {
        return;
    }

    /* use malloc in sub thread */
    buf_len = strlen(filename) + strlen(funcname) + DEFAULT_LOG_BUF_LEN;
    buf = (char*)malloc(buf_len);
    if (buf == NULL)
    {
        /* no log */
        return;
    }

    /* construction log, format: elevel | lineno | filename | funcname | log content */
    *(int*)(buf + offset) = elevel;
    offset += sizeof(elevel);
    *(int*)(buf + offset) = lineno;
    offset += sizeof(lineno);
    memcpy(buf + offset, filename, strlen(filename) + 1);
    offset += (strlen(filename) + 1);
    memcpy(buf + offset, funcname, strlen(funcname) + 1);
    offset += (strlen(funcname) + 1);

    /*
     * because the main thread writes the log of the sub thread asynchronously,
     * record the actual log writing time here
     */
    setup_formatted_current_log_time(formatted_current_log_time);
    memcpy(buf + offset, formatted_current_log_time, strlen(formatted_current_log_time));
    offset += strlen(formatted_current_log_time);
    *(char*)(buf + offset) = ' ';
    offset += sizeof(char);

    /* Generate actual output --- have to use appendStringInfoVA */
    for (;;)
    {
        va_list		args;
        int			avail;
        int			nprinted;

        avail = buf_len - offset - 1;
        va_start(args, fmt);
        nprinted = vsnprintf(buf + offset, avail, fmt, args);
        va_end(args);
        if (nprinted >= 0 && nprinted < avail - 1)
        {
            offset += nprinted;
            *(char*)(buf + offset) = '\0';
			offset += sizeof(char);
            break;
        }

        buf_len = (buf_len * 2 > (int) MaxAllocSize) ? MaxAllocSize : buf_len * 2;
        buf = (char *) realloc(buf, buf_len);
        if (buf == NULL)
        {
            /* no log */
            return;
        }
    }

    /* put log into thread log queue, drop log if queue is full */
    if (-1 == PipePut(g_ThreadLogQueue, buf))
    {
        free(buf);
    }
}

/*
 * write subthread log in main thread
 */
static void
pooler_handle_subthread_log(bool is_pooler_exit)
{
    int write_log_cnt = 0;
    int offset = 0;
    int elevel = LOG;
    int lineno = 0;
    char *log_buf = NULL;
    char *filename = NULL;
    char *funcname = NULL;
    char *log_content = NULL;

    while ((log_buf = (char*)PipeGet(g_ThreadLogQueue)) != NULL)
    {
        /* elevel | lineno | filename | funcname | log content */
        elevel = *(int*)log_buf;
        offset = sizeof(elevel);
        lineno = *(int*)(log_buf + offset);
        offset += sizeof(lineno);
        filename = log_buf + offset;
        offset += (strlen(filename) + 1);
        funcname = log_buf + offset;
        offset += (strlen(funcname) + 1);
        log_content = log_buf + offset;

        /* write log here */
        elog_start(filename, lineno,
#ifdef USE_MODULE_MSGIDS
                PGXL_MSG_MODULE, PGXL_MSG_FILEID, __COUNTER__,
#endif
                funcname);
        elog_finish(elevel, "%s", log_content);

        free(log_buf);

        /*
         * if the number of logs written at one time exceeds POOLER_WRITE_LOG_ONCE_LIMIT,
         * in order not to block the main thread, return here
         */
        if (write_log_cnt++ >= POOLER_WRITE_LOG_ONCE_LIMIT && !is_pooler_exit)
        {
            return;
        }
    }
}

/*
 * Main handling loop
 */
static void
PoolerLoop(void)
{
	bool           warm_inited = false;
    StringInfoData input_message;
    int            maxfd       = MaxConnections + 1024;
    struct pollfd *pool_fd;
    int            i;
    int            ret;
    time_t           last_maintenance = (time_t) 0;
    int               timeout_val      = 0;
    PGXCPoolConnThreadParam     connParam[MAX_SYNC_NETWORK_THREAD];    
    
#ifdef HAVE_UNIX_SOCKETS
    if (Unix_socket_directories)
    {
        char       *rawstring;
        List       *elemlist;
        ListCell   *l;
        int         success = 0;

        /* Need a modifiable copy of Unix_socket_directories */
        rawstring = pstrdup(Unix_socket_directories);

        /* Parse string into list of directories */
        if (!SplitDirectoriesString(rawstring, ',', &elemlist))
        {
            /* syntax error in list */
            ereport(FATAL,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("invalid list syntax in parameter \"%s\"",
                            "unix_socket_directories")));
        }

        foreach(l, elemlist)
        {
            char       *socketdir = (char *) lfirst(l);
            int         saved_errno;

            /* Connect to the pooler */
            server_fd = pool_listen(PoolerPort, socketdir);
            if (server_fd < 0)
            {
                saved_errno = errno;
                ereport(WARNING,
                        (errmsg("could not create Unix-domain socket in directory \"%s\", errno %d, server_fd %d",
                                socketdir, saved_errno, server_fd)));
            }
            else
            {
                success++;
            }
        }

        if (!success && elemlist != NIL)
            ereport(ERROR,
                    (errmsg("failed to start listening on Unix-domain socket for pooler: %m")));

        list_free_deep(elemlist);
        pfree(rawstring);
    }
#endif

    /* create log queue */
    g_ThreadLogQueue = CreatePipe(MAX_THREAD_LOG_PIPE_LEN);

    /* create utility thread */
    g_AsynUtilityPipeSender = CreatePipe(POOL_ASYN_WARM_PIPE_LEN);
    ThreadSemaInit(&g_AsnyUtilitysem, 0);
    g_AsynUtilityPipeRcver  = CreatePipe(POOL_ASYN_WARM_PIPE_LEN);
    ret = CreateThread(pooler_async_utility_thread, NULL, MT_THR_DETACHED);
    if (ret)
    {
        elog(ERROR, POOL_MGR_PREFIX"Pooler:create utility thread failed");    
    }

    /* Create concurrent connection pipes for build batch connection request and close connection request */
    pooler_init_sync_control(&g_PoolConnControl);
    for (i = 0; i < MAX_SYNC_NETWORK_THREAD; i++)
    {
        g_PoolConnControl.request[i]  = CreatePipe(MAX_SYNC_NETWORK_PIPE_LEN);
        g_PoolConnControl.response[i] = CreatePipe(MAX_SYNC_NETWORK_PIPE_LEN);
        ThreadSemaInit(&g_PoolConnControl.sem[i], 0);
        connParam[i].threadIndex = i;
        ret = CreateThread(pooler_async_connection_management_thread, (void*)&connParam[i], MT_THR_DETACHED);
        if (ret)
        {
            elog(ERROR, POOL_MGR_PREFIX"Pooler:create connection manage thread failed");    
        }
    }    


    /* Create sync network operation pipes for remote nodes */
    pooler_init_sync_control(&g_PoolSyncNetworkControl);
    for (i = 0; i < MAX_SYNC_NETWORK_THREAD; i++)
    {
        g_PoolSyncNetworkControl.request[i]  = CreatePipe(MAX_SYNC_NETWORK_PIPE_LEN);
        g_PoolSyncNetworkControl.response[i] = CreatePipe(MAX_SYNC_NETWORK_PIPE_LEN);
        ThreadSemaInit(&g_PoolSyncNetworkControl.sem[i], 0);
        connParam[i].threadIndex = i;
        ret = CreateThread(pooler_sync_remote_operator_thread, (void*)&connParam[i], MT_THR_DETACHED);
        if (ret)
        {
            elog(ERROR, POOL_MGR_PREFIX"Pooler:create connection manage thread failed");    
        }
    }    
    
    pool_fd = (struct pollfd *) palloc(maxfd * sizeof(struct pollfd));
    
    if (server_fd == -1)
    {
        /* log error */
        return;
    }

    elog(LOG, POOL_MGR_PREFIX"PoolerLoop begin server_fd:%d", server_fd);
    
    initStringInfo(&input_message);

    pool_fd[0].fd = server_fd;
    pool_fd[0].events = POLLIN; //POLLRDNORM;
    
    for (i = 1; i < maxfd; i++)
    {
        pool_fd[i].fd = -1;
        pool_fd[i].events = POLLIN | POLLPRI | POLLRDNORM | POLLRDBAND;
    }

	reset_pooler_statistics();
    init_pooler_cmd_statistics();

    for (;;)
    {
        int retval;
        int i;

        if (got_SIGHUP)
        {
            got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
        }
        
        /*
         * Emergency bailout if postmaster has died.  This is to avoid the
         * necessity for manual cleanup of all postmaster children.
         */
        if (!PostmasterIsAlive())
        {
            pooler_handle_subthread_log(true);
            exit(1);
        }
        
        /* watch for incoming messages */
        RebuildAgentIndex();
        
        for (i = 0; i < agentCount; i++)
        {
            int32      index = 0;
            int        sockfd;
            PoolAgent *agent = NULL;
            
            index  = agentIndexes[i];
            agent  = poolAgents[index];
            
            /* skip the agents in async deconstruct progress */
            sockfd = Socket(agent->port);
            pool_fd[i + 1].fd = sockfd;
            
        }

        if (shutdown_requested)
        {
            /* 
             *  Just close the socket and exit. Linux will help to release the resouces.
              */        
            close(server_fd);
            pooler_handle_subthread_log(true);
            exit(0);
        }        

        if (PoolMaintenanceTimeout > 0)
        {            
            double            timediff;

            /*
             * Decide the timeout value based on when the last
             * maintenance activity was carried out. If the last
             * maintenance was done quite a while ago schedule the select
             * with no timeout. It will serve any incoming activity
             * and if there's none it will cause the maintenance
             * to be scheduled as soon as possible
             */
            timediff = difftime(time(NULL), last_maintenance);
            if (timediff > PoolMaintenanceTimeout)
            {
                timeout_val = 0;
            }
            else
            {
                timeout_val = PoolMaintenanceTimeout - rint(timediff);
            }
    
            /* wait for event */
            retval = poll(pool_fd, agentCount + 1, timeout_val * 1000);
        }
        else
        {
            retval = poll(pool_fd, agentCount + 1, -1);
        }        
        
        if (retval < 0)
        {

            if (errno == EINTR)
                continue;
            elog(FATAL, POOL_MGR_PREFIX"poll returned with error %d", retval);

        }
        
        /* add warmed connection to node pool before agent acquiring new connection */
        pooler_sync_connections_to_nodepool();
        pooler_handle_sync_response_queue();
        if (retval > 0)
        {
            /*
             * Agent may be removed from the array while processing
             * and trailing items are shifted, so scroll downward
             * to avoid problem
             */
            int32      index;
            PoolAgent *agent;
            int        sockfd;
            for (i = agentCount - 1; i >= 0; i--)
            {
                index  = agentIndexes[i];
                agent  = poolAgents[index];            
                
                sockfd = Socket(agent->port);
                if ((sockfd == pool_fd[i + 1].fd) && pool_fd[i + 1].revents)
                {
                    agent_handle_input(agent, &input_message);
                }
                
            }

            if (pool_fd[0].revents & POLLIN)
            {
                int new_fd = accept(server_fd, NULL, NULL);

                if (new_fd < 0)
                {
                    int saved_errno = errno;
                    ereport(LOG,
                            (errcode(ERRCODE_CONNECTION_FAILURE), errmsg(POOL_MGR_PREFIX"Pooler manager failed to accept connection: %m")));
                    errno = saved_errno;
                }
                else
                {
                    agent_create(new_fd);
                }
            }
        }

        /* maintaince time out */
        if (0 == timeout_val && PoolMaintenanceTimeout > 0)
        {
            /* maintain the connection pool */
            pools_maintenance();
            PoolAsyncPingNodes();
            last_maintenance = time(NULL);
        }

        /* create preload database pooler */
		if (!warm_inited)
        {
            connect_pools();
			warm_inited = true;
        }
        pooler_pools_warm();
        
#ifdef _POOLER_CHECK_
        RebuildAgentIndex();

        check_pooler_slot();
        check_hashtab_slots();
        check_duplicate_allocated_conn();
#endif
        print_pooler_statistics();

		/* handle sub thread's log */
        pooler_handle_subthread_log(false);
    }
}


/*
 * Clean Connection in all Database Pools for given Datanode and Coordinator list
 */
int
clean_connection(List *node_discard, const char *database, const char *user_name)
{// #lizard forgives
    DatabasePool *databasePool;
    int            res = CLEAN_CONNECTION_COMPLETED;

    databasePool = databasePools;

    while (databasePool)
    {
        ListCell *lc;

        if ((database && strcmp(database, databasePool->database)) ||
                (user_name && strcmp(user_name, databasePool->user_name)))
        {
            /* The pool does not match to request, skip */
            databasePool = databasePool->next;
            continue;
        }

        /*
         * Clean each requested node pool
         */
        foreach(lc, node_discard)
        {
            PGXCNodePool *nodePool;
            Oid node = lfirst_oid(lc);

            nodePool = hash_search(databasePool->nodePools, &node, HASH_FIND,
                                   NULL);

            if (nodePool)
            {
                /* Check if connections are in use */
                if (nodePool->freeSize < nodePool->size)
                {
                    elog(WARNING, POOL_MGR_PREFIX"Pool of Database %s is using Datanode %u connections, freeSize:%d, size:%d",
                                databasePool->database, node, nodePool->freeSize, nodePool->size);
                    res = CLEAN_CONNECTION_NOT_COMPLETED;
                }

                /* Destroy connections currently in Node Pool */
                if (nodePool->slot)
                {
                    int i;
                    int32 nodeidx;
                    nodeidx = get_node_index_by_nodeoid(nodePool->nodeoid);
                    for (i = 0; i < nodePool->freeSize; i++)
                    {
						destroy_slot(nodeidx, nodePool->nodeoid, nodePool->slot[i], false);
                        nodePool->slot[i] = NULL;
                    }
                }
                nodePool->size -= nodePool->freeSize;
                nodePool->freeSize = 0;
            }
        }

        databasePool = databasePool->next;
    }

    /* Release lock on Pooler, to allow transactions to connect again. */
    is_pool_locked = false;
    return res;
}

/*
 * Take a Lock on Pooler.
 * Abort PIDs registered with the agents for the given database.
 * Send back to client list of PIDs signaled to watch them.
 */
int *
abort_pids(int *len, int pid, const char *database, const char *user_name)
{// #lizard forgives
    int   *pids = NULL;
    int   i = 0;
    int   count;
    int32 index;

    Assert(!is_pool_locked);
    Assert(agentCount > 0);

    is_pool_locked = true;

    RebuildAgentIndex();
    
    pids = (int *) palloc((agentCount - 1) * sizeof(int));

    /* Send a SIGTERM signal to all processes of Pooler agents except this one */
    for (count = 0; count < agentCount; count++)
    {
        index = agentIndexes[count];
        if (poolAgents[index]->pid == pid)
            continue;

        if (database && strcmp(poolAgents[index]->pool->database, database) != 0)
            continue;

        if (user_name && strcmp(poolAgents[index]->pool->user_name, user_name) != 0)
            continue;

        if (kill(poolAgents[index]->pid, SIGTERM) < 0)
            elog(ERROR, POOL_MGR_PREFIX"kill(%ld,%d) failed: %m",
                        (long) poolAgents[index]->pid, SIGTERM);

        pids[i++] = poolAgents[index]->pid;
    }

    *len = i;

    return pids;
}

/*
 *
 */
static void
pooler_die(SIGNAL_ARGS)
{
    shutdown_requested = true;
}


static void
pooler_sig_hup_handler(SIGNAL_ARGS)
{
    got_SIGHUP = true;
}

/*
 *
 */
static void
pooler_quickdie(SIGNAL_ARGS)
{
    PG_SETMASK(&BlockSig);
    exit(2);
}

bool
IsPoolHandle(void)
{
    return poolHandle != NULL;
}


/*
 * Given node identifier, dbname and user name build connection string.
 * Get node connection details from the shared memory node table
 */
static char *
build_node_conn_str(Oid node, DatabasePool *dbPool)
{
    NodeDefinition *nodeDef;
    char            *connstr;

    nodeDef = PgxcNodeGetDefinition(node);
    if (nodeDef == NULL)
    {
        /* No such definition, node is dropped? */
        return NULL;
    }

    connstr = PGXCNodeConnStr(NameStr(nodeDef->nodehost),
                              nodeDef->nodeport,
                              dbPool->database,
                              dbPool->user_name,
                              dbPool->pgoptions,
                              IS_PGXC_COORDINATOR ? "coordinator" : "datanode",
                              PGXCNodeName);
    pfree(nodeDef);

    return connstr;
}
/*
 * Check all pooled connections, and close which have been released more then
 * PooledConnKeepAlive seconds ago.
 * Return true if shrink operation closed all the connections and pool can be
 * ddestroyed, false if there are still connections or pool is in use.
 */
static bool
shrink_pool(DatabasePool *pool)
{// #lizard forgives
    int32           freeCount = 0;
    time_t             now = time(NULL);
    HASH_SEQ_STATUS hseq_status;
    PGXCNodePool   *nodePool;
    int             i;
    int32             nodeidx;
    bool            empty = true;

    /* Negative PooledConnKeepAlive disables automatic connection cleanup */
    if (PoolConnKeepAlive < 0)
    {
        return false;
    }
    
    pool->oldest_idle = (time_t) 0;
    hash_seq_init(&hseq_status, pool->nodePools);
    while ((nodePool = (PGXCNodePool *) hash_seq_search(&hseq_status)))
    {
        /* 
         *    Go thru the free slots and destroy those that are free too long, free a long free connection.
         *  Use MAX_FREE_CONNECTION_NUM to control the loop number.
        */
        freeCount = 0;
        nodeidx = get_node_index_by_nodeoid(nodePool->nodeoid);
        for (i = 0; i < nodePool->freeSize && freeCount < MAX_FREE_CONNECTION_NUM && nodePool->size >= MinPoolSize && nodePool->freeSize >= MinFreeSize; )
        {
            PGXCNodePoolSlot *slot = nodePool->slot[i];
            if (slot)
            {
                /* no need to shrik warmed slot, only discard them when they use too much memroy */
                if (!slot->bwarmed && ((difftime(now, slot->released) > PoolConnKeepAlive) || 
                                      (difftime(now, slot->created) >= PoolConnMaxLifetime)))
                {                    
                    if (PoolConnectDebugPrint)
                    {
						elog(LOG,
							POOL_MGR_PREFIX"shrink_pool destroy a connection to "
							"database:%s user:%s "
							"node:%s backend_pid:%d nodeidx:%d "
							"nodepool size:%d freeSize:%d",
							pool->database, pool->user_name,
							nodePool->node_name, slot->backend_pid, nodeidx,
							nodePool->size, nodePool->freeSize);
                    }
                    /* connection is idle for long, close it */                    
					destroy_slot(nodeidx, nodePool->nodeoid, slot, false);
                    
                    /* reduce pool size and total number of connections */
                    DecreasePoolerFreesize(nodePool,__FILE__,__LINE__);
                    DecreasePoolerSize(nodePool,__FILE__, __LINE__);
                    
                    /* move last connection in place, if not at last already */
                    if (i < nodePool->freeSize)
                    {
                        nodePool->slot[i]                  = nodePool->slot[nodePool->freeSize];
                        nodePool->slot[nodePool->freeSize] = NULL;
                    }
                    else if (i == nodePool->freeSize)
                    {
                        nodePool->slot[nodePool->freeSize] = NULL;
                    }
                    freeCount++;
                }
                else
                {
                    if (pool->oldest_idle == (time_t) 0 ||
                            difftime(pool->oldest_idle, slot->released) > 0)
                    {
                        pool->oldest_idle = slot->released;
                    }
                    i++;
                }
            }
            else
            {
                elog(WARNING, POOL_MGR_PREFIX"invalid NULL index %d node:%u, poolsize:%d, freeSize:%d", i,
                                                                                          nodePool->nodeoid, 
                                                                                          nodePool->size, 
                                                                                          nodePool->freeSize);    
                DecreasePoolerFreesize(nodePool,__FILE__,__LINE__);
                DecreasePoolerSize(nodePool,__FILE__, __LINE__);
                if (i < nodePool->freeSize)
                {
                    nodePool->slot[i] = nodePool->slot[nodePool->freeSize];
                    nodePool->slot[nodePool->freeSize] = NULL;
                }
                else if (i == nodePool->freeSize)
                {
                    nodePool->slot[nodePool->freeSize] = NULL;
                }
            }
        }

        if (freeCount)
        {
            if (PoolConnectDebugPrint)
            {
				elog(LOG,
					POOL_MGR_PREFIX"close %d long time free connections of "
						"database:%s user:%s "
						"node:%u, poolsize:%d, freeSize:%d",
						freeCount,
						pool->database, pool->user_name,
						nodePool->nodeoid, nodePool->size, nodePool->freeSize);
            }

            /* only grow pool when pool needed. */
            if (pool->bneed_pool)
            {
                grow_pool(pool, nodeidx, nodePool->nodeoid, nodePool->coord);
            }
        }
        
		if (PoolConnectDebugPrint)
		{
			for (i = 0; i < nodePool->freeSize; i++)
			{
				PGXCNodePoolSlot *slot = nodePool->slot[i];

				if (slot && !slot->bdestoryed && difftime(time(NULL), slot->created) > PoolConnMaxLifetime)
				{
					elog(WARNING,
						POOL_MGR_PREFIX"shrink_pool found connection to "
						"database:%s user:%s "
						"nodename:%s nodeid:%d "
						"nodepool size:%d freeSize:%d that should be destoried.",
						pool->database, pool->user_name,
						nodePool->node_name, nodePool->nodeoid,
						nodePool->size, nodePool->freeSize);

					break;
				}
			}
		}

        if (nodePool->size > 0)
        {
            empty = false;
        }
        else
        {
			if (PoolConnectDebugPrint)
			{
				elog(LOG,
					"shrink_pool: nodePools of node (%u, %s) is removed.",
					nodePool->nodeoid, nodePool->node_name);
			}
            destroy_node_pool(nodePool);
            hash_search(pool->nodePools, &nodePool->nodeoid, HASH_REMOVE, NULL);
        }
    }

    /*
     * Last check, if any active agent is referencing the pool do not allow to
     * destroy it, because there will be a problem if session wakes up and try
     * to get a connection from non existing pool.
     * If all such sessions will eventually disconnect the pool will be
     * destroyed during next maintenance procedure.
     */
    if (empty)
    {
        int32 index = 0;
        RebuildAgentIndex();
        
        for (i = 0; i < agentCount; i++)
        {
            index = agentIndexes[i];
            if (poolAgents[index]->pool == pool)
            {
                return false;
            }
        }
    }

    return empty;
}

/*
 * Scan connection pools and release connections which are idle for long.
 * If pool gets empty after releasing connections it is destroyed.
 */
static void
pools_maintenance(void)
{
    bool            bresult = false;
    //DatabasePool   *prev = NULL;
    DatabasePool   *curr = databasePools;
    time_t            now = time(NULL);
    int                count = 0;
    

    /* Iterate over the pools */
    while (curr)
    {
        /*
         * If current pool has connections to close and it is emptied after
         * shrink remove the pool and free memory.
         * Otherwithe move to next pool.
         */
        bresult = shrink_pool(curr);
        if (bresult)
        {
            curr = curr->next;
#if 0
            MemoryContext mem = curr->mcxt;
            curr = curr->next;
            if (prev)
            {
                prev->next = curr;
            }
            else
            {
                databasePools = curr;
            }
            MemoryContextDelete(mem);
            count++;
#endif
        }
        else
        {
            /* async warm the pool */
            pooler_async_warm_database_pool(curr);

            //prev = curr;
            curr = curr->next;
        }
    }
    elog(DEBUG1, POOL_MGR_PREFIX"Pool maintenance, done in %f seconds, removed %d pools",
            difftime(time(NULL), now), count);
}

/* Process async msg from async threads */
static void pooler_handle_sync_response_queue(void)
{// #lizard forgives
    int32              addcount     = 0;
    int32              threadIndex  = 0;
    PGXCPoolAsyncReq   *connRsp     = NULL;    
    PoolAgent            *agent        = NULL;
    /* sync new connection node into hash table */
    threadIndex = 0;
    while (threadIndex < MAX_SYNC_NETWORK_THREAD)
    {    
        for (addcount = 0; addcount < POOL_SYN_REQ_CONNECTION_NUM; addcount++)
        {
            connRsp = (PGXCPoolAsyncReq*)PipeGet(g_PoolSyncNetworkControl.response[threadIndex]);
            if (NULL == connRsp)
            {
                break;
            }

            /* decrease agent ref count */
            agent = connRsp->agent;
            agent_decrease_ref_count(agent);            
            switch (connRsp->cmd)
            {
                case 'g':    /* acquire connection */
                {        
                    
                    record_time(connRsp->start_time, connRsp->end_time);
                    
                    switch (get_task_status(connRsp->taskControl))
                    {
                        case PoolAyncCtlStaus_init:
                        {
                            if (PoolConnectDebugPrint)
                            {
                                elog(LOG, POOL_MGR_PREFIX"[pooler_handle_sync_response_queue] PoolAyncCtlStaus_init status match, conn msg:%s", connRsp->errmsg);
                            }
                            pfree(connRsp);
                            break;
                        }
                        case PoolAyncCtlStaus_dispatched:
                        {
                            if (PoolConnectDebugPrint)
                            {
                                elog(LOG, POOL_MGR_PREFIX"[pooler_handle_sync_response_queue] PoolAyncCtlStaus_dispatched status match, conn msg:%s", connRsp->errmsg);
                            }
                            pfree(connRsp);
                            break;
                        }    
                        case PoolAyncCtlStaus_butty:
                        {
                            if (PoolConnectDebugPrint)
                            {
                                elog(LOG, POOL_MGR_PREFIX"[pooler_handle_sync_response_queue] PoolAyncCtlStaus_butty status match, conn msg:%s", connRsp->errmsg);
                            }
                            pfree(connRsp);
                            break;
                        }    
                        case PoolAyncCtlStaus_done:
                        {                                    
                            if (connRsp->needfree)
                            {            
                                if (PoolConnectDebugPrint)
                                {
                                    elog(LOG, POOL_MGR_PREFIX"pooler_handle_sync_response_queue last acquire request node:%u nodeindex:%d pid:%d req_seq:%d finish, conn msg:%s", connRsp->nodepool->nodeoid, connRsp->nodeindex, agent->pid, connRsp->req_seq, connRsp->errmsg);
                                }
                                list_free(connRsp->taskControl->m_datanodelist);
                                list_free(connRsp->taskControl->m_coordlist);
                                pfree(connRsp->taskControl->m_result);
                                pfree(connRsp->taskControl->m_pidresult);
                                pfree(connRsp->taskControl);                                
                            }
                            else
                            {
                                if (PoolConnectDebugPrint)
                                {
                                    elog(LOG, POOL_MGR_PREFIX"pooler_handle_sync_response_queue middle acquire request node:%u nodeindex:%d pid:%d req_seq:%d finish, conn msg:%s", connRsp->nodepool->nodeoid, connRsp->nodeindex, agent->pid, connRsp->req_seq, connRsp->errmsg);
                                }                                
                            }    
                            pfree(connRsp);
                            break;
                        }

                        case PoolAyncCtlStaus_error:
                        {
                            switch (connRsp->current_status)
                            {                
                                case PoolConnectStaus_connected:
                                case PoolConnectStaus_set_param:
                                case PoolConnectStaus_done:
                                case PoolConnectStaus_destory:
                                {    
                                    if (connRsp->needfree)
                                    {            
                                        if (PoolConnectDebugPrint)
                                        {
                                            elog(LOG, POOL_MGR_PREFIX"pooler_handle_sync_response_queue controller error last request node:%u nodeindex:%d pid:%d req_seq:%d finish, conn msg:%s", connRsp->nodepool->nodeoid, connRsp->nodeindex, agent->pid, connRsp->req_seq, connRsp->errmsg);
                                        }
                            
                                        list_free(connRsp->taskControl->m_datanodelist);
                                        list_free(connRsp->taskControl->m_coordlist);
                                        pfree(connRsp->taskControl->m_result);
                                        pfree(connRsp->taskControl->m_pidresult);
                                        pfree(connRsp->taskControl);
                                        /* no need to free slot */
                                    }                
                                    else
                                    {
                                        if (PoolConnectDebugPrint)
                                        {
                                            elog(LOG, POOL_MGR_PREFIX"pooler_handle_sync_response_queue controller error middle request node:%u nodeindex:%d pid:%d req_seq:%d finish, conn msg:%s", connRsp->nodepool->nodeoid, connRsp->nodeindex, agent->pid, connRsp->req_seq, connRsp->errmsg);
                                        }                                        
                                    }
                                    pfree(connRsp);
                                    break;
                                }

                                case PoolConnectStaus_error:
                                {
                                    elog(LOG, POOL_MGR_PREFIX"pooler_handle_sync_response_queue node:%u nodeindex:%d  pid:%d parallel thread errmsg:%s req_seq:%d", connRsp->nodepool->nodeoid, connRsp->nodeindex, agent->pid, connRsp->errmsg, connRsp->req_seq);
                                    
                                    if (connRsp->needfree)
                                    {            
                                        if (PoolConnectDebugPrint)
                                        {
                                            elog(LOG, POOL_MGR_PREFIX"++++pooler_handle_sync_response_queue last acquire request node:%u nodeindex:%d  pid:%d finish with ERROR_MSG:%s req_seq:%d", connRsp->nodepool->nodeoid, connRsp->nodeindex, agent->pid, connRsp->errmsg, connRsp->req_seq);
                                        }
                                        list_free(connRsp->taskControl->m_datanodelist);
                                        list_free(connRsp->taskControl->m_coordlist);
                                        pfree(connRsp->taskControl->m_result);
                                        pfree(connRsp->taskControl->m_pidresult);
                                        pfree(connRsp->taskControl);
                                    }
                                    else
                                    {
                                        if (PoolConnectDebugPrint)
                                        {
                                            elog(LOG, POOL_MGR_PREFIX"pooler_handle_sync_response_queue middle acquire request node:%u nodeindex:%d  pid:%d finish with ERROR_MSG:%s req_seq:%d", connRsp->nodepool->nodeoid, connRsp->nodeindex, agent->pid, connRsp->errmsg, connRsp->req_seq);
                                        }
                                    }
                                    
                                    /* free the slot */
                                    elog(LOG, POOL_MGR_PREFIX"pooler_handle_sync_response_queue destory ERROR!! acquire request node:%u nodeindex:%d pid:%d finish with ERROR_MSG:%s req_seq:%d", connRsp->nodepool->nodeoid, connRsp->nodeindex, agent->pid, connRsp->errmsg, connRsp->req_seq);

                                    /* then destory the slot */
                                    if (connRsp->needConnect)
                                    {            
                                        /* failed, decrease count */
                                        DecreasePoolerSizeAsync(connRsp->nodepool, connRsp->req_seq, __FILE__,__LINE__);        
                                        if (connRsp->slot)
                                        {
                                            if (connRsp->slot->conn)
                                            {                                        
												destroy_slot(connRsp->nodeindex, connRsp->nodepool->nodeoid, connRsp->slot, false);
                                            }
                                            else
                                            {
                                                pfree(connRsp->slot);
                                            }
                                        }
                                    }            
                                    else if (connRsp->final_status != PoolConnectStaus_destory)
                                    {
                                        /* Close the slot when connect error. */
                                        Oid               nodeoid = InvalidOid;
                                        PGXCNodePoolSlot *slot    = NULL;
                                        if (connRsp->bCoord)
                                        {
                                            nodeoid =  connRsp->agent->coord_conn_oids[connRsp->nodeindex];
                                            slot    =  connRsp->agent->coord_connections[connRsp->nodeindex];
                                            connRsp->agent->coord_connections[connRsp->nodeindex] = NULL;
                                        }
                                        else
                                        {
                                            nodeoid =  connRsp->agent->dn_conn_oids[connRsp->nodeindex];
                                            slot    =  connRsp->agent->dn_connections[connRsp->nodeindex];
                                            connRsp->agent->dn_connections[connRsp->nodeindex] = NULL;
                                        }
                            
                                        elog(LOG, POOL_MGR_PREFIX"pooler_handle_sync_response_queue force to close connection for timeout node:%u bCoord:%d nodeindex:%d pid:%d when acquire", nodeoid, connRsp->bCoord, connRsp->nodeindex, agent->pid);
                                            
                                        /* Force to close the connection. */
                                        if (slot)
                                        {
											release_connection(connRsp->agent->pool, slot, connRsp->nodeindex, nodeoid, true, connRsp->bCoord, false);
                                        }
                                    }
                                    pfree(connRsp);
                                    break;
                                }
                                default:
                                {
                                    /* should never happen */
                                    abort();
                                }
                            }
                        }
                    }
                    break;
                }        
                
                case 'd':
                {
                    /* Disconnect */
                    switch (connRsp->current_status)
                    {
                        case  PoolResetStatus_reset:
                        {
                            if (PoolConnectDebugPrint)
                            {
                                elog(LOG, POOL_MGR_PREFIX"++++pooler_handle_sync_response_queue middle disconnect bCoord:%d nodeindex:%d pid:%d finish++++ ", connRsp->bCoord, connRsp->nodeindex, agent->pid);
                            }

                            /* Error occured, we close the connection now. */
                            if (connRsp->error_flag)
                            {
                                Oid               nodeOid = InvalidOid;
                                PGXCNodePoolSlot *slot    = NULL;
                                if (connRsp->bCoord)
                                {
                                    slot    = connRsp->agent->coord_connections[connRsp->nodeindex];
                                    nodeOid = connRsp->agent->coord_conn_oids[connRsp->nodeindex];
                                    connRsp->agent->coord_connections[connRsp->nodeindex] = NULL;
                                    
                                }
                                else
                                {
                                    slot = connRsp->agent->dn_connections[connRsp->nodeindex];
                                    nodeOid = connRsp->agent->dn_conn_oids[connRsp->nodeindex];
                                    connRsp->agent->dn_connections[connRsp->nodeindex] = NULL;
                                }                                
                                
                                elog(LOG, POOL_MGR_PREFIX"pooler_handle_sync_response_queue force to close connection for node:%u bCoord:%d nodeindex:%d pid:%d finish. setquery_status=%d", nodeOid, connRsp->bCoord, connRsp->nodeindex, agent->pid, connRsp->setquery_status);

                                /* Force to close the connection. */
                                if (slot)
                                {
									release_connection(connRsp->agent->pool, slot, connRsp->nodeindex, nodeOid, true, connRsp->bCoord, false);
                                }
                                else
                                {
                                    elog(LOG, POOL_MGR_PREFIX"pooler_handle_sync_response_queue no necessary to force to close connection for node:%u bCoord:%d nodeindex:%d pid:%d finish. slot already closed.", 
                                                             nodeOid, connRsp->bCoord, connRsp->nodeindex, agent->pid);
                                }
                            }
                            pfree(connRsp);
                            break;
                        }
                        case  PoolResetStatus_destory:
                        {
                            if (PoolConnectDebugPrint)
                            {
                                elog(LOG, POOL_MGR_PREFIX"++++pooler_handle_sync_response_queue last disconnect bCoord:%d nodeindex:%d pid:%d finish++++", connRsp->bCoord, connRsp->nodeindex, agent->pid);
                            }
                            
                            /* return connections to nodepool */
                            agent_return_connections(connRsp->agent);
                            
                            /* Free memory. All connection slots are NULL at this point */
                            MemoryContextDelete(connRsp->agent->mcxt);                            

                            agent_destory_task_control(connRsp->agent);
                            pfree(connRsp->agent);
                            pfree(connRsp->taskControl);
                            pfree(connRsp);
                            
                            /* agent has been destoryed, clear the pointer */
                            agent = NULL;
                            break;
                        }
                        default:
                        {
                            abort();
                        }
                    }
                    break;
                }

                case 'b':
                {
                    /* Fire transaction-block commands on given nodes */
                    switch (connRsp->current_status)
                    {                        
                        case PoolLocalSetStatus_reset:    
                        {        
                            if (PoolConnectDebugPrint)
                            {
                                elog(LOG, POOL_MGR_PREFIX"++++pooler_handle_sync_response_queue middle local set request bCoord:%d nodeindex:%d pid:%d finish++++", connRsp->bCoord, connRsp->nodeindex, agent->pid);
                            }

                            if (connRsp->error_flag)
                            {
                                elog(LOG, POOL_MGR_PREFIX"pooler_handle_sync_response_queue middle local set request bCoord:%d nodeindex:%d pid:%d ERROR!! ERROR_MSG:%s, setquery_status=%d", connRsp->bCoord, connRsp->nodeindex, agent->pid, connRsp->errmsg, connRsp->setquery_status);
                            }
                            pfree(connRsp);
                            break;
                        }
                        
                        case PoolLocalSetStatus_destory:
                        {                    
                            if (PoolConnectDebugPrint)
                            {
                                elog(LOG, POOL_MGR_PREFIX"++++pooler_handle_sync_response_queue last local set request bCoord:%d nodeindex:%d pid:%d finish++++", connRsp->bCoord, connRsp->nodeindex, agent->pid);
                            }

                            if (connRsp->error_flag)
                            {
                                elog(LOG, POOL_MGR_PREFIX"pooler_handle_sync_response_queue middle local set request bCoord:%d nodeindex:%d pid:%d ERROR!! ERROR_MSG:%s, setquery_status=%d", connRsp->bCoord, connRsp->nodeindex, agent->pid, connRsp->errmsg, connRsp->setquery_status);
                            }                            
                            
                            pfree(connRsp->taskControl);
                            pfree(connRsp);
                            break;
                        }
                        
                        default:
                        {
                            /* should never happens */
                            abort();
                        }                        
                    }
                    break;
                }

                case 'h':
                {
                    /* Cancel SQL Command in progress on specified connections */
                    switch (connRsp->current_status)
                    {                        
                        case PoolCancelStatus_cancel:    
                        case PoolCancelStatus_end_query:
                        {
                            PGXCNodePoolSlot *slot    = NULL;
                            if (connRsp->bCoord)
                            {
                                slot = connRsp->agent->coord_connections[connRsp->nodeindex];
                                
                            }
                            else
                            {
                                slot = connRsp->agent->dn_connections[connRsp->nodeindex];
                            }                                
                            
                            
                            if (PoolConnectDebugPrint)
                            {
                                if (slot)
                                {
                                    elog(LOG, POOL_MGR_PREFIX"++++pooler_handle_sync_response_queue middle CANCLE_QUERY bCoord:%d nodeindex:%d nodename:%s backend_pid:%d session_pid:%d finish++++", connRsp->bCoord, connRsp->nodeindex, slot->node_name,slot->backend_pid, agent->pid);
                                }
                            }

                            if (connRsp->error_flag)
                            {
                                if (slot)
                                {
                                    elog(LOG, POOL_MGR_PREFIX"pooler_handle_sync_response_queue CANCLE_QUERY bCoord:%d nodeindex:%d nodename:%s backend_pid:%d session_pid:%d ERROR!! ERROR_MSG:%s, setquery_status=%d", connRsp->bCoord, connRsp->nodeindex, slot->node_name,slot->backend_pid, agent->pid, connRsp->errmsg, connRsp->setquery_status);
                                }
                            }
                            pfree(connRsp);
                            break;
                        }
                        
                        case PoolCancelStatus_destory:
                        {                    
                            PGXCNodePoolSlot *slot    = NULL;
                            if (connRsp->bCoord)
                            {
                                slot = connRsp->agent->coord_connections[connRsp->nodeindex];
                                
                            }
                            else
                            {
                                slot = connRsp->agent->dn_connections[connRsp->nodeindex];
                            }
                            
                            if (PoolConnectDebugPrint)
                            {
                                if (slot)
                                {
                                    elog(LOG, POOL_MGR_PREFIX"++++pooler_handle_sync_response_queue last CANCLE_QUERY bCoord:%d nodeindex:%d nodename:%s backend_pid:%d session_pid:%d finish++++", connRsp->bCoord, connRsp->nodeindex, slot->node_name,slot->backend_pid, agent->pid);
                                }
                            }

                            if (connRsp->error_flag)
                            {
                                if (slot)
                                {
                                    elog(LOG, POOL_MGR_PREFIX"pooler_handle_sync_response_queue last CANCLE_QUERY bCoord:%d nodeindex:%d nodename:%s backend_pid:%d session_pid:%d ERROR!! ERROR_MSG:%s, setquery_status=%d", connRsp->bCoord, connRsp->nodeindex, slot->node_name,slot->backend_pid, agent->pid, connRsp->errmsg, connRsp->setquery_status);
                                }
                            }                        
                            
                            pfree(connRsp->taskControl);
                            pfree(connRsp);
                            break;
                        }
                        
                        default:
                        {
                            /* should never happens */
                            abort();
                        }    
                    }
                    break;
                }
                
                case 's':
                {
                    /* Session-related COMMAND */
                    switch (connRsp->current_status)
                    {
                        case  PoolSetCommandStatus_set:
                        {
                            Oid               nodeoid = InvalidOid;
                            PGXCNodePoolSlot *slot    = NULL;    
                            
                            if (PoolConnectDebugPrint)
                            {
                                elog(LOG, POOL_MGR_PREFIX"++++pooler_handle_sync_response_queue set command bCoord:%d nodeindex:%d pid:%d finish++++", connRsp->bCoord, connRsp->nodeindex, agent->pid);
                            }                                                        
                            
                            /* Close the slot when connect error. */                                
                            if (connRsp->bCoord)
                            {
                                nodeoid =  connRsp->agent->coord_conn_oids[connRsp->nodeindex];
                                slot    =  connRsp->agent->coord_connections[connRsp->nodeindex];
                            }
                            else
                            {
                                nodeoid =  connRsp->agent->dn_conn_oids[connRsp->nodeindex];
                                slot    =  connRsp->agent->dn_connections[connRsp->nodeindex];
                            }
                            
                            /* when set expired, close conn. If error, keep it */
                            if (connRsp->error_flag && connRsp->setquery_status == SendSetQuery_EXPIRED)
                            {
                                if (connRsp->bCoord)
                                {
                                    connRsp->agent->coord_connections[connRsp->nodeindex] = NULL;
                                }
                                else
                                {
                                    connRsp->agent->dn_connections[connRsp->nodeindex] = NULL;
                                }
                                
                                elog(LOG, POOL_MGR_PREFIX"pooler_handle_sync_response_queue middle set command request bCoord:%d nodeindex:%d node:%u pid:%d ERROR!! ERROR_MSG:%s, setquery_status=%d", connRsp->bCoord, connRsp->nodeindex, nodeoid, agent->pid, connRsp->errmsg, connRsp->setquery_status);                
                                elog(LOG, POOL_MGR_PREFIX"pooler_handle_sync_response_queue force to close connection for timeout node:%u bCoord:%d nodeindex:%d pid:%d when set", nodeoid, connRsp->bCoord, connRsp->nodeindex, agent->pid);
                                    
                                /* Force to close the connection. */
                                if (slot)
                                {
									release_connection(connRsp->agent->pool, slot, connRsp->nodeindex, nodeoid, true, connRsp->bCoord, false);
                                }
                            }
                            else if (connRsp->error_flag)
                            {
                                /* error happend when set. but we only close conn when set timeout */
                                elog(LOG, POOL_MGR_PREFIX"pooler_handle_sync_response_queue middle set command request bCoord:%d nodeindex:%d nodeoid:%u pid:%d ERROR!! ERROR_MSG:%s, setquery_status=%d", connRsp->bCoord, connRsp->nodeindex, nodeoid, agent->pid, connRsp->errmsg, connRsp->setquery_status);
                            }
                            pfree(connRsp);
                            break;
                        }
                        case  PoolSetCommandStatus_destory:
                        {
                            if (PoolConnectDebugPrint)
                            {
                                elog(LOG, POOL_MGR_PREFIX"++++pooler_handle_sync_response_queue final set command bCoord:%d nodeindex:%d pid:%d finish++++", connRsp->bCoord, connRsp->nodeindex, agent->pid);
                            }

                            if (connRsp->error_flag)
                            {
                                elog(LOG, POOL_MGR_PREFIX"pooler_handle_sync_response_queue final set command request bCoord:%d nodeindex:%d pid:%d ERROR!! ERROR_MSG:%s, setquery_status=%d", connRsp->bCoord, connRsp->nodeindex, agent->pid, connRsp->errmsg, connRsp->setquery_status);
                            }
                            
                            /* free memory */
                            pfree(connRsp->taskControl->m_command);
                            pfree(connRsp->taskControl);
                            pfree(connRsp);
                            break;
                        }
                        default:
                        {
                            abort();
                        }
                    }
                    break;
                }
                default:
                {
                    /* should never happens */
                    abort();
                }
            }

            if (connRsp->cmd_start_time != 0)
            {
                update_pooler_cmd_statistics(connRsp->cmd, connRsp->cmd_end_time - connRsp->cmd_start_time);
            }
            
            /* handle pending agent, if any */
            agent_handle_pending_agent(agent);
        }
        threadIndex++;
    }
    return;
}

static void pooler_sync_connections_to_nodepool(void)
{// #lizard forgives
    bool               found;
    int32              nodeidx     = 0;
    int32              addcount    = 0;
    int32              threadIndex = 0;
    int32              connIndex   = 0;
    
    int32              nClose       = 0;
    int32              nReset       = 0;
    int32              nWarm        = 0;
    int32               nJudge       = 0;
    
    
    PGXCNodePool       *nodePool    = NULL;
    PGXCAsyncWarmInfo  *asyncInfo   = NULL;
    PGXCPoolConnectReq *connRsp     = NULL;
    PGXCNodePoolSlot   *slot         = NULL;    
    MemoryContext     oldcontext;

    /* sync async connection command from connection manage thread */
    while (addcount < POOL_SYN_CONNECTION_NUM)
    {        
        asyncInfo = (PGXCAsyncWarmInfo*)PipeGet(g_AsynUtilityPipeRcver);
        if (NULL == asyncInfo)
        {
            break;
        }
        
        switch (asyncInfo->cmd)
        {
            case COMMAND_CONNECTION_NEED_CLOSE:
            {
                nodePool = (PGXCNodePool *) hash_search(asyncInfo->dbPool->nodePools, &asyncInfo->node,
                                    HASH_FIND, &found);
                if (!found)
                {
                    elog(LOG, POOL_MGR_PREFIX"Pooler:no entry inside the hashtable of Oid:%u", asyncInfo->node);
                }

                if (PoolConnectDebugPrint)
                {
                    elog(LOG, POOL_MGR_PREFIX"Pooler:connection of node:%u has %d MBytes, colse now", asyncInfo->node, asyncInfo->size);
                }

                if (asyncInfo->slot->bwarmed)
                {
                    elog(LOG, POOL_MGR_PREFIX"Pooler:warmed connection of node:%u has %d MBytes, colse now", asyncInfo->node, asyncInfo->size);
                }
                
                /* time to close the connection */
				destroy_slot(asyncInfo->nodeindex, asyncInfo->node, asyncInfo->slot, false);
                if (nodePool)
                {
                    /* Decrement pool size */
                    DecreasePoolerSize(nodePool,__FILE__, __LINE__);
                }
                nClose++;
            }
            break;

            case COMMAND_CONNECTION_WARM:
            case COMMAND_JUDGE_CONNECTION_MEMSIZE:                
            {    
                char  *name_str = NULL;
                
                nodePool = (PGXCNodePool *) hash_search(asyncInfo->dbPool->nodePools, &asyncInfo->node,
                                    HASH_ENTER, &found);
                if (!found)
                {
					elog(WARNING, POOL_MGR_PREFIX"The nodePool has not found when the slot is warmed up.");

                    oldcontext = MemoryContextSwitchTo(PoolerMemoryContext);
                    nodePool->connstr = build_node_conn_str(asyncInfo->node, asyncInfo->dbPool);
                    if (!nodePool->connstr)
                    {
                        ereport(ERROR,
                                (errcode(ERRCODE_INTERNAL_ERROR),
                                 errmsg(POOL_MGR_PREFIX"could not build connection string for node %u", asyncInfo->node)));
                    }
                    
                    nodePool->slot = (PGXCNodePoolSlot **) palloc0(MaxPoolSize * sizeof(PGXCNodePoolSlot *));
                    if (!nodePool->slot)
                    {
                        ereport(ERROR,
                                (errcode(ERRCODE_OUT_OF_MEMORY),
                                 errmsg(POOL_MGR_PREFIX"out of memory")));
                    }
                    MemoryContextSwitchTo(oldcontext);
                    
                    nodePool->freeSize   = 0;
                    nodePool->size         = 0;
                    nodePool->coord      = false; /* in this case, only datanode */
                    nodePool->nwarming   = 0;
                    nodePool->nquery     = 0;
					nodePool->m_version = asyncInfo->dbPool->version++;

                    name_str = get_node_name_by_nodeoid(asyncInfo->node);
                    if (NULL == name_str)
                    {
                        ereport(ERROR,
                                (errcode(ERRCODE_INTERNAL_ERROR),
                                 errmsg(POOL_MGR_PREFIX"get node %u name failed", asyncInfo->node)));
                    }
                    snprintf(nodePool->node_name, NAMEDATALEN, "%s", name_str);
                }

                /* time out when try to warm the connection, close it now */
                if (COMMAND_CONNECTION_WARM == asyncInfo->cmd && false == asyncInfo->slot->bwarmed)
                {
                    nodeidx = get_node_index_by_nodeoid(asyncInfo->node);
					destroy_slot(nodeidx, asyncInfo->node, asyncInfo->slot, false);
                    
                    /* Decrease pool size */
                    DecreasePoolerSize(nodePool,__FILE__, __LINE__);                                        
                    elog(WARNING, POOL_MGR_PREFIX"destory connection to node:%u nodeidx:%d nodepool size:%d freeSize:%d for failed warm. set_query_status=%d", asyncInfo->node, nodeidx, nodePool->size, nodePool->freeSize, asyncInfo->set_query_status);                    
                    break;
                }
                
                /* fresh the touch timestamp */
                asyncInfo->slot->released  = time(NULL);        

                /* fresh the create timestamp to stop immediate check size */
                if (COMMAND_JUDGE_CONNECTION_MEMSIZE == asyncInfo->cmd)
                {
                    asyncInfo->slot->checked = time(NULL);    
                }

                if (asyncInfo->slot->m_version == nodePool->m_version)
                {
                    /* Not Increase count of pool size, just the free size */
                    asyncInfo->slot->pid = -1;
                    nodePool->slot[nodePool->freeSize] = asyncInfo->slot;
                    IncreasePoolerFreesize(nodePool,__FILE__,__LINE__);
                    if (PoolConnectDebugPrint)
                    {
                        elog(LOG, POOL_MGR_PREFIX"pooler_sync_connections_to_nodepool return connection to node:%s backend_pid:%d nodeidx:%d nodepool size:%d freeSize:%d", nodePool->node_name, asyncInfo->slot->backend_pid, nodeidx, nodePool->size, nodePool->freeSize);
                    }
                }
                else
                {                    
                    if (PoolConnectDebugPrint)
                    {
						elog(LOG, POOL_MGR_PREFIX"destory connection to node:%u "
							"nodeidx:%d nodepool size:%d freeSize:%d for unmatch "
							"version, slot->m_version:"INT64_FORMAT", nodePool->m_version:"INT64_FORMAT,
							asyncInfo->node,
							nodeidx, nodePool->size, nodePool->freeSize,
							asyncInfo->slot->m_version, nodePool->m_version);
                    }
					nodeidx = get_node_index_by_nodeoid(asyncInfo->node);

					destroy_slot(nodeidx, asyncInfo->node, asyncInfo->slot, false);

					break;
                }
                
                if (COMMAND_CONNECTION_WARM == asyncInfo->cmd)
                {                    
                    nWarm++;
                    nodePool->nwarming --;
                    if (PoolConnectDebugPrint)
                    {
                        elog(LOG, POOL_MGR_PREFIX"async warm node:%u succeed, poolsize:%d, freeSize:%d, nwarming:%d, slot_seq:%d", nodePool->nodeoid, nodePool->size, nodePool->freeSize, nodePool->nwarming, asyncInfo->slot->seqnum);
                    }
                }
                else if (COMMAND_JUDGE_CONNECTION_MEMSIZE == asyncInfo->cmd)
                {                    
                    nJudge++;
                    nodePool->nquery --;
                    if (PoolConnectDebugPrint)
                    {
                        elog(LOG, POOL_MGR_PREFIX"async query node:%u succeed, poolsize:%d, freeSize:%d, nquery:%d, node:%u %d Mbytes, slot_seq:%d", nodePool->nodeoid, nodePool->size, nodePool->freeSize, nodePool->nquery, asyncInfo->node, asyncInfo->size, asyncInfo->slot->seqnum);
                    }
                }                

                if (nodePool->nwarming < 0 || nodePool->nquery < 0)
                {
                    elog(LOG, POOL_MGR_PREFIX"node pool:%u invalid count nwarmin:%d nquery:%d ", asyncInfo->node, nodePool->nwarming, nodePool->nquery);
                }
            }
            break;

            case COMMAND_PING_NODE:
            {
                elog(DEBUG1, "Node (%s) back online!", NameStr(asyncInfo->nodename));
                if (!PgxcNodeUpdateHealth(asyncInfo->node, true))
                    elog(WARNING, "Could not update health status of node (%s)",
                         NameStr(asyncInfo->nodename));
                else
                    elog(LOG, "Health map updated to reflect HEALTHY node (%s)",
                         NameStr(asyncInfo->nodename));
            }
            break;


            default:
            {
                elog(LOG, POOL_MGR_PREFIX"invalid async command %d", asyncInfo->cmd);
            }
            break;
        }            
        pfree((void*)asyncInfo);    
        addcount++;
    }

    if (addcount > 100)
    {
        elog(LOG, POOL_MGR_PREFIX"async connection process:Total:%d nClose:%d  nReset:%d nWarm:%d nJudge:%d", addcount, nClose, nReset, nWarm, nJudge);
    }
    
    /* sync new connection node into hash table */
    threadIndex = 0;
    while (threadIndex < MAX_SYNC_NETWORK_THREAD)
    {    
        time_t now = time(NULL);
        for (addcount = 0; addcount < POOL_SYN_REQ_CONNECTION_NUM; addcount++)
        {
            connRsp = (PGXCPoolConnectReq*)PipeGet(g_PoolConnControl.response[threadIndex]);
            if (NULL == connRsp)
            {
                break;
            }
            switch (connRsp->cmd)
            {
                case COMMAND_CONNECTION_BUILD:
                {
                    char  *name_str;
                    /* search node pool hash to find the pool */
                    nodePool = (PGXCNodePool *) hash_search(connRsp->dbPool->nodePools, &connRsp->nodeoid,
                                            HASH_ENTER, &found);

                    if (!found)
                    {
                        oldcontext = MemoryContextSwitchTo(PoolerMemoryContext);
                        nodePool->connstr = build_node_conn_str(connRsp->nodeoid, connRsp->dbPool);
                        if (!nodePool->connstr)
                        {
                            ereport(ERROR,
                                    (errcode(ERRCODE_INTERNAL_ERROR),
                                     errmsg(POOL_MGR_PREFIX"Pooler could not build connection string for node %u", connRsp->nodeoid)));
                        }
                
                        nodePool->slot = (PGXCNodePoolSlot **) palloc0(MaxPoolSize * sizeof(PGXCNodePoolSlot *));
                        if (!nodePool->slot)
                        {
                            ereport(ERROR,
                                    (errcode(ERRCODE_OUT_OF_MEMORY),
                                     errmsg(POOL_MGR_PREFIX"Pooler out of memory")));
                        }
                        MemoryContextSwitchTo(oldcontext);
                        
                        nodePool->freeSize   = 0;
                        nodePool->size         = 0;
                        nodePool->coord      = connRsp->bCoord; 
                        nodePool->nwarming   = 0;
                        nodePool->nquery     = 0;

                        name_str = get_node_name_by_nodeoid(connRsp->nodeoid);
                        if (NULL == name_str)
                        {
                            ereport(ERROR,
                                    (errcode(ERRCODE_INTERNAL_ERROR),
                                     errmsg(POOL_MGR_PREFIX"get node %u name failed", connRsp->nodeoid)));
                        }
                        snprintf(nodePool->node_name, NAMEDATALEN, "%s", name_str);
						nodePool->m_version = connRsp->dbPool->version++;
						elog(LOG,
							"pooler_sync_connections_to_nodepool: nodePools of "
							"node (%u, %s) is created.",
							nodePool->nodeoid, nodePool->node_name);
                    }

                    /* add connection to hash table */
                    for (connIndex = 0; connIndex < connRsp->validSize; connIndex++)
                    {
                        oldcontext = MemoryContextSwitchTo(PoolerMemoryContext);
                        slot = (PGXCNodePoolSlot *) palloc0(sizeof(PGXCNodePoolSlot));
                        if (slot == NULL)
                        {
                            ereport(ERROR,
                                    (errcode(ERRCODE_OUT_OF_MEMORY),
                                     errmsg(POOL_MGR_PREFIX"Pooler out of memory")));
                        }
                        MemoryContextSwitchTo(oldcontext);
                        
                        /* assign value to slot */
                        *slot = connRsp->slot[connIndex];                
                        
                        /* give the slot a seq number */
                        slot->seqnum = pooler_get_slot_seq_num();
                        slot->pid      = -1;
                        
                        /* store the connection info into node pool, if space is enough */
                        if (nodePool->size < MaxPoolSize && connRsp->m_version == nodePool->m_version)
                        {
                            slot->created   = now;
                            slot->released  = now;
                            slot->checked   = now;    
                            slot->m_version = nodePool->m_version;
                            slot->node_name = nodePool->node_name;
                            slot->backend_pid = ((PGconn *) slot->conn)->be_pid;
                            nodePool->slot[nodePool->freeSize] = slot;                            
                            IncreasePoolerSize(nodePool, __FILE__, __LINE__);
                            IncreasePoolerFreesize(nodePool,__FILE__,__LINE__);
                            if (PoolConnectDebugPrint)
                            {
                                elog(LOG, POOL_MGR_PREFIX"pooler_sync_connections_to_nodepool add new connection to node:%s "
                                                         "backend_pid:%d nodeidx:%d nodepool size:%d freeSize:%d connIndex:%d validSize:%d", 
                                                            nodePool->node_name, slot->backend_pid, nodeidx, nodePool->size, nodePool->freeSize,
                                                            connIndex, connRsp->validSize);
                                // print_pooler_slot(slot);
                            }
                        }
                        else
                        {
							destroy_slot(connRsp->nodeindex, connRsp->nodeoid, slot, false);
                            if (PoolConnectDebugPrint)
                            {
								elog(LOG, POOL_MGR_PREFIX"destroy slot poolsize:%d, "
									"freeSize:%d, node:%u, MaxPoolSize:%d, "
									"connRsp->m_version:"INT64_FORMAT", nodePool->m_version:"INT64_FORMAT,
                                                                                                                           nodePool->size, 
									nodePool->freeSize, nodePool->nodeoid, MaxPoolSize,
									connRsp->m_version, nodePool->m_version);
                            }
                        }
                                    
                    }
                    nodePool->asyncInProgress = false;

                    if (PoolConnectDebugPrint)
                    {
                        elog(LOG, POOL_MGR_PREFIX"async build succeed, poolsize:%d, freeSize:%d, node:%u", nodePool->size, 
                                                                                            nodePool->freeSize, 
                                                                                            nodePool->nodeoid);
                    }
                    
                    /* check if some node failed to connect, just release last socket */
                    if (connRsp->validSize < connRsp->size && connRsp->failed)
                    {                
                        ereport(LOG,
                                (errcode(ERRCODE_CONNECTION_FAILURE),
                                 errmsg(POOL_MGR_PREFIX"failed to connect to Datanode:[%s], validSize:%d, size:%d, errmsg:%s", nodePool->connstr, 
                                                                                                             connRsp->validSize,
                                                                                                             connRsp->size,
                                                                                                             PQerrorMessage((PGconn *)(connRsp->slot[connRsp->validSize].conn)))));

                        if (!PGXCNodeConnected(connRsp->slot[connRsp->validSize].conn))
                        {
                            /* here, we need only close the connection */
                            close_slot(connRsp->nodeindex, connRsp->nodeoid, &connRsp->slot[connRsp->validSize]);
                        }    
                    }
                    
                    /* free the memory */
                    pfree(connRsp->connstr);
                    pfree((void*)connRsp);
                    break;
                }

                case COMMAND_CONNECTION_CLOSE:
                {
                    /* just free the request */
                    pfree((void*)connRsp);
                    break;
                }

                default:
                {
                    /* should never happens */
                    abort();
                }
            }
        }
        threadIndex++;
    }
    return;
}

/* async warm a conection */
static void pooler_async_warm_connection(DatabasePool *pool, PGXCNodePoolSlot *slot, PGXCNodePool *nodePool, Oid node)
{
	uint64 pipeput_loops = 0;
    PGXCAsyncWarmInfo *asyncInfo = NULL;
    MemoryContext     oldcontext;
    
    oldcontext = MemoryContextSwitchTo(PoolerMemoryContext);
    asyncInfo                 = (PGXCAsyncWarmInfo*)palloc0(sizeof(PGXCAsyncWarmInfo));
    asyncInfo->cmd            = COMMAND_CONNECTION_WARM;
    asyncInfo->dbPool         = pool;
    asyncInfo->slot           = slot;
    asyncInfo->node           = node;
    while (-1 == PipePut(g_AsynUtilityPipeSender, (void*)asyncInfo))
    {
		pipeput_loops++;
    }

	if (pipeput_loops > 0)
	{
        elog(LOG, POOL_MGR_PREFIX"fail to async warm connection db:%s user:%s node:%u for loops %lu", pool->database, pool->user_name, node, pipeput_loops);
	}

    /* increase warming count */
    nodePool->nwarming++;
    
    ThreadSemaUp(&g_AsnyUtilitysem);
    if (PoolConnectDebugPrint)
    {
        elog(LOG, POOL_MGR_PREFIX"async warm connection db:%s user:%s nwarming:%d node:%u slot_seq:%d", pool->database, pool->user_name, nodePool->nwarming, node, slot->seqnum);
    }
    MemoryContextSwitchTo(oldcontext);
}
/* async query the memrory usage of a conection */
static void pooler_async_query_connection(DatabasePool *pool, PGXCNodePoolSlot *slot, int32 nodeidx, Oid node)
{
	uint64 pipeput_loops = 0;
    PGXCAsyncWarmInfo *asyncInfo = NULL;
    
    MemoryContext     oldcontext;    
    oldcontext = MemoryContextSwitchTo(PoolerMemoryContext);
    
    asyncInfo                 = (PGXCAsyncWarmInfo*)palloc0(sizeof(PGXCAsyncWarmInfo));
    asyncInfo->cmd            = COMMAND_JUDGE_CONNECTION_MEMSIZE;
    asyncInfo->dbPool         = pool;
    asyncInfo->slot           = slot;
    asyncInfo->nodeindex    = nodeidx;
    asyncInfo->node           = node;
    while (-1 == PipePut(g_AsynUtilityPipeSender, (void*)asyncInfo))
    {
		pipeput_loops++;
    }

	if (pipeput_loops > 0)
	{
	    elog(LOG, POOL_MGR_PREFIX"fail to async query connection db:%s user:%s node:%u for loops %lu", pool->database, pool->user_name, node, pipeput_loops);
	}

    ThreadSemaUp(&g_AsnyUtilitysem);
    if (PoolConnectDebugPrint)
    {
        elog(LOG, POOL_MGR_PREFIX"async query connection db:%s user:%s node:%u", pool->database, pool->user_name, node);
    }
    MemoryContextSwitchTo(oldcontext);
}

/* async ping a node */
static void pooler_async_ping_node(Oid node)
{
	uint64 pipeput_loops = 0;
    PGXCAsyncWarmInfo *asyncInfo = NULL;
    MemoryContext     oldcontext;
    NodeDefinition       *nodeDef   = NULL;

    nodeDef = PgxcNodeGetDefinition(node);
    if (nodeDef == NULL)
    {
        /* No such definition, node dropped? */
        elog(DEBUG1, "Could not find node (%u) definition,"
             " skipping health check", node);
        return;
    }
    if (nodeDef->nodeishealthy)
    {
        /* hmm, can this happen? */
        elog(DEBUG1, "node (%u) healthy!"
             " skipping health check", node);
        if (nodeDef)
            pfree(nodeDef);
        return;
    }

    elog(LOG, "node (%s:%u) down! Trying ping",
         NameStr(nodeDef->nodename), node);

    
    oldcontext = MemoryContextSwitchTo(PoolerMemoryContext);
    asyncInfo                 = (PGXCAsyncWarmInfo*)palloc0(sizeof(PGXCAsyncWarmInfo));
    asyncInfo->cmd            = COMMAND_PING_NODE;
    asyncInfo->node           = node;
    memcpy(NameStr(asyncInfo->nodename), NameStr(nodeDef->nodename), NAMEDATALEN);
    memcpy(NameStr(asyncInfo->nodehost), NameStr(nodeDef->nodehost), NAMEDATALEN);
    asyncInfo->nodeport        = nodeDef->nodeport;

    if (PoolConnectDebugPrint)
    {
        elog(LOG, POOL_MGR_PREFIX"async ping node, param: node=%u, nodehost=%s, nodeport=%d", 
                    node, NameStr(asyncInfo->nodehost), asyncInfo->nodeport);
    }
    
    while (-1 == PipePut(g_AsynUtilityPipeSender, (void*)asyncInfo))
    {
		pipeput_loops++;
    }

	if (pipeput_loops > 0)
	{
		elog(LOG, POOL_MGR_PREFIX"fail to async ping node:%u for loops %lu", node, pipeput_loops);
	}
    
    ThreadSemaUp(&g_AsnyUtilitysem);
    if (PoolConnectDebugPrint)
    {
        elog(LOG, POOL_MGR_PREFIX"async ping node:%u", node);
    }
    MemoryContextSwitchTo(oldcontext);

    if (nodeDef)
        pfree(nodeDef); 
}



/* async batch connection build  */
static bool pooler_async_build_connection(DatabasePool *pool, int64 pool_version, int32 nodeidx, Oid node, int32 size, char *connStr, bool bCoord)
{
	int32 threadid; 
	uint64 pipeput_loops = 0;
    PGXCPoolConnectReq *connReq = NULL;
    MemoryContext     oldcontext;    

	/* if no free pipe line avaliable, do nothing */
	threadid = pooler_async_task_pick_thread(&g_PoolConnControl, nodeidx);
	if (-1 == threadid)
	{
		elog(LOG, POOL_MGR_PREFIX"no pipeline avaliable, pooler_async_build_connection node:%u nodeidx:%d", node, nodeidx);	
		return false;
	}
	
    oldcontext = MemoryContextSwitchTo(PoolerMemoryContext);
    connReq  = (PGXCPoolConnectReq*)palloc0(sizeof(PGXCPoolConnectReq) + (size - 1) * sizeof(PGXCNodePoolSlot));
    connReq->cmd       = COMMAND_CONNECTION_BUILD;
    connReq->connstr   = pstrdup(connStr);
    connReq->nodeindex = nodeidx;
    connReq->nodeoid   = node;
    connReq->dbPool    = pool;
    connReq->bCoord    = bCoord;
    connReq->size      = size;
    connReq->validSize = 0;
    connReq->m_version = pool_version;

	while (-1 == PipePut(g_PoolConnControl.request[threadid], (void*)connReq))
    {
		pipeput_loops++;		
    }

	if (pipeput_loops > 0)
	{
	    elog(LOG, POOL_MGR_PREFIX"fail to async build connection db:%s user:%s node:%u size:%d for loops %lu", pool->database, pool->user_name, node, size, pipeput_loops);
	}
    
    /* signal thread to start build job */
	ThreadSemaUp(&g_PoolConnControl.sem[threadid]);
    if (PoolConnectDebugPrint)
    {
        elog(LOG, POOL_MGR_PREFIX"async build connection db:%s user:%s node:%u size:%d", pool->database, pool->user_name, node, size);
    }
    MemoryContextSwitchTo(oldcontext);

	return true;
}

/* aync acquire connection */
static bool dispatch_async_network_operation(PGXCPoolAsyncReq *req)
{
    int32 threadid = 0;
    Oid   node     = 0;
	uint64 pipeput_loops = 0;

    /* choose a thread to handle the msg */
    threadid = pooler_async_task_pick_thread(&g_PoolSyncNetworkControl, req->nodeindex);
    /* failed to pick up a thread */
    if (-1 == threadid)
    {
        if (req->bCoord)
        {
            node = req->agent->coord_conn_oids[req->nodeindex];
        }
        else
        {
            node = req->agent->dn_conn_oids[req->nodeindex];
        }    
        elog(LOG, POOL_MGR_PREFIX"pid:%d fail to pick a thread for operation coord:%d node:%u nodeindex:%d current_status:%d final_status:%d req_seq:%d", req->agent->pid, req->bCoord, node, req->nodeindex, req->current_status, req->final_status, req->req_seq);        
        return false;
    }

    /* remember how many request we have dispatched */
    SpinLockAcquire(&req->taskControl->m_lock);
    req->taskControl->m_mumber_total++;
    SpinLockRelease(&req->taskControl->m_lock);

    /* dispatch the msg to the handling thread */
    while (-1 == PipePut(g_PoolSyncNetworkControl.request[threadid % MAX_SYNC_NETWORK_THREAD], (void*)req))
    {
		pipeput_loops++;		
    }

	if (pipeput_loops > 0)
	{
        elog(LOG, POOL_MGR_PREFIX"fail to async network operation pid:%d bCoord:%d nodeindex:%d current_status:%d final_status:%d thread:%d req_seq:%d for loops %lu", req->agent->pid, req->bCoord, req->nodeindex, req->current_status, req->final_status, threadid, req->req_seq, pipeput_loops);
	}

    /* increase agent ref count */
    agent_increase_ref_count(req->agent);
    
    /* signal thread to start build job */
    ThreadSemaUp(&g_PoolSyncNetworkControl.sem[threadid % MAX_SYNC_NETWORK_THREAD]);
    if (PoolConnectDebugPrint)
    {
        elog(LOG, POOL_MGR_PREFIX"async network operation pid:%d bCoord:%d nodeindex:%d succeed current_status:%d final_status:%d thread:%d req_seq:%d", req->agent->pid, req->bCoord, req->nodeindex, req->current_status, req->final_status, threadid, req->req_seq);    
    }

    return true;
}

/* async warm a database pool */
static void pooler_async_warm_database_pool(DatabasePool *pool)
{// #lizard forgives
    HASH_SEQ_STATUS hseq_status;
    PGXCNodePool   *nodePool;
    int32             i;
    
    /* no need warm */
    if (!pool->bneed_warm)
    {
        return;
    }
    
    hash_seq_init(&hseq_status, pool->nodePools);
    while ((nodePool = (PGXCNodePool *) hash_seq_search(&hseq_status)))
    {
        /* skip coordinator */
        if (nodePool->coord)
        {
            continue;
        }

        /* Go thru the free slots and find the unwarmed ones, one nodePool can only warm one each time. */
        for (i = 0; i < nodePool->freeSize && i < nodePool->size && nodePool->nwarming < POOL_ASYN_WARN_NUM; i++)
        {
            PGXCNodePoolSlot *slot = nodePool->slot[i];
            
            if (!slot->bwarmed)
            {                
                /* pipe is full, no need to continue */
                if (IS_ASYNC_PIPE_FULL())
                {
                    hash_seq_term(&hseq_status);
                    return;
                }        

                /* not reduce pool size and total number of connections, for the connection is still logically in pooler */
                DecreasePoolerFreesize(nodePool,__FILE__,__LINE__);
                if (PoolConnectDebugPrint)
                {
                    elog(LOG, POOL_MGR_PREFIX"pooler_async_warm_database_pool warm a connection to node:%s backend_pid:%d nodepool size:%d freeSize:%d", nodePool->node_name, slot->backend_pid, nodePool->size, nodePool->freeSize);
                }

                /* async warm the node connection */
                pooler_async_warm_connection(pool, slot, nodePool, nodePool->nodeoid);
                
                /* move last connection in place, if not at last already */
                if (i < nodePool->freeSize)
                {
                    nodePool->slot[i] = nodePool->slot[nodePool->freeSize];
                    nodePool->slot[nodePool->freeSize] = NULL;
                }
                else if (i == nodePool->freeSize)
                {
                    nodePool->slot[nodePool->freeSize] = NULL;
                }
            }            
        }
    }

    return;
}
static bool
pooler_pools_warm(void)
{
    DatabasePool   *curr = databasePools;

    /* Iterate over the pools */
    while (curr)
    {        
        if (!preconnect_and_warm(curr))
        {
            return false;
        }
        curr = curr->next;
    }
    /* all init done, return true */
    return true;
}

static void
connect_pools(void)
{// #lizard forgives
    if (g_PoolerWarmBufferInfo)
    {
        /* format "database:user, database:user" */
        #define ELEMENT_SEP ":"
        #define DATA_SEP    ","
        #define TEMP_PATH_LEN 1024
        char    *sep  = NULL;
        char    *db   = NULL;
        char    *user = NULL;
        char    *p    = NULL;
        DatabasePool *dbpool = NULL;
        char    str[TEMP_PATH_LEN] = {0};
    
        snprintf(str, TEMP_PATH_LEN, "%s", g_PoolerWarmBufferInfo);
        p = str;
        do
        {            
            db = p;
            user = strstr(p, ELEMENT_SEP);
            if (NULL == user)
            {
                break;
            }
            *user = '\0';
            user += 1;

            sep = strstr(user, DATA_SEP);
            if (sep)
            {
                *sep =  '\0';
                p = sep + 1;
            }

            /* warm db pool */
            elog(LOG, POOL_MGR_PREFIX"Pooler: db:%s user:%s need precreate and warm ", db, user);
            dbpool = find_database_pool((char*)db, (char*)user, session_options());
            if (NULL == dbpool)
            {
                elog(LOG, POOL_MGR_PREFIX"Pooler: no database_pool entry found for db:%s user:%s, create a new one ", db, user);
                dbpool = create_database_pool((char*)db, (char*)user, session_options());
                if (NULL == dbpool)
                {
                    elog(LOG, POOL_MGR_PREFIX"Pooler: create database_pool entry for db:%s user:%s failed ", db, user);
                    break;
                }
            }

            elog(LOG, POOL_MGR_PREFIX"Pooler: set database_pool entry flag for db:%s user:%s ", db, user);
            dbpool->bneed_warm      = true;
            dbpool->bneed_precreate = true;
            if (NULL == sep)
            {
                break;
            }
            
            if (p == '\0')
            {
                break;
            }
        }while(1);
    }
}

/*
 * Set cancel socket keepalive and user_timeout.
 * We can use this to detect the broken connection quickly.
 * see SetSockKeepAlive
 */
static void
set_cancel_conn_keepalive(PGcancel *cancelConn)
{
    uint32 user_timeout = UINT32_MAX / 1000 < tcp_keepalives_idle ?
                          0 : tcp_keepalives_idle * (uint32) 1000;

    if (cancelConn == NULL)
    {
        return;
    }

    /*
     * If the connection did not use the connection option
     * set the option here
     * */
    if (cancelConn->keepalives == -1)
    {
        /* use TCP keepalives */
        cancelConn->keepalives = 1;

        if (tcp_keepalives_idle > 0)
        {
            /* time between TCP keepalives */
            cancelConn->keepalives_idle = tcp_keepalives_idle;
        }

        if (tcp_keepalives_interval > 0)
        {
            /*time between TCP keepalive retransmits */
            cancelConn->keepalives_interval = tcp_keepalives_interval;
        }

        if (tcp_keepalives_count > 0)
        {
            /* maximum number of TCP keepalive retransmits */
            cancelConn->keepalives_count = tcp_keepalives_count;
        }
    }

    if (cancelConn->pgtcp_user_timeout == -1 && user_timeout > 0)
    {
        /* tcp user timeout */
        cancelConn->pgtcp_user_timeout = user_timeout;
    }
}

static bool
preconnect_and_warm(DatabasePool *dbPool)
{// #lizard forgives
    PGXCNodePool   *nodePool;
    Oid             *dnOids = NULL;
    int             num_dns = 0;
    int             i       = 0;
    bool            found;
    bool            *success = NULL;
    
    Assert(dbPool);
    if (!dbPool->bneed_precreate)
    {
        return true;
    }

    /* enter only once */
    dbPool->bneed_precreate = false;
    
    PgxcNodeGetOids(NULL, &dnOids, NULL, &num_dns, false);
    if (0 == num_dns)
    {
        return false;
    }
    
    success = (bool*)palloc0(sizeof(bool) * num_dns);
    for (i = 0; i < num_dns; i++)
    {
        char *name_str;
        nodePool = (PGXCNodePool *) hash_search(dbPool->nodePools, &dnOids[i],
                                            HASH_ENTER, &found);

        if (!found)
        {
            nodePool->connstr = build_node_conn_str(dnOids[i], dbPool);
            if (!nodePool->connstr)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg(POOL_MGR_PREFIX"could not build connection string for node %u", dnOids[i])));
            }

            nodePool->slot = (PGXCNodePoolSlot **) palloc0(MaxPoolSize * sizeof(PGXCNodePoolSlot *));
            if (!nodePool->slot)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_OUT_OF_MEMORY),
                         errmsg(POOL_MGR_PREFIX"out of memory")));
            }
            nodePool->freeSize = 0;
            nodePool->size     = 0;
            nodePool->coord    = false;
            nodePool->nwarming   = 0;
            nodePool->nquery     = 0;

            name_str = get_node_name_by_nodeoid(dnOids[i]);
            if (NULL == name_str)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg(POOL_MGR_PREFIX"get node %u name failed", dnOids[i])));
            }
            snprintf(nodePool->node_name, NAMEDATALEN, "%s", name_str);

			nodePool->m_version = dbPool->version++;
			elog(LOG,
				"preconnect_and_warm: nodePools of node (%u, %s) is created.",
				nodePool->nodeoid, nodePool->node_name);
        }

        while (nodePool->size < MinPoolSize || (nodePool->freeSize < MinFreeSize && nodePool->size < MaxPoolSize))
        {
            int32             nodeidx = 0;
            PGXCNodePoolSlot *slot    = NULL;

            /* Allocate new slot */
            slot = (PGXCNodePoolSlot *) palloc0(sizeof(PGXCNodePoolSlot));
            if (slot == NULL)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_OUT_OF_MEMORY),
                         errmsg(POOL_MGR_PREFIX"out of memory")));
            }

            /* If connection fails, be sure that slot is destroyed cleanly */
            slot->xc_cancelConn = NULL;

            /* Establish connection */
            slot->conn = PGXCNodeConnect(nodePool->connstr);
            if (!PGXCNodeConnected(slot->conn))
            {
                ereport(DEBUG1,
                        (errcode(ERRCODE_CONNECTION_FAILURE),
                         errmsg(POOL_MGR_PREFIX"failed to connect to Datanode:[%s],errmsg[%s]", nodePool->connstr, PQerrorMessage((PGconn*)(slot->conn)))));
                nodeidx = get_node_index_by_nodeoid(nodePool->nodeoid);
				destroy_slot(nodeidx, nodePool->nodeoid, slot, false);
                pfree((void*)dnOids);
                pfree((void*)success);
                return false;
            }

			slot->xc_cancelConn = (NODE_CANCEL *) PQgetCancel((PGconn *)slot->conn);
			SetSockKeepAlive(((PGconn *)slot->conn)->sock);
            set_cancel_conn_keepalive((PGcancel *)slot->xc_cancelConn);
			
			/* Increase count of pool size */			
			nodePool->slot[nodePool->freeSize] = slot;
			
			/* Insert at the end of the pool */
			IncreasePoolerSize(nodePool, __FILE__, __LINE__);
			IncreasePoolerFreesize(nodePool,__FILE__,__LINE__);
			slot->released = time(NULL);
			slot->checked  = slot->released;
			slot->created  = slot->released;
			slot->node_name = nodePool->node_name;
			slot->backend_pid = ((PGconn *) slot->conn)->be_pid;
			if (dbPool->oldest_idle == (time_t) 0)
			{
				dbPool->oldest_idle = slot->released;
			}

            if (PoolConnectDebugPrint)
            {
                elog(LOG, POOL_MGR_PREFIX"preconnect_and_warm add new connection to node:%s backend_pid:%d nodepool size:%d freeSize:%d", nodePool->node_name, slot->backend_pid, nodePool->size, nodePool->freeSize);
            }
            elog(DEBUG1, POOL_MGR_PREFIX"Pooler: init and warm pool size to %d for pool %s ",
                 nodePool->size,
                 nodePool->connstr);
                    
            
        }
        
        if (nodePool->size > MinPoolSize)
        {
            elog(LOG, POOL_MGR_PREFIX"Pooler: pool %s size is %d, MinPoolSize:%d",
                 nodePool->connstr,
                 nodePool->size, MinPoolSize);
            success[i] = true;
        }
    }
    
    if (dnOids)
    {
        pfree((void*)dnOids);
    }    
    
    /* disable precreate after the connection has been created */
    if (success)
    {
        for (i = 0; i < num_dns; i++)
        {
            if (!success[i])
            {
                pfree((void*)success);
                return false;
            }
        }
        pfree((void*)success);
        dbPool->bneed_precreate = false;
        
        elog(LOG, POOL_MGR_PREFIX"Pooler: db:%s user:%s precreate and warm connection success",                   
                  dbPool->database,
                  dbPool->user_name);
    }
    
    return true;
}

/*
 * Thread that will build connection async
 */
void *pooler_async_connection_management_thread(void *arg)
{
    int32               i           = 0;
    int32                 threadIndex = 0;
    PGXCPoolConnectReq  *request     = NULL;
    PGXCNodePoolSlot     *slot        = NULL;    

	threadIndex = ((PGXCPoolConnThreadParam*)arg)->threadIndex;
	while (1)
	{
		/* wait for signal */
		ThreadSemaDown(&g_PoolConnControl.sem[threadIndex]);		
		
		/* create connect as needed */
		request = (PGXCPoolConnectReq*)PipeGet(g_PoolConnControl.request[threadIndex]);
		if (request)
		{
			/* record status of the task */
			pooler_async_task_start(&g_PoolConnControl, threadIndex, request->nodeindex, NULL, InvalidOid, request->cmd);
			
			switch (request->cmd)
			{
				case COMMAND_CONNECTION_BUILD:
				{
					for (i = 0; i < request->size; i++, request->validSize++)
					{			
						slot =  &request->slot[i]; 
						/* If connection fails, be sure that slot is destroyed cleanly */
						slot->xc_cancelConn = NULL;

						/* Establish connection */
						slot->conn = PGXCNodeConnectBarely(request->connstr);
						if (!PGXCNodeConnected(slot->conn))
						{		
							request->failed = true;
							break;
						}		
						slot->xc_cancelConn = (NODE_CANCEL *) PQgetCancel((PGconn *)slot->conn);
						slot->bwarmed       = false;
						SetSockKeepAlive(((PGconn *)slot->conn)->sock);
                        set_cancel_conn_keepalive((PGcancel *)slot->xc_cancelConn);
					}					
					break;
				}

				case COMMAND_CONNECTION_CLOSE:
				{					
					PQfreeCancel((PGcancel *)request->slot[0].xc_cancelConn);	
					PGXCNodeClose(request->slot[0].conn);
					break;	
				}

				default:
				{
					/* should never happen */
					abort();
				}
			}

            /* clear the work status */
            pooler_async_task_done(&g_PoolConnControl, threadIndex);    
            
            /* return the request to main thread, so that the memory can be freed */
            while(-1 == PipePut(g_PoolConnControl.response[threadIndex], request))
            {             
                pg_usleep(1000L);
            }
        }        
    }
    return NULL;
}


/*
 * Thread that will handle sync network operation
 */
void *pooler_sync_remote_operator_thread(void *arg)
{// #lizard forgives
    bool                bsucceed    = false;
    int32                  ret         = 0;
    int32               res            = 0;
    int32                 threadIndex = 0;
    PGXCPoolAsyncReq    *request     = NULL;
    PGXCNodePoolSlot     *slot        = NULL;    
    Oid                 nodeoid        = InvalidOid;
    PGXCNodePoolSlot     *slot2        = NULL;    
    
    threadIndex = ((PGXCPoolConnThreadParam*)arg)->threadIndex;
    while (1)
    {
        /* wait for signal */
        ThreadSemaDown(&g_PoolSyncNetworkControl.sem[threadIndex]);        

        nodeoid        = InvalidOid;
        slot2        = NULL;
        
        /* create connect as needed */
        request = (PGXCPoolAsyncReq*)PipeGet(g_PoolSyncNetworkControl.request[threadIndex]);
        if (request)
        {
            /* Only few circumstances, we can get the nodeoid and slot */
            if (is_slot_avail(request))
            {
                /* get nodeoid and slot */
                if (request->bCoord)
                {
                    nodeoid =  request->agent->coord_conn_oids[request->nodeindex];
                    slot2    =  request->agent->coord_connections[request->nodeindex];
                }
                else
                {
                    nodeoid =  request->agent->dn_conn_oids[request->nodeindex];
                    slot2    =  request->agent->dn_connections[request->nodeindex];
                }
            }
                
            /* set task status */
            pooler_async_task_start(&g_PoolSyncNetworkControl, threadIndex, request->nodeindex, slot2, nodeoid, request->cmd);
            
            switch (request->cmd)
            {
                case 'b':
                {
                    /* Fire transaction-block commands on given nodes */                    
                    switch (request->final_status)
                    {
                        case PoolLocalSetStatus_reset:
                        {
                            CommandId commandId = InvalidCommandId;
                            
                            /* get slot first */
                            if (request->bCoord)
                            {            
                                slot = request->agent->coord_connections[request->nodeindex];
                            }
                            else
                            {
                                slot = request->agent->dn_connections[request->nodeindex];
                            }
                        
                            res = -1;
                            if (slot)
                            {
                                res = PGXCNodeSendSetQuery(slot->conn, request->agent->local_params, request->errmsg, POOLER_ERROR_MSG_LEN, &request->setquery_status, &commandId);
                            }
                            
                            if (res)
                            {
                                request->error_flag = true;
                            }
                            add_task_result(request->taskControl, res);
                            finish_task_request(request->taskControl);    
                            break;
                        }
                        case PoolLocalSetStatus_destory:
                        {
                            /* last request */                            
                            finish_task_request(request->taskControl);
                            
                            /* wait for others to finish their job */
                            while (!check_is_task_done(request->taskControl))
                            {
                                pg_usleep(1000L);
                            }

                            /* send response to session */
                            res = get_task_result(request->taskControl);                            
                            ret = pool_sendres(&request->agent->port, res, request->errmsg, POOLER_ERROR_MSG_LEN, false);
                            if (ret)
                            {
                                request->error_flag = true;
                            }
                            
                            /* all job done, just set all status */
                            set_task_status(request->taskControl, PoolAyncCtlStaus_done);
                            request->current_status = PoolLocalSetStatus_destory;
                            break;
                        }
                    }                                        
                    break;
                }

                case 'd':
                {        
                    /* Disconnect */
                    switch (request->current_status)
                    {
                        case PoolResetStatus_reset:
                        {
                            CommandId commandId = InvalidCommandId;
                            if (request->bCoord)
                            {
                                slot = request->agent->coord_connections[request->nodeindex];
                            }
                            else
                            {
                                slot = request->agent->dn_connections[request->nodeindex];
                            }
                            
                            /* reset connection */
                            if (slot)
                            {
                                res = PGXCNodeSendSetQuery(slot->conn, "DISCARD ALL;", NULL, 0, &request->setquery_status, &commandId);
                            }

                            if (res)
                            {
                                request->error_flag = true;
                            }
                            finish_task_request(request->taskControl);
                            break;

                        }
                        case PoolResetStatus_destory:
                        {
                            /* set self status */
                            finish_task_request(request->taskControl);

                            /* wait for others */
                            while (!check_is_task_done(request->taskControl))
                            {
                                pg_usleep(1000L);
                            }
                            break;
                        }
                        
                        default:
                        {
                            abort();
                        }
                    }                    
                    break;    
                }

                case 'f':
                {
                    /* CLEAN CONNECTION */                    
                    break;
                }

                case 'g':            
                {
                    /* GET CONNECTIONS */
                    int32             node_number   = 0;
                    PGXCNodePoolSlot *slot             = NULL;
                    ListCell         *nodelist_item = NULL;
                    
                    while (request->current_status <= request->final_status)
                    {                        
                        switch (request->current_status)
                        {
                            case PoolConnectStaus_init:
                            {                
                                /* task error, do nothing; just tell controller I am done */
                                if (PoolAyncCtlStaus_error == get_task_status(request->taskControl))
                                {
                                    /* jump to the last status */
                                    request->current_status = PoolConnectStaus_error;
                                    finish_task_request(request->taskControl);
                                    break;    
                                }

                                /* record message */
                                record_task_message(&g_PoolSyncNetworkControl, threadIndex, request->nodepool->connstr);
                                
                                slot                 =  request->slot;
                                slot->xc_cancelConn = NULL;                
                                
                                slot->conn = PGXCNodeConnect(request->nodepool->connstr);                                                                                        
                                if (NULL == slot->conn)
                                {
                                    snprintf(request->errmsg, POOLER_ERROR_MSG_LEN, "connect node:[%s] failed", request->nodepool->connstr);
                                    request->current_status = PoolConnectStaus_error;
#ifdef __TBASE__
                                    SpinLockAcquire(&request->agent->port.lock);
                                    request->agent->port.error_code = POOL_ERR_GET_CONNECTIONS_OOM;
                                    snprintf(request->agent->port.err_msg, POOL_ERR_MSG_LEN, "%s, connection info [%s]", poolErrorMsg[POOL_ERR_GET_CONNECTIONS_OOM],
                                        request->nodepool->connstr);
                                    SpinLockRelease(&request->agent->port.lock);
#endif
                                    set_task_status(request->taskControl, PoolAyncCtlStaus_error);
                                    finish_task_request(request->taskControl);
                                    break;
                                }

                                /* Error, free the memory */
                                if (!PGXCNodeConnected(slot->conn))
                                {                                    
                                    snprintf(request->errmsg, POOLER_ERROR_MSG_LEN, "connection not connect for node:[%s] failed errmsg: %s", 
                                        request->nodepool->connstr,
                                        PQerrorMessage((PGconn *)(slot->conn)));
                                    if (slot->conn)
                                    {                                        
                                        PGXCNodeClose(slot->conn);
                                        slot->conn = NULL;
                                    }
									
									finish_task_request(request->taskControl);
									
									if (request->taskControl->m_missing_ok)
									{
										request->current_status = PoolConnectStaus_done;
										break;
									}
									else
									{
                                        request->current_status = PoolConnectStaus_error;
#ifdef __TBASE__
                                        SpinLockAcquire(&request->agent->port.lock);
                                        request->agent->port.error_code = POOL_ERR_GET_CONNECTIONS_CONNECTION_BAD;
                                        snprintf(request->agent->port.err_msg, POOL_ERR_MSG_LEN, "%s, connection info [%s]", poolErrorMsg[POOL_ERR_GET_CONNECTIONS_CONNECTION_BAD],
                                            request->nodepool->connstr);
                                        SpinLockRelease(&request->agent->port.lock);
#endif
                                        set_task_status(request->taskControl, PoolAyncCtlStaus_error);		
                                        pooler_thread_logger(LOG, "connection not connect for node:[%s] failed errno %d", request->nodepool->connstr, errno);
                                        break;
								    }				
								}				

								slot->xc_cancelConn = (NODE_CANCEL *) PQgetCancel((PGconn *)slot->conn);
								slot->bwarmed       = false;
								SetSockKeepAlive(((PGconn *)slot->conn)->sock);
                                set_cancel_conn_keepalive((PGcancel *)slot->xc_cancelConn);
								
								/* set the time flags */
								slot->released = time(NULL);
								slot->checked  = slot->released;
								slot->created  = slot->released;
								
								/* increase usecount */
								slot->usecount++;
								slot->node_name = request->nodepool->node_name;
								slot->backend_pid = ((PGconn *) slot->conn)->be_pid;
								if (request->bCoord)
								{
									request->agent->coord_connections[request->nodeindex] = slot;
								}
								else
								{
									request->agent->dn_connections[request->nodeindex] 	  = slot;
								}
								request->current_status = PoolConnectStaus_connected;	
#ifdef _POOLER_CHECK_								
								snprintf(request->errmsg, POOLER_ERROR_MSG_LEN, "parallel connect thread build connection to node:%s backend_pid:%d nodeidx:%d succeed", slot->node_name, slot->backend_pid, request->nodeindex);
#endif
                                continue;
                            }
                            
                            case PoolConnectStaus_connected:
                            {
                                if (PoolConnectStaus_connected == request->final_status)
                                {
								    /* Increase success count first and then finish count */
                                    acquire_command_increase_succeed(request->taskControl);
									finish_task_request(request->taskControl);
                                    request->current_status = PoolConnectStaus_done;
                                }
                                else
                                {
                                    request->current_status = PoolConnectStaus_set_param;
                                }
                                
                                continue;
                            }
                            
                            case PoolConnectStaus_set_param:
                            {                        
                                CommandId commandId = InvalidCommandId;
                                if (PoolConnectStaus_set_param == request->final_status)
                                {
                                    res = 0;
                                    if (request->agent->session_params)
                                    {
                                        /* 
                                         * sepcial case in 'g', othes set in front of pooler_sync_remote_operator_thread
                                         * by call pooler_async_task_start
                                         */
                                        
                                        /* get nodeoid and slot */
                                        if (request->bCoord)
                                        {
                                            nodeoid =  request->agent->coord_conn_oids[request->nodeindex];
                                            slot2    =  request->agent->coord_connections[request->nodeindex];
                                        }
                                        else
                                        {
                                            nodeoid =  request->agent->dn_conn_oids[request->nodeindex];
                                            slot2    =  request->agent->dn_connections[request->nodeindex];
                                        } 
                                        record_slot_info(&g_PoolSyncNetworkControl, threadIndex, slot2, nodeoid);
                                        /* record message */
                                        record_task_message(&g_PoolSyncNetworkControl, threadIndex, request->agent->session_params);
                                        
                                        if (request->bCoord)
                                        {
                                            res = PGXCNodeSendSetQuery(request->agent->coord_connections[request->nodeindex]->conn, request->agent->session_params, request->errmsg, POOLER_ERROR_MSG_LEN, &request->setquery_status, &commandId);
                                        }
                                        else
                                        {
                                            res = PGXCNodeSendSetQuery(request->agent->dn_connections[request->nodeindex]->conn, request->agent->session_params, request->errmsg, POOLER_ERROR_MSG_LEN, &request->setquery_status, &commandId);
                                        }
                                    }

                                    /* Error, free the connection here only when we build the connection here */
                                    if (res)
                                    {
                                        if (request->needConnect)
                                        {
                                            if (request->bCoord)
                                            {
                                                slot = request->agent->coord_connections[request->nodeindex];
                                                request->agent->coord_connections[request->nodeindex] = NULL;
                                            }
                                            else
                                            {
                                                slot = request->agent->dn_connections[request->nodeindex];
                                                request->agent->dn_connections[request->nodeindex] = NULL;
                                            }

                                            if (slot->xc_cancelConn)
                                            {
                                                PQfreeCancel((PGcancel *)slot->xc_cancelConn);    
                                                slot->xc_cancelConn = NULL;
                                            }

                                            if (slot->conn)
                                            {                                        
                                                PGXCNodeClose(slot->conn);
                                                slot->conn = NULL;
                                            }
                                        }
                                        request->current_status = PoolConnectStaus_error;/* PoolConnectStaus_error will break the loop */
                                        set_task_status(request->taskControl, PoolAyncCtlStaus_error);
                                        finish_task_request(request->taskControl);
                                    }
                                    else
                                    {
                                        /* job succeed */
                                        request->current_status = PoolConnectStaus_done;
                                        /* Increase success count first and then finish count */
                                        acquire_command_increase_succeed(request->taskControl);
										finish_task_request(request->taskControl);
                                    }
                                }                                                            
                                continue;
                            }
                            
                            case PoolConnectStaus_done:
                            {                
                                /* here we finish the job, just break */                    
                                break;
                            }

                            case PoolConnectStaus_destory:
                            {
                                int32                ret2    = 0;
                                                
                                /* Increase success count first and then finish count */
                                acquire_command_increase_succeed(request->taskControl);
                                /* set myself finish count */
                                finish_task_request(request->taskControl);
                                
                                /* wait for others to finish */    
                                while (!check_is_task_done(request->taskControl))
                                {
                                    pg_usleep(1000L);
                                }

                                /* Only when we reach the PoolAyncCtlStaus_dispatched and we succeed doing all things, we are able to return the result. */
                                if (PoolAyncCtlStaus_dispatched == get_task_status(request->taskControl) && get_acquire_success_status(request->taskControl))
                                {        
#ifdef _POOLER_CHECK_    
                                    char **hostip = NULL;
                                    char **hostport = NULL;
                                    
                                    hostip = (char**)malloc((list_length(request->taskControl->m_datanodelist) + list_length(request->taskControl->m_coordlist)) * sizeof(char *));
                                    if (hostip == NULL)
                                    {
                                        ereport(ERROR,
                                                (errcode(ERRCODE_OUT_OF_MEMORY),
                                                 errmsg("out of memory")));
                                    }
                                    memset(hostip, 0, (list_length(request->taskControl->m_datanodelist) + list_length(request->taskControl->m_coordlist)) * sizeof(char *));
                            
                                    hostport = (char**)malloc((list_length(request->taskControl->m_datanodelist) + list_length(request->taskControl->m_coordlist)) * sizeof(char *));
                                    if (hostport == NULL)
                                    {
                                        ereport(ERROR,
                                                (errcode(ERRCODE_OUT_OF_MEMORY),
                                                 errmsg("out of memory")));
                                    }
                                    memset(hostport, 0, (list_length(request->taskControl->m_datanodelist) + list_length(request->taskControl->m_coordlist)) * sizeof(char *));
#endif    
                                
                                    /* handle response */
                                    node_number = 0;
                                    
                                    /* Save in array fds of Datanodes first */
                                    foreach(nodelist_item, request->taskControl->m_datanodelist)
                                    {
                                        int            node = lfirst_int(nodelist_item);
                                        if (request->agent->dn_connections[node])
                                        {
                                            request->taskControl->m_result[node_number] = PQsocket((PGconn *) request->agent->dn_connections[node]->conn);
                                            request->taskControl->m_pidresult[node_number] = ((PGconn *) request->agent->dn_connections[node]->conn)->be_pid;
#ifdef     _POOLER_CHECK_    
                                            hostip[node_number] = strdup(((PGconn *) request->agent->dn_connections[node]->conn)->pghost);
                                            hostport[node_number] = strdup(((PGconn *) request->agent->dn_connections[node]->conn)->pgport);
#endif
                                            node_number++;
                                        }
										else if (request->taskControl->m_missing_ok)
										{
											request->taskControl->m_result[node_number] = 0;
											request->taskControl->m_pidresult[node_number] = 0;
											node_number++;
										}
                                    }

                                    /* Save then in the array fds for Coordinators */
                                    foreach(nodelist_item, request->taskControl->m_coordlist)
                                    {
                                        int            node = lfirst_int(nodelist_item);
                                        if (request->agent->coord_connections[node])
                                        {
                                            request->taskControl->m_result[node_number] = PQsocket((PGconn *) request->agent->coord_connections[node]->conn);
                                            request->taskControl->m_pidresult[node_number] = ((PGconn *) request->agent->coord_connections[node]->conn)->be_pid;    
#ifdef     _POOLER_CHECK_    
                                            hostip[node_number] = strdup(((PGconn *) request->agent->coord_connections[node]->conn)->pghost);
                                            hostport[node_number] = strdup(((PGconn *) request->agent->coord_connections[node]->conn)->pgport);
#endif
                                            node_number++;
                                        }
										else
										{
											request->taskControl->m_result[node_number] = 0;
											request->taskControl->m_pidresult[node_number] = 0;
											node_number++;
										}
                                    }                

#ifdef     _POOLER_CHECK_    
                                    {
                                        int i = 0;
                                        int j = 0;                                        
                                        for (i = 0; i < node_number; i ++)
                                        {                
                                            for (j = 0; j < node_number; j ++)
                                            {
                                                if (request->taskControl->m_pidresult[i] == request->taskControl->m_pidresult[j] &&
                                                    i != j                                  &&
                                                    strcmp(hostip[i], hostip[j]) == 0     &&
                                                    strcmp(hostport[i], hostport[j]) == 0)
                                                {
                                                    abort();
                                                }
                                            }
                                        }

                                        for (i = 0; i < node_number; i ++)
                                        {
                                            if (hostip[i])
                                            {
                                                free(hostip[i]);
                                                hostip[i] = NULL;
                                            }
                                            if (hostport[i])
                                            {
                                                free(hostport[i]);
                                                hostport[i] = NULL;
                                            }
                                        }
                                        if (hostip)
                                        {
                                            free(hostip);

                                            hostip = NULL;
                                        }
                                        if (hostport)
                                        {
                                            free(hostport);
                                            hostport = NULL;
                                        }
                                    }
#endif                                                                    
                                    ret = pool_sendfds(&request->agent->port, request->taskControl->m_result, node_number, request->errmsg, POOLER_ERROR_MSG_LEN);
                                    /*
                                     * Also send the PIDs of the remote backend processes serving
                                     * these connections
                                     */
                                    ret2 = pool_sendpids(&request->agent->port, request->taskControl->m_pidresult, node_number, request->errmsg, POOLER_ERROR_MSG_LEN);
                                    
                                    if (ret || ret2)
                                    {
                                        /* error */
                                        request->needfree       = true;
                                        request->current_status = PoolConnectStaus_error;    
                                        set_task_status(request->taskControl, PoolAyncCtlStaus_error);
                                    }
                                    else
                                    {
                                        request->needfree       = true;
                                        request->current_status = PoolConnectStaus_destory;    
                                        set_task_status(request->taskControl, PoolAyncCtlStaus_done);
                                    }
                                }
                                else
                                {
                                    /* failed to acquire connection */
                                    pool_sendfds(&request->agent->port, NULL, 0, request->errmsg, POOLER_ERROR_MSG_LEN);
                                    /*
                                     * Also send the PIDs of the remote backend processes serving
                                     * these connections
                                     */
                                    pool_sendpids(&request->agent->port, NULL, 0,request->errmsg, POOLER_ERROR_MSG_LEN);
                                    
                                    request->needfree       = true;
                                    request->current_status = PoolConnectStaus_error;
                                    set_task_status(request->taskControl, PoolAyncCtlStaus_error);
                                }
                                break;
                            }    
                        }

                        /* finish the request here */
                        if (PoolConnectStaus_done == request->current_status || PoolConnectStaus_destory == request->current_status)
                        {
                            break;
                        }                        
                    }
                    break;
                }

                case 'h':            
                {    
                    /* Cancel SQL Command in progress on specified connections */
                    switch (request->final_status)
                    {
                        case PoolCancelStatus_cancel:
                        case PoolCancelStatus_end_query:
                        {        
                            /* get slot first */
                            if (request->bCoord)
                            {            
                                slot = request->agent->coord_connections[request->nodeindex];
                            }
                            else
                            {
                                slot = request->agent->dn_connections[request->nodeindex];
                            }
                        
                            if (slot)
                            {
                                if (slot->xc_cancelConn)
                                {
                                    if (request->final_status == PoolCancelStatus_cancel)
                                    {
                                        bsucceed = PQcancel((PGcancel *) slot->xc_cancelConn, request->errmsg, POOLER_ERROR_MSG_LEN);
                                    }
                                    else
                                    {
                                        bsucceed = PQend_query((PGcancel *) slot->xc_cancelConn, request->errmsg, POOLER_ERROR_MSG_LEN);
                                    }

                                    if (!bsucceed)
                                    {
                                        char *host = PQhost((PGconn *)slot->conn);
                                        char *port = PQport((PGconn *)slot->conn);
                                        SpinLockAcquire(&request->agent->port.lock);
                                        request->agent->port.error_code = POOL_ERR_CANCEL_SEND_FAILED;
                                        if (host && port)
                                        {
                                            snprintf(request->agent->port.err_msg, POOL_ERR_MSG_LEN, "%s, ip %s, port %s", poolErrorMsg[POOL_ERR_CANCEL_SEND_FAILED], host, port);
                                        }
                                        else
                                        {
                                            snprintf(request->agent->port.err_msg, POOL_ERR_MSG_LEN, "%s", poolErrorMsg[POOL_ERR_CANCEL_SEND_FAILED]);
                                        }
                                        SpinLockRelease(&request->agent->port.lock);
                                    }
                                }
                                else
                                {
                                    snprintf(request->errmsg, POOLER_ERROR_MSG_LEN, "xc_cancelConn element of agent slot is null.");
                                    bsucceed = false;
                                }
                            }
                            else
                            {
                                bsucceed = false;
                                snprintf(request->errmsg, POOLER_ERROR_MSG_LEN, "slot element of agent is null.");
                            }
                            

                            if (!bsucceed)
                            {
                                request->error_flag = true;
                            }

                            if (bsucceed)
                            {
                                add_task_result(request->taskControl, 1);
                            }
                            finish_task_request(request->taskControl);    
                            break;
                        }
                        case PoolCancelStatus_destory:
                        {                    
                            /* the package is a fake one */
                            finish_task_request(request->taskControl);
                            
                            /* wait for others to finish their job */
                            while (!check_is_task_done(request->taskControl))
                            {
                                pg_usleep(1000L);
                            }
                            
                            /* all job done, just set all status */
                            set_task_status(request->taskControl, PoolAyncCtlStaus_done);

                            /* send response to session */
                            res = get_task_result(request->taskControl);                            
                            ret = pool_sendres(&request->agent->port, res, request->errmsg, POOLER_ERROR_MSG_LEN, false);
                            if (ret)
                            {
                                request->error_flag = true;
                            }
                            break;
                        }
                    }                                        
                    break;
                }

                case 'r':            
                {
                    /* RELEASE CONNECTIONS */
                    /* nothing to do */
                    break;
                }

                case 's':            
                {
                    /* Session-related COMMAND */                        
                    switch (request->current_status)
                    {
                        case PoolSetCommandStatus_set:
                        {
                            CommandId commandID = InvalidCommandId;
                            if (request->bCoord)
                            {
                                slot = request->agent->coord_connections[request->nodeindex];
                            }
                            else
                            {
                                slot = request->agent->dn_connections[request->nodeindex];
                            }
                            
                            /* set connection command */
                            if (slot)
                            {
                                res = PGXCNodeSendSetQuery(slot->conn, request->taskControl->m_command, request->errmsg, POOLER_ERROR_MSG_LEN, &request->setquery_status, &commandID);
                            }

                            /* on success, we increase successful counter. */
                            if (res)
                            {
                                request->error_flag = true;
                                set_task_error_msg(request->taskControl, request->errmsg);
                            }
                            else
                            {
                                set_command_increase_succeed(request->taskControl);
                                set_task_max_command_id(request->taskControl, commandID);
                            }
                            finish_task_request(request->taskControl);
                            break;
                        }
                        case PoolSetCommandStatus_destory:
                        {
                            CommandId commandID = InvalidCommandId;
                            /* set self status */
                            finish_task_request(request->taskControl);
                            
                            /* wait for others */
                            while (!check_is_task_done(request->taskControl))
                            {
                                pg_usleep(1000L);
                            }

                            /* get command status, and send response */
                            res       = get_command_success_status(request->taskControl) ? 0 : -1;
                            commandID = get_task_max_commandID(request->taskControl);
                            ret       = pool_sendres_with_command_id(&request->agent->port, res, commandID, request->errmsg, POOLER_ERROR_MSG_LEN, request->taskControl->m_error_msg, false);
                            if (ret)
                            {
                                request->error_flag = true;
                            }
                            break;
                        }
                        
                        default:
                        {
                            abort();
                        }
                    }                    
                    break;
                }
                default:
                {
                    /* should never happen */
                    abort();
                }
            }

            /* record each conn request end time */
            if (PoolPrintStatTimeout > 0 && 'g' == request->cmd)
            {
                gettimeofday(&request->end_time, NULL);        
            }

            if (request->cmd_start_time != 0)
            {
                request->cmd_end_time = get_system_time();
            }
            
            /* clear task status */
            pooler_async_task_done(&g_PoolSyncNetworkControl, threadIndex);            
            
            /* return the request to main thread, so that the memory can be freed */
            while(-1 == PipePut(g_PoolSyncNetworkControl.response[threadIndex], request))
            {             
                pg_usleep(1000L);
            }
        }        
    }
    return NULL;
}

/*
 * Thread used to process async connection requeset
 */
void *pooler_async_utility_thread(void *arg)
{// #lizard forgives    
    int32   ret = 0;
    PGXCAsyncWarmInfo *pWarmInfo = NULL;
    while (1)
    {
        ThreadSemaDown(&g_AsnyUtilitysem);
        pWarmInfo = (PGXCAsyncWarmInfo*)PipeGet(g_AsynUtilityPipeSender);
        if (pWarmInfo)
        {
            switch (pWarmInfo->cmd)
            {
                case COMMAND_CONNECTION_WARM:
                {
                    CommandId commandID = InvalidCommandId;

					ret = PGXCNodeSendSetQuery(
						(NODE_CONNECTION *) pWarmInfo->slot->conn,
						"set warm_shared_buffer to true;",
						NULL,
						0,
						&pWarmInfo->set_query_status, &commandID);
                    /* only set warm flag when warm succeed */
                    if (0 == ret)
                    {
                        pWarmInfo->slot->bwarmed = true;                    
                    }
                }
                break;
                
                case COMMAND_JUDGE_CONNECTION_MEMSIZE:
                {
                    int   mbytes = 0;
                    char *size = NULL;
					CommandId commandID = InvalidCommandId;

					(void) PGXCNodeSendSetQuery(
						(NODE_CONNECTION *) pWarmInfo->slot->conn,
						"set remotetype to application;",
						NULL,
						0,
						&pWarmInfo->set_query_status, &commandID);

					size = PGXCNodeSendShowQuery(
						(NODE_CONNECTION *) pWarmInfo->slot->conn,
						"show session_memory_size;");
                    pWarmInfo->cmd = COMMAND_JUDGE_CONNECTION_MEMSIZE;
                    mbytes = atoi(size);
					if (PoolMaxMemoryLimit > 0 && mbytes >= PoolMaxMemoryLimit)
                    {
                        pWarmInfo->cmd = COMMAND_CONNECTION_NEED_CLOSE;
                    }
                    pWarmInfo->size = mbytes;

					if (IS_PGXC_COORDINATOR)
					{
						(void) PGXCNodeSendSetQuery(
							(NODE_CONNECTION *) pWarmInfo->slot->conn,
							"set remotetype to coordinator;",
							NULL,
							0,
							&pWarmInfo->set_query_status, &commandID);
					}
					else
					{
						(void) PGXCNodeSendSetQuery(
							(NODE_CONNECTION *) pWarmInfo->slot->conn,
							"set remotetype to datanode;",
							NULL,
							0,
							&pWarmInfo->set_query_status, &commandID);
					}
                }
                break;

                case COMMAND_PING_NODE:
                {
                    char connstr[MAXPGPATH * 2 + 256] = {0};
                    sprintf(connstr, "host=%s port=%d", NameStr(pWarmInfo->nodehost),
                                    pWarmInfo->nodeport);
                    pWarmInfo->nodestatus = PGXCNodePing(connstr);
                }
                break;
                
                default:
                {
                    /* should never happen */
                    abort();
                    break;
                }
            }
            
            /* loop and try to put the warmed connection back into queue */
            while (-1 == PipePut(g_AsynUtilityPipeRcver, (PGXCAsyncWarmInfo*)pWarmInfo))
            {
                pg_usleep(1000);    
            }
        }
                
    }
    
    return NULL;
}

static inline PGXCASyncTaskCtl* create_task_control(List *datanodelist,    List *coordlist, int32 *fd_result, int32 *pid_result)
{
    PGXCASyncTaskCtl *asyncTaskCtl;
    MemoryContext     oldcontext;
    
    oldcontext = MemoryContextSwitchTo(PoolerMemoryContext);
    asyncTaskCtl = (PGXCASyncTaskCtl *) palloc0(sizeof(PGXCASyncTaskCtl));
    MemoryContextSwitchTo(oldcontext);
        
    /* init spinlock */
    SpinLockInit(&asyncTaskCtl->m_lock);
    asyncTaskCtl->m_status         = PoolAyncCtlStaus_init;
    asyncTaskCtl->m_mumber_total   = 0;
    asyncTaskCtl->m_number_done    = 0;
    asyncTaskCtl->m_datanodelist   = datanodelist;
    asyncTaskCtl->m_coordlist      = coordlist;
    asyncTaskCtl->m_result           = fd_result;
    asyncTaskCtl->m_pidresult       = pid_result;
    asyncTaskCtl->m_res               = 0;
    asyncTaskCtl->m_max_command_id = InvalidCommandId;
    asyncTaskCtl->m_error_offset   = 0;
    if (PoolConnectDebugPrint)
    {
        elog(LOG, POOL_MGR_PREFIX"create task controller succeed");
    }
    return asyncTaskCtl;
}

static inline bool dispatch_connection_request(PGXCASyncTaskCtl  *taskControl,
                                                bool               bCoord,
                                                PoolAgent         *agent,
                                                PGXCNodePool      *nodepool,
                                                int32              status,
                                                int32              finStatus,
                                                int32              nodeindex,
                                                int32              reqseq,
                                                bool               dispatched)
                                        
{// #lizard forgives
    bool               ret;
    PGXCPoolAsyncReq *req;
    PGXCNodePoolSlot *slot = NULL;

    if (PoolConnectStaus_init == status && nodepool->size >= MaxPoolSize)
    {
        elog(LOG, POOL_MGR_PREFIX"[dispatch_connection_request] nodepool size:%d equal or larger than MaxPoolSize:%d, "
                                 "pid:%d node:%s nodeindex:%d req_seq:%d, freesize:%d", 
                                 nodepool->size, MaxPoolSize, 
                                 agent->pid, nodepool->node_name, nodeindex, reqseq, nodepool->freeSize);
        /* return failed */
        return false;
    }
    
    req = (PGXCPoolAsyncReq *) palloc0(sizeof(PGXCPoolAsyncReq));
    
    req->cmd                          = 'g';
    req->bCoord                      = bCoord;
    req->agent                        = agent;
    req->taskControl               = taskControl;
    req->nodepool                  = nodepool;
    req->current_status              = status;
    req->final_status                = finStatus;
    req->nodeindex                  = nodeindex;
    req->needfree                  = dispatched;
    req->req_seq                  = reqseq;

    /*record request begin time*/
    if (PoolPrintStatTimeout > 0 && !dispatched)
    {
        gettimeofday(&req->start_time, NULL);
    }

    /* only init stauts need to alloc a slot */
    if (PoolConnectStaus_init == status)
    {
        MemoryContext     oldcontext;
        oldcontext = MemoryContextSwitchTo(PoolerMemoryContext);
        
        /* Allocate new slot */
        slot = (PGXCNodePoolSlot *) palloc0(sizeof(PGXCNodePoolSlot));        
        
        /* set seqnum */
        slot->seqnum     = pooler_get_slot_seq_num();
        slot->pid         = agent->pid;
        req->slot        = slot;
        req->needConnect = true;
        MemoryContextSwitchTo(oldcontext);
        
        if (PoolConnectDebugPrint)
        {
            elog(LOG, POOL_MGR_PREFIX"dispatch async connection pid:%d node:%s nodeindex:%d connection current status:%d final status:%d slot_seq:%d req_seq:%d", agent->pid, nodepool->node_name, nodeindex, req->current_status, req->final_status, req->slot->seqnum, reqseq);
        }

        /* set slot to null */
        nodepool->slot[nodepool->size] = NULL;
        
        IncreasePoolerSize(nodepool,__FILE__,__LINE__);
        IncreaseSlotRefCount(slot,__FILE__,__LINE__);
        
        /* use version to tag every slot */
        slot->m_version = nodepool->m_version;    
		slot->created = time(NULL);
		slot->checked = slot->created;
		slot->released = slot->created;
    }


    /* last request of the session */
    if (dispatched)
    {
        taskControl->m_status     = PoolAyncCtlStaus_dispatched;

		/* also use this request to response to session*/
		req->final_status         = PoolConnectStaus_destory;

		/* if last request, transfer cmd_start_time to req */
        req->cmd_start_time       = agent->cmd_start_time;
	}

    if (PoolConnectDebugPrint)
    {
        elog(LOG, POOL_MGR_PREFIX"dispatch async connection pid:%d node:%s nodeindex:%d connection current status:%d final status:%d req_seq:%d", agent->pid, nodepool->node_name, nodeindex, req->current_status, req->final_status, reqseq);
    }

    if (dispatched)
    {
        if (PoolConnectDebugPrint)
        {
            elog(LOG, POOL_MGR_PREFIX"dispatch last connection request!! pid:%d node:%s nodeindex:%d connection, current status:%d final status:%d request_num:%d req_seq:%d",agent->pid, nodepool->node_name, nodeindex, req->current_status, req->final_status, taskControl->m_mumber_total, reqseq);
        }
    }
    
    ret = dispatch_async_network_operation(req);
    if (!ret)
    {
        elog(LOG, POOL_MGR_PREFIX"dispatch connection request failed!! pid:%d node:%s nodeindex:%d connection, current status:%d final status:%d request_num:%d req_seq:%d", agent->pid, nodepool->node_name, nodeindex, req->current_status, req->final_status, taskControl->m_mumber_total, reqseq);        
        if (slot)
        {
            pfree(slot);
        }        
        pfree(req);        
        
        /* failed, decrease count */
        if (req->needConnect)
        {
            DecreasePoolerSize(nodepool,__FILE__,__LINE__);        
        }
#ifdef __TBASE__
        SpinLockAcquire(&agent->port.lock);
        agent->port.error_code = POOL_ERR_GET_CONNECTIONS_DISPATCH_FAILED;
        snprintf(agent->port.err_msg, POOL_ERR_MSG_LEN, "%s", poolErrorMsg[agent->port.error_code]);
        SpinLockRelease(&agent->port.lock);
#endif
	}
    else
    {
        if (dispatched)
        {
            /* dispatch success, clear cmd start time in agent */
            agent->cmd_start_time = 0;
        }
    }
	return ret;
}


static inline bool dispatch_local_set_request(PGXCASyncTaskCtl  *taskControl,
                                                bool               bCoord,
                                                PoolAgent         *agent,
                                                int32              nodeindex,
                                                bool               dispatched)
                                        
{
    bool              ret;
    PGXCPoolAsyncReq *req;
    req = (PGXCPoolAsyncReq *) palloc0(sizeof(PGXCPoolAsyncReq));
    

    req->cmd                          = 'b';
    req->bCoord                      = bCoord;
    req->agent                        = agent;
    req->taskControl               = taskControl;
    req->nodepool                  = NULL;
    req->current_status              = PoolLocalSetStatus_reset;
    req->final_status             = PoolLocalSetStatus_reset;
    req->nodeindex                  = nodeindex;
    req->needfree                  = dispatched;
    req->error_flag                  = false;
    req->setquery_status          = SendSetQuery_OK;
    

    /* last request of the session */
    if (dispatched)
    {
        taskControl->m_status     = PoolAyncCtlStaus_dispatched;

		/* also use this request to response to session*/
		req->final_status         = PoolLocalSetStatus_destory;
		req->current_status    	  = PoolLocalSetStatus_destory;

        /* if last request, transfer cmd_start_time to req */
        req->cmd_start_time       = agent->cmd_start_time;
	}	
	

    if (PoolConnectDebugPrint)
    {
        elog(LOG, POOL_MGR_PREFIX"pid:%d dispatch async local set nodeindex:%d connection, current status:%d final status:%d", agent->pid, nodeindex, req->current_status, req->final_status);
    }

	if (dispatched)
	{
		if (PoolConnectDebugPrint)
		{
			elog(LOG, POOL_MGR_PREFIX"pid:%d dispatch last local set request!! nodeindex:%d connection, current status:%d final status:%d request_num:%d", agent->pid, nodeindex, req->current_status, req->final_status, taskControl->m_mumber_total);
		}
	}
	ret = dispatch_async_network_operation(req);
	if (!ret)
	{
		elog(LOG, POOL_MGR_PREFIX"pid:%d dispatch async local set request failed!! nodeindex:%d connection, current status:%d final status:%d request_num:%d", agent->pid, nodeindex, req->current_status, req->final_status, taskControl->m_mumber_total);		
		pfree(req);
	}
    else
    {
        if (dispatched)
        {
            /* dispatch success, clear cmd start time in agent */
            agent->cmd_start_time = 0;
        }
    }

	return ret;
}

static inline bool dispatch_set_command_request(PGXCASyncTaskCtl  *taskControl,
                                                bool               bCoord,
                                                PoolAgent         *agent,
                                                int32              nodeindex,
                                                bool               dispatched)
                                        
{// #lizard forgives
    bool              ret;
    PGXCPoolAsyncReq *req  = NULL;
    PGXCNodePoolSlot *slot = NULL;
    if (bCoord)
    {
        slot = agent->coord_connections[nodeindex];
        
    }
    else
    {
        slot = agent->dn_connections[nodeindex];
    }
    
    req = (PGXCPoolAsyncReq *) palloc0(sizeof(PGXCPoolAsyncReq));
    

    req->cmd                          = 's';
    req->bCoord                      = bCoord;
    req->agent                        = agent;
    req->taskControl               = taskControl;
    req->nodepool                  = NULL;
    req->current_status              = PoolSetCommandStatus_set;
    req->final_status                = PoolSetCommandStatus_set;
    req->nodeindex                  = nodeindex;
    req->needfree                  = dispatched;
    req->error_flag                  = false;
    req->setquery_status          = SendSetQuery_OK;

    /* last request of the session */
    if (dispatched)
    {
        taskControl->m_status     = PoolAyncCtlStaus_dispatched;

		/* also use this request to response to session*/
		req->final_status         = PoolSetCommandStatus_destory;
		req->current_status       = PoolSetCommandStatus_destory;

        /* if last request, transfer cmd_start_time to req */
        req->cmd_start_time       = agent->cmd_start_time;
	}	

    if (PoolConnectDebugPrint)
    {
        if (slot)
        {
            elog(LOG, POOL_MGR_PREFIX"pid:%d dispatch async set command nodeindex:%d connection nodename:%s backend_pid:%d current status:%d final status:%d command:%s", agent->pid, nodeindex, slot->node_name, slot->backend_pid, req->current_status, req->final_status, taskControl->m_command);
        }
    }

    if (dispatched)
    {
        if (PoolConnectDebugPrint)
        {
            if (slot)
            {
                elog(LOG, POOL_MGR_PREFIX"pid:%d dispatch last set command request!! nodeindex:%d connection nodename:%s backend_pid:%d current status:%d final status:%d request_num:%d command:%s", agent->pid, nodeindex, slot->node_name, slot->backend_pid,  req->current_status, req->final_status, taskControl->m_mumber_total, taskControl->m_command);
            }
        }
    }

	ret = dispatch_async_network_operation(req); 
	if (!ret)
	{
		if (slot)
		{
			elog(LOG, POOL_MGR_PREFIX"pid:%d dispatch async set command request failed!! nodeindex:%d connection nodename:%s backend_pid:%d current status:%d final status:%d request_num:%d command:%s", agent->pid, nodeindex, slot->node_name, slot->backend_pid, req->current_status, req->final_status, taskControl->m_mumber_total, taskControl->m_command);		
		}
		pfree(req);
	}
    else
    {
        if (dispatched)
        {
            /* dispatch success, clear cmd start time in agent */
            agent->cmd_start_time = 0;
        }
    }

    if (PoolConnectDebugPrint)
    {
        if (slot)
        {
            elog(LOG, POOL_MGR_PREFIX"pid:%d Pooler SetCommand: send to nodeindex:%d  slot->node_name:%s slot->backend_pid:%d with command %s", agent->pid, nodeindex, slot->node_name, slot->backend_pid, taskControl->m_command);
        }
    }
    return ret;
}



static inline bool dispatch_cancle_request(PGXCASyncTaskCtl  *taskControl,
                                            bool               bCoord,
                                            PoolAgent         *agent,
                                            int32              nodeindex,
                                            bool               dispatched,
                                            int                signal)
                                        
{// #lizard forgives
    bool              ret;
    PGXCPoolAsyncReq *req;
    PGXCNodePoolSlot *slot    = NULL;
    req = (PGXCPoolAsyncReq *) palloc0(sizeof(PGXCPoolAsyncReq));
    

    req->cmd                          = 'h';
    req->bCoord                      = bCoord;
    req->agent                        = agent;
    req->taskControl               = taskControl;
    req->nodepool                  = NULL;
    if (signal == SIGNAL_SIGUSR2)
    {
        req->current_status              = PoolCancelStatus_end_query;
        req->final_status                = PoolCancelStatus_end_query;
    }
    else
    {
        req->current_status              = PoolCancelStatus_cancel;
        req->final_status                = PoolCancelStatus_cancel;
    }
    req->nodeindex                  = nodeindex;
    req->needfree                  = dispatched;
    req->error_flag                  = false;
    req->setquery_status          = SendSetQuery_OK;

    /* last request of the session */
    if (dispatched)
    {
        taskControl->m_status     = PoolAyncCtlStaus_dispatched;

		/* use this request to response to session*/
		req->current_status    	  = PoolCancelStatus_destory;
		req->final_status      	  = PoolCancelStatus_destory;

        /* if last request, transfer cmd_start_time to req */
        req->cmd_start_time       = agent->cmd_start_time;
	}
	
	if (bCoord)
	{
		slot = agent->coord_connections[nodeindex];
		
	}
	else
	{
		slot = agent->dn_connections[nodeindex];
	}
							
	if (PoolConnectDebugPrint)
	{
		if (slot)
		{
			elog(LOG, POOL_MGR_PREFIX"pid:%d dispatch async CANCLE_QUERY nodeindex:%d connection, nodename:%s backend_pid:%d current status:%d final status:%d", 
																										    agent->pid, 
																										    nodeindex, 
																										    slot->node_name, 
																										    slot->backend_pid, 
																										    req->current_status, 
																										    req->final_status);
		}
	}

    if (dispatched)
    {
        if (PoolConnectDebugPrint)
        {
            if (slot)
            {
                elog(LOG, POOL_MGR_PREFIX"pid:%d dispatch last CANCLE_QUERY request!! nodeindex:%d connection nodename:%s backend_pid:%d current status:%d final status:%d request_num:%d", 
                                                                                                            agent->pid, 
                                                                                                            nodeindex, 
                                                                                                            slot->node_name, 
                                                                                                            slot->backend_pid, 
                                                                                                            req->current_status, 
                                                                                                            req->final_status, 
                                                                                                            taskControl->m_mumber_total);
            }
        }
    }
    
    ret = dispatch_async_network_operation(req); 
    if (!ret)
    {
        if (slot)
        {
            elog(LOG, POOL_MGR_PREFIX"pid:%d dispatch async CANCLE_QUERY failed!! nodeindex:%d connection nodename:%s backend_pid:%d current status:%d final status:%d request_num:%d", 
                                                                                                            agent->pid, 
                                                                                                            nodeindex, 
                                                                                                            slot->node_name, 
                                                                                                            slot->backend_pid, 
                                                                                                            req->current_status, 
                                                                                                            req->final_status, 
                                                                                                            taskControl->m_mumber_total);        
        }
        pfree(req);
#ifdef __TBASE__
        SpinLockAcquire(&agent->port.lock);
        agent->port.error_code = POOL_ERR_CANCEL_DISPATCH_FAILED;
        snprintf(agent->port.err_msg, POOL_ERR_MSG_LEN, "%s", poolErrorMsg[agent->port.error_code]);
        SpinLockRelease(&agent->port.lock);
#endif
	}
    else
    {
        if (dispatched)
        {
            /* dispatch success, clear cmd start time in agent */
            agent->cmd_start_time = 0;
        }
    }

	return ret;
}


static inline bool dispatch_reset_request(PGXCASyncTaskCtl  *taskControl,
                                            bool               bCoord,
                                            PoolAgent         *agent,
                                            int32              status,    /* PoolResetStatus */
                                            int32              nodeindex,
                                            bool               dispatched)
                                        
{
    Oid               node;
    PGXCNodePoolSlot *slot;
    bool               ret;
    PGXCPoolAsyncReq *req;
    req = (PGXCPoolAsyncReq *) palloc0(sizeof(PGXCPoolAsyncReq));

    req->cmd                          = 'd';
    req->bCoord                      = bCoord;
    req->agent                        = agent;
    req->taskControl               = taskControl;
    req->current_status              = status;
    req->nodeindex                  = nodeindex;
    req->needfree                  = dispatched;
    req->error_flag                  = false;
    req->setquery_status          = SendSetQuery_OK;
    
    if (PoolConnectDebugPrint)
    {
        elog(LOG, POOL_MGR_PREFIX"pid:%d dispatch async reset session bCoord:%d nodeindex:%d connection, current status:%d", agent->pid, bCoord, nodeindex, req->current_status);
    }
    
    ret = dispatch_async_network_operation(req); 
    if (!ret)
    {
        /* we fail to reset the connection, just release it */
        elog(LOG, POOL_MGR_PREFIX"pid:%d dispatch async reset session request failed!! nodeindex:%d connection, current status:%d final status:%d request_num:%d;just free it", agent->pid, nodeindex, req->current_status, req->final_status, taskControl->m_mumber_total);                
        pfree(req);
        
        /* when PoolResetStatus_destory, we don't have a slot to destory */
        if (status != PoolResetStatus_destory)
        {            
            if (bCoord)
            {
                node = agent->coord_conn_oids[nodeindex];
                slot = agent->coord_connections[nodeindex];
                agent->coord_connections[nodeindex] = NULL;
            }
            else
            {
                node = agent->dn_conn_oids[nodeindex];
                slot = agent->dn_connections[nodeindex];
                agent->dn_connections[nodeindex] = NULL;
            }
            if (PoolConnectDebugPrint)
            {
                elog(LOG, POOL_MGR_PREFIX"++++dispatch_reset_request pid:%d release slot_seq:%d++++", agent->pid, slot->seqnum);
            }
			release_connection(agent->pool, slot, nodeindex, node, false, bCoord, false);
        }
    }
    return ret;
}


static inline void finish_task_request(PGXCASyncTaskCtl  *taskControl)                                        
{    
    /* remember how many request we have dispatched */
    SpinLockAcquire(&taskControl->m_lock);
    taskControl->m_number_done++;
    SpinLockRelease(&taskControl->m_lock);
}

static inline void set_task_status(PGXCASyncTaskCtl  *taskControl, int32 status)                                        
{        
    SpinLockAcquire(&taskControl->m_lock);
    taskControl->m_status = status;
    SpinLockRelease(&taskControl->m_lock);
}
static inline int32 get_task_status(PGXCASyncTaskCtl  *taskControl)
{
    return taskControl->m_status;
}
static inline bool check_is_task_done(PGXCASyncTaskCtl  *taskControl)                                        
{    
    bool done = false;
    /* remember how many request we have dispatched */
    SpinLockAcquire(&taskControl->m_lock);
    done = taskControl->m_number_done == taskControl->m_mumber_total;
    SpinLockRelease(&taskControl->m_lock);
    return done;
}
static inline void add_task_result(PGXCASyncTaskCtl  *taskControl, int32 res)                                        
{    
    /* remember how many request we have dispatched */
    SpinLockAcquire(&taskControl->m_lock);
    taskControl->m_res += res;
    SpinLockRelease(&taskControl->m_lock);
}
static inline int32 get_task_result(PGXCASyncTaskCtl  *taskControl)                                        
{    
    return taskControl->m_res;
}

static inline void set_command_total(PGXCASyncTaskCtl  *taskControl, int32 number)                                        
{        
    SpinLockAcquire(&taskControl->m_lock);
    taskControl->m_total = number;
    SpinLockRelease(&taskControl->m_lock);
}

static inline void set_command_increase_succeed(PGXCASyncTaskCtl  *taskControl)                                        
{        
    SpinLockAcquire(&taskControl->m_lock);
    taskControl->m_succeed++;
    SpinLockRelease(&taskControl->m_lock);
}

static inline bool get_command_success_status(PGXCASyncTaskCtl  *taskControl)                        
{        
    bool bsucceed;
    SpinLockAcquire(&taskControl->m_lock);
    bsucceed = taskControl->m_succeed == taskControl->m_total;
    SpinLockRelease(&taskControl->m_lock);
    return bsucceed;
}


static inline void acquire_command_increase_succeed(PGXCASyncTaskCtl  *taskControl)                                        
{        
    SpinLockAcquire(&taskControl->m_lock);
    taskControl->m_number_succeed++;
    SpinLockRelease(&taskControl->m_lock);
}


static inline bool get_acquire_success_status(PGXCASyncTaskCtl  *taskControl)                        
{        
    bool bsucceed;
    SpinLockAcquire(&taskControl->m_lock);
	bsucceed = taskControl->m_number_done == taskControl->m_number_succeed ||
	           taskControl->m_missing_ok;
    SpinLockRelease(&taskControl->m_lock);
    return bsucceed;
}

static inline void set_task_max_command_id(PGXCASyncTaskCtl  *taskControl, CommandId id)                                        
{        
    if (InvalidCommandId == id)
    {
        return;
    }
    
    SpinLockAcquire(&taskControl->m_lock);    
    taskControl->m_max_command_id = (InvalidCommandId == taskControl->m_max_command_id) ? id : (id > taskControl->m_max_command_id ? id : taskControl->m_max_command_id);
    SpinLockRelease(&taskControl->m_lock);
}

static inline void set_task_error_msg(PGXCASyncTaskCtl  *taskControl, char *error)                                        
{
    SpinLockAcquire(&taskControl->m_lock);    
    taskControl->m_error_offset += snprintf(taskControl->m_error_msg + taskControl->m_error_offset, PGXC_POOL_ERROR_MSG_LEN - taskControl->m_error_offset, "%s", error);
    SpinLockRelease(&taskControl->m_lock);
}

static inline CommandId get_task_max_commandID(PGXCASyncTaskCtl  *taskControl)                        
{            
    return taskControl->m_max_command_id;
}


static void create_node_map(void)
{
    MemoryContext    oldcontext;
    PGXCMapNode     *node      = NULL;
    NodeDefinition  *node_def = NULL;
    HASHCTL            hinfo;
    int                hflags;

    Oid               *coOids;
    Oid               *dnOids;
    int                numCo;
    int                numDn;
    int             nodeindex;
    bool            found;
    
    oldcontext = MemoryContextSwitchTo(PoolerMemoryContext);
    /* Init node hashtable */
    MemSet(&hinfo, 0, sizeof(hinfo));
    hflags = 0;

    hinfo.keysize = sizeof(Oid);
    hinfo.entrysize = sizeof(PGXCMapNode);
    hflags |= HASH_ELEM;

    hinfo.hcxt = PoolerMemoryContext;
    hflags |= HASH_CONTEXT;    

    hinfo.hash = oid_hash;
    hflags |= HASH_FUNCTION;

    g_nodemap = hash_create("Node Map Hash", TBASE_MAX_DATANODE_NUMBER + TBASE_MAX_COORDINATOR_NUMBER,
                                              &hinfo, hflags);    

    PgxcNodeGetOids(&coOids, &dnOids, &numCo, &numDn, false);

    for (nodeindex = 0; nodeindex < numCo; nodeindex++)
    { 
        node = (PGXCMapNode *) hash_search(g_nodemap, &coOids[nodeindex],
                                            HASH_ENTER, &found);    
        if (found)
        {
            elog(ERROR, POOL_MGR_PREFIX"node:%u duplicate in hash table", coOids[nodeindex]);
        }
        
        if (node)
        {            
            node->node_type = PGXC_NODE_COORDINATOR;
            node->nodeidx = nodeindex;

            node_def = PgxcNodeGetDefinition(coOids[nodeindex]);
            snprintf(node->node_name, NAMEDATALEN, "%s", NameStr(node_def->nodename));
        }
    }    

    for (nodeindex = 0; nodeindex < numDn; nodeindex++)
    {
 
        node = (PGXCMapNode *) hash_search(g_nodemap, &dnOids[nodeindex],
                                            HASH_ENTER, &found);    
        if (found)
        {
            elog(ERROR, POOL_MGR_PREFIX"node:%u duplicate in hash table", dnOids[nodeindex]);
        }
        
        if (node)
        {
            node->node_type = PGXC_NODE_DATANODE;
            node->nodeidx = nodeindex;

            node_def = PgxcNodeGetDefinition(dnOids[nodeindex]);
            snprintf(node->node_name, NAMEDATALEN, "%s", NameStr(node_def->nodename));
        }
    }    

    MemoryContextSwitchTo(oldcontext);
    /* Release palloc'ed memory */
    pfree(coOids);
    pfree(dnOids);
}

static void refresh_node_map(void)
{
    PGXCMapNode     *node;
    Oid            *coOids;
    Oid            *dnOids;    
    NodeDefinition  *node_def = NULL;
    int             numCo;
    int             numDn;
    int             nodeindex;
    bool            found;
#ifdef __TBASE__
	bool           inited = false;
#endif

    if (NULL == g_nodemap)
    {
        /* init node map */
        create_node_map();
#ifdef __TBASE__
		inited = true;
#endif
    }
#ifdef __TBASE__
	/* reset hashtab */
	if (!inited)
	{
		HASH_SEQ_STATUS scan_status;
		PGXCMapNode  *item;

		hash_seq_init(&scan_status, g_nodemap);
		while ((item = (PGXCMapNode *) hash_seq_search(&scan_status)) != NULL)
		{
			if (hash_search(g_nodemap, (const void *) &item->nodeoid,
							HASH_REMOVE, NULL) == NULL)
			{
				elog(ERROR, POOL_MGR_PREFIX"node map hash table corrupted");
			}
		}
	}
#endif
    PgxcNodeGetOids(&coOids, &dnOids, &numCo, &numDn, false);

    for (nodeindex = 0; nodeindex < numCo; nodeindex++)
    { 
        node = (PGXCMapNode *) hash_search(g_nodemap, &coOids[nodeindex],
                                            HASH_ENTER, &found);        
        if (node)
        {
            node->nodeidx   = nodeindex;
            node->node_type = PGXC_NODE_COORDINATOR;

            node_def = PgxcNodeGetDefinition(coOids[nodeindex]);
            snprintf(node->node_name, NAMEDATALEN, "%s", NameStr(node_def->nodename));
        }
    }    

    for (nodeindex = 0; nodeindex < numDn; nodeindex++)
    {
 
        node = (PGXCMapNode *) hash_search(g_nodemap, &dnOids[nodeindex],
                                            HASH_ENTER, &found);    
        if (node)
        {
            node->nodeidx = nodeindex;
            node->node_type = PGXC_NODE_DATANODE;

            node_def = PgxcNodeGetDefinition(dnOids[nodeindex]);
            snprintf(node->node_name, NAMEDATALEN, "%s", NameStr(node_def->nodename));
        }
    }    

    /* Release palloc'ed memory */
    pfree(coOids);
    pfree(dnOids);
}

static int32 get_node_index_by_nodeoid(Oid node)
{
    PGXCMapNode     *entry = NULL;    
    bool            found  = false;
    
    if (NULL == g_nodemap)
    {
        /* init node map */
        create_node_map();
    }
    
    entry = (PGXCMapNode *) hash_search(g_nodemap, &node, HASH_FIND, &found);        
    if (found)
    {
        return entry->nodeidx;
    }
    else
    {
        elog(ERROR, POOL_MGR_PREFIX"query node type by oid:%u failed", node);
        return -1;
    }
}


static char* get_node_name_by_nodeoid(Oid node)
{
    PGXCMapNode     *entry = NULL;    
    bool            found  = false;
    
    if (NULL == g_nodemap)
    {
        /* init node map */
        create_node_map();
    }
    
    entry = (PGXCMapNode *) hash_search(g_nodemap, &node, HASH_FIND, &found);        
    if (found)
    {
        return entry->node_name;
    }
    else
    {
        elog(LOG, POOL_MGR_PREFIX"query node name by oid:%u failed", node);
        return NULL;
    }
}


static int32 get_node_info_by_nodeoid(Oid node, char *type)
{
    PGXCMapNode     *entry = NULL;    
    bool            found  = false;
    
    if (NULL == g_nodemap)
    {
        /* init node map */
        create_node_map();
    }
    
    entry = (PGXCMapNode *) hash_search(g_nodemap, &node, HASH_FIND, &found);        
    if (found)
    {
        *type = entry->node_type;
        return entry->nodeidx;
    }
    else
    {
        elog(ERROR, POOL_MGR_PREFIX"query node index by oid:%u failed", node);
        return -1;
    }
}


static inline bool pooler_is_async_task_done(void)
{
    int32 threadIndex = 0;

    if (!IsEmpty(g_AsynUtilityPipeRcver))
    {
        return false;
    }

    threadIndex = 0;
    while (threadIndex < MAX_SYNC_NETWORK_THREAD)
    {
        if (!IsEmpty(g_PoolConnControl.response[threadIndex]))
        {
            return false;
        }
        threadIndex++;
    }

    threadIndex = 0;
    while (threadIndex < MAX_SYNC_NETWORK_THREAD)
    {
        if (!IsEmpty(g_PoolSyncNetworkControl.response[threadIndex]))
        {
            return false;
        }
        threadIndex++;
    }
    return true;
}

static inline bool pooler_wait_for_async_task_done(void)
{
    bool  bdone    = false;
    int32 loop_num = 0;

    /* wait a little while */
    do
    {    
        bdone = pooler_is_async_task_done();
        if (bdone)
        {
            break;
        }
        pg_usleep(1000L);
        pooler_sync_connections_to_nodepool();
        pooler_handle_sync_response_queue();    
        loop_num++;
    }while (!bdone && loop_num < 100);
    
    return bdone;
}

static inline void pooler_async_task_start(PGXCPoolSyncNetWorkControl *control, int32 thread, int32 nodeindex, PGXCNodePoolSlot *slot, Oid nodeoid, int cmdtype)
{
    control->status[thread]      = PoolAsyncStatus_busy;
    control->nodeindex[thread]     = nodeindex;
    control->start_stamp[thread] = time(NULL);
    control->cmdtype[thread]     = cmdtype;

    record_slot_info(control, thread, slot, nodeoid);
}

static inline void pooler_async_task_done(PGXCPoolSyncNetWorkControl *control, int32 thread)
{
    control->status[thread]      = PoolAsyncStatus_idle;
    control->nodeindex[thread]     = -1;
    control->start_stamp[thread] = 0;

    if (control->remote_ip[thread])
    {
        free(control->remote_ip[thread]);
        control->remote_ip[thread] = NULL;
    }

    control->remote_backend_pid[thread]  = -1;
    control->remote_port[thread]         = -1;
    control->remote_nodeoid[thread]         = InvalidOid;
    control->cmdtype[thread]             = -1;
    if (control->message[thread])
    {
        free(control->message[thread]);
        control->message[thread] = NULL;
    }
}

static inline int32 pooler_async_task_pick_thread(PGXCPoolSyncNetWorkControl *control, int32 nodeindex)
{// #lizard forgives
    int32 loop_count   = 0;
    pg_time_t gap      = 0;
    pg_time_t now      = 0;
    pg_time_t stamp    = 0;
    int32 thread_index = 0;

    thread_index = nodeindex % MAX_SYNC_NETWORK_THREAD;
    /* find a avaliable thread */
    now = time(NULL);
    while (loop_count < MAX_SYNC_NETWORK_THREAD)
    {

        /* for test */
        if (PoolConnectDebugPrint)
        {
            elog(LOG, POOL_MGR_PREFIX"pooler_async_task_pick_thread loop_count=%d, thread_index=%d, control->status[thread_index]=%d, nodeindex=%d, "
                                     "request_pipelength=%d, response_pipelength=%d, "
                                     "remote_ip:%s, remote_port:%d, remote_nodeoid:%d, remote_backend_pid:%d",
                                              loop_count,    
                                             thread_index,
                                             control->status[thread_index],
                                             nodeindex,
                                             PipeLength(control->request[thread_index]),
                                             PipeLength(control->response[thread_index]),
                                             control->remote_ip[thread_index], 
                                             control->remote_port[thread_index], 
                                             control->remote_nodeoid[thread_index],
                                             control->remote_backend_pid[thread_index]);
        }
    
        /* pipe is not full and in idle status, OK to return */
        if (PoolAsyncStatus_idle == control->status[thread_index] && !PipeIsFull(control->request[thread_index]))
        {            
            if (PoolConnectDebugPrint)
            {
                elog(LOG, POOL_MGR_PREFIX"pooler_async_task_pick_thread use thread index:%d nodeindex:%d in idle status ", thread_index, nodeindex);
            }
            return thread_index;
        }
        else
        {
            stamp = control->start_stamp[thread_index];
            gap   = stamp ? (now - stamp) : 0;
            if (gap  >= PoolConnectTimeOut)
            {                
                /* nodeindex is stuck in the thread */
                if (nodeindex == control->nodeindex[thread_index])
                {
                    elog(LOG, POOL_MGR_PREFIX"pooler_async_task_pick_thread thread index:%d nodeindex:%d got stuck for %ld seconds, cmdtype=%d, message=%s", 
                                             thread_index, nodeindex, gap, control->cmdtype[thread_index], control->message[thread_index]);
                    elog(LOG, POOL_MGR_PREFIX"pooler_async_task_pick_thread stuck at remote_ip:%s, remote_port:%d, remote_nodeoid:%d, remote remote_backend_pid:%d",
                                             control->remote_ip[thread_index], 
                                             control->remote_port[thread_index], 
                                             control->remote_nodeoid[thread_index],
                                             control->remote_backend_pid[thread_index]);
                }                
            }
            else if (!PipeIsFull(control->request[thread_index])) 
            {
                /* not stuck too long and the pipe has space, choose it */
                if (gap)
                {
                    elog(LOG, POOL_MGR_PREFIX"pooler_async_task_pick_thread use thread index:%d nodeindex:%d in busy status, duration:%ld", thread_index, nodeindex, gap);
                }
                return thread_index;
            }
        }
        loop_count++;
        
        thread_index++;
        thread_index = thread_index % MAX_SYNC_NETWORK_THREAD;
    }
    
    /* can't find a avaliable thread*/
    if (PoolerStuckExit)
    {
        elog(FATAL, POOL_MGR_PREFIX"fail to pick a thread for operation nodeindex:%d, pooler exit", nodeindex);    
    }
    else
    {
        elog(LOG, POOL_MGR_PREFIX"fail to pick a thread for operation nodeindex:%d", nodeindex);    
    }
    return -1;
}

static inline void agent_increase_ref_count(PoolAgent *agent)
{
     agent->ref_count++;
}

static inline void agent_decrease_ref_count(PoolAgent *agent)
{
     agent->ref_count--;
}

static inline bool agent_can_destory(PoolAgent *agent)
{
     return 0 == agent->ref_count;
}

static inline void agent_pend_destory(PoolAgent *agent)
{
     elog(LOG, POOL_MGR_PREFIX"agent_pend_destory end, ref_count:%d, pid:%d", agent->ref_count, agent->pid);
     agent->destory_pending = true;
}


static inline void agent_set_destory(PoolAgent *agent)
{
     agent->destory_pending = false;
}

static inline bool agent_pending(PoolAgent *agent)
{
     return agent->destory_pending;
}

static inline void agent_handle_pending_agent(PoolAgent *agent)
{
    if (agent)
    {
        if (agent_pending(agent) && agent_can_destory(agent))
        {
            destroy_pend_agent(agent);
        }
    }
}

static inline void pooler_init_sync_control(PGXCPoolSyncNetWorkControl *control)
{    
    control->request        = (PGPipe**)palloc0(MAX_SYNC_NETWORK_THREAD * sizeof(PGPipe*));
    control->response       = (PGPipe**)palloc0(MAX_SYNC_NETWORK_THREAD * sizeof(PGPipe*));
    control->sem            = (ThreadSema*)palloc0(MAX_SYNC_NETWORK_THREAD * sizeof(ThreadSema));
    control->nodeindex      = (int32*)palloc0(MAX_SYNC_NETWORK_THREAD * sizeof(int32));
    control->status         = (int32*)palloc0(MAX_SYNC_NETWORK_THREAD * sizeof(int32));
    control->start_stamp    = (pg_time_t*)palloc0(MAX_SYNC_NETWORK_THREAD * sizeof(pg_time_t));

    control->remote_backend_pid = (int32*)palloc0(MAX_SYNC_NETWORK_THREAD * sizeof(int32));
    control->remote_nodeoid        = (Oid*)palloc0(MAX_SYNC_NETWORK_THREAD * sizeof(Oid));
    control->remote_ip            = (char**)palloc0(MAX_SYNC_NETWORK_THREAD * sizeof(char*));
    control->remote_port         = (int32*)palloc0(MAX_SYNC_NETWORK_THREAD * sizeof(int32));
    control->message            = (char**)palloc0(MAX_SYNC_NETWORK_THREAD * sizeof(char*));
    control->cmdtype            = (int32*)palloc0(MAX_SYNC_NETWORK_THREAD * sizeof(int32));
}
/* generate a sequence number for slot */
static inline int32 pooler_get_slot_seq_num(void)
{
    return g_Slot_Seqnum++;
}
/* get connection acquire sequence number */
static inline int32 pooler_get_connection_acquire_num(void)
{
    return g_Connection_Acquire_Count++;
}

/* bitmap index occupytation management */
BitmapMgr *BmpMgrCreate(uint32 objnum)
{
    BitmapMgr *mgr = NULL;

    mgr = (BitmapMgr*)palloc0(sizeof(BitmapMgr));
    
    mgr->m_fsmlen  = DIVIDE_UP(objnum, BITS_IN_LONGLONG);
    mgr->m_nobject = mgr->m_fsmlen * BITS_IN_LONGLONG;  /* align the object number to 64 */   
    
    mgr->m_fsm = palloc(sizeof(uint64) * mgr->m_fsmlen);
    
    /* zero the FSM memory */
    memset(mgr->m_fsm, 0X00, sizeof(uint64) * mgr->m_fsmlen);    
             
    mgr->m_version  = INIT_VERSION;
    return mgr;
};

/* find an unused index and return it */
int BmpMgrAlloc(BitmapMgr *mgr)
{// #lizard forgives     
    uint32  i       = 0;
    uint32  j         = 0;
    uint32  k        = 0;
    int     offset  = 0;    
    uint8   *ucaddr = NULL;
    uint32  *uiaddr = NULL;
    
    /* sequencial scan the FSM map */
    for (i = 0; i < mgr->m_fsmlen; i++)
    {          
        /* got free space */ 
        if (mgr->m_fsm[i] < MAX_UINT64)
        {
            /* first word has free space */
            uiaddr = (uint32*)&(mgr->m_fsm[i]);
            if (uiaddr[0] < MAX_UINT32)
            {
                ucaddr = (uint8*)&uiaddr[0];
                offset = 0;
            }
            else
            {
                /* in second word */
                ucaddr = (uint8*)&uiaddr[1];
                offset = BITS_IN_WORD;
            }
            
            /* find free space */                    
            for (j = 0; j < sizeof(uint32); j++)
            {
                if (ucaddr[j] < MAX_UINT8)
                {
                    for (k = 0; k < BITS_IN_BYTE; k++)
                    {
                        if (BIT_CLEAR(ucaddr[j], k))
                        {
                            SET_BIT(ucaddr[j], k);    
                            mgr->m_version++;
                            return offset + i * BITS_IN_LONGLONG + j * BITS_IN_BYTE + k;
                        }
                    }
                }
            }
        }               
    }
    
    /* out of space */            
    return -1;
}

/* free index */
void BmpMgrFree(BitmapMgr *mgr, int index)
{
    uint32  fsmidx = 0;
    uint32  offset = 0;
    uint8  *pAddr  = NULL;
    
    if (index < 0 || (uint32)index >= mgr->m_nobject)
    {
        elog(PANIC, POOL_MGR_PREFIX"invalid index:%d", index);
    }            

    offset = index % BITS_IN_LONGLONG;
    fsmidx = index / BITS_IN_LONGLONG;
     
    pAddr  = (uint8*)&(mgr->m_fsm[fsmidx]);            
    CLEAR_BIT(pAddr[offset / BITS_IN_BYTE], offset % BITS_IN_BYTE);
    mgr->m_version++;
} 

bool BmpMgrHasIndexAndClear(BitmapMgr *mgr, int index)
{
    uint32  fsmidx = 0;
    uint32  offset = 0;
    uint8  *pAddr  = NULL;
    
    if (index < 0 || (uint32)index >= mgr->m_nobject)
    {
        elog(PANIC, POOL_MGR_PREFIX"invalid index:%d", index);
    }            

    offset = index % BITS_IN_LONGLONG;
    fsmidx = index / BITS_IN_LONGLONG;
     
    pAddr  = (uint8*)&(mgr->m_fsm[fsmidx]);            
    if( BIT_SET(pAddr[offset / BITS_IN_BYTE], offset % BITS_IN_BYTE))
    {
        CLEAR_BIT(pAddr[offset / BITS_IN_BYTE], offset % BITS_IN_BYTE);
        mgr->m_version++;
        return true;
    }
    return false;
} 


uint32 BmpMgrGetVersion(BitmapMgr *mgr)
{
    return mgr->m_version;
}

int BmpMgrGetUsed(BitmapMgr *mgr, int32 *indexes, int32 indexlen)
{// #lizard forgives    
    uint32   u64num  = 0;
    uint32   u32num  = 0;
    uint32   u8num   = 0;
    uint32   ubitnum = 0;
    int32    number  = 0;
    uint8   *ucaddr  = NULL;
    uint32  *uiaddr  = NULL;
    
    if (indexlen != mgr->m_nobject)
    {
        return -1;
    }
   
    /* scan the array */
    for (u64num = 0; u64num < mgr->m_fsmlen; u64num++)
    {
        if (mgr->m_fsm[u64num])
        {
            uiaddr = (uint32*)&(mgr->m_fsm[u64num]);            
            for (u32num = 0; u32num < sizeof(uint64)/sizeof(uint32); u32num++)
            {    
                if (uiaddr[u32num])
                {
                    ucaddr = (uint8*)&uiaddr[u32num];                
                                        
                    for (u8num = 0; u8num < sizeof(uint32); u8num++)
                    {
                        if (ucaddr[u8num])
                        {
                            for (ubitnum = 0; ubitnum < BITS_IN_BYTE; ubitnum++)
                            {
                                if (BIT_SET(ucaddr[u8num], ubitnum))
                                {
                                    indexes[number] = u64num * BITS_IN_LONGLONG + BITS_IN_WORD * u32num + u8num * BITS_IN_BYTE + ubitnum;
                                    number++;
                                }
                            }
                        }
                    }
                }
            }
        }               
    }
         
    return number;
}

static void record_slot_info(PGXCPoolSyncNetWorkControl *control, int32 thread, PGXCNodePoolSlot *slot, Oid nodeoid)
{// #lizard forgives
    struct pg_conn* conn = NULL;
    
    if (!control || thread < 0)
        return;

    if (slot && slot->conn)
    {
        conn = (struct pg_conn*)slot->conn;
        control->remote_backend_pid[thread] = conn->be_pid;
        control->remote_port[thread]        = atoi(conn->pgport);
        control->remote_nodeoid[thread]        = nodeoid;
        
        if (conn->pghostaddr && conn->pghostaddr[0] != '\0')
        {
            control->remote_ip[thread] = strdup(conn->pghostaddr);
        }
        else if (conn->pghost && conn->pghost[0] != '\0')
        {
            control->remote_ip[thread] = strdup(conn->pghost);
        }
    }
}


/* 
 * Need to print slot info when conn is stucked. 
 * But we can not get the valid slot info under all circumstances.
 * So, we choose the circumstances which indeed use the slot and slot->conn to 
 * connect to and execute sql on remote cn/dn,  and return true.
 * other cases, return false.
 */
static bool is_slot_avail(PGXCPoolAsyncReq* request)
{// #lizard forgives
    bool ret = false;
    switch (request->cmd)
    {
        case 'b':
            if (PoolLocalSetStatus_reset == request->final_status)
            {
                ret = true;
            }
            break;

        case 'd':
            if (PoolResetStatus_reset == request->current_status)
            {
                ret = true;
            }
            break;
            
        case 'g':
            /* should set inside pooler_sync_remote_operator_thread  */
            break;

        case 'h':
            if (PoolCancelStatus_cancel == request->final_status ||
                PoolCancelStatus_end_query == request->final_status)
            {
                ret = true;
            }
            break;

        case 's':
            if (PoolSetCommandStatus_set == request->current_status)
            {
                ret = true;
            }
            break;

        default:
            break;
    }
    
    return ret;
}

/* When slot is obtained by agent, slot should not exists in nodepool */
#ifdef _POOLER_CHECK_
static void check_pooler_slot(void)
{
    do_check_pooler_slot();
}

static void do_check_pooler_slot(void)
{// #lizard forgives

    int i;
    int j;
    int index;
    PoolAgent              *agent             = NULL;
    PGXCNodePoolSlot     *slot             = NULL;
    
    for (i = 0; i < agentCount; i++)
    {
        index = agentIndexes[i];
        agent = poolAgents[index];

        if (NULL == agent)
            continue;
        
        

        /* loop through all cn slots */
        for (j = 0; j < agent->num_coord_connections; j++)
        {
            if (agent->coord_connections)
            {
                slot = agent->coord_connections[j];
                if (slot)
                {
                    check_single_slot(slot);
                }
            }
        }

        /* loop through all dn slots */
        for (j = 0; j < agent->num_dn_connections; j++)
        {
            if (agent->dn_connections)
            {
                slot = agent->dn_connections[j];
                if (slot)
                {
                    check_single_slot(slot);
                }
            }
            
        }
    }

}

static void check_agent_duplicate_conn(int32 agent_index, bool coord, int32 node_index, int32 pid)
{// #lizard forgives
    int  i;
    int  j;
    int  index                            = 0;
    PoolAgent              *agent             = NULL;
    PGXCNodePoolSlot     *slot             = NULL;
    for (i = 0; i < agentCount; i++)
    {
        index = agentIndexes[i];
        agent = poolAgents[index];

        if (NULL == agent)
        {
            continue;
        }    
        

        /* loop through all dn slots */
        for (j = 0; j < agent->num_dn_connections; j++)
        {
            if (agent->dn_connections)
            {
                if (agent_index == index && j == node_index && !coord)
                {
                    continue;
                }
                
                slot = agent->dn_connections[j];
                if (slot)     
                {
                    if (((PGconn *) slot->conn)->be_pid == pid)
                    {
                        abort();
                    }
                }
            }            
        }

        /* loop through all cn slots */
        for (j = 0; j < agent->num_coord_connections; j++)
        {
            if (agent->coord_connections)
            {
                if (agent_index == index && j == node_index && coord)
                {
                    continue;
                }
                
                slot = agent->coord_connections[j];
                if (slot)     
                {
                    if (((PGconn *) slot->conn)->be_pid == pid)
                    {
                        abort();
                    }
                }
            }
        }
    }
}    

static void check_duplicate_allocated_conn(void)
{// #lizard forgives
    int                 i;
    int                 j;
    int                 index;
    PoolAgent              *agent             = NULL;
    PGXCNodePoolSlot     *slot             = NULL;    
    
    for (i = 0; i < agentCount; i++)
    {
        index = agentIndexes[i];
        agent = poolAgents[index];

        if (NULL == agent)
        {
            continue;
        }
        

        /* loop through all dn slots */
        for (j = 0; j < agent->num_dn_connections; j++)
        {
            if (agent->dn_connections)
            {
                slot = agent->dn_connections[j];
                if (slot)
                {
                    check_agent_duplicate_conn(index, false, j, ((PGconn *) slot->conn)->be_pid);
                }
            }            
        }

        /* loop through all cn slots */
        for (j = 0; j < agent->num_coord_connections; j++)
        {
            if (agent->coord_connections)
            {
                slot = agent->coord_connections[j];
                if (slot)
                {
                    check_agent_duplicate_conn(index, true, j, ((PGconn *) slot->conn)->be_pid);
                }
            }
        }

    }
}

static void check_single_slot(PGXCNodePoolSlot *slot)
{// #lizard forgives
    static DatabasePool *db_pool                = NULL;
    HASH_SEQ_STATUS     hseq_status;
    PGXCNodePool         *nodePool             = NULL;
    int                 i                     = 0;
    PGXCNodePoolSlot     *slot_in_nodepool    = NULL;

    if (NULL == slot)
    {
        return ;
    }

    db_pool = databasePools;    
    while (db_pool)
    {
        hash_seq_init(&hseq_status, db_pool->nodePools);
        while ((nodePool = (PGXCNodePool *) hash_seq_search(&hseq_status)))
        {
            for (i = 0; i < nodePool->size; i++)
            {
                if (i >= nodePool->freeSize)
                {
                    if (NULL != nodePool->slot[i])
                    {
                        elog(LOG, "non-empty slot found");
                        abort();
                    }
                    continue;
                }
            
                slot_in_nodepool = nodePool->slot[i];
                /* Invalid situation */
                if (slot_in_nodepool && slot == slot_in_nodepool)
                {
                    print_pooler_slot(slot);
                    /* should abort? */
                    abort();
                }
            }
        }
        db_pool = db_pool->next;
    }
}


static void print_pooler_slot(PGXCNodePoolSlot  *slot)
{
    struct pg_conn* conn = NULL;
    char          * host = NULL;

    if (!slot)
    {
        elog(LOG, "[error]empty slot");
    }
    else
    {
		elog(LOG, "slot=%p bwarmed=%d usecount=%d refcount=%d m_version="INT64_FORMAT" pid=%d seqnum=%d "
                  "bdestoryed=%d file=%s lineno=%d node_name=%s backend_pid=%d",
                  slot, slot->bwarmed,
                  slot->usecount, slot->refcount,slot->m_version,slot->pid,slot->seqnum,
                  slot->bdestoryed,slot->file, slot->lineno,
                  slot->node_name, slot->backend_pid);
        /* print conn info */
        conn = (struct pg_conn*)slot->conn;
        if (conn)
        {
            if (conn->pghostaddr && conn->pghostaddr[0] != '\0')
            {
                host = conn->pghostaddr;
            }
            else if (conn->pghost && conn->pghost[0] != '\0')
            {
                host = conn->pghost;
            }

            elog(LOG, "slot=%p, remote_ip=%s, remote_port=%s, remote_backend_pid=%d",
                      slot, host, conn->pgport, conn->be_pid);
        }
    }
}

static void check_hashtab_slots(void)
{// #lizard forgives
    DatabasePool         *db_pool            = NULL;
    HASH_SEQ_STATUS     hseq_status;
    PGXCNodePool         *nodePool             = NULL;
    PGXCNodePoolSlot     *slot_in_nodepool    = NULL;

    db_pool = databasePools;    
    while (db_pool)
    {
        hash_seq_init(&hseq_status, db_pool->nodePools);
        while ((nodePool = (PGXCNodePool *) hash_seq_search(&hseq_status)))
        {
            /* Self check. */
            int i = 0;
            int j = 0;
            PGXCNodePoolSlot *inter_slot = NULL;
            for (i = 0; i < nodePool->freeSize; i++)
            {
                inter_slot = nodePool->slot[i];
                for (j = 0; j < nodePool->freeSize; j++)
                {
                    if (inter_slot == nodePool->slot[j] && i != j)
                    {
                        abort();
                    }
                }    
            }
            
            for (i = 0; i < nodePool->size; i++)
            {
                if (i >= nodePool->freeSize)
                {
                    if (NULL != nodePool->slot[i])
                    {
                        abort();
                    }
                    continue;
                }                
                
                slot_in_nodepool = nodePool->slot[i];
                hashtab_check_single_slot(slot_in_nodepool, db_pool->nodePools, nodePool);
            }
        }        
        db_pool = db_pool->next;
    }
}

static void hashtab_check_single_slot(PGXCNodePoolSlot *slot, HTAB  *nodeHtb, PGXCNodePool *outerPool)
{// #lizard forgives
    DatabasePool         *db_pool                = NULL;
    HASH_SEQ_STATUS     hseq_status;
    PGXCNodePool         *nodePool             = NULL;
    int                 i                     = 0;
    PGXCNodePoolSlot     *slot_in_nodepool    = NULL;


    db_pool = databasePools;    
    while (db_pool)
    {
        if (nodeHtb == db_pool->nodePools)
        {
            db_pool = db_pool->next;
            continue;
        }
        
        hash_seq_init(&hseq_status, db_pool->nodePools);
        while ((nodePool = (PGXCNodePool *) hash_seq_search(&hseq_status)))
        {            
            if (outerPool == nodePool)
            {
                break;
            }
            
            for (i = 0; i < nodePool->freeSize; i++)
            {            
                slot_in_nodepool = nodePool->slot[i];
                /* Invalid situation */
                if (slot_in_nodepool && slot == slot_in_nodepool)
                {
                    abort();
                }
                
            }
        }        
        db_pool = db_pool->next;
    }
}

#endif

/*
 * Ping an UNHEALTHY node and if it succeeds, update SHARED node
 * information
 */
static void
TryPingUnhealthyNode(Oid nodeoid)
{
    int status;
    NodeDefinition *nodeDef;
    char connstr[MAXPGPATH * 2 + 256];

    nodeDef = PgxcNodeGetDefinition(nodeoid);
    if (nodeDef == NULL)
    {
        /* No such definition, node dropped? */
        elog(DEBUG1, "Could not find node (%u) definition,"
             " skipping health check", nodeoid);
        return;
    }
    if (nodeDef->nodeishealthy)
    {
        /* hmm, can this happen? */
        elog(DEBUG1, "node (%u) healthy!"
             " skipping health check", nodeoid);
        return;
    }

    elog(LOG, "node (%s:%u) down! Trying ping",
         NameStr(nodeDef->nodename), nodeoid);
    sprintf(connstr,
            "host=%s port=%d", NameStr(nodeDef->nodehost),
            nodeDef->nodeport);
    status = PGXCNodePing(connstr);
    if (status != 0)
    {
        pfree(nodeDef);
        return;
    }

    elog(DEBUG1, "Node (%s) back online!", NameStr(nodeDef->nodename));
    if (!PgxcNodeUpdateHealth(nodeoid, true))
        elog(WARNING, "Could not update health status of node (%s)",
             NameStr(nodeDef->nodename));
    else
        elog(LOG, "Health map updated to reflect HEALTHY node (%s)",
             NameStr(nodeDef->nodename));
    pfree(nodeDef);

    return;
}

/*
 * Check if a node is indeed down and if it is update its UNHEALTHY
 * status
 */
void
PoolPingNodeRecheck(Oid nodeoid)
{
	int status;
	NodeDefinition *nodeDef;
	char connstr[MAXPGPATH * 2 + 256];
	bool	healthy;
	const char *username = NULL;
	char *errstr = NULL;

	username = get_user_name(&errstr);
	if (errstr != NULL)
	{
		elog(WARNING, "Could not get current username errmsg: %s", errstr);
		return;
	}

    nodeDef = PgxcNodeGetDefinition(nodeoid);
    if (nodeDef == NULL)
    {
        /* No such definition, node dropped? */
        elog(DEBUG1, "Could not find node (%u) definition,"
             " skipping health check", nodeoid);
        return;
    }

    sprintf(connstr,
			"host=%s port=%d user=%s dbname=%s", NameStr(nodeDef->nodehost),
			nodeDef->nodeport, username, get_database_name(MyDatabaseId));
    status = PGXCNodePing(connstr);
    healthy = (status == 0);

    /* if no change in health bit, return */
    if (healthy == nodeDef->nodeishealthy)
    {
        pfree(nodeDef);
        return;
    }

    if (!PgxcNodeUpdateHealth(nodeoid, healthy))
        elog(WARNING, "Could not update health status of node (%s)",
             NameStr(nodeDef->nodename));
    else
        elog(LOG, "Health map updated to reflect (%s) node (%s)",
             healthy ? "HEALTHY" : "UNHEALTHY", NameStr(nodeDef->nodename));
    pfree(nodeDef);

    return;
}

void
PoolAsyncPingNodes()
{// #lizard forgives
    Oid                *coOids = NULL;
    Oid                *dnOids = NULL;
    bool            *coHealthMap = NULL;
    bool            *dnHealthMap = NULL;
    int                numCo;
    int                numDn;
    int                i;

	/* pipe is full, no need to continue */
	if (IS_ASYNC_PIPE_FULL())
	{
		return;
	}	

    coOids = (Oid*)palloc(sizeof(Oid) * TBASE_MAX_COORDINATOR_NUMBER);
    if (coOids == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory for coOids")));
    }

    dnOids = (Oid*)palloc(sizeof(Oid) * TBASE_MAX_DATANODE_NUMBER);
    if (dnOids == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory for dnOids")));
    }

    coHealthMap = (bool*)palloc(sizeof(bool) * TBASE_MAX_COORDINATOR_NUMBER);
    if (coHealthMap == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory for coHealthMap")));
    }

    dnHealthMap = (bool*)palloc(sizeof(bool) * TBASE_MAX_DATANODE_NUMBER);
    if (dnHealthMap == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory for dnHealthMap")));
    }

    PgxcNodeGetHealthMap(coOids, dnOids, &numCo, &numDn,
                         coHealthMap, dnHealthMap);

    /*
     * Find unhealthy datanodes and try to re-ping them
     */
    for (i = 0; i < numDn; i++)
    {
        if (!dnHealthMap[i])
        {
            Oid     nodeoid = dnOids[i];
            pooler_async_ping_node(nodeoid);
        }
    }
    /*
     * Find unhealthy coordinators and try to re-ping them
     */
    for (i = 0; i < numCo; i++)
    {
        if (!coHealthMap[i])
        {
            Oid     nodeoid = coOids[i];
            pooler_async_ping_node(nodeoid);
        }
    }

    if (coOids)
    {
        pfree(coOids);
        coOids = NULL;
    }

    if (dnOids)
    {
        pfree(dnOids);
        dnOids = NULL;
    }

    if (coHealthMap)
    {
        pfree(coHealthMap);
        coHealthMap = NULL;
    }

    if (dnHealthMap)
    {
        pfree(dnHealthMap);
        dnHealthMap = NULL;
    }
}


/*
 * Ping UNHEALTHY nodes as part of the maintenance window
 */
void
PoolPingNodes()
{// #lizard forgives
    Oid                *coOids = NULL;
    Oid                *dnOids = NULL;
    bool            *coHealthMap = NULL;
    bool            *dnHealthMap = NULL;
    int                numCo;
    int                numDn;
    int                i;

    coOids = (Oid*)palloc(sizeof(Oid) * TBASE_MAX_COORDINATOR_NUMBER);
    if (coOids == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory for coOids")));
    }

    dnOids = (Oid*)palloc(sizeof(Oid) * TBASE_MAX_DATANODE_NUMBER);
    if (dnOids == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory for dnOids")));
    }

    coHealthMap = (bool*)palloc(sizeof(bool) * TBASE_MAX_COORDINATOR_NUMBER);
    if (coHealthMap == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory for coHealthMap")));
    }

    dnHealthMap = (bool*)palloc(sizeof(bool) * TBASE_MAX_DATANODE_NUMBER);
    if (dnHealthMap == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory for dnHealthMap")));
    }

    PgxcNodeGetHealthMap(coOids, dnOids, &numCo, &numDn,
                         coHealthMap, dnHealthMap);

    /*
     * Find unhealthy datanodes and try to re-ping them
     */
    for (i = 0; i < numDn; i++)
    {
        if (!dnHealthMap[i])
        {
            Oid     nodeoid = dnOids[i];
            TryPingUnhealthyNode(nodeoid);
        }
    }
    /*
     * Find unhealthy coordinators and try to re-ping them
     */
    for (i = 0; i < numCo; i++)
    {
        if (!coHealthMap[i])
        {
            Oid     nodeoid = coOids[i];
            TryPingUnhealthyNode(nodeoid);
        }
    }

    if (coOids)
    {
        pfree(coOids);
        coOids = NULL;
    }

    if (dnOids)
    {
        pfree(dnOids);
        dnOids = NULL;
    }

    if (coHealthMap)
    {
        pfree(coHealthMap);
        coHealthMap = NULL;
    }

    if (dnHealthMap)
    {
        pfree(dnHealthMap);
        dnHealthMap = NULL;
    }
}


static void
handle_abort(PoolAgent * agent, StringInfo s)
{
    int        len;
    int       *pids;
    const char *database = NULL;
    const char *user_name = NULL;

    pool_getmessage(&agent->port, s, 0);
    len = pq_getmsgint(s, 4);
    if (len > 0)
        database = pq_getmsgbytes(s, len);

    len = pq_getmsgint(s, 4);
    if (len > 0)
        user_name = pq_getmsgbytes(s, len);

    pq_getmsgend(s);

    pids = abort_pids(&len, agent->pid, database, user_name);

    pool_sendpids(&agent->port, pids, len, NULL, 0);
    if (pids)
        pfree(pids);
}

static void 
handle_command_to_nodes(PoolAgent * agent, StringInfo s)
{
    int            datanodecount;
    int            coordcount;
    //List       *nodelist = NIL;
    List       *datanodelist = NIL;
    List       *coordlist = NIL;
    int            i, res;
    /*
     * Length of message is caused by:
     * - Message header = 4bytes
     * - Number of Datanodes sent = 4bytes
     * - List of Datanodes = NumPoolDataNodes * 4bytes (max)
     * - Number of Coordinators sent = 4bytes
     * - List of Coordinators = NumPoolCoords * 4bytes (max)
     */
    pool_getmessage(&agent->port, s, 4 * agent->num_dn_connections + 4 * agent->num_coord_connections + 12);
    datanodecount = pq_getmsgint(s, 4);
    for (i = 0; i < datanodecount; i++)
    {
        datanodelist = lappend_int(datanodelist, pq_getmsgint(s, 4));
    }
    
    coordcount = pq_getmsgint(s, 4);
    /* It is possible that no Coordinators are involved in the transaction */
    for (i = 0; i < coordcount; i++)
    {
        coordlist = lappend_int(coordlist, pq_getmsgint(s, 4));
    }
    pq_getmsgend(s);
    /* Send local commands if any to the nodes involved in the transaction */
    res = send_local_commands(agent, datanodelist, coordlist);
    
    list_free(datanodelist);
    list_free(coordlist);

    /* nothing to do, just send result */
    if (res <= 0)
    {
        pool_sendres(&agent->port, res, NULL, 0, true);
    }
}


static void
handle_connect(PoolAgent * agent, StringInfo s)
{
    int    len;
    const char *database = NULL;
    const char *user_name = NULL;
    const char *pgoptions = NULL;

    pool_getmessage(&agent->port, s, 0);
    agent->pid = pq_getmsgint(s, 4);

    len = pq_getmsgint(s, 4);
    database = pq_getmsgbytes(s, len);

    len = pq_getmsgint(s, 4);
    user_name = pq_getmsgbytes(s, len);

    len = pq_getmsgint(s, 4);
    pgoptions = pq_getmsgbytes(s, len);

    /*
     * Coordinator pool is not initialized.
     * With that it would be impossible to create a Database by default.
     */
    agent_init(agent, database, user_name, pgoptions);
    pq_getmsgend(s);
}

static void
handle_clean_connection(PoolAgent * agent, StringInfo s)
{
    int i, len, res;
    int    datanodecount, coordcount;
    const char *database = NULL;
    const char *user_name = NULL;
    List       *nodelist = NIL;

    pool_getmessage(&agent->port, s, 0);

    /* It is possible to clean up only datanode connections */
    datanodecount = pq_getmsgint(s, 4);
    for (i = 0; i < datanodecount; i++)
    {
        /* Translate index to Oid */
        int index = pq_getmsgint(s, 4);
        Oid node = agent->dn_conn_oids[index];
        nodelist = lappend_oid(nodelist, node);
    }

    /* It is possible to clean up only coordinator connections */
    coordcount = pq_getmsgint(s, 4);
    for (i = 0; i < coordcount; i++)
    {
        /* Translate index to Oid */
        int index = pq_getmsgint(s, 4);
        Oid node = agent->coord_conn_oids[index];
        nodelist = lappend_oid(nodelist, node);
    }

    len = pq_getmsgint(s, 4);
    if (len > 0)
        database = pq_getmsgbytes(s, len);

    len = pq_getmsgint(s, 4);
    if (len > 0)
        user_name = pq_getmsgbytes(s, len);

    pq_getmsgend(s);

    /* Clean up connections here */
    res = clean_connection(nodelist, database, user_name);

    list_free(nodelist);

    /* Send success result */
    pool_sendres(&agent->port, res, NULL, 0, true);
}

static void
handle_get_connections(PoolAgent * agent, StringInfo s)
{// #lizard forgives
    int        i;
    int       *fds = NULL;
    int    *pids = NULL;
    int     ret;
    int        datanodecount, coordcount;
    List   *datanodelist = NIL;
    List   *coordlist = NIL;
    int     connect_num = 0;
	bool    raise_error = true;
    /*
     * Length of message is caused by:
     * - Message header = 4bytes
     * - List of Datanodes = NumPoolDataNodes * 4bytes (max)
     * - List of Coordinators = NumPoolCoords * 4bytes (max)
     * - Number of Datanodes sent = 4bytes
     * - Number of Coordinators sent = 4bytes
	 * - Raise error flag = 1byte
     * It is better to send in a same message the list of Co and Dn at the same
     * time, this permits to reduce interactions between postmaster and pooler
     */
	pool_getmessage(&agent->port, s, 4 * agent->num_dn_connections + 4 * agent->num_coord_connections + 13);
    datanodecount = pq_getmsgint(s, 4);
    for (i = 0; i < datanodecount; i++)
    {
        datanodelist = lappend_int(datanodelist, pq_getmsgint(s, 4));
    }

    if (PoolConnectDebugPrint)
    {
        elog(LOG, POOL_MGR_PREFIX"backend required %d datanode connections, pid:%d", datanodecount, agent->pid);
    }

    coordcount = pq_getmsgint(s, 4);
    /* It is possible that no Coordinators are involved in the transaction */
    for (i = 0; i < coordcount; i++)
    {
        coordlist = lappend_int(coordlist, pq_getmsgint(s, 4));
    }
    if (PoolConnectDebugPrint)
    {
        elog(LOG, POOL_MGR_PREFIX"backend required %d coordinator connections, pid:%d", coordcount, agent->pid);
    }
	
	raise_error = pq_getmsgbyte(s);
    pq_getmsgend(s);

    if(!is_pool_locked)
    {
        
        /*
         * In case of error agent_acquire_connections will log
         * the error and return -1
         */
		ret = agent_acquire_connections(agent, datanodelist, coordlist, raise_error, &connect_num, &fds, &pids);
        /* async acquire connection will be done in parallel threads */
        if (0 == ret && fds && pids)
        {
            list_free(datanodelist);
            list_free(coordlist);

            if (PoolConnectDebugPrint)
            {
                elog(LOG, POOL_MGR_PREFIX"return %d database connections pid:%d", connect_num, agent->pid);
            }
            pool_sendfds(&agent->port, fds, fds ? connect_num : 0, NULL, 0);
            if (fds)
            {
                pfree(fds);
                fds = NULL;
            }

            /*
             * Also send the PIDs of the remote backend processes serving
             * these connections
             */
            pool_sendpids(&agent->port, pids, pids ? connect_num : 0, NULL, 0);
            if (pids)
            {
                pfree(pids);
                pids = NULL;
            }

            if (PoolPrintStatTimeout > 0)
            {
                g_pooler_stat.client_request_from_hashtab++;
            }
        }
        else if (0 == ret)
        {
            if (PoolConnectDebugPrint)
            {
                elog(LOG, POOL_MGR_PREFIX"cannot get conn immediately. thread will do the work. pid:%d", agent->pid);
            }
            if (PoolPrintStatTimeout > 0)
            {
                g_pooler_stat.client_request_from_thread++;
            }
        }
        else
        {
            if (fds)
            {
                pfree(fds);
                fds = NULL;
            }
            if (pids)
            {
                pfree(pids);
                pids = NULL;
            }
            pool_sendfds(&agent->port, NULL, 0, NULL, 0);
            /*
             * Also send the PIDs of the remote backend processes serving
             * these connections
             */
            pool_sendpids(&agent->port, NULL, 0, NULL, 0);
            elog(LOG, POOL_MGR_PREFIX"error happen when agent_acquire_connections. pid:%d, ret=%d", agent->pid, ret);
        }
    }
    else
    {
        /* pooler is locked, just refuse the 'g' request */
        elog(WARNING,POOL_MGR_PREFIX"Pool connection get request cannot run during pool lock");
        list_free(datanodelist);
        list_free(coordlist);
#ifdef __TBASE__
        /* set error message */
        SpinLockAcquire(&agent->port.lock);
        agent->port.error_code = POOL_ERR_GET_CONNECTIONS_POOLER_LOCKED;
        snprintf(agent->port.err_msg, POOL_ERR_MSG_LEN, "%s", poolErrorMsg[agent->port.error_code]);
        SpinLockRelease(&agent->port.lock);
#endif
        pool_sendfds(&agent->port, NULL, 0, NULL, 0);
        /*
         * Also send the PIDs of the remote backend processes serving
         * these connections
         */
        pool_sendpids(&agent->port, NULL, 0, NULL, 0);
    }
    
}

static void
handle_query_cancel(PoolAgent * agent, StringInfo s)
{
    int        i;
    int        datanodecount, coordcount;
    List   *datanodelist = NIL;
    List   *coordlist = NIL;
    int     res;
    int     signal;

    /*
     * Length of message is caused by:
     * - Message header = 4bytes
     * - List of Datanodes = NumPoolDataNodes * 4bytes (max)
     * - List of Coordinators = NumPoolCoords * 4bytes (max)
     * - Number of Datanodes sent = 4bytes
     * - Number of Coordinators sent = 4bytes
     * - sent signal 4bytes
     */
    pool_getmessage(&agent->port, s, 4 * agent->num_dn_connections + 4 * agent->num_coord_connections + 12 + 4);

    datanodecount = pq_getmsgint(s, 4);
    for (i = 0; i < datanodecount; i++)
        datanodelist = lappend_int(datanodelist, pq_getmsgint(s, 4));

    coordcount = pq_getmsgint(s, 4);
    /* It is possible that no Coordinators are involved in the transaction */
    for (i = 0; i < coordcount; i++)
        coordlist = lappend_int(coordlist, pq_getmsgint(s, 4));

    /* get signal type */
    signal =  pq_getmsgint(s, 4);

    pq_getmsgend(s);

    res = cancel_query_on_connections(agent, datanodelist, coordlist, signal);
    list_free(datanodelist);
    list_free(coordlist);


    /* just send result. when res > 0, thread will exec pool_sendres. */
    if (res <= 0)
    {
        pool_sendres(&agent->port, res, NULL, 0, true);
    }
}

static void 
handle_session_command(PoolAgent * agent, StringInfo s)
{// #lizard forgives
    PoolCommandType     command_type;
    const char         *set_command = NULL;
    int              res = 0;
    int                 len = 0;
    int                 oid_count = 0;
    Oid                 *oids     = NULL;
    
    pool_getmessage(&agent->port, s, 0);    

    /* Get oid count */
    oid_count = pq_getmsgint(s, 4);

    if (PoolConnectDebugPrint)
    {
        elog(LOG, POOL_MGR_PREFIX"handle_session_command pid:%d  async set command node_count:%d ", agent->pid, oid_count);
    }
    
    if (oid_count != POOL_SET_COMMAND_NONE && oid_count != POOL_SET_COMMAND_ALL)
    {
        int  i = 0;

        /* get oids */
        oids = (Oid*)palloc0(sizeof(Oid)* oid_count);
        for (i = 0; i < oid_count; i++)
        {
            oids[i] = pq_getmsgint(s, 4);
            if (PoolConnectDebugPrint)
            {
                elog(LOG, POOL_MGR_PREFIX"handle_session_command pid:%d  async set command node_count:%d, node_index:%d node_oid:%u ", agent->pid, oid_count, i, oids[i]);
            }
        }
    }

    /* Determine if command is local or session */
    command_type = (PoolCommandType) pq_getmsgint(s, 4);
    
    /* Get the SET command if necessary */
    len = pq_getmsgint(s, 4);
    if (len != 0)
    {
        set_command = pq_getmsgbytes(s, len);
    }
    pq_getmsgend(s);

    /* Manage command depending on its type */
    res = agent_session_command(agent, set_command, oids, oid_count, command_type);

    /* Send success result. in parallel process, we got positive number returned. */
    if (res <= 0)
    {
        res = pool_sendres_with_command_id(&agent->port, res, InvalidCommandId, NULL, 0, NULL, true);
        if (res)
        {
            elog(LOG, POOL_MGR_PREFIX"handle_session_command pid:%d async set command command:%s failed!", agent->pid, set_command);
        }
    }
    
    if (oids)
    {
        pfree(oids);
    }
}

/*
 * refresh_database_pools
 *        refresh information for all database pools
 *
 * Connection information refresh concerns all the database pools.
 * A database pool is refreshed as follows for each remote node:
 *
 * - node pool is deleted if its port or host information is changed.
 *   Subsequently all its connections are dropped.
 *
 * If any other type of activity is found, we error out.
 *
 * XXX I don't see any cases that would error out. Isn't the comment
 * simply obsolete?
 */
static int
refresh_database_pools(PoolAgent *agent)
{// #lizard forgives
    DatabasePool *databasePool;
    Oid               *coOids;
    Oid               *dnOids;
    int                numCo;
    int                numDn;
    int             res = POOL_REFRESH_SUCCESS;

    elog(LOG, "Refreshing database pools");

    /*
     * re-check if agent's node information matches current contents of the
     * shared memory table.
     */
    PgxcNodeGetOids(&coOids, &dnOids, &numCo, &numDn, false);

    if (agent->num_coord_connections != numCo ||
            agent->num_dn_connections != numDn ||
            memcmp(agent->coord_conn_oids, coOids, numCo * sizeof(Oid)) ||
            memcmp(agent->dn_conn_oids, dnOids, numDn * sizeof(Oid)))
        res = POOL_REFRESH_FAILED;

    /* Release palloc'ed memory */
    pfree(coOids);
    pfree(dnOids);

    /*
     * Scan the list and destroy any altered pool. They will be recreated
     * upon subsequent connection acquisition.
     */
    databasePool = databasePools;
    while (res == POOL_REFRESH_SUCCESS && databasePool)
    {
        HASH_SEQ_STATUS hseq_status;
        PGXCNodePool   *nodePool;

        hash_seq_init(&hseq_status, databasePool->nodePools);
        while ((nodePool = (PGXCNodePool *) hash_seq_search(&hseq_status)))
        {
            char *connstr_chk = build_node_conn_str(nodePool->nodeoid, databasePool);

            /*
             * Since we re-checked the numbers above, we should not get
             * the case of an ADDED or a DELETED node here..
             */
            if (connstr_chk == NULL)
            {
                elog(LOG, "Found a deleted node (%u)", nodePool->nodeoid);
                hash_seq_term(&hseq_status);
                res = POOL_REFRESH_FAILED;
                break;
            }

            if (strcmp(connstr_chk, nodePool->connstr))
            {
				if (nodePool->size == nodePool->freeSize)
                {
					elog(LOG,
						"refresh_database_pools: Found an altered node (%u %s) "
						"size %d freesize %d is removed. "
						"connstr_chk=%s, nodePool->connstr=%s",
						nodePool->nodeoid, nodePool->node_name,
						nodePool->size, nodePool->freeSize,
						connstr_chk, nodePool->connstr);
                destroy_node_pool(nodePool);
                hash_search(databasePool->nodePools, &nodePool->nodeoid,
                            HASH_REMOVE, NULL);
            }
				else
				{
					destroy_node_pool_free_slots(nodePool);

					/* increase the node pool version */
					nodePool->m_version = databasePool->version++;
					elog(LOG,
						"refresh_database_pools: Found an altered node (%u %s) "
						"size %d freesize %d increased m_version "INT64_FORMAT
						"connstr_chk=%s, nodePool->connstr=%s",
						nodePool->nodeoid, nodePool->node_name,
						nodePool->size, nodePool->freeSize, nodePool->m_version,
						connstr_chk, nodePool->connstr);

					/* fresh the connect string so that new coming connection will connect to the new node  */
					if (connstr_chk)
					{
						if (nodePool->connstr)
						{
							pfree(nodePool->connstr);
						}
						nodePool->connstr = pstrdup(connstr_chk);
					}
				}
			}

            if (connstr_chk)
                pfree(connstr_chk);
        }

        databasePool = databasePool->next;
    }
    return res;
}


bool
check_persistent_connections(bool *newval, void **extra, GucSource source)
{
    if (*newval && IS_PGXC_DATANODE)
    {
        elog(WARNING, "persistent_datanode_connections = ON is currently not "
                "supported on datanodes - ignoring");
        *newval = false;
    }
    return true;
}


/*
 * Refresh connection data in pooler and drop connections for those nodes
 * that have changed. Thus, this operation is less destructive as compared
 * to PoolManagerReloadConnectionInfo and should typically be called when
 * NODE ALTER has been performed
 */
int
PoolManagerRefreshConnectionInfo(void)
{
    int res;

    HOLD_POOLER_RELOAD();

    Assert(poolHandle);
	PgxcNodeListAndCountWrapTransaction();
    pool_putmessage(&poolHandle->port, 'R', NULL, 0);
    pool_flush(&poolHandle->port);

	res = pool_recvres(&poolHandle->port, true);

    RESUME_POOLER_RELOAD();

    if (res == POOL_CHECK_SUCCESS)
        return true;

    return false;
}


static void reset_pooler_statistics(void)
{
    g_pooler_stat.acquire_conn_from_hashtab = 0;
    g_pooler_stat.acquire_conn_from_hashtab_and_set = 0;
    g_pooler_stat.acquire_conn_from_thread = 0;
    g_pooler_stat.client_request_conn_total = 0;
    g_pooler_stat.client_request_from_hashtab = 0;
    g_pooler_stat.client_request_from_thread = 0;
    g_pooler_stat.acquire_conn_time = 0;
}

static void print_pooler_statistics(void)
{
    static time_t last_print_stat_time = 0;
    time_t           now ;
    double           timediff;

    if (PoolPrintStatTimeout > 0)
    {
        if (0 == last_print_stat_time)
        {
            last_print_stat_time = time(NULL);
        }
        
        now = time(NULL);
        timediff = difftime(now, last_print_stat_time);
        
        if (timediff >= PoolPrintStatTimeout)
        {
            last_print_stat_time = time(NULL);

            elog(LOG, "[pooler stat]client_request_conn_total=%d, client_request_from_hashtab=%d, "
                      "client_request_from_thread=%d, acquire_conn_from_hashtab=%d, "
                        "acquire_conn_from_hashtab_and_set=%d, acquire_conn_from_thread=%d, "
                        "acquire_conn_time=%lu, "
                        "each_client_conn_request_cost_time=%f us",
                  g_pooler_stat.client_request_conn_total, 
                  g_pooler_stat.client_request_from_hashtab,
                  g_pooler_stat.client_request_from_thread,
                  g_pooler_stat.acquire_conn_from_hashtab,
                  g_pooler_stat.acquire_conn_from_hashtab_and_set,
                  g_pooler_stat.acquire_conn_from_thread,
                  g_pooler_stat.acquire_conn_time,
                  (double)g_pooler_stat.acquire_conn_time / (double)g_pooler_stat.client_request_conn_total);
            reset_pooler_statistics();
        }
    }


    
}


static void record_task_message(PGXCPoolSyncNetWorkControl* control, int32 thread, char* message)
{
    if (control->message[thread])
    {
        free(control->message[thread]);
        control->message[thread] = NULL;
    }
    control->message[thread] = strdup(message);
}


static void record_time(struct timeval start_time, struct timeval end_time)
{
    unsigned  long diff = 0;

    if (PoolPrintStatTimeout <= 0)
        return;
    
    if (start_time.tv_sec == 0 && start_time.tv_usec == 0)
        return;
    
    diff = 1000000 * (end_time.tv_sec - start_time.tv_sec) + end_time.tv_usec - start_time.tv_usec;
    g_pooler_stat.acquire_conn_time += diff;
}

static inline void RebuildAgentIndex(void)
{
    if (mgrVersion != BmpMgrGetVersion(poolAgentMgr))
    {
        usedAgentSize= BmpMgrGetUsed(poolAgentMgr, agentIndexes, poolAgentSize);
        if (usedAgentSize != agentCount)
        {
            elog(PANIC, POOL_MGR_PREFIX"invalid BmpMgr status");
        }
        mgrVersion = BmpMgrGetVersion(poolAgentMgr);
    }
}

static bool connection_need_pool(char *filter, const char *name)
{
    /* format "xxx:yyy" */
    #define FILTER_ELEMENT_SEP ","
    #define TEMP_PATH_LEN 1024
    char    *token   = NULL;
    char    *next   = NULL;
    char    str[TEMP_PATH_LEN] = {0};

    /* no filter, need pool */
    if (NULL == filter)
    {
        return true;
    }

    /* no name, no  need pool */
    if (NULL == name)
    {
        return false;
    }

    snprintf(str, TEMP_PATH_LEN, "%s", filter);
    token = str;
    next  = token;
    do
    {            
        token = next;
        next = strstr(token, FILTER_ELEMENT_SEP);
        if (NULL == next)
        {
            /* not pool if listed */
            if (0 == strcmp(token, name))
            {
                return false;
            }
            else
            {
                return true;
            }
        }
        *next = '\0';
        next += 1;
        /* not pool if listed */
        if (0 == strcmp(token, name))
        {
            return false;
        }
    } while(*next != '\0');
        
    return true;
}


/* close pooled connection */
int
PoolManagerClosePooledConnections(const char *dbname, const char *username)
{
    int        n32 = 0;
    int     msglen = 0;
    char    msgtype = 't';
    int     res = 0;
    int        dblen = dbname ? strlen(dbname) + 1 : 0;
    int        userlen = username ? strlen(username) + 1 : 0;

    HOLD_POOLER_RELOAD();
    
    if (poolHandle == NULL)
    {
		ConnectPoolManager();
    }

    /* Message type */
    pool_putbytes(&poolHandle->port, &msgtype, 1);

    /* Message length */
    msglen = dblen + userlen + 12;
    n32 = htonl(msglen);
    pool_putbytes(&poolHandle->port, (char *) &n32, 4);

    /* Length of Database string */
    n32 = htonl(dblen);
    pool_putbytes(&poolHandle->port, (char *) &n32, 4);

    /* Send database name, followed by \0 terminator if necessary */
    if (dbname)
        pool_putbytes(&poolHandle->port, dbname, dblen);

    /* Length of Username string */
    n32 = htonl(userlen);
    pool_putbytes(&poolHandle->port, (char *) &n32, 4);

    /* Send user name, followed by \0 terminator if necessary */
    if (username)
        pool_putbytes(&poolHandle->port, username, userlen);

    pool_flush(&poolHandle->port);

	/* Then Get back Pids from Pooler */
	res = pool_recvres(&poolHandle->port, true);
	elog(LOG, "PoolManagerClosePooledConnections res:%d", res);

    RESUME_POOLER_RELOAD();

    return res;
}


static int
handle_close_pooled_connections(PoolAgent * agent, StringInfo s)
{
    int                len = 0;
    const char         *database = NULL;
    const char         *user_name = NULL;
    DatabasePool     *databasePool = NULL;
    int             res = POOL_CONN_RELEASE_SUCCESS;

    pool_getmessage(&agent->port, s, 0);
    len = pq_getmsgint(s, 4);
    if (len > 0)
        database = pq_getmsgbytes(s, len);

    len = pq_getmsgint(s, 4);
    if (len > 0)
        user_name = pq_getmsgbytes(s, len);

    pq_getmsgend(s);

    /*
     * Scan the list and destroy any altered pool. They will be recreated
     * upon subsequent connection acquisition.
     */
    databasePool = databasePools;
    while (databasePool)
    {
        /* Update each database pool slot with new connection information */
        HASH_SEQ_STATUS hseq_status;
        PGXCNodePool   *nodePool;

        if (match_databasepool(databasePool, user_name, database))
        {
            hash_seq_init(&hseq_status, databasePool->nodePools);
            while ((nodePool = (PGXCNodePool *) hash_seq_search(&hseq_status)))
            {
                destroy_node_pool_free_slots(nodePool);

                /* increase the node pool version */
				nodePool->m_version = databasePool->version++;
            }
        }

        databasePool = databasePool->next;
    }

    return res;
}

static bool match_databasepool(DatabasePool *databasePool, const char* user_name, const char* database)
{// #lizard forgives
    if (!databasePool)
        return false;
    
    if (!user_name && !database)
        return false;

    if (user_name && strcmp(databasePool->user_name, user_name) != 0)
        return false;
    
    if (database && strcmp(databasePool->database, database) != 0)    
        return false;

    return true;
}
#ifdef __TBASE__
static void
ConnectPoolManager(void)
{
	bool need_abort = false;
	PoolHandle *handle = NULL;
	
	handle = GetPoolManagerHandle();
	if (!IsTransactionOrTransactionBlock())
	{
		StartTransactionCommand();
		need_abort = true;
	}
	PoolManagerConnect(handle, get_database_name(MyDatabaseId),
					   GetClusterUserName(), session_options());
	if (need_abort)
	{
		AbortCurrentTransaction();
	}

}
#endif

/*
 * init pooler command statistics
 */
static void
init_pooler_cmd_statistics(void)
{
    int i = 0;
    memset(g_qtype2id, -1, sizeof(g_qtype2id));

    /* init type to id map */
    for (i = 0; i < POOLER_CMD_COUNT; i++)
    {
        g_qtype2id[g_pooler_cmd[i]] = i;
    }

    /* init global statistics array */
    g_pooler_cmd_stat = (PoolerCmdStatistics*) palloc(POOLER_CMD_COUNT * sizeof(PoolerCmdStatistics));
    for (i = 0; i < POOLER_CMD_COUNT; i++)
    {
        g_pooler_cmd_stat[i].total_request_times = 0;
        g_pooler_cmd_stat[i].total_costtime = 0;
        g_pooler_cmd_stat[i].max_costtime = 0;
        g_pooler_cmd_stat[i].min_costtime = MAX_UINT64;
    }
}

/*
 * reset pooler command statistics
 */
static void
reset_pooler_cmd_statistics(void)
{
    int i = 0;
    /* reset global statistics array */
    for (i = 0; i < POOLER_CMD_COUNT; i++)
    {
        g_pooler_cmd_stat[i].total_request_times = 0;
        g_pooler_cmd_stat[i].total_costtime = 0;
        g_pooler_cmd_stat[i].max_costtime = 0;
        g_pooler_cmd_stat[i].min_costtime = MAX_UINT64;
    }
}

/*
 * update pooler command statistics info
 */
static void
update_pooler_cmd_statistics(unsigned char qtype, uint64 costtime)
{
    uint8 id = g_qtype2id[qtype];
    if (id == MAX_UINT8)
    {
        return;
    }

    g_pooler_cmd_stat[id].total_request_times += 1;
    g_pooler_cmd_stat[id].total_costtime += costtime;

    if (costtime > g_pooler_cmd_stat[id].max_costtime)
    {
        g_pooler_cmd_stat[id].max_costtime = costtime;
    }

    if (costtime < g_pooler_cmd_stat[id].min_costtime)
    {
        g_pooler_cmd_stat[id].min_costtime = costtime;
    }
}

/*
 * handle get command statistics
 */
static void
handle_get_cmd_statistics(PoolAgent *agent)
{
    int    i = 0;
    uint64 n64 = 0;
    char   msgtype = 'x';

    /* response message type */
    pool_putbytes(&agent->port, &msgtype, 1);

    /* fixed length command statistics info */
    for (i = 0; i < POOLER_CMD_COUNT; i++)
    {
        n64 = htobe64(g_pooler_cmd_stat[i].total_request_times);
        pool_putbytes(&agent->port, (char *) &n64, sizeof(n64));

        n64 = htobe64(g_pooler_cmd_stat[i].total_costtime);
        pool_putbytes(&agent->port, (char *) &n64, sizeof(n64));

        n64 = htobe64(g_pooler_cmd_stat[i].max_costtime);
        pool_putbytes(&agent->port, (char *) &n64, sizeof(n64));

        n64 = htobe64(g_pooler_cmd_stat[i].min_costtime);
        pool_putbytes(&agent->port, (char *) &n64, sizeof(n64));
    }

    pool_flush(&agent->port);
}

/*
 * handle get connections statistics
 */
static void
handle_get_conn_statistics(PoolAgent *agent)
{
    DatabasePool     *database_pool = databasePools;
    HASH_SEQ_STATUS  hseq_status;
    PGXCNodePool     *node_pool = NULL;

    uint32           node_cnt = 0;         /* the nodes count use the same database and username */
    uint32           total_node_cnt = 0;   /* total nodes count */

    /* var offset in buf */
    uint32           node_cnt_offset = 0;
    uint32           total_node_cnt_offset = 0;

    uint32           exceed_keepalive_cnt = 0;
    uint32           exceed_maxlifetime_cnt = 0;
    int              i = 0;
    PGXCNodePoolSlot *slot = NULL;
    time_t           now = time(NULL);
    StringInfoData   buf;

    initStringInfo(&buf);
    /* reserve a place for total_node_cnt, record the offset of total_node_cnt */
    total_node_cnt_offset = buf.len;
    pq_sendint(&buf, total_node_cnt, sizeof(uint32));

    /* total node count | database | username | node count in the same database and username | node pool conn statistics | ... | database | username | ... */
    while (database_pool)
    {
        pq_sendstring(&buf, database_pool->database);
        pq_sendstring(&buf, database_pool->user_name);

        /* reserve a place for node_cnt, record the offset of node_cnt */
        node_cnt = 0;
        node_cnt_offset = buf.len;
        pq_sendint(&buf, node_cnt, sizeof(uint32));

        /* traverse all node_pool in hashtable */
        hash_seq_init(&hseq_status, database_pool->nodePools);
        while ((node_pool = (PGXCNodePool *) hash_seq_search(&hseq_status)))
        {
            node_cnt++;

            pq_sendstring(&buf, node_pool->node_name);
            pq_sendint(&buf, node_pool->nodeoid, sizeof(Oid));
            pq_sendint(&buf, node_pool->coord, sizeof(bool));
            pq_sendint(&buf, node_pool->size, sizeof(uint32));
            pq_sendint(&buf, node_pool->freeSize, sizeof(uint32));
            pq_sendint(&buf, node_pool->nwarming, sizeof(uint32));
            pq_sendint(&buf, node_pool->nquery, sizeof(uint32));

            /* reset statistics count */
            exceed_keepalive_cnt = 0;
            exceed_maxlifetime_cnt = 0;
            /* statistical connection life cycle */
            if (node_pool->slot)
            {
                for (i = 0; i < node_pool->freeSize; i++)
                {
                    slot = node_pool->slot[i];
                    if (difftime(now, slot->released) > PoolConnKeepAlive)
                    {
                        exceed_keepalive_cnt++;
                    }

                    if (difftime(now, slot->created) >= PoolConnMaxLifetime)
                    {
                        exceed_maxlifetime_cnt++;
                    }
                }
            }

            pq_sendint(&buf, exceed_keepalive_cnt, sizeof(uint32));
            pq_sendint(&buf, exceed_maxlifetime_cnt, sizeof(uint32));
        }


        total_node_cnt += node_cnt;

        /* change the nodes count in message buff */
        node_cnt = htonl(node_cnt);
        pq_updatemsgbytes(&buf, node_cnt_offset, (char*) &node_cnt, sizeof(uint32));
        database_pool = database_pool->next;
    }

    /* change the total nodes count in message buff */
    total_node_cnt = htonl(total_node_cnt);
    pq_updatemsgbytes(&buf, total_node_cnt_offset, (char*) &total_node_cnt, sizeof(uint32));

    /* send messages */
    pool_putmessage(&agent->port, 'z', buf.data, buf.len);
    pool_flush(&agent->port);

    pfree(buf.data);
}
