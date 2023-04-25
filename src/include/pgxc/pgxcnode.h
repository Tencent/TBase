/*-------------------------------------------------------------------------
 *
 * pgxcnode.h
 *
 *	  Utility functions to communicate to Datanodes and Coordinators
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group ?
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/pgxcnode.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PGXCNODE_H
#define PGXCNODE_H
#include "postgres.h"
#include "gtm/gtm_c.h"
#include "utils/timestamp.h"
#include "nodes/pg_list.h"
#include "utils/snapshot.h"
#include <unistd.h>

#define NO_SOCKET -1

/* Connection to Datanode maintained by Pool Manager */
typedef struct PGconn NODE_CONNECTION;
typedef struct PGcancel NODE_CANCEL;

/* Helper structure to access Datanode from Session */
typedef enum
{
	DN_CONNECTION_STATE_IDLE,			/* idle, ready for query */
	DN_CONNECTION_STATE_QUERY,			/* query is sent, response expected */
	DN_CONNECTION_STATE_CLOSE,			/* close is sent, confirmation expected */
	DN_CONNECTION_STATE_ERROR_FATAL,	/* fatal error */
	DN_CONNECTION_STATE_COPY_IN,
	DN_CONNECTION_STATE_COPY_OUT
}	DNConnectionState;

typedef enum
{
	HANDLE_IDLE,
	HANDLE_ERROR,
	HANDLE_DEFAULT
}	PGXCNode_HandleRequested;


#ifdef __TBASE__
typedef enum
{
	DNStatus_OK      = 0, 
	DNStatus_ERR     = 1,
	DNStatus_EXPIRED = 2,
	DNStatus_BUTTY
}DNStateEnum;

typedef enum
{
	SendSetQuery_OK					= 0,
	SendSetQuery_EXPIRED			= 1,
	SendSetQuery_SendQuery_ERROR	= 2,
	SendSetQuery_Set_ERROR			= 3,
	SendSetQuery_BUTTY
}SendSetQueryStatus;

#define     MAX_ERROR_MSG_LENGTH 		 1024
#endif


#define DN_CONNECTION_STATE_ERROR(dnconn) \
		((dnconn)->state == DN_CONNECTION_STATE_ERROR_FATAL \
			|| (dnconn)->transaction_status == 'E')

#define HAS_MESSAGE_BUFFERED(conn) \
		((conn)->inCursor + 4 < (conn)->inEnd \
			&& (conn)->inCursor + ntohl(*((uint32_t *) ((conn)->inBuffer + (conn)->inCursor + 1))) < (conn)->inEnd)

struct pgxc_node_handle
{
	Oid			nodeoid;
	int			nodeid;
	char		nodename[NAMEDATALEN];
	char		nodehost[NAMEDATALEN];
	int			nodeport;

	/* fd of the connection */
	int		sock;
	/* pid of the remote backend process */
	int		backend_pid;

	/* Connection state */
	char		transaction_status;
	DNConnectionState state;
	bool		read_only;
	struct ResponseCombiner *combiner;
#ifdef DN_CONNECTION_DEBUG
	bool		have_row_desc;
#endif
#ifndef __USE_GLOBAL_SNAPSHOT__
	uint64		sendGxidVersion;
#endif
	char		error[MAX_ERROR_MSG_LENGTH];
	/* Output buffer */
	char		*outBuffer;
	size_t		outSize;
	size_t		outEnd;
	/* Input buffer */
	char		*inBuffer;
	size_t		inSize;
	size_t		inStart;
	size_t		inEnd;
	size_t		inCursor;
	/*
	 * Have a variable to enable/disable response checking and
	 * if enable then read the result of response checking
	 *
	 * For details see comments of RESP_ROLLBACK
	 */
	bool		ck_resp_rollback;

	bool		in_extended_query;
	bool		needSync; /* set when error and extend query. */
#ifdef __TBASE__
	bool		sock_fatal_occurred;	/*Network failure occurred, and sock descriptor was closed */
	char        last_command; /*last command we processed. */
	long        recv_datarows;
	bool 		plpgsql_need_begin_sub_txn;
	bool 		plpgsql_need_begin_txn;
	char        node_type;
#endif
};
typedef struct pgxc_node_handle PGXCNodeHandle;

/* Structure used to get all the handles involved in a transaction */
typedef struct
{
	PGXCNodeHandle	   *primary_handle;	/* Primary connection to PGXC node */
	int					dn_conn_count;	/* number of Datanode Handles including primary handle */
	PGXCNodeHandle	  **datanode_handles;	/* an array of Datanode handles */
	int					co_conn_count;	/* number of Coordinator handles */
	PGXCNodeHandle	  **coord_handles;	/* an array of Coordinator handles */
} PGXCNodeAllHandles;

extern PGXCNodeAllHandles *current_transaction_handles;

extern volatile bool HandlesInvalidatePending;

extern void InitMultinodeExecutor(bool is_force);
extern Oid get_nodeoid_from_nodeid(int nodeid, char node_type);


/* Open/close connection routines (invoked from Pool Manager) */
extern char *PGXCNodeConnStr(char *host, int port, char *dbname, char *user,
							 char *pgoptions,
							 char *remote_type, char *parent_node);
extern NODE_CONNECTION *PGXCNodeConnect(char *connstr);
extern void PGXCNodeClose(NODE_CONNECTION * conn);
extern int PGXCNodeConnected(NODE_CONNECTION * conn);
extern int PGXCNodeConnClean(NODE_CONNECTION * conn);
extern void PGXCNodeCleanAndRelease(int code, Datum arg);
extern int PGXCNodePing(const char *connstr);
#ifdef __TBASE__
extern NODE_CONNECTION *PGXCNodeConnectBarely(char *connstr);
extern char* PGXCNodeSendShowQuery(NODE_CONNECTION *conn, const char *sql_command);
extern int  PGXCNodeSendSetQuery(NODE_CONNECTION *conn, const char *sql_command, char *errmsg_buf, int32 buf_len, SendSetQueryStatus* status, CommandId *cmdId);
#endif

extern PGXCNodeHandle *get_any_handle(List *datanodelist);
/* Look at information cached in node handles */
extern int PGXCNodeGetNodeId(Oid nodeoid, char *node_type);
extern Oid PGXCGetLocalNodeOid(Oid nodeoid);
extern Oid PGXCGetMainNodeOid(Oid nodeoid);
extern int PGXCNodeGetNodeIdFromName(char *node_name, char *node_type);
extern Oid PGXCNodeGetNodeOid(int nodeid, char node_type);

extern PGXCNodeAllHandles *get_handles(List *datanodelist, List *coordlist,
									   bool is_query_coord_only, bool is_global_session, bool raise_error);

extern PGXCNodeAllHandles *get_current_handles(void);
#ifdef __TBASE__
extern PGXCNodeAllHandles *get_current_txn_handles(void);
extern PGXCNodeAllHandles *get_current_cn_handles(void);
extern PGXCNodeAllHandles *get_current_dn_handles(void);
extern PGXCNodeAllHandles * get_sock_fatal_handles(void);
extern void init_transaction_handles(void);
extern void reset_transaction_handles(void);
extern void register_transaction_handles(PGXCNodeHandle* handle);
#endif
extern void pfree_pgxc_all_handles(PGXCNodeAllHandles *handles);

extern void release_handles(bool force);
extern void clear_handles(void);

extern int get_transaction_nodes(PGXCNodeHandle ** connections,
								  char client_conn_type,
								  PGXCNode_HandleRequested type_requested);
extern char* collect_pgxcnode_names(char *nodestring, int conn_count, PGXCNodeHandle ** connections, char client_conn_type);
extern char* collect_localnode_name(char *nodestring);
extern int	get_active_nodes(PGXCNodeHandle ** connections);

extern int	ensure_in_buffer_capacity(size_t bytes_needed, PGXCNodeHandle * handle);
extern int	ensure_out_buffer_capacity(size_t bytes_needed, PGXCNodeHandle * handle);

extern int	pgxc_node_send_query(PGXCNodeHandle * handle, const char *query);
extern int	pgxc_node_send_rollback(PGXCNodeHandle * handle, const char *query);
extern int	pgxc_node_send_describe(PGXCNodeHandle * handle, bool is_statement,
						const char *name);
extern int	pgxc_node_send_execute(PGXCNodeHandle * handle, const char *portal, int fetch);
extern int	pgxc_node_send_close(PGXCNodeHandle * handle, bool is_statement,
					 const char *name);
extern int	pgxc_node_send_sync(PGXCNodeHandle * handle);
extern int	pgxc_node_send_my_sync(PGXCNodeHandle * handle);

#ifdef __SUBSCRIPTION__
extern int pgxc_node_send_apply(PGXCNodeHandle * handle, char * buf, int len, bool ignore_pk_conflict);
#endif
#ifdef __TBASE__
extern int pgxc_node_send_disconnect(PGXCNodeHandle * handle, char *cursor, int cons);
#endif
extern int	pgxc_node_send_bind(PGXCNodeHandle * handle, const char *portal,
								const char *statement, int paramlen, const char *params,
								int eqpctxlen, const char *epqctx, StringInfo shardmap);
extern int	pgxc_node_send_parse(PGXCNodeHandle * handle, const char* statement,
								 const char *query, short num_params, Oid *param_types);
extern int	pgxc_node_send_flush(PGXCNodeHandle * handle);
extern int	pgxc_node_send_query_extended(PGXCNodeHandle *handle, const char *query,
							  const char *statement, const char *portal,
							  int num_params, Oid *param_types,
							  int paramlen, char *params,
							  bool send_describe, int fetch_size);
extern int  pgxc_node_send_plan(PGXCNodeHandle * handle, const char *statement,
					const char *query, const char *planstr,
					short num_params, Oid *param_types, int instrument_options);
extern int pgxc_node_send_gid(PGXCNodeHandle *handle, char* gid);
#ifdef __TWO_PHASE_TRANS__
extern int pgxc_node_send_starter(PGXCNodeHandle *handle, char* startnode);
extern int pgxc_node_send_startxid(PGXCNodeHandle *handle, GlobalTransactionId transactionid);
extern int pgxc_node_send_partnodes(PGXCNodeHandle *handle, char* partnodes);
extern int pgxc_node_send_clean(PGXCNodeHandle *handle);
extern int pgxc_node_send_readonly(PGXCNodeHandle *handle);
extern int pgxc_node_send_after_prepare(PGXCNodeHandle *handle);
#endif
extern int	pgxc_node_send_gxid(PGXCNodeHandle * handle, GlobalTransactionId gxid);
extern int	pgxc_node_send_cmd_id(PGXCNodeHandle *handle, CommandId cid);
extern int	pgxc_node_send_snapshot(PGXCNodeHandle * handle, Snapshot snapshot);
extern int	pgxc_node_send_timestamp(PGXCNodeHandle * handle, TimestampTz timestamp);
extern int
pgxc_node_send_prepare_timestamp(PGXCNodeHandle *handle, GlobalTimestamp timestamp);
extern int
pgxc_node_send_prefinish_timestamp(PGXCNodeHandle *handle, GlobalTimestamp timestamp);
extern int
pgxc_node_send_global_timestamp(PGXCNodeHandle *handle, GlobalTimestamp timestamp);

#ifdef __TBASE__
extern int	pgxc_node_send_coord_info(PGXCNodeHandle * handle, int coord_pid, TransactionId coord_vxid);
extern int	pgxc_node_receive(const int conn_count,
				  PGXCNodeHandle ** connections, struct timeval * timeout);
extern bool node_ready_for_query(PGXCNodeHandle *conn);
extern bool validate_handles(void);
#else
extern bool	pgxc_node_receive(const int conn_count,
				  PGXCNodeHandle ** connections, struct timeval * timeout);
#endif
extern int	pgxc_node_read_data(PGXCNodeHandle * conn, bool close_if_error);
extern int	pgxc_node_is_data_enqueued(PGXCNodeHandle *conn);

extern int	send_some(PGXCNodeHandle * handle, int len);
extern int	pgxc_node_flush(PGXCNodeHandle *handle);
extern int	pgxc_node_flush_read(PGXCNodeHandle *handle);

extern char get_message(PGXCNodeHandle *conn, int *len, char **msg);

extern void add_error_message(PGXCNodeHandle * handle, const char *message);

extern Datum pgxc_execute_on_nodes(int numnodes, Oid *nodelist, char *query);

extern void PGXCNodeSetParam(bool local, const char *name, const char *value,
				int flags);
extern void PGXCNodeResetParams(bool only_local);
extern char *PGXCNodeGetSessionParamStr(void);
extern char *PGXCNodeGetTransactionParamStr(void);
extern void pgxc_node_set_query(PGXCNodeHandle *handle, const char *set_query);
extern void RequestInvalidateRemoteHandles(void);
extern void RequestRefreshRemoteHandles(void);
extern bool PoolerMessagesPending(void);
extern void PGXCNodeSetConnectionState(PGXCNodeHandle *handle,
		DNConnectionState new_state);
extern bool PgxcNodeDiffBackendHandles(List **nodes_alter,
			   List **nodes_delete, List **nodes_add);
extern void PgxcNodeRefreshBackendHandlesShmem(List *nodes_alter);
extern void HandlePoolerMessages(void);
extern void pgxc_print_pending_data(PGXCNodeHandle *handle, bool reset);

#ifdef __TBASE__
void add_error_message_from_combiner(PGXCNodeHandle *handle, void *combiner_input);
void pgxc_set_coordinator_proc_pid(int proc_pid);
int pgxc_get_coordinator_proc_pid(void);
void pgxc_set_coordinator_proc_vxid(TransactionId proc_vxid);
TransactionId pgxc_get_coordinator_proc_vxid(void);
PGXCNodeHandle* find_ddl_leader_cn(void);
inline bool  is_ddl_leader_cn(char *leader_cn);
void CheckInvalidateRemoteHandles(void);
extern int pgxc_node_send_sessionid(PGXCNodeHandle * handle);
extern void SerializeSessionId(Size maxsize, char *start_address);
extern void StartParallelWorkerSessionId(char *address);
extern bool is_pgxc_handles_init(void);
void delete_leadercn_handle(PGXCNodeAllHandles *pgxc_connections,
						PGXCNodeHandle* leader_cn_handle);
#endif

#ifdef __AUDIT__
extern const char * PGXCNodeTypeString(char node_type);
#endif

#ifdef __AUDIT_FGA__
extern void PGXCGetCoordOidOthers(Oid *nodelist);
extern void PGXCGetAllDnOid(Oid *nodelist);
#endif

#endif /* PGXCNODE_H */

