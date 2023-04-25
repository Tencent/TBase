#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"

#include <stdio.h>
#include <sys/stat.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

#include "storage/procarray.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "utils/varlena.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"
#include "utils/builtins.h"

#include "executor/tuptable.h"
#include "pgxc/execRemote.h"
#include "pgxc/pgxcnode.h"
#include "access/tupdesc.h"
#include "access/htup_details.h"
#include "lib/stringinfo.h"

#include "access/gtm.h"
#include "datatype/timestamp.h"
#include "access/xact.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/poolmgr.h"
#include "utils/timestamp.h"
#include "catalog/pg_control.h"
#include "commands/dbcommands.h"

#include "utils/memutils.h"
#include "nodes/memnodes.h"

#ifdef XCP
#include "catalog/pg_type.h"
#include "catalog/pgxc_node.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "utils/snapmgr.h"
#endif
#ifdef PGXC
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#endif

#include "storage/fd.h"
#include "pgstat.h"
#include "access/xact.h"
#include "access/twophase.h"
#include "access/hash.h"

/*hash_create hash_search*/
#include "utils/hsearch.h"

#define TWOPHASE_RECORD_DIR "pg_2pc"
int  transaction_threshold = 200000;
#define MAXIMUM_CLEAR_FILE 10000
#define MAXIMUM_OUTPUT_FILE 1000
#define XIDPREFIX "_$XC$"
#define DEFAULT_CLEAN_TIME_INTERVAL 120
#define LEAST_CLEAN_TIME_INTERVAL     3 /* should not clean twophase trans prepared in 3s */
#define LEAST_CHECK_TIME_INTERVAL     1 /* should not check twophase trans prepared in 1s */

GlobalTimestamp clean_time_interval = DEFAULT_CLEAN_TIME_INTERVAL * USECS_PER_SEC;

PG_MODULE_MAGIC;

#define MAX_GID               64

#define CLEAN_CHECK_TIMES_DEFAULT    3
#define CLEAN_CHECK_INTERVAL_DEFAULT 100000

#define CLEAN_NODE_CHECK_TIMES       5
#define CLEAN_NODE_CHECK_INTERVAL    500000

#define MAX_DBNAME	64
#define GET_START_XID "startxid:"
#define GET_COMMIT_TIMESTAMP "global_commit_timestamp:"
#define GET_START_NODE "startnode:"
#define GET_NODE "nodes:"
#define GET_XID "\nxid:"
#define GET_READONLY "readonly"
#define GIDSIZE (200 + 24)
#define MAX_TWOPC_TXN 1000
#define STRING_BUFF_LEN 1024

#define MAX_CMD_LENGTH 120

#define XIDFOUND 1
#define XIDNOTFOUND -1
#define XIDEXECFAIL -2

#define FILEFOUND 1
#define FILEUNKOWN -1
#define FILENOTFOUND -2

#define INIT(x)\
do{\
	x = NULL;\
	x##_count = 0;\
	x##_size = 0;\
}while(0);

#define RPALLOC(x)\
do{\
    if (x##_size < x##_count+1)\
    {\
        int temp_size = (x##_size > 0) ? x##_size : 1;\
        if (NULL == x)\
        {\
			x = palloc0(2*temp_size*sizeof(*x));\
		}\
        else\
        {\
        	x = repalloc(x, 2*temp_size*sizeof(*x));\
        }\
    	x##_size = 2*temp_size;\
    }\
}while(0);

#define PALLOC(x, y)\
do{\
    RPALLOC(x);\
    x[x##_count] = y;\
    x##_count++;\
}while(0);

#define RFREE(x)\
do{\
    if (x##_size > 0)\
    {\
        pfree(x);\
    }\
    x = NULL;\
    x##_count = 0;\
    x##_size = 0;\
}while(0);
	
#define ENUM_TOCHAR_CASE(x)   case x: return(#x);

/*data structures*/
typedef enum TXN_STATUS
{
	TXN_STATUS_INITIAL = 0,	/* Initial */
	TXN_STATUS_PREPARED,
	TXN_STATUS_COMMITTED,
	TXN_STATUS_ABORTED,
	TXN_STATUS_INPROGRESS,
	TXN_STATUS_FAILED,		/* Error detected while interacting with the node */
	TXN_STATUS_UNKNOWN	/* Unknown: Frozen, running, or not started */
} TXN_STATUS;


typedef enum 
{
	UNDO = 0,
	ABORT,
	COMMIT
} OPERATION;

typedef enum
{
    TWOPHASE_FILE_EXISTS = 0,
    TWOPHASE_FILE_NOT_EXISTS,
    TWOPHASE_FILE_OLD, 
    TWOPHASE_FILE_ERROR
}TWOPHASE_FILE_STATUS;
	
typedef struct txn_info
{
	char			gid[MAX_GID];
	uint32			*xid;				/* xid used in prepare */
	TimestampTz		*prepare_timestamp;
	char			*owner;
    char            *participants;
	Oid				origcoord;			/* Original coordinator who initiated the txn */
    bool            after_first_phase;
    uint32          startxid;           /* xid in Original coordinator */
	bool			isorigcoord_part;	/* Is original coordinator a
										   participant? */
	int				num_dnparts;		/* Number of participant datanodes */
	int				num_coordparts;		/* Number of participant coordinators */
	int				*dnparts;			/* Whether a node was participant in the txn */
	int				*coordparts;
	TXN_STATUS		*txn_stat;			/* Array for each nodes */
	char			*msg;				/* Notice message for this txn. */
	GlobalTimestamp  global_commit_timestamp;	/* get global_commit_timestamp from node once it is committed*/

	TXN_STATUS		global_txn_stat;
	OPERATION		op;
	bool			op_issuccess;
    bool            is_readonly;
    bool            belong_abnormal_node;
}txn_info;

typedef struct database_info
{
	struct database_info *next;
	char *database_name;

    HTAB *all_txn_info;
#if 0 
	txn_info *head_txn_info;
	txn_info *last_txn_info;
#endif
} database_info;

typedef struct 
{
	int index;
	txn_info **txn;
	int txn_count;
	int txn_size;
	MemoryContext mycontext;
} print_txn_info;

typedef struct
{
	int index;
	int count;
	char **gid;
	int gid_count;
	int gid_size;
	char **database;
	int database_count;
	int database_size;
	char **global_status;
	int global_status_count;
	int global_status_size;
	char **status;
	int status_count;
	int status_size;
	MemoryContext mycontext;
} print_status;

typedef struct 
{
	char ***slot;	/*slot[i][j] stores value of row i, colum j*/
	int slot_count;	/*number of rows*/
	int slot_size;
	int attnum;
}TupleTableSlots;

/*global variable*/
static Oid	        *cn_node_list = NULL;
static Oid	        *dn_node_list = NULL;
static bool         *cn_health_map = NULL;
static bool         *dn_health_map = NULL;
static int	        cn_nodes_num = 0;
static int	        dn_nodes_num = 0;
static int	        pgxc_clean_node_count = 0;
static Oid	        my_nodeoid;
static 
database_info       *head_database_info = NULL;
static 
database_info       *last_database_info = NULL;
bool		        execute = false;
int                 total_twopc_txn = 0;

TimestampTz         current_time;
GlobalTimestamp     abnormal_time = InvalidGlobalTimestamp;
char                *abnormal_nodename = NULL;
Oid                 abnormal_nodeoid = InvalidOid;
bool                clear_2pc_belong_node = false;


/*function list*/
	/*plugin entry function*/

static bool check_node_health(Oid node_oid);
static Datum 
	 execute_query_on_single_node(Oid node, const char * query, int attnum, TupleTableSlots * tuples);
void DestroyTxnHash(void);
static void ResetGlobalVariables(void);

static Oid  
	 getMyNodeoid(void);
static void 
	 getDatabaseList(void);
static char* TTSgetvalue(TupleTableSlots *result, int tup_num, int field_num);
static void DropTupleTableSlots(TupleTableSlots *
Slots);
static void 
	 getTxnInfoOnNodesAll(void);
void getTxnInfoOnNode(Oid node);
void add_txn_info(char * dbname, Oid node_oid, uint32 xid, char * gid, char * owner, 
					  TimestampTz prepared_time, TXN_STATUS status);
TWOPHASE_FILE_STATUS GetTransactionPartNodes(txn_info * txn, Oid node_oid);
static txn_info *
	 find_txn(char *gid);
txn_info*	
	 make_txn_info(char * dbname, char * gid, char * owner);
database_info*	
	 find_database_info(char *database_name);
database_info*
	 add_database_info(char *database_name);
int	 find_node_index(Oid node_oid);
Oid  find_node_oid(int node_idx);
void getTxnInfoOnOtherNodesAll(void);
void getTxnInfoOnOtherNodesForDatabase(database_info *database);
void getTxnInfoOnOtherNodes(txn_info *txn);
int Get2PCXidByGid(Oid node_oid, char * gid, uint32 * transactionid);
int Get2PCFile(Oid node_oid, char * gid, uint32 * transactionid);

char *get2PCInfo(const char *tid);

void getTxnStatus(txn_info * txn, int node_idx);
void recover2PCForDatabaseAll(void);
void recover2PCForDatabase(database_info * db_info);
#if 0    
static bool 
	 setMaintenanceMode(bool status);
#endif
bool send_query_clean_transaction(PGXCNodeHandle * conn, txn_info * txn, const char * finish_cmd);
bool check_2pc_belong_node(txn_info * txn);
bool check_node_participate(txn_info * txn, int node_idx);

bool check_2pc_start_from_node(txn_info *txn);

void recover2PC(txn_info * txn);
TXN_STATUS 
	 check_txn_global_status(txn_info *txn);
bool clean_2PC_iscommit(txn_info *txn, bool is_commit, bool is_check);
bool clean_2PC_files(txn_info *txn);
void Init_print_txn_info(print_txn_info *print_txn);
void Init_print_stats_all(print_status *pstatus);
void Init_print_stats(txn_info * txn, char * database, print_status * pstatus);
static const char *
	 txn_status_to_string(TXN_STATUS status);
static const char *
	 txn_op_to_string(OPERATION op);
static void 
     CheckFirstPhase(txn_info *txn);
static void 
     get_transaction_handles(PGXCNodeAllHandles **pgxc_handles, txn_info *txn);
static void 
     get_node_handles(PGXCNodeAllHandles ** pgxc_handles, Oid nodeoid);

Datum	pg_clean_execute(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_clean_execute);
Datum	pg_clean_execute(PG_FUNCTION_ARGS)
{
#ifdef ACCESS_CONTROL_ATTR_NUM
#undef ACCESS_CONTROL_ATTR_NUM
#endif
#define ACCESS_CONTROL_ATTR_NUM  4
	FuncCallContext 	*funcctx;
	HeapTuple			tuple;		
	print_txn_info		*print_txn = NULL;
	txn_info 			*temp_txn;
	char				txn_gid[100];
	char				txn_status[100];
	char				txn_op[100];
	char				txn_op_issuccess[100];
	
	Datum		values[ACCESS_CONTROL_ATTR_NUM];
	bool		nulls[ACCESS_CONTROL_ATTR_NUM];
	
	if(!IS_PGXC_COORDINATOR)
	{
		elog(ERROR, "can only called on coordinator");
	}

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc	tupdesc;
		MemoryContext mycontext;
		funcctx = SRF_FIRSTCALL_INIT();
		
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		tupdesc = CreateTemplateTupleDesc(ACCESS_CONTROL_ATTR_NUM, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "gid",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "global_transaction_status",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "operation",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "operation_status",
						   TEXTOID, -1, 0);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);
		
		funcctx->user_fctx = (print_txn_info *)palloc0(sizeof(print_txn_info));
		print_txn = (print_txn_info *) funcctx->user_fctx;
	
		
		MemoryContextSwitchTo(oldcontext);
		mycontext = AllocSetContextCreate(funcctx->multi_call_memory_ctx,
												  "clean_check",
												  ALLOCSET_DEFAULT_MINSIZE,
												  ALLOCSET_DEFAULT_INITSIZE,
												  ALLOCSET_DEFAULT_MAXSIZE);
		oldcontext = MemoryContextSwitchTo(mycontext);
		
        /*clear Global*/
        ResetGlobalVariables();
        execute = true;

        clean_time_interval = PG_GETARG_INT32(0);
        if (LEAST_CLEAN_TIME_INTERVAL > clean_time_interval)
        {
            elog(WARNING, "least clean time interval is %ds",
                LEAST_CLEAN_TIME_INTERVAL);
            clean_time_interval = LEAST_CLEAN_TIME_INTERVAL;
        }
        clean_time_interval *= USECS_PER_SEC;
        
		/*get node list*/
		PgxcNodeGetOids(&cn_node_list, &dn_node_list, 
						&cn_nodes_num, &dn_nodes_num, true);
		pgxc_clean_node_count = cn_nodes_num + dn_nodes_num;
		my_nodeoid = getMyNodeoid();
		cn_health_map = palloc0(cn_nodes_num * sizeof(bool));
		dn_health_map = palloc0(dn_nodes_num * sizeof(bool));

		/*add my database info*/
		add_database_info(get_database_name(MyDatabaseId));

		/*get all info of 2PC transactions*/
		getTxnInfoOnNodesAll();

		/*get txn info on other nodes all*/
		getTxnInfoOnOtherNodesAll();

		/*recover all 2PC transactions*/
		recover2PCForDatabaseAll();

		Init_print_txn_info(print_txn);
		
		print_txn->mycontext = mycontext;
		
		MemoryContextSwitchTo(oldcontext);

	}
	
	funcctx = SRF_PERCALL_SETUP();	
	print_txn = (print_txn_info *) funcctx->user_fctx;
	
	if (print_txn->index < print_txn->txn_count)
	{
		temp_txn = print_txn->txn[print_txn->index];
		strncpy(txn_gid, temp_txn->gid, 100);
		strncpy(txn_status, txn_status_to_string(temp_txn->global_txn_stat), 100);
		strncpy(txn_op, txn_op_to_string(temp_txn->op), 100);
		if (temp_txn->op_issuccess)
			strncpy(txn_op_issuccess, "success", 100);
		else
			strncpy(txn_op_issuccess, "fail", 100);
		
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		values[0] = PointerGetDatum(cstring_to_text(txn_gid));
		values[1] = PointerGetDatum(cstring_to_text(txn_status));
		values[2] = PointerGetDatum(cstring_to_text(txn_op));
		values[3] = PointerGetDatum(cstring_to_text(txn_op_issuccess));
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		print_txn->index++;
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	else
	{
		
		//MemoryContextDelete(print_txn->mycontext);
		DestroyTxnHash();
		ResetGlobalVariables();
		SRF_RETURN_DONE(funcctx);
	}
}

/*
 * clear 2pc after oss detect abnormal node and restart it , 
 * only clear 2pc belong the abnormal node and before the abnormal time
 */
Datum	pg_clean_execute_on_node(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_clean_execute_on_node);
Datum	pg_clean_execute_on_node(PG_FUNCTION_ARGS)
{
#ifdef ACCESS_CONTROL_ATTR_NUM
#undef ACCESS_CONTROL_ATTR_NUM
#endif
#define ACCESS_CONTROL_ATTR_NUM  4
	FuncCallContext 	*funcctx;
	HeapTuple			tuple;		
	print_txn_info		*print_txn = NULL;
	txn_info 			*temp_txn;
	char				txn_gid[100];
	char				txn_status[100];
	char				txn_op[100];
	char				txn_op_issuccess[100];
	
	Datum		values[ACCESS_CONTROL_ATTR_NUM];
	bool		nulls[ACCESS_CONTROL_ATTR_NUM];
	
	if(!IS_PGXC_COORDINATOR)
	{
		elog(ERROR, "can only called on coordinator");
	}

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc	tupdesc;
		MemoryContext mycontext;
		funcctx = SRF_FIRSTCALL_INIT();
		
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		tupdesc = CreateTemplateTupleDesc(ACCESS_CONTROL_ATTR_NUM, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "gid",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "global_transaction_status",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "operation",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "operation_status",
						   TEXTOID, -1, 0);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);
		
		funcctx->user_fctx = (print_txn_info *)palloc0(sizeof(print_txn_info));
		print_txn = (print_txn_info *) funcctx->user_fctx;
	
		
		MemoryContextSwitchTo(oldcontext);
		mycontext = AllocSetContextCreate(funcctx->multi_call_memory_ctx,
												  "clean_check",
												  ALLOCSET_DEFAULT_MINSIZE,
												  ALLOCSET_DEFAULT_INITSIZE,
												  ALLOCSET_DEFAULT_MAXSIZE);
		oldcontext = MemoryContextSwitchTo(mycontext);
		
        /*clear Global*/
        ResetGlobalVariables();
        execute = true;
        clear_2pc_belong_node = true;

        abnormal_nodename = text_to_cstring(PG_GETARG_TEXT_P(0));
        abnormal_nodeoid = get_pgxc_nodeoid(abnormal_nodename);
        if (InvalidOid == abnormal_nodeoid)
        {
            elog(ERROR, "pg_clean_execute_on_node, cannot clear 2pc of invalid nodename '%s'", abnormal_nodename);
        }
        abnormal_time = PG_GETARG_INT64(1);
        current_time = GetCurrentTimestamp();
        if (abnormal_time >= current_time - LEAST_CLEAN_TIME_INTERVAL * USECS_PER_SEC)
        {
            elog(ERROR, "pg_clean_execute_on_node, least clean time interval is %ds, "
                "abnormal time: " INT64_FORMAT ", current_time: " INT64_FORMAT,
                LEAST_CLEAN_TIME_INTERVAL, abnormal_time, current_time);
        }
        
		/*get node list*/
		PgxcNodeGetOids(&cn_node_list, &dn_node_list, 
						&cn_nodes_num, &dn_nodes_num, true);
		pgxc_clean_node_count = cn_nodes_num + dn_nodes_num;
		my_nodeoid = getMyNodeoid();
		cn_health_map = palloc0(cn_nodes_num * sizeof(bool));
		dn_health_map = palloc0(dn_nodes_num * sizeof(bool));

		/*add my database info*/
		add_database_info(get_database_name(MyDatabaseId));

		/*get all info of 2PC transactions*/
		getTxnInfoOnNodesAll();

		/*get txn info on other nodes all*/
		getTxnInfoOnOtherNodesAll();

		/*recover all 2PC transactions*/
		recover2PCForDatabaseAll();

		Init_print_txn_info(print_txn);
		
		print_txn->mycontext = mycontext;
		
		MemoryContextSwitchTo(oldcontext);

	}
	
	funcctx = SRF_PERCALL_SETUP();	
	print_txn = (print_txn_info *) funcctx->user_fctx;
	
	if (print_txn->index < print_txn->txn_count)
	{
		temp_txn = print_txn->txn[print_txn->index];
		strncpy(txn_gid, temp_txn->gid, 100);
		strncpy(txn_status, txn_status_to_string(temp_txn->global_txn_stat), 100);
		strncpy(txn_op, txn_op_to_string(temp_txn->op), 100);
		if (temp_txn->op_issuccess)
			strncpy(txn_op_issuccess, "success", 100);
		else
			strncpy(txn_op_issuccess, "fail", 100);
		
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		values[0] = PointerGetDatum(cstring_to_text(txn_gid));
		values[1] = PointerGetDatum(cstring_to_text(txn_status));
		values[2] = PointerGetDatum(cstring_to_text(txn_op));
		values[3] = PointerGetDatum(cstring_to_text(txn_op_issuccess));
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		print_txn->index++;
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	else
	{
		DestroyTxnHash();
        pfree(abnormal_nodename);
		ResetGlobalVariables();
		SRF_RETURN_DONE(funcctx);
	}
}


Datum	pg_clean_check_txn(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_clean_check_txn);
Datum	pg_clean_check_txn(PG_FUNCTION_ARGS)
{
#ifdef ACCESS_CONTROL_ATTR_NUM
#undef ACCESS_CONTROL_ATTR_NUM
#endif
#define ACCESS_CONTROL_ATTR_NUM  4
	FuncCallContext 	*funcctx;
	HeapTuple			tuple;		
	print_status		*pstatus = NULL;
	
	Datum		values[ACCESS_CONTROL_ATTR_NUM];
	bool		nulls[ACCESS_CONTROL_ATTR_NUM];
	execute = false;
    
	if(!IS_PGXC_COORDINATOR)
	{
		elog(ERROR, "can only called on coordinator");
	}

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		MemoryContext mycontext;
		TupleDesc	tupdesc;
		funcctx = SRF_FIRSTCALL_INIT();
		
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		tupdesc = CreateTemplateTupleDesc(ACCESS_CONTROL_ATTR_NUM, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "gid",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "database",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "global_transaction_status",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "transaction_status_on_allnodes",
						   TEXTOID, -1, 0);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);
		
		funcctx->user_fctx = (print_status *)palloc0(sizeof(print_status));
		pstatus = (print_status *) funcctx->user_fctx;
		pstatus->index = pstatus->count = 0;
		pstatus->gid = NULL;
		pstatus->global_status = pstatus->status = (char **)NULL;
		pstatus->database = NULL;
		pstatus->mycontext = NULL;
	

		MemoryContextSwitchTo(oldcontext);

		mycontext = AllocSetContextCreate(funcctx->multi_call_memory_ctx,
												  "clean_check",
												  ALLOCSET_DEFAULT_MINSIZE,
												  ALLOCSET_DEFAULT_INITSIZE,
												  ALLOCSET_DEFAULT_MAXSIZE);
		oldcontext = MemoryContextSwitchTo(mycontext);

        /*clear Global*/
        ResetGlobalVariables();
        
        clean_time_interval = PG_GETARG_INT32(0);
        if (LEAST_CHECK_TIME_INTERVAL > clean_time_interval)
        {
            elog(WARNING, "least check time interval is %ds",
				LEAST_CHECK_TIME_INTERVAL);
            clean_time_interval = LEAST_CHECK_TIME_INTERVAL;
        }
        clean_time_interval *= USECS_PER_SEC;

		/*get node list*/
		PgxcNodeGetOids(&cn_node_list, &dn_node_list, 
						&cn_nodes_num, &dn_nodes_num, true);
        if (cn_node_list == NULL || dn_node_list == NULL)
            elog(ERROR, "pg_clean:fail to get cn_node_list and dn_node_list");
		pgxc_clean_node_count = cn_nodes_num + dn_nodes_num;
		my_nodeoid = getMyNodeoid();
		cn_health_map = palloc0(cn_nodes_num * sizeof(bool));
		dn_health_map = palloc0(dn_nodes_num * sizeof(bool));

		/*get all database info*/
		getDatabaseList();

		/*get all info of 2PC transactions*/
		getTxnInfoOnNodesAll();

		/*get txn info on other nodes all*/
		getTxnInfoOnOtherNodesAll();

		/*recover all 2PC transactions*/
		Init_print_stats_all(pstatus);
	
		pstatus->mycontext = mycontext;
	
		MemoryContextSwitchTo(oldcontext);

	}
	
	funcctx = SRF_PERCALL_SETUP();	
	pstatus = (print_status *) funcctx->user_fctx;
	
	if (pstatus->index < pstatus->count)
	{
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		values[0] = PointerGetDatum(cstring_to_text(pstatus->gid[pstatus->index]));
		values[1] = PointerGetDatum(cstring_to_text(pstatus->database[pstatus->index]));
		values[2] = PointerGetDatum(cstring_to_text(pstatus->global_status[pstatus->index]));
		values[3] = PointerGetDatum(cstring_to_text(pstatus->status[pstatus->index]));
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		pstatus->index++;
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	else
	{
		/*
		MemoryContextDelete(pstatus->mycontext);
		DropDatabaseInfo();
		*/
		DestroyTxnHash();
		ResetGlobalVariables();
		SRF_RETURN_DONE(funcctx);
	}
}

void DestroyTxnHash(void)
{
    database_info *dbinfo = head_database_info;
    while (dbinfo)
    {
        hash_destroy(dbinfo->all_txn_info);
        dbinfo = dbinfo->next;
    }
}

static void ResetGlobalVariables(void)
{
	cn_node_list = NULL;
	dn_node_list = NULL;
	cn_health_map = NULL;
	dn_health_map = NULL;
	cn_nodes_num = 0;
	dn_nodes_num = 0;
	pgxc_clean_node_count = 0;
	execute = false;
    total_twopc_txn = 0;

	head_database_info = last_database_info = NULL;

    current_time = 0;
    abnormal_time = InvalidGlobalTimestamp;
    abnormal_nodename = NULL;
    abnormal_nodeoid = InvalidOid;
    clear_2pc_belong_node = false;

}

static Oid getMyNodeoid(void)
{
	return get_pgxc_nodeoid(PGXCNodeName);
}

/* 
 * execute_query_on_single_node -- execute query on certain node and get results
 * input: 	node oid, execute query, number of attribute in results, results
 * return:	(Datum) 0
 */
static Datum
execute_query_on_single_node(Oid node, const char *query, int attnum, TupleTableSlots *tuples)  //delete numnodes, delete nodelist, insert node
{
	int 		ii;
	bool		issuccess = false;

	/*check health of node*/
	bool ishealthy = check_node_health(node);

#ifdef XCP
	EState				*estate;
	MemoryContext		oldcontext;
	RemoteQuery			*plan;
	RemoteQueryState	*pstate;
	TupleTableSlot		*result = NULL;
	Var			   		*dummy;
	char ntype = PGXC_NODE_NONE;

	/*
	 * Make up RemoteQuery plan node
	 */
	plan = makeNode(RemoteQuery);
	plan->combine_type = COMBINE_TYPE_NONE;
	plan->exec_nodes = makeNode(ExecNodes);
	plan->exec_type = EXEC_ON_NONE;

	plan->exec_nodes->nodeList = lappend_int(plan->exec_nodes->nodeList,
		PGXCNodeGetNodeId(node, &ntype));
	if (ntype == PGXC_NODE_NONE)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Unknown node Oid: %u", node)));
	else if (ntype == PGXC_NODE_COORDINATOR) 
	{
		plan->exec_type = EXEC_ON_COORDS;
	}
	else
	{
		plan->exec_type = EXEC_ON_DATANODES;
	}

	plan->sql_statement = (char *)query;
	plan->force_autocommit = false;
	/*
	 * We only need the target entry to determine result data type.
	 * So create dummy even if real expression is a function.
	 */
	for (ii = 1; ii <= attnum; ii++)
	{
		dummy = makeVar(1, ii, TEXTOID, 0, InvalidOid, 0);
		plan->scan.plan.targetlist = lappend(plan->scan.plan.targetlist,
										  makeTargetEntry((Expr *) dummy, ii, NULL, false));
	}
	/* prepare to execute */
	estate = CreateExecutorState();
	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);
	estate->es_snapshot = GetActiveSnapshot();
	pstate = ExecInitRemoteQuery(plan, estate, 0);
	MemoryContextSwitchTo(oldcontext);

	/*execute query on node when node is healthy*/
	INIT(tuples->slot);
	tuples->attnum = 0;	
	if (ishealthy)
	{
		int i_tuple = 0;
		int i_attnum = 0;
		issuccess = true;
		result = ExecRemoteQuery((PlanState *) pstate);
		tuples->attnum = attnum;
		while (result != NULL && !TupIsNull(result))
		{
			slot_getallattrs(result); 
			RPALLOC(tuples->slot);
			tuples->slot[i_tuple] = (char **) palloc0(attnum * sizeof(char *));
		
			for (i_attnum = 0; i_attnum < attnum; i_attnum++)
			{
				/*if (result->tts_values[i_attnum] != (Datum)0)*/
				if (result->tts_isnull[i_attnum] == false)
				{
					tuples->slot[i_tuple][i_attnum] = text_to_cstring(DatumGetTextP(result->tts_values[i_attnum]));
				}
				else
				{
					tuples->slot[i_tuple][i_attnum] = NULL;
				}
			}
			tuples->slot_count++;

			result = ExecRemoteQuery((PlanState *) pstate);
			i_tuple++;
		}
	}
	ExecEndRemoteQuery(pstate);
#endif
	return issuccess == true ? (Datum) 1 : (Datum) 0;
}

static bool check_node_health(Oid node_oid)
{
	int i;
	bool ishealthy = false;
	
	PoolPingNodeRecheck(node_oid);
	PgxcNodeGetHealthMap(cn_node_list, dn_node_list, 
						&cn_nodes_num, &dn_nodes_num, 
						cn_health_map, dn_health_map);
	if (get_pgxc_nodetype(node_oid) == 'C')
	{
		for (i = 0; i < cn_nodes_num; i++)
		{
			if (cn_node_list[i] == node_oid)
			{
				ishealthy = cn_health_map[i];
			}
		}
	}
	else
	{
		for (i = 0; i < dn_nodes_num; i++)
		{
			if (dn_node_list[i] == node_oid)
			{
				ishealthy = dn_health_map[i];
			}
		}
	}
	return ishealthy;
}

static void getDatabaseList(void)
{
	int i;
	TupleTableSlots result_db;
	const char *query_db = "select datname::text from pg_database;";
	/*add datname into tail of head_database_info*/
	if (execute_query_on_single_node(my_nodeoid, query_db, 1, &result_db) == (Datum) 1)
	{
		for (i = 0; i < result_db.slot_count; i++)
		{
			if (TTSgetvalue(&result_db, i, 0))
			{
				add_database_info(TTSgetvalue(&result_db, i, 0));
			}
		}
	}
	else
	{
		elog(LOG, "pg_clean: failed get database list on node %s", get_pgxc_nodename(my_nodeoid));
	}
	DropTupleTableSlots(&result_db);
}

/* 
 * TTSgetvalue -- get attribute from TupleTableSlots
 * input: 	result, index of tuple, index of field
 * return:	attribute result
 */
static char * TTSgetvalue(TupleTableSlots *result, int tup_num, int field_num)
{
	return result->slot[tup_num][field_num];
}

static void DropTupleTableSlots(TupleTableSlots *
Slots)
{
	int i;
	int j;
	for (i = 0; i < Slots->slot_count; i++)
	{
		if (Slots->slot[i])
		{
			for (j = 0; j < Slots->attnum; j++)
			{
				if (Slots->slot[i][j])
				{
					pfree(Slots->slot[i][j]);
				}
			}
			pfree(Slots->slot[i]);
		}
	}
	RFREE(Slots->slot);
	Slots->attnum = 0;
	return;
}

static void getTxnInfoOnNodesAll(void)
{
	int i;
	current_time = GetCurrentTimestamp();
	/*upload 2PC transaction from CN*/
	for (i = 0; i < cn_nodes_num; i++)
	{
        if (total_twopc_txn >= MAX_TWOPC_TXN)
            return;
		getTxnInfoOnNode(cn_node_list[i]);
	}

	/*upload 2PC transaction from DN*/
	for (i = 0; i < dn_nodes_num; i++)
	{
        if (total_twopc_txn >= MAX_TWOPC_TXN)
            return;
		getTxnInfoOnNode(dn_node_list[i]);
	}
}

void getTxnInfoOnNode(Oid node)
{
	int i;
	TupleTableSlots result_txn;
	Datum execute_res;
	char query_execute[1024];
	const char *query_txn_status = "select transaction::text, gid::text, owner::text, database::text, timestamptz_out(prepared)::text "
										  "from pg_prepared_xacts;";
	const char *query_txn_status_execute = "select transaction::text, gid::text, owner::text, database::text, timestamptz_out(prepared)::text "
										  		  "from pg_prepared_xacts where database = '%s';";
	snprintf(query_execute, 1024, query_txn_status_execute, get_database_name(MyDatabaseId));

	if (execute)
		execute_res = execute_query_on_single_node(node, query_execute, 5, &result_txn);
	else
		execute_res = execute_query_on_single_node(node, query_txn_status, 5, &result_txn);
	
	if (execute_res == (Datum) 1)
	{
		for (i = 0; i < result_txn.slot_count; i++)
		{
			uint32	xid;
			char*	gid;
			char*	owner;
			char*	datname;
			TimestampTz	prepared_time;
			
			/*read results from each tuple*/
			xid		= strtoul(TTSgetvalue(&result_txn, i, 0), NULL, 10);
			gid		= TTSgetvalue(&result_txn, i, 1);
			owner	= TTSgetvalue(&result_txn, i, 2);
			datname	= TTSgetvalue(&result_txn, i, 3);
			prepared_time = DatumGetTimestampTz(DirectFunctionCall3(timestamptz_in,
												CStringGetDatum(TTSgetvalue(&result_txn, i, 4)),
												ObjectIdGetDatum(InvalidOid),
												Int32GetDatum(-1)));
			
			if (gid == NULL)
			{
				elog(ERROR, "node(%d) gid is null, xid: %d", node, xid);
			}
			else if (owner == NULL)
			{
				elog(ERROR, "node(%d) owner is null, xid: %d, gid: %s",
					node, xid, gid);
			}
			else if (datname == NULL)
			{
				elog(ERROR, "node(%d) db name is null, xid: %d, gid: %s, owner: %s",
					node, xid, gid, owner);
			}

			/*add txn to database*/
			add_txn_info(datname, node, xid, gid, owner, prepared_time, TXN_STATUS_PREPARED);
            if (total_twopc_txn >= MAX_TWOPC_TXN)
            {
                break;
            }
		}
	}
	else
	{
		elog(LOG, "pg_clean: failed get database list on node %s", get_pgxc_nodename(node));
	}
	DropTupleTableSlots(&result_txn);
}

void add_txn_info(char* dbname, Oid node_oid, uint32 xid, char * gid, 
						char * owner, TimestampTz prepared_time, TXN_STATUS status)
{
	txn_info *txn = NULL;
	int	nodeidx;

	if ((txn = find_txn(gid)) == NULL)
	{
		txn = make_txn_info(dbname, gid, owner);
        total_twopc_txn++;
		if (txn == NULL)
		{
			/*no more memory*/
			elog(ERROR, "there is no more memory for palloc a 2PC transaction");
		}
	}
	nodeidx = find_node_index(node_oid);
	txn->txn_stat[nodeidx] = status;
	txn->xid[nodeidx] = xid;
	txn->prepare_timestamp[nodeidx] = prepared_time;
	if (nodeidx < cn_nodes_num)
	{
		txn->coordparts[nodeidx] = 1;
		txn->num_coordparts++;
	}
	else
	{
		txn->dnparts[nodeidx-cn_nodes_num] = 1;
		txn->num_dnparts++;
	}
	return;
}

TWOPHASE_FILE_STATUS GetTransactionPartNodes(txn_info *txn, Oid node_oid)
{
	/*get all the participates and initiate to each transactions*/
	TWOPHASE_FILE_STATUS res = TWOPHASE_FILE_NOT_EXISTS;
	TupleTableSlots result;
	char *partnodes = NULL;
    char *startnode = NULL;
    char *file_content = NULL;
    uint32 startxid = 0;
    char *str_startxid = NULL;
    char *str_timestamp = NULL;
	char *temp = NULL;
	Oid	 temp_nodeoid;
	char temp_nodetype;
	int  temp_nodeidx;
	char stmt[1024];
	static const char *STMT_FORM = "select pgxc_get_2pc_file('%s')::text";
	snprintf(stmt, 1024, STMT_FORM, txn->gid, txn->gid, txn->gid, txn->gid);
    
	if (execute_query_on_single_node(node_oid, stmt, 1, &result) == (Datum) 1)
	{
		if (result.slot_count && TTSgetvalue(&result, 0, 0))
#if 0
            TTSgetvalue(&result, 0, 0) && 
            TTSgetvalue(&result, 0, 1) && 
            TTSgetvalue(&result, 0, 2))
#endif
		{
            file_content = TTSgetvalue(&result, 0, 0);    
            
            if (!IsXidImplicit(txn->gid) && strstr(file_content, GET_READONLY))
            {
                txn->is_readonly = true;
                txn->global_txn_stat = TXN_STATUS_COMMITTED;
                DropTupleTableSlots(&result);
	            return TWOPHASE_FILE_EXISTS;
            }
            startnode = strstr(file_content, GET_START_NODE);
            str_startxid = strstr(file_content, GET_START_XID);
            partnodes = strstr(file_content, GET_NODE);
            temp = strstr(file_content, GET_COMMIT_TIMESTAMP);
            
            /* get the last global_commit_timestamp */
            while (temp)
            {
                str_timestamp = temp;
                temp += strlen(GET_COMMIT_TIMESTAMP);
                temp = strstr(temp, GET_COMMIT_TIMESTAMP);
            }
            
            if (startnode)
            {
                startnode += strlen(GET_START_NODE);
                startnode = strtok(startnode, "\n");
                txn->origcoord = get_pgxc_nodeoid(startnode);
            }
            
            if (str_startxid)
            {
                str_startxid += strlen(GET_START_XID);
                str_startxid = strtok(str_startxid, "\n");
                startxid = strtoul(str_startxid, NULL, 10);
                txn->startxid = startxid;
            }
            
            if (partnodes)
            {
                partnodes += strlen(GET_NODE);
                partnodes = strtok(partnodes, "\n");
                txn->participants = (char *) palloc0(strlen(partnodes) + 1);
                strncpy(txn->participants, partnodes, strlen(partnodes) + 1);
            }
            
            if (NULL == startnode || NULL == str_startxid)
            {
                res = TWOPHASE_FILE_OLD;
                DropTupleTableSlots(&result);
                return res;
            }

            if (NULL == partnodes)
            {
                res = TWOPHASE_FILE_ERROR;
                DropTupleTableSlots(&result);
                return res;
            }

            if (str_timestamp)
            {
                str_timestamp += strlen(GET_COMMIT_TIMESTAMP);
                str_timestamp = strtok(str_timestamp, "\n");
                txn->global_commit_timestamp = strtoull(str_timestamp, NULL, 10);
            }
            
            elog(DEBUG1, "get 2pc txn:%s partnodes in nodename: %s (nodeoid:%u) result: partnodes:%s, startnode:%s, startnodeoid:%u, startxid:%u", 
                txn->gid, get_pgxc_nodename(node_oid), node_oid, partnodes, startnode, txn->origcoord, startxid);
            /* in explicit transaction startnode participate the transaction */
            if (strstr(partnodes, startnode) || !IsXidImplicit(txn->gid))
            {
                txn->isorigcoord_part = true;
            }
            else
            {
                txn->isorigcoord_part = false;
            }
            
			res = TWOPHASE_FILE_EXISTS;
			txn->num_coordparts = 0;
			txn->num_dnparts = 0;
			temp = strtok(partnodes,", ");
			while(temp)
			{
				/*check node type*/
				temp_nodeoid = get_pgxc_nodeoid(temp);
                if (temp_nodeoid == InvalidOid)
                {
                    res = TWOPHASE_FILE_ERROR;
                    break;
                }
				temp_nodetype = get_pgxc_nodetype(temp_nodeoid);
				temp_nodeidx = find_node_index(temp_nodeoid);
				
				switch (temp_nodetype)
				{
					case 'C':
						txn->coordparts[temp_nodeidx] = 1;
						txn->num_coordparts++;
						break;
					case 'D':
						txn->dnparts[temp_nodeidx-cn_nodes_num] = 1;
						txn->num_dnparts++;
						break;
					default:
						elog(ERROR,"nodetype of %s is not 'C' or 'D'", temp);
						break;
				}
				temp = strtok(NULL,", ");
			}
		}
	}
	else
	{
		elog(LOG, "pg_clean: failed get database list on node %s", get_pgxc_nodename(node_oid));
		res = TWOPHASE_FILE_ERROR;
	}
	DropTupleTableSlots(&result);
	return res;
}

static txn_info *find_txn(char *gid)
{
  bool found;
  database_info *cur_db;
  txn_info *txn;

  for (cur_db = head_database_info; cur_db; cur_db = cur_db->next)
  {
#if 0
	  for (cur_txn = cur_db->head_txn_info; cur_txn; cur_txn = cur_txn->next)
	  {
		  if (0 == strcmp(cur_txn->gid, gid))
			  return cur_txn;
	  }
#endif
      txn = (txn_info *)hash_search(cur_db->all_txn_info, (void *)gid, HASH_FIND, &found);
      if (found)
        return txn;
  }
  return NULL;
}

txn_info* make_txn_info(char* dbname, char* gid, char* owner)
{
    bool found;
    txn_info *txn_insert_pos = NULL;
	database_info *dbinfo;
	txn_info *txn;

	dbinfo = add_database_info(dbname);
	txn = (txn_info *)palloc0(sizeof(txn_info));
	if (txn == NULL)
		return NULL;
	//txn->next = NULL;
	
	//txn->gid = (char *)palloc0(strlen(gid)+1);
	strncpy(txn->gid, gid, strlen(gid)+1);
	txn->owner = (char *)palloc0(strlen(owner)+1);
	strncpy(txn->owner, owner, strlen(owner)+1);
	
	txn->txn_stat = (TXN_STATUS *)palloc0(sizeof(TXN_STATUS) * pgxc_clean_node_count);
	txn->xid = (uint32 *)palloc0(sizeof(uint32) * pgxc_clean_node_count);
	txn->prepare_timestamp = (TimestampTz *)palloc0(sizeof(TimestampTz) * pgxc_clean_node_count);
	txn->coordparts = (int *)palloc0(cn_nodes_num * sizeof(int));
	
	txn->dnparts = (int *)palloc0(dn_nodes_num * sizeof(int));
	if (txn->gid == NULL || txn->owner == NULL || txn->txn_stat == NULL
		|| txn->xid == NULL || txn->coordparts == NULL || txn->dnparts == NULL || txn->prepare_timestamp == NULL)
	{
		pfree(txn);
		return(NULL);
	}

    txn_insert_pos = (txn_info *)hash_search(dbinfo->all_txn_info, 
                   (void *)txn->gid, HASH_ENTER, &found);
    if (!found)
        memcpy(txn_insert_pos, txn, sizeof(txn_info));

#if 0        
	if (dbinfo->head_txn_info == NULL)
	{
		dbinfo->head_txn_info = dbinfo->last_txn_info = txn;
	}
	else
	{
		dbinfo->last_txn_info->next = txn;
		dbinfo->last_txn_info = txn;
	}
#endif

	return txn_insert_pos;
}

database_info *find_database_info(char *database_name)
{
	database_info *cur_database_info = head_database_info;

	for (;cur_database_info; cur_database_info = cur_database_info->next)
	{
		if(cur_database_info->database_name &&
		   database_name && 
		   strcmp(cur_database_info->database_name, database_name) == 0)
			return(cur_database_info);
	}
	return(NULL);
}

database_info *add_database_info(char *database_name)
{
	database_info *rv;
    HASHCTL txn_ctl;
    char tabname[STRING_BUFF_LEN];

	if ((rv = find_database_info(database_name)) != NULL)
		return rv;		/* Already in the list */
	rv = (database_info *)palloc0(sizeof(database_info));
	if (rv == NULL)
		return NULL;
	rv->next = NULL;
	rv->database_name = (char *)palloc0(strlen(database_name) + 1);
	strncpy(rv->database_name, database_name, strlen(database_name) + 1);
	if (rv->database_name == NULL)
	{
		pfree(rv);
		return NULL;
	}
#if 0    
	rv->head_txn_info = NULL;
	rv->last_txn_info = NULL;
#endif

    snprintf(tabname, STRING_BUFF_LEN, "%s txn info", rv->database_name);
    txn_ctl.keysize = MAX_GID;
    txn_ctl.entrysize = sizeof(txn_info); 
    rv->all_txn_info = hash_create(tabname, 64, 
                                   &txn_ctl, HASH_ELEM);
	if (head_database_info == NULL)
	{
		head_database_info = last_database_info = rv;
		return rv;
	}
	else
	{
		last_database_info->next = rv;
		last_database_info = rv;
		return rv;
	}
}

int find_node_index(Oid node_oid)
{
	int res = -1;
	int i;
	if (get_pgxc_nodetype(node_oid) == 'C')
	{
		for (i = 0; i < cn_nodes_num; i++)
		{
			if (node_oid == cn_node_list[i])
			{
				res = i;
				break;
			}
		}
	}
	else
	{
		for (i = 0; i < dn_nodes_num; i++)
		{
			if (node_oid == dn_node_list[i])
			{
				res = i+cn_nodes_num;
				break;
			}
		}
	}
	return res;
}

Oid find_node_oid(int node_idx)
{
	return (node_idx < cn_nodes_num) ? cn_node_list[node_idx] :
									   dn_node_list[node_idx-cn_nodes_num];
}

void getTxnInfoOnOtherNodesAll(void)
{
	database_info *cur_database;

	for (cur_database = head_database_info; cur_database; cur_database = cur_database->next)
	{
		getTxnInfoOnOtherNodesForDatabase(cur_database);
	}
}

void getTxnInfoOnOtherNodesForDatabase(database_info *database)
{
	txn_info *cur_txn;
	HASH_SEQ_STATUS status;
    HTAB *txn = database->all_txn_info;
	hash_seq_init(&status, txn);

    while ((cur_txn = (txn_info *) hash_seq_search(&status)) != NULL)
    {
		getTxnInfoOnOtherNodes(cur_txn);
    }
#if 0
	for (cur_txn = database->head_txn_info; cur_txn; cur_txn = cur_txn->next)
	{
		getTxnInfoOnOtherNodes(cur_txn);
	}
#endif
}

void getTxnInfoOnOtherNodes(txn_info *txn)
{
	int ii;
    int ret;
	char node_type;
    TWOPHASE_FILE_STATUS status = TWOPHASE_FILE_NOT_EXISTS;
    Oid node_oid;
    uint32 transactionid = 0;
    char gid[MAX_GID];
    char *ptr = NULL;

    if (IsXidImplicit(txn->gid))
    {
        strncpy(gid, txn->gid, strlen(txn->gid)+1);
        ptr = strtok(gid, ":");
        ptr = strtok(NULL, ":");
        node_oid = get_pgxc_nodeoid(ptr);
        status = GetTransactionPartNodes(txn, node_oid);
    }
    else
    {
        for (ii = 0; ii < cn_nodes_num + dn_nodes_num; ii++)
        {
            if (ii < cn_nodes_num)
            {
                status = GetTransactionPartNodes(txn, cn_node_list[ii]);
                if (TWOPHASE_FILE_EXISTS == status || 
                    TWOPHASE_FILE_OLD == status || 
                    TWOPHASE_FILE_ERROR == status)
                {
                    node_oid = cn_node_list[ii];
                    break;
                }
            }
            else
            {
                status = GetTransactionPartNodes(txn, dn_node_list[ii - cn_nodes_num]);
                if (TWOPHASE_FILE_EXISTS == status || 
                    TWOPHASE_FILE_OLD == status || 
                    TWOPHASE_FILE_ERROR == status)
                {
                    node_oid = dn_node_list[ii - cn_nodes_num];
                    break;
                }
            }
        }
        
        /* since there may be explicit readonly  twophase transactions */
        if (txn->is_readonly)
        {
            return;
        }
        if (TWOPHASE_FILE_EXISTS == status && 
            InvalidGlobalTimestamp == txn->global_commit_timestamp && 
            node_oid != txn->origcoord)
        {
            status = GetTransactionPartNodes(txn, txn->origcoord);
        }

    }
    
    if (TWOPHASE_FILE_EXISTS != status)
    {
        /*
         * if 2pc file not exists in all nodes, the trans did not pass the prepared phase, 
         * 
         */
        txn->global_txn_stat = (TWOPHASE_FILE_NOT_EXISTS == status) ? 
                                TXN_STATUS_ABORTED : TXN_STATUS_UNKNOWN;
        return;
    }


    /* judge the range of global status */
    CheckFirstPhase(txn);

	for (ii = 0; ii < pgxc_clean_node_count; ii++)
	{
		if (txn->txn_stat[ii] == TXN_STATUS_INITIAL)
		{
			/*check node ii is 'C' or 'D'*/
            node_oid = find_node_oid(ii);
            if (node_oid == txn->origcoord)
                continue;
			node_type = get_pgxc_nodetype(node_oid);
			if (node_type == 'C' && txn->coordparts[ii] != 1)
				continue;
			if (node_type == 'D' && txn->dnparts[ii - cn_nodes_num] != 1)
				continue;
			/*check coordparts or dnparts*/
			if (txn->xid[ii] == 0)
			{
                ret = Get2PCXidByGid(node_oid, txn->gid, &transactionid);
                if (ret == XIDFOUND)
                {
                    txn->xid[ii] = transactionid;
                    if (txn->xid[ii] > 0)
                        getTxnStatus(txn, ii);
                }
                else if (ret == XIDNOTFOUND)
                {
                    if (txn->after_first_phase)
                        txn->txn_stat[ii] = TXN_STATUS_COMMITTED;
                }
                else
                    txn->txn_stat[ii] = TXN_STATUS_UNKNOWN;

			}
		}
	}
}

/*get xid by gid on node_oid*/
int Get2PCXidByGid(Oid node_oid, char *gid, uint32 *transactionid)
{
    int ret = XIDFOUND;
	TupleTableSlots result;
	uint32 xid = 0;
	static const char *STMT_FORM = "select pgxc_get_2pc_xid('%s')::text;";
	char stmt[100];
	snprintf(stmt, 100, STMT_FORM, gid);
	/*if exist get xid by gid on node_oid*/
	if (execute_query_on_single_node(node_oid, stmt, 1, &result) != (Datum) 0)
	{
		if (result.slot_count)
		{
			if (TTSgetvalue(&result, 0, 0))
			{
				xid = strtoul(TTSgetvalue(&result, 0, 0), NULL, 10);
                *transactionid = xid;
                if (xid == 0)
                    ret = XIDNOTFOUND;
			}
            else
                ret = XIDNOTFOUND;
		}
		else
			ret = XIDNOTFOUND;
	}
	else
		ret = XIDEXECFAIL;
	DropTupleTableSlots(&result);
	return ret;
}

int Get2PCFile(Oid node_oid, char * gid, uint32 * transactionid)
{
    int ret = FILEFOUND;
	TupleTableSlots result;
	static const char *STMT_FORM = "select pgxc_get_2pc_file('%s')::text;";
	char stmt[100];
	snprintf(stmt, 100, STMT_FORM, gid);
	/*if exist get xid by gid on node_oid*/
	if (execute_query_on_single_node(node_oid, stmt, 1, &result) != (Datum) 0)
	{
		if (result.slot_count)
		{
			if (!TTSgetvalue(&result, 0, 0))
			{
                ret = FILENOTFOUND;
			}
            else
            {
                ret = FILEFOUND;
            }
		}
		else
			ret = FILENOTFOUND;
	}
	else
		ret = FILEUNKOWN;
	DropTupleTableSlots(&result);
	return ret;
}


void getTxnStatus(txn_info *txn, int node_idx)
{
	Oid				node_oid;
	char			stmt[1024];
	char			*att1;
	TupleTableSlots result;

	static const char *STMT_FORM = "SELECT pgxc_is_committed('%d'::xid)::text";
	snprintf(stmt, 1024, STMT_FORM, txn->xid[node_idx], txn->xid[node_idx]);

	node_oid = find_node_oid(node_idx);
	if (0 != execute_query_on_single_node(node_oid, stmt, 1, &result))
	{
		att1 = TTSgetvalue(&result, 0, 0);
		
		if (att1)
		{
			if (strcmp(att1, "true") == 0)
			{
				txn->txn_stat[node_idx] = TXN_STATUS_COMMITTED;
			}
			else
				txn->txn_stat[node_idx] = TXN_STATUS_ABORTED;
		}
		else
		{
            txn->txn_stat[node_idx] = TXN_STATUS_INITIAL;
		}
	}
	else
		txn->txn_stat[node_idx] = TXN_STATUS_UNKNOWN;
	DropTupleTableSlots(&result);
}

char *get2PCInfo(const char *tid)
{
    char *result = NULL;
    char *info = NULL;
    int size = 0;
    File fd = -1;
    int ret = -1;
    struct stat filestate;
    char path[MAXPGPATH];
    
    info = get_2pc_info_from_cache(tid);
    if (NULL != info)
    {
        size = strlen(info);
        result = (char *)palloc0(size + 1);
        memcpy(result, info, size);
        return result;
    }

    elog(DEBUG1, "try to get 2pc info from disk, tid: %s", tid);
    
    snprintf(path, MAXPGPATH, TWOPHASE_RECORD_DIR "/%s", tid);
    if(access(path, F_OK) == 0)
    {
    	if(stat(path, &filestate) == -1)
    	{
    		ereport(ERROR,
    			(errcode_for_file_access(),
    			errmsg("could not get status of file \"%s\"", path)));
    	}
        
        size = filestate.st_size;

        if (0 == size) 
        {
            return NULL;
        }

        result = (char *)palloc0(size + 1);

        fd = PathNameOpenFile(path, O_RDONLY, S_IRUSR | S_IWUSR);
    	if (fd < 0)
    	{   
            pfree(result);
    		ereport(ERROR,
    			(errcode_for_file_access(),
    			errmsg("could not open file \"%s\" for read", path)));
    	} 

        ret = FileRead(fd, result, size, WAIT_EVENT_BUFFILE_READ);
        if(ret != size)
    	{
            pfree(result);
    		ereport(ERROR,
    			(errcode_for_file_access(),
    			errmsg("could not read file \"%s\"", path)));
    	}

        FileClose(fd);
        return result;
    }

    return NULL;
}

Datum pgxc_get_2pc_file(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pgxc_get_2pc_file);
Datum pgxc_get_2pc_file(PG_FUNCTION_ARGS)
{
    char *tid = NULL;
    char *result = NULL;
    text *t_result = NULL;

    tid = text_to_cstring(PG_GETARG_TEXT_P(0));
    result = get2PCInfo(tid);
    if (NULL != result)
		{
			t_result = cstring_to_text(result);
        pfree(result);
			return PointerGetDatum(t_result);
		}
    PG_RETURN_NULL();
}


Datum pgxc_get_2pc_nodes(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pgxc_get_2pc_nodes);
Datum pgxc_get_2pc_nodes(PG_FUNCTION_ARGS)
{
    char *tid = NULL;
    char *result = NULL;
    char *nodename = NULL;
	text *t_result = NULL;
    
    tid = text_to_cstring(PG_GETARG_TEXT_P(0));
    result = get2PCInfo(tid);
    if (NULL != result)
		{
			nodename = strstr(result, GET_NODE);
        if (NULL != nodename)
			{
				nodename += strlen(GET_NODE);
				nodename = strtok(nodename, "\n");
				t_result = cstring_to_text(nodename);
            pfree(result);
				return PointerGetDatum(t_result);
			}
		}

    PG_RETURN_NULL();
}

Datum pgxc_get_2pc_startnode(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pgxc_get_2pc_startnode);
Datum pgxc_get_2pc_startnode(PG_FUNCTION_ARGS)
{
    char *tid = NULL;
    char *result = NULL;
    char *nodename = NULL;
	text *t_result = NULL;
    
    tid = text_to_cstring(PG_GETARG_TEXT_P(0));
    result = get2PCInfo(tid);
    if (NULL != result)
		{
			nodename = strstr(result, GET_START_NODE);
        if (NULL != nodename)
			{
				nodename += strlen(GET_START_NODE);
				nodename = strtok(nodename, "\n");
				t_result = cstring_to_text(nodename);
            pfree(result);
				return PointerGetDatum(t_result);

		}
    }
    PG_RETURN_NULL();
}

Datum pgxc_get_2pc_startxid(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pgxc_get_2pc_startxid);
Datum pgxc_get_2pc_startxid(PG_FUNCTION_ARGS)
{
    char *tid = NULL;
    char *result = NULL;
    char *startxid = NULL;
	text *t_result = NULL;
    
    tid = text_to_cstring(PG_GETARG_TEXT_P(0));
    result = get2PCInfo(tid);
    if (NULL != result)
		{
			startxid = strstr(result, GET_START_XID);
        if (NULL != startxid)
			{
				startxid += strlen(GET_START_XID);
				startxid = strtok(startxid, "\n");
				t_result = cstring_to_text(startxid);
            pfree(result);
				return PointerGetDatum(t_result);
			}
		}
    PG_RETURN_NULL();
}


Datum pgxc_get_2pc_commit_timestamp(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pgxc_get_2pc_commit_timestamp);
Datum pgxc_get_2pc_commit_timestamp(PG_FUNCTION_ARGS)
{
    char *tid = NULL;
    char *result = NULL;
    char *commit_timestamp = NULL;
	text *t_result = NULL;
    
    tid = text_to_cstring(PG_GETARG_TEXT_P(0));
    result = get2PCInfo(tid);
    if (NULL != result)
		{
			commit_timestamp = strstr(result, GET_COMMIT_TIMESTAMP);
        if (NULL != commit_timestamp)
			{
				commit_timestamp += strlen(GET_COMMIT_TIMESTAMP);
				commit_timestamp = strtok(commit_timestamp, "\n");
				t_result = cstring_to_text(commit_timestamp);
            pfree(result);
				return PointerGetDatum(t_result);
			}
		}
    PG_RETURN_NULL();
}



Datum pgxc_get_2pc_xid(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pgxc_get_2pc_xid);
Datum pgxc_get_2pc_xid(PG_FUNCTION_ARGS)
{
    char *tid = NULL;
    char *result = NULL;
    char *str_xid = NULL;
    GlobalTransactionId xid;
    
    tid = text_to_cstring(PG_GETARG_TEXT_P(0));
    result = get2PCInfo(tid);
    if (NULL != result)
    {
		str_xid = strstr(result, GET_XID);
        if (NULL != str_xid)
		{
			str_xid += strlen(GET_XID);
			str_xid = strtok(str_xid, "\n");
			xid = strtoul(str_xid, NULL, 10);
            pfree(result);
			PG_RETURN_UINT32(xid);
		}
    }
    PG_RETURN_NULL();
}

Datum pgxc_remove_2pc_records(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pgxc_remove_2pc_records);
Datum pgxc_remove_2pc_records(PG_FUNCTION_ARGS)
{
    char *tid = text_to_cstring(PG_GETARG_TEXT_P(0));
	remove_2pc_records(tid, true);
    pfree(tid);
    PG_RETURN_BOOL(true);
}

Datum pgxc_clear_2pc_records(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pgxc_clear_2pc_records);
Datum pgxc_clear_2pc_records(PG_FUNCTION_ARGS)
{
	MemoryContext oldcontext;
	MemoryContext mycontext;
	
	int i = 0;
    int count = 0;
    TupleTableSlots *result;
    TupleTableSlots clear_result;
    const char *query = "select pgxc_get_record_list()::text";
    const char *CLEAR_STMT = "select pgxc_remove_2pc_records('%s')::text";
    char clear_query[100];
    char *twopcfiles = NULL;
    char *ptr = NULL;
    bool res = true;
    
	if(!IS_PGXC_COORDINATOR)
	{
		elog(ERROR, "can only called on coordinator");
	}
	
	mycontext = AllocSetContextCreate(CurrentMemoryContext,
											  "clean_check",
											  ALLOCSET_DEFAULT_MINSIZE,
											  ALLOCSET_DEFAULT_INITSIZE,
											  ALLOCSET_DEFAULT_MAXSIZE);
	oldcontext = MemoryContextSwitchTo(mycontext);

    ResetGlobalVariables();
#if 0
	if((dir = opendir(TWOPHASE_RECORD_DIR)))
	{		
		while((ptr = readdir(dir)) != NULL)
	    {
	    	if (count > 999)
				break;
	        if(strcmp(ptr->d_name,".") == 0 || strcmp(ptr->d_name,"..") == 0)
	        {
	            continue;
	        }       
			snprintf(path[count], MAX_GID, "/%s", ptr->d_name);
			//snprintf(path[count], MAX_GID, "/%s", ptr->d_name);
			count++;
		}

		closedir(dir);
	}
#endif

	/*get node list*/
	PgxcNodeGetOids(&cn_node_list, &dn_node_list, 
					&cn_nodes_num, &dn_nodes_num, true);
	pgxc_clean_node_count = cn_nodes_num + dn_nodes_num;
	my_nodeoid = getMyNodeoid();
	cn_health_map = palloc0(cn_nodes_num * sizeof(bool));
	dn_health_map = palloc0(dn_nodes_num * sizeof(bool));
    result = (TupleTableSlots *)palloc0(pgxc_clean_node_count * sizeof(TupleTableSlots));

    /*collect the 2pc file in nodes*/
    for (i = 0; i < cn_nodes_num; i++)
    {
        (void) execute_query_on_single_node(cn_node_list[i], query, 1, result+i);
    }

    for (i = 0; i < dn_nodes_num; i++)
    {
        (void) execute_query_on_single_node(dn_node_list[i], query, 1, result+cn_nodes_num+i);
    }
	/*get all database info*/
	getDatabaseList();
	
	/*get all info of 2PC transactions*/
	getTxnInfoOnNodesAll();
#if 0
	if((dir = opendir(TWOPHASE_RECORD_DIR)))
	{		
		while (i < count)
		{
			if (!find_txn(path[i]))
			{
				unlink(path[i]);
				WriteClean2pcXlogRec(path[i]);
			}
			i++;
		}

		closedir(dir);
	}
#endif
    /*delete all rest 2pc file in each nodes*/
    for (i = 0; i < cn_nodes_num; i++)
    {
        if (0 == result[i].slot_count)
        {
            continue;
        }
        if (!(twopcfiles = TTSgetvalue(result+i, 0, 0)))
            continue;
        ptr = strtok(twopcfiles, ",");
        while(ptr)
        {
            if (count >= MAXIMUM_CLEAR_FILE)
                break;
            if (!find_txn(ptr))
            {
                snprintf(clear_query, 100, CLEAR_STMT, ptr);
                if (execute_query_on_single_node(cn_node_list[i], clear_query, 1, &clear_result) == (Datum)0)
                    res = false;
                DropTupleTableSlots(&clear_result);
                count++;
            }
            ptr = strtok(NULL, ",");
        }
    }

    for (i = 0; i < dn_nodes_num; i++)
    {
        if (0 == result[cn_nodes_num+i].slot_count)
        {
            continue;
        }
        if (!(twopcfiles = TTSgetvalue(result+cn_nodes_num+i, 0, 0)))
            continue;
        ptr = strtok(twopcfiles, ",");
        while(ptr)
        {
            if (count >= MAXIMUM_CLEAR_FILE)
                break;
            if (!find_txn(ptr))
            {
                snprintf(clear_query, 100, CLEAR_STMT, ptr);
                if (execute_query_on_single_node(dn_node_list[i], clear_query, 1, &clear_result) == (Datum)0)
                    res = false;
                DropTupleTableSlots(&clear_result);
                count++;
            }
            ptr = strtok(NULL, ",");
        }
    }

    for (i = 0; i < pgxc_clean_node_count; i++)
        DropTupleTableSlots(result+i);
    
    DestroyTxnHash();
	ResetGlobalVariables();

	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(mycontext);
	
	
    PG_RETURN_BOOL(res);
}

Datum pgxc_get_record_list(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pgxc_get_record_list);
Datum pgxc_get_record_list(PG_FUNCTION_ARGS)
{
    int count = 0;
    DIR *dir = NULL;
    struct dirent *ptr = NULL;
    char *recordList = NULL;
	text *t_recordList = NULL;

    /* get from hash table */
    recordList = get_2pc_list_from_cache(&count);
    if (count >= MAXIMUM_OUTPUT_FILE)
    {
        Assert(NULL != recordList);
        t_recordList = cstring_to_text(recordList);
        return PointerGetDatum(t_recordList);
    }

    /* get from disk */
	if(!(dir = opendir(TWOPHASE_RECORD_DIR)))
	{
        if(NULL == recordList)
        {
		PG_RETURN_NULL();
	}

        t_recordList = cstring_to_text(recordList);
        return PointerGetDatum(t_recordList);
    }

    while((ptr = readdir(dir)) != NULL)
    {
        if(strcmp(ptr->d_name,".") == 0 || strcmp(ptr->d_name,"..") == 0)
        {
            continue;
        }       
        if (count >= MAXIMUM_OUTPUT_FILE)
        {
            break;
        }
        
        if(!recordList)
        {
            recordList = (char *)palloc0(strlen(ptr->d_name) + 1);
            sprintf(recordList, "%s", ptr->d_name);
        }
        else
        {
    		recordList = (char *) repalloc(recordList,
								   strlen(ptr->d_name) + strlen(recordList) + 2);
            sprintf(recordList, "%s,%s", recordList, ptr->d_name);
        }
        count++;
    }

    closedir(dir);
    
    if(!recordList)
    {
        PG_RETURN_NULL();
    }
    else
    {
    	t_recordList = cstring_to_text(recordList);
        return PointerGetDatum(t_recordList);
    }
}

Datum pgxc_commit_on_node(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pgxc_commit_on_node);
Datum pgxc_commit_on_node(PG_FUNCTION_ARGS)
{
    /* nodename, gid */
    char *nodename;
    Oid  nodeoid;
    char *gid;
    txn_info *txn;
	char command[MAX_CMD_LENGTH];
	PGXCNodeHandle **connections = NULL;
	int					conn_count = 0;
	ResponseCombiner	combiner;
	PGXCNodeAllHandles *pgxc_handles = NULL;
    PGXCNodeHandle *conn = NULL;
    
    /*clear Global*/
    ResetGlobalVariables();
    /*get node list*/
    PgxcNodeGetOids(&cn_node_list, &dn_node_list, 
                    &cn_nodes_num, &dn_nodes_num, true);
    if (cn_node_list == NULL || dn_node_list == NULL)
        elog(ERROR, "pg_clean:fail to get cn_node_list and dn_node_list");
    pgxc_clean_node_count = cn_nodes_num + dn_nodes_num;
    my_nodeoid = getMyNodeoid();
    cn_health_map = palloc0(cn_nodes_num * sizeof(bool));
    dn_health_map = palloc0(dn_nodes_num * sizeof(bool));
 
    nodename = text_to_cstring(PG_GETARG_TEXT_P(0));
    gid = text_to_cstring(PG_GETARG_TEXT_P(1));
    nodeoid = get_pgxc_nodeoid(nodename);
    if (InvalidOid == nodeoid)
    {
        elog(ERROR, "Invalid nodename '%s'", nodename);
    }
    
	txn = (txn_info *)palloc0(sizeof(txn_info));
	if (txn == NULL)
	{
		PG_RETURN_BOOL(false);
	}
	txn->txn_stat = (TXN_STATUS *)palloc0(sizeof(TXN_STATUS) * pgxc_clean_node_count);
	txn->xid = (uint32 *)palloc0(sizeof(uint32) * pgxc_clean_node_count);
	txn->prepare_timestamp = (TimestampTz *)palloc0(sizeof(TimestampTz) * pgxc_clean_node_count);
	txn->coordparts = (int *)palloc0(cn_nodes_num * sizeof(int));
	txn->dnparts = (int *)palloc0(dn_nodes_num * sizeof(int));

	strncpy(txn->gid, gid, strlen(gid)+1);
    getTxnInfoOnOtherNodes(txn);
	snprintf(command, MAX_CMD_LENGTH, "commit prepared '%s'", txn->gid);


    if (InvalidGlobalTimestamp == txn->global_commit_timestamp)
    {
        if (!txn->is_readonly)
        {
            elog(ERROR, "in pg_clean, fail to get global_commit_timestamp for transaction '%s' on", gid);
        }
        else
        {
            txn->global_commit_timestamp = GetGlobalTimestampGTM();
        }
    }
    
	connections = (PGXCNodeHandle**)palloc(sizeof(PGXCNodeHandle*));
    get_node_handles(&pgxc_handles, nodeoid);

    conn = (PGXC_NODE_COORDINATOR == get_pgxc_nodetype(nodeoid)) ? 
            pgxc_handles->coord_handles[0] : pgxc_handles->datanode_handles[0];
    if (!send_query_clean_transaction(conn, txn, command))
    {
        elog(ERROR, "pg_clean: send query '%s' from '%s' to '%s' failed ", 
            command, get_pgxc_nodename(my_nodeoid) , nodename);
    }
    else
    {
        connections[conn_count++] = conn;
    }
    /* receive response */
    if (conn_count)
    {
        InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
        if (pgxc_node_receive_responses(conn_count, connections, NULL, &combiner) ||
                !validate_combiner(&combiner))
        {
            if (combiner.errorMessage)
                pgxc_node_report_error(&combiner);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("Failed to FINISH the transaction on one or more nodes")));
        }
        else
            CloseCombiner(&combiner);
    }
    /*clear Global*/
    ResetGlobalVariables();
	clear_handles();
	pfree_pgxc_all_handles(pgxc_handles);
    pgxc_handles = NULL;
    pfree(connections);
    connections = NULL;

    PG_RETURN_BOOL(true);
}

Datum pgxc_abort_on_node(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pgxc_abort_on_node);
Datum pgxc_abort_on_node(PG_FUNCTION_ARGS)
{
    /* nodename, gid */
    char *nodename;
    Oid  nodeoid;
    char *gid;
    txn_info *txn;
	char command[MAX_CMD_LENGTH];
	PGXCNodeHandle **connections = NULL;
	int					conn_count = 0;
	ResponseCombiner	combiner;
	PGXCNodeAllHandles *pgxc_handles = NULL;
    PGXCNodeHandle *conn = NULL;
    
    /*clear Global*/
    ResetGlobalVariables();
    /*get node list*/
    PgxcNodeGetOids(&cn_node_list, &dn_node_list, 
                    &cn_nodes_num, &dn_nodes_num, true);
    if (cn_node_list == NULL || dn_node_list == NULL)
        elog(ERROR, "pg_clean:fail to get cn_node_list and dn_node_list");
    pgxc_clean_node_count = cn_nodes_num + dn_nodes_num;
    my_nodeoid = getMyNodeoid();
    cn_health_map = palloc0(cn_nodes_num * sizeof(bool));
    dn_health_map = palloc0(dn_nodes_num * sizeof(bool));
 
    nodename = text_to_cstring(PG_GETARG_TEXT_P(0));
    gid = text_to_cstring(PG_GETARG_TEXT_P(1));
    nodeoid = get_pgxc_nodeoid(nodename);
    if (InvalidOid == nodeoid)
    {
        elog(ERROR, "Invalid nodename '%s'", nodename);
    }
    
	txn = (txn_info *)palloc0(sizeof(txn_info));
	if (txn == NULL)
	{
		PG_RETURN_BOOL(false);
	}
	txn->txn_stat = (TXN_STATUS *)palloc0(sizeof(TXN_STATUS) * pgxc_clean_node_count);
	txn->xid = (uint32 *)palloc0(sizeof(uint32) * pgxc_clean_node_count);
	txn->prepare_timestamp = (TimestampTz *)palloc0(sizeof(TimestampTz) * pgxc_clean_node_count);
	txn->coordparts = (int *)palloc0(cn_nodes_num * sizeof(int));
	txn->dnparts = (int *)palloc0(dn_nodes_num * sizeof(int));

	strncpy(txn->gid, gid, strlen(gid)+1);
	connections = (PGXCNodeHandle**)palloc(sizeof(PGXCNodeHandle*));
    getTxnInfoOnOtherNodes(txn);
	snprintf(command, MAX_CMD_LENGTH, "rollback prepared '%s'", txn->gid);
#if 0    
	if (!setMaintenanceMode(true))
	{
		elog(ERROR, "Error: fail to set maintenance mode on in pg_clean");
	}
#endif    

    get_node_handles(&pgxc_handles, nodeoid);

    conn = (PGXC_NODE_COORDINATOR == get_pgxc_nodetype(nodeoid)) ? 
            pgxc_handles->coord_handles[0] : pgxc_handles->datanode_handles[0];
    if (!send_query_clean_transaction(conn, txn, command))
    {
        elog(ERROR, "pg_clean: send query '%s' from '%s' to '%s' failed ", 
            command, get_pgxc_nodename(my_nodeoid) , nodename);
    }
    else
    {
        connections[conn_count++] = conn;
    }
    /* receive response */
    if (conn_count)
    {
        InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
        if (pgxc_node_receive_responses(conn_count, connections, NULL, &combiner) ||
                !validate_combiner(&combiner))
        {
            if (combiner.errorMessage)
                pgxc_node_report_error(&combiner);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("Failed to FINISH the transaction on one or more nodes")));
        }
        else
            CloseCombiner(&combiner);
    }
    /*clear Global*/
    ResetGlobalVariables();
	clear_handles();
	pfree_pgxc_all_handles(pgxc_handles);
    pgxc_handles = NULL;
    pfree(connections);
    connections = NULL;

    PG_RETURN_BOOL(true);
}



void recover2PCForDatabaseAll(void)
{
	database_info *cur_db = head_database_info;
	while (cur_db)
	{
		recover2PCForDatabase(cur_db);
		cur_db = cur_db->next;
	}
	//clean_old_2PC_files();
}

void recover2PCForDatabase(database_info * db_info)
{
	txn_info *cur_txn;
	HASH_SEQ_STATUS status;
    HTAB *txn = db_info->all_txn_info;

	hash_seq_init(&status, txn);
	while ((cur_txn = (txn_info *) hash_seq_search(&status)) != NULL)
	{
		recover2PC(cur_txn);
    }
}

bool send_query_clean_transaction(PGXCNodeHandle* conn, txn_info *txn, const char *finish_cmd)
{
#ifdef __TWO_PHASE_TESTS__
    if (PG_CLEAN_SEND_CLEAN <= twophase_exception_case &&
        PG_CLEAN_SEND_QUERY >= twophase_exception_case)
    {
        twophase_in = IN_PG_CLEAN;
    }
#endif
	if (!GlobalTimestampIsValid(txn->global_commit_timestamp) && 
        TXN_STATUS_COMMITTED == txn->global_txn_stat &&
        !txn->is_readonly)
		return false;
	
    if (pgxc_node_send_clean(conn))
    {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("in pg_clean failed to send pg_clean flag for %s PREPARED command",
                        TXN_STATUS_COMMITTED == txn->global_txn_stat ? "COMMIT" : "ROLLBACK")));
        return false;
    }
    if (txn->is_readonly && pgxc_node_send_readonly(conn))
    {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("in pg_clean failed to send readonly flag for %s PREPARED command",
                        TXN_STATUS_COMMITTED == txn->global_txn_stat ? "COMMIT" : "ROLLBACK")));
        return false;
    }

    if (txn->after_first_phase && pgxc_node_send_after_prepare(conn))
    {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("in pg_clean failed to send after prepare flag for %s PREPARED command",
                        TXN_STATUS_COMMITTED == txn->global_txn_stat ? "COMMIT" : "ROLLBACK")));
        return false;
    }
    
    /* 
     * only transaction finished in commit prepared/rollback prepared phase send timestamp 
     * partial prepared transaction has no need to send other information
     */
	if (InvalidGlobalTimestamp != txn->global_commit_timestamp && 
        pgxc_node_send_global_timestamp(conn, txn->global_commit_timestamp))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("in pg_clean failed to send global committs for %s PREPARED command",
						TXN_STATUS_COMMITTED == txn->global_txn_stat ? "COMMIT" : "ROLLBACK")));
	}
    if (!txn->is_readonly)
    {
        if (InvalidOid != txn->origcoord && pgxc_node_send_starter(conn, get_pgxc_nodename(txn->origcoord)))
        {
    		ereport(ERROR,
    				(errcode(ERRCODE_INTERNAL_ERROR),
    				 errmsg("in pg_clean failed to send start node for %s PREPARED command",
    						TXN_STATUS_COMMITTED == txn->global_txn_stat ? "COMMIT" : "ROLLBACK")));
        }

        if (InvalidTransactionId != txn->startxid && pgxc_node_send_startxid(conn, txn->startxid))
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("in pg_clean failed to send start xid for %s PREPARED command",
                            TXN_STATUS_COMMITTED == txn->global_txn_stat ? "COMMIT" : "ROLLBACK")));
        }
        
        if (NULL != txn->participants && pgxc_node_send_partnodes(conn, txn->participants))
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("in pg_clean failed to send participants for %s PREPARED command",
                            TXN_STATUS_COMMITTED == txn->global_txn_stat ? "COMMIT" : "ROLLBACK")));
        }
    }
    
    if (pgxc_node_send_query(conn, finish_cmd))
    {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("in pg_clean failed to send query for %s PREPARED command",
                        TXN_STATUS_COMMITTED == txn->global_txn_stat ? "COMMIT" : "ROLLBACK")));
        return false;
    }
	return true;
}

bool check_2pc_belong_node(txn_info * txn)
{
    int node_index = 0;
    char node_type;
    node_index = find_node_index(abnormal_nodeoid);
    Assert(InvalidOid != abnormal_nodeoid);
    if (abnormal_nodeoid == txn->origcoord)
    {
        txn->belong_abnormal_node = true;
        return true;
    }
    node_type = get_pgxc_nodetype(abnormal_nodeoid);
    if (node_type == 'C' && txn->coordparts[node_index] == 1)
    {
        txn->belong_abnormal_node = true;
        return true;
    }
    if (node_type == 'D' && txn->dnparts[node_index - cn_nodes_num] == 1)
    {
        txn->belong_abnormal_node = true;
        return true;
    }

    if (InvalidOid == txn->origcoord)
    {
        char *startnode = NULL;
        int   node_oid  = InvalidOid;
        char  gid[MAX_GID];

        if (!IsXidImplicit(txn->gid))
        {
            txn->belong_abnormal_node = true;
            return true;
        }

        Assert(IsXidImplicit(txn->gid));

        /* get start node from gid */
        strcpy(gid, txn->gid);
        startnode = strtok(gid, ":");
        if (NULL == startnode)
        {
            elog(WARNING, "get startnode(%s) from gid(%s) failed",
                startnode, gid);
            txn->belong_abnormal_node = false;
            return false;
        }

        startnode = strtok(NULL, ":");
        if (NULL == startnode)
        {
            elog(WARNING, "get startnode(%s) from gid(%s) failed",
                startnode, gid);
            txn->belong_abnormal_node = false;
            return false;
        }

        node_oid = get_pgxc_nodeoid(startnode);
        if (NULL == startnode)
        {
            elog(WARNING, "get invalid oid for startnode(%s) from gid(%s)",
                startnode, gid);
            txn->belong_abnormal_node = false;
            return false;
        }

        elog(DEBUG5, "get oid(%d) for startnode(%s) from gid(%s)",
            node_oid, startnode, gid);

        if (abnormal_nodeoid == node_oid)
        {
            txn->belong_abnormal_node = true;
            return true;
        }
    }

    txn->belong_abnormal_node = false;
    return false;
}

bool check_node_participate(txn_info * txn, int node_idx)
{
    char node_type = get_pgxc_nodetype(abnormal_nodeoid);
    if (PGXC_NODE_COORDINATOR == node_type) 
    {
        return txn->coordparts[node_idx] == 1 ? true : false;
    } else if (PGXC_NODE_DATANODE == node_type)
    {
        return txn->dnparts[node_idx] == 1 ? true : false;
    }
    return false;
}

void recover2PC(txn_info * txn)
{
	int i = 0;
	bool check_ok = false;
	int check_times = CLEAN_CHECK_TIMES_DEFAULT;
	int check_interval = CLEAN_CHECK_INTERVAL_DEFAULT;
	MemoryContext current_context = NULL;
	ErrorData* edata = NULL;
	TXN_STATUS txn_stat;
	txn_stat = check_txn_global_status(txn);
	txn->global_txn_stat = txn_stat;

	if (clear_2pc_belong_node)
	{
		check_times = CLEAN_NODE_CHECK_TIMES;
		check_interval = CLEAN_NODE_CHECK_INTERVAL;
	}

#ifdef DEBUG_EXECABORT
	txn_stat = TXN_STATUS_ABORTED;
#endif

	switch (txn_stat)
	{
		case TXN_STATUS_FAILED:
			elog(LOG, "cannot recover 2PC transaction %s for TXN_STATUS_FAILED", txn->gid);
            txn->op = UNDO;
			txn->op_issuccess = true;
			break;
		
		case TXN_STATUS_UNKNOWN:
			elog(LOG, "cannot recover 2PC transaction %s for TXN_STATUS_UNKNOWN", txn->gid);
            txn->op = UNDO;
			txn->op_issuccess = true;
			break;
		
		case TXN_STATUS_PREPARED:
			elog(DEBUG1, "2PC recovery of transaction %s not needed for TXN_STATUS_PREPARED", txn->gid);
            txn->op = UNDO;
			txn->op_issuccess = true;
			break;
		
		case TXN_STATUS_COMMITTED:
            if (InvalidOid == txn->origcoord || txn->is_readonly)
            {
                txn->op = UNDO;
                txn->op_issuccess = true;
            }
            else
            {
    			txn->op = COMMIT;
				/* check whether all nodes can commit prepared */
				for (i = 0; i < check_times; i++)
				{
					check_ok = true;
					current_context = CurrentMemoryContext;
					PG_TRY();
					{
    			if (!clean_2PC_iscommit(txn, true, true))
    			{
							check_ok = false;
							elog(LOG, "check commit 2PC transaction %s failed",
								txn->gid);
						}
					}
					PG_CATCH();
					{
						(void)MemoryContextSwitchTo(current_context);
						edata = CopyErrorData();
						FlushErrorState();

						check_ok = false;
						elog(WARNING, "check commit 2PC transaction %s error: %s",
							txn->gid, edata->message);
					}
					PG_END_TRY();

					if (!check_ok)
					{
    				txn->op_issuccess = false;
    				return;
    			}

					pg_usleep(check_interval);
				}

    			/* send commit prepared to all nodes */
    			if (!clean_2PC_iscommit(txn, true, false))
    			{
    				txn->op_issuccess = false;
					elog(LOG, "commit 2PC transaction %s failed", txn->gid);
    				return;
    			}
    			txn->op_issuccess = true;
    			clean_2PC_files(txn);
            }
			break;
		
		case TXN_STATUS_ABORTED:
			txn->op = ABORT;
			/* check whether all nodes can rollback prepared */
			for (i = 0; i < check_times; i++)
			{
				check_ok = true;
				current_context = CurrentMemoryContext;
				PG_TRY();
				{
			if (!clean_2PC_iscommit(txn, false, true))
			{
						check_ok = false;
						elog(LOG, "check rollback 2PC transaction %s failed",
							txn->gid);
					}
				}
				PG_CATCH();
				{
					check_ok = false;
					(void)MemoryContextSwitchTo(current_context);
					edata = CopyErrorData();
					FlushErrorState();

					elog(WARNING, "check rollback 2PC transaction %s error: %s",
						txn->gid, edata->message);
				}
				PG_END_TRY();

				if (!check_ok)
				{
				txn->op_issuccess = false;
				return;
			}

				pg_usleep(check_interval);
			}

			/* send rollback prepared to all nodes */
			if (!clean_2PC_iscommit(txn, false, false))
			{
				txn->op_issuccess = false;
				elog(LOG, "rollback 2PC transaction %s failed", txn->gid);
				return;
			}
			txn->op_issuccess = true;
			clean_2PC_files(txn);
			break;
		
		case TXN_STATUS_INPROGRESS:
			elog(DEBUG1, "2PC recovery of transaction %s not needed for TXN_STATUS_INPROGRESS", txn->gid);
            txn->op = UNDO;
			txn->op_issuccess = true;
			break;
		
		default:
			elog(ERROR, "cannot recover 2PC transaction %s for unkown status", txn->gid);
			break;
	}
	return;
}

TXN_STATUS check_txn_global_status(txn_info *txn)
{
#define TXN_PREPARED 	0x0001
#define TXN_COMMITTED 	0x0002
#define TXN_ABORTED		0x0004
#define TXN_UNKNOWN		0x0008
#define TXN_INITIAL		0x0010
#define TXN_INPROGRESS	0X0020
	int ii;
	int check_flag = 0;
    int node_idx = 0;
	TimestampTz prepared_time = 0;
	TimestampTz time_gap = clean_time_interval;

    if (!IsXidImplicit(txn->gid) && txn->is_readonly)
    {
        return TXN_STATUS_COMMITTED;
    }
    if (txn->global_txn_stat == TXN_STATUS_UNKNOWN)
    {
        check_flag |= TXN_UNKNOWN;
    }
    if (txn->global_txn_stat == TXN_STATUS_ABORTED)
    {
        check_flag |= TXN_ABORTED;
    }

	/*check dn participates*/
	for (ii = 0; ii < dn_nodes_num; ii++)
	{
		if (txn->dnparts[ii] == 1)
		{
			if (txn->txn_stat[ii + cn_nodes_num] == TXN_STATUS_INITIAL)
				check_flag |= TXN_INITIAL;
			else if (txn->txn_stat[ii + cn_nodes_num] == TXN_STATUS_UNKNOWN)
				check_flag |= TXN_UNKNOWN;
			else if (txn->txn_stat[ii + cn_nodes_num] == TXN_STATUS_PREPARED)
			{
				check_flag |= TXN_PREPARED;
				prepared_time = txn->prepare_timestamp[ii + cn_nodes_num] > prepared_time ? 
								txn->prepare_timestamp[ii + cn_nodes_num] : prepared_time;
			}
			else if (txn->txn_stat[ii + cn_nodes_num] == TXN_STATUS_INPROGRESS)
				check_flag |= TXN_INPROGRESS;
			else if (txn->txn_stat[ii + cn_nodes_num] == TXN_STATUS_COMMITTED)
				check_flag |= TXN_COMMITTED;
			else if (txn->txn_stat[ii + cn_nodes_num] == TXN_STATUS_ABORTED)
				check_flag |= TXN_ABORTED;
			else
				return TXN_STATUS_FAILED;
		}
	}
	/*check cn participates*/
	for (ii = 0; ii < cn_nodes_num; ii++)
	{
		if (txn->coordparts[ii] == 1)
		{
			if (txn->txn_stat[ii] == TXN_STATUS_INITIAL)
				check_flag |= TXN_ABORTED;
			else if (txn->txn_stat[ii] == TXN_STATUS_UNKNOWN)
				check_flag |= TXN_UNKNOWN;
			else if (txn->txn_stat[ii] == TXN_STATUS_PREPARED)
			{
				check_flag |= TXN_PREPARED;
				prepared_time = txn->prepare_timestamp[ii] > prepared_time ? 
								txn->prepare_timestamp[ii] : prepared_time;
			}
			else if (txn->txn_stat[ii] == TXN_STATUS_INPROGRESS)
				check_flag |= TXN_INPROGRESS;
			else if (txn->txn_stat[ii] == TXN_STATUS_COMMITTED)
				check_flag |= TXN_COMMITTED;
			else if (txn->txn_stat[ii] == TXN_STATUS_ABORTED)
				check_flag |= TXN_ABORTED;
			else
				return TXN_STATUS_FAILED;
		}
	}

    /*
     * first check the prepare timestamp of both implicit and explicit trans within the time_gap or not
     * if not, check the commit timestamp explicit trans within the time_gap or not 
     */
#if 0     
    if ((check_flag & TXN_INPROGRESS) ||
        (IsXidImplicit(txn->gid) && current_time - prepared_time <= time_gap) ||
        (!IsXidImplicit(txn->gid) && 
            ((!txn->after_first_phase && current_time - prepared_time <= time_gap) ||
            (txn->after_first_phase && 
                (InvalidGlobalTimestamp != commit_time && 
                current_time - commit_time <= time_gap)))))
    {
		/* transaction inprogress */
        return TXN_STATUS_INPROGRESS;
    }
#endif                
    if (clear_2pc_belong_node)
        {
        if (!check_2pc_belong_node(txn))
        {
            return TXN_STATUS_INPROGRESS;
        }

        if (!check_2pc_start_from_node(txn))
        {
            return TXN_STATUS_INPROGRESS;
        }

        node_idx = find_node_index(abnormal_nodeoid);
        if (node_idx >= 0)
        {
            if (abnormal_time < txn->prepare_timestamp[node_idx])
        {
                elog(WARNING, "gid: %s, abnormal time: " INT64_FORMAT
                    ", prepare timestamp[%d]: " INT64_FORMAT, txn->gid,
                    abnormal_time, node_idx, txn->prepare_timestamp[node_idx]);

            return TXN_STATUS_INPROGRESS;
        }
        }
        else
        {
            elog(WARNING, "gid: %s, node_idx: %d", txn->gid, node_idx);
        }

        if (abnormal_time < prepared_time)
            {
            elog(WARNING, "gid: %s, abnormal time: " INT64_FORMAT
                ", prepared time: " INT64_FORMAT, txn->gid,
                abnormal_time, prepared_time);

                return TXN_STATUS_INPROGRESS;
            }
        }
    else
    {
        if (check_flag & TXN_INPROGRESS ||current_time - prepared_time <= time_gap)
        {
            /* transaction inprogress */
            return TXN_STATUS_INPROGRESS;
        }
    }


    if (!IsXidImplicit(txn->gid) && txn->after_first_phase && (TXN_PREPARED == check_flag))
    {
        return TXN_STATUS_PREPARED;
    }

	if (check_flag & TXN_UNKNOWN)
		return TXN_STATUS_UNKNOWN;
    
	if ((check_flag & TXN_COMMITTED) && (check_flag & TXN_ABORTED))
		/* Mix of committed and aborted. This should not happen. */
		return TXN_STATUS_UNKNOWN;
    
	if ((check_flag & TXN_PREPARED) == 0)
		/* Should be at least one "prepared statement" in nodes */
		return TXN_STATUS_FAILED;
		
	if (check_flag & TXN_COMMITTED)
		/* Some 2PC transactions are committed.  Need to commit others. */
		return TXN_STATUS_COMMITTED;
	/* All the transactions remain prepared.   No need to recover. */
	return TXN_STATUS_ABORTED;
}

bool clean_2PC_iscommit(txn_info *txn, bool is_commit, bool is_check)
{
	int ii;
	static const char *STMT_FORM = "%s prepared '%s';";
	static const char *STMT_FORM_CHECK = "%s prepared '%s' for check only;";
	char command[MAX_CMD_LENGTH];
	int node_idx;
    Oid node_oid;
	PGXCNodeHandle **connections = NULL;
	int					conn_count = 0;
	ResponseCombiner	combiner;
	PGXCNodeAllHandles *pgxc_handles = NULL;

	if (is_commit)
	{
		if (is_check)
		{
			snprintf(command, MAX_CMD_LENGTH, STMT_FORM_CHECK, "commit", txn->gid);
		}
	else
		{
			snprintf(command, MAX_CMD_LENGTH, STMT_FORM, "commit", txn->gid);
		}
	}
	else
	{
		if (is_check)
		{
			snprintf(command, MAX_CMD_LENGTH, STMT_FORM_CHECK, "rollback", txn->gid);
		}
		else
		{
			snprintf(command, MAX_CMD_LENGTH, STMT_FORM, "rollback", txn->gid);
		}
	}
	if (is_commit && InvalidGlobalTimestamp == txn->global_commit_timestamp)	
	{
		elog(ERROR, "twophase transaction '%s' has InvalidGlobalCommitTimestamp", txn->gid);
	}

	connections = (PGXCNodeHandle**)palloc(sizeof(PGXCNodeHandle*) * (txn->num_dnparts + txn->num_coordparts));
	if (connections == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory for connections")));
    }
    get_transaction_handles(&pgxc_handles, txn);

#ifdef __TWO_PHASE_TESTS__
    if (PG_CLEAN_SEND_CLEAN <= twophase_exception_case && 
        PG_CLEAN_ELOG_ERROR >= twophase_exception_case)
    {
        exception_count = 0;
    }
#endif
	for (ii = 0; ii < pgxc_handles->dn_conn_count; ii++)
	{
        node_oid = pgxc_handles->datanode_handles[ii]->nodeoid;
    	node_idx = find_node_index(node_oid);
        if (TXN_STATUS_PREPARED != txn->txn_stat[ node_idx])
        {
            continue;
        }
		/*send global timestamp to dn_node_list[ii]*/
		if (!send_query_clean_transaction(pgxc_handles->datanode_handles[ii], txn, command))
		{
			elog(LOG, "pg_clean: send query '%s' from '%s' to '%s' failed ", 
				command, get_pgxc_nodename(my_nodeoid) , pgxc_handles->datanode_handles[ii]->nodename);
			return false;
		}
        else
        {
            connections[conn_count++] = pgxc_handles->datanode_handles[ii];
#ifdef __TWO_PHASE_TESTS__
            if (PG_CLEAN_SEND_CLEAN <= twophase_exception_case && 
                PG_CLEAN_ELOG_ERROR >= twophase_exception_case)
            {
                exception_count++;
                if (1 == exception_count && 
                    PG_CLEAN_ELOG_ERROR == twophase_exception_case)
                {
                    elog(ERROR, "PG_CLEAN_ELOG_ERROR complish");
                }
            }
#endif
        }
	}

	for (ii = 0; ii < pgxc_handles->co_conn_count; ii++)
	{
        node_oid = pgxc_handles->coord_handles[ii]->nodeoid;
    	node_idx = find_node_index(node_oid);
        if (TXN_STATUS_PREPARED != txn->txn_stat[ node_idx])
        {
            continue;
        }
		/*send global timestamp to dn_node_list[ii]*/
		if (!send_query_clean_transaction(pgxc_handles->coord_handles[ii], txn, command))
		{
			elog(LOG, "pg_clean: send query '%s' from '%s' to '%s' failed ", 
				command, get_pgxc_nodename(my_nodeoid) , pgxc_handles->coord_handles[ii]->nodename);
			return false;
		}
        else
        {
            connections[conn_count++] = pgxc_handles->coord_handles[ii];
#ifdef __TWO_PHASE_TESTS__
            if (PG_CLEAN_SEND_CLEAN <= twophase_exception_case && 
                PG_CLEAN_ELOG_ERROR >= twophase_exception_case)
            {
                exception_count++;
                if (1 == exception_count && 
                    PG_CLEAN_ELOG_ERROR == twophase_exception_case)
                {
                    elog(ERROR, "PG_CLEAN_ELOG_ERROR complish");
                }
            }
#endif
        }

	}

    /* receive response */
    if (conn_count)
    {
        InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
        if (pgxc_node_receive_responses(conn_count, connections, NULL, &combiner) ||
                !validate_combiner(&combiner))
        {
            if (combiner.errorMessage)
                pgxc_node_report_error(&combiner);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("Failed to FINISH the transaction on one or more nodes")));
        }
        else
            CloseCombiner(&combiner);
    }
    if (enable_distri_print)
    {
        for (ii = 0; ii < conn_count; ii++)
        {
            if (DN_CONNECTION_STATE_IDLE != connections[ii]->state)
            {
                elog(WARNING, "IN pg_clean node:%s invalid stauts:%d", connections[ii]->nodename, connections[ii]->state);
            }
        }
    }
    conn_count = 0;
	clear_handles();
	pfree_pgxc_all_handles(pgxc_handles);
    pgxc_handles = NULL;

	/*last commit or rollback on origcoord if it participate this txn, since after commit the 2pc file is deleted on origcoord*/
    if (txn->origcoord != InvalidOid)
    {
    	node_idx = find_node_index(txn->origcoord);
    	if (txn->coordparts[node_idx] == 1)
    	{
			/*send global timestamp to dn_node_list[ii]*/
            
			if (txn->txn_stat[node_idx] == TXN_STATUS_PREPARED)
			{
                get_node_handles(&pgxc_handles, txn->origcoord);
                if (!send_query_clean_transaction(pgxc_handles->coord_handles[0], txn, command))
                {
                    elog(LOG, "pg_clean: send query '%s' from %s to %s failed ", 
                        command, get_pgxc_nodename(my_nodeoid) , pgxc_handles->coord_handles[0]->nodename);
                    return false;
                }
                else
                {
                    connections[conn_count++] = pgxc_handles->coord_handles[0];
                }
            }
    	}
    }
	
    /* receive response */
    if (conn_count)
    {
        InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
        if (pgxc_node_receive_responses(conn_count, connections, NULL, &combiner) ||
                !validate_combiner(&combiner))
        {
            if (combiner.errorMessage)
                pgxc_node_report_error(&combiner);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("Failed to FINISH the transaction on one or more nodes")));
        }
        else
            CloseCombiner(&combiner);
    }
	/*free hash record from gtm*/
	FinishGIDGTM(txn->gid);
    
	clear_handles();
	pfree_pgxc_all_handles(pgxc_handles);
    pgxc_handles = NULL;
    pfree(connections);
    connections = NULL;
	return true;
}

bool clean_2PC_files(txn_info * txn)
{
	int ii;
	TupleTableSlots result;
	bool issuccess = true;
	static const char *STMT_FORM = "select pgxc_remove_2pc_records('%s')::text";
	char query[MAX_CMD_LENGTH];
	
	snprintf(query, MAX_CMD_LENGTH, STMT_FORM, txn->gid);

	for (ii = 0; ii < dn_nodes_num; ii++)
	{
		if (execute_query_on_single_node(dn_node_list[ii], query, 1, &result) == (Datum) 1)
		{
			if (TTSgetvalue(&result, 0, 0) == false)
			{
				elog(LOG, "pg_clean: delete 2PC file failed of transaction %s on node %s",
						  txn->gid, get_pgxc_nodename(txn->dnparts[ii]));
				issuccess = false;
			}
		}
		else
		{
			elog(LOG, "pg_clean: failed clean 2pc file of transaction %s on node %s", txn->gid, get_pgxc_nodename(dn_node_list[ii]));
			issuccess = false;
		}
		DropTupleTableSlots(&result);
		if (!issuccess)
			return false;
	}

	for (ii = 0; ii < cn_nodes_num; ii++)
	{
		if (execute_query_on_single_node(cn_node_list[ii], query, 1, &result) == (Datum) 1)
		{
			if (TTSgetvalue(&result, 0, 0) == false)
			{
				elog(LOG, "Error:delete 2PC file failed of transaction %s on node %s",
						  txn->gid, get_pgxc_nodename(txn->coordparts[ii]));
				issuccess = false;
			}
		}
		else
		{
			elog(LOG, "pg_clean: failed clean 2pc file of transaction %s on node %s", txn->gid, get_pgxc_nodename(cn_node_list[ii]));
			issuccess = false;
		}
		DropTupleTableSlots(&result);
		if (!issuccess)
			return false;
	}
	return true;
}

void Init_print_txn_info(print_txn_info * print_txn)
{
	database_info *cur_database = head_database_info;
	txn_info *cur_txn;
	HASH_SEQ_STATUS status;
    HTAB *txn;

	print_txn->index = 0;
	INIT(print_txn->txn);

	for (; cur_database; cur_database = cur_database->next)
	{
        txn = cur_database->all_txn_info;
        hash_seq_init(&status, txn);
        while ((cur_txn = (txn_info *) hash_seq_search(&status)) != NULL)
        {
            if (clear_2pc_belong_node && !cur_txn->belong_abnormal_node)
            {
                continue;
            }
			if (cur_txn->global_txn_stat != TXN_STATUS_INPROGRESS)
				PALLOC(print_txn->txn, cur_txn);
        }
        
#if 0
		cur_txn = cur_database->head_txn_info;
		for (; cur_txn; cur_txn = cur_txn->next)
		{
			if (cur_txn->global_txn_stat != TXN_STATUS_INPROGRESS)
				PALLOC(print_txn->txn, cur_txn);
		}
#endif
	}
}

void Init_print_stats_all(print_status *pstatus)
{
	database_info *cur_database;
	txn_info *cur_txn;
	HASH_SEQ_STATUS status;
    HTAB *txn;

	pstatus->index = 0;
	pstatus->count = 0;
	INIT(pstatus->gid);
	INIT(pstatus->global_status);
	INIT(pstatus->status);
	INIT(pstatus->database);

	for (cur_database = head_database_info; cur_database; cur_database = cur_database->next)
	{
        txn = cur_database->all_txn_info;
        hash_seq_init(&status, txn);
        while ((cur_txn = (txn_info *) hash_seq_search(&status)) != NULL)
        {
			cur_txn->global_txn_stat = check_txn_global_status(cur_txn);
			if (cur_txn->global_txn_stat != TXN_STATUS_INPROGRESS)
				Init_print_stats(cur_txn, cur_database->database_name, pstatus);
        }
#if 0
		for (cur_txn = cur_database->head_txn_info; cur_txn; cur_txn = cur_txn->next)
		{
			cur_txn->global_txn_stat = check_txn_global_status(cur_txn);
			if (cur_txn->global_txn_stat != TXN_STATUS_INPROGRESS)
				Init_print_stats(cur_txn, cur_database->database_name, pstatus);
		}
#endif
	}
}

void Init_print_stats(txn_info *txn, char *database, print_status * pstatus)
{
	int ii;
	StringInfoData	query;	
	initStringInfo(&query);

	RPALLOC(pstatus->gid);
	RPALLOC(pstatus->global_status);
	RPALLOC(pstatus->status);
	RPALLOC(pstatus->database);

	pstatus->gid[pstatus->count] = (char *)palloc0(100 * sizeof(char));
	pstatus->database[pstatus->count] = (char *)palloc0(100 * sizeof(char));
	pstatus->global_status[pstatus->count] = (char *)palloc0(100 * sizeof(char));

	strncpy(pstatus->gid[pstatus->count], txn->gid, 100);
	strncpy(pstatus->database[pstatus->count], database, 100);
	strncpy(pstatus->global_status[pstatus->count], txn_status_to_string(check_txn_global_status(txn)), 100);

	for (ii = 0; ii < pgxc_clean_node_count; ii++)
	{
		appendStringInfo(&query, "%-12s:%-15s", get_pgxc_nodename(find_node_oid(ii)), 
						txn_status_to_string(txn->txn_stat[ii]));
		if (ii < pgxc_clean_node_count - 1)
		{
			appendStringInfoChar(&query, '\n');
		}
	}

	pstatus->status[pstatus->count] = (char *)palloc0((strlen(query.data)+1) * sizeof(char));
	strncpy(pstatus->status[pstatus->count], query.data, strlen(query.data)+1);
	pstatus->gid_count++;
	pstatus->database_count++;
	pstatus->global_status_count++;
	pstatus->status_count++;
	pstatus->count++;
}

static const char *txn_status_to_string(TXN_STATUS status)
{
	switch (status)
	{
		ENUM_TOCHAR_CASE(TXN_STATUS_INITIAL)
	    ENUM_TOCHAR_CASE(TXN_STATUS_UNKNOWN)
	    ENUM_TOCHAR_CASE(TXN_STATUS_PREPARED)
	    ENUM_TOCHAR_CASE(TXN_STATUS_COMMITTED)       
	    ENUM_TOCHAR_CASE(TXN_STATUS_ABORTED)
	    ENUM_TOCHAR_CASE(TXN_STATUS_INPROGRESS)
	    ENUM_TOCHAR_CASE(TXN_STATUS_FAILED)
	}
	return NULL;
}

static const char *txn_op_to_string(OPERATION op)
{
	switch (op)
	{
		ENUM_TOCHAR_CASE(UNDO)
	    ENUM_TOCHAR_CASE(ABORT)
	    ENUM_TOCHAR_CASE(COMMIT)
	}
	return NULL;
}


static void 
CheckFirstPhase(txn_info *txn)
{
//    int ret;
    Oid orignode = txn->origcoord;
    uint32 startxid = txn->startxid;
//    uint32 transactionid;
    int nodeidx;

    /*
     * if the twophase trans does not success in prepare phase, the orignode == InvalidOid.
     */
    if (InvalidOid == orignode)
    {
        return;
    }
    nodeidx = find_node_index(orignode);
    if (0 == txn->xid[nodeidx])
    {
        txn->xid[nodeidx] = startxid;
    }
    /* start node participate */
    if (txn->isorigcoord_part)
    {
        if (0 == txn->coordparts[nodeidx])
        {
            txn->coordparts[nodeidx] = 1;
            txn->num_coordparts++;
        }
        if (txn->txn_stat[nodeidx] == TXN_STATUS_INITIAL)
        {
            /*select * from pgxc_is_committed...*/
            getTxnStatus(txn, nodeidx);
        }
        if (txn->txn_stat[nodeidx] == TXN_STATUS_PREPARED && txn->global_commit_timestamp != InvalidGlobalTimestamp)
        {
            txn->after_first_phase = true;
        }
    }
    /* start node node participate */
    else
    {
#if 0        
        ret = Get2PCFile(orignode, txn->gid, &transactionid);
        if (ret == FILENOTFOUND)
            txn->after_first_phase = false;
        else if (ret == FILEUNKOWN)
            txn->global_txn_stat = TXN_STATUS_UNKNOWN;
        else if (ret == FILEFOUND && txn->global_commit_timestamp != InvalidGlobalTimestamp)
            txn->after_first_phase = true;
#endif
        if (txn->global_commit_timestamp != InvalidGlobalTimestamp)
        {
            txn->after_first_phase = true;
        } else {
            txn->after_first_phase = false;
        }
    }
}

void get_transaction_handles(PGXCNodeAllHandles **pgxc_handles, txn_info *txn)
{
    int dn_index = 0;
    int cn_index = 0;
    int  nodeIndex;
    char nodetype;
	List *coordlist = NIL;
	List *nodelist = NIL;
    
    while (dn_index < dn_nodes_num)
    {

        /* Get node type and index */
        nodetype = PGXC_NODE_NONE;
        if (TXN_STATUS_PREPARED != txn->txn_stat[dn_index + cn_nodes_num])
        {
            dn_index++;
            continue;
        }
        nodeIndex = PGXCNodeGetNodeIdFromName(get_pgxc_nodename(dn_node_list[dn_index]), &nodetype);
        if (nodetype == PGXC_NODE_NONE)
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                     errmsg("PGXC Node %s: object not defined",
                            get_pgxc_nodename(dn_node_list[dn_index]))));

        /* Check if node is requested is the self-node or not */
        if (nodetype == PGXC_NODE_DATANODE)
        {
            nodelist = lappend_int(nodelist, nodeIndex);
        }
        dn_index++;

    }

    while (cn_index < cn_nodes_num)
    {
        /* Get node type and index */
        nodetype = PGXC_NODE_NONE;
        if (TXN_STATUS_PREPARED != txn->txn_stat[cn_index] || cn_node_list[cn_index] == txn->origcoord)
        {
            cn_index++;
            continue;
        }
        nodeIndex = PGXCNodeGetNodeIdFromName(get_pgxc_nodename(cn_node_list[cn_index]), &nodetype);
        if (nodetype == PGXC_NODE_NONE)
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                     errmsg("PGXC Node %s: object not defined",
                            get_pgxc_nodename(cn_node_list[cn_index]))));

        /* Check if node is requested is the self-node or not */
        if (nodetype == PGXC_NODE_COORDINATOR)
        {
            coordlist = lappend_int(coordlist, nodeIndex);
        }
        cn_index++;
    }
    *pgxc_handles = get_handles(nodelist, coordlist, false, true, true);
}

void get_node_handles(PGXCNodeAllHandles **pgxc_handles, Oid nodeoid)
{
    char nodetype = PGXC_NODE_NONE;
	int nodeIndex;
	List *coordlist = NIL;
	List *nodelist = NIL;

	nodeIndex = PGXCNodeGetNodeIdFromName(get_pgxc_nodename(nodeoid), &nodetype);
	if (nodetype == PGXC_NODE_COORDINATOR)
	{
		coordlist = lappend_int(coordlist, nodeIndex);
	}
    else
    {
        nodelist = lappend_int(nodelist, nodeIndex);
    }
	*pgxc_handles = get_handles(nodelist, coordlist, false, true, true);
}


bool check_2pc_start_from_node(txn_info *txn)
{
	char node_type;

	Assert(InvalidOid != abnormal_nodeoid);

	if (abnormal_nodeoid == txn->origcoord)
	{
		return true;
	}

	node_type = get_pgxc_nodetype(abnormal_nodeoid);
	if (node_type == 'D')
	{
		return false;
	}

	if (InvalidOid == txn->origcoord)
	{
		char *startnode = NULL;
		int   node_oid  = InvalidOid;
		char  gid[MAX_GID];

		if (!IsXidImplicit(txn->gid))
		{
			return true;
		}

		Assert(IsXidImplicit(txn->gid));

		/* get start node from gid */
		strcpy(gid, txn->gid);
		startnode = strtok(gid, ":");
		if (NULL == startnode)
		{
			elog(WARNING, "get startnode(%s) from gid(%s) failed",
				startnode, gid);
			return false;
		}

		startnode = strtok(NULL, ":");
		if (NULL == startnode)
{
			elog(WARNING, "get startnode(%s) from gid(%s) failed",
				startnode, gid);
			return false;
		}

		node_oid = get_pgxc_nodeoid(startnode);
		if (NULL == startnode)
			{
			elog(WARNING, "get invalid oid for startnode(%s) from gid(%s)",
				startnode, gid);
			return false;
	}

		elog(DEBUG1, "get oid(%d) for startnode(%s) from gid(%s)",
			node_oid, startnode, gid);

		if (abnormal_nodeoid == node_oid)
	{
		return true;
	}
	}

	return false;
}
