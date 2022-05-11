/*-------------------------------------------------------------------------
 *
 * squeue.c
 *
 *      Shared queue is for data exchange in shared memory between sessions,
 * one of which is a producer, providing data rows. Others are consumer agents -
 * sessions initiated from other datanodes, the main purpose of them is to read
 * rows from the shared queue and send then to the parent data node.
 *    The producer is usually a consumer at the same time, it sends back tuples
 * to the parent node without putting it to the queue.
 *
 * Copyright (c) 2012-2014, TransLattice, Inc.
 *
 * IDENTIFICATION
 *      $$
 *
 *
 *-------------------------------------------------------------------------
 */

#include <sys/time.h>

#include "postgres.h"

#include "miscadmin.h"
#include "access/gtm.h"
#include "catalog/pgxc_node.h"
#include "commands/prepare.h"
#include "executor/executor.h"
#include "nodes/pg_list.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/squeue.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"
#include "utils/resowner.h"
#include "pgstat.h"
#ifdef __TBASE__
#include <pthread.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include "storage/spin.h"
#include "storage/s_lock.h"
#include "miscadmin.h"
#include "libpq/libpq-be.h"
#include "utils/lsyscache.h"
#include "storage/fd.h"
#include "storage/shm_toc.h"
#include "access/parallel.h"
#include "postmaster/postmaster.h"
#include "access/printtup.h"
#include "catalog/pg_type.h"
#include "utils/typcache.h"
#include "access/htup_details.h"
#include "executor/execParallel.h"
#include "utils/memutils.h"
#include "utils/elog.h"
#include "commands/vacuum.h"
#endif
int   NSQueues = 64;
int   SQueueSize = 64;

#ifdef __TBASE__
extern ProtocolVersion FrontendProtocol;

bool  g_UseDataPump         = true;/* Use data pumb, true default. */
bool  g_DataPumpDebug       = false;/* enable debug info */
int32 g_SndThreadNum        = 8;    /* Two sender threads default.  */
int32 g_SndThreadBufferSize = 16;   /* in Kilo bytes. */
int32 g_SndBatchSize        = 8;    /* in Kilo bytes. */
int   consumer_connect_timeout = 128; /* in seconds */
int   g_DisConsumer_timeout = 60; /* in minutes */

#define MAX_CURSOR_LEN      64 
#define DATA_PUMP_SOCKET_DIR  "pg_datapump"   /* socket dir for data pump */

#define PARALLEL_SEND_SHARE_DATA       UINT64CONST(0xFFFFFFFFFFFFFF01)
#define PARALLEL_SEND_CONS_MAP         UINT64CONST(0xFFFFFFFFFFFFFF02)
#define PARALLEL_SEND_SENDER_SEM       UINT64CONST(0xFFFFFFFFFFFFFF03)
#define PARALLEL_SEND_DATA_BUFFER      UINT64CONST(0xFFFFFFFFFFFFFF04)
#define PARALLEL_SEND_WORKER_STATUS    UINT64CONST(0xFFFFFFFFFFFFFF05)
#define PARALLEL_SEND_NODE_NO_SOCKET   UINT64CONST(0xFFFFFFFFFFFFFF06)
#define PARALLEL_SEND_NODE_SEND_DONE   UINT64CONST(0xFFFFFFFFFFFFFF07)
#define PARALLEL_SEND_SENDER_ERROR     UINT64CONST(0xFFFFFFFFFFFFFF08)



static dsm_handle parallel_send_seg_handle = DSM_HANDLE_INVALID;
static dsm_segment *dsm_seg                = NULL;

static SqueueRole role = Squeue_None;

static SharedQueue share_sq = NULL;

static SharedQueue share_sq_bak = NULL;

bool in_data_pump = false;

static bool execute_error = false;

volatile sig_atomic_t end_query_requested = false;


#define PARALLEL_MAGIC                        0x50477c7c

#endif

#define LONG_TUPLE -42

typedef struct ConsumerSync
{
    LWLock       *cs_lwlock;         /* Synchronize access to the consumer queue */
    Latch         cs_latch;     /* The latch consumer is waiting on */
} ConsumerSync;


/*
 * Shared memory structure to store synchronization info to access shared queues
 */
typedef struct SQueueSync
{
    void        *queue;             /* NULL if not assigned to any queue */
    LWLock       *sqs_producer_lwlock; /* Synchronize access to the queue */
    Latch         sqs_producer_latch; /* the latch producer is waiting on */
    ConsumerSync sqs_consumer_sync[0]; /* actual length is TBASE_MAX_DATANODE_NUMBER-1 is
                                        * not known on compile time */
} SQueueSync;

/* Both producer and consumer are working */
#define CONSUMER_ACTIVE 0
/* Producer have finished work successfully and waits for consumer */
#define CONSUMER_EOF 1
/* Producer encountered error and waits for consumer to disconnect */
#define CONSUMER_ERROR 2
/* Consumer is finished with the query, OK to unbind */
#define CONSUMER_DONE 3


/* State of a single consumer */
typedef struct
{
    int            cs_pid;            /* Process id of the consumer session */
    int            cs_node;        /* Node id of the consumer parent */
    /*
     * Queue state. The queue is a cyclic queue where stored tuples in the
     * DataRow format, first goes the lengths of the tuple in host format,
     * because it never sent over network followed by tuple bytes.
     */
    int            cs_ntuples;     /* Number of tuples in the queue */
    int            cs_status;         /* See CONSUMER_* defines above */
    char       *cs_qstart;        /* Where consumer queue begins */
    int            cs_qlength;        /* The size of the consumer queue */
    int            cs_qreadpos;    /* The read position in the consumer queue */
    int            cs_qwritepos;    /* The write position in the consumer queue */
#ifdef __TBASE__
    bool        send_fd;        /* true if send fd to producer */
    bool        cs_done;
#endif
#ifdef SQUEUE_STAT
    long         stat_writes;
    long        stat_reads;
    long         stat_buff_writes;
    long        stat_buff_reads;
    long        stat_buff_returns;
#endif
} ConsState;

/* Shared queue header */
typedef struct SQueueHeader
{
    char        sq_key[SQUEUE_KEYSIZE]; /* Hash entry key should be at the
                                 * beginning of the hash entry */
    int            sq_pid;         /* Process id of the producer session */
    int            sq_nodeid;        /* Node id of the producer parent */
    SQueueSync *sq_sync;        /* Associated sinchronization objects */
    int            sq_refcnt;        /* Reference count to this entry */
#ifdef SQUEUE_STAT
    bool        stat_finish;
    long        stat_paused;
#endif
#ifdef __TBASE__
     DataPumpSender sender; /* used for locally data transfering */
    bool        with_params;
    bool        sender_destroy;
    bool        parallelWorkerSendTuple;
    int         numParallelWorkers;
    ParallelSender parallelSendControl;
    int16       nodeMap[MAX_NODES_NUMBER];
    bool        sq_error;
    char        err_msg[ERR_MSGSIZE];
    bool        has_err_msg;
    bool        producer_done;
    int         nConsumer_done;
    slock_t        lock;
#endif
    int            sq_nconsumers;    /* Number of consumers */
    ConsState     sq_consumers[0];/* variable length array */
} SQueueHeader;


/*
 * Hash table where all shared queues are stored. Key is the queue name, value
 * is SharedQueue
 */
static HTAB *SharedQueues = NULL;
static LWLockPadded *SQueueLocks = NULL;

/*
 * Pool of synchronization items
 */
static void *SQueueSyncs;

#define SQUEUE_SYNC_SIZE \
    (sizeof(SQueueSync) + (TBASE_MAX_DATANODE_NUMBER-1) * sizeof(ConsumerSync))

#define GET_SQUEUE_SYNC(idx) \
    ((SQueueSync *) (((char *) SQueueSyncs) + (idx) * SQUEUE_SYNC_SIZE))

#define SQUEUE_HDR_SIZE(nconsumers) \
    (sizeof(SQueueHeader) + (nconsumers) * sizeof(ConsState))

#define QUEUE_FREE_SPACE(cstate) \
    ((cstate)->cs_ntuples > 0 ? \
        ((cstate)->cs_qreadpos >= (cstate)->cs_qwritepos ? \
            (cstate)->cs_qreadpos - (cstate)->cs_qwritepos : \
            (cstate)->cs_qlength + (cstate)->cs_qreadpos \
                                 - (cstate)->cs_qwritepos) \
        : (cstate)->cs_qlength)

#define QUEUE_WRITE(cstate, len, buf) \
    do \
    { \
        if ((cstate)->cs_qwritepos + (len) <= (cstate)->cs_qlength) \
        { \
            memcpy((cstate)->cs_qstart + (cstate)->cs_qwritepos, buf, len); \
            (cstate)->cs_qwritepos += (len); \
            if ((cstate)->cs_qwritepos == (cstate)->cs_qlength) \
                (cstate)->cs_qwritepos = 0; \
        } \
        else \
        { \
            int part = (cstate)->cs_qlength - (cstate)->cs_qwritepos; \
            memcpy((cstate)->cs_qstart + (cstate)->cs_qwritepos, buf, part); \
            (cstate)->cs_qwritepos = (len) - part; \
            memcpy((cstate)->cs_qstart, (buf) + part, (cstate)->cs_qwritepos); \
        } \
    } while(0)


#define QUEUE_READ(cstate, len, buf) \
    do \
    { \
        if ((cstate)->cs_qreadpos + (len) <= (cstate)->cs_qlength) \
        { \
            memcpy(buf, (cstate)->cs_qstart + (cstate)->cs_qreadpos, len); \
            (cstate)->cs_qreadpos += (len); \
            if ((cstate)->cs_qreadpos == (cstate)->cs_qlength) \
                (cstate)->cs_qreadpos = 0; \
        } \
        else \
        { \
            int part = (cstate)->cs_qlength - (cstate)->cs_qreadpos; \
            memcpy(buf, (cstate)->cs_qstart + (cstate)->cs_qreadpos, part); \
            (cstate)->cs_qreadpos = (len) - part; \
            memcpy((buf) + part, (cstate)->cs_qstart, (cstate)->cs_qreadpos); \
        } \
    } while(0)


static bool sq_push_long_tuple(ConsState *cstate, RemoteDataRow datarow);
static void sq_pull_long_tuple(ConsState *cstate, RemoteDataRow datarow,
                                int consumerIdx, SQueueSync *sqsync);

#ifdef __TBASE__
typedef struct DisConsumer
{
    char sq_key[SQUEUE_KEYSIZE];
    int  nConsumer;
    TimestampTz time;
    bool disconnect[TBASE_MAX_DATANODE_NUMBER];
} DisConsumer;

static HTAB *DisConsumerHash = NULL;
#endif

#ifdef __TBASE__
/*
typedef struct
{
    int             m_cnt;
    pthread_mutex_t m_mutex;
    pthread_cond_t  m_cond;
}ThreadSema;
*/
extern void ThreadSemaInit(ThreadSema *sema, int32 init);
extern void ThreadSemaDown(ThreadSema *sema);
extern void ThreadSemaUp(ThreadSema *sema);

typedef  slock_t pg_spin_lock;

static void spinlock_init(pg_spin_lock *lock);
static void spinlock_lock(pg_spin_lock *lock);
static void spinlock_unlock(pg_spin_lock *lock);


#define  INVALID_BORDER     (~((uint32)0))
typedef struct 
{
    char               *m_buf;    /* Data buffer */
    unsigned           m_Length;  /* Data buffer length */
    
    /* lock to protect offset and status */
    pg_spin_lock       pointerlock;                                        
    
    volatile uint32       m_Head;       /* Head of the loop */
    volatile uint32    m_Tail;       /* Tail of the buffer */
    volatile uint32    m_Border;     /* end of last tuple, so that we can send a complete tuple */
    volatile uint32    m_WrapAround; /* wrap around of the queue , for read only */
}DataPumpBuf;
/*
typedef enum  
{ 
    MT_THR_JOINABLE, 
    MT_THR_DETACHED 
}MT_thr_detach;
*/
typedef enum 
{ 
    DataPumpSndStatus_no_socket       = 0, 
    DataPumpSndStatus_set_socket      = 1, 
    DataPumpSndStatus_data_sending       = 2,
    DataPumpSndStatus_incomplete_data = 3,
    DataPumpSndStatus_done              = 4,
    DataPumpSndStatus_error           = 5, 
    DataPumpSndStatus_butty 
}DataPumpSndStatus;
typedef struct
{
    int32              nodeindex; /* Node index */
    int32              sock;      /* socket to transfer data */
    DataPumpSndStatus  status;    /* status of the data sending */

    
    pg_spin_lock       lock;      /* lock to protect status */

    int32              errorno;   /* error number of system call */
    
    DataPumpBuf        *buffer;      /* buffer used to send data */
    
    uint32              last_offset;/* used for fast send */
    uint32              remaining_length;

    
    size_t                ntuples;     /* counter for tuple */
    size_t              ntuples_put; /* number of tuples put into tuplestore */
    size_t              ntuples_get; /* number of tuples get from tuplestore */
    size_t                nfast_send;  /* counter for tuple */

    size_t                sleep_count; /* counter sleep */
}DataPumpNodeControl;

typedef struct
{
    /* Nodes control of the cursor. */
    DataPumpNodeControl   *nodes;
    
    /* Nodes range of this thread. */
    int32                node_base; /* include */        
    int32              node_end;  /* exclude */
    int32              node_num;  /* total node number */
    
    
    bool               thread_need_quit; /* quit flag */
    bool               error;
    bool               quit_status;         /* succeessful quit or not */
    
    bool               thread_running;     /* running flag */
    ThreadSema         send_sem;         /* used to wait for data */
    ThreadSema         quit_sem;         /* used to wait for thread quit */
}DataPumpThreadControl;

/* */
typedef struct ConvertControl
{
    char          sqname[MAX_CURSOR_LEN];   
    int           maxConn;
    int           connect_num;
    int           errNO;
    ConvertStatus cstatus;
    ThreadSema    quit_sem;         /* used to wait for thread quit */

    TimestampTz   begin_stamp;
    TimestampTz   finish_stamp;
}ConvertControl;

/* One struct for each cursor. */
typedef struct DataPumpSenderControl
{
    int32                 node_num;       /* number of node to send data */
    DataPumpNodeControl   *nodes;         /* sending status for nodes of this cursor */

    int32                 thread_num;     /* number of thread to send data */
    DataPumpThreadControl *thread_control;/* thread control of the sending threads */

    ConvertControl        convert_control;/* control info of thread convert */

    TupleTableSlot        *temp_slot;     /* temp slot used to put_tuplestore */
    int32                  tuple_len;      /* MAX tuplelen of sent tuple */
}DataPumpSenderControl;

/*
  *
  * This part is used for parallel workers to send tuples directly without gather/gatherMerge.
  *
  */
typedef struct ParallelSendDataQueue
{
    int32            nodeId;            /* which node to send data */
    int32            parallelWorkerNum;  /* send data by which worker */
    size_t           tuples_put;
    size_t           tuples_get;
    size_t           ntuples;
    size_t           fast_send;
    size_t           normal_send;
    size_t           send_times;
    size_t           no_data;
    size_t           send_data_len;
    size_t           write_data_len;
    bool             long_tuple;
	bool             wait_free_space;
    DataPumpSndStatus     status;      /* status of the data sending */
    bool             stuck;
    bool             last_send;
    

    /* lock to protect offset and status */
    pg_spin_lock     bufLock;                                        

    volatile bool    bufFull;            /* buffer is full? */
    volatile uint32     bufHead;            /* Head of the loop */
    volatile uint32  bufTail;            /* Tail of the buffer */
    volatile uint32  bufBorder;          /* end of last tuple, so that we can send a complete tuple */

    ThreadSema       sendSem;            /* worker need to wait for sem to be set when queue is full */
    unsigned         bufLength;          /* data buffer length */
    char             buffer[0];          /* data buffer */
} ParallelSendDataQueue;

#define PW_DATA_QUEUE_SIZE (offsetof(ParallelSendDataQueue, buffer) + g_SndThreadBufferSize * 1024)

typedef struct ParallelSendNodeControl
{
    int32                   nodeId;             /* which node to send data */
    int32                   sock;               /* socket to send data */
    int32                   numParallelWorkers; /* total number of parallel workers */
    DataPumpSndStatus       status;             /* status of the data sending */

    pg_spin_lock            lock;                /* lock to protect status */

    int32                   errorno;            /* error number of system call */

    int32                   current_buffer;     /* which buffer to be sent */
    ParallelSendDataQueue   **buffer;            /* buffer used to send data */
    
    uint32                  last_offset;        /* used for fast send */
    uint32                  remaining_length;

    size_t                    ntuples;            /* counter for tuple */
    size_t                    sleep_count;        /* counter sleep */
    size_t                  send_timies;
} ParallelSendNodeControl;

typedef struct ParallelWorkerControl
{
    int32   parallelWorkerNum;                  
    int32   numNodes;                           /* number of datanodes */
    int32   tupleLen;    
    int32   numThreads;

    ThreadSema *threadSem;             /* wake up sender to send data */

    ParallelSendDataQueue   **buffer;           /* data buffer to datanodes */
} ParallelWorkerControl;

typedef enum ParallelSendStatus
{
    ParallelSend_Error,
    ParallelSend_None,
    ParallelSend_Init,
    ParallelSend_ExecDone,
    ParallelSend_SendDone,
    ParallelSend_Finish
} ParallelSendStatus;

static ParallelSendStatus *senderstatus = NULL;

typedef struct ParallelSendSharedData
{
    int32      numExpectedParallelWorkers;
    int32      numLaunchedParallelWorkers;
    int32      numNodes;  
    int32      numSenderThreads;               /* number of thread senders */
    bool       *sender_error;                   /* sender thread occured error, need to quit */
    char       sq_key[SQUEUE_KEYSIZE];
    int16      nodeMap[MAX_NODES_NUMBER];
    bool       *nodeNoSocket;
    ParallelSendStatus *status;
    bool       *nodeSendDone;
        
    /* used to create locator for parallel workers */
    char       distributionType;
    AttrNumber  distributionKey;
    Oid        keytype;
    int        len;
    int        *consMap;

    ThreadSema *threadSem;                     /* sem used to wake up thread sender */

    ParallelSendDataQueue     *buffer;         /* data buffer to datanodes */
} ParallelSendSharedData;

typedef struct ParallelSendThreadControl
{
    int32              threadNum;
    /* Nodes control of the cursor. */
    ParallelSendNodeControl *nodes;
    
    /* Nodes range of this thread. */
    int32                node_base;        /* include */        
    int32              node_end;         /* exclude */
    int32              node_num;         /* total node number */
    
    
    bool               thread_need_quit; /* quit flag */
    bool               quit_status;         /* succeessful quit or not */
    
    bool               thread_running;     /* running flag */
    ThreadSema         quit_sem;         /* used to wait for thread quit */
    ThreadSema *threadSem;       /* wait for data */
} ParallelSendThreadControl;

typedef struct ParallelSendControl
{
    int32                       numParallelWorkers;/* number of parallel workers */
    int32                       numNodes;          /* number of node to send data */
    ParallelSendNodeControl     *nodes;            /* sending status for nodes of this cursor */

    int32                       numThreads;        /* number of thread to send data */
    ParallelSendThreadControl   *threadControl;    /* thread control of the sending threads */

    ConvertControl              convertControl;    /* control info of thread convert */

    bool                       *nodeNoSocket;
    bool                       *nodeSendDone;
    ParallelSendStatus         *status;

    ParallelSendSharedData      *sharedData;       /* global data in share memory */
} ParallelSendControl;

typedef struct ParallelSendDestReceiver
{
    DestReceiver pub;                /* public fields */
    dsm_segment *seg;               /* share memory */
    Locator        *locator;           /* decide which node the tuple should be sent to */
    ParallelWorkerControl *control; /* worker control */
    AttrNumber distKey;    
    ParallelSendSharedData *sharedData;
    Tuplestorestate **tstores;                    /* storage to buffer data if destination queue
                                                         * is full */
    MemoryContext mycontext;        /* context containing TQueueDestReceiver */
    MemoryContext tmpcxt;
    TupleDesc      tupledesc;        /* current top-level tuple descriptor */
    SharedQueue squeue;             /* share queue */
#ifdef __TBASE__
    uint64      send_tuples;        /* total tuples sent to shm_mq */
    TimestampTz send_total_time;    /* total time for sending the tuples */
#endif
} ParallelSendDestReceiver;

static bool DataPumpNodeCheck(void *sndctl, int32 nodeindex);
static int    DataPumpRawSendData(DataPumpNodeControl *node, int32 sock, char *data, int32 len, int32 *reason);
static uint32 DataSize(DataPumpBuf *buf);
static uint32 FreeSpace(DataPumpBuf *buf);
static char  *GetData(DataPumpBuf *buf, uint32 *uiLen);
static void   IncDataOff(DataPumpBuf *buf, uint32 uiLen);
static char  *GetWriteOff(DataPumpBuf *buf, uint32 *uiLen);
static void   IncWriteOff(DataPumpBuf *buf, uint32 uiLen);
static char  *GetWriteOff(DataPumpBuf *buf, uint32 *uiLen);
static int    ReserveSpace(DataPumpBuf *buf, uint32 len, uint32 *offset);
static void   FillReserveSpace(DataPumpBuf *buf, uint32 offset, char *p, uint32 len);
static uint32 FreeSpace(DataPumpBuf *buf);
static void   SetBorder(DataPumpBuf *buf);
static void  *DataPumpSenderThread(void *arg);
static void  PutData(DataPumpBuf *buf, char *data, uint32 len);
//static int32 CreateThread(void *(*f) (void *), void *arg, int32 mode);
static DataPumpBuf *BuildDataPumpBuf(void);
static void InitDataPumpNodeControl(int32 nodeindex, DataPumpNodeControl *control);
static void DataPumpCleanThread(DataPumpSenderControl *sender);
static bool DataPumpFlushAllData(DataPumpNodeControl  *nodes, DataPumpThreadControl* control);
static void DataPumpSendLoop(DataPumpNodeControl  *nodes, DataPumpThreadControl* control);
static void DestoryDataPumpBuf(DataPumpBuf *buffer);
static void InitDataPumpThreadControl(DataPumpThreadControl *control, DataPumpNodeControl *nodes, int32 base, int32 end, int32 total);
static bool CreateSenderThread(DataPumpSenderControl *sender);
#define DATA_PUMP_PREFIX "DataPump "

static int convert_connect(char *sqname);
static int convert_listen(char *sqname, int maxconn);
static int convert_sendfds(int fd, int *fds_to_send, int count, int *err);
static int convert_recvfds(int fd, int *fds, int count);
static bool send_fd_with_nodeid(char *sqname, int nodeid, int consumerIdx);
static void *ConvertThreadMain(void *arg);
static bool ConvertDone(ConvertControl *convert);
static int32 DataPumpNodeReadyForSend(void *sndctl, int32 nodeindex, int32 nodeId);
static int32 DataPumpSendToNode(void *sndctl, char *data, size_t len, int32 nodeindex);
static bool DataPumpTupleStoreDump(void *sndctl, int32 nodeindex, int32 nodeId,
                                             TupleTableSlot *tmpslot, 
                                             Tuplestorestate *tuplestore);
static bool socket_set_nonblocking(int fd, bool non_block);
static void DataPumpWakeupSender(void *sndctl, int32 nodeindex);
static bool ExecFastSendDatarow(TupleTableSlot *slot, void *sndctl, int32 nodeindex, MemoryContext tmpcxt);
static int  ReturnSpace(DataPumpBuf *buf, uint32 offset);
static uint32 BufferOffsetAdd(DataPumpBuf *buf, uint32 pointer, uint32 offset);

static ParallelSendControl* BuildParallelSendControl(SharedQueue sq);
static void InitParallelSendNodeControl(int32 nodeId, ParallelSendNodeControl *control, int32 numParallelWorkers);
static void InitParallelSendThreadControl(int32 threadNum, ParallelSendThreadControl *control, ParallelSendNodeControl *nodes);
static void InitParallelSendSharedData(SharedQueue sq, ParallelSendControl *senderControl, int consMap_len);
static void MapNodeDataBuffer(ParallelSendControl *senderControl);
static void *ParallelConvertThreadMain(void *arg);
static void *ParallelSenderThreadMain(void *arg);
static void ParallelSenderSendData(ParallelSendThreadControl *threadControl, bool last_send);
static bool SendNodeData(ParallelSendNodeControl *node, bool last_send);
static char *GetNodeData(ParallelSendDataQueue *buf, uint32 *uiLen, bool *long_tuple);
static uint32 NodeDataSize(ParallelSendDataQueue *buf, bool *long_tuple, bool *wait_free_space);
static void  IncNodeDataOff(ParallelSendDataQueue *buf, uint32 uiLen);
static int RawSendNodeData(ParallelSendNodeControl *node, int32 sock, char * data, int32 len, int32 * reason);
static int32 SetNodeSocket(void *sndctl, int32 nodeindex, int32 nodeId,  int32 socket);
static bool  ParallelSendCleanThread(ParallelSendControl *sender);
static void DestoryParallelSendControl (void* sndctl, int nodeid);
static bool ParallelSendReceiveSlot(TupleTableSlot *slot, DestReceiver *self);
static void ParallelSendStartupReceiver(DestReceiver *self, int operation, TupleDesc typeinfo);
static void ParallelSendShutdownReceiver(DestReceiver *self);
static void ParallelSendDestroyReceiver(DestReceiver *self);
static void SendNodeDataRemote(SharedQueue squeue, ParallelWorkerControl *control, ParallelSendDataQueue *buf, int32 consumerIdx,
                           TupleTableSlot *slot, Tuplestorestate **tuplestore, MemoryContext tmpcxt);
static bool ParallelSendDataRow(ParallelWorkerControl *control, ParallelSendDataQueue *buf, char *data, size_t len, int32 consumerIdx);
static uint32 BufferFreeSpace(ParallelSendDataQueue *buf);
static void SetBufferBorderAndWaitFlag(ParallelSendDataQueue *buf, bool long_tuple, bool wait_free_space);
static void PutNodeData(ParallelSendDataQueue *buf, char *data, uint32 len);
static char *GetBufferWriteOff(ParallelSendDataQueue *buf, uint32 *uiLen);
static void IncBufferWriteOff(ParallelSendDataQueue *buf, uint32 uiLen);
static bool ParallelFastSendDatarow(ParallelSendDataQueue *buf, TupleTableSlot *slot, void *ctl, int32 nodeindex, MemoryContext tmpcxt);
static int ReserveBufferSpace(ParallelSendDataQueue *buf, uint32 len, uint32 *offset);
static void FillReserveBufferSpace(ParallelSendDataQueue *buf, uint32 offset, char *p, uint32 len);
static uint32 DataBufferOffsetAdd(ParallelSendDataQueue *buf, uint32 pointer, uint32 offset);
static int ReturnBufferSpace(ParallelSendDataQueue *buf, uint32 offset);
static bool PumpTupleStoreToBuffer(ParallelWorkerControl *control, ParallelSendDataQueue *buf, int32 consumerIdx,
                               TupleTableSlot *tmpslot, Tuplestorestate *tuplestore, MemoryContext tmpcxt);
static void DestroyParallelSendReceiver(DestReceiver *self);
static bool NodeStatusCheck(SharedQueue squeue, ParallelSendControl *parallelSendControl, int numLaunchedParallelWorkers);
static void ParallelSendCleanAll(ParallelSendControl *sender, int nodeid);
static bool ParallelWorkerExecDone(int numLaunchedParallelWorkers, ParallelSendStatus *status);
static bool ParallelWorkerSendDone(int numLaunchedParallelWorkers, ParallelSendStatus *status);
static bool CreateParallelSenderThread(ParallelSendControl *sender);
static bool ConsumerExit(SharedQueue sq, ParallelSendControl *sender, int numParallelWorkers, 
                              int *count, Bitmapset **unconnect);
static void ReportErrorConsumer(SharedQueue squeue);

#endif

/*
 * SharedQueuesInit
 *    Initialize the reference on the shared memory hash table where all shared
 * queues are stored. Invoked during postmaster initialization.
 */
void
SharedQueuesInit(void)
{
    HASHCTL info;
    int        hash_flags;
    bool     found;

    info.keysize = SQUEUE_KEYSIZE;

    if(g_UseDataPump)
        info.entrysize = SQUEUE_HDR_SIZE(TBASE_MAX_DATANODE_NUMBER);
    else
        info.entrysize = SQUEUE_SIZE;

    /*
     * Create hash table of fixed size to avoid running out of
     * SQueueSyncs
     */
    hash_flags = HASH_ELEM | HASH_FIXED_SIZE;

    SharedQueues = ShmemInitHash("Shared Queues", NUM_SQUEUES,
                                 NUM_SQUEUES, &info, hash_flags);
#ifdef __TBASE__
    if (g_UseDataPump)
    {
        HASHCTL ctl;
        int        flags;

        ctl.keysize = SQUEUE_KEYSIZE;
        ctl.entrysize = sizeof(DisConsumer);

        flags = HASH_ELEM | HASH_FIXED_SIZE;

        DisConsumerHash = ShmemInitHash("Disconnect Consumers", NUM_SQUEUES,
                             NUM_SQUEUES, &ctl, flags);
    }
#endif

    /*
     * Synchronization stuff is in separate structure because we need to
     * initialize all items now while in the postmaster.
     * The structure is actually an array, each array entry is assigned to
     * each instance of SharedQueue in use.
     */
    SQueueSyncs = ShmemInitStruct("Shared Queues Sync",
                                  SQUEUE_SYNC_SIZE * NUM_SQUEUES,
                                  &found);
    if (!found)
    {
        int    i, l;
        int    nlocks = (NUM_SQUEUES * (TBASE_MAX_DATANODE_NUMBER)); /* 
                                                      * (TBASE_MAX_DATANODE_NUMBER - 1)
                                                      * consumers + 1 producer
                                                      */
        bool    foundLocks;

        /* Initialize LWLocks for queues */
        SQueueLocks = (LWLockPadded *) ShmemInitStruct("Shared Queue Locks",
                        sizeof(LWLockPadded) * nlocks, &foundLocks);

        /* either both syncs and locks, or none of them */
        Assert(! foundLocks);

        /* Register the trannche tranche in the main tranches array */
        LWLockRegisterTranche(LWTRANCHE_SHARED_QUEUES, "Shared Queue Locks");

        l = 0;
        for (i = 0; i < NUM_SQUEUES; i++)
        {
            SQueueSync *sqs = GET_SQUEUE_SYNC(i);
            int            j;

            sqs->queue = NULL;
            LWLockInitialize(&(SQueueLocks[l]).lock, LWTRANCHE_SHARED_QUEUES);
            sqs->sqs_producer_lwlock = &(SQueueLocks[l++]).lock;
            InitSharedLatch(&sqs->sqs_producer_latch);

            for (j = 0; j < TBASE_MAX_DATANODE_NUMBER-1; j++)
            {
                InitSharedLatch(&sqs->sqs_consumer_sync[j].cs_latch);

                LWLockInitialize(&(SQueueLocks[l]).lock,
                                 LWTRANCHE_SHARED_QUEUES);

                sqs->sqs_consumer_sync[j].cs_lwlock = &(SQueueLocks[l++]).lock;
            }
        }
    }
}


Size
SharedQueueShmemSize(void)
{
    Size sqs_size;

	/* Shared Queues Sync */
    sqs_size = mul_size(NUM_SQUEUES, SQUEUE_SYNC_SIZE);
	/* Shared Queue Locks */
    sqs_size = add_size(sqs_size, mul_size((NUM_SQUEUES * (TBASE_MAX_DATANODE_NUMBER)), sizeof(LWLockPadded)));

#ifdef __TBASE__
    if (g_UseDataPump)
    {
	    /* Disconnect Consumers */
        sqs_size = add_size(sqs_size, hash_estimate_size(NUM_SQUEUES, sizeof(DisConsumer)));
    }
#endif

    /* Shared Queues */
    if(g_UseDataPump)
        return add_size(sqs_size, hash_estimate_size(NUM_SQUEUES, SQUEUE_HDR_SIZE(TBASE_MAX_DATANODE_NUMBER)));
    else
        return add_size(sqs_size, hash_estimate_size(NUM_SQUEUES, SQUEUE_SIZE));
}

/*
 * SharedQueueAcquire
 *     Reserve a named shared queue for future data exchange between processes
 * supplying tuples to remote Datanodes. Invoked when a remote query plan is
 * registered on the Datanode. The number of consumers is known at this point,
 * so shared queue may be formatted during reservation. The first process that
 * is acquiring the shared queue on the Datanode does the formatting.
 */
#ifdef __TBASE__
void
SharedQueueAcquire(const char *sqname, int ncons, bool parallelSend, int numParallelWorkers, bool with_params)
#else
void
SharedQueueAcquire(const char *sqname, int ncons)
#endif
{// #lizard forgives
    bool        found;
    SharedQueue sq;
    int trycount = 0;

    Assert(IsConnFromDatanode());
    Assert(ncons > 0);

tryagain:
    LWLockAcquire(SQueuesLock, LW_EXCLUSIVE);

    /*
     * Setup PGXC_PARENT_NODE_ID right now to ensure that the cleanup happens
     * correctly even if the consumer never really binds to the shared queue.
     */
    PGXC_PARENT_NODE_ID = PGXCNodeGetNodeIdFromName(PGXC_PARENT_NODE,
            &PGXC_PARENT_NODE_TYPE);

    if (PGXC_PARENT_NODE_ID == -1)
    {
        LWLockRelease(SQueuesLock);
        elog(ERROR, "could not get nodeid, maybe node %s is droppped.", PGXC_PARENT_NODE);
    }

    sq = (SharedQueue) hash_search(SharedQueues, sqname, HASH_ENTER_NULL, &found);
    if (!sq)
    {
        LWLockRelease(SQueuesLock);
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                errmsg("out of shared queue, please increase shared_queues")));
    }

    /* First process acquiring queue should format it */
    if (!found)
    {
        int        qsize;   /* Size of one queue */
        int        i;
        char   *heapPtr;
#ifdef __TBASE__
        SQueueSync *sqsync = NULL;
#endif
        elog(DEBUG1, "Create a new SQueue %s and format it for %d consumers", sqname, ncons);

        /* Initialize the shared queue */
        sq->sq_pid = 0;
        sq->sq_nodeid = -1;
        sq->sq_refcnt = 1;
#ifdef SQUEUE_STAT
        sq->stat_finish = false;
        sq->stat_paused = 0;
#endif
#ifdef __TBASE__
        sq->sender_destroy = false;
        sq->sq_error = false;
        sq->with_params = with_params;

        sq->parallelWorkerSendTuple = parallelSend;
        sq->numParallelWorkers      = numParallelWorkers;

        memset(sq->err_msg, 0, ERR_MSGSIZE);
        sq->has_err_msg = false;

        sq->producer_done = false;
        sq->nConsumer_done = 0;

        SpinLockInit(&sq->lock);
#endif
        /*
         * Assign sync object (latches to wait on)
         * XXX We may want to optimize this and do smart search instead of
         * iterating the array.
         */
        for (i = 0; i < NUM_SQUEUES; i++)
        {
            SQueueSync *sqs = GET_SQUEUE_SYNC(i);
            if (sqs->queue == NULL)
            {
                sqs->queue = (void *) sq;
                sq->sq_sync = sqs;
                break;
            }
        }

        Assert(sq->sq_sync != NULL);

        if (g_UseDataPump)
        {
            sq->sq_nconsumers = ncons + 1;
        }
        else
        {
            sq->sq_nconsumers = ncons;
        }
        
        /* Determine queue size for a single consumer */
        qsize = (SQUEUE_SIZE - SQUEUE_HDR_SIZE(sq->sq_nconsumers)) / sq->sq_nconsumers;

        heapPtr = (char *) sq;
        /* Skip header */
        heapPtr += SQUEUE_HDR_SIZE(sq->sq_nconsumers);

#ifdef __TBASE__
		/* Init latch */
		sqsync = sq->sq_sync;
        InitSharedLatch(&sqsync->sqs_producer_latch);
#endif

        /* Set up consumer queues */
        for (i = 0; i < sq->sq_nconsumers; i++)
        {
            ConsState *cstate = &(sq->sq_consumers[i]);

            cstate->cs_pid = 0;
            cstate->cs_node = -1;
            cstate->cs_ntuples = 0;
            cstate->cs_status = CONSUMER_ACTIVE;
            cstate->cs_qstart = heapPtr;
            cstate->cs_qlength = qsize;
            cstate->cs_qreadpos = 0;
            cstate->cs_qwritepos = 0;
#ifdef __TBASE__
            cstate->send_fd = false;
            cstate->cs_done = false;
            InitSharedLatch(&sqsync->sqs_consumer_sync[i].cs_latch);
#endif
            heapPtr += qsize;
        }
        
        Assert(heapPtr <= ((char *) sq) + SQUEUE_SIZE);
    }
    else
    {
        int i;

        elog(DEBUG1, "Found an existing SQueue %s - (sq_pid:%d, sq_nodeid:%d,"
            " sq_nconsumers:%d",
            sqname, sq->sq_pid, sq->sq_nodeid, sq->sq_nconsumers);

        for (i = 0; i < sq->sq_nconsumers; i++)
        {
            elog(DEBUG1, "SQueue %s, consumer (%d) information (cs_pid:%d,"
                    " cs_node:%d, cs_ntuples:%d, cs_status: %d",
                    sqname, i,
                    sq->sq_consumers[i].cs_pid, 
                    sq->sq_consumers[i].cs_node, 
                    sq->sq_consumers[i].cs_ntuples, 
                    sq->sq_consumers[i].cs_status); 
        }

        /*
         * A race condition is possible here. The previous operation might  use
         * the same Shared Queue name if that was different execution of the
         * same Portal. So here we should try to determine if that Shared Queue
         * belongs to this execution or that is not-yet-released Shared Queue
         * of previous operation.
         * Though at the moment I am not sure, but I believe the BIND stage is
         * only happening after completion of ACQUIRE stage, so it is enough
         * to verify the producer (the very first node that binds) is not bound
         * yet. If it is bound, sleep for a moment and try again. No reason to
         * sleep longer, the producer needs just a quantum of CPU time to UNBIND
         * itself.
         */
        if (sq->sq_pid != 0)
        {
            int            i;
            bool        old_squeue = true;
            for (i = 0; i < sq->sq_nconsumers; i++)
            {
                ConsState *cstate = &(sq->sq_consumers[i]);
                if (cstate->cs_node == PGXC_PARENT_NODE_ID)
                {
                    SQueueSync *sqsync = sq->sq_sync;

                    LWLockAcquire(sqsync->sqs_consumer_sync[i].cs_lwlock,
                                  LW_EXCLUSIVE);
                    /* verify status */
                    if (cstate->cs_status != CONSUMER_DONE)
                        old_squeue = false;

                    LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
                    break;
                }
            }
            if (old_squeue)
            {
                LWLockRelease(SQueuesLock);
				(trycount < 10) ? pg_usleep(10000L) : pg_usleep(1000000L);
                elog(DEBUG1, "SQueue race condition, give the old producer to "
                        "finish the work and retry again");
                trycount++;
                if (trycount >= 20)
                    elog(ERROR, "Couldn't resolve SQueue race condition after"
                            " %d tries", trycount);
                goto tryagain;
            }
        }
        LWLockAcquire(sq->sq_sync->sqs_producer_lwlock, LW_EXCLUSIVE);
        sq->sq_refcnt++;
        LWLockRelease(sq->sq_sync->sqs_producer_lwlock);
    }
    LWLockRelease(SQueuesLock);

    if (g_DataPumpDebug)
    {
        elog(LOG, "Squeue %s acquire, nodeid %d, pid %d.", sqname, PGXC_PARENT_NODE_ID, MyProcPid);
    }
}


/*
 * SharedQueueBind
 *    Bind to the shared queue specified by sqname either as a consumer or as a
 * producer. The first process that binds to the shared queue becomes a producer
 * and receives the consumer map, others become consumers and receive queue
 * indexes to read tuples from.
 * The consNodes int list identifies the nodes involved in the current step.
 * The distNodes int list describes result distribution of the current step.
 * The consNodes should be a subset of distNodes.
 * The myindex and consMap parameters are binding results. If caller process
 * is bound to the query as a producer myindex is set to -1 and index of the
 * each consumer (order number in the consNodes) is stored to the consMap array
 * at the position of the node in the distNodes. For the producer node
 * SQ_CONS_SELF is stored, nodes from distNodes list which are not members of
 * consNodes or if it was reported they won't read results, they are represented
 * as SQ_CONS_NONE.
 */
SharedQueue
SharedQueueBind(const char *sqname, List *consNodes,
                                   List *distNodes, int *myindex, int *consMap
#ifdef __TBASE__
                                   ,
                                    DataPumpSender *sender
#endif
                                    )
{// #lizard forgives
    bool        found;
    SharedQueue sq;
    LWLockAcquire(SQueuesLock, LW_EXCLUSIVE);

    PGXC_PARENT_NODE_ID = PGXCNodeGetNodeIdFromName(PGXC_PARENT_NODE,
            &PGXC_PARENT_NODE_TYPE);
    sq = (SharedQueue) hash_search(SharedQueues, sqname, HASH_FIND, &found);

    if (PGXC_PARENT_NODE_ID == -1)
    {
        LWLockRelease(SQueuesLock);
        elog(ERROR, "could not get nodeid, maybe node %s is droppped.", PGXC_PARENT_NODE);
    }
    
    /*
     * It's not clear but it seems that if the producer fails even before a
     * consumer binds to the shared queue, the producer may remove the shared
     * queue (or would refcount mechanism fully protect us against that?). So
     * instead of panicing, just throw a soft error.
     */
    if (!found)
    {
        LWLockRelease(SQueuesLock);
        elog(ERROR, "Shared queue %s not found", sqname);
    }

    /*
     * Now acquire the queue-specific lock and then release the top level lock.
     * We must follow a strict ordering between SQueuesLock,
     * sqs_producer_lwlock and the consumer cs_lwlock to avoid a deadlock.
     */
    LWLockAcquire(sq->sq_sync->sqs_producer_lwlock, LW_EXCLUSIVE);
    LWLockRelease(SQueuesLock);

    if (sq->sq_pid == 0)
    {
            /* Producer */
            int        i;
            ListCell *lc;
#ifdef __TBASE__
            int pro_nodeid = 0;
            int pro_index  = 0;
#endif
            Assert(consMap);

			elog(DEBUG1, "Bind node %s to squeue of step %s as a producer, parentPGXCNode %s, parentPGXCPid %d",
				 PGXC_PARENT_NODE, sqname, parentPGXCNode, parentPGXCPid);

            /* Initialize the shared queue */
            sq->sq_pid = MyProcPid;
            sq->sq_nodeid = PGXC_PARENT_NODE_ID;
            OwnLatch(&sq->sq_sync->sqs_producer_latch);

            for (i = 0; i < MAX_NODES_NUMBER; i++)
            {
                sq->nodeMap[i] = SQ_CONS_INIT;
            }
                            
            i = 0;

            role = Squeue_Producer;
            share_sq = sq;
            share_sq_bak = sq;
                
            if (g_UseDataPump)
            {
                foreach(lc, distNodes)
                {
                    int            nodeid = lfirst_int(lc);

                    ConsState  *cstate;
                    int         j;

                    if (list_member_int(consNodes, nodeid))
                    {
                        for (j = 0; j < sq->sq_nconsumers; j++)
                        {
                            cstate = &(sq->sq_consumers[j]);
                            if (cstate->cs_node == nodeid)
                            {
                                /* The process already reported that queue won't read */
                                elog(DEBUG1, "Node %d of SQueue %s is released already "
                                        "at consumer %d, cs_status %d",
                                     nodeid, sqname, j, cstate->cs_status);
                                consMap[i++] = SQ_CONS_NONE;

                                sq->nodeMap[nodeid] = SQ_CONS_NONE;
                                
                                break;
                            }
                            else if (cstate->cs_node == -1)
                            {
                                /* found unused slot, assign the consumer to it */
                                elog(DEBUG1, "Node %d of SQueue %s is bound at consumer "
                                        "%d, cs_status %d",
                                        nodeid, sqname, j, cstate->cs_status);
                                consMap[i++] = j;
                                cstate->cs_node = nodeid;

                                if (nodeid >= MAX_NODES_NUMBER)
                                {
                                    LWLockRelease(sq->sq_sync->sqs_producer_lwlock);
                                    elog(ERROR, "nodeid(%d) exceeds max number(%d) of nodes.", nodeid, MAX_NODES_NUMBER);
                                }
                                
                                sq->nodeMap[nodeid] = j;

                                if(nodeid == PGXC_PARENT_NODE_ID)
                                {
                                    pro_index = j;
                                    pro_nodeid = nodeid;
                                    cstate->cs_status = CONSUMER_DONE;
                                    cstate->cs_pid = MyProcPid;
                                }
                                
                                break;
                            }
                        }
                    }
                    else
                    {
                        elog(DEBUG1, "Node %d of SQueue %s is not in the "
                                "redistribution list and hence would never connect",
                                nodeid, sqname);
                        consMap[i++] = SQ_CONS_NONE;
                        sq->nodeMap[nodeid] = SQ_CONS_NONE;
                    }
                }
            }
            else
            {
                foreach(lc, distNodes)
                {
                    int            nodeid = lfirst_int(lc);

                    /*
                     * Producer won't go to shared queue to hand off tuple to itself,
                     * so we do not need to create queue for that entry.
                     */
                    if (nodeid == PGXC_PARENT_NODE_ID)
                    {
                        /* Producer must be in the consNodes list */
                        Assert(list_member_int(consNodes, nodeid));
                        elog(DEBUG1, "SQueue %s consumer @%d is set to self",
                                sqname, i);
                        consMap[i++] = SQ_CONS_SELF;

                        if (nodeid >= MAX_NODES_NUMBER)
                        {
                            LWLockRelease(sq->sq_sync->sqs_producer_lwlock);
                            elog(ERROR, "nodeid(%d) exceeds max number(%d) of nodes.", nodeid, MAX_NODES_NUMBER);
                        }
                        
                        sq->nodeMap[nodeid] = SQ_CONS_SELF;
                    }
                    /*
                     * This node may connect as a consumer, store consumer id to the map
                     * and initialize consumer queue
                     */
                    else if (list_member_int(consNodes, nodeid))
                    {
                        ConsState  *cstate;
                        int         j;

                        for (j = 0; j < sq->sq_nconsumers; j++)
                        {
                            cstate = &(sq->sq_consumers[j]);
                            if (cstate->cs_node == nodeid)
                            {
                                /* The process already reported that queue won't read */
                                elog(DEBUG1, "Node %d of SQueue %s is released already "
                                        "at consumer %d, cs_status %d",
                                     nodeid, sqname, j, cstate->cs_status);
                                consMap[i++] = SQ_CONS_NONE;

                                if (nodeid >= MAX_NODES_NUMBER)
                                {
                                    LWLockRelease(sq->sq_sync->sqs_producer_lwlock);
                                    elog(ERROR, "nodeid(%d) exceeds max number(%d) of nodes.", nodeid, MAX_NODES_NUMBER);
                                }
                                
                                sq->nodeMap[nodeid] = SQ_CONS_NONE;
                                break;
                            }
                            else if (cstate->cs_node == -1)
                            {
                                /* found unused slot, assign the consumer to it */
                                elog(DEBUG1, "Node %d of SQueue %s is bound at consumer "
                                        "%d, cs_status %d",
                                        nodeid, sqname, j, cstate->cs_status);
                                consMap[i++] = j;
                                cstate->cs_node = nodeid;

                                if (nodeid >= MAX_NODES_NUMBER)
                                {
                                    LWLockRelease(sq->sq_sync->sqs_producer_lwlock);
                                    elog(ERROR, "nodeid(%d) exceeds max number(%d) of nodes.", nodeid, MAX_NODES_NUMBER);
                                }
                                
                                sq->nodeMap[nodeid] = j;
                                break;
                            }
                        }
                     }
                    /*
                     * Consumer from this node won't ever connect as upper level step
                     * is not executed on the node. Discard resuls that may go to that
                     * node, if any.
                     */
                    else
                    {
                        elog(DEBUG1, "Node %d of SQueue %s is not in the "
                                "redistribution list and hence would never connect",
                                nodeid, sqname);
                        consMap[i++] = SQ_CONS_NONE;
                    }
                }
            }
            if (myindex)
                *myindex = -1;

            /*
             * Increment the refcnt only when producer binds. This is a bit
             * asymmetrical, but the way things are currently setup, a consumer
             * though calls SharedQueueBind, never calls SharedQueueUnBind. The
             * unbinding is done only by the producer after it waits for all
             * consumers to finish.
             *
             * XXX This ought to be fixed someday to simplify things in Shared
             * Queue handling
             */ 
            //sq->sq_refcnt++;

            if (g_UseDataPump)
            {
                /*
                  * if parallel workers can send tuples directly, use parallel mode;
                  * else use the default single mode.
                  */
                if (sq->parallelWorkerSendTuple)
                {
                    ParallelSendControl *parallelSender = BuildParallelSendControl(sq);

                    if (parallelSender == NULL)
                    {
                        LWLockRelease(sq->sq_sync->sqs_producer_lwlock);
                        elog(ERROR, "failed to create parallel sender control.");
                    }

                    /* store nodeid, consumerIdx, sockfd */
                    if(SetNodeSocket(sq->parallelSendControl, pro_index, pro_nodeid, MyProcPort->sock) != DataPumpOK)
                    {
                        LWLockRelease(sq->sq_sync->sqs_producer_lwlock);
                        elog(ERROR, "could not set sockfd of producer.");
                    }
                }
                else
                {
                    *sender = BuildDataPumpSenderControl(sq);

                    if (*sender == NULL)
                    {
                        LWLockRelease(sq->sq_sync->sqs_producer_lwlock);
                        elog(ERROR, "failed to create sender control.");
                    }

                    /* store nodeid, consumerIdx, sockfd */
                    if(DataPumpSetNodeSocket(sq->sender, pro_index, pro_nodeid, MyProcPort->sock) != DataPumpOK)
                    {
                        LWLockRelease(sq->sq_sync->sqs_producer_lwlock);
                        elog(ERROR, "could not set sockfd of producer.");
                    }
                }
            }

            if (g_DataPumpDebug)
            {
                elog(LOG, "Squeue %s bind as producer, nodeid %d, pid %d, consumeridx %d.",
                          sqname, pro_nodeid, MyProcPid, pro_index);
            }
    }
    else
    {
        int     nconsumers;
        ListCell *lc;

        /* Producer should be different process */
        Assert(sq->sq_pid != MyProcPid);

        elog(DEBUG1, "SQueue %s has a bound producer from node %d, pid %d",
                sqname, sq->sq_nodeid, sq->sq_pid);
		elog(DEBUG1, "Bind node %s to SQueue %s as a consumer %d, parentPGXCNode %s, parentPGXCPid %d", PGXC_PARENT_NODE, sqname, sq->sq_pid, parentPGXCNode, parentPGXCPid);

        /* Sanity checks */
        Assert(myindex);
        *myindex = -1;
        /* Ensure the passed in consumer list matches the queue */
        nconsumers = 0;
        foreach (lc, consNodes)
        {
            int         nodeid = lfirst_int(lc);
            int            i;

            if (nodeid == sq->sq_nodeid)
            {
                /*
                 * This node is a producer it should be in the consumer list,
                 * but no consumer queue for it
                 */
                continue;
            }

            /* find consumer queue for the node */
            for (i = 0; i < sq->sq_nconsumers; i++)
            {
                ConsState *cstate = &(sq->sq_consumers[i]);
                if (cstate->cs_node == nodeid)
                {
                    nconsumers++;
                    if (nodeid == PGXC_PARENT_NODE_ID)
                    {
                        /*
                         * Current consumer queue is that from which current
                         * session will be sending out data rows.
                         * Initialize the queue to let producer know we are
                         * here and runnng.
                         */
                        SQueueSync *sqsync = sq->sq_sync;

                        elog(DEBUG1, "SQueue %s, consumer node %d is same as "
                                "the parent node", sqname, nodeid);
                        LWLockAcquire(sqsync->sqs_consumer_sync[i].cs_lwlock,
                                      LW_EXCLUSIVE);
                        /* Make sure no consumer bound to the queue already */
                        Assert(cstate->cs_pid == 0);
                        /* make sure the queue is ready to read */
                        Assert(cstate->cs_qlength > 0);
                        /* verify status */
                        if (cstate->cs_status == CONSUMER_ERROR ||
                                cstate->cs_status == CONSUMER_DONE)
                        {
                            int status = cstate->cs_status;
                            /*
                             * Producer failed by the time the consumer connect.
                             * Change status to "Done" to allow producer unbind
                             * and report problem to the parent.
                             */
                            //cstate->cs_status = CONSUMER_DONE;
                            /* Producer may be waiting for status change */
                            SetLatch(&sqsync->sqs_producer_latch);
                            LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
                            LWLockRelease(sqsync->sqs_producer_lwlock);
                            ereport(ERROR,
                                    (errcode(ERRCODE_PRODUCER_ERROR),
                                     errmsg("Squeue %s Pid %d:Producer failed while we were waiting - status was %d, err_msg %s", 
                                     sq->sq_key, MyProcPid, status, sq->err_msg)));
                        }
                        /*
                         * Any other status is acceptable. Normally it would be
                         * ACTIVE. If producer have had only few rows to emit
                         * and it is already done the status would be EOF.
                         */

                        /* Set up the consumer */
                        cstate->cs_pid = MyProcPid;

                        elog(DEBUG1, "SQueue %s, consumer at %d, status %d - "
                                "setting up consumer node %d, pid %d",
                                sqname, i, cstate->cs_status, cstate->cs_node,
                                cstate->cs_pid);
                        /* return found index */
                        *myindex = i;

                        OwnLatch(&sqsync->sqs_consumer_sync[i].cs_latch);
                        LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);

                        role = Squeue_Consumer;
                        share_sq = sq;
#if 0
                        if (g_UseDataPump)
                        {
                            LWLockRelease(sq->sq_sync->sqs_producer_lwlock);
                            
                            while(1)
                            {
                                if (send_fd_with_nodeid(sq->sq_key, nodeid, i))
                                {
                                    if (g_DataPumpDebug)
                                    {
                                        elog(LOG, "Squeue %s:succeed to send fd, nodeid %d, consumeridx %d, pid %d.",
                                                  sq->sq_key, nodeid, i, MyProcPid);
                                    }
                                    break;
                                }
                                else
                                {
                                    LWLockAcquire(sqsync->sqs_consumer_sync[i].cs_lwlock, LW_SHARED);
                                    if (cstate->cs_status == CONSUMER_EOF)
                                    {
                                        if (g_DataPumpDebug)
                                        {
                                            elog(LOG, "Squeue %s:failed to send fd because producer have finished work, nodeid %d, consumeridx %d, pid %d.",
                                                      sq->sq_key, nodeid, i, MyProcPid);
                                        }
                                        LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
                                        break;
                                    }
                                    else if (cstate->cs_status == CONSUMER_ERROR ||
                                            cstate->cs_status == CONSUMER_DONE)
                                    {
                                        LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
                                        ereport(ERROR,
                                            (errcode(ERRCODE_PRODUCER_ERROR),
                                             errmsg("Producer failed while we were waiting for sending fd - status was %d", cstate->cs_status)));
                                    }
                                    LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
                                }

                                pg_usleep(1000L);
                            }

                            if (g_DataPumpDebug)
                            {
                                elog(LOG, "Squeue %s bind as consumer, nodeid %d, pid %d, consumeridx %d.",
                                          sqname, nodeid, MyProcPid, i);
                            }

                            return sq;
                        }

                        elog(DEBUG1, "queue %s, nodeid %d, pid %d, bind.", sqname, nodeid, MyProcPid);
#endif
                    }
                    else
                        elog(DEBUG1, "SQueue %s, consumer node %d is not same as "
                                "the parent node %d", sqname, nodeid,
                                PGXC_PARENT_NODE_ID);
                    break;
                }
            }
            /* Check if entry was found and therefore loop was broken */
            Assert(i < sq->sq_nconsumers);
        }
#ifdef __TBASE__
        /* should not happen */
        if (*myindex == -1)
        {
            LWLockRelease(sq->sq_sync->sqs_producer_lwlock);
            elog(ERROR, "Squeue %s bind as comsumer:failed to found slot for consumer, pid %d.", sqname, MyProcPid);
        }
#endif
        /* Check the consumer is found */
        Assert(*myindex != -1);
        if (g_UseDataPump)
        {
            Assert(sq->sq_nconsumers == (nconsumers + 1));
        }
        else
        {
            Assert(sq->sq_nconsumers == nconsumers);
        }
    }
    LWLockRelease(sq->sq_sync->sqs_producer_lwlock);
    return sq;
}


/*
 * Push data from the local tuplestore to the queue for specified consumer.
 * Return true if succeeded and the tuplestore is now empty. Return false
 * if specified queue has not enough room for the next tuple.
 */
static bool
SharedQueueDump(SharedQueue squeue, int consumerIdx,
                           TupleTableSlot *tmpslot, Tuplestorestate *tuplestore)
{// #lizard forgives
    ConsState  *cstate = &(squeue->sq_consumers[consumerIdx]);

    elog(DEBUG3, "Dumping SQueue %s data for consumer at %d, "
            "producer - node %d, pid %d, "
            "consumer - node %d, pid %d, status %d",
            squeue->sq_key, consumerIdx,
            squeue->sq_nodeid, squeue->sq_pid,
            cstate->cs_node, cstate->cs_pid, cstate->cs_status);

    /* discard stored data if consumer is not active */
    if (cstate->cs_status != CONSUMER_ACTIVE)
    {
        elog(DEBUG3, "Discarding SQueue %s data for consumer at %d not active",
                squeue->sq_key, consumerIdx);
        tuplestore_clear(tuplestore);
        return true;
    }

    /*
     * Tuplestore does not clear eof flag on the active read pointer, causing
     * the store is always in EOF state once reached when there is a single
     * read pointer. We do not want behavior like this and workaround by using
     * secondary read pointer. Primary read pointer (0) is active when we are
     * writing to the tuple store, also it is used to bookmark current position
     * when reading to be able to roll back and return just read tuple back to
     * the store if we failed to write it out to the queue.
     * Secondary read pointer is for reading, and its eof flag is cleared if a
     * tuple is written to the store.
     */
    tuplestore_select_read_pointer(tuplestore, 1);

    /* If we have something in the tuplestore try to push this to the queue */
    while (!tuplestore_ateof(tuplestore))
    {
        /* save position */
        tuplestore_copy_read_pointer(tuplestore, 1, 0);

        /* Try to get next tuple to the temporary slot */
        if (!tuplestore_gettupleslot(tuplestore, true, false, tmpslot))
        {
            /* false means the tuplestore in EOF state */
            elog(DEBUG3, "Tuplestore for SQueue %s returned EOF",
                    squeue->sq_key);
            break;
        }
#ifdef SQUEUE_STAT
        cstate->stat_buff_reads++;
#endif

        /* The slot should contain a data row */
        Assert(tmpslot->tts_datarow);

        /* check if queue has enough room for the data */
        if (QUEUE_FREE_SPACE(cstate) < sizeof(int) + tmpslot->tts_datarow->msglen)
        {
            /*
             * If stored tuple does not fit empty queue we are entering special
             * procedure of pushing it through.
             */
            if (cstate->cs_ntuples <= 0)
            {
                /*
                 * If pushing throw is completed wake up and proceed to next
                 * tuple, there could be enough space in the consumer queue to
                 * fit more.
                 */
                bool done = sq_push_long_tuple(cstate, tmpslot->tts_datarow);

                /*
                 * sq_push_long_tuple writes some data anyway, so wake up
                 * the consumer.
                 */
                SetLatch(&squeue->sq_sync->sqs_consumer_sync[consumerIdx].cs_latch);

                if (done)
                    continue;
            }

            /* Restore read position to get same tuple next time */
            tuplestore_copy_read_pointer(tuplestore, 0, 1);
#ifdef SQUEUE_STAT
            cstate->stat_buff_returns++;
#endif

            /* We might advance the mark, try to truncate */
            tuplestore_trim(tuplestore);

            /* Prepare for writing, set proper read pointer */
            tuplestore_select_read_pointer(tuplestore, 0);

            /* ... and exit */
            return false;
        }
        else
        {
            /* Enqueue data */
            QUEUE_WRITE(cstate, sizeof(int), (char *) &tmpslot->tts_datarow->msglen);
            QUEUE_WRITE(cstate, tmpslot->tts_datarow->msglen, tmpslot->tts_datarow->msg);

            /* Increment tuple counter. If it was 0 consumer may be waiting for
             * data so try to wake it up */
            if ((cstate->cs_ntuples)++ == 0)
                SetLatch(&squeue->sq_sync->sqs_consumer_sync[consumerIdx].cs_latch);
        }
    }

    /* Remove rows we have just read */
    tuplestore_trim(tuplestore);

    /* prepare for writes, set read pointer 0 as active */
    tuplestore_select_read_pointer(tuplestore, 0);

    return true;
}


/*
 * SharedQueueWrite
 *    Write data from the specified slot to the specified queue. If the
 * tuplestore passed in has tuples try and write them first.
 * If specified queue is full the tuple is put into the tuplestore which is
 * created if necessary
 */
void
SharedQueueWrite(SharedQueue squeue, int consumerIdx,
                            TupleTableSlot *slot, Tuplestorestate **tuplestore,
                            MemoryContext tmpcxt)
{// #lizard forgives
    ConsState  *cstate = &(squeue->sq_consumers[consumerIdx]);
    SQueueSync *sqsync = squeue->sq_sync;
    LWLockId    clwlock = sqsync->sqs_consumer_sync[consumerIdx].cs_lwlock;
    RemoteDataRow datarow;
    bool        free_datarow;

    Assert(cstate->cs_qlength > 0);

    LWLockAcquire(clwlock, LW_EXCLUSIVE);

#ifdef SQUEUE_STAT
    cstate->stat_writes++;
#endif

    /*
     * If we have anything in the local storage try to dump this first,
     * but do not try to dump often to avoid overhead of creating temporary
     * tuple slot. It should be OK to dump if queue is half empty.
     */
    if (*tuplestore)
    {
        bool dumped = false;

        if (QUEUE_FREE_SPACE(cstate) > cstate->cs_qlength / 2)
        {
            TupleTableSlot *tmpslot;

            tmpslot = MakeSingleTupleTableSlot(slot->tts_tupleDescriptor);
            dumped = SharedQueueDump(squeue, consumerIdx, tmpslot, *tuplestore);
            ExecDropSingleTupleTableSlot(tmpslot);
        }
        if (!dumped)
        {
            /* No room to even dump local store, append the tuple to the store
             * and exit */
#ifdef SQUEUE_STAT
            cstate->stat_buff_writes++;
#endif
            LWLockRelease(clwlock);
            tuplestore_puttupleslot(*tuplestore, slot);
            return;
        }
    }

    /* Get datarow from the tuple slot */
    if (slot->tts_datarow)
    {
        /*
         * The function ExecCopySlotDatarow always make a copy, but here we
         * can optimize and avoid copying the data, so we just get the reference
         */
        datarow = slot->tts_datarow;
        free_datarow = false;
    }
    else
    {
        datarow = ExecCopySlotDatarow(slot, tmpcxt);
        free_datarow = true;
    }
    if (QUEUE_FREE_SPACE(cstate) < sizeof(int) + datarow->msglen)
    {
        /* Not enough room, store tuple locally */
        LWLockRelease(clwlock);

        /* clean up */
        if (free_datarow)
            pfree(datarow);

        /* Create tuplestore if does not exist */
        if (*tuplestore == NULL)
        {
            int            ptrno PG_USED_FOR_ASSERTS_ONLY;
            char         storename[64];

#ifdef SQUEUE_STAT
            elog(DEBUG1, "Start buffering %s node %d, %d tuples in queue, %ld writes and %ld reads so far",
                 squeue->sq_key, cstate->cs_node, cstate->cs_ntuples, cstate->stat_writes, cstate->stat_reads);
#endif
            *tuplestore = tuplestore_begin_datarow(false, work_mem, tmpcxt);
            /* We need is to be able to remember/restore the read position */
            snprintf(storename, 64, "%s node %d", squeue->sq_key, cstate->cs_node);
            tuplestore_collect_stat(*tuplestore, storename);
            /*
             * Allocate a second read pointer to read from the store. We know
             * it must have index 1, so needn't store that.
             */
            ptrno = tuplestore_alloc_read_pointer(*tuplestore, 0);
            Assert(ptrno == 1);
        }

#ifdef SQUEUE_STAT
        cstate->stat_buff_writes++;
#endif
        /* Append the slot to the store... */
        tuplestore_puttupleslot(*tuplestore, slot);

        /* ... and exit */
        return;
    }
    else
    {
        /* do not supply data to closed consumer */
        if (cstate->cs_status == CONSUMER_ACTIVE)
        {
            elog(DEBUG3, "SQueue %s, consumer is active, writing data",
                    squeue->sq_key);
            /* write out the data */
            QUEUE_WRITE(cstate, sizeof(int), (char *) &datarow->msglen);
            QUEUE_WRITE(cstate, datarow->msglen, datarow->msg);
            /* Increment tuple counter. If it was 0 consumer may be waiting for
             * data so try to wake it up */
            if ((cstate->cs_ntuples)++ == 0)
                SetLatch(&sqsync->sqs_consumer_sync[consumerIdx].cs_latch);
        }
        else
            elog(DEBUG2, "SQueue %s, consumer is not active, no need to supply data",
                    squeue->sq_key);

        /* clean up */
        if (free_datarow)
            pfree(datarow);
    }
    LWLockRelease(clwlock);
}


/*
 * SharedQueueRead
 *    Read one data row from the specified queue into the provided tupleslot.
 * Returns true if EOF is reached on the specified consumer queue.
 * If the queue is empty, behavior is controlled by the canwait parameter.
 * If canwait is true it is waiting while row is available or EOF or error is
 * reported, if it is false, the slot is emptied and false is returned.
 */
bool
SharedQueueRead(SharedQueue squeue, int consumerIdx,
                            TupleTableSlot *slot, bool canwait)
{// #lizard forgives
    ConsState  *cstate = &(squeue->sq_consumers[consumerIdx]);
    SQueueSync *sqsync = squeue->sq_sync;
    RemoteDataRow datarow;
    int         datalen;
    Assert(cstate->cs_qlength > 0);


#ifdef __TBASE__
    if (g_UseDataPump)
    {
        if (!cstate->send_fd)
        {
            int nodeid = cstate->cs_node;
            char *sqname = squeue->sq_key;

            if (g_DataPumpDebug)
            {
                elog(LOG, "Squeue %s:try to send fd, nodeid %d, consumeridx %d, pid %d.",
                          squeue->sq_key, nodeid, consumerIdx, MyProcPid);
            }
            
            while(1)
            {
                if (send_fd_with_nodeid(squeue->sq_key, nodeid, consumerIdx))
                {
                    if (g_DataPumpDebug)
                    {
                        elog(LOG, "Squeue %s:succeed to send fd, nodeid %d, consumeridx %d, pid %d.",
                                  squeue->sq_key, nodeid, consumerIdx, MyProcPid);
                    }

                    cstate->send_fd = true;
                    break;
                }
                else
                {
                    LWLockAcquire(sqsync->sqs_consumer_sync[consumerIdx].cs_lwlock, LW_SHARED);
                    if (cstate->cs_status == CONSUMER_EOF)
                    {
                        if (g_DataPumpDebug)
                        {
                            elog(LOG, "Squeue %s:failed to send fd because producer have finished work, nodeid %d, consumeridx %d, pid %d.",
                                      squeue->sq_key, nodeid, consumerIdx, MyProcPid);
                        }
                        LWLockRelease(sqsync->sqs_consumer_sync[consumerIdx].cs_lwlock);
                        break;
                    }
                    else if (cstate->cs_status == CONSUMER_ERROR ||
                            cstate->cs_status == CONSUMER_DONE)
                    {
                        LWLockRelease(sqsync->sqs_consumer_sync[consumerIdx].cs_lwlock);
                        ereport(ERROR,
                            (errcode(ERRCODE_PRODUCER_ERROR),
                             errmsg("Squeue %s Pid %d:Producer failed while we were waiting for sending fd - status was %d, err_msg %s", 
                             squeue->sq_key, MyProcPid, cstate->cs_status, squeue->err_msg)));
                    }
                    LWLockRelease(sqsync->sqs_consumer_sync[consumerIdx].cs_lwlock);
                }

				if (squeue->sender_destroy)
				{
					elog(ERROR, "squeue %s Pid %d: Producer failed(destroy) while we were waiting for sending fd - status was %d, err_msg %s",
						   squeue->sq_key, MyProcPid, cstate->cs_status, squeue->err_msg);
				}

                pg_usleep(1000L);
            }

            if (g_DataPumpDebug)
            {
                elog(LOG, "Squeue %s bind as consumer, nodeid %d, pid %d, consumeridx %d.",
                          sqname, nodeid, MyProcPid, consumerIdx);
            }
        }
    }
#endif

    /*
     * If we run out of produced data while reading, we would like to wake up
     * and tell the producer to produce more. But in order to ensure that the
     * producer does not miss the signal, we must obtain sufficient lock on the
     * queue. In order to allow multiple consumers to read from their
     * respective queues at the same time, we obtain a SHARED lock on the
     * queue. But the producer must obtain an EXCLUSIVE lock to ensure it does
     * not miss the signal.
     *
     * Again, important to follow strict lock ordering.
     */ 
    LWLockAcquire(sqsync->sqs_producer_lwlock, LW_SHARED);
    LWLockAcquire(sqsync->sqs_consumer_sync[consumerIdx].cs_lwlock, LW_EXCLUSIVE);

    Assert(cstate->cs_status != CONSUMER_DONE);
    while (cstate->cs_ntuples <= 0)
    {
        elog(DEBUG3, "SQueue %s, consumer node %d, pid %d, status %d - "
                "no tuples in the queue", squeue->sq_key,
                cstate->cs_node, cstate->cs_pid, cstate->cs_status);

        if (cstate->cs_status == CONSUMER_EOF)
        {
            elog(DEBUG1, "SQueue %s, consumer node %d, pid %d, status %d - "
                    "EOF marked. Informing produer by setting CONSUMER_DONE",
                    squeue->sq_key,
                    cstate->cs_node, cstate->cs_pid, cstate->cs_status);

            /* Inform producer the consumer have done the job */
            cstate->cs_status = CONSUMER_DONE;
            /* no need to receive notifications */
            DisownLatch(&sqsync->sqs_consumer_sync[consumerIdx].cs_latch);
            /* producer done the job and no more rows expected, clean up */
            LWLockRelease(sqsync->sqs_consumer_sync[consumerIdx].cs_lwlock);
            ExecClearTuple(slot);
            /*
             * notify the producer, it may be waiting while consumers
             * are finishing
             */
            SetLatch(&sqsync->sqs_producer_latch);
            LWLockRelease(sqsync->sqs_producer_lwlock);
            share_sq = NULL;
            return true;
        }
        else if (cstate->cs_status == CONSUMER_ERROR)
        {
            elog(DEBUG1, "SQueue %s, consumer node %d, pid %d, status %d - "
                    "CONSUMER_ERROR set",
                    squeue->sq_key,
                    cstate->cs_node, cstate->cs_pid, cstate->cs_status);
            /*
             * There was a producer error while waiting.
             * Release all the locks and report problem to the caller.
             */
            LWLockRelease(sqsync->sqs_consumer_sync[consumerIdx].cs_lwlock);
            LWLockRelease(sqsync->sqs_producer_lwlock);

            /*
             * Reporting error will cause transaction rollback and clean up of
             * all portals. We can not mark the portal so it does not access
             * the queue so we should hold it for now. We should prevent queue
             * unbound in between.
             */
#ifdef __PG_REGRESS__
            ereport(ERROR,
                    (errcode(ERRCODE_PRODUCER_ERROR),
                     errmsg("Failed to read from SQueue %s, "
                         "consumer (node %d, pid %d, status %d) - "
                         "CONSUMER_ERROR set, err_msg %s",
                         squeue->sq_key,
                         cstate->cs_node, cstate->cs_pid, cstate->cs_status, squeue->err_msg)));
#else
			ereport(ERROR,
					(errcode(ERRCODE_PRODUCER_ERROR),
					 errmsg("Failed to read from SQueue, "
						 "CONSUMER_ERROR set, err_msg %s",
						 squeue->err_msg)));
#endif
        }
#ifdef __TBASE__
		if (squeue->sender_destroy)
		{
			LWLockRelease(sqsync->sqs_consumer_sync[consumerIdx].cs_lwlock);
			LWLockRelease(sqsync->sqs_producer_lwlock);
			elog(ERROR, "squeue %s Pid %d: Producer failed while we were reading data from squeue - status was %d, err_msg %s",
				   squeue->sq_key, MyProcPid, cstate->cs_status, squeue->err_msg);
		}
        /* got end query request */
        if (end_query_requested)
        {
            end_query_requested = false;

            cstate->cs_done = true;

            SpinLockAcquire(&squeue->lock);
            squeue->nConsumer_done++;
            SpinLockRelease(&squeue->lock);

            if (g_DataPumpDebug)
            {
                elog(LOG, "squeue %s consumer %d pid %d set query done.", squeue->sq_key, consumerIdx, MyProcPid);
            }
        }
#endif
        if (canwait)
        {
            /* Prepare waiting on empty buffer */
            ResetLatch(&sqsync->sqs_consumer_sync[consumerIdx].cs_latch);
            LWLockRelease(sqsync->sqs_consumer_sync[consumerIdx].cs_lwlock);

            elog(DEBUG3, "SQueue %s, consumer (node %d, pid %d, status %d) - "
                    "no queued tuples to read, waiting "
                    "for producer to produce more data",
                    squeue->sq_key,
                    cstate->cs_node, cstate->cs_pid, cstate->cs_status);

            /* Inform the producer to produce more while we wait for it */
            SetLatch(&sqsync->sqs_producer_latch);
            LWLockRelease(sqsync->sqs_producer_lwlock);

            /* Wait for notification about available info */
            WaitLatch(&sqsync->sqs_consumer_sync[consumerIdx].cs_latch,
                    WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT, 1000L,
                    WAIT_EVENT_MQ_INTERNAL);

            /* got the notification, restore lock and try again */
            LWLockAcquire(sqsync->sqs_producer_lwlock, LW_SHARED);
            LWLockAcquire(sqsync->sqs_consumer_sync[consumerIdx].cs_lwlock, LW_EXCLUSIVE);
        }
        else
        {
            LWLockRelease(sqsync->sqs_consumer_sync[consumerIdx].cs_lwlock);
            LWLockRelease(sqsync->sqs_producer_lwlock);

            elog(DEBUG3, "SQueue %s, consumer (node %d, pid %d, status %d) - "
                    "no queued tuples to read, caller can't wait ",
                    squeue->sq_key,
                    cstate->cs_node, cstate->cs_pid, cstate->cs_status);
            ExecClearTuple(slot);
            return false;
        }

    }

    elog(DEBUG3, "SQueue %s, consumer (node %d, pid %d, status %d) - "
            "%d queued tuples to read",
            squeue->sq_key,
            cstate->cs_node, cstate->cs_pid, cstate->cs_status,
            cstate->cs_ntuples);

    /* have at least one row, read it in and store to slot */
    QUEUE_READ(cstate, sizeof(int), (char *) (&datalen));
    datarow = (RemoteDataRow) palloc(sizeof(RemoteDataRowData) + datalen);
    datarow->msgnode = InvalidOid;
    datarow->msglen = datalen;
    if (datalen > cstate->cs_qlength - sizeof(int))
        sq_pull_long_tuple(cstate, datarow, consumerIdx, sqsync);
    else
        QUEUE_READ(cstate, datalen, datarow->msg);
    ExecStoreDataRowTuple(datarow, slot, true);
    (cstate->cs_ntuples)--;
#ifdef SQUEUE_STAT
    cstate->stat_reads++;
#endif
    /* sanity check */
    Assert((cstate->cs_ntuples == 0) == (cstate->cs_qreadpos == cstate->cs_qwritepos));
    LWLockRelease(sqsync->sqs_consumer_sync[consumerIdx].cs_lwlock);
    LWLockRelease(sqsync->sqs_producer_lwlock);
    return false;
}


/*
 * Mark specified consumer as closed discarding all input which may already be
 * in the queue.
 * If consumerIdx is -1 the producer is cleaned up. Producer need to wait for
 * consumers before releasing the queue, so if there are yet active consumers,
 * they are notified about the problem and they should disconnect from the
 * queue as soon as possible.
 */
void
SharedQueueReset(SharedQueue squeue, int consumerIdx)
{// #lizard forgives
    SQueueSync *sqsync = squeue->sq_sync;

    /* 
     * We may have already cleaned up, but then an abort signalled us to clean up.
     * Avoid segmentation fault on abort
     */
    if (!sqsync)
        return;

    if (consumerIdx == -1)
    {
        int i;

        if (g_DataPumpDebug)
        {
            elog(DEBUG1, "SQueue %s, requested to reset producer node %d, pid %d - "
                    "Now also resetting all consumers",
                    squeue->sq_key, squeue->sq_nodeid, squeue->sq_pid);
        }

#ifdef __TBASE__
        /* tell sender and convert to exit */
        if (g_UseDataPump)
        {
            ReportErrorConsumer(squeue);
            
            if(!squeue->sender_destroy && squeue->sq_pid == MyProcPid)
            {
                if (!squeue->parallelWorkerSendTuple)
                {
                    DataPumpWaitSenderDone(squeue->sender, false);
                    DestoryDataPumpSenderControl(squeue->sender, squeue->sq_nodeid);
                    squeue->sender = NULL;
                    squeue->sender_destroy = true;
                }
                else
                {
                    ParallelSendCleanThread(squeue->parallelSendControl);
                    DestoryParallelSendControl(squeue->parallelSendControl, squeue->sq_nodeid);
                    squeue->parallelSendControl = NULL;
                    squeue->sender_destroy = true;
                }

                share_sq = NULL;
            }

            RemoveDisConsumerHash(squeue->sq_key);
        }
#endif

        LWLockAcquire(sqsync->sqs_producer_lwlock, LW_SHARED);
        /* check queue states */
        for (i = 0; i < squeue->sq_nconsumers; i++)
        {
            ConsState *cstate = &squeue->sq_consumers[i];
            LWLockAcquire(sqsync->sqs_consumer_sync[i].cs_lwlock, LW_EXCLUSIVE);

            /*
             * If producer being reset before it is reached the end of the
             * result set, that means consumer probably would not get all
             * the rows and it should report error if the consumer's parent ever
             * try to read. No need to raise error if consumer is just closed.
             * If consumer is done already we do not need to change the status.
             */
            if (cstate->cs_status != CONSUMER_EOF &&
                    cstate->cs_status != CONSUMER_DONE)
            {
                elog(DEBUG1, "SQueue %s, reset consumer at %d, "
                        "consumer node %d, pid %d, status %d - marking CONSUMER_ERROR",
                        squeue->sq_key, i, cstate->cs_node, cstate->cs_pid,
                        cstate->cs_status);

                cstate->cs_status = CONSUMER_ERROR;
                /* discard tuples which may already be in the queue */
                cstate->cs_ntuples = 0;
                /* keep consistent with cs_ntuples*/
                cstate->cs_qreadpos = cstate->cs_qwritepos = 0;

                /* wake up consumer if it is sleeping */
                SetLatch(&sqsync->sqs_consumer_sync[i].cs_latch);

                /* Tell producer about change in the state */
                SetLatch(&sqsync->sqs_producer_latch);

                if (g_DataPumpDebug)
                {
                    elog(DEBUG1, "Squeue %s:producer(pid %d, nodeid %d) reset consumer idx %d, nodeid %d, pid %d.",
                               squeue->sq_key, squeue->sq_pid, squeue->sq_nodeid, i, cstate->cs_node, cstate->cs_pid);
                }
            }
            LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
        }
        LWLockRelease(sqsync->sqs_producer_lwlock);
    }
    else
    {
        ConsState  *cstate = &(squeue->sq_consumers[consumerIdx]);
        
        LWLockAcquire(sqsync->sqs_producer_lwlock, LW_SHARED);

        if (g_DataPumpDebug)
        {
            elog(DEBUG1, "SQueue %s, requested to reset consumer at %d, "
                    "consumer node %d, pid %d, status %d",
                    squeue->sq_key, consumerIdx, cstate->cs_node, cstate->cs_pid,
                    cstate->cs_status);
        }

        LWLockAcquire(sqsync->sqs_consumer_sync[consumerIdx].cs_lwlock,
                      LW_EXCLUSIVE);

        if (cstate->cs_status != CONSUMER_DONE)
        {
            elog(DEBUG1, "SQueue %s, consumer at %d, "
                "consumer node %d, pid %d, status %d - marking CONSUMER_DONE",
                squeue->sq_key, consumerIdx, cstate->cs_node, cstate->cs_pid,
                cstate->cs_status);

            /* Inform producer the consumer have done the job */
            cstate->cs_status = CONSUMER_DONE;
            /*
             * No longer need to receive notifications. If consumer has not
             * connected the latch is not owned
             */
            if (cstate->cs_pid > 0)
                DisownLatch(&sqsync->sqs_consumer_sync[consumerIdx].cs_latch);
            /*
             * notify the producer, it may be waiting while consumers
             * are finishing
             */
            SetLatch(&sqsync->sqs_producer_latch);

            if (g_DataPumpDebug)
            {
                elog(DEBUG1, "Squeue %s:consumer reset consumer idx %d, nodeid %d, pid %d.",
                           squeue->sq_key, consumerIdx, cstate->cs_node, cstate->cs_pid);
            }
        }

        LWLockRelease(sqsync->sqs_consumer_sync[consumerIdx].cs_lwlock);

        LWLockRelease(sqsync->sqs_producer_lwlock);
    }
}

#ifdef __TBASE__
void
SetDisConnectConsumer(const char *sqname, int cons)
{// #lizard forgives
    bool found;
    DisConsumer *ent = NULL;

    if (g_DataPumpDebug)
    {
        elog(LOG, "SetDisConnectConsumer: squeue %s pid %d cons %d", sqname, MyProcPid, cons);
    }

    LWLockAcquire(DisconnectConsLock, LW_EXCLUSIVE);

    /* vacuum */
    if (hash_get_num_entries(DisConsumerHash) > (NUM_SQUEUES / 4) && g_DisConsumer_timeout)
    {
        HASH_SEQ_STATUS scan_status;
        DisConsumer  *item;
        TimestampTz t = GetCurrentTimestamp();

        hash_seq_init(&scan_status, DisConsumerHash);
        while ((item = (DisConsumer *) hash_seq_search(&scan_status)) != NULL)
        {
            if (t - item->time >= (g_DisConsumer_timeout * 60 * USECS_PER_SEC))
            {
                hash_search(DisConsumerHash, item->sq_key, HASH_REMOVE, NULL);
            }
        }
    }

    ent = (DisConsumer *) hash_search(DisConsumerHash, sqname, HASH_ENTER_NULL, &found);

    if (!ent)
    {
        LWLockRelease(DisconnectConsLock);
        elog(ERROR, "SetDisConnectConsumer: could not find hash entry, out of memory");
    }

    PGXC_PARENT_NODE_ID = PGXCNodeGetNodeIdFromName(PGXC_PARENT_NODE,
                        &PGXC_PARENT_NODE_TYPE);

    if (PGXC_PARENT_NODE_ID == -1)
    {
        LWLockRelease(DisconnectConsLock);
        elog(ERROR, "SetDisConnectConsumer: could not get nodeid, maybe node %s is droppped.", PGXC_PARENT_NODE);
    }

    if (!found)
    {
        ent->nConsumer = 0;
        ent->time = GetCurrentTimestamp();
        memset(ent->disconnect, 0, sizeof(bool) * TBASE_MAX_DATANODE_NUMBER);
    }

    if (!ent->disconnect[PGXC_PARENT_NODE_ID])
    {
        ent->nConsumer++;
    }
    
    ent->disconnect[PGXC_PARENT_NODE_ID] = true;

    if (ent->nConsumer == cons)
    {
        hash_search(DisConsumerHash, sqname, HASH_REMOVE, NULL);
    }
        
    LWLockRelease(DisconnectConsLock);
}

bool
IsConsumerDisConnect(char *sqname, int nodeid)
{
    bool found;
    bool disconnect;
    DisConsumer *ent = NULL;
    
    LWLockAcquire(DisconnectConsLock, LW_SHARED);
    
    ent = (DisConsumer *) hash_search(DisConsumerHash, sqname, HASH_FIND, &found);

    if (!found)
    {
        disconnect = false;
    }
    else
    {
        disconnect = ent->disconnect[nodeid];
    }

    LWLockRelease(DisconnectConsLock);

    return disconnect;
}

void
RemoveDisConsumerHash(char *sqname)
{
    LWLockAcquire(DisconnectConsLock, LW_EXCLUSIVE);

    hash_search(DisConsumerHash, sqname, HASH_REMOVE, NULL);
    
    LWLockRelease(DisconnectConsLock);
}
#endif

/*
 * Disconnect a remote consumer for the given shared queue.
 *
 * A node may not join a shared queue in certain circumstances such as when the
 * other side of the join has not produced any rows and the RemoteSubplan is
 * not at all executed on the node. Even in that case, we should receive a
 * 'statement close' message from the remote node and mark that specific
 * consumer as DONE.
 */
void
SharedQueueDisconnectConsumer(const char *sqname)
{// #lizard forgives
    bool        found;
    SharedQueue squeue;
    int            i;
    SQueueSync *sqsync;
    
    /*
     * Be prepared to be called even when there are no shared queues setup.
     */
    if (!SharedQueues)
        return;
#ifdef __TBASE__
    elog(DEBUG1, "Squeue %s: SharedQueueDisconnectConsumer Pid %d, nodeid %d", sqname, MyProcPid, PGXC_PARENT_NODE_ID);
#endif
    LWLockAcquire(SQueuesLock, LW_EXCLUSIVE);

    squeue = (SharedQueue) hash_search(SharedQueues, sqname, HASH_FIND, &found);
#ifdef __TBASE__
    if (!found || squeue->sender_destroy)
#else
    if (!found || squeue->sq_pid == 0)
#endif
    {
        /*
         * If the shared queue with the given name is not found or if the
         * producer has not yet bound, nothing is done.
         *
         * XXX Is it possible that the producer binds after this remote
         * consumer has closed the statement? If that happens, the prodcuer
         * will not know that this consumer is not going to connect. We
         * need to study this further and make adjustments if necessary.
         */
        LWLockRelease(SQueuesLock);
#ifdef __TBASE__
        elog(DEBUG1, "Squeue %s: SharedQueueDisconnectConsumer squeue not Found Pid %d, nodeid %d", sqname, MyProcPid, PGXC_PARENT_NODE_ID);
#endif
        return;
    }

    sqsync = squeue->sq_sync;

#if 0
    PGXC_PARENT_NODE_ID = PGXCNodeGetNodeIdFromName(PGXC_PARENT_NODE,
                            &PGXC_PARENT_NODE_TYPE);
#endif

    LWLockAcquire(sqsync->sqs_producer_lwlock, LW_EXCLUSIVE);
    LWLockRelease(SQueuesLock);
    
    /* check queue states */
    for (i = 0; i < squeue->sq_nconsumers; i++)
    {
        ConsState *cstate = &squeue->sq_consumers[i];
        LWLockAcquire(sqsync->sqs_consumer_sync[i].cs_lwlock, LW_EXCLUSIVE);

        if (cstate->cs_node == PGXC_PARENT_NODE_ID)
        {
            cstate->cs_status = CONSUMER_DONE;
            /* discard tuples which may already be in the queue */
            cstate->cs_ntuples = 0;
            /* keep consistent with cs_ntuples*/
            cstate->cs_qreadpos = cstate->cs_qwritepos = 0;

            LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);

            if (g_DataPumpDebug)
            {
                elog(LOG, "Squeue %s:consumer disconnect consumer idx %d, nodeid %d, pid %d.", 
                           sqname, i, cstate->cs_node, MyProcPid);
            }

            break;
        }
#ifdef __TBASE__
        else if (cstate->cs_node == -1 && squeue->sq_pid == 0)
        {
            cstate->cs_node = PGXC_PARENT_NODE_ID;
            cstate->cs_status = CONSUMER_DONE;
            /* discard tuples which may already be in the queue */
            cstate->cs_ntuples = 0;
            /* keep consistent with cs_ntuples*/
            cstate->cs_qreadpos = cstate->cs_qwritepos = 0;

            LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);

            if (g_DataPumpDebug)
            {
                elog(LOG, "Squeue %s:consumer(not found) disconnect consumer idx %d, nodeid %d, pid %d.", 
                           sqname, i, cstate->cs_node, MyProcPid);
            }

            break;
        }
#endif        
        LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
    }
    SetLatch(&sqsync->sqs_producer_latch);
    LWLockRelease(sqsync->sqs_producer_lwlock);
}

/*
 * Assume that not yet connected consumers won't connect and reset them.
 * That should allow to Finish/UnBind the queue gracefully and prevent
 * producer hanging.
 */
void
SharedQueueResetNotConnected(SharedQueue squeue)
{
    SQueueSync *sqsync = squeue->sq_sync;
    int result = 0;
    int i;

    elog(DEBUG1, "SQueue %s, resetting all unconnected consumers",
            squeue->sq_key);

    LWLockAcquire(squeue->sq_sync->sqs_producer_lwlock, LW_EXCLUSIVE);

    /* check queue states */
    for (i = 0; i < squeue->sq_nconsumers; i++)
    {
        ConsState *cstate = &squeue->sq_consumers[i];
        LWLockAcquire(sqsync->sqs_consumer_sync[i].cs_lwlock, LW_EXCLUSIVE);

        if (cstate->cs_pid == 0 &&
                cstate->cs_status != CONSUMER_DONE)
        {
            result++;
            elog(DEBUG1, "SQueue %s, consumer at %d, consumer node %d, pid %d, "
                    "status %d is cancelled - marking CONSUMER_ERROR", squeue->sq_key, i,
                    cstate->cs_node, cstate->cs_pid, cstate->cs_status);
            cstate->cs_status = CONSUMER_DONE;
            /* discard tuples which may already be in the queue */
            cstate->cs_ntuples = 0;
            /* keep consistent with cs_ntuples*/
            cstate->cs_qreadpos = cstate->cs_qwritepos = 0;

            /* wake up consumer if it is sleeping */
            SetLatch(&sqsync->sqs_consumer_sync[i].cs_latch);

            if (g_DataPumpDebug)
            {
                elog(LOG, "Squeue %s: producer(pid %d, nodeid %d) reset NotConnected consumer idx %d, nodeid %d, pid %d.",
                           squeue->sq_key, squeue->sq_pid, squeue->sq_nodeid, i, cstate->cs_node, cstate->cs_pid);
            }
        }
        LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
    }

    LWLockRelease(sqsync->sqs_producer_lwlock);
}

/*
 * Wait on the producer latch, for timeout msec. If timeout occurs, return
 * true, else return false.
 */
bool
SharedQueueWaitOnProducerLatch(SharedQueue squeue, long timeout)
{
    SQueueSync *sqsync = squeue->sq_sync;
    int rc = WaitLatch(&sqsync->sqs_producer_latch,
            WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT,
            timeout, WAIT_EVENT_MQ_INTERNAL);
    ResetLatch(&sqsync->sqs_producer_latch);
    return (rc & (WL_TIMEOUT|WL_POSTMASTER_DEATH));
}

/*
 * Determine if producer can safely pause work.
 * The producer can pause if all consumers have enough data to read while
 * producer is sleeping.
 * Obvoius case when the producer can not pause if at least one queue is empty.
 */
bool
SharedQueueCanPause(SharedQueue squeue)
{// #lizard forgives
    SQueueSync *sqsync = squeue->sq_sync;
    bool         result = true;
    int         usedspace;
    int            ncons;
    int         i;

    usedspace = 0;
    ncons = 0;
    for (i = 0; result && (i < squeue->sq_nconsumers); i++)
    {
        ConsState *cstate = &(squeue->sq_consumers[i]);
        LWLockAcquire(sqsync->sqs_consumer_sync[i].cs_lwlock, LW_SHARED);
        /*
         * Count only consumers that may be blocked.
         * If producer has finished scanning and pushing local buffers some
         * consumers may be finished already.
         */
        if (cstate->cs_status == CONSUMER_ACTIVE)
        {
            /* can not pause if some queue is empty */
            result = (cstate->cs_ntuples > 0);
            usedspace += (cstate->cs_qwritepos > cstate->cs_qreadpos ?
                              cstate->cs_qwritepos - cstate->cs_qreadpos :
                              cstate->cs_qlength + cstate->cs_qwritepos
                                                 - cstate->cs_qreadpos);
            ncons++;
        }
        LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
    }
    
    if (!ncons)
        return false;

    /*
     * Pause only if average consumer queue is full more then on half.
     */
    if (result)
        result = (usedspace / ncons > squeue->sq_consumers[0].cs_qlength / 2);
#ifdef SQUEUE_STAT
    if (result)
        squeue->stat_paused++;
#endif
    return result;
}


int
SharedQueueFinish(SharedQueue squeue, TupleDesc tupDesc,
                              Tuplestorestate **tuplestore)
{// #lizard forgives
    bool            tuplestore_done = false;
    int32           unfinish_tuplestore = 0;
    SQueueSync *sqsync = squeue->sq_sync;
    TupleTableSlot *tmpslot = NULL;
    int             i;
    int             nstores = 0;
    int             send_times = 0;
    int             timeout = consumer_connect_timeout * 1000;

    elog(DEBUG1, "SQueue %s, finishing the SQueue - producer node %d, "
            "pid %d, nconsumers %d", squeue->sq_key, squeue->sq_nodeid,
            squeue->sq_pid, squeue->sq_nconsumers);
    
#ifdef __TBASE__
    if (!sqsync)
        return 0;

    if (g_DataPumpDebug)
    {
        elog(LOG, "SQueue %s, finishing the SQueue - producer node %d, "
            "pid %d, nconsumers %d", squeue->sq_key, squeue->sq_nodeid,
            squeue->sq_pid, squeue->sq_nconsumers);
    }
    
    if (g_UseDataPump)
    {
        if(!squeue->sender_destroy && squeue->sq_pid == MyProcPid)
        {
            if (squeue->parallelWorkerSendTuple)
            {
                int  i = 0;
                int  j = 0;
                ParallelSendControl *parallelSendControl = squeue->parallelSendControl;
                ParallelSendNodeControl  *nodes = parallelSendControl->nodes;
                ParallelSendSharedData *sharedData = parallelSendControl->sharedData;

                ParallelSendCleanThread(parallelSendControl);

                if (NodeStatusCheck(squeue, parallelSendControl, 0))
                {
                    elog(ERROR, "parallel sender:node status abnormal.");
                }
                
                /* sanity check and log stats */
                for (i = 0; i < squeue->sq_nconsumers; i++)
                {
                    ConsState *cstate = &squeue->sq_consumers[i];
                    ParallelSendNodeControl  *node = &nodes[i];
#ifdef __TBASE__                    
                    if (node->status == DataPumpSndStatus_set_socket)
#endif
                    {
                        for (j = 0; j < sharedData->numLaunchedParallelWorkers; j++)
                        {
                            ParallelSendDataQueue *buffer = node->buffer[j];

                            if (buffer->tuples_get != buffer->tuples_put)
                            {
                                elog(ERROR, "node %d, workerId %d, get_tuples:%zu, put_tuples:%zu, fast_send:%zu, normal_send:%zu, total:%zu",
                                             node->nodeId, j, buffer->tuples_get, buffer->tuples_put, buffer->fast_send, buffer->normal_send, buffer->ntuples);
                            }

                            if (buffer->normal_send + buffer->fast_send != buffer->ntuples)
                            {
                                elog(ERROR, "node %d, workerId %d, fast_send:%zu, normal_send:%zu, total:%zu",
                                             node->nodeId, j, buffer->fast_send, buffer->normal_send, buffer->ntuples);
                            }

                            if (buffer->bufHead != buffer->bufTail)
                            {
                                elog(ERROR, "data left in buffer. node %d, buffer %d, head %u, tail %u, border %u.",
                                            node->nodeId, j, buffer->bufHead, buffer->bufTail, buffer->bufBorder);
                            }

                            if (g_DataPumpDebug)
                            {
                                elog(LOG, "SendTuples Statistic node %d, workerId %d, get_tuples:%zu, put_tuples:%zu, fast_send:%zu, normal_send:%zu, total:%zu." 
                                           "buffer head:%u, buffer tail:%u, buffer border:%u.", node->nodeId, j, buffer->tuples_get, buffer->tuples_put, buffer->fast_send, 
                                           buffer->normal_send, buffer->ntuples, buffer->bufHead, buffer->bufTail, buffer->bufBorder);
                            }
                        }

                        if (g_DataPumpDebug)
                        {
                            elog(LOG, "node %d, sleep_count:%zu", node->nodeId, node->sleep_count);
                        }
                    }
                    else
                    {
                        for (j = 0; j < sharedData->numLaunchedParallelWorkers; j++)
                        {
                            ParallelSendDataQueue *buffer = node->buffer[j];

                            if (g_DataPumpDebug)
                            {
                                elog(LOG, "node %d does not fetch tuples, workerId %d, get_tuples:%zu, put_tuples:%zu, fast_send:%zu, normal_send:%zu, total:%zu",
                                           node->nodeId, j, buffer->tuples_get, buffer->tuples_put, buffer->fast_send, buffer->normal_send, buffer->ntuples);
                            }
                        }
                    }

                    if (cstate->cs_node != squeue->sq_nodeid)
                    {
                        if (node->sock != NO_SOCKET)
                        {
                            close(node->sock);
                            node->sock = NO_SOCKET;
                        }
                    }
                }
                
                DestoryParallelSendControl(parallelSendControl, squeue->sq_nodeid);
                squeue->parallelSendControl = NULL;
                
                squeue->sender_destroy = true;

                share_sq = NULL;

                if (g_DataPumpDebug)
                {
                    elog(LOG, "SQueue %s, parallel finishing the SQueue - producer node %d, "
                                 "pid %d, nconsumers %d", squeue->sq_key, squeue->sq_nodeid,
                                 squeue->sq_pid, squeue->sq_nconsumers);
                }
            }
            else
SEND_DATA:
            {
                DataPumpSenderControl *sender   = (DataPumpSenderControl*)squeue->sender;
                do
                {
                    unfinish_tuplestore = 0;
                    nstores = 0;
                    for (i = 0; i < squeue->sq_nconsumers; i++)
                    {
                        ConsState *cstate         = &squeue->sq_consumers[i];
                        DataPumpNodeControl *node = &sender->nodes[i];

                        //LWLockAcquire(sqsync->sqs_consumer_sync[i].cs_lwlock, LW_EXCLUSIVE);

                        if (end_query_requested)
                        {
                            squeue->producer_done = true;
                            end_query_requested = false;

                            SpinLockAcquire(&squeue->lock);
                            squeue->nConsumer_done++;
                            SpinLockRelease(&squeue->lock);

                            if (g_DataPumpDebug)
                            {
                                elog(LOG, "SharedQueueFinish:squeue %s producer set query done", squeue->sq_key);
                            }
                        }

                        if (IsConsumerDisConnect(squeue->sq_key, cstate->cs_node))
                        {
                            LWLockAcquire(sqsync->sqs_consumer_sync[i].cs_lwlock, LW_EXCLUSIVE);
                            if (cstate->cs_status == CONSUMER_ACTIVE && cstate->cs_pid == 0)
                            {
                                cstate->cs_status = CONSUMER_DONE;

                                if (g_DataPumpDebug)
                                {
                                    elog(LOG, "SharedQueueFinish:squeue %s set consumer %d done", squeue->sq_key, i);
                                }
                            }
                            LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
                        }

                        if (tuplestore[i])
                        {
                            /* If the consumer is not reading just destroy the tuplestore */
                            if ((cstate->cs_status != CONSUMER_ACTIVE && cstate->cs_node != squeue->sq_nodeid) ||
                                (cstate->cs_node == squeue->sq_nodeid && squeue->producer_done) ||
                                (cstate->cs_done && cstate->send_fd))
                            {
                                tuplestore_end(tuplestore[i]);
                                tuplestore[i] = NULL;

                                if (g_DataPumpDebug)
                                {
                                    elog(LOG, "Squeue %s finish: consumer idx %d, nodeid %d does not need the data,"
                                              "destroy the tuplestore.", squeue->sq_key, i, cstate->cs_node);
                                }

                                /* consumer do not need more data, such as limit case */
                                if ((cstate->cs_done && cstate->send_fd) ||
                                    (cstate->cs_node == squeue->sq_nodeid && squeue->producer_done))
                                {
                                    while (node->buffer->m_Head != node->buffer->m_Tail)
                                    {
                                        DataPumpWakeupSender(squeue->sender, i);
                                        if(!DataPumpNodeCheck(squeue->sender, i))
                                        {
                                            //LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
                                            elog(ERROR, "SharedQueueFinish:node %d status abnormal.", i);
                                        }
                                        pg_usleep(50L);
                                    }
                                    
                                    if (cstate->cs_done && cstate->send_fd)
                                    {
                                        /* release fd */
                                        if (node->sock != NO_SOCKET)
                                        {
                                            close(node->sock);
                                            node->sock = NO_SOCKET;
                                        }
                                        cstate->cs_status = CONSUMER_EOF;
                                    }
                                }
                            }
                            else
                            {
                                nstores++;
                            
                                if (tmpslot == NULL)
                                {
                                    tmpslot = MakeSingleTupleTableSlot(tupDesc);
                                }
                                else
                                {
                                    ExecClearTuple(tmpslot);
                                }
                                tuplestore_done = DataPumpTupleStoreDump(squeue->sender, i, 
                                                                         cstate->cs_node,
                                                                         tmpslot, 
                                                                         tuplestore[i]);
                                                     
                                if (tuplestore_done)
                                {                
                                    while (node->buffer->m_Head != node->buffer->m_Tail)
                                    {
                                        DataPumpWakeupSender(squeue->sender, i);
                                        if(!DataPumpNodeCheck(squeue->sender, i))
                                        {
                                            //LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
                                            elog(ERROR, "SharedQueueFinish:node %d status abnormal.", i);
                                        }
                                        pg_usleep(50L);
                                    }
                                    
                                    nstores--;
                                    
                                    /* sanity check */
                                    if(node->ntuples_get != node->ntuples_put)
                                    {
                                        //LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
                                        elog(ERROR, "tuplestore with nodeid:%d, cursor %s ended abnormally, put_tuples:%zu, get_tuples:%zu", 
                                            cstate->cs_node, squeue->sq_key, node->ntuples_put, node->ntuples_get);
                                    }
                                    
                                    tuplestore_end(tuplestore[i]);
                                    tuplestore[i] = NULL;

                                    LWLockAcquire(sqsync->sqs_consumer_sync[i].cs_lwlock, LW_EXCLUSIVE);
                                    if (cstate->cs_status == CONSUMER_ACTIVE)
                                    {
                                        /* release fd */
                                        if (node->sock != NO_SOCKET)
                                        {
                                            close(node->sock);
                                            node->sock = NO_SOCKET;
                                        }
                                        cstate->cs_status = CONSUMER_EOF;

                                    }
                                    
                                    LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);

                                    {
                                        elog(DEBUG1, "ended tuplestore with nodeid:%d, cursor %s, put_tuples:%zu, get_tuples:%zu, send_tuples:%zu, nfast_send:%zu, sleep_count:%zu",
                                        cstate->cs_node, squeue->sq_key, node->ntuples_put, node->ntuples_get, node->ntuples, node->nfast_send, node->sleep_count);
                                    }
                                }
                                else
                                {
                                    if (node->status == DataPumpSndStatus_set_socket)
                                    {
                                        nstores--;
                                        unfinish_tuplestore++;

                                        if(g_DataPumpDebug)
                                        {
                                            elog(LOG, "unfinished tuplestore with nodeid:%d, cursor %s, put_tuples:%zu, get_tuples:%zu, nfast_send:%zu, sleep_count:%zu",
                                            cstate->cs_node, squeue->sq_key, node->ntuples_put, node->ntuples_get, node->nfast_send, node->sleep_count);
                                        }
                                    }
                                    else if(node->status == DataPumpSndStatus_incomplete_data ||
                                            node->status == DataPumpSndStatus_error)
                                    {
                                        //LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
                                        elog(ERROR, "node %d status abnormal!errmsg:%s.", i, strerror(node->errorno));
                                    }
                                }
                            }
                        }
                        else
                        {                            
                            while (node->buffer->m_Head != node->buffer->m_Tail)
                            {
                                DataPumpWakeupSender(squeue->sender, i);
                                if(!DataPumpNodeCheck(squeue->sender, i))
                                {
                                    //LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
                                    elog(ERROR, "SharedQueueFinish:node %d status abnormal.", i);
                                }
                                pg_usleep(50L);
                            }

                            LWLockAcquire(sqsync->sqs_consumer_sync[i].cs_lwlock, LW_EXCLUSIVE);
                            if (cstate->cs_status == CONSUMER_ACTIVE)
                            {
                                /* release fd */
                                if (node->sock != NO_SOCKET)
                                {
                                    close(node->sock);
                                    node->sock = NO_SOCKET;
                                }

                                if (cstate->cs_pid != 0)
                                {
                                    cstate->cs_status = CONSUMER_EOF;
                                }
                                else
                                {
                                    /* wait all consumers done */
                                    nstores++;
                                }
                            }
                            LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);

                            if (g_DataPumpDebug)
                            {
                                elog(DEBUG1, "squeue %s finish consumer %d pid %d", squeue->sq_key, i, cstate->cs_pid);
                            }
                        }

                        //LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
                    }

					/*
					 * Check sq_error status to avoid endless loop here
					 */
                    if (squeue->sq_error)
                    {
                        elog(ERROR, "SharedQueueFinish: shared_queue %s error because of query-cancel.", squeue->sq_key);
                    }

                    if (unfinish_tuplestore)
                    {
                        pg_usleep(1000L);
                    }
                }while(unfinish_tuplestore);
                
                if (nstores == 0)
                {
                    bool ret;
                    
                    ret = DataPumpWaitSenderDone(squeue->sender, false);

                    if (!ret)
                    {
                        elog(ERROR, "sender thread failed to exit.");
                    }

                    /* sanity check */
                    for (i = 0; i < squeue->sq_nconsumers; i++)
                    {
                        DataPumpNodeControl *node = &sender->nodes[i];

                        if (node->status == DataPumpSndStatus_done)
                        {
                            if(node->buffer->m_Head != node->buffer->m_Tail)
                            {
                                elog(ERROR, "node buffer corrupted.");
                            }
                        }

                        LWLockAcquire(sqsync->sqs_consumer_sync[i].cs_lwlock, LW_EXCLUSIVE);
                        SetLatch(&sqsync->sqs_consumer_sync[i].cs_latch);
                        LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
                    }
                    DestoryDataPumpSenderControl(squeue->sender, squeue->sq_nodeid);
                    squeue->sender = NULL;
                    squeue->sender_destroy = true;
                    share_sq = NULL;
                    RemoveDisConsumerHash(squeue->sq_key);

                    if (tmpslot)
                        ExecDropSingleTupleTableSlot(tmpslot);

                    if (g_DataPumpDebug)
                    {
                        elog(LOG, "SQueue %s, finishing the SQueue - producer node %d, "
                                    "pid %d, nconsumers %d", squeue->sq_key, squeue->sq_nodeid,
                                    squeue->sq_pid, squeue->sq_nconsumers);
                    }
                    return 0;
                }
                else
                {
#if 0
                    if (tmpslot)
                        ExecDropSingleTupleTableSlot(tmpslot);

                    return nstores;
#else
                    /* send all data to consumer until end */
                    pg_usleep(1000L);

                    send_times++;

                    if (send_times == timeout)
                    {
                        send_times = 0;

                        /* reset consumer in case of unexpected exception */
                        if (SharedQueueWaitOnProducerLatch(squeue, 10000L))
                        {
                            SharedQueueResetNotConnected(squeue);
                        }
                    }

                    goto SEND_DATA;
#endif
                }
                
            }
        }
    }
#endif

    for (i = 0; i < squeue->sq_nconsumers; i++)
    {
        ConsState *cstate = &squeue->sq_consumers[i];
        LWLockAcquire(sqsync->sqs_consumer_sync[i].cs_lwlock, LW_EXCLUSIVE);
#ifdef SQUEUE_STAT
        if (!squeue->stat_finish)
            elog(DEBUG1, "Finishing %s node %d, %ld writes and %ld reads so far, %ld buffer writes, %ld buffer reads, %ld tuples returned to buffer",
                 squeue->sq_key, cstate->cs_node, cstate->stat_writes, cstate->stat_reads, cstate->stat_buff_writes, cstate->stat_buff_reads, cstate->stat_buff_returns);
#endif
        elog(DEBUG1, "SQueue %s finishing, consumer at %d, consumer node %d, pid %d, "
                "status %d", squeue->sq_key, i,
                cstate->cs_node, cstate->cs_pid, cstate->cs_status);
        /*
         * if the tuplestore has data and consumer queue has space for some
         * try to push rows to the queue. We do not want to do that often
         * to avoid overhead of temp tuple slot allocation.
         */
        if (tuplestore[i])
        {
            /* If the consumer is not reading just destroy the tuplestore */
            if (cstate->cs_status != CONSUMER_ACTIVE)
            {
                tuplestore_end(tuplestore[i]);
                tuplestore[i] = NULL;
            }
            else
            {
                nstores++;
                /*
                 * Attempt to dump tuples from the store require tuple slot
                 * allocation, that is not a cheap operation, so proceed if
                 * target queue has enough space.
                 */
                if (QUEUE_FREE_SPACE(cstate) > cstate->cs_qlength / 2)
                {
                    if (tmpslot == NULL)
                        tmpslot = MakeSingleTupleTableSlot(tupDesc);
                    if (SharedQueueDump(squeue, i, tmpslot, tuplestore[i]))
                    {
                        tuplestore_end(tuplestore[i]);
                        tuplestore[i] = NULL;
                        cstate->cs_status = CONSUMER_EOF;
                        nstores--;
                    }
                    /* Consumer may be sleeping, wake it up */
                    SetLatch(&sqsync->sqs_consumer_sync[i].cs_latch);

                    /*
                     * XXX This can only be called by the producer. So no need
                     * to set producer latch.
                     */
                }
            }
        }
        else
        {
            /* it set eof if not yet set */
            if (cstate->cs_status == CONSUMER_ACTIVE)
            {
                cstate->cs_status = CONSUMER_EOF;
                SetLatch(&sqsync->sqs_consumer_sync[i].cs_latch);
                /*
                 * XXX This can only be called by the producer. So no need to
                 * set producer latch.
                 */
            }
        }
        LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
    }
    if (tmpslot)
        ExecDropSingleTupleTableSlot(tmpslot);

#ifdef SQUEUE_STAT
    squeue->stat_finish = true;
#endif

    return nstores;
}


/*
 * SharedQueueUnBind
 *    Cancel binding of current process to the shared queue. If the process
 * was a producer it should pass in the array of tuplestores where tuples were
 * queueed when it was unsafe to block. If any of the tuplestores holds data
 * rows they are written to the queue. The length of the array of the
 * tuplestores should be the same as the count of consumers. It is OK if some
 * entries are NULL. When a consumer unbinds from the shared queue it should
 * set the tuplestore parameter to NULL.
 */
void
SharedQueueUnBind(SharedQueue squeue, bool failed)
{// #lizard forgives
    SQueueSync *sqsync = squeue->sq_sync;
    int            wait_result = 0;
    int         i                = 0;
    int         consumer_running = 0;

    elog(DEBUG1, "SQueue %s, unbinding the SQueue (failed: %c) - producer node %d, "
            "pid %d, nconsumers %d", squeue->sq_key, failed ? 'T' : 'F',
            squeue->sq_nodeid, squeue->sq_pid, squeue->sq_nconsumers);

#ifdef __TBASE__
    if (!sqsync)
        return;
#endif

CHECK:
    /* loop while there are active consumers */
    for (;;)
    {
        int i;
        int c_count = 0;
        int unbound_count = 0;

        LWLockAcquire(sqsync->sqs_producer_lwlock, LW_EXCLUSIVE);
        /* check queue states */
        for (i = 0; i < squeue->sq_nconsumers; i++)
        {
            ConsState *cstate = &squeue->sq_consumers[i];
            LWLockAcquire(sqsync->sqs_consumer_sync[i].cs_lwlock, LW_EXCLUSIVE);

            elog(DEBUG1, "SQueue %s unbinding, check consumer at %d, consumer node %d, pid %d, "
                    "status %d", squeue->sq_key, i,
                    cstate->cs_node, cstate->cs_pid, cstate->cs_status);

            /* is consumer working yet ? */
            if (cstate->cs_status == CONSUMER_ACTIVE && failed)
            {
                elog(DEBUG1, "SQueue %s, consumer status CONSUMER_ACTIVE, but "
                        "the operation has failed - marking CONSUMER_ERROR",
                        squeue->sq_key);

                cstate->cs_status = CONSUMER_ERROR;
            }
            else if (cstate->cs_status != CONSUMER_DONE && !failed)
            {
                elog(DEBUG1, "SQueue %s, consumer not yet done, wake it up and "
                        "wait for it to finish reading", squeue->sq_key);
                c_count++;
                /* Wake up consumer if it is sleeping */
                SetLatch(&sqsync->sqs_consumer_sync[i].cs_latch);
                /* producer will continue waiting */
                ResetLatch(&sqsync->sqs_producer_latch);

                if (cstate->cs_pid == 0)
                    unbound_count++;
            }

            LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
        }

        LWLockRelease(sqsync->sqs_producer_lwlock);

        if (c_count == 0)
            break;

        if (g_DataPumpDebug)
        {
            elog(DEBUG1, "SQueue %s, wait while %d consumers finish, %d consumers"
                    "not yet bound", squeue->sq_key, c_count, unbound_count);
        }
        /* wait for a notification */
        wait_result = WaitLatch(&sqsync->sqs_producer_latch,
                                WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT,
                                10000L, WAIT_EVENT_MQ_INTERNAL);

        /*
         * If we hit a timeout, reset the consumers which still hasn't
         * connected. We already make an assumption that consumers that don't
         * connect in time, would never connect and drop those consumers.
         *
         * XXX Unfortunately, while this is not the best way to handle the
         * problem, we have not found a reliable way to tell whether a specific
         * consumer will ever connect or not. So this kludge at least avoids a
         * infinite hang.
         */
        if (wait_result & WL_TIMEOUT)
            SharedQueueResetNotConnected(squeue);
    }
#ifdef SQUEUE_STAT
    elog(DEBUG1, "Producer %s is done, there were %ld pauses", squeue->sq_key, squeue->stat_paused);
#endif
    if (g_DataPumpDebug)
    {
        elog(DEBUG1, "SQueue %s, producer node %d, pid %d - unbound successfully",
                squeue->sq_key, squeue->sq_nodeid, squeue->sq_pid);
    }

#ifdef __TBASE__
    if (g_UseDataPump)
    {
        SqueueProducerExit();
    }
#endif

    LWLockAcquire(SQueuesLock, LW_EXCLUSIVE);
    LWLockAcquire(sqsync->sqs_producer_lwlock, LW_EXCLUSIVE);

    /*
     * In rear situation, after consumers just bind to the shared queue, the producer timeout and remove the shared queue.
     * This will cause a SEGV in the consumer. So here recheck if there are some consumers binded to the queue, if so, we need to wait them to 
     * finish.
     */
    consumer_running = 0;
    for (i = 0; i < squeue->sq_nconsumers; i++)
    {
        ConsState *cstate = &squeue->sq_consumers[i];

        LWLockAcquire(sqsync->sqs_consumer_sync[i].cs_lwlock, LW_EXCLUSIVE);

        /* found a consumer running */
        if (CONSUMER_ACTIVE == cstate->cs_status && cstate->cs_pid != 0)
        {
            elog(DEBUG1, "SQueue %s, consumer node %d, pid %d, status %d, "
                    "started running after we finished unbind", squeue->sq_key,
                    cstate->cs_node, cstate->cs_pid, cstate->cs_status);
            consumer_running++;
        }

        LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
    }

    if (consumer_running)
    {
        elog(DEBUG1, "SQueue %s have %d consumers started running after we "
                "unbound, recheck now", squeue->sq_key, consumer_running);
        LWLockRelease(sqsync->sqs_producer_lwlock);
        LWLockRelease(SQueuesLock);
        goto CHECK;
    }

    /* All is done, clean up */
    DisownLatch(&sqsync->sqs_producer_latch);

#if 0
    if (--squeue->sq_refcnt == 0)
    {
        /* Now it is OK to remove hash table entry */
        squeue->sq_sync = NULL;
        sqsync->queue = NULL;
        if (hash_search(SharedQueues, squeue->sq_key, HASH_REMOVE, NULL) != squeue)
            elog(PANIC, "Shared queue data corruption");
    }
#endif

    LWLockRelease(sqsync->sqs_producer_lwlock);
    LWLockRelease(SQueuesLock);

#ifdef __TBASE__
    if (distributed_query_analyze)
    {
        DropQueryAnalyzeInfo(squeue->sq_key);
    }
#endif
}


/*
 * If queue with specified name still exists set mark respective consumer as
 * "Done". Due to executor optimization consumer may never connect the queue,
 * and should allow producer to finish it up if it is known the consumer will
 * never connect.
 */
void
SharedQueueRelease(const char *sqname)
{// #lizard forgives
    bool                    found;
    volatile SharedQueue     sq;

    LWLockAcquire(SQueuesLock, LW_EXCLUSIVE);

    sq = (SharedQueue) hash_search(SharedQueues, sqname, HASH_FIND, &found);
    if (found)
    {
        volatile SQueueSync    *sqsync = sq->sq_sync;
        int                        i;

        Assert(sqsync && sqsync->queue == sq);

        elog(DEBUG1, "SQueue %s producer node %d, pid %d  - requested to release",
                sqname, sq->sq_nodeid, sq->sq_pid);

        LWLockAcquire(sqsync->sqs_producer_lwlock, LW_EXCLUSIVE);

        /*
         * If the SharedQ is not bound, we can't just remove it because
         * somebody might have just created a fresh entry and is going to bind
         * to it soon. We assume that the future producer will eventually
         * release the SharedQ
         */
        if (sq->sq_nodeid == -1)
        {
            elog(DEBUG1, "SQueue %s, producer not bound ", sqname);
            LWLockRelease(sqsync->sqs_producer_lwlock);
            goto done;
        }

        /*
         * Do not bother releasing producer, all necessary work will be
         * done upon UnBind.
         */
        if (sq->sq_nodeid != PGXC_PARENT_NODE_ID)
        {
            elog(DEBUG1, "SQueue %s, we are consumer from node %d", sqname,
                    PGXC_PARENT_NODE_ID);
            /* find specified node in the consumer lists */
            for (i = 0; i < sq->sq_nconsumers; i++)
            {
                ConsState *cstate = &(sq->sq_consumers[i]);
                if (cstate->cs_node == PGXC_PARENT_NODE_ID)
                {
                    LWLockAcquire(sqsync->sqs_consumer_sync[i].cs_lwlock,
                                  LW_EXCLUSIVE);
                    elog(DEBUG1, "SQueue %s, consumer node %d, pid %d, "
                            "status %d",  sq->sq_key, cstate->cs_node,
                            cstate->cs_pid, cstate->cs_status);

                    /*
                     * If the consumer pid is not set, we are looking at a race
                     * condition where the old producer (which supplied the
                     * tuples to this remote datanode) may have finished and
                     * marked all consumers as CONSUMER_EOF, the consumers
                     * themeselves consumed all the tuples and marked
                     * themselves as CONSUMER_DONE. The old producer in that
                     * case may have actually removed the SharedQ from shared
                     * memory. But if a new execution for this same portal
                     * comes before the consumer sends a "Close Portal" message
                     * (which subsequently calls this function), we may end up
                     * corrupting state for the upcoming consumer for this new
                     * execution of the portal.
                     *
                     * It seems best to just ignore the release call in such
                     * cases.
                     */
                    if (cstate->cs_pid == 0)
                    {
                        elog(DEBUG1, "SQueue %s, consumer node %d, already released",
                            sq->sq_key, cstate->cs_node);
                    }
                    else if (cstate->cs_status != CONSUMER_DONE)
                    {
                        /* Inform producer the consumer have done the job */
                        cstate->cs_status = CONSUMER_DONE;
                        /* no need to receive notifications */
                        if (cstate->cs_pid > 0)
                        {
                            DisownLatch(&sqsync->sqs_consumer_sync[i].cs_latch);
                            cstate->cs_pid = 0;
                        }
                        /*
                         * notify the producer, it may be waiting while
                         * consumers are finishing
                         */
                        SetLatch(&sqsync->sqs_producer_latch);
                        elog(DEBUG1, "SQueue %s, release consumer at %d, node "
                                "%d, pid %d, status %d ", sqname, i,
                                cstate->cs_node, cstate->cs_pid,
                                cstate->cs_status);
                    }
                    LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
                    LWLockRelease(sqsync->sqs_producer_lwlock);
                    /* exit */
                    goto done;
                }
            }

            elog(DEBUG1, "SQueue %s, consumer from node %d never bound",
                    sqname, PGXC_PARENT_NODE_ID);
            /*
             * The consumer was never bound. Find empty consumer slot and
             * register node here to let producer know that the node will never
             * be consuming.
             */
            for (i = 0; i < sq->sq_nconsumers; i++)
            {
                ConsState *cstate = &(sq->sq_consumers[i]);
                if (cstate->cs_node == -1)
                {
                    LWLockAcquire(sqsync->sqs_consumer_sync[i].cs_lwlock,
                                  LW_EXCLUSIVE);
                    /* Inform producer the consumer have done the job */
                    cstate->cs_status = CONSUMER_DONE;
                    SetLatch(&sqsync->sqs_producer_latch);
                    elog(DEBUG1, "SQueue %s, consumer at %d marking as "
                            "CONSUMER_DONE", sqname, i);
                    LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
                }
            }
        }
        LWLockRelease(sqsync->sqs_producer_lwlock);
    }
done:
#ifdef __TBASE__
    if (sq && sq->sq_pid == MyProcPid && sq->sq_nodeid == PGXC_PARENT_NODE_ID)
    {
        if (sq->sq_sync)
        {
            LWLockAcquire(sq->sq_sync->sqs_producer_lwlock, LW_EXCLUSIVE);
            if (sq->sq_sync->sqs_producer_latch.owner_pid == MyProcPid)
            {
                DisownLatch(&sq->sq_sync->sqs_producer_latch);
            }
            LWLockRelease(sq->sq_sync->sqs_producer_lwlock);
        }

        RemoveDisConsumerHash(sq->sq_key);
    }

    if (sq && sq->with_params)
    {
        RemoveDisConsumerHash(sq->sq_key);
    }
#endif
    /*
     * If we are the last holder of the SQueue, remove it from the hash table
     * to avoid any leak
     */
    if (sq && --sq->sq_refcnt == 0)
    {
        /* Now it is OK to remove hash table entry */
        sq->sq_sync->queue = NULL;
        sq->sq_sync = NULL;
        if (hash_search(SharedQueues, sq->sq_key, HASH_REMOVE, NULL) != sq)
            elog(PANIC, "Shared queue data corruption");
    }
    LWLockRelease(SQueuesLock);
}


/*
 * Called when the backend is ending.
 */
void
SharedQueuesCleanup(int code, Datum arg)
{
    /* Need to be able to look into catalogs */
    CurrentResourceOwner = ResourceOwnerCreate(NULL, "SharedQueuesCleanup");

    /*
     * Release all registered prepared statements.
     * If a shared queue name is associated with the statement this queue will
     * be released.
     */
    DropAllPreparedStatements();

    /* Release everything */
    ResourceOwnerRelease(CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, true, true);
    ResourceOwnerRelease(CurrentResourceOwner, RESOURCE_RELEASE_LOCKS, true, true);
    ResourceOwnerRelease(CurrentResourceOwner, RESOURCE_RELEASE_AFTER_LOCKS, true, true);
    CurrentResourceOwner = NULL;
}


/*
 * sq_push_long_tuple
 *    Routine to push through the consumer state tuple longer the the consumer
 *    queue. Long tuple is written by a producer partially, and only when the
 *    consumer queue is empty.
 *    The consumer can determine that the tuple being read is long if the length
 *    of the tuple which is read before data is exceeding queue length.
 *       Consumers is switching to the long tuple mode and read in the portion of
 *      data which is already in the queue. After reading in each portion of data
 *    consumer sets cs_ntuples to LONG_TUPLE to indicate it is in long tuple
 *    mode, and writes out number of already read bytes to the beginning of the
 *    queue.
 *    While Consumer is reading in tuple data Producer may work on other task:
 *    execute query and send tuples to other Customers. If Producer sees the
 *    LONG_TUPLE indicator it may write out next portion. The tuple remains
 *    current in the tuplestore, and Producer just needs to read offset from
 *    the buffer to know what part of data to write next.
 *    After tuple is completely written the Producer is advancing to next tuple
 *    and continue operation in normal mode.
 */
static bool
sq_push_long_tuple(ConsState *cstate, RemoteDataRow datarow)
{
    if (cstate->cs_ntuples == 0)
    {
        /* the tuple is too big to fit the queue, start pushing it through */
        int len;
        /*
         * Output actual message size, to prepare consumer:
         * allocate memory and set up transmission.
         */
        QUEUE_WRITE(cstate, sizeof(int), (char *) &datarow->msglen);
        /* Output as much as possible */
        len = cstate->cs_qlength - sizeof(int);
        Assert(datarow->msglen > len);
        QUEUE_WRITE(cstate, len, datarow->msg);
        cstate->cs_ntuples = 1;
        return false;
    }
    else
    {
        int offset;
        int    len;

        /* Continue pushing through long tuple */
        Assert(cstate->cs_ntuples == LONG_TUPLE);
        /*
         * Consumer outputs number of bytes already read at the beginning of
         * the queue.
         */
        memcpy(&offset, cstate->cs_qstart, sizeof(int));

        Assert(offset > 0 && offset < datarow->msglen);

        /* remaining data */
        len = datarow->msglen - offset;
        /*
         * We are sending remaining lengs just for sanity check at the consumer
         * side
         */
        QUEUE_WRITE(cstate, sizeof(int), (char *) &len);
        if (len > cstate->cs_qlength - sizeof(int))
        {
            /* does not fit yet */
            len = cstate->cs_qlength - sizeof(int);
            QUEUE_WRITE(cstate, len, datarow->msg + offset);
            cstate->cs_ntuples = 1;
            return false;
        }
        else
        {
            /* now we are done */
            QUEUE_WRITE(cstate, len, datarow->msg + offset);
            cstate->cs_ntuples = 1;
            return true;
        }
    }
}


/*
 * sq_pull_long_tuple
 *    Read in from the queue data of a long tuple which does not the queue.
 *    See sq_push_long_tuple for more details
 *
 *    The function is entered with LWLocks held on the consumer as well as
 *    procuder sync. The function exits with both of those locks held, even
 *    though internally it may release those locks before going to sleep.
 */
static void
sq_pull_long_tuple(ConsState *cstate, RemoteDataRow datarow,
                               int consumerIdx, SQueueSync *sqsync)
{
    int offset = 0;
    int len = datarow->msglen;
    ConsumerSync *sync = &sqsync->sqs_consumer_sync[consumerIdx];

    for (;;)
    {
        /* determine how many bytes to read */
        if (len > cstate->cs_qlength - sizeof(int))
            len = cstate->cs_qlength - sizeof(int);

        /* read data */
        QUEUE_READ(cstate, len, datarow->msg + offset);

        /* remember how many we read already */
        offset += len;

        /* check if we are done */
        if (offset == datarow->msglen)
            return;

        /* need more, set up queue to accept data from the producer */
        Assert(cstate->cs_ntuples == 1); /* allow exactly one incomplete tuple */
        cstate->cs_ntuples = LONG_TUPLE; /* long tuple mode marker */
        /* Inform producer how many bytes we have already */
        memcpy(cstate->cs_qstart, &offset, sizeof(int));
        /* Release locks and wait until producer supply more data */
        while (cstate->cs_ntuples == LONG_TUPLE)
        {
            /*
             * First up wake the producer
             */
            SetLatch(&sqsync->sqs_producer_latch);

            /*
             * We must reset the consumer latch while holding the lock to
             * ensure the producer can't change the state in between.
             */
            ResetLatch(&sync->cs_latch);

            /*
             * Now release all locks before going into a wait state
             */
            LWLockRelease(sync->cs_lwlock);
            LWLockRelease(sqsync->sqs_producer_lwlock);

            /* Wait for notification about available info */
            WaitLatch(&sync->cs_latch, WL_LATCH_SET | WL_POSTMASTER_DEATH, -1,
                    WAIT_EVENT_MQ_INTERNAL);
            /* got the notification, restore lock and try again */
            LWLockAcquire(sqsync->sqs_producer_lwlock, LW_SHARED);
            LWLockAcquire(sync->cs_lwlock, LW_EXCLUSIVE);
        }
        /* Read length of remaining data */
        QUEUE_READ(cstate, sizeof(int), (char *) &len);

        /* Make sure we are doing the same tuple */
        Assert(offset + len == datarow->msglen);

        /* next iteration */
    }
}

#ifdef __TBASE__
void ThreadSemaInit(ThreadSema *sema, int32 init)
{
    if (sema)
    {
        sema->m_cnt = init;
        pthread_mutex_init(&sema->m_mutex, 0);
        pthread_cond_init(&sema->m_cond, 0);
    }
}

void ThreadSemaDown(ThreadSema *sema)
{            
    if (sema)
    {
        (void)pthread_mutex_lock(&sema->m_mutex);
#if 1
        if (--(sema->m_cnt) < 0) 
        {
            /* thread goes to sleep */

            (void)pthread_cond_wait(&sema->m_cond, &sema->m_mutex);
        }
#endif
        (void)pthread_mutex_unlock(&sema->m_mutex);
    }
}

void ThreadSemaUp(ThreadSema *sema)
{
    if (sema)
    {
        (void)pthread_mutex_lock(&sema->m_mutex);
#if 1
        if ((sema->m_cnt)++ < 0) 
        {
            /*wake up sleeping thread*/                
            (void)pthread_cond_signal(&sema->m_cond);
        }
#endif

        (void)pthread_mutex_unlock(&sema->m_mutex);
    }
}

void spinlock_init(pg_spin_lock *lock)
{    
    SpinLockInit(lock);
}

void spinlock_lock(pg_spin_lock *lock)
{
    if (lock)
    {
        SpinLockAcquire(lock); 
    }
}

void spinlock_unlock(pg_spin_lock *lock)
{
    if (lock)
    {
        SpinLockRelease(lock); 
    }
}


/*
 * The following funciton is used to handle lockless message queue.
 */
 
/* Get data pointer, use with the following functions. */
char *GetData(DataPumpBuf *buf, uint32 *uiLen)
{
    uint32 border = 0;
    uint32 tail = 0;
    char  *data;
    if (buf)
    {
        if (0 == DataSize(buf))
        {
            return NULL;
        }
        
        spinlock_lock(&(buf->pointerlock));
        border = buf->m_Border;
        tail   = buf->m_Tail;
        spinlock_unlock(&(buf->pointerlock));
        if (INVALID_BORDER == border)
        {
            *uiLen = 0;
            return NULL;
        }

        /* read from tail to border*/
        if (border >=  tail)
        { 
             /* Only sender increases m_Tail, no need to lock. */
             *uiLen = border - tail;  
             data = buf->m_buf + tail;    
             
             spinlock_lock(&(buf->pointerlock));
             /* No more data. */
             if (border == buf->m_Border)
              {                 
                buf->m_Border = INVALID_BORDER;
              }
             spinlock_unlock(&(buf->pointerlock));
             
        }
        else
        {  
            /* read from tail to end */
            *uiLen = buf->m_Length - tail;
            data = buf->m_buf + tail;
            buf->m_WrapAround = true;
        } 
        return data;
    }
    else
    {
        *uiLen = 0;
        return NULL;
    }
}

/* Increate data offset, used after finishing read data from queue. */
void IncDataOff(DataPumpBuf *buf, uint32 uiLen)
{
    if (buf)
    {
        spinlock_lock(&(buf->pointerlock));
        buf->m_Tail  =  (buf->m_Tail + uiLen)% buf->m_Length;                  
        spinlock_unlock(&(buf->pointerlock));
    }
}

/* Return total data size in buffer */
uint32 DataSize(DataPumpBuf *buf)
{
    uint32 border = 0;
    uint32 head   = 0;
    uint32 tail   = 0;
    uint32 size   = 0;
    if (buf)
    {
        spinlock_lock(&(buf->pointerlock));
        head   = buf->m_Head;
        tail   = buf->m_Tail;
        border = buf->m_Border;
        spinlock_unlock(&(buf->pointerlock));
        
        if (INVALID_BORDER == border)
        {
            return 0;
        }
        
        if (tail <= head)
        {    
            size = head - tail;
        }
        else
        {
            size = buf->m_Length - tail + head;
        }
        
        return size;
    }
    return 0;
}
/* Get the pointer to write and return the length to write. */
char *GetWriteOff(DataPumpBuf *buf, uint32 *uiLen)
{
    uint32 head = 0;
    uint32 tail = 0;
    char  *ptr  = NULL;
    if (0 == FreeSpace(buf))
    {
        return NULL;
    }
    
    if (buf)
    {
        spinlock_lock(&(buf->pointerlock));
        head = buf->m_Head;
        tail = buf->m_Tail;
        spinlock_unlock(&(buf->pointerlock));
        
        if (head >=  tail)
        { 
           /* tail is the beginning of the queue. */
           if (tail != 0)
           {
               
               *uiLen = buf->m_Length - head;                 
           }
           else
           {
               /* Reserved one byte as flag. */
               *uiLen = buf->m_Length - head - 1;                 
           }
        }
        else
        {               
           /* Reserved one byte as flag. */
           *uiLen = tail - head - 1;
        }        
        ptr = buf->m_buf + head;
        return ptr;
    }
    else
    {
        return NULL;
    }    
}

/* Used to increase the write pointer after write some data. */
void IncWriteOff(DataPumpBuf *buf, uint32 uiLen)
{
    if (buf)
    {
        spinlock_lock(&(buf->pointerlock));
           buf->m_Head  += uiLen;
           buf->m_Head  =  buf->m_Head % buf->m_Length;
        spinlock_unlock(&(buf->pointerlock));
    }
}

/* Reserve space in print buffer */
int ReserveSpace(DataPumpBuf *buf, uint32 len, uint32 *offset)
{
    /* not enough space avaliable, wait */
    if (FreeSpace(buf) < len)
    {
        return -1;
    }

    if (buf)
    {
        *offset    = buf->m_Head;
           buf->m_Head  =  (buf->m_Head  + len)% buf->m_Length;
    }
    return 0;        
}

uint32 BufferOffsetAdd(DataPumpBuf *buf, uint32 pointer, uint32 offset)
{

    if (buf)
    {    
           return (pointer + offset) % buf->m_Length;
    }
    return 0;        
}
/* No need to lock, reader never read the data before we set border. */
int ReturnSpace(DataPumpBuf *buf, uint32 offset)
{
    if (buf)
    {
        buf->m_Head = offset;
    }
    return 0;        
}

/* Fill data into reserved by ReserveSpace */
void FillReserveSpace(DataPumpBuf *buf, uint32 offset, char *p, uint32 len)
{
    uint32 bytes2end      = 0;
    uint32 bytesfrombegin = 0;   

    if (buf)
    {        
        bytes2end = buf->m_Length - offset;
        if (len <= bytes2end)
        {
            memcpy(buf->m_buf + offset, p, len);    
        }
        else
        {
            bytesfrombegin = len - bytes2end;                
            memcpy(buf->m_buf + offset, p, bytes2end);                    
            memcpy(buf->m_buf, (char*)p + bytes2end, bytesfrombegin);
        }    
    }
}

/* Return free space of the buffer. */
uint32 FreeSpace(DataPumpBuf *buf)
{
    uint32 head = 0;
    uint32 tail = 0;
    uint32 len  = 0;
    if (buf)
    {
        spinlock_lock(&(buf->pointerlock));
        head = buf->m_Head;
        tail = buf->m_Tail;
        spinlock_unlock(&(buf->pointerlock));

        if (tail <= head)
        {
            len = tail + buf->m_Length - head - 1;
        }
        else
        {
            len = tail - head - 1;
        }
        return len;
    }
    else
    {
        return 0;
    }
}

/* Set tuple end border of the buffer. */
void SetBorder(DataPumpBuf *buf)
{
    spinlock_lock(&(buf->pointerlock));
    buf->m_Border = buf->m_Head;
    spinlock_unlock(&(buf->pointerlock));
}
/* Send data into buffer */
void PutData(DataPumpBuf *buf, char *data, uint32 len)
{    
    char   *ptr;
    uint32  bufferLen;
    uint32  needLen;
    uint32  offset = 0;
    needLen = len;
    while (1)
    {            
        ptr = GetWriteOff(buf, &bufferLen);
        if (ptr)
        {
            if (bufferLen >= needLen)
            {
                memcpy(ptr, data + offset, needLen);
                IncWriteOff(buf, needLen);
                return;
            }
            else
            {
                memcpy(ptr, data + offset, bufferLen);
                IncWriteOff(buf, bufferLen);
                needLen -= bufferLen;
                offset  += bufferLen;
            }
        }
    }
}

int32 CreateThread(void *(*f) (void *), void *arg, int32 mode)
{

    pthread_attr_t attr;
    pthread_t      threadid;
    int            ret = 0;

    pthread_attr_init(&attr);
    switch (mode) 
    {
        case MT_THR_JOINABLE:
            {
                pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
                break;
            }
        case MT_THR_DETACHED:
            {
                pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
                break;
            }        
        default:
            {
                elog(ERROR, "invalid thread mode %d\n", mode);
            }
    }
    ret = pthread_create(&threadid, &attr, f, arg);
    return ret;
}

/* ignore all signals, leave signal for main thread to process */
static void ThreadSigmask(void)
{
    sigset_t  new_mask;
    sigemptyset(&new_mask);
    (void) sigaddset(&new_mask, SIGQUIT);
    (void) sigaddset(&new_mask, SIGALRM);
    (void) sigaddset(&new_mask, SIGHUP);
    (void) sigaddset(&new_mask, SIGINT);
    (void) sigaddset(&new_mask, SIGTERM);
    (void) sigaddset(&new_mask, SIGQUIT);
    (void) sigaddset(&new_mask, SIGPIPE);
    (void) sigaddset(&new_mask, SIGUSR1);
    (void) sigaddset(&new_mask, SIGUSR2);
    (void) sigaddset(&new_mask, SIGFPE);
    (void) sigaddset(&new_mask, SIGCHLD);
    
    /* ignore signals*/
    (void) pthread_sigmask(SIG_BLOCK, &new_mask, NULL);
}
/*
 * Build data pump buffer.
 */
DataPumpBuf *BuildDataPumpBuf(void)
{
    DataPumpBuf *buff = NULL;
    buff = (DataPumpBuf*)palloc0(sizeof(DataPumpBuf));
    buff->m_Length = g_SndThreadBufferSize * 1024;
    buff->m_buf = (char*)palloc0(buff->m_Length);

    spinlock_init(&(buff->pointerlock));
    buff->m_Head            = 0;
    buff->m_Tail            = 0;    
    buff->m_WrapAround     = 0;    
    buff->m_Border          = INVALID_BORDER;        
    return buff;
}
/*
 * Build data pump node control.
 */
void InitDataPumpNodeControl(int32 nodeindex, DataPumpNodeControl *control)
{
    control->nodeindex   = nodeindex;
    control->sock         = NO_SOCKET;
    control->status         = DataPumpSndStatus_no_socket;
    spinlock_init(&control->lock);
    control->buffer      = BuildDataPumpBuf();
    control->ntuples_get = 0;
    control->ntuples_put = 0;
}
/*
 * Build data pump thread control.
 */
void InitDataPumpThreadControl(DataPumpThreadControl *control, DataPumpNodeControl *nodes, int32 base, int32 end, int32 total)
{
    control->nodes               = nodes;
    control->node_base         = base;
    control->node_end          = end;
    control->node_num          = total;  

    control->thread_need_quit  = false;
    control->thread_running    = false;
    ThreadSemaInit(&control->send_sem, 0);
    ThreadSemaInit(&control->quit_sem, 0);
}
/*
 * Create data pump sender thread.
 */
bool CreateSenderThread(DataPumpSenderControl *sender)
{
    bool succeed = true;
    int  i       = 0;
    int  ret     = 0;
    
    for (i = 0; i < sender->thread_num; i++)
    {        
        ret = CreateThread(DataPumpSenderThread, (void *)&sender->thread_control[i], MT_THR_DETACHED);
        if (ret)
        {
            succeed = false;
            break;
        }
        /* Set running status for the thread. */
        sender->thread_control[i].thread_running = true;
    }

    if (succeed)
    {
        /* create convert thread */
        ThreadSemaInit(&sender->convert_control.quit_sem, 0);
        ret = CreateThread(ConvertThreadMain, (void *)sender, MT_THR_DETACHED);
        if(ret)
            succeed = false;
        else
            sender->convert_control.cstatus = ConvertRunning;
    }
    
    if (!succeed)
    {
        DataPumpCleanThread(sender);        
    }
    return succeed;
}
/*
 * Create data pump control and init the threads.
 */
DataPumpSender BuildDataPumpSenderControl(SharedQueue sq)
{
    bool      succeed = false;
    int       i    = 0;
    int32     base = 0;
    int32     step = 0;    
    int32     end  = 0;    
    DataPumpSenderControl *sender_control = NULL;

    sender_control = palloc0(sizeof(DataPumpSenderControl));
    sender_control->node_num = sq->sq_nconsumers;
    sender_control->nodes    = (DataPumpNodeControl*)palloc0(sizeof(DataPumpNodeControl) * sender_control->node_num);

    for(i = 0; i < sq->sq_nconsumers; i++)
    {
        ConsState  *cstate = &(sq->sq_consumers[i]);

        InitDataPumpNodeControl(cstate->cs_node, &sender_control->nodes[i]);
    }

    /* Use the minimal one as thread number. */
    sender_control->thread_num = g_SndThreadNum > sq->sq_nconsumers ? sq->sq_nconsumers : g_SndThreadNum;
    sender_control->thread_control = (DataPumpThreadControl*)palloc0(sizeof(DataPumpThreadControl) * sender_control->thread_num);

    base = 0;
    end  = 0;
    step = DIVIDE_UP(sender_control->node_num, sender_control->thread_num);
    for (i = 0; i < sender_control->thread_num; i++)
    {
        if (i < sender_control->thread_num  - 1)
        {
            base = end;
            end  = base + step;
        }
        else
        {
            base = end;
            end = sender_control->node_num;
        }
        InitDataPumpThreadControl(&sender_control->thread_control[i], sender_control->nodes, base, end, sender_control->node_num);
    }

    /* set sqname and max connection */
    snprintf(sender_control->convert_control.sqname, MAX_CURSOR_LEN, "%s", sq->sq_key);
    sender_control->convert_control.maxConn = sender_control->node_num;
    
    succeed = CreateSenderThread(sender_control);
    if (!succeed)
    {
        DestoryDataPumpSenderControl(sender_control, sq->sq_nodeid);
        sq->sender_destroy = true;
        sq->sender = NULL;
        elog(LOG, DATA_PUMP_PREFIX"start thread failed for %s", strerror(errno));
        return NULL;
    }
    
    sq->sender = sender_control;

    elog(DEBUG1, "Squeue:%s(Pid:%d), create %d sender, 1 convert.", sq->sq_key, MyProcPid, sender_control->thread_num);
    
    return sender_control;
}

void DestoryDataPumpSenderControl (void* sndctl, int nodeid)
{
    int  i         = 0;
    DataPumpSenderControl *sender   = (DataPumpSenderControl*)sndctl;

    if (!sndctl)
    {
        return;
    }

    if (sender->thread_control)
    {
        pfree(sender->thread_control);
        sender->thread_control = NULL;
    }

    if (sender->nodes)
    {
        for (i = 0; i < sender->node_num; i++)
        {        
            DestoryDataPumpBuf(sender->nodes[i].buffer);

            if (sender->nodes[i].sock != NO_SOCKET && sender->nodes[i].nodeindex != nodeid)
            {
                close(sender->nodes[i].sock);
                sender->nodes[i].sock = NO_SOCKET;
            }
        }
        pfree(sender->nodes);
        sender->nodes = NULL;

        pfree(sender);
    }
    
}

void DestoryDataPumpBuf(DataPumpBuf *buffer)
{
    pfree(buffer->m_buf);
}

void DataPumpSendLoop(DataPumpNodeControl  *nodes, DataPumpThreadControl* control)
{// #lizard forgives
    int32  stuck_nodes = 0;
    int32  reason      = 0;
    int32  status      = 0;
    int32  nodeindex   = 0;
    int    ret         = 0;
    uint32 len         = 0;
    char   *data       = NULL;
    
    /* Loop to send data. */
    do 
    {
        stuck_nodes = 0;
        for (nodeindex = control->node_base; nodeindex < control->node_end && nodeindex < control->node_num; nodeindex++)
        {
            spinlock_lock(&nodes[nodeindex].lock);
            status = nodes[nodeindex].status;
            spinlock_unlock(&nodes[nodeindex].lock);
            
            /* status is valid */
            if (status >= DataPumpSndStatus_set_socket && status  <= DataPumpSndStatus_data_sending)
            {
                do 
                {
                    data = GetData(nodes[nodeindex].buffer, &len);
                    if (data)
                    {
                        ret = DataPumpRawSendData(&nodes[nodeindex], nodes[nodeindex].sock, data, len, &reason);                
                        if (EOF == ret)
                        {
                            /* We got error. */
                            spinlock_lock(&nodes[nodeindex].lock);
                            nodes[nodeindex].status  = DataPumpSndStatus_error;
                            nodes[nodeindex].errorno = errno;
                            spinlock_unlock(&nodes[nodeindex].lock);
                            break;
                        }                    
                        
                        /* increase data offset */
                        IncDataOff(nodes[nodeindex].buffer, ret);    
                        
                        /* Socket got stuck. */
                        if (reason == EAGAIN || reason == EWOULDBLOCK)
                        {
                            len = DataSize(nodes[nodeindex].buffer);
                            if (len > 0)
                            {
                                /* Break sending to the node, switch to the next one. */
                                stuck_nodes++;
                                break;
                            }
                        }
                    }
                    else
                    {
                        /* No more complete tuples. */
                        break;
                    }
                    len = DataSize(nodes[nodeindex].buffer);

                    /* Get status. */
                    spinlock_lock(&nodes[nodeindex].lock);
                    status = nodes[nodeindex].status;
                    spinlock_unlock(&nodes[nodeindex].lock);
                }while ((status >= DataPumpSndStatus_set_socket && status  <= DataPumpSndStatus_data_sending) && len);
            }
        }
    }while(stuck_nodes);
}
/*
 * Ensure all data flush out when cursor is done.
 */
bool DataPumpFlushAllData(DataPumpNodeControl  *nodes, DataPumpThreadControl* control)
{// #lizard forgives
    int32  stuck_nodes = 0;
    int32  reason      = 0;
    int32  status    = 0;
    bool   succeed   = true;
    int32  nodeindex = 0;
    int    ret       = 0;
    uint32 len       = 0;
    char   *data     = NULL;
    
    /* Loop to send data. */
    do
    {
        stuck_nodes = 0;
        for (nodeindex = control->node_base; nodeindex < control->node_end && nodeindex < control->node_num; nodeindex++)
        {
            spinlock_lock(&nodes[nodeindex].lock);
            status = nodes[nodeindex].status;
            spinlock_unlock(&nodes[nodeindex].lock);
            
            /* status is valid */
            if (status >= DataPumpSndStatus_set_socket && status  <= DataPumpSndStatus_data_sending)
            {
                do 
                {
                    /* Data left in buffer, send them all. */
                    if (nodes[nodeindex].buffer->m_Tail != nodes[nodeindex].buffer->m_Head)
                    {
                        nodes[nodeindex].buffer->m_Border = nodes[nodeindex].buffer->m_Head;
                    }
                    
                    data = GetData(nodes[nodeindex].buffer, &len);
                    if (data)
                    {
                        ret = DataPumpRawSendData(&nodes[nodeindex], nodes[nodeindex].sock, data, len, &reason);                
                        if (EOF == ret)
                        {
                            /* We got error. */
                            spinlock_lock(&nodes[nodeindex].lock);
                            nodes[nodeindex].status  = DataPumpSndStatus_error;
                            nodes[nodeindex].errorno = errno;
                            spinlock_unlock(&nodes[nodeindex].lock);
                            succeed = false;
                            break;
                        }
                        
                        /* increase data offset */
                        IncDataOff(nodes[nodeindex].buffer, ret);

                        /* Socket got stuck. */
                        if (reason == EAGAIN || reason == EWOULDBLOCK)
                        {
                            len = DataSize(nodes[nodeindex].buffer);
                            if (len > 0)
                            {
                                /* Break sending to the node, switch to the next one. */
                                stuck_nodes++;
                                break;
                            }
                        }
                    }
                    else
                    {
                        /* No more complete tuples, it is vary wired situation to get into the branch. */
                        if (DataSize(nodes[nodeindex].buffer))
                        {
                            spinlock_lock(&nodes[nodeindex].lock);
                            nodes[nodeindex].status  = DataPumpSndStatus_incomplete_data;
                            nodes[nodeindex].errorno = errno;
                            spinlock_unlock(&nodes[nodeindex].lock);
                            succeed = false;
                        }    
                        else
                        {
                            /* Job done, set status. */
                            spinlock_lock(&nodes[nodeindex].lock);
                            nodes[nodeindex].status  = DataPumpSndStatus_done;
                            spinlock_unlock(&nodes[nodeindex].lock);
                        }
                        break;
                    }
                    
                    /* Get status. */
                    spinlock_lock(&nodes[nodeindex].lock);
                    status = nodes[nodeindex].status;
                    spinlock_unlock(&nodes[nodeindex].lock);
                }while ((status >= DataPumpSndStatus_set_socket && status  <= DataPumpSndStatus_data_sending));
            }
        }
    }while(stuck_nodes);
    return succeed;
}
/*
 * Datapump sender thread.
 */
void *DataPumpSenderThread(void *arg)
{
    DataPumpNodeControl  * nodes   = NULL;
    DataPumpThreadControl* thread  = NULL;

    thread = (DataPumpThreadControl*)arg;
    nodes  =  thread->nodes;
    ThreadSigmask();
    thread->thread_running = true;
    while (1)
    {
        /* Waiting for orders. */
        ThreadSemaDown(&thread->send_sem);

        /* error, quit directly */
        if (thread->error)
            break;
        /* We have been told to quit. */
        if (thread->thread_need_quit)
        {
            thread->quit_status = DataPumpFlushAllData(nodes, thread);
            break;
        }
        
        /* Loop to send data. */
        DataPumpSendLoop(nodes, thread);
        
    }
    thread->thread_running = false;
    /* Tell main thread quit is done. */
    ThreadSemaUp(&thread->quit_sem);
    return NULL;
}

/* check node status and report error if node is abnormal */
static bool DataPumpNodeCheck(void *sndctl, int32 nodeindex)
{
    DataPumpSenderControl *sender   = NULL;
    DataPumpNodeControl   *node     = NULL;

    sender = (DataPumpSenderControl*)sndctl;
    
    node = &sender->nodes[nodeindex];

    if (node->status == DataPumpSndStatus_incomplete_data ||
        node->status == DataPumpSndStatus_error)
    {
        elog(LOG, "node %d status abnormal.errmsg:%s.", nodeindex, strerror(node->errorno));
        return false;
    }

    if (sender->convert_control.cstatus != ConvertRunning)
    {
        elog(LOG, "convert status abnormal.errmsg:%s.", strerror(sender->convert_control.errNO));
        return false;
    }

    return true;
}

/* Return data write to the socket. */
static int DataPumpRawSendData(DataPumpNodeControl *node, int32 sock, char *data, int32 len, int32 *reason)
{
    int32  offset       = 0;
    int32  nbytes_write = 0;

    while (offset < len)
    {
        nbytes_write = send(sock, data + offset, len - offset, 0);
        if (nbytes_write <= 0)
        {
            if (errno == EINTR)
            {
                continue;        /* Ok if we were interrupted */
            }

            /*
             * Ok if no data writable without blocking, and the socket is in
             * non-blocking mode.
             */
            if (errno == EAGAIN ||
                errno == EWOULDBLOCK)
            {                
                pg_usleep(1000L);
                node->sleep_count++;
                *reason = errno;
                return offset;
            }
            *reason = errno;
            return EOF;
        }
        offset += nbytes_write;
    }
    
    return offset;
}

bool
DataPumpTupleStoreDump(void *sndctl, int32 nodeindex, int32 nodeId,
                                 TupleTableSlot *tmpslot, 
                                 Tuplestorestate *tuplestore)
{// #lizard forgives    
    bool                  sendall   = false;
    int32                   ret        = 0;    
    DataPumpNodeControl   *node     = NULL;
    DataPumpSenderControl *sender   = (DataPumpSenderControl*)sndctl;

    node = &sender->nodes[nodeindex];
    ret = DataPumpNodeReadyForSend(sndctl, nodeindex, nodeId);
    /* Can't send data now, append data to the tuplestore. */
    if (ret != DataPumpOK)
    {
        if (DataPumpSndError_unreachable_node == ret)
        {
            /* No need to send data, trade it as done. */
            elog(LOG, "DataPumpTupleStoreDump::remote node:%d never read data.", nodeId);
            
            return true;
        }
        else if (DataPumpConvert_error == ret ||
                 DataPumpSndError_node_error == ret ||
                 DataPumpSndError_io_error == ret)
        {
            elog(ERROR, "DataPumpTupleStoreDump::DataPump status:%d abnormal!", ret);

            return false;
        }
        
        /* Append the slot to the store... */
        if (!TupIsNull(tmpslot))
        {
            tuplestore_puttupleslot(tuplestore, tmpslot);

            node->ntuples_put++;
        }
        sendall = false;
    }
    else 
    {
        
        /* Read from tuplestore and send the tuple.*/
        /*
         * Tuplestore does not clear eof flag on the active read pointer, causing
         * the store is always in EOF state once reached when there is a single
         * read pointer. We do not want behavior like this and workaround by using
         * secondary read pointer. Primary read pointer (0) is active when we are
         * writing to the tuple store, also it is used to bookmark current position
         * when reading to be able to roll back and return just read tuple back to
         * the store if we failed to write it out to the queue.
         * Secondary read pointer is for reading, and its eof flag is cleared if a
         * tuple is written to the store.
         */
        tuplestore_select_read_pointer(tuplestore, 1);

        /* If we have something in the tuplestore try to push this to the queue */
        while (!tuplestore_ateof(tuplestore))
        {
            /* save position */
            tuplestore_copy_read_pointer(tuplestore, 1, 0);

            /* Try to get next tuple to the temporary slot */
            if (!tuplestore_gettupleslot(tuplestore, true, false, tmpslot))
            {
                /* false means the tuplestore in EOF state */    
                sendall = true;
                break;
            }

            node->ntuples_get++;
            
            /* The slot should contain a data row */
            Assert(tmpslot->tts_datarow);

            ret = DataPumpSendToNode(sndctl, tmpslot->tts_datarow->msg, tmpslot->tts_datarow->msglen, nodeindex);
            /* check if queue has enough room for the data */
            if (ret != DataPumpOK)
            {

                /* Restore read position to get same tuple next time */
                tuplestore_copy_read_pointer(tuplestore, 0, 1);

                /* We might advance the mark, try to truncate */
                tuplestore_trim(tuplestore);

                /* Prepare for writing, set proper read pointer */
                tuplestore_select_read_pointer(tuplestore, 0);
                
                sendall = false;

                node->ntuples_get--;
                break;
            }
            else
            {
                /* Big enough, send data. */
                if (DataSize(node->buffer) > g_SndBatchSize * 1024)
                {    
                    DataPumpWakeupSender(sndctl, nodeindex);
                }
            }
        }

        /* Handle the latest row. */
        if (!tuplestore_ateof(tuplestore))
        {
            sendall = false;
        }
        else
        {
            sendall = true;
        }
        /* Remove rows we have just read */
        tuplestore_trim(tuplestore);

        /* prepare for writes, set read pointer 0 as active */
        tuplestore_select_read_pointer(tuplestore, 0);
    }
    return sendall;
}
int32 DataPumpSendToNode(void *sndctl, char *data, size_t len, int32 nodeindex)
{// #lizard forgives
    bool                  long_tuple = false;
    int                   cursor     = 0;
    size_t                   tuple_len = 0;

    DataPumpSenderControl *sender   = (DataPumpSenderControl*)sndctl;
    DataPumpNodeControl *node = &sender->nodes[nodeindex];
        
    tuple_len = 1; /* msg type 'D' */
    if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3)
    {
        tuple_len += 4;
    }
    tuple_len += len;

    if (tuple_len >= node->buffer->m_Length - 1)
    {
        long_tuple = true;
    }
    
    if (FreeSpace(node->buffer) < (uint32)tuple_len)
    {
        if (!long_tuple)
        {
            DataPumpWakeupSender(sndctl, nodeindex);
            return DataPumpSndError_no_space;
        }
    }

    if (!long_tuple)
    {
        /* MsgType */
        PutData(node->buffer, "D", 1);

        /* Data length */
        if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3)
        {
            uint32        n32;

            n32 = htonl((uint32) (len + 4));
            PutData(node->buffer, (char*)&n32, 4);
        }
        /* Data */
        PutData(node->buffer, data, len);
        SetBorder(node->buffer);
    }
    else
    {
        uint32 header_len = 0;
        uint32 data_len = FreeSpace(node->buffer);

        header_len = 1;
        
        if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3)
        {
            header_len += 4;
        }
        
        /* put message 'D' */
        while (data_len < header_len)
        {
            DataPumpWakeupSender(sndctl, nodeindex);
            pg_usleep(50L);

            if (node->status == DataPumpSndStatus_error)
            {
                return DataPumpSndError_io_error;
            }
            
            data_len = FreeSpace(node->buffer);
        }

        PutData(node->buffer, "D", 1);

        /* Data length */
        if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3)
        {
            uint32        n32;

            n32 = htonl((uint32) (len + 4));
            PutData(node->buffer, (char*)&n32, 4);
        }

        cursor = 0;
        
        while (len)
        {
            data_len = FreeSpace(node->buffer);

            if (data_len)
            {
                int write_len = len > data_len ? data_len : len;
                
                PutData(node->buffer, data + cursor, write_len);

                cursor = cursor + write_len;

                len = len - write_len;
            }
            else
            {
                DataPumpWakeupSender(sndctl, nodeindex);
                pg_usleep(50L);

                if (node->status == DataPumpSndStatus_error)
                {
                    return DataPumpSndError_io_error;
                }
            }
        }
    }
    
    node->ntuples++;

    return DataPumpOK;
}
int32  DataPumpNodeReadyForSend(void *sndctl, int32 nodeindex, int32 nodeId)
{
    DataPumpNodeControl   *node     = NULL;
    DataPumpSenderControl *sender   = (DataPumpSenderControl*)sndctl;
    
    if (sender->convert_control.cstatus != ConvertRunning)
    {
        elog(LOG, "convert thread status:%d abnormal, maxconn:%d, conn_num:%d, errno:%d", 
                                                        sender->convert_control.cstatus, 
                                                        sender->convert_control.maxConn, 
                                                        sender->convert_control.connect_num, 
                                                        sender->convert_control.errNO);
        return DataPumpConvert_error;
    }
    
    node = &sender->nodes[nodeindex];
    if (node->nodeindex != nodeId)
    {
        return DataPumpSndError_node_error;
    }

    /* Use lock to check status and socket */
    spinlock_lock(&node->lock);
    if (NO_SOCKET == node->sock || DataPumpSndStatus_no_socket == node->status)
    {
        spinlock_unlock(&node->lock);
        return DataPumpSndError_no_socket;
    }
    
    if (DataPumpSndStatus_error == node->status)
    {
        spinlock_unlock(&node->lock);
        return DataPumpSndError_io_error;
    }
    spinlock_unlock(&node->lock);

    if (DATAPUMP_UNREACHABLE_NODE_FD == node->sock)
    {
        return DataPumpSndError_unreachable_node;
    }
    return DataPumpOK;
}
/*
 * Send DataRow to DataPumb.
 */
int32  DataPumpSendDataRow(void *sndctl, int32 nodeindex, int32 nodeId,  char *data, size_t len)
{
    int32                   ret        = 0;    
    DataPumpNodeControl   *node     = NULL;
    DataPumpSenderControl *sender   = (DataPumpSenderControl*)sndctl;

    
    ret = DataPumpNodeReadyForSend(sndctl, nodeindex, nodeId);
    if (ret != DataPumpOK)
    {
        return ret;
    }
    node = &sender->nodes[nodeindex];
    ret = DataPumpSendToNode(sndctl, data, len, nodeindex);
    if (ret != DataPumpOK)
    {
        return ret;
    }

    /* Big enough, send data. */
    if (DataSize(node->buffer) > g_SndBatchSize * 1024)
    {    
        DataPumpWakeupSender(sndctl, nodeindex);
    }
    return DataPumpOK;
}
/*
 * When job done, tell sender to finish.
 */
bool DataPumpWaitSenderDone(void *sndctl, bool error)
{// #lizard forgives
    bool                   succeed   = true;
    bool                  ret       = 0;
    int32                   threadid  = 0;
    int32                 nodeindex = 0;
    DataPumpNodeControl   *node     = NULL;
    DataPumpThreadControl *thread   = NULL;
    DataPumpSenderControl *sender   = (DataPumpSenderControl*)sndctl;
    bool                  *send_quit = NULL;

    if (!sndctl)
    {
        return true;
    }

    send_quit = (bool *)palloc0(sizeof(bool) * sender->thread_num);

    if (sender->thread_control)
    {
        for (threadid = 0; threadid < sender->thread_num; threadid ++)
        {
            thread = &sender->thread_control[threadid];
            /* Set quit flag and tell sender to quit. */
            if (thread->thread_running)
            {
                thread->thread_need_quit = true;

                thread->error = error;
                
                ThreadSemaUp(&thread->send_sem);    

                send_quit[threadid] = true;
            }
        }

        for (threadid = 0; threadid < sender->thread_num; threadid ++)
        {
            if (send_quit[threadid])
            {
                thread = &sender->thread_control[threadid];
                /* Wait for sender to quit. */        
                ThreadSemaDown(&thread->quit_sem);    
                if (!thread->quit_status)
                {
                    elog(DEBUG1, DATA_PUMP_PREFIX"thread:%d send data finish with error", threadid);    
                    for (nodeindex = thread->node_base; nodeindex < thread->node_end && nodeindex< thread->node_num; nodeindex++)
                    {
                        node = &thread->nodes[nodeindex];
                        if (node->status != DataPumpSndStatus_done)
                        {
                            elog(DEBUG1, DATA_PUMP_PREFIX"thread:%d node:%d remaining datasize:%u failed for %s, errno:%d", threadid, node->nodeindex, DataSize(node->buffer), strerror(node->errorno), node->errorno);
                        }
                    }
                    succeed = false;
                }

                if (g_DataPumpDebug)
                {
                    elog(DEBUG1, "Squeue %s: thread id %d done.", sender->convert_control.sqname, threadid);
                }
            }
        }
    }

    pfree(send_quit);
    
    /* tell convert to exit */
    ret = ConvertDone(&sender->convert_control);

    if (succeed)
        succeed = ret;

    elog(DEBUG1, "Squeue:%s(Pid:%d), destroy %d sender, 1 convert.", sender->convert_control.sqname, MyProcPid, sender->thread_num);
    
    return succeed;
}

/*
 * When create thread failed, tell other thread to quit.
 */
void DataPumpCleanThread(DataPumpSenderControl *sender)
{
    int32                   threadid  = 0;
    DataPumpThreadControl *thread   = NULL;
    bool                  *send_quit = (bool *)palloc0(sizeof(bool) * sender->thread_num);
    
    for (threadid = 0; threadid < sender->thread_num; threadid ++)
    {
        thread = &sender->thread_control[threadid];
        /* Set quit flag and tell sender to quit. */
        if (thread->thread_running)
        {
            thread->thread_need_quit = true;
            ThreadSemaUp(&thread->send_sem);        
            send_quit[threadid] = true;
        }
    }

    for (threadid = 0; threadid < sender->thread_num; threadid ++)
    {
	    if (send_quit[threadid])
        {
        thread = &sender->thread_control[threadid];
		/* Wait for sender to quit. */	
			ThreadSemaDown(&thread->quit_sem);
		}
    }
    pfree(send_quit);

    ConvertDone(&sender->convert_control);    
}

/*
 * Set node socket.
 */
int32  DataPumpSetNodeSocket(void *sndctl, int32 nodeindex, int32 nodeId,  int32 socket)
{
    DataPumpNodeControl   *node     = NULL;
    DataPumpSenderControl *sender   = (DataPumpSenderControl*)sndctl;
    if (nodeindex >= sender->node_num)
    {
        return DataPumpSndError_node_error;
    }


    node = &sender->nodes[nodeindex];
    if (node->nodeindex != nodeId)
    {
        return DataPumpSndError_node_error;
    }

    /* Use lock to check status and socket */
    socket_set_nonblocking(socket, true);
    spinlock_lock(&node->lock);
    if (NO_SOCKET == node->sock && DataPumpSndStatus_no_socket == node->status)
    {
        node->sock   = socket;
        node->status = DataPumpSndStatus_set_socket;
        spinlock_unlock(&node->lock);
        return DataPumpOK;
    }
    else
    {
        spinlock_unlock(&node->lock);
        return DataPumpSndError_bad_status;
    }
}

/* make connection to convert */
static int
convert_connect(char *sqname)
{
    int            fd,
                len;
    char sock_path[MAXPGPATH];
    struct sockaddr_un unix_addr;

#ifdef HAVE_UNIX_SOCKETS

    snprintf(sock_path, MAXPGPATH, "%s/%s", DATA_PUMP_SOCKET_DIR, sqname);

    /* create a Unix domain stream socket */
    if ((fd = socket(AF_UNIX, SOCK_STREAM, 0)) < 0)
    {
        if (errno == EMFILE)
        {
            ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("make socket to producer:%s failed for %s", sock_path, strerror(errno))));
        }
        ereport(DEBUG1,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("make socket to producer:%s failed for %s", sock_path, strerror(errno))));

        return -1;
    }
    
    memset(&unix_addr, 0, sizeof(unix_addr));
    unix_addr.sun_family = AF_UNIX;
    strcpy(unix_addr.sun_path, sock_path);
    len = sizeof(unix_addr.sun_family) +
        strlen(unix_addr.sun_path) + 1;

    if (connect(fd, (struct sockaddr *) & unix_addr, len) < 0)
    {
        ereport(DEBUG1,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("connect to producer:%s failed for %s, pid:%d", sock_path, strerror(errno), MyProcPid)));
        close(fd);
        return -1;
    }

    return fd;
#else
    /* TODO support for non-unix platform */
    ereport(FATAL,
            (errcode(ERRCODE_INTERNAL_ERROR),
             errmsg("only supports UNIX socket")));
    return -1;
#endif
}

static int
convert_listen(char *sock_path, int maxconn)
{
    int            fd,
                len;
    struct sockaddr_un unix_addr;

#ifdef HAVE_UNIX_SOCKETS

    /* create a Unix domain stream socket */
    if ((fd = socket(AF_UNIX, SOCK_STREAM, 0)) < 0)
        return -1;

    /* fill in socket address structure */
    memset(&unix_addr, 0, sizeof(unix_addr));
    unix_addr.sun_family = AF_UNIX;
    strcpy(unix_addr.sun_path, sock_path);
    len = sizeof(unix_addr.sun_family) +
        strlen(unix_addr.sun_path) + 1;

    /* bind the name to the descriptor */
    if (bind(fd, (struct sockaddr *) & unix_addr, len) < 0)
    {
        close(fd);
        return -1;
    }

    /* tell kernel we're a server */
    if (listen(fd, maxconn) < 0)
    {
        close(fd);
        return -1;
    }

    return fd;
#else
    return -1;
#endif
}
#define SEND_MSG_BUFFER_SIZE 9
/*
 * Build up a message carrying file descriptors or process numbers and send them over specified
 * connection
 */
static int
convert_sendfds(int fd, int *fds_to_send, int count, int *err)
{
    struct iovec iov[1];
    struct msghdr msg = {0};
    char   buf[SEND_MSG_BUFFER_SIZE];
    uint32 n32;
    struct cmsghdr *cmptr = NULL;
    char fd_buf[CMSG_SPACE(sizeof(int) * count)];

    buf[0] = 'f';
    n32 = htonl((uint32) 8);
    memcpy(buf + 1, &n32, 4);
    n32 = htonl((uint32) count);
    memcpy(buf + 5, &n32, 4);

    iov[0].iov_base = buf;
    iov[0].iov_len = SEND_MSG_BUFFER_SIZE;
    msg.msg_iov = iov;
    msg.msg_iovlen = 1;
    msg.msg_name = NULL;
    msg.msg_namelen = 0;
    if (count == 0)
    {
        msg.msg_control = NULL;
        msg.msg_controllen = 0;
    }
    else
    {
        int *fdptr;

        msg.msg_control = fd_buf;
        msg.msg_controllen = sizeof(fd_buf);
        cmptr = CMSG_FIRSTHDR(&msg);
        cmptr->cmsg_level = SOL_SOCKET;
        cmptr->cmsg_type = SCM_RIGHTS;
        cmptr->cmsg_len = CMSG_LEN(sizeof(int) * count);

        fdptr = (int *) CMSG_DATA(cmptr);

        /* the fd to pass */
        memcpy(fdptr, fds_to_send, sizeof(int) * count);
        //memcpy(CMSG_DATA(CMSG_FIRSTHDR(&msg)), fds_to_send, count * sizeof(int));
    }

    if (sendmsg(fd, &msg, 0) != SEND_MSG_BUFFER_SIZE)
    {
        *err = errno;
        /* 
          * if errno is broken pipe or connection reset by peer, the producer may have finished
          * work, and we do not need to send fd any more.
          */
        if (*err == EPIPE || *err == ECONNRESET)
            elog(LOG, "convert_sendfds sendmsg failed for:%s.", strerror(*err));
        else
            elog(ERROR, "convert_sendfds sendmsg failed for:%s.", strerror(*err));
        return EOF;
    }
    return 0;
}

/*
 * Read a message from the specified connection carrying file descriptors
 */
static int
convert_recvfds(int fd, int *fds, int count)
{// #lizard forgives
    int            r;
    uint        n32;
    char        buf[SEND_MSG_BUFFER_SIZE];
    struct iovec iov[1];
    struct msghdr msg = {0};
    struct cmsghdr * cmsg;
    char fd_buf[CMSG_SPACE(sizeof(int) * count)];

    iov[0].iov_base = buf;
    iov[0].iov_len = SEND_MSG_BUFFER_SIZE;
    msg.msg_iov = iov;
    msg.msg_iovlen = 1;
    msg.msg_name = NULL;
    msg.msg_namelen = 0;
    msg.msg_control = fd_buf;
    msg.msg_controllen = sizeof(fd_buf);

    r = recvmsg(fd, &msg, 0);
    if (r < 0)
        goto failure;
    else if (r == 0)
        goto failure;
    else if (r != SEND_MSG_BUFFER_SIZE)
        goto failure;

    /* Verify response */
    if (buf[0] != 'f')
        goto failure;

    memcpy(&n32, buf + 1, 4);
    n32 = ntohl(n32);
    if (n32 != 8)
        goto failure;

    /*
     * If connection count is 0 it means pool does not have connections
     * to  fulfill request. Otherwise number of returned connections
     * should be equal to requested count. If it not the case consider this
     * a protocol violation. (Probably connection went out of sync)
     */
    memcpy(&n32, buf + 5, 4);
    n32 = ntohl(n32);
    if (n32 == 0)
        goto failure;

    if (n32 != count)
        goto failure;

    cmsg = CMSG_FIRSTHDR(&msg);

    if (!cmsg)
        goto failure;

    memcpy(fds, CMSG_DATA(CMSG_FIRSTHDR(&msg)), count * sizeof(int));
    return 0;
failure:
    return EOF;
}

/* send nodeid and consumerIdx to convert */
static bool
send_fd_with_nodeid(char *sqname, int nodeid, int consumerIdx)
{// #lizard forgives
    int fd;
    int n32;
    int ret;
    int err;
    
    fd = convert_connect(sqname);
    if (fd < 0)
    {
        ereport(LOG,
                (errmsg("could not connect to convert with cursor \"%s\", pid:%d",
                        sqname, MyProcPid)));
        return false;
    }

    /* send nodeid */
    n32 = htonl(nodeid);
    ret = send(fd, (char *)&n32, 4, 0);

    if(ret != 4)
    {
        err = errno;
        
        close(fd);

        if (err == EPIPE || err == ECONNRESET)
        {
            /* producer may have finished work, and we do not need to send anything. */
            elog(LOG, "could not send nodeid to convert, errmsg:%s; producer may have finished work.", strerror(err));
            return false;
        }
        else
            elog(ERROR, "could not send nodeid to convert, errmsg:%s.", strerror(err));
    }

    /* send consumer index  */
    n32 = htonl(consumerIdx);
    ret = send(fd, (char *)&n32, 4, 0);
    if(ret != 4)
    {
        err = errno;
        
        close(fd);

        if (err == EPIPE || err == ECONNRESET)
        {
            /* producer may have finished work, and we do not need to send anything. */
            elog(LOG, "could not send consumerIdx to convert, errmsg:%s; producer may have finished work.", strerror(err));
            return false;
        }
        else
            elog(ERROR, "could not send consumerIdx to convert, errmsg:%s.", strerror(err));
    }

    /* send fd */
    if(convert_sendfds(fd, (int *)&MyProcPort->sock, 1, &err) != 0)
    {
        close(fd);

        if (err == EPIPE || err == ECONNRESET)
        {
            /* producer may have finished work, and we do not need to send anything. */
            elog(LOG, "could not send sockfd to convert, errmsg:%s; producer may have finished work.", strerror(err));
            return false;
        }
        else
            elog(ERROR, "could not send sockfd to convert, errmsg:%s.", strerror(err));
    }

    close(fd);

    return true;
}

static void *
ConvertThreadMain(void *arg)
{// #lizard forgives
#define SOCK_PATH_LEN 128
    bool exit_flag = false;
    int nodeid;
    int consumerIdx;
    int sockfd;
    int listen_fd;
    int con_fd;
    int ret;
    DataPumpSenderControl *control = NULL;
    char sock_path[SOCK_PATH_LEN];

    ThreadSigmask();

    control = (DataPumpSenderControl *)arg;
    control->convert_control.begin_stamp = GetCurrentTimestamp();
    
    snprintf(sock_path, SOCK_PATH_LEN, "%s/%s", DATA_PUMP_SOCKET_DIR, control->convert_control.sqname);
    
    listen_fd = convert_listen(sock_path, control->convert_control.maxConn);
    if(listen_fd < 0)
    {
        control->convert_control.errNO = errno;
        control->convert_control.cstatus = ConvertListenError;
        return NULL;
    }    

    while(true)
    {
#if 0
        if (control->convert_control.connect_num == (control->convert_control.maxConn - 1))
        {
            control->convert_control.finish_stamp = GetCurrentTimestamp();
        }
        
        if (exit_flag && control->convert_control.connect_num == control->convert_control.maxConn)
        {
            close(listen_fd);
            control->convert_control.cstatus = ConvertExit;
            unlink(sock_path);
            break;
        }
#endif
        if (exit_flag)
        {
            control->convert_control.finish_stamp = GetCurrentTimestamp();
            close(listen_fd);
            control->convert_control.cstatus = ConvertExit;
            unlink(sock_path);
            break;
        }
        
        con_fd = accept(listen_fd, NULL, NULL);
        if (con_fd < 0)
        {
            close(listen_fd);
            control->convert_control.errNO = errno;
            control->convert_control.cstatus = ConvertAcceptError;
            unlink(sock_path);
            break;
        }

        control->convert_control.connect_num++;
        
        /* recv nodeid */
        ret = recv(con_fd, (char *)&nodeid, 4, 0);
        if(ret != 4)
        {
            close(con_fd);
            close(listen_fd);
            control->convert_control.errNO = errno;
            control->convert_control.cstatus = ConvertRecvNodeidError;
            unlink(sock_path);
            break;
        }
        
        nodeid = ntohl(nodeid);

        if(nodeid == -1)
        {
            exit_flag = true;
            close(con_fd);
            continue;
        }

        /* recv consumer index  */
        ret = recv(con_fd, (char *)&consumerIdx, 4, 0);
        if(ret != 4)
        {
            close(con_fd);
            close(listen_fd);
            control->convert_control.errNO = errno;
            control->convert_control.cstatus = ConvertRecvNodeindexError;
            unlink(sock_path);
            break;
        }

        consumerIdx = ntohl(consumerIdx);

        /* recv fd */
        if(convert_recvfds(con_fd, (int *)&sockfd, 1) != 0)
        {
            close(con_fd);
            close(listen_fd);
            control->convert_control.errNO   = errno;
            control->convert_control.cstatus = ConvertRecvSockfdError;
            unlink(sock_path);
            break;
        }

        /* store nodeid, consumerIdx, sockfd */
        if(DataPumpSetNodeSocket(arg, consumerIdx, nodeid, sockfd) != DataPumpOK)
        {
            close(con_fd);
            close(listen_fd);
            control->convert_control.cstatus = ConvertSetSockfdError;
            unlink(sock_path);
            break;
        }

        close(con_fd);
    }

    ThreadSemaUp(&control->convert_control.quit_sem);
    return NULL;
}

static bool
ConvertDone(ConvertControl *convert)
{// #lizard forgives
#define MAX_RETRY 60000
    bool succeed = true;
    int fd;
    int ret;
    int nodeid = -1;
    int retry = 0;

    if (convert->cstatus != ConvertRunning)
    {
        return succeed;
    }

RETRY:

    fd = convert_connect(convert->sqname);
    
    if(fd < 0)
    {
        retry++;

        if (retry < MAX_RETRY)
        {
            pg_usleep(1000L);

            if (convert->cstatus != ConvertRunning)
            {
                succeed = true;

                return succeed;
            }
            goto RETRY;
        }
        elog(DEBUG1, "producer failed to connect convert for %d times, errmsg:%s", retry, strerror(errno));

        if (convert->cstatus != ConvertRunning)
        {
            succeed = true;
        }
        else
        {
            succeed = false;
        }
        return succeed;
    }

        /* send nodeid */
    nodeid = htonl(nodeid);
    ret = send(fd, (char *)&nodeid, 4, 0);
    if(ret != 4)
    {
        elog(DEBUG1, "producer failed to send nodeid to convert, errmsg:%s.", strerror(errno));
        
        if (convert->cstatus != ConvertRunning)
        {
            succeed = true;
        }
        else
        {
            succeed = false;
        }

        close(fd);

        return succeed;
    }
    close(fd);
    ThreadSemaDown(&convert->quit_sem);
    if (succeed)
    {
        elog(DEBUG1, "Squeue:%s establish connection cost %ld us", convert->sqname, convert->finish_stamp - convert->begin_stamp);
    }
    return succeed;
}

void
SendDataRemote(SharedQueue squeue, int32 consumerIdx, TupleTableSlot *slot, Tuplestorestate **tuplestore, MemoryContext tmpcxt)
{// #lizard forgives    
    bool fast_send_done = false;
    bool send_all       = false;
    bool free_datarow   = false;
    int  nodeid = 0;
    int  ret    = 0;
    ConsState  *cstate = &(squeue->sq_consumers[consumerIdx]);
    RemoteDataRow datarow = NULL;
    DataPumpSenderControl *sender   = (DataPumpSenderControl*)squeue->sender;
    DataPumpNodeControl   *node     = &sender->nodes[consumerIdx];

#if 1
    if (squeue->sq_error)
    {
        elog(LOG, "SendDataRemote: shared_queue %s error.", squeue->sq_key);
        goto ERR;
    }
#endif

    if (end_query_requested)
    {
        squeue->producer_done = true;
        end_query_requested = false;

        SpinLockAcquire(&squeue->lock);
        squeue->nConsumer_done++;
        SpinLockRelease(&squeue->lock);

        if (g_DataPumpDebug)
        {
            elog(LOG, "SendDataRemote:squeue %s producer %d set query done", squeue->sq_key, MyProcPid);
        }
    }

    /* all consumers done, do not produce data anymore */
    if (squeue->nConsumer_done == squeue->sq_nconsumers)
    {
        Executor_done = true;

        if (g_DataPumpDebug)
        {
            elog(LOG, "SendDataRemote:squeue %s producer %d all consumers done.", squeue->sq_key, MyProcPid);
        }
        
        return;
    }

    /* consumer do not need data anymore */
    if ((cstate->cs_pid == squeue->sq_pid && squeue->producer_done) ||
        (cstate->cs_pid != squeue->sq_pid && cstate->cs_done))
    {
        return;
    }

    /* if consumer do not need data anymore*/

    nodeid = cstate->cs_node;
    ret = DataPumpNodeReadyForSend(squeue->sender, consumerIdx, nodeid);
    if (DataPumpOK == ret)
    {
        /* No tuplestore created, we send datarow directly. */
        if (NULL == *tuplestore)
        {
            /* Get datarow from the tuple slot */
            if (slot->tts_datarow)
            {
                datarow = slot->tts_datarow;
                free_datarow = false;
            }
            else
            {
                /* Try fast send. */
                fast_send_done = ExecFastSendDatarow(slot, sender, consumerIdx, tmpcxt);

                if (fast_send_done)
                {
                    return;
                }        
                in_data_pump = true;
                datarow = ExecCopySlotDatarow(slot, tmpcxt);
                in_data_pump = false;
                free_datarow = true;
            }
            ret = DataPumpSendDataRow(squeue->sender, consumerIdx, nodeid, datarow->msg, datarow->msglen);    
            if(DataPumpOK == ret)
            {
                if(free_datarow)
                {
                    pfree(datarow);
                    datarow = NULL;
                }
                return;
            }
            else if (DataPumpConvert_error == ret ||
                     DataPumpSndError_node_error == ret ||
                     DataPumpSndError_io_error == ret)
            {
                if(free_datarow)
                {
                    pfree(datarow);
                    datarow = NULL;
                }
                goto ERR;
            }
            else if (DataPumpSndError_unreachable_node == ret)
            {
                /* consumer never need data */
                if(free_datarow)
                {
                    pfree(datarow);
                    datarow = NULL;
                }
                
                return;
            }
            
            if(free_datarow)
            {
                pfree(datarow);
                datarow = NULL;
            }
            /* DataPumpSndError_no_socket or DataPumpSndError_no_space */
            if(g_DataPumpDebug)
            {
                elog(LOG, "DataPump status:%d, need to put slot into tuplestore.", ret);
            }
        }
        else
        {
            /* Empty tuple store, send the data. */
            if (tuplestore_ateof(*tuplestore))
            {
                /* Get datarow from the tuple slot */
                if (slot->tts_datarow)
                {
                    datarow = slot->tts_datarow;
                    free_datarow = false;
                }
                else
                {
                    /* Try fast send. */
                    fast_send_done = ExecFastSendDatarow(slot, sender, consumerIdx, tmpcxt);
                    if (fast_send_done)
                    {
                        return;
                    }        

                    in_data_pump = true;
                    datarow = ExecCopySlotDatarow(slot, tmpcxt);
                    in_data_pump = false;
                    free_datarow = true;
                }
                
                ret = DataPumpSendDataRow(squeue->sender, consumerIdx, nodeid, datarow->msg, datarow->msglen);    
                if(DataPumpOK == ret)
                {
                    if(free_datarow)
                    {
                        pfree(datarow);
                        datarow = NULL;
                    }
                    return;
                }
                else if (DataPumpConvert_error == ret ||
                         DataPumpSndError_node_error == ret ||
                         DataPumpSndError_io_error == ret)
                {
                    if(free_datarow)
                    {
                        pfree(datarow);
                        datarow = NULL;
                    }
                    goto ERR;
                }
                else if (DataPumpSndError_unreachable_node == ret)
                {
                    /* consumer never need data */
                    if(free_datarow)
                    {
                        pfree(datarow);
                        datarow = NULL;
                    }
                    return;
                }
                
                if(free_datarow)
                {
                    pfree(datarow);
                    datarow = NULL;
                }
                /* DataPumpSndError_no_socket or DataPumpSndError_no_space */
                if(g_DataPumpDebug)
                {
                    elog(LOG, "DataPump status:%d, need to put slot into tuplestore.", ret);
                }
            }
            else
            {
                /*Build the temp slot. We will not DROP this slot when finish the cursor. We need upper level to reset the MCTX. */
                if (NULL == squeue->sender->temp_slot)
                {
                    squeue->sender->temp_slot = MakeSingleTupleTableSlot(slot->tts_tupleDescriptor);
                }
                else
                {
                    ExecClearTuple(squeue->sender->temp_slot);
                }
                        
                
                /* We got a tuplestore, dump it first. */
                send_all = DataPumpTupleStoreDump(squeue->sender, 
                                                   consumerIdx, 
                                                   nodeid,
                                                      squeue->sender->temp_slot, 
                                                   *tuplestore);
                /* Send the tuple if the tuplestore is empty. */
                if (send_all)
                {                    
                    if (!TupIsNull(slot))
                    {
                        /* Get datarow from the tuple slot */
                        if (slot->tts_datarow)
                        {
                            datarow = slot->tts_datarow;
                            free_datarow = false;
                        }
                        else
                        {
                            /* Try fast send. */
                            fast_send_done = ExecFastSendDatarow(slot, sender, consumerIdx, tmpcxt);
                            if (fast_send_done)
                            {
                                return;
                            }        

                            in_data_pump = true;
                            datarow = ExecCopySlotDatarow(slot, tmpcxt);
                            in_data_pump = false;
                            free_datarow = true;
                        }
                        
                        ret = DataPumpSendDataRow(squeue->sender, consumerIdx, nodeid, datarow->msg, datarow->msglen);
                        if (DataPumpOK == ret)
                        {
                            if(free_datarow)
                            {
                                pfree(datarow);
                                datarow = NULL;
                            }
                            return;
                        }

                        if(free_datarow)
                        {
                            pfree(datarow);
                            datarow = NULL;
                        }
                        /* Failure!! Put tuple to tuplestore. */
                    }
                    else
                    {
                        return;
                    }
                }
            }
        }
    }
    else if (DataPumpSndError_unreachable_node == ret)
    {
        /* Remote node unreachable, just drop the tuple. */
        elog(LOG, "SendDataRemote::remote node:%d never read data.", nodeid);
        
        return;
    }
    else if (DataPumpConvert_error == ret ||
             DataPumpSndError_node_error == ret ||
             DataPumpSndError_io_error == ret)
    {
        goto ERR;
    }

    /* Error ocurred or the node is not ready at present,  store the tuple into the tuplestore. */
    /* Create tuplestore if does not exist.*/
    if (NULL == *tuplestore)
    {
        int            ptrno PG_USED_FOR_ASSERTS_ONLY;
        char         storename[64];


        *tuplestore = tuplestore_begin_datarow(false, work_mem / NumDataNodes, tmpcxt);
        /* We need is to be able to remember/restore the read position */
        snprintf(storename, 64, "%s node %d", squeue->sq_key, cstate->cs_node);
        tuplestore_collect_stat(*tuplestore, storename);
        /*
         * Allocate a second read pointer to read from the store. We know
         * it must have index 1, so needn't store that.
         */
        ptrno = tuplestore_alloc_read_pointer(*tuplestore, 0);
        Assert(ptrno == 1);

        if(g_DataPumpDebug)
            elog(LOG, "create tuplestore with nodeid %d, cursor %s.", nodeid, squeue->sq_key);
    }
    in_data_pump = true;
    /* Append the slot to the store... */
    tuplestore_puttupleslot(*tuplestore, slot);
    in_data_pump = false;

    node->ntuples_put++;
    
    return;

ERR:
    DataPumpWaitSenderDone(squeue->sender, false);
    DestoryDataPumpSenderControl(squeue->sender, squeue->sq_nodeid);
    squeue->sender_destroy = true;
    squeue->sender = NULL;
    elog(ERROR, "SendDataRemote::DataPump status:%d abnormal!", ret);
}
bool socket_set_nonblocking(int fd, bool non_block)
{
    int flags = 0;

    /* get old flags */
    flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0)
    {
        return false;
    }

    /* flip O_NONBLOCK */
    if (non_block)
    {
        flags |= O_NONBLOCK;
    }
    else
    {
        flags &= ~O_NONBLOCK;
    }

    /* set new flags */
    if (fcntl(fd, F_SETFL, flags) < 0)
    {
        return false;
    }
    return true;
}

bool
ExecFastSendDatarow(TupleTableSlot *slot, void *sndctl, int32 nodeindex, MemoryContext tmpcxt)
{// #lizard forgives
#define DEFAULT_RESERVE_STEP 128
#define MAX_SLEEP_TIMES 50
    uint32          free_size       = 0;
    uint16             n16             = 0;
    uint32            tuple_len       = 0;
    int             i                = 0;
    DataPumpSenderControl *sender   = NULL;
    DataPumpNodeControl   *node     = NULL;
    TupleDesc         tdesc = slot->tts_tupleDescriptor;
    StringInfoData      data;
    uint32 head = 0;

    sender   = (DataPumpSenderControl*)sndctl;
    node     = &sender->nodes[nodeindex];
    
    
    /* Get tuple length. */
    tuple_len = 1; /* msg type 'D' */
    if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3)
    {
        tuple_len += 4;
    }
    if (0 == sender->tuple_len)
    {
        sender->tuple_len = DEFAULT_RESERVE_STEP;
    }
    tuple_len += sender->tuple_len;    

    head = node->buffer->m_Head;
    free_size = FreeSpace(node->buffer);
    if (free_size > tuple_len)    
    {
        int32  ret         = 0;
        uint32 tmp         = 0;
        uint32 leng_ptr    = 0;
        uint32 write_len   = 0;        
        uint32 n32         = 0;
        uint32 data_offset = 0;
        uint32 remaining_length = 0;
        MemoryContext    savecxt = NULL;

        (void) ReserveSpace(node->buffer, tuple_len, &data_offset);
        remaining_length = tuple_len;
        /* MsgType */
        FillReserveSpace(node->buffer, data_offset, "D", 1);
        data_offset = BufferOffsetAdd(node->buffer, data_offset, 1);
        remaining_length -= 1;
        
        /* Init length, exclude command tag. */
        write_len = 1;
        /* Data length */
        if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3)
        {
            leng_ptr         = data_offset;        
            data_offset = BufferOffsetAdd(node->buffer, data_offset, sizeof(n32));
            remaining_length -= sizeof(n32);    

            write_len += sizeof(n32);
        }
        
        /* Data */
        
        /* ensure we have all values */
        slot_getallattrs(slot);
        
        /* if temporary memory context is specified reset it */
        if (tmpcxt)
        {
            MemoryContextReset(tmpcxt);
            savecxt = MemoryContextSwitchTo(tmpcxt);
        }

        initStringInfo(&data);
        
        /* Number of parameter values */
        n16 = htons(tdesc->natts);

        /* No possiblity of memory short, we reserved DEFAULT_RESERVE_STEP bytes above. */
        FillReserveSpace(node->buffer, data_offset, (char *) &n16, sizeof(n16));
        data_offset = BufferOffsetAdd(node->buffer, data_offset, sizeof(n16));
        remaining_length -= sizeof(n16);

        /* Write length */
        write_len += sizeof(n16);
        
        #if 0
        appendBinaryStringInfo(&buf, (char *) &n16, 2);
        #endif
        
        for (i = 0; i < tdesc->natts; i++)
        {
            if (slot->tts_isnull[i])
            {
                n32 = htonl(-1);
                #if 0
                appendBinaryStringInfo(&buf, (char *) &n32, 4);
                #endif

                if (write_len + sizeof(n32) >= node->buffer->m_Length - 1)
                {
                    appendBinaryStringInfo(&data, (char *) &n32, 4);
                }
                else
                {
                    if (remaining_length < sizeof(n32))
                    {
                        int sleep_times = 0;
                        
                        do
                        {
                            ret = ReserveSpace(node->buffer, DEFAULT_RESERVE_STEP, &tmp);
                            if (!ret)
                            {
                                break;
                            }
                            pg_usleep(1000L);
                            if (!DataPumpNodeCheck(sndctl, nodeindex))
                            {
                                ReturnSpace(node->buffer, head);
                                pfree(data.data);
                                if (savecxt)
                                {
                                    MemoryContextSwitchTo(savecxt);
                                }
                                elog(ERROR, "ExecFastSendDatarow:node %d status abnormal.", nodeindex);
                            }

                            sleep_times++;

                            if (sleep_times == MAX_SLEEP_TIMES)
                            {
                                ReturnSpace(node->buffer, head);

                                pfree(data.data);

                                if (savecxt)
                                {
                                    MemoryContextSwitchTo(savecxt);
                                }
                                
                                return false;
                            }
                        }while(ret == -1);

                        /* sanity check */
                        if ((remaining_length + data_offset) % node->buffer->m_Length != tmp)
                        {
                            ReturnSpace(node->buffer, head);
                            pfree(data.data);
                            if (savecxt)
                            {
                                MemoryContextSwitchTo(savecxt);
                            }
                            elog(ERROR, "mismatch in Fast end.");
                        }
                        
                        remaining_length += DEFAULT_RESERVE_STEP;
                    }
                    FillReserveSpace(node->buffer, data_offset, (char *) &n32, sizeof(n32));
                    data_offset = BufferOffsetAdd(node->buffer, data_offset, sizeof(n32));
                    remaining_length -= sizeof(n32);
                }
                
                /* Write length */
                write_len += sizeof(n32);
            }
            else
            {
                Form_pg_attribute attr = tdesc->attrs[i];
                uint32  reserve_len  = 0;
                Oid        typOutput;
                bool    typIsVarlena;
                Datum    pval;
                char   *pstring;
                int        len;

                /* Get info needed to output the value */
                getTypeOutputInfo(attr->atttypid, &typOutput, &typIsVarlena);
                /*
                 * If we have a toasted datum, forcibly detoast it here to avoid
                 * memory leakage inside the type's output routine.
                 */
                if (typIsVarlena)
                    pval = PointerGetDatum(PG_DETOAST_DATUM(slot->tts_values[i]));
                else
                    pval = slot->tts_values[i];

                /*
                  * column is composite type, need to send tupledesc to remote node
                  */
                if (attr->atttypid == RECORDOID)
                {
                    HeapTupleHeader rec;
                    Oid            tupType;
                    int32        tupTypmod;
                    TupleDesc    tupdesc;
                    StringInfoData  tupdesc_data;
                    
                    initStringInfo(&tupdesc_data);
                    
                    /* -2 to indicate this is composite type */
                    n32 = htonl(-2);

                    if (write_len + sizeof(n32) >= node->buffer->m_Length - 1)
                    {
                        appendBinaryStringInfo(&data, (char *) &n32, 4);
                    }
                    else
                    {
                        if (remaining_length < sizeof(n32))
                        {
                            int sleep_times = 0;
                            
                            do
                            {
                                ret = ReserveSpace(node->buffer, DEFAULT_RESERVE_STEP, &tmp);
                                if (!ret)
                                {
                                    break;
                                }
                                pg_usleep(1000L);
                                if (!DataPumpNodeCheck(sndctl, nodeindex))
                                {
                                    ReturnSpace(node->buffer, head);
                                    pfree(data.data);
                                    if (savecxt)
                                    {
                                        MemoryContextSwitchTo(savecxt);
                                    }
                                    elog(ERROR, "ExecFastSendDatarow:node %d status abnormal.", nodeindex);
                                }
                                
                                sleep_times++;

                                if (sleep_times == MAX_SLEEP_TIMES)
                                {
                                    ReturnSpace(node->buffer, head);

                                    pfree(data.data);

                                    if (savecxt)
                                    {
                                        MemoryContextSwitchTo(savecxt);
                                    }
                                    
                                    return false;
                                }
                            }while(ret == -1);

                            /* sanity check */
                            if ((remaining_length + data_offset) % node->buffer->m_Length != tmp)
                            {
                                ReturnSpace(node->buffer, head);
                                pfree(data.data);
                                if (savecxt)
                                {
                                    MemoryContextSwitchTo(savecxt);
                                }
                                elog(ERROR, "mismatch in Fast end.");
                            }
                            
                            remaining_length += DEFAULT_RESERVE_STEP;
                        }
                        FillReserveSpace(node->buffer, data_offset, (char *) &n32, sizeof(n32));
                        data_offset = BufferOffsetAdd(node->buffer, data_offset, sizeof(n32));
                        remaining_length -= sizeof(n32);
                    }
                    
                    /* Write length */
                    write_len += sizeof(n32);


                    rec = DatumGetHeapTupleHeader(pval);

                    /* Extract type info from the tuple itself */
                    tupType = HeapTupleHeaderGetTypeId(rec);
                    tupTypmod = HeapTupleHeaderGetTypMod(rec);
                    tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

                    FormRowDescriptionMessage(tupdesc, NULL, NULL, &tupdesc_data);

                    ReleaseTupleDesc(tupdesc);

                    len = tupdesc_data.len;
                    n32 = htonl(len);

                    /* write rowDesctiption */
                    if (write_len + len +sizeof(n32) >= node->buffer->m_Length - 1)
                    {
                        appendBinaryStringInfo(&data, (char *) &n32, sizeof(n32));
                        appendBinaryStringInfo(&data, tupdesc_data.data, len);
                    }
                    else
                    {
                        if (remaining_length < (sizeof(n32) + len))
                        {
                            int sleep_times = 0;
                            reserve_len = (sizeof(n32) + len) + DEFAULT_RESERVE_STEP;
                            do
                            {
                                ret = ReserveSpace(node->buffer, reserve_len, &tmp);
                                if (!ret)
                                {
                                    break;
                                }
                                pg_usleep(1000L);
                                if (!DataPumpNodeCheck(sndctl, nodeindex))
                                {
                                    ReturnSpace(node->buffer, head);
                                    pfree(data.data);
                                    if (savecxt)
                                    {
                                        MemoryContextSwitchTo(savecxt);
                                    }
                                    elog(ERROR, "ExecFastSendDatarow:node %d status abnormal.", nodeindex);
                                }

                                sleep_times++;

                                if (sleep_times == MAX_SLEEP_TIMES)
                                {
                                    ReturnSpace(node->buffer, head);

                                    pfree(data.data);

                                    if (savecxt)
                                    {
                                        MemoryContextSwitchTo(savecxt);
                                    }
                                    
                                    return false;
                                }
                            }while(ret == -1);

                            /* sanity check */
                            if ((remaining_length + data_offset) % node->buffer->m_Length != tmp)
                            {
                                ReturnSpace(node->buffer, head);
                                pfree(data.data);
                                if (savecxt)
                                {
                                    MemoryContextSwitchTo(savecxt);
                                }
                                elog(ERROR, "mismatch in Fast end.");
                            }
                            
                            remaining_length += reserve_len;
                        }
                        FillReserveSpace(node->buffer, data_offset, (char *) &n32, sizeof(n32));
                        data_offset = BufferOffsetAdd(node->buffer, data_offset, sizeof(n32));
                        remaining_length -= sizeof(n32);
                    
                        FillReserveSpace(node->buffer, data_offset, tupdesc_data.data, len);
                        data_offset = BufferOffsetAdd(node->buffer, data_offset, len);
                        remaining_length -= len;
                    }
                    
                    write_len += sizeof(n32);
                    write_len += len;

                    /* MemoryContextReset will do it */
                    //pfree(tupdesc_data.data);
                }
                
                /* Convert Datum to string */
                pstring = OidOutputFunctionCall(typOutput, pval);

                /* copy data to the buffer */
                len = strlen(pstring);
                n32 = htonl(len);
                #if 0
                appendBinaryStringInfo(&buf, (char *) &n32, 4);
                appendBinaryStringInfo(&buf, pstring, len);
                #endif

                if (write_len + sizeof(n32) + len >= node->buffer->m_Length - 1)
                {
                    appendBinaryStringInfo(&data, (char *) &n32, 4);
                    appendBinaryStringInfo(&data, pstring, len);
                }
                else
                {
                    /* Reserve space. */
                    if (remaining_length < (sizeof(n32) + len))
                    {
                        int sleep_times = 0;
                        
                        reserve_len = (sizeof(n32) + len) + DEFAULT_RESERVE_STEP;
                        do
                        {
                            ret = ReserveSpace(node->buffer, reserve_len, &tmp);
                            if (!ret)
                            {
                                break;
                            }
                            pg_usleep(1000L);
                            if (!DataPumpNodeCheck(sndctl, nodeindex))
                            {
                                ReturnSpace(node->buffer, head);
                                pfree(data.data);
                                if (savecxt)
                                {
                                    MemoryContextSwitchTo(savecxt);
                                }
                                elog(ERROR, "ExecFastSendDatarow:node %d status abnormal.", nodeindex);
                            }

                            sleep_times++;

                            if (sleep_times == MAX_SLEEP_TIMES)
                            {
                                ReturnSpace(node->buffer, head);

                                pfree(data.data);

                                pfree(pstring);

                                if (savecxt)
                                {
                                    MemoryContextSwitchTo(savecxt);
                                }
                                
                                return false;
                            }
                        }while(ret == -1);
                        /* sanity check */
                        if ((remaining_length + data_offset) % node->buffer->m_Length != tmp)
                        {
                            ReturnSpace(node->buffer, head);
                            pfree(data.data);
                            if (savecxt)
                            {
                                MemoryContextSwitchTo(savecxt);
                            }
                            elog(ERROR, "mismatch in Fast end.");
                        }
                        
                        remaining_length += reserve_len;
                    }
                    
                    /* Data length */
                    FillReserveSpace(node->buffer, data_offset, (char *) &n32, sizeof(n32));
                    data_offset = BufferOffsetAdd(node->buffer, data_offset, sizeof(n32));
                    remaining_length -= sizeof(n32);

                    /* Data self */
                    FillReserveSpace(node->buffer, data_offset, pstring, len);
                    data_offset = BufferOffsetAdd(node->buffer, data_offset, len);
                    remaining_length -= len;
                }

                /* MemoryContextReset will do it */
                //pfree(pstring);
                //pstring = NULL;
                
                /* Write length */
                write_len += sizeof(n32);

                /* Write length */
                write_len += len;
            }
        }

        #if 0
        /* restore memory context to allocate result */
        if (savecxt)
        {
            MemoryContextSwitchTo(savecxt);
        }

        /* copy data to the buffer */
        datarow = (RemoteDataRow) palloc(sizeof(RemoteDataRowData) + buf.len);
        datarow->msgnode = InvalidOid;
        datarow->msglen = buf.len;
        memcpy(datarow->msg, buf.data, buf.len);
        pfree(buf.data);
        #endif

        /* Write data length, we reserve data above. */
        n32 = htonl(write_len - 1);
        FillReserveSpace(node->buffer, leng_ptr, (char *)&n32, sizeof(n32));
        
        /* Return space if needed. */
        if (remaining_length)
        {
            ReturnSpace(node->buffer, data_offset);
        }        

        if (data.len)
        {
            //int sleep_times = 0;
            int data_len = data.len;
            data.cursor = 0;
            
            while (data_len)
            {
                uint32 len = FreeSpace(node->buffer);

                if (len)
                {
                    uint32 write_len = data_len > len ? len : data_len;
                        
                    PutData(node->buffer, data.data + data.cursor, write_len);

                    data.cursor = data.cursor + write_len;

                    data_len = data_len - write_len;
                }
                else
                {
                    DataPumpWakeupSender(sndctl, nodeindex);
                    pg_usleep(50L);
                    if (!DataPumpNodeCheck(sndctl, nodeindex))
                    {
                        pfree(data.data);
                        if (savecxt)
                        {
                            MemoryContextSwitchTo(savecxt);
                        }
                        elog(ERROR, "ExecFastSendDatarow:node %d status abnormal.", nodeindex);
                    }

#if 0
                    sleep_times++;

                    if (sleep_times == MAX_SLEEP_TIMES)
                    {
                        ReturnSpace(node->buffer, head);

                        pfree(data.data);
                        
                        return false;
                    }
#endif
                }
            }
        }
        else
        {
            /* Big enough, send data. */
            if (DataSize(node->buffer) > g_SndBatchSize * 1024)
            {    
                DataPumpWakeupSender(sndctl, nodeindex);
            }
            else
            {
                SetBorder(node->buffer);
            }
        }

        if (!data.len)
        {
            /* Save max tuple_len */
            sender->tuple_len = sender->tuple_len > (write_len - 5) ? sender->tuple_len : (write_len - 5);
        }

        /* MemoryContextReset will do it */
        //pfree(data.data);

        if (savecxt)
        {
            MemoryContextSwitchTo(savecxt);
        }
        
        node->ntuples++;
        node->nfast_send++;
        return true;
    }
    else
    {
        /* Not enough space, wakeup sender. */
        DataPumpWakeupSender(sndctl, nodeindex);
        if (!DataPumpNodeCheck(sndctl, nodeindex))
        {
            elog(ERROR, "ExecFastSendDatarow:node %d status abnormal.", nodeindex);
        }
        return false;
    }
}

void DataPumpWakeupSender(void *sndctl, int32 nodeindex)
{
    int32 threadid = 0;
    int32 step     = 0;
    DataPumpThreadControl *thread = NULL;
    DataPumpSenderControl *sender   = NULL;
    DataPumpNodeControl   *node     = NULL;

    sender   = (DataPumpSenderControl*)sndctl;
    step = DIVIDE_UP(sender->node_num, sender->thread_num);
    threadid = nodeindex / step;        
    
    thread = &sender->thread_control[threadid];
    
    /* Tell thread to send data. */
    node = &sender->nodes[nodeindex];
    SetBorder(node->buffer);
    ThreadSemaUp(&thread->send_sem);
}

void
create_datapump_socket_dir(void)
{
    DIR           *temp_dir;
    struct dirent *temp_de;
    char        rm_path[MAXPGPATH * 2];

    /*
      * create data pump socket dir if not exist,
      * else clean the socket dir
      */
    temp_dir = AllocateDir(DATA_PUMP_SOCKET_DIR);
    if (temp_dir == NULL)
    {
        /* does not exist, create dir  */
        if (errno == ENOENT)
        {
            if(mkdir(DATA_PUMP_SOCKET_DIR, S_IRWXU) != 0)
            {
                elog(ERROR, "could not create dir \"%s\" for data pump, errMsg:%s", 
                     DATA_PUMP_SOCKET_DIR, strerror(errno));
            }
        }
        else
        {
            elog(LOG, "could not open dir \"%s\", errMsg:%s", DATA_PUMP_SOCKET_DIR, strerror(errno));
        }

        return;
    }
    else
    {
        /* exist, remove all the files */
        while ((temp_de = ReadDir(temp_dir, DATA_PUMP_SOCKET_DIR)) != NULL)
        {
            if (strcmp(temp_de->d_name, ".") == 0 ||
                strcmp(temp_de->d_name, "..") == 0)
                continue;

            snprintf(rm_path, sizeof(rm_path), "%s/%s",
                     DATA_PUMP_SOCKET_DIR, temp_de->d_name);

            unlink(rm_path);
        }

        FreeDir(temp_dir);
    }
}

/* build control center to manager data transfering */
static ParallelSendControl* 
BuildParallelSendControl(SharedQueue sq)
{
    bool      succeed = false;
    int       i    = 0;
    int32     base = 0;
    int32     step = 0;    
    int32     end  = 0;    
    
    ParallelSendControl *senderControl = NULL;

    senderControl = (ParallelSendControl *)palloc0(sizeof(ParallelSendControl));

    /* remote node number and sender number */
    senderControl->numNodes   = sq->sq_nconsumers;
    senderControl->numParallelWorkers = sq->numParallelWorkers;
    senderControl->numThreads = g_SndThreadNum > sq->sq_nconsumers ? sq->sq_nconsumers : g_SndThreadNum;

    /* init remote nodes control */
    senderControl->nodes = (ParallelSendNodeControl *)palloc0(sizeof(ParallelSendNodeControl) * senderControl->numNodes);

    for (i = 0; i < senderControl->numNodes; i++)
    {
        ConsState  *cstate = &(sq->sq_consumers[i]);

        InitParallelSendNodeControl(cstate->cs_node, &senderControl->nodes[i], sq->numParallelWorkers);
    }

    /* init sender control */
    senderControl->threadControl = (ParallelSendThreadControl *)palloc0(sizeof(ParallelSendThreadControl) * senderControl->numThreads);

    base = 0;
    end  = 0;
    step = DIVIDE_UP(senderControl->numNodes, senderControl->numThreads);
    
    for (i = 0; i < senderControl->numThreads; i++)    
    {
        InitParallelSendThreadControl(i, &senderControl->threadControl[i], senderControl->nodes);

        if (i < senderControl->numThreads  - 1)
        {
            base = end;
            end  = base + step;
        }
        else
        {
            base = end;
            end = senderControl->numNodes;
        }

        senderControl->threadControl[i].node_base = base;
        senderControl->threadControl[i].node_end  = end;
        senderControl->threadControl[i].node_num  = senderControl->numNodes;
    }

    /* init convert control */
    snprintf(senderControl->convertControl.sqname, MAX_CURSOR_LEN, "%s", sq->sq_key);
    senderControl->convertControl.maxConn = senderControl->numNodes;
    ThreadSemaInit(&senderControl->convertControl.quit_sem, 0);

    /* init share data structure */
    InitParallelSendSharedData(sq, senderControl, senderControl->numNodes);

    /* map node buffer to shared buffer */
    MapNodeDataBuffer(senderControl);

    /* map sender's sem to shared sem */
    //MapThreadSenderSem(senderControl);

    senderControl->status                 = senderControl->sharedData->status;
    senderControl->nodeNoSocket           = senderControl->sharedData->nodeNoSocket;
    senderControl->nodeSendDone           = senderControl->sharedData->nodeSendDone;

    /* all resources are ready, fire senders up */
    succeed = CreateParallelSenderThread(senderControl);

    if (!succeed)
    {
        DestoryParallelSendControl(senderControl, sq->sq_nodeid);
        sq->sender_destroy = true;
        sq->parallelSendControl = NULL;
        elog(LOG, DATA_PUMP_PREFIX"start thread failed for %s", strerror(errno));
        return NULL;
    }
    
    sq->parallelSendControl = senderControl;

    execute_error = false;

    return senderControl;
}

/*
 * init parallel worker node control
 */
static void 
InitParallelSendNodeControl(int32 nodeId, ParallelSendNodeControl *control, int32 numParallelWorkers)
{
    control->nodeId             = nodeId;
    control->sock               = NO_SOCKET;
    control->numParallelWorkers = numParallelWorkers;
    control->status             = DataPumpSndStatus_no_socket;
    spinlock_init(&control->lock);
    control->ntuples            = 0;
    control->sleep_count        = 0;
    control->current_buffer     = 0;
    control->send_timies        = 0;

    /* pointer to data buffer in share memory, set later */
    control->buffer = (ParallelSendDataQueue **)palloc0(sizeof(ParallelSendDataQueue *) * numParallelWorkers);
    
}

/* init sender control */
static void
InitParallelSendThreadControl(int32 threadNum, ParallelSendThreadControl *control, ParallelSendNodeControl *nodes)
{
    control->threadNum        = threadNum;
    control->thread_need_quit = false;
    control->nodes            = nodes;
    ThreadSemaInit(&control->quit_sem, 0);
    control->thread_need_quit = false;
    control->thread_running   = false;
}

/* allocate space in share memory for sender and parallel workers to send tuples */
static void
InitParallelSendSharedData(SharedQueue sq, ParallelSendControl *senderControl, int consMap_len)
{// #lizard forgives
    int i           = 0;
    int j           = 0;
    int bufferIndex = 0;
    Size segsize    = 0;
    shm_toc     *toc;
    shm_toc_estimator estimator;
    ParallelSendSharedData  *sharedData = NULL; 

    /* estimate the total space needed in share memory */
    shm_toc_initialize_estimator(&estimator);

    shm_toc_estimate_chunk(&estimator, sizeof(ParallelSendSharedData));
    shm_toc_estimate_keys(&estimator, 1);

    shm_toc_estimate_chunk(&estimator, sizeof(ParallelSendStatus) * senderControl->numParallelWorkers);
    shm_toc_estimate_keys(&estimator, 1);

    shm_toc_estimate_chunk(&estimator, sizeof(bool) * senderControl->numNodes);
    shm_toc_estimate_keys(&estimator, 1);

    shm_toc_estimate_chunk(&estimator, sizeof(bool) * senderControl->numNodes * senderControl->numParallelWorkers);
    shm_toc_estimate_keys(&estimator, 1);

    /* consmap */
    shm_toc_estimate_chunk(&estimator, sizeof(int) * consMap_len);
    shm_toc_estimate_keys(&estimator, 1);

    
    shm_toc_estimate_chunk(&estimator, sizeof(bool));
    shm_toc_estimate_keys(&estimator, 1);

#if 0
    /* sender sem */
    shm_toc_estimate_chunk(&estimator, sizeof(ThreadSema) * senderControl->numThreads);
    shm_toc_estimate_keys(&estimator, 1);
#endif

    /* data buffer */
    shm_toc_estimate_chunk(&estimator, PW_DATA_QUEUE_SIZE * senderControl->numNodes * senderControl->numParallelWorkers);
    shm_toc_estimate_keys(&estimator, 1);

    /* estimate done, got total space needed in share memory, set up! */
    segsize = shm_toc_estimate(&estimator);
    dsm_seg = dsm_create(segsize, DSM_CREATE_NULL_IF_MAXSEGMENTS);

    if (dsm_seg == NULL)
    {
        elog(ERROR, "could not create dsm for parallel workers to send tuples.");
    }

    toc = shm_toc_create(PARALLEL_MAGIC,
                         dsm_segment_address(dsm_seg),
                         segsize);

    /* init each part of share data */
    sharedData = (ParallelSendSharedData *)
                  shm_toc_allocate(toc, sizeof(ParallelSendSharedData));
    shm_toc_insert(toc, PARALLEL_SEND_SHARE_DATA, sharedData);

    sharedData->numExpectedParallelWorkers = senderControl->numParallelWorkers;
    sharedData->numNodes                   = senderControl->numNodes;
    sharedData->numSenderThreads           = senderControl->numThreads;
    sharedData->sender_error               = (bool *)shm_toc_allocate(toc, sizeof(bool));
    memset(sharedData->sender_error, 0, sizeof(bool));
    shm_toc_insert(toc, PARALLEL_SEND_SENDER_ERROR, sharedData->sender_error);
    
    memcpy(sharedData->nodeMap, sq->nodeMap, sizeof(int16) * MAX_NODES_NUMBER);

    snprintf(sharedData->sq_key, SQUEUE_KEYSIZE, "%s", sq->sq_key);

    sharedData->status  = (ParallelSendStatus *)shm_toc_allocate(toc, sizeof(ParallelSendStatus) * senderControl->numParallelWorkers);
    shm_toc_insert(toc, PARALLEL_SEND_WORKER_STATUS, sharedData->status);
    for (i = 0; i < senderControl->numParallelWorkers; i++)
    {
        sharedData->status[i] = ParallelSend_None;
    }

    sharedData->nodeNoSocket = (bool *)shm_toc_allocate(toc, sizeof(bool) * senderControl->numNodes);
    shm_toc_insert(toc, PARALLEL_SEND_NODE_NO_SOCKET, sharedData->nodeNoSocket);
    memset(sharedData->nodeNoSocket, 0, sizeof(bool) * senderControl->numNodes);

    sharedData->nodeSendDone = (bool *)shm_toc_allocate(toc, sizeof(bool) * senderControl->numNodes * senderControl->numParallelWorkers);
    memset(sharedData->nodeSendDone, 0, sizeof(bool) * senderControl->numNodes * senderControl->numParallelWorkers);
    shm_toc_insert(toc, PARALLEL_SEND_NODE_SEND_DONE, sharedData->nodeSendDone);
    
    /* cons_map will be filled later */
    sharedData->consMap = (int *)shm_toc_allocate(toc, sizeof(int) * consMap_len);
    shm_toc_insert(toc, PARALLEL_SEND_CONS_MAP, sharedData->consMap);

#if 0
    /* init sender sem */
    sharedData->threadSem = (ThreadSema *)shm_toc_allocate(toc, sizeof(ThreadSema) * senderControl->numThreads);
    for (i = 0; i < senderControl->numThreads; i++)
    {
        ThreadSemaInit(&sharedData->threadSem[i], 0);
    }
    shm_toc_insert(toc, PARALLEL_SEND_SENDER_SEM, sharedData->threadSem);
#endif
    /* 
      * init each data buffer 
      */
    sharedData->buffer = (ParallelSendDataQueue *)shm_toc_allocate(toc, 
                          PW_DATA_QUEUE_SIZE * senderControl->numNodes * senderControl->numParallelWorkers);
    for (i = 0 ; i < senderControl->numParallelWorkers; i++)
    {
        for (j = 0; j < senderControl->numNodes; j++)
        {
            ParallelSendDataQueue *buffer = NULL;
            ConsState  *cstate = &(sq->sq_consumers[j]);
            
            bufferIndex = i * senderControl->numNodes + j;

            buffer = (ParallelSendDataQueue *)((char *)sharedData->buffer + bufferIndex * PW_DATA_QUEUE_SIZE);

            buffer->bufLength = g_SndThreadBufferSize * 1024;
            buffer->bufHead   = 0;
            buffer->bufTail   = 0;
            buffer->bufBorder = 0;
            buffer->parallelWorkerNum = i;
            buffer->nodeId  = cstate->cs_node;
            buffer->bufFull = false;
            buffer->ntuples = 0;
            buffer->tuples_get  = 0;
            buffer->tuples_put  = 0;
            buffer->fast_send   = 0;
            buffer->normal_send = 0;
            buffer->send_times  = 0;
            buffer->no_data     = 0;
            buffer->send_data_len  = 0;
            buffer->write_data_len = 0;
            buffer->long_tuple     = 0;
			buffer->wait_free_space = false;
            buffer->status         = DataPumpSndStatus_no_socket;
            buffer->stuck          = false;
            buffer->last_send      = false;
            ThreadSemaInit(&buffer->sendSem, 0);
            spinlock_init(&buffer->bufLock);
        }
    }
    shm_toc_insert(toc, PARALLEL_SEND_DATA_BUFFER, sharedData->buffer);

    parallel_send_seg_handle = dsm_segment_handle(dsm_seg);

    senderControl->sharedData = sharedData;

    if (g_DataPumpDebug)
    {
        elog(LOG, "Squeue %s: InitParallelSendSharedData successfully.", sq->sq_key);
    }
}

/* 
  * data buffers have been already create in share memory, 
  * we need to point to the corresponding buffer for each node.
  */
static void
MapNodeDataBuffer(ParallelSendControl *senderControl)
{
    int i                           = 0;
    int j                           = 0;
    int numParallelWorkers          = senderControl->numParallelWorkers;
    int nodeId                      = 0;
    int numNodes                    = senderControl->numNodes;
    ParallelSendNodeControl *nodes  = NULL;
    ParallelSendDataQueue   *buffer = senderControl->sharedData->buffer;

    for (i = 0; i < numNodes; i++)
    {
        nodes  = &senderControl->nodes[i];
        nodeId = nodes->nodeId;
        
        for (j = 0; j < numParallelWorkers; j++)
        {
            nodes->buffer[j] = (ParallelSendDataQueue *)((char *)buffer + (j * numNodes + i) * PW_DATA_QUEUE_SIZE);

            if (nodeId != nodes->buffer[j]->nodeId)
                elog(ERROR, "map node buffer %d to wrong shared buffer %d.", nodeId, nodes->buffer[j]->nodeId);
        }
    }
}

/*
 * create sender thread to send tuples 
 */
static bool 
CreateParallelSenderThread(ParallelSendControl *sender)
{
    bool succeed = true;
    int  i       = 0;
    int  ret     = 0;
    
    for (i = 0; i < sender->numThreads; i++)
    {        
        ret = CreateThread(ParallelSenderThreadMain, (void *)&sender->threadControl[i], MT_THR_DETACHED);
        if (ret)
        {
            succeed = false;
            break;
        }
        /* Set running status for the thread. */
        sender->threadControl[i].thread_running = true;
    }

    if (succeed)
    {
        ret = CreateThread(ParallelConvertThreadMain, (void *)sender, MT_THR_DETACHED);
        if(ret)
            succeed = false;
        else
            sender->convertControl.cstatus = ConvertRunning;
    }
    
    if (!succeed)
    {
        ParallelSendCleanThread(sender);        
    }
    return succeed;
}

static void *
ParallelConvertThreadMain(void *arg)
{// #lizard forgives
#define SOCK_PATH_LEN 128
    bool exit_flag = false;
    int nodeid;
    int consumerIdx;
    int sockfd;
    int listen_fd;
    int con_fd;
    int ret;
    ParallelSendControl *control = NULL;
    char sock_path[SOCK_PATH_LEN];

    ThreadSigmask();
    
    control = (ParallelSendControl *)arg;
    control->convertControl.begin_stamp = GetCurrentTimestamp();
    
    snprintf(sock_path, SOCK_PATH_LEN, "%s/%s", DATA_PUMP_SOCKET_DIR, control->convertControl.sqname);
    
    listen_fd = convert_listen(sock_path, control->convertControl.maxConn);
    if(listen_fd < 0)
    {
        control->convertControl.errNO = errno;
        control->convertControl.cstatus = ConvertListenError;
        return NULL;
    }    

    while(true)
    {
#if 0
        if (control->convert_control.connect_num == (control->convert_control.maxConn - 1))
        {
            control->convert_control.finish_stamp = GetCurrentTimestamp();
        }
        
        if (exit_flag && control->convert_control.connect_num == control->convert_control.maxConn)
        {
            close(listen_fd);
            control->convert_control.cstatus = ConvertExit;
            unlink(sock_path);
            break;
        }
#endif
        if (exit_flag)
        {
            control->convertControl.finish_stamp = GetCurrentTimestamp();
            close(listen_fd);
            control->convertControl.cstatus = ConvertExit;
            unlink(sock_path);
            break;
        }
        
        con_fd = accept(listen_fd, NULL, NULL);
        if (con_fd < 0)
        {
            close(listen_fd);
            control->convertControl.errNO = errno;
            control->convertControl.cstatus = ConvertAcceptError;
            unlink(sock_path);
            break;
        }

        control->convertControl.connect_num++;
        
        /* recv nodeid */
        ret = recv(con_fd, (char *)&nodeid, 4, 0);
        if(ret != 4)
        {
            close(con_fd);
            close(listen_fd);
            control->convertControl.errNO = errno;
            control->convertControl.cstatus = ConvertRecvNodeidError;
            unlink(sock_path);
            break;
        }
        
        nodeid = ntohl(nodeid);

        if(nodeid == -1)
        {
            exit_flag = true;
            close(con_fd);
            continue;
        }

        /* recv consumer index  */
        ret = recv(con_fd, (char *)&consumerIdx, 4, 0);
        if(ret != 4)
        {
            close(con_fd);
            close(listen_fd);
            control->convertControl.errNO = errno;
            control->convertControl.cstatus = ConvertRecvNodeindexError;
            unlink(sock_path);
            break;
        }

        consumerIdx = ntohl(consumerIdx);

        /* recv fd */
        if(convert_recvfds(con_fd, (int *)&sockfd, 1) != 0)
        {
            close(con_fd);
            close(listen_fd);
            control->convertControl.errNO   = errno;
            control->convertControl.cstatus = ConvertRecvSockfdError;
            unlink(sock_path);
            break;
        }

        /* store nodeid, consumerIdx, sockfd */
        if(SetNodeSocket(arg, consumerIdx, nodeid, sockfd) != DataPumpOK)
        {
            close(con_fd);
            close(listen_fd);
            control->convertControl.cstatus = ConvertSetSockfdError;
            unlink(sock_path);
            break;
        }

        close(con_fd);
    }

    ThreadSemaUp(&control->convertControl.quit_sem);
    return NULL;
}

static void *
ParallelSenderThreadMain(void *arg)
{
    int send_times = 0;
    
    ParallelSendThreadControl *threadControl = (ParallelSendThreadControl *)arg;

    ThreadSigmask();

    /* loop to send data until told to exit */
    while(1)
    {
        /* wait for data coming */
        //ThreadSemaDown(threadControl->threadSem);
        pg_usleep(100L);

        send_times++;
        
        /* send data */
        ParallelSenderSendData(threadControl, false);

        /* try to send data if any, then exit */
        if (threadControl->thread_need_quit)
        {
            ParallelSenderSendData(threadControl, true);
            break;
        }
    }

    threadControl->thread_running = false;
    ThreadSemaUp(&threadControl->quit_sem);
    return NULL;
}

/* sender thread send data to remote nodes */
static void
ParallelSenderSendData(ParallelSendThreadControl *threadControl, bool last_send)
{// #lizard forgives
    bool stuck                     = 0;
    int nodeIndex                  = 0;
    int base                       = threadControl->node_base;
    int end                        = threadControl->node_end;
    int numNodes                   = threadControl->node_num;
    int32  nodeStatus              = 0;
    ParallelSendNodeControl *node  = NULL;

    do
    {
        stuck = false;
        for (nodeIndex = base; nodeIndex < end && nodeIndex < numNodes; nodeIndex++)
        {
            node = &threadControl->nodes[nodeIndex];

            spinlock_lock(&node->lock);
            nodeStatus = node->status;
            spinlock_unlock(&node->lock);

            /* socket is valid, try to send data */
            if (nodeStatus >= DataPumpSndStatus_set_socket && nodeStatus  <= DataPumpSndStatus_data_sending)
            {
                if (SendNodeData(node, last_send))
                {
                    /* node socket got stuck, retry later */
                    stuck = true;
                }
                else
                {
                    if (last_send)
                    {
                        spinlock_lock(&node->lock);
                        if (node->status != DataPumpSndStatus_error)
                        {
                            node->status = DataPumpSndStatus_done;
                        }
                        spinlock_unlock(&node->lock);
                    }
                }
            }
        }
    }while(stuck);
}

/* send data to node
  * if socket got stuck, return true
  * else return false 
  */
static bool
SendNodeData(ParallelSendNodeControl *node, bool last_send)
{// #lizard forgives
    bool   should_send   = false;
    bool   long_tuple    = false;
	bool   wait_free_space = false;
    int    i             = 0;
    uint32 len           = 0;
    int32  ret           = 0;
    int32  reason        = 0;
    char   *data         = NULL;
    int32 current_buffer = node->current_buffer;
    uint32  data_size    = 0;

    node->send_timies++;
    
    /* 
      * each node has n buffers to n parallel workers. we try to send all data in current buffer,
      * then advance to next buffer, this must be more efficient. if got any errors, break out.
      * If socket got stuck, return stuck to upper level.
      */
    while (1)
    {
        ParallelSendDataQueue *buffer = node->buffer[node->current_buffer];

        if (buffer->status == DataPumpSndStatus_set_socket)
        {
            if (buffer->stuck)
            {
                should_send = true;
            }
            else if (buffer->last_send)
            {
                should_send = true;
            }
            else
            {
                should_send = last_send;
            }

            if (!should_send)
            {
				data_size = NodeDataSize(buffer, &long_tuple, &wait_free_space);

				/* 
				 * If wait_free_space is true, sender thread should send data to free buffer space,
				 * else wait until data_size reach to batch threshold.
				 */
				if (!wait_free_space && data_size < g_SndBatchSize * 1024)
                {
                    node->current_buffer = (node->current_buffer + 1) % node->numParallelWorkers;

                    if (current_buffer == node->current_buffer)
                    {
                        break;
                    }

                    continue;
                }
            }
            
            do
            {
                data = GetNodeData(buffer, &len, &long_tuple);

                if (len == 0)
                {
                    if (long_tuple)
                    {
                        buffer->stuck = true;
                        return true;
                    }
                    else
                    {
                        buffer->no_data++;
                        break;
                    }
                }

                if (data)
                {
                    buffer->send_times++;

#if 0
                    if (!buffer->stuck)
                    {
                        if (data[0] != 'D' && buffer->bufTail != 0)
                        {            
                            /* data content mismatch. we need 'D' here, but got others. */
                            abort();
                        }
                    }
#endif
                    ret = RawSendNodeData(node, node->sock, data, len, &reason);                
                    if (EOF == ret)
                    {
                        /* We got error. */
                        spinlock_lock(&node->lock);
                        node->status  = DataPumpSndStatus_error;
                        node->errorno = reason;
                        spinlock_unlock(&node->lock);
                        for (i = 0; i < node->numParallelWorkers; i++)
                        {
                            node->buffer[i]->status = DataPumpSndStatus_error;
                        }
                        return false;
                    }

                    buffer->send_data_len += ret;
                    
                    /* increase data offset */
                    IncNodeDataOff(buffer, ret);    
                    
                    /* Socket got stuck. */
                    if (reason == EAGAIN || reason == EWOULDBLOCK)
                    {
                        if (len > ret)
                        {
                            node->errorno = reason;
                            buffer->stuck = true;
                            return true;
                        }
                    }

                    /* get left data length */
					len = NodeDataSize(buffer, &long_tuple, &wait_free_space);

                    if (len == 0)
                    {
                        if (long_tuple)
                        {
                            buffer->stuck = true;
                            return true;
                        }
                    }
                }
            }while(len);

            buffer->stuck      = false;
            
            if (last_send)
            {
                buffer->status = DataPumpSndStatus_done;
            }
        }

        node->current_buffer = (node->current_buffer + 1) % node->numParallelWorkers;

        if (current_buffer == node->current_buffer)
        {
            break;
        }
    }

    /* send successfully */
    return false;
}

/* Get data pointer, use with the following functions. */
static char *
GetNodeData(ParallelSendDataQueue *buf, uint32 *uiLen, bool *long_tuple)
{
    uint32 border = 0;
    uint32 tail = 0;
    char  *data;
    if (buf)
    {
		bool wait_flag = false;
		if (0 == NodeDataSize(buf, long_tuple, &wait_flag))
        {
            *uiLen = 0;
            return NULL;
        }
        
        spinlock_lock(&(buf->bufLock));
        border = buf->bufBorder;
        tail   = buf->bufTail;
        *long_tuple = buf->long_tuple;
        spinlock_unlock(&(buf->bufLock));
        if (INVALID_BORDER == border)
        {
            *uiLen = 0;
            return NULL;
        }

        /* read from tail to border*/
        if (border >=  tail)
        { 
             /* Only sender increases m_Tail, no need to lock. */
             *uiLen = border - tail;  
             data   = buf->buffer + tail;    
#if 0
             spinlock_lock(&(buf->bufLock));
             /* No more data. */
             if (border == buf->bufBorder)
              {                 
                buf->bufBorder = INVALID_BORDER;
              }
             spinlock_unlock(&(buf->bufLock));
#endif
             
        }
        else
        {  
            /* read from tail to end */
            *uiLen = buf->bufLength - tail;
            data = buf->buffer + tail;
        } 
        
        return data;
    }
    else
    {
        *uiLen = 0;
        return NULL;
    }
}

/* Return total data size in buffer */
static uint32 
NodeDataSize(ParallelSendDataQueue *buf, bool *long_tuple, bool *wait_free_space)
{
    uint32 border = 0;
    uint32 tail   = 0;
    uint32 size   = 0;
    if (buf)
    {
        spinlock_lock(&(buf->bufLock));
        tail   = buf->bufTail;
        border = buf->bufBorder;
        *long_tuple = buf->long_tuple;
		*wait_free_space = buf->wait_free_space;
        spinlock_unlock(&(buf->bufLock));
        
        if (INVALID_BORDER == border)
        {
            return 0;
        }
        
        if (tail <= border)
        {    
            size = border - tail;
        }
        else
        {
            size = buf->bufLength - tail + border;
        }
        
        return size;
    }
    return 0;
}

/* Increate data offset, used after finishing read data from queue. */
static void 
IncNodeDataOff(ParallelSendDataQueue *buf, uint32 uiLen)
{
    if (buf)
    {
        spinlock_lock(&(buf->bufLock));
        buf->bufTail =  (buf->bufTail + uiLen) % buf->bufLength;                  
        spinlock_unlock(&(buf->bufLock));
    }
}

/* Return data write to the socket. */
static int 
RawSendNodeData(ParallelSendNodeControl *node, int32 sock, char * data, int32 len, int32 * reason)
{
    int32  offset       = 0;
    int32  nbytes_write = 0;

    while (offset < len)
    {
        nbytes_write = send(sock, data + offset, len - offset, 0);
        if (nbytes_write <= 0)
        {
            if (errno == EINTR)
            {
                continue;        /* Ok if we were interrupted */
            }

            /*
             * Ok if no data writable without blocking, and the socket is in
             * non-blocking mode.
             */
            if (errno == EAGAIN ||
                errno == EWOULDBLOCK)
            {                
                pg_usleep(1000L);
                node->sleep_count++;
                *reason = errno;
                return offset;
            }
            *reason = errno;
            return EOF;
        }
        offset += nbytes_write;
    }
    
    return offset;
}

static int32  
SetNodeSocket(void *sndctl, int32 nodeindex, int32 nodeId,  int32 socket)
{
    ParallelSendNodeControl  *node     = NULL;
    ParallelSendControl   *sender   = (ParallelSendControl *)sndctl;
    if (nodeindex >= sender->numNodes)
    {
        return DataPumpSndError_node_error;
    }


    node = &sender->nodes[nodeindex];
    if (node->nodeId != nodeId)
    {
        return DataPumpSndError_node_error;
    }

    /* Use lock to check status and socket */
    socket_set_nonblocking(socket, true);
    spinlock_lock(&node->lock);
    if (NO_SOCKET == node->sock && DataPumpSndStatus_no_socket == node->status)
    {
        int i = 0;
        node->sock   = socket;
        node->status = DataPumpSndStatus_set_socket;

        for (i = 0; i < sender->numParallelWorkers; i++)
        {
            node->buffer[i]->status = DataPumpSndStatus_set_socket;
        }
        spinlock_unlock(&node->lock);
        return DataPumpOK;
    }
    else
    {
        spinlock_unlock(&node->lock);
        return DataPumpSndError_bad_status;
    }
}

static bool 
ParallelSendCleanThread(ParallelSendControl *sender)
{// #lizard forgives
    bool                      ret       = true;
    int32                       threadid  = 0;
    ParallelSendThreadControl *thread   = NULL;
    bool                      *send_quit = NULL;

    if (!sender)
    {
        return true;
    }

    send_quit = (bool *)palloc0(sizeof(bool) * sender->numThreads);

    if (sender->threadControl)
    {
        for (threadid = 0; threadid < sender->numThreads; threadid++)
        {
            thread = &sender->threadControl[threadid];
            /* Set quit flag and tell sender to quit. */
            if (thread->thread_running)
            {
                thread->thread_need_quit = true;
                ThreadSemaUp(thread->threadSem);    

                send_quit[threadid] = true;

                if (g_DataPumpDebug)
                {
                    elog(DEBUG1, "Squeue %s: tell thread %d to exit.", sender->convertControl.sqname, threadid);
                }
            }
        }

        for (threadid = 0; threadid < sender->numThreads; threadid ++)
        {
            if (send_quit[threadid])
            {
                thread = &sender->threadControl[threadid];
                /* Wait for sender to quit. */        
                ThreadSemaDown(&thread->quit_sem);

                if (thread->thread_running)
                {
                    elog(DEBUG1, "Squeue %s:thread %d done, but status is running.", sender->convertControl.sqname,
                              threadid);
                }

                if (g_DataPumpDebug)
                {
                    elog(DEBUG1, "Squeue %s:thread %d exit.", sender->convertControl.sqname, threadid);
                }
            }
        }
    }

    pfree(send_quit);

    ret = ConvertDone(&sender->convertControl);    

    return ret;
}

static void 
DestoryParallelSendControl (void* sndctl, int nodeid)
{// #lizard forgives
    int  i         = 0;
    ParallelSendControl *sender   = (ParallelSendControl*)sndctl;

    if (!sndctl)
    {
        return;
    }

    if (sender->threadControl)
    {
        pfree(sender->threadControl);
        sender->threadControl = NULL;
    }

    if (sender->nodes)
    {
        for (i = 0; i < sender->numNodes; i++)
        {        
            pfree(sender->nodes[i].buffer);

            if (sender->nodes[i].sock != NO_SOCKET && sender->nodes[i].nodeId != nodeid)
            {
                close(sender->nodes[i].sock);
                sender->nodes[i].sock = NO_SOCKET;
            }
        }
        pfree(sender->nodes);
        sender->nodes = NULL;

        pfree(sender);
    }

    if (dsm_seg)
    {
        dsm_detach(dsm_seg);
        dsm_seg = NULL;
    }

    if (g_DataPumpDebug)
    {
        elog(DEBUG1, "Squeue %s: DestoryParallelSendControl.", sender->convertControl.sqname);
    }
}

bool
needParallelSend(SharedQueue squeue)
{
    return squeue->parallelWorkerSendTuple;
}

void 
SetLocatorInfo(SharedQueue squeue, int *consMap, int len, char distributionType, Oid keytype, AttrNumber distributionKey)
{
    ParallelSendControl *sender = squeue->parallelSendControl;

    sender->sharedData->len = len;
    sender->sharedData->distributionType = distributionType;
    sender->sharedData->keytype = keytype;
    sender->sharedData->distributionKey = distributionKey;

    memcpy(sender->sharedData->consMap, consMap, sizeof(int) * len);
}

dsm_handle
GetParallelSendSegHandle(void)
{
    return parallel_send_seg_handle;
}

/* set up data queue between parallel workers and senders */
DestReceiver *
GetParallelSendReceiver(dsm_handle handle)
{
    bool found                          = false;
    int i                               = 0;
    dsm_segment *seg                    = NULL;
    shm_toc *toc                        = NULL;
    ParallelSendDestReceiver *receiver  = NULL; 
    ParallelSendSharedData    *sharedData = NULL; 
    ParallelSendSharedData    *sData      = NULL;
    ParallelSendDataQueue   *head       = NULL;
    
    seg = dsm_attach(handle);
    if (seg == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("could not map dynamic shared memory segment in GetParallelSendReceiver")));
    
    toc = shm_toc_attach(PARALLEL_MAGIC, dsm_segment_address(seg));
    if (toc == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("invalid magic number in dynamic shared memory segment in GetParallelSendReceiver")));
    
    receiver          = (ParallelSendDestReceiver *)palloc0(sizeof(ParallelSendDestReceiver));
    receiver->control = (ParallelWorkerControl *)palloc0(sizeof(ParallelWorkerControl));

    receiver->seg = seg;

    dsm_seg = seg;

    receiver->mycontext = CurrentMemoryContext;

    receiver->tmpcxt = AllocSetContextCreate(CurrentMemoryContext,
                                             "ParallelSendReceiver temp memoryContext",
                                             ALLOCSET_DEFAULT_SIZES);

    sData               = (ParallelSendSharedData *)palloc0(sizeof(ParallelSendSharedData));

    sharedData          = shm_toc_lookup(toc, PARALLEL_SEND_SHARE_DATA, false);

    sData->numExpectedParallelWorkers = sharedData->numExpectedParallelWorkers;
    sData->numNodes                   = sharedData->numNodes;
    sData->numSenderThreads           = sharedData->numSenderThreads;

    sData->consMap      = shm_toc_lookup(toc, PARALLEL_SEND_CONS_MAP, false);

    sData->distributionKey  = sharedData->distributionKey;
    sData->distributionType = sharedData->distributionType;
    sData->keytype          = sharedData->keytype;
    sData->len              = sharedData->len;
    memcpy(sData->nodeMap, sharedData->nodeMap, sizeof(int16) * MAX_NODES_NUMBER);
    memcpy(sData->sq_key, sharedData->sq_key, sizeof(char) * SQUEUE_KEYSIZE);

    receiver->squeue = (SharedQueue) hash_search(SharedQueues, sharedData->sq_key, HASH_FIND, &found);

    if (!found)
    {
        elog(ERROR, "Parallel Worker %d Shared queue %s not found", ParallelWorkerNumber, sharedData->sq_key);
    }

#if 0
    sharedData->threadSem          = shm_toc_lookup(toc, PARALLEL_SEND_SENDER_SEM, false);
#endif

    sData->status                 = shm_toc_lookup(toc, PARALLEL_SEND_WORKER_STATUS, false);
    sData->status[ParallelWorkerNumber] = ParallelSend_Init;

    senderstatus = sData->status;

    sData->nodeNoSocket       = shm_toc_lookup(toc, PARALLEL_SEND_NODE_NO_SOCKET, false);
    sData->nodeSendDone       = shm_toc_lookup(toc, PARALLEL_SEND_NODE_SEND_DONE, false);
    sData->sender_error       = shm_toc_lookup(toc, PARALLEL_SEND_SENDER_ERROR, false);

    receiver->control->parallelWorkerNum = ParallelWorkerNumber;
    receiver->control->numNodes   = sData->numNodes;
    receiver->control->buffer     = (ParallelSendDataQueue **)palloc0(sizeof(ParallelSendDataQueue *) * sData->numNodes);
    //receiver->control->threadSem  = sharedData->threadSem;
    receiver->control->numThreads = sData->numSenderThreads;

    sData->buffer = shm_toc_lookup(toc, PARALLEL_SEND_DATA_BUFFER, false);

    head   = (ParallelSendDataQueue *)((char *)sData->buffer + sData->numNodes * ParallelWorkerNumber * PW_DATA_QUEUE_SIZE);

    for (i = 0; i < sData->numNodes; i++)
    {
        receiver->control->buffer[i] = (ParallelSendDataQueue *)((char *)head + i * PW_DATA_QUEUE_SIZE);
    }

    receiver->tstores = (Tuplestorestate **)
                palloc0(sData->numNodes * sizeof(Tuplestorestate *));

    receiver->locator = createLocator(sData->distributionType,
                                    RELATION_ACCESS_INSERT,
                                    sData->keytype,
                                    LOCATOR_LIST_INT,
                                    sData->len,
                                    sData->consMap,
                                    NULL,
                                    false,
                                    InvalidOid, InvalidOid, InvalidOid, InvalidAttrNumber, InvalidOid);

    receiver->sharedData      = sData;

    receiver->distKey         = sData->distributionKey;
    receiver->pub.receiveSlot = ParallelSendReceiveSlot;
    receiver->pub.rStartup    = ParallelSendStartupReceiver;
    receiver->pub.rShutdown   = ParallelSendShutdownReceiver;
    receiver->pub.rDestroy    = ParallelSendDestroyReceiver;
    receiver->pub.mydest      = DestParallelSend;

    if (g_DataPumpDebug)
    {
        elog(LOG, "Squeue %s: worker %d init parallel sendReceiver.", sData->sq_key, ParallelWorkerNumber);
    }
    return (DestReceiver *) receiver;
}

static bool
ParallelSendReceiveSlot(TupleTableSlot *slot, DestReceiver *self)
{// #lizard forgives
    Datum        value;
    bool        isnull;
    int         ncount, i;
    int         *result = NULL;
    ParallelSendDestReceiver *receiver = (ParallelSendDestReceiver *)self;
    ParallelWorkerControl *control     = receiver->control;
    ParallelSendSharedData *sharedData = receiver->sharedData;

    if (receiver->distKey == InvalidAttrNumber)
    {
        value = (Datum) 0;
        isnull = true;
    }
    else
        value = slot_getattr(slot, receiver->distKey, &isnull);

#ifdef __COLD_HOT__
    ncount = GET_NODES(receiver->locator, value, isnull, 0, true, NULL);
#else
    ncount = GET_NODES(receiver->locator, value, isnull, NULL);
#endif

    result = (int *) getLocatorResults(receiver->locator);
        /* Dispatch the tuple */
    for (i = 0; i < ncount; i++)
    {
        TimestampTz begin = 0;
        TimestampTz end   = 0;
        int consumerIdx = result[i];
        MemoryContext savecontext;

        if (getLocatorDisType(receiver->locator) == 'S')
        {
            int nodeid = result[i];

            consumerIdx = sharedData->nodeMap[nodeid];

            if (consumerIdx == SQ_CONS_INIT)
            {
                elog(ERROR, "Invalid nodeid %d in ParallelSendReceiveSlot.", nodeid);
            }
        }
        else
        {
            consumerIdx = result[i];
        }

        if (consumerIdx == SQ_CONS_NONE)
        {
            continue;
        }

        savecontext = MemoryContextSwitchTo(receiver->mycontext);

        if (enable_statistic)
        {
            begin = GetCurrentTimestamp();
        }

        if (sharedData->sender_error[0])
        {
            DestroyParallelSendReceiver(self);
            elog(ERROR, "could not send data to node buffer.");
        }    
            
        if (!receiver->tupledesc)
        {
            receiver->tupledesc = CreateTupleDescCopy(slot->tts_tupleDescriptor);
        }

        if (control->buffer[consumerIdx]->parallelWorkerNum != ParallelWorkerNumber)
        {
            elog(ERROR, "parallel send worker mismatch, buffer worker %d, current worker %d.",
                         control->buffer[consumerIdx]->parallelWorkerNum, ParallelWorkerNumber);
        }
        
        SendNodeDataRemote(receiver->squeue, control, control->buffer[consumerIdx], consumerIdx, slot, 
                           &receiver->tstores[consumerIdx], receiver->tmpcxt);

        receiver->send_tuples++;
        
        if (enable_statistic)
        {
            end   = GetCurrentTimestamp();
            receiver->send_total_time += (end - begin);
        }

        MemoryContextSwitchTo(savecontext);
    }

    return true;
}

static void
ParallelSendStartupReceiver(DestReceiver *self, int operation, TupleDesc typeinfo)
{
    /* do nothing */
}

static void
ParallelSendShutdownReceiver(DestReceiver *self)
{// #lizard forgives
    int i = 0;
    bool  *send_done = NULL;
    int   numDone    = 0;
    MemoryContext savecontext;
    ParallelSendDestReceiver *receiver = (ParallelSendDestReceiver *)self;
    ParallelSendSharedData *sharedData = receiver->sharedData;
    ParallelWorkerControl *control     = receiver->control;
    TupleTableSlot * tmpslot = NULL;
    SharedQueue squeue = receiver->squeue;

    sharedData->status[ParallelWorkerNumber] = ParallelSend_ExecDone;

    if (g_DataPumpDebug)
    {
        elog(LOG, "Squeue %s:parallel worker %d, exec done!", sharedData->sq_key , ParallelWorkerNumber);
    }
    
    if (receiver->send_tuples)
    {
        savecontext = MemoryContextSwitchTo(receiver->mycontext);

        tmpslot = MakeSingleTupleTableSlot(receiver->tupledesc);

        send_done = (bool *)palloc0(sizeof(bool) * sharedData->numNodes);

        while(i < sharedData->numNodes)
        {
            int node_index = ParallelWorkerNumber * sharedData->numNodes + i;
            ConsState  *cstate = &(squeue->sq_consumers[i]);
                
            if (sharedData->sender_error[0] || ParallelError())
            {
                DestroyParallelSendReceiver(self);
                elog(ERROR, "could not send data to node buffer.");
            }
                    
            if (!send_done[i])
            {
                if (!sharedData->nodeNoSocket[i])
                {
                    if (control->buffer[i]->status == DataPumpSndStatus_set_socket)
                    {
                        if ((cstate->cs_done && cstate->send_fd) ||
                            (cstate->cs_node == squeue->sq_nodeid && squeue->producer_done))
                        {
                            send_done[i] = true;
                        }
                        else
                        {
                            send_done[i] = PumpTupleStoreToBuffer(control, control->buffer[i], i, tmpslot, receiver->tstores[i], receiver->mycontext);
                        }
                        
                        if (send_done[i])
                        {
                            numDone++;

                            control->buffer[i]->last_send = true;

                            sharedData->nodeSendDone[node_index] = true;
                        }
                    }
                }
                else
                {
                    send_done[i] = true;
                    numDone++;

                    sharedData->nodeSendDone[node_index] = true;
                }
            }

            if (numDone == sharedData->numNodes)
            {
                break;
            }
            
            i = (i + 1) % sharedData->numNodes;
        }

        ExecDropSingleTupleTableSlot(tmpslot);
        pfree(send_done);
        
        MemoryContextSwitchTo(savecontext);
    }
    else
    {
        for (i = 0; i < sharedData->numNodes; i++)
        {
            int node_index = ParallelWorkerNumber * sharedData->numNodes + i;

            sharedData->nodeSendDone[node_index] = true;
        }
    }

    sharedData->status[ParallelWorkerNumber] = ParallelSend_SendDone;

    if (g_DataPumpDebug)
    {
        elog(LOG, "Squeue %s:parallel worker %d, send done!", sharedData->sq_key , ParallelWorkerNumber);
    }
}

static void
ParallelSendDestroyReceiver(DestReceiver *self)
{
    ParallelSendDestReceiver *receiver = (ParallelSendDestReceiver *)self;
    
    if (enable_statistic)
    {
        elog(LOG, "ParallelSend: send_tuples:%lu, send_total_time:%ld, avg_time:%lf.",
                   receiver->send_tuples, receiver->send_total_time,
                   ((double)receiver->send_total_time) / ((double)receiver->send_tuples));
    }
    
    DestroyParallelSendReceiver(self);

    ParallelDsmDetach();
}

static void
SendNodeDataRemote(SharedQueue squeue, ParallelWorkerControl *control, ParallelSendDataQueue *buf, int32 consumerIdx,
                           TupleTableSlot *slot, Tuplestorestate **tuplestore, MemoryContext tmpcxt)
{// #lizard forgives
    bool ret              = false;
    RemoteDataRow datarow = NULL;
    ConsState  *cstate = &(squeue->sq_consumers[consumerIdx]);

    /* all consumers done, do not produce data anymore */
    if (squeue->nConsumer_done == squeue->sq_nconsumers)
    {
        Executor_done = true;

        if (g_DataPumpDebug)
        {
            elog(LOG, "SendNodeDataRemote:squeue %s parallelwoker %d pid %d all consumers done.", squeue->sq_key, ParallelWorkerNumber, MyProcPid);
        }
        
        return;
    }

    /* consumer do not need data anymore */
    if ((cstate->cs_pid == squeue->sq_pid && squeue->producer_done) ||
        (cstate->cs_pid != squeue->sq_pid && cstate->cs_done))
    {
        return;
    }


    if (buf->status == DataPumpSndStatus_set_socket)
    {
        if (slot->tts_datarow)
        {
            datarow = slot->tts_datarow;

            ret = ParallelSendDataRow(control, buf, datarow->msg, datarow->msglen, consumerIdx);

            if (ret)
            {
                return;
            }
        }
        else
        {
            /* Try fast send. */
            ret = ParallelFastSendDatarow(buf, slot, control, consumerIdx, tmpcxt);
            
            if (ret)
            {
                return;
            }    
        }
    }
    else if (buf->status == DataPumpSndStatus_error)
    {
        return;
    }

    /* Error ocurred or the node is not ready at present,  store the tuple into the tuplestore. */
    /* Create tuplestore if does not exist.*/
    if (NULL == *tuplestore)
    {
        int            ptrno PG_USED_FOR_ASSERTS_ONLY;
        char         storename[64];


        *tuplestore = tuplestore_begin_datarow(false, work_mem / NumDataNodes, tmpcxt);
        /* We need is to be able to remember/restore the read position */
        snprintf(storename, 64, "worker %d node %d", buf->parallelWorkerNum, buf->nodeId);
        tuplestore_collect_stat(*tuplestore, storename);
        /*
         * Allocate a second read pointer to read from the store. We know
         * it must have index 1, so needn't store that.
         */
        ptrno = tuplestore_alloc_read_pointer(*tuplestore, 0);
        Assert(ptrno == 1);
    }
    in_data_pump = true;
    
    /* Append the slot to the store... */
    tuplestore_puttupleslot(*tuplestore, slot);

    in_data_pump = false;

    buf->tuples_put++;
    
    return;
}

/* 
  * directly put data into data buffer, no matter what happened except that buffer is full,
  * then put data into tuplestore.
  */
static bool
ParallelSendDataRow(ParallelWorkerControl *control, ParallelSendDataQueue *buf, char *data, size_t len, int32 consumerIdx)
{// #lizard forgives
    bool                  long_tuple = false;
    int                   cursor     = 0;
    size_t                   tuple_len = 0;
        
    tuple_len = 1; /* msg type 'D' */
    if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3)
    {
        tuple_len += 4;
    }
    tuple_len += len;

    if (tuple_len >= buf->bufLength - 1)
    {
        long_tuple = true;
    }

    /* no space left, */
    if (BufferFreeSpace(buf) < (uint32)tuple_len)
    {
		/* Set flag to notice sender thread send data without waiting batch size threshold */
        if (!long_tuple)
        {
			SetBufferBorderAndWaitFlag(buf, false, true);
            pg_usleep(50L);
            return false;
        }
    }

    if (!long_tuple)
    {        
        /* MsgType */
        PutNodeData(buf, "D", 1);

        /* Data length */
        if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3)
        {
            uint32        n32;

            n32 = htonl((uint32) (len + 4));
            PutNodeData(buf, (char*)&n32, 4);
        }
        /* Data */
        PutNodeData(buf, data, len);

		SetBufferBorderAndWaitFlag(buf, false, false);
    }
    else
    {
        uint32 header_len = 0;
        uint32 data_len = BufferFreeSpace(buf);

        header_len = 1;
        
        if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3)
        {
            header_len += 4;
        }
        
        /* put message 'D' */
        while (data_len < header_len)
        {
			SetBufferBorderAndWaitFlag(buf, false, true);
            pg_usleep(100L);
            data_len = BufferFreeSpace(buf);

            if (buf->status == DataPumpSndStatus_error)
            {
                return true;
            }
        }
        
        PutNodeData(buf, "D", 1);

        /* Data length */
        if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3)
        {
            uint32        n32;

            n32 = htonl((uint32) (len + 4));
            PutNodeData(buf, (char*)&n32, 4);
        }

        cursor = 0;
        
        while (len)
        {
            data_len = BufferFreeSpace(buf);

            if (data_len)
            {
                int write_len = len > data_len ? data_len : len;
                
                PutNodeData(buf, data + cursor, write_len);

                cursor = cursor + write_len;

                len = len - write_len;
            }
            else
            {
				SetBufferBorderAndWaitFlag(buf, true, true);
                pg_usleep(100L);

                if (buf->status == DataPumpSndStatus_error)
                {
                    return true;
                }
            }
        }

		SetBufferBorderAndWaitFlag(buf, false, false);
    }

    buf->ntuples++;
    buf->normal_send++;

    return true;
}

static uint32 
BufferFreeSpace(ParallelSendDataQueue *buf)
{
    uint32 head = 0;
    uint32 tail = 0;
    uint32 len  = 0;
    if (buf)
    {
        spinlock_lock(&(buf->bufLock));
        head = buf->bufHead;
        tail = buf->bufTail;
        spinlock_unlock(&(buf->bufLock));

        if (tail <= head)
        {
            len = tail + buf->bufLength - head - 1;
        }
        else
        {
            len = tail - head - 1;
        }
        return len;
    }
    else
    {
        return 0;
    }
}

static void
SetBufferBorderAndWaitFlag(ParallelSendDataQueue *buf, bool long_tuple, bool wait_free_space)
{
    spinlock_lock(&(buf->bufLock));
    buf->bufBorder = buf->bufHead;
    buf->long_tuple = long_tuple;
	buf->wait_free_space = wait_free_space;
    spinlock_unlock(&(buf->bufLock));
}

/* Send data into buffer */
static void 
PutNodeData(ParallelSendDataQueue *buf, char *data, uint32 len)
{    
    char   *ptr;
    uint32  bufferLen;
    uint32  needLen;
    uint32  offset = 0;
    needLen = len;

    buf->write_data_len += len;
    
    while (1)
    {            
        ptr = GetBufferWriteOff(buf, &bufferLen);
        if (ptr)
        {
            if (bufferLen >= needLen)
            {
                memcpy(ptr, data + offset, needLen);
                IncBufferWriteOff(buf, needLen);
                return;
            }
            else
            {
                memcpy(ptr, data + offset, bufferLen);
                IncBufferWriteOff(buf, bufferLen);
                needLen -= bufferLen;
                offset  += bufferLen;
            }
        }
    }
}

static char *
GetBufferWriteOff(ParallelSendDataQueue *buf, uint32 *uiLen)
{
    uint32 head = 0;
    uint32 tail = 0;
    char  *ptr  = NULL;
    if (0 == BufferFreeSpace(buf))
    {
        *uiLen = 0;
        return NULL;
    }
    
    if (buf)
    {
        spinlock_lock(&(buf->bufLock));
        head = buf->bufHead;
        tail = buf->bufTail;
        spinlock_unlock(&(buf->bufLock));
        
        if (head >=  tail)
        { 
           /* tail is the beginning of the queue. */
           if (tail != 0)
           {
               
               *uiLen = buf->bufLength- head;                 
           }
           else
           {
               /* Reserved one byte as flag. */
               *uiLen = buf->bufLength- head - 1;                 
           }
        }
        else
        {               
           /* Reserved one byte as flag. */
           *uiLen = tail - head - 1;
        }        
        ptr = buf->buffer + head;
        return ptr;
    }
    else
    {
        *uiLen = 0;
        return NULL;
    }    
}

/* Used to increase the write pointer after write some data. */
static void 
IncBufferWriteOff(ParallelSendDataQueue *buf, uint32 uiLen)
{
    if (buf)
    {
        spinlock_lock(&(buf->bufLock));
           buf->bufHead += uiLen;
           buf->bufHead =  buf->bufHead % buf->bufLength;
        spinlock_unlock(&(buf->bufLock));
    }
}

static bool
ParallelFastSendDatarow(ParallelSendDataQueue *buf, TupleTableSlot *slot, void *ctl, int32 nodeindex, MemoryContext tmpcxt)
{// #lizard forgives
#define DEFAULT_RESERVE_STEP 128
#define MAX_SLEEP_TIMES 50
    uint32          free_size       = 0;
    uint16             n16             = 0;
    uint32            tuple_len       = 0;
    int             i                = 0;
    ParallelWorkerControl *control  = NULL;
    TupleDesc         tdesc = slot->tts_tupleDescriptor;
    uint32 head = 0;
    uint32 tail PG_USED_FOR_ASSERTS_ONLY = 0;
    StringInfoData        data;

    control  = (ParallelWorkerControl*)ctl;
    
    /* Get tuple length. */
    tuple_len = 1; /* msg type 'D' */
    if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3)
    {
        tuple_len += 4;
    }
    if (0 == control->tupleLen)
    {
        control->tupleLen = DEFAULT_RESERVE_STEP;
    }
    tuple_len += control->tupleLen;    

    head      = buf->bufHead;
    tail      = buf->bufTail;
    free_size = BufferFreeSpace(buf);
    if (free_size > tuple_len)    
    {
        int32  ret         = 0;
        uint32 tmp         = 0;
        uint32 leng_ptr    = 0;
        uint32 write_len   = 0;        
        uint32 n32         = 0;
        uint32 data_offset = 0;
        uint32 remaining_length = 0;
        MemoryContext    savecxt = NULL;

        (void) ReserveBufferSpace(buf, tuple_len, &data_offset);
        remaining_length = tuple_len;
        /* MsgType */
        FillReserveBufferSpace(buf, data_offset, "D", 1);
        data_offset = DataBufferOffsetAdd(buf, data_offset, 1);
        remaining_length -= 1;
        
        /* Init length, exclude command tag. */
        write_len = 1;
        /* Data length */
        if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3)
        {
            leng_ptr         = data_offset;        
            data_offset = DataBufferOffsetAdd(buf, data_offset, sizeof(n32));
            remaining_length -= sizeof(n32);    

            write_len += sizeof(n32);
        }
        
        /* Data */
        
        /* ensure we have all values */
        slot_getallattrs(slot);

        /* if temporary memory context is specified reset it */
        if (tmpcxt)
        {
            MemoryContextReset(tmpcxt);
            savecxt = MemoryContextSwitchTo(tmpcxt);
        }

        initStringInfo(&data);
        
        /* Number of parameter values */
        n16 = htons(tdesc->natts);

        /* No possiblity of memory short, we reserved DEFAULT_RESERVE_STEP bytes above. */
        FillReserveBufferSpace(buf, data_offset, (char *) &n16, sizeof(n16));
        data_offset = DataBufferOffsetAdd(buf, data_offset, sizeof(n16));
        remaining_length -= sizeof(n16);

        /* Write length */
        write_len += sizeof(n16);
        
        
        for (i = 0; i < tdesc->natts; i++)
        {
            if (slot->tts_isnull[i])
            {
                n32 = htonl(-1);

                if (write_len + sizeof(n32) >= buf->bufLength - 1)
                {
                    appendBinaryStringInfo(&data, (char *) &n32, sizeof(n32));
                }
                else
                {
                    if (remaining_length < sizeof(n32))
                    {
                        int sleep_time = 0;
                        
                        do
                        {
                            ret = ReserveBufferSpace(buf, DEFAULT_RESERVE_STEP, &tmp);
                            if (!ret)
                            {
                                break;
                            }
                            pg_usleep(1000L);

                            sleep_time++;

                            if (sleep_time == MAX_SLEEP_TIMES)
                            {
                                ReturnBufferSpace(buf, head);

                                pfree(data.data);

                                if (savecxt)
                                {
                                    MemoryContextSwitchTo(savecxt);
                                }

                                return false;
                            }
                        }while(ret == -1);

                        /* sanity check */
                        if ((remaining_length + data_offset) % buf->bufLength != tmp)
                        {
                            ReturnBufferSpace(buf, head);
                            pfree(data.data);
                            if (savecxt)
                            {
                                MemoryContextSwitchTo(savecxt);
                            }
                            elog(ERROR, "mismatch in Fast end.");
                        }
                        
                        remaining_length += DEFAULT_RESERVE_STEP;
                    }
                    FillReserveBufferSpace(buf, data_offset, (char *) &n32, sizeof(n32));
                    data_offset = DataBufferOffsetAdd(buf, data_offset, sizeof(n32));
                    remaining_length -= sizeof(n32);
                }
                
                /* Write length */
                write_len += sizeof(n32);
            }
            else
            {
                Form_pg_attribute attr = tdesc->attrs[i];
                uint32  reserve_len  = 0;
                Oid        typOutput;
                bool    typIsVarlena;
                Datum    pval;
                char   *pstring;
                int        len;

                /* Get info needed to output the value */
                getTypeOutputInfo(attr->atttypid, &typOutput, &typIsVarlena);
                /*
                 * If we have a toasted datum, forcibly detoast it here to avoid
                 * memory leakage inside the type's output routine.
                 */
                if (typIsVarlena)
                    pval = PointerGetDatum(PG_DETOAST_DATUM(slot->tts_values[i]));
                else
                    pval = slot->tts_values[i];

                /*
                  * column is composite type, need to send tupledesc to remote node
                  */
                if (attr->atttypid == RECORDOID)
                {
                    HeapTupleHeader rec;
                    Oid            tupType;
                    int32        tupTypmod;
                    TupleDesc    tupdesc;
                    StringInfoData  tupdesc_data;
                    
                    initStringInfo(&tupdesc_data);
                    
                    /* -2 to indicate this is composite type */
                    n32 = htonl(-2);

                    if (write_len + sizeof(n32) >= buf->bufLength - 1)
                    {
                        appendBinaryStringInfo(&data, (char *) &n32, sizeof(n32));
                    }
                    else
                    {
                        if (remaining_length < sizeof(n32))
                        {
                            int sleep_time = 0;
                            
                            do
                            {
                                ret = ReserveBufferSpace(buf, DEFAULT_RESERVE_STEP, &tmp);
                                if (!ret)
                                {
                                    break;
                                }
                                pg_usleep(1000L);

                                sleep_time++;

                                if (sleep_time == MAX_SLEEP_TIMES)
                                {
                                    ReturnBufferSpace(buf, head);

                                    pfree(data.data);

                                    if (savecxt)
                                    {
                                        MemoryContextSwitchTo(savecxt);
                                    }

                                    return false;
                                }
                            }while(ret == -1);
                    
                            /* sanity check */
                            if ((remaining_length + data_offset) % buf->bufLength != tmp)
                            {
                                ReturnBufferSpace(buf, head);
                                pfree(data.data);
                                if (savecxt)
                                {
                                    MemoryContextSwitchTo(savecxt);
                                }
                                elog(ERROR, "mismatch in Fast end.");
                            }
                            
                            remaining_length += DEFAULT_RESERVE_STEP;
                        }
                        FillReserveBufferSpace(buf, data_offset, (char *) &n32, sizeof(n32));
                        data_offset = DataBufferOffsetAdd(buf, data_offset, sizeof(n32));
                        remaining_length -= sizeof(n32);
                    }
                    
                    /* Write length */
                    write_len += sizeof(n32);

                    rec = DatumGetHeapTupleHeader(pval);

                    /* Extract type info from the tuple itself */
                    tupType = HeapTupleHeaderGetTypeId(rec);
                    tupTypmod = HeapTupleHeaderGetTypMod(rec);
                    tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

                    FormRowDescriptionMessage(tupdesc, NULL, NULL, &tupdesc_data);

                    ReleaseTupleDesc(tupdesc);

                    len = tupdesc_data.len;
                    n32 = htonl(len);

                    if (write_len + len + sizeof(n32) >= buf->bufLength - 1)
                    {
                        appendBinaryStringInfo(&data, (char *) &n32, sizeof(n32));
                        appendBinaryStringInfo(&data, tupdesc_data.data, len);
                    }
                    else
                    {
                        /* Reserve space. */
                        if (remaining_length < (sizeof(n32) + len))
                        {
                            int sleep_time = 0;
                            
                            reserve_len = (sizeof(n32) + len) + DEFAULT_RESERVE_STEP;
                            
                            do
                            {
                                ret = ReserveBufferSpace(buf, reserve_len, &tmp);
                                if (!ret)
                                {
                                    break;
                                }
                                pg_usleep(1000L);
                                
                                sleep_time++;

                                if (sleep_time == MAX_SLEEP_TIMES)
                                {
                                    ReturnBufferSpace(buf, head);

                                    pfree(data.data);

                                    if (savecxt)
                                    {
                                        MemoryContextSwitchTo(savecxt);
                                    }

                                    return false;
                                }
                            }while(ret == -1);
                            /* sanity check */
                            if ((remaining_length + data_offset) % buf->bufLength!= tmp)
                            {
                                ReturnBufferSpace(buf, head);
                                pfree(data.data);
                                if (savecxt)
                                {
                                    MemoryContextSwitchTo(savecxt);
                                }
                                elog(ERROR, "mismatch in Fast end.");
                            }
                            
                            remaining_length += reserve_len;
                        }
                        
                        /* Data length */
                        FillReserveBufferSpace(buf, data_offset, (char *) &n32, sizeof(n32));
                        data_offset = DataBufferOffsetAdd(buf, data_offset, sizeof(n32));
                        remaining_length -= sizeof(n32);
                    
                        /* Data self */
                        FillReserveBufferSpace(buf, data_offset, tupdesc_data.data, len);
                        data_offset = DataBufferOffsetAdd(buf, data_offset, len);
                        remaining_length -= len;
                    }
                    
                    /* Write length */
                    write_len += sizeof(n32);
                    
                    /* Write length */
                    write_len += len;

                    /* MemoryContextReset will do it */
                    //pfree(tupdesc_data.data);
                }

                /* Convert Datum to string */
                pstring = OidOutputFunctionCall(typOutput, pval);

                /* copy data to the buffer */
                len = strlen(pstring);
                n32 = htonl(len);

                if (write_len + len + sizeof(n32) >= buf->bufLength - 1)
                {
                    appendBinaryStringInfo(&data, (char *) &n32, sizeof(n32));
                    appendBinaryStringInfo(&data, pstring, len);
                }
                else
                {
                    /* Reserve space. */
                    if (remaining_length < (sizeof(n32) + len))
                    {
                        int sleep_time = 0;
                        
                        reserve_len = (sizeof(n32) + len) + DEFAULT_RESERVE_STEP;
                        do
                        {
                            ret = ReserveBufferSpace(buf, reserve_len, &tmp);
                            if (!ret)
                            {
                                break;
                            }
                            pg_usleep(1000L);

                            sleep_time++;

                            if (sleep_time == MAX_SLEEP_TIMES)
                            {
                                ReturnBufferSpace(buf, head);

                                pfree(data.data);

                                pfree(pstring);
                                
                                if (savecxt)
                                {
                                    MemoryContextSwitchTo(savecxt);
                                }

                                return false;
                            }
                        }while(ret == -1);
                        /* sanity check */
                        if ((remaining_length + data_offset) % buf->bufLength!= tmp)
                        {
                            ReturnBufferSpace(buf, head);
                            pfree(data.data);
                            if (savecxt)
                            {
                                MemoryContextSwitchTo(savecxt);
                            }
                            elog(ERROR, "mismatch in Fast end.");
                        }
                        
                        remaining_length += reserve_len;
                    }
                    
                    /* Data length */
                    FillReserveBufferSpace(buf, data_offset, (char *) &n32, sizeof(n32));
                    data_offset = DataBufferOffsetAdd(buf, data_offset, sizeof(n32));
                    remaining_length -= sizeof(n32);

                    /* Data self */
                    FillReserveBufferSpace(buf, data_offset, pstring, len);
                    data_offset = DataBufferOffsetAdd(buf, data_offset, len);
                    remaining_length -= len;
                }

                /* MemoryContextReset will do it */
                //pfree(pstring);
                //pstring = NULL;
                
                /* Write length */
                write_len += sizeof(n32);

                /* Write length */
                write_len += len;
            }
        }

        /* Write data length, we reserve data above. */
        n32 = htonl(write_len - 1);
        FillReserveBufferSpace(buf, leng_ptr, (char *)&n32, sizeof(n32));
        
        /* Return space if needed. */
        if (remaining_length)
        {
            ReturnBufferSpace(buf, data_offset);
        }    

        if (data.len)
        {
            //int sleep_time = 0;
            int data_len = data.len;
            data.cursor = 0;
            
            while (data_len)
            {
                uint32 len = BufferFreeSpace(buf);

                if (len)
                {
                    uint32 write_len = data_len > len ? len : data_len;

                    PutNodeData(buf, data.data + data.cursor, write_len);

                    data.cursor = data.cursor + write_len;

                    data_len = data_len - write_len;
                }
                else
                {
					SetBufferBorderAndWaitFlag(buf, true, true);
                    pg_usleep(50L);

                    if (buf->status == DataPumpSndStatus_error)
                    {
                        pfree(data.data);
                        if (savecxt)
                        {
                            MemoryContextSwitchTo(savecxt);
                        }
                        /* socket error */
                        return true;
                    }
#if 0
                    sleep_time++;

                    if (sleep_time == MAX_SLEEP_TIMES)
                    {
                        ReturnBufferSpace(buf, head);

                        pfree(data.data);

                        return false;
                    }
#endif
                }
            }

			SetBufferBorderAndWaitFlag(buf, false, false);
        }
        else
        {            
			SetBufferBorderAndWaitFlag(buf, false, false);
        }

        
        if (!data.len)
        {
            /* Save max tuple_len */
            control->tupleLen = control->tupleLen > (write_len - 5) ? control->tupleLen : (write_len - 5);
        }

        /* MemoryContextReset will do it */
        //pfree(data.data);

        if (savecxt)
        {
            MemoryContextSwitchTo(savecxt);
        }
                                        
        buf->ntuples++;
        buf->fast_send++;
        buf->write_data_len += (write_len);
        return true;
    }
    else
    {
#if 1
        /* Not enough space, wakeup sender. */
        //ParallelSendWakeupSender(control, buf, nodeindex);
		SetBufferBorderAndWaitFlag(buf, false, true);
        //pg_usleep(50L);
#endif
        return false;
    }
}

/* Reserve space in print buffer */
static int 
ReserveBufferSpace(ParallelSendDataQueue *buf, uint32 len, uint32 *offset)
{
    /* not enough space avaliable, wait */
    if (BufferFreeSpace(buf) < len)
    {
        return -1;
    }

    if (buf)
    {
        *offset    = buf->bufHead;
           buf->bufHead =  (buf->bufHead + len)% buf->bufLength;
    }
    return 0;        
}

static void 
FillReserveBufferSpace(ParallelSendDataQueue *buf, uint32 offset, char *p, uint32 len)
{
    uint32 bytes2end      = 0;
    uint32 bytesfrombegin = 0;   

    if (buf)
    {        
        bytes2end = buf->bufLength - offset;
        if (len <= bytes2end)
        {
            memcpy(buf->buffer + offset, p, len);    
        }
        else
        {
            bytesfrombegin = len - bytes2end;                
            memcpy(buf->buffer + offset, p, bytes2end);                    
            memcpy(buf->buffer, (char*)p + bytes2end, bytesfrombegin);
        }    
    }
}

static uint32 
DataBufferOffsetAdd(ParallelSendDataQueue *buf, uint32 pointer, uint32 offset)
{

    if (buf)
    {    
           return (pointer + offset) % buf->bufLength;
    }
    return 0;        
}

static int 
ReturnBufferSpace(ParallelSendDataQueue *buf, uint32 offset)
{
    if (buf)
    {
        buf->bufHead = offset;
    }
    return 0;        
}

static bool
PumpTupleStoreToBuffer(ParallelWorkerControl *control, ParallelSendDataQueue *buf, int32 consumerIdx,
                               TupleTableSlot *tmpslot, Tuplestorestate *tuplestore, MemoryContext tmpcxt)
{
    int ret = 0;
    
    if (tuplestore)
    {
        tuplestore_select_read_pointer(tuplestore, 1);

        /* If we have something in the tuplestore try to push this to the queue */
        while (!tuplestore_ateof(tuplestore))
        {
            /* save position */
            tuplestore_copy_read_pointer(tuplestore, 1, 0);

            /* Try to get next tuple to the temporary slot */
            if (!tuplestore_gettupleslot(tuplestore, true, false, tmpslot))
            {
                break;
            }

            buf->tuples_get++;
            
            /* The slot should contain a data row */
            Assert(tmpslot->tts_datarow);

            ret = ParallelSendDataRow(control, buf, tmpslot->tts_datarow->msg, tmpslot->tts_datarow->msglen, consumerIdx);;
            /* check if queue has enough room for the data */
            if (!ret)
            {

                /* Restore read position to get same tuple next time */
                tuplestore_copy_read_pointer(tuplestore, 0, 1);

                /* We might advance the mark, try to truncate */
                tuplestore_trim(tuplestore);

                /* Prepare for writing, set proper read pointer */
                tuplestore_select_read_pointer(tuplestore, 0);

                buf->tuples_get--;
                return false;
            }        
        }

        /* Remove rows we have just read */
        tuplestore_trim(tuplestore);

        /* prepare for writes, set read pointer 0 as active */
        tuplestore_select_read_pointer(tuplestore, 0);

        return true;
    }

    return true;
}

static void
DestroyParallelSendReceiver(DestReceiver *self)
{
    int i = 0;
    MemoryContext savecontext;
    ParallelSendDestReceiver *receiver = (ParallelSendDestReceiver *)self;
    ParallelSendSharedData *sharedData = receiver->sharedData;
    ParallelWorkerControl *control     = receiver->control;

    savecontext = MemoryContextSwitchTo(receiver->mycontext);

    for (i = 0; i < sharedData->numNodes; i++)
    {
        if (receiver->tstores[i])
        {
            tuplestore_end(receiver->tstores[i]);
            receiver->tstores[i] = NULL;
        }
    }

    pfree(control->buffer);
    pfree(receiver->control);
    receiver->control= NULL;

    pfree(receiver->tstores);
    receiver->tstores = NULL;

    //dsm_detach(receiver->seg);

    pfree(receiver);

    MemoryContextSwitchTo(savecontext);
}

void
WaitForParallelWorkerDone(int numParallelWorkers, bool launch_failed)
{// #lizard forgives
    bool found;
    shm_toc *toc       = NULL;
    SharedQueue squeue = NULL;
    ParallelSendSharedData    *sharedData = NULL; 
    int unconnect_count = 0;
    Bitmapset *unconnection_node = NULL;
    
    toc = shm_toc_attach(PARALLEL_MAGIC, dsm_segment_address(dsm_seg));
    if (toc == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("invalid magic number in dynamic shared memory segment in SetParallelWorkerNum.")));

    sharedData    = shm_toc_lookup(toc, PARALLEL_SEND_SHARE_DATA, false);

    sharedData->numLaunchedParallelWorkers = numParallelWorkers;

    squeue = (SharedQueue) hash_search(SharedQueues, sharedData->sq_key, HASH_FIND, &found);

    if (!found)
        elog(ERROR, "Shared queue %s not found", sharedData->sq_key);

    if (squeue->parallelWorkerSendTuple)
    {
        ParallelSendControl *parallelSendControl = squeue->parallelSendControl;

        if (launch_failed)
        {
            ParallelSendCleanAll(parallelSendControl, squeue->sq_nodeid);

            squeue->sender_destroy = true;

            squeue->parallelSendControl = NULL;

            snprintf(squeue->err_msg, ERR_MSGSIZE, "could not lunch parallel workers");

            squeue->has_err_msg = true;
            
            elog(ERROR, "could not lunch parallel workers.");
        }
        
        do
        {
            if (NodeStatusCheck(squeue, parallelSendControl, numParallelWorkers))
            {
                ParallelSendCleanAll(parallelSendControl, squeue->sq_nodeid);

                squeue->sender_destroy = true;

                squeue->parallelSendControl = NULL;
                
                elog(ERROR, "could not send data to remote nodes.");
            }
            
            pg_usleep(1000L);

            if (end_query_requested)
            {
                squeue->producer_done = true;
                end_query_requested = false;

                SpinLockAcquire(&squeue->lock);
                squeue->nConsumer_done++;
                SpinLockRelease(&squeue->lock);

                if (g_DataPumpDebug)
                {
                    elog(LOG, "WaitForParallelWorkerDone(%s:%d):squeue %s producer %d set query done", __FILE__, __LINE__, squeue->sq_key, MyProcPid);
                }
            }
        }while(!ParallelWorkerExecDone(numParallelWorkers, parallelSendControl->status));

        /*
          * Assume that not yet connected consumers won't connect and reset them,
          * discard all tuples for them.
          */
        do
        {
            if (NodeStatusCheck(squeue, parallelSendControl, numParallelWorkers))
            {
                ParallelSendCleanAll(parallelSendControl, squeue->sq_nodeid);

                squeue->sender_destroy = true;

                squeue->parallelSendControl = NULL;
                
                elog(ERROR, "could not send data to remote nodes.");
            }
            
            pg_usleep(1000L);

            if (end_query_requested)
            {
                squeue->producer_done = true;
                end_query_requested = false;

                SpinLockAcquire(&squeue->lock);
                squeue->nConsumer_done++;
                SpinLockRelease(&squeue->lock);

                if (g_DataPumpDebug)
                {
                    elog(LOG, "WaitForParallelWorkerDone(%s:%d):squeue %s producer %d set query done", __FILE__, __LINE__, squeue->sq_key, MyProcPid);
                }
            }

            ConsumerExit(squeue, parallelSendControl, numParallelWorkers, &unconnect_count, &unconnection_node);
            
        }while(!ParallelWorkerSendDone(numParallelWorkers, parallelSendControl->status));

        unconnect_count = 0;
        bms_free(unconnection_node);
        unconnection_node = NULL;
        do
        {
            if (NodeStatusCheck(squeue, parallelSendControl, numParallelWorkers))
            {
                ParallelSendCleanAll(parallelSendControl, squeue->sq_nodeid);

                squeue->sender_destroy = true;

                squeue->parallelSendControl = NULL;
                
                elog(ERROR, "could not send data to remote nodes.");
            }
            pg_usleep(1000L);
        }while(!ConsumerExit(squeue, parallelSendControl, numParallelWorkers, &unconnect_count, &unconnection_node));

        RemoveDisConsumerHash(squeue->sq_key);

        if (g_DataPumpDebug)
        {
            elog(LOG, "Squeue %s: all parallel workers have finished work.",
                       sharedData->sq_key);
        }
    }
}

static bool
NodeStatusCheck(SharedQueue squeue, ParallelSendControl *parallelSendControl, int numLaunchedParallelWorkers)
{// #lizard forgives
    int i = 0;
    ParallelSendNodeControl  *nodes = parallelSendControl->nodes;
    ParallelSendSharedData *sharedData = parallelSendControl->sharedData;
    ParallelSendStatus *status = parallelSendControl->status;

    if (parallelSendControl->convertControl.cstatus != ConvertRunning &&
        parallelSendControl->convertControl.cstatus != ConvertExit)
    {
        elog(LOG, "convert thread status:%d abnormal, maxconn:%d, conn_num:%d, errno:%d", 
                                                        parallelSendControl->convertControl.cstatus, 
                                                        parallelSendControl->convertControl.maxConn, 
                                                        parallelSendControl->convertControl.connect_num, 
                                                        parallelSendControl->convertControl.errNO);
        sharedData->sender_error[0] = true;

        snprintf(squeue->err_msg, ERR_MSGSIZE, "[squeue %s producer %d]:convert thread status:%d abnormal, maxconn:%d, conn_num:%d, errno:%d",
                                      squeue->sq_key, squeue->sq_pid, parallelSendControl->convertControl.cstatus, 
                                                        parallelSendControl->convertControl.maxConn, 
                                                        parallelSendControl->convertControl.connect_num, 
                                                        parallelSendControl->convertControl.errNO);
        
        squeue->has_err_msg = true;
        
        return true;
    }

    for (i = 0; i < parallelSendControl->numNodes; i++)
    {
        spinlock_lock(&nodes[i].lock);

        if (nodes[i].status == DataPumpSndStatus_error)
        {
            spinlock_unlock(&nodes[i].lock);
            
            sharedData->sender_error[0] = true;

            elog(LOG, "node %d status abnormal, socket error, msg %s", nodes[i].nodeId, strerror(nodes[i].errorno));

            snprintf(squeue->err_msg, ERR_MSGSIZE, "[squeue %s producer %d]: node %d status abnormal, socket error, msg %s",
                                          squeue->sq_key, squeue->sq_pid, nodes[i].nodeId, strerror(nodes[i].errorno));

            squeue->has_err_msg = true;

            return true;
        }

        spinlock_unlock(&nodes[i].lock);
    }

    if (numLaunchedParallelWorkers)
    {
        for (i = 0; i < numLaunchedParallelWorkers; i++)
        {
            if (status[i] == ParallelSend_Error)
            {
                sharedData->sender_error[0] = true;
                
                elog(LOG, "[%s:%d]worker %d exit abnormally." , __FILE__, __LINE__, i);

                snprintf(squeue->err_msg, ERR_MSGSIZE, "[squeue %s producer %d]: worker %d exit abnormally",
                                                     squeue->sq_key, squeue->sq_pid, i);
                squeue->has_err_msg = true;
                
                return true;
            }
        }

        if (execute_error)
        {
            sharedData->sender_error[0] = true;
            
            elog(LOG, "some workers may not be launched or exit abnormally.");

            snprintf(squeue->err_msg, ERR_MSGSIZE, "[squeue %s producer %d]: some workers may not be launched or exit abnormally",
                                                     squeue->sq_key, squeue->sq_pid);

            squeue->has_err_msg = true;

            return true;
        }
    }

    if (squeue->sq_error)
    {
        sharedData->sender_error[0] = true;
        
        elog(LOG, "ParallelSendData: shared_queue %s error.", squeue->sq_key);

        snprintf(squeue->err_msg, ERR_MSGSIZE, "[squeue %s producer %d]: ParallelSendData: shared_queue %s error",
                                         squeue->sq_key, squeue->sq_pid, squeue->sq_key);
        
        squeue->has_err_msg = true;
        
        return true;
    }
    
    return false;
}

static void
ParallelSendCleanAll(ParallelSendControl *sender, int nodeid)
{
    ParallelSendCleanThread(sender);

    DestoryParallelSendControl(sender, nodeid);
}

static bool
ParallelWorkerExecDone(int numLaunchedParallelWorkers, ParallelSendStatus *status)
{
    int i = 0;
    int exec_done = 0;

    for (i = 0; i < numLaunchedParallelWorkers; i++)
    {
        if (status[i] >= ParallelSend_ExecDone)
        {
            exec_done++;
        }
    }

    if (exec_done == numLaunchedParallelWorkers)
    {
        return true;
    }
    else
    {
        return false;
    }
}

static bool
ParallelWorkerSendDone(int numLaunchedParallelWorkers, ParallelSendStatus *status)
{
    int i = 0;
    int exec_done = 0;

    for (i = 0; i < numLaunchedParallelWorkers; i++)
    {
        if (status[i] >= ParallelSend_SendDone)
        {
            exec_done++;
        }
    }

    if (exec_done == numLaunchedParallelWorkers)
    {
        return true;
    }
    else
    {
        return false;
    }
}

int
GetConsumerIdx(SharedQueue sq, int nodeid)
{
    return sq->nodeMap[nodeid];
}

bool
IsSqueueProducer(void)
{
    return (role == Squeue_Producer);
}

bool
IsSqueueConsumer(void)
{
    return (role == Squeue_Consumer);
}

void
SetSqueueError(void)
{
    if (share_sq)
    {
        share_sq->sq_error = true;
    }
}

void
SqueueProducerExit(void)
{
    if (share_sq)
    {
        if(!share_sq->sender_destroy && share_sq->sq_pid == MyProcPid)
        {
            if (!share_sq->parallelWorkerSendTuple)
            {
                DataPumpWaitSenderDone(share_sq->sender, false);
                DestoryDataPumpSenderControl(share_sq->sender, share_sq->sq_nodeid);
                share_sq->sender = NULL;
                share_sq->sender_destroy = true;
            }
            else
            {
                ParallelSendCleanThread(share_sq->parallelSendControl);
                DestoryParallelSendControl(share_sq->parallelSendControl, share_sq->sq_nodeid);
                share_sq->parallelSendControl = NULL;
                share_sq->sender_destroy = true;
            }
        }
        share_sq = NULL;
    }
}

void
ParallelSendEreport(void)
{
    if (senderstatus)
    {
        senderstatus[ParallelWorkerNumber] = ParallelSend_Error;
        senderstatus = NULL;
    }
}

void 
ParallelDsmDetach(void)
{
    if (dsm_seg)
    {
        senderstatus = NULL;
        dsm_detach(dsm_seg);
        dsm_seg = NULL;
    }
}
static bool
ConsumerExit(SharedQueue sq, ParallelSendControl *sender, int numParallelWorkers, 
                  int *count, Bitmapset **unconnect)
{// #lizard forgives
#define TIMEOUT (consumer_connect_timeout * 1000)
    int i;
    int node_num = sq->sq_nconsumers;
    bool all_done = true;
    bool *nodeSendDone = sender->nodeSendDone;
    SQueueSync *sqsync = sq->sq_sync;
    Bitmapset *unconnect_node = NULL;
    
    for (i = 0; i < node_num; i++)
    {
        bool done = false;
        int j = 0;
        int nWorkerDone = 0;
        int nBufferDone = 0;
        int index = 0;
        ConsState *cstate = &sq->sq_consumers[i];
        ParallelSendNodeControl *node = &sender->nodes[i];

        for (j = 0; j < numParallelWorkers; j++)
        {
            index = j * node_num + i;
            
            if (nodeSendDone[index])
            {
                nWorkerDone++;
            }
        }

        if (nWorkerDone == numParallelWorkers)
        {
            for (j = 0; j < numParallelWorkers; j++)
            {
                ParallelSendDataQueue *buffer = node->buffer[j];

                if (buffer->bufHead == buffer->bufTail)
                {
                    nBufferDone++;
                }
            }

            if (nBufferDone == numParallelWorkers)
            {
                done = true;
            }
        }

        if (done)
        {
            LWLockAcquire(sqsync->sqs_consumer_sync[i].cs_lwlock, LW_EXCLUSIVE);

            if (cstate->cs_status == CONSUMER_ACTIVE && cstate->cs_pid != 0)
            {
                cstate->cs_status = CONSUMER_EOF;
            }
            
            SetLatch(&sqsync->sqs_consumer_sync[i].cs_latch);

            LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
        }

        /* consumer do not need data any more, tell worker to discard all data to this consumer */
        LWLockAcquire(sqsync->sqs_consumer_sync[i].cs_lwlock, LW_SHARED);

        if (cstate->cs_status == CONSUMER_ACTIVE && cstate->cs_pid == 0)
        {
            unconnect_node = bms_add_member(unconnect_node, i);

            if (IsConsumerDisConnect(sq->sq_key, cstate->cs_node))
            {
                cstate->cs_status = CONSUMER_DONE;

                if (g_DataPumpDebug)
                {
                    elog(LOG, "ConsumerExit: squeue %s Set consumer %d done.", sq->sq_key, i);
                }
            }
        }

        if (cstate->cs_status == CONSUMER_DONE && node->status == DataPumpSndStatus_no_socket)
        {
            sender->nodeNoSocket[i] = true;
        }

        if (cstate->cs_status == CONSUMER_ACTIVE)
        {
            all_done = false;
        }

        LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
    }


    /* 
      *some consumers may went down with exception, and never inform us, we need
      * reset conncetion ourselves
      */
    if (unconnect_node && bms_subset_compare(unconnect_node, *unconnect) == BMS_EQUAL)
    {
        (*count)++;
        
        bms_free(unconnect_node);
    }
    else
    {
        (*count) = 0;
        
        bms_free(*unconnect);

        *unconnect = unconnect_node;
    }

    if (*count == TIMEOUT)
    {
        while((i = bms_first_member(*unconnect)) != -1)
        {
            ConsState *cstate = &sq->sq_consumers[i];

            snprintf(sq->err_msg, ERR_MSGSIZE, "[squeue %s producer %d]: set consumer %d done, because of unconnect",
                                               sq->sq_key, sq->sq_pid, i);

            sq->has_err_msg = true;

            LWLockAcquire(sqsync->sqs_consumer_sync[i].cs_lwlock, LW_EXCLUSIVE);

            cstate->cs_status = CONSUMER_DONE;

            LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);

            elog(LOG, "squeue %s Set consumer %d done, because of unconnect.", sq->sq_key, i);
        }

        (*count) = 0;

        bms_free(*unconnect);

        *unconnect = NULL;
    }

    return all_done;
}

void
SetParallelSendError(void)
{
    execute_error = true;
}

bool
GetParallelSendError(void)
{
    return execute_error;
}

/* sigusr2 handler */
void
RemoteSubplanSigusr2Handler(SIGNAL_ARGS)
{
    int            save_errno = errno;

    if (g_DataPumpDebug)
    {
        ereport(LOG,
                (errmsg("Pid %d got SIGUSR2 in RemoteSubplan: %m", MyProcPid)));
    }

    end_query_requested = true;

    errno = save_errno;
}

static void
ReportErrorConsumer(SharedQueue squeue)
{
    if (!squeue->has_err_msg)
    {
        StringInfoData    data;

        initStringInfo(&data);

        if (GetErrorMsg(&data))
        {
            snprintf(squeue->err_msg, ERR_MSGSIZE, "%s", data.data);

            squeue->has_err_msg = true;
        }

        pfree(data.data);
    }
}
#endif

#if 1
PGPipe* CreatePipe(uint32 size)
{
    PGPipe *pPipe = NULL;
    pPipe = palloc0( sizeof(PGPipe));
    pPipe->m_List = (void**)palloc0( sizeof(void*) * size);    
    pPipe->m_Length = size;            
    pPipe->m_Head   = 0;
    pPipe->m_Tail   = 0;
    spinlock_init(&(pPipe->m_lock));
    return pPipe;
}
void DestoryPipe(PGPipe *pPipe)
{
    if (pPipe)
    {
        pfree(pPipe->m_List);
        pfree(pPipe);
    }
}
/*从队列中取出一个单元使用，如果队列为空就返回空指针*/
void *PipeGet(PGPipe *pPipe)
{
    void *ptr = NULL;
    spinlock_lock(&(pPipe->m_lock));
    if (pPipe->m_Head == pPipe->m_Tail)
    {
        spinlock_unlock(&(pPipe->m_lock));
        return NULL;                
    }            
    ptr                             = pPipe->m_List[pPipe->m_Head];
    pPipe->m_List[pPipe->m_Head] = NULL;                
    pPipe->m_Head                   = (pPipe->m_Head  + 1) % pPipe->m_Length;  
    spinlock_unlock(&(pPipe->m_lock));
    return ptr;
}
/*往队列中放入一个对象，如果队列满就返回非零*/
int PipePut(PGPipe *pPipe, void *p)
{
    spinlock_lock(&(pPipe->m_lock));
    if ((pPipe->m_Tail + 1) % pPipe->m_Length == pPipe->m_Head)
    {
        spinlock_unlock(&(pPipe->m_lock));
        return -1;
    }
    pPipe->m_List[pPipe->m_Tail] = p;
    pPipe->m_Tail = (pPipe->m_Tail  + 1) % pPipe->m_Length;  
    spinlock_unlock(&(pPipe->m_lock));    
    return 0;
}
bool PipeIsFull(PGPipe *pPipe)
{
    spinlock_lock(&(pPipe->m_lock));
    if ((pPipe->m_Tail + 1) % pPipe->m_Length == pPipe->m_Head)
    {
        spinlock_unlock(&(pPipe->m_lock));
        return true;
    }
    else
    {
        spinlock_unlock(&(pPipe->m_lock));
        return false;
    }
}
bool IsEmpty(PGPipe *pPipe)
{
    spinlock_lock(&(pPipe->m_lock));
    if (pPipe->m_Tail == pPipe->m_Head)
    {
        spinlock_unlock(&(pPipe->m_lock));
        return true;
    }
    else
    {
        spinlock_unlock(&(pPipe->m_lock));
        return false;
    }
}
int PipeLength(PGPipe *pPipe)
{
    int len = -1;
    spinlock_lock(&(pPipe->m_lock));
    len = (pPipe->m_Tail - pPipe->m_Head + pPipe->m_Length) % pPipe->m_Length;
    spinlock_unlock(&(pPipe->m_lock));
    return len;
}

#endif

const char *
SqueueName(SharedQueue sq)
{
	return sq->sq_key;
}
