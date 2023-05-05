/*-------------------------------------------------------------------------
 *
 * barrier.h
 *
 *	  Definitions for the shared queue handling
 *
 *
 * Copyright (c) 2012-2014, TransLattice, Inc.
 *
 * IDENTIFICATION
 *	  $$
 *
 *-------------------------------------------------------------------------
 */

#ifndef SQUEUE_H
#define SQUEUE_H

#include "postgres.h"
#include "executor/tuptable.h"
#include "nodes/pg_list.h"
#include "utils/tuplestore.h"
#ifdef __TBASE__
#include "tcop/dest.h"
#include "storage/dsm_impl.h"
#endif

#ifdef __TBASE__
#include "storage/s_lock.h"
#include "storage/spin.h"
#include <signal.h>
#endif
extern PGDLLIMPORT int NSQueues;
extern PGDLLIMPORT int SQueueSize;

/* Fixed size of shared queue, maybe need to be GUC configurable */
#define SQUEUE_SIZE ((long) SQueueSize * TBASE_MAX_DATANODE_NUMBER * 1024L)
/* Number of shared queues, maybe need to be GUC configurable */
#ifndef __TBASE__
#define NUM_SQUEUES Max((long) NSQueues, MaxConnections / 4)
#else
#define NUM_SQUEUES NSQueues
#endif

#define SQUEUE_KEYSIZE (64)

#define SQ_CONS_SELF -1
#define SQ_CONS_NONE -2
#define SQ_CONS_INIT -3

#ifdef __TBASE__
#define WORD_IN_LONGLONG        2
#define BITS_IN_BYTE            8
#define BITS_IN_WORD            32
#define BITS_IN_LONGLONG        64
#define MAX_UINT8               0XFF
#define MAX_UINT32              0XFFFFFFFF
#define MAX_UINT64              (~(uint64)(0))
#define ERR_MSGSIZE             (256)
#endif

typedef struct SQueueHeader *SharedQueue;

#ifdef __TBASE__
typedef struct DataPumpSenderControl* DataPumpSender;
typedef struct ParallelSendControl* ParallelSender;
#endif

extern Size SharedQueueShmemSize(void);
extern void SharedQueuesInit(void);
#ifdef __TBASE__
extern void SharedQueueAcquire(const char *sqname, int ncons, bool parallelSend, int numParallelWorkers, bool with_params);
#else
extern void SharedQueueAcquire(const char *sqname, int ncons);
#endif
extern SharedQueue SharedQueueBind(const char *sqname, List *consNodes,
								   List *distNodes, int *myindex, int *consMap
#ifdef __TBASE__
								   ,
									DataPumpSender *sender
#endif
									);
extern void SharedQueueUnBind(SharedQueue squeue, bool failed);
extern void SharedQueueRelease(const char *sqname);
extern void SharedQueuesCleanup(int code, Datum arg);

extern int	SharedQueueFinish(SharedQueue squeue, TupleDesc tupDesc,
				  Tuplestorestate **tuplestore);

extern void SharedQueueWrite(SharedQueue squeue, int consumerIdx,
				 TupleTableSlot *slot, Tuplestorestate **tuplestore,
				 MemoryContext tmpcxt);
extern bool SharedQueueRead(SharedQueue squeue, int consumerIdx,
				TupleTableSlot *slot, bool canwait);
extern void SharedQueueDisconnectConsumer(const char *sqname);
extern void SharedQueueReset(SharedQueue squeue, int consumerIdx);
extern void SharedQueueResetNotConnected(SharedQueue squeue);
extern bool SharedQueueCanPause(SharedQueue squeue);
extern bool SharedQueueWaitOnProducerLatch(SharedQueue squeue, long timeout);
#ifdef __TBASE__
typedef enum 
{ 
	DataPumpOK						       = 0,
	DataPumpSndError_no_socket             = -1, 
	DataPumpSndError_no_space              = -2, 
	DataPumpSndError_io_error 	           = -3,
	DataPumpSndError_node_error            = -4,
	DataPumpSndError_bad_status      	   = -5,
	DataPumpSndError_unreachable_node      = -6,
	DataPumpConvert_error             	   = -7
}DataPumpSndError;

#define  DATAPUMP_UNREACHABLE_NODE_FD     (-2)

typedef enum
{
	ConvertInit,
	ConvertRunning,
	ConvertListenError,
	ConvertAcceptError,
	ConvertRecvNodeidError,
	ConvertRecvNodeindexError,
	ConvertRecvSockfdError,
	ConvertSetSockfdError,
	ConvertExit
}ConvertStatus;

typedef enum 
{
	Squeue_Consumer,
	Squeue_Producer,
	Squeue_None
} SqueueRole;

extern bool IsSqueueProducer(void);

extern bool IsSqueueConsumer(void);

extern void SetSqueueError(void);

extern void SqueueProducerExit(void);

#define ALIGN_UP(a, b)   (((a) + (b) - 1)/(b)) * (b)
#define ALIGN_DOWN(a, b) (((a))/(b)) * (b)
#define DIVIDE_UP(a, b)   (((a) + (b) - 1)/(b))
#define DIVIDE_DOWN(a, b)   (((a))/(b)) 
extern bool  g_UseDataPump;
extern bool  g_DataPumpDebug;
extern int32 g_SndThreadNum;
extern int32 g_SndThreadBufferSize;
extern int32 g_SndBatchSize;
extern int   consumer_connect_timeout;
extern int   g_DisConsumer_timeout;

extern bool in_data_pump;

extern volatile sig_atomic_t end_query_requested;

extern DataPumpSender BuildDataPumpSenderControl(SharedQueue sq);
extern int32  DataPumpSendDataRow(void *sender, int32 nodeindex, int32 nodeId,  char *data, size_t len);
extern int32  DataPumpSetNodeSocket(void *sender, int32 nodeindex, int32 nodeId,  int32 socket);
/*
 * To finish the cursor. Step 1:DataPumpWaitSenderDone, Step 2:DestoryDataPumpSenderControl
 */
extern bool   DataPumpWaitSenderDone(void *sndctl, bool error);
extern void   DestoryDataPumpSenderControl (void* sndctl, int nodeid);

extern void SendDataRemote(SharedQueue squeue, int32 consumerIdx, TupleTableSlot *slot, Tuplestorestate **tuplestore, MemoryContext tmpcxt);

extern void create_datapump_socket_dir(void);

extern bool needParallelSend(SharedQueue squeue);
extern void SetLocatorInfo(SharedQueue squeue, int *consMap, int len, char distributionType, Oid keytype, AttrNumber distributionKey);

extern DestReceiver *GetParallelSendReceiver(dsm_handle handle);
extern dsm_handle GetParallelSendSegHandle(void);

extern void WaitForParallelWorkerDone(int numParallelWorkers, bool launch_failed);

extern int GetConsumerIdx(SharedQueue sq, int nodeid);

extern void ParallelSendEreport(void);

extern void ParallelDsmDetach(void);

extern void SetParallelSendError(void);

extern bool GetParallelSendError(void);

extern void SetDisConnectConsumer(const char *sqname, int cons);

extern bool IsConsumerDisConnect(char *sqname, int nodeid);

extern void RemoveDisConsumerHash(char *sqname);

extern void RemoteSubplanSigusr2Handler(SIGNAL_ARGS);
#ifdef __TBASE__
enum MT_thr_detach 
{ 
	MT_THR_JOINABLE, 
	MT_THR_DETACHED 
};

typedef struct
{
	int             m_cnt;
	pthread_mutex_t m_mutex;
	pthread_cond_t  m_cond;
}ThreadSema;

extern void ThreadSemaInit(ThreadSema *sema, int32 init);
extern void ThreadSemaDown(ThreadSema *sema);
extern void ThreadSemaUp(ThreadSema *sema);


typedef struct 
{
	void                 **m_List; /*循环队列数组*/
	uint32               m_Length; /*队列队列长度*/
	slock_t              m_lock;   /*保护下面的两个变量*/
	volatile uint32      m_Head;   /*队列头部，数据插入往头部插入，头部加一等于尾则队列满*/
	volatile uint32      m_Tail;   /*队列尾部，尾部等于头部，则队列为空*/
}PGPipe;
extern PGPipe* CreatePipe(uint32 size);
extern void    DestoryPipe(PGPipe *pPipe);
extern void    *PipeGet(PGPipe *pPipe);
extern int     PipePut(PGPipe *pPipe, void *p);
extern bool    PipeIsFull(PGPipe *pPipe);
extern bool    IsEmpty(PGPipe *pPipe);
extern int 	   PipeLength(PGPipe *pPipe);

extern int32 CreateThread(void *(*f) (void *), void *arg, int32 mode);

extern const char *SqueueName(SharedQueue sq);

#endif


#endif
#endif
