/*-------------------------------------------------------------------------
 *
 * producerReceiver.c
 *      An implementation of DestReceiver that distributes the result tuples to
 *      multiple customers via a SharedQueue.
 *
 *
 * Copyright (c) 2012-2014, TransLattice, Inc.
 *
 * IDENTIFICATION
 *      src/backend/executor/producerReceiver.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "executor/producerReceiver.h"
#include "pgxc/nodemgr.h"
#include "tcop/pquery.h"
#include "utils/tuplestore.h"
#include "utils/timestamp.h"
#include "postmaster/postmaster.h"

typedef struct
{
    DestReceiver pub;
    /* parameters: */
    DestReceiver *consumer;            /* where to put the tuples for self */
    AttrNumber distKey;                /* distribution key attribute in the tuple */
    Locator *locator;                /* locator is determining destination nodes */
    int *distNodes;                    /* array where to get locator results */
    int *consMap;                    /* map of consumers: consMap[node-1] indicates
                                     * the target consumer */
    SharedQueue squeue;                /* a SharedQueue for result distribution */
#ifdef __TBASE__
    DataPumpSender sender;             /* used to send data locally, could be NULL */
    int16 *nodeMap;
#endif
    MemoryContext tmpcxt;           /* holds temporary data */
    Tuplestorestate **tstores;        /* storage to buffer data if destination queue
                                      * is full */
    TupleDesc typeinfo;                /* description of received tuples */
    long tcount;
    long selfcount;
    long othercount;
#ifdef __TBASE__
    uint64      send_tuples;        /* number of tuples sent to remote */
    TimestampTz send_total_time;    /* total time to send tuples */
#endif
} ProducerState;


/*
 * Prepare to receive tuples from executor.
 */
static void
producerStartupReceiver(DestReceiver *self, int operation, TupleDesc typeinfo)
{
    ProducerState *myState = (ProducerState *) self;

    if (ActivePortal)
    {
        /* Normally ExecutorContext is current here. However we should better
         * create local producer storage in the Portal's context: producer
         * may keep pushing records to consumers after executor is destroyed.
         */
        MemoryContext savecontext;
        savecontext = MemoryContextSwitchTo(PortalGetHeapMemory(ActivePortal));
        if (myState->typeinfo)
            pfree(myState->typeinfo);
        myState->typeinfo = CreateTupleDescCopy(typeinfo);
        MemoryContextSwitchTo(savecontext);
    }
    else
        myState->typeinfo = typeinfo;

    if (myState->consumer)
        (*myState->consumer->rStartup) (myState->consumer, operation, typeinfo);
}

/*
 * Receive a tuple from the executor and dispatch it to the proper consumer
 */
static bool
producerReceiveSlot(TupleTableSlot *slot, DestReceiver *self)
{// #lizard forgives
    ProducerState *myState = (ProducerState *) self;
    Datum        value;
    bool        isnull;
    int         ncount, i;

    if (myState->distKey == InvalidAttrNumber)
    {
        value = (Datum) 0;
        isnull = true;
    }
    else
        value = slot_getattr(slot, myState->distKey, &isnull);
#ifdef __COLD_HOT__
    ncount = GET_NODES(myState->locator, value, isnull, 0, true, NULL);
#else
    ncount = GET_NODES(myState->locator, value, isnull, NULL);
#endif
    myState->tcount++;
    /* Dispatch the tuple */
    for (i = 0; i < ncount; i++)
    {
        int consumerIdx;

        char locatorType = getLocatorDisType(myState->locator);

        if ('S' == locatorType)
        {
            int nodeid = myState->distNodes[i];

            Assert(nodeid < MAX_NODES_NUMBER);

            /* data distributed by share queue */
            if (myState->squeue)
            {
                consumerIdx = GetConsumerIdx(myState->squeue, nodeid);
            }
            else /* data replication */
            {
                consumerIdx = myState->nodeMap[nodeid];
            }
            
            if (SQ_CONS_INIT == consumerIdx)
            {
                elog(ERROR, "Invalid nodeid %d in producerReceiveSlot.", nodeid);
            }
        }
        else
        {
            consumerIdx = myState->distNodes[i];
        }

        if (consumerIdx == SQ_CONS_NONE)
        {
            continue;
        }
        else if (consumerIdx == SQ_CONS_SELF)
        {
            Assert(myState->consumer);
            (*myState->consumer->receiveSlot) (slot, myState->consumer);
            myState->selfcount++;
        }
        else if (myState->squeue)
        {
            /*
             * If the tuple will not fit to the consumer queue it will be stored
             * in the local tuplestore. The tuplestore should be in the portal
             * context, because ExecutorContext may be destroyed when tuples
             * are not yet pushed to the consumer queue.
             */
            MemoryContext savecontext;
            Assert(ActivePortal);
            savecontext = MemoryContextSwitchTo(PortalGetHeapMemory(ActivePortal));
            if (g_UseDataPump)
            {
                TimestampTz begin = 0;
                TimestampTz end   = 0;

                if (enable_statistic)
                {
                    begin = GetCurrentTimestamp();
                }
                
                SendDataRemote(myState->squeue, consumerIdx, slot, 
                                                            &myState->tstores[consumerIdx], 
                                                            myState->tmpcxt);

                if (enable_statistic)
                {
                    end   = GetCurrentTimestamp();

                    myState->send_tuples++;
                    myState->send_total_time += (end - begin);
                }
            }
            else
            {
                SharedQueueWrite(myState->squeue, consumerIdx, slot,
                                 &myState->tstores[consumerIdx], myState->tmpcxt);
            }
            MemoryContextSwitchTo(savecontext);
            myState->othercount++;
        }
    }

    return true;
}


/*
 * Clean up at end of an executor run
 */
static void
producerShutdownReceiver(DestReceiver *self)
{
    ProducerState *myState = (ProducerState *) self;

#ifdef __TBASE__
    if (enable_statistic)
    {
        elog(LOG, "ProducerSend: send_tuples:%lu, send_total_time:%ld, avg_time:%lf.",
                   myState->send_tuples, myState->send_total_time,
                   ((double)myState->send_total_time) / ((double)myState->send_tuples));
    }
#endif

    if (myState->consumer)
        (*myState->consumer->rShutdown) (myState->consumer);
}


/*
 * Destroy receiver when done with it
 */
static void
producerDestroyReceiver(DestReceiver *self)
{// #lizard forgives
    ProducerState *myState = (ProducerState *) self;

    elog(DEBUG2, "Producer stats: total %ld tuples, %ld tuples to self, %ld to other nodes",
         myState->tcount, myState->selfcount, myState->othercount);

    if (myState->consumer)
    {
        (*myState->consumer->rDestroy) (myState->consumer);
        myState->consumer = NULL;
    }

#ifdef __TBASE__
    if (myState->nodeMap)
    {
        pfree(myState->nodeMap);
        myState->nodeMap = NULL;
    }
#endif

    /* Make sure all data are in the squeue */
    while (myState->tstores)
    {
        CHECK_FOR_INTERRUPTS();

        if (SharedQueueFinish(myState->squeue, myState->typeinfo,
                              myState->tstores) == 0)
        {
            elog(DEBUG3, "SharedQueueFinish returned 0 - freeing tstores");
            pfree(myState->tstores);
            myState->tstores = NULL;
        }
        else
        {
            if (SharedQueueWaitOnProducerLatch(myState->squeue, 10000L))
                /*
                 * Do not wait for consumers that was not even connected after
                 * 10 seconds after start waiting for their disconnection.
                 * That should help to break the loop which would otherwise
                 * endless.  The error will be emitted later in
                 * SharedQueueUnBind
                 */
                SharedQueueResetNotConnected(myState->squeue);
        }
    }

    /* wait while consumer are finishing and release shared resources */
    if (myState->squeue)
        SharedQueueUnBind(myState->squeue, false);
    myState->squeue = NULL;

    /* Release workspace if any */
    if (myState->locator)
        freeLocator(myState->locator);
    pfree(myState);
}


/*
 * Initially create a DestReceiver object.
 */
DestReceiver *
CreateProducerDestReceiver(void)
{
    ProducerState *self = (ProducerState *) palloc0(sizeof(ProducerState));

    self->pub.receiveSlot = producerReceiveSlot;
    self->pub.rStartup = producerStartupReceiver;
    self->pub.rShutdown = producerShutdownReceiver;
    self->pub.rDestroy = producerDestroyReceiver;
    self->pub.mydest = DestProducer;

    /* private fields will be set by SetTuplestoreDestReceiverParams */
    self->tcount = 0;
    self->selfcount = 0;
    self->othercount = 0;
#ifdef __TBASE__
    self->send_tuples     = 0;
    self->send_total_time = 0;
    self->nodeMap = NULL;
#endif

    return (DestReceiver *) self;
}


/*
 * Set parameters for a ProducerDestReceiver
 */
void
SetProducerDestReceiverParams(DestReceiver *self,
                              AttrNumber distKey,
                              Locator *locator,
                              SharedQueue squeue
#ifdef __TBASE__                              
                                ,
                              DataPumpSender sender
#endif
                              )
{
    ProducerState *myState = (ProducerState *) self;

    Assert(myState->pub.mydest == DestProducer);
    myState->distKey = distKey;
    myState->locator = locator;
    myState->squeue = squeue;
#ifdef __TBASE__
    myState->sender = sender;
#endif
    myState->typeinfo = NULL;
    myState->tmpcxt = NULL;
    /* Create workspace */
    myState->distNodes = (int *) getLocatorResults(locator);
    if (squeue)
#ifdef __TBASE__
        myState->tstores = (Tuplestorestate **)
            palloc0(getLocatorNodeCount(locator) * sizeof(Tuplestorestate *));
#else
        myState->tstores = (Tuplestorestate **)
            palloc0(NumDataNodes * sizeof(Tuplestorestate *));
#endif
}


/*
 * Set a DestReceiver to receive tuples targeted to "self".
 * Returns old value of the self consumer
 */
DestReceiver *
SetSelfConsumerDestReceiver(DestReceiver *self,
                            DestReceiver *consumer)
{
    ProducerState *myState = (ProducerState *) self;
    DestReceiver *oldconsumer;

    Assert(myState->pub.mydest == DestProducer);
    oldconsumer = myState->consumer;
    myState->consumer = consumer;
    return oldconsumer;
}


/*
 * Set a memory context to hold temporary data
 */
void
SetProducerTempMemory(DestReceiver *self, MemoryContext tmpcxt)
{
    ProducerState *myState = (ProducerState *) self;

    Assert(myState->pub.mydest == DestProducer);
    myState->tmpcxt = tmpcxt;
}


/*
 * Push data from the local tuplestores to the shared memory so consumers can
 * read them. Returns true if all data are pushed, false if something remains
 * in the tuplestores yet.
 */
bool
ProducerReceiverPushBuffers(DestReceiver *self)
{
    ProducerState *myState = (ProducerState *) self;

    Assert(myState->pub.mydest == DestProducer);
    if (myState->tstores)
    {
        if (SharedQueueFinish(myState->squeue, myState->typeinfo,
                              myState->tstores) == 0)
        {
            elog(DEBUG3, "SharedQueueFinish returned 0, freeing tstores");
            pfree(myState->tstores);
            myState->tstores = NULL;
        }
        else
        {
            elog(DEBUG3, "SharedQueueFinish returned non-zero value");
            return false;
        }
    }
    return true;
}
#ifdef __TBASE__
void
SetProducerNodeMap(DestReceiver *self, int16 *nodemap)
{
    ProducerState *myState = (ProducerState *) self;

    myState->nodeMap = (int16 *)palloc0(sizeof(int16) * MAX_NODES_NUMBER);

    memcpy(myState->nodeMap, nodemap, sizeof(int16) * MAX_NODES_NUMBER);
}
#endif
