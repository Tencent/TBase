/*-------------------------------------------------------------------------
 *
 * hio.h
 *      POSTGRES heap access method input/output definitions.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/hio.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HIO_H
#define HIO_H

#include "access/heapam.h"
#include "access/htup.h"
#include "utils/relcache.h"
#include "storage/buf.h"


/*
 * state for bulk inserts --- private to heapam.c and hio.c
 *
 * If current_buf isn't InvalidBuffer, then we are holding an extra pin
 * on that buffer.
 *
 * "typedef struct BulkInsertStateData *BulkInsertState" is in heapam.h
 */
typedef struct BulkInsertStateData
{
    BufferAccessStrategy strategy;    /* our BULKWRITE strategy object */
    Buffer        current_buf;    /* current insertion target page */
#ifdef _SHARDING_
    ShardID        sid;
#endif
}            BulkInsertStateData;


extern void RelationPutHeapTuple(Relation relation, Buffer buffer,
                     HeapTuple tuple, bool token);

#define RelationGetBufferForTuple(relation, len, otherBuffer, options, bistate, vmbuffer, vmbuffer_other) \
    RelationGetBufferForTuple_shard(relation, InvalidShardID, len, \
                          otherBuffer, options, bistate, \
                          vmbuffer, vmbuffer_other);

extern Buffer RelationGetBufferForTuple_shard(Relation relation, ShardID sid, Size len,
                          Buffer otherBuffer, int options,
                          BulkInsertState bistate,
                          Buffer *vmbuffer, Buffer *vmbuffer_other);
#ifdef _SHARDING_
extern void RelationExtendHeapForRedo(RelFileNode rnode, ExtentID eid, ShardID sid);
#endif

#endif                            /* HIO_H */
