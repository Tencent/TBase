/*-------------------------------------------------------------------------
 *
 * freespace.h
 *      POSTGRES free space map for quickly finding free space in relations
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/freespace.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef FREESPACE_H_
#define FREESPACE_H_

#include "storage/block.h"
#include "storage/relfilenode.h"
#include "utils/relcache.h"

/* prototypes for public functions in freespace.c */
extern Size GetRecordedFreeSpace(Relation rel, BlockNumber heapBlk);
extern BlockNumber GetPageWithFreeSpace(Relation rel, Size spaceNeeded);
#ifdef _SHARDING_
extern BlockNumber GetPageWithFreeSpace_withshard(Relation rel, Size spaceNeeded, ShardID sid);
extern BlockNumber GetPageWithFreeSpace_fromextent(Relation rel, Size spaceNeeded, ExtentID eid);
#endif

extern BlockNumber RecordAndGetPageWithFreeSpace(Relation rel,
                              BlockNumber oldPage,
                              Size oldSpaceAvail,
                              Size spaceNeeded);
#ifdef _SHARDING_
extern BlockNumber RecordAndGetPageWithFreeSpace_extent(Relation rel, 
                              BlockNumber oldPage,
                              Size oldSpaceAvail,
                              Size spaceNeeded,
                              ShardID sid);
extern void RecordNewPageWithFullFreeSpace(Relation rel, BlockNumber heapBlk);
#endif
extern void RecordPageWithFreeSpace(Relation rel, BlockNumber heapBlk,
                        Size spaceAvail);
#ifdef _SHARDING_
extern void RecordPageWithFreeSpace_extent(Relation rel, ShardID sid, 
                            BlockNumber heapBlk, Size spaceAvail);

#endif
extern void XLogRecordPageWithFreeSpace_extent(RelFileNode rnode, BlockNumber heapBlk,
                            Size spaceAvail, bool hasExtent);
#ifdef _SHARDING_
#define XLogRecordPageWithFreeSpace(rnode, heapBlk, spaceAvail) \
    XLogRecordPageWithFreeSpace_extent(rnode, heapBlk, spaceAvail, false);
uint8 GetMaxAvailWithExtent(Relation rel, ExtentID eid);
#endif

extern void FreeSpaceMapTruncateRel(Relation rel, BlockNumber nblocks);
extern void FreeSpaceMapVacuum(Relation rel);
extern void UpdateFreeSpaceMap(Relation rel,
                   BlockNumber startBlkNum,
                   BlockNumber endBlkNum,
                   Size freespace);

#endif                            /* FREESPACE_H_ */
