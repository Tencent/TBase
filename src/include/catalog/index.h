/*
 * Tencent is pleased to support the open source community by making TBase available.  
 * 
 * Copyright (C) 2019 THL A29 Limited, a Tencent company.  All rights reserved.
 * 
 * TBase is licensed under the BSD 3-Clause License, except for the third-party component listed below. 
 * 
 * A copy of the BSD 3-Clause License is included in this file.
 * 
 * Other dependencies and licenses:
 * 
 * Open Source Software Licensed Under the PostgreSQL License: 
 * --------------------------------------------------------------------
 * 1. Postgres-XL XL9_5_STABLE
 * Portions Copyright (c) 2015-2016, 2ndQuadrant Ltd
 * Portions Copyright (c) 2012-2015, TransLattice, Inc.
 * Portions Copyright (c) 2010-2017, Postgres-XC Development Group
 * Portions Copyright (c) 1996-2015, The PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, The Regents of the University of California
 * 
 * Terms of the PostgreSQL License: 
 * --------------------------------------------------------------------
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written agreement
 * is hereby granted, provided that the above copyright notice and this
 * paragraph and the following two paragraphs appear in all copies.
 * 
 * IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
 * DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * 
 * THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
 * ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 * 
 * 
 * Terms of the BSD 3-Clause License:
 * --------------------------------------------------------------------
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 * 
 * 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation 
 * and/or other materials provided with the distribution.
 * 
 * 3. Neither the name of THL A29 Limited nor the names of its contributors may be used to endorse or promote products derived from this software without 
 * specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, 
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS 
 * BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE 
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT 
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH 
 * DAMAGE.
 * 
 */
/*-------------------------------------------------------------------------
 *
 * index.h
 *      prototypes for catalog/index.c.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/index.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef INDEX_H
#define INDEX_H

#include "catalog/objectaddress.h"
#include "nodes/execnodes.h"


#define DEFAULT_INDEX_TYPE    "btree"

/* Typedef for callback function for IndexBuildHeapScan */
typedef void (*IndexBuildCallback) (Relation index,
                                    HeapTuple htup,
                                    Datum *values,
                                    bool *isnull,
                                    bool tupleIsAlive,
                                    void *state);

/* Action code for index_set_state_flags */
typedef enum
{
    INDEX_CREATE_SET_READY,
    INDEX_CREATE_SET_VALID,
    INDEX_DROP_CLEAR_VALID,
    INDEX_DROP_SET_DEAD
} IndexStateFlagsAction;


extern void index_check_primary_key(Relation heapRel,
                        IndexInfo *indexInfo,
						bool is_alter_table,
                                                IndexStmt *stmt);

#define    INDEX_CREATE_IS_PRIMARY             (1 << 0)
#define    INDEX_CREATE_ADD_CONSTRAINT         (1 << 1)
#define    INDEX_CREATE_SKIP_BUILD             (1 << 2)
#define    INDEX_CREATE_CONCURRENT             (1 << 3)
#define    INDEX_CREATE_IF_NOT_EXISTS          (1 << 4)
#define	INDEX_CREATE_PARTITIONED			(1 << 5)
#define INDEX_CREATE_INVALID				(1 << 6)

extern Oid index_create(Relation heapRelation,
             const char *indexRelationName,
             Oid indexRelationId,
			 Oid parentIndexRelid,
			 Oid parentConstraintId,
             Oid relFileNode,
             IndexInfo *indexInfo,
             List *indexColNames,
             Oid accessMethodObjectId,
             Oid tableSpaceId,
             Oid *collationObjectId,
             Oid *classObjectId,
             int16 *coloptions,
             Datum reloptions,
			 bits16 flags,
			 bits16 constr_flags,
             bool allow_system_table_mods,
			 bool is_internal,
			 Oid *constraintId);

#define	INDEX_CONSTR_CREATE_MARK_AS_PRIMARY	(1 << 0)
#define	INDEX_CONSTR_CREATE_DEFERRABLE		(1 << 1)
#define	INDEX_CONSTR_CREATE_INIT_DEFERRED	(1 << 2)
#define	INDEX_CONSTR_CREATE_UPDATE_INDEX	(1 << 3)
#define	INDEX_CONSTR_CREATE_REMOVE_OLD_DEPS	(1 << 4)

extern ObjectAddress index_constraint_create(Relation heapRelation,
                        Oid indexRelationId,
						Oid parentConstraintId,
                        IndexInfo *indexInfo,
                        const char *constraintName,
                        char constraintType,
						bits16 constr_flags,
                        bool allow_system_table_mods,
                        bool is_internal);

extern void index_drop(Oid indexId, bool concurrent);

extern IndexInfo *BuildIndexInfo(Relation index);

extern bool CompareIndexInfo(IndexInfo *info1, IndexInfo *info2,
				 Oid *collations1, Oid *collations2,
				 Oid *opfamilies1, Oid *opfamilies2,
				 AttrNumber *attmap, int maplen);

extern void BuildSpeculativeIndexInfo(Relation index, IndexInfo *ii);

extern void FormIndexDatum(IndexInfo *indexInfo,
               TupleTableSlot *slot,
               EState *estate,
               Datum *values,
               bool *isnull);

extern void index_build(Relation heapRelation,
            Relation indexRelation,
            IndexInfo *indexInfo,
            bool isprimary,
            bool isreindex);

extern double IndexBuildHeapScan(Relation heapRelation,
                   Relation indexRelation,
                   IndexInfo *indexInfo,
                   bool allow_sync,
                   IndexBuildCallback callback,
                   void *callback_state);
extern double IndexBuildHeapRangeScan(Relation heapRelation,
                        Relation indexRelation,
                        IndexInfo *indexInfo,
                        bool allow_sync,
                        bool anyvisible,
                        BlockNumber start_blockno,
                        BlockNumber end_blockno,
                        IndexBuildCallback callback,
                        void *callback_state);

extern void validate_index(Oid heapId, Oid indexId, Snapshot snapshot);

extern void index_set_state_flags(Oid indexId, IndexStateFlagsAction action);

extern void reindex_index(Oid indexId, bool skip_constraint_checks,
              char relpersistence, int options);

/* Flag bits for reindex_relation(): */
#define REINDEX_REL_PROCESS_TOAST            0x01
#define REINDEX_REL_SUPPRESS_INDEX_USE        0x02
#define REINDEX_REL_CHECK_CONSTRAINTS        0x04
#define REINDEX_REL_FORCE_INDEXES_UNLOGGED    0x08
#define REINDEX_REL_FORCE_INDEXES_PERMANENT 0x10

extern bool reindex_relation(Oid relid, int flags, int options);

extern bool ReindexIsProcessingHeap(Oid heapOid);
extern bool ReindexIsProcessingIndex(Oid indexOid);
extern Oid    IndexGetRelation(Oid indexId, bool missing_ok);

#ifdef __TBASE__
extern bool index_is_interval(Oid indexId);
#endif

extern void IndexSetParentIndex(Relation idx, Oid parentOid);
extern void IndexCreateSetValid(Oid index, Oid rel);

#endif                            /* INDEX_H */
