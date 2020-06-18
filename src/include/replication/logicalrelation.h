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
 * logicalrelation.h
 *      Relation definitions for logical replication relation mapping.
 *
 * Portions Copyright (c) 2016-2017, PostgreSQL Global Development Group
 *
 * src/include/replication/logicalrelation.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOGICALRELATION_H
#define LOGICALRELATION_H

#include "replication/logicalproto.h"
#ifdef __SUBSCRIPTION__
#include "pgxc/locator.h"
#endif

typedef struct LogicalRepRelMapEntry
{
    LogicalRepRelation remoterel;    /* key is remoterel.remoteid */

    /* Mapping to local relation, filled as needed. */
    Oid            localreloid;    /* local relation id */
    Relation    localrel;        /* relcache entry */
    AttrNumber *attrmap;        /* map of local attributes to remote ones */
    bool        updatable;        /* Can apply updates/deletes? */

    /* Sync state. */
    char        state;
    XLogRecPtr    statelsn;
#ifdef __STORAGE_SCALABLE__
    uint64      ntups_insert;
    uint64      ntups_delete;
    uint64      checksum_insert;
    uint64      checksum_delete;
    void        *ent;
#endif
#ifdef __SUBSCRIPTION__
    Locator        *locator;        /* the locator object */
#endif
} LogicalRepRelMapEntry;

extern void logicalrep_relmap_update(LogicalRepRelation *remoterel);

extern LogicalRepRelMapEntry *logicalrep_rel_open(LogicalRepRelId remoteid,
                    LOCKMODE lockmode);
extern void logicalrep_rel_close(LogicalRepRelMapEntry *rel,
                     LOCKMODE lockmode);

extern void logicalrep_typmap_update(LogicalRepTyp *remotetyp);
extern Oid    logicalrep_typmap_getid(Oid remoteid);

#ifdef __STORAGE_SCALABLE__
extern void logicalrep_statis_update_for_sync(Oid relid, Oid subid, char *subname);

extern void logicalrep_statis_update_for_apply(Oid subid, char *subname);
#endif

#ifdef __SUBSCRIPTION__
extern Oid GetRelationIdentityOrPK(Relation rel);

extern void logicl_apply_set_ignor_pk_conflict(bool ignore);
extern void logicl_aply_rset_ignor_pk_conflict(void);
extern bool logical_apply_ignore_pk_conflict(void);

extern void TbaseSubscriptionApplyWorkerReset(void);
extern void TbaseSubscriptionApplyWorkerSet(void);
extern bool AmTbaseSubscriptionApplyWorker(void);
#endif

#endif                            /* LOGICALRELATION_H */
