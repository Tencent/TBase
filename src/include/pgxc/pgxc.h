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
 * pgxc.h
 *        Postgres-XC flags and connection control information
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2011  PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/pgxc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGXC_H
#define PGXC_H

#include "postgres.h"
#include "nodes/pg_list.h"

extern bool isPGXCCoordinator;
extern bool isPGXCDataNode;
extern bool isRestoreMode;
extern char *parentPGXCNode;
extern int parentPGXCPid;
extern int    parentPGXCNodeId;
extern char    parentPGXCNodeType;

typedef enum
{
    REMOTE_CONN_APP,
    REMOTE_CONN_COORD,
    REMOTE_CONN_DATANODE,
    REMOTE_CONN_GTM,
    REMOTE_CONN_GTM_PROXY
} RemoteConnTypes;

/* Determine remote connection type for a PGXC backend */
extern int		remoteConnType;

/* Local node name and numer */
extern char    *PGXCNodeName;
extern int    PGXCNodeId;
extern bool IsPGXCMainCluster;
extern uint32    PGXCNodeIdentifier;
extern char *PGXCClusterName;
extern char *PGXCMainClusterName;
extern char *PGXCDefaultClusterName;
#ifdef __TBASE__
extern char PGXCSessionId[NAMEDATALEN];
extern int PGXCLevelId;
extern List *PGXCGroupNodeList;
#endif


extern Datum xc_lockForBackupKey1;
extern Datum xc_lockForBackupKey2;

#define IS_PGXC_COORDINATOR isPGXCCoordinator
#define IS_PGXC_DATANODE isPGXCDataNode

#define IS_PGXC_LOCAL_COORDINATOR    \
    (IS_PGXC_COORDINATOR && !IsConnFromCoord())
#define IS_PGXC_REMOTE_COORDINATOR    \
    (IS_PGXC_COORDINATOR && IsConnFromCoord())
#define IS_PGXC_MAINCLUSTER_SLAVENODE \
    (IsPGXCMainCluster && RecoveryInProgress())

#define PGXC_PARENT_NODE parentPGXCNode
#define PGXC_PARENT_NODE_ID    parentPGXCNodeId
#define PGXC_PARENT_NODE_TYPE    parentPGXCNodeType
#define REMOTE_CONN_TYPE remoteConnType

#define IsConnFromApp() (remoteConnType == REMOTE_CONN_APP)
#define IsConnFromCoord() (remoteConnType == REMOTE_CONN_COORD)
#define IsConnFromDatanode() (remoteConnType == REMOTE_CONN_DATANODE)
#define IsConnFromGtm() (remoteConnType == REMOTE_CONN_GTM)
#define IsConnFromGtmProxy() (remoteConnType == REMOTE_CONN_GTM_PROXY)

/* key pair to be used as object id while using advisory lock for backup */
#define XC_LOCK_FOR_BACKUP_KEY_1      0xFFFF
#define XC_LOCK_FOR_BACKUP_KEY_2      0xFFFF

#endif   /* PGXC */
