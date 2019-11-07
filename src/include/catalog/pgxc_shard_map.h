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
#ifndef PGXC_SHARD_MAP_H
#define PGXC_SHARD_MAP_H

#include "catalog/genbki.h"
/* oid */
#define PgxcShardMapRelationId  9023

CATALOG(pgxc_shard_map,9023) BKI_WITHOUT_OIDS BKI_SHARED_RELATION
{
    Oid     disgroup;
    int32    shardgroupid;
    int16    ncopy;    
    Oid        primarycopy;    /* data node OID */
    Oid        copy1;
    Oid        copy2;
    char    primarystatus;    
    char    status1;
    char    status2;    
    int16   extended;      
    int32   ntuples;
} FormData_pgxc_shard_map;

typedef FormData_pgxc_shard_map *Form_pgxc_shard_map;
/* the numbers of attribute  */
#define Natts_pgxc_shard_map                    11
/* the number of attribute  */
#define Anum_pgxc_shard_map_nodegroup           1
#define Anum_pgxc_shard_map_shardgroupid        2
#define Anum_pgxc_shard_map_ncopy                3
#define Anum_pgxc_shard_map_primarycopy            4
#define Anum_pgxc_shard_map_copy1                5
#define Anum_pgxc_shard_map_copy2                6
#define Anum_pgxc_shard_map_primarystatus        7
#define Anum_pgxc_shard_map_status1                8
#define Anum_pgxc_shard_map_status2                9
#define Anum_pgxc_shard_map_extend                10
#define Anum_pgxc_shard_map_ntuples                11


#define SHARD_MAP_STATUS_ERROR        'E'
#define SHARD_MAP_STATUS_UNDEFINED    'N'
#define SHARD_MAP_STATUS_USING        'U'
#define SHARD_MAP_STATUS_MIGRATING    'M'
#define SHARD_MAP_STATUS_INVALID    'I'
/* related function declaration */
extern bool is_group_sharding_inited(Oid group);
extern void InitShardMap(int32 nShardGroup, int32 nNodes, Oid *nodes, int16 extend, Oid nodeGroup);
extern bool is_already_inited(void);
extern bool NodeHasShard(Oid node);
extern void UpdateRelationShardMap(Oid group, Oid from_node, Oid to_node,
                                int shardgroup_num,    int* shardgroups);

extern void DropShardMap_Node(Oid group);
#endif  /* PGXC_SHARD_MAP_H */
