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
 * pg_partition_interval.h
 *      definition of the system "interval partitioned table" relation
 *      along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 *
 * src/include/catalog/pg_partition_interval.h
 *
 * NOTES
 *      the genbki.sh script reads this file and generates .bki
 *      information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PARTITION_INTERVAL_H
#define PG_PARTITION_INTERVAL_H

#include "catalog/genbki.h"

#include "datatype/timestamp.h"

#define timestamptz int64

#define PgPartitionIntervalRelationId 8100

CATALOG(pg_partition_interval,8100) BKI_WITHOUT_OIDS
{
    Oid            partrelid;
    int16       partpartkey;
    int16        partinterval_type;
    Oid            partdatatype;
    int32        partnparts;
    int64        partstartvalue_int;
    timestamptz    partstartvalue_ts;
    int32        partinterval_int;
}FormData_pg_partition_interval;

#undef timestamptz

typedef FormData_pg_partition_interval *Form_pg_partition_interval;


#define Natts_pg_partition_interval                        8

#define Anum_pg_partition_interval_partrelid            1
#define Anum_pg_partition_interval_partpartkey            2
#define Anum_pg_partition_interval_partinterval_type    3
#define Anum_pg_partition_interval_partdatatype            4
#define Anum_pg_partition_interval_partnparts            5
#define Anum_pg_partition_interval_partstartvalue_int    6
#define Anum_pg_partition_interval_partstartvalue_ts    7
#define Anum_pg_partition_interval_partinterval_int        8


typedef enum PartitionIntervalType
{
    IntervalType_Int2,
    IntervalType_Int4,
    IntervalType_Int8,
    IntervalType_Hour,
    IntervalType_Day,
    IntervalType_Month,
    IntervalType_Year,
    IntervalType_Reserve
} PartitionIntervalType;


extern void CreateIntervalPartition(Oid relid, 
                                AttrNumber partkey,
                                int16 intervaltype,
                                Oid partdatatype,
                                int nparts, 
                                int64 startval, 
                                int32 interval);

extern void RemoveIntervalPartition(Oid relid);

extern void AddPartitions(Oid relid, int num);

extern bool IsIntervalPartition(Oid relid);

extern void ModifyPartitionStartValue(Oid relid, int64 startval);
#endif   /* PG_PARTITION_INTERVAL_H */
