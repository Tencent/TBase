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
/* -------------------------------------------------------------------------
 *
 * pg_subscription_statistic.h
 *        Statistic info about shards of a subscription (pg_subscription_statistic).
 *
 * Portions Copyright (c) 2018, Tencent TBase Group
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_SUBSCRIPTION_STATISTIC_H
#define PG_SUBSCRIPTION_STATISTIC_H

#include "postgres.h"
#include "fmgr.h"



extern int32 g_PubStatHashSize;
extern int32 g_PubTableStatHashSize;
extern int32 g_SubStatHashSize;
extern int32 g_SubTableStatHashSize;


/* ----------------
 *        state constants
 * ----------------
 */
#define STATE_INIT        'i' /* initializing (sublsn NULL) */
#define STATE_DATACOPY    'c' /* copy data to relation */
#define STATE_COPYDONE    'd' /* finished to copy data */
#define STATE_APPLY        'a' /* apply logical log */
#define STATE_SYNCDONE  's' /* finish to sync relation */

extern Size PubStatDataShmemSize(int ssize, int tsize);

extern void InitPubStatData(int ssize, int tsize);

extern Size SubStatDataShmemSize(int ssize, int tsize);

extern void InitSubStatData(int ssize, int tsize);

extern void UpdatePubStatistics(char *subname, uint64 ntups_copy, uint64 ntups_insert, uint64 ntups_delete,
                                        uint64 checksum_insert, uint64 checksum_delete, bool init);
extern void UpdatePubTableStatistics(Oid subid, Oid relid, uint64 ntups_copy, uint64 ntups_insert, uint64 ntups_delete,
                                        uint64 checksum_insert, uint64 checksum_delete, bool init);

extern void RemovePubStatistics(char *subname);

extern void SetPubStatCheck(bool pubstatcount, bool pubstatchecksum);

extern void UpdateSubStatistics(char *subname, uint64 ntups_copy, uint64 ntups_insert, uint64 ntups_delete,
                                        uint64 checksum_insert, uint64 checksum_delete, bool init);
extern void UpdateSubTableStatistics(Oid subid, Oid relid, uint64 ntups_copy, uint64 ntups_insert, uint64 ntups_delete,
                                        uint64 checksum_insert, uint64 checksum_delete, char state, bool init);
extern void RemoveSubStatistics(char *subname);

extern void SetSubStatCheck(bool substatcount, bool substatchecksum);

extern void GetSubTableEntry(Oid subid, Oid relid, void **entry, CmdType cmd);

extern Datum tbase_get_pub_stat(PG_FUNCTION_ARGS);
extern Datum tbase_get_all_pub_stat(PG_FUNCTION_ARGS);
extern Datum tbase_get_sub_stat(PG_FUNCTION_ARGS);
extern Datum tbase_get_all_sub_stat(PG_FUNCTION_ARGS);
extern Datum tbase_get_pubtable_stat(PG_FUNCTION_ARGS);
extern Datum tbase_get_all_pubtable_stat(PG_FUNCTION_ARGS);
extern Datum tbase_get_subtable_stat(PG_FUNCTION_ARGS);
extern Datum tbase_get_all_subtable_stat(PG_FUNCTION_ARGS);
extern Datum tbase_set_pub_stat_check(PG_FUNCTION_ARGS);
extern Datum tbase_set_sub_stat_check(PG_FUNCTION_ARGS);
extern Datum tbase_get_pub_stat_check(PG_FUNCTION_ARGS);
extern Datum tbase_get_sub_stat_check(PG_FUNCTION_ARGS);
extern Datum tbase_remove_pub_stat(PG_FUNCTION_ARGS);
extern Datum tbase_remove_pubtable_stat(PG_FUNCTION_ARGS);
extern Datum tbase_remove_sub_stat(PG_FUNCTION_ARGS);
extern Datum tbase_remove_subtable_stat(PG_FUNCTION_ARGS);
#endif                            /* PG_SUBSCRIPTION_STATISTIC_H */

