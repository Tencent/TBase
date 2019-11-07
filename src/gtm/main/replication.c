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
 * replication.c
 *  Controlling the initialization and end of replication process of GTM data
 *
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *      src/gtm/recovery/replication.c
 *
 *-------------------------------------------------------------------------
 */
#include "gtm/replication.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "gtm/gtm_c.h"
#include "gtm/gtm.h"
#include "gtm/gtm_txn.h"
#include "gtm/standby_utils.h"
#include "gtm/gtm_standby.h"
#include "gtm/register.h"
#include "gtm/assert.h"
#include <stdio.h>
#include "gtm/libpq.h"
#include "gtm/pqformat.h"
#include "gtm/gtm_msg.h"
#include "gtm/gtm_ip.h"

/*
 * Process MSG_NODE_BEGIN_REPLICATION_INIT
 */
void
ProcessBeginReplicaInitSyncRequest(Port *myport, StringInfo message)
{
    StringInfoData buf;
    MemoryContext oldContext;

    pq_getmsgend(message);

    if (Recovery_IsStandby())
        ereport(ERROR,
            (EPERM,
             errmsg("Operation not permitted under the standby mode.")));

    oldContext = MemoryContextSwitchTo(TopMemoryContext);

    /* Acquire global locks to copy resource data to the standby. */
    GTM_RWLockAcquire(&GTMTransactions.gt_XidGenLock, GTM_LOCKMODE_WRITE);
    GTM_RWLockAcquire(&GTMTransactions.gt_TransArrayLock, GTM_LOCKMODE_WRITE);
    elog(LOG, "Prepared for copying data with holding XidGenLock and TransArrayLock.");

    MemoryContextSwitchTo(oldContext);

    BeforeReplyToClientXLogTrigger();
    
    pq_beginmessage(&buf, 'S');
    pq_sendint(&buf, NODE_BEGIN_REPLICATION_INIT_RESULT, 4);
    if (myport->remote_type == GTM_NODE_GTM_PROXY)
    {
        GTM_ProxyMsgHeader proxyhdr;
        proxyhdr.ph_conid = myport->conn_id;
        pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
    }
    pq_endmessage(myport, &buf);

    /*
     * Beause this command comes from the standby, we don't have to flush
     * messages to the standby here.
     */
    if (myport->remote_type != GTM_NODE_GTM_PROXY)
        pq_flush(myport);

    elog(LOG, "ProcessBeginReplicationInitialSyncRequest() ok.");

    return;
}

/*
 * Process MSG_NODE_END_REPLICATION_INIT
 */
void
ProcessEndReplicaInitSyncRequest(Port *myport, StringInfo message)
{
    StringInfoData buf;
    MemoryContext oldContext;

    pq_getmsgend(message);

    if (Recovery_IsStandby())
        ereport(ERROR,
            (EPERM,
             errmsg("Operation not permitted under the standby mode.")));

    oldContext = MemoryContextSwitchTo(TopMemoryContext);

    /*
     * Release global locks after copying resource data to the standby.
     */
    GTM_RWLockRelease(&GTMTransactions.gt_TransArrayLock);
    GTM_RWLockRelease(&GTMTransactions.gt_XidGenLock);
    elog(LOG, "XidGenLock and TransArrayLock released.");

    MemoryContextSwitchTo(oldContext);

    BeforeReplyToClientXLogTrigger();
    
    pq_beginmessage(&buf, 'S');
    pq_sendint(&buf, NODE_END_REPLICATION_INIT_RESULT, 4);
    if (myport->remote_type == GTM_NODE_GTM_PROXY)
    {
        GTM_ProxyMsgHeader proxyhdr;
        proxyhdr.ph_conid = myport->conn_id;
        pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
    }
    pq_endmessage(myport, &buf);

    /*
     * Beause this command comes from the standby, we don't have to flush
     * messages to the standby here.
     */
    if (myport->remote_type != GTM_NODE_GTM_PROXY)
        pq_flush(myport);

    elog(LOG, "ProcessEndReplicationInitialSyncRequest() ok.");

    return;
}
