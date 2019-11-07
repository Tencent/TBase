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
 * libpq_be.h
 *      This file contains definitions for structures and externs used
 *      by the postmaster during client authentication.
 *
 *      Note that this is backend-internal and is NOT exported to clients.
 *      Structs that need to be client-visible are in pqcomm.h.
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * $PostgreSQL: pgsql/src/include/libpq/libpq-be.h,v 1.69 2009/01/01 17:23:59 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef LIBPQ_BE_H
#define LIBPQ_BE_H

#include "gtm/pqcomm.h"
#include "gtm/gtm_c.h"

#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif
#ifdef HAVE_NETINET_TCP_H
#include <netinet/tcp.h>
#endif

/*
 * This is used by the postmaster in its communication with frontends.    It
 * contains all state information needed during this communication before the
 * backend is run.    The Port structure is kept in malloc'd memory and is
 * still available when a backend is running (see MyProcPort).    The data
 * it points to must also be malloc'd, or else palloc'd in TopMostMemoryContext,
 * so that it survives into GTM_ThreadMain execution!
 */

typedef struct Port
{
    void        *conn;                /* pointer back to connection */
    int            sock;                /* File descriptor */
    SockAddr    laddr;                /* local addr (postmaster) */
    SockAddr    raddr;                /* remote addr (client) */
#ifndef __TBASE__
    char        *remote_host;        /* name (or ip addr) of remote host */
    char        *remote_port;        /* text rep of remote port */
#endif
    GTM_PortLastCall last_call;        /* Last syscall to this port */
    int            last_errno;            /* Last errno. zero if the last call succeeds */
    bool        is_nonblocking;        /* nonblocking? */

    GTMProxy_ConnID    conn_id;        /* RequestID of this command */

    GTM_PGXCNodeType    remote_type;    /* Type of remote connection */
    char        *node_name;
    bool        is_postmaster;        /* Is remote a node postmaster? */
    uint32        remote_backend_pid;
#define PQ_BUFFER_SIZE 8192

    char        PqSendBuffer[PQ_BUFFER_SIZE];
    int            PqSendPointer;        /* Next index to store a byte in PqSendBuffer */

    char         PqRecvBuffer[PQ_BUFFER_SIZE];
    int            PqRecvPointer;        /* Next index to read a byte from PqRecvBuffer */
    int            PqRecvLength;        /* End of data available in PqRecvBuffer */

    /*
     * TCP keepalive settings.
     *
     * default values are 0 if AF_UNIX or not yet known; current values are 0
     * if AF_UNIX or using the default. Also, -1 in a default value means we
     * were unable to find out the default (getsockopt failed).
     */
    int            default_keepalives_idle;
    int            default_keepalives_interval;
    int            default_keepalives_count;
    int            keepalives_idle;
    int            keepalives_interval;
    int            keepalives_count;

    /*
     * GTM communication error handling.  See libpq-int.h for details.
     */
    int            connErr_WaitOpt;
    int            connErr_WaitInterval;
    int            connErr_WaitCount;
} Port;

/* TCP keepalives configuration. These are no-ops on an AF_UNIX socket. */

extern int    pq_getkeepalivesidle(Port *port);
extern int    pq_getkeepalivesinterval(Port *port);
extern int    pq_getkeepalivescount(Port *port);

extern int    pq_setkeepalivesidle(int idle, Port *port);
extern int    pq_setkeepalivesinterval(int interval, Port *port);
extern int    pq_setkeepalivescount(int count, Port *port);

#ifdef __XLOG__
extern bool pq_hasdataleft(Port *port);
#endif

#endif   /* LIBPQ_BE_H */
