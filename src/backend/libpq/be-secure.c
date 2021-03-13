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
 * be-secure.c
 *      functions related to setting up a secure connection to the frontend.
 *      Secure connections are expected to provide confidentiality,
 *      message integrity and endpoint authentication.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *      src/backend/libpq/be-secure.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/stat.h>
#include <signal.h>
#include <fcntl.h>
#include <ctype.h>
#include <sys/socket.h>
#include <unistd.h>
#include <netdb.h>
#include <netinet/in.h>
#ifdef HAVE_NETINET_TCP_H
#include <netinet/tcp.h>
#include <arpa/inet.h>
#endif

#include "libpq/libpq.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "storage/ipc.h"
#include "storage/proc.h"


char       *ssl_cert_file;
char       *ssl_key_file;
char       *ssl_ca_file;
char       *ssl_crl_file;
char       *ssl_dh_params_file;

#ifdef USE_SSL
bool        ssl_loaded_verify_locations = false;
#endif

/* GUC variable controlling SSL cipher list */
char       *SSLCipherSuites = NULL;

/* GUC variable for default ECHD curve. */
char       *SSLECDHCurve;

/* GUC variable: if false, prefer client ciphers */
bool        SSLPreferServerCiphers;

/* ------------------------------------------------------------ */
/*             Procedures common to all secure sessions            */
/* ------------------------------------------------------------ */

/*
 *    Initialize global context.
 *
 * If isServerStart is true, report any errors as FATAL (so we don't return).
 * Otherwise, log errors at LOG level and return -1 to indicate trouble,
 * preserving the old SSL state if any.  Returns 0 if OK.
 */
int
secure_initialize(bool isServerStart)
{
#ifdef USE_SSL
    return be_tls_init(isServerStart);
#else
    return 0;
#endif
}

/*
 *    Destroy global context, if any.
 */
void
secure_destroy(void)
{
#ifdef USE_SSL
    be_tls_destroy();
#endif
}

/*
 * Indicate if we have loaded the root CA store to verify certificates
 */
bool
secure_loaded_verify_locations(void)
{
#ifdef USE_SSL
    return ssl_loaded_verify_locations;
#else
    return false;
#endif
}

/*
 *    Attempt to negotiate secure session.
 */
int
secure_open_server(Port *port)
{
    int            r = 0;

#ifdef USE_SSL
    r = be_tls_open_server(port);
#endif

    return r;
}

/*
 *    Close secure session.
 */
void
secure_close(Port *port)
{
#ifdef USE_SSL
    if (port->ssl_in_use)
        be_tls_close(port);
#endif
}

/*
 *    Read data from a secure connection.
 */
ssize_t
secure_read(Port *port, void *ptr, size_t len)
{// #lizard forgives
    ssize_t        n;
    int            waitfor;

retry:
#ifdef USE_SSL
    waitfor = 0;
    if (port->ssl_in_use)
    {
        n = be_tls_read(port, ptr, len, &waitfor);
    }
    else
#endif
    {
        n = secure_raw_read(port, ptr, len);
        waitfor = WL_SOCKET_READABLE;
    }

    /* In blocking mode, wait until the socket is ready */
    if (n < 0 && !port->noblock && (errno == EWOULDBLOCK || errno == EAGAIN))
    {
        WaitEvent    event;

        Assert(waitfor);

        ModifyWaitEvent(FeBeWaitSet, 0, waitfor, NULL);

        WaitEventSetWait(FeBeWaitSet, -1 /* no timeout */ , &event, 1,
                         WAIT_EVENT_CLIENT_READ);

        /*
         * If the postmaster has died, it's not safe to continue running,
         * because it is the postmaster's job to kill us if some other backend
         * exists uncleanly.  Moreover, we won't run very well in this state;
         * helper processes like walwriter and the bgwriter will exit, so
         * performance may be poor.  Finally, if we don't exit, pg_ctl will be
         * unable to restart the postmaster without manual intervention, so no
         * new connections can be accepted.  Exiting clears the deck for a
         * postmaster restart.
         *
         * (Note that we only make this check when we would otherwise sleep on
         * our latch.  We might still continue running for a while if the
         * postmaster is killed in mid-query, or even through multiple queries
         * if we never have to wait for read.  We don't want to burn too many
         * cycles checking for this very rare condition, and this should cause
         * us to exit quickly in most cases.)
         */
        if (event.events & WL_POSTMASTER_DEATH)
            ereport(FATAL,
                    (errcode(ERRCODE_ADMIN_SHUTDOWN),
                     errmsg("terminating connection due to unexpected postmaster exit")));

        /* Handle interrupt. */
        if (event.events & WL_LATCH_SET)
        {
            ResetLatch(MyLatch);
            ProcessClientReadInterrupt(true);

            /*
             * We'll retry the read. Most likely it will return immediately
             * because there's still no data available, and we'll wait for the
			 * socket to become ready again. But we should check interrupts
			 * before retry incase of conflict interrupt.
             */
			CHECK_FOR_INTERRUPTS();
        }
        goto retry;
    }

    /*
     * Process interrupts that happened while (or before) receiving. Note that
     * we signal that we're not blocking, which will prevent some types of
     * interrupts from being processed.
     */
    ProcessClientReadInterrupt(false);

    return n;
}

ssize_t
secure_raw_read(Port *port, void *ptr, size_t len)
{
    ssize_t        n;

    /*
     * Try to read from the socket without blocking. If it succeeds we're
     * done, otherwise we'll wait for the socket using the latch mechanism.
     */
#ifdef WIN32
    pgwin32_noblock = true;
#endif
    n = recv(port->sock, ptr, len, 0);
#ifdef WIN32
    pgwin32_noblock = false;
#endif

    return n;
}


/*
 *    Write data to a secure connection.
 */
ssize_t
secure_write(Port *port, void *ptr, size_t len)
{// #lizard forgives
    ssize_t        n;
    int            waitfor;

retry:
    waitfor = 0;
#ifdef USE_SSL
    if (port->ssl_in_use)
    {
        n = be_tls_write(port, ptr, len, &waitfor);
    }
    else
#endif
    {
        n = secure_raw_write(port, ptr, len);
        waitfor = WL_SOCKET_WRITEABLE;
    }

    if (n < 0 && !port->noblock && (errno == EWOULDBLOCK || errno == EAGAIN))
    {
        WaitEvent    event;

        Assert(waitfor);

        ModifyWaitEvent(FeBeWaitSet, 0, waitfor, NULL);

        WaitEventSetWait(FeBeWaitSet, -1 /* no timeout */ , &event, 1,
                         WAIT_EVENT_CLIENT_WRITE);

        /* See comments in secure_read. */
        if (event.events & WL_POSTMASTER_DEATH)
            ereport(FATAL,
                    (errcode(ERRCODE_ADMIN_SHUTDOWN),
                     errmsg("terminating connection due to unexpected postmaster exit")));

        /* Handle interrupt. */
        if (event.events & WL_LATCH_SET)
        {
            ResetLatch(MyLatch);
            ProcessClientWriteInterrupt(true);

            /*
             * We'll retry the write. Most likely it will return immediately
             * because there's still no data available, and we'll wait for the
             * socket to become ready again.
             */
        }
        goto retry;
    }

    /*
     * Process interrupts that happened while (or before) sending. Note that
     * we signal that we're not blocking, which will prevent some types of
     * interrupts from being processed.
     */
    ProcessClientWriteInterrupt(false);

    return n;
}

ssize_t
secure_raw_write(Port *port, const void *ptr, size_t len)
{
    ssize_t        n;

#ifdef WIN32
    pgwin32_noblock = true;
#endif
    n = send(port->sock, ptr, len, 0);
#ifdef WIN32
    pgwin32_noblock = false;
#endif

    return n;
}
