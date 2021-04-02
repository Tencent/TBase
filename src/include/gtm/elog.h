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
 * elog.h
 *      POSTGRES error reporting/logging definitions.
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * $PostgreSQL: pgsql/src/include/utils/elog.h,v 1.98 2009/01/01 17:24:02 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef ELOG_H
#define ELOG_H

#include "c.h"
#include "stringinfo.h"

/* Error level codes */
#define DEBUG8         9
#define DEBUG7        9
#define DEBUG6        9
#define DEBUG5        10            /* Debugging messages, in categories of
                                 * decreasing detail. */
#define DEBUG4        11
#define DEBUG3        12
#define DEBUG2        13
#define DEBUG1        14            /* used by GUC debug_* variables */
#define LOG            15            /* Server operational messages; sent only to
                                 * server log by default. */
#define COMMERROR    16            /* Client communication problems; same as LOG
                                 * for server reporting, but never sent to
                                 * client. */
#define INFO        17            /* Messages specifically requested by user
                                 * (eg VACUUM VERBOSE output); always sent to
                                 * client regardless of client_min_messages,
                                 * but by default not sent to server log. */
#define NOTICE        18            /* Helpful messages to users about query
                                 * operation; sent to client and server log
                                 * by default. */
#define WARNING        19            /* Warnings.  NOTICE is for expected messages
                                 * like implicit sequence creation by SERIAL.
                                 * WARNING is for unexpected messages. */
#define ERROR        20            /* user error - abort transaction; return to
                                 * known state */
#define ERROR2        21            /* user error - only send error message to the
                                 * client */
#define FATAL        22            /* fatal error - abort process */
#define PANIC        23            /* take down the other backends with me */

 /* #define DEBUG DEBUG1 */    /* Backward compatibility with pre-7.3 */


/* Which __func__ symbol do we have, if any? */
#ifdef HAVE_FUNCNAME__FUNC
#define PG_FUNCNAME_MACRO    __func__
#else
#ifdef HAVE_FUNCNAME__FUNCTION
#define PG_FUNCNAME_MACRO    __FUNCTION__
#else
#define PG_FUNCNAME_MACRO    NULL
#endif
#endif

/*
 * ErrorData holds the data accumulated during any one ereport() cycle.
 * Any non-NULL pointers must point to palloc'd data.
 * (The const pointers are an exception; we assume they point at non-freeable
 * constant strings.)
 */
typedef struct ErrorData
{
    int            elevel;            /* error level */
    bool        output_to_server;        /* will report to server log? */
    bool        output_to_client;        /* will report to client? */
    bool        show_funcname;    /* true to force funcname inclusion */
    const char *filename;        /* __FILE__ of ereport() call */
    int            lineno;            /* __LINE__ of ereport() call */
    const char *funcname;        /* __func__ of ereport() call */
    const char *domain;            /* message domain */
    char       *message;        /* primary error message */
    char       *detail;            /* detail error message */
    char       *detail_log;        /* detail error message for server log only */
    char       *hint;            /* hint message */
    char       *context;        /* context message */
    int            saved_errno;    /* errno at entry */
} ErrorData;


/*----------
 * New-style error reporting API: to be used in this way:
 *        ereport(ERROR,
 *                (errcode(ERRCODE_UNDEFINED_CURSOR),
 *                 errmsg("portal \"%s\" not found", stmt->portalname),
 *                 ... other errxxx() fields as needed ...));
 *
 * The error level is required, and so is a primary error message (errmsg
 * or errmsg_internal).  All else is optional.    errcode() defaults to
 * ERRCODE_INTERNAL_ERROR if elevel is ERROR or more, ERRCODE_WARNING
 * if elevel is WARNING, or ERRCODE_SUCCESSFUL_COMPLETION if elevel is
 * NOTICE or below.
 *
 * ereport_domain() allows a message domain to be specified, for modules that
 * wish to use a different message catalog from the backend's.    To avoid having
 * one copy of the default text domain per .o file, we define it as NULL here
 * and have errstart insert the default text domain.  Modules can either use
 * ereport_domain() directly, or preferably they can override the TEXTDOMAIN
 * macro.
 *----------
 */
#define TEXTDOMAIN "GTM"

#define ereport_domain(elevel, domain, rest)    \
    (errstart(elevel, __FILE__, __LINE__, PG_FUNCNAME_MACRO, domain) ? \
     (errfinish rest) : (void) 0)

#define ereport(level, rest)    \
    ereport_domain(level, TEXTDOMAIN, rest)


#define PG_RE_THROW()        pg_re_throw()

extern bool errstart(int elevel, const char *filename, int lineno,
         const char *funcname, const char *domain);
extern void errfinish(int dummy,...);

extern int
errmsg(const char *fmt,...)
/* This extension allows gcc to check the format string for consistency with
   the supplied arguments. */
__attribute__((format(printf, 1, 2)));

extern int
errmsg_internal(const char *fmt,...)
/* This extension allows gcc to check the format string for consistency with
   the supplied arguments. */
__attribute__((format(printf, 1, 2)));

extern int
errdetail(const char *fmt,...)
/* This extension allows gcc to check the format string for consistency with
   the supplied arguments. */
__attribute__((format(printf, 1, 2)));

extern int
errdetail_log(const char *fmt,...)
/* This extension allows gcc to check the format string for consistency with
   the supplied arguments. */
__attribute__((format(printf, 1, 2)));

extern int
errhint(const char *fmt,...)
/* This extension allows gcc to check the format string for consistency with
   the supplied arguments. */
__attribute__((format(printf, 1, 2)));

/*----------
 * Old-style error reporting API: to be used in this way:
 *        elog(ERROR, "portal \"%s\" not found", stmt->portalname);
 *----------
 */
#define elog    elog_start(__FILE__, __LINE__, PG_FUNCNAME_MACRO), elog_finish

extern void elog_start(const char *filename, int lineno, const char *funcname);
extern void
elog_finish(int elevel, const char *fmt,...)
/* This extension allows gcc to check the format string for consistency with
   the supplied arguments. */
__attribute__((format(printf, 2, 3)));

/*----------
 * API for catching ereport(ERROR) exits.  Use these macros like so:
 *
 *        PG_TRY();
 *        {
 *            ... code that might throw ereport(ERROR) ...
 *        }
 *        PG_CATCH();
 *        {
 *            ... error recovery code ...
 *        }
 *        PG_END_TRY();
 *
 * (The braces are not actually necessary, but are recommended so that
 * pg_indent will indent the construct nicely.)  The error recovery code
 * can optionally do PG_RE_THROW() to propagate the same error outwards.
 *
 * Note: while the system will correctly propagate any new ereport(ERROR)
 * occurring in the recovery section, there is a small limit on the number
 * of levels this will work for.  It's best to keep the error recovery
 * section simple enough that it can't generate any new errors, at least
 * not before popping the error stack.
 *
 * Note: an ereport(FATAL) will not be caught by this construct; control will
 * exit straight through proc_exit().  Therefore, do NOT put any cleanup
 * of non-process-local resources into the error recovery section, at least
 * not without taking thought for what will happen during ereport(FATAL).
 * The PG_ENSURE_ERROR_CLEANUP macros provided by storage/ipc.h may be
 * helpful in such cases.
 *----------
 */
#define PG_TRY()  \
    do { \
        sigjmp_buf *save_exception_stack = PG_exception_stack; \
        sigjmp_buf local_sigjmp_buf; \
        if (sigsetjmp(local_sigjmp_buf, 0) == 0) \
        { \
            PG_exception_stack = &local_sigjmp_buf

#define PG_CATCH()    \
        } \
        else \
        { \
            PG_exception_stack = save_exception_stack; \

#define PG_END_TRY()  \
        } \
        PG_exception_stack = save_exception_stack; \
    } while (0)

int errfunction(const char *funcname);

extern void EmitErrorReport(void *port);

/* GUC-configurable parameters */

typedef enum
{
    PGERROR_TERSE,                /* single-line error messages */
    PGERROR_DEFAULT,            /* recommended style */
    PGERROR_VERBOSE                /* all the facts, ma'am */
} PGErrorVerbosity;

/* Log destination bitmap */
#define LOG_DESTINATION_STDERR     1
#define LOG_DESTINATION_SYSLOG     2
#define LOG_DESTINATION_EVENTLOG 4
#define LOG_DESTINATION_CSVLOG     8

#define GTM_DEFAULT_EXIT_FLAG	 1024
extern int	exit_flag;
#define exit(x) (exit)(exit_flag = (x))

/* Other exported functions */
extern void pg_re_throw(void);
extern void DebugFileOpen(void);
extern void FlushErrorState(void);


/*
 * Write errors to stderr (or by equal means when stderr is
 * not available). Used before ereport/elog can be used
 * safely (memory context, GUC load etc)
 */
extern void
write_stderr(const char *fmt,...)
/* This extension allows gcc to check the format string for consistency with
   the supplied arguments. */
__attribute__((format(printf, 1, 2)));


/* log collection function hook */
typedef void (*errlog_collection_hook_type) (ErrorData *edata, StringInfo buff);
extern errlog_collection_hook_type errlog_collection_func;

#endif   /* GTM_ELOG_H */
