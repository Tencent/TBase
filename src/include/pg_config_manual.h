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
/*------------------------------------------------------------------------
 * PostgreSQL manual configuration settings
 *
 * This file contains various configuration symbols and limits.  In
 * all cases, changing them is only useful in very rare situations or
 * for developers.  If you edit any of these, be sure to do a *full*
 * rebuild (and an initdb if noted).
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/pg_config_manual.h
 *------------------------------------------------------------------------
 */

/*
 * Maximum length for identifiers (e.g. table names, column names,
 * function names).  Names actually are limited to one less byte than this,
 * because the length must include a trailing zero byte.
 *
 * Changing this requires an initdb.
 */
#define NAMEDATALEN 64


#define MAXFULLNAMEDATALEN (NAMEDATALEN * 3 + 2)

/*
 * Maximum number of arguments to a function.
 *
 * The minimum value is 8 (GIN indexes use 8-argument support functions).
 * The maximum possible value is around 600 (limited by index tuple size in
 * pg_proc's index; BLCKSZ larger than 8K would allow more).  Values larger
 * than needed will waste memory and processing time, but do not directly
 * cost disk space.
 *
 * Changing this does not require an initdb, but it does require a full
 * backend recompile (including any user-defined C functions).
 */
#define FUNC_MAX_ARGS        100

/*
 * Maximum number of columns in an index.  There is little point in making
 * this anything but a multiple of 32, because the main cost is associated
 * with index tuple header size (see access/itup.h).
 *
 * Changing this requires an initdb.
 */
#define INDEX_MAX_KEYS        32

/*
 * Maximum number of columns in a partition key
 */
#define PARTITION_MAX_KEYS    32

/*
 * When we don't have native spinlocks, we use semaphores to simulate them.
 * Decreasing this value reduces consumption of OS resources; increasing it
 * may improve performance, but supplying a real spinlock implementation is
 * probably far better.
 */
#define NUM_SPINLOCK_SEMAPHORES        128

/*
 * When we have neither spinlocks nor atomic operations support we're
 * implementing atomic operations on top of spinlock on top of semaphores. To
 * be safe against atomic operations while holding a spinlock separate
 * semaphores have to be used.
 */
#define NUM_ATOMICS_SEMAPHORES        64

/*
 * Define this if you want to allow the lo_import and lo_export SQL
 * functions to be executed by ordinary users.  By default these
 * functions are only available to the Postgres superuser.  CAUTION:
 * These functions are SECURITY HOLES since they can read and write
 * any file that the PostgreSQL server has permission to access.  If
 * you turn this on, don't say we didn't warn you.
 */
/* #define ALLOW_DANGEROUS_LO_FUNCTIONS */

/*
 * MAXPGPATH: standard size of a pathname buffer in PostgreSQL (hence,
 * maximum usable pathname length is one less).
 *
 * We'd use a standard system header symbol for this, if there weren't
 * so many to choose from: MAXPATHLEN, MAX_PATH, PATH_MAX are all
 * defined by different "standards", and often have different values
 * on the same platform!  So we just punt and use a reasonably
 * generous setting here.
 */
#define MAXPGPATH        1024

/*
 * PG_SOMAXCONN: maximum accept-queue length limit passed to
 * listen(2).  You'd think we should use SOMAXCONN from
 * <sys/socket.h>, but on many systems that symbol is much smaller
 * than the kernel's actual limit.  In any case, this symbol need be
 * twiddled only if you have a kernel that refuses large limit values,
 * rather than silently reducing the value to what it can handle
 * (which is what most if not all Unixen do).
 */
#define PG_SOMAXCONN    10000

/*
 * You can try changing this if you have a machine with bytes of
 * another size, but no guarantee...
 */
#define BITS_PER_BYTE        8

/*
 * Preferred alignment for disk I/O buffers.  On some CPUs, copies between
 * user space and kernel space are significantly faster if the user buffer
 * is aligned on a larger-than-MAXALIGN boundary.  Ideally this should be
 * a platform-dependent value, but for now we just hard-wire it.
 */
#define ALIGNOF_BUFFER    32

/*
 * Disable UNIX sockets for certain operating systems.
 */
#if defined(WIN32)
#undef HAVE_UNIX_SOCKETS
#endif

/*
 * Define this if your operating system supports link()
 */
#if !defined(WIN32) && !defined(__CYGWIN__)
#define HAVE_WORKING_LINK 1
#endif

/*
 * USE_POSIX_FADVISE controls whether Postgres will attempt to use the
 * posix_fadvise() kernel call.  Usually the automatic configure tests are
 * sufficient, but some older Linux distributions had broken versions of
 * posix_fadvise().  If necessary you can remove the #define here.
 */
#if HAVE_DECL_POSIX_FADVISE && defined(HAVE_POSIX_FADVISE)
#define USE_POSIX_FADVISE
#endif

/*
 * USE_PREFETCH code should be compiled only if we have a way to implement
 * prefetching.  (This is decoupled from USE_POSIX_FADVISE because there
 * might in future be support for alternative low-level prefetch APIs.)
 */
#ifdef USE_POSIX_FADVISE
#define USE_PREFETCH
#endif

/*
 * Default and maximum values for backend_flush_after, bgwriter_flush_after
 * and checkpoint_flush_after; measured in blocks.  Currently, these are
 * enabled by default if sync_file_range() exists, ie, only on Linux.  Perhaps
 * we could also enable by default if we have mmap and msync(MS_ASYNC)?
 */
#ifdef HAVE_SYNC_FILE_RANGE
#define DEFAULT_BACKEND_FLUSH_AFTER 0    /* never enabled by default */
#define DEFAULT_BGWRITER_FLUSH_AFTER 64
#define DEFAULT_CHECKPOINT_FLUSH_AFTER 32
#else
#define DEFAULT_BACKEND_FLUSH_AFTER 0
#define DEFAULT_BGWRITER_FLUSH_AFTER 0
#define DEFAULT_CHECKPOINT_FLUSH_AFTER 0
#endif
/* upper limit for all three variables */
#define WRITEBACK_MAX_PENDING_FLUSHES 256

/*
 * USE_SSL code should be compiled only when compiling with an SSL
 * implementation.  (Currently, only OpenSSL is supported, but we might add
 * more implementations in the future.)
 */
#ifdef USE_OPENSSL
#define USE_SSL
#endif

/*
 * This is the default directory in which AF_UNIX socket files are
 * placed.  Caution: changing this risks breaking your existing client
 * applications, which are likely to continue to look in the old
 * directory.  But if you just hate the idea of sockets in /tmp,
 * here's where to twiddle it.  You can also override this at runtime
 * with the postmaster's -k switch.
 */
#define DEFAULT_PGSOCKET_DIR  "/tmp"

/*
 * This is the default event source for Windows event log.
 */
#define DEFAULT_EVENT_SOURCE  "PostgreSQL"

/*
 * The random() function is expected to yield values between 0 and
 * MAX_RANDOM_VALUE.  Currently, all known implementations yield
 * 0..2^31-1, so we just hardwire this constant.  We could do a
 * configure test if it proves to be necessary.  CAUTION: Think not to
 * replace this with RAND_MAX.  RAND_MAX defines the maximum value of
 * the older rand() function, which is often different from --- and
 * considerably inferior to --- random().
 */
#define MAX_RANDOM_VALUE  PG_INT32_MAX

/*
 * On PPC machines, decide whether to use the mutex hint bit in LWARX
 * instructions.  Setting the hint bit will slightly improve spinlock
 * performance on POWER6 and later machines, but does nothing before that,
 * and will result in illegal-instruction failures on some pre-POWER4
 * machines.  By default we use the hint bit when building for 64-bit PPC,
 * which should be safe in nearly all cases.  You might want to override
 * this if you are building 32-bit code for a known-recent PPC machine.
 */
#ifdef HAVE_PPC_LWARX_MUTEX_HINT    /* must have assembler support in any case */
#if defined(__ppc64__) || defined(__powerpc64__)
#define USE_PPC_LWARX_MUTEX_HINT
#endif
#endif

/*
 * On PPC machines, decide whether to use LWSYNC instructions in place of
 * ISYNC and SYNC.  This provides slightly better performance, but will
 * result in illegal-instruction failures on some pre-POWER4 machines.
 * By default we use LWSYNC when building for 64-bit PPC, which should be
 * safe in nearly all cases.
 */
#if defined(__ppc64__) || defined(__powerpc64__)
#define USE_PPC_LWSYNC
#endif

/*
 * Assumed cache line size. This doesn't affect correctness, but can be used
 * for low-level optimizations. Currently, this is used to pad some data
 * structures in xlog.c, to ensure that highly-contended fields are on
 * different cache lines. Too small a value can hurt performance due to false
 * sharing, while the only downside of too large a value is a few bytes of
 * wasted memory. The default is 128, which should be large enough for all
 * supported platforms.
 */
#define PG_CACHE_LINE_SIZE        128

/*
 *------------------------------------------------------------------------
 * The following symbols are for enabling debugging code, not for
 * controlling user-visible features or resource limits.
 *------------------------------------------------------------------------
 */

/*
 * Include Valgrind "client requests", mostly in the memory allocator, so
 * Valgrind understands PostgreSQL memory contexts.  This permits detecting
 * memory errors that Valgrind would not detect on a vanilla build.  See also
 * src/tools/valgrind.supp.  "make installcheck" runs 20-30x longer under
 * Valgrind.  Note that USE_VALGRIND slowed older versions of Valgrind by an
 * additional order of magnitude; Valgrind 3.8.1 does not have this problem.
 * The client requests fall in hot code paths, so USE_VALGRIND also slows
 * native execution by a few percentage points.
 *
 * You should normally use MEMORY_CONTEXT_CHECKING with USE_VALGRIND;
 * instrumentation of repalloc() is inferior without it.
 */
/* #define USE_VALGRIND */

/*
 * Define this to cause pfree()'d memory to be cleared immediately, to
 * facilitate catching bugs that refer to already-freed values.
 * Right now, this gets defined automatically if --enable-cassert.
 */
#ifdef USE_ASSERT_CHECKING
#define CLOBBER_FREED_MEMORY
#endif

/*
 * Define this to check memory allocation errors (scribbling on more
 * bytes than were allocated).  Right now, this gets defined
 * automatically if --enable-cassert or USE_VALGRIND.
 */
#if defined(USE_ASSERT_CHECKING) || defined(USE_VALGRIND)
#define MEMORY_CONTEXT_CHECKING
#endif

/*
 * Define this to cause palloc()'d memory to be filled with random data, to
 * facilitate catching code that depends on the contents of uninitialized
 * memory.  Caution: this is horrendously expensive.
 */
/* #define RANDOMIZE_ALLOCATED_MEMORY */

/*
 * Define this to force all parse and plan trees to be passed through
 * copyObject(), to facilitate catching errors and omissions in
 * copyObject().
 */
/* #define COPY_PARSE_PLAN_TREES */

/*
 * Define this to force all raw parse trees for DML statements to be scanned
 * by raw_expression_tree_walker(), to facilitate catching errors and
 * omissions in that function.
 */
/* #define RAW_EXPRESSION_COVERAGE_TEST */

/*
 * Enable debugging print statements for lock-related operations.
 */
/* #define LOCK_DEBUG */

/*
 * Enable debugging print statements for WAL-related operations; see
 * also the wal_debug GUC var.
 */
/* #define WAL_DEBUG */

/*
 * Enable tracing of resource consumption during sort operations;
 * see also the trace_sort GUC var.  For 8.1 this is enabled by default.
 */
#define TRACE_SORT 1

/*
 * Enable tracing of syncscan operations (see also the trace_syncscan GUC var).
 */
/* #define TRACE_SYNCSCAN */

/*
 * Other debug #defines (documentation, anyone?)
 */
/* #define HEAPDEBUGALL */
/* #define ACLDEBUG */


/* all code written by Tencent Team must be in macro __TBASE__ block*/
#define __TBASE__ 0
#define __SUPPORT_DISTRIBUTED_TRANSACTION__ 0
//#define __PAGE_TIMESTAMP_LOG__ 0
//#define __USE_GLOBAL_SNAPSHOT__ 0
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
#undef __USE_GLOBAL_SNAPSHOT__
//#define __TBASE_DEBUG__ 0
//#define __SNAPSHOT_CHECK__ 0
//#define __USE_SYSTEM_TIMESTAMP__ 0
#define __PULLUP_QUERY__ 0
#endif
#define _MIGRATE_ 0
#define _SHARDING_
#define __AUDIT__ 0
#define _MLS_ 0
#define __AUDIT_FGA__
#define __STORAGE_SCALABLE__ 0
#define __XLOG__ 0
#define __COLD_HOT__ 0
#define __SUBSCRIPTION__ 0
#define _PUB_SUB_RELIABLE_ 0
#define __TWO_PHASE_TRANS__ 0
//#define __TWO_PHASE_TESTS__ 0
/* MAX NODES NUMBER of the cluster */
#define     MAX_NODES_NUMBER             4096
