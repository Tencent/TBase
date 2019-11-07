/*
 *
 * gtm_common.h
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * Portions Copyright (c) 2014 Translattice Inc
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */

#ifndef _GTM_COMMON_H
#define _GTM_COMMON_H

#define GTM_COMMON_THREAD_INFO \
    GTM_ThreadID            thr_id; \
    uint32                    thr_localid; \
    bool                    is_main_thread; \
    void * (* thr_startroutine)(void *); \
    MemoryContext    thr_thread_context; \
    MemoryContext    thr_message_context; \
    MemoryContext    thr_current_context; \
    MemoryContext    thr_error_context; \
    MemoryContext    thr_parent_context; \
    sigjmp_buf        *thr_sigjmp_buf; \
    ErrorData        thr_error_data[ERRORDATA_STACK_SIZE]; \
    int                thr_error_stack_depth; \
    int                thr_error_recursion_depth; \
    int                thr_criticalsec_count;


#endif
