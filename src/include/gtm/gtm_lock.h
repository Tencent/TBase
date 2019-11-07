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
 * gtm_lock.h
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */

#ifndef GTM_LOCK_H
#define GTM_LOCK_H


#include <pthread.h>

#define GTM_RWLOCK_FLAG_STORE 0x01

typedef struct GTM_RWLock
{
#ifdef __XLOG__
    int              lock_flag;
#endif
    pthread_rwlock_t lk_lock;
#ifdef GTM_LOCK_DEBUG
#define GTM_LOCK_DEBUG_MAX_READ_TRACKERS    1024
    pthread_mutex_t    lk_debug_mutex;
    int                wr_waiters_count;
    pthread_t        wr_waiters[GTM_LOCK_DEBUG_MAX_READ_TRACKERS];
    bool            wr_granted;
    pthread_t        wr_owner;
    int                rd_holders_count;
    bool            rd_holders_overflow;
    pthread_t        rd_holders[GTM_LOCK_DEBUG_MAX_READ_TRACKERS];
    int                rd_waiters_count;
    bool            rd_waiters_overflow;
    pthread_t        rd_waiters[GTM_LOCK_DEBUG_MAX_READ_TRACKERS];
#endif
} GTM_RWLock;

typedef struct GTM_MutexLock
{
    pthread_mutex_t lk_lock;
} GTM_MutexLock;

typedef enum GTM_LockMode
{
    GTM_LOCKMODE_WRITE,
    GTM_LOCKMODE_READ
} GTM_LockMode;

typedef struct GTM_CV
{
    pthread_cond_t    cv_condvar;
} GTM_CV;

extern bool GTM_RWLockAcquire(GTM_RWLock *lock, GTM_LockMode mode);
extern bool GTM_RWLockRelease(GTM_RWLock *lock);
#ifdef __XLOG__
extern int GTM_RWLockInit(GTM_RWLock *lock);
#else
extern int GTM_RWLockInit(GTM_RWLock *lock);
#endif
extern int GTM_RWLockDestroy(GTM_RWLock *lock);
extern bool GTM_RWLockConditionalAcquire(GTM_RWLock *lock, GTM_LockMode mode);

extern bool GTM_MutexLockAcquire(GTM_MutexLock *lock);
extern bool GTM_MutexLockRelease(GTM_MutexLock *lock);
extern int GTM_MutexLockInit(GTM_MutexLock *lock);
extern int GTM_MutexLockDestroy(GTM_MutexLock *lock);
extern bool GTM_MutexLockConditionalAcquire(GTM_MutexLock *lock);

extern int GTM_CVInit(GTM_CV *cv);
extern int GTM_CVDestroy(GTM_CV *cv);
extern int GTM_CVSignal(GTM_CV *cv);
extern int GTM_CVBcast(GTM_CV *cv);
extern int GTM_CVWait(GTM_CV *cv, GTM_MutexLock *lock);
extern int GTM_CVTimeWait(GTM_CV *cv, GTM_MutexLock *lock,int micro_seconds);

typedef int s_lock_t;
extern void SpinLockInit(s_lock_t *lock);

extern void SpinLockAcquire(s_lock_t *lock);

extern void SpinLockRelease(s_lock_t *lock);


typedef struct 
{
    void                 **q_list; 
    uint32               q_length; 
    s_lock_t              q_lock;  
    volatile uint32      q_head;   
    volatile uint32      q_tail;  
}GTM_Queue;
extern GTM_Queue* CreateQueue(uint32 size);
extern void    DestoryQueue(GTM_Queue *queue);
extern void    *QueuePop(GTM_Queue *queue);
extern int     QueueEnq(GTM_Queue *queue, void *p);
extern bool    QueueIsFull(GTM_Queue *queue);
extern bool    QueueIsEmpty(GTM_Queue *queue);
extern int        QueueLength(GTM_Queue *queue);

#ifdef __TBASE__
extern void    RWLockCleanUp(void);
#endif
#endif
