/*-------------------------------------------------------------------------
 *
 * gtm_lock.c
 *    Handling for locks in GTM
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *      $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#include "gtm/gtm_c.h"
#include "gtm/gtm_lock.h"
#include "gtm/elog.h"
#include "gtm/gtm.h"

static void
get_abstime_wait(int microseconds, struct timespec *abstime);

/*
 * Acquire the request lock. Block if the lock is not available
 *
 * TODO We should track the locks acquired in the thread specific context. If an
 * error is thrown and cought, we don't want to keep holding to those locks
 * since that would lead to a deadlock. Right now, we assume that the caller
 * will appropriately catch errors and release the locks sanely.
 */
bool
GTM_RWLockAcquire(GTM_RWLock *lock, GTM_LockMode mode)
{// #lizard forgives
    int status = EINVAL;
#ifdef GTM_LOCK_DEBUG
    int indx;
    int ii;
#endif
#ifdef __XLOG__
    GTM_ThreadInfo *my_threadinfo = GetMyThreadInfo;
    int i = 0;

    if(my_threadinfo && my_threadinfo->xlog_inserting && lock->lock_flag&GTM_RWLOCK_FLAG_STORE)
    {
        for(i = 0; i < my_threadinfo->current_write_number;i++)
        {
            if(my_threadinfo->write_locks_hold[i] == lock)
            {
                my_threadinfo->write_counters[i]++;
                return true;
            }
        }
    }
#endif

    switch (mode)
    {
        case GTM_LOCKMODE_WRITE:
#ifdef GTM_LOCK_DEBUG
            pthread_mutex_lock(&lock->lk_debug_mutex);
            for (ii = 0; ii < lock->rd_holders_count; ii++)
            {
                if (pthread_equal(lock->rd_holders[ii], pthread_self()))
                    elog(WARNING, "Thread %p already owns a read-lock and may deadlock",
                            (void *) pthread_self());
            }
            if (pthread_equal(lock->wr_owner, pthread_self()))
                elog(WARNING, "Thread %p already owns a write-lock and may deadlock",
                        (void *) pthread_self());
            indx = lock->wr_waiters_count;
            if (indx < GTM_LOCK_DEBUG_MAX_READ_TRACKERS)
                lock->wr_waiters[lock->wr_waiters_count++] = pthread_self();
            else
                indx = -1;
            pthread_mutex_unlock(&lock->lk_debug_mutex);
#endif
            status = pthread_rwlock_wrlock(&lock->lk_lock);
#ifdef GTM_LOCK_DEBUG
            if (!status)
            {
                pthread_mutex_lock(&lock->lk_debug_mutex);
                lock->wr_granted = true;
                lock->wr_owner = pthread_self();
                lock->rd_holders_count = 0;
                lock->rd_holders_overflow = false;
                if (indx != -1)
                {
                    lock->wr_waiters[indx] = 0;
                    lock->wr_waiters_count--;
                }
                pthread_mutex_unlock(&lock->lk_debug_mutex);
            }
            else
                elog(ERROR, "pthread_rwlock_wrlock returned %d", status);
#endif
            break;

        case GTM_LOCKMODE_READ:
#ifdef GTM_LOCK_DEBUG
            pthread_mutex_lock(&lock->lk_debug_mutex);
            if (lock->wr_waiters_count > 0)
            {
                for (ii = 0; ii < lock->rd_holders_count; ii++)
                {
                    if (pthread_equal(lock->rd_holders[ii], pthread_self()))
                        elog(WARNING, "Thread %p already owns a read-lock and "
                                "there are blocked writers - this may deadlock",
                                    (void *) pthread_self());
                }
            }
            if (pthread_equal(lock->wr_owner, pthread_self()))
                elog(WARNING, "Thread %p already owns a write-lock and may deadlock",
                        (void *) pthread_self());
            indx = lock->rd_waiters_count;
            if (indx < GTM_LOCK_DEBUG_MAX_READ_TRACKERS)
                lock->rd_waiters[lock->rd_waiters_count++] = pthread_self();
            else
                indx = -1;
            pthread_mutex_unlock(&lock->lk_debug_mutex);
#endif
            /* Now acquire the lock */
            status = pthread_rwlock_rdlock(&lock->lk_lock);

#ifdef GTM_LOCK_DEBUG
            if (!status)
            {
                pthread_mutex_lock(&lock->lk_debug_mutex);
                lock->wr_granted = false;
                if (lock->rd_holders_count == GTM_LOCK_DEBUG_MAX_READ_TRACKERS)
                    lock->rd_holders_overflow = true;
                else
                {
                    lock->rd_holders[lock->rd_holders_count++] = pthread_self();
                    lock->rd_holders_overflow = false;
                    if (indx != -1)
                    {
                        lock->rd_waiters[indx] = 0;
                        lock->rd_waiters_count--;
                    }
                }
                pthread_mutex_unlock(&lock->lk_debug_mutex);
            }
            else
                elog(ERROR, "pthread_rwlock_rdlock returned %d", status);
#endif
            break;

        default:
            elog(ERROR, "Invalid lockmode");
            break;
    }

    if(status != 0)
    {
        elog(PANIC,"rw lock acquire fails %d %s",status,strerror(errno));
    }

    /* Remember the lock we got. */
    if (my_threadinfo && my_threadinfo->max_lock_number != -1)
    {
        if (my_threadinfo->current_number >= my_threadinfo->max_lock_number)
        {
            abort();
        }
        my_threadinfo->locks_hold[my_threadinfo->current_number++] = lock;

#ifdef __XLOG__
        if(mode == GTM_LOCKMODE_WRITE && my_threadinfo->xlog_inserting && lock->lock_flag&GTM_RWLOCK_FLAG_STORE)
        {
            my_threadinfo->write_counters[my_threadinfo->current_write_number] = 1;
            my_threadinfo->write_locks_hold[my_threadinfo->current_write_number++] = lock;
        }
#endif
    }
    return status ? false : true;
}

/*
 * Release previously acquired lock
 */
bool
GTM_RWLockRelease(GTM_RWLock *lock)
{// #lizard forgives
    int status;
    int    i = 0;
    int j = 0;
#ifdef  __XLOG__
    GTM_ThreadInfo *my_threadinfo = GetMyThreadInfo;

    if(my_threadinfo && my_threadinfo->xlog_inserting && lock->lock_flag&GTM_RWLOCK_FLAG_STORE)
    {
        for(i = 0; i < my_threadinfo->current_write_number;i++)
        {
            if(my_threadinfo->write_locks_hold[i] == lock)
            {
                my_threadinfo->write_counters[i]--;
                return true;
            }
        }
    }
    status = pthread_rwlock_unlock(&lock->lk_lock);
#else
    status = pthread_rwlock_unlock(&lock->lk_lock);
#endif

#ifdef GTM_LOCK_DEBUG
    if (status)
        elog(PANIC, "pthread_rwlock_unlock returned %d", status);
    else
    {
        pthread_mutex_lock(&lock->lk_debug_mutex);
        if (lock->wr_granted)
        {
            Assert(pthread_equal(lock->wr_owner, pthread_self()));
            lock->wr_granted = false;
            lock->wr_owner = 0;
        }
        else
        {
            int ii;
            bool found = false;
            for (ii = 0; ii < lock->rd_holders_count; ii++)
            {
                if (pthread_equal(lock->rd_holders[ii], pthread_self()))
                {
                    found = true;
                    lock->rd_holders[ii] =
                        lock->rd_holders[lock->rd_holders_count - 1];
                    lock->rd_holders_count--;
                    lock->rd_holders[lock->rd_holders_count] = 0;
                    break;
                }
            }

            if (!found && !lock->rd_holders_overflow)
                elog(PANIC, "Thread %p does not own a read-lock",
                        (void *)pthread_self());
        }
        pthread_mutex_unlock(&lock->lk_debug_mutex);
    }
#endif
    if(status != 0)
    {
        elog(PANIC,"rw lock release fails %d %s",status,strerror(errno));
    }

    /* Remember the lock we got. */
    if (true)
    {
        if (my_threadinfo && my_threadinfo->max_lock_number != -1)
        {
            for (i = 0; i < my_threadinfo->current_number; i++)
            {
                if (my_threadinfo->locks_hold[i] == lock)
                {
                    for (j = i; j < my_threadinfo->current_number - 1; j++)
                        my_threadinfo->locks_hold[j] = my_threadinfo->locks_hold[j + 1];
                    my_threadinfo->locks_hold[my_threadinfo->current_number - 1] = NULL;
                    my_threadinfo->current_number--;
                    break;
                }
            }
        }
    }    
    return status ? false : true;
}
#ifdef __TBASE__
void RWLockCleanUp(void)
{
    int    i = 0;
    GTM_ThreadInfo *my_threadinfo = NULL;
    my_threadinfo = GetMyThreadInfo;

    elog(LOG,"execute lock clean up for thread %p",my_threadinfo);

    if(my_threadinfo && my_threadinfo->max_lock_number != -1)
    {
        my_threadinfo->xlog_inserting = false;
        for (i = 0; i < my_threadinfo->current_number; i++)
        {
            pthread_rwlock_unlock(&my_threadinfo->locks_hold[i]->lk_lock);
            my_threadinfo->locks_hold[i] = NULL;
        }
        my_threadinfo->current_number = 0;
        my_threadinfo->current_write_number = 0;
    }

}
#endif
/*
 * Initialize a lock
 */
int
GTM_RWLockInit(GTM_RWLock *lock)
{
#ifdef GTM_LOCK_DEBUG
    memset(lock, 0, sizeof (GTM_RWLock));
    pthread_mutex_init(&lock->lk_debug_mutex, NULL);
#endif
    lock->lock_flag = 0;
    return pthread_rwlock_init(&lock->lk_lock, NULL);
}

/*
 * Destroy a lock
 */
int
GTM_RWLockDestroy(GTM_RWLock *lock)
{
    GTM_ThreadInfo *thr = GetMyThreadInfo;
    int i,j;

    if(thr && thr->xlog_inserting && lock->lock_flag&GTM_RWLOCK_FLAG_STORE)
    {
        elog(LOG,"lock destory %p",lock);
        for(i = 0; i < thr->current_write_number ; i++ )
        {
            if(thr->write_locks_hold[i] == lock)
            {
                for(j = i ; j < thr->current_write_number - 1 ; j++)
                {
                    thr->write_locks_hold[j] = thr->write_locks_hold[j + 1];
                }
                thr->current_write_number--;
                break;
            }
        }
    }
    return pthread_rwlock_destroy(&lock->lk_lock);
}

/*
 * Conditionally acquire a lock. If the lock is not available, the function
 * immediately returns without blocking.
 *
 * Returns true if lock is successfully acquired. Otherwise returns false
 */
bool
GTM_RWLockConditionalAcquire(GTM_RWLock *lock, GTM_LockMode mode)
{// #lizard forgives
    int status = EINVAL;

    switch (mode)
    {
        case GTM_LOCKMODE_WRITE:
            status = pthread_rwlock_trywrlock(&lock->lk_lock);
#ifdef GTM_LOCK_DEBUG
            if (!status)
            {
                pthread_mutex_lock(&lock->lk_debug_mutex);
                lock->wr_granted = true;
                lock->wr_owner = pthread_self();
                lock->rd_holders_count = 0;
                lock->rd_holders_overflow = false;
                pthread_mutex_unlock(&lock->lk_debug_mutex);
            }
#endif
            break;

        case GTM_LOCKMODE_READ:
            status = pthread_rwlock_tryrdlock(&lock->lk_lock);
#ifdef GTM_LOCK_DEBUG
            if (!status)
            {
                pthread_mutex_lock(&lock->lk_debug_mutex);
                if (lock->rd_holders_count == GTM_LOCK_DEBUG_MAX_READ_TRACKERS)
                {
                    elog(WARNING, "Too many threads waiting for a read-lock");
                    lock->rd_holders_overflow = true;
                }
                else
                {
                    lock->rd_holders[lock->rd_holders_count++] = pthread_self();
                    lock->rd_holders_overflow = false;
                }
                pthread_mutex_unlock(&lock->lk_debug_mutex);
            }
#endif
            break;

        default:
            elog(ERROR, "Invalid lockmode");
            break;
    }

    return status ? false : true;
}

/*
 * Initialize a mutex lock
 */
int
GTM_MutexLockInit(GTM_MutexLock *lock)
{
    return pthread_mutex_init(&lock->lk_lock, NULL);
}

/*
 * Destroy a mutex lock
 */
int
GTM_MutexLockDestroy(GTM_MutexLock *lock)
{
    return pthread_mutex_destroy(&lock->lk_lock);
}

/*
 * Acquire a mutex lock
 *
 * Return true if the lock is successfully acquired, else return false.
 */
bool
GTM_MutexLockAcquire(GTM_MutexLock *lock)
{
    int status = pthread_mutex_lock(&lock->lk_lock);
    return status ? false : true;
}

/*
 * Release previously acquired lock
 */
bool
GTM_MutexLockRelease(GTM_MutexLock *lock)
{
    int status =  pthread_mutex_unlock(&lock->lk_lock);
    return status ? false : true;
}

/*
 * Conditionally acquire a lock. If the lock is not available, the function
 * immediately returns without blocking.
 *
 * Returns true if lock is successfully acquired. Otherwise returns false
 */
bool
GTM_MutexLockConditionalAcquire(GTM_MutexLock *lock)
{
    int status = pthread_mutex_trylock(&lock->lk_lock);
    return status ? false : true;
}

/*
 * Initialize a condition variable
 */
int
GTM_CVInit(GTM_CV *cv)
{
    return pthread_cond_init(&cv->cv_condvar, NULL);
}

/*
 * Destroy the conditional variable
 */
int
GTM_CVDestroy(GTM_CV *cv)
{
    return pthread_cond_destroy(&cv->cv_condvar);
}

/*
 * Wake up all the threads waiting on this conditional variable
 */
int
GTM_CVBcast(GTM_CV *cv)
{
    return pthread_cond_broadcast(&cv->cv_condvar);
}

/*
 * Wake up only one thread waiting on this conditional variable
 */
int
GTM_CVSignal(GTM_CV *cv)
{
    return pthread_cond_signal(&cv->cv_condvar);
}

/*
 * Wait on a conditional variable. The caller must have acquired the mutex lock
 * already.
 */
int
GTM_CVWait(GTM_CV *cv, GTM_MutexLock *lock)
{
    return pthread_cond_wait(&cv->cv_condvar, &lock->lk_lock);
}

static void 
get_abstime_wait(int microseconds, struct timespec *abstime)
{
    struct timeval tv;
    long long absmsec;
    gettimeofday(&tv, NULL);
    absmsec = tv.tv_sec * 1000ll + tv.tv_usec / 1000ll;
    absmsec += microseconds;

    abstime->tv_sec = absmsec / 1000ll;
    abstime->tv_nsec = absmsec % 1000ll * 1000000ll;
}

int
GTM_CVTimeWait(GTM_CV *cv, GTM_MutexLock *lock,int micro_seconds)
{
    struct timespec time;
    get_abstime_wait(micro_seconds,&time);
    return pthread_cond_timedwait(&cv->cv_condvar, &lock->lk_lock,&time);
}

void SpinLockInit(s_lock_t *lock)
{
    *lock = 0;
}

void SpinLockAcquire(s_lock_t *lock)
{
    while(__sync_lock_test_and_set(lock, 1))
        ;

}

void SpinLockRelease(s_lock_t *lock)
{
    __sync_lock_release(lock);
}


GTM_Queue * CreateQueue(uint32 size)
{
    GTM_Queue *queue = NULL;
    queue = palloc0( sizeof(GTM_Queue));
    queue->q_list = (void**)palloc0( sizeof(void*) * size);    
    queue->q_length = size;            
    queue->q_head   = 0;
    queue->q_tail   = 0;
    SpinLockInit(&(queue->q_lock));
    return queue;
}
void DestoryQueue(GTM_Queue *queue)
{
    if (queue)
    {
        pfree(queue->q_list);
        pfree(queue);
    }
}

void *QueuePop(GTM_Queue *queue)
{
    void *ptr = NULL;
    SpinLockAcquire(&(queue->q_lock));
    if (queue->q_head == queue->q_tail)
    {
        SpinLockRelease(&(queue->q_lock));
        return NULL;                
    }            
    ptr                             = queue->q_list[queue->q_head];
    queue->q_list[queue->q_head] = NULL;                
    queue->q_head                   = (queue->q_head  + 1) % queue->q_length;  
    SpinLockRelease(&(queue->q_lock));
    return ptr;
}

int QueueEnq(GTM_Queue *queue, void *p)
{
    SpinLockAcquire(&(queue->q_lock));
    if ((queue->q_tail + 1) % queue->q_length == queue->q_head)
    {
        SpinLockRelease(&(queue->q_lock));
        return -1;
    }
    queue->q_list[queue->q_tail] = p;
    queue->q_tail = (queue->q_tail  + 1) % queue->q_length;  
    SpinLockRelease(&(queue->q_lock));    
    return 0;
}
bool QueueIsFull(GTM_Queue *queue)
{
    SpinLockAcquire(&(queue->q_lock));
    if ((queue->q_tail + 1) % queue->q_length == queue->q_head)
    {
        SpinLockRelease(&(queue->q_lock));
        return true;
    }
    else
    {
        SpinLockRelease(&(queue->q_lock));
        return false;
    }
}
bool QueueIsEmpty(GTM_Queue *queue)
{
    SpinLockAcquire(&(queue->q_lock));
    if (queue->q_tail == queue->q_head)
    {
        SpinLockRelease(&(queue->q_lock));
        return true;
    }
    else
    {
        SpinLockRelease(&(queue->q_lock));
        return false;
    }
}
int QueueLength(GTM_Queue *queue)
{
    int len = -1;
    SpinLockAcquire(&(queue->q_lock));
    len = (queue->q_tail - queue->q_head + queue->q_length) % queue->q_length;
    SpinLockRelease(&(queue->q_lock));
    return len;
}

