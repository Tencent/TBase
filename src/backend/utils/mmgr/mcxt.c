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
 * mcxt.c
 *      POSTGRES memory context management code.
 *
 * This module handles context management operations that are independent
 * of the particular kind of context being operated on.  It calls
 * context-type-specific operations via the function pointers in a
 * context's MemoryContextMethods struct.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *      src/backend/utils/mmgr/mcxt.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "utils/memdebug.h"
#include "utils/memutils.h"

#ifdef __TBASE__
#include <sys/types.h>
#include <unistd.h>
#endif


/*****************************************************************************
 *      GLOBAL MEMORY                                                             *
 *****************************************************************************/

/*
 * CurrentMemoryContext
 *        Default memory context for allocations.
 */
MemoryContext CurrentMemoryContext = NULL;

/*
 * Standard top-level contexts. For a description of the purpose of each
 * of these contexts, refer to src/backend/utils/mmgr/README
 */
MemoryContext TopMemoryContext = NULL;
MemoryContext ErrorContext = NULL;
MemoryContext PostmasterContext = NULL;
MemoryContext CacheMemoryContext = NULL;
MemoryContext MessageContext = NULL;
MemoryContext TopTransactionContext = NULL;
MemoryContext CurTransactionContext = NULL;

/* This is a transient link to the active portal's memory context: */
MemoryContext PortalContext = NULL;

#ifdef __AUDIT__
MemoryContext AuditContext = NULL;
#endif

static void MemoryContextCallResetCallbacks(MemoryContext context);
static void MemoryContextStatsInternal(MemoryContext context, int level,
                           bool print, int max_children,
                           MemoryContextCounters *totals);

#ifdef PGXC
void *allocTopCxt(size_t s);
#endif

/*
 * You should not do memory allocations within a critical section, because
 * an out-of-memory error will be escalated to a PANIC. To enforce that
 * rule, the allocation functions Assert that.
 */
#define AssertNotInCriticalSection(context) \
    Assert(CritSectionCount == 0 || (context)->allowInCritSection)

/*****************************************************************************
 *      EXPORTED ROUTINES                                                         *
 *****************************************************************************/


/*
 * MemoryContextInit
 *        Start up the memory-context subsystem.
 *
 * This must be called before creating contexts or allocating memory in
 * contexts.  TopMemoryContext and ErrorContext are initialized here;
 * other contexts must be created afterwards.
 *
 * In normal multi-backend operation, this is called once during
 * postmaster startup, and not at all by individual backend startup
 * (since the backends inherit an already-initialized context subsystem
 * by virtue of being forked off the postmaster).  But in an EXEC_BACKEND
 * build, each process must do this for itself.
 *
 * In a standalone backend this must be called during backend startup.
 */
void
MemoryContextInit(void)
{
    AssertState(TopMemoryContext == NULL);

    /*
     * First, initialize TopMemoryContext, which will hold the MemoryContext
     * nodes for all other contexts.  (There is special-case code in
     * MemoryContextCreate() to handle this call.)
     */
    TopMemoryContext = AllocSetContextCreate((MemoryContext) NULL,
                                             "TopMemoryContext",
                                             ALLOCSET_DEFAULT_SIZES);

    /*
     * Not having any other place to point CurrentMemoryContext, make it point
     * to TopMemoryContext.  Caller should change this soon!
     */
    CurrentMemoryContext = TopMemoryContext;

    /*
     * Initialize ErrorContext as an AllocSetContext with slow growth rate ---
     * we don't really expect much to be allocated in it. More to the point,
     * require it to contain at least 8K at all times. This is the only case
     * where retained memory in a context is *essential* --- we want to be
     * sure ErrorContext still has some memory even if we've run out
     * elsewhere! Also, allow allocations in ErrorContext within a critical
     * section. Otherwise a PANIC will cause an assertion failure in the error
     * reporting code, before printing out the real cause of the failure.
     *
     * This should be the last step in this function, as elog.c assumes memory
     * management works once ErrorContext is non-null.
     */
    ErrorContext = AllocSetContextCreate(TopMemoryContext,
                                         "ErrorContext",
                                         8 * 1024,
                                         8 * 1024,
                                         8 * 1024);
    MemoryContextAllowInCriticalSection(ErrorContext, true);
}

/*
 * MemoryContextReset
 *        Release all space allocated within a context and delete all its
 *        descendant contexts (but not the named context itself).
 */
void
MemoryContextReset(MemoryContext context)
{
    AssertArg(MemoryContextIsValid(context));

    /* save a function call in common case where there are no children */
    if (context->firstchild != NULL)
        MemoryContextDeleteChildren(context);

    /* save a function call if no pallocs since startup or last reset */
    if (!context->isReset)
        MemoryContextResetOnly(context);
}

/*
 * MemoryContextResetOnly
 *        Release all space allocated within a context.
 *        Nothing is done to the context's descendant contexts.
 */
void
MemoryContextResetOnly(MemoryContext context)
{
    AssertArg(MemoryContextIsValid(context));

    /* Nothing to do if no pallocs since startup or last reset */
    if (!context->isReset)
    {
        MemoryContextCallResetCallbacks(context);
        (*context->methods->reset) (context);
        context->isReset = true;
        VALGRIND_DESTROY_MEMPOOL(context);
        VALGRIND_CREATE_MEMPOOL(context, 0, false);
    }
}

/*
 * MemoryContextResetChildren
 *        Release all space allocated within a context's descendants,
 *        but don't delete the contexts themselves.  The named context
 *        itself is not touched.
 */
void
MemoryContextResetChildren(MemoryContext context)
{
    MemoryContext child;

    AssertArg(MemoryContextIsValid(context));

    for (child = context->firstchild; child != NULL; child = child->nextchild)
    {
        MemoryContextResetChildren(child);
        MemoryContextResetOnly(child);
    }
}

/*
 * MemoryContextDelete
 *        Delete a context and its descendants, and release all space
 *        allocated therein.
 *
 * The type-specific delete routine removes all subsidiary storage
 * for the context, but we have to delete the context node itself,
 * as well as recurse to get the children.  We must also delink the
 * node from its parent, if it has one.
 */
void
MemoryContextDelete(MemoryContext context)
{
    AssertArg(MemoryContextIsValid(context));
    /* We had better not be deleting TopMemoryContext ... */
    Assert(context != TopMemoryContext);
    /* And not CurrentMemoryContext, either */
    Assert(context != CurrentMemoryContext);

    MemoryContextDeleteChildren(context);

    /*
     * It's not entirely clear whether 'tis better to do this before or after
     * delinking the context; but an error in a callback will likely result in
     * leaking the whole context (if it's not a root context) if we do it
     * after, so let's do it before.
     */
    MemoryContextCallResetCallbacks(context);

    /*
     * We delink the context from its parent before deleting it, so that if
     * there's an error we won't have deleted/busted contexts still attached
     * to the context tree.  Better a leak than a crash.
     */
    MemoryContextSetParent(context, NULL);

    (*context->methods->delete_context) (context);
    VALGRIND_DESTROY_MEMPOOL(context);
    pfree(context);
}

/*
 * MemoryContextDeleteChildren
 *        Delete all the descendants of the named context and release all
 *        space allocated therein.  The named context itself is not touched.
 */
void
MemoryContextDeleteChildren(MemoryContext context)
{
    AssertArg(MemoryContextIsValid(context));

    /*
     * MemoryContextDelete will delink the child from me, so just iterate as
     * long as there is a child.
     */
    while (context->firstchild != NULL)
        MemoryContextDelete(context->firstchild);
}

/*
 * MemoryContextRegisterResetCallback
 *        Register a function to be called before next context reset/delete.
 *        Such callbacks will be called in reverse order of registration.
 *
 * The caller is responsible for allocating a MemoryContextCallback struct
 * to hold the info about this callback request, and for filling in the
 * "func" and "arg" fields in the struct to show what function to call with
 * what argument.  Typically the callback struct should be allocated within
 * the specified context, since that means it will automatically be freed
 * when no longer needed.
 *
 * There is no API for deregistering a callback once registered.  If you
 * want it to not do anything anymore, adjust the state pointed to by its
 * "arg" to indicate that.
 */
void
MemoryContextRegisterResetCallback(MemoryContext context,
                                   MemoryContextCallback *cb)
{
    AssertArg(MemoryContextIsValid(context));

    /* Push onto head so this will be called before older registrants. */
    cb->next = context->reset_cbs;
    context->reset_cbs = cb;
    /* Mark the context as non-reset (it probably is already). */
    context->isReset = false;
}

/*
 * MemoryContextCallResetCallbacks
 *        Internal function to call all registered callbacks for context.
 */
static void
MemoryContextCallResetCallbacks(MemoryContext context)
{
    MemoryContextCallback *cb;

    /*
     * We pop each callback from the list before calling.  That way, if an
     * error occurs inside the callback, we won't try to call it a second time
     * in the likely event that we reset or delete the context later.
     */
    while ((cb = context->reset_cbs) != NULL)
    {
        context->reset_cbs = cb->next;
        (*cb->func) (cb->arg);
    }
}

/*
 * MemoryContextSetParent
 *        Change a context to belong to a new parent (or no parent).
 *
 * We provide this as an API function because it is sometimes useful to
 * change a context's lifespan after creation.  For example, a context
 * might be created underneath a transient context, filled with data,
 * and then reparented underneath CacheMemoryContext to make it long-lived.
 * In this way no special effort is needed to get rid of the context in case
 * a failure occurs before its contents are completely set up.
 *
 * Callers often assume that this function cannot fail, so don't put any
 * elog(ERROR) calls in it.
 *
 * A possible caller error is to reparent a context under itself, creating
 * a loop in the context graph.  We assert here that context != new_parent,
 * but checking for multi-level loops seems more trouble than it's worth.
 */
void
MemoryContextSetParent(MemoryContext context, MemoryContext new_parent)
{
    AssertArg(MemoryContextIsValid(context));
    AssertArg(context != new_parent);

    /* Fast path if it's got correct parent already */
    if (new_parent == context->parent)
        return;

    /* Delink from existing parent, if any */
    if (context->parent)
    {
        MemoryContext parent = context->parent;

        if (context->prevchild != NULL)
            context->prevchild->nextchild = context->nextchild;
        else
        {
            Assert(parent->firstchild == context);
            parent->firstchild = context->nextchild;
        }

        if (context->nextchild != NULL)
            context->nextchild->prevchild = context->prevchild;
    }

    /* And relink */
    if (new_parent)
    {
        AssertArg(MemoryContextIsValid(new_parent));
        context->parent = new_parent;
        context->prevchild = NULL;
        context->nextchild = new_parent->firstchild;
        if (new_parent->firstchild != NULL)
            new_parent->firstchild->prevchild = context;
        new_parent->firstchild = context;
    }
    else
    {
        context->parent = NULL;
        context->prevchild = NULL;
        context->nextchild = NULL;
    }
}

/*
 * MemoryContextAllowInCriticalSection
 *        Allow/disallow allocations in this memory context within a critical
 *        section.
 *
 * Normally, memory allocations are not allowed within a critical section,
 * because a failure would lead to PANIC.  There are a few exceptions to
 * that, like allocations related to debugging code that is not supposed to
 * be enabled in production.  This function can be used to exempt specific
 * memory contexts from the assertion in palloc().
 */
void
MemoryContextAllowInCriticalSection(MemoryContext context, bool allow)
{
    AssertArg(MemoryContextIsValid(context));

    context->allowInCritSection = allow;
}

/*
 * GetMemoryChunkSpace
 *        Given a currently-allocated chunk, determine the total space
 *        it occupies (including all memory-allocation overhead).
 *
 * This is useful for measuring the total space occupied by a set of
 * allocated chunks.
 */
Size
GetMemoryChunkSpace(void *pointer)
{
    MemoryContext context = GetMemoryChunkContext(pointer);

    return (context->methods->get_chunk_space) (context,
                                                pointer);
}

/*
 * MemoryContextGetParent
 *        Get the parent context (if any) of the specified context
 */
MemoryContext
MemoryContextGetParent(MemoryContext context)
{
    AssertArg(MemoryContextIsValid(context));

    return context->parent;
}

/*
 * MemoryContextIsEmpty
 *        Is a memory context empty of any allocated space?
 */
bool
MemoryContextIsEmpty(MemoryContext context)
{
    AssertArg(MemoryContextIsValid(context));

    /*
     * For now, we consider a memory context nonempty if it has any children;
     * perhaps this should be changed later.
     */
    if (context->firstchild != NULL)
        return false;
    /* Otherwise use the type-specific inquiry */
    return (*context->methods->is_empty) (context);
}

/*
 * MemoryContextStats
 *        Print statistics about the named context and all its descendants.
 *
 * This is just a debugging utility, so it's not very fancy.  However, we do
 * make some effort to summarize when the output would otherwise be very long.
 * The statistics are sent to stderr.
 */
void
MemoryContextStats(MemoryContext context)
{
    /* A hard-wired limit on the number of children is usually good enough */
    MemoryContextStatsDetail(context, 100);
}

/*
 * MemoryContextStatsDetail
 *
 * Entry point for use if you want to vary the number of child contexts shown.
 */
void
MemoryContextStatsDetail(MemoryContext context, int max_children)
{
    MemoryContextCounters grand_totals;

    memset(&grand_totals, 0, sizeof(grand_totals));

    MemoryContextStatsInternal(context, 0, true, max_children, &grand_totals);

    fprintf(stderr,
            "Grand total: %zu bytes in %zd blocks; %zu free (%zd chunks); %zu used\n",
            grand_totals.totalspace, grand_totals.nblocks,
            grand_totals.freespace, grand_totals.freechunks,
            grand_totals.totalspace - grand_totals.freespace);
}

/*
 * MemoryContextStatsInternal
 *        One recursion level for MemoryContextStats
 *
 * Print this context if print is true, but in any case accumulate counts into
 * *totals (if given).
 */
static void
MemoryContextStatsInternal(MemoryContext context, int level,
                           bool print, int max_children,
                           MemoryContextCounters *totals)
{
    MemoryContextCounters local_totals;
    MemoryContext child;
    int            ichild;

    AssertArg(MemoryContextIsValid(context));

    /* Examine the context itself */
    (*context->methods->stats) (context, level, print, totals);

    /*
     * Examine children.  If there are more than max_children of them, we do
     * not print the rest explicitly, but just summarize them.
     */
    memset(&local_totals, 0, sizeof(local_totals));

    for (child = context->firstchild, ichild = 0;
         child != NULL;
         child = child->nextchild, ichild++)
    {
        if (ichild < max_children)
            MemoryContextStatsInternal(child, level + 1,
                                       print, max_children,
                                       totals);
        else
            MemoryContextStatsInternal(child, level + 1,
                                       false, max_children,
                                       &local_totals);
    }

    /* Deal with excess children */
    if (ichild > max_children)
    {
        if (print)
        {
            int            i;

            for (i = 0; i <= level; i++)
                fprintf(stderr, "  ");
            fprintf(stderr,
                    "%d more child contexts containing %zu total in %zd blocks; %zu free (%zd chunks); %zu used\n",
                    ichild - max_children,
                    local_totals.totalspace,
                    local_totals.nblocks,
                    local_totals.freespace,
                    local_totals.freechunks,
                    local_totals.totalspace - local_totals.freespace);
        }

        if (totals)
        {
            totals->nblocks += local_totals.nblocks;
            totals->freechunks += local_totals.freechunks;
            totals->totalspace += local_totals.totalspace;
            totals->freespace += local_totals.freespace;
        }
    }
}

/*
 * MemoryContextCheck
 *        Check all chunks in the named context.
 *
 * This is just a debugging utility, so it's not fancy.
 */
#ifdef MEMORY_CONTEXT_CHECKING
void
MemoryContextCheck(MemoryContext context)
{
    MemoryContext child;

    AssertArg(MemoryContextIsValid(context));

    (*context->methods->check) (context);
    for (child = context->firstchild; child != NULL; child = child->nextchild)
        MemoryContextCheck(child);
}
#endif

/*
 * MemoryContextContains
 *        Detect whether an allocated chunk of memory belongs to a given
 *        context or not.
 *
 * Caution: this test is reliable as long as 'pointer' does point to
 * a chunk of memory allocated from *some* context.  If 'pointer' points
 * at memory obtained in some other way, there is a small chance of a
 * false-positive result, since the bits right before it might look like
 * a valid chunk header by chance.
 */
bool
MemoryContextContains(MemoryContext context, void *pointer)
{
    MemoryContext ptr_context;

    /*
     * NB: Can't use GetMemoryChunkContext() here - that performs assertions
     * that aren't acceptable here since we might be passed memory not
     * allocated by any memory context.
     *
     * Try to detect bogus pointers handed to us, poorly though we can.
     * Presumably, a pointer that isn't MAXALIGNED isn't pointing at an
     * allocated chunk.
     */
    if (pointer == NULL || pointer != (void *) MAXALIGN(pointer))
        return false;

    /*
     * OK, it's probably safe to look at the context.
     */
    ptr_context = *(MemoryContext *) (((char *) pointer) - sizeof(void *));

    return ptr_context == context;
}

/*--------------------
 * MemoryContextCreate
 *        Context-type-independent part of context creation.
 *
 * This is only intended to be called by context-type-specific
 * context creation routines, not by the unwashed masses.
 *
 * The context creation procedure is a little bit tricky because
 * we want to be sure that we don't leave the context tree invalid
 * in case of failure (such as insufficient memory to allocate the
 * context node itself).  The procedure goes like this:
 *    1.  Context-type-specific routine first calls MemoryContextCreate(),
 *        passing the appropriate tag/size/methods values (the methods
 *        pointer will ordinarily point to statically allocated data).
 *        The parent and name parameters usually come from the caller.
 *    2.  MemoryContextCreate() attempts to allocate the context node,
 *        plus space for the name.  If this fails we can ereport() with no
 *        damage done.
 *    3.  We fill in all of the type-independent MemoryContext fields.
 *    4.  We call the type-specific init routine (using the methods pointer).
 *        The init routine is required to make the node minimally valid
 *        with zero chance of failure --- it can't allocate more memory,
 *        for example.
 *    5.  Now we have a minimally valid node that can behave correctly
 *        when told to reset or delete itself.  We link the node to its
 *        parent (if any), making the node part of the context tree.
 *    6.  We return to the context-type-specific routine, which finishes
 *        up type-specific initialization.  This routine can now do things
 *        that might fail (like allocate more memory), so long as it's
 *        sure the node is left in a state that delete will handle.
 *
 * This protocol doesn't prevent us from leaking memory if step 6 fails
 * during creation of a top-level context, since there's no parent link
 * in that case.  However, if you run out of memory while you're building
 * a top-level context, you might as well go home anyway...
 *
 * Normally, the context node and the name are allocated from
 * TopMemoryContext (NOT from the parent context, since the node must
 * survive resets of its parent context!).  However, this routine is itself
 * used to create TopMemoryContext!  If we see that TopMemoryContext is NULL,
 * we assume we are creating TopMemoryContext and use malloc() to allocate
 * the node.
 *
 * Note that the name field of a MemoryContext does not point to
 * separately-allocated storage, so it should not be freed at context
 * deletion.
 *--------------------
 */
MemoryContext
MemoryContextCreate(NodeTag tag, Size size,
                    MemoryContextMethods *methods,
                    MemoryContext parent,
                    const char *name)
{
    MemoryContext node;
    Size        needed = size + strlen(name) + 1;

    /* creating new memory contexts is not allowed in a critical section */
    Assert(CritSectionCount == 0);

    /* Get space for node and name */
    if (TopMemoryContext != NULL)
    {
        /* Normal case: allocate the node in TopMemoryContext */
        node = (MemoryContext) MemoryContextAlloc(TopMemoryContext,
                                                  needed);
    }
    else
    {
        /* Special case for startup: use good ol' malloc */
        node = (MemoryContext) malloc(needed);
        Assert(node != NULL);
    }

    /* Initialize the node as best we can */
    MemSet(node, 0, size);
    node->type = tag;
    node->methods = methods;
    node->parent = NULL;        /* for the moment */
    node->firstchild = NULL;
    node->prevchild = NULL;
    node->nextchild = NULL;
    node->isReset = true;
    node->name = ((char *) node) + size;
    strcpy(node->name, name);

    /* Type-specific routine finishes any other essential initialization */
    (*node->methods->init) (node);

    /* OK to link node to parent (if any) */
    /* Could use MemoryContextSetParent here, but doesn't seem worthwhile */
    if (parent)
    {
        node->parent = parent;
        node->nextchild = parent->firstchild;
        if (parent->firstchild != NULL)
            parent->firstchild->prevchild = node;
        parent->firstchild = node;
        /* inherit allowInCritSection flag from parent */
        node->allowInCritSection = parent->allowInCritSection;
    }

    VALGRIND_CREATE_MEMPOOL(node, 0, false);

    /* Return to type-specific creation routine to finish up */
    return node;
}

/*
 * MemoryContextAlloc
 *        Allocate space within the specified context.
 *
 * This could be turned into a macro, but we'd have to import
 * nodes/memnodes.h into postgres.h which seems a bad idea.
 */
void *
MemoryContextAlloc(MemoryContext context, Size size)
{
    void       *ret;

    AssertArg(MemoryContextIsValid(context));
    AssertNotInCriticalSection(context);

    if (!AllocSizeIsValid(size))
        elog(ERROR, "invalid memory alloc request size %zu", size);

    context->isReset = false;

    ret = (*context->methods->alloc) (context, size);
    if (ret == NULL)
    {
        MemoryContextStats(TopMemoryContext);
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory"),
                 errdetail("Failed on request of size %zu.", size)));
    }

    VALGRIND_MEMPOOL_ALLOC(context, ret, size);

    return ret;
}

/*
 * MemoryContextAllocZero
 *        Like MemoryContextAlloc, but clears allocated memory
 *
 *    We could just call MemoryContextAlloc then clear the memory, but this
 *    is a very common combination, so we provide the combined operation.
 */
void *
MemoryContextAllocZero(MemoryContext context, Size size)
{
    void       *ret;

    AssertArg(MemoryContextIsValid(context));
    AssertNotInCriticalSection(context);

    if (!AllocSizeIsValid(size))
        elog(ERROR, "invalid memory alloc request size %zu", size);

    context->isReset = false;

    ret = (*context->methods->alloc) (context, size);
    if (ret == NULL)
    {
        MemoryContextStats(TopMemoryContext);
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory"),
                 errdetail("Failed on request of size %zu.", size)));
    }

    VALGRIND_MEMPOOL_ALLOC(context, ret, size);

    MemSetAligned(ret, 0, size);

    return ret;
}

/*
 * MemoryContextAllocZeroAligned
 *        MemoryContextAllocZero where length is suitable for MemSetLoop
 *
 *    This might seem overly specialized, but it's not because newNode()
 *    is so often called with compile-time-constant sizes.
 */
void *
MemoryContextAllocZeroAligned(MemoryContext context, Size size)
{
    void       *ret;

    AssertArg(MemoryContextIsValid(context));
    AssertNotInCriticalSection(context);

    if (!AllocSizeIsValid(size))
        elog(ERROR, "invalid memory alloc request size %zu", size);

    context->isReset = false;

    ret = (*context->methods->alloc) (context, size);
    if (ret == NULL)
    {
        MemoryContextStats(TopMemoryContext);
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory"),
                 errdetail("Failed on request of size %zu.", size)));
    }

    VALGRIND_MEMPOOL_ALLOC(context, ret, size);

    MemSetLoop(ret, 0, size);

    return ret;
}

/*
 * MemoryContextAllocExtended
 *        Allocate space within the specified context using the given flags.
 */
void *
MemoryContextAllocExtended(MemoryContext context, Size size, int flags)
{// #lizard forgives
    void       *ret;

    AssertArg(MemoryContextIsValid(context));
    AssertNotInCriticalSection(context);

    if (((flags & MCXT_ALLOC_HUGE) != 0 && !AllocHugeSizeIsValid(size)) ||
        ((flags & MCXT_ALLOC_HUGE) == 0 && !AllocSizeIsValid(size)))
        elog(ERROR, "invalid memory alloc request size %zu", size);

    context->isReset = false;

    ret = (*context->methods->alloc) (context, size);
    if (ret == NULL)
    {
        if ((flags & MCXT_ALLOC_NO_OOM) == 0)
        {
            MemoryContextStats(TopMemoryContext);
            ereport(ERROR,
                    (errcode(ERRCODE_OUT_OF_MEMORY),
                     errmsg("out of memory"),
                     errdetail("Failed on request of size %zu.", size)));
        }
        return NULL;
    }

    VALGRIND_MEMPOOL_ALLOC(context, ret, size);

    if ((flags & MCXT_ALLOC_ZERO) != 0)
        MemSetAligned(ret, 0, size);

    return ret;
}

void *
palloc(Size size)
{
    /* duplicates MemoryContextAlloc to avoid increased overhead */
    void       *ret;

    AssertArg(MemoryContextIsValid(CurrentMemoryContext));
    AssertNotInCriticalSection(CurrentMemoryContext);

    if (!AllocSizeIsValid(size))
        elog(ERROR, "invalid memory alloc request size %zu", size);

    CurrentMemoryContext->isReset = false;

    ret = (*CurrentMemoryContext->methods->alloc) (CurrentMemoryContext, size);
    if (ret == NULL)
    {
        MemoryContextStats(TopMemoryContext);
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory"),
                 errdetail("Failed on request of size %zu.", size)));
    }

    VALGRIND_MEMPOOL_ALLOC(CurrentMemoryContext, ret, size);

    return ret;
}

void *
palloc0(Size size)
{
    /* duplicates MemoryContextAllocZero to avoid increased overhead */
    void       *ret;

    AssertArg(MemoryContextIsValid(CurrentMemoryContext));
    AssertNotInCriticalSection(CurrentMemoryContext);

    if (!AllocSizeIsValid(size))
        elog(ERROR, "invalid memory alloc request size %zu", size);

    CurrentMemoryContext->isReset = false;

    ret = (*CurrentMemoryContext->methods->alloc) (CurrentMemoryContext, size);
    if (ret == NULL)
    {
        MemoryContextStats(TopMemoryContext);
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory"),
                 errdetail("Failed on request of size %zu.", size)));
    }

    VALGRIND_MEMPOOL_ALLOC(CurrentMemoryContext, ret, size);

    MemSetAligned(ret, 0, size);

    return ret;
}

void *
palloc_extended(Size size, int flags)
{// #lizard forgives
    /* duplicates MemoryContextAllocExtended to avoid increased overhead */
    void       *ret;

    AssertArg(MemoryContextIsValid(CurrentMemoryContext));
    AssertNotInCriticalSection(CurrentMemoryContext);

    if (((flags & MCXT_ALLOC_HUGE) != 0 && !AllocHugeSizeIsValid(size)) ||
        ((flags & MCXT_ALLOC_HUGE) == 0 && !AllocSizeIsValid(size)))
        elog(ERROR, "invalid memory alloc request size %zu", size);

    CurrentMemoryContext->isReset = false;

    ret = (*CurrentMemoryContext->methods->alloc) (CurrentMemoryContext, size);
    if (ret == NULL)
    {
        if ((flags & MCXT_ALLOC_NO_OOM) == 0)
        {
            MemoryContextStats(TopMemoryContext);
            ereport(ERROR,
                    (errcode(ERRCODE_OUT_OF_MEMORY),
                     errmsg("out of memory"),
                     errdetail("Failed on request of size %zu.", size)));
        }
        return NULL;
    }

    VALGRIND_MEMPOOL_ALLOC(CurrentMemoryContext, ret, size);

    if ((flags & MCXT_ALLOC_ZERO) != 0)
        MemSetAligned(ret, 0, size);

    return ret;
}

/*
 * pfree
 *        Release an allocated chunk.
 */
void
pfree(void *pointer)
{
    MemoryContext context = GetMemoryChunkContext(pointer);

    (*context->methods->free_p) (context, pointer);
    VALGRIND_MEMPOOL_FREE(context, pointer);
}

/*
 * repalloc
 *        Adjust the size of a previously allocated chunk.
 */
void *
repalloc(void *pointer, Size size)
{
    MemoryContext context = GetMemoryChunkContext(pointer);
    void       *ret;

    if (!AllocSizeIsValid(size))
        elog(ERROR, "invalid memory alloc request size %zu", size);

    AssertNotInCriticalSection(context);

    /* isReset must be false already */
    Assert(!context->isReset);

    ret = (*context->methods->realloc) (context, pointer, size);
    if (ret == NULL)
    {
        MemoryContextStats(TopMemoryContext);
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory"),
                 errdetail("Failed on request of size %zu.", size)));
    }

    VALGRIND_MEMPOOL_CHANGE(context, pointer, ret, size);

    return ret;
}

/*
 * MemoryContextAllocHuge
 *        Allocate (possibly-expansive) space within the specified context.
 *
 * See considerations in comment at MaxAllocHugeSize.
 */
void *
MemoryContextAllocHuge(MemoryContext context, Size size)
{
    void       *ret;

    AssertArg(MemoryContextIsValid(context));
    AssertNotInCriticalSection(context);

    if (!AllocHugeSizeIsValid(size))
        elog(ERROR, "invalid memory alloc request size %zu", size);

    context->isReset = false;

    ret = (*context->methods->alloc) (context, size);
    if (ret == NULL)
    {
        MemoryContextStats(TopMemoryContext);
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory"),
                 errdetail("Failed on request of size %zu.", size)));
    }

    VALGRIND_MEMPOOL_ALLOC(context, ret, size);

    return ret;
}

/*
 * repalloc_huge
 *        Adjust the size of a previously allocated chunk, permitting a large
 *        value.  The previous allocation need not have been "huge".
 */
void *
repalloc_huge(void *pointer, Size size)
{
    MemoryContext context = GetMemoryChunkContext(pointer);
    void       *ret;

    if (!AllocHugeSizeIsValid(size))
        elog(ERROR, "invalid memory alloc request size %zu", size);

    AssertNotInCriticalSection(context);

    /* isReset must be false already */
    Assert(!context->isReset);

    ret = (*context->methods->realloc) (context, pointer, size);
    if (ret == NULL)
    {
        MemoryContextStats(TopMemoryContext);
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory"),
                 errdetail("Failed on request of size %zu.", size)));
    }

    VALGRIND_MEMPOOL_CHANGE(context, pointer, ret, size);

    return ret;
}

/*
 * MemoryContextStrdup
 *        Like strdup(), but allocate from the specified context
 */
char *
MemoryContextStrdup(MemoryContext context, const char *string)
{
    char       *nstr;
    Size        len = strlen(string) + 1;

    nstr = (char *) MemoryContextAlloc(context, len);

    memcpy(nstr, string, len);

    return nstr;
}

char *
pstrdup(const char *in)
{
    return MemoryContextStrdup(CurrentMemoryContext, in);
}

/*
 * pnstrdup
 *        Like pstrdup(), but append null byte to a
 *        not-necessarily-null-terminated input string.
 */
char *
pnstrdup(const char *in, Size len)
{
    char       *out = palloc(len + 1);

    memcpy(out, in, len);
    out[len] = '\0';
    return out;
}

#ifdef PGXC
#include "gen_alloc.h"

void *current_memcontext(void);

void *current_memcontext()
{
    return((void *)CurrentMemoryContext);
}

void *allocTopCxt(size_t s)
{
    return MemoryContextAlloc(TopMemoryContext, (Size)s);
}

Gen_Alloc genAlloc_class = {(void *)MemoryContextAlloc,
                            (void *)MemoryContextAllocZero,
                            (void *)repalloc,
                            (void *)pfree,
                            (void *)current_memcontext,
                            (void *)allocTopCxt};

#endif

/*
 * Make copy of string with all trailing newline characters removed.
 */
char *
pchomp(const char *in)
{
    size_t        n;

    n = strlen(in);
    while (n > 0 && in[n - 1] == '\n')
        n--;
    return pnstrdup(in, n);
}
#ifdef __TBASE__
#define    MAX_MCTX_STAT_LENGTH    2048

typedef struct
{
    char     *mctx_name;
    char     *parent_mctx;
    int     level;
    int        index_on_parent;
    long    self_totalspace;
    long    self_freespace;
    long    all_totalspace;
    long    all_freespace;
}MctxStat;

typedef struct
{
    int    currIdx;
    int length;
    MctxStat    mctxstat[MAX_MCTX_STAT_LENGTH];    
} ShmMgr_State;


static int get_mctx_stat(MemoryContext mctx, 
                    MemoryContext parent, 
                    int level,
                    int ind_on_parent, 
                    const int ind_on_stat, 
                    MctxStat *stat_arr);

int get_mctx_stat(MemoryContext mctx, 
                    MemoryContext parent, 
                    int level,
                    int ind_on_parent, 
                    const int ind_on_stat, 
                    MctxStat *stat_arr)
{
    MemoryContext    iter;
    int                child_index = 0;
    int                itr_indx_on_stat = 0;
    int             next_ind_on_stat = 0;
    MctxStat * stat = NULL;

    if(ind_on_stat >= MAX_MCTX_STAT_LENGTH)
        return ind_on_stat;

    stat = &stat_arr[ind_on_stat];
    stat->mctx_name = pstrdup(mctx->name);
    stat->parent_mctx = parent ? pstrdup(parent->name) : NULL;
    stat->index_on_parent = ind_on_parent;
    stat->level = level;
    stat->self_freespace = -1;
    stat->self_totalspace = -1;
    if(IsA(mctx,AllocSetContext))
    {
        AllocSetStats_Output(mctx, &stat->self_totalspace, &stat->self_freespace);
        stat->all_freespace = stat->self_freespace;
        stat->all_totalspace = stat->self_totalspace;
    }

    itr_indx_on_stat = ind_on_stat + 1;
    child_index = 0;
    iter = mctx->firstchild;
    while(iter)
    {
        next_ind_on_stat = get_mctx_stat(iter, mctx, level+1, child_index, itr_indx_on_stat, stat_arr);
        iter = iter->nextchild;

        stat->all_freespace += stat_arr[itr_indx_on_stat].all_freespace;
        stat->all_totalspace += stat_arr[itr_indx_on_stat].all_totalspace;

        itr_indx_on_stat = next_ind_on_stat;
         
        child_index++;
    }
    
    return itr_indx_on_stat;
}


int32 get_total_memory_size(void)
{
#define LINUX_KERNEL_PAGE_SIZE 4096
#define FILE_BUF_LEN           1024
    int32  size          = 0; 
    Size   nTotalPage    = 0;
    Size   nRssPage      = 0;
    Size   nSharePage    = 0;
    char   kfile[FILE_BUF_LEN]   = {0};
    char   linebuf[FILE_BUF_LEN] = {0};
    FILE   *handle       = NULL;
    ShmMgr_State    *mctx_status  = NULL;

    /* try to use linux kernel info to calculate first*/    
    snprintf(kfile, FILE_BUF_LEN, "/proc/%d/statm", getpid());
    handle = fopen(kfile, "r");
    if (NULL == handle)
    {
        elog(LOG, "open file:%s failed for %s", kfile, strerror(errno));
        mctx_status = (ShmMgr_State *) palloc(sizeof(ShmMgr_State));
        get_mctx_stat(TopMemoryContext, NULL, 0, 0, 0, mctx_status->mctxstat);
        size = (mctx_status->mctxstat[0].all_totalspace + (1024 * 1024) - 1)/ (1024 * 1024);
        pfree((void*)mctx_status);        
    }
    else
    {
        if (fgets(linebuf, FILE_BUF_LEN, handle) > 0)
        {
            /* read first three value of the file */
            if (3 == sscanf(linebuf, "%lu %lu %lu", &nTotalPage, &nRssPage, &nSharePage))
            {
                if (nRssPage >= nSharePage)
                {
                    size = ((nRssPage - nSharePage) * LINUX_KERNEL_PAGE_SIZE ) / (1024 * 1024);
                }                
            }
        }            
        fclose(handle);
    }        
    return size;
}
#endif
