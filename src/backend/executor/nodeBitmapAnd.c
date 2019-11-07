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
 * nodeBitmapAnd.c
 *      routines to handle BitmapAnd nodes.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *      src/backend/executor/nodeBitmapAnd.c
 *
 *-------------------------------------------------------------------------
 */
/* INTERFACE ROUTINES
 *        ExecInitBitmapAnd    - initialize the BitmapAnd node
 *        MultiExecBitmapAnd    - retrieve the result bitmap from the node
 *        ExecEndBitmapAnd    - shut down the BitmapAnd node
 *        ExecReScanBitmapAnd - rescan the BitmapAnd node
 *
 *     NOTES
 *        BitmapAnd nodes don't make use of their left and right
 *        subtrees, rather they maintain a list of subplans,
 *        much like Append nodes.  The logic is much simpler than
 *        Append, however, since we needn't cope with forward/backward
 *        execution.
 */

#include "postgres.h"

#include "executor/execdebug.h"
#include "executor/nodeBitmapAnd.h"


/* ----------------------------------------------------------------
 *        ExecBitmapAnd
 *
 *        stub for pro forma compliance
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecBitmapAnd(PlanState *pstate)
{
    elog(ERROR, "BitmapAnd node does not support ExecProcNode call convention");
    return NULL;
}

/* ----------------------------------------------------------------
 *        ExecInitBitmapAnd
 *
 *        Begin all of the subscans of the BitmapAnd node.
 * ----------------------------------------------------------------
 */
BitmapAndState *
ExecInitBitmapAnd(BitmapAnd *node, EState *estate, int eflags)
{
    BitmapAndState *bitmapandstate = makeNode(BitmapAndState);
    PlanState **bitmapplanstates;
    int            nplans;
    int            i;
    ListCell   *l;
    Plan       *initNode;

    /* check for unsupported flags */
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    /*
     * Set up empty vector of subplan states
     */
    nplans = list_length(node->bitmapplans);

    bitmapplanstates = (PlanState **) palloc0(nplans * sizeof(PlanState *));

    /*
     * create new BitmapAndState for our BitmapAnd node
     */
    bitmapandstate->ps.plan = (Plan *) node;
    bitmapandstate->ps.state = estate;
    bitmapandstate->ps.ExecProcNode = ExecBitmapAnd;
    bitmapandstate->bitmapplans = bitmapplanstates;
    bitmapandstate->nplans = nplans;

    /*
     * Miscellaneous initialization
     *
     * BitmapAnd plans don't have expression contexts because they never call
     * ExecQual or ExecProject.  They don't need any tuple slots either.
     */

    /*
     * call ExecInitNode on each of the plans to be executed and save the
     * results into the array "bitmapplanstates".
     */
    i = 0;
    foreach(l, node->bitmapplans)
    {
        initNode = (Plan *) lfirst(l);
        bitmapplanstates[i] = ExecInitNode(initNode, estate, eflags);
        i++;
    }

    return bitmapandstate;
}

/* ----------------------------------------------------------------
 *       MultiExecBitmapAnd
 * ----------------------------------------------------------------
 */
Node *
MultiExecBitmapAnd(BitmapAndState *node)
{// #lizard forgives
    PlanState **bitmapplans;
    int            nplans;
    int            i;
    TIDBitmap  *result = NULL;

    /* must provide our own instrumentation support */
    if (node->ps.instrument)
        InstrStartNode(node->ps.instrument);

    /*
     * get information from the node
     */
    bitmapplans = node->bitmapplans;
    nplans = node->nplans;

    /*
     * Scan all the subplans and AND their result bitmaps
     */
    for (i = 0; i < nplans; i++)
    {
        PlanState  *subnode = bitmapplans[i];
        TIDBitmap  *subresult;

        subresult = (TIDBitmap *) MultiExecProcNode(subnode);

        if (!subresult || !IsA(subresult, TIDBitmap))
            elog(ERROR, "unrecognized result from subplan");

        if (result == NULL)
            result = subresult; /* first subplan */
        else
        {
            tbm_intersect(result, subresult);
            tbm_free(subresult);
        }

        /*
         * If at any stage we have a completely empty bitmap, we can fall out
         * without evaluating the remaining subplans, since ANDing them can no
         * longer change the result.  (Note: the fact that indxpath.c orders
         * the subplans by selectivity should make this case more likely to
         * occur.)
         */
        if (tbm_is_empty(result))
            break;
    }

    if (result == NULL)
        elog(ERROR, "BitmapAnd doesn't support zero inputs");

    /* must provide our own instrumentation support */
    if (node->ps.instrument)
        InstrStopNode(node->ps.instrument, 0 /* XXX */ );

    return (Node *) result;
}

/* ----------------------------------------------------------------
 *        ExecEndBitmapAnd
 *
 *        Shuts down the subscans of the BitmapAnd node.
 *
 *        Returns nothing of interest.
 * ----------------------------------------------------------------
 */
void
ExecEndBitmapAnd(BitmapAndState *node)
{
    PlanState **bitmapplans;
    int            nplans;
    int            i;

    /*
     * get information from the node
     */
    bitmapplans = node->bitmapplans;
    nplans = node->nplans;

    /*
     * shut down each of the subscans (that we've initialized)
     */
    for (i = 0; i < nplans; i++)
    {
        if (bitmapplans[i])
            ExecEndNode(bitmapplans[i]);
    }
}

void
ExecReScanBitmapAnd(BitmapAndState *node)
{
    int            i;

    for (i = 0; i < node->nplans; i++)
    {
        PlanState  *subnode = node->bitmapplans[i];

        /*
         * ExecReScan doesn't know about my subplans, so I have to do
         * changed-parameter signaling myself.
         */
        if (node->ps.chgParam != NULL)
            UpdateChangedParamSet(subnode, node->ps.chgParam);

        /*
         * If chgParam of subnode is not null then plan will be re-scanned by
         * first ExecProcNode.
         */
        if (subnode->chgParam == NULL)
            ExecReScan(subnode);
    }
}
