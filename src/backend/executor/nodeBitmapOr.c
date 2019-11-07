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
 * nodeBitmapOr.c
 *      routines to handle BitmapOr nodes.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *      src/backend/executor/nodeBitmapOr.c
 *
 *-------------------------------------------------------------------------
 */
/* INTERFACE ROUTINES
 *        ExecInitBitmapOr    - initialize the BitmapOr node
 *        MultiExecBitmapOr    - retrieve the result bitmap from the node
 *        ExecEndBitmapOr        - shut down the BitmapOr node
 *        ExecReScanBitmapOr    - rescan the BitmapOr node
 *
 *     NOTES
 *        BitmapOr nodes don't make use of their left and right
 *        subtrees, rather they maintain a list of subplans,
 *        much like Append nodes.  The logic is much simpler than
 *        Append, however, since we needn't cope with forward/backward
 *        execution.
 */

#include "postgres.h"

#include "executor/execdebug.h"
#include "executor/nodeBitmapOr.h"
#include "miscadmin.h"


/* ----------------------------------------------------------------
 *        ExecBitmapOr
 *
 *        stub for pro forma compliance
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecBitmapOr(PlanState *pstate)
{
    elog(ERROR, "BitmapOr node does not support ExecProcNode call convention");
    return NULL;
}

/* ----------------------------------------------------------------
 *        ExecInitBitmapOr
 *
 *        Begin all of the subscans of the BitmapOr node.
 * ----------------------------------------------------------------
 */
BitmapOrState *
ExecInitBitmapOr(BitmapOr *node, EState *estate, int eflags)
{
    BitmapOrState *bitmaporstate = makeNode(BitmapOrState);
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
     * create new BitmapOrState for our BitmapOr node
     */
    bitmaporstate->ps.plan = (Plan *) node;
    bitmaporstate->ps.state = estate;
    bitmaporstate->ps.ExecProcNode = ExecBitmapOr;
    bitmaporstate->bitmapplans = bitmapplanstates;
    bitmaporstate->nplans = nplans;

    /*
     * Miscellaneous initialization
     *
     * BitmapOr plans don't have expression contexts because they never call
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

    return bitmaporstate;
}

/* ----------------------------------------------------------------
 *       MultiExecBitmapOr
 * ----------------------------------------------------------------
 */
Node *
MultiExecBitmapOr(BitmapOrState *node)
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
     * Scan all the subplans and OR their result bitmaps
     */
    for (i = 0; i < nplans; i++)
    {
        PlanState  *subnode = bitmapplans[i];
        TIDBitmap  *subresult;

        /*
         * We can special-case BitmapIndexScan children to avoid an explicit
         * tbm_union step for each child: just pass down the current result
         * bitmap and let the child OR directly into it.
         */
        if (IsA(subnode, BitmapIndexScanState))
        {
            if (result == NULL) /* first subplan */
            {
                /* XXX should we use less than work_mem for this? */
                result = tbm_create(work_mem * 1024L,
                                    ((BitmapOr *) node->ps.plan)->isshared ?
                                    node->ps.state->es_query_dsa : NULL);
            }

            ((BitmapIndexScanState *) subnode)->biss_result = result;

            subresult = (TIDBitmap *) MultiExecProcNode(subnode);

            if (subresult != result)
                elog(ERROR, "unrecognized result from subplan");
        }
        else
        {
            /* standard implementation */
            subresult = (TIDBitmap *) MultiExecProcNode(subnode);

            if (!subresult || !IsA(subresult, TIDBitmap))
                elog(ERROR, "unrecognized result from subplan");

            if (result == NULL)
                result = subresult; /* first subplan */
            else
            {
                tbm_union(result, subresult);
                tbm_free(subresult);
            }
        }
    }

    /* We could return an empty result set here? */
    if (result == NULL)
        elog(ERROR, "BitmapOr doesn't support zero inputs");

    /* must provide our own instrumentation support */
    if (node->ps.instrument)
        InstrStopNode(node->ps.instrument, 0 /* XXX */ );

    return (Node *) result;
}

/* ----------------------------------------------------------------
 *        ExecEndBitmapOr
 *
 *        Shuts down the subscans of the BitmapOr node.
 *
 *        Returns nothing of interest.
 * ----------------------------------------------------------------
 */
void
ExecEndBitmapOr(BitmapOrState *node)
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
ExecReScanBitmapOr(BitmapOrState *node)
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
