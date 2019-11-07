/*-------------------------------------------------------------------------
 *
 * gtm_serialize_debug.c
 *  Debug functionalities of serialization management
 *
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *        src/gtm/common/gtm_serialize_debug.c
 *
 *-------------------------------------------------------------------------
 */

#include "gtm/gtm_c.h"
#include "gtm/elog.h"
#include "gtm/palloc.h"
#include "gtm/gtm.h"
#include "gtm/gtm_txn.h"
#include "gtm/gtm_seq.h"
#include "gtm/assert.h"
#include "gtm/register.h"
#include "gtm/stringinfo.h"
#include "gtm/libpq.h"
#include "gtm/pqformat.h"
#include "gtm/gtm_msg.h"

#include "gtm/gtm_serialize.h"

void
dump_transactioninfo_elog(GTM_TransactionInfo *txn)
{
    int ii;

    elog(LOG, "  ========= GTM_TransactionInfo =========");
    elog(LOG, "gti_handle: %d", txn->gti_handle);
    elog(LOG, "gti_client_id: %u", txn->gti_client_id);
    elog(LOG, "gti_in_use: %d", txn->gti_in_use);
    elog(LOG, "gti_gxid: %d", txn->gti_gxid);
    elog(LOG, "gti_state: %d", txn->gti_state);
    elog(LOG, "gti_xmin: %d", txn->gti_xmin);
    elog(LOG, "gti_isolevel: %d", txn->gti_isolevel);
    elog(LOG, "gti_readonly: %d", txn->gti_readonly);
    elog(LOG, "gti_proxy_client_id: %d", txn->gti_proxy_client_id);
    elog(LOG, "gti_nodestring: %s", txn->nodestring);
    elog(LOG, "gti_gid: %s", txn->gti_gid);

    elog(LOG, "  sn_xmin: %d", txn->gti_current_snapshot.sn_xmin);
    elog(LOG, "  sn_xmax: %d", txn->gti_current_snapshot.sn_xmax);
    elog(LOG, "  sn_xcnt: %d", txn->gti_current_snapshot.sn_xcnt);

    /* Print all the GXIDs in snapshot */
    for (ii = 0; ii < txn->gti_current_snapshot.sn_xcnt; ii++)
    {
        elog (LOG, "  sn_xip[%d]: %d", ii, txn->gti_current_snapshot.sn_xip[ii]);
    }

    elog(LOG, "gti_snapshot_set: %d", txn->gti_snapshot_set);
    elog(LOG, "gti_vacuum: %d", txn->gti_vacuum);
    elog(LOG, "  ========================================");
}

void
dump_transactions_elog(GTM_Transactions *txn, int num_txn)
{
    int i;

    elog(LOG, "============ GTM_Transactions ============");
    elog(LOG, "  gt_txn_count: %d", txn->gt_txn_count);
    elog(LOG, "  gt_XidGenLock: %p", &txn->gt_XidGenLock);
    elog(LOG, "  gt_nextXid: %d", txn->gt_nextXid);
    elog(LOG, "  gt_oldestXid: %d", txn->gt_oldestXid);
    elog(LOG, "  gt_xidVacLimit: %d", txn->gt_xidVacLimit);
    elog(LOG, "  gt_xidWarnLimit: %d", txn->gt_xidWarnLimit);
    elog(LOG, "  gt_xidStopLimit: %d", txn->gt_xidStopLimit);
    elog(LOG, "  gt_xidWrapLimit: %d", txn->gt_xidWrapLimit);
    elog(LOG, "  gt_latestCompletedXid: %d", txn->gt_latestCompletedXid);
    elog(LOG, "  gt_recent_global_xmin: %d", txn->gt_recent_global_xmin);
    elog(LOG, "  gt_lastslot: %d", txn->gt_lastslot);

    for (i = 0; i < num_txn; i++)
    {
        if (txn->gt_transactions_array[i].gti_gxid != InvalidGlobalTransactionId)
            dump_transactioninfo_elog(&txn->gt_transactions_array[i]);
    }

    elog(LOG, "  gt_TransArrayLock: %p", &txn->gt_TransArrayLock);
    elog(LOG, "==========================================");
}
