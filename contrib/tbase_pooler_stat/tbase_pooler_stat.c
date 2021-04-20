/*
 * contrib/tbase_pooler_stat/tbase_pooler_stat.c
 *
 * tbase_pooler_stat.c
 *
 * Copyright (c) 2020 Tbase Kernel Group
 *
 * Permission to use, copy, modify, and distribute this software and
 * its documentation for any purpose, without fee, and without a
 * written agreement is hereby granted, provided that the above
 * copyright notice and this paragraph and the following two
 * paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE TO ANY PARTY FOR DIRECT,
 * INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
 * DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * THE AUTHOR SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS
 * IS" BASIS, AND THE AUTHOR HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE,
 * SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 */

#include "postgres.h"
#include "funcapi.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include <endian.h>
#include "pgxc/poolmgr.h"
#include "libpq/pqformat.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(tbase_get_pooler_cmd_statistics);
PG_FUNCTION_INFO_V1(tbase_reset_pooler_cmd_statistics);
PG_FUNCTION_INFO_V1(tbase_get_pooler_conn_statistics);

typedef struct
{
    uint32               currIdx;         /* current handle item id */
    PoolerCmdStatistics  *buf;            /* a fixed length buf store the result */
} Pooler_CmdState;

typedef struct
{
    uint32	     total_node_cursor;    /* total connection nodes count */
    const char   *database;            /* node_cursor's database */
    const char   *username;            /* node_cursor's username */
    uint32       node_cursor;          /* current handle node cursor */
    StringInfo   buf;                  /* a stringInfo buf store the result */
} Pooler_ConnState;


/* the g_pooler_cmd_name_tab and g_pooler_cmd must be in the same order */
static char *g_pooler_cmd_name_tab[POOLER_CMD_COUNT] =
{
    "ABORT",                  /* ABORT */
    "FIRE_TRANSACTION_BLOCK", /* Fire transaction-block commands on given nodes */
    "CONNECT",                /* CONNECT */
    "DISCONNECT",             /* DISCONNECT */
    "CLEAN_CONN",             /* CLEAN CONNECTION */
    "GET_CONN",               /* GET CONNECTIONS */
    "CANCEL_SQL",             /* Cancel SQL Command in progress on specified connections */
    "LOCK_UNLOCK_POOLER",     /* Lock/unlock pooler */
    "RELOAD_CONN",            /* Reload connection info */
    "PING_CONN",              /* Ping connection info */
    "CHECK_CONN",             /* Check connection info consistency */
    "RELEASE_CONN",           /* RELEASE CONNECTIONS */
    "REFRESH_CONN",           /* Refresh connection info */
    "SESSION_RELATED",        /* Session-related COMMAND */
    "CLOSE_POOLER_CONN",      /* Close pooler connections*/
    "GET_CMD_STATSTICS",      /* Get command statistics */
    "RESET_CMD_STATISTICS",   /* Reset command statistics */
    "GET_CONN_STATISTICS"     /* Get connection statistics */
};

/*
 * get pooler command statistics
 */
Datum
tbase_get_pooler_cmd_statistics(PG_FUNCTION_ARGS)
{
#define  LIST_POOLER_CMD_STATISTICS_COLUMNS 5
    FuncCallContext 	*funcctx;
    int32               ret = 0;
    Pooler_CmdState     *status = NULL;
    Datum		        values[LIST_POOLER_CMD_STATISTICS_COLUMNS];
    bool		        nulls[LIST_POOLER_CMD_STATISTICS_COLUMNS];
    HeapTuple	        tuple;
    Datum		        result;
    PoolerCmdStatistics stat_info;
    int                 size = sizeof(PoolerCmdStatistics) * POOLER_CMD_COUNT;

    MemSet(values, 0, sizeof(values));
    MemSet(nulls,  0, sizeof(nulls));

    if (SRF_IS_FIRSTCALL())
    {
        MemoryContext oldcontext;
        TupleDesc	  tupdesc;

        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(LIST_POOLER_CMD_STATISTICS_COLUMNS, false);

        TupleDescInitEntry(tupdesc, (AttrNumber) 1, "command_type",
                           TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 2, "request_times",
                           INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 3, "avg_costtime",
                           INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 4, "max_costtime",
                           INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 5, "min_costtime",
                           INT8OID, -1, 0);
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        status = (Pooler_CmdState*) palloc(sizeof(Pooler_CmdState));
        status->currIdx = 0;
        status->buf = (PoolerCmdStatistics*) palloc(size);

        funcctx->user_fctx = (void*) status;

        ret = PoolManagerGetCmdStatistics((char*)status->buf, size);
        if (ret)
        {
            elog(ERROR, "get pooler cmd statictics info from pooler failed");
        }

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    status  = (Pooler_CmdState *) funcctx->user_fctx;

    while (status->currIdx < POOLER_CMD_COUNT)
    {
        stat_info.total_request_times = be64toh(status->buf[status->currIdx].total_request_times);
        stat_info.total_costtime = be64toh(status->buf[status->currIdx].total_costtime);
        stat_info.max_costtime = be64toh(status->buf[status->currIdx].max_costtime);
        stat_info.min_costtime = be64toh(status->buf[status->currIdx].min_costtime);

        /* avg_costtime */
        stat_info.avg_costtime = (stat_info.total_request_times == 0) ? 0 : (stat_info.total_costtime / stat_info.total_request_times);

        values[0] = CStringGetTextDatum(g_pooler_cmd_name_tab[status->currIdx]);
        values[1] = Int64GetDatum(stat_info.total_request_times);
        values[2] = Int64GetDatum(stat_info.avg_costtime);
        values[3] = Int64GetDatum(stat_info.max_costtime);
        values[4] = Int64GetDatum(stat_info.min_costtime);

        status->currIdx++;

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    }

    SRF_RETURN_DONE(funcctx);
}

/*
 * reset pooler command statistics
 */
Datum
tbase_reset_pooler_cmd_statistics(PG_FUNCTION_ARGS)
{
    PoolManagerResetCmdStatistics();

    PG_RETURN_VOID();
}

/*
 * get pooler connections statistics
 */
Datum
tbase_get_pooler_conn_statistics(PG_FUNCTION_ARGS)
{
#define  LIST_POOLER_CONN_STATISTICS_COLUMNS 12
    FuncCallContext 	 *funcctx = NULL;
    int32                ret = 0;
    Pooler_ConnState     *status = NULL;
    Datum		         values[LIST_POOLER_CONN_STATISTICS_COLUMNS];
    bool		         nulls[LIST_POOLER_CONN_STATISTICS_COLUMNS];
    HeapTuple	         tuple;
    Datum		         result;

    if (SRF_IS_FIRSTCALL())
    {
        MemoryContext oldcontext;
        TupleDesc	  tupdesc;

        /* content will destroy in SRF_RETURN_DONE */
        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(LIST_POOLER_CONN_STATISTICS_COLUMNS, false);
        TupleDescInitEntry(tupdesc, (AttrNumber) 1, "database",
                           NAMEOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 2, "user_name",
                           NAMEOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 3, "node_name",
                           NAMEOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 4, "oid",
                           OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 5, "is_coord",
                           BOOLOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 6, "conn_cnt",
                           INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 7, "free_cnt",
                           INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 8, "warming_cnt",
                           INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 9, "query_cnt",
                           INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 10, "exceed_keepalive_cnt",
                           INT4OID, -1, 0);
        /*
         * This field is reserved for compatibility
         */
        TupleDescInitEntry(tupdesc, (AttrNumber) 11, "exceed_deadtime_cnt",
                           INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 12, "exceed_maxlifetime_cnt",
                           INT4OID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        status = (Pooler_ConnState*) palloc(sizeof(Pooler_ConnState));
        status->database = NULL;
        status->username = NULL;
        status->node_cursor = 0;
        status->buf = makeStringInfo();

        funcctx->user_fctx = (void*) status;

        ret = PoolManagerGetConnStatistics(status->buf);
        if (ret)
        {
            elog(ERROR, "get pooler conn statictics info from pooler failed");
        }
        else
        {
            status->total_node_cursor = pq_getmsgint(status->buf, sizeof(uint32));
        }

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    status  = (Pooler_ConnState *) funcctx->user_fctx;

    while (status->total_node_cursor)
    {
        MemSet(values, 0, sizeof(values));
        MemSet(nulls,  0, sizeof(nulls));

        if (status->node_cursor == 0)
        {
            /* get next database and username */
            status->database = pq_getmsgstring(status->buf);
            status->username = pq_getmsgstring(status->buf);
            status->node_cursor = pq_getmsgint(status->buf, sizeof(uint32));
        }

        values[0] = CStringGetDatum(status->database);
        values[1] = CStringGetDatum(status->username);
        if (status->node_cursor == 0)
        {
            nulls[2] = true;
            nulls[3] = true;
            nulls[4] = true;
            nulls[5] = true;
            nulls[6] = true;
            nulls[7] = true;
            nulls[8] = true;
            nulls[9] = true;
            nulls[10] = true;
            nulls[11] = true;
        }
        else
        {
            values[2] = CStringGetDatum(pq_getmsgstring(status->buf));
            values[3] = ObjectIdGetDatum(pq_getmsgint(status->buf, sizeof(Oid)));
            values[4] = BoolGetDatum(pq_getmsgint(status->buf, sizeof(bool)));
            values[5] = UInt32GetDatum(pq_getmsgint(status->buf, sizeof(uint32)));
            values[6] = UInt32GetDatum(pq_getmsgint(status->buf, sizeof(uint32)));
            values[7] = UInt32GetDatum(pq_getmsgint(status->buf, sizeof(uint32)));
            values[8] = UInt32GetDatum(pq_getmsgint(status->buf, sizeof(uint32)));
            values[9] = UInt32GetDatum(pq_getmsgint(status->buf, sizeof(uint32)));
            values[10] = UInt32GetDatum(0);
            values[11] = UInt32GetDatum(pq_getmsgint(status->buf, sizeof(uint32)));
            status->node_cursor--;
        }

        status->total_node_cursor--;

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    }

    SRF_RETURN_DONE(funcctx);
}