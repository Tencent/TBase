/*-------------------------------------------------------------------------
 *
 * remotecopy.c
 *        Implements an extension of COPY command for remote management
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012, Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *        src/backend/pgxc/copy/remotecopy.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "optimizer/pgxcship.h"
#include "optimizer/planner.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/remotecopy.h"
#include "rewrite/rewriteHandler.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#ifdef PGXC
#include "utils/lsyscache.h"
#endif

static void RemoteCopy_QuoteStr(StringInfo query_buf, char *value);

/*
 * RemoteCopy_GetRelationLoc
 * Get relation node list based on COPY data involved. An empty list is
 * returned to caller if relation involved has no locator information
 * as it is the case of a system relation.
 */
void
RemoteCopy_GetRelationLoc(RemoteCopyData *state,
                          Relation rel,
                          List *attnums)
{
    /*
     * If target table does not exists on nodes (e.g. system table)
     * the location info returned is NULL. This is the criteria, when
     * we need to run COPY on Coordinator
     */
    state->rel_loc = GetRelationLocInfo(RelationGetRelid(rel));

    if (state->rel_loc &&
            AttributeNumberIsValid(state->rel_loc->partAttrNum))
    {
        TupleDesc tdesc;
        Form_pg_attribute pattr;
        /* determine distribution column data type */
        tdesc = RelationGetDescr(rel);

        pattr = tdesc->attrs[state->rel_loc->partAttrNum - 1];
        state->dist_type = pattr->atttypid;

#ifdef __COLD_HOT__
        if (AttributeNumberIsValid(state->rel_loc->secAttrNum))
        {
            pattr = tdesc->attrs[state->rel_loc->secAttrNum - 1];

            state->sec_dist_type = pattr->atttypid;
        }
        else
        {
            state->sec_dist_type = InvalidOid;
        }
#endif
    }
    else
    {
        state->dist_type = InvalidOid;

#ifdef __COLD_HOT__
        state->sec_dist_type = InvalidOid;
#endif
    }

    state->locator = NULL;
}

/*
 * RemoteCopy_BuildStatement
 * Build a COPY query for remote management
 */
void
RemoteCopy_BuildStatement(RemoteCopyData *state,
                          Relation rel,
                          RemoteCopyOptions *options,
                          List *attnamelist,
                          List *attnums,
#ifdef _SHARDING_
                          const Bitmapset *shards
#endif
                            )
{// #lizard forgives
    int            attnum;
    TupleDesc    tupDesc = RelationGetDescr(rel);
#ifdef __TBASE__
    RelationLocInfo *relation_loc_info = rel->rd_locator_info;
#endif
    /*
     * Build up query string for the Datanodes, it should match
     * to original string, but should have STDIN/STDOUT instead
     * of filename.
     */
    initStringInfo(&state->query_buf);
    appendStringInfoString(&state->query_buf, "COPY ");

    /*
     * The table name should be qualified, unless the table is a temporary table
     */
    if (rel->rd_backend == MyBackendId)
        appendStringInfo(&state->query_buf, "%s",
                         quote_identifier(RelationGetRelationName(rel)));
    else
        appendStringInfo(&state->query_buf, "%s",
                         quote_qualified_identifier(
                                get_namespace_name(RelationGetNamespace(rel)),
                                RelationGetRelationName(rel)));

    if (attnamelist)
    {
        ListCell *cell;
        ListCell *prev = NULL;
        appendStringInfoString(&state->query_buf, " (");
        foreach (cell, attnamelist)
        {
            if (prev)
                appendStringInfoString(&state->query_buf, ", ");
            appendStringInfoString(&state->query_buf,
                                   quote_identifier(strVal(lfirst(cell))));
            prev = cell;
        }

        /*
         * For COPY FROM, we need to append unspecified attributes that have
         * default expressions associated.
         */
        if (state->is_from)
        {
            for (attnum = 1; attnum <= tupDesc->natts; attnum++)
            {
                /* Don't let dropped attributes go into the column list */
                if (tupDesc->attrs[attnum - 1]->attisdropped)
                    continue;

                if (!list_member_int(attnums, attnum))
                {
                    /* Append only if the default expression is not shippable. */
                    Expr *defexpr = (Expr*) build_column_default(rel, attnum);
                    if (defexpr &&
                        !pgxc_is_expr_shippable(expression_planner(defexpr), NULL))
                        {
                            appendStringInfoString(&state->query_buf, ", ");
                            appendStringInfoString(&state->query_buf,
                               quote_identifier(NameStr(tupDesc->attrs[attnum - 1]->attname)));
                        }
#ifdef __TBASE__
                    /* distributed column also need to be set as default column */
                    else if (defexpr && IsDistributedColumn(attnum, relation_loc_info))
                    {
                            appendStringInfoString(&state->query_buf, ", ");
                            appendStringInfoString(&state->query_buf,
                               quote_identifier(NameStr(tupDesc->attrs[attnum - 1]->attname)));
                    }
#endif
                }
            }
        }

        appendStringInfoChar(&state->query_buf, ')');
    }

#ifdef _SHARDING_
    if(shards)
    {
        Bitmapset * tmp_bms = bms_copy(shards);
        int shardid = 0;
        bool isfirst = true;

        appendStringInfoString(&state->query_buf, " SHARDING (");
        
        while((shardid = bms_first_member(tmp_bms)) != -1)
        {
            if(!isfirst)            
                appendStringInfoString(&state->query_buf, ", ");
            else
                isfirst = false;

            appendStringInfo(&state->query_buf, "%d", shardid);
        }

        appendStringInfoChar(&state->query_buf, ')');

        bms_free(tmp_bms);
    }
#endif

    if (state->is_from)
        appendStringInfoString(&state->query_buf, " FROM STDIN");
    else
        appendStringInfoString(&state->query_buf, " TO STDOUT");

    if (options->rco_binary)
        appendStringInfoString(&state->query_buf, " BINARY");

    if (options->rco_oids)
        appendStringInfoString(&state->query_buf, " OIDS");

    if (options->rco_delim)
    {
        if ((!options->rco_csv_mode && options->rco_delim[0] != '\t')
            || (options->rco_csv_mode && options->rco_delim[0] != ','))
        {
            appendStringInfoString(&state->query_buf, " DELIMITER AS ");
            RemoteCopy_QuoteStr(&state->query_buf, options->rco_delim);
        }
    }

    if (options->rco_null_print)
    {
        if ((!options->rco_csv_mode && strcmp(options->rco_null_print, "\\N"))
            || (options->rco_csv_mode && strcmp(options->rco_null_print, "")))
        {
            appendStringInfoString(&state->query_buf, " NULL AS ");
            RemoteCopy_QuoteStr(&state->query_buf, options->rco_null_print);
        }
    }

    if (options->rco_csv_mode)
        appendStringInfoString(&state->query_buf, " CSV");
#ifdef __TBASE__
    if (options->rco_insert_into)
    {
        appendStringInfoString(&state->query_buf, " ROWS");
    }
#endif

    /*
     * It is not necessary to send the HEADER part to Datanodes.
     * Sending data is sufficient.
     */
    if (options->rco_quote && options->rco_quote[0] != '"')
    {
        appendStringInfoString(&state->query_buf, " QUOTE AS ");
        RemoteCopy_QuoteStr(&state->query_buf, options->rco_quote);
    }

    if (options->rco_escape && options->rco_quote && options->rco_escape[0] != options->rco_quote[0])
    {
        appendStringInfoString(&state->query_buf, " ESCAPE AS ");
        RemoteCopy_QuoteStr(&state->query_buf, options->rco_escape);
    }

    if (options->rco_force_quote)
    {
        ListCell *cell;
        ListCell *prev = NULL;
        appendStringInfoString(&state->query_buf, " FORCE QUOTE ");
        foreach (cell, options->rco_force_quote)
        {
            if (prev)
                appendStringInfoString(&state->query_buf, ", ");
            appendStringInfoString(&state->query_buf,
                                   quote_identifier(strVal(lfirst(cell))));
            prev = cell;
        }
    }

    if (options->rco_force_notnull)
    {
        ListCell *cell;
        ListCell *prev = NULL;
        appendStringInfoString(&state->query_buf, " FORCE NOT NULL ");
        foreach (cell, options->rco_force_notnull)
        {
            if (prev)
                appendStringInfoString(&state->query_buf, ", ");
            appendStringInfoString(&state->query_buf,
                                   quote_identifier(strVal(lfirst(cell))));
            prev = cell;
        }
    }
}


/*
 * Build a default set for RemoteCopyOptions
 */
RemoteCopyOptions *
makeRemoteCopyOptions(void)
{
    RemoteCopyOptions *res = (RemoteCopyOptions *) palloc(sizeof(RemoteCopyOptions));
    res->rco_binary = false;
    res->rco_oids = false;
    res->rco_csv_mode = false;
    res->rco_delim = NULL;
    res->rco_null_print = NULL;
    res->rco_quote = NULL;
    res->rco_escape = NULL;
    res->rco_force_quote = NIL;
    res->rco_force_notnull = NIL;
#ifdef __TBASE__
    res->rco_insert_into = false;
#endif
    return res;
}


/*
 * FreeRemoteCopyOptions
 * Free remote COPY options structure
 */
void
FreeRemoteCopyOptions(RemoteCopyOptions *options)
{// #lizard forgives
    /* Leave if nothing */
    if (options == NULL)
        return;

    /* Free field by field */
    if (options->rco_delim)
        pfree(options->rco_delim);
    if (options->rco_null_print)
        pfree(options->rco_null_print);
    if (options->rco_quote)
        pfree(options->rco_quote);
    if (options->rco_escape)
        pfree(options->rco_escape);
    if (options->rco_force_quote)
        list_free(options->rco_force_quote);
    if (options->rco_force_notnull)
        list_free(options->rco_force_notnull);

    /* Then finish the work */
    pfree(options);
}


/*
 * FreeRemoteCopyData
 * Free remote COPY state data structure
 */
void
FreeRemoteCopyData(RemoteCopyData *state)
{
    /* Leave if nothing */
    if (state == NULL)
        return;
    if (state->locator)
        freeLocator(state->locator);
    if (state->query_buf.data)
        pfree(state->query_buf.data);
    FreeRelationLocInfo(state->rel_loc);
    pfree(state);
}

#define APPENDSOFAR(query_buf, start, current) \
    if (current > start) \
        appendBinaryStringInfo(query_buf, start, current - start)

/*
 * RemoteCopy_QuoteStr
 * Append quoted value to the query buffer. Value is escaped if needed
 * When rewriting query to be sent down to nodes we should escape special
 * characters, that may present in the value. The characters are backslash(\)
 * and single quote ('). These characters are escaped by doubling. We do not
 * have to escape characters like \t, \v, \b, etc. because Datanode interprets
 * them properly.
 * We use E'...' syntax for literals containing backslashes.
 */
static void
RemoteCopy_QuoteStr(StringInfo query_buf, char *value)
{
    char   *start = value;
    char   *current = value;
    char    c;
    bool    has_backslash = (strchr(value, '\\') != NULL);

    if (has_backslash)
        appendStringInfoChar(query_buf, 'E');

    appendStringInfoChar(query_buf, '\'');

    while ((c = *current) != '\0')
    {
        switch (c)
        {
            case '\\':
            case '\'':
                APPENDSOFAR(query_buf, start, current);
                /* Double current */
                appendStringInfoChar(query_buf, c);
                /* Second current will be appended next time */
                start = current;
                /* fallthru */
            default:
                current++;
        }
    }
    APPENDSOFAR(query_buf, start, current);
    appendStringInfoChar(query_buf, '\'');
}
