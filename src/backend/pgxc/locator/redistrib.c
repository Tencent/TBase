/*-------------------------------------------------------------------------
 *
 * redistrib.c
 *      Routines related to online data redistribution
 *
 * Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *      src/backend/pgxc/locator/redistrib.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "access/hash.h"
#include "access/htup.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "catalog/pgxc_node.h"
#include "commands/tablecmds.h"
#include "pgxc/copyops.h"
#include "pgxc/execRemote.h"
#include "pgxc/pgxc.h"
#include "pgxc/redistrib.h"
#include "pgxc/remotecopy.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#define IsCommandTypePreUpdate(x) (x == CATALOG_UPDATE_BEFORE || \
                                   x == CATALOG_UPDATE_BOTH)
#define IsCommandTypePostUpdate(x) (x == CATALOG_UPDATE_AFTER || \
                                    x == CATALOG_UPDATE_BOTH)

/* Functions used for the execution of redistribution commands */
static void distrib_execute_query(char *sql, bool is_temp, ExecNodes *exec_nodes);
static void distrib_execute_command(RedistribState *distribState, RedistribCommand *command);
static void distrib_copy_to(RedistribState *distribState);
static void distrib_copy_from(RedistribState *distribState, ExecNodes *exec_nodes);
static void distrib_truncate(RedistribState *distribState, ExecNodes *exec_nodes);
static void distrib_reindex(RedistribState *distribState, ExecNodes *exec_nodes);
static void distrib_delete_hash(RedistribState *distribState, ExecNodes *exec_nodes);

/* Functions used to build the command list */
static void pgxc_redist_build_entry(RedistribState *distribState,
                                RelationLocInfo *oldLocInfo,
                                RelationLocInfo *newLocInfo);
static void pgxc_redist_build_replicate(RedistribState *distribState,
                                RelationLocInfo *oldLocInfo,
                                RelationLocInfo *newLocInfo);
static void pgxc_redist_build_replicate_to_distrib(RedistribState *distribState,
                                RelationLocInfo *oldLocInfo,
                                RelationLocInfo *newLocInfo);

static void pgxc_redist_build_default(RedistribState *distribState);
static void pgxc_redist_add_reindex(RedistribState *distribState);


/*
 * PGXCRedistribTable
 * Execute redistribution operations after catalog update
 */
void
PGXCRedistribTable(RedistribState *distribState, RedistribCatalog type)
{// #lizard forgives
    ListCell *item;

    /* Nothing to do if no redistribution operation */
    if (!distribState)
        return;

    /* Nothing to do if on remote node */
    if (IS_PGXC_DATANODE || IsConnFromCoord())
        return;

    /* Execute each command if necessary */
    foreach(item, distribState->commands)
    {
        RedistribCommand *command = (RedistribCommand *)lfirst(item);

        /* Check if command can be run */
        if (!IsCommandTypePostUpdate(type) &&
            IsCommandTypePostUpdate(command->updateState))
            continue;
        if (!IsCommandTypePreUpdate(type) &&
            IsCommandTypePreUpdate(command->updateState))
            continue;

        /* Now enter in execution list */
        distrib_execute_command(distribState, command);
    }
}


/*
 * PGXCRedistribCreateCommandList
 * Look for the list of necessary commands to perform table redistribution.
 */
void
PGXCRedistribCreateCommandList(RedistribState *distribState, RelationLocInfo *newLocInfo)
{
    Relation rel;
    RelationLocInfo *oldLocInfo;

    rel = relation_open(distribState->relid, NoLock);
    oldLocInfo = RelationGetLocInfo(rel);

    /* Build redistribution command list */
    pgxc_redist_build_entry(distribState, oldLocInfo, newLocInfo);

    relation_close(rel, NoLock);
}


/*
 * pgxc_redist_build_entry
 * Entry point for command list building
 */
static void
pgxc_redist_build_entry(RedistribState *distribState,
                        RelationLocInfo *oldLocInfo,
                        RelationLocInfo *newLocInfo)
{
    /* If distribution has not changed at all, nothing to do */
    if (IsLocatorInfoEqual(oldLocInfo, newLocInfo))
        return;

    /* Evaluate cases for replicated tables */
    pgxc_redist_build_replicate(distribState, oldLocInfo, newLocInfo);

    /* Evaluate cases for replicated to distributed tables */
    pgxc_redist_build_replicate_to_distrib(distribState, oldLocInfo, newLocInfo);

    /* PGXCTODO: perform more complex builds of command list */

    /* Fallback to default */
    pgxc_redist_build_default(distribState);
}


/*
 * pgxc_redist_build_replicate_to_distrib
 * Build redistribution command list from replicated to distributed
 * table.
 */
static void
pgxc_redist_build_replicate_to_distrib(RedistribState *distribState,
                            RelationLocInfo *oldLocInfo,
                            RelationLocInfo *newLocInfo)
{// #lizard forgives
    List *removedNodes;
    List *newNodes;

    /* If a command list has already been built, nothing to do */
    if (list_length(distribState->commands) != 0)
        return;

    /* Redistribution is done from replication to distributed (with value) */
    if (!IsLocatorReplicated(oldLocInfo->locatorType) ||
        !IsLocatorDistributedByValue(newLocInfo->locatorType))
        return;

    /* Get the list of nodes that are added to the relation */
    removedNodes = list_difference_int(oldLocInfo->rl_nodeList, newLocInfo->rl_nodeList);

    /* Get the list of nodes that are removed from relation */
    newNodes = list_difference_int(newLocInfo->rl_nodeList, oldLocInfo->rl_nodeList);

    /*
     * If some nodes are added, turn back to default, we need to fetch data
     * and then redistribute it properly.
     */
    if (newNodes != NIL)
        return;

    /* Nodes removed have to be truncated, so add a TRUNCATE commands to removed nodes */
    if (removedNodes != NIL)
    {
        ExecNodes *execNodes = makeNode(ExecNodes);
        execNodes->nodeList = removedNodes;
        /* Add TRUNCATE command */
        distribState->commands = lappend(distribState->commands,
                     makeRedistribCommand(DISTRIB_TRUNCATE, CATALOG_UPDATE_BEFORE, execNodes));
    }

    /*
     * If the table is redistributed to a single node, a TRUNCATE on removed nodes
     * is sufficient so leave here.
     */
    if (list_length(newLocInfo->rl_nodeList) == 1)
    {
        /* Add REINDEX command if necessary */
        pgxc_redist_add_reindex(distribState);
        return;
    }

    /*
     * If we are here we are sure that redistribution only requires to delete data on remote
     * nodes on the new subset of nodes. So launch to remote nodes a DELETE command that only
     * eliminates the data not verifying the new hashing condition.
     */
    if (newLocInfo->locatorType == LOCATOR_TYPE_HASH)
    {
        ExecNodes *execNodes = makeNode(ExecNodes);
        execNodes->nodeList = newLocInfo->rl_nodeList;
        distribState->commands = lappend(distribState->commands,
                     makeRedistribCommand(DISTRIB_DELETE_HASH, CATALOG_UPDATE_AFTER, execNodes));
    }
    else if (newLocInfo->locatorType == LOCATOR_TYPE_MODULO)
    {
        ExecNodes *execNodes = makeNode(ExecNodes);
        execNodes->nodeList = newLocInfo->rl_nodeList;
        distribState->commands = lappend(distribState->commands,
                     makeRedistribCommand(DISTRIB_DELETE_MODULO, CATALOG_UPDATE_AFTER, execNodes));
    }
    else
        ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("Incorrect redistribution operation")));

    /* Add REINDEX command if necessary */
    pgxc_redist_add_reindex(distribState);
}


/*
 * pgxc_redist_build_replicate
 * Build redistribution command list for replicated tables
 */
static void
pgxc_redist_build_replicate(RedistribState *distribState,
                            RelationLocInfo *oldLocInfo,
                            RelationLocInfo *newLocInfo)
{
    List *removedNodes;
    List *newNodes;

    /* If a command list has already been built, nothing to do */
    if (list_length(distribState->commands) != 0)
        return;

    /* Case of a replicated table whose set of nodes is changed */
    if (!IsLocatorReplicated(newLocInfo->locatorType) ||
        !IsLocatorReplicated(oldLocInfo->locatorType))
        return;

    /* Get the list of nodes that are added to the relation */
    removedNodes = list_difference_int(oldLocInfo->rl_nodeList, newLocInfo->rl_nodeList);

    /* Get the list of nodes that are removed from relation */
    newNodes = list_difference_int(newLocInfo->rl_nodeList, oldLocInfo->rl_nodeList);

    /*
     * If nodes have to be added, we need to fetch data for redistribution first.
     * So add a COPY TO command to fetch data.
     */
    if (newNodes != NIL)
    {
        /* Add COPY TO command */
        distribState->commands = lappend(distribState->commands,
                     makeRedistribCommand(DISTRIB_COPY_TO, CATALOG_UPDATE_BEFORE, NULL));
    }

    /* Nodes removed have to be truncated, so add a TRUNCATE commands to removed nodes */
    if (removedNodes != NIL)
    {
        ExecNodes *execNodes = makeNode(ExecNodes);
        execNodes->nodeList = removedNodes;
        /* Add TRUNCATE command */
        distribState->commands = lappend(distribState->commands,
                     makeRedistribCommand(DISTRIB_TRUNCATE, CATALOG_UPDATE_BEFORE, execNodes));
    }

    /* If necessary, COPY the data obtained at first step to the new nodes. */
    if (newNodes != NIL)
    {
        ExecNodes *execNodes = makeNode(ExecNodes);
        execNodes->nodeList = newNodes;
        /* Add COPY FROM command */
        distribState->commands = lappend(distribState->commands,
                     makeRedistribCommand(DISTRIB_COPY_FROM, CATALOG_UPDATE_AFTER, execNodes));
    }

    /* Add REINDEX command if necessary */
    pgxc_redist_add_reindex(distribState);
}


/*
 * pgxc_redist_build_default
 * Build a default list consisting of
 * COPY TO -> TRUNCATE -> COPY FROM ( -> REINDEX )
 */
static void
pgxc_redist_build_default(RedistribState *distribState)
{
    /* If a command list has already been built, nothing to do */
    if (list_length(distribState->commands) != 0)
        return;

    /* COPY TO command */
    distribState->commands = lappend(distribState->commands,
                     makeRedistribCommand(DISTRIB_COPY_TO, CATALOG_UPDATE_BEFORE, NULL));
    /* TRUNCATE command */
    distribState->commands = lappend(distribState->commands,
                     makeRedistribCommand(DISTRIB_TRUNCATE, CATALOG_UPDATE_BEFORE, NULL));
    /* COPY FROM command */
    distribState->commands = lappend(distribState->commands,
                     makeRedistribCommand(DISTRIB_COPY_FROM, CATALOG_UPDATE_AFTER, NULL));

    /* REINDEX command */
    pgxc_redist_add_reindex(distribState);
}


/*
 * pgxc_redist_build_reindex
 * Add a reindex command if necessary
 */
static void
pgxc_redist_add_reindex(RedistribState *distribState)
{
    Relation rel;

    rel = relation_open(distribState->relid, NoLock);

    /* Build REINDEX command if necessary */
    if (RelationGetIndexList(rel) != NIL)
    {
        distribState->commands = lappend(distribState->commands,
             makeRedistribCommand(DISTRIB_REINDEX, CATALOG_UPDATE_AFTER, NULL));
    }

    relation_close(rel, NoLock);
}


/*
 * distrib_execute_command
 * Execute a redistribution operation
 */
static void
distrib_execute_command(RedistribState *distribState, RedistribCommand *command)
{// #lizard forgives
    struct rusage        start_r;
    struct timeval        start_t;
    char                *command_str = "unknown distrib_execute_command";

    ResetUsageCommon(&start_r, &start_t);

    /* Execute redistribution command */
    switch (command->type)
    {
        case DISTRIB_COPY_TO:
            distrib_copy_to(distribState);
            command_str = "Redistribution step: fetch remote tuples";
            break;
        case DISTRIB_COPY_FROM:
            distrib_copy_from(distribState, command->execNodes);
            command_str = "Redistribution step: distribute tuples";
            break;
        case DISTRIB_TRUNCATE:
            distrib_truncate(distribState, command->execNodes);
            command_str = "Redistribution step: truncate relation";
            break;
        case DISTRIB_REINDEX:
            distrib_reindex(distribState, command->execNodes);
            command_str = "Redistribution step: reindex relation";
            break;
        case DISTRIB_DELETE_HASH:
        case DISTRIB_DELETE_MODULO:
            distrib_delete_hash(distribState, command->execNodes);
            command_str = "Redistribution step: delete tuples";
            break;
        case DISTRIB_NONE:
        default:
            Assert(0); /* Should not happen */
    }

    ShowUsageCommon(command_str, &start_r, &start_t);
}


/*
 * distrib_copy_to
 * Copy all the data of table to be distributed.
 * This data is saved in a tuplestore saved in distribution state.
 * a COPY FROM operation is always done on nodes determined by the locator data
 * in catalogs, explaining why this cannot be done on a subset of nodes. It also
 * insures that no read operations are done on nodes where data is not yet located.
 */
static void
distrib_copy_to(RedistribState *distribState)
{
    Oid            relOid = distribState->relid;
    Relation    rel;
    RemoteCopyOptions *options;
    RemoteCopyData *copyState;
    Tuplestorestate *store; /* Storage of redistributed data */

    /* Fetch necessary data to prepare for the table data acquisition */
    options = makeRemoteCopyOptions();

    /* All the fields are separated by tabs in redistribution */
    options->rco_delim = palloc(2);
    options->rco_delim[0] = COPYOPS_DELIMITER;
    options->rco_delim[1] = '\0';

    copyState = (RemoteCopyData *) palloc0(sizeof(RemoteCopyData));
    copyState->is_from = false;

    /* A sufficient lock level needs to be taken at a higher level */
    rel = relation_open(relOid, NoLock);
    RemoteCopy_GetRelationLoc(copyState, rel, NIL);
    RemoteCopy_BuildStatement(copyState, rel, options, NIL, NIL, NULL);

    /* Inform client of operation being done */
    ereport(DEBUG1,
            (errmsg("Copying data for relation \"%s.%s\"",
                    get_namespace_name(RelationGetNamespace(rel)),
                    RelationGetRelationName(rel))));

    /* Begin the COPY process */
    DataNodeCopyBegin(copyState);

    /* Create tuplestore storage */
    store = tuplestore_begin_message(false, work_mem);

    tuplestore_collect_stat(store, "Redistribute_TS");

    /* Then get rows and copy them to the tuplestore used for redistribution */
    DataNodeCopyStore(
            (PGXCNodeHandle **) getLocatorNodeMap(copyState->locator),
            getLocatorNodeCount(copyState->locator), store);

    /* Do necessary clean-up */
    FreeRemoteCopyOptions(options);

    /* Lock is maintained until transaction commits */
    relation_close(rel, NoLock);

    /* Save results */
    distribState->store = store;
}


/*
 * PGXCDistribTableCopyFrom
 * Execute commands related to COPY FROM
 * Redistribute all the data of table with a COPY FROM from given tuplestore.
 */
static void
distrib_copy_from(RedistribState *distribState, ExecNodes *exec_nodes)
{// #lizard forgives
    Oid relOid = distribState->relid;
    Tuplestorestate *store = distribState->store;
    Relation    rel;
    RemoteCopyOptions *options;
    RemoteCopyData *copyState;
    TupleDesc tupdesc;
    /* May be needed to decode partitioning value */
    int         partIdx = -1;
    FmgrInfo     in_function;
    Oid         typioparam;
    int         typmod = 0;
#ifdef __COLD_HOT__
    int         secPartIdx = -1;
    FmgrInfo     in_function_for_sec;
    Oid         typioparam_for_sec;
    int         typmod_for_sec = 0;
#endif

    /* Nothing to do if on remote node */
    if (IS_PGXC_DATANODE || IsConnFromCoord())
        return;

    /* Fetch necessary data to prepare for the table data acquisition */
    options = makeRemoteCopyOptions();
    /* All the fields are separated by tabs in redistribution */
    options->rco_delim = palloc(2);
    options->rco_delim[0] = COPYOPS_DELIMITER;
    options->rco_delim[1] = '\0';

    copyState = (RemoteCopyData *) palloc0(sizeof(RemoteCopyData));
    copyState->is_from = true;

    /* A sufficient lock level needs to be taken at a higher level */
    rel = relation_open(relOid, NoLock);
    RemoteCopy_GetRelationLoc(copyState, rel, NIL);
    RemoteCopy_BuildStatement(copyState, rel, options, NIL, NIL, NULL);

    /* Modify relation location as requested */
    if (exec_nodes)
    {
        if (exec_nodes->nodeList)
            copyState->rel_loc->rl_nodeList = exec_nodes->nodeList;
    }

    tupdesc = RelationGetDescr(rel);
    if (AttributeNumberIsValid(copyState->rel_loc->partAttrNum))
    {
        Oid in_func_oid;
        int dropped = 0;
        int i;

        partIdx = copyState->rel_loc->partAttrNum - 1;

        /* prepare function to decode partitioning value */
        getTypeInputInfo(copyState->dist_type,
                         &in_func_oid, &typioparam);
        fmgr_info(in_func_oid, &in_function);
        typmod = tupdesc->attrs[partIdx]->atttypmod;

        /*
         * Make partIdx pointing to correct field of the datarow.
         * The data row does not contain data of dropped attributes, we should
         * decrement partIdx appropriately
         */
        for (i = 0; i < partIdx; i++)
        {
            if (tupdesc->attrs[i]->attisdropped)
                dropped++;
        }
        partIdx -= dropped;

#ifdef __COLD_HOT__
        if (AttributeNumberIsValid(copyState->rel_loc->secAttrNum))
        {
            secPartIdx = copyState->rel_loc->secAttrNum - 1;

            /* prepare function to decode second partitioning value */
            getTypeInputInfo(copyState->sec_dist_type,
                             &in_func_oid, &typioparam_for_sec);
            fmgr_info(in_func_oid, &in_function_for_sec);
            typmod_for_sec = tupdesc->attrs[secPartIdx]->atttypmod;

            /*
             * Make partIdx pointing to correct field of the datarow.
             * The data row does not contain data of dropped attributes, we should
             * decrement partIdx appropriately
             */
            for (i = 0; i < secPartIdx; i++)
            {
                if (tupdesc->attrs[i]->attisdropped)
                    dropped++;
            }
            secPartIdx -= dropped;
        }
#endif
    }

    /* Inform client of operation being done */
    ereport(DEBUG1,
            (errmsg("Redistributing data for relation \"%s.%s\"",
                    get_namespace_name(RelationGetNamespace(rel)),
                    RelationGetRelationName(rel))));

    DataNodeCopyBegin(copyState);

    /* Send each COPY message stored to remote nodes */
    while (true)
    {
        char   *data;
        int     len;
        Datum    value = (Datum) 0;
        bool    is_null = true;
#ifdef __COLD_HOT__
        Datum   secValue = (Datum) 0;
        bool    is_sec_null = true;
#endif

        /* 
         * Get message from the tuplestore
         *
         * The trailing \n is already removed while storing the message.
         */
        data = tuplestore_getmessage(store, &len);
        if (!data)
            break;

        /* Find value of distribution column if necessary */
        if (AttributeNumberIsValid(copyState->rel_loc->partAttrNum))
        {
            char       **fields;
            char       *tmpbuf = NULL;
            
            /*
             * Split message on an array of fields.
             */
            fields = CopyOps_RawDataToArrayField(tupdesc, data, len, &tmpbuf);

            Assert(partIdx >= 0);
            /* Determine partitioning value */
            if (fields[partIdx])
            {
                value = InputFunctionCall(&in_function, fields[partIdx],
                                          typioparam, typmod);
                is_null = false;

#ifdef __COLD_HOT__
                if (AttributeNumberIsValid(copyState->rel_loc->secAttrNum))
                {
                    if (secPartIdx >= 0)
                    {
                        secValue = InputFunctionCall(&in_function_for_sec, fields[secPartIdx],
                                          typioparam_for_sec, typmod_for_sec);
                        is_sec_null = false;
                    }
                }
#endif
            }

            if (tmpbuf)
                pfree(tmpbuf);
            pfree(fields);
        }

        if (DataNodeCopyIn(data, len,
#ifdef __COLD_HOT__
                           GET_NODES(copyState->locator, value, is_null, secValue, is_sec_null, NULL),
#else
                           GET_NODES(copyState->locator, value, is_null, NULL),
#endif
                           (PGXCNodeHandle**)
                           getLocatorResults(copyState->locator),
                           false))
        {
            int conn_count;
            int loop;
            PGXCNodeHandle** copy_connections;
            StringInfoData   sqldata;
            StringInfo       sql; 

            sql = &sqldata;

            initStringInfo(sql);
#ifdef __COLD_HOT__
            conn_count = GET_NODES(copyState->locator, value, is_null, secValue, is_sec_null, NULL);
#else
            conn_count = GET_NODES(copyState->locator, value, is_null, NULL);
#endif
            copy_connections = (PGXCNodeHandle**)getLocatorResults(copyState->locator);
            for(loop = 0; loop < conn_count; loop++)
            {
                PGXCNodeHandle *handle = copy_connections[loop];
                if ('\0' != handle->error[0])
                {
                    appendStringInfo(sql, "%s;", handle->error);
                }
            }

            ereport(ERROR,
                        (errcode(ERRCODE_CONNECTION_EXCEPTION),
                         errmsg("Copy failed on a data node:%s", sql->data)));
        }

        /* Clean up */
        pfree(data);
    }
    DataNodeCopyFinish(getLocatorNodeCount(copyState->locator),
            (PGXCNodeHandle **) getLocatorNodeMap(copyState->locator));

    /* Lock is maintained until transaction commits */
    relation_close(rel, NoLock);
}


/*
 * distrib_truncate
 * Truncate all the data of specified table.
 * This is used as a second step of online data redistribution.
 */
static void
distrib_truncate(RedistribState *distribState, ExecNodes *exec_nodes)
{
    Relation    rel;
    StringInfo    buf;
    Oid            relOid = distribState->relid;

    /* Nothing to do if on remote node */
    if (IS_PGXC_DATANODE || IsConnFromCoord())
        return;

    /* A sufficient lock level needs to be taken at a higher level */
    rel = relation_open(relOid, NoLock);

    /* Inform client of operation being done */
    ereport(DEBUG1,
            (errmsg("Truncating data for relation \"%s.%s\"",
                    get_namespace_name(RelationGetNamespace(rel)),
                    RelationGetRelationName(rel))));

    /* Initialize buffer */
    buf = makeStringInfo();

    /* Build query to clean up table before redistribution */
    appendStringInfo(buf, "TRUNCATE %s.%s",
                     get_namespace_name(RelationGetNamespace(rel)),
                     RelationGetRelationName(rel));

    /*
     * Lock is maintained until transaction commits,
     * relation needs also to be closed before effectively launching the query.
     */
    relation_close(rel, NoLock);

    /* Execute the query */
    distrib_execute_query(buf->data, IsTempTable(relOid), exec_nodes);

    /* Clean buffers */
    pfree(buf->data);
    pfree(buf);
}


/*
 * distrib_reindex
 * Reindex the table that has been redistributed
 */
static void
distrib_reindex(RedistribState *distribState, ExecNodes *exec_nodes)
{
    Relation    rel;
    StringInfo    buf;
    Oid            relOid = distribState->relid;

    /* Nothing to do if on remote node */
    if (IS_PGXC_DATANODE || IsConnFromCoord())
        return;

    /* A sufficient lock level needs to be taken at a higher level */
    rel = relation_open(relOid, NoLock);

    /* Inform client of operation being done */
    ereport(DEBUG1,
            (errmsg("Reindexing relation \"%s.%s\"",
                    get_namespace_name(RelationGetNamespace(rel)),
                    RelationGetRelationName(rel))));

    /* Initialize buffer */
    buf = makeStringInfo();

    /* Generate the query */
    appendStringInfo(buf, "REINDEX TABLE %s.%s",
                     get_namespace_name(RelationGetNamespace(rel)),
                     RelationGetRelationName(rel));

    /* Execute the query */
    distrib_execute_query(buf->data, IsTempTable(relOid), exec_nodes);

    /* Clean buffers */
    pfree(buf->data);
    pfree(buf);

    /* Lock is maintained until transaction commits */
    relation_close(rel, NoLock);
}


/*
 * distrib_delete_hash
 * Perform a partial tuple deletion of remote tuples not checking the correct hash
 * condition. The new distribution condition is set up in exec_nodes when building
 * the command list.
 */
static void
distrib_delete_hash(RedistribState *distribState, ExecNodes *exec_nodes)
{
    Relation    rel;
    StringInfo    buf;
    Oid            relOid = distribState->relid;
    ListCell   *item;

    /* Nothing to do if on remote node */
    if (IS_PGXC_DATANODE || IsConnFromCoord())
        return;

    /* A sufficient lock level needs to be taken at a higher level */
    rel = relation_open(relOid, NoLock);

    /* Inform client of operation being done */
    ereport(DEBUG1,
            (errmsg("Deleting necessary tuples \"%s.%s\"",
                    get_namespace_name(RelationGetNamespace(rel)),
                    RelationGetRelationName(rel))));

    /* Initialize buffer */
    buf = makeStringInfo();

    /* Build query to clean up table before redistribution */
    appendStringInfo(buf, "DELETE FROM %s.%s",
                     get_namespace_name(RelationGetNamespace(rel)),
                     RelationGetRelationName(rel));

    /*
     * Launch the DELETE query to each node as the DELETE depends on
     * local conditions for each node.
     */
    foreach(item, exec_nodes->nodeList)
    {
        StringInfo    buf2;
        char       *hashfuncname, *colname = NULL;
        Oid            hashtype;
        RelationLocInfo *locinfo = RelationGetLocInfo(rel);
        int            nodenum = lfirst_int(item);
        int            nodepos = 0;
        ExecNodes  *local_exec_nodes = makeNode(ExecNodes);
        TupleDesc    tupDesc = RelationGetDescr(rel);
        Form_pg_attribute *attr = tupDesc->attrs;
        ListCell   *item2;

        /* Here the query is launched to a unique node */
        local_exec_nodes->nodeList = lappend_int(NIL, nodenum);

        /* Get the hash type of relation */
        hashtype = attr[locinfo->partAttrNum - 1]->atttypid;

        /* Get function hash name */
        hashfuncname = get_compute_hash_function(hashtype, locinfo->locatorType);

        /* Get distribution column name */
        if (locinfo->locatorType == LOCATOR_TYPE_HASH)
            colname = GetRelationHashColumn(locinfo);
        else if (locinfo->locatorType == LOCATOR_TYPE_MODULO)
            colname = GetRelationModuloColumn(locinfo);
        else
            ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg("Incorrect redistribution operation")));

        /*
         * Find the correct node position in node list of locator information.
         * So scan the node list and fetch the position of node.
         */
        foreach(item2, locinfo->rl_nodeList)
        {
            int loc = lfirst_int(item2);
            if (loc == nodenum)
                break;
            nodepos++;
        }

        /*
         * Then build the WHERE clause for deletion.
         * The condition that allows to keep the tuples on remote nodes
         * is of the type "RemoteNodeNumber != (hash_func(dis_col)) %
         * NumDatanodes".
         *
         * (Earlier we were using abs(hashvalue), but that does not render the
         * same result as (unsigned int) (signed value). So we must do a modulo
         * 2^32 computation.
         *
         * The remote Datanode has no knowledge of its position in cluster so this
         * number needs to be compiled locally on Coordinator.
         * Taking the absolute value is necessary as hash may return a negative value.
         * For hash distributions a condition with correct hash function is used.
         * For modulo distribution, well we might need a hash function call but not
         * all the time, this is determined implicitely by get_compute_hash_function.
         */
        buf2 = makeStringInfo();
        if (hashfuncname)
            appendStringInfo(buf2, "%s WHERE ((2^32 + %s(%s))::bigint %% (2^32)::bigint) %% %d != %d",
                             buf->data, hashfuncname, colname,
                             list_length(locinfo->rl_nodeList), nodepos);
        else
            appendStringInfo(buf2, "%s WHERE ((2^32 + %s)::bigint %% (2^32)::bigint) %% %d != %d", buf->data, colname,
                             list_length(locinfo->rl_nodeList), nodepos);

        /* Then launch this single query */
        distrib_execute_query(buf2->data, IsTempTable(relOid), local_exec_nodes);

        FreeExecNodes(&local_exec_nodes);
        pfree(buf2->data);
        pfree(buf2);
    }

    relation_close(rel, NoLock);

    /* Clean buffers */
    pfree(buf->data);
    pfree(buf);
}


/*
 * makeRedistribState
 * Build a distribution state operator
 */
RedistribState *
makeRedistribState(Oid relOid)
{
    RedistribState *res = (RedistribState *) palloc(sizeof(RedistribState));
    res->relid = relOid;
    res->commands = NIL;
    res->store = NULL;
    return res;
}


/*
 * FreeRedistribState
 * Free given distribution state
 */
void
FreeRedistribState(RedistribState *state)
{
    ListCell *item;

    /* Leave if nothing to do */
    if (!state)
        return;

    foreach(item, state->commands)
        FreeRedistribCommand((RedistribCommand *) lfirst(item));
    if (list_length(state->commands) > 0)
        list_free(state->commands);
    if (state->store)
        tuplestore_end(state->store);
    pfree(state);
}

/*
 * makeRedistribCommand
 * Build a distribution command
 */
RedistribCommand *
makeRedistribCommand(RedistribOperation type, RedistribCatalog updateState, ExecNodes *nodes)
{
    RedistribCommand *res = (RedistribCommand *) palloc0(sizeof(RedistribCommand));
    res->type = type;
    res->updateState = updateState;
    res->execNodes = nodes;
    return res;
}

/*
 * FreeRedistribCommand
 * Free given distribution command
 */
void
FreeRedistribCommand(RedistribCommand *command)
{
    ExecNodes *nodes;
    /* Leave if nothing to do */
    if (!command)
        return;
    nodes = command->execNodes;

    if (nodes)
        FreeExecNodes(&nodes);
    pfree(command);
}

/*
 * distrib_execute_query
 * Execute single raw query on given list of nodes
 */
static void
distrib_execute_query(char *sql, bool is_temp, ExecNodes *exec_nodes)
{
    RemoteQuery *step = makeNode(RemoteQuery);
    step->combine_type = COMBINE_TYPE_SAME;
    step->exec_nodes = exec_nodes;
    step->sql_statement = pstrdup(sql);
    step->force_autocommit = false;

    /* Redistribution operations only concern Datanodes */
    step->exec_type = EXEC_ON_DATANODES;
#ifdef __TBASE__
	ExecRemoteUtility(step, NULL, NON_PARALLEL_DDL);
#else
    ExecRemoteUtility(step);
#endif
    pfree(step->sql_statement);
    pfree(step);

    /* Be sure to advance the command counter after the last command */
    CommandCounterIncrement();
}
