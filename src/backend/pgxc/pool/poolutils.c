/*-------------------------------------------------------------------------
 *
 * poolutils.c
 *
 * Utilities for Postgres-XC pooler
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * IDENTIFICATION
 *    $$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include "libpq/pqsignal.h"

#include "pgxc/pgxc.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "pgxc/poolmgr.h"
#include "pgxc/locator.h"
#include "pgxc/nodemgr.h"
#include "pgxc/poolutils.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/execRemote.h"
#include "access/gtm.h"
#include "access/xact.h"
#include "catalog/pgxc_node.h"
#include "commands/dbcommands.h"
#include "commands/prepare.h"
#include "storage/ipc.h"
#include "storage/procarray.h"
#include "storage/latch.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

/*
 * pgxc_pool_check
 *
 * Check if Pooler information in catalog is consistent
 * with information cached.
 */
Datum
pgxc_pool_check(PG_FUNCTION_ARGS)
{
    if (!superuser())
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 (errmsg("must be superuser to manage pooler"))));

    /* A Datanode has no pooler active, so do not bother about that */
    if (IS_PGXC_DATANODE)
        PG_RETURN_BOOL(true);

    /* Simply check with pooler */
    PG_RETURN_BOOL(PoolManagerCheckConnectionInfo());
}

/*
 * pgxc_pool_reload
 *
 * This function checks if a refresh should be carried out first. A refresh
 * is carried out if NODEs have only been ALTERed in the catalog. Otherwise
 * reload is performed as below.
 *
 * Reload data cached in pooler and reload node connection
 * information in all the server sessions. This aborts all
 * the existing transactions on this node and reinitializes pooler.
 * First a lock is taken on Pooler to keep consistency of node information
 * being updated. If connection information cached is already consistent
 * in pooler, reload is not executed.
 * Reload itself is made in 2 phases:
 * 1) Update database pools with new connection information based on catalog
 *    pgxc_node. Remote node pools are changed as follows:
 *      - cluster nodes dropped in new cluster configuration are deleted and all
 *      their remote connections are dropped.
 *    - cluster nodes whose port or host value is modified are dropped the same
 *      way, as connection information has changed.
 *    - cluster nodes whose port or host has not changed are kept as is, but
 *      reorganized respecting the new cluster configuration.
 *    - new cluster nodes are added.
 * 2) Reload information in all the sessions of the local node.
 *    All the sessions in server are signaled to reconnect to pooler to get
 *    newest connection information and update connection information related
 *    to remote nodes. This results in losing prepared and temporary objects
 *    in all the sessions of server. All the existing transactions are aborted
 *    and a WARNING message is sent back to client.
 *    Session that invoked the reload does the same process, but no WARNING
 *    message is sent back to client.
 */
Datum
pgxc_pool_reload(PG_FUNCTION_ARGS)
{// #lizard forgives
    if (IsTransactionBlock())
        ereport(ERROR,
                (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
                 errmsg("pgxc_pool_reload cannot run inside a transaction block")));

    /*
     * Always check if we can get away with a LESS destructive refresh
     * operation.
     */
#if 0     
    if (PgxcNodeRefresh())
        PG_RETURN_BOOL(true);
#endif
#if 0
    /* TODO: disable node refresh now, consider the handle fd state and enable refresh later */
    /* Always send reload msg to pooler */
    PgxcNodeRefresh();
#endif
    /* Session is being reloaded, drop prepared and temporary objects */
    DropAllPreparedStatements();

    /* Reinitialize session, it updates the shared memory table */
    InitMultinodeExecutor(true);

#ifdef __TBASE__
    /* Take a lock on pooler to forbid any action during reload */
    PoolManagerLock(true);
#endif

    /* Be sure it is done consistently */
    do
    {
        /* Reload connection information in pooler */
        PoolManagerReloadConnectionInfo();
    }while (!PoolManagerCheckConnectionInfo());

#ifdef __TBASE__
    PoolManagerLock(false);
#endif

    /* Signal other sessions to reconnect to pooler if have privileges */
    if (superuser())
        ReloadConnInfoOnBackends(false);

    PG_RETURN_BOOL(true);
}

bool
PgxcNodeRefresh(void)
{
    List *nodes_alter = NIL, *nodes_delete = NIL, *nodes_add = NIL;

    /*
     * Check if NODE metadata has been ALTERed only. If there are DELETIONs
     * or ADDITIONs of NODEs, then we tell the caller to use reload
     * instead
     */
    if (!PgxcNodeDiffBackendHandles(&nodes_alter, &nodes_delete, &nodes_add))
    {
        elog(LOG, "Self node altered. Performing reload"
             " to re-create connections!");
        return false;
    }

    if (nodes_delete != NIL || nodes_add != NIL)
    {
        elog(LOG, "Nodes added/deleted. Performing reload"
             " to re-create connections!");
        return false;
    }

    if (nodes_alter == NIL)
    {
        elog(LOG, "No nodes altered. Returning");
        return true;
    }

    /* Be sure it is done consistently */
    while (!PoolManagerCheckConnectionInfo())
    {
        /* Refresh connection information in pooler */
        PoolManagerRefreshConnectionInfo();
    }

    PgxcNodeRefreshBackendHandlesShmem(nodes_alter);

    /* Signal other sessions to reconnect to pooler if have privileges */
    if (superuser())
        ReloadConnInfoOnBackends(true);

    list_free(nodes_alter);
    list_free(nodes_add);
    list_free(nodes_delete);

    return false;
}

/*
 * CleanConnection()
 *
 * Utility to clean up Postgres-XC Pooler connections.
 * This utility is launched to all the Coordinators of the cluster
 *
 * Use of CLEAN CONNECTION is limited to a super user.
 * It is advised to clean connections before shutting down a Node or drop a Database.
 *
 * SQL query synopsis is as follows:
 * CLEAN CONNECTION TO
 *        (COORDINATOR num | DATANODE num | ALL {FORCE})
 *        [ FOR DATABASE dbname ]
 *        [ TO USER username ]
 *
 * Connection cleaning can be made on a chosen database called dbname
 * or/and a chosen user.
 * Cleaning is done for all the users of a given database
 * if no user name is specified.
 * Cleaning is done for all the databases for one user
 * if no database name is specified.
 *
 * It is also possible to clean connections of several Coordinators or Datanodes
 * Ex:    CLEAN CONNECTION TO DATANODE dn1,dn2,dn3 FOR DATABASE template1
 *        CLEAN CONNECTION TO COORDINATOR co2,co4,co3 FOR DATABASE template1
 *        CLEAN CONNECTION TO DATANODE dn2,dn5 TO USER postgres
 *        CLEAN CONNECTION TO COORDINATOR co6,co1 FOR DATABASE template1 TO USER postgres
 *
 * Or even to all Coordinators/Datanodes at the same time
 * Ex:    CLEAN CONNECTION TO DATANODE * FOR DATABASE template1
 *        CLEAN CONNECTION TO COORDINATOR * FOR DATABASE template1
 *        CLEAN CONNECTION TO COORDINATOR * TO USER postgres
 *        CLEAN CONNECTION TO COORDINATOR * FOR DATABASE template1 TO USER postgres
 *
 * When FORCE is used, all the transactions using pooler connections are aborted,
 * and pooler connections are cleaned up.
 * Ex:    CLEAN CONNECTION TO ALL FORCE FOR DATABASE template1;
 *        CLEAN CONNECTION TO ALL FORCE TO USER postgres;
 *        CLEAN CONNECTION TO ALL FORCE FOR DATABASE template1 TO USER postgres;
 *
 * FORCE can only be used with TO ALL, as it takes a lock on pooler to stop requests
 * asking for connections, aborts all the connections in the cluster, and cleans up
 * pool connections associated to the given user and/or database.
 */
void
CleanConnection(CleanConnStmt *stmt)
{// #lizard forgives
    ListCell   *nodelist_item;
    List       *co_list = NIL;
    List       *dn_list = NIL;
    List       *stmt_nodes = NIL;
    char       *dbname = stmt->dbname;
    char       *username = stmt->username;
    bool        is_coord = stmt->is_coord;
    bool        is_force = stmt->is_force;

    /* Database name or user name is mandatory */
    if (!dbname && !username)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("must define Database name or user name")));

    /* Check if the Database exists by getting its Oid */
    if (dbname &&
        !OidIsValid(get_database_oid(dbname, true)))
    {
        ereport(WARNING,
                (errcode(ERRCODE_UNDEFINED_DATABASE),
                 errmsg("database \"%s\" does not exist", dbname)));
        return;
    }

    /* Check if role exists */
    if (username &&
        !OidIsValid(get_role_oid(username, false)))
    {
        ereport(WARNING,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("role \"%s\" does not exist", username)));
        return;
    }

    /*
     * Permission checks
     */
    if (!pg_database_ownercheck(get_database_oid(dbname, true), GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_DATABASE,
                       dbname);

    /*
     * FORCE is activated,
     * Send a SIGTERM signal to all the processes and take a lock on Pooler
     * to avoid backends to take new connections when cleaning.
     * Only Disconnect is allowed.
     */
    if (is_force)
    {
        int loop = 0;
        int *proc_pids = NULL;
        int num_proc_pids, count;

        num_proc_pids = PoolManagerAbortTransactions(dbname, username, &proc_pids);

        /*
         * Watch the processes that received a SIGTERM.
         * At the end of the timestamp loop, processes are considered as not finished
         * and force the connection cleaning has failed
         */

        while (num_proc_pids > 0 && loop < TIMEOUT_CLEAN_LOOP)
        {
            for (count = num_proc_pids - 1; count >= 0; count--)
            {
                switch(kill(proc_pids[count],0))
                {
                    case 0: /* Termination not done yet */
                        break;

                    default:
                        /* Move tail pid in free space */
                        proc_pids[count] = proc_pids[num_proc_pids - 1];
                        num_proc_pids--;
                        break;
                }
            }
            pg_usleep(1000000);
            loop++;
        }

        if (proc_pids)
            pfree(proc_pids);

        if (loop >= TIMEOUT_CLEAN_LOOP)
            ereport(WARNING,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("All Transactions have not been aborted")));
    }

    foreach(nodelist_item, stmt->nodes)
    {
        char *node_name = strVal(lfirst(nodelist_item));
        char node_type = PGXC_NODE_NONE;
        stmt_nodes = lappend_int(stmt_nodes,
                                 PGXCNodeGetNodeIdFromName(node_name,
                                                           &node_type));
        if (node_type == PGXC_NODE_NONE)
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("PGXC Node %s: object not defined",
                                    node_name)));
    }

    /* Build lists to be sent to Pooler Manager */
    if (stmt->nodes && is_coord)
        co_list = stmt_nodes;
    else if (stmt->nodes && !is_coord)
        dn_list = stmt_nodes;
    else
    {
		co_list = GetEntireCoordNodes();
        dn_list = GetAllDataNodes();
    }

    /*
     * If force is launched, send a signal to all the processes
     * that are in transaction and take a lock.
     * Get back their process number and watch them locally here.
     * Process are checked as alive or not with pg_usleep and when all processes are down
     * go out of the control loop.
     * If at the end of the loop processes are not down send an error to client.
     * Then Make a clean with normal pool cleaner.
     * Always release the lock when calling CLEAN CONNECTION.
     */

    /* Finish by contacting Pooler Manager */
    PoolManagerCleanConnection(dn_list, co_list, dbname, username);

    /* Clean up memory */
    if (co_list)
        list_free(co_list);
    if (dn_list)
        list_free(dn_list);
}

/*
 * DropDBCleanConnection
 *
 * Clean Connection for given database before dropping it
 * FORCE is not used here
 */
void
DropDBCleanConnection(char *dbname)
{
	List	*co_list = GetEntireCoordNodes();
    List    *dn_list = GetAllDataNodes();

    /* Check permissions for this database */
    if (!pg_database_ownercheck(get_database_oid(dbname, true), GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_DATABASE,
                       dbname);

    PoolManagerCleanConnection(dn_list, co_list, dbname, NULL);

    /* Clean up memory */
    if (co_list)
        list_free(co_list);
    if (dn_list)
        list_free(dn_list);
}

/*
 * HandlePoolerReload
 *
 * This is called when PROCSIG_PGXCPOOL_RELOAD is activated.
 * Abort the current transaction if any, then reconnect to pooler.
 * and reinitialize session connection information.
 */
void
HandlePoolerReload(void)
{
    if (proc_exit_inprogress)
        return;

    /* Prevent using of cached connections to remote nodes */
    RequestInvalidateRemoteHandles();
}

/*
 * HandlePoolerRefresh
 *
 * This is called when PROCSIG_PGXCPOOL_REFRESH is activated.
 * Reconcile local backend connection info with the one in
 * shared memory
 */
void
HandlePoolerRefresh(void)
{
    if (proc_exit_inprogress)
        return;

    InterruptPending = true;

    RequestRefreshRemoteHandles();

    /* make sure the event is processed in due course */
    SetLatch(MyLatch);
}

#ifdef _MLS_
Datum
pgxc_pool_disconnect(PG_FUNCTION_ARGS)
{
    Name   database_arg = PG_GETARG_NAME(0);
    Name   username_arg = PG_GETARG_NAME(1);
    char * database;
    char * username;
    
    if (strlen(database_arg->data) == 0)
    {
        database = NULL;
    }
    else
    {
        database = database_arg->data;
    }

    if (strlen(username_arg->data) == 0)
    {
        username = NULL;
    }
    else
    {
        username = username_arg->data;
    }
    
    if (0 == PoolManagerClosePooledConnections(database, username))
    {
        PG_RETURN_BOOL(true);
    }

    PG_RETURN_BOOL(false);
}
#endif

