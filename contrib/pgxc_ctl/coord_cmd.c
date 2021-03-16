/*-------------------------------------------------------------------------
 *
 * coord_cmd.c
 *
 *    Coordinator command module of Postgres-XC configuration and operation tool.
 *
 * Copyright (c) 2013 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
 */
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <signal.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <setjmp.h>
#include <string.h>
#include <readline/readline.h>
#include <readline/history.h>

#include "pgxc_ctl.h"
#include "do_command.h"
#include "variables.h"
#include "varnames.h"
#include "pgxc_ctl_log.h"
#include "config.h"
#include "do_shell.h"
#include "utils.h"
#include "coord_cmd.h"
#include "gtm_util.h"


static int failover_oneCoordinator(int coordIdx);
static cmd_t *prepare_configureDataNode(char *nodeName);
static cmd_t *prepare_configureDataNode_multicluster(char *nodeName);


static char date[MAXTOKEN+1];

/*
 *======================================================================
 *
 * Coordinator staff
 *
 *=====================================================================
 */
/*
 * Initialize coordinator masters -----------------------------------------------------------
 */
int init_coordinator_master_all(void)
{
    elog(NOTICE, "Initialize all the coordinator masters.\n");
    return(init_coordinator_master(aval(VAR_coordNames)));
}

cmd_t *prepare_initCoordinatorMaster(char *nodeName)
{
    cmd_t *cmd, *cmdInitdb, *cmdPgConf, *cmdWalArchDir, *cmdWalArch, *cmdPgHba;
    int jj, kk;
    // int gtmPxyIdx;
    char **confFiles = NULL;
    FILE *f;
    char localStdin[MAXPATH+1];
    //char *gtmHost, *gtmPort;
    char timestamp[MAXTOKEN+1];
    char remoteDirCheck[MAXPATH * 2 + 128];

    /* Reset coordinator master directory and run initdb */
    if ((jj = coordIdx(nodeName)) < 0)
    {
        elog(ERROR, "ERROR: Node %s is not a coordinator.\n", nodeName);
        return(NULL);
    }
    if(pingNode(aval(VAR_coordMasterServers)[jj], aval(VAR_coordPorts)[jj]) == 0)
    {
        elog(ERROR, "ERROR: target coordinator master %s is running now.   Skip initilialization.\n",
             nodeName);
        return(NULL);
    }

    remoteDirCheck[0] = '\0';
    if (!forceInit)
    {
        sprintf(remoteDirCheck, "if [ '$(ls -A %s 2> /dev/null)' ]; then echo 'ERROR: "
                "target directory (%s) exists and not empty. "
                "Skip Coordinator initilialization'; exit; fi;",
                aval(VAR_coordMasterDirs)[jj],
                aval(VAR_coordMasterDirs)[jj]
               );
    }

    cmd = cmdInitdb = initCmd(aval(VAR_coordMasterServers)[jj]);
    snprintf(newCommand(cmdInitdb), MAXLINE, 
             "%s"
             "rm -rf %s;"
             "mkdir -p %s;"
             "PGXC_CTL_SILENT=1 initdb --nodename %s --nodetype coordinator -D %s "
             "--master_gtm_nodename %s --master_gtm_ip %s --master_gtm_port %s",
             remoteDirCheck,
             aval(VAR_coordMasterDirs)[jj],
             aval(VAR_coordMasterDirs)[jj],
             nodeName,
             aval(VAR_coordMasterDirs)[jj],
             sval(VAR_gtmName),
             sval(VAR_gtmMasterServer),
             sval(VAR_gtmMasterPort));

    /* Update postgresql.conf */

    /* coordSpecificExtraConfig */
    //gtmPxyIdx = getEffectiveGtmProxyIdxFromServerName(aval(VAR_coordMasterServers)[jj]);
    //gtmHost = (gtmPxyIdx >= 0) ? aval(VAR_gtmProxyServers)[gtmPxyIdx] : sval(VAR_gtmMasterServer);
    //gtmPort = (gtmPxyIdx >= 0) ? aval(VAR_gtmProxyPorts)[gtmPxyIdx] : sval(VAR_gtmMasterPort);
    appendCmdEl(cmdInitdb, (cmdPgConf = initCmd(aval(VAR_coordMasterServers)[jj])));
    snprintf(newCommand(cmdPgConf), MAXLINE,
             "cat >> %s/postgresql.conf", aval(VAR_coordMasterDirs)[jj]);
    if (doesExist(VAR_coordExtraConfig, 0) &&
        !is_none(sval(VAR_coordExtraConfig)))
        AddMember(confFiles, sval(VAR_coordExtraConfig));
    if (doesExist(VAR_coordSpecificExtraConfig, jj) &&
        !is_none(aval(VAR_coordSpecificExtraConfig)[jj]))
        AddMember(confFiles, aval(VAR_coordSpecificExtraConfig)[jj]);
    if ((f = prepareLocalStdin((cmdPgConf->localStdin = Malloc(MAXPATH+1)), MAXPATH, confFiles)) == NULL)
    {
        cleanCmd(cmd);
        Free(confFiles);
        return(NULL);
    }
    /* From configuration variables */

    fprintf(f,
            "#===========================================\n"
            "# Added at initialization. %s\n"
            "port = %d\n"
            "pooler_port = %s\n"
            "# End of Additon\n",
            timeStampString(timestamp, MAXTOKEN),
            atoi(aval(VAR_coordPorts)[jj]),
            aval(VAR_poolerPorts)[jj]
            );
    
    if(isVarYes(VAR_multiCluster) && !is_none(aval(VAR_coordMasterCluster)[jj]))
    {
        fprintf(f, 
                "pgxc_main_cluster_name = %s\n"
                "pgxc_cluster_name = %s\n",
                sval(VAR_pgxcMainClusterName),
                aval(VAR_coordMasterCluster)[jj]);
    }

    fclose(f);
    CleanArray(confFiles);

    /* Log Shipping */

    if (isVarYes(VAR_coordSlave) && !is_none(aval(VAR_coordSlaveServers)[jj]))
    {
        /* Build WAL archive target directory */
        appendCmdEl(cmdInitdb, (cmdWalArchDir = initCmd(aval(VAR_coordSlaveServers)[jj])));
        snprintf(newCommand(cmdWalArchDir), MAXLINE,
                 "rm -rf %s;mkdir -p %s; chmod 0700 %s",
                 aval(VAR_coordArchLogDirs)[jj], aval(VAR_coordArchLogDirs)[jj], 
                 aval(VAR_coordArchLogDirs)[jj]);
        /* Build master's postgresql.conf */
        appendCmdEl(cmdInitdb, (cmdWalArch = initCmd(aval(VAR_coordMasterServers)[jj])));
        if ((f = prepareLocalStdin(localStdin, MAXPATH, NULL)) == NULL)
        {
            cleanCmd(cmd);
            return(NULL);
        }
        fprintf(f, 
                "#========================================\n"
                "# Addition for log shipping, %s\n"
                "wal_level = logical \n"
                "archive_mode = off\n"
                "# archive_command = 'rsync %%p %s@%s:%s/%%f'\n"
                "max_wal_senders = %s\n"
                "# End of Addition\n",
                timeStampString(timestamp, MAXPATH),
                sval(VAR_pgxcUser), aval(VAR_coordSlaveServers)[jj], aval(VAR_coordArchLogDirs)[jj],
                is_none(aval(VAR_coordMaxWALSenders)[jj]) ? "0" : aval(VAR_coordMaxWALSenders)[jj]);
        fclose(f);
        cmdWalArch->localStdin = Strdup(localStdin);
        snprintf(newCommand(cmdWalArch), MAXLINE,
                 "cat >> %s/postgresql.conf",
                 aval(VAR_coordMasterDirs)[jj]);
    }

    /* pg_hba.conf */

    appendCmdEl(cmdInitdb, (cmdPgHba = initCmd(aval(VAR_coordMasterServers)[jj])));
    if ((f = prepareLocalStdin(localStdin, MAXPATH, NULL)) == NULL)
    {
        cleanCmd(cmd);
        Free(cmdInitdb);
        return(NULL);
    }
    fprintf(f, 
            "#=================================================\n"
            "# Addition at initialization, %s\n",
            timeStampString(timestamp, MAXTOKEN));
    if (doesExist(VAR_coordExtraPgHba, 0) && !is_none(sval(VAR_coordExtraPgHba)))
        AddMember(confFiles, sval(VAR_coordExtraPgHba));
    if (doesExist(VAR_coordSpecificExtraPgHba, jj) && !is_none(aval(VAR_coordSpecificExtraPgHba)[jj]))
        AddMember(confFiles, aval(VAR_coordSpecificExtraPgHba)[jj]);
    appendFiles(f, confFiles);
    CleanArray(confFiles);
    for (kk = 0; aval(VAR_coordPgHbaEntries)[kk]; kk++)
    {
        fprintf(f,"host all %s %s trust\n",    sval(VAR_pgxcOwner), aval(VAR_coordPgHbaEntries)[kk]);
        if (isVarYes(VAR_coordSlave))
            if (!is_none(aval(VAR_coordSlaveServers)[jj]))
                fprintf(f, "host replication %s %s trust\n",
                        sval(VAR_pgxcOwner), aval(VAR_coordPgHbaEntries)[kk]);
    }
    fprintf(f, "# End of addition\n");
    fclose(f);
    cmdPgHba->localStdin = Strdup(localStdin);
    snprintf(newCommand(cmdPgHba), MAXLINE,
             "cat >> %s/pg_hba.conf", aval(VAR_coordMasterDirs)[jj]);

    /*
     * Now prepare statements to create/alter nodes.
     */
    return(cmd);
}

int init_coordinator_master(char **nodeList)
{
    char **actualNodeList;
    int ii;
    cmdList_t *cmdList;
    cmd_t *cmd;
    int rc;

    actualNodeList = makeActualNodeList(nodeList);
    /*
     * Build directory and run initdb
     */
    cmdList = initCmdList();
    for (ii = 0; actualNodeList[ii]; ii++)
    {
        elog(NOTICE, "Initialize coordinator master %s.\n", actualNodeList[ii]);
        if ((cmd = prepare_initCoordinatorMaster(actualNodeList[ii])))
            addCmd(cmdList, cmd);
    }
    rc = doCmdList(cmdList);
    cleanCmdList(cmdList);
    CleanArray(actualNodeList);
    elog(NOTICE, "Done.\n");
    return(rc);
}

/*
 * Initialize coordinator slaves ---------------------------------------------------------------
 */
int init_coordinator_slave_all(void)
{
    elog(NOTICE, "Initialize all the coordinator slaves.\n");
    return(init_coordinator_slave(aval(VAR_coordNames)));
}

cmd_t *prepare_initCoordinatorSlave(char *nodeName)
{
    cmd_t *cmd, *cmdBuildDir, *cmdStartMaster, *cmdBaseBkup, *cmdRecoveryConf, *cmdPgConf;
    int idx;
    FILE *f;
    char localStdin[MAXPATH+1];
    char timestamp[MAXTOKEN+1];
    char remoteDirCheck[MAXPATH * 2 + 128];

    if ((idx = coordIdx(nodeName)) < 0)
    {
        elog(ERROR, "ERROR: %s is not a coordinator.\n", nodeName);
        return(NULL);
    }
    if (is_none(aval(VAR_coordSlaveServers)[idx]))
    {
        elog(ERROR, "ERROR: Slave of the coordinator %s is not configured.\n", nodeName);
        return(NULL);
    }

    remoteDirCheck[0] = '\0';
    if (!forceInit)
    {
        sprintf(remoteDirCheck, "if [ '$(ls -A %s 2> /dev/null)' ]; then echo 'ERROR: "
                "target directory (%s) exists and not empty. "
                "Skip Coordinator slave initilialization'; exit; fi;",
                aval(VAR_coordSlaveDirs)[idx],
                aval(VAR_coordSlaveDirs)[idx]
               );
    }

    /* Build work directory */
    cmd = cmdBuildDir = initCmd(aval(VAR_coordSlaveServers)[idx]);
    snprintf(newCommand(cmdBuildDir), MAXLINE,
             "%s"
             "rm -rf %s;"
             "mkdir -p %s;"
             "chmod 0700 %s",
             remoteDirCheck,
             aval(VAR_coordSlaveDirs)[idx], aval(VAR_coordSlaveDirs)[idx], aval(VAR_coordSlaveDirs)[idx]);
    /* 
     * Check if the master is running --> May not need change if we have watchdog.   This case, we need
     * a master which can handle the request.   So GTM should be running. We can test all of them by
     * single 'select 1' command.
     */
    if (pingNode(aval(VAR_coordMasterServers)[idx], aval(VAR_coordPorts)[idx]) != 0)
    {
        /* Master is not running. Must start it first */
        appendCmdEl(cmdBuildDir, (cmdStartMaster = initCmd(aval(VAR_coordMasterServers)[idx])));
        snprintf(newCommand(cmdStartMaster), MAXLINE,
                 "pg_ctl start -w -Z coordinator -D %s -o -i",
                 aval(VAR_coordMasterDirs)[idx]);
    }
    /*
     * Obtain base backup of the master
     */
    appendCmdEl(cmdBuildDir, (cmdBaseBkup = initCmd(aval(VAR_coordSlaveServers)[idx])));
    snprintf(newCommand(cmdBaseBkup), MAXLINE,
			 "pg_basebackup -U %s -p %s -h %s -D %s --wal-method=stream",
			 aval(VAR_pgxcOwner)[0],aval(VAR_coordPorts)[idx], aval(VAR_coordMasterServers)[idx], aval(VAR_coordSlaveDirs)[idx]);

    /* Configure recovery.conf file at the slave */
    appendCmdEl(cmdBuildDir, (cmdRecoveryConf = initCmd(aval(VAR_coordSlaveServers)[idx])));
    if ((f = prepareLocalStdin(localStdin, MAXPATH, NULL)) == NULL)
    {
        cleanCmd(cmd);
        return(NULL);
    }
    fprintf(f,
            "#==========================================\n"
            "# Added to initialize the slave, %s\n"
            "standby_mode = on\n"
            "primary_conninfo = 'host = %s port = %s "
            "user = %s application_name = %s'\n"
            "# restore_command = 'cp %s/%%f %%p'\n"
            "# archive_cleanup_command = 'pg_archivecleanup %s %%r'\n"
            "# End of addition\n",
            timeStampString(timestamp, MAXTOKEN), aval(VAR_coordMasterServers)[idx], aval(VAR_coordPorts)[idx],
            sval(VAR_pgxcOwner), aval(VAR_coordNames)[idx], 
            aval(VAR_coordArchLogDirs)[idx], aval(VAR_coordArchLogDirs)[idx]);
    fclose(f);
    cmdRecoveryConf->localStdin = Strdup(localStdin);
    snprintf(newCommand(cmdRecoveryConf), MAXLINE,
             "cat >> %s/recovery.conf\n", aval(VAR_coordSlaveDirs)[idx]);

    /* Configure postgresql.conf at the slave */
    appendCmdEl(cmdBuildDir, (cmdPgConf = initCmd(aval(VAR_coordSlaveServers)[idx])));
    if ((f = prepareLocalStdin(localStdin, MAXPATH, NULL)) == NULL)
    {
        cleanCmd(cmd);
        return(NULL);
    }
    
    if(isVarYes(VAR_multiCluster) && !is_none(aval(VAR_coordMasterCluster)[idx]))
    {
        
        fprintf(f,
                "#==========================================\n"
                "# Added to initialize the slave, %s\n"
                "prefer_olap = true\n"
                "olap_optimizer = true\n"
                "hot_standby = on\n"
                "port = %s\n"
                "pooler_port = %s\n"
                "wal_level = logical \n"
                "archive_mode = off\n"
                "archive_command = ''\n"
                "max_wal_senders = 0\n"
                "pgxc_main_cluster_name = %s\n"
                "pgxc_cluster_name = %s\n"
                "# End of Addition\n",
                timeStampString(timestamp, MAXTOKEN),
                aval(VAR_coordSlavePorts)[idx],
                aval(VAR_coordSlavePoolerPorts)[idx],
                sval(VAR_pgxcMainClusterName),
                aval(VAR_coordSlaveCluster)[idx]);

    }
    else
    {
        fprintf(f,
                "#==========================================\n"
                "# Added to initialize the slave, %s\n"
                "hot_standby = on\n"
                "port = %s\n"
                "pooler_port = %s\n"
                "wal_level = logical \n"
                "archive_mode = off\n"
                "archive_command = ''\n"
                "max_wal_senders = 0\n"
                "# End of Addition\n",
                timeStampString(timestamp, MAXTOKEN),
                aval(VAR_coordSlavePorts)[idx],
                aval(VAR_coordSlavePoolerPorts)[idx]);
    }
    fclose(f);
    cmdPgConf->localStdin = Strdup(localStdin);
    snprintf(newCommand(cmdPgConf), MAXLINE,
             "cat >> %s/postgresql.conf", aval(VAR_coordSlaveDirs)[idx]);
    return(cmd);
}

        
int init_coordinator_slave(char **nodeList)
{
    char **actualNodeList;
    int ii;
    cmdList_t *cmdList;
    int rc;
    cmd_t *cmd;

    if (!isVarYes(VAR_coordSlave))
    {
        elog(ERROR, "ERROR: Coordinator slaves are not configured.\n");
        return(1);
    }
    actualNodeList = makeActualNodeList(nodeList);
    cmdList = initCmdList();
    /*
     * First step: initialize work directory and run the master if necessary
     */
    for (ii = 0; actualNodeList[ii]; ii++)
    {
        elog(INFO, "Initialize the coordinator slave %s.\n", actualNodeList[ii]);
        if ((cmd = prepare_initCoordinatorSlave(actualNodeList[ii])))
            addCmd(cmdList, cmd);
    }
    rc = doCmdList(cmdList);
    cleanCmdList(cmdList);
    CleanArray(actualNodeList);
    elog(INFO, "Done.\n");
    return(rc);
}

/*
 * Configure nodes in each coordinator -------------------------------------------
 *
 * Issues CREATE NODE/ALTER NODE through psql.
 *
 * Please note that CREATE/ALTER/DROP NODE are handled only locally.  You have to
 * visit all the coordinators.
 */
int configure_nodes_all(void)
{
    configure_nodes(aval(VAR_coordNames));
    return configure_datanodes(aval(VAR_datanodeNames));
}

int configure_nodes(char **nodeList)
{
    char **actualNodeList;
    int ii;
    cmdList_t *cmdList;
    cmd_t *cmd;
    int rc;

    actualNodeList = makeActualNodeList(nodeList);
    cmdList = initCmdList();
    for (ii = 0; actualNodeList[ii]; ii++)
    {
        if(isVarYes(VAR_multiCluster))
        {
            if ((cmd = prepare_configureNode_multicluster(actualNodeList[ii])))
                addCmd(cmdList, cmd);
        }
        else
        {
            if ((cmd = prepare_configureNode(actualNodeList[ii])))
                addCmd(cmdList, cmd);
        }
    }
    rc = doCmdList(cmdList);
    cleanCmdList(cmdList);
    CleanArray(actualNodeList);
    elog(INFO, "Done.\n");
    return(rc);
}

int configure_datanodes(char **nodeList)
{
    char **actualNodeList;
    int ii;
    cmdList_t *cmdList;
    cmd_t *cmd;
    int rc;

    actualNodeList = makeActualNodeList(nodeList);
    cmdList = initCmdList();
    for (ii = 0; actualNodeList[ii]; ii++)
    {
        if(isVarYes(VAR_multiCluster))
        {
            if ((cmd = prepare_configureDataNode_multicluster(actualNodeList[ii])))
                addCmd(cmdList, cmd);
        }
        else
        {
            if ((cmd = prepare_configureDataNode(actualNodeList[ii])))
                addCmd(cmdList, cmd);
        }
    }
    rc = doCmdList(cmdList);
    cleanCmdList(cmdList);
    CleanArray(actualNodeList);
    elog(INFO, "Done.\n");
    return(rc);
}

cmd_t *prepare_configureNode(char *nodeName)
{
    cmd_t *cmd;
    int ii;
    int idx;
    FILE *f;

    if ((idx = coordIdx(nodeName)) < 0)
    {
        elog(ERROR, "ERROR: %s is not a coordinator.\n", nodeName);
        return NULL;
    }
    if (is_none(aval(VAR_coordMasterServers)[idx]))
        return NULL;
    cmd = initCmd(NULL);
    snprintf(newCommand(cmd), MAXLINE,
             "psql -p %d -h %s -a %s %s",
             atoi(aval(VAR_coordPorts)[idx]),
             aval(VAR_coordMasterServers)[idx],
             sval(VAR_defaultDatabase),
             sval(VAR_pgxcOwner));
    if ((f = prepareLocalStdin(newFilename(cmd->localStdin), MAXPATH, NULL)) == NULL)
    {
        cleanCmd(cmd);
        Free(cmd);
        return NULL;
    }
    /* Setup coordinators */
    for (ii = 0; aval(VAR_coordNames)[ii]; ii++)
    {
        int targetIdx;
        if (is_none(aval(VAR_coordNames)[ii]))
            continue;
        if ((targetIdx = coordIdx(aval(VAR_coordNames)[ii])) < 0)
            continue;

        if (!is_none(aval(VAR_coordMasterServers)[ii]))
        {
            if (idx != targetIdx)
                /* Register outside coordinator */
                fprintf(f, "CREATE NODE %s WITH (TYPE='coordinator', HOST='%s', PORT=%d);\n",
                        aval(VAR_coordNames)[ii], 
                        aval(VAR_coordMasterServers)[ii],
                        atoi(aval(VAR_coordPorts)[ii]));
            else
                /* Update myself */
                fprintf(f, "ALTER NODE %s WITH (HOST='%s', PORT=%d);\n",
                        aval(VAR_coordNames)[ii],
                        aval(VAR_coordMasterServers)[ii],
                        atoi(aval(VAR_coordPorts)[ii]));
        }
    }
    /* Setup datanodes */
    for (ii = 0; aval(VAR_datanodeNames)[ii]; ii++)
    {
        int dnIdx;

        if (is_none(aval(VAR_datanodeNames)[ii]))
            continue;

        if ((dnIdx = datanodeIdx(aval(VAR_datanodeNames)[ii])) < 0)
        {
            fclose(f);
            cleanCmd(cmd);
            return NULL;
        }

        if (is_none(aval(VAR_datanodeMasterServers)[dnIdx]))
            continue;

        if (sval(VAR_primaryDatanode) && (strcmp(sval(VAR_primaryDatanode), aval(VAR_datanodeNames)[dnIdx]) == 0))
        {
            /* Primary Node */
            if (strcmp(aval(VAR_coordMasterServers)[idx], aval(VAR_datanodeMasterServers)[dnIdx]) == 0)
                /* Primay and preferred node */
                fprintf(f, "CREATE NODE %s WITH (TYPE='datanode', HOST='%s', PORT=%d, PRIMARY, PREFERRED);\n",
                        aval(VAR_datanodeNames)[dnIdx], aval(VAR_datanodeMasterServers)[dnIdx],
                        atoi(aval(VAR_datanodePorts)[dnIdx]));
            else
                /* Primary but not prefereed node */
                fprintf(f, "CREATE NODE %s WITH (TYPE='datanode', HOST='%s', PORT=%d, PRIMARY);\n",
                        aval(VAR_datanodeNames)[dnIdx], aval(VAR_datanodeMasterServers)[dnIdx],
                        atoi(aval(VAR_datanodePorts)[dnIdx]));
        }
        else
        {
            /* Non-primary node */
            if (strcmp(aval(VAR_coordMasterServers)[idx], aval(VAR_datanodeMasterServers)[dnIdx]) == 0)
                /* Preferred node */
                fprintf(f, "CREATE NODE %s WITH (TYPE='datanode', HOST='%s', PORT=%d, PREFERRED);\n",
                        aval(VAR_datanodeNames)[dnIdx], aval(VAR_datanodeMasterServers)[dnIdx],
                        atoi(aval(VAR_datanodePorts)[dnIdx]));
            else
                /* non-Preferred node */
                fprintf(f, "CREATE NODE %s WITH (TYPE='datanode', HOST='%s', PORT=%d);\n",
                        aval(VAR_datanodeNames)[dnIdx], aval(VAR_datanodeMasterServers)[dnIdx],
                        atoi(aval(VAR_datanodePorts)[dnIdx]));
        }
    }
    fprintf(f, "SELECT pgxc_pool_reload();\n");
    fclose(f);
    return(cmd);
}    

static cmd_t *prepare_configureDataNode(char *nodeName)
{
    cmd_t *cmd;
    int ii;
    int jj;
    int idx;
    int connCordIndx;
    FILE *f;
    bool is_preferred;

    if ((idx = datanodeIdx(nodeName)) < 0)
    {
        elog(ERROR, "ERROR: %s is not a datanode.\n", nodeName);
        return NULL;
    }
    if (is_none(aval(VAR_datanodeMasterServers)[idx]))
        return NULL;
    cmd = initCmd(NULL);

    /* We use one of the coordinators to send queries to datanodes */
    connCordIndx = get_any_available_coord(-1);
    if (connCordIndx == -1)
        return NULL;

    snprintf(newCommand(cmd), MAXLINE,
             "psql -p %d -h %s -a %s %s",
             atoi(aval(VAR_coordPorts)[connCordIndx]),
             aval(VAR_coordMasterServers)[connCordIndx],
             sval(VAR_defaultDatabase),
             sval(VAR_pgxcOwner));
    if ((f = prepareLocalStdin(newFilename(cmd->localStdin), MAXPATH, NULL)) == NULL)
    {
        cleanCmd(cmd);
        Free(cmd);
        return NULL;
    }
    /* Setup coordinators */
    for (ii = 0; aval(VAR_coordNames)[ii]; ii++)
    {
        int targetIdx;
        if (is_none(aval(VAR_coordNames)[ii]))
            continue;
        if ((targetIdx = coordIdx(aval(VAR_coordNames)[ii])) < 0)
            continue;
        if (!is_none(aval(VAR_coordMasterServers)[ii]))
        {
            /* Register outside coordinator */
            fprintf(f, "EXECUTE DIRECT ON (%s) 'CREATE NODE %s WITH (TYPE=''coordinator'', HOST=''%s'', PORT=%d)';\n",
                    aval(VAR_datanodeNames)[idx],
                    aval(VAR_coordNames)[ii],
                    aval(VAR_coordMasterServers)[ii],
                    atoi(aval(VAR_coordPorts)[ii]));
        }
    }

    /* Setup datanodes */
    for (ii = 0; aval(VAR_datanodeNames)[ii]; ii++)
    {
        int dnIdx;

        if (is_none(aval(VAR_datanodeNames)[ii]))
            continue;

        if ((dnIdx = datanodeIdx(aval(VAR_datanodeNames)[ii])) < 0)
        {
            fclose(f);
            cleanCmd(cmd);
            return NULL;
        }
        
        if (is_none(aval(VAR_datanodeMasterServers)[dnIdx]))
            continue;

        // See if this data node is on the same host as a coordinator
        is_preferred = false;
        for (jj = 0; aval(VAR_coordNames)[jj]; jj++)
        {
            if (strcmp(aval(VAR_coordMasterServers)[jj], aval(VAR_datanodeMasterServers)[dnIdx]) == 0)
            {
                is_preferred = true;
                break;
            }
        }

        if (sval(VAR_primaryDatanode) && (strcmp(sval(VAR_primaryDatanode), aval(VAR_datanodeNames)[dnIdx]) == 0))
        {
            if (idx != dnIdx)
            {
                /* Primary Node */
                if (is_preferred)
                {
                    /* Primay and preferred node */
                    fprintf(f, "EXECUTE DIRECT ON (%s) 'CREATE NODE %s WITH (TYPE=''datanode'', HOST=''%s'', PORT=%d, PRIMARY, PREFERRED)';\n",
                            aval(VAR_datanodeNames)[idx],
                            aval(VAR_datanodeNames)[dnIdx], aval(VAR_datanodeMasterServers)[dnIdx],
                            atoi(aval(VAR_datanodePorts)[dnIdx]));
                }
                else
                    /* Primary but not prefereed node */
                    fprintf(f, "EXECUTE DIRECT ON (%s) 'CREATE NODE %s WITH (TYPE=''datanode'', HOST=''%s'', PORT=%d, PRIMARY)';\n",
                            aval(VAR_datanodeNames)[idx],
                            aval(VAR_datanodeNames)[dnIdx], aval(VAR_datanodeMasterServers)[dnIdx],
                            atoi(aval(VAR_datanodePorts)[dnIdx]));
            }
            else
            {
                /* Primary Node */
                if (is_preferred)
                    /* Primay and preferred node */
                    fprintf(f, "EXECUTE DIRECT ON (%s) 'ALTER NODE %s WITH (TYPE=''datanode'', HOST=''%s'', PORT=%d, PRIMARY, PREFERRED)';\n",
                            aval(VAR_datanodeNames)[idx],
                            aval(VAR_datanodeNames)[dnIdx], aval(VAR_datanodeMasterServers)[dnIdx],
                            atoi(aval(VAR_datanodePorts)[dnIdx]));
                else
                    /* Primary but not prefereed node */
                    fprintf(f, "EXECUTE DIRECT ON (%s) 'ALTER NODE %s WITH (TYPE=''datanode'', HOST=''%s'', PORT=%d, PRIMARY)';\n",
                            aval(VAR_datanodeNames)[idx],
                            aval(VAR_datanodeNames)[dnIdx], aval(VAR_datanodeMasterServers)[dnIdx],
                            atoi(aval(VAR_datanodePorts)[dnIdx]));
            }
        }
        else
        {
            if (idx != dnIdx)
            {
                /* Non-primary node */
                if (is_preferred)
                    /* Preferred node */
                    fprintf(f, "EXECUTE DIRECT ON (%s) 'CREATE NODE %s WITH (TYPE=''datanode'', HOST=''%s'', PORT=%d, PREFERRED)';\n",
                            aval(VAR_datanodeNames)[idx],
                            aval(VAR_datanodeNames)[dnIdx], aval(VAR_datanodeMasterServers)[dnIdx],
                            atoi(aval(VAR_datanodePorts)[dnIdx]));
                else
                    /* non-Preferred node */
                    fprintf(f, "EXECUTE DIRECT ON (%s) 'CREATE NODE %s WITH (TYPE=''datanode'', HOST=''%s'', PORT=%d)';\n",
                            aval(VAR_datanodeNames)[idx],
                            aval(VAR_datanodeNames)[dnIdx], aval(VAR_datanodeMasterServers)[dnIdx],
                            atoi(aval(VAR_datanodePorts)[dnIdx]));
            }
            else
            {
                /* Non-primary node */
                if (is_preferred)
                    /* Preferred node */
                    fprintf(f, "EXECUTE DIRECT ON (%s) 'ALTER NODE %s WITH (TYPE=''datanode'', HOST=''%s'', PORT=%d, PREFERRED)';\n",
                            aval(VAR_datanodeNames)[idx],
                            aval(VAR_datanodeNames)[dnIdx], aval(VAR_datanodeMasterServers)[dnIdx],
                            atoi(aval(VAR_datanodePorts)[dnIdx]));
                else
                    /* non-Preferred node */
                    fprintf(f, "EXECUTE DIRECT ON (%s) 'ALTER NODE %s WITH (TYPE=''datanode'', HOST=''%s'', PORT=%d)';\n",
                            aval(VAR_datanodeNames)[idx],
                            aval(VAR_datanodeNames)[dnIdx], aval(VAR_datanodeMasterServers)[dnIdx],
                            atoi(aval(VAR_datanodePorts)[dnIdx]));
            }
        }
    }
    fprintf(f, "EXECUTE DIRECT ON (%s) 'SELECT pgxc_pool_reload()';\n", aval(VAR_datanodeNames)[idx]);
    fclose(f);
    return(cmd);
}



cmd_t *prepare_configureNode_multicluster(char *nodeName)
{
    cmd_t *cmd;
    int ii;
    int idx;
    FILE *f;

    if ((idx = coordIdx(nodeName)) < 0)
    {
        elog(ERROR, "ERROR: %s is not a coordinator.\n", nodeName);
        return NULL;
    }
    if (is_none(aval(VAR_coordMasterServers)[idx]))
        return NULL;
    cmd = initCmd(NULL);
    snprintf(newCommand(cmd), MAXLINE,
             "psql -p %d -h %s -a %s %s",
             atoi(aval(VAR_coordPorts)[idx]),
             aval(VAR_coordMasterServers)[idx],
             sval(VAR_defaultDatabase),
             sval(VAR_pgxcOwner));
    if ((f = prepareLocalStdin(newFilename(cmd->localStdin), MAXPATH, NULL)) == NULL)
    {
        cleanCmd(cmd);
        Free(cmd);
        return NULL;
    }
    /* Setup coordinators */
    for (ii = 0; aval(VAR_coordNames)[ii]; ii++)
    {
        int targetIdx;
        if (is_none(aval(VAR_coordNames)[ii]))
            continue;
        if ((targetIdx = coordIdx(aval(VAR_coordNames)[ii])) < 0)
            continue;

        if (!is_none(aval(VAR_coordMasterServers)[ii]))
        {
            if (idx != targetIdx)
                /* Register outside coordinator */
                fprintf(f, "CREATE NODE %s WITH (TYPE='coordinator', HOST='%s', PORT=%d, CLUSTER='%s');\n",
                        aval(VAR_coordNames)[ii], 
                        aval(VAR_coordMasterServers)[ii],
                        atoi(aval(VAR_coordPorts)[ii]),
                        aval(VAR_coordMasterCluster)[ii]);
            else
                /* Update myself */
                fprintf(f, "ALTER NODE %s WITH (HOST='%s', PORT=%d, CLUSTER='%s');\n",
                        aval(VAR_coordNames)[ii],
                        aval(VAR_coordMasterServers)[ii],
                        atoi(aval(VAR_coordPorts)[ii]),
                        aval(VAR_coordMasterCluster)[ii]);
        }
        if (!is_none(aval(VAR_coordSlaveServers)[ii]))
        {
            fprintf(f, "CREATE NODE %s WITH (TYPE='coordinator', HOST='%s', PORT=%d, CLUSTER='%s');\n",
                        aval(VAR_coordNames)[ii], 
                        aval(VAR_coordSlaveServers)[ii],
                        atoi(aval(VAR_coordSlavePorts)[ii]),
                        aval(VAR_coordSlaveCluster)[ii]);
    
        }
    }
    /* Setup datanodes */
    for (ii = 0; aval(VAR_datanodeNames)[ii]; ii++)
    {
        int dnIdx;

        if (is_none(aval(VAR_datanodeNames)[ii]))
            continue;

        if ((dnIdx = datanodeIdx(aval(VAR_datanodeNames)[ii])) < 0)
        {
            fclose(f);
            cleanCmd(cmd);
            return NULL;
        }

        if (is_none(aval(VAR_datanodeMasterServers)[dnIdx]))
            continue;

        if (sval(VAR_primaryDatanode) && (strcmp(sval(VAR_primaryDatanode), aval(VAR_datanodeNames)[dnIdx]) == 0))
        {
            /* Primary Node */
            if (strcmp(aval(VAR_coordMasterServers)[idx], aval(VAR_datanodeMasterServers)[dnIdx]) == 0)
                /* Primay and preferred node */
                fprintf(f, "CREATE NODE %s WITH (TYPE='datanode', HOST='%s', PORT=%d, CLUSTER='%s', PRIMARY, PREFERRED);\n",
                        aval(VAR_datanodeNames)[dnIdx], aval(VAR_datanodeMasterServers)[dnIdx],
                        atoi(aval(VAR_datanodePorts)[dnIdx]),
                        aval(VAR_datanodeMasterCluster)[dnIdx]);
            else
                /* Primary but not prefereed node */
                fprintf(f, "CREATE NODE %s WITH (TYPE='datanode', HOST='%s', PORT=%d, CLUSTER='%s', PRIMARY);\n",
                        aval(VAR_datanodeNames)[dnIdx], aval(VAR_datanodeMasterServers)[dnIdx],
                        atoi(aval(VAR_datanodePorts)[dnIdx]),
                        aval(VAR_datanodeMasterCluster)[dnIdx]);
        }
        else
        {
            /* Non-primary node */
            if (strcmp(aval(VAR_coordMasterServers)[idx], aval(VAR_datanodeMasterServers)[dnIdx]) == 0)
                /* Preferred node */
                fprintf(f, "CREATE NODE %s WITH (TYPE='datanode', HOST='%s', PORT=%d, CLUSTER='%s', PREFERRED);\n",
                        aval(VAR_datanodeNames)[dnIdx], aval(VAR_datanodeMasterServers)[dnIdx],
                        atoi(aval(VAR_datanodePorts)[dnIdx]),
                        aval(VAR_datanodeMasterCluster)[dnIdx]);
            else
                /* non-Preferred node */
                fprintf(f, "CREATE NODE %s WITH (TYPE='datanode', HOST='%s', PORT=%d, CLUSTER='%s');\n",
                        aval(VAR_datanodeNames)[dnIdx], aval(VAR_datanodeMasterServers)[dnIdx],
                        atoi(aval(VAR_datanodePorts)[dnIdx]),
                        aval(VAR_datanodeMasterCluster)[dnIdx]);
        }

        if (isVarYes(VAR_datanodeSlave))
        {            
            fprintf(f, "CREATE NODE %s WITH (TYPE='datanode', HOST='%s', PORT=%d, CLUSTER='%s');\n",
                aval(VAR_datanodeNames)[dnIdx], aval(VAR_datanodeSlaveServers)[dnIdx],
                atoi(aval(VAR_datanodeSlavePorts)[dnIdx]),
                aval(VAR_datanodeSlaveCluster)[dnIdx]);

        }
        
    }

    fprintf(f, "SELECT pgxc_pool_reload();\n");
    fclose(f);
    return(cmd);
}    

static cmd_t *prepare_configureDataNode_multicluster(char *nodeName)
{
    cmd_t *cmd;
    int ii;
    int jj;
    int idx;
    int connCordIndx;
    FILE *f;
    bool is_preferred;

    if ((idx = datanodeIdx(nodeName)) < 0)
    {
        elog(ERROR, "ERROR: %s is not a datanode.\n", nodeName);
        return NULL;
    }
    if (is_none(aval(VAR_datanodeMasterServers)[idx]))
        return NULL;
    cmd = initCmd(NULL);

    /* We use one of the coordinators to send queries to datanodes */
    connCordIndx = get_any_available_coord(-1);
    if (connCordIndx == -1)
        return NULL;

    snprintf(newCommand(cmd), MAXLINE,
             "psql -p %d -h %s -a %s %s",
             atoi(aval(VAR_coordPorts)[connCordIndx]),
             aval(VAR_coordMasterServers)[connCordIndx],
             sval(VAR_defaultDatabase),
             sval(VAR_pgxcOwner));
    if ((f = prepareLocalStdin(newFilename(cmd->localStdin), MAXPATH, NULL)) == NULL)
    {
        cleanCmd(cmd);
        Free(cmd);
        return NULL;
    }
    /* Setup coordinators */
    for (ii = 0; aval(VAR_coordNames)[ii]; ii++)
    {
        int targetIdx;
        if (is_none(aval(VAR_coordNames)[ii]))
            continue;
        if ((targetIdx = coordIdx(aval(VAR_coordNames)[ii])) < 0)
            continue;
        if (!is_none(aval(VAR_coordMasterServers)[ii]))
        {
            /* Register outside coordinator */
            fprintf(f, "EXECUTE DIRECT ON (%s) 'CREATE NODE %s WITH (TYPE=''coordinator'', HOST=''%s'', PORT=%d, CLUSTER=''%s'')';\n",
                    aval(VAR_datanodeNames)[idx],
                    aval(VAR_coordNames)[ii],
                    aval(VAR_coordMasterServers)[ii],
                    atoi(aval(VAR_coordPorts)[ii]),
                    aval(VAR_coordMasterCluster)[ii]);
        }
        if (!is_none(aval(VAR_coordSlaveServers)[ii]))
        {
            fprintf(f, "EXECUTE DIRECT ON (%s) 'CREATE NODE %s WITH (TYPE=''coordinator'', HOST=''%s'', PORT=%d, CLUSTER=''%s'')';\n",
                aval(VAR_datanodeNames)[idx],
                aval(VAR_coordNames)[ii],
                aval(VAR_coordSlaveServers)[ii],
                atoi(aval(VAR_coordSlavePorts)[ii]),
                aval(VAR_coordSlaveCluster)[ii]);
    
        }
    }

    /* Setup datanodes */
    for (ii = 0; aval(VAR_datanodeNames)[ii]; ii++)
    {
        int dnIdx;

        if (is_none(aval(VAR_datanodeNames)[ii]))
            continue;

        if ((dnIdx = datanodeIdx(aval(VAR_datanodeNames)[ii])) < 0)
        {
            fclose(f);
            cleanCmd(cmd);
            return NULL;
        }
        
        if (is_none(aval(VAR_datanodeMasterServers)[dnIdx]))
            continue;

        // See if this data node is on the same host as a coordinator
        is_preferred = false;
        for (jj = 0; aval(VAR_coordNames)[jj]; jj++)
        {
            if (strcmp(aval(VAR_coordMasterServers)[jj], aval(VAR_datanodeMasterServers)[dnIdx]) == 0)
            {
                is_preferred = true;
                break;
            }
        }

        if (sval(VAR_primaryDatanode) && (strcmp(sval(VAR_primaryDatanode), aval(VAR_datanodeNames)[dnIdx]) == 0))
        {
            if (idx != dnIdx)
            {
                /* Primary Node */
                if (is_preferred)
                {
                    /* Primay and preferred node */
                    fprintf(f, "EXECUTE DIRECT ON (%s) 'CREATE NODE %s WITH (TYPE=''datanode'', HOST=''%s'', PORT=%d, CLUSTER=''%s'', PRIMARY, PREFERRED)';\n",
                            aval(VAR_datanodeNames)[idx],
                            aval(VAR_datanodeNames)[dnIdx], aval(VAR_datanodeMasterServers)[dnIdx],
                            atoi(aval(VAR_datanodePorts)[dnIdx]),
                            aval(VAR_datanodeMasterCluster)[dnIdx]);
                }
                else
                    /* Primary but not prefereed node */
                    fprintf(f, "EXECUTE DIRECT ON (%s) 'CREATE NODE %s WITH (TYPE=''datanode'', HOST=''%s'', PORT=%d, CLUSTER=''%s'', PRIMARY)';\n",
                            aval(VAR_datanodeNames)[idx],
                            aval(VAR_datanodeNames)[dnIdx], aval(VAR_datanodeMasterServers)[dnIdx],
                            atoi(aval(VAR_datanodePorts)[dnIdx]),
                            aval(VAR_datanodeMasterCluster)[dnIdx]);
            }
            else
            {
                /* Primary Node */
                if (is_preferred)
                    /* Primay and preferred node */
                    fprintf(f, "EXECUTE DIRECT ON (%s) 'ALTER NODE %s WITH (TYPE=''datanode'', HOST=''%s'', PORT=%d, CLUSTER=''%s'', PRIMARY, PREFERRED)';\n",
                            aval(VAR_datanodeNames)[idx],
                            aval(VAR_datanodeNames)[dnIdx], aval(VAR_datanodeMasterServers)[dnIdx],
                            atoi(aval(VAR_datanodePorts)[dnIdx]),
                            aval(VAR_datanodeMasterCluster)[dnIdx]);
                else
                    /* Primary but not prefereed node */
                    fprintf(f, "EXECUTE DIRECT ON (%s) 'ALTER NODE %s WITH (TYPE=''datanode'', HOST=''%s'', PORT=%d, CLUSTER=''%s'', PRIMARY)';\n",
                            aval(VAR_datanodeNames)[idx],
                            aval(VAR_datanodeNames)[dnIdx], aval(VAR_datanodeMasterServers)[dnIdx],
                            atoi(aval(VAR_datanodePorts)[dnIdx]),
                            aval(VAR_datanodeMasterCluster)[dnIdx]);
            }
        }
        else
        {
            if (idx != dnIdx)
            {
                /* Non-primary node */
                if (is_preferred)
                    /* Preferred node */
                    fprintf(f, "EXECUTE DIRECT ON (%s) 'CREATE NODE %s WITH (TYPE=''datanode'', HOST=''%s'', PORT=%d, CLUSTER=''%s'',PREFERRED)';\n",
                            aval(VAR_datanodeNames)[idx],
                            aval(VAR_datanodeNames)[dnIdx], aval(VAR_datanodeMasterServers)[dnIdx],
                            atoi(aval(VAR_datanodePorts)[dnIdx]),
                            aval(VAR_datanodeMasterCluster)[dnIdx]);
                else
                    /* non-Preferred node */
                    fprintf(f, "EXECUTE DIRECT ON (%s) 'CREATE NODE %s WITH (TYPE=''datanode'', HOST=''%s'', PORT=%d, CLUSTER=''%s'')';\n",
                            aval(VAR_datanodeNames)[idx],
                            aval(VAR_datanodeNames)[dnIdx], aval(VAR_datanodeMasterServers)[dnIdx],
                            atoi(aval(VAR_datanodePorts)[dnIdx]),
                            aval(VAR_datanodeMasterCluster)[dnIdx]);
            }
            else
            {
                /* Non-primary node */
                if (is_preferred)
                    /* Preferred node */
                    fprintf(f, "EXECUTE DIRECT ON (%s) 'ALTER NODE %s WITH (TYPE=''datanode'', HOST=''%s'', PORT=%d, CLUSTER=''%s'', PREFERRED)';\n",
                            aval(VAR_datanodeNames)[idx],
                            aval(VAR_datanodeNames)[dnIdx], aval(VAR_datanodeMasterServers)[dnIdx],
                            atoi(aval(VAR_datanodePorts)[dnIdx]),
                            aval(VAR_datanodeMasterCluster)[dnIdx]);
                else
                    /* non-Preferred node */
                    fprintf(f, "EXECUTE DIRECT ON (%s) 'ALTER NODE %s WITH (TYPE=''datanode'', HOST=''%s'', PORT=%d, CLUSTER=''%s'')';\n",
                            aval(VAR_datanodeNames)[idx],
                            aval(VAR_datanodeNames)[dnIdx], aval(VAR_datanodeMasterServers)[dnIdx],
                            atoi(aval(VAR_datanodePorts)[dnIdx]),
                            aval(VAR_datanodeMasterCluster)[dnIdx]);
            }
        }

        if (isVarYes(VAR_datanodeSlave))
        {
            
            fprintf(f, "EXECUTE DIRECT ON (%s) 'CREATE NODE %s WITH (TYPE=''datanode'', HOST=''%s'', PORT=%d, CLUSTER=''%s'')';\n",
                aval(VAR_datanodeNames)[idx],
                aval(VAR_datanodeNames)[dnIdx], aval(VAR_datanodeSlaveServers)[dnIdx],
                atoi(aval(VAR_datanodeSlavePorts)[dnIdx]),
                aval(VAR_datanodeSlaveCluster)[dnIdx]);

        }
    }
    fprintf(f, "EXECUTE DIRECT ON (%s) 'SELECT pgxc_pool_reload()';\n", aval(VAR_datanodeNames)[idx]);
    fclose(f);
    return(cmd);
}


/*
 * Kill coordinator masters -------------------------------------------------------------
 *
 * It is not recommended to kill them in such a manner.   This is just for emergence.
 * You should try to stop component by "stop" command.
 */

int kill_coordinator_master_all(void)
{
    elog(INFO, "Killing all the coordinator masters.\n");
    return(kill_coordinator_master(aval(VAR_coordNames)));
}

cmd_t * prepare_killCoordinatorMaster(char *nodeName)
{
    int idx;
    pid_t pmPid;
    cmd_t *cmdKill = NULL, *cmd = NULL;

    if ((idx = coordIdx(nodeName)) < 0)
    {
        elog(WARNING, "WARNING: node %s is not a coordinator.\n", nodeName);
        return(NULL);
    }
    cmd = cmdKill = initCmd(aval(VAR_coordMasterServers)[idx]);
    if ((pmPid = get_postmaster_pid(aval(VAR_coordMasterServers)[idx], aval(VAR_coordMasterDirs)[idx])) > 0)
    {
        char *pidList = getChPidList(aval(VAR_coordMasterServers)[idx], pmPid);

        snprintf(newCommand(cmdKill), MAXLINE,
                 "kill -9 %d %s; rm -f /tmp/.s.'*'%d'*'",
                 pmPid, pidList, atoi(aval(VAR_coordPorts)[idx]));
        freeAndReset(pidList);
    }
    else
    {
        elog(WARNING, "WARNING: pid for coordinator master \"%s\" was not found.  Remove socket only.\n", nodeName);
        snprintf(newCommand(cmdKill), MAXLINE,
                 "rm -f /tmp/.s.'*'%d'*'",
                 atoi(aval(VAR_coordPorts)[idx]));
    }
    return cmd;
}

int kill_coordinator_master(char **nodeList)
{
    char **actualNodeList;
    int ii;
    cmdList_t *cmdList;
    cmd_t *cmd;
    int rc;

    actualNodeList = makeActualNodeList(nodeList);
    cmdList = initCmdList();
    for (ii = 0; actualNodeList[ii]; ii++)
    {
        elog(INFO, "Killing coordinator master %s.\n", actualNodeList[ii]);
        if ((cmd = prepare_killCoordinatorMaster(actualNodeList[ii])))
            addCmd(cmdList, cmd);
    }
    rc = doCmdList(cmdList);
    cleanCmdList(cmdList);
    CleanArray(actualNodeList);
    return(rc);
}

/*
 * Kill coordinator masters -------------------------------------------------------------
 *
 * It is not recommended to kill them in such a manner.   This is just for emergence.
 * You should try to stop component by "stop" command.
 */
int kill_coordinator_slave_all(void)
{
    elog(INFO, "Killing all the coordinator slaves.\n");
    return(kill_coordinator_slave(aval(VAR_coordNames)));
}

cmd_t *prepare_killCoordinatorSlave(char *nodeName)
{
    int idx;
    pid_t pmPid;
    cmd_t *cmd = NULL;

    if ((idx = coordIdx(nodeName)) < 0)
    {
        elog(WARNING, "WARNING: %s is not a coordinator.\n", nodeName);
        return(NULL);
    }
    cmd = initCmd(aval(VAR_coordSlaveServers)[idx]);
    if ((pmPid = get_postmaster_pid(aval(VAR_coordSlaveServers)[idx], aval(VAR_coordSlaveDirs)[idx])) > 0)
    {
        char *pidList = getChPidList(aval(VAR_coordSlaveServers)[idx], pmPid);

        snprintf(newCommand(cmd), MAXLINE,
                 "kill -9 %d %s", pmPid, pidList);
        freeAndReset(pidList);
    }
    else
    {
        elog(WARNING, "WARNING: pid for coordinator slave \"%s\" was not found.  Remove socket only.\n", nodeName);
        snprintf(newCommand(cmd), MAXLINE,
             "rm -f /tmp/.s.'*'%d'*'", atoi(aval(VAR_coordSlavePorts)[idx]));
    }
    return(cmd);
}

int kill_coordinator_slave(char **nodeList)
{
    char **actualNodeList;
    int ii;
    cmdList_t *cmdList;
    cmd_t *cmd;
    int rc;

    if (!isVarYes(VAR_coordSlave))
    {
        elog(ERROR, "ERROR: Coordinator slaves are not configured.\n");
        return(1);
    }
    cmdList = initCmdList();
    actualNodeList = makeActualNodeList(nodeList);
    for (ii = 0; actualNodeList[ii]; ii++)
    {
        elog(INFO, "Killing coordinator slave %s.\n", actualNodeList[ii]);
        if ((cmd = prepare_killCoordinatorSlave(actualNodeList[ii])))
            addCmd(cmdList, cmd);
    }
    rc = doCmdList(cmdList);
    cleanCmdList(cmdList);
    CleanArray(actualNodeList);
    elog(INFO, "Done.\n");
    return (rc);
}

cmd_t *prepare_cleanCoordinatorMaster(char *nodeName)
{
    cmd_t *cmd;
    int idx;

    if ((idx = coordIdx(nodeName)) < 0)
        return NULL;
    if (is_none(aval(VAR_coordMasterServers)[idx]))
        return NULL;
    cmd = initCmd(aval(VAR_coordMasterServers)[idx]);
    snprintf(newCommand(cmd), MAXLINE,
             "rm -rf %s;mkdir -p %s;chmod 0700 %s; rm -f /tmp/.s.*%d*; rm -f /tmp/.s.*%d*",
             aval(VAR_coordMasterDirs)[idx], aval(VAR_coordMasterDirs)[idx], aval(VAR_coordMasterDirs)[idx],
             atoi(aval(VAR_coordPorts)[idx]), atoi(aval(VAR_poolerPorts)[idx]));
    return cmd;
}

/*
 * Cleanup coordinator master resources -- directory and socket.
 */
int clean_coordinator_master(char **nodeList)
{
    char **actualNodeList;
    cmdList_t *cmdList;
    int ii;
    cmd_t *cmd;
    int rc;

    actualNodeList = makeActualNodeList(nodeList);
    cmdList = initCmdList();
    for (ii = 0; actualNodeList[ii]; ii++)
    {
        elog(INFO, "Clean coordinator master %s resources.\n", actualNodeList[ii]);
        if ((cmd = prepare_cleanCoordinatorMaster(actualNodeList[ii])))
            addCmd(cmdList, cmd);
        else
            elog(WARNING, "WARNING: coordinator master %s not found.\n", actualNodeList[ii]);
    }
    rc = doCmdList(cmdList);
    cleanCmdList(cmdList);
    CleanArray(actualNodeList);
    elog(INFO, "Done.\n");
    return (rc);
}

int clean_coordinator_master_all(void)
{
    elog(INFO, "Cleaning all the coordinator masters resources.\n");
    return(clean_coordinator_master(aval(VAR_coordNames)));
}

/*
 * Cleanup coordinator slave resources -- directory and the socket.
 */
cmd_t *prepare_cleanCoordinatorSlave(char *nodeName)
{
    cmd_t *cmd;
    int idx;

    if ((idx = coordIdx(nodeName)) < 0)
    {
        elog(ERROR, "ERROR: %s is not a coordinator.\n", nodeName);
        return NULL;
    }
    if (!doesExist(VAR_coordSlaveServers, idx) || is_none(aval(VAR_coordSlaveServers)[idx]))
        return NULL;
    cmd = initCmd(aval(VAR_coordMasterServers)[idx]);
    snprintf(newCommand(cmd), MAXLINE,
             "rm -rf %s;mkdir -p %s;chmod 0700 %s; rm -f /tmp/.s.*%d*; rm -f /tmp/.s.*%d*",
             aval(VAR_coordSlaveDirs)[idx], aval(VAR_coordSlaveDirs)[idx], aval(VAR_coordSlaveDirs)[idx],
             atoi(aval(VAR_coordSlavePorts)[idx]), atoi(aval(VAR_coordSlavePoolerPorts)[idx]));
    return cmd;
}

int clean_coordinator_slave(char **nodeList)
{
    char **actualNodeList;
    cmdList_t *cmdList;
    int ii;
    cmd_t *cmd;
    int rc;

    actualNodeList = makeActualNodeList(nodeList);
    cmdList = initCmdList();
    for (ii = 0; actualNodeList[ii]; ii++)
    {
        elog(INFO, "Clean coordinator slave %s resources.\n", actualNodeList[ii]);
        if ((cmd = prepare_cleanCoordinatorSlave(actualNodeList[ii])))
            addCmd(cmdList, cmd);
        else
            elog(WARNING, "WARNING: coordinator slave %s not found.\n", actualNodeList[ii]);
    }
    rc = doCmdList(cmdList);
    cleanCmdList(cmdList);
    CleanArray(actualNodeList);
    elog(INFO, "Done.\n");
    return (rc);
}

int clean_coordinator_slave_all(void)
{
    elog(INFO, "Cleaning all the coordinator slave resources.\n");
    return(clean_coordinator_slave(aval(VAR_coordNames)));
}

/*------------------------------------------------------------------------
 *
 * Add command
 *
 *-----------------------------------------------------------------------*/
int add_coordinatorMaster(char *name, char *host, int port, int pooler,
        char *dir, char *extraConf, char *extraPgHbaConf)
{
    FILE *f, *lockf;
    int size, idx;
    char port_s[MAXTOKEN+1];
    char pooler_s[MAXTOKEN+1];
    char max_wal_senders_s[MAXTOKEN+1];
    int connCordIndx;
    char pgdumpall_out[MAXPATH+1];
    char **nodelist = NULL;
    int ii, jj;
    char **confFiles = NULL;
    char **pgHbaConfFiles = NULL;

    /* Check if all the coordinator masters are running */
    if (!check_AllCoordRunning())
    {
        elog(ERROR, "ERROR: Some of the coordinator masters are not running. Cannot add one.\n");
        return 1;
    }
    /* Check if there's no conflict with the current configuration */
    if (checkNameConflict(name, FALSE))
    {
        elog(ERROR, "ERROR: Node name %s duplicate.\n", name);
        return 1;
    }
    if (checkPortConflict(host, port) || checkPortConflict(host, pooler))
    {
        elog(ERROR, "ERROR: port numbrer (%d) or pooler port (%d) at host %s conflicts.\n", port, pooler, host);
        return 1;
    }
    if (checkDirConflict(host, dir))
    {
        elog(ERROR, "ERROR: directory \"%s\" conflicts at host %s.\n", dir, host);
        return 1;
    }
    /*
     * Check if coordinator masgter configuration is consistent
     */
    idx = size = arraySizeName(VAR_coordNames);
    if ((arraySizeName(VAR_coordPorts) != size) ||
        (arraySizeName(VAR_poolerPorts) != size) ||
        (arraySizeName(VAR_coordMasterServers) != size) ||
        (arraySizeName(VAR_coordMasterDirs) != size) ||
        (arraySizeName(VAR_coordMaxWALSenders) != size) ||
        (arraySizeName(VAR_coordSpecificExtraConfig) != size) ||
        (arraySizeName(VAR_coordSpecificExtraPgHba) != size))
    {
        elog(ERROR, "ERROR: Found some conflicts in coordinator master configuration.");
        return 1;
    }
    /*
     * Now reconfigure
     */
    /* Need an API to expand the array to desired size */
    if ((extendVar(VAR_coordNames, idx + 1, "none") != 0) ||
        (extendVar(VAR_coordMasterServers, idx + 1, "none")  != 0) ||
        (extendVar(VAR_coordPorts, idx + 1, "none")  != 0) ||
        (extendVar(VAR_poolerPorts, idx + 1, "none")  != 0) ||
        (extendVar(VAR_coordMasterDirs, idx + 1, "none")  != 0) ||
        (extendVar(VAR_coordMaxWALSenders, idx + 1, "none")  != 0) ||
        (extendVar(VAR_coordSlaveServers, idx + 1, "none")  != 0) ||
        (extendVar(VAR_coordSlavePorts, idx + 1, "none")  != 0) ||
        (extendVar(VAR_coordSlaveDirs, idx + 1, "none")  != 0) ||
        (extendVar(VAR_coordArchLogDirs, idx + 1, "none")  != 0) ||
        (extendVar(VAR_coordSpecificExtraConfig, idx + 1, "none")  != 0) ||
        (extendVar(VAR_coordSpecificExtraPgHba, idx + 1, "none") != 0))
    {
        elog(PANIC, "PANIC: Internal error, inconsistent coordinator information\n");
        return 1;
    }
    /*
     * 000 We need another way to configure specific pg_hba.conf and max_wal_senders.
     */
    snprintf(port_s, MAXTOKEN, "%d", port);
    snprintf(pooler_s, MAXTOKEN, "%d", pooler);
    snprintf(max_wal_senders_s, MAXTOKEN, "%d", getDefaultWalSender(true));
    assign_arrayEl(VAR_coordNames, idx, name, NULL);
    assign_arrayEl(VAR_coordMasterServers, idx, host, NULL);
    assign_arrayEl(VAR_coordPorts, idx, port_s, "-1");
    assign_arrayEl(VAR_poolerPorts, idx, pooler_s, NULL);
    assign_arrayEl(VAR_coordMasterDirs, idx, dir, NULL);
    assign_arrayEl(VAR_coordMaxWALSenders, idx, max_wal_senders_s, NULL);
    assign_arrayEl(VAR_coordSlaveServers, idx, "none", NULL);
    assign_arrayEl(VAR_coordSlavePorts, idx, "none", NULL);
    assign_arrayEl(VAR_coordSlavePoolerPorts, idx, "none", NULL);
    assign_arrayEl(VAR_coordSlaveDirs, idx, "none", NULL);
    assign_arrayEl(VAR_coordArchLogDirs, idx, "none", NULL);
    assign_arrayEl(VAR_coordSpecificExtraConfig, idx, extraConf, NULL);
    assign_arrayEl(VAR_coordSpecificExtraPgHba, idx, extraPgHbaConf, NULL);
    handle_no_slaves();
    /*
     * Update the configuration file and backup it
     */
    /*
     * Take care of extra conf file
     */
    if (doesExist(VAR_coordExtraConfig, 0) && !is_none(sval(VAR_coordExtraConfig)))
        AddMember(confFiles, sval(VAR_coordExtraConfig));
    if (doesExist(VAR_coordSpecificExtraConfig, idx) && !is_none(aval(VAR_coordSpecificExtraConfig)[idx]))
        AddMember(confFiles, aval(VAR_coordSpecificExtraConfig)[idx]);

    /*
     * Take care of extra pg_hba conf file
     */
    if (doesExist(VAR_coordExtraPgHba, 0) && !is_none(sval(VAR_coordExtraPgHba)))
        AddMember(pgHbaConfFiles, sval(VAR_coordExtraPgHba));
    if (doesExist(VAR_coordSpecificExtraPgHba, idx) && !is_none(aval(VAR_coordSpecificExtraPgHba)[idx]))
        AddMember(pgHbaConfFiles, aval(VAR_coordSpecificExtraPgHba)[idx]);
    
    /*
     * Main part
     */
    if ((f = fopen(pgxc_ctl_config_path, "a")) == NULL)
    {
        /* Should it be panic? */
        elog(ERROR, "ERROR: cannot open configuration file \"%s\", %s\n", pgxc_ctl_config_path, strerror(errno));
        return 1;
    }
    fprintf(f, 
            "#===================================================\n"
            "# pgxc configuration file updated due to coordinator master addition\n"
            "#        %s\n",
            timeStampString(date, MAXTOKEN+1));
    fprintAval(f, VAR_coordNames);
    fprintAval(f, VAR_coordMasterServers);
    fprintAval(f, VAR_coordPorts);
    fprintAval(f, VAR_poolerPorts);
    fprintAval(f, VAR_coordMasterDirs);
    fprintAval(f, VAR_coordMaxWALSenders);
    fprintSval(f, VAR_coordSlave);
    fprintAval(f, VAR_coordSlaveServers);
    fprintAval(f, VAR_coordSlavePorts);
    fprintAval(f, VAR_coordSlavePoolerPorts);
    fprintAval(f, VAR_coordSlaveDirs);
    fprintAval(f, VAR_coordArchLogDirs);
    fprintAval(f, VAR_coordSpecificExtraConfig);
    fprintAval(f, VAR_coordSpecificExtraPgHba);
    fprintf(f, "%s", "#----End of reconfiguration -------------------------\n");
    fclose(f);
    backup_configuration();

    /* Now add the master */

    /* initdb */
    doImmediate(host, NULL, "PGXC_CTL_SILENT=1 initdb -D %s --nodename %s --nodetype coordinator "
                            "--master_gtm_nodename %s --master_gtm_ip %s --master_gtm_port %s", 
                            dir, name,
                            sval(VAR_gtmName),
                            sval(VAR_gtmMasterServer),
                            sval(VAR_gtmMasterPort));

    /* Edit configurations */
    if ((f = pgxc_popen_w(host, "cat >> %s/postgresql.conf", dir)))
    {
        appendFiles(f, confFiles);
        fprintf(f,
                "#===========================================\n"
                "# Added at initialization. %s\n"
                "port = %d\n"
                "pooler_port = %d\n"
                "# End of Additon\n",
                timeStampString(date, MAXTOKEN+1),
                port, pooler);
        pclose(f);
    }
    CleanArray(confFiles);
    jj = coordIdx(name);
    if ((f = pgxc_popen_w(host, "cat >> %s/pg_hba.conf", dir)))
    {
        int kk;
        
        fprintf(f, "#===========================================\n");
        fprintf(f, "# Added at initialization.\n");

        appendFiles(f, pgHbaConfFiles);
        for (kk = 0; aval(VAR_coordPgHbaEntries)[kk]; kk++)
        {
            fprintf(f,"host all %s %s trust\n",    sval(VAR_pgxcOwner), aval(VAR_coordPgHbaEntries)[kk]);
            if (isVarYes(VAR_coordSlave))
                if (!is_none(aval(VAR_coordSlaveServers)[jj]))
                    fprintf(f, "host replication %s %s trust\n",
                            sval(VAR_pgxcOwner), aval(VAR_coordPgHbaEntries)[kk]);
        }
        fprintf(f, "# End of addition\n");
        pclose(f);
    }

    /* find any available coordinator */
    connCordIndx = get_any_available_coord(-1);
    if (connCordIndx == -1)
    {
        /*
         * This is the FIRST coordinator being added into
         * the cluster. Just start it and be done with it.
         *
         * Start the new coordinator with --coordinator option
         */
        AddMember(nodelist, name);
        start_coordinator_master(nodelist);
        CleanArray(nodelist);

        /* ALTER our own definition appropriately */
        goto selfadd;
    }

    /* Lock ddl */
    if ((lockf = pgxc_popen_wRaw("psql -h %s -p %s %s",
                    aval(VAR_coordMasterServers)[connCordIndx],
                    aval(VAR_coordPorts)[connCordIndx],
                    sval(VAR_defaultDatabase))) == NULL)
    {
        elog(ERROR, "ERROR: could not open psql command, %s\n", strerror(errno));
        Free(pgHbaConfFiles);
        return 1;
    }
    fprintf(lockf, "select pgxc_lock_for_backup();\n");    /* Keep open until the end of the addition. */
    fflush(lockf);

    /* pg_dumpall */
    createLocalFileName(GENERAL, pgdumpall_out, MAXPATH);
    doImmediateRaw("pg_dumpall -p %s -h %s -s --include-nodes --dump-nodes --file=%s",
                   aval(VAR_coordPorts)[connCordIndx],
                   aval(VAR_coordMasterServers)[connCordIndx], pgdumpall_out);

    /* Start the new coordinator */
    doImmediate(host, NULL, "pg_ctl start -w -Z restoremode -D %s -o -i", dir);

    /* Allow the new coordinator to start up by sleeping for a couple of seconds */
    pg_usleep(2000000L);

    /* Restore the backup */
    doImmediateRaw("psql -h %s -p %d -d %s -f %s", host, port, sval(VAR_defaultDatabase), pgdumpall_out);
    doImmediateRaw("rm -f %s", pgdumpall_out);

    /* Quit the new coordinator */
    doImmediate(host, NULL, "pg_ctl stop -w -Z restoremode -D %s", dir);

    /* Start the new coordinator with --coordinator option */
    AddMember(nodelist, name);
    start_coordinator_master(nodelist);
    CleanArray(nodelist);

    /* Issue CREATE NODE on coordinators */
    for (ii = 0; aval(VAR_coordNames)[ii]; ii++)
    {
        if (!is_none(aval(VAR_coordNames)[ii]) && strcmp(aval(VAR_coordNames)[ii], name) != 0)
        {
            if ((f = pgxc_popen_wRaw("psql -h %s -p %d %s", aval(VAR_coordMasterServers)[ii], atoi(aval(VAR_coordPorts)[ii]), sval(VAR_defaultDatabase))) == NULL)
            {
                elog(ERROR, "ERROR: cannot connect to the coordinator master %s.\n", aval(VAR_coordNames)[ii]);
                continue;
            }
            fprintf(f, "CREATE NODE %s WITH (TYPE = 'coordinator', host='%s', PORT=%d);\n", name, host, port);
            fprintf(f, "SELECT pgxc_pool_reload();\n");
            fprintf(f, "\\q\n");
            pclose(f);
        }
    }
    /* Issue CREATE NODE on datanodes */
    for (ii = 0; aval(VAR_datanodeNames)[ii]; ii++)
    {
        if (!is_none(aval(VAR_datanodeNames)[ii]))
        {
            if ((f = pgxc_popen_wRaw("psql -h %s -p %d %s",
                            aval(VAR_datanodeMasterServers)[ii],
                            atoi(aval(VAR_datanodePorts)[ii]),
                            sval(VAR_defaultDatabase))) == NULL)
            {
                elog(ERROR, "ERROR: cannot connect to the datanode master %s.\n", aval(VAR_datanodeNames)[ii]);
                continue;
            }
            fprintf(f, "CREATE NODE %s WITH (TYPE = 'coordinator', host='%s', PORT=%d);\n", name, host, port);
            fprintf(f, "SELECT pgxc_pool_reload();\n");
            fprintf(f, "\\q\n");
            pclose(f);
        }
    }
    /* Quit DDL lokkup session */
    fprintf(lockf, "\\q\n");
    pclose(lockf);

selfadd:
    if ((f = pgxc_popen_wRaw("psql -h %s -p %d %s", host, port, sval(VAR_defaultDatabase))) == NULL)
        elog(ERROR, "ERROR: cannot connect to the coordinator master %s.\n", name);
    else
    {
        fprintf(f, "ALTER NODE %s WITH (host='%s', PORT=%d);\n", name, host, port);
        fprintf(f, "SELECT pgxc_pool_reload();\n");
        fprintf(f, "\\q\n");
        pclose(f);
    }
    return 0;
}

int add_coordinatorSlave(char *name, char *host, int port, int pooler_port, char *dir, char *archDir)
{
    int idx;
    FILE *f;
    char port_s[MAXTOKEN+1];
    char pooler_s[MAXTOKEN+1];
    int kk;
    int size;
    char *__p__ = NULL;

    /* Check if the name is valid coordinator */
    if ((idx = coordIdx(name)) < 0)
    {
        elog(ERROR, "ERROR: Specified coordiantor %s is not configured.\n", name);
        return 1;
    }
    /* Check if the coordinator slave is not configured */
    if (isVarYes(VAR_coordSlave) && doesExist(VAR_coordSlaveServers, idx) && !is_none(aval(VAR_coordSlaveServers)[idx]))
    {
        elog(ERROR, "ERROR: Slave for the coordinator %s has already been configured.\n", name);
        return 1;
    }
    /* Check if the resource does not conflict */
    if (strcmp(dir, archDir) == 0)
    {
        elog(ERROR, "ERROR: working directory is the same as WAL archive directory.\n");
        return 1;
    }
    /* 
     * We don't check the name conflict here because acquiring valid coordiinator index means that
     * there's no name conflict.
     */
    if (checkPortConflict(host, port))
    {
        elog(ERROR, "ERROR: the port %s has already been used in the host %s.\n",  aval(VAR_coordPorts)[idx], host);
        return 1;
    }
    if (checkDirConflict(host, dir) || checkDirConflict(host, archDir))
    {
        elog(ERROR, "ERROR: directory %s or %s has already been used by other node.\n", dir, archDir);
        return 1;
    }
    /* Check if the coordinator master is running */
    if (pingNode(aval(VAR_coordMasterServers)[idx], aval(VAR_coordPorts)[idx]) != 0)
    {
        elog(ERROR, "ERROR: Coordinator master %s is not running.\n", name);
        return 1;
    }

    snprintf(port_s, MAXTOKEN, "%d", port);
    snprintf(pooler_s, MAXTOKEN, "%d", pooler_port);
    
    /* Prepare the resources (directories) */
    doImmediate(host, NULL, "mkdir -p %s;chmod 0700 %s", dir, dir);
    doImmediate(host, NULL, "rm -rf %s; mkdir -p %s;chmod 0700 %s", archDir, archDir, archDir);
    /* Reconfigure the master with WAL archive */
    /* Update the configuration and backup the configuration file */
    if ((f = pgxc_popen_w(aval(VAR_coordMasterServers)[idx], "cat >> %s/postgresql.conf", aval(VAR_coordMasterDirs)[idx])) == NULL)
    {
        elog(ERROR, "ERROR: Cannot open coordinator master's configuration file, %s/postgresql.conf, %s\n",
             aval(VAR_coordMasterDirs)[idx], strerror(errno));
        return 1;
    }
    fprintf(f, 
            "#========================================\n"
            "# Addition for log shipping, %s\n"
            "wal_level = logical \n"
            "archive_mode = off \n"
            "# archive_command = 'rsync %%p %s@%s:%s/%%f'\n"
            "max_wal_senders = %d\n"
            "# End of Addition\n",
			timeStampString(date, MAXTOKEN+1),
            sval(VAR_pgxcUser), host, archDir,
            getDefaultWalSender(TRUE));
    pclose(f);
    /* pg_hba.conf for replication */
    if ((f = pgxc_popen_w(aval(VAR_coordMasterServers)[idx], "cat >> %s/pg_hba.conf", aval(VAR_coordMasterDirs)[idx])) == NULL)
    {
        elog(ERROR, "ERROR: Cannot open coordinator master's pg_hba.conf file, %s/pg_hba.conf, %s\n", 
             aval(VAR_coordMasterDirs)[idx], strerror(errno));
        return 1;
    }
    fprintf(f, 
            "#================================================\n"
            "# Additional entry by adding the slave, %s\n",
            timeStampString(date, MAXTOKEN+1));

    for (kk = 0; aval(VAR_coordPgHbaEntries)[kk]; kk++)
    {
        fprintf(f, "host replication %s %s trust\n",
                sval(VAR_pgxcOwner), aval(VAR_coordPgHbaEntries)[kk]);
    }

    __p__=getIpAddress(host);
    fprintf(f,
            "host replication %s %s/32 trust\n"
            "# End of addition ===============================\n",
            sval(VAR_pgxcOwner),__p__);
    Free(__p__);
    pclose(f);
    /* Reconfigure pgxc_ctl configuration with the new slave */
    size = arraySizeName(VAR_coordNames);
    /* Need an API to expand the array to desired size */
    if ((extendVar(VAR_coordSlaveServers, size, "none") != 0) ||
        (extendVar(VAR_coordSlaveDirs, size, "none")  != 0) ||
        (extendVar(VAR_coordSlavePorts, size, "none")  != 0) ||
        (extendVar(VAR_coordSlavePoolerPorts, size, "none")  != 0) ||
        (extendVar(VAR_coordArchLogDirs, size, "none") != 0))
    {
        elog(PANIC, "PANIC: Internal error, inconsistent coordinator information\n");
        return 1;
    }
    if (!isVarYes(VAR_coordSlave))
        assign_sval(VAR_coordSlave, "y");
    replace_arrayEl(VAR_coordSlaveServers, idx, host, NULL);
    replace_arrayEl(VAR_coordSlavePorts, idx, port_s, NULL);
    replace_arrayEl(VAR_coordSlavePoolerPorts, idx, pooler_s, NULL);
    replace_arrayEl(VAR_coordSlaveDirs, idx, dir, NULL);
    replace_arrayEl(VAR_coordArchLogDirs, idx, archDir, NULL);
    /* Update the configuration file and backup it */
    if ((f = fopen(pgxc_ctl_config_path, "a")) == NULL)
    {
        /* Should it be panic? */
        elog(ERROR, "ERROR: cannot open configuration file \"%s\", %s\n", pgxc_ctl_config_path, strerror(errno));
        return 1;
    }
    fprintf(f, 
            "#===================================================\n"
            "# pgxc configuration file updated due to coordinator slave addition\n"
            "#        %s\n",
            timeStampString(date, MAXTOKEN+1));
    fprintSval(f, VAR_coordSlave);
    fprintAval(f, VAR_coordSlaveServers);
    fprintAval(f, VAR_coordSlavePorts);
    fprintAval(f, VAR_coordSlavePoolerPorts);
    fprintAval(f, VAR_coordArchLogDirs);
    fprintAval(f, VAR_coordSlaveDirs);
    fprintf(f, "%s", "#----End of reconfiguration -------------------------\n");
    fclose(f);
    backup_configuration();

    /* Restart the master */
    /*
     * It's not a good idea to use "restart" here because some connection from other coordinators
     * may be alive.   They are posessed by the pooler and we have to reload the pool to release them,
     * which aborts all the transactions.
     *
     * Beacse we need to issue pgxc_pool_reload() at all the coordinators, we need to give up all the
     * transactions in the whole cluster.
     *
     * It is much better to shutdow the target coordinator master fast because it does not affect
     * transactions this coordinator is not involved.
     */
    doImmediate(aval(VAR_coordMasterServers)[idx], NULL, 
                "pg_ctl stop -w -Z coordinator -D %s -m fast", aval(VAR_coordMasterDirs)[idx]);
    doImmediate(aval(VAR_coordMasterServers)[idx], NULL, 
                "pg_ctl start -w -Z coordinator -D %s", aval(VAR_coordMasterDirs)[idx]);
    /* pg_basebackup */
	doImmediate(host, NULL, "pg_basebackup -U %s -p %s -h %s -D %s --wal-method=stream",
				aval(VAR_pgxcOwner)[0],aval(VAR_coordPorts)[idx], aval(VAR_coordMasterServers)[idx], dir);
    /* Update the slave configuration with hot standby and port */
    if ((f = pgxc_popen_w(host, "cat >> %s/postgresql.conf", dir)) == NULL)
    {
        elog(ERROR, "ERROR: Cannot open the new slave's postgresql.conf, %s\n", strerror(errno));
        return 1;
    }
    fprintf(f,
            "#==========================================\n"
            "# Added to initialize the slave, %s\n"
            "hot_standby = on\n"
            "port = %d\n"
            "pooler_port = %d\n"
            "wal_level = logical \n"
            "archive_mode = off\n"        /* No archive mode */
            "archive_command = ''\n"    /* No archive mode */
            "max_wal_senders = 0\n"        /* Minimum WAL senders */
            "# End of Addition\n",
            timeStampString(date, MAXTOKEN),
            atoi(aval(VAR_coordSlavePorts)[idx]),
            atoi(aval(VAR_coordSlavePoolerPorts)[idx]));
    pclose(f);
    /* Update the slave recovery.conf */
    if ((f = pgxc_popen_w(host, "cat >> %s/recovery.conf", dir)) == NULL)
    {
        elog(ERROR, "ERROR: Cannot open the slave's recovery.conf, %s\n", strerror(errno));
        return 1;
    }
    fprintf(f,
            "#==========================================\n"
            "# Added to add the slave, %s\n"
            "standby_mode = on\n"
            "primary_conninfo = 'host = %s port = %s "
            "user = %s application_name = %s'\n"
            "restore_command = 'cp %s/%%f %%p'\n"
            "archive_cleanup_command = 'pg_archivecleanup %s %%r'\n"
            "# End of addition\n",
            timeStampString(date, MAXTOKEN), aval(VAR_coordMasterServers)[idx], aval(VAR_coordPorts)[idx],
            sval(VAR_pgxcOwner), aval(VAR_coordNames)[idx], 
            aval(VAR_coordArchLogDirs)[idx], aval(VAR_coordArchLogDirs)[idx]);
    pclose(f);

    /* Start the slave */
    doImmediate(host, NULL, "pg_ctl start -w -Z coordinator -D %s", dir);
    return 0;
}


/*------------------------------------------------------------------------
 *
 * Remove command
 *
 *-----------------------------------------------------------------------*/
int remove_coordinatorMaster(char *name, int clean_opt)
{
    /*

    Removing an existing coordinator
    ==========================

    Assume a two coordinator cluster, COORD_1 & COORD_2
    Suppose we want to remove COORD2 for any reason.

    1. Stop the coordinator to be removed.
    In our example we need to stop COORD_2.

    2. Connect to any of the coordinators except the one to be removed.
    In our example assuming COORD_1 is running on port 5432,
    the following command would connect to COORD_1

    psql postgres -p 5432

    3. Drop the coordinator to be removed.
    For example to drop coordinator COORD_2

    DROP NODE COORD_2;

    4. Update the connection information cached in pool.

    SELECT pgxc_pool_reload();

    COORD_2 is now removed from the cluster & COORD_1 would work as if COORD_2 never existed.

    CAUTION : If COORD_2 is still running and clients are connected to it,  any queries issued would create inconsistencies in the cluster.

    Please note that there is no need to block DDLs because either way DDLs will fail after step 1 and before step 4.

     */

    int idx;
    int ii;
    FILE *f;
    char **namelist = NULL;
    char date[MAXTOKEN+1];

    /* Check if the coordinator is configured */
    if ((idx = coordIdx(name)) < 0)
    {
        elog(ERROR, "ERROR: Coordinator %s is not configured.\n", name);
        return 1;
    }
    /* Check if all the other coordinators are running */
    for (ii = 0; aval(VAR_coordNames)[ii]; ii++)
    {
        if ((ii != idx) && !is_none(aval(VAR_coordNames)[ii]) && (pingNode(aval(VAR_coordMasterServers)[ii], aval(VAR_coordPorts)[ii]) != 0))
        {
            elog(ERROR, "ERROR: Coordinator master %s is not running.\n", aval(VAR_coordNames)[ii]);
            return 1;
        }
    }
    /* Check if there's a slave configured */
    if (doesExist(VAR_coordSlaveServers, idx) && !is_none(aval(VAR_coordSlaveServers)[idx]))
        remove_coordinatorSlave(name, clean_opt);
#if 0
    /* Stop the coordinator master if running */
    if (pingNode(aval(VAR_coordMasterServers)[idx], aval(VAR_coordPorts)[idx]) == 0)
    {
        AddMember(namelist, name);
        stop_coordinator_master(namelist, "fast");
        CleanArray(namelist);
    }
    /* Cleanup the coordinator master resource if specified */
    if (clean_opt)
        doImmediate(aval(VAR_coordMasterServers)[idx], NULL, "rm -rf %s", aval(VAR_coordMasterDirs)[idx]);
#endif
    /* Issue "drop node" at all the other coordinators */
    for (ii = 0; aval(VAR_coordNames)[ii]; ii++)
    {
        if ((ii != idx) && doesExist(VAR_coordNames, ii) && !is_none(aval(VAR_coordNames)[ii]))
        {
            f = pgxc_popen_wRaw("psql -p %d -h %s %s", atoi(aval(VAR_coordPorts)[ii]), aval(VAR_coordMasterServers)[ii], sval(VAR_defaultDatabase));
            if (f == NULL)
            {
                elog(ERROR, "ERROR: cannot begin psql for the coordinator master %s\n", aval(VAR_coordNames)[ii]);
                continue;
            }
            fprintf(f, "DROP NODE %s;\n", name);
            fprintf(f, "SELECT pgxc_pool_reload();\n");
            fprintf(f, "\\q");
            pclose(f);
        }
    }
    /* Issue "drop node" at all the datanodes */
    for (ii = 0; aval(VAR_datanodeNames)[ii]; ii++)
    {
        if (doesExist(VAR_datanodeNames, ii) && !is_none(aval(VAR_datanodeNames)[ii]))
        {
            int coord_idx = get_any_available_coord(idx);
            if (coord_idx == -1)
                return 1;

            f = pgxc_popen_wRaw("psql -p %d -h %s %s", atoi(aval(VAR_coordPorts)[coord_idx]), aval(VAR_coordMasterServers)[coord_idx], sval(VAR_defaultDatabase));
            if (f == NULL)
            {
                elog(ERROR, "ERROR: cannot begin psql for the coordinator master %s\n", aval(VAR_coordNames)[coord_idx]);
                continue;
            }
            fprintf(f, "EXECUTE DIRECT ON (%s) 'DROP NODE %s';\n",
                    aval(VAR_datanodeNames)[ii], name);
            fprintf(f, "EXECUTE DIRECT ON (%s) 'SELECT pgxc_pool_reload()';\n",
                    aval(VAR_datanodeNames)[ii]);
            fprintf(f, "\\q");
            pclose(f);
        }
    }
#if 1
    /* Stop the coordinator master if running */
    if (pingNode(aval(VAR_coordMasterServers)[idx], aval(VAR_coordPorts)[idx]) == 0)
    {
        AddMember(namelist, name);
        stop_coordinator_master(namelist, "fast");
        CleanArray(namelist);
    }
    /* Cleanup the coordinator master resource if specified */
    if (clean_opt)
        doImmediate(aval(VAR_coordMasterServers)[idx], NULL, "rm -rf %s", aval(VAR_coordMasterDirs)[idx]);
#endif
    /* Update configuration and backup --> should cleanup "none" entries here */
    replace_arrayEl(VAR_coordNames, idx, "none", NULL);
    replace_arrayEl(VAR_coordMasterDirs, idx, "none", NULL);
    replace_arrayEl(VAR_coordPorts, idx, "-1", "-1");
    replace_arrayEl(VAR_poolerPorts, idx, "-1", "-1");
    replace_arrayEl(VAR_coordMasterServers, idx, "none", NULL);
    replace_arrayEl(VAR_coordMaxWALSenders, idx, "0", "0");
    replace_arrayEl(VAR_coordSlaveServers, idx, "none", NULL);
    replace_arrayEl(VAR_coordSlavePorts, idx, "none", NULL);
    replace_arrayEl(VAR_coordSlavePoolerPorts, idx, "none", NULL);
    replace_arrayEl(VAR_coordSlaveDirs, idx, "none", NULL);
    replace_arrayEl(VAR_coordArchLogDirs, idx, "none", NULL);
    replace_arrayEl(VAR_coordSpecificExtraConfig, idx, "none", NULL);
    handle_no_slaves();
    /*
     * Write config files
     */
    if ((f = fopen(pgxc_ctl_config_path, "a")) == NULL)
    {
        /* Should it be panic? */
        elog(ERROR, "ERROR: cannot open configuration file \"%s\", %s\n", pgxc_ctl_config_path, strerror(errno));
        return 1;
    }
    fprintf(f, 
            "#================================================================\n"
            "# pgxc configuration file updated due to coodinator master removal\n"
            "#        %s\n",
            timeStampString(date, MAXTOKEN+1));
    fprintSval(f, VAR_coordSlave);
    fprintAval(f, VAR_coordNames);
    fprintAval(f, VAR_coordMasterDirs);
    fprintAval(f, VAR_coordPorts);
    fprintAval(f, VAR_poolerPorts);
    fprintAval(f, VAR_coordMasterServers);
    fprintAval(f, VAR_coordMaxWALSenders);
    fprintAval(f, VAR_coordSlaveServers);
    fprintAval(f, VAR_coordSlavePorts);
    fprintAval(f, VAR_coordSlavePoolerPorts);
    fprintAval(f, VAR_coordSlaveDirs);
    fprintAval(f, VAR_coordArchLogDirs);
    fprintAval(f, VAR_coordSpecificExtraConfig);
    fclose(f);
    backup_configuration();
    return 0;
}

int remove_coordinatorSlave(char *name, int clean_opt)
{
    int idx;
    char **nodelist = NULL;
    FILE *f;

    if (!isVarYes(VAR_coordSlave))
    {
        elog(ERROR, "ERROR: coordinator slave is not configured.\n");
        return 1;
    }
    idx = coordIdx(name);
    if (idx < 0)
    {
        elog(ERROR, "ERROR: coordinator %s is not configured.\n", name);
        return 1;
    }
    if (!doesExist(VAR_coordSlaveServers, idx) || is_none(aval(VAR_coordSlaveServers)[idx]))
    {
        elog(ERROR, "ERROR: coordinator slave %s is not configured.\n", name);
        return 1;
    }
    AddMember(nodelist, name);
    if (pingNodeSlave(aval(VAR_coordSlaveServers)[idx], aval(VAR_coordSlaveDirs)[idx]) == 0)
        stop_coordinator_slave(nodelist, "immediate");
    {
        FILE *f;
        if ((f = pgxc_popen_w(aval(VAR_coordMasterServers)[idx], "cat >> %s/postgresql.conf", aval(VAR_coordMasterDirs)[idx])) == NULL)
        {
            elog(ERROR, "ERROR: cannot open %s/postgresql.conf at %s, %s\n", aval(VAR_coordMasterDirs)[idx], aval(VAR_coordMasterServers)[idx], strerror(errno));
            return 1;
        }
        fprintf(f,
                "#=======================================\n"
                "# Updated to remove the slave %s\n"
                "archive_mode = off\n"
                "synchronous_standby_names = ''\n"
                "archive_command = ''\n"
                "max_wal_senders = 0\n"
                "wal_level = minimal\n"
                "# End of the update\n",
                timeStampString(date, MAXTOKEN));
        pclose(f);
    }
    doImmediate(aval(VAR_coordMasterServers)[idx], NULL, "pg_ctl restart -Z coordinator -D %s", aval(VAR_coordMasterDirs)[idx]);
    if (clean_opt)
        clean_coordinator_slave(nodelist);
    /*
     * Maintain variables
     */
    replace_arrayEl(VAR_coordSlaveServers, idx, "none", NULL);
    replace_arrayEl(VAR_coordSlavePorts, idx, "none", NULL);
    replace_arrayEl(VAR_coordSlavePoolerPorts, idx, "none", NULL);
    replace_arrayEl(VAR_coordSlaveDirs, idx, "none", NULL);
    replace_arrayEl(VAR_coordArchLogDirs, idx, "none", NULL);
    handle_no_slaves();
    /*
     * Maintain configuration file
     */
    if ((f = fopen(pgxc_ctl_config_path, "a")) == NULL)
    {
        /* Should it be panic? */
        elog(ERROR, "ERROR: cannot open configuration file \"%s\", %s\n", pgxc_ctl_config_path, strerror(errno));
        Free(nodelist);
        return 1;
    }
    fprintf(f, 
            "#================================================================\n"
            "# pgxc configuration file updated due to coodinator slave removal\n"
            "#        %s\n",
            timeStampString(date, MAXTOKEN));
    fprintSval(f, VAR_coordSlave);
    fprintAval(f, VAR_coordSlaveServers);
    fprintAval(f, VAR_coordSlavePorts);
    fprintAval(f, VAR_coordSlavePoolerPorts);
    fprintAval(f, VAR_coordSlaveDirs);
    fprintAval(f, VAR_coordArchLogDirs);
    fclose(f);
    backup_configuration();
    CleanArray(nodelist);
    return 0;

}



/*
 * Start coordinator master ---------------------------------------------
 */
int start_coordinator_master_all(void)
{
    elog(INFO, "Starting coordinator master.\n");
    return(start_coordinator_master(aval(VAR_coordNames)));
}

cmd_t *prepare_startCoordinatorMaster(char *nodeName)
{
    cmd_t *cmd = NULL, *cmdPgCtl;
    int idx;

    if ((idx = coordIdx(nodeName)) < 0)
    {
        elog(WARNING, "WARNING: %s is not a coordinator, skipping.\n", nodeName);
        return(NULL);
    }
    /*
     * Check if the coordinator is running
     */
    if (pingNode(aval(VAR_coordMasterServers)[idx], aval(VAR_coordPorts)[idx]) == 0)
    {
        elog(ERROR, "ERROR: target coordinator master %s is already running now.   Skip initialization.\n",
             nodeName);
        return(NULL);
    }
    cmd = cmdPgCtl = initCmd(aval(VAR_coordMasterServers)[idx]);
    snprintf(newCommand(cmdPgCtl), MAXLINE,
             "pg_ctl start -w -Z coordinator -D %s -o -i",
             aval(VAR_coordMasterDirs)[idx]);
    return(cmd);
}

int start_coordinator_master(char **nodeList)
{
    char **actualNodeList;
    int ii;
    cmdList_t *cmdList;
    cmd_t *cmd;
    int rc;

    actualNodeList = makeActualNodeList(nodeList);
    cmdList = initCmdList();
    for (ii = 0; actualNodeList[ii]; ii++)
    {
        elog(INFO, "Starting coordinator master %s\n", actualNodeList[ii]);
        if ((cmd = prepare_startCoordinatorMaster(actualNodeList[ii])))
            addCmd(cmdList, cmd);
    }
    rc = doCmdList(cmdList);
    cleanCmdList(cmdList);
    CleanArray(actualNodeList);
    elog(INFO, "Done.\n");
    return(rc);
}

/*
 * Start coordinator slaves ----------------------------------------
 */
int start_coordinator_slave_all(void)
{
    elog(INFO, "Starting all the coordinator slaves.\n");
    return(start_coordinator_slave(aval(VAR_coordNames)));
}

cmd_t *prepare_startCoordinatorSlave(char *nodeName)
{
    int idx;
    FILE *f;
    char timestamp[MAXTOKEN+1];
    cmd_t *cmd = NULL, *cmdPgCtlStart, *cmdPgConfMaster, *cmdMasterReload;

    if ((idx = coordIdx(nodeName)) < 0)
    {
        elog(WARNING, "WARNING: %s is not a coordinator, skipping.\n", nodeName);
        return(NULL);
    }
    /*
     * Check if the coordinator is running
     */
    if (pingNode(aval(VAR_coordMasterServers)[idx], aval(VAR_coordPorts)[idx]) != 0)
    {
        elog(ERROR, "ERROR: Coordinator master %s is not running now. Cannot start the slave.\n",
             aval(VAR_coordNames)[idx]);
        return(NULL);
    }
    cmd = cmdPgCtlStart = initCmd(aval(VAR_coordSlaveServers)[idx]);
    snprintf(newCommand(cmdPgCtlStart), MAXLINE,
             "pg_ctl start -w -Z coordinator -D %s -o -i",
             aval(VAR_coordSlaveDirs)[idx]);

    /* Postgresql.conf at the Master */

    appendCmdEl(cmdPgCtlStart, (cmdPgConfMaster = initCmd(aval(VAR_coordMasterServers)[idx])));
    snprintf(newCommand(cmdPgConfMaster), MAXLINE,
             "cat >> %s/postgresql.conf", aval(VAR_coordMasterDirs)[idx]);
    if ((f = prepareLocalStdin(newFilename(cmdPgConfMaster->localStdin), MAXPATH, NULL)) == NULL)
    {
        cleanCmd(cmd);
        Free(cmdPgCtlStart);
        return(NULL);
    }
    fprintf(f,
            "#==========================================================\n"
            "# Added to start the slave in sync. mode, %s\n"
            "# synchronous_standby_names = '%s'\n"
            "# End of the addition\n",
            timeStampString(timestamp, MAXTOKEN),
            aval(VAR_coordNames)[idx]);
    fclose(f);
        
    /* Reload postgresql.conf change */
    appendCmdEl(cmdPgCtlStart, (cmdMasterReload = initCmd(aval(VAR_coordMasterServers)[idx])));
    snprintf(newCommand(cmdMasterReload), MAXLINE,
             "pg_ctl reload -Z coordinator -D %s",
             aval(VAR_coordMasterDirs)[idx]);
    return(cmd);
}

int start_coordinator_slave(char **nodeList)
{
    char **actualNodeList;
    int ii;
    cmdList_t *cmdList;
    cmd_t *cmd;
    int rc;

    if (!isVarYes(VAR_coordSlave))
    {
        elog(ERROR, "ERROR: Coordinator slaves are not configured.\n");
        return(1);
    }
    cmdList = initCmdList();
    actualNodeList = makeActualNodeList(nodeList);
    for (ii = 0; actualNodeList[ii]; ii++)
    {
        elog(INFO, "Starting coordinator slave %s.\n", actualNodeList[ii]);
        if ((cmd = prepare_startCoordinatorSlave(actualNodeList[ii])))
            addCmd(cmdList, cmd);
    }
    rc = doCmdList(cmdList);
    cleanCmdList(cmdList);
    CleanArray(actualNodeList);
    elog(INFO, "Done\n");
    return(rc);
}

/*
 * Stop coordinator masters ---------------------------------------------------
 */
/* Does not check if immediate is valid here */
int stop_coordinator_master_all(char *immediate)
{
    elog(INFO, "Stopping all the coordinator masters.\n");
    return(stop_coordinator_master(aval(VAR_coordNames), immediate));
}

cmd_t *prepare_stopCoordinatorMaster(char *nodeName, char *immediate)
{
    int idx;
    cmd_t *cmd;

    if ((idx = coordIdx(nodeName)) < 0)
    {
        elog(WARNING, "WARNING: %s is not a coordinator.\n", nodeName);
        return(NULL);
    }
    cmd = initCmd(aval(VAR_coordMasterServers)[idx]);
    if (immediate)
        snprintf(newCommand(cmd), MAXLINE,
                 "pg_ctl stop -w -Z coordinator -D %s -m %s",
                 aval(VAR_coordMasterDirs)[idx], immediate);
    else
        snprintf(newCommand(cmd), MAXLINE,
                 "pg_ctl stop -w -Z coordinator -D %s",
                 aval(VAR_coordMasterDirs)[idx]);
    return(cmd);
}
             

/* Does not check if immediate is valid here. */
int stop_coordinator_master(char **nodeList, char *immediate)
{
    char **actualNodeList;
    int ii;
    cmdList_t *cmdList;
    int rc;

    if (immediate == NULL)
        immediate = FAST;
    actualNodeList = makeActualNodeList(nodeList);
    cmdList = initCmdList();
    for (ii = 0; actualNodeList[ii]; ii++)
    {
        cmd_t *cmd;
        elog(INFO, "Stopping coordinator master %s.\n", actualNodeList[ii]);
        if ((cmd = prepare_stopCoordinatorMaster(actualNodeList[ii], immediate)))
            addCmd(cmdList, cmd);
    }
    rc = doCmdList(cmdList);
    cleanCmdList(cmdList);
    CleanArray(actualNodeList);
    elog(INFO, "Done.\n");
    return(rc);
}


/*
 * Stop coordinator slaves ----------------------------------------------------
 */
int stop_coordinator_slave_all(char *immediate)
{
    elog(INFO, "Stopping all the coordinator slaves.\n");
    return(stop_coordinator_slave(aval(VAR_coordNames), immediate));
}

cmd_t *prepare_stopCoordinatorSlave(char *nodeName, char *immediate)
{
    int idx;
    cmd_t *cmd = NULL, *cmdMasterReload, *cmdPgCtlStop;
    FILE *f;
    char localStdin[MAXPATH+1];
    char timestamp[MAXTOKEN+1];

    if ((idx = coordIdx(nodeName)) < 0)
    {
        elog(WARNING, "WARNING: %s is not a coordinator.\n", nodeName);
        return(NULL);
    }
    if (!doesExist(VAR_coordSlaveServers, idx) || is_none(aval(VAR_coordSlaveServers)[idx]))
    {
        elog(WARNING, "coordinator %s does not have a slave. Skipping.\n", nodeName);
        return(NULL);
    }
    if (pingNode(aval(VAR_coordMasterServers)[idx], aval(VAR_coordPorts)[idx]) == 0)
    {
        /* Master is running.  Need to switch log shipping to asynchronous mode. */
        cmd = cmdMasterReload = initCmd(aval(VAR_coordMasterServers)[idx]);
        if ((f = prepareLocalStdin(localStdin, MAXPATH, NULL)) == NULL)
        {
            cleanCmd(cmd);
            Free(cmdMasterReload);
            return(NULL);
        }
        fprintf(f,
                "#=======================================\n"
                "# Updated to trun off the slave %s\n"
                "synchronous_standby_names = ''\n"
                "# End of the update\n",
                timeStampString(timestamp, MAXTOKEN));
        fclose(f);
        snprintf(newCommand(cmdMasterReload), MAXLINE,
                 "cat >> %s/postgresql.conf",
                 aval(VAR_coordMasterDirs)[idx]);
        cmdMasterReload->localStdin = Strdup(localStdin);
    }
    if (cmd)
        appendCmdEl(cmdMasterReload, (cmdPgCtlStop = initCmd(aval(VAR_coordSlaveServers)[idx])));
    else
        cmd = cmdPgCtlStop = initCmd(aval(VAR_coordSlaveServers)[idx]);
    if (immediate)
        snprintf(newCommand(cmdPgCtlStop), MAXLINE,
                 "pg_ctl stop -w -Z coordinator -D %s -m %s",
                 aval(VAR_coordSlaveDirs)[idx], immediate);
    else
        snprintf(newCommand(cmdPgCtlStop), MAXLINE,
                 "pg_ctl stop -w -Z coordinator -D %s",
                 aval(VAR_coordSlaveDirs)[idx]);
    return(cmd);
}


int stop_coordinator_slave(char **nodeList, char *immediate)
{
    char **actualNodeList;
    int ii;
    cmdList_t *cmdList;
    cmd_t *cmd;
    int rc;

    if (!isVarYes(VAR_coordSlave))
    {
        elog(ERROR, "ERROR: Coordinator slaves are not configured.\n");
        return(1);
    }
    if (immediate == NULL)
        immediate = "fast";
    actualNodeList = makeActualNodeList(nodeList);
    cmdList = initCmdList();
    for (ii = 0; actualNodeList[ii]; ii++)
    {
        elog(INFO, "Stopping the coordinator slave %s.\n", actualNodeList[ii]);
        if ((cmd = prepare_stopCoordinatorSlave(actualNodeList[ii], immediate)))
            addCmd(cmdList, cmd);
    }
    rc = doCmdList(cmdList);
    cleanCmdList(cmdList);
    CleanArray(actualNodeList);
    return(rc);
}

/*
 * Failover coordinator ---------------------------------------------------------
 */
int failover_coordinator(char **nodeList)
{
    char **actualNodeList;
    int ii;
    int rc = 0;

    elog(INFO, "Failover coordinators.\n");
    if (!isVarYes(VAR_coordSlave))
    {
        elog(ERROR, "ERROR: Coordinator slaves are not configured.\n");
        return(2);
    }
    actualNodeList = makeActualNodeList(nodeList);
    for (ii = 0; actualNodeList[ii]; ii++)
    {
        int idx;
        int rc_local;

        elog(INFO, "Failover the coordinator %s.\n", actualNodeList[ii]);
        if ((idx = coordIdx(actualNodeList[ii])) < 0)
        {
            elog(ERROR, "ERROR: %s is not a coordinator. Skipping.\n", actualNodeList[ii]);
            continue;
        }
        if (is_none(aval(VAR_coordSlaveServers)[idx]))
        {
            elog(ERROR, "ERROR: slave of the coordinator %s is not configured. Skipping\n",
                 actualNodeList[ii]);
            continue;
        }
        rc_local = failover_oneCoordinator(idx);
        if (rc_local < 0)
        {
            Free(actualNodeList);
            return(rc_local);
        }
        else
            if (rc_local > rc)
                rc = rc_local;
    }
    elog(INFO, "Done.\n");
    return(rc);
}

static int failover_oneCoordinator(int coordIdx)
{
    int    rc = 0;
    int rc_local;
    int jj;
    int    gtmPxyIdx;
    //char *gtmHost;
    //char *gtmPort;
    FILE *f;
    char timestamp[MAXTOKEN+1];
    char cmd[MAXLINE];
    int     cmdlen;
    bool dnReconfigured;
    
#define checkRc() do{if(WEXITSTATUS(rc_local) > rc) rc = WEXITSTATUS(rc_local);}while(0)

    /*
     * Determine the target gtm
     */
    gtmPxyIdx= getEffectiveGtmProxyIdxFromServerName(aval(VAR_coordSlaveServers)[coordIdx]);
    //gtmHost = (gtmPxyIdx < 0) ? sval(VAR_gtmMasterServer) :
        //aval(VAR_gtmProxyServers)[gtmPxyIdx];
    //gtmPort = (gtmPxyIdx < 0) ? sval(VAR_gtmMasterPort) :
        //aval(VAR_gtmProxyPorts)[gtmPxyIdx];
    if (gtmPxyIdx >= 0)
        elog(NOTICE, "Failover coordinator %s using gtm %s\n",
             aval(VAR_coordNames)[coordIdx], aval(VAR_gtmProxyNames)[gtmPxyIdx]);
    else
        elog(NOTICE, "Failover coordinator %s using GTM itself\n",
             aval(VAR_coordNames)[coordIdx]);

    /* Promote the slave */
    rc_local = doImmediate(aval(VAR_coordSlaveServers)[coordIdx], NULL,
                           "pg_ctl promote -Z coordinator -D %s",
                           aval(VAR_coordSlaveDirs)[coordIdx]);
    checkRc();

    /* Reconfigure new coordinator master with new gtm_proxy or gtm */

    if ((f =  pgxc_popen_w(aval(VAR_coordSlaveServers)[coordIdx],
                           "cat >> %s/postgresql.conf",
                           aval(VAR_coordSlaveDirs)[coordIdx])) == NULL)
    {
        elog(ERROR, "ERROR: Could not prepare to update postgresql.conf, %s", strerror(errno));
        return(-1);
    }
    fprintf(f,
            "#=================================================\n"
            "# Added to promote, %s\n"
            "# End of addition\n",
            timeStampString(timestamp, MAXTOKEN));
    pclose(f);

    /* Restart coord Slave Server */
    rc_local = doImmediate(aval(VAR_coordSlaveServers)[coordIdx], NULL,
                           "pg_ctl restart -Z coordinator -D %s -w -o -i; sleep 1",
                           aval(VAR_coordSlaveDirs)[coordIdx]);
    checkRc();
    
    /* Update the configuration variable */
    var_assign(&(aval(VAR_coordMasterServers)[coordIdx]), Strdup(aval(VAR_coordSlaveServers)[coordIdx]));
    var_assign(&(aval(VAR_coordPorts)[coordIdx]), Strdup(aval(VAR_coordSlavePorts)[coordIdx]));
    var_assign(&(aval(VAR_poolerPorts)[coordIdx]), Strdup(aval(VAR_coordSlavePoolerPorts)[coordIdx]));
    var_assign(&(aval(VAR_coordSlaveServers)[coordIdx]), Strdup("none"));
    var_assign(&(aval(VAR_coordSlavePorts)[coordIdx]), Strdup("none"));
    var_assign(&(aval(VAR_coordSlavePoolerPorts)[coordIdx]), Strdup("none"));
    var_assign(&(aval(VAR_coordMasterDirs)[coordIdx]), Strdup(aval(VAR_coordSlaveDirs)[coordIdx]));
    var_assign(&(aval(VAR_coordSlaveDirs)[coordIdx]), Strdup("none"));

    if ((f = fopen(pgxc_ctl_config_path, "a")) == NULL)
    {
        elog(ERROR, "ERROR: Failed to open configuration file %s, %s\n", pgxc_ctl_config_path, strerror(errno));
        return(-1);
    }
    fprintf(f,
            "#=====================================================\n"
            "# Updated due to the coordinator failover, %s, %s\n"
            "coordMasterServers=( %s )\n"
            "coordPorts=( %s )\n"
            "poolerPorts=( %s )\n"
            "coordMasterDirs=( %s )\n"
            "coordSlaveServers=( %s )\n"
            "coordSlavePorts=( %s )\n"
            "coordSlavePoolerPorts=( %s )\n"
            "coordSlaveDirs=( %s )\n"
            "# End of the update\n",
            aval(VAR_coordNames)[coordIdx], timeStampString(timestamp, MAXTOKEN),
            listValue(VAR_coordMasterServers),
            listValue(VAR_coordPorts),
            listValue(VAR_poolerPorts),
            listValue(VAR_coordMasterDirs),
            listValue(VAR_coordSlaveServers),
            listValue(VAR_coordSlavePorts),
            listValue(VAR_coordSlavePoolerPorts),
            listValue(VAR_coordSlaveDirs));
    fclose(f);

    /* Backup the configuration file */
    if (isVarYes(VAR_configBackup))
    {
        rc_local = doConfigBackup();
        checkRc();
    }

    cmdlen = 0;
    cmd[0] = '\0';
    /*
     * Reconfigure datanodes with the new datanode. We prepare the commands and
     * pass them to the first coordinator we reconfigure later
     */
    for (jj = 0; aval(VAR_datanodeNames)[jj]; jj++)
    {
        int len;

        if (is_none(aval(VAR_datanodeMasterServers)[jj]))
            continue;
            
        if (pingNode(aval(VAR_datanodeMasterServers)[jj], aval(VAR_datanodePorts)[jj]) != 0)
        {
            elog(ERROR, "Datanode %s is not running.  Skip reconfiguration for this datanode.\n",
                 aval(VAR_coordNames)[jj]);
            continue;
        }
        
        len = snprintf(cmd + cmdlen, MAXLINE - cmdlen, "EXECUTE DIRECT ON (%s) 'ALTER NODE %s WITH (HOST=''%s'', PORT=%s)';\n"
                "EXECUTE DIRECT ON (%s) 'select pgxc_pool_reload()';\n",
                                 aval(VAR_datanodeNames)[jj],
                                 aval(VAR_coordNames)[coordIdx],
                                 aval(VAR_coordMasterServers)[coordIdx],
                                 aval(VAR_coordPorts)[coordIdx],
                                 aval(VAR_datanodeNames)[jj]);
        if (len > (MAXLINE - cmdlen))
        {
            elog(ERROR, "Datanode command exceeds the maximum allowed length");
            return -1;
        }
        cmdlen += len;
    }
    dnReconfigured = false;

    /*
     * Reconfigure coordinators with new coordinator
     */
    for (jj = 0; aval(VAR_coordNames)[jj]; jj++)
    {
        if (is_none(aval(VAR_coordMasterServers)[jj]))
            continue;
            
        if (pingNode(aval(VAR_coordMasterServers)[jj], aval(VAR_coordPorts)[jj]) != 0)
        {
            elog(ERROR, "Coordinator %s is not running.  Skip reconfiguration for this coordinator.\n",
                 aval(VAR_coordNames)[jj]);
            continue;
        }
        if ((f = pgxc_popen_wRaw("psql -p %s -h %s %s %s",
                                 aval(VAR_coordPorts)[jj],
                                 aval(VAR_coordMasterServers)[jj],
                                 sval(VAR_defaultDatabase),
                                 sval(VAR_pgxcOwner)))
            == NULL)
        {
            elog(ERROR, "ERROR: failed to start psql for coordinator %s, %s\n", aval(VAR_coordNames)[jj], strerror(errno));
            continue;
        }
        fprintf(f,
                "ALTER NODE %s WITH (HOST='%s', PORT=%s);\n"
                "select pgxc_pool_reload();\n"
                "%s"
                "\\q\n",
                aval(VAR_coordNames)[coordIdx],
                aval(VAR_coordMasterServers)[coordIdx],
                aval(VAR_coordPorts)[coordIdx],
                dnReconfigured ? "" : cmd);
        pclose(f);
    }
    return(rc);

#    undef checkRc
}

/*
 * Show coordinator configuration
 */
int show_config_coordMasterSlaveMulti(char **nodeList)
{
    int ii;
    uint idx;

    lockLogFile();
    for (ii = 0; nodeList[ii]; ii++)
    {
        if ((idx = coordIdx(nodeList[ii])) < 0)
            continue;
        else
        {
            show_config_coordMaster(TRUE, idx, aval(VAR_coordMasterServers)[idx]);
            if (isVarYes(VAR_coordSlave))
                show_config_coordSlave(TRUE, idx, aval(VAR_coordSlaveServers)[idx]);
        }
    }
    unlockLogFile();
    return 0;
}

int show_config_coordMasterMulti(char **nodeList)
{
    int ii;
    uint idx;

    lockLogFile();
    for (ii = 0; nodeList[ii]; ii++)
    {
        if ((idx = coordIdx(nodeList[ii])) < 0)
            continue;
        else
            show_config_coordMaster(TRUE, idx, aval(VAR_coordMasterServers)[idx]);
    }
    unlockLogFile();
    return 0;
}

int show_config_coordSlaveMulti(char **nodeList)
{
    int ii;
    int idx;

    if (!isVarYes(VAR_coordSlave))
        return(1);
    lockLogFile();
    for (ii = 0; nodeList[ii]; ii++)
    {
        if ((idx = coordIdx(nodeList[ii])) < 0)
            continue;
        else
            show_config_coordSlave(TRUE, idx, aval(VAR_coordSlaveServers)[idx]);
    }
    unlockLogFile();
    return 0;
}

int show_config_coordMaster(int flag, uint idx, char *hostname)
{
    int ii;
    char outBuf[MAXLINE+1];
    char editBuf[MAXPATH+1];

    outBuf[0] = 0;
    if (flag)
        strncat(outBuf, "Coordinator Master: ", MAXLINE);
    if (hostname)
    {
        snprintf(editBuf, MAXPATH, "host: %s", hostname);
        strncat(outBuf, editBuf, MAXLINE);
    }
    if (flag || hostname)
        strncat(outBuf, "\n", MAXLINE);
    lockLogFile();
    if (outBuf[0])
        elog(NOTICE, "%s", outBuf);
    elog(NOTICE, "    Nodename: '%s', port: %s, pooler port: %s\n", 
         aval(VAR_coordNames)[idx], aval(VAR_coordPorts)[idx], aval(VAR_poolerPorts)[idx]);
    elog(NOTICE, "    MaxWalSenders: %s, Dir: '%s'\n", 
         aval(VAR_coordMaxWALSenders)[idx], aval(VAR_coordMasterDirs)[idx]);
    elog(NOTICE, "    ExtraConfig: '%s', Specific Extra Config: '%s'\n",
         sval(VAR_coordExtraConfig), aval(VAR_coordSpecificExtraConfig)[idx]);
    strncpy(outBuf, "    pg_hba entries ( ", MAXLINE);
    for (ii = 0; aval(VAR_coordPgHbaEntries)[ii]; ii++)
    {
        snprintf(editBuf, MAXPATH, "'%s' ", aval(VAR_coordPgHbaEntries)[ii]);
        strncat(outBuf, editBuf, MAXLINE);
    }
    elog(NOTICE, "%s)\n", outBuf);
    elog(NOTICE, "    Extra pg_hba: '%s', Specific Extra pg_hba: '%s'\n",
                sval(VAR_coordExtraPgHba), aval(VAR_coordSpecificExtraPgHba)[idx]);
    unlockLogFile();
    return 0;
}

int show_config_coordSlave(int flag, int idx, char *hostname)
{
    char outBuf[MAXLINE+1];
    char editBuf[MAXPATH+1];

    outBuf[0] = 0;
    if (flag)
        strncat(outBuf, "Coordinator Slave: ", MAXLINE);
    if (hostname)
    {
        snprintf(editBuf, MAXPATH,  "host: %s", hostname);
        strncat(outBuf, editBuf, MAXLINE);
    }
    if (flag || hostname)
        strncat(outBuf, "\n", MAXLINE);
    lockLogFile();
    if (outBuf[0])
        elog(NOTICE, "%s", outBuf);
    elog(NOTICE,"    Nodename: '%s', port: %s, pooler port: %s\n",
         aval(VAR_coordNames)[idx], aval(VAR_coordSlavePorts)[idx],
         aval(VAR_coordSlavePoolerPorts)[idx]);
    elog(NOTICE, "    Dir: '%s', Archive Log Dir: '%s'\n",
         aval(VAR_coordSlaveDirs)[idx], aval(VAR_coordArchLogDirs)[idx]);
    unlockLogFile();
    return 0;
}


/*
 * Checks if all the coordinators are running
 *
 * Returns FALSE if any of them are not running.
 */
int check_AllCoordRunning(void)
{
    int ii;

    for (ii = 0; aval(VAR_coordMasterServers)[ii]; ii++)
    {
        if (!is_none(aval(VAR_coordMasterServers)[ii]))
            if (pingNode(aval(VAR_coordMasterServers)[ii], aval(VAR_coordPorts)[ii]) != 0)
                return FALSE;
    }
    return TRUE;
}
