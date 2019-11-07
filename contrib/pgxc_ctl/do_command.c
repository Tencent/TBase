/*-------------------------------------------------------------------------
 *
 * do_command.c
 *
 *    Main command module of Postgres-XC configuration and operation tool.
 *
 * Copyright (c) 2013 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
 */
/*
 * This file provides a frontend module to pgxc_ctl operation.
 */
#include <stdio.h>
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
#include "gtm_cmd.h"
#include "coord_cmd.h"
#include "datanode_cmd.h"
#include "gtm_util.h"
#include "monitor.h"

extern char *pgxc_ctl_conf_prototype[];
extern char *pgxc_ctl_conf_prototype_minimal[];
extern char *pgxc_ctl_conf_prototype_empty[];

int forceInit = false;

#define Exit(c) exit(myWEXITSTATUS(c))
#define GetToken() (line = get_word(line, &token))
#define TestToken(word) ((token != NULL) && (strcasecmp(token, word) == 0))
#define testToken(word) ((token != NULL) && (strcmp(token, word) == 0))

static void kill_something(char *token);
static void do_deploy(char *line);
static void deploy_xc(char **hostlist);
static void show_config_something(char *nodeName);
static void show_config_something_multi(char **nodeList);
extern void show_config_hostList(char **hostList);
static void show_config_host(char *hostname);
static void show_basicConfig(void);
static void show_config_servers(char **hostList);
static void do_clean_command(char *line);
static void do_start_command(char *line);
static void start_all(void);
static void do_stop_command(char *line);
static void stop_all(char *immediate);
static int show_Resource(char *datanodeName, char *databasename, char *username);
static void do_show_help(char *line);

typedef enum ConfigType
{
    CONFIG_EMPTY,
    CONFIG_MINIMAL,
    CONFIG_COMPLETE
} ConfigType;

static void do_echo_command(char * line)
{
    printf("do_echo_command\n");
}

static void do_prepareConfFile(char *Path, ConfigType config_type)
{
    char *path = NULL;
    FILE *conf;
    int ii;
    char **my_pgxc_conf_prototype;

    if (Path)
        path = Path;
    else
    {
        if (find_var(VAR_configFile) && sval(VAR_configFile))
            path = sval(VAR_configFile);
        else
        {
            elog(ERROR, "ERROR: Configuration file path was not specified.\n");
            return;
        }
    }
    conf = fopen(path, "w");
    if (conf == NULL)
    {
        elog(ERROR, "ERROR: Could not open the configuration file \"%s\", %s.\n", path, strerror(errno));
        return;
    }

    if (config_type == CONFIG_EMPTY)
        my_pgxc_conf_prototype = pgxc_ctl_conf_prototype_empty;
    else if (config_type == CONFIG_MINIMAL)
        my_pgxc_conf_prototype = pgxc_ctl_conf_prototype_minimal;
    else
        my_pgxc_conf_prototype = pgxc_ctl_conf_prototype;

    for (ii = 0; my_pgxc_conf_prototype[ii]; ii++)
    {
        fprintf(conf, "%s\n", my_pgxc_conf_prototype[ii]);
    }
    fclose(conf);
    return;
}

/*
 * Deploy pgxc binaries
 */

static void do_deploy(char *line)
{
    char *token;
    char **hostlist = NULL;

    if (GetToken() == NULL)
    {
        elog(ERROR, "ERROR: Please specify option for deploy command.\n");
        return;
    }
    if (TestToken("all"))
    {
        elog(NOTICE, "Deploying Postgres-XL components to all the target servers.\n");
        deploy_xc(aval(VAR_allServers));
    }
    else
    {
        elog(NOTICE, "Deploying Postgres-XL components.\n");
        /*
         * Please note that the following code does not check if the specified nost
         * appears in the configuration file.
         * We should deploy xc binary to the target not in the current configuraiton
         * to add gtm slave, gtm_proxy, coordinator/datanode master/slave online.
         */
        do {
            AddMember(hostlist, token);
        } while(GetToken());
        deploy_xc(hostlist);
        CleanArray(hostlist);
    }
}

static void deploy_xc(char **hostlist)
{
    char tarFile[MAXPATH+1];
    cmdList_t *cmdList;
    int ii;

    /* Build tarball --> need to do foreground */
    elog(NOTICE, "Prepare tarball to deploy ... \n");
    snprintf(tarFile, MAXPATH, "%d.tgz", getpid());
    doImmediate(NULL, NULL, "tar czCf %s %s/%s bin include lib share",
                sval(VAR_pgxcInstallDir),
                sval(VAR_localTmpDir), tarFile);

    /* Backgroud jobs */

    cmdList = initCmdList();
    /* Build install dir */
    for (ii = 0; hostlist[ii]; ii++)
    {
        cmd_t *cmd;
        cmd_t *cmdScp;
        cmd_t *cmdTarExtract;

        elog(NOTICE, "Deploying to the server %s.\n", hostlist[ii]);
        /* Build target directory */
        addCmd(cmdList, (cmd = initCmd(hostlist[ii])));
        snprintf(newCommand(cmd), MAXLINE,
                 "rm -rf %s/bin %s/include %s/lib %s/share; mkdir -p %s",
                 sval(VAR_pgxcInstallDir),
                 sval(VAR_pgxcInstallDir),
                 sval(VAR_pgxcInstallDir),
                 sval(VAR_pgxcInstallDir),
                 sval(VAR_pgxcInstallDir));
        /* SCP tarball */
        appendCmdEl(cmd, (cmdScp = initCmd(NULL)));
        snprintf(newCommand(cmdScp), MAXLINE,
                 "scp %s/%s %s@%s:%s",
                 sval(VAR_localTmpDir), tarFile, sval(VAR_pgxcUser), hostlist[ii], sval(VAR_tmpDir));
        /* Extract Tarball and remove it */
        appendCmdEl(cmd, (cmdTarExtract = initCmd(hostlist[ii])));
        snprintf(newCommand(cmdTarExtract), MAXLINE,
                 "tar xzCf %s %s/%s; rm %s/%s",
                  sval(VAR_pgxcInstallDir), 
                  sval(VAR_tmpDir), tarFile,
                  sval(VAR_tmpDir), tarFile);
    }
    doCmdList(cmdList);
    cleanCmdList(cmdList);
    doImmediate(NULL, NULL, "rm -f %s/%s",
                sval(VAR_tmpDir), tarFile);
    elog(NOTICE, "Deployment done.\n");
}

static void do_set(char *line)
{
    
    char *token;
    char *varname;
    pgxc_ctl_var *var;

    if (GetToken() == NULL)
    {
        elog(ERROR, "ERROR: No variable name was given\n");
        return;
    }
    varname = Strdup(token);
    var = confirm_var(varname);
    reset_value(var);
    while(GetToken())
    {
        add_val(var, token);
    }
    print_var(varname);
    log_var(varname);
    Free(varname);
    return;
}

/*
 * Failover command ... failover gtm
 *                        failover coordinator nodename
 *                        failover datanode nodename
 *                        failover nodename
 */
static void do_failover_command(char *line)
{
    char *token;
    int idx;

    if (GetToken() == NULL)
    {
        elog(ERROR, "ERROR: Please specify failover command option.\n");
        return;
    }
    else if (TestToken("gtm"))
    {
        if (isVarYes(VAR_gtmSlave) && !is_none(sval(VAR_gtmSlaveServer)))
            failover_gtm();
        else
            elog(ERROR, "ERROR: no gtm slave is configured.\n");
        return;
    }
    else if (TestToken("coordinator"))
    {
        if (!isVarYes(VAR_coordSlave))
            elog(ERROR, "ERROR: coordinator slave is not configured.\n");
        else if (!GetToken())
            elog(ERROR, "ERROR: please specify failover coordinator command option.\n");
        else
        {
            char **nodeList = NULL;

            do
            {
                if ((idx = coordIdx(token)) < 0)
                    elog(ERROR, "ERROR: %s is not a coordinator\n", token);
                else if (is_none(aval(VAR_coordSlaveServers)[idx]))
                    elog(ERROR, "ERROR: slave for the coordinator %s is not configured.\n", token);
                else
                    AddMember(nodeList, token);
            } while(GetToken());
            if (nodeList)
                failover_coordinator(nodeList);
            CleanArray(nodeList);
        }
        return;
    }
    else if (TestToken("datanode"))
    {
        if (!isVarYes(VAR_datanodeSlave))
            elog(ERROR, "ERROR: datanode slave is not configured.\n");
        else if (!GetToken())
            elog(ERROR, "ERROR: please specify failover datanode command option.\n");
        else
        {
            char **nodeList = NULL;

            do
            {
                if ((idx = datanodeIdx(token)) < 0)
                    elog(ERROR, "ERROR: %s is not a datanode.\n", token);
                else if (is_none(aval(VAR_datanodeSlaveServers)[idx]))
                    elog(ERROR, "ERROR: slave for the datanode %s is not configured,\n", token);
                else
                    AddMember(nodeList, token);
            } while(GetToken());
            if (nodeList)
                failover_datanode(nodeList);
            CleanArray(nodeList);
        }
    }
    else
        elog(ERROR, "ERROR: invalid failover command option %s.\n", token);
}

/*
 * Reconnect command ... reconnect gtm_proxy [all | nodename ... ]
 */
static void do_reconnect_command(char *line)
{
    char *token;

    if (GetToken() == NULL)
        elog(ERROR, "ERROR: Please specifiy option to reconnect command.\n");
    else if (TestToken("gtm_proxy"))
    {
        if (!isVarYes(VAR_gtmProxy))
            elog(ERROR, "ERROR: gtm proxy is not configured.\n");
        else if ((GetToken() == NULL) || TestToken("all"))
            reconnect_gtm_proxy_all();
        else
        {
            char **nodeList = NULL;
            int idx;
            do
            {
                if ((idx = gtmProxyIdx(token)) < 0)
                    elog(ERROR, "ERROR: %s is not gtm_proxy.\n", token);
                else
                    AddMember(nodeList, token);
            } while(GetToken());
            if (nodeList)
                reconnect_gtm_proxy(nodeList);
            CleanArray(nodeList);
        }
    }
    else
        elog(ERROR, "ERROR: invalid option %s for reconnect command.\n", token);
    return;
}

                

/*
 * Kill command ... kill nodename, kill all,
 *                    kill gtm [master|slave|all],
 *                    kill gtm_proxy [nodename|all] ...
 *                    kill coordinator [nodename ... |master [all | nodenames ... ] | slave [all | nodenames ... ] |all]
 *                    kill datanode [nodename ... |master [all | nodenames ... ] | slave [all | nodenames ... ] |all]
 */
static void do_kill_command(char *line)
{
    char *token;

    if (GetToken() == NULL)
        elog(ERROR, "ERROR: Please specifiy option to kill command\n");
    else if (TestToken("gtm"))
    {
        if ((GetToken() == NULL) || TestToken("all"))
        {
            kill_gtm_master();
            if (isVarYes(VAR_gtmSlave))
                kill_gtm_slave();
        }
        else if (TestToken("master"))
            kill_gtm_master();
        else if (TestToken("slave"))
        {
            if (isVarYes(VAR_gtmSlave))
                kill_gtm_slave();
            else
                elog(ERROR, "ERROR: GTM slave is not configured.\n");
        }
        else
            elog(ERROR, "ERROR: input value \"%s\" is invalid.\n", token);
        return;
    }
    else if (TestToken("gtm_proxy"))
    {
        if (GetToken() == NULL)
            elog(ERROR, "ERROR: Please specify additonal option to kill gtm_proxies\n");
        else if (TestToken("all"))
            kill_gtm_proxy(aval(VAR_gtmProxyNames));
        else
        {
            char **nodeList = Malloc0(sizeof(char*));
            do {
                AddMember(nodeList, token);
            } while(GetToken());
            kill_gtm_proxy(nodeList);
            clean_array(nodeList);
        }
        return;
    }
    else if (TestToken("coordinator"))
    {
        if ((GetToken() == NULL) || TestToken("all"))
        {
            kill_coordinator_master(aval(VAR_coordNames));
            if (isVarYes(VAR_coordSlave))
                kill_coordinator_slave(aval(VAR_coordNames));
        }
        if (TestToken("master"))
        {
            if ((GetToken() == NULL) || (TestToken("all")))
                kill_coordinator_master(aval(VAR_coordNames));
            else
            {
                char **nodeList = Malloc0(sizeof(char *));
                do {
                    AddMember(nodeList, token);
                } while (GetToken());
                kill_coordinator_master(nodeList);
                clean_array(nodeList);
            }
        }
        else if (TestToken("slave"))
        {
            if ((GetToken() == NULL) || (TestToken("all")))
                kill_coordinator_slave(aval(VAR_coordNames));
            else
            {
                char **nodeList = Malloc0(sizeof(char *));
                do {
                    AddMember(nodeList, token);
                } while (GetToken());
                kill_coordinator_slave(nodeList);
                clean_array(nodeList);
            }
        }
        else
        {
            char **nodeList = Malloc0(sizeof(char *));
            do {
                AddMember(nodeList, token);
            } while (GetToken());
            kill_coordinator_master(nodeList);
            if (isVarYes(VAR_coordSlave))
                kill_coordinator_slave(nodeList);
            clean_array(nodeList);
        }
        return;
    }
    else if (TestToken("datanode"))
    {
        if ((GetToken() == NULL) || (TestToken("all")))
        {
            kill_datanode_master(aval(VAR_datanodeNames));
            if (isVarYes(VAR_datanodeSlave))
                kill_datanode_slave(aval(VAR_coordNames));
        }
        else if (TestToken("master"))
        {
            if ((GetToken() == NULL) || (TestToken("all")))
                kill_datanode_master(aval(VAR_datanodeNames));
            else
            {
                char **nodeList = Malloc0(sizeof(char *));
                do{
                    AddMember(nodeList, token);
                } while (GetToken());
                kill_datanode_master(nodeList);
                clean_array(nodeList);
            }
        }
        else if (TestToken("slave"))
        {
            if ((GetToken() == NULL) || (TestToken("all")))
                kill_datanode_slave(aval(VAR_datanodeNames));
            else
            {
                char **nodeList = Malloc0(sizeof(char *));
                do {
                    AddMember(nodeList, token);
                } while (GetToken());
                kill_datanode_slave(nodeList);
                clean_array(nodeList);
            }
        }
        else
        {
            char **nodeList = Malloc0(sizeof(char *));
            do {
                AddMember(nodeList, token);
            } while (GetToken());
            kill_datanode_master(nodeList);
            if (isVarYes(VAR_datanodeSlave))
                kill_datanode_slave(nodeList);
            clean_array(nodeList);
        }
    }
    else if (TestToken("all"))
    {
        if(isVarYes(VAR_datanodeSlave))
            kill_datanode_slave(aval(VAR_datanodeNames));
        kill_datanode_master(aval(VAR_datanodeNames));
        if (isVarYes(VAR_coordSlave))
            kill_coordinator_slave(aval(VAR_coordNames));
        kill_coordinator_master(aval(VAR_coordNames));
        if (isVarYes(VAR_gtmProxy))
            kill_gtm_proxy(aval(VAR_gtmProxyNames));
        if (isVarYes(VAR_gtmSlave))
            kill_gtm_slave();
        kill_gtm_master();
    }
    else
    {
        do {
            kill_something(token);
        } while (GetToken());
    }
    return;
}


static void init_all(void)
{
    init_gtm_master(true);
    start_gtm_master();
    if (isVarYes(VAR_gtmSlave))
    {
        init_gtm_slave();
        start_gtm_slave();
    }
    if (isVarYes(VAR_gtmProxy))
    {
        init_gtm_proxy_all();
        start_gtm_proxy_all();
    }
    init_coordinator_master_all();
    start_coordinator_master_all();
    if (isVarYes(VAR_coordSlave))
    {
        init_coordinator_slave_all();
        start_coordinator_slave_all();
    }
    init_datanode_master_all();
    start_datanode_master_all();
    if (isVarYes(VAR_datanodeSlave))
    {
        init_datanode_slave_all();
        start_datanode_slave_all();
    }
    configure_nodes_all();
}


/*
 * Init command ... init all
 *                    init gtm [master|slave|all],
 *                    init gtm_proxy [all| nodename ...]
 *                    init coordinator [all | master [all | nodename ... ]| slave [all | nodename ... ]| nodename ... ]
 *                    init datanode [all | master [all | nodename ...] | slave [all | nodename ... ] | nodename ... ]
 */
static void do_init_command(char *line)
{
    char *token;

    if (GetToken() == NULL)
        elog(ERROR, "ERROR: Please specify option to init command.\n");
    
    if (TestToken("force"))
    {
        forceInit = true;
        if (GetToken() == NULL)
            elog(ERROR, "ERROR: Please specify option to init command.\n");
    }

    if (TestToken("all"))
        init_all();
    else if (TestToken("gtm"))
    {
        if (!GetToken() || (TestToken("all")))
        {
            init_gtm_master(true);
            if (isVarYes(VAR_gtmSlave))
                init_gtm_slave();
        }
        else if (TestToken("master"))
            init_gtm_master(true);
        else if (TestToken("slave"))
            init_gtm_slave();
        else
            elog(ERROR, "ERROR: please specify master, slave or all for init gtm command.\n");
    }
    else if (TestToken("gtm_proxy"))
        if (!GetToken() || TestToken("all"))
            init_gtm_proxy_all();
        else
        {
            char **nodeList = Malloc0(sizeof(char *));
            do {
                AddMember(nodeList, token);
            } while(GetToken());
            init_gtm_proxy(nodeList);
            clean_array(nodeList);
        }
    else if (TestToken("coordinator"))
        if (!GetToken() || TestToken("all"))
        {
            init_coordinator_master_all();
            if (isVarYes(VAR_coordSlave))
                init_coordinator_slave_all();
        }
        else if (TestToken("master"))
            if (!GetToken() || TestToken("all"))
                init_coordinator_master_all();
            else
            {
                char **nodeList = Malloc0(sizeof(char *));
                do {
                    AddMember(nodeList, token);
                } while(GetToken());
                init_coordinator_master(nodeList);
                clean_array(nodeList);
            }
        else if (TestToken("slave"))
            if (!GetToken() || TestToken("all"))
                init_coordinator_slave_all();
            else
            {
                char **nodeList = Malloc0(sizeof(char *));
                do {
                    AddMember(nodeList, token);
                } while(GetToken());
                init_coordinator_slave(nodeList);
                clean_array(nodeList);
            }
        else
        {
            char **nodeList = Malloc0(sizeof(char *));
            do
                AddMember(nodeList, token);
            while(GetToken());
            init_coordinator_master(nodeList);
            if (isVarYes(VAR_coordSlave))
                init_coordinator_slave(nodeList);
            clean_array(nodeList);
        }
    else if (TestToken("datanode"))
        if (!GetToken() || TestToken("all"))
        {
            init_datanode_master_all();
            if (isVarYes(VAR_datanodeSlave))
                init_datanode_slave_all();
        }
        else if (TestToken("master"))
            if (!GetToken() || TestToken("all"))
                init_datanode_master_all();
            else
            {
                char **nodeList = Malloc0(sizeof(char *));
                do
                    AddMember(nodeList, token);
                while(GetToken());
                init_datanode_master(nodeList);
                clean_array(nodeList);
            }
        else if (TestToken("slave"))
            if (!GetToken() || TestToken("all"))
                init_datanode_slave_all();
            else
            {
                char **nodeList = Malloc0(sizeof(char *));
                do
                    AddMember(nodeList, token);
                while(GetToken());
                init_datanode_slave(nodeList);
                clean_array(nodeList);
            }
        else 
        {
            char **nodeList = Malloc0(sizeof(char *));
            do
                AddMember(nodeList, token);
            while(GetToken());
            init_datanode_master(nodeList);
            if (isVarYes(VAR_datanodeSlave))
                init_datanode_slave(nodeList);
            Free(nodeList);
        }
    else
        elog(ERROR, "ERROR: invalid option for init command.\n");
    return;
}

/*
 * Start command ... start nodename, start all,
 *                     start gtm [master|slave|all],
 *                     start gtm_proxy [nodename|all] ...
 *                     start coordinator [nodename ... |master [all | nodenames ... ] | slave [all | nodenames ... ] |all]
 *                     start datanode [nodename ... |master [all | nodenames ... ] | slave [all | nodenames ... ] |all]
 */
static void start_all(void)
{
    start_gtm_master();
    if (isVarYes(VAR_gtmSlave))
        start_gtm_slave();
    if (isVarYes(VAR_gtmProxy))
        start_gtm_proxy_all();
    start_coordinator_master_all();
    if (isVarYes(VAR_coordSlave))
        start_coordinator_slave_all();
    start_datanode_master_all();
    if (isVarYes(VAR_datanodeSlave))
        start_datanode_slave_all();
}

static void do_start_command(char *line)
{
    char *token;

    if (GetToken() == NULL)
        elog(ERROR, "ERROR: Please specify option to start command.\n");
    else if (TestToken("all"))
        start_all();
    else if (TestToken("gtm"))
    {
        if (!GetToken() || (TestToken("all")))
        {
            start_gtm_master();
            if (isVarYes(VAR_gtmSlave))
                start_gtm_slave();
        }
        else if (TestToken("master"))
            start_gtm_master();
        else if (TestToken("slave"))
            start_gtm_slave();
        else
            elog(ERROR, "ERROR: please specify master, slave or all for start gtm command.\n");
    }
    else if (TestToken("gtm_proxy"))
        if (!GetToken() || TestToken("all"))
            start_gtm_proxy_all();
        else
        {
            char **nodeList = NULL;
            do
                AddMember(nodeList, token);
            while (GetToken());
            start_gtm_proxy(nodeList);
            clean_array(nodeList);
        }
    else if (TestToken("coordinator"))
        if (!GetToken() || TestToken("all"))
        {
            start_coordinator_master_all();
            if (isVarYes(VAR_coordSlave))
                start_coordinator_slave_all();
        }
        else if (TestToken("master"))
            if (!GetToken() || TestToken("all"))
                start_coordinator_master_all();
            else
            {
                char **nodeList = NULL;
                do
                    AddMember(nodeList, token);
                while(GetToken());
                start_coordinator_master(nodeList);
                clean_array(nodeList);
            }
        else if (TestToken("slave"))
            if (!GetToken() || TestToken("all"))
                start_coordinator_slave_all();
            else
            {
                char **nodeList = NULL;
                do
                    AddMember(nodeList, token);
                while(GetToken());
                start_coordinator_slave(nodeList);
                clean_array(nodeList);
            }
        else
        {
            char **nodeList = NULL;
            do
                AddMember(nodeList, token);
            while(GetToken());
            start_coordinator_master(nodeList);
            if (isVarYes(VAR_coordSlave))
                start_coordinator_slave(nodeList);
            clean_array(nodeList);
        }
    else if (TestToken("datanode"))
        if (!GetToken() || TestToken("all"))
        {
            start_datanode_master_all();
            if (isVarYes(VAR_datanodeSlave))
                start_datanode_slave_all();
        }
        else if (TestToken("master"))
            if (!GetToken() || TestToken("all"))
                start_datanode_master_all();
            else
            {
                char **nodeList = Malloc0(sizeof(char *));
                do
                    AddMember(nodeList, token);
                while(GetToken());
                start_datanode_master(nodeList);
                clean_array(nodeList);
            }
        else if (TestToken("slave"))
            if (!GetToken() || TestToken("all"))
                start_datanode_slave_all();
            else
            {
                char **nodeList = Malloc0(sizeof(char *));
                do
                    AddMember(nodeList, token);
                while(GetToken());
                start_datanode_slave(nodeList);
                clean_array(nodeList);
            }
        else 
        {
            char **nodeList = Malloc0(sizeof(char *));
            do
                AddMember(nodeList, token);
            while(GetToken());
            start_datanode_master(nodeList);
            if (isVarYes(VAR_datanodeSlave))
                start_datanode_slave(nodeList);
            Free(nodeList);
        }
    else
        elog(ERROR, "ERROR: invalid option for start command.\n");
    return;
}

/*
 * Stop command ... stop [-m smart | fast | immediate] all
 *                     stop gtm [master | slave | all],
 *                     stop gtm_proxy [ nodename | all] ...
 *                     stop [-m smart | fast | immediate ] coordinator [nodename ... | master [all | nodenames ... ] | slave [all | nodenames ... ] |all]
 *                     stop [-m smart | fast | immediate ] datanode [nodename ... | master [all | nodenames ... ] | slave [all | nodenames ... ] |all]
 *
 */
static void stop_all(char *immediate)
{
    if (isVarYes(VAR_coordSlave))
        stop_coordinator_slave_all(immediate);
    stop_coordinator_master_all(immediate);
    if (isVarYes(VAR_datanodeSlave))
        stop_datanode_slave_all(immediate);
    stop_datanode_master_all(immediate);
    if (isVarYes(VAR_gtmProxy))
        stop_gtm_proxy_all();
    if (isVarYes(VAR_gtmSlave))
        stop_gtm_slave();
    if (!is_none(sval(VAR_gtmMasterServer)))
        stop_gtm_master();
}


#define GetAndSet(var, msg) do{if(!GetToken()){elog(ERROR, msg); return;} var=Strdup(token);}while(0)
/*
 * Add command
 */
static void do_add_command(char *line)
{
    char *token;
    char *name;
    char *host;
    char *port;
    char *pooler;
    char *dir;
    char *walDir;
    char *archDir;
    char *extraConf;
    char *extraPgHbaConf;

    if (!GetToken())
    {
        elog(ERROR, "ERROR: Specify options for add command.\n");
        return;
    }
    if (!TestToken("gtm") && is_none(sval(VAR_gtmMasterServer)))
    {
        elog(ERROR, "ERROR: GTM master must be added before adding any other component.\n");
        return;
    }

    if (TestToken("gtm"))
    {
        /*
         * add gtm master name host port dir
         */

        if (!GetToken())
        {
            elog(ERROR, "ERROR: Specify option for add gtm command.\n");
            return;
        }
         if (TestToken("master"))
         {
             GetAndSet(name, "ERROR: please specify the name of gtm master\n");
             GetAndSet(host, "ERROR: please specify the host name for gtm master\n");
             GetAndSet(port, "ERROR: please specify the port number for gtm master\n");
             GetAndSet(dir, "ERROR: please specify the working director for gtm master\n");
             add_gtmMaster(name, host, atoi(port), dir);
         }
         else if (TestToken("slave"))
        {
            GetAndSet(name, "ERROR: please specify the name of gtm slave\n");
            GetAndSet(host, "ERROR: please specify the host name for gtm slave\n");
            GetAndSet(port, "ERROR: please specify the port number for gtm slave\n");
            GetAndSet(dir, "ERROR: please specify the working director for gtm slave\n");
            add_gtmSlave(name, host, atoi(port), dir);
        }
         else
         {
             elog(ERROR, "ERROR: you can specify only master/slave to add gtm command. %s is invalid.\n", token);
             return;
         }
        freeAndReset(name);
        freeAndReset(host);
        freeAndReset(port);
        freeAndReset(dir);
    }
    else if (TestToken("gtm_proxy"))
    {
        /*
         * Add gtm_proxy name host port dir
         */
        GetAndSet(name, "ERROR: please specify the name of gtm_proxy\n");
        GetAndSet(host, "ERROR: please specify the host name for gtm_proxy\n");
        GetAndSet(port, "ERROR: please specify the port number for gtm_proxy\n");
        GetAndSet(dir, "ERROR: please specify the working director for gtm_proxy\n");
        add_gtmProxy(name, host, atoi(port), dir);
        freeAndReset(name);
        freeAndReset(host);
        freeAndReset(port);
        freeAndReset(dir);
    }
    else if (TestToken("coordinator"))
    {
        /*
         * Add coordinator master name host port pooler dir
         * Add coordinator slave name host dir
         */
        if (!GetToken() || (!TestToken("master") && !TestToken("slave")))
        {
            elog(ERROR, "ERROR: please specify master or slave.\n");
            return;
        }
        if (TestToken("master"))
        {
            GetAndSet(name, "ERROR: please specify the name of the coordinator master\n");
            GetAndSet(host, "ERROR: please specify the host for the coordinator masetr\n");
            GetAndSet(port, "ERROR: please specify the port number for the coordinator master\n");
            GetAndSet(pooler, "ERROR: please specify the pooler port number for the coordinator master.\n");
            GetAndSet(dir, "ERROR: please specify the working directory for the coordinator master\n");
            GetAndSet(extraConf, "ERROR: please specify file to read extra configuration. Specify 'none' if nothing extra to be added.\n");
            GetAndSet(extraPgHbaConf, "ERROR: please specify file to read extra pg_hba configuration. Specify 'none' if nothing extra to be added.\n");
            add_coordinatorMaster(name, host, atoi(port), atoi(pooler), dir,
                    extraConf, extraPgHbaConf);
            freeAndReset(name);
            freeAndReset(host);
            freeAndReset(port);
            freeAndReset(pooler);
            freeAndReset(dir);
            freeAndReset(extraConf);
            freeAndReset(extraPgHbaConf);
        }
        else
        {
            GetAndSet(name, "ERROR: please specify the name of the coordinator slave\n");
            GetAndSet(host, "ERROR: please specify the host for the coordinator slave\n");
            GetAndSet(port, "ERROR: please specify the port number for the coordinator slave\n");
            GetAndSet(pooler, "ERROR: please specify the pooler port number for the coordinator slave.\n");
            GetAndSet(dir, "ERROR: please specify the working director for coordinator slave\n");
            GetAndSet(archDir, "ERROR: please specify WAL archive directory for coordinator slave\n");
            add_coordinatorSlave(name, host, atoi(port), atoi(pooler), dir, archDir);
            freeAndReset(name);
            freeAndReset(host);
            freeAndReset(dir);
            freeAndReset(archDir);
        }
    }
    else if (TestToken("datanode"))
    {
        if (!GetToken() || (!TestToken("master") && !TestToken("slave")))
        {
            elog(ERROR, "ERROR: please specify master or slave.\n");
            return;
        }
        if (TestToken("master"))
        {
            GetAndSet(name, "ERROR: please specify the name of the datanode master\n");
            GetAndSet(host, "ERROR: please specify the host for the datanode masetr\n");
            GetAndSet(port, "ERROR: please specify the port number for the datanode master\n");
            GetAndSet(pooler, "ERROR: please specify the pooler port number for the datanode master.\n");
            GetAndSet(dir, "ERROR: please specify the working director for the datanode master\n");
            GetAndSet(walDir, "ERROR: please specify the WAL directory for the datanode master WAL. Specify 'none' for default\n");
            GetAndSet(extraConf, "ERROR: please specify file to read extra configuration. Specify 'none' if nothig extra to be added.\n");
            GetAndSet(extraPgHbaConf, "ERROR: please specify file to read extra pg_hba configuration. Specify 'none' if nothig extra to be added.\n");
            add_datanodeMaster(name, host, atoi(port), atoi(pooler), dir,
                    walDir, extraConf, extraPgHbaConf);
            freeAndReset(name);
            freeAndReset(host);
            freeAndReset(port);
            freeAndReset(pooler);
            freeAndReset(dir);
            freeAndReset(walDir);
            freeAndReset(extraConf);
            freeAndReset(extraPgHbaConf);
        }
        else
        {
            GetAndSet(name, "ERROR: please specify the name of the datanode slave\n");
            GetAndSet(host, "ERROR: please specify the host for the datanode slave\n");
            GetAndSet(port, "ERROR: please specify the port number for the datanode slave\n");
            GetAndSet(pooler, "ERROR: please specify the pooler port number for the datanode slave.\n");
            GetAndSet(dir, "ERROR: please specify the working directory for datanode slave\n");
            GetAndSet(walDir, "ERROR: please specify the WAL directory for datanode slave WAL. Specify 'none' for default.\n");
            GetAndSet(archDir, "ERROR: please specify WAL archive directory for datanode slave\n");
            
            add_datanodeSlave(name, host, atoi(port), atoi(pooler), dir,
                    walDir, archDir);
            freeAndReset(name);
            freeAndReset(host);
            freeAndReset(port);
            freeAndReset(pooler);
            freeAndReset(dir);
            freeAndReset(walDir);
            freeAndReset(archDir);
        }
    }
    return;
}

static void do_remove_command(char *line)
{
    char *token;
    char *name;
    bool clean_opt = FALSE;

    if (!GetToken())
    {
        elog(ERROR, "ERROR: Please specify gtm, gtm_master, coordinator or datanode after add command.\n");
        return;
    }
    if (TestToken("gtm"))
    {
        if (!GetToken())
        {
            elog(ERROR, "ERROR: Specify option to remove gtm command\n");
            return;
        }
        if (TestToken("master"))
        {
            if (GetToken() && TestToken("clean"))
                clean_opt = TRUE;
            remove_gtmMaster(clean_opt);
        }
        else if (TestToken("slave"))
        {
            if (GetToken() && TestToken("clean"))
                clean_opt = TRUE;
            remove_gtmSlave(clean_opt);
        }
        else 
        {
            elog(ERROR, "ERROR: you can specify only master/slave to remove gtm command. %s is invalid.\n", token);
            return;
        }
    }
    else if (TestToken("gtm_proxy"))
    {
        GetAndSet(name, "ERROR: please specify gtm proxy name to remove.\n");
         if (GetToken() && TestToken("clean"))
            clean_opt = TRUE;
        remove_gtmProxy(name, clean_opt    );
        freeAndReset(name);
    }
    else if (TestToken("coordinator"))
    {
        if (!GetToken() || (!TestToken("master") && !TestToken("slave")))
        {
            elog(ERROR, "ERROR: please specify master or slave.\n");
            return;
        }
        if (TestToken("master"))
        {
            GetAndSet(name, "ERROR: please specify the name of the coordinator master\n");
            if (GetToken() && TestToken("clean"))
                clean_opt = TRUE;
            remove_coordinatorMaster(name, clean_opt);
            freeAndReset(name);
        }
        else
        {
            GetAndSet(name, "ERROR: please specify the name of the coordinator slave\n");
            if (GetToken() && TestToken("clean"))
                clean_opt = TRUE;
            remove_coordinatorSlave(name, clean_opt);
            freeAndReset(name);
        }
    }
    else if (TestToken("datanode"))
    {
        if (!GetToken() || (!TestToken("master") && !TestToken("slave")))
        {
            elog(ERROR, "ERROR: please specify master or slave.\n");
            return;
        }
        if (TestToken("master"))
        {
            GetAndSet(name, "ERROR: please specify the name of the datanode master\n");
            if (GetToken() && TestToken("clean"))
                clean_opt = TRUE;
            remove_datanodeMaster(name, clean_opt);
            freeAndReset(name);
        }
        else
        {
            GetAndSet(name, "ERROR: please specify the name of the datanode slave\n");
            if (GetToken() && TestToken("clean"))
                clean_opt = TRUE;
            remove_datanodeSlave(name, clean_opt);
            freeAndReset(name);
        }
    }
    else
        elog(ERROR, "ERROR: invalid argument %s to add command.\n", token);
    return;
}
        
static char *m_Option;

static char *handle_m_option(char *line, char **m_option)
{
    char *token;

    freeAndReset(m_Option);
    if (GetToken() == NULL)
        return(line);
    else if (TestToken("immediate"))
        m_Option = Strdup("immediate");
    else if (TestToken("fast"))
        m_Option = Strdup("fast");
    else if (TestToken("smart"))
        m_Option = Strdup("smart");
    else
        elog(ERROR, "ERROR: specify smart, fast or immediate for -m option value.\n");
    return(line);
}
        


static void do_stop_command(char *line)
{
    char *token;

    freeAndReset(m_Option);
    if (GetToken() == NULL)
        elog(ERROR, "ERROR: Please specify option to stop command.\n");
    else if (testToken("-m"))
    {
        line = handle_m_option(line, &m_Option);
        GetToken();
    }
    if (TestToken("all"))
    {
        if (GetToken() && TestToken("-m"))
            handle_m_option(line, &m_Option);
        stop_all(m_Option);
    }
    else if (TestToken("gtm"))
    {
        if (m_Option)
            elog(WARNING, "-m option is not available with gtm. Ignoring.\n");
        if (!GetToken() || (TestToken("all")))
        {
            if (!is_none(sval(VAR_gtmMasterServer)))
                stop_gtm_master();
            if (isVarYes(VAR_gtmSlave))
                stop_gtm_slave();
        }
        else if (TestToken("master") && !is_none(sval(VAR_gtmMasterServer)))
            stop_gtm_master();
        else if (TestToken("slave") && isVarYes(VAR_gtmSlave))
            stop_gtm_slave();
        else
            elog(ERROR, "ERROR: please specify master, slave or all for stop gtm command.\n");
    }
    else if (TestToken("gtm_proxy"))
    {
        if (m_Option)
            elog(WARNING, "-m option is not available with gtm_prxy. Ignoring.\n");
        if (!GetToken() || TestToken("all"))
            stop_gtm_proxy_all();
        else
        {
            char **nodeList = NULL;
            do
                AddMember(nodeList, token);
            while (GetToken());
            stop_gtm_proxy(nodeList);
            clean_array(nodeList);
        }
    }
    else if (TestToken("coordinator"))
    {
        if (!GetToken() || TestToken("all"))
        {
            stop_coordinator_master_all(m_Option);
            if (isVarYes(VAR_coordSlave))
                stop_coordinator_slave_all(m_Option);
        }
        else if (TestToken("master"))
            if (!GetToken() || TestToken("all"))
                stop_coordinator_master_all(m_Option);
            else
            {
                char **nodeList = NULL;
                do
                    AddMember(nodeList, token);
                while(GetToken());
                stop_coordinator_master(nodeList, m_Option);
                clean_array(nodeList);
            }
        else if (TestToken("slave"))
            if (!GetToken() || TestToken("all"))
                stop_coordinator_slave_all(m_Option);
            else
            {
                char **nodeList = NULL;
                do
                    AddMember(nodeList, token);
                while(GetToken());
                stop_coordinator_slave(nodeList, m_Option);
                clean_array(nodeList);
            }
        else
        {
            char **nodeList = NULL;
            do
                AddMember(nodeList, token);
            while(GetToken());
            stop_coordinator_master(nodeList, m_Option);
            if (isVarYes(VAR_coordSlave))
                stop_coordinator_slave(nodeList, m_Option);
            clean_array(nodeList);
        }
    }
    else if (TestToken("datanode"))
    {
        if (!GetToken() || TestToken("all"))
        {
            stop_datanode_master_all(m_Option);
            if (isVarYes(VAR_datanodeSlave))
                stop_datanode_slave_all(m_Option);
        }
        else if (TestToken("master"))
            if (!GetToken() || TestToken("all"))
                stop_datanode_master_all(m_Option);
            else
            {
                char **nodeList = Malloc0(sizeof(char *));
                do
                    AddMember(nodeList, token);
                while(GetToken());
                stop_datanode_master(nodeList, m_Option);
                clean_array(nodeList);
            }
        else if (TestToken("slave"))
            if (!GetToken() || TestToken("all"))
                stop_datanode_slave_all(m_Option);
            else
            {
                char **nodeList = Malloc0(sizeof(char *));
                do
                    AddMember(nodeList, token);
                while(GetToken());
                stop_datanode_slave(nodeList, m_Option);
                clean_array(nodeList);
            }
        else 
        {
            char **nodeList = Malloc0(sizeof(char *));
            do
                AddMember(nodeList, token);
            while(GetToken());
            stop_datanode_master(nodeList, m_Option);
            if (isVarYes(VAR_datanodeSlave))
                stop_datanode_slave(nodeList, m_Option);
            Free(nodeList);
        }
    }
    else
        elog(ERROR, "ERROR: invalid option for stop command.\n");
    return;
}

/*
 * Test staff
 */
static void do_test(char *line)
{
    char *token;
    int logLevel;
    int printLevel;

    logLevel = setLogMsgLevel(DEBUG3);
    printLevel = setPrintMsgLevel(DEBUG3);
    
    GetToken();
    if (TestToken("ssh") && (line != NULL))
    {
        cmdList_t *cmdList;
        cmd_t *cmd;

        GetToken();
        cmdList = initCmdList();
        cmd = Malloc0(sizeof(cmd_t));
        cmd->host = Strdup(token);
        cmd->command = Strdup(line);
        cmd->localStdin = NULL;
        addCmd(cmdList, cmd);
        elog(INFO, "INFO: Testing ssh %s \"%s\"\n", token, line);
        doCmdList(cmdList);
        cleanCmdList(cmdList);
    }
    else if (TestToken("ssh-stdin") && (line != NULL))
    {
        cmdList_t *cmdList;
        cmd_t *cmd;

        cmdList = initCmdList();
        cmd = Malloc0(sizeof(cmd_t));
        GetToken();
        cmd->host = Strdup(token);
        GetToken();
        cmd->localStdin = Strdup(token);
        cmd->command = Strdup(line);
        addCmd(cmdList, cmd);
        elog(INFO, "Testing ssh %s \"%s\" < %s\n", cmd->host, cmd->command, cmd->localStdin);
        doCmdList(cmdList);
        cleanCmdList(cmdList);
    }
    else if (TestToken("local") && (line != NULL))
    {
        cmdList_t *cmdList;
        cmd_t *cmd;

        cmdList = initCmdList();
        addCmd(cmdList, (cmd = initCmd(NULL)));
        cmd->command = Strdup(line);
        elog(INFO, "Testing local, \"%s\"\n", cmd->command);
        doCmdList(cmdList);
        cleanCmdList(cmdList);
    }
    else if (TestToken("local-stdin") && (line != NULL))
    {
        cmdList_t *cmdList;
        cmd_t *cmd;

        cmdList = initCmdList();
        addCmd(cmdList, (cmd = initCmd(NULL)));
        GetToken();
        cmd->localStdin = Strdup(token);
        cmd->command = Strdup(line);
        elog(INFO, "Testing local-stdin, \"%s\"\n", cmd->command);
        doCmdList(cmdList);
        cleanCmdList(cmdList);
    }
    setLogMsgLevel(logLevel);
    setPrintMsgLevel(printLevel);
}


/* ==================================================================
 *
 * Staff specified by "node name", not node type
 *
 * ==================================================================
 */
static void kill_something(char *nodeName)
{
    char *nodeList[2];

    nodeList[1] = NULL;
    switch(getNodeType(nodeName))
    {
        case NodeType_UNDEF:
            elog(ERROR, "ERROR: Could not find name \"%s\" in any node type.\n", nodeName);
            return;
        case NodeType_GTM:
            elog(ERROR, "ERROR: Issue kill gtm command to kill gtm master/slave\n");
            return;
        case NodeType_GTM_PROXY:
            nodeList[0] = nodeName;
            kill_gtm_proxy(nodeList);
            return;
        case NodeType_COORDINATOR:
            nodeList[0] = nodeName;
            kill_coordinator_master(nodeList);
            if (isVarYes(VAR_coordSlave))
                kill_coordinator_slave(nodeList);
            return;
        case NodeType_DATANODE:
            nodeList[0] = nodeName;
            kill_datanode_master(nodeList);
            if (isVarYes(VAR_datanodeSlave))
                kill_datanode_slave(nodeList);
            return;
        default:
            elog(ERROR, "ERROR: internal error.  Should not come here!\n");
            return;
    }
}

static void show_config_something_multi(char **nodeList)
{
    int ii;

    for (ii = 0; nodeList[ii]; ii++)
        show_config_something(nodeList[ii]);
}

static void show_config_something(char *nodeName)
{
    uint idx;

    switch(getNodeType(nodeName))
    {
        case NodeType_UNDEF:
            elog(ERROR, "ERROR: Could not find name \"%s\" in any node type.\n", nodeName);
            return;
        case NodeType_GTM:
            show_config_gtmMaster(TRUE, sval(VAR_gtmMasterServer));
            if (isVarYes(VAR_gtmSlave))
                show_config_gtmSlave(TRUE, sval(VAR_gtmSlaveServer));
            return;
        case NodeType_GTM_PROXY:
            idx = gtmProxyIdx(nodeName);
            show_config_gtmProxy(TRUE, idx, aval(VAR_gtmProxyServers)[idx]);
            return;
        case NodeType_COORDINATOR:
            idx = coordIdx(nodeName);
            show_config_coordMaster(TRUE, idx, aval(VAR_coordMasterServers)[idx]);
            if (isVarYes(VAR_coordSlave))
                show_config_coordSlave(TRUE, idx, aval(VAR_coordSlaveServers)[idx]);
            return;
        case NodeType_DATANODE:
            idx = datanodeIdx(nodeName);
            show_config_datanodeMaster(TRUE, idx, aval(VAR_datanodeMasterServers)[idx]);
            if (isVarYes(VAR_datanodeSlave))
                show_config_datanodeSlave(TRUE, idx, aval(VAR_datanodeSlaveServers)[idx]);
            return;
        case NodeType_SERVER:
        {
            char *hostList[2];
            hostList[0] = nodeName;
            hostList[1] = NULL;
            show_config_servers(hostList);
            return;
        }
        default:
            elog(ERROR, "ERROR: internal error.  Should not come here!\n");
            return;
    }
}



/* ========================================================================================
 *
 * Configuration staff
 *
 * ========================================================================================
 */
static void show_config_servers(char **hostList)
{
    int ii;
    for (ii = 0; hostList[ii]; ii++)
        if (!is_none(hostList[ii]))
            show_config_host(hostList[ii]);
    return;
}

/*
 * show {config|configuration} [all | name .... | gtm [master|slave|all] | gtm_proxy [all | name ...] |
 *                                coordinator [all | master | slave | name ... ] |
 *                                host name .... ]
 * With no option, will print common configuartion parameters and exit.
 *
 */
static void show_basicConfig(void)
{
    elog(NOTICE, "========= Postgres-XL configuration Common Info ========================\n");
    elog(NOTICE, "=== Overall ===\n");
    elog(NOTICE, "Postgres-XL owner: %s\n", sval(VAR_pgxcOwner));
    elog(NOTICE, "Postgres-XL user: %s\n", sval(VAR_pgxcUser));
    elog(NOTICE, "Postgres-XL install directory: %s\n", sval(VAR_pgxcInstallDir));
    elog(NOTICE, "pgxc_ctl home: %s\n", pgxc_ctl_home);
    elog(NOTICE, "pgxc_ctl configuration file: %s\n", pgxc_ctl_config_path);
    elog(NOTICE, "pgxc_ctl tmpDir: %s\n", sval(VAR_tmpDir));
    elog(NOTICE, "pgxc_ctl localTempDir: %s\n", sval(VAR_localTmpDir));
    elog(NOTICE, "pgxc_ctl log file: %s\n", logFileName);
    elog(NOTICE, "pgxc_ctl configBackup: %s\n", isVarYes(VAR_configBackup) ? "y" : "n");
    elog(NOTICE, "pgxc_ctl configBackupHost: %s\n", isVarYes(VAR_configBackup) ? sval(VAR_configBackupHost) : "none");
    elog(NOTICE, "pgxc_ctl configBackupFile: %s\n", isVarYes(VAR_configBackup) ? sval(VAR_configBackupFile) : "none");
    elog(NOTICE, "========= Postgres-XL configuration End Common Info ===================\n");
}


static void show_configuration(char *line)
{
    char *token;

    GetToken();
    if (line == NULL)
        elog(ERROR, "ERROR: No configuration option is specified. Retruning.\n");
    else if (TestToken("basic"))
        show_basicConfig();
    else if (TestToken("all"))
    {
        show_basicConfig();
        show_config_servers(aval(VAR_allServers));
    }
    else if (TestToken("basic"))
    {
        show_basicConfig();
    }
    else if (TestToken("host"))
    {
        char **hostList = Malloc0(sizeof(char *));
        do {
            AddMember(hostList, token);
        } while(GetToken());
        if (hostList[0])
            show_config_servers(hostList);
        clean_array(hostList);
    }
    else if (TestToken("gtm"))
    {
        if ((GetToken() == NULL) || (TestToken("all")))
        {
            show_config_gtmMaster(TRUE, sval(VAR_gtmMasterServer));
            if (isVarYes(VAR_gtmSlave))
                show_config_gtmSlave(TRUE, sval(VAR_gtmSlaveServer));
        }
        else if (TestToken("master"))
            show_config_gtmMaster(TRUE, sval(VAR_gtmMasterServer));
        else if (TestToken("slave"))
        {
            if (isVarYes(VAR_gtmSlave))
                show_config_gtmSlave(TRUE, sval(VAR_gtmSlaveServer));
            else
                elog(NOTICE, "NOTICE: gtm slave is not configured.\n");
        }
        else
            elog(ERROR, "ERROR: invalid option %s for 'show config gtm' command.\n", token);
    }
    else if (TestToken("gtm_proxy"))
    {
        if (!isVarYes(VAR_gtmProxy))
        {
            elog(ERROR, "ERROR: gtm proxies are not configured.\n");
        }
        else if ((GetToken() == NULL) || (TestToken("all")))
            show_config_gtmProxies(aval(VAR_gtmProxyNames));
        else 
        {
            char **nodeList = Malloc0(sizeof(char *));
            do{
                int idx;
                idx = gtmProxyIdx(token);
                if (idx < 0)
                    elog(ERROR, "ERROR: Specified name %s is not GTM Proxy.\n", token);
                else
                    AddMember(nodeList, token);
            } while(GetToken());
            show_config_gtmProxies(nodeList);
            clean_array(nodeList);
        }
    }
    else if (TestToken("coordinator"))
    {
        if ((GetToken() == NULL) || (TestToken("all")))
            show_config_coordMasterSlaveMulti(aval(VAR_coordNames));
        else if (TestToken("master"))
        {
            if (GetToken() == NULL)
                show_config_coordMasterMulti(aval(VAR_coordNames));
            else
            {
                char **nodeList = Malloc0(sizeof(char *));
                do
                    AddMember(nodeList, token);
                while(GetToken());
                show_config_coordMasterMulti(nodeList);
                clean_array(nodeList);
            }
        }
        else if (TestToken("slave"))
        {
            if (!isVarYes(VAR_coordSlave))
                elog(ERROR, "ERROR: Coordinator slave is not configured.\n");
            else if (GetToken() == NULL)
                show_config_coordMasterMulti(aval(VAR_coordNames));
            else
            {
                char **nodeList = Malloc0(sizeof(char *));
                do
                    AddMember(nodeList, token);
                while(GetToken());
                show_config_coordMasterMulti(nodeList);
                clean_array(nodeList);
            }
        }
        else
            elog(ERROR, "ERROR: Invalid option %s for 'show config coordinator' command.\n", token);
    }
    else if (TestToken("datanode"))
    {
        if ((GetToken() == NULL) || (TestToken("all")))
            show_config_datanodeMasterSlaveMulti(aval(VAR_datanodeNames));
        else if (TestToken("master"))
        {
            if (GetToken() == NULL)
                show_config_datanodeMasterMulti(aval(VAR_datanodeNames));
            else
            {
                char **nodeList = Malloc0(sizeof(char *));
                do
                    AddMember(nodeList, token);
                while(GetToken());
                show_config_datanodeMasterMulti(nodeList);
                clean_array(nodeList);
            }
        }
        else if (TestToken("slave"))
        {
            if (!isVarYes(VAR_datanodeSlave))
                elog(ERROR, "ERROR: Datanode slave is not configured.\n");
            else if (GetToken() == NULL)
                show_config_datanodeMasterMulti(aval(VAR_datanodeNames));
            else
            {
                char **nodeList = Malloc0(sizeof(char *));
                do
                    AddMember(nodeList, token);
                while(GetToken());
                show_config_datanodeMasterMulti(nodeList);
                clean_array(nodeList);
            }
        }
        else
            elog(ERROR, "ERROR: Invalid option %s for 'show config datanode' command.\n", token);
    }
    else
    {
        char **nodeList = NULL;
        do
            AddMember(nodeList, token);
        while(GetToken());
        show_config_something_multi(nodeList);
        clean_array(nodeList);
    }
    return;
}

void print_simple_node_info(char *nodeName, char *port, char *dir,
                            char *extraConfig, char *specificExtraConfig)
{
    elog(NOTICE, 
         "    Nodename: '%s', port: %s, dir: '%s'"
         "    ExtraConfig: '%s', Specific Extra Config: '%s'\n",
         nodeName, port, dir, extraConfig, specificExtraConfig);

}


static void show_config_host(char *hostname)
{
    uint ii;

    lockLogFile();
    elog(NOTICE, "====== Server: %s =======\n", hostname);
    /* GTM Master */
    if (strcmp(hostname, sval(VAR_gtmMasterServer)) == 0)
        show_config_gtmMaster(TRUE, NULL);
    /* GTM Slave */
    if (isVarYes(VAR_gtmSlave) && (strcmp(sval(VAR_gtmSlaveServer), hostname) == 0))
        show_config_gtmSlave(TRUE, NULL);
    /* GTM Proxy */
    if (isVarYes(VAR_gtmProxy))
        for (ii = 0; aval(VAR_gtmProxyServers)[ii]; ii++)
            if (strcmp(aval(VAR_gtmProxyServers)[ii], hostname) == 0)
                show_config_gtmProxy(TRUE, ii, NULL);
    /* Coordinator Master */
    for (ii = 0; aval(VAR_coordMasterServers)[ii]; ii++)
        if (strcmp(aval(VAR_coordMasterServers)[ii], hostname) == 0)
            show_config_coordMaster(TRUE, ii, NULL);
    /* Coordinator Slave */
    if (isVarYes(VAR_coordSlave))
        for (ii = 0; aval(VAR_coordSlaveServers)[ii]; ii++)
            if (strcmp(aval(VAR_coordSlaveServers)[ii], hostname) == 0)
                show_config_coordSlave(TRUE, ii, NULL);
    /* Datanode Master */
    for (ii = 0; aval(VAR_datanodeMasterServers)[ii]; ii++)
        if (strcmp(aval(VAR_datanodeMasterServers)[ii], hostname) == 0)
            show_config_datanodeMaster(TRUE, ii, NULL);
    /* Datanode Slave */
    if (isVarYes(VAR_datanodeSlave))
        for (ii = 0; aval(VAR_datanodeSlaveServers)[ii]; ii++)
            if (strcmp(aval(VAR_datanodeSlaveServers)[ii], hostname) == 0)
                show_config_datanodeSlave(TRUE, ii, NULL);
    unlockLogFile();
}

void show_config_hostList(char **hostList)
{
    int ii;
    for (ii = 0; hostList[ii]; ii++)
        show_config_host(hostList[ii]);
}
/*
 * Clean command
 *
 * clean {all | 
 *        gtm [ all | master | slave ] |
 *          gtm_proxy [ all | nodename ... ]
 *          coordinator [[all | master | slave ] [nodename ... ]] |
 *        datanode [ [all | master | slave] [nodename ... ]}
 */
static void do_clean_command(char *line)
{
    char *token;
    cmdList_t *cmdList = NULL;

    GetToken();
    if (token == NULL)
    {
        elog(ERROR, "ERROR: Please specify options for clean command.\n");
        return;
    }
    if (TestToken("all"))
    {
        elog(INFO, "Stopping all components before cleaning\n");
        stop_all("immediate");

        elog(INFO, "Cleaning all the directories and sockets.\n");
        if (!is_none(sval(VAR_gtmMasterServer)))
            clean_gtm_master();
        if (isVarYes(VAR_gtmSlave))
            clean_gtm_slave();
        if (isVarYes(VAR_gtmProxy))
            clean_gtm_proxy_all();
        clean_coordinator_master_all();
        if (isVarYes(VAR_coordSlave))
            clean_coordinator_slave_all();
        clean_datanode_master_all();
        if (isVarYes(VAR_datanodeSlave))
            clean_datanode_slave_all();
    }
    else if (TestToken("gtm"))
    {
        GetToken();
        if ((token == NULL) || TestToken("all"))
        {
            elog(INFO, "Stopping and cleaning GTM slave/master \n");
            if (!is_none(sval(VAR_gtmMasterServer)))
                stop_gtm_master();
            if (isVarYes(VAR_gtmSlave))
                stop_gtm_slave();

            if (!is_none(sval(VAR_gtmMasterServer)))
                clean_gtm_master();
            if (isVarYes(VAR_gtmSlave))
                clean_gtm_slave();
        }
        else if (TestToken("master") && !is_none(sval(VAR_gtmMasterServer)))
        {
            stop_gtm_master();
            clean_gtm_master();
        }
        else if (TestToken("slave"))
        {
            if (isVarYes(VAR_gtmSlave))
            {
                stop_gtm_slave();
                clean_gtm_slave();
            }
            else
                elog(ERROR, "ERROR: gtm slave is not configured.\n");
        }
        else
            elog(ERROR, "ERROR: invalid clean command option %s.\n", token);
    }
    else if (TestToken("gtm_proxy"))
    {
        elog(INFO, "Stopping and cleaning specified gtm_proxy.\n");
        GetToken();
        if (!isVarYes(VAR_gtmProxy))
            elog(ERROR, "ERROR: gtm proxy is not configured.\n");
        else if ((token == NULL) || TestToken("all"))
        {
            stop_gtm_proxy_all();
            clean_gtm_proxy_all();
        }
        else
        {
            char **nodeList = NULL;
            do
                AddMember(nodeList, token);
            while(GetToken());

            stop_gtm_proxy(nodeList);
            clean_gtm_proxy(nodeList);
            CleanArray(nodeList);
        }
    }
    else if (TestToken("coordinator"))
    {
        GetToken();
        if (token == NULL)
        {
            elog(INFO, "Stopping and cleaning coordinator master and slave.\n");
            stop_coordinator_master_all("immediate");
            if (isVarYes(VAR_coordSlave))
                stop_coordinator_slave_all("immediate");

            clean_coordinator_master_all();
            if (isVarYes(VAR_coordSlave))
                clean_coordinator_slave_all();
        }
        else if (TestToken("all"))
        {
            elog(INFO, "Stopping and cleaning coordinator master and slave.\n");
            GetToken();
            if (token == NULL)
            {
                stop_coordinator_master_all("immediate");
                clean_coordinator_master_all();
                if (isVarYes(VAR_coordSlave))
                {
                    stop_coordinator_slave_all("immediate");
                    clean_coordinator_slave_all();
                }
            }
            else
            {
                char **nodeList = NULL;
                do
                    AddMember(nodeList, token);
                while(GetToken());
                stop_coordinator_master(nodeList, "immediate");
                clean_coordinator_master(nodeList);
                if (isVarYes(VAR_coordSlave))
                {
                    stop_coordinator_slave(nodeList,"immediate");
                    clean_coordinator_slave(nodeList);
                }
                CleanArray(nodeList);
            }
        }
        else if (TestToken("master"))
        {
            elog(INFO, "Stopping and cleaning specified coordinator master.\n");
            GetToken();
            if (token == NULL)
            {
                stop_coordinator_master_all("immediate");
                clean_coordinator_master_all();
            }
            else
            {
                char **nodeList = NULL;
                do
                    AddMember(nodeList, token);
                while (GetToken());
                stop_coordinator_master(nodeList, "immediate");
                clean_coordinator_master(nodeList);
                CleanArray(nodeList);
            }
        }
        else if (TestToken("slave"))
        {
            elog(INFO, "Stopping and cleaning specified coordinator slave.\n");
            if (!isVarYes(VAR_coordSlave))
            {
                elog(ERROR, "ERROR: Coordinator slave is not configured.\n");
                return;
            }
            GetToken();
            if (token == NULL)
            {
                stop_coordinator_slave_all("immediate");
                clean_coordinator_slave_all();
            }
            else
            {
                char **nodeList = NULL;
                do
                    AddMember(nodeList, token);
                while (GetToken());
                stop_coordinator_slave(nodeList, "immediate");
                clean_coordinator_slave(nodeList);
                CleanArray(nodeList);
            }
        }
        else
        {
            char **nodeList = NULL;
            elog(INFO, "Stopping and cleaning specified coordinator.\n");
            do
                AddMember(nodeList, token);
            while (GetToken());
            stop_coordinator_master(nodeList, "immediate");
            clean_coordinator_master(nodeList);
            if (isVarYes(VAR_coordSlave))
            {
                stop_coordinator_slave(nodeList, "immediate");
                clean_coordinator_slave(nodeList);
            }
            CleanArray(nodeList);
        }
    }
    else if(TestToken("datanode"))
    {
        GetToken();
        if (token == NULL)
        {
            elog(INFO, "Stopping and cleaning all the datanodes.\n");
            stop_datanode_master_all("immediate");
            clean_datanode_master_all();
            if (isVarYes(VAR_datanodeSlave))
            {
                stop_datanode_slave_all("immediate");
                clean_datanode_slave_all();
            }
        }
        else if (TestToken("all"))
        {
            GetToken();
            if (token == NULL)
            {
                elog(INFO, "Stopping and cleaning all the datanodes.\n");
                stop_datanode_master_all("immediate");
                clean_datanode_master_all();
                if (isVarYes(VAR_datanodeSlave))
                {
                    stop_datanode_slave_all("immediate");
                    clean_datanode_slave_all();
                }
            }
            else
            {
                char **nodeList = NULL;
                elog(INFO, "Stopping and cleaning specified datanodes\n");
                do
                    AddMember(nodeList, token);
                while(GetToken());
                stop_datanode_master(nodeList, "immediate");
                clean_datanode_master(nodeList);
                if (isVarYes(VAR_datanodeSlave))
                {
                    stop_datanode_slave(nodeList, "immediate");
                    clean_datanode_slave(nodeList);
                }
                Free(nodeList);
            }
        }
        else if (TestToken("master"))
        {
            GetToken();
            if (token == NULL)
            {
                elog(INFO, "Stopping and cleaning all the datanode masters.\n");
                stop_datanode_master_all("immediate");
                clean_datanode_master_all();
            }
            else
            {
                char **nodeList = NULL;
                elog(INFO, "Stopping and cleaning specified datanode masters.\n");
                do
                    AddMember(nodeList, token);
                while (GetToken());
                stop_datanode_master(nodeList, "immediate");
                clean_datanode_master(nodeList);
                CleanArray(nodeList);
            }
        }
        else if (TestToken("slave"))
        {
            elog(INFO, "Stopping and cleaning specified datanode slaves.\n");
            if (!isVarYes(VAR_datanodeSlave))
            {
                elog(ERROR, "ERROR: Datanode slave is not configured.\n");
                return;
            }
            GetToken();
            if (token == NULL)
            {
                stop_datanode_slave_all("immediate");
                clean_datanode_slave_all();
            }
            else
            {
                char **nodeList = NULL;
                do
                    AddMember(nodeList, token);
                while (GetToken());
                stop_datanode_slave(nodeList, "immediate");
                clean_datanode_slave(nodeList);
                CleanArray(nodeList);
            }
        }
        else
        {
            char **nodeList = NULL;
            do
                AddMember(nodeList, token);
            while (GetToken());
            stop_datanode_master(nodeList, "immediate");
            clean_datanode_master(nodeList);
            if (isVarYes(VAR_datanodeSlave))
            {
                stop_datanode_slave(nodeList, "immediate");
                clean_datanode_slave(nodeList);
            }
            CleanArray(nodeList);
        }
    }
    else
    {
        elog(INFO, "Stopping and cleaning specifieid nodes.\n");
        do
        {
            switch(getNodeType(token))
            {
                case NodeType_UNDEF:
                    elog(ERROR, "ERROR: %s is not found, skipping\n", token);
                    continue;
                case NodeType_GTM:
                    elog(INFO, "Stopping and cleaning GTM master.\n");
                    if (cmdList == NULL)
                        cmdList = initCmdList();
                    addCmd(cmdList, prepare_stopGtmMaster());
                    addCmd(cmdList, prepare_cleanGtmMaster());
                    if (isVarYes(VAR_gtmSlave))
                    {
                        elog(INFO, "Stopping and cleaning GTM slave.\n");
                        addCmd(cmdList, prepare_stopGtmSlave());
                        addCmd(cmdList, prepare_cleanGtmSlave());
                    }
                    continue;
                case NodeType_GTM_PROXY:
                    elog(INFO, "Stopping and cleaning GTM proxy %s.\n", token);
                    if (cmdList == NULL)
                        cmdList = initCmdList();
                    addCmd(cmdList, prepare_stopGtmProxy(token));
                    addCmd(cmdList, prepare_cleanGtmProxy(token));
                    continue;
                case NodeType_COORDINATOR:
                    elog(INFO, "Stopping and cleaning coordinator master %s\n", token);
                    if (cmdList == NULL)
                        cmdList = initCmdList();
                    addCmd(cmdList, prepare_stopCoordinatorMaster(token, "immediate"));
                    addCmd(cmdList, prepare_cleanCoordinatorMaster(token));
                    if (isVarYes(VAR_coordSlave))
                    {
                        elog(INFO, "Stopping and cleaning coordinator slave %s\n", token);
                        addCmd(cmdList, prepare_stopCoordinatorSlave(token, "immediate"));
                        addCmd(cmdList, prepare_cleanCoordinatorSlave(token));
                    }
                    continue;
                case NodeType_DATANODE:
                    elog(INFO, "Stopping and cleaning datanode master %s\n", token);
                    if (cmdList == NULL)
                        cmdList = initCmdList();
                    addCmd(cmdList, prepare_stopDatanodeMaster(token, "immediate"));
                    addCmd(cmdList, prepare_cleanDatanodeMaster(token));
                    if (isVarYes(VAR_coordSlave))
                    {
                        elog(INFO, "Stopping and cleaning datanode slave %s\n", token);
                        addCmd(cmdList, prepare_stopDatanodeSlave(token, "immediate"));
                        addCmd(cmdList, prepare_cleanDatanodeSlave(token));
                    }
                    continue;
                case NodeType_SERVER:
                    elog(ERROR, "ERROR: clearing host is not supported yet. Skipping\n");
                    continue;
                default:
                    elog(ERROR, "ERROR: internal error.\n");
                    continue;
            } 
        } while(GetToken());
        if (cmdList)
        {
            int rc;
            rc = doCmdList(cmdList);
            cleanCmdList(cmdList);
            elog(INFO, "rc=%d Done.\n", rc);
        }
    return;
    }
}

static void do_configure_command(char *line)
{
    char *token;
    char **nodeList = NULL;

    if (!GetToken() || TestToken("all"))
    {
        configure_nodes_all();
    }
    else
    {
        bool is_datanode;

        if (TestToken("datanode"))
            is_datanode = true;
        else if (TestToken("coordinator"))
            is_datanode = false;
        else
        {
            elog(ERROR, "ERROR: must specify either coordinator or datanode\n");
            return;
        }

        while (GetToken())
            AddMember(nodeList, token);
        
        if (is_datanode)
            configure_datanodes(nodeList);
        else
            configure_nodes(nodeList);

        CleanArray(nodeList);
    }
}
    
static int selectCoordinator(void)
{
    int sz = arraySizeName(VAR_coordNames);
    int i;

    for (;;)
    {
        i = rand() % sz;
        if (is_none(aval(VAR_coordMasterServers)[i]))
            continue;
        else
            return i;
    }
    return -1;
}


static int show_Resource(char *datanodeName, char *databasename, char *username)
{
    int cdIdx = selectCoordinator();
    int dnIdx = datanodeIdx(datanodeName);
    FILE *f;
    char queryFname[MAXPATH+1];

    elog(NOTICE, "NOTICE: showing tables in the datanode '%s', database %s, user %s\n",
         datanodeName, 
         databasename ? databasename : "NULL",
         username ? username : "NULL");
    if (dnIdx < 0)
    {
        elog(ERROR, "ERROR: %s is not a datanode.\n", datanodeName);
        return 1;
    }
    createLocalFileName(GENERAL, queryFname, MAXPATH);
    if ((f = fopen(queryFname, "w")) == NULL)
    {
        elog(ERROR, "ERROR: Could not create temporary file %s, %s\n", queryFname, strerror(errno));
        return 1;
    }
    fprintf(f,
            "SELECT pg_class.relname relation,\n"
            "           CASE\n"
            "             WHEN pclocatortype = 'H' THEN 'Hash'\n"
            "             WHEN pclocatortype = 'M' THEN 'Modulo'\n"
            "             WHEN pclocatortype = 'N' THEN 'Round Robin'\n"
            "             WHEN pclocatortype = 'R' THEN 'Replicate'\n"
            "             ELSE 'Unknown'\n"
            "           END AS distribution,\n"
            "            pg_attribute.attname attname,\n"
            "            pgxc_node.node_name nodename\n"
            "        FROM pg_class, pgxc_class, pg_attribute, pgxc_node\n"
            "        WHERE pg_class.oid = pgxc_class.pcrelid\n"
            "              and pg_class.oid = pg_attribute.attrelid\n"
            "              and pgxc_class.pcattnum = pg_attribute.attnum\n"
            "              and pgxc_node.node_name = '%s'\n"
            "              and pgxc_node.oid = ANY (pgxc_class.nodeoids)\n"
            "    UNION\n"
            "    SELECT pg_class.relname relation,\n"
            "          CASE\n"
            "            WHEN pclocatortype = 'H' THEN 'Hash'\n"
            "            WHEN pclocatortype = 'M' THEN 'Modulo'\n"
            "            WHEN pclocatortype = 'N' THEN 'Round Robin'\n"
            "            WHEN pclocatortype = 'R' THEN 'Replicate'\n"
            "            ELSE 'Unknown'\n"
            "          END AS distribution,\n"
            "           '- none -' attname,\n"
            "           pgxc_node.node_name nodename\n"
            "       FROM pg_class, pgxc_class, pg_attribute, pgxc_node\n"
            "       WHERE pg_class.oid = pgxc_class.pcrelid\n"
            "             and pg_class.oid = pg_attribute.attrelid\n"
            "             and pgxc_class.pcattnum = 0\n"
            "             and pgxc_node.node_name = '%s'\n"
            "             and pgxc_node.oid = ANY (pgxc_class.nodeoids)\n"
            "             ;\n",
            datanodeName, datanodeName);
    fclose(f);
    if (databasename == NULL)
        doImmediateRaw("psql -p %d -h %s --quiet -f %s",
                       atoi(aval(VAR_coordPorts)[cdIdx]), aval(VAR_coordMasterServers)[cdIdx],
                       queryFname);
    else if (username == NULL)
        doImmediateRaw("psql -p %d -h %s --quiet -f %s -d %s",
                       atoi(aval(VAR_coordPorts)[cdIdx]), aval(VAR_coordMasterServers)[cdIdx],
                       queryFname, databasename);
    else
        doImmediateRaw("psql -p %d -h %s --quiet -f %s -d %s -U %s",
                       atoi(aval(VAR_coordPorts)[cdIdx]), aval(VAR_coordMasterServers)[cdIdx],
                       queryFname, databasename, username);
    doImmediateRaw("rm -f %s", queryFname);
    return 0;
}

/*
 * =======================================================================================
 *
 * Loop of main command processor
 *
 * ======================================================================================
 */
void do_command(FILE *inf, FILE *outf)
{
    int istty = ((inf == stdin) && isatty(fileno(stdin)));
    int interactive = ((inf == stdin) && (outf == stdout));
    char *wkline = NULL;
    char buf[MAXLINE+1];
    int rc;
    char histfile[MAXPATH + 20];

#define HISTFILE    ".pgxc_ctl_history"
    
    histfile[0] = '\0';
    if (pgxc_ctl_home[0] != '\0')
    {
        snprintf(histfile, MAXPATH + 20, "%s/%s", pgxc_ctl_home, HISTFILE);
        read_history(histfile);
    }

    /*
     * Set the long jump path so that we can come out straight here in case of
     * an error. There is not much to reinitialize except may be freeing up the
     * wkline buffer and resetting the long jump buffer pointer. But if
     * anything else needs to reset, that should happen in the following block
     */
    if (setjmp(dcJmpBufMainLoop) != 0)
    {
        whereToJumpMainLoop = NULL;
        if (wkline)
            freeAndReset(wkline);
    }

    for (;;)
    {
        if (wkline)
            free(wkline);
        if (istty)
        {
            wkline = readline(sval(VAR_xc_prompt));
            if (wkline == NULL)
            {
                wkline = Strdup("q\n");
                putchar('\n');
            }
            else if (wkline[0] != '\0')
                add_history(wkline);
            strncpy(buf, wkline, MAXLINE);
        }
        else
        {
            if (interactive)
                fputs(sval(VAR_xc_prompt), stdout);
            if (fgets(buf, MAXLINE+1, inf) == NULL)
                break;
        }
        trimNl(buf);
        writeLogOnly("PGXC %s\n", buf);

        whereToJumpMainLoop = &dcJmpBufMainLoop;
        rc = do_singleLine(buf, wkline);
        whereToJumpMainLoop = NULL;

        freeAndReset(wkline);
        if (rc)    /* "q" command was found */
        {
            if (histfile[0] != '\0')
                write_history(histfile);
            return;
        }
    }
}



/*
 * ---------------------------------------------------------------------------
 *
 * Single line command processor
 *
 * -----------------------------------------------------------------------------
 */
int do_singleLine(char *buf, char *wkline)
{
    char *token;
    char *line = buf;
    GetToken();
    /*
     * Parsecommand
     */
    if (!token) return 0;
    if (TestToken("q") || TestToken("quit") || TestToken("exit"))
        /* Exit command */
        return 1;
    else if (TestToken("echo"))
    {
        do_echo_command(line);
        return 0;
    }
    else if (TestToken("deploy"))
    {
        do_deploy(line);
        return 0;
    }
    else if (TestToken("prepare"))
    {
        char *config_path = NULL;
        ConfigType    config_type = CONFIG_COMPLETE;

        if (GetToken() != NULL)
        {
            if (TestToken("config"))
                GetToken();

            if (TestToken("empty"))
                config_type = CONFIG_EMPTY;
            else if (TestToken("minimal"))
                config_type = CONFIG_MINIMAL;
            else if (TestToken("complete"))
                config_type = CONFIG_COMPLETE;
            else if (token)
                config_path = strdup(token);

            if (GetToken() != NULL)
                config_path = strdup(token);
        }

        do_prepareConfFile(config_path, config_type);
        free(config_path);
        return 0;
    }
    else if (TestToken("kill"))
    {
        do_kill_command(line);
        return 0;
    }
    else if (TestToken("init"))
    {
        do_init_command(line);
        return 0;
    }
    else if (TestToken("start"))
    {
        do_start_command(line);
        return 0;
    }
    else if (TestToken("stop"))
    {
        do_stop_command(line);
        return 0;
    }
    else if (TestToken("monitor"))
    {
        do_monitor_command(line);
        return 0;
    }
    else if (TestToken("failover"))
    {
        do_failover_command(line);
        return 0;
    }
    else if (TestToken("reconnect"))
    {
        do_reconnect_command(line);
        return 0;
    }
    else if (TestToken("add"))
    {
        do_add_command(line);
        return 0;
    }
    else if (TestToken("remove"))
    {
        do_remove_command(line);
        return 0;
    }
    /*
     * Show commnand ... show [variable | var] varname ...
     *                     show [variable | var] all
     *                     show config[uration] ....
     */
    else if (TestToken("show"))
    {
        if (GetToken() == NULL)
            elog(ERROR, "ERROR: Please specify what to show\n");
        else
        {
            if (TestToken("variable") || TestToken("var"))
            {
                /* Variable */
                if (GetToken() == NULL)
                    elog(ERROR, "ERROR: Please specify variable name to print\n");
                else if (TestToken("all"))
                    print_vars();
                else while (line)
                {
                    print_var(token);
                    GetToken();
                }
            }
            else if (TestToken("configuration") || TestToken("config") || TestToken("configure"))
                /* Configuration */
                show_configuration(line);
            else if (TestToken("resource"))
            {
                if ((GetToken() == NULL) || !TestToken("datanode"))
                    elog(ERROR, "ERROR: please specify datanode for show resource command.\n");
                else
                {
                    char *datanodeName = NULL;
                    char *dbname = NULL;
                    char *username = NULL;
                    if (GetToken() == NULL)
                        elog(ERROR, "ERROR: please specify datanode name\n");
                    else
                    {
                        datanodeName = Strdup(token);
                        if (GetToken())
                        {
                            dbname = Strdup(token);
                            if (GetToken())
                                username = Strdup(token);
                        }
                        show_Resource(datanodeName, dbname, username);
                        Free(datanodeName);
                        Free(dbname);
                        Free(username);
                    }
                }
            }
            else
                elog(ERROR, "ERROR: Cannot show %s now, sorry.\n", token);
        }
        return 0;
    }
    /*
     * Log command    log variable varname ...
     *                log variable all
     *                log msg artitrary_message_to_the_end_of_the_line
     */
    else if (TestToken("log"))
    {
        if (GetToken() == NULL)
            elog(ERROR, "ERROR: Please specify what to log\n");
        else
        {
            if (TestToken("variable") || TestToken("var"))
            {
                if (GetToken() == NULL)
                    elog(ERROR, "ERROR: Please specify variable name to log\n");
                else if (TestToken("all"))
                    print_vars();
                else while (line)
                {
                    print_var(token);
                    GetToken();
                }
                fflush(logFile);
            }
            else if (TestToken("msg") || TestToken("message"))
                writeLogOnly("USERLOG: \"%s\"\n", line);
            else
                elog(ERROR, "ERROR: Cannot log %s in this version.\n", token);
        }
        return 0;
    }
    else if (TestToken("deploy"))
    {
        do_deploy(line);
        return 0;
    }
    else if (TestToken("configure"))
    {
        do_configure_command(line);
        return 0;
    }
    else if (testToken("Psql"))
    {
        int idx;
        char *cmdLine;
        
        cmdLine = Strdup(line);
        if (GetToken() && TestToken("-"))
        {
            if (!GetToken())
                elog(ERROR, "ERROR: Please specify coordinator name after '-'.\n");
            else if ((idx = coordIdx(token)) < 0)
                elog(ERROR, "ERROR: Specified node %s is not a coordinator.\n", token);
            else
                doImmediateRaw("psql -p %d -h %s %s",
                               atoi(aval(VAR_coordPorts)[idx]),
                               aval(VAR_coordMasterServers)[idx],
                               line);
        }
        else
        {
            idx = selectCoordinator();
            elog(INFO, "Selected %s.\n", aval(VAR_coordNames)[idx]);
            doImmediateRaw("psql -p %d -h %s %s",
                           atoi(aval(VAR_coordPorts)[idx]),
                           aval(VAR_coordMasterServers)[idx],
                           cmdLine);
        }
        Free(cmdLine);
        return 0;
    }
    else if (testToken("Createdb"))
    {
        int idx;
        char *cmdLine;
        
        cmdLine = Strdup(line);
        if (GetToken() && TestToken("-"))
        {
            if (!GetToken())
                elog(ERROR, "ERROR: Please specify coordinator name after '-'.\n");
            else if ((idx = coordIdx(token)) < 0)
                elog(ERROR, "ERROR: Specified node %s is not a coordinator.\n", token);
            else
                doImmediateRaw("createdb -p %d -h %s %s",
                               atoi(aval(VAR_coordPorts)[idx]),
                               aval(VAR_coordMasterServers)[idx],
                               line);
        }
        else
        {
            idx = selectCoordinator();
            elog(INFO, "Selected %s.\n", aval(VAR_coordNames)[idx]);
            doImmediateRaw("createdb -p %d -h %s %s",
                           atoi(aval(VAR_coordPorts)[idx]),
                           aval(VAR_coordMasterServers)[idx],
                           cmdLine);
        }
        Free(cmdLine);
        return 0;
    }
    else if (testToken("Createuser"))
    {
        int idx;
        char *cmdLine;
        
        cmdLine = Strdup(line);
        if (GetToken() && TestToken("-"))
        {
            if (!GetToken())
                elog(ERROR, "ERROR: Please specify coordinator name after '-'.\n");
            else if ((idx = coordIdx(token)) < 0)
                elog(ERROR, "ERROR: Specified node %s is not a coordinator.\n", token);
            else
                doImmediateRaw("createuser -p %d -h %s %s",
                               atoi(aval(VAR_coordPorts)[idx]),
                               aval(VAR_coordMasterServers)[idx],
                               line);
        }
        else
        {
            idx = selectCoordinator();
            elog(INFO, "Selected %s.\n", aval(VAR_coordNames)[idx]);
            doImmediateRaw("createuser -p %d -h %s %s",
                           atoi(aval(VAR_coordPorts)[idx]),
                           aval(VAR_coordMasterServers)[idx],
                           cmdLine);
        }
        Free(cmdLine);
        return 0;
    }
    else if (TestToken("test"))
    {
        do_test(line);
        return 0;
    }
    else if (TestToken("set"))
    {
        do_set(line);
        return 0;
    }
    /*
     * Clean command
     *
     * clean [all | 
     *        gtm [ all | master | slave ] |
     *          gtm_proxy [ all | nodename ... ]
     *          coordinator [[all | master | slave ] [nodename ... ]] |
     *        datanode [ [all | master | slave] [nodename ... ]
     */
    else if (TestToken("clean"))
    {
        do_clean_command(line);
    }
    else if (TestToken("cd"))
    {
        /*
         * CD command
         */
        if (GetToken() == NULL)
            Chdir(pgxc_ctl_home, FALSE);
        else
            Chdir(token, FALSE);
        return 0;
    }
    else if (TestToken("ssh"))
    {
        doImmediateRaw("%s", wkline);
    }
    else if (TestToken("help"))
    {
        do_show_help(line);
    }
    else
    {
        doImmediateRaw("%s", wkline);
        return 0;
    }
    return 0;
}

static void
show_all_help()
{
    printf("You are using pgxc_ctl, the configuration utility for PGXL\n"
           "Type:\n"
           "    help <command>\n"
           "    where <command> is either add, Createdb, Createuser, clean,\n"
           "        configure, deploy, failover, init, kill, log, monitor,\n"
           "        prepare, q, reconnect, remove, set, show, start, \n"
           "        stop or unregister\n");
}

static void
do_show_help(char *line)
{
    char *token;

    GetToken();
    if ((token == NULL) || TestToken("all"))
    {
        show_all_help();
        return;
    }

    if (TestToken("add"))
    {
        printf(
                "\n"
                "add gtm slave slave_name host port dir\n"
                "add gtm_proxy name host port dir\n"
                "add coordinator master name host port pooler dir extra_conf extra_pghba\n"
                "add coordinator slave name host port pooler dir archDir\n"
                "add datanode master name host port pooler dir xlogdir restore_datanode_name extra_conf extra_pghba\n"
                "add datanode slave name host port pooler dir xlogdir archDir\n"
                "\n"
                "Add the specified node to your postgres-xl cluster:\n"
                "For more details, please see the pgxc_ctl documentation\n"
                "\n"
              );
    }
    else if (TestToken("Createdb"))
    {
        printf(
                "\n"
                "Createdb [ - coordinator ] createdb_option ...\n"
                "\n"
                "Invokes createdb utility to create a new database using specified coordinator\n"
                "For more details, please see the pgxc_ctl documentation\n"
                "\n"
              );
    }
    else if (TestToken("Createuser"))
    {
        printf(
                "\n"
                "Createuser[ - coordinator ] createuser_option ...\n"
                "\n"
                "Invokes createuser utility to create a new user using specified coordinator\n"
                "For more details, please see the pgxc_ctl documentation\n"
                "\n"
              );
    }
    else if (TestToken("clean"))
    {
        printf(
                "\n"
                "clean all\n"
                "clean gtm [all | master | slave]\n"
                "clean gtm_proxy [all | nodename ... ]\n"
                "clean coordinator [[all | master | slave ] [nodename ... ]]\n"
                "clean datanode [[all | master | slave ] [nodename ... ]]\n"
                "\n"
                "Stop specified node in immediate mode and clean all resources including data directory\n"
                "For more details, please see the pgxc_ctl documentation\n"
                "\n"
                );
    }
    else if (TestToken("configure"))
    {
        printf("\n"
                "configure all\n"
                "configure datanode nodename ...\n"
                "configure coordinator nodename ...\n"
                "\n"
                "Configure specified node with the node information and reload pooler information\n"
                "For more details, please see the pgxc_ctl documentation\n"
                "\n"
              );
    }
    else if (TestToken("deploy"))
    {
        printf(
                "\n"
                "deploy [ all | host ... ]\n"
                "\n"
                "Deploys postgres-xl binaries and other installation material to specified hosts\n"
                "For more details, please see the pgxc_ctl documentation\n"
                "\n"
              );
    }
    else if (TestToken("failover"))
    {
        printf(
                "\n"
                "failover [ gtm | coordinator nodename | datanode nodename | nodename ]\n"
                "\n"
                "Failover specified node to its master\n"
                "For more details, please see the pgxc_ctl documentation\n"
                "\n"
              );
    }
    else if (TestToken("init"))
    {
        printf(
                "\n"
                "init [force] all\n"
                "init [force] nodename ...\n"
                "init [force] gtm [ master | slave | all ]\n"
                "init [force] gtm_proxy [ all | nodename ... ]\n"
                "init [force] coordinator nodename ...\n"
                "init [force] coordinator [ master | slave ] [ all | nodename ... ]\n"
                "init [force] datanode nodename ...\n"
                "init [force] datanode [ master | slave ] [ all | nodename ... ]\n"
                "\n"
                "Initializes specified nodes.\n"
                "    [force] option removes existing data directories even if they are not empty\n"
                "For more details, please see the pgxc_ctl documentation\n"
                "\n"
              );

    }
    else if (TestToken("kill"))
    {
        printf(
                "\n"
                "kill all\n"
                "kill nodename ...\n"
                "kill gtm [ master | slave | all ]\n"
                "kill gtm_proxy [ all | nodename ... ]\n"
                "kill coordinator nodename ...\n"
                "kill coordinator [ master | slave ] [ all | nodename ... ]\n"
                "kill datanode nodename ...\n"
                "kill datanode [ master | slave ] [ all | nodename ... ]\n"
                "\n"
                "Kills specified node:\n"
                "For more details, please see the pgxc_ctl documentation\n"
                "\n"
              );

    }
    else if (TestToken("log"))
    {
        printf(
                "\n"
                "log [ variable | var ] varname\n"
                "log [ message | msg ] message_body\n"
                "\n"
                "Prints the specified contents to the log file\n"
                "For more details, please see the pgxc_ctl documentation\n"
                "\n"
              );
    }
    else if (TestToken("monitor"))
    {
        printf(
                "\n"
                "monitor all\n"
                "monitor nodename ...\n"
                "monitor gtm [ master | slave | all ]\n"
                "monitor gtm_proxy [ all | nodename ... ]\n"
                "monitor coordinator nodename ...\n"
                "monitor coordinator [ master | slave ] [ all | nodename ... ]\n"
                "monitor datanode nodename ...\n"
                "monitor datanode [ master | slave ] [ all | nodename ...  ]\n"
                "\n"
                "Monitors if specified nodes are running\n"
                "For more details, please see the pgxc_ctl documentation\n"
                "\n"
              );
    }
    else if (TestToken("prepare"))
    {
        printf(
                "\n"
                "prepare [ path ]\n"
                "\n"
                "Write pgxc_ctl configuration file template to the specified file\n"
                "For more details, please see the pgxc_ctl documentation\n"
                "\n"
              );
    }
    else if (TestToken("Psql"))
    {
        printf(
                "\n"
                "Psql [ - coordinator ] psql_option ... \n"
                "\n"
                "Invokes psql targetted to specified coordinator\n"
                "For more details, please see the pgxc_ctl documentation\n"
                "\n"
                );
    }
    else if (TestToken("q"))
    {
        printf(
                "\n"
                "q | quit | exit\n"
                "\n"
                "Exits pgxc_ctl\n"
                "For more details, please see the pgxc_ctl documentation\n"
                "\n"
                );
    }
    else if (TestToken("reconnect"))
    {
        printf(
                "\n"
                "reconnect gtm_proxy [ all | nodename ... ]\n"
                "\n"
                "Reconnects specified gtm_proxy to new gtm\n"
                "For more details, please see the pgxc_ctl documentation\n"
                "\n"
                );
    }
    else if (TestToken("remove"))
    {
        printf(
                "\n"
                "remove gtm slave\n"
                "remove gtm_proxy nodename [ clean ]\n"
                "remove coordinator [ master| slave ] nodename [ clean ]\n"
                "remove datanode [ master| slave ] nodename [ clean ]\n"
                "\n"
                "Removes the specified node from the cluster\n"
                "For more details, please see the pgxc_ctl documentation\n"
                "\n"
                );
    }
    else if (TestToken("set"))
    {
        printf(
                "\n"
                "set varname value ...\n"
                "\n"
                "Set variable value\n"
                "For more details, please see the pgxc_ctl documentation\n"
                "\n"
                );
    }
    else if (TestToken("show"))
    {
        printf(
                "\n"
                "show [ configuration | configure | config ] [ all | basic ]\n"
                "show [ configuration | configure | config ] host hostname ... \n"
                "show [ configuration | configure | config ] gtm [ all | master | slave ]\n"
                "show [ configuration | configure | config ] gtm_proxy [ all | gtm_proxy_name ... ]\n"
                "show [ configuration | configure | config ] [ coordinator | datanode ] [ all | master | slave ] nodename ...\n"
                "\n"
                "Shows postgres-xl configuration\n"
                "\n"
                "show resource datanode datanodename [ databasename [ username ] ]\n"
                "\n"
                "Shows table names specified datanode is involved\n"
                "\n"
                "show [ variable | var ] [ all | varname ... ]\n"
                "\n"
                "Displays configuration or variable name and its value\n"
                "For more details, please see the pgxc_ctl documentation\n"
              );
    }
    else if (TestToken("start"))
    {
        printf(
                "\n"
                "start all\n"
                "start nodename ...\n"
                "start gtm [ master | slave | all ]\n"
                "start gtm_proxy [ all | nodename ... ]\n"
                "start coordinator nodename ...\n"
                "start coordinator [ master | slave ] [ all | nodename ... ]\n"
                "start datanode nodename ...\n"
                "start datanode [ master | slave ] [ all | nodename ... ]\n"
                "\n"
                "Starts specified node\n"
                "For more details, please see the pgxc_ctl documentation\n"
                "\n"
              );
    }
    else if (TestToken("stop"))
    {
        printf(
                "\n"
                "stop [ -m smart | fast | immediate ] all\n"
                "stop gtm [ master | slave | all ]\n"
                "stop gtm_proxy [ all | nodename ... ]\n"
                "stop [ -m smart | fast | immediate ] coordinator nodename ... \n"
                "stop [ -m smart | fast | immediate ] coordinator [ master | slave ] [ all | nodename ... ] \n"
                "stop [ -m smart | fast | immediate ] datanode nodename ... \n"
                "stop [ -m smart | fast | immediate ] datanode [ master | slave ] [ all | nodename ... ] \n"
                "\n"
                "Stops specified node\n"
                "For more details, please see the pgxc_ctl documentation\n"
                "\n"
              );
    }
    else if (TestToken("unregister"))
    {
        printf(
                "\n"
                "unregister unregister_option ...\n"
                "\n"
                "Unregisteres specified node from the gtm\n"
                "For more details, please see the pgxc_ctl documentation\n"
                "\n"
                );
    }
    else
    {
        printf(
                "\n"
                "Unrecognized command: such commands are sent to shell for execution\n"
                "\n"
              );
    }
}

int
get_any_available_coord(int except)
{
    int ii;
    for (ii = 0; aval(VAR_coordMasterServers)[ii]; ii++)
    {
        if (ii == except)
            continue;

        if (!is_none(aval(VAR_coordMasterServers)[ii]))
        {
            if (pingNode(aval(VAR_coordMasterServers)[ii],
                         aval(VAR_coordPorts)[ii]) == 0)
                return ii;
        }
    }

    /*
     * this could be the first coordinator that is being added.
     * This call would happen *after* expanding the array to
     * accomodate the new coordinator. Hence we check for size
     * being more than 1
     */
    if (arraySizeName(VAR_coordNames) > 1)
    {
        for (ii = 0; aval(VAR_coordNames)[ii]; ii++)
        {
            if (!is_none(aval(VAR_coordNames)[ii]))
            {
                elog(ERROR, "ERROR: failed to find any running coordinator");
                return -1;
            }
        }
    }
    return -1;
}
 
int
get_any_available_datanode(int except)
{
    int ii;
    for (ii = 0; aval(VAR_datanodeMasterServers)[ii]; ii++)
    {
        if (ii == except)
            continue;

        if (!is_none(aval(VAR_datanodeMasterServers)[ii]))
        {
            if (pingNode(aval(VAR_datanodeMasterServers)[ii],
                         aval(VAR_datanodePorts)[ii]) == 0)
                return ii;
        }
    }

    /*
     * this could be the first datanode that is being added.
     * This call would happen *after* expanding the array to
     * accomodate the new datanode. Hence we check for size
     * being more than 1
     */
    if (arraySizeName(VAR_datanodeNames) > 1)
    {
        for (ii = 0; aval(VAR_datanodeNames)[ii]; ii++)
        {
            if (!is_none(aval(VAR_datanodeNames)[ii]))
            {
                elog(ERROR, "ERROR: failed to find any running datanode");
                return -1;
            }
        }
    }
    return -1;
}
