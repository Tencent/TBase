#include "txninfo.h"

static int check_xid_is_implicit(char *xid);
static txn_info *find_txn(TransactionId gxid);
static txn_info *make_txn_info(char *dbname, TransactionId gxid, char *xid, char *owner);
static void find_txn_participant_nodes(txn_info *txn);

#define XIDPREFIX "_$XC$"

database_info *find_database_info(char *database_name)
{
    database_info *cur_database_info = head_database_info;

    for (;cur_database_info; cur_database_info = cur_database_info->next)
    {
        if(strcmp(cur_database_info->database_name, database_name) == 0)
            return(cur_database_info);
    }
    return(NULL);
}

database_info *add_database_info(char *database_name)
{
    database_info *rv;

    if ((rv = find_database_info(database_name)) != NULL)
        return rv;        /* Already in the list */
    rv = malloc(sizeof(database_info));
    if (rv == NULL)
        return NULL;
    rv->next = NULL;
    rv->database_name = strdup(database_name);
    if (rv->database_name == NULL)
    {
        free(rv);
        return NULL;
    }
    rv->head_txn_info = NULL;
    rv->last_txn_info = NULL;
    if (head_database_info == NULL)
    {
        head_database_info = last_database_info = rv;
        return rv;
    }
    else
    {
        last_database_info->next = rv;
        last_database_info = rv;
        return rv;
    }
}

int set_node_info(char *node_name, int port, char *host, NODE_TYPE type,
        int nodeid, int index)
{
    node_info *cur_node_info;

    if (index >= pgxc_clean_node_count)
        return -1;
    cur_node_info = &pgxc_clean_node_info[index];
    if (cur_node_info->node_name)
        free(cur_node_info->node_name);
    if (cur_node_info->host)
        free(cur_node_info->host);
    cur_node_info->node_name = strdup(node_name);
    if (cur_node_info->node_name == NULL)
        return -1;
    cur_node_info->port = port;
    cur_node_info->host = strdup(host);
    if (cur_node_info->host == NULL)
        return -1;
    cur_node_info->type = type;
    cur_node_info->nodeid = nodeid;
    return 0;
}

node_info *find_node_info(char *node_name)
{
    int i;
    for (i = 0; i < pgxc_clean_node_count; i++)
    {
        if (pgxc_clean_node_info[i].node_name == NULL)
            continue;
        if (strcmp(pgxc_clean_node_info[i].node_name, node_name) == 0)
            return &pgxc_clean_node_info[i];
    }
    return(NULL);
}

node_info *find_node_info_by_nodeid(int nodeid)
{
    int i;
    for (i = 0; i < pgxc_clean_node_count; i++)
    {
        if (pgxc_clean_node_info[i].nodeid == nodeid)
            return &pgxc_clean_node_info[i];
    }
    return(NULL);
}

int find_node_index(char *node_name)
{
    int i;
    for (i = 0; i < pgxc_clean_node_count; i++)
    {
        if (pgxc_clean_node_info[i].node_name == NULL)
            continue;
        if (strcmp(pgxc_clean_node_info[i].node_name, node_name) == 0)
            return i;
    }
    return  -1;
}

int find_node_index_by_nodeid(int nodeid)
{
    int i;
    for (i = 0; i < pgxc_clean_node_count; i++)
    {
        if (pgxc_clean_node_info[i].nodeid == nodeid)
            return i;
    }
    return  -1;
}

int add_txn_info(char *dbname, char *node_name, TransactionId gxid, char *xid, char *owner, TXN_STATUS status)
{
    txn_info *txn;
    int    nodeidx;

    if ((txn = find_txn(gxid)) == NULL)
    {
        txn = make_txn_info(dbname, gxid, xid, owner);
        if (txn == NULL)
        {
            fprintf(stderr, "No more memory.\n");
            exit(1);
        }
    }
    nodeidx = find_node_index(node_name);
    txn->txn_stat[nodeidx] = status;
    return 1;
}


static txn_info *
make_txn_info(char *dbname, TransactionId gxid, char *xid, char *owner)
{
    database_info *dbinfo;
    txn_info *txn;

    if ((dbinfo = find_database_info(dbname)) == NULL)
        dbinfo = add_database_info(dbname);
    txn = (txn_info *)malloc(sizeof(txn_info));
    if (txn == NULL)
        return NULL;
    memset(txn, 0, sizeof(txn_info));
    txn->gxid = gxid;
    txn->xid = strdup(xid);
    if (txn->xid == NULL)
    {
        free(txn);
        return NULL;
    }
    txn->owner = strdup(owner);
    if (txn->owner == NULL)
    {
        free(txn);
        return NULL;
    }
    if (dbinfo->head_txn_info == NULL)
    {
        dbinfo->head_txn_info = dbinfo->last_txn_info = txn;
    }
    else
    {
        dbinfo->last_txn_info->next = txn;
        dbinfo->last_txn_info = txn;
    }
    txn->txn_stat = (TXN_STATUS *)malloc(sizeof(TXN_STATUS) * pgxc_clean_node_count);
    if (txn->txn_stat == NULL)
        return(NULL);
    memset(txn->txn_stat, 0, sizeof(TXN_STATUS) * pgxc_clean_node_count);
    return txn;
}


/* Ugly ---> Remove this */
txn_info *init_txn_info(char *database_name, TransactionId gxid)
{
    database_info *database;
    txn_info *cur_txn_info;

    if ((database = find_database_info(database_name)) == NULL)
        return NULL;

    if (database->head_txn_info == NULL)
    {
        database->head_txn_info = database->last_txn_info = (txn_info *)malloc(sizeof(txn_info));
        if (database->head_txn_info == NULL)
            return NULL;
        memset(database->head_txn_info, 0, sizeof(txn_info));
        return database->head_txn_info;
    }
    for(cur_txn_info = database->head_txn_info; cur_txn_info; cur_txn_info = cur_txn_info->next)
    {
        if (cur_txn_info->gxid == gxid)
            return(cur_txn_info);
    }
    if(cur_txn_info != NULL)
    {
        cur_txn_info->next = database->last_txn_info = (txn_info *)malloc(sizeof(txn_info));
        if (cur_txn_info->next == NULL)
            return(NULL);
        memset(cur_txn_info->next, 0, sizeof(txn_info));
        if ((cur_txn_info->next->txn_stat = (TXN_STATUS *)malloc(sizeof(TXN_STATUS) * pgxc_clean_node_count)) == NULL)
            return(NULL);
        memset(cur_txn_info->next->txn_stat, 0, sizeof(TXN_STATUS) * pgxc_clean_node_count);
        return cur_txn_info->next;
    }
    else
    {
        return(NULL);
    }
}


static txn_info *find_txn(TransactionId gxid)
{
    database_info *cur_db;
    txn_info *cur_txn;

    for (cur_db = head_database_info; cur_db; cur_db = cur_db->next)
    {
        for (cur_txn = cur_db->head_txn_info; cur_txn; cur_txn = cur_txn->next)
        {
            if (cur_txn->gxid == gxid)
                return cur_txn;
        }
    }
    return NULL;
}

int set_txn_status(TransactionId gxid, char *node_name, TXN_STATUS status)
{
    txn_info *txn;
    int node_idx;

    txn = find_txn(gxid);
    if (txn == NULL)
        return -1;

    node_idx = find_node_index(node_name);
    if (node_idx < 0)
        return -1;

    txn->txn_stat[node_idx] = status;
    return 0;
}

/*
 * This function should be called "after" all the 2PC info
 * has been collected.
 *
 * To determine if a prepared transaction is implicit or explicit,
 * we use gxid.   If gxid ~ '__XC[0-9]+', it is implicit 2PC.
 */

TXN_STATUS check_txn_global_status_gxid(TransactionId gxid)
{
    return(check_txn_global_status(find_txn(gxid)));
}

static void find_txn_participant_nodes(txn_info *txn)
{
    int ii;
    char *xid;
    char *val;

    if (txn == NULL)
        return;

    if ((txn->xid == NULL || *txn->xid == '\0'))
        return;

    xid = strdup(txn->xid);

#define SEP    ":"
    val = strtok(xid, SEP);
    if ((val != NULL) && (strncmp(val, XIDPREFIX, strlen(XIDPREFIX)) != 0))
    {
        fprintf(stderr, "Invalid format for implicit XID (%s).\n", txn->xid);
        exit(1);
    }

    /* Get originating coordinator name */
    val = strtok(NULL, SEP);
    if (val == NULL)
    {
        fprintf(stderr, "Invalid format for implicit XID (%s).\n", txn->xid);
        exit(1);
    }
    txn->origcoord = strdup(val);

    /* Get if the originating coordinator was involved in the txn */
    val = strtok(NULL, SEP);
    if (val == NULL)
    {
        fprintf(stderr, "Invalid format for implicit XID (%s).\n", txn->xid);
        exit(1);
    }
    txn->isorigcoord_part = atoi(val);

    /* Get participating datanode count */
    val = strtok(NULL, SEP);
    if (val == NULL)
    {
        fprintf(stderr, "Invalid format for implicit XID (%s).\n", txn->xid);
        exit(1);
    }
    txn->num_dnparts = atoi(val);

    /* Get participating coordinator count */
    val = strtok(NULL, SEP);
    if (val == NULL)
    {
        fprintf(stderr, "Invalid format for implicit XID (%s).\n", txn->xid);
        exit(1);
    }
    txn->num_coordparts = atoi(val);

    txn->dnparts = (int *) malloc(sizeof (int) * txn->num_dnparts);
    txn->coordparts = (int *) malloc(sizeof (int) * txn->num_coordparts);

    for (ii = 0; ii < txn->num_dnparts; ii++)
    {
        val = strtok(NULL, SEP);
        if (val == NULL)
        {
            fprintf(stderr, "Invalid format for implicit XID (%s).\n", txn->xid);
            exit(1);
        }
        txn->dnparts[ii] = atoi(val);
    }

    for (ii = 0; ii < txn->num_coordparts; ii++)
    {
        val = strtok(NULL, SEP);
        if (val == NULL)
        {
            fprintf(stderr, "Invalid format for implicit XID (%s).\n", txn->xid);
            exit(1);
        }
        txn->coordparts[ii] = atoi(val);
    }
    free(xid);
    return;
}

TXN_STATUS check_txn_global_status(txn_info *txn)
{
#define TXN_PREPARED     0x0001
#define TXN_COMMITTED     0x0002
#define TXN_ABORTED        0x0004

    int ii;
    int check_flag = 0;
    int nodeindx;

    if (txn == NULL)
        return TXN_STATUS_INITIAL;

    find_txn_participant_nodes(txn);

    for (ii = 0; ii < txn->num_dnparts; ii++)
    {
        nodeindx = find_node_index_by_nodeid(txn->dnparts[ii]);
        if (nodeindx == -1)
        {
            fprintf(stderr, "Participant datanode %d not reachable. Can't "
                    "resolve the transaction %s", txn->dnparts[ii], txn->xid);
                return TXN_STATUS_FAILED;
        }

        if (txn->txn_stat[nodeindx] == TXN_STATUS_INITIAL ||
                txn->txn_stat[nodeindx] == TXN_STATUS_UNKNOWN)
            check_flag |= TXN_ABORTED;
        else if (txn->txn_stat[nodeindx] == TXN_STATUS_PREPARED)
            check_flag |= TXN_PREPARED;
        else if (txn->txn_stat[nodeindx] == TXN_STATUS_COMMITTED)
            check_flag |= TXN_COMMITTED;
        else if (txn->txn_stat[nodeindx] == TXN_STATUS_ABORTED)
            check_flag |= TXN_ABORTED;
        else
            return TXN_STATUS_FAILED;
    }

    for (ii = 0; ii < txn->num_coordparts; ii++)
    {
        nodeindx = find_node_index_by_nodeid(txn->coordparts[ii]);
        if (nodeindx == -1)
        {
            fprintf(stderr, "Participant datanode %d not reachable. Can't "
                    "resolve the transaction %s", txn->coordparts[ii], txn->xid);
                return TXN_STATUS_FAILED;
        }

        if (txn->txn_stat[nodeindx] == TXN_STATUS_INITIAL ||
                txn->txn_stat[nodeindx] == TXN_STATUS_UNKNOWN)
            check_flag |= TXN_ABORTED;
        else if (txn->txn_stat[nodeindx] == TXN_STATUS_PREPARED)
            check_flag |= TXN_PREPARED;
        else if (txn->txn_stat[nodeindx] == TXN_STATUS_COMMITTED)
            check_flag |= TXN_COMMITTED;
        else if (txn->txn_stat[nodeindx] == TXN_STATUS_ABORTED)
            check_flag |= TXN_ABORTED;
        else
            return TXN_STATUS_FAILED;
    }

    if ((check_flag & TXN_PREPARED) == 0)
        /* Should be at least one "prepared statement" in nodes */
        return TXN_STATUS_FAILED;
    if ((check_flag & TXN_COMMITTED) && (check_flag & TXN_ABORTED))
        /* Mix of committed and aborted. This should not happen. */
        return TXN_STATUS_FAILED;
    if (check_flag & TXN_COMMITTED)
        /* Some 2PC transactions are committed.  Need to commit others. */
        return TXN_STATUS_COMMITTED;
    if (check_flag & TXN_ABORTED)
        /* Some 2PC transactions are aborted.  Need to abort others. */
        return TXN_STATUS_ABORTED;
    /* All the transactions remain prepared.   No need to recover. */
    if (check_xid_is_implicit(txn->xid))
        return TXN_STATUS_COMMITTED;
    else
        return TXN_STATUS_PREPARED;
}


/*
 * Returns 1 if implicit, 0 otherwise.
 *
 * Should this be replaced with regexp calls?
 */
static int check_xid_is_implicit(char *xid)
{
    if (strncmp(xid, XIDPREFIX, strlen(XIDPREFIX)) != 0)
        return 0;
    return 1;
}

bool check2PCExists(void)
{
    database_info *cur_db;

    for (cur_db = head_database_info; cur_db; cur_db = cur_db->next)
    {
        txn_info *cur_txn;

        cur_txn = cur_db->head_txn_info;
        if(cur_txn)
        {
            return (true);
        }
    }
    return (false);
}

char *str_txn_stat(TXN_STATUS status)
{
    switch(status)
    {
        case TXN_STATUS_INITIAL:
            return("initial");
        case TXN_STATUS_UNKNOWN:
            return("unknown");
        case TXN_STATUS_PREPARED:
            return("prepared");
        case TXN_STATUS_COMMITTED:
            return("committed");
        case TXN_STATUS_ABORTED:
            return("aborted");
        case TXN_STATUS_FAILED:
            return("failed");
        default:
            return("undefined status");
    }
}
