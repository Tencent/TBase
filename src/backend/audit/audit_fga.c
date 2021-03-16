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
#include "postgres.h"
#include "fmgr.h"
#include "audit/audit_fga.h"
#include "utils/builtins.h"
#include "catalog/pg_audit.h"

#include "utils/lsyscache.h"
#include "catalog/indexing.h"
#include "access/attnum.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/tupdesc.h"
#include "access/heapam.h"
#include "utils/relcache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "miscadmin.h"

#include "parser/analyze.h"
#include "parser/parser.h"
#include "tcop/tcopprot.h"
#include "pgxc/planner.h"
#include "parser/parse_clause.h"
#include "parser/parse_relation.h"
#include "access/skey.h"
#include "access/genam.h"
#include "utils/fmgroids.h"
#include "tcop/utility.h"
#include "nodes/nodes.h"
#include "pgxc/pgxcnode.h"

#include "pgxc/nodemgr.h"
#include "tsearch/ts_locale.h"

#include "libpq/libpq.h"
#include "postmaster/bgworker.h"
#include "storage/pg_shmem.h"
#include "storage/shmem.h"
#include "parser/parse_func.h"
#include "nodes/pg_list.h"
#include "../interfaces/libpq/libpq-fe.h"
#include "commands/dbcommands.h"
#include "postmaster/postmaster.h"
#include "parser/parse_collate.h"
#include "miscadmin.h"

#include "postmaster/auditlogger.h"

#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/pmsignal.h"

#include "pgstat.h"
#include "catalog/pg_authid.h"


#ifdef _PG_REGRESS_
bool enable_fga    = true;
#else
bool enable_fga    = false;
#endif


#define TEXTOID 25
#define FORMATTED_TS_LEN 128
#define    FGA_LATCH_MICROSEC    5000000L
#define num_audit_fga_tigger_info MaxBackends


static char formatted_start_time[FORMATTED_TS_LEN];
static char formatted_log_time[FORMATTED_TS_LEN];


/*
 * Flags set by interrupt handlers for later service in the main loop.
 */
static volatile sig_atomic_t fga_gotSIGHUP = false;            /* pg_ctl reload */
static volatile sig_atomic_t fga_gotSIGTERM = false;        /* pg_ctl stop -m smart */
static volatile sig_atomic_t fga_gotSIGUSR1= false;            /* wakeup */

static volatile sig_atomic_t fga_consume_requested = false;    /* SIGUSR1, killed by write_trigger_handle_to_shmem() */

static audit_fga_tigger_info *BackendAuditFgaArray = NULL;



static char *pg_strdup(const char *in);
static void add_stringlist_item(_stringlist **listhead, const char *str);
static void free_stringlist(_stringlist **listhead);
static void split_to_stringlist(const char *s, const char *delim, _stringlist **listhead);
static char *func_ltrim(char *str);
static char *func_rtrim(char *str);
static char *func_trim(char *str);
static void setup_formatted_log_time(void);

static void    worker_audit_fga_sighup(SIGNAL_ARGS);
static void worker_audit_fga_wakeup(SIGNAL_ARGS);
static void process_fga_trigger(bool timeout);
static void reset_shem_info(int);
static bool is_single_cmd(char * cmd);



Oid
schema_name_2_oid(text *in_string)
{
    char *str;
    Oid str_oid;

    str = pnstrdup(VARDATA_ANY(in_string), VARSIZE_ANY_EXHDR(in_string));
    elog(LOG, "str = \"%s\"", str);

    str_oid = get_namespaceid(str);
    elog(LOG, "str_oid oid = %d", str_oid);

    return str_oid;
}

Oid
object_name_2_oid(text *in_string, Oid schema_oid)
{
    char *str;
    Oid str_oid;

    str = pnstrdup(VARDATA_ANY(in_string), VARSIZE_ANY_EXHDR(in_string));
    elog(LOG, "str = \"%s\"", str);

    str_oid = get_relname_relid(str, schema_oid);
    elog(LOG, "str_oid oid = %d", str_oid);

    return str_oid;
}

Oid
function_name_2_oid(text *in_string, Oid schema_oid)
{
    char *str;
    Oid str_oid;

    str = pnstrdup(VARDATA_ANY(in_string), VARSIZE_ANY_EXHDR(in_string));
    elog(LOG, "str = \"%s\"", str);

    str_oid = get_relname_relid(str, schema_oid);
    elog(LOG, "str_oid oid = %d", str_oid);

    return str_oid;
}


Datum
text_2_namedata_datum(text *in_string)
{
    char *str;
    char *low_str;
    Datum value;

    if ( VARSIZE_ANY_EXHDR(in_string) < NAMEDATALEN)
    {
        str = pnstrdup(VARDATA_ANY(in_string), VARSIZE_ANY_EXHDR(in_string));
        low_str = lowerstr(str);
        value = CStringGetDatum(low_str);
    }
    else
    {
        str = pnstrdup(VARDATA_ANY(in_string), NAMEDATALEN - 1);
        low_str = lowerstr(str);
        value = CStringGetDatum(low_str);
    }

    pfree(str);

    return value;
}

/*
 * "Safe" wrapper around strdup().
 */
static char *
pg_strdup(const char *in)
{
    char       *tmp;

    if (!in)
    {
        fprintf(stderr,
                _("cannot duplicate null pointer (internal error)\n"));
        exit(EXIT_FAILURE);
    }
    tmp = strdup(in);
    if (!tmp)
    {
        fprintf(stderr, _("out of memory\n"));
        exit(EXIT_FAILURE);
    }
    return tmp;
}

static char *
func_rtrim(char *str)
{  
    int len;
    char *p;

    if (str == NULL || *str == '\0')  
    {  
        return str;  
    }  
  
    len = strlen(str);  
    p = str + len - 1;  
    while (p >= str  && isspace(*p))  
    {  
        *p = '\0';  
        --p;  
    }  
  
    return str;  
}

static char *
func_ltrim(char *str)  
{  
    int len = 0;
    char *p;

    if (str == NULL || *str == '\0')  
    {  
        return str;  
    }  


    p = str;  
    while (*p != '\0' && isspace(*p))  
    {  
        ++p;  
        ++len;  
    }  

    memmove(str, p, strlen(str) - len + 1);  

    return str;  
}  

static char *
func_trim(char *str)  
{  
    str = func_rtrim(str);  
    str = func_ltrim(str);  
      
    return str;
}

#if 0
static char *GetUserName(void)
{
    struct passwd *passwd;

    passwd = getpwuid(getuid());
    if (passwd)
        return(strdup(passwd->pw_name));
    else
    {
        fprintf(stderr, "%s: could not get current user name: %s\n", progname, strerror(errno));
        exit(1);
    }
    return NULL;
}
#endif

static PGconn *
conn_database(char *host, char *port, char *user, char *dbname)
{
    PGconn *coord_conn;
#define PARAMS_ARRAY_SIZE 7
    const char *keywords[PARAMS_ARRAY_SIZE];
    const char *values[PARAMS_ARRAY_SIZE];


    keywords[0] = "host";
    values[0] = host;
    keywords[1] = "port";
    values[1] = port;
    keywords[2] = "user";
    values[2] = user;
    keywords[3] = "password";
    values[3] = NULL;
    keywords[4] = "dbname";
    values[4] = dbname;
    keywords[5] = "fallback_application_name";
    values[5] = "test policy";
    keywords[6] = NULL;
    values[6] = NULL;

    coord_conn = PQconnectdbParams(keywords, values, true);

    if (PQstatus(coord_conn) == CONNECTION_BAD)
    {
        PQfinish(coord_conn);
        return NULL;
    }

    return coord_conn;
}


/*
 * Add an item at the end of a stringlist.
 */
static void
add_stringlist_item(_stringlist **listhead, const char *str)
{
    _stringlist *newentry = palloc(sizeof(_stringlist));
    _stringlist *oldentry;

    newentry->str = pstrdup(str);
    newentry->next = NULL;
    if (*listhead == NULL)
        *listhead = newentry;
    else
    {
        for (oldentry = *listhead; oldentry->next; oldentry = oldentry->next)
             /* skip */ ;
        oldentry->next = newentry;
    }
}

/*
 * Free a stringlist.
 */
static void
free_stringlist(_stringlist **listhead)
{
    if (listhead == NULL || *listhead == NULL)
        return;
    if ((*listhead)->next != NULL)
        free_stringlist(&((*listhead)->next));
    pfree((*listhead)->str);
    pfree(*listhead);
    *listhead = NULL;
}

/*
 * Split a delimited string into a stringlist
 */
static void
split_to_stringlist(const char *s, const char *delim, _stringlist **listhead)
{
    char       *sc = pg_strdup(s);
    char       *token = strtok(sc, delim);

    while (token)
    {
        add_stringlist_item(listhead, token);
        token = strtok(NULL, delim);
    }
    free(sc);
}

void
exec_policy_funct_on_other_node(char *query_string)
{
    Oid     *cn_node_list = NULL;
    Oid     *dn_node_list = NULL;
    int     cn_nodes_num;
    int     dn_nodes_num;

    if (IS_PGXC_LOCAL_COORDINATOR)
    {
        cn_nodes_num = NumCoords - 1;
        if (cn_nodes_num > 0)
        {
            cn_node_list = (Oid *) palloc0(cn_nodes_num * sizeof(Oid));
                
            PGXCGetCoordOidOthers(cn_node_list);
            pgxc_execute_on_nodes(cn_nodes_num, cn_node_list, query_string);
        }
    }

    if(IS_PGXC_COORDINATOR && !IsConnFromCoord())
    {        
        dn_nodes_num = NumDataNodes;
        dn_node_list = (Oid *) palloc0(dn_nodes_num * sizeof(Oid));
        PGXCGetAllDnOid(dn_node_list);    

        pgxc_execute_on_nodes(dn_nodes_num, dn_node_list, query_string);

        if (cn_node_list != NULL)
            pfree(cn_node_list);

        if (dn_node_list != NULL)
            pfree(dn_node_list);
    }

    return ;
}

/*
 * setup formatted_log_time, for consistent times between CSV and regular logs
 */
static void
setup_formatted_log_time(void)
{
    struct timeval tv;
    time_t    stamp_time;
    char        msbuf[8];

    gettimeofday(&tv, NULL);
    stamp_time = (time_t) tv.tv_sec;

    strftime(formatted_log_time, FORMATTED_TS_LEN,
                /* leave room for milliseconds... */
                "%Y-%m-%d %H:%M:%S     %Z",
                localtime(&stamp_time));

    /* 'paste' milliseconds into place... */
    sprintf(msbuf, ".%03d", (int) (tv.tv_usec / 1000));
    strncpy(formatted_log_time + 19, msbuf, 4);
}

/*
 * setup formatted_start_time
 */
static void
setup_formatted_start_time(void)
{
    pg_time_t    stamp_time = (pg_time_t) MyStartTime;

    /*
     * Note: we expect that guc.c will ensure that log_timezone is set up (at
     * least with a minimal GMT value) before Log_line_prefix can become
     * nonempty or CSV mode can be selected.
     */
    pg_strftime(formatted_start_time, FORMATTED_TS_LEN,
                "%Y-%m-%d %H:%M:%S %Z",
                pg_localtime(&stamp_time, log_timezone));
}

static inline void
appendCSVLiteral(StringInfo buf, const char *data)
{
    const char *p = data;
    char        c;

    /* avoid confusing an empty string with NULL */
    if (p == NULL)
        return;

    appendStringInfoCharMacro(buf, '"');
    while ((c = *p++) != '\0')
    {
        if (c == '"')
            appendStringInfoCharMacro(buf, '"');
        appendStringInfoCharMacro(buf, c);
    }
    appendStringInfoCharMacro(buf, '"');
}


void audit_fga_log_prefix(StringInfoData *buf)
{// #lizard forgives
    initStringInfo(buf);

    if (formatted_log_time[0] == '\0')
        setup_formatted_log_time();

    appendStringInfoString(buf, formatted_log_time);
    appendStringInfoChar(buf, ',');

    /* username */
    if (MyProcPort)
        appendCSVLiteral(buf, MyProcPort->user_name);
    appendStringInfoChar(buf, ',');

    /* database name */
    if (MyProcPort)
        appendCSVLiteral(buf, MyProcPort->database_name);
    appendStringInfoChar(buf, ',');

    /* Process id  */
    if (MyProcPid != 0)
        appendStringInfo(buf, "%d", MyProcPid);
    appendStringInfoChar(buf, ',');

    /* Remote host and port */
    if (MyProcPort && MyProcPort->remote_host)
    {
        appendStringInfoChar(buf, '"');
        appendStringInfoString(buf, MyProcPort->remote_host);
        if (MyProcPort->remote_port && MyProcPort->remote_port[0] != '\0')
        {
            appendStringInfoChar(buf, ':');
            appendStringInfoString(buf, MyProcPort->remote_port);
        }
        appendStringInfoChar(buf, '"');
    }
    appendStringInfoChar(buf, ',');

    /* node name */
    if (PGXCNodeName)
        appendCSVLiteral(buf, PGXCNodeName);
    appendStringInfoChar(buf, ',');

    /* session start timestamp */
    if (formatted_start_time[0] == '\0')
        setup_formatted_start_time();
    appendStringInfoString(buf, formatted_start_time);
    appendStringInfoChar(buf, ',');  
}


void audit_fga_log_prefix_json(StringInfoData *buf)
{// #lizard forgives
    initStringInfo(buf);

    /*log time */
    appendStringInfoString(buf, "LogTime: ");

    if (formatted_log_time[0] == '\0')
        setup_formatted_log_time();

    appendCSVLiteral(buf, formatted_log_time);
    appendStringInfoChar(buf, ',');

    /* username */
    appendStringInfoString(buf, "UserName: ");
    if (MyProcPort)
        appendCSVLiteral(buf, MyProcPort->user_name);
    appendStringInfoChar(buf, ',');

    /* database name */
    appendStringInfoString(buf, "DatabaseName: ");
    if (MyProcPort)
        appendCSVLiteral(buf, MyProcPort->database_name);
    appendStringInfoChar(buf, ',');

    /* Process id  */
    appendStringInfoString(buf, "ProcessPid: ");
    if (MyProcPid != 0)
        appendStringInfo(buf, "%d", MyProcPid);
    appendStringInfoChar(buf, ',');

    /* Remote host and port */
    appendStringInfoString(buf, "RemoteIpPort: ");
    if (MyProcPort && MyProcPort->remote_host)
    {
        appendStringInfoChar(buf, '"');
        appendStringInfoString(buf, MyProcPort->remote_host);
        if (MyProcPort->remote_port && MyProcPort->remote_port[0] != '\0')
        {
            appendStringInfoChar(buf, ':');
            appendStringInfoString(buf, MyProcPort->remote_port);
        }
        appendStringInfoChar(buf, '"');
    }
    appendStringInfoChar(buf, ',');

    /* node name */
    appendStringInfoString(buf, "NodeName: ");
    if (PGXCNodeName)
        appendCSVLiteral(buf, PGXCNodeName);
    appendStringInfoChar(buf, ',');

    /* session start timestamp */
    appendStringInfoString(buf, "SessionStartTime: ");
    if (formatted_start_time[0] == '\0')
        setup_formatted_start_time();
    appendCSVLiteral(buf, formatted_start_time);
    appendStringInfoChar(buf, ',');
}

void audit_fga_log_policy_info(AuditFgaPolicy *policy_s, char * cmd_type)
{// #lizard forgives
    char        *null_str = "null";
    Oid         schema_oid;
    Oid         object_oid; 
    Oid         handler_module_oid;
    char        *schema_name;
    char        *object_name;

    Relation    audit_fga_rel;
    HeapTuple    policy_tuple;
    Datum        schema_datum;
    Datum        object_datum;
    Datum       handler_module_datum;
    bool        schema_is_null;
    bool        object_is_null;
    bool        handler_module_is_null;

    StringInfoData buf;

    audit_fga_log_prefix_json(&buf);

    if (policy_s && policy_s->policy_name)
    {
        audit_fga_rel = heap_open(PgAuditFgaConfRelationId, RowExclusiveLock);
        policy_tuple = SearchSysCache1(AUDITFGAPOLICYCONF, CStringGetDatum(policy_s->policy_name));

        
        schema_datum = heap_getattr(policy_tuple, Anum_audit_fga_conf_object_schema,
                            RelationGetDescr(audit_fga_rel), &schema_is_null);
        object_datum = heap_getattr(policy_tuple, Anum_audit_fga_conf_object_id,
                            RelationGetDescr(audit_fga_rel), &object_is_null);
        handler_module_datum = heap_getattr(policy_tuple, Anum_audit_fga_conf_handler_module,
                            RelationGetDescr(audit_fga_rel), &handler_module_is_null);

        if (!handler_module_is_null)
        {
            handler_module_oid = DatumGetObjectId(handler_module_datum);

            write_trigger_handle_to_shmem(handler_module_oid);
        }

        schema_oid = DatumGetObjectId(schema_datum);
        object_oid = DatumGetObjectId(object_datum);
        
        //table schema name
        appendStringInfoString(&buf, "TableSchema: ");

        schema_name = get_namespace_name(schema_oid);
        appendCSVLiteral(&buf, (schema_name != NULL) ? schema_name : null_str);
        appendStringInfoChar(&buf, ',');

        //table name
        appendStringInfoString(&buf, "TableName: ");

        object_name = get_rel_name(object_oid);
        appendCSVLiteral(&buf, (object_name != NULL) ? object_name : null_str);
        appendStringInfoChar(&buf, ',');       

        ReleaseSysCache(policy_tuple);
        heap_close(audit_fga_rel, RowExclusiveLock);
    }

    //policy name
    appendStringInfoString(&buf, "PolicyName: ");
    
    if (policy_s && policy_s->policy_name)
    {
        appendCSVLiteral(&buf, policy_s->policy_name);
    }
    appendStringInfoChar(&buf, ',');

    //query string
    appendStringInfoString(&buf, "QueryString: ");

    if (policy_s && policy_s->query_string)
    {
        appendCSVLiteral(&buf, policy_s->query_string);
    }
    appendStringInfoChar(&buf, ',');

    //command type
    appendStringInfoString(&buf, "CommandType: ");

    if (cmd_type)
        appendCSVLiteral(&buf, cmd_type);

    audit_log_fga("%s", buf.data);
    pfree(buf.data);
}

void audit_fga_log_policy_info_2(audit_fga_policy_state *policy_s, char * cmd_type)
{// #lizard forgives
    char        *null_str = "null";
    Oid         schema_oid;
    Oid         object_oid;
    Oid         handler_module_oid;
    char        *schema_name;
    char        *object_name;

    Relation    audit_fga_rel;
    HeapTuple    policy_tuple;
    Datum        schema_datum;
    Datum        object_datum;
    Datum       handler_module_datum;
    bool        schema_is_null;
    bool        object_is_null;
    bool        handler_module_is_null;

    StringInfoData buf;

    audit_fga_log_prefix_json(&buf);

    if (policy_s && policy_s->policy_name)
    {
        audit_fga_rel = heap_open(PgAuditFgaConfRelationId, RowExclusiveLock);
        policy_tuple = SearchSysCache1(AUDITFGAPOLICYCONF, CStringGetDatum(policy_s->policy_name));

        if (!policy_tuple)
        {
            heap_close(audit_fga_rel, RowExclusiveLock);
            pfree(buf.data);
            return;
        }
        
        object_datum = heap_getattr(policy_tuple, Anum_audit_fga_conf_object_id,
                            RelationGetDescr(audit_fga_rel), &object_is_null);
        schema_datum = heap_getattr(policy_tuple, Anum_audit_fga_conf_object_schema,
                            RelationGetDescr(audit_fga_rel), &schema_is_null);
        handler_module_datum = heap_getattr(policy_tuple, Anum_audit_fga_conf_handler_module,
                            RelationGetDescr(audit_fga_rel), &handler_module_is_null);

        if (!handler_module_is_null)
        {
            handler_module_oid = DatumGetObjectId(handler_module_datum);

            write_trigger_handle_to_shmem(handler_module_oid);
        }

        object_oid = DatumGetObjectId(object_datum);
        schema_oid = DatumGetObjectId(schema_datum);

        //table schema name
        appendStringInfoString(&buf, "TableSchema: ");

        schema_name = get_namespace_name(schema_oid);
        appendCSVLiteral(&buf, (schema_name != NULL) ? schema_name : null_str);
        appendStringInfoChar(&buf, ',');

        //table name
        appendStringInfoString(&buf, "TableName: ");

        object_name = get_rel_name(object_oid);
        appendCSVLiteral(&buf, (object_name != NULL)  ?  object_name : null_str);
        appendStringInfoChar(&buf, ',');       

        ReleaseSysCache(policy_tuple);
        heap_close(audit_fga_rel,  RowExclusiveLock);
    }

    //policy name
    appendStringInfoString(&buf, "PolicyName: ");

    if (policy_s && policy_s->policy_name)
    {
        appendCSVLiteral(&buf,  policy_s->policy_name);
    }
    appendStringInfoChar(&buf, ',');

    //query string
    appendStringInfoString(&buf, "QueryString: ");

    if (policy_s && policy_s->query_string)
    {
        appendCSVLiteral(&buf,  policy_s->query_string);
    }
    appendStringInfoChar(&buf, ',');

    //command type
    appendStringInfoString(&buf, "CommandType: ");

    if (cmd_type)
    {
        appendCSVLiteral(&buf, cmd_type);
    }

    audit_log_fga("%s", buf.data);
    pfree(buf.data);
}

/*
  * Is the command single sql instead of multi sql separated with ";"
  */
bool is_single_cmd(char * cmd)
{
    int cnt = 0;
    char *del = ";";
    char *result = NULL;
    
    result = strtok(cmd, del);
    while( result != NULL )
    {
        cnt++;
        result = strtok(NULL, del);
    }

    if (cnt > 1)
    {
        return false;
    }

    return true;    
}

Datum
add_policy(PG_FUNCTION_ARGS)
{// #lizard forgives
    Datum       values[Natts_audit_fga_conf];
    bool        nulls[Natts_audit_fga_conf];
    int         i;
    text        *in_string;
    Oid         schema_oid;
    Oid         object_oid;
    Oid         handler_schema_oid;
    Relation    rel;
    HeapTuple   tup;
    Oid            *inTypes;
    oidvector   *nodes_array;

    _stringlist *col_list = NULL;
    _stringlist *sl;
    //char        *delim = ',';
    char        *input_columns;
    int         input_column_cnt = 0;
    HeapTuple    atttuple;

    char parse_sql[AUDIT_FGA_SQL_LEN];
    char *schema_name;
    char *object_name;
    char *audit_condition;
    List *parsetree_list;
    ListCell   *parsetree_item;

    char *sql_cmd = pstrdup(debug_query_string);

    if (!is_single_cmd(sql_cmd))
    {
        elog(ERROR, "add_policy function cannot used with other command;");
    }

    /* only adt_admin can add policy */
    if ((Oid)DEFAULT_ROLE_AUDIT_SYS_USERID != GetUserId())
    {
        elog(ERROR, "Only audit admin can do this");
    }
    
    /* Iterate through attributes initializing nulls and values */
    for (i = 0; i < Natts_audit_fga_conf; i++)
    {
        nulls[i] = false;
        values[i] = (Datum) 0;
    }

    /* Set auditor Oid */
    values[Anum_audit_fga_conf_auditor_id - 1] = ObjectIdGetDatum(GetUserId());

    // check object_schema
    if ( !PG_ARGISNULL(0) )
    {
        in_string = PG_GETARG_TEXT_PP(0);
        schema_oid = schema_name_2_oid(in_string);
        values[Anum_audit_fga_conf_object_schema - 1] = ObjectIdGetDatum(schema_oid);

        schema_name = text_to_cstring(in_string);

        if(values[Anum_audit_fga_conf_object_schema - 1] == InvalidOid)
        {
            elog(ERROR, "The object schema does not exist");
        }
    }
    else
    {
        elog(ERROR, "missing object schema name");
    }

    // check object_name
    if ( !PG_ARGISNULL(1) )
    {
        in_string = PG_GETARG_TEXT_PP(1); 
        object_oid = object_name_2_oid(in_string, schema_oid);
        values[Anum_audit_fga_conf_object_id - 1] = ObjectIdGetDatum(object_oid);

        object_name = text_to_cstring(in_string);

        if(values[Anum_audit_fga_conf_object_id - 1] == InvalidOid)
        {
            elog(ERROR, "The object name does not exist");
        }
    }
    else
    {
        elog(ERROR, "missing object name");
    }

    // check policy_name
    if ( !PG_ARGISNULL(2) )
    {
        in_string = PG_GETARG_TEXT_PP(2);
        values[Anum_audit_fga_conf_policy_name - 1] = text_2_namedata_datum(in_string);             
    }
    else
    {
        elog(ERROR, "missing policy name");
    }

    // check audit_columns
    if ( !PG_ARGISNULL(3) )
    {
        in_string = PG_GETARG_TEXT_PP(3);
        values[Anum_audit_fga_conf_audit_columns - 1] = PG_GETARG_DATUM(3);
        input_columns = pnstrdup(VARDATA_ANY(in_string), VARSIZE_ANY_EXHDR(in_string));

        split_to_stringlist(input_columns, ",", &col_list);
        for (sl = col_list; sl; sl = sl->next)
        {
            atttuple = SearchSysCacheAttName(object_oid, func_trim(sl->str));
            if (!HeapTupleIsValid(atttuple))
            {
                free_stringlist(&col_list);
                elog(ERROR, "Error column: %s", sl->str);
            }
            
            input_column_cnt++;
            ReleaseSysCache(atttuple);   
        } 
    }
    else
    {
        nulls[Anum_audit_fga_conf_audit_columns - 1] = true;
    }

    // check audit_column_oids
    if (nulls[Anum_audit_fga_conf_audit_columns - 1] == true)
    {
        inTypes = (Oid *) palloc(sizeof(Oid));
        inTypes[0] = InvalidOid;
        nodes_array = buildoidvector(inTypes, 1);
        values[Anum_audit_fga_conf_audit_column_ids - 1] = PointerGetDatum(nodes_array);
    }
    else
    {
        Oid * nodes = (Oid *) palloc(input_column_cnt * sizeof(Oid));
        int i = 0;
        for (sl = col_list; sl; sl = sl->next)
        {
            nodes[i] = (Oid) get_attnum(object_oid, sl->str);
            i++;
        }

        nodes_array = buildoidvector(nodes, input_column_cnt);
        values[Anum_audit_fga_conf_audit_column_ids - 1] = PointerGetDatum(nodes_array);      

        if (col_list != NULL)
            free_stringlist(&col_list);
    }
    
    // check audit_condition
    if ( !PG_ARGISNULL(4) )
    {    
        Node *audit_fga_qual = NULL;
        ParseState *parsestate = make_parsestate(NULL);
        RangeTblEntry *rte;
        Relation    target_table;
        
        in_string = PG_GETARG_TEXT_PP(4);
        audit_condition = pnstrdup(VARDATA_ANY(PG_GETARG_TEXT_PP(4)), VARSIZE_ANY_EXHDR(in_string));
        snprintf(parse_sql, 
                            AUDIT_FGA_SQL_LEN, 
                            "select * from %s.%s where %s",
                            schema_name,
                            object_name,
                            audit_condition);

        
        target_table = relation_open(object_oid, NoLock);
        rte = addRangeTableEntryForRelation(parsestate, target_table,
                                        NULL, false, false);
        addRTEtoQuery(parsestate, rte, false, true, true);
        
        parsetree_list = pg_parse_query(parse_sql);
        
        foreach(parsetree_item, parsetree_list)
        {
            RawStmt    *parsetree = lfirst_node(RawStmt, parsetree_item);
            SelectStmt *n = (SelectStmt *) parsetree->stmt;
            //transformFromClause(parsestate, n->fromClause);
            audit_fga_qual = transformWhereClause(parsestate, n->whereClause,
                                                   EXPR_KIND_WHERE, "WHERE");
            assign_expr_collations(parsestate, audit_fga_qual);
        }
        relation_close(target_table, NoLock);

        values[Anum_audit_fga_conf_audit_condition_str - 1] = PG_GETARG_DATUM(4);

        if (audit_fga_qual)
        {
            values[Anum_audit_fga_conf_audit_condition - 1] = CStringGetTextDatum(nodeToString(audit_fga_qual));

        }
        else
            nulls[Anum_audit_fga_conf_audit_condition - 1] = true;        
    }
    else
    {
        nulls[Anum_audit_fga_conf_audit_condition - 1] = true;
        nulls[Anum_audit_fga_conf_audit_condition_str - 1] = true;
    }

    // check handler_schema
    if ( !PG_ARGISNULL(5) )
    {
        in_string = PG_GETARG_TEXT_PP(5);
        handler_schema_oid = schema_name_2_oid(in_string);
        values[Anum_audit_fga_conf_handler_schema - 1] = ObjectIdGetDatum(handler_schema_oid);

        if(values[Anum_audit_fga_conf_handler_schema - 1] == InvalidOid)
        {
            elog(ERROR, "The handler schema does not exist");
        }
    }
    else
    {
        nulls[Anum_audit_fga_conf_handler_schema - 1] = true;
    }

    // check handler_module
    if ( !PG_ARGISNULL(6) )
    {
        char    *func_name;
        Oid     funcargtypes[1];
        List    *func_list = NIL;
        
        //funcargtypes[0] = TEXTOID;
        in_string = PG_GETARG_TEXT_PP(6);
        func_name = pnstrdup(VARDATA_ANY(in_string), VARSIZE_ANY_EXHDR(in_string));
        func_list = list_make1(makeString(func_name));
        
        object_oid = LookupFuncName(func_list, 0, funcargtypes, false);
        //object_oid = LookupFuncName(func_list, 1, funcargtypes, false);
        values[Anum_audit_fga_conf_handler_module - 1] = ObjectIdGetDatum(object_oid);

        if(values[Anum_audit_fga_conf_handler_module - 1] == InvalidOid)
        {
            elog(ERROR, "The handler module function does not exist");
        }
    }
    else
    {
        nulls[Anum_audit_fga_conf_handler_module - 1] = true;
    }

    // check audit_enable
    if ( !PG_ARGISNULL(7) )
    {
        values[Anum_audit_fga_conf_audit_enable - 1] = PG_GETARG_DATUM(7);
    }
    else
    {
        nulls[Anum_audit_fga_conf_audit_enable - 1] = true;
    }

    // check statement_types
    if ( !PG_ARGISNULL(8) )
    {
        in_string = PG_GETARG_TEXT_PP(8);
        values[Anum_audit_fga_conf_statement_types - 1] = text_2_namedata_datum(in_string);             
    }
    else
    {
        nulls[Anum_audit_fga_conf_statement_types - 1] = true;
    }

    // check audit_column_opts
    if ( !PG_ARGISNULL(9) )
    {
        values[Anum_audit_fga_conf_audit_column_opts - 1] = PG_GETARG_DATUM(9);
    }
    else
    {
        nulls[Anum_audit_fga_conf_audit_column_opts - 1] = true;
    }

    rel = heap_open(PgAuditFgaConfRelationId, RowExclusiveLock);
    
    tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);
    
    CatalogTupleInsert(rel, tup);

    heap_close(rel, RowExclusiveLock);

    exec_policy_funct_on_other_node(sql_cmd);

    PG_RETURN_BOOL(true);
}

Datum
drop_policy(PG_FUNCTION_ARGS)
{
    Relation    rel;
    HeapTuple   tup;
    char        *policy_name;
    text        *in_string;

    char *sql_cmd = pstrdup(debug_query_string);

    if (!is_single_cmd(sql_cmd))
    {
        elog(ERROR, "drop_policy function cannot used with other command;");
    }

    /* only adt_admin can add policy */
    if ((Oid)DEFAULT_ROLE_AUDIT_SYS_USERID != GetUserId())
    {
        elog(ERROR, "Only audit admin can do this");
    }
    
    // check object_schema
    if ( PG_ARGISNULL(0) )
    {
        elog(ERROR, "missing object schema name");
    }

    // check object_name
    if ( PG_ARGISNULL(1) )
    {
        elog(ERROR, "missing object name");
    }

    // check policy_name
    if ( !PG_ARGISNULL(2) )
    {
        in_string = PG_GETARG_TEXT_PP(2);
        policy_name = pnstrdup(VARDATA_ANY(in_string), VARSIZE_ANY_EXHDR(in_string));        
    }
    else
    {
        elog(ERROR, "missing policy name");
    }

    rel = heap_open(PgAuditFgaConfRelationId, RowExclusiveLock);
    tup = SearchSysCache1(AUDITFGAPOLICYCONF, CStringGetDatum(policy_name));

    if(!HeapTupleIsValid(tup))
        elog(ERROR,"policy[%s] is not exist", policy_name);

    simple_heap_delete(rel, &tup->t_self);
    ReleaseSysCache(tup);
    heap_close(rel, RowExclusiveLock);

    exec_policy_funct_on_other_node(sql_cmd);
  
    PG_RETURN_BOOL(true);
}

Datum
enable_policy(PG_FUNCTION_ARGS)
{
    Relation    rel;
    HeapTuple   tup;
    HeapTuple   new_tup;
    char        *policy_name;
    text        *in_string;

    Datum       values[Natts_audit_fga_conf];
    bool        nulls[Natts_audit_fga_conf];
    bool        replaces[Natts_audit_fga_conf];

    char *sql_cmd = pstrdup(debug_query_string);

    if (!is_single_cmd(sql_cmd))
    {
        elog(ERROR, "enable_policy function cannot used with other command; ");
    }

    /* only adt_admin can add policy */
    if ( (Oid)DEFAULT_ROLE_AUDIT_SYS_USERID != GetUserId() )
    {
        elog(ERROR, "Only audit admin can do this ");
    }
    
    // check object_schema
    if (PG_ARGISNULL(0))
    {
        elog(ERROR, "missing object schema name");
    }

    // check object_name
    if (PG_ARGISNULL(1))
    {
        elog(ERROR, "missing object name");
    }

    // check policy_name
    if (!PG_ARGISNULL(2))
    {
        in_string = PG_GETARG_TEXT_PP(2);
        policy_name = pnstrdup(VARDATA_ANY(in_string), VARSIZE_ANY_EXHDR(in_string));         
    }
    else
    {
        elog(ERROR,  "missing policy name");
    }

    rel = heap_open(PgAuditFgaConfRelationId, RowExclusiveLock);
    tup = SearchSysCache1(AUDITFGAPOLICYCONF, CStringGetDatum(policy_name));

    if(!HeapTupleIsValid(tup))
        elog(ERROR,"policy[%s] is not exist!", policy_name);

    MemSet(values, 0, sizeof(values));
    MemSet(nulls, false, sizeof(nulls));
    MemSet(replaces, false, sizeof(replaces));

    replaces[Anum_audit_fga_conf_audit_enable - 1] = true;
    values[Anum_audit_fga_conf_audit_enable - 1] = BoolGetDatum(true);

    new_tup = heap_modify_tuple(tup, RelationGetDescr(rel), values,
                                 nulls, replaces);

    CatalogTupleUpdate(rel, &new_tup->t_self, new_tup);

    ReleaseSysCache(tup);
    
    heap_close(rel, RowExclusiveLock);

    exec_policy_funct_on_other_node(sql_cmd);

    PG_RETURN_BOOL(true);
}

Datum
disable_policy(PG_FUNCTION_ARGS)
{
    Relation    rel;
    HeapTuple   tup;
    HeapTuple   new_tup;
    char        *object_schema;
    char        *object_name;
    char        *policy_name;
    text        *in_string;

    Datum       values[Natts_audit_fga_conf];
    bool        nulls[Natts_audit_fga_conf];
    bool        replaces[Natts_audit_fga_conf];

    char *sql_cmd = pstrdup(debug_query_string);

    if (!is_single_cmd(sql_cmd))
    {
        elog(ERROR, "disable_policy function cannot used with other command;");
    }

    /* only adt_admin can add policy */
    if ((Oid)DEFAULT_ROLE_AUDIT_SYS_USERID != GetUserId())
    {
        elog(ERROR, "Only audit admin can do this");
    }
    
    // check object_schema
    if ( !PG_ARGISNULL(0) )
    {
        in_string = PG_GETARG_TEXT_PP(0);
        object_schema = pnstrdup(VARDATA_ANY(in_string), VARSIZE_ANY_EXHDR(in_string));
        elog(LOG, "str = \"%s\"", object_schema);
    }
    else
    {
        elog(ERROR, "missing object schema name");
    }

    // check object_name
    if ( !PG_ARGISNULL(1) )
    {
        in_string = PG_GETARG_TEXT_PP(1);
        object_name = pnstrdup(VARDATA_ANY(in_string), VARSIZE_ANY_EXHDR(in_string));
        elog(LOG, "str = \"%s\"", object_name);
    }
    else
    {
        elog(ERROR, "missing object name");
    }

    // check policy_name
    if ( !PG_ARGISNULL(2) )
    {
        in_string = PG_GETARG_TEXT_PP(2);
        policy_name = pnstrdup(VARDATA_ANY(in_string), VARSIZE_ANY_EXHDR(in_string));
        elog(LOG,  "str = \"%s\"",  policy_name);            
    }
    else
    {
        elog(ERROR,  "missing policy name");
    }

    rel = heap_open(PgAuditFgaConfRelationId, RowExclusiveLock);
    tup = SearchSysCache1(AUDITFGAPOLICYCONF, CStringGetDatum(policy_name));

    if(!HeapTupleIsValid(tup))
        elog(ERROR,"policy[%s] is not exist!", policy_name);

    MemSet(nulls, false, sizeof(nulls));
    MemSet(values, 0, sizeof(values));
    MemSet(replaces, false, sizeof(replaces));

    replaces[Anum_audit_fga_conf_audit_enable - 1] = true;
    values[Anum_audit_fga_conf_audit_enable - 1] = BoolGetDatum(false);

    new_tup = heap_modify_tuple(tup, RelationGetDescr(rel), values,
                                 nulls, replaces);

    CatalogTupleUpdate(rel, &new_tup->t_self, new_tup);

    ReleaseSysCache(tup);
    
    heap_close(rel, RowExclusiveLock);

    exec_policy_funct_on_other_node(sql_cmd);

    PG_RETURN_BOOL(true);
}

Datum
enable_all_policy(PG_FUNCTION_ARGS)
{
    Relation    rel;
    HeapTuple   tup;
    HeapTuple   new_tup;

    SysScanDesc scan;

    Datum       values[Natts_audit_fga_conf];
    bool        nulls[Natts_audit_fga_conf];
    bool        replaces[Natts_audit_fga_conf];

    char *sql_cmd = pstrdup(debug_query_string);

    if (!is_single_cmd(sql_cmd))
    {
        elog(ERROR, "enable_policy function cannot used with other command;");
    }

    /* only adt_admin can add policy */
    if ((Oid)DEFAULT_ROLE_AUDIT_SYS_USERID != GetUserId())
    {
        elog(ERROR, "Only audit admin can do this");
    }

    rel = heap_open(PgAuditFgaConfRelationId, RowExclusiveLock);    
    scan = systable_beginscan(rel, InvalidOid, false, NULL, 0, NULL);

    tup = systable_getnext(scan);

    while(HeapTupleIsValid(tup))
    {
        MemSet(values, 0, sizeof(values));
        MemSet(nulls, false, sizeof(nulls));
        MemSet(replaces, false, sizeof(replaces));

        replaces[Anum_audit_fga_conf_audit_enable - 1] = true;
        values[Anum_audit_fga_conf_audit_enable - 1] = BoolGetDatum(true);

        new_tup = heap_modify_tuple(tup, RelationGetDescr(rel), values,
                                     nulls, replaces);

        CatalogTupleUpdate(rel, &new_tup->t_self, new_tup);

        tup = systable_getnext(scan);
    }    
    
    systable_endscan(scan);
    
    heap_close(rel, RowExclusiveLock);

    exec_policy_funct_on_other_node(sql_cmd);

    PG_RETURN_BOOL(true);
}

Datum
disable_all_policy(PG_FUNCTION_ARGS)
{
    Relation    rel;
    HeapTuple   tup;
    HeapTuple   new_tup;

    SysScanDesc scan;

    Datum       values[Natts_audit_fga_conf];
    bool        nulls[Natts_audit_fga_conf];
    bool        replaces[Natts_audit_fga_conf];

    char *sql_cmd = pstrdup(debug_query_string);

    if (!is_single_cmd(sql_cmd))
    {
        elog(ERROR, "disable_policy function cannot used with other command;");
    }

    /* only adt_admin can add policy */
    if ((Oid)DEFAULT_ROLE_AUDIT_SYS_USERID != GetUserId())
    {
        elog(ERROR, "Only audit admin can do this");
    }

    rel = heap_open(PgAuditFgaConfRelationId, RowExclusiveLock);
    scan = systable_beginscan(rel, InvalidOid, false, NULL, 0, NULL);

    tup = systable_getnext(scan);

    while(HeapTupleIsValid(tup))
    {
        MemSet(values, 0, sizeof(values));
        MemSet(nulls, false, sizeof(nulls));
        MemSet(replaces, false, sizeof(replaces));

        replaces[Anum_audit_fga_conf_audit_enable - 1] = true;
        values[Anum_audit_fga_conf_audit_enable - 1] = BoolGetDatum(false);

        new_tup = heap_modify_tuple(tup, RelationGetDescr(rel), values,
                                     nulls, replaces);

        CatalogTupleUpdate(rel, &new_tup->t_self, new_tup);

        tup = systable_getnext(scan);
    }

    systable_endscan(scan);

    heap_close(rel, RowExclusiveLock);

    exec_policy_funct_on_other_node(sql_cmd);

    PG_RETURN_BOOL(true);
}

Datum
drop_all_policy(PG_FUNCTION_ARGS)
{
    Relation    rel;
    HeapTuple   tup;
    SysScanDesc scan;

    char *sql_cmd = pstrdup(debug_query_string);

    if (!is_single_cmd(sql_cmd))
    {
        elog(ERROR, "disable_policy function cannot used with other command;");
    }

    /* only adt_admin can add policy */
    if ((Oid)DEFAULT_ROLE_AUDIT_SYS_USERID != GetUserId())
    {
        elog(ERROR, "Only audit admin can do this");
    }

    rel = heap_open(PgAuditFgaConfRelationId, RowExclusiveLock);
    scan = systable_beginscan(rel, InvalidOid, false, NULL, 0, NULL);

    tup = systable_getnext(scan);

    while(HeapTupleIsValid(tup))
    {
        simple_heap_delete(rel, &tup->t_self);

        tup = systable_getnext(scan);
    }

    systable_endscan(scan);

    heap_close(rel, RowExclusiveLock);

    exec_policy_funct_on_other_node(sql_cmd);

    PG_RETURN_BOOL(true);
}


bool 
has_policy_matched_cmd(char * cmd_type, Datum statement_types_datum, bool is_null)
{
    bool        ret = false;
    char        *statement_types;
    char        *default_type = "select";

    statement_types = lowerstr(DatumGetCString(statement_types_datum));

    if (is_null)
    {
        if(strstr(statement_types, default_type))
            ret = true;
    }
    else
    {
        if(strstr(statement_types, lowerstr(cmd_type)))
            ret = true; 
    }

    return ret;
}

bool 
has_policy_matched_columns(List * tlist, oidvector *audit_column_oids, bool audit_column_opts)
{// #lizard forgives
    bool        ret = false;
    ListCell    *target_column;
    TargetEntry * col_expr;
    Var * expr;

    if ((audit_column_oids == NULL) || (tlist == NULL))
         ret = true; // means audit all  

    /* audit_column_opts:
        * true: all columns matched
        * false: any columns matched
        * false default
        */
    if (audit_column_opts)
    {
        ret = true;
        foreach(target_column, tlist)
        {   
            col_expr = (TargetEntry *) lfirst(target_column);

            expr = (Var *)col_expr->expr;
            if (!IsA(expr, Var))
            {
                ret = false;
                break;
            }

            if (!oidvector_member( audit_column_oids, (Oid) expr->varattno))
            {
                ret = false;
                break;
            }
        }
    }
    else
    {
        foreach(target_column, tlist)
        {
            col_expr = (TargetEntry *) lfirst(target_column);

            expr = (Var *)col_expr->expr;
            if (!IsA(expr, Var))
            {
                ret = false;
                break;
            }
            if (oidvector_member( audit_column_oids, (Oid) expr->varattno))
                ret = true;
        }
    }

    return ret;            
}


bool  
get_audit_fga_quals(Oid rel, char * cmd_type, List *tlist, List **audit_fga_policy_list)
{// #lizard forgives
    Relation    audit_fga_rel;
    ScanKeyData skey[3];
    SysScanDesc sscan;
    HeapTuple    policy_tuple;
    
    AuditFgaPolicy * audit_fga_policy_item;
    Expr *      qual_expr = NULL;

    bool        need_audit_fga_quals = false;
    int         nfga = 0;

    audit_fga_rel = heap_open(PgAuditFgaConfRelationId, AccessShareLock);

    /* Add key - policy's relation id. */
    ScanKeyInit(&skey[0],
                Anum_audit_fga_conf_object_schema,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(get_rel_namespace(rel)));

    /* Add key - policy's name. */
    ScanKeyInit(&skey[1],
                Anum_audit_fga_conf_object_id,
                BTEqualStrategyNumber, F_OIDEQ, 
                ObjectIdGetDatum(rel));

    ScanKeyInit(&skey[2],
                Anum_audit_fga_conf_audit_enable,
                BTEqualStrategyNumber, F_BOOLEQ, 
                BoolGetDatum(true));
    
    sscan = systable_beginscan(audit_fga_rel,
                               AuditFgaConfObjschOjbPolicyNameIndexID, false, NULL, 3,
                               skey);

    /*
     * If we don't find a valid HeapTuple, it must mean No policy
     * exist for object
     */
    while (HeapTupleIsValid(policy_tuple = systable_getnext(sscan)))
    {
        Datum        statement_types_datum;
        bool        isNull;

        Datum        qual_datum;
        Datum       policy_name_datum;
        char        *qual_value;
        char        *policy_name;
        bool        qual_is_null;
        bool        policy_name_is_null;

        Datum        column_datum;
        Datum        column_opts_datum;
        bool        column_is_null;
        bool        column_opts_is_null;
        oidvector   *audit_column_oids = NULL;
        bool        audit_column_opts = true;
        
        /* get audit_columns  */
        statement_types_datum = heap_getattr(policy_tuple, Anum_audit_fga_conf_statement_types,
                            RelationGetDescr(audit_fga_rel), &isNull);

        column_datum = heap_getattr(policy_tuple, Anum_audit_fga_conf_audit_column_ids,
                            RelationGetDescr(audit_fga_rel), &column_is_null);
        column_opts_datum = heap_getattr(policy_tuple, Anum_audit_fga_conf_audit_column_opts,
                                        RelationGetDescr(audit_fga_rel), &column_opts_is_null);

        qual_datum = heap_getattr(policy_tuple, Anum_audit_fga_conf_audit_condition,
                            RelationGetDescr(audit_fga_rel), &qual_is_null);
        
        policy_name_datum = heap_getattr(policy_tuple, Anum_audit_fga_conf_policy_name,
                            RelationGetDescr(audit_fga_rel), &policy_name_is_null);
                   
        /*  check command type */
        need_audit_fga_quals = has_policy_matched_cmd(cmd_type, statement_types_datum, isNull);

        if (!column_is_null)
            audit_column_oids = (oidvector *) DatumGetPointer(column_datum);

        if (!column_opts_is_null)   
            audit_column_opts = DatumGetBool(column_opts_datum);
        
        /*  check column list */
        if (need_audit_fga_quals)
        {
            need_audit_fga_quals = has_policy_matched_columns(tlist, audit_column_oids, audit_column_opts);    
        }
        else
            continue;

        if (need_audit_fga_quals)
            nfga++;
        else
            continue;

        /*  check audit condition expr */
        policy_name = DatumGetCString(policy_name_datum);

        audit_fga_policy_item = makeNode(AuditFgaPolicy);
        audit_fga_policy_item->policy_name = pstrdup(policy_name);
        audit_fga_policy_item->query_string = pstrdup(debug_query_string);
        
        if (qual_datum)
        {
            qual_value = TextDatumGetCString(qual_datum);
            qual_expr = copyObject((Expr *) stringToNode(qual_value));
            
            audit_fga_policy_item->qual = 
                lappend(audit_fga_policy_item->qual, (Node *) qual_expr);


            *audit_fga_policy_list = lappend(*audit_fga_policy_list, audit_fga_policy_item);
        }
        else
        {
            audit_fga_policy_item->qual = NULL;
            *audit_fga_policy_list = lappend(*audit_fga_policy_list, audit_fga_policy_item);
        }
    }

    systable_endscan(sscan);
    heap_close(audit_fga_rel, AccessShareLock);

    if (nfga > 0)
        return true;
    else
        return false;
}


/*
 * Signal handler for SIGHUP
 *        Set a flag to tell the main loop to reread the config file, and set
 *        our latch to wake it up.
 */
void
worker_audit_fga_sighup(SIGNAL_ARGS)
{
    int            save_errno = errno;

    fga_gotSIGHUP = true;
    SetLatch(MyLatch);

    errno = save_errno;
}

void
worker_audit_fga_wakeup(SIGNAL_ARGS)
{
    int            save_errno = errno;

    fga_gotSIGUSR1 = true;
    fga_consume_requested = true;
    SetLatch(MyLatch);

    errno = save_errno;
}


void reset_shem_info(int i)
{
    BackendAuditFgaArray[i].backend_pid = 0;
    BackendAuditFgaArray[i].status = FGA_STATUS_INIT;
    BackendAuditFgaArray[i].handler_module = 0;
    MemSet(BackendAuditFgaArray[i].exec_feedback, 0, AUDIT_TRIGGER_FEEDBACK_LEN);
    MemSet(BackendAuditFgaArray[i].db_name, 0, NAMEDATALEN);
    MemSet(BackendAuditFgaArray[i].user_name, 0, NAMEDATALEN);
    MemSet(BackendAuditFgaArray[i].func_name, 0, NAMEDATALEN);
    MemSet(BackendAuditFgaArray[i].host, 0, 32);
    MemSet(BackendAuditFgaArray[i].port, 0, 32);
}

/*
  * process fga trigger function
  */
void 
process_fga_trigger(bool timeout)
{// #lizard forgives
    PGconn         *conn;
    audit_fga_tigger_info func_info;
    PGresult *res;
    int i;
    char stmt[1024];

    if (fga_consume_requested)
    {           
        /* get task one by one from shemem */
        for (i = 0; i < num_audit_fga_tigger_info; i++)
        {
            if (BackendAuditFgaArray[i].backend_pid != 0 && 
                BackendAuditFgaArray[i].status == FGA_STATUS_INIT)
            {
                func_info = BackendAuditFgaArray[i];

                snprintf(stmt, 1024, "select %s()", BackendAuditFgaArray[i].func_name);
               

                conn = conn_database(func_info.host, func_info.port, func_info.user_name, func_info.db_name);
                if (conn && PQstatus(conn) == CONNECTION_OK)
                {
                    BackendAuditFgaArray[i].status = FGA_STATUS_DOING;
                    res = PQexec(conn, stmt);

                    elog(LOG, "AUDIT_FGA: call function %s", stmt);

                    if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK ||
                            PQgetisnull(res, 0, 0))
                    {
                        elog(LOG, "AUDIT_FGA: call function %s error, %s", stmt, PQerrorMessage(conn));
                    }
                    else
                    {
                        BackendAuditFgaArray[i].status = FGA_STATUS_OK;
                        strncpy(BackendAuditFgaArray[i].exec_feedback, "succeed",AUDIT_TRIGGER_FEEDBACK_LEN-1);
                    }

                    PQclear(res);
                    PQfinish(conn);
                    
                    reset_shem_info(i);
                }
                else
                {
                    elog(LOG, "AUDIT_FGA: cannot connect to db");
	                PQfinish(conn);
                }
            }
        }
    }
}


/*
 * Main loop for the apply launcher process.
 */
void
ApplyAuditFgaMain(Datum main_arg)
{   
    ereport(DEBUG1,
            (errmsg("audit fga worker started")));

    /* Establish signal handlers. */    
    pqsignal(SIGHUP, worker_audit_fga_sighup);
    pqsignal(SIGTERM, die);
    pqsignal(SIGUSR1, worker_audit_fga_wakeup); 
    BackgroundWorkerUnblockSignals();

    /*
     * Establish connection to nailed catalogs (we only ever access
     * pg_subscription).
     */
    //BackgroundWorkerInitializeConnection(NULL, NULL);

    /* Enter main loop */
    while(PostmasterIsAlive())
    {
        int    rc = 0;

        CHECK_FOR_INTERRUPTS();

        /* Clear any already-pending wakeups */
        ResetLatch(MyLatch);
        
        process_fga_trigger(true);
        fga_consume_requested = false;

        rc = WaitLatch(MyLatch,
                       WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                       FGA_LATCH_MICROSEC,
                       WAIT_EVENT_AUDIT_FGA_MAIN);

        if (rc & WL_POSTMASTER_DEATH)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("audit fga worker exit after postmaster die")));
            exit(6);
        }
        else if (rc & WL_TIMEOUT)
        {
            process_fga_trigger(false);
        }
    }
    /* Not reachable */
}


/*
 * ApplyAuditFgaRegister
 *        Register a background worker running the Audit FGA warning.
 */

void
ApplyAuditFgaRegister(void)
{
    BackgroundWorker bgw;

    memset(&bgw, 0, sizeof(bgw));
    bgw.bgw_flags = BGWORKER_CLASS_AUDIT_FGA | BGWORKER_SHMEM_ACCESS |
        BGWORKER_BACKEND_DATABASE_CONNECTION;
    bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
    snprintf(bgw.bgw_library_name, BGW_MAXLEN, "postgres");
    snprintf(bgw.bgw_function_name, BGW_MAXLEN, "ApplyAuditFgaMain");
    snprintf(bgw.bgw_name, BGW_MAXLEN,
             "audit fga worker");
    bgw.bgw_restart_time = 5;
    bgw.bgw_notify_pid = 0;
    bgw.bgw_main_arg = (Datum) 0;

    RegisterBackgroundWorker(&bgw);
}

Size
AuditFgaShmemSize(void)
{
    Size        size;

    size = mul_size(sizeof(audit_fga_tigger_info), num_audit_fga_tigger_info);

    return size;
}

void
AuditFgaShmemInit(void)
{
    
    Size        size;
    bool        found;

    /* Create or attach to the shared array */
    size = AuditFgaShmemSize();
    BackendAuditFgaArray = (audit_fga_tigger_info *)
        ShmemInitStruct("Backend FGA Trigger Buffer", size, &found);

    if (!found)
    {
        /*
              * We're the first - initialize.
             */
        MemSet(BackendAuditFgaArray, 0, size);
    }
}

void
write_trigger_handle_to_shmem(Oid func_oid)
{
    audit_fga_tigger_info func_info;
    char port_s[32];
    int idx = -1;
    
    snprintf(port_s, 32, "%d", get_pgxc_nodeport(MyCoordId));
    
    func_info.backend_pid = MyProcPid;
    func_info.handler_module = func_oid;
    func_info.status = FGA_STATUS_INIT; 
    strncpy(func_info.db_name, get_database_name(MyDatabaseId),NAMEDATALEN);
    strncpy(func_info.user_name, GetUserNameFromId(GetSessionUserId(), false),NAMEDATALEN);
    strncpy(func_info.func_name, get_func_name(func_oid),NAMEDATALEN);
    strncpy(func_info.host, get_pgxc_nodehost(MyCoordId),32);
    strncpy(func_info.port, port_s,32);
    
    MemSet(func_info.exec_feedback, 0, AUDIT_TRIGGER_FEEDBACK_LEN);

    idx = MyProc->pgprocno;
    elog(LOG, "proc idx: %d", idx);
    Assert(idx >= 0 && idx < MaxBackends);

    if (BackendAuditFgaArray[idx].backend_pid == 0)
    {
        memcpy(&BackendAuditFgaArray[idx], &func_info, sizeof(audit_fga_tigger_info));
        if (!fga_consume_requested)
        {
            SendPostmasterSignal(PMSIGNAL_WAKEN_AUDIT_FGA_TRIGGER);
        }
    }

    return ;
}


