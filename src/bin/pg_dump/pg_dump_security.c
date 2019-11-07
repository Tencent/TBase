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
 * pg_dumpall.c
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * pg_dumpall forces all pg_dump output to be text, since it also outputs
 * text into the same output stream.
 *
 * src/bin/pg_dump/pg_dump_security.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <time.h>
#include <unistd.h>

#include "getopt_long.h"

#include "dumputils.h"
#include "pg_backup.h"
#include "common/file_utils.h"
#include "fe_utils/string_utils.h"

#include "catalog/pg_authid.h"

/* version string we expect back from pg_dump */
#define PGDUMP_VERSIONSTR "pg_dump (TBase) " PG_VERSION "\n"

static void help(void);

static void dumpTimestamp(const char *msg);


static PGconn *connectDatabase(const char *dbname, const char *connstr, const char *pghost, const char *pgport,
                const char *pguser, trivalue prompt_password, bool fail_on_error);
static char *constructConnStr(const char **keywords, const char **values);
static PGresult *executeQuery(PGconn *conn, const char *query);
static void executeCommand(PGconn *conn, const char *query);

static const char *progname;
static PQExpBuffer pgdumpopts;
static char *connstr = "";
static bool verbose = false;
static bool dosync = true;

static int    server_version;

#define PG_AUTHID "pg_authid"
#define PG_ROLES  "pg_roles "

static FILE *OPF;
static char *filename = NULL;

#define exit_nicely(code) exit(code)

static void dump_pg_data_mask_map(PGconn *conn);
static void dump_pg_data_mask_user(PGconn *conn);
static void dump_pg_transparent_crypt_policy_algorithm(PGconn *conn);
static void dump_pg_transparent_crypt_policy_map(PGconn *conn);

int
main(int argc, char *argv[])
{// #lizard forgives
    static struct option long_options[] = {

        {"file", required_argument, NULL, 'f'},
        {"host", required_argument, NULL, 'h'},
        {"database", required_argument, NULL, 'l'},
        {"port", required_argument, NULL, 'p'},
        {"verbose", no_argument, NULL, 'v'},
        {"password", no_argument, NULL, 'W'},

        {NULL, 0, NULL, 0}
    };

    char       *pghost = NULL;
    char       *pgport = NULL;
    char       *pguser = MLS_USER;
    char       *pgdb = NULL;
    char       *use_role = NULL;
    trivalue    prompt_password = TRI_DEFAULT;
    PGconn       *conn;
    int            encoding;
    const char *std_strings;
    int            optindex;
    int            c;

    set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("pg_dump"));

    progname = get_progname(argv[0]);

    if (argc > 1)
    {
        if ((strcmp(argv[1], "--help") == 0) || (strcmp(argv[1], "-?") == 0))
        {
            help();
            exit_nicely(0);
        }
        if ((strcmp(argv[1], "--version") == 0) || (strcmp(argv[1], "-V") == 0))
        {
            puts("pg_dump_security (TBase) " PG_VERSION);
            exit_nicely(0);
        }
    }

    pgdumpopts = createPQExpBuffer();

    while ((c = getopt_long(argc, argv, "f:h:l:p:v", long_options, &optindex)) != -1)
    {
        switch (c)
        {
            case 'f':
                filename = pg_strdup(optarg);
                appendPQExpBufferStr(pgdumpopts, " -f ");
                appendShellString(pgdumpopts, filename);
                break;

            case 'h':
                pghost = pg_strdup(optarg);
                break;

            case 'l':
                pgdb = pg_strdup(optarg);
                break;

            case 'p':
                pgport = pg_strdup(optarg);
                break;

            case 'v':
                verbose = true;
                appendPQExpBufferStr(pgdumpopts, " -v");
                break;

            default:
                fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
                exit_nicely(1);
        }
    }

    /* Complain if any arguments remain */
    if (optind < argc)
    {
        fprintf(stderr, _("%s: too many command-line arguments (first is \"%s\")\n"),
                progname, argv[optind]);
        fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
                progname);
        exit_nicely(1);
    }

    /*
     * If there was a database specified on the command line, use that,
     * otherwise try to connect to database "postgres", and failing that
     * "template1".  "postgres" is the preferred choice for 8.1 and later
     * servers, but it usually will not exist on older ones.
     */
    if (pgdb)
    {
        conn = connectDatabase(pgdb, NULL, pghost, pgport, pguser,
                               prompt_password, false);

        if (!conn)
        {
            fprintf(stderr, _("%s: could not connect to database \"%s\" \n"),
                    progname, pgdb);
            exit_nicely(1);
        }
    }
    else
    {
        conn = connectDatabase("postgres", connstr, pghost, pgport, pguser,
                               prompt_password, false);
        if (!conn)
        {
            conn = connectDatabase("template1", connstr, pghost, pgport, pguser,
                                   prompt_password, true);
        }
        if (!conn)
        {
            fprintf(stderr, _("%s: could not connect to databases \"postgres\" or \"template1\"\n"
                              "Please specify an alternative database.\n"),
                    progname);
            fprintf(stderr, _("Try \"%s --help\" for more information.\n "),
                    progname);
            exit_nicely(1);
        }
    }

    /*
     * Open the output file if required, otherwise use stdout
     */
    if (filename)
    {
        OPF = fopen(filename, PG_BINARY_W);
        if (!OPF)
        {
            fprintf(stderr, _("%s: could not open the output file \"%s\": %s\n "),
                    progname, filename, strerror(errno));
            exit_nicely(1);
        }
    }
    else
    {
        OPF = stdout;
    }

    /*
     * Get the active encoding and the standard_conforming_strings setting, so
     * we know how to escape strings.
     */
    encoding = PQclientEncoding(conn);
    std_strings = PQparameterStatus(conn, "standard_conforming_strings");
    if (!std_strings)
    {
        std_strings = "off";
    }

    /* Set the role if requested */
    if (use_role && server_version >= 80100)
    {
        PQExpBuffer query = createPQExpBuffer();

        appendPQExpBuffer(query, "SET ROLE %s", fmtId(use_role));
        executeCommand(conn, query->data);
        destroyPQExpBuffer(query);
    }

    /* Force quoting of all identifiers if requested. */
    if (quote_all_identifiers && server_version >= 90100)
        executeCommand(conn, "SET quote_all_identifiers = true");

    fprintf(OPF, "--\n-- TBase security dump\n--\n\n");
    if (verbose)
        dumpTimestamp("Started on");

    /*
     * We used to emit \connect postgres here, but that served no purpose
     * other than to break things for installations without a postgres
     * database.  Everything we're restoring here is a global, so whichever
     * database we're connected to at the moment is fine.
     */

    /* Restore will need to write to the target cluster */
    fprintf(OPF, "SET default_transaction_read_only = off;\n\n");

    /* Replicate encoding and std_strings in output */
    fprintf(OPF, "SET client_encoding = '%s';\n",
            pg_encoding_to_char(encoding));
    fprintf(OPF, "SET standard_conforming_strings = %s;\n", std_strings);
    if (strcmp(std_strings, "off") == 0)
        fprintf(OPF, "SET escape_string_warning = off;\n");
    fprintf(OPF, "\n");

    fprintf(OPF, "SET allow_dml_on_datanode = on; \n");

    /* package all in one transaction */
    fprintf(OPF, "BEGIN; \n");

    dump_pg_data_mask_map(conn);
    dump_pg_data_mask_user(conn);
    dump_pg_transparent_crypt_policy_algorithm(conn);
    dump_pg_transparent_crypt_policy_map(conn);
    
    PQfinish(conn);

    /* package all in one transaction */
    fprintf(OPF, "COMMIT; \n");

    if (verbose)
        dumpTimestamp("Completed on");
    fprintf(OPF, "--\n-- TBase security dump complete\n--\n\n");

    if (filename)
    {
        fclose(OPF);

        /* sync the resulting file, errors are not fatal */
        if (dosync)
            (void) fsync_fname(filename, false, progname);
    }

    exit_nicely(0);
}


static void
help(void)
{
    printf(_("%s extracts a TBase database into an SQL script file.\n\n"), progname);
    printf(_("Usage:\n"));
    printf(_("  %s [OPTION]...\n"), progname);

    printf(_("\nGeneral options:\n"));
    
    printf(_("  -v, --verbose                verbose mode\n"));
    printf(_("  -V, --version                output version information, then exit\n"));
    printf(_("  -?, --help                   show this help, then exit\n"));
    
    printf(_("\nConnection options:\n"));
    printf(_("  -h, --host=HOSTNAME      database server host or socket directory\n"));
    printf(_("  -l, --database=DBNAME    alternative default database\n"));
    printf(_("  -p, --port=PORT          database server port number\n"));
    printf(_("  -f, --file=FILENAME          output file name\n"));
    
    printf(_("\nIf -f/--file is not used, then the SQL script will be written to the standard\n"
             "output.\n\n"));
    printf(_("Report bugs to <pgsql-bugs@postgresql.org>.\n"));
}

/*
 * Make a database connection with the given parameters.  An
 * interactive password prompt is automatically issued if required.
 *
 * If fail_on_error is false, we return NULL without printing any message
 * on failure, but preserve any prompted password for the next try.
 *
 * On success, the global variable 'connstr' is set to a connection string
 * containing the options used.
 */
static PGconn *
connectDatabase(const char *dbname, const char *connection_string,
                const char *pghost, const char *pgport, const char *pguser,
                trivalue prompt_password, bool fail_on_error)
{// #lizard forgives
    PGconn       *conn;
    bool        new_pass;
    const char *remoteversion_str;
    int            my_version;
    const char **keywords = NULL;
    const char **values = NULL;
    PQconninfoOption *conn_opts = NULL;
    static bool have_password = false;
    static char password[100];

    if (prompt_password == TRI_YES && !have_password)
    {
        simple_prompt("Password: ", password, sizeof(password), false);
        have_password = true;
    }

    /*
     * Start the connection.  Loop until we have a password if requested by
     * backend.
     */
    do
    {
        int            argcount = 6;
        PQconninfoOption *conn_opt;
        char       *err_msg = NULL;
        int            i = 0;

        if (keywords)
            free(keywords);
        if (values)
            free(values);
        if (conn_opts)
            PQconninfoFree(conn_opts);

        /*
         * Merge the connection info inputs given in form of connection string
         * and other options.  Explicitly discard any dbname value in the
         * connection string; otherwise, PQconnectdbParams() would interpret
         * that value as being itself a connection string.
         */
        if (connection_string)
        {
            conn_opts = PQconninfoParse(connection_string, &err_msg);
            if (conn_opts == NULL)
            {
                fprintf(stderr, "%s: %s", progname, err_msg);
                exit_nicely(1);
            }

            for (conn_opt = conn_opts; conn_opt->keyword != NULL; conn_opt++)
            {
                if ((conn_opt->val != NULL) && (conn_opt->val[0] != '\0') &&
                    strcmp(conn_opt->keyword, "dbname") != 0)
                {
                    argcount++;
                }
            }

            values = pg_malloc0((argcount + 1) * sizeof(*values));
            keywords = pg_malloc0((argcount + 1) * sizeof(*keywords));

            for (conn_opt = conn_opts; conn_opt->keyword != NULL; conn_opt++)
            {
                if (conn_opt->val != NULL && (conn_opt->val[0] != '\0') &&
                    strcmp(conn_opt->keyword, "dbname") != 0)
                {
                    values[i] = conn_opt->val;
                    keywords[i] = conn_opt->keyword;
                    i++;
                }
            }
        }
        else
        {
            values = pg_malloc0((argcount + 1) * sizeof(*values));
            keywords = pg_malloc0((argcount + 1) * sizeof(*keywords));
        }

        if (pghost)
        {
            keywords[i] = "host";
            values[i] = pghost;
            i++;
        }
        if (pgport)
        {
            keywords[i] = "port";
            values[i] = pgport;
            i++;
        }
        if (pguser)
        {
            keywords[i] = "user";
            values[i] = pguser;
            i++;
        }
        if (have_password)
        {
            keywords[i] = "password";
            values[i] = password;
            i++;
        }
        if (dbname)
        {
            keywords[i] = "dbname";
            values[i] = dbname;
            i++;
        }
        keywords[i] = "fallback_application_name";
        values[i] = progname;
        i++;

        new_pass = false;
        conn = PQconnectdbParams(keywords, values, true);

        if (!conn)
        {
            fprintf(stderr, _("%s: could not connect to database \"%s\"\n"),
                    progname, dbname);
            exit_nicely(1);
        }

        if (PQstatus(conn) == CONNECTION_BAD &&
            PQconnectionNeedsPassword(conn) &&
            !have_password &&
            prompt_password != TRI_NO)
        {
            PQfinish(conn);
            simple_prompt("Password: ", password, sizeof(password), false);
            new_pass = true;
            have_password = true;
        }
    } while (new_pass);

    /* check to see that the backend connection was successfully made */
    if (PQstatus(conn) == CONNECTION_BAD)
    {
        if (fail_on_error)
        {
            fprintf(stderr,
                    _("%s: could not connect to database \"%s\": %s\n ") ,
                    progname, dbname, PQerrorMessage(conn));
            exit_nicely(1);
        }
        else
        {
            PQfinish(conn);

            free(values);
            free(keywords);
            PQconninfoFree(conn_opts);

            return NULL;
        }
    }

    /*
     * Ok, connected successfully. Remember the options used, in the form of a
     * connection string.
     */
    connstr = constructConnStr(keywords, values);

    free(values);
    free(keywords);
    PQconninfoFree(conn_opts);

    /* Check version */
    remoteversion_str = PQparameterStatus(conn, "server_version");
    if ( !remoteversion_str )
    {
        fprintf(stderr, _("%s: could not get server version\n"), progname);
        exit_nicely(1);
    }
    server_version = PQserverVersion(conn);
    if ( server_version == 0 )
    {
        fprintf(stderr, _("%s: could not parse server version \"%s\"\n" ),
                progname, remoteversion_str);
        exit_nicely(1);
    }

    my_version = PG_VERSION_NUM;

    /*
     * We allow the server to be back to 8.0, and up to any minor release of
     * our own major version.  (See also version check in pg_dump.c.)
     */
    if ((my_version != server_version)
        && (server_version < 80000 ||
            (server_version / 100) > (my_version / 100)))
    {
        fprintf(stderr, _("server version: %s; %s version: %s\n"),
                remoteversion_str, progname, PG_VERSION);
        fprintf(stderr, _("aborting because of server version mismatch\n"));
        exit_nicely(1);
    }

    /*
     * Make sure we are not fooled by non-system schemas in the search path.
     */
    executeCommand(conn, "SET search_path = pg_catalog");

    return conn;
}

/* ----------
 * Construct a connection string from the given keyword/value pairs. It is
 * used to pass the connection options to the pg_dump subprocess.
 *
 * The following parameters are excluded:
 *    dbname        - varies in each pg_dump invocation
 *    password    - it's not secure to pass a password on the command line
 *    fallback_application_name - we'll let pg_dump set it
 * ----------
 */
static char *
constructConnStr(const char **keywords, const char **values)
{
    PQExpBuffer buf = createPQExpBuffer();
    char       *connstr;
    int            i;
    bool        firstkeyword = true;

    /* Construct a new connection string in key='value' format. */
    for (i = 0; keywords[i] != NULL; i++)
    {
        if (strcmp(keywords[i], "dbname") == 0 ||
            strcmp(keywords[i], "password") == 0 ||
            strcmp(keywords[i], "fallback_application_name") == 0)
            continue;

        if (!firstkeyword)
            appendPQExpBufferChar(buf, ' ');
        firstkeyword = false;
        appendPQExpBuffer(buf, "%s=", keywords[i]);
        appendConnStrVal(buf, values[i]);
    }

    connstr = pg_strdup(buf->data);
    destroyPQExpBuffer(buf);
    return connstr;
}

/*
 * Run a query, return the results, exit program on failure.
 */
static PGresult *
executeQuery(PGconn *conn, const char *query)
{
    PGresult   *res;

    if (verbose)
        fprintf(stderr, _("%s: executing %s\n"), progname, query);

    res = PQexec(conn, query);
    if (!res || (PQresultStatus(res) != PGRES_TUPLES_OK))
    {
        fprintf(stderr, _("%s: query failed: %s "),
                progname, PQerrorMessage(conn) );
        fprintf(stderr, _("%s: query was: %s \n"),
                progname, query);
        PQfinish(conn);
        exit_nicely(1);
    }

    return res;
}

/*
 * As above for a SQL command (which returns nothing).
 */
static void
executeCommand(PGconn *conn, const char *query)
{
    PGresult   *res;

    if (verbose)
    {
        fprintf(stderr, _("%s: executing %s\n"), progname, query);
    }

    res = PQexec(conn, query);
    if (!res || (PQresultStatus(res) != PGRES_COMMAND_OK))
    {
        fprintf(stderr, _("%s: query failed: %s" ),
                progname, PQerrorMessage(conn));
        fprintf(stderr, _("%s: query was: %s\n" ),
                progname, query);
        PQfinish(conn);
        exit_nicely(1);
    }

    PQclear(res);
    return;
}


/*
 * dumpTimestamp
 */
static void
dumpTimestamp(const char *msg)
{
    char        buf[64];
    time_t        now = time(NULL);

    if (strftime(buf, sizeof(buf), PGDUMP_STRFTIME_FMT, localtime(&now)) != 0)
        fprintf(OPF, "-- %s %s\n\n", msg, buf);
    return;
}

static void dump_pg_data_mask_map(PGconn *conn)
{
    PQExpBuffer query;
    PGresult   *res;
    int            num;
    int            i;

    query = createPQExpBuffer();
    
    appendPQExpBuffer(query,
                        "select 'insert into pg_data_mask_map (relid, attnum, enable, option, datamask, nspname, tblname, maskfunc, defaultval) values ( "
                        "pg_get_table_oid_by_name('''|| nspname ||'.' || tblname || '''),' || attnum || ',' || enable || ',' || option || ',' || datamask || ',''' || "
                        "nspname || ''',''' || tblname || ''',' || maskfunc || ',''' || defaultval || ''');' as stmt "
                        "from pg_data_mask_map x , pg_class c "
                        "where x.relid = c.oid;");

    res = executeQuery(conn, query->data);

    num = PQntuples(res);

    if (num > 0)
        fprintf(OPF, "--\n-- pg_data_mask_map\n--\n\n");

    for (i = 0; i < num; i++)
    {
        fprintf(OPF, "%s\n", PQgetvalue(res, i, PQfnumber(res, "stmt")));
    }
    fprintf(OPF, "\n");

    PQclear(res);
    destroyPQExpBuffer(query);
    return;
}

static void dump_pg_data_mask_user(PGconn *conn)
{
    PQExpBuffer query;
    PGresult   *res;
    int            num;
    int            i;

    query = createPQExpBuffer();
    
    appendPQExpBuffer(query,
                        "select 'insert into pg_data_mask_user (relid, userid, attnum, enable, username, nspname, tblname) values ( "
                        "pg_get_table_oid_by_name('''|| nspname ||'.' || tblname || '''), pg_get_role_oid_by_name ('''||username||''') , '|| attnum || ',' || enable || ',''' || "
                        "username || ''',''' || nspname || ''',''' ||tblname ||''');' as stmt "
                        "from pg_data_mask_user x , pg_class c , pg_authid a "
                        "where x.relid = c.oid and x.userid = a.oid; ");

    res = executeQuery(conn, query->data);

    num = PQntuples(res);

    if (num > 0)
        fprintf(OPF, "--\n-- pg_data_mask_user\n--\n\n");

    for (i = 0; i < num; i++)
    {
        fprintf(OPF, "%s\n", PQgetvalue(res, i, PQfnumber(res, "stmt")));
    }
    fprintf(OPF, "\n");

    PQclear(res);
    destroyPQExpBuffer(query);
    return;
}

static void dump_pg_transparent_crypt_policy_algorithm(PGconn *conn)
{
    PQExpBuffer query;
    PGresult   *res;
    int            num;
    int            i;

    query = createPQExpBuffer();
    
    appendPQExpBuffer(query,
                        "select 'insert into pg_transparent_crypt_policy_algorithm (algorithm_id, option, algorithm_name, encrypt_oid, decrypt_oid, password, pubkey, privatekey, option_args) values ( '|| "
                        "algorithm_id ||','|| option || ',''' || algorithm_name ||''',' || encrypt_oid || ',' || decrypt_oid || ',''' || password || ''',''' || "
                        "case when pubkey is NULL then '' else pubkey end || ''',''' || "
                        "case when privatekey is NULL then '' else privatekey end|| ''',''' || "
                        "case when option_args is NULL then '' else option_args end || ''');' as stmt "
                        "from pg_transparent_crypt_policy_algorithm; ");

    res = executeQuery(conn, query->data);

    num = PQntuples(res);

    if (num > 0)
        fprintf(OPF, "--\n-- pg_transparent_crypt_policy_algorithm\n--\n\n");

    for (i = 0; i < num; i++)
    {
        fprintf(OPF, "%s\n", PQgetvalue(res, i, PQfnumber(res, "stmt")));
    }
    fprintf(OPF, "\n");

    PQclear(res);
    destroyPQExpBuffer(query);
    return;
}


static void dump_pg_transparent_crypt_policy_map(PGconn *conn)
{
    PQExpBuffer query;
    PGresult   *res;
    int            num;
    int            i;

    query = createPQExpBuffer();
    
    appendPQExpBuffer(query,
                        "select 'insert into pg_transparent_crypt_policy_map (relid, attnum, algorithm_id, spcoid, schemaoid, spcname, nspname, tblname) values ( "
                        "pg_get_table_oid_by_name('''|| nspname ||'.' || tblname || '''),' || attnum || ',' || algorithm_id || ',' || spcoid || ',' || schemaoid || ',''' || "
                        "spcname || ''',''' || nspname || ''',''' || tblname || ''');' as stmt "
                        "from pg_transparent_crypt_policy_map x , pg_class c "
                        "where x.relid = c.oid;");

    res = executeQuery(conn, query->data);

    num = PQntuples(res);

    if (num > 0)
        fprintf(OPF, "--\n-- pg_transparent_crypt_policy_map\n--\n\n");

    for (i = 0; i < num; i++)
    {
        fprintf(OPF, "%s\n", PQgetvalue(res, i, PQfnumber(res, "stmt")));
    }
    fprintf(OPF, "\n");

    PQclear(res);
    destroyPQExpBuffer(query);
    return;
}


