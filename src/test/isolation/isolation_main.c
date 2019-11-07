/*-------------------------------------------------------------------------
 *
 * isolation_main --- pg_regress test launcher for isolation tests
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/test/isolation/isolation_main.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include "pg_regress.h"
#include "getopt_long.h"

char        saved_argv0[MAXPGPATH];
char        isolation_exec[MAXPGPATH];
bool        looked_up_isolation_exec = false;

#define PG_ISOLATION_VERSIONSTR "isolationtester (PostgreSQL) " PG_VERSION "\n"
#ifdef __TBASE__
static void check_connection_conf(void);
static void format_isolation_test(void);
static void cmd_exec_result(const char *cmd);
#endif
/*
 * start an isolation tester process for specified file (including
 * redirection), and return process ID
 */
static PID_TYPE
isolation_start_test(const char *testname,
                     _stringlist **resultfiles,
                     _stringlist **expectfiles,
                     _stringlist **tags)
{
    PID_TYPE    pid;
    char        infile[MAXPGPATH];
    char        outfile[MAXPGPATH];
    char        expectfile[MAXPGPATH];
    char        psql_cmd[MAXPGPATH * 3];
    size_t        offset = 0;

    /* need to do the path lookup here, check isolation_init() for details */
    if (!looked_up_isolation_exec)
    {
        /* look for isolationtester binary */
        if (find_other_exec(saved_argv0, "isolationtester",
                            PG_ISOLATION_VERSIONSTR, isolation_exec) != 0)
        {
            fprintf(stderr, _("could not find proper isolationtester binary\n"));
            exit(2);
        }
        looked_up_isolation_exec = true;
    }

    /*
     * Look for files in the output dir first, consistent with a vpath search.
     * This is mainly to create more reasonable error messages if the file is
     * not found.  It also allows local test overrides when running pg_regress
     * outside of the source tree.
     */
    snprintf(infile, sizeof(infile), "%s/specs/%s.spec",
             outputdir, testname);
    if (!file_exists(infile))
        snprintf(infile, sizeof(infile), "%s/specs/%s.spec",
                 inputdir, testname);

    snprintf(outfile, sizeof(outfile), "%s/results/%s.out",
             outputdir, testname);

    snprintf(expectfile, sizeof(expectfile), "%s/expected/%s.out",
             outputdir, testname);
    if (!file_exists(expectfile))
        snprintf(expectfile, sizeof(expectfile), "%s/expected/%s.out",
                 inputdir, testname);

    add_stringlist_item(resultfiles, outfile);
    add_stringlist_item(expectfiles, expectfile);

    if (launcher)
        offset += snprintf(psql_cmd + offset, sizeof(psql_cmd) - offset,
                           "%s ", launcher);

    snprintf(psql_cmd + offset, sizeof(psql_cmd) - offset,
             "\"%s\" \"dbname=%s\" < \"%s\" > \"%s\" 2>&1",
             isolation_exec,
             dblist->str,
             infile,
             outfile);

    pid = spawn_process(psql_cmd);

    if (pid == INVALID_PID)
    {
        fprintf(stderr, _("could not start process for test %s\n"),
                testname);
        exit(2);
    }

    return pid;
}

static void
isolation_init(int argc, char **argv)
{
    size_t        argv0_len;
    
#ifdef __TBASE__    
    int         i;
#endif
    /*
     * We unfortunately cannot do the find_other_exec() lookup to find the
     * "isolationtester" binary here.  regression_main() calls the
     * initialization functions before parsing the commandline arguments and
     * thus hasn't changed the library search path at this point which in turn
     * can cause the "isolationtester -V" invocation that find_other_exec()
     * does to fail since it's linked to libpq.  So we instead copy argv[0]
     * and do the lookup the first time through isolation_start_test().
     */
    argv0_len = strlcpy(saved_argv0, argv[0], MAXPGPATH);
    if (argv0_len >= MAXPGPATH)
    {
        fprintf(stderr, _("path for isolationtester executable is longer than %d bytes\n"),
                (int) (MAXPGPATH - 1));
        exit(2);
    }

    /* set default regression database name */
    add_stringlist_item(&dblist, "isolation_regression");
#ifdef __TBASE__
    for(i= 0; i < argc; i++)
    {
        if (strcmp(argv[i], "--txn-test") == 0)
        {
            check_connection_conf();
            format_isolation_test();
            break;
        }
    }
#endif    
    return;
}

int
main(int argc, char *argv[])
{
    return regression_main(argc, argv, isolation_init, isolation_start_test);
}

#ifdef __TBASE__
#define CONNECTION_CONF_FILENAME    "isolation_test.conf"   
#define MAX_CONNECTION              10
#define FORMAT_SHELL_NAME           "isolation_test_format.sh"    

static void check_connection_conf(void)
{
    char infile[MAXPGPATH];
    char inputbuf[MAXPGPATH];   
    FILE * fp;
    int    i;
    
    snprintf(infile, sizeof(infile), "./%s", CONNECTION_CONF_FILENAME);

    fp = fopen(infile, "r");
    if (NULL == fp)
    {
        fprintf(stderr, "INFO:there is no %s\n", CONNECTION_CONF_FILENAME);
        return;
    }
    
    i = 0;  
    while (fgets(inputbuf, sizeof(inputbuf), fp))
    {
        if (strncmp(inputbuf, "connect:", strlen("connect:")) == 0)
        {
            fprintf(stderr, "connection info No.%d, info:\"%s\"\n", i, inputbuf);
            
            if (i >= MAX_CONNECTION)
            {
                fprintf(stderr, "FATAL:too many connection for isolation test, max is %d\n", MAX_CONNECTION);
                exit(1);
            }
            i++;
        }
    }

    if (0 == i)
    {
        fprintf(stderr, "FATAL:%s has no connection info\n", CONNECTION_CONF_FILENAME);
        exit(1);
    }

    return;
}

static void format_isolation_test(void)
{
    char        infile[MAXPGPATH];
    char        cmdline[MAXPGPATH];
    
    snprintf(infile, sizeof(infile), "%s/%s", inputdir, FORMAT_SHELL_NAME);

    if (!file_exists(infile))
    {
        fprintf(stderr, "FATAL:isolation_test_format.sh does not exist\n");
        exit(1);
    }
    
    snprintf(cmdline, sizeof(cmdline), "chmod +x %s; sh %s 2>&1 ", infile, infile);

    cmd_exec_result(cmdline);

    return;    
}

static void cmd_exec_result(const char *cmd)
{
    char line_buffer[MAXPGPATH] = {0};
    FILE *fp = NULL;
    
    fprintf(stderr, "cmd_exec_result cmd is %s\n", cmd);
    
    fp = popen(cmd, "r");
    if (NULL == fp)
    {
        fprintf(stderr, "FATAL: fail to popen cmd:%s\n", cmd);
        exit(1);
    }    

    while (fgets(line_buffer, MAXPGPATH, fp))
    {    
        fprintf(stderr, "exec result:%s", line_buffer);
    }    

    pclose(fp);

    return;
}


#endif
