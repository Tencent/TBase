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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/time.h>

#include "getopt_long.h"
#include "util.h"
#include "conf.h"
#include "var.h"
#include "log.h"

#define MAX_BUFF_LEN MAXPATH

enum 
{
    CONF_MOD = 0,
    CONF_DEL = 1,
    CONF_BUTT
};

char * g_actiontypestr;
char * g_datapath;
char * g_conffilename;
char * g_gucname;
char * g_newvalue;
char * g_newconffile;
FILE * g_confpf;
FILE * g_newconfpf;
int    g_actiontype;

char g_abs_path_to_conffile[MAX_BUFF_LEN + 1] = {0};
char g_abs_path_to_newfile[MAX_BUFF_LEN + 1] = {0};
char g_newvalue_buff[MAX_BUFF_LEN + 1] = { 0 };

static void usage(void)
{
    printf("confmod: check input conf file to mod or del guc option\n"
           "Usage:\n"
           " confmod -a actiontype -d path -f conffile -g gucname [-v newvalue] \n\n"
           "Options:\n");

    printf("  -a, --actiontype=xxx     mod = modify or add, del = omit \n");
    printf("  -d, --datapath=path      path to conffile\n");
    printf("  -f, --filename=xxx       conf file name\n");
    printf("  -g, --gucname=xxx        guc name\n");
    printf("  -v, --newvalue=xxx       if mod = del, this option is no use\n");
    printf("  -?, --help               print this message.\n");
}

static void parse_confmod_options(int argc, char *argv[])
{// #lizard forgives
    static struct option long_options[] =
    {
        {"actiontype", required_argument, NULL, 'a'},
        {"datapath", required_argument, NULL, 'd'},
        {"filename", required_argument, NULL, 'f'},
        {"gucname", required_argument, NULL, 'g'},
        {"newvalue", required_argument, NULL, 'v'},
            
        {"help", no_argument, NULL, '?'},
        {NULL, 0, NULL, 0}
    };

    int optindex;
    extern char *optarg;
    extern int optind;
    int c;

 
    while ((c = getopt_long(argc, argv, "a:d:f:g:v:?", long_options, &optindex)) != -1)
    {
        switch(c)
        {
            case 'a':
                g_actiontypestr = Strdup(optarg);
                break;
            case 'd':
                g_datapath = Strdup(optarg);
                break;
            case 'f':
                g_conffilename = Strdup(optarg);
                break;
            case 'g':
                g_gucname = Strdup(optarg);
                break;
            case 'v':
                g_newvalue = Strdup(optarg);
                break;
            
            case '?':
                if (strcmp(argv[optind - 1], "-?") == 0 || strcmp(argv[optind - 1], "--help") == 0)
                {
                    usage();
                    exit(0);
                }
                else
                {
                    elog(ERROR, "Try \" --help\" for more information.\n" );
                    exit(1);
                }
                break;
            default:
                elog(ERROR, "Try \" --help\" for more information.\n" );
                exit(1);
                break;
        }
    }

    return;
}

static unsigned long long int
get_current_timestamp(void)
{
    unsigned long long int result = 0;
    struct timeval tp;

    const int UNIX_EPOCH_JDATE = 2440588; /* == date2j(1970, 1, 1) */
    const int POSTGRES_EPOCH_JDATE = 2451545; /* == date2j(2000, 1, 1) */

    const int SECS_PER_DAY = 86400;

#ifdef HAVE_INT64_TIMESTAMP
    const int USECS_PER_SEC = 1000000;
#endif

    (void)gettimeofday(&tp, NULL);

    result = (unsigned long long int) tp.tv_sec -
        ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY);

#ifdef HAVE_INT64_TIMESTAMP
    result = (unsigned long long int)((result * USECS_PER_SEC) + tp.tv_usec);
#else
    result = (unsigned long long int)(result + (tp.tv_usec / 1000000.0));
#endif

    return result;
}

static void check_args()
{// #lizard forgives
    if ((NULL == g_actiontypestr)
        ||(NULL == g_datapath)
        ||(NULL == g_conffilename)
        ||(NULL == g_gucname))
    {
        usage();
        exit(1);
    }

    if (strcmp(g_actiontypestr, "mod") == 0)
    {
        g_actiontype = CONF_MOD;
        if (NULL == g_newvalue)
        {
            usage();
            exit(1);
        }
    }
    else if (strcmp(g_actiontypestr, "del") == 0)
    {
        g_actiontype = CONF_DEL;
    }
    else
    {
        usage();
        exit(1);
    }

    snprintf(g_abs_path_to_conffile, MAX_BUFF_LEN, "%s/%s", g_datapath, g_conffilename);
    g_confpf = Fopen(g_abs_path_to_conffile, "r");
    if (NULL == g_confpf)
    {
        exit(1);
    }

    snprintf(g_abs_path_to_newfile, MAX_BUFF_LEN, "%s/%s.rename.tm%llu.pid%llu.ppid%llu.tid%llu",
        g_datapath, 
        g_conffilename,
        (unsigned long long int)get_current_timestamp(),
        (unsigned long long int)getpid(),
        (unsigned long long int)getppid(),
        (unsigned long long int)pthread_self());
    g_newconfpf = Fopen(g_abs_path_to_newfile, "w");
    if (NULL == g_newconfpf)
    {
        exit(1);
    }

    return;
}

static void
var_traverse(void * data)
{
    pg_conf_var * head = (pg_conf_var *)data;
    int num = fprintf(g_newconfpf, "%s = %s", head->name, head->value);

    if (num < 0)
    {
        elog(ERROR, "Failed to write %s = %s into file %s, fprintf ret code = %d, %s, exit \n",
            head->name, head->value, g_abs_path_to_newfile, num, strerror(errno));
        exit(1);
    }
}

static void create_new_conf_file()
{
    int lineno = 0;
    stree * root = NULL;

    lineno = read_vars(g_confpf, (CONF_DEL == g_actiontype) ? g_gucname : NULL);
    if (0 == lineno)
    {
        elog(ERROR, "Read NULL guc from %s, exit \n", g_abs_path_to_conffile);
        exit(1);
    }

    if (CONF_MOD == g_actiontype)
    {
        pg_conf_var *newv;

        if (!(newv = confirm_var(g_gucname)))
        {
            elog(ERROR, "Failed to confirm target modify guc '%s' in hash table, exit \n", g_gucname);
            exit(1);
        }

        snprintf(g_newvalue_buff, MAX_BUFF_LEN, "%s \n", g_newvalue);
        set_value(newv, g_newvalue_buff);
        if (newv->line == 0)
            set_line(newv, ++lineno);
    }

    root = var_hash_2_stree();
    stree_pre_traverse(root, var_traverse);

    fclose(g_confpf);
    fclose(g_newconfpf);
}

static void rename_conf_file()
{
    int loops = 0;
    
    while (rename(g_abs_path_to_newfile, g_abs_path_to_conffile) < 0)
    {
        if (errno != EACCES)
        {
            elog(ERROR, "Failed to rename '%s' to '%s', %s \n", 
                    g_abs_path_to_newfile,
                    g_abs_path_to_conffile,
                    strerror(errno));
            exit(-1);
        }
        
        if (++loops > 2)        /* time out after 10 sec */
        {
            elog(ERROR, "Failed to rename '%s' to '%s' after retry %d times, %s \n", 
                    g_abs_path_to_newfile,
                    g_abs_path_to_conffile,
                    loops, strerror(errno));
            exit(-1);
        }
        
        sleep(1);        /* us */
    }
}

int main(int argc, char *argv[])
{
    if (argc > 1)
    {
        if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
        {
            usage();
            exit(0);
        }
    }

    parse_confmod_options(argc, argv);

    check_args();

    create_new_conf_file();

    rename_conf_file();    
    
    return 0;
}

