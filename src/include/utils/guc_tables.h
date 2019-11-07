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
 * guc_tables.h
 *        Declarations of tables used by GUC.
 *
 * See src/backend/utils/misc/README for design notes.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 *
 *      src/include/utils/guc_tables.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GUC_TABLES_H
#define GUC_TABLES_H 1

#include "utils/guc.h"

/*
 * GUC supports these types of variables:
 */
enum config_type
{
    PGC_BOOL,
    PGC_INT,
    PGC_UINT,
    PGC_REAL,
    PGC_STRING,
    PGC_ENUM
};

union config_var_val
{
    bool        boolval;
    int            intval;
    uint        uintval;
    double        realval;
    char       *stringval;
    int            enumval;
};

/*
 * The actual value of a GUC variable can include a malloc'd opaque struct
 * "extra", which is created by its check_hook and used by its assign_hook.
 */
typedef struct config_var_value
{
    union config_var_val val;
    void       *extra;
} config_var_value;

/*
 * Groupings to help organize all the run-time options for display
 */
enum config_group
{
    UNGROUPED,
    FILE_LOCATIONS,
    CONN_AUTH,
    CONN_AUTH_SETTINGS,
    CONN_AUTH_SECURITY,
    RESOURCES,
    RESOURCES_MEM,
    RESOURCES_DISK,
    RESOURCES_KERNEL,
    RESOURCES_VACUUM_DELAY,
    RESOURCES_BGWRITER,
    RESOURCES_ASYNCHRONOUS,
    WAL,
    WAL_SETTINGS,
    WAL_CHECKPOINTS,
    WAL_ARCHIVING,
    REPLICATION,
    REPLICATION_SENDING,
    REPLICATION_MASTER,
    REPLICATION_STANDBY,
    REPLICATION_SUBSCRIBERS,
    QUERY_TUNING,
    QUERY_TUNING_METHOD,
    QUERY_TUNING_COST,
    QUERY_TUNING_GEQO,
    QUERY_TUNING_OTHER,
    LOGGING,
    LOGGING_WHERE,
    LOGGING_WHEN,
    LOGGING_WHAT,
    PROCESS_TITLE,
    STATS,
    STATS_MONITORING,
    STATS_COLLECTOR,
    AUTOVACUUM,
    CLIENT_CONN,
    CLIENT_CONN_STATEMENT,
    CLIENT_CONN_LOCALE,
    CLIENT_CONN_PRELOAD,
    CLIENT_CONN_OTHER,
    LOCK_MANAGEMENT,
    COMPAT_OPTIONS,
    COMPAT_OPTIONS_PREVIOUS,
    COMPAT_OPTIONS_CLIENT,
    ERROR_HANDLING_OPTIONS,
    PRESET_OPTIONS,
    CUSTOM_OPTIONS,
#ifdef _SHARDING_
    SHARD_VISIBLE_MODE,
#endif
#ifdef PGXC
    DEVELOPER_OPTIONS,
    DATA_NODES,
    GTM,
    COORDINATORS,
    XC_HOUSEKEEPING_OPTIONS
#else
    DEVELOPER_OPTIONS
#endif
};

/*
 * Stack entry for saving the state a variable had prior to an uncommitted
 * transactional change
 */
typedef enum
{
    /* This is almost GucAction, but we need a fourth state for SET+LOCAL */
    GUC_SAVE,                    /* entry caused by function SET option */
    GUC_SET,                    /* entry caused by plain SET command */
    GUC_LOCAL,                    /* entry caused by SET LOCAL command */
    GUC_SET_LOCAL                /* entry caused by SET then SET LOCAL */
} GucStackState;

typedef struct guc_stack
{
    struct guc_stack *prev;        /* previous stack item, if any */
    int            nest_level;        /* nesting depth at which we made entry */
    GucStackState state;        /* see enum above */
    GucSource    source;            /* source of the prior value */
    /* masked value's source must be PGC_S_SESSION, so no need to store it */
    GucContext    scontext;        /* context that set the prior value */
    GucContext    masked_scontext;    /* context that set the masked value */
    config_var_value prior;        /* previous value of variable */
    config_var_value masked;    /* SET value in a GUC_SET_LOCAL entry */
} GucStack;

/*
 * Generic fields applicable to all types of variables
 *
 * The short description should be less than 80 chars in length. Some
 * applications may use the long description as well, and will append
 * it to the short description. (separated by a newline or '. ')
 *
 * Note that sourcefile/sourceline are kept here, and not pushed into stacked
 * values, although in principle they belong with some stacked value if the
 * active value is session- or transaction-local.  This is to avoid bloating
 * stack entries.  We know they are only relevant when source == PGC_S_FILE.
 */
struct config_generic
{
    /* constant fields, must be set correctly in initial value: */
    const char *name;            /* name of variable - MUST BE FIRST */
    GucContext    context;        /* context required to set the variable */
    enum config_group group;    /* to help organize variables by function */
    const char *short_desc;        /* short desc. of this variable's purpose */
    const char *long_desc;        /* long desc. of this variable's purpose */
    int            flags;            /* flag bits, see guc.h */
    /* variable fields, initialized at runtime: */
    enum config_type vartype;    /* type of variable (set only at startup) */
    int            status;            /* status bits, see below */
    GucSource    source;            /* source of the current actual value */
    GucSource    reset_source;    /* source of the reset_value */
    GucContext    scontext;        /* context that set the current value */
    GucContext    reset_scontext; /* context that set the reset value */
    GucStack   *stack;            /* stacked prior values */
    void       *extra;            /* "extra" pointer for current actual value */
    char       *sourcefile;        /* file current setting is from (NULL if not
                                 * set in config file) */
    int            sourceline;        /* line in source file */
};

/* bit values in status field */
#define GUC_IS_IN_FILE        0x0001    /* found it in config file */
/*
 * Caution: the GUC_IS_IN_FILE bit is transient state for ProcessConfigFile.
 * Do not assume that its value represents useful information elsewhere.
 */
#define GUC_PENDING_RESTART 0x0002


/* GUC records for specific variable types */

struct config_bool
{
    struct config_generic gen;
    /* constant fields, must be set correctly in initial value: */
    bool       *variable;
    bool        boot_val;
    GucBoolCheckHook check_hook;
    GucBoolAssignHook assign_hook;
    GucShowHook show_hook;
    /* variable fields, initialized at runtime: */
    bool        reset_val;
    void       *reset_extra;
};

struct config_int
{
    struct config_generic gen;
    /* constant fields, must be set correctly in initial value: */
    int           *variable;
    int            boot_val;
    int            min;
    int            max;
    GucIntCheckHook check_hook;
    GucIntAssignHook assign_hook;
    GucShowHook show_hook;
    /* variable fields, initialized at runtime: */
    int            reset_val;
    void       *reset_extra;
};

struct config_uint
{
    struct config_generic gen;
    /* constant fields, must be set correctly in initial value: */
    uint       *variable;
    uint        boot_val;
    uint        min;
    uint        max;
    GucUintCheckHook check_hook;
    GucUintAssignHook assign_hook;
    GucShowHook show_hook;
    /* variable fields, initialized at runtime: */
    uint        reset_val;
    void       *reset_extra;
};

struct config_real
{
    struct config_generic gen;
    /* constant fields, must be set correctly in initial value: */
    double       *variable;
    double        boot_val;
    double        min;
    double        max;
    GucRealCheckHook check_hook;
    GucRealAssignHook assign_hook;
    GucShowHook show_hook;
    /* variable fields, initialized at runtime: */
    double        reset_val;
    void       *reset_extra;
};

struct config_string
{
    struct config_generic gen;
    /* constant fields, must be set correctly in initial value: */
    char      **variable;
    const char *boot_val;
    GucStringCheckHook check_hook;
    GucStringAssignHook assign_hook;
    GucShowHook show_hook;
    /* variable fields, initialized at runtime: */
    char       *reset_val;
    void       *reset_extra;
};

struct config_enum
{
    struct config_generic gen;
    /* constant fields, must be set correctly in initial value: */
    int           *variable;
    int            boot_val;
    const struct config_enum_entry *options;
    GucEnumCheckHook check_hook;
    GucEnumAssignHook assign_hook;
    GucShowHook show_hook;
    /* variable fields, initialized at runtime: */
    int            reset_val;
    void       *reset_extra;
};

/* constant tables corresponding to enums above and in guc.h */
extern const char *const config_group_names[];
extern const char *const config_type_names[];
extern const char *const GucContext_Names[];
extern const char *const GucSource_Names[];

/* get the current set of variables */
extern struct config_generic **get_guc_variables(void);

extern void build_guc_variables(void);

/* search in enum options */
extern const char *config_enum_lookup_by_value(struct config_enum *record, int val);
extern bool config_enum_lookup_by_name(struct config_enum *record,
                           const char *value, int *retval);

#endif                            /* GUC_TABLES_H */
