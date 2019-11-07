/*-------------------------------------------------------------------------
 *
 * remotecopy.h
 *        Routines for extension of COPY command for cluster management
 *
 *
 * Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *        src/include/pgxc/remotecopy.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef REMOTECOPY_H
#define REMOTECOPY_H

#include "nodes/parsenodes.h"
#include "pgxc/locator.h"

/*
 * This contains the set of data necessary for remote COPY control.
 */
typedef struct RemoteCopyData {
    /* COPY FROM/TO? */
    bool        is_from;

    /*
     * On Coordinator we need to rewrite query.
     * While client may submit a copy command dealing with file, Datanodes
     * always send/receive data to/from the Coordinator. So we can not use
     * original statement and should rewrite statement, specifing STDIN/STDOUT
     * as copy source or destination
     */
    StringInfoData query_buf;
    Locator            *locator;        /* the locator object */
    Oid                dist_type;        /* data type of the distribution column */
#ifdef __COLD_HOT__
    Oid             sec_dist_type;
#endif
    /* Locator information */
    RelationLocInfo *rel_loc;        /* the locator key */
} RemoteCopyData;

/*
 * List of all the options used for query deparse step
 * As CopyStateData stays private in copy.c and in order not to
 * make Postgres-XC code too much intrusive in PostgreSQL code,
 * this intermediate structure is used primarily to generate remote
 * COPY queries based on deparsed options.
 */
typedef struct RemoteCopyOptions {
    bool        rco_binary;            /* binary format? */
    bool        rco_oids;            /* include OIDs? */
    bool        rco_csv_mode;        /* Comma Separated Value format? */
#ifdef __TBASE__
    bool        rco_insert_into;
#endif
    char       *rco_delim;            /* column delimiter (must be 1 byte) */
    char       *rco_null_print;        /* NULL marker string (server encoding!) */
    char       *rco_quote;            /* CSV quote char (must be 1 byte) */
    char       *rco_escape;            /* CSV escape char (must be 1 byte) */
    List       *rco_force_quote;    /* list of column names */
    List       *rco_force_notnull;    /* list of column names */
} RemoteCopyOptions;

extern void RemoteCopy_BuildStatement(RemoteCopyData *state,
                                      Relation rel,
                                      RemoteCopyOptions *options,
                                      List *attnamelist,
                                      List *attnums,
#ifdef _SHARDING_
                                      const Bitmapset *shards
#endif
);
extern void RemoteCopy_GetRelationLoc(RemoteCopyData *state,
                                      Relation rel,
                                      List *attnums);
extern RemoteCopyOptions *makeRemoteCopyOptions(void);
extern void FreeRemoteCopyData(RemoteCopyData *state);
extern void FreeRemoteCopyOptions(RemoteCopyOptions *options);
#endif
