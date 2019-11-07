/*-------------------------------------------------------------------------
 *
 * relpath.h
 *        Declarations for GetRelationPath() and friends
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/common/relpath.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RELPATH_H
#define RELPATH_H

/*
 * Stuff for fork names.
 *
 * The physical storage of a relation consists of one or more forks.
 * The main fork is always created, but in addition to that there can be
 * additional forks for storing various metadata. ForkNumber is used when
 * we need to refer to a specific fork in a relation.
 */
typedef enum ForkNumber
{
    InvalidForkNumber = -1,
    MAIN_FORKNUM = 0,
    FSM_FORKNUM,
    VISIBILITYMAP_FORKNUM,
#ifdef _SHARDING_
    EXTENT_FORKNUM,
#endif
    INIT_FORKNUM

    /*
     * NOTE: if you add a new fork, change MAX_FORKNUM and possibly
     * FORKNAMECHARS below, and update the forkNames array in
     * src/common/relpath.c
     */
} ForkNumber;

#define MAX_FORKNUM        INIT_FORKNUM

#ifdef _SHARDING_
#define FORKNAMECHARS    5        /* max chars for a fork name */
#else
#define FORKNAMECHARS    4
#endif

extern const char *const forkNames[];

extern ForkNumber forkname_to_number(const char *forkName);
extern int    forkname_chars(const char *str, ForkNumber *fork);

/*
 * Stuff for computing filesystem pathnames for relations.
 */
extern char *GetDatabasePath(Oid dbNode, Oid spcNode);
#ifdef XCP
extern char *GetDatabasePath_client(Oid dbNode, Oid spcNode, const char *nodename);
#endif

extern char *GetRelationPath(Oid dbNode, Oid spcNode, Oid relNode,
                int backendId, ForkNumber forkNumber);
#ifdef XCP
extern char *GetRelationPath_client(Oid dbNode, Oid spcNode, Oid relNode,
                int backendId, ForkNumber forkNumber,
                const char *nodename);
#endif

/*
 * Wrapper macros for GetRelationPath.  Beware of multiple
 * evaluation of the RelFileNode or RelFileNodeBackend argument!
 */

/* First argument is a RelFileNode */
#define relpathbackend(rnode, backend, forknum) \
    GetRelationPath((rnode).dbNode, (rnode).spcNode, (rnode).relNode, \
                    backend, forknum)
#ifdef XCP
#define relpathbackend_client(rnode, backend, forknum, nodename) \
    GetRelationPath_client((rnode).dbNode, (rnode).spcNode, (rnode).relNode, \
                    backend, forknum, nodename)
#endif

/* First argument is a RelFileNode */
#define relpathperm(rnode, forknum) \
    relpathbackend(rnode, InvalidBackendId, forknum)
#ifdef XCP
#define relpathperm_client(rnode, forknum, nodename) \
    relpathbackend_client(rnode, InvalidBackendId, forknum, nodename)
#endif

/* First argument is a RelFileNodeBackend */
#ifdef XCP
#define relpath(rnode, forknum) \
    relpathbackend((rnode).node, InvalidBackendId, forknum)
#else
#define relpath(rnode, forknum) \
    relpathbackend((rnode).node, (rnode).backend, forknum)
#endif
#endif                            /* RELPATH_H */
