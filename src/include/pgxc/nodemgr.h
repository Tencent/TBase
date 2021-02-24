/*-------------------------------------------------------------------------
 *
 * nodemgr.h
 *  Routines for node management
 *
 *
 * Portions Copyright (c) 1996-2011  PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/nodemgr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEMGR_H
#define NODEMGR_H

#include "nodes/parsenodes.h"

#define PGXC_NODENAME_LENGTH    64
#define        TBASE_MAX_COORDINATOR_NUMBER 2048
#define        TBASE_MAX_DATANODE_NUMBER    2048

/* Global number of nodes */
extern int     NumDataNodes;
extern int     NumCoords;

#ifdef __TBASE__
extern char *PGXCNodeHost;
#endif

/* Node definition */
typedef struct
{
    Oid         nodeoid;
    NameData    nodename;
    NameData    nodehost;
    int            nodeport;
    bool        nodeisprimary;
    bool         nodeispreferred;
    bool        nodeishealthy;
} NodeDefinition;

extern void NodeTablesShmemInit(void);
#ifdef __TBASE__
extern void NodeDefHashTabShmemInit(void);
extern Size NodeHashTableShmemSize(void);
#endif
extern Size NodeTablesShmemSize(void);

extern void PgxcNodeListAndCountWrapTransaction(void);
extern void
PgxcNodeGetOidsExtend(Oid **coOids, Oid **dnOids, Oid **sdnOids,
                int *num_coords, int *num_dns, int *num_sdns, bool update_preferred);
#define PgxcNodeGetOids(coOids, dnOids, num_coords, num_dns, update_preferred) \
    PgxcNodeGetOidsExtend(coOids, dnOids, NULL, num_coords, num_dns, NULL, update_preferred)
extern void
PgxcNodeGetHealthMapExtend(Oid *coOids, Oid *dnOids, Oid *sdnOids,
                int *num_coords, int *num_dns, int *num_sdns, bool *coHealthMap,
                bool *dnHealthMap, bool *sdnHealthMap);
#define PgxcNodeGetHealthMap(coOids, dnOids, num_coords, num_dns, coHealthMap, dnHealthMap) \
                PgxcNodeGetHealthMapExtend(coOids, dnOids, NULL,num_coords, num_dns, NULL, coHealthMap, \
                dnHealthMap, NULL)
extern NodeDefinition *PgxcNodeGetDefinition(Oid node);
extern void PgxcNodeAlter(AlterNodeStmt *stmt);
extern void PgxcNodeCreate(CreateNodeStmt *stmt);
extern void PgxcNodeRemove(DropNodeStmt *stmt);
extern void PgxcNodeDnListHealth(List *nodeList, bool *dnhealth);
extern bool PgxcNodeUpdateHealth(Oid node, bool status);

extern bool PrimaryNodeNumberChanged(void);
/* GUC parameter */
extern bool enable_multi_cluster;
extern bool enable_multi_cluster_print;
#endif    /* NODEMGR_H */
