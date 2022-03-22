/*
 * dbsize.c
 *        Database object size functions, and related inquiries
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Copyright (c) 2002-2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *      src/backend/utils/adt/dbsize.c
 *
 */

#include "postgres.h"

#include <sys/stat.h>

#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_tablespace.h"
#include "commands/dbcommands.h"
#include "commands/tablespace.h"
#include "commands/tablecmds.h"
#include "executor/spi.h"
#include "miscadmin.h"
#ifdef PGXC
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#endif
#include "storage/fd.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/numeric.h"
#include "utils/rel.h"
#include "utils/relfilenodemap.h"
#include "utils/relmapper.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#ifdef XCP
#include "catalog/pg_type.h"
#include "catalog/pgxc_node.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "pgxc/execRemote.h"
#include "utils/snapmgr.h"
#endif
#ifdef __TBASE__
#include "utils/ruleutils.h"
#endif
#ifdef _MLS_
#include "utils/mls.h"
#include "catalog/pg_inherits_fn.h"
#endif

#ifdef PGXC
static Datum pgxc_database_size(Oid dbOid);
static Datum pgxc_allocated_database_size(Oid dbOid);
static Datum pgxc_tablespace_size(Oid tbOid);
static Datum pgxc_allocated_tablespace_size(Oid tbOid);
static int64 pgxc_exec_sizefunc(Oid relOid, char *funcname, char *extra_arg);

/*
 * Below macro is important when the object size functions are called
 * for system catalog tables. For pg_catalog tables and other Coordinator-only
 * tables, we should return the data from Coordinator. If we don't find
 * locator info, that means it is a Coordinator-only table.
 * also, skip temporary relation.
 */
#define COLLECT_FROM_DATANODES(relid) \
    (IS_PGXC_LOCAL_COORDINATOR && \
    (GetRelationLocInfo((relid)) != NULL)&&(false == IsTempTable((relid))))
#endif
/* Divide by two and round towards positive infinity. */
#define half_rounded(x)   (((x) + ((x) < 0 ? 0 : 1)) / 2)

/* Return physical size of directory contents, or 0 if dir doesn't exist */
static int64
db_dir_size(const char *path, int64 *alloc_size)
{// #lizard forgives
    int64        dirsize = 0;
    struct dirent *direntry;
    DIR           *dirdesc;
    char        filename[MAXPGPATH * 2];

    dirdesc = AllocateDir(path);

    if (!dirdesc)
        return 0;

#ifdef _SHARDING_
    if(alloc_size)
        *alloc_size = 0;
#endif

    while ((direntry = ReadDir(dirdesc, path)) != NULL)
    {
        struct stat fst;

        CHECK_FOR_INTERRUPTS();

        if (strcmp(direntry->d_name, ".") == 0 ||
            strcmp(direntry->d_name, "..") == 0)
            continue;

        snprintf(filename, sizeof(filename), "%s/%s", path, direntry->d_name);

        if (stat(filename, &fst) < 0)
        {
            if (errno == ENOENT)
                continue;
            else
                ereport(ERROR,
                        (errcode_for_file_access(),
                         errmsg("could not stat file \"%s\": %m", filename)));
        }
        dirsize += fst.st_size;
#ifdef _SHARDING_
        if(alloc_size)
            *alloc_size += fst.st_blocks * 512;
#endif
    }

    FreeDir(dirdesc);
    return dirsize;
}

/*
 * calculate size of database in all tablespaces
 */
static int64
calculate_database_size(Oid dbOid, int64 *alloc_size)
{// #lizard forgives
    int64        totalsize;
#ifdef _SHARDING_
    int64        ph_size;
#endif
    DIR           *dirdesc;
    struct dirent *direntry;
    char        dirpath[MAXPGPATH];
    char        pathname[MAXPGPATH + 12 + sizeof(TABLESPACE_VERSION_DIRECTORY)];
    AclResult    aclresult;

#ifdef _SHARDING_
    if(alloc_size)
        *alloc_size = 0;
#endif

    /*
     * User must have connect privilege for target database or be a member of
     * pg_read_all_stats
     */
    aclresult = pg_database_aclcheck(dbOid, GetUserId(), ACL_CONNECT);
    if (aclresult != ACLCHECK_OK &&
        !is_member_of_role(GetUserId(), DEFAULT_ROLE_READ_ALL_STATS))
    {
        aclcheck_error(aclresult, ACL_KIND_DATABASE,
                       get_database_name(dbOid));
    }

    /* Shared storage in pg_global is not counted */

    /* Include pg_default storage */
    snprintf(pathname, sizeof(pathname), "base/%u", dbOid);
    totalsize = db_dir_size(pathname, &ph_size);
#ifdef _SHARDING_
    if(alloc_size)
        *alloc_size += ph_size;
#endif    

    /* Scan the non-default tablespaces */
    snprintf(dirpath, MAXPGPATH, "pg_tblspc");
    dirdesc = AllocateDir(dirpath);
    if (!dirdesc)
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("could not open tablespace directory \"%s\": %m",
                        dirpath)));

    while ((direntry = ReadDir(dirdesc, dirpath)) != NULL)
    {
        CHECK_FOR_INTERRUPTS();

        if (strcmp(direntry->d_name, ".") == 0 ||
            strcmp(direntry->d_name, "..") == 0)
            continue;

#ifdef PGXC
        /* Postgres-XC tablespaces include node name in path */
        snprintf(pathname, sizeof(pathname), "pg_tblspc/%s/%s_%s/%u",
                 direntry->d_name, TABLESPACE_VERSION_DIRECTORY, PGXCNodeName, dbOid);
#else
        snprintf(pathname, sizeof(pathname), "pg_tblspc/%s/%s/%u",
                 direntry->d_name, TABLESPACE_VERSION_DIRECTORY, dbOid);
#endif
        totalsize += db_dir_size(pathname, &ph_size);
#ifdef _SHARDING_
        if(alloc_size)
            *alloc_size += ph_size;
#endif
    }

    FreeDir(dirdesc);

    return totalsize;
}

Datum
pg_database_size_oid(PG_FUNCTION_ARGS)
{
    Oid            dbOid = PG_GETARG_OID(0);
    int64        size;

#ifdef PGXC
    if (IS_PGXC_LOCAL_COORDINATOR)
        PG_RETURN_DATUM(pgxc_database_size(dbOid));
#endif

    size = calculate_database_size(dbOid, NULL);

    if (size == 0)
        PG_RETURN_NULL();

    PG_RETURN_INT64(size);
}

Datum
pg_database_size_name(PG_FUNCTION_ARGS)
{
    Name        dbName = PG_GETARG_NAME(0);
    Oid            dbOid = get_database_oid(NameStr(*dbName), false);
    int64        size;

#ifdef PGXC
    if (IS_PGXC_LOCAL_COORDINATOR)
        PG_RETURN_DATUM(pgxc_database_size(dbOid));
#endif

    size = calculate_database_size(dbOid, NULL);

    if (size == 0)
        PG_RETURN_NULL();

    PG_RETURN_INT64(size);
}

#ifdef _SHARDING_
Datum
pg_allocated_database_size_oid(PG_FUNCTION_ARGS)
{
    Oid            dbOid = PG_GETARG_OID(0);
    int64        size;

#ifdef PGXC
    if (IS_PGXC_LOCAL_COORDINATOR)
        PG_RETURN_DATUM(pgxc_allocated_database_size(dbOid));
#endif

    calculate_database_size(dbOid, &size);

    if (size == 0)
        PG_RETURN_NULL();

    PG_RETURN_INT64(size);
}

Datum
pg_allocated_database_size_name(PG_FUNCTION_ARGS)
{
    Name        dbName = PG_GETARG_NAME(0);
    Oid            dbOid = get_database_oid(NameStr(*dbName), false);
    int64        size;

#ifdef PGXC
    if (IS_PGXC_LOCAL_COORDINATOR)
        PG_RETURN_DATUM(pgxc_allocated_database_size(dbOid));
#endif

    calculate_database_size(dbOid, &size);

    if (size == 0)
        PG_RETURN_NULL();

    PG_RETURN_INT64(size);
}
#endif

/*
 * Calculate total size of tablespace. Returns -1 if the tablespace directory
 * cannot be found.
 */
static int64
calculate_tablespace_size(Oid tblspcOid, int64 *alloc_size)
{// #lizard forgives
    char        tblspcPath[MAXPGPATH];
    char        pathname[MAXPGPATH * 2];
    int64        totalsize = 0;
#ifdef _SHARDING_
    int64        ph_size;
#endif
    DIR           *dirdesc;
    struct dirent *direntry;
    AclResult    aclresult;

#ifdef _SHARDING_
    if(alloc_size)
        *alloc_size = 0;
#endif

    /*
     * User must be a member of pg_read_all_stats or have CREATE privilege for
     * target tablespace, either explicitly granted or implicitly because it
     * is default for current database.
     */
    if (tblspcOid != MyDatabaseTableSpace &&
        !is_member_of_role(GetUserId(), DEFAULT_ROLE_READ_ALL_STATS))
    {
        aclresult = pg_tablespace_aclcheck(tblspcOid, GetUserId(), ACL_CREATE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error(aclresult, ACL_KIND_TABLESPACE,
                           get_tablespace_name(tblspcOid));
    }

    if (tblspcOid == DEFAULTTABLESPACE_OID)
        snprintf(tblspcPath, MAXPGPATH, "base");
    else if (tblspcOid == GLOBALTABLESPACE_OID)
        snprintf(tblspcPath, MAXPGPATH, "global");
    else
#ifdef PGXC
        /* Postgres-XC tablespaces include node name in path */
        snprintf(tblspcPath, MAXPGPATH, "pg_tblspc/%u/%s_%s", tblspcOid,
                 TABLESPACE_VERSION_DIRECTORY, PGXCNodeName);
#else
        snprintf(tblspcPath, MAXPGPATH, "pg_tblspc/%u/%s", tblspcOid,
                 TABLESPACE_VERSION_DIRECTORY);
#endif

    dirdesc = AllocateDir(tblspcPath);

    if (!dirdesc)
        return -1;

    while ((direntry = ReadDir(dirdesc, tblspcPath)) != NULL)
    {
        struct stat fst;

        CHECK_FOR_INTERRUPTS();

        if (strcmp(direntry->d_name, ".") == 0 ||
            strcmp(direntry->d_name, "..") == 0)
            continue;

        snprintf(pathname, sizeof(pathname), "%s/%s", tblspcPath, direntry->d_name);

        if (stat(pathname, &fst) < 0)
        {
            if (errno == ENOENT)
                continue;
            else
                ereport(ERROR,
                        (errcode_for_file_access(),
                         errmsg("could not stat file \"%s\": %m", pathname)));
        }

        if (S_ISDIR(fst.st_mode))
        {
            totalsize += db_dir_size(pathname, &ph_size);
#ifdef _SHARDING_
            if(alloc_size)
                *alloc_size += ph_size;
#endif
        }

        totalsize += fst.st_size;
#ifdef _SHARDING_
        if(alloc_size)
            *alloc_size += fst.st_blocks * 512;
#endif

    }

    FreeDir(dirdesc);

    return totalsize;
}

Datum
pg_tablespace_size_oid(PG_FUNCTION_ARGS)
{
    Oid            tblspcOid = PG_GETARG_OID(0);
    int64        size;

#ifdef PGXC
    if (IS_PGXC_LOCAL_COORDINATOR)
        PG_RETURN_DATUM(pgxc_tablespace_size(tblspcOid));
#endif

    size = calculate_tablespace_size(tblspcOid, NULL);

    if (size < 0)
        PG_RETURN_NULL();

    PG_RETURN_INT64(size);
}

Datum
pg_tablespace_size_name(PG_FUNCTION_ARGS)
{
    Name        tblspcName = PG_GETARG_NAME(0);
    Oid            tblspcOid = get_tablespace_oid(NameStr(*tblspcName), false);
    int64        size;

#ifdef PGXC
    if (IS_PGXC_LOCAL_COORDINATOR)
        PG_RETURN_DATUM(pgxc_tablespace_size(tblspcOid));
#endif

    size = calculate_tablespace_size(tblspcOid, NULL);

    if (size < 0)
        PG_RETURN_NULL();

    PG_RETURN_INT64(size);
}

#ifdef _SHARDING_
Datum
pg_allocated_tablespace_size_oid(PG_FUNCTION_ARGS)
{
    Oid            tblspcOid = PG_GETARG_OID(0);
    int64        size;

#ifdef PGXC
    if (IS_PGXC_LOCAL_COORDINATOR)
        PG_RETURN_DATUM(pgxc_allocated_tablespace_size(tblspcOid));
#endif

    calculate_tablespace_size(tblspcOid, &size);

    if (size < 0)
        PG_RETURN_NULL();

    PG_RETURN_INT64(size);
}

Datum
pg_allocated_tablespace_size_name(PG_FUNCTION_ARGS)
{
    Name        tblspcName = PG_GETARG_NAME(0);
    Oid            tblspcOid = get_tablespace_oid(NameStr(*tblspcName), false);
    int64        size;

#ifdef PGXC
    if (IS_PGXC_LOCAL_COORDINATOR)
        PG_RETURN_DATUM(pgxc_allocated_tablespace_size(tblspcOid));
#endif

    calculate_tablespace_size(tblspcOid, &size);

    if (size < 0)
        PG_RETURN_NULL();

    PG_RETURN_INT64(size);
}
#endif

/*
 * calculate size of (one fork of) a relation
 *
 * Note: we can safely apply this to temp tables of other sessions, so there
 * is no check here or at the call sites for that.
 */
static int64
calculate_relation_size(RelFileNode *rfn, BackendId backend, ForkNumber forknum, int64 *alloc_size)
{// #lizard forgives
    int64        totalsize = 0;
#ifdef _SHARDING_
    int64        total_physical_size = 0;
#endif
    char       *relationpath;
    char        pathname[MAXPGPATH];
    unsigned int segcount = 0;

    relationpath = relpathbackend(*rfn, backend, forknum);

    for (segcount = 0;; segcount++)
    {
        struct stat fst;

        CHECK_FOR_INTERRUPTS();

        if (segcount == 0)
            snprintf(pathname, MAXPGPATH, "%s",
                     relationpath);
        else
            snprintf(pathname, MAXPGPATH, "%s.%u",
                     relationpath, segcount);

        if (stat(pathname, &fst) < 0)
        {
            if (errno == ENOENT)
                break;
            else
                ereport(ERROR,
                        (errcode_for_file_access(),
                         errmsg("could not stat file \"%s\": %m", pathname)));
        }
        totalsize += fst.st_size;
#ifdef _SHARDING_
        total_physical_size += fst.st_blocks*512;
#endif
    }

#ifdef _SHARDING_
    if(alloc_size)
        *alloc_size = total_physical_size;
#endif

    return totalsize;
}

Datum
pg_relation_size(PG_FUNCTION_ARGS)
{
    Oid            relOid = PG_GETARG_OID(0);
    text       *forkName = PG_GETARG_TEXT_PP(1);
    Relation    rel;
    Relation    child_rel;
    int64        size;
    List       *children = NIL;
#ifdef PGXC
    if (COLLECT_FROM_DATANODES(relOid))
    {
        size = pgxc_exec_sizefunc(relOid, "pg_relation_size", text_to_cstring(forkName));
        PG_RETURN_INT64(size);
    }
#endif /* PGXC */

    rel = try_relation_open(relOid, AccessShareLock);

    /*
     * Before 9.2, we used to throw an error if the relation didn't exist, but
     * that makes queries like "SELECT pg_relation_size(oid) FROM pg_class"
     * less robust, because while we scan pg_class with an MVCC snapshot,
     * someone else might drop the table. It's better to return NULL for
     * already-dropped tables than throw an error and abort the whole query.
     */
    if (rel == NULL)
        PG_RETURN_NULL();

    size = calculate_relation_size(&(rel->rd_node), rel->rd_backend,
                                   forkname_to_number(text_to_cstring(forkName)), NULL);

#ifdef _MLS_
    /* scan all partitions if exists */
    children = FetchAllParitionList(relOid);
    if (NIL != children)
    {
        ListCell *    lc;
        Oid         partoid;

        foreach(lc, children)
        {
            partoid = lfirst_oid(lc);
            
            child_rel = try_relation_open(partoid, AccessShareLock);
			/* skip calculate size of child not exists */
            if (NULL == child_rel)
            {
				continue;
            }
            size += calculate_relation_size(&(child_rel->rd_node), child_rel->rd_backend,
                                           forkname_to_number(text_to_cstring(forkName)), NULL);

            relation_close(child_rel, AccessShareLock);
        }
    }
#endif

    relation_close(rel, AccessShareLock);

    PG_RETURN_INT64(size);
}

#ifdef _SHARDING_
Datum
pg_allocated_relation_size(PG_FUNCTION_ARGS)
{
    Oid            relOid = PG_GETARG_OID(0);
    text       *forkName = PG_GETARG_TEXT_PP(1);
    Relation    rel;
    int64        size;

#ifdef PGXC
    if (COLLECT_FROM_DATANODES(relOid))
    {
        size = pgxc_exec_sizefunc(relOid, "pg_allocated_relation_size", text_to_cstring(forkName));
        PG_RETURN_INT64(size);
    }
#endif /* PGXC */

    rel = try_relation_open(relOid, AccessShareLock);

    /*
     * Before 9.2, we used to throw an error if the relation didn't exist, but
     * that makes queries like "SELECT pg_relation_size(oid) FROM pg_class"
     * less robust, because while we scan pg_class with an MVCC snapshot,
     * someone else might drop the table. It's better to return NULL for
     * already-dropped tables than throw an error and abort the whole query.
     */
    if (rel == NULL)
        PG_RETURN_NULL();

    calculate_relation_size(&(rel->rd_node), rel->rd_backend,
                                   forkname_to_number(text_to_cstring(forkName)), &size);

    relation_close(rel, AccessShareLock);

    PG_RETURN_INT64(size);
}
#endif

/*
 * Calculate total on-disk size of a TOAST relation, including its indexes.
 * Must not be applied to non-TOAST relations.
 */
static int64
calculate_toast_table_size(Oid toastrelid, int64 *alloc_size)
{// #lizard forgives
    int64        size = 0;
#ifdef _SHARDING_    
    int64        ph_size = 0;
#endif
    Relation    toastRel;
    ForkNumber    forkNum;
    ListCell   *lc;
    List       *indexlist;

#ifdef _SHARDING_
    if(alloc_size)
        *alloc_size = 0;
#endif
    toastRel = relation_open(toastrelid, AccessShareLock);

    /* toast heap size, including FSM and VM size */
    for (forkNum = 0; forkNum <= MAX_FORKNUM; forkNum++)
    {
        size += calculate_relation_size(&(toastRel->rd_node),
                                        toastRel->rd_backend, forkNum, &ph_size);
#ifdef _SHARDING_
        if(alloc_size)
            *alloc_size += ph_size;
#endif
    }

    /* toast index size, including FSM and VM size */
    indexlist = RelationGetIndexList(toastRel);

    /* Size is calculated using all the indexes available */
    foreach(lc, indexlist)
    {
        Relation    toastIdxRel;

        toastIdxRel = relation_open(lfirst_oid(lc),
                                    AccessShareLock);
        for (forkNum = 0; forkNum <= MAX_FORKNUM; forkNum++)
            size += calculate_relation_size(&(toastIdxRel->rd_node),
                                            toastIdxRel->rd_backend, forkNum, &ph_size);
#ifdef _SHARDING_
        if(alloc_size)
            *alloc_size += ph_size;
#endif
        relation_close(toastIdxRel, AccessShareLock);
    }
    list_free(indexlist);
    relation_close(toastRel, AccessShareLock);

    return size;
}

/*
 * Calculate total on-disk size of a given table,
 * including FSM and VM, plus TOAST table if any.
 * Indexes other than the TOAST table's index are not included.
 *
 * Note that this also behaves sanely if applied to an index or toast table;
 * those won't have attached toast tables, but they can have multiple forks.
 */
static int64
calculate_table_size(Relation rel, int64 *alloc_size)
{// #lizard forgives
    int64        size = 0;
#ifdef _SHARDING_
    int64        ph_size = 0;
#endif
    ForkNumber    forkNum;

#ifdef _SHARDING_
    if(alloc_size)
        *alloc_size = 0;
#endif
    /*
     * heap size, including FSM and VM
     */
    for (forkNum = 0; forkNum <= MAX_FORKNUM; forkNum++)
    {
        size += calculate_relation_size(&(rel->rd_node), rel->rd_backend,
                                        forkNum,
                                        &ph_size);
#ifdef _SHARDING_
    if(alloc_size)
        *alloc_size += ph_size;
#endif    
    }
    
    /*
     * Size of toast relation
     */
    if (OidIsValid(rel->rd_rel->reltoastrelid))
    {
        size += calculate_toast_table_size(rel->rd_rel->reltoastrelid, &ph_size);
#ifdef _SHARDING_
        if(alloc_size)
            *alloc_size += ph_size;
#endif
    }

    return size;
}

/*
 * Calculate total on-disk size of all indexes attached to the given table.
 *
 * Can be applied safely to an index, but you'll just get zero.
 */
static int64
calculate_indexes_size(Relation rel)
{
    int64        size = 0;

    /*
     * Aggregate all indexes on the given relation
     */
    if (rel->rd_rel->relhasindex)
    {
        List       *index_oids = RelationGetIndexList(rel);
        ListCell   *cell;

        foreach(cell, index_oids)
        {
            Oid            idxOid = lfirst_oid(cell);
            Relation    idxRel;
            ForkNumber    forkNum;

            idxRel = relation_open(idxOid, AccessShareLock);

            for (forkNum = 0; forkNum <= MAX_FORKNUM; forkNum++)
                size += calculate_relation_size(&(idxRel->rd_node),
                                                idxRel->rd_backend,
                                                forkNum,
                                                NULL);

            relation_close(idxRel, AccessShareLock);
        }

        list_free(index_oids);
    }

    return size;
}

Datum
pg_table_size(PG_FUNCTION_ARGS)
{// #lizard forgives
    Oid            relOid = PG_GETARG_OID(0);
    Relation    rel;
    int64        size = 0;

#ifdef PGXC
    if (COLLECT_FROM_DATANODES(relOid))
        PG_RETURN_INT64(pgxc_exec_sizefunc(relOid, "pg_table_size", NULL));
#endif /* PGXC */

    rel = try_relation_open(relOid, AccessShareLock);

    if (rel == NULL)
        PG_RETURN_NULL();

#ifdef __TBASE__
    if(!isRestoreMode && IS_PGXC_DATANODE && RELATION_IS_INTERVAL(rel))
    {
        List *        parts;
        ListCell *    lc;
        Oid         partoid;
        Relation    partrel;
        parts = RelationGetAllPartitions(rel);

        size = 0;
        foreach(lc, parts)
        {
            partoid = lfirst_oid(lc);
            partrel = try_relation_open(partoid, AccessShareLock);
            if(!partrel)
            {
                elog(WARNING, "partition %d of relation %s can not be found", partoid, RelationGetRelationName(rel));
                continue;
            }
            size += calculate_table_size(partrel, NULL);

            relation_close(partrel, AccessShareLock);
        }
    }
    else if (!isRestoreMode && IS_PGXC_DATANODE && (RELKIND_PARTITIONED_TABLE == rel->rd_rel->relkind))
    {
        /*
         * this is orignal partition, use \d+ show the parent and all its children 'table size' together,
         * while, if it is a sub parent, the result contains this sub parent table and all its children. 
         */
        List     *  children;
        ListCell *    lc;
        Oid         partoid;
        Relation    child_rel;
        
        children = find_all_inheritors(relOid, AccessShareLock, NULL);
        
        foreach(lc, children)
        {
            partoid = lfirst_oid(lc);
            
            child_rel = try_relation_open(partoid, AccessShareLock);
            if (NULL == child_rel)
            {
                elog(WARNING, "partition %d of relation %s can not be found", partoid, RelationGetRelationName(rel));
                continue;
            }
            size += calculate_table_size(child_rel, NULL);

            relation_close(child_rel, AccessShareLock);
        }
    }
    else
    {
#endif
    size = calculate_table_size(rel, NULL);
#ifdef __TBASE__
    }
#endif

    relation_close(rel, AccessShareLock);

    PG_RETURN_INT64(size);
}

#ifdef _SHARDING_
Datum
pg_allocated_table_size(PG_FUNCTION_ARGS)
{// #lizard forgives
    Oid            relOid = PG_GETARG_OID(0);
    Relation    rel;
    int64        size;

#ifdef PGXC
    if (COLLECT_FROM_DATANODES(relOid))
        PG_RETURN_INT64(pgxc_exec_sizefunc(relOid, "pg_allocated_table_size", NULL));
#endif /* PGXC */

    rel = try_relation_open(relOid, AccessShareLock);

    if (rel == NULL)
        PG_RETURN_NULL();

#ifdef __TBASE__
    if(!isRestoreMode && IS_PGXC_DATANODE && RELATION_IS_INTERVAL(rel))
    {
        List *        parts;
        ListCell *    lc;
        Oid         partoid;
        Relation    partrel;
        int64        asize;
        parts = RelationGetAllPartitions(rel);

        size = 0;
        foreach(lc, parts)
        {
            partoid = lfirst_oid(lc);
            partrel = try_relation_open(partoid, AccessShareLock);
            if(!partrel)
            {
                elog(WARNING, "partition %d of relation %s can not be found", partoid, RelationGetRelationName(rel));
                continue;
            }
            
            calculate_table_size(partrel, &asize);

            size += asize;

            relation_close(partrel, AccessShareLock);
        }
    }
    else
    {
#endif
    calculate_table_size(rel, &size);
#ifdef __TBASE__
    }
#endif

    relation_close(rel, AccessShareLock);

    PG_RETURN_INT64(size);
}
#endif

Datum
pg_indexes_size(PG_FUNCTION_ARGS)
{
    Oid            relOid = PG_GETARG_OID(0);
    Relation    rel;
    int64        size;

#ifdef PGXC
    if (COLLECT_FROM_DATANODES(relOid))
        PG_RETURN_INT64(pgxc_exec_sizefunc(relOid, "pg_indexes_size", NULL));
#endif /* PGXC */

    rel = try_relation_open(relOid, AccessShareLock);

    if (rel == NULL)
        PG_RETURN_NULL();

    size = calculate_indexes_size(rel);

    relation_close(rel, AccessShareLock);

    PG_RETURN_INT64(size);
}

/*
 *    Compute the on-disk size of all files for the relation,
 *    including heap data, index data, toast data, FSM, VM.
 */
static int64
calculate_total_relation_size(Relation rel, int64 *alloc_size)
{
    int64        size;
#ifdef _SHARDING_
    int64        ph_size;

    if(alloc_size)
        *alloc_size = 0;
#endif
    /*
     * Aggregate the table size, this includes size of the heap, toast and
     * toast index with free space and visibility map
     */
    size = calculate_table_size(rel, &ph_size);
#ifdef _SHARDING_
    if(alloc_size)
        *alloc_size += ph_size;
#endif

    /*
     * Add size of all attached indexes as well
     */
    ph_size = calculate_indexes_size(rel);
    size += ph_size;
#ifdef _SHARDING_
    if(alloc_size)
        *alloc_size += ph_size;
#endif    

    return size;
}

#ifdef __TBASE__
int64
get_total_relation_size(Relation rel)
{
    int64 size = 0;
    
    if (rel)
    {
        size = calculate_total_relation_size(rel, NULL);
    }

    return size;
}
#endif

Datum
pg_total_relation_size(PG_FUNCTION_ARGS)
{
    Oid            relOid = PG_GETARG_OID(0);
    Relation    rel;
    int64        size;

#ifdef PGXC
    if (COLLECT_FROM_DATANODES(relOid))
        PG_RETURN_INT64(pgxc_exec_sizefunc(relOid, "pg_total_relation_size", NULL));
#endif /* PGXC */

    rel = try_relation_open(relOid, AccessShareLock);

    if (rel == NULL)
        PG_RETURN_NULL();

    size = calculate_total_relation_size(rel, NULL);

    relation_close(rel, AccessShareLock);

    PG_RETURN_INT64(size);
}

#ifdef _SHARDING_
Datum
pg_allocated_total_relation_size(PG_FUNCTION_ARGS)
{
    Oid            relOid = PG_GETARG_OID(0);
    Relation    rel;
    int64        size;

#ifdef PGXC
    if (COLLECT_FROM_DATANODES(relOid))
        PG_RETURN_INT64(pgxc_exec_sizefunc(relOid, "pg_allocated_total_relation_size", NULL));
#endif /* PGXC */

    rel = try_relation_open(relOid, AccessShareLock);

    if (rel == NULL)
        PG_RETURN_NULL();

    calculate_total_relation_size(rel, &size);

    relation_close(rel, AccessShareLock);

    PG_RETURN_INT64(size);
}

#endif

/*
 * formatting with size units
 */
Datum
pg_size_pretty(PG_FUNCTION_ARGS)
{
    int64        size = PG_GETARG_INT64(0);
    char        buf[64];
    int64        limit = 10 * 1024;
    int64        limit2 = limit * 2 - 1;

    if (Abs(size) < limit)
        snprintf(buf, sizeof(buf), INT64_FORMAT " bytes", size);
    else
    {
        size >>= 9;                /* keep one extra bit for rounding */
        if (Abs(size) < limit2)
            snprintf(buf, sizeof(buf), INT64_FORMAT " kB",
                     half_rounded(size));
        else
        {
            size >>= 10;
            if (Abs(size) < limit2)
                snprintf(buf, sizeof(buf), INT64_FORMAT " MB",
                         half_rounded(size));
            else
            {
                size >>= 10;
                if (Abs(size) < limit2)
                    snprintf(buf, sizeof(buf), INT64_FORMAT " GB",
                             half_rounded(size));
                else
                {
                    size >>= 10;
                    snprintf(buf, sizeof(buf), INT64_FORMAT " TB",
                             half_rounded(size));
                }
            }
        }
    }

    PG_RETURN_TEXT_P(cstring_to_text(buf));
}

static char *
numeric_to_cstring(Numeric n)
{
    Datum        d = NumericGetDatum(n);

    return DatumGetCString(DirectFunctionCall1(numeric_out, d));
}

static Numeric
int64_to_numeric(int64 v)
{
    Datum        d = Int64GetDatum(v);

    return DatumGetNumeric(DirectFunctionCall1(int8_numeric, d));
}

static bool
numeric_is_less(Numeric a, Numeric b)
{
    Datum        da = NumericGetDatum(a);
    Datum        db = NumericGetDatum(b);

    return DatumGetBool(DirectFunctionCall2(numeric_lt, da, db));
}

static Numeric
numeric_absolute(Numeric n)
{
    Datum        d = NumericGetDatum(n);
    Datum        result;

    result = DirectFunctionCall1(numeric_abs, d);
    return DatumGetNumeric(result);
}

static Numeric
numeric_half_rounded(Numeric n)
{
    Datum        d = NumericGetDatum(n);
    Datum        zero;
    Datum        one;
    Datum        two;
    Datum        result;

    zero = DirectFunctionCall1(int8_numeric, Int64GetDatum(0));
    one = DirectFunctionCall1(int8_numeric, Int64GetDatum(1));
    two = DirectFunctionCall1(int8_numeric, Int64GetDatum(2));

    if (DatumGetBool(DirectFunctionCall2(numeric_ge, d, zero)))
        d = DirectFunctionCall2(numeric_add, d, one);
    else
        d = DirectFunctionCall2(numeric_sub, d, one);

    result = DirectFunctionCall2(numeric_div_trunc, d, two);
    return DatumGetNumeric(result);
}

static Numeric
numeric_shift_right(Numeric n, unsigned count)
{
    Datum        d = NumericGetDatum(n);
    Datum        divisor_int64;
    Datum        divisor_numeric;
    Datum        result;

    divisor_int64 = Int64GetDatum((int64) (1 << count));
    divisor_numeric = DirectFunctionCall1(int8_numeric, divisor_int64);
    result = DirectFunctionCall2(numeric_div_trunc, d, divisor_numeric);
    return DatumGetNumeric(result);
}

Datum
pg_size_pretty_numeric(PG_FUNCTION_ARGS)
{
    Numeric        size = PG_GETARG_NUMERIC(0);
    Numeric        limit,
                limit2;
    char       *result;

    limit = int64_to_numeric(10 * 1024);
    limit2 = int64_to_numeric(10 * 1024 * 2 - 1);

    if (numeric_is_less(numeric_absolute(size), limit))
    {
        result = psprintf("%s bytes", numeric_to_cstring(size));
    }
    else
    {
        /* keep one extra bit for rounding */
        /* size >>= 9 */
        size = numeric_shift_right(size, 9);

        if (numeric_is_less(numeric_absolute(size), limit2))
        {
            size = numeric_half_rounded(size);
            result = psprintf("%s kB", numeric_to_cstring(size));
        }
        else
        {
            /* size >>= 10 */
            size = numeric_shift_right(size, 10);
            if (numeric_is_less(numeric_absolute(size), limit2))
            {
                size = numeric_half_rounded(size);
                result = psprintf("%s MB", numeric_to_cstring(size));
            }
            else
            {
                /* size >>= 10 */
                size = numeric_shift_right(size, 10);

                if (numeric_is_less(numeric_absolute(size), limit2))
                {
                    size = numeric_half_rounded(size);
                    result = psprintf("%s GB", numeric_to_cstring(size));
                }
                else
                {
                    /* size >>= 10 */
                    size = numeric_shift_right(size, 10);
                    size = numeric_half_rounded(size);
                    result = psprintf("%s TB", numeric_to_cstring(size));
                }
            }
        }
    }

    PG_RETURN_TEXT_P(cstring_to_text(result));
}

/*
 * Convert a human-readable size to a size in bytes
 */
Datum
pg_size_bytes(PG_FUNCTION_ARGS)
{// #lizard forgives
    text       *arg = PG_GETARG_TEXT_PP(0);
    char       *str,
               *strptr,
               *endptr;
    char        saved_char;
    Numeric        num;
    int64        result;
    bool        have_digits = false;

    str = text_to_cstring(arg);

    /* Skip leading whitespace */
    strptr = str;
    while (isspace((unsigned char) *strptr))
        strptr++;

    /* Check that we have a valid number and determine where it ends */
    endptr = strptr;

    /* Part (1): sign */
    if (*endptr == '-' || *endptr == '+')
        endptr++;

    /* Part (2): main digit string */
    if (isdigit((unsigned char) *endptr))
    {
        have_digits = true;
        do
            endptr++;
        while (isdigit((unsigned char) *endptr));
    }

    /* Part (3): optional decimal point and fractional digits */
    if (*endptr == '.')
    {
        endptr++;
        if (isdigit((unsigned char) *endptr))
        {
            have_digits = true;
            do
                endptr++;
            while (isdigit((unsigned char) *endptr));
        }
    }

    /* Complain if we don't have a valid number at this point */
    if (!have_digits)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("invalid size: \"%s\"", str)));

    /* Part (4): optional exponent */
    if (*endptr == 'e' || *endptr == 'E')
    {
        long        exponent;
        char       *cp;

        /*
         * Note we might one day support EB units, so if what follows 'E'
         * isn't a number, just treat it all as a unit to be parsed.
         */
        exponent = strtol(endptr + 1, &cp, 10);
        (void) exponent;        /* Silence -Wunused-result warnings */
        if (cp > endptr + 1)
            endptr = cp;
    }

    /*
     * Parse the number, saving the next character, which may be the first
     * character of the unit string.
     */
    saved_char = *endptr;
    *endptr = '\0';

    num = DatumGetNumeric(DirectFunctionCall3(numeric_in,
                                              CStringGetDatum(strptr),
                                              ObjectIdGetDatum(InvalidOid),
                                              Int32GetDatum(-1)));

    *endptr = saved_char;

    /* Skip whitespace between number and unit */
    strptr = endptr;
    while (isspace((unsigned char) *strptr))
        strptr++;

    /* Handle possible unit */
    if (*strptr != '\0')
    {
        int64        multiplier = 0;

        /* Trim any trailing whitespace */
        endptr = str + VARSIZE_ANY_EXHDR(arg) - 1;

        while (isspace((unsigned char) *endptr))
            endptr--;

        endptr++;
        *endptr = '\0';

        /* Parse the unit case-insensitively */
        if (pg_strcasecmp(strptr, "bytes") == 0)
            multiplier = (int64) 1;
        else if (pg_strcasecmp(strptr, "kb") == 0)
            multiplier = (int64) 1024;
        else if (pg_strcasecmp(strptr, "mb") == 0)
            multiplier = ((int64) 1024) * 1024;

        else if (pg_strcasecmp(strptr, "gb") == 0)
            multiplier = ((int64) 1024) * 1024 * 1024;

        else if (pg_strcasecmp(strptr, "tb") == 0)
            multiplier = ((int64) 1024) * 1024 * 1024 * 1024;

        else
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("invalid size: \"%s\"", text_to_cstring(arg)),
                     errdetail("Invalid size unit: \"%s\".", strptr),
                     errhint("Valid units are \"bytes\", \"kB\", \"MB\", \"GB\", and \"TB\".")));

        if (multiplier > 1)
        {
            Numeric        mul_num;

            mul_num = DatumGetNumeric(DirectFunctionCall1(int8_numeric,
                                                          Int64GetDatum(multiplier)));

            num = DatumGetNumeric(DirectFunctionCall2(numeric_mul,
                                                      NumericGetDatum(mul_num),
                                                      NumericGetDatum(num)));
        }
    }

    result = DatumGetInt64(DirectFunctionCall1(numeric_int8,
                                               NumericGetDatum(num)));

    PG_RETURN_INT64(result);
}

/*
 * Get the filenode of a relation
 *
 * This is expected to be used in queries like
 *        SELECT pg_relation_filenode(oid) FROM pg_class;
 * That leads to a couple of choices.  We work from the pg_class row alone
 * rather than actually opening each relation, for efficiency.  We don't
 * fail if we can't find the relation --- some rows might be visible in
 * the query's MVCC snapshot even though the relations have been dropped.
 * (Note: we could avoid using the catcache, but there's little point
 * because the relation mapper also works "in the now".)  We also don't
 * fail if the relation doesn't have storage.  In all these cases it
 * seems better to quietly return NULL.
 */
Datum
pg_relation_filenode(PG_FUNCTION_ARGS)
{// #lizard forgives
    Oid            relid = PG_GETARG_OID(0);
    Oid            result;
    HeapTuple    tuple;
    Form_pg_class relform;

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tuple))
        PG_RETURN_NULL();
    relform = (Form_pg_class) GETSTRUCT(tuple);

    switch (relform->relkind)
    {
        case RELKIND_RELATION:
        case RELKIND_MATVIEW:
        case RELKIND_INDEX:
        case RELKIND_SEQUENCE:
        case RELKIND_TOASTVALUE:
            /* okay, these have storage */
            if (relform->relfilenode)
                result = relform->relfilenode;
            else                /* Consult the relation mapper */
                result = RelationMapOidToFilenode(relid,
                                                  relform->relisshared);
            break;

        default:
            /* no storage, return NULL */
            result = InvalidOid;
            break;
    }

    ReleaseSysCache(tuple);

    if (!OidIsValid(result))
        PG_RETURN_NULL();

    PG_RETURN_OID(result);
}

/*
 * Get the relation via (reltablespace, relfilenode)
 *
 * This is expected to be used when somebody wants to match an individual file
 * on the filesystem back to its table. That's not trivially possible via
 * pg_class, because that doesn't contain the relfilenodes of shared and nailed
 * tables.
 *
 * We don't fail but return NULL if we cannot find a mapping.
 *
 * InvalidOid can be passed instead of the current database's default
 * tablespace.
 */
Datum
pg_filenode_relation(PG_FUNCTION_ARGS)
{
    Oid            reltablespace = PG_GETARG_OID(0);
    Oid            relfilenode = PG_GETARG_OID(1);
    Oid            heaprel = InvalidOid;

    heaprel = RelidByRelfilenode(reltablespace, relfilenode);

    if (!OidIsValid(heaprel))
        PG_RETURN_NULL();
    else
        PG_RETURN_OID(heaprel);
}

/*
 * Get the pathname (relative to $PGDATA) of a relation
 *
 * See comments for pg_relation_filenode.
 */
Datum
pg_relation_filepath(PG_FUNCTION_ARGS)
{// #lizard forgives
    Oid            relid = PG_GETARG_OID(0);
    HeapTuple    tuple;
    Form_pg_class relform;
    RelFileNode rnode;
    BackendId    backend;
    char       *path;

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tuple))
        PG_RETURN_NULL();
    relform = (Form_pg_class) GETSTRUCT(tuple);

    switch (relform->relkind)
    {
        case RELKIND_RELATION:
        case RELKIND_MATVIEW:
        case RELKIND_INDEX:
        case RELKIND_SEQUENCE:
        case RELKIND_TOASTVALUE:
            /* okay, these have storage */

            /* This logic should match RelationInitPhysicalAddr */
            if (relform->reltablespace)
                rnode.spcNode = relform->reltablespace;
            else
                rnode.spcNode = MyDatabaseTableSpace;
            if (rnode.spcNode == GLOBALTABLESPACE_OID)
                rnode.dbNode = InvalidOid;
            else
                rnode.dbNode = MyDatabaseId;
            if (relform->relfilenode)
                rnode.relNode = relform->relfilenode;
            else                /* Consult the relation mapper */
                rnode.relNode = RelationMapOidToFilenode(relid,
                                                         relform->relisshared);
            break;

        default:
            /* no storage, return NULL */
            rnode.relNode = InvalidOid;
            /* some compilers generate warnings without these next two lines */
            rnode.dbNode = InvalidOid;
            rnode.spcNode = InvalidOid;
            break;
    }

    if (!OidIsValid(rnode.relNode))
    {
        ReleaseSysCache(tuple);
        PG_RETURN_NULL();
    }

    /* Determine owning backend. */
    switch (relform->relpersistence)
    {
        case RELPERSISTENCE_UNLOGGED:
        case RELPERSISTENCE_PERMANENT:
            backend = InvalidBackendId;
            break;
        case RELPERSISTENCE_TEMP:
            if (isTempOrTempToastNamespace(relform->relnamespace))
#ifdef XCP
                backend = OidIsValid(MyCoordId) ? InvalidBackendId : MyBackendId;
#else
                backend = BackendIdForTempRelations();
#endif
            else
            {
                /* Do it the hard way. */
                backend = GetTempNamespaceBackendId(relform->relnamespace);
                Assert(backend != InvalidBackendId);
            }
            break;
        default:
            elog(ERROR, "invalid relpersistence: %c", relform->relpersistence);
            backend = InvalidBackendId; /* placate compiler */
            break;
    }

    ReleaseSysCache(tuple);

    path = relpathbackend(rnode, backend, MAIN_FORKNUM);

    PG_RETURN_TEXT_P(cstring_to_text(path));
}


#ifdef PGXC

/*
 * pgxc_tablespace_size
 * Given a tablespace oid, return sum of pg_tablespace_size() executed on all the Datanodes
 */
static Datum
pgxc_tablespace_size(Oid tsOid)
{
    StringInfoData  buf;
    char           *tsname = get_tablespace_name(tsOid);
    Oid                *coOids, *dnOids;
    int numdnodes, numcoords;

    if (!tsname)
        ereport(ERROR,
            (ERRCODE_UNDEFINED_OBJECT,
             errmsg("tablespace with OID %u does not exist", tsOid)));

    initStringInfo(&buf);
    appendStringInfo(&buf, "SELECT pg_catalog.pg_tablespace_size('%s')", tsname);

    PgxcNodeGetOids(&coOids, &dnOids, &numcoords, &numdnodes, false);

    return pgxc_execute_on_nodes(numdnodes, dnOids, buf.data);
}

#ifdef _SHARDING_
static Datum
pgxc_allocated_tablespace_size(Oid tsOid)
{
    StringInfoData  buf;
    char           *tsname = get_tablespace_name(tsOid);
    Oid                *coOids, *dnOids;
    int numdnodes, numcoords;

    if (!tsname)
        ereport(ERROR,
            (ERRCODE_UNDEFINED_OBJECT,
             errmsg("tablespace with OID %u does not exist", tsOid)));

    initStringInfo(&buf);
    appendStringInfo(&buf, "SELECT pg_catalog.pg_allocated_tablespace_size('%s')", tsname);

    PgxcNodeGetOids(&coOids, &dnOids, &numcoords, &numdnodes, false);

    return pgxc_execute_on_nodes(numdnodes, dnOids, buf.data);
}
#endif

/*
 * pgxc_database_size
 * Given a dboid, return sum of pg_database_size() executed on all the Datanodes
 */
static Datum
pgxc_database_size(Oid dbOid)
{
    StringInfoData  buf;
    char           *dbname = get_database_name(dbOid);
    Oid                *coOids, *dnOids;
    int numdnodes, numcoords;

    if (!dbname)
        ereport(ERROR,
            (ERRCODE_UNDEFINED_DATABASE,
             errmsg("database with OID %u does not exist", dbOid)));

    initStringInfo(&buf);
    appendStringInfo(&buf, "SELECT pg_catalog.pg_database_size('%s')", dbname);

    PgxcNodeGetOids(&coOids, &dnOids, &numcoords, &numdnodes, false);

    return pgxc_execute_on_nodes(numdnodes, dnOids, buf.data);
}

#ifdef _SHARDING_
static Datum
pgxc_allocated_database_size(Oid dbOid)
{
    StringInfoData  buf;
    char           *dbname = get_database_name(dbOid);
    Oid                *coOids, *dnOids;
    int numdnodes, numcoords;

    if (!dbname)
        ereport(ERROR,
            (ERRCODE_UNDEFINED_DATABASE,
             errmsg("database with OID %u does not exist", dbOid)));

    initStringInfo(&buf);
    appendStringInfo(&buf, "SELECT pg_catalog.pg_allocated_database_size('%s')", dbname);

    PgxcNodeGetOids(&coOids, &dnOids, &numcoords, &numdnodes, false);

    return pgxc_execute_on_nodes(numdnodes, dnOids, buf.data);
}
#endif

/*
 * pgxc_execute_on_nodes
 * Execute 'query' on all the nodes in 'nodelist', and returns int64 datum
 * which has the sum of all the results. If multiples nodes are involved, it
 * assumes that the query returns exactly one row with one attribute of type
 * int64. If there is a single node, it just returns the datum as-is without
 * checking the type of the returned value.
 *
 * Note: nodelist should either have all coordinators or all datanodes in it.
 * Mixing both will result an error being thrown
 */
Datum
pgxc_execute_on_nodes(int numnodes, Oid *nodelist, char *query)
{// #lizard forgives
    int             i;
    int64           total_size = 0;
    int64           size = 0;
    Datum            datum = (Datum) 0;
    bool        isnull = false;

#ifdef XCP
    EState           *estate;
    MemoryContext    oldcontext;
    RemoteQuery       *plan;
    RemoteQueryState   *pstate;
    TupleTableSlot       *result = NULL;
    Var               *dummy;

    /*
     * Make up RemoteQuery plan node
     */
    plan = makeNode(RemoteQuery);
    plan->combine_type = COMBINE_TYPE_NONE;
    plan->exec_nodes = makeNode(ExecNodes);
    plan->exec_type = EXEC_ON_NONE;

    for (i = 0; i < numnodes; i++)
    {
        char ntype = PGXC_NODE_NONE;
        plan->exec_nodes->nodeList = lappend_int(plan->exec_nodes->nodeList,
            PGXCNodeGetNodeId(nodelist[i], &ntype));
        if (ntype == PGXC_NODE_NONE)
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Unknown node Oid: %u", nodelist[i])));
        else if (ntype == PGXC_NODE_COORDINATOR) 
        {
            if (plan->exec_type == EXEC_ON_DATANODES)
                ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Cannot mix datanodes and coordinators")));
            plan->exec_type = EXEC_ON_COORDS;
        }
        else
        {
            if (plan->exec_type == EXEC_ON_COORDS)
                ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Cannot mix datanodes and coordinators")));
            plan->exec_type = EXEC_ON_DATANODES;
        }

    }
    plan->sql_statement = query;
    plan->force_autocommit = false;
    /*
     * We only need the target entry to determine result data type.
     * So create dummy even if real expression is a function.
     */
    dummy = makeVar(1, 1, INT8OID, 0, InvalidOid, 0);
    plan->scan.plan.targetlist = lappend(plan->scan.plan.targetlist,
                                      makeTargetEntry((Expr *) dummy, 1, NULL, false));
    /* prepare to execute */
    estate = CreateExecutorState();
    oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);
    estate->es_snapshot = GetActiveSnapshot();
    pstate = ExecInitRemoteQuery(plan, estate, 0);
    MemoryContextSwitchTo(oldcontext);

    result = ExecRemoteQuery((PlanState *) pstate);
    while (result != NULL && !TupIsNull(result))
    {
        datum = slot_getattr(result, 1, &isnull);
        result = ExecRemoteQuery((PlanState *) pstate);

        /* For single node, don't assume the type of datum. It can be bool also. */
        if (numnodes == 1)
            continue;
        /* We should not cast a null into an int */
        if (isnull)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Expected Int64 but got null instead "
                        "while executing query '%s'",
                        query)));
            break;
        }

        size = DatumGetInt64(datum);
        total_size += size;
    }
    ExecEndRemoteQuery(pstate);
#else
    /*
     * Connect to SPI manager
     */
    if ((ret = SPI_connect()) < 0)
        /* internal error */
        elog(ERROR, "SPI connect failure - returned %d", ret);

    initStringInfo(&buf);

    /* Get pg_***_size function results from all Datanodes */
    for (i = 0; i < numnodes; i++)
    {
        nodename = get_pgxc_nodename(nodelist[i]);

        ret = SPI_execute_direct(query, nodename);
        spi_tupdesc = SPI_tuptable->tupdesc;

        if (ret != SPI_OK_SELECT)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("failed to execute query '%s' on node '%s'",
                            query, nodename)));
        }

        /*
         * The query must always return one row having one column:
         */
        Assert(SPI_processed == 1 && spi_tupdesc->natts == 1);

        datum = SPI_getbinval(SPI_tuptable->vals[0], spi_tupdesc, 1, &isnull);

        /* For single node, don't assume the type of datum. It can be bool also. */
        if (numnodes == 1)
            break;

        size = DatumGetInt64(datum);
        total_size += size;
    }

    SPI_finish();
#endif

    if (numnodes == 1)
    {
        /* 
         * if result is NULL, so no need to check isnull value, just return 0
         * isnull also needs explict asignment.
         */
        if (isnull 
#ifdef _MLS_
            && (NULL != result))
#endif            
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Expected datum but got null instead "
                        "while executing query '%s'",
                        query)));
        PG_RETURN_DATUM(datum);
    }
    else
        PG_RETURN_INT64(total_size);
}


/*
 * pgxc_exec_sizefunc
 * Execute the given object size system function on all the Datanodes associated
 * with relOid, and return the sum of all.
 *
 * Args:
 *
 * relOid: Oid of the table for which the object size function is to be executed.
 *
 * funcname: Name of the system function.
 *
 * extra_arg: The first argument to such sys functions is always table name.
 * Some functions can have a second argument. To pass this argument, extra_arg
 * is used. Currently only pg_relation_size() is the only one that requires
 * a 2nd argument: fork text.
 */
static int64
pgxc_exec_sizefunc(Oid relOid, char *funcname, char *extra_arg)
{
    int             numnodes;
    Oid            *nodelist;
    char           *relname = NULL;
    StringInfoData  buf;
    Relation        rel;

    rel = relation_open(relOid, AccessShareLock);

    if (rel->rd_locator_info)
    /* get relation name including any needed schema prefix and quoting */
    relname = quote_qualified_identifier(get_namespace_name(rel->rd_rel->relnamespace),
                                         RelationGetRelationName(rel));
    initStringInfo(&buf);
    if (!extra_arg)
        appendStringInfo(&buf, "SELECT pg_catalog.%s('%s')", funcname, relname);
    else
        appendStringInfo(&buf, "SELECT pg_catalog.%s('%s', '%s')", funcname, relname, extra_arg);

    numnodes = get_pgxc_classnodes(RelationGetRelid(rel), &nodelist);

    relation_close(rel, AccessShareLock);

    return DatumGetInt64(pgxc_execute_on_nodes(numnodes, nodelist, buf.data));
}

#endif /* PGXC */
