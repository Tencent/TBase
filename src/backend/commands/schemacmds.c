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
 * schemacmds.c
 *      schema creation/manipulation commands
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *      src/backend/commands/schemacmds.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/heapam.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_authid.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_namespace.h"
#include "commands/dbcommands.h"
#include "commands/event_trigger.h"
#include "commands/schemacmds.h"
#include "miscadmin.h"
#include "parser/parse_utilcmd.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#ifdef PGXC
#include "pgxc/pgxc.h"
#include "pgxc/planner.h"
#endif
#ifdef _MLS_
#include "utils/mls.h"
#endif

static void AlterSchemaOwner_internal(HeapTuple tup, Relation rel, Oid newOwnerId);

/*
 * CREATE SCHEMA
 *
 * Note: caller should pass in location information for the whole
 * CREATE SCHEMA statement, which in turn we pass down as the location
 * of the component commands.  This comports with our general plan of
 * reporting location/len for the whole command even when executing
 * a subquery.
 */
Oid
CreateSchemaCommand(CreateSchemaStmt *stmt, const char *queryString,
                    bool sentToRemote,
                    int stmt_location, int stmt_len)
{// #lizard forgives
    const char *schemaName = stmt->schemaname;
    Oid            namespaceId;
    OverrideSearchPath *overridePath;
    List       *parsetree_list;
    ListCell   *parsetree_item;
    Oid            owner_uid;
    Oid            saved_uid;
    int            save_sec_context;
    AclResult    aclresult;
    ObjectAddress address;

    GetUserIdAndSecContext(&saved_uid, &save_sec_context);

    /*
     * Who is supposed to own the new schema?
     */
    if (stmt->authrole)
        owner_uid = get_rolespec_oid(stmt->authrole, false);
    else
        owner_uid = saved_uid;

    /* fill schema name with the user name if not specified */
    if (!schemaName)
    {
        HeapTuple    tuple;

        tuple = SearchSysCache1(AUTHOID, ObjectIdGetDatum(owner_uid));
        if (!HeapTupleIsValid(tuple))
            elog(ERROR, "cache lookup failed for role %u", owner_uid);
        schemaName =
            pstrdup(NameStr(((Form_pg_authid) GETSTRUCT(tuple))->rolname));
        ReleaseSysCache(tuple);
    }

    /*
     * To create a schema, must have schema-create privilege on the current
     * database and must be able to become the target role (this does not
     * imply that the target role itself must have create-schema privilege).
     * The latter provision guards against "giveaway" attacks.  Note that a
     * superuser will always have both of these privileges a fortiori.
     */
    aclresult = pg_database_aclcheck(MyDatabaseId, saved_uid, ACL_CREATE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, ACL_KIND_DATABASE,
                       get_database_name(MyDatabaseId));

    check_is_member_of_role(saved_uid, owner_uid);

    /* Additional check to protect reserved schema names */
    if (!allowSystemTableMods && IsReservedName(schemaName))
        ereport(ERROR,
                (errcode(ERRCODE_RESERVED_NAME),
                 errmsg("unacceptable schema name \"%s\"", schemaName),
                 errdetail("The prefix \"pg_\" is reserved for system schemas.")));

    /*
     * If if_not_exists was given and the schema already exists, bail out.
     * (Note: we needn't check this when not if_not_exists, because
     * NamespaceCreate will complain anyway.)  We could do this before making
     * the permissions checks, but since CREATE TABLE IF NOT EXISTS makes its
     * creation-permission check first, we do likewise.
     */
    if (stmt->if_not_exists &&
        SearchSysCacheExists1(NAMESPACENAME, PointerGetDatum(schemaName)))
    {
        ereport(NOTICE,
                (errcode(ERRCODE_DUPLICATE_SCHEMA),
                 errmsg("schema \"%s\" already exists, skipping",
                        schemaName)));
        return InvalidOid;
    }

    /*
     * If the requested authorization is different from the current user,
     * temporarily set the current user so that the object(s) will be created
     * with the correct ownership.
     *
     * (The setting will be restored at the end of this routine, or in case of
     * error, transaction abort will clean things up.)
     */
    if (saved_uid != owner_uid)
        SetUserIdAndSecContext(owner_uid,
                               save_sec_context | SECURITY_LOCAL_USERID_CHANGE);

    /* Create the schema's namespace */
    namespaceId = NamespaceCreate(schemaName, owner_uid, false);

    /* Advance cmd counter to make the namespace visible */
    CommandCounterIncrement();

    /*
     * Temporarily make the new namespace be the front of the search path, as
     * well as the default creation target namespace.  This will be undone at
     * the end of this routine, or upon error.
     */
    overridePath = GetOverrideSearchPath(CurrentMemoryContext);
    overridePath->schemas = lcons_oid(namespaceId, overridePath->schemas);
    /* XXX should we clear overridePath->useTemp? */
    PushOverrideSearchPath(overridePath);

    /*
     * Report the new schema to possibly interested event triggers.  Note we
     * must do this here and not in ProcessUtilitySlow because otherwise the
     * objects created below are reported before the schema, which would be
     * wrong.
     */
    ObjectAddressSet(address, NamespaceRelationId, namespaceId);
    EventTriggerCollectSimpleCommand(address, InvalidObjectAddress,
                                     (Node *) stmt);

    /*
     * Examine the list of commands embedded in the CREATE SCHEMA command, and
     * reorganize them into a sequentially executable order with no forward
     * references.  Note that the result is still a list of raw parsetrees ---
     * we cannot, in general, run parse analysis on one statement until we
     * have actually executed the prior ones.
     */
    parsetree_list = transformCreateSchemaStmt(stmt);

#ifdef PGXC
    /*
     * Add a RemoteQuery node for a query at top level on a remote Coordinator,
     * if not done already.
     */
    if (!sentToRemote)
        parsetree_list = AddRemoteQueryNode(parsetree_list, queryString,
                                            EXEC_ON_ALL_NODES);
#endif

    /*
     * Execute each command contained in the CREATE SCHEMA.  Since the grammar
     * allows only utility commands in CREATE SCHEMA, there is no need to pass
     * them through parse_analyze() or the rewriter; we can just hand them
     * straight to ProcessUtility.
     */
    foreach(parsetree_item, parsetree_list)
    {
        Node       *stmt = (Node *) lfirst(parsetree_item);
        PlannedStmt *wrapper;

        /* need to make a wrapper PlannedStmt */
        wrapper = makeNode(PlannedStmt);
        wrapper->commandType = CMD_UTILITY;
        wrapper->canSetTag = false;
        wrapper->utilityStmt = stmt;
        wrapper->stmt_location = stmt_location;
        wrapper->stmt_len = stmt_len;

        /* do this step */
        ProcessUtility(wrapper,
                       queryString,
                       PROCESS_UTILITY_SUBCOMMAND,
                       NULL,
                       NULL,
                       None_Receiver,
#ifdef PGXC
                       true,
#endif /* PGXC */
                       NULL);

        /* make sure later steps can see the object created here */
        CommandCounterIncrement();
    }

    /* Reset search path to normal state */
    PopOverrideSearchPath();

    /* Reset current user and security context */
    SetUserIdAndSecContext(saved_uid, save_sec_context);

    return namespaceId;
}

/*
 * Guts of schema deletion.
 */
void
RemoveSchemaById(Oid schemaOid)
{
    Relation    relation;
    HeapTuple    tup;

    relation = heap_open(NamespaceRelationId, RowExclusiveLock);

    tup = SearchSysCache1(NAMESPACEOID,
                          ObjectIdGetDatum(schemaOid));
    if (!HeapTupleIsValid(tup)) /* should not happen */
        elog(ERROR, "cache lookup failed for namespace %u", schemaOid);

    CatalogTupleDelete(relation, &tup->t_self);

    ReleaseSysCache(tup);

    heap_close(relation, RowExclusiveLock);
}


/*
 * Rename schema
 */
ObjectAddress
RenameSchema(const char *oldname, const char *newname)
{// #lizard forgives
    Oid            nspOid;
    HeapTuple    tup;
    Relation    rel;
    AclResult    aclresult;
    ObjectAddress address;

    rel = heap_open(NamespaceRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy1(NAMESPACENAME, CStringGetDatum(oldname));
    if (!HeapTupleIsValid(tup))
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_SCHEMA),
                 errmsg("schema \"%s\" does not exist", oldname)));

    nspOid = HeapTupleGetOid(tup);

    /* make sure the new name doesn't exist */
    if (OidIsValid(get_namespace_oid(newname, true)))
        ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_SCHEMA),
                 errmsg("schema \"%s\" already exists", newname)));

#ifdef _MLS_
    if (true == mls_check_schema_permission(nspOid))
    {
        elog(ERROR, "could not rename schema:%s, cause there are tables having mls poilcy bound in this schema", 
                    oldname);
    }
#endif

    /* must be owner */
    if (!pg_namespace_ownercheck(HeapTupleGetOid(tup), GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_NAMESPACE,
                       oldname);

    /* must have CREATE privilege on database */
    aclresult = pg_database_aclcheck(MyDatabaseId, GetUserId(), ACL_CREATE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, ACL_KIND_DATABASE,
                       get_database_name(MyDatabaseId));

    if (!allowSystemTableMods && IsReservedName(newname))
        ereport(ERROR,
                (errcode(ERRCODE_RESERVED_NAME),
                 errmsg("unacceptable schema name \"%s\"", newname),
                 errdetail("The prefix \"pg_\" is reserved for system schemas.")));

    /* rename */
    namestrcpy(&(((Form_pg_namespace) GETSTRUCT(tup))->nspname), newname);
    CatalogTupleUpdate(rel, &tup->t_self, tup);

    InvokeObjectPostAlterHook(NamespaceRelationId, HeapTupleGetOid(tup), 0);

    ObjectAddressSet(address, NamespaceRelationId, nspOid);

#ifdef PGXC
    if (IS_PGXC_COORDINATOR)
    {
        ObjectAddress        object;
        Oid                    namespaceId;

        /* Check object dependency and see if there is a sequence. If yes rename it */
        namespaceId = GetSysCacheOid(NAMESPACENAME,
                                     CStringGetDatum(oldname),
                                     0, 0, 0);
        /* Create the object that will be checked for the dependencies */
        object.classId = NamespaceRelationId;
        object.objectId = namespaceId;
        object.objectSubId = 0;

        /* Rename all the objects depending on this schema */
        performRename(&object, oldname, newname);
    }
#endif

    heap_close(rel, NoLock);
    heap_freetuple(tup);

    return address;
}

void
AlterSchemaOwner_oid(Oid oid, Oid newOwnerId)
{
    HeapTuple    tup;
    Relation    rel;

    rel = heap_open(NamespaceRelationId, RowExclusiveLock);

    tup = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(oid));
    if (!HeapTupleIsValid(tup))
        elog(ERROR, "cache lookup failed for schema %u", oid);

    AlterSchemaOwner_internal(tup, rel, newOwnerId);

    ReleaseSysCache(tup);

    heap_close(rel, RowExclusiveLock);
}


/*
 * Change schema owner
 */
ObjectAddress
AlterSchemaOwner(const char *name, Oid newOwnerId)
{
    Oid            nspOid;
    HeapTuple    tup;
    Relation    rel;
    ObjectAddress address;

    rel = heap_open(NamespaceRelationId, RowExclusiveLock);

    tup = SearchSysCache1(NAMESPACENAME, CStringGetDatum(name));
    if (!HeapTupleIsValid(tup))
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_SCHEMA),
                 errmsg("schema \"%s\" does not exist", name)));

    nspOid = HeapTupleGetOid(tup);

    AlterSchemaOwner_internal(tup, rel, newOwnerId);

    ObjectAddressSet(address, NamespaceRelationId, nspOid);

    ReleaseSysCache(tup);

    heap_close(rel, RowExclusiveLock);

    return address;
}

static void
AlterSchemaOwner_internal(HeapTuple tup, Relation rel, Oid newOwnerId)
{
    Form_pg_namespace nspForm;

    Assert(tup->t_tableOid == NamespaceRelationId);
    Assert(RelationGetRelid(rel) == NamespaceRelationId);

    nspForm = (Form_pg_namespace) GETSTRUCT(tup);

    /*
     * If the new owner is the same as the existing owner, consider the
     * command to have succeeded.  This is for dump restoration purposes.
     */
    if (nspForm->nspowner != newOwnerId)
    {
        Datum        repl_val[Natts_pg_namespace];
        bool        repl_null[Natts_pg_namespace];
        bool        repl_repl[Natts_pg_namespace];
        Acl           *newAcl;
        Datum        aclDatum;
        bool        isNull;
        HeapTuple    newtuple;
        AclResult    aclresult;

        /* Otherwise, must be owner of the existing object */
        if (!pg_namespace_ownercheck(HeapTupleGetOid(tup), GetUserId()))
            aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_NAMESPACE,
                           NameStr(nspForm->nspname));

        /* Must be able to become new owner */
        check_is_member_of_role(GetUserId(), newOwnerId);

        /*
         * must have create-schema rights
         *
         * NOTE: This is different from other alter-owner checks in that the
         * current user is checked for create privileges instead of the
         * destination owner.  This is consistent with the CREATE case for
         * schemas.  Because superusers will always have this right, we need
         * no special case for them.
         */
        aclresult = pg_database_aclcheck(MyDatabaseId, GetUserId(),
                                         ACL_CREATE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error(aclresult, ACL_KIND_DATABASE,
                           get_database_name(MyDatabaseId));

        memset(repl_null, false, sizeof(repl_null));
        memset(repl_repl, false, sizeof(repl_repl));

        repl_repl[Anum_pg_namespace_nspowner - 1] = true;
        repl_val[Anum_pg_namespace_nspowner - 1] = ObjectIdGetDatum(newOwnerId);

        /*
         * Determine the modified ACL for the new owner.  This is only
         * necessary when the ACL is non-null.
         */
        aclDatum = SysCacheGetAttr(NAMESPACENAME, tup,
                                   Anum_pg_namespace_nspacl,
                                   &isNull);
        if (!isNull)
        {
            newAcl = aclnewowner(DatumGetAclP(aclDatum),
                                 nspForm->nspowner, newOwnerId);
            repl_repl[Anum_pg_namespace_nspacl - 1] = true;
            repl_val[Anum_pg_namespace_nspacl - 1] = PointerGetDatum(newAcl);
        }

        newtuple = heap_modify_tuple(tup, RelationGetDescr(rel), repl_val, repl_null, repl_repl);

        CatalogTupleUpdate(rel, &newtuple->t_self, newtuple);

        heap_freetuple(newtuple);

        /* Update owner dependency reference */
        changeDependencyOnOwner(NamespaceRelationId, HeapTupleGetOid(tup),
                                newOwnerId);
    }

    InvokeObjectPostAlterHook(NamespaceRelationId,
                              HeapTupleGetOid(tup), 0);
}

#ifdef _MLS_
char * GetSchemaNameByOid(Oid schemaOid)
{   
    HeapTuple   tp;
    char       *result;
    
    tp = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(schemaOid));
    if (HeapTupleIsValid(tp))
    {
        Form_pg_namespace reltup = (Form_pg_namespace) GETSTRUCT(tp);
        
        result = pstrdup(NameStr(reltup->nspname));
        
        ReleaseSysCache(tp);
        
        return result;
    }

    return NULL;
}

#endif

