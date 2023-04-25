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
 * groupmgr.c
 *      Routines to support manipulation of the pgxc_group catalog
 *      This includes support for DDL on objects NODE GROUP
 *
 * Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/pg_type.h"
#include "catalog/pgxc_node.h"
#include "catalog/pgxc_group.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/array.h"
#include "pgxc/groupmgr.h"
#ifdef _MIGRATE_
#include "access/relscan.h"
#include "access/genam.h"
#include "pgxc/pgxcnode.h"
#include "utils/formatting.h"
#endif


static void PgxcGroupAlterToDefault(const char *group_name);


/*
 * PgxcGroupCreate
 *
 * Create a PGXC node group
 */
void
PgxcGroupCreate(CreateGroupStmt *stmt)
{// #lizard forgives
#ifdef _MIGRATE_
    bool        have_default = false;
    HeapScanDesc scan;    
    HeapTuple    tuple;
    Form_pgxc_group group = NULL;
    int j = 0;
#endif
    const char *group_name = stmt->group_name;
    List       *nodes = stmt->nodes;
    oidvector  *nodes_array;
    Oid           *inTypes;
    Relation    rel;
    HeapTuple    tup;
    bool        nulls[Natts_pgxc_group];
    Datum        values[Natts_pgxc_group];
    int            member_count = list_length(stmt->nodes);
    ListCell   *lc;
    int            i = 0;

    /* Only a DB administrator can add cluster node groups */
    if (!superuser())
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("must be superuser to create cluster node groups")));

    /* Check if given group already exists */
    if (OidIsValid(get_pgxc_groupoid(group_name)))
        ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_OBJECT),
                 errmsg("PGXC Group %s: group already defined",
                        group_name)));

    inTypes = (Oid *) palloc(member_count * sizeof(Oid));

    /* Build list of Oids for each node listed */
    foreach(lc, nodes)
    {
        char   *node_name = strVal(lfirst(lc));
        Oid    noid = get_pgxc_nodeoid(node_name);

        if (!OidIsValid(noid))
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                     errmsg("PGXC Node %s: object not defined",
                            node_name)));

        if (get_pgxc_nodetype(noid) != PGXC_NODE_DATANODE)
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("PGXC node %s: only Datanodes can be group members",
                            node_name)));

        /* OK to pick up Oid of this node */
        inTypes[i] = noid;
        i++;
    }

#ifdef _MIGRATE_
    /* cross check to ensure one node can only be in one node group */    
    have_default = false;
    rel = heap_open(PgxcGroupRelationId, AccessShareLock);
    scan = heap_beginscan_catalog(rel, 0, NULL);
    while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
    {
        group = (Form_pgxc_group)GETSTRUCT(tuple);
        for (i = 0; i < group->group_members.dim1; i++)
        {
            for (j = 0; j < member_count; j++)
            {
                if (group->group_members.values[i] == inTypes[j])
                {
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("PGXC node:%u already in group:%s",
                                    inTypes[j], NameStr(group->group_name))));
                }

                if (group->default_group)
                {
                    have_default = true;
                }
            }            
        }
    }
    heap_endscan(scan);
    heap_close(rel, AccessShareLock);

    /* only one default group can be defined in cluster */
    if (have_default && stmt->default_group)
    {
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("PGXC only one default group allowwed")));
    }
#endif

    /* Build array of Oids to be inserted */
    nodes_array = buildoidvector(inTypes, member_count);

    /* Iterate through all attributes initializing nulls and values */
    for (i = 0; i < Natts_pgxc_group; i++)
    {
        nulls[i]  = false;
        values[i] = (Datum) 0;
    }

    /* Insert Data correctly */
    values[Anum_pgxc_group_name - 1] =
        DirectFunctionCall1(namein, CStringGetDatum(group_name));
#ifdef _MIGRATE_
    values[Anum_pgxc_group_default - 1] = Int32GetDatum(stmt->default_group);
#endif
    values[Anum_pgxc_group_members - 1] = PointerGetDatum(nodes_array);

    /* Open the relation for insertion */
    rel = heap_open(PgxcGroupRelationId, RowExclusiveLock);
    tup = heap_form_tuple(rel->rd_att, values, nulls);

    CatalogTupleInsert(rel, tup);

    heap_close(rel, RowExclusiveLock);
}


/*
 * PgxcNodeGroupsRemove():
 *
 * Remove a PGXC node group
 */
void
PgxcGroupRemove(DropGroupStmt *stmt)
{
    Relation    relation;
    HeapTuple    tup;
    const char *group_name = stmt->group_name;
    Oid            group_oid = get_pgxc_groupoid(group_name);

    /* Only a DB administrator can remove cluster node groups */
    if (!superuser())
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("must be superuser to remove cluster node groups")));

    /* Check if group exists */
    if (!OidIsValid(group_oid))
        ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_OBJECT),
                 errmsg("PGXC Group %s: group not defined",
                        group_name)));

    /* Delete the pgxc_group tuple */
    relation = heap_open(PgxcGroupRelationId, RowExclusiveLock);
    tup = SearchSysCache(PGXCGROUPOID, ObjectIdGetDatum(group_oid), 0, 0, 0);

    if (!HeapTupleIsValid(tup)) /* should not happen */
        elog(ERROR, "PGXC Group %s: group not defined", group_name);

    simple_heap_delete(relation, &tup->t_self);

    ReleaseSysCache(tup);

    heap_close(relation, RowExclusiveLock);
}

void PgxcGroupAlter(AlterGroupStmt *stmt)
{
    const char *group_name = stmt->group_name;
    Oid            group_oid = get_pgxc_groupoid(group_name);
    ListCell   *lcmd;
    

    /* Only a DB administrator can remove cluster node groups */
    if (!superuser())
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("must be superuser to alter cluster node groups")));

    /* Check if group exists */
    if (!OidIsValid(group_oid))
        ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_OBJECT),
                 errmsg("PGXC Group %s: group not defined",
                        group_name)));

    foreach(lcmd, stmt->cmds)
    {
        AlterGroupCmd *cmd = (AlterGroupCmd *) lfirst(lcmd);
        switch (cmd->subtype)
        {
            case AG_SetDefault:
            {
                PgxcGroupAlterToDefault(group_name);
                break;
            }
            default:    
                elog(ERROR, "Unsupported subtype of Alter Node Group");
                break;
        }
    }

    
}


static void PgxcGroupAlterToDefault(const char *group_name)
{
    Oid default_groupoid = InvalidOid;
    Relation    relation;
    Datum        new_record[Natts_pgxc_group];
    bool        new_record_nulls[Natts_pgxc_group];
    bool        new_record_repl[Natts_pgxc_group];
    HeapTuple    oldtup, newtup;
    Form_pgxc_group groupForm;
    
    default_groupoid = GetDefaultGroup();
    if (OidIsValid(default_groupoid))
    {
        elog(ERROR, "default group already exists, groupoid:%d", default_groupoid);
    }
    
    relation = heap_open(PgxcGroupRelationId, RowExclusiveLock);

    MemSet(new_record, 0, sizeof(new_record));
    MemSet(new_record_nulls, false, sizeof(new_record_nulls));
    MemSet(new_record_repl, false, sizeof(new_record_repl));

    new_record_repl[Anum_pgxc_group_default - 1] = true;

    oldtup = SearchSysCacheCopy1(PGXCGROUPNAME, CStringGetDatum(group_name));
    if (!HeapTupleIsValid(oldtup))
    {
        elog(ERROR, "cache lookup failed for group %s", group_name);
    }
    
    groupForm = (Form_pgxc_group) GETSTRUCT(oldtup);
    if (groupForm->default_group == 1)
    {
        elog(ERROR, "group %s is already default group", group_name);
    }

    new_record[Anum_pgxc_group_default-1]    = 1;

    /* Update relation */
    newtup = heap_modify_tuple(oldtup, RelationGetDescr(relation),
                               new_record,
                               new_record_nulls, new_record_repl);
    CatalogTupleUpdate(relation, &oldtup->t_self, newtup);

    /* Release lock at Commit */
    heap_close(relation, RowExclusiveLock);
    heap_freetuple(oldtup);
    heap_freetuple(newtup);
}

#ifdef _MIGRATE_
Oid GetDefaultGroup(void)
{
    Oid          group_oid  = InvalidOid;
    Relation     rel;
    HeapScanDesc scan;    
    HeapTuple     tuple; 
    Form_pgxc_group group   = NULL;
    
    /* cross check to ensure one node can only be in one node group */  
    rel = heap_open(PgxcGroupRelationId, AccessShareLock);
    scan = heap_beginscan_catalog(rel, 0, NULL);
    while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
    {
        group = (Form_pgxc_group)GETSTRUCT(tuple);
        if (group->default_group)
        {
            group_oid = HeapTupleGetOid(tuple);
            break;
        }
    }
    heap_endscan(scan);
    heap_close(rel, AccessShareLock);
    return group_oid;
}



void AddNodeToGroup(Oid nodeoid, Oid groupoid)
{
    Relation    relation;
    HeapTuple    tup;
    HeapTuple    newtup;
    Form_pgxc_group oldgroup;
    Datum        *replvalues;
    bool         *replisnull;
    bool        *doreplace;
    oidvector    *oldnodes;
    oidvector    *newnodes;
    
    
    if(!OidIsValid(nodeoid))
    {
        elog(ERROR, "node oid [%d] is invalid", nodeoid);
    }

    if(!OidIsValid(groupoid))
    {
        groupoid = GetDefaultGroup();

        if(!OidIsValid(groupoid))
        {
            elog(ERROR, "group oid [%d] is not valid", groupoid);
        }
    }

    /* Delete the pgxc_group tuple */
    relation = heap_open(PgxcGroupRelationId, RowExclusiveLock);
    tup = SearchSysCache(PGXCGROUPOID, ObjectIdGetDatum(groupoid), 0, 0, 0);

    if (!HeapTupleIsValid(tup)) /* should not happen */
        elog(ERROR, "PGXC Group %d: group not defined", groupoid);

    replvalues = (Datum*)palloc0(Natts_pgxc_group * sizeof(Datum));
    replisnull = (bool *)palloc0(Natts_pgxc_group * sizeof(bool));
    doreplace = (bool *)palloc0(Natts_pgxc_group * sizeof(bool));

    doreplace[Anum_pgxc_group_members-1] = true;
    
    oldgroup = (Form_pgxc_group)GETSTRUCT(tup);
    oldnodes = &oldgroup->group_members;

    newnodes = oidvector_append(oldnodes, nodeoid);

    replvalues[Anum_pgxc_group_members-1] = PointerGetDatum(newnodes);

    newtup = heap_modify_tuple(tup, RelationGetDescr(relation), replvalues, replisnull, doreplace);

    
    CatalogTupleUpdate(relation, &newtup->t_self, newtup);


    ReleaseSysCache(tup);
    //heap_freetuple(newtup);
    
    heap_close(relation, RowExclusiveLock);

    pfree(replvalues);
    pfree(replisnull);
    pfree(doreplace);
    pfree(newnodes);
}

Oid RemoveNodeFromGroup(Oid nodeoid)
{
    Relation    relation;
    HeapTuple    tup;
    HeapTuple    newtup;
    Form_pgxc_group oldgroup;
    Datum        *replvalues;
    bool         *replisnull;
    bool        *doreplace;
    oidvector    *newnodes;
    Oid         group = InvalidOid;

    SysScanDesc scan;
    int i;
    bool found = false;
    
    
    if(!OidIsValid(nodeoid))
    {
        elog(ERROR, "node oid [%d] is invalid", nodeoid);
    }

    /* Delete the pgxc_group tuple */
    
    relation = heap_open(PgxcGroupRelationId, RowExclusiveLock);
    
    scan = systable_beginscan(relation, InvalidOid, false, NULL, 0, NULL);

    tup = systable_getnext(scan);

    while(HeapTupleIsValid(tup))
    {
        oldgroup = (Form_pgxc_group)GETSTRUCT(tup);

        for (i = 0; i < oldgroup->group_members.dim1; i++)
        {
            if (oldgroup->group_members.values[i] == nodeoid)
            {
                found = true;
                break;
            }
        }

        if(found)
        {
            newnodes = oidvector_remove(&oldgroup->group_members, nodeoid);

            replvalues = (Datum*)palloc0(Natts_pgxc_group * sizeof(Datum));
            replisnull = (bool *)palloc0(Natts_pgxc_group * sizeof(bool));
            doreplace = (bool *)palloc0(Natts_pgxc_group * sizeof(bool));

            doreplace[Anum_pgxc_group_members-1] = true;
            replvalues[Anum_pgxc_group_members-1] = PointerGetDatum(newnodes);

            newtup = heap_modify_tuple(tup, RelationGetDescr(relation), replvalues, replisnull, doreplace);        
            CatalogTupleUpdate(relation, &newtup->t_self, newtup);

            pfree(replvalues);
            pfree(replisnull);
            pfree(doreplace);
            pfree(newnodes);

            group = HeapTupleGetOid(tup);
            break;
        }
        tup = systable_getnext(scan);
    }

    systable_endscan(scan);
    heap_close(relation, RowExclusiveLock);


#ifndef _PG_REGRESS_
    if(!found)
    {
        elog(WARNING, "this node[%d] is not exist in any group.", nodeoid);
    }
#endif
    return group;
}


Oid GetGroupOidByNode(Oid nodeoid)
{
	Relation	relation;
	SysScanDesc scan;
	HeapTuple	tup;
	Form_pgxc_group group;
	int i;
	Oid         groupoid   = InvalidOid;

    nodeoid = PGXCGetMainNodeOid(nodeoid);

	relation = heap_open(PgxcGroupRelationId, AccessShareLock);
		
	scan = systable_beginscan(relation, InvalidOid, false, NULL, 0, NULL);

	tup = systable_getnext(scan);

	while(HeapTupleIsValid(tup))
	{
		group = (Form_pgxc_group)GETSTRUCT(tup);

		for (i = 0; i < group->group_members.dim1; i++)
		{
			if (group->group_members.values[i] == nodeoid)
			{
				groupoid = HeapTupleGetOid(tup);
				break;
			}
		}

		if (OidIsValid(groupoid))
		{
			break;
		}
		
		tup = systable_getnext(scan);
	}

	systable_endscan(scan);
	heap_close(relation, AccessShareLock);
	
	return groupoid;
}

List *
GetGroupNodeList(Oid group)
{
    int i = 0;
    List *nodelist = NULL;
    Relation    relation;
    HeapTuple    tup;
    Form_pgxc_group oldgroup;

    if (!OidIsValid(group))
        return NULL;

    relation = heap_open(PgxcGroupRelationId, RowExclusiveLock);
    tup = SearchSysCache(PGXCGROUPOID, ObjectIdGetDatum(group), 0, 0, 0);

    if (!HeapTupleIsValid(tup)) /* should not happen */
    {
        heap_close(relation, RowExclusiveLock);
        elog(ERROR, "PGXC Group %d: group not defined", group);
    }

    oldgroup = (Form_pgxc_group)GETSTRUCT(tup);

    for (i = 0; i < oldgroup->group_members.dim1; i++)
    {
        Oid nodeoid = oldgroup->group_members.values[i];

        char node_type = PGXC_NODE_DATANODE;
        
        int nodeid  = PGXCNodeGetNodeId(nodeoid, &node_type);

        if (nodeid == -1)
        {
            ReleaseSysCache(tup);
            heap_close(relation, RowExclusiveLock);
            elog(ERROR, "node %d found in group %d, but could not get nodeid.", nodeoid, group);
        }
        else
        {
            nodelist = lappend_int(nodelist, nodeid);
        }
    }
    
    ReleaseSysCache(tup);
    
    heap_close(relation, RowExclusiveLock);

    return nodelist;
}

/* return group name of current node, null if not found */
char *
GetMyGroupName(void)
{
    char *node_name = asc_tolower(PGXCNodeName, strlen(PGXCNodeName));

    Oid node_oid = get_pgxc_nodeoid(node_name);

    if (OidIsValid(node_oid))
    {
        Relation    relation;
         SysScanDesc scan;
         HeapTuple    tup;
         Form_pgxc_group group;
         char        *groupname = NULL;
         int i;
         
         relation = heap_open(PgxcGroupRelationId, AccessShareLock);
             
         scan = systable_beginscan(relation, InvalidOid, false, NULL, 0, NULL);
 
         tup = systable_getnext(scan);
 
         while(HeapTupleIsValid(tup))
         {
             group = (Form_pgxc_group)GETSTRUCT(tup);
 
             for (i = 0; i < group->group_members.dim1; i++)
             {
                 if (group->group_members.values[i] == node_oid)
                 {
                     groupname = NameStr(group->group_name);
                     break;
                 }
             }
             
             tup = systable_getnext(scan);
         }
 
         systable_endscan(scan);
         heap_close(relation, AccessShareLock);

        return groupname;
    }

    return NULL;
}

char *
GetGroupNameByNode(Oid nodeoid)
{
    Relation    relation;
    SysScanDesc scan;
    HeapTuple    tup;
    Form_pgxc_group group;
    char        *groupname = NULL;
    int i;
    
    relation = heap_open(PgxcGroupRelationId, AccessShareLock);
        
    scan = systable_beginscan(relation, InvalidOid, false, NULL, 0, NULL);

    tup = systable_getnext(scan);

    while(HeapTupleIsValid(tup))
    {
        group = (Form_pgxc_group)GETSTRUCT(tup);

        for (i = 0; i < group->group_members.dim1; i++)
        {
            if (group->group_members.values[i] == nodeoid)
            {
                groupname = NameStr(group->group_name);
                break;
            }
        }
        
        tup = systable_getnext(scan);
    }

    systable_endscan(scan);
    heap_close(relation, AccessShareLock);

    if (groupname)
    {
        return groupname;
    }
    
    return NULL;
}


#endif

