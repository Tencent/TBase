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
#include "postgres.h"
#include "postgres_ext.h"
#include "access/genam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/heapam.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "catalog/pg_mls.h"
#include "catalog/indexing.h"
#include "executor/tuptable.h"
#include "parser/parsetree.h"
#include "parser/parse_relation.h"
#include "parser/parse_clause.h"
#include "parser/parse_collate.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "storage/lockdefs.h"
#include "tcop/tcopprot.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/palloc.h"
#include "utils/fmgroids.h"
#include "utils/relcache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/mls.h"
#include "utils/cls.h"

/*
 * cls column value in every row
 */
typedef struct ClsItem
{
    int16 polid;
    int16 labelid;
}ClsItem;

/*
 * stands for one label
 */
typedef struct tagClsLabel
{
    int16  levelid;
    List * compartmentlist;
    List * grouptree;
    List * unionset;
}ClsLabel;

/*
 * every user has one of this global variable 
 */
typedef struct tagClsUserAuthority
{
    MemoryContext mctx;
    int16 polid;
    int16 privilege;
    int16 max_read_label;
    int16 max_write_label;
    int16 min_write_label;
    int16 default_read_label;
    int16 default_write_label;
    int16 default_row_label;
    ClsLabel * def_read_label_stru;
    ClsLabel * def_write_label_stru;
    ClsLabel * def_row_label_stru;
}ClsUserAuthority;

typedef struct tagClsGroupInfo
{
    int     childid;
    int     parentid;
    bool    valid;
}ClsGroupInfo;

/* every user has one of this global variable */
ClsUserAuthority g_user_cls_priv;

int g_command_tag_enum = CLS_CMD_UNKNOWN;

/* only the parent of root node could be invalid node, other has only one parent */
#define CLS_GROUP_INVALID_NODE              -1
/* zero mean invalid */
#define CLS_INVALID_POLICY_ID               0

#define CLS_ARRAY_NUM(CLS_ARRAY)    (ARR_DIMS(CLS_ARRAY)[0])
#define CLS_ARRAY_DAT(CLS_ARRAY)    ((ClsItem *) ARR_DATA_PTR(CLS_ARRAY))
#define CLS_ARRAY_N_SIZE(N)         (ARR_OVERHEAD_NONULLS(1) + ((N) * sizeof(ClsItem)))
#define CLS_ARRAY_SIZE(CLS_ARRAY)   ARR_SIZE(CLS_ARRAY)

#define DatumGetClsItemP(X)        ((ClsItem *) DatumGetPointer(X))
#define PG_GETARG_CLSITEM_P(n)     DatumGetClsItemP(PG_GETARG_DATUM(n))
#define PG_RETURN_CLSITEM_P(x)     PG_RETURN_POINTER(x)

#define CLS_MAX_POLICY_ID_STRING_LEN    5
#define CLS_MAX_LABEL_ID_STRING_LEN     5
#define CLS_DELIMETER_STRING_LEN        1
#define CLS_MAX_CLS_ITEM_STRING_LEN     (CLS_MAX_POLICY_ID_STRING_LEN+CLS_DELIMETER_STRING_LEN+CLS_MAX_LABEL_ID_STRING_LEN)

#define CLS_TDCLSCOL_CHANGE_TO_ATTNUM(_tdclscol) ((_tdclscol)-1)

/* check current user was in the same policy with '_polid' */
#define CLS_AUTH_CHECK_IN_POLICY(_polid) ((g_user_cls_priv.polid == (_polid)) && (CLS_INVALID_POLICY_ID != (_polid)))

static void cls_parse_clsitem_and_assign(const char * str, ClsItem * clstiem);
static int16 cls_check_table_has_cls_policy(Oid relid);
static ClsLabel * cls_get_label(int16 polid, int16 labelid);
static ClsExprStruct * cls_create_func_expr(Relation rel);
static bool cls_check_write(ClsItem *arg);
static bool cls_check_read(ClsItem *arg);
static List * cls_parse_compartment(Datum compartment_datum);
static List * cls_parse_group(Datum group_datum);
static int list_compare_int(List * list1, List * list2);
static bool cls_group_node_match_child_and_parent(int polid, int childid, int parentid);
static int cls_get_clscol_from_pg_attribute(Form_pg_attribute *attrs, int natts);
static List * array_datum_convert_to_int2_list(Datum datum);
static bool cls_group_compare(int polid, List * rowgrouplist, List * usergrouplist);
static bool cls_compartment_compare(List * rowcompartmentlist, List * usercompartmentlist);
Datum clsitemin(PG_FUNCTION_ARGS);
Datum clsitemout(PG_FUNCTION_ARGS);

#if MARK("utility")
/*
 * pre-condition : list1 and list2 are unique lists, and elements are sorted.
 * return 1, expect list2 has a bigger set than list1, 
 * return 0, list1 and list2 are same,
 * else -1.
 */
static int list_compare_int(List * list1, List * list2)
{// #lizard forgives
    ListCell   *cell1;
    ListCell   *cell2;
    
    /* no care list2 is null or not */
    if (NULL == list1)
    {
        return 0;
    }

    /* here, list1 is not null */
    if (NULL == list2)
    {
        return -1;
    }

    /* now list1 and list2 are not null */

    /* if list1 has more elements, there is no suspense, list2 does not have a bigger set */
    if (list_length(list1) > list_length(list2))
    {
        return -1;
    }

    /* compare elements one by one */
    for ((cell1) = list_head(list1), (cell2) = list_head(list2);        
         (cell1) != NULL && (cell2) != NULL;                
         )
    {
        /* the front ones are the same, both step to the next */
        if (lfirst_int(cell1) == lfirst_int(cell2))
        {
            (cell1) = lnext(cell1);
            (cell2) = lnext(cell2);
            
            continue;
        }
        
        /* else step list2 to next element until list2 end */
        (cell2) = lnext(cell2);
    }

    /* if list1 is to the end, means list2 is not smaller than list2 */
    if (NULL == cell1)
    {
        if (NULL == cell2)
        {
            /* list2 is also to the end, they are just same */
            return 0;
        }
        else
        {
            /* while, list2 has more elements */
            return 1;
        }
    }
         
    /* else list2 dose not have all elements in list1 */         
    return -1;
}
static List * array_datum_convert_to_int2_list(Datum datum)
{
    ArrayType * array;
    int         i;
    int         dims;
    int16       element;
    int16     * elements;
    List      * list = NIL;
    
    array = (ArrayType *)PG_DETOAST_DATUM(datum);

    /* mark this part is empty, return NULL */
    if (1 != ARR_NDIM(array))
    {
        return NULL;
    }

    dims     =  ARR_DIMS(array)[0];
    elements =  (int16*)ARR_DATA_PTR(array);

    for (i = 0; i < dims; i++)
    {
        element = elements[i];
        list = list_append_unique_int(list, element);    
    }
    
    return list;
}
/*
 * check attr if cls column exists
 */
static int cls_get_clscol_from_pg_attribute(Form_pg_attribute *attrs, int natts)
{
    int i;
    int clscol = InvalidAttrNumber;

    for (i = 0; i < natts; i++)
    {
        if (CLSITEM_OID == attrs[i]->atttypid)
        {
            clscol = i + 1;
        }
    }
    
    return clscol;
}

static void cls_parse_clsitem_and_assign(const char * str, ClsItem * clstiem)
{
    //elog(LOG, "[neoqguo]cls_parse_clsitem_and_assign get str arg:%s", str);
    
    sscanf(str, "%hd:%hd", &clstiem->polid, &clstiem->labelid);

    return;
}



static bool cls_get_group_info(int polid, int groupid, ClsGroupInfo * group_info)
{
    bool        ret;
    Relation    rel;
    SysScanDesc scan;
    HeapTuple    tup;
    ScanKeyData key[2];
    Form_pg_cls_group group_form;

    ScanKeyInit(&key[0],
                Anum_pg_cls_group_polid,
                BTEqualStrategyNumber, 
                F_OIDEQ,
                ObjectIdGetDatum(polid));
    ScanKeyInit(&key[1],
                Anum_pg_cls_group_groupid,
                BTEqualStrategyNumber, 
                F_OIDEQ,
                ObjectIdGetDatum(groupid));

    ret = false;
    rel = heap_open(ClsGroupRelationId, AccessShareLock);

    scan = systable_beginscan(rel, PgClsGroupPolidGroupidIndexId, true, NULL, 2, key);

    if (HeapTupleIsValid(tup = systable_getnext(scan)))
    {
        group_form = (Form_pg_cls_group) GETSTRUCT(tup);
        
        if (group_info)
        {
            group_info->childid  = group_form->groupid;
            group_info->parentid = group_form->parentid;

            if (HeapTupleHasNulls(tup) 
                && att_isnull(Anum_pg_cls_group_longname, tup->t_data->t_bits))
            {
                group_info->valid = false;
            }
            else
            {
                group_info->valid = true;
            }
        }

        ret = true;
    }

    systable_endscan(scan);

    heap_close(rel, AccessShareLock);
    
    return ret;
}


#endif
#if MARK("algorithm")
/*
 * check relationship between child and parent, means parent is the upper node of child.
 */
static bool cls_group_node_match_child_and_parent(int polid, int childid, int parentid)
{
    bool    ret;
    int     current_nodeid;
    ClsGroupInfo nodeinfo;
    
    current_nodeid = childid;
    ret            = false;

    for(;;)
    {
        if (true == cls_get_group_info(polid, current_nodeid, &nodeinfo))
        {   
            if (nodeinfo.parentid == parentid)
            {
                if (nodeinfo.valid)
                {
                    ret = true;
                }
                else
                {
                    /* get the parentid, but it was invalid, so returns fail*/
                    ret = false;
                }
                break;
            }
            else if (CLS_GROUP_INVALID_NODE == nodeinfo.parentid)
            {
                /* already up to the root, fails */
                ret = false;
                break;
            }

            /* step to upper node */
            current_nodeid = nodeinfo.parentid;
            continue;
        }

        /* groupid not found, fails */
        ret = false;
        break;
    }

    return ret;
}

/*
 * expect user comparelist covering all elements in row comparelist.
 */
static bool cls_compartment_compare(List * rowcompartmentlist, List * usercompartmentlist)
{
    if (list_compare_int(rowcompartmentlist, usercompartmentlist) < 0)
    {
        return false;
    }
    
    return true;
}

/*
 * expect user grouplist has at least one element is 'NOT UNDER' the element in rowgrouplist.
 * NodeA is NOT UNDER NodeB means NodeA equal to NodeB OR NodeA is parent node of NodeB. 
 */
static bool cls_group_compare(int polid, List * rowgrouplist, List * usergrouplist)
{
    ListCell *  rowcell;
    ListCell *  usercell;
    int         rowgroupid;
    int         usergroupid;

    /* there is no group in row label, just skip */
    if (NULL == rowgrouplist)
    {
        return true;
    }

    /* here, row is not null, if user was null, fails */
    if (NULL == usergrouplist)
    {
        return false;
    }

    /* here, both row and user are not null, go to pk */
    foreach(rowcell, rowgrouplist)
    {
        rowgroupid = lfirst_int(rowcell);
        foreach(usercell, usergrouplist)
        {
            usergroupid = lfirst_int(usercell);
            if(rowgroupid == usergroupid)
            {
                return true;
            }
            
            if (cls_group_node_match_child_and_parent(polid, rowgroupid, usergroupid))
            {
                return true;
            }
        }
    }

    return false;
}
#endif
#if MARK("cls action check")
/*
 * check if table was binding cls policy, return _cls attnum if exists, else InvalidAttrNumber.
 */
static int16 cls_check_table_has_cls_policy(Oid relid)
{
    SysScanDesc scan;
    ScanKeyData skey[1];
    HeapTuple   htup;
    Relation    rel;
    int16       attnum = InvalidAttrNumber;

    /* skip system tables */
    if (IS_SYSTEM_REL(relid))
    {
        return InvalidAttrNumber;
    }
    
    ScanKeyInit(&skey[0],
                    Anum_pg_cls_table_relid,
                    BTEqualStrategyNumber, 
                    F_OIDEQ,
                    ObjectIdGetDatum(relid));

    rel = heap_open(ClsTableRelationId, AccessShareLock);
    scan = systable_beginscan(rel, 
                              PgClsTablePolidRelidIndexId, 
                              true,
                              NULL, 
                              1, 
                              skey);

    
    while (HeapTupleIsValid(htup = systable_getnext(scan)))
    {
        Form_pg_cls_table form_cls_table = (Form_pg_cls_table) GETSTRUCT(htup);
        if (true == form_cls_table->enable)
        {
            attnum = form_cls_table->attnum;
            break;
        }
    }

    systable_endscan(scan);
    heap_close(rel, AccessShareLock);

    return attnum;
}
/*
 * go through the parse routine and get the clause node tree.
 */
static ClsExprStruct * cls_create_func_expr(Relation rel)
{
#define PARSE_SQL_LEN 256    
    ParseState *parsestate;
    char        parse_sql[PARSE_SQL_LEN] = {0};
    Node       *qual                     = NULL;
    List       *parsetree_list;
    ListCell   *parsetree_item;
    RangeTblEntry * rte;

    ClsExprStruct * cls_struct;
    MemoryContext   tempctx;
    MemoryContext   oldcxt;
    
    tempctx = AllocSetContextCreate(CacheMemoryContext,
                                       RelationGetRelationName(rel),
                                       ALLOCSET_SMALL_SIZES);

    oldcxt = MemoryContextSwitchTo(tempctx);
    
    cls_struct = palloc(sizeof(ClsExprStruct));

    cls_struct->mctx = tempctx;
    
    parsestate = make_parsestate(NULL);
    rte        = addRangeTableEntryForRelation(parsestate, rel, NULL, true, true);
    addRTEtoQuery(parsestate, rte, true, true, true);
    
    snprintf(parse_sql, PARSE_SQL_LEN, "SELECT * FROM %s WHERE pg_cls_check(_cls)",
                                        NameStr(rel->rd_rel->relname));
    parsetree_list = pg_parse_query(parse_sql);
    
    foreach(parsetree_item, parsetree_list)
    {
        RawStmt    *parsetree;
        SelectStmt *n;

        parsetree = lfirst_node(RawStmt, parsetree_item);

        n    = (SelectStmt *) parsetree->stmt;
        qual = transformWhereClause(parsestate, n->whereClause, EXPR_KIND_WHERE, "WHERE");
        
        assign_expr_collations(parsestate, qual);
    }

    cls_struct->rd_cls_expr = qual;

    MemoryContextSwitchTo(oldcxt);

    return cls_struct;
}

/* 
 * parse
 */
static List * cls_parse_compartment(Datum compartment_datum)
{
    return array_datum_convert_to_int2_list(compartment_datum);
}

static List * cls_parse_group(Datum group_datum)
{
    return array_datum_convert_to_int2_list(group_datum);
}

/*
 * get clslabel according to policy id and label id.
 */
static ClsLabel * cls_get_label(int16 polid, int16 labelid)
{
    HeapTuple   tp;
    bool        is_null;
    Datum       compartment_datum;
    Datum       group_datum;
    ClsLabel  * clslabel;

    clslabel = palloc(sizeof(ClsLabel));
    
    tp = SearchSysCache2(CLSLABELOID, ObjectIdGetDatum(polid), ObjectIdGetDatum(labelid));
    
    if (HeapTupleIsValid(tp))
    {
        Form_pg_cls_label label_tup = (Form_pg_cls_label) GETSTRUCT(tp);

        /* get level */
        clslabel->levelid = label_tup->levelid;

        /* get compartment if exists */
        compartment_datum = SysCacheGetAttr(CLSLABELOID, tp, Anum_pg_cls_label_compartmentid, &is_null);
        if (false == is_null)
        {
            clslabel->compartmentlist = cls_parse_compartment(compartment_datum);
        }

        /* get group if exists */
        group_datum = SysCacheGetAttr(CLSLABELOID, tp, Anum_pg_cls_label_groupid, &is_null);
        if (false == is_null)
        {
            clslabel->grouptree = cls_parse_group(group_datum);
        }
    }
    
    if (NULL != tp)
    {
        ReleaseSysCache(tp);
        return clslabel;
    }
    
    pfree(clslabel);
    
    return NULL;
}

/*
 * cls checking in WRITE action procedure.
 */
static bool  cls_check_write(ClsItem *arg)
{// #lizard forgives
    ClsLabel * rowclslabel;
    ClsLabel * userclslabel;
    int16      polid;
    int16      labelid;

    polid   = arg->polid;
    labelid = arg->labelid;    
    
    /* diffrent policy, return false directly */
    if (false == CLS_AUTH_CHECK_IN_POLICY(polid))
    {
        return false;
    }
    
    /* the same label means the same authority */
    if (g_user_cls_priv.default_write_label == labelid)
    {
        return true;
    }
    
    /* get cls label of datarow */
    rowclslabel = cls_get_label(polid, labelid);
    if (NULL == rowclslabel)
    {
        return false;
    }
    /* get cls label of user */
    userclslabel = g_user_cls_priv.def_write_label_stru;

    /* STEP1. compare level */
    if (userclslabel->levelid < rowclslabel->levelid)
    {
        return false;
    }

    /* STEP2.compare group and compartment if exists */
    if (NULL != rowclslabel->grouptree)
    {
        /*STEP 2.1.1 */
        if (false == cls_group_compare(polid, rowclslabel->grouptree, userclslabel->grouptree))
        {
            return false;
        }

        /* STEP2.1.2 compare compartment with READ authority if exists */
        if (false == cls_compartment_compare(rowclslabel->compartmentlist, g_user_cls_priv.def_read_label_stru->compartmentlist))
        {
            return false;
        }
    }
    else
    {
        /* STEP2.2.1 compare compartment with WRITE authority if exists */
        if (false == cls_compartment_compare(rowclslabel->compartmentlist, userclslabel->compartmentlist))
        {
            return false;
        }
    }
    
    return true;
}

/*
 * cls checking in READ action procedure.
 */
static bool  cls_check_read(ClsItem *arg)
{
    ClsLabel * rowclslabel;
    ClsLabel * userclslabel;
    int16      polid;
    int16      labelid;

    polid   = arg->polid;
    labelid = arg->labelid;    

    /* diffrent policy, return false directly */
    if (false == CLS_AUTH_CHECK_IN_POLICY(polid))
    {
        return false;
    }

    /* the same label means the same authority */
    if (g_user_cls_priv.default_read_label == labelid)
    {
        return true;
    }

    /* get cls label of datarow */
    rowclslabel = cls_get_label(polid, labelid);
    if (NULL == rowclslabel)
    {
        return false;
    }
    /* get cls label of user */
    userclslabel = g_user_cls_priv.def_read_label_stru;

    /* STEP1.compare level */
    if (userclslabel->levelid < rowclslabel->levelid)
    {
        return false;
    }

    /* STEP2.compare group if exists */
    if (false == cls_group_compare(polid, rowclslabel->grouptree, userclslabel->grouptree))
    {
        return false;
    }

    /* STEP3.compare compartment if exists */
    if (false == cls_compartment_compare(rowclslabel->compartmentlist, userclslabel->compartmentlist))
    {
        return false;
    }

    /* pass all */ 
    return true;
}
#endif

#if MARK("external api")
/*
 * this is the enterance of cls check process, all cls work strategies work and judge here, 
 * the result would make a direct effect on the visiblity of current tuple
 */
Datum pg_cls_check(PG_FUNCTION_ARGS)
{
    ClsItem *arg;
    bool     ret;
    
    arg = (ClsItem*)PG_GETARG_DATUM(0);

    if (CLS_AUTH_CHECK_IN_POLICY(arg->polid))
    {
        ret = false;
        
        if (CLS_CMD_READ == g_command_tag_enum)
        {
            ret = cls_check_read(arg);
        }
        else if (CLS_CMD_WRITE == g_command_tag_enum)
        {
            ret = cls_check_write(arg);
        }
/*        
        else if (CLS_CMD_ROW == g_command_tag_enum)
        {
            
        }
*/      
        /* TODO: we just output the datum value, later there would be policies algorithm */
        elog(LOG, "[neoqguo]pg_cls_check is ready, get value:%d:%d", arg->polid, arg->labelid);
        
        PG_RETURN_BOOL(ret);
    }

    PG_RETURN_BOOL(false);
}

Datum clsitemin(PG_FUNCTION_ARGS)
{
    const char *arg1 = PG_GETARG_CSTRING(0);
    ClsItem    *clsitem;

    clsitem = (ClsItem *) palloc(sizeof(ClsItem));
    cls_parse_clsitem_and_assign(arg1, clsitem);
    
    PG_RETURN_CLSITEM_P(clsitem);
}

Datum clsitemout(PG_FUNCTION_ARGS)
{
    ClsItem    *clsitem = PG_GETARG_CLSITEM_P(0);
    char       *out;
    
    out = palloc0(CLS_MAX_CLS_ITEM_STRING_LEN + 1);

    snprintf(out, CLS_MAX_CLS_ITEM_STRING_LEN + 1, "%d:%d", clsitem->polid, clsitem->labelid);

    PG_RETURN_CSTRING(out);
}

/*
 * rows inserted with row label of current user
 */
void mls_update_cls_with_current_user(TupleTableSlot *slot)
{
    bool     isnull;
    int      cls_col_id;
    ClsItem* clsitem;
    
    cls_col_id = cls_get_clscol_from_pg_attribute(slot->tts_tupleDescriptor->attrs, slot->tts_tupleDescriptor->natts);
    if (InvalidAttrNumber == cls_col_id)
    {
        return ;
    }

    /* user dose not have cls policy, use the default value. */
    if (NULL == g_user_cls_priv.def_row_label_stru)
    {
        return;
    }

    /* maybe copy data from another relation or inner results, do not change the original _cls value */
    if (CLS_CMD_ROW != g_command_tag_enum)
    {
        return;
    }
    
    /* if tts_values is not ready, to deform it */
    if (cls_col_id > slot->tts_nvalid)
    {
        slot_getattr(slot, cls_col_id, &isnull);
    }

    clsitem = (ClsItem*)(slot->tts_values[cls_col_id - 1]);
    
    /* user's policy:label is the same as default value, just return. */
    if ((g_user_cls_priv.polid == clsitem->polid) && (g_user_cls_priv.default_row_label == clsitem->labelid))
    {
        return;
    }

    /* update tts_values with user */
    clsitem->polid   = g_user_cls_priv.polid;
    clsitem->labelid = g_user_cls_priv.default_row_label;

    return;
}


/*
 * just like switch memory context, to mark current action type, type see ClsCmdType.
 */
int mls_command_tag_switch_to(int tag)
{
    int oldtag;

    oldtag = g_command_tag_enum;
    
    g_command_tag_enum = tag;
    
    return oldtag;
}
/*
 * a new round for query, set to init type.
 */ 
void mls_reset_command_tag(void)
{
    g_command_tag_enum = CLS_CMD_READ;
    
    return;
}

/*
 * assign user clsitem infos, hold all memory allocated in "user clsitem info" memory context.
 * skip if system users.
 */
void mls_assign_user_clsitem(void)
{   
    int16         polid;
    MemoryContext oldcontext;
    SysScanDesc   scan;
    ScanKeyData   skey[1];
    HeapTuple     htup;
    Relation      rel;       

    /* skip system users */
    if (IS_SYSTEM_REL(GetAuthenticatedUserId()))
    {
        return;
    }

    if (NULL == g_user_cls_priv.mctx)
    {
        g_user_cls_priv.mctx = AllocSetContextCreate(TopMemoryContext,
                              "user clsitem info",
                              ALLOCSET_DEFAULT_SIZES);
    }
    else
    {
        /* delete memory context to free all the labels */
        MemoryContextResetAndDeleteChildren(g_user_cls_priv.mctx);
    }

    /* STEP 1. get the label values of current user */
    ScanKeyInit(&skey[0],
                    Anum_pg_cls_user_userid,
                    BTEqualStrategyNumber, 
                    F_OIDEQ,
                    ObjectIdGetDatum(GetAuthenticatedUserId()));

    rel = heap_open(ClsUserRelationId, AccessShareLock);
    scan = systable_beginscan(rel, 
                              PgClsUserPolidUseridIndexId, 
                              true,
                              NULL, 
                              1, 
                              skey);

    if (HeapTupleIsValid(htup = systable_getnext(scan)))
    {
        Form_pg_cls_user form_cls_user = (Form_pg_cls_user) GETSTRUCT(htup);

        g_user_cls_priv.polid               = form_cls_user->polid;
        g_user_cls_priv.privilege           = form_cls_user->privilege;
        g_user_cls_priv.max_read_label      = form_cls_user->max_read_label;
        g_user_cls_priv.max_write_label     = form_cls_user->max_write_label;
        g_user_cls_priv.min_write_label     = form_cls_user->min_write_label;
        g_user_cls_priv.default_read_label  = form_cls_user->default_read_label;
        g_user_cls_priv.default_write_label = form_cls_user->default_write_label;
        g_user_cls_priv.default_row_label   = form_cls_user->default_row_label;
    }

    systable_endscan(scan);
    heap_close(rel, AccessShareLock);

    /* STEP 2. get the label structs */
    
    oldcontext = MemoryContextSwitchTo(g_user_cls_priv.mctx);

    polid = g_user_cls_priv.polid;
    /* 
     * get and to cache each label, if the label is same as before, just use the it 
     */

    /* 2.1 for read label */
    g_user_cls_priv.def_read_label_stru = cls_get_label(polid, g_user_cls_priv.default_read_label);

    /* 2.2 for write label */
    if (g_user_cls_priv.default_write_label == g_user_cls_priv.default_read_label)
    {
        g_user_cls_priv.def_write_label_stru = g_user_cls_priv.def_read_label_stru;
    }
    else
    {
        g_user_cls_priv.def_write_label_stru = cls_get_label(polid, g_user_cls_priv.default_write_label);
    }

    /* 2.3 for row label */
    if (g_user_cls_priv.default_row_label == g_user_cls_priv.default_read_label)
    {
        g_user_cls_priv.def_row_label_stru = g_user_cls_priv.def_read_label_stru;
    }
    else if (g_user_cls_priv.default_row_label == g_user_cls_priv.default_write_label)
    {
        g_user_cls_priv.def_row_label_stru = g_user_cls_priv.def_write_label_stru;
    }
    else
    {
        g_user_cls_priv.def_row_label_stru = cls_get_label(polid, g_user_cls_priv.default_row_label);
    }

    MemoryContextSwitchTo(oldcontext);
    
    return ;
}

void mls_create_cls_check_expr(Relation rel)
{
    Oid relid;
    int16 attnum;

    /* skip other relation except tables and orignal partition tables */
    if ((RELKIND_RELATION != rel->rd_rel->relkind)
       &&(RELKIND_PARTITIONED_TABLE != rel->rd_rel->relkind))
    {
        return;
    }
    
    /* policy is bound on parent relation */
    relid  = mls_get_parent_oid(rel);

    /* skip system table */
    if (IS_SYSTEM_REL(relid))
    {
        return;
    }
    
    attnum = cls_check_table_has_cls_policy(relid);
    
    if (InvalidAttrNumber != attnum)
    {
        rel->rd_cls_struct         = cls_create_func_expr(rel);
        rel->rd_cls_struct->attnum = attnum;
    }
    else
    {
        rel->rd_cls_struct = NULL;
    }

    return;
}

/*
 * in procedure of copy to, filter hidden rows.
 */
bool mls_cls_check_row_validation_in_cp(Datum datum)
{
    ClsItem * clsitem;

    clsitem = (ClsItem *)datum;

    return cls_check_read(clsitem);
}

/*
 * only mls_admin could alter _cls
 */
bool mls_cls_column_drop_check(char * name)
{
    if (0 == pg_strcasecmp(name, "_cls"))
    {
        if (is_mls_user())
        {
            return true;
        }
        return false;
    }
    
    return true;
}

/*
 * only mls_admin could alter _cls, and type is clsitem
 */
bool mls_cls_column_add_check(char * colname, Oid typoid)
{
    if (0 == pg_strcasecmp(colname, "_cls"))
    {
        if (false == is_mls_user())
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                     errmsg("keywords _cls is reserverd")));
        }
        
        if (CLSITEM_OID != typoid)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                     errmsg("type for _cls column should be clsitem")));
        }
    }
    return true;
}

/*
 * to alter cls column is forbidded. 
 */
bool cls_check_table_col_has_policy(Oid relid, int attnum)
{
    if (CLSITEM_OID == get_atttype(relid, attnum))
    {
        return true;
    }

    if (is_mls_user())
    {
        ereport(ERROR,
                    (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                     errmsg("mls_admin could not do this")));
    }
    
    return false;
}

/*
 * check table has policy
 */
bool cls_check_table_has_policy(Oid relid)
{
	int16       attnum = InvalidAttrNumber;

	attnum = cls_check_table_has_cls_policy(relid);
	if (attnum != InvalidAttrNumber)
	{
		return true;
	}
	return false;
}

/*
 * check user whether has policy
 */
bool cls_check_user_has_policy(Oid roleid)
{
	SysScanDesc scan;
	ScanKeyData skey[1];
	HeapTuple   htup;
	Relation    rel;
	bool        found = false;

	ScanKeyInit(&skey[0],
	            Anum_pg_cls_user_userid,
	            BTEqualStrategyNumber,
	            F_OIDEQ,
	            ObjectIdGetDatum(roleid));

	rel = heap_open(ClsUserRelationId, AccessShareLock);
	scan = systable_beginscan(rel,
	                          PgClsUserPolidUseridIndexId,
	                          true,
	                          NULL,
	                          1,
	                          skey);

	while (HeapTupleIsValid(htup = systable_getnext(scan)))
	{
		Form_pg_cls_user form_cls_user = (Form_pg_cls_user) GETSTRUCT(htup);

		if (form_cls_user)
		{
			found = true;
			break;
		}
	}

	systable_endscan(scan);
	heap_close(rel, AccessShareLock);

	return found;
}

#endif
