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
#include "access/htup_details.h"
#include "access/xlogreader.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_audit.h"
#include "catalog/pg_class.h"
#include "catalog/pg_database.h"
#include "catalog/pg_type.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_mls.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "contrib/pgcrypto/pgp.h"
#include "executor/tuptable.h"
#include "executor/spi.h"
#include "nodes/makefuncs.h"
#include "nodes/primnodes.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "parser/parse_relation.h"
#include "storage/bufmgr.h"
#include "storage/lockdefs.h"
#include "storage/lwlock.h"
#include "storage/sinval.h"
#include "storage/shmem.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/mls.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"
#include "utils/memutils.h"
#include "utils/inval.h"
#include "utils/snapshot.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/ruleutils.h"
#include "utils/resowner_private.h"
#include "utils/relcrypt.h"
#include "utils/relcryptcache.h"
#include "utils/relcryptmisc.h"

#include "utils/relcryptmap.h"
#include "utils/datamask.h"

#include "pgxc/pgxcnode.h"
#include "pgxc/nodemgr.h"
#include "pgstat.h"



#if MARK("datamask")

/*just test stub */
#define DATAMASK_TEST_TEXT_STUB     "XXXX"
#define DATAMASK_TEST_INT8_STUB     PG_INT8_MAX
#define DATAMASK_TEST_INT16_STUB    PG_INT16_MAX
#define DATAMASK_TEST_INT32_STUB    PG_INT32_MAX
#define DATAMASK_TEST_INT64_STUB    PG_INT64_MAX


/*
 * value of option in pg_data_mask_map, indentify kinds of data mask.
 */
enum
{
    DATAMASK_KIND_INVALID       = 0,
    DATAMASK_KIND_VALUE         = 1,    /* exchange value with datamask */
    DATAMASK_KIND_STR_PREFIX    = 2,    /* to mask several characters in string, the 'several' is indendified by datamask */
    DATAMASK_KIND_DEFAULT_VAL   = 3,    /* datamask use default value kept in pg_data_mask_map.defaultval */
    DATAMASK_KIND_STR_POSTFIX   = 4,    /* same as DATAMASK_KIND_STR_PREFIX, except treating string from end to begin */
    DATAMASK_KIND_FUNC_PROCESS  = 5,    /* value treate by identified function */
    DATAMASK_KIND_BUTT
};

static Datum datamask_exchange_one_col_value(Form_pg_attribute attr, Datum inputval, bool isnull, DataMaskAttScan *mask,
                                             bool *datumvalid);
static bool datamask_attr_mask_is_valid(Datamask   *datamask, int attnum);
static char * transfer_str_prefix(text * text_str, int mask_bit_count);
static char * transfer_str_postfix(text * text_str, int mask_bit_count);

/*
 * relative to input_str, to alloc a new memory, and exchange serveral chars from end to begin
 * and returns up to max('mask_bit_count', strlen(input_str) characters
 * if input_str is null, a string of 'X' with 'mask_bit_count' length would be returned
 */
static char * transfer_str_postfix(text * text_str, int mask_bit_count)
{// #lizard forgives
    char *  str;
    char *  input_str = NULL;
    int     input_str_len = 0;
    int     character_len = 0;
    int     character_idx = 0;
    int     dst_loop   = 0;
    int     input_loop = 0;
    int     char_len   = 0;
    static char    mask_len  = 0;
    static char   *mask_char = NULL;

    if(mask_char == NULL)
    {
        /* perform conversion */
        mask_char = (char *) pg_do_encoding_conversion((unsigned char *)"X",
                             1,
                             PG_SQL_ASCII,
                             GetDatabaseEncoding());
        mask_len = strlen(mask_char);
    }
    
    /* string mask must be valid */
    Assert(mask_bit_count);

    if (text_str)
    {
        input_str     = VARDATA_ANY(text_str);
        input_str_len = VARSIZE_ANY_EXHDR(text_str);
		character_len = pg_mbstrlen_with_len(input_str,input_str_len);
    }

	if(character_len > mask_bit_count)
	{
		/* assume mini encoding byte to be 1,to avoid repalloc or calculation */
	    str = palloc0(input_str_len + (mask_len - 1) * mask_bit_count + 1);

	    while(input_loop < input_str_len)
	    {
	    	char_len = pg_mblen(input_str + input_loop);
			
			if(character_len - character_idx <= mask_bit_count)
			{
				memcpy(str + dst_loop,mask_char,(uint)mask_len);
				dst_loop += mask_len;
			}
			else
			{
				memcpy(str + dst_loop,input_str + input_loop,(uint)char_len);
				dst_loop += char_len;
			}

			input_loop += char_len;
			character_idx++;
	    }
	}
	else
	{
		str = palloc0(mask_len * mask_bit_count + 1);
		
		for(character_idx = 0; character_idx < mask_bit_count;character_idx++)
		{
			memcpy(str + dst_loop,mask_char,(uint)mask_len);
			dst_loop += mask_len;
		}
	}

    str[dst_loop] = '\0';

    return str;
}

/*
 * exactly same as transfer_str_postfix, except the direction of exchanging string
 */
static char * transfer_str_prefix(text * text_str, int mask_bit_count)
{// #lizard forgives
    char *  str;
    char *  input_str = NULL;
    int     input_str_len = 0;
    int     character_idx = 0;
    int     character_len = 0;
    int     dst_loop   = 0;
    int     input_loop = 0;
    int     char_len   = 0;
    static char    mask_len  = 0;
    static char   *mask_char = NULL;

    if(mask_char == NULL)
    {
        /* perform conversion */
        mask_char = (char *) pg_do_encoding_conversion((unsigned char *)"X",
                             1,
                             PG_SQL_ASCII,
                             GetDatabaseEncoding());
        mask_len = strlen(mask_char);

        Assert(mask_char);
    }

    /* string mask must be valid */
    Assert(mask_bit_count);

    if (text_str)
    {
        input_str     = VARDATA_ANY(text_str);
        input_str_len = VARSIZE_ANY_EXHDR(text_str);
		character_len = pg_mbstrlen_with_len(input_str,input_str_len);
    }

	if(character_len > mask_bit_count)
	{
	    str = palloc0(input_str_len + (mask_len - 1) * mask_bit_count + 1);

	    while(input_loop < input_str_len)
	    {
	    	char_len = pg_mblen(input_str + input_loop);
			
			if(character_idx < mask_bit_count)
			{
				memcpy(str + dst_loop,mask_char,(uint)mask_len);
				dst_loop += mask_len;
			}
			else
			{
				memcpy(str + dst_loop,input_str + input_loop,(uint)char_len);
				dst_loop += char_len;
			}

			input_loop += char_len;
			character_idx++;
	    }	    
	}
	else
	{
		str = palloc0(mask_len * mask_bit_count + 1);
		
		for(character_idx = 0; character_idx < mask_bit_count;character_idx++)
		{
			memcpy(str + dst_loop,mask_char,(uint)mask_len);
			dst_loop += mask_len;
		}
	}

	
	str[dst_loop] = '\0';
 
    return str;
}

bool dmask_chk_usr_and_col_in_whit_list(Oid relid, Oid userid, int16 attnum)
{
    SysScanDesc scan;
    ScanKeyData skey[3];
    HeapTuple   htup;
    Relation    rel;
    bool        found;

    found = false;
    
    ScanKeyInit(&skey[0],
                    Anum_pg_data_mask_user_relid,
                    BTEqualStrategyNumber, 
                    F_OIDEQ,
                    ObjectIdGetDatum(relid));
    ScanKeyInit(&skey[1],
                    Anum_pg_data_mask_user_userid,
                    BTEqualStrategyNumber, 
                    F_OIDEQ,
                    ObjectIdGetDatum(userid));
    ScanKeyInit(&skey[2],
                    Anum_pg_data_mask_user_attnum,
                    BTEqualStrategyNumber, 
                    F_INT2EQ,
                    Int16GetDatum(attnum));
    
    rel = heap_open(DataMaskUserRelationId, AccessShareLock);
    scan = systable_beginscan(rel, 
                              PgDataMaskUserIndexId, 
                              true,
                              NULL, 
                              3, 
                              skey);

    if (HeapTupleIsValid(htup = systable_getnext(scan)))
    {
        Form_pg_data_mask_user form_pg_datamask_user = (Form_pg_data_mask_user) GETSTRUCT(htup);
        if (true == form_pg_datamask_user->enable)
        {
            found = true;
        }
    }

    systable_endscan(scan);
    heap_close(rel, AccessShareLock);

    return found;
}

/*
 * return list of user name in white list
 * 
 * allocating in current memory context, so remember to free it.
 */
List * datamask_get_user_in_white_list(void)
{
    List * retlist = NIL;
    SysScanDesc scan;
    HeapTuple   htup;
    Relation    rel;
    HeapTuple   rtup;
    
    rel = heap_open(DataMaskUserRelationId, AccessShareLock);
    scan = systable_beginscan(rel, 
                              InvalidOid, 
                              false,
                              NULL, 
                              0, 
                              NULL);

    while (HeapTupleIsValid(htup = systable_getnext(scan)))
    {
        Form_pg_data_mask_user form_pg_datamask_user = (Form_pg_data_mask_user) GETSTRUCT(htup);
        if (true == form_pg_datamask_user->enable)
        {
            rtup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(form_pg_datamask_user->userid));
            if (HeapTupleIsValid(rtup))
            {
                Name username = palloc(sizeof(NameData));

                memcpy(username->data, NameStr(form_pg_datamask_user->username), sizeof(NameData));

                retlist = lappend(retlist, username);
            }
            ReleaseSysCache(rtup);
        }
    }

    systable_endscan(scan);
    heap_close(rel, AccessShareLock);

    return retlist;
}

bool datamask_check_user_in_white_list(Oid userid)
{
    SysScanDesc scan;
    ScanKeyData skey[1];
    HeapTuple   htup;
    Relation    rel;
    bool        found;

    if (false == g_enable_data_mask)
    {
        return false;
    }

    found = false;
    
    ScanKeyInit(&skey[0],
                    Anum_pg_data_mask_user_userid,
                    BTEqualStrategyNumber, 
                    F_OIDEQ,
                    ObjectIdGetDatum(userid));
   
    rel = heap_open(DataMaskUserRelationId, AccessShareLock);
    scan = systable_beginscan(rel, 
                              PgDataMaskUserIndexId, 
                              true,
                              NULL, 
                              1, 
                              skey);

    while (HeapTupleIsValid(htup = systable_getnext(scan)))
    {
        Form_pg_data_mask_user form_pg_datamask_user = (Form_pg_data_mask_user) GETSTRUCT(htup);
        if (true == form_pg_datamask_user->enable)
        {
            found = true;
            break;
        }
    }

    systable_endscan(scan);
    heap_close(rel, AccessShareLock);

    return found;
}


/*
 *  exchange one column once, while, only several basic type supported, 
 *      such as integer(int2\int4\int8),varchar,text 
 *  the col 'datamask' of pg_data_mask_map, that would be more flexible.
 */
static Datum datamask_exchange_one_col_value(Form_pg_attribute attr, Datum inputval, bool isnull, DataMaskAttScan *mask,
                                             bool *datumvalid)
{// #lizard forgives
    bool unknown_option_kind;
    bool unsupport_data_type;
    Datum value;
    char *ret_str;
    int option;
    int typmod;
    int string_len;

    value = Int32GetDatum(0);
    option = DATAMASK_KIND_INVALID;
    unknown_option_kind = false;
    unsupport_data_type = false;

    if (!mask->enable)
    {
        *datumvalid = false;
        return value;
    }

    if (mls_support_data_type(attr->atttypid))
    {
        *datumvalid = true;
        option = mask->option;

        switch (option)
        {
            case DATAMASK_KIND_VALUE:
            {
                if (INT4OID == attr->atttypid || INT2OID == attr->atttypid || INT8OID == attr->atttypid)
                {
                    value = Int64GetDatum(mask->datamask);
                }
                else
                {
                    unsupport_data_type = true;
                }
                break;
            }
            case DATAMASK_KIND_STR_PREFIX:
            {
                if (VARCHAROID == attr->atttypid
                    || TEXTOID == attr->atttypid
                    || VARCHAR2OID == attr->atttypid
                    || BPCHAROID == attr->atttypid)
                {
                    ret_str = transfer_str_prefix(isnull ? NULL : DatumGetTextP(inputval),
                                                  mask->datamask);

                    value = CStringGetTextDatum(ret_str);
                }
                else
                {
                    unsupport_data_type = true;
                }
                break;
            }
            case DATAMASK_KIND_STR_POSTFIX:
            {
                if (VARCHAROID == attr->atttypid
                    || TEXTOID == attr->atttypid
                    || VARCHAR2OID == attr->atttypid
                    || BPCHAROID == attr->atttypid)
                {
                    ret_str = transfer_str_postfix(isnull ? NULL : DatumGetTextP(inputval),
                                                   mask->datamask);

                    value = CStringGetTextDatum(ret_str);
                }
                else
                {
                    unsupport_data_type = true;
                }
                break;
            }
            case DATAMASK_KIND_DEFAULT_VAL:
            {
                if (TIMESTAMPOID == attr->atttypid)
                {
                    value = InputFunctionCall(&mask->flinfo,
                                              mask->defaultval,
                                              TIMESTAMPOID,
                                              -1); /* the typmod of timestamp is -1 */
                }
                else if (FLOAT4OID == attr->atttypid)
                {
                    value = InputFunctionCall(&mask->flinfo,
                                              mask->defaultval,
                                              TIMESTAMPOID,
                                              -1); /* the typmod of float4 is -1 */
                }
                else if (FLOAT8OID == attr->atttypid)
                {
                    value = InputFunctionCall(&mask->flinfo,
                                              mask->defaultval,
                                              TIMESTAMPOID,
                                              -1); /* the typmod of float8 is -1 */
                }
                else if (BPCHAROID == attr->atttypid)
                {
                    string_len = strlen(mask->defaultval);
                    if (string_len > (attr->atttypmod - VARHDRSZ))
                    {
                        typmod = -1;
                    }
                    else
                    {
                        typmod = attr->atttypmod;
                    }
                    value = InputFunctionCall(&mask->flinfo,
                                              mask->defaultval,
                                              BPCHAROID,
                                              typmod);
                }
                else if (VARCHAR2OID == attr->atttypid)
                {
                    value = InputFunctionCall(&mask->flinfo,
                                              mask->defaultval,
                                              VARCHAR2OID,
                                              -1); /* the typmod of varchar2 is -1 */
                }
                else if (NUMERICOID == attr->atttypid)
                {
                    value = InputFunctionCall(&mask->flinfo,
                                              mask->defaultval,
                                              NUMERICOID,
                                              -1); /* the typmod of numeric is -1 */
                }
                else if (VARCHAROID == attr->atttypid)
                {
                    value = InputFunctionCall(&mask->flinfo,
                                              mask->defaultval,
                                              VARCHAROID,
                                              -1); /* the typmod of varchar is -1 */
                }
                else if (TEXTOID == attr->atttypid)
                {
                    value = InputFunctionCall(&mask->flinfo,
                                              mask->defaultval,
                                              TEXTOID,
                                              -1); /* the text of varchar is -1 */
                }
                else
                {
                    unsupport_data_type = true;
                }
                break;
            }
            default:
                unknown_option_kind = true;
                break;
        }
    }
    else
    {
        unsupport_data_type = true;
    }

    if (unknown_option_kind)
        elog(ERROR, "datamask:unsupported type, typeid:%d for option:%d", attr->atttypid, option);

    if (unsupport_data_type)
        elog(ERROR, "datamask:unsupported type, typeid:%d for option:%d", attr->atttypid, option);

    return value;
}

bool datamask_scan_key_contain_mask(ScanState *node)
{
	int i = 0;
    Datamask   *datamask  = NULL;
	ScanKey		ScanKeys;
	int			NumScanKeys;

	if(node == NULL)
		return false;

	if(!IsA(node, IndexScanState) && !IsA(node, IndexOnlyScanState))
		return false;

    if (node->ss_currentRelation &&
		node->ss_currentRelation->rd_att)
    {
        datamask = node->ss_currentRelation->rd_att->tdatamask;
    }
	else
	{
		return false;
	}

	if(IsA(node, IndexScanState))
	{
		IndexScanState *state  = (IndexScanState *)node;
		ScanKeys = state->iss_ScanKeys;
		NumScanKeys = state->iss_NumScanKeys;
	}
	if (IsA(node, IndexOnlyScanState))
	{
		IndexOnlyScanState *state  = (IndexOnlyScanState *)node;
		ScanKeys = state->ioss_ScanKeys;
		NumScanKeys = state->ioss_NumScanKeys;
	}

	for(i = 0; i < NumScanKeys ;i++)
	{
		if(datamask_attr_mask_is_valid(datamask, ScanKeys[i].sk_attno - 1))
			return true;
	}

	return false;
}

/* 
 *  one col needs data masking when the corresponding mask_array is valid
 */
static bool datamask_attr_mask_is_valid(Datamask *datamask, int attnum)
{
    if (datamask)
    {
        return datamask->mask_array[attnum];
    }
    return false;
}

static void fill_att_mask_func(DataMaskAttScan *info, int attypid)
{
    Oid functionId;

    if(info->option != DATAMASK_KIND_DEFAULT_VAL)
        return ;

    switch(attypid)
    {
        case TIMESTAMPOID:
            functionId = TIMESTAMP_IN_OID;
            break;
        case FLOAT4OID:
            functionId = FLOAT4_IN_OID;
            break;
        case FLOAT8OID:
            functionId = FLOAT8_IN_OID;
            break;
        case BPCHAROID:
            functionId = BPCHAR_IN_OID;
            break;
        case VARCHAR2OID:
            functionId = VARCHAR2_IN_OID;
            break;
        case NUMERICOID:
            functionId = NUMERIC_IN_OID;
            break;
        case VARCHAROID:
            functionId = VARCHAR_IN_OID;
            break;
        case TEXTOID:
            functionId = TEXT_IN_OID;
            break;
        default:
            elog(ERROR,"type not supported.");
    }

    fmgr_info(functionId, &info->flinfo);
}

static void fill_att_mask_info(Oid relid, Form_pg_attribute attr, DataMaskAttScan *info)
{
    SysScanDesc scan;
    ScanKeyData skey[2];
    HeapTuple   htup;
    Relation    rel;

    info->enable = false;

    if (!mls_support_data_type(attr->atttypid))
        return ;

    ScanKeyInit(&skey[0],
                Anum_pg_data_mask_map_relid,
                BTEqualStrategyNumber,
                F_OIDEQ,
                ObjectIdGetDatum(relid));
    ScanKeyInit(&skey[1],
                Anum_pg_data_mask_map_attnum,
                BTEqualStrategyNumber,
                F_OIDEQ,
                Int32GetDatum(attr->attnum));

    rel = heap_open(DataMaskMapRelationId, AccessShareLock);
    scan = systable_beginscan(rel,
                              PgDataMaskMapIndexId,
                              true,
                              NULL,
                              2,
                              skey);

    if (HeapTupleIsValid(htup = systable_getnext(scan)))
    {
        Form_pg_data_mask_map form_pg_datamask = (Form_pg_data_mask_map) GETSTRUCT(htup);

        info->enable     = form_pg_datamask->enable;
        info->datamask   = form_pg_datamask->datamask;
        info->defaultval = TextDatumGetCString(&form_pg_datamask->defaultval);
        info->option     = form_pg_datamask->option;

        fill_att_mask_func(info,attr->atttypid);
    }

    systable_endscan(scan);
    heap_close(rel, AccessShareLock);
}

DataMaskState *init_datamask_desc(Oid relid, Form_pg_attribute *attrs, Datamask *datamask)
{
    DataMaskAttScan *att_info;
    DataMaskState *desc;
    int attno;
    int natts;

    natts = datamask->attmasknum;

    desc = palloc0(sizeof(DataMaskState));
    if (desc == NULL)
        elog(ERROR, "out of memory");

    desc->maskinfo = palloc0(sizeof(DataMaskAttScan) * natts);
    if (desc->maskinfo == NULL)
        elog(ERROR, "out of memory");

    for (attno = 0; attno < natts; attno++)
    {
        att_info = &desc->maskinfo[attno];

        if (!datamask_attr_mask_is_valid(datamask, attno))
        {
            att_info->enable = false;
            continue;
        }

        if(dmask_chk_usr_and_col_in_whit_list(relid, GetUserId(), attrs[attno]->attnum))
        {
            att_info->enable = false;
            continue;
        }

        att_info->enable = true;
        fill_att_mask_info(relid, attrs[attno], att_info);
    }

    return desc;
}

/* 
 * after tuple deform to slot, exchange the col values with those defined by user or defaults.
 */
void datamask_exchange_all_cols_value(Node *node, TupleTableSlot *slot)
{// #lizard forgives
    bool        need_exchange_slot_tts_tuple;
    bool        datumvalid;
    int         attnum;
    int         natts;
    TupleDesc   tupleDesc;
    Datum      *tuple_values;
    bool       *tuple_isnull;
    Datum      *slot_values;
    bool       *slot_isnull;
    Datamask   *datamask;
    HeapTuple   new_tuple;
    MemoryContext      old_memctx;
    Form_pg_attribute *att;
    ScanState       *scanstate;
    DataMaskAttScan *maskState;

    scanstate   = (ScanState *)node;
    tupleDesc   = slot->tts_tupleDescriptor;
    maskState   = scanstate->ss_currentMaskDesc->maskinfo;
    datamask    = tupleDesc->tdatamask;
    natts       = tupleDesc->natts;

    slot_values = slot->tts_values;
    slot_isnull = slot->tts_isnull;
    att         = tupleDesc->attrs;

    need_exchange_slot_tts_tuple = false;

    if (datamask)
    {
        if (slot->tts_tuple)
        {
            need_exchange_slot_tts_tuple = true;
        }

        old_memctx = MemoryContextSwitchTo(slot->tts_mls_mcxt);

        if (need_exchange_slot_tts_tuple)
        {
            tuple_values = (Datum *) palloc(natts * sizeof(Datum));
            tuple_isnull = (bool *) palloc(natts * sizeof(bool));

            heap_deform_tuple(slot->tts_tuple, tupleDesc, tuple_values, tuple_isnull);

        }

        for (attnum = 0; attnum < natts; attnum++)
        {
            if (datamask_attr_mask_is_valid(datamask, attnum))
            {
                Form_pg_attribute thisatt = att[attnum];

                datumvalid = false;
                if (need_exchange_slot_tts_tuple)
                {
                    slot_values[attnum]  = datamask_exchange_one_col_value(
                            thisatt,
                            tuple_values[attnum],
                            tuple_isnull[attnum],
                            &maskState[attnum],
                            &datumvalid);
                }
                else
                {
                    /* tuple_values are null, so try slot_values */
                    slot_values[attnum]  = datamask_exchange_one_col_value(
                            thisatt,
                            slot_values[attnum],
                            slot_isnull[attnum],
                            &maskState[attnum],
                            &datumvalid);
                }
                slot_isnull[attnum]  = false;

                /* 
                 * if datum is invalid, slot_values is invalid either, keep orginal value in tuple_value
                 * it seems a little bored
                 */
                if (need_exchange_slot_tts_tuple && datumvalid)
                {
                    tuple_values[attnum] = slot_values[attnum];
                    tuple_isnull[attnum] = slot_isnull[attnum];
                }
            }
        }

        if (need_exchange_slot_tts_tuple)
        {
            /* do not forget to set shardid */
            if (RelationIsSharded(scanstate->ss_currentRelation))
            {
                new_tuple = heap_form_tuple_plain(tupleDesc, tuple_values, tuple_isnull, RelationGetDisKey(scanstate->ss_currentRelation),
                                                  RelationGetSecDisKey(scanstate->ss_currentRelation), RelationGetRelid(scanstate->ss_currentRelation));
            }
            else
            {
                new_tuple = heap_form_tuple(tupleDesc, tuple_values, tuple_isnull);
            }

            /* remember to do this copy manually */
            new_tuple->t_self       = slot->tts_tuple->t_self;
            new_tuple->t_tableOid   = slot->tts_tuple->t_tableOid;
            new_tuple->t_xc_node_id = slot->tts_tuple->t_xc_node_id;

            if (slot->tts_shouldFree)
            {
                heap_freetuple(slot->tts_tuple);
            }

            slot->tts_tuple      = new_tuple;
            slot->tts_shouldFree = true;

            /* fresh tts_values in slot */
            slot_deform_tuple_extern((void*)slot, natts);

            pfree(tuple_values);
            pfree(tuple_isnull);
        }

        MemoryContextSwitchTo(old_memctx);
    }

    return;
}

/*
 * a little quick check whether this table binding a datamask.
 */
bool datamask_check_table_has_datamask(Oid relid)
{
    SysScanDesc scan;
    ScanKeyData skey[1];
    HeapTuple   htup;
    Relation    rel;
    bool        hasdatamask;

    ScanKeyInit(&skey[0],
                    Anum_pg_data_mask_map_relid,
                    BTEqualStrategyNumber,
                    F_OIDEQ,
                    ObjectIdGetDatum(relid));

    rel = heap_open(DataMaskMapRelationId, AccessShareLock);
    scan = systable_beginscan(rel,
                              PgDataMaskMapIndexId,
                              true,
                              NULL,
                              1,
                              skey);

    hasdatamask = false;
    while (HeapTupleIsValid(htup = systable_getnext(scan)))
    {
        Form_pg_data_mask_map form_data_mask_map = (Form_pg_data_mask_map) GETSTRUCT(htup);
        if (true == form_data_mask_map->enable)
        {
            hasdatamask = true;
            break;
        }
    }

    systable_endscan(scan);
    heap_close(rel, AccessShareLock);

    return hasdatamask;
}

/*
 * here, we already known this table coupled with datamask, so check attributes one by one to mark.
 */
bool dmask_check_table_col_has_dmask(Oid relid, int attnum)
{
    SysScanDesc scan;
    ScanKeyData skey[2];
    HeapTuple   htup;
    Relation    rel;
    bool        hasdatamask;

    if (false == g_enable_data_mask)
    {
        return false;
    }

    ScanKeyInit(&skey[0],
                    Anum_pg_data_mask_map_relid,
                    BTEqualStrategyNumber,
                    F_OIDEQ,
                    ObjectIdGetDatum(relid));
    ScanKeyInit(&skey[1],
                    Anum_pg_data_mask_map_attnum,
                    BTEqualStrategyNumber,
                    F_OIDEQ,
                    Int32GetDatum(attnum));

    rel = heap_open(DataMaskMapRelationId, AccessShareLock);
    scan = systable_beginscan(rel,
                              PgDataMaskMapIndexId,
                              true,
                              NULL,
                              2,
                              skey);

    hasdatamask = false;
    if (HeapTupleIsValid(htup = systable_getnext(scan)))
    {
        Form_pg_data_mask_map form_pg_datamask = (Form_pg_data_mask_map) GETSTRUCT(htup);
        if (true == form_pg_datamask->enable)
        {
            hasdatamask = true;
        }
    }

    systable_endscan(scan);
    heap_close(rel, AccessShareLock);

    return hasdatamask;
}

static Datamask * datamask_alloc_datamask_struct(int attmasknum)
{
    Datamask * datamask;

    datamask = (Datamask * )palloc(sizeof(Datamask));
    datamask->attmasknum = attmasknum;
    datamask->mask_array = (bool*)palloc0(sizeof(bool)*attmasknum);

    return datamask;
}

/*
 * to make up tupdescfree.
 */
void datamask_free_datamask_struct(Datamask *datamask)
{
    if (datamask)
    {
        if (datamask->mask_array)
        {
            pfree(datamask->mask_array);
        }

        pfree(datamask);
    }

    return ;
}

/*
 * we place datamask in tupledesc
 */
void dmask_assgin_relat_tupledesc_fld(Relation relation)
{
    int         attnum;
    int         natts;
    Datamask*   datamask;
    Oid         parent_oid;

    if (false == g_enable_data_mask)
    {
        return;
    }

    if (!IS_SYSTEM_REL(RelationGetRelid(relation)))
    {
        parent_oid = mls_get_parent_oid(relation);

        /* quick check wether this table has col with datamask */
        if (false == datamask_check_table_has_datamask(parent_oid))
        {
            if (relation->rd_att->tdatamask)
            {
                datamask_free_datamask_struct(relation->rd_att->tdatamask);
            }
            relation->rd_att->tdatamask = NULL;
            return;
        }

        natts = relation->rd_att->natts;

        datamask = (Datamask * )MemoryContextAllocZero(CacheMemoryContext, sizeof(Datamask));
        datamask->attmasknum = natts;
        datamask->mask_array = (bool*)MemoryContextAllocZero(CacheMemoryContext, sizeof(bool)*natts);

        for (attnum = 0; attnum < natts; attnum++)
        {
            /* skip the dropped column */
            if (0 == relation->rd_att->attrs[attnum]->attisdropped)
            {
                datamask->mask_array[attnum] = dmask_check_table_col_has_dmask(parent_oid, attnum+1);
            }
        }
        
        relation->rd_att->tdatamask = datamask;
    }
    else
    {
        relation->rd_att->tdatamask = NULL;
    }
}

/*
 * to make up tupledesccopy.
 */ 
void datamask_alloc_and_copy(TupleDesc dst, TupleDesc src)
{
    if (false == g_enable_data_mask)
    {
        return;
    }
    
    /*skip system table*/
    if (src->attrs)
    {
        if (!IS_SYSTEM_REL(src->attrs[0]->attrelid))
        {
            if (dst->tdatamask)
            {
                datamask_free_datamask_struct(dst->tdatamask);
                dst->tdatamask = NULL;
            }
            if (src->tdatamask)
            {
                if (src->tdatamask->attmasknum)
                {
                    dst->tdatamask = datamask_alloc_datamask_struct(src->tdatamask->attmasknum);

                    memcpy(dst->tdatamask->mask_array,
                            src->tdatamask->mask_array,
                            sizeof(bool) * src->tdatamask->attmasknum);
                }
            }
        }
    }
    return;
}

/*
 * to make up the equal judgement of tupledesc.
 */ 
bool datamask_check_datamask_equal(Datamask * dm1, Datamask * dm2)
{// #lizard forgives
    if (false == g_enable_data_mask)
    {
        return true;
    }

    if ((dm1 && !dm2) || (!dm1 && dm2))
    {
        return false;
    }

    if (dm1 == dm2)
    {
        return true;
    }

    if (dm1->attmasknum != dm2->attmasknum)
    {
        return false;
    }

    if (0 != memcpy(dm1->mask_array, dm2->mask_array, dm1->attmasknum))
    {
        return false;
    }

    return true;
}

void dmask_exchg_all_cols_value_copy(TupleDesc tupleDesc, Datum   *tuple_values, bool*tuple_isnull, Oid relid)
{
    int         attnum;
    int         natts;
    Datamask   *datamask;
    Datum       datum_ret;
    bool        datumvalid;
    Form_pg_attribute *att;
    DataMaskState *maskstate;

    natts    = tupleDesc->natts;
    att      = tupleDesc->attrs;
    datamask = tupleDesc->tdatamask;

    maskstate = init_datamask_desc(relid, att, datamask);

    for (attnum = 0; attnum < natts; attnum++)
    {
        if (datamask_attr_mask_is_valid(datamask, attnum))
        {
            Form_pg_attribute thisatt = att[attnum];

            datumvalid = false;

            datum_ret = datamask_exchange_one_col_value(
                    thisatt,
                    tuple_values[attnum],
                    tuple_isnull[attnum],
                    &maskstate->maskinfo[attnum],
                    &datumvalid);

            if (datumvalid)
                tuple_values[attnum] = datum_ret;

            tuple_isnull[attnum] = false;
        }
    }
}

bool datamask_check_column_in_expr(Node * node, void * context)
{// #lizard forgives
    ParseState   * parsestate;
    Var          * var;
    RangeTblEntry* rte;
    bool           found;
    bool           is_legaluser;
    char         * attrname;
    Oid            parent_oid;
    Oid            relid;
    //ListCell     * cell;
    TargetEntry  * targetentry;

    if (node == NULL)
    {
        return false;
    }

    if (IsA(node, Var))
    {
        parsestate = (ParseState *)context;
        var        = (Var*)node;

        if (parsestate->p_rtable)
        {
            rte = GetRTEByRangeTablePosn(parsestate, var->varno, var->varlevelsup);
            if (NULL != rte)
            {
                if (RTE_SUBQUERY == rte->rtekind)
                {
                    targetentry = (TargetEntry*)list_nth(rte->subquery->targetList, var->varattno - 1);

                    if ((InvalidOid != targetentry->resorigtbl) && (InvalidAttrNumber != targetentry->resorigcol))
                    {   
                        parent_oid  = mls_get_parent_oid_by_relid(targetentry->resorigtbl);
                        found       = dmask_check_table_col_has_dmask(parent_oid, targetentry->resorigcol);
                        if (found)
                        {
                            is_legaluser = dmask_chk_usr_and_col_in_whit_list(parent_oid, GetAuthenticatedUserId(), var->varattno);
                            if (!is_legaluser)
                            {
                                attrname = get_relid_attribute_name(parent_oid, targetentry->resorigcol);
                                if (NULL != attrname)
                                {
                                    elog(ERROR, "column(attrname:%s) with datamask policy is forbidden in expression", attrname);
                                }
                                else
                                {
                                    elog(ERROR, "column(attrnum:%d) with datamask policy is forbidden in expression", targetentry->resorigcol);
                                }
                            }
                        }
                    }         
                }
                else if (RTE_RELATION == rte->rtekind)
                {
                    relid = rte->relid;
                    
                    parent_oid = mls_get_parent_oid_by_relid(relid);
                    found      = dmask_check_table_col_has_dmask(parent_oid, var->varattno);
                    if (found)
                    {
                        is_legaluser = dmask_chk_usr_and_col_in_whit_list(parent_oid, GetAuthenticatedUserId(), var->varattno);
                        if (!is_legaluser)
                        {
                            attrname = get_rte_attribute_name(rte, var->varattno);
                            if (NULL != attrname)
                            {
                                elog(ERROR, "column(attrname:%s) with datamask policy is forbidden in expression", attrname);
                            }
                            else
                            {
                                elog(ERROR, "column(attrnum:%d) with datamask policy is forbidden in expression", var->varattno);
                            }
                        }
                    }
                }
            }
        }
    }
    else if (IsA(node, TargetEntry))
    {
        TargetEntry* te;
        
        te = (TargetEntry*)node;
        
        parsestate = (ParseState *)context;
        if (parsestate->p_target_relation)
        {
            relid      = parsestate->p_target_relation->rd_id;
            
            parent_oid = mls_get_parent_oid_by_relid(relid);
            found      = dmask_check_table_col_has_dmask(parent_oid, te->resno);
            if (found)
            {
                is_legaluser = dmask_chk_usr_and_col_in_whit_list(parent_oid, GetAuthenticatedUserId(), te->resno);
                if (!is_legaluser)
                {
                    if (NULL != te->resname)
                    {
                        elog(ERROR, "column(attrname:%s) with datamask policy is forbidden in expression", te->resname);
                    }
                    else
                    {
                        elog(ERROR, "column(attrnum:%d) with datamask policy is forbidden in expression", te->resno);
                    }
                }
            }
        }
    }

    return expression_tree_walker(node, datamask_check_column_in_expr, context);
}


#endif

