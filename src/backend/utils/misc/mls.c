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
#include "access/heapam.h"
#include "access/relscan.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_audit.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_mls.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/partition.h"
#include "catalog/pg_inherits_fn.h"
#include "contrib/pgcrypto/pgp.h"
#include "commands/schemacmds.h"
#include "commands/tablespace.h"
#include "commands/extension.h"
#include "executor/tuptable.h"
#include "executor/spi.h"
#include "nodes/makefuncs.h"
#include "nodes/primnodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/execnodes.h"
#include "parser/parsetree.h"
#include "parser/parse_relation.h"
#include "storage/bufmgr.h"
#include "storage/buf_internals.h"
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
#include "utils/guc.h"

#include "utils/mls_extension.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/nodemgr.h"
#include "pgstat.h"




#ifdef _PG_REGRESS_
bool g_enable_cls               = true;
bool g_enable_data_mask         = true;
bool g_enable_transparent_crypt = true;
bool g_enable_crypt_debug       = true;
#else
bool g_enable_cls               = false;
bool g_enable_data_mask         = false;
bool g_enable_transparent_crypt = false;
bool g_enable_crypt_debug       = false;
#endif
int g_rel_crypt_hash_size = 2048;


#define MLS_QUERY_STRING_PRUNE_DELIMETER '('


Datum pg_execute_query_on_all_nodes(PG_FUNCTION_ARGS);
Datum pg_get_table_oid_by_name(PG_FUNCTION_ARGS);
Datum pg_get_role_oid_by_name(PG_FUNCTION_ARGS);
Datum pg_get_function_oid_by_name(PG_FUNCTION_ARGS);
Datum pg_get_schema_oid_by_name(PG_FUNCTION_ARGS);
Datum pg_get_tablespace_oid_by_tablename(PG_FUNCTION_ARGS);
Datum pg_get_tablespace_name_by_tablename(PG_FUNCTION_ARGS);
Datum pg_get_current_database_oid(PG_FUNCTION_ARGS);
Datum pg_trsprt_crypt_support_datatype(PG_FUNCTION_ARGS);

extern List * FunctionGetOidsByNameString(List * func_name);

bool g_is_mls_user = false;

static Oid  g_mls_acl_table_oid     = InvalidOid;
static Oid  g_mls_acl_namespace_oid = InvalidOid;

#if MARK("sys function")
/*
 * this is a special function, called by cls user or audit user, to modify cls or audit system tables directly,
 * and would keep the transaction consistence and relcache fresh.
 */
Datum pg_execute_query_on_all_nodes(PG_FUNCTION_ARGS)
{
    char    *query_str;
    Oid     *co_oids;
    Oid     *dn_oids;
    int     num_dnodes;
    int     num_coords;
    Oid     authenticate_userid;

    authenticate_userid = GetAuthenticatedUserId();

    if (!is_mls_user() && (DEFAULT_ROLE_AUDIT_SYS_USERID != authenticate_userid))
    {
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("permission denied, function only could be called by cls user or audit user")));  
    }

    query_str = text_to_cstring(PG_GETARG_TEXT_PP(0));
    if (NULL == query_str)
    {
        elog(ERROR, "input query is NULL");
    }

    PgxcNodeGetOids(&co_oids, &dn_oids, &num_coords, &num_dnodes, false);

    pgxc_execute_on_nodes(num_coords, co_oids, query_str);
    pgxc_execute_on_nodes(num_dnodes, dn_oids, query_str);
    
    return BoolGetDatum(true);
}

/*
 * there are basic functions, get object id with name, 
 * cause oid on nodes maybe different, so get the oid locally.
 */
Datum pg_get_table_oid_by_name(PG_FUNCTION_ARGS)
{
    text       *tablename = PG_GETARG_TEXT_PP(0);
    Oid            tableoid;

    tableoid = convert_table_name(tablename);

    PG_RETURN_OID(tableoid);
}

Datum pg_get_role_oid_by_name(PG_FUNCTION_ARGS)
{
    Name        rolename = PG_GETARG_NAME(0);
    Oid            roleoid;

    roleoid = get_role_oid(NameStr(*rolename), false);

    PG_RETURN_OID(roleoid);
}

Datum pg_get_function_oid_by_name(PG_FUNCTION_ARGS)
{
    Name        funcname = PG_GETARG_NAME(0);
    Oid            funcoid;
    List *      funcnamelist;
    List *      funcoidlist;

    funcnamelist = list_make1(NameStr(*funcname));

    funcoidlist  = FunctionGetOidsByNameString(funcnamelist);
    
    funcoid      = list_nth_oid(funcoidlist, 0);

    PG_RETURN_OID(funcoid);
}

Datum pg_get_schema_oid_by_name(PG_FUNCTION_ARGS)
{
    Oid            schemaoid;
    Name        nspname = PG_GETARG_NAME(0);
    
    schemaoid  = get_namespace_oid(NameStr(*nspname), false);

    PG_RETURN_OID(schemaoid);
}

Datum pg_get_tablespace_oid_by_tablename(PG_FUNCTION_ARGS)
{
    Oid              relid;
    Oid              spcoid;
    HeapTuple      reltup;
    Form_pg_class relform;
    text       *  tablename;
    
    tablename = PG_GETARG_TEXT_PP(0);

    relid     = convert_table_name(tablename);
    
    reltup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(reltup))
    {
        elog(ERROR, "cache lookup failed for relation %u", relid);
    }
    relform = (Form_pg_class) GETSTRUCT(reltup);

    if (InvalidOid == relform->reltablespace)
    {
        spcoid = DEFAULTTABLESPACE_OID;
    }
    else
    {
        spcoid = relform->reltablespace;
    }

    ReleaseSysCache(reltup);

    PG_RETURN_OID(spcoid);
}

Datum pg_get_tablespace_name_by_tablename(PG_FUNCTION_ARGS)
{
    Oid              relid;
    Oid              spcoid;
    Name          spcname;
    HeapTuple      reltup;
    Form_pg_class relform;
    text       *  tablename;
    Form_pg_tablespace spcform;
    
    tablename = PG_GETARG_TEXT_PP(0);

    /* get relid of table string line */
    relid     = convert_table_name(tablename);

    /* get spaceoid of relid */
    reltup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(reltup))
    {
        elog(ERROR, "cache lookup failed for relation %u", relid);
    }
    relform = (Form_pg_class) GETSTRUCT(reltup);
    if (InvalidOid == relform->reltablespace)
    {
        spcoid = DEFAULTTABLESPACE_OID;
    }
    else
    {
        spcoid = relform->reltablespace;
    }
    ReleaseSysCache(reltup);

    /* alloc result set */
    spcname = (Name) palloc(NAMEDATALEN);
    memset(NameStr(*spcname), 0, NAMEDATALEN);

    /* get spcname of spaceoid */
    reltup = SearchSysCache1(TABLESPACEOID, ObjectIdGetDatum(spcoid));
    if (!HeapTupleIsValid(reltup))
    {
        elog(ERROR, "cache lookup failed for tablespace %u", spcoid);
    }
    spcform = (Form_pg_tablespace) GETSTRUCT(reltup);
    namestrcpy(spcname, NameStr(spcform->spcname));
    ReleaseSysCache(reltup);
    
    /* here we go */
    PG_RETURN_NAME(spcname);
}

Datum pg_get_current_database_oid(PG_FUNCTION_ARGS)
{
    PG_RETURN_OID(MyDatabaseId);
}

Datum pg_trsprt_crypt_support_datatype(PG_FUNCTION_ARGS)
{
    Oid datatype;
    bool support;

    datatype = PG_GETARG_OID(0);

    support = mls_support_data_type(datatype);
    
    PG_RETURN_BOOL(support);
}
#endif

#if MARK("mls permission control")
/*
 * check relid having mls policy bound. 
 * at the same time, schema_bound means wether the schema where relid belongs is bound mls policy,
 * under this condition, the output arg is usefull, one relation could be dropped directly whose schema is crypted,
 * cause, the relation would be crypted when created again, 
 * so there is no corner for somebody who wants to escape relfile crypto by a 'fake' with the same relname.
 */
bool mls_check_relation_permission(Oid relid, bool * schema_bound)
{
    Oid  parent_oid;

    if (!IS_SYSTEM_REL(relid))
    {
        if (schema_bound)
        {
            *schema_bound = false;
        }
        
        parent_oid = mls_get_parent_oid_by_relid(relid);
        
        if (datamask_check_table_has_datamask(parent_oid) ||
		        datamask_check_table_has_datamask(relid))
        {
            return true;
        }

        if (trsprt_crypt_check_table_has_crypt(parent_oid,  true, schema_bound) ||
		        trsprt_crypt_check_table_has_crypt(relid,  true, schema_bound))
        {
            return true;
        }

        if (cls_check_table_has_policy(parent_oid) ||
                cls_check_table_has_policy(relid))
        {
        	return true;
        }

    }

    return false;
}

bool mls_check_schema_permission(Oid schemaoid)
{
    bool        found;
    List       *relid_list;
    ListCell   *cell;
    Oid         relOid;

    if (TRANSP_CRYPT_INVALID_ALGORITHM_ID != mls_check_schema_crypted(schemaoid))
    {
        return true;
    }

    found      = false;
    relid_list = NIL;

    relid_list = mls_get_relid_list_in_schema(schemaoid);
    if (NIL != relid_list)
    {
        foreach(cell, relid_list)
        {
            relOid = lfirst_oid(cell);

            found = mls_check_relation_permission(relOid, NULL);
            if (true == found)
            {
                return found;
            }
        }
    }

    return found;
}

bool mls_check_column_permission(Oid relid, int attnum)
{
    Oid  parent_oid;

    if (!IS_SYSTEM_REL(relid))
    {
        parent_oid = mls_get_parent_oid_by_relid(relid);

        if (dmask_check_table_col_has_dmask(parent_oid, attnum) ||
		        dmask_check_table_col_has_dmask(relid, attnum))
        {
            return true;
        }

        if (trsprt_crypt_chk_tbl_col_has_crypt(parent_oid, attnum) ||
		        trsprt_crypt_chk_tbl_col_has_crypt(relid, attnum))
        {
            return true;
        }

        if (cls_check_table_col_has_policy(parent_oid, attnum) ||
		        cls_check_table_col_has_policy(relid, attnum))
        {
            return true;
        }        
    }

    return false;

}

bool mls_check_role_permission(Oid roleid)
{
    bool found;

    found = datamask_check_user_in_white_list(roleid);

    return found;
}

bool mls_user(void)
{
    if (DEFAULT_ROLE_MLS_SYS_USERID == GetAuthenticatedUserId())
    {
        return true;
    }
    return false;
}

/*
 * pooler connection option contains MLS_CONN_OPTION, so, if exists, skip md5 authentication.
 */
bool mls_check_inner_conn(const char * cmd_option)
{
    if (cmd_option)
    {
        if(strstr(cmd_option, MLS_CONN_OPTION) != NULL)
        {
            return true;
        }
    }
    return false;
}


#endif

#if MARK("queue")
#pragma pack (4)
typedef struct QueueData 
{
    volatile int head;
    volatile int tail;

    /* protect above members */
    //slock_t      lock;
    pthread_mutex_t lock;
    int          length;
    int          element_size;
    
    void     *   data;
}QueueData;

#pragma pack () 

typedef QueueData * Queue;

typedef struct BufEncryptElement
{
    int buf_id;
    int algo_id;
    int slot_id;
    int status;
    CryptKeyInfo cryptkey; 
}BufEncryptElement;

typedef struct BufCryptedElement
{
    int buf_id;         /* orignal buf id */
    int slot_id;        /* crypted slot id */
    int error_code;     /* if error_code is not zero, use buf_id to get buftowrite */
    int status;
}BufCryptedElement;

static bool QueueInit(Queue q, int elementcnt, int elmentsz, void * data);
static bool QueuePutSingle(Queue q, void * element);
static bool QueueGetSingle(Queue q, void * element);
static int QueueGetLength(Queue q);
static bool QueueIsFull(Queue q);

static bool QueueInit(Queue q, int elementcnt, int elmentsz, void * data)
{
    if (q)
    {
        q->length       = elementcnt;
        q->element_size = elmentsz; 
        q->head         = 0;
        q->tail         = 0;
        //SpinLockInit(&q->lock);
        pthread_mutex_init(&q->lock, NULL);
        q->data         = (void*)(data);
        return true;
    }

    return false;
}

static bool QueuePutSingle(Queue q, void * element)
{
    //SpinLockAcquire(&q->lock);
    pthread_mutex_lock(&q->lock);
    if ((q->tail + 1) % q->length == q->head)
    {
        //SpinLockRelease(&q->lock);
        pthread_mutex_unlock(&q->lock);
        return false;
    }

    memcpy((char*)(q->data) + (q->tail * q->element_size), (char*)element, q->element_size);
    
    q->tail = (q->tail + 1) % q->length;  
    
    //SpinLockRelease(&q->lock);
    pthread_mutex_unlock(&q->lock);
    return true;
}

static bool QueueGetSingle(Queue q, void * element)
{
    //SpinLockAcquire(&q->lock);
    pthread_mutex_lock(&q->lock);
    if (q->head == q->tail)
    {
        //SpinLockRelease(&q->lock);
        pthread_mutex_unlock(&q->lock);
        return false;                
    }            

    memcpy((char*)element, (char*)(q->data) + (q->head * q->element_size), q->element_size);         

    q->head = (q->head + 1) % q->length;  

    //SpinLockRelease(&q->lock);
    pthread_mutex_unlock(&q->lock);
    return true;
}

static int QueueGetLength(Queue q)
{
    int len;
    
    //SpinLockAcquire(&q->lock);
    pthread_mutex_lock(&q->lock);
    if (q->head == q->tail)
    {
        //SpinLockRelease(&q->lock);
        pthread_mutex_unlock(&q->lock);
        return 0;                
    }            
#if 0
    if (q->head < q->tail)
    {
        len = q->tail - q->head;
    }
    else
    {
        len = q->tail - q->head + q->length;    
    }
#endif
    len = (q->tail - q->head + q->length) % q->length;

    //SpinLockRelease(&q->lock);
    pthread_mutex_unlock(&q->lock);
    return len;
}

static bool QueueIsFull(Queue q)
{
    //SpinLockAcquire(&(q->lock));
    pthread_mutex_lock(&q->lock);
    if ((q->tail + 1) % q->length == q->head)
    {
        //SpinLockRelease(&(q->lock));
        pthread_mutex_unlock(&q->lock);
        return true;
    }

    //SpinLockRelease(&(q->lock));
    pthread_mutex_unlock(&q->lock);
    return false;
}

#endif

#if MARK("crypt buffer parellel")

/* slot id queue should be a littel more, in case encrypt and crypted queue are full */
#define CRYPT_SLOT_QUEUE_CNT_MULTI_FACTOR    3
/* every crypting slot has 2 blocks */
#define CRYPT_ONE_BUF_NEED_TEMP_MULTI_FACTOR 2

/* hold all infos of crypt workers */
typedef struct tagArgsForEncryptWorker
{
    Queue         encrypt_queue;
    Queue         crypted_queue;
    Queue         slot_queue;
    char         *slot_pool;
    unsigned long crypted_cnt;
    int           worker_id;
}ArgsForEncryptWorker;
ArgsForEncryptWorker **g_crypt_worker_info;

/* number of default crypt workers and crypt queue */
int g_checkpoint_crypt_worker       = 4;
int g_checkpoint_crypt_queue_length = 8;
int g_crypt_parellel_main_running   = false;

static int mls_get_crypt_worker_id(void);
static char * mls_get_crypt_block(char * pool, int idx);
static void* mls_crypt_worker(void * input);


extern void mls_start_crypt_parellel_workers(void);
/*
 * split the pool by offset
 */
static char * mls_get_crypt_block(char * pool, int idx)
{
    if (idx >= 0 && idx < g_checkpoint_crypt_queue_length*CRYPT_SLOT_QUEUE_CNT_MULTI_FACTOR - 1 )
    {
        return pool + idx * BLCKSZ * CRYPT_ONE_BUF_NEED_TEMP_MULTI_FACTOR;
    }
    abort();
}

/*
 * crypt worker main
 */
static void* mls_crypt_worker(void * input)
{
    ArgsForEncryptWorker* arg;       
    char                * buf;
    char                * slot_pool;
    char                * buf_need_encrypt;
    char                * page_new;
    BufferDesc          * bufdesc;
    int                   ret;
    Queue                 encrypt_queue; 
    Queue                 crypted_queue; 
    BufEncryptElement     encrypt_element;
    BufCryptedElement     crypted_element;
    int                   localcnt;
    int                   workerid;
    
    arg = (ArgsForEncryptWorker *) input;

    encrypt_queue = arg->encrypt_queue;
    crypted_queue = arg->crypted_queue;
    slot_pool     = arg->slot_pool;
    workerid      = arg->worker_id;
    localcnt      = 0;
    
    arg->crypted_cnt = 0;

    for (;;)
    {
        bool need_mprotect = false;

        if (false == g_crypt_parellel_main_running)
        {
            break;
        }
        
        /* 1. get one buf for encrypt */
        if (0 == QueueGetLength(encrypt_queue))
        {
            /* if there is no job go to sleep */
            pg_usleep(100000L);
            continue;
        }

        QueueGetSingle(encrypt_queue, &encrypt_element);

        localcnt++;
        if (0 == localcnt%1000)
        {
            arg->crypted_cnt += localcnt;
        }
        
        /* 2. transform to buf and encrypt it */
        bufdesc  = GetBufferDescriptor(encrypt_element.buf_id);
        buf      = BufHdrGetBlockFunc(bufdesc);

        /* 2.1 offset pool for crypting buf */
        page_new         = mls_get_crypt_block(slot_pool, encrypt_element.slot_id);
        buf_need_encrypt = page_new + BLCKSZ;

        /* 2.2 do the encrypt */
        need_mprotect = enable_buffer_mprotect && !BufferIsLocal(encrypt_element.buf_id);
        if (need_mprotect)
        {
            BufDisableMemoryProtection(buf, false);
        }
        ret      = rel_crypt_page_encrypting_parellel(encrypt_element.algo_id, buf, buf_need_encrypt, page_new, encrypt_element.cryptkey, workerid);
        if (need_mprotect)
        {
            BufEnableMemoryProtection(buf, false);
        }

        /* 3. put it to crypted queue */
        while (QueueIsFull(crypted_queue))
        {
            //sleep
            pg_usleep(10000L);
        }

        crypted_element.buf_id     = encrypt_element.buf_id;
        crypted_element.slot_id    = encrypt_element.slot_id;
        crypted_element.status     = encrypt_element.status;
        crypted_element.error_code = ret;        
        
        QueuePutSingle(crypted_queue, &crypted_element);
    }
    
    return NULL;
}

/*
 * free slot for worker
 */
void mls_crypt_worker_free_slot(int worker_id, int slot_id)
{
    ArgsForEncryptWorker * args;

    args = g_crypt_worker_info[worker_id];

    if (false == QueuePutSingle(args->slot_queue, &slot_id))
    {
        /* should never happen */
        abort();
    }

    return;
}

/*
 * MUST get an 'free' worker whose queue length is shortest.
 */
static int mls_get_crypt_worker_id(void)
{
    static int worker_id = 0;
    int        ret_worker_id;
    ArgsForEncryptWorker * args;

    ret_worker_id = worker_id;
    
    for (;;)
    {
        args = g_crypt_worker_info[ret_worker_id];
        if (false == QueueIsFull(args->encrypt_queue))
        {
            worker_id = (ret_worker_id+1)%g_checkpoint_crypt_worker;
            break;
        }
        
        ret_worker_id = (ret_worker_id+1)%g_checkpoint_crypt_worker;
        
        pg_usleep(10);  
    }
    
    return ret_worker_id;
}

/*
 * look for crypted buffers and write them out.
 * collect buffers from all workers.
 */
List * mls_get_crypted_buflist(List * buf_id_list)
{
    int i;
    ArgsForEncryptWorker * args;
    BufCryptedElement      element;
    char                 * buf;
    BufferDesc           * bufdesc;
    
    for (i = 0; i < g_checkpoint_crypt_worker; i++)
    {
        args = g_crypt_worker_info[i];
        while (0 != QueueGetLength(args->crypted_queue))
        {
            QueueGetSingle(args->crypted_queue, &element);
            if (CRYPT_RET_SUCCESS == element.error_code)
            {   
                /* offset slot_id to crypted block, hold the slot_id and free after smgrwrite */
                buf = mls_get_crypt_block(args->slot_pool, element.slot_id);
                
                buf_id_list = SyncBufidListAppend(buf_id_list, element.buf_id, element.status, element.slot_id, i, buf);
            }
            else
            {
                /* error happens in crypting, so use orignal ones, and algo_id in page should be INVALID(TRANSP_CRYPT_INVALID_ALGORITHM_ID) */
                bufdesc = GetBufferDescriptor(element.buf_id);
                buf     = BufHdrGetBlockFunc(bufdesc);
                
                buf_id_list = SyncBufidListAppend(buf_id_list, element.buf_id, element.status, element.slot_id, i, buf);
                
                elog(LOG, "CHECKPOINT:buf:%d encrypt error:%d, use orignal one, algo_id:%d in buf", 
                    element.buf_id, element.error_code, PageGetAlgorithmId(buf));
                
                Assert(TRANSP_CRYPT_INVALID_ALGORITHM_ID == PageGetAlgorithmId(buf));
            }
        }
    }

    return buf_id_list;
}

/*
 * judge if there was buffer in encrypt process.
 */
bool mls_encrypt_queue_is_empty(void)
{
    int i;
    ArgsForEncryptWorker * args;
    
    for (i = 0; i < g_checkpoint_crypt_worker; i++)
    {
        args = g_crypt_worker_info[i];

        if (false == QueueIsFull(args->slot_queue))
        {
            return false;
        }
    }
    
    return true;
}


/* 
 * put buf_id into encrypt queue 
 */
List* mls_encrypt_buf_parellel(List * buf_id_list, int16 algo_id, int buf_id, int status)
{
    bool                  found;
    int                   worker_id;
    int                   slot_id;
    ArgsForEncryptWorker* args;
    BufEncryptElement     element;

    static int16          algo_id_keep  = TRANSP_CRYPT_INVALID_ALGORITHM_ID;
    static CryptKeyInfo   cryptkey_keep = NULL;

    /* 1. find one crypt worker */
    worker_id = mls_get_crypt_worker_id();

    args = g_crypt_worker_info[worker_id];

    /* 2. get slot for crypting */
    if (false == QueueGetSingle(args->slot_queue, &slot_id))
    {
        /* slot is sufficient, should not happen */
        abort();
    }

    /* 3. get cryptkey */
    if (algo_id == algo_id_keep && cryptkey_keep)
    {
        ; 
    }
    else
    {   
        found = crypt_key_info_hash_lookup(algo_id, &cryptkey_keep);
        if (false == found)
        {
            elog(ERROR, "algo_id:%d dose not exist, mls_encrypt_buf_parellel", algo_id);
        }
        
        /* cache it for next time using the same crypt key info */
        algo_id_keep = algo_id;
    }
    
    element.buf_id   = buf_id;
    element.algo_id  = algo_id;
    element.slot_id  = slot_id;
    element.status   = status;
    element.cryptkey = cryptkey_keep;

    /* 4. put it into queue */ 
    while (false == QueuePutSingle(args->encrypt_queue, (void*)&element))
    {
        /* do wait 100ms as ISO procedure, in fact, it would go through after getting workerid */
        pg_usleep(100000L);
    }

    /* 5. again, to fetch more crypted buf if exists */
    buf_id_list = mls_get_crypted_buflist(buf_id_list);
    
    return buf_id_list;
}

/*
 * create workers for crypting in parellel in checkpoint_main
 */
void mls_start_crypt_parellel_workers(void)
{
#define CONTEXT_NAME_LEN 64
    int i;
    int ret;    
    int slot_idx;
    char * tmp;
    MemoryContext oldctx;
    ArgsForEncryptWorker * args;

    if (g_crypt_parellel_main_running)
    {
        return;
    }

    g_crypt_parellel_main_running = true;

    oldctx = MemoryContextSwitchTo(TopMemoryContext);

    /* hold all infos of workers */
    g_crypt_worker_info = (ArgsForEncryptWorker **)palloc(sizeof(ArgsForEncryptWorker *) * g_checkpoint_crypt_worker);

    if (g_enable_crypt_check)
    {
        rel_crypt_init();
    }

    /* every worker has its own resource */
    for (i = 0; i < g_checkpoint_crypt_worker; i++)
    {
        args = palloc(sizeof(ArgsForEncryptWorker));
        args->worker_id     = i;
        args->crypted_cnt   = 0;
        args->encrypt_queue = palloc(sizeof(QueueData));
        tmp = palloc(sizeof(BufEncryptElement)* g_checkpoint_crypt_queue_length);
        QueueInit(args->encrypt_queue, g_checkpoint_crypt_queue_length,   sizeof(BufEncryptElement), tmp);

        args->crypted_queue = palloc(sizeof(QueueData));
        tmp = palloc(sizeof(BufCryptedElement)* g_checkpoint_crypt_queue_length);
        QueueInit(args->crypted_queue, g_checkpoint_crypt_queue_length,   sizeof(BufCryptedElement), tmp);

        args->slot_queue    = palloc(sizeof(QueueData));
        tmp = palloc(sizeof(int)* g_checkpoint_crypt_queue_length * CRYPT_SLOT_QUEUE_CNT_MULTI_FACTOR);
        QueueInit(args->slot_queue,    g_checkpoint_crypt_queue_length*CRYPT_SLOT_QUEUE_CNT_MULTI_FACTOR, sizeof(int), tmp);

        for (slot_idx = 0; slot_idx < g_checkpoint_crypt_queue_length*CRYPT_SLOT_QUEUE_CNT_MULTI_FACTOR - 1; slot_idx++)
        {
            /* init slot queue id */
            if (false == QueuePutSingle(args->slot_queue, &slot_idx))
            {
                abort();
            }
        }

        /*
         * alloc a continuous huge memory for crypting, then split them by slot_id.
         * every slot has 'CRYPT_ONE_BUF_NEED_TEMP_MULTI_FACTOR' = 2 block.
         * prepare 'CRYPT_SLOT_QUEUE_CNT_MULTI_FACTOR' = 3 times slots for use, in case encrypt and crypted queue are full at the same time. 
         */
        args->slot_pool = palloc(BLCKSZ 
                                  *CRYPT_SLOT_QUEUE_CNT_MULTI_FACTOR
                                  *CRYPT_ONE_BUF_NEED_TEMP_MULTI_FACTOR
                                  *g_checkpoint_crypt_queue_length);
     
        ret = CreateThread(mls_crypt_worker, (void *)args, MT_THR_DETACHED);
        if (ret != 0)
        {
            /* pause 1 second, in case too many output leads to side effect to disc load */
            pg_usleep(1000000L);
            
            /* failed to create thread, exit */
            ereport(FATAL,(errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("could not start encrypt worker:%d, checkpointer exits, try it later, ret:%d", i, ret)));
        }

        /* track the infos */
        g_crypt_worker_info[i] = args;
    }

    MemoryContextSwitchTo(oldctx);

    return;
}

void mls_crypt_parellel_main_exit(void)
{
    g_crypt_parellel_main_running = false;
    return;
}

uint32 mls_crypt_parle_get_queue_capacity(void)
{
    return g_checkpoint_crypt_queue_length*g_checkpoint_crypt_worker*2;
}

List * SyncBufidListAppend(List * buf_id_list, int buf_id, int status, int slot_id, int worker_id, char* bufToWrite)
{
    SyncBufIdInfo  * node;
    
    node = makeNode(SyncBufIdInfo);
    node->buf_id        = buf_id;
    node->slot_id       = slot_id;
    node->worker_id     = worker_id;
    node->status        = status;
    node->encrypted_buf = bufToWrite;
    buf_id_list = lappend(buf_id_list, node);

    return buf_id_list;
}

void mls_log_crypt_worker_detail(void)
{
    int i;   
    StringInfoData    ds;
    
    for(i = 0; i < g_checkpoint_crypt_worker; i++)
    {
        initStringInfo(&ds);

        appendStringInfo(&ds, "id:%d-bufs:%lu, ", i, g_crypt_worker_info[i]->crypted_cnt);
        
        elog(LOG, "detail for workers:%s", ds.data);
        
        pfree(ds.data);
    }
}

#endif


#if MARK("datamask")
void mls_check_datamask_need_passby(ScanState * scanstate, Oid relid)
{
    Oid parent_oid;

    if (InvalidOid == relid)
    {
        scanstate->ps.skip_data_mask_check = DATA_MASK_SKIP_ALL_TRUE;
    }
    
    parent_oid = mls_get_parent_oid_by_relid(relid);

    if (datamask_check_table_has_datamask(parent_oid))
    {
        if (false == dmask_chk_usr_and_col_in_whit_list(parent_oid, GetUserId(), -1))
        {
            scanstate->ps.skip_data_mask_check = DATA_MASK_SKIP_ALL_FALSE;
            return;
        }
    }

    scanstate->ps.skip_data_mask_check = DATA_MASK_SKIP_ALL_TRUE;
}
#endif

/*
 * cls and audit add several system tables, we manage them in this sample way.
 */
int transfer_rel_kind_ext(Oid relid)
{// #lizard forgives
    init_extension_table_oids();

    if(relid == g_mls_acl_namespace_oid ||
       relid == g_mls_acl_table_oid)
    {
        return RELKIND_MLS_SYS_TABLE;
    }

    switch (relid)
    {
        case ClsCompartmentRelationId:
        case ClsGroupRelationId:
        case ClsLabelRelationId:
        case ClsLevelRelationId:
        case ClsPolicyRelationId:
        case ClsTableRelationId:
        case ClsUserRelationId:
        case DataMaskMapRelationId:
        case DataMaskUserRelationId:    
        case TransparentCryptPolicyMapRelationId:
        case TransparentCryptPolicyAlgorithmId:
        case TransparentCryptPolicyTablespaceRelationId:
        case TransparentCryptPolicySchemaRelationId:
            return RELKIND_MLS_SYS_TABLE;
            break;
        case PgAuditObjDefOptsRelationId:
        case PgAuditObjConfRelationId:
        case PgAuditStmtConfRelationId:
        case PgAuditUserConfRelationId:
        case PgAuditFgaConfRelationId:
            return RELKIND_AUDIT_SYS_TABLE;
            break;
        default:
            break;
    }

    if (relid < FirstNormalObjectId)
    {
        return RELKIND_SYS_TABLE;
    }

    return RELKIND_NORMAL_TABLE;
}

/*
 * while, we define three role kind to extend orginal role kind, 
 * cls user in charge of security, audit user in charge audit event.
 */
int transfer_rol_kind_ext(Oid rolid)
{
    if(is_mls_user())
    {
        return ROLE_MLS_USER;
    }
    else if (DEFAULT_ROLE_AUDIT_SYS_USERID == rolid)
    {
        return ROLE_AUDIT_USER;
    }

    return ROLE_NORMAL_USER;
}

bool is_mls_or_audit_user(void)
{
    if ((DEFAULT_ROLE_MLS_SYS_USERID == GetAuthenticatedUserId()) 
        || (DEFAULT_ROLE_AUDIT_SYS_USERID == GetAuthenticatedUserId()))
    {
        return true;
    }
    return false;
}

Oid mls_get_parent_oid(Relation rel)
{
    return mls_get_parent_oid_by_relid(rel->rd_id);
}

int mls_check_schema_crypted(Oid schemaoid)
{
    int          algoid;
    Relation     rel;
    HeapScanDesc scandesc;
    HeapTuple     tuple;
    ScanKeyData  entry[1];

    algoid = TRANSP_CRYPT_INVALID_ALGORITHM_ID;

    if (InvalidOid == schemaoid)
    {
        return TRANSP_CRYPT_INVALID_ALGORITHM_ID;
    }
    
    rel = heap_open(TransparentCryptPolicySchemaRelationId, AccessShareLock);

    ScanKeyInit(&entry[0],
                Anum_pg_transparent_crypt_policy_schema_schemaoid,
                BTEqualStrategyNumber, 
                F_OIDEQ,
                ObjectIdGetDatum(schemaoid));
    
    scandesc = heap_beginscan_catalog(rel, 1, entry);
    tuple    = heap_getnext(scandesc, ForwardScanDirection);

    /* We assume that there can be at most one matching tuple */
    if (HeapTupleIsValid(tuple))
    {
        algoid = ((Form_pg_transparent_crypt_policy_schema) GETSTRUCT(tuple))->algorithm_id;
    }

    heap_endscan(scandesc);
    heap_close(rel, AccessShareLock);

    return algoid;
}

int mls_check_tablespc_crypted(Oid tablespcoid)
{
    int          algoid;
    Relation     rel;
    HeapScanDesc scandesc;
    HeapTuple     tuple;
    ScanKeyData  entry[1];

    algoid = TRANSP_CRYPT_INVALID_ALGORITHM_ID;

    rel = heap_open(TransparentCryptPolicyTablespaceRelationId, AccessShareLock);

    ScanKeyInit(&entry[0],
                Anum_pg_transparent_crypt_policy_spc_spcoid,
                BTEqualStrategyNumber, 
                F_OIDEQ,
                ObjectIdGetDatum(tablespcoid));
    
    scandesc = heap_beginscan_catalog(rel, 1, entry);
    tuple    = heap_getnext(scandesc, ForwardScanDirection);

    /* We assume that there can be at most one matching tuple */
    if (HeapTupleIsValid(tuple))
    {
        algoid = ((Form_pg_transparent_crypt_policy_tablespace) GETSTRUCT(tuple))->algorithm_id;
    }

    heap_endscan(scandesc);
    heap_close(rel, AccessShareLock);

    return algoid;
}

void InsertTrsprtCryptPolicyMapTuple(Relation pg_transp_crypt_map_desc,
                                                    Oid relnamespace,
                                                    Oid    reltablespace)
{// #lizard forgives
    Relation        rel_crypt_map;
    Form_pg_class   rd_rel;
    HeapTuple        tup;
    int             algoid;
    Oid             spaceoid;
    Datum            values[Natts_pg_transparent_crypt_policy_map];
    bool            nulls[Natts_pg_transparent_crypt_policy_map];
    NameData        schemaname;
    NameData        spcname;
    CatalogIndexState indstate;
    char            relkind;
    
    if (IsBootstrapProcessingMode())
    {
        return;
    }

    if (InvalidOid == relnamespace)
    {
        return;
    }

    if (InvalidOid == reltablespace)
    {
        spaceoid = DEFAULTTABLESPACE_OID;
    }
    else
    {
        spaceoid = reltablespace;
    }

    relkind = pg_transp_crypt_map_desc->rd_rel->relkind;
    if (RELKIND_VIEW == relkind || RELKIND_SEQUENCE == relkind)
    {
        /* skip recording view and seq */
        return;
    }
    
    algoid = mls_check_schema_crypted(relnamespace);
    if (TRANSP_CRYPT_INVALID_ALGORITHM_ID == algoid)
    {
        algoid = mls_check_tablespc_crypted(spaceoid);
        if (TRANSP_CRYPT_INVALID_ALGORITHM_ID == algoid)
        {
            return ;
        }
    }

    rd_rel = pg_transp_crypt_map_desc->rd_rel;

    memset(NameStr(schemaname), 0, NAMEDATALEN);
    strncpy(NameStr(schemaname), GetSchemaNameByOid(relnamespace), NAMEDATALEN);
    memset(NameStr(spcname),    0, NAMEDATALEN);
    strncpy(NameStr(spcname), get_tablespace_name(spaceoid), NAMEDATALEN);

    
    /*
     * open pg_transparent_crypt_policy_map and its indexes.
     */
    rel_crypt_map = heap_open(TransparentCryptPolicyMapRelationId, RowExclusiveLock);
    indstate      = CatalogOpenIndexes(rel_crypt_map);


    /* make a tuple and insert */
    memset(values, 0, sizeof(values));
    memset(nulls, false, sizeof(nulls));

    values[Anum_pg_transparent_crypt_policy_map_relid - 1]      = ObjectIdGetDatum(pg_transp_crypt_map_desc->rd_id);
    values[Anum_pg_transparent_crypt_policy_map_attnum - 1]     = ObjectIdGetDatum(REL_FILE_CRYPT_ATTR_NUM);
    values[Anum_pg_transparent_crypt_policy_map_algorithm - 1]  = ObjectIdGetDatum(algoid);
    values[Anum_pg_transparent_crypt_policy_map_spcoid - 1]     = ObjectIdGetDatum(spaceoid);
    values[Anum_pg_transparent_crypt_policy_map_schemaoid - 1]  = ObjectIdGetDatum(relnamespace);
    values[Anum_pg_transparent_crypt_policy_map_spcname - 1]    = NameGetDatum(&spcname);
    values[Anum_pg_transparent_crypt_policy_map_nspname - 1]    = NameGetDatum(&schemaname);
    values[Anum_pg_transparent_crypt_policy_map_tblname - 1]    = NameGetDatum(&(rd_rel->relname));

    tup = heap_form_tuple(RelationGetDescr(rel_crypt_map), values, nulls);

    /* finally insert the new tuple, update the indexes, and clean up */
    CatalogTupleInsert(rel_crypt_map, tup);

    heap_freetuple(tup);

    /* close relation */
    CatalogCloseIndexes(indstate);
    heap_close(rel_crypt_map, RowExclusiveLock);
    
    return;
}

/*
 * return parent oid if exists, or return itself
 */
Oid mls_get_parent_oid_by_relid(Oid relid)
{
    Oid      parent_oid = InvalidOid;
    Oid      tbl_oid;
    HeapTuple    tp;
    HeapTuple    tbl_tp  = NULL;
    
    if (!IS_SYSTEM_REL(relid))
    {  
        tbl_oid = relid;
        
        tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
        if (HeapTupleIsValid(tp))
        {
            Form_pg_class reltup = (Form_pg_class) GETSTRUCT(tp);

            /* convert index oid to relation oid */
            if (RELKIND_INDEX == reltup->relkind)
            {
                tbl_oid = IndexGetRelation(relid, false);
                tbl_tp  = SearchSysCache1(RELOID, ObjectIdGetDatum(tbl_oid));
                if (g_allow_force_ddl && !tbl_tp)
                {
                    ReleaseSysCache(tp);
                    return InvalidOid;
                }
                reltup  = (Form_pg_class)GETSTRUCT(tbl_tp);
            }
            
            /* partition internal */
            if (RELPARTKIND_CHILD == reltup->relpartkind)
            {
                /* if child, return parent */
                parent_oid = reltup->relparent;
            }
            else 
            {
                /*
                 * 1. RELPARTKIND_PARENT == reltup->relpartkind
                 * 2. (true == rel->rd_rel->relispartition)
                 * 3. (RELKIND_PARTITIONED_TABLE == rel->rd_rel->relkind)
                 * 4. normal relation
                 * return itself
                 */
                parent_oid = tbl_oid;
            }

            if (HeapTupleIsValid(tbl_tp))
            {
                ReleaseSysCache(tbl_tp);
            }
            
            ReleaseSysCache(tp);
        }
    }

    return parent_oid;
}

/*
 * focus on datatype supported currently, if new datatype added, this is the enterance function on critical path
 */
bool mls_support_data_type(Oid typid)
{// #lizard forgives
    switch(typid)
    {
        case TIMESTAMPOID:
        case FLOAT4OID:
        case FLOAT8OID:
        case NUMERICOID:
        case INT2OID:
        case INT4OID:
        case INT8OID:
            
        case BPCHAROID:     /* char */
        case VARCHAR2OID:        
        case VARCHAROID:
        case TEXTOID:
        case BYTEAOID:
        
            return true;
            break;
        default:
            break;
    }
    return false;
}



/*
 * all relative row level control feature entrance, such as datamask, cls, transparent crypt.
 */
void MlsExecCheck(ScanState *node, TupleTableSlot *slot)
{// #lizard forgives
    Oid parent_oid = InvalidOid;
    
    if (g_enable_cls || g_enable_data_mask || g_enable_transparent_crypt)
    {
        if (node)
        {
            if (node->ss_currentRelation)
            {
                if (node->ss_currentRelation->rd_att)
                {
                    /* 
                     * entrance: transparent crypt 
                     */
                    if (g_enable_transparent_crypt)
                    {
                        if (node->ss_currentRelation->rd_att->transp_crypt )
                       //     || (TransparentCryptPolicyAlgorithmId == RelationGetRelid(node->ss_currentRelation)))
                        {
                            parent_oid = mls_get_parent_oid(node->ss_currentRelation);
                            trsprt_crypt_dcrpt_all_col_vale(node, slot, parent_oid);
                        }
                    }

                    /* 
                     * entrance: datamask 
                     */
                    if (g_enable_data_mask)
                    {
                        if (node->ss_currentRelation->rd_att->tdatamask)
                        {
                            if(node->ss_currentMaskDesc == NULL)
                            {
                                parent_oid = mls_get_parent_oid(node->ss_currentRelation);
                                node->ss_currentMaskDesc = init_datamask_desc(parent_oid,
                                                                              slot->tts_tupleDescriptor->attrs,
                                                                              node->ss_currentRelation->rd_att->tdatamask);
                            }

                            /* 
                             * skip_data_mask_check is assigned in execinitnode, 
                             * so concurrent changing to datamask has not effect on current select 
                             */
                            if (DATA_MASK_SKIP_ALL_TRUE != node->ps.skip_data_mask_check)
                                datamask_exchange_all_cols_value((Node *) node, slot);
                        }
                    }
                }
            }
        }
    }
    return;
}

/*
 * return children part list if exist, including interval and original partitions
 * NOTE, the parent relation is not included in returning list, so if this is a normal relation, NIL is returned.
 */
List * FetchAllParitionList(Oid relid)
{
    char     relkind;
    Relation rel;
    List    *children = NIL;
    /* in case the relation has already been eliminated */
    rel = try_relation_open(relid, NoLock);
    if (NULL == rel)
    {
        return NIL;
    }

    /* this is interval partition */
    if (RELATION_IS_INTERVAL(rel))
    {
        children = RelationGetAllPartitions(rel);
    }

    relkind = rel->rd_rel->relkind ;

    relation_close(rel, NoLock);

    /* treat partition tables */
    if (NIL == children && RELKIND_PARTITIONED_TABLE == relkind )
    {
        children = find_all_inheritors(relid, AccessShareLock, NULL);
    }

    return children;
}

/*
 * this function cover two kinds of partition, interval and original partition
 */
void CacheInvalidateRelcacheAllPatition(Oid databaseid, Oid relid)
{
    List    *children = NIL;

    children = FetchAllParitionList(relid);

    if (NIL != children)
    {
        ListCell *    lc;
        Oid         partoid;

        foreach(lc, children)
        {
            partoid = lfirst_oid(lc);
            MlsRegisterRelcacheInvalidation(databaseid, partoid);
        }
    }

    return;
}

/*
 * hide sensitive info from query string.
 *
 * we cut off from the first '(', cause, infos like password must appear in (clause_stmt).
 *
 * new querystring is allocateing from errorcontext, which would be reset for every query, 
 * so need to worry about memory leak.
 */
const char * mls_query_string_prune(const char * querystring)
{
    MemoryContext old_memctx;
    char *        string_prune = NULL;
    char *        string_delimeter;
    int           string_len;
    
    if (querystring)
    {
        string_delimeter = strchr(querystring, MLS_QUERY_STRING_PRUNE_DELIMETER);

        if (NULL == string_delimeter)
        {
            return querystring;
        }

        string_len = string_delimeter - querystring;
        
        old_memctx = MemoryContextSwitchTo(ErrorContext);
        string_prune = palloc(string_len + 1);
        MemoryContextSwitchTo(old_memctx);
        
        memcpy(string_prune, querystring, string_len);

        string_prune[string_len] = '\0';
    }
    
    return string_prune;
}

void MlsShmemInit(void)
{   
    cyprt_key_info_hash_init();
    rel_cyprt_hash_init();

    if (IsBootstrapProcessingMode())
    {
        return;
    }

    /* for vfd access */
    MlsInitFileAccess();
    
    crypt_key_info_load_mapfile();
    elog(LOG, "start rel crypt load mapfile");
    rel_crypt_load_mapfile();
	elog(LOG, "end rel crypt load mapfile");

    /* after vfd access, rollback all init actions */
    MlsCleanFileAccess();
        
    return;
}

Size MlsShmemSize(void)
{
    return rel_crypt_hash_shmem_size() + crypt_key_info_hash_shmem_size();
}

void init_extension_table_oids(void)
{
    if (g_mls_acl_table_oid == InvalidOid) {
        Oid nsid ;
        if(OidIsValid(nsid = get_namespace_oid(MLS_EXTENSION_NAMESPACE_NAME,true)))
        {
            g_mls_acl_table_oid     = get_relname_relid(MLS_RELATION_ACL_NAME,nsid);
            g_mls_acl_namespace_oid = get_relname_relid(MLS_SCHEMA_ACL_NAME,nsid);
        }
    }
}

/*
 * check if current user has mls permission for target_relid
 */
bool check_user_has_acl_for_relation(Oid target_relid) {
    Oid nsid;
    Oid relid;
    List *index_list = NULL;
    Relation table;
    Relation index;
    IndexScanDesc scan;
    ScanKeyData skey[2];
    HeapTuple tuple    = NULL;
    bool user_has_acl = false;

    if(!OidIsValid(nsid = get_namespace_oid(MLS_EXTENSION_NAMESPACE_NAME, true)) ||
       !OidIsValid(relid = get_relname_relid(MLS_RELATION_ACL_NAME, nsid)))
        return false;

    table = heap_open(relid, AccessShareLock);
    index_list = RelationGetIndexList(table);

    Assert(1 == list_length(index_list));

    index = index_open(lfirst_oid(list_head(index_list)),AccessShareLock);

    ScanKeyInit(&(skey[0]),
                1,
                BTEqualStrategyNumber, F_OIDEQ,
                GetUserId());

    ScanKeyInit(&(skey[1]),
                2,
                BTEqualStrategyNumber, F_OIDEQ,
                target_relid);

    /* Scan in the table  */
    scan = index_beginscan(table, index, GetLocalTransactionSnapshot(), 2, 0);
    index_rescan(scan,skey,2,NULL,0);

    if((tuple = index_getnext(scan, ForwardScanDirection)) != NULL)
    {
        TupleDesc tup_desc PG_USED_FOR_ASSERTS_ONLY = RelationGetDescr(table);
        bool tuple_isnull  PG_USED_FOR_ASSERTS_ONLY = true;

        Assert(GetUserId()   == DatumGetObjectId(fastgetattr(tuple, 1, tup_desc, &tuple_isnull)));
        Assert(target_relid  == DatumGetObjectId(fastgetattr(tuple, 2, tup_desc, &tuple_isnull)));
        user_has_acl = true;
    }

    index_endscan(scan);
    index_close(index, AccessShareLock);
    relation_close(table, AccessShareLock);

    return user_has_acl;
}

/*
 * check if current user has mls permission for target_namespace
 */
bool check_user_has_acl_for_namespace(Oid target_namespace) {
    Oid nsid;
    Oid relid;
    List *index_list = NULL;
    Relation table;
    Relation index;
    IndexScanDesc scan;
    ScanKeyData skey[2];
    HeapTuple tuple    = NULL;
    bool user_has_acl = false;

    if(!OidIsValid(nsid = get_namespace_oid(MLS_EXTENSION_NAMESPACE_NAME, true)) ||
       !OidIsValid(relid = get_relname_relid(MLS_SCHEMA_ACL_NAME, nsid)))
        return false;

    table = heap_open(relid, AccessShareLock);
    index_list = RelationGetIndexList(table);

    Assert(1 == list_length(index_list));

    index = index_open(lfirst_oid(list_head(index_list)),AccessShareLock);

    ScanKeyInit(&(skey[0]),
                1,
                BTEqualStrategyNumber, F_OIDEQ,
                GetUserId());

    ScanKeyInit(&(skey[1]),
                2,
                BTEqualStrategyNumber, F_OIDEQ,
                target_namespace);

    /* Scan in the table  */
    scan = index_beginscan(table, index, GetLocalTransactionSnapshot(), 2, 0);
    index_rescan(scan,skey,2,NULL,0);

    if((tuple = index_getnext(scan, ForwardScanDirection)) != NULL)
    {
        TupleDesc tup_desc  PG_USED_FOR_ASSERTS_ONLY = RelationGetDescr(table);
        bool tuple_isnull  PG_USED_FOR_ASSERTS_ONLY  = true;

        Assert(GetUserId()   == DatumGetObjectId(fastgetattr(tuple, 1, tup_desc, &tuple_isnull)));
        Assert(target_namespace  == DatumGetObjectId(fastgetattr(tuple, 2, tup_desc, &tuple_isnull)));
        user_has_acl = true;
    }

    index_endscan(scan);
    index_close(index, AccessShareLock);
    relation_close(table, AccessShareLock);

    return user_has_acl;
}

/*
 * check if user has the permission for the mls object
 */
void CheckMlsTableUserAcl(ResultRelInfo *resultRelInfo, HeapTuple tuple)
{// #lizard forgives
    Relation rel = resultRelInfo->ri_RelationDesc;
    TupleDesc tupdesc = RelationGetDescr(rel);
    Oid relid = RelationGetRelid(rel);
    Oid target_oid;
    int num_attr = 0;
    bool is_null;
    bool is_relation = false;

    check_tbase_mls_extension();

    if (is_mls_root_user())
        return ;

    if (relid == g_mls_acl_table_oid)
    {
        num_attr = Anum_pg_mls_relation_acl_relid;
        is_relation = true;
    }
    else if (relid == g_mls_acl_namespace_oid)
    {
        num_attr = Anum_pg_mls_schema_acl_relid;
    }
    else {
        switch (relid) {
            case DataMaskMapRelationId:
                num_attr = Anum_pg_data_mask_map_relid;
                is_relation = true;
                break;
            case DataMaskUserRelationId:
                num_attr = Anum_pg_data_mask_user_relid;
                is_relation = true;
                break;
            case TransparentCryptPolicyMapRelationId:
                num_attr = Anum_pg_transparent_crypt_policy_map_relid;
                is_relation = true;
                break;
            case TransparentCryptPolicySchemaRelationId:
                num_attr = Anum_pg_transparent_crypt_policy_schema_schemaoid;
                is_relation = false;
                break;
            default:
                return ;
        }
    }

    target_oid = DatumGetObjectId(heap_getattr(tuple,num_attr,tupdesc,&is_null));
    if(!OidIsValid(target_oid))
    {
        ereport(ERROR,
                (errcode(ERRCODE_DATA_EXCEPTION),
                        errmsg("Extract oid from mls tuple failed "),
                        errdetail("Oid is not valid")));
    }

    if(is_relation)
    {
       Relation target_relation;

       if(check_user_has_acl_for_relation(target_oid))
           return ;
    
       target_relation = RelationIdGetRelation(target_oid);
       if(!RelationIsValid(target_relation))
           ereport(ERROR,
                   (errcode(ERRCODE_DATA_EXCEPTION),
                           errmsg("could not find relation with oid %u",
                                  target_oid)));

       target_oid = RelationGetNamespace(target_relation);
    }

    if(check_user_has_acl_for_namespace(target_oid))
       return ;

    ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("No mls permission"),
                    errdetail("No mls permission for table or schema.")));
}

/*
 * check if tbase_mls extension is installed
 */
void check_tbase_mls_extension(void)
{
    Oid extOid = InvalidOid;

    extOid = get_extension_oid(MLS_EXTENSION_NAME, true);

    if (!OidIsValid(extOid))
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("This operation is not allowed until the extension \"%s\" is installed.",
                               MLS_EXTENSION_NAME)));
}

