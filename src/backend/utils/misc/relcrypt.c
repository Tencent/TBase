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
 * relcrypt.c
 *
 *
 * Portions Copyright (c) 2018, Tbase Global Development Group
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *    src/backend/utils/misc/relcrypt.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "postgres_ext.h"
#include "access/genam.h"
#include "access/htup_details.h"
#include "access/xlogreader.h"

#include "utils/relcache.h"
#include "access/htup.h"
#include "access/tupdesc.h"
#include "fmgr.h"
#include "catalog/pg_attribute.h"

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
#include "contrib/pgcrypto/pgp.h"
#include "contrib/sm/sm4.h"

#include "miscadmin.h"


#include "utils/syscache.h"
#include "utils/builtins.h"
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


#include "utils/mls.h"


#include "utils/relcrypt.h"

#include "utils/relcryptmap.h"
#include "utils/relcryptcache.h"
#include "utils/relcryptmisc.h"
#include "access/relcryptaccess.h"
#include "commands/relcryptcommand.h"
#include "storage/relcryptstorage.h"


void print_page_header(PageHeader header);


#if MARK("crypt key info")

typedef struct tagCryptKeyUdf
{
    Oid     encrypt_oid;
    Oid     decrypt_oid;
    int     encrypt_prosrc_offset;
    int     encrypt_probin_offset;
    int     decrypt_prosrc_offset;
    int     decrypt_probin_offset;
}CryptKeyUdf;

typedef struct tagCryptKeyNonSym
{
    int     encrypt_offset;
    int     decrypt_offset;
}CryptKeyNonSym;



//#if MARK("for process execept backend, such checkpoint")
typedef text* (*CryptUdfCallbackFunc) (text *, text*);
//static HTAB *g_crypt_udf_local_hash = NULL;
char ** g_crypt_check_array         = NULL;


typedef struct tagCryptUdfLocalHashEntry
{
    Oid                  funcoid;
#if 1
    PGFunction           udf_func;
#else
    CryptUdfCallbackFunc udf_func;
#endif
}CryptUdfLocalHashEntry;


#define CRYPT_OPTION_VALID(_option) (CRYPT_KEY_INFO_OPTION_SYMKEY == (_option) \
                                    || CRYPT_KEY_INFO_OPTION_ANYKEY == (_option)\
                                    || CRYPT_KEY_INFO_OPTION_UDF == (_option)\
                                    || CRYPT_KEY_INFO_OPTION_SM4 == (option))

#define PAGE_ENCRYPT_LEN                        (BLCKSZ - sizeof(PageHeaderData))

#define DECRYPT_CONTEXT_LENGTH_VALID(_len) (INVALID_CONTEXT_LENGTH != (_len))

static void crypt_key_info_udf_get_prosrc_probin(Oid functionId, char * prosrc_ret, char * probin_ret);
static CryptKeyInfo crypt_key_info_alloc_and_fill(AlgoId algo_id);
//static text * crypt_key_info_get_dearmor_key(char* key);
static CryptKeyInfo crypt_key_info_fill_struct_inner(HeapTuple htup, TupleDesc tupledesc);
static void crypt_key_info_load_by_algoid(int16 algo_id);


static CryptKeyInfo crypt_key_info_fill_struct_inner(HeapTuple htup, TupleDesc tupledesc)
{// #lizard forgives
    Form_pg_transparent_crypt_policy_algorithm form_transp_crypt_algo;
    bool    attr_isnull;
    Datum   passwd_datum;
    Datum   option_args_datum;
    Datum   pubkey_datum;
    Datum   privatekey_datum;
    text  * textval;
    int     option;
    CryptKeyInfo cryptkey;
    
    form_transp_crypt_algo = (Form_pg_transparent_crypt_policy_algorithm) GETSTRUCT(htup);
    option = form_transp_crypt_algo->option;

    /* first, alloc */
    cryptkey = crypt_key_info_alloc(option);    

    cryptkey->algo_id     = form_transp_crypt_algo->algorithm_id;
    cryptkey->option      = option;
    
    if (CRYPT_OPTION_VALID(option))
    {
        passwd_datum = heap_getattr(htup,
                                   Anum_pg_transparent_crypt_policy_algorithm_passwd,
                                   tupledesc,
                                   &attr_isnull);
        if (false == attr_isnull)
        {
            textval = DatumGetTextPCopy(passwd_datum);
            if (VARSIZE_ANY(textval) <= CRYPT_KEY_INFO_MAX_PASSWORD_LEN)
            {
                memcpy(cryptkey->password, textval, VARSIZE_ANY(textval));
            }
            else
            {
                elog(ERROR, "password is over length");
            }
            //default_private_key_dearmor_bytea = transparent_crypt_get_dearmor_key(transparent_crypt_get_private_key(), memcxt);
               
            //transp_crypt->password = decrypt_internal(1, 1, transp_crypt->password, default_private_key_dearmor_bytea, NULL, NULL);
        }

        option_args_datum = heap_getattr(htup,
                                   Anum_pg_transparent_crypt_policy_algorithm_option_args,
                                   tupledesc,
                                   &attr_isnull);
        if (false == attr_isnull)
        {
            textval = DatumGetTextPCopy(option_args_datum);
            if (VARSIZE_ANY(textval) <= CRYPT_KEY_INFO_MAX_OPT_ARGS_LEN)
            {
                memcpy(cryptkey->option_args, textval, VARSIZE_ANY(textval));
            }
            else
            {
                elog(ERROR, "option args string is over length");
            }
        }
        
        if (CRYPT_KEY_INFO_OPTION_UDF == option)
        {
            if (cryptkey->udf)
            {
                cryptkey->udf->encrypt_oid = form_transp_crypt_algo->encrypt_oid;
                cryptkey->udf->decrypt_oid = form_transp_crypt_algo->decrypt_oid;
            }
        }

        if (CRYPT_KEY_INFO_OPTION_ANYKEY == option || CRYPT_KEY_INFO_OPTION_UDF == option)
        {
            if (cryptkey->keypair)
            {
                pubkey_datum = heap_getattr(htup,
                                           Anum_pg_transparent_crypt_policy_algorithm_pubkey,
                                           tupledesc,
                                           &attr_isnull);
                if (false == attr_isnull)
                {
                    textval = DatumGetTextPCopy(pubkey_datum);
                    if (VARSIZE_ANY(textval) <= CRYPT_KEY_INFO_MAX_PUBKEY_LEN)
                    {
                        memcpy(cryptkey->keypair->publickey, textval, VARSIZE_ANY(textval));
                    }
                    else
                    {
                        elog(ERROR, "public key is over length");
                    }
                }

                privatekey_datum = heap_getattr(htup,
                                           Anum_pg_transparent_crypt_policy_algorithm_prikey,
                                           tupledesc,
                                           &attr_isnull);
                if (false == attr_isnull)
                {
                    textval = DatumGetTextPCopy(privatekey_datum);
                    if (VARSIZE_ANY(textval) <= CRYPT_KEY_INFO_MAX_PRIKEY_LEN)
                    {
                        memcpy(cryptkey->keypair->privatekey, textval, VARSIZE_ANY(textval));
                    }
                    else
                    {
                        elog(ERROR, "private key is over length");
                    }
                }
            }
        }

        /* accumulate size */
        cryptkey->keysize = crypt_key_info_cal_key_size(cryptkey);
        
    }
    else
    {
        elog(ERROR, "invalid option:%d in pg_transparent_crypt_policy_algorithm", option);
    }

    return cryptkey;
}

static CryptKeyInfo crypt_key_info_alloc_and_fill(AlgoId algo_id)
{
    SysScanDesc scan;
    ScanKeyData skey[1];
    HeapTuple   htup;
    Relation    rel;
    CryptKeyInfo cryptkey = NULL;
    
    ScanKeyInit(&skey[0],
                        Anum_pg_transparent_crypt_policy_algorithm_id,
                        BTEqualStrategyNumber, 
                        F_OIDEQ,
                        Int16GetDatum(algo_id));

    rel  = heap_open(TransparentCryptPolicyAlgorithmId, AccessShareLock);
    scan = systable_beginscan(rel, 
                              PgTransparentCryptPolicyAlgorithmIndexId, 
                              true,
                              NULL, 
                              1, 
                              skey);

    if (HeapTupleIsValid(htup = systable_getnext(scan)))
    {
        cryptkey = crypt_key_info_fill_struct_inner(htup, rel->rd_att);
    }

    systable_endscan(scan);
    heap_close(rel, AccessShareLock);

    return cryptkey;
}


static void crypt_key_info_udf_get_prosrc_probin(Oid functionId, char * prosrc_ret, char * probin_ret)
{
    HeapTuple    procedureTuple;
    Datum        prosrcdatum;
    Datum        probindatum;
    bool        isnull;
    char *      prosrc;
    char *      probin;

    procedureTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(functionId));
    if (!HeapTupleIsValid(procedureTuple))
    {
        elog(ERROR, "cache lookup failed for function %u", functionId);
    }

    prosrcdatum = SysCacheGetAttr(PROCOID, 
                                  procedureTuple,
                                  Anum_pg_proc_prosrc, 
                                  &isnull);
    if (false == isnull)
    {
        prosrc = TextDatumGetCString(prosrcdatum);
        memcpy(prosrc_ret, prosrc, strnlen(prosrc, CRYPT_KEY_INFO_PROC_SRC_LEN));
        pfree(prosrc);
    }
    else
    {
        elog(ERROR, "prosrc of funcionid:%d is NULL", functionId);
    }
    probindatum = SysCacheGetAttr(PROCOID, 
                                  procedureTuple,
                                  Anum_pg_proc_probin, 
                                  &isnull);
    if (false == isnull)
    {
        probin = TextDatumGetCString(probindatum);
        memcpy(probin_ret, probin, strnlen(probin, CRYPT_KEY_INFO_PROC_BIN_LEN));
        pfree(probin);
    }
    else
    {
        elog(ERROR, "probin of funcionid:%d is NULL", functionId);
    }

    ReleaseSysCache(procedureTuple);

    return;
}


/*
 * load crypt key into shmem hash by algoid, so it will check wether the algo_id already exists
 */
static void crypt_key_info_load_by_algoid(int16 algo_id)
{// #lizard forgives
    bool              found;
    CryptKeyInfo      cryptkey = NULL;
    CryptKeyInfoUDF * udf      = NULL;
    /*
     * FIRST, look up in shmem hash
     */
    found = crypt_key_info_hash_lookup(algo_id, NULL);
    if (true == found)
    {
        return;
    }

    /* 
     * THEN, not found, prepare cryptkeyinfo
     */

    /* special treat for default inner algo */
    if (CRYPT_DEFAULT_INNER_ALGO_ID == algo_id)
    {
        if (useLocalXid)
        {
            if (g_enable_crypt_debug)                
            {
                elog(LOG, "CRYPT_KEY_INSERT, crypt_key_info_load_by_algoid, algo_id:%d, useLocalXid:%d", 
                    CRYPT_DEFAULT_INNER_ALGO_ID, useLocalXid);
            }
            crypt_key_info_load_default_key();
            return;
        }

        elog(ERROR, "default algo should be loaded durning postmaster startup");
        return;
    }

    cryptkey = crypt_key_info_alloc_and_fill(algo_id);

    if (NULL != cryptkey)
    {
        /* special treat for udf */
        if (CRYPT_KEY_INFO_OPTION_UDF == cryptkey->option)
        {
            if (NULL != cryptkey->udf)
            {
                udf = cryptkey->udf;
                /*collect func callback */
                crypt_key_info_udf_get_prosrc_probin(udf->encrypt_oid, 
                                                     udf->encrypt_prosrc, udf->encrypt_probin);
                udf->encrypt_func = load_external_function(udf->encrypt_probin, 
                                                            udf->encrypt_prosrc,
                                                            true, 
                                                            NULL);
                
                crypt_key_info_udf_get_prosrc_probin(udf->decrypt_oid, 
                                                     udf->decrypt_prosrc, udf->decrypt_probin);
                udf->decrypt_func = load_external_function(udf->decrypt_probin, 
                                                            udf->decrypt_prosrc,
                                                            true, 
                                                            NULL);
            }
            else
            {
                elog(ERROR, "get invalid cryptkey->udf return");
            }
        }
        else
        {
            /* others skip */
        }

        /*
         * LAST, insert into shmem hash for next round lookup.
         */
        if (g_enable_crypt_debug)                
        {
            elog(LOG, "CRYPT_KEY_INSERT, crypt_key_info_load_by_algoid, algo_id:%d, option:%d, size:%d, option_args:%s", 
                cryptkey->algo_id, cryptkey->option, cryptkey->keysize, VARDATA_ANY(cryptkey->option_args));
        }
        crypt_key_info_hash_insert(cryptkey, true, false);

        crypt_key_info_free(cryptkey);
    }
    else
    {
        elog(ERROR, "get invalid cryptkey return");
    }

    return ;
}


void crypt_key_info_load_by_tuple(HeapTuple heaptuple, TupleDesc tupledesc)
{
    CryptKeyInfo cryptkey;

    cryptkey = crypt_key_info_fill_struct_inner(heaptuple, tupledesc);

    if (g_enable_crypt_debug)                
    {
        elog(LOG, "CRYPT_KEY_INSERT, crypt_key_info_load_by_tuple, algo_id:%d, option:%d, size:%d, option_args:%s", 
            cryptkey->algo_id, cryptkey->option, cryptkey->keysize, VARDATA_ANY(cryptkey->option_args));
    }
    crypt_key_info_hash_insert(cryptkey, true, false);

    crypt_key_info_free(cryptkey);
    
    return;
}



#if 0 //MARK("for udf crypt")
#define CRTPT_UDF_LOCAL_HASH_SIZE               (256)

static int crypt_udf_local_hash_cmp (const void *key1, const void *key2, Size keysize);
static PGFunction  crypt_udf_lookup(Oid func_id, const char * probin, const char * prosrc);
static CryptUdfLocalHashEntry *  crypt_udf_local_hash_lookup(Oid fn_oid);
static void crypt_udf_local_hash_insert(Oid fn_oid, PGFunction udf_func);


static int crypt_udf_local_hash_cmp (const void *key1, const void *key2, Size keysize)
{
    const Oid *tagPtr1 = key1, *tagPtr2 = key2;

    if (*tagPtr1 == *tagPtr2)
    {
        return 0;
    }
    
    return 1;
}

static PGFunction  crypt_udf_lookup(Oid func_id, const char * probin, const char * prosrc)
{
    CryptUdfLocalHashEntry * entry;
    PGFunction     udf_func;
    
    entry = crypt_udf_local_hash_lookup(func_id);
    if (NULL != entry)
    {
        return entry->udf_func;
    }

    udf_func = (PGFunction)load_external_function(probin, 
                                                        prosrc,
                                                        true, 
                                                        NULL);

    crypt_udf_local_hash_insert(func_id, udf_func);

    return udf_func;
}

static CryptUdfLocalHashEntry *  crypt_udf_local_hash_lookup(Oid fn_oid)
{
    CryptUdfLocalHashEntry *entry = NULL;

    if (NULL == g_crypt_udf_local_hash)
    {
        return entry;
    }

    entry = (CryptUdfLocalHashEntry *)hash_search(g_crypt_udf_local_hash,
                                                &fn_oid,
                                                HASH_FIND, 
                                                NULL);

    return entry;
}

static void crypt_udf_local_hash_insert(Oid fn_oid, PGFunction udf_func)
{
    CryptUdfLocalHashEntry *entry;
    bool                    found = false;

    /* Create the hash table if it doesn't exist yet */
    if (g_crypt_udf_local_hash == NULL)
    {
        HASHCTL        hash_ctl;

        MemSet(&hash_ctl, 0, sizeof(hash_ctl));
        hash_ctl.keysize   = sizeof(Oid);
        hash_ctl.entrysize = sizeof(CryptUdfLocalHashEntry);
        hash_ctl.match     = crypt_udf_local_hash_cmp;
        g_crypt_udf_local_hash = hash_create("mls udf local hash",
                                CRTPT_UDF_LOCAL_HASH_SIZE,
                                &hash_ctl,
                                HASH_ELEM | HASH_BLOBS| HASH_COMPARE);
    }

    entry = (CryptUdfLocalHashEntry * )hash_search(g_crypt_udf_local_hash,
                                        &fn_oid,
                                        HASH_ENTER,
                                        &found);
    if (false == found)
    {
        /* OID is already filled in */
        entry->udf_func = udf_func;
    }
    
    return;
}

#endif


#endif

#if MARK("rel crypt")
static void rel_crypt_create(RelFileNode * rnode, AlgoId algo_id, bool wal_write);
static void rel_crypt_create_one_relation(Oid relid, int16 algo_id);
static Oid rel_crypt_get_table_oid(Relation rel);
static text * encrypt_procedure_inner(CryptKeyInfo cryptkey_local, text * text_src, char * page_new_output);
static void crypt_check(int16 algo_id, text * text_src, text * text_crypted, int length, int workerid);

static void rel_crypt_create(RelFileNode * rnode, AlgoId algo_id, bool wal_write)
{
    bool     found;
    
    found = rel_crypt_hash_lookup(rnode, NULL);
    if (true == found)
    {
        return;
    }

    /* if rnode is not found, it would be insert into relcrypt hash, while, we check algo_id is ready. */
    crypt_key_info_load_by_algoid(algo_id);

    if (g_enable_crypt_debug)                
    {
        elog(LOG, "REL_CRYPT_INSERT, rel_crypt_create, relfilenode:%d:%d:%d, algo_id:%d", 
                        rnode->dbNode, rnode->spcNode, rnode->relNode,
                        algo_id);
    }
    rel_crypt_hash_insert(rnode, algo_id, wal_write, false);
    
    //elog(DEBUG, "pid:%d create relfilenode:%d:%d:%d", getpid(), rnode->spcNode, rnode->dbNode, rnode->relNode);

    return;
}

void rel_crypt_struct_init(RelCrypt relcrypt)
{
    if (NULL != relcrypt)
    {
        relcrypt->algo_id = TRANSP_CRYPT_INVALID_ALGORITHM_ID;
    }
    
    return;
}

/*
 * check relation having crypt policy and return algoid if exists
 */
static int16  rel_crypt_get_relation_algoid(Oid relid)
{
    //RelCrypt     relcrypt;
    SysScanDesc  scan;
    ScanKeyData  skey[2];
    HeapTuple    htup;
    Relation     rel;
    bool         found;
    int16        algo_id;
    Oid          parent_oid;

    parent_oid = mls_get_parent_oid_by_relid(relid);

    if (TransparentCryptPolicyAlgorithmId == parent_oid)
    {
        elog(ERROR, "rel crypt for algo table shoule be loaded when postmaster start up");
        return CRYPT_DEFAULT_INNER_ALGO_ID;
    }

    if (!IS_SYSTEM_REL(parent_oid))
    {    
        found = false;
        ScanKeyInit(&skey[0],
                        Anum_pg_transparent_crypt_policy_map_relid,
                        BTEqualStrategyNumber, 
                        F_OIDEQ,
                        ObjectIdGetDatum(parent_oid));
        ScanKeyInit(&skey[1],
                        Anum_pg_transparent_crypt_policy_map_attnum,
                        BTEqualStrategyNumber, 
                        F_INT2EQ,
                        Int16GetDatum(REL_FILE_CRYPT_ATTR_NUM));

        rel = relation_open(TransparentCryptPolicyMapRelationId, AccessShareLock);
        scan = systable_beginscan(rel, 
                                  PgTransparentCryptPolicyMapIndexId, 
                                  true,
                                  NULL, 
                                  2, 
                                  skey);
        
        if (HeapTupleIsValid(htup = systable_getnext(scan)))
        {
            Form_pg_transparent_crypt_policy_map form_trans_crypt = (Form_pg_transparent_crypt_policy_map) GETSTRUCT(htup);
            algo_id = form_trans_crypt->algorithm_id;
            found   = true;
        }
        
        systable_endscan(scan);
        relation_close(rel, AccessShareLock);

        if (found)
        {
            return algo_id;
        }
    }
    
    return TRANSP_CRYPT_INVALID_ALGORITHM_ID;
}

static Oid rel_crypt_get_table_oid(Relation rel)
{
    if (RELKIND_INDEX == rel->rd_rel->relkind)
    {
        return IndexGetRelation(rel->rd_id, true);       
    }
    else if (RELKIND_RELATION == rel->rd_rel->relkind)
    {
        return rel->rd_id;
    }

    return InvalidOid;
}

/*
 * check heap table having crypt, and insert index rnode into shmem hash.
 */
void rel_crypt_index_check_policy(Oid heap_oid, Relation indexrel)
{
    Relation rel;
    int16    algo_id;
    Oid      table_oid;

    if (IS_SYSTEM_REL(heap_oid))
    {
        return;
    }

    /* try locking this relation to keep consistency in case dropping concurrently */
    rel = try_relation_open(heap_oid, AccessShareLock);
    if (NULL == rel)
    {
        return;
    }

    /* get table oid */
    table_oid = rel_crypt_get_table_oid(rel);
    if (InvalidOid == table_oid)
    {
        relation_close(rel, AccessShareLock);    
        return;
    }

    /* looking up wether crypt was binding or not */
    algo_id = rel_crypt_get_relation_algoid(table_oid);
    if (TRANSP_CRYPT_ALGO_ID_IS_VALID(algo_id))
    {
        rel_crypt_create(&(indexrel->rd_node), algo_id, true);
    }
    else
    {   
        /* 
         * also, need to check its heap wether bound.
         * for example, an internal partition table was created, then, the schema was bound with cryptition,
         * later, new parition was add, we should make sure new partition and its indexes crypted.
         * in this scene, parent oid was not bound with crypt, so need to check the table of index.
         */
        RelCryptEntry relcrypt;
        if (true == rel_crypt_hash_lookup(&(rel->rd_node), &relcrypt))
        {
            rel_crypt_create(&(indexrel->rd_node), relcrypt.algo_id, true);
        }
    }

    relation_close(rel, AccessShareLock);
    return;
}

void rel_crypt_relat_chk_plcy_wth_rnode(HeapTuple tuple, Oid databaseid)
{
    RelFileNode rnode;
    int16       algo_id;
    Oid         relid;
    Oid         table_oid;
    Relation    rel;
    Form_pg_class form_pg_class;

    relid = HeapTupleGetOid(tuple);

    if (IS_SYSTEM_REL(relid))
    {
        return;
    }

    /* try locking this relation to keep consistency in case dropping concurrently */
    rel = try_relation_open(relid, AccessShareLock);
    if (NULL == rel)
    {
        return;
    }

    /* get table oid */
    table_oid = rel_crypt_get_table_oid(rel);
    if (InvalidOid == table_oid)
    {
        relation_close(rel, AccessShareLock);    
        return;
    }

    /* looking up wether crypt was binding or not */
    algo_id = rel_crypt_get_relation_algoid(table_oid);
    if (TRANSP_CRYPT_ALGO_ID_IS_VALID(algo_id))
    {
        form_pg_class  = (Form_pg_class)GETSTRUCT(tuple);
    
        rnode.dbNode   = databaseid;
        if (InvalidOid == form_pg_class->reltablespace)
        {
            rnode.spcNode  = DEFAULTTABLESPACE_OID;
        }
        else
        {
            rnode.spcNode  = form_pg_class->reltablespace;
        }
        rnode.relNode  = form_pg_class->relfilenode;

        rel_crypt_create(&rnode, algo_id, true);
    }

    relation_close(rel, AccessShareLock);
    
    return;
}

/*
 * this relid maybe the oid of index, so get table oid
 */
void rel_crypt_relation_check_policy(Oid relid)
{
    Relation rel;
    int16    algo_id;
    Oid      table_oid;

    if (IS_SYSTEM_REL(relid))
    {
        return;
    }

    /* try locking this relation to keep consistency in case dropping concurrently */
    rel = try_relation_open(relid, AccessShareLock);
    if (NULL == rel)
    {
        return;
    }

    /* get table oid */
    table_oid = rel_crypt_get_table_oid(rel);
    if (InvalidOid == table_oid)
    {
        relation_close(rel, AccessShareLock);    
        return;
    }

    /* looking up wether crypt was binding or not */
    algo_id = rel_crypt_get_relation_algoid(table_oid);
    if (TRANSP_CRYPT_ALGO_ID_IS_VALID(algo_id))
    {
        rel_crypt_create_direct(table_oid, algo_id);
    }

    relation_close(rel, AccessShareLock);
    return;
}

/*
 * to bind table itself and its children if exist with algo_id, 
 * so rnode would be inserted into relcrypt shmem hash.
 * while, if api binding just a partition(not the parent), we do this upto parent level
 * NOTE:
 * 1. caller must be sure that relid is not system table
 * 2. share invalid message should be sent seperately
 */
void rel_crypt_create_direct(Oid relid, int16 algo_id)
{
    Oid         parent_oid;
    Oid         child_oid;
    List     *  children;
    ListCell *    lc;
    char        relkind;
    
    relkind = get_rel_relkind(relid);
    if (RELKIND_VIEW == relkind || RELKIND_SEQUENCE == relkind)
    {
        /* skip recording view and seq */
        return;
    }

    parent_oid = mls_get_parent_oid_by_relid(relid);    
    
    /* 1. treat itself */
    rel_crypt_create_one_relation(parent_oid, algo_id);

    /* 2. treat children */
    children = FetchAllParitionList(parent_oid);
    foreach(lc, children)
    {    
        child_oid = lfirst_oid(lc);
        rel_crypt_create_one_relation(child_oid, algo_id);
    }

    return;
}

/*
 * insert relation into relcrypt shmem hash, and insert all its indexes if exist
 */
static void rel_crypt_create_one_relation(Oid relid, int16 algo_id)
{
    Relation rel;
    List    *index_list;
    ListCell*l;
    
    rel = try_relation_open(relid, AccessShareLock);
    if (NULL == rel)
    {
        /* this should not happen */
        return;
    }
    
    rel_crypt_create(&(rel->rd_node), algo_id, true);

    /* treat indexes */
    index_list = RelationGetIndexList(rel);
    foreach(l, index_list)
    {
        Oid            indexOid;
        Relation    indexDesc;

        indexOid = lfirst_oid(l);

        indexDesc = index_open(indexOid, AccessShareLock);
        rel_crypt_create(&(indexDesc->rd_node), algo_id, true);
        index_close(indexDesc, AccessShareLock);
    }
    
    relation_close(rel, AccessShareLock);
    
    return;
}

Page rel_crypt_page_encrypt(RelCrypt relcrypt, Page page)
{
    int     len;
    AlgoId  algo_id;
    text   *encryptpage;
    static text *need_encrypt_text = NULL;
    static Page  page_new          = NULL;

    algo_id = relcrypt->algo_id;

    /*
     * We allocate the copy space once and use it over on each subsequent
     * call.  The point of palloc'ing here, rather than having a static char
     * array, is first to ensure adequate alignment for the checksumming code
     * and second to avoid wasting space in processes that never call this.
     */
    if (NULL == need_encrypt_text)
    {
        need_encrypt_text = MemoryContextAlloc(TopMemoryContext, (VARHDRSZ + PAGE_ENCRYPT_LEN));
    }
    
    if (NULL == page_new)
    {
        page_new = MemoryContextAlloc(TopMemoryContext, BLCKSZ);
    }

    if (PageIsNew(page))
    {
        PageSetAlgorithmId(page, TRANSP_CRYPT_INVALID_ALGORITHM_ID);        
        return page;
    }

    memset(page_new, 0, BLCKSZ);

    memset((char*)need_encrypt_text, 0, (VARHDRSZ + PAGE_ENCRYPT_LEN));
    SET_VARSIZE(need_encrypt_text, (VARHDRSZ + PAGE_ENCRYPT_LEN));
    memcpy(VARDATA(need_encrypt_text), (void *)((char*)page + sizeof(PageHeaderData)), PAGE_ENCRYPT_LEN);

    /* run encrypt algorithm */
    encryptpage = encrypt_procedure(algo_id, need_encrypt_text, (char*)page_new + sizeof(PageHeaderData));

    if (encryptpage)
    {
        /* aes128/192/256 has compression, so, the return data is a standard text, it contains length */
        len = VARSIZE_ANY(encryptpage);

        if (len <= PAGE_ENCRYPT_LEN)
        {   
            memcpy((char*)page_new, (char*)page, sizeof(PageHeaderData));
            memcpy((char*)page_new + sizeof(PageHeaderData), encryptpage, len);
            PageSetAlgorithmId(page, algo_id);
            PageSetAlgorithmId(page_new, algo_id);

            crypt_free(encryptpage);
            
            return page_new;
        }
    }
    else
    {
        /* 
         * crypt has no return, so, no length judge, page_new conntains the crypted context except the pageheader
         * such as guomi, sm4, we consider the length of crypted context is (blcksz - sizeof(PageHeaderData)) as default.
         */
        memcpy((char*)page_new, (char*)page, sizeof(PageHeaderData));
        PageSetAlgorithmId(page, algo_id);
        PageSetAlgorithmId(page_new, algo_id);

        /* encryptpage is NULL, no free */

        return page_new;
    }
    
    elog(LOG, "relfilenode:%d:%d:%d with algo_id:%d oversize", 
                        relcrypt->relfilenode.spcNode,
                        relcrypt->relfilenode.dbNode,
                        relcrypt->relfilenode.relNode,
                        relcrypt->algo_id);
 
    PageSetAlgorithmId(page, TRANSP_CRYPT_INVALID_ALGORITHM_ID);  
    
    crypt_free(encryptpage);
    
    return page;
}

/*
 * parellel crypt in workers
 * there is a difference between aes128 and guomi(sm4) in encrypt_procedure_inner 
 */
int rel_crypt_page_encrypting_parellel(int16 algo_id, char * page, char * buf_need_encrypt_input, char * page_new_output, CryptKeyInfo cryptkey, int workerid)
{     
    int     len;
    text   *encryptpage;
    text   *buf_need_encrypt;
    char   *page_new;

    page_new         = page_new_output;
    memset(page_new, 0, BLCKSZ);

    /* copy the page context except pageheader, and make a standard text struct */
    buf_need_encrypt = (text*)buf_need_encrypt_input;
    memset((char*)buf_need_encrypt, 0, (VARHDRSZ + PAGE_ENCRYPT_LEN));
    SET_VARSIZE(buf_need_encrypt, (VARHDRSZ + PAGE_ENCRYPT_LEN));
    memcpy(VARDATA(buf_need_encrypt), (void *)((char*)page + sizeof(PageHeaderData)), PAGE_ENCRYPT_LEN);

    /* run encrypt algorithm */
    encryptpage = encrypt_procedure_inner(cryptkey, buf_need_encrypt, page_new + sizeof(PageHeaderData));

    if (NULL != encryptpage)
    {
        if (g_enable_crypt_check)
        {
            crypt_check(algo_id, buf_need_encrypt, encryptpage, INVALID_CONTEXT_LENGTH, workerid);
        }
        
        /* aes128/192/256 would return a copy of crypted page context, so, copy it to dst page */
        len = VARSIZE_ANY(encryptpage);

        if (len <= PAGE_ENCRYPT_LEN)
        {
            memcpy((char*)page_new, (char*)page, sizeof(PageHeaderData));
            memcpy((char*)page_new + sizeof(PageHeaderData), (char*)encryptpage, len);
            PageSetAlgorithmId(page, algo_id);
            PageSetAlgorithmId(page_new, algo_id);

            crypt_free(encryptpage);
            
            return CRYPT_RET_SUCCESS;
        }
    }
    else
    {
        /* 
         * crypt has no return, so, no length judge, page_new conntains the crypted context except the pageheader
         * such as guomi, sm4, we consider the length of crypted context is (blcksz - sizeof(PageHeaderData)) as default.
         */
        if (g_enable_crypt_check)
        {
            crypt_check(algo_id, buf_need_encrypt, (text*)(page_new + sizeof(PageHeaderData)), BLCKSZ-sizeof(PageHeaderData), workerid);
        }
        
        memcpy((char*)page_new, (char*)page, sizeof(PageHeaderData));
        /* page_new is assigned in encrypt_procedure_inner except page header */    
        PageSetAlgorithmId(page, algo_id);
        PageSetAlgorithmId(page_new, algo_id);

        /* encryptpage is NULL, no free */

        return CRYPT_RET_SUCCESS;
    }

    PageSetAlgorithmId(page, TRANSP_CRYPT_INVALID_ALGORITHM_ID);  

    crypt_free(encryptpage);
    
    return CRYPT_RET_OVERSIZE;  
}


void print_page_header(PageHeader header)
{
    elog(LOG, "----print pagehead begin----\n"
              "pd_lsn(xlogid:%u xrecoff:%u), pd_checksum:%u, pd_flags:%u, pd_shard:%d, "
              "pd_lower:%u, pd_upper:%u, pd_special:%u, pd_pagesize_version:%u, pd_algorithm_id:%d, "
              "pd_prune_ts:"INT64_FORMAT ", pd_prune_xid:%u "
              "----print pagehead end----\n",
        header->pd_lsn.xlogid, header->pd_lsn.xrecoff,
        header->pd_checksum, header->pd_flags,
        header->pd_shard, header->pd_lower, header->pd_upper,
        header->pd_special, header->pd_pagesize_version, 
        header->pd_algorithm_id, header->pd_prune_ts, header->pd_prune_xid
    );
    return;
}

void rel_crypt_page_decrypt(RelCrypt relcrypt, Page page)
{
    int16   algo_id;
    text  *cryptedpage;
    text  *decryptpage;

    algo_id = PageGetAlgorithmId(page);

    cryptedpage = (text*)((char*)page + sizeof(PageHeaderData));

    /* run decrypt algorithm, context length for page decrypt is default:(BLCKSZ - sizeof(PageHeaderData)), this is used for guomi(sm4) */
    decryptpage = decrypt_procedure(algo_id, cryptedpage, BLCKSZ - sizeof(PageHeaderData));

    if (decryptpage)
    {
        /* just exchange data region */
        memcpy((char*)page + sizeof(PageHeaderData), VARDATA_ANY(decryptpage), VARSIZE_ANY_EXHDR(decryptpage));

        /* remember to free */
        //crypt_free(decryptpage);
    }
    else
    {
        /* guomi(sm4) decrypts and rewrites orignal page, no return, so no need to copy */
    }

    return;
}

/*
 * do the encrypt action
 * this function support several scenarios.
 * 1. sym/non-sym/udf crypt function.
 * 2. page_new_output is no use in sym/non-sym crypt.
 * 3. in crypt, such as guomi(sm4), page_new_output will be supplied for page crypt, and NULL for column crypt.
 * 4. column crypt will alloc memory in this function, to compatible with parellel crypt, 
 *    use malloc, and the memory would release with process quit.
 */
static text * encrypt_procedure_inner(CryptKeyInfo cryptkey_local, text * text_src, char * page_new_output)
{// #lizard forgives
    text * text_ret;
    text * password;
    text * option_args;
    text * pubkey;
    int16  option;
    int    datum_len;
    char * datum_ptr;
    static char * page_new_inner = NULL;
 
    option = cryptkey_local->option;

    if (CRYPT_KEY_INFO_OPTION_SYMKEY == option)
    {
        password    = (text*)(cryptkey_local->password);
        option_args = (text*)(cryptkey_local->option_args);
        text_ret    = encrypt_internal(0, 1, text_src, password, option_args);
    }
    else if (CRYPT_KEY_INFO_OPTION_ANYKEY == option)
    {
        pubkey      = (text*)(cryptkey_local->keypair->publickey);
        text_ret    = encrypt_internal(1, 1, text_src, pubkey, NULL);
    }
    else if (CRYPT_KEY_INFO_OPTION_SM4 == option)
    {
        //sm4_context ctx;
        //password    = (text*)(cryptkey_local->password);
        //sm4_setkey_enc(&ctx, (unsigned char *)VARDATA_ANY(password));
        if (page_new_output)
        {
            /* for page encrypt */
            /* 
             * arg1: sm4context
             * arg2: encrypt action = 1
             * arg3: length of context to encrypt
             * arg4: context to encrypt
             * arg5: encrypt result
             */
            sm4_crypt_ecb(&(cryptkey_local->sm4_ctx_encrypt), 1, VARSIZE_ANY_EXHDR(text_src), (unsigned char *)VARDATA_ANY(text_src), (unsigned char *)page_new_output);
            text_ret = NULL;
        }
        else
        {
            /* for column encrypt */
            if (VARSIZE_ANY(text_src) > BLCKSZ - sizeof(PageHeaderData))
            {
                elog(ERROR, "the column to crypt is oversize");
            }
            datum_len = VARSIZE_ANY_EXHDR(text_src) + VARHDRSZ;
            datum_ptr = palloc(datum_len);

            memset(datum_ptr, 0, datum_len);

            SET_VARSIZE(datum_ptr, datum_len);

            sm4_crypt_ecb(&(cryptkey_local->sm4_ctx_encrypt), 1, VARSIZE_ANY_EXHDR(text_src), (unsigned char *)VARDATA_ANY(text_src), (unsigned char *)datum_ptr + VARHDRSZ);
            
            text_ret = (text*)datum_ptr;
        }
    }
    else if (CRYPT_KEY_INFO_OPTION_UDF == option)
    {
        pubkey      = (text*)(cryptkey_local->keypair->publickey);

        if (page_new_output)
        {
            /* function 'DirectFunctionCall3Coll' force to make a return, otherwise, will make an error, so return value is necessary. */
            DirectFunctionCall3Coll(cryptkey_local->udf->encrypt_func, 
                                            InvalidOid,
                                            PointerGetDatum(text_src),
                                            PointerGetDatum(pubkey),
                                            PointerGetDatum(page_new_output));
            /* src and dst buffer are sent to udf crypt func */
            text_ret = NULL;
        }
        else
        {
            /* for column crypt */
            if (VARSIZE_ANY(text_src) > BLCKSZ - VARHDRSZ)
            {
                elog(ERROR, "the column to crypt is oversize");
            }
            
            if (NULL == page_new_inner)
            {
                page_new_inner = malloc(BLCKSZ);
            }
            memset(page_new_inner, 0, BLCKSZ);

            SET_VARSIZE(page_new_inner, BLCKSZ);

            DirectFunctionCall3Coll(cryptkey_local->udf->encrypt_func, 
                                                InvalidOid,
                                                PointerGetDatum(text_src),
                                                PointerGetDatum(pubkey),
                                                PointerGetDatum(VARDATA(page_new_inner)));

            text_ret = (text*)page_new_inner;
        }        
    }
    else
    {
        text_ret = NULL;
        elog(ERROR, "unknown option value:%d in encrypt_procedure", option);
    }
    
    return text_ret;
}

static void crypt_mem_cmp(const char * s1, const char * s2, int length)
{
    int i;
    
    for (i = 0; i < length; i++)
    {
        if (*(char*)(s1+i) != *(char*)(s2+i))
        {
            abort();
            Assert(0);
        }
    }

    return;
}

static void crypt_check(int16 algo_id, text * text_src, text * text_crypted, int length, int workerid)
{
    text * text_ret;
    char * text_crypt_copy = NULL;

    if (INVALID_CONTEXT_LENGTH == length)
    {
        text_ret = decrypt_procedure(algo_id, text_crypted, length);
        crypt_mem_cmp((char*)text_src, (char*)text_ret, VARSIZE_ANY(text_src));
    }
    else
    {
        text_crypt_copy = g_crypt_check_array[workerid];
        
        memset(text_crypt_copy, 0, BLCKSZ);

        memcpy(text_crypt_copy, (char*)text_crypted, length);

        decrypt_procedure(algo_id, (text*)text_crypt_copy, length);
        crypt_mem_cmp((char*)VARDATA_ANY(text_src), (char*)text_crypt_copy, length);
    }

    return;
}

void rel_crypt_init(void)
{
    int i;
    
    g_crypt_check_array = (char**)palloc(sizeof(char*) * g_checkpoint_crypt_worker);

    for(i = 0;i < g_checkpoint_crypt_worker; i++)
    {
        g_crypt_check_array[i] = palloc(BLCKSZ);
    }
    return;
}

#endif

#if MARK("column crypt")

#define TRANSP_CRYPT_INVALID_CACHEOFF       -1  /* relative to attcacheoff -1 */

#define TRANSP_CRYPT_ENABLED(X)             (NULL != (X))

static void transparent_crypt_check_and_fill_table_col_crypto(Oid relid, int attnum, TranspCrypt * transp_crypt, MemoryContext memcxt, Form_pg_attribute attr, Form_pg_attribute attr_ext, bool * has_plain_to_compress);
static void transparent_crypt_init_element(TranspCrypt * transp_crypt);
//static void transparent_crypt_assign_policy_algo_key(TranspCrypt * transp_crypt, MemoryContext memcxt, int attnum);
//static bytea * transparent_crypt_get_dearmor_key(char* key, MemoryContext memcxt);
static Datum transparent_crypt_encrypt_datum(Datum value, TranspCrypt * transp_crypt, Form_pg_attribute attr);
static text * transparent_crypt_datum_get_text(Datum value, Form_pg_attribute attr);
static void transparent_crypt_attr_plain_storage_to_compress_storage(Form_pg_attribute attr);
static Datum transparent_crypt_text_get_datum(text * value, Form_pg_attribute attr);
static void transparent_crypt_assign_tupledesc_field_inner(Oid relid, TupleDesc    tupledesc, MemoryContext memctx);

/*
 * a little quicker check whether this table was binding a crypt.
 * mix is true, no matter relcrypt or column crypt bound, will return yes,
 * otherwise, only column has crypt policy would return yes.
 */
bool trsprt_crypt_check_table_has_crypt(Oid relid, bool mix, bool * schema_bound)
{
    SysScanDesc scan;
    ScanKeyData skey[1];
    HeapTuple   htup;
    Relation    rel;
    bool        hascrypt;

    ScanKeyInit(&skey[0],
                    Anum_pg_transparent_crypt_policy_map_relid,
                    BTEqualStrategyNumber, 
                    F_OIDEQ,
                    ObjectIdGetDatum(relid));

    rel = heap_open(TransparentCryptPolicyMapRelationId, AccessShareLock);
    scan = systable_beginscan(rel, 
                              PgTransparentCryptPolicyMapIndexId, 
                              true,
                              NULL, 
                              1, 
                              skey);

    hascrypt = false;
    while (HeapTupleIsValid(htup = systable_getnext(scan)))
    {
        Form_pg_transparent_crypt_policy_map form_pol_map = (Form_pg_transparent_crypt_policy_map) GETSTRUCT(htup);
        if (mix)
        {
            hascrypt = true;
            if (schema_bound)
            {
                /* check if schema is bound crypto */
                if (TRANSP_CRYPT_INVALID_ALGORITHM_ID != mls_check_schema_crypted(form_pol_map->schemaoid))
                {
                    *schema_bound = true;
                }
                else
                {
                    *schema_bound = false;
                }
            }
        }
        else
        {
            if (REL_FILE_CRYPT_ATTR_NUM != form_pol_map->attnum)
            {
                hascrypt = true;
                break;
            }
        }
    }

    systable_endscan(scan);
    heap_close(rel, AccessShareLock);

    return hascrypt;
}

static void transparent_crypt_init_element(TranspCrypt * transp_crypt)
{
    transp_crypt->algo_id       = TRANSP_CRYPT_INVALID_ALGORITHM_ID;
#if 0    
    transp_crypt->option        = 0;
    transp_crypt->password      = NULL;
    transp_crypt->pubkey        = NULL;
    transp_crypt->privatekey    = NULL;
    transp_crypt->option_args   = NULL;
#endif
    return;
}

/*
 * here, we already known this table coupled with crypto, so check attributes one by one and mark them.
 */
static void transparent_crypt_check_and_fill_table_col_crypto(Oid relid, 
                                                                    int attnum, 
                                                                    TranspCrypt * transp_crypt, 
                                                                    MemoryContext memcxt, 
                                                                    Form_pg_attribute attr,
                                                                    Form_pg_attribute attr_ext,
                                                                    bool * has_plain_to_compress)
{
    SysScanDesc scan;
    ScanKeyData skey[2];
    HeapTuple   htup;
    Relation    rel;
    bool        found;
    bool        unsupport_data_type;

    (void)memcxt;

    found               = false;
    unsupport_data_type = false;

    ScanKeyInit(&skey[0],
                    Anum_pg_transparent_crypt_policy_map_relid,
                    BTEqualStrategyNumber, 
                    F_OIDEQ,
                    ObjectIdGetDatum(relid));
    ScanKeyInit(&skey[1],
                    Anum_pg_transparent_crypt_policy_map_attnum,
                    BTEqualStrategyNumber, 
                    F_INT2EQ,
                    Int16GetDatum(attnum));

    rel = heap_open(TransparentCryptPolicyMapRelationId, AccessShareLock);
    scan = systable_beginscan(rel, 
                              PgTransparentCryptPolicyMapIndexId, 
                              true,
                              NULL, 
                              2, 
                              skey);
    
    if (HeapTupleIsValid(htup = systable_getnext(scan)))
    {
        /* attribute is bound with crypt policy, then check type is supported or not */
        if (true == mls_support_data_type(attr->atttypid))
        {
            Form_pg_transparent_crypt_policy_map form_trans_crypt 
                    = (Form_pg_transparent_crypt_policy_map) GETSTRUCT(htup);
            //if (true == form_trans_crypt->enable)
            {
                transp_crypt->algo_id = form_trans_crypt->algorithm_id;

                crypt_key_info_load_by_algoid(transp_crypt->algo_id);
                found = true; 

                /* 
                 * backup attribute infos for these firm length datatype, such int2/4/8, 
                 * to mark the modify of storage type.
                 * while, this treating method is not perfect in one case, to make an example as follows,
                 * we change attcacheoff to '-1' directly, in fact, the first variable length col, 
                 * its attcacheoff could be valid.
                 * we omit this case.
                 */
                transparent_crypt_attr_plain_storage_to_compress_storage(attr_ext);
                *has_plain_to_compress = true;
            }
        }
        else
        {
            unsupport_data_type = true;
        }
    }

    systable_endscan(scan);
    heap_close(rel, AccessShareLock);

    if (false == found)
    {
        transparent_crypt_init_element(transp_crypt);
    }

    if (true == unsupport_data_type)
    {
        elog(ERROR, "transparent crypt:unsupport type, typid:%d", 
            attr->atttypid);
    }

    return ;
}

bool trsprt_crypt_chk_tbl_col_has_crypt(Oid relid, int attnum)
{
    SysScanDesc scan;
    ScanKeyData skey[2];
    HeapTuple   htup;
    Relation    rel;
    bool        found;
    
    found               = false;

    ScanKeyInit(&skey[0],
                    Anum_pg_transparent_crypt_policy_map_relid,
                    BTEqualStrategyNumber, 
                    F_OIDEQ,
                    ObjectIdGetDatum(relid));
    ScanKeyInit(&skey[1],
                    Anum_pg_transparent_crypt_policy_map_attnum,
                    BTEqualStrategyNumber, 
                    F_INT2EQ,
                    Int16GetDatum(attnum));

    rel = heap_open(TransparentCryptPolicyMapRelationId, AccessShareLock);
    scan = systable_beginscan(rel, 
                              PgTransparentCryptPolicyMapIndexId, 
                              true,
                              NULL, 
                              2, 
                              skey);
    
    if (HeapTupleIsValid(htup = systable_getnext(scan)))
    {
        found = true;
    }

    systable_endscan(scan);
    heap_close(rel, AccessShareLock);

    return found;
}

bool trsprt_crypt_chk_tbl_has_col_crypt(Oid relid)
{
    SysScanDesc scan;
    ScanKeyData skey[1];
    HeapTuple   htup;
    Relation    rel;
    bool        found;
    
    found               = false;

    ScanKeyInit(&skey[0],
                    Anum_pg_transparent_crypt_policy_map_relid,
                    BTEqualStrategyNumber, 
                    F_OIDEQ,
                    ObjectIdGetDatum(relid));

    rel = heap_open(TransparentCryptPolicyMapRelationId, AccessShareLock);
    scan = systable_beginscan(rel, 
                              PgTransparentCryptPolicyMapIndexId, 
                              true,
                              NULL, 
                              1, 
                              skey);
    
    while (HeapTupleIsValid(htup = systable_getnext(scan)))
    {
        Form_pg_transparent_crypt_policy_map form = (Form_pg_transparent_crypt_policy_map) GETSTRUCT(htup);

        if (form->attnum > InvalidAttrNumber)
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
 *  decrypt one column when 'SELECT' value, while, only several basic type supported, 
 *      such as varchar,text 
 */
Datum trsprt_crypt_decrypt_one_col_value(TranspCrypt*transp_crypt,
                                                        Form_pg_attribute attr, 
                                                        Datum inputval)
{
    Datum   datum_ret;
    text *  datum_text;
    text *  input_text;

    if (TRANSP_CRYPT_INVALID_ALGORITHM_ID != transp_crypt->algo_id)
    {
    	input_text = DatumGetTextP(inputval);
    	
        datum_text = decrypt_procedure(transp_crypt->algo_id, input_text, INVALID_CONTEXT_LENGTH);
        if (datum_text)
        {
        datum_ret = transparent_crypt_text_get_datum(datum_text, attr);
        }
        else
        {
	        datum_ret = transparent_crypt_text_get_datum(input_text, attr);
        }

        return datum_ret;
    }
    
    elog(ERROR, "get an invalid transp_crypt->algo_id:%d", TRANSP_CRYPT_INVALID_ALGORITHM_ID);
    
    return Int32GetDatum(0);   
}

void transparent_crypt_copy_attrs(Form_pg_attribute * dst_attrs, Form_pg_attribute * src_attrs, int natts)
{
    int i;
    
    for (i = 0; i < natts; i++)
    {
        memcpy(dst_attrs[i], src_attrs[i], ATTRIBUTE_FIXED_PART_SIZE);
    }
    
    return;
}

/*
 * assign transp_crypt in tupledesc
 * check the table(relid) has transparent crypt policy or not,
 * if so, assign passwd or keys, then create an copy of attrs in case of those 'attbyval' columns
 */
static void transparent_crypt_assign_tupledesc_field_inner(Oid relid, TupleDesc    tupledesc, MemoryContext memctx)
{
    bool               has_plain_to_compress;
    int                attnum;
    int                natts;
    int                loop;
    MemoryContext      old_memctx;
    Form_pg_attribute *attrs;
    Form_pg_attribute *attrs_ext;
    TranspCrypt       *transp_crypt;
    Oid                parent_oid;
    
    if (!IS_SYSTEM_REL(relid))
    {
        parent_oid = mls_get_parent_oid_by_relid(relid);
        
        /* quick check wether this table has col with crypt */
        if (false == trsprt_crypt_check_table_has_crypt(parent_oid, false, NULL))
        {
        	trsprt_crypt_free_strut_in_tupdesc(tupledesc, tupledesc->natts);
            return;
        }

        natts = tupledesc->natts;

        old_memctx = MemoryContextSwitchTo(memctx);

        /* build transp_crypt */
        transp_crypt = (TranspCrypt *)palloc0(sizeof(TranspCrypt) * natts);

        /* build attrs_ext */
        attrs_ext = (Form_pg_attribute *)palloc0(sizeof(Form_pg_attribute) * natts);
        for (loop = 0; loop < natts; loop++)
        {
            attrs_ext[loop] = (Form_pg_attribute)palloc0(sizeof(FormData_pg_attribute));
        }
        attrs = tupledesc->attrs;
        transparent_crypt_copy_attrs(attrs_ext, attrs, natts);

        has_plain_to_compress = false;

        /* check col one by one */
        for (attnum = 0; attnum < natts; attnum++)
        {
            /* skip the droped column */
            if (0 == tupledesc->attrs[attnum]->attisdropped)
            {
                transparent_crypt_check_and_fill_table_col_crypto(parent_oid, 
                                                            attnum + 1, 
                                                            &transp_crypt[attnum], 
                                                            memctx, 
                                                            attrs[attnum],
                                                            attrs_ext[attnum],
                                                            &has_plain_to_compress);
                /* 
                 * if there was one attribute storage changing from plain to compress, 
                 * later, the following attributes could not be fetched by cacheoff, 
                 * so we mark the attrcacheoff of following ones to invalid 
                 */
                if (has_plain_to_compress)
                {
                    attrs_ext[attnum]->attcacheoff = TRANSP_CRYPT_INVALID_CACHEOFF;
                }
            }
        }

        MemoryContextSwitchTo(old_memctx);
        
        tupledesc->transp_crypt = transp_crypt;
        tupledesc->attrs_ext    = attrs_ext;
    }
    else
    {
        tupledesc->transp_crypt = NULL;
        tupledesc->attrs_ext    = NULL;
        tupledesc->use_attrs_ext= false;
    }
    
    return;
}

/*
 * we place transp_crypt in tupledesc
 */
void trsprt_crypt_asign_rela_tuldsc_fld(Relation relation)
{
    if (false == g_enable_transparent_crypt)
    {
        return;
    }
    
    transparent_crypt_assign_tupledesc_field_inner(RelationGetRelid(relation), relation->rd_att, CacheMemoryContext);

    return;    
}

/*
 * to make up tupdescfree.
 */ 
void trsprt_crypt_free_strut_in_tupdesc(TupleDesc tupledesc, int natts)
{
    int                 loop;
    TranspCrypt        *transp_crypt;
    Form_pg_attribute  *attrs_ext;

    /* treat transp_crypt */
    transp_crypt = tupledesc->transp_crypt;
    if (transp_crypt)
    {
        pfree(transp_crypt);
    }
    tupledesc->transp_crypt = NULL;

    /* treat attrs_ext */
    attrs_ext = tupledesc->attrs_ext;
    if (attrs_ext)
    {
        for (loop = 0; loop < natts; loop++)
        {
            if (attrs_ext[loop])
            {
                pfree(attrs_ext[loop]);
            }
        }

        pfree(attrs_ext);
    }
    tupledesc->attrs_ext = NULL;

    return ;
}



/* 
 * after tuple deform to slot, exchange the col values with decrypt result.
 */
void trsprt_crypt_dcrpt_all_col_vale(ScanState *node, TupleTableSlot *slot, Oid relid)
{// #lizard forgives
    bool                need_exchange_slot_tts_tuple = false;
    int                    attnum      = 0;
    TupleDesc            tupleDesc   = slot->tts_tupleDescriptor;
    Datum               *tuple_values= NULL;
    bool               *tuple_isnull= NULL;
    Datum               *slot_values = slot->tts_values;
    bool               *slot_isnull = slot->tts_isnull;
    TranspCrypt        *transp_crypt= slot->tts_tupleDescriptor->transp_crypt;
    Form_pg_attribute  *att         = tupleDesc->attrs;
    int                 numberOfAttributes = slot->tts_tupleDescriptor->natts;
    HeapTuple           new_tuple;
    MemoryContext       old_memctx;

    if (transp_crypt)
    {
        old_memctx = MemoryContextSwitchTo(slot->tts_mls_mcxt);
        
        if (slot->tts_tuple)
        {
            need_exchange_slot_tts_tuple = true;
        }

        if (need_exchange_slot_tts_tuple)
        {
            tuple_values = (Datum *) palloc(numberOfAttributes * sizeof(Datum));
            tuple_isnull = (bool *) palloc(numberOfAttributes * sizeof(bool));

            TRANSP_CRYPT_ATTRS_EXT_ENABLE(tupleDesc);
            heap_deform_tuple(slot->tts_tuple, tupleDesc, tuple_values, tuple_isnull);
            TRANSP_CRYPT_ATTRS_EXT_DISABLE(tupleDesc);
        }
        
        for (attnum = 0; attnum < numberOfAttributes; attnum++)
        {
            /* skip null col */
            if (need_exchange_slot_tts_tuple && tuple_isnull[attnum])
            {
                continue;
            }
            else if (!need_exchange_slot_tts_tuple && slot_isnull[attnum])
            {
                continue;
            }
            
            if (TRANSP_CRYPT_INVALID_ALGORITHM_ID != transp_crypt[attnum].algo_id)
            {
                if (need_exchange_slot_tts_tuple)
                {
                    slot_values[attnum]  = trsprt_crypt_decrypt_one_col_value(&transp_crypt[attnum],
                                                                            att[attnum], 
                                                                            tuple_values[attnum]);
                }
                else
                {
                    /* tuple_values are null, so try slot_values */
                    slot_values[attnum]  = trsprt_crypt_decrypt_one_col_value(&transp_crypt[attnum],
                                                                            att[attnum], 
                                                                            slot_values[attnum]);
                }

                /* 
                 * if datum is invalid, slot_values is invalid either, keep orginal value in tuple_value
                 * it seems a little bored
                 */
                if (need_exchange_slot_tts_tuple)
                {
                    tuple_values[attnum] = slot_values[attnum];
                }
            }
        }

        if (need_exchange_slot_tts_tuple)
        {
            /* do not forget to fill shardid */
            if (RelationIsSharded(node->ss_currentRelation))
            {
                new_tuple = heap_form_tuple_plain(tupleDesc, tuple_values, tuple_isnull, RelationGetDisKey(node->ss_currentRelation),
                                                   RelationGetSecDisKey(node->ss_currentRelation), RelationGetRelid(node->ss_currentRelation));
            }
            else
            {
                new_tuple = heap_form_tuple(tupleDesc, tuple_values, tuple_isnull);
            }

            /* remember to do this copy manually */
            new_tuple->t_self       = slot->tts_tuple->t_self;
            new_tuple->t_tableOid   = slot->tts_tuple->t_tableOid;
            new_tuple->t_xc_node_id = slot->tts_tuple->t_xc_node_id;

            /* after forming a new tuple, the orginal could be free if needed */
            if (slot->tts_shouldFree)
            {
                heap_freetuple(slot->tts_tuple);
                slot->tts_tuple = NULL;
            }            
            
            slot->tts_tuple = new_tuple;
            
            slot->tts_shouldFree = true;
            
            pfree(tuple_values);
            pfree(tuple_isnull);
        }

        MemoryContextSwitchTo(old_memctx);
    }
    
    return;
}

void trsprt_crypt_dcrpt_all_col_vale_cp(HeapTuple tuple, TupleDesc tupleDesc,
                                                            Datum *values, bool *nulls)
{
    int                    attnum;
    TranspCrypt        *transp_crypt;
    Form_pg_attribute  *attrs;
    int                 numberOfAttributes;

    transp_crypt       = tupleDesc->transp_crypt;
    numberOfAttributes = tupleDesc->natts;
    attrs              = tupleDesc->attrs;

    if (transp_crypt)
    {
        //old_memctx = MemoryContextSwitchTo(slot->tts_mcxt);

        TRANSP_CRYPT_ATTRS_EXT_ENABLE(tupleDesc);
        heap_deform_tuple(tuple, tupleDesc, values, nulls);
        TRANSP_CRYPT_ATTRS_EXT_DISABLE(tupleDesc);

        for (attnum = 0; attnum < numberOfAttributes; attnum++)
        {
            /* skip null col */
            if (nulls[attnum])
            {
                continue;
            }
            
            if (TRANSP_CRYPT_INVALID_ALGORITHM_ID != transp_crypt[attnum].algo_id)
            {
                values[attnum]  = trsprt_crypt_decrypt_one_col_value(&transp_crypt[attnum],
                                                                            attrs[attnum], 
                                                                            values[attnum]);
            }
        }
        //MemoryContextSwitchTo(old_memctx);
    }
    
    return;
}

static void transparent_crypt_attr_plain_storage_to_compress_storage(Form_pg_attribute attr)
{
    attr->attlen        = -1;
    attr->attbyval      = 0;
    attr->attcacheoff   = -1;
    attr->attstorage    = 'x';
    attr->attalign      = 'i';
    return;
}

static Datum transparent_crypt_text_get_datum(text * value, Form_pg_attribute attr)
{// #lizard forgives
    Oid     typid;
    text *  datum_text;
    Datum   datum_ret;
    char    datum_str[70] = {0};

    typid = attr->atttypid;
    switch(typid)
    {
        case VARCHAR2OID:
        case VARCHAROID:
        case TEXTOID:
        case BPCHAROID:
        case BYTEAOID:
            PG_RETURN_TEXT_P(value);
            break;
        case INT2OID:
        case INT4OID:
        case INT8OID:
        case FLOAT4OID:
        case FLOAT8OID:
        case NUMERICOID:
        case TIMESTAMPOID:
            {
                Oid    typinput;
                Oid typioparam;
                
                datum_text = DatumGetTextP(value);  

                getTypeInputInfo(typid, &typinput, &typioparam);

                memcpy(datum_str, VARDATA_ANY(datum_text), VARSIZE_ANY_EXHDR(datum_text));

                datum_ret = OidInputFunctionCall(typinput, datum_str,
                                            typioparam, -1);
                return datum_ret;

#if 0
                if (INT2OID == typid)
                {
                    datum_ret = Int16GetDatum(atoi(VARDATA_ANY(datum_text)));
                    return datum_ret;
                }
                else if (INT4OID == typid)
                {
                    datum_ret = Int32GetDatum(atoi(VARDATA_ANY(datum_text)));
                    return datum_ret;
                }
                else if (INT8OID == typid)
                {
                    datum_ret = Int64GetDatum(atoll(VARDATA_ANY(datum_text)));
                    return datum_ret;
                }
                else if (FLOAT4OID == typid)
                {
                    datum_ret = DirectFunctionCall1(float4in, VARDATA_ANY(datum_text));
                    return datum_ret;
                }
#endif                
            }
            break;
        default:
            break;
    }
    
    /* keep compiler quite */
    return Int32GetDatum(0);
}

/*
 * get text struct of datum, if datum is in plain storage, that should be converted to compress
 */
static text * transparent_crypt_datum_get_text(Datum value, Form_pg_attribute attr)
{// #lizard forgives
#define TRANSPARENT_CRYPT_DIGITAL_MAX_LEN 32
    
    Oid typid;

    typid = attr->atttypid;
    switch(typid)
    {
        case VARCHAR2OID:
        case VARCHAROID:
        case TEXTOID:
        case BPCHAROID:
        case BYTEAOID:
            return DatumGetTextPP(value);
            break;
            
        case INT2OID:
        case INT4OID:
        case INT8OID:
        case FLOAT4OID:
        case FLOAT8OID:
        case NUMERICOID:
        case TIMESTAMPOID:
            {
                char   digital_str[TRANSPARENT_CRYPT_DIGITAL_MAX_LEN];
                int    len;
                text * datum_text;
                Oid       typoutput;
                bool   typisvarlen;
                
                len = 0;

                memset(digital_str, 0, TRANSPARENT_CRYPT_DIGITAL_MAX_LEN);
                
                getTypeOutputInfo(typid, &typoutput, &typisvarlen);
                
                snprintf(digital_str, TRANSPARENT_CRYPT_DIGITAL_MAX_LEN, "%s", 
                                    DatumGetCString(OidOutputFunctionCall(typoutput, value)));
                
                len = strnlen(digital_str, TRANSPARENT_CRYPT_DIGITAL_MAX_LEN);    

                datum_text = palloc(VARHDRSZ + len);
                SET_VARSIZE(datum_text, VARHDRSZ + len);
                memcpy(VARDATA(datum_text), digital_str, len);

                return datum_text;
            }
            break;    
        default:
            elog(ERROR, "datatype:%d dose not be supported by transparent crypt", typid);
            break;
    }

    /* keep compiler quite */
    return NULL;
}

/* 
 * the caller must sure that algo_id was valid 
 */
static Datum transparent_crypt_encrypt_datum(Datum value, TranspCrypt * transp_crypt, Form_pg_attribute attr)
{
    text *        datum_text;
    
    Assert(transp_crypt);

    datum_text = transparent_crypt_datum_get_text(value, attr);

    datum_text = encrypt_procedure(transp_crypt->algo_id, datum_text, NULL);
        
    return PointerGetDatum(datum_text);
}

HeapTuple transparent_crypt_encrypt_columns(Relation relation, HeapTuple tup, MemoryContext memctx)
{
    int                    attnum;
    TupleDesc            tupleDesc;
    Datum               *tuple_values;
    bool               *tuple_isnull;
    int                 numberOfAttributes;
    TranspCrypt        *transp_crypt;
    Form_pg_attribute  *attrs;
    HeapTuple           heaptup;
    MemoryContext       old_memctx;
    HeapTupleHeader     tupheader;
    
    tupleDesc = relation->rd_att;
    
    numberOfAttributes = tupleDesc->natts;
    transp_crypt       = tupleDesc->transp_crypt;
    attrs              = tupleDesc->attrs;

    old_memctx = MemoryContextSwitchTo(memctx);

    tuple_values = (Datum *) palloc(numberOfAttributes * sizeof(Datum));
    tuple_isnull = (bool *)  palloc(numberOfAttributes * sizeof(bool));

    //TRANSP_CRYPT_ATTRS_EXT_ENABLE(tupleDesc);
    heap_deform_tuple(tup, tupleDesc, tuple_values, tuple_isnull);
    //TRANSP_CRYPT_ATTRS_EXT_DISABLE(tupleDesc);
    
    for (attnum = 0; attnum < numberOfAttributes; attnum++)
    {
        /* skip null col */
        if (tuple_isnull[attnum])
        {
            continue;
        }
        
        if (TRANSP_CRYPT_INVALID_ALGORITHM_ID != transp_crypt[attnum].algo_id)
        {
            tuple_values[attnum] = transparent_crypt_encrypt_datum(tuple_values[attnum], 
                                                                    &transp_crypt[attnum], 
                                                                    attrs[attnum]);
        }
    }
    
    TRANSP_CRYPT_ATTRS_EXT_ENABLE(tupleDesc);
    if (RelationIsSharded(relation))
    {
        heaptup = heap_form_tuple_plain(tupleDesc, tuple_values, tuple_isnull, RelationGetDisKey(relation),
                                        RelationGetSecDisKey(relation), RelationGetRelid(relation));
    }
    else
    {
        heaptup = heap_form_tuple(tupleDesc, tuple_values, tuple_isnull);
    }
    TRANSP_CRYPT_ATTRS_EXT_DISABLE(tupleDesc);
    tupheader = heaptup->t_data;

    heaptup->t_self       = tup->t_self;
    heaptup->t_tableOid   = tup->t_tableOid;
    heaptup->t_xc_node_id = tup->t_xc_node_id;

    memcpy((char*)tupheader, (char*)tup->t_data, SizeofHeapTupleHeader);

    MemoryContextSwitchTo(old_memctx);
    
    return heaptup;
}
#endif

#if MARK("extern")
text * encrypt_procedure(AlgoId algo_id, text * text_src, char * page_new_output)
{// #lizard forgives
    text * text_ret;
    bool   found;
    /* accelerate looking for */
    static int16        algo_id_keep  = TRANSP_CRYPT_INVALID_ALGORITHM_ID;
    static CryptKeyInfo cryptkey_keep = NULL;

    if (algo_id == algo_id_keep && cryptkey_keep)
    {
        ; 
    }
    else
    {   
        found = crypt_key_info_hash_lookup(algo_id, &cryptkey_keep);

        if (false == found)
        {
            elog(ERROR, "algo_id:%d dose not exist", algo_id);
        }

        /* cache it for next time using the same crypt key info */
        algo_id_keep  = algo_id;
    }

    text_ret = encrypt_procedure_inner(cryptkey_keep, text_src, page_new_output);
#if 0    
    if (g_enable_crypt_check)
    {
        if (text_ret)
        {
            crypt_check(algo_id, text_src, text_ret, INVALID_CONTEXT_LENGTH);
        }
        else if (page_new_output)
        {
            crypt_check(algo_id, text_src, (text*)page_new_output, BLCKSZ - sizeof(PageHeaderData));
        }
    }
#endif
    return text_ret;
}

text * decrypt_procedure(AlgoId algo_id, text * text_src, int context_length)
{// #lizard forgives
    text * text_ret;
    text * password;
    text * privatekey;
    bool   found;
    int16  option;
    
    /* accelerate looking for */
    static int16        algo_id_keep = TRANSP_CRYPT_INVALID_ALGORITHM_ID;
    static CryptKeyInfo cryptkey     = NULL;

    if (algo_id == algo_id_keep && cryptkey)
    {
        ; 
    }
    else
    {   
        found = crypt_key_info_hash_lookup(algo_id, &cryptkey);
        if (false == found)
        {
            elog(ERROR, "algo_id:%d dose not exist", algo_id);
        }
        
        /* cache it for next time using the same crypt key info */
        algo_id_keep = algo_id;
    }
    
    option = cryptkey->option;

    if (CRYPT_KEY_INFO_OPTION_SYMKEY == option)
    {
        password    = (text*)(cryptkey->password);
        text_ret    = decrypt_internal(0, 1, text_src, password, NULL, NULL);   
    }
    else if (CRYPT_KEY_INFO_OPTION_ANYKEY == option)
    {
        privatekey  = (text*)(cryptkey->keypair->privatekey);
        text_ret    = decrypt_internal(1, 1, text_src, privatekey, NULL, NULL);
    }
    else if (CRYPT_KEY_INFO_OPTION_SM4 == option)
    {

        if (DECRYPT_CONTEXT_LENGTH_VALID(context_length))
        {
            /* for page decrypt */
            /* 
             * arg1: sm4context
             * arg2: decrypt action = 0
             * arg3: length of context to decrypt
             * arg4: context to encrypt, not (text*) type
             * arg5: encrypt result, not (text*) type
             */
            sm4_crypt_ecb(&(cryptkey->sm4_ctx_decrypt), 0, context_length, (unsigned char*)text_src, (unsigned char*)text_src);
            
            text_ret = NULL;
        }
        else
        {
            /* for column decrypt */
            int ctx_len;
            
            ctx_len  = VARSIZE_ANY_EXHDR(text_src);
            
            sm4_crypt_ecb(&(cryptkey->sm4_ctx_decrypt), 0, ctx_len, (unsigned char*)VARDATA_ANY(text_src), (unsigned char*)VARDATA_ANY(text_src));
            
            text_ret = NULL;
        }
        
    }
    else if (CRYPT_KEY_INFO_OPTION_UDF == option)
    {
        privatekey  = (text*)(cryptkey->keypair->privatekey);

        DirectFunctionCall3Coll(cryptkey->udf->decrypt_func, 
                                            InvalidOid,
                                            PointerGetDatum(text_src),
                                            PointerGetDatum(privatekey),
                                            PointerGetDatum(text_src));
        text_ret    = NULL;
    }
    else
    {
        text_ret    = NULL;
        elog(ERROR, "unknown option value:%d in decrypt_procedure", option);
    }
    
    return text_ret;
}

/*
 * check object having crypt policy or not, if the schema had policy, the policies also should be deleted.
 */
void DeleteCryptPolicy(const Oid relid)
{
    Relation    rel;
    ScanKeyData skey[1];
    SysScanDesc scan;
    HeapTuple    oldtup;
    bool        schema_bound;

    if (IS_SYSTEM_REL(relid))
    {
        return;
    }

    if (true == mls_check_relation_permission(relid, &schema_bound))
    {
        if (true == schema_bound)
        {
            /*
             * if schema is crypted, crypt policies in TransparentCryptPolicyMapRelationId should be delete cascade.
             */
            ScanKeyInit(&skey[0],
                        Anum_pg_transparent_crypt_policy_map_relid,
                        BTEqualStrategyNumber, 
                        F_OIDEQ,
                        ObjectIdGetDatum(relid));

            rel = heap_open(TransparentCryptPolicyMapRelationId, RowExclusiveLock);
            scan = systable_beginscan(rel, 
                                      PgTransparentCryptPolicyMapIndexId, 
                                      true,
                                      NULL, 
                                      1, 
                                      skey);
            /*
             * delete all policies of the relation.   
             */
            while (HeapTupleIsValid(oldtup = systable_getnext(scan)))
            {
                CatalogTupleDelete(rel, &oldtup->t_self);
            }
            
            systable_endscan(scan);

            heap_close(rel, RowExclusiveLock);
        }        
    }    

    return;
}

void RenameCryptRelation(Oid myrelid, const char *newrelname)
{
    bool        nulls[Natts_pg_transparent_crypt_policy_map];
    bool        replaces[Natts_pg_transparent_crypt_policy_map];
    Datum        values[Natts_pg_transparent_crypt_policy_map];
    Relation    rel;
    ScanKeyData skey[1];
    SysScanDesc scan;
    HeapTuple    oldtup;
    HeapTuple    newtup;
    bool        schema_bound;
    
    if (IS_SYSTEM_REL(myrelid))
    {
        return;
    }
    
    if (true == mls_check_relation_permission(myrelid, &schema_bound))
    {
        if (true == schema_bound)
        {
            /*
             * if schema is crypted, tables could be renamed, 
             * and the copies in TransparentCryptPolicyMapRelationId should also be updated.
             */
            ScanKeyInit(&skey[0],
                        Anum_pg_transparent_crypt_policy_map_relid,
                        BTEqualStrategyNumber, 
                        F_OIDEQ,
                        ObjectIdGetDatum(myrelid));

            rel = heap_open(TransparentCryptPolicyMapRelationId, RowExclusiveLock);
            scan = systable_beginscan(rel, 
                                      PgTransparentCryptPolicyMapIndexId, 
                                      true,
                                      NULL, 
                                      1, 
                                      skey);
            
            /* Everything ok, form a new tuple. */
            memset(values, 0, sizeof(values));
            memset(nulls, false, sizeof(nulls));
            memset(replaces, false, sizeof(replaces));

            values[Anum_pg_transparent_crypt_policy_map_tblname - 1]   = CStringGetDatum(newrelname);
            replaces[Anum_pg_transparent_crypt_policy_map_tblname - 1] = true;

            /*
             * update all policies of the relation.   
             */
            while (HeapTupleIsValid(oldtup = systable_getnext(scan)))
            {
                newtup = heap_modify_tuple(oldtup, RelationGetDescr(rel), values, nulls, replaces);

                CatalogTupleUpdate(rel, &newtup->t_self, newtup);
                
                heap_freetuple(newtup);
            }
            
            systable_endscan(scan);

            heap_close(rel, RowExclusiveLock);
        }        
    }    
    
    return;
}

/*
 * if table is altered schema, the copies in TransparentCryptPolicyMapRelationId should also be updated.
 */
void AlterCryptdTableNamespace(Oid myrelid, Oid newschemaoid, const char *newschemaname)
{
    bool        nulls[Natts_pg_transparent_crypt_policy_map];
    bool        replaces[Natts_pg_transparent_crypt_policy_map];
    Datum        values[Natts_pg_transparent_crypt_policy_map];
    Relation    rel;
    ScanKeyData skey[1];
    SysScanDesc scan;
    HeapTuple    oldtup;
    HeapTuple    newtup;
    
    if (IS_SYSTEM_REL(myrelid) || (NULL == newschemaname))
    {
        return;
    }
    
    if (true == mls_check_relation_permission(myrelid, NULL))
    {
        ScanKeyInit(&skey[0],
                    Anum_pg_transparent_crypt_policy_map_relid,
                    BTEqualStrategyNumber, 
                    F_OIDEQ,
                    ObjectIdGetDatum(myrelid));

        rel = heap_open(TransparentCryptPolicyMapRelationId, RowExclusiveLock);
        scan = systable_beginscan(rel, 
                                  PgTransparentCryptPolicyMapIndexId, 
                                  true,
                                  NULL, 
                                  1, 
                                  skey);
        
        /* Everything ok, form a new tuple. */
        memset(values, 0, sizeof(values));
        memset(nulls, false, sizeof(nulls));
        memset(replaces, false, sizeof(replaces));

        values[Anum_pg_transparent_crypt_policy_map_schemaoid - 1]   = ObjectIdGetDatum(newschemaoid);
        replaces[Anum_pg_transparent_crypt_policy_map_schemaoid - 1] = true;
        values[Anum_pg_transparent_crypt_policy_map_nspname - 1]     = CStringGetDatum(newschemaname);
        replaces[Anum_pg_transparent_crypt_policy_map_nspname - 1]   = true;

        /*
         * update all policies of the relation.   
         */
        while (HeapTupleIsValid(oldtup = systable_getnext(scan)))
        {
            newtup = heap_modify_tuple(oldtup, RelationGetDescr(rel), values, nulls, replaces);

            CatalogTupleUpdate(rel, &newtup->t_self, newtup);
            
            heap_freetuple(newtup);
        }
        
        systable_endscan(scan);

        heap_close(rel, RowExclusiveLock);
    }    
    
    return;
}


#endif
