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
 * relcryptmap.c
 *   relation file crypt map
 *
 * Portions Copyright (c) 2018, Tbase Global Development Group
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/backend/utils/cache/relcryptmap.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <catalog/namespace.h>

#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "catalog/catalog.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_tablespace.h"
#include "catalog/storage.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "pgstat.h"
#include "storage/fd.h"
#include "storage/lwlock.h"
#include "storage/bufpage.h"
#include "storage/buffile.h"
#include "utils/inval.h"
#include "utils/tuplestore.h"
#include "utils/builtins.h"
#include "funcapi.h"

#include "postmaster/bgwriter.h"

#include "catalog/pg_mls.h"
#include "utils/mls.h"
#include "utils/lsyscache.h"
#include "utils/hsearch.h"

#include "contrib/sm/sm4.h"

#include "storage/bufmgr.h"
#include "utils/relcrypt.h"
#include "utils/relcryptmisc.h"
#include "storage/relcryptstorage.h"
#include "utils/relcryptmap.h"
#include "catalog/indexing.h"
#include "utils/fmgroids.h"
#include "utils/relfilenodemap.h"

#ifdef _MLS_
#include "utils/mls_extension.h"
#endif


#define CRYPT_KEY_MAP_FILENAME                  "pg_crypt_key.map"
#define CRYPT_KEY_MAP_FILEMAGIC                 0x952701    /* version ID value */
#define CRYPT_KEY_PAGE_SIZE                     8192
#define CRYPT_KEY_BUF_SIZE                      (8192 - VARHDRSZ)
/* there are CRYPT_KEY_INFO_HASHTABLE_INIT_SIZE crypt keys, hope this is enough */
#define CRYPT_KEY_INFO_HASHTABLE_INIT_SIZE      256
#define CRYPT_KEY_INFO_HASHTABLE_MAX_SIZE       CRYPT_KEY_INFO_HASHTABLE_INIT_SIZE
#define CRYPT_KEY_INFO_HASHTABLE_NUM_PARITIONS  16


#define REL_CRYPT_MAP_FILENAME                  "pg_rel_crypt.map"
#define REL_CRYPT_MAP_FILEMAGIC                 0x952702    /* version ID value */
#define REL_CRYPT_HASHTABLE_MAX_SIZE            ((g_rel_crypt_hash_size > (1 << 20))? g_rel_crypt_hash_size : (1 << 20))
#define REL_CRYPT_HASHTABLE_INIT_SIZE           g_rel_crypt_hash_size
#define REL_CRYPT_HASHTABLE_NUM_PARTITIONS      128


typedef struct tagCryptKeyInfoLockData
{
    /* LWLocks */
    int            lwlock_tranche_id;
    LWLockPadded locks[CRYPT_KEY_INFO_HASHTABLE_NUM_PARITIONS];
} CryptKeyInfoLockData;
typedef CryptKeyInfoLockData * CryptKeyInfoLock;
static HTAB * g_crypt_key_info_hash             = NULL;
static CryptKeyInfoLock g_crypt_key_info_lock   = NULL;
#define crypt_key_info_hash_code(_algo_id)              (get_hash_value(g_crypt_key_info_hash, (void *)(_algo_id)))
#define crypt_key_info_hash_partition(_hashcode)         ((_hashcode) % CRYPT_KEY_INFO_HASHTABLE_NUM_PARITIONS)
#define crypt_key_info_hash_get_partition_lock(_partid) (&(g_crypt_key_info_lock->locks[(_partid)].lock))


typedef struct tagRelCryptLockData
{
    /* LWLocks */
    int             lwlock_tranche_id;
    LWLockPadded locks[REL_CRYPT_HASHTABLE_NUM_PARTITIONS];
} RelCryptLockData;
static RelCryptLockData * g_rel_crypt_lock      = NULL;
static HTAB             * g_rel_crypt_hash      = NULL;
#define rel_crypt_hash_partition(_hashcode)     ((_hashcode) % REL_CRYPT_HASHTABLE_NUM_PARTITIONS)
#define rel_crypt_get_partition_lock(_partid)   (&(g_rel_crypt_lock->locks[(_partid)].lock))
#define rel_crypt_hash_code(_rnode)             (get_hash_value(g_rel_crypt_hash, (void *) (_rnode)))

/* 
 * this struct describes dump file mapping of relfilenode and algoid, it is a fix size.
 * we encrypt it with encrypt_procedure, the function use text* type as input,
 * therefore, we keep the struct size as 4k - VARHDRSZ, that means obligate 4 bytes for VARHDR.
 */
/* (4096 - sizeof(int)*4) / sizeof(RelCryptMapElement) = ((4096 / sizeof(RelCryptMapElement)) - 1)*/
#define REL_CRYPT_MAP_ELEMENT_CNT_PER_PAGE  255 
typedef struct tagRelCryptMapFile
{
    int                 magic;          /* always REL_CRYPT_MAP_FILEMAGIC */
    pg_crc32c           crc;            /* CRC of followings */
    int                 element_count;  /* how many rel file node in RelCryptEntry array */
    RelCryptEntry       elements[REL_CRYPT_MAP_ELEMENT_CNT_PER_PAGE];   
} RelCryptMapFile;

/*
 * this struct describes crypt key info into dump files, the size of file is variable,
 * cause the necessary infos needed are different among sym crypt, non-sym crypt, udf crypt.
 * while, we also try to keep memory aligned, that means every 8k page having one CryptKeyMapFile header,
 * and the count of keys record in element_count.
 */
typedef struct tagCryptKeyMapFile
{
    int                 magic;          /* always CRYPT_KEY_MAP_FILEMAGIC */
    pg_crc32c           crc;            /* CRC of followings */
    int                 element_count;
    int                 total_len;      /* the whole length of valid bytes contains CryptKeyMapFile header */
} CryptKeyMapFile;


#if MARK("crypt key manager")
static void crypt_key_info_init(CryptKeyInfo cryptkey);
static int crypt_key_info_hash_algoid_cmp (const void *key1, const void *key2, Size keysize);
static void crypt_key_info_load_mapfile_inner(char * key_info_from_map, CryptKeyInfo cryptkey);
static void crypt_key_info_write_mapfile(void);
static int crypt_key_info_mapfile_get_next_key(char * buffer, int offset, int * keysize_out);
static void crypt_key_info_key_serialization(char * buffer, int offset_input, CryptKeyInfo cryptkey_input);
static void crypt_key_info_key_deserialization(char * buffer, CryptKeyInfo cryptkey_output);
static void crypt_key_info_map_check(char *buffer, CryptKeyMapFile **map_output);

static void crypt_key_info_init(CryptKeyInfo cryptkey)
{
    int option ;

    option = cryptkey->option;
    
    memset((char*)cryptkey, 0x0, offsetof(CryptKeyInfoEntry, udf));

    if (cryptkey->udf && (CRYPT_KEY_INFO_OPTION_UDF == option))
    {
        memset((char*)cryptkey->udf, 0x0, sizeof(CryptKeyInfoUDF));
    }

    if (cryptkey->keypair && (CRYPT_KEY_INFO_OPTION_ANYKEY == option || CRYPT_KEY_INFO_OPTION_UDF == option))
    {
        memset((char*)cryptkey->keypair, 0x0, sizeof(CryptKeyInfoKeyPair));
    }
    
    return;
}

void crypt_key_info_free(CryptKeyInfo cryptkey)
{
    if (cryptkey)
    {
        if (cryptkey->udf)
        {
            pfree(cryptkey->udf);
        }

        if (cryptkey->keypair)
        {
            pfree(cryptkey->keypair);
        }

        pfree(cryptkey);
    }
    return;
}

CryptKeyInfo crypt_key_info_alloc(int option)
{
    CryptKeyInfo cryptkey;

    cryptkey = palloc0(sizeof(CryptKeyInfoEntry));

    cryptkey->option  = option;
    cryptkey->udf     = NULL;
    cryptkey->keypair = NULL;

    if (CRYPT_KEY_INFO_OPTION_UDF == option)
    {
        cryptkey->udf = palloc0(sizeof(CryptKeyInfoUDF));
    }

    if (CRYPT_KEY_INFO_OPTION_UDF == option || CRYPT_KEY_INFO_OPTION_ANYKEY == option)
    {
        cryptkey->keypair = palloc0(sizeof(CryptKeyInfoKeyPair));
    }
    
    return cryptkey;
}



void cyprt_key_info_hash_init(void)
{
    HASHCTL    info;
    bool    found;
    int     i;
    /* assume no locking is needed yet */

    /* BufferTag maps to Buffer */
    info.keysize          = sizeof(AlgoId);
    info.entrysize        = sizeof(CryptKeyInfoEntry);
    info.num_partitions   = CRYPT_KEY_INFO_HASHTABLE_NUM_PARITIONS;
    info.match            = crypt_key_info_hash_algoid_cmp;
    g_crypt_key_info_hash = ShmemInitHash("crypt key info shared hash table",
                                          CRYPT_KEY_INFO_HASHTABLE_INIT_SIZE, 
                                          CRYPT_KEY_INFO_HASHTABLE_MAX_SIZE,
                                          &info,
        								  HASH_ELEM  | HASH_PARTITION | HASH_COMPARE | HASH_BLOBS);
    g_crypt_key_info_lock = (CryptKeyInfoLock) ShmemInitStruct("crypt key info lock shmem",
                                                              MAXALIGN64(sizeof(CryptKeyInfoLockData)),
                                                              &found);
    if (false == found)
    {
        g_crypt_key_info_lock->lwlock_tranche_id = LWTRANCHE_CRYPT_KEY_INFO_LOCK;
        
        for (i = 0; i < CRYPT_KEY_INFO_HASHTABLE_NUM_PARITIONS; i++)
        {
            LWLockInitialize(&(g_crypt_key_info_lock->locks[i].lock), g_crypt_key_info_lock->lwlock_tranche_id);
        }
        
        LWLockRegisterTranche(g_crypt_key_info_lock->lwlock_tranche_id, "crypt key info lock");
    }

    return;
}

Size crypt_key_info_hash_shmem_size(void)
{
    return add_size(MAXALIGN64(hash_estimate_size(CRYPT_KEY_INFO_HASHTABLE_MAX_SIZE, sizeof(CryptKeyInfoEntry)))
                    , MAXALIGN64(sizeof(CryptKeyInfoLockData)));
}

static int crypt_key_info_hash_algoid_cmp (const void *key1, const void *key2, Size keysize)
{
    const AlgoId *tagPtr1 = key1, *tagPtr2 = key2;

    if (*tagPtr1 == *tagPtr2)
    {
        return 0;
    }
    
    return 1;
}

void crypt_key_info_hash_insert(CryptKeyInfo cryptkey_input, bool write_wal, bool in_building_procedure)
{// #lizard forgives
    CryptKeyInfo cryptkey;
    bool     found;
    uint32   hashcode;
    int      partitionno;
    LWLock    *partitionLock;
    int      option;
    int      size;
    char    *buffer;
    int16    algo_id;
    
    algo_id = cryptkey_input->algo_id;
    option  = cryptkey_input->option;
    size    = crypt_key_info_cal_key_size(cryptkey_input);
    
    hashcode      = crypt_key_info_hash_code(&algo_id);
    partitionno   = crypt_key_info_hash_partition(hashcode);
    partitionLock = crypt_key_info_hash_get_partition_lock(partitionno);

    LWLockAcquire(partitionLock, LW_EXCLUSIVE);

    cryptkey = (CryptKeyInfo)hash_search_with_hash_value(g_crypt_key_info_hash,
                                                    (void *) &algo_id,
                                                    hashcode,
                                                    HASH_ENTER,
                                                    &found);
    if (false == found)
    {
        if ((false == in_building_procedure)
            &&(CRYPT_DEFAULT_INNER_ALGO_ID != algo_id))
        {
            RequestFlushCryptkeyMap();
        }
        
        cryptkey->algo_id                 = algo_id;
        cryptkey->option                  = cryptkey_input->option;
        cryptkey->keysize                 = cryptkey_input->keysize;

        memcpy(cryptkey->password,          cryptkey_input->password,         CRYPT_KEY_INFO_MAX_PASSWORD_LEN);
        memcpy(cryptkey->option_args,       cryptkey_input->option_args,      CRYPT_KEY_INFO_MAX_OPT_ARGS_LEN);

        /* guomi sm4 */
        sm4_setkey_enc(&(cryptkey->sm4_ctx_encrypt), (unsigned char *)VARDATA_ANY(cryptkey->password));
        sm4_setkey_dec(&(cryptkey->sm4_ctx_decrypt), (unsigned char *)VARDATA_ANY(cryptkey->password));
        
        if (CRYPT_KEY_INFO_OPTION_UDF == option || CRYPT_KEY_INFO_OPTION_ANYKEY == option)
        {
            if (CRYPT_KEY_INFO_OPTION_UDF == option)
            {
                if (cryptkey->udf)
                {
                    cryptkey->udf->encrypt_oid             = cryptkey_input->udf->encrypt_oid;
                    cryptkey->udf->decrypt_oid             = cryptkey_input->udf->decrypt_oid;

                    memcpy(cryptkey->udf->encrypt_prosrc,    cryptkey_input->udf->encrypt_prosrc,   CRYPT_KEY_INFO_PROC_SRC_LEN);
                    memcpy(cryptkey->udf->encrypt_probin,    cryptkey_input->udf->encrypt_probin,   CRYPT_KEY_INFO_PROC_BIN_LEN);
                    memcpy(cryptkey->udf->decrypt_prosrc,    cryptkey_input->udf->decrypt_prosrc,   CRYPT_KEY_INFO_PROC_SRC_LEN);
                    memcpy(cryptkey->udf->decrypt_probin,    cryptkey_input->udf->decrypt_probin,   CRYPT_KEY_INFO_PROC_BIN_LEN);

                    /* load function address for shared lib */
                    cryptkey->udf->decrypt_func = load_external_function(cryptkey->udf->decrypt_probin, 
                                                                        cryptkey->udf->decrypt_prosrc,
                                                                        true, 
                                                                        NULL);

                    cryptkey->udf->encrypt_func = load_external_function(cryptkey->udf->encrypt_probin, 
                                                                        cryptkey->udf->encrypt_prosrc,
                                                                        true, 
                                                                        NULL);
                }
            }
            if (cryptkey->keypair)
            {
                memcpy(cryptkey->keypair->publickey,         cryptkey_input->keypair->publickey,        CRYPT_KEY_INFO_MAX_PUBKEY_LEN);
                memcpy(cryptkey->keypair->privatekey,        cryptkey_input->keypair->privatekey,       CRYPT_KEY_INFO_MAX_PRIKEY_LEN);
            }
        }
    }

    /* Critical section done */
    if (write_wal && !found && (CRYPT_DEFAULT_INNER_ALGO_ID != algo_id))
    {
        XLogRecPtr  lsn;
        
        buffer = palloc0(size);
        crypt_key_info_key_serialization(buffer, 0, cryptkey_input);

        /* now errors are fatal ... */
        START_CRIT_SECTION();

        XLogBeginInsert();
        XLogRegisterData(buffer, size);
        
        lsn = XLogInsert(RM_REL_CRYPT_ID, XLOG_CRYPT_KEY_INSERT);

        /* As always, WAL must hit the disk before the data update does */
        XLogFlush(lsn);
        
        END_CRIT_SECTION();
        
        pfree(buffer);
    }

    LWLockRelease(partitionLock);

    if (g_enable_crypt_debug)                
    {
        if (write_wal)
        {
            if (!in_building_procedure)
            {
                if (!found)
                {
                    if (CRYPT_DEFAULT_INNER_ALGO_ID != algo_id)
                    {
                        elog(LOG, "CRYPT_KEY_INSERT, new crypt key insert, need checkpoint to flush, "
                            "algo_id:%d, option:%d, option_args:%s", 
                            algo_id, cryptkey_input->option, VARDATA_ANY(cryptkey_input->option_args));
                    }
                    else
                    {
                        elog(ERROR, "CRYPT_KEY_INSERT, default crypt key insert, should NOT happen, position 1, "
                            "algo_id:%d, option:%d, option_args:%s", 
                            algo_id, cryptkey_input->option, VARDATA_ANY(cryptkey_input->option_args));
                    }
                }
                else
                {
                    if (CRYPT_DEFAULT_INNER_ALGO_ID != algo_id)
                    {
                        elog(LOG, "CRYPT_KEY_INSERT, new crypt key insert concurrently, procedure duplicate xlog insert, "
                                "need checkpoint to flush, algo_id:%d, option:%d, option_args:%s", 
                                algo_id, cryptkey_input->option, VARDATA_ANY(cryptkey_input->option_args));
                    }
                    else
                    {
                        elog(ERROR, "CRYPT_KEY_INSERT, default crypt key insert, should NOT happen, position 2, "
                            "algo_id:%d, option:%d, option_args:%s", 
                            algo_id, cryptkey_input->option, VARDATA_ANY(cryptkey_input->option_args));
                    }
                }
            }
            else
            {
                elog(ERROR, "CRYPT_KEY_INSERT, default crypt key insert, should NOT happen, position 3, "
                            "algo_id:%d, option:%d, option_args:%s", 
                            algo_id, cryptkey_input->option, VARDATA_ANY(cryptkey_input->option_args));
            }
        }
        else
        {
            if (!in_building_procedure)
            {
                if (!found)
                {
                    if (CRYPT_DEFAULT_INNER_ALGO_ID != algo_id)
                    {
                        elog(LOG, "CRYPT_KEY_INSERT, new crypt key insert in REDO procedure, need checkpoint to flush, "
                            "algo_id:%d, option:%d, option_args:%s", 
                            algo_id, cryptkey_input->option, VARDATA_ANY(cryptkey_input->option_args));
                    }
                    else
                    {
                        elog(ERROR, "CRYPT_KEY_INSERT, default crypt key insert, key should be loaded in postmaster start up, "
                            "should NOT happen, position 4, algo_id:%d, option:%d, option_args:%s", 
                            algo_id, cryptkey_input->option, VARDATA_ANY(cryptkey_input->option_args));
                    }
                }
                else
                {
                    if (CRYPT_DEFAULT_INNER_ALGO_ID != algo_id)
                    {
                        elog(LOG, "CRYPT_KEY_INSERT, new crypt key insert concurrently, duplicate xlog insert in REDO procedure, "
                                "need checkpoint to flush, algo_id:%d, option:%d, option_args:%s", 
                                algo_id, cryptkey_input->option, VARDATA_ANY(cryptkey_input->option_args));
                    }
                    else
                    {
                        elog(ERROR, "CRYPT_KEY_INSERT, default crypt key insert, should NOT happen, position 5, "
                            "algo_id:%d, option:%d, option_args:%s", 
                            algo_id, cryptkey_input->option, VARDATA_ANY(cryptkey_input->option_args));
                    }
                }
            }
            else
            {
                if (!found)
                {
                    if (CRYPT_DEFAULT_INNER_ALGO_ID != algo_id)
                    {
                        elog(LOG, "CRYPT_KEY_INSERT, new crypt key insert in loading crypt key map, "
                            "algo_id:%d, option:%d, option_args:%s", 
                            algo_id, cryptkey_input->option, VARDATA_ANY(cryptkey_input->option_args));
                    }
                    else
                    {
                        elog(LOG, "CRYPT_KEY_INSERT, default crypt key insert, postmaster maybe starting up, "
                            "loading crypt key map, algo_id:%d, option:%d, option_args:%s", 
                            algo_id, cryptkey_input->option, VARDATA_ANY(cryptkey_input->option_args));
                    }
                }
                else
                {
                    if (CRYPT_DEFAULT_INNER_ALGO_ID != algo_id)
                    {
                        elog(WARNING, "CRYPT_KEY_INSERT, new crypt key insert, duplicate crypt key in crypt key map, "
                                "should NOT happen, position 6, algo_id:%d, option:%d, option_args:%s", 
                                algo_id, cryptkey_input->option, VARDATA_ANY(cryptkey_input->option_args));
                    }
                    else
                    {
                        elog(ERROR, "CRYPT_KEY_INSERT, default crypt key insert, should NOT happen, position 7, "
                                "algo_id:%d, option:%d, option_args:%s", 
                                algo_id, cryptkey_input->option, VARDATA_ANY(cryptkey_input->option_args));
                    }
                }
            }
        }
    }

    return;
}

bool crypt_key_info_hash_lookup(AlgoId algo_id, CryptKeyInfo * cryptkey_ret)
{
    CryptKeyInfo cryptkey = NULL;
    uint32   hashcode;
    int      partitionno;
    LWLock    *partitionLock;
    bool     found;

    hashcode      = crypt_key_info_hash_code(&algo_id);
    partitionno   = crypt_key_info_hash_partition(hashcode);
    partitionLock = crypt_key_info_hash_get_partition_lock(partitionno);

    LWLockAcquire(partitionLock, LW_SHARED);
    cryptkey = (CryptKeyInfo)hash_search_with_hash_value(g_crypt_key_info_hash,
                                                    (void *) &algo_id,
                                                    hashcode,
                                                    HASH_FIND,
                                                    NULL);
    found = false;
    if (NULL != cryptkey)
    {
        found = true;
        if (NULL != cryptkey_ret)
        {
            *cryptkey_ret = cryptkey;
            //memcpy((char*)relcrypt_ret, relcrypt, sizeof(CryptKeyInfoEntry));
        }
    }
    LWLockRelease(partitionLock);

    return found;
}


static void crypt_key_info_map_check(char *buffer, CryptKeyMapFile **map_output)
{
    pg_crc32c   crc;
    CryptKeyMapFile * map;
    text            * decrypt_buf;
    
    /* decrpyt */
    map = *map_output;
    
    /* decrypt map file */
    decrypt_buf = decrypt_procedure(CRYPT_DEFAULT_INNER_ALGO_ID, (text*)buffer, INVALID_CONTEXT_LENGTH);
    memcpy((char*)map, VARDATA_ANY(decrypt_buf), VARSIZE_ANY_EXHDR(decrypt_buf));

    crypt_free(decrypt_buf);
    
    /* check for correct magic number, etc */
    if (map->magic != CRYPT_KEY_MAP_FILEMAGIC ||
        map->element_count < 0 ||
        map->total_len > CRYPT_KEY_BUF_SIZE)
    {
        ereport(FATAL,
                (errmsg("crypt key file contains invalid data")));
    }
    
    /* verify the CRC */
    INIT_CRC32C(crc);
    COMP_CRC32C(crc, (char *) map + sizeof(int)*2, CRYPT_KEY_BUF_SIZE - sizeof(int)*2);
    FIN_CRC32C(crc);

    if (!EQ_CRC32C(crc, map->crc))
    {
        ereport(FATAL,
                (errmsg("crypt key file contains incorrect checksum")));
    }
    
    return;
}

void crypt_key_info_load_mapfile(void)
{// #lizard forgives
    CryptKeyMapFile *map;
    char            *buffer;
    CryptKeyInfo     cryptkey;
    char        mapfilename[MAXPGPATH];
    int         len;
    int         loop;
    int         offset;
    int         current_key_info_offset;
    int         current_key_info_len;
    BufFile    *buffile;

    /* STEP 1, for default crypt key */
    crypt_key_info_load_default_key();
    
    /* STEP 2, OTHERS */        
    snprintf(mapfilename, sizeof(mapfilename), "%s/%s", "global", CRYPT_KEY_MAP_FILENAME);

    buffile = BufFileOpen(mapfilename, O_RDONLY | PG_BINARY, S_IRUSR | S_IWUSR, true, LOG);
    if (NULL == buffile)
    {
        return;
    }

    buffer   = palloc0(CRYPT_KEY_PAGE_SIZE);
    map      = palloc(CRYPT_KEY_PAGE_SIZE);
    cryptkey = crypt_key_info_alloc(CRYPT_KEY_INFO_OPTION_UDF);

    /* ---- TODO ---- there should be loop to read map file */
    do{
        pgstat_report_wait_start(WAIT_EVENT_CRYPT_KEY_MAP_READ);
        len = BufFileRead(buffile, buffer, CRYPT_KEY_PAGE_SIZE);
        if (0 == len)
        {
            pgstat_report_wait_end();
            break;
        }
        
        if (len != CRYPT_KEY_PAGE_SIZE)
        {
            elog(ERROR, "could not read datalen:%d in crypt key bufFile, readlen:%d.", 
                CRYPT_KEY_PAGE_SIZE, len);            
        }
        pgstat_report_wait_end();
    
        /* get one valid buffer */
        crypt_key_info_map_check(buffer, &map);

        /* load buffer context */
        offset = sizeof(CryptKeyMapFile);
        if (g_enable_crypt_debug)                
        {
             elog(LOG, "CRYPT_KEY_INSERT, postmaster is starting up, ----load begin----");
        }
        for (loop = 0; loop < map->element_count && offset < map->total_len; loop++)
        {
            current_key_info_offset = crypt_key_info_mapfile_get_next_key((char*)map, offset, &current_key_info_len);
            
            crypt_key_info_load_mapfile_inner((char*)map + current_key_info_offset, cryptkey);

            offset = current_key_info_offset + current_key_info_len;
        }
        if (g_enable_crypt_debug)                
        {
             elog(LOG, "CRYPT_KEY_INSERT, postmaster is starting up, ----load end----");
        }
        Assert(offset < CRYPT_KEY_BUF_SIZE);
        
    }while(1);

    if (BufFileReadDone(buffile))
    {
        BufFileClose(buffile);
    }
    else
    {
        elog(ERROR, "no data in crypt key buffile while reading data.");
    }

    /* ---- TODO ---- there should be loop to read map file */
    pfree(map);
    pfree(buffer);
    crypt_key_info_free(cryptkey);

    return;
}

static void crypt_key_info_load_mapfile_inner(char * key_info_from_map, CryptKeyInfo cryptkey)
{
    crypt_key_info_key_deserialization(key_info_from_map, cryptkey);
    
    crypt_key_info_hash_insert(cryptkey, false, true);
    
    return;
}

void crypt_key_info_load_default_key(void)
{
    CryptKeyInfo cryptkey;
    
    cryptkey= crypt_key_info_alloc(CRYPT_KEY_INFO_OPTION_SYMKEY);

    cryt_ky_inf_fil_stru_for_deflt_alg(CRYPT_DEFAULT_INNER_ALGO_ID, cryptkey);
    /* no wal_write and no need to flush map file */
    if (g_enable_crypt_debug)                
    {
        elog(LOG, "CRYPT_KEY_INSERT, crypt_key_info_load_default_key, algo_id:%d, option:%d, option_args:%s", 
            CRYPT_DEFAULT_INNER_ALGO_ID, cryptkey->option, VARDATA_ANY(cryptkey->option_args));
    }
    
    crypt_key_info_hash_insert(cryptkey, false, true);  

    crypt_key_info_free(cryptkey);
    
    return;
}
void cryt_ky_inf_fil_stru_for_deflt_alg(AlgoId algo_id, CryptKeyInfo  cryptkey)
{
#if 1
    char * option_char = "cipher-algo=AES128, compress-algo=2, compress-level=9";
    text * option_text;
    text * passwd_text;

    cryptkey->algo_id = algo_id;
    cryptkey->option = CRYPT_KEY_INFO_OPTION_SYMKEY;
    
    passwd_text = palloc0(VARHDRSZ + strlen(MLS_USER_DEFAULT_PASSWD));
    SET_VARSIZE(passwd_text, VARHDRSZ + strlen(MLS_USER_DEFAULT_PASSWD));
    memcpy(VARDATA(passwd_text), MLS_USER_DEFAULT_PASSWD, strlen(MLS_USER_DEFAULT_PASSWD));

    option_text = palloc0(VARHDRSZ + strlen(option_char));
    SET_VARSIZE(option_text, VARHDRSZ + strlen(option_char));
    memcpy(VARDATA(option_text), option_char, strlen(option_char));

    memcpy(cryptkey->password,    passwd_text, VARSIZE_ANY(passwd_text));
    memcpy(cryptkey->option_args, option_text, VARSIZE_ANY(option_text));

    cryptkey->keysize = crypt_key_info_cal_key_size(cryptkey);
    
    pfree(option_text);
    pfree(passwd_text);
#else    
    text * default_private_key_dearmor;
    text * default_public_key_dearmor;

    cryptkey->option = CRYPT_KEY_INFO_OPTION_ANYKEY;

    default_private_key_dearmor = crypt_key_info_get_dearmor_key(transparent_crypt_get_private_key());
    default_public_key_dearmor  = crypt_key_info_get_dearmor_key(transparent_crypt_get_pub_key());

    if (VARSIZE_ANY(default_public_key_dearmor) > CRYPT_KEY_INFO_MAX_PUBKEY_LEN)
    {
        elog(FATAL, "public key dearmor is over length");
    }

    if (VARSIZE_ANY(default_private_key_dearmor) > CRYPT_KEY_INFO_MAX_PRIKEY_LEN)
    {
        elog(FATAL, "private key dearmor is over length");
    }
    
    memcpy(cryptkey->publickey,  default_public_key_dearmor,  VARSIZE_ANY(default_public_key_dearmor));
    memcpy(cryptkey->privatekey, default_private_key_dearmor, VARSIZE_ANY(default_private_key_dearmor));
#endif
    return;
}

static void crypt_key_info_map_init(CryptKeyMapFile *map)
{
    memset((char*)map, 0x0, CRYPT_KEY_PAGE_SIZE);
    
    map->magic = CRYPT_KEY_MAP_FILEMAGIC;
    
    return;
}   
static void crypt_key_info_write_mapfile_post(CryptKeyMapFile *map, int element_cnt, BufFile *buffile)
{
    static text * need_encrypt_text = NULL;
    text * encrypt_text;
    
    if (NULL == need_encrypt_text)
    {
        need_encrypt_text = palloc(CRYPT_KEY_PAGE_SIZE);
    }

    /* fill header */
    map->element_count = element_cnt;

    /* crc */
    INIT_CRC32C(map->crc);
    COMP_CRC32C(map->crc, (char *) map + sizeof(int)*2, CRYPT_KEY_BUF_SIZE - sizeof(int)*2);
    FIN_CRC32C(map->crc);

    /* encrypt procedure */
    memset((char*)need_encrypt_text, 0x0, CRYPT_KEY_PAGE_SIZE);
    
    SET_VARSIZE(need_encrypt_text, CRYPT_KEY_PAGE_SIZE);
    memcpy(VARDATA(need_encrypt_text), map, CRYPT_KEY_BUF_SIZE);

    /* use default algo, so no need to consider about page_new_output */
    encrypt_text = encrypt_procedure(CRYPT_DEFAULT_INNER_ALGO_ID, need_encrypt_text, NULL);
    
    /* this should not happen */
    if (VARSIZE_ANY(encrypt_text) > CRYPT_KEY_PAGE_SIZE)
    {
        elog(ERROR, "encyrypt crypt key mapfile page is oversize:%lu", VARSIZE_ANY(encrypt_text));
    }
    
    /* write */
    pgstat_report_wait_start(WAIT_EVENT_CRYPT_KEY_MAP_WRITE);
    BufFileWrite(buffile,encrypt_text,CRYPT_KEY_PAGE_SIZE);
    pgstat_report_wait_end();

    crypt_free(encrypt_text);
    
    return;
}

int crypt_key_info_cal_key_size(CryptKeyInfo cryptkey)
{
    int size;
    int option;

    option = cryptkey->option;

    size   = offsetof(CryptKeyInfoEntry, udf);

    switch(option)
    {
        case CRYPT_KEY_INFO_OPTION_SYMKEY:
        case CRYPT_KEY_INFO_OPTION_SM4:
            break;
        case CRYPT_KEY_INFO_OPTION_ANYKEY:
            size = size + VARSIZE_ANY(cryptkey->keypair->publickey) + VARSIZE_ANY(cryptkey->keypair->privatekey);
            break;
        case CRYPT_KEY_INFO_OPTION_UDF:            
            size = size + sizeof(Oid)*2;
            size = size + VARSIZE_ANY(cryptkey->udf->encrypt_prosrc) + VARSIZE_ANY(cryptkey->udf->encrypt_probin);
            size = size + VARSIZE_ANY(cryptkey->udf->decrypt_prosrc) + VARSIZE_ANY(cryptkey->udf->decrypt_probin);
            size = size + VARSIZE_ANY(cryptkey->keypair->publickey)  + VARSIZE_ANY(cryptkey->keypair->privatekey);
            break;
        default :
            elog(ERROR, "unknown option, when cal key size");
            break;
    }
    
    return size;
}


static void crypt_key_info_write_mapfile(void)
{// #lizard forgives
    int              offset;
    int              loop;
    int              keysize;
    int              lock_loop;
    char            *mapfilename;
    char            *mapfilename_new;
    BufFile         *buffile;
    HASH_SEQ_STATUS  status;
    CryptKeyInfo     cryptkey;
    static CryptKeyMapFile *buffer = NULL;
    
    /*
     * check the change from last flush
     */
    if (false == NeedFlushCryptkeyMap())
    {
        return;
    }

    mapfilename     = palloc0(MAXPGPATH);
    mapfilename_new = palloc0(MAXPGPATH);
    snprintf(mapfilename,     MAXPGPATH, "%s/%s",    "global", CRYPT_KEY_MAP_FILENAME);
    snprintf(mapfilename_new, MAXPGPATH, "%s/%s.%d", "global", CRYPT_KEY_MAP_FILENAME, MyProcPid);
    
    if (NULL == buffer)
    {
        buffer = palloc(CRYPT_KEY_PAGE_SIZE);
    }

    /* init */
    crypt_key_info_map_init(buffer);

    buffile = BufFileOpen(mapfilename_new, (O_WRONLY|O_CREAT|PG_BINARY), (S_IRUSR|S_IWUSR), true, ERROR);
    
    /* lock all partition lock*/
    for (lock_loop = 0; lock_loop < CRYPT_KEY_INFO_HASHTABLE_NUM_PARITIONS; lock_loop++)
    {
        LWLockAcquire(crypt_key_info_hash_get_partition_lock(lock_loop), LW_SHARED);
    }  

    do{
        /* init */
        loop   = 0;
        offset = sizeof(CryptKeyMapFile);

        hash_seq_init(&status, g_crypt_key_info_hash);

        while ((cryptkey = (CryptKeyInfo) hash_seq_search(&status)) != NULL)
        {
            if (CRYPT_DEFAULT_INNER_ALGO_ID != cryptkey->algo_id)
            {
                keysize = cryptkey->keysize;

                if (offset + keysize > CRYPT_KEY_BUF_SIZE)
                {
                    /* current buffer is full, flush it */
                    buffer->element_count = loop;
                    buffer->total_len     = offset;
                    crypt_key_info_write_mapfile_post(buffer, loop, buffile);

                    /* next round, init */
                    loop   = 0;
                    offset = sizeof(CryptKeyMapFile);
                    crypt_key_info_map_init(buffer); 
                }

                /* fill in current buffer */
                crypt_key_info_key_serialization((char*)buffer, offset, cryptkey);
                
                loop++;
                offset = offset + keysize;
            }
        }

        /* hash is empty, flush */
        buffer->element_count = loop;
        buffer->total_len     = offset;
        crypt_key_info_write_mapfile_post(buffer, loop, buffile);
    }while(0);

    pgstat_report_wait_start(WAIT_EVENT_CRYPT_KEY_MAP_SYNC);
    BufFileClose(buffile);
    pgstat_report_wait_end();
    
    /* atom action */
    rename(mapfilename_new, mapfilename);   

    /* release all */
    for (lock_loop = CRYPT_KEY_INFO_HASHTABLE_NUM_PARITIONS - 1; lock_loop >= 0; lock_loop--)
    {
        LWLockRelease(crypt_key_info_hash_get_partition_lock(lock_loop));
    }

    /* finish this flush, so clear and wait for next */
    ClearRequestFlushCryptkeyMap();

    if (g_enable_crypt_debug)                
    {
        elog(LOG, "CRYPT_KEY_INSERT, crypt key hash changed, finish flushing");
    }
    
    if (mapfilename)
    {
        pfree(mapfilename);
    }
    if (mapfilename_new)
    {
        pfree(mapfilename_new);
    }
    
    return;
}

static int crypt_key_info_mapfile_get_next_key(char * buffer, int offset, int * keysize_out)
{
    CryptKeyInfo cryptkey;
    
    cryptkey = (CryptKeyInfo)(buffer + offset);

    *keysize_out = cryptkey->keysize;

    return offset;
}

static void crypt_key_info_key_serialization(char * buffer, int offset_input, CryptKeyInfo cryptkey)
{// #lizard forgives
    int option;
    int offset;
    
    option = cryptkey->option;
    offset = offsetof(CryptKeyInfoEntry, udf);

    memcpy(buffer + offset_input, cryptkey, offset);

    offset += offset_input;

    switch (option)
    {
        case CRYPT_KEY_INFO_OPTION_SYMKEY:
        case CRYPT_KEY_INFO_OPTION_SM4:
            break;
        case CRYPT_KEY_INFO_OPTION_ANYKEY:
            {
                if (cryptkey->keypair)
                {
                    memcpy((char*)(buffer + offset), (char*)(cryptkey->keypair->publickey), VARSIZE_ANY(cryptkey->keypair->publickey));
                    offset += VARSIZE_ANY(cryptkey->keypair->publickey);
                    memcpy((char*)(buffer + offset), (char*)(cryptkey->keypair->privatekey), VARSIZE_ANY(cryptkey->keypair->privatekey));
                    offset += VARSIZE_ANY(cryptkey->keypair->privatekey);
                }
                else
                {
                    elog(ERROR, "keypair is NULL, when anykey serialization");
                }
                break;
            }
        case CRYPT_KEY_INFO_OPTION_UDF:
            {
                if (cryptkey->udf)
                {
                    memcpy((char*)(buffer + offset), (char*)&(cryptkey->udf->encrypt_oid), sizeof(Oid));
                    offset += sizeof(Oid);
                    memcpy((char*)(buffer + offset), (char*)&(cryptkey->udf->decrypt_oid), sizeof(Oid));
                    offset += sizeof(Oid);
                    memcpy((char*)(buffer + offset), (char*)cryptkey->udf->encrypt_prosrc, VARSIZE_ANY(cryptkey->udf->encrypt_prosrc));
                    offset += VARSIZE_ANY(cryptkey->udf->encrypt_prosrc);
                    memcpy((char*)(buffer + offset), (char*)(cryptkey->udf->encrypt_probin), VARSIZE_ANY(cryptkey->udf->encrypt_probin));
                    offset += VARSIZE_ANY(cryptkey->udf->encrypt_probin);
                    memcpy((char*)(buffer + offset), (char*)(cryptkey->udf->decrypt_prosrc), VARSIZE_ANY(cryptkey->udf->decrypt_prosrc));
                    offset += VARSIZE_ANY(cryptkey->udf->decrypt_prosrc);
                    memcpy((char*)(buffer + offset), (char*)(cryptkey->udf->decrypt_probin), VARSIZE_ANY(cryptkey->udf->decrypt_probin));
                    offset += VARSIZE_ANY(cryptkey->udf->decrypt_probin);
                }
                else
                {
                    elog(ERROR, "udf is NULL, when udf serialization");
                }
            
                if (cryptkey->keypair)
                {
                    memcpy((char*)(buffer + offset), (char*)(cryptkey->keypair->publickey), VARSIZE_ANY(cryptkey->keypair->publickey));
                    offset += VARSIZE_ANY(cryptkey->keypair->publickey);
                    memcpy((char*)(buffer + offset), (char*)(cryptkey->keypair->privatekey), VARSIZE_ANY(cryptkey->keypair->privatekey));
                    offset += VARSIZE_ANY(cryptkey->keypair->privatekey);
                }
                else
                {
                    elog(ERROR, "keypair is NULL, when udf serialization");
                }
                break;
            }   
        default:
            elog(ERROR, "unknown option, when serialize key");
            break;
    }

    return;
}

static void crypt_key_info_key_deserialization(char * buffer, CryptKeyInfo cryptkey_output)
{// #lizard forgives
    int           option;
    int           offset;

    /* clear base */
    crypt_key_info_init(cryptkey_output);

    /* copy common part */
    offset  = offsetof(CryptKeyInfoEntry, udf);
    memcpy((char*)cryptkey_output, (char*)buffer, offset);

    /* get useful option */
    option  = cryptkey_output->option;

    switch (option)
    {
        case CRYPT_KEY_INFO_OPTION_SYMKEY:
        case CRYPT_KEY_INFO_OPTION_SM4:
            break;
        case CRYPT_KEY_INFO_OPTION_ANYKEY:
            {
                if (cryptkey_output->keypair)
                {
                    memcpy((char*)(cryptkey_output->keypair->publickey), (char*)(buffer + offset), VARSIZE_ANY(buffer+offset));
                    offset = offset + VARSIZE_ANY(buffer+offset);
                    memcpy((char*)(cryptkey_output->keypair->privatekey), (char*)(buffer + offset), VARSIZE_ANY(buffer+offset));
                    offset = offset + VARSIZE_ANY(buffer+offset);
                }
                else
                {
                    elog(ERROR, "keypair is NULL, when anykey deserialization");
                }
                break;
            }
        case CRYPT_KEY_INFO_OPTION_UDF:
            {
                if (cryptkey_output->udf)
                {
                    memcpy((char*)&(cryptkey_output->udf->encrypt_oid), (char*)(buffer + offset), sizeof(Oid));
                    offset = offset + sizeof(Oid);
                    memcpy((char*)&(cryptkey_output->udf->decrypt_oid), (char*)(buffer + offset), sizeof(Oid));
                    offset = offset + sizeof(Oid);
                    memcpy((char*)(cryptkey_output->udf->encrypt_prosrc), (char*)(buffer + offset), VARSIZE_ANY(buffer+offset));
                    offset = offset + VARSIZE_ANY(buffer+offset);
                    memcpy((char*)(cryptkey_output->udf->encrypt_probin), (char*)(buffer + offset), VARSIZE_ANY(buffer+offset));
                    offset = offset + VARSIZE_ANY(buffer+offset);
                    memcpy((char*)(cryptkey_output->udf->decrypt_prosrc), (char*)(buffer + offset), VARSIZE_ANY(buffer+offset));
                    offset = offset + VARSIZE_ANY(buffer+offset);
                    memcpy((char*)(cryptkey_output->udf->decrypt_probin), (char*)(buffer + offset), VARSIZE_ANY(buffer+offset));
                    offset = offset + VARSIZE_ANY(buffer+offset);
                }
                else
                {
                    elog(ERROR, "udf is NULL, when udf deserialization");
                }
                
                if (cryptkey_output->keypair)
                {
                    memcpy((char*)(cryptkey_output->keypair->publickey), (char*)(buffer + offset), VARSIZE_ANY(buffer+offset));
                    offset = offset + VARSIZE_ANY(buffer+offset);
                    memcpy((char*)(cryptkey_output->keypair->privatekey), (char*)(buffer + offset), VARSIZE_ANY(buffer+offset));
                    offset = offset + VARSIZE_ANY(buffer+offset);
                }
                else
                {
                    elog(ERROR, "keypair is NULL, when udf deserialization");
                }
                break;
            }
        default:
            elog(ERROR, "unknown crypt key info:%d ", option);
            break;
    }
    
    return;
}




#endif

#if MARK("rel crypt manager")

#define REL_CRYPT_MAPFILE_BUF_SIZE  (4096 - VARHDRSZ)
#define REL_CRYPT_MAPFILE_PAGE_SIZE 4096

static int rel_crypt_hash_key_cmp (const void *key1, const void *key2, Size keysize);
static void rel_crypt_mapfile_check(char * buffer, RelCryptMapFile ** map_output);
static void rel_crypt_write_mapfile_init(RelCryptMapFile *map);
static void rel_crypt_load_default(void);


void rel_crypt_redo(XLogReaderState *record)
{
    uint8                 info;
    static CryptKeyInfo  cryptkey;
    char                *buffer;
    
    if (NULL == cryptkey)
    {
        cryptkey = crypt_key_info_alloc(CRYPT_KEY_INFO_OPTION_UDF);
    }
    
    info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    
    switch(info & XLR_RMGR_INFO_MASK)
    {
        case XLOG_CRYPT_KEY_INSERT:
            {   
                CryptKeyInfo  xlrec;
                
                buffer =  XLogRecGetData(record);
                crypt_key_info_key_deserialization(buffer, cryptkey);

                if (g_enable_crypt_debug)                
                {   
                    xlrec   = (CryptKeyInfo)buffer;
                    
                    elog(LOG, "CRYPT_KEY_INSERT, redo XLOG_CRYPT_KEY_INSERT, algo_id:%d, option:%d, size:%d, option_args:%s", 
                        xlrec->algo_id, xlrec->option, xlrec->keysize, VARDATA_ANY(xlrec->option_args));
                }
                crypt_key_info_hash_insert(cryptkey, false, false);
                
                break;
            }
        case XLOG_CRYPT_KEY_DELETE:
            elog(ERROR, "xlog type is comming, info:%u", XLOG_CRYPT_KEY_DELETE);
            break;
        case XLOG_REL_CRYPT_INSERT:
            {
                xl_rel_crypt_insert * xlrec;
                xlrec = (xl_rel_crypt_insert *) XLogRecGetData(record);
                if (g_enable_crypt_debug)                
                {
                    elog(LOG, "REL_CRYPT_INSERT, redo XLOG_REL_CRYPT_INSERT, relfilenode:%d:%d:%d, algo_id:%d", 
                        xlrec->rnode.dbNode, xlrec->rnode.spcNode, xlrec->rnode.relNode,
                        xlrec->algo_id);
                }
                rel_crypt_hash_insert(&(xlrec->rnode), xlrec->algo_id, false, false);
                
                break;
            }
        case XLOG_REL_CRYPT_DELETE:
            {
        	    xl_rel_crypt_delete *xlrec;
        	    xlrec = (xl_rel_crypt_delete *) XLogRecGetData(record);
        	    if (g_enable_crypt_debug)
	            {
        	    	elog(LOG, "REL_CRYPT_DELETE, redo XLOG_REL_CRYPT_DELETE, relfilenode:%d:%d:%d, algo_id:%d",
        	    			xlrec->rnode.dbNode, xlrec->rnode.spcNode, xlrec->rnode.relNode,
        	    			xlrec->algo_id);
	            }
        	    rel_crypt_hash_delete(&(xlrec->rnode), false);
            break;
            }
        default:
            elog(ERROR, "recrypt redo, unknown info, info:%u", info & XLR_RMGR_INFO_MASK);
            break;
    }

    return;
}

void rel_cyprt_hash_init(void)
{
    HASHCTL    info;
    bool    found;
    int     i;
    /* assume no locking is needed yet */

    /* BufferTag maps to Buffer */
    info.keysize    = sizeof(RelFileNode);
    info.entrysize  = sizeof(RelCryptEntry);
    info.num_partitions = REL_CRYPT_HASHTABLE_NUM_PARTITIONS;
    info.match = rel_crypt_hash_key_cmp;
    g_rel_crypt_hash = ShmemInitHash("relation crypt table",
                                  REL_CRYPT_HASHTABLE_INIT_SIZE, 
                                  REL_CRYPT_HASHTABLE_MAX_SIZE,
                                  &info,
                                  HASH_ELEM  | HASH_BLOBS | HASH_PARTITION | HASH_COMPARE);

    g_rel_crypt_lock = (RelCryptLockData *) ShmemInitStruct("global relation crypt data",
                                                      MAXALIGN64(sizeof(RelCryptLockData)),
                                                      &found);
    if (false == found)
    {
        g_rel_crypt_lock->lwlock_tranche_id = LWTRANCHE_REL_CRYPT_LOCK;
        
        for (i = 0; i < REL_CRYPT_HASHTABLE_NUM_PARTITIONS; i++)
        {
            LWLockInitialize(&(g_rel_crypt_lock->locks[i].lock), g_rel_crypt_lock->lwlock_tranche_id);
        }
        
        LWLockRegisterTranche(g_rel_crypt_lock->lwlock_tranche_id, "rel crypt lock");
    }

    return;
}
Size rel_crypt_hash_shmem_size(void)
{
    return add_size(MAXALIGN64(hash_estimate_size(REL_CRYPT_HASHTABLE_MAX_SIZE, sizeof(RelCryptEntry))), 
                    MAXALIGN64(sizeof(RelCryptLockData)));
}

static int rel_crypt_hash_key_cmp (const void *key1, const void *key2, Size keysize)
{
    const RelFileNode *tagPtr1 = key1, *tagPtr2 = key2;

	if (tagPtr1->relNode == tagPtr2->relNode
        && tagPtr1->dbNode == tagPtr2->dbNode 
        && tagPtr1->spcNode == tagPtr2->spcNode )
        return 0;

    return 1;
}

/*
 * this function is used to remove hash elem
 *
 * if write_wal is true, remove action will write wal
 */
void remove_rel_crypt_hash_elem(RelCrypt relCrypt, bool write_wal)
{
	if (relCrypt != NULL)
	{
		/*
		 * if the algo_id is invalid, skip
		 */
		if (relCrypt->algo_id == TRANSP_CRYPT_INVALID_ALGORITHM_ID)
		{
			return;
		}
		/*
		 * do remove the rnode and algo_id map in rel_crypt_hash table
	     */
		rel_crypt_hash_delete(&(relCrypt->relfilenode), write_wal);
	}
}

/*
 * do delete rel crypt hash elem about a rnode
 */
void rel_crypt_hash_delete(RelFileNode *rnode, bool write_wal)
{
	RelCrypt relCrypt;
	bool     found = false;

	uint32 hashcode;
	int 	 partitionno;
	LWLock	*partitionLock;

	hashcode = rel_crypt_hash_code(rnode);
	partitionno   = rel_crypt_hash_partition(hashcode);
	partitionLock = rel_crypt_get_partition_lock(partitionno);

	LWLockAcquire(partitionLock, LW_EXCLUSIVE);

	relCrypt = (RelCrypt) hash_search_with_hash_value(g_rel_crypt_hash,
	                                                  (void *) rnode,
	                                                  hashcode,
	                                                  HASH_REMOVE,
	                                                  &found);

	if (found)
	{
		/*
		 * need to flush crypt map in next checkpoint
		 */
		RequestFlushRelcryptMap();
	}

	/*
	 * Critical section
	 */
	if (found && write_wal)
	{
		xl_rel_crypt_delete xlrec;
		XLogRecPtr	lsn;

		/* now errors are fatal ... */
		START_CRIT_SECTION();

		xlrec.rnode   = relCrypt->relfilenode;
		xlrec.algo_id = relCrypt->algo_id;

		XLogBeginInsert();
		XLogRegisterData((char *) (&xlrec), sizeof(xl_rel_crypt_delete));

		lsn = XLogInsert(RM_REL_CRYPT_ID, XLOG_REL_CRYPT_DELETE);

		/* As always, WAL must hit the disk before the data update does */
		XLogFlush(lsn);

		END_CRIT_SECTION();
	}

	LWLockRelease(partitionLock);
}

void rel_crypt_hash_insert(RelFileNode * rnode, AlgoId algo_id, bool write_wal, bool in_building_procedure)
{// #lizard forgives
    RelCrypt relcrypt;
    bool     found;
    uint32   hashcode;
    int      partitionno;
    LWLock    *partitionLock;

    hashcode      = rel_crypt_hash_code(rnode);
    partitionno   = rel_crypt_hash_partition(hashcode);
    partitionLock = rel_crypt_get_partition_lock(partitionno);

    LWLockAcquire(partitionLock, LW_EXCLUSIVE);

    relcrypt = (RelCrypt)hash_search_with_hash_value(g_rel_crypt_hash,
                                                    (void *) rnode,
                                                    hashcode,
                                                    HASH_ENTER,
                                                    &found);

    if (false == found)
    {
        relcrypt->algo_id        = algo_id;
        if ((false == in_building_procedure) 
            && (CRYPT_DEFAULT_INNER_ALGO_ID != algo_id))
        {
            /* need to flush crypt map */
            RequestFlushRelcryptMap();
        }
    }

    /* Critical section */
    if (write_wal && !found && (CRYPT_DEFAULT_INNER_ALGO_ID != algo_id))
    {
        xl_rel_crypt_insert xlrec;
        XLogRecPtr    lsn;

        /* now errors are fatal ... */
        START_CRIT_SECTION();

        xlrec.rnode   = *rnode;
        xlrec.algo_id = algo_id;

        XLogBeginInsert();
        XLogRegisterData((char *) (&xlrec), sizeof(xl_rel_crypt_insert));

        lsn = XLogInsert(RM_REL_CRYPT_ID, XLOG_REL_CRYPT_INSERT);

        /* As always, WAL must hit the disk before the data update does */
        XLogFlush(lsn);
        
        END_CRIT_SECTION();
    }

    /* release lock here, in case of xlog insert by other concurrently */
    LWLockRelease(partitionLock);    

    if (g_enable_crypt_debug)                
    {
        if (write_wal)
        {
            if (!in_building_procedure)
            {
                if (!found)
                {
                    if (CRYPT_DEFAULT_INNER_ALGO_ID != algo_id)
                    {
                        elog(LOG, "REL_CRYPT_INSERT, new rel crypt insert and need checkpoint to flush later, "
                                    "relfilenode:%d:%d:%d, algo_id:%d", 
                                    rnode->dbNode, rnode->spcNode, rnode->relNode, algo_id);
                    }
                    else
                    {
                        elog(ERROR, "REL_CRYPT_INSERT, default rel crypt insert, shoule NOT happen, postion 1, "
                                    "relfilenode:%d:%d:%d, algo_id:%d", 
                                    rnode->dbNode, rnode->spcNode, rnode->relNode, algo_id);
                    }
                }
                else
                {
                    if (CRYPT_DEFAULT_INNER_ALGO_ID != algo_id)
                    {
                        elog(ERROR, "REL_CRYPT_INSERT, new rel crypt insert concurrently, shoule NOT happen, postion 2, "
                                    "relfilenode:%d:%d:%d, algo_id:%d", 
                                    rnode->dbNode, rnode->spcNode, rnode->relNode, algo_id);
                    }
                    else
                    {
                        elog(ERROR, "REL_CRYPT_INSERT, default rel crypt insert, shoule NOT happen, postion 3, "
                                    "relfilenode:%d:%d:%d, algo_id:%d", 
                                    rnode->dbNode, rnode->spcNode, rnode->relNode, algo_id);
                    }
                }
            }
            else
            {
                elog(ERROR, "REL_CRYPT_INSERT, rel crypt insert, shoule NOT happen, postion 4, "
                                    "relfilenode:%d:%d:%d, algo_id:%d", 
                                    rnode->dbNode, rnode->spcNode, rnode->relNode, algo_id);
            }
        }
        else
        {
            if (!in_building_procedure)
            {
                if (!found)
                {
                    if (CRYPT_DEFAULT_INNER_ALGO_ID != algo_id)
                    {
                        elog(LOG, "REL_CRYPT_INSERT, new rel crypt insert in REDO procedure, "
                                    "checkpoint needs to flush later, relfilenode:%d:%d:%d, algo_id:%d", 
                                    rnode->dbNode, rnode->spcNode, rnode->relNode, algo_id);
                    }
                    else
                    {
                        elog(ERROR, "REL_CRYPT_INSERT, default rel crypt insert, shoule NOT happen, postion 5, "
                                    "relfilenode:%d:%d:%d, algo_id:%d", 
                                    rnode->dbNode, rnode->spcNode, rnode->relNode, algo_id);
                    }
                }
                else
                {
                    if (CRYPT_DEFAULT_INNER_ALGO_ID != algo_id)
                    {
                        elog(WARNING, "REL_CRYPT_INSERT, new rel crypt insert concurrently, "
                                    "while there shoule NO DUPLICATE xlog insert, postion 6, "
                                    "relfilenode:%d:%d:%d, algo_id:%d", 
                                    rnode->dbNode, rnode->spcNode, rnode->relNode,algo_id);
                    }
                    else
                    {
                        elog(ERROR, "REL_CRYPT_INSERT, default rel crypt insert, shoule NOT happen, postion 7, "
                                    "relfilenode:%d:%d:%d, algo_id:%d", 
                                    rnode->dbNode, rnode->spcNode, rnode->relNode,algo_id);
                    }
                }
            }
            else
            {
                if (!found)
                {
                    if (CRYPT_DEFAULT_INNER_ALGO_ID != algo_id)
                    {
                        elog(LOG, "REL_CRYPT_INSERT, new rel crypt insert, here we are loading relcrypt map, "
                                    "and keep in shmem hash, relfilenode:%d:%d:%d, algo_id:%d", 
                                    rnode->dbNode, rnode->spcNode, rnode->relNode, algo_id);
                    }
                    else
                    {
                        elog(LOG, "REL_CRYPT_INSERT, default rel crypt insert, first open algorithm relation, "
                                    "maybe dboid is invalid, relfilenode:%d:%d:%d, algo_id:%d", 
                                    rnode->dbNode, rnode->spcNode, rnode->relNode, algo_id);
                    }
                }
                else
                {
                    if (CRYPT_DEFAULT_INNER_ALGO_ID != algo_id)
                    {
                        elog(ERROR, "REL_CRYPT_INSERT, new rel crypt insert concurrently, "
                                    "while there shoule NO DUPLICATE in rel crypt map, postion 8, "
                                    "relfilenode:%d:%d:%d, algo_id:%d", 
                                    rnode->dbNode, rnode->spcNode, rnode->relNode,algo_id);
                    }
                    else
                    {
                        elog(ERROR, "REL_CRYPT_INSERT, default rel crypt insert, shoule NOT happen, postion 9, "
                                    "relfilenode:%d:%d:%d, algo_id:%d", 
                                    rnode->dbNode, rnode->spcNode, rnode->relNode, algo_id);
                    }
                }
            }
        }
    }

    return;
}
bool rel_crypt_hash_lookup(RelFileNode * rnode, RelCrypt relcrypt_ret)
{
    RelCrypt relcrypt = NULL;
    uint32   hashcode;
    int      partitionno;
    LWLock    *partitionLock;
    bool     found;

    hashcode      = rel_crypt_hash_code(rnode);
    partitionno   = rel_crypt_hash_partition(hashcode);
    partitionLock = rel_crypt_get_partition_lock(partitionno);

    LWLockAcquire(partitionLock, LW_SHARED);
    relcrypt = (RelCrypt)hash_search_with_hash_value(g_rel_crypt_hash,
                                                    (void *) rnode,
                                                    hashcode,
                                                    HASH_FIND,
                                                    NULL);
    found = false;
    if (NULL != relcrypt)
    {
        found = true;
        if (NULL != relcrypt_ret)
        {
            memcpy((char*)relcrypt_ret, (char*)relcrypt, sizeof(RelCryptEntry));
        }
    }
    LWLockRelease(partitionLock);

    return found;
}
static void rel_crypt_mapfile_check(char * buffer, RelCryptMapFile ** map_output)
{
    pg_crc32c   crc;
    RelCryptMapFile * map;
    text            * decrypt_buf;

    map = *map_output;
    
    /* decrypt map file */
    decrypt_buf = decrypt_procedure(CRYPT_DEFAULT_INNER_ALGO_ID, (text*)buffer, INVALID_CONTEXT_LENGTH);
    memcpy((char*)map, VARDATA_ANY(decrypt_buf), VARSIZE_ANY_EXHDR(decrypt_buf));

    crypt_free(decrypt_buf);
    
    /* check for correct magic number, etc */
    if (map->magic != REL_CRYPT_MAP_FILEMAGIC ||
        map->element_count < 0 ||
        map->element_count > REL_CRYPT_MAP_ELEMENT_CNT_PER_PAGE)
    {
        ereport(FATAL,(errmsg("rel crypt mapping file contains invalid data")));
    }

    /* verify the CRC */
    INIT_CRC32C(crc);
    COMP_CRC32C(crc, (char *) map + sizeof(int)*2, REL_CRYPT_MAPFILE_BUF_SIZE - sizeof(int)*2);
    FIN_CRC32C(crc);

    if (!EQ_CRC32C(crc, map->crc))
    {
        ereport(FATAL,
                (errmsg("rel crypt mapping file contains incorrect checksum")));
    }

    return;
}

static void rel_crypt_load_default(void)
{
    RelFileNode rnode;

    rnode.dbNode  = 0;
    rnode.spcNode = GLOBALTABLESPACE_OID;
    rnode.relNode = TransparentCryptPolicyAlgorithmId;

    if (g_enable_crypt_debug)                
    {
        elog(LOG, "REL_CRYPT_INSERT, when postmaster start, load default, relfilenode:%d:%d:%d, algo_id:%d", 
                        rnode.dbNode, rnode.spcNode, rnode.relNode,
                        CRYPT_DEFAULT_INNER_ALGO_ID);
    }
    rel_crypt_hash_insert(&rnode, CRYPT_DEFAULT_INNER_ALGO_ID, false, true);

    return;
}

void rel_crypt_load_mapfile(void)
{// #lizard forgives
    RelCryptMapFile *map;
    char       *buffer;
    char        mapfilename[MAXPGPATH];
    
    BufFile    *buffile;
    int         loop;
    int         len;

    /*STEP 1, when postmaster startup, load rnode of pg_transparent_crypt_policy_algorithm */
    rel_crypt_load_default();

    /*STEP 2*/
    snprintf(mapfilename, sizeof(mapfilename), "%s/%s", "global", REL_CRYPT_MAP_FILENAME);
    
    buffile = BufFileOpen(mapfilename, O_RDONLY | PG_BINARY, S_IRUSR | S_IWUSR, true, LOG);
    if (NULL == buffile)
    {
        return;
    }
    
    buffer = palloc0(REL_CRYPT_MAPFILE_PAGE_SIZE);
    map    = palloc(REL_CRYPT_MAPFILE_PAGE_SIZE);

    /* ---- TODO ---- there should be loop to read map file */
    do{
        pgstat_report_wait_start(WAIT_EVENT_REL_CRYPT_MAP_READ);
        len = BufFileRead(buffile, buffer, REL_CRYPT_MAPFILE_PAGE_SIZE);
        if (0 == len)
        {
            /* file is empty */
            pgstat_report_wait_end();
            break;
        }
        
        if (len != REL_CRYPT_MAPFILE_PAGE_SIZE)
        {
            elog(ERROR, "could not read datalen:%d in relcrypt bufFile.", REL_CRYPT_MAPFILE_PAGE_SIZE);
        }
        pgstat_report_wait_end();

        /* get one valid buffer */
        rel_crypt_mapfile_check(buffer, &map);

        if (g_enable_crypt_debug)                
        {
            elog(LOG, "REL_CRYPT_INSERT, postmaster is starting up, ----load begin----");
        }
        for (loop = 0; loop < map->element_count; loop++)
        {
            rel_crypt_hash_insert(&(map->elements[loop].relfilenode), map->elements[loop].algo_id, false, true);
        }
        if (g_enable_crypt_debug)                
        {
            elog(LOG, "REL_CRYPT_INSERT, postmaster is starting up, ----load end----");
        }
    }while(1);
    /* ---- TODO ---- there should be loop to read map file */

    if (BufFileReadDone(buffile))
    {
        BufFileClose(buffile);
    }
    else
    {
        elog(ERROR, "no data in relcrypt buffile while reading data.");
    }
    pfree(map);
    pfree(buffer);
    
    return;
}

static void rel_crypt_write_mapfile_init(RelCryptMapFile *map)
{
    memset(map, 0x0, REL_CRYPT_MAPFILE_PAGE_SIZE);
    
    map->magic = REL_CRYPT_MAP_FILEMAGIC;

    return;
}

static void rel_crypt_write_mapfile_post(RelCryptMapFile *map, int element_cnt, BufFile * buffile)
{
    static text * need_encrypt_text = NULL;
    text * encrypt_text;
    
    if (NULL == need_encrypt_text)
    {
        need_encrypt_text = palloc(REL_CRYPT_MAPFILE_PAGE_SIZE);
    }

    /* fill header */
    map->element_count = element_cnt;

    /* crc procedure */
    INIT_CRC32C(map->crc);
    COMP_CRC32C(map->crc, (char *) map + sizeof(int)*2, REL_CRYPT_MAPFILE_BUF_SIZE - sizeof(int)*2);
    FIN_CRC32C(map->crc);
    
    /* encrypt procedure */
    memset((char*)need_encrypt_text, 0x0, REL_CRYPT_MAPFILE_PAGE_SIZE);
    
    SET_VARSIZE(need_encrypt_text, REL_CRYPT_MAPFILE_PAGE_SIZE);
    memcpy(VARDATA(need_encrypt_text), (char*)map, REL_CRYPT_MAPFILE_BUF_SIZE);
    
    /* use default algo, so no need to consider about page_new_output */
    encrypt_text = encrypt_procedure(CRYPT_DEFAULT_INNER_ALGO_ID, need_encrypt_text, NULL);

    /* this should not happen */
    if (VARSIZE_ANY(encrypt_text) > REL_CRYPT_MAPFILE_PAGE_SIZE)
    {
        elog(ERROR, "encyrypt rel crypt mapfile page is oversize:%lu", VARSIZE_ANY(encrypt_text));
    }
    
    /* flush down */
    pgstat_report_wait_start(WAIT_EVENT_REL_CRYPT_MAP_WRITE);
    BufFileWrite(buffile, encrypt_text, REL_CRYPT_MAPFILE_PAGE_SIZE);
    pgstat_report_wait_end();

    crypt_free(encrypt_text);
    
    return;
}

/*
 * if is_backup is true, it means to backup the pg_rel_crypt.map
 * to pg_rel_crypt.map.backup, if is_backup is false, it means
 * flush the data to disk
 */
void rel_crypt_write_mapfile(bool is_backup)
{
    int              loop;
    int              lock_loop;
    char            *mapfilename;
    char            *mapfilename_new;
    HASH_SEQ_STATUS  status;
    RelCryptEntry   *relcrypt;
    BufFile         *buffile;
    static RelCryptMapFile *map = NULL;
    
    /*
     * check the change from last flush
     */
    if (false == NeedFlushRelcryptMap())
    {
        return;
    }
    
    mapfilename     = palloc0(MAXPGPATH);
    mapfilename_new = palloc0(MAXPGPATH);

    /*
     * if backup the file, the filename will be renamed as pg_rel_crypt.map.backup
     * else the file named as pg_rel_crypt.map
     */
    if (is_backup)
    {
    	snprintf(mapfilename, MAXPGPATH, "%s/%s.backup", "global", REL_CRYPT_MAP_FILENAME);
    }
    else
    {
    snprintf(mapfilename,     MAXPGPATH, "%s/%s",    "global", REL_CRYPT_MAP_FILENAME);
    }
    snprintf(mapfilename_new, MAXPGPATH, "%s/%s.%d", "global", REL_CRYPT_MAP_FILENAME, MyProcPid);

    buffile = BufFileOpen(mapfilename_new, (O_WRONLY|O_CREAT|PG_BINARY), (S_IRUSR|S_IWUSR), true, ERROR);

    /* prepare for buffer */
    if (NULL == map)
    {
        map = palloc(REL_CRYPT_MAPFILE_PAGE_SIZE);
    }

    rel_crypt_write_mapfile_init(map);
    
    /* lock all partition lock*/
    for (lock_loop = 0; lock_loop < REL_CRYPT_HASHTABLE_NUM_PARTITIONS; lock_loop++)
    {
        LWLockAcquire(rel_crypt_get_partition_lock(lock_loop), LW_SHARED);
    }  

    do{
        loop = 0;

        hash_seq_init(&status, g_rel_crypt_hash);

        while ((relcrypt = (RelCryptEntry *) hash_seq_search(&status)) != NULL)
        {
            /* skip default algo id */
            if (relcrypt->algo_id == CRYPT_DEFAULT_INNER_ALGO_ID)
            {
                continue;
            }
            
            map->elements[loop].relfilenode = relcrypt->relfilenode;
            map->elements[loop].algo_id     = relcrypt->algo_id;
            loop++;
            if (loop >= REL_CRYPT_MAP_ELEMENT_CNT_PER_PAGE)
            {
                /* this buffer is full, flush it */
                rel_crypt_write_mapfile_post(map, loop, buffile);
                
                /* after flush one buffer, init buffer again */
                loop = 0;
                rel_crypt_write_mapfile_init(map);
            }
        }

        /* hash is empty, flush buffer */
        rel_crypt_write_mapfile_post(map, loop, buffile);

        pgstat_report_wait_start(WAIT_EVENT_REL_CRYPT_MAP_SYNC);
        BufFileClose(buffile);
        pgstat_report_wait_end();
        
    }while(0);

    /* atom action */
    rename(mapfilename_new, mapfilename);   

    /* release all */
    for (lock_loop = REL_CRYPT_HASHTABLE_NUM_PARTITIONS - 1; lock_loop >= 0; lock_loop--)
    {
        LWLockRelease(rel_crypt_get_partition_lock(lock_loop));
    } 

    ClearRequestFlushRelcryptMap();
    
    if (g_enable_crypt_debug)                
    {
        elog(LOG, "REL_CRYPT_INSERT, rel crypt hash changed, finish flushing");
    }

    if (mapfilename_new)
    {
        pfree(mapfilename_new);
    }

    if (mapfilename)
    {
        pfree(mapfilename);
    }
    
    return;
}

#endif

bool userid_is_mls_user(Oid userid)
{
    if (DEFAULT_ROLE_MLS_SYS_USERID == userid)
        return true;

    if(pg_strncasecmp(GetUserNameFromId(userid,false),
                      MLS_USER_PREFIX,
                      MLS_USER_PREFIX_LEN) == 0)
        return true;
    return false;
}

bool is_mls_user(void)
{
    return g_is_mls_user;
}

bool is_mls_root_user(void)
{
    if (DEFAULT_ROLE_MLS_SYS_USERID == GetAuthenticatedUserId())
        return true;

    return false;
}

#if MARK("extern")

Datum pg_rel_crypt_hash_dump(PG_FUNCTION_ARGS)
{// #lizard forgives
#define PG_REL_CRYPT_DUMP_COLUMN_NUM    5
    ReturnSetInfo * rsinfo;
    TupleDesc        tupdesc;
    Tuplestorestate*tupstore;
    MemoryContext   per_query_ctx;
    MemoryContext   oldcontext;
    int             lock_loop;
    int             loop;
    HASH_SEQ_STATUS status;
    RelCryptEntry * relcrypt;
    Datum            values[PG_REL_CRYPT_DUMP_COLUMN_NUM];
    bool            nulls[PG_REL_CRYPT_DUMP_COLUMN_NUM];

    if (!is_mls_user())
    {
        elog(ERROR, "execute by mls user please");
    }

    rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

    /* check to see if caller supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("set-valued function called in context that cannot accept a set")));
    if (!(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("materialize mode required, but it is not " \
                        "allowed in this context")));

    /* Build a tuple descriptor for our result type */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "return type must be a row type");

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    tupstore = tuplestore_begin_heap(true, false, work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;

    MemoryContextSwitchTo(oldcontext);

    memset(nulls, false, PG_REL_CRYPT_DUMP_COLUMN_NUM);

    /* lock all partition lock*/
    for (lock_loop = 0; lock_loop < REL_CRYPT_HASHTABLE_NUM_PARTITIONS; lock_loop++)
    {
        LWLockAcquire(rel_crypt_get_partition_lock(lock_loop), LW_SHARED);
    }  

    hash_seq_init(&status, g_rel_crypt_hash);
    loop = 0;
    while ((relcrypt = (RelCryptEntry *) hash_seq_search(&status)) != NULL)
    {
        loop++;
        values[0] = Int32GetDatum(loop);
        values[1] = UInt32GetDatum(relcrypt->relfilenode.dbNode);
        values[2] = UInt32GetDatum(relcrypt->relfilenode.spcNode);
        values[3] = UInt32GetDatum(relcrypt->relfilenode.relNode);
        values[4] = Int16GetDatum(relcrypt->algo_id);
        tuplestore_putvalues(tupstore, tupdesc, values, nulls);
    }
    
    /* release all */
    for (lock_loop = REL_CRYPT_HASHTABLE_NUM_PARTITIONS - 1; lock_loop >= 0; lock_loop--)
    {
        LWLockRelease(rel_crypt_get_partition_lock(lock_loop));
    } 

    /* clean up and return the tuplestore */
    tuplestore_donestoring(tupstore);

    return (Datum) 0;
}

Datum pg_crypt_key_hash_dump(PG_FUNCTION_ARGS)
{// #lizard forgives
#define PG_CRYPT_KEY_DUMP_COLUMN_NUM    13
    ReturnSetInfo * rsinfo;
    TupleDesc       tupdesc;
    Tuplestorestate*tupstore;
    MemoryContext   per_query_ctx;
    MemoryContext   oldcontext;
    int             lock_loop;
    int             loop;
    HASH_SEQ_STATUS status;
    CryptKeyInfo    cryptkey;
    Datum           values[PG_CRYPT_KEY_DUMP_COLUMN_NUM];
    bool            nulls[PG_CRYPT_KEY_DUMP_COLUMN_NUM];

    if (!is_mls_user())
    {
        elog(ERROR, "execute by mls user please");
    }

    rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

    /* check to see if caller supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("set-valued function called in context that cannot accept a set")));
    if (!(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("materialize mode required, but it is not " \
                        "allowed in this context")));

    /* Build a tuple descriptor for our result type */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "return type must be a row type");

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    tupstore = tuplestore_begin_heap(true, false, work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;

    MemoryContextSwitchTo(oldcontext);

    memset(nulls, false, PG_CRYPT_KEY_DUMP_COLUMN_NUM);

    /* lock all partition lock*/
    for (lock_loop = 0; lock_loop < CRYPT_KEY_INFO_HASHTABLE_NUM_PARITIONS; lock_loop++)
    {
        LWLockAcquire(crypt_key_info_hash_get_partition_lock(lock_loop), LW_SHARED);
    }  

    hash_seq_init(&status, g_crypt_key_info_hash);
    loop = 0;
    while ((cryptkey = (CryptKeyInfo) hash_seq_search(&status)) != NULL)
    {
        loop++;
        values[0] = Int32GetDatum(loop);
        values[1] = Int16GetDatum(cryptkey->algo_id);
        values[2] = Int16GetDatum(cryptkey->option);
        values[3] = PointerGetDatum(cryptkey->password);

        if (0 != strlen(cryptkey->option_args))
        {
            values[4] = PointerGetDatum(cryptkey->option_args);
            nulls[4]  = false;
        }
        else
        {
            nulls[4]  = true;
        }

        if (cryptkey->udf && CRYPT_KEY_INFO_OPTION_UDF == cryptkey->option)
        {
            values[5]  = UInt32GetDatum(cryptkey->udf->encrypt_oid);
            values[6]  = UInt32GetDatum(cryptkey->udf->decrypt_oid);
            values[7]  = PointerGetDatum(cryptkey->udf->encrypt_prosrc);
            values[8]  = PointerGetDatum(cryptkey->udf->encrypt_probin);
            values[9]  = PointerGetDatum(cryptkey->udf->decrypt_prosrc);
            values[10] = PointerGetDatum(cryptkey->udf->decrypt_probin);

            nulls[5]  = false;
            nulls[6]  = false;
            nulls[7]  = false;
            nulls[8]  = false;
            nulls[9]  = false;
            nulls[10] = false;
        }
        else
        {
            nulls[5]  = true;
            nulls[6]  = true;
            nulls[7]  = true;
            nulls[8]  = true;
            nulls[9]  = true;
            nulls[10] = true;
        }

        if (cryptkey->keypair && CRYPT_KEY_INFO_OPTION_UDF == cryptkey->option)
        {
            values[11] = false;
            if (VARSIZE_ANY(cryptkey->keypair->publickey))
            {
                values[11] = true;
            }

            values[12] = false;
            if (VARSIZE_ANY(cryptkey->keypair->privatekey))
            {
                values[12] = true;
            }
            nulls[11] = false;
            nulls[12] = false;
        }
        else
        {
            nulls[11] = true;
            nulls[12] = true;
        }

        tuplestore_putvalues(tupstore, tupdesc, values, nulls);
    }
    
    /* release all */
    for (lock_loop = CRYPT_KEY_INFO_HASHTABLE_NUM_PARITIONS - 1; lock_loop >= 0; lock_loop--)
    {
        LWLockRelease(crypt_key_info_hash_get_partition_lock(lock_loop));
    }

    /* clean up and return the tuplestore */
    tuplestore_donestoring(tupstore);

    return (Datum) 0;
}

/*
 * Check the relfilenode exist
 */
bool CheckRelFileNodeExists(RelFileNode *rnode)
{
	Oid relid;

	if (rnode != NULL)
	{
		relid = RelidByRelfilenode(rnode->spcNode, rnode->relNode);

		if (OidIsValid(relid))
		{
			return true;
		}
	}

	return false;
}

/*
 * mark the invalid elem in g_rel_crypt_hash to delete
 */
List * MarkRelCryptInvalid(void)
{
	List * result = NIL;
	HASH_SEQ_STATUS status;
	int lock_loop = 0;
	RelCryptEntry *relcrypt;
	bool is_exist = false;

	/* lock all partition lock */
	for (lock_loop = 0; lock_loop < REL_CRYPT_HASHTABLE_NUM_PARTITIONS; lock_loop++)
	{
		LWLockAcquire(rel_crypt_get_partition_lock(lock_loop), LW_SHARED);
	}

	hash_seq_init(&status, g_rel_crypt_hash);
	while ((relcrypt = (RelCryptEntry *) hash_seq_search(&status)) != NULL)
	{
		 /* only deal with current database */
		if (relcrypt->relfilenode.dbNode != MyDatabaseId)
		{
			continue;
		}

		is_exist = CheckRelFileNodeExists(&(relcrypt->relfilenode));
		if (!is_exist)
		{
			elog(DEBUG5, "check relfilenode exist, dbNode:%d, spcNode:%d, relNode:%d",
			     relcrypt->relfilenode.dbNode, relcrypt->relfilenode.spcNode, relcrypt->relfilenode.relNode);
			result = lappend(result, relcrypt);
		}
	}

	/* release all */
	for (lock_loop = REL_CRYPT_HASHTABLE_NUM_PARTITIONS - 1; lock_loop >= 0; lock_loop--)
	{
		LWLockRelease(rel_crypt_get_partition_lock(lock_loop));
	}

	return result;
}

/*
 * do checkpoint to flush crypt map file to disk
 */
void CheckPointRelCrypt(void)
{
    if (g_enable_crypt_debug)
    {
        elog(LOG, "CheckPointRelCrypt check to flush crypt mapfile BEGIN");
    }
    rel_crypt_write_mapfile(false);
    crypt_key_info_write_mapfile();
    if (g_enable_crypt_debug)
    {
        elog(LOG, "CheckPointRelCrypt check to flush crypt mapfile END");
    }
    return;
}

/*
 * if system in startup state, need to flush crypt map file
 */
void StartupReachConsistentState(void)
{
    if (g_enable_crypt_debug)
    {
        elog(LOG, "StartupReachConsistentState check to flush crypt mapfile BEGIN");
    }
    rel_crypt_write_mapfile(false);
    crypt_key_info_write_mapfile();
    if (g_enable_crypt_debug)
    {
        elog(LOG, "StartupReachConsistentState check to flush crypt mapfile END");
    }
    return;
}


#endif

