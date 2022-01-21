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
 * gtm_store.c
 *    GTM storage handling on GTM
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * Portions Copyright (c) 2012-2018 TBase Development Group
 *
 *
 * IDENTIFICATION
 *      $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#include <unistd.h>
#include <stdio.h>
#include "gtm/assert.h"
#include "gtm/elog.h"
#include "gtm/gtm.h"
#include "gtm/gtm_client.h"
#include "gtm/gtm_seq.h"
#include "gtm/gtm_serialize.h"
#include "gtm/gtm_standby.h"
#include "gtm/standby_utils.h"
#include "gtm/libpq.h"
#include "gtm/libpq-int.h"
#include "gtm/pqformat.h"
#include "gtm/gtm_backup.h"
#include "gtm/gtm_store.h"
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <signal.h>
#include <gtm/gtm_xlog.h>

#include "gtm/gtm_time.h"

typedef enum
{
    SCAN = 0,
    SNAPSHOT_SCAN,
    SNAPSHOT_SCAN_BEFORE_PREPARE,
    SNAPSHOT_SCAN_AFTER_PREPARE,
    SNAPSHOT_SCAN_AFTER_COMMITTED,
    SNAPSHOT_SCAN_AFTER_ABORT,
    SEQ_SCAN,
    BITMAP_SCAN,
    INDEX_SCAN,
    PARALLEL_SEQ_SCAN,
    PARALLEL_BITMAP_SCAN,
    PARALLEL_INDEX_SCAN,
    INSERT_TUPLES
}ScanType;

struct scan_enum_name
{
    int type;
    char *name;
};

extern bool enable_gtm_sequence_debug;
extern bool first_init;

int32 g_GTMStoreMapFile = -1;
char *g_GTMStoreMapAddr = NULL;
size_t g_GTMStoreSize   = 0;
FILE * g_GTMDebugLogFile = NULL;
FILE * g_GTMDebugScanLogFile = NULL;
char *g_GTMDebugStoreAddr = NULL;
size_t g_GTMDebugStoreSize = 0;
GTMControlHeader   *g_GTM_Store_Header     = NULL;

GTM_RWLock            *g_GTM_Store_Head_Lock = NULL;

static GTM_RWLock            *g_GTM_Seq_Store_Hash_Lock = NULL;
static GTM_RWLock            *g_GTM_Txn_Store_Hash_Lock = NULL;

/* Lock for lock gtm store block. */
static GTM_RWLock             g_GTM_store_lock;
static int                      g_GTM_store_lock_cnt   = 0;

static GTM_StoredHashTable       *g_GTM_Store_SeqHashTab = NULL;
static GTM_StoredHashTable       *g_GTM_Store_TxnHashTab = NULL;
static GTM_StoredSeqInfo         *g_GTM_Store_SeqInfo    = NULL;
static GTM_StoredTransactionInfo *g_GTM_Store_TxnInfo    = NULL;
static GTM_TransactionDebugInfo *g_GTM_TxnDebugInfo = NULL;
static GTMDebugControlHeader    g_GTM_DebugHeader;
static GTM_RWLock                g_GTM_Debug_Lock;
static GTM_RWLock                g_GTM_Scan_Debug_Lock;

#ifdef __XLOG__
GTM_TimerHandle  g_GTM_Backup_Timer;
GTM_RWLock         g_GTM_Backup_Timer_Lock;
#endif

/*
 * Advise:
 * Following table can be formatted using gtm_msg.h definitions.
 */
static struct scan_enum_name scan_type_tab[] =
{
    {SCAN, "scan"},
    {SNAPSHOT_SCAN, "snapshotscan"},
    {SNAPSHOT_SCAN_BEFORE_PREPARE, "scanbeforeprepare"},
    {SNAPSHOT_SCAN_AFTER_PREPARE, "scanafterprepare"},
    {SNAPSHOT_SCAN_AFTER_COMMITTED, "scanaftercommitted"},
    {SNAPSHOT_SCAN_AFTER_ABORT, "scanafterabort"},
    {SEQ_SCAN, "seqscan"},
    {BITMAP_SCAN, "bitmapscan"},
    {INDEX_SCAN, "indexscan"},
    {PARALLEL_SEQ_SCAN, "parallelseqscan"},
    {PARALLEL_BITMAP_SCAN, "parallelbitmapscan"},
    {PARALLEL_INDEX_SCAN, "parallelindexscan"},
    {INSERT_TUPLES, "insert"},
    {-1, NULL}
};

#define GetTxnDebugEntry(i) (GTM_TransactionDebugInfo*) (g_GTM_TxnDebugInfo + (i))
#define GetScanDebugEntry(i) (GTM_TransactionDebugInfo *) (g_GTM_ScanDebugInfo + (i))

#define GetTxnStore(txn) (GTM_StoredTransactionInfo*)(g_GTM_Store_TxnInfo + txn)
#define GetSeqStore(seq) (GTM_StoredSeqInfo*)(g_GTM_Store_SeqInfo + seq)

#define AcquireTxnHashLock(txn, mode) (GTM_RWLockAcquire(g_GTM_Txn_Store_Hash_Lock + txn, mode))
#define ReleaseTxnHashLock(txn) (GTM_RWLockRelease(g_GTM_Txn_Store_Hash_Lock + txn))


#define AcquireSeqHashLock(seq, mode) (GTM_RWLockAcquire(g_GTM_Seq_Store_Hash_Lock + seq, mode))
#define ReleaseSeqHashLock(seq) (GTM_RWLockRelease(g_GTM_Seq_Store_Hash_Lock + seq))

#define GetSeqHashBucket(seq) (g_GTM_Store_SeqHashTab->m_buckets[seq])
#define GetTxnHashBucket(txn) (g_GTM_Store_TxnHashTab->m_buckets[txn])


#define SetSeqHashBucket(bucket, handle) (g_GTM_Store_SeqHashTab->m_buckets[bucket] = handle)
#define SetTxnHashBucket(bucket, handle) (g_GTM_Store_TxnHashTab->m_buckets[bucket] = handle)

#define VALID_SEQ_HANDLE(seq_handle) (seq_handle >= 0 && seq_handle < GTM_MAX_SEQ_NUMBER)
#define VALID_TXN_HANDLE(txn_handle) (txn_handle >= 0 && txn_handle < MAX_PREPARED_TXN)

static GTMStorageHandle GTM_StoreTxnHashSearch(char *gid);
static GTMStorageHandle GTM_StoreSeqHashSearch(char *seq_key, int32 type);
static uint32 GTM_StoreGetHashValue(char *key, int32 len);
static int32 GTM_StoreHeaderRunning(void);
static int32 GTM_StoreHeaderShutdown(void);
static GTMStorageHandle GTM_StoreAllocSeq(char *key, int32 key_type);
static void RebuildTransactionList();
static void RebuildSequenceList();
#ifdef __XLOG__
static int32 GTM_InitStoreSyncHeader(bool needLsn);
#endif
static int32 GTM_StoreSyncHeader(bool needLsn);
static int32 GTM_StoreSyncSeq(GTMStorageHandle handle);
static int32 GTM_ResetSyncSeq(GTMStorageHandle handle);
static int32 GTM_StoreSyncTxn(GTMStorageHandle handle);
static void  GTM_StoreInitRawTxn(GTM_StoredTransactionInfo *txn);
static void  GTM_StoreInitRawSeq(GTM_StoredSeqInfo *seq);

static int32 GTM_StoreSyncTxnHashBucket(int32 bucket);
static int32 GTM_StoreSyncSeqHashBucket(int32 bucket);
static int32 GTM_StoreFreeSeq(GTMStorageHandle seq);
static bool GTM_StoreAddTxnToHash(char *gid, GTMStorageHandle txn);
static void GTM_StoreAddSeqToHash(GTMStorageHandle seq);
static GTMStorageHandle GTM_StoreTxnHashSearch(char *gid);
static GTMStorageHandle GTM_StoreSeqHashSearch(char *seq_key, int32 type);
static int32  GTM_StoreFreeTxn(GTMStorageHandle txn);
static uint32 GTM_StoreGetHashBucket(char *key, int32 len);
static int32  GTM_StoreSync(char *data, size_t size);
static int32  GTM_StoreInitSync(char *data, size_t size);
static bool   GTM_StoreCheckHeaderCRC(void);
static int32  GTM_StoreGetHeader(GTMControlHeader *header);
int32  GTM_StoreGetUsedSeq(void);
int32  GTM_StoreGetUsedTxn(void);
static bool   GTM_StoreCheckSeqCRC(GTM_StoredSeqInfo *seq);
static bool   GTM_StoreCheckTxnCRC(GTM_StoredTransactionInfo *txn);
static bool   GTM_StoreSeqInFreelist(GTM_StoredSeqInfo *seq);
static bool   GTM_StoreTxnInFreelist(GTM_StoredTransactionInfo *txn);
/* Caculate the hash value. */
static uint32
GTM_StoreGetHashValue(char *key, int32 len)
{
    uint32 total = 0;
    int    ii    = 0;

    for (ii = 0; ii < len; ii++)
    {
        total += key[ii];
    }
    return total;
}

uint32 GTM_StoreGetHashBucket(char *key, int32 len)
{
    uint32 hash_value  = 0;
    uint32 hash_bucket = 0;
    
    hash_value = GTM_StoreGetHashValue(key, len);
    hash_bucket = hash_value % GTM_STORED_HASH_TABLE_NBUCKET;
    return hash_bucket;
}
/*
 * Init stand storage file. 
 */
int32 GTM_StoreStandbyInit(char *data_dir, char *data, uint32 length)
{// #lizard forgives
    bool                        result = false;
    int32                       fd     = -1;
    int32                        ret    = -1;
    int32                        i      = 0;
    size_t                     size   = 0;
    char                       path[NODE_STRING_MAX_LENGTH];

    struct stat statbuf;

    if (NULL == data_dir)
    {
        elog(LOG, "GTM_StoreMasterInit invalid NULL file path.");
        return GTM_STORE_ERROR;
    }
    
    /* init the memory structure */
    size  = ALIGN_PAGE(sizeof(GTMControlHeader));    /* header */ 
    size += ALIGN_PAGE(sizeof(GTM_StoredHashTable)); /* seq hash table */ 
    size += ALIGN_PAGE(sizeof(GTM_StoredHashTable)); /* txn hash table */ 
    size += ALIGN_PAGE(sizeof(GTM_StoredSeqInfo) * GTM_MAX_SEQ_NUMBER); /* sequence table */ 
    size += ALIGN_PAGE(sizeof(GTM_StoredTransactionInfo) * MAX_PREPARED_TXN); /* txn table */ 
    size += PAGE_SIZE * 2;        /* two more pages for align*/

    if (length != size)
    {
        elog(LOG, "GTM_StoreStandbyInit invalid data length:%u, required length:%zu.", length, size);
        return GTM_STORE_ERROR;
    }
    
    g_GTMStoreSize = size;
    snprintf(path, NODE_STRING_MAX_LENGTH, "%s/%s", data_dir, GTM_MAP_FILE_NAME);
    fd = open(GTM_MAP_FILE_NAME, O_RDWR, S_IRUSR | S_IWUSR);
    if (fd < 0)
    {
        elog(LOG, "GTM_StoreStandbyInit open file:%s failed for:%s, try to create a new file.", GTM_MAP_FILE_NAME, strerror(errno));
    }
    else 
    {
        if (fstat(fd, &statbuf) < 0)
        {
            close(fd);
            unlink(GTM_MAP_BACKUP_NAME);
            rename(GTM_MAP_FILE_NAME, GTM_MAP_BACKUP_NAME);        
            elog(LOG, "GTM_StoreStandbyInit stat file:%s failed for:%s, try to create a new file.", GTM_MAP_FILE_NAME, strerror(errno));
        }
        else 
        {
            if (statbuf.st_size != g_GTMStoreSize)
            {
                close(fd);
                unlink(GTM_MAP_BACKUP_NAME);
                rename(GTM_MAP_FILE_NAME, GTM_MAP_BACKUP_NAME);    
                elog(LOG, "GTM_StoreStandbyInit stat file:%s size:%zu not equal required size:%zu, file maybe corrupted, backup it to %s", GTM_MAP_FILE_NAME, statbuf.st_size, g_GTMStoreSize, GTM_MAP_BACKUP_NAME);
            }
            else 
            {
                close(fd);
                unlink(GTM_MAP_BACKUP_NAME);
                rename(GTM_MAP_FILE_NAME, GTM_MAP_BACKUP_NAME);        
                elog(LOG, "GTM_StoreStandbyInit backup file:%s to %s.", GTM_MAP_FILE_NAME, GTM_MAP_BACKUP_NAME);
            }
        }
    }    
    
    /* in standby node, we need to reinitliaze the map file */        
    g_GTMStoreMapFile = open(GTM_MAP_FILE_NAME, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    if (g_GTMStoreMapFile < 0)
    {
        elog(LOG, "GTM_StoreStandbyInit create file:%s failed for:%s.", GTM_MAP_FILE_NAME, strerror(errno));
        return GTM_STORE_ERROR;
    }
    
    g_GTMStoreMapAddr = palloc(g_GTMStoreSize);
    if (g_GTMStoreMapAddr == NULL) 
    {        
        close(g_GTMStoreMapFile);
        unlink(GTM_MAP_FILE_NAME);
        elog(LOG, "GTM_StoreStandbyInit mmap file:%s failed for:%s.", GTM_MAP_FILE_NAME, strerror(errno));
        return GTM_STORE_ERROR;
    }
    memcpy(g_GTMStoreMapAddr, data, g_GTMStoreSize);
    
    /* sync the data to file */
    ret = GTM_StoreInitSync(g_GTMStoreMapAddr, g_GTMStoreSize);
    if (ret)
    {
        pfree(g_GTMStoreMapAddr);
        elog(LOG, "GTM_StoreStandbyInit msync file:%s failed for:%s.", GTM_MAP_FILE_NAME, strerror(errno));
        return GTM_STORE_ERROR;
    }                
    
    /* init the global pointers */
    g_GTM_Store_Header     = (GTMControlHeader*)g_GTMStoreMapAddr;
    result = GTM_StoreCheckHeaderCRC();
    if (!result)
    {
        close(g_GTMStoreMapFile);
        pfree(g_GTMStoreMapAddr);
        elog(LOG, "GTM_StoreMasterInit file:%s header CRC check failed.", GTM_MAP_FILE_NAME);
        return GTM_STORE_ERROR;
    }
    elog(LOG, "GTM_StoreStandbyInit file:%s header CRC check succeed.", GTM_MAP_FILE_NAME);

    g_GTM_Store_SeqHashTab = (GTM_StoredHashTable*)(g_GTMStoreMapAddr + ALIGN_PAGE(sizeof(GTMControlHeader)));
    g_GTM_Store_TxnHashTab = (GTM_StoredHashTable*)(g_GTMStoreMapAddr + ALIGN_PAGE(sizeof(GTMControlHeader)) + ALIGN_PAGE(sizeof(GTM_StoredHashTable)));
    g_GTM_Store_SeqInfo    = (GTM_StoredSeqInfo*)(g_GTMStoreMapAddr + ALIGN_PAGE(sizeof(GTMControlHeader)) + ALIGN_PAGE(sizeof(GTM_StoredHashTable)) + ALIGN_PAGE(sizeof(GTM_StoredHashTable)));
    g_GTM_Store_TxnInfo    = (GTM_StoredTransactionInfo*)(g_GTMStoreMapAddr + ALIGN_PAGE(sizeof(GTMControlHeader)) + ALIGN_PAGE(sizeof(GTM_StoredHashTable)) + ALIGN_PAGE(sizeof(GTM_StoredHashTable)) + ALIGN_PAGE(sizeof(GTM_StoredSeqInfo) * GTM_MAX_SEQ_NUMBER));


    g_GTM_Store_Head_Lock = (GTM_RWLock*)palloc(sizeof(GTM_RWLock));
    if (NULL == g_GTM_Store_Head_Lock)
    {
        pfree(g_GTMStoreMapAddr);
        close(g_GTMStoreMapFile);
        unlink(GTM_MAP_FILE_NAME);
        elog(LOG, "GTM_StoreStandbyInit out of memory.");
        return GTM_STORE_ERROR;
    }
    
    g_GTM_Seq_Store_Hash_Lock = (GTM_RWLock*)palloc(sizeof(GTM_RWLock) * GTM_STORED_HASH_TABLE_NBUCKET);
    if (NULL == g_GTM_Seq_Store_Hash_Lock)
    {
        pfree(g_GTMStoreMapAddr);
        pfree(g_GTM_Store_Head_Lock);
        close(g_GTMStoreMapFile);
        unlink(GTM_MAP_FILE_NAME);
        elog(LOG, "GTM_StoreStandbyInit out of memory.");
        return GTM_STORE_ERROR;
    }
    
    g_GTM_Txn_Store_Hash_Lock = (GTM_RWLock*)palloc(sizeof(GTM_RWLock) * GTM_STORED_HASH_TABLE_NBUCKET);
    if (NULL == g_GTM_Seq_Store_Hash_Lock)
    {
        pfree(g_GTMStoreMapAddr);
        pfree(g_GTM_Store_Head_Lock);
        pfree(g_GTM_Seq_Store_Hash_Lock);
        close(g_GTMStoreMapFile);
        unlink(GTM_MAP_FILE_NAME);
        elog(LOG, "GTM_StoreStandbyInit out of memory.");
        return GTM_STORE_ERROR;
    }

    /* init the locks */
    ret = GTM_RWLockInit(g_GTM_Store_Head_Lock);
    if (ret)
    {
        goto INIT_ERROR;
    }
    
    for (i = 0; i < GTM_STORED_HASH_TABLE_NBUCKET; i++)
    {
        ret = GTM_RWLockInit(&g_GTM_Seq_Store_Hash_Lock[i]);
        if (ret)
        {
            goto INIT_ERROR;
        }    
    }

    for (i = 0; i < GTM_STORED_HASH_TABLE_NBUCKET; i++)
    {
        ret = GTM_RWLockInit(&g_GTM_Txn_Store_Hash_Lock[i]);
        if (ret)
        {
            goto INIT_ERROR;
        }    
    }

    ret = GTM_RWLockInit(&g_GTM_store_lock);
    if (ret)
    {
        goto INIT_ERROR;
    }

    GTM_StoreHeaderRunning();
    elog(LOG, "GTM_StoreStandbyInit succeed, storage file:%s.", GTM_MAP_FILE_NAME);
    return GTM_STORE_OK;    
    
INIT_ERROR:
    pfree(g_GTMStoreMapAddr);
    pfree(g_GTM_Store_Head_Lock);
    pfree(g_GTM_Seq_Store_Hash_Lock);
    pfree(g_GTM_Txn_Store_Hash_Lock);
    close(g_GTMStoreMapFile);
    unlink(GTM_MAP_FILE_NAME);
    elog(LOG, "GTM_StoreMasterInit failed.");
    return GTM_STORE_ERROR;
}
/* Function to check Header CRC result. */
bool GTM_StoreCheckHeaderCRC(void)
{
    bool                result = false;
    pg_crc32c            crc_result;
    GTMControlHeader    *header = g_GTM_Store_Header;
    
    if (g_GTM_Store_Header)
    {    
        INIT_CRC32C(crc_result);
        COMP_CRC32C(crc_result,
                    (char *) header,
                    offsetof(GTMControlHeader, m_crc));
        FIN_CRC32C(crc_result);
        result =  EQ_CRC32C(header->m_crc, crc_result);
        if (result)
        {
            elog(LOG, "GTM Storage file VERSION:%d.%d, GTM VERSION:%d.%d", header->m_major_version, header->m_minor_version, GTM_STORE_MAJOR_VERSION, GTM_STORE_MINOR_VERSION);
        }
        return result;
    }
    else
    {
        return false;
    }
}
/* Function to init the gtm storage file when system init. */
int32 GTM_StoreMasterInit(char *data_dir)
{// #lizard forgives
    bool                        bneed_create = false;
    int32                       fd     = -1;
    int32                        ret    = -1;
    int32                        i      = 0;
    size_t                     size   = 0;
    GTM_Timestamp              now;
    
    GTMControlHeader          *header = NULL;
    GTM_StoredHashTable       *seqHashTab = NULL;
    GTM_StoredHashTable       *txnHashTab = NULL;
    GTM_StoredSeqInfo         *seqInfo = NULL;
    GTM_StoredTransactionInfo *txnInfo = NULL;
    struct timeval                current_time;
    char                       path[NODE_STRING_MAX_LENGTH];

    struct stat statbuf;

    if (NULL == data_dir)
    {
        elog(LOG, "GTM_StoreMasterInit invalid NULL file path.");
        return GTM_STORE_ERROR;
    }
    
    /* init the memory structure */
    size  = ALIGN_PAGE(sizeof(GTMControlHeader));    /* header */ 
    size += ALIGN_PAGE(sizeof(GTM_StoredHashTable)); /* seq hash table */ 
    size += ALIGN_PAGE(sizeof(GTM_StoredHashTable)); /* txn hash table */ 
    size += ALIGN_PAGE(sizeof(GTM_StoredSeqInfo) * GTM_MAX_SEQ_NUMBER); /* sequence table */ 
    size += ALIGN_PAGE(sizeof(GTM_StoredTransactionInfo) * MAX_PREPARED_TXN); /* txn table */ 
    size += PAGE_SIZE * 2;        /* two more pages for align*/
    g_GTMStoreSize = size;
    snprintf(path, NODE_STRING_MAX_LENGTH, "%s/%s", data_dir, GTM_MAP_FILE_NAME);
    fd = open(GTM_MAP_FILE_NAME, O_RDWR, S_IRUSR | S_IWUSR);
    if (fd < 0)
    {
        bneed_create = true;
        elog(LOG, "GTM_StoreMasterInit open file:%s failed for:%s, try to create a new file.", GTM_MAP_FILE_NAME, strerror(errno));
    }
    else 
    {
        if (fstat(fd, &statbuf) < 0)
        {
            close(fd);
            elog(LOG, "GTM_StoreMasterInit stat file:%s failed for:%s.", GTM_MAP_FILE_NAME, strerror(errno));
            return GTM_STORE_ERROR;
        }

        if (statbuf.st_size != g_GTMStoreSize)
        {
            close(fd);    
            elog(LOG, "GTM_StoreMasterInit stat file:%s size:%zu not equal required size:%zu, file maybe corrupted, try to create a new file.", GTM_MAP_FILE_NAME, statbuf.st_size, g_GTMStoreSize);
            return GTM_STORE_ERROR;
        }
    }    

    if(enable_gtm_debug)
    {
        g_GTMDebugLogFile = fopen(GTM_DEBUG_FILE_NAME, "w");
        if (g_GTMDebugLogFile == NULL)
        {
            if (fd >= 0)
            {
                close(fd);
            }
            elog(LOG, "GTM_StoreMasterInit create debug log file:%s failed for:%s.", GTM_DEBUG_FILE_NAME, strerror(errno));
            return GTM_STORE_ERROR;
        }
        g_GTMDebugScanLogFile = fopen(GTM_SCAN_DEBUG_FILE_NAME, "w");
        if (g_GTMDebugScanLogFile == NULL)
        {
            if (fd >= 0)
            {
                close(fd);
            }
            elog(LOG, "GTM_StoreMasterInit create debug scan log file:%s failed for:%s.", GTM_SCAN_DEBUG_FILE_NAME, strerror(errno));
            return GTM_STORE_ERROR;
        }
        g_GTMDebugStoreSize = sizeof(GTM_TransactionDebugInfo) * GTM_MAX_DEBUG_TXN_INFO;
        g_GTMDebugStoreAddr = palloc(g_GTMDebugStoreSize);
        if (NULL == g_GTMDebugStoreAddr) 
        {
            if (fd >= 0)
            {
                close(fd);
            }
            elog(LOG, "GTM_StoreMasterInit failed for:%s.", strerror(errno));
            return GTM_STORE_ERROR;
        }
        memset(g_GTMDebugStoreAddr, 0, g_GTMDebugStoreSize);
        g_GTM_TxnDebugInfo = (GTM_TransactionDebugInfo *)(g_GTMDebugStoreAddr);
        
        g_GTM_DebugHeader.m_txn_buffer_len = GTM_MAX_DEBUG_TXN_INFO;
        g_GTM_DebugHeader.m_txn_buffer_last = 0;
        GTM_RWLockInit(&g_GTM_Debug_Lock);
        GTM_RWLockInit(&g_GTM_Scan_Debug_Lock);
    }
    
    if (bneed_create)
    {    
        g_GTMStoreMapFile = open(GTM_MAP_FILE_NAME, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
        if (g_GTMStoreMapFile < 0)
        {
            elog(LOG, "GTM_StoreMasterInit create file:%s failed for:%s.", GTM_MAP_FILE_NAME, strerror(errno));
            return GTM_STORE_ERROR;
        }
        
        g_GTMStoreMapAddr = palloc(g_GTMStoreSize);
        if (NULL == g_GTMStoreMapAddr) 
        {        
            close(g_GTMStoreMapFile);
            unlink(GTM_MAP_FILE_NAME);
            elog(LOG, "GTM_StoreMasterInit mmap file:%s failed for:%s.", GTM_MAP_FILE_NAME, strerror(errno));
            return GTM_STORE_ERROR;
        }
        
        memset(g_GTMStoreMapAddr, 0X00, g_GTMStoreSize);

        now = GTM_TimestampGetCurrent();
        header = (GTMControlHeader*)g_GTMStoreMapAddr;

        /* init header */
        header->m_major_version    = GTM_STORE_MAJOR_VERSION;
        header->m_minor_version    = GTM_STORE_MINOR_VERSION;     
        header->m_gtm_status       = GTM_STARTING;           
        header->m_global_xmin      = FirstNormalGlobalTransactionId;        
        header->m_next_gxid        = FirstNormalGlobalTransactionId; 
        header->m_next_gts           = FirstGlobalTimestamp;
        header->m_seq_freelist     = 0; /* first time through, we are on the first of the freelist. */
        header->m_txn_freelist     = 0; /* first time through, we are on the first of the freelist. */
        header->m_lsn               = 0; /* init the gtm lsn */
        header->m_last_update_time = now;    
        
        gettimeofday(&current_time, NULL);

        /* use us of current time as the system identifier */
        header->m_identifier = (current_time.tv_sec * 1000000 + current_time.tv_usec);

        INIT_CRC32C(header->m_crc);
        COMP_CRC32C(header->m_crc,
                    (char *) header,
                    offsetof(GTMControlHeader, m_crc));
        FIN_CRC32C(header->m_crc);

        /* init seq hashtab */
        seqHashTab = (GTM_StoredHashTable*)(g_GTMStoreMapAddr + ALIGN_PAGE(sizeof(GTMControlHeader)));
        for (i = 0; i < GTM_STORED_HASH_TABLE_NBUCKET; i++)
        {
            seqHashTab->m_buckets[i] = INVALID_STORAGE_HANDLE;
        }

        /* init txn hashtab */
        txnHashTab = (GTM_StoredHashTable*)(g_GTMStoreMapAddr + ALIGN_PAGE(sizeof(GTMControlHeader)) + ALIGN_PAGE(sizeof(GTM_StoredHashTable)));
        for (i = 0; i < GTM_STORED_HASH_TABLE_NBUCKET; i++)
        {
            txnHashTab->m_buckets[i] = INVALID_STORAGE_HANDLE;
        }

        /* init seq freelist */
        seqInfo = (GTM_StoredSeqInfo*)(g_GTMStoreMapAddr + ALIGN_PAGE(sizeof(GTMControlHeader)) + ALIGN_PAGE(sizeof(GTM_StoredHashTable)) + ALIGN_PAGE(sizeof(GTM_StoredHashTable)));
        for (i = 0; i < GTM_MAX_SEQ_NUMBER; i++)
        {
            seqInfo[i].gs_next            = i + 1;
            seqInfo[i].gti_store_handle   = i;
            seqInfo[i].gs_status             = GTM_STORE_SEQ_STATUS_NOT_USE;
            seqInfo[i].m_last_update_time = now;
            GTM_StoreInitRawSeq(&seqInfo[i]);
        }
        /* Set last element invalid */
        seqInfo[i - 1].gs_next = INVALID_STORAGE_HANDLE;
        GTM_StoreInitRawSeq(&seqInfo[i - 1]);

        /* init txn freelist */
        txnInfo = (GTM_StoredTransactionInfo*)(g_GTMStoreMapAddr + ALIGN_PAGE(sizeof(GTMControlHeader)) + ALIGN_PAGE(sizeof(GTM_StoredHashTable)) + ALIGN_PAGE(sizeof(GTM_StoredHashTable)) + ALIGN_PAGE(sizeof(GTM_StoredSeqInfo) * GTM_MAX_SEQ_NUMBER));
        for (i = 0; i < MAX_PREPARED_TXN; i++)
        {
            txnInfo[i].gs_next                 = i + 1;
            txnInfo[i].gti_store_handle           = i;
            txnInfo[i].gti_state               = GTM_TXN_INIT;
            GTM_StoreInitRawTxn(&txnInfo[i]);
        }
        /* Set last element invalid */
        txnInfo[i - 1].gs_next = INVALID_STORAGE_HANDLE;
        GTM_StoreInitRawTxn(&txnInfo[i - 1]);
        
        ret = GTM_StoreInitSync(g_GTMStoreMapAddr, g_GTMStoreSize);
        if (ret)
        {
            pfree(g_GTMStoreMapAddr);
            elog(LOG, "GTM_StoreMasterInit msync file:%s failed for:%s.", GTM_MAP_FILE_NAME, strerror(errno));
            return GTM_STORE_ERROR;
        }        
    }
    else
    {
        size_t nbytes = 0;
            
        elog(LOG, "GTM_StoreMasterInit stat file:%s already exist, continue to startup.", GTM_MAP_FILE_NAME);
        g_GTMStoreMapFile = fd;

        /* load the map file into memory */
        g_GTMStoreMapAddr = palloc(g_GTMStoreSize);
        if (g_GTMStoreMapAddr == NULL) 
        {        
            close(g_GTMStoreMapFile);
            elog(LOG, "GTM_StoreMasterInit mmap file:%s failed for:%s.", GTM_MAP_FILE_NAME, strerror(errno));
            return GTM_STORE_ERROR;
        }
        
        nbytes = read(g_GTMStoreMapFile, g_GTMStoreMapAddr, g_GTMStoreSize);
        if (nbytes != g_GTMStoreSize) 
        {        
            close(g_GTMStoreMapFile);
            pfree(g_GTMStoreMapAddr);
            elog(LOG, "GTM_StoreMasterInit read file:%s failed for:%s, reqiured size:%zu, read size:%zu.", GTM_MAP_FILE_NAME, strerror(errno), g_GTMStoreSize, nbytes);
            return GTM_STORE_ERROR;
        }
    }
    
    /* init the global pointers */
    g_GTM_Store_Header     = (GTMControlHeader*)g_GTMStoreMapAddr;
    g_GTM_Store_SeqHashTab = (GTM_StoredHashTable*)(g_GTMStoreMapAddr + ALIGN_PAGE(sizeof(GTMControlHeader)));
    g_GTM_Store_TxnHashTab = (GTM_StoredHashTable*)(g_GTMStoreMapAddr + ALIGN_PAGE(sizeof(GTMControlHeader)) + ALIGN_PAGE(sizeof(GTM_StoredHashTable)));
    g_GTM_Store_SeqInfo    = (GTM_StoredSeqInfo*)(g_GTMStoreMapAddr + ALIGN_PAGE(sizeof(GTMControlHeader)) + ALIGN_PAGE(sizeof(GTM_StoredHashTable)) + ALIGN_PAGE(sizeof(GTM_StoredHashTable)));
    g_GTM_Store_TxnInfo    = (GTM_StoredTransactionInfo*)(g_GTMStoreMapAddr + ALIGN_PAGE(sizeof(GTMControlHeader)) + ALIGN_PAGE(sizeof(GTM_StoredHashTable)) + ALIGN_PAGE(sizeof(GTM_StoredHashTable)) + ALIGN_PAGE(sizeof(GTM_StoredSeqInfo) * GTM_MAX_SEQ_NUMBER));

    if(enable_gtm_debug)
        GTM_PrintControlHeader();

    if (!bneed_create)
    {
        bool result = false;
        result = GTM_StoreCheckHeaderCRC();
        if (!result)
        {
            close(g_GTMStoreMapFile);
            pfree(g_GTMStoreMapAddr);
            elog(LOG, "GTM_StoreMasterInit file:%s header CRC check failed.", GTM_MAP_FILE_NAME);
            return GTM_STORE_ERROR;
        }
        elog(LOG, "GTM_StoreMasterInit file:%s header CRC check succeed.", GTM_MAP_FILE_NAME);
    }

    g_GTM_Store_Head_Lock = (GTM_RWLock*)palloc(sizeof(GTM_RWLock));
    if (NULL == g_GTM_Store_Head_Lock)
    {
        pfree(g_GTMStoreMapAddr);
        close(g_GTMStoreMapFile);
        unlink(GTM_MAP_FILE_NAME);
        elog(LOG, "GTM_StoreMasterInit out of memory.");
        return GTM_STORE_ERROR;
    }
    
    g_GTM_Seq_Store_Hash_Lock = (GTM_RWLock*)palloc(sizeof(GTM_RWLock) * GTM_STORED_HASH_TABLE_NBUCKET);
    if (NULL == g_GTM_Seq_Store_Hash_Lock)
    {
        pfree(g_GTMStoreMapAddr);        
        pfree(g_GTM_Store_Head_Lock);
        close(g_GTMStoreMapFile);
        unlink(GTM_MAP_FILE_NAME);
        elog(LOG, "GTM_StoreMasterInit out of memory.");
        return GTM_STORE_ERROR;
    }
    
    g_GTM_Txn_Store_Hash_Lock = (GTM_RWLock*)palloc(sizeof(GTM_RWLock) * GTM_STORED_HASH_TABLE_NBUCKET);
    if (NULL == g_GTM_Seq_Store_Hash_Lock)
    {
        pfree(g_GTMStoreMapAddr);
        pfree(g_GTM_Store_Head_Lock);
        pfree(g_GTM_Seq_Store_Hash_Lock);
        close(g_GTMStoreMapFile);
        unlink(GTM_MAP_FILE_NAME);
        elog(LOG, "GTM_StoreMasterInit out of memory.");
        return GTM_STORE_ERROR;
    }

    /* init the locks */
    ret = GTM_RWLockInit(g_GTM_Store_Head_Lock);
    if (ret)
    {
        goto INIT_ERROR;
    }

    g_GTM_Store_Head_Lock->lock_flag = GTM_RWLOCK_FLAG_STORE;

    for (i = 0; i < GTM_STORED_HASH_TABLE_NBUCKET; i++)
    {
        ret = GTM_RWLockInit(&g_GTM_Seq_Store_Hash_Lock[i]);
        if (ret)
        {
            goto INIT_ERROR;
        }
        g_GTM_Seq_Store_Hash_Lock[i].lock_flag = GTM_RWLOCK_FLAG_STORE;
    }

    for (i = 0; i < GTM_STORED_HASH_TABLE_NBUCKET; i++)
    {
        ret = GTM_RWLockInit(&g_GTM_Txn_Store_Hash_Lock[i]);
        if (ret)
        {
            goto INIT_ERROR;
        }
        g_GTM_Txn_Store_Hash_Lock[i].lock_flag = GTM_RWLOCK_FLAG_STORE;
    }

    ret = GTM_RWLockInit(&g_GTM_store_lock);
    if (ret)
    {
        goto INIT_ERROR;
    }
    g_GTM_store_lock.lock_flag = GTM_RWLOCK_FLAG_STORE;

    GTM_StoreHeaderRunning();
    elog(LOG, "GTM_StoreMasterInit succeed, storage file:%s.", GTM_MAP_FILE_NAME);
    return GTM_STORE_OK;    
    
INIT_ERROR:
    pfree(g_GTMStoreMapAddr);
    pfree(g_GTM_Store_Head_Lock);
    pfree(g_GTM_Seq_Store_Hash_Lock);
    pfree(g_GTM_Txn_Store_Hash_Lock);
    close(g_GTMStoreMapFile);
    unlink(GTM_MAP_FILE_NAME);
    elog(LOG, "GTM_StoreMasterInit failed.");
    return GTM_STORE_ERROR;
}

/* shutdown the GTM store. */
int32 GTM_StoreShutDown(void)
{
    int32 ret = -1;
    /* Lock store to block any write. */
    GTM_StoreLock();

    /* we are shutting down ,just lock the header ensure no one can write. */
    GTM_RWLockAcquire(g_GTM_Store_Head_Lock, GTM_LOCKMODE_WRITE);    
    GTM_StoreHeaderShutdown();    
    
    ret = GTM_StoreInitSync(g_GTMStoreMapAddr, g_GTMStoreSize);
    if (ret)
    {
        return -1;
    }    
    pfree(g_GTMStoreMapAddr);
    close(g_GTMStoreMapFile);
    if(enable_gtm_debug)
    {
        fclose(g_GTMDebugLogFile);
        fclose(g_GTMDebugScanLogFile);
    }
    g_GTMStoreMapAddr = NULL;
    g_GTMStoreSize    = 0;
    g_GTMStoreMapFile = -1;
    
    elog(LOG, "GTM_StoreShutDown succeed.");
    return GTM_STORE_OK;
}

/*
 *    Alloc a SEQ for a specific txn.
 */
GTMStorageHandle GTM_StoreAllocSeq(char *key, int32 key_type)
{// #lizard forgives
    bool ret                                = false;
    bool flush_head                         = false;
    GTM_StoredSeqInfo         *head         = NULL;
    GTM_StoredSeqInfo         *next         = NULL;
    GTM_StoredSeqInfo         *current      = NULL;
    
    if (NULL == key)
    {
        return INVALID_STORAGE_HANDLE;
    }

    /* alloc a seq from freelist */
    ret = GTM_RWLockAcquire(g_GTM_Store_Head_Lock, GTM_LOCKMODE_WRITE);
    if (!ret)
    {
        return INVALID_STORAGE_HANDLE;
    }

    if (INVALID_STORAGE_HANDLE == g_GTM_Store_Header->m_seq_freelist)
    {
        GTM_RWLockRelease(g_GTM_Store_Head_Lock);    
        return INVALID_STORAGE_HANDLE;
    }

    head = GetSeqStore(g_GTM_Store_Header->m_seq_freelist);
    
    /* last free element. */
    if (INVALID_STORAGE_HANDLE == head->gs_next)
    {
        current                               = head;
        flush_head                            = true;
        g_GTM_Store_Header->m_seq_freelist = INVALID_STORAGE_HANDLE;
    }
    else
    {
        /* use second element in the free list for allocation, avoid writing head too often. */
        next = GetSeqStore(head->gs_next);
        if (next->gs_next != INVALID_STORAGE_HANDLE)
        {
            current = GetSeqStore(next->gs_next);
            next->gs_next = current->gs_next;
        }
        else
        {
            current = next;
            head->gs_next = INVALID_STORAGE_HANDLE;
        }
    }

    current->gs_next   = INVALID_STORAGE_HANDLE;
    /* just allocated */
    current->gs_status = GTM_STORE_SEQ_STATUS_ALLOCATE;
    
    /* add the seq to hash */
    snprintf(current->gs_key.gsk_key, SEQ_KEY_MAX_LENGTH, "%s", key);
    current->gs_key.gsk_type = key_type;
    GTM_StoreAddSeqToHash(current->gti_store_handle);
    
    /* flush header */
    if (flush_head)
    {
        GTM_StoreSyncHeader(true);
    }
    
    /* flush current seq */
    GTM_StoreSyncSeq(current->gti_store_handle);

    if (next != current && next)
    {
        GTM_StoreSyncSeq(next->gti_store_handle);
    }
    
    /* flush seq head */
    if (head != current && head)
    {
        GTM_StoreSyncSeq(head->gti_store_handle);
    }    
    GTM_RWLockRelease(g_GTM_Store_Head_Lock);

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreAllocSeq seq:%s key_type:%d handle:%d.", key, key_type, current->gti_store_handle);
    }
    
    return current->gti_store_handle;
}

int32 GTM_StoreSyncSeq(GTMStorageHandle handle)
{
    int32  ret;
    GTM_StoredSeqInfo *seq = NULL;

    if (!VALID_SEQ_HANDLE(handle))
    {
        elog(LOG, "GTM_StoreSyncSeq invalid handle:%d", handle);
        return     GTM_STORE_ERROR;
    }
    
    /* increase LSN of header and sync to disk. */
    GTM_StoreSyncHeader(true);
    
    seq = GetSeqStore(handle);
    seq->m_last_update_time = GTM_TimestampGetCurrent();

    /* calculate CRC */
    INIT_CRC32C(seq->gs_crc);
    COMP_CRC32C(seq->gs_crc,
                (char *) seq,
                offsetof(GTM_StoredSeqInfo, gs_crc));
    FIN_CRC32C(seq->gs_crc);
    
    ret = GTM_StoreSync((char*)seq, ALIGN8(sizeof(GTM_StoredSeqInfo)));
    if (ret)
    {
        elog(LOG, "GTM_StoreSyncSeq seq:%d failed for: %s.", handle, strerror(errno));
    }

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreSyncSeq seq:%d done.", handle);
    }
    return ret;
}


void  GTM_StoreInitRawSeq(GTM_StoredSeqInfo *seq)
{    
    seq->m_last_update_time = GTM_TimestampGetCurrent();

    /* calculate CRC */
    INIT_CRC32C(seq->gs_crc);
    COMP_CRC32C(seq->gs_crc,
                (char *) seq,
                offsetof(GTM_StoredSeqInfo, gs_crc));
    FIN_CRC32C(seq->gs_crc);
}

int32 GTM_CommitSyncSeq(GTMStorageHandle handle)
{
    int32  ret;
    GTM_StoredSeqInfo *seq = NULL;

    seq = GetSeqStore(handle);
    seq->m_last_update_time = GTM_TimestampGetCurrent();
    seq->gs_status          = GTM_STORE_SEQ_STATUS_COMMITED;
    
    ret = GTM_StoreSync((char*)seq, ALIGN8(sizeof(GTM_StoredSeqInfo)));
    if (ret)
    {
        elog(LOG, "GTM_CommitSyncSeq reset seq:%d failed for: %s.", handle, strerror(errno));
    }

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_CommitSyncSeq seq:%d done.", handle);
    }
    return ret;
}

int32 GTM_ResetSyncSeq(GTMStorageHandle handle)
{
    int32  ret;
    
    GTM_StoredSeqInfo *seq = NULL;
    seq = GetSeqStore(handle);
    seq->m_last_update_time = GTM_TimestampGetCurrent();
    seq->gs_value           = seq->gs_init_value;
    
    ret = GTM_StoreSync((char*)seq, ALIGN8(sizeof(GTM_StoredSeqInfo)));
    if (ret)
    {
        elog(LOG, "GTM_ResetSyncSeq reset seq:%d failed for: %s.", handle, strerror(errno));
    }

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_ResetSyncSeq seq:%s seq:%d done", seq->gs_key.gsk_key, handle);
    }
    return ret;
}

#ifdef __XLOG__
/*
 * Sync the storage header to disk.
 */
int32 GTM_InitStoreSyncHeader(bool needLsn)
{
    int32  ret;

    if (needLsn)
    {
        g_GTM_Store_Header->m_lsn++;
    }
    g_GTM_Store_Header->m_last_update_time = GTM_TimestampGetCurrent();

    /* calculate CRC */
    INIT_CRC32C(g_GTM_Store_Header->m_crc);
    COMP_CRC32C(g_GTM_Store_Header->m_crc,
                (char *) g_GTM_Store_Header,
                offsetof(GTMControlHeader, m_crc));
    FIN_CRC32C(g_GTM_Store_Header->m_crc);

    ret = GTM_StoreInitSync((char*)g_GTM_Store_Header, ALIGN8(sizeof(GTMControlHeader)));
    if (ret)
    {
        elog(LOG, "GTM_StoreSyncHeader msync header failed for: %s.", strerror(errno));
    }
    return ret;
}
#endif

/*
 * Sync the storage header to disk.
 */
int32 GTM_StoreSyncHeader(bool needLsn)
{
    int32  ret = GTM_STORE_OK;
    
    if (needLsn)
    {
        g_GTM_Store_Header->m_lsn++;
    }
    g_GTM_Store_Header->m_last_update_time = GTM_TimestampGetCurrent();

    /* calculate CRC */
    INIT_CRC32C(g_GTM_Store_Header->m_crc);
    COMP_CRC32C(g_GTM_Store_Header->m_crc,
                (char *) g_GTM_Store_Header,
                offsetof(GTMControlHeader, m_crc));
    FIN_CRC32C(g_GTM_Store_Header->m_crc);

    if(Recovery_IsStandby() == false)
    {
        ret = GTM_StoreSync((char*)g_GTM_Store_Header, ALIGN8(sizeof(GTMControlHeader)));
        if (ret)
        {
            elog(LOG, "GTM_StoreSyncHeader msync header failed for: %s.", strerror(errno));
        }
    }
    return ret;
}

int32 GTM_StoreHeaderShutdown(void)
{
    int       ret;
    g_GTM_Store_Header->m_gtm_status = GTM_SHUTTING_DOWN;
    ret = GTM_StoreSyncHeader(false);
    elog(LOG, "GTM_StoreHeaderShutdown done, latest xid:%u, global_xmin:%u.", g_GTM_Store_Header->m_next_gxid, g_GTM_Store_Header->m_global_xmin);
    if (ret)
    {
        elog(LOG, "GTM_StoreHeaderShutdown sync header to disk failed for:%s.", strerror(errno));
    }
    return ret;
}

int32 GTM_StoreHeaderRunning(void)
{
    int32  ret;
    g_GTM_Store_Header->m_gtm_status = GTM_RUNNING;

    ret = GTM_InitStoreSyncHeader(false);
    if (ret)
    {
        elog(LOG, "GTM_StoreHeaderRunning sync header to disk failed for:%s.", strerror(errno));
    }

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreHeaderRunning done.");
    }
    return ret;
}

int32 GTM_StoreSyncTxn(GTMStorageHandle handle)
{
    int32  ret;
    GTM_StoredTransactionInfo *txn = NULL;
    if (!VALID_TXN_HANDLE(handle))
    {
        elog(LOG, "GTM_StoreSyncTxn invalid txn handle:%d", handle);
        return     GTM_STORE_ERROR;
    }

    /* increase LSN of header and sync to disk. */
    GTM_StoreSyncHeader(true);
    
    txn = GetTxnStore(handle);
    txn->m_last_update_time = GTM_TimestampGetCurrent();

    /* calculate CRC */
    INIT_CRC32C(txn->gti_crc);
    COMP_CRC32C(txn->gti_crc,
                (char *) txn,
                offsetof(GTM_StoredTransactionInfo, gti_crc));
    FIN_CRC32C(txn->gti_crc);
    
    ret = GTM_StoreSync((char*)txn, ALIGN8(sizeof(GTM_StoredTransactionInfo)));
    if (ret)
    {
        elog(LOG, "GTM_StoreSyncTxn sync txn:%d info to disk failed for:%s.", handle,strerror(errno));
    }
    
    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreSyncTxn seq:%d gid:%s node_string:%s.", handle,txn->gti_gid,txn->nodestring);
    }
    return ret;
}

void GTM_StoreInitRawTxn(GTM_StoredTransactionInfo *txn)
{
    txn->m_last_update_time = GTM_TimestampGetCurrent();

    /* calculate CRC */
    INIT_CRC32C(txn->gti_crc);
    COMP_CRC32C(txn->gti_crc,
                (char *) txn,
                offsetof(GTM_StoredTransactionInfo, gti_crc));
    FIN_CRC32C(txn->gti_crc);
}

int32 GTM_StoreSyncTxnHashBucket(int32 bucket)
{
    int32 ret;    
    ret =  GTM_StoreSync((char*)(g_GTM_Store_TxnHashTab->m_buckets + bucket), ALIGN8(sizeof(GTMStorageHandle)));
    if (ret)
    {
        elog(LOG, "GTM_StoreSyncTxnHashBucket sync bucket:%d failed for: %s.", bucket, strerror(errno));
    }

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreSyncTxnHashBucket bucket:%d.", bucket);
    }
    return ret;
}

int32 GTM_StoreSyncSeqHashBucket(int32 bucket)
{
    int32 ret;    
    ret = GTM_StoreSync((char*)(g_GTM_Store_SeqHashTab->m_buckets + bucket), ALIGN8(sizeof(GTMStorageHandle)));
    if (ret)
    {
        elog(LOG, "GTM_StoreSyncSeqHashBucket sync bucket:%d failed for: %s.", bucket, strerror(errno));
    }

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreSyncSeqHashBucket bucket:%d.", bucket);
    }
    return ret;
}

/*
 * Free the specific seq to store.
 */
int32 GTM_StoreFreeSeq(GTMStorageHandle seq)
{// #lizard forgives    
    bool                        flush_bucket  = false;
    bool                       ret           = false;
    bool                       flush_head    = false;
    bool                       flush_bucket_info = false;
    GTM_StoredSeqInfo         *head          = NULL;
    GTM_StoredSeqInfo         *current       = NULL;
    GTM_StoredSeqInfo         *next          = NULL;

    uint32                       bucket         = 0;
    GTMStorageHandle           bucket_handle = INVALID_STORAGE_HANDLE;
    GTM_StoredSeqInfo         *seq_info      = NULL;
    GTM_StoredSeqInfo         *bucket_info     = NULL;
    
    if (!VALID_SEQ_HANDLE(seq))
    {
        elog(LOG, "GTM_StoreFreeSeq invalid handle:%d", seq);
        return GTM_STORE_ERROR;
    }

    /* remove the txn from hash table */
    seq_info = GetSeqStore(seq);
    if (GTM_STORE_SEQ_STATUS_NOT_USE == seq_info->gs_status)
    {
        elog(LOG, "GTM_StoreFreeSeq handle:%d already freed", seq);
        return GTM_STORE_OK;
    }
    
    bucket = GTM_StoreGetHashBucket(seq_info->gs_key.gsk_key, strnlen(seq_info->gs_key.gsk_key, GTM_MAX_SESSION_ID_LEN));
    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreFreeSeq seq:%s key_type:%d bucket:%d.", seq_info->gs_key.gsk_key, seq_info->gs_key.gsk_type, bucket);
    }

#ifdef __XLOG__
    ret = GTM_RWLockAcquire(g_GTM_Store_Head_Lock, GTM_LOCKMODE_WRITE);
    if (!ret)
    {
        elog(LOG, "GTM_StoreFreeSeq acquire seq lock failed");
        return GTM_STORE_ERROR;
    }
#endif

    ret = AcquireSeqHashLock(bucket, GTM_LOCKMODE_WRITE);
    if (!ret)
    {
        GTM_RWLockRelease(g_GTM_Store_Head_Lock);
        elog(LOG, "GTM_StoreFreeSeq AcquireSeqHashLock bucktet:%d failed", bucket);
        return GTM_STORE_ERROR;
    }

    /* loop through the hash list */
    bucket_handle = GetSeqHashBucket(bucket);
    while (bucket_handle != seq && bucket_handle != INVALID_STORAGE_HANDLE)
    {
        bucket_info   = GetSeqStore(bucket_handle);    
        bucket_handle = bucket_info->gs_next;
    }

    if (bucket_handle == seq)
    {
        /* first element */
        seq_info = GetSeqStore(bucket_handle);    
        if (bucket_handle == GetSeqHashBucket(bucket))
        {            
            if (enable_gtm_sequence_debug)
            {
                elog(LOG, "GTM_StoreFreeSeq seq:%s key_type:%d is the first in bucket:%d.", seq_info->gs_key.gsk_key, seq_info->gs_key.gsk_type, bucket);
            }
            SetSeqHashBucket(bucket, seq_info->gs_next);
            seq_info->gs_next = INVALID_STORAGE_HANDLE;
            flush_bucket = true;
        }
        else
        {
            bucket_info->gs_next = seq_info->gs_next;
            if (enable_gtm_sequence_debug)
            {
                elog(LOG, "GTM_StoreFreeSeq seq:%s key_type:%d is the middle of bucket:%d.", seq_info->gs_key.gsk_key, seq_info->gs_key.gsk_type, bucket);
            }
            flush_bucket_info = true;
        }
        ReleaseSeqHashLock(bucket);
    }
    else if (INVALID_STORAGE_HANDLE == bucket_handle)
    {
        /* should not happen */
        GTM_RWLockRelease(g_GTM_Store_Head_Lock);
        ReleaseSeqHashLock(bucket);
        elog(LOG, "GTM_StoreFreeSeq seq:%d not found", seq);

        if (enable_gtm_sequence_debug)
        {
            elog(LOG, "GTM_StoreFreeSeq seq:%s key_type:%d not found in bucket:%d.", seq_info->gs_key.gsk_key, seq_info->gs_key.gsk_type, bucket);
        }
        return GTM_STORE_ERROR;
    }

#ifndef __XLOG__
    ret = GTM_RWLockAcquire(g_GTM_Store_Head_Lock, GTM_LOCKMODE_WRITE);
    if (!ret)
    {
        elog(LOG, "GTM_StoreFreeSeq acquire seq lock failed");
        return GTM_STORE_ERROR;
    }
#endif

    current = GetSeqStore(seq);
    if (INVALID_STORAGE_HANDLE == g_GTM_Store_Header->m_seq_freelist)
    {    
        /* just print */
        elog(LOG, "GTM_StoreFreeSeq no more element in freelist");
        current->gs_next = INVALID_STORAGE_HANDLE;
        g_GTM_Store_Header->m_seq_freelist = seq;        
        flush_head    = true;    
        
        if (enable_gtm_sequence_debug)
        {
            elog(LOG, "GTM_StoreFreeSeq seq:%s key_type:%d handle:%d will be the only one in freelist.", seq_info->gs_key.gsk_key, seq_info->gs_key.gsk_type, seq);
        }
    }
    else
    {
        head = GetSeqStore(g_GTM_Store_Header->m_seq_freelist);    
        if (INVALID_STORAGE_HANDLE == head->gs_next)
        {            
            /* only one element in freelist, we will be the last one, also the second one */
            current->gs_next = INVALID_STORAGE_HANDLE;
            head->gs_next = seq;
            if (enable_gtm_sequence_debug)
            {
                elog(LOG, "GTM_StoreFreeSeq seq:%s key_type:%d handle:%d will be the second one in freelist.", seq_info->gs_key.gsk_key, seq_info->gs_key.gsk_type, seq);
            }
        }
        else
        {
            next = GetSeqStore(head->gs_next);    
            head->gs_next = seq;
            current->gs_next = next->gti_store_handle;
            
            if (enable_gtm_sequence_debug)
            {
                elog(LOG, "GTM_StoreFreeSeq seq:%s key_type:%d handle:%d will in the middle of freelist.", seq_info->gs_key.gsk_key, seq_info->gs_key.gsk_type, seq);
            }
        }
    }
    
    /* reset the status */
    current->gs_status = GTM_STORE_SEQ_STATUS_NOT_USE;
        
    /* flush header */
    if (flush_head)
    {
        GTM_StoreSyncHeader(true);
    }

    /* flush seq head */
    if (head)
    {
        GTM_StoreSyncSeq(head->gti_store_handle);    
    }

    /* flush seq on prelink */
    if(flush_bucket_info)
    {
        GTM_StoreSyncSeq(bucket_info->gti_store_handle);    
    }
    
    /* flush current seq */
    GTM_StoreSyncSeq(seq);    

    /* flush pre hash */
    if (!flush_bucket)
    {
        GTM_StoreSyncSeq(bucket_handle);    
    }

    /* flush hash bucket */
    if (flush_bucket)
    {
        GTM_StoreSyncSeqHashBucket(bucket);
    }
    
    GTM_RWLockRelease(g_GTM_Store_Head_Lock);
    return GTM_STORE_OK;
}
/*
 * Add the txn info into the hash list.
 */
bool GTM_StoreAddTxnToHash(char *gid, GTMStorageHandle txn)
{// #lizard forgives
    bool                       ret             = false;
    uint32                       bucket        = 0;
    GTMStorageHandle           bucket_handle = INVALID_STORAGE_HANDLE;
    GTM_StoredTransactionInfo *txn_info      = NULL;
    GTM_StoredTransactionInfo *bucket_into   = NULL;

    if (!VALID_TXN_HANDLE(txn) || !gid)
    {
        elog(LOG, "GTM_StoreAddTxnToHash invalid parameter.");
        return false;
    }    

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreAddTxnToHash gid:%s txn:%d begin.", gid, txn);
    }
    
    txn_info = GetTxnStore(txn);
    bucket = GTM_StoreGetHashBucket(txn_info->gti_gid, strnlen(txn_info->gti_gid, GTM_MAX_SESSION_ID_LEN));
    
    ret = AcquireTxnHashLock(bucket, GTM_LOCKMODE_WRITE);
    if (!ret)
    {
        elog(LOG, "GTM_StoreAddTxnToHash AcquireTxnHashLock bucket:%d lock failed for %s.", bucket, strerror(errno));
        return false;
    }
    
    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreAddTxnToHash gid:%s txn:%d will be in bucket:%d.", gid, txn, bucket);
    }
    
    bucket_handle = GetTxnHashBucket(bucket);
    /* first element */
    if (INVALID_STORAGE_HANDLE == bucket_handle)
    {
        if (enable_gtm_sequence_debug)
        {
            elog(LOG, "GTM_StoreAddTxnToHash gid:%s txn:%d will be the first of bucktet:%d.", gid, txn, bucket);
        }
        txn_info->gs_next = INVALID_STORAGE_HANDLE;
        SetTxnHashBucket(bucket, txn);    
    }
    else
    {
        /* set the new transaction as the bucket header */
        bucket_into = GetTxnStore(bucket_handle);    
        txn_info->gs_next = bucket_into->gti_store_handle;
        SetTxnHashBucket(bucket, txn);        
        if (enable_gtm_sequence_debug)
        {
            elog(LOG, "GTM_StoreAddTxnToHash gid:%s txn:%d will be the middle of bucktet:%d.", gid, txn, bucket);
        }
    }

    GTM_StoreSyncTxn(txn);
    GTM_StoreSyncTxnHashBucket(bucket);
    ReleaseTxnHashLock(bucket);
    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreAddTxnToHash gid:%s txn:%d done.", gid, txn);
    }
    return true;
}
/*
 * Add seq to hash.
 */
void GTM_StoreAddSeqToHash(GTMStorageHandle seq)
{// #lizard forgives
    bool                       ret             = false;
    uint32                       bucket         = 0;
    GTMStorageHandle           bucket_handle = INVALID_STORAGE_HANDLE;
    GTM_StoredSeqInfo           *seq_info      = NULL;
    GTM_StoredSeqInfo          *bucket_info     = NULL;

    if (!VALID_SEQ_HANDLE(seq))
    {
        elog(LOG, "GTM_StoreAddSeqToHash invalid handle:%d", seq);
        return;
    }    

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreAddSeqToHash seq:%d begin.", seq);
    }
    
    seq_info = GetSeqStore(seq);    
    bucket   = GTM_StoreGetHashBucket(seq_info->gs_key.gsk_key, strnlen(seq_info->gs_key.gsk_key, GTM_MAX_SESSION_ID_LEN));

    ret = AcquireSeqHashLock(bucket, GTM_LOCKMODE_WRITE);
    if (!ret)
    {
        elog(LOG, "GTM_StoreAddSeqToHash AcquireSeqHashLock failed for %s, seq:%d, bucket:%d", strerror(errno), seq, bucket);
        return;
    }
    
    bucket_handle = GetSeqHashBucket(bucket);
    /* first element */
    if (INVALID_STORAGE_HANDLE == bucket_handle)
    {
        seq_info->gs_next = INVALID_STORAGE_HANDLE;
        SetSeqHashBucket(bucket, seq);    
        if (enable_gtm_sequence_debug)
        {
            elog(LOG, "GTM_StoreAddSeqToHash seq:%d is the first element in bucket:%d.", seq, bucket);
        }
    }
    else
    {
        /* set the new transaction as the bucket header */
        bucket_info = GetSeqStore(bucket_handle);    
        seq_info->gs_next = bucket_info->gti_store_handle;
        SetSeqHashBucket(bucket, seq);    
        if (enable_gtm_sequence_debug)
        {
            elog(LOG, "GTM_StoreAddSeqToHash add seq:%d to bucket:%d, next is:%d.", seq, bucket, seq_info->gs_next);
        }
    }

    GTM_StoreSyncSeq(seq);
    GTM_StoreSyncSeqHashBucket(bucket);
    ReleaseSeqHashLock(bucket);
    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreAddSeqToHash seq:%d done.", seq);
    }
}

/*
 * Search the txn hash and return the txn handle.
 */
static GTMStorageHandle GTM_StoreTxnHashSearch(char *gid)
{// #lizard forgives
    bool                       found         = false;
    bool                       ret            = false;
    uint32                       bucket        = 0;
    GTMStorageHandle           bucket_handle = INVALID_STORAGE_HANDLE;
    GTM_StoredTransactionInfo *txn_info      = NULL;

    if (NULL == gid)
    {
        elog(LOG, "GTM_StoreTxnHashSearch invalid null ptr.");
        return INVALID_STORAGE_HANDLE;
    }

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreTxnHashSearch gid:%s begin.", gid);
    }
    
    bucket = GTM_StoreGetHashBucket(gid, strnlen(gid, GTM_MAX_SESSION_ID_LEN));
    ret = AcquireTxnHashLock(bucket, GTM_LOCKMODE_READ);
    if (!ret)
    {
        return INVALID_STORAGE_HANDLE;
    }

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreTxnHashSearch gid:%s should be in hash bucktet:%d.", gid, bucket);
    }

    found = false;
    bucket_handle = GetTxnHashBucket(bucket);
    while (INVALID_STORAGE_HANDLE != bucket_handle)
    {    
        txn_info = GetTxnStore(bucket_handle);
        if (0 == strncmp(txn_info->gti_gid,  gid, GTM_MAX_SESSION_ID_LEN))
        {
            found = true;
            break;
        }

        bucket_handle = txn_info->gs_next;
    }
    ReleaseTxnHashLock(bucket);

    if (enable_gtm_sequence_debug)
    {
        if (!found)
        {
            elog(LOG, "GTM_StoreTxnHashSearch gid:%s not found in hash bucktet:%d.", gid, bucket);
        }        
    }
    return found ? bucket_handle : INVALID_STORAGE_HANDLE;
}


/*
 * Search the seq hash and return the seq handle.
 */
static GTMStorageHandle GTM_StoreSeqHashSearch(char *seq_key, int32 type)
{
    bool                       found         = false;
    bool                       ret            = false;
    uint32                       bucket        = 0;
    GTMStorageHandle           bucket_handle = INVALID_STORAGE_HANDLE;
    GTM_StoredSeqInfo           *seq_info      = NULL;

    if (NULL == seq_key)
    {
        return INVALID_STORAGE_HANDLE;
    }    
    
    bucket = GTM_StoreGetHashBucket(seq_key, strnlen(seq_key, GTM_MAX_SESSION_ID_LEN));
    ret = AcquireSeqHashLock(bucket, GTM_LOCKMODE_READ);
    if (!ret)
    {
        return INVALID_STORAGE_HANDLE;
    }

    found = false;
    bucket_handle = GetSeqHashBucket(bucket);
    while (INVALID_STORAGE_HANDLE != bucket_handle)
    {    
        seq_info = GetSeqStore(bucket_handle);
        if (0 == strncmp(seq_info->gs_key.gsk_key,  seq_key, GTM_MAX_SESSION_ID_LEN))
        {
            found = true;
            break;
        }

        bucket_handle = seq_info->gs_next;
    }
    ReleaseSeqHashLock(bucket);
    return found ? bucket_handle : INVALID_STORAGE_HANDLE;
}


/*
 * Alloc a TXN control info.
 */
GTMStorageHandle GTM_StoreAllocTxn(char *gid)
{// #lizard forgives
    bool                                 ret          = false;
    bool                                flush_head   = false;
    GTM_StoredTransactionInfo          *head         = NULL;
    GTM_StoredTransactionInfo          *next         = NULL;
    GTM_StoredTransactionInfo          *current        = NULL;    

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreAllocTxn  begin gid:%s", gid);
    }
            
    ret = GTM_RWLockAcquire(g_GTM_Store_Head_Lock, GTM_LOCKMODE_WRITE);
    if (!ret)
    {
        elog(LOG, "GTM_StoreAllocTxn  GTM_RWLockAcquire g_GTM_Store_Head_Lock failed:%s", strerror(errno));
        return INVALID_STORAGE_HANDLE;
    }

    if (INVALID_STORAGE_HANDLE == g_GTM_Store_Header->m_txn_freelist)
    {
        GTM_RWLockRelease(g_GTM_Store_Head_Lock);    
        elog(LOG, "GTM_StoreAllocTxn no more txn handle");
        return INVALID_STORAGE_HANDLE;
    }
    
    head = GetTxnStore(g_GTM_Store_Header->m_txn_freelist);
    
    /* last free element. */
    if (INVALID_STORAGE_HANDLE == head->gs_next)
    {
        current                               = head;
        flush_head                            = true;
        g_GTM_Store_Header->m_txn_freelist = INVALID_STORAGE_HANDLE;
        if (enable_gtm_sequence_debug)
        {
            elog(LOG, "GTM_StoreAllocTxn only one element left in freelist");
        }
    }
    else
    {
        /* use second element in the free list for allocation, avoid writing head too often. */
        next = GetTxnStore(head->gs_next);
        if (next->gs_next != INVALID_STORAGE_HANDLE)
        {
            current = GetTxnStore(next->gs_next);
            next->gs_next = current->gs_next;
        }
        else
        {
            current = next;
            head->gs_next = INVALID_STORAGE_HANDLE;
        }
    }
    
    current->gs_next   = INVALID_STORAGE_HANDLE;
    /* just allocated */
    current->gti_state = GTM_TXN_STARTING;
    
    /* add the txn to hash */
    snprintf(current->gti_gid, GTM_MAX_SESSION_ID_LEN, "%s", gid);
    GTM_StoreAddTxnToHash(gid, current->gti_store_handle);
    
    /* flush header */
    if (flush_head)
    {
        GTM_StoreSyncHeader(true);
    }
    
    /* flush current seq */
    GTM_StoreSyncTxn(current->gti_store_handle);

    if (next != current && next != NULL)
    {
        GTM_StoreSyncTxn(next->gti_store_handle);
    }
    
    /* flush seq head */
    if (head != current && head != NULL)
    {
        GTM_StoreSyncTxn(head->gti_store_handle);
    }    
    GTM_RWLockRelease(g_GTM_Store_Head_Lock);

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreAllocTxn  done gid:%s", gid);
    }
    return current->gti_store_handle;
}
/*
 * Free a TXN control info, move the txn from hash table to freelist.
 */
int32 GTM_StoreFreeTxn(GTMStorageHandle txn)
{// #lizard forgives    
    bool                               flush_bucket   = false;
    bool                              ret             = false;
    bool                              flush_head     = false;
    GTM_StoredTransactionInfo          *head          = NULL;
    GTM_StoredTransactionInfo          *current         = NULL;
    GTM_StoredTransactionInfo          *next          = NULL;    
    uint32                               bucket         = 0;
    GTMStorageHandle                   bucket_handle = INVALID_STORAGE_HANDLE;
    GTM_StoredTransactionInfo         *txn_info      = NULL;
    GTM_StoredTransactionInfo         *bucket_info     = NULL;
    
    
    if (!VALID_TXN_HANDLE(txn))
    {
        elog(LOG, "GTM_StoreFreeTxn invalid txn:%d", txn);
        return GTM_STORE_ERROR;
    }

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreFreeTxn for txn:%d begin", txn);
    }
    
    /* remove the txn from hash table */
    txn_info = GetTxnStore(txn);

    if (GTM_TXN_INIT == txn_info->gti_state)
    {
        elog(LOG, "GTM_StoreFreeTxn handle:%d already freed", txn);
        return GTM_STORE_OK;
    }
    
    bucket = GTM_StoreGetHashBucket(txn_info->gti_gid, strnlen(txn_info->gti_gid, GTM_MAX_SESSION_ID_LEN));
    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreFreeTxn gid:%s is in bucket:%d", txn_info->gti_gid, bucket);
    }

#ifdef __XLOG__
    ret = GTM_RWLockAcquire(g_GTM_Store_Head_Lock, GTM_LOCKMODE_WRITE);
    if (!ret)
    {
        elog(LOG, "GTM_StoreFreeTxn GTM_RWLockAcquire g_GTM_Store_Head_Lock failed:%s", strerror(errno));
        return GTM_STORE_ERROR;
    }
#endif

    ret = AcquireTxnHashLock(bucket, GTM_LOCKMODE_WRITE);
    if (!ret)
    {
        elog(LOG, "GTM_StoreFreeTxn acquire lock for bucket:%d failed", bucket);
        GTM_RWLockRelease(g_GTM_Store_Head_Lock);
        return GTM_STORE_ERROR;
    }

    /* loop through the hash list */
    bucket_handle = GetTxnHashBucket(bucket);
    while (bucket_handle != txn && bucket_handle!= INVALID_STORAGE_HANDLE)
    {
        bucket_info   = GetTxnStore(bucket_handle);    
        bucket_handle = bucket_info->gs_next;
    }

    if (bucket_handle == txn)
    {
        /* first element */
        txn_info = GetTxnStore(bucket_handle);    
        if (bucket_handle == GetTxnHashBucket(bucket))
        {            
            SetTxnHashBucket(bucket, txn_info->gs_next);
            txn_info->gs_next = INVALID_STORAGE_HANDLE;
            flush_bucket = true;            
            
            if (enable_gtm_sequence_debug)
            {
                elog(LOG, "GTM_StoreFreeTxn gid:%s is the only one in bucket:%d", txn_info->gti_gid, bucket);
            }
        }
        else
        {
            bucket_info->gs_next = txn_info->gs_next;
            if (enable_gtm_sequence_debug)
            {
                elog(LOG, "GTM_StoreFreeTxn gid:%s is in the middle of bucket:%d", txn_info->gti_gid, bucket);
            }
        }
        ReleaseTxnHashLock(bucket);
    }
    else if (INVALID_STORAGE_HANDLE == bucket_handle)
    {
        /* should not happen */
        GTM_RWLockRelease(g_GTM_Store_Head_Lock);
        ReleaseTxnHashLock(bucket);
        elog(LOG, "GTM_StoreFreeTxn gid:%s not found in bucket:%d", txn_info->gti_gid, bucket);
        return GTM_STORE_ERROR;
    }

#ifndef __XLOG__
    /* return it to the freelist*/
    ret = GTM_RWLockAcquire(g_GTM_Store_Head_Lock, GTM_LOCKMODE_WRITE);
    if (!ret)
    {
        elog(LOG, "GTM_StoreFreeTxn GTM_RWLockAcquire g_GTM_Store_Head_Lock failed:%s", strerror(errno));
        return GTM_STORE_ERROR;
    }
#endif

    current = GetTxnStore(txn);
    if (INVALID_STORAGE_HANDLE == g_GTM_Store_Header->m_txn_freelist)
    {
        current->gs_next                   = INVALID_STORAGE_HANDLE;
        g_GTM_Store_Header->m_txn_freelist = txn;        
        flush_head    = true;     
    }
    else
    {
        head = GetTxnStore(g_GTM_Store_Header->m_txn_freelist); 
        if (INVALID_STORAGE_HANDLE == head->gs_next)
        {
            current->gs_next = INVALID_STORAGE_HANDLE;
            head->gs_next    = txn;
        }
        else
        {
            next             = GetTxnStore(head->gs_next);    
            head->gs_next    = txn;
            current->gs_next = next->gti_store_handle;
        }
    }

    /* reset the status field */
    current->gti_state           = GTM_TXN_INIT;    
    
    /* flush header */
    if (flush_head)
    {
        GTM_StoreSyncHeader(true);
    }

    /* flush txn head */
    if (head)
    {
        GTM_StoreSyncTxn(head->gti_store_handle);    
    }
    
    /* flush current txn */
    GTM_StoreSyncTxn(txn);    

    /* flush pre hash */
    if (!flush_bucket)
    {
        GTM_StoreSyncTxn(bucket_handle);    
    }
    
    /* flush hash bucket */
    if (flush_bucket)
    {
        GTM_StoreSyncTxnHashBucket(bucket);
    }
    GTM_RWLockRelease(g_GTM_Store_Head_Lock);

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreFreeTxn txn:%d done", txn);
    }
    return GTM_STORE_OK;
}

/* Move the txn from the TXN hash table to free list */
int32 GTM_StoreFinishTxn(char *gid)
{
    int32                      ret = 0;
    GTMStorageHandle          txn = INVALID_STORAGE_HANDLE; 
    
    if (NULL == gid)
    {
        elog(LOG, "GTM_StoreFinishTxn null gid");
        return GTM_STORE_ERROR;
    }

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreFinishTxn for txn:%s begin", gid);
    }
    
    txn = GTM_StoreTxnHashSearch(gid);
    if (INVALID_STORAGE_HANDLE == txn)
    {
        elog(LOG, "GTM_StoreFinishTxn GTM_StoreTxnHashSearch for txn:%s failed", gid);
        return GTM_STORE_ERROR;
    }
    
    ret = GTM_StoreFreeTxn(txn);    
    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreFinishTxn for txn:%s done", gid);
    }
    return ret;
}

/* move the txn from the TXN hash table to free list */
int32 GTM_StoreCommitTxn(char *gid)
{
    int32                      ret = 0;
    GTMStorageHandle          txn = INVALID_STORAGE_HANDLE; 
    
    if (NULL == gid)
    {
        elog(LOG, "GTM_StoreCommitTxn null gid");
        return GTM_STORE_ERROR;
    }

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreCommitTxn for txn:%s", gid);
    }
    
    txn = GTM_StoreTxnHashSearch(gid);
    if (INVALID_STORAGE_HANDLE == txn)
    {
        elog(LOG, "GTM_StoreCommitTxn GTM_StoreTxnHashSearch for txn:%s failed", gid);
        return GTM_STORE_ERROR;
    }
    
    /* process created seq */
    ret = GTM_StoreFreeTxn(txn);
    return ret;
}

/* move the txn from the TXN hash table to free list and abort the seqences*/
int32 GTM_StoreAbortTxn(char *gid)
{
    int32                      ret        = 0;
    GTMStorageHandle          txn        = INVALID_STORAGE_HANDLE; 
    
    if (NULL == gid)
    {
        elog(LOG, "GTM_StoreAbortTxn null gid");
        return GTM_STORE_ERROR;
    }

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreAbortTxn for txn:%s", gid);
    }
    
    txn = GTM_StoreTxnHashSearch(gid);
    if (INVALID_STORAGE_HANDLE == txn)
    {
        elog(LOG, "GTM_StoreAbortTxn GTM_StoreTxnHashSearch for txn:%s failed", gid);
        return GTM_STORE_ERROR;
    }    
    
    /* rollback the sequence status */
    ret = GTM_StoreFreeTxn(txn);
    return ret;
}

#ifdef __XLOG__
void GTM_StoreSizeInit(void)
{
    size_t size = 0;
    size  = ALIGN_PAGE(sizeof(GTMControlHeader));    /* header */ 
    size += ALIGN_PAGE(sizeof(GTM_StoredHashTable)); /* seq hash table */ 
    size += ALIGN_PAGE(sizeof(GTM_StoredHashTable)); /* txn hash table */ 
    size += ALIGN_PAGE(sizeof(GTM_StoredSeqInfo) * GTM_MAX_SEQ_NUMBER); /* sequence table */ 
    size += ALIGN_PAGE(sizeof(GTM_StoredTransactionInfo) * MAX_PREPARED_TXN); /* txn table */ 
    size += PAGE_SIZE * 2;      /* two more pages for align*/
    g_GTMStoreSize = size;

    g_GTM_store_lock_cnt = 0;
    g_GTM_Backup_Timer = INVALID_TIMER_HANDLE;
    GTM_RWLockInit(&g_GTM_Backup_Timer_Lock);
}
#endif

/*
 * Reserve the specific XID number and flush the head to disk.
 */
int32 GTM_StoreReserveXid(int32 count)
{
    bool                ret  = false;
    GlobalTransactionId result;
    GTM_Timestamp now;
    
    now = GTM_TimestampGetCurrent();    
    ret = GTM_RWLockAcquire(g_GTM_Store_Head_Lock, GTM_LOCKMODE_WRITE);
    if (!ret)
    {
        return GTM_STORE_ERROR;
    }    

    result = g_GTM_Store_Header->m_next_gxid + count;
    if (result > g_GTM_Store_Header->m_next_gxid)
    {
        g_GTM_Store_Header->m_next_gxid = result;
    }
    else
    {
        /* over flow */
        g_GTM_Store_Header->m_next_gxid = count - (MaxGlobalTransactionId - g_GTM_Store_Header->m_next_gxid);
        if (g_GTM_Store_Header->m_next_gxid < FirstNormalGlobalTransactionId)
        {
            g_GTM_Store_Header->m_next_gxid = FirstNormalGlobalTransactionId;
        }
    }          
    g_GTM_Store_Header->m_last_update_time = now;    
    GTM_StoreSyncHeader(true);
    GTM_RWLockRelease(g_GTM_Store_Head_Lock);


    elog(LOG, "GTM_StoreReserveXid reserved %d gxid succeed, latest_xid:%u", count, g_GTM_Store_Header->m_next_gxid);
    return GTM_STORE_OK;
}

/*
 * Store latest global timestamp and flush the head to disk.
 */
int32 GTM_StoreGlobalTimestamp(GlobalTimestamp gts)
{
    bool                ret  = false;
    GTM_Timestamp         now;
    int32                 res = GTM_STORE_OK;
    
    now = GTM_TimestampGetCurrent();    
    ret = GTM_RWLockAcquire(g_GTM_Store_Head_Lock, GTM_LOCKMODE_WRITE);
    if (!ret)
    {
        return GTM_STORE_ERROR;
    }    

    /* no need to increase the LSN of header when we store GTS. */
    g_GTM_Store_Header->m_next_gts = gts;
    g_GTM_Store_Header->m_last_update_time = now;

    XLogBeginInsert();

    res = GTM_StoreSyncHeader(false);
    if(res != GTM_STORE_OK)
    {
        elog(LOG, "syncing header fails return %d",res);
        exit(1);
    }

    GTM_RWLockRelease(g_GTM_Store_Head_Lock);

    /* backup gts to slaves */
    BeforeReplyToClientXLogTrigger();

    if(enable_gtm_debug)
    {
        elog(LOG, "GTM_StoreGlobalTimestamp " INT64_FORMAT, g_GTM_Store_Header->m_next_gts);
    }
    return res;
}


/*
 * Restore the gxid and global xim.
 */
int32 GTM_StoreRestore(GlobalTimestamp *gts, GlobalTransactionId *gxid, GlobalTransactionId *global_xmin)
{
    if (NULL == gxid || NULL == global_xmin)
    {
        return GTM_STORE_ERROR;
    }
    *gts          = g_GTM_Store_Header->m_next_gts;
    *gxid        = g_GTM_Store_Header->m_next_gxid;
    *global_xmin = g_GTM_Store_Header->m_global_xmin;
    return GTM_STORE_OK;
}

/*
 * Reserve seq value.
 */
static void GTM_StoreReserveSeqValueIntern(GTM_StoredSeqInfo *seqinfo, int32 count)
{    
    GTM_Sequence distance = 0;
    
    if (count > MAX_SEQUENCE_RESERVED)
    {
        count = MAX_SEQUENCE_RESERVED;
    }
    
    distance = seqinfo->gs_increment_by * count;
    if (SEQ_IS_ASCENDING(seqinfo))
    {
        if ((seqinfo->gs_max_value - seqinfo->gs_value) >= distance)
        {
            seqinfo->gs_value = seqinfo->gs_value + distance;
        }
        else
        {
            if (SEQ_IS_CYCLE(seqinfo))
            {
                seqinfo->gs_value = seqinfo->gs_min_value + (distance - (seqinfo->gs_max_value - seqinfo->gs_value));
            }
            else
            {
                seqinfo->gs_value = seqinfo->gs_max_value;
            }
        }
    }
    else
    {
        if ((seqinfo->gs_min_value - seqinfo->gs_value) >= distance)
        {
            seqinfo->gs_value = seqinfo->gs_value + distance;
        }
        else
        {
            if (SEQ_IS_CYCLE(seqinfo))
            {
                seqinfo->gs_value = seqinfo->gs_max_value + (distance - (seqinfo->gs_min_value - seqinfo->gs_value));
            }
            else
            {
                seqinfo->gs_value = seqinfo->gs_min_value;
            }
        }
    }
}
/*
 * Reserve the specific seq value, and flush the new result to disk.
 */
int32 GTM_StoreReserveSeqValue(GTMStorageHandle seq_handle, int32 value)
{
    GTM_StoredSeqInfo        *seq_info  = NULL;
    
    if (INVALID_STORAGE_HANDLE == seq_handle || seq_handle >= GTM_MAX_SEQ_NUMBER)
    {
        return GTM_STORE_ERROR;
    }

    seq_info = GetSeqStore(seq_handle);
    GTM_StoreReserveSeqValueIntern(seq_info, value);
    
    /* first call */
    if (!seq_info->gs_called)
    {
        seq_info->gs_called = true;
        if (enable_gtm_sequence_debug)
        {
            elog(LOG, "GTM_StoreReserveSeqValue set gs_called for seq:%d", seq_handle);
        }
    }    

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreSyncSeqValue reserve value:%d to gs_value:%zu for seq:%d", value, seq_info->gs_value, seq_handle);
    }
    return GTM_StoreSyncSeq(seq_handle);
}

/*
 * Sync seq new value, and flush the new result to disk.
 */
int32 GTM_StoreSyncSeqValue(GTMStorageHandle seq_handle, GTM_Sequence value)
{
    GTM_StoredSeqInfo        *seq_info  = NULL;
    
    if (!VALID_SEQ_HANDLE(seq_handle))
    {
        elog(LOG, "GTM_StoreSyncSeqValue invalid handle:%d", seq_handle);
        return GTM_STORE_ERROR;
    }

    seq_info = GetSeqStore(seq_handle);
    
    /* first call */
    if (!seq_info->gs_called)
    {
        seq_info->gs_called = true;
        if (enable_gtm_sequence_debug)
        {
            elog(LOG, "GTM_StoreSyncSeqValue set gs_called for seq:%d", seq_handle);
        }
    }    
    seq_info->gs_value = value;
    
    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreSyncSeqValue set gs_value:%zu for seq:%d", value, seq_handle);
    }    
    return GTM_StoreSyncSeq(seq_handle);
}

/*
 * Reset a sequence.
 */
int32 GTM_StoreResetSeq(GTMStorageHandle seq_handle)
{
    return GTM_ResetSyncSeq(seq_handle);
}

/*
 * Set a sequence value.
 */
int32 GTM_StoreSetSeqValue(GTMStorageHandle seq_handle, GTM_Sequence value, bool is_called)
{
    GTM_StoredSeqInfo *seq = NULL;
    
    seq = GetSeqStore(seq_handle);
    seq->m_last_update_time = GTM_TimestampGetCurrent();
    seq->gs_value           = value;

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreSetSeqValue seq:%d value:%zu is_called:%d", seq_handle, value, is_called);
    }    
    return GTM_StoreSync((char*)seq, ALIGN8(sizeof(GTM_StoredSeqInfo)));
}


/*
 * Mark seq as called.
 */
int32 GTM_StoreMarkSeqCalled(GTMStorageHandle seq_handle)
{
    GTM_StoredSeqInfo *seq = NULL;
    
    seq = GetSeqStore(seq_handle);
    seq->m_last_update_time = GTM_TimestampGetCurrent();
    seq->gs_called          = true;

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreMarkSeqCalled seq:%d is called", seq_handle);
    }    
    return GTM_StoreSync((char*)seq, ALIGN8(sizeof(GTM_StoredSeqInfo)));
}

/*
 * Set reserve flag of the seq.
 */
int32 GTM_StoreSetSeqReserve(GTMStorageHandle seq_handle, bool reserve)
{
    GTM_StoredSeqInfo *seq = NULL;
    
    seq = GetSeqStore(seq_handle);
    seq->m_last_update_time = GTM_TimestampGetCurrent();
    seq->gs_reserved        = reserve;

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreSetSeqReserve seq:%d gs_reserved:%d", seq_handle, reserve);
    }    
    return GTM_StoreSync((char*)seq, ALIGN8(sizeof(GTM_StoredSeqInfo)));
}

/*
 * Close a sequence.
 */
int32 GTM_StoreCloseSeq(GTMStorageHandle seq_handle)
{
    return GTM_StoreSyncSeq(seq_handle);
}

/*
 * Drop a sequence.
 */
int32 GTM_StoreDropSeq(GTMStorageHandle seq_handle)
{
    return GTM_StoreFreeSeq(seq_handle);
}
/*
 * Create a new sequence.
 */
GTMStorageHandle
GTM_StoreSeqCreate(GTM_SeqInfo         *raw_seq,
                   char                   *gid)
{
    GTMStorageHandle          seq_handle = INVALID_STORAGE_HANDLE;
    GTM_StoredSeqInfo         *seq_info  = NULL;    

    gid = gid;

    /* create seq in GTM store */
    seq_handle = GTM_StoreAllocSeq(raw_seq->gs_key->gsk_key, raw_seq->gs_key->gsk_type);
    if (INVALID_STORAGE_HANDLE == seq_handle)
    {
        return INVALID_STORAGE_HANDLE;
    }
    
    /* init seq info */
    seq_info = GetSeqStore(seq_handle);    
    seq_info->gs_value         = raw_seq->gs_value;
    seq_info->gs_init_value    = raw_seq->gs_init_value;
    seq_info->gs_increment_by  = raw_seq->gs_increment_by;
    seq_info->gs_min_value     = raw_seq->gs_min_value;
    seq_info->gs_max_value     = raw_seq->gs_max_value;
    seq_info->gs_cycle            = raw_seq->gs_cycle;
    seq_info->gs_called        = raw_seq->gs_called;
    seq_info->gs_reserved       = raw_seq->gs_reserved;
    seq_info->gs_status           = GTM_STORE_SEQ_STATUS_ALLOCATE;    
    
    /* sync the data into data files. */
    GTM_StoreSyncSeq(seq_handle);        
    return seq_handle;
}
/*
 * Rename a sequence.
 */
int32 GTM_StoreSeqRename(GTMStorageHandle seq_handle, 
                         char  *pre_key,
                         char  *cur_key,
                          int32 pre_key_type,
                         int32 cur_key_type)
{// #lizard forgives
    bool                        flush_bucket  = false;
    bool                       ret           = false;

    uint32                       bucket         = 0;
    GTMStorageHandle           bucket_handle = INVALID_STORAGE_HANDLE;
    GTM_StoredSeqInfo         *bucket_info     = NULL;
    GTM_StoredSeqInfo         *seq_info  = NULL;
    
    if (!VALID_SEQ_HANDLE(seq_handle))
    {
        elog(LOG, "GTM_StoreSeqRename invalid handle:%d", seq_handle);
        return GTM_STORE_ERROR;
    }

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreSeqRename seq:%d pre_key:%s cur_key:%s begin", seq_handle, pre_key, cur_key);
    }    
    
    seq_info = GetSeqStore(seq_handle);    
    if (strncmp(seq_info->gs_key.gsk_key, pre_key, SEQ_KEY_MAX_LENGTH) != 0 || 
        seq_handle != seq_info->gti_store_handle)
    {
        elog(LOG, "GTM_StoreSeqRename seq:%d store_handle:%d pre_key:%s store_key:%s not equal", seq_handle, seq_info->gti_store_handle, pre_key, seq_info->gs_key.gsk_key);
        return GTM_STORE_ERROR;
    }

    bucket = GTM_StoreGetHashBucket(seq_info->gs_key.gsk_key, strnlen(seq_info->gs_key.gsk_key, GTM_MAX_SESSION_ID_LEN));
    ret = AcquireSeqHashLock(bucket, GTM_LOCKMODE_WRITE);
    if (!ret)
    {
        return GTM_STORE_ERROR;
    }

    /* loop through the hash list */
    bucket_handle = GetSeqHashBucket(bucket);
    while (bucket_handle != seq_handle && bucket_handle != INVALID_STORAGE_HANDLE)
    {
        bucket_info   = GetSeqStore(bucket_handle); 
        bucket_handle = bucket_info->gs_next;
    }

    if (bucket_handle == seq_handle)
    {
        /* first element, remove seq from the bucket */
        seq_info = GetSeqStore(bucket_handle);    
        if (bucket_handle == GetSeqHashBucket(bucket))
        {            
            SetSeqHashBucket(bucket, seq_info->gs_next);
            seq_info->gs_next = INVALID_STORAGE_HANDLE;
            flush_bucket = true;

            if (enable_gtm_sequence_debug)
            {
                elog(LOG, "GTM_StoreSeqRename seq:%d pre_key:%s is the first element of bucket:%d ", seq_handle, pre_key, bucket);
            }
        }
        else
        {
            bucket_info->gs_next = seq_info->gs_next;
            if (enable_gtm_sequence_debug)
            {
                elog(LOG, "GTM_StoreSeqRename seq:%d pre_key:%s is in the middle of bucket:%d ", seq_handle, pre_key, bucket);
            }
        }
    }
    else
    {
        /* should never happen */
        ReleaseSeqHashLock(bucket);
        elog(LOG, "GTM_StoreSeqRename seq:%d pre_key:%s not found", seq_handle, pre_key);
        return GTM_STORE_ERROR;
    }

    if (flush_bucket)
    {
        GTM_StoreSyncSeqHashBucket(bucket);
    }
    else
    {
        GTM_StoreSyncSeq(bucket_info->gti_store_handle);
    }

    ReleaseSeqHashLock(bucket);

    /* use the new name */
    snprintf(seq_info->gs_key.gsk_key, SEQ_KEY_MAX_LENGTH, "%s", cur_key);
    seq_info->gs_key.gsk_type = cur_key_type;
    GTM_StoreAddSeqToHash(seq_handle);

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreSeqRename seq:%d pre_key:%s cur_key:%s done", seq_handle, pre_key, cur_key);
    }
    return GTM_STORE_OK;    
}

/*
 * alter the sequence, the operation is not transactional.
 * no way to make the operation transactional. 
 */
int32
GTM_StoreSeqAlter(GTM_SeqInfo         *raw_seq,
                  GTMStorageHandle     seq_handle,
                  bool                   restart)
{// #lizard forgives
    GTM_StoredSeqInfo         *seq_info  = NULL;

    if (!VALID_SEQ_HANDLE(seq_handle))
    {
        elog(LOG, "GTM_StoreSeqAlter invalid handle:%d", seq_handle);
        return GTM_STORE_ERROR;
    }

    /* init seq info */
    seq_info = GetSeqStore(seq_handle);    
    
    /* Modify the data if necessary */
    if (seq_info->gs_cycle != raw_seq->gs_cycle)
    {
        seq_info->gs_cycle = raw_seq->gs_cycle;
    }
    
    if (seq_info->gs_min_value !=  raw_seq->gs_min_value)
    {
        seq_info->gs_min_value =  raw_seq->gs_min_value;
    }
    
    if (seq_info->gs_max_value != raw_seq->gs_max_value)
    {
        seq_info->gs_max_value = raw_seq->gs_max_value;
    }
    
    if (seq_info->gs_increment_by != raw_seq->gs_increment_by)
    {
        seq_info->gs_increment_by = raw_seq->gs_increment_by;
    }

    /*
     * Check start/restart processes.
     * Check first if restart is necessary and reset sequence in that case.
     * If not, check if a simple start is necessary and update sequence.
     */
    if (restart)
    {
        /* Restart command has been used, reset the sequence */
        seq_info->gs_called   = false;
        seq_info->gs_value    = raw_seq->gs_value;
        seq_info->gs_reserved = false;
    }
    
    if (seq_info->gs_init_value != raw_seq->gs_init_value)
    {
        seq_info->gs_init_value = raw_seq->gs_init_value;
    }    
    
    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreSeqAlter seq:%s seq:%d done", seq_info->gs_key.gsk_key, seq_handle);
    }
    
    /* sync the data into data files. */
    GTM_StoreSyncSeq(seq_handle);
    return GTM_STORE_OK;
}
/*
 * Load a new sequence into memory from GTM store.
 */
GTMStorageHandle GTM_StoreLoadSeq(GTM_SeqInfo *raw_seq)
{
    GTMStorageHandle          seq_handle = INVALID_STORAGE_HANDLE;
    GTM_StoredSeqInfo         *seq_info  = NULL;
    

    /* create seq in GTM store */
    seq_handle = GTM_StoreSeqHashSearch(raw_seq->gs_key->gsk_key, raw_seq->gs_key->gsk_type);
    if (INVALID_STORAGE_HANDLE == seq_handle)
    {
        elog(LOG, "GTM_StoreLoadSeq seq: %s not found in gtm store", raw_seq->gs_key->gsk_key);
        return INVALID_STORAGE_HANDLE;
    }
    
    /* init seq info */
    seq_info = GetSeqStore(seq_handle);    
    raw_seq->gs_value         = seq_info->gs_value;
    raw_seq->gs_init_value    = seq_info->gs_init_value;
    raw_seq->gs_increment_by  = seq_info->gs_increment_by;
    raw_seq->gs_min_value     = seq_info->gs_min_value;
    raw_seq->gs_max_value     = seq_info->gs_max_value;
    raw_seq->gs_cycle           = seq_info->gs_cycle;
    raw_seq->gs_called        = seq_info->gs_called;
    raw_seq->gs_reserved      = seq_info->gs_reserved;

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreLoadSeq seq: %s found in store, seq:%d", raw_seq->gs_key->gsk_key, seq_handle);
    }
    return seq_handle;
}

/*
 * Begin a prepare transaction in GTM STORE.
 */
int32 GTM_StoreBeginPrepareTxn(char *gid, char *node_string)
{
    GTMStorageHandle           txn             = INVALID_STORAGE_HANDLE;
    GTM_StoredTransactionInfo *store_txn_info  = NULL;

    if (!gid || !node_string)
    {
        elog(LOG, "GTM_StoreBeginPrepareTxn invalid null parameter");
        return GTM_STORE_ERROR;
    }

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreBeginPrepareTxn gid:%s node_string:%s", gid, node_string);
    }
    
    /* Check whether the transaction already exist */
    txn = GTM_StoreTxnHashSearch(gid);
    if (txn != INVALID_STORAGE_HANDLE)
    {        
        elog(ERROR, "GTM_StoreBeginPrepareTxn gid:%s node_string:%s already exist", gid, node_string);
        return GTM_STORE_ERROR;
    }
    else
    {
        if (enable_gtm_sequence_debug)
        {
            elog(LOG, "GTM_StoreBeginPrepareTxn create new txn gid:%s node_string:%s", gid, node_string);
        }
        txn = GTM_StoreAllocTxn(gid);
        store_txn_info = GetTxnStore(txn);
        snprintf(store_txn_info->gti_gid, GTM_MAX_SESSION_ID_LEN, "%s", gid);
        snprintf(store_txn_info->nodestring, GTM_MAX_SESSION_ID_LEN, "%s", node_string);    
        store_txn_info->gti_state     = GTM_TXN_PREPARED;
    }
    
    return GTM_StoreSyncTxn(txn);
}

static GTM_TransactionDebugInfo* GTM_SearchLogEntry(const char *gid)
{
    int i;
    GTM_TransactionDebugInfo *gti;
    
    for(i = g_GTM_DebugHeader.m_txn_buffer_last; i < g_GTM_DebugHeader.m_txn_buffer_len; i++)
    {
        gti = GetTxnDebugEntry(i);
        if(gti->state != GTMTxnEmpty && strcmp(gid, gti->gid) == 0)
        {
            return gti;
        }
    }

    for(i = 0; i < g_GTM_DebugHeader.m_txn_buffer_last; i++)
    {
        gti = GetTxnDebugEntry(i);
        if(gti->state != GTMTxnEmpty && strcmp(gid, gti->gid) == 0)
        {
            return gti;
        }
    }
    
    
    return NULL;
}
static char timebuf[128] = { 0 };
static char *log_time(void)
{
    struct tm tm_s;
    time_t now;
    
    now = time(NULL);
    localtime_r(&now,&tm_s);

    snprintf(timebuf, 128, "%02d-%02d-%02d %02d:%02d:%02d", 
             ((tm_s.tm_year+1900) >= 2000) ? (tm_s.tm_year + (1900 - 2000)) : tm_s.tm_year, 
             tm_s.tm_mon+1, tm_s.tm_mday, tm_s.tm_hour, tm_s.tm_min, tm_s.tm_sec);
    return timebuf;
}


static bool GTMLogEntryEmpty(GTM_TransactionDebugInfo *gti)
{
    if(gti->state == GTMTxnEmpty)
        return true;

    if(gti->entryType == GTMTypeTransaction &&
        (GTM_TimestampGetCurrent() - gti->entry_create_timestamp > ENTRY_STALE_THRESHOLD))
    {
        GTM_NodeDebugInfo *nti = gti->node_list_head, *prev;

          fprintf(g_GTMDebugLogFile, "[%s] incomplete entry gxid %d gid %s nodestring %s node count %d isCommit %d "
                                          "global prepare timestamp "INT64_FORMAT " global commit timestamp "INT64_FORMAT "\n", 
                log_time(), gti->gxid, gti->gid, gti->nodestring, gti->node_count, 
                gti->isCommit, gti->prepare_timestamp, gti->commit_timestamp);
        while(nti)
        {

            fprintf(g_GTMDebugLogFile, "[%s] incomplete entry gxid %d gid %s node %s isCommit %d local prepare timestamp "
                                        INT64_FORMAT " local commit timestamp "INT64_FORMAT "\n", 
                log_time(), nti->gxid, gti->gid,  nti->node_name, nti->isCommit, nti->prepare_timestamp, nti->commit_timestamp);
            
            prev = nti;
            nti = nti->next;
            pfree(prev);
        }
        memset(gti, 0, sizeof(GTM_TransactionDebugInfo));
        return true;
    }

    return false;
}
static GTM_TransactionDebugInfo* GTM_SearchEmptyLogEntry(void)
{
    int i;
    GTM_TransactionDebugInfo *gti;
    
    for(i = g_GTM_DebugHeader.m_txn_buffer_last; i < g_GTM_DebugHeader.m_txn_buffer_len; i++)
    {
        gti = GetTxnDebugEntry(i);
        if(GTMLogEntryEmpty(gti))
        {
            g_GTM_DebugHeader.m_txn_buffer_last = i;
            return gti;
        }
    }

    for(i = 0; i < g_GTM_DebugHeader.m_txn_buffer_last; i++)
    {
        gti = GetTxnDebugEntry(i);
        if(GTMLogEntryEmpty(gti))
        {
            g_GTM_DebugHeader.m_txn_buffer_last = i;
            return gti;
        }
    }

    return NULL;
}

static void GTM_PrintAndClearLogEntry(GTM_TransactionDebugInfo *gti)
{
    GTM_NodeDebugInfo *nti = gti->node_list_head, *prev;

   fprintf(g_GTMDebugLogFile, "[%s] global entry gxid %d gid %s nodestring %s node count %d isCommit %d "
                                "global prepare timestamp "INT64_FORMAT " global commit timestamp "INT64_FORMAT "\n", 
        log_time(), gti->gxid, gti->gid, gti->nodestring, gti->node_count, 
        gti->isCommit, gti->prepare_timestamp, gti->commit_timestamp);
    while(nti)
    {

        fprintf(g_GTMDebugLogFile, "[%s] gxid %d gid %s node %s isCommit %d local prepare timestamp "
                                    INT64_FORMAT " local commit timestamp "INT64_FORMAT "\n", 
            log_time(), gti->gxid, gti->gid,  nti->node_name, nti->isCommit, nti->prepare_timestamp, nti->commit_timestamp);
        
        prev = nti;
        nti = nti->next;
        pfree(prev);
    }
    gti->state = GTMTxnComplete;
    memset(gti, 0, sizeof(GTM_TransactionDebugInfo));

}


int32 GTM_StoreLogTransaction(GlobalTransactionId gxid,
                                        const char *gid, 
                                        const char *node_string, 
                                        int node_count, 
                                        int isGlobal, 
                                        int isCommit, 
                                        GlobalTimestamp prepare_ts, 
                                        GlobalTimestamp commit_ts)
{// #lizard forgives
    GTM_TransactionDebugInfo *gti;

    elog(DEBUG1, "Store log transaction gxid %u gid %s node string %s node count %d isGlobal %d isCommit %d "
                "prepare ts "INT64_FORMAT " commit ts "INT64_FORMAT,
                gxid, gid, node_string, node_count, isGlobal, isCommit, prepare_ts, commit_ts);
    GTM_RWLockAcquire(&g_GTM_Debug_Lock, GTM_LOCKMODE_WRITE);
    if(!isGlobal)
    {
        gti = GTM_SearchLogEntry(gid);
        if(NULL == gti)
        {
            elog(LOG, "no entry found for gid %s", gid);
            GTM_RWLockRelease(&g_GTM_Debug_Lock);
            return GTM_STORE_ERROR;
        }
        gti->complete_node_count++;
        if(NULL == gti->node_list_tail)
        {
            gti->node_list_tail = gti->node_list_head = palloc(sizeof(GTM_NodeDebugInfo));
        }
        else
        {
            gti->node_list_tail->next = palloc(sizeof(GTM_NodeDebugInfo));
            gti->node_list_tail = gti->node_list_tail->next;

        }
        gti->node_list_tail->next = NULL;
        if(node_string)
        {
            strcpy(gti->node_list_tail->node_name, node_string);
        }
        else
        {
            gti->node_list_tail->node_name[0] = '\0';
        }
        gti->node_list_tail->prepare_timestamp = prepare_ts;
        gti->node_list_tail->commit_timestamp = commit_ts;
        gti->node_list_tail->isCommit = isCommit;
        gti->node_list_tail->gxid = gxid;
        
        if(gti->complete_node_count == gti->node_count)
        {    
            GTM_PrintAndClearLogEntry(gti);
        }
    
    }
    else
    {
        gti = GTM_SearchLogEntry(gid);
        if(gti)
        {
            if(isCommit)
            {
                elog(LOG, "existing entry found for global commit gid %s", gid);
                GTM_RWLockRelease(&g_GTM_Debug_Lock);
                return GTM_STORE_ERROR;
            }
            else //abort condition 
            {
                gti->isCommit = false;
                GTM_PrintAndClearLogEntry(gti);
                GTM_RWLockRelease(&g_GTM_Debug_Lock);
                return GTM_STORE_OK;
            }
        }
        
        gti = GTM_SearchEmptyLogEntry();
        if(NULL == gti)
        {
            elog(LOG, "no empty slot for gid %s", gid);
            GTM_RWLockRelease(&g_GTM_Debug_Lock);
            return GTM_STORE_ERROR;
        }

        gti->entryType = GTMTypeTransaction;
        gti->entry_create_timestamp = GTM_TimestampGetCurrent();
        strcpy(gti->gid, gid);
        if(node_string)
        {
            strcpy(gti->nodestring, node_string);
        }
        else
        {
            gti->nodestring[0] = '\0';
        }
        gti->prepare_timestamp = prepare_ts;
        gti->commit_timestamp = commit_ts;
        gti->isCommit = isCommit;
        gti->node_count = node_count;
        gti->complete_node_count = 0;
        gti->gxid = gxid;
        gti->state = GTMTxnInit;
    }
    

    GTM_RWLockRelease(&g_GTM_Debug_Lock);
    return GTM_STORE_OK;
}
int32 GTM_StoreLogScan(GlobalTransactionId gxid,
                                 const char *nodestring,
                                GlobalTimestamp start_ts,
                                GlobalTimestamp local_start_ts,
                                GlobalTimestamp local_complete_ts,
                                int scan_type,
                                 const char *rel_name,
                                 int64 scan_number)
{

    elog(DEBUG1, "Store log scan gxid %u node string %s start_ts "INT64_FORMAT 
        " local start ts "INT64_FORMAT
        " local complete ts "INT64_FORMAT
        " scan_type %s rel_name %s scan number "INT64_FORMAT,
                gxid, nodestring, start_ts, local_start_ts, local_complete_ts, 
                scan_type_tab[scan_type].name, rel_name, scan_number);
    GTM_RWLockAcquire(&g_GTM_Scan_Debug_Lock, GTM_LOCKMODE_WRITE);

    fprintf(g_GTMDebugScanLogFile, "[%s] scan entry gxid %d node %s start_ts "INT64_FORMAT
                    " local start ts "INT64_FORMAT
                    " local complete ts "INT64_FORMAT
                    " scantype %s rel_name %s scan number "INT64_FORMAT "\n", 
            log_time(), gxid, nodestring, start_ts, local_start_ts, local_complete_ts, 
                    scan_type_tab[scan_type].name, rel_name, scan_number);
    GTM_RWLockRelease(&g_GTM_Scan_Debug_Lock);
    return GTM_STORE_OK;
}

/*
 * Retrive the GTM store to get the prepared transaction info. 
 */
GTMStorageHandle GTM_StoreGetPreparedTxnInfo(char *gid, GlobalTransactionId *gxid, char **nodestring)
{
    GTMStorageHandle           txn             = INVALID_STORAGE_HANDLE;
    GTM_StoredTransactionInfo *store_txn_info  = NULL;
    
    /* Check whether the transaction already exist */
    txn = GTM_StoreTxnHashSearch(gid);
    if (txn != INVALID_STORAGE_HANDLE)
    {
        store_txn_info = GetTxnStore(txn);
        if (0 == strncmp(gid, store_txn_info->gti_gid, GTM_MAX_SESSION_ID_LEN) &&
            GTM_TXN_IMPLICATE_PREPARED == store_txn_info->gti_state)
        {
            ereport(ERROR,
                    (EPROTO,
                      errmsg("expect transaction name is '%s' status is '%d', the stored one is '%s' status is '%d'", gid, GTM_TXN_IMPLICATE_PREPARED, store_txn_info->gti_gid, store_txn_info->gti_state)));
            return INVALID_STORAGE_HANDLE;
        }
        *gxid = InvalidGlobalTransactionId;
        *nodestring = store_txn_info->nodestring;
        return store_txn_info->gti_store_handle;
    }
    return INVALID_STORAGE_HANDLE;
}
/*
 * write the store to disk. 
 */
int32 GTM_StoreInitSync(char *data, size_t size)
{// #lizard forgives
    int32  nbytes = 0;
    int32  ret    = 0;
    size_t offset = 0;    
    offset = data - g_GTMStoreMapAddr;
    if (data < g_GTMStoreMapAddr || 
        offset > g_GTMStoreSize  || 
        -1 == g_GTMStoreMapFile  ||
        (size + offset) > g_GTMStoreSize)
    {
        return GTM_STORE_ERROR;
    }

    /* seek to the position */
    nbytes = lseek(g_GTMStoreMapFile, offset, SEEK_SET);
    if (nbytes != offset)
    {
        elog(LOG, "could not seek map file for: %s", strerror(errno));
        return GTM_STORE_ERROR;
    }

    /* write the data */
    nbytes = write(g_GTMStoreMapFile, data, size);
    if (size != nbytes)
    {
        elog(LOG, "could not write map for: %s, required bytes:%zu, return bytes:%d", strerror(errno), size, nbytes);
        return GTM_STORE_ERROR;
    }    

    ret = fsync(g_GTMStoreMapFile);
    if (ret)
    {
        elog(LOG, "could not fsync map file for: %s", strerror(errno));
        return GTM_STORE_ERROR;
    }
    return ret;
}
/*
 * write the store to xlog. 
 */
int32 GTM_StoreSync(char *data, size_t size)
{
    size_t offset = 0;    
    offset = data - g_GTMStoreMapAddr;
    
    if (data < g_GTMStoreMapAddr || 
        offset > g_GTMStoreSize  || 
        -1 == g_GTMStoreMapFile  ||
        (size + offset) > g_GTMStoreSize)
    {
        return GTM_STORE_ERROR;
    }

    if( GetMyThreadInfo->register_buff == NULL)
        return GTM_STORE_SKIP;

    XLogRegisterRangeOverwrite(offset,size,data);
    
    return GTM_STORE_OK;
}

bool GTM_StoreCheckSeqCRC(GTM_StoredSeqInfo *seq)
{
    pg_crc32c          crc32 = 0;    
    
    /* calculate CRC */
    INIT_CRC32C(crc32);
    COMP_CRC32C(crc32,
                (char *) seq,
                offsetof(GTM_StoredSeqInfo, gs_crc));
    FIN_CRC32C(crc32);
    
    return crc32 == seq->gs_crc;
}

bool GTM_StoreCheckTxnCRC(GTM_StoredTransactionInfo *txn)
{
    pg_crc32c          crc32 = 0;    
    
    /* calculate CRC */
    INIT_CRC32C(crc32);
    COMP_CRC32C(crc32,
                (char *) txn,
                offsetof(GTM_StoredTransactionInfo, gti_crc));
    FIN_CRC32C(crc32);    
    return crc32 == txn->gti_crc;
}


bool  GTM_StoreSeqInFreelist(GTM_StoredSeqInfo *seq)
{// #lizard forgives
    bool                found         = false;
    int32                 ret;
    GTMStorageHandle    bucket_handle = INVALID_STORAGE_HANDLE;
    GTM_StoredSeqInfo  *seq_info      = NULL;
    
    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreSeqInFreelist enter");
    }
    
    ret = GTM_RWLockAcquire(g_GTM_Store_Head_Lock, GTM_LOCKMODE_READ);
    if (!ret)
    {
        elog(LOG, "GTM_StoreSeqInFreelist lock failure");
        return false;
    }
    
    if (INVALID_STORAGE_HANDLE == g_GTM_Store_Header->m_seq_freelist)
    {
        ret = GTM_RWLockRelease(g_GTM_Store_Head_Lock);
        if (!ret)
        {
            elog(LOG, "GTM_StoreSeqInFreelist release lock failure");            
        }
        return false;
    }


    bucket_handle = g_GTM_Store_Header->m_seq_freelist;
    while (INVALID_STORAGE_HANDLE != bucket_handle)
    {    
        seq_info = GetSeqStore(bucket_handle);        
        if (seq->gti_store_handle == seq_info->gti_store_handle)
        {
            found = true;
            break;
        }        
        bucket_handle = seq_info->gs_next;
    }
    
    ret = GTM_RWLockRelease(g_GTM_Store_Head_Lock);
    if (!ret)
    {
        elog(LOG, "GTM_StoreSeqInFreelist release lock failure");
    }
    
    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreSeqInFreelist done");
    }
    return found;
}


bool  GTM_StoreTxnInFreelist(GTM_StoredTransactionInfo *txn)
{// #lizard forgives
    bool                found         = false;
    int32                 ret;
    GTMStorageHandle    bucket_handle = INVALID_STORAGE_HANDLE;
    GTM_StoredTransactionInfo  *txn_info      = NULL;
    
    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreTxnInFreelist enter");
    }
    
    ret = GTM_RWLockAcquire(g_GTM_Store_Head_Lock, GTM_LOCKMODE_READ);
    if (!ret)
    {
        elog(LOG, "GTM_StoreTxnInFreelist lock failure");
        return false;
    }
    
    if (INVALID_STORAGE_HANDLE == g_GTM_Store_Header->m_txn_freelist)
    {
        ret = GTM_RWLockRelease(g_GTM_Store_Head_Lock);
        if (!ret)
        {
            elog(LOG, "GTM_StoreTxnInFreelist release lock failure");            
        }
        return false;
    }


    bucket_handle = g_GTM_Store_Header->m_txn_freelist;
    while (INVALID_STORAGE_HANDLE != bucket_handle)
    {    
        txn_info = GetTxnStore(bucket_handle);        
        if (txn->gti_store_handle == txn_info->gti_store_handle)
        {
            found = true;
            break;
        }        
        bucket_handle = txn_info->gs_next;
    }
    
    ret = GTM_RWLockRelease(g_GTM_Store_Head_Lock);
    if (!ret)
    {
        elog(LOG, "GTM_StoreTxnInFreelist release lock failure");
    }
    
    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreTxnInFreelist done");
    }
    return found;
}

/*
 * Handle storage transfer command.
 */
void
ProcessStorageTransferCommand(Port *myport, StringInfo message)
{// #lizard forgives
#define   MAX_PKG_PER_SEND  2048
    int32          loop_cnt         = 0;
    uint32         storage_size = 0;
    int32           offset        = 0;    
    int32           snd_length    = 0;    
    StringInfoData buf;
    int i;

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "ProcessStorageTransferCommand begin");
    }    
    
    /* Lock the storage, this will block some sequence opration and prepared stmt */    
    pq_getmsgend(message);

    GTM_RWLockAcquire(&g_GTM_Backup_Timer_Lock,GTM_LOCKMODE_WRITE);
    if(g_GTM_Backup_Timer != INVALID_TIMER_HANDLE)
    {
        GTM_RWLockRelease(&g_GTM_Backup_Timer_Lock);
        elog(ERROR, "one standby is in basebackup;");
    }
    GTM_RWLockRelease(&g_GTM_Backup_Timer_Lock);

    pq_beginmessage(&buf, 'S');
    pq_sendint(&buf, STORAGE_TRANSFER_RESULT, 4);

    if (myport->remote_type == GTM_NODE_GTM_PROXY)
    {
        GTM_ProxyMsgHeader proxyhdr;
        proxyhdr.ph_conid = myport->conn_id;
        pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
    }
#ifdef __XLOG__
    if(GTM_StoreLock() == false)
    {
        elog(LOG,"Error store lock fails");
        exit(1);
    }

    GTM_RWLockAcquire(&g_GTM_Backup_Timer_Lock,GTM_LOCKMODE_WRITE);
    g_GTM_Backup_Timer =  GTM_AddTimer(LockStoreStandbyCrashHandler, GTM_TIMER_TYPE_ONCE, LOCK_STORE_CRASH_HANDL_TIMEOUT, GetMyThreadInfo);
    if(g_GTM_Backup_Timer == INVALID_TIMER_HANDLE)
    {
        GTM_RWLockRelease(&g_GTM_Backup_Timer_Lock);
        elog(ERROR, "Failed to register lock store crash handler, will exit!");
    }
    GTM_RWLockRelease(&g_GTM_Backup_Timer_Lock);

    /* send xlog replication relative data */
    pq_sendint64(&buf,GetXLogFlushRecPtr()); /* start pos */
    pq_sendint(&buf,GetCurrentTimeLineID(),4); /* time line */
#endif

    /* Send a number of sequences */
    storage_size = (uint32)g_GTMStoreSize;
    pq_sendint(&buf, storage_size, sizeof(storage_size));    
    
    loop_cnt = ALIGN_UP(storage_size, MAX_PKG_PER_SEND) / MAX_PKG_PER_SEND;        
    pq_sendint(&buf, loop_cnt, sizeof(loop_cnt));    
    
    offset = 0;
    for (i = 0 ; i < loop_cnt ; i++)
    {            
        if ((offset + MAX_PKG_PER_SEND) <= storage_size)
        {
            snd_length = MAX_PKG_PER_SEND;
        }
        else
        {
            snd_length = storage_size - offset;
        }
        pq_sendint(&buf, snd_length, sizeof(snd_length));
        pq_sendbytes(&buf, g_GTMStoreMapAddr + offset, snd_length);
        offset += snd_length;
    }

    if (offset != storage_size)
    {
        elog(PANIC, "ProcessStorageTransferCommand fatal error.");
    }
    pq_endmessage(myport, &buf);
        
    elog(DEBUG1, "ProcessSequenceListCommand() done.");

    if (myport->remote_type != GTM_NODE_GTM_PROXY)
    {
        /* Don't flush to the backup because this does not change the internal status */
        pq_flush(myport);
    }

    /* release the lock */    
    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "ProcessStorageTransferCommand done, offset:%d", offset);
    }
}

/*
 * Handle get gtm header command.
 */
void
ProcessGetGTMHeaderCommand(Port *myport, StringInfo message)
{
    int32           ret          = 0;    
    int32          used_seq     = 0;
    int32          used_txn     = 0;
    
    StringInfoData buf;
    GTMControlHeader header;

    if (Recovery_IsStandby())
    {
        ereport(ERROR,
            (EPERM,
             errmsg("Operation not permitted under the standby mode.")));
    }

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "ProcessGetGTMHeaderCommand begin");
    }    

    pq_getmsgend(message);
    
    ret = GTM_StoreGetHeader(&header);
    if (ret)
    {
        elog(ERROR, "ProcessGetGTMHeaderCommand GTM_StoreGetHeader failed");
    }
    
    used_seq = GTM_StoreGetUsedSeq();
    used_txn = GTM_StoreGetUsedTxn();
    
    pq_beginmessage(&buf, 'S');
    pq_sendint(&buf, MSG_LIST_GTM_STORE_RESULT, 4);

    if (myport->remote_type == GTM_NODE_GTM_PROXY)
    {
        GTM_ProxyMsgHeader proxyhdr;
        proxyhdr.ph_conid = myport->conn_id;
        pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
    }

    pq_sendint64(&buf, header.m_identifier);
    pq_sendint(&buf, header.m_major_version, sizeof(header.m_major_version));    
    pq_sendint(&buf, header.m_minor_version, sizeof(header.m_minor_version));    
    pq_sendint(&buf, header.m_gtm_status, sizeof(header.m_gtm_status));    
    pq_sendint64(&buf, header.m_next_gts);
    
    pq_sendint(&buf, header.m_global_xmin, sizeof(header.m_global_xmin));
    pq_sendint(&buf, header.m_next_gxid, sizeof(header.m_next_gxid));    
    pq_sendint(&buf, header.m_seq_freelist, sizeof(header.m_seq_freelist));
    pq_sendint(&buf, header.m_txn_freelist, sizeof(header.m_txn_freelist));

    pq_sendint64(&buf, header.m_lsn);
    pq_sendint64(&buf, header.m_last_update_time);
    
    pq_sendint(&buf, header.m_crc, sizeof(header.m_crc));

    /* used info */
    pq_sendint(&buf, GTM_MAX_SEQ_NUMBER, sizeof(int32));
    pq_sendint(&buf, used_seq, sizeof(int32));
    pq_sendint(&buf, MAX_PREPARED_TXN, sizeof(int32));
    pq_sendint(&buf, used_txn, sizeof(int32));
    
    pq_endmessage(myport, &buf);    

    if (myport->remote_type != GTM_NODE_GTM_PROXY)
    {
        /* Don't flush to the backup because this does not change the internal status */
        pq_flush(myport);
    }

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "ProcessGetGTMHeaderCommand done");
    }
}

int32 GTM_StoreGetHeader(GTMControlHeader *header)
{
    bool ret                                = false;

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreGetHeader enter");
    }
    
    ret = GTM_RWLockAcquire(g_GTM_Store_Head_Lock, GTM_LOCKMODE_WRITE);
    if (!ret)
    {
        elog(LOG, "GTM_StoreGetHeader lock failure");
        return GTM_STORE_ERROR;
    }

    memcpy((char*)header, g_GTM_Store_Header, sizeof(GTMControlHeader));
    ret = GTM_RWLockRelease(g_GTM_Store_Head_Lock);
    if (!ret)
    {
        elog(LOG, "GTM_StoreGetHeader release lock failure");
        return GTM_STORE_ERROR;
    }
    
    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreGetHeader done");
    }
    return GTM_STORE_OK;
}

int32 GTM_StoreGetUsedSeq(void)
{
    int32 i     = 0;
    int32 seq_count = 0;
    bool ret    = false;
    GTMStorageHandle bucket_handle = INVALID_STORAGE_HANDLE;
    GTM_StoredSeqInfo *seq_info;

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreGetUsedSeq enter");
    }
    
    for (i = 0 ; i < GTM_STORED_HASH_TABLE_NBUCKET ; i++)
    {
        ret = AcquireSeqHashLock(i, GTM_LOCKMODE_READ);
        if (!ret)
        {
            elog(LOG, "AcquireTxnHashLock %d failed", i);
        }        
        
        bucket_handle = GetSeqHashBucket(i);
        while (INVALID_STORAGE_HANDLE != bucket_handle)
        {    
            seq_info = GetSeqStore(bucket_handle);
                    
            seq_count++;            
            bucket_handle = seq_info->gs_next;
        }
        ReleaseSeqHashLock(i);
    }    
    
    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreGetUsedSeq done");
    }
    return seq_count;
}

int32 GTM_StoreGetUsedTxn(void)
{
    int32 i     = 0;
    int32 txn_count = 0;
    bool ret    = false;
    GTMStorageHandle bucket_handle = INVALID_STORAGE_HANDLE;
    GTM_StoredTransactionInfo *txn_info;

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreGetUsedTxn enter");
    }
    
    for (i = 0 ; i < GTM_STORED_HASH_TABLE_NBUCKET ; i++)
    {
        ret = AcquireTxnHashLock(i, GTM_LOCKMODE_READ);
        if (!ret)
        {
            elog(LOG, "AcquireTxnHashLock %d failed", i);
        }        
        
        bucket_handle = GetTxnHashBucket(i);
        while (INVALID_STORAGE_HANDLE != bucket_handle)
        {    
            txn_info = GetTxnStore(bucket_handle);
                    
            txn_count++;            
            bucket_handle = txn_info->gs_next;
        }
        ReleaseTxnHashLock(i);
    }    
    
    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreGetUsedTxn done");
    }
    return txn_count;
}

int32 GTM_StoreDropAllSeqInDatabase(GTM_SequenceKey seq_database_key)
{// #lizard forgives
    int                    seq_count     = 0;
    int                    seq_maxcount  = 0;
    GTM_StoredSeqInfo  *seq_list      = NULL;
    int                    i             = 0;
    GTMStorageHandle    bucket_handle = INVALID_STORAGE_HANDLE;
    GTM_StoredSeqInfo  *seq_info      = NULL;
    bool                ret           = false;

    if (Recovery_IsStandby())
    {
        ereport(ERROR,
            (EPERM,
             errmsg("GTM_StoreDropAllSeqInDatabase Operation not permitted under the standby mode.")));
    }

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreDropAllSeqInDatabase enter");
    }


    seq_count    = 0;
    seq_maxcount = 1024;
    seq_list = (GTM_StoredSeqInfo *) palloc(seq_maxcount * sizeof(GTM_StoredSeqInfo));
    for (i = 0 ; i < GTM_STORED_HASH_TABLE_NBUCKET ; i++)
    {
        ret = AcquireSeqHashLock(i, GTM_LOCKMODE_READ);
        if (!ret)
        {
            elog(LOG, "AcquireTxnHashLock %d failed", i);
        }

        bucket_handle = GetSeqHashBucket(i);
        while (INVALID_STORAGE_HANDLE != bucket_handle)
        {
            seq_info = GetSeqStore(bucket_handle);

            if(!(strncmp(seq_database_key->gsk_key,seq_info->gs_key.gsk_key,seq_database_key->gsk_keylen - 1) == 0 &&
                 seq_info->gs_key.gsk_key[seq_database_key->gsk_keylen - 1] == '.'))
            {
                bucket_handle = seq_info->gs_next;
                continue;
            }

            if (seq_count >= seq_maxcount)
            {
                int                   newcount = NULL;
                GTM_StoredSeqInfo    *newlist  = NULL;

                newcount = 2 * seq_maxcount;
                newlist  = (GTM_StoredSeqInfo *) repalloc(seq_list, newcount * sizeof(GTM_StoredSeqInfo));
                /*
                 * If failed try to get less. It is unlikely to happen, but
                 * let's be safe.
                 */
                while (newlist == NULL)
                {
                    newcount = seq_maxcount + (newcount - seq_maxcount) / 2 - 1;
                    if (newcount <= seq_maxcount)
                    {
                        ReleaseSeqHashLock(i);
                        /* give up */
                        ereport(ERROR,
                                (ERANGE,
                                 errmsg("Can not list all the sequences")));
                    }
                    newlist = (GTM_StoredSeqInfo *) repalloc(seq_list, newcount * sizeof(GTM_StoredSeqInfo));
                }
                seq_maxcount = newcount;
                seq_list = newlist;
            }
            memcpy(&seq_list[seq_count], seq_info, sizeof(GTM_StoredSeqInfo));
            seq_count++;

            bucket_handle = seq_info->gs_next;
        }
        ReleaseSeqHashLock(i);
    }

    for(i = 0 ; i < seq_count;i++)
    {
        if (enable_gtm_sequence_debug)
        {
            elog(LOG, "GTM_StoreDropAllSeqInDatabase drop %s",seq_list[i].gs_key.gsk_key);
        }

        if (GTM_StoreDropSeq(seq_list[i].gti_store_handle))
        {
            ereport(LOG,
                    (ERANGE,
                            errmsg("GTM_StoreDropSeq fail")));
        }
    }

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreDropAllSeqInDatabase finish");
    }

    return  GTM_STORE_OK;
}

void
ProcessListStorageSequenceCommand(Port *myport, StringInfo message)
{// #lizard forgives
    StringInfoData         buf;
    bool                ret             = false;
    int                    seq_count     = 0;
    int                    seq_maxcount  = 0;
    GTM_StoredSeqInfo  *seq_list      = NULL;
    int                    i             = 0;
    GTMStorageHandle    bucket_handle = INVALID_STORAGE_HANDLE;
    GTM_StoredSeqInfo  *seq_info      = NULL;

    if (Recovery_IsStandby())
    {
        ereport(ERROR,
            (EPERM,
             errmsg("ProcessListStorageSequenceCommand Operation not permitted under the standby mode.")));
    }

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "ProcessListStorageSequenceCommand enter");
    }
    
    seq_count    = 0;
    seq_maxcount = 1024;
    seq_list = (GTM_StoredSeqInfo *) palloc(seq_maxcount * sizeof(GTM_StoredSeqInfo));
    for (i = 0 ; i < GTM_STORED_HASH_TABLE_NBUCKET ; i++)
    {
        ret = AcquireSeqHashLock(i, GTM_LOCKMODE_READ);
        if (!ret)
        {
            elog(LOG, "AcquireTxnHashLock %d failed", i);
        }        
        
        bucket_handle = GetSeqHashBucket(i);
        while (INVALID_STORAGE_HANDLE != bucket_handle)
        {    
            seq_info = GetSeqStore(bucket_handle);
            if (seq_count >= seq_maxcount)
            {
                int                   newcount = NULL;
                GTM_StoredSeqInfo    *newlist  = NULL;
                
                newcount = 2 * seq_maxcount;
                newlist  = (GTM_StoredSeqInfo *) repalloc(seq_list, newcount * sizeof(GTM_StoredSeqInfo));
                /*
                 * If failed try to get less. It is unlikely to happen, but
                 * let's be safe.
                 */
                while (newlist == NULL)
                {
                    newcount = seq_maxcount + (newcount - seq_maxcount) / 2 - 1;
                    if (newcount <= seq_maxcount)
                    {
                        ReleaseSeqHashLock(i);
                        /* give up */
                        ereport(ERROR,
                                (ERANGE,
                                 errmsg("Can not list all the sequences")));
                    }
                    newlist = (GTM_StoredSeqInfo *) repalloc(seq_list, newcount * sizeof(GTM_StoredSeqInfo));
                }
                seq_maxcount = newcount;
                seq_list = newlist;
            }
            memcpy(&seq_list[seq_count], seq_info, sizeof(GTM_StoredSeqInfo));            
            seq_count++;
            
            bucket_handle = seq_info->gs_next;
        }
        ReleaseSeqHashLock(i);
    }    

    pq_getmsgend(message);

    pq_beginmessage(&buf, 'S');
    pq_sendint(&buf, MSG_LIST_GTM_STORE_SEQ_RESULT, 4);

    if (myport->remote_type == GTM_NODE_GTM_PROXY)
    {
        GTM_ProxyMsgHeader proxyhdr;
        proxyhdr.ph_conid = myport->conn_id;
        pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
    }

    /* Send a number of sequences */
    pq_sendint(&buf, seq_count, sizeof(seq_count));

    /*
     * Send sequences from the array
     */            
    for (i = 0 ; i < seq_count ; i++)
    {        
        pq_sendbytes(&buf, (char*)&seq_list[i], sizeof(GTM_StoredSeqInfo));
    }
    pq_endmessage(myport, &buf);

    pfree(seq_list);

    if (myport->remote_type != GTM_NODE_GTM_PROXY)
    {
        /* Don't flush to the backup because this does not change the internal status */
        pq_flush(myport);
    }

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "ProcessListStorageSequenceCommand done");
    }
}

void
ProcessListStorageTransactionCommand(Port *myport, StringInfo message)
{// #lizard forgives
    StringInfoData         buf;
    bool                ret             = false;
    int                    txn_count     = 0;
    int                    txn_maxcount  = 0;
    GTM_StoredTransactionInfo  *txn_list      = NULL;
    int                    i             = 0;
    GTMStorageHandle    bucket_handle = INVALID_STORAGE_HANDLE;
    GTM_StoredTransactionInfo  *txn_info      = NULL;

    if (Recovery_IsStandby())
    {
        ereport(ERROR,
            (EPERM,
             errmsg("ProcessListStorageSequenceCommand Operation not permitted under the standby mode.")));
    }

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "ProcessListStorageTransactionCommand enter");
    }
    
    txn_count    = 0;
    txn_maxcount = 1024;
    txn_list = (GTM_StoredTransactionInfo *) palloc(txn_maxcount * sizeof(GTM_StoredTransactionInfo));
    for (i = 0 ; i < GTM_STORED_HASH_TABLE_NBUCKET ; i++)
    {
        ret = AcquireTxnHashLock(i, GTM_LOCKMODE_READ);
        if (!ret)
        {
            elog(LOG, "AcquireTxnHashLock %d failed", i);
        }        
        
        bucket_handle = GetTxnHashBucket(i);
        while (INVALID_STORAGE_HANDLE != bucket_handle)
        {    
            txn_info = GetTxnStore(bucket_handle);
            if (txn_count >= txn_maxcount)
            {
                int             newcount = NULL;
                GTM_StoredTransactionInfo   *newlist     = NULL;
                
                newcount = 2 * txn_maxcount;
                newlist  = (GTM_StoredTransactionInfo *) repalloc(txn_list, newcount * sizeof(GTM_StoredTransactionInfo));
                /*
                 * If failed try to get less. It is unlikely to happen, but
                 * let's be safe.
                 */
                while (newlist == NULL)
                {
                    newcount = txn_maxcount + (newcount - txn_maxcount) / 2 - 1;
                    if (newcount <= txn_maxcount)
                    {
                        /* give up */
                        ReleaseTxnHashLock(i);
                        ereport(ERROR,
                                (ERANGE,
                                 errmsg("Can not list all the transactions")));
                    }
                    newlist = (GTM_StoredTransactionInfo *) repalloc(txn_list, newcount * sizeof(GTM_StoredTransactionInfo));
                }
                txn_maxcount = newcount;
                txn_list = newlist;
            }
            memcpy(&txn_list[txn_count], txn_info, sizeof(GTM_StoredTransactionInfo));            
            txn_count++;
            
            bucket_handle = txn_info->gs_next;
        }
        ReleaseTxnHashLock(i);
    }    

    pq_getmsgend(message);

    pq_beginmessage(&buf, 'S');
    pq_sendint(&buf, MSG_LIST_GTM_TXN_STORE_RESULT, 4);

    if (myport->remote_type == GTM_NODE_GTM_PROXY)
    {
        GTM_ProxyMsgHeader proxyhdr;
        proxyhdr.ph_conid = myport->conn_id;
        pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
    }

    /* Send a number of sequences */
    pq_sendint(&buf, txn_count, sizeof(txn_count));

    /*
     * Send sequences from the array
     */            
    for (i = 0 ; i < txn_count ; i++)
    {        
        pq_sendbytes(&buf, (char*)&txn_list[i], sizeof(GTM_StoredTransactionInfo));
    }
    pq_endmessage(myport, &buf);

    pfree(txn_list);
    
    if (myport->remote_type != GTM_NODE_GTM_PROXY)
    {
        /* Don't flush to the backup because this does not change the internal status */
        pq_flush(myport);
    }

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "ProcessListStorageTransactionCommand done");
    }
}

static void RebuildSequenceList()
{
    int i,ret ;
    GTMStorageHandle    bucket_handle = INVALID_STORAGE_HANDLE;
    GTM_StoredSeqInfo  *seq_info      = NULL;

    if (enable_gtm_sequence_debug)
        elog(LOG, "RebuildSequenceList start");

    GTM_RWLockAcquire(g_GTM_Store_Head_Lock,GTM_LOCKMODE_WRITE);
    for (i = 0 ; i < GTM_STORED_HASH_TABLE_NBUCKET ; i++)
    {
        ret = AcquireSeqHashLock(i, GTM_LOCKMODE_WRITE);
        if (!ret)
            elog(PANIC, "AcquireSeqHashLock %d failed", i);
        SetSeqHashBucket(i,INVALID_STORAGE_HANDLE);
    }
    g_GTM_Store_Header->m_seq_freelist = INVALID_STORAGE_HANDLE;

    for(i = 0 ; i < GTM_MAX_SEQ_NUMBER ; i++)
    {
        seq_info = GetSeqStore(i);

        if(seq_info->gs_status == GTM_STORE_SEQ_STATUS_NOT_USE)
        {
            seq_info->gs_next = g_GTM_Store_Header->m_seq_freelist;
            g_GTM_Store_Header->m_seq_freelist = i;
        }
        else
        {
            bucket_handle     = GTM_StoreGetHashBucket(seq_info->gs_key.gsk_key,strnlen(seq_info->gs_key.gsk_key,GTM_MAX_SEQ_NUMBER));
            seq_info->gs_next = GetSeqHashBucket(bucket_handle);
            SetSeqHashBucket(bucket_handle,i);
        }

        GTM_StoreSyncSeq(i);
    }

    GTM_StoreSyncHeader(false);
    GTM_RWLockRelease(g_GTM_Store_Head_Lock);
    for (i = 0 ; i < GTM_STORED_HASH_TABLE_NBUCKET ; i++)
    {
        GTM_StoreSyncSeqHashBucket(i);
        ReleaseSeqHashLock(i);
    }

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "RebuildSequenceList done");
    }
}
void
ProcessCheckStorageSequenceCommand(Port *myport, StringInfo message)
{// #lizard forgives
    bool                need_fix      = true;
    StringInfoData         buf;
    int                    seq_count     = 0;
    GTMStorageSequneceStatus  *seq_list      = NULL;
    int                    i             = 0;
    GTMStorageHandle    bucket_handle = INVALID_STORAGE_HANDLE;
    GTM_StoredSeqInfo  *seq_info      = NULL;

    if (Recovery_IsStandby())
    {
        ereport(ERROR,
            (EPERM,
             errmsg("ProcessCheckStorageSequenceCommand Operation not permitted under the standby mode.")));
    }

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "ProcessCheckStorageSequenceCommand enter");
    }

    need_fix = (bool)pq_getmsgint(message, sizeof (int32));
    
    seq_count    = 0;
    seq_list = (GTMStorageSequneceStatus *) palloc(GTM_MAX_CHECK_SEQ_NUM * sizeof(GTMStorageSequneceStatus));    
    for (i = 0; i < GTM_MAX_SEQ_NUMBER && seq_count < GTM_MAX_CHECK_SEQ_NUM; i++)
    {
        bool  crc_result  = false;
        bool  in_freelist = false;
        int32 error = 0;            
        
        seq_info = GetSeqStore(i);
        crc_result = GTM_StoreCheckSeqCRC(seq_info);
        if (!crc_result)
        {
            error |= GTMStorageStatus_CRC_error;
            if (GTM_STORE_SEQ_STATUS_NOT_USE == seq_info->gs_status)
            {
                /* should be in the free list*/
                in_freelist = GTM_StoreSeqInFreelist(seq_info);
                if(!in_freelist)
                {
                    error |= GTMStorageStatus_freelist_error;
                }                
            }
            else 
            {
                /* should be in the hash table*/
                bucket_handle = GTM_StoreSeqHashSearch(seq_info->gs_key.gsk_key, seq_info->gs_key.gsk_type);
                if (INVALID_STORAGE_HANDLE == bucket_handle)
                {
                    error |= GTMStorageStatus_hashtab_error;
                }
            }
        }
        
        if (error)
        {
            memcpy(&seq_list[seq_count].sequence, seq_info, sizeof(GTM_StoredSeqInfo));            
            if (error & GTMStorageStatus_CRC_error)
            {
                if (need_fix)
                {
                    GTM_StoreSyncSeq(seq_info->gti_store_handle);
                    seq_list[seq_count].status |= GTMStorageStatus_CRC_fixed;
                }
                else
                {
                    seq_list[seq_count].status |= GTMStorageStatus_CRC_unchanged;
                }
            }

            if (error & GTMStorageStatus_freelist_error)
            {
                seq_list[seq_count].status |= GTMStorageStatus_freelist_unchanged;
            }

            if (error & GTMStorageStatus_hashtab_error)
            {
                seq_list[seq_count].status |= GTMStorageStatus_hashtab_unchanged;
            }
            seq_count++;
        }
    }

    if(need_fix)
        RebuildSequenceList();

    pq_getmsgend(message);

    pq_beginmessage(&buf, 'S');
    pq_sendint(&buf, MSG_CHECK_GTM_SEQ_STORE_RESULT, 4);

    if (myport->remote_type == GTM_NODE_GTM_PROXY)
    {
        GTM_ProxyMsgHeader proxyhdr;
        proxyhdr.ph_conid = myport->conn_id;
        pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
    }

    /* Send a number of sequences */
    pq_sendint(&buf, seq_count, sizeof(seq_count));

    /*
     * Send sequences from the array
     */            
    for (i = 0 ; i < seq_count ; i++)
    {        
        pq_sendbytes(&buf, (char*)&seq_list[i], sizeof(GTMStorageSequneceStatus));
    }
    pq_endmessage(myport, &buf);

    pfree(seq_list);
    if (myport->remote_type != GTM_NODE_GTM_PROXY)
    {
        /* Don't flush to the backup because this does not change the internal status */
        pq_flush(myport);
    }

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "ProcessCheckStorageSequenceCommand done");
    }
}

static void RebuildTransactionList()
{
    int i,ret ;
    GTMStorageHandle    bucket_handle         = INVALID_STORAGE_HANDLE;
    GTM_StoredTransactionInfo  *txn_info      = NULL;

    if (enable_gtm_sequence_debug)
        elog(LOG, "RebuildTransactionList start");

    GTM_RWLockAcquire(g_GTM_Store_Head_Lock,GTM_LOCKMODE_WRITE);
    for (i = 0 ; i < GTM_STORED_HASH_TABLE_NBUCKET ; i++)
    {
        ret = AcquireTxnHashLock(i, GTM_LOCKMODE_WRITE);
        if (!ret)
            elog(PANIC, "AcquireTxnHashLock %d failed", i);
        SetTxnHashBucket(i,INVALID_STORAGE_HANDLE);
    }
    g_GTM_Store_Header->m_txn_freelist = INVALID_STORAGE_HANDLE;

    for(i = 0 ; i < MAX_PREPARED_TXN ; i++)
    {
        txn_info = GetTxnStore(i);

        if(txn_info->gti_state == GTM_TXN_INIT)
        {
            txn_info->gs_next = g_GTM_Store_Header->m_txn_freelist;
            g_GTM_Store_Header->m_txn_freelist = i;
        }
        else
        {
            bucket_handle     = GTM_StoreGetHashBucket(txn_info->gti_gid,strlen(txn_info->gti_gid));
            txn_info->gs_next = GetTxnHashBucket(bucket_handle);
            SetTxnHashBucket(bucket_handle,i);
        }

        GTM_StoreSyncTxn(i);
    }

    GTM_StoreSyncHeader(false);
    GTM_RWLockRelease(g_GTM_Store_Head_Lock);
    for (i = 0 ; i < GTM_STORED_HASH_TABLE_NBUCKET ; i++)
    {
        GTM_StoreSyncTxnHashBucket(i);
        ReleaseTxnHashLock(i);
    }

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "RebuildTransactionList done");
    }
}

void
ProcessCheckStorageTransactionCommand(Port *myport, StringInfo message)
{// #lizard forgives
    bool                need_fix      = false;    
    StringInfoData        buf;
    int                 txn_count      = 0;
    GTMStorageTransactionStatus  *txn_list      = NULL;
    int                 i              = 0;
    GTMStorageHandle    bucket_handle = INVALID_STORAGE_HANDLE;
    GTM_StoredTransactionInfo  *txn_info      = NULL;

    if (Recovery_IsStandby())
    {
        ereport(ERROR,
            (EPERM,
             errmsg("ProcessCheckStorageTransactionCommand Operation not permitted under the standby mode.")));
    }

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "ProcessCheckStorageTransactionCommand enter");
    }
    
    need_fix = (bool)pq_getmsgint(message, sizeof (int32));    
    txn_count     = 0;
    txn_list = (GTMStorageTransactionStatus *) palloc(GTM_MAX_CHECK_TXN_NUM * sizeof(GTMStorageTransactionStatus));    
    for (i = 0; i < MAX_PREPARED_TXN && txn_count < GTM_MAX_CHECK_TXN_NUM; i++)
    {
        bool  crc_result  = false;
        bool  in_freelist = false;
        int32 error = 0;            
        
        txn_info = GetTxnStore(i);
        crc_result = GTM_StoreCheckTxnCRC(txn_info);
        if (!crc_result)
        {
            error |= GTMStorageStatus_CRC_error;
            if (GTM_STORE_SEQ_STATUS_NOT_USE == txn_info->gti_state)
            {
                /* should be in the free list*/
                in_freelist = GTM_StoreTxnInFreelist(txn_info);
                if(!in_freelist)
                {
                    error |= GTMStorageStatus_freelist_error;
                }                
            }
            else 
            {
                /* should be in the hash table*/
                bucket_handle = GTM_StoreTxnHashSearch(txn_info->gti_gid);
                if (INVALID_STORAGE_HANDLE == bucket_handle)
                {
                    error |= GTMStorageStatus_hashtab_error;
                }
            }
        }

        if (error)
        {
            memcpy(&txn_list[txn_count].txn, txn_info, sizeof(GTM_StoredTransactionInfo));         
			if (error & GTMStorageStatus_CRC_error)
            {
                if (need_fix)
                {
                    GTM_StoreSyncTxn(txn_info->gti_store_handle);
                    txn_list[txn_count].status |= GTMStorageStatus_CRC_fixed;
                }
                else
                {
                    txn_list[txn_count].status |= GTMStorageStatus_CRC_unchanged;
                }
            }

			if (error & GTMStorageStatus_freelist_error)
            {
                txn_list[txn_count].status |= GTMStorageStatus_freelist_unchanged;
            }

			if (error & GTMStorageStatus_hashtab_error)
            {
                txn_list[txn_count].status |= GTMStorageStatus_hashtab_unchanged;
            }
            txn_count++;
        }
    }

    if(need_fix)
        RebuildTransactionList();

    pq_getmsgend(message);

    BeforeReplyToClientXLogTrigger();

    pq_beginmessage(&buf, 'S');
    pq_sendint(&buf, MSG_CHECK_GTM_SEQ_STORE_RESULT, 4);

    if (myport->remote_type == GTM_NODE_GTM_PROXY)
    {
        GTM_ProxyMsgHeader proxyhdr;
        proxyhdr.ph_conid = myport->conn_id;
        pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
    }

    /* Send a number of sequences */
    pq_sendint(&buf, txn_count, sizeof(txn_count));

    /*
     * Send sequences from the array
     */         
    for (i = 0 ; i < txn_count ; i++)
    {        
        pq_sendbytes(&buf, (char*)&txn_list[i], sizeof(GTMStorageTransactionStatus));
    }
    pq_endmessage(myport, &buf);

    pfree(txn_list);
    if (myport->remote_type != GTM_NODE_GTM_PROXY)
    {
        /* Don't flush to the backup because this does not change the internal status */
        pq_flush(myport);
    }

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "ProcessCheckStorageTransactionCommand done");
    }
}

/* Lock the gtm store, block any write request. */
bool GTM_StoreLock(void)
{
    bool bret = false;
    int  ii    = 0;

    bret = GTM_RWLockAcquire(&g_GTM_store_lock, GTM_LOCKMODE_WRITE);
    if (!bret)
    {
        elog(LOG, "GTM_StoreLock  GTM_RWLockAcquire g_GTM_store_lock failed:%s", strerror(errno));
        return false;
    }

    if(g_GTM_store_lock_cnt == 0)
    {
        bret = GTM_RWLockAcquire(&GTMThreads->gt_lock,GTM_LOCKMODE_READ);
        if (!bret)
        {
            elog(LOG, "GTM_StoreLock  GTM_RWLockAcquire GTMThreads->gt_lock failed:%s", strerror(errno));
            return false;
        }

        for (ii = 0; ii < GTMThreads->gt_array_size; ii++)
        {
            if (GTMThreads->gt_threads[ii])
            {
                GTM_RWLockAcquire(&GTMThreads->gt_threads[ii]->thr_lock, GTM_LOCKMODE_WRITE);
            }
        }

        GTMThreads->gt_block_new_connection = true;

        GTM_RWLockRelease(&GTMThreads->gt_lock);
    }

    g_GTM_store_lock_cnt++;
    if(enable_gtm_debug)
        elog(LOG,"Store lock success %u",g_GTM_store_lock_cnt);

    GTM_RWLockRelease(&g_GTM_store_lock);
    return true;
}

/* Lock the gtm store, block any write request. */
bool GTM_StoreUnLock(void)
{
    bool bret = false;
    bool bfailed = false;
    int  ii    = 0;

    bret = GTM_RWLockAcquire(&g_GTM_store_lock, GTM_LOCKMODE_WRITE);
    if (!bret)
    {
        elog(LOG, "GTM_StoreLock  GTM_RWLockAcquire g_GTM_store_lock failed:%s", strerror(errno));
        return false;
    }

    g_GTM_store_lock_cnt--;

    if(g_GTM_store_lock_cnt == 0)
    {
        bret = GTM_RWLockAcquire(&GTMThreads->gt_lock,GTM_LOCKMODE_READ);
        if (!bret)
        {
            elog(LOG, "GTM_StoreLock GTM_RWLockAcquire GTMThreads->gt_lock failed:%s", strerror(errno));
            return false;
        }

        for (ii = 0; ii < GTMThreads->gt_array_size; ii++)
        {
            if (GTMThreads->gt_threads[ii])
            {
                GTM_RWLockRelease(&GTMThreads->gt_threads[ii]->thr_lock);
            }
        }

        GTMThreads->gt_block_new_connection = false;
        GTM_RWLockRelease(&GTMThreads->gt_lock);
    }

    if(enable_gtm_debug)
        elog(LOG,"Store unlock success %u",g_GTM_store_lock_cnt);

    GTM_RWLockRelease(&g_GTM_store_lock);
    return !bfailed;
}

/* Timer handler for lock store stuck. To avoid stuck of master node.*/
void *LockStoreStandbyCrashHandler(void *param)
{// #lizard forgives    
#ifndef __XLOG__
    bool    found = false;
    bool    thread_exist = false;
    int        ii    = 0;
    int     ret   = 0;
    GTM_ThreadInfo *threadinfo = NULL;
    threadinfo = (GTM_ThreadInfo*)param;
    if (!threadinfo)
    {
        return NULL;
    }
    
    /* Acquire the thread locks to check whether the thread is still running. */
    found = false;
    thread_exist = false;
    GTM_RWLockAcquire(&GTMThreads->gt_lock, GTM_LOCKMODE_WRITE);
    for (ii = 0; ii < GTMThreads->gt_array_size; ii++)
    {
        if (GTMThreads->gt_threads[ii] == threadinfo)
        {
            found = true;
            break;
        }
    }

    /* still in the thread list , check whether it is still running. */
    if (found)
    {
        ret = pthread_kill(threadinfo->thr_id, 0);
        if (ESRCH == ret || EINVAL == ret)
        {
            thread_exist = false;    
        }
        else
        {
            /* kill the still running thread, when it exit, it will clean up the locks. */
            thread_exist = true;
            ret = pthread_kill(threadinfo->thr_id, SIGQUIT);
            if (ESRCH == ret || EINVAL == ret)
            {    
                elog(LOG, "LockStoreStandbyCrashHandler thread exits, kill backup thread failed, unlock the store.");
            }
            else
            {        
                elog(LOG, "LockStoreStandbyCrashHandler kill backup thread succeed");
            }
        }
    }
    GTM_RWLockRelease(&GTMThreads->gt_lock);

    /* Should never happen: thread not running, we just unlock the store. */
    if (!thread_exist)
    {
        elog(LOG, "LockStoreStandbyCrashHandler thread not exist, GTM_StoreUnLock done");
    }        
#else
    bool do_unlock = false;

    GTM_RWLockAcquire(&g_GTM_Backup_Timer_Lock,GTM_LOCKMODE_WRITE);
    if(g_GTM_Backup_Timer != INVALID_TIMER_HANDLE)
        do_unlock = true;
    g_GTM_Backup_Timer = INVALID_TIMER_HANDLE;
    GTM_RWLockRelease(&g_GTM_Backup_Timer_Lock);

    if(do_unlock)
    {
        if(GTM_StoreUnLock() == false)
        {
            elog(LOG, "LockStoreStandbyCrashHandler unlock store fails");
            exit(1);
        }
        elog(LOG, "LockStoreStandbyCrashHandler do unlock");
    }

    elog(LOG, "LockStoreStandbyCrashHandler GTM_StoreUnLock done");
#endif
    return NULL;
}
/* Get sysinfo including: identifier ,lsn, gts. */
bool GTM_StoreGetSysInfo(int64 *identifier, int64 *lsn, GlobalTimestamp *gts)
{    
    bool ret                     = false;
    int  fd                      = -1;
    int  nbytes                  = 0;
    GTMControlHeader     *header = NULL;
    /* for gtm master */
    if (g_GTM_Store_Header)
    {        
        ret = GTM_RWLockAcquire(g_GTM_Store_Head_Lock, GTM_LOCKMODE_WRITE);
        if (!ret)
        {
            elog(LOG, "GTM_StoreGetSysInfo lock store header failed for:%s.", strerror(errno));
            return false;
        }
        *identifier = g_GTM_Store_Header->m_identifier;
        *lsn        = g_GTM_Store_Header->m_lsn;
        *gts        = g_GTM_Store_Header->m_next_gts;
        GTM_RWLockRelease(g_GTM_Store_Head_Lock);
        return true;
    }
    else
    {
        /* for gtm standby */
        fd = open(GTM_MAP_FILE_NAME, O_RDWR, S_IRUSR | S_IWUSR);
        if (fd < 0)
        {            
            elog(LOG, "GTM_StoreGetSysInfo open file:%s failed for:%s.", GTM_MAP_FILE_NAME, strerror(errno));
            return false;
        }
        else 
        {

            header = (GTMControlHeader*)palloc(sizeof(GTMControlHeader));
            if (header == NULL) 
            {        
                close(fd);
                elog(LOG, "GTM_StoreGetSysInfo palloc failed.");
                return false;
            }
            
            nbytes = read(fd, header, sizeof(GTMControlHeader));        
            if (nbytes < 0)
            {
                pfree(header);
                close(fd);
                elog(LOG, "GTM_StoreGetSysInfo read gtm store header failed.");
                return false;
            }        
            
            *identifier = header->m_identifier;
            *lsn         = header->m_lsn;
            *gts        = header->m_next_gts;
            pfree(header);
            close(fd);
            return true;
        }
    }
}

void
GTM_PrintControlHeader(void)
{
    elog(LOG,"gtm_header");
    elog(LOG,"m_identifier = %lu",g_GTM_Store_Header->m_identifier);
    elog(LOG,"m_major_version = %d",g_GTM_Store_Header->m_major_version);
    elog(LOG,"m_minor_version = %d",g_GTM_Store_Header->m_minor_version);
    elog(LOG,"m_gtm_status = %d",g_GTM_Store_Header->m_gtm_status);
    elog(LOG,"m_global_xmin = %d",g_GTM_Store_Header->m_global_xmin);
    elog(LOG,"m_next_gxid = %d",g_GTM_Store_Header->m_next_gxid);
    elog(LOG,"m_next_gts = %lu",g_GTM_Store_Header->m_next_gts);
    elog(LOG,"m_seq_freelist = %d",g_GTM_Store_Header->m_seq_freelist);
    elog(LOG,"m_txn_freelist = %d",g_GTM_Store_Header->m_txn_freelist);
    elog(LOG,"m_lsn = %lu",g_GTM_Store_Header->m_lsn);
    elog(LOG,"m_last_update_time = %ld",g_GTM_Store_Header->m_last_update_time);
    elog(LOG,"m_crc = %d",g_GTM_Store_Header->m_crc);
}
GTMStorageHandle *GTM_StoreGetAllSeqInDatabase(GTM_SequenceKey seq_database_key, int32 *number)
{// #lizard forgives
    int                    seq_count     = 0;
    int                    seq_maxcount  = 0;
    GTMStorageHandle   *seq_list      = NULL;
    int                    i             = 0;
    GTMStorageHandle    bucket_handle = INVALID_STORAGE_HANDLE;
    GTM_StoredSeqInfo  *seq_info      = NULL;
    bool                ret           = false;
	Assert(seq_database_key->gsk_keylen <= SEQ_KEY_MAX_LENGTH);

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreGetAllSeqInDatabase enter");
    }


    seq_count    = 0;
    seq_maxcount = 2048;
    seq_list = (GTMStorageHandle*) palloc(seq_maxcount * sizeof(GTMStorageHandle));
    for (i = 0 ; i < GTM_STORED_HASH_TABLE_NBUCKET ; i++)
    {
        ret = AcquireSeqHashLock(i, GTM_LOCKMODE_READ);
        if (!ret)
        {
            elog(LOG, "AcquireTxnHashLock %d failed", i);
        }

        bucket_handle = GetSeqHashBucket(i);
        while (INVALID_STORAGE_HANDLE != bucket_handle)
        {
            seq_info = GetSeqStore(bucket_handle);

			if(!(strncmp(seq_database_key->gsk_key,seq_info->gs_key.gsk_key,seq_database_key->gsk_keylen - 1) == 0 &&
			        seq_info->gs_key.gsk_key[seq_database_key->gsk_keylen - 1] == '.'))
            {
                bucket_handle = seq_info->gs_next;
                continue;
            }

            if (seq_count >= seq_maxcount)
            {
                int                  newcount = NULL;
                GTMStorageHandle    *newlist  = NULL;

                newcount = 2 * seq_maxcount;
                newlist  = (GTMStorageHandle *) repalloc(seq_list, newcount * sizeof(GTMStorageHandle));
                /*
                 * If failed try to get less. It is unlikely to happen, but
                 * let's be safe.
                 */
                while (newlist == NULL)
                {
                    newcount = seq_maxcount + (newcount - seq_maxcount) / 2 - 1;
                    if (newcount <= seq_maxcount)
                    {
                        ReleaseSeqHashLock(i);
                        /* give up */
                        ereport(ERROR,
                                (ERANGE,
                                 errmsg("Can not list all the sequences")));
                    }
                    newlist = (GTMStorageHandle *) repalloc(seq_list, newcount * sizeof(GTMStorageHandle));
                }
                seq_maxcount = newcount;
                seq_list = newlist;
            }
            seq_list[seq_count] = seq_info->gti_store_handle;
            seq_count++;

            bucket_handle = seq_info->gs_next;
        }
        ReleaseSeqHashLock(i);
    }    

    if (enable_gtm_sequence_debug)
    {
        elog(LOG, "GTM_StoreDropAllSeqInDatabase finish");
    }
    *number = seq_count;
    if (0 == seq_count)
    {
        pfree(seq_list);
        seq_list = NULL;
    }
    return  seq_list;
}
void GTM_StoreGetSeqKey(GTMStorageHandle handle, char *key)
{
    GTM_StoredSeqInfo  *seq_info      = NULL;
    
    seq_info = GetSeqStore(handle);
    snprintf(key, SEQ_KEY_MAX_LENGTH, "%s", seq_info->gs_key.gsk_key);
}

/*
 * get seq create info
 */
void GTM_StoreGetSeqCreateInfo(GTMStorageHandle handle, GTM_SeqCreateInfo *create_info)
{
    GTM_StoredSeqInfo  *seq_info = NULL;

    seq_info = GetSeqStore(handle);
    snprintf(create_info->seqkey, SEQ_KEY_MAX_LENGTH, "%s", seq_info->gs_key.gsk_key);
    create_info->increment_by = seq_info->gs_increment_by;
    create_info->minval = seq_info->gs_min_value;
    create_info->maxval = seq_info->gs_max_value;
    /* get gs_value as new sequence's startval */
    create_info->startval = seq_info->gs_value;
    create_info->cycle = seq_info->gs_cycle;
}
