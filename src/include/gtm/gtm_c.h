/*-------------------------------------------------------------------------
 *
 * gtm_c.h
 *      Fundamental C definitions.  This is included by every .c file in
 *      PostgreSQL (via either postgres.h or postgres_fe.h, as appropriate).
 *
 *      Note that the definitions here are not intended to be exposed to clients
 *      of the frontend interface libraries --- so we don't worry much about
 *      polluting the namespace with lots of stuff...
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * $PostgreSQL: pgsql/src/include/c.h,v 1.234 2009/01/01 17:23:55 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef GTM_C_H
#define GTM_C_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <stdarg.h>
#ifdef HAVE_STRINGS_H
#include <strings.h>
#endif
#include <sys/types.h>

#include <errno.h>
#include <pthread.h>
#include "c.h"
#ifdef __TBASE__
#include "port/pg_crc32c.h"
#endif

typedef uint32    GlobalTransactionId;        /* 32-bit global transaction ids */
typedef int16    GTMProxy_ConnID;
typedef uint32    GTM_StrLen;

#define InvalidGTMProxyConnID    -1

typedef pthread_t    GTM_ThreadID;

typedef uint32        GTM_PGXCNodeId;
typedef uint32        GTM_PGXCNodePort;

/* Possible type of nodes for registration */
typedef enum GTM_PGXCNodeType
{
    GTM_NODE_GTM_PROXY = 1,
    GTM_NODE_GTM_PROXY_POSTMASTER = 2,
                /* Used by Proxy to communicate with GTM and not use Proxy headers */
    GTM_NODE_COORDINATOR = 3,
    GTM_NODE_DATANODE = 4,
    GTM_NODE_GTM = 5,
#ifdef __TBASE__
	GTM_NODE_GTM_CTL   = 6,	/* gtm ctl will never register and unregister, maybe used by gtm_ctl or pgxc_ctl */
#endif
    GTM_NODE_DEFAULT/* In case nothing is associated to connection */
} GTM_PGXCNodeType;

/*
 * A unique handle to identify transaction at the GTM. It could just be
 * an index in an array or a pointer to the structure
 *
 * Note: If we get rid of BEGIN transaction at the GTM, we can use GXID
 * as a handle because we would never have a transaction state at the
 * GTM without assigned GXID.
 */
typedef int32    GTM_TransactionHandle;

#define InvalidTransactionHandle    -1

/*
 * As GTM and Postgres-XC packages are separated, GTM and XC's API
 * use different type names for timestamps and sequences, but they have to be the same!
 */
typedef int64    GTM_Timestamp;    /* timestamp data is 64-bit based */

typedef int64    GTM_Sequence;    /* a 64-bit sequence */

/* Type of sequence name used when dropping it */
typedef enum GTM_SequenceKeyType
{
    GTM_SEQ_FULL_NAME,    /* Full sequence key */
    GTM_SEQ_DB_NAME        /* DB name part of sequence key */
} GTM_SequenceKeyType;

typedef struct GTM_SequenceKeyData
{
    uint32        gsk_keylen;
    char        *gsk_key;
    GTM_SequenceKeyType    gsk_type; /* see constants below */
} GTM_SequenceKeyData;    /* Counter key, set by the client */

typedef GTM_SequenceKeyData *GTM_SequenceKey;

#define GTM_MAX_SEQKEY_LENGTH        1024

#define InvalidSequenceValue        0x7fffffffffffffffLL
#define SEQVAL_IS_VALID(v)        ((v) != InvalidSequenceValue)

#define GTM_MAX_GLOBAL_TRANSACTIONS    16384

typedef enum GTM_IsolationLevel
{
    GTM_ISOLATION_SERIALIZABLE, /* serializable txn */
    GTM_ISOLATION_RC        /* read-committed txn */
} GTM_IsolationLevel;

typedef struct GTM_SnapshotData
{
    GlobalTransactionId        sn_xmin;
    GlobalTransactionId        sn_xmax;
    uint32                sn_xcnt;
    GlobalTransactionId        *sn_xip;
} GTM_SnapshotData;

typedef GTM_SnapshotData *GTM_Snapshot;

/* Define max size of node name in start up packet */
#define SP_NODE_NAME        64

typedef struct GTM_StartupPacket {
    char                    sp_node_name[SP_NODE_NAME];
    GTM_PGXCNodeType        sp_remotetype;
    bool                    sp_ispostmaster;
    uint32                    sp_client_id;
    uint32                    sp_backend_pid;
} GTM_StartupPacket;

typedef enum GTM_PortLastCall
{
    GTM_LastCall_NONE = 0,
    GTM_LastCall_SEND,
    GTM_LastCall_RECV,
    GTM_LastCall_READ,
    GTM_LastCall_WRITE
} GTM_PortLastCall;

#define InvalidGlobalTransactionId        ((GlobalTransactionId) 0)

/*
 * Initial GXID value to start with, when -x option is not specified at the first run.
 *
 * This value is supposed to be safe enough.   If initdb involves huge amount of initial
 * statements/transactions, users should consider to tweak this value with explicit
 * -x option.
 */
#define InitialGXIDValue_Default        ((GlobalTransactionId) 10000)

#define GlobalTransactionIdIsValid(gxid) (((GlobalTransactionId) (gxid)) != InvalidGlobalTransactionId)

#define _(x) gettext(x)

#define GTM_ERRCODE_TOO_OLD_XMIN 1
#define GTM_ERRCODE_NODE_NOT_REGISTERED 2
#define GTM_ERRCODE_NODE_EXCLUDED 3
#define GTM_ERRCODE_UNKNOWN 4

#ifdef __TBASE__
#define InvalidGTS ((GTM_Timestamp) 0)
typedef  int32 GTMStorageHandle;  
#define GTM_MAX_SESSION_ID_LEN            (256) /* max align(64) (64 + 13 + 13) */
#define GTM_MAX_OP_SEQ_NUMBER            (10)

#define  INVALID_STORAGE_HANDLE      (0XFFFFFFFF)
#define  SEQ_KEY_MAX_LENGTH           256

#define  GTM_STORE_MAJOR_VERSION       2
#define  GTM_STORE_MINOR_VERSION       0

#define  GTM_MAP_FILE_NAME                "gtm_kernel.map"
#define  GTM_MAP_BACKUP_NAME           "gtm_kernel.backup"
#define  GTM_DEBUG_FILE_NAME           "transaction_debug_log"
#define  GTM_SCAN_DEBUG_FILE_NAME           "scan_debug_log"
#define  GTM_STORED_HASH_TABLE_NBUCKET 1024
#define  NODE_STRING_MAX_LENGTH        4096
#define  GTM_MAX_SEQ_NUMBER               200000 /* MAX sequence number of GTM*/
#define  MAX_PREPARED_TXN               5000   /* MAX prepared TXN number of GTM*/
#define  MAX_SEQUENCE_RESERVED           10000
#define  GTM_MAX_DEBUG_TXN_INFO           10000
#define  GTM_MAX_WALSENDER            100
#define  PAGE_SIZE                        4096
#define  ALIGN_UP(a, b)                (((a) + (b) - 1)/(b)) * (b)
#define  ALIGN8(a)                     ALIGN_UP(a, 8)
#define  ALIGN_PAGE(a)                 ALIGN_UP(a, PAGE_SIZE)


typedef enum
{
    GTM_STORE_OK     = 0,
    GTM_STORE_ERROR = -1,
    GTM_STORE_SKIP   = -2,
    GTM_STORE_BUTTY
}GTM_STORE_RET_VALUE;

typedef enum
{
    GTM_TXN_SEQ_CODE_CREATE = 0,
    GTM_TXN_SEQ_CODE_DROP   = 1,
    GTM_TXN_SEQ_CODE_ALTER  = 2,
    GTM_TXN_SEQ_CODE_BUTTY
}GTM_TXN_SEQ_CODE_ENUM;

#pragma pack(8)
typedef struct
{
    int64                 m_identifier;                /* system identifier, to identify a cluster. */
    int32                  m_major_version;            /* gtm major version */
    int32                  m_minor_version;            /* gtm minor version */
    int32                m_gtm_status;               /* gtm node status */
    GlobalTransactionId m_global_xmin;                /* latest completed xid of cluster */
    GlobalTransactionId    m_next_gxid;                   /* next gxid of cluster */ 
    GlobalTimestamp        m_next_gts;                    /* next global timestamp */
    
    GTMStorageHandle    m_seq_freelist;
    GTMStorageHandle    m_txn_freelist;
    int64                m_lsn;                        /* operation seq number, for operation need to flush to disk. */
    GTM_Timestamp       m_last_update_time;            /* last status change time */
    pg_crc32c            m_crc;                        /* crc check value */
}GTMControlHeader;

/* Use a circular memory for storing debug info */
typedef struct
{
    int     m_txn_buffer_len;
    int     m_txn_buffer_last;
    
}GTMDebugControlHeader;


typedef enum
{
    GTM_STORE_SEQ_STATUS_NOT_USE  = 0,
    GTM_STORE_SEQ_STATUS_ALLOCATE = 1,    
    GTM_STORE_SEQ_STATUS_COMMITED = 2,
    GTM_STORE_SEQ_STATUS_ABORTED  = 3,
    GTM_STORE_SEQ_STATUS_ERROE
}GTM_STORE_SEQ_STATUS;

typedef struct GTMStoredSequenceKeyData
{
    char                gsk_key[SEQ_KEY_MAX_LENGTH];
    GTM_SequenceKeyType    gsk_type; /* see constants below */
} GTMStoredSequenceKeyData;    

typedef struct GTM_StoredSeqInfo
{
    GTMStoredSequenceKeyData        gs_key;    
    GTM_Sequence                    gs_value;
    GTM_Sequence                    gs_init_value;
    GTM_Sequence                    gs_increment_by;
    GTM_Sequence                    gs_min_value;
    GTM_Sequence                    gs_max_value;
    bool                            gs_cycle;
    bool                            gs_called;
    bool                            gs_reserved;    
    int32                            gs_status;            /* seq status, see GTM_STORE_SEQ_STATUS*/
    GTMStorageHandle                 gti_store_handle;   /* self handle */    

    GTM_Timestamp                   m_last_update_time; /* last modified timestamp */    
    
    GTMStorageHandle                gs_next;            /* pointer to next if we are in a list */
    pg_crc32c                        gs_crc;                /* crc check value */
}GTM_StoredSeqInfo;

typedef struct GTM_SeqCreateInfo
{
    char seqkey[SEQ_KEY_MAX_LENGTH];
    GTM_Sequence increment_by;
    GTM_Sequence minval;
    GTM_Sequence maxval;
    GTM_Sequence startval;
    bool cycle;
} GTM_SeqCreateInfo;

typedef struct GTM_StoredTransactionInfo
{
    char                        gti_gid[GTM_MAX_SESSION_ID_LEN];    
    char                        nodestring[NODE_STRING_MAX_LENGTH]; /* List of nodes prepared */
    
    int32                        gti_state;
    
    /* no serialization below when standby initliaze */
    GTMStorageHandle             gti_store_handle;
    GTM_Timestamp               m_last_update_time; /* last modified timestamp */
    
    GTMStorageHandle            gs_next; /* pointer to next if we are in a list */
    pg_crc32c                    gti_crc;                /* crc check value */
} GTM_StoredTransactionInfo;


typedef struct GTM_NodeDebugInfo
{
    char                        node_name[GTM_MAX_SESSION_ID_LEN];    
    GlobalTransactionId            gxid;
    GTM_Timestamp                start_timestamp;
    GTM_Timestamp                prepare_timestamp;
    GTM_Timestamp                commit_timestamp;
    bool                        isCommit;
    int32                        number_scanned_tuples;
    struct GTM_NodeDebugInfo            *next;
}GTM_NodeDebugInfo;

typedef enum
{
    GTMTxnEmpty        = 0,
    GTMTxnInit,
    GTMTxnComplete
}GTMTxnDebugState;

typedef enum
{
    GTMTypeEmpty        = 0,
    GTMTypeScan,
    GTMTypeTransaction
}GTMTxnDebugEntryType;

#define ENTRY_STALE_THRESHOLD (10*60*1000000UL)

typedef struct GTM_TransactionDebugInfo
{
    char                        gid[GTM_MAX_SESSION_ID_LEN];    
    char                        nodestring[NODE_STRING_MAX_LENGTH]; /* List of nodes involved */
    GlobalTransactionId            gxid;
    int32                        node_count;
    int32                        complete_node_count;
    int32                        state;
    int32                        isCommit;
    int32                        entryType;
    
    GTM_Timestamp                  start_timestamp;
    GTM_Timestamp               prepare_timestamp;
    GTM_Timestamp               commit_timestamp;
    GTM_Timestamp                entry_create_timestamp;
    int32                        number_scanned_tuples;
    GTM_NodeDebugInfo            *node_list_head;
    GTM_NodeDebugInfo            *node_list_tail;
    
} GTM_TransactionDebugInfo;



typedef struct GTM_StoredHashTable
{
    GTMStorageHandle         m_buckets[GTM_STORED_HASH_TABLE_NBUCKET];
} GTM_StoredHashTable;

#define GTM_MAX_CHECK_SEQ_NUM 1024
#define GTM_MAX_CHECK_TXN_NUM 1024
typedef struct 
{
    GTMControlHeader header;
    int32            seq_total;
    int32            seq_used;
    int32            txn_total;
    int32            txn_used;
}GTMStorageStatus;

typedef struct 
{
    GTM_StoredSeqInfo sequence;
    int32             error;
    int32             status;
}GTMStorageSequneceStatus;

typedef struct 
{
    GTM_StoredTransactionInfo txn;
    int32             error;
    int32             status;
}GTMStorageTransactionStatus;

typedef enum
{
    GTMStorageStatus_CRC_error      = 0X01,
    GTMStorageStatus_freelist_error = 0X02,
    GTMStorageStatus_hashtab_error  = 0X04,
    GTMStorageStatus_error_butty
}GTMStorageError;

typedef enum
{
    GTMStorageStatus_CRC_fixed          = 0X01,
    GTMStorageStatus_CRC_unchanged      = 0X02,
    GTMStorageStatus_freelist_fixed     = 0X04,
    GTMStorageStatus_freelist_unchanged = 0X08,
    GTMStorageStatus_hashtab_fixed      = 0X10,
    GTMStorageStatus_hashtab_unchanged  = 0X20,
    GTMStorageStatus_status_butty
}GTMStorageCheckStatus;

typedef int64 pg_time_t;

/* 
 * Add delta 100s by assuming 10 timstamp/us 
 * at which rate the GTM can provide 1000w/s throughput.
 */
#define GTM_GTS_ONE_SECOND               (1000 * 1000L)
#define GTM_SYNC_CYCLE                     (5   * GTM_GTS_ONE_SECOND)
#define GTM_SYNC_TIME_LIMIT              (60  * GTM_GTS_ONE_SECOND)
#define GTM_LOG_COLLECT_CYCLE		     (5   * GTM_GTS_ONE_SECOND)
#define MAXGTMPATH	                     256
#pragma pack()

#endif
#endif   /* GTM_C_H */
