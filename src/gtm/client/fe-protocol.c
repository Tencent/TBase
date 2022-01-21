/*-------------------------------------------------------------------------
 *
 * fe-protocol3.c
 *      functions that are specific to frontend/backend protocol version 3
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *      $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#include "gtm/gtm_c.h"

#include <ctype.h>
#include <fcntl.h>

#include "gtm/libpq-fe.h"
#include "gtm/libpq-int.h"
#include "gtm/gtm_seq.h"
#include "gtm/gtm_client.h"
#include "gtm/gtm_serialize.h"
#include "gtm/register.h"
#include "gtm/gtm_xlog.h"

#include <unistd.h>
#include <netinet/in.h>
#include <gtm/gtm_client.h>


/*
 * This macro lists the backend message types that could be "long" (more
 * than a couple of kilobytes).
 */
#define VALID_LONG_MESSAGE_TYPE(id) \
    ((id) == 'S' || (id) == 'E')

static void handleSyncLoss(GTM_Conn *conn, char id, int msgLength);
static GTM_Result *pqParseInput(GTM_Conn *conn);
static int gtmpqParseSuccess(GTM_Conn *conn, GTM_Result *result);
static int gtmpqReadSeqKey(GTM_SequenceKey seqkey, GTM_Conn *conn);

/*
 * parseInput: if appropriate, parse input data from backend
 * until input is exhausted or a stopping state is reached.
 * Note that this function will NOT attempt to read more data from the backend.
 */
static GTM_Result *
pqParseInput(GTM_Conn *conn)
{// #lizard forgives
    char        id;
    int            msgLength;
    int            avail;
    GTM_Result    *result = NULL;

    if (conn->result == NULL)
    {
        conn->result = (GTM_Result *) malloc(sizeof (GTM_Result));
        memset(conn->result, 0, sizeof (GTM_Result));
    }
    else
        gtmpqFreeResultData(conn->result, conn->remote_type);

    result = conn->result;

    /*
     * Try to read a message.  First get the type code and length. Return
     * if not enough data.
     */
    conn->inCursor = conn->inStart;
    if (gtmpqGetc(&id, conn))
        return NULL;
    if (gtmpqGetInt(&msgLength, 4, conn))
        return NULL;

    /*
     * Try to validate message type/length here.  A length less than 4 is
     * definitely broken.  Large lengths should only be believed for a few
     * message types.
     */
    if (msgLength < 4)
    {
        handleSyncLoss(conn, id, msgLength);
        return NULL;
    }
    if (msgLength > 30000 && !VALID_LONG_MESSAGE_TYPE(id))
    {
        handleSyncLoss(conn, id, msgLength);
        return NULL;
    }

    /*
     * Can't process if message body isn't all here yet.
     */
    conn->result->gr_msglen = msgLength -= 4;
    avail = conn->inEnd - conn->inCursor;
    if (avail < msgLength)
    {
        /*
         * Before returning, enlarge the input buffer if needed to hold
         * the whole message.  This is better than leaving it to
         * gtmpqReadData because we can avoid multiple cycles of realloc()
         * when the message is large; also, we can implement a reasonable
         * recovery strategy if we are unable to make the buffer big
         * enough.
         */
        if (gtmpqCheckInBufferSpace(conn->inCursor + (size_t) msgLength,
                                 conn))
        {
            /*
             * XXX add some better recovery code... plan is to skip over
             * the message using its length, then report an error. For the
             * moment, just treat this like loss of sync (which indeed it
             * might be!)
             */
            handleSyncLoss(conn, id, msgLength);
        }
        return NULL;
    }

    switch (id)
    {
        case 'S':        /* command complete */
            if (gtmpqParseSuccess(conn, result))
                return NULL;
            break;

        case 'E':        /* error return */
            if (gtmpqGetError(conn, result))
                return NULL;
            result->gr_status = GTM_RESULT_ERROR;
            break;
        default:
            printfGTMPQExpBuffer(&conn->errorMessage,
                              "unexpected response from server; first received character was \"%c\"\n",
                              id);
            conn->inCursor += msgLength;
			result->gr_status = GTM_RESULT_ERROR;
            break;
    }                    /* switch on protocol character */
    /* Successfully consumed this message */
    if (conn->inCursor == conn->inStart + 5 + msgLength)
    {
        /* Normal case: parsing agrees with specified length */
        conn->inStart = conn->inCursor;
    }
    else
    {
        /* Trouble --- report it */
        printfGTMPQExpBuffer(&conn->errorMessage,
                          "message contents do not agree with length in message type \"%c\"\n",
                          id);
        /* trust the specified message length as what to skip */
        conn->inStart += 5 + msgLength;

		result->gr_status = GTM_RESULT_ERROR;
    }

    return result;
}

/*
 * handleSyncLoss: clean up after loss of message-boundary sync
 *
 * There isn't really a lot we can do here except abandon the connection.
 */
static void
handleSyncLoss(GTM_Conn *conn, char id, int msgLength)
{
    printfGTMPQExpBuffer(&conn->errorMessage,
    "lost synchronization with server: got message type \"%c\", length %d\n",
                      id, msgLength);
    close(conn->sock);
    conn->sock = -1;
    conn->status = CONNECTION_BAD;        /* No more connection to backend */
}

/*
 * Attempt to read an Error or Notice response message.
 * This is possible in several places, so we break it out as a subroutine.
 * Entry: 'E' message type and length have already been consumed.
 * Exit: returns 0 if successfully consumed message.
 *         returns EOF if not enough data.
 */
int
gtmpqGetError(GTM_Conn *conn, GTM_Result *result)
{// #lizard forgives
    char        id;

    /*
     * If we are a GTM proxy, expect an additional proxy header in the incoming
     * message.
     */
    if (conn->remote_type == GTM_NODE_GTM_PROXY)
    {
        if (gtmpqGetnchar((char *)&result->gr_proxyhdr,
                    sizeof (GTM_ProxyMsgHeader), conn))
            return 1;
        result->gr_msglen -= sizeof (GTM_ProxyMsgHeader);

        /*
         * If the allocated buffer is not large enough to hold the proxied
         * data, realloc the buffer.
         *
         * Since the client side code is shared between the proxy and the
         * backend, we don't want any memory context management etc here. So
         * just use plain realloc. Anyways, we don't indent to free the memory.
         */
        if (result->gr_proxy_datalen < result->gr_msglen)
        {
            result->gr_proxy_data = (char *)realloc(
                    result->gr_proxy_data, result->gr_msglen);
            result->gr_proxy_datalen = result->gr_msglen;
        }

        if (gtmpqGetnchar((char *)result->gr_proxy_data,
                    result->gr_msglen, conn))
        {
            result->gr_status = GTM_RESULT_UNKNOWN;
            return 1;
        }

        return 0;
    }
    else
        result->gr_proxyhdr.ph_conid = InvalidGTMProxyConnID;

    /*
     * Read the fields and save into res.
     */
    for (;;)
    {
        if (gtmpqGetc(&id, conn))
            goto fail;
        if (id == '\0')
            break;
        if (gtmpqGets(&conn->errorMessage, conn))
            goto fail;
    }
    return 0;

fail:
    return EOF;
}

/*
 * gtmpQgetResult
 *      Get the next GTM_Result produced.  Returns NULL if no
 *      query work remains or an error has occurred (e.g. out of
 *      memory).
 */

GTM_Result *
GTMPQgetResult(GTM_Conn *conn)
{// #lizard forgives
    GTM_Result *res;

    if (!conn)
        return NULL;

    /* Parse any available data, if our state permits. */
    while ((res = pqParseInput(conn)) == NULL)
    {
        int            flushResult;

        /*
         * If data remains unsent, send it.  Else we might be waiting for the
         * result of a command the backend hasn't even got yet.
         */
        while ((flushResult = gtmpqFlush(conn)) > 0)
        {
            if (gtmpqWait(false, true, conn))
            {
                flushResult = -1;
                break;
            }
        }

        /* Wait for some more data, and load it. */
        if (flushResult ||
            gtmpqWait(true, false, conn) ||
            gtmpqReadData(conn) < 0)
        {
            /*
             * conn->errorMessage has been set by gtmpqWait or gtmpqReadData.
             */
            return NULL;
        }
    }

    return res;
}

/*
 * return 0 if parsing command is totally completed.
 * return 1 if it needs to be read continuously.
 */
static int
gtmpqParseSuccess(GTM_Conn *conn, GTM_Result *result)
{// #lizard forgives
    int xcnt, xsize;
    int i;
    GlobalTransactionId *xip = NULL;

    result->gr_status = GTM_RESULT_OK;

    if (gtmpqGetInt((int *)&result->gr_type, 4, conn))
        return 1;
    result->gr_msglen -= 4;

    if (conn->remote_type == GTM_NODE_GTM_PROXY)
    {
        if (gtmpqGetnchar((char *)&result->gr_proxyhdr,
                    sizeof (GTM_ProxyMsgHeader), conn))
            return 1;
        result->gr_msglen -= sizeof (GTM_ProxyMsgHeader);
    }
    else
        result->gr_proxyhdr.ph_conid = InvalidGTMProxyConnID;

    /*
     * If we are dealing with a proxied message, just read the remaining binary
     * data which can then be forwarded to the right backend.
     */
    if (result->gr_proxyhdr.ph_conid != InvalidGTMProxyConnID)
    {
        /*
         * If the allocated buffer is not large enough to hold the proxied
         * data, realloc the buffer.
         *
         * Since the client side code is shared between the proxy and the
         * backend, we don't want any memory context management etc here. So
         * just use plain realloc. Anyways, we don't indent to free the memory.
         */
        if (result->gr_proxy_datalen < result->gr_msglen)
        {
            result->gr_proxy_data = (char *)realloc(
                    result->gr_proxy_data, result->gr_msglen);
            result->gr_proxy_datalen = result->gr_msglen;
        }

        if (gtmpqGetnchar((char *)result->gr_proxy_data,
                    result->gr_msglen, conn))
        {
            result->gr_status = GTM_RESULT_UNKNOWN;
            return 1;
        }

        return result->gr_status;
    }

    result->gr_status = GTM_RESULT_OK;

    switch (result->gr_type)
    {
        case SYNC_STANDBY_RESULT:
            break;

        case NODE_BEGIN_REPLICATION_INIT_RESULT:
            break;

        case NODE_END_REPLICATION_INIT_RESULT:
            break;

        case BEGIN_BACKUP_SUCCEED_RESULT:
            result->gr_resdata.backup_result = true;
            break;

        case BEGIN_BACKUP_FAIL_RESULT:
            result->gr_resdata.backup_result = false;
            break;

        case END_BACKUP_RESULT:
            break;

#ifdef XCP
        case REGISTER_SESSION_RESULT:
break;
#endif

        case TXN_BEGIN_RESULT:
            if (gtmpqGetnchar((char *) &result->gr_resdata.grd_txnhandle,
                              sizeof(GTM_TransactionHandle), conn))
                result->gr_status = GTM_RESULT_ERROR;
            break;

        case TXN_BEGIN_GETGXID_RESULT:
            if (gtmpqGetnchar((char *) &result->gr_resdata.grd_gxid_tp.gxid,
                              sizeof(GlobalTransactionId), conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }

            if (gtmpqGetnchar((char *) &result->gr_resdata.grd_gxid_tp.timestamp,
                              sizeof(GTM_Timestamp), conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }
            break;

#ifdef __TBASE__
            /* Add for global timestamp */
        case TXN_BEGIN_GETGTS_RESULT:
            if (gtmpqGetnchar((char *) &result->gr_resdata.grd_gts.grd_gts,
                              sizeof(GTM_Timestamp), conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }
            /* compatible with former protocol,gtm sends read-only flag only if it's in read-only state */
            if (gtmpqGetc(&result->gr_resdata.grd_gts.gtm_readonly, conn) == EOF)
            {
                result->gr_resdata.grd_gts.gtm_readonly = false;
            }
            break;

        case TXN_BEGIN_GETGTS_MULTI_RESULT:

            if (gtmpqGetnchar((char *) &result->gr_resdata.grd_gts.grd_gts,
                              sizeof(GTM_Timestamp), conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }
            break;


        case TXN_CHECK_GTM_STATUS_RESULT:
        {
            int len = 0;
            int count = 0;
            memset(&result->gr_resdata.grd_gts, 0, sizeof(result->gr_resdata.grd_gts));

            if (gtmpqGetnchar((char *) &result->gr_resdata.grd_gts.node_status,
                              sizeof(int), conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }

            if (gtmpqGetnchar((char *) &result->gr_resdata.grd_gts.grd_gts,
                              sizeof(GTM_Timestamp), conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }

            if (gtmpqGetInt64((int64 *)(&result->gr_resdata.grd_gts.master_flush),conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }

            if (gtmpqGetInt(&count, sizeof(int), conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }

            result->gr_resdata.grd_gts.standby_count = count;

            if(count == 0)
                break;

            if(count > GTM_MAX_WALSENDER)
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }

            result->gr_resdata.grd_gts.slave_is_sync   = malloc(sizeof(int) * count);
            result->gr_resdata.grd_gts.slave_timestamp = malloc(sizeof(GTM_Timestamp) * count);
            result->gr_resdata.grd_gts.slave_flush_ptr = malloc(sizeof(XLogRecPtr) * count);

            for( i = 0 ; i < count ; i++)
            {
                if(gtmpqGetInt(result->gr_resdata.grd_gts.slave_is_sync + i,sizeof(int),conn))
                {
                    result->gr_status = GTM_RESULT_ERROR;
                    break;
                }

                if(gtmpqGetInt(&len,sizeof(int),conn))
                {
                    result->gr_status = GTM_RESULT_ERROR;
                    break;
                }

                result->gr_resdata.grd_gts.application_name[i] = malloc(len);
                if(result->gr_resdata.grd_gts.application_name[i] == NULL)
                {
                    result->gr_status = GTM_RESULT_ERROR;
                    break;
                }

                if(gtmpqGetnchar(result->gr_resdata.grd_gts.application_name[i],len,conn))
                {
                    result->gr_status = GTM_RESULT_ERROR;
                    break;
                }

                if (gtmpqGetnchar((char *) (result->gr_resdata.grd_gts.slave_timestamp + i),
                                  sizeof(GTM_Timestamp), conn))
                {
                    result->gr_status = GTM_RESULT_ERROR;
                    break;
                }

                if(gtmpqGetInt64((int64 *)(result->gr_resdata.grd_gts.slave_flush_ptr + i),conn))
                {
                    result->gr_status = GTM_RESULT_ERROR;
                    break;
                }
            }
            break;
        }
#endif
        case TXN_BEGIN_GETGXID_AUTOVACUUM_RESULT:
        case TXN_PREPARE_RESULT:
        case TXN_START_PREPARED_RESULT:
        case TXN_LOG_TRANSACTION_RESULT:
        case TXN_LOG_SCAN_RESULT:
        case TXN_ROLLBACK_RESULT:
            if (gtmpqGetnchar((char *) &result->gr_resdata.grd_gxid,
                              sizeof(GlobalTransactionId), conn))
                result->gr_status = GTM_RESULT_ERROR;
            break;

        case TXN_COMMIT_RESULT:
        case TXN_COMMIT_PREPARED_RESULT:
            if (gtmpqGetnchar((char *) &result->gr_resdata.grd_eof_txn.gxid,
                              sizeof(GlobalTransactionId), conn))
                result->gr_status = GTM_RESULT_ERROR;
            if (gtmpqGetnchar((char *) &result->gr_resdata.grd_eof_txn.status,
                              sizeof(int), conn))
                result->gr_status = GTM_RESULT_ERROR;
            break;

        case TXN_GET_GXID_RESULT:
            if (gtmpqGetnchar((char *) &result->gr_resdata.grd_txn.txnhandle,
                              sizeof(GTM_TransactionHandle), conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }

            if (gtmpqGetnchar((char *) &result->gr_resdata.grd_txn.gxid,
                              sizeof(GlobalTransactionId), conn))
                result->gr_status = GTM_RESULT_ERROR;
            break;

        case TXN_GET_NEXT_GXID_RESULT:
            if (gtmpqGetInt((int *) &result->gr_resdata.grd_next_gxid,
                            sizeof(int32), conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }
            break;

        case TXN_BEGIN_GETGXID_MULTI_RESULT:
            if (gtmpqGetnchar((char *) &result->gr_resdata.grd_txn_get_multi.txn_count,
                              sizeof(int), conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }
            if (gtmpqGetnchar((char *) result->gr_resdata.grd_txn_get_multi.txn_gxid,
                              sizeof(GlobalTransactionId) * result->gr_resdata.grd_txn_get_multi.txn_count,
                              conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }
            if (gtmpqGetnchar((char *) &result->gr_resdata.grd_txn_get_multi.timestamp,
                              sizeof(GTM_Timestamp), conn))
                result->gr_status = GTM_RESULT_ERROR;
            break;

        case TXN_COMMIT_MULTI_RESULT:
        case TXN_ROLLBACK_MULTI_RESULT:
            if (gtmpqGetnchar((char *) &result->gr_resdata.grd_txn_rc_multi.txn_count,
                              sizeof(int), conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }
            if (gtmpqGetnchar((char *) result->gr_resdata.grd_txn_rc_multi.status,
                              sizeof(int) * result->gr_resdata.grd_txn_rc_multi.txn_count, conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }
            break;

        case SNAPSHOT_GXID_GET_RESULT:
            if (gtmpqGetnchar((char *) &result->gr_resdata.grd_txn_snap_multi.txnhandle,
                              sizeof(GTM_TransactionHandle), conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }
            /* Fall through */
        case SNAPSHOT_GET_RESULT:
            if (gtmpqGetnchar((char *) &result->gr_resdata.grd_txn_snap_multi.gxid,
                              sizeof(GlobalTransactionId), conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }
            /* Fall through */
        case SNAPSHOT_GET_MULTI_RESULT:
            if (gtmpqGetnchar((char *) &result->gr_resdata.grd_txn_snap_multi.txn_count,
                              sizeof(int), conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }
            if (gtmpqGetnchar((char *) result->gr_resdata.grd_txn_snap_multi.status,
                              sizeof(int) * result->gr_resdata.grd_txn_snap_multi.txn_count, conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }

            if (gtmpqGetnchar((char *) &result->gr_snapshot.sn_xmin,
                              sizeof(GlobalTransactionId), conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }

            if (gtmpqGetnchar((char *) &result->gr_snapshot.sn_xmax,
                              sizeof(GlobalTransactionId), conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }

            if (gtmpqGetInt((int *) &result->gr_snapshot.sn_xcnt,
                            sizeof(int32), conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }

            xsize = result->gr_xip_size;
            xcnt = result->gr_snapshot.sn_xcnt;
            xip = result->gr_snapshot.sn_xip;

            if (!xip || xcnt > xsize)
            {
                if (!xip)
                    xip = (GlobalTransactionId *) malloc(sizeof(GlobalTransactionId) *
                                                         GTM_MAX_GLOBAL_TRANSACTIONS);
                else
                    xip = (GlobalTransactionId *) realloc(xip,
                                                          sizeof(GlobalTransactionId) * xcnt);

                result->gr_snapshot.sn_xip = xip;
                result->gr_xip_size = xcnt;
            }

            if (gtmpqGetnchar((char *) xip, sizeof(GlobalTransactionId) * xcnt, conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }

            break;

        case SEQUENCE_INIT_RESULT:
        case SEQUENCE_RESET_RESULT:
        case SEQUENCE_CLOSE_RESULT:
        case SEQUENCE_RENAME_RESULT:
        case SEQUENCE_COPY_RESULT:
        case SEQUENCE_ALTER_RESULT:
        case SEQUENCE_SET_VAL_RESULT:
        case MSG_DB_SEQUENCE_RENAME_RESULT:
            if (gtmpqReadSeqKey(&result->gr_resdata.grd_seqkey, conn))
                result->gr_status = GTM_RESULT_ERROR;
            break;

        case SEQUENCE_GET_CURRENT_RESULT:
        case SEQUENCE_GET_NEXT_RESULT:
        case SEQUENCE_GET_LAST_RESULT:
            if (gtmpqReadSeqKey(&result->gr_resdata.grd_seq.seqkey, conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }
            if (gtmpqGetnchar((char *) &result->gr_resdata.grd_seq.seqval,
                              sizeof(GTM_Sequence), conn))
                result->gr_status = GTM_RESULT_ERROR;
#ifdef XCP
        if (result->gr_type == SEQUENCE_GET_NEXT_RESULT &&
gtmpqGetnchar((char *)&result->gr_resdata.grd_seq.rangemax,
sizeof (GTM_Sequence), conn))
result->gr_status = GTM_RESULT_ERROR;
#endif
            break;

#ifdef __TBASE__
        case STORAGE_TRANSFER_RESULT:
        {
            int32 loop_count = 0;
            int32 offset = 0;
            int data_len = 0;
            char *data_buf = NULL;

#ifdef __XLOG__
            /* get xlog start pos and timeline */
            if (gtmpqGetInt64((int64 *)&result->grd_storage_data.start_pos, conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }

            if (gtmpqGetInt((int *)&result->grd_storage_data.time_line,
                            sizeof(TimeLineID), conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }
#endif

			/* communication protocol: total data len, pkg number, {pkg_len,pkg_data}, {pkg_len,pkg_data},*/
			if (gtmpqGetInt(&result->grd_storage_data.len,
							sizeof(uint32), conn))
			{
				result->gr_status = GTM_RESULT_ERROR;
				break;
			}

			/* get loop count */
			if (gtmpqGetInt(&loop_count,
							sizeof(uint32), conn))
			{
				result->gr_status = GTM_RESULT_ERROR;
				break;
			}

			result->grd_storage_data.data = (char *) malloc(result->grd_storage_data.len);
			data_buf = result->grd_storage_data.data;
			for (i = 0; i < loop_count; i++)
			{
				/* a length of the next send pkg */
				if (gtmpqGetInt(&data_len, sizeof(int32), conn))
				{
					result->gr_status = GTM_RESULT_ERROR;
					break;
				}

				/* pkg body */
				if (gtmpqGetnchar(data_buf + offset, data_len, conn))
				{
					result->gr_status = GTM_RESULT_ERROR;
					break;
				}
				offset += data_len;
			}

			if (result->gr_status != GTM_RESULT_OK)
			{
				if (offset != result->grd_storage_data.len)
				{
					abort();
				}
			}
		}
			break;

		case TXN_FINISH_GID_RESULT:
		{
			if (gtmpqGetInt(&result->gr_finish_status,
							sizeof(uint32), conn))
			{
				result->gr_status = GTM_RESULT_ERROR;
				break;
			}
			break;
		}

		case MSG_LIST_GTM_STORE_RESULT:
		{
			if (gtmpqGetInt64(&result->gtm_status.header.m_identifier, conn))
			{
				result->gr_status = GTM_RESULT_ERROR;
				break;
			}

			if (gtmpqGetInt(&result->gtm_status.header.m_major_version, sizeof(int32), conn))
			{
				result->gr_status = GTM_RESULT_ERROR;
				break;
			}

			if (gtmpqGetInt(&result->gtm_status.header.m_minor_version, sizeof(int32), conn))
			{
				result->gr_status = GTM_RESULT_ERROR;
				break;
			}

			if (gtmpqGetInt(&result->gtm_status.header.m_gtm_status, sizeof(int32), conn))
			{
				result->gr_status = GTM_RESULT_ERROR;
				break;
			}

			if (gtmpqGetInt64(&result->gtm_status.header.m_next_gts, conn))
			{
				result->gr_status = GTM_RESULT_ERROR;
				break;
			}

			if (gtmpqGetInt((int32 *) &result->gtm_status.header.m_global_xmin, sizeof(int32), conn))
			{
				result->gr_status = GTM_RESULT_ERROR;
				break;
			}

			if (gtmpqGetInt((int32 *) &result->gtm_status.header.m_next_gxid, sizeof(int32), conn))
			{
				result->gr_status = GTM_RESULT_ERROR;
				break;
			}

			if (gtmpqGetInt((int32 *) &result->gtm_status.header.m_seq_freelist, sizeof(int32), conn))
			{
				result->gr_status = GTM_RESULT_ERROR;
				break;
			}

			if (gtmpqGetInt((int32 *) &result->gtm_status.header.m_txn_freelist, sizeof(int32), conn))
			{
				result->gr_status = GTM_RESULT_ERROR;
				break;
			}

			if (gtmpqGetInt64(&result->gtm_status.header.m_lsn, conn))
			{
				result->gr_status = GTM_RESULT_ERROR;
				break;
			}


			if (gtmpqGetInt64(&result->gtm_status.header.m_last_update_time, conn))
			{
				result->gr_status = GTM_RESULT_ERROR;
				break;
			}

			if (gtmpqGetInt((int32 *) &result->gtm_status.header.m_crc, sizeof(int32), conn))
			{
				result->gr_status = GTM_RESULT_ERROR;
				break;
			}

			if (gtmpqGetInt((int32 *) &result->gtm_status.seq_total, sizeof(int32), conn))
			{
				result->gr_status = GTM_RESULT_ERROR;
				break;
			}

			if (gtmpqGetInt((int32 *) &result->gtm_status.seq_used, sizeof(int32), conn))
			{
				result->gr_status = GTM_RESULT_ERROR;
				break;
			}

			if (gtmpqGetInt((int32 *) &result->gtm_status.txn_total, sizeof(int32), conn))
			{
				result->gr_status = GTM_RESULT_ERROR;
				break;
			}

			if (gtmpqGetInt((int32 *) &result->gtm_status.txn_used, sizeof(int32), conn))
			{
				result->gr_status = GTM_RESULT_ERROR;
				break;
			}
			break;
		}

		case MSG_LIST_GTM_STORE_SEQ_RESULT:    /* List  gtm running sequence info */
		{
			if (gtmpqGetInt(&conn->result->grd_store_seq.count,
							sizeof(int32), conn))
			{
				result->gr_status = GTM_RESULT_ERROR;
				break;
			}

			conn->result->grd_store_seq.seqs =
					(GTM_StoredSeqInfo *) malloc(sizeof(GTM_StoredSeqInfo) *
												 conn->result->grd_store_seq.count);
			for (i = 0; i < conn->result->grd_store_seq.count; i++)
			{
				if (gtmpqGetnchar((char *) &conn->result->grd_store_seq.seqs[i], sizeof(GTM_StoredSeqInfo), conn))
				{
					result->gr_status = GTM_RESULT_ERROR;
					break;
				}
			}
			break;
		}

		case MSG_LIST_GTM_TXN_STORE_RESULT:    /* List  gtm running sequence info */
		{
			if (gtmpqGetInt(&conn->result->grd_store_txn.count,
							sizeof(int32), conn))
			{
				result->gr_status = GTM_RESULT_ERROR;
				break;
			}

			conn->result->grd_store_txn.txns =
					(GTM_StoredTransactionInfo *) malloc(sizeof(GTM_StoredTransactionInfo) *
														 conn->result->grd_store_txn.count);
			for (i = 0; i < conn->result->grd_store_txn.count; i++)
			{
				if (gtmpqGetnchar((char *) &conn->result->grd_store_txn.txns[i], sizeof(GTM_StoredTransactionInfo),
								  conn))
				{
					result->gr_status = GTM_RESULT_ERROR;
					break;
				}
			}
			break;
		}


		case MSG_CHECK_GTM_SEQ_STORE_RESULT:    /* Check gtm sequence valid info */
		{
			if (gtmpqGetInt(&conn->result->grd_store_check_seq.count,
							sizeof(int32), conn))
			{
				result->gr_status = GTM_RESULT_ERROR;
				break;
			}

			conn->result->grd_store_check_seq.seqs =
					(GTMStorageSequneceStatus *) malloc(sizeof(GTMStorageSequneceStatus) *
														conn->result->grd_store_check_seq.count);
			for (i = 0; i < conn->result->grd_store_check_seq.count; i++)
			{
				if (gtmpqGetnchar((char *) &conn->result->grd_store_check_seq.seqs[i], sizeof(GTMStorageSequneceStatus),
								  conn))
				{
					result->gr_status = GTM_RESULT_ERROR;
					break;
				}
			}
			break;
		}

		case MSG_CHECK_GTM_TXN_STORE_RESULT:    /* Check gtm transaction usage info */
		{
			if (gtmpqGetInt(&conn->result->grd_store_check_txn.count,
							sizeof(int32), conn))
			{
				result->gr_status = GTM_RESULT_ERROR;
				break;
			}

			conn->result->grd_store_check_txn.txns =
					(GTMStorageTransactionStatus *) malloc(sizeof(GTMStorageTransactionStatus) *
														   conn->result->grd_store_check_txn.count);
			for (i = 0; i < conn->result->grd_store_check_txn.count; i++)
			{
				if (gtmpqGetnchar((char *) &conn->result->grd_store_check_txn.txns[i],
								  sizeof(GTMStorageTransactionStatus), conn))
				{
					result->gr_status = GTM_RESULT_ERROR;
					break;
				}
			}
			break;
		}

        case MSG_GET_GTM_STATISTICS_RESULT:
        {
            if (gtmpqGetInt64(&result->gr_resdata.statistic_result.start_time, conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }

            if (gtmpqGetInt64(&result->gr_resdata.statistic_result.end_time, conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }

            if (gtmpqGetInt(&result->gr_resdata.statistic_result.sequences_remained,
                              sizeof(int32), conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }

            if (gtmpqGetInt(&result->gr_resdata.statistic_result.txn_remained,
                            sizeof(int32), conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }

            for (i = 0; i < CMD_STATISTICS_TYPE_COUNT; i++)
            {
                if (gtmpqGetInt((int32*) &result->gr_resdata.statistic_result.stat_info[i].total_request_times,
                                sizeof(int32), conn))
                {
                    result->gr_status = GTM_RESULT_ERROR;
                    break;
                }

                if (gtmpqGetInt((int32*) &result->gr_resdata.statistic_result.stat_info[i].avg_costtime,
                                sizeof(int32), conn))
                {
                    result->gr_status = GTM_RESULT_ERROR;
                    break;
                }

                if (gtmpqGetInt((int32*) &result->gr_resdata.statistic_result.stat_info[i].max_costtime,
                                sizeof(int32), conn))
                {
                    result->gr_status = GTM_RESULT_ERROR;
                    break;
                }

                if (gtmpqGetInt((int32*) &result->gr_resdata.statistic_result.stat_info[i].min_costtime,
                                sizeof(int32), conn))
                {
                    result->gr_status = GTM_RESULT_ERROR;
                    break;
                }
            }

            break;
        }
	    case MSG_GET_GTM_ERRORLOG_RESULT:
        {
            result->grd_errlog.len = result->gr_msglen;
            if (result->gr_msglen == 0)
            {
                break;
            }

            result->grd_errlog.errlog =
                    (char *) malloc(result->gr_msglen);
            if (gtmpqGetnchar((char *) result->grd_errlog.errlog,
                              result->gr_msglen, conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }
            break;
        }

#endif
        case SEQUENCE_LIST_RESULT:
            if (gtmpqGetInt(&result->gr_resdata.grd_seq_list.seq_count,
                            sizeof(int32), conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }

            result->gr_resdata.grd_seq_list.seq =
                    (GTM_SeqInfo *) malloc(sizeof(GTM_SeqInfo) *
                                           result->gr_resdata.grd_seq_list.seq_count);

            for (i = 0; i < result->gr_resdata.grd_seq_list.seq_count; i++)
            {
                int buflen;
                char *buf;

                /* a length of the next serialized sequence */
                if (gtmpqGetInt(&buflen, sizeof(int32), conn))
                {
                    result->gr_status = GTM_RESULT_ERROR;
                    break;
                }

                /* a data body of the serialized sequence */
                buf = (char *) malloc(buflen);
                if (gtmpqGetnchar(buf, buflen, conn))
                {
                    result->gr_status = GTM_RESULT_ERROR;
                    free(buf);
                    break;
                }

                gtm_deserialize_sequence(result->gr_resdata.grd_seq_list.seq + i,
                                         buf, buflen);

                free(buf);
            }
            break;

        case TXN_GET_STATUS_RESULT:
            break;

        case TXN_GET_ALL_PREPARED_RESULT:
            break;

        case TXN_GET_GID_DATA_RESULT:
            if (gtmpqGetnchar((char *) &result->gr_resdata.grd_txn_get_gid_data.gxid,
                              sizeof(GlobalTransactionId), conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }
            if (gtmpqGetnchar((char *) &result->gr_resdata.grd_txn_get_gid_data.prepared_gxid,
                              sizeof(GlobalTransactionId), conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }
            if (gtmpqGetInt(&result->gr_resdata.grd_txn_get_gid_data.nodelen,
                            sizeof(int32), conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }
            if (result->gr_resdata.grd_txn_get_gid_data.nodelen != 0)
            {
				/* Do necessary allocation, free outside */
                result->gr_resdata.grd_txn_get_gid_data.nodestring =
                        (char *) malloc(sizeof(char *) * result->gr_resdata.grd_txn_get_gid_data.nodelen + 1);
                if (result->gr_resdata.grd_txn_get_gid_data.nodestring == NULL)
                {
                    result->gr_status = GTM_RESULT_ERROR;
                    break;
                }

                /* get the string itself */
                if (gtmpqGetnchar(result->gr_resdata.grd_txn_get_gid_data.nodestring,
                                  result->gr_resdata.grd_txn_get_gid_data.nodelen, conn))
                {
                    result->gr_status = GTM_RESULT_ERROR;
                    break;
                }

                /* null terminate the name*/
                result->gr_resdata.grd_txn_get_gid_data.nodestring[result->gr_resdata.grd_txn_get_gid_data.nodelen] = '\0';
            }
            else
                result->gr_resdata.grd_txn_get_gid_data.nodestring = NULL;

            break;

        case TXN_GXID_LIST_RESULT:
            result->gr_resdata.grd_txn_gid_list.len = 0;
            result->gr_resdata.grd_txn_gid_list.ptr = NULL;

            if (gtmpqGetInt((int *) &result->gr_resdata.grd_txn_gid_list.len,
                            sizeof(int32), conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }

            /*
             * I don't understand why malloc() here?  Should be palloc()?
             */
            result->gr_resdata.grd_txn_gid_list.ptr =
                    (char *) malloc(result->gr_resdata.grd_txn_gid_list.len);

            if (result->gr_resdata.grd_txn_gid_list.ptr == NULL)
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }

            if (gtmpqGetnchar(result->gr_resdata.grd_txn_gid_list.ptr,
                              result->gr_resdata.grd_txn_gid_list.len,
                              conn))  /* serialized GTM_Transactions */
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }
            break;

        case NODE_UNREGISTER_RESULT:
        case NODE_REGISTER_RESULT:
            result->gr_resdata.grd_node.len = 0;
            result->gr_resdata.grd_node.node_name = NULL;

            if (gtmpqGetnchar((char *) &result->gr_resdata.grd_node.type,
                              sizeof(GTM_PGXCNodeType), conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }
            if (gtmpqGetInt((int *) &result->gr_resdata.grd_node.len,
                            sizeof(int32), conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }

            result->gr_resdata.grd_node.node_name =
                    (char *) malloc(result->gr_resdata.grd_node.len + 1);

            if (result->gr_resdata.grd_node.node_name == NULL)
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }

            if (gtmpqGetnchar(result->gr_resdata.grd_node.node_name,
                              result->gr_resdata.grd_node.len,
                              conn))  /* serialized GTM_Transactions */
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }
            result->gr_resdata.grd_node.node_name[result->gr_resdata.grd_node.len] = '\0';

            break;

		case NODE_LIST_RESULT:
		{
			int i;
            char *buf = NULL;
            int   buf_size = 8192;

            memset(result->gr_resdata.grd_node_list.nodeinfo, 0, sizeof(result->gr_resdata.grd_node_list.nodeinfo));

            if (gtmpqGetInt(&result->gr_resdata.grd_node_list.num_node, sizeof(int32), conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }

            buf = (char *) malloc(buf_size);
            if (buf == NULL)
            {
                result->gr_status = GTM_RESULT_ERROR;
                printfGTMPQExpBuffer(&conn->errorMessage, "malloc buffer for node list data failed");
                break;
            }

			for (i = 0; i < result->gr_resdata.grd_node_list.num_node; i++)
			{
				int size;
				GTM_PGXCNodeInfo *data = (GTM_PGXCNodeInfo *) malloc(sizeof(GTM_PGXCNodeInfo));
                memset(data, 0, sizeof(GTM_PGXCNodeInfo));

				if (gtmpqGetInt(&size, sizeof(int32), conn))
				{
					result->gr_status = GTM_RESULT_ERROR;
					free(data);
					break;
				}

				if (size > buf_size)
				{
                    buf = (char *) realloc(buf, size);
                    if (buf == NULL)
                    {
                        result->gr_status = GTM_RESULT_ERROR;
                        printfGTMPQExpBuffer(&conn->errorMessage, "realloc buffer for node list data failed");
                        free(data);
                        break;
                    }
                    buf_size = size;
				}

				if (gtmpqGetnchar(buf, size, conn))
				{
					result->gr_status = GTM_RESULT_ERROR;
					free(data);
					break;
				}
				if (!gtm_deserialize_pgxcnodeinfo(data, buf, size, &conn->errorMessage))
				{
					result->gr_status = GTM_RESULT_ERROR;
                    if (data->nodename)
                    {
                        genFree(data->nodename);
                    }
                    if (data->proxyname)
                    {
                        genFree(data->proxyname);
                    }
                    if (data->ipaddress)
                    {
                        genFree(data->ipaddress);
                    }
                    if (data->datafolder)
                    {
                        genFree(data->datafolder);
                    }
                    if (data->sessions)
                    {
                        genFree(data->sessions);
                    }
					free(data);
					break;
				}
				else
				{
					result->gr_resdata.grd_node_list.nodeinfo[i] = data;
				}
			}

			if (buf != NULL)
            {
			    free(buf);
            }
			break;
		}
		case BARRIER_RESULT:
			break;

        case REPORT_XMIN_RESULT:
            if (gtmpqGetnchar((char *) &result->gr_resdata.grd_report_xmin.latest_completed_xid,
                              sizeof(GlobalTransactionId), conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }
            if (gtmpqGetnchar((char *) &result->gr_resdata.grd_report_xmin.global_xmin,
                              sizeof(GlobalTransactionId), conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }
            if (gtmpqGetnchar((char *) &result->gr_resdata.grd_report_xmin.errcode,
                              sizeof(int), conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }
            break;

        case MSG_CLEAN_SESSION_SEQ_RESULT:
            break;

#ifdef __XLOG__
        case MSG_REPLICATION_START_RESULT_SUCCESS:
            break;

        case MSG_REPLICATION_START_RESULT_FAIL:
            break;

        case MSG_REPLICATION_STATUS:
            if (gtmpqGetInt64((int64 *) &result->gr_resdata.grd_replication.flush, conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }

            if (gtmpqGetInt64((int64 *) &result->gr_resdata.grd_replication.write, conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }

            if (gtmpqGetInt64((int64 *) &result->gr_resdata.grd_replication.apply, conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }

            break;

        case MSG_REPLICATION_CONTENT:
        {
            int loop_count = 0;
            int offset     = 0;
            int pack_size  = 0;
            int i          = 0;
            result->gr_resdata.grd_xlog_data.length = 0;
            result->gr_resdata.grd_xlog_data.xlog_data = NULL;

            if (gtmpqGetInt(&result->gr_resdata.grd_xlog_data.status, sizeof(int), conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }

            if (result->gr_resdata.grd_xlog_data.status != Send_OK)
            {
                break;
            }

            if (gtmpqGetInt64((int64 *)&result->gr_resdata.grd_xlog_data.flush, conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }

            if (gtmpqGetInt(&result->gr_resdata.grd_xlog_data.reply,sizeof(int), conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }

            if (gtmpqGetInt64((int64 *) &result->gr_resdata.grd_xlog_data.pos, conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }

            if (gtmpqGetInt(&result->gr_resdata.grd_xlog_data.length,
                            sizeof(int), conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }

            if(result->gr_resdata.grd_xlog_data.length > GTM_XLOG_SEG_SIZE)
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }

            if(result->gr_resdata.grd_xlog_data.length == 0)
                break;

            /* get loop count */
            if (gtmpqGetInt(&loop_count, sizeof(int), conn))
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }

            result->gr_resdata.grd_xlog_data.xlog_data = (char *) malloc(result->gr_resdata.grd_xlog_data.length);

            for(i = 0 ; i < loop_count;i++ )
            {
                if (gtmpqGetInt(&pack_size, sizeof(int), conn))
                {
                    result->gr_status = GTM_RESULT_ERROR;
                    break;
                }

                if (gtmpqGetnchar(result->gr_resdata.grd_xlog_data.xlog_data + offset, (uint32_t)pack_size, conn))
                {
                    result->gr_status = GTM_RESULT_ERROR;
                    break;
                }

                offset += pack_size;
            }

            if(offset != result->gr_resdata.grd_xlog_data.length)
            {
                result->gr_status = GTM_RESULT_ERROR;
                break;
            }

            break;
    }
#endif
        default:
            printfGTMPQExpBuffer(&conn->errorMessage,
                              "unexpected result type from server; result typr was \"%d\"\n",
                              result->gr_type);
            result->gr_status = GTM_RESULT_ERROR;
            break;
    }

    return (result->gr_status);
}

static int
gtmpqReadSeqKey(GTM_SequenceKey seqkey, GTM_Conn *conn)
{
    /*
     * Read keylength
     */
    if (gtmpqGetInt((int *)&seqkey->gsk_keylen, 4, conn))
        return EINVAL;

    /*
     * Do some sanity checks on the keylength
     */
    if (seqkey->gsk_keylen <= 0 || seqkey->gsk_keylen > GTM_MAX_SEQKEY_LENGTH)
        return EINVAL;

    if ((seqkey->gsk_key = (char *) malloc(seqkey->gsk_keylen))    == NULL)
        return ENOMEM;

    if (gtmpqGetnchar(seqkey->gsk_key, seqkey->gsk_keylen, conn))
        return EINVAL;

    return 0;
}

/*
 * release the one-time-applied memory. if the memory design is reused,
 * please release it last in freeGTM_Conn
 */
void
gtmpqFreeResultResource(GTM_Result *result)
{
    int i = 0;
    switch (result->gr_type)
    {
        case SEQUENCE_INIT_RESULT:
        case SEQUENCE_RESET_RESULT:
        case SEQUENCE_CLOSE_RESULT:
        case SEQUENCE_RENAME_RESULT:
        case SEQUENCE_COPY_RESULT:
        case SEQUENCE_ALTER_RESULT:
        case SEQUENCE_SET_VAL_RESULT:
        case MSG_DB_SEQUENCE_RENAME_RESULT:
            if (result->gr_resdata.grd_seqkey.gsk_key != NULL)
                free(result->gr_resdata.grd_seqkey.gsk_key);
            result->gr_resdata.grd_seqkey.gsk_key = NULL;
            break;

        case SEQUENCE_GET_CURRENT_RESULT:
        case SEQUENCE_GET_NEXT_RESULT:
        case SEQUENCE_GET_LAST_RESULT:
            if (result->gr_resdata.grd_seq.seqkey.gsk_key != NULL)
                free(result->gr_resdata.grd_seq.seqkey.gsk_key);
            result->gr_resdata.grd_seqkey.gsk_key = NULL;
            break;

        case TXN_GET_STATUS_RESULT:
            break;

        case TXN_GET_ALL_PREPARED_RESULT:
            break;

        case BARRIER_RESULT:
            break;

        case SNAPSHOT_GET_RESULT:
        case SNAPSHOT_GXID_GET_RESULT:
            /*
             * Lets not free the xip array in the snapshot since we may need it
             * again shortly
             */
            break;
        case NODE_UNREGISTER_RESULT:
        case NODE_REGISTER_RESULT:
            if (result->gr_resdata.grd_node.node_name)
            {
                free(result->gr_resdata.grd_node.node_name);
                result->gr_resdata.grd_node.node_name = NULL;
            }
            break;
        case NODE_LIST_RESULT:
            if (result->gr_resdata.grd_node_list.num_node)
            {
                for (i = 0; i < result->gr_resdata.grd_node_list.num_node; i++)
                {
                    if (result->gr_resdata.grd_node_list.nodeinfo[i])
                    {
                        GTM_PGXCNodeInfo *data = result->gr_resdata.grd_node_list.nodeinfo[i];
                        if (data->nodename)
                        {
                            genFree(data->nodename);
                            data->nodename = NULL;
                        }
                        if (data->proxyname)
                        {
                            genFree(data->proxyname);
                            data->proxyname = NULL;
                        }
                        if (data->ipaddress)
                        {
                            genFree(data->ipaddress);
                            data->ipaddress = NULL;
                        }
                        if (data->datafolder)
                        {
                            genFree(data->datafolder);
                            data->datafolder = NULL;
                        }
                        if (data->sessions)
                        {
                            genFree(data->sessions);
                            data->sessions = NULL;
                        }
                        free(result->gr_resdata.grd_node_list.nodeinfo[i]);
                        result->gr_resdata.grd_node_list.nodeinfo[i] = NULL;
                    }
                }
                result->gr_resdata.grd_node_list.num_node = 0;
            }
            break;
#ifdef __XLOG__
        case MSG_REPLICATION_CONTENT:
            if (result->gr_resdata.grd_xlog_data.length && result->gr_resdata.grd_xlog_data.xlog_data)
            {
                free(result->gr_resdata.grd_xlog_data.xlog_data);
                result->gr_resdata.grd_xlog_data.xlog_data = NULL;
                result->gr_resdata.grd_xlog_data.length = 0;
            }
            break;
#endif
#ifdef __TBASE__
        case TXN_CHECK_GTM_STATUS_RESULT:
            if (result->gr_resdata.grd_gts.standby_count > 0 &&
                result->gr_resdata.grd_gts.standby_count <= GTM_MAX_WALSENDER)
            {
                if (result->gr_resdata.grd_gts.slave_is_sync)
                {
                    free(result->gr_resdata.grd_gts.slave_is_sync);
                    result->gr_resdata.grd_gts.slave_is_sync = NULL;
                }

                if (result->gr_resdata.grd_gts.slave_timestamp)
                {
                    free(result->gr_resdata.grd_gts.slave_timestamp);
                    result->gr_resdata.grd_gts.slave_timestamp = NULL;
                }

                if (result->gr_resdata.grd_gts.slave_flush_ptr)
                {
                    free(result->gr_resdata.grd_gts.slave_flush_ptr);
                    result->gr_resdata.grd_gts.slave_flush_ptr = NULL;
                }

                for (i = 0; i < result->gr_resdata.grd_gts.standby_count; i++)
                {
                    if (result->gr_resdata.grd_gts.application_name[i])
                    {
                        free(result->gr_resdata.grd_gts.application_name[i]);
                        result->gr_resdata.grd_gts.application_name[i] = NULL;
                    }
                }

                result->gr_resdata.grd_gts.standby_count = 0;
            }
            break;
        case MSG_GET_GTM_ERRORLOG_RESULT:
            if (result->grd_errlog.len && result->grd_errlog.errlog)
            {
                free(result->grd_errlog.errlog);
                result->grd_errlog.errlog = NULL;
                result->grd_errlog.len = 0;
            }
            break;
        case STORAGE_TRANSFER_RESULT:
            /* free result of last call */
            if (result->grd_storage_data.len && result->grd_storage_data.data)
            {
                free(result->grd_storage_data.data);
                result->grd_storage_data.data = NULL;
                result->grd_storage_data.len = 0;
            }
            break;
        case MSG_LIST_GTM_STORE_SEQ_RESULT:
            if (result->grd_store_seq.count && result->grd_store_seq.seqs)
            {
                free(result->grd_store_seq.seqs);
                result->grd_store_seq.seqs = NULL;
                result->grd_store_seq.count  = 0;
            }
            break;
        case MSG_LIST_GTM_TXN_STORE_RESULT:
            if (result->grd_store_txn.count && result->grd_store_txn.txns)
            {
                free(result->grd_store_txn.txns);
                result->grd_store_txn.txns = NULL;
                result->grd_store_txn.count = 0;
            }
            break;
        case MSG_CHECK_GTM_SEQ_STORE_RESULT:
            if (result->grd_store_check_seq.count && result->grd_store_check_seq.seqs)
            {
                free(result->grd_store_check_seq.seqs);
                result->grd_store_check_seq.seqs = NULL;
                result->grd_store_check_seq.count = 0;
            }
            break;
        case MSG_CHECK_GTM_TXN_STORE_RESULT:
            if (result->grd_store_check_txn.count && result->grd_store_check_txn.txns)
            {
                free(result->grd_store_check_txn.txns);
                result->grd_store_check_txn.txns = NULL;
                result->grd_store_check_txn.count = 0;
            }
            break;
#endif
        default:
            break;
    }
}

void
gtmpqFreeResultData(GTM_Result *result, GTM_PGXCNodeType remote_type)
{

    /*
	 * If we are running as a GTM proxy, we don't have anything to do. This may
	 * change though as we add more message types below and some of them may
	 * need cleanup even at the proxy level
	 */
	if (remote_type == GTM_NODE_GTM_PROXY)
		return;

    gtmpqFreeResultResource(result);
}
