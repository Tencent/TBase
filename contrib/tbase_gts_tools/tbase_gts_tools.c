#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "catalog/pg_type.h"
#include "catalog/namespace.h"
#include "utils/timestamp.h"
#include "utils/varlena.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "access/commit_ts.h"
#include "access/htup_details.h"
#include "storage/bufmgr.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

static Datum
items(PG_FUNCTION_ARGS, int log_level, bool with_data, bool only_id);

/*
 * bits_to_text
 *
 * Converts a bits8-array of 'len' bits to a human-readable
 * c-string representation.
 */
static char *
bits_to_text(bits8 *bits, int len)
{
	int i;
	char *str;

	str = palloc(len + 1);

	for (i = 0; i < len; i++)
		str[i] = (bits[(i / 8)] & (1 << (i % 8))) ? '1' : '0';

	str[i] = '\0';

	return str;
}

PG_FUNCTION_INFO_V1(txid_gts);

Datum
txid_gts(PG_FUNCTION_ARGS)
{
	TransactionId xid = PG_GETARG_UINT32(0);
	TimestampTz gts;
	bool found = false;

	if (TransactionIdIsNormal(xid))
	{
		found = TransactionIdGetCommitTsData(xid, &gts, NULL);
	}

	if (!found)
	{
		PG_RETURN_NULL();
	}

	PG_RETURN_INT64(gts);
}

/*
 * heap_page_items_with_gts
 *
 * Allows inspection of line pointers and tuple headers of a heap page.
 */
PG_FUNCTION_INFO_V1(heap_page_items_with_gts);

typedef struct heap_page_items_state
{
	TupleDesc tupd;
	Page page;
	uint16 offset;
} heap_page_items_state;

Datum
heap_page_items_with_gts(PG_FUNCTION_ARGS)
{
	return items(fcinfo, 0, true, false);
}

PG_FUNCTION_INFO_V1(heap_page_items_with_gts_log);

Datum
heap_page_items_with_gts_log(PG_FUNCTION_ARGS)
{
	return items(fcinfo, 1, true, false);
}

PG_FUNCTION_INFO_V1(heap_page_ids);

Datum
heap_page_ids(PG_FUNCTION_ARGS)
{
	return items(fcinfo, 1, false, true);
}

PG_FUNCTION_INFO_V1(heap_page_items_without_data);

Datum
heap_page_items_without_data(PG_FUNCTION_ARGS)
{
	return items(fcinfo, 1, false, false);
}

static Datum
items(PG_FUNCTION_ARGS, int log_level, bool with_data, bool only_id)
{
	bytea	*raw_page;
	int		raw_page_size;
	heap_page_items_state *inter_call_data = NULL;
	FuncCallContext *fctx;

	if (!superuser())
	{
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to use raw page functions"))));
	}

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc tupdesc;
		MemoryContext mctx;

		fctx = SRF_FIRSTCALL_INIT();
		mctx = MemoryContextSwitchTo(fctx->multi_call_memory_ctx);

		raw_page = PG_GETARG_BYTEA_P(0);
		raw_page_size = VARSIZE(raw_page) - VARHDRSZ;
		if (raw_page_size < SizeOfPageHeaderData)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("input page too small (%d bytes)", raw_page_size)));
		}

		inter_call_data = palloc(sizeof(heap_page_items_state));

		/* Build a tuple descriptor for our result type */
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		{
			elog(ERROR, "return type must be a row type");
		}

		inter_call_data->tupd = tupdesc;

		inter_call_data->offset = FirstOffsetNumber;
		inter_call_data->page = VARDATA(raw_page);

		fctx->max_calls = PageGetMaxOffsetNumber(inter_call_data->page);
		fctx->user_fctx = inter_call_data;

		MemoryContextSwitchTo(mctx);
	}

	fctx = SRF_PERCALL_SETUP();
	inter_call_data = fctx->user_fctx;

	if (fctx->call_cntr < fctx->max_calls)
	{
		Page page = inter_call_data->page;
		HeapTuple resultTuple;
		Datum result;
		ItemId id;
		Datum values[17];
		bool nulls[17];
		uint16 lp_offset;
		uint16 lp_flags;
		uint16 lp_len;

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		/* Extract information from the line pointer */

		id = PageGetItemId(page, inter_call_data->offset);

		lp_offset = ItemIdGetOffset(id);
		lp_flags = ItemIdGetFlags(id);
		lp_len = ItemIdGetLength(id);

		values[0] = UInt16GetDatum(inter_call_data->offset);
		values[1] = UInt16GetDatum(lp_offset);
		values[2] = UInt16GetDatum(lp_flags);
		values[3] = UInt16GetDatum(lp_len);

		/*
		 * We do just enough validity checking to make sure we don't reference
		 * data outside the page passed to us. The page could be corrupt in
		 * many other ways, but at least we won't crash.
		 */
		if (!only_id &&
			ItemIdHasStorage(id) &&
			lp_len >= MinHeapTupleSize &&
			lp_offset == MAXALIGN(lp_offset) &&
			lp_offset + lp_len <= BLCKSZ)
		{
			HeapTupleHeader tuphdr;
			bytea *tuple_data_bytea;
			int tuple_data_len;

			/* Extract information from the tuple header */

			tuphdr = (HeapTupleHeader)PageGetItem(page, id);

			values[4] = UInt32GetDatum(HeapTupleHeaderGetRawXmin(tuphdr));
			values[5] = UInt32GetDatum(HeapTupleHeaderGetRawXmax(tuphdr));
			values[6] = Int64GetDatum(HeapTupleHeaderGetXminTimestamp(tuphdr));
			values[7] = Int64GetDatum(HeapTupleHeaderGetXmaxTimestamp(tuphdr));

			/* shared with xvac */
			values[8] = UInt32GetDatum(HeapTupleHeaderGetRawCommandId(tuphdr));
			values[9] = PointerGetDatum(&tuphdr->t_ctid);
			values[10] = UInt32GetDatum(tuphdr->t_infomask2);
			values[11] = UInt32GetDatum(tuphdr->t_infomask);
#ifdef _MIGRATE_
			values[12] = Int32GetDatum(tuphdr->t_shardid);
			values[13] = UInt8GetDatum(tuphdr->t_hoff);
#else
			values[12] = UInt8GetDatum(tuphdr->t_hoff);
#endif

			if (with_data)
			{
				/* Copy raw tuple data into bytea attribute */
				tuple_data_len = lp_len - tuphdr->t_hoff;
				tuple_data_bytea = (bytea *)palloc(tuple_data_len + VARHDRSZ);
				SET_VARSIZE(tuple_data_bytea, tuple_data_len + VARHDRSZ);
				memcpy(VARDATA(tuple_data_bytea), (char *)tuphdr + tuphdr->t_hoff,
					   tuple_data_len);
				values[16] = PointerGetDatum(tuple_data_bytea);
			}
			else
			{
				nulls[16] = true;
			}

			/*
			 * We already checked that the item is completely within the raw
			 * page passed to us, with the length given in the line pointer.
			 * Let's check that t_hoff doesn't point over lp_len, before using
			 * it to access t_bits and oid.
			 */
			if (tuphdr->t_hoff >= SizeofHeapTupleHeader &&
				tuphdr->t_hoff <= lp_len &&
				tuphdr->t_hoff == MAXALIGN(tuphdr->t_hoff))
			{
				if (tuphdr->t_infomask & HEAP_HASNULL)
				{
					int bits_len;

					bits_len =
						((tuphdr->t_infomask2 & HEAP_NATTS_MASK) / 8 + 1) * 8;
					values[14] = CStringGetTextDatum(
						bits_to_text(tuphdr->t_bits, bits_len));
				}
				else
				{
					nulls[14] = true;
				}

				if (tuphdr->t_infomask & HEAP_HASOID)
				{
					values[15] = HeapTupleHeaderGetOid(tuphdr);
				}
				else
				{
					nulls[15] = true;
				}
			}
			else
			{
				nulls[14] = true;
				nulls[15] = true;
			}
		}
		else
		{
			/*
			 * The line pointer is not used, or it's invalid. Set the rest of
			 * the fields to NULL
			 */
			int i;

			for (i = 4; i <= 16; i++)
				nulls[i] = true;
		}

		if (log_level > 0)
		{
			elog(LOG, "heap_page_items_with_gts_log: null[0~16] = "
				"%d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d \n",
				 nulls[0], nulls[1], nulls[2], nulls[3],
				 nulls[4], nulls[5], nulls[6], nulls[7],
				 nulls[8], nulls[9], nulls[10], nulls[11],
				 nulls[12], nulls[13], nulls[14], nulls[15],
				 nulls[16]);

			if (only_id)
			{
				elog(LOG, "heap_page_items_with_gts_log: "
					"lp=%d, lp_off=%d, lp_flags=%d, lp_len=%d \n",
					 DatumGetUInt16(values[0]),
					 DatumGetUInt16(values[1]),
					 DatumGetUInt16(values[2]),
					 DatumGetUInt16(values[3]));
			}
			else
			{
				elog(LOG, "heap_page_items_with_gts_log: "
						  "lp=%d, lp_off=%d, lp_flags=%d, lp_len=%d "
						  "t_xmin=%u t_xmax=%u t_xmin_gts=%ld t_xmax_gts=%ld "
						  "t_field3=%u t_infomask2=%u t_infomask=%u "
						  "t_share=%d t_hoff=%d t_oid=%u "
						  "\n",
					 DatumGetUInt16(values[0]),
					 DatumGetUInt16(values[1]),
					 DatumGetUInt16(values[2]),
					 DatumGetUInt16(values[3]),
					 DatumGetUInt32(values[4]),
					 DatumGetUInt32(values[5]),
					 DatumGetInt64(values[6]),
					 DatumGetInt64(values[7]),
					 DatumGetUInt32(values[8]),
					 /* ignore tid */
					 DatumGetUInt32(values[10]),
					 DatumGetUInt32(values[11]),
					 DatumGetInt32(values[12]),
					 DatumGetUInt8(values[13]),
					 /* ignore text */
					 (Oid)values[15]
					 /*
					  * ignore oid
					  * ignore byte
					  */
				);
			}
		}

		/* Build and return the result tuple. */
		resultTuple = heap_form_tuple(inter_call_data->tupd, values, nulls);
		result = HeapTupleGetDatum(resultTuple);

		inter_call_data->offset++;

		SRF_RETURN_NEXT(fctx, result);
	}
	else
	{
		SRF_RETURN_DONE(fctx);
	}
}
