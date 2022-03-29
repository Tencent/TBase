/*-------------------------------------------------------------------------
 *
 * heaptuple.c
 *      This file contains heap tuple accessor and mutator routines, as well
 *      as various tuple utilities.
 *
 * Some notes about varlenas and this code:
 *
 * Before Postgres 8.3 varlenas always had a 4-byte length header, and
 * therefore always needed 4-byte alignment (at least).  This wasted space
 * for short varlenas, for example CHAR(1) took 5 bytes and could need up to
 * 3 additional padding bytes for alignment.
 *
 * Now, a short varlena (up to 126 data bytes) is reduced to a 1-byte header
 * and we don't align it.  To hide this from datatype-specific functions that
 * don't want to deal with it, such a datum is considered "toasted" and will
 * be expanded back to the normal 4-byte-header format by pg_detoast_datum.
 * (In performance-critical code paths we can use pg_detoast_datum_packed
 * and the appropriate access macros to avoid that overhead.)  Note that this
 * conversion is performed directly in heap_form_tuple, without invoking
 * tuptoaster.c.
 *
 * This change will break any code that assumes it needn't detoast values
 * that have been put into a tuple but never sent to disk.  Hopefully there
 * are few such places.
 *
 * Varlenas still have alignment 'i' (or 'd') in pg_type/pg_attribute, since
 * that's the normal requirement for the untoasted format.  But we ignore that
 * for the 1-byte-header format.  This means that the actual start position
 * of a varlena datum may vary depending on which format it has.  To determine
 * what is stored, we have to require that alignment padding bytes be zero.
 * (Postgres actually has always zeroed them, but now it's required!)  Since
 * the first byte of a 1-byte-header varlena can never be zero, we can examine
 * the first byte after the previous datum to tell if it's a pad byte or the
 * start of a 1-byte-header varlena.
 *
 * Note that while formerly we could rely on the first varlena column of a
 * system catalog to be at the offset suggested by the C struct for the
 * catalog, this is now risky: it's only safe if the preceding field is
 * word-aligned, so that there will never be any padding.
 *
 * We don't pack varlenas whose attstorage is 'p', since the data type
 * isn't expecting to have to detoast values.  This is used in particular
 * by oidvector and int2vector, which are used in the system catalogs
 * and we'd like to still refer to them via C struct offsets.
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *      src/backend/access/common/heaptuple.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#ifdef PGXC
#include "funcapi.h"
#endif
#include "access/sysattr.h"
#include "access/tuptoaster.h"
#include "executor/tuptable.h"
#ifdef XCP
#include "lib/stringinfo.h"
#include "utils/memutils.h"
#endif
#ifdef _MLS_
#include "access/tupdesc.h"
#include "access/tupdesc_details.h"
#include "utils/relcrypt.h"
#endif
#include "utils/expandeddatum.h"
#include "pgxc/shardmap.h"
#ifdef __TBASE__
#include "utils/typcache.h"
#include "pgxc/execRemote.h"
#include "catalog/pg_type.h"
#include "mb/pg_wchar.h"
#endif

/* Does att's datatype allow packing into the 1-byte-header varlena format? */
#define ATT_IS_PACKABLE(att) \
    ((att)->attlen == -1 && (att)->attstorage != 'p')
/* Use this if it's already known varlena */
#define VARLENA_ATT_IS_PACKABLE(att) \
    ((att)->attstorage != 'p')


/* ----------------------------------------------------------------
 *                        misc support routines
 * ----------------------------------------------------------------
 */

#ifdef _MLS_
static Datum getmissingattr(TupleDesc tupleDesc, int attnum, bool *isnull);
static void slot_getmissingattrs(TupleTableSlot *slot, int startAttNum, int lastAttNum);
static void expand_tuple(HeapTuple *targetHeapTuple, MinimalTuple *targetMinimalTuple, HeapTuple sourceTuple, TupleDesc tupleDesc);

/*
 * Per-attribute helper for heap_fill_tuple and other routines building tuples.
 *
 * Fill in either a data value or a bit in the null bitmask
 */
static inline void
fill_val(Form_pg_attribute att,
         bits8 **bit,
         int *bitmask,
         char **dataP,
         uint16 *infomask,
         Datum datum,
         bool isnull)
{// #lizard forgives
    Size        data_length;
    char       *data = *dataP;

    /*
     * If we're building a null bitmap, set the appropriate bit for the
     * current column value here.
     */
    if (bit != NULL)
    {
        if (*bitmask != HIGHBIT)
            *bitmask <<= 1;
        else
        {
            *bit += 1;
            **bit = 0x0;
            *bitmask = 1;
        }

        if (isnull)
        {
            *infomask |= HEAP_HASNULL;
            return;
        }

        **bit |= *bitmask;
    }

    /*
     * XXX we use the att_align macros on the pointer value itself, not on an
     * offset.  This is a bit of a hack.
     */
    if (att->attbyval)
    {
        /* pass-by-value */
        data = (char *) att_align_nominal(data, att->attalign);
        store_att_byval(data, datum, att->attlen);
        data_length = att->attlen;
    }
    else if (att->attlen == -1)
    {
        /* varlena */
        Pointer        val = DatumGetPointer(datum);

        *infomask |= HEAP_HASVARWIDTH;
        if (VARATT_IS_EXTERNAL(val))
        {
            if (VARATT_IS_EXTERNAL_EXPANDED(val))
            {
                /*
                 * we want to flatten the expanded value so that the
                 * constructed tuple doesn't depend on it
                 */
                ExpandedObjectHeader *eoh = DatumGetEOHP(datum);

                data = (char *) att_align_nominal(data,
                                                  att->attalign);
                data_length = EOH_get_flat_size(eoh);
                EOH_flatten_into(eoh, data, data_length);
            }
            else
            {
                *infomask |= HEAP_HASEXTERNAL;
                /* no alignment, since it's short by definition */
                data_length = VARSIZE_EXTERNAL(val);
                memcpy(data, val, data_length);
            }
        }
        else if (VARATT_IS_SHORT(val))
        {
            /* no alignment for short varlenas */
            data_length = VARSIZE_SHORT(val);
            memcpy(data, val, data_length);
        }
        else if (VARLENA_ATT_IS_PACKABLE(att) &&
                 VARATT_CAN_MAKE_SHORT(val))
        {
            /* convert to short varlena -- no alignment */
            data_length = VARATT_CONVERTED_SHORT_SIZE(val);
            SET_VARSIZE_SHORT(data, data_length);
            memcpy(data + 1, VARDATA(val), data_length - 1);
        }
        else
        {
            /* full 4-byte header varlena */
            data = (char *) att_align_nominal(data,
                                              att->attalign);
            data_length = VARSIZE(val);
            memcpy(data, val, data_length);
        }
    }
    else if (att->attlen == -2)
    {
        /* cstring ... never needs alignment */
        *infomask |= HEAP_HASVARWIDTH;
        Assert(att->attalign == 'c');
        data_length = strlen(DatumGetCString(datum)) + 1;
        memcpy(data, DatumGetPointer(datum), data_length);
    }
    else
    {
        /* fixed-length pass-by-reference */
        data = (char *) att_align_nominal(data, att->attalign);
        Assert(att->attlen > 0);
        data_length = att->attlen;
        memcpy(data, DatumGetPointer(datum), data_length);
    }

    data += data_length;
    *dataP = data;
}

#endif

/*
 * heap_compute_data_size
 *        Determine size of the data area of a tuple to be constructed
 */
Size
heap_compute_data_size(TupleDesc tupleDesc,
                       Datum *values,
                       bool *isnull)
{// #lizard forgives
    Size        data_length = 0;
    int            i;
    int            numberOfAttributes = tupleDesc->natts;
    Form_pg_attribute *att = tupleDesc->attrs;

#ifdef _MLS_
    if (TRANSP_CRYPT_ATTRS_EXT_IS_ENABLED(tupleDesc))
    {
        att = tupleDesc->attrs_ext;
    }
#endif

    for (i = 0; i < numberOfAttributes; i++)
    {
        Datum        val;
        Form_pg_attribute atti;

        if (isnull[i])
            continue;

        val = values[i];
        atti = att[i];

        if (ATT_IS_PACKABLE(atti) &&
            VARATT_CAN_MAKE_SHORT(DatumGetPointer(val)))
        {
            /*
             * we're anticipating converting to a short varlena header, so
             * adjust length and don't count any alignment
             */
            data_length += VARATT_CONVERTED_SHORT_SIZE(DatumGetPointer(val));
        }
        else if (atti->attlen == -1 &&
                 VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(val)))
        {
            /*
             * we want to flatten the expanded value so that the constructed
             * tuple doesn't depend on it
             */
            data_length = att_align_nominal(data_length, atti->attalign);
            data_length += EOH_get_flat_size(DatumGetEOHP(val));
        }
        else
        {
            data_length = att_align_datum(data_length, atti->attalign,
                                          atti->attlen, val);
            data_length = att_addlength_datum(data_length, atti->attlen,
                                              val);
        }
    }

    return data_length;
}

/*
 * heap_fill_tuple
 *        Load data portion of a tuple from values/isnull arrays
 *
 * We also fill the null bitmap (if any) and set the infomask bits
 * that reflect the tuple's data contents.
 *
 * NOTE: it is now REQUIRED that the caller have pre-zeroed the data area.
 */
void
heap_fill_tuple(TupleDesc tupleDesc,
                Datum *values, bool *isnull,
                char *data, Size data_size,
                uint16 *infomask, bits8 *bit)
{// #lizard forgives
    bits8       *bitP;
    int            bitmask;
    int            i;
    int            numberOfAttributes = tupleDesc->natts;
    Form_pg_attribute *att = tupleDesc->attrs;

#ifdef USE_ASSERT_CHECKING
    char       *start = data;
#endif

#ifdef _MLS_
    if (TRANSP_CRYPT_ATTRS_EXT_IS_ENABLED(tupleDesc))
    {
        att = tupleDesc->attrs_ext;
    }
#endif

    if (bit != NULL)
    {
        bitP = &bit[-1];
        bitmask = HIGHBIT;
    }
    else
    {
        /* just to keep compiler quiet */
        bitP = NULL;
        bitmask = 0;
    }

    *infomask &= ~(HEAP_HASNULL | HEAP_HASVARWIDTH | HEAP_HASEXTERNAL);

    for (i = 0; i < numberOfAttributes; i++)
    {
#ifdef _MLS_        
        Form_pg_attribute attr = att[i];

        fill_val(attr,
                 bitP ? &bitP : NULL,
                 &bitmask,
                 &data,
                 infomask,
                 values ? values[i] : PointerGetDatum(NULL),
                 isnull ? isnull[i] : true);
#endif
    }

    Assert((data - start) == data_size);
}


/* ----------------------------------------------------------------
 *                        heap tuple interface
 * ----------------------------------------------------------------
 */

/* ----------------
 *        heap_attisnull    - returns TRUE iff tuple attribute is not present
 * ----------------
 */
bool
heap_attisnull(HeapTuple tup, int attnum
#ifdef _MLS_
, TupleDesc tupleDesc
#endif
)
{// #lizard forgives
#ifdef _MLS_    
    /*
     * We allow a NULL tupledesc for relations not expected to have missing
     * values, such as catalog relations and indexes.
     */
    Assert(!tupleDesc || attnum <= tupleDesc->natts);
    if (attnum > (int) HeapTupleHeaderGetNatts(tup->t_data))
    {
        if (tupleDesc && TupleDescAttr(tupleDesc, attnum - 1)->atthasmissing)
            return false;
        else
            return true;
    }
#endif
    
    if (attnum > 0)
    {
        if (HeapTupleNoNulls(tup))
            return false;
        return att_isnull(attnum - 1, tup->t_data->t_bits);
    }

    switch (attnum)
    {
        case TableOidAttributeNumber:
        case SelfItemPointerAttributeNumber:
        case ObjectIdAttributeNumber:
        case MinTransactionIdAttributeNumber:
        case MinCommandIdAttributeNumber:
        case MaxTransactionIdAttributeNumber:
        case MaxCommandIdAttributeNumber:
#ifdef PGXC
        case XC_NodeIdAttributeNumber:
#endif
            /* these are never null */
            break;

        default:
            elog(ERROR, "invalid attnum: %d", attnum);
    }

    return false;
}

/* ----------------
 *        nocachegetattr
 *
 *        This only gets called from fastgetattr() macro, in cases where
 *        we can't use a cacheoffset and the value is not null.
 *
 *        This caches attribute offsets in the attribute descriptor.
 *
 *        An alternative way to speed things up would be to cache offsets
 *        with the tuple, but that seems more difficult unless you take
 *        the storage hit of actually putting those offsets into the
 *        tuple you send to disk.  Yuck.
 *
 *        This scheme will be slightly slower than that, but should
 *        perform well for queries which hit large #'s of tuples.  After
 *        you cache the offsets once, examining all the other tuples using
 *        the same attribute descriptor will go much quicker. -cim 5/4/91
 *
 *        NOTE: if you need to change this code, see also heap_deform_tuple.
 *        Also see nocache_index_getattr, which is the same code for index
 *        tuples.
 * ----------------
 */
Datum
nocachegetattr(HeapTuple tuple,
               int attnum,
               TupleDesc tupleDesc)
{// #lizard forgives
    HeapTupleHeader tup = tuple->t_data;
    Form_pg_attribute *att = tupleDesc->attrs;
    char       *tp;                /* ptr to data part of tuple */
    bits8       *bp = tup->t_bits;    /* ptr to null bitmap in tuple */
    bool        slow = false;    /* do we have to walk attrs? */
    int            off;            /* current offset within data */
#ifdef _MLS_
    if (TRANSP_CRYPT_ATTRS_EXT_IS_ENABLED(tupleDesc))
    {
        att = tupleDesc->attrs_ext;
    }
#endif
    /* ----------------
     *     Three cases:
     *
     *     1: No nulls and no variable-width attributes.
     *     2: Has a null or a var-width AFTER att.
     *     3: Has nulls or var-widths BEFORE att.
     * ----------------
     */

    attnum--;

    if (!HeapTupleNoNulls(tuple))
    {
        /*
         * there's a null somewhere in the tuple
         *
         * check to see if any preceding bits are null...
         */
        int            byte = attnum >> 3;
        int            finalbit = attnum & 0x07;

        /* check for nulls "before" final bit of last byte */
        if ((~bp[byte]) & ((1 << finalbit) - 1))
            slow = true;
        else
        {
            /* check for nulls in any "earlier" bytes */
            int            i;

            for (i = 0; i < byte; i++)
            {
                if (bp[i] != 0xFF)
                {
                    slow = true;
                    break;
                }
            }
        }
    }

    tp = (char *) tup + tup->t_hoff;

    if (!slow)
    {
        /*
         * If we get here, there are no nulls up to and including the target
         * attribute.  If we have a cached offset, we can use it.
         */
        if (att[attnum]->attcacheoff >= 0)
        {
            return fetchatt(att[attnum],
                            tp + att[attnum]->attcacheoff);
        }

        /*
         * Otherwise, check for non-fixed-length attrs up to and including
         * target.  If there aren't any, it's safe to cheaply initialize the
         * cached offsets for these attrs.
         */
        if (HeapTupleHasVarWidth(tuple))
        {
            int            j;

            for (j = 0; j <= attnum; j++)
            {
                if (att[j]->attlen <= 0)
                {
                    slow = true;
                    break;
                }
            }
        }
    }

    if (!slow)
    {
        int            natts = tupleDesc->natts;
        int            j = 1;

        /*
         * If we get here, we have a tuple with no nulls or var-widths up to
         * and including the target attribute, so we can use the cached offset
         * ... only we don't have it yet, or we'd not have got here.  Since
         * it's cheap to compute offsets for fixed-width columns, we take the
         * opportunity to initialize the cached offsets for *all* the leading
         * fixed-width columns, in hope of avoiding future visits to this
         * routine.
         */
        att[0]->attcacheoff = 0;

        /* we might have set some offsets in the slow path previously */
        while (j < natts && att[j]->attcacheoff > 0)
            j++;

        off = att[j - 1]->attcacheoff + att[j - 1]->attlen;

        for (; j < natts; j++)
        {
            if (att[j]->attlen <= 0)
                break;

            off = att_align_nominal(off, att[j]->attalign);

            att[j]->attcacheoff = off;

            off += att[j]->attlen;
        }

        Assert(j > attnum);

        off = att[attnum]->attcacheoff;
    }
    else
    {
        bool        usecache = true;
        int            i;

        /*
         * Now we know that we have to walk the tuple CAREFULLY.  But we still
         * might be able to cache some offsets for next time.
         *
         * Note - This loop is a little tricky.  For each non-null attribute,
         * we have to first account for alignment padding before the attr,
         * then advance over the attr based on its length.  Nulls have no
         * storage and no alignment padding either.  We can use/set
         * attcacheoff until we reach either a null or a var-width attribute.
         */
        off = 0;
        for (i = 0;; i++)        /* loop exit is at "break" */
        {
            if (HeapTupleHasNulls(tuple) && att_isnull(i, bp))
            {
                usecache = false;
                continue;        /* this cannot be the target att */
            }

            /* If we know the next offset, we can skip the rest */
            if (usecache && att[i]->attcacheoff >= 0)
                off = att[i]->attcacheoff;
            else if (att[i]->attlen == -1)
            {
                /*
                 * We can only cache the offset for a varlena attribute if the
                 * offset is already suitably aligned, so that there would be
                 * no pad bytes in any case: then the offset will be valid for
                 * either an aligned or unaligned value.
                 */
                if (usecache &&
                    off == att_align_nominal(off, att[i]->attalign))
                    att[i]->attcacheoff = off;
                else
                {
                    off = att_align_pointer(off, att[i]->attalign, -1,
                                            tp + off);
                    usecache = false;
                }
            }
            else
            {
                /* not varlena, so safe to use att_align_nominal */
                off = att_align_nominal(off, att[i]->attalign);

                if (usecache)
                    att[i]->attcacheoff = off;
            }

            if (i == attnum)
                break;

            off = att_addlength_pointer(off, att[i]->attlen, tp + off);

            if (usecache && att[i]->attlen <= 0)
                usecache = false;
        }
    }

    return fetchatt(att[attnum], tp + off);
}

/* ----------------
 *        heap_getsysattr
 *
 *        Fetch the value of a system attribute for a tuple.
 *
 * This is a support routine for the heap_getattr macro.  The macro
 * has already determined that the attnum refers to a system attribute.
 * ----------------
 */
Datum
heap_getsysattr(HeapTuple tup, int attnum, TupleDesc tupleDesc, bool *isnull)
{// #lizard forgives
    Datum        result;

    Assert(tup);

    /* Currently, no sys attribute ever reads as NULL. */
    *isnull = false;

    switch (attnum)
    {
        case SelfItemPointerAttributeNumber:
            /* pass-by-reference datatype */
            result = PointerGetDatum(&(tup->t_self));
            break;
        case ObjectIdAttributeNumber:
            result = ObjectIdGetDatum(HeapTupleGetOid(tup));
            break;
        case MinTransactionIdAttributeNumber:
            result = TransactionIdGetDatum(HeapTupleHeaderGetRawXmin(tup->t_data));
            break;
        case MaxTransactionIdAttributeNumber:
            result = TransactionIdGetDatum(HeapTupleHeaderGetRawXmax(tup->t_data));
            break;
        case MinCommandIdAttributeNumber:
        case MaxCommandIdAttributeNumber:

            /*
             * cmin and cmax are now both aliases for the same field, which
             * can in fact also be a combo command id.  XXX perhaps we should
             * return the "real" cmin or cmax if possible, that is if we are
             * inside the originating transaction?
             */
            result = CommandIdGetDatum(HeapTupleHeaderGetRawCommandId(tup->t_data));
            break;
        case TableOidAttributeNumber:
            result = ObjectIdGetDatum(tup->t_tableOid);
            break;
#ifdef PGXC
        case XC_NodeIdAttributeNumber:
            result = UInt32GetDatum(tup->t_xc_node_id);
            break;
            
#ifdef _MIGRATE_
        case ShardIdAttributeNumber:
            result = UInt32GetDatum(HeapTupleHeaderGetShardId(tup->t_data));
            break;

        case XmaxGTSIdAttributeNumber:
            result = UInt64GetDatum(HeapTupleHeaderGetXmaxTimestamp(tup->t_data));
            break;

        case XminGTSAttributeNumber:
            result = UInt64GetDatum(HeapTupleHeaderGetXminTimestamp(tup->t_data));
            break;

#endif
#endif
        default:
            elog(ERROR, "invalid attnum: %d", attnum);
            result = 0;            /* keep compiler quiet */
            break;
    }
    return result;
}

/* ----------------
 *        heap_copytuple
 *
 *        returns a copy of an entire tuple
 *
 * The HeapTuple struct, tuple header, and tuple data are all allocated
 * as a single palloc() block.
 * ----------------
 */
HeapTuple
heap_copytuple(HeapTuple tuple)
{
    HeapTuple    newTuple;

    if (!HeapTupleIsValid(tuple) || tuple->t_data == NULL)
        return NULL;

    newTuple = (HeapTuple) palloc(HEAPTUPLESIZE + tuple->t_len);
    newTuple->t_len = tuple->t_len;
    newTuple->t_self = tuple->t_self;
    newTuple->t_tableOid = tuple->t_tableOid;
#ifdef PGXC
    newTuple->t_xc_node_id = tuple->t_xc_node_id;
#endif
    newTuple->t_data = (HeapTupleHeader) ((char *) newTuple + HEAPTUPLESIZE);
    memcpy((char *) newTuple->t_data, (char *) tuple->t_data, tuple->t_len);
    return newTuple;
}

/* ----------------
 *        heap_copytuple_with_tuple
 *
 *        copy a tuple into a caller-supplied HeapTuple management struct
 *
 * Note that after calling this function, the "dest" HeapTuple will not be
 * allocated as a single palloc() block (unlike with heap_copytuple()).
 * ----------------
 */
void
heap_copytuple_with_tuple(HeapTuple src, HeapTuple dest)
{
    if (!HeapTupleIsValid(src) || src->t_data == NULL)
    {
        dest->t_data = NULL;
        return;
    }

    dest->t_len = src->t_len;
    dest->t_self = src->t_self;
    dest->t_tableOid = src->t_tableOid;
#ifdef PGXC
    dest->t_xc_node_id = src->t_xc_node_id;
#endif
    dest->t_data = (HeapTupleHeader) palloc(src->t_len);
    memcpy((char *) dest->t_data, (char *) src->t_data, src->t_len);
}

/* ----------------
 *        heap_copy_tuple_as_datum
 *
 *        copy a tuple as a composite-type Datum
 * ----------------
 */
Datum
heap_copy_tuple_as_datum(HeapTuple tuple, TupleDesc tupleDesc)
{
    HeapTupleHeader td;

    /*
     * If the tuple contains any external TOAST pointers, we have to inline
     * those fields to meet the conventions for composite-type Datums.
     */
    if (HeapTupleHasExternal(tuple))
        return toast_flatten_tuple_to_datum(tuple->t_data,
                                            tuple->t_len,
                                            tupleDesc);

    /*
     * Fast path for easy case: just make a palloc'd copy and insert the
     * correct composite-Datum header fields (since those may not be set if
     * the given tuple came from disk, rather than from heap_form_tuple).
     */
    td = (HeapTupleHeader) palloc(tuple->t_len);
    memcpy((char *) td, (char *) tuple->t_data, tuple->t_len);

    HeapTupleHeaderSetDatumLength(td, tuple->t_len);
    HeapTupleHeaderSetTypeId(td, tupleDesc->tdtypeid);
    HeapTupleHeaderSetTypMod(td, tupleDesc->tdtypmod);

    return PointerGetDatum(td);
}

/*
 * heap_form_tuple
 *        construct a tuple from the given values[] and isnull[] arrays,
 *        which are of the length indicated by tupleDescriptor->natts
 *
 * The result is allocated in the current memory context.
 */
HeapTuple
heap_form_tuple_shard(TupleDesc tupleDescriptor,
                Datum *values,
                bool *isnull
#ifdef _SHARDING_
                , SetShardFlag sflag,
                AttrNumber diskey,
                AttrNumber secdiskey,
                Oid relid,
                ShardID    sid
#endif
                )
{
    HeapTuple    tuple;            /* return tuple */
    HeapTupleHeader td;            /* tuple data */
    Size        len,
                data_len;
    int            hoff;
    bool        hasnull = false;
    int            numberOfAttributes = tupleDescriptor->natts;
    int            i;

    if (numberOfAttributes > MaxTupleAttributeNumber)
        ereport(ERROR,
                (errcode(ERRCODE_TOO_MANY_COLUMNS),
                 errmsg("number of columns (%d) exceeds limit (%d)",
                        numberOfAttributes, MaxTupleAttributeNumber)));

    /*
     * Check for nulls
     */
    for (i = 0; i < numberOfAttributes; i++)
    {
        if (isnull[i])
        {
            hasnull = true;
            break;
        }
    }

    /*
     * Determine total space needed
     */
    len = offsetof(HeapTupleHeaderData, t_bits);

    if (hasnull)
        len += BITMAPLEN(numberOfAttributes);

    if (tupleDescriptor->tdhasoid)
        len += sizeof(Oid);

    hoff = len = MAXALIGN(len); /* align user data safely */

    data_len = heap_compute_data_size(tupleDescriptor, values, isnull);

    len += data_len;

    /*
     * Allocate and zero the space needed.  Note that the tuple body and
     * HeapTupleData management structure are allocated in one chunk.
     */
    tuple = (HeapTuple) palloc0(HEAPTUPLESIZE + len);
    tuple->t_data = td = (HeapTupleHeader) ((char *) tuple + HEAPTUPLESIZE);

    /*
     * And fill in the information.  Note we fill the Datum fields even though
     * this tuple may never become a Datum.  This lets HeapTupleHeaderGetDatum
     * identify the tuple type if needed.
     */
    tuple->t_len = len;
    ItemPointerSetInvalid(&(tuple->t_self));
    tuple->t_tableOid = InvalidOid;
#ifdef PGXC
    tuple->t_xc_node_id = 0;
#endif

    HeapTupleHeaderSetDatumLength(td, len);
    HeapTupleHeaderSetTypeId(td, tupleDescriptor->tdtypeid);
    HeapTupleHeaderSetTypMod(td, tupleDescriptor->tdtypmod);
    /* We also make sure that t_ctid is invalid unless explicitly set */
    ItemPointerSetInvalid(&(td->t_ctid));

    HeapTupleHeaderSetNatts(td, numberOfAttributes);
    td->t_hoff = hoff;

    if (tupleDescriptor->tdhasoid)    /* else leave infomask = 0 */
        td->t_infomask = HEAP_HASOID;

    heap_fill_tuple(tupleDescriptor,
                    values,
                    isnull,
                    (char *) td + hoff,
                    data_len,
                    &td->t_infomask,
                    (hasnull ? td->t_bits : NULL));
#ifdef _SHARDING_
    switch(sflag)
    {
        case SetFlag_PlainShard:
            {
                int   shardId;
                Datum value;
                bool  isdisnull;
                Oid   typeOfDistCol;
                Datum secvalue;
                Oid   sectypeOfDistCol;
                bool  secisnull;
                
                if(diskey < 1 || diskey > tupleDescriptor->natts)
                {
                    elog(ERROR, "AttrNum[%d] of distribute key is invalid, ",diskey);
                }

                if(secdiskey  > tupleDescriptor->natts)
                {
                    elog(ERROR, "AttrNum[%d] of second distribute key is invalid, ", secdiskey);
                }        

                /* process sharding maping */        
                typeOfDistCol = tupleDescriptor->attrs[diskey - 1]->atttypid;
                value           = values[diskey - 1];
                isdisnull      = isnull[diskey - 1];

                /* secondary distribute key */
                if (secdiskey != InvalidAttrNumber)
                {
                    sectypeOfDistCol = tupleDescriptor->attrs[secdiskey - 1]->atttypid;
                    secvalue          = values[secdiskey - 1];
                    secisnull         = isnull[secdiskey - 1];
                }
                else
                {
                    sectypeOfDistCol = InvalidOid;    
                    secvalue         = 0;
                    secisnull        = true;
                }
        
                shardId = EvaluateShardId(typeOfDistCol, isdisnull, value, 
                                          sectypeOfDistCol, secisnull, secvalue, relid);
                HeapTupleHeaderSetShardId(td, shardId);    
            }
            break;
        case SetFlag_ToastShard:
            HeapTupleHeaderSetShardId(td, sid);    
            break;
        case SetFlag_NoShard:
        default:
            HeapTupleHeaderSetShardId(td, InvalidShardID);
            break;
    }
#endif
    return tuple;
}

/*
 * heap_modify_tuple
 *        form a new tuple from an old tuple and a set of replacement values.
 *
 * The replValues, replIsnull, and doReplace arrays must be of the length
 * indicated by tupleDesc->natts.  The new tuple is constructed using the data
 * from replValues/replIsnull at columns where doReplace is true, and using
 * the data from the old tuple at columns where doReplace is false.
 *
 * The result is allocated in the current memory context.
 */
HeapTuple
heap_modify_tuple(HeapTuple tuple,
                  TupleDesc tupleDesc,
                  Datum *replValues,
                  bool *replIsnull,
                  bool *doReplace)
{
    int            numberOfAttributes = tupleDesc->natts;
    int            attoff;
    Datum       *values;
    bool       *isnull;
    HeapTuple    newTuple;

    /*
     * allocate and fill values and isnull arrays from either the tuple or the
     * repl information, as appropriate.
     *
     * NOTE: it's debatable whether to use heap_deform_tuple() here or just
     * heap_getattr() only the non-replaced columns.  The latter could win if
     * there are many replaced columns and few non-replaced ones. However,
     * heap_deform_tuple costs only O(N) while the heap_getattr way would cost
     * O(N^2) if there are many non-replaced columns, so it seems better to
     * err on the side of linear cost.
     */
    values = (Datum *) palloc(numberOfAttributes * sizeof(Datum));
    isnull = (bool *) palloc(numberOfAttributes * sizeof(bool));

    heap_deform_tuple(tuple, tupleDesc, values, isnull);

    for (attoff = 0; attoff < numberOfAttributes; attoff++)
    {
        if (doReplace[attoff])
        {
            values[attoff] = replValues[attoff];
            isnull[attoff] = replIsnull[attoff];
        }
    }

    /*
     * create a new tuple from the values and isnull arrays
     */
    newTuple = heap_form_tuple(tupleDesc, values, isnull);

    pfree(values);
    pfree(isnull);

    /*
     * copy the identification info of the old tuple: t_ctid, t_self, and OID
     * (if any)
     */
    newTuple->t_data->t_ctid = tuple->t_data->t_ctid;
    newTuple->t_self = tuple->t_self;
    newTuple->t_tableOid = tuple->t_tableOid;
#ifdef PGXC
    newTuple->t_xc_node_id = tuple->t_xc_node_id;
#endif
    if (tupleDesc->tdhasoid)
        HeapTupleSetOid(newTuple, HeapTupleGetOid(tuple));

    return newTuple;
}

/*
 * heap_modify_tuple_by_cols
 *        form a new tuple from an old tuple and a set of replacement values.
 *
 * This is like heap_modify_tuple, except that instead of specifying which
 * column(s) to replace by a boolean map, an array of target column numbers
 * is used.  This is often more convenient when a fixed number of columns
 * are to be replaced.  The replCols, replValues, and replIsnull arrays must
 * be of length nCols.  Target column numbers are indexed from 1.
 *
 * The result is allocated in the current memory context.
 */
HeapTuple
heap_modify_tuple_by_cols(HeapTuple tuple,
                          TupleDesc tupleDesc,
                          int nCols,
                          int *replCols,
                          Datum *replValues,
                          bool *replIsnull)
{
    int            numberOfAttributes = tupleDesc->natts;
    Datum       *values;
    bool       *isnull;
    HeapTuple    newTuple;
    int            i;

    /*
     * allocate and fill values and isnull arrays from the tuple, then replace
     * selected columns from the input arrays.
     */
    values = (Datum *) palloc(numberOfAttributes * sizeof(Datum));
    isnull = (bool *) palloc(numberOfAttributes * sizeof(bool));

    heap_deform_tuple(tuple, tupleDesc, values, isnull);

    for (i = 0; i < nCols; i++)
    {
        int            attnum = replCols[i];

        if (attnum <= 0 || attnum > numberOfAttributes)
            elog(ERROR, "invalid column number %d", attnum);
        values[attnum - 1] = replValues[i];
        isnull[attnum - 1] = replIsnull[i];
    }

    /*
     * create a new tuple from the values and isnull arrays
     */
    newTuple = heap_form_tuple(tupleDesc, values, isnull);

    pfree(values);
    pfree(isnull);

    /*
     * copy the identification info of the old tuple: t_ctid, t_self, and OID
     * (if any)
     */
    newTuple->t_data->t_ctid = tuple->t_data->t_ctid;
    newTuple->t_self = tuple->t_self;
    newTuple->t_tableOid = tuple->t_tableOid;
    if (tupleDesc->tdhasoid)
        HeapTupleSetOid(newTuple, HeapTupleGetOid(tuple));

    return newTuple;
}

/*
 * heap_deform_tuple
 *        Given a tuple, extract data into values/isnull arrays; this is
 *        the inverse of heap_form_tuple.
 *
 *        Storage for the values/isnull arrays is provided by the caller;
 *        it should be sized according to tupleDesc->natts not
 *        HeapTupleHeaderGetNatts(tuple->t_data).
 *
 *        Note that for pass-by-reference datatypes, the pointer placed
 *        in the Datum will point into the given tuple.
 *
 *        When all or most of a tuple's fields need to be extracted,
 *        this routine will be significantly quicker than a loop around
 *        heap_getattr; the loop will become O(N^2) as soon as any
 *        noncacheable attribute offsets are involved.
 */
void
heap_deform_tuple(HeapTuple tuple, TupleDesc tupleDesc,
                  Datum *values, bool *isnull)
{// #lizard forgives
    HeapTupleHeader tup = tuple->t_data;
    bool        hasnulls = HeapTupleHasNulls(tuple);
    Form_pg_attribute *att = tupleDesc->attrs;
    int            tdesc_natts = tupleDesc->natts;
    int            natts;            /* number of atts to extract */
    int            attnum;
    char       *tp;                /* ptr to tuple data */
    long        off;            /* offset in tuple data */
    bits8       *bp = tup->t_bits;    /* ptr to null bitmap in tuple */
    bool        slow = false;    /* can we use/set attcacheoff? */

    natts = HeapTupleHeaderGetNatts(tup);

#ifdef _MLS_
    if (TRANSP_CRYPT_ATTRS_EXT_IS_ENABLED(tupleDesc))
    {
        att = tupleDesc->attrs_ext;
    }
#endif

    /*
     * In inheritance situations, it is possible that the given tuple actually
     * has more fields than the caller is expecting.  Don't run off the end of
     * the caller's arrays.
     */
    natts = Min(natts, tdesc_natts);

    tp = (char *) tup + tup->t_hoff;

    off = 0;

    for (attnum = 0; attnum < natts; attnum++)
    {
        Form_pg_attribute thisatt = att[attnum];

        if (hasnulls && att_isnull(attnum, bp))
        {
            values[attnum] = (Datum) 0;
            isnull[attnum] = true;
            slow = true;        /* can't use attcacheoff anymore */
            continue;
        }

        isnull[attnum] = false;

        if (!slow && thisatt->attcacheoff >= 0)
            off = thisatt->attcacheoff;
        else if (thisatt->attlen == -1)
        {
            /*
             * We can only cache the offset for a varlena attribute if the
             * offset is already suitably aligned, so that there would be no
             * pad bytes in any case: then the offset will be valid for either
             * an aligned or unaligned value.
             */
            if (!slow &&
                off == att_align_nominal(off, thisatt->attalign))
                thisatt->attcacheoff = off;
            else
            {
                off = att_align_pointer(off, thisatt->attalign, -1,
                                        tp + off);
                slow = true;
            }
        }
        else
        {
            /* not varlena, so safe to use att_align_nominal */
            off = att_align_nominal(off, thisatt->attalign);

            if (!slow)
                thisatt->attcacheoff = off;
        }

        values[attnum] = fetchatt(thisatt, tp + off);

        off = att_addlength_pointer(off, thisatt->attlen, tp + off);

        if (thisatt->attlen <= 0)
            slow = true;        /* can't use attcacheoff anymore */
    }

    /*
     * If tuple doesn't have all the atts indicated by tupleDesc, read the
     * rest as nulls or missing values as appropriate.
     */
    for (; attnum < tdesc_natts; attnum++)
    {
#ifdef _MLS_        
        values[attnum] = getmissingattr(tupleDesc, attnum + 1, &isnull[attnum]);
#endif
    }
}

/*
 * slot_deform_tuple
 *        Given a TupleTableSlot, extract data from the slot's physical tuple
 *        into its Datum/isnull arrays.  Data is extracted up through the
 *        natts'th column (caller must ensure this is a legal column number).
 *
 *        This is essentially an incremental version of heap_deform_tuple:
 *        on each call we extract attributes up to the one needed, without
 *        re-computing information about previously extracted attributes.
 *        slot->tts_nvalid is the number of attributes already extracted.
 */
static void
slot_deform_tuple(TupleTableSlot *slot, int natts)
{// #lizard forgives
    HeapTuple    tuple = slot->tts_tuple;
    TupleDesc    tupleDesc = slot->tts_tupleDescriptor;
    Datum       *values = slot->tts_values;
    bool       *isnull = slot->tts_isnull;
    HeapTupleHeader tup = tuple->t_data;
    bool        hasnulls = HeapTupleHasNulls(tuple);
    Form_pg_attribute *att = tupleDesc->attrs;
    int            attnum;
    char       *tp;                /* ptr to tuple data */
    long        off;            /* offset in tuple data */
    bits8       *bp = tup->t_bits;    /* ptr to null bitmap in tuple */
    bool        slow;            /* can we use/set attcacheoff? */

#ifdef _MLS_
    if (TRANSP_CRYPT_ATTRS_EXT_IS_ENABLED(tupleDesc))
    {
        att = tupleDesc->attrs_ext;
    }
#endif

    /*
     * Check whether the first call for this tuple, and initialize or restore
     * loop state.
     */
    attnum = slot->tts_nvalid;
    if (attnum == 0)
    {
        /* Start from the first attribute */
        off = 0;
        slow = false;
    }
    else
    {
        /* Restore state from previous execution */
        off = slot->tts_off;
        slow = slot->tts_slow;
    }

    tp = (char *) tup + tup->t_hoff;

    for (; attnum < natts; attnum++)
    {
        Form_pg_attribute thisatt = att[attnum];

        if (hasnulls && att_isnull(attnum, bp))
        {
            values[attnum] = (Datum) 0;
            isnull[attnum] = true;
            slow = true;        /* can't use attcacheoff anymore */
            continue;
        }

        isnull[attnum] = false;

        if (!slow && thisatt->attcacheoff >= 0)
            off = thisatt->attcacheoff;
        else if (thisatt->attlen == -1)
        {
            /*
             * We can only cache the offset for a varlena attribute if the
             * offset is already suitably aligned, so that there would be no
             * pad bytes in any case: then the offset will be valid for either
             * an aligned or unaligned value.
             */
            if (!slow &&
                off == att_align_nominal(off, thisatt->attalign))
                thisatt->attcacheoff = off;
            else
            {
                off = att_align_pointer(off, thisatt->attalign, -1,
                                        tp + off);
                slow = true;
            }
        }
        else
        {
            /* not varlena, so safe to use att_align_nominal */
            off = att_align_nominal(off, thisatt->attalign);

            if (!slow)
                thisatt->attcacheoff = off;
        }

        values[attnum] = fetchatt(thisatt, tp + off);

        off = att_addlength_pointer(off, thisatt->attlen, tp + off);

        if (thisatt->attlen <= 0)
            slow = true;        /* can't use attcacheoff anymore */
    }

    /*
     * Save state for next execution
     */
    slot->tts_nvalid = attnum;
    slot->tts_off = off;
    slot->tts_slow = slow;
}

/**
 * get maximum bytes number from column define size, if column is bounded string, return -1
 * then InputFunctionCall -> varchar2_input|varchar_input|varchar2_input|nvarchar2_input
 * avoid to verification the length of string which encoded by client encode
 */
static int
get_typioparam_mod(Oid typioparam, int32 typmod)
{
    switch (typioparam)
    {
        case CHAROID:
        case BPCHAROID:
        case VARCHAROID:
#ifdef _PG_ORCL_
        case VARCHAR2OID:
		case NVARCHAR2OID:
#endif
            return -1;

        default:
            return typmod;
    }
}

/*
 * slot_deform_datarow
 *         Extract data from the DataRow message into Datum/isnull arrays.
 *
 * We always extract all attributes, as specified in tts_tupleDescriptor,
 * because there is no easy way to find random attribute in the DataRow.
 *
 * XXX There's an opportunity for optimization - we might extract only the
 * attributes we already need (up to some attnum), and keep a pointer to
 * the next byte in the DataRow message. On the next call we can either
 * return immediately if the attnum is already extracted, or deform next
 * chunk of the message. Not sure if this is worth the effort, as we're
 * likely to extract all attributes from the message eventually.
 */
static void
slot_deform_datarow(TupleTableSlot *slot)
{// #lizard forgives
    int natts;
    int i;
    int         col_count;
    char       *cur = slot->tts_datarow->msg;
    StringInfo  buffer;
    uint16        n16;
    uint32        n32;
    MemoryContext oldcontext;

    Assert(slot->tts_tupleDescriptor != NULL);
    Assert(slot->tts_datarow != NULL);

    natts = slot->tts_tupleDescriptor->natts;

    /* fastpath: exit if values already extracted */
    if (slot->tts_nvalid == natts)
        return;

    memcpy(&n16, cur, 2);
    cur += 2;
    col_count = ntohs(n16);

    if (col_count != natts)
        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Tuple does not match the descriptor, tuple cols %d, descriptor cols %d", 
				 col_count, natts)));

    if (slot->tts_attinmeta == NULL)
    {
        /*
         * Ensure info about input functions is available as long as slot lives
         */
        oldcontext = MemoryContextSwitchTo(slot->tts_mcxt);
        slot->tts_attinmeta = TupleDescGetAttInMetadata(slot->tts_tupleDescriptor);
        MemoryContextSwitchTo(oldcontext);
    }

    /*
     * Store values to separate context to easily free them when base datarow is
     * freed
     */
    if (slot->tts_drowcxt == NULL)
    {
        slot->tts_drowcxt = AllocSetContextCreate(slot->tts_mcxt,
                                                  "Datarow",
                                                  ALLOCSET_DEFAULT_MINSIZE,
                                                  ALLOCSET_DEFAULT_INITSIZE,
                                                  ALLOCSET_DEFAULT_MAXSIZE);
    }

    buffer = makeStringInfo();
    for (i = 0; i < natts; i++)
    {
        Form_pg_attribute attr = slot->tts_tupleDescriptor->attrs[i];
        int len;

        /* get size */
        memcpy(&n32, cur, 4);
        cur += 4;
        len = ntohl(n32);

        /* get data */
        if (len == -1)
        {
            slot->tts_values[i] = (Datum) 0;
            slot->tts_isnull[i] = true;
        }
#ifdef __TBASE__
        else if (len == -2)
        {
            /* composite type */
            TupleDesc tupDesc;
                
            memcpy(&n32, cur, 4);
            cur += 4;
            len = ntohl(n32);

            appendBinaryStringInfo(buffer, cur, len);

            tupDesc = create_tuple_desc(buffer->data, len);

            assign_record_type_typmod(tupDesc);

            resetStringInfo(buffer);

            cur += len;

            memcpy(&n32, cur, 4);
            cur += 4;
            len = ntohl(n32);

            appendBinaryStringInfo(buffer, cur, len);
            cur += len;

            slot->tts_values[i] = InputFunctionCall(slot->tts_attinmeta->attinfuncs + i,
                                                    buffer->data,
                                                    slot->tts_attinmeta->attioparams[i],
                                                    tupDesc->tdtypmod);
            slot->tts_isnull[i] = false;

            resetStringInfo(buffer);

            if (!attr->attbyval)
            {
                Pointer        val = DatumGetPointer(slot->tts_values[i]);
                Size        data_length;
                void       *data;

                if (attr->attlen == -1)
                {
                    /* varlena */
                    data_length = VARSIZE_ANY(val);
                }
                else if (attr->attlen == -2)
                {
                    /* cstring */
                    data_length = strlen(val) + 1;
                }
                else
                {
                    /* fixed-length pass-by-reference */
                    data_length = attr->attlen;
                }
                data = MemoryContextAlloc(slot->tts_drowcxt, data_length);
                memcpy(data, val, data_length);

                pfree(val);

                slot->tts_values[i] = PointerGetDatum(data);
            }
        }
#endif
        else
        {
            int typmod = slot->tts_attinmeta->atttypmods[i];
            appendBinaryStringInfo(buffer, cur, len);
            cur += len;

            if (GetDatabaseEncoding() != pg_get_client_encoding() &&
                            pg_get_client_encoding() != PG_SQL_ASCII && IS_PGXC_LOCAL_COORDINATOR)
                typmod = get_typioparam_mod(slot->tts_attinmeta->attioparams[i], typmod);

            slot->tts_values[i] = InputFunctionCall(slot->tts_attinmeta->attinfuncs + i,
                                                    buffer->data,
                                                    slot->tts_attinmeta->attioparams[i],
                                                    typmod);
            slot->tts_isnull[i] = false;

            resetStringInfo(buffer);

            /*
             * The input function was executed in caller's memory context,
             * because it may be allocating working memory, and caller may
             * want to clean it up.
             * However returned Datums need to be in the special context, so
             * if attribute is pass-by-reference, copy it.
             */
            if (!attr->attbyval)
            {
                Pointer        val = DatumGetPointer(slot->tts_values[i]);
                Size        data_length;
                void       *data;

                if (attr->attlen == -1)
                {
                    /* varlena */
                    data_length = VARSIZE_ANY(val);
                }
                else if (attr->attlen == -2)
                {
                    /* cstring */
                    data_length = strlen(val) + 1;
                }
                else
                {
                    /* fixed-length pass-by-reference */
                    data_length = attr->attlen;
                }
                data = MemoryContextAlloc(slot->tts_drowcxt, data_length);
                memcpy(data, val, data_length);

                pfree(val);

                slot->tts_values[i] = PointerGetDatum(data);
            }
        }
    }
    pfree(buffer->data);
    pfree(buffer);

    slot->tts_nvalid = natts;
}

/*
 * slot_getattr
 *        This function fetches an attribute of the slot's current tuple.
 *        It is functionally equivalent to heap_getattr, but fetches of
 *        multiple attributes of the same tuple will be optimized better,
 *        because we avoid O(N^2) behavior from multiple calls of
 *        nocachegetattr(), even when attcacheoff isn't usable.
 *
 *        A difference from raw heap_getattr is that attnums beyond the
 *        slot's tupdesc's last attribute will be considered NULL even
 *        when the physical tuple is longer than the tupdesc.
 */
Datum
slot_getattr(TupleTableSlot *slot, int attnum, bool *isnull)
{// #lizard forgives
    HeapTuple    tuple = slot->tts_tuple;
    TupleDesc    tupleDesc = slot->tts_tupleDescriptor;
    HeapTupleHeader tup;

    /*
     * system attributes are handled by heap_getsysattr
     */
    if (attnum <= 0)
    {
        if (tuple == NULL)        /* internal error */
            elog(ERROR, "cannot extract system attribute from virtual tuple");
        if (tuple == &(slot->tts_minhdr))    /* internal error */
            elog(ERROR, "cannot extract system attribute from minimal tuple");
        return heap_getsysattr(tuple, attnum, tupleDesc, isnull);
    }

    /*
     * fast path if desired attribute already cached
     */
    if (attnum <= slot->tts_nvalid)
    {
        *isnull = slot->tts_isnull[attnum - 1];
        return slot->tts_values[attnum - 1];
    }

    /*
     * return NULL if attnum is out of range according to the tupdesc
     */
    if (attnum > tupleDesc->natts)
    {
        *isnull = true;
        return (Datum) 0;
    }

#ifdef PGXC
    /* If it is a data row tuple extract all and return requested */
    if (slot->tts_datarow)
    {
        slot_deform_datarow(slot);
        *isnull = slot->tts_isnull[attnum - 1];
        return slot->tts_values[attnum - 1];
    }
#endif

    /*
     * otherwise we had better have a physical tuple (tts_nvalid should equal
     * natts in all virtual-tuple cases)
     */
    if (tuple == NULL)            /* internal error */
        elog(ERROR, "cannot extract attribute from empty tuple slot");

    /*
     * return NULL if attnum is out of range according to the tuple
     *
     * (We have to check this separately because of various inheritance and
     * table-alteration scenarios: the tuple could be either longer or shorter
     * than the tupdesc.)
     */
    tup = tuple->t_data;
    if (attnum > HeapTupleHeaderGetNatts(tup))
    {
#ifdef _MLS_        
        return getmissingattr(slot->tts_tupleDescriptor, attnum, isnull);
#endif
    }

    /*
     * check if target attribute is null: no point in groveling through tuple
     */
    if (HeapTupleHasNulls(tuple) && att_isnull(attnum - 1, tup->t_bits))
    {
        *isnull = true;
        return (Datum) 0;
    }

    /*
     * If the attribute's column has been dropped, we force a NULL result.
     * This case should not happen in normal use, but it could happen if we
     * are executing a plan cached before the column was dropped.
     */
    if (tupleDesc->attrs[attnum - 1]->attisdropped)
    {
        *isnull = true;
        return (Datum) 0;
    }

    /*
     * Extract the attribute, along with any preceding attributes.
     */
    slot_deform_tuple(slot, attnum);

    /*
     * The result is acquired from tts_values array.
     */
    *isnull = slot->tts_isnull[attnum - 1];
    return slot->tts_values[attnum - 1];
}

/*
 * slot_getallattrs
 *        This function forces all the entries of the slot's Datum/isnull
 *        arrays to be valid.  The caller may then extract data directly
 *        from those arrays instead of using slot_getattr.
 */
void
slot_getallattrs(TupleTableSlot *slot)
{
    int            tdesc_natts = slot->tts_tupleDescriptor->natts;
    int            attnum;
    HeapTuple    tuple;

    /* Quick out if we have 'em all already */
    if (slot->tts_nvalid == tdesc_natts)
        return;

#ifdef PGXC
    /* Handle the DataRow tuple case */
    if (slot->tts_datarow)
    {
        slot_deform_datarow(slot);
        return;
    }
#endif

    /*
     * otherwise we had better have a physical tuple (tts_nvalid should equal
     * natts in all virtual-tuple cases)
     */
    tuple = slot->tts_tuple;
    if (tuple == NULL)            /* internal error */
        elog(ERROR, "cannot extract attribute from empty tuple slot");

    /*
     * load up any slots available from physical tuple
     */
    attnum = HeapTupleHeaderGetNatts(tuple->t_data);
    attnum = Min(attnum, tdesc_natts);

    slot_deform_tuple(slot, attnum);

    /*
     * If tuple doesn't have all the atts indicated by tupleDesc, read the
     * rest as NULLS or missing values.
     */
#ifdef _MLS_    
    if (attnum < tdesc_natts)
    {
        slot_getmissingattrs(slot, attnum, tdesc_natts);
    }
#endif 
    slot->tts_nvalid = tdesc_natts;
}

/*
 * slot_getsomeattrs
 *        This function forces the entries of the slot's Datum/isnull
 *        arrays to be valid at least up through the attnum'th entry.
 */
void
slot_getsomeattrs(TupleTableSlot *slot, int attnum)
{// #lizard forgives
    HeapTuple    tuple;
    int            attno;

    /* Quick out if we have 'em all already */
    if (slot->tts_nvalid >= attnum)
        return;

#ifdef PGXC
    /* Handle the DataRow tuple case */
    if (slot->tts_datarow)
    {
        slot_deform_datarow(slot);
        return;
    }
#endif

    /* Check for caller error */
    if (attnum <= 0 || attnum > slot->tts_tupleDescriptor->natts)
        elog(ERROR, "invalid attribute number %d", attnum);

    /*
     * otherwise we had better have a physical tuple (tts_nvalid should equal
     * natts in all virtual-tuple cases)
     */
    tuple = slot->tts_tuple;
    if (tuple == NULL)            /* internal error */
        elog(ERROR, "cannot extract attribute from empty tuple slot");

    /*
     * load up any slots available from physical tuple
     */
    attno = HeapTupleHeaderGetNatts(tuple->t_data);
    attno = Min(attno, attnum);

    slot_deform_tuple(slot, attno);

    /*
     * If tuple doesn't have all the atts indicated by tupleDesc, read the
     * rest as NULLs or missing values
     */
#ifdef _MLS_     
    if (attno < attnum)
    {
        slot_getmissingattrs(slot, attno, attnum);
    }
#endif    
    slot->tts_nvalid = attnum;
}

/*
 * slot_attisnull
 *        Detect whether an attribute of the slot is null, without
 *        actually fetching it.
 */
bool
slot_attisnull(TupleTableSlot *slot, int attnum)
{// #lizard forgives
    HeapTuple    tuple = slot->tts_tuple;
    TupleDesc    tupleDesc = slot->tts_tupleDescriptor;

    /*
     * system attributes are handled by heap_attisnull
     */
    if (attnum <= 0)
    {
        if (tuple == NULL)        /* internal error */
            elog(ERROR, "cannot extract system attribute from virtual tuple");
        if (tuple == &(slot->tts_minhdr))    /* internal error */
            elog(ERROR, "cannot extract system attribute from minimal tuple");
#ifdef _MLS_    
        return heap_attisnull(tuple, attnum, tupleDesc);
#endif
    }

    /*
     * fast path if desired attribute already cached
     */
    if (attnum <= slot->tts_nvalid)
        return slot->tts_isnull[attnum - 1];

    /*
     * return NULL if attnum is out of range according to the tupdesc
     */
    if (attnum > tupleDesc->natts)
        return true;

#ifdef PGXC
    /* If it is a data row tuple extract all and return requested */
    if (slot->tts_datarow)
    {
        slot_deform_datarow(slot);
        return slot->tts_isnull[attnum - 1];
    }
#endif

    /*
     * otherwise we had better have a physical tuple (tts_nvalid should equal
     * natts in all virtual-tuple cases)
     */
    if (tuple == NULL)            /* internal error */
        elog(ERROR, "cannot extract attribute from empty tuple slot");

    /* and let the tuple tell it */
#ifdef _MLS_    
    return heap_attisnull(tuple, attnum, tupleDesc);
#endif
}

/*
 * heap_freetuple
 */
void
heap_freetuple(HeapTuple htup)
{
    pfree(htup);
}


/*
 * heap_form_minimal_tuple
 *        construct a MinimalTuple from the given values[] and isnull[] arrays,
 *        which are of the length indicated by tupleDescriptor->natts
 *
 * This is exactly like heap_form_tuple() except that the result is a
 * "minimal" tuple lacking a HeapTupleData header as well as room for system
 * columns.
 *
 * The result is allocated in the current memory context.
 */
MinimalTuple
heap_form_minimal_tuple(TupleDesc tupleDescriptor,
                        Datum *values,
                        bool *isnull)
{// #lizard forgives
    MinimalTuple tuple;            /* return tuple */
    Size        len,
                data_len;
    int            hoff;
    bool        hasnull = false;
    int            numberOfAttributes = tupleDescriptor->natts;
    int            i;

    if (numberOfAttributes > MaxTupleAttributeNumber)
        ereport(ERROR,
                (errcode(ERRCODE_TOO_MANY_COLUMNS),
                 errmsg("number of columns (%d) exceeds limit (%d)",
                        numberOfAttributes, MaxTupleAttributeNumber)));

    /*
     * Check for nulls
     */
    for (i = 0; i < numberOfAttributes; i++)
    {
        if (isnull[i])
        {
            hasnull = true;
            break;
        }
    }

    /*
     * Determine total space needed
     */
    len = SizeofMinimalTupleHeader;

    if (hasnull)
        len += BITMAPLEN(numberOfAttributes);

    if (tupleDescriptor->tdhasoid)
        len += sizeof(Oid);

    hoff = len = MAXALIGN(len); /* align user data safely */

    data_len = heap_compute_data_size(tupleDescriptor, values, isnull);

    len += data_len;

    /*
     * Allocate and zero the space needed.
     */
    tuple = (MinimalTuple) palloc0(len);

    /*
     * And fill in the information.
     */
    tuple->t_len = len;
    HeapTupleHeaderSetNatts(tuple, numberOfAttributes);
    tuple->t_hoff = hoff + MINIMAL_TUPLE_OFFSET;

    if (tupleDescriptor->tdhasoid)    /* else leave infomask = 0 */
        tuple->t_infomask = HEAP_HASOID;

    heap_fill_tuple(tupleDescriptor,
                    values,
                    isnull,
                    (char *) tuple + hoff,
                    data_len,
                    &tuple->t_infomask,
                    (hasnull ? tuple->t_bits : NULL));
#ifdef _SHARDING_
    tuple->t_shardid = InvalidShardID;
#endif
    return tuple;
}

/*
 * heap_free_minimal_tuple
 */
void
heap_free_minimal_tuple(MinimalTuple mtup)
{
    pfree(mtup);
}

/*
 * heap_copy_minimal_tuple
 *        copy a MinimalTuple
 *
 * The result is allocated in the current memory context.
 */
MinimalTuple
heap_copy_minimal_tuple(MinimalTuple mtup)
{
    MinimalTuple result;

    result = (MinimalTuple) palloc(mtup->t_len);
    memcpy(result, mtup, mtup->t_len);
    return result;
}

/*
 * heap_tuple_from_minimal_tuple
 *        create a HeapTuple by copying from a MinimalTuple;
 *        system columns are filled with zeroes
 *
 * The result is allocated in the current memory context.
 * The HeapTuple struct, tuple header, and tuple data are all allocated
 * as a single palloc() block.
 */
HeapTuple
heap_tuple_from_minimal_tuple(MinimalTuple mtup)
{
    HeapTuple    result;
    uint32        len = mtup->t_len + MINIMAL_TUPLE_OFFSET;

    result = (HeapTuple) palloc(HEAPTUPLESIZE + len);
    result->t_len = len;
    ItemPointerSetInvalid(&(result->t_self));
    result->t_tableOid = InvalidOid;
#ifdef PGXC
    result->t_xc_node_id = 0;
#endif
    result->t_data = (HeapTupleHeader) ((char *) result + HEAPTUPLESIZE);
    memcpy((char *) result->t_data + MINIMAL_TUPLE_OFFSET, mtup, mtup->t_len);
    memset(result->t_data, 0, offsetof(HeapTupleHeaderData, t_infomask2));
    return result;
}

/*
 * minimal_tuple_from_heap_tuple
 *        create a MinimalTuple by copying from a HeapTuple
 *
 * The result is allocated in the current memory context.
 */
MinimalTuple
minimal_tuple_from_heap_tuple(HeapTuple htup)
{
    MinimalTuple result;
    uint32        len;

    Assert(htup->t_len > MINIMAL_TUPLE_OFFSET);
    len = htup->t_len - MINIMAL_TUPLE_OFFSET;
    result = (MinimalTuple) palloc(len);
    memcpy(result, (char *) htup->t_data + MINIMAL_TUPLE_OFFSET, len);
    result->t_len = len;
    return result;
}


#ifdef _MLS_
/*
 * Return the missing value of an attribute, or NULL if there isn't one.
 */
static Datum
getmissingattr(TupleDesc tupleDesc,
               int attnum, bool *isnull)
{
    Form_pg_attribute att;

    Assert(attnum <= tupleDesc->natts);
    Assert(attnum > 0);

    att = TupleDescAttr(tupleDesc, attnum - 1);

    if (att->atthasmissing && (false == att->attisdropped))
    {
        AttrMissing *attrmiss;

        Assert(tupleDesc->constr);
        Assert(tupleDesc->constr->missing);

        attrmiss = tupleDesc->constr->missing + (attnum - 1);

        if (attrmiss->ammissingPresent)
        {
            *isnull = false;
            return attrmiss->ammissing;
        }
    }

    *isnull = true;
    return PointerGetDatum(NULL);
}

/*
 * Fill in missing values for a TupleTableSlot.
 *
 * This is only exposed because it's needed for JIT compiled tuple
 * deforming. That exception aside, there should be no callers outside of this
 * file.
 */
void
slot_getmissingattrs(TupleTableSlot *slot, int startAttNum, int lastAttNum)
{
    AttrMissing *attrmiss = NULL;
    int            missattnum;

    if (slot->tts_tupleDescriptor->constr)
        attrmiss = slot->tts_tupleDescriptor->constr->missing;

    if (!attrmiss)
    {
        /* no missing values array at all, so just fill everything in as NULL */
        memset(slot->tts_values + startAttNum, 0,
               (lastAttNum - startAttNum) * sizeof(Datum));
        memset(slot->tts_isnull + startAttNum, 1,
               (lastAttNum - startAttNum) * sizeof(bool));
    }
    else
    {
        /* if there is a missing values array we must process them one by one */
        for (missattnum = startAttNum;
             missattnum < lastAttNum;
             missattnum++)
        {
            slot->tts_values[missattnum] = attrmiss[missattnum].ammissing;
            slot->tts_isnull[missattnum] =
                !attrmiss[missattnum].ammissingPresent;
        }
    }
}

/*
 * Expand a tuple which has less attributes than required. For each attribute
 * not present in the sourceTuple, if there is a missing value that will be
 * used. Otherwise the attribute will be set to NULL.
 *
 * The source tuple must have less attributes than the required number.
 *
 * Only one of targetHeapTuple and targetMinimalTuple may be supplied. The
 * other argument must be NULL.
 */
static void
expand_tuple(HeapTuple *targetHeapTuple,
             MinimalTuple *targetMinimalTuple,
             HeapTuple sourceTuple,
             TupleDesc tupleDesc)
{// #lizard forgives
    AttrMissing *attrmiss = NULL;
    int            attnum;
    int            firstmissingnum = 0;
    bool        hasNulls = HeapTupleHasNulls(sourceTuple);
    HeapTupleHeader targetTHeader;
    HeapTupleHeader sourceTHeader = sourceTuple->t_data;
    int            sourceNatts = HeapTupleHeaderGetNatts(sourceTHeader);
    int            natts = tupleDesc->natts;
    int            sourceNullLen;
    int            targetNullLen;
    Size        sourceDataLen = sourceTuple->t_len - sourceTHeader->t_hoff;
    Size        targetDataLen;
    Size        len;
    int            hoff;
    bits8       *nullBits = NULL;
    int            bitMask = 0;
    char       *targetData;
    uint16       *infoMask;

    Assert((targetHeapTuple && !targetMinimalTuple)
           || (!targetHeapTuple && targetMinimalTuple));

    Assert(sourceNatts < natts);

    sourceNullLen = (hasNulls ? BITMAPLEN(sourceNatts) : 0);

    targetDataLen = sourceDataLen;

    if (tupleDesc->constr &&
        tupleDesc->constr->missing)
    {
        /*
         * If there are missing values we want to put them into the tuple.
         * Before that we have to compute the extra length for the values
         * array and the variable length data.
         */
        attrmiss = tupleDesc->constr->missing;

        /*
         * Find the first item in attrmiss for which we don't have a value in
         * the source. We can ignore all the missing entries before that.
         */
        for (firstmissingnum = sourceNatts;
             firstmissingnum < natts;
             firstmissingnum++)
        {
            if (attrmiss[firstmissingnum].ammissingPresent)
                break;
        }

        /*
         * If there are no more missing values everything else must be NULL
         */
        if (firstmissingnum >= natts)
        {
            hasNulls = true;
        }
        else
        {

            /*
             * Now walk the missing attributes. If there is a missing value
             * make space for it. Otherwise, it's going to be NULL.
             */
            for (attnum = firstmissingnum;
                 attnum < natts;
                 attnum++)
            {
                if (attrmiss[attnum].ammissingPresent)
                {
                    Form_pg_attribute att = TupleDescAttr(tupleDesc, attnum);

                    targetDataLen = att_align_datum(targetDataLen,
                                                    att->attalign,
                                                    att->attlen,
                                                    attrmiss[attnum].ammissing);

                    targetDataLen = att_addlength_pointer(targetDataLen,
                                                          att->attlen,
                                                          attrmiss[attnum].ammissing);
                }
                else
                {
                    /* no missing value, so it must be null */
                    hasNulls = true;
                }
            }
        }
    }                            /* end if have missing values */
    else
    {
        /*
         * If there are no missing values at all then NULLS must be allowed,
         * since some of the attributes are known to be absent.
         */
        hasNulls = true;
    }

    len = 0;

    if (hasNulls)
    {
        targetNullLen = BITMAPLEN(natts);
        len += targetNullLen;
    }
    else
        targetNullLen = 0;

    if (tupleDesc->tdhasoid)
        len += sizeof(Oid);

    /*
     * Allocate and zero the space needed.  Note that the tuple body and
     * HeapTupleData management structure are allocated in one chunk.
     */
    if (targetHeapTuple)
    {
        len += offsetof(HeapTupleHeaderData, t_bits);
        hoff = len = MAXALIGN(len); /* align user data safely */
        len += targetDataLen;

        *targetHeapTuple = (HeapTuple) palloc0(HEAPTUPLESIZE + len);
        (*targetHeapTuple)->t_data
            = targetTHeader
            = (HeapTupleHeader) ((char *) *targetHeapTuple + HEAPTUPLESIZE);
        (*targetHeapTuple)->t_len = len;
        (*targetHeapTuple)->t_tableOid = sourceTuple->t_tableOid;
		(*targetHeapTuple)->t_self = sourceTuple->t_self;

        targetTHeader->t_infomask = sourceTHeader->t_infomask;
        targetTHeader->t_hoff = hoff;
        HeapTupleHeaderSetNatts(targetTHeader, natts);
        HeapTupleHeaderSetDatumLength(targetTHeader, len);
        HeapTupleHeaderSetTypeId(targetTHeader, tupleDesc->tdtypeid);
        HeapTupleHeaderSetTypMod(targetTHeader, tupleDesc->tdtypmod);
        /* We also make sure that t_ctid is invalid unless explicitly set */
        ItemPointerSetInvalid(&(targetTHeader->t_ctid));
        if (targetNullLen > 0)
            nullBits = (bits8 *) ((char *) (*targetHeapTuple)->t_data
                                  + offsetof(HeapTupleHeaderData, t_bits));
        targetData = (char *) (*targetHeapTuple)->t_data + hoff;
        infoMask = &(targetTHeader->t_infomask);
    }
    else
    {
        len += SizeofMinimalTupleHeader;
        hoff = len = MAXALIGN(len); /* align user data safely */
        len += targetDataLen;

        *targetMinimalTuple = (MinimalTuple) palloc0(len);
        (*targetMinimalTuple)->t_len = len;
        (*targetMinimalTuple)->t_hoff = hoff + MINIMAL_TUPLE_OFFSET;
        (*targetMinimalTuple)->t_infomask = sourceTHeader->t_infomask;
        /* Same macro works for MinimalTuples */
        HeapTupleHeaderSetNatts(*targetMinimalTuple, natts);
        if (targetNullLen > 0)
            nullBits = (bits8 *) ((char *) *targetMinimalTuple
                                  + offsetof(MinimalTupleData, t_bits));
        targetData = (char *) *targetMinimalTuple + hoff;
        infoMask = &((*targetMinimalTuple)->t_infomask);
    }

    if (targetNullLen > 0)
    {
        if (sourceNullLen > 0)
        {
            /* if bitmap pre-existed copy in - all is set */
            memcpy(nullBits,
                   ((char *) sourceTHeader)
                   + offsetof(HeapTupleHeaderData, t_bits),
                   sourceNullLen);
            nullBits += sourceNullLen - 1;
        }
        else
        {
            sourceNullLen = BITMAPLEN(sourceNatts);
            /* Set NOT NULL for all existing attributes */
            memset(nullBits, 0xff, sourceNullLen);

            nullBits += sourceNullLen - 1;

            if (sourceNatts & 0x07)
            {
                /* build the mask (inverted!) */
                bitMask = 0xff << (sourceNatts & 0x07);
                /* Voila */
                *nullBits = ~bitMask;
            }
        }

        bitMask = (1 << ((sourceNatts - 1) & 0x07));
    }                            /* End if have null bitmap */

    memcpy(targetData,
           ((char *) sourceTuple->t_data) + sourceTHeader->t_hoff,
           sourceDataLen);

    targetData += sourceDataLen;

    /* Now fill in the missing values */
    for (attnum = sourceNatts; attnum < natts; attnum++)
    {

        Form_pg_attribute attr = TupleDescAttr(tupleDesc, attnum);

        if (attrmiss && attrmiss[attnum].ammissingPresent)
        {
            fill_val(attr,
                     nullBits ? &nullBits : NULL,
                     &bitMask,
                     &targetData,
                     infoMask,
                     attrmiss[attnum].ammissing,
                     false);
        }
        else
        {
            fill_val(attr,
                     &nullBits,
                     &bitMask,
                     &targetData,
                     infoMask,
                     (Datum) 0,
                     true);
        }
    }                            /* end loop over missing attributes */
}

/*
 * Fill in the missing values for a minimal HeapTuple
 */
MinimalTuple
minimal_expand_tuple(HeapTuple sourceTuple, TupleDesc tupleDesc)
{
    MinimalTuple minimalTuple;

    expand_tuple(NULL, &minimalTuple, sourceTuple, tupleDesc);
    return minimalTuple;
}

/*
 * Fill in the missing values for an ordinary HeapTuple
 */
HeapTuple
heap_expand_tuple(HeapTuple sourceTuple, TupleDesc tupleDesc)
{
    HeapTuple    heapTuple;

    expand_tuple(&heapTuple, NULL, sourceTuple, tupleDesc);
    return heapTuple;
}

void slot_deform_tuple_extern(void *slot, int natts)
{
    slot_deform_tuple((TupleTableSlot*)slot, natts);
    return;
}

void
heap_tuple_set_shardid(HeapTuple tup, void *tupleslot, AttrNumber diskey, AttrNumber secdiskey,
				             Oid relid)
{
	int   shardId;
	Datum value;
	bool  isdisnull;
	Oid   typeOfDistCol;
	Datum secvalue;
	Oid   sectypeOfDistCol;
	bool  secisnull;
	TupleTableSlot *slot = (TupleTableSlot *)tupleslot;
	
	if(diskey < 1 || diskey > slot->tts_tupleDescriptor->natts)
	{
		elog(ERROR, "AttrNum[%d] of distribute key is invalid, ",diskey);
	}

	if(secdiskey  > slot->tts_tupleDescriptor->natts)
	{
		elog(ERROR, "AttrNum[%d] of second distribute key is invalid, ", secdiskey);
	}		

	/* process sharding maping */		
	typeOfDistCol = slot->tts_tupleDescriptor->attrs[diskey - 1]->atttypid;
	value     	  = slot->tts_values[diskey - 1];
	isdisnull	  = slot->tts_isnull[diskey - 1];

	/* secondary distribute key */
	if (secdiskey != InvalidAttrNumber)
	{
		sectypeOfDistCol = slot->tts_tupleDescriptor->attrs[secdiskey - 1]->atttypid;
		secvalue     	 = slot->tts_values[secdiskey - 1];
		secisnull	     = slot->tts_isnull[secdiskey - 1];
	}
	else
	{
		sectypeOfDistCol = InvalidOid;	
		secvalue         = 0;
		secisnull        = true;
	}

	shardId = EvaluateShardId(typeOfDistCol, isdisnull, value, 
		                      sectypeOfDistCol, secisnull, secvalue, relid);
	HeapTupleHeaderSetShardId(tup->t_data, shardId);	
}
#endif
