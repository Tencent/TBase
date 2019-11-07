/*-------------------------------------------------------------------------
 *
 * tupdesc.c
 *      POSTGRES tuple descriptor support code
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *      src/backend/access/common/tupdesc.c
 *
 * NOTES
 *      some of the executor utility code such as "ExecTypeFromTL" should be
 *      moved here.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "parser/parse_type.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/resowner_private.h"
#include "utils/syscache.h"

#ifdef _MLS_
#include "access/tupdesc_details.h"
#include "utils/datum.h"
#include "utils/mls.h"
#include "utils/relcrypt.h"
#include "utils/datamask.h"
#include "access/relcryptaccess.h"
#endif

#ifdef _MLS_
static bool transparent_crypt_check_transp_crypt_equal(TranspCrypt * tc1, TranspCrypt * tc2, int tc1attnum, int tc2attnum);
static void transparent_crypt_alloc_and_copy(TupleDesc dst, TupleDesc src);
#endif

/*
 * CreateTemplateTupleDesc
 *        This function allocates an empty tuple descriptor structure.
 *
 * Tuple type ID information is initially set for an anonymous record type;
 * caller can overwrite this if needed.
 */
TupleDesc
CreateTemplateTupleDesc(int natts, bool hasoid)
{
    TupleDesc    desc;
    char       *stg;
    int            attroffset;

    /*
     * sanity checks
     */
    AssertArg(natts >= 0);

    /*
     * Allocate enough memory for the tuple descriptor, including the
     * attribute rows, and set up the attribute row pointers.
     *
     * Note: we assume that sizeof(struct tupleDesc) is a multiple of the
     * struct pointer alignment requirement, and hence we don't need to insert
     * alignment padding between the struct and the array of attribute row
     * pointers.
     *
     * Note: Only the fixed part of pg_attribute rows is included in tuple
     * descriptors, so we only need ATTRIBUTE_FIXED_PART_SIZE space per attr.
     * That might need alignment padding, however.
     */
    attroffset = sizeof(struct tupleDesc) + natts * sizeof(Form_pg_attribute);
    attroffset = MAXALIGN(attroffset);
    stg = palloc(attroffset + natts * MAXALIGN(ATTRIBUTE_FIXED_PART_SIZE));
    desc = (TupleDesc) stg;

    if (natts > 0)
    {
        Form_pg_attribute *attrs;
        int            i;

        attrs = (Form_pg_attribute *) (stg + sizeof(struct tupleDesc));
        desc->attrs = attrs;
        stg += attroffset;
        for (i = 0; i < natts; i++)
        {
            attrs[i] = (Form_pg_attribute) stg;
            stg += MAXALIGN(ATTRIBUTE_FIXED_PART_SIZE);
        }
    }
    else
        desc->attrs = NULL;

    /*
     * Initialize other fields of the tupdesc.
     */
    desc->natts = natts;
    desc->constr = NULL;
    desc->tdtypeid = RECORDOID;
    desc->tdtypmod = -1;
    desc->tdhasoid = hasoid;
    desc->tdrefcount = -1;        /* assume not reference-counted */
#ifdef _MLS_
    desc->use_attrs_ext= false;
    desc->tdclscol     = InvalidAttrNumber;
    desc->tdatamask    = NULL;
    desc->transp_crypt = NULL;
    desc->attrs_ext    = NULL;
#endif

    return desc;
}

/*
 * CreateTupleDesc
 *        This function allocates a new TupleDesc pointing to a given
 *        Form_pg_attribute array.
 *
 * Note: if the TupleDesc is ever freed, the Form_pg_attribute array
 * will not be freed thereby.
 *
 * Tuple type ID information is initially set for an anonymous record type;
 * caller can overwrite this if needed.
 */
TupleDesc
CreateTupleDesc(int natts, bool hasoid, Form_pg_attribute *attrs)
{
    TupleDesc    desc;

    /*
     * sanity checks
     */
    AssertArg(natts >= 0);

    desc = (TupleDesc) palloc(sizeof(struct tupleDesc));
    desc->attrs = attrs;
    desc->natts = natts;
    desc->constr = NULL;
    desc->tdtypeid = RECORDOID;
    desc->tdtypmod = -1;
    desc->tdhasoid = hasoid;
    desc->tdrefcount = -1;        /* assume not reference-counted */
#ifdef _MLS_
    desc->use_attrs_ext= false;
    desc->tdclscol     = InvalidAttrNumber;
    desc->tdatamask    = NULL;//datamask_check_and_create_datamask(attrs, natts);
    desc->transp_crypt = NULL;
    desc->attrs_ext    = NULL;
#endif

    return desc;
}

/*
 * CreateTupleDescCopy
 *        This function creates a new TupleDesc by copying from an existing
 *        TupleDesc.
 *
 * !!! Constraints and defaults are not copied !!!
 */
TupleDesc
CreateTupleDescCopy(TupleDesc tupdesc)
{
    TupleDesc    desc;
    int            i;

    desc = CreateTemplateTupleDesc(tupdesc->natts, tupdesc->tdhasoid);

    for (i = 0; i < desc->natts; i++)
    {
        memcpy(desc->attrs[i], tupdesc->attrs[i], ATTRIBUTE_FIXED_PART_SIZE);
        desc->attrs[i]->attnotnull = false;
        desc->attrs[i]->atthasdef = false;
#ifdef _MLS_        
        desc->attrs[i]->atthasmissing = false;
#endif
        desc->attrs[i]->attidentity = '\0';
    }

    desc->tdtypeid = tupdesc->tdtypeid;
    desc->tdtypmod = tupdesc->tdtypmod;
#ifdef _MLS_
    desc->use_attrs_ext = tupdesc->use_attrs_ext;
    desc->tdclscol      = tupdesc->tdclscol;
    datamask_alloc_and_copy(desc, tupdesc);
    transparent_crypt_alloc_and_copy(desc, tupdesc);
#endif

    return desc;
}

/*
 * CreateTupleDescCopyConstr
 *        This function creates a new TupleDesc by copying from an existing
 *        TupleDesc (including its constraints and defaults).
 */
TupleDesc
CreateTupleDescCopyConstr(TupleDesc tupdesc)
{// #lizard forgives
    TupleDesc    desc;
    TupleConstr *constr = tupdesc->constr;
    int            i;

    desc = CreateTemplateTupleDesc(tupdesc->natts, tupdesc->tdhasoid);

    for (i = 0; i < desc->natts; i++)
    {
        memcpy(desc->attrs[i], tupdesc->attrs[i], ATTRIBUTE_FIXED_PART_SIZE);
    }

    if (constr)
    {
        TupleConstr *cpy = (TupleConstr *) palloc0(sizeof(TupleConstr));

        cpy->has_not_null = constr->has_not_null;

        if ((cpy->num_defval = constr->num_defval) > 0)
        {
            cpy->defval = (AttrDefault *) palloc(cpy->num_defval * sizeof(AttrDefault));
            memcpy(cpy->defval, constr->defval, cpy->num_defval * sizeof(AttrDefault));
            for (i = cpy->num_defval - 1; i >= 0; i--)
            {
                if (constr->defval[i].adbin)
                    cpy->defval[i].adbin = pstrdup(constr->defval[i].adbin);
            }
        }

#ifdef _MLS_
        if (constr->missing)
        {
            cpy->missing = (AttrMissing *) palloc(tupdesc->natts * sizeof(AttrMissing));
            memcpy(cpy->missing, constr->missing, tupdesc->natts * sizeof(AttrMissing));
            for (i = tupdesc->natts - 1; i >= 0; i--)
            {
                if (constr->missing[i].ammissingPresent)
                {
                    Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

                    cpy->missing[i].ammissing = datumCopy(constr->missing[i].ammissing,
                                                          attr->attbyval,
                                                          attr->attlen);
                }
            }
        }
#endif

        if ((cpy->num_check = constr->num_check) > 0)
        {
            cpy->check = (ConstrCheck *) palloc(cpy->num_check * sizeof(ConstrCheck));
            memcpy(cpy->check, constr->check, cpy->num_check * sizeof(ConstrCheck));
            for (i = cpy->num_check - 1; i >= 0; i--)
            {
                if (constr->check[i].ccname)
                    cpy->check[i].ccname = pstrdup(constr->check[i].ccname);
                if (constr->check[i].ccbin)
                    cpy->check[i].ccbin = pstrdup(constr->check[i].ccbin);
                cpy->check[i].ccvalid = constr->check[i].ccvalid;
                cpy->check[i].ccnoinherit = constr->check[i].ccnoinherit;
            }
        }

        desc->constr = cpy;
    }

    desc->tdtypeid = tupdesc->tdtypeid;
    desc->tdtypmod = tupdesc->tdtypmod;
#ifdef _MLS_
    desc->use_attrs_ext = tupdesc->use_attrs_ext;
    desc->tdclscol      = tupdesc->tdclscol;
    datamask_alloc_and_copy(desc, tupdesc);
    transparent_crypt_alloc_and_copy(desc, tupdesc);
#endif

    return desc;
}

/*
 * TupleDescCopyEntry
 *        This function copies a single attribute structure from one tuple
 *        descriptor to another.
 *
 * !!! Constraints and defaults are not copied !!!
 */
void
TupleDescCopyEntry(TupleDesc dst, AttrNumber dstAttno,
                   TupleDesc src, AttrNumber srcAttno)
{
    /*
     * sanity checks
     */
    AssertArg(PointerIsValid(src));
    AssertArg(PointerIsValid(dst));
    AssertArg(srcAttno >= 1);
    AssertArg(srcAttno <= src->natts);
    AssertArg(dstAttno >= 1);
    AssertArg(dstAttno <= dst->natts);

    memcpy(dst->attrs[dstAttno - 1], src->attrs[srcAttno - 1],
           ATTRIBUTE_FIXED_PART_SIZE);

    /*
     * Aside from updating the attno, we'd better reset attcacheoff.
     *
     * XXX Actually, to be entirely safe we'd need to reset the attcacheoff of
     * all following columns in dst as well.  Current usage scenarios don't
     * require that though, because all following columns will get initialized
     * by other uses of this function or TupleDescInitEntry.  So we cheat a
     * bit to avoid a useless O(N^2) penalty.
     */
    dst->attrs[dstAttno - 1]->attnum = dstAttno;
    dst->attrs[dstAttno - 1]->attcacheoff = -1;

    /* since we're not copying constraints or defaults, clear these */
    dst->attrs[dstAttno - 1]->attnotnull = false;
    dst->attrs[dstAttno - 1]->atthasdef = false;
#ifdef _MLS_    
    dst->attrs[dstAttno - 1]->atthasmissing = false;
#endif
    dst->attrs[dstAttno - 1]->attidentity = '\0';
}

/*
 * Free a TupleDesc including all substructure
 */
void
FreeTupleDesc(TupleDesc tupdesc)
{// #lizard forgives
    int            i;

    /*
     * Possibly this should assert tdrefcount == 0, to disallow explicit
     * freeing of un-refcounted tupdescs?
     */
    Assert(tupdesc->tdrefcount <= 0);

    if (tupdesc->constr)
    {
        if (tupdesc->constr->num_defval > 0)
        {
            AttrDefault *attrdef = tupdesc->constr->defval;

            for (i = tupdesc->constr->num_defval - 1; i >= 0; i--)
            {
                if (attrdef[i].adbin)
                    pfree(attrdef[i].adbin);
            }
            pfree(attrdef);
        }
#ifdef _MLS_
        if (tupdesc->constr->missing)
        {
            AttrMissing *attrmiss = tupdesc->constr->missing;

            for (i = tupdesc->natts - 1; i >= 0; i--)
            {
                if (attrmiss[i].ammissingPresent
                    && !TupleDescAttr(tupdesc, i)->attbyval)
                    pfree(DatumGetPointer(attrmiss[i].ammissing));
            }
            pfree(attrmiss);
        }
#endif
        if (tupdesc->constr->num_check > 0)
        {
            ConstrCheck *check = tupdesc->constr->check;

            for (i = tupdesc->constr->num_check - 1; i >= 0; i--)
            {
                if (check[i].ccname)
                    pfree(check[i].ccname);
                if (check[i].ccbin)
                    pfree(check[i].ccbin);
            }
            pfree(check);
        }
        pfree(tupdesc->constr);
    }
#ifdef _MLS_
    datamask_free_datamask_struct(tupdesc->tdatamask);
    trsprt_crypt_free_strut_in_tupdesc(tupdesc, tupdesc->natts);
#endif
    pfree(tupdesc);
}

/*
 * Increment the reference count of a tupdesc, and log the reference in
 * CurrentResourceOwner.
 *
 * Do not apply this to tupdescs that are not being refcounted.  (Use the
 * macro PinTupleDesc for tupdescs of uncertain status.)
 */
void
IncrTupleDescRefCount(TupleDesc tupdesc)
{
    Assert(tupdesc->tdrefcount >= 0);

    ResourceOwnerEnlargeTupleDescs(CurrentResourceOwner);
    tupdesc->tdrefcount++;
    ResourceOwnerRememberTupleDesc(CurrentResourceOwner, tupdesc);
}

/*
 * Decrement the reference count of a tupdesc, remove the corresponding
 * reference from CurrentResourceOwner, and free the tupdesc if no more
 * references remain.
 *
 * Do not apply this to tupdescs that are not being refcounted.  (Use the
 * macro ReleaseTupleDesc for tupdescs of uncertain status.)
 */
void
DecrTupleDescRefCount(TupleDesc tupdesc)
{
    Assert(tupdesc->tdrefcount > 0);

    ResourceOwnerForgetTupleDesc(CurrentResourceOwner, tupdesc);
    if (--tupdesc->tdrefcount == 0)
        FreeTupleDesc(tupdesc);
}

/*
 * Compare two TupleDesc structures for logical equality
 *
 * Note: we deliberately do not check the attrelid and tdtypmod fields.
 * This allows typcache.c to use this routine to see if a cached record type
 * matches a requested type, and is harmless for relcache.c's uses.
 * We don't compare tdrefcount, either.
 */
bool
equalTupleDescs(TupleDesc tupdesc1, TupleDesc tupdesc2)
{// #lizard forgives
    int            i,
                j,
                n;

    if (tupdesc1->natts != tupdesc2->natts)
        return false;
    if (tupdesc1->tdtypeid != tupdesc2->tdtypeid)
        return false;
    if (tupdesc1->tdhasoid != tupdesc2->tdhasoid)
        return false;
#ifdef _MLS_
    if (tupdesc1->tdclscol != tupdesc2->tdclscol)
        return false;
#endif

    for (i = 0; i < tupdesc1->natts; i++)
    {
        Form_pg_attribute attr1 = tupdesc1->attrs[i];
        Form_pg_attribute attr2 = tupdesc2->attrs[i];

        /*
         * We do not need to check every single field here: we can disregard
         * attrelid and attnum (which were used to place the row in the attrs
         * array in the first place).  It might look like we could dispense
         * with checking attlen/attbyval/attalign, since these are derived
         * from atttypid; but in the case of dropped columns we must check
         * them (since atttypid will be zero for all dropped columns) and in
         * general it seems safer to check them always.
         *
         * attcacheoff must NOT be checked since it's possibly not set in both
         * copies.
         */
        if (strcmp(NameStr(attr1->attname), NameStr(attr2->attname)) != 0)
            return false;
        if (attr1->atttypid != attr2->atttypid)
            return false;
        if (attr1->attstattarget != attr2->attstattarget)
            return false;
        if (attr1->attlen != attr2->attlen)
            return false;
        if (attr1->attndims != attr2->attndims)
            return false;
        if (attr1->atttypmod != attr2->atttypmod)
            return false;
        if (attr1->attbyval != attr2->attbyval)
            return false;
        if (attr1->attstorage != attr2->attstorage)
            return false;
        if (attr1->attalign != attr2->attalign)
            return false;
        if (attr1->attnotnull != attr2->attnotnull)
            return false;
        if (attr1->atthasdef != attr2->atthasdef)
            return false;
        if (attr1->attidentity != attr2->attidentity)
            return false;
        if (attr1->attisdropped != attr2->attisdropped)
            return false;
        if (attr1->attislocal != attr2->attislocal)
            return false;
        if (attr1->attinhcount != attr2->attinhcount)
            return false;
        if (attr1->attcollation != attr2->attcollation)
            return false;
        /* attacl, attoptions and attfdwoptions are not even present... */
    }

    if (tupdesc1->constr != NULL)
    {
        TupleConstr *constr1 = tupdesc1->constr;
        TupleConstr *constr2 = tupdesc2->constr;

        if (constr2 == NULL)
            return false;
        if (constr1->has_not_null != constr2->has_not_null)
            return false;
        n = constr1->num_defval;
        if (n != (int) constr2->num_defval)
            return false;
        for (i = 0; i < n; i++)
        {
            AttrDefault *defval1 = constr1->defval + i;
            AttrDefault *defval2 = constr2->defval;

            /*
             * We can't assume that the items are always read from the system
             * catalogs in the same order; so use the adnum field to identify
             * the matching item to compare.
             */
            for (j = 0; j < n; defval2++, j++)
            {
                if (defval1->adnum == defval2->adnum)
                    break;
            }
            if (j >= n)
                return false;
            if (strcmp(defval1->adbin, defval2->adbin) != 0)
                return false;
        }
#ifdef _MLS_
        if (constr1->missing)
        {
            if (!constr2->missing)
                return false;
            for (i = 0; i < tupdesc1->natts; i++)
            {
                AttrMissing *missval1 = constr1->missing + i;
                AttrMissing *missval2 = constr2->missing + i;

                if (missval1->ammissingPresent != missval2->ammissingPresent)
                    return false;
                if (missval1->ammissingPresent)
                {
                    Form_pg_attribute missatt1 = TupleDescAttr(tupdesc1, i);

                    if (!datumIsEqual(missval1->ammissing, missval2->ammissing,
                                      missatt1->attbyval, missatt1->attlen))
                        return false;
                }
            }
        }
        else if (constr2->missing)
            return false;
#endif
        n = constr1->num_check;
        if (n != (int) constr2->num_check)
            return false;
        for (i = 0; i < n; i++)
        {
            ConstrCheck *check1 = constr1->check + i;
            ConstrCheck *check2 = constr2->check;

            /*
             * Similarly, don't assume that the checks are always read in the
             * same order; match them up by name and contents. (The name
             * *should* be unique, but...)
             */
            for (j = 0; j < n; check2++, j++)
            {
                if (strcmp(check1->ccname, check2->ccname) == 0 &&
                    strcmp(check1->ccbin, check2->ccbin) == 0 &&
                    check1->ccvalid == check2->ccvalid &&
                    check1->ccnoinherit == check2->ccnoinherit)
                    break;
            }
            if (j >= n)
                return false;
        }
    }
    else if (tupdesc2->constr != NULL)
        return false;
    
#ifdef _MLS_
    if (false == datamask_check_datamask_equal(tupdesc1->tdatamask, tupdesc2->tdatamask))
    {
        return false;
    }
    
    if (false == transparent_crypt_check_transp_crypt_equal(tupdesc1->transp_crypt, tupdesc2->transp_crypt, 
                                                            tupdesc1->natts, tupdesc2->natts))
    {
        return false;
    }
#endif    
    return true;
}

/*
 * TupleDescInitEntry
 *        This function initializes a single attribute structure in
 *        a previously allocated tuple descriptor.
 *
 * If attributeName is NULL, the attname field is set to an empty string
 * (this is for cases where we don't know or need a name for the field).
 * Also, some callers use this function to change the datatype-related fields
 * in an existing tupdesc; they pass attributeName = NameStr(att->attname)
 * to indicate that the attname field shouldn't be modified.
 *
 * Note that attcollation is set to the default for the specified datatype.
 * If a nondefault collation is needed, insert it afterwards using
 * TupleDescInitEntryCollation.
 */
void
TupleDescInitEntry(TupleDesc desc,
                   AttrNumber attributeNumber,
                   const char *attributeName,
                   Oid oidtypeid,
                   int32 typmod,
                   int attdim)
{
    HeapTuple    tuple;
    Form_pg_type typeForm;
    Form_pg_attribute att;

    /*
     * sanity checks
     */
    AssertArg(PointerIsValid(desc));
    AssertArg(attributeNumber >= 1);
    AssertArg(attributeNumber <= desc->natts);

    /*
     * initialize the attribute fields
     */
    att = desc->attrs[attributeNumber - 1];

    att->attrelid = 0;            /* dummy value */

    /*
     * Note: attributeName can be NULL, because the planner doesn't always
     * fill in valid resname values in targetlists, particularly for resjunk
     * attributes. Also, do nothing if caller wants to re-use the old attname.
     */
    if (attributeName == NULL)
        MemSet(NameStr(att->attname), 0, NAMEDATALEN);
    else if (attributeName != NameStr(att->attname))
        namestrcpy(&(att->attname), attributeName);

    att->attstattarget = -1;
    att->attcacheoff = -1;
    att->atttypmod = typmod;

    att->attnum = attributeNumber;
    att->attndims = attdim;

    att->attnotnull = false;
    att->atthasdef = false;
#ifdef _MLS_
    att->atthasmissing = false;
#endif
    att->attidentity = '\0';
    att->attisdropped = false;
    att->attislocal = true;
    att->attinhcount = 0;
    /* attacl, attoptions and attfdwoptions are not present in tupledescs */

    tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(oidtypeid));
    if (!HeapTupleIsValid(tuple))
        elog(ERROR, "cache lookup failed for type %u", oidtypeid);
    typeForm = (Form_pg_type) GETSTRUCT(tuple);

    att->atttypid = oidtypeid;
    att->attlen = typeForm->typlen;
    att->attbyval = typeForm->typbyval;
    att->attalign = typeForm->typalign;
    att->attstorage = typeForm->typstorage;
    att->attcollation = typeForm->typcollation;

    ReleaseSysCache(tuple);
}

/*
 * TupleDescInitBuiltinEntry
 *        Initialize a tuple descriptor without catalog access.  Only
 *        a limited range of builtin types are supported.
 */
void
TupleDescInitBuiltinEntry(TupleDesc desc,
                          AttrNumber attributeNumber,
                          const char *attributeName,
                          Oid oidtypeid,
                          int32 typmod,
                          int attdim)
{
    Form_pg_attribute att;

    /* sanity checks */
    AssertArg(PointerIsValid(desc));
    AssertArg(attributeNumber >= 1);
    AssertArg(attributeNumber <= desc->natts);

    /* initialize the attribute fields */
    att = desc->attrs[attributeNumber - 1];
    att->attrelid = 0;            /* dummy value */

    /* unlike TupleDescInitEntry, we require an attribute name */
    Assert(attributeName != NULL);
    namestrcpy(&(att->attname), attributeName);

    att->attstattarget = -1;
    att->attcacheoff = -1;
    att->atttypmod = typmod;

    att->attnum = attributeNumber;
    att->attndims = attdim;

    att->attnotnull = false;
    att->atthasdef = false;
#ifdef _MLS_
    att->atthasmissing = false;
#endif
    att->attidentity = '\0';
    att->attisdropped = false;
    att->attislocal = true;
    att->attinhcount = 0;
    /* attacl, attoptions and attfdwoptions are not present in tupledescs */

    att->atttypid = oidtypeid;

    /*
     * Our goal here is to support just enough types to let basic builtin
     * commands work without catalog access - e.g. so that we can do certain
     * things even in processes that are not connected to a database.
     */
    switch (oidtypeid)
    {
        case TEXTOID:
        case TEXTARRAYOID:
            att->attlen = -1;
            att->attbyval = false;
            att->attalign = 'i';
            att->attstorage = 'x';
            att->attcollation = DEFAULT_COLLATION_OID;
            break;

        case BOOLOID:
            att->attlen = 1;
            att->attbyval = true;
            att->attalign = 'c';
            att->attstorage = 'p';
            att->attcollation = InvalidOid;
            break;

        case INT4OID:
            att->attlen = 4;
            att->attbyval = true;
            att->attalign = 'i';
            att->attstorage = 'p';
            att->attcollation = InvalidOid;
            break;

        case INT8OID:
            att->attlen = 8;
            att->attbyval = FLOAT8PASSBYVAL;
            att->attalign = 'd';
            att->attstorage = 'p';
            att->attcollation = InvalidOid;
            break;
    }
}

/*
 * TupleDescInitEntryCollation
 *
 * Assign a nondefault collation to a previously initialized tuple descriptor
 * entry.
 */
void
TupleDescInitEntryCollation(TupleDesc desc,
                            AttrNumber attributeNumber,
                            Oid collationid)
{
    /*
     * sanity checks
     */
    AssertArg(PointerIsValid(desc));
    AssertArg(attributeNumber >= 1);
    AssertArg(attributeNumber <= desc->natts);

    desc->attrs[attributeNumber - 1]->attcollation = collationid;
}


/*
 * BuildDescForRelation
 *
 * Given a relation schema (list of ColumnDef nodes), build a TupleDesc.
 *
 * Note: the default assumption is no OIDs; caller may modify the returned
 * TupleDesc if it wants OIDs.  Also, tdtypeid will need to be filled in
 * later on.
 */
TupleDesc
BuildDescForRelation(List *schema)
{// #lizard forgives
    int            natts;
    AttrNumber    attnum;
    ListCell   *l;
    TupleDesc    desc;
    bool        has_not_null;
    char       *attname;
    Oid            atttypid;
    int32        atttypmod;
    Oid            attcollation;
    int            attdim;

    /*
     * allocate a new tuple descriptor
     */
    natts = list_length(schema);
    desc = CreateTemplateTupleDesc(natts, false);
    has_not_null = false;

    attnum = 0;

    foreach(l, schema)
    {
        ColumnDef  *entry = lfirst(l);
        AclResult    aclresult;

        /*
         * for each entry in the list, get the name and type information from
         * the list and have TupleDescInitEntry fill in the attribute
         * information we need.
         */
        attnum++;
#ifdef __TBASE__
        if (entry->is_dropped)
        {
            Form_pg_attribute attr = (Form_pg_attribute)entry->ptr;
            memcpy(desc->attrs[attnum - 1], attr, ATTRIBUTE_FIXED_PART_SIZE);
            continue;
        }
#endif
        attname = entry->colname;
        typenameTypeIdAndMod(NULL, entry->typeName, &atttypid, &atttypmod);

        aclresult = pg_type_aclcheck(atttypid, GetUserId(), ACL_USAGE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error_type(aclresult, atttypid);

        attcollation = GetColumnDefCollation(NULL, entry, atttypid);
        attdim = list_length(entry->typeName->arrayBounds);

        if (entry->typeName->setof)
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                     errmsg("column \"%s\" cannot be declared SETOF",
                            attname)));

        TupleDescInitEntry(desc, attnum, attname,
                           atttypid, atttypmod, attdim);

        /* Override TupleDescInitEntry's settings as requested */
        TupleDescInitEntryCollation(desc, attnum, attcollation);
        if (entry->storage)
            desc->attrs[attnum - 1]->attstorage = entry->storage;

        /* Fill in additional stuff not handled by TupleDescInitEntry */
        desc->attrs[attnum - 1]->attnotnull = entry->is_not_null;
        has_not_null |= entry->is_not_null;
        desc->attrs[attnum - 1]->attislocal = entry->is_local;
        desc->attrs[attnum - 1]->attinhcount = entry->inhcount;
    }

    if (has_not_null)
    {
        TupleConstr *constr = (TupleConstr *) palloc0(sizeof(TupleConstr));

        constr->has_not_null = true;
        constr->defval = NULL;
#ifdef _MLS_
        constr->missing = NULL;
#endif
        constr->num_defval = 0;
        constr->check = NULL;
        constr->num_check = 0;
        desc->constr = constr;
    }
    else
    {
        desc->constr = NULL;
    }

    return desc;
}

/*
 * BuildDescFromLists
 *
 * Build a TupleDesc given lists of column names (as String nodes),
 * column type OIDs, typmods, and collation OIDs.
 *
 * No constraints are generated.
 *
 * This is essentially a cut-down version of BuildDescForRelation for use
 * with functions returning RECORD.
 */
TupleDesc
BuildDescFromLists(List *names, List *types, List *typmods, List *collations)
{
    int            natts;
    AttrNumber    attnum;
    ListCell   *l1;
    ListCell   *l2;
    ListCell   *l3;
    ListCell   *l4;
    TupleDesc    desc;

    natts = list_length(names);
    Assert(natts == list_length(types));
    Assert(natts == list_length(typmods));
    Assert(natts == list_length(collations));

    /*
     * allocate a new tuple descriptor
     */
    desc = CreateTemplateTupleDesc(natts, false);

    attnum = 0;

    l2 = list_head(types);
    l3 = list_head(typmods);
    l4 = list_head(collations);
    foreach(l1, names)
    {
        char       *attname = strVal(lfirst(l1));
        Oid            atttypid;
        int32        atttypmod;
        Oid            attcollation;

        atttypid = lfirst_oid(l2);
        l2 = lnext(l2);
        atttypmod = lfirst_int(l3);
        l3 = lnext(l3);
        attcollation = lfirst_oid(l4);
        l4 = lnext(l4);

        attnum++;

        TupleDescInitEntry(desc, attnum, attname, atttypid, atttypmod, 0);
        TupleDescInitEntryCollation(desc, attnum, attcollation);
    }

    return desc;
}


#ifdef _MLS_
static bool transparent_crypt_check_transp_crypt_equal(TranspCrypt * tc1, TranspCrypt * tc2, int tc1attnum, int tc2attnum)
{// #lizard forgives
    int loop;

    if (false == g_enable_transparent_crypt)
    {
        return true;
    }
    
    if (tc1 == tc2)
    {
        return true;
    }

    if ((NULL != tc1 && NULL == tc2) || (NULL == tc1 && NULL != tc2))
    {
        return false;
    }

    if (tc1attnum != tc2attnum)
    {
        return false;
    }

    for (loop = 0; loop < tc1attnum; loop++)
    {
        if (tc1[loop].algo_id != tc2[loop].algo_id)
        {
            return false;
        }
    }
    
    return true;
}
/*
 * to make up tupledesccopy.
 */ 
static void transparent_crypt_alloc_and_copy(TupleDesc dst, TupleDesc src)
{
    int loop;

    if (false == g_enable_transparent_crypt)
    {
        return;
    }
    
    /*skip system table*/
    if (src->attrs)
    {
        if (!IS_SYSTEM_REL(src->attrs[0]->attrelid))
        {
        	trsprt_crypt_free_strut_in_tupdesc(dst, src->natts);

            if (src->transp_crypt)
            {
                dst->transp_crypt = (TranspCrypt * )palloc0(sizeof(TranspCrypt) * src->natts);
                
                for (loop = 0; loop < src->natts; loop++)
                {
                    dst->transp_crypt[loop].algo_id = src->transp_crypt[loop].algo_id;            
                }
            }

            if (src->attrs_ext)
            {
                dst->attrs_ext = (Form_pg_attribute *)palloc0(sizeof(Form_pg_attribute) * (src->natts));
                for (loop = 0; loop < src->natts; loop++)
                {
                    (dst->attrs_ext)[loop] = (Form_pg_attribute)palloc0(sizeof(FormData_pg_attribute));
                }
                
                transparent_crypt_copy_attrs(dst->attrs_ext, src->attrs_ext, src->natts);
            }
        }
    }
    return;
}

#endif
