/*-------------------------------------------------------------------------
 *
 * printtup.c
 *      Routines to print out tuples to the destination (both frontend
 *      clients and standalone backends are supported here).
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *      src/backend/access/common/printtup.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/printtup.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "tcop/pquery.h"
#include "utils/lsyscache.h"
#ifdef PGXC
#include "pgxc/pgxc.h"
#endif
#include "utils/memdebug.h"
#include "utils/memutils.h"

#include "miscadmin.h"
#ifdef __TBASE__
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "postmaster/postmaster.h"
#include "pgxc/squeue.h"
#include "executor/executor.h"
#include "utils/typcache.h"
extern bool IsAbortedTransactionBlockState(void);
#endif
static void printtup_startup(DestReceiver *self, int operation,
                 TupleDesc typeinfo);
static bool printtup(TupleTableSlot *slot, DestReceiver *self);
static bool printtup_20(TupleTableSlot *slot, DestReceiver *self);
static bool printtup_internal_20(TupleTableSlot *slot, DestReceiver *self);
static void printtup_shutdown(DestReceiver *self);
static void printtup_destroy(DestReceiver *self);


/* ----------------------------------------------------------------
 *        printtup / debugtup support
 * ----------------------------------------------------------------
 */

/* ----------------
 *        Private state for a printtup destination object
 *
 * NOTE: finfo is the lookup info for either typoutput or typsend, whichever
 * we are using for this column.
 * ----------------
 */
typedef struct
{                                /* Per-attribute information */
    Oid            typoutput;        /* Oid for the type's text output fn */
    Oid            typsend;        /* Oid for the type's binary output fn */
    bool        typisvarlena;    /* is it varlena (ie possibly toastable)? */
    int16        format;            /* format code for this column */
    FmgrInfo    finfo;            /* Precomputed call info for output fn */
} PrinttupAttrInfo;

typedef struct
{
    DestReceiver pub;            /* publicly-known function pointers */
    Portal        portal;            /* the Portal we are printing from */
    bool        sendDescrip;    /* send RowDescription at startup? */
    TupleDesc    attrinfo;        /* The attr info we are set up for */
    int            nattrs;
    PrinttupAttrInfo *myinfo;    /* Cached info about each attr */
    MemoryContext tmpcontext;    /* Memory context for per-row workspace */
#ifdef __TBASE__
    long        nTuples;
#endif
} DR_printtup;

/* ----------------
 *        Initialize: create a DestReceiver for printtup
 * ----------------
 */
DestReceiver *
printtup_create_DR(CommandDest dest)
{
    DR_printtup *self = (DR_printtup *) palloc0(sizeof(DR_printtup));

    self->pub.receiveSlot = printtup;    /* might get changed later */
    self->pub.rStartup = printtup_startup;
    self->pub.rShutdown = printtup_shutdown;
    self->pub.rDestroy = printtup_destroy;
    self->pub.mydest = dest;

    /*
     * Send T message automatically if DestRemote, but not if
     * DestRemoteExecute
     */
    self->sendDescrip = (dest == DestRemote);

    self->attrinfo = NULL;
    self->nattrs = 0;
    self->myinfo = NULL;
    self->tmpcontext = NULL;
#ifdef __TBASE__
    self->nTuples = 0;
#endif

    return (DestReceiver *) self;
}

/*
 * Set parameters for a DestRemote (or DestRemoteExecute) receiver
 */
void
SetRemoteDestReceiverParams(DestReceiver *self, Portal portal)
{
    DR_printtup *myState = (DR_printtup *) self;

    Assert(myState->pub.mydest == DestRemote ||
           myState->pub.mydest == DestRemoteExecute);

    myState->portal = portal;

    if (PG_PROTOCOL_MAJOR(FrontendProtocol) < 3)
    {
        /*
         * In protocol 2.0 the Bind message does not exist, so there is no way
         * for the columns to have different print formats; it's sufficient to
         * look at the first one.
         */
        if (portal->formats && portal->formats[0] != 0)
            myState->pub.receiveSlot = printtup_internal_20;
        else
            myState->pub.receiveSlot = printtup_20;
    }
}

static void
printtup_startup(DestReceiver *self, int operation, TupleDesc typeinfo)
{
    DR_printtup *myState = (DR_printtup *) self;
    Portal        portal = myState->portal;

    /*
     * Create a temporary memory context that we can reset once per row to
     * recover palloc'd memory.  This avoids any problems with leaks inside
     * datatype output routines, and should be faster than retail pfree's
     * anyway.
     */
    myState->tmpcontext = AllocSetContextCreate(CurrentMemoryContext,
                                                "printtup",
                                                ALLOCSET_DEFAULT_SIZES);

    if (PG_PROTOCOL_MAJOR(FrontendProtocol) < 3)
    {
        /*
         * Send portal name to frontend (obsolete cruft, gone in proto 3.0)
         *
         * If portal name not specified, use "blank" portal.
         */
        const char *portalName = portal->name;

        if (portalName == NULL || portalName[0] == '\0')
            portalName = "blank";

        pq_puttextmessage('P', portalName);
    }

    /*
     * If we are supposed to emit row descriptions, then send the tuple
     * descriptor of the tuples.
     */
    if (myState->sendDescrip)
        SendRowDescriptionMessage(typeinfo,
                                  FetchPortalTargetList(portal),
                                  portal->formats);

    /* ----------------
     * We could set up the derived attr info at this time, but we postpone it
     * until the first call of printtup, for 2 reasons:
     * 1. We don't waste time (compared to the old way) if there are no
     *      tuples at all to output.
     * 2. Checking in printtup allows us to handle the case that the tuples
     *      change type midway through (although this probably can't happen in
     *      the current executor).
     * ----------------
     */
}

/*
 * SendRowDescriptionMessage --- send a RowDescription message to the frontend
 *
 * Notes: the TupleDesc has typically been manufactured by ExecTypeFromTL()
 * or some similar function; it does not contain a full set of fields.
 * The targetlist will be NIL when executing a utility function that does
 * not have a plan.  If the targetlist isn't NIL then it is a Query node's
 * targetlist; it is up to us to ignore resjunk columns in it.  The formats[]
 * array pointer might be NULL (if we are doing Describe on a prepared stmt);
 * send zeroes for the format codes in that case.
 */
void
SendRowDescriptionMessage(TupleDesc typeinfo, List *targetlist, int16 *formats)
{// #lizard forgives
    Form_pg_attribute *attrs = typeinfo->attrs;
    int            natts = typeinfo->natts;
    int            proto = PG_PROTOCOL_MAJOR(FrontendProtocol);
    int            i;
    StringInfoData buf;
    ListCell   *tlist_item = list_head(targetlist);

    pq_beginmessage(&buf, 'T'); /* tuple descriptor message type */
    pq_sendint(&buf, natts, 2); /* # of attrs in tuples */

    for (i = 0; i < natts; ++i)
    {
        Oid            atttypid = attrs[i]->atttypid;
        int32        atttypmod = attrs[i]->atttypmod;

        pq_sendstring(&buf, NameStr(attrs[i]->attname));

#ifdef PGXC
        /*
         * Send the type name from a Postgres-XC backend node.
         * This preserves from OID inconsistencies as architecture is shared nothing.
         */
		if (IsConnFromCoord())
        {
            char       *typename;
			typename = get_typenamespace_typename(atttypid);
            pq_sendstring(&buf, typename);
        }
#endif

        /* column ID info appears in protocol 3.0 and up */
        if (proto >= 3)
        {
            /* Do we have a non-resjunk tlist item? */
            while (tlist_item &&
                   ((TargetEntry *) lfirst(tlist_item))->resjunk)
                tlist_item = lnext(tlist_item);
            if (tlist_item)
            {
                TargetEntry *tle = (TargetEntry *) lfirst(tlist_item);

                pq_sendint(&buf, tle->resorigtbl, 4);
                pq_sendint(&buf, tle->resorigcol, 2);
                tlist_item = lnext(tlist_item);
            }
            else
            {
                /* No info available, so send zeroes */
                pq_sendint(&buf, 0, 4);
                pq_sendint(&buf, 0, 2);
            }
        }
        /* If column is a domain, send the base type and typmod instead */
        atttypid = getBaseTypeAndTypmod(atttypid, &atttypmod);
        pq_sendint(&buf, (int) atttypid, sizeof(atttypid));
        pq_sendint(&buf, attrs[i]->attlen, sizeof(attrs[i]->attlen));
        pq_sendint(&buf, atttypmod, sizeof(atttypmod));
        /* format info appears in protocol 3.0 and up */
        if (proto >= 3)
        {
            if (formats)
                pq_sendint(&buf, formats[i], 2);
            else
                pq_sendint(&buf, 0, 2);
        }
    }
    pq_endmessage(&buf);
}

/*
 * Get the lookup info that printtup() needs
 */
static void
printtup_prepare_info(DR_printtup *myState, TupleDesc typeinfo, int numAttrs)
{
    int16       *formats = myState->portal->formats;
    int            i;

    /* get rid of any old data */
    if (myState->myinfo)
        pfree(myState->myinfo);
    myState->myinfo = NULL;

    myState->attrinfo = typeinfo;
    myState->nattrs = numAttrs;
    if (numAttrs <= 0)
        return;

    myState->myinfo = (PrinttupAttrInfo *)
        palloc0(numAttrs * sizeof(PrinttupAttrInfo));

    for (i = 0; i < numAttrs; i++)
    {
        PrinttupAttrInfo *thisState = myState->myinfo + i;
        int16        format = (formats ? formats[i] : 0);

        thisState->format = format;
        if (format == 0)
        {
            getTypeOutputInfo(typeinfo->attrs[i]->atttypid,
                              &thisState->typoutput,
                              &thisState->typisvarlena);
            fmgr_info(thisState->typoutput, &thisState->finfo);
        }
        else if (format == 1)
        {
            getTypeBinaryOutputInfo(typeinfo->attrs[i]->atttypid,
                                    &thisState->typsend,
                                    &thisState->typisvarlena);
            fmgr_info(thisState->typsend, &thisState->finfo);
        }
        else
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("unsupported format code: %d", format)));
    }
}

/* ----------------
 *        printtup --- print a tuple in protocol 3.0
 * ----------------
 */
static bool
printtup(TupleTableSlot *slot, DestReceiver *self)
{// #lizard forgives
    TupleDesc    typeinfo = slot->tts_tupleDescriptor;
    DR_printtup *myState = (DR_printtup *) self;
    MemoryContext oldcontext;
    StringInfoData buf;
    int            natts = typeinfo->natts;
    int            i;
    bool        binary = false;
    bool        needEncodingConvert = false;

#ifdef __TBASE__
    if (end_query_requested)
    {
        end_query_requested = false;
        Executor_done = true;
    }
#endif

    /* Set or update my derived attribute info, if needed */
    if (myState->attrinfo != typeinfo || myState->nattrs != natts)
        printtup_prepare_info(myState, typeinfo, natts);

#ifdef PGXC
    /*
     * The datanodes would have sent all attributes in TEXT form. But
     * if the client has asked for any attribute to be sent in a binary format,
     * then we must decode the datarow and send every attribute in the format
     * that the client has asked for. Otherwise its ok to just forward the
     * datarow as it is
     */
    for (i = 0; i < natts; ++i)
    {
        PrinttupAttrInfo *thisState = myState->myinfo + i;
        if (thisState->format != 0)
            binary = true;
    }
    /*
     * If we are having DataRow-based tuple we do not have to encode attribute
     * values, just send over the DataRow message as we received it from the
     * Datanode
     */
    if (slot->tts_datarow && !binary)
    {
        pq_putmessage('D', slot->tts_datarow->msg, slot->tts_datarow->msglen);

        if (IsAbortedTransactionBlockState())
        {
            abort();
        }
        return true;
    }
#endif

    /* Make sure the tuple is fully deconstructed */
    slot_getallattrs(slot);

    /* Switch into per-row context so we can recover memory below */
    oldcontext = MemoryContextSwitchTo(myState->tmpcontext);

    /*
     * Prepare a DataRow message (note buffer is in per-row context)
     */
    pq_beginmessage(&buf, 'D');

#ifdef __TBASE__
    if (enable_statistic)
    {
        myState->nTuples++;
    }
#endif

    pq_sendint(&buf, natts, 2);

	/* encoding convert only on datanode when connect from coordinator node or connect from app */
	if (isPGXCDataNode && (IsConnFromCoord() || IsConnFromApp()))
    {
        needEncodingConvert = true;
    }

    /*
     * send the attributes of this tuple
     */
    for (i = 0; i < natts; ++i)
    {
        PrinttupAttrInfo *thisState = myState->myinfo + i;
        Datum        attr = slot->tts_values[i];

        if (slot->tts_isnull[i])
        {
            pq_sendint(&buf, -1, 4);
            continue;
        }

        /*
         * Here we catch undefined bytes in datums that are returned to the
         * client without hitting disk; see comments at the related check in
         * PageAddItem().  This test is most useful for uncompressed,
         * non-external datums, but we're quite likely to see such here when
         * testing new C functions.
         */
        if (thisState->typisvarlena)
            VALGRIND_CHECK_MEM_IS_DEFINED(DatumGetPointer(attr),
                                          VARSIZE_ANY(attr));

        if (thisState->format == 0)
        {
            /* Text output */
            char       *outputstr;

            outputstr = OutputFunctionCall(&thisState->finfo, attr);

            if (needEncodingConvert)
            {
            pq_sendcountedtext(&buf, outputstr, strlen(outputstr), false);
        }
        else
        {
	            int len = strlen(outputstr);
#ifdef __TBASE__
	            if (slot->tts_tupleDescriptor->attrs[i]->atttypid == RECORDOID && self->mydest == DestRemoteExecute)
	            {
		            Oid			    tupType;
		            int32           tupTypmod;
		            TupleDesc       tupdesc;
		            uint32          n32;
		            StringInfoData  tupdesc_data;
		            HeapTupleHeader rec;
		            /* RECORD must be varlena */
		            Datum   attr_detoast = PointerGetDatum(PG_DETOAST_DATUM(slot->tts_values[i]));
		
		            rec = DatumGetHeapTupleHeader(attr_detoast);
		            
		            initStringInfo(&tupdesc_data);
		            
		            /* Extract type info from the tuple itself */
		            tupType = HeapTupleHeaderGetTypeId(rec);
		            tupTypmod = HeapTupleHeaderGetTypMod(rec);
		            tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
		
		            /* -2 to indicate this is composite type */
		            n32 = htonl(-2);
		            appendBinaryStringInfo(&buf, (char *) &n32, 4);
		
		            FormRowDescriptionMessage(tupdesc, NULL, NULL, &tupdesc_data);
		            ReleaseTupleDesc(tupdesc);
		            n32 = htonl(tupdesc_data.len);
		            /* write rowDesctiption */
		            appendBinaryStringInfo(&buf, (char *) &n32, 4);
		            appendBinaryStringInfo(&buf, tupdesc_data.data, tupdesc_data.len);
		
		            pfree(tupdesc_data.data);
	            }
#endif
                pq_sendint(&buf, len, 4);
                appendBinaryStringInfo(&buf, outputstr, len);
            }
		}
		else
		{
            /* Binary output */
            bytea       *outputbytes;

            outputbytes = SendFunctionCall(&thisState->finfo, attr);
            pq_sendint(&buf, VARSIZE(outputbytes) - VARHDRSZ, 4);
            pq_sendbytes(&buf, VARDATA(outputbytes),
                         VARSIZE(outputbytes) - VARHDRSZ);
        }
    }

    pq_endmessage(&buf);

    /* Return to caller's context, and flush row's temporary memory */
    MemoryContextSwitchTo(oldcontext);
    MemoryContextReset(myState->tmpcontext);

    return true;
}

/* ----------------
 *        printtup_20 --- print a tuple in protocol 2.0
 * ----------------
 */
static bool
printtup_20(TupleTableSlot *slot, DestReceiver *self)
{// #lizard forgives
    TupleDesc    typeinfo = slot->tts_tupleDescriptor;
    DR_printtup *myState = (DR_printtup *) self;
    MemoryContext oldcontext;
    StringInfoData buf;
    int            natts = typeinfo->natts;
    int            i,
                j,
                k;

    /* Set or update my derived attribute info, if needed */
    if (myState->attrinfo != typeinfo || myState->nattrs != natts)
        printtup_prepare_info(myState, typeinfo, natts);

    /* Make sure the tuple is fully deconstructed */
    slot_getallattrs(slot);

    /* Switch into per-row context so we can recover memory below */
    oldcontext = MemoryContextSwitchTo(myState->tmpcontext);

    /*
     * tell the frontend to expect new tuple data (in ASCII style)
     */
    pq_beginmessage(&buf, 'D');

    /*
     * send a bitmap of which attributes are not null
     */
    j = 0;
    k = 1 << 7;
    for (i = 0; i < natts; ++i)
    {
        if (!slot->tts_isnull[i])
            j |= k;                /* set bit if not null */
        k >>= 1;
        if (k == 0)                /* end of byte? */
        {
            pq_sendint(&buf, j, 1);
            j = 0;
            k = 1 << 7;
        }
    }
    if (k != (1 << 7))            /* flush last partial byte */
        pq_sendint(&buf, j, 1);

    /*
     * send the attributes of this tuple
     */
    for (i = 0; i < natts; ++i)
    {
        PrinttupAttrInfo *thisState = myState->myinfo + i;
        Datum        attr = slot->tts_values[i];
        char       *outputstr;

        if (slot->tts_isnull[i])
            continue;

        Assert(thisState->format == 0);

        outputstr = OutputFunctionCall(&thisState->finfo, attr);
        pq_sendcountedtext(&buf, outputstr, strlen(outputstr), true);
    }

    pq_endmessage(&buf);

    /* Return to caller's context, and flush row's temporary memory */
    MemoryContextSwitchTo(oldcontext);
    MemoryContextReset(myState->tmpcontext);

    return true;
}

/* ----------------
 *        printtup_shutdown
 * ----------------
 */
static void
printtup_shutdown(DestReceiver *self)
{
    DR_printtup *myState = (DR_printtup *) self;

    if (myState->myinfo)
        pfree(myState->myinfo);
    myState->myinfo = NULL;

    myState->attrinfo = NULL;

    if (myState->tmpcontext)
        MemoryContextDelete(myState->tmpcontext);
    myState->tmpcontext = NULL;

#ifdef __TBASE__
    if (enable_statistic)
    {
        elog(LOG, "PrintTup Pid %d sendTuples %ld", MyProcPid, myState->nTuples);
    }
#endif
}

/* ----------------
 *        printtup_destroy
 * ----------------
 */
static void
printtup_destroy(DestReceiver *self)
{
    pfree(self);
}

/* ----------------
 *        printatt
 * ----------------
 */
static void
printatt(unsigned attributeId,
         Form_pg_attribute attributeP,
         char *value)
{
    printf("\t%2d: %s%s%s%s\t(typeid = %u, len = %d, typmod = %d, byval = %c)\n",
           attributeId,
           NameStr(attributeP->attname),
           value != NULL ? " = \"" : "",
           value != NULL ? value : "",
           value != NULL ? "\"" : "",
           (unsigned int) (attributeP->atttypid),
           attributeP->attlen,
           attributeP->atttypmod,
           attributeP->attbyval ? 't' : 'f');
}

/* ----------------
 *        debugStartup - prepare to print tuples for an interactive backend
 * ----------------
 */
void
debugStartup(DestReceiver *self, int operation, TupleDesc typeinfo)
{
    int            natts = typeinfo->natts;
    Form_pg_attribute *attinfo = typeinfo->attrs;
    int            i;

    /*
     * show the return type of the tuples
     */
    for (i = 0; i < natts; ++i)
        printatt((unsigned) i + 1, attinfo[i], NULL);
    printf("\t----\n");
}

/* ----------------
 *        debugtup - print one tuple for an interactive backend
 * ----------------
 */
bool
debugtup(TupleTableSlot *slot, DestReceiver *self)
{
    TupleDesc    typeinfo = slot->tts_tupleDescriptor;
    int            natts = typeinfo->natts;
    int            i;
    Datum        attr;
    char       *value;
    bool        isnull;
    Oid            typoutput;
    bool        typisvarlena;

    for (i = 0; i < natts; ++i)
    {
        attr = slot_getattr(slot, i + 1, &isnull);
        if (isnull)
            continue;
        getTypeOutputInfo(typeinfo->attrs[i]->atttypid,
                          &typoutput, &typisvarlena);

        value = OidOutputFunctionCall(typoutput, attr);

        printatt((unsigned) i + 1, typeinfo->attrs[i], value);
    }
    printf("\t----\n");

    return true;
}

/* ----------------
 *        printtup_internal_20 --- print a binary tuple in protocol 2.0
 *
 * We use a different message type, i.e. 'B' instead of 'D' to
 * indicate a tuple in internal (binary) form.
 *
 * This is largely same as printtup_20, except we use binary formatting.
 * ----------------
 */
static bool
printtup_internal_20(TupleTableSlot *slot, DestReceiver *self)
{// #lizard forgives
    TupleDesc    typeinfo = slot->tts_tupleDescriptor;
    DR_printtup *myState = (DR_printtup *) self;
    MemoryContext oldcontext;
    StringInfoData buf;
    int            natts = typeinfo->natts;
    int            i,
                j,
                k;

    /* Set or update my derived attribute info, if needed */
    if (myState->attrinfo != typeinfo || myState->nattrs != natts)
        printtup_prepare_info(myState, typeinfo, natts);

    /* Make sure the tuple is fully deconstructed */
    slot_getallattrs(slot);

    /* Switch into per-row context so we can recover memory below */
    oldcontext = MemoryContextSwitchTo(myState->tmpcontext);

    /*
     * tell the frontend to expect new tuple data (in binary style)
     */
    pq_beginmessage(&buf, 'B');

    /*
     * send a bitmap of which attributes are not null
     */
    j = 0;
    k = 1 << 7;
    for (i = 0; i < natts; ++i)
    {
        if (!slot->tts_isnull[i])
            j |= k;                /* set bit if not null */
        k >>= 1;
        if (k == 0)                /* end of byte? */
        {
            pq_sendint(&buf, j, 1);
            j = 0;
            k = 1 << 7;
        }
    }
    if (k != (1 << 7))            /* flush last partial byte */
        pq_sendint(&buf, j, 1);

    /*
     * send the attributes of this tuple
     */
    for (i = 0; i < natts; ++i)
    {
        PrinttupAttrInfo *thisState = myState->myinfo + i;
        Datum        attr = slot->tts_values[i];
        bytea       *outputbytes;

        if (slot->tts_isnull[i])
            continue;

        Assert(thisState->format == 1);

        outputbytes = SendFunctionCall(&thisState->finfo, attr);
        pq_sendint(&buf, VARSIZE(outputbytes) - VARHDRSZ, 4);
        pq_sendbytes(&buf, VARDATA(outputbytes),
                     VARSIZE(outputbytes) - VARHDRSZ);
    }

    pq_endmessage(&buf);

    /* Return to caller's context, and flush row's temporary memory */
    MemoryContextSwitchTo(oldcontext);
    MemoryContextReset(myState->tmpcontext);

    return true;
}
#ifdef __TBASE__
void
FormRowDescriptionMessage(TupleDesc typeinfo, List *targetlist, int16 *formats, StringInfo buf)
{// #lizard forgives
    Form_pg_attribute *attrs = typeinfo->attrs;
    int            natts = typeinfo->natts;
    int            proto = PG_PROTOCOL_MAJOR(FrontendProtocol);
    int            i;
    ListCell   *tlist_item = list_head(targetlist);

    pq_sendint(buf, natts, 2); /* # of attrs in tuples */

    for (i = 0; i < natts; ++i)
    {
        Oid            atttypid = attrs[i]->atttypid;
        int32        atttypmod = attrs[i]->atttypmod;

        pq_sendstring(buf, NameStr(attrs[i]->attname));

#ifdef PGXC
        /*
         * Send the type name from a Postgres-XC backend node.
         * This preserves from OID inconsistencies as architecture is shared nothing.
         */
        {
            char       *typename;
            typename = get_typename(atttypid);
            pq_sendstring(buf, typename);
        }
#endif

        /* column ID info appears in protocol 3.0 and up */
        if (proto >= 3)
        {
            /* Do we have a non-resjunk tlist item? */
            while (tlist_item &&
                   ((TargetEntry *) lfirst(tlist_item))->resjunk)
                tlist_item = lnext(tlist_item);
            if (tlist_item)
            {
                TargetEntry *tle = (TargetEntry *) lfirst(tlist_item);

                pq_sendint(buf, tle->resorigtbl, 4);
                pq_sendint(buf, tle->resorigcol, 2);
                tlist_item = lnext(tlist_item);
            }
            else
            {
                /* No info available, so send zeroes */
                pq_sendint(buf, 0, 4);
                pq_sendint(buf, 0, 2);
            }
        }
        /* If column is a domain, send the base type and typmod instead */
        atttypid = getBaseTypeAndTypmod(atttypid, &atttypmod);
        pq_sendint(buf, (int) atttypid, sizeof(atttypid));
        pq_sendint(buf, attrs[i]->attlen, sizeof(attrs[i]->attlen));
        pq_sendint(buf, atttypmod, sizeof(atttypmod));
        /* format info appears in protocol 3.0 and up */
        if (proto >= 3)
        {
            if (formats)
                pq_sendint(buf, formats[i], 2);
            else
                pq_sendint(buf, 0, 2);
        }
    }
}
#endif
