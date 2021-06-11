/*-------------------------------------------------------------------------
 *
 * sequence.c
 *      PostgreSQL sequences support code.
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *      src/backend/commands/sequence.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <math.h>

#include "access/bufmask.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#ifdef __TBASE__
#include "catalog/pg_namespace.h"
#endif
#include "catalog/objectaccess.h"
#include "catalog/pg_sequence.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/sequence.h"
#include "commands/tablecmds.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "parser/parse_type.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "storage/smgr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/resowner.h"
#include "utils/syscache.h"
#include "commands/dbcommands.h"

#ifdef PGXC
#include "pgxc/pgxc.h"
/* PGXC_COORD */
#include "access/gtm.h"
#include "utils/memutils.h"
#ifdef XCP
#include "utils/timestamp.h"
#endif
#endif
#include "utils/varlena.h"

/*
 * We don't want to log each fetching of a value from a sequence,
 * so we pre-log a few fetches in advance. In the event of
 * crash we can lose (skip over) as many values as we pre-logged.
 */
#define SEQ_LOG_VALS    32

/*
 * The "special area" of a sequence's buffer page looks like this.
 */
#define SEQ_MAGIC      0x1717

/* Configuration options */
#ifdef XCP

int            SequenceRangeVal = 1;
#endif

typedef struct sequence_magic
{
    uint32        magic;
} sequence_magic;

/*
 * We store a SeqTable item for every sequence we have touched in the current
 * session.  This is needed to hold onto nextval/currval state.  (We can't
 * rely on the relcache, since it's only, well, a cache, and may decide to
 * discard entries.)
 */
typedef struct SeqTableData
{
    Oid            relid;            /* pg_class OID of this sequence (hash key) */
    Oid            filenode;        /* last seen relfilenode of this sequence */
    LocalTransactionId lxid;    /* xact in which we last did a seq op */
    bool        last_valid;        /* do we have a valid "last" value? */
    int64        last;            /* value last returned by nextval */
    int64        cached;            /* last value already cached for nextval */
    /* if last != cached, we have not used up all the cached values */
    int64        increment;        /* copy of sequence's increment field */
    /* note that increment is zero until we first do nextval_internal() */
#ifdef XCP
    TimestampTz last_call_time; /* the time when the last call as made */
    int64        range_multiplier; /* multiply this value with 2 next time */
#endif
} SeqTableData;

typedef SeqTableData *SeqTable;

static HTAB *seqhashtab = NULL; /* hash table for SeqTable items */

#ifdef PGXC
/*
 * Arguments for callback of sequence drop on GTM
 */
typedef struct drop_sequence_callback_arg
{
    char *seqname;
    GTM_SequenceDropType type;
    GTM_SequenceKeyType key;
} drop_sequence_callback_arg;

/*
 * Arguments for callback of sequence rename on GTM
 */
typedef struct rename_sequence_callback_arg
{
    char *newseqname;
    char *oldseqname;
} rename_sequence_callback_arg;
#endif

/*
 * last_used_seq is updated by nextval() to point to the last used
 * sequence.
 */
static SeqTableData *last_used_seq = NULL;

static void fill_seq_with_data(Relation rel, HeapTuple tuple);
static Relation lock_and_open_sequence(SeqTable seq);
static void create_seq_hashtable(void);
static void init_sequence(Oid relid, SeqTable *p_elm, Relation *p_rel);
static Form_pg_sequence_data read_seq_tuple(Relation rel,
               Buffer *buf, HeapTuple seqdatatuple);
static void init_params(ParseState *pstate, List *options, bool for_identity,
            bool isInit,
            Form_pg_sequence seqform,
            Form_pg_sequence_data seqdataform,
            bool *need_seq_rewrite,
            List **owned_by,
            bool *is_restart);
static void do_setval(Oid relid, int64 next, bool iscalled);
static void process_owned_by(Relation seqrel, List *owned_by, bool for_identity);
#ifdef __TBASE__
extern bool  g_GTM_skip_catalog;
#endif

#ifdef __TBASE__
extern bool is_txn_has_parallel_ddl;

/*
 * Check sequence exists or not
 */
bool PrecheckDefineSequence(CreateSeqStmt *seq)
{
	Oid		seqoid;
	Oid		nspid;
	bool	need_send = true;

	if (g_GTM_skip_catalog && IS_PGXC_DATANODE)
	{
		ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("skip_gtm_catalog can not be true on datanode.")));
	}
	
	if (!g_GTM_skip_catalog)
	{
		/* Unlogged sequences are not implemented -- not clear if useful. */
		if (seq->sequence->relpersistence == RELPERSISTENCE_UNLOGGED)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("unlogged sequences are not supported")));

		/*
		 * If if_not_exists was given and a relation with the same name already
		 * exists, bail out. (Note: we needn't check this when not if_not_exists,
		 * because DefineRelation will complain anyway.)
		 */
		if (seq->if_not_exists)
		{
			nspid = RangeVarGetAndCheckCreationNamespace(seq->sequence, NoLock,
															&seqoid);
			if (OidIsValid(seqoid))
			{
				ereport(NOTICE,
						(errcode(ERRCODE_DUPLICATE_TABLE),
						 errmsg("relation \"%s\" already exists, skipping",
								seq->sequence->relname)));
				need_send = false;
			}
			UnlockDatabaseObject(NamespaceRelationId, nspid, 0,
									AccessShareLock);
		}
	}

	return need_send;
}

/*
 * DefineSequence
 *                Creates a new sequence relation
 */
ObjectAddress
DefineSequence(ParseState *pstate, CreateSeqStmt *seq, bool exists_ok)
#else
ObjectAddress
DefineSequence(ParseState *pstate, CreateSeqStmt *seq)
#endif
{
    FormData_pg_sequence seqform;
    FormData_pg_sequence_data seqdataform;
    bool        need_seq_rewrite;
    List       *owned_by;
    CreateStmt *stmt = makeNode(CreateStmt);
    Oid            seqoid;
	ObjectAddress address = InvalidObjectAddress;
    Relation    rel;
    HeapTuple    tuple;
    TupleDesc    tupDesc;
    Datum        value[SEQ_COL_LASTCOL];
    bool        null[SEQ_COL_LASTCOL];
    Datum        pgs_values[Natts_pg_sequence];
    bool        pgs_nulls[Natts_pg_sequence];
    int            i;
#ifdef PGXC /* PGXC_COORD */
    GTM_Sequence    start_value = 1;
    GTM_Sequence    min_value = 1;
    GTM_Sequence    max_value = InvalidSequenceValue;
    GTM_Sequence    increment = 1;
    bool        cycle = false;
    bool        is_restart;
    char        *seqname = NULL;
#endif

#ifdef __TBASE__
    if (g_GTM_skip_catalog && IS_PGXC_DATANODE)
    {
        ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("skip_gtm_catalog can not be true on datanode.")));
    }
    
    if (!g_GTM_skip_catalog)
    {
#endif
        /* Unlogged sequences are not implemented -- not clear if useful. */
        if (seq->sequence->relpersistence == RELPERSISTENCE_UNLOGGED)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("unlogged sequences are not supported")));

        /*
         * If if_not_exists was given and a relation with the same name already
         * exists, bail out. (Note: we needn't check this when not if_not_exists,
         * because DefineRelation will complain anyway.)
         */
        if (seq->if_not_exists)
        {
            RangeVarGetAndCheckCreationNamespace(seq->sequence, NoLock, &seqoid);
            if (OidIsValid(seqoid))
            {
#ifdef __TBASE__
				if (!exists_ok)
					ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_TABLE),
						 errmsg("relation \"%s\" already exists",
								seq->sequence->relname)));
				else
#endif
                ereport(NOTICE,
                        (errcode(ERRCODE_DUPLICATE_TABLE),
                         errmsg("relation \"%s\" already exists, skipping",
                                seq->sequence->relname)));
                return InvalidObjectAddress;
            }
        }

        /* Check and set all option values */
        init_params(pstate, seq->options, seq->for_identity, true,
                    &seqform, &seqdataform,
                    &need_seq_rewrite, &owned_by,
                    &is_restart);

        /*
         * Create relation (and fill value[] and null[] for the tuple)
         */
        stmt->tableElts = NIL;
        for (i = SEQ_COL_FIRSTCOL; i <= SEQ_COL_LASTCOL; i++)
        {
            ColumnDef  *coldef = makeNode(ColumnDef);

            coldef->inhcount = 0;
            coldef->is_local = true;
            coldef->is_not_null = true;
            coldef->is_from_type = false;
            coldef->is_from_parent = false;
            coldef->storage = 0;
            coldef->raw_default = NULL;
            coldef->cooked_default = NULL;
            coldef->collClause = NULL;
            coldef->collOid = InvalidOid;
            coldef->constraints = NIL;
            coldef->location = -1;

            null[i - 1] = false;

            switch (i)
            {
                case SEQ_COL_LASTVAL:
                    coldef->typeName = makeTypeNameFromOid(INT8OID, -1);
                    coldef->colname = "last_value";
                    value[i - 1] = Int64GetDatumFast(seqdataform.last_value);
                    break;
                case SEQ_COL_LOG:
                    coldef->typeName = makeTypeNameFromOid(INT8OID, -1);
                    coldef->colname = "log_cnt";
                    value[i - 1] = Int64GetDatum((int64) 0);
                    break;
                case SEQ_COL_CALLED:
                    coldef->typeName = makeTypeNameFromOid(BOOLOID, -1);
                    coldef->colname = "is_called";
                    value[i - 1] = BoolGetDatum(false);
                    break;
            }
            stmt->tableElts = lappend(stmt->tableElts, coldef);
        }

        stmt->relation = seq->sequence;
        stmt->inhRelations = NIL;
        stmt->constraints = NIL;
        stmt->options = NIL;
        stmt->oncommit = ONCOMMIT_NOOP;
        stmt->tablespacename = NULL;
        stmt->if_not_exists = seq->if_not_exists;

        address = DefineRelation(stmt, RELKIND_SEQUENCE, seq->ownerId, NULL, NULL);
        seqoid = address.objectId;
        Assert(seqoid != InvalidOid);

        rel = heap_open(seqoid, AccessExclusiveLock);
        tupDesc = RelationGetDescr(rel);

        /* now initialize the sequence's data */
        tuple = heap_form_tuple(tupDesc, value, null);
        fill_seq_with_data(rel, tuple);

        /* process OWNED BY if given */
        if (owned_by)
            process_owned_by(rel, owned_by, seq->for_identity);

        seqname = GetGlobalSeqName(rel, NULL, NULL);
        increment = seqform.seqincrement;
        min_value = seqform.seqmin;
        max_value = seqform.seqmax;
        start_value = seqform.seqstart;
        cycle = seqform.seqcycle;

        heap_close(rel, NoLock);

        /* fill in pg_sequence */
        rel = heap_open(SequenceRelationId, RowExclusiveLock);
        tupDesc = RelationGetDescr(rel);

        memset(pgs_nulls, 0, sizeof(pgs_nulls));

        pgs_values[Anum_pg_sequence_seqrelid - 1] = ObjectIdGetDatum(seqoid);
        pgs_values[Anum_pg_sequence_seqtypid - 1] = ObjectIdGetDatum(seqform.seqtypid);
        pgs_values[Anum_pg_sequence_seqstart - 1] = Int64GetDatumFast(seqform.seqstart);
        pgs_values[Anum_pg_sequence_seqincrement - 1] = Int64GetDatumFast(seqform.seqincrement);
        pgs_values[Anum_pg_sequence_seqmax - 1] = Int64GetDatumFast(seqform.seqmax);
        pgs_values[Anum_pg_sequence_seqmin - 1] = Int64GetDatumFast(seqform.seqmin);
        pgs_values[Anum_pg_sequence_seqcache - 1] = Int64GetDatumFast(seqform.seqcache);
        pgs_values[Anum_pg_sequence_seqcycle - 1] = BoolGetDatum(seqform.seqcycle);

        tuple = heap_form_tuple(tupDesc, pgs_values, pgs_nulls);
        CatalogTupleInsert(rel, tuple);

        heap_freetuple(tuple);
        heap_close(rel, RowExclusiveLock);
#ifdef __TBASE__
    }
    else
    {
        
        /* Check and set all option values */
        init_params(pstate, seq->options, seq->for_identity, true,
                    &seqform, &seqdataform,
                    &need_seq_rewrite, &owned_by,
                    &is_restart);
        
        increment   = seqform.seqincrement;
        min_value   = seqform.seqmin;
        max_value   = seqform.seqmax;
        start_value = seqform.seqstart;
        cycle       = seqform.seqcycle;

        /* build the sequenct name. */
        seqname = (char*)palloc(1024);
        if (seq->sequence->catalogname)
        {
            snprintf(seqname, 1024, "%s.%s.%s", seq->sequence->catalogname, seq->sequence->schemaname, seq->sequence->relname);
        }
        else if (seq->sequence->schemaname)
        {
            snprintf(seqname, 1024, "%s.%s", seq->sequence->schemaname, seq->sequence->relname);
        }
        else 
        {
            snprintf(seqname, 1024, "%s", seq->sequence->relname);
        }
        
    }
#endif

#ifdef PGXC  /* PGXC_COORD */
    /*
     * Remote Coordinator is in charge of creating sequence in GTM.
     * If sequence is temporary, it is not necessary to create it on GTM.
     */
    if (IS_PGXC_LOCAL_COORDINATOR)
    {
        /* We also need to create it on the GTM */
        if (CreateSequenceGTM(seqname,
                              increment,
                              min_value,
                              max_value,
                              start_value, cycle) < 0)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("GTM error, could not create sequence %s", seqname)));
        }

#ifdef __TBASE__
        RegisterSeqCreate(seqname, GTM_SEQ_FULL_NAME);
#endif
        pfree(seqname);
        address.classId     = InvalidOid;
        address.objectId    = InvalidOid;
        address.objectSubId = InvalidOid;

        if (g_GTM_skip_catalog)
        {
            ereport(INFO,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("NO CATALOG TOUCHED.")));
        }
    }
#endif
    return address;
}

/*
 * Reset a sequence to its initial value.
 *
 * The change is made transactionally, so that on failure of the current
 * transaction, the sequence will be restored to its previous state.
 * We do that by creating a whole new relfilenode for the sequence; so this
 * works much like the rewriting forms of ALTER TABLE.
 *
 * Caller is assumed to have acquired AccessExclusiveLock on the sequence,
 * which must not be released until end of transaction.  Caller is also
 * responsible for permissions checking.
 */
void
ResetSequence(Oid seq_relid)
{
    Relation    seq_rel;
    SeqTable    elm;
    Form_pg_sequence_data seq;
    Buffer        buf;
    HeapTupleData seqdatatuple;
    HeapTuple    tuple;
    HeapTuple    pgstuple;
    Form_pg_sequence pgsform;
    int64        startv;

    /*
     * Read the old sequence.  This does a bit more work than really
     * necessary, but it's simple, and we do want to double-check that it's
     * indeed a sequence.
     */
    init_sequence(seq_relid, &elm, &seq_rel);
    (void) read_seq_tuple(seq_rel, &buf, &seqdatatuple);

    pgstuple = SearchSysCache1(SEQRELID, ObjectIdGetDatum(seq_relid));
    if (!HeapTupleIsValid(pgstuple))
        elog(ERROR, "cache lookup failed for sequence %u", seq_relid);
    pgsform = (Form_pg_sequence) GETSTRUCT(pgstuple);
    startv = pgsform->seqstart;
    ReleaseSysCache(pgstuple);

    /*
     * Copy the existing sequence tuple.
     */
    tuple = heap_copytuple(&seqdatatuple);

    /* Now we're done with the old page */
    UnlockReleaseBuffer(buf);

    /*
     * Modify the copied tuple to execute the restart (compare the RESTART
     * action in AlterSequence)
     */
    seq = (Form_pg_sequence_data) GETSTRUCT(tuple);
    seq->last_value = startv;
    seq->is_called = false;
    seq->log_cnt = 0;

    /*
     * Create a new storage file for the sequence.  We want to keep the
     * sequence's relfrozenxid at 0, since it won't contain any unfrozen XIDs.
     * Same with relminmxid, since a sequence will never contain multixacts.
     */
    RelationSetNewRelfilenode(seq_rel, seq_rel->rd_rel->relpersistence,
                              InvalidTransactionId, InvalidMultiXactId);

    /*
     * Insert the modified tuple into the new storage file.
     */
    fill_seq_with_data(seq_rel, tuple);

    /* Clear local cache so that we don't think we have cached numbers */
    /* Note that we do not change the currval() state */
    elm->cached = elm->last;

    relation_close(seq_rel, NoLock);
}

/*
 * Initialize a sequence's relation with the specified tuple as content
 */
static void
fill_seq_with_data(Relation rel, HeapTuple tuple)
{
    Buffer        buf;
    Page        page;
    sequence_magic *sm;
    OffsetNumber offnum;

    /* Initialize first page of relation with special magic number */

    buf = ReadBuffer(rel, P_NEW);
    Assert(BufferGetBlockNumber(buf) == 0);

	/* Now insert sequence tuple */
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    page = BufferGetPage(buf);

    PageInit(page, BufferGetPageSize(buf), sizeof(sequence_magic));
    sm = (sequence_magic *) PageGetSpecialPointer(page);
    sm->magic = SEQ_MAGIC;

    /*
     * Since VACUUM does not process sequences, we have to force the tuple to
     * have xmin = FrozenTransactionId now.  Otherwise it would become
     * invisible to SELECTs after 2G transactions.  It is okay to do this
     * because if the current transaction aborts, no other xact will ever
     * examine the sequence tuple anyway.
     */
    HeapTupleHeaderSetXmin(tuple->t_data, FrozenTransactionId);
    HeapTupleHeaderSetXminFrozen(tuple->t_data);
    HeapTupleHeaderSetCmin(tuple->t_data, FirstCommandId);
    HeapTupleHeaderSetXmax(tuple->t_data, InvalidTransactionId);
    tuple->t_data->t_infomask |= HEAP_XMAX_INVALID;
    ItemPointerSet(&tuple->t_data->t_ctid, 0, FirstOffsetNumber);

    /* check the comment above nextval_internal()'s equivalent call. */
    if (RelationNeedsWAL(rel))
        GetTopTransactionId();

    START_CRIT_SECTION();

    MarkBufferDirty(buf);

    offnum = PageAddItem(page, (Item) tuple->t_data, tuple->t_len,
                         InvalidOffsetNumber, false, false);
    if (offnum != FirstOffsetNumber)
        elog(ERROR, "failed to add sequence tuple to page");

    /* XLOG stuff */
    if (RelationNeedsWAL(rel))
    {
        xl_seq_rec    xlrec;
        XLogRecPtr    recptr;

        XLogBeginInsert();
        XLogRegisterBuffer(0, buf, REGBUF_WILL_INIT);

        xlrec.node = rel->rd_node;

        XLogRegisterData((char *) &xlrec, sizeof(xl_seq_rec));
        XLogRegisterData((char *) tuple->t_data, tuple->t_len);

        recptr = XLogInsert(RM_SEQ_ID, XLOG_SEQ_LOG);

        PageSetLSN(page, recptr);
    }

    END_CRIT_SECTION();

    UnlockReleaseBuffer(buf);
}

/*
 * AlterSequence
 *
 * Modify the definition of a sequence relation
 */
ObjectAddress
AlterSequence(ParseState *pstate, AlterSeqStmt *stmt)
{// #lizard forgives
    Oid            relid;
    SeqTable    elm;
    Relation    seqrel;
    Buffer        buf;
    HeapTupleData datatuple;
    Form_pg_sequence seqform;
    Form_pg_sequence_data newdataform;
    bool        need_seq_rewrite;
    List       *owned_by;
#ifdef PGXC
    GTM_Sequence    start_value;
    GTM_Sequence    last_value;
    GTM_Sequence    min_value;
    GTM_Sequence    max_value;
    GTM_Sequence    increment;
    bool            cycle;
    bool            is_restart;
#endif
	ObjectAddress address = InvalidObjectAddress;
    Relation    rel;
    HeapTuple    seqtuple;
    HeapTuple    newdatatuple;

    /* Open and lock sequence, and check for ownership along the way. */
    relid = RangeVarGetRelidExtended(stmt->sequence,
                                     ShareRowExclusiveLock,
                                     stmt->missing_ok,
                                     false,
                                     RangeVarCallbackOwnsRelation,
                                     NULL);
    if (relid == InvalidOid)
    {
        ereport(NOTICE,
                (errmsg("relation \"%s\" does not exist, skipping",
                        stmt->sequence->relname)));
        return InvalidObjectAddress;
    }
    

    init_sequence(relid, &elm, &seqrel);
#ifdef __TBASE__
    if (g_GTM_skip_catalog && IS_PGXC_DATANODE)
    {
        ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("skip_gtm_catalog can not be true on datanode.")));
    }

    if (!g_GTM_skip_catalog)
    {
#endif
        rel = heap_open(SequenceRelationId, RowExclusiveLock);
        seqtuple = SearchSysCacheCopy1(SEQRELID,
                                       ObjectIdGetDatum(relid));
        if (!HeapTupleIsValid(seqtuple))
            elog(ERROR, "cache lookup failed for sequence %u",
                 relid);

        seqform = (Form_pg_sequence) GETSTRUCT(seqtuple);

        /* lock page's buffer and read tuple into new sequence structure */
        (void) read_seq_tuple(seqrel, &buf, &datatuple);

        /* copy the existing sequence data tuple, so it can be modified locally */
        newdatatuple = heap_copytuple(&datatuple);
        newdataform = (Form_pg_sequence_data) GETSTRUCT(newdatatuple);

        UnlockReleaseBuffer(buf);

        /* Check and set new values */
        init_params(pstate, stmt->options, stmt->for_identity, false,
                    seqform, newdataform,
                    &need_seq_rewrite, &owned_by,
                    &is_restart);

        /* Clear local cache so that we don't think we have cached numbers */
        /* Note that we do not change the currval() state */
        elm->cached = elm->last;

        /* Now okay to update the on-disk tuple */
#ifdef PGXC
        increment = seqform->seqincrement;
        min_value = seqform->seqmin;
        max_value = seqform->seqmax;
        start_value = seqform->seqstart;
        last_value = newdataform->last_value;
        cycle = seqform->seqcycle;
#endif

        /* If needed, rewrite the sequence relation itself */
        if (need_seq_rewrite)
        {
            /* check the comment above nextval_internal()'s equivalent call. */
            if (RelationNeedsWAL(seqrel))
                GetTopTransactionId();

            /*
             * Create a new storage file for the sequence, making the state
             * changes transactional.  We want to keep the sequence's relfrozenxid
             * at 0, since it won't contain any unfrozen XIDs.  Same with
             * relminmxid, since a sequence will never contain multixacts.
             */
            RelationSetNewRelfilenode(seqrel, seqrel->rd_rel->relpersistence,
                                      InvalidTransactionId, InvalidMultiXactId);

            /*
             * Insert the modified tuple into the new storage file.
             */
            fill_seq_with_data(seqrel, newdatatuple);
        }

        /* process OWNED BY if given */
        if (owned_by)
            process_owned_by(seqrel, owned_by, stmt->for_identity);

        /* update the pg_sequence tuple (we could skip this in some cases...) */
        CatalogTupleUpdate(rel, &seqtuple->t_self, seqtuple);

        InvokeObjectPostAlterHook(RelationRelationId, relid, 0);

        ObjectAddressSet(address, RelationRelationId, relid);

        heap_close(rel, RowExclusiveLock);    
        relation_close(seqrel, NoLock);
#ifdef __TBASE__
    }
    else
    {
        /* just get the values we needed. */
        FormData_pg_sequence seqdata ;
        FormData_pg_sequence_data seqnewform;
        rel = heap_open(SequenceRelationId, RowExclusiveLock);
        seqtuple = SearchSysCacheCopy1(SEQRELID,
                                       ObjectIdGetDatum(relid));
        if (!HeapTupleIsValid(seqtuple))
            elog(ERROR, "cache lookup failed for sequence %u",
                 relid);

        seqform = (Form_pg_sequence) GETSTRUCT(seqtuple);
        memcpy((void*)&seqdata, seqform, sizeof(seqdata));

        /* lock page's buffer and read tuple into new sequence structure */
        (void) read_seq_tuple(seqrel, &buf, &datatuple);

        /* copy the existing sequence data tuple, so it can be modified locally */
        newdatatuple = heap_copytuple(&datatuple);
        newdataform = (Form_pg_sequence_data) GETSTRUCT(newdatatuple);
        memcpy((char*)&seqnewform, newdataform, sizeof(seqnewform));
        UnlockReleaseBuffer(buf);

        /* Check and set new values */
        init_params(pstate, stmt->options, stmt->for_identity, false,
                    &seqdata, &seqnewform,
                    &need_seq_rewrite, &owned_by,
                    &is_restart);

        /* Clear local cache so that we don't think we have cached numbers */
        /* Note that we do not change the currval() state */
        elm->cached = elm->last;

        /* Now okay to update the on-disk tuple */

        increment = seqdata.seqincrement;
        min_value = seqdata.seqmin;
        max_value = seqdata.seqmax;
        start_value = seqdata.seqstart;
        last_value = seqnewform.last_value;
        cycle = seqdata.seqcycle;
        
        heap_close(rel, RowExclusiveLock);    
        relation_close(seqrel, NoLock);
    }
#endif
    

#ifdef PGXC
    /*
     * Remote Coordinator is in charge of create sequence in GTM
     * If sequence is temporary, no need to go through GTM.
     */
    if (IS_PGXC_LOCAL_COORDINATOR && seqrel->rd_backend != MyBackendId)
    {
        char *seqname = GetGlobalSeqName(seqrel, NULL, NULL);

        /* We also need to create it on the GTM */
        if (AlterSequenceGTM(seqname,
                             increment,
                             min_value,
                             max_value,
                             start_value,
                             last_value,
                             cycle,
                             is_restart) < 0)
            ereport(ERROR,
                    (errcode(ERRCODE_CONNECTION_FAILURE),
                     errmsg("GTM error, could not alter sequence")));
        pfree(seqname);

        if (g_GTM_skip_catalog)
        {
            ereport(INFO,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("NO CATALOG TOUCHED.")));
        }
    }
#endif
    return address;
}

void
DeleteSequenceTuple(Oid relid)
{
    Relation    rel;
    HeapTuple    tuple;

    rel = heap_open(SequenceRelationId, RowExclusiveLock);

    tuple = SearchSysCache1(SEQRELID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tuple))
        elog(ERROR, "cache lookup failed for sequence %u", relid);

    CatalogTupleDelete(rel, &tuple->t_self);

    ReleaseSysCache(tuple);
    heap_close(rel, RowExclusiveLock);
}

/*
 * Note: nextval with a text argument is no longer exported as a pg_proc
 * entry, but we keep it around to ease porting of C code that may have
 * called the function directly.
 */
Datum
nextval(PG_FUNCTION_ARGS)
{
    text       *seqin = PG_GETARG_TEXT_PP(0);
    RangeVar   *sequence;
    Oid            relid;

    sequence = makeRangeVarFromNameList(textToQualifiedNameList(seqin));

    /*
     * XXX: This is not safe in the presence of concurrent DDL, but acquiring
     * a lock here is more expensive than letting nextval_internal do it,
     * since the latter maintains a cache that keeps us from hitting the lock
     * manager more than once per transaction.  It's not clear whether the
     * performance penalty is material in practice, but for now, we do it this
     * way.
     */
    relid = RangeVarGetRelid(sequence, NoLock, false);

    PG_RETURN_INT64(nextval_internal(relid, true));
}

Datum
nextval_oid(PG_FUNCTION_ARGS)
{
    Oid            relid = PG_GETARG_OID(0);

    PG_RETURN_INT64(nextval_internal(relid, true));
}

int64
nextval_internal(Oid relid, bool check_permissions)
{// #lizard forgives
    SeqTable    elm;
    Relation    seqrel;
    Buffer        buf;
    HeapTuple    pgstuple;
    Form_pg_sequence pgsform;
    HeapTupleData seqdatatuple;
    Form_pg_sequence_data seq;
    int64        incby;
    int64        cache;
    int64        result = 0;

    /* open and lock sequence */
    init_sequence(relid, &elm, &seqrel);

    if (check_permissions &&
        pg_class_aclcheck(elm->relid, GetUserId(),
                          ACL_USAGE | ACL_UPDATE) != ACLCHECK_OK)
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("permission denied for sequence %s",
                        RelationGetRelationName(seqrel))));

    /* read-only transactions may only modify temp sequences */
    if (!seqrel->rd_islocaltemp)
        PreventCommandIfReadOnly("nextval()");

    /*
     * Forbid this during parallel operation because, to make it work, the
     * cooperating backends would need to share the backend-local cached
     * sequence information.  Currently, we don't support that.
     */
    PreventCommandIfParallelMode("nextval()");

    if (elm->last != elm->cached)    /* some numbers were cached */
    {
        Assert(elm->last_valid);
        Assert(elm->increment != 0);
        elm->last += elm->increment;
        relation_close(seqrel, NoLock);
        last_used_seq = elm;
        elog(DEBUG1, "[nextval_internal] skip connect gtm. procid:%d get nextval:%lld, increment:%lld, cached:%lld",  
                    MyProcPid, (long long int)elm->last, (long long int)elm->increment, (long long int)elm->cached);
        return elm->last;
    }

    pgstuple = SearchSysCache1(SEQRELID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(pgstuple))
        elog(ERROR, "cache lookup failed for sequence %u", relid);
    pgsform = (Form_pg_sequence) GETSTRUCT(pgstuple);
    incby = pgsform->seqincrement;
    cache = pgsform->seqcache;
    ReleaseSysCache(pgstuple);

    /* lock page' buffer and read tuple */
    seq = read_seq_tuple(seqrel, &buf, &seqdatatuple);

    {
        int64 range = cache; /* how many values to ask from GTM? */
        int64 rangemax; /* the max value returned from the GTM for our request */
        char *seqname = GetGlobalSeqName(seqrel, NULL, NULL);

        /*
         * Above, we still use the page as a locking mechanism to handle
         * concurrency
         *
         * If the user has set a CACHE parameter, we use that. Else we pass in
         * the SequenceRangeVal value
         */
        if (range == DEFAULT_CACHEVAL && SequenceRangeVal > range)
        {
            TimestampTz curtime = GetCurrentTimestamp();

            if (!TimestampDifferenceExceeds(elm->last_call_time,
                                                    curtime, 1000))
            {
                /*
                 * The previous GetNextValGTM call was made just a while back.
                 * Request double the range of what was requested in the
                 * earlier call. Honor the SequenceRangeVal boundary
                 * value to limit very large range requests!
                 */
                elm->range_multiplier *= 2;
                if (elm->range_multiplier < SequenceRangeVal)
                    range = elm->range_multiplier;
                else
                    elm->range_multiplier = range = SequenceRangeVal;

                elog(DEBUG1, "increase sequence range %ld", range);
            }
            else if (TimestampDifferenceExceeds(elm->last_call_time,
                                                curtime, 5000))
            {
                /* The previous GetNextValGTM call was pretty old */
                range = elm->range_multiplier = DEFAULT_CACHEVAL;
                elog(DEBUG1, "reset sequence range %ld", range);
            }
            else if (TimestampDifferenceExceeds(elm->last_call_time,
                                                curtime, 3000))
            {
                /*
                 * The previous GetNextValGTM call was made quite some time
                 * ago. Try to reduce the range request to reduce the gap
                 */
                if (elm->range_multiplier != DEFAULT_CACHEVAL)
                {
                    range = elm->range_multiplier =
                                rint(elm->range_multiplier/2);
                    elog(DEBUG1, "decrease sequence range %ld", range);
                }
            }
            else
            {
                /*
                 * Current range_multiplier alllows to cache sequence values
                 * for 1-3 seconds of work. Keep that rate.
                 */
                range = elm->range_multiplier;
            }
            elm->last_call_time = curtime;
        }
#ifdef _PG_REGRESS_
        /* Always set range to 1 when regress */
        range  = 1; 
#endif
        result = (int64) GetNextValGTM(seqname, range, &rangemax);
        elog(DEBUG1, "[nextval_internal] connect gtm. seqname:%s procid:%d get nextval:%lld, range:%lld, rangemax:%lld",  
                    seqname, MyProcPid, (long long int)result, (long long int)range, (long long int)rangemax);
        pfree(seqname);

        /* Update the on-disk data */
        seq->last_value = result; /* last fetched number */
        seq->is_called = true;

        /* save info in local cache */
        elm->last = result;            /* last returned number */
        elm->cached = rangemax;        /* last fetched range max limit */
        elm->last_valid = true;

        last_used_seq = elm;
    }

    elm->increment = incby;

#ifdef __TBASE__
    /* ready to change the on-disk (or really, in-buffer) tuple */
    START_CRIT_SECTION();
    
    /*
     * We must mark the buffer dirty before doing XLogInsert(); see notes in
     * SyncOneBuffer().  However, we don't apply the desired changes just yet.
     * This looks like a violation of the buffer update protocol, but it is in
     * fact safe because we hold exclusive lock on the buffer.    Any other
     * process, including a checkpoint, that tries to examine the buffer
     * contents will block until we release the lock, and then will see the
     * final state that we install below.
     */
    MarkBufferDirty(buf);

    END_CRIT_SECTION();

#endif

    UnlockReleaseBuffer(buf);

    relation_close(seqrel, NoLock);

    return result;
}

Datum
currval_oid(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	int64		result;
	SeqTable	elm;
	Relation	seqrel;
	char *seqname = NULL;

	/* open and lock sequence */
	init_sequence(relid, &elm, &seqrel);

	if (pg_class_aclcheck(elm->relid, GetUserId(),
						  ACL_SELECT | ACL_USAGE) != ACLCHECK_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for sequence %s",
						RelationGetRelationName(seqrel))));

	if (elm->last_valid)
	{
		result = elm->last;
		relation_close(seqrel, NoLock);
		PG_RETURN_INT64(result);
	}

#ifdef XCP
    {
        /*
          * Always contact GTM for currval
          */
        {
            seqname = GetGlobalSeqName(seqrel, NULL, NULL);
            result = (int64) GetCurrentValGTM(seqname);
            elog(DEBUG1, "[currval_oid]seqname:%s procid:%d result:%lld", seqname, MyProcPid, (long long int)result);
            pfree(seqname);
        }
    }
#endif

    relation_close(seqrel, NoLock);

    PG_RETURN_INT64(result);
}

Datum
lastval(PG_FUNCTION_ARGS)
{
    Relation    seqrel;
    int64        result;

    if (last_used_seq == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("lastval is not yet defined in this session")));

    /* Someone may have dropped the sequence since the last nextval() */
    if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(last_used_seq->relid)))
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("lastval is not yet defined in this session")));

    seqrel = lock_and_open_sequence(last_used_seq);

    /* nextval() must have already been called for this sequence */
    Assert(last_used_seq->last_valid);

    if (pg_class_aclcheck(last_used_seq->relid, GetUserId(),
                          ACL_SELECT | ACL_USAGE) != ACLCHECK_OK)
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("permission denied for sequence %s",
                        RelationGetRelationName(seqrel))));

    result = last_used_seq->last;
    relation_close(seqrel, NoLock);

    PG_RETURN_INT64(result);
}

/*
 * Main internal procedure that handles 2 & 3 arg forms of SETVAL.
 *
 * Note that the 3 arg version (which sets the is_called flag) is
 * only for use in pg_dump, and setting the is_called flag may not
 * work if multiple users are attached to the database and referencing
 * the sequence (unlikely if pg_dump is restoring it).
 *
 * It is necessary to have the 3 arg version so that pg_dump can
 * restore the state of a sequence exactly during data-only restores -
 * it is the only way to clear the is_called flag in an existing
 * sequence.
 */
static void
do_setval(Oid relid, int64 next, bool iscalled)
{// #lizard forgives
    SeqTable    elm;
    Relation    seqrel;
    Buffer        buf;
    HeapTupleData seqdatatuple;
    Form_pg_sequence_data seq;
    HeapTuple    pgstuple;
    Form_pg_sequence pgsform;
    int64        maxv,
                minv;

    /* open and lock sequence */
    init_sequence(relid, &elm, &seqrel);

    if (pg_class_aclcheck(elm->relid, GetUserId(), ACL_UPDATE) != ACLCHECK_OK)
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("permission denied for sequence %s",
                        RelationGetRelationName(seqrel))));

    pgstuple = SearchSysCache1(SEQRELID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(pgstuple))
        elog(ERROR, "cache lookup failed for sequence %u", relid);
    pgsform = (Form_pg_sequence) GETSTRUCT(pgstuple);
    maxv = pgsform->seqmax;
    minv = pgsform->seqmin;
    ReleaseSysCache(pgstuple);

    /* read-only transactions may only modify temp sequences */
    if (!seqrel->rd_islocaltemp)
        PreventCommandIfReadOnly("setval()");

    /*
     * Forbid this during parallel operation because, to make it work, the
     * cooperating backends would need to share the backend-local cached
     * sequence information.  Currently, we don't support that.
     */
    PreventCommandIfParallelMode("setval()");

    /* lock page' buffer and read tuple */
    seq = read_seq_tuple(seqrel, &buf, &seqdatatuple);

    if ((next < minv) || (next > maxv))
    {
        char        bufv[100],
                    bufm[100],
                    bufx[100];

        snprintf(bufv, sizeof(bufv), INT64_FORMAT, next);
        snprintf(bufm, sizeof(bufm), INT64_FORMAT, minv);
        snprintf(bufx, sizeof(bufx), INT64_FORMAT, maxv);
        ereport(ERROR,
                (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                 errmsg("setval: value %s is out of bounds for sequence \"%s\" (%s..%s)",
                        bufv, RelationGetRelationName(seqrel),
                        bufm, bufx)));
    }

    {
        char *seqname = GetGlobalSeqName(seqrel, NULL, NULL);

        if (SetValGTM(seqname, next, iscalled) < 0)
            ereport(ERROR,
                    (errcode(ERRCODE_CONNECTION_FAILURE),
                     errmsg("GTM error, could not obtain sequence value")));
        pfree(seqname);
        /* Update the on-disk data */
        seq->last_value = next; /* last fetched number */
        seq->is_called = iscalled;
        seq->log_cnt = (iscalled) ? 0 : 1;

        if (iscalled)
        {
            elm->last = next;        /* last returned number */
            elm->last_valid = true;
        }
    }
    /* In any case, forget any future cached numbers */
    elm->cached = elm->last;

    /* check the comment above nextval_internal()'s equivalent call. */
    if (RelationNeedsWAL(seqrel))
        GetTopTransactionId();

    /* ready to change the on-disk (or really, in-buffer) tuple */
    START_CRIT_SECTION();

    seq->last_value = next;        /* last fetched number */
    seq->is_called = iscalled;
    seq->log_cnt = 0;

    MarkBufferDirty(buf);

    /* XLOG stuff */
    if (RelationNeedsWAL(seqrel))
    {
        xl_seq_rec    xlrec;
        XLogRecPtr    recptr;
        Page        page = BufferGetPage(buf);

        XLogBeginInsert();
        XLogRegisterBuffer(0, buf, REGBUF_WILL_INIT);

        xlrec.node = seqrel->rd_node;
        XLogRegisterData((char *) &xlrec, sizeof(xl_seq_rec));
        XLogRegisterData((char *) seqdatatuple.t_data, seqdatatuple.t_len);

        recptr = XLogInsert(RM_SEQ_ID, XLOG_SEQ_LOG);

        PageSetLSN(page, recptr);
        elm->cached = elm->last;
    }

    END_CRIT_SECTION();

    UnlockReleaseBuffer(buf);

    relation_close(seqrel, NoLock);
}

/*
 * Implement the 2 arg setval procedure.
 * See do_setval for discussion.
 */
Datum
setval_oid(PG_FUNCTION_ARGS)
{
    Oid            relid = PG_GETARG_OID(0);
    int64        next = PG_GETARG_INT64(1);

    do_setval(relid, next, true);

    PG_RETURN_INT64(next);
}

/*
 * Implement the 3 arg setval procedure.
 * See do_setval for discussion.
 */
Datum
setval3_oid(PG_FUNCTION_ARGS)
{
    Oid            relid = PG_GETARG_OID(0);
    int64        next = PG_GETARG_INT64(1);
    bool        iscalled = PG_GETARG_BOOL(2);

    do_setval(relid, next, iscalled);

    PG_RETURN_INT64(next);
}


/*
 * Open the sequence and acquire lock if needed
 *
 * If we haven't touched the sequence already in this transaction,
 * we need to acquire a lock.  We arrange for the lock to
 * be owned by the top transaction, so that we don't need to do it
 * more than once per xact.
 */
static Relation
lock_and_open_sequence(SeqTable seq)
{
    LocalTransactionId thislxid = MyProc->lxid;

    /* Get the lock if not already held in this xact */
    if (seq->lxid != thislxid)
    {
        ResourceOwner currentOwner;

        currentOwner = CurrentResourceOwner;
        PG_TRY();
        {
            CurrentResourceOwner = TopTransactionResourceOwner;
            LockRelationOid(seq->relid, RowExclusiveLock);
        }
        PG_CATCH();
        {
            /* Ensure CurrentResourceOwner is restored on error */
            CurrentResourceOwner = currentOwner;
            PG_RE_THROW();
        }
        PG_END_TRY();
        CurrentResourceOwner = currentOwner;

        /* Flag that we have a lock in the current xact */
        seq->lxid = thislxid;
    }

    /* We now know we have the lock, and can safely open the rel */
    return relation_open(seq->relid, NoLock);
}

/*
 * Creates the hash table for storing sequence data
 */
static void
create_seq_hashtable(void)
{
    HASHCTL        ctl;

    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize = sizeof(Oid);
    ctl.entrysize = sizeof(SeqTableData);

    seqhashtab = hash_create("Sequence values", 16, &ctl,
                             HASH_ELEM | HASH_BLOBS);
}

/*
 * Given a relation OID, open and lock the sequence.  p_elm and p_rel are
 * output parameters.
 */
static void
init_sequence(Oid relid, SeqTable *p_elm, Relation *p_rel)
{
    SeqTable    elm;
    Relation    seqrel;
    bool        found;

    /* Find or create a hash table entry for this sequence */
    if (seqhashtab == NULL)
        create_seq_hashtable();

    elm = (SeqTable) hash_search(seqhashtab, &relid, HASH_ENTER, &found);

    /*
     * Initialize the new hash table entry if it did not exist already.
     *
     * NOTE: seqtable entries are stored for the life of a backend (unless
     * explicitly discarded with DISCARD). If the sequence itself is deleted
     * then the entry becomes wasted memory, but it's small enough that this
     * should not matter.
     */
    if (!found)
    {
        /* relid already filled in */
        elm->filenode = InvalidOid;
        elm->lxid = InvalidLocalTransactionId;
        elm->last_valid = false;
#ifdef XCP
        elm->last_call_time = 0;
        elm->range_multiplier = DEFAULT_CACHEVAL;
#endif
        elm->last = elm->cached = 0;
    }

    /*
     * Open the sequence relation.
     */
    seqrel = lock_and_open_sequence(elm);

    if (seqrel->rd_rel->relkind != RELKIND_SEQUENCE)
        ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("\"%s\" is not a sequence",
                        RelationGetRelationName(seqrel))));

    /*
     * If the sequence has been transactionally replaced since we last saw it,
     * discard any cached-but-unissued values.  We do not touch the currval()
     * state, however.
     */
    if (seqrel->rd_rel->relfilenode != elm->filenode)
    {
        elm->filenode = seqrel->rd_rel->relfilenode;
        elm->cached = elm->last;
    }

    /* Return results */
    *p_elm = elm;
    *p_rel = seqrel;
}


/*
 * Given an opened sequence relation, lock the page buffer and find the tuple
 *
 * *buf receives the reference to the pinned-and-ex-locked buffer
 * *seqdatatuple receives the reference to the sequence tuple proper
 *        (this arg should point to a local variable of type HeapTupleData)
 *
 * Function's return value points to the data payload of the tuple
 */
static Form_pg_sequence_data
read_seq_tuple(Relation rel, Buffer *buf, HeapTuple seqdatatuple)
{
    Page        page;
    ItemId        lp;
    sequence_magic *sm;
    Form_pg_sequence_data seq;

    *buf = ReadBuffer(rel, 0);
    LockBuffer(*buf, BUFFER_LOCK_EXCLUSIVE);

    page = BufferGetPage(*buf);
    sm = (sequence_magic *) PageGetSpecialPointer(page);

    if (sm->magic != SEQ_MAGIC)
        elog(ERROR, "bad magic number in sequence \"%s\": %08X",
             RelationGetRelationName(rel), sm->magic);

    lp = PageGetItemId(page, FirstOffsetNumber);
    Assert(ItemIdIsNormal(lp));

    /* Note we currently only bother to set these two fields of *seqdatatuple */
    seqdatatuple->t_data = (HeapTupleHeader) PageGetItem(page, lp);
    seqdatatuple->t_len = ItemIdGetLength(lp);

    /*
     * Previous releases of Postgres neglected to prevent SELECT FOR UPDATE on
     * a sequence, which would leave a non-frozen XID in the sequence tuple's
     * xmax, which eventually leads to clog access failures or worse. If we
     * see this has happened, clean up after it.  We treat this like a hint
     * bit update, ie, don't bother to WAL-log it, since we can certainly do
     * this again if the update gets lost.
     */
    Assert(!(seqdatatuple->t_data->t_infomask & HEAP_XMAX_IS_MULTI));
    if (HeapTupleHeaderGetRawXmax(seqdatatuple->t_data) != InvalidTransactionId)
    {
        HeapTupleHeaderSetXmax(seqdatatuple->t_data, InvalidTransactionId);
        seqdatatuple->t_data->t_infomask &= ~HEAP_XMAX_COMMITTED;
        seqdatatuple->t_data->t_infomask |= HEAP_XMAX_INVALID;
        MarkBufferDirtyHint(*buf, true);
    }

    seq = (Form_pg_sequence_data) GETSTRUCT(seqdatatuple);

    return seq;
}

/*
 * init_params: process the options list of CREATE or ALTER SEQUENCE, and
 * store the values into appropriate fields of seqform, for changes that go
 * into the pg_sequence catalog, and fields of seqdataform for changes to the
 * sequence relation itself.  Set *need_seq_rewrite to true if we changed any
 * parameters that require rewriting the sequence's relation (interesting for
 * ALTER SEQUENCE).  Also set *owned_by to any OWNED BY option, or to NIL if
 * there is none.
 *
 * If isInit is true, fill any unspecified options with default values;
 * otherwise, do not change existing options that aren't explicitly overridden.
 *
 * Note: we force a sequence rewrite whenever we change parameters that affect
 * generation of future sequence values, even if the seqdataform per se is not
 * changed.  This allows ALTER SEQUENCE to behave transactionally.  Currently,
 * the only option that doesn't cause that is OWNED BY.  It's *necessary* for
 * ALTER SEQUENCE OWNED BY to not rewrite the sequence, because that would
 * break pg_upgrade by causing unwanted changes in the sequence's relfilenode.
 */
static void
init_params(ParseState *pstate, List *options, bool for_identity,
            bool isInit,
            Form_pg_sequence seqform,
            Form_pg_sequence_data seqdataform,
            bool *need_seq_rewrite,
            List **owned_by,
            bool *is_restart)
{// #lizard forgives
    DefElem    *as_type = NULL;
    DefElem    *start_value = NULL;
    DefElem    *restart_value = NULL;
    DefElem    *increment_by = NULL;
    DefElem    *max_value = NULL;
    DefElem    *min_value = NULL;
    DefElem    *cache_value = NULL;
    DefElem    *is_cycled = NULL;
    ListCell   *option;
    bool        reset_max_value = false;
    bool        reset_min_value = false;

#ifdef PGXC
    *is_restart = false;
#endif

    *need_seq_rewrite = false;
    *owned_by = NIL;

    foreach(option, options)
    {
        DefElem    *defel = (DefElem *) lfirst(option);

        if (strcmp(defel->defname, "as") == 0)
        {
            if (as_type)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("conflicting or redundant options"),
                         parser_errposition(pstate, defel->location)));
            as_type = defel;
            *need_seq_rewrite = true;
        }
        else if (strcmp(defel->defname, "increment") == 0)
        {
            if (increment_by)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("conflicting or redundant options"),
                         parser_errposition(pstate, defel->location)));
            increment_by = defel;
            *need_seq_rewrite = true;
        }
        else if (strcmp(defel->defname, "start") == 0)
        {
            if (start_value)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("conflicting or redundant options"),
                         parser_errposition(pstate, defel->location)));
            start_value = defel;
            *need_seq_rewrite = true;
        }
        else if (strcmp(defel->defname, "restart") == 0)
        {
            if (restart_value)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("conflicting or redundant options"),
                         parser_errposition(pstate, defel->location)));
            restart_value = defel;
            *need_seq_rewrite = true;
        }
        else if (strcmp(defel->defname, "maxvalue") == 0)
        {
            if (max_value)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("conflicting or redundant options"),
                         parser_errposition(pstate, defel->location)));
            max_value = defel;
            *need_seq_rewrite = true;
        }
        else if (strcmp(defel->defname, "minvalue") == 0)
        {
            if (min_value)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("conflicting or redundant options"),
                         parser_errposition(pstate, defel->location)));
            min_value = defel;
            *need_seq_rewrite = true;
        }
        else if (strcmp(defel->defname, "cache") == 0)
        {
            if (cache_value)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("conflicting or redundant options"),
                         parser_errposition(pstate, defel->location)));
            cache_value = defel;
            *need_seq_rewrite = true;
        }
        else if (strcmp(defel->defname, "cycle") == 0)
        {
            if (is_cycled)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("conflicting or redundant options"),
                         parser_errposition(pstate, defel->location)));
            is_cycled = defel;
            *need_seq_rewrite = true;
        }
        else if (strcmp(defel->defname, "owned_by") == 0)
        {
            if (*owned_by)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("conflicting or redundant options"),
                         parser_errposition(pstate, defel->location)));
            *owned_by = defGetQualifiedName(defel);
        }
        else if (strcmp(defel->defname, "sequence_name") == 0)
        {
            /*
             * The parser allows this, but it is only for identity columns, in
             * which case it is filtered out in parse_utilcmd.c.  We only get
             * here if someone puts it into a CREATE SEQUENCE.
             */
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("invalid sequence option SEQUENCE NAME"),
                     parser_errposition(pstate, defel->location)));
        }
        else
            elog(ERROR, "option \"%s\" not recognized",
                 defel->defname);
    }

    /*
     * We must reset log_cnt when isInit or when changing any parameters that
     * would affect future nextval allocations.
     */
    if (isInit)
        seqdataform->log_cnt = 0;

    /* AS type */
    if (as_type != NULL)
    {
        Oid            newtypid = typenameTypeId(pstate, defGetTypeName(as_type));

        if (newtypid != INT2OID &&
            newtypid != INT4OID &&
            newtypid != INT8OID)
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     for_identity
                     ? errmsg("identity column type must be smallint, integer, or bigint")
                     : errmsg("sequence type must be smallint, integer, or bigint")));

        if (!isInit)
        {
            /*
             * When changing type and the old sequence min/max values were the
             * min/max of the old type, adjust sequence min/max values to
             * min/max of new type.  (Otherwise, the user chose explicit
             * min/max values, which we'll leave alone.)
             */
            if ((seqform->seqtypid == INT2OID && seqform->seqmax == PG_INT16_MAX) ||
                (seqform->seqtypid == INT4OID && seqform->seqmax == PG_INT32_MAX) ||
                (seqform->seqtypid == INT8OID && seqform->seqmax == PG_INT64_MAX))
                reset_max_value = true;
            if ((seqform->seqtypid == INT2OID && seqform->seqmin == PG_INT16_MIN) ||
                (seqform->seqtypid == INT4OID && seqform->seqmin == PG_INT32_MIN) ||
                (seqform->seqtypid == INT8OID && seqform->seqmin == PG_INT64_MIN))
                reset_min_value = true;
        }

        seqform->seqtypid = newtypid;
    }
    else if (isInit)
    {
        seqform->seqtypid = INT8OID;
    }

    /* INCREMENT BY */
    if (increment_by != NULL)
    {
        seqform->seqincrement = defGetInt64(increment_by);
        if (seqform->seqincrement == 0)
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("INCREMENT must not be zero")));
        seqdataform->log_cnt = 0;
    }
    else if (isInit)
    {
        seqform->seqincrement = 1;
    }

    /* CYCLE */
    if (is_cycled != NULL)
    {
        seqform->seqcycle = intVal(is_cycled->arg);
        Assert(BoolIsValid(seqform->seqcycle));
        seqdataform->log_cnt = 0;
    }
    else if (isInit)
    {
        seqform->seqcycle = false;
    }

    /* MAXVALUE (null arg means NO MAXVALUE) */
    if (max_value != NULL && max_value->arg)
    {
        seqform->seqmax = defGetInt64(max_value);
        seqdataform->log_cnt = 0;
    }
    else if (isInit || max_value != NULL || reset_max_value)
    {
        if (seqform->seqincrement > 0 || reset_max_value)
        {
            /* ascending seq */
            if (seqform->seqtypid == INT2OID)
                seqform->seqmax = PG_INT16_MAX;
            else if (seqform->seqtypid == INT4OID)
                seqform->seqmax = PG_INT32_MAX;
            else
                seqform->seqmax = PG_INT64_MAX;
        }
        else
            seqform->seqmax = -1;    /* descending seq */
        seqdataform->log_cnt = 0;
    }

    if ((seqform->seqtypid == INT2OID && (seqform->seqmax < PG_INT16_MIN || seqform->seqmax > PG_INT16_MAX))
        || (seqform->seqtypid == INT4OID && (seqform->seqmax < PG_INT32_MIN || seqform->seqmax > PG_INT32_MAX))
        || (seqform->seqtypid == INT8OID && (seqform->seqmax < PG_INT64_MIN || seqform->seqmax > PG_INT64_MAX)))
    {
        char        bufx[100];

        snprintf(bufx, sizeof(bufx), INT64_FORMAT, seqform->seqmax);

        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("MAXVALUE (%s) is out of range for sequence data type %s",
                        bufx, format_type_be(seqform->seqtypid))));
    }

    /* MINVALUE (null arg means NO MINVALUE) */
    if (min_value != NULL && min_value->arg)
    {
        seqform->seqmin = defGetInt64(min_value);
        seqdataform->log_cnt = 0;
    }
    else if (isInit || min_value != NULL || reset_min_value)
    {
        if (seqform->seqincrement < 0 || reset_min_value)
        {
            /* descending seq */
            if (seqform->seqtypid == INT2OID)
                seqform->seqmin = PG_INT16_MIN;
            else if (seqform->seqtypid == INT4OID)
                seqform->seqmin = PG_INT32_MIN;
            else
                seqform->seqmin = PG_INT64_MIN;
        }
        else
            seqform->seqmin = 1;    /* ascending seq */
        seqdataform->log_cnt = 0;
    }

    if ((seqform->seqtypid == INT2OID && (seqform->seqmin < PG_INT16_MIN || seqform->seqmin > PG_INT16_MAX))
        || (seqform->seqtypid == INT4OID && (seqform->seqmin < PG_INT32_MIN || seqform->seqmin > PG_INT32_MAX))
        || (seqform->seqtypid == INT8OID && (seqform->seqmin < PG_INT64_MIN || seqform->seqmin > PG_INT64_MAX)))
    {
        char        bufm[100];

        snprintf(bufm, sizeof(bufm), INT64_FORMAT, seqform->seqmin);

        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("MINVALUE (%s) is out of range for sequence data type %s",
                        bufm, format_type_be(seqform->seqtypid))));
    }

    /* crosscheck min/max */
    if (seqform->seqmin >= seqform->seqmax)
    {
        char        bufm[100],
                    bufx[100];

        snprintf(bufm, sizeof(bufm), INT64_FORMAT, seqform->seqmin);
        snprintf(bufx, sizeof(bufx), INT64_FORMAT, seqform->seqmax);
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("MINVALUE (%s) must be less than MAXVALUE (%s)",
                        bufm, bufx)));
    }

    /* START WITH */
    if (start_value != NULL)
    {
        seqform->seqstart = defGetInt64(start_value);
    }
    else if (isInit)
    {
        if (seqform->seqincrement > 0)
            seqform->seqstart = seqform->seqmin;    /* ascending seq */
        else
            seqform->seqstart = seqform->seqmax;    /* descending seq */
    }

    /* crosscheck START */
    if (seqform->seqstart < seqform->seqmin)
    {
        char        bufs[100],
                    bufm[100];

        snprintf(bufs, sizeof(bufs), INT64_FORMAT, seqform->seqstart);
        snprintf(bufm, sizeof(bufm), INT64_FORMAT, seqform->seqmin);
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("START value (%s) cannot be less than MINVALUE (%s)",
                        bufs, bufm)));
    }
    if (seqform->seqstart > seqform->seqmax)
    {
        char        bufs[100],
                    bufm[100];

        snprintf(bufs, sizeof(bufs), INT64_FORMAT, seqform->seqstart);
        snprintf(bufm, sizeof(bufm), INT64_FORMAT, seqform->seqmax);
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("START value (%s) cannot be greater than MAXVALUE (%s)",
                        bufs, bufm)));
    }

    /* RESTART [WITH] */
    if (restart_value != NULL)
    {
        if (restart_value->arg != NULL)
            seqdataform->last_value = defGetInt64(restart_value);
        else
            seqdataform->last_value = seqform->seqstart;
#ifdef PGXC
        *is_restart = true;
#endif
        seqdataform->is_called = false;
        seqdataform->log_cnt = 0;
    }
    else if (isInit)
    {
        seqdataform->last_value = seqform->seqstart;
        seqdataform->is_called = false;
    }

    /* crosscheck RESTART (or current value, if changing MIN/MAX) */
    if (seqdataform->last_value < seqform->seqmin)
    {
        char        bufs[100],
                    bufm[100];

        snprintf(bufs, sizeof(bufs), INT64_FORMAT, seqdataform->last_value);
        snprintf(bufm, sizeof(bufm), INT64_FORMAT, seqform->seqmin);
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("RESTART value (%s) cannot be less than MINVALUE (%s)",
                        bufs, bufm)));
    }
    if (seqdataform->last_value > seqform->seqmax)
    {
        char        bufs[100],
                    bufm[100];

        snprintf(bufs, sizeof(bufs), INT64_FORMAT, seqdataform->last_value);
        snprintf(bufm, sizeof(bufm), INT64_FORMAT, seqform->seqmax);
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("RESTART value (%s) cannot be greater than MAXVALUE (%s)",
                        bufs, bufm)));
    }

    /* CACHE */
    if (cache_value != NULL)
    {
        seqform->seqcache = defGetInt64(cache_value);
        if (seqform->seqcache <= 0)
        {
            char        buf[100];

            snprintf(buf, sizeof(buf), INT64_FORMAT, seqform->seqcache);
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("CACHE (%s) must be greater than zero",
                            buf)));
        }
        seqdataform->log_cnt = 0;
    }
    else if (isInit)
    {
        seqform->seqcache = 1;
    }
}

#ifdef PGXC
/*
 * GetGlobalSeqName
 *
 * Returns a global sequence name adapted to GTM
 * Name format is dbname.schemaname.seqname
 * so as to identify in a unique way in the whole cluster each sequence
 */
char *
GetGlobalSeqName(Relation seqrel, const char *new_seqname, const char *new_schemaname)
{
    char *seqname, *dbname, *relname;
    char namespace[NAMEDATALEN * 2];
    int charlen;
	bool is_temp = false;
 
#ifdef PGXC
	/*
	 * In case of distributed session use MyFirstBackendId for temp objects.
	 */
	if (OidIsValid(MyCoordId))
	        is_temp = (seqrel->rd_backend == MyFirstBackendId);
	else
#endif
        is_temp = (seqrel->rd_backend == MyBackendId);

    /* Get all the necessary relation names */
    dbname = get_database_name(seqrel->rd_node.dbNode);

    if (new_seqname)
        relname = (char *) new_seqname;
    else
        relname = RelationGetRelationName(seqrel);

    if (!is_temp)
    {
        /*
         * For a permanent sequence, use schema qualified name. That can
         * uniquely identify the sequences.
         */
        char *schema = get_namespace_name(RelationGetNamespace(seqrel));
        sprintf(namespace, "%s", new_schemaname ? new_schemaname : schema);
        pfree(schema);
    }
    else
    {
        /*
         * For temporary sequences, we use originating coordinator name and
         * originating coordinator PID to qualify the sequence name. If we are
         * running on the local coordinator, we can readily fetch that
         * information from PGXCNodeName and MyProcPid, but when running on
         * remote datanode, we must consult MyCoordName and MyProcPid to get
         * the correct information.
         */
        if (IS_PGXC_LOCAL_COORDINATOR)
            sprintf(namespace, "%s.%d", PGXCNodeName, MyProcPid);
        else
            sprintf(namespace, "%s.%d", MyCoordName, MyCoordPid);
    }

    /* Calculate the global name size including the dots and \0 */
    charlen = strlen(dbname) + strlen(namespace) + strlen(relname) + 3;
    seqname = (char *) palloc(charlen);

    /* Form a unique sequence name with schema and database name for GTM */
    snprintf(seqname,
             charlen,
             "%s.%s.%s",
             dbname,
             namespace,
             relname);

    if (dbname)
        pfree(dbname);

    return seqname;
}

/*
 * IsTempSequence
 *
 * Determine if given sequence is temporary or not.
 */
bool
IsTempSequence(Oid relid)
{
    Relation seqrel;
    bool res;
    SeqTable    elm;

    /* open and AccessShareLock sequence */
    init_sequence(relid, &elm, &seqrel);
	
#ifdef PGXC
        if (OidIsValid(MyCoordId))
                res = (seqrel->rd_backend == MyFirstBackendId);
        else
#endif
	res = (seqrel->rd_backend == MyBackendId);
    relation_close(seqrel, NoLock);
    return res;
}
#endif

/*
 * Process an OWNED BY option for CREATE/ALTER SEQUENCE
 *
 * Ownership permissions on the sequence are already checked,
 * but if we are establishing a new owned-by dependency, we must
 * enforce that the referenced table has the same owner and namespace
 * as the sequence.
 */
static void
process_owned_by(Relation seqrel, List *owned_by, bool for_identity)
{// #lizard forgives
    DependencyType deptype;
    int            nnames;
    Relation    tablerel;
    AttrNumber    attnum;

    deptype = for_identity ? DEPENDENCY_INTERNAL : DEPENDENCY_AUTO;

    nnames = list_length(owned_by);
    Assert(nnames > 0);
    if (nnames == 1)
    {
        /* Must be OWNED BY NONE */
        if (strcmp(strVal(linitial(owned_by)), "none") != 0)
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("invalid OWNED BY option"),
                     errhint("Specify OWNED BY table.column or OWNED BY NONE.")));
        tablerel = NULL;
        attnum = 0;
    }
    else
    {
        List       *relname;
        char       *attrname;
        RangeVar   *rel;

        /* Separate relname and attr name */
        relname = list_truncate(list_copy(owned_by), nnames - 1);
        attrname = strVal(lfirst(list_tail(owned_by)));

        /* Open and lock rel to ensure it won't go away meanwhile */
        rel = makeRangeVarFromNameList(relname);
        tablerel = relation_openrv(rel, AccessShareLock);

        /* Must be a regular or foreign table */
        if (!(tablerel->rd_rel->relkind == RELKIND_RELATION ||
              tablerel->rd_rel->relkind == RELKIND_FOREIGN_TABLE ||
              tablerel->rd_rel->relkind == RELKIND_VIEW ||
              tablerel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE))
            ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg("referenced relation \"%s\" is not a table or foreign table",
                            RelationGetRelationName(tablerel))));

        /* We insist on same owner and schema */
        if (seqrel->rd_rel->relowner != tablerel->rd_rel->relowner)
            ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                     errmsg("sequence must have same owner as table it is linked to")));
        if (RelationGetNamespace(seqrel) != RelationGetNamespace(tablerel))
            ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                     errmsg("sequence must be in same schema as table it is linked to")));

        /* Now, fetch the attribute number from the system cache */
        attnum = get_attnum(RelationGetRelid(tablerel), attrname);
        if (attnum == InvalidAttrNumber)
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_COLUMN),
                     errmsg("column \"%s\" of relation \"%s\" does not exist",
                            attrname, RelationGetRelationName(tablerel))));
    }

    /*
     * Catch user explicitly running OWNED BY on identity sequence.
     */
    if (deptype == DEPENDENCY_AUTO)
    {
        Oid            tableId;
        int32        colId;

        if (sequenceIsOwned(RelationGetRelid(seqrel), DEPENDENCY_INTERNAL, &tableId, &colId))
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("cannot change ownership of identity sequence"),
                     errdetail("Sequence \"%s\" is linked to table \"%s\".",
                               RelationGetRelationName(seqrel),
                               get_rel_name(tableId))));
    }

    /*
     * OK, we are ready to update pg_depend.  First remove any existing
     * dependencies for the sequence, then optionally add a new one.
     */
    deleteDependencyRecordsForClass(RelationRelationId, RelationGetRelid(seqrel),
                                    RelationRelationId, deptype);

    if (tablerel)
    {
        ObjectAddress refobject,
                    depobject;

        refobject.classId = RelationRelationId;
        refobject.objectId = RelationGetRelid(tablerel);
        refobject.objectSubId = attnum;
        depobject.classId = RelationRelationId;
        depobject.objectId = RelationGetRelid(seqrel);
        depobject.objectSubId = 0;
        recordDependencyOn(&depobject, &refobject, deptype);
    }

    /* Done, but hold lock until commit */
    if (tablerel)
        relation_close(tablerel, NoLock);
}


/*
 * Return sequence parameters in a list of the form created by the parser.
 */
List *
sequence_options(Oid relid)
{
    HeapTuple    pgstuple;
    Form_pg_sequence pgsform;
    List       *options = NIL;

    pgstuple = SearchSysCache1(SEQRELID, relid);
    if (!HeapTupleIsValid(pgstuple))
        elog(ERROR, "cache lookup failed for sequence %u", relid);
    pgsform = (Form_pg_sequence) GETSTRUCT(pgstuple);

    options = lappend(options, makeDefElem("cache", (Node *) makeInteger(pgsform->seqcache), -1));
    options = lappend(options, makeDefElem("cycle", (Node *) makeInteger(pgsform->seqcycle), -1));
    options = lappend(options, makeDefElem("increment", (Node *) makeInteger(pgsform->seqincrement), -1));
    options = lappend(options, makeDefElem("maxvalue", (Node *) makeInteger(pgsform->seqmax), -1));
    options = lappend(options, makeDefElem("minvalue", (Node *) makeInteger(pgsform->seqmin), -1));
    options = lappend(options, makeDefElem("start", (Node *) makeInteger(pgsform->seqstart), -1));

    ReleaseSysCache(pgstuple);

    return options;
}

/*
 * Return sequence parameters (formerly for use by information schema)
 */
Datum
pg_sequence_parameters(PG_FUNCTION_ARGS)
{
    Oid            relid = PG_GETARG_OID(0);
    TupleDesc    tupdesc;
    Datum        values[7];
    bool        isnull[7];
    HeapTuple    pgstuple;
    Form_pg_sequence pgsform;

    if (pg_class_aclcheck(relid, GetUserId(), ACL_SELECT | ACL_UPDATE | ACL_USAGE) != ACLCHECK_OK)
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("permission denied for sequence %s",
                        get_rel_name(relid))));

    tupdesc = CreateTemplateTupleDesc(7, false);
    TupleDescInitEntry(tupdesc, (AttrNumber) 1, "start_value",
                       INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 2, "minimum_value",
                       INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 3, "maximum_value",
                       INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 4, "increment",
                       INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 5, "cycle_option",
                       BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 6, "cache_size",
                       INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 7, "data_type",
                       OIDOID, -1, 0);

    BlessTupleDesc(tupdesc);

    memset(isnull, 0, sizeof(isnull));

    pgstuple = SearchSysCache1(SEQRELID, relid);
    if (!HeapTupleIsValid(pgstuple))
        elog(ERROR, "cache lookup failed for sequence %u", relid);
    pgsform = (Form_pg_sequence) GETSTRUCT(pgstuple);

    values[0] = Int64GetDatum(pgsform->seqstart);
    values[1] = Int64GetDatum(pgsform->seqmin);
    values[2] = Int64GetDatum(pgsform->seqmax);
    values[3] = Int64GetDatum(pgsform->seqincrement);
    values[4] = BoolGetDatum(pgsform->seqcycle);
    values[5] = Int64GetDatum(pgsform->seqcache);
    values[6] = ObjectIdGetDatum(pgsform->seqtypid);

    ReleaseSysCache(pgstuple);

    return HeapTupleGetDatum(heap_form_tuple(tupdesc, values, isnull));
}

/*
 * Return the last value from the sequence
 *
 * Note: This has a completely different meaning than lastval().
 */
Datum
pg_sequence_last_value(PG_FUNCTION_ARGS)
{
    Oid            relid = PG_GETARG_OID(0);
    SeqTable    elm;
    Relation    seqrel;
    Buffer        buf;
    HeapTupleData seqtuple;
    Form_pg_sequence_data seq;
    bool        is_called;
    int64        result;

    /* open and lock sequence */
    init_sequence(relid, &elm, &seqrel);

    if (pg_class_aclcheck(relid, GetUserId(), ACL_SELECT | ACL_USAGE) != ACLCHECK_OK)
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("permission denied for sequence %s",
                        RelationGetRelationName(seqrel))));

    seq = read_seq_tuple(seqrel, &buf, &seqtuple);

    is_called = seq->is_called;
    result = seq->last_value;

    UnlockReleaseBuffer(buf);
    relation_close(seqrel, NoLock);

    if (is_called)
        PG_RETURN_INT64(result);
    else
        PG_RETURN_NULL();
}


void
seq_redo(XLogReaderState *record)
{
    XLogRecPtr    lsn = record->EndRecPtr;
    uint8        info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    Buffer        buffer;
    Page        page;
    Page        localpage;
    char       *item;
    Size        itemsz;
    xl_seq_rec *xlrec = (xl_seq_rec *) XLogRecGetData(record);
    sequence_magic *sm;

    if (info != XLOG_SEQ_LOG)
        elog(PANIC, "seq_redo: unknown op code %u", info);

    buffer = XLogInitBufferForRedo(record, 0);
    page = (Page) BufferGetPage(buffer);

    /*
     * We always reinit the page.  However, since this WAL record type is also
     * used for updating sequences, it's possible that a hot-standby backend
     * is examining the page concurrently; so we mustn't transiently trash the
     * buffer.  The solution is to build the correct new page contents in
     * local workspace and then memcpy into the buffer.  Then only bytes that
     * are supposed to change will change, even transiently. We must palloc
     * the local page for alignment reasons.
     */
    localpage = (Page) palloc(BufferGetPageSize(buffer));

    PageInit(localpage, BufferGetPageSize(buffer), sizeof(sequence_magic));
    sm = (sequence_magic *) PageGetSpecialPointer(localpage);
    sm->magic = SEQ_MAGIC;

    item = (char *) xlrec + sizeof(xl_seq_rec);
    itemsz = XLogRecGetDataLen(record) - sizeof(xl_seq_rec);

    if (PageAddItem(localpage, (Item) item, itemsz,
                    FirstOffsetNumber, false, false) == InvalidOffsetNumber)
        elog(PANIC, "seq_redo: failed to add item to page");

    PageSetLSN(localpage, lsn);

    memcpy(page, localpage, BufferGetPageSize(buffer));
    MarkBufferDirty(buffer);
    UnlockReleaseBuffer(buffer);

    pfree(localpage);
}

/*
 * Flush cached sequence information.
 */
void
ResetSequenceCaches(void)
{
    if (seqhashtab)
    {
        hash_destroy(seqhashtab);
        seqhashtab = NULL;
    }

    last_used_seq = NULL;

#ifdef __TBASE__
    /* connect GTM to clean up all current session related sequence */
    CleanGTMSeq();
#endif
}

/*
 * Mask a Sequence page before performing consistency checks on it.
 */
void
seq_mask(char *page, BlockNumber blkno)
{
    mask_page_lsn(page);

    mask_unused_space(page);
}

#ifdef __TBASE__
void RenameDatabaseSequence(const char* oldname, const char* newname)
{    
    if (RenameDBSequenceGTM(oldname, newname) < 0)
    {
        ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_FAILURE),
                 errmsg("GTM error, could not rename database sequence")));
    }
}
#endif
