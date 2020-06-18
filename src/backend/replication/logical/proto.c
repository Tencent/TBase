/*-------------------------------------------------------------------------
 *
 * proto.c
 *        logical replication protocol functions
 *
 * Copyright (c) 2015-2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *        src/backend/replication/logical/proto.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/sysattr.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "libpq/pqformat.h"
#include "replication/logicalproto.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#ifdef __SUBSCRIPTION__
#include "access/hash.h"
#include "pgxc/locator.h"
#endif

/*
 * Protocol message flags.
 */
#define LOGICALREP_IS_REPLICA_IDENTITY 1

static void logicalrep_write_attrs(StringInfo out, Relation rel);
static void logicalrep_write_tuple(StringInfo out, Relation rel,
#ifdef __SUBSCRIPTION__
                        int32 tuple_hash,
#endif
                        HeapTuple tuple);

static void logicalrep_read_attrs(StringInfo in, LogicalRepRelation *rel);
static void logicalrep_read_tuple(StringInfo in, LogicalRepTupleData *tuple);

static void logicalrep_write_namespace(StringInfo out, Oid nspid);
static const char *logicalrep_read_namespace(StringInfo in);

#ifdef __SUBSCRIPTION__
static int32 logicalrep_dml_hashmod = 0;        /* The number of parallel tbase-sub-subscriptions on the subnet, 
                                                 * used to calculate the hash value when the tuple is sent
                                                 */
static int32 logicalrep_dml_hashvalue = 0;        /* Send Tuple to the subscriber only if the Hash value is equal to this value
                                                  */
static bool     logicalrep_dml_send_all = true;    /* Do I need to send all tuples to the subscriber? */
#endif

/*
 * Write BEGIN to the output stream.
 */
void
logicalrep_write_begin(StringInfo out, ReorderBufferTXN *txn)
{
    pq_sendbyte(out, 'B');        /* BEGIN */

    /* fixed fields */
    pq_sendint64(out, txn->final_lsn);
    pq_sendint64(out, txn->commit_time);
    pq_sendint(out, txn->xid, 4);
}

/*
 * Read transaction BEGIN from the stream.
 */
void
logicalrep_read_begin(StringInfo in, LogicalRepBeginData *begin_data)
{
    /* read fields */
    begin_data->final_lsn = pq_getmsgint64(in);
    if (begin_data->final_lsn == InvalidXLogRecPtr)
        elog(ERROR, "final_lsn not set in begin message");
    begin_data->committime = pq_getmsgint64(in);
    begin_data->xid = pq_getmsgint(in, 4);
}


/*
 * Write COMMIT to the output stream.
 */
void
logicalrep_write_commit(StringInfo out, ReorderBufferTXN *txn,
                        XLogRecPtr commit_lsn)
{
    uint8        flags = 0;

    pq_sendbyte(out, 'C');        /* sending COMMIT */

    /* send the flags field (unused for now) */
    pq_sendbyte(out, flags);

    /* send fields */
    pq_sendint64(out, commit_lsn);
    pq_sendint64(out, txn->end_lsn);
    pq_sendint64(out, txn->commit_time);
}

/*
 * Read transaction COMMIT from the stream.
 */
void
logicalrep_read_commit(StringInfo in, LogicalRepCommitData *commit_data)
{
    /* read flags (unused for now) */
    uint8        flags = pq_getmsgbyte(in);

    if (flags != 0)
        elog(ERROR, "unrecognized flags %u in commit message", flags);

    /* read fields */
    commit_data->commit_lsn = pq_getmsgint64(in);
    commit_data->end_lsn = pq_getmsgint64(in);
    commit_data->committime = pq_getmsgint64(in);
}

/*
 * Write ORIGIN to the output stream.
 */
void
logicalrep_write_origin(StringInfo out, const char *origin,
                        XLogRecPtr origin_lsn)
{
    pq_sendbyte(out, 'O');        /* ORIGIN */

    /* fixed fields */
    pq_sendint64(out, origin_lsn);

    /* origin string */
    pq_sendstring(out, origin);
}

/*
 * Read ORIGIN from the output stream.
 */
char *
logicalrep_read_origin(StringInfo in, XLogRecPtr *origin_lsn)
{
    /* fixed fields */
    *origin_lsn = pq_getmsgint64(in);

    /* return origin */
    return pstrdup(pq_getmsgstring(in));
}

/*
 * Write INSERT to the output stream.
 */
void
logicalrep_write_insert(StringInfo out, Relation rel,
#ifdef __SUBSCRIPTION__
                        int32 tuple_hash,
#endif
                        HeapTuple newtuple)
{
    pq_sendbyte(out, 'I');        /* action INSERT */

    Assert(rel->rd_rel->relreplident == REPLICA_IDENTITY_DEFAULT ||
           rel->rd_rel->relreplident == REPLICA_IDENTITY_FULL ||
           rel->rd_rel->relreplident == REPLICA_IDENTITY_INDEX);

    /* use Oid as relation identifier */
    pq_sendint(out, RelationGetRelid(rel), 4);

#ifdef __SUBSCRIPTION__
    do
    {
        char * relname = NULL;

        /* send qualified relation name */
        logicalrep_write_namespace(out, RelationGetNamespace(rel));
        relname = RelationGetRelationName(rel);
        pq_sendstring(out, relname);

        /* send replica identity */
        pq_sendbyte(out, rel->rd_rel->relreplident);
    } while (0);
#endif

    pq_sendbyte(out, 'N');        /* new tuple follows */
    logicalrep_write_tuple(out, rel,
        #ifdef __SUBSCRIPTION__
        tuple_hash,
        #endif
        newtuple);
}

/*
 * Read INSERT from stream.
 *
 * Fills the new tuple.
 */
LogicalRepRelId
logicalrep_read_insert(StringInfo in, 
#ifdef __SUBSCRIPTION__
                        char **nspname, char **relname, char *replident,
#endif
                        LogicalRepTupleData *newtup)
{
    char        action;
    LogicalRepRelId relid;

    /* read the relation id */
    relid = pq_getmsgint(in, 4);

#ifdef __SUBSCRIPTION__
    /* Read relation namespace from stream */
    if (nspname != NULL)
    {
        *nspname = pstrdup(logicalrep_read_namespace(in));
    }
    else
    {
        logicalrep_read_namespace(in);
    }

    /* Read relation name from stream */
    if (relname != NULL)
    {
        *relname = pstrdup(pq_getmsgstring(in));
    }
    else
    {
        pq_getmsgstring(in);
    }

    /* Read the replica identity. */
    if (replident != NULL)
    {
        *replident = pq_getmsgbyte(in);
    }
    else
    {
        pq_getmsgbyte(in);
    }
#endif

    action = pq_getmsgbyte(in);
    if (action != 'N')
        elog(ERROR, "expected new tuple but got %d",
             action);

    logicalrep_read_tuple(in, newtup);

    return relid;
}

/*
 * Write UPDATE to the output stream.
 */
void
logicalrep_write_update(StringInfo out, Relation rel, 
#ifdef __SUBSCRIPTION__
                        int32 tuple_hash,
#endif
                        HeapTuple oldtuple,
                        HeapTuple newtuple)
{// #lizard forgives    
    pq_sendbyte(out, 'U');        /* action UPDATE */

    Assert(rel->rd_rel->relreplident == REPLICA_IDENTITY_DEFAULT ||
           rel->rd_rel->relreplident == REPLICA_IDENTITY_FULL ||
           rel->rd_rel->relreplident == REPLICA_IDENTITY_INDEX);

    /* use Oid as relation identifier */
    pq_sendint(out, RelationGetRelid(rel), 4);

#ifdef __SUBSCRIPTION__
    do
    {
        char * relname = NULL;

        /* send qualified relation name */
        logicalrep_write_namespace(out, RelationGetNamespace(rel));
        relname = RelationGetRelationName(rel);
        pq_sendstring(out, relname);

        /* send replica identity */
        pq_sendbyte(out, rel->rd_rel->relreplident);
    } while (0);
#endif

    if (oldtuple != NULL)
    {
        if (rel->rd_rel->relreplident == REPLICA_IDENTITY_FULL)
            pq_sendbyte(out, 'O');    /* old tuple follows */
        else
            pq_sendbyte(out, 'K');    /* old key follows */
        logicalrep_write_tuple(out, rel, 
            #ifdef __SUBSCRIPTION__
            tuple_hash,
            #endif
            oldtuple);
    }

    pq_sendbyte(out, 'N');        /* new tuple follows */
    logicalrep_write_tuple(out, rel, 
        #ifdef __SUBSCRIPTION__
        tuple_hash,
        #endif
        newtuple);
}

/*
 * Read UPDATE from stream.
 */
LogicalRepRelId
logicalrep_read_update(StringInfo in, 
#ifdef __SUBSCRIPTION__
                       char **nspname, char **relname, char *replident,
#endif
                       bool *has_oldtuple,
                       LogicalRepTupleData *oldtup,
                       LogicalRepTupleData *newtup)
{// #lizard forgives
    char        action;
    LogicalRepRelId relid;

    /* read the relation id */
    relid = pq_getmsgint(in, 4);

#ifdef __SUBSCRIPTION__
    /* Read relation namespace from stream */
    if (nspname != NULL)
    {
        *nspname = pstrdup(logicalrep_read_namespace(in));
    }
    else
    {
        logicalrep_read_namespace(in);
    }

    /* Read relation name from stream */
    if (relname != NULL)
    {
        *relname = pstrdup(pq_getmsgstring(in));
    }
    else
    {
        pq_getmsgstring(in);
    }

    /* Read the replica identity. */
    if (replident != NULL)
    {
        *replident = pq_getmsgbyte(in);
    }
    else
    {
        pq_getmsgbyte(in);
    }
#endif

    /* read and verify action */
    action = pq_getmsgbyte(in);
    if (action != 'K' && action != 'O' && action != 'N')
        elog(ERROR, "expected action 'N', 'O' or 'K', got %c",
             action);

    /* check for old tuple */
    if (action == 'K' || action == 'O')
    {
        logicalrep_read_tuple(in, oldtup);
        *has_oldtuple = true;

        action = pq_getmsgbyte(in);
    }
    else
        *has_oldtuple = false;

    /* check for new  tuple */
    if (action != 'N')
        elog(ERROR, "expected action 'N', got %c",
             action);

    logicalrep_read_tuple(in, newtup);

    return relid;
}

/*
 * Write DELETE to the output stream.
 */
void
logicalrep_write_delete(StringInfo out, Relation rel,
#ifdef __SUBSCRIPTION__
                        int32 tuple_hash,
#endif
                        HeapTuple oldtuple)
{
    Assert(rel->rd_rel->relreplident == REPLICA_IDENTITY_DEFAULT ||
           rel->rd_rel->relreplident == REPLICA_IDENTITY_FULL ||
           rel->rd_rel->relreplident == REPLICA_IDENTITY_INDEX);

    pq_sendbyte(out, 'D');        /* action DELETE */

    /* use Oid as relation identifier */
    pq_sendint(out, RelationGetRelid(rel), 4);

#ifdef __SUBSCRIPTION__
    do
    {
        char * relname = NULL;

        /* send qualified relation name */
        logicalrep_write_namespace(out, RelationGetNamespace(rel));
        relname = RelationGetRelationName(rel);
        pq_sendstring(out, relname);

        /* send replica identity */
        pq_sendbyte(out, rel->rd_rel->relreplident);
    } while (0);
#endif

    if (rel->rd_rel->relreplident == REPLICA_IDENTITY_FULL)
        pq_sendbyte(out, 'O');    /* old tuple follows */
    else
        pq_sendbyte(out, 'K');    /* old key follows */

    logicalrep_write_tuple(out, rel, 
        #ifdef __SUBSCRIPTION__
        tuple_hash,
        #endif
        oldtuple);
}

/*
 * Read DELETE from stream.
 *
 * Fills the old tuple.
 */
LogicalRepRelId
logicalrep_read_delete(StringInfo in, 
#ifdef __SUBSCRIPTION__
                       char **nspname, char **relname, char *replident,
#endif
                        LogicalRepTupleData *oldtup)
{
    char        action;
    LogicalRepRelId relid;

    /* read the relation id */
    relid = pq_getmsgint(in, 4);

#ifdef __SUBSCRIPTION__
    /* Read relation namespace from stream */
    if (nspname != NULL)
    {
        *nspname = pstrdup(logicalrep_read_namespace(in));
    }
    else
    {
        logicalrep_read_namespace(in);
    }

    /* Read relation name from stream */
    if (relname != NULL)
    {
        *relname = pstrdup(pq_getmsgstring(in));
    }
    else
    {
        pq_getmsgstring(in);
    }

    /* Read the replica identity. */
    if (replident != NULL)
    {
        *replident = pq_getmsgbyte(in);
    }
    else
    {
        pq_getmsgbyte(in);
    }
#endif

    /* read and verify action */
    action = pq_getmsgbyte(in);
    if (action != 'K' && action != 'O')
        elog(ERROR, "expected action 'O' or 'K', got %c", action);

    logicalrep_read_tuple(in, oldtup);

    return relid;
}

/*
 * Write relation description to the output stream.
 */
void
logicalrep_write_rel(StringInfo out, Relation rel)
{
    char       *relname;

    pq_sendbyte(out, 'R');        /* sending RELATION */

    /* use Oid as relation identifier */
    pq_sendint(out, RelationGetRelid(rel), 4);

    /* send qualified relation name */
    logicalrep_write_namespace(out, RelationGetNamespace(rel));
    relname = RelationGetRelationName(rel);
    pq_sendstring(out, relname);

    /* send replica identity */
    pq_sendbyte(out, rel->rd_rel->relreplident);

    /* send the attribute info */
    logicalrep_write_attrs(out, rel);
}

/*
 * Read the relation info from stream and return as LogicalRepRelation.
 */
LogicalRepRelation *
logicalrep_read_rel(StringInfo in)
{
	LogicalRepRelation *rel = NULL;

	rel = palloc0(sizeof(LogicalRepRelation));
    rel->remoteid = pq_getmsgint(in, 4);

    /* Read relation name from stream */
    rel->nspname = pstrdup(logicalrep_read_namespace(in));
    rel->relname = pstrdup(pq_getmsgstring(in));

    /* Read the replica identity. */
    rel->replident = pq_getmsgbyte(in);

    /* Get attribute description */
    logicalrep_read_attrs(in, rel);

    return rel;
}

#ifdef __SUBSCRIPTION__
void logicalrep_relation_free(LogicalRepRelation * rel)
{
	if (rel != NULL)
	{
		if (rel->attnames != NULL)
		{
			if (rel->natts > 0)
			{
				int	i = 0;
				int	natts = 0;

				natts = rel->natts;
				for (i = 0; i < natts; i++)
				{
					if (rel->attnames[i] != NULL)
					{
						pfree(rel->attnames[i]);
						rel->attnames[i] = NULL;
					}
				}
			}

			pfree(rel->attnames);
			rel->attnames = NULL;
		}

		if (rel->atttyps != NULL)
		{
			pfree(rel->atttyps);
			rel->atttyps = NULL;
		}

		if (rel->attkeys != NULL)
		{
			bms_free(rel->attkeys);
			rel->attkeys = NULL;
		}

		if (rel->relname != NULL)
		{
			pfree(rel->relname);
			rel->relname = NULL;
		}

		if (rel->nspname != NULL)
		{
			pfree(rel->nspname);
			rel->nspname = NULL;
		}

		pfree(rel);
		rel = NULL;
	}
}
#endif

/*
 * Write type info to the output stream.
 *
 * This function will always write base type info.
 */
void
logicalrep_write_typ(StringInfo out, Oid typoid)
{
    Oid            basetypoid = getBaseType(typoid);
    HeapTuple    tup;
    Form_pg_type typtup;

    pq_sendbyte(out, 'Y');        /* sending TYPE */

    tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(basetypoid));
    if (!HeapTupleIsValid(tup))
        elog(ERROR, "cache lookup failed for type %u", basetypoid);
    typtup = (Form_pg_type) GETSTRUCT(tup);

    /* use Oid as relation identifier */
    pq_sendint(out, typoid, 4);

    /* send qualified type name */
    logicalrep_write_namespace(out, typtup->typnamespace);
    pq_sendstring(out, NameStr(typtup->typname));

    ReleaseSysCache(tup);
}

/*
 * Read type info from the output stream.
 */
void
logicalrep_read_typ(StringInfo in, LogicalRepTyp *ltyp)
{
    ltyp->remoteid = pq_getmsgint(in, 4);

    /* Read type name from stream */
    ltyp->nspname = pstrdup(logicalrep_read_namespace(in));
    ltyp->typname = pstrdup(pq_getmsgstring(in));
}

/*
 * Write a tuple to the outputstream, in the most efficient format possible.
 */
static void
logicalrep_write_tuple(StringInfo out, Relation rel, 
#ifdef __SUBSCRIPTION__
                        int32 tuple_hash,
#endif
                        HeapTuple tuple)
{// #lizard forgives
    TupleDesc    desc;
    Datum        values[MaxTupleAttributeNumber];
    bool        isnull[MaxTupleAttributeNumber];
    int            i;
    uint16        nliveatts = 0;

    desc = RelationGetDescr(rel);

    for (i = 0; i < desc->natts; i++)
    {
        if (desc->attrs[i]->attisdropped)
            continue;
        nliveatts++;
    }

#ifdef __SUBSCRIPTION__
    /* write tuple hash value first */
    pq_sendint(out, tuple_hash, 4);
#endif

    pq_sendint(out, nliveatts, 2);

    /* try to allocate enough memory from the get-go */
    enlargeStringInfo(out, tuple->t_len +
                      nliveatts * (1 + 4));

    heap_deform_tuple(tuple, desc, values, isnull);

    /* Write the values */
    for (i = 0; i < desc->natts; i++)
    {
        HeapTuple    typtup;
        Form_pg_type typclass;
        Form_pg_attribute att = desc->attrs[i];
        char       *outputstr;

        /* skip dropped columns */
        if (att->attisdropped)
            continue;

        if (isnull[i])
        {
            pq_sendbyte(out, 'n');    /* null column */
            continue;
        }
        else if (att->attlen == -1 && VARATT_IS_EXTERNAL_ONDISK(values[i]))
        {
            pq_sendbyte(out, 'u');    /* unchanged toast column */
            continue;
        }

        typtup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(att->atttypid));
        if (!HeapTupleIsValid(typtup))
            elog(ERROR, "cache lookup failed for type %u", att->atttypid);
        typclass = (Form_pg_type) GETSTRUCT(typtup);

        pq_sendbyte(out, 't');    /* 'text' data follows */

        outputstr = OidOutputFunctionCall(typclass->typoutput, values[i]);
        pq_sendcountedtext(out, outputstr, strlen(outputstr), false);
        pfree(outputstr);

        ReleaseSysCache(typtup);
    }
}

/*
 * Read tuple in remote format from stream.
 *
 * The returned tuple points into the input stringinfo.
 */
static void
logicalrep_read_tuple(StringInfo in, LogicalRepTupleData *tuple)
{
    int            i;
    int            natts;

#ifdef __SUBSCRIPTION__
    /* read tuple hash value first */
    tuple->tuple_hash = pq_getmsgint(in, 4);
#endif

    /* Get number of attributes */
    natts = pq_getmsgint(in, 2);

    memset(tuple->changed, 0, sizeof(tuple->changed));

    /* Read the data */
    for (i = 0; i < natts; i++)
    {
        char        kind;

        kind = pq_getmsgbyte(in);

        switch (kind)
        {
            case 'n':            /* null */
                tuple->values[i] = NULL;
                tuple->changed[i] = true;
                break;
            case 'u':            /* unchanged column */
                /* we don't receive the value of an unchanged column */
                tuple->values[i] = NULL;
                break;
            case 't':            /* text formatted value */
                {
                    int            len;

                    tuple->changed[i] = true;

                    len = pq_getmsgint(in, 4);    /* read length */

                    /* and data */
                    tuple->values[i] = palloc(len + 1);
                    pq_copymsgbytes(in, tuple->values[i], len);
                    tuple->values[i][len] = '\0';
                }
                break;
            default:
                elog(ERROR, "unrecognized data representation type '%c'", kind);
        }
    }
}

/*
 * Write relation attributes to the stream.
 */
static void
logicalrep_write_attrs(StringInfo out, Relation rel)
{// #lizard forgives
    TupleDesc    desc;
    int            i;
    uint16        nliveatts = 0;
    Bitmapset  *idattrs = NULL;
    bool        replidentfull;

    desc = RelationGetDescr(rel);

    /* send number of live attributes */
    for (i = 0; i < desc->natts; i++)
    {
        if (desc->attrs[i]->attisdropped)
            continue;
        nliveatts++;
    }
    pq_sendint(out, nliveatts, 2);

    /* fetch bitmap of REPLICATION IDENTITY attributes */
    replidentfull = (rel->rd_rel->relreplident == REPLICA_IDENTITY_FULL);
    if (!replidentfull)
        idattrs = RelationGetIndexAttrBitmap(rel,
                                             INDEX_ATTR_BITMAP_IDENTITY_KEY);

    /* send the attributes */
    for (i = 0; i < desc->natts; i++)
    {
        Form_pg_attribute att = desc->attrs[i];
        uint8        flags = 0;

        if (att->attisdropped)
            continue;

        /* REPLICA IDENTITY FULL means all columns are sent as part of key. */
        if (replidentfull ||
            bms_is_member(att->attnum - FirstLowInvalidHeapAttributeNumber,
                          idattrs))
            flags |= LOGICALREP_IS_REPLICA_IDENTITY;

        pq_sendbyte(out, flags);

        /* attribute name */
        pq_sendstring(out, NameStr(att->attname));

        /* attribute type id */
        pq_sendint(out, (int) att->atttypid, sizeof(att->atttypid));

        /* attribute mode */
        pq_sendint(out, att->atttypmod, sizeof(att->atttypmod));
    }

    bms_free(idattrs);
}

/*
 * Read relation attribute names from the stream.
 */
static void
logicalrep_read_attrs(StringInfo in, LogicalRepRelation *rel)
{
	int			i = 0;
	int			natts = 0;
	char	  **attnames = NULL;
	Oid		   *atttyps = NULL;
    Bitmapset  *attkeys = NULL;

    natts = pq_getmsgint(in, 2);
	attnames = palloc0(natts * sizeof(char *));
	atttyps = palloc0(natts * sizeof(Oid));

    /* read the attributes */
    for (i = 0; i < natts; i++)
    {
		uint8		flags = 0;

        /* Check for replica identity column */
        flags = pq_getmsgbyte(in);
        if (flags & LOGICALREP_IS_REPLICA_IDENTITY)
            attkeys = bms_add_member(attkeys, i);

        /* attribute name */
        attnames[i] = pstrdup(pq_getmsgstring(in));

        /* attribute type id */
        atttyps[i] = (Oid) pq_getmsgint(in, 4);

        /* we ignore attribute mode for now */
        (void) pq_getmsgint(in, 4);
    }

    rel->attnames = attnames;
    rel->atttyps = atttyps;
    rel->attkeys = attkeys;
    rel->natts = natts;
}

/*
 * Write the namespace name or empty string for pg_catalog (to save space).
 */
static void
logicalrep_write_namespace(StringInfo out, Oid nspid)
{
    if (nspid == PG_CATALOG_NAMESPACE)
        pq_sendbyte(out, '\0');
    else
    {
        char       *nspname = get_namespace_name(nspid);

        if (nspname == NULL)
            elog(ERROR, "cache lookup failed for namespace %u",
                 nspid);

        pq_sendstring(out, nspname);
    }
}

/*
 * Read the namespace name while treating empty string as pg_catalog.
 */
static const char *
logicalrep_read_namespace(StringInfo in)
{
    const char *nspname = pq_getmsgstring(in);

    if (nspname[0] == '\0')
        nspname = "pg_catalog";

    return nspname;
}

#ifdef __SUBSCRIPTION__
void logicalrep_dml_set_hashmod(int32 sub_parallel_number)
{
    Assert(sub_parallel_number >= 1);
    if (!(sub_parallel_number >= 1)) abort();
    logicalrep_dml_hashmod = sub_parallel_number;
}

void logicalrep_dml_set_hashvalue(int32 sub_parallel_index)
{
    Assert(sub_parallel_index >= 0);
    if (!(sub_parallel_index >= 0)) abort();
    logicalrep_dml_hashvalue = sub_parallel_index;
}

void logicalrep_dml_set_send_all(bool send_all)
{
    logicalrep_dml_send_all = send_all;
}

int32 logicalrep_dml_get_hashmod(void)
{
    return logicalrep_dml_hashmod;
}

int32 logicalrep_dml_get_hashvalue(void)
{
    return logicalrep_dml_hashvalue;
}

bool logicalrep_dml_get_send_all(void)
{
    return logicalrep_dml_send_all;
}

int32 logicalrep_dml_calc_hash(Relation rel, HeapTuple tuple)
{// #lizard forgives
    TupleDesc    desc = NULL;
    Bitmapset  *idattrs = NULL;
    bool        replidentfull = false;
    int            i = 0;

    Datum        values[MaxTupleAttributeNumber] = { 0 };
    bool        isnull[MaxTupleAttributeNumber] = { false };

    uint64        sum = 0;
    int32        ret = 0;

    desc = RelationGetDescr(rel);
    heap_deform_tuple(tuple, desc, values, isnull);

    /* fetch bitmap of REPLICATION IDENTITY attributes */
    replidentfull = (rel->rd_rel->relreplident == REPLICA_IDENTITY_FULL);
    if (!replidentfull)
        idattrs = RelationGetIndexAttrBitmap(rel, INDEX_ATTR_BITMAP_IDENTITY_KEY);

    /* travers the attributes */
    for (i = 0; i < desc->natts; i++)
    {
        Form_pg_attribute att = desc->attrs[i];

        if (att->attisdropped == true || isnull[i] == true)
            continue;

        /* REPLICA IDENTITY FULL means all columns are sent as part of key. */
        if (replidentfull ||
            bms_is_member(att->attnum - FirstLowInvalidHeapAttributeNumber,
                          idattrs))
        {
            Datum    attr_hash_datum = compute_hash(att->atttypid, values[i], LOCATOR_TYPE_HASH);
            uint32    attr_hash_uint32 = DatumGetUInt32(attr_hash_datum);
            sum += attr_hash_uint32;
        }
    }

    ret = sum % logicalrep_dml_hashmod;
    Assert(ret >= 0 && ret < logicalrep_dml_hashmod);
    if (!(ret >= 0 && ret < logicalrep_dml_hashmod)) abort();

    return ret;
}

bool AmTbaseSubscriptionWalSender(void)
{
    return (IS_PGXC_DATANODE && logicalrep_dml_hashmod > 0);
}
#endif

