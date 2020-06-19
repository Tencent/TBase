/*-------------------------------------------------------------------------
 *
 * tupdesc.h
 *      POSTGRES tuple descriptor definitions.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/tupdesc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TUPDESC_H
#define TUPDESC_H

#include "access/attnum.h"
#include "catalog/pg_attribute.h"
#include "nodes/pg_list.h"

typedef struct attrDefault
{
    AttrNumber    adnum;
    char       *adbin;            /* nodeToString representation of expr */
} AttrDefault;

typedef struct constrCheck
{
    char       *ccname;
    char       *ccbin;            /* nodeToString representation of expr */
    bool        ccvalid;
    bool        ccnoinherit;    /* this is a non-inheritable constraint */
} ConstrCheck;

#ifdef  _MLS_
typedef struct attrMissing *MissingPtr;
#endif

/* This structure contains constraints of a tuple */
typedef struct tupleConstr
{
    AttrDefault *defval;        /* array */
    ConstrCheck *check;            /* array */
#ifdef _MLS_
    MissingPtr    missing;        /* missing attributes values, NULL if none */
#endif
    uint16        num_defval;
    uint16        num_check;
    bool        has_not_null;
} TupleConstr;

#ifdef _MLS_

/* 
 * append the following structs into tupledesc 
 */
 
typedef struct datamask
{
    int     attmasknum;     /* a copy of nattr */ 
    bool   *mask_array;     /* to record datamask info separately */ 
}Datamask;


typedef struct transp_crypt
{
    int16   algo_id;        /* this algo_id is caculated by FUNC API, default is TRANSP_CRYPT_INVALID_ALGORITHM_ID */   
}TranspCrypt;
#endif

/*
 * This struct is passed around within the backend to describe the structure
 * of tuples.  For tuples coming from on-disk relations, the information is
 * collected from the pg_attribute, pg_attrdef, and pg_constraint catalogs.
 * Transient row types (such as the result of a join query) have anonymous
 * TupleDesc structs that generally omit any constraint info; therefore the
 * structure is designed to let the constraints be omitted efficiently.
 *
 * Note that only user attributes, not system attributes, are mentioned in
 * TupleDesc; with the exception that tdhasoid indicates if OID is present.
 *
 * If the tupdesc is known to correspond to a named rowtype (such as a table's
 * rowtype) then tdtypeid identifies that type and tdtypmod is -1.  Otherwise
 * tdtypeid is RECORDOID, and tdtypmod can be either -1 for a fully anonymous
 * row type, or a value >= 0 to allow the rowtype to be looked up in the
 * typcache.c type cache.
 *
 * Tuple descriptors that live in caches (relcache or typcache, at present)
 * are reference-counted: they can be deleted when their reference count goes
 * to zero.  Tuple descriptors created by the executor need no reference
 * counting, however: they are simply created in the appropriate memory
 * context and go away when the context is freed.  We set the tdrefcount
 * field of such a descriptor to -1, while reference-counted descriptors
 * always have tdrefcount >= 0.
 */
typedef struct tupleDesc
{
    int            natts;            /* number of attributes in the tuple */
    Form_pg_attribute *attrs;
    /* attrs[N] is a pointer to the description of Attribute Number N+1 */
    TupleConstr *constr;        /* constraints, or NULL if none */
    Oid            tdtypeid;        /* composite type ID for tuple type */
    int32        tdtypmod;        /* typmod for tuple type */
    bool        tdhasoid;        /* tuple has oid attribute in its header */
    int            tdrefcount;        /* reference count, or -1 if not counting */
#ifdef _MLS_
    bool        use_attrs_ext;  /* mark if attrs or attrs_ext should be used when form or deform datum */
    int         tdclscol;       /* use for cls feature later */
    Datamask   *tdatamask;      /* record those cols needing to be mask */
    TranspCrypt*transp_crypt;   /* record keys for cols crypt */
    Form_pg_attribute *attrs_ext;/*a similar copy of attrs, while recording plain storage to compress storage */
#endif
}           *TupleDesc;


extern TupleDesc CreateTemplateTupleDesc(int natts, bool hasoid);

extern TupleDesc CreateTupleDesc(int natts, bool hasoid,
                Form_pg_attribute *attrs);

extern TupleDesc CreateTupleDescCopy(TupleDesc tupdesc);

extern TupleDesc CreateTupleDescCopyConstr(TupleDesc tupdesc);

extern void TupleDescCopyEntry(TupleDesc dst, AttrNumber dstAttno,
                   TupleDesc src, AttrNumber srcAttno);

extern void FreeTupleDesc(TupleDesc tupdesc);

extern void IncrTupleDescRefCount(TupleDesc tupdesc);
extern void DecrTupleDescRefCount(TupleDesc tupdesc);

#define PinTupleDesc(tupdesc) \
    do { \
        if ((tupdesc)->tdrefcount >= 0) \
            IncrTupleDescRefCount(tupdesc); \
    } while (0)

#define ReleaseTupleDesc(tupdesc) \
    do { \
        if ((tupdesc)->tdrefcount >= 0) \
            DecrTupleDescRefCount(tupdesc); \
    } while (0)

#ifdef _MLS_
/* Accessor for the i'th attribute of tupdesc. */
#define TupleDescAttr(_tupdesc, _i) ((_tupdesc)->attrs[(_i)])
#endif

extern bool equalTupleDescs(TupleDesc tupdesc1, TupleDesc tupdesc2);

extern void TupleDescInitEntry(TupleDesc desc,
                   AttrNumber attributeNumber,
                   const char *attributeName,
                   Oid oidtypeid,
                   int32 typmod,
                   int attdim);

extern void TupleDescInitBuiltinEntry(TupleDesc desc,
                          AttrNumber attributeNumber,
                          const char *attributeName,
                          Oid oidtypeid,
                          int32 typmod,
                          int attdim);

extern void TupleDescInitEntryCollation(TupleDesc desc,
                            AttrNumber attributeNumber,
                            Oid collationid);

extern TupleDesc BuildDescForRelation(List *schema);

extern TupleDesc BuildDescFromLists(List *names, List *types, List *typmods, List *collations);

#endif                            /* TUPDESC_H */
