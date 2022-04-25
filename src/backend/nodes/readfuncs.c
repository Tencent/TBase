/*-------------------------------------------------------------------------
 *
 * readfuncs.c
 *      Reader functions for Postgres tree nodes.
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *      src/backend/nodes/readfuncs.c
 *
 * NOTES
 *      Path nodes do not have any readfuncs support, because we never
 *      have occasion to read them in.  (There was once code here that
 *      claimed to read them, but it was broken as well as unused.)  We
 *      never read executor state trees, either.
 *
 *      Parse location fields are written out by outfuncs.c, but only for
 *      possible debugging use.  When reading a location field, we discard
 *      the stored value and set the location field to -1 (ie, "unknown").
 *      This is because nodes coming from a stored rule should not be thought
 *      to have a known location in the current query's text.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "fmgr.h"
#include "nodes/extensible.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "nodes/readfuncs.h"
#ifdef PGXC
#include "access/htup.h"
#endif
#ifdef XCP
#include "fmgr.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "nodes/plannodes.h"
#include "pgxc/execRemote.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

#ifdef __AUDIT_FGA__
#include "audit/audit_fga.h"
#endif


#ifdef __AUDIT__
#include "audit/audit.h"
#endif

#ifdef __TBASE__
#include "catalog/pg_constraint_fn.h"
#include "commands/defrem.h"
#include "catalog/pg_am.h"
#endif
/*
 * When we sending query plans between nodes we need to send OIDs of various
 * objects - relations, data types, functions, etc.
 * On different nodes OIDs of these objects may differ, so we need to send an
 * identifier, depending on object type, allowing to lookup OID on target node.
 * On the other hand we want to save space when storing rules, or in other cases
 * when we need to encode and decode nodes on the same node.
 * For now default format is not portable, as it is in original Postgres code.
 * Later we may want to add extra parameter in stringToNode() function
 */
static bool portable_input = false;
bool
set_portable_input(bool value)
{
    bool old_portable_input = portable_input;
    portable_input = value;
    return old_portable_input;
}
#endif /* XCP */

/*
 * Macros to simplify reading of different kinds of fields.  Use these
 * wherever possible to reduce the chance for silly typos.  Note that these
 * hard-wire conventions about the names of the local variables in a Read
 * routine.
 */

/* Macros for declaring appropriate local variables */

/* A few guys need only local_node */
#define READ_LOCALS_NO_FIELDS(nodeTypeName) \
    nodeTypeName *local_node = makeNode(nodeTypeName)

/* And a few guys need only the pg_strtok support fields */
#define READ_TEMP_LOCALS()    \
    char       *token;        \
    int            length

/* ... but most need both */
#define READ_LOCALS(nodeTypeName)            \
    READ_LOCALS_NO_FIELDS(nodeTypeName);    \
    READ_TEMP_LOCALS()

/* Read an integer field (anything written as ":fldname %d") */
#define READ_INT_FIELD(fldname) \
    token = pg_strtok(&length);        /* skip :fldname */ \
    token = pg_strtok(&length);        /* get field value */ \
    local_node->fldname = atoi(token)

/* Read an unsigned integer field (anything written as ":fldname %u") */
#define READ_UINT_FIELD(fldname) \
    token = pg_strtok(&length);        /* skip :fldname */ \
    token = pg_strtok(&length);        /* get field value */ \
    local_node->fldname = atoui(token)

/* Read an integer field (anything written as ":fldname %d") */
#define READ_INT64_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = atoll(token)

#ifdef XCP
/* Read a long integer field (anything written as ":fldname %ld") */
#define READ_LONG_FIELD(fldname) \
    token = pg_strtok(&length);        /* skip :fldname */ \
    token = pg_strtok(&length);        /* get field value */ \
    local_node->fldname = atol(token)
#endif

/* Read an OID field (don't hard-wire assumption that OID is same as uint) */
#ifdef XCP
#define READ_OID_FIELD(fldname) \
    (AssertMacro(!portable_input),     /* only allow to read OIDs within a node */ \
     token = pg_strtok(&length),    /* skip :fldname */ \
     token = pg_strtok(&length),    /* get field value */ \
     local_node->fldname = atooid(token))
#else
#define READ_OID_FIELD(fldname) \
    token = pg_strtok(&length);        /* skip :fldname */ \
    token = pg_strtok(&length);        /* get field value */ \
    local_node->fldname = atooid(token)
#endif

/* Read a char field (ie, one ascii character) */
#define READ_CHAR_FIELD(fldname) \
    token = pg_strtok(&length);        /* skip :fldname */ \
    token = pg_strtok(&length);        /* get field value */ \
    /* avoid overhead of calling debackslash() for one char */ \
    local_node->fldname = (length == 0) ? '\0' : (token[0] == '\\' ? token[1] : token[0])

/* Read an enumerated-type field that was written as an integer code */
#define READ_ENUM_FIELD(fldname, enumtype) \
    token = pg_strtok(&length);        /* skip :fldname */ \
    token = pg_strtok(&length);        /* get field value */ \
    local_node->fldname = (enumtype) atoi(token)

/* Read a float field */
#define READ_FLOAT_FIELD(fldname) \
    token = pg_strtok(&length);        /* skip :fldname */ \
    token = pg_strtok(&length);        /* get field value */ \
    local_node->fldname = atof(token)

/* Read a boolean field */
#define READ_BOOL_FIELD(fldname) \
    token = pg_strtok(&length);        /* skip :fldname */ \
    token = pg_strtok(&length);        /* get field value */ \
    local_node->fldname = strtobool(token)

/* Read a character-string field */
#define READ_STRING_FIELD(fldname) \
    token = pg_strtok(&length);        /* skip :fldname */ \
    token = pg_strtok(&length);        /* get field value */ \
    local_node->fldname = nullable_string(token, length)

/* Read a parse location field (and throw away the value, per notes above) */
#define READ_LOCATION_FIELD(fldname) \
    token = pg_strtok(&length);        /* skip :fldname */ \
    token = pg_strtok(&length);        /* get field value */ \
    (void) token;                /* in case not used elsewhere */ \
    local_node->fldname = -1    /* set field to "unknown" */

/* Read a Node field */
#define READ_NODE_FIELD(fldname) \
    do { \
        token = pg_strtok(&length);        /* skip :fldname */ \
        (void) token;                /* in case not used elsewhere */ \
        local_node->fldname = nodeRead(NULL, 0); \
    } while (0)

/* Read a bitmapset field */
#define READ_BITMAPSET_FIELD(fldname) \
    token = pg_strtok(&length);        /* skip :fldname */ \
    (void) token;                /* in case not used elsewhere */ \
    local_node->fldname = _readBitmapset()

#ifdef XCP
/*
 * Macros to read an identifier and lookup the OID
 * The identifier depends on object type.
 */
#define NSP_OID(nspname) LookupNamespaceNoError(nspname)

/* Read relation identifier and lookup the OID */
#define READ_RELID_INTERNAL(relid, warn) \
    do { \
        char       *nspname; /* namespace name */ \
        char       *relname; /* relation name */ \
        token = pg_strtok(&length); /* get nspname */ \
        nspname = nullable_string(token, length); \
        token = pg_strtok(&length); /* get relname */ \
        relname = nullable_string(token, length); \
        if (relname) \
        { \
            relid = get_relname_relid(relname, \
                                                    NSP_OID(nspname)); \
            if (!OidIsValid((relid)) && (warn)) \
                elog(WARNING, "could not find OID for relation %s.%s", nspname,\
                        relname); \
        } \
        else \
            relid = InvalidOid; \
    } while (0)

#define READ_RELID_FIELD_NOWARN(fldname) \
    do { \
        Oid relid; \
        token = pg_strtok(&length);        /* skip :fldname */ \
        READ_RELID_INTERNAL(relid, false); \
        local_node->fldname = relid; \
    } while (0)

#define READ_RELID_FIELD(fldname) \
    do { \
        Oid relid; \
        token = pg_strtok(&length);        /* skip :fldname */ \
        READ_RELID_INTERNAL(relid, true); \
        local_node->fldname = relid; \
    } while (0)

#define READ_RELID_LIST_FIELD(fldname) \
    do { \
        token = pg_strtok(&length);        /* skip :fldname */ \
        token = pg_strtok(&length);     /* skip '(' */ \
        if (length > 0 ) \
        { \
            Assert(token[0] == '('); \
            for (;;) \
            { \
                Oid relid; \
                READ_RELID_INTERNAL(relid, true); \
                local_node->fldname = lappend_oid(local_node->fldname, relid); \
                token = pg_strtok(&length); \
                if (token[0] == ')') \
                break; \
            } \
        } \
        else \
            local_node->fldname = NIL; \
    } while (0)

/* Read data type identifier and lookup the OID */
#define READ_TYPID_INTERNAL(typid) \
    do { \
        char       *nspname; /* namespace name */ \
        char       *typname; /* data type name */ \
        token = pg_strtok(&length); /* get nspname */ \
        nspname = nullable_string(token, length); \
        token = pg_strtok(&length); /* get typname */ \
        typname = nullable_string(token, length); \
        if (typname) \
        { \
            typid = get_typname_typid(typname, \
                                        NSP_OID(nspname)); \
            if (!OidIsValid((typid))) \
                elog(WARNING, "could not find OID for type %s.%s", nspname,\
                        typname); \
        } \
        else \
            typid = InvalidOid; \
    } while (0)

#define READ_TYPID_FIELD(fldname) \
    do { \
        Oid typid; \
        token = pg_strtok(&length);        /* skip :fldname */ \
        READ_TYPID_INTERNAL(typid); \
        local_node->fldname = typid; \
    } while (0)

#define READ_TYPID_LIST_FIELD(fldname) \
    do { \
        token = pg_strtok(&length);        /* skip :fldname */ \
        token = pg_strtok(&length);     /* skip '(' */ \
        if (length > 0 ) \
        { \
            Assert(token[0] == '('); \
            for (;;) \
            { \
                Oid typid; \
                READ_TYPID_INTERNAL(typid); \
                local_node->fldname = lappend_oid(local_node->fldname, typid); \
                token = pg_strtok(&length); \
                if (token[0] == ')') \
                break; \
            } \
        } \
        else \
            local_node->fldname = NIL; \
    } while (0)

/* Read function identifier and lookup the OID */
#define READ_FUNCID_FIELD(fldname) \
    do { \
        char       *nspname; /* namespace name */ \
        char       *funcname; /* function name */ \
        int         nargs; /* number of arguments */ \
        Oid           *argtypes; /* argument types */ \
        token = pg_strtok(&length);        /* skip :fldname */ \
        token = pg_strtok(&length); /* get nspname */ \
        nspname = nullable_string(token, length); \
        token = pg_strtok(&length); /* get funcname */ \
        funcname = nullable_string(token, length); \
        token = pg_strtok(&length); /* get nargs */ \
        nargs = atoi(token); \
        if (funcname) \
        { \
            int    i; \
            argtypes = palloc(nargs * sizeof(Oid)); \
            for (i = 0; i < nargs; i++) \
            { \
                char *typnspname; /* argument type namespace */ \
                char *typname; /* argument type name */ \
                token = pg_strtok(&length); /* get type nspname */ \
                typnspname = nullable_string(token, length); \
                token = pg_strtok(&length); /* get type name */ \
                typname = nullable_string(token, length); \
                argtypes[i] = get_typname_typid(typname, \
                                                NSP_OID(typnspname)); \
            } \
            local_node->fldname = get_funcid(funcname, \
                                             buildoidvector(argtypes, nargs), \
                                             NSP_OID(nspname)); \
        } \
        else \
            local_node->fldname = InvalidOid; \
    } while (0)

/* Read operator identifier and lookup the OID */
#define READ_OPERID_FIELD(fldname) \
    do { \
        char       *nspname; /* namespace name */ \
        char       *oprname; /* operator name */ \
        char       *leftnspname; /* left type namespace */ \
        char       *leftname; /* left type name */ \
        Oid            oprleft; /* left type */ \
        char       *rightnspname; /* right type namespace */ \
        char       *rightname; /* right type name */ \
        Oid            oprright; /* right type */ \
        token = pg_strtok(&length);        /* skip :fldname */ \
        token = pg_strtok(&length); /* get nspname */ \
        nspname = nullable_string(token, length); \
        token = pg_strtok(&length); /* get operator name */ \
        oprname = nullable_string(token, length); \
        token = pg_strtok(&length); /* left type namespace */ \
        leftnspname = nullable_string(token, length); \
        token = pg_strtok(&length); /* left type name */ \
        leftname = nullable_string(token, length); \
        token = pg_strtok(&length); /* right type namespace */ \
        rightnspname = nullable_string(token, length); \
        token = pg_strtok(&length); /* right type name */ \
        rightname = nullable_string(token, length); \
        if (oprname) \
        { \
            if (leftname) \
                oprleft = get_typname_typid(leftname, \
                                            NSP_OID(leftnspname)); \
            else \
                oprleft = InvalidOid; \
            if (rightname) \
                oprright = get_typname_typid(rightname, \
                                             NSP_OID(rightnspname)); \
            else \
                oprright = InvalidOid; \
            local_node->fldname = get_operid(oprname, \
                                             oprleft, \
                                             oprright, \
                                             NSP_OID(nspname)); \
        } \
        else \
            local_node->fldname = InvalidOid; \
    } while (0)

/* Read collation identifier and lookup the OID */
#define READ_COLLID_FIELD(fldname) \
    do { \
        char       *nspname; /* namespace name */ \
        char       *collname; /* collation name */ \
        int         collencoding; /* collation encoding */ \
        token = pg_strtok(&length);        /* skip :fldname */ \
        token = pg_strtok(&length); /* get nspname */ \
        nspname = nullable_string(token, length); \
        token = pg_strtok(&length); /* get collname */ \
        collname = nullable_string(token, length); \
        token = pg_strtok(&length); /* get collencoding */ \
        collencoding = atoi(token); \
        if (collname) \
            local_node->fldname = get_collid(collname, \
                                             collencoding, \
                                             NSP_OID(nspname)); \
        else \
            local_node->fldname = InvalidOid; \
    } while (0)

#define READ_OPCLASS_FIELD(fldname) \
    do { \
        char *nspname; \
        char *name; \
        token = pg_strtok(&length);         \
        token = pg_strtok(&length); \
        nspname = nullable_string(token, length); \
        token = pg_strtok(&length);  \
        name = nullable_string(token, length); \
        if (nspname) \
        {\
            List *opclassname = list_make1(makeString(nspname)); \
            lappend(opclassname, makeString(name)); \
            local_node->fldname = get_opclass_oid(BTREE_AM_OID, \
                                                   opclassname, false); \
        } \
        else \
            local_node->fldname = InvalidOid; \
    } while (0)

#define READ_CONSTRAINT_FIELD(fldname) \
    do { \
        char *cons_name; \
        char *rel_namespace; \
        char *rel_name; \
        token = pg_strtok(&length);         \
        token = pg_strtok(&length); \
        cons_name = nullable_string(token, length); \
        token = pg_strtok(&length); \
        rel_namespace = nullable_string(token, length); \
        token = pg_strtok(&length); \
        rel_name = nullable_string(token, length); \
        if (cons_name && rel_name) \
        {\
            Oid nsp_oid = get_namespaceid(rel_namespace); \
            Oid relid =  get_relname_relid(rel_name, nsp_oid); \
            local_node->fldname = get_relation_constraint_oid(relid, cons_name, false); \
        }\
        else \
            local_node->fldname = InvalidOid; \
    } while (0) 
#endif

/* Read an attribute number array */
#define READ_ATTRNUMBER_ARRAY(fldname, len) \
    token = pg_strtok(&length);        /* skip :fldname */ \
    local_node->fldname = readAttrNumberCols(len);

/* Read an oid array */
#define READ_OID_ARRAY(fldname, len) \
    token = pg_strtok(&length);        /* skip :fldname */ \
    local_node->fldname = readOidCols(len);

/* Read an int array */
#define READ_INT_ARRAY(fldname, len) \
    token = pg_strtok(&length);        /* skip :fldname */ \
    local_node->fldname = readIntCols(len);

/* Read a bool array */
#define READ_BOOL_ARRAY(fldname, len) \
    token = pg_strtok(&length);        /* skip :fldname */ \
    local_node->fldname = readBoolCols(len);

/* Routine exit */
#define READ_DONE() \
    return local_node


/*
 * NOTE: use atoi() to read values written with %d, or atoui() to read
 * values written with %u in outfuncs.c.  An exception is OID values,
 * for which use atooid().  (As of 7.1, outfuncs.c writes OIDs as %u,
 * but this will probably change in the future.)
 */
#define atoui(x)  ((unsigned int) strtoul((x), NULL, 10))

#define strtobool(x)  ((*(x) == 't') ? true : false)

#define nullable_string(token,length)  \
    ((length) == 0 ? NULL : debackslash(token, length))

#ifdef XCP
static Datum scanDatum(Oid typid, int typmod);
#endif

/*
 * _readBitmapset
 */
static Bitmapset *
_readBitmapset(void)
{// #lizard forgives
    Bitmapset  *result = NULL;

    READ_TEMP_LOCALS();

    token = pg_strtok(&length);
    if (token == NULL)
        elog(ERROR, "incomplete Bitmapset structure");
    if (length != 1 || token[0] != '(')
        elog(ERROR, "unrecognized token: \"%.*s\"", length, token);

    token = pg_strtok(&length);
    if (token == NULL)
        elog(ERROR, "incomplete Bitmapset structure");
    if (length != 1 || token[0] != 'b')
        elog(ERROR, "unrecognized token: \"%.*s\"", length, token);

    for (;;)
    {
        int            val;
        char       *endptr;

        token = pg_strtok(&length);
        if (token == NULL)
            elog(ERROR, "unterminated Bitmapset structure");
        if (length == 1 && token[0] == ')')
            break;
        val = (int) strtol(token, &endptr, 10);
        if (endptr != token + length)
            elog(ERROR, "unrecognized integer: \"%.*s\"", length, token);
        result = bms_add_member(result, val);
    }

    return result;
}

/*
 * for use by extensions which define extensible nodes
 */
Bitmapset *
readBitmapset(void)
{
    return _readBitmapset();
}

/*
 * _readQuery
 */
static Query *
_readQuery(void)
{
    READ_LOCALS(Query);

    READ_ENUM_FIELD(commandType, CmdType);
    READ_ENUM_FIELD(querySource, QuerySource);
    local_node->queryId = 0;    /* not saved in output format */
    READ_BOOL_FIELD(canSetTag);
    READ_NODE_FIELD(utilityStmt);
    READ_INT_FIELD(resultRelation);
    READ_BOOL_FIELD(hasAggs);
    READ_BOOL_FIELD(hasWindowFuncs);
    READ_BOOL_FIELD(hasTargetSRFs);
    READ_BOOL_FIELD(hasSubLinks);
    READ_BOOL_FIELD(hasDistinctOn);
    READ_BOOL_FIELD(hasRecursive);
    READ_BOOL_FIELD(hasModifyingCTE);
    READ_BOOL_FIELD(hasForUpdate);
    READ_BOOL_FIELD(hasRowSecurity);
	token = pg_strtok(&length);		/* get :fldname hasRowSecurity or cteList */
	Assert(length != 0);
	if (strncmp(debackslash(token, length), ":hasCoordFuncs", length) == 0)
	{
		token = pg_strtok(&length);		/* get field value */
		local_node->hasCoordFuncs = strtobool(token);
		token = pg_strtok(&length);		/* skip :fldname cteList */
	}
	else
	{
		local_node->hasCoordFuncs = false;
	}
	local_node->cteList = nodeRead(NULL, 0);
    READ_NODE_FIELD(rtable);
    READ_NODE_FIELD(jointree);
    READ_NODE_FIELD(targetList);
    READ_ENUM_FIELD(override, OverridingKind);
    READ_NODE_FIELD(onConflict);
    READ_NODE_FIELD(returningList);
    READ_NODE_FIELD(groupClause);
    READ_NODE_FIELD(groupingSets);
    READ_NODE_FIELD(havingQual);
    READ_NODE_FIELD(windowClause);
    READ_NODE_FIELD(distinctClause);
    READ_NODE_FIELD(sortClause);
    READ_NODE_FIELD(limitOffset);
    READ_NODE_FIELD(limitCount);
    READ_NODE_FIELD(rowMarks);
    READ_NODE_FIELD(setOperations);
    READ_NODE_FIELD(constraintDeps);
    /* withCheckOptions intentionally omitted, see comment in parsenodes.h */
    READ_LOCATION_FIELD(stmt_location);
    READ_LOCATION_FIELD(stmt_len);

    READ_DONE();
}

/*
 * _readNotifyStmt
 */
static NotifyStmt *
_readNotifyStmt(void)
{
    READ_LOCALS(NotifyStmt);

    READ_STRING_FIELD(conditionname);
    READ_STRING_FIELD(payload);

    READ_DONE();
}

/*
 * _readDeclareCursorStmt
 */
static DeclareCursorStmt *
_readDeclareCursorStmt(void)
{
    READ_LOCALS(DeclareCursorStmt);

    READ_STRING_FIELD(portalname);
    READ_INT_FIELD(options);
    READ_NODE_FIELD(query);

    READ_DONE();
}

/*
 * _readWithCheckOption
 */
static WithCheckOption *
_readWithCheckOption(void)
{
    READ_LOCALS(WithCheckOption);

    READ_ENUM_FIELD(kind, WCOKind);
    READ_STRING_FIELD(relname);
    READ_STRING_FIELD(polname);
    READ_NODE_FIELD(qual);
    READ_BOOL_FIELD(cascaded);

    READ_DONE();
}

/*
 * _readSortGroupClause
 */
static SortGroupClause *
_readSortGroupClause(void)
{
    READ_LOCALS(SortGroupClause);

    READ_UINT_FIELD(tleSortGroupRef);
#ifdef XCP
    if (portable_input)
        READ_OPERID_FIELD(eqop);
    else
#endif
    READ_OID_FIELD(eqop);
#ifdef XCP
    if (portable_input)
        READ_OPERID_FIELD(sortop);
    else
#endif
    READ_OID_FIELD(sortop);
    READ_BOOL_FIELD(nulls_first);
    READ_BOOL_FIELD(hashable);

    READ_DONE();
}

/*
 * _readGroupingSet
 */
static GroupingSet *
_readGroupingSet(void)
{
    READ_LOCALS(GroupingSet);

    READ_ENUM_FIELD(kind, GroupingSetKind);
    READ_NODE_FIELD(content);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readWindowClause
 */
static WindowClause *
_readWindowClause(void)
{
    READ_LOCALS(WindowClause);

    READ_STRING_FIELD(name);
    READ_STRING_FIELD(refname);
    READ_NODE_FIELD(partitionClause);
    READ_NODE_FIELD(orderClause);
    READ_INT_FIELD(frameOptions);
    READ_NODE_FIELD(startOffset);
    READ_NODE_FIELD(endOffset);
    READ_UINT_FIELD(winref);
    READ_BOOL_FIELD(copiedOrder);

    READ_DONE();
}

/*
 * _readRowMarkClause
 */
static RowMarkClause *
_readRowMarkClause(void)
{
    READ_LOCALS(RowMarkClause);

    READ_UINT_FIELD(rti);
    READ_ENUM_FIELD(strength, LockClauseStrength);
    READ_ENUM_FIELD(waitPolicy, LockWaitPolicy);
    READ_BOOL_FIELD(pushedDown);

    READ_DONE();
}

/*
 * _readCommonTableExpr
 */
static CommonTableExpr *
_readCommonTableExpr(void)
{
    READ_LOCALS(CommonTableExpr);

	READ_STRING_FIELD(ctename);
	READ_NODE_FIELD(aliascolnames);
	READ_ENUM_FIELD(ctematerialized, CTEMaterialize);
	READ_NODE_FIELD(ctequery);
	READ_LOCATION_FIELD(location);
	READ_BOOL_FIELD(cterecursive);
	READ_INT_FIELD(cterefcount);
	READ_NODE_FIELD(ctecolnames);
	READ_NODE_FIELD(ctecoltypes);
	READ_NODE_FIELD(ctecoltypmods);
	READ_NODE_FIELD(ctecolcollations);

    READ_DONE();
}

/*
 * _readSetOperationStmt
 */
static SetOperationStmt *
_readSetOperationStmt(void)
{
    READ_LOCALS(SetOperationStmt);

    READ_ENUM_FIELD(op, SetOperation);
    READ_BOOL_FIELD(all);
    READ_NODE_FIELD(larg);
    READ_NODE_FIELD(rarg);
    READ_NODE_FIELD(colTypes);
    READ_NODE_FIELD(colTypmods);
    READ_NODE_FIELD(colCollations);
    READ_NODE_FIELD(groupClauses);

    READ_DONE();
}


/*
 *    Stuff from primnodes.h.
 */

static Alias *
_readAlias(void)
{
    READ_LOCALS(Alias);

    READ_STRING_FIELD(aliasname);
    READ_NODE_FIELD(colnames);

    READ_DONE();
}

static RangeVar *
_readRangeVar(void)
{
    READ_LOCALS(RangeVar);

    local_node->catalogname = NULL; /* not currently saved in output format */

    READ_STRING_FIELD(schemaname);
    READ_STRING_FIELD(relname);
    READ_BOOL_FIELD(inh);
    READ_CHAR_FIELD(relpersistence);
    READ_NODE_FIELD(alias);
    READ_LOCATION_FIELD(location);
#ifdef __TBASE__
    READ_BOOL_FIELD(intervalparent);
    READ_NODE_FIELD(partitionvalue);
#endif
#ifdef __STORAGE_SCALABLE__
    READ_STRING_FIELD(pubname);
#endif

    READ_DONE();
}

/*
 * _readTableFunc
 */
static TableFunc *
_readTableFunc(void)
{
    READ_LOCALS(TableFunc);

    READ_NODE_FIELD(ns_uris);
    READ_NODE_FIELD(ns_names);
    READ_NODE_FIELD(docexpr);
    READ_NODE_FIELD(rowexpr);
    READ_NODE_FIELD(colnames);
    READ_NODE_FIELD(coltypes);
    READ_NODE_FIELD(coltypmods);
    READ_NODE_FIELD(colcollations);
    READ_NODE_FIELD(colexprs);
    READ_NODE_FIELD(coldefexprs);
    READ_BITMAPSET_FIELD(notnulls);
    READ_INT_FIELD(ordinalitycol);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

static IntoClause *
_readIntoClause(void)
{
    READ_LOCALS(IntoClause);

    READ_NODE_FIELD(rel);
    READ_NODE_FIELD(colNames);
    READ_NODE_FIELD(options);
    READ_ENUM_FIELD(onCommit, OnCommitAction);
    READ_STRING_FIELD(tableSpaceName);
    READ_NODE_FIELD(viewQuery);
    READ_BOOL_FIELD(skipData);

    READ_DONE();
}

/*
 * _readVar
 */
static Var *
_readVar(void)
{
    READ_LOCALS(Var);

    READ_UINT_FIELD(varno);
    READ_INT_FIELD(varattno);
#ifdef XCP
    if (portable_input)
        READ_TYPID_FIELD(vartype);
    else
#endif
    READ_OID_FIELD(vartype);
    READ_INT_FIELD(vartypmod);
#ifdef XCP
    if (portable_input)
        READ_COLLID_FIELD(varcollid);
    else
#endif
    READ_OID_FIELD(varcollid);
    READ_UINT_FIELD(varlevelsup);
    READ_UINT_FIELD(varnoold);
    READ_INT_FIELD(varoattno);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readConst
 */
static Const *
_readConst(void)
{// #lizard forgives
    READ_LOCALS(Const);

#ifdef XCP
    if (portable_input)
        READ_TYPID_FIELD(consttype);
    else
#endif
    READ_OID_FIELD(consttype);
    READ_INT_FIELD(consttypmod);
#ifdef XCP
    if (portable_input)
        READ_COLLID_FIELD(constcollid);
    else
#endif
    READ_OID_FIELD(constcollid);
    READ_INT_FIELD(constlen);
    READ_BOOL_FIELD(constbyval);
    READ_BOOL_FIELD(constisnull);
    READ_LOCATION_FIELD(location);

    token = pg_strtok(&length); /* skip :constvalue */
    if (local_node->constisnull)
        token = pg_strtok(&length); /* skip "<>" */
    else
#ifdef XCP
        if (portable_input)
            local_node->constvalue = scanDatum(local_node->consttype,
                                               local_node->consttypmod);
        else
#endif
        local_node->constvalue = readDatum(local_node->constbyval);

    READ_DONE();
}

/*
 * _readParam
 */
static Param *
_readParam(void)
{
    READ_LOCALS(Param);

    READ_ENUM_FIELD(paramkind, ParamKind);
    READ_INT_FIELD(paramid);
#ifdef XCP
    if (portable_input)
        READ_TYPID_FIELD(paramtype);
    else
#endif
    READ_OID_FIELD(paramtype);
    READ_INT_FIELD(paramtypmod);
#ifdef XCP
    if (portable_input)
        READ_COLLID_FIELD(paramcollid);
    else
#endif
    READ_OID_FIELD(paramcollid);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readAggref
 */
static Aggref *
_readAggref(void)
{// #lizard forgives
    READ_LOCALS(Aggref);

#ifdef XCP
    if (portable_input)
        READ_FUNCID_FIELD(aggfnoid);
    else
#endif
    READ_OID_FIELD(aggfnoid);
#ifdef XCP
    if (portable_input)
        READ_TYPID_FIELD(aggtype);
    else
#endif
    READ_OID_FIELD(aggtype);
#ifdef XCP
    if (portable_input)
        READ_COLLID_FIELD(aggcollid);
    else
#endif
    READ_OID_FIELD(aggcollid);
#ifdef XCP
    if (portable_input)
        READ_COLLID_FIELD(inputcollid);
    else
#endif
    READ_OID_FIELD(inputcollid);
#ifdef XCP
    if (portable_input)
        READ_TYPID_FIELD(aggtranstype);
    else
#endif
    READ_OID_FIELD(aggtranstype);
#ifdef XCP
    if (portable_input)
        READ_TYPID_LIST_FIELD(aggargtypes);
    else
#endif
    READ_NODE_FIELD(aggargtypes);
    READ_NODE_FIELD(aggdirectargs);
    READ_NODE_FIELD(args);
    READ_NODE_FIELD(aggorder);
    READ_NODE_FIELD(aggdistinct);
    READ_NODE_FIELD(aggfilter);
    READ_BOOL_FIELD(aggstar);
    READ_BOOL_FIELD(aggvariadic);
    READ_CHAR_FIELD(aggkind);
    READ_UINT_FIELD(agglevelsup);
    READ_ENUM_FIELD(aggsplit, AggSplit);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readGroupingFunc
 */
static GroupingFunc *
_readGroupingFunc(void)
{
    READ_LOCALS(GroupingFunc);

    READ_NODE_FIELD(args);
    READ_NODE_FIELD(refs);
    READ_NODE_FIELD(cols);
    READ_UINT_FIELD(agglevelsup);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readWindowFunc
 */
static WindowFunc *
_readWindowFunc(void)
{// #lizard forgives
    READ_LOCALS(WindowFunc);

#ifdef XCP
    if (portable_input)
        READ_FUNCID_FIELD(winfnoid);
    else
#endif
    READ_OID_FIELD(winfnoid);
#ifdef XCP
    if (portable_input)
        READ_TYPID_FIELD(wintype);
    else
#endif
    READ_OID_FIELD(wintype);
#ifdef XCP
    if (portable_input)
        READ_COLLID_FIELD(wincollid);
    else
#endif
    READ_OID_FIELD(wincollid);
#ifdef XCP
    if (portable_input)
        READ_COLLID_FIELD(inputcollid);
    else
#endif
    READ_OID_FIELD(inputcollid);
    READ_NODE_FIELD(args);
    READ_NODE_FIELD(aggfilter);
    READ_UINT_FIELD(winref);
    READ_BOOL_FIELD(winstar);
    READ_BOOL_FIELD(winagg);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readArrayRef
 */
static ArrayRef *
_readArrayRef(void)
{
    READ_LOCALS(ArrayRef);

#ifdef XCP
    if (portable_input)
        READ_TYPID_FIELD(refarraytype);
    else
#endif
    READ_OID_FIELD(refarraytype);
#ifdef XCP
    if (portable_input)
        READ_TYPID_FIELD(refelemtype);
    else
#endif
    READ_OID_FIELD(refelemtype);
    READ_INT_FIELD(reftypmod);
#ifdef XCP
    if (portable_input)
        READ_COLLID_FIELD(refcollid);
    else
#endif
    READ_OID_FIELD(refcollid);
    READ_NODE_FIELD(refupperindexpr);
    READ_NODE_FIELD(reflowerindexpr);
    READ_NODE_FIELD(refexpr);
    READ_NODE_FIELD(refassgnexpr);

    READ_DONE();
}

/*
 * _readFuncExpr
 */
static FuncExpr *
_readFuncExpr(void)
{// #lizard forgives
    READ_LOCALS(FuncExpr);

#ifdef XCP
    if (portable_input)
        READ_FUNCID_FIELD(funcid);
    else
#endif
    READ_OID_FIELD(funcid);
#ifdef XCP
    if (portable_input)
        READ_TYPID_FIELD(funcresulttype);
    else
#endif
    READ_OID_FIELD(funcresulttype);
    READ_BOOL_FIELD(funcretset);
    READ_BOOL_FIELD(funcvariadic);
    READ_ENUM_FIELD(funcformat, CoercionForm);
#ifdef XCP
    if (portable_input)
        READ_COLLID_FIELD(funccollid);
    else
#endif
    READ_OID_FIELD(funccollid);
#ifdef XCP
    if (portable_input)
        READ_COLLID_FIELD(inputcollid);
    else
#endif
    READ_OID_FIELD(inputcollid);
    READ_NODE_FIELD(args);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readNamedArgExpr
 */
static NamedArgExpr *
_readNamedArgExpr(void)
{
    READ_LOCALS(NamedArgExpr);

    READ_NODE_FIELD(arg);
    READ_STRING_FIELD(name);
    READ_INT_FIELD(argnumber);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readOpExpr
 */
static OpExpr *
_readOpExpr(void)
{// #lizard forgives
    READ_LOCALS(OpExpr);

#ifdef XCP
    if (portable_input)
        READ_OPERID_FIELD(opno);
    else
#endif
    READ_OID_FIELD(opno);
#ifdef XCP
    if (portable_input)
        READ_FUNCID_FIELD(opfuncid);
    else
#endif
    READ_OID_FIELD(opfuncid);
#ifdef XCP
    if (portable_input)
        READ_TYPID_FIELD(opresulttype);
    else
#endif
    READ_OID_FIELD(opresulttype);
    READ_BOOL_FIELD(opretset);
#ifdef XCP
    if (portable_input)
        READ_COLLID_FIELD(opcollid);
    else
#endif
    READ_OID_FIELD(opcollid);
#ifdef XCP
    if (portable_input)
        READ_COLLID_FIELD(inputcollid);
    else
#endif
    READ_OID_FIELD(inputcollid);
    READ_NODE_FIELD(args);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readDistinctExpr
 */
static DistinctExpr *
_readDistinctExpr(void)
{// #lizard forgives
    READ_LOCALS(DistinctExpr);

#ifdef XCP
    if (portable_input)
        READ_OPERID_FIELD(opno);
    else
#endif
    READ_OID_FIELD(opno);
#ifdef XCP
    if (portable_input)
        READ_FUNCID_FIELD(opfuncid);
    else
#endif
    READ_OID_FIELD(opfuncid);
#ifdef XCP
    if (portable_input)
        READ_TYPID_FIELD(opresulttype);
    else
#endif
    READ_OID_FIELD(opresulttype);
    READ_BOOL_FIELD(opretset);
#ifdef XCP
    if (portable_input)
        READ_COLLID_FIELD(opcollid);
    else
#endif
    READ_OID_FIELD(opcollid);
#ifdef XCP
    if (portable_input)
        READ_COLLID_FIELD(inputcollid);
    else
#endif
    READ_OID_FIELD(inputcollid);
    READ_NODE_FIELD(args);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readNullIfExpr
 */
static NullIfExpr *
_readNullIfExpr(void)
{// #lizard forgives
    READ_LOCALS(NullIfExpr);

#ifdef XCP
    if (portable_input)
        READ_OPERID_FIELD(opno);
    else
#endif
    READ_OID_FIELD(opno);
#ifdef XCP
    if (portable_input)
        READ_FUNCID_FIELD(opfuncid);
    else
#endif
    READ_OID_FIELD(opfuncid);
#if 0
    /*
     * The opfuncid is stored in the textual format primarily for debugging
     * and documentation reasons.  We want to always read it as zero to force
     * it to be re-looked-up in the pg_operator entry.  This ensures that
     * stored rules don't have hidden dependencies on operators' functions.
     * (We don't currently support an ALTER OPERATOR command, but might
     * someday.)
     */
#ifdef XCP
    /* Do not invalidate if we have just looked up the value */
    if (!portable_input)
#endif
    local_node->opfuncid = InvalidOid;
#endif
#ifdef XCP
    if (portable_input)
        READ_TYPID_FIELD(opresulttype);
    else
#endif
    READ_OID_FIELD(opresulttype);
    READ_BOOL_FIELD(opretset);
#ifdef XCP
    if (portable_input)
        READ_COLLID_FIELD(opcollid);
    else
#endif
    READ_OID_FIELD(opcollid);
#ifdef XCP
    if (portable_input)
        READ_COLLID_FIELD(inputcollid);
    else
#endif
    READ_OID_FIELD(inputcollid);
    READ_NODE_FIELD(args);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readScalarArrayOpExpr
 */
static ScalarArrayOpExpr *
_readScalarArrayOpExpr(void)
{
    READ_LOCALS(ScalarArrayOpExpr);

#ifdef XCP
    if (portable_input)
        READ_OPERID_FIELD(opno);
    else
#endif
    READ_OID_FIELD(opno);
#ifdef XCP
    if (portable_input)
        READ_FUNCID_FIELD(opfuncid);
    else
#endif
    READ_OID_FIELD(opfuncid);
    READ_BOOL_FIELD(useOr);
#ifdef XCP
    if (portable_input)
        READ_COLLID_FIELD(inputcollid);
    else
#endif
    READ_OID_FIELD(inputcollid);
    READ_NODE_FIELD(args);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readBoolExpr
 */
static BoolExpr *
_readBoolExpr(void)
{
    READ_LOCALS(BoolExpr);

    /* do-it-yourself enum representation */
    token = pg_strtok(&length); /* skip :boolop */
    token = pg_strtok(&length); /* get field value */
    if (strncmp(token, "and", 3) == 0)
        local_node->boolop = AND_EXPR;
    else if (strncmp(token, "or", 2) == 0)
        local_node->boolop = OR_EXPR;
    else if (strncmp(token, "not", 3) == 0)
        local_node->boolop = NOT_EXPR;
    else
        elog(ERROR, "unrecognized boolop \"%.*s\"", length, token);

    READ_NODE_FIELD(args);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readSubLink
 */
static SubLink *
_readSubLink(void)
{
    READ_LOCALS(SubLink);

    READ_ENUM_FIELD(subLinkType, SubLinkType);
    READ_INT_FIELD(subLinkId);
    READ_NODE_FIELD(testexpr);
    READ_NODE_FIELD(operName);
    READ_NODE_FIELD(subselect);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readFieldSelect
 */
static FieldSelect *
_readFieldSelect(void)
{
    READ_LOCALS(FieldSelect);

    READ_NODE_FIELD(arg);
    READ_INT_FIELD(fieldnum);
#ifdef XCP
    if (portable_input)
        READ_TYPID_FIELD(resulttype);
    else
#endif
    READ_OID_FIELD(resulttype);
    READ_INT_FIELD(resulttypmod);
#ifdef XCP
    if (portable_input)
        READ_COLLID_FIELD(resultcollid);
    else
#endif
    READ_OID_FIELD(resultcollid);

    READ_DONE();
}

/*
 * _readFieldStore
 */
static FieldStore *
_readFieldStore(void)
{
    READ_LOCALS(FieldStore);

    READ_NODE_FIELD(arg);
    READ_NODE_FIELD(newvals);
    READ_NODE_FIELD(fieldnums);
#ifdef XCP
    if (portable_input)
        READ_TYPID_FIELD(resulttype);
    else
#endif
    READ_OID_FIELD(resulttype);

    READ_DONE();
}

/*
 * _readRelabelType
 */
static RelabelType *
_readRelabelType(void)
{
    READ_LOCALS(RelabelType);

    READ_NODE_FIELD(arg);
#ifdef XCP
    if (portable_input)
        READ_TYPID_FIELD(resulttype);
    else
#endif
    READ_OID_FIELD(resulttype);
    READ_INT_FIELD(resulttypmod);
#ifdef XCP
    if (portable_input)
        READ_COLLID_FIELD(resultcollid);
    else
#endif
    READ_OID_FIELD(resultcollid);
    READ_ENUM_FIELD(relabelformat, CoercionForm);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readCoerceViaIO
 */
static CoerceViaIO *
_readCoerceViaIO(void)
{
    READ_LOCALS(CoerceViaIO);

    READ_NODE_FIELD(arg);
#ifdef XCP
    if (portable_input)
        READ_TYPID_FIELD(resulttype);
    else
#endif
    READ_OID_FIELD(resulttype);
#ifdef XCP
    if (portable_input)
        READ_COLLID_FIELD(resultcollid);
    else
#endif
    READ_OID_FIELD(resultcollid);
    READ_ENUM_FIELD(coerceformat, CoercionForm);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readArrayCoerceExpr
 */
static ArrayCoerceExpr *
_readArrayCoerceExpr(void)
{
    READ_LOCALS(ArrayCoerceExpr);

    READ_NODE_FIELD(arg);
#ifdef XCP
    if (portable_input)
        READ_FUNCID_FIELD(elemfuncid);
    else
#endif
    READ_OID_FIELD(elemfuncid);
#ifdef XCP
    if (portable_input)
        READ_TYPID_FIELD(resulttype);
    else
#endif
    READ_OID_FIELD(resulttype);
    READ_INT_FIELD(resulttypmod);
#ifdef XCP
    if (portable_input)
        READ_COLLID_FIELD(resultcollid);
    else
#endif
    READ_OID_FIELD(resultcollid);
    READ_BOOL_FIELD(isExplicit);
    READ_ENUM_FIELD(coerceformat, CoercionForm);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readConvertRowtypeExpr
 */
static ConvertRowtypeExpr *
_readConvertRowtypeExpr(void)
{
    READ_LOCALS(ConvertRowtypeExpr);

    READ_NODE_FIELD(arg);
#ifdef XCP
    if (portable_input)
        READ_TYPID_FIELD(resulttype);
    else
#endif
    READ_OID_FIELD(resulttype);
    READ_ENUM_FIELD(convertformat, CoercionForm);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readCollateExpr
 */
static CollateExpr *
_readCollateExpr(void)
{
    READ_LOCALS(CollateExpr);

    READ_NODE_FIELD(arg);
#ifdef __TBASE__
    if (portable_input)
    {
        READ_COLLID_FIELD(collOid);
    }
    else
#endif
    READ_OID_FIELD(collOid);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readCaseExpr
 */
static CaseExpr *
_readCaseExpr(void)
{
    READ_LOCALS(CaseExpr);

#ifdef XCP
    if (portable_input)
        READ_TYPID_FIELD(casetype);
    else
#endif
    READ_OID_FIELD(casetype);
#ifdef XCP
    if (portable_input)
        READ_COLLID_FIELD(casecollid);
    else
#endif
    READ_OID_FIELD(casecollid);
    READ_NODE_FIELD(arg);
    READ_NODE_FIELD(args);
    READ_NODE_FIELD(defresult);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readCaseWhen
 */
static CaseWhen *
_readCaseWhen(void)
{
    READ_LOCALS(CaseWhen);

    READ_NODE_FIELD(expr);
    READ_NODE_FIELD(result);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readCaseTestExpr
 */
static CaseTestExpr *
_readCaseTestExpr(void)
{
    READ_LOCALS(CaseTestExpr);

#ifdef XCP
    if (portable_input)
        READ_TYPID_FIELD(typeId);
    else
#endif
    READ_OID_FIELD(typeId);
    READ_INT_FIELD(typeMod);
#ifdef XCP
    if (portable_input)
        READ_COLLID_FIELD(collation);
    else
#endif
    READ_OID_FIELD(collation);

    READ_DONE();
}

/*
 * _readArrayExpr
 */
static ArrayExpr *
_readArrayExpr(void)
{
    READ_LOCALS(ArrayExpr);

#ifdef XCP
    if (portable_input)
        READ_TYPID_FIELD(array_typeid);
    else
#endif
    READ_OID_FIELD(array_typeid);
#ifdef XCP
    if (portable_input)
        READ_COLLID_FIELD(array_collid);
    else
#endif
    READ_OID_FIELD(array_collid);
#ifdef XCP
    if (portable_input)
        READ_TYPID_FIELD(element_typeid);
    else
#endif
    READ_OID_FIELD(element_typeid);
    READ_NODE_FIELD(elements);
    READ_BOOL_FIELD(multidims);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readRowExpr
 */
static RowExpr *
_readRowExpr(void)
{
    READ_LOCALS(RowExpr);

    READ_NODE_FIELD(args);
#ifdef XCP
    if (portable_input)
        READ_TYPID_FIELD(row_typeid);
    else
#endif
    READ_OID_FIELD(row_typeid);
    READ_ENUM_FIELD(row_format, CoercionForm);
    READ_NODE_FIELD(colnames);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readRowCompareExpr
 */
static RowCompareExpr *
_readRowCompareExpr(void)
{
    READ_LOCALS(RowCompareExpr);

    READ_ENUM_FIELD(rctype, RowCompareType);
    READ_NODE_FIELD(opnos);
    READ_NODE_FIELD(opfamilies);
    READ_NODE_FIELD(inputcollids);
    READ_NODE_FIELD(largs);
    READ_NODE_FIELD(rargs);

    READ_DONE();
}

/*
 * _readCoalesceExpr
 */
static CoalesceExpr *
_readCoalesceExpr(void)
{
    READ_LOCALS(CoalesceExpr);

#ifdef XCP
    if (portable_input)
        READ_TYPID_FIELD(coalescetype);
    else
#endif
    READ_OID_FIELD(coalescetype);
#ifdef XCP
    if (portable_input)
        READ_COLLID_FIELD(coalescecollid);
    else
#endif
    READ_OID_FIELD(coalescecollid);
    READ_NODE_FIELD(args);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readMinMaxExpr
 */
static MinMaxExpr *
_readMinMaxExpr(void)
{
    READ_LOCALS(MinMaxExpr);

#ifdef XCP
    if (portable_input)
        READ_TYPID_FIELD(minmaxtype);
    else
#endif
    READ_OID_FIELD(minmaxtype);
#ifdef XCP
    if (portable_input)
        READ_COLLID_FIELD(minmaxcollid);
    else
#endif
    READ_OID_FIELD(minmaxcollid);
#ifdef XCP
    if (portable_input)
        READ_COLLID_FIELD(inputcollid);
    else
#endif
    READ_OID_FIELD(inputcollid);
    READ_ENUM_FIELD(op, MinMaxOp);
    READ_NODE_FIELD(args);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readSQLValueFunction
 */
static SQLValueFunction *
_readSQLValueFunction(void)
{
    READ_LOCALS(SQLValueFunction);

    READ_ENUM_FIELD(op, SQLValueFunctionOp);
    if (portable_input)
        READ_TYPID_FIELD(type);
    else
        READ_OID_FIELD(type);
    READ_INT_FIELD(typmod);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readNextValueExpr
 */
static NextValueExpr *
_readNextValueExpr(void)
{
    READ_LOCALS(NextValueExpr);

    if (portable_input)
    {
        READ_RELID_FIELD(seqid);
        READ_TYPID_FIELD(typeId);
    }
    else
    {
        READ_OID_FIELD(seqid);
        READ_OID_FIELD(typeId);
    }
    READ_DONE();
}

/*
 * _readXmlExpr
 */
static XmlExpr *
_readXmlExpr(void)
{
    READ_LOCALS(XmlExpr);

    READ_ENUM_FIELD(op, XmlExprOp);
    READ_STRING_FIELD(name);
    READ_NODE_FIELD(named_args);
    READ_NODE_FIELD(arg_names);
    READ_NODE_FIELD(args);
    READ_ENUM_FIELD(xmloption, XmlOptionType);
#ifdef XCP
    if (portable_input)
        READ_TYPID_FIELD(type);
    else
#endif
    READ_OID_FIELD(type);
    READ_INT_FIELD(typmod);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readNullTest
 */
static NullTest *
_readNullTest(void)
{
    READ_LOCALS(NullTest);

    READ_NODE_FIELD(arg);
    READ_ENUM_FIELD(nulltesttype, NullTestType);
    READ_BOOL_FIELD(argisrow);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readBooleanTest
 */
static BooleanTest *
_readBooleanTest(void)
{
    READ_LOCALS(BooleanTest);

    READ_NODE_FIELD(arg);
    READ_ENUM_FIELD(booltesttype, BoolTestType);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readCoerceToDomain
 */
static CoerceToDomain *
_readCoerceToDomain(void)
{
    READ_LOCALS(CoerceToDomain);

    READ_NODE_FIELD(arg);
#ifdef XCP
    if (portable_input)
        READ_TYPID_FIELD(resulttype);
    else
#endif
    READ_OID_FIELD(resulttype);
    READ_INT_FIELD(resulttypmod);
#ifdef XCP
    if (portable_input)
        READ_COLLID_FIELD(resultcollid);
    else
#endif
    READ_OID_FIELD(resultcollid);
    READ_ENUM_FIELD(coercionformat, CoercionForm);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readCoerceToDomainValue
 */
static CoerceToDomainValue *
_readCoerceToDomainValue(void)
{
    READ_LOCALS(CoerceToDomainValue);

#ifdef XCP
    if (portable_input)
        READ_TYPID_FIELD(typeId);
    else
#endif
    READ_OID_FIELD(typeId);
    READ_INT_FIELD(typeMod);
#ifdef XCP
    if (portable_input)
        READ_COLLID_FIELD(collation);
    else
#endif
    READ_OID_FIELD(collation);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readSetToDefault
 */
static SetToDefault *
_readSetToDefault(void)
{
    READ_LOCALS(SetToDefault);

#ifdef XCP
    if (portable_input)
        READ_TYPID_FIELD(typeId);
    else
#endif
    READ_OID_FIELD(typeId);
    READ_INT_FIELD(typeMod);
#ifdef XCP
    if (portable_input)
        READ_COLLID_FIELD(collation);
    else
#endif
    READ_OID_FIELD(collation);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readCurrentOfExpr
 */
static CurrentOfExpr *
_readCurrentOfExpr(void)
{
    READ_LOCALS(CurrentOfExpr);

    READ_UINT_FIELD(cvarno);
    READ_STRING_FIELD(cursor_name);
    READ_INT_FIELD(cursor_param);

    READ_DONE();
}

/*
 * _readInferenceElem
 */
static InferenceElem *
_readInferenceElem(void)
{
    READ_LOCALS(InferenceElem);

    READ_NODE_FIELD(expr);
#ifdef __TBASE__
    if (portable_input)
    {
        READ_COLLID_FIELD(infercollid);
        READ_OPCLASS_FIELD(inferopclass);
    }
    else
    {
#endif
    READ_OID_FIELD(infercollid);
    READ_OID_FIELD(inferopclass);
#ifdef __TBASE__
    }
#endif
    READ_DONE();
}

/*
 * _readTargetEntry
 */
static TargetEntry *
_readTargetEntry(void)
{
    READ_LOCALS(TargetEntry);

    READ_NODE_FIELD(expr);
    READ_INT_FIELD(resno);
    READ_STRING_FIELD(resname);
    READ_UINT_FIELD(ressortgroupref);
#ifdef XCP
    if (portable_input)
        READ_RELID_FIELD_NOWARN(resorigtbl);
    else
#endif
    READ_OID_FIELD(resorigtbl);
    READ_INT_FIELD(resorigcol);
    READ_BOOL_FIELD(resjunk);

    READ_DONE();
}

/*
 * _readRangeTblRef
 */
static RangeTblRef *
_readRangeTblRef(void)
{
    READ_LOCALS(RangeTblRef);

    READ_INT_FIELD(rtindex);

    READ_DONE();
}

/*
 * _readJoinExpr
 */
static JoinExpr *
_readJoinExpr(void)
{
    READ_LOCALS(JoinExpr);

    READ_ENUM_FIELD(jointype, JoinType);
    READ_BOOL_FIELD(isNatural);
    READ_NODE_FIELD(larg);
    READ_NODE_FIELD(rarg);
    READ_NODE_FIELD(usingClause);
    READ_NODE_FIELD(quals);
    READ_NODE_FIELD(alias);
    READ_INT_FIELD(rtindex);

    READ_DONE();
}

/*
 * _readFromExpr
 */
static FromExpr *
_readFromExpr(void)
{
    READ_LOCALS(FromExpr);

    READ_NODE_FIELD(fromlist);
    READ_NODE_FIELD(quals);

    READ_DONE();
}

/*
 * _readOnConflictExpr
 */
static OnConflictExpr *
_readOnConflictExpr(void)
{
    READ_LOCALS(OnConflictExpr);

    READ_ENUM_FIELD(action, OnConflictAction);
    READ_NODE_FIELD(arbiterElems);
    READ_NODE_FIELD(arbiterWhere);
#ifdef __TBASE__
    if (portable_input)
    {
        READ_CONSTRAINT_FIELD(constraint);
    }
    else
#endif
    READ_OID_FIELD(constraint);
    READ_NODE_FIELD(onConflictSet);
    READ_NODE_FIELD(onConflictWhere);
    READ_INT_FIELD(exclRelIndex);
    READ_NODE_FIELD(exclRelTlist);

    READ_DONE();
}

static PartitionPruneStepOp *
_readPartitionPruneStepOp(void)
{
   READ_LOCALS(PartitionPruneStepOp);

   READ_INT_FIELD(step.step_id);
   READ_INT_FIELD(opstrategy);
   READ_NODE_FIELD(exprs);
   READ_NODE_FIELD(cmpfns);
   READ_BITMAPSET_FIELD(nullkeys);

   READ_DONE();
}

static PartitionPruneStepCombine *
_readPartitionPruneStepCombine(void)
{
   READ_LOCALS(PartitionPruneStepCombine);

   READ_INT_FIELD(step.step_id);
   READ_ENUM_FIELD(combineOp, PartitionPruneCombineOp);
   READ_NODE_FIELD(source_stepids);

   READ_DONE();
}


/*
 *    Stuff from parsenodes.h.
 */

/*
 * _readRangeTblEntry
 */
static RangeTblEntry *
_readRangeTblEntry(void)
{// #lizard forgives
    READ_LOCALS(RangeTblEntry);

    /* put alias + eref first to make dump more legible */
    READ_NODE_FIELD(alias);
    READ_NODE_FIELD(eref);
    READ_ENUM_FIELD(rtekind, RTEKind);

    switch (local_node->rtekind)
    {
        case RTE_RELATION:
            READ_CHAR_FIELD(relkind);
#ifdef XCP
            if (portable_input)
            {
                if ((local_node->relkind != RELKIND_MATVIEW) &&
                        (local_node->relkind != RELKIND_VIEW))
                    READ_RELID_FIELD(relid);
                else
                    READ_RELID_FIELD_NOWARN(relid);
            }
            else
#endif
            READ_OID_FIELD(relid);
            READ_NODE_FIELD(tablesample);
            break;
        case RTE_SUBQUERY:
            READ_NODE_FIELD(subquery);
            READ_BOOL_FIELD(security_barrier);
            break;
        case RTE_JOIN:
            READ_ENUM_FIELD(jointype, JoinType);
            READ_NODE_FIELD(joinaliasvars);
            break;
        case RTE_FUNCTION:
            READ_NODE_FIELD(functions);
            READ_BOOL_FIELD(funcordinality);
            break;
        case RTE_TABLEFUNC:
            READ_NODE_FIELD(tablefunc);
            break;
        case RTE_VALUES:
            READ_NODE_FIELD(values_lists);
            READ_NODE_FIELD(coltypes);
            READ_NODE_FIELD(coltypmods);
            READ_NODE_FIELD(colcollations);
            break;
        case RTE_CTE:
            READ_STRING_FIELD(ctename);
            READ_UINT_FIELD(ctelevelsup);
            READ_BOOL_FIELD(self_reference);
            READ_NODE_FIELD(coltypes);
            READ_NODE_FIELD(coltypmods);
            READ_NODE_FIELD(colcollations);
            break;
        case RTE_NAMEDTUPLESTORE:
            READ_STRING_FIELD(enrname);
            READ_FLOAT_FIELD(enrtuples);
#ifdef __TBASE__
            if (portable_input)
            {
                READ_RELID_FIELD(relid);
            }
            else
#endif
            READ_OID_FIELD(relid);
            READ_NODE_FIELD(coltypes);
            READ_NODE_FIELD(coltypmods);
            READ_NODE_FIELD(colcollations);
            break;
#ifdef PGXC
        case RTE_REMOTE_DUMMY:
            /* Nothing to do */
            break;
#endif /* PGXC */
        default:
            elog(ERROR, "unrecognized RTE kind: %d",
                 (int) local_node->rtekind);
            break;
    }

    READ_BOOL_FIELD(lateral);
    READ_BOOL_FIELD(inh);
    READ_BOOL_FIELD(inFromCl);
    READ_UINT_FIELD(requiredPerms);
#ifdef XCP
    if (portable_input)
    {
        local_node->requiredPerms = 0; /* no permission checks on data node */
        token = pg_strtok(&length);    /* skip :fldname */ \
        token = pg_strtok(&length);    /* skip field value */ \
        local_node->checkAsUser = InvalidOid;
    }
    else
#endif
    READ_OID_FIELD(checkAsUser);
    READ_BITMAPSET_FIELD(selectedCols);
    READ_BITMAPSET_FIELD(insertedCols);
    READ_BITMAPSET_FIELD(updatedCols);
    READ_NODE_FIELD(securityQuals);

#ifdef __TBASE__
    READ_BOOL_FIELD(intervalparent);
    READ_BOOL_FIELD(isdefault);
    READ_NODE_FIELD(partvalue);
#endif

    READ_DONE();
}

/*
 * _readRangeTblFunction
 */
static RangeTblFunction *
_readRangeTblFunction(void)
{
    READ_LOCALS(RangeTblFunction);

    READ_NODE_FIELD(funcexpr);
    READ_INT_FIELD(funccolcount);
    READ_NODE_FIELD(funccolnames);
    if (portable_input)
    {
        READ_TYPID_LIST_FIELD(funccoltypes);
    }
    else
    {
    READ_NODE_FIELD(funccoltypes);
    }
    READ_NODE_FIELD(funccoltypmods);
    READ_NODE_FIELD(funccolcollations);
    READ_BITMAPSET_FIELD(funcparams);

    READ_DONE();
}

/*
 * _readTableSampleClause
 */
static TableSampleClause *
_readTableSampleClause(void)
{
    READ_LOCALS(TableSampleClause);

#ifdef XCP
    if (portable_input)
        READ_FUNCID_FIELD(tsmhandler);
    else
#endif
    READ_OID_FIELD(tsmhandler);
    READ_NODE_FIELD(args);
    READ_NODE_FIELD(repeatable);

    READ_DONE();
}


/*
 * ReadCommonPlan
 *    Assign the basic stuff of all nodes that inherit from Plan
 */
static void
ReadCommonPlan(Plan *local_node)
{
    READ_TEMP_LOCALS();

    READ_FLOAT_FIELD(startup_cost);
    READ_FLOAT_FIELD(total_cost);
    READ_FLOAT_FIELD(plan_rows);
    READ_INT_FIELD(plan_width);
    READ_BOOL_FIELD(parallel_aware);
    READ_BOOL_FIELD(parallel_safe);
    READ_INT_FIELD(plan_node_id);
    READ_NODE_FIELD(targetlist);
    READ_NODE_FIELD(qual);
    READ_NODE_FIELD(lefttree);
    READ_NODE_FIELD(righttree);
    READ_NODE_FIELD(initPlan);
    READ_BITMAPSET_FIELD(extParam);
    READ_BITMAPSET_FIELD(allParam);
#ifdef __TBASE__
    READ_BOOL_FIELD(isempty);
#endif
#ifdef __AUDIT_FGA__
    READ_NODE_FIELD(audit_fga_quals);
#endif

}

/*
 * _readDefElem
 */
static DefElem *
_readDefElem(void)
{
    READ_LOCALS(DefElem);

    READ_STRING_FIELD(defnamespace);
    READ_STRING_FIELD(defname);
    READ_NODE_FIELD(arg);
    READ_ENUM_FIELD(defaction, DefElemAction);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readPlan
 */
static Plan *
_readPlan(void)
{
    READ_LOCALS_NO_FIELDS(Plan);

    ReadCommonPlan(local_node);

    READ_DONE();
}

/*
 * _readPlannedStmt
 */
static PlannedStmt *
_readPlannedStmt(void)
{
    READ_LOCALS(PlannedStmt);

    READ_ENUM_FIELD(commandType, CmdType);
    READ_UINT_FIELD(queryId);
    READ_BOOL_FIELD(hasReturning);
    READ_BOOL_FIELD(hasModifyingCTE);
    READ_BOOL_FIELD(canSetTag);
    READ_BOOL_FIELD(transientPlan);
    READ_BOOL_FIELD(dependsOnRole);
    READ_BOOL_FIELD(parallelModeNeeded);
    READ_NODE_FIELD(planTree);
    READ_NODE_FIELD(rtable);
    READ_NODE_FIELD(resultRelations);
    READ_NODE_FIELD(nonleafResultRelations);
    READ_NODE_FIELD(rootResultRelations);
    READ_NODE_FIELD(subplans);
    READ_BITMAPSET_FIELD(rewindPlanIDs);
    READ_NODE_FIELD(rowMarks);
    READ_NODE_FIELD(relationOids);
    READ_NODE_FIELD(invalItems);
    READ_INT_FIELD(nParamExec);
    READ_NODE_FIELD(utilityStmt);
    READ_LOCATION_FIELD(stmt_location);
    READ_LOCATION_FIELD(stmt_len);
#ifdef __TBASE__
    READ_BOOL_FIELD(haspart_tobe_modify);
    READ_UINT_FIELD(partrelindex);
    READ_BITMAPSET_FIELD(partpruning);
#endif

#ifdef __AUDIT__
    READ_STRING_FIELD(queryString);
    READ_NODE_FIELD(parseTree);
#endif

    READ_DONE();
}

/*
 * _readResult
 */
static Result *
_readResult(void)
{
    READ_LOCALS(Result);

    ReadCommonPlan(&local_node->plan);

    READ_NODE_FIELD(resconstantqual);

    READ_DONE();
}

/*
 * _readProjectSet
 */
static ProjectSet *
_readProjectSet(void)
{
    READ_LOCALS_NO_FIELDS(ProjectSet);

    ReadCommonPlan(&local_node->plan);

    READ_DONE();
}

/*
 * _readModifyTable
 */
static ModifyTable *
_readModifyTable(void)
{
    READ_LOCALS(ModifyTable);

    ReadCommonPlan(&local_node->plan);

    READ_ENUM_FIELD(operation, CmdType);
    READ_BOOL_FIELD(canSetTag);
    READ_UINT_FIELD(nominalRelation);
    READ_NODE_FIELD(partitioned_rels);
	READ_BOOL_FIELD(partColsUpdated);
    READ_NODE_FIELD(resultRelations);
    READ_INT_FIELD(resultRelIndex);
    READ_INT_FIELD(rootResultRelIndex);
    READ_NODE_FIELD(plans);
    READ_NODE_FIELD(withCheckOptionLists);
    READ_NODE_FIELD(returningLists);
    READ_NODE_FIELD(fdwPrivLists);
    READ_BITMAPSET_FIELD(fdwDirectModifyPlans);
    READ_NODE_FIELD(rowMarks);
    READ_INT_FIELD(epqParam);
    READ_ENUM_FIELD(onConflictAction, OnConflictAction);
    if (portable_input)
        READ_RELID_LIST_FIELD(arbiterIndexes);
    else
        READ_NODE_FIELD(arbiterIndexes);
    READ_NODE_FIELD(onConflictSet);
    READ_NODE_FIELD(onConflictWhere);
    READ_UINT_FIELD(exclRelRTI);
    READ_NODE_FIELD(exclRelTlist);
#ifdef __TBASE__
    READ_BOOL_FIELD(haspartparent);
    READ_UINT_FIELD(partrelidx);
    READ_INT_FIELD(parentplanidx);
    READ_NODE_FIELD(partplans);
    READ_BITMAPSET_FIELD(partpruning);
#endif
    READ_DONE();
}

/*
 * _readAppend
 */
static Append *
_readAppend(void)
{
    READ_LOCALS(Append);

    ReadCommonPlan(&local_node->plan);

    READ_NODE_FIELD(partitioned_rels);
    READ_NODE_FIELD(appendplans);
#ifdef __TBASE__
    READ_BOOL_FIELD(interval);
#endif

    READ_DONE();
}

/*
 * _readMergeAppend
 */
static MergeAppend *
_readMergeAppend(void)
{// #lizard forgives
    int i;
    READ_LOCALS(MergeAppend);

    ReadCommonPlan(&local_node->plan);

    READ_NODE_FIELD(partitioned_rels);
    READ_NODE_FIELD(mergeplans);
    READ_INT_FIELD(numCols);
    READ_ATTRNUMBER_ARRAY(sortColIdx, local_node->numCols);

    token = pg_strtok(&length);        /* skip :sortOperators */
    local_node->sortOperators = (Oid *) palloc(local_node->numCols * sizeof(Oid));
    for (i = 0; i < local_node->numCols; i++)
    {
        token = pg_strtok(&length);
        if (portable_input)
        {
            char       *nspname; /* namespace name */
            char       *oprname; /* operator name */
            char       *leftnspname; /* left type namespace */
            char       *leftname; /* left type name */
            Oid            oprleft; /* left type */
            char       *rightnspname; /* right type namespace */
            char       *rightname; /* right type name */
            Oid            oprright; /* right type */
            /* token is already set to nspname */
            nspname = nullable_string(token, length);
            token = pg_strtok(&length); /* get operator name */
            oprname = nullable_string(token, length);
            token = pg_strtok(&length); /* left type namespace */
            leftnspname = nullable_string(token, length);
            token = pg_strtok(&length); /* left type name */
            leftname = nullable_string(token, length);
            token = pg_strtok(&length); /* right type namespace */
            rightnspname = nullable_string(token, length);
            token = pg_strtok(&length); /* right type name */
            rightname = nullable_string(token, length);
            if (leftname)
                oprleft = get_typname_typid(leftname,
                                            NSP_OID(leftnspname));
            else
                oprleft = InvalidOid;
            if (rightname)
                oprright = get_typname_typid(rightname,
                                             NSP_OID(rightnspname));
            else
                oprright = InvalidOid;
            local_node->sortOperators[i] = get_operid(oprname,
                                                      oprleft,
                                                      oprright,
                                                      NSP_OID(nspname));
        }
        else
        local_node->sortOperators[i] = atooid(token);
    }

    token = pg_strtok(&length);        /* skip :collations */
    local_node->collations = (Oid *) palloc(local_node->numCols * sizeof(Oid));
    for (i = 0; i < local_node->numCols; i++)
    {
        token = pg_strtok(&length);
        if (portable_input)
        {
            char       *nspname; /* namespace name */
            char       *collname; /* collation name */
            int         collencoding; /* collation encoding */
            /* the token is already read */
            nspname = nullable_string(token, length);
            token = pg_strtok(&length); /* get collname */
            collname = nullable_string(token, length);
            token = pg_strtok(&length); /* get nargs */
            collencoding = atoi(token);
            if (collname)
                local_node->collations[i] = get_collid(collname,
                                                       collencoding,
                                                       NSP_OID(nspname));
            else
                local_node->collations[i] = InvalidOid;
        }
        else
        local_node->collations[i] = atooid(token);
    }

    READ_BOOL_ARRAY(nullsFirst, local_node->numCols);
#ifdef __TBASE__
    READ_BOOL_FIELD(interval);
#endif

    READ_DONE();
}

/*
 * _readRecursiveUnion
 */
static RecursiveUnion *
_readRecursiveUnion(void)
{
    READ_LOCALS(RecursiveUnion);

    ReadCommonPlan(&local_node->plan);

    READ_INT_FIELD(wtParam);
    READ_INT_FIELD(numCols);
    READ_ATTRNUMBER_ARRAY(dupColIdx, local_node->numCols);
    READ_OID_ARRAY(dupOperators, local_node->numCols);
    READ_LONG_FIELD(numGroups);

    READ_DONE();
}

/*
 * _readBitmapAnd
 */
static BitmapAnd *
_readBitmapAnd(void)
{
    READ_LOCALS(BitmapAnd);

    ReadCommonPlan(&local_node->plan);

    READ_NODE_FIELD(bitmapplans);

    READ_DONE();
}

/*
 * _readBitmapOr
 */
static BitmapOr *
_readBitmapOr(void)
{
    READ_LOCALS(BitmapOr);

    ReadCommonPlan(&local_node->plan);

    READ_BOOL_FIELD(isshared);
    READ_NODE_FIELD(bitmapplans);

    READ_DONE();
}

/*
 * ReadCommonScan
 *    Assign the basic stuff of all nodes that inherit from Scan
 */
static void
ReadCommonScan(Scan *local_node)
{
    READ_TEMP_LOCALS();

    ReadCommonPlan(&local_node->plan);

    READ_UINT_FIELD(scanrelid);
#ifdef __TBASE__
    READ_BOOL_FIELD(ispartchild);
    READ_INT_FIELD(childidx);
#endif
}

/*
 * _readScan
 */
static Scan *
_readScan(void)
{
    READ_LOCALS_NO_FIELDS(Scan);

    ReadCommonScan(local_node);

    READ_DONE();
}

/*
 * _readSeqScan
 */
static SeqScan *
_readSeqScan(void)
{
    READ_LOCALS_NO_FIELDS(SeqScan);

    ReadCommonScan(local_node);

    READ_DONE();
}

/*
 * _readSampleScan
 */
static SampleScan *
_readSampleScan(void)
{
    READ_LOCALS(SampleScan);

    ReadCommonScan(&local_node->scan);

    READ_NODE_FIELD(tablesample);

    READ_DONE();
}

/*
 * _readIndexScan
 */
static IndexScan *
_readIndexScan(void)
{
    READ_LOCALS(IndexScan);

    ReadCommonScan(&local_node->scan);

    if (portable_input)
        READ_RELID_FIELD(indexid);
    else
        READ_OID_FIELD(indexid);
    READ_NODE_FIELD(indexqual);
    READ_NODE_FIELD(indexqualorig);
    READ_NODE_FIELD(indexorderby);
    READ_NODE_FIELD(indexorderbyorig);
    READ_NODE_FIELD(indexorderbyops);
    READ_ENUM_FIELD(indexorderdir, ScanDirection);

    READ_DONE();
}

/*
 * _readIndexOnlyScan
 */
static IndexOnlyScan *
_readIndexOnlyScan(void)
{
    READ_LOCALS(IndexOnlyScan);

    ReadCommonScan(&local_node->scan);

    if (portable_input)
        READ_RELID_FIELD(indexid);
    else
        READ_OID_FIELD(indexid);
    READ_NODE_FIELD(indexqual);
    READ_NODE_FIELD(indexorderby);
    READ_NODE_FIELD(indextlist);
    READ_ENUM_FIELD(indexorderdir, ScanDirection);

    READ_DONE();
}

/*
 * _readBitmapIndexScan
 */
static BitmapIndexScan *
_readBitmapIndexScan(void)
{
    READ_LOCALS(BitmapIndexScan);

    ReadCommonScan(&local_node->scan);

    if (portable_input)
        READ_RELID_FIELD(indexid);
    else
        READ_OID_FIELD(indexid);
    READ_BOOL_FIELD(isshared);
    READ_NODE_FIELD(indexqual);
    READ_NODE_FIELD(indexqualorig);

    READ_DONE();
}

/*
 * _readBitmapHeapScan
 */
static BitmapHeapScan *
_readBitmapHeapScan(void)
{
    READ_LOCALS(BitmapHeapScan);

    ReadCommonScan(&local_node->scan);

    READ_NODE_FIELD(bitmapqualorig);

    READ_DONE();
}

/*
 * _readTidScan
 */
static TidScan *
_readTidScan(void)
{
    READ_LOCALS(TidScan);

    ReadCommonScan(&local_node->scan);

    READ_NODE_FIELD(tidquals);

    READ_DONE();
}

/*
 * _readSubqueryScan
 */
static SubqueryScan *
_readSubqueryScan(void)
{
    READ_LOCALS(SubqueryScan);

    ReadCommonScan(&local_node->scan);

    READ_NODE_FIELD(subplan);

    READ_DONE();
}

/*
 * _readFunctionScan
 */
static FunctionScan *
_readFunctionScan(void)
{
    READ_LOCALS(FunctionScan);

    ReadCommonScan(&local_node->scan);

    READ_NODE_FIELD(functions);
    READ_BOOL_FIELD(funcordinality);

    READ_DONE();
}

/*
 * _readValuesScan
 */
static ValuesScan *
_readValuesScan(void)
{
    READ_LOCALS(ValuesScan);

    ReadCommonScan(&local_node->scan);

    READ_NODE_FIELD(values_lists);

    READ_DONE();
}

/*
 * _readTableFuncScan
 */
static TableFuncScan *
_readTableFuncScan(void)
{
    READ_LOCALS(TableFuncScan);

    ReadCommonScan(&local_node->scan);

    READ_NODE_FIELD(tablefunc);

    READ_DONE();
}

/*
 * _readCteScan
 */
static CteScan *
_readCteScan(void)
{
    READ_LOCALS(CteScan);

    ReadCommonScan(&local_node->scan);

    READ_INT_FIELD(ctePlanId);
    READ_INT_FIELD(cteParam);

    READ_DONE();
}

/*
 * _readWorkTableScan
 */
static WorkTableScan *
_readWorkTableScan(void)
{
    READ_LOCALS(WorkTableScan);

    ReadCommonScan(&local_node->scan);

    READ_INT_FIELD(wtParam);

    READ_DONE();
}

/*
 * _readForeignScan
 */
static ForeignScan *
_readForeignScan(void)
{
    READ_LOCALS(ForeignScan);

    ReadCommonScan(&local_node->scan);

    READ_ENUM_FIELD(operation, CmdType);
    READ_OID_FIELD(fs_server);
    READ_NODE_FIELD(fdw_exprs);
    READ_NODE_FIELD(fdw_private);
    READ_NODE_FIELD(fdw_scan_tlist);
    READ_NODE_FIELD(fdw_recheck_quals);
    READ_BITMAPSET_FIELD(fs_relids);
    READ_BOOL_FIELD(fsSystemCol);

    READ_DONE();
}

/*
 * _readCustomScan
 */
static CustomScan *
_readCustomScan(void)
{
    READ_LOCALS(CustomScan);
    char       *custom_name;
    const CustomScanMethods *methods;

    ReadCommonScan(&local_node->scan);

    READ_UINT_FIELD(flags);
    READ_NODE_FIELD(custom_plans);
    READ_NODE_FIELD(custom_exprs);
    READ_NODE_FIELD(custom_private);
    READ_NODE_FIELD(custom_scan_tlist);
    READ_BITMAPSET_FIELD(custom_relids);

    /* Lookup CustomScanMethods by CustomName */
    token = pg_strtok(&length); /* skip methods: */
    token = pg_strtok(&length); /* CustomName */
    custom_name = nullable_string(token, length);
    methods = GetCustomScanMethods(custom_name, false);
    local_node->methods = methods;

    READ_DONE();
}

/*
 * ReadCommonJoin
 *    Assign the basic stuff of all nodes that inherit from Join
 */
static void
ReadCommonJoin(Join *local_node)
{
    READ_TEMP_LOCALS();

    ReadCommonPlan(&local_node->plan);

    READ_ENUM_FIELD(jointype, JoinType);
    READ_BOOL_FIELD(inner_unique);
    READ_NODE_FIELD(joinqual);
#ifdef __TBASE__
    READ_BOOL_FIELD(prefetch_inner);
#endif
}

/*
 * _readJoin
 */
static Join *
_readJoin(void)
{
    READ_LOCALS_NO_FIELDS(Join);

    ReadCommonJoin(local_node);

    READ_DONE();
}

/*
 * _readNestLoop
 */
static NestLoop *
_readNestLoop(void)
{
    READ_LOCALS(NestLoop);

    ReadCommonJoin(&local_node->join);

    READ_NODE_FIELD(nestParams);

    READ_DONE();
}

/*
 * _readMergeJoin
 */
static MergeJoin *
_readMergeJoin(void)
{
    int            i;
    int            numCols;

    READ_LOCALS(MergeJoin);

    ReadCommonJoin(&local_node->join);

    READ_BOOL_FIELD(skip_mark_restore);
    READ_NODE_FIELD(mergeclauses);

    numCols = list_length(local_node->mergeclauses);

    READ_OID_ARRAY(mergeFamilies, numCols);

    token = pg_strtok(&length);        /* skip :mergeCollations */
    local_node->mergeCollations = (Oid *) palloc(numCols * sizeof(Oid));
    for (i = 0; i < numCols; i++)
    {
        token = pg_strtok(&length);
        if (portable_input)
        {
            char       *nspname; /* namespace name */
            char       *collname; /* collation name */
            int         collencoding; /* collation encoding */
            /* the token is already read */
            nspname = nullable_string(token, length);
            token = pg_strtok(&length); /* get collname */
            collname = nullable_string(token, length);
            token = pg_strtok(&length); /* get nargs */
            collencoding = atoi(token);
            if (collname)
                local_node->mergeCollations[i] = get_collid(collname,
                                                            collencoding,
                                                            NSP_OID(nspname));
            else
                local_node->mergeCollations[i] = InvalidOid;
        }
        else
        local_node->mergeCollations[i] = atooid(token);
    }

    READ_INT_ARRAY(mergeStrategies, numCols);
    READ_BOOL_ARRAY(mergeNullsFirst, numCols);

    READ_DONE();
}

/*
 * _readHashJoin
 */
static HashJoin *
_readHashJoin(void)
{
    READ_LOCALS(HashJoin);

    ReadCommonJoin(&local_node->join);

    READ_NODE_FIELD(hashclauses);

    READ_DONE();
}

/*
 * _readMaterial
 */
static Material *
_readMaterial(void)
{
    READ_LOCALS_NO_FIELDS(Material);

    ReadCommonPlan(&local_node->plan);

    READ_DONE();
}

/*
 * _readSort
 */
static Sort *
_readSort(void)
{// #lizard forgives
    int i;
    READ_LOCALS(Sort);

    ReadCommonPlan(&local_node->plan);

    READ_INT_FIELD(numCols);

    token = pg_strtok(&length);        /* skip :sortColIdx */
    local_node->sortColIdx = (AttrNumber *) palloc(local_node->numCols * sizeof(AttrNumber));
    for (i = 0; i < local_node->numCols; i++)
    {
        token = pg_strtok(&length);
        local_node->sortColIdx[i] = atoi(token);
    }

    token = pg_strtok(&length);        /* skip :sortOperators */
    local_node->sortOperators = (Oid *) palloc(local_node->numCols * sizeof(Oid));
    for (i = 0; i < local_node->numCols; i++)
    {
        token = pg_strtok(&length);
        if (portable_input)
        {
            char       *nspname; /* namespace name */
            char       *oprname; /* operator name */
            char       *leftnspname; /* left type namespace */
            char       *leftname; /* left type name */
            Oid            oprleft; /* left type */
            char       *rightnspname; /* right type namespace */
            char       *rightname; /* right type name */
            Oid            oprright; /* right type */
            /* token is already set to nspname */
            nspname = nullable_string(token, length);
            token = pg_strtok(&length); /* get operator name */
            oprname = nullable_string(token, length);
            token = pg_strtok(&length); /* left type namespace */
            leftnspname = nullable_string(token, length);
            token = pg_strtok(&length); /* left type name */
            leftname = nullable_string(token, length);
            token = pg_strtok(&length); /* right type namespace */
            rightnspname = nullable_string(token, length);
            token = pg_strtok(&length); /* right type name */
            rightname = nullable_string(token, length);
            if (leftname)
                oprleft = get_typname_typid(leftname,
                                            NSP_OID(leftnspname));
            else
                oprleft = InvalidOid;
            if (rightname)
                oprright = get_typname_typid(rightname,
                                             NSP_OID(rightnspname));
            else
                oprright = InvalidOid;
            local_node->sortOperators[i] = get_operid(oprname,
                                                      oprleft,
                                                      oprright,
                                                      NSP_OID(nspname));
        }
        else
        local_node->sortOperators[i] = atooid(token);
    }

    token = pg_strtok(&length);        /* skip :collations */
    local_node->collations = (Oid *) palloc(local_node->numCols * sizeof(Oid));
    for (i = 0; i < local_node->numCols; i++)
    {
        token = pg_strtok(&length);
        if (portable_input)
        {
            char       *nspname; /* namespace name */
            char       *collname; /* collation name */
            int         collencoding; /* collation encoding */
            /* the token is already read */
            nspname = nullable_string(token, length);
            token = pg_strtok(&length); /* get collname */
            collname = nullable_string(token, length);
            token = pg_strtok(&length); /* get nargs */
            collencoding = atoi(token);
            if (collname)
                local_node->collations[i] = get_collid(collname,
                                                       collencoding,
                                                       NSP_OID(nspname));
            else
                local_node->collations[i] = InvalidOid;
        }
        else
        local_node->collations[i] = atooid(token);
    }

    token = pg_strtok(&length);        /* skip :nullsFirst */
    local_node->nullsFirst = (bool *) palloc(local_node->numCols * sizeof(bool));
    for (i = 0; i < local_node->numCols; i++)
    {
        token = pg_strtok(&length);
        local_node->nullsFirst[i] = strtobool(token);
    }

    READ_DONE();
}

/*
 * _readGroup
 */
static Group *
_readGroup(void)
{
    int    i;
    READ_LOCALS(Group);

    ReadCommonPlan(&local_node->plan);

    READ_INT_FIELD(numCols);

    token = pg_strtok(&length);        /* skip :grpColIdx */
    local_node->grpColIdx = (AttrNumber *) palloc(local_node->numCols * sizeof(AttrNumber));
    for (i = 0; i < local_node->numCols; i++)
    {
        token = pg_strtok(&length);
        local_node->grpColIdx[i] = atoi(token);
    }

    token = pg_strtok(&length);        /* skip :grpOperators */
    local_node->grpOperators = (Oid *) palloc(local_node->numCols * sizeof(Oid));
    for (i = 0; i < local_node->numCols; i++)
    {
        token = pg_strtok(&length);
        if (portable_input)
        {
            char       *nspname; /* namespace name */
            char       *oprname; /* operator name */
            char       *leftnspname; /* left type namespace */
            char       *leftname; /* left type name */
            Oid            oprleft; /* left type */
            char       *rightnspname; /* right type namespace */
            char       *rightname; /* right type name */
            Oid            oprright; /* right type */
            /* token is already set to nspname */
            nspname = nullable_string(token, length);
            token = pg_strtok(&length); /* get operator name */
            oprname = nullable_string(token, length);
            token = pg_strtok(&length); /* left type namespace */
            leftnspname = nullable_string(token, length);
            token = pg_strtok(&length); /* left type name */
            leftname = nullable_string(token, length);
            token = pg_strtok(&length); /* right type namespace */
            rightnspname = nullable_string(token, length);
            token = pg_strtok(&length); /* right type name */
            rightname = nullable_string(token, length);
            if (leftname)
                oprleft = get_typname_typid(leftname,
                                            NSP_OID(leftnspname));
            else
                oprleft = InvalidOid;
            if (rightname)
                oprright = get_typname_typid(rightname,
                                             NSP_OID(rightnspname));
            else
                oprright = InvalidOid;
            local_node->grpOperators[i] = get_operid(oprname,
                                                     oprleft,
                                                     oprright,
                                                     NSP_OID(nspname));
        }
        else
            local_node->grpOperators[i] = atooid(token);
}

    READ_DONE();
}

/*
 * _readAgg
 */
static Agg *
_readAgg(void)
{
    int i;
    READ_LOCALS(Agg);

    ReadCommonPlan(&local_node->plan);

    READ_ENUM_FIELD(aggstrategy, AggStrategy);
    READ_ENUM_FIELD(aggsplit, AggSplit);
    READ_INT_FIELD(numCols);
    READ_ATTRNUMBER_ARRAY(grpColIdx, local_node->numCols);

#ifdef PGXC
    token = pg_strtok(&length);        /* skip :grpOperators */
    local_node->grpOperators = (Oid *) palloc(local_node->numCols * sizeof(Oid));
    for (i = 0; i < local_node->numCols; i++)
    {
        token = pg_strtok(&length);
        if (portable_input)
        {
            char       *nspname; /* namespace name */
            char       *oprname; /* operator name */
            char       *leftnspname; /* left type namespace */
            char       *leftname; /* left type name */
            Oid            oprleft; /* left type */
            char       *rightnspname; /* right type namespace */
            char       *rightname; /* right type name */
            Oid            oprright; /* right type */
            /* token is already set to nspname */
            nspname = nullable_string(token, length);
            token = pg_strtok(&length); /* get operator name */
            oprname = nullable_string(token, length);
            token = pg_strtok(&length); /* left type namespace */
            leftnspname = nullable_string(token, length);
            token = pg_strtok(&length); /* left type name */
            leftname = nullable_string(token, length);
            token = pg_strtok(&length); /* right type namespace */
            rightnspname = nullable_string(token, length);
            token = pg_strtok(&length); /* right type name */
            rightname = nullable_string(token, length);
            if (leftname)
                oprleft = get_typname_typid(leftname,
                                            NSP_OID(leftnspname));
            else
                oprleft = InvalidOid;
            if (rightname)
                oprright = get_typname_typid(rightname,
                                             NSP_OID(rightnspname));
            else
                oprright = InvalidOid;
            local_node->grpOperators[i] = get_operid(oprname,
                                                     oprleft,
                                                     oprright,
                                                     NSP_OID(nspname));
        }
        else
            local_node->grpOperators[i] = atooid(token);
    }
#else
    READ_OID_ARRAY(grpOperators, local_node->numCols);
#endif

    READ_LONG_FIELD(numGroups);
    READ_BITMAPSET_FIELD(aggParams);
    READ_NODE_FIELD(groupingSets);
    READ_NODE_FIELD(chain);
#ifdef __TBASE__
	READ_UINT_FIELD(entrySize);
	READ_BOOL_FIELD(hybrid);
	READ_BOOL_FIELD(noDistinct);
#endif

    READ_DONE();
}

/*
 * _readWindowAgg
 */
static WindowAgg *
_readWindowAgg(void)
{// #lizard forgives
    int i;

    READ_LOCALS(WindowAgg);

    ReadCommonPlan(&local_node->plan);

    READ_UINT_FIELD(winref);
    READ_INT_FIELD(partNumCols);

    token = pg_strtok(&length);        /* skip :partColIdx */
    local_node->partColIdx = (AttrNumber *) palloc(local_node->partNumCols * sizeof(AttrNumber));
    for (i = 0; i < local_node->partNumCols; i++)
    {
        token = pg_strtok(&length);
        local_node->partColIdx[i] = atoi(token);
    }

    token = pg_strtok(&length);        /* skip :partOperators */
    local_node->partOperators = (Oid *) palloc(local_node->partNumCols * sizeof(Oid));
    for (i = 0; i < local_node->partNumCols; i++)
    {
        token = pg_strtok(&length);
        if (portable_input)
        {
            char       *nspname; /* namespace name */
            char       *oprname; /* operator name */
            char       *leftnspname; /* left type namespace */
            char       *leftname; /* left type name */
            Oid            oprleft; /* left type */
            char       *rightnspname; /* right type namespace */
            char       *rightname; /* right type name */
            Oid            oprright; /* right type */
            /* token is already set to nspname */
            nspname = nullable_string(token, length);
            token = pg_strtok(&length); /* get operator name */
            oprname = nullable_string(token, length);
            token = pg_strtok(&length); /* left type namespace */
            leftnspname = nullable_string(token, length);
            token = pg_strtok(&length); /* left type name */
            leftname = nullable_string(token, length);
            token = pg_strtok(&length); /* right type namespace */
            rightnspname = nullable_string(token, length);
            token = pg_strtok(&length); /* right type name */
            rightname = nullable_string(token, length);
            if (leftname)
                oprleft = get_typname_typid(leftname,
                                            NSP_OID(leftnspname));
            else
                oprleft = InvalidOid;
            if (rightname)
                oprright = get_typname_typid(rightname,
                                             NSP_OID(rightnspname));
            else
                oprright = InvalidOid;
            local_node->partOperators[i] = get_operid(oprname,
                                                      oprleft,
                                                      oprright,
                                                      NSP_OID(nspname));
        }
        else
            local_node->partOperators[i] = atooid(token);
    }

    READ_INT_FIELD(ordNumCols);

    token = pg_strtok(&length);        /* skip :ordColIdx */
    local_node->ordColIdx = (AttrNumber *) palloc(local_node->ordNumCols * sizeof(AttrNumber));
    for (i = 0; i < local_node->ordNumCols; i++)
    {
        token = pg_strtok(&length);
        local_node->ordColIdx[i] = atoi(token);
    }

    token = pg_strtok(&length);        /* skip :ordOperators */
    local_node->ordOperators = (Oid *) palloc(local_node->ordNumCols * sizeof(Oid));
    for (i = 0; i < local_node->ordNumCols; i++)
    {
        token = pg_strtok(&length);
        if (portable_input)
        {
            char       *nspname; /* namespace name */
            char       *oprname; /* operator name */
            char       *leftnspname; /* left type namespace */
            char       *leftname; /* left type name */
            Oid            oprleft; /* left type */
            char       *rightnspname; /* right type namespace */
            char       *rightname; /* right type name */
            Oid            oprright; /* right type */
            /* token is already set to nspname */
            nspname = nullable_string(token, length);
            token = pg_strtok(&length); /* get operator name */
            oprname = nullable_string(token, length);
            token = pg_strtok(&length); /* left type namespace */
            leftnspname = nullable_string(token, length);
            token = pg_strtok(&length); /* left type name */
            leftname = nullable_string(token, length);
            token = pg_strtok(&length); /* right type namespace */
            rightnspname = nullable_string(token, length);
            token = pg_strtok(&length); /* right type name */
            rightname = nullable_string(token, length);
            if (leftname)
                oprleft = get_typname_typid(leftname,
                                            NSP_OID(leftnspname));
            else
                oprleft = InvalidOid;
            if (rightname)
                oprright = get_typname_typid(rightname,
                                             NSP_OID(rightnspname));
            else
                oprright = InvalidOid;
            local_node->ordOperators[i] = get_operid(oprname,
                                                     oprleft,
                                                     oprright,
                                                     NSP_OID(nspname));
        }
        else
            local_node->ordOperators[i] = atooid(token);
    }

    READ_INT_FIELD(frameOptions);
    READ_NODE_FIELD(startOffset);
    READ_NODE_FIELD(endOffset);

    READ_DONE();
}

/*
 * _readUnique
 */
static Unique *
_readUnique(void)
{
    int i;
    READ_LOCALS(Unique);

    ReadCommonPlan(&local_node->plan);

    READ_INT_FIELD(numCols);
    READ_ATTRNUMBER_ARRAY(uniqColIdx, local_node->numCols);

    token = pg_strtok(&length);        /* skip :uniqOperators */
    local_node->uniqOperators = (Oid *) palloc(local_node->numCols * sizeof(Oid));
    for (i = 0; i < local_node->numCols; i++)
    {
        token = pg_strtok(&length);
        if (portable_input)
        {
            char       *nspname; /* namespace name */
            char       *oprname; /* operator name */
            char       *leftnspname; /* left type namespace */
            char       *leftname; /* left type name */
            Oid            oprleft; /* left type */
            char       *rightnspname; /* right type namespace */
            char       *rightname; /* right type name */
            Oid            oprright; /* right type */
            /* token is already set to nspname */
            nspname = nullable_string(token, length);
            token = pg_strtok(&length); /* get operator name */
            oprname = nullable_string(token, length);
            token = pg_strtok(&length); /* left type namespace */
            leftnspname = nullable_string(token, length);
            token = pg_strtok(&length); /* left type name */
            leftname = nullable_string(token, length);
            token = pg_strtok(&length); /* right type namespace */
            rightnspname = nullable_string(token, length);
            token = pg_strtok(&length); /* right type name */
            rightname = nullable_string(token, length);
            if (leftname)
                oprleft = get_typname_typid(leftname,
                                            NSP_OID(leftnspname));
            else
                oprleft = InvalidOid;
            if (rightname)
                oprright = get_typname_typid(rightname,
                                             NSP_OID(rightnspname));
            else
                oprright = InvalidOid;
            local_node->uniqOperators[i] = get_operid(oprname,
                                                      oprleft,
                                                      oprright,
                                                      NSP_OID(nspname));
        }
        else
            local_node->uniqOperators[i] = atooid(token);
    }

    READ_DONE();
}

/*
 * _readGather
 */
static Gather *
_readGather(void)
{
    READ_LOCALS(Gather);

    ReadCommonPlan(&local_node->plan);

    READ_INT_FIELD(num_workers);
    READ_BOOL_FIELD(single_copy);
    READ_BOOL_FIELD(invisible);
#ifdef __TBASE__
    READ_BOOL_FIELD(parallelWorker_sendTuple);
#endif

    READ_DONE();
}

/*
 * _readGatherMerge
 */
static GatherMerge *
_readGatherMerge(void)
{
    READ_LOCALS(GatherMerge);

    ReadCommonPlan(&local_node->plan);

    READ_INT_FIELD(num_workers);
    READ_INT_FIELD(numCols);
    READ_ATTRNUMBER_ARRAY(sortColIdx, local_node->numCols);
    READ_OID_ARRAY(sortOperators, local_node->numCols);
    READ_OID_ARRAY(collations, local_node->numCols);
    READ_BOOL_ARRAY(nullsFirst, local_node->numCols);

    READ_DONE();
}

/*
 * _readHash
 */
static Hash *
_readHash(void)
{
    READ_LOCALS(Hash);

    ReadCommonPlan(&local_node->plan);

    if (portable_input)
        READ_RELID_FIELD(skewTable);
    else
        READ_OID_FIELD(skewTable);
    READ_INT_FIELD(skewColumn);
    READ_BOOL_FIELD(skewInherit);

    READ_DONE();
}

/*
 * _readSetOp
 */
static SetOp *
_readSetOp(void)
{
    int i;
    READ_LOCALS(SetOp);

    ReadCommonPlan(&local_node->plan);

    READ_ENUM_FIELD(cmd, SetOpCmd);
    READ_ENUM_FIELD(strategy, SetOpStrategy);

    READ_INT_FIELD(numCols);

    token = pg_strtok(&length);        /* skip :dupColIdx */
    local_node->dupColIdx = (AttrNumber *) palloc(local_node->numCols * sizeof(AttrNumber));
    for (i = 0; i < local_node->numCols; i++)
    {
        token = pg_strtok(&length);
        local_node->dupColIdx[i] = atoi(token);
    }

    token = pg_strtok(&length);        /* skip :dupOperators */
    local_node->dupOperators = (Oid *) palloc(local_node->numCols * sizeof(Oid));
    for (i = 0; i < local_node->numCols; i++)
    {
        if (portable_input)
        {
            char       *nspname; /* namespace name */
            char       *oprname; /* operator name */
            char       *leftnspname; /* left type namespace */
            char       *leftname; /* left type name */
            Oid            oprleft; /* left type */
            char       *rightnspname; /* right type namespace */
            char       *rightname; /* right type name */
            Oid            oprright; /* right type */

            token = pg_strtok(&length);    /* get operator namespace */
            nspname = nullable_string(token, length);

            token = pg_strtok(&length); /* get operator name */
            oprname = nullable_string(token, length);

            token = pg_strtok(&length); /* left type namespace */
            leftnspname = nullable_string(token, length);

            token = pg_strtok(&length); /* left type name */
            leftname = nullable_string(token, length);

            token = pg_strtok(&length); /* right type namespace */
            rightnspname = nullable_string(token, length);

            token = pg_strtok(&length); /* right type name */
            rightname = nullable_string(token, length);

            if (leftname)
                oprleft = get_typname_typid(leftname,
                                            NSP_OID(leftnspname));
            else
                oprleft = InvalidOid;

            if (rightname)
                oprright = get_typname_typid(rightname,
                                             NSP_OID(rightnspname));
            else
                oprright = InvalidOid;

            local_node->dupOperators[i] = get_operid(oprname,
                                                     oprleft,
                                                     oprright,
                                                     NSP_OID(nspname));
        }
        else
        {
            token = pg_strtok(&length);
            local_node->dupOperators[i] = atooid(token);
        }
    }

    READ_INT_FIELD(flagColIdx);
    READ_INT_FIELD(firstFlag);
    READ_LONG_FIELD(numGroups);

    READ_DONE();
}

/*
 * _readLockRows
 */
static LockRows *
_readLockRows(void)
{
    READ_LOCALS(LockRows);

    ReadCommonPlan(&local_node->plan);

    READ_NODE_FIELD(rowMarks);
    READ_INT_FIELD(epqParam);

    READ_DONE();
}

/*
 * _readLimit
 */
static Limit *
_readLimit(void)
{
    READ_LOCALS(Limit);

    ReadCommonPlan(&local_node->plan);

    READ_NODE_FIELD(limitOffset);
    READ_NODE_FIELD(limitCount);
#ifdef __TBASE__
	READ_BOOL_FIELD(skipEarlyFinish);
#endif
    READ_DONE();
}

/*
 * _readNestLoopParam
 */
static NestLoopParam *
_readNestLoopParam(void)
{
    READ_LOCALS(NestLoopParam);

    READ_INT_FIELD(paramno);
    READ_NODE_FIELD(paramval);

    READ_DONE();
}

/*
 * _readPlanRowMark
 */
static PlanRowMark *
_readPlanRowMark(void)
{
    READ_LOCALS(PlanRowMark);

    READ_UINT_FIELD(rti);
    READ_UINT_FIELD(prti);
    READ_UINT_FIELD(rowmarkId);
    READ_ENUM_FIELD(markType, RowMarkType);
    READ_INT_FIELD(allMarkTypes);
    READ_ENUM_FIELD(strength, LockClauseStrength);
    READ_ENUM_FIELD(waitPolicy, LockWaitPolicy);
    READ_BOOL_FIELD(isParent);

    READ_DONE();
}

/*
 * _readPlanInvalItem
 */
static PlanInvalItem *
_readPlanInvalItem(void)
{
    READ_LOCALS(PlanInvalItem);

    READ_INT_FIELD(cacheId);
    READ_UINT_FIELD(hashValue);

    READ_DONE();
}

/*
 * _readSubPlan
 */
static SubPlan *
_readSubPlan(void)
{
    READ_LOCALS(SubPlan);

    READ_ENUM_FIELD(subLinkType, SubLinkType);
    READ_NODE_FIELD(testexpr);
    READ_NODE_FIELD(paramIds);
    READ_INT_FIELD(plan_id);
    READ_STRING_FIELD(plan_name);
    if (portable_input)
        READ_TYPID_FIELD(firstColType);
    else
        READ_OID_FIELD(firstColType);
    READ_INT_FIELD(firstColTypmod);
    if (portable_input)
        READ_COLLID_FIELD(firstColCollation);
    else
        READ_OID_FIELD(firstColCollation);
    READ_BOOL_FIELD(useHashTable);
    READ_BOOL_FIELD(unknownEqFalse);
    READ_BOOL_FIELD(parallel_safe);
    READ_NODE_FIELD(setParam);
    READ_NODE_FIELD(parParam);
    READ_NODE_FIELD(args);
    READ_FLOAT_FIELD(startup_cost);
    READ_FLOAT_FIELD(per_call_cost);

    READ_DONE();
}

/*
 * _readAlternativeSubPlan
 */
static AlternativeSubPlan *
_readAlternativeSubPlan(void)
{
    READ_LOCALS(AlternativeSubPlan);

    READ_NODE_FIELD(subplans);

    READ_DONE();
}

/*
 * _readExtensibleNode
 */
static ExtensibleNode *
_readExtensibleNode(void)
{
    const ExtensibleNodeMethods *methods;
    ExtensibleNode *local_node;
    const char *extnodename;

    READ_TEMP_LOCALS();

    token = pg_strtok(&length); /* skip :extnodename */
    token = pg_strtok(&length); /* get extnodename */

    extnodename = nullable_string(token, length);
    if (!extnodename)
        elog(ERROR, "extnodename has to be supplied");
    methods = GetExtensibleNodeMethods(extnodename, false);

    local_node = (ExtensibleNode *) newNode(methods->node_size,
                                            T_ExtensibleNode);
    local_node->extnodename = extnodename;

    /* deserialize the private fields */
    methods->nodeRead(local_node);

    READ_DONE();
}


/*
 * _readRemoteSubplan
 */
static RemoteSubplan *
_readRemoteSubplan(void)
{
    READ_LOCALS(RemoteSubplan);
    ReadCommonScan(&local_node->scan);

    READ_CHAR_FIELD(distributionType);
    READ_INT_FIELD(distributionKey);
    READ_NODE_FIELD(distributionNodes);
    READ_NODE_FIELD(distributionRestrict);
    READ_NODE_FIELD(nodeList);
    READ_BOOL_FIELD(execOnAll);
    READ_NODE_FIELD(sort);
    READ_STRING_FIELD(cursor);
    READ_INT64_FIELD(unique);
    READ_BOOL_FIELD(parallelWorkerSendTuple);
	READ_BITMAPSET_FIELD(initPlanParams);

    READ_DONE();
}

#ifdef __TBASE__
/*
 * _readRemoteQuery
 */
static RemoteQuery *
_readRemoteQuery(void)
{
    int i;
    READ_LOCALS(RemoteQuery);
    ReadCommonScan(&local_node->scan);
    READ_ENUM_FIELD(exec_direct_type, ExecDirectType);
    READ_STRING_FIELD(sql_statement);
    READ_NODE_FIELD(exec_nodes);
    READ_ENUM_FIELD(combine_type, CombineType);
    READ_NODE_FIELD(sort);
    READ_BOOL_FIELD(read_only);
    READ_BOOL_FIELD(force_autocommit);
    READ_STRING_FIELD(statement);
    READ_STRING_FIELD(cursor);
    READ_INT_FIELD(rq_num_params);
    if (local_node->rq_num_params > 0)
    {
        local_node->rq_param_types = (Oid *) palloc(local_node->rq_num_params * sizeof(Oid));
        token = pg_strtok(&length); /* skip :rq_param_types */
        for (i = 0; i < local_node->rq_num_params; i++)
        {
            token = pg_strtok(&length); 
            local_node->rq_param_types[i] = atooid(token);
        }
    }
    READ_ENUM_FIELD(exec_type, RemoteQueryExecType);
    READ_INT_FIELD(reduce_level);
    READ_STRING_FIELD(outer_alias);
    READ_STRING_FIELD(inner_alias);
    READ_INT_FIELD(outer_reduce_level);
    READ_INT_FIELD(inner_reduce_level);
    READ_BITMAPSET_FIELD(outer_relids);
    READ_BITMAPSET_FIELD(inner_relids);
    READ_STRING_FIELD(inner_statement);
    READ_STRING_FIELD(outer_statement);
    READ_STRING_FIELD(join_condition);
    READ_BOOL_FIELD(has_row_marks);
    READ_BOOL_FIELD(has_ins_child_sel_parent);

    READ_BOOL_FIELD(rq_finalise_aggs);
    READ_BOOL_FIELD(rq_sortgroup_colno);
    READ_NODE_FIELD(remote_query);
    READ_NODE_FIELD(base_tlist);
    READ_NODE_FIELD(coord_var_tlist);
    READ_NODE_FIELD(query_var_tlist);
    READ_BOOL_FIELD(is_temp);

    READ_DONE();
}

static ExecNodes *
_readExecNodes(void)
{
    READ_LOCALS(ExecNodes);

    READ_NODE_FIELD(primarynodelist);
    READ_NODE_FIELD(nodeList);
    READ_CHAR_FIELD(baselocatortype);
    READ_NODE_FIELD(en_expr);
#ifdef __COLD_HOT__
    READ_NODE_FIELD(sec_en_expr);
#endif
#ifdef __TBASE__
    if (portable_input)
    {
        READ_RELID_FIELD(en_relid);
    }
    else
#endif
    READ_OID_FIELD(en_relid);
    READ_ENUM_FIELD(accesstype, RelationAccessType);

    READ_DONE();
}
#endif


#ifdef __AUDIT_FGA__
static AuditFgaPolicy *
_readAuditFgaStmt(void)
    {
        READ_LOCALS(AuditFgaPolicy);
    
        READ_STRING_FIELD(policy_name);
        READ_NODE_FIELD(qual);
        READ_STRING_FIELD(query_string);

        READ_DONE();
    }

#endif


/*
 * _readRemoteStmt
 */
static RemoteStmt *
_readRemoteStmt(void)
{
    int i;
    READ_LOCALS(RemoteStmt);

    READ_ENUM_FIELD(commandType, CmdType);
    READ_BOOL_FIELD(hasReturning);
    READ_NODE_FIELD(planTree);
    READ_NODE_FIELD(rtable);
    READ_NODE_FIELD(resultRelations);
    READ_NODE_FIELD(subplans);
    READ_INT_FIELD(nParamExec);
    READ_INT_FIELD(nParamRemote);
    if (local_node->nParamRemote > 0)
    {
        local_node->remoteparams = (RemoteParam *) palloc(
                local_node->nParamRemote * sizeof(RemoteParam));
        for (i = 0; i < local_node->nParamRemote; i++)
        {
            RemoteParam *rparam = &(local_node->remoteparams[i]);
            token = pg_strtok(&length); /* skip  :paramkind */
            token = pg_strtok(&length);
            rparam->paramkind = (ParamKind) atoi(token);

            token = pg_strtok(&length); /* skip  :paramid */
            token = pg_strtok(&length);
            rparam->paramid = atoi(token);

            token = pg_strtok(&length); /* skip  :paramused */
            token = pg_strtok(&length);
            rparam->paramused = atoi(token);

            token = pg_strtok(&length); /* skip  :paramtype */
            if (portable_input)
            {
                char       *nspname; /* namespace name */
                char       *typname; /* data type name */
                token = pg_strtok(&length); /* get nspname */
                nspname = nullable_string(token, length);
                token = pg_strtok(&length); /* get typname */
                typname = nullable_string(token, length);
                if (typname)
                    rparam->paramtype = get_typname_typid(typname,
                                                          NSP_OID(nspname));
                else
                    rparam->paramtype = InvalidOid;
            }
            else
            {
                token = pg_strtok(&length);
                rparam->paramtype = atooid(token);
            }
        }
    }
    else
        local_node->remoteparams = NULL;

    READ_NODE_FIELD(rowMarks);
    READ_CHAR_FIELD(distributionType);
    READ_INT_FIELD(distributionKey);
    READ_NODE_FIELD(distributionNodes);
    READ_NODE_FIELD(distributionRestrict);
#ifdef __TBASE__
    READ_BOOL_FIELD(parallelModeNeeded);
    READ_BOOL_FIELD(parallelWorkerSendTuple);

    READ_BOOL_FIELD(haspart_tobe_modify);
    READ_UINT_FIELD(partrelindex);
    READ_BITMAPSET_FIELD(partpruning);
#endif

#ifdef __AUDIT__
    READ_STRING_FIELD(queryString);
    READ_NODE_FIELD(parseTree);
#endif

    READ_DONE();
}


/*
 * _readSimpleSort
 */
static SimpleSort *
_readSimpleSort(void)
{// #lizard forgives
    int i;
    READ_LOCALS(SimpleSort);

    READ_INT_FIELD(numCols);

    token = pg_strtok(&length);        /* skip :sortColIdx */
    local_node->sortColIdx = (AttrNumber *) palloc(local_node->numCols * sizeof(AttrNumber));
    for (i = 0; i < local_node->numCols; i++)
    {
        token = pg_strtok(&length);
        local_node->sortColIdx[i] = atoi(token);
    }

    token = pg_strtok(&length);        /* skip :sortOperators */
    local_node->sortOperators = (Oid *) palloc(local_node->numCols * sizeof(Oid));
    for (i = 0; i < local_node->numCols; i++)
    {
        token = pg_strtok(&length);
        if (portable_input)
        {
            char       *nspname; /* namespace name */
            char       *oprname; /* operator name */
            char       *leftnspname; /* left type namespace */
            char       *leftname; /* left type name */
            Oid            oprleft; /* left type */
            char       *rightnspname; /* right type namespace */
            char       *rightname; /* right type name */
            Oid            oprright; /* right type */
            /* token is already set to nspname */
            nspname = nullable_string(token, length);
            token = pg_strtok(&length); /* get operator name */
            oprname = nullable_string(token, length);
            token = pg_strtok(&length); /* left type namespace */
            leftnspname = nullable_string(token, length);
            token = pg_strtok(&length); /* left type name */
            leftname = nullable_string(token, length);
            token = pg_strtok(&length); /* right type namespace */
            rightnspname = nullable_string(token, length);
            token = pg_strtok(&length); /* right type name */
            rightname = nullable_string(token, length);
            if (leftname)
                oprleft = get_typname_typid(leftname,
                                            NSP_OID(leftnspname));
            else
                oprleft = InvalidOid;
            if (rightname)
                oprright = get_typname_typid(rightname,
                                             NSP_OID(rightnspname));
            else
                oprright = InvalidOid;
            local_node->sortOperators[i] = get_operid(oprname,
                                                      oprleft,
                                                      oprright,
                                                      NSP_OID(nspname));
        }
        else
            local_node->sortOperators[i] = atooid(token);
    }

    token = pg_strtok(&length);        /* skip :sortCollations */
    local_node->sortCollations = (Oid *) palloc(local_node->numCols * sizeof(Oid));
    for (i = 0; i < local_node->numCols; i++)
    {
        token = pg_strtok(&length);
        if (portable_input)
        {
            char       *nspname; /* namespace name */
            char       *collname; /* collation name */
            int         collencoding; /* collation encoding */
            /* the token is already read */
            nspname = nullable_string(token, length);
            token = pg_strtok(&length); /* get collname */
            collname = nullable_string(token, length);
            token = pg_strtok(&length); /* get nargs */
            collencoding = atoi(token);
            if (collname)
                local_node->sortCollations[i] = get_collid(collname,
                                                       collencoding,
                                                       NSP_OID(nspname));
            else
                local_node->sortCollations[i] = InvalidOid;
        }
        else
            local_node->sortCollations[i] = atooid(token);
    }

    token = pg_strtok(&length);        /* skip :nullsFirst */
    local_node->nullsFirst = (bool *) palloc(local_node->numCols * sizeof(bool));
    for (i = 0; i < local_node->numCols; i++)
    {
        token = pg_strtok(&length);
        local_node->nullsFirst[i] = strtobool(token);
    }

    READ_DONE();
}


/*
 * _readPartitionBoundSpec
 */
static PartitionBoundSpec *
_readPartitionBoundSpec(void)
{
    READ_LOCALS(PartitionBoundSpec);

    READ_CHAR_FIELD(strategy);
	READ_BOOL_FIELD(is_default);
	READ_INT_FIELD(modulus);
	READ_INT_FIELD(remainder);
    READ_NODE_FIELD(listdatums);
    READ_NODE_FIELD(lowerdatums);
    READ_NODE_FIELD(upperdatums);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readPartitionRangeDatum
 */
static PartitionRangeDatum *
_readPartitionRangeDatum(void)
{
    READ_LOCALS(PartitionRangeDatum);

    READ_ENUM_FIELD(kind, PartitionRangeDatumKind);
    READ_NODE_FIELD(value);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

#ifdef __AUDIT__

static AuditStmt *
_readAuditStmt(void)
{
    READ_LOCALS(AuditStmt);

    READ_BOOL_FIELD(audit_ison);
    READ_ENUM_FIELD(audit_type, AuditType);
    READ_ENUM_FIELD(audit_mode, AuditMode);
    READ_NODE_FIELD(action_list);
    READ_NODE_FIELD(user_list);
    READ_NODE_FIELD(object_name);
    READ_ENUM_FIELD(object_type, ObjectType);

    READ_DONE();
}

static CleanAuditStmt *
_readCleanAuditStmt(void)
{
    READ_LOCALS(CleanAuditStmt);

    READ_ENUM_FIELD(clean_type, CleanAuditType);
    READ_NODE_FIELD(user_list);
    READ_NODE_FIELD(object_name);
    READ_ENUM_FIELD(object_type, ObjectType);

    READ_DONE();
}
#endif

#ifdef __TBASE__
static PlaceHolderVar *
_readPlaceHolderVar(void)
{
    READ_LOCALS(PlaceHolderVar);

    READ_NODE_FIELD(phexpr);
    READ_BITMAPSET_FIELD(phrels);
    READ_UINT_FIELD(phid);
    READ_UINT_FIELD(phlevelsup);

    READ_DONE();
}

static PlaceHolderInfo *
_readPlaceHolderInfo(void)
{
    READ_LOCALS(PlaceHolderInfo);

    READ_UINT_FIELD(phid);
    READ_NODE_FIELD(ph_var);
    READ_BITMAPSET_FIELD(ph_eval_at);
    READ_BITMAPSET_FIELD(ph_lateral);
    READ_BITMAPSET_FIELD(ph_needed);
    READ_INT_FIELD(ph_width);

    READ_DONE();
}
#endif
#ifdef __TBASE__
static PartitionBy *
_readPartitionBy(void)
{
    READ_LOCALS(PartitionBy);

    READ_CHAR_FIELD(strategy);
    READ_STRING_FIELD(colname);
    READ_INT_FIELD(colattr);        
    READ_INT_FIELD(intervaltype);  
    if (portable_input)
    {
        READ_TYPID_FIELD(partdatatype);
    }
    else
    {
        READ_OID_FIELD(partdatatype);   
    }
    READ_NODE_FIELD(startvalue);    
    READ_NODE_FIELD(step);          
    READ_INT_FIELD(interval);       
    READ_INT_FIELD(nPartitions);  

    READ_DONE();
}

static AddDropPartitions *
_readAddDropPartitions(void)
{
    READ_LOCALS(AddDropPartitions);

    READ_INT_FIELD(nparts);

    READ_DONE();
}

static PartitionForExpr *
_readPartitionForExpr(void)
{
    READ_LOCALS(PartitionForExpr);

    READ_BOOL_FIELD(isdefault);
    READ_NODE_FIELD(router_src);

    READ_DONE();
}

#endif

/*
 * parseNodeString
 *
 * Given a character string representing a node tree, parseNodeString creates
 * the internal node structure.
 *
 * The string to be read must already have been loaded into pg_strtok().
 */
Node *
parseNodeString(void)
{// #lizard forgives
    void       *return_value;

    READ_TEMP_LOCALS();

    token = pg_strtok(&length);

#define MATCH(tokname, namelen) \
    (length == namelen && memcmp(token, tokname, namelen) == 0)

    if (MATCH("QUERY", 5))
        return_value = _readQuery();
    else if (MATCH("WITHCHECKOPTION", 15))
        return_value = _readWithCheckOption();
    else if (MATCH("SORTGROUPCLAUSE", 15))
        return_value = _readSortGroupClause();
    else if (MATCH("GROUPINGSET", 11))
        return_value = _readGroupingSet();
    else if (MATCH("WINDOWCLAUSE", 12))
        return_value = _readWindowClause();
    else if (MATCH("ROWMARKCLAUSE", 13))
        return_value = _readRowMarkClause();
    else if (MATCH("COMMONTABLEEXPR", 15))
        return_value = _readCommonTableExpr();
    else if (MATCH("SETOPERATIONSTMT", 16))
        return_value = _readSetOperationStmt();
    else if (MATCH("ALIAS", 5))
        return_value = _readAlias();
    else if (MATCH("RANGEVAR", 8))
        return_value = _readRangeVar();
    else if (MATCH("INTOCLAUSE", 10))
        return_value = _readIntoClause();
    else if (MATCH("TABLEFUNC", 9))
        return_value = _readTableFunc();
    else if (MATCH("VAR", 3))
        return_value = _readVar();
    else if (MATCH("CONST", 5))
        return_value = _readConst();
    else if (MATCH("PARAM", 5))
        return_value = _readParam();
    else if (MATCH("AGGREF", 6))
        return_value = _readAggref();
    else if (MATCH("GROUPINGFUNC", 12))
        return_value = _readGroupingFunc();
    else if (MATCH("WINDOWFUNC", 10))
        return_value = _readWindowFunc();
    else if (MATCH("ARRAYREF", 8))
        return_value = _readArrayRef();
    else if (MATCH("FUNCEXPR", 8))
        return_value = _readFuncExpr();
    else if (MATCH("NAMEDARGEXPR", 12))
        return_value = _readNamedArgExpr();
    else if (MATCH("OPEXPR", 6))
        return_value = _readOpExpr();
    else if (MATCH("DISTINCTEXPR", 12))
        return_value = _readDistinctExpr();
    else if (MATCH("NULLIFEXPR", 10))
        return_value = _readNullIfExpr();
    else if (MATCH("SCALARARRAYOPEXPR", 17))
        return_value = _readScalarArrayOpExpr();
    else if (MATCH("BOOLEXPR", 8))
        return_value = _readBoolExpr();
    else if (MATCH("SUBLINK", 7))
        return_value = _readSubLink();
    else if (MATCH("FIELDSELECT", 11))
        return_value = _readFieldSelect();
    else if (MATCH("FIELDSTORE", 10))
        return_value = _readFieldStore();
    else if (MATCH("RELABELTYPE", 11))
        return_value = _readRelabelType();
    else if (MATCH("COERCEVIAIO", 11))
        return_value = _readCoerceViaIO();
    else if (MATCH("ARRAYCOERCEEXPR", 15))
        return_value = _readArrayCoerceExpr();
    else if (MATCH("CONVERTROWTYPEEXPR", 18))
        return_value = _readConvertRowtypeExpr();
    else if (MATCH("COLLATE", 7))
        return_value = _readCollateExpr();
    else if (MATCH("CASE", 4))
        return_value = _readCaseExpr();
    else if (MATCH("WHEN", 4))
        return_value = _readCaseWhen();
    else if (MATCH("CASETESTEXPR", 12))
        return_value = _readCaseTestExpr();
    else if (MATCH("ARRAY", 5))
        return_value = _readArrayExpr();
    else if (MATCH("ROW", 3))
        return_value = _readRowExpr();
    else if (MATCH("ROWCOMPARE", 10))
        return_value = _readRowCompareExpr();
    else if (MATCH("COALESCE", 8))
        return_value = _readCoalesceExpr();
    else if (MATCH("MINMAX", 6))
        return_value = _readMinMaxExpr();
    else if (MATCH("SQLVALUEFUNCTION", 16))
        return_value = _readSQLValueFunction();
    else if (MATCH("NEXTVALUEEXPR", 13))
        return_value = _readNextValueExpr();
    else if (MATCH("XMLEXPR", 7))
        return_value = _readXmlExpr();
    else if (MATCH("NULLTEST", 8))
        return_value = _readNullTest();
    else if (MATCH("BOOLEANTEST", 11))
        return_value = _readBooleanTest();
    else if (MATCH("COERCETODOMAIN", 14))
        return_value = _readCoerceToDomain();
    else if (MATCH("COERCETODOMAINVALUE", 19))
        return_value = _readCoerceToDomainValue();
    else if (MATCH("SETTODEFAULT", 12))
        return_value = _readSetToDefault();
    else if (MATCH("CURRENTOFEXPR", 13))
        return_value = _readCurrentOfExpr();
    else if (MATCH("NEXTVALUEEXPR", 13))
        return_value = _readNextValueExpr();
    else if (MATCH("INFERENCEELEM", 13))
        return_value = _readInferenceElem();
    else if (MATCH("TARGETENTRY", 11))
        return_value = _readTargetEntry();
    else if (MATCH("RANGETBLREF", 11))
        return_value = _readRangeTblRef();
    else if (MATCH("JOINEXPR", 8))
        return_value = _readJoinExpr();
    else if (MATCH("FROMEXPR", 8))
        return_value = _readFromExpr();
    else if (MATCH("ONCONFLICTEXPR", 14))
        return_value = _readOnConflictExpr();
	else if (MATCH("PARTITIONPRUNESTEPOP", 20))
        return_value = _readPartitionPruneStepOp();
    else if (MATCH("PARTITIONPRUNESTEPCOMBINE", 25))
        return_value = _readPartitionPruneStepCombine();
    else if (MATCH("RTE", 3))
        return_value = _readRangeTblEntry();
    else if (MATCH("RANGETBLFUNCTION", 16))
        return_value = _readRangeTblFunction();
    else if (MATCH("TABLESAMPLECLAUSE", 17))
        return_value = _readTableSampleClause();
    else if (MATCH("NOTIFY", 6))
        return_value = _readNotifyStmt();
    else if (MATCH("DEFELEM", 7))
        return_value = _readDefElem();
    else if (MATCH("DECLARECURSOR", 13))
        return_value = _readDeclareCursorStmt();
    else if (MATCH("PLANNEDSTMT", 11))
        return_value = _readPlannedStmt();
    else if (MATCH("PLAN", 4))
        return_value = _readPlan();
    else if (MATCH("RESULT", 6))
        return_value = _readResult();
    else if (MATCH("PROJECTSET", 10))
        return_value = _readProjectSet();
    else if (MATCH("MODIFYTABLE", 11))
        return_value = _readModifyTable();
    else if (MATCH("APPEND", 6))
        return_value = _readAppend();
    else if (MATCH("MERGEAPPEND", 11))
        return_value = _readMergeAppend();
    else if (MATCH("RECURSIVEUNION", 14))
        return_value = _readRecursiveUnion();
    else if (MATCH("BITMAPAND", 9))
        return_value = _readBitmapAnd();
    else if (MATCH("BITMAPOR", 8))
        return_value = _readBitmapOr();
    else if (MATCH("SCAN", 4))
        return_value = _readScan();
    else if (MATCH("SEQSCAN", 7))
        return_value = _readSeqScan();
    else if (MATCH("SAMPLESCAN", 10))
        return_value = _readSampleScan();
    else if (MATCH("INDEXSCAN", 9))
        return_value = _readIndexScan();
    else if (MATCH("INDEXONLYSCAN", 13))
        return_value = _readIndexOnlyScan();
    else if (MATCH("BITMAPINDEXSCAN", 15))
        return_value = _readBitmapIndexScan();
    else if (MATCH("BITMAPHEAPSCAN", 14))
        return_value = _readBitmapHeapScan();
    else if (MATCH("TIDSCAN", 7))
        return_value = _readTidScan();
    else if (MATCH("SUBQUERYSCAN", 12))
        return_value = _readSubqueryScan();
    else if (MATCH("FUNCTIONSCAN", 12))
        return_value = _readFunctionScan();
    else if (MATCH("VALUESSCAN", 10))
        return_value = _readValuesScan();
    else if (MATCH("TABLEFUNCSCAN", 13))
        return_value = _readTableFuncScan();
    else if (MATCH("CTESCAN", 7))
        return_value = _readCteScan();
    else if (MATCH("WORKTABLESCAN", 13))
        return_value = _readWorkTableScan();
    else if (MATCH("FOREIGNSCAN", 11))
        return_value = _readForeignScan();
    else if (MATCH("CUSTOMSCAN", 10))
        return_value = _readCustomScan();
    else if (MATCH("JOIN", 4))
        return_value = _readJoin();
    else if (MATCH("NESTLOOP", 8))
        return_value = _readNestLoop();
    else if (MATCH("MERGEJOIN", 9))
        return_value = _readMergeJoin();
    else if (MATCH("HASHJOIN", 8))
        return_value = _readHashJoin();
    else if (MATCH("MATERIAL", 8))
        return_value = _readMaterial();
    else if (MATCH("SORT", 4))
        return_value = _readSort();
    else if (MATCH("GROUP", 5))
        return_value = _readGroup();
    else if (MATCH("AGG", 3))
        return_value = _readAgg();
    else if (MATCH("WINDOWAGG", 9))
        return_value = _readWindowAgg();
    else if (MATCH("UNIQUE", 6))
        return_value = _readUnique();
    else if (MATCH("GATHER", 6))
        return_value = _readGather();
    else if (MATCH("GATHERMERGE", 11))
        return_value = _readGatherMerge();
    else if (MATCH("HASH", 4))
        return_value = _readHash();
    else if (MATCH("SETOP", 5))
        return_value = _readSetOp();
    else if (MATCH("LOCKROWS", 8))
        return_value = _readLockRows();
    else if (MATCH("LIMIT", 5))
        return_value = _readLimit();
    else if (MATCH("NESTLOOPPARAM", 13))
        return_value = _readNestLoopParam();
    else if (MATCH("PLANROWMARK", 11))
        return_value = _readPlanRowMark();
    else if (MATCH("PLANINVALITEM", 13))
        return_value = _readPlanInvalItem();
    else if (MATCH("SUBPLAN", 7))
        return_value = _readSubPlan();
    else if (MATCH("ALTERNATIVESUBPLAN", 18))
        return_value = _readAlternativeSubPlan();
    else if (MATCH("EXTENSIBLENODE", 14))
        return_value = _readExtensibleNode();
    else if (MATCH("REMOTESUBPLAN", 13))
        return_value = _readRemoteSubplan();
    else if (MATCH("REMOTESTMT", 10))
        return_value = _readRemoteStmt();
    else if (MATCH("SIMPLESORT", 10))
        return_value = _readSimpleSort();
    else if (MATCH("PARTITIONBOUNDSPEC", 18))
        return_value = _readPartitionBoundSpec();
    else if (MATCH("PARTITIONRANGEDATUM", 19))
        return_value = _readPartitionRangeDatum();
#ifdef __TBASE__
    else if (MATCH("PARTITIONBY", 11))
        return_value = _readPartitionBy();
    else if (MATCH("ADDDROPPARTITIONS", 17))
        return_value = _readAddDropPartitions();
    else if (MATCH("PARTITIONFOREXPR", 16))
        return_value = _readPartitionForExpr();
#endif
#ifdef __AUDIT_FGA__
    else if (MATCH("AUDITFGAPOLICY", 14))
        return_value = _readAuditFgaStmt();
#endif
#ifdef __TBASE__
    else if (MATCH("REMOTEQUERY", 11))
        return_value = _readRemoteQuery();    
    else if (MATCH("EXEC_NODES", 10))
        return_value = _readExecNodes();
    else if (MATCH("PLACEHOLDERVAR", 14))
        return_value = _readPlaceHolderVar();
    else if (MATCH("PLACEHOLDERINFO", 15))
        return_value = _readPlaceHolderInfo();
#endif
#ifdef __AUDIT__
    else if (MATCH("AUDIT", 5))
        return_value = _readAuditStmt();
    else if (MATCH("CLEAN_AUDIT", 11))
        return_value = _readCleanAuditStmt();
#endif
    else
    {
        elog(ERROR, "badly formatted node string \"%.32s\"...", token);
        return_value = NULL;    /* keep compiler quiet */
    }

    return (Node *) return_value;
}


/*
 * readDatum
 *
 * Given a string representation of a constant, recreate the appropriate
 * Datum.  The string representation embeds length info, but not byValue,
 * so we must be told that.
 */
Datum
readDatum(bool typbyval)
{// #lizard forgives
    Size        length,
                i;
    int            tokenLength;
    char       *token;
    Datum        res;
    char       *s;

    /*
     * read the actual length of the value
     */
    token = pg_strtok(&tokenLength);
    length = atoui(token);

    token = pg_strtok(&tokenLength);    /* read the '[' */
    if (token == NULL || token[0] != '[')
        elog(ERROR, "expected \"[\" to start datum, but got \"%s\"; length = %zu",
             token ? (const char *) token : "[NULL]", length);

    if (typbyval)
    {
        if (length > (Size) sizeof(Datum))
            elog(ERROR, "byval datum but length = %zu", length);
        res = (Datum) 0;
        s = (char *) (&res);
        for (i = 0; i < (Size) sizeof(Datum); i++)
        {
            token = pg_strtok(&tokenLength);
            s[i] = (char) atoi(token);
        }
    }
    else if (length <= 0)
        res = (Datum) NULL;
    else
    {
        s = (char *) palloc(length);
        for (i = 0; i < length; i++)
        {
            token = pg_strtok(&tokenLength);
            s[i] = (char) atoi(token);
        }
        res = PointerGetDatum(s);
    }

    token = pg_strtok(&tokenLength);    /* read the ']' */
    if (token == NULL || token[0] != ']')
        elog(ERROR, "expected \"]\" to end datum, but got \"%s\"; length = %zu",
             token ? (const char *) token : "[NULL]", length);

    return res;
}

#ifdef XCP
/*
 * scanDatum
 *
 * Recreate Datum from the text format understandable by the input function
 * of the specified data type.
 */
static Datum
scanDatum(Oid typid, int typmod)
{
    Oid            typInput;
    Oid            typioparam;
    FmgrInfo    finfo;
    FunctionCallInfoData fcinfo;
    char       *value;
    Datum        res;
    READ_TEMP_LOCALS();

    /* Get input function for the type */
    getTypeInputInfo(typid, &typInput, &typioparam);
    fmgr_info(typInput, &finfo);

    /* Read the value */
    token = pg_strtok(&length);
    value = nullable_string(token, length);

    /* The value can not be NULL, so we actually received empty string */
    if (value == NULL)
        value = "";

    /* Invoke input function */
    InitFunctionCallInfoData(fcinfo, &finfo, 3, InvalidOid, NULL, NULL);

    fcinfo.arg[0] = CStringGetDatum(value);
    fcinfo.arg[1] = ObjectIdGetDatum(typioparam);
    fcinfo.arg[2] = Int32GetDatum(typmod);
    fcinfo.argnull[0] = false;
    fcinfo.argnull[1] = false;
    fcinfo.argnull[2] = false;

    res = FunctionCallInvoke(&fcinfo);

    return res;
}
#endif

/*
 * readAttrNumberCols
 */
AttrNumber *
readAttrNumberCols(int numCols)
{
    int            tokenLength,
                i;
    char       *token;
    AttrNumber *attr_vals;

    if (numCols <= 0)
        return NULL;

    attr_vals = (AttrNumber *) palloc(numCols * sizeof(AttrNumber));
    for (i = 0; i < numCols; i++)
    {
        token = pg_strtok(&tokenLength);
        attr_vals[i] = atoi(token);
    }

    return attr_vals;
}

/*
 * readOidCols
 */
Oid *
readOidCols(int numCols)
{
    int            tokenLength,
                i;
    char       *token;
    Oid           *oid_vals;

    if (numCols <= 0)
        return NULL;

    oid_vals = (Oid *) palloc(numCols * sizeof(Oid));
    for (i = 0; i < numCols; i++)
    {
        token = pg_strtok(&tokenLength);
        oid_vals[i] = atooid(token);
    }

    return oid_vals;
}

/*
 * readIntCols
 */
int *
readIntCols(int numCols)
{
    int            tokenLength,
                i;
    char       *token;
    int           *int_vals;

    if (numCols <= 0)
        return NULL;

    int_vals = (int *) palloc(numCols * sizeof(int));
    for (i = 0; i < numCols; i++)
    {
        token = pg_strtok(&tokenLength);
        int_vals[i] = atoi(token);
    }

    return int_vals;
}

/*
 * readBoolCols
 */
bool *
readBoolCols(int numCols)
{
    int            tokenLength,
                i;
    char       *token;
    bool       *bool_vals;

    if (numCols <= 0)
        return NULL;

    bool_vals = (bool *) palloc(numCols * sizeof(bool));
    for (i = 0; i < numCols; i++)
    {
        token = pg_strtok(&tokenLength);
        bool_vals[i] = strtobool(token);
    }

    return bool_vals;
}
