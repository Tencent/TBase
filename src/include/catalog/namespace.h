/*
 * Tencent is pleased to support the open source community by making TBase available.  
 * 
 * Copyright (C) 2019 THL A29 Limited, a Tencent company.  All rights reserved.
 * 
 * TBase is licensed under the BSD 3-Clause License, except for the third-party component listed below. 
 * 
 * A copy of the BSD 3-Clause License is included in this file.
 * 
 * Other dependencies and licenses:
 * 
 * Open Source Software Licensed Under the PostgreSQL License: 
 * --------------------------------------------------------------------
 * 1. Postgres-XL XL9_5_STABLE
 * Portions Copyright (c) 2015-2016, 2ndQuadrant Ltd
 * Portions Copyright (c) 2012-2015, TransLattice, Inc.
 * Portions Copyright (c) 2010-2017, Postgres-XC Development Group
 * Portions Copyright (c) 1996-2015, The PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, The Regents of the University of California
 * 
 * Terms of the PostgreSQL License: 
 * --------------------------------------------------------------------
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written agreement
 * is hereby granted, provided that the above copyright notice and this
 * paragraph and the following two paragraphs appear in all copies.
 * 
 * IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
 * DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * 
 * THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
 * ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 * 
 * 
 * Terms of the BSD 3-Clause License:
 * --------------------------------------------------------------------
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 * 
 * 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation 
 * and/or other materials provided with the distribution.
 * 
 * 3. Neither the name of THL A29 Limited nor the names of its contributors may be used to endorse or promote products derived from this software without 
 * specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, 
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS 
 * BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE 
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT 
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH 
 * DAMAGE.
 * 
 */
/*-------------------------------------------------------------------------
 *
 * namespace.h
 *      prototypes for functions in backend/catalog/namespace.c
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/namespace.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NAMESPACE_H
#define NAMESPACE_H

#include "nodes/primnodes.h"
#include "storage/lock.h"


/*
 *    This structure holds a list of possible functions or operators
 *    found by namespace lookup.  Each function/operator is identified
 *    by OID and by argument types; the list must be pruned by type
 *    resolution rules that are embodied in the parser, not here.
 *    See FuncnameGetCandidates's comments for more info.
 */
typedef struct _FuncCandidateList
{
    struct _FuncCandidateList *next;
    int            pathpos;        /* for internal use of namespace lookup */
    Oid            oid;            /* the function or operator's OID */
    int            nargs;            /* number of arg types returned */
    int            nvargs;            /* number of args to become variadic array */
    int            ndargs;            /* number of defaulted args */
    int           *argnumbers;        /* args' positional indexes, if named call */
    Oid            args[FLEXIBLE_ARRAY_MEMBER];    /* arg types */
}           *FuncCandidateList;

/*
 *    Structure for xxxOverrideSearchPath functions
 */
typedef struct OverrideSearchPath
{
    List       *schemas;        /* OIDs of explicitly named schemas */
    bool        addCatalog;        /* implicitly prepend pg_catalog? */
    bool        addTemp;        /* implicitly prepend temp schema? */
} OverrideSearchPath;

typedef void (*RangeVarGetRelidCallback) (const RangeVar *relation, Oid relId,
                                          Oid oldRelId, void *callback_arg);

#define RangeVarGetRelid(relation, lockmode, missing_ok) \
    RangeVarGetRelidExtended(relation, lockmode, missing_ok, false, NULL, NULL)

extern Oid RangeVarGetRelidExtended(const RangeVar *relation,
                         LOCKMODE lockmode, bool missing_ok, bool nowait,
                         RangeVarGetRelidCallback callback,
                         void *callback_arg);
extern Oid    RangeVarGetCreationNamespace(const RangeVar *newRelation);
extern Oid RangeVarGetAndCheckCreationNamespace(RangeVar *newRelation,
                                     LOCKMODE lockmode,
                                     Oid *existing_relation_id);
extern void RangeVarAdjustRelationPersistence(RangeVar *newRelation, Oid nspid);
extern Oid    RelnameGetRelid(const char *relname);
extern bool RelationIsVisible(Oid relid);
extern char *TypidGetTypename(Oid typid);
extern Oid    TypenameGetTypid(const char *typname);
extern bool TypeIsVisible(Oid typid);

extern FuncCandidateList FuncnameGetCandidates(List *names,
                      int nargs, List *argnames,
                      bool expand_variadic,
                      bool expand_defaults,
                      bool missing_ok);
extern bool FunctionIsVisible(Oid funcid);

extern Oid    OpernameGetOprid(List *names, Oid oprleft, Oid oprright);
extern FuncCandidateList OpernameGetCandidates(List *names, char oprkind,
                      bool missing_schema_ok);
extern bool OperatorIsVisible(Oid oprid);

extern Oid    OpclassnameGetOpcid(Oid amid, const char *opcname);
extern bool OpclassIsVisible(Oid opcid);

extern Oid    OpfamilynameGetOpfid(Oid amid, const char *opfname);
extern bool OpfamilyIsVisible(Oid opfid);

extern Oid    CollationGetCollid(const char *collname);
extern bool CollationIsVisible(Oid collid);

extern Oid    ConversionGetConid(const char *conname);
extern bool ConversionIsVisible(Oid conid);

extern Oid    get_statistics_object_oid(List *names, bool missing_ok);
extern bool StatisticsObjIsVisible(Oid stxid);

extern Oid    get_ts_parser_oid(List *names, bool missing_ok);
extern bool TSParserIsVisible(Oid prsId);

extern Oid    get_ts_dict_oid(List *names, bool missing_ok);
extern bool TSDictionaryIsVisible(Oid dictId);

extern Oid    get_ts_template_oid(List *names, bool missing_ok);
extern bool TSTemplateIsVisible(Oid tmplId);

extern Oid    get_ts_config_oid(List *names, bool missing_ok);
extern bool TSConfigIsVisible(Oid cfgid);

extern void DeconstructQualifiedName(List *names,
                         char **nspname_p,
                         char **objname_p);
extern Oid    LookupNamespaceNoError(const char *nspname);
extern Oid    LookupExplicitNamespace(const char *nspname, bool missing_ok);
extern Oid    get_namespace_oid(const char *nspname, bool missing_ok);

extern Oid    LookupCreationNamespace(const char *nspname);
extern void CheckSetNamespace(Oid oldNspOid, Oid nspOid);
extern Oid    QualifiedNameGetCreationNamespace(List *names, char **objname_p);
extern RangeVar *makeRangeVarFromNameList(List *names);
extern char *NameListToString(List *names);
extern char *NameListToQuotedString(List *names);

extern bool isTempNamespace(Oid namespaceId);
extern bool isTempToastNamespace(Oid namespaceId);
extern bool isTempOrTempToastNamespace(Oid namespaceId);
extern bool isAnyTempNamespace(Oid namespaceId);
extern bool isOtherTempNamespace(Oid namespaceId);
extern int    GetTempNamespaceBackendId(Oid namespaceId);
extern Oid    GetTempToastNamespace(void);
extern void GetTempNamespaceState(Oid *tempNamespaceId,
                      Oid *tempToastNamespaceId);
extern void SetTempNamespaceState(Oid tempNamespaceId,
                      Oid tempToastNamespaceId);
extern void ResetTempTableNamespace(void);
#ifdef XCP
extern void ForgetTempTableNamespace(void);
#endif

extern OverrideSearchPath *GetOverrideSearchPath(MemoryContext context);
extern OverrideSearchPath *CopyOverrideSearchPath(OverrideSearchPath *path);
extern bool OverrideSearchPathMatchesCurrent(OverrideSearchPath *path);
extern void PushOverrideSearchPath(OverrideSearchPath *newpath);
extern void PopOverrideSearchPath(void);

extern Oid    get_collation_oid(List *collname, bool missing_ok);
extern Oid    get_conversion_oid(List *conname, bool missing_ok);
extern Oid    FindDefaultConversionProc(int32 for_encoding, int32 to_encoding);


/* initialization & transaction cleanup code */
extern void InitializeSearchPath(void);
extern void AtEOXact_Namespace(bool isCommit, bool parallel);
extern void AtEOSubXact_Namespace(bool isCommit, SubTransactionId mySubid,
                      SubTransactionId parentSubid);

/* stuff for search_path GUC variable */
extern char *namespace_search_path;

extern List *fetch_search_path(bool includeImplicit);
extern int    fetch_search_path_array(Oid *sarray, int sarray_len);

#ifdef __AUDIT__
/* get all oids by function name */
extern List * FunctionGetOidsByName(List * func_name);

extern char * RangeVarGetName(RangeVar *relation);
#endif
#ifdef _MLS_
extern List * FunctionGetOidsByNameString(List * func_name);
#endif
#endif                            /* NAMESPACE_H */
