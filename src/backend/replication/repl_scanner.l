%{
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
 * repl_scanner.l
 *	  a lexical scanner for the replication commands
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/replication/repl_scanner.l
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/builtins.h"
#include "parser/scansup.h"

/* Avoid exit() on fatal scanner errors (a bit ugly -- see yy_fatal_error) */
#undef fprintf
#define fprintf(file, fmt, msg)  fprintf_to_ereport(fmt, msg)

static void
fprintf_to_ereport(const char *fmt, const char *msg)
{
	ereport(ERROR, (errmsg_internal("%s", msg)));
}

/* Handle to the buffer that the lexer uses internally */
static YY_BUFFER_STATE scanbufhandle;

static StringInfoData litbuf;

static void startlit(void);
static char *litbufdup(void);
static void addlit(char *ytext, int yleng);
static void addlitchar(unsigned char ychar);

%}

%option 8bit
%option never-interactive
%option nodefault
%option noinput
%option nounput
%option noyywrap
%option warn
%option prefix="replication_yy"

%x xq xd

/* Extended quote
 * xqdouble implements embedded quote, ''''
 */
xqstart			{quote}
xqdouble		{quote}{quote}
xqinside		[^']+

/* Double quote
 * Allows embedded spaces and other special characters into identifiers.
 */
dquote			\"
xdstart			{dquote}
xdstop			{dquote}
xddouble		{dquote}{dquote}
xdinside		[^"]+

digit			[0-9]+
hexdigit		[0-9A-Za-z]+

quote			'
quotestop		{quote}

ident_start		[A-Za-z\200-\377_]
ident_cont		[A-Za-z\200-\377_0-9\$]

identifier		{ident_start}{ident_cont}*

%%

BASE_BACKUP			{ return K_BASE_BACKUP; }
FAST			{ return K_FAST; }
IDENTIFY_SYSTEM		{ return K_IDENTIFY_SYSTEM; }
SHOW		{ return K_SHOW; }
LABEL			{ return K_LABEL; }
NOWAIT			{ return K_NOWAIT; }
PROGRESS			{ return K_PROGRESS; }
MAX_RATE		{ return K_MAX_RATE; }
WAL			{ return K_WAL; }
TABLESPACE_MAP			{ return K_TABLESPACE_MAP; }
TIMELINE			{ return K_TIMELINE; }
START_REPLICATION	{ return K_START_REPLICATION; }
CREATE_REPLICATION_SLOT		{ return K_CREATE_REPLICATION_SLOT; }
DROP_REPLICATION_SLOT		{ return K_DROP_REPLICATION_SLOT; }
TIMELINE_HISTORY	{ return K_TIMELINE_HISTORY; }
PHYSICAL			{ return K_PHYSICAL; }
RESERVE_WAL			{ return K_RESERVE_WAL; }
LOGICAL				{ return K_LOGICAL; }
SLOT				{ return K_SLOT; }
TEMPORARY			{ return K_TEMPORARY; }
EXPORT_SNAPSHOT		{ return K_EXPORT_SNAPSHOT; }
NOEXPORT_SNAPSHOT	{ return K_NOEXPORT_SNAPSHOT; }
USE_SNAPSHOT		{ return K_USE_SNAPSHOT; }
SUBSCRIPTION_NAME   { return K_SUBSCRIPTION_NAME; }
ID_SUBSCRIPTION     { return K_ID_SUBSCRIPTION; }
REL_NAMESPACE       { return K_REL_NAMESPACE; }
NAME_REL            { return K_NAME_REL; }

","				{ return ','; }
";"				{ return ';'; }
"("				{ return '('; }
")"				{ return ')'; }

[\n]			;
[\t]			;
" "				;

{digit}+		{
					yylval.uintval = strtoul(yytext, NULL, 10);
					return UCONST;
				}

{hexdigit}+\/{hexdigit}+		{
					uint32	hi,
							lo;
					if (sscanf(yytext, "%X/%X", &hi, &lo) != 2)
						yyerror("invalid streaming start location");
					yylval.recptr = ((uint64) hi) << 32 | lo;
					return RECPTR;
				}

{xqstart}		{
					BEGIN(xq);
					startlit();
				}

<xq>{quotestop}	{
					yyless(1);
					BEGIN(INITIAL);
					yylval.str = litbufdup();
					return SCONST;
				}

<xq>{xqdouble}	{
					addlitchar('\'');
				}

<xq>{xqinside}  {
					addlit(yytext, yyleng);
				}

{xdstart}		{
					BEGIN(xd);
					startlit();
				}

<xd>{xdstop}	{
					int len;
					yyless(1);
					BEGIN(INITIAL);
					yylval.str = litbufdup();
					len = strlen(yylval.str);
					truncate_identifier(yylval.str, len, true);
					return IDENT;
				}

<xd>{xdinside}  {
					addlit(yytext, yyleng);
				}

{identifier}	{
					int len = strlen(yytext);

					yylval.str = downcase_truncate_identifier(yytext, len, true);
					return IDENT;
				}

<xq,xd><<EOF>>	{ yyerror("unterminated quoted string"); }


<<EOF>>			{
					yyterminate();
				}

.				{
					return T_WORD;
				}
%%


static void
startlit(void)
{
	initStringInfo(&litbuf);
}

static char *
litbufdup(void)
{
	return litbuf.data;
}

static void
addlit(char *ytext, int yleng)
{
	appendBinaryStringInfo(&litbuf, ytext, yleng);
}

static void
addlitchar(unsigned char ychar)
{
	appendStringInfoChar(&litbuf, ychar);
}

void
yyerror(const char *message)
{
	ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg_internal("%s", message)));
}


void
replication_scanner_init(const char *str)
{
	Size		slen = strlen(str);
	char	   *scanbuf;

	/*
	 * Might be left over after ereport()
	 */
	if (YY_CURRENT_BUFFER)
		yy_delete_buffer(YY_CURRENT_BUFFER);

	/*
	 * Make a scan buffer with special termination needed by flex.
	 */
	scanbuf = (char *) palloc(slen + 2);
	memcpy(scanbuf, str, slen);
	scanbuf[slen] = scanbuf[slen + 1] = YY_END_OF_BUFFER_CHAR;
	scanbufhandle = yy_scan_buffer(scanbuf, slen + 2);
}

void
replication_scanner_finish(void)
{
	yy_delete_buffer(scanbufhandle);
	scanbufhandle = NULL;
}
