/*-------------------------------------------------------------------------
 *
 * view.h
 *
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/view.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VIEW_H
#define VIEW_H

#include "catalog/objectaddress.h"
#include "nodes/parsenodes.h"

extern void validateWithCheckOption(char *value);

extern ObjectAddress DefineView(ViewStmt *stmt, const char *queryString,
		   int stmt_location, int stmt_len);

extern void StoreViewQuery(Oid viewOid, Query *viewParse, bool replace);

extern Query *MakeViewParse(ViewStmt* stmt, const char* query_string,
									int stmt_location, int stmt_len);
#ifdef __TBASE__
extern bool IsViewTemp(ViewStmt* stmt, const char* query_string,
						int stmt_location, int stmt_len, List **relation_list);
#endif
#endif							/* VIEW_H */
