/*-------------------------------------------------------------------------
 *
 * user.h
 *	  Commands for manipulating roles (formerly called users).
 *
 *
 * src/include/commands/user.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef USER_H
#define USER_H

#include "catalog/objectaddress.h"
#include "libpq/crypt.h"
#include "nodes/parsenodes.h"
#include "parser/parse_node.h"

/* GUC. Is actually of type PasswordType. */
extern int	Password_encryption;

/* Hook to check passwords in CreateRole() and AlterRole() */
typedef void (*check_password_hook_type) (const char *username, const char *shadow_pass, PasswordType password_type, Datum validuntil_time, bool validuntil_null);

extern PGDLLIMPORT check_password_hook_type check_password_hook;

extern Oid	CreateRole(ParseState *pstate, CreateRoleStmt *stmt);
extern Oid	AlterRole(AlterRoleStmt *stmt);
extern Oid	AlterRoleSet(AlterRoleSetStmt *stmt);
extern void DropRoleByTuple(char *role, HeapTuple tuple,
                                Relation pg_authid_rel,
					            Relation pg_auth_members_rel);
extern bool DropRole(DropRoleStmt *stmt, bool missing_ok, char *query_string);
extern void GrantRole(GrantRoleStmt *stmt);
extern ObjectAddress RenameRole(const char *oldname, const char *newname);
extern void DropOwnedObjects(DropOwnedStmt *stmt);
extern void ReassignOwnedObjects(ReassignOwnedStmt *stmt);
extern List *roleSpecsToIds(List *memberNames);

#ifdef __TBASE__
extern bool PreCheckDropRole(DropRoleStmt *stmt, char *query_string,
                                List **exist_roles);
extern void DropRoleParallelMode(List *role_list);
#endif

#endif							/* USER_H */
