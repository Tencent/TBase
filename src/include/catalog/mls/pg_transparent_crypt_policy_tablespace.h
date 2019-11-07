#ifndef PG_TRANSPARENT_CRYPT_POLICY_TABLESPACE_H
#define PG_TRANSPARENT_CRYPT_POLICY_TABLESPACE_H

#include "catalog/genbki.h"
/* oid */
#define TransparentCryptPolicyTablespaceRelationId  6122
    
CATALOG(pg_transparent_crypt_policy_tablespace,6122) BKI_WITHOUT_OIDS 
{
    Oid         spcoid;
    int16       algorithm_id;   
    text        option;
}FormData_transparent_crypt_policy_tablespace;

typedef FormData_transparent_crypt_policy_tablespace *    Form_pg_transparent_crypt_policy_tablespace;
/* the numbers of attribute  */
#define Natts_pg_transparent_crypt_policy_tablespace    3

#define Anum_pg_transparent_crypt_policy_spc_spcoid     1
#define Anum_pg_transparent_crypt_policy_spc_algorithm  2
#define Anum_pg_transparent_crypt_policy_spc_option     3

#endif   /* PG_TRANSPARENT_CRYPT_POLICY_TABLESPACE_H */
