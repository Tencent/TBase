#ifndef PG_TRANSPARENT_CRYPT_POLICY_SCHEMA_H
#define PG_TRANSPARENT_CRYPT_POLICY_SCHEMA_H

#include "catalog/genbki.h"
/* oid */
#define TransparentCryptPolicySchemaRelationId  6103
    
CATALOG(pg_transparent_crypt_policy_schema,6103) BKI_WITHOUT_OIDS 
{
    Oid         schemaoid;
    int16       algorithm_id;   
    text        option;
}FormData_transparent_crypt_policy_schema;

typedef FormData_transparent_crypt_policy_schema *    Form_pg_transparent_crypt_policy_schema;
/* the numbers of attribute  */
#define Natts_pg_transparent_crypt_policy_schema        3

#define Anum_pg_transparent_crypt_policy_schema_schemaoid  1
#define Anum_pg_transparent_crypt_policy_schema_algorithm  2
#define Anum_pg_transparent_crypt_policy_schema_option     3

#endif   /* PG_TRANSPARENT_CRYPT_POLICY_SCHEMA_H */
