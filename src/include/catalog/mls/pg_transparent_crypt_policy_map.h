#ifndef PG_TRANSPARENT_CRYPT_POLICY_MAP_H
#define PG_TRANSPARENT_CRYPT_POLICY_MAP_H

#include "catalog/genbki.h"
/* oid */
#define TransparentCryptPolicyMapRelationId  3417
    
CATALOG(pg_transparent_crypt_policy_map,3417) BKI_WITHOUT_OIDS 
{
    Oid         relid;
    int16       attnum;
    int16       algorithm_id;   
    Oid         spcoid;
    Oid         schemaoid;
    NameData    spcname;
    NameData    nspname;
    NameData    tblname;
}FormData_transparent_crypt_policy_map;

typedef FormData_transparent_crypt_policy_map *    Form_pg_transparent_crypt_policy_map;
/* the numbers of attribute  */
#define Natts_pg_transparent_crypt_policy_map           8

/* the number of attribute  */
#define Anum_pg_transparent_crypt_policy_map_relid      1
#define Anum_pg_transparent_crypt_policy_map_attnum     2
#define Anum_pg_transparent_crypt_policy_map_algorithm  3
#define Anum_pg_transparent_crypt_policy_map_spcoid     4
#define Anum_pg_transparent_crypt_policy_map_schemaoid  5
#define Anum_pg_transparent_crypt_policy_map_spcname    6
#define Anum_pg_transparent_crypt_policy_map_nspname    7
#define Anum_pg_transparent_crypt_policy_map_tblname    8

#endif   /* PG_TRANSPARENT_CRYPT_POLICY_MAP_H */
