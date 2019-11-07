#ifndef PG_TRANSPARENT_CRYPT_ALGORITHM_H
#define PG_TRANSPARENT_CRYPT_ALGORITHM_H

#include "catalog/genbki.h"
/* oid */
#define TransparentCryptPolicyAlgorithmId  4598
    
CATALOG(pg_transparent_crypt_policy_algorithm,4598) BKI_WITHOUT_OIDS BKI_SHARED_RELATION
{
    int16       algorithm_id;
    int16       option;
    NameData    algorithm_name;
    Oid         encrypt_oid;
    Oid         decrypt_oid;
    text        password;
    text        pubkey;
    text        privatekey;
    text        option_args;
}FormData_transparent_crypt_policy_algorithm;

typedef FormData_transparent_crypt_policy_algorithm *    Form_pg_transparent_crypt_policy_algorithm;
/* the numbers of attribute  */
#define Natts_pg_transparent_crypt_policy_algorithm             9
/* the number of attribute  */
#define Anum_pg_transparent_crypt_policy_algorithm_id           1
#define Anum_pg_transparent_crypt_policy_algorithm_option       2
#define Anum_pg_transparent_crypt_policy_algorithm_name         3
#define Anum_pg_transparent_crypt_policy_algorithm_encrypt_oid  4
#define Anum_pg_transparent_crypt_policy_algorithm_decrypt_oid  5       
#define Anum_pg_transparent_crypt_policy_algorithm_passwd       6
#define Anum_pg_transparent_crypt_policy_algorithm_pubkey       7
#define Anum_pg_transparent_crypt_policy_algorithm_prikey       8
#define Anum_pg_transparent_crypt_policy_algorithm_option_args  9


#endif   /* PG_TRANSPARENT_CRYPT_ALGORITHM_H */
