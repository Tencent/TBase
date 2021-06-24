#include "postgres.h"

#include "catalog/catalog.h"
#include "catalog/storage.h"
#include "miscadmin.h"
#include "fmgr.h"
#include "postmaster/bgwriter.h"

#include "storage/bufmgr.h"
#include "utils/relcrypt.h"
#include "utils/relcryptmap.h"
#include "utils/mls.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(pg_rel_crypt_hash_clean);

/*
 *  Add function to clean the rel_crypt_hash table invalid elem
 */
Datum pg_rel_crypt_hash_clean(PG_FUNCTION_ARGS)
{
	RelCryptEntry *relcrypt;
	List *mark_delete = NIL;
	ListCell *	lc;

	if (!is_mls_user())
	{
		elog(ERROR, "execute by mls user please");
	}

	/* set to flush rel crypt map */
	RequestFlushRelcryptMap();

	/* make rel crypt map for a backup file */
	rel_crypt_write_mapfile(true);

	mark_delete = MarkRelCryptInvalid();
	/* delete the elem one by one */
	foreach(lc, mark_delete)
	{
		relcrypt = (RelCryptEntry *) lfirst(lc);
		rel_crypt_hash_delete(&(relcrypt->relfilenode), true);
	}

	PG_RETURN_BOOL(true);
}
