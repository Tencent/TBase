
#ifndef SLOTFUNCS_H
#define SLOTFUNCS_H

#include "catalog/objectaddress.h"

extern ObjectAddress RenameSlot(const char *oldname, const char *newname);

/* for pg_get_object_address */
extern Oid get_replication_slot_slotid(const char *slotname, bool missing_ok);
extern Oid get_replication_slot_dbid(const char *slotname, bool missing_ok);

#endif //SLOTFUNCS_H
