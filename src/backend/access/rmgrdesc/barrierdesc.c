/*-------------------------------------------------------------------------
 *
 * barrierdesc.c
 *      rmgr descriptor routines for backend/pgxc/barrier/barrier.c
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *      src/backend/access/rmgrdesc/barrierdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "pgxc/barrier.h"

void
barrier_desc(StringInfo buf, XLogReaderState *record)
{
    char       *rec = XLogRecGetData(record);
#ifdef USE_ASSERT_CHECKING
    uint8       info = XLogRecGetInfo(record);
    Assert(info == XLOG_BARRIER_CREATE);
#endif
    appendStringInfo(buf, "BARRIER %s", rec);
}

const char *
barrier_identify(uint8 info)
{
    return "CREATE";
}
