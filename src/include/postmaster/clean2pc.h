/*--------------------------------------------------------------------
 * clean2pc.h
 * A clean 2pc process is a process able to clean the abnormal 2pc.
 *
 *
 * Portions Copyright (c) 1996-2021, TDSQL-PG Development Group
 *
 * IDENTIFICATION
 *		src/include/postmaster/clean2pc.h
 *--------------------------------------------------------------------
 */
#ifndef CLEAN2PC_H
#define CLEAN2PC_H

#include "storage/block.h"

extern bool enable_clean_2pc_launcher;

extern int auto_clean_2pc_interval;
extern int auto_clean_2pc_delay;
extern int auto_clean_2pc_timeout;
extern int auto_clean_2pc_max_check_time;

extern bool IsClean2pcLauncher(void);
extern bool IsClean2pcWorker(void);

#define IsAnyClean2pcProcess() \
	(IsClean2pcLauncher() || IsClean2pcWorker())

extern int StartClean2pcLauncher(void);
extern int StartClean2pcWorker(void);

#ifdef EXEC_BACKEND
extern void Clean2pcLauncherMain(int argc, char *argv[]) pg_attribute_noreturn();
extern void Clean2pcWorkerMain(int argc, char *argv[]) pg_attribute_noreturn();

extern void Clean2pcLauncherIAm(void);
extern void Clean2pcWorkerIAm(void);
#endif

/* shared memory stuff */
extern Size Clean2pcShmemSize(void);
extern void Clean2pcShmemInit(void);

#endif /* CLEAN2PC_H */
