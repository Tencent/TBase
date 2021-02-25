/*-------------------------------------------------------------------------
 *
 * postmaster.h
 *	  Exports from postmaster/postmaster.c.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/postmaster/postmaster.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _POSTMASTER_H
#define _POSTMASTER_H

/* GUC options */
extern bool EnableSSL;
extern int	ReservedBackends;
extern int	PostPortNumber;
extern int	Unix_socket_permissions;
extern char *Unix_socket_group;
extern char *Unix_socket_directories;
extern char *ListenAddresses;
extern bool ClientAuthInProgress;
extern int	PreAuthDelay;
extern int	AuthenticationTimeout;
extern bool Log_connections;
extern bool log_hostname;
extern bool enable_bonjour;
extern char *bonjour_name;
extern bool restart_after_crash;
#ifdef __TBASE__
extern bool enable_statistic;
extern bool g_enable_bouncer;
extern bool g_enable_gtm_proxy;
extern char *g_BouncerConf;
extern bool enable_null_string;
extern bool g_concurrently_index;
extern bool g_set_global_snapshot;
extern char *gtm_unix_socket_directory;
#endif

#ifdef __COLD_HOT__
extern int32 g_create_key_value_mode;
extern int   g_ColdDataThreashold;
extern struct pg_tm   g_ManualHotDataTime; 
extern int32    g_ManualHotDataGapWithMonths;
extern int32    g_ManualHotDataGapWithDays;
extern char  *g_ManualHotDate;
extern char  *g_TempColdDate;
extern struct pg_tm   g_TempColdDataTime;
extern char  *g_TempKeyValue;
extern char  *g_TempHotDate;
extern struct pg_tm   g_TempHotDataTime;
extern int g_ColdHotPartitionType;
extern char *g_ColdHotPartitionMode;
enum
{
    CREATE_KEY_VALUE_EXEC_ALL = 0,
    CREATE_KEY_VALUE_EXEC_CN,
    CREATE_KEY_VALUE_EXEC_DN,
    CREATE_KEY_VALUE_EXEC_BUTT
};

#endif

#ifdef WIN32
extern HANDLE PostmasterHandle;
#else
extern int	postmaster_alive_fds[2];

/*
 * Constants that represent which of postmaster_alive_fds is held by
 * postmaster, and which is used in children to check for postmaster death.
 */
#define POSTMASTER_FD_WATCH		0	/* used in children to check for
									 * postmaster death */
#define POSTMASTER_FD_OWN		1	/* kept open by postmaster only */
#endif

extern const char *progname;

extern void PostmasterMain(int argc, char *argv[]) pg_attribute_noreturn();
extern void ClosePostmasterPorts(bool am_syslogger);

extern int	MaxLivePostmasterChildren(void);

extern int	GetNumShmemAttachedBgworkers(void);
extern bool PostmasterMarkPIDForWorkerNotify(int);

#ifdef EXEC_BACKEND
extern pid_t postmaster_forkexec(int argc, char *argv[]);
extern void SubPostmasterMain(int argc, char *argv[]) pg_attribute_noreturn();

extern Size ShmemBackendArraySize(void);
extern void ShmemBackendArrayAllocation(void);
#endif

/*
 * Note: MAX_BACKENDS is limited to 2^18-1 because that's the width reserved
 * for buffer references in buf_internals.h.  This limitation could be lifted
 * by using a 64bit state; but it's unlikely to be worthwhile as 2^18-1
 * backends exceed currently realistic configurations. Even if that limitation
 * were removed, we still could not a) exceed 2^23-1 because inval.c stores
 * the backend ID as a 3-byte signed integer, b) INT_MAX/4 because some places
 * compute 4*MaxBackends without any overflow check.  This is rechecked in the
 * relevant GUC check hooks and in RegisterBackgroundWorker().
 */
#define MAX_BACKENDS	0x3FFFF
#ifdef __TBASE__
extern void PostmasterEnableLogTimeout(void);
extern void PostmasterDisableTimeout(void);
extern bool PostmasterIsPrimaryAndNormal(void);
#endif
#endif							/* _POSTMASTER_H */
