/*-------------------------------------------------------------------------
 *
 * syslogger.h
 *	  Exports from gtm/syslogger.c.
 *
 * Copyright (c) 2021-Present TBase development team, Tencent
 *
 * src/include/gtm/syslogger.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _SYSLOGGER_H
#define _SYSLOGGER_H

#include <limits.h>				/* for PIPE_BUF */


/*
 * Primitive protocol structure for writing to syslogger pipe(s).  The idea
 * here is to divide long messages into chunks that are not more than
 * PIPE_BUF bytes long, which according to POSIX spec must be written into
 * the pipe atomically.  The pipe reader then uses the protocol headers to
 * reassemble the parts of a message into a single string.  The reader can
 * also cope with non-protocol data coming down the pipe, though we cannot
 * guarantee long strings won't get split apart.
 *
 * We use non-nul bytes in is_last to make the protocol a tiny bit
 * more robust against finding a false double nul byte prologue. But
 * we still might find it in the len and/or pid bytes unless we're careful.
 */

#ifdef PIPE_BUF
/* Are there any systems with PIPE_BUF > 64K?  Unlikely, but ... */
#if PIPE_BUF > 65536
#define PIPE_CHUNK_SIZE  65536
#else
#define PIPE_CHUNK_SIZE  ((int) PIPE_BUF)
#endif
#else							/* not defined */
/* POSIX says the value of PIPE_BUF must be at least 512, so use that */
#define PIPE_CHUNK_SIZE  512
#endif

/*
 * We read() into a temp buffer twice as big as a chunk, so that any fragment
 * left after processing can be moved down to the front and we'll still have
 * room to read a full chunk.
 */
#define READ_BUF_SIZE (2 * PIPE_CHUNK_SIZE)

typedef struct
{
	char		nuls[2];		/* always \0\0 */
	uint16		len;			/* size of this chunk (counts data only) */
	int32		pid;			/* writer's pid */
	char		is_last;		/* last chunk of message? 't' or 'f' ('T' or
								 * 'F' for CSV case) */
	char		data[FLEXIBLE_ARRAY_MEMBER];	/* data payload starts here */
} PipeProtoHeader;

typedef union
{
	PipeProtoHeader proto;
	char		filler[PIPE_CHUNK_SIZE];
} PipeProtoChunk;

#define PIPE_HEADER_SIZE  offsetof(PipeProtoHeader, data)
#define PIPE_MAX_PAYLOAD  ((int) (PIPE_CHUNK_SIZE - PIPE_HEADER_SIZE))


/* GUC options */
extern bool Logging_collector;
extern int	Log_RotationAge;
extern int	Log_RotationSize;
extern char *Log_directory;
extern char *Log_filename;
extern bool Log_truncate_on_rotation;
extern int	Log_file_mode;

extern int	syslogPipe[2];
extern int	signalPipe[2];
extern bool rotation_disabled;
extern pg_time_t next_rotation_time;
extern pg_time_t first_syslogger_file_time;
extern FILE *gtmlogFile;
extern bool rotation_requested;

extern int	SysLogger_Start(void);
extern void logfile_rotate(bool time_based_rotation, int size_rotation_for);
extern void write_syslogger_file(const char *buffer, int count, int dest);
extern void set_next_rotation_time(void);
extern void process_pipe_input(char *logbuffer, int *bytes_in_logbuffer, bool *pipe_eof_seen);
extern void flush_pipe_input(char *logbuffer, int *bytes_in_logbuffer);
extern void GTM_LogFileInit(void);
extern void GTM_SendNotifyByte(void);
extern void GTM_drainNotifyBytes(void);
extern int GTM_InitSysloggerEpoll(void);
#endif							/* _SYSLOGGER_H */
