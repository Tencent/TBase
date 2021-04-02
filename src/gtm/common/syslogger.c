/*-------------------------------------------------------------------------
 *
 * syslogger.c
 *
 * The system logger (syslogger) catches all
 * stderr output from the gtm thread by redirecting to a pipe, and
 * writes it to a set of logfiles. It's possible to have size and
 * age limits for the logfile configured in gtm.conf. If these limits
 * are reached or passed, the current logfile is closed and a new one
 * is created (rotated) The logfiles are stored in a subdirectory gtm_log.
 *
 * Copyright (c) 2021-Present TBase development team, Tencent
 *
 * IDENTIFICATION
 *	  src/gtm/common/syslogger.c
 *
 *-------------------------------------------------------------------------
 */

#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/epoll.h>
#include "gtm/gtm_c.h"
#include "gtm/gtm.h"
#include "gtm/stringinfo.h"
#include "gtm/gtm_list.h"
#include "gtm/palloc.h"
#include "gtm/syslogger.h"
#include "gtm/gtm_time.h"
#include "gtm/elog.h"

/*
 * GUC parameters.  Logging_collector cannot be changed after postmaster
 * start, but the rest can change at SIGHUP.
 */
bool		Logging_collector = false;
int			Log_RotationAge = HOURS_PER_DAY * MINS_PER_HOUR;
int			Log_RotationSize = 10 * 1024;
char	   *Log_directory = NULL;
char	   *Log_filename = "gtm-%Y-%m-%d_%H%M%S.log";
bool		Log_truncate_on_rotation = false;
int			Log_file_mode = S_IRUSR | S_IWUSR;

/*
 * Private state
 */
pg_time_t next_rotation_time;
bool rotation_disabled = false;
FILE *gtmlogFile = NULL;
pg_time_t first_syslogger_file_time = 0;
static char *last_file_name = NULL;
bool rotation_requested = false;

/*
 * Buffers for saving partial messages from different backends.
 *
 * Keep NBUFFER_LISTS lists of these, with the entry for a given source pid
 * being in the list numbered (pid % NBUFFER_LISTS), so as to cut down on
 * the number of entries we have to examine for any one incoming message.
 * There must never be more than one entry for the same source pid.
 *
 * An inactive buffer is not removed from its list, just held for re-use.
 * An inactive buffer has pid == 0 and undefined contents of data.
 */
typedef struct
{
    int32		pid;			/* PID of source process */
    StringInfoData data;		/* accumulated data, as a StringInfo */
} save_buffer;

#define NBUFFER_LISTS 256
static gtm_List *buffer_lists[NBUFFER_LISTS];


int	syslogPipe[2] = {-1, -1};
int	signalPipe[2] = {-1, -1};


void flush_pipe_input(char *logbuffer, int *bytes_in_logbuffer);
static FILE *logfile_open(const char *filename, const char *mode,
                          bool allow_errors);

void logfile_rotate(bool time_based_rotation, int size_rotation_for);
static char *logfile_getname(pg_time_t timestamp, const char *suffix);


/* --------------------------------
 *		pipe protocol handling
 * --------------------------------
 */

/*
 * Process data received through the syslogger pipe.
 *
 * This routine interprets the log pipe protocol which sends log messages as
 * (hopefully atomic) chunks - such chunks are detected and reassembled here.
 *
 * The protocol has a header that starts with two nul bytes, then has a 16 bit
 * length, the pid of the sending process, and a flag to indicate if it is
 * the last chunk in a message. Incomplete chunks are saved until we read some
 * more, and non-final chunks are accumulated until we get the final chunk.
 *
 * All of this is to avoid 2 problems:
 * . partial messages being written to logfiles (messes rotation), and
 * . messages from different backends being interleaved (messages garbled).
 *
 * Any non-protocol messages are written out directly. These should only come
 * from non-PostgreSQL sources, however (e.g. third party libraries writing to
 * stderr).
 *
 * logbuffer is the data input buffer, and *bytes_in_logbuffer is the number
 * of bytes present.  On exit, any not-yet-eaten data is left-justified in
 * logbuffer, and *bytes_in_logbuffer is updated.
 */
void
process_pipe_input(char *logbuffer, int *bytes_in_logbuffer, bool* pipe_eof_seen)
{
    char	   *cursor = logbuffer;
    int			count = *bytes_in_logbuffer;
    int			dest = LOG_DESTINATION_STDERR;

    /* While we have enough for a header, process data... */
    while (count >= (int) (offsetof(PipeProtoHeader, data) + 1))
    {
        PipeProtoHeader p;
        int			chunklen;

        /* Do we have a valid header? */
        memcpy(&p, cursor, offsetof(PipeProtoHeader, data));
        if (p.nuls[0] == '\0' && p.nuls[1] == '\0' &&
            p.len > 0 && p.len <= PIPE_MAX_PAYLOAD &&
            (p.is_last == 't' || p.is_last == 'f' ||
             p.is_last == 'T' || p.is_last == 'F'))
        {
            gtm_List	   *buffer_list;
            gtm_ListCell   *cell;
            save_buffer *existing_slot = NULL,
                    *free_slot = NULL;
            StringInfo	str;

            chunklen = PIPE_HEADER_SIZE + p.len;

            if (p.pid == 0)
            {
                *pipe_eof_seen = true;
            }

            /* Fall out of loop if we don't have the whole chunk yet */
            if (count < chunklen)
                break;

            dest = (p.is_last == 'T' || p.is_last == 'F') ?
                   LOG_DESTINATION_CSVLOG : LOG_DESTINATION_STDERR;

            /* Locate any existing buffer for this source pid */
            buffer_list = buffer_lists[p.pid % NBUFFER_LISTS];
            gtm_foreach(cell, buffer_list)
            {
                save_buffer *buf = (save_buffer *) gtm_lfirst(cell);

                if (buf->pid == p.pid)
                {
                    existing_slot = buf;
                    break;
                }
                if (buf->pid == 0 && free_slot == NULL)
                    free_slot = buf;
            }

            if (p.is_last == 'f' || p.is_last == 'F')
            {
                /*
                 * Save a complete non-final chunk in a per-pid buffer
                 */
                if (existing_slot != NULL)
                {
                    /* Add chunk to data from preceding chunks */
                    str = &(existing_slot->data);
                    appendBinaryStringInfo(str,
                                           cursor + PIPE_HEADER_SIZE,
                                           p.len);
                }
                else
                {
                    /* First chunk of message, save in a new buffer */
                    if (free_slot == NULL)
                    {
                        /*
                         * Need a free slot, but there isn't one in the list,
                         * so create a new one and extend the list with it.
                         */
                        free_slot = palloc(sizeof(save_buffer));
                        buffer_list = gtm_lappend(buffer_list, free_slot);
                        buffer_lists[p.pid % NBUFFER_LISTS] = buffer_list;
                    }
                    free_slot->pid = p.pid;
                    str = &(free_slot->data);
                    initStringInfo(str);
                    appendBinaryStringInfo(str,
                                           cursor + PIPE_HEADER_SIZE,
                                           p.len);
                }
            }
            else
            {
                /*
                 * Final chunk --- add it to anything saved for that pid, and
                 * either way write the whole thing out.
                 */
                if (existing_slot != NULL)
                {
                    str = &(existing_slot->data);
                    appendBinaryStringInfo(str,
                                           cursor + PIPE_HEADER_SIZE,
                                           p.len);
                    write_syslogger_file(str->data, str->len, dest);
                    /* Mark the buffer unused, and reclaim string storage */
                    existing_slot->pid = 0;
                    pfree(str->data);
                }
                else
                {
                    /* The whole message was one chunk, evidently. */
                    write_syslogger_file(cursor + PIPE_HEADER_SIZE, p.len,
                                         dest);
                }
            }

            /* Finished processing this chunk */
            cursor += chunklen;
            count -= chunklen;
        }
        else
        {
            /* Process non-protocol data */

            /*
             * Look for the start of a protocol header.  If found, dump data
             * up to there and repeat the loop.  Otherwise, dump it all and
             * fall out of the loop.  (Note: we want to dump it all if at all
             * possible, so as to avoid dividing non-protocol messages across
             * logfiles.  We expect that in many scenarios, a non-protocol
             * message will arrive all in one read(), and we want to respect
             * the read() boundary if possible.)
             */
            for (chunklen = 1; chunklen < count; chunklen++)
            {
                if (cursor[chunklen] == '\0')
                    break;
            }
            /* fall back on the stderr log as the destination */
            write_syslogger_file(cursor, chunklen, LOG_DESTINATION_STDERR);
            cursor += chunklen;
            count -= chunklen;
        }
    }

    /* We don't have a full chunk, so left-align what remains in the buffer */
    if (count > 0 && cursor != logbuffer)
        memmove(logbuffer, cursor, count);
    *bytes_in_logbuffer = count;
}

/*
 * Force out any buffered data
 *
 * This is currently used only at syslogger shutdown, but could perhaps be
 * useful at other times, so it is careful to leave things in a clean state.
 */
void
flush_pipe_input(char *logbuffer, int *bytes_in_logbuffer)
{
    int			i;

    /* Dump any incomplete protocol messages */
    for (i = 0; i < NBUFFER_LISTS; i++)
    {
        gtm_List	   *list = buffer_lists[i];
        gtm_ListCell   *cell;

        gtm_foreach(cell, list)
        {
            save_buffer *buf = (save_buffer *) gtm_lfirst(cell);

            if (buf->pid != 0)
            {
                StringInfo	str = &(buf->data);

                write_syslogger_file(str->data, str->len, LOG_DESTINATION_STDERR);
                /* Mark the buffer unused, and reclaim string storage */
                buf->pid = 0;
                pfree(str->data);
            }
        }
    }

    /*
     * Force out any remaining pipe data as-is; we don't bother trying to
     * remove any protocol headers that may exist in it.
     */
    if (*bytes_in_logbuffer > 0)
        write_syslogger_file(logbuffer, *bytes_in_logbuffer, LOG_DESTINATION_STDERR);
    *bytes_in_logbuffer = 0;
}


/* --------------------------------
 *		logfile routines
 * --------------------------------
 */

/*
 * Write text to the currently open logfile
 *
 * This is exported so that elog.c can call it when am_syslogger is true.
 * This allows the syslogger process to record elog messages of its own,
 * even though its stderr does not point at the syslog pipe.
 */
void
write_syslogger_file(const char *buffer, int count, int destination)
{
    int			rc;

    if (destination != LOG_DESTINATION_STDERR)
    {
        return;
    }

    if (gtmlogFile == NULL)
    {
        write(fileno(stderr), buffer, count);
        return;
    }

    rc = fwrite(buffer, 1, count, gtmlogFile);

    /* can't use ereport here because of possible recursion */
    if (rc != count)
        write_stderr("could not write to log file: %s\n", strerror(errno));
}

/*
 * Open a new logfile with proper permissions and buffering options.
 *
 * If allow_errors is true, we just log any open failure and return NULL
 * (with errno still correct for the fopen failure).
 * Otherwise, errors are treated as fatal.
 */
static FILE *
logfile_open(const char *filename, const char *mode, bool allow_errors)
{
    FILE	   *fh;
    mode_t		oumask;

    /*
     * Note we do not let Log_file_mode disable IWUSR, since we certainly want
     * to be able to write the files ourselves.
     */
    oumask = umask((mode_t) ((~(Log_file_mode | S_IWUSR)) & (S_IRWXU | S_IRWXG | S_IRWXO)));
    fh = fopen(filename, mode);
    umask(oumask);

    if (fh)
    {
        setvbuf(fh, NULL, PG_IOLBF, 0);

#ifdef WIN32
        /* use CRLF line endings on Windows */
        _setmode(_fileno(fh), _O_TEXT);
#endif
    }
    else
    {
        int			save_errno = errno;

        ereport(allow_errors ? LOG : FATAL,
                (errmsg("could not open log file \"%s\": %m",
                               filename)));
        errno = save_errno;
    }

    return fh;
}

/*
 * perform logfile rotation
 */
void
logfile_rotate(bool time_based_rotation, int size_rotation_for)
{
    char	   *filename;
    pg_time_t	fntime;
    FILE	   *fh;

    rotation_requested = false;

    /*
     * When doing a time-based rotation, invent the new logfile name based on
     * the planned rotation time, not current time, to avoid "slippage" in the
     * file name when we don't do the rotation immediately.
     */
    if (time_based_rotation)
        fntime = next_rotation_time;
    else
        fntime = time(NULL);
    filename = logfile_getname(fntime, NULL);

    /*
     * Decide whether to overwrite or append.  We can overwrite if (a)
     * Log_truncate_on_rotation is set, (b) the rotation was triggered by
     * elapsed time and not something else, and (c) the computed file name is
     * different from what we were previously logging into.
     *
     * Note: last_file_name should never be NULL here, but if it is, append.
     */
    if (time_based_rotation || (size_rotation_for & LOG_DESTINATION_STDERR))
    {
        if (Log_truncate_on_rotation && time_based_rotation &&
            last_file_name != NULL &&
            strcmp(filename, last_file_name) != 0)
            fh = logfile_open(filename, "w", true);
        else
            fh = logfile_open(filename, "a", true);

        if (!fh)
        {
            /*
             * ENFILE/EMFILE are not too surprising on a busy system; just
             * keep using the old file till we manage to get a new one.
             * Otherwise, assume something's wrong with Log_directory and stop
             * trying to create files.
             */
            if (errno != ENFILE && errno != EMFILE)
            {
                ereport(LOG,
                        (errmsg("disabling automatic rotation (use SIGHUP to re-enable)")));
                rotation_disabled = true;
            }

            if (filename)
                pfree(filename);
            return;
        }

        fclose(gtmlogFile);
        gtmlogFile = fh;

        /* instead of pfree'ing filename, remember it for next time */
        if (last_file_name != NULL)
            pfree(last_file_name);
        last_file_name = filename;
        filename = NULL;
    }

    if (filename)
        pfree(filename);

    set_next_rotation_time();
}


/*
 * construct logfile name using timestamp information
 *
 * If suffix isn't NULL, append it to the name, replacing any ".log"
 * that may be in the pattern.
 *
 * Result is palloc'd.postgresql-%Y-%m-%d_%H%M%S.log
 */
static char *
logfile_getname(pg_time_t timestamp, const char *suffix)
{
    char	   *filename;
    int			len;
    time_t	stamp_time;
    struct tm   timeinfo;
    filename = palloc(MAXPGPATH);

    snprintf(filename, MAXPGPATH, "%s/", Log_directory);

    len = strlen(filename);

    stamp_time = time(NULL);
    localtime_r(&stamp_time,&timeinfo);
    /* treat Log_filename as a strftime pattern */
    strftime(filename + len, MAXPGPATH - len, Log_filename,
             &timeinfo);

    if (suffix != NULL)
    {
        len = strlen(filename);
        if (len > 4 && (strcmp(filename + (len - 4), ".log") == 0))
            len -= 4;
        strlcpy(filename + len, suffix, MAXPGPATH - len);
    }

    return filename;
}

/*
 * Determine the next planned rotation time, and store in next_rotation_time.
 */
void
set_next_rotation_time(void)
{
    pg_time_t	now;
    struct tm timeinfo;
    int			rotinterval;

    /* nothing to do if time-based rotation is disabled */
    if (Log_RotationAge <= 0)
        return;

    /*
     * The requirements here are to choose the next time > now that is a
     * "multiple" of the log rotation interval.  "Multiple" can be interpreted
     * fairly loosely.  In this version we align to log_timezone rather than
     * GMT.
     */
    rotinterval = Log_RotationAge * SECS_PER_MINUTE;	/* convert to seconds */
    now = (pg_time_t) time(NULL);
    localtime_r(&now,&timeinfo);
    now += timeinfo.tm_gmtoff;
    now -= now % rotinterval;
    now += rotinterval;
    now -= timeinfo.tm_gmtoff;
    next_rotation_time = now;
}

/*
 * Initialization of error output file
 */
void
GTM_LogFileInit(void)
{
    char	   *filename;

    /*
     * Create log directory if not present; ignore errors
     */
    mkdir(Log_directory, S_IRWXU);

    first_syslogger_file_time = time(NULL);
    filename = logfile_getname(first_syslogger_file_time, NULL);

    gtmlogFile = logfile_open(filename, "a", false);

    pfree(filename);
}

/*
 * Send one byte to the signal pipe, to wake up syslogger
 */
void
GTM_SendNotifyByte(void)
{
    int			rc;
    char		dummy = 0;

    if (signalPipe[1] == -1)
    {
        return;
    }

retry:
    rc = write(signalPipe[1], &dummy, 1);
    if (rc < 0)
    {
        /* If interrupted by signal, just retry */
        if (errno == EINTR)
            goto retry;

        /*
         * If the pipe is full, we don't need to retry, the data that's there
         * already is enough to wake up WaitLatch.
         */
        if (errno == EAGAIN || errno == EWOULDBLOCK)
            return;

        /*
         * Oops, the write() failed for some other reason. We might be in a
         * signal handler, so it's not safe to elog(). We have no choice but
         * silently ignore the error.
         */
        return;
    }
}

/*
 * Read all available data from the signal pipe
 */
void
GTM_drainNotifyBytes(void)
{
    /*
     * There shouldn't normally be more than one byte in the pipe, or maybe a
     * few bytes if multiple processes run SetLatch at the same instant.
     */
    char		buf[16];
    int			rc;

    if (signalPipe[0] == -1)
    {
        return;
    }

    for (;;)
    {
        rc = read(signalPipe[0], buf, sizeof(buf));
        if (rc < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                break;			/* the pipe is empty */
            else if (errno == EINTR)
                continue;		/* retry */
            else
            {
                elog(LOG, "read() on signalPipe failed: %m");
                break;
            }
        }
        else if (rc == 0)
        {
            elog(LOG, "unexpected EOF on signalPipe");
            break;
        }
        else if (rc < sizeof(buf))
        {
            /* we successfully drained the pipe; no need to read() again */
            break;
        }
        /* else buffer wasn't big enough, so read again */
    }
}

int
GTM_InitSysloggerEpoll(void)
{
    int efd = -1;
    struct epoll_event event;

    if (syslogPipe[0] == -1 || signalPipe[0] == -1)
    {
        return -1;
    }

    efd = epoll_create1(0);
    if(efd == -1)
    {
        elog(LOG, "failed to create epoll");
        return -1;
    }

    event.data.fd = syslogPipe[0];
    event.events = EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP;
    if(-1 == epoll_ctl (efd, EPOLL_CTL_ADD, syslogPipe[0], &event))
    {
        elog(LOG, "failed to add socket to epoll");
        return -1;
    }

    event.data.fd = signalPipe[0];
    event.events = EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP;
    if(-1 == epoll_ctl (efd, EPOLL_CTL_ADD, signalPipe[0], &event))
    {
        elog(LOG, "failed to add socket to epoll");
        return -1;
    }

    return efd;
}
