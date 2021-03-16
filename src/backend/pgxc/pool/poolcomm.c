/*-------------------------------------------------------------------------
 *
 * poolcomm.c
 *
 *      Communication functions between the pool manager and session
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
 */

#ifdef __sun
#define _XOPEN_SOURCE 500
#define uint uint32_t
#endif

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <stddef.h>
#include "c.h"
#include "postgres.h"
#include "pgxc/poolcomm.h"
#include "storage/ipc.h"
#include "utils/elog.h"
#include "miscadmin.h"
#include "pgxc/poolmgr.h"

static int    pool_recvbuf(PoolPort *port);
static int    pool_discardbytes(PoolPort *port, size_t len);

#ifdef HAVE_UNIX_SOCKETS

#define POOLER_UNIXSOCK_PATH(path, port, sockdir) \
    snprintf(path, sizeof(path), "%s/.s.PGPOOL.%d", \
            ((sockdir) && *(sockdir) != '\0') ? (sockdir) : \
            DEFAULT_PGSOCKET_DIR, \
            (port))

static char sock_path[MAXPGPATH];

static void StreamDoUnlink(int code, Datum arg);

static int    Lock_AF_UNIX(unsigned short port, const char *unixSocketName);
#endif

/*
 * Open server socket on specified port to accept connection from sessions
 */
int
pool_listen(unsigned short port, const char *unixSocketName)
{
    int            fd,
                len;
    struct sockaddr_un unix_addr;
    int            maxconn;


#ifdef HAVE_UNIX_SOCKETS
    if (Lock_AF_UNIX(port, unixSocketName) < 0)
        return -1;

    /* create a Unix domain stream socket */
    if ((fd = socket(AF_UNIX, SOCK_STREAM, 0)) < 0)
        return -1;

    /* fill in socket address structure */
    memset(&unix_addr, 0, sizeof(unix_addr));
    unix_addr.sun_family = AF_UNIX;
    strcpy(unix_addr.sun_path, sock_path);
    len = sizeof(unix_addr.sun_family) +
        strlen(unix_addr.sun_path) + 1;



    /* bind the name to the descriptor */
    if (bind(fd, (struct sockaddr *) & unix_addr, len) < 0)
	{
		close(fd);
        return -1;
	}

    /*
     * Select appropriate accept-queue length limit.  PG_SOMAXCONN is only
     * intended to provide a clamp on the request on platforms where an
     * overly large request provokes a kernel error (are there any?).
     */
    maxconn = MaxBackends * 2;
    if (maxconn > PG_SOMAXCONN)
        maxconn = PG_SOMAXCONN;

    /* tell kernel we're a server */
    if (listen(fd, maxconn) < 0)
	{
		close(fd);
        return -1;
	}



    /* Arrange to unlink the socket file at exit */
    on_proc_exit(StreamDoUnlink, 0);

    return fd;
#else
    /* TODO support for non-unix platform */
    ereport(FATAL,
            (errcode(ERRCODE_INTERNAL_ERROR),
             errmsg("pool manager only supports UNIX socket")));
    return -1;
#endif
}

/* StreamDoUnlink()
 * Shutdown routine for pooler connection
 * If a Unix socket is used for communication, explicitly close it.
 */
#ifdef HAVE_UNIX_SOCKETS
static void
StreamDoUnlink(int code, Datum arg)
{
    Assert(sock_path[0]);
    unlink(sock_path);
}
#endif   /* HAVE_UNIX_SOCKETS */

#ifdef HAVE_UNIX_SOCKETS
static int
Lock_AF_UNIX(unsigned short port, const char *unixSocketName)
{
    POOLER_UNIXSOCK_PATH(sock_path, port, unixSocketName);

    CreateSocketLockFile(sock_path, true, "");

    unlink(sock_path);

    return 0;
}
#endif

/*
 * Connect to pooler listening on specified port
 */
int
pool_connect(unsigned short port, const char *unixSocketName)
{
    int            fd,
                len;
    struct sockaddr_un unix_addr;

#ifdef HAVE_UNIX_SOCKETS
    /* create a Unix domain stream socket */
    if ((fd = socket(AF_UNIX, SOCK_STREAM, 0)) < 0)
        return -1;

    /* fill socket address structure w/server's addr */
    POOLER_UNIXSOCK_PATH(sock_path, port, unixSocketName);

    memset(&unix_addr, 0, sizeof(unix_addr));
    unix_addr.sun_family = AF_UNIX;
    strcpy(unix_addr.sun_path, sock_path);
    len = sizeof(unix_addr.sun_family) +
        strlen(unix_addr.sun_path) + 1;

    if (connect(fd, (struct sockaddr *) & unix_addr, len) < 0)
	{
		close(fd);
        return -1;
	}

    return fd;
#else
    /* TODO support for non-unix platform */
    ereport(FATAL,
            (errcode(ERRCODE_INTERNAL_ERROR),
             errmsg("pool manager only supports UNIX socket")));
    return -1;
#endif
}


/*
 * Get one byte from the buffer, read data from the connection if buffer is empty
 */
int
pool_getbyte(PoolPort *port)
{
    while (port->RecvPointer >= port->RecvLength)
    {
        if (pool_recvbuf(port)) /* If nothing in buffer, then recv some */
            return EOF;            /* Failed to recv data */
    }
    return (unsigned char) port->RecvBuffer[port->RecvPointer++];
}


/*
 * Get one byte from the buffer if it is not empty
 */
int
pool_pollbyte(PoolPort *port)
{
    if (port->RecvPointer >= port->RecvLength)
    {
        return EOF;                /* Empty buffer */
    }
    return (unsigned char) port->RecvBuffer[port->RecvPointer++];
}


/*
 * Read pooler protocol message from the buffer.
 */
int
pool_getmessage(PoolPort *port, StringInfo s, int maxlen)
{
    int32        len;

    resetStringInfo(s);

    /* Read message length word */
    if (pool_getbytes(port, (char *) &len, 4) == EOF)
    {
        ereport(ERROR,
                (errcode(ERRCODE_PROTOCOL_VIOLATION),
                 errmsg("unexpected EOF within message length word")));
        return EOF;
    }

    len = ntohl(len);

    if (len < 4 ||
        (maxlen > 0 && len > maxlen))
    {
        ereport(ERROR,
                (errcode(ERRCODE_PROTOCOL_VIOLATION),
                 errmsg("invalid message length")));
        return EOF;
    }

    len -= 4;                    /* discount length itself */

    if (len > 0)
    {
        /*
         * Allocate space for message.    If we run out of room (ridiculously
         * large message), we will elog(ERROR)
         */
        PG_TRY();
        {
            enlargeStringInfo(s, len);
        }
        PG_CATCH();
        {
            if (pool_discardbytes(port, len) == EOF){
                //while(1);
                ereport(PANIC,
                        (errcode(ERRCODE_PROTOCOL_VIOLATION),
                         errmsg("incomplete message from client")));
            }
            PG_RE_THROW();
        }
        PG_END_TRY();

        /* And grab the message */
        if (pool_getbytes(port, s->data, len) == EOF)
        {
            //while(1);
            ereport(PANIC,
                    (errcode(ERRCODE_PROTOCOL_VIOLATION),
                     errmsg("incomplete message from client")));
            return EOF;
        }
        s->len = len;
        /* Place a trailing null per StringInfo convention */
        s->data[len] = '\0';
    }

    return 0;
}


/* --------------------------------
 * pool_getbytes - get a known number of bytes from connection
 *
 * returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int
pool_getbytes(PoolPort *port, char *s, size_t len)
{
    size_t        amount;

    while (len > 0)
    {
        while (port->RecvPointer >= port->RecvLength)
        {
            if (pool_recvbuf(port))        /* If nothing in buffer, then recv
                                         * some */
                return EOF;        /* Failed to recv data */
        }
        amount = port->RecvLength - port->RecvPointer;
        if (amount > len)
            amount = len;
        memcpy(s, port->RecvBuffer + port->RecvPointer, amount);
        port->RecvPointer += amount;
        s += amount;
        len -= amount;
    }
    return 0;
}


/* --------------------------------
 * pool_discardbytes - discard a known number of bytes from connection
 *
 * returns 0 if OK, EOF if trouble
 * --------------------------------
 */
static int
pool_discardbytes(PoolPort *port, size_t len)
{
    size_t        amount;

    while (len > 0)
    {
        while (port->RecvPointer >= port->RecvLength)
        {
            if (pool_recvbuf(port))        /* If nothing in buffer, then recv
                                         * some */
                return EOF;        /* Failed to recv data */
        }
        amount = port->RecvLength - port->RecvPointer;
        if (amount > len)
            amount = len;
        port->RecvPointer += amount;
        len -= amount;
    }
    return 0;
}


/* --------------------------------
 * pool_recvbuf - load some bytes into the input buffer
 *
 * returns 0 if OK, EOF if trouble
 * --------------------------------
 */
static int
pool_recvbuf(PoolPort *port)
{
    if (port->RecvPointer > 0)
    {
        if (port->RecvLength > port->RecvPointer)
        {
            /* still some unread data, left-justify it in the buffer */
            memmove(port->RecvBuffer, port->RecvBuffer + port->RecvPointer,
                    port->RecvLength - port->RecvPointer);
            port->RecvLength -= port->RecvPointer;
            port->RecvPointer = 0;
        }
        else
            port->RecvLength = port->RecvPointer = 0;
    }

    /* Can fill buffer from PqRecvLength and upwards */
    for (;;)
    {
        int            r;

        r = recv(Socket(*port), port->RecvBuffer + port->RecvLength,
                 POOL_BUFFER_SIZE - port->RecvLength, 0);

        if (r < 0)
        {
            if (errno == EINTR)
                continue;        /* Ok if interrupted */

            /*
             * Report broken connection
             */
            ereport(LOG,
                    (errcode_for_socket_access(),
                     errmsg("could not receive data from client: %m")));
            return EOF;
        }
        if (r == 0)
        {
            /*
             * EOF detected.  We used to write a log message here, but it's
             * better to expect the ultimate caller to do that.
             */
            return EOF;
        }
        /* r contains number of bytes read, so just incr length */
        port->RecvLength += r;
        return 0;
    }
}


/*
 * Put a known number of bytes into the connection buffer
 */
int
pool_putbytes(PoolPort *port, const char *s, size_t len)
{
    size_t        amount;

    while (len > 0)
    {
        /* If buffer is full, then flush it out */
        if (port->SendPointer >= POOL_BUFFER_SIZE)
            if (pool_flush(port))
                return EOF;
        amount = POOL_BUFFER_SIZE - port->SendPointer;
        if (amount > len)
            amount = len;
        memcpy(port->SendBuffer + port->SendPointer, s, amount);
        port->SendPointer += amount;
        s += amount;
        len -= amount;
    }
    return 0;
}


/* --------------------------------
 *        pool_flush        - flush pending output
 *
 *        returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int
pool_flush(PoolPort *port)
{
    static int    last_reported_send_errno = 0;

    char       *bufptr = port->SendBuffer;
    char       *bufend = port->SendBuffer + port->SendPointer;

    while (bufptr < bufend)
    {
        int            r;

        r = send(Socket(*port), bufptr, bufend - bufptr, 0);

        if (r <= 0)
        {
            if (errno == EINTR)
                continue;        /* Ok if we were interrupted */

            if (errno != last_reported_send_errno)
            {
                last_reported_send_errno = errno;

                /*
                 * Handle a seg fault that may later occur in proc array
                 * when this fails when we are already shutting down
                 * If shutting down already, do not call.
                 */
                if (!proc_exit_inprogress)
                    return 0;
            }

            /*
             * We drop the buffered data anyway so that processing can
             * continue, even though we'll probably quit soon.
             */
            port->SendPointer = 0;
            return EOF;
        }

        last_reported_send_errno = 0;    /* reset after any successful send */
        bufptr += r;
    }

    port->SendPointer = 0;
    return 0;
}


/*
 * Put the pooler protocol message into the connection buffer
 */
int
pool_putmessage(PoolPort *port, char msgtype, const char *s, size_t len)
{
    uint        n32;

    if (pool_putbytes(port, &msgtype, 1))
        return EOF;

    n32 = htonl((uint32) (len + 4));
    if (pool_putbytes(port, (char *) &n32, 4))
        return EOF;

    if (pool_putbytes(port, s, len))
        return EOF;

    return 0;
}

/* message code('f'), size(8), node_count, err_code */
#define SEND_MSG_BUFFER_SIZE 13

/* message code('s'), result */
#define SEND_RES_BUFFER_SIZE (9 + POOL_ERR_MSG_LEN) /* tag + length + err_code + err_msg */
#define SEND_PID_BUFFER_SIZE (5 + (MaxConnections - 1) * 4)

/* message code('s'), result , commandID*/
#define SEND_RES_BUFFER_HEDAER_SIZE   5


/*
 * Build up a message carrying file descriptors or process numbers and send them over specified
 * connection
 */
int
pool_sendfds(PoolPort *port, int *fds, int count, char *errbuf, int32 buf_len)
{// #lizard forgives
    int32  r       = 0;
    int32  offset = 0;
    int32  err      = 0;
    int    error_code = 0;
    struct iovec iov[1];
    struct msghdr msg;
    char        buf[SEND_MSG_BUFFER_SIZE];
    uint        n32;
    int            controllen = sizeof(struct cmsghdr) + count * sizeof(int);
    struct cmsghdr *cmptr = NULL;

    buf[0] = 'f';
    n32 = htonl((uint32) 8);
    memcpy(buf + 1, &n32, 4);
    n32 = htonl((uint32) count);
    memcpy(buf + 5, &n32, 4);
    /* send error code */
    n32 = htonl((uint32) port->error_code);
    memcpy(buf + 9, &n32, 4);
    error_code = port->error_code;
    port->error_code  = POOL_ERR_NONE;

    iov[0].iov_base = buf;
    iov[0].iov_len = SEND_MSG_BUFFER_SIZE;
    msg.msg_iov = iov;
    msg.msg_iovlen = 1;
    msg.msg_name = NULL;
    msg.msg_namelen = 0;
    if (count == 0)
    {
        msg.msg_control = NULL;
        msg.msg_controllen = 0;
    }
    else
    {
        if ((cmptr = malloc(controllen)) == NULL)
        {
            return EOF;
        }
        cmptr->cmsg_level = SOL_SOCKET;
        cmptr->cmsg_type = SCM_RIGHTS;
        cmptr->cmsg_len = controllen;
        msg.msg_control = (caddr_t) cmptr;
        msg.msg_controllen = controllen;
        /* the fd to pass */
        memcpy(CMSG_DATA(cmptr), fds, count * sizeof(int));
    }

    r       = 0;
    offset = 0;
    while (offset < SEND_MSG_BUFFER_SIZE)
    {
        r = sendmsg(Socket(*port), &msg, 0);
        if (r < 0)
        {
            if (cmptr)
            {
                free(cmptr);
            }

            if (errbuf && buf_len)
            {
                err = errno;            
                snprintf(errbuf, buf_len, POOL_MGR_PREFIX"Pooler pool_sendfds flush failed for:%s", strerror(err));
            }
            else
            {
                err = errno;
                elog(LOG, POOL_MGR_PREFIX"Pooler pool_sendfds flush failed for:%s", strerror(err));
            }
            return EOF;
        }
        else
        {
            offset += r;
            if (SEND_MSG_BUFFER_SIZE == offset)
            {
                break;
            }
            else if (offset < SEND_MSG_BUFFER_SIZE)
            {
                /* send the rest data. */
                iov[0].iov_base = buf + offset;
                iov[0].iov_len  = SEND_MSG_BUFFER_SIZE - offset;
            }
            else
            {
                if (cmptr)
                {
                    free(cmptr);
                }
                
                if (errbuf && buf_len)
                {
                    err = errno;            
                    snprintf(errbuf, buf_len, POOL_MGR_PREFIX"Pooler invalid send length:%d", offset);
                }
                else
                {
                    err = errno;
                    elog(LOG, POOL_MGR_PREFIX"Pooler invalid send length:%d", offset);
                }
                return EOF;
            }    
        }
    }

    if (cmptr)
    {
        free(cmptr);
    }

    /* send error message if error occured */
    if (PoolErrIsValid(error_code))
    {
        int         size   = POOL_ERR_MSG_LEN;
        int         sended = 0;
        char         *ptr   = NULL;

        ptr = port->err_msg;
        
        for(;;)
        {
            r = send(Socket(*port), ptr + sended, size - sended, 0);
            if (r < 0)
            {
                if(errno == EINTR)
                    continue;
                else 
                {
                    err = errno;
                    goto failure;
                }
            }
            
            if(r == 0)
            {
                if(sended == size)
                {    
                    return 0;
                } 
                else 
                {
                    err = errno;
                    goto failure;
                }
            }
            sended += r;
            if(sended == size)
            {
                return 0;
            }
        }
    }
        
    return 0;
    
failure:
    if (errbuf && buf_len)
    {        
        snprintf(errbuf, buf_len, POOL_MGR_PREFIX"Pooler pool_sendfds flush failed for:%s", strerror(err));
    }
    else
    {
        elog(LOG, POOL_MGR_PREFIX"Pooler pool_sendfds flush failed for:%s", strerror(err));
    }
    return EOF;
}

/*
 * Read a message from the specified connection carrying file descriptors
 */
int
pool_recvfds(PoolPort *port, int *fds, int count)
{// #lizard forgives
    int            r      = 0;
    int            offset = 0;
    uint        n32    = 0;
    uint        err    = 0;
    int         error_no = 0;
    char        buf[SEND_MSG_BUFFER_SIZE];
    char        err_msg[POOL_ERR_MSG_LEN];
    struct iovec iov[1];
    struct msghdr msg;
    int    controllen = 0;
    struct cmsghdr *cmptr = NULL;

    controllen = CMSG_LEN(count * sizeof(int));
    cmptr = malloc(CMSG_SPACE(count * sizeof(int)));
    if (cmptr == NULL)
    {
        if (PoolConnectDebugPrint)
        {
            elog(LOG, "[pool_recvfds]cmptr == NULL, return EOF");
        }
        return EOF;
    }

    /* Use recv buf to receive data. */
    iov[0].iov_base = buf;
    iov[0].iov_len  = SEND_MSG_BUFFER_SIZE;
    msg.msg_iov = iov;
    msg.msg_iovlen = 1;
    msg.msg_name = NULL;
    msg.msg_namelen = 0;
    msg.msg_control = (caddr_t) cmptr;
    msg.msg_controllen = controllen;
    
    offset = 0;
    while (offset < SEND_MSG_BUFFER_SIZE)
    {
        r = recvmsg(Socket(*port), &msg, 0);
        if (r < 0)
        {
            /*
             * report broken connection
             */
            ereport(LOG,
                            (errcode_for_socket_access(),
                             errmsg("could not receive data from client: %m")));
            goto failure;
        }
        else if (0 == r)
        {
            struct stat st;
            if (fstat(Socket(*port), &st) < 0)
            {
                break;
            }
            
            //elog(LOG, "[pool_recvfds]r == 0, errmsg=%s", strerror(errno));
            if (errno)
            {
                /* if errno is not zero, it means connection pipe got error */
                break;
            }
            continue;
        }
        else
        {
            offset += r;
            if (SEND_MSG_BUFFER_SIZE == offset)
            {
                break;
            }
            else if (offset < SEND_MSG_BUFFER_SIZE)
            {                
                /* only receive the left data, no more. */
                iov[0].iov_len  = SEND_MSG_BUFFER_SIZE - offset;
                iov[0].iov_base = buf + offset;
            }            
            else
            {
                ereport(ERROR,
                    (errcode_for_socket_access(),
                     errmsg("invalid msg len:%d received from pooler.", offset)));
            }            
        }
    }
    
    /* Verify response */
    if (buf[0] != 'f')
    {
        ereport(LOG,
                (errcode(ERRCODE_PROTOCOL_VIOLATION),
                 errmsg("unexpected message code")));
        goto failure;
    }

    memcpy(&n32, buf + 1, 4);
    n32 = ntohl(n32);
    if (n32 != 8)
    {
        ereport(LOG,
                (errcode(ERRCODE_PROTOCOL_VIOLATION),
                 errmsg("invalid message size")));
        goto failure;
    }

    /*
     * If connection count is 0 it means pool does not have connections
     * to  fulfill request. Otherwise number of returned connections
     * should be equal to requested count. If it not the case consider this
     * a protocol violation. (Probably connection went out of sync)
     */
    memcpy(&n32, buf + 5, 4);
    n32 = ntohl(n32);
    /* get error code */
    memcpy(&err, buf + 9, 4);
    err = ntohl(err);

    /* receive error message if error occured */
    if (PoolErrIsValid(err))
    {
        char *ptr = err_msg;
        int recved_size = 0;
        int size = POOL_ERR_MSG_LEN;
        
        for(;;)
        {
            r = recv(Socket(*port), ptr + recved_size, size - recved_size, 0);
            if (r < 0)
            {
                /*
                 * Report broken connection
                 */
                elog(LOG, "recv size %d size %d n32 %d.", recved_size, size, n32);
                ereport(LOG,
                        (errcode_for_socket_access(),
                         errmsg("could not receive data from client: %m")));
                error_no = errno;
                goto receive_error;
            }
            else if (r == 0)
            {
                    error_no = errno;
                    goto receive_error;
                }

            recved_size += r;
            if(recved_size == size)
                break;
        }
    }
    
    if (n32 == 0)
    {
        ereport(LOG,
                (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                 errmsg("failed to acquire connections")));
        goto failure;
    }

    if (n32 != count)
    {
        ereport(LOG,
                (errcode(ERRCODE_PROTOCOL_VIOLATION),
                 errmsg("unexpected connection count")));
        goto failure;
    }

    memcpy(fds, CMSG_DATA(CMSG_FIRSTHDR(&msg)), count * sizeof(int));
    free(cmptr);
    if (PoolConnectDebugPrint)
    {
        elog(LOG, "[pool_recvfds]success. fds=%p", fds);
    }
    return 0;
failure:
    free(cmptr);
    if (PoolErrIsValid(err))
    {
        elog(LOG, "%s, errno:%d, errmsg:%s", err_msg, errno, strerror(errno));
    }
    else
    {
        elog(LOG, "[pool_recvfds]failure, return EOF, errno:%d, errmsg:%s", errno, strerror(errno));
    }
    return EOF;
receive_error:
    elog(LOG, "[pool_recvfds]failure, fail to receive error message, err_msg %s", strerror(error_no));
    return EOF;
}

/*
 * Send result to specified connection
 */
int
pool_sendres(PoolPort *port, int res, char *errbuf, int32 buf_len, bool need_log)
{// #lizard forgives
    int32       err;
    uint        n32;
    char        buf[SEND_RES_BUFFER_SIZE];
    int sended = 0, r, size = SEND_RES_BUFFER_SIZE;
    char *ptr = buf;

    /* Header */
    buf[0] = 's';
    /* Result */
    n32 = htonl(res);
    memcpy(buf + 1, &n32, 4);
    /* send error code */
    n32 = htonl(port->error_code);
    memcpy(buf + 5, &n32, 4);

    /*
     * send error message if error occured
     */
    if (PoolErrIsValid(port->error_code))
    {
        memcpy(buf + 9, port->err_msg, POOL_ERR_MSG_LEN);
    }
    else
    {
        size = SEND_RES_BUFFER_SIZE - POOL_ERR_MSG_LEN;
    }
    
    port->error_code = POOL_ERR_NONE;

    for(;;)
    {
        r = send(Socket(*port), ptr + sended, size - sended, 0);
        if (r <= 0)
        {
            if (r < 0)
            {
                if(errno == EINTR)
                    continue;
                else 
                    goto failure;
            }
            
            if((r == 0) && (sended == size))
            {    
                if (need_log)
                    elog(DEBUG5, "send size %d size %d.", sended, size);
                return 0;
            } 
            if (errbuf && buf_len)
            {
                err = errno;
                snprintf(errbuf, buf_len, POOL_MGR_PREFIX" pool_sendres send data failed for %s", strerror(err));
            }
            else
            {
                err = errno;
                if (need_log)
                {
                    elog(LOG, POOL_MGR_PREFIX" pool_sendres send data failed for %s", strerror(err));
                }
            }
            return EOF;
        }
        
        sended += r;
        if(sended == size)
        {
            if (need_log)
            {
                elog(DEBUG5, "send size %d size %d.", sended, size);
            }
            return 0;
        }
        
    }
    return 0;

failure:
    return EOF;    
}

/*
 * Send result and commandId to specified connection, used for 's' command.
 */
int
pool_sendres_with_command_id(PoolPort *port, int res, CommandId cmdID, char *errbuf, int32 buf_len, char *errmsg, bool need_log)
{// #lizard forgives
    int32       err;
    int32        n32             = 0;
    int32       offset       = 0;
    int32       send_buf_len = 0;
    char        *buf         = NULL;
    int         sended = 0;
    int32       r = 0; 
    int32       size = 0;
    char        *ptr = NULL;

    /* protocol format: command + total_len + return_code + command_id + error_msg */
    if (errmsg)
    {
        /* error. */
        send_buf_len = 1 + sizeof(int32) + sizeof(int32) + sizeof(CommandId) + strlen(errmsg) + 1; /* reserved space for '\0' */
    }
    else
    {
        /* no error. */
        send_buf_len = 1 + sizeof(int32) + sizeof(int32) + sizeof(CommandId);
    }
    
    if (PoolConnectDebugPrint)
    {
        if (need_log)
        {
            elog(LOG, POOL_MGR_PREFIX"pool_sendres_with_command_id ENTER, res:%d commandid:%u", res, cmdID);
        }
    }

    buf = malloc(send_buf_len);
    if (NULL == buf)
    {
        if (errbuf && buf_len)
        {
            err = errno;
            snprintf(errbuf, buf_len, POOL_MGR_PREFIX" pool_sendres_with_command_id out of memory size:%d", send_buf_len);            
        }
        else
        {
            err = errno;
            {
                if (need_log)
                {
                    elog(LOG, POOL_MGR_PREFIX" pool_sendres_with_command_id out of memory size:%d", send_buf_len);
                }
            }
        }        
        
        return EOF;
    }
    
    /* Header */
    buf[0] = 's';
    offset = 1;
    
    /* Total len */
    n32 = htonl(send_buf_len);
    memcpy(buf + offset, &n32, 4);
    offset += 4;
    
    /* Result */
    n32 = htonl(res);
    memcpy(buf + offset, &n32, 4);
    offset += 4;

    /* CommandID */
    n32 = htonl(cmdID);
    memcpy(buf + offset, &n32, 4);
    offset += 4;

    if (offset < send_buf_len)
    {
        memcpy(buf + offset, errmsg, strlen(errmsg));
        offset += strlen(errmsg);
        buf[offset] = '\0';
    }

    size = send_buf_len;
    ptr  = buf;
    for(;;)
    {
        r = send(Socket(*port), ptr + sended, size - sended, 0);
        if (r <= 0)
        {
            if (r < 0)
            {
                if(errno == EINTR)
                {
                    continue;
                }
                else 
                {
                    err = errno;
                    if (errbuf && buf_len)
                    {
                        snprintf(errbuf, buf_len, POOL_MGR_PREFIX" pool_sendres_with_command_id send data failed for %s", strerror(err));
                    }
                    else if (need_log)
                    {
                        elog(LOG, POOL_MGR_PREFIX" pool_sendres_with_command_id send data failed for %s", strerror(err));
                    }
                    goto failure;
                }
            }            
            else if((r == 0) && (sended == size))
            {
                if (PoolConnectDebugPrint)
                {
                    if (need_log)
                    {
                        elog(LOG, POOL_MGR_PREFIX"pool_sendres_with_command_id EXIT, res:%d commandid:%u send succeed", res, cmdID);
                    }
                }
                
                if (buf)
                {
                    free(buf);
                    buf = NULL;
                }
                return 0;
            } 

            
            if (errbuf && buf_len)
            {
                err = errno;
                snprintf(errbuf, buf_len, POOL_MGR_PREFIX" pool_sendres_with_command_id send data failed for %s", strerror(err));
            }
            else
            {
                err = errno;
                {
                    if (need_log)
                    {
                        elog(LOG, POOL_MGR_PREFIX" pool_sendres_with_command_id EXIT, send data failed for %s", strerror(err));
                    }
                }
            }
            
            if (buf)
            {
                free(buf);
                buf = NULL;
            }
            return EOF;
        }
        
        sended += r;
        if(sended == size)
        {
            if (PoolConnectDebugPrint)
            {
                if (need_log)
                {
                    elog(LOG, POOL_MGR_PREFIX"pool_sendres_with_command_id EXIT, res:%d commandid:%u send succeed", res, cmdID);
                }
            }
            
            if (buf)
            {
                free(buf);
                buf = NULL;
            }
            return 0;
        }
        
    }
    
    if (buf)
    {
        free(buf);
        buf = NULL;
    }    
    return 0;

failure:
    if (buf)
    {
        free(buf);
		buf = NULL;
    }
    
    if (PoolConnectDebugPrint)
    {
        if (need_log)
        {
            elog(LOG, POOL_MGR_PREFIX"pool_sendres_with_command_id EXIT, res:%d commandid:%u send failed", res, cmdID);
        }
    }
    return EOF;    
}

/*
 * Read result from specified connection.
 * Return 0 at success or EOF at error. Used for 's' command.
 */
int
pool_recvres_with_commandID(PoolPort *port, CommandId *cmdID, const char *sql)
{// #lizard forgives
    int            r;
    int         offset     = 0;
    int            pooler_res = 0;
    int         result_len = 0;
    uint        n32 = 0;
    char        buf[SEND_RES_BUFFER_HEDAER_SIZE];
    int         recved_size = 0;
    int32       size = SEND_RES_BUFFER_HEDAER_SIZE; /* init the size to header size */
    char       *ptr = buf;
    char       *error = NULL;

    /* protocol format: command + total_len + return_code + command_id + error_msg */
    for(;;)
    {
        r = recv(Socket(*port), ptr + recved_size, size - recved_size, 0);
        if (r < 0)
        {
            /*
             * Report broken connection
             */
            elog(LOG, "[pool_recvres_with_commandID] ERROR recv size %d size %d n32 %d.", recved_size, size, n32);
            ereport(LOG,
                    (errcode_for_socket_access(),
                     errmsg("[pool_recvres_with_commandID]could not receive data from client: %m")));
            goto failure;
        }
        else if (r == 0)
        {            
            if(recved_size == result_len && result_len > 0)
            {
                break;
            }
            else
            {
                elog(ERROR, "[pool_recvres_with_commandID]ERROR recv size %d size %d.", recved_size, size);
            }
        }

        recved_size += r;
        if(SEND_RES_BUFFER_HEDAER_SIZE == recved_size)
        {
            /* Verify response */
            if (buf[0] != 's')
            {
                ereport(ERROR,
                        (errcode(ERRCODE_PROTOCOL_VIOLATION),
                         errmsg("[pool_recvres_with_commandID] unexpected message code:%c", buf[0])));
                goto failure;    
            }
            
            /* Get result len. */
            memcpy(&n32, buf + 1, 4);
            result_len = ntohl(n32);
            offset     = SEND_RES_BUFFER_HEDAER_SIZE;
            
            /* Set the actual result len. */
            size = result_len;    
            ptr  = palloc0(result_len);
            if (NULL == ptr)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_PROTOCOL_VIOLATION),
                           errmsg("[pool_recvres_with_commandID] out of memory, size%d", result_len)));                    
            }
            memcpy(ptr, buf, SEND_RES_BUFFER_HEDAER_SIZE);
        }
        
        if(recved_size == size)
        {
            break;
        }
    }

    /* result */
    memcpy(&n32, ptr + offset, 4);
    n32 = ntohl(n32);    
    pooler_res    =  n32;
    offset += 4;
    
    /* command ID */
    memcpy(&n32, ptr + offset, 4);
    n32 = ntohl(n32);    
    *cmdID = n32;
    offset += 4;

    /* ERROR msg */
    if (result_len > offset)
    {
        error = ptr + offset;
        if (pooler_res)
        {
            elog(ERROR, "MyPid %d SET Command:%s failed for %s", MyProcPid, sql, error);
        }
    }
    

    if (PoolConnectDebugPrint)
    {
        elog(LOG, "[pool_recvres_with_commandID] res=%d, cmdID=%u", pooler_res, *cmdID);
    }
    
    if (ptr && ptr != buf)
    {
        pfree(ptr);
    }
    return pooler_res;

failure:
    if (ptr && ptr != buf)
    {
        pfree(ptr);
    }
    *cmdID = InvalidCommandId;
    elog(LOG, "[pool_recvres_with_commandID] ERROR failed res=%d, cmdID=%u", pooler_res, *cmdID);
    return EOF;
}
/*
 * Read result from specified connection.
 * Return 0 at success or EOF at error.
 */
int
pool_recvres(PoolPort *port, bool need_log)
{
	int			r;
	uint		n32 = 0;
	uint        err = 0;
	char		buf[SEND_RES_BUFFER_SIZE - POOL_ERR_MSG_LEN];
	char        err_msg[POOL_ERR_MSG_LEN];
	int recved_size = 0;
	int size = SEND_RES_BUFFER_SIZE - POOL_ERR_MSG_LEN;
	char *ptr = buf;
	
	/* receive message header first */
	for(;;)
	{
		r = recv(Socket(*port), ptr + recved_size, size - recved_size, 0);
		if (r < 0)
		{
			/*
			 * Report broken connection
			 */
			elog(LOG, "recv size %d size %d n32 %d.", recved_size, size, n32);
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("could not receive data from client: %m")));
			goto failure;
		}
		else if (r == 0)
		{
				goto failure;
		}

		recved_size += r;
		if(recved_size == size)
			break;

	}
	/* Verify response */
	if (buf[0] != 's')
	{
		ereport(LOG,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("unexpected message code:%c", buf[0])));
		goto failure;
	}

	memcpy(&n32, buf + 1, 4);
	n32 = ntohl(n32);
	if (n32 != 0 && need_log)
	{
		ereport(LOG,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("pool_recvres return code:%d", n32)));
	}

	memcpy(&err, buf + 5, 4);
	err = ntohl(err);

	/* if has err_msg, receive error message */
	if (PoolErrIsValid(err))
	{
		ptr = err_msg;
		size = POOL_ERR_MSG_LEN;
		recved_size = 0;
		for(;;)
		{
			r = recv(Socket(*port), ptr + recved_size, size - recved_size, 0);
			if (r < 0)
			{
				/*
				 * Report broken connection
				 */
				elog(LOG, "recv size %d size %d n32 %d.", recved_size, size, n32);
				ereport(LOG,
						(errcode_for_socket_access(),
						 errmsg("could not receive data from client: %m")));
				goto failure;
			}
			else if (r == 0)
			{
				if(recved_size == size)
					break;
				else
					goto failure;
			}

			recved_size += r;
			if(recved_size == size)
				break;

		}

		elog(WARNING, "%s", err_msg);
	}
	
	return n32;
	
failure:
    return EOF;
}

/*
 * Read a message from the specified connection carrying pid numbers
 * of transactions interacting with pooler
 */
int
pool_recvpids(PoolPort *port, int **pids)
{// #lizard forgives
    int            r   = 0;
    int            i   = 0;
    uint        n32 = 0;
    char        *buf = NULL;
    int         recved_size = 0;
    int          size = 5;
    char         *ptr = NULL;
    
    buf = (char*)malloc(SEND_PID_BUFFER_SIZE);
    if (NULL == buf)
    {
        ereport(LOG,
                (errcode_for_socket_access(),
                 errmsg("pool_recvpids failed to alloc %d size memory.", SEND_PID_BUFFER_SIZE)));
        return 0;
    }

    ptr = buf;
    /*
     * Buffer size is upper bounded by the maximum number of connections,
     * as in the pooler each connection has one Pooler Agent.
     */
    for(;;)
    {
        r = recv(Socket(*port), ptr + recved_size, size - recved_size, 0);
        elog(DEBUG1, "recv %d size %d.", r, size - recved_size);
        if (r < 0)
        {
            /*
             * Report broken connection
             */
            ereport(LOG,
                    (errcode_for_socket_access(),
                     errmsg("could not receive data from client: %m recved_size %d size %d.", recved_size, size)));
            goto failure;
        }
        else if (r == 0)
        {
                goto failure;
        }
        
        recved_size += r;
        if(recved_size == size)
            break;
        
    }

    /* Verify response */
    if (buf[0] != 'p')
    {
        elog(LOG, "recv code %c.", buf[0]);
        ereport(LOG,
                (errcode(ERRCODE_PROTOCOL_VIOLATION),
                 errmsg("unexpected message code %c", buf[0])));
        goto failure;
    }

    memcpy(&n32, buf + 1, 4);
    n32 = ntohl(n32);
    if (n32 == 0)
    {
        elog(WARNING, "No transaction to abort");
		free(buf);
        return 0;
    }

    size = n32 * sizeof(int);
    ptr = buf + 5;
    recved_size = 0;
    for(;;)
    {
        r = recv(Socket(*port), ptr + recved_size, size - recved_size, 0);
        elog(DEBUG1, "recv %d size %d.", r, size - recved_size);
        if (r < 0)
        {
            /*
             * Report broken connection
             */
            elog(LOG, "recv size %d size %d n32 %d.", recved_size, size, n32);
            ereport(LOG,
                    (errcode_for_socket_access(),
                     errmsg("could not receive data from client: %m")));
            goto failure;
        }
        else if (r == 0)
        {
            if(recved_size == size)
                break;
            else
                goto failure;
        }

        recved_size += r;
        if(recved_size == size)
            break;
        
    }

    *pids = (int *) palloc(sizeof(int) * n32);

    for (i = 0; i < n32; i++)
    {
        int n;
        memcpy(&n, buf + 5 + i * sizeof(int), sizeof(int));
        (*pids)[i] = ntohl(n);
    }
    
    if (PoolConnectDebugPrint)
    {
        elog(LOG, "recv size %d size %d n32 %d.", recved_size, size, n32);
    }

    free(buf);
    return n32;
    
failure:
    free(buf);
    ereport(LOG,
                    (errcode_for_socket_access(),
                     errmsg("recvpids failure recv size %d size %d count %d.", recved_size, size, n32)));
    return 0;
}

/*
 * Send a message containing pid numbers to the specified connection
 */
int
pool_sendpids(PoolPort *port, int *pids, int count, char *errbuf, int32 buf_len)
{// #lizard forgives
    int         i      = 0;
    int32       err    = 0;    
    char        *buf   = NULL;
    uint        n32    = 0;
    int         size   = 0;
    int         sended = 0;
    int            r       = 0;
    char         *ptr   = NULL;

    buf = (char*)malloc(SEND_PID_BUFFER_SIZE);
    if (NULL == buf)
    {
        err = errno;
        snprintf(errbuf + strlen(errbuf) + 1, buf_len - strlen(errbuf) - 1, 
                 POOL_MGR_PREFIX"pool_sendpids malloc %d memory failed.", 
                 SEND_PID_BUFFER_SIZE);
        return EOF;
    }
    
    buf[0] = 'p';
    n32 = htonl((uint32) count);
    memcpy(buf + 1, &n32, 4);
    for (i = 0; i < count; i++)
    {
        int n;
        n = htonl((uint32) pids[i]);
        memcpy(buf + 5 + i * sizeof(int), &n, 4);
    }
    size = 5 + count * sizeof(int);

    /* try to send data. */
    sended = 0;
    ptr = buf;
    for(;;)
    {
        r = send(Socket(*port), ptr + sended, size - sended, 0);
        if (r < 0)
        {
            if(errno == EINTR)
                continue;
            else 
                goto failure;
        }
        
        if(r == 0)
        {
            if(sended == size)
            {    
                if (!errbuf)
                {
                    elog(DEBUG1, "send size %d size %d count %d.", sended, size, count);
                }
                free(buf);
                return 0;
            } 
            else 
            {
                goto failure;
            }
        }
        sended += r;
        if(sended == size)
        {
            if (!errbuf)
            {
                elog(DEBUG1, "send size %d size %d count %d.", sended, size, count);
            }
            free(buf);
            return 0;
        }
    }
failure:
    if (errbuf && buf_len)
    {
        err = errno;
        snprintf(errbuf+strlen(errbuf)+1, buf_len-strlen(errbuf)-1, 
                POOL_MGR_PREFIX"pool_sendpids send data failed for %s. failure send size %d size %d count %d.", 
                strerror(err), sended, size, count);
    }
    else
    {
        err = errno;
        elog(LOG, POOL_MGR_PREFIX"pool_sendpids send data failed for %s. failure send size %d size %d count %d.",
                strerror(err), sended, size, count);
    }
    free(buf);
    return EOF;
}
