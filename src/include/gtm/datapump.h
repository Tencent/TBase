/*-------------------------------------------------------------------------
 *
 * datapump.h
 *
 *
 *	  lockless message queue
 *
 * Copyright (c) 2020-Present TBase development team, Tencent
 *
 *
 * IDENTIFICATION
 *	  src/include/gtm/datapump.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _DATAPUMP_H
#define _DATAPUMP_H

#include "gtm/gtm_c.h"
#include "gtm/gtm_lock.h"

#define  INVALID_BORDER 	(~((uint32)0))
typedef struct
{
    char               *buf;         /* Data buffer */
    unsigned           length;       /* Data buffer length */
    s_lock_t	       pointer_lock; /* lock to protect offset and status */
    volatile uint32	   head;         /* Head of the loop */
    volatile uint32    tail;         /* Tail of the buffer */
    volatile uint32    border;       /* end of last tuple, so that we can send a complete tuple */
    volatile uint32    wrap_around;  /* wrap around of the queue , for read only */
} DataPumpBuf;

uint32 DataSize(DataPumpBuf *buf);
uint32 FreeSpace(DataPumpBuf *buf);
char  *GetData(DataPumpBuf *buf, uint32 *uiLen);
void   IncDataOff(DataPumpBuf *buf, uint32 uiLen);
char  *GetWriteOff(DataPumpBuf *buf, uint32 *uiLen);
void   IncWriteOff(DataPumpBuf *buf, uint32 uiLen);
char  *GetWriteOff(DataPumpBuf *buf, uint32 *uiLen);
uint32 BufferOffsetAdd(DataPumpBuf *buf, uint32 pointer, uint32 offset);
int    ReserveSpace(DataPumpBuf *buf, uint32 len, uint32 *offset);
int ReturnSpace(DataPumpBuf *buf, uint32 offset);
void   FillReserveSpace(DataPumpBuf *buf, uint32 offset, char *p, uint32 len);
void   SetBorder(DataPumpBuf *buf);
void  *DataPumpSenderThread(void *arg);
void  PutData(DataPumpBuf *buf, char *data, uint32 len);




#endif
