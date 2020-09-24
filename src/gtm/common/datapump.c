/*-------------------------------------------------------------------------
 *
 * datapump.c
 *
 *
 *	  lockless message queue
 *
 * Copyright (c) 2020-Present TBase development team, Tencent
 *
 *
 * IDENTIFICATION
 *	  src/gtm/common/datapump.c
 *
 *-------------------------------------------------------------------------
 */

#include "gtm/datapump.h"


/*
 * The following funciton is used to handle lockless message queue.
 */
 
/*
 * Get data pointer, use with the following functions.
 */
char *
GetData(DataPumpBuf *buf, uint32 *uiLen)
{
	uint32 border = 0;
	uint32 tail = 0;
	char  *data;
	if (buf)
	{
		if (0 == DataSize(buf))
		{
			return NULL;
		}
		
		SpinLockAcquire(&(buf->pointer_lock));
		border = buf->border;
		tail   = buf->tail;
		SpinLockRelease(&(buf->pointer_lock));
		if (INVALID_BORDER == border)
		{
			*uiLen = 0;
			return NULL;
		}

		/* read from tail to border*/
		if (border >=  tail)
		{ 
			 /* Only sender increases tail, no need to lock. */
			 *uiLen = border - tail;  
			 data = buf->buf + tail;
		}
		else
		{  
			/* read from tail to end */
			*uiLen = buf->length - tail;
			data = buf->buf + tail;
			buf->wrap_around = true;
		} 
		return data;
	}
	else
	{
		*uiLen = 0;
		return NULL;
	}
}

/*
 * Increate data offset, used after finishing read data from queue.
 */
void
IncDataOff(DataPumpBuf *buf, uint32 uiLen)
{
	if (buf)
	{
		SpinLockAcquire(&(buf->pointer_lock));
		buf->tail  = (buf->tail + uiLen) % buf->length;
		if (buf->tail == buf->border)
        {
            buf->border = INVALID_BORDER;
        }
		SpinLockRelease(&(buf->pointer_lock));
	}
}

/*
 * Return total data size in buffer
 */
uint32
DataSize(DataPumpBuf *buf)
{
	uint32 border = 0;
	uint32 head   = 0;
	uint32 tail   = 0;
	uint32 size   = 0;
	if (buf)
	{
		SpinLockAcquire(&(buf->pointer_lock));
		head   = buf->head;
		tail   = buf->tail;
		border = buf->border;
		SpinLockRelease(&(buf->pointer_lock));

		if (INVALID_BORDER == border)
		{
			return 0;
		}

		if (tail <= head)
		{	
			size = head - tail;
		}
		else
		{
			size = buf->length - tail + head;
		}
		
		return size;
	}
	return 0;
}

/*
 * Get the pointer to write and return the length to write.
 */
char *
GetWriteOff(DataPumpBuf *buf, uint32 *uiLen)
{
	uint32 head = 0;
	uint32 tail = 0;
	char  *ptr  = NULL;
	if (0 == FreeSpace(buf))
	{
		return NULL;
	}
	
	if (buf)
	{
		SpinLockAcquire(&(buf->pointer_lock));
		head = buf->head;
		tail = buf->tail;
		SpinLockRelease(&(buf->pointer_lock));
		
		if (head >=  tail)
		{ 
		   /* tail is the beginning of the queue. */
		   if (tail != 0)
		   {
			   
			   *uiLen = buf->length - head;
		   }
		   else
		   {
			   /* Reserved one byte as flag. */
			   *uiLen = buf->length - head - 1;
		   }
		}
		else
		{			   
		   /* Reserved one byte as flag. */
		   *uiLen = tail - head - 1;
		}		
		ptr = buf->buf + head;
		return ptr;
	}
	else
	{
		return NULL;
	}	
}

/*
 * Used to increase the write pointer after write some data.
 */
void
IncWriteOff(DataPumpBuf *buf, uint32 uiLen)
{
	if (buf)
	{
		SpinLockAcquire(&(buf->pointer_lock));
   		buf->head  += uiLen;
   		buf->head  = buf->head % buf->length;
		SpinLockRelease(&(buf->pointer_lock));
	}
}

/*
 * Reserve space in print buffer
 */
int
ReserveSpace(DataPumpBuf *buf, uint32 len, uint32 *offset)
{
	/* not enough space avaliable, wait */
	if (FreeSpace(buf) < len)
	{
		return -1;
	}

	if (buf)
	{
		*offset	= buf->head;
   		buf->head  = (buf->head + len) % buf->length;
	}
	return 0;		
}

uint32
BufferOffsetAdd(DataPumpBuf *buf, uint32 pointer, uint32 offset)
{

	if (buf)
	{	
   		return (pointer + offset) % buf->length;
	}
	return 0;		
}

/*
 * No need to lock, reader never read the data before we set border.
 */
int
ReturnSpace(DataPumpBuf *buf, uint32 offset)
{
	if (buf)
	{
		buf->head = offset;
	}
	return 0;		
}

/*
 * Fill data into reserved by ReserveSpace
 */
void
FillReserveSpace(DataPumpBuf *buf, uint32 offset, char *p, uint32 len)
{
	uint32 bytes2end      = 0;
	uint32 bytesfrombegin = 0;   

	if (buf)
	{		
		bytes2end = buf->length - offset;
		if (len <= bytes2end)
		{
			memcpy(buf->buf + offset, p, len);
		}
		else
		{
			bytesfrombegin = len - bytes2end;				
			memcpy(buf->buf + offset, p, bytes2end);
			memcpy(buf->buf, (char*)p + bytes2end, bytesfrombegin);
		}	
	}
}

/*
 * Return free space of the buffer.
 */
uint32
FreeSpace(DataPumpBuf *buf)
{
	uint32 head = 0;
	uint32 tail = 0;
	uint32 len  = 0;
	if (buf)
	{
		SpinLockAcquire(&(buf->pointer_lock));
		head = buf->head;
		tail = buf->tail;
		SpinLockRelease(&(buf->pointer_lock));

		if (tail <= head)
		{
			len = tail + buf->length - head - 1;
		}
		else
		{
			len = tail - head - 1;
		}
		return len;
	}
	else
	{
		return 0;
	}
}

/*
 * Set tuple end border of the buffer.
 */
void
SetBorder(DataPumpBuf *buf)
{
	SpinLockAcquire(&(buf->pointer_lock));
	buf->border = buf->head;
	SpinLockRelease(&(buf->pointer_lock));
}

/*
 * Send data into buffer
 */
void
PutData(DataPumpBuf *buf, char *data, uint32 len)
{	
	char   *ptr;
	uint32  bufferLen;
	uint32  needLen;
	uint32  offset = 0;
	needLen = len;
	while (1)
	{			
		ptr = GetWriteOff(buf, &bufferLen);
		if (ptr)
		{
			if (bufferLen >= needLen)
			{
				memcpy(ptr, data + offset, needLen);
				IncWriteOff(buf, needLen);
				return;
			}
			else
			{
				memcpy(ptr, data + offset, bufferLen);
				IncWriteOff(buf, bufferLen);
				needLen -= bufferLen;
				offset  += bufferLen;
			}
		}
	}
}


