/*
 * contrib/audit_test/audit_test.c
 */

#include "postgres_fe.h"

#include "libpq-fe.h"
#include "pg_getopt.h"
#include "port/atomics.h"
#include <assert.h>
#include <sys/stat.h>
#include <time.h>
#include <pthread.h>

#define TestAlogProducerCount  1000
#define TestAlogQueueSize 1200
#define TestAlogBuffSize 40960
#define TestAlogFileSize 102400000

#ifdef Assert
#undef Assert
#endif

#define Assert assert

typedef struct TestAuditLogQueue
{
	pid_t					q_pid;
	int						q_size;
	char					q_lock;
	volatile int			q_head;
	volatile int			q_tail;
	char					q_area[FLEXIBLE_ARRAY_MEMBER];
} AlogQueue;

static int shared_queue_idx[TestAlogProducerCount] = { 0 };
static AlogQueue * shared_queue [TestAlogProducerCount] = { 0 };
static AlogQueue * local_cache = NULL;
static char * alog_file_name = "test_alog.txt";
static FILE * alog_file_fp = NULL;

static char * 	alog_queue_offset_to(AlogQueue * queue, int offset);
static bool 	alog_queue_is_full(int q_size, int q_head, int q_tail);
static bool 	alog_queue_is_empty(int q_size, int q_head, int q_tail);
static bool 	alog_queue_is_enough(int q_size, int q_head, int q_tail, int N);
static int 		alog_queue_remain(int q_size, int q_head, int q_tail);
static int 		alog_queue_used(int q_size, int q_head, int q_tail);
static bool 	alog_queue_push(AlogQueue * queue, char * buff, int len);
static bool 	alog_queue_push2(AlogQueue * queue, char * buff1, int len1, char * buff2, int len2);
static bool 	alog_queue_pushn(AlogQueue * queue, char * buff[], int len[], int n);
static int 		alog_queue_get_str_len(AlogQueue * queue, int offset);
static void 	alog_queue_clear_str_len(AlogQueue * queue, int offset);
static bool 	alog_queue_pop_to_queue(AlogQueue * from, AlogQueue * to);
static bool 	alog_queue_pop_to_file(AlogQueue * from, FILE * logfile);
static int		alog_write_log_file(const char *buffer, int count, FILE * logfile);
static int 		alog_random_string(char buff[TestAlogBuffSize]);

int test_alog();
int test_alog0();


/* --------------------------------
 *		AlogQueue routines
 * --------------------------------
 */

/*
 * Get a write pointer in queue
 */
static char * alog_queue_offset_to(AlogQueue * queue, int offset)
{
	char * start = (char *) queue;

	Assert(offset >= 0 && offset < queue->q_size);

	start += offsetof(AlogQueue, q_area);
	start += offset;

	return start;
}

static bool alog_queue_is_full(int q_size, int q_head, int q_tail)
{
	Assert(q_size > 0 && q_head >= 0 && q_tail >= 0);
	Assert(q_head < q_size && q_tail < q_size);

    if ((q_tail + 1) % q_size == q_head)
    {
        return true;
    }
    else
    {
        return false;
    }
}

static bool alog_queue_is_empty(int q_size, int q_head, int q_tail)
{
	Assert(q_size > 0 && q_head >= 0 && q_tail >= 0);
	Assert(q_head < q_size && q_tail < q_size);

    if (q_tail == q_head)
    {
        return true;
    }
    else
    {
        return false;
    }
}

/*
 * how many bytes already in used
 */

static int alog_queue_used(int q_size, int q_head, int q_tail)
{
	int used = (q_tail - q_head + q_size) % q_size;

	Assert(q_size > 0 && q_head >= 0 && q_tail >= 0);
	Assert(q_head < q_size && q_tail < q_size);

	return used;
}


/*
 * how many bytes remain in Queue
 */
static int alog_queue_remain(int q_size, int q_head, int q_tail)
{
	int remain = (q_head - q_tail + q_size - 1) % q_size;

	Assert(q_size > 0 && q_head >= 0 && q_tail >= 0);
	Assert(q_head < q_size && q_tail < q_size);
	Assert(remain == (q_size - 1) - ((q_tail - q_head + q_size) % q_size));
	Assert(remain == (q_size - 1) - alog_queue_used(q_size, q_head, q_tail));

	return remain;
}

/*
 * whether queue has enough space for N bytes ?
 */
static bool alog_queue_is_enough(int q_size, int q_head, int q_tail, int N)
{
	int remain = alog_queue_remain(q_size, q_head, q_tail);

	Assert(q_size > 0 && q_head >= 0 && q_tail >= 0 && N > 0);
	Assert(q_head < q_size && q_tail < q_size);

	if (remain > N)
	{
		return true;
	}

	return false;
}

/*
 * write buff to queue
 *
 * len = size(int) + strlen(str)
 *
 */
static bool alog_queue_push(AlogQueue * queue, char * buff, int len)
{
	char * buff_array [] = { buff };
	int len_array [] = { len };

	return alog_queue_pushn(queue, buff_array, len_array, sizeof(len_array)/sizeof(len_array[0]));
}

/*
 * write buff1 and buff2 to queue
 */
static bool alog_queue_push2(AlogQueue * queue, char * buff1, int len1, char * buff2, int len2)
{
	char * buff_array[] = {buff1, buff2};
	int len_array[] = {len1, len2};

	return alog_queue_pushn(queue, buff_array, len_array, sizeof(len_array)/sizeof(len_array[0]));
}

static bool alog_queue_pushn(AlogQueue * queue, char * buff[], int len[], int n)
{
	volatile int q_head = queue->q_head;
	volatile int q_tail = queue->q_tail;
	volatile int q_size = queue->q_size;

	int q_head_before = q_head;
	int q_tail_before = q_tail;
	int q_size_before = q_size;

	int q_used_before = 0;
	int q_used_after = 0;

	int total_len = 0;
	int i = 0;

	for (i = 0; i < n; i++)
	{
		total_len += len[i];
	}

	pg_memory_barrier();

	Assert(q_size > 0 && q_head >= 0 && q_tail >= 0);
	Assert(q_head < q_size && q_tail < q_size);
	Assert(buff != NULL && len != 0 && n > 0 && total_len > 0);

	q_used_before = alog_queue_used(q_size_before, q_head_before, q_tail_before);

	if (alog_queue_is_full(q_size, q_head, q_tail))
	{
		return false;
	}

	if (!alog_queue_is_enough(q_size, q_head, q_tail, total_len))
	{
		return false;
	}

	for (i = 0; i < n; i++)
	{
		char * curr_buff = buff[i];
		int curr_len = len[i];

		/* has enough space, write directly */
		if (q_size - q_tail >= curr_len)
		{
			char * p_start = alog_queue_offset_to(queue, q_tail);
			memcpy(p_start, curr_buff, curr_len);
		}
		else
		{
			/* must write as two parts */
			int first_len = q_size - q_tail;
			int second_len = curr_len - first_len;

			char * first_buf = curr_buff + 0;
			char * second_buf = curr_buff + first_len;

			char * p_start = NULL;

			pg_memory_barrier();

			Assert(first_len > 0 && first_len < q_size);
			Assert(second_len > 0 && second_len < q_size);

			/* 01. write the first parts into the tail of queue->q_area */
			p_start = alog_queue_offset_to(queue, q_tail);
			memcpy(p_start, first_buf, first_len);

			Assert((q_tail + first_len) % q_size == 0);

			/* 02. write the remain parts into the head of queue->q_area */
			p_start = alog_queue_offset_to(queue, 0);
			memcpy(p_start, second_buf, second_len);
		}

		q_tail = (q_tail + curr_len) % q_size;
	}

	queue->q_tail = q_tail;

	q_used_after = alog_queue_used(q_size, q_head, q_tail);
	Assert(q_used_before + total_len == q_used_after);

	return true;
}

/*
 * |<- strlen value ->|<- string message content ->|
 * |											   |
 * |											   |
 * |<------------------ buff --------------------->|
 *
 * len = size(int) + strlen(str)
 *
 */
static int alog_queue_get_str_len(AlogQueue * queue, int offset)
{
	volatile int q_size = queue->q_size;
	char buff[sizeof(int)] = { '\0' };
	int len = 0;

	pg_memory_barrier();

	Assert(offset >= 0 && offset < q_size);

	/* read len directly */
	if (q_size - offset >= sizeof(int))
	{
		char * q_start = alog_queue_offset_to(queue, offset);
		memcpy(buff, q_start, sizeof(int));
	}
	else
	{
		/* must read as two parts */
		int first_len = q_size - offset;
		int second_len = sizeof(int) - first_len;

		char * p_start = NULL;

		pg_memory_barrier();

		Assert(first_len > 0 && first_len < q_size);
		Assert(second_len > 0 && second_len < sizeof(int));

		/* 01. copy the first parts */
		p_start = alog_queue_offset_to(queue, offset);
		memcpy(buff, p_start, first_len);

		/* 02. copy the remain parts */
		p_start = alog_queue_offset_to(queue, 0);
		memcpy(buff + first_len, p_start, second_len);
	}

	memcpy((char *)(&len), buff, sizeof(int));

	Assert(len > 0 && len < q_size);

	return len;
}

static void alog_queue_clear_str_len(AlogQueue * queue, int offset)
{
	volatile int q_size = queue->q_size;
	char buff[sizeof(int)] = { '\0' };

	pg_memory_barrier();

	Assert(offset >= 0 && offset < q_size);

	/* read len directly */
	if (q_size - offset >= sizeof(int))
	{
		char * q_start = alog_queue_offset_to(queue, offset);
		memcpy(q_start, buff, sizeof(int));
	}
	else
	{
		/* must read as two parts */
		int first_len = q_size - offset;
		int second_len = sizeof(int) - first_len;

		char * p_start = NULL;

		pg_memory_barrier();

		Assert(first_len > 0 && first_len < q_size);
		Assert(second_len > 0 && second_len < sizeof(int));

		/* 01. copy the first parts */
		p_start = alog_queue_offset_to(queue, offset);
		memcpy(p_start, buff, first_len);

		/* 02. copy the remain parts */
		p_start = alog_queue_offset_to(queue, 0);
		memcpy(p_start, buff, second_len);
	}
}

/*
 * copy message from queue to another as much as possible
 *
 * |<- strlen value ->|<- string message content ->|
 * |											   |
 * |											   |
 * |<------------------ buff --------------------->|
 *
 * len = size(int) + strlen(str)
 *
 */
static bool alog_queue_pop_to_queue(AlogQueue * from, AlogQueue * to)
{
	volatile int q_from_head = from->q_head;
	volatile int q_from_tail = from->q_tail;
	volatile int q_from_size = from->q_size;

	volatile int q_to_head = to->q_head;
	volatile int q_to_tail = to->q_tail;
	volatile int q_to_size = to->q_size;

	int from_head = q_from_head;
	int from_tail = q_from_tail;
	int from_size = q_from_size;

	int to_head = q_to_head;
	int to_tail = q_to_tail;
	int to_size = q_to_size;

	int from_total = 0;

	int from_used = 0;
	int from_copyed = 0;

	int to_used = 0;
	int to_copyed = 0;

	pg_memory_barrier();

	from_total = from_used = alog_queue_used(from_size, from_head, from_tail);
	to_used = alog_queue_used(to_size, to_head, to_tail);

	Assert(from_size > 0 && from_head >= 0 && from_tail >= 0);
	Assert(from_head < from_size && from_tail < from_size && from_used <= from_size);

	Assert(to_size > 0 && to_head >= 0 && to_tail >= 0);
	Assert(to_head < to_size && to_tail < to_size && to_used <= to_size);

	/* from is empty, ignore */
	if (alog_queue_is_empty(from_size, from_head, from_tail))
	{
		return false;
	}

	/* to is full, can not write */
	if (alog_queue_is_full(to_size, to_head, to_tail))
	{
		return false;
	}

	/* copy message into queue until to is full or from is empty */
	do
	{
		int string_len = alog_queue_get_str_len(from, from_head);
		int copy_len = sizeof(int) + string_len;

		pg_memory_barrier();

		Assert(string_len > 0 && string_len < from_size);
		Assert(copy_len > 0 && copy_len < from_size);

		if (!alog_queue_is_enough(to_size, to_head, to_tail, copy_len))
		{
			break;
		}

		/* just copy dierctly */
		if (from_size - from_head >= copy_len)
		{
			char * p_start = alog_queue_offset_to(from, from_head);
			if (!alog_queue_push(to, p_start, copy_len))
			{
				break;
			}
		}
		else
		{
			/* must copy as two parts */
			int first_len = from_size - from_head;
			int second_len = copy_len - first_len;
			char * p_first_start = NULL;
			char * p_second_start = NULL;

			Assert(first_len > 0 && first_len < from_size);
			Assert(second_len > 0 && second_len < from_size);

			p_first_start = alog_queue_offset_to(from, from_head);
			p_second_start = alog_queue_offset_to(from, 0);

			/* 01. copy the content parts into the tail of to->q_area */
			if (!alog_queue_push2(to, p_first_start, first_len, p_second_start, second_len))
			{
				break;
			}
		}

		from_head = (from_head + copy_len) % from_size;
		to_tail = (to_tail + copy_len) % to_size;

		from_copyed += copy_len;
		to_copyed += copy_len;

		Assert(from_copyed <= from_total);
		Assert(from_used - copy_len >= 0);
		Assert(to_used + copy_len <= to_size);
		Assert(from_used - copy_len == alog_queue_used(from_size, from_head, from_tail));
		Assert(to_used + copy_len == alog_queue_used(to_size, to_head, to_tail));

		from_used = alog_queue_used(from_size, from_head, from_tail);
		to_used = alog_queue_used(to_size, to_head, to_tail);
	} while (!alog_queue_is_empty(from_size, from_head, from_tail));

	from->q_head = from_head;

	return true;
}

/*
 * copy message from queue to file as much as possible
 */
static bool alog_queue_pop_to_file(AlogQueue * from, FILE * logfile)
{
	volatile int q_from_head = from->q_head;
	volatile int q_from_tail = from->q_tail;
	volatile int q_from_size = from->q_size;

	int from_head = q_from_head;
	int from_tail = q_from_tail;
	int from_size = q_from_size;

	int from_total = 0;

	int from_used = 0;
	int from_copyed = 0;

	pg_memory_barrier();

	from_total = from_used = alog_queue_used(from_size, from_head, from_tail);

	Assert(from_size > 0 && from_head >= 0 && from_tail >= 0);
	Assert(from_head < from_size && from_tail < from_size && from_used <= from_size);

	/* from is empty, ignore */
	if (alog_queue_is_empty(from_size, from_head, from_tail))
	{
		return false;
	}

	/* copy message into file until from is empty */
	do
	{
		int string_len = alog_queue_get_str_len(from, from_head);
		int copy_len = sizeof(int) + string_len;

		pg_memory_barrier();

		/* just copy dierctly */
		if (from_size - from_head >= copy_len)
		{
			char * p_start = alog_queue_offset_to(from, from_head + sizeof(int));

			/* only copy message content, not write message len */
			alog_write_log_file(p_start, string_len, logfile);
		}
		else if (from_size - from_head > sizeof(int))
		{
			/* must copy as two parts */
			int first_len = from_size - from_head - sizeof(int);
			int second_len = string_len - first_len;
			char * p_start = NULL;

			Assert(first_len > 0 && first_len < from_size);
			Assert(second_len > 0 && second_len < from_size);

			p_start = alog_queue_offset_to(from, from_head + sizeof(int));
			alog_write_log_file(p_start, first_len, logfile);

			p_start = alog_queue_offset_to(from, 0);
			alog_write_log_file(p_start, second_len, logfile);
		}
		else
		{
			/* just copy content only */
			int cpy_offset = (from_head + sizeof(int)) % from_size;
			char * p_start = alog_queue_offset_to(from, cpy_offset);

			Assert(from_size - from_head <= sizeof(int));
			alog_write_log_file(p_start, string_len, logfile);
		}

		from_head = (from_head + copy_len) % from_size;
		from_copyed += copy_len;

		Assert(from_copyed <= from_total);
		Assert(from_used - copy_len >= 0);
		Assert(from_used - copy_len == alog_queue_used(from_size, from_head, from_tail));

		from_used = alog_queue_used(from_size, from_head, from_tail);
	} while (!alog_queue_is_empty(from_size, from_head, from_tail));

	from->q_head = from_head;

	return true;
}

static int
alog_write_log_file(const char *buffer, int count, FILE * logfile)
{
	int	rc = 0;
	rc = fwrite(buffer, 1, count, logfile);

	/* can't use ereport here because of possible recursion */
	if (rc != count)
	{
		printf("could not write to audit log file: %s\n", strerror(errno));
		return -1;
	}

	return 0;
}

static AlogQueue *
alog_make_queue(int q_size_kb)
{
	AlogQueue * queue = NULL;
	Size alogSize = 0;

	alogSize = offsetof(AlogQueue, q_area);
	alogSize = alogSize + q_size_kb * 1024;

	queue = (AlogQueue *)malloc(alogSize);
	if (queue == NULL)
	{
		return NULL;
	}

	memset(queue, 0, alogSize);

	queue->q_pid = 0;
	queue->q_size = q_size_kb * 1024;
	queue->q_lock = 0;
	queue->q_head = 0;
	queue->q_tail = 0;

	return queue;
}

static FILE *
alog_open_log_file(const char *filename, const char *mode)
{
	FILE	   *fh = NULL;
	mode_t		oumask = 0;

	oumask = umask((mode_t) ((~(S_IWUSR | S_IRUSR | S_IWUSR)) & (S_IRWXU | S_IRWXG | S_IRWXO)));
	fh = fopen(filename, mode);
	umask(oumask);

	if (fh)
	{
		setvbuf(fh, NULL, PG_IOLBF, 0);
	}

	return fh;
}

static int alog_random_string(char buff[TestAlogBuffSize])
{
	int i = 0;

	char letter[] = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
					  'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z' };

	int len = rand() % TestAlogBuffSize;

	buff[0] = '\0';

	if (len == 0)
	{
		len += (TestAlogBuffSize/10);
	}
	else if (len < 0)
	{
		len *= -1;
	}

	if (len >= TestAlogBuffSize - 10)
	{
		len  = TestAlogBuffSize - 10;
	}

	// len = 100;

	memcpy(buff, (char *)(&len), sizeof(int));
	for (i = 0; i < len - 1; i++)
	{
		int j = i % sizeof(letter);
		buff[sizeof(int) + i] = letter[j];
	}

	buff[sizeof(int) + len - 1] = '\n';

	return sizeof(int) + len;
}

static void * alog_producer(void * para)
{
	int * idx = (int *) para;
	char buff[TestAlogBuffSize] = { '0' };

	srand(time(NULL));

	while (1)
	{
		int len = alog_random_string(buff);
		AlogQueue * queue = shared_queue [*idx];

		while (!alog_queue_push(queue, buff, len))
		{
			usleep(10000);
		}
	}

	return NULL;
}

static void * alog_consumer(void * para)
{
	while (1)
	{
		int i = 0;

		for (i = 0; i < TestAlogProducerCount; i++)
		{
			alog_queue_pop_to_queue(shared_queue[i], local_cache);

			if (0)
			{
				if (ftell(alog_file_fp) >= TestAlogFileSize * 1024L)
				{
					FILE * fh = alog_open_log_file(alog_file_name, "w");
					fclose(alog_file_fp);
					alog_file_fp = fh;
				}

				alog_queue_pop_to_file(shared_queue[i], alog_file_fp);
			}
		}
	}
	return NULL;
}

static void * alog_writer(void * para)
{
	FILE * file = alog_file_fp;

	while (1)
	{
		if (1)
		{
			if (ftell(file) >= TestAlogFileSize * 1024L)
			{
				FILE * fh = alog_open_log_file(alog_file_name, "w");
				fclose(file);
				file = fh;
			}

			alog_queue_pop_to_file(local_cache, file);
		}
	}

	return NULL;
}

enum MT_thr_detach
{
	MT_THR_JOINABLE,
	MT_THR_DETACHED
};

static int32 CreateThread(void *(*f) (void *), void *arg, int32 mode)
{

    pthread_attr_t attr;
    pthread_t      threadid;
    int            ret = 0;

    pthread_attr_init(&attr);
    switch (mode)
    {
        case MT_THR_JOINABLE:
            {
                pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
                break;
            }
        case MT_THR_DETACHED:
            {
                pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
                break;
            }
        default:
			break;
    }
    ret = pthread_create(&threadid, &attr, f, arg);
    return ret;
}

int test_alog()
{
	int queue_size_kb = TestAlogQueueSize;

	int i = 0;

	for (i = 0; i < TestAlogProducerCount; i++)
	{
		shared_queue[i] = alog_make_queue(queue_size_kb);
		shared_queue_idx[i] = i;
	}

	local_cache = alog_make_queue(queue_size_kb);

	alog_file_fp = alog_open_log_file(alog_file_name, "a");

	CreateThread(alog_writer, NULL ,MT_THR_DETACHED);

	for (i = 0; i < TestAlogProducerCount; i++)
	{
		CreateThread(alog_producer, (void *) (&(shared_queue_idx[i])), MT_THR_DETACHED);
	}

	alog_consumer(NULL);

	return 0;
}

int test_alog0()
{

	int queue_size_kb = TestAlogQueueSize;
	AlogQueue * q0 = NULL;
	AlogQueue * q1 = NULL;

	char buff[TestAlogBuffSize] = { '0' };
	int len = 0;

	srand(time(NULL));

	q0 = alog_make_queue(queue_size_kb);
	q1 = alog_make_queue(queue_size_kb);

	do
	{
		len = alog_random_string(buff);
	} while (alog_queue_push(q0, buff, len));

	alog_queue_pop_to_queue(q0, q1);

	do
	{
		FILE * file = alog_open_log_file(alog_file_name, "a");
		alog_queue_pop_to_file(q1, file);
	} while(0);

	return 0;
}

