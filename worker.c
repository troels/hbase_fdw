#include "postgres.h"

#include "hbase_fdw.h"

#include "storage/spin.h"
#include "storage/s_lock.h"
#include "storage/shm_mq.h"
#include "port/atomics.h"
#include "miscadmin.h"

#include <stdio.h>
#include <pthread.h>
#include <unistd.h>

typedef struct thread_data  {
	slock_t mutex;
	pthread_cond_t cond;
	pthread_mutex_t cond_mutex;
	pthread_t thread;
	int worker_num;
	bool shutdown_worker;
	void *jvm_env;
	HBaseCommand *command;
	HBaseColumn *columns;
	HBaseFilter *filters;
	shm_mq_handle *tuples_mq;
} thread_data;

thread_data *threads;

static void *
run_worker(void *thread_data);

static bool
check_for_exit(thread_data *thread_data);

void
thread_start_worker(int n, shm_mq_handle *tuples_mq,
					HBaseCommand *command,
					HBaseColumn *columns,
					HBaseFilter *filters)
{
	thread_data *data = &threads[n];
	pthread_mutex_lock(&data->cond_mutex);
	data->tuples_mq = tuples_mq;
	data->command = command;
	data->columns = columns;
	data->filters = filters;
	pthread_cond_signal(&data->cond);
	pthread_mutex_unlock(&data->cond_mutex);
}

void
thread_reset_worker(int n)
{
	thread_data *data = &threads[n];
	data->tuples_mq = NULL;
	data->command = NULL;
	data->columns = NULL;
	data->filters = NULL;
	reset_worker(n);
	SetLatch(MyLatch);
}

bool
thread_is_working(int n)
{
	thread_data *data = &threads[n];
	pg_read_barrier();
	return data->command != NULL;
}

void
allocate_threads()
{
	threads = palloc0(sizeof(*threads) * HBASE_FDW_NUM_WORKERS);
	for (int i = 0; i < HBASE_FDW_NUM_WORKERS; i++)
	{
		threads[i].jvm_env = NULL;
		threads[i].worker_num = i;
		threads[i].shutdown_worker = false;
		threads[i].command = NULL;
		pthread_cond_init(&threads[i].cond, NULL);
		pthread_mutex_init(&threads[i].cond_mutex, NULL);
		SpinLockInit(&threads[i].mutex);
		pthread_create(&threads[i].thread, NULL, run_worker, &threads[i]);
	}
}

static void *
run_worker(void *data)
{
	thread_data *thread_data = data;
	void *jvm_cols;
	struct timespec time = { 5, 0 };

	pthread_mutex_lock(&thread_data->cond_mutex);
	thread_data->jvm_env = jvm_attach_thread();

	while (!check_for_exit(thread_data)) {
		pthread_cond_wait(
			&thread_data->cond,
			&thread_data->cond_mutex);

		if (thread_data->command != NULL)
		{
			ScannerData scanner_data;
			bool more_rows = true;
			shm_mq_result res;
			scanner_data = setup_scanner(
				thread_data->jvm_env,
				thread_data->command->table_name,
				thread_data->columns,
				thread_data->command->nr_columns,
				thread_data->filters,
				thread_data->command->nr_filters);

			if (scanner_data.scanner == NULL)
				more_rows = false;

			while (more_rows)
			{
				HBaseFdwMessage end_message;
				HBaseFdwMessage *msg;
				int len;
				more_rows = scan_row(thread_data->jvm_env, &scanner_data);

				if (more_rows) {
					len = *(int*)scanner_data.ptr;
					msg = (HBaseFdwMessage*)scanner_data.ptr;
					msg->msg_type = msg_type_tuple;
				}
				else
				{
					msg = &end_message;
					msg->msg_type = msg_type_end_of_stream;
					len = sizeof(*msg);
				}
				res = shm_mq_send(thread_data->tuples_mq, len, msg, false);
				if (res == SHM_MQ_DETACHED)
				{
					pg_elog(WARNING, "Subprocess detached");
					break;
				}
			}
			destroy_scanner(thread_data->jvm_env, &scanner_data);
			thread_reset_worker(thread_data->worker_num);
		}
	}
	jvm_detach_thread();
	return NULL;
}

static bool
check_for_exit(thread_data *thread_data)
{
	bool shutdown;
	pg_read_barrier();
	shutdown = thread_data->shutdown_worker;
	pg_read_barrier();
	return shutdown;
}

void
shutdown_threads(void)
{
	for (int i = 0; i < HBASE_FDW_NUM_WORKERS; i++)
	{
		threads[i].shutdown_worker = true;
		pg_write_barrier();
		pthread_cond_signal(&threads[i].cond);
	}

	for (int i = 0; i < HBASE_FDW_NUM_WORKERS; i++)
	{
		pthread_join(threads[i].thread, NULL);
	}
}
