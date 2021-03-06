#include "postgres.h"

#include "hbase_fdw.h"

#include "storage/shmem.h"
#include "storage/dsm.h"
#include "storage/shm_toc.h"
#include "storage/s_lock.h"
#include "storage/spin.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "miscadmin.h"

typedef struct hbase_fdw_worker
{
	slock_t mutex;
	int worker_num;
	bool shutdown;
	bool is_activated;
	bool is_working;
	dsm_handle dsm_handle;
	dsm_segment *seg;
	HBaseCommand command;
} hbase_fdw_worker;

typedef struct hbase_fdw_control {
	LWLock *lock;
	slock_t mutex;
	int num_workers;
	Latch *latch;
	hbase_fdw_worker worker[FLEXIBLE_ARRAY_MEMBER];
} hbase_fdw_control;

static hbase_fdw_control *control;
static void hbase_fdw_shmem_startup(void);
static size_t ss_size(void);

static shmem_startup_hook_type old_startup_hook;

void
initialize_shared_memory(void)
{
	elog(LOG, "Running this");

	RequestAddinShmemSpace(ss_size());
	RequestNamedLWLockTranche("hbase_fdw", 1);

	old_startup_hook = shmem_startup_hook;
	shmem_startup_hook = hbase_fdw_shmem_startup;
}

static size_t
ss_size(void)
{
	return sizeof(*control) +
		HBASE_FDW_NUM_WORKERS * sizeof(hbase_fdw_worker);
}

static void
hbase_fdw_shmem_startup(void)
{
	bool found;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	control = ShmemInitStruct(
		"HBase FDW Background Worker Data",
		ss_size(),
		&found);

	if (!found) {
		control->lock = &(GetNamedLWLockTranche("hbase_fdw"))->lock;
		SpinLockInit(&control->mutex);
		control->num_workers = HBASE_FDW_NUM_WORKERS;

		for (int i = 0; i < control->num_workers; i++)
		{
			hbase_fdw_worker *worker = &control->worker[i];
			SpinLockInit(&worker->mutex);
			worker->is_activated = false;
			worker->is_working = false;
			worker->worker_num = i;
			worker->shutdown = false;
			worker->dsm_handle = 0;
		}
	}

	elog(LOG, "Initialized shared memory");

	LWLockRelease(AddinShmemInitLock);

	if (old_startup_hook != NULL)
		old_startup_hook();
}

void
setup_bgworker(void)
{
	control->latch = MyLatch;
	pg_write_barrier();
}

void
maintain_workers(void)
{
	for (int i = 0; i < control->num_workers; i++)
	{
		hbase_fdw_worker *worker = &control->worker[i];
 		SpinLockAcquire(&worker->mutex);
		if (worker->is_activated)
		{
			dsm_segment *seg;
			shm_mq *mq;
			shm_mq_handle *handle;
			shm_toc *toc;
			HBaseCommand *command;
			HBaseColumn *columns;
			HBaseFilter *filters;

			worker->is_activated = false;
			if (worker->dsm_handle == 0)
			{
				pg_elog(WARNING, "Expected a dsm handle");
				goto unlock_worker;
			}
			seg = dsm_attach(worker->dsm_handle);
			if (seg == NULL)
			{
				pg_elog(WARNING, "Failed to find segment");
				goto unlock_worker;
			}
			toc = shm_toc_attach(
				HBASE_FDW_SHM_TOC_MAGIC,
				dsm_segment_address(seg));

			if (toc == NULL)
			{
				pg_elog(WARNING, "Failed to connect to toc");
				goto unlock_worker;
			}

			command = shm_toc_lookup(toc, 1);
			columns = shm_toc_lookup(toc, 2);
			filters = shm_toc_lookup(toc, 3);
			mq = shm_toc_lookup(toc, 4);
			shm_mq_set_sender(mq, MyProc);

			handle = shm_mq_attach(mq, seg, NULL);
			worker->is_working = true;
			worker->seg = seg;
			thread_start_worker(i, handle, command, columns, filters);
		}

	unlock_worker:
		SpinLockRelease(&worker->mutex);
	}
}

bool
activate_worker(dsm_handle handle)
{
	for (int i = 0; i < control->num_workers; i++)
	{
		bool success = false;
		hbase_fdw_worker *worker = &control->worker[i];
		SpinLockAcquire(&worker->mutex);
		if (!worker->is_activated && !worker->is_working && !worker->shutdown)
		{
			pg_elog(LOG, "Activating worker");
			worker->is_activated = true;
			worker->dsm_handle = handle;
			worker->seg = NULL;
			success = true;
		}
		SpinLockRelease(&worker->mutex);
		if (success)
		{
			SetLatch(control->latch);
			return true;
		}
	}
	return false;
}

void
reset_worker(int n)
{
	hbase_fdw_worker *worker = &control->worker[n];
	SpinLockAcquire(&worker->mutex);
	worker->is_working = false;
	worker->is_activated = false;
	if (worker->seg != NULL)
	{
		dsm_detach(worker->seg);
		worker->seg = NULL;
	}
	worker->dsm_handle = 0;
	SpinLockRelease(&worker->mutex);
}
