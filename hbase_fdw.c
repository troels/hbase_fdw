#include "postgres.h"

#include "hbase_fdw.h"

/* These are always necessary for a bgworker */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/dsm.h"
#include "fmgr.h"
#include "utils/guc.h"
#include "utils/resowner.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

PG_MODULE_MAGIC;

static bool directory_exists(char *dir);
static bool file_exists(char *fn);
static void startup_background_worker(void);
static void initialize_jvm(void);

pthread_mutex_t postgres_mutex;

void hbase_fdw_main(Datum);
void _PG_init(void);

static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

static char *java_home;
static char *java_classpath;

// static dsm_segment_handle hbase_fdw_segment_handle;

char *candidate_paths[] = {
	"jre/lib/amd64/server/libjvm.so",
	"lib/amd64/server/libjvm.so",
	NULL
};

void
_PG_init(void)
{
	DefineCustomStringVariable(
		"hbase_fdw.java_home",
		"Java Home Directory",
		NULL,
		&java_home,
		NULL,
		PGC_SIGHUP,
		0,
		NULL,
		NULL,
		NULL
		);

	DefineCustomStringVariable(
		"hbase_fdw.classpath",
		"Java Classpath",
		NULL,
		&java_classpath,
		NULL,
		PGC_SIGHUP,
		0,
		NULL,
		NULL,
		NULL);

	if (!process_shared_preload_libraries_in_progress)
		return;

	initialize_shared_memory();

	startup_background_worker();
}

static void
startup_background_worker()
{
	BackgroundWorker worker;
	pid_t pid;

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_main = NULL;
	sprintf(worker.bgw_library_name, "hbase_fdw");
	sprintf(worker.bgw_function_name, "hbase_fdw_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "HBase FDW worker %d", 1);
	worker.bgw_notify_pid = 0;
	worker.bgw_main_arg = Int32GetDatum(1);

	RegisterBackgroundWorker(&worker);
}

static void
initialize_jvm(void)
{
	char *end_path = NULL;

	if (java_home == NULL)
		elog(FATAL, "hbase_fdw.java_home is null");

	if (!directory_exists(java_home))
		elog(FATAL, "Directory %s does not exist", java_home);

	for (int i = 0; candidate_paths[i] != NULL; i++)
	{
		char *path = end_path = palloc(strlen(java_home) + strlen(candidate_paths[i]) + 2);
		if (path == NULL) {
			elog(FATAL, "Failed to allocate memory for path");
		}
		strcpy(path, java_home);
		path += strlen(java_home);
		if (path[strlen(path) - 1] != '/')
		{
			strcpy(path, "/");
			path += 1;
		}
		strcpy(path, candidate_paths[i]);
		if (file_exists(end_path))
			break;
		pfree(end_path);

		end_path = NULL;
	}

	if (end_path == NULL)
		elog(FATAL, "Failed to find path to libjvm.so");

	elog(INFO, "Found libjvm.so: %s", end_path);
	open_jvm_lib(end_path);
	create_java_vm(java_classpath);

	pfree(end_path);
}


static void
shutdown_jvm()
{
	destroy_java_vm();
	close_jvm_lib();
}

static void
hbase_fdw_sighup(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_sighup = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

static void
hbase_fdw_sigterm(SIGNAL_ARGS)
{
	int save_errno = errno;

	SetLatch(MyLatch);
	if (!proc_exit_inprogress)
	{
		InterruptPending = true;
		ProcDiePending = true;
	}

	got_sigterm = true;

	errno = save_errno;
}

void
hbase_fdw_main(Datum main_arg)
{
	int worker_num = DatumGetInt32(main_arg);
	bool foundPtr;
	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, hbase_fdw_sighup);
	pqsignal(SIGTERM, hbase_fdw_sigterm);

	pthread_mutex_init(&postgres_mutex, NULL);
	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	elog(LOG, "JVM Initialized");

	CurrentResourceOwner = ResourceOwnerCreate(NULL, "test_shm_mq worker");

	setup_bgworker();
	initialize_jvm();
	initialize_hbase_connector();
	allocate_threads();

	while (!got_sigterm) {
		int rc;

		rc = WaitLatch(
			MyLatch,
			WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
			10000L);
		ResetLatch(MyLatch);

		if (rc & WL_POSTMASTER_DEATH)
		{
			shutdown_threads();
			shutdown_jvm();
			proc_exit(1);
		}
		maintain_workers();
	}

	shutdown_threads();
	destroy_hbase_connector();
	shutdown_jvm();
	proc_exit(1);
}

static bool
directory_exists(char *dir)
{
	struct stat s;
	int err = stat(dir, &s);
	if (err == -1)
	{
		if(errno == ENOENT) {
			return false;
		} else {
			elog(FATAL, "Could not check for existence of: %s", dir);
		}
	}
	else
		return S_ISDIR(s.st_mode);
}

static bool
file_exists(char *fn) {
	struct stat s;
	int err = stat(fn, &s);
	if (err == -1)
	{
		if(errno == ENOENT) {
			return false;
		} else {
			elog(FATAL, "Could not check for existence of: %s", fn);
		}
	}
	else
		return S_ISREG(s.st_mode);
}
