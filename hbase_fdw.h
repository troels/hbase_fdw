#ifndef HBASE_FDW_H
#define HBASE_FDW_H

#include <pthread.h>
#include "storage/shm_mq.h"
#include "storage/dsm.h"

#define HBASE_FDW_NUM_WORKERS 8
#define HBASE_FDW_WORKMEM_PER_WORKER 1048576

#define HBASE_FDW_MAX_FAMILY_LEN 31
#define HBASE_FDW_MAX_QUALIFIER_LEN 255

#define HBASE_FDW_MAX_HBASE_COLUMNS 64
#define HBASE_FDW_MAX_TABLE_NAME_LEN 64

#define HBASE_FDW_MAX_ROW_KEY_FILTER_LEN 128
#define HBASE_FDW_MAX_FILTERS 16

extern pthread_mutex_t postgres_mutex;
extern void *hbase_connector;

typedef struct ScannerData
{
	void *scan;
	void *scanner;
	void *byte_array;
	char *ptr;
} ScannerData;

typedef enum HBaseFdwMsgType {
	msg_type_tuple,
	msg_type_end_of_stream
} HBaseFdwMsgType;

typedef struct HBaseFdwMessage {
	HBaseFdwMsgType msg_type;
	char data[FLEXIBLE_ARRAY_MEMBER];
} HBaseFdwMessage;

typedef struct HBaseColumn {
	int attnum;

	bool row_key;
	bool family;
	bool column;

	char family_name[HBASE_FDW_MAX_FAMILY_LEN + 1];
	char qualifier[HBASE_FDW_MAX_QUALIFIER_LEN + 1];
} HBaseColumn;

typedef struct HBaseFilter {
	enum {
		filter_type_row_key_equals
	} filter_type;

	union {
		struct {
			char row_key[HBASE_FDW_MAX_ROW_KEY_FILTER_LEN + 1];
		} row_key_equals;
	};
} HBaseFilter;

typedef struct HBaseCommand {
	char table_name[HBASE_FDW_MAX_TABLE_NAME_LEN + 1];
	int nr_filters;
	int nr_columns;
	HBaseFilter filters[HBASE_FDW_MAX_FILTERS];
	HBaseColumn columns[HBASE_FDW_MAX_HBASE_COLUMNS];
} HBaseCommand;

#define with_pg_lock(ARG) \
   do { \
      pthread_mutex_lock(&postgres_mutex);  \
      ARG; \
      pthread_mutex_unlock(&postgres_mutex); \
   } while (0);

#define pg_elog(...) with_pg_lock(elog(__VA_ARGS__))

#define pg_palloc(VAR, SIZE) \
	with_pg_lock((VAR) = palloc0(SIZE))

#define pg_pfree(VAR) \
	with_pg_lock(pfree(VAR))


void *jvm_attach_thread(void);
void jvm_detach_thread(void);

ScannerData
setup_scanner(void *env_, char *table, HBaseColumn *columns, int nr_columns);
void
destroy_scanner(void *env_, ScannerData *scanner_data);
bool
scan_row(void *env, ScannerData *data);

void *
create_pg_hbase_columns(void *env_,
						HBaseColumn *columns,
						size_t n_columns);
void
free_local_jvm_obj(void *env_, void *object);

void pg_jsonb(void *env_, char *s);

void initialize_hbase_connector(void);
void destroy_hbase_connector(void);

void allocate_threads(void);
void shutdown_threads(void);

void thread_start_worker(int n, shm_mq_handle *tuples_mq, HBaseCommand *command);
void thread_reset_worker(int n);
bool thread_is_working(int n);

void setup_bgworker(void);
void maintain_workers(void);
void initialize_shared_memory(void);
void open_jvm_lib(char *libjvm_path) ;
void create_java_vm(char *classpath);
void do_jvm_op(void);
void destroy_java_vm(void);
void close_jvm_lib(void);

void pg_datum(void *env, char *s);

bool
activate_worker(char *table_name, HBaseColumn *columns, int nr_columns, dsm_handle handle);
void
reset_worker(int n);

#endif
