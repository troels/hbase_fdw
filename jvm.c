#include "postgres.h"

#include "hbase_fdw.h"
#include "utils/memutils.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"

#include <string.h>
#include <unistd.h>
#include <jni.h>

#include <dlfcn.h>

static void *jvm_lib = NULL;
static JavaVM *jvm = NULL;
static JNIEnv *jvm_env = NULL;
void *hbase_connector = NULL;

static void log_exception(JNIEnv *env);
static void hbase_worker(void);
static jbyteArray make_byte_array(JNIEnv *env, char *bytes, int len);
static void parse_hbase_data(char *data);
static void release_scanner_bytes(void *env_, ScannerData *scanner_data);

static jobject create_hbase_connector(JNIEnv *env);

static jobject
create_filters(JNIEnv *env, HBaseFilter *filters, int nr_filters);


void open_jvm_lib(char *libjvm_path)
{
	if (jvm_lib != NULL)
		elog(FATAL, "There is already an open JVM");

	jvm_lib = dlopen(libjvm_path, RTLD_LAZY);
	if (jvm_lib == NULL)
		elog(FATAL, "Failed to open JVM: %s", dlerror());

	elog(LOG, "Loaded JVM: %s", libjvm_path);
}

static jint (*JNI_CreateJavaVM_ptr)(JavaVM **pvm, void **penv, void *args);

void initialize_hbase_connector(void)
{
	hbase_connector = create_hbase_connector(jvm_env);
	if (hbase_connector == NULL)
	{
		pg_elog(ERROR, "Failed to create HBaseConnector");
	}
}

void
destroy_hbase_connector(void)
{
	if (hbase_connector == NULL) return;
	(*jvm_env)->DeleteGlobalRef(jvm_env, hbase_connector);
	hbase_connector = NULL;
}


void create_java_vm(char *java_classpath)
{
	char *classpath_prefix = "-Djava.class.path=";
	JavaVMInitArgs vm_args;
	JavaVMOption options[4];
	char *classpath;

	if (java_classpath == NULL)
		elog(FATAL, "Java classpath must be set");

	classpath = palloc(strlen(classpath_prefix) + strlen(java_classpath) + 1);
	strcpy(classpath, classpath_prefix);
	strcat(classpath, java_classpath);

	options[3].optionString = "-Xrs";
	options[1].optionString = "-Xusealtsigs";
	options[2].optionString = "-Xmx1024M";
	options[0].optionString = classpath;

	vm_args.version = JNI_VERSION_1_8;
    vm_args.nOptions = 4;
    vm_args.options = &options[0];
    vm_args.ignoreUnrecognized = 0;

	if (jvm != NULL)
		return;

	if (jvm_lib == NULL)
		elog(FATAL, "Jvm lib was not opened");

	if (JNI_CreateJavaVM_ptr == NULL)
	{
		elog(INFO, "Fetching JNI_CreateJavaVM pointer");
		JNI_CreateJavaVM_ptr = dlsym(jvm_lib, "JNI_CreateJavaVM");
		if (JNI_CreateJavaVM_ptr == NULL)
			elog(FATAL, "Failed to find JNI_CreateJavaVM: %s", dlerror());
	}

	elog(LOG, "Creating JVM");
	if (JNI_CreateJavaVM_ptr(&jvm, (void**)&jvm_env, &vm_args) < 0)
		elog(FATAL, "Could not create JavaVM");
	pfree(classpath);
}

void
do_jvm_op(void)
{

	jclass clz;
	jmethodID met = NULL;
	jstring str = NULL;
	jstring res = NULL;
	char *env = "java.class.path";


	pg_elog(LOG, "Got HBase connector");
	return;
	/* hbase_worker(); */

	clz = (*jvm_env)->FindClass(jvm_env, "java/lang/System");
	if (clz == NULL)
	{
		log_exception(jvm_env);
		elog(WARNING, "Failed to get class");
		goto exit;
	}

	met = (*jvm_env)->GetStaticMethodID(jvm_env, clz, "getProperty", "(Ljava/lang/String;)Ljava/lang/String;");

	if (met == NULL)
	{
		elog(WARNING, "Failed to get method: getProperty");
		goto exit;
	}

	str = (*jvm_env)->NewStringUTF(jvm_env, env);
	res = (*jvm_env)->CallStaticObjectMethod(jvm_env, clz, met, str);
	if (res == NULL)
	{
		log_exception(jvm_env);
		elog(WARNING, "Failed to get string.");
		goto exit;
	}
	{
		const char *str = (*jvm_env)->GetStringUTFChars(jvm_env, res, 0);
		elog(LOG, "classpath: %s", str);
		(*jvm_env)->ReleaseStringUTFChars(jvm_env, res, str);
	}
	(*jvm_env)->DeleteLocalRef(jvm_env, res);
 exit:
	(*jvm_env)->DeleteLocalRef(jvm_env, str);
	(*jvm_env)->DeleteLocalRef(jvm_env, clz);
}

void
close_jvm_lib(void)
{
	if (jvm_lib == NULL)
		return;

	if (dlclose(jvm_lib) != 0)
		elog(WARNING, "Failed to close JVM");
	jvm_lib = NULL;
}

void
destroy_java_vm(void)
{
	if (jvm == NULL)
		return;

	if ((*jvm)->DestroyJavaVM(jvm) < 0)
		elog(WARNING, "Failed to destroy JavaVM");
	else
		elog(LOG, "Java VM destroyed succesfully");
	jvm = NULL;
	jvm_env = NULL;
}

static void
log_exception(JNIEnv *env)
{
	jthrowable t = NULL;
	jclass clz = NULL;
	jmethodID toString = NULL;
	jobject obj = NULL;
	jmethodID printStackTrace = NULL;

	const char *chars;
	if (!(*env)->ExceptionCheck(env))
		return;

	t = (*env)->ExceptionOccurred(env);
	if (t == NULL)
	{
		pg_elog(WARNING, "No exceptions. But I expected one");
		return;
	}

	(*env)->ExceptionClear(env);

	clz = (*env)->GetObjectClass(env, t);
	if (clz == NULL)
	{
		pg_elog(WARNING, "Failed to get class for object.");
		goto exit;
	}

	toString = (*env)->GetMethodID(env, clz, "toString", "()Ljava/lang/String;");
	if (toString == NULL)
	{
		pg_elog(WARNING, "Failed to get method toString from exception class");
		goto exit;
	}

	printStackTrace = (*env)->GetMethodID(env, clz, "printStackTrace", "()V");
	if (printStackTrace == NULL)
	{
		pg_elog(WARNING, "Failed to get method printStackTrace");
		goto exit;
	}
	(*env)->CallVoidMethodA(env, t, printStackTrace, NULL);

 exit:
	(*env)->DeleteLocalRef(env, clz);
	(*env)->DeleteLocalRef(env, obj);
	(*env)->DeleteLocalRef(env, t);
}

static void
hbase_worker(void)
{
	jclass hbase_connector = NULL;
	jclass hbase_scanner = NULL;
	jmethodID makeScanner = NULL;
	jbyteArray arr = NULL;
	jobject scanner = NULL;
	jobject byte_buffer = NULL;
	char *data = NULL;
	jmethodID scan = NULL;
	jboolean finished;

	hbase_connector  = (*jvm_env)->FindClass(jvm_env, "org/bifrost/HBaseConnector");

	if (hbase_connector == NULL)
	{
		log_exception(jvm_env);
		elog(WARNING, "Failed to find class org/bifrost/HBaseConnector");
		goto exit;
	}

	makeScanner = (*jvm_env)->GetStaticMethodID(
		jvm_env,
		hbase_connector,
		"makeScanner",
		"([B)Lorg/bifrost/Scanner;");
	if (makeScanner == NULL)
	{
		elog(WARNING, "Failed to get method make scanner");
		goto exit;
	}

	arr = make_byte_array(jvm_env, "table", 5);
	if (arr == NULL)
	{
		elog(WARNING, "Failed to generate byte array");
		goto exit;
	}

	scanner = (*jvm_env)->CallStaticObjectMethod(
		jvm_env,
		hbase_connector,
		makeScanner,
		arr);
	if ((*jvm_env)->ExceptionCheck(jvm_env))
	{
		elog(WARNING, "Exception thrown while calling makeScanner");
		log_exception(jvm_env);
		goto exit;
	}

	if (scanner == NULL)
	{
		elog(WARNING, "Got NULL from makeScanner");
		goto exit;
	}

	elog(LOG, "Got a scanner!");

	data = palloc(16384);
	memset(data, 0, 16384);
	if (data == NULL)
	{
		elog(WARNING, "Failed to allocate memory.");
		goto exit;
	}

	byte_buffer = (*jvm_env)->NewDirectByteBuffer(jvm_env, data, 16384);
	if (byte_buffer == NULL)
	{
		elog(WARNING, "Failed to allocate bytebuffer.");
		goto exit;
	}

	hbase_scanner = (*jvm_env)->GetObjectClass(jvm_env, scanner);
	if (hbase_scanner == NULL)
	{
		elog(WARNING, "Failed to get hbase scanner class.");
		goto exit;
	}

	scan = (*jvm_env)->GetMethodID(jvm_env,
								   hbase_scanner,
								   "scan",
								   "(Ljava/nio/ByteBuffer;)Z");
	if (scan == NULL)
	{
		elog(WARNING, "Failed to find scan method.");
		goto exit;
	}

	elog(LOG, "Got scan method");

	finished = (*jvm_env)->CallBooleanMethod(jvm_env, scanner, scan, byte_buffer);
	if ((*jvm_env)->ExceptionCheck(jvm_env))
	{
		elog(WARNING, "Exception calling int-method");
		log_exception(jvm_env);
		goto exit;
	}

	if (finished)
	{
		elog(LOG, "Finished scanning");
	} else {
		elog(LOG, "There is more from java.");
	}

	parse_hbase_data(data);
 exit:
	if ((*jvm_env)->ExceptionCheck(jvm_env))
		(*jvm_env)->ExceptionClear(jvm_env);

	if (hbase_scanner != NULL)
		(*jvm_env)->DeleteLocalRef(jvm_env, hbase_scanner);
	if (byte_buffer != NULL)
		(*jvm_env)->DeleteLocalRef(jvm_env, byte_buffer);

	if (data)
		pfree(data);

	if (scanner != NULL)
		(*jvm_env)->DeleteLocalRef(jvm_env, scanner);


	if (arr != NULL)
		(*jvm_env)->DeleteLocalRef(jvm_env, arr);

	if (hbase_connector != NULL)
		(*jvm_env)->DeleteLocalRef(jvm_env, hbase_connector);
}

static jbyteArray
make_byte_array(JNIEnv *env, char *bytes, int len)
{
	jbyteArray arr = (*env)->NewByteArray(env, len);
	char *raw_array = NULL;
	if (arr == NULL) {
		log_exception(env);
		pg_elog(WARNING, "Failed to get byte array.");
		goto error_exit;
	}

	raw_array = (*env)->GetPrimitiveArrayCritical(env, arr, 0);
	if (raw_array == NULL)
	{
		log_exception(env);
		pg_elog(WARNING, "Failed to get raw array");
		goto error_exit;
	}

	memcpy(raw_array, bytes, len);

	(*env)->ReleasePrimitiveArrayCritical(env, arr, raw_array, 0);
	return arr;

 error_exit:
	if (raw_array != NULL)
		(*env)->ReleasePrimitiveArrayCritical(env, arr, raw_array, 0);

	if (arr != NULL)
		(*env)->DeleteLocalRef(env, arr);

	return NULL;
}

static jobject
create_hbase_connector(JNIEnv *env)
{
	char *hbase_connector_class_name = "org/bifrost/HBaseConnector";
	char *hbase_connector_constructor_name = "<init>";
	char *hbase_connector_constructor_signature = "()V";
	jclass hbase_connector_class = NULL;
	jmethodID constructor = NULL;
	jobject local_hbase_connector = NULL;
	jobject global_hbase_connector = NULL;

	hbase_connector_class = (*env)->FindClass(env, hbase_connector_class_name);
	if (hbase_connector_class == NULL)
	{
		log_exception(env);
		pg_elog(WARNING, "Failed to get %s", hbase_connector_class_name);
		goto exit;
	}

	constructor = (*env)->GetMethodID(
		env,
		hbase_connector_class,
		hbase_connector_constructor_name,
		hbase_connector_constructor_signature);
	if (constructor == NULL)
	{
		log_exception(env);
		pg_elog(WARNING, "Failed to get constructor for %s", hbase_connector_class_name);
		goto exit;
	}

	local_hbase_connector = (*env)->NewObject(env,
											  hbase_connector_class,
											  constructor);
	if (local_hbase_connector == NULL || (*env)->ExceptionCheck(env))
	{
		log_exception(env);
		pg_elog(WARNING, "Failed to construct %s", hbase_connector_class_name);
		goto exit;
	}

	global_hbase_connector = (*env)->NewGlobalRef(env, local_hbase_connector);
	if (global_hbase_connector == NULL)
	{
		log_exception(env);
		pg_elog(WARNING, "Failed to make global reference to hbase connector");
		goto exit;
	}

 exit:
	if (local_hbase_connector != NULL)
		(*env)->DeleteLocalRef(env, local_hbase_connector);
	if (hbase_connector_class != NULL)
		(*env)->DeleteLocalRef(env, hbase_connector_class);
	return global_hbase_connector;
}

static void
parse_hbase_data(char *data)
{
	int next_offset = 0;

	while (*(int*)(data + next_offset))
	{
		char *cur_data = data + next_offset;
		int next_family;
		char *family_data;
		int row_key_len = *(int*)(cur_data + sizeof(int));
		char *row_key = palloc(row_key_len + 1);
		memcpy(row_key, cur_data + 2 * sizeof(int), row_key_len);
		row_key[row_key_len] = '\0';
		elog(LOG, "Row key was: %s", row_key);
		pfree(row_key);

		next_family = INTALIGN(next_offset + row_key_len + 2 * sizeof(int));
		while (*(int*) (data + next_family)) {
			int cur_family = next_family;
			char *family;
			int family_len;
			next_family = *(int*)(data + cur_family);
			elog(LOG, "Next family: %d", next_family);
			elog(LOG, "Cur family: %d", cur_family);

			family_len = *(int*)(data + cur_family + sizeof(int));
			elog(LOG, "Cur len: %d", family_len);

			family = palloc(family_len + 1);
			memcpy(family,  data + cur_family + 2 * sizeof(int), family_len);
			family[family_len] = '\0';
			elog(LOG, "Family was: %s", family);
			pfree(family);
		};
		next_offset = *(int*)(data + next_offset);
		elog(LOG, "Found pointer to offset: %d", next_offset);
	}
}

void
pg_datum(void *env_, char *s)
{
	JNIEnv *env = env_;
	char *pg_datum_classname = "org/bifrost/PgDatum";
	char *write_text_datum_method_name = "writeTextDatum";
	char *write_text_datum_signature = "(Ljava/lang/String;Ljava/nio/ByteBuffer;)I";
	jclass pg_datum;
	jmethodID writeTextDatum;
	jstring str = NULL;
	char *buf = NULL;
	jobject byte_buf = NULL;
	jint num_bytes;
	text *datum;
	char *out;
	pg_palloc(buf, 1024);

	pg_datum = (*env)->FindClass(env, pg_datum_classname);
	if (pg_datum == NULL)
	{
		pg_elog(WARNING, "Failed to find class %s", pg_datum_classname);
		log_exception(env);
		goto exit;
	}

	writeTextDatum = (*env)->GetStaticMethodID(
		env,
		pg_datum,
		write_text_datum_method_name,
		write_text_datum_signature
	);

	if (writeTextDatum == NULL)
	{
		pg_elog(WARNING, "Failed to get method %s", write_text_datum_method_name);
		log_exception(env);
		goto exit;
	}

	str = (*env)->NewStringUTF(env, "teststring");
	if (str == NULL)
	{
		pg_elog(WARNING, "Failed to get method %s", write_text_datum_method_name);
		log_exception(env);
		goto exit;
	}

	byte_buf = (*env)->NewDirectByteBuffer(env, buf, 1024);
	if (byte_buf == NULL)
	{
		pg_elog(WARNING, "Failed to to create byte buffer");
		log_exception(env);
		goto exit;
	}

	num_bytes = (*env)->CallStaticIntMethod(env,
											pg_datum,
											writeTextDatum,
											str,
											byte_buf);
	log_exception(env);
	pg_elog(LOG, "Got %d data", num_bytes);
	datum = (text*)(buf);


	pg_elog(
		LOG, "Got %lu %p %p", VARSIZE_ANY_EXHDR(datum),
		VARDATA_ANY(datum),
		datum);
	with_pg_lock(out = text_to_cstring(datum));
	pg_elog(LOG, "Data: %s", out);

 exit:
	if (str != NULL)
		(*env)->DeleteLocalRef(env, str);

	if (byte_buf != NULL)
		(*env)->DeleteLocalRef(env, byte_buf);

	if (pg_datum != NULL)
		(*env)->DeleteLocalRef(env, pg_datum);

	pg_pfree(buf);
}

void
pg_jsonb(void *env_, char *s)
{
	JNIEnv *env = env_;
	char *pg_datum_classname = "org/bifrost/PgDatum";
	char *write_jsonb_datum_method_name = "writeJsonb";
	char *write_jsonb_datum_signature = "(Ljava/nio/ByteBuffer;)I";
	jclass pg_datum;
	jmethodID writeJsonbDatum;
	jstring str = NULL;
	char *buf = NULL;
	jobject byte_buf = NULL;
	jint num_bytes;
	Datum datum;
	Datum jsonb_datum;
	char *out;
	pg_palloc(buf, 1024);

	pg_datum = (*env)->FindClass(env, pg_datum_classname);
	if (pg_datum == NULL)
	{
		pg_elog(WARNING, "Failed to find class %s", pg_datum_classname);
		log_exception(env);
		goto exit;
	}

	writeJsonbDatum = (*env)->GetStaticMethodID(
		env,
		pg_datum,
		write_jsonb_datum_method_name,
		write_jsonb_datum_signature
	);

	if (writeJsonbDatum == NULL)
	{
		pg_elog(WARNING, "Failed to get method %s", write_jsonb_datum_method_name);
		log_exception(env);
		goto exit;
	}

	str = (*env)->NewStringUTF(env, "teststring");
	if (str == NULL)
	{
		pg_elog(WARNING, "Failed to get method %s", write_jsonb_datum_method_name);
		log_exception(env);
		goto exit;
	}

	byte_buf = (*env)->NewDirectByteBuffer(env, buf, 1024);
	if (byte_buf == NULL)
	{
		pg_elog(WARNING, "Failed to to create byte buffer");
		log_exception(env);
		goto exit;
	}

	num_bytes = (*env)->CallStaticIntMethod(env,
											pg_datum,
											writeJsonbDatum,
											byte_buf);
	log_exception(env);
	pg_elog(LOG, "Got %d data", num_bytes);
	jsonb_datum = (Datum)buf;

	with_pg_lock(
		datum = DirectFunctionCall1(jsonb_pretty, jsonb_datum);
		do {
			text *t = (text*)datum;
			char *c = text_to_cstring(t);
			elog(LOG, "Text: %s", c);
		} while (0)
		);


 exit:
	if (str != NULL)
		(*env)->DeleteLocalRef(env, str);

	if (byte_buf != NULL)
		(*env)->DeleteLocalRef(env, byte_buf);

	if (pg_datum != NULL)
		(*env)->DeleteLocalRef(env, pg_datum);

	pg_pfree(buf);
}

void *
jvm_attach_thread(void)
{
	void* env = NULL;
	int i = 0;
	if ((i = (*jvm)->AttachCurrentThread(jvm, &env, NULL)) < 0) {
		pg_elog(WARNING, "There was a problem: %d", i);
	}
	return env;
}

void
jvm_detach_thread(void)
{
	(*jvm)->DetachCurrentThread(jvm);
}

void *
create_pg_hbase_columns(void *env_, HBaseColumn *columns, size_t n_columns)
{
	JNIEnv *env = env_;
	char *hbase_column_class_name = "org/bifrost/PgHbaseColumn";
	char *hbase_column_constructor_name = "<init>";
	char *hbase_column_constructor_signature = "(ZZZ[B[B)V";
	jmethodID hbase_column_constructor = NULL;
	jclass hbase_column_class = NULL;
	jobjectArray res = NULL;
	size_t i;

	hbase_column_class = (*env)->FindClass(env, hbase_column_class_name);
	if (hbase_column_class == NULL)
	{
		pg_elog(WARNING, "Failed to find %s", hbase_column_class_name);
		log_exception(env);
		goto error_exit;
	}

	hbase_column_constructor = (*env)->GetMethodID(
		env,
		hbase_column_class,
		hbase_column_constructor_name,
		hbase_column_constructor_signature);
	if (hbase_column_constructor == NULL)
	{
		pg_elog(WARNING, "Failed to get constructor for %s", hbase_column_class_name);
		log_exception(env);
		goto error_exit;
	}

	res = (*env)->NewObjectArray(env, n_columns, hbase_column_class, NULL);
	if (res == NULL)
	{
		pg_elog(WARNING, "Failed to create object array of %s", hbase_column_class_name);
		log_exception(env);
		goto error_exit;
	}

	for  (i = 0; i < n_columns; i++)
	{
		HBaseColumn *col = &columns[i];
		jbyteArray family_name = NULL;
		jbyteArray qualifier = NULL;
		jobject column = NULL;
		bool error = false;

		if (col->family_name[0] != '\0')
		{
			family_name = make_byte_array(env, col->family_name, strlen(col->family_name));
			if (family_name == NULL)
			{
				error = true;
				goto loop_exit;
			}
		}

		if (col->qualifier[0] != '\0')
		{
			qualifier = make_byte_array(env, col->qualifier, strlen(col->qualifier));
			if (qualifier == NULL)
			{
				error = true;
				goto loop_exit;
			}
		}

		column = (*env)->NewObject(
			env,
			hbase_column_class,
			hbase_column_constructor,
			(jboolean)col->row_key,
			(jboolean)col->family,
			(jboolean)col->column,
			family_name,
			qualifier
			);

		if (column == NULL || (*env)->ExceptionCheck(env))
		{
			pg_elog(WARNING, "Failed to create HBase Column");
			log_exception(env);
			error = true;
			goto loop_exit;
		}

		(*env)->SetObjectArrayElement(env, res, i, column);
		if ((*env)->ExceptionCheck(env))
		{
			pg_elog(WARNING, "Failed to set object array element");
			log_exception(env);
			error = true;
			goto loop_exit;
		}

		loop_exit:

		if (column != NULL)
			(*env)->DeleteLocalRef(env, column);

		if (family_name != NULL)
			(*env)->DeleteLocalRef(env, column);

		if (qualifier != NULL)
			(*env)->DeleteLocalRef(env, column);

		if (error)
			goto error_exit;
	}

	goto exit;

 error_exit:
	if (res != NULL)
	{
		(*env)->DeleteLocalRef(env, res);
		res = NULL;
	}

 exit:
	if (hbase_column_class != NULL)
		(*env)->DeleteLocalRef(env, hbase_column_class);

	return res;
}

static jobject
create_filters(JNIEnv *env, HBaseFilter *filters, int nr_filters)
{
	char *filter_creator_class_name = "org/bifrost/HBaseFilterCreator";
	char *row_key_equals_creator_method_name = "addRowKeyEqualsFilter";
	char *row_key_equals_creator_method_signature = "([B)V";
	char *constructor_method_name = "<init>";
	char *constructor_method_signature = "()V";
	jclass filter_creator_class = NULL;
	jmethodID constructor_method = NULL;
	jmethodID row_key_equals_creator = NULL;
	jobject creator = NULL;

	filter_creator_class = (*env)->FindClass(env, filter_creator_class_name);
	if (filter_creator_class == NULL)
	{
		log_exception(env);
		pg_elog(WARNING, "Failed get %s class", filter_creator_class_name);
		goto error_exit;
	}

	constructor_method = (*env)->GetMethodID(
		env,
		filter_creator_class,
		constructor_method_name,
		constructor_method_signature);

	if (constructor_method == NULL)
	{
		log_exception(env);
		pg_elog(WARNING, "Failed to get %s from %s",
				constructor_method_name,
				filter_creator_class_name);
		goto error_exit;
	}

	row_key_equals_creator = (*env)->GetMethodID(
		env,
		filter_creator_class,
		row_key_equals_creator_method_name,
		row_key_equals_creator_method_signature);

	if (row_key_equals_creator == NULL)
	{
		log_exception(env);
		pg_elog(WARNING, "Failed to get %s from %s",
				row_key_equals_creator_method_name,
				filter_creator_class_name);
		goto error_exit;
	}

	creator = (*env)->NewObject(
		env,
		filter_creator_class,
		constructor_method);

	if (creator == NULL)
	{
		log_exception(env);
		pg_elog(WARNING, "Failed to create %s object",
				filter_creator_class_name);
		goto error_exit;
	}

	for (int i = 0; i < nr_filters; i++)
	{
		HBaseFilter *filter = &filters[i];
		switch (filter->filter_type)
		{
			case filter_type_row_key_equals:
			{
				jobject row_key = make_byte_array(env,
												  filter->row_key_equals.row_key,
												  strlen(filter->row_key_equals.row_key));
				if(row_key == NULL)
				{
					log_exception(env);
					pg_elog(WARNING, "Failed to create row key byte array");
					goto error_exit;
				}

				(*env)->CallVoidMethod(
					env,
					creator,
					row_key_equals_creator,
					row_key);

				(*env)->DeleteLocalRef(env, row_key);
				if ((*env)->ExceptionCheck(env))
				{
					log_exception(env);
					pg_elog(WARNING, "Failed to create row_key_equals filter");
					goto error_exit;
				}
				break;
			}
			default:
				continue;
		}
	}

	goto exit;

 error_exit:
	(*env)->DeleteLocalRef(env, creator);
	creator = NULL;

 exit:
	(*env)->DeleteLocalRef(env, filter_creator_class);
	return creator;
}

ScannerData
setup_scanner(void *env_, char *table,
			  HBaseColumn *c_columns, int nr_columns,
			  HBaseFilter *filters, int nr_filters)
{
	char *make_scanner_method_name = "makeScanner";
	char *make_scanner_method_signature =
		"([B[Lorg/bifrost/PgHbaseColumn;Lorg/bifrost/HBaseFilterCreator;)Lorg/bifrost/Scanner;";
	char *scan_method_name = "scan";
	char *scan_method_signature = "()[B";

	JNIEnv *env = env_;
	jobject columns = NULL;
	jbyteArray table_name = NULL;
	jclass hbase_connector_class = NULL;
	jmethodID make_scanner = NULL;
	jobject local_scanner_ref = NULL;
	jobject global_scanner_ref = NULL;
	jclass scanner_class = NULL;
	jmethodID scan_method = NULL;
	ScannerData res = { NULL, NULL, NULL, NULL };
	jobject filter_obj = NULL;

	filter_obj = create_filters(env, filters, nr_filters);
	if (filter_obj== NULL)
	{
		log_exception(env);
		pg_elog(WARNING, "Failed to create filters");
		goto exit;
	}

	columns = create_pg_hbase_columns(env_, c_columns, nr_columns);
	if (columns == NULL)
	{
		log_exception(env);
		pg_elog(WARNING, "Failed to create columns");
		goto exit;
	}

	hbase_connector_class = (*env)->GetObjectClass(env, hbase_connector);
	if (hbase_connector_class == NULL)
	{
		log_exception(env);
		pg_elog(WARNING, "Failed get hbase_connector class");
		goto exit;
	}

	make_scanner = (*env)->GetMethodID(env, hbase_connector_class,
									   make_scanner_method_name,
									   make_scanner_method_signature);
	if (make_scanner == NULL)
	{
		log_exception(env);
		pg_elog(WARNING, "Failed to get make scanner method");
		goto exit;
	}

	table_name = make_byte_array(env, table, strlen(table));
	if (table_name == NULL)
	{
		log_exception(env);
		pg_elog(WARNING, "Failed to make table name byte array");
		goto exit;
	}

	local_scanner_ref = (*env)->CallObjectMethod(
		env,
		hbase_connector,
		make_scanner,
		table_name,
		columns,
		filter_obj
		);
	if (local_scanner_ref == NULL || (*env)->ExceptionCheck(env))
	{
		log_exception(env);
		pg_elog(WARNING, "Failed to create scanner");
		goto exit;
	}

	global_scanner_ref = (*env)->NewGlobalRef(env, local_scanner_ref);
	if (global_scanner_ref == NULL)
	{
		log_exception(env);
		pg_elog(WARNING, "Failed to create global scanner ref");
		goto exit;
	}

	scanner_class = (*env)->GetObjectClass(env, global_scanner_ref);
	if (scanner_class == NULL)
	{
		log_exception(env);
		pg_elog(WARNING, "Failed to create global scanner ref");
		goto exit;
	}

	scan_method = (*env)->GetMethodID(env,
									  scanner_class,
									  scan_method_name,
									  scan_method_signature);
	if (scan_method == NULL)
	{
		log_exception(env);
		pg_elog(WARNING, "Failed to create global scanner ref");
		goto exit;
	}

	res.scan = scan_method;
	res.scanner = global_scanner_ref;


 exit:
	(*env)->DeleteLocalRef(env, scanner_class);
	(*env)->DeleteLocalRef(env, local_scanner_ref);
	(*env)->DeleteLocalRef(env, table_name);
	(*env)->DeleteLocalRef(env, hbase_connector_class);
	(*env)->DeleteLocalRef(env, columns);
	(*env)->DeleteLocalRef(env, filter_obj);

	if (res.scanner == NULL)
		(*env)->DeleteGlobalRef(env, global_scanner_ref);

	return res;
}

bool
scan_row(void *env_, ScannerData *data)
{
	JNIEnv *env = env_;
	jobject byte_array;
	release_scanner_bytes(env, data);
	byte_array = (*env)->CallObjectMethod(
		env,
		data->scanner,
		data->scan);
	if ((*env)->ExceptionCheck(env))
	{
		pg_elog(WARNING, "Failed to do scan.");
		log_exception(env);
		return false;
	}
	if (byte_array == NULL)
		return false;

	data->byte_array = byte_array;
	data->ptr = (char*)(*env)->GetByteArrayElements(env, byte_array, 0);
	if (data->ptr == NULL)
	{
		pg_elog(WARNING, "Failed to get scanner data");
		log_exception(env);
		release_scanner_bytes(env, data);
		return false;
	}
	return true;
}

static void
release_scanner_bytes(void *env_, ScannerData *scanner_data)
{
	JNIEnv *env = env_;
	if (scanner_data->ptr != NULL) {
		(*env)->ReleaseByteArrayElements(
			env,
			scanner_data->byte_array,
			(signed char*)scanner_data->ptr,
			0);
		scanner_data->ptr = NULL;
	}
	(*env)->DeleteLocalRef(env, scanner_data->byte_array);
	scanner_data->byte_array = NULL;
}

void
destroy_scanner(void *env_, ScannerData *scanner_data)
{
	JNIEnv *env = env_;
	(*env)->DeleteGlobalRef(env, scanner_data->scanner);
	scanner_data->scanner = NULL;
	scanner_data->scan = NULL;
}

void
free_local_jvm_obj(void *env_, void *object)
{
	JNIEnv *env = env_;
	if (object != NULL)
		(*env)->DeleteLocalRef(env, object);
}
