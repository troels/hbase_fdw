JVM_CPPFLAGS = -I/usr/lib/jvm/java-8-jdk/include -I/usr/lib/jvm/java-8-jdk/include/linux

MODULE_big = hbase_fdw
EXTENSION = hbase_fdw
OBJS =  hbase_fdw.o jvm.o fdw_driver.o process_communication.o worker.o

PG_CPPFLAGS =  ${JVM_CPPFLAGS} -lpthread -Wall -Wextra -Werror -Wno-unused -g
SHLINK_LINK =  -lpthread

DATA = hbase_fdw--1.0.sql
PGFILEDESC = "hbase_fdw - foreign data wrapper for PostgreSQL"

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
