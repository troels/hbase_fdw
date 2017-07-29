
-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION hbase_fdw" to load this file. \quit

CREATE FUNCTION hbase_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER hbase_fdw
  HANDLER hbase_fdw_handler;
