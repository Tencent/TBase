/* contrib/tbase_pooler_stat/tbase_pooler_stat--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION tbase_pooler_stat" to load this file. \quit

-- Register functions.
CREATE OR REPLACE FUNCTION tbase_get_pooler_cmd_statistics(
	OUT command_type text,
	OUT request_times int8,
	OUT avg_costtime int8,
	OUT max_costtime int8,
	OUT min_costtime int8
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C;


CREATE OR REPLACE FUNCTION tbase_reset_pooler_cmd_statistics()
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION tbase_get_pooler_conn_statistics(
	OUT database name,
	OUT user_name name,
	OUT node_name name,
	OUT oid Oid,
	OUT is_coord bool,
	OUT conn_cnt int4,
	OUT free_cnt int4,
	OUT warming_cnt int4,
	OUT query_cnt int4,
	OUT exceed_keepalive_cnt int4,
	OUT exceed_deadtime_cnt int4,
	OUT exceed_maxlifetime_cnt int4
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C;

