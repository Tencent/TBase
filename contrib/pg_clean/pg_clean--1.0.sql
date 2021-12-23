/* contrib/pg_clean/pg_clean--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_clean" to load this file. \quit

-- Register functions.
CREATE FUNCTION pg_clean_execute(IN time_interval integer DEFAULT 120,
	OUT gid text,
	OUT global_transaction_status text,
	OUT operation text,
	OUT operation_status text
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION pg_clean_execute_on_node(IN abnormal_nodename text, IN abnormal_time bigint,
	OUT gid text,
	OUT global_transaction_status text,
	OUT operation text,
	OUT operation_status text
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;


CREATE FUNCTION pg_clean_check_txn(IN time_interval integer DEFAULT 120,
	OUT gid text,
	OUT database text,
	OUT global_transaction_status text,
	OUT transaction_status_on_allnodes text
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION pgxc_get_2pc_nodes(gid text)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE FUNCTION pgxc_get_2pc_startnode(gid text)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE FUNCTION pgxc_get_2pc_startxid(gid text)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE FUNCTION pgxc_get_2pc_commit_timestamp(gid text)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE FUNCTION pgxc_get_2pc_xid(gid text)
RETURNS integer
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE FUNCTION pgxc_get_2pc_file(gid text)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE FUNCTION pgxc_remove_2pc_records(gid text)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE FUNCTION pgxc_clear_2pc_records()
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE FUNCTION pgxc_get_record_list()
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE FUNCTION pgxc_commit_on_node(nodename text, gid text)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE FUNCTION pgxc_abort_on_node(nodename text, gid text)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C;

GRANT ALL ON FUNCTION pg_clean_execute(time_interval integer) TO PUBLIC;
GRANT ALL ON FUNCTION pg_clean_execute_on_node(abnormal_nodename text, abnormal_time bigint) TO PUBLIC;
GRANT ALL ON FUNCTION pg_clean_check_txn(time_interval integer) TO PUBLIC;
GRANT ALL ON FUNCTION pgxc_get_2pc_nodes(gid text) TO PUBLIC;
GRANT ALL ON FUNCTION pgxc_get_2pc_startnode(gid text) TO PUBLIC;
GRANT ALL ON FUNCTION pgxc_get_2pc_startxid(gid text) TO PUBLIC;
GRANT ALL ON FUNCTION pgxc_get_2pc_commit_timestamp(gid text) TO PUBLIC;
GRANT ALL ON FUNCTION pgxc_get_2pc_xid(gid text) TO PUBLIC;
GRANT ALL ON FUNCTION pgxc_get_2pc_file(gid text) TO PUBLIC;
GRANT ALL ON FUNCTION pgxc_remove_2pc_records(gid text) TO PUBLIC;
GRANT ALL ON FUNCTION pgxc_clear_2pc_records() TO PUBLIC;
GRANT ALL ON FUNCTION pgxc_get_record_list() TO PUBLIC;
GRANT ALL ON FUNCTION pgxc_commit_on_node(nodename text, gid text) TO PUBLIC;
GRANT ALL ON FUNCTION pgxc_abort_on_node(nodename text, gid text) TO PUBLIC;
