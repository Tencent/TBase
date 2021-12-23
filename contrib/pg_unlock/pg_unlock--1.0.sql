/* contrib/pg_unlock/pg_unlock--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_unlock" to load this file. \quit

-- Register functions.
CREATE FUNCTION pg_unlock_execute(
	OUT executetime int8,
	OUT txnindex int8,
	OUT rollbacktxnifo text,
	OUT nodename text,
	OUT cancel_query text
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE FUNCTION pg_unlock_check_deadlock(
	OUT deadlockid int8,
	OUT	deadlocks text,
	OUT nodename text,
	OUT query text
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE FUNCTION pg_unlock_check_dependency(
	OUT dependencyid int8,
	OUT	dependency text,
	OUT nodename text,
	OUT query text
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE FUNCTION pg_unlock_killbypid(
	IN	nodename text, 
	IN	pid	int4
)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE FUNCTION pg_findgxid(IN txnid int8)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C;

GRANT ALL ON FUNCTION pg_unlock_execute() TO PUBLIC;
GRANT ALL ON FUNCTION pg_unlock_check_deadlock() TO PUBLIC;
 GRANT ALL ON FUNCTION pg_unlock_killbypid(nodename text, pid int4) TO PUBLIC;
GRANT ALL ON FUNCTION pg_unlock_check_dependency() TO PUBLIC;
GRANT ALL ON FUNCTION pg_findgxid(txnid int8) TO PUBLIC;

