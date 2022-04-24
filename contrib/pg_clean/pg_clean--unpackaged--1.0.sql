/* contrib/pg_clean/pg_clean--unpackaged--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_clean" to load this file. \quit

ALTER EXTENSION pg_clean ADD function pg_clean_execute(time_interval integer);
ALTER EXTENSION pg_clean ADD function pg_clean_execute_on_node(abnormal_nodename text, abnormal_time bigint);
ALTER EXTENSION pg_clean ADD function pg_clean_check_txn(time_interval integer);
ALTER EXTENSION pg_clean ADD function pgxc_get_2pc_nodes(gid text);
ALTER EXTENSION pg_clean ADD function pgxc_get_2pc_startnode(gid text);
ALTER EXTENSION pg_clean ADD function pgxc_get_2pc_startxid(gid text);
ALTER EXTENSION pg_clean ADD function pgxc_get_2pc_commit_timestamp(gid text);
ALTER EXTENSION pg_clean ADD function pgxc_get_2pc_xid(gid text);
ALTER EXTENSION pg_clean ADD function pgxc_get_2pc_file(gid text);
ALTER EXTENSION pg_clean ADD function pgxc_remove_2pc_records(gid text);
ALTER EXTENSION pg_clean ADD function pgxc_clear_2pc_records();
ALTER EXTENSION pg_clean ADD function pgxc_get_record_list();
ALTER EXTENSION pg_clean ADD function pgxc_commit_on_node(nodename text, gid text);
ALTER EXTENSION pg_clean ADD function pgxc_abort_on_node(nodename text, gid text);
