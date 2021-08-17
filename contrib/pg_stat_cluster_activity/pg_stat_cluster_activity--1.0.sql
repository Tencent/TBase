/* contrib/pg_stat_cluster_activity/pg_stat_cluster_activity--1.0.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "CREATE EXTENSION pg_stat_cluster_activity" to load this file. \quit

/* Now redefine */
CREATE OR REPLACE FUNCTION pg_stat_get_cluster_activity(
    sessionid text,
    coordonly bool,
    localonly bool,
    OUT sessionid text,
    OUT pid integer,
    OUT client_addr inet,
    OUT client_hostname text,
    OUT client_port integer,
    OUT nodename text,
    OUT role text,
    OUT datname text,
    OUT usename text,
    OUT wait_event_type text,
    OUT wait_event text,
    OUT state text,
    OUT sqname text,
    OUT sqdone bool,
    OUT query text,
    OUT planstate text,
    OUT portal text,
    OUT cursors text,
    OUT backend_start timestamp with time zone,
    OUT xact_start timestamp with time zone,
    OUT query_start timestamp with time zone,
    OUT state_change timestamp with time zone
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION pg_signal_session(text, integer, bool)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION pg_terminate_session(text)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION pg_cancel_session(text)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE VIEW pg_stat_cluster_activity AS
  SELECT * FROM pg_stat_get_cluster_activity(NULL, false, false);

CREATE OR REPLACE VIEW pg_stat_cluster_activity_cn AS
  SELECT * FROM pg_stat_get_cluster_activity(NULL, true, false);

GRANT SELECT ON pg_stat_cluster_activity TO PUBLIC;
GRANT SELECT ON pg_stat_cluster_activity_cn TO PUBLIC;
