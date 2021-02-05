/* contrib/tbase_memory/tbase_memory_tools--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "create EXTENSION tbase_memory_tools" to load this file. \quit

--
-- pg_node_memory_detail()
--
CREATE FUNCTION pg_node_memory_detail(
    OUT nodename text,
    OUT pid int,
    OUT memorytype text,
    OUT memorykbytes int)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_node_memory_detail'
LANGUAGE C STRICT PARALLEL SAFE;

--
-- pg_session_memory_detail()
--
CREATE FUNCTION pg_session_memory_detail(
    OUT contextname text,
    OUT contextlevel int,
    OUT parent text,
    OUT totalsize int,
    OUT freesize int)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_session_memory_detail'
LANGUAGE C STRICT PARALLEL SAFE;