/* contrib/tbase_pooler_stat/tbase_pooler_stat--unpackaged--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION tbase_pooler_stat FROM unpackaged" to load this file. \quit

ALTER EXTENSION tbase_pooler_stat ADD function tbase_get_pooler_cmd_statistics();
ALTER EXTENSION tbase_pooler_stat ADD function tbase_reset_pooler_cmd_statistics();
ALTER EXTENSION tbase_pooler_stat ADD function tbase_get_pooler_conn_statistics();