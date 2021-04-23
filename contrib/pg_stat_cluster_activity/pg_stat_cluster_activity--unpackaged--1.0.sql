/* contrib/pg_stat_cluster_activity/pg_stat_cluster_activity--unpackaged--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_stat_cluster_activity" to load this file. \quit

ALTER EXTENSION pg_stat_cluster_activity ADD function pg_stat_cluster_get_activity();
ALTER EXTENSION pg_stat_statements ADD view pg_stat_cluster_activity;
ALTER EXTENSION pg_stat_statements ADD view pg_stat_cluster_activity_cn;
