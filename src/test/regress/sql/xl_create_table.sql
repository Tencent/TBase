-- check that CTAS inserts rows into correct table
CREATE TABLE ctas_t1 AS SELECT 1;
CREATE TEMP TABLE ctas_t1 AS SELECT 2;
SELECT * FROM ctas_t1;
SELECT * FROM public.ctas_t1;
SELECT * FROM pg_temp.ctas_t1;
-- this drops the temp table
DROP TABLE ctas_t1;
-- also drop the public table
DROP TABLE ctas_t1;

-- same tests with special table name
CREATE TABLE "ctas t1" AS SELECT 1;
CREATE TEMP TABLE "ctas t1" AS SELECT 2;
SELECT * FROM "ctas t1";
SELECT * FROM public."ctas t1";
SELECT * FROM pg_temp."ctas t1";
DROP TABLE "ctas t1";
DROP TABLE "ctas t1";

CREATE SCHEMA "ctas schema";
SET search_path TO "ctas schema";
CREATE TABLE "ctas t1" AS SELECT 1;
CREATE TEMP TABLE "ctas t1" AS SELECT 2;
SELECT * FROM "ctas t1";
SELECT * FROM public."ctas t1";
SELECT * FROM "ctas schema"."ctas t1";
SELECT * FROM pg_temp."ctas t1";
DROP TABLE "ctas t1";
DROP TABLE "ctas t1";
