-- pg_regress should ensure that this default value applies; however
-- we can't rely on any specific default value of vacuum_cost_delay
SHOW datestyle;

-- SET to some nondefault value
SET vacuum_cost_delay TO 40;
SET datestyle = 'ISO, YMD';
SHOW vacuum_cost_delay;
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;

-- SET LOCAL has no effect outside of a transaction
SET LOCAL vacuum_cost_delay TO 50;
SHOW vacuum_cost_delay;
SET LOCAL datestyle = 'SQL';
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;

-- SET LOCAL within a transaction that commits
BEGIN;
SET LOCAL vacuum_cost_delay TO 50;
SHOW vacuum_cost_delay;
SET LOCAL datestyle = 'SQL';
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;
COMMIT;
SHOW vacuum_cost_delay;
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;

-- SET should be reverted after ROLLBACK
BEGIN;
SET vacuum_cost_delay TO 60;
SHOW vacuum_cost_delay;
SET datestyle = 'German';
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;
ROLLBACK;
SHOW vacuum_cost_delay;
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;

-- SET followed by SET LOCAL
BEGIN;
SET vacuum_cost_delay TO 40;
SET LOCAL vacuum_cost_delay TO 50;
SHOW vacuum_cost_delay;
SET datestyle = 'ISO, DMY';
SET LOCAL datestyle = 'Postgres, MDY';
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;
COMMIT;
SHOW vacuum_cost_delay;
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;

--
-- Test RESET.  We use datestyle because the reset value is forced by
-- pg_regress, so it doesn't depend on the installation's configuration.
--
SET datestyle = iso, ymd;
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;
RESET datestyle;
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;

--
-- Test DISCARD TEMP
--
CREATE TEMP TABLE reset_test ( data text ) ON COMMIT DELETE ROWS;
SELECT relname FROM pg_class WHERE relname = 'reset_test';
DISCARD TEMP;
SELECT relname FROM pg_class WHERE relname = 'reset_test';

--
-- Test DISCARD ALL
--

-- do changes
DECLARE foo CURSOR WITH HOLD FOR SELECT 1;
PREPARE foo AS SELECT 1;
LISTEN foo_event;
SET vacuum_cost_delay = 13;
CREATE TEMP TABLE tmp_foo (data text) ON COMMIT DELETE ROWS;
CREATE ROLE regress_guc_user;
SET SESSION AUTHORIZATION regress_guc_user;
-- look changes
SELECT pg_listening_channels();
SELECT name FROM pg_prepared_statements;
SELECT name FROM pg_cursors;
SHOW vacuum_cost_delay;
SELECT relname from pg_class where relname = 'tmp_foo';
SELECT current_user = 'regress_guc_user';
-- discard everything
DISCARD ALL;
-- look again
SELECT pg_listening_channels();
SELECT name FROM pg_prepared_statements;
SELECT name FROM pg_cursors;
SHOW vacuum_cost_delay;
SELECT relname from pg_class where relname = 'tmp_foo';
SELECT current_user = 'regress_guc_user';
DROP ROLE regress_guc_user;

--
-- search_path should react to changes in pg_namespace
--

set search_path = foo, public, not_there_initially;
select current_schemas(false);
create schema not_there_initially;
select current_schemas(false);
drop schema not_there_initially;
select current_schemas(false);
reset search_path;

--
-- Tests for function-local GUC settings
--

set work_mem = '3MB';

create function report_guc(text) returns text as
$$ select current_setting($1) $$ language sql
set work_mem = '1MB';

select report_guc('work_mem'), current_setting('work_mem');

alter function report_guc(text) set work_mem = '2MB';

select report_guc('work_mem'), current_setting('work_mem');

alter function report_guc(text) reset all;

select report_guc('work_mem'), current_setting('work_mem');

-- SET LOCAL is restricted by a function SET option
create or replace function myfunc(int) returns text as $$
begin
  set local work_mem = '2MB';
  return current_setting('work_mem');
end $$
language plpgsql
set work_mem = '1MB';

select myfunc(0), current_setting('work_mem');

alter function myfunc(int) reset all;

select myfunc(0), current_setting('work_mem');

set work_mem = '3MB';

-- but SET isn't
create or replace function myfunc(int) returns text as $$
begin
  set work_mem = '2MB';
  return current_setting('work_mem');
end $$
language plpgsql
set work_mem = '1MB';

select myfunc(0), current_setting('work_mem');

set work_mem = '3MB';

-- it should roll back on error, though
create or replace function myfunc(int) returns text as $$
begin
  set work_mem = '2MB';
  perform 1/$1;
  return current_setting('work_mem');
end $$
language plpgsql
set work_mem = '1MB';

select myfunc(0);
select current_setting('work_mem');
select myfunc(1), current_setting('work_mem');

-- check current_setting()'s behavior with invalid setting name

select current_setting('nosuch.setting');  -- FAIL
select current_setting('nosuch.setting', false);  -- FAIL
select current_setting('nosuch.setting', true) is null;

-- after this, all three cases should yield 'nada'
set nosuch.setting = 'nada';

select current_setting('nosuch.setting');
select current_setting('nosuch.setting', false);
select current_setting('nosuch.setting', true);

-- Normally, CREATE FUNCTION should complain about invalid values in
-- function SET options; but not if check_function_bodies is off,
-- because that creates ordering hazards for pg_dump

create function func_with_bad_set() returns int as $$ select 1 $$
language sql
set default_text_search_config = no_such_config;

set check_function_bodies = off;

create function func_with_bad_set() returns int as $$ select 1 $$
language sql
set default_text_search_config = no_such_config;

select func_with_bad_set();

reset check_function_bodies;

SET application_name TO "special name";
CREATE TABLE testtab (a int);
INSERT INTO testtab VALUES (1), (2), (3);
SELECT * FROM testtab order by 1;
DROP TABLE testtab;

SET default_transaction_isolation TO "read committed";
CREATE TABLE testtab (a int);
INSERT INTO testtab VALUES (1), (2), (3);
SELECT * FROM testtab order by 1;
DROP TABLE testtab;

SET work_mem TO '64kB';
CREATE TABLE testtab (a int);
INSERT INTO testtab VALUES (1), (2), (3);
SELECT * FROM testtab order by 1;
DROP TABLE testtab;

SET work_mem TO "64kB";
CREATE TABLE testtab (a int);
INSERT INTO testtab VALUES (1), (2), (3);
SELECT * FROM testtab order by 1;
DROP TABLE testtab;

SET log_min_duration_statement = '1s';
CREATE TABLE testtab (a int);
INSERT INTO testtab VALUES (1), (2), (3);
SELECT * FROM testtab order by 1;
DROP TABLE testtab;

CREATE SCHEMA testschema;
CREATE SCHEMA "testschema 2";
CREATE SCHEMA "testschema 3";
CREATE SCHEMA READ;
CREATE SCHEMA "READ";

-- ERROR
CREATE SCHEMA SELECT;

-- Ok
CREATE SCHEMA "SELECT";

SET search_path TO testschema;
CREATE TABLE testtab (a int);
\d+ testtab
INSERT INTO testtab VALUES (1), (2), (3);
SELECT * FROM testtab order by 1;
DROP TABLE testtab;

SET search_path TO "testschema";
CREATE TABLE testtab (a int);
\d+ testtab
INSERT INTO testtab VALUES (1), (2), (3);
SELECT * FROM testtab order by 1;
DROP TABLE testtab;

SET search_path TO testschema, "testschema 2";
CREATE TABLE testtab (a int);
\d+ testtab
INSERT INTO testtab VALUES (1), (2), (3);
SELECT * FROM testtab order by 1;
DROP TABLE testtab;

SET search_path TO "testschema 3", "testschema 2";
CREATE TABLE testtab (a int);
\d+ testtab
INSERT INTO testtab VALUES (1), (2), (3);
SELECT * FROM testtab order by 1;
DROP TABLE testtab;

-- ERROR
SET search_path TO "testschema 3", SELECT;

SET search_path TO "SELECT", "testschema 3";
CREATE TABLE testtab (a int);
\d+ testtab
CREATE TABLE "testschema 3".testtab (a int);
\d+ testtab
INSERT INTO "testschema 3".testtab VALUES (1), (2), (3);
INSERT INTO "SELECT".testtab VALUES (4);
SELECT * FROM "testschema 3".testtab order by 1;
INSERT INTO testtab SELECT * FROM "testschema 3".testtab;
SELECT * FROM "testschema 3".testtab order by 1;
\d+ testtab
SELECT * FROM testtab order by 1;
INSERT INTO testtab SELECT * FROM testtab;
SELECT * FROM testtab order by 1;
DROP TABLE testtab;

DROP SCHEMA testschema CASCADE;
DROP SCHEMA "testschema 2" CASCADE;
DROP SCHEMA "testschema 3" CASCADE;
DROP SCHEMA "READ" CASCADE;
DROP SCHEMA SELECT CASCADE;
DROP SCHEMA "SELECT" CASCADE;
