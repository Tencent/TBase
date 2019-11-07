-- #57
-- When distribution column is changed for a table, we don't check for dependecies such as UNIQUE indexes
CREATE TABLE xl_atm (
    product_no INT8,
    product_id INT2,
    primary key (product_id)
) DISTRIBUTE BY HASH (product_no);
CREATE TABLE xl_atm (
    product_no INT8,
    product_id INT2,
    primary key (product_id)
) DISTRIBUTE BY HASH (product_id);
\d+ xl_atm;
INSERT into xl_atm VALUES (11,1);
INSERT into xl_atm VALUES (12,2);
INSERT into xl_atm VALUES (13,3);
INSERT into xl_atm VALUES (14,4);
INSERT into xl_atm VALUES (11,5);--repeate a value in non-distribution column
ALTER TABLE xl_atm DISTRIBUTE BY HASH (product_no);
\d+ xl_atm;
DROP TABLE xl_atm;
-- #27
-- Distribution column can be dropped for MODULO distribution tables.
CREATE TABLE xl_at2m (
    product_no INT8,
    product_id INT2
) DISTRIBUTE BY MODULO (product_id);
ALTER TABLE xl_at2m DROP COLUMN product_id;
DROP TABLE xl_at2m;
-- #6
-- REFRESH MATERIALISED VIEW CONCURRENTLY gives error
CREATE TABLE t (a int, b int);
CREATE MATERIALIZED VIEW mvt AS SELECT * FROM t;
CREATE UNIQUE INDEX mvidx ON mvt(a);
REFRESH MATERIALIZED VIEW mvt;
REFRESH MATERIALIZED VIEW CONCURRENTLY mvt;
DROP MATERIALIZED VIEW mvt;
DROP TABLE t;
-- #74
-- SQL error codes are not correctly sent back to the client when dealing with error on COPY protocol
\set VERBOSITY verbose
create table copytbl(a integer, b text default 'a_copytbl', primary key (a));
insert into copytbl select generate_series(1,200);
COPY copytbl (a, b) from stdin;
10000	789
10001	789
1	789
10002	789
10003	789
\.
drop table copytbl;
\set VERBOSITY default
-- #13
-- INSERT query with SELECT part using joins on OID fails to insert all rows correctly
create table tmp_films(a int, b text default 'a_tmp_film') with oids;
create table films(a int, b text default 'a_film') with oids;
insert into tmp_films select generate_series(1, 10000);-- 10K entries
select count(*) from tmp_films;
insert into films select * from tmp_films where oid >= (select oid from tmp_films order by oid limit 1);
select count(*) from films;
-- #9
-- Fails to see DDL's effect inside a function
create function xl_getint() returns integer as $$
declare
	i integer;
BEGIN
	create table inttest(a int, b int);
	insert into inttest values (1,1);
	select a into i from inttest limit 1;
	RETURN i;
END;$$ language plpgsql;

select xl_getint();
select * from inttest;

create function xl_cleanup() returns integer as $$
declare
	i integer;
BEGIN
	drop function xl_getint();
	drop table inttest;
	select a into i from inttest limit 1;
	RETURN i;
END;$$ language plpgsql;

select xl_cleanup();

drop function xl_cleanup();

-- #4
-- Tableoid to relation name mapping broken
create table cities (
	name		text,
	population	float8,
	altitude	int		-- (in ft)
);

create table capitals (
	state		char(2)
) inherits (cities);

-- Create unique indexes.  Due to a general limitation of inheritance,
-- uniqueness is only enforced per-relation.  Unique index inference
-- specification will do the right thing, though.
create unique index cities_names_unique on cities (name);
create unique index capitals_names_unique on capitals (name);

-- prepopulate the tables.
insert into cities values ('San Francisco', 7.24E+5, 63);
insert into cities values ('Las Vegas', 2.583E+5, 2174);
insert into cities values ('Mariposa', 1200, 1953);

insert into capitals values ('Sacramento', 3.694E+5, 30, 'CA');
insert into capitals values ('Madison', 1.913E+5, 845, 'WI');

-- Tests proper for inheritance:
select * from capitals;

-- Succeeds:
insert into cities values ('Las Vegas', 2.583E+5, 2174) on conflict do nothing;
insert into capitals values ('Sacramento', 4664.E+5, 30, 'CA') on conflict (name) do update set population = excluded.population;
-- Wrong "Sacramento", so do nothing:
insert into capitals values ('Sacramento', 50, 2267, 'NE') on conflict (name) do nothing;
select * from capitals;
insert into cities values ('Las Vegas', 5.83E+5, 2001) on conflict (name) do update set population = excluded.population, altitude = excluded.altitude;
select tableoid::regclass, * from cities;
insert into capitals values ('Las Vegas', 5.83E+5, 2222, 'NV') on conflict (name) do update set population = excluded.population;
-- Capitals will contain new capital, Las Vegas:
select * from capitals;
-- Cities contains two instances of "Las Vegas", since unique constraints don't
-- work across inheritance:
select tableoid::regclass, * from cities;
-- This only affects "cities" version of "Las Vegas":
insert into cities values ('Las Vegas', 5.86E+5, 2223) on conflict (name) do update set population = excluded.population, altitude = excluded.altitude;
select tableoid::regclass, * from cities;

-- clean up
drop table capitals;
drop table cities;

-- #16
-- Windowing function throws an error when subquery has ORDER BY clause
CREATE TABLE test (a int, b int);
EXPLAIN SELECT last_value(a) OVER (PARTITION by b) FROM (SELECT * FROM test) AS s ORDER BY a;
EXPLAIN SELECT last_value(a) OVER (PARTITION by b) FROM (SELECT * FROM test  ORDER BY a) AS s ORDER BY a;
DROP TABLE test;

-- #5
-- Type corresponding to a view does not exist on datanode
CREATE TABLE test (a int, b int);
CREATE VIEW v AS SELECT * FROM test;

-- Using view type throws an error
CREATE FUNCTION testf (x v) RETURNS INTEGER AS $$ SELECT 1; $$ LANGUAGE SQL;

-- Same works for table type though
CREATE FUNCTION testf (x test) RETURNS INTEGER AS $$ SELECT 1; $$ LANGUAGE SQL;

DROP FUNCTION testf (x test);
DROP VIEW v;
DROP TABLE test;


-- #7
-- "cache lookup failed" error
CREATE TABLE test (a int, b int);
CREATE VIEW v AS SELECT * FROM test;
SELECT v FROM v;
DROP VIEW v;
DROP TABLE test;
