\set ECHO all
SET client_min_messages = warning;
SET datestyle TO ISO;
SET client_encoding = utf8;

-- test guc enable_oracle_compatible
select name, setting, context, vartype, source, boot_val
    from pg_settings 
    where name = 'enable_oracle_compatible';

-- test pg_oracle namespace
select oid, nspname, nspowner 
    from pg_namespace 
    where oid = 5500;

set enable_oracle_compatible = on;
set search_path = foo, public, not_there_initially;
select current_schemas(true);
create schema not_there_initially;
select current_schemas(true);
drop schema not_there_initially;
select current_schemas(true);
reset search_path;
reset enable_oracle_compatible;

select current_schemas(true);

set enable_oracle_compatible = off;
set search_path = foo, public, not_there_initially;
select current_schemas(true);
create schema not_there_initially;
select current_schemas(true);
drop schema not_there_initially;
select current_schemas(true);
reset search_path;
reset enable_oracle_compatible;

select current_schemas(true);

-- test Alias
select * from (VALUES(1,2,3)) as v0;
select * from (VALUES(1,2,3));

drop table if exists test1;
create table test1(tc1 int2, tc2 int4, tc3 int8, tc4 float4, tc5 float8);
drop table if exists test2;
create table test2(tc1 int2, tc2 int4, tc3 int8, tc4 float4, tc5 float8);

insert into test1 values(1,2,3,4,5);
insert into test1 values(6,7,8,9,0);

insert into test2 values(1,2,3,4,5);
insert into test2 values(6,7,8,9,0);

select * 
    from (select * from test1 order by tc1, tc2, tc3, tc4, tc5),
         (select * from test2 order by tc5, tc4, tc3, tc2, tc1);

drop table test1;
drop table test2;

-- test number/binary_float/binary_double
drop table if exists test1;
create table test1(tc1 binary_float, tc2 binary_double, tc3 number, tc4 float4, tc5 float8, tc6 numeric);
create index idx_test1_tc1 on test1(tc1);
create index idx_test1_tc2 on test1(tc2);
create index idx_test1_tc3 on test1(tc3);
create index idx_test1_tc4 on test1(tc4);
create index idx_test1_tc5 on test1(tc5);
create index idx_test1_tc6 on test1(tc6);

\d+ test1

insert into test1 values(10.0123456789, 11.0123456789, 12.0123456789, 13.0123456789, 14.0123456789, 15.0123456789);
insert into test1 values(20.0123456789, 22.0123456789, 22.0123456789, 23.0123456789, 24.0123456789, 25.0123456789);
insert into test1 values(30.0123456789, 33.0123456789, 32.0123456789, 33.0123456789, 34.0123456789, 35.0123456789);
insert into test1 values(40.0123456789, 44.0123456789, 42.0123456789, 43.0123456789, 44.0123456789, 45.0123456789);
insert into test1 values(50.0123456789, 55.0123456789, 52.0123456789, 53.0123456789, 54.0123456789, 55.0123456789);
insert into test1 values(60.0123456789, 66.0123456789, 62.0123456789, 63.0123456789, 64.0123456789, 65.0123456789);
insert into test1 values(70.0123456789, 77.0123456789, 72.0123456789, 73.0123456789, 74.0123456789, 75.0123456789);
insert into test1 values(80.0123456789, 88.0123456789, 82.0123456789, 83.0123456789, 84.0123456789, 85.0123456789);
insert into test1 values(90.0123456789, 99.0123456789, 92.0123456789, 93.0123456789, 94.0123456789, 95.0123456789);

-- explain select * from test1 where tc1 > 20.0123456789 and tc1 < 70.0123456789 order by tc1;
-- explain select * from test1 where tc2 > 20.0123456789 and tc2 < 70.0123456789 order by tc2;
-- explain select * from test1 where tc3 > 20.0123456789 and tc3 < 70.0123456789 order by tc3;
-- explain select * from test1 where tc4 > 20.0123456789 and tc4 < 70.0123456789 order by tc4;
-- explain select * from test1 where tc5 > 20.0123456789 and tc5 < 70.0123456789 order by tc5;
-- explain select * from test1 where tc6 > 20.0123456789 and tc6 < 70.0123456789 order by tc6;

select * from test1 where tc1 > 20.0123456789 and tc1 < 70.0123456789 order by tc1;
select * from test1 where tc2 > 20.0123456789 and tc2 < 70.0123456789 order by tc2;
select * from test1 where tc3 > 20.0123456789 and tc3 < 70.0123456789 order by tc3;
select * from test1 where tc4 > 20.0123456789 and tc4 < 70.0123456789 order by tc4;
select * from test1 where tc5 > 20.0123456789 and tc5 < 70.0123456789 order by tc5;
select * from test1 where tc6 > 20.0123456789 and tc6 < 70.0123456789 order by tc6;

drop table test1;

-- test sysdate
select sysdate - sysdate as mi;

-- test int4div
set enable_oracle_compatible = off;
select 1/2 as div;
select 1/2::int2 as div;
select 1/2::int8 as div;
set enable_oracle_compatible = on;
select 1/2 as div;
select 1/2::int2 as div;
select 1/2::int8 as div;

drop table if exists test;
create table test(tc1 int4, tc2 int4);
insert into test values(1,2);
insert into test values(1,3);
insert into test values(1,4);
insert into test values(2,2);
insert into test values(3,2);
insert into test values(4,2);
insert into test values(5,2);
insert into test values(null, null);
insert into test values(null, 1);
insert into test values(1, null);

\d+ test

set enable_oracle_compatible = on;
select tc1 / tc2 as div1,
        tc2 / tc1 as div2,
        (tc1 + tc2) / (tc1) as div3,
        (tc1::int2 + tc2::int2) / (tc1::int2) as div4, 
        tc1::int4 / tc2::int8 as div5,
        tc2::int2 / tc1::int2 as div6
    from test 
    order by div1, div2, div3, div4, div5, div6;

set enable_oracle_compatible = off;
select tc1 / tc2 as div1,
        tc2 / tc1 as div2,
        (tc1 + tc2) / (tc1) as div3,
        (tc1::int2 + tc2::int2) / (tc1::int2) as div4, 
        tc1::int4 / tc2::int8 as div5,
        tc2::int2 / tc1::int2 as div6
    from test 
    order by div1, div2, div3, div4, div5, div6;

drop table test;

-- test Oracle date
set enable_oracle_compatible = off;

drop table if exists test1;
create table test1(tc1 date, tc2 timestamp);

\d+ test1

insert into test1 values('2003-08-01 10:12:21.01234', '2003-08-01 10:12:21.09876');
insert into test1 values('2004-08-01 10:12:21.02234', '2004-08-01 10:12:21.09875');
insert into test1 values('2005-08-01 10:12:21.03234', '2005-08-01 10:12:21.09874');
insert into test1 values('2006-08-01 10:12:21.04234', '2006-08-01 10:12:21.09873');
insert into test1 values('2007-08-01 10:12:21.05234', '2007-08-01 10:12:21.09872');
insert into test1 values('2008-08-01 10:12:21.06234', '2008-08-01 10:12:21.09871');
insert into test1 values('2009-08-01 10:12:21.07234', '2009-08-01 10:12:21.09870');

insert into test1 select tc1 + interval '1 year' 
                             + interval '2 month'
                             + interval '3 week'
                             + interval '4 day'
                             + interval '5 hour'
                             + interval '6 minute'
                             + interval '7.09876 second' as tc1,
                         tc2 + interval '1 year'
                             + interval '2 month'
                             + interval '3 week'
                             + interval '4 day'
                             + interval '5 hour'
                             + interval '6 minute' 
                             + interval '7.09876 second' as tc2
                        from test1; 

select tc1, tc2 from test1 order by tc1, tc2;

set enable_oracle_compatible = on;

drop table if exists test2;
create table test2(tc1 date, tc2 timestamp);

\d+ test2

insert into test2 values('2003-08-01 10:12:21.01234', '2003-08-01 10:12:21.09876');
insert into test2 values('2004-08-01 10:12:21.02234', '2004-08-01 10:12:21.09875');
insert into test2 values('2005-08-01 10:12:21.03234', '2005-08-01 10:12:21.09874');
insert into test2 values('2006-08-01 10:12:21.04234', '2006-08-01 10:12:21.09873');
insert into test2 values('2007-08-01 10:12:21.05234', '2007-08-01 10:12:21.09872');
insert into test2 values('2008-08-01 10:12:21.06234', '2008-08-01 10:12:21.09871');
insert into test2 values('2009-08-01 10:12:21.07234', '2009-08-01 10:12:21.09870');

insert into test2 select tc1 + interval '1 year' 
                             + interval '2 month'
                             + interval '3 week'
                             + interval '4 day'
                             + interval '5 hour'
                             + interval '6 minute'
                             + interval '7.09876 second' as tc1,
                         tc2 + interval '1 year'
                             + interval '2 month'
                             + interval '3 week'
                             + interval '4 day'
                             + interval '5 hour'
                             + interval '6 minute' 
                             + interval '7.09876 second' as tc2
                        from test2; 

select tc1, tc2 from test2 order by tc1, tc2;

-- test timestamp_pl_day/timestamp_mi_day/timestamp_mi

set enable_oracle_compatible = off;

select tc1 - 1::int2::int4 as t1 from test1 order by t1;
select tc1 - 2::int4 as t2  from test1 order by t2;
select tc1 - 3::int8 as t3 from test1 order by t3;
select tc1 - 4::float4::int4 as t4 from test1 order by t4;
select tc1 - 5::float8::int4 as t5 from test1 order by t5;
select tc2 - 1::int2 as t6 from test1  order by t6;
select tc2 - 2::int4 as t7 from test1  order by t7;
select tc2 - 3::int8 as t8 from test1  order by t8;
select tc2 - 4::float4::numeric as t9 from test1  order by t9;
select tc2 - 5::float8::numeric as t10 from test1 order by t10;
select tc1 - tc2 as t11 from test1  order by t11; -- 
select tc2 - tc1 as t12 from test1  order by t12; --
select tc1 - tc1 as t13 from test1  order by t13;
select tc2 - tc2 as t14  from test1  order by t14;

select tc1 - 1::int2::int4 as t1,
       tc1 - 2::int4 as t2,  
       tc1 - 3::int8 as t3, 
       tc1 - 4::float4::int4 as t4, 
       tc1 - 5::float8::int4 as t5,
       tc2 - 1::int2 as t6,
       tc2 - 2::int4 as t7, 
       tc2 - 3::int8 as t8, 
       tc2 - 4::float4::numeric as t9,
       tc2 - 5::float8::numeric as t10,
       tc1 - tc2 as t11,
       tc2 - tc1 as t12,
       tc1 - tc1 as t13,
       tc2 - tc2 as t14
    from test1 
    order by t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14;

select tc1 + 1::int2::int4 as t1,
       tc1 + 2::int4 as t2,  
       tc1 + 3::int8 as t3, 
       tc1 + 4::float4::int4 as t4, 
       tc1 + 5::float8::int4 as t5,
       tc2 + 1::int2 as t6,
       tc2 + 2::int4 as t7, 
       tc2 + 3::int8 as t8, 
       tc2 + 4::float4::numeric as t9,
       tc2 + 5::float8::numeric as t10
    from test1 
    order by t1, t2, t3, t4, t5, t6, t7, t8, t9, t10;
    
select tc1 - 1::int2::int4 as t1,
       tc1 - 2::int4 as t2,  
       tc1 - 3::int8 as t3, 
       tc1 - 4::float4::int4 as t4, 
       tc1 - 5::float8::int4 as t5,
       tc2 - 1::int2 as t6,
       tc2 - 2::int4 as t7, 
       tc2 - 3::int8 as t8, 
       tc2 - 4::float4::numeric as t9,
       tc2 - 5::float8::numeric as t10,
       tc1 - tc2 as t11,
       tc2 - tc1 as t12,
       tc1 - tc1 as t13,
       tc2 - tc2 as t14
    from test2 
    order by t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14;

select tc1 + 1::int2::int4 as t1,
       tc1 + 2::int4 as t2,  
       tc1 + 3::int8 as t3, 
       tc1 + 4::float4::int4 as t4, 
       tc1 + 5::float8::int4 as t5,
       tc2 + 1::int2 as t6,
       tc2 + 2::int4 as t7, 
       tc2 + 3::int8 as t8, 
       tc2 + 4::float4::numeric as t9,
       tc2 + 5::float8::numeric as t10
    from test2 
    order by t1, t2, t3, t4, t5, t6, t7, t8, t9, t10;
    
select tc1 - tc2 as t1, tc2 - tc1 as t2 from test1 order by t1, t2;
select tc1 - tc2 as t1, tc2 - tc1 as t2 from test2 order by t1, t2;
select tc1, tc1 + 1/24/3600  t1 from test1 order by tc1, t1;
select tc1, tc1 + 1/24/3600  t1 from test2 order by tc1, t1;
select tc1, tc1 - 1/24/3600  t1 from test1 order by tc1, t1;
select tc1, tc1 - 1/24/3600  t1 from test2 order by tc1, t1;

set enable_oracle_compatible = on;

select tc1 - 1::int2::int4 as t1 from test1 order by t1;
select tc1 - 2::int4 as t2  from test1 order by t2;
select tc1 - 3::int8 as t3 from test1 order by t3;
select tc1 - 4::float4::int4 as t4 from test1 order by t4;
select tc1 - 5::float8::int4 as t5 from test1 order by t5;
select tc2 - 1::int2 as t6 from test1  order by t6;
select tc2 - 2::int4 as t7 from test1  order by t7;
select tc2 - 3::int8 as t8 from test1  order by t8;
select tc2 - 4::float4::numeric as t9 from test1  order by t9;
select tc2 - 5::float8::numeric as t10 from test1 order by t10;
select tc1 - tc2 as t11 from test1  order by t11; -- 
select tc2 - tc1 as t12 from test1  order by t12; --
select tc1 - tc1 as t13 from test1  order by t13;
select tc2 - tc2 as t14  from test1  order by t14;

select tc1 - 1::int2::int4 as t1,
       tc1 - 2::int4 as t2,  
       tc1 - 3::int8 as t3, 
       tc1 - 4::float4::int4 as t4, 
       tc1 - 5::float8::int4 as t5,
       tc2 - 1::int2 as t6,
       tc2 - 2::int4 as t7, 
       tc2 - 3::int8 as t8, 
       tc2 - 4::float4::numeric as t9,
       tc2 - 5::float8::numeric as t10,
       tc1 - tc2 as t11,
       tc2 - tc1 as t12,
       tc1 - tc1 as t13,
       tc2 - tc2 as t14
    from test1 
    order by t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14;

select tc1 + 1::int2::int4 as t1,
       tc1 + 2::int4 as t2,  
       tc1 + 3::int8 as t3, 
       tc1 + 4::float4::int4 as t4, 
       tc1 + 5::float8::int4 as t5,
       tc2 + 1::int2 as t6,
       tc2 + 2::int4 as t7, 
       tc2 + 3::int8 as t8, 
       tc2 + 4::float4::numeric as t9,
       tc2 + 5::float8::numeric as t10
    from test1 
    order by t1, t2, t3, t4, t5, t6, t7, t8, t9, t10;
    
select tc1 - 1::int2::int4 as t1,
       tc1 - 2::int4 as t2,  
       tc1 - 3::int8 as t3, 
       tc1 - 4::float4::int4 as t4, 
       tc1 - 5::float8::int4 as t5,
       tc2 - 1::int2 as t6,
       tc2 - 2::int4 as t7, 
       tc2 - 3::int8 as t8, 
       tc2 - 4::float4::numeric as t9,
       tc2 - 5::float8::numeric as t10,
       tc1 - tc2 as t11,
       tc2 - tc1 as t12,
       tc1 - tc1 as t13,
       tc2 - tc2 as t14
    from test2 
    order by t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14;

select tc1 + 1::int2::int4 as t1,
       tc1 + 2::int4 as t2,  
       tc1 + 3::int8 as t3, 
       tc1 + 4::float4::int4 as t4, 
       tc1 + 5::float8::int4 as t5,
       tc2 + 1::int2 as t6,
       tc2 + 2::int4 as t7, 
       tc2 + 3::int8 as t8, 
       tc2 + 4::float4::numeric as t9,
       tc2 + 5::float8::numeric as t10
    from test2 
    order by t1, t2, t3, t4, t5, t6, t7, t8, t9, t10;

select tc1 - tc2 as t1, tc2 - tc1 as t2 from test1 order by t1, t2;
select tc1 - tc2 as t1, tc2 - tc1 as t2 from test2 order by t1, t2;
select tc1, tc1 + 1/24/3600  t1 from test1 order by tc1, t1;
select tc1, tc1 + 1/24/3600  t1 from test2 order by tc1, t1;
select tc1, tc1 - 1/24/3600  t1 from test1 order by tc1, t1;
select tc1, tc1 - 1/24/3600  t1 from test2 order by tc1, t1;

drop table test1;
drop table test2;

reset enable_oracle_compatible;

--test sequence

set enable_oracle_compatible = off;
create sequence public.my_serial start 11;

select nextval('public.my_serial');
select currval('public.my_serial');
select lastval();

select setval('public.my_serial', 12);
select currval('public.my_serial');
select nextval('public.my_serial');
select lastval();

select setval('public.my_serial', 22, true);
select currval('public.my_serial');
select nextval('public.my_serial');
select lastval();

select setval('public.my_serial', 33, false);
select currval('public.my_serial');
select nextval('public.my_serial');
select lastval();

select currval('postgres.public.my_serial');
select nextval('postgres.public.my_serial');
select lastval();

select currval('my_serial');
select nextval('my_serial');
select lastval();

drop sequence public.my_serial;
create sequence public.my_serial start 11;
set enable_oracle_compatible = on;

select public.my_serial.nextval;
select public.my_serial.currval;
select lastval();

select setval('public.my_serial', 12);
select public.my_serial.currval;
select public.my_serial.nextval;
select lastval();

select setval('public.my_serial', 22, true);
select public.my_serial.currval;
select public.my_serial.nextval;
select lastval();

select setval('public.my_serial', 33, false);
select public.my_serial.currval;
select public.my_serial.nextval;
select lastval();

select postgres.public.my_serial.currval;
select postgres.public.my_serial.nextval;
select lastval();

select my_serial.currval;
select my_serial.nextval;
select lastval();

drop sequence public.my_serial;
reset enable_oracle_compatible;

-- test dual;
select * from dual;
select 1 as t from dual;
select sysdate - sysdate AS now from dual;

-- test using index
drop table if exists t cascade;
create table t (f1 integer not null,nc text);
alter table t add constraint t_f1_PK primary key (f1) using index;
drop table t cascade;

drop table if exists t;
create table t (f1 integer not null,nc text);
alter table t add constraint t_f1_PK primary key (f1);
drop table t cascade;

drop table if exists t;
create table t (f1 integer not null primary key using index,nc text);
drop table t cascade;

drop table if exists t;
create table t (f1 integer not null primary key,nc text);
drop table t cascade;

-- test varchar2
    -- ERROR (typmod >= 1)
    CREATE TABLE foo (a VARCHAR2(0));
    -- ERROR (number of typmods = 1)
    CREATE TABLE foo (a VARCHAR2(10, 1));
    -- OK
    CREATE TABLE foo (a VARCHAR(5000));
    -- cleanup
    DROP TABLE foo;

    -- OK
    CREATE TABLE foo (a VARCHAR2(5));
    CREATE INDEX ON foo(a);

    --
    -- test that no value longer than maxlen is allowed
    --
    -- ERROR (length > 5)
    INSERT INTO foo VALUES ('abcdef');

    -- ERROR (length > 5);
    -- VARCHAR2 does not truncate blank spaces on implicit coercion
    INSERT INTO foo VALUES ('abcde  ');

    -- OK
    INSERT INTO foo VALUES ('abcde');
    -- OK
    INSERT INTO foo VALUES ('abcdef'::VARCHAR2(5));
    -- OK
    INSERT INTO foo VALUES ('abcde  '::VARCHAR2(5));
    --OK
    INSERT INTO foo VALUES ('abc'::VARCHAR2(5));

    --
    -- test whitespace semantics on comparison
    --
    -- equal
    SELECT 'abcde   '::VARCHAR2(10) = 'abcde   '::VARCHAR2(10);
    -- not equal
    SELECT 'abcde  '::VARCHAR2(10) = 'abcde   '::VARCHAR2(10);

    -- cleanup
    DROP TABLE foo;

-- test nvarchar2
    --
    -- test type modifier related rules
    --
    -- ERROR (typmod >= 1)
    CREATE TABLE bar (a NVARCHAR2(0));
    -- ERROR (number of typmods = 1)
    CREATE TABLE bar (a NVARCHAR2(10, 1));

    -- OK
    CREATE TABLE bar (a VARCHAR(5000));
    CREATE INDEX ON bar(a);

    -- cleanup
    DROP TABLE bar;

    -- OK
    CREATE TABLE bar (a NVARCHAR2(5));

    --
    -- test that no value longer than maxlen is allowed
    --
    -- ERROR (length > 5)
    INSERT INTO bar VALUES ('abcdef');

    -- ERROR (length > 5);
    -- NVARCHAR2 does not truncate blank spaces on implicit coercion
    INSERT INTO bar VALUES ('abcde  ');
    -- OK
    INSERT INTO bar VALUES ('abcde');
    -- OK
    INSERT INTO bar VALUES ('abcdef'::NVARCHAR2(5));
    -- OK
    INSERT INTO bar VALUES ('abcde  '::NVARCHAR2(5));
    --OK
    INSERT INTO bar VALUES ('abc'::NVARCHAR2(5));

    --
    -- test whitespace semantics on comparison
    --
    -- equal
    SELECT 'abcde   '::NVARCHAR2(10) = 'abcde   '::NVARCHAR2(10);
    -- not equal
    SELECT 'abcde  '::NVARCHAR2(10) = 'abcde   '::NVARCHAR2(10);

    -- cleanup
    DROP TABLE bar;

-- test cast string to numeric
drop table if exists test_t;
create table test_t(id varchar2(20));

insert into test_t values('1'); 
insert into test_t values(23);
insert into test_t values(24);
insert into test_t values(25);

set enable_oracle_compatible = off;

select * from test_t where id = 1 order by id asc;
select * from test_t where id = '1' order by id asc;
select * from test_t where id=1::text order by id asc;
select * from test_t where id = 'xxx' order by id asc;

select * from test_t where id = 1 order by id asc;
select * from test_t where id = '1' order by id asc;
select * from test_t where id=1::text order by id asc;
select * from test_t where id = 'xxx' order by id asc;

set enable_oracle_compatible = on;

select * from test_t where id = 1 order by id asc;
select * from test_t where id = '1' order by id asc;
select * from test_t where id=1::text order by id asc;
select * from test_t where id = 'xxx' order by id asc;

select * from test_t where id = 1 order by id asc;
select * from test_t where id = '1' order by id asc;
select * from test_t where id=1::text order by id asc;
select * from test_t where id = 'xxx' order by id asc;

insert into test_t values('xxx');

set enable_oracle_compatible = off;

select * from test_t where id = 1 order by id asc;
select * from test_t where id = '1' order by id asc;
select * from test_t where id=1::text order by id asc;
select * from test_t where id = 'xxx' order by id asc;

select * from test_t where id = 1 order by id asc;
select * from test_t where id = '1' order by id asc;
select * from test_t where id=1::text order by id asc;
select * from test_t where id = 'xxx' order by id asc;

set enable_oracle_compatible = on;

select * from test_t where id = 1 order by id asc;
select * from test_t where id = '1' order by id asc;
select * from test_t where id=1::text order by id asc;
select * from test_t where id = 'xxx' order by id asc;

select * from test_t where id = 1 order by id asc;
select * from test_t where id = '1' order by id asc;
select * from test_t where id=1::text order by id asc;
select * from test_t where id = 'xxx' order by id asc;

drop table if exists test_t;

select 'xxx' || 1 as r;
select 1 || 'xxxx' as r;

drop table if exists test_t;
create table test_t (n int);
insert into test_t values(1);
insert into test_t values(2);
insert into test_t values(3);
insert into test_t values(4);
insert into test_t values(5);
insert into test_t values(6);
insert into test_t values(7);
insert into test_t values(8);
insert into test_t values(9);

select r from (SELECT '7' as r from dual UNION ALL SELECT n+1 as r FROM test_t WHERE n < 7 ) order by r;
drop table if exists test_t;

--test nlssort
SET client_encoding = utf8;
DROP DATABASE IF EXISTS regression_sort;
CREATE DATABASE regression_sort WITH TEMPLATE = template0 ENCODING='SQL_ASCII' LC_COLLATE='C' LC_CTYPE='C';
\c regression_sort
CREATE TABLE test_sort (name TEXT);
INSERT INTO test_sort VALUES ('red'), ('brown'), ('yellow'), ('Purple');
INSERT INTO test_sort VALUES ('guangdong'), ('shenzhen'), ('Tencent'), ('TBase');
SELECT * FROM test_sort ORDER BY NLSSORT(name, 'en_US.utf8');
SELECT * FROM test_sort ORDER BY NLSSORT(name, '');
SELECT set_nls_sort('invalid');
SELECT * FROM test_sort ORDER BY NLSSORT(name);
SELECT set_nls_sort('');
SELECT * FROM test_sort ORDER BY NLSSORT(name);
SELECT set_nls_sort('en_US.utf8');
SELECT * FROM test_sort ORDER BY NLSSORT(name);
INSERT INTO test_sort VALUES(NULL);
SELECT * FROM test_sort ORDER BY NLSSORT(name);

SELECT set_nls_sort('nls_sort = russian');
SELECT * FROM test_sort ORDER BY NLSSORT(name);
SELECT set_nls_sort('nls_sortr = pt_PT.iso885915@euro');
SELECT * FROM test_sort ORDER BY NLSSORT(name);
SELECT set_nls_sort('nls_sortr = en_US.iso885915');
SELECT * FROM test_sort ORDER BY NLSSORT(name, 'Nls_sort =   ');
SELECT * FROM test_sort ORDER BY NLSSORT(name, 'Nls_sort =  zh_CN.gb18030 ');
SELECT * FROM test_sort ORDER BY NLSSORT(name, 'Nls_sort =  wa_BE.iso885915@euro ');
SELECT * FROM test_sort ORDER BY NLSSORT(name, 'NLS_SORT =  tt_RU.utf8@iqtelif ');
SELECT * FROM test_sort ORDER BY NLSSORT(name, 'Nls_sortR =  tt_RU.utf8@iqtelif ');
drop table test_sort;
\c postgres
DROP DATABASE IF EXISTS regression_sort;
SET client_encoding to default;

-- test !=- operator
set enable_oracle_compatible = off;
drop table if exists test cascade;
create table test(tc1 int);

insert into test values(1),(-1),(null);
insert into test values(2),(-2),(null);

set enable_oracle_compatible = off;
select * from test where tc1 != -1  order by tc1 asc;
select * from test where tc1 !=-1  order by tc1 asc;	-- error
select * from test where tc1 <> -1  order by tc1 asc;
select * from test where tc1 <>-1  order by tc1 asc;

set enable_oracle_compatible to on;
select * from test where tc1 != -1  order by tc1 asc;
select * from test where tc1 !=-1  order by tc1 asc;
select * from test where tc1 <> -1  order by tc1 asc;
select * from test where tc1 <>-1  order by tc1 asc;

set enable_oracle_compatible = off;
CREATE OPERATOR pg_oracle.!=- (
  LEFTARG   = INTEGER,
  RIGHTARG  = INTEGER,
  PROCEDURE = pg_catalog.int4div
);

set enable_oracle_compatible = off;
select * from test where tc1 != -1  order by tc1 asc;
select * from test where tc1 !=-1  order by tc1 asc;
select * from test where tc1 <> -1  order by tc1 asc;
select * from test where tc1 <>-1  order by tc1 asc;

set enable_oracle_compatible = on;
select * from test where tc1 != -1  order by tc1 asc;
select * from test where tc1 !=-1  order by tc1 asc;
select * from test where tc1 <> -1  order by tc1 asc;
select * from test where tc1 <>-1  order by tc1 asc;

set enable_oracle_compatible = on;
drop operator pg_oracle.!=-(integer, integer);
set enable_oracle_compatible = off;
drop operator pg_oracle.!=-(integer, integer);

set enable_oracle_compatible to on;
CREATE OPERATOR pg_oracle.!=- (
  LEFTARG   = INTEGER,
  RIGHTARG  = INTEGER,
  PROCEDURE = pg_catalog.int4div
);

drop table if exists test cascade;

--test to_date

set enable_oracle_compatible to off;

select to_date('2005-01-01 13:14:20', 'YYYY-MM-DD HH24:MI:SS') from dual;
select to_date('2005-01-01 13:14:20') from dual;
select to_date('2005-01-01 13:14:20', '') from dual;
select to_date('', 'YYYY-MM-DD HH24:MI:SS') from dual;
select to_date('','') from dual;

select to_timestamp('2005-01-01 13:14:20', 'YYYY-MM-DD HH24:MI:SS') from dual;
select to_timestamp('2005-01-01 13:14:20') from dual;
select to_timestamp('2005-01-01 13:14:20', '') from dual;
select to_timestamp('', 'YYYY-MM-DD HH24:MI:SS') from dual;
select to_timestamp('','') from dual;
SELECT to_timestamp('10-Sep-02 14:10:10.123000', 'DD-Mon-RR HH24:MI:SS.FF') from dual;
SELECT to_timestamp('10-Sep-02 14:10:10.123000', 'DD-Mon-YY HH24:MI:SS.FF') from dual;

set enable_oracle_compatible to on;

select to_date('2005-01-01 13:14:20', 'YYYY-MM-DD HH24:MI:SS') from dual;
select to_date('2005-01-01 13:14:20') from dual;
select to_date('2005-01-01 13:14:20', '') from dual;
select to_date('', 'YYYY-MM-DD HH24:MI:SS') from dual;
select to_date('','') from dual;

select to_timestamp('2005-01-01 13:14:20', 'YYYY-MM-DD HH24:MI:SS') from dual;
select to_timestamp('2005-01-01 13:14:20') from dual;
select to_timestamp('2005-01-01 13:14:20', '') from dual;
select to_timestamp('', 'YYYY-MM-DD HH24:MI:SS') from dual;
select to_timestamp('','') from dual;
SELECT to_timestamp('10-Sep-02 14:10:10.123000', 'DD-Mon-RR HH24:MI:SS.FF') from dual;
SELECT to_timestamp('10-Sep-02 14:10:10.123000', 'DD-Mon-YY HH24:MI:SS.FF') from dual;

select TO_TIMESTAMP_TZ('2005-01-01 13:14:20', 'YYYY-MM-DD HH24:MI:SS') from dual;
select TO_TIMESTAMP_TZ('2005-01-01 13:14:20') from dual;
select TO_TIMESTAMP_TZ('2005-01-01 13:14:20', '') from dual;
select TO_TIMESTAMP_TZ('', 'YYYY-MM-DD HH24:MI:SS') from dual;
select TO_TIMESTAMP_TZ('','') from dual;
SELECT TO_TIMESTAMP_TZ('1999-12-01 11:00:00 -8:00', 'YYYY-MM-DD HH:MI:SS TZH:TZM') FROM DUAL;
SELECT TO_TIMESTAMP_TZ('10-Sep-02 14:10:10.123000', 'DD-Mon-YY HH24:MI:SS.FF') from dual;

set enable_oracle_compatible to on;
SET datestyle TO ISO;
set intervalstyle to sql_standard;

select to_date('2017-12-04 10:00:00','yyyy-mm-dd hh24:mi:ss') + 365000 from dual;
select to_date('2017-12-04 10:00:00','yyyy-mm-dd hh24:mi:ss') + 0.365000 from dual;
select to_date('2017-12-04 10:00:00','yyyy-mm-dd hh24:mi:ss') + -365000 from dual;
select to_date('2017-12-04 10:00:00','yyyy-mm-dd hh24:mi:ss') - 365000 from dual;
select to_date('2017-12-04 10:00:00','yyyy-mm-dd hh24:mi:ss') - 0.365000 from dual;
select to_date('2017-12-04 10:00:00','yyyy-mm-dd hh24:mi:ss') - -365000 from dual;
select to_date('2017-12-04 10:00:00','yyyy-mm-dd hh24:mi:ss') + 1/24/60 from dual;
select to_date('2017-12-04 10:00:00','yyyy-mm-dd hh24:mi:ss') - 1/24/60 from dual;
select to_date('2017-12-04 10:00:00','yyyy-mm-dd hh24:mi:ss') + 365.0 from dual;
select to_date('2017-12-04 10:00:00','yyyy-mm-dd hh24:mi:ss') - 365.0 from dual;
select to_date('2017-12-04 10:00:00','yyyy-mm-dd hh24:mi:ss') + 0.466757686856445 from dual;
select to_date('2017-12-04 10:00:00','yyyy-mm-dd hh24:mi:ss') - 0.466757686856445 from dual;
select to_date('2017-12-04 10:00:00','yyyy-mm-dd hh24:mi:ss') - to_date('2017-05-23 09:23:14','yyyy-mm-dd hh24:mi:ss') from dual;
select to_date('2017-12-04 10:00:00','yyyy-mm-dd hh24:mi:ss') - to_date('2017-05-23 09:23:14','yyyy-mm-dd hh24:mi:ss') from dual;
select to_date('2017-12-04 10:00:00','yyyy-mm-dd hh24:mi:ss') - to_date('2022-12-04 10:00:00','yyyy-mm-dd hh24:mi:ss') from dual;
select to_date('2017-12-04 10:00:00','yyyy-mm-dd hh24:mi:ss') - to_date('2022-12-04 10:00:00','yyyy-mm-dd hh24:mi:ss') from dual;
select to_date(sysdate,'yyyy-mm-dd hh24:mi:ss') - to_date(sysdate,'yyyy-mm-dd hh24:mi:ss') from dual;
select to_date(current_date,'yyyy-mm-dd hh24:mi:ss') - to_date(current_date,'yyyy-mm-dd hh24:mi:ss') from dual;

--test add_months
SELECT add_months ('2003-08-01', 3);
SELECT add_months ('2003-08-01', -3);
SELECT add_months ('2003-08-21', -3);
SELECT add_months ('2003-01-31', 1);
SELECT add_months ('2008-02-28', 1);
SELECT add_months ('2008-02-29', 1);
SELECT add_months ('2008-01-31', 12);
SELECT add_months ('2008-01-31', -12);
SELECT add_months ('2008-01-31', 95903);
SELECT add_months ('2008-01-31', -80640);
SELECT add_months ('03-21-2008',3);
SELECT add_months ('21-MAR-2008',3);
SELECT add_months ('21-MAR-08',3);
SELECT add_months ('2008-MAR-21',3);
SELECT add_months ('March 21,2008',3);
SELECT add_months('03/21/2008',3);
SELECT add_months('20080321',3);
SELECT add_months('080321',3);

SELECT add_months ('2003-08-01 10:12:21', 3);
SELECT add_months ('2003-08-01 10:21:21', -3);
SELECT add_months ('2003-08-21 12:21:21', -3);
SELECT add_months ('2003-01-31 01:12:45', 1);
SELECT add_months ('2008-02-28 02:12:12', 1);
SELECT add_months ('2008-02-29 12:12:12', 1);
SELECT add_months ('2008-01-31 11:11:21', 12);
SELECT add_months ('2008-01-31 11:21:21', -12);
SELECT add_months ('2008-01-31 12:12:12', 95903);
SELECT add_months ('2008-01-31 11:32:12', -80640);
SELECT add_months ('03-21-2008 08:12:22',3);
SELECT add_months ('21-MAR-2008 06:02:12',3);
SELECT add_months ('21-MAR-08 12:11:22',3);
SELECT add_months ('2008-MAR-21 11:32:43',3);
SELECT add_months ('March 21,2008 12:32:12',3);
SELECT add_months('03/21/2008 12:32:12',3);
SELECT add_months('20080321 123244',3);
SELECT add_months('080321 121212',3);

SELECT add_months('10-Sep-02 14:10:10.123000', 3);

SELECT add_months ('10-Sep-02 14:10:10.123000'::date, 3);
SELECT add_months ('10-Sep-02 14:10:10.123000'::timestamp, 3);
SELECT add_months ('10-Sep-02 14:10:10.123000'::timestamptz, 3);

select add_months(to_date('2016-01-31','YYYY-MM-DD'),1) from dual;
select add_months(to_date('2016-03-31','YYYY-MM-DD'),1) from dual;
select add_months(to_date('2016-04-30','YYYY-MM-DD'),1) from dual;
select add_months(to_date('2016-05-31','YYYY-MM-DD'),1) from dual;
select add_months(to_date('2016-06-30','YYYY-MM-DD'),1) from dual;
select add_months(to_date('2016-07-31','YYYY-MM-DD'),1) from dual;
select add_months(to_date('2016-08-31','YYYY-MM-DD'),1) from dual;
select add_months(to_date('2016-09-30','YYYY-MM-DD'),1) from dual;
select add_months(to_date('2016-10-31','YYYY-MM-DD'),1) from dual;
select add_months(to_date('2016-11-30','YYYY-MM-DD'),1) from dual;
select add_months(to_date('2016-12-31','YYYY-MM-DD'),1) from dual;
select add_months(to_date('2017-02-28','YYYY-MM-DD'),1) from dual;
select add_months(to_date('2016-02-29','YYYY-MM-DD'),1) from dual;

SELECT pg_catalog.timestamp_pl_interval('2003-08-01 10:12:21', pg_catalog.interval_mul(interval '1 month', 3));
SELECT pg_catalog.timestamptz_pl_interval(TO_TIMESTAMP_TZ('10-Sep-02 14:10:10.123000', 'DD-Mon-YY HH24:MI:SS.FF'), pg_catalog.interval_mul(interval '1 month', 3));

select add_months(to_date('2005-01-01 13:14:20', 'YYYY-MM-DD HH24:MI:SS'), 3) from dual;

-- test last_day
SELECT last_day(to_date('2003/03/15', 'yyyy/mm/dd'));
SELECT last_day(to_date('2003/02/03', 'yyyy/mm/dd'));
SELECT last_day(to_date('2004/02/03', 'yyyy/mm/dd'));
SELECT last_day('1900-02-01');
SELECT last_day('2000-02-01');
SELECT last_day('2007-02-01');
SELECT last_day('2008-02-01');

SELECT last_day(to_date('2003/03/15 11:12:21', 'yyyy/mm/dd hh:mi:ss'));
SELECT last_day(to_date('2003/02/03 10:21:32', 'yyyy/mm/dd hh:mi:ss'));
SELECT last_day(to_date('2004/02/03 11:32:12', 'yyyy/mm/dd hh:mi:ss'));
SELECT last_day('1900-02-01 12:12:11');
SELECT last_day('2000-02-01 121143');
SELECT last_day('2007-02-01 12:21:33');
SELECT last_day('2008-02-01 121212');

SELECT (pg_catalog.date_trunc('MONTH', '1900-02-01 12:12:11'::timestamp) + INTERVAL '1 MONTH - 1 day' + '1900-02-01 12:12:11'::time)::pg_catalog.timestamp;
SELECT (pg_catalog.date_trunc('MONTH', TO_TIMESTAMP_TZ('10-Sep-02 14:10:10.123000', 'DD-Mon-YY HH24:MI:SS.FF')) + INTERVAL '1 MONTH - 1 day' + TO_TIMESTAMP_TZ('10-Sep-02 14:10:10.123000', 'DD-Mon-YY HH24:MI:SS.FF')::time)::pg_catalog.timestamptz;


SELECT last_day ('10-Sep-02 14:10:10.123000'::date);
SELECT last_day ('10-Sep-02 14:10:10.123000'::timestamp);
SELECT last_day ('10-Sep-02 14:10:10.123000'::timestamptz);

-- test months_between
SELECT months_between (to_date ('2003/01/01', 'yyyy/mm/dd'), to_date ('2003/03/14', 'yyyy/mm/dd'));
SELECT months_between (to_date ('2003/07/01', 'yyyy/mm/dd'), to_date ('2003/03/14', 'yyyy/mm/dd'));
SELECT months_between (to_date ('2003/07/02', 'yyyy/mm/dd'), to_date ('2003/07/02', 'yyyy/mm/dd'));
SELECT months_between (to_date ('2003/08/02', 'yyyy/mm/dd'), to_date ('2003/06/02', 'yyyy/mm/dd'));
SELECT months_between ('2007-02-28', '2007-04-30');
SELECT months_between ('2008-01-31', '2008-02-29');
SELECT months_between ('2008-02-29', '2008-03-31');
SELECT months_between ('2008-02-29', '2008-04-30');
SELECT trunc(months_between('21-feb-2008', '2008-02-29'));

SELECT months_between (to_date ('2003/01/01 12:12:12', 'yyyy/mm/dd h24:mi:ss'), to_date ('2003/03/14 11:11:11', 'yyyy/mm/dd h24:mi:ss'));
SELECT months_between (to_date ('2003/07/01 10:11:11', 'yyyy/mm/dd h24:mi:ss'), to_date ('2003/03/14 10:12:12', 'yyyy/mm/dd h24:mi:ss'));
SELECT months_between (to_date ('2003/07/02 11:21:21', 'yyyy/mm/dd h24:mi:ss'), to_date ('2003/07/02 11:11:11', 'yyyy/mm/dd h24:mi:ss'));
SELECT months_between (to_timestamp ('2003/08/02 10:11:12', 'yyyy/mm/dd h24:mi:ss'), to_date ('2003/06/02 10:10:11', 'yyyy/mm/dd h24:mi:ss'));
SELECT months_between ('2007-02-28 111111', '2007-04-30 112121');
SELECT months_between ('2008-01-31 11:32:11', '2008-02-29 11:12:12');
SELECT months_between ('2008-02-29 10:11:13', '2008-03-31 10:12:11' );
SELECT months_between ('2008-02-29 111111', '2008-04-30 12:12:12');
SELECT trunc(months_between('21-feb-2008 12:11:11', '2008-02-29 11:11:11'));

SELECT months_between ( last_day ('10-Sep-02 14:10:10.123000'::date), add_months ('10-Sep-02 14:10:10.123000'::date, 3));
SELECT months_between ( last_day ('10-Sep-02 14:10:10.123000'::timestamp), add_months ('10-Sep-02 14:10:10.123000'::timestamp, 3));
SELECT months_between ( last_day ('10-Sep-02 14:10:10.123000'::timestamptz), add_months ('10-Sep-02 14:10:10.123000'::timestamptz, 3));

-- test next_day
SELECT next_day ('2003-08-01', 'TUESDAY');
SELECT next_day ('2003-08-06', 'WEDNESDAY');
SELECT next_day ('2003-08-06', 'SUNDAY');
SELECT next_day ('2008-01-01', 'sun');
SELECT next_day ('2008-01-01', 'sunAAA');
SELECT next_day ('2008-01-01', 1);
SELECT next_day ('2008-01-01', 7);

SELECT next_day ('2003-08-01 111211', 'TUESDAY');
SELECT next_day ('2003-08-06 10:11:43', 'WEDNESDAY');
SELECT next_day ('2003-08-06 11:21:21', 'SUNDAY');
SELECT next_day ('2008-01-01 111343', 'sun');
SELECT next_day ('2008-01-01 121212', 'sunAAA');
SELECT next_day ('2008-01-01 111213', 1);
SELECT next_day ('2008-01-01 11:12:13', 7);

SELECT (pg_oracle.next_day('2003-08-06 10:11:43'::pg_catalog.date, 'WEDNESDAY') + '2003-08-06 10:11:43'::time)::pg_catalog.timestamp;
SELECT (pg_oracle.next_day(TO_TIMESTAMP_TZ('10-Sep-02 14:10:10.123000', 'DD-Mon-YY HH24:MI:SS.FF')::pg_catalog.date, 'WEDNESDAY') + TO_TIMESTAMP_TZ('10-Sep-02 14:10:10.123000', 'DD-Mon-YY HH24:MI:SS.FF')::timetz)::pg_catalog.timestamptz;

select next_day(to_date('01-Aug-03', 'DD-MON-YY'), 'TUESDAY')  =  to_date ('05-Aug-03', 'DD-MON-YY');
select next_day(to_date('06-Aug-03', 'DD-MON-YY'), 'WEDNESDAY') =  to_date ('13-Aug-03', 'DD-MON-YY');
select next_day(to_date('06-Aug-03', 'DD-MON-YY'), 'SUNDAY')  =  to_date ('10-Aug-03', 'DD-MON-YY');

select next_day(to_date('01-Aug-03 101111', 'DD-MON-YY h24miss'), 'TUESDAY') = to_date ('05-Aug-03 101111', 'DD-MON-YY h24miss');
select next_day(to_date('06-Aug-03 10:12:13', 'DD-MON-YY H24:MI:SS'), 'WEDNESDAY') = to_date ('13-Aug-03 10:12:13', 'DD-MON-YY H24:MI:SS');
select next_day(to_date('06-Aug-03 11:11:11', 'DD-MON-YY HH:MI:SS'), 'SUNDAY') = to_date ('10-Aug-03 11:11:11', 'DD-MON-YY HH:MI:SS');

SELECT next_day ('10-Sep-02 14:10:10.123000'::date, 4), next_day ('10-Sep-02 14:10:10.123000'::date, 'WEDNESDAY');
SELECT next_day ('10-Sep-02 14:10:10.123000'::timestamp, 4), next_day ('10-Sep-02 14:10:10.123000'::timestamp, 'WEDNESDAY');
SELECT next_day ('10-Sep-02 14:10:10.123000'::timestamptz, 4), next_day ('10-Sep-02 14:10:10.123000'::timestamptz, 'WEDNESDAY');
select next_day('2013-08-01', 'TUESDAY') - next_day('2003-08-01', 'TUESDAY') from dual;
select to_date('2013-08-01 11:21:22', 'yyyy-mm-dd hh24:mi:ss') - next_day('2013-08-01', 'TUESDAY') from dual; 

-- test round
select round(TIMESTAMP WITH TIME ZONE'12/08/1990 05:35:25','YEAR') = '1991-01-01 00:00:00';
select round(TIMESTAMP WITH TIME ZONE'05/08/1990 05:35:25','Q') = '1990-04-01 00:00:00';
select round(TIMESTAMP WITH TIME ZONE'12/08/1990 05:35:25','MONTH') = '1990-12-01 00:00:00';
select round(TIMESTAMP WITH TIME ZONE'12/08/1990 05:35:25','DDD') = '1990-12-08 00:00:00';
select round(TIMESTAMP WITH TIME ZONE'12/08/1990 05:35:25','DAY') = '1990-12-09 00:00:00';
select round(TIMESTAMP WITH TIME ZONE'12/08/1990 05:35:25','hh') = '1990-12-08 06:00:00';
select round(TIMESTAMP WITH TIME ZONE'12/08/1990 05:35:25','mi') = '1990-12-08 05:35:00';

select round(to_date ('22-AUG-03', 'DD-MON-YY'),'YEAR')  =  to_date ('01-JAN-04', 'DD-MON-YY');
select round(to_date ('22-AUG-03', 'DD-MON-YY'),'Q')  =  to_date ('01-OCT-03', 'DD-MON-YY');
select round(to_date ('22-AUG-03', 'DD-MON-YY'),'MONTH') =  to_date ('01-SEP-03', 'DD-MON-YY');
select round(to_date ('22-AUG-03', 'DD-MON-YY'),'DDD')  =  to_date ('22-AUG-03', 'DD-MON-YY');
select round(to_date ('22-AUG-03', 'DD-MON-YY'),'DAY')  =  to_date ('24-AUG-03', 'DD-MON-YY');

SELECT round(to_date('210514 12:13:44','DDMMYY HH24:MI:SS'));
SELECT round(1.234::double precision, 2), trunc(1.234::double precision, 2);
SELECT round(1.234::float, 2), trunc(1.234::float, 2);

SELECT round(1.234::double precision), trunc(1.234::double precision);
SELECT round(1.234::float), trunc(1.234::float);

SELECT round ('10-Sep-02 14:10:10.123000'::date);
SELECT round ('10-Sep-02 14:10:10.123000'::timestamp);
SELECT round ('10-Sep-02 14:10:10.123000'::timestamptz);

SELECT round ('10-Sep-02 14:10:10.123000'::date, 'MONTH');
SELECT round ('10-Sep-02 14:10:10.123000'::timestamp, 'MONTH');
SELECT round ('10-Sep-02 14:10:10.123000'::timestamptz, 'MONTH');

-- test trunc
SELECT trunc(to_date('210514 12:13:44','DDMMYY HH24:MI:SS'));

select trunc(to_date('22-AUG-03', 'DD-MON-YY'), 'YEAR')  =  to_date ('01-JAN-03', 'DD-MON-YY');
select trunc(to_date('22-AUG-03', 'DD-MON-YY'), 'Q')  =  to_date ('01-JUL-03', 'DD-MON-YY');
select trunc(to_date('22-AUG-03', 'DD-MON-YY'), 'MONTH') =  to_date ('01-AUG-03', 'DD-MON-YY');
select trunc(to_date('22-AUG-03', 'DD-MON-YY'), 'DDD')  =  to_date ('22-AUG-03', 'DD-MON-YY');
select trunc(to_date('22-AUG-03', 'DD-MON-YY'), 'DAY')  =  to_date ('17-AUG-03', 'DD-MON-YY');

select trunc(TIMESTAMP WITH TIME ZONE '2004-10-19 10:23:54+02','YEAR') = '2004-01-01 00:00:00-08';
select trunc(TIMESTAMP WITH TIME ZONE '2004-10-19 10:23:54+02','Q') = '2004-10-01 00:00:00-07';
select trunc(TIMESTAMP WITH TIME ZONE '2004-10-19 10:23:54+02','MONTH') = '2004-10-01 00:00:00-07';
select trunc(TIMESTAMP WITH TIME ZONE '2004-10-19 10:23:54+02','DDD') = '2004-10-19 00:00:00-07';
select trunc(TIMESTAMP WITH TIME ZONE '2004-10-19 10:23:54+02','DAY') = '2004-10-17 00:00:00-07';
select trunc(TIMESTAMP WITH TIME ZONE '2004-10-19 10:23:54+02','HH') = '2004-10-19 01:00:00-07';
select trunc(TIMESTAMP WITH TIME ZONE '2004-10-19 10:23:54+02','MI') = '2004-10-19 01:23:00-07';


SELECT trunc ('10-Sep-02 14:10:10.123000'::date);
SELECT trunc ('10-Sep-02 14:10:10.123000'::timestamp);
SELECT trunc ('10-Sep-02 14:10:10.123000'::timestamptz);

SELECT trunc ('10-Sep-02 14:10:10.123000'::date, 'MONTH');
SELECT trunc ('10-Sep-02 14:10:10.123000'::timestamp, 'MONTH');
SELECT trunc ('10-Sep-02 14:10:10.123000'::timestamptz, 'MONTH');

-- test numtoyminterval
select numtoyminterval(100, 'year') from dual;
select numtoyminterval(100, 'month') from dual;
select numtoyminterval(100, 'day') from dual;
select numtoyminterval(100, 'hour') from dual;
select numtoyminterval(100, 'minute') from dual;
select numtoyminterval(100, 'second') from dual;
select numtoyminterval(0.45, 'year') from dual;
select numtoyminterval(0.36, 'month') from dual;
select numtoyminterval(12.254, 'month') from dual;
select numtoyminterval(-0.45, 'year') from dual;
select numtoyminterval(-0.36, 'month') from dual;

-- test numtodsinterval
select numtodsinterval(100, 'year') from dual;
select numtodsinterval(100, 'month') from dual;
select numtodsinterval(100, 'day') from dual;
select numtodsinterval(100, 'hour') from dual;
select numtodsinterval(100, 'minute') from dual;
select numtodsinterval(100, 'second') from dual;
select numtodsinterval(120,'hour') from dual;
select numtodsinterval(60,'hour') from dual;
select numtodsinterval(30,'hour') from dual;
select numtodsinterval(24,'hour') from dual;
select numtodsinterval(23,'hour') from dual;
select numtodsinterval(120,'day') from dual;
select numtodsinterval(120,'minute') from dual;
select numtodsinterval(120,'second') from dual;
select numtodsinterval(-120.45,'day') from dual;
select numtodsinterval(120.39,'day') from dual;
select numtodsinterval(-48.34,'hour') from dual;
select numtodsinterval(48.34,'hour') from dual;
select numtodsinterval(-120.59,'minute') from dual;
select numtodsinterval(120.59,'minute') from dual;
select numtodsinterval(-120.59,'second') from dual;
select numtodsinterval(120.59,'second') from dual;
select numtodsinterval(100/3,'minute') from dual;
select numtodsinterval(0.1,'day') from dual;
select numtodsinterval(0.1,'hour') from dual;
select numtodsinterval(0.1,'minute') from dual;
select numtodsinterval(0.1,'second') from dual;
select numtodsinterval(1.1,'day') from dual;
select numtodsinterval(1.1,'hour') from dual;
select numtodsinterval(1.1,'minute') from dual;
select numtodsinterval(1.1,'second') from dual;


-- test to_yminterval
select TO_YMINTERVAL('01-02') "14 months" from dual;
select TO_YMINTERVAL('P1Y2M') from dual;

-- test to_dsinterval
select TO_DSINTERVAL('100 00:00:00') from dual;
select TO_DSINTERVAL('P100DT05H') from dual;
select to_dsinterval('99999999 23:59:59') from dual;
select to_dsinterval('999999999 23:59:59') from dual;
select to_dsinterval('1000000000 23:59:59') from dual;
select to_dsinterval('0 23:59:59') from dual;
select to_dsinterval('-1 23:59:59') from dual;
select to_dsinterval('-999999999 23:59:59') from dual;
select to_dsinterval('100 24:00:00') from dual;
select to_dsinterval('100 25:00:00') from dual;
select to_dsinterval('100 11:60:00') from dual;
select to_dsinterval('100 11:00:60') from dual;
select to_dsinterval('95 18:30:00') * 3 from dual;
select to_dsinterval('95 18:30:00') * 1 from dual;
select to_dsinterval('95 18:30:00') * 1.5 from dual;
select to_dsinterval('95 18:30:00') * 3.5 from dual;
select to_dsinterval('95 18:30:00') * -3 from dual;
select to_dsinterval('95 18:30:00') * -3.5 from dual;

-- test dbtimezone
--select dbtimezone from dual;
--select dbtimezone() from dual;

-- test sessiontimezone
--select sessiontimezone from dual;
--select sessiontimezone() from dual;

-- test instr
select instr('Tech on the net', 'e') = 2;
select instr('Tech on the net', 'e', 1, 1) = 2;
select instr('Tech on the net', 'e', 1, 2) = 11;
select instr('Tech on the net', 'e', 1, 3) = 14;
select instr('Tech on the net', 'e', -3, 2) = 2;
select instr('abc', NULL) IS NULL;
select instr('abc', '') IS NULL;
select instr('', 'a') IS NULL;
select 1 = instr('abc', 'a');
select 3 = instr('abc', 'c');
select 0 = instr('abc', 'z');
select 1 = instr('abcabcabc', 'abca', 1);
select 4 = instr('abcabcabc', 'abca', 2);
select 0 = instr('abcabcabc', 'abca', 7);
select 0 = instr('abcabcabc', 'abca', 9);
select 4 = instr('abcabcabc', 'abca', -1);
select 1 = instr('abcabcabc', 'abca', -8);
select 1 = instr('abcabcabc', 'abca', -9);
select 0 = instr('abcabcabc', 'abca', -10);
select 1 = instr('abcabcabc', 'abca', 1, 1);
select 4 = instr('abcabcabc', 'abca', 1, 2);
select 0 = instr('abcabcabc', 'abca', 1, 3);

--test to_char
select to_char(22);
select to_char(99::smallint);
select to_char(-44444);
select to_char(1234567890123456::bigint);
select to_char(123.456::real);
select to_char(1234.5678::double precision);
select to_char(12345678901234567890::numeric);
select to_char(1234567890.12345);
select to_char('4.00'::numeric);
select to_char('4.0010'::numeric);

SET nls_date_format to '';
select pg_oracle.to_char(to_date('19-APR-16 21:41:48'));
set nls_date_format='YY-MonDD HH24:MI:SS';
select pg_oracle.to_char(to_date('14-Jan08 11:44:49+05:30'));
set nls_date_format='YY-DDMon HH24:MI:SS';
select pg_oracle.to_char(to_date('14-08Jan 11:44:49+05:30'));
set nls_date_format='DDMMYYYY HH24:MI:SS';
select pg_oracle.to_char(to_date('21052014 12:13:44+05:30'));
set nls_date_format='DDMMYY HH24:MI:SS';
select pg_oracle.to_char(to_date('210514 12:13:44+05:30'));
set nls_date_format='DDMMYYYY HH24:MI:SS';
select pg_oracle.to_char(pg_oracle.to_date('2014/04/25 10:13', 'YYYY/MM/DD HH:MI'));
set nls_date_format='YY-DDMon HH24:MI:SS';
select pg_oracle.to_char(pg_oracle.to_date('16-Feb-09 10:11:11', 'DD-Mon-YY HH:MI:SS'));
set nls_date_format='YY-DDMon HH24:MI:SS';
select pg_oracle.to_char(pg_oracle.to_date('02/16/09 04:12:12', 'MM/DD/YY HH24:MI:SS'));
set nls_date_format='YY-MonDD HH24:MI:SS';
select pg_oracle.to_char(pg_oracle.to_date('021609 111213', 'MMDDYY HHMISS'));
set nls_date_format='DDMMYYYY HH24:MI:SS';
select pg_oracle.to_char(pg_oracle.to_date('16-Feb-09 11:12:12', 'DD-Mon-YY HH:MI:SS'));
set nls_date_format='DDMMYYYY HH24:MI:SS';
select pg_oracle.to_char(pg_oracle.to_date('Feb/16/09 11:21:23', 'Mon/DD/YY HH:MI:SS'));
set nls_date_format='DDMMYYYY HH24:MI:SS';
select pg_oracle.to_char(pg_oracle.to_date('February.16.2009 10:11:12', 'Month.DD.YYYY HH:MI:SS'));
set nls_date_format='YY-MonDD HH24:MI:SS';
select pg_oracle.to_char(pg_oracle.to_date('20020315111212', 'yyyymmddhh12miss'));
set nls_date_format='DDMMYYYY HH24:MI:SS';
select pg_oracle.to_char(pg_oracle.to_date('January 15, 1989, 11:00 A.M.','Month dd, YYYY, HH:MI A.M.'));
set nls_date_format='DDMMYY HH24:MI:SS';
select pg_oracle.to_char(pg_oracle.to_date('14-Jan08 11:44:49+05:30' ,'YY-MonDD HH24:MI:SS'));
set nls_date_format='DDMMYYYY HH24:MI:SS';
select pg_oracle.to_char(pg_oracle.to_date('14-08Jan 11:44:49+05:30','YY-DDMon HH24:MI:SS'));
set nls_date_format='YY-MonDD HH24:MI:SS';
select pg_oracle.to_char(pg_oracle.to_date('21052014 12:13:44+05:30','DDMMYYYY HH24:MI:SS'));
set nls_date_format='DDMMYY HH24:MI:SS';
select pg_oracle.to_char(pg_oracle.to_date('210514 12:13:44+05:30','DDMMYY HH24:MI:SS'));

select to_char(TO_YMINTERVAL('01-02')) from dual;
select to_char(TO_YMINTERVAL('P1Y2M')) from dual;
select to_char(TO_DSINTERVAL('100 00:00:00')) from dual;
select to_char(TO_DSINTERVAL('P100DT05H')) from dual;

--test to_number
SELECT to_number('-34,338,492', '99G999G999') from dual;
SELECT to_number('-34,338,492.654,878', '99G999G999D999G999') from dual;
SELECT to_number('<564646.654564>', '999999.999999PR') from dual;
SELECT to_number('0.00001-', '9.999999S') from dual;
SELECT to_number('5.01-', 'FM9.999999S') from dual;
SELECT to_number('5.01-', 'FM9.999999MI') from dual;
SELECT to_number('5 4 4 4 4 8 . 7 8', '9 9 9 9 9 9 . 9 9') from dual;
SELECT to_number('.01', 'FM9.99') from dual;
SELECT to_number('.0', '99999999.99999999') from dual;
SELECT to_number('0', '99.99') from dual;
SELECT to_number('.-01', 'S99.99') from dual;
SELECT to_number('.01-', '99.99S') from dual;
SELECT to_number(' . 0 1-', ' 9 9 . 9 9 S') from dual;

SELECT to_number('-34338492') from dual;
SELECT to_number('-34338492.654878') from dual;
SELECT to_number('564646.654564') from dual;
SELECT to_number('0.00001') from dual;
SELECT to_number('5.01') from dual;
SELECT to_number('.01') from dual;
SELECT to_number('.0') from dual;
SELECT to_number('0') from dual;
SELECT to_number('01') from dual;

select to_number(123.09e34::float4) from dual;
select to_number(1234.094e23::float8) from dual;

SELECT to_number('123'::text);
SELECT to_number('123.456'::text);
SELECT to_number(123);
SELECT to_number(123::smallint);
SELECT to_number(123::int);
SELECT to_number(123::bigint);
SELECT to_number(123::numeric);
SELECT to_number(123.456);

-- test to_multi_byte
-- SELECT to_multi_byte('123$test') from dual;

-- SELECT octet_length('abc') from dual;
-- SELECT octet_length(to_multi_byte('abc')) from dual;

-- testto_single_byte
-- SELECT to_single_byte('123$test') from dual;
-- SELECT to_single_byte('１２３＄ｔｅｓｔ') from dual;

-- SELECT octet_length('ａｂｃ');
-- SELECT octet_length(to_single_byte('ａｂｃ')) from dual;

-- test sinh cosh tanh
SELECT sinh(1.570796), cosh(1.570796), tanh(4);
SELECT sinh(1.570796::numeric), cosh(1.570796::numeric), tanh(4::numeric);

--test nanvl
SELECT nanvl(12345, 1), nanvl('NaN', 1);
SELECT nanvl(12345::float4, 1), nanvl('NaN'::float4, 1);
SELECT nanvl(12345::float8, 1), nanvl('NaN'::float8, 1);
SELECT nanvl(12345::numeric, 1), nanvl('NaN'::numeric, 1);
SELECT nanvl(12345, '1'::varchar), nanvl('NaN', 1::varchar);
SELECT nanvl(12345::float4, '1'::varchar), nanvl('NaN'::float4, '1'::varchar);
SELECT nanvl(12345::float8, '1'::varchar), nanvl('NaN'::float8, '1'::varchar);
SELECT nanvl(12345::numeric, '1'::varchar), nanvl('NaN'::numeric, '1'::varchar);
SELECT nanvl(12345, '1'::char), nanvl('NaN', 1::char);
SELECT nanvl(12345::float4, '1'::char), nanvl('NaN'::float4, '1'::char);
SELECT nanvl(12345::float8, '1'::char), nanvl('NaN'::float8, '1'::char);
SELECT nanvl(12345::numeric, '1'::char), nanvl('NaN'::numeric, '1'::char);

--test nvl
select nvl('A'::text, 'B');
select nvl(NULL::text, 'B');
select nvl(NULL::text, NULL);
select nvl(1, 2);
select nvl(NULL, 2);

--test nvl2
select nvl2('A'::text, 'B', 'C');
select nvl2(NULL::text, 'B', 'C');
select nvl2('A'::text, NULL, 'C');
select nvl2(NULL::text, 'B', NULL);
select nvl2(1, 2, 3);
select nvl2(NULL, 2, 3);

-- test lnnvl
select lnnvl(true);
select lnnvl(false);
select lnnvl(NULL);

set enable_oracle_compatible to on;
-- test dump
SELECT dump('Yellow dog'::text) ~ E'^Typ=25 Len=(\\d+): \\d+(,\\d+)*$' AS t;
SELECT dump('Yellow dog'::text, 10) ~ E'^Typ=25 Len=(\\d+): \\d+(,\\d+)*$' AS t;
SELECT dump('Yellow dog'::text, 17) ~ E'^Typ=25 Len=(\\d+): .(,.)*$' AS t;
SELECT dump(10::int2) ~ E'^Typ=21 Len=2: \\d+(,\\d+){1}$' AS t;
SELECT dump(10::int4) ~ E'^Typ=23 Len=4: \\d+(,\\d+){3}$' AS t;
SELECT dump(10::int8) ~ E'^Typ=20 Len=8: \\d+(,\\d+){7}$' AS t;
SELECT dump(10.23::float4) ~ E'^Typ=700 Len=4: \\d+(,\\d+){3}$' AS t;
SELECT dump(10.23::float8) ~ E'^Typ=701 Len=8: \\d+(,\\d+){7}$' AS t;
SELECT dump(10.23::numeric) ~ E'^Typ=1700 Len=(\\d+): \\d+(,\\d+)*$' AS t;
SELECT dump('2008-10-10'::date) ~ E'^Typ=1082 Len=4: \\d+(,\\d+){3}$' AS t;
SELECT dump('2008-10-10'::timestamp) ~ E'^Typ=1114 Len=8: \\d+(,\\d+){7}$' AS t;
SELECT dump('2009-10-10'::timestamp) ~ E'^Typ=1114 Len=8: \\d+(,\\d+){7}$' AS t;

-- test substr
select substr('This is a test', 6, 2) = 'is';
select substr('This is a test', 6) =  'is a test';
select substr('TechOnTheNet', 1, 4) =  'Tech';
select substr('TechOnTheNet', -3, 3) =  'Net';
select substr('TechOnTheNet', -6, 3) =  'The';
select substr('TechOnTheNet', -8, 2) =  'On';
select substr('TechOnTheNet', -8, 0) =  '';
select substr('TechOnTheNet', -8, -1) =  '';
select substr(1234567,3.6::smallint)='4567';
select substr(1234567,3.6::int)='4567';
select substr(1234567,3.6::bigint)='4567';
select substr(1234567,3.6::numeric)='34567';
select substr(1234567,-1)='7';
select substr(1234567,3.6::smallint,2.6)='45';
select substr(1234567,3.6::smallint,2.6::smallint)='456';
select substr(1234567,3.6::smallint,2.6::int)='456';
select substr(1234567,3.6::smallint,2.6::bigint)='456';
select substr(1234567,3.6::smallint,2.6::numeric)='45';
select substr(1234567,3.6::int,2.6::smallint)='456';
select substr(1234567,3.6::int,2.6::int)='456';
select substr(1234567,3.6::int,2.6::bigint)='456';
select substr(1234567,3.6::int,2.6::numeric)='45';
select substr(1234567,3.6::bigint,2.6::smallint)='456';
select substr(1234567,3.6::bigint,2.6::int)='456';
select substr(1234567,3.6::bigint,2.6::bigint)='456';
select substr(1234567,3.6::bigint,2.6::numeric)='45';
select substr(1234567,3.6::numeric,2.6::smallint)='345';
select substr(1234567,3.6::numeric,2.6::int)='345';
select substr(1234567,3.6::numeric,2.6::bigint)='345';
select substr(1234567,3.6::numeric,2.6::numeric)='34';
select substr('abcdef'::varchar,3.6::smallint)='def';
select substr('abcdef'::varchar,3.6::int)='def';
select substr('abcdef'::varchar,3.6::bigint)='def';
select substr('abcdef'::varchar,3.6::numeric)='cdef';
select substr('abcdef'::varchar,3.5::int,3.5::int)='def';
select substr('abcdef'::varchar,3.5::numeric,3.5::numeric)='cde';
select substr('abcdef'::varchar,3.5::numeric,3.5::int)='cdef';

-- test lengthb
select length('oracle'),lengthB('oracle') from dual;

-- test strposb
select strposb('abc', '') from dual;
select strposb('abc', 'a') from dual;
select strposb('abc', 'c') from dual;
select strposb('abc', 'z') from dual;
select strposb('abcabcabc', 'abca') from dual;

--test decode
select decode(1, 1, 100, 2, 200);
select decode(2, 1, 100, 2, 200);
select decode(3, 1, 100, 2, 200);
select decode(3, 1, 100, 2, 200, 300);
select decode(NULL, 1, 100, NULL, 200, 300);
select decode('1'::text, '1', 100, '2', 200);
select decode(2, 1, 'ABC', 2, 'DEF');
select decode('2009-02-05'::date, '2009-02-05', 'ok');
select decode('2009-02-05 01:02:03'::timestamp, '2009-02-05 01:02:03', 'ok');

-- For type 'bpchar'
select decode('a'::bpchar, 'a'::bpchar,'postgres'::bpchar);
select decode('c'::bpchar, 'a'::bpchar,'postgres'::bpchar);
select decode('a'::bpchar, 'a'::bpchar,'postgres'::bpchar,'default value'::bpchar);
select decode('c', 'a'::bpchar,'postgres'::bpchar,'default value'::bpchar);

select decode('a'::bpchar, 'a'::bpchar,'postgres'::bpchar,'b'::bpchar,'database'::bpchar);
select decode('d'::bpchar, 'a'::bpchar,'postgres'::bpchar,'b'::bpchar,'database'::bpchar);
select decode('a'::bpchar, 'a'::bpchar,'postgres'::bpchar,'b'::bpchar,'database'::bpchar,'default value'::bpchar);
select decode('d'::bpchar, 'a'::bpchar,'postgres'::bpchar,'b'::bpchar,'database'::bpchar,'default value'::bpchar);

select decode('a'::bpchar, 'a'::bpchar,'postgres'::bpchar,'b'::bpchar,'database'::bpchar, 'c'::bpchar, 'system'::bpchar);
select decode('d'::bpchar, 'a'::bpchar,'postgres'::bpchar,'b'::bpchar,'database'::bpchar, 'c'::bpchar, 'system'::bpchar);
select decode('a'::bpchar, 'a'::bpchar,'postgres'::bpchar,'b'::bpchar,'database'::bpchar, 'c'::bpchar, 'system'::bpchar,'default value'::bpchar);
select decode('d'::bpchar, 'a'::bpchar,'postgres'::bpchar,'b'::bpchar,'database'::bpchar, 'c'::bpchar, 'system'::bpchar,'default value'::bpchar);

select decode(NULL, 'a'::bpchar, 'postgres'::bpchar, NULL,'database'::bpchar);
select decode(NULL, 'a'::bpchar, 'postgres'::bpchar, 'b'::bpchar,'database'::bpchar);
select decode(NULL, 'a'::bpchar, 'postgres'::bpchar, NULL,'database'::bpchar,'default value'::bpchar);
select decode(NULL, 'a'::bpchar, 'postgres'::bpchar, 'b'::bpchar,'database'::bpchar,'default value'::bpchar);

-- For type 'bigint'
select decode(2147483651::bigint, 2147483650::bigint,2147483650::bigint);
select decode(2147483653::bigint, 2147483651::bigint,2147483650::bigint);
select decode(2147483653::bigint, 2147483651::bigint,2147483650::bigint,9999999999::bigint);
select decode(2147483653::bigint, 2147483651::bigint,2147483650::bigint,9999999999::bigint);

select decode(2147483651::bigint, 2147483651::bigint,2147483650::bigint,2147483652::bigint,2147483651::bigint);
select decode(2147483654::bigint, 2147483651::bigint,2147483650::bigint,2147483652::bigint,2147483651::bigint);
select decode(2147483651::bigint, 2147483651::bigint,2147483650::bigint,2147483652::bigint,2147483651::bigint,9999999999::bigint);
select decode(2147483654::bigint, 2147483651::bigint,2147483650::bigint,2147483652::bigint,2147483651::bigint,9999999999::bigint);

select decode(2147483651::bigint, 2147483651::bigint,2147483650::bigint, 2147483652::bigint,2147483651::bigint, 2147483653::bigint, 2147483652::bigint);
select decode(2147483654::bigint, 2147483651::bigint,2147483650::bigint, 2147483652::bigint,2147483651::bigint, 2147483653::bigint, 2147483652::bigint);
select decode(2147483651::bigint, 2147483651::bigint,2147483650::bigint, 2147483652::bigint,2147483651::bigint, 2147483653::bigint, 2147483652::bigint,9999999999::bigint);
select decode(2147483654::bigint, 2147483651::bigint,2147483650::bigint, 2147483652::bigint,2147483651::bigint, 2147483653::bigint, 2147483652::bigint,9999999999::bigint);

select decode(NULL, 2147483651::bigint, 2147483650::bigint, NULL,2147483651::bigint);
select decode(NULL, 2147483651::bigint, 2147483650::bigint, 2147483652::bigint,2147483651::bigint);
select decode(NULL, 2147483651::bigint, 2147483650::bigint, NULL,2147483651::bigint,9999999999::bigint);
select decode(NULL, 2147483651::bigint, 2147483650::bigint, 2147483652::bigint,2147483651::bigint,9999999999::bigint);

-- For type 'numeric'
select decode(12.001::numeric(5,3), 12.001::numeric(5,3),214748.3650::numeric(10,4));
select decode(12.003::numeric(5,3), 12.001::numeric(5,3),214748.3650::numeric(10,4));
select decode(12.001::numeric(5,3), 12.001::numeric(5,3),214748.3650::numeric(10,4),999999.9999::numeric(10,4));
select decode(12.003::numeric(5,3), 12.001::numeric(5,3),214748.3650::numeric(10,4),999999.9999::numeric(10,4));

select decode(12.001::numeric(5,3), 12.001::numeric(5,3),214748.3650::numeric(10,4),12.002::numeric(5,3),214748.3651::numeric(10,4));
select decode(12.004::numeric(5,3), 12.001::numeric(5,3),214748.3650::numeric(10,4),12.002::numeric(5,3),214748.3651::numeric(10,4));
select decode(12.001::numeric(5,3), 12.001::numeric(5,3),214748.3650::numeric(10,4),12.002::numeric(5,3),214748.3651::numeric(10,4),999999.9999::numeric(10,4));
select decode(12.004::numeric(5,3), 12.001::numeric(5,3),214748.3650::numeric(10,4),12.002::numeric(5,3),214748.3651::numeric(10,4),999999.9999::numeric(10,4));

select decode(12.001::numeric(5,3), 12.001::numeric(5,3),214748.3650::numeric(10,4),12.002::numeric(5,3),214748.3651::numeric(10,4), 12.003::numeric(5,3), 214748.3652::numeric(10,4));
select decode(12.004::numeric(5,3), 12.001::numeric(5,3),214748.3650::numeric(10,4),12.002::numeric(5,3),214748.3651::numeric(10,4), 12.003::numeric(5,3), 214748.3652::numeric(10,4));
select decode(12.001::numeric(5,3), 12.001::numeric(5,3),214748.3650::numeric(10,4),12.002::numeric(5,3),214748.3651::numeric(10,4), 12.003::numeric(5,3), 214748.3652::numeric(10,4),999999.9999::numeric(10,4));
select decode(12.004::numeric(5,3), 12.001::numeric(5,3),214748.3650::numeric(10,4),12.002::numeric(5,3),214748.3651::numeric(10,4), 12.003::numeric(5,3), 214748.3652::numeric(10,4),999999.9999::numeric(10,4));

select decode(NULL, 12.001::numeric(5,3), 214748.3650::numeric(10,4), NULL,214748.3651::numeric(10,4));
select decode(NULL, 12.001::numeric(5,3), 214748.3650::numeric(10,4), 12.002::numeric(5,3),214748.3651::numeric(10,4));
select decode(NULL, 12.001::numeric(5,3), 214748.3650::numeric(10,4), NULL,214748.3651::numeric(10,4),999999.9999::numeric(10,4));
select decode(NULL, 12.001::numeric(5,3), 214748.3650::numeric(10,4), 12.002::numeric(5,3),214748.3651::numeric(10,4),999999.9999::numeric(10,4));

--For type 'date'
select decode('2020-01-01'::date, '2020-01-01'::date,'2012-12-20'::date);
select decode('2020-01-03'::date, '2020-01-01'::date,'2012-12-20'::date);
select decode('2020-01-01'::date, '2020-01-01'::date,'2012-12-20'::date,'2012-12-21'::date);
select decode('2020-01-03'::date, '2020-01-01'::date,'2012-12-20'::date,'2012-12-21'::date);

select decode('2020-01-01'::date, '2020-01-01'::date,'2012-12-20'::date,'2020-01-02'::date,'2012-12-21'::date);
select decode('2020-01-04'::date, '2020-01-01'::date,'2012-12-20'::date,'2020-01-02'::date,'2012-12-21'::date);
select decode('2020-01-01'::date, '2020-01-01'::date,'2012-12-20'::date,'2020-01-02'::date,'2012-12-21'::date,'2012-12-31'::date);
select decode('2020-01-04'::date, '2020-01-01'::date,'2012-12-20'::date,'2020-01-02'::date,'2012-12-21'::date,'2012-12-31'::date);

select decode('2020-01-01'::date, '2020-01-01'::date,'2012-12-20'::date,'2020-01-02'::date,'2012-12-21'::date, '2020-01-03'::date, '2012-12-31'::date);
select decode('2020-01-04'::date, '2020-01-01'::date,'2012-12-20'::date,'2020-01-02'::date,'2012-12-21'::date, '2020-01-03'::date, '2012-12-31'::date);
select decode('2020-01-01'::date, '2020-01-01'::date,'2012-12-20'::date,'2020-01-02'::date,'2012-12-21'::date, '2020-01-03'::date, '2012-12-31'::date,'2013-01-01'::date);
select decode('2020-01-04'::date, '2020-01-01'::date,'2012-12-20'::date,'2020-01-02'::date,'2012-12-21'::date, '2020-01-03'::date, '2012-12-31'::date,'2013-01-01'::date);

select decode(NULL, '2020-01-01'::date, '2012-12-20'::date, NULL,'2012-12-21'::date);
select decode(NULL, '2020-01-01'::date, '2012-12-20'::date, '2020-01-02'::date,'2012-12-21'::date);
select decode(NULL, '2020-01-01'::date, '2012-12-20'::date, NULL,'2012-12-21'::date,'2012-12-31'::date);
select decode(NULL, '2020-01-01'::date, '2012-12-20'::date, '2020-01-02'::date,'2012-12-21'::date,'2012-12-31'::date);

-- For type 'time'
select decode('01:00:01'::time, '01:00:01'::time,'09:00:00'::time);
select decode('01:00:03'::time, '01:00:01'::time,'09:00:00'::time);
select decode('01:00:01'::time, '01:00:01'::time,'09:00:00'::time,'00:00:00'::time);
select decode('01:00:03'::time, '01:00:01'::time,'09:00:00'::time,'00:00:00'::time);

select decode('01:00:01'::time, '01:00:01'::time,'09:00:00'::time,'01:00:02'::time,'12:00:00'::time);
select decode('01:00:04'::time, '01:00:01'::time,'09:00:00'::time,'01:00:02'::time,'12:00:00'::time);
select decode('01:00:01'::time, '01:00:01'::time,'09:00:00'::time,'01:00:02'::time,'12:00:00'::time,'00:00:00'::time);
select decode('01:00:04'::time, '01:00:01'::time,'09:00:00'::time,'01:00:01'::time,'12:00:00'::time,'00:00:00'::time);

select decode('01:00:01'::time, '01:00:01'::time,'09:00:00'::time,'01:00:02'::time,'12:00:00'::time, '01:00:03'::time, '15:00:00'::time);
select decode('01:00:04'::time, '01:00:01'::time,'09:00:00'::time,'01:00:02'::time,'12:00:00'::time, '01:00:03'::time, '15:00:00'::time);
select decode('01:00:01'::time, '01:00:01'::time,'09:00:00'::time,'01:00:02'::time,'12:00:00'::time, '01:00:03'::time, '15:00:00'::time,'00:00:00'::time);
select decode('01:00:04'::time, '01:00:01'::time,'09:00:00'::time,'01:00:02'::time,'12:00:00'::time, '01:00:03'::time, '15:00:00'::time,'00:00:00'::time);

select decode(NULL, '01:00:01'::time, '09:00:00'::time, NULL,'12:00:00'::time);
select decode(NULL, '01:00:01'::time, '09:00:00'::time, '01:00:02'::time,'12:00:00'::time);
select decode(NULL, '01:00:01'::time, '09:00:00'::time, NULL,'12:00:00'::time,'00:00:00'::time);
select decode(NULL, '01:00:01'::time, '09:00:00'::time, '01:00:02'::time,'12:00:00'::time,'00:00:00'::time);

-- For type 'timestamp'
select decode('2020-01-01 01:00:01'::timestamp, '2020-01-01 01:00:01'::timestamp,'2012-12-20 09:00:00'::timestamp);
select decode('2020-01-03 01:00:01'::timestamp, '2020-01-01 01:00:01'::timestamp,'2012-12-20 09:00:00'::timestamp);
select decode('2020-01-01 01:00:01'::timestamp, '2020-01-01 01:00:01'::timestamp,'2012-12-20 09:00:00'::timestamp,'2012-12-20 00:00:00'::timestamp);
select decode('2020-01-03 01:00:01'::timestamp, '2020-01-01 01:00:01'::timestamp,'2012-12-20 09:00:00'::timestamp,'2012-12-20 00:00:00'::timestamp);

select decode('2020-01-01 01:00:01'::timestamp, '2020-01-01 01:00:01'::timestamp,'2012-12-20 09:00:00'::timestamp,'2020-01-02 01:00:01'::timestamp,'2012-12-20 12:00:00'::timestamp);
select decode('2020-01-04 01:00:01'::timestamp, '2020-01-01 01:00:01'::timestamp,'2012-12-20 09:00:00'::timestamp,'2020-01-02 01:00:01'::timestamp,'2012-12-20 12:00:00'::timestamp);
select decode('2020-01-01 01:00:01'::timestamp, '2020-01-01 01:00:01'::timestamp,'2012-12-20 09:00:00'::timestamp,'2020-01-02 01:00:01'::timestamp,'2012-12-20 12:00:00'::timestamp,'2012-12-20 00:00:00'::timestamp);
select decode('2020-01-04 01:00:01'::timestamp, '2020-01-01 01:00:01'::timestamp,'2012-12-20 09:00:00'::timestamp,'2020-01-02 01:00:01'::timestamp,'2012-12-20 12:00:00'::timestamp,'2012-12-20 00:00:00'::timestamp);

select decode('2020-01-01 01:00:01'::timestamp, '2020-01-01 01:00:01'::timestamp,'2012-12-20 09:00:00'::timestamp,'2020-01-02 01:00:01'::timestamp,'2012-12-20 12:00:00'::timestamp, '2020-01-03 01:00:01'::timestamp, '2012-12-20 15:00:00'::timestamp);
select decode('2020-01-04 01:00:01'::timestamp, '2020-01-01 01:00:01'::timestamp,'2012-12-20 09:00:00'::timestamp,'2020-01-02 01:00:01'::timestamp,'2012-12-20 12:00:00'::timestamp, '2020-01-03 01:00:01'::timestamp, '2012-12-20 15:00:00'::timestamp);
select decode('2020-01-01 01:00:01'::timestamp, '2020-01-01 01:00:01'::timestamp,'2012-12-20 09:00:00'::timestamp,'2020-01-02 01:00:01'::timestamp,'2012-12-20 12:00:00'::timestamp, '2020-01-03 01:00:01'::timestamp, '2012-12-20 15:00:00'::timestamp,'2012-12-20 00:00:00'::timestamp);
select decode('2020-01-04 01:00:01'::timestamp, '2020-01-01 01:00:01'::timestamp,'2012-12-20 09:00:00'::timestamp,'2020-01-02 01:00:01'::timestamp,'2012-12-20 12:00:00'::timestamp, '2020-01-03 01:00:01'::timestamp, '2012-12-20 15:00:00'::timestamp,'2012-12-20 00:00:00'::timestamp);

select decode(NULL, '2020-01-01 01:00:01'::timestamp, '2012-12-20 09:00:00'::timestamp, NULL,'2012-12-20 12:00:00'::timestamp);
select decode(NULL, '2020-01-01 01:00:01'::timestamp, '2012-12-20 09:00:00'::timestamp, '2020-01-02 01:00:01'::timestamp,'2012-12-20 12:00:00'::timestamp);
select decode(NULL, '2020-01-01 01:00:01'::timestamp, '2012-12-20 09:00:00'::timestamp, NULL,'2012-12-20 12:00:00'::timestamp,'2012-12-20 00:00:00'::timestamp);
select decode(NULL, '2020-01-01 01:00:01'::timestamp, '2012-12-20 09:00:00'::timestamp, '2020-01-02 01:00:01'::timestamp,'2012-12-20 12:00:00'::timestamp,'2012-12-20 00:00:00'::timestamp);

-- For type 'timestamptz'
select decode('2020-01-01 01:00:01-08'::timestamptz, '2020-01-01 01:00:01-08'::timestamptz,'2012-12-20 09:00:00-08'::timestamptz);
select decode('2020-01-03 01:00:01-08'::timestamptz, '2020-01-01 01:00:01-08'::timestamptz,'2012-12-20 09:00:00-08'::timestamptz);
select decode('2020-01-01 01:00:01-08'::timestamptz, '2020-01-01 01:00:01-08'::timestamptz,'2012-12-20 09:00:00-08'::timestamptz,'2012-12-20 00:00:00-08'::timestamptz);
select decode('2020-01-03 01:00:01-08'::timestamptz, '2020-01-01 01:00:01-08'::timestamptz,'2012-12-20 09:00:00-08'::timestamptz,'2012-12-20 00:00:00-08'::timestamptz);

select decode('2020-01-01 01:00:01-08'::timestamptz, '2020-01-01 01:00:01-08'::timestamptz,'2012-12-20 09:00:00-08'::timestamptz,'2020-01-02 01:00:01-08'::timestamptz,'2012-12-20 12:00:00-08'::timestamptz);
select decode('2020-01-04 01:00:01-08'::timestamptz, '2020-01-01 01:00:01-08'::timestamptz,'2012-12-20 09:00:00-08'::timestamptz,'2020-01-02 01:00:01-08'::timestamptz,'2012-12-20 12:00:00-08'::timestamptz);
select decode('2020-01-01 01:00:01-08'::timestamptz, '2020-01-01 01:00:01-08'::timestamptz,'2012-12-20 09:00:00-08'::timestamptz,'2020-01-02 01:00:01-08'::timestamptz,'2012-12-20 12:00:00-08'::timestamptz,'2012-12-20 00:00:00-08'::timestamptz);
select decode('2020-01-04 01:00:01-08'::timestamptz, '2020-01-01 01:00:01-08'::timestamptz,'2012-12-20 09:00:00-08'::timestamptz,'2020-01-02 01:00:01-08'::timestamptz,'2012-12-20 12:00:00-08'::timestamptz,'2012-12-20 00:00:00-08'::timestamptz);

select decode('2020-01-01 01:00:01-08'::timestamptz, '2020-01-01 01:00:01-08'::timestamptz,'2012-12-20 09:00:00-08'::timestamptz,'2020-01-02 01:00:01-08'::timestamptz,'2012-12-20 12:00:00-08'::timestamptz, '2020-01-03 01:00:01-08'::timestamptz, '2012-12-20 15:00:00-08'::timestamptz);
select decode('2020-01-04 01:00:01-08'::timestamptz, '2020-01-01 01:00:01-08'::timestamptz,'2012-12-20 09:00:00-08'::timestamptz,'2020-01-02 01:00:01-08'::timestamptz,'2012-12-20 12:00:00-08'::timestamptz, '2020-01-03 01:00:01-08'::timestamptz, '2012-12-20 15:00:00-08'::timestamptz);
select decode('2020-01-01 01:00:01-08'::timestamptz, '2020-01-01 01:00:01-08'::timestamptz,'2012-12-20 09:00:00-08'::timestamptz,'2020-01-02 01:00:01-08'::timestamptz,'2012-12-20 12:00:00-08'::timestamptz, '2020-01-03 01:00:01-08'::timestamptz, '2012-12-20 15:00:00-08'::timestamptz,'2012-12-20 00:00:00-08'::timestamptz);
select decode(4, 1,'2012-12-20 09:00:00-08'::timestamptz,2,'2012-12-20 12:00:00-08'::timestamptz, 3, '2012-12-20 15:00:00-08'::timestamptz,'2012-12-20 00:00:00-08'::timestamptz);

select decode(NULL, '2020-01-01 01:00:01-08'::timestamptz, '2012-12-20 09:00:00-08'::timestamptz, NULL,'2012-12-20 12:00:00-08'::timestamptz);
select decode(NULL, '2020-01-01 01:00:01-08'::timestamptz, '2012-12-20 09:00:00-08'::timestamptz, '2020-01-02 01:00:01-08'::timestamptz,'2012-12-20 12:00:00-08'::timestamptz);
select decode(NULL, '2020-01-01 01:00:01-08'::timestamptz, '2012-12-20 09:00:00-08'::timestamptz, NULL,'2012-12-20 12:00:00-08'::timestamptz,'2012-12-20 00:00:00-08'::timestamptz);
select decode(NULL, '2020-01-01 01:00:01-08'::timestamptz, '2012-12-20 09:00:00-08'::timestamptz, '2020-01-02 01:00:01-08'::timestamptz,'2012-12-20 12:00:00-08'::timestamptz,'2012-12-20 00:00:00-08'::timestamptz);

select decode(1, 1,'2011-12-20 09:00:00-08',2,'2012-12-20 12:00:00-08', 3, '2013-12-20 15:00:00-08', 4, '2014-12-20 00:00:00-08', 5,'2015-12-20 00:00:00-08',6 ,'2016-12-20 00:00:00-08', 7 ,'2017-12-20 00:00:00-08', 8 ,'2012-18-20 00:00:00-08', 9 ,'2019-12-20 00:00:00-08',  10 ,'2020-12-20 00:00:00-08',  11 ,'2021-12-20 00:00:00-08',  12 ,'2022-12-20 00:00:00-08',  13 ,'20123-12-20 00:00:00-08',  14 ,'2024-12-20 00:00:00-08',  15 ,'2025-12-20 00:00:00-08', 16,'2026-12-20 00:00:00-08', 17,'2027-12-20 00:00:00-08', 18,'2028-12-20 00:00:00-08', 19,'2029-12-20 00:00:00-08', 'default');
select decode(2, 1,'2011-12-20 09:00:00-08',2,'2012-12-20 12:00:00-08', 3, '2013-12-20 15:00:00-08', 4, '2014-12-20 00:00:00-08', 5,'2015-12-20 00:00:00-08',6 ,'2016-12-20 00:00:00-08', 7 ,'2017-12-20 00:00:00-08', 8 ,'2012-18-20 00:00:00-08', 9 ,'2019-12-20 00:00:00-08',  10 ,'2020-12-20 00:00:00-08',  11 ,'2021-12-20 00:00:00-08',  12 ,'2022-12-20 00:00:00-08',  13 ,'20123-12-20 00:00:00-08',  14 ,'2024-12-20 00:00:00-08',  15 ,'2025-12-20 00:00:00-08', 16,'2026-12-20 00:00:00-08', 17,'2027-12-20 00:00:00-08', 18,'2028-12-20 00:00:00-08', 19,'2029-12-20 00:00:00-08', 'default');
select decode(19, 1,'2011-12-20 09:00:00-08',2,'2012-12-20 12:00:00-08', 3, '2013-12-20 15:00:00-08', 4, '2014-12-20 00:00:00-08', 5,'2015-12-20 00:00:00-08',6 ,'2016-12-20 00:00:00-08', 7 ,'2017-12-20 00:00:00-08', 8 ,'2012-18-20 00:00:00-08', 9 ,'2019-12-20 00:00:00-08',  10 ,'2020-12-20 00:00:00-08',  11 ,'2021-12-20 00:00:00-08',  12 ,'2022-12-20 00:00:00-08',  13 ,'20123-12-20 00:00:00-08',  14 ,'2024-12-20 00:00:00-08',  15 ,'2025-12-20 00:00:00-08', 16,'2026-12-20 00:00:00-08', 17,'2027-12-20 00:00:00-08', 18,'2028-12-20 00:00:00-08', 19,'2029-12-20 00:00:00-08', 'default');
select decode(20, 1,'2011-12-20 09:00:00-08',2,'2012-12-20 12:00:00-08', 3, '2013-12-20 15:00:00-08', 4, '2014-12-20 00:00:00-08', 5,'2015-12-20 00:00:00-08',6 ,'2016-12-20 00:00:00-08', 7 ,'2017-12-20 00:00:00-08', 8 ,'2012-18-20 00:00:00-08', 9 ,'2019-12-20 00:00:00-08',  10 ,'2020-12-20 00:00:00-08',  11 ,'2021-12-20 00:00:00-08',  12 ,'2022-12-20 00:00:00-08',  13 ,'20123-12-20 00:00:00-08',  14 ,'2024-12-20 00:00:00-08',  15 ,'2025-12-20 00:00:00-08', 16,'2026-12-20 00:00:00-08', 17,'2027-12-20 00:00:00-08', 18,'2028-12-20 00:00:00-08', 19,'2029-12-20 00:00:00-08', 'default');
select decode(21, 1,'2011-12-20 09:00:00-08',2,'2012-12-20 12:00:00-08', 3, '2013-12-20 15:00:00-08', 4, '2014-12-20 00:00:00-08', 5,'2015-12-20 00:00:00-08',6 ,'2016-12-20 00:00:00-08', 7 ,'2017-12-20 00:00:00-08', 8 ,'2012-18-20 00:00:00-08', 9 ,'2019-12-20 00:00:00-08',  10 ,'2020-12-20 00:00:00-08',  11 ,'2021-12-20 00:00:00-08',  12 ,'2022-12-20 00:00:00-08',  13 ,'20123-12-20 00:00:00-08',  14 ,'2024-12-20 00:00:00-08',  15 ,'2025-12-20 00:00:00-08', 16,'2026-12-20 00:00:00-08', 17,'2027-12-20 00:00:00-08', 18,'2028-12-20 00:00:00-08', 19,'2029-12-20 00:00:00-08', 'default');
select decode(22, 1,'2011-12-20 09:00:00-08',2,'2012-12-20 12:00:00-08', 3, '2013-12-20 15:00:00-08', 4, '2014-12-20 00:00:00-08', 5,'2015-12-20 00:00:00-08',6 ,'2016-12-20 00:00:00-08', 7 ,'2017-12-20 00:00:00-08', 8 ,'2012-18-20 00:00:00-08', 9 ,'2019-12-20 00:00:00-08',  10 ,'2020-12-20 00:00:00-08',  11 ,'2021-12-20 00:00:00-08',  12 ,'2022-12-20 00:00:00-08',  13 ,'20123-12-20 00:00:00-08',  14 ,'2024-12-20 00:00:00-08',  15 ,'2025-12-20 00:00:00-08', 16,'2026-12-20 00:00:00-08', 17,'2027-12-20 00:00:00-08', 18,'2028-12-20 00:00:00-08', 19,'2029-12-20 00:00:00-08', 'default');
select decode(23, 1,'2011-12-20 09:00:00-08',2,'2012-12-20 12:00:00-08', 3, '2013-12-20 15:00:00-08', 4, '2014-12-20 00:00:00-08', 5,'2015-12-20 00:00:00-08',6 ,'2016-12-20 00:00:00-08', 7 ,'2017-12-20 00:00:00-08', 8 ,'2012-18-20 00:00:00-08', 9 ,'2019-12-20 00:00:00-08',  10 ,'2020-12-20 00:00:00-08',  11 ,'2021-12-20 00:00:00-08',  12 ,'2022-12-20 00:00:00-08',  13 ,'20123-12-20 00:00:00-08',  14 ,'2024-12-20 00:00:00-08',  15 ,'2025-12-20 00:00:00-08', 16,'2026-12-20 00:00:00-08', 17,'2027-12-20 00:00:00-08', 18,'2028-12-20 00:00:00-08', 19,'2029-12-20 00:00:00-08', 'default');
select decode(20, 1,'2011-12-20 09:00:00-08',2,'2012-12-20 12:00:00-08', 3, '2013-12-20 15:00:00-08', 4, '2014-12-20 00:00:00-08', 5,'2015-12-20 00:00:00-08',6 ,'2016-12-20 00:00:00-08', 7 ,'2017-12-20 00:00:00-08', 8 ,'2012-18-20 00:00:00-08', 9 ,'2019-12-20 00:00:00-08',  10 ,'2020-12-20 00:00:00-08',  11 ,'2021-12-20 00:00:00-08',  12 ,'2022-12-20 00:00:00-08',  13 ,'20123-12-20 00:00:00-08',  14 ,'2024-12-20 00:00:00-08',  15 ,'2025-12-20 00:00:00-08', 16,'2026-12-20 00:00:00-08', 17,'2027-12-20 00:00:00-08', 18,'2028-12-20 00:00:00-08', 19,'2029-12-20 00:00:00-08');
select decode(21, 1,'2011-12-20 09:00:00-08',2,'2012-12-20 12:00:00-08', 3, '2013-12-20 15:00:00-08', 4, '2014-12-20 00:00:00-08', 5,'2015-12-20 00:00:00-08',6 ,'2016-12-20 00:00:00-08', 7 ,'2017-12-20 00:00:00-08', 8 ,'2012-18-20 00:00:00-08', 9 ,'2019-12-20 00:00:00-08',  10 ,'2020-12-20 00:00:00-08',  11 ,'2021-12-20 00:00:00-08',  12 ,'2022-12-20 00:00:00-08',  13 ,'20123-12-20 00:00:00-08',  14 ,'2024-12-20 00:00:00-08',  15 ,'2025-12-20 00:00:00-08', 16,'2026-12-20 00:00:00-08', 17,'2027-12-20 00:00:00-08', 18,'2028-12-20 00:00:00-08', 19,'2029-12-20 00:00:00-08');
select decode(22, 1,'2011-12-20 09:00:00-08',2,'2012-12-20 12:00:00-08', 3, '2013-12-20 15:00:00-08', 4, '2014-12-20 00:00:00-08', 5,'2015-12-20 00:00:00-08',6 ,'2016-12-20 00:00:00-08', 7 ,'2017-12-20 00:00:00-08', 8 ,'2012-18-20 00:00:00-08', 9 ,'2019-12-20 00:00:00-08',  10 ,'2020-12-20 00:00:00-08',  11 ,'2021-12-20 00:00:00-08',  12 ,'2022-12-20 00:00:00-08',  13 ,'20123-12-20 00:00:00-08',  14 ,'2024-12-20 00:00:00-08',  15 ,'2025-12-20 00:00:00-08', 16,'2026-12-20 00:00:00-08', 17,'2027-12-20 00:00:00-08', 18,'2028-12-20 00:00:00-08', 19,'2029-12-20 00:00:00-08');
select decode(23, 1,'2011-12-20 09:00:00-08',2,'2012-12-20 12:00:00-08', 3, '2013-12-20 15:00:00-08', 4, '2014-12-20 00:00:00-08', 5,'2015-12-20 00:00:00-08',6 ,'2016-12-20 00:00:00-08', 7 ,'2017-12-20 00:00:00-08', 8 ,'2012-18-20 00:00:00-08', 9 ,'2019-12-20 00:00:00-08',  10 ,'2020-12-20 00:00:00-08',  11 ,'2021-12-20 00:00:00-08',  12 ,'2022-12-20 00:00:00-08',  13 ,'20123-12-20 00:00:00-08',  14 ,'2024-12-20 00:00:00-08',  15 ,'2025-12-20 00:00:00-08', 16,'2026-12-20 00:00:00-08', 17,'2027-12-20 00:00:00-08', 18,'2028-12-20 00:00:00-08', 19,'2029-12-20 00:00:00-08');

-- test lpad
SELECT '|' || lpad('X123bcd'::char(8), 10) || '|';
SELECT '|' || lpad('X123bcd'::char(8),  5) || '|';
SELECT '|' || lpad('X123bcd'::char(8), 1) || '|';

SELECT '|' || lpad('X123bcd'::char(5), 10, '321X'::char(3)) || '|';
SELECT '|' || lpad('X123bcd'::char(5),  5, '321X'::char(3)) || '|';

SELECT '|' || lpad('X123bcd'::char(5), 10, '321X'::text) || '|';
SELECT '|' || lpad('X123bcd'::char(5), 10, '321X'::varchar2(5)) || '|';
SELECT '|' || lpad('X123bcd'::char(5), 10, '321X'::nvarchar2(3)) || '|';

SELECT '|' || lpad('X123bcd'::text, 10, '321X'::char(3)) || '|';
SELECT '|' || lpad('X123bcd'::text,  5, '321X'::char(3)) || '|';

SELECT '|' || lpad('X123bcd'::varchar2(5), 10, '321X'::char(3)) || '|';
SELECT '|' || lpad('X123bcd'::varchar2(5),  5, '321X'::char(3)) || '|';

SELECT '|' || lpad('X123bcd'::nvarchar2(5), 10, '321X'::char(3)) || '|';
SELECT '|' || lpad('X123bcd'::nvarchar2(5),  5, '321X'::char(3)) || '|';

SELECT '|' || lpad('X123bcd'::text, 10) || '|';
SELECT '|' || lpad('X123bcd'::text,  5) || '|';

SELECT '|' || lpad('X123bcd'::varchar2(10), 10) || '|';
SELECT '|' || lpad('X123bcd'::varchar2(10), 5) || '|';

SELECT '|' || lpad('X123bcd'::nvarchar2(10), 10) || '|';
SELECT '|' || lpad('X123bcd'::nvarchar2(10), 5) || '|';

SELECT '|' || lpad('X123bcd'::text, 10, '321X'::text) || '|';
SELECT '|' || lpad('X123bcd'::text, 10, '321X'::varchar2(5)) || '|';
SELECT '|' || lpad('X123bcd'::text, 10, '321X'::nvarchar2(3)) || '|';

SELECT '|' || lpad('X123bcd'::varchar2(5), 10, '321X'::text) || '|';
SELECT '|' || lpad('X123bcd'::varchar2(5), 10, '321X'::varchar2(5)) || '|';
SELECT '|' || lpad('X123bcd'::varchar2(5), 10, '321X'::nvarchar2(5)) || '|';

SELECT '|' || lpad('X123bcd'::nvarchar2(5), 10, '321X'::text) || '|';
SELECT '|' || lpad('X123bcd'::nvarchar2(5), 10, '321X'::varchar2(5)) || '|';
SELECT '|' || lpad('X123bcd'::nvarchar2(5), 10, '321X'::nvarchar2(5)) || '|';

-- test number like text
select 9999 like '9%' from dual;
select 9999 like '09%' from dual;
select 9999 ilike '9%' from dual;
select 9999 ilike '09%' from dual;
select 9999 not like '9%' from dual;
select 9999 not like '09%' from dual;
select 9999 not ilike '9%' from dual;
select 9999 not ilike '09%' from dual;

reset enable_oracle_compatible;
reset client_min_messages;
reset datestyle;
reset intervalstyle;
reset client_encoding;


