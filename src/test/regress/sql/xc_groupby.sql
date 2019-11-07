-- this file contains tests for GROUP BY with combinations of following
-- 1. enable_hashagg = on/off (to force the grouping by sorting)
-- 2. distributed or replicated tables across the datanodes
-- If a testcase is added to any of the combinations, please check if it's
-- applicable in other combinations as well.

-- Since we want to test the plan reduction of GROUP and AGG nodes, disable fast
-- query shipping
set enable_fast_query_shipping to off;

-- Combination 1: enable_hashagg on and distributed tables
set enable_hashagg to on;
-- create required tables and fill them with data
create table xc_groupby_tab1 (val int, val2 int);
create table xc_groupby_tab2 (val int, val2 int);
insert into xc_groupby_tab1 values (1, 1), (2, 1), (3, 1), (2, 2), (6, 2), (4, 3), (1, 3), (6, 3);
insert into xc_groupby_tab2 values (1, 1), (4, 1), (8, 1), (2, 4), (9, 4), (3, 4), (4, 2), (5, 2), (3, 2);
select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from xc_groupby_tab1 group by val2 order by 1, 2;
explain (verbose true, costs false, nodes false) select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from xc_groupby_tab1 group by val2 order by 1, 2;
explain (verbose true, costs false, nodes false) select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from xc_groupby_tab1 group by val2;
-- joins and group by
select count(*), sum(xc_groupby_tab1.val * xc_groupby_tab2.val), avg(xc_groupby_tab1.val*xc_groupby_tab2.val), sum(xc_groupby_tab1.val*xc_groupby_tab2.val)::float8/count(*), xc_groupby_tab1.val2, xc_groupby_tab2.val2 from xc_groupby_tab1 full outer join xc_groupby_tab2 on xc_groupby_tab1.val2 = xc_groupby_tab2.val2 group by xc_groupby_tab1.val2, xc_groupby_tab2.val2 order by count(*);
explain (verbose true, costs false, nodes false) select count(*), sum(xc_groupby_tab1.val * xc_groupby_tab2.val), avg(xc_groupby_tab1.val*xc_groupby_tab2.val), sum(xc_groupby_tab1.val*xc_groupby_tab2.val)::float8/count(*), xc_groupby_tab1.val2, xc_groupby_tab2.val2 from xc_groupby_tab1 full outer join xc_groupby_tab2 on xc_groupby_tab1.val2 = xc_groupby_tab2.val2 group by xc_groupby_tab1.val2, xc_groupby_tab2.val2;
-- aggregates over aggregates
select sum(y) from (select sum(val) y, val2%2 x from xc_groupby_tab1 group by val2) q1 group by x order by 1;
explain (verbose true, costs false, nodes false) select sum(y) from (select sum(val) y, val2%2 x from xc_groupby_tab1 group by val2) q1 group by x order by 1;
explain (verbose true, costs false, nodes false) select sum(y) from (select sum(val) y, val2%2 x from xc_groupby_tab1 group by val2) q1 group by x;
-- group by without aggregate
select val2 from xc_groupby_tab1 group by val2 order by val2;
explain (verbose true, costs false, nodes false) select val2 from xc_groupby_tab1 group by val2 order by val2;
explain (verbose true, costs false, nodes false) select val2 from xc_groupby_tab1 group by val2;
select val + val2 from xc_groupby_tab1 group by val + val2 order by 1;
explain (verbose true, costs false, nodes false) select val + val2 from xc_groupby_tab1 group by val + val2 order by 1;
explain (verbose true, costs false, nodes false) select val + val2 from xc_groupby_tab1 group by val + val2;
select val + val2, val, val2 from xc_groupby_tab1 group by val, val2 order by 1, 2, 3;
explain (verbose true, costs false, nodes false) select val + val2, val, val2 from xc_groupby_tab1 group by val, val2 order by 1, 2;
explain (verbose true, costs false, nodes false) select val + val2, val, val2 from xc_groupby_tab1 group by val, val2;
select xc_groupby_tab1.val + xc_groupby_tab2.val2, xc_groupby_tab1.val, xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val, xc_groupby_tab2.val2 order by 1, 2, 3;
explain (verbose true, costs false, nodes false) select xc_groupby_tab1.val + xc_groupby_tab2.val2, xc_groupby_tab1.val, xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val, xc_groupby_tab2.val2 order by 1, 2, 3;
explain (verbose true, costs false, nodes false) select xc_groupby_tab1.val + xc_groupby_tab2.val2, xc_groupby_tab1.val, xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val, xc_groupby_tab2.val2;
select xc_groupby_tab1.val + xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val + xc_groupby_tab2.val2 order by 1;
explain (verbose true, costs false, nodes false) select xc_groupby_tab1.val + xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val + xc_groupby_tab2.val2 order by 1;
explain (verbose true, costs false, nodes false) select xc_groupby_tab1.val + xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val + xc_groupby_tab2.val2;
-- group by with aggregates in expression
select count(*) + sum(val) + avg(val), val2 from xc_groupby_tab1 group by val2 order by 1;
explain (verbose true, costs false, nodes false) select count(*) + sum(val) + avg(val), val2 from xc_groupby_tab1 group by val2 order by 1;
explain (verbose true, costs false, nodes false) select count(*) + sum(val) + avg(val), val2 from xc_groupby_tab1 group by val2;
-- group by with expressions in group by clause
select sum(val), avg(val), 2 * val2 from xc_groupby_tab1 group by 2 * val2 order by 1;
explain (verbose true, costs false, nodes false) select sum(val), avg(val), 2 * val2 from xc_groupby_tab1 group by 2 * val2 order by 1;
explain (verbose true, costs false, nodes false) select sum(val), avg(val), 2 * val2 from xc_groupby_tab1 group by 2 * val2;
drop table xc_groupby_tab1;
drop table xc_groupby_tab2;

-- some tests involving nulls, characters, float type etc.
create table xc_groupby_def(a int, b varchar(25)); 
insert into xc_groupby_def VALUES (NULL, NULL);
insert into xc_groupby_def VALUES (1, NULL);
insert into xc_groupby_def VALUES (NULL, 'One');
insert into xc_groupby_def VALUES (2, 'Two');
insert into xc_groupby_def VALUES (2, 'Two');
insert into xc_groupby_def VALUES (3, 'Three');
insert into xc_groupby_def VALUES (4, 'Three');
insert into xc_groupby_def VALUES (5, 'Three');
insert into xc_groupby_def VALUES (6, 'Two');
insert into xc_groupby_def VALUES (7, NULL);
insert into xc_groupby_def VALUES (8, 'Two');
insert into xc_groupby_def VALUES (9, 'Three');
insert into xc_groupby_def VALUES (10, 'Three');

select a,count(a) from xc_groupby_def group by a order by a;
explain (verbose true, costs false, nodes false) select a,count(a) from xc_groupby_def group by a order by a;
select avg(a) from xc_groupby_def group by a order by 1;
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_def group by a order by 1;
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_def group by a;
select avg(a) from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_def group by b;
select sum(a) from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select sum(a) from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select sum(a) from xc_groupby_def group by b;
select count(*) from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select count(*) from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select count(*) from xc_groupby_def group by b;
select count(*) from xc_groupby_def where a is not null group by a order by 1;
explain (verbose true, costs false, nodes false) select count(*) from xc_groupby_def where a is not null group by a order by 1;
explain (verbose true, costs false, nodes false) select count(*) from xc_groupby_def where a is not null group by a;

select * from (select b from xc_groupby_def group by b) q order by q.b;
explain (verbose true, costs false, nodes false) select * from (select b from xc_groupby_def group by b) q order by q.b;
select * from (select b,count(b) from xc_groupby_def group by b) q order by q.b;
explain (verbose true, costs false, nodes false) select * from (select b,count(b) from xc_groupby_def group by b) q order by q.b;
select count(*) from xc_groupby_def where b is null group by b order by 1;
explain (verbose true, costs false, nodes false) select count(*) from xc_groupby_def where b is null group by b;

create table xc_groupby_g(a int, b float, c numeric);
insert into xc_groupby_g values(1,2.1,3.2);
insert into xc_groupby_g values(1,2.1,3.2);
insert into xc_groupby_g values(2,2.3,5.2);

select sum(a) from xc_groupby_g group by a;
explain (verbose true, costs false, nodes false) select sum(a) from xc_groupby_g group by a;
select sum(b) from xc_groupby_g group by b order by 1;
explain (verbose true, costs false, nodes false) select sum(b) from xc_groupby_g group by b order by 1;
explain (verbose true, costs false, nodes false) select sum(b) from xc_groupby_g group by b;
select sum(c) from xc_groupby_g group by b order by 1;
explain (verbose true, costs false, nodes false) select sum(c) from xc_groupby_g group by b order by 1;
explain (verbose true, costs false, nodes false) select sum(c) from xc_groupby_g group by b;

select avg(a) from xc_groupby_g group by b order by 1;
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_g group by b order by 1;
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_g group by b;
select avg(b) from xc_groupby_g group by c order by 1;
explain (verbose true, costs false, nodes false) select avg(b) from xc_groupby_g group by c order by 1;
explain (verbose true, costs false, nodes false) select avg(b) from xc_groupby_g group by c;
select avg(c) from xc_groupby_g group by c order by 1;
explain (verbose true, costs false, nodes false) select avg(c) from xc_groupby_g group by c order by 1;

drop table xc_groupby_def;
drop table xc_groupby_g;

-- Combination 2, enable_hashagg on and replicated tables.
-- repeat the same tests for replicated tables
-- create required tables and fill them with data
create table xc_groupby_tab1 (val int, val2 int) distribute by replication;
create table xc_groupby_tab2 (val int, val2 int) distribute by replication;
insert into xc_groupby_tab1 values (1, 1), (2, 1), (3, 1), (2, 2), (6, 2), (4, 3), (1, 3), (6, 3);
insert into xc_groupby_tab2 values (1, 1), (4, 1), (8, 1), (2, 4), (9, 4), (3, 4), (4, 2), (5, 2), (3, 2);
select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from xc_groupby_tab1 group by val2 order by 1, 2, 3;
explain (verbose true, costs false, nodes false) select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from xc_groupby_tab1 group by val2 order by 1, 2, 3;
explain (verbose true, costs false, nodes false) select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from xc_groupby_tab1 group by val2;
-- joins and group by
select * from (select count(*), sum(xc_groupby_tab1.val * xc_groupby_tab2.val), avg(xc_groupby_tab1.val*xc_groupby_tab2.val), sum(xc_groupby_tab1.val*xc_groupby_tab2.val)::float8/count(*), xc_groupby_tab1.val2 c1, xc_groupby_tab2.val2 c2 from xc_groupby_tab1 full outer join xc_groupby_tab2 on xc_groupby_tab1.val2 = xc_groupby_tab2.val2 group by xc_groupby_tab1.val2, xc_groupby_tab2.val2) q order by q.c1, q.c2;
explain (verbose true, costs false, nodes false) select * from (select count(*), sum(xc_groupby_tab1.val * xc_groupby_tab2.val), avg(xc_groupby_tab1.val*xc_groupby_tab2.val), sum(xc_groupby_tab1.val*xc_groupby_tab2.val)::float8/count(*), xc_groupby_tab1.val2 c1, xc_groupby_tab2.val2 c2 from xc_groupby_tab1 full outer join xc_groupby_tab2 on xc_groupby_tab1.val2 = xc_groupby_tab2.val2 group by xc_groupby_tab1.val2, xc_groupby_tab2.val2) q order by q.c1, q.c2;
-- aggregates over aggregates
select * from (select sum(y) sum from (select sum(val) y, val2%2 x from xc_groupby_tab1 group by val2) q1 group by x) q order by q.sum;
explain (verbose true, costs false, nodes false) select * from (select sum(y) sum from (select sum(val) y, val2%2 x from xc_groupby_tab1 group by val2) q1 group by x) q order by q.sum;
-- group by without aggregate
select val2 from xc_groupby_tab1 group by val2 order by 1;
explain (verbose true, costs false, nodes false) select val2 from xc_groupby_tab1 group by val2 order by 1;
explain (verbose true, costs false, nodes false) select val2 from xc_groupby_tab1 group by val2;
select * from (select val + val2 sum from xc_groupby_tab1 group by val + val2) q order by q.sum;
explain (verbose true, costs false, nodes false) select * from (select val + val2 sum from xc_groupby_tab1 group by val + val2) q order by q.sum;
select * from (select val + val2, val, val2 from xc_groupby_tab1 group by val, val2) q order by q.val, q.val2;
explain (verbose true, costs false, nodes false) select * from (select val + val2, val, val2 from xc_groupby_tab1 group by val, val2) q order by q.val, q.val2;
select * from (select xc_groupby_tab1.val + xc_groupby_tab2.val2, xc_groupby_tab1.val, xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val, xc_groupby_tab2.val2) q order by q.val, q.val2;
explain (verbose true, costs false, nodes false) select * from (select xc_groupby_tab1.val + xc_groupby_tab2.val2, xc_groupby_tab1.val, xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val, xc_groupby_tab2.val2) q order by q.val, q.val2;
select * from (select xc_groupby_tab1.val + xc_groupby_tab2.val2 sum from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val + xc_groupby_tab2.val2) q order by q.sum;
explain (verbose true, costs false, nodes false) select * from (select xc_groupby_tab1.val + xc_groupby_tab2.val2 sum from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val + xc_groupby_tab2.val2) q order by q.sum;
-- group by with aggregates in expression
select count(*) + sum(val) + avg(val), val2 from xc_groupby_tab1 group by val2 order by val2;
explain (verbose true, costs false, nodes false) select count(*) + sum(val) + avg(val), val2 from xc_groupby_tab1 group by val2 order by val2;
explain (verbose true, costs false, nodes false) select count(*) + sum(val) + avg(val), val2 from xc_groupby_tab1 group by val2;
-- group by with expressions in group by clause
select sum(val), avg(val), 2 * val2 from xc_groupby_tab1 group by 2 * val2 order by 2 * val2;
explain (verbose true, costs false, nodes false) select sum(val), avg(val), 2 * val2 from xc_groupby_tab1 group by 2 * val2 order by 2 * val2;
explain (verbose true, costs false, nodes false) select sum(val), avg(val), 2 * val2 from xc_groupby_tab1 group by 2 * val2;
drop table xc_groupby_tab1;
drop table xc_groupby_tab2;

-- some tests involving nulls, characters, float type etc.
create table xc_groupby_def(a int, b varchar(25)) distribute by replication; 
insert into xc_groupby_def VALUES (NULL, NULL);
insert into xc_groupby_def VALUES (1, NULL);
insert into xc_groupby_def VALUES (NULL, 'One');
insert into xc_groupby_def VALUES (2, 'Two');
insert into xc_groupby_def VALUES (2, 'Two');
insert into xc_groupby_def VALUES (3, 'Three');
insert into xc_groupby_def VALUES (4, 'Three');
insert into xc_groupby_def VALUES (5, 'Three');
insert into xc_groupby_def VALUES (6, 'Two');
insert into xc_groupby_def VALUES (7, NULL);
insert into xc_groupby_def VALUES (8, 'Two');
insert into xc_groupby_def VALUES (9, 'Three');
insert into xc_groupby_def VALUES (10, 'Three');

select a,count(a) from xc_groupby_def group by a order by a;
explain (verbose true, costs false, nodes false) select a,count(a) from xc_groupby_def group by a order by a;
select avg(a) from xc_groupby_def group by a order by 1; 
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_def group by a order by 1;
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_def group by a;
select avg(a) from xc_groupby_def group by a order by 1;
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_def group by a order by 1;
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_def group by a;
select avg(a) from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_def group by b;
select sum(a) from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select sum(a) from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select sum(a) from xc_groupby_def group by b;
select count(*) from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select count(*) from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select count(*) from xc_groupby_def group by b;
select count(*) from xc_groupby_def where a is not null group by a order by 1;
explain (verbose true, costs false, nodes false) select count(*) from xc_groupby_def where a is not null group by a order by 1;
explain (verbose true, costs false, nodes false) select count(*) from xc_groupby_def where a is not null group by a;

select * from (select b from xc_groupby_def group by b) q order by q.b;
explain (verbose true, costs false, nodes false) select * from (select b from xc_groupby_def group by b) q order by q.b;
select * from (select b,count(b) from xc_groupby_def group by b) q order by q.b;
explain (verbose true, costs false, nodes false) select * from (select b,count(b) from xc_groupby_def group by b) q order by q.b;
select count(*) from xc_groupby_def where b is null group by b order by 1;
explain (verbose true, costs false, nodes false) select count(*) from xc_groupby_def where b is null group by b;

create table xc_groupby_g(a int, b float, c numeric) distribute by replication;
insert into xc_groupby_g values(1,2.1,3.2);
insert into xc_groupby_g values(1,2.1,3.2);
insert into xc_groupby_g values(2,2.3,5.2);

select sum(a) from xc_groupby_g group by a;
explain (verbose true, costs false, nodes false) select sum(a) from xc_groupby_g group by a;
select sum(b) from xc_groupby_g group by b order by 1;
explain (verbose true, costs false, nodes false) select sum(b) from xc_groupby_g group by b order by 1;
explain (verbose true, costs false, nodes false) select sum(b) from xc_groupby_g group by b;
select sum(c) from xc_groupby_g group by b order by 1;
explain (verbose true, costs false, nodes false) select sum(c) from xc_groupby_g group by b order by 1;
explain (verbose true, costs false, nodes false) select sum(c) from xc_groupby_g group by b;

select avg(a) from xc_groupby_g group by b order by 1;
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_g group by b order by 1;
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_g group by b;
select avg(b) from xc_groupby_g group by c order by 1;
explain (verbose true, costs false, nodes false) select avg(b) from xc_groupby_g group by c order by 1;
explain (verbose true, costs false, nodes false) select avg(b) from xc_groupby_g group by c;
select avg(c) from xc_groupby_g group by c order by 1;
explain (verbose true, costs false, nodes false) select avg(c) from xc_groupby_g group by c order by 1;
explain (verbose true, costs false, nodes false) select avg(c) from xc_groupby_g group by c;

drop table xc_groupby_def;
drop table xc_groupby_g;
reset enable_hashagg;

-- Combination 3 enable_hashagg off and distributed tables
set enable_hashagg to off;
-- create required tables and fill them with data
create table xc_groupby_tab1 (val int, val2 int);
create table xc_groupby_tab2 (val int, val2 int);
insert into xc_groupby_tab1 values (1, 1), (2, 1), (3, 1), (2, 2), (6, 2), (4, 3), (1, 3), (6, 3);
insert into xc_groupby_tab2 values (1, 1), (4, 1), (8, 1), (2, 4), (9, 4), (3, 4), (4, 2), (5, 2), (3, 2);
select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from xc_groupby_tab1 group by val2 order by 1;
explain (verbose true, costs false, nodes false) select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from xc_groupby_tab1 group by val2;
-- joins and group by
select count(*), sum(xc_groupby_tab1.val * xc_groupby_tab2.val), avg(xc_groupby_tab1.val*xc_groupby_tab2.val), sum(xc_groupby_tab1.val*xc_groupby_tab2.val)::float8/count(*), xc_groupby_tab1.val2, xc_groupby_tab2.val2 from xc_groupby_tab1 full outer join xc_groupby_tab2 on xc_groupby_tab1.val2 = xc_groupby_tab2.val2 group by xc_groupby_tab1.val2, xc_groupby_tab2.val2 order by count(*);
explain (verbose true, costs false, nodes false) select count(*), sum(xc_groupby_tab1.val * xc_groupby_tab2.val), avg(xc_groupby_tab1.val*xc_groupby_tab2.val), sum(xc_groupby_tab1.val*xc_groupby_tab2.val)::float8/count(*), xc_groupby_tab1.val2, xc_groupby_tab2.val2 from xc_groupby_tab1 full outer join xc_groupby_tab2 on xc_groupby_tab1.val2 = xc_groupby_tab2.val2 group by xc_groupby_tab1.val2, xc_groupby_tab2.val2;
-- aggregates over aggregates
select sum(y) from (select sum(val) y, val2%2 x from xc_groupby_tab1 group by val2) q1 group by x;
explain (verbose true, costs false, nodes false) select sum(y) from (select sum(val) y, val2%2 x from xc_groupby_tab1 group by val2) q1 group by x;
-- group by without aggregate
select val2 from xc_groupby_tab1 group by val2 order by 1;
explain (verbose true, costs false, nodes false) select val2 from xc_groupby_tab1 group by val2;
select val + val2 from xc_groupby_tab1 group by val + val2 order by 1;
explain (verbose true, costs false, nodes false) select val + val2 from xc_groupby_tab1 group by val + val2;
select val + val2, val, val2 from xc_groupby_tab1 group by val, val2 order by val, val2;
explain (verbose true, costs false, nodes false) select val + val2, val, val2 from xc_groupby_tab1 group by val, val2 order by val, val2;
select xc_groupby_tab1.val + xc_groupby_tab2.val2, xc_groupby_tab1.val, xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val, xc_groupby_tab2.val2 order by 1;
explain (verbose true, costs false, nodes false) select xc_groupby_tab1.val + xc_groupby_tab2.val2, xc_groupby_tab1.val, xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val, xc_groupby_tab2.val2;
select xc_groupby_tab1.val + xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val + xc_groupby_tab2.val2 order by 1;
explain (verbose true, costs false, nodes false) select xc_groupby_tab1.val + xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val + xc_groupby_tab2.val2;
-- group by with aggregates in expression
select count(*) + sum(val) + avg(val), val2 from xc_groupby_tab1 group by val2 order by 1;
explain (verbose true, costs false, nodes false) select count(*) + sum(val) + avg(val), val2 from xc_groupby_tab1 group by val2;
-- group by with expressions in group by clause
select sum(val), avg(val), 2 * val2 from xc_groupby_tab1 group by 2 * val2 order by 1;
explain (verbose true, costs false, nodes false) select sum(val), avg(val), 2 * val2 from xc_groupby_tab1 group by 2 * val2;
drop table xc_groupby_tab1;
drop table xc_groupby_tab2;

-- some tests involving nulls, characters, float type etc.
create table xc_groupby_def(a int, b varchar(25)); 
insert into xc_groupby_def VALUES (NULL, NULL);
insert into xc_groupby_def VALUES (1, NULL);
insert into xc_groupby_def VALUES (NULL, 'One');
insert into xc_groupby_def VALUES (2, 'Two');
insert into xc_groupby_def VALUES (2, 'Two');
insert into xc_groupby_def VALUES (3, 'Three');
insert into xc_groupby_def VALUES (4, 'Three');
insert into xc_groupby_def VALUES (5, 'Three');
insert into xc_groupby_def VALUES (6, 'Two');
insert into xc_groupby_def VALUES (7, NULL);
insert into xc_groupby_def VALUES (8, 'Two');
insert into xc_groupby_def VALUES (9, 'Three');
insert into xc_groupby_def VALUES (10, 'Three');

select a,count(a) from xc_groupby_def group by a order by a;
explain (verbose true, costs false, nodes false) select a,count(a) from xc_groupby_def group by a order by a;
select avg(a) from xc_groupby_def group by a order by 1;
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_def group by a order by 1;
select avg(a) from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_def group by b;
select sum(a) from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select sum(a) from xc_groupby_def group by b;
select count(*) from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select count(*) from xc_groupby_def group by b;
select count(*) from xc_groupby_def where a is not null group by a order by 1;
explain (verbose true, costs false, nodes false) select count(*) from xc_groupby_def where a is not null group by a;

select b from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select b from xc_groupby_def group by b;
select b,count(b) from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select b,count(b) from xc_groupby_def group by b;
select count(*) from xc_groupby_def where b is null group by b order by 1;
explain (verbose true, costs false, nodes false) select count(*) from xc_groupby_def where b is null group by b;

create table xc_groupby_g(a int, b float, c numeric);
insert into xc_groupby_g values(1,2.1,3.2);
insert into xc_groupby_g values(1,2.1,3.2);
insert into xc_groupby_g values(2,2.3,5.2);

select sum(a) from xc_groupby_g group by a;
explain (verbose true, costs false, nodes false) select sum(a) from xc_groupby_g group by a;
select sum(b) from xc_groupby_g group by b order by 1;
explain (verbose true, costs false, nodes false) select sum(b) from xc_groupby_g group by b;
select sum(c) from xc_groupby_g group by b order by 1;
explain (verbose true, costs false, nodes false) select sum(c) from xc_groupby_g group by b;

select avg(a) from xc_groupby_g group by b order by 1;
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_g group by b;
select avg(b) from xc_groupby_g group by c order by 1;
explain (verbose true, costs false, nodes false) select avg(b) from xc_groupby_g group by c;
select avg(c) from xc_groupby_g group by c order by 1;
explain (verbose true, costs false, nodes false) select avg(c) from xc_groupby_g group by c;

drop table xc_groupby_def;
drop table xc_groupby_g;

-- Combination 4 enable_hashagg off and replicated tables.
-- repeat the same tests for replicated tables
-- create required tables and fill them with data
create table xc_groupby_tab1 (val int, val2 int) distribute by replication;
create table xc_groupby_tab2 (val int, val2 int) distribute by replication;
insert into xc_groupby_tab1 values (1, 1), (2, 1), (3, 1), (2, 2), (6, 2), (4, 3), (1, 3), (6, 3);
insert into xc_groupby_tab2 values (1, 1), (4, 1), (8, 1), (2, 4), (9, 4), (3, 4), (4, 2), (5, 2), (3, 2);
select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from xc_groupby_tab1 group by val2;
explain (verbose true, costs false, nodes false) select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from xc_groupby_tab1 group by val2;
-- joins and group by
select count(*), sum(xc_groupby_tab1.val * xc_groupby_tab2.val), avg(xc_groupby_tab1.val*xc_groupby_tab2.val), sum(xc_groupby_tab1.val*xc_groupby_tab2.val)::float8/count(*), xc_groupby_tab1.val2, xc_groupby_tab2.val2 from xc_groupby_tab1 full outer join xc_groupby_tab2 on xc_groupby_tab1.val2 = xc_groupby_tab2.val2 group by xc_groupby_tab1.val2, xc_groupby_tab2.val2 order by count(*);
explain (verbose true, costs false, nodes false) select count(*), sum(xc_groupby_tab1.val * xc_groupby_tab2.val), avg(xc_groupby_tab1.val*xc_groupby_tab2.val), sum(xc_groupby_tab1.val*xc_groupby_tab2.val)::float8/count(*), xc_groupby_tab1.val2, xc_groupby_tab2.val2 from xc_groupby_tab1 full outer join xc_groupby_tab2 on xc_groupby_tab1.val2 = xc_groupby_tab2.val2 group by xc_groupby_tab1.val2, xc_groupby_tab2.val2;
-- aggregates over aggregates
select sum(y) from (select sum(val) y, val2%2 x from xc_groupby_tab1 group by val2) q1 group by x;
explain (verbose true, costs false, nodes false) select sum(y) from (select sum(val) y, val2%2 x from xc_groupby_tab1 group by val2) q1 group by x;
-- group by without aggregate
select val2 from xc_groupby_tab1 group by val2 order by 1;
explain (verbose true, costs false, nodes false) select val2 from xc_groupby_tab1 group by val2;
select val + val2 from xc_groupby_tab1 group by val + val2 order by 1;
explain (verbose true, costs false, nodes false) select val + val2 from xc_groupby_tab1 group by val + val2;
select val + val2, val, val2 from xc_groupby_tab1 group by val, val2;
explain (verbose true, costs false, nodes false) select val + val2, val, val2 from xc_groupby_tab1 group by val, val2;
select xc_groupby_tab1.val + xc_groupby_tab2.val2, xc_groupby_tab1.val, xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val, xc_groupby_tab2.val2 order by 1;
explain (verbose true, costs false, nodes false) select xc_groupby_tab1.val + xc_groupby_tab2.val2, xc_groupby_tab1.val, xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val, xc_groupby_tab2.val2;
select xc_groupby_tab1.val + xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val + xc_groupby_tab2.val2 order by 1;
explain (verbose true, costs false, nodes false) select xc_groupby_tab1.val + xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val + xc_groupby_tab2.val2;
-- group by with aggregates in expression
select count(*) + sum(val) + avg(val), val2 from xc_groupby_tab1 group by val2 order by 1;
explain (verbose true, costs false, nodes false) select count(*) + sum(val) + avg(val), val2 from xc_groupby_tab1 group by val2;
-- group by with expressions in group by clause
select sum(val), avg(val), 2 * val2 from xc_groupby_tab1 group by 2 * val2 order by 1;
explain (verbose true, costs false, nodes false) select sum(val), avg(val), 2 * val2 from xc_groupby_tab1 group by 2 * val2;
drop table xc_groupby_tab1;
drop table xc_groupby_tab2;

-- some tests involving nulls, characters, float type etc.
create table xc_groupby_def(a int, b varchar(25)) distribute by replication; 
insert into xc_groupby_def VALUES (NULL, NULL);
insert into xc_groupby_def VALUES (1, NULL);
insert into xc_groupby_def VALUES (NULL, 'One');
insert into xc_groupby_def VALUES (2, 'Two');
insert into xc_groupby_def VALUES (2, 'Two');
insert into xc_groupby_def VALUES (3, 'Three');
insert into xc_groupby_def VALUES (4, 'Three');
insert into xc_groupby_def VALUES (5, 'Three');
insert into xc_groupby_def VALUES (6, 'Two');
insert into xc_groupby_def VALUES (7, NULL);
insert into xc_groupby_def VALUES (8, 'Two');
insert into xc_groupby_def VALUES (9, 'Three');
insert into xc_groupby_def VALUES (10, 'Three');

select a,count(a) from xc_groupby_def group by a order by a;
explain (verbose true, costs false, nodes false) select a,count(a) from xc_groupby_def group by a order by a;
select avg(a) from xc_groupby_def group by a; 
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_def group by a; 
select avg(a) from xc_groupby_def group by a;
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_def group by a;
select avg(a) from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_def group by b;
select sum(a) from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select sum(a) from xc_groupby_def group by b;
select count(*) from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select count(*) from xc_groupby_def group by b;
select count(*) from xc_groupby_def where a is not null group by a;
explain (verbose true, costs false, nodes false) select count(*) from xc_groupby_def where a is not null group by a;

select b from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select b from xc_groupby_def group by b;
select b,count(b) from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select b,count(b) from xc_groupby_def group by b;
select count(*) from xc_groupby_def where b is null group by b;
explain (verbose true, costs false, nodes false) select count(*) from xc_groupby_def where b is null group by b;

create table xc_groupby_g(a int, b float, c numeric) distribute by replication;
insert into xc_groupby_g values(1,2.1,3.2);
insert into xc_groupby_g values(1,2.1,3.2);
insert into xc_groupby_g values(2,2.3,5.2);

select sum(a) from xc_groupby_g group by a;
explain (verbose true, costs false, nodes false) select sum(a) from xc_groupby_g group by a;
select sum(b) from xc_groupby_g group by b;
explain (verbose true, costs false, nodes false) select sum(b) from xc_groupby_g group by b;
select sum(c) from xc_groupby_g group by b;
explain (verbose true, costs false, nodes false) select sum(c) from xc_groupby_g group by b;

select avg(a) from xc_groupby_g group by b;
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_g group by b;
select avg(b) from xc_groupby_g group by c;
explain (verbose true, costs false, nodes false) select avg(b) from xc_groupby_g group by c;
select avg(c) from xc_groupby_g group by c;
explain (verbose true, costs false, nodes false) select avg(c) from xc_groupby_g group by c;

drop table xc_groupby_def;
drop table xc_groupby_g;

reset enable_hashagg;
reset enable_fast_query_shipping;

-- Now repeat all the tests with FQS turned on
set enable_fast_query_shipping to on;

-- Combination 1: enable_hashagg on and distributed tables
set enable_hashagg to on;
-- create required tables and fill them with data
create table xc_groupby_tab1 (val int, val2 int);
create table xc_groupby_tab2 (val int, val2 int);
insert into xc_groupby_tab1 values (1, 1), (2, 1), (3, 1), (2, 2), (6, 2), (4, 3), (1, 3), (6, 3);
insert into xc_groupby_tab2 values (1, 1), (4, 1), (8, 1), (2, 4), (9, 4), (3, 4), (4, 2), (5, 2), (3, 2);
select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from xc_groupby_tab1 group by val2 order by 1, 2;
explain (verbose true, costs false, nodes false) select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from xc_groupby_tab1 group by val2 order by 1, 2;
explain (verbose true, costs false, nodes false) select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from xc_groupby_tab1 group by val2;
-- joins and group by
select count(*), sum(xc_groupby_tab1.val * xc_groupby_tab2.val), avg(xc_groupby_tab1.val*xc_groupby_tab2.val), sum(xc_groupby_tab1.val*xc_groupby_tab2.val)::float8/count(*), xc_groupby_tab1.val2, xc_groupby_tab2.val2 from xc_groupby_tab1 full outer join xc_groupby_tab2 on xc_groupby_tab1.val2 = xc_groupby_tab2.val2 group by xc_groupby_tab1.val2, xc_groupby_tab2.val2 order by count(*);
explain (verbose true, costs false, nodes false) select count(*), sum(xc_groupby_tab1.val * xc_groupby_tab2.val), avg(xc_groupby_tab1.val*xc_groupby_tab2.val), sum(xc_groupby_tab1.val*xc_groupby_tab2.val)::float8/count(*), xc_groupby_tab1.val2, xc_groupby_tab2.val2 from xc_groupby_tab1 full outer join xc_groupby_tab2 on xc_groupby_tab1.val2 = xc_groupby_tab2.val2 group by xc_groupby_tab1.val2, xc_groupby_tab2.val2;
-- aggregates over aggregates
select sum(y) from (select sum(val) y, val2%2 x from xc_groupby_tab1 group by val2) q1 group by x order by 1;
explain (verbose true, costs false, nodes false) select sum(y) from (select sum(val) y, val2%2 x from xc_groupby_tab1 group by val2) q1 group by x order by 1;
explain (verbose true, costs false, nodes false) select sum(y) from (select sum(val) y, val2%2 x from xc_groupby_tab1 group by val2) q1 group by x;
-- group by without aggregate
select val2 from xc_groupby_tab1 group by val2 order by 1;
explain (verbose true, costs false, nodes false) select val2 from xc_groupby_tab1 group by val2 order by 1;
explain (verbose true, costs false, nodes false) select val2 from xc_groupby_tab1 group by val2;
select val + val2 from xc_groupby_tab1 group by val + val2 order by 1;
explain (verbose true, costs false, nodes false) select val + val2 from xc_groupby_tab1 group by val + val2 order by 1;
explain (verbose true, costs false, nodes false) select val + val2 from xc_groupby_tab1 group by val + val2;
select val + val2, val, val2 from xc_groupby_tab1 group by val, val2 order by 1, 2;
explain (verbose true, costs false, nodes false) select val + val2, val, val2 from xc_groupby_tab1 group by val, val2 order by 1, 2;
explain (verbose true, costs false, nodes false) select val + val2, val, val2 from xc_groupby_tab1 group by val, val2;
select xc_groupby_tab1.val + xc_groupby_tab2.val2, xc_groupby_tab1.val, xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val, xc_groupby_tab2.val2 order by val, val2;
explain (verbose true, costs false, nodes false) select xc_groupby_tab1.val + xc_groupby_tab2.val2, xc_groupby_tab1.val, xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val, xc_groupby_tab2.val2 order by val, val2;
explain (verbose true, costs false, nodes false) select xc_groupby_tab1.val + xc_groupby_tab2.val2, xc_groupby_tab1.val, xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val, xc_groupby_tab2.val2;
select xc_groupby_tab1.val + xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val + xc_groupby_tab2.val2 order by 1;
explain (verbose true, costs false, nodes false) select xc_groupby_tab1.val + xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val + xc_groupby_tab2.val2 order by 1;
explain (verbose true, costs false, nodes false) select xc_groupby_tab1.val + xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val + xc_groupby_tab2.val2;
-- group by with aggregates in expression
select count(*) + sum(val) + avg(val), val2 from xc_groupby_tab1 group by val2 order by val2;
explain (verbose true, costs false, nodes false) select count(*) + sum(val) + avg(val), val2 from xc_groupby_tab1 group by val2 order by val2;
explain (verbose true, costs false, nodes false) select count(*) + sum(val) + avg(val), val2 from xc_groupby_tab1 group by val2;
-- group by with expressions in group by clause
select sum(val), avg(val), 2 * val2 from xc_groupby_tab1 group by 2 * val2 order by 2 * val2;
explain (verbose true, costs false, nodes false) select sum(val), avg(val), 2 * val2 from xc_groupby_tab1 group by 2 * val2 order by 2 * val2;
explain (verbose true, costs false, nodes false) select sum(val), avg(val), 2 * val2 from xc_groupby_tab1 group by 2 * val2;
drop table xc_groupby_tab1;
drop table xc_groupby_tab2;

-- some tests involving nulls, characters, float type etc.
create table xc_groupby_def(a int, b varchar(25)); 
insert into xc_groupby_def VALUES (NULL, NULL);
insert into xc_groupby_def VALUES (1, NULL);
insert into xc_groupby_def VALUES (NULL, 'One');
insert into xc_groupby_def VALUES (2, 'Two');
insert into xc_groupby_def VALUES (2, 'Two');
insert into xc_groupby_def VALUES (3, 'Three');
insert into xc_groupby_def VALUES (4, 'Three');
insert into xc_groupby_def VALUES (5, 'Three');
insert into xc_groupby_def VALUES (6, 'Two');
insert into xc_groupby_def VALUES (7, NULL);
insert into xc_groupby_def VALUES (8, 'Two');
insert into xc_groupby_def VALUES (9, 'Three');
insert into xc_groupby_def VALUES (10, 'Three');

select a,count(a) from xc_groupby_def group by a order by a;
explain (verbose true, costs false, nodes false) select a,count(a) from xc_groupby_def group by a order by a;
select avg(a) from xc_groupby_def group by a order by 1;
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_def group by a order by 1;
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_def group by a;
select avg(a) from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_def group by b;
select sum(a) from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select sum(a) from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select sum(a) from xc_groupby_def group by b;
select count(*) from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select count(*) from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select count(*) from xc_groupby_def group by b;
select count(*) from xc_groupby_def where a is not null group by a order by 1;
explain (verbose true, costs false, nodes false) select count(*) from xc_groupby_def where a is not null group by a order by 1;
explain (verbose true, costs false, nodes false) select count(*) from xc_groupby_def where a is not null group by a;

select * from (select b from xc_groupby_def group by b) q order by q.b;
explain (verbose true, costs false, nodes false) select * from (select b from xc_groupby_def group by b) q order by q.b;
select * from (select b,count(b) from xc_groupby_def group by b) q order by q.b;
explain (verbose true, costs false, nodes false) select * from (select b,count(b) from xc_groupby_def group by b) q order by q.b;
select count(*) from xc_groupby_def where b is null group by b order by 1;
explain (verbose true, costs false, nodes false) select count(*) from xc_groupby_def where b is null group by b;

create table xc_groupby_g(a int, b float, c numeric);
insert into xc_groupby_g values(1,2.1,3.2);
insert into xc_groupby_g values(1,2.1,3.2);
insert into xc_groupby_g values(2,2.3,5.2);

select sum(a) from xc_groupby_g group by a;
explain (verbose true, costs false, nodes false) select sum(a) from xc_groupby_g group by a;
select sum(b) from xc_groupby_g group by b order by 1;
explain (verbose true, costs false, nodes false) select sum(b) from xc_groupby_g group by b order by 1;
explain (verbose true, costs false, nodes false) select sum(b) from xc_groupby_g group by b;
select sum(c) from xc_groupby_g group by b order by 1;
explain (verbose true, costs false, nodes false) select sum(c) from xc_groupby_g group by b order by 1;
explain (verbose true, costs false, nodes false) select sum(c) from xc_groupby_g group by b;

select avg(a) from xc_groupby_g group by b order by 1;
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_g group by b order by 1;
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_g group by b;
select avg(b) from xc_groupby_g group by c order by 1;
explain (verbose true, costs false, nodes false) select avg(b) from xc_groupby_g group by c order by 1;
explain (verbose true, costs false, nodes false) select avg(b) from xc_groupby_g group by c;
select avg(c) from xc_groupby_g group by c order by 1;
explain (verbose true, costs false, nodes false) select avg(c) from xc_groupby_g group by c order by 1;
explain (verbose true, costs false, nodes false) select avg(c) from xc_groupby_g group by c;

drop table xc_groupby_def;
drop table xc_groupby_g;

-- Combination 2, enable_hashagg on and replicated tables.
-- repeat the same tests for replicated tables
-- create required tables and fill them with data
create table xc_groupby_tab1 (val int, val2 int) distribute by replication;
create table xc_groupby_tab2 (val int, val2 int) distribute by replication;
insert into xc_groupby_tab1 values (1, 1), (2, 1), (3, 1), (2, 2), (6, 2), (4, 3), (1, 3), (6, 3);
insert into xc_groupby_tab2 values (1, 1), (4, 1), (8, 1), (2, 4), (9, 4), (3, 4), (4, 2), (5, 2), (3, 2);
select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from xc_groupby_tab1 group by val2 order by 1, 2;
explain (verbose true, costs false, nodes false) select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from xc_groupby_tab1 group by val2 order by 1, 2;
explain (verbose true, costs false, nodes false) select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from xc_groupby_tab1 group by val2;
-- joins and group by
select * from (select count(*), sum(xc_groupby_tab1.val * xc_groupby_tab2.val), avg(xc_groupby_tab1.val*xc_groupby_tab2.val), sum(xc_groupby_tab1.val*xc_groupby_tab2.val)::float8/count(*), xc_groupby_tab1.val2 c1, xc_groupby_tab2.val2 c2 from xc_groupby_tab1 full outer join xc_groupby_tab2 on xc_groupby_tab1.val2 = xc_groupby_tab2.val2 group by xc_groupby_tab1.val2, xc_groupby_tab2.val2) q order by q.c1, q.c2;
explain (verbose true, costs false, nodes false) select * from (select count(*), sum(xc_groupby_tab1.val * xc_groupby_tab2.val), avg(xc_groupby_tab1.val*xc_groupby_tab2.val), sum(xc_groupby_tab1.val*xc_groupby_tab2.val)::float8/count(*), xc_groupby_tab1.val2 c1, xc_groupby_tab2.val2 c2 from xc_groupby_tab1 full outer join xc_groupby_tab2 on xc_groupby_tab1.val2 = xc_groupby_tab2.val2 group by xc_groupby_tab1.val2, xc_groupby_tab2.val2) q order by q.c1, q.c2;
-- aggregates over aggregates
select * from (select sum(y) sum from (select sum(val) y, val2%2 x from xc_groupby_tab1 group by val2) q1 group by x) q order by q.sum;
explain (verbose true, costs false, nodes false) select * from (select sum(y) sum from (select sum(val) y, val2%2 x from xc_groupby_tab1 group by val2) q1 group by x) q order by q.sum;
-- group by without aggregate
select val2 from xc_groupby_tab1 group by val2 order by 1;
explain (verbose true, costs false, nodes false) select val2 from xc_groupby_tab1 group by val2 order by 1;
explain (verbose true, costs false, nodes false) select val2 from xc_groupby_tab1 group by val2;
select * from (select val + val2 sum from xc_groupby_tab1 group by val + val2) q order by q.sum;
explain (verbose true, costs false, nodes false) select * from (select val + val2 sum from xc_groupby_tab1 group by val + val2) q order by q.sum;
select * from (select val + val2, val, val2 from xc_groupby_tab1 group by val, val2) q order by q.val, q.val2;
explain (verbose true, costs false, nodes false) select * from (select val + val2, val, val2 from xc_groupby_tab1 group by val, val2) q order by q.val, q.val2;
select * from (select xc_groupby_tab1.val + xc_groupby_tab2.val2, xc_groupby_tab1.val, xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val, xc_groupby_tab2.val2) q order by q.val, q.val2;
explain (verbose true, costs false, nodes false) select * from (select xc_groupby_tab1.val + xc_groupby_tab2.val2, xc_groupby_tab1.val, xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val, xc_groupby_tab2.val2) q order by q.val, q.val2;
select * from (select xc_groupby_tab1.val + xc_groupby_tab2.val2 sum from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val + xc_groupby_tab2.val2) q order by q.sum;
explain (verbose true, costs false, nodes false) select * from (select xc_groupby_tab1.val + xc_groupby_tab2.val2 sum from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val + xc_groupby_tab2.val2) q order by q.sum;
-- group by with aggregates in expression
select count(*) + sum(val) + avg(val), val2 from xc_groupby_tab1 group by val2 order by 1;
explain (verbose true, costs false, nodes false) select count(*) + sum(val) + avg(val), val2 from xc_groupby_tab1 group by val2 order by 1;
explain (verbose true, costs false, nodes false) select count(*) + sum(val) + avg(val), val2 from xc_groupby_tab1 group by val2;
-- group by with expressions in group by clause
select sum(val), avg(val), 2 * val2 from xc_groupby_tab1 group by 2 * val2 order by 1, 2;
explain (verbose true, costs false, nodes false) select sum(val), avg(val), 2 * val2 from xc_groupby_tab1 group by 2 * val2 order by 1, 2;
explain (verbose true, costs false, nodes false) select sum(val), avg(val), 2 * val2 from xc_groupby_tab1 group by 2 * val2;
drop table xc_groupby_tab1;
drop table xc_groupby_tab2;

-- some tests involving nulls, characters, float type etc.
create table xc_groupby_def(a int, b varchar(25)) distribute by replication; 
insert into xc_groupby_def VALUES (NULL, NULL);
insert into xc_groupby_def VALUES (1, NULL);
insert into xc_groupby_def VALUES (NULL, 'One');
insert into xc_groupby_def VALUES (2, 'Two');
insert into xc_groupby_def VALUES (2, 'Two');
insert into xc_groupby_def VALUES (3, 'Three');
insert into xc_groupby_def VALUES (4, 'Three');
insert into xc_groupby_def VALUES (5, 'Three');
insert into xc_groupby_def VALUES (6, 'Two');
insert into xc_groupby_def VALUES (7, NULL);
insert into xc_groupby_def VALUES (8, 'Two');
insert into xc_groupby_def VALUES (9, 'Three');
insert into xc_groupby_def VALUES (10, 'Three');

select a,count(a) from xc_groupby_def group by a order by a;
explain (verbose true, costs false, nodes false) select a,count(a) from xc_groupby_def group by a order by a;
select avg(a) from xc_groupby_def group by a order by a; 
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_def group by a order by a;
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_def group by a; 
select avg(a) from xc_groupby_def group by a order by 1;
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_def group by a order by 1;
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_def group by a;
select avg(a) from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_def group by b;
select sum(a) from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select sum(a) from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select sum(a) from xc_groupby_def group by b;
select count(*) from xc_groupby_def group by b order by b;
explain (verbose true, costs false, nodes false) select count(*) from xc_groupby_def group by b order by b;
explain (verbose true, costs false, nodes false) select count(*) from xc_groupby_def group by b;
select count(*) from xc_groupby_def where a is not null group by a order by 1;
explain (verbose true, costs false, nodes false) select count(*) from xc_groupby_def where a is not null group by a order by 1;
explain (verbose true, costs false, nodes false) select count(*) from xc_groupby_def where a is not null group by a;

select * from (select b from xc_groupby_def group by b) q order by q.b;
explain (verbose true, costs false, nodes false) select * from (select b from xc_groupby_def group by b) q order by q.b;
select * from (select b,count(b) from xc_groupby_def group by b) q order by q.b;
explain (verbose true, costs false, nodes false) select * from (select b,count(b) from xc_groupby_def group by b) q order by q.b;
select count(*) from xc_groupby_def where b is null group by b;
explain (verbose true, costs false, nodes false) select count(*) from xc_groupby_def where b is null group by b;

create table xc_groupby_g(a int, b float, c numeric) distribute by replication;
insert into xc_groupby_g values(1,2.1,3.2);
insert into xc_groupby_g values(1,2.1,3.2);
insert into xc_groupby_g values(2,2.3,5.2);

select sum(a) from xc_groupby_g group by a;
explain (verbose true, costs false, nodes false) select sum(a) from xc_groupby_g group by a;
select sum(b) from xc_groupby_g group by b order by 1;
explain (verbose true, costs false, nodes false) select sum(b) from xc_groupby_g group by b order by 1;
explain (verbose true, costs false, nodes false) select sum(b) from xc_groupby_g group by b;
select sum(c) from xc_groupby_g group by b order by 1;
explain (verbose true, costs false, nodes false) select sum(c) from xc_groupby_g group by b order by 1;
explain (verbose true, costs false, nodes false) select sum(c) from xc_groupby_g group by b;

select avg(a) from xc_groupby_g group by b order by 1;
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_g group by b order by 1;
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_g group by b;
select avg(b) from xc_groupby_g group by c order by c;
explain (verbose true, costs false, nodes false) select avg(b) from xc_groupby_g group by c order by c;
explain (verbose true, costs false, nodes false) select avg(b) from xc_groupby_g group by c;
select avg(c) from xc_groupby_g group by c order by c;
explain (verbose true, costs false, nodes false) select avg(c) from xc_groupby_g group by c order by c;
explain (verbose true, costs false, nodes false) select avg(c) from xc_groupby_g group by c;

drop table xc_groupby_def;
drop table xc_groupby_g;
reset enable_hashagg;

-- Combination 3 enable_hashagg off and distributed tables
set enable_hashagg to off;
-- create required tables and fill them with data
create table xc_groupby_tab1 (val int, val2 int);
create table xc_groupby_tab2 (val int, val2 int);
insert into xc_groupby_tab1 values (1, 1), (2, 1), (3, 1), (2, 2), (6, 2), (4, 3), (1, 3), (6, 3);
insert into xc_groupby_tab2 values (1, 1), (4, 1), (8, 1), (2, 4), (9, 4), (3, 4), (4, 2), (5, 2), (3, 2);
select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from xc_groupby_tab1 group by val2 order by 1, 2;
explain (verbose true, costs false, nodes false) select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from xc_groupby_tab1 group by val2 order by 1, 2;
explain (verbose true, costs false, nodes false) select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from xc_groupby_tab1 group by val2;
-- joins and group by
select count(*), sum(xc_groupby_tab1.val * xc_groupby_tab2.val), avg(xc_groupby_tab1.val*xc_groupby_tab2.val), sum(xc_groupby_tab1.val*xc_groupby_tab2.val)::float8/count(*), xc_groupby_tab1.val2, xc_groupby_tab2.val2 from xc_groupby_tab1 full outer join xc_groupby_tab2 on xc_groupby_tab1.val2 = xc_groupby_tab2.val2 group by xc_groupby_tab1.val2, xc_groupby_tab2.val2 order by count(*);
explain (verbose true, costs false, nodes false) select count(*), sum(xc_groupby_tab1.val * xc_groupby_tab2.val), avg(xc_groupby_tab1.val*xc_groupby_tab2.val), sum(xc_groupby_tab1.val*xc_groupby_tab2.val)::float8/count(*), xc_groupby_tab1.val2, xc_groupby_tab2.val2 from xc_groupby_tab1 full outer join xc_groupby_tab2 on xc_groupby_tab1.val2 = xc_groupby_tab2.val2 group by xc_groupby_tab1.val2, xc_groupby_tab2.val2;
-- aggregates over aggregates
select sum(y) from (select sum(val) y, val2%2 x from xc_groupby_tab1 group by val2) q1 group by x;
explain (verbose true, costs false, nodes false) select sum(y) from (select sum(val) y, val2%2 x from xc_groupby_tab1 group by val2) q1 group by x;
-- group by without aggregate
select val2 from xc_groupby_tab1 group by val2 order by 1;
explain (verbose true, costs false, nodes false) select val2 from xc_groupby_tab1 group by val2 order by 1;
explain (verbose true, costs false, nodes false) select val2 from xc_groupby_tab1 group by val2;
select val + val2 from xc_groupby_tab1 group by val + val2 order by 1;
explain (verbose true, costs false, nodes false) select val + val2 from xc_groupby_tab1 group by val + val2 order by 1;
explain (verbose true, costs false, nodes false) select val + val2 from xc_groupby_tab1 group by val + val2;
select val + val2, val, val2 from xc_groupby_tab1 group by val, val2 order by 1, 2;
explain (verbose true, costs false, nodes false) select val + val2, val, val2 from xc_groupby_tab1 group by val, val2 order by 1, 2;
explain (verbose true, costs false, nodes false) select val + val2, val, val2 from xc_groupby_tab1 group by val, val2;
select xc_groupby_tab1.val + xc_groupby_tab2.val2, xc_groupby_tab1.val, xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val, xc_groupby_tab2.val2 order by 1, 2;
explain (verbose true, costs false, nodes false) select xc_groupby_tab1.val + xc_groupby_tab2.val2, xc_groupby_tab1.val, xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val, xc_groupby_tab2.val2 order by 1, 2;
explain (verbose true, costs false, nodes false) select xc_groupby_tab1.val + xc_groupby_tab2.val2, xc_groupby_tab1.val, xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val, xc_groupby_tab2.val2;
select xc_groupby_tab1.val + xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val + xc_groupby_tab2.val2 order by 1;
explain (verbose true, costs false, nodes false) select xc_groupby_tab1.val + xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val + xc_groupby_tab2.val2 order by 1;
explain (verbose true, costs false, nodes false) select xc_groupby_tab1.val + xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val + xc_groupby_tab2.val2;
-- group by with aggregates in expression
select count(*) + sum(val) + avg(val), val2 from xc_groupby_tab1 group by val2 order by 1;
explain (verbose true, costs false, nodes false) select count(*) + sum(val) + avg(val), val2 from xc_groupby_tab1 group by val2 order by 1;
explain (verbose true, costs false, nodes false) select count(*) + sum(val) + avg(val), val2 from xc_groupby_tab1 group by val2;
-- group by with expressions in group by clause
select sum(val), avg(val), 2 * val2 from xc_groupby_tab1 group by 2 * val2 order by 3;
explain (verbose true, costs false, nodes false) select sum(val), avg(val), 2 * val2 from xc_groupby_tab1 group by 2 * val2 order by 3;
explain (verbose true, costs false, nodes false) select sum(val), avg(val), 2 * val2 from xc_groupby_tab1 group by 2 * val2;
drop table xc_groupby_tab1;
drop table xc_groupby_tab2;

-- some tests involving nulls, characters, float type etc.
create table xc_groupby_def(a int, b varchar(25)); 
insert into xc_groupby_def VALUES (NULL, NULL);
insert into xc_groupby_def VALUES (1, NULL);
insert into xc_groupby_def VALUES (NULL, 'One');
insert into xc_groupby_def VALUES (2, 'Two');
insert into xc_groupby_def VALUES (2, 'Two');
insert into xc_groupby_def VALUES (3, 'Three');
insert into xc_groupby_def VALUES (4, 'Three');
insert into xc_groupby_def VALUES (5, 'Three');
insert into xc_groupby_def VALUES (6, 'Two');
insert into xc_groupby_def VALUES (7, NULL);
insert into xc_groupby_def VALUES (8, 'Two');
insert into xc_groupby_def VALUES (9, 'Three');
insert into xc_groupby_def VALUES (10, 'Three');

select a,count(a) from xc_groupby_def group by a order by a;
explain (verbose true, costs false, nodes false) select a,count(a) from xc_groupby_def group by a order by a;
select avg(a) from xc_groupby_def group by a order by 1;
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_def group by a order by 1;
select avg(a) from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_def group by b;
select sum(a) from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select sum(a) from xc_groupby_def group by b;
select count(*) from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select count(*) from xc_groupby_def group by b;
select count(*) from xc_groupby_def where a is not null group by a order by 1;
explain (verbose true, costs false, nodes false) select count(*) from xc_groupby_def where a is not null group by a;

select b from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select b from xc_groupby_def group by b;
select b,count(b) from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select b,count(b) from xc_groupby_def group by b;
select count(*) from xc_groupby_def where b is null group by b order by 1;
explain (verbose true, costs false, nodes false) select count(*) from xc_groupby_def where b is null group by b;

create table xc_groupby_g(a int, b float, c numeric);
insert into xc_groupby_g values(1,2.1,3.2);
insert into xc_groupby_g values(1,2.1,3.2);
insert into xc_groupby_g values(2,2.3,5.2);

select sum(a) from xc_groupby_g group by a order by 1;
explain (verbose true, costs false, nodes false) select sum(a) from xc_groupby_g group by a;
select sum(b) from xc_groupby_g group by b order by 1;
explain (verbose true, costs false, nodes false) select sum(b) from xc_groupby_g group by b;
select sum(c) from xc_groupby_g group by b order by 1;
explain (verbose true, costs false, nodes false) select sum(c) from xc_groupby_g group by b;

select avg(a) from xc_groupby_g group by b order by 1;
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_g group by b;
select avg(b) from xc_groupby_g group by c order by 1;
explain (verbose true, costs false, nodes false) select avg(b) from xc_groupby_g group by c;
select avg(c) from xc_groupby_g group by c order by 1;
explain (verbose true, costs false, nodes false) select avg(c) from xc_groupby_g group by c;

drop table xc_groupby_def;
drop table xc_groupby_g;

-- Combination 4 enable_hashagg off and replicated tables.
-- repeat the same tests for replicated tables
-- create required tables and fill them with data
create table xc_groupby_tab1 (val int, val2 int) distribute by replication;
create table xc_groupby_tab2 (val int, val2 int) distribute by replication;
insert into xc_groupby_tab1 values (1, 1), (2, 1), (3, 1), (2, 2), (6, 2), (4, 3), (1, 3), (6, 3);
insert into xc_groupby_tab2 values (1, 1), (4, 1), (8, 1), (2, 4), (9, 4), (3, 4), (4, 2), (5, 2), (3, 2);
select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from xc_groupby_tab1 group by val2;
explain (verbose true, costs false, nodes false) select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from xc_groupby_tab1 group by val2;
-- joins and group by
select count(*), sum(xc_groupby_tab1.val * xc_groupby_tab2.val), avg(xc_groupby_tab1.val*xc_groupby_tab2.val), sum(xc_groupby_tab1.val*xc_groupby_tab2.val)::float8/count(*), xc_groupby_tab1.val2, xc_groupby_tab2.val2 from xc_groupby_tab1 full outer join xc_groupby_tab2 on xc_groupby_tab1.val2 = xc_groupby_tab2.val2 group by xc_groupby_tab1.val2, xc_groupby_tab2.val2 order by count(*);
explain (verbose true, costs false, nodes false) select count(*), sum(xc_groupby_tab1.val * xc_groupby_tab2.val), avg(xc_groupby_tab1.val*xc_groupby_tab2.val), sum(xc_groupby_tab1.val*xc_groupby_tab2.val)::float8/count(*), xc_groupby_tab1.val2, xc_groupby_tab2.val2 from xc_groupby_tab1 full outer join xc_groupby_tab2 on xc_groupby_tab1.val2 = xc_groupby_tab2.val2 group by xc_groupby_tab1.val2, xc_groupby_tab2.val2;
-- aggregates over aggregates
select sum(y) from (select sum(val) y, val2%2 x from xc_groupby_tab1 group by val2) q1 group by x;
explain (verbose true, costs false, nodes false) select sum(y) from (select sum(val) y, val2%2 x from xc_groupby_tab1 group by val2) q1 group by x;
-- group by without aggregate
select val2 from xc_groupby_tab1 group by val2 order by 1;
explain (verbose true, costs false, nodes false) select val2 from xc_groupby_tab1 group by val2;
select val + val2 from xc_groupby_tab1 group by val + val2 order by 1;
explain (verbose true, costs false, nodes false) select val + val2 from xc_groupby_tab1 group by val + val2;
select val + val2, val, val2 from xc_groupby_tab1 group by val, val2;
explain (verbose true, costs false, nodes false) select val + val2, val, val2 from xc_groupby_tab1 group by val, val2;
select xc_groupby_tab1.val + xc_groupby_tab2.val2, xc_groupby_tab1.val, xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val, xc_groupby_tab2.val2 order by 1;
explain (verbose true, costs false, nodes false) select xc_groupby_tab1.val + xc_groupby_tab2.val2, xc_groupby_tab1.val, xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val, xc_groupby_tab2.val2;
select xc_groupby_tab1.val + xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val + xc_groupby_tab2.val2 order by 1;
explain (verbose true, costs false, nodes false) select xc_groupby_tab1.val + xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val + xc_groupby_tab2.val2;
-- group by with aggregates in expression
select count(*) + sum(val) + avg(val), val2 from xc_groupby_tab1 group by val2 order by 1;
explain (verbose true, costs false, nodes false) select count(*) + sum(val) + avg(val), val2 from xc_groupby_tab1 group by val2;
-- group by with expressions in group by clause
select sum(val), avg(val), 2 * val2 from xc_groupby_tab1 group by 2 * val2 order by 1;
explain (verbose true, costs false, nodes false) select sum(val), avg(val), 2 * val2 from xc_groupby_tab1 group by 2 * val2;
drop table xc_groupby_tab1;
drop table xc_groupby_tab2;

-- some tests involving nulls, characters, float type etc.
create table xc_groupby_def(a int, b varchar(25)) distribute by replication; 
insert into xc_groupby_def VALUES (NULL, NULL);
insert into xc_groupby_def VALUES (1, NULL);
insert into xc_groupby_def VALUES (NULL, 'One');
insert into xc_groupby_def VALUES (2, 'Two');
insert into xc_groupby_def VALUES (2, 'Two');
insert into xc_groupby_def VALUES (3, 'Three');
insert into xc_groupby_def VALUES (4, 'Three');
insert into xc_groupby_def VALUES (5, 'Three');
insert into xc_groupby_def VALUES (6, 'Two');
insert into xc_groupby_def VALUES (7, NULL);
insert into xc_groupby_def VALUES (8, 'Two');
insert into xc_groupby_def VALUES (9, 'Three');
insert into xc_groupby_def VALUES (10, 'Three');

select a,count(a) from xc_groupby_def group by a order by a;
explain (verbose true, costs false, nodes false) select a,count(a) from xc_groupby_def group by a order by a;
select avg(a) from xc_groupby_def group by a; 
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_def group by a; 
select avg(a) from xc_groupby_def group by a;
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_def group by a;
select avg(a) from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_def group by b;
select sum(a) from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select sum(a) from xc_groupby_def group by b;
select count(*) from xc_groupby_def group by b;
explain (verbose true, costs false, nodes false) select count(*) from xc_groupby_def group by b;
select count(*) from xc_groupby_def where a is not null group by a;
explain (verbose true, costs false, nodes false) select count(*) from xc_groupby_def where a is not null group by a;

select b from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select b from xc_groupby_def group by b;
select b,count(b) from xc_groupby_def group by b order by 1;
explain (verbose true, costs false, nodes false) select b,count(b) from xc_groupby_def group by b;
select count(*) from xc_groupby_def where b is null group by b;
explain (verbose true, costs false, nodes false) select count(*) from xc_groupby_def where b is null group by b;

create table xc_groupby_g(a int, b float, c numeric) distribute by replication;
insert into xc_groupby_g values(1,2.1,3.2);
insert into xc_groupby_g values(1,2.1,3.2);
insert into xc_groupby_g values(2,2.3,5.2);

select sum(a) from xc_groupby_g group by a;
explain (verbose true, costs false, nodes false) select sum(a) from xc_groupby_g group by a;
select sum(b) from xc_groupby_g group by b;
explain (verbose true, costs false, nodes false) select sum(b) from xc_groupby_g group by b;
select sum(c) from xc_groupby_g group by b;
explain (verbose true, costs false, nodes false) select sum(c) from xc_groupby_g group by b;

select avg(a) from xc_groupby_g group by b;
explain (verbose true, costs false, nodes false) select avg(a) from xc_groupby_g group by b;
select avg(b) from xc_groupby_g group by c;
explain (verbose true, costs false, nodes false) select avg(b) from xc_groupby_g group by c;
select avg(c) from xc_groupby_g group by c;
explain (verbose true, costs false, nodes false) select avg(c) from xc_groupby_g group by c;

drop table xc_groupby_def;
drop table xc_groupby_g;

reset enable_hashagg;
reset enable_fast_query_shipping;
