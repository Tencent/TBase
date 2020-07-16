--
-- PARALLEL
--

create or replace function parallel_restricted(int) returns int as
  $$begin return $1; end$$ language plpgsql parallel restricted;

-- Serializable isolation would disable parallel query, so explicitly use an
-- arbitrary other level.
begin isolation level repeatable read;

-- encourage use of parallel plans
set parallel_setup_cost=0;
set parallel_tuple_cost=0;
set olap_optimizer = off;
set min_parallel_table_scan_size=0;
set max_parallel_workers_per_gather=4;
set olap_optimizer = off;
explain (costs off)
  select count(*) from a_star;
select count(*) from a_star;

-- test that parallel_restricted function doesn't run in worker
alter table tenk1 set (parallel_workers = 4);
explain (verbose, costs off)
select parallel_restricted(unique1) from tenk1
  where stringu1 = 'GRAAAA' order by 1;

-- test parallel plan when group by expression is in target list.
explain (costs off)
	select length(stringu1) from tenk1 group by length(stringu1);
select length(stringu1) from tenk1 group by length(stringu1);

explain (costs off)
	select stringu1, count(*) from tenk1 group by stringu1 order by stringu1;

explain (costs off)
	select count(stringu1) as num, (CASE WHEN length(stringu1) > 5 THEN 'LONG' ELSE 'SHORT' END) as islong
		from tenk1 group by islong order by num;
select count(stringu1) as num, (CASE WHEN length(stringu1) > 5 THEN 'LONG' ELSE 'SHORT' END) as islong
	from tenk1 group by islong order by num;

-- test that parallel plan for aggregates is not selected when
-- target list contains parallel restricted clause.
explain (costs off)
	select  sum(parallel_restricted(unique1)) from tenk1
	group by(parallel_restricted(unique1));

-- test parallel plans for queries containing un-correlated subplans.
alter table tenk2 set (parallel_workers = 0);
explain (costs off)
	select count(*) from tenk1 where (two, four) not in
	(select hundred, thousand from tenk2 where thousand > 100);
select count(*) from tenk1 where (two, four) not in
	(select hundred, thousand from tenk2 where thousand > 100);
-- this is not parallel-safe due to use of random() within SubLink's testexpr:
explain (costs off)
	select * from tenk1 where (unique1 + random())::integer not in
	(select ten from tenk2);
alter table tenk2 reset (parallel_workers);

-- test parallel index scans.
set enable_seqscan to off;
set enable_bitmapscan to off;

explain (costs off)
	select  count((unique1)) from tenk1 where hundred > 1;
select  count((unique1)) from tenk1 where hundred > 1;

-- test parallel index-only scans.
explain (costs off)
	select  count(*) from tenk1 where thousand > 95;
select  count(*) from tenk1 where thousand > 95;

reset enable_seqscan;
reset enable_bitmapscan;

-- test parallel bitmap heap scan.
set enable_seqscan to off;
set enable_indexscan to off;
set enable_hashjoin to off;
set enable_mergejoin to off;
set enable_material to off;
-- test prefetching, if the platform allows it
--DO $$
--BEGIN
-- SET effective_io_concurrency = 50;
--END $$;
set work_mem='64kB';  --set small work mem to force lossy pages
explain (costs off)
	select count(*) from tenk1, tenk2 where tenk1.hundred > 1 and tenk2.thousand=0;
select count(*) from tenk1, tenk2 where tenk1.hundred > 1 and tenk2.thousand=0;

create table bmscantest (a int, t text);
insert into bmscantest select r, 'fooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo' FROM generate_series(1,100000) r;
create index i_bmtest ON bmscantest(a);
select count(*) from bmscantest where a>1;

reset enable_seqscan;
reset enable_indexscan;
reset enable_hashjoin;
reset enable_mergejoin;
reset enable_material;
reset effective_io_concurrency;
reset work_mem;
drop table bmscantest;

-- test parallel merge join path.
set enable_hashjoin to off;
set enable_nestloop to off;

explain (costs off)
	select  count(*) from tenk1, tenk2 where tenk1.unique1 = tenk2.unique1;
select  count(*) from tenk1, tenk2 where tenk1.unique1 = tenk2.unique1;

reset enable_hashjoin;
reset enable_nestloop;

--test gather merge
set enable_hashagg to off;

explain (costs off)
   select  string4, count((unique2)) from tenk1 group by string4 order by string4;

select  string4, count((unique2)) from tenk1 group by string4 order by string4;

reset enable_hashagg;

set force_parallel_mode=1;

explain (costs off)
  select stringu1::int2 from tenk1 where unique1 = 1;

-- to increase the parallel query test coverage
EXPLAIN (timing off, summary off, costs off) SELECT * FROM tenk1;

EXPLAIN (analyze, timing off, summary off, costs off) SELECT * FROM tenk1;

-- make sure identifier was set in workers
CREATE TABLE t_worker_identifier (a int);
INSERT INTO t_worker_identifier values(1);
EXPLAIN (costs off) SELECT xc_node_id != 0 FROM t_worker_identifier;
SELECT xc_node_id != 0 FROM t_worker_identifier;

-- provoke error in worker
SAVEPOINT settings;
select stringu1::int2 from tenk1 where unique1 = 1;
ROLLBACK TO SAVEPOINT settings;

-- test interaction with set-returning functions
SAVEPOINT settings;

-- multiple subqueries under a single Gather node
-- must set parallel_setup_cost > 0 to discourage multiple Gather nodes
SET LOCAL parallel_setup_cost = 10;
EXPLAIN (COSTS OFF)
SELECT unique1 FROM tenk1 WHERE fivethous = tenthous + 1
UNION ALL
SELECT unique1 FROM tenk1 WHERE fivethous = tenthous + 1;
ROLLBACK TO SAVEPOINT settings;

-- can't use multiple subqueries under a single Gather node due to initPlans
EXPLAIN (COSTS OFF)
SELECT unique1 FROM tenk1 WHERE fivethous =
   (SELECT unique1 FROM tenk1 WHERE fivethous = 1 LIMIT 1)
UNION ALL
SELECT unique1 FROM tenk1 WHERE fivethous =
   (SELECT unique2 FROM tenk1 WHERE fivethous = 1 LIMIT 1)
ORDER BY 1;

-- test interaction with SRFs
SELECT * FROM information_schema.foreign_data_wrapper_options
ORDER BY 1, 2, 3;

rollback;
