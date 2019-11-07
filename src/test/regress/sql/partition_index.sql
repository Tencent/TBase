-- 1 day interval --
create table t_day_1(c1 int, c2 timestamp without time zone, c3 int)
partition by range(c2) begin(timestamp without time zone '2015-09-01') step(interval '1 day') partitions(23)
distribute by shard(c1);

create index t_day_1_c1 on t_day_1(c1);
create index t_day_1_c2 on t_day_1(c2);

insert into t_day_1 values(1, timestamp without time zone '2015-09-01 13:11:00', 1);
insert into t_day_1 select generate_series(2,3), timestamp without time zone '2015-09-02 13:11:00', 1;
insert into t_day_1 select generate_series(4,7), timestamp without time zone '2015-09-03 13:11:00', 1;
insert into t_day_1 select generate_series(8,15), timestamp without time zone '2015-09-04 13:11:00', 1;
insert into t_day_1 select generate_series(16,31), timestamp without time zone '2015-09-05 13:11:00', 1;
insert into t_day_1 select generate_series(32,63), timestamp without time zone '2015-09-06 13:11:00', 1;
insert into t_day_1 select generate_series(64,127), timestamp without time zone '2015-09-07 13:11:00', 1;
insert into t_day_1 select generate_series(128,255), timestamp without time zone '2015-09-08 13:11:00', 1;
insert into t_day_1 select generate_series(256,511), timestamp without time zone '2015-09-09 13:11:00', 1;
insert into t_day_1 select generate_series(512,1023), timestamp without time zone '2015-09-10 13:11:00', 1;
insert into t_day_1 select generate_series(1024,2047), timestamp without time zone '2015-09-11 13:11:00', 1;
insert into t_day_1 select generate_series(2048,4095), timestamp without time zone '2015-09-12 13:11:00', 1;
insert into t_day_1 select generate_series(4096,8191), timestamp without time zone '2015-09-13 13:11:00', 1;
insert into t_day_1 select generate_series(8192,16383), timestamp without time zone '2015-09-14 13:11:00', 1;

--create index when table is not empty--
create index t_day_1_c3_c2 on t_day_1(c3,c2);

--not-partition key --

select c1,c2 from t_day_1 where c2 < timestamp without time zone '2015-09-04' order by c1;

select c1,c2 from t_day_1 where c2 < timestamp without time zone '2015-09-04' order by c1 limit 5;

select shardid,c2 from t_day_1 where c2 < timestamp without time zone '2015-09-04' order by c1 limit 5;

select shardid,c1+c3 from t_day_1 where c2 < timestamp without time zone '2015-09-04' order by c1 limit 5;

--partition key --
select c1,c2 from t_day_1 where c2 < timestamp without time zone '2015-09-04' order by c2;

select c1,c2 from t_day_1 where c2 < timestamp without time zone '2015-09-04' order by c2 limit 5;

select c1,c2 from t_day_1 where c2 < timestamp without time zone '2015-09-04' order by c2 desc limit 5;

select shardid,c2 from t_day_1 where c2 < timestamp without time zone '2015-09-04' order by c2 limit 5;

select shardid,c1+c3 from t_day_1 where c2 < timestamp without time zone '2015-09-04' order by c2 limit 5;

select shardid,c1+c3 from t_day_1 where c2 < timestamp without time zone '2015-09-04' order by c3,c2 limit 5;

select shardid,c1 from t_day_1 where c2 < timestamp without time zone '2015-09-04' and mod(c1,2) = 1 order by c1 limit 5;

drop index t_day_1_c3_c2;

drop table t_day_1;

-- 1 week interval--
create table t_day_7(c1 int, c2 timestamp without time zone, c3 int)
partition by range(c2) begin(timestamp without time zone '2015-09-01') step(interval '7 day') partitions(10)
distribute by shard(c1);

create index t_day_7_c1 on t_day_7(c1);
create index t_day_7_c2 on t_day_7(c2);

insert into t_day_7 values(1, timestamp without time zone '2015-09-01 13:11:00', 1);
insert into t_day_7 select generate_series(2,3), timestamp without time zone '2015-09-08 13:11:00', 1;
insert into t_day_7 select generate_series(4,7), timestamp without time zone '2015-09-15 13:11:00', 1;
insert into t_day_7 select generate_series(8,15), timestamp without time zone '2015-09-22 13:11:00', 1;
insert into t_day_7 select generate_series(16,31), timestamp without time zone '2015-09-29 13:11:00', 1;
insert into t_day_7 select generate_series(32,63), timestamp without time zone '2015-10-06 13:11:00', 1;
insert into t_day_7 select generate_series(64,127), timestamp without time zone '2015-10-13 13:11:00', 1;
insert into t_day_7 select generate_series(128,255), timestamp without time zone '2015-10-20 13:11:00', 1;
insert into t_day_7 select generate_series(256,511), timestamp without time zone '2015-10-27 13:11:00', 1;
insert into t_day_7 select generate_series(512,1023), timestamp without time zone '2015-11-03 13:11:00', 1;

--create index when table is not empty--
create index t_day_7_c3_c2 on t_day_7(c3,c2);

--not-partition key --
 
select c1,c2 from t_day_7 where c2 < timestamp without time zone '2015-09-20' order by c1;
 
select c1,c2 from t_day_7 where c2 < timestamp without time zone '2015-09-20' order by c1 limit 5;

select shardid,c2 from t_day_7 where c2 < timestamp without time zone '2015-09-20' order by c1 limit 5;

select shardid,c1+c3 from t_day_7 where c2 < timestamp without time zone '2015-09-20' order by c1 limit 5;

--partition key --
select c1,c2 from t_day_7 where c2 < timestamp without time zone '2015-09-20' order by c2;

select c1,c2 from t_day_7 where c2 < timestamp without time zone '2015-09-20' order by c2 limit 5;

select c1,c2 from t_day_7 where c2 < timestamp without time zone '2015-09-20' order by c2 desc limit 5;

select shardid,c2 from t_day_7 where c2 < timestamp without time zone '2015-09-20' order by c2 limit 5;

select shardid,c1+c3 from t_day_7 where c2 < timestamp without time zone '2015-09-20' order by c2 limit 5;

select shardid,c1+c3 from t_day_7 where c2 < timestamp without time zone '2015-09-20' order by c3,c2 limit 5;

select shardid,c1 from t_day_7 where c2 < timestamp without time zone '2015-09-20' and mod(c1,2) = 1 order by c1 desc limit 5;

drop index t_day_7_c3_c2;

drop table t_day_7;

-- 3 month interval --
create table t_month_3(c1 int, c2 timestamp without time zone, c3 int)
partition by range(c2) begin(timestamp without time zone '2015-01-01') step(interval '3 month') partitions(8)
distribute by shard(c1);

create index t_month_3_c1 on t_month_3(c1);
create index t_month_3_c2 on t_month_3(c2);

insert into t_month_3 values(1, timestamp without time zone '2015-01-01 13:11:00', 1);
insert into t_month_3 select generate_series(2,3), timestamp without time zone '2015-04-01 13:11:00', 1;
insert into t_month_3 select generate_series(4,7), timestamp without time zone '2015-07-01 13:11:00', 1;
insert into t_month_3 select generate_series(8,15), timestamp without time zone '2015-10-01 13:11:00', 1;
insert into t_month_3 select generate_series(16,31), timestamp without time zone '2016-01-01 13:11:00', 1;
insert into t_month_3 select generate_series(32,63), timestamp without time zone '2016-04-01 13:11:00', 1;
insert into t_month_3 select generate_series(64,127), timestamp without time zone '2016-07-01 13:11:00', 1;
insert into t_month_3 select generate_series(128,255), timestamp without time zone '2016-10-01 13:11:00', 1;

--create index when table is not empty--
create index t_month_3_c3_c2 on t_month_3(c3,c2);

--not-partition key --
 
select c1,c2 from t_month_3 where c2 < timestamp without time zone '2016-02-01' order by c1;
 
select c1,c2 from t_month_3 where c2 < timestamp without time zone '2016-02-01' order by c1 limit 5;

select shardid,c2 from t_month_3 where c2 < timestamp without time zone '2016-02-01' order by c1 limit 5;

select shardid,c1+c3 from t_month_3 where c2 < timestamp without time zone '2016-02-01' order by c1 limit 5;

--partition key --
select c1,c2 from t_month_3 where c2 < timestamp without time zone '2015-10-01' order by c1;

select c1,c2 from t_month_3 where c2 < timestamp without time zone '2016-02-01' order by c2 limit 5;

select c1,c2 from t_month_3 where c2 < timestamp without time zone '2016-02-01' order by c2 desc limit 5;

select shardid,c2 from t_month_3 where c2 < timestamp without time zone '2016-02-01' order by c2 limit 5;

select shardid,c1+c3 from t_month_3 where c2 < timestamp without time zone '2016-02-01' order by c2 limit 5;

select shardid,c1+c3 from t_month_3 where c2 < timestamp without time zone '2016-02-01' order by c3,c2 limit 5;

select shardid,c1 from t_month_3 where c2 < timestamp without time zone '2016-02-01' and mod(c1,2) = 1 order by c1 desc limit 5;

drop index t_month_3_c3_c2;

drop table t_month_3;

