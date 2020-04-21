-- 1 day interval --
create table t_day_1(c1 int, c2 timestamp without time zone, c3 int)
partition by range(c2) begin(timestamp without time zone '2015-09-01') step(interval '1 day') partitions(63)
distribute by shard(c1);

insert into t_day_1 values(1, timestamp without time zone '2015-08-31 13:11:00', 1);
select c1,c2,c3 from t_day_1 partition for(timestamp without time zone '2015-08-31 13:11:00');

insert into t_day_1 values(1, timestamp without time zone '2015-09-01 00:00:00', 1);
select c1,c2,c3 from t_day_1 partition for(timestamp without time zone '2015-09-01 01:00:00');

insert into t_day_1 values(1, timestamp without time zone '2015-09-03 00:00:00', 1);
select c1,c2,c3 from t_day_1 partition for(timestamp without time zone '2015-09-03 01:00:00');

insert into t_day_1 values(1, timestamp without time zone '2015-11-02 00:00:00', 1);
select c1,c2,c3 from t_day_1 partition for(timestamp without time zone '2015-11-02 01:00:00');

insert into t_day_1 values(1, timestamp without time zone '2015-11-03 00:00:00', 1);
select c1,c2,c3 from t_day_1 partition for(timestamp without time zone '2015-11-03 00:00:00');

delete from t_day_1;
select count(1) from t_day_1;

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

--pruning--
select count(1) from t_day_1 where c2 < timestamp without time zone '2015-09-01 00:00:00';
select count(1) from t_day_1 where c2 <= timestamp without time zone '2015-09-01 00:00:00';
select count(1) from t_day_1 where c2 < timestamp without time zone '2015-09-01 23:00:00';
select count(1) from t_day_1 where c2 > timestamp without time zone '2015-09-01 01:00:00' and c2 < timestamp without time zone '2015-09-05 00:00:00';
select count(1) from t_day_1 where c2 > timestamp without time zone '2015-09-01 01:00:00' and c2 <= timestamp without time zone '2015-09-05 00:00:00';

select count(1) from t_day_1 where c2 > timestamp without time zone '2015-11-04 13:00:00';

select count(1) from t_day_1 where c2 > timestamp without time zone '2015-11-04 13:00:00' OR c2 < timestamp without time zone '2015-09-01 00:00:00';

select count(1) from t_day_1 where c2 < timestamp without time zone '2015-09-03 01:00:00' OR c2 > timestamp without time zone '2015-09-13 00:00:00';

--update--
update t_day_1 set c3=2 where c2 < timestamp without time zone '2015-09-01 00:00:00';
update t_day_1 set c3=3 where c2 > timestamp without time zone '2015-11-04 13:00:00';
update t_day_1 set c3=4 where c2 < timestamp without time zone '2015-09-03 00:00:00';

select * from t_day_1 where c3=2;
select * from t_day_1 where c3=3;
select * from t_day_1 where c3=4 order by c1;
--truncate --
truncate table t_day_1 partition for(timestamp without time zone '2015-08-30 00:00:00');
truncate table t_day_1 partition for(timestamp without time zone '2015-09-02 00:00:00');
select count(1) from t_day_1 partition for(timestamp without time zone '2015-09-02 00:00:00');
select count(1) from t_day_1;
truncate table t_day_1 partition for(timestamp without time zone '2015-11-04 00:00:00');

drop table t_day_1;

-- 1 week interval--
create table t_day_7(c1 int, c2 timestamp without time zone, c3 int)
partition by range(c2) begin(timestamp without time zone '2015-09-01') step(interval '7 day') partitions(10)
distribute by shard(c1);

insert into t_day_7 values(1, timestamp without time zone '2015-08-31 13:11:00', 1);
select c1,c2,c3 from t_day_7 partition for(timestamp without time zone '2015-08-31 13:11:00');

insert into t_day_7 values(1, timestamp without time zone '2015-09-01 00:00:00', 1);
select c1,c2,c3 from t_day_7 partition for(timestamp without time zone '2015-09-01 01:00:00');

insert into t_day_7 values(1, timestamp without time zone '2015-09-08 00:00:00', 1);
select c1,c2,c3 from t_day_7 partition for(timestamp without time zone '2015-09-08 01:00:00');

insert into t_day_7 values(1, timestamp without time zone '2015-11-09 00:00:00', 1);
select c1,c2,c3 from t_day_7 partition for(timestamp without time zone '2015-11-09 01:00:00');

insert into t_day_7 values(1, timestamp without time zone '2015-11-10 00:00:00', 1);
select c1,c2,c3 from t_day_7 partition for(timestamp without time zone '2015-11-10 00:00:00');

delete from t_day_7;
select count(1) from t_day_7;

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

--pruning--                
select count(1) from t_day_7 where c2 < timestamp without time zone '2015-09-01 00:00:00';
select count(1) from t_day_7 where c2 <= timestamp without time zone '2015-09-01 00:00:00';
select count(1) from t_day_7 where c2 < timestamp without time zone '2015-09-01 14:00:00';
select count(1) from t_day_7 where c2 > timestamp without time zone '2015-09-01 01:00:00' and c2 < timestamp without time zone '2015-09-16 00:00:00';
select count(1) from t_day_7 where c2 > timestamp without time zone '2015-09-01 01:00:00' and c2 <= timestamp without time zone '2015-09-16 00:00:00';
                           
select count(1) from t_day_7 where c2 > timestamp without time zone '2015-11-10 13:00:00';
                           
select count(1) from t_day_7 where c2 > timestamp without time zone '2015-11-10 13:00:00' OR c2 < timestamp without time zone '2015-09-01 00:00:00';
                           
select count(1) from t_day_7 where c2 < timestamp without time zone '2015-09-13 01:00:00' OR c2 > timestamp without time zone '2015-10-27 00:00:00';

--update--
update t_day_7 set c3=2 where c2 < timestamp without time zone '2015-09-01 00:00:00';
update t_day_7 set c3=3 where c2 > timestamp without time zone '2015-11-10 13:00:00';
update t_day_7 set c3=4 where c2 < timestamp without time zone '2015-09-09 00:00:00';

select * from t_day_7 where c3=2;
select * from t_day_7 where c3=3;
select * from t_day_7 where c3=4 order by c1;
--truncate --
truncate table t_day_7 partition for(timestamp without time zone '2015-08-30 00:00:00');
truncate table t_day_7 partition for(timestamp without time zone '2015-09-09 00:00:00');
select count(1) from t_day_7 partition for(timestamp without time zone '2015-09-09 00:00:00');
select count(1) from t_day_7;
truncate table t_day_7 partition for(timestamp without time zone '2015-11-10 00:00:00');

drop table t_day_7;

-- 1 month interval -- 
create table t_month_1(c1 int, c2 timestamp without time zone, c3 int)
partition by range(c2) begin(timestamp without time zone '2015-01-01') step(interval '1 month') partitions(24)
distribute by shard(c1);

insert into t_month_1 values(1, timestamp without time zone '2014-12-31 13:11:00', 1);
select c1,c2,c3 from t_month_1 partition for(timestamp without time zone '2014-12-31 13:11:00');

insert into t_month_1 values(1, timestamp without time zone '2015-01-01 00:00:00', 1);
select c1,c2,c3 from t_month_1 partition for(timestamp without time zone '2015-01-01 01:00:00');

insert into t_month_1 values(1, timestamp without time zone '2015-08-03 00:00:00', 1);
select c1,c2,c3 from t_month_1 partition for(timestamp without time zone '2015-08-03 01:00:00');

insert into t_month_1 values(1, timestamp without time zone '2016-11-03 00:00:00', 1);
select c1,c2,c3 from t_month_1 partition for(timestamp without time zone '2016-11-01 01:00:00');

insert into t_month_1 values(1, timestamp without time zone '2017-01-01 00:00:00', 1);
select c1,c2,c3 from t_month_1 partition for(timestamp without time zone '2017-01-01 00:00:00');

delete from t_month_1;
select count(1) from t_month_1;

insert into t_month_1 values(1, timestamp without time zone '2015-01-01 13:11:00', 1);
insert into t_month_1 select generate_series(2,3), timestamp without time zone '2015-02-01 13:11:00', 1;
insert into t_month_1 select generate_series(4,7), timestamp without time zone '2015-03-01 13:11:00', 1;
insert into t_month_1 select generate_series(8,15), timestamp without time zone '2015-04-01 13:11:00', 1;
insert into t_month_1 select generate_series(16,31), timestamp without time zone '2015-05-01 13:11:00', 1;
insert into t_month_1 select generate_series(32,63), timestamp without time zone '2015-06-01 13:11:00', 1;
insert into t_month_1 select generate_series(64,127), timestamp without time zone '2015-07-01 13:11:00', 1;
insert into t_month_1 select generate_series(128,255), timestamp without time zone '2015-08-01 13:11:00', 1;
insert into t_month_1 select generate_series(256,511), timestamp without time zone '2015-09-01 13:11:00', 1;
insert into t_month_1 select generate_series(512,1023), timestamp without time zone '2015-10-01 13:11:00', 1;
insert into t_month_1 select generate_series(1024,2047), timestamp without time zone '2015-11-01 13:11:00', 1;
insert into t_month_1 select generate_series(2048,4095), timestamp without time zone '2015-12-01 13:11:00', 1;
insert into t_month_1 select generate_series(4096,8191), timestamp without time zone '2016-01-01 13:11:00', 1;
insert into t_month_1 select generate_series(8192,16383), timestamp without time zone '2016-02-01 13:11:00', 1;

--pruning--           
select count(1) from t_month_1 where c2 < timestamp without time zone '2014-12-31 00:00:00';
select count(1) from t_month_1 where c2 <= timestamp without time zone '2015-01-02 00:00:00';
select count(1) from t_month_1 where c2 > timestamp without time zone '2015-04-01 13:00:00';
select count(1) from t_month_1 where c2 > timestamp without time zone '2015-11-01 01:00:00' and c2 < timestamp without time zone '2016-1-05 00:00:00';
select count(1) from t_month_1 where c2 > timestamp without time zone '2015-11-01 01:00:00' and c2 <= timestamp without time zone '2016-02-01 00:00:00';      
select count(1) from t_month_1 where c2 > timestamp without time zone '2017-01-01 13:00:00';                   
select count(1) from t_month_1 where c2 > timestamp without time zone '2017-01-01 13:00:00' OR c2 < timestamp without time zone '2014-12-31 00:00:00';                     
select count(1) from t_month_1 where c2 < timestamp without time zone '2015-02-03 01:00:00' OR c2 > timestamp without time zone '2016-2-01 00:00:00';

--update--
update t_month_1 set c3=2 where c2 < timestamp without time zone '2015-01-01 00:00:00';
update t_month_1 set c3=3 where c2 > timestamp without time zone '2017-01-01 13:00:00';
update t_month_1 set c3=4 where c2 < timestamp without time zone '2015-02-03 00:00:00';

select * from t_month_1 where c3=2;
select * from t_month_1 where c3=3;
select * from t_month_1 where c3=4 order by c1;
--truncate --
truncate table t_month_1 partition for(timestamp without time zone '2014-12-31 00:00:00');
truncate table t_month_1 partition for(timestamp without time zone '2015-09-02 00:00:00');
select count(1) from t_month_1 partition for(timestamp without time zone '2015-09-02 00:00:00');
select count(1) from t_month_1;
truncate table t_month_1 partition for(timestamp without time zone '2017-01-04 00:00:00');

drop table t_month_1;

-- 3 month interval --
create table t_month_3(c1 int, c2 timestamp without time zone, c3 int)
partition by range(c2) begin(timestamp without time zone '2015-01-01') step(interval '3 month') partitions(8)
distribute by shard(c1);

insert into t_month_3 values(1, timestamp without time zone '2014-12-31 13:11:00', 1);
select c1,c2,c3 from t_month_3 partition for(timestamp without time zone '2014-12-31 13:11:00');

insert into t_month_3 values(1, timestamp without time zone '2015-01-01 00:00:00', 1);
select c1,c2,c3 from t_month_3 partition for(timestamp without time zone '2015-01-01 01:00:00');

insert into t_month_3 values(1, timestamp without time zone '2015-08-03 00:00:00', 1);
select c1,c2,c3 from t_month_3 partition for(timestamp without time zone '2015-08-03 01:00:00');

insert into t_month_3 values(1, timestamp without time zone '2016-11-03 00:00:00', 1);
select c1,c2,c3 from t_month_3 partition for(timestamp without time zone '2016-11-01 01:00:00');

insert into t_month_3 values(1, timestamp without time zone '2017-01-01 00:00:00', 1);
select c1,c2,c3 from t_month_3 partition for(timestamp without time zone '2017-01-01 00:00:00');

delete from t_month_3;
select count(1) from t_month_3;

insert into t_month_3 values(1, timestamp without time zone '2015-01-01 13:11:00', 1);
insert into t_month_3 select generate_series(2,3), timestamp without time zone '2015-04-01 13:11:00', 1;
insert into t_month_3 select generate_series(4,7), timestamp without time zone '2015-07-01 13:11:00', 1;
insert into t_month_3 select generate_series(8,15), timestamp without time zone '2015-10-01 13:11:00', 1;
insert into t_month_3 select generate_series(16,31), timestamp without time zone '2016-01-01 13:11:00', 1;
insert into t_month_3 select generate_series(32,63), timestamp without time zone '2016-04-01 13:11:00', 1;
insert into t_month_3 select generate_series(64,127), timestamp without time zone '2016-07-01 13:11:00', 1;
insert into t_month_3 select generate_series(128,255), timestamp without time zone '2016-10-01 13:11:00', 1;

select count(1) from t_month_3 partition for(timestamp without time zone '2014-12-31 13:11:00');
select count(1) from t_month_3 partition for(timestamp without time zone '2015-09-05 13:11:00');
select count(1) from t_month_3 partition for(timestamp without time zone '2017-01-04 13:11:00');
--pruning--                  3
select count(1) from t_month_3 where c2 < timestamp without time zone '2014-12-31 00:00:00';
select count(1) from t_month_3 where c2 <= timestamp without time zone '2015-01-02 00:00:00';
select count(1) from t_month_3 where c2 > timestamp without time zone '2015-04-01 13:00:00';
select count(1) from t_month_3 where c2 > timestamp without time zone '2015-11-01 01:00:00' and c2 < timestamp without time zone '2016-2-05 00:00:00';
select count(1) from t_month_3 where c2 > timestamp without time zone '2015-11-01 01:00:00' and c2 <= timestamp without time zone '2016-02-01 00:00:00';      
select count(1) from t_month_3 where c2 > timestamp without time zone '2017-01-01 13:00:00';                   
select count(1) from t_month_3 where c2 > timestamp without time zone '2017-01-01 13:00:00' OR c2 < timestamp without time zone '2014-12-31 00:00:00';                     
select count(1) from t_month_3 where c2 < timestamp without time zone '2015-02-03 01:00:00' OR c2 > timestamp without time zone '2016-2-01 00:00:00';

--update--
update t_month_1 set c3=2 where c2 < timestamp without time zone '2015-01-01 00:00:00';
update t_month_1 set c3=3 where c2 > timestamp without time zone '2017-01-01 13:00:00';
update t_month_1 set c3=4 where c2 < timestamp without time zone '2015-02-03 00:00:00';

select * from t_month_3 where c3=2;
select * from t_month_3 where c3=3;
select * from t_month_3 where c3=4 order by c1;
--truncate --
truncate table t_month_3 partition for(timestamp without time zone '2014-12-31 00:00:00');
truncate table t_month_3 partition for(timestamp without time zone '2015-09-02 00:00:00');
select count(1) from t_month_3 partition for(timestamp without time zone '2015-09-02 00:00:00');
select count(1) from t_month_3;
truncate table t_month_3 partition for(timestamp without time zone '2017-01-01 00:00:00');

drop table t_month_3;

-- 1 year interval --
create table t_year_1(c1 int, c2 timestamp without time zone, c3 int)
partition by range(c2) begin(timestamp without time zone '2015-01-01') step(interval '1 year') partitions(5)
distribute by shard(c1);

insert into t_year_1 values(1, timestamp without time zone '2014-12-31 13:11:00', 1);
explain verbose select c1,c2,c3 from t_year_1 partition for(timestamp without time zone '2014-12-31 13:11:00');
select c1,c2,c3 from t_year_1 partition for(timestamp without time zone '2014-12-31 13:11:00');

insert into t_year_1 values(1, timestamp without time zone '2015-01-01 00:00:00', 1);
explain verbose select c1,c2,c3 from t_year_1 partition for(timestamp without time zone '2015-01-01 01:00:00');
select c1,c2,c3 from t_year_1 partition for(timestamp without time zone '2015-01-01 01:00:00');

insert into t_year_1 values(1, timestamp without time zone '2016-08-03 00:00:00', 1);
explain verbose select c1,c2,c3 from t_year_1 partition for(timestamp without time zone '2016-08-03 01:00:00');
select c1,c2,c3 from t_year_1 partition for(timestamp without time zone '2015-06-03 01:00:00');

insert into t_year_1 values(1, timestamp without time zone '2019-11-03 00:00:00', 1);
explain verbose select c1,c2,c3 from t_year_1 partition for(timestamp without time zone '2019-11-01 01:00:00');
select c1,c2,c3 from t_year_1 partition for(timestamp without time zone '2019-11-01 01:00:00');

insert into t_year_1 values(1, timestamp without time zone '2020-01-01 00:00:00', 1);
explain verbose select c1,c2,c3 from t_year_1 partition for(timestamp without time zone '2020-01-01 00:00:00');
select c1,c2,c3 from t_year_1 partition for(timestamp without time zone '2020-01-01 00:00:00');

delete from t_year_1;
select count(1) from t_year_1;

insert into t_year_1 values(1, timestamp without time zone '2015-01-01 13:11:00', 1);
insert into t_year_1 values(generate_series(2,3), timestamp without time zone '2016-01-01 13:11:00', 1);
insert into t_year_1 values(generate_series(4,7), timestamp without time zone '2017-01-01 13:11:00', 1);
insert into t_year_1 values(generate_series(8,15), timestamp without time zone '2018-01-01 13:11:00', 1);
insert into t_year_1 values(generate_series(16,31), timestamp without time zone '2019-01-01 13:11:00', 1);

select count(1) from t_year_1 partition for(timestamp without time zone '2014-12-31 13:11:00');
select count(1) from t_year_1 partition for(timestamp without time zone '2017-09-05 13:11:00');
select count(1) from t_year_1 partition for(timestamp without time zone '2020-01-04 13:11:00');
--pruning--            
select count(1) from t_year_1 where c2 < timestamp without time zone '2014-12-31 00:00:00';
select count(1) from t_year_1 where c2 <= timestamp without time zone '2015-01-02 00:00:00';
select count(1) from t_year_1 where c2 > timestamp without time zone '2018-04-01 13:00:00';
select count(1) from t_year_1 where c2 > timestamp without time zone '2017-11-01 01:00:00' and c2 < timestamp without time zone '2019-2-05 00:00:00';
select count(1) from t_year_1 where c2 > timestamp without time zone '2019-01-01 01:00:00' and c2 <= timestamp without time zone '2015-02-01 00:00:00';      
select count(1) from t_year_1 where c2 > timestamp without time zone '2020-01-01 13:00:00';                   
select count(1) from t_year_1 where c2 > timestamp without time zone '2020-01-01 13:00:00' OR c2 < timestamp without time zone '2014-12-31 00:00:00';                     
select count(1) from t_year_1 where c2 < timestamp without time zone '2015-02-03 01:00:00' OR c2 > timestamp without time zone '2019-01-01 00:00:00';

--update--
update t_year_1 set c3=2 where c2 < timestamp without time zone '2015-01-01 00:00:00';
update t_year_1 set c3=3 where c2 > timestamp without time zone '2020-01-01 13:00:00';
update t_year_1 set c3=4 where c2 < timestamp without time zone '2016-02-03 00:00:00';

select * from t_year_1 where c3=2;
select * from t_year_1 where c3=3;
select * from t_year_1 where c3=4 order by c1;
--truncate --
truncate table t_year_1 partition for(timestamp without time zone '2014-12-31 00:00:00');
truncate table t_year_1 partition for(timestamp without time zone '2016-09-02 00:00:00');
select count(1) from t_year_1 partition for(timestamp without time zone '2016-09-02 00:00:00');
select count(1) from t_year_1;
truncate table t_year_1 partition for(timestamp without time zone '2020-01-01 00:00:00');

drop table t_year_1;

--add partition & drop partiton--
create table t_drop(f1 int not null,f2 timestamp not null,f3 varchar(10),primary key(f1)) partition by range (f2) begin (timestamp without time zone '2019-01-01 0:0:0') step (interval '1 month') partitions (2) distribute by shard(f1) to group default_group;
ALTER TABLE t_drop ADD PARTITIONS 2; 
insert into t_drop select generate_series(1,10), timestamp without time zone '2019-01-31 23:23:59', 'aaa';
insert into t_drop select generate_series(11,30), timestamp without time zone '2019-02-01 10:23:59', 'aaa';
insert into t_drop select generate_series(31,50), timestamp without time zone '2019-03-31 23:23:59', 'aaa';
insert into t_drop select generate_series(51,100), timestamp without time zone '2019-04-01 00:00:00', 'aaa';
select count(1) from t_drop;
drop table t_drop_part_0;
drop table t_drop_part_1;
drop table t_drop_part_3;
select count(1) from t_drop partition for(timestamp without time zone '2019-01-01 00:00:00');
select count(1) from t_drop  where f2 = timestamp without time zone '2019-01-01 00:00:00';
select count(1) from t_drop;
select * from t_drop order by f1 limit 3;
ALTER TABLE t_drop ADD PARTITIONS 3; 
insert into t_drop select generate_series(101,110), timestamp without time zone '2019-05-01 00:00:00', 'aaa';
select count(1) from t_drop;
select * from t_drop where f2 = timestamp without time zone '2019-05-01 00:00:00' order by f1 desc limit 3;

--DDL--
alter table t_drop add f4 int;
insert into t_drop select generate_series(111,120), timestamp without time zone '2019-05-01 00:00:00', 'bbb', 100;
select count(1) from t_drop where f4 = 100; 
alter table t_drop drop f4;
alter table t_drop rename f3 to f3s;
select count(1) from t_drop where f3s = 'bbb';
alter table t_drop alter f3s type char(30);
VACUUM t_drop;

	
--DML--
update t_drop set f3s='cccccccccccc' where f3s = 'bbb';
update t_drop set f3s='ddd' where f2='2019-05-01';
delete from t_drop where f2 = timestamp without time zone '2019-02-01 00:00:00';
delete from t_drop where f2 >= timestamp without time zone '2019-05-01 00:00:00' and f2< timestamp without time zone '2019-06-01 00:00:00';
insert into t_drop values(130,'2019-02-01','a');
insert into t_drop values(130,'2019-03-01','multi-value'),(140,'2019-05-01','multi-value'),(150,'2019-06-01','multi-value');
insert into t_drop select generate_series(200, 2226),'2019-02-01','a';
insert into t_drop select generate_series(200, 2226),'2019-06-01','ffffff';
copy t_drop from stdin;
3000	'2019-07-01'	'hhhhhhh'
3001	'2019-07-01'	'hhhhhhh'
3002	'2019-07-01'	'hhhhhhh'
3003	'2019-07-01'	'hhhhhhh'
4000	'2019-08-01'	'lllllllll'
4001	'2019-08-01'	'lllllllll'
4002	'2019-08-01'	'lllllllll'
4003	'2019-08-01'	'lllllllll'
\.
copy (select * from t_drop where f2 = timestamp without time zone '2019-07-01 00:00:00') to stdout;

--truncate--
truncate table t_drop partition for(timestamp without time zone '2019-01-01 00:00:00');
truncate table t_drop partition for(timestamp without time zone '2019-12-01 00:00:00');
truncate table t_drop partition for(timestamp without time zone '2019-07-01 00:00:00');
drop table t_drop;