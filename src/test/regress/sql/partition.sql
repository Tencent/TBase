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

--date partition: add partition & drop partiton--
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
copy (select * from t_drop where f2 = timestamp without time zone '2019-07-01 00:00:00' order by 1,2,3) to stdout;

--truncate--
truncate table t_drop partition for(timestamp without time zone '2019-01-01 00:00:00');
truncate table t_drop partition for(timestamp without time zone '2019-12-01 00:00:00');
truncate table t_drop partition for(timestamp without time zone '2019-07-01 00:00:00');
drop table t_drop;

--int partition: add partition & drop partiton--
create table int_drop(f1 bigint,f2 timestamp default now(), f3 integer) partition by range (f3) begin (1) step (50) partitions (2) distribute by shard(f1) to group default_group;
ALTER TABLE int_drop ADD PARTITIONS 2; 
insert into int_drop select generate_series(1,10), null, 10;
insert into int_drop select generate_series(51,70), null, 80;
insert into int_drop select generate_series(101,120), null, 130;
insert into int_drop select generate_series(151,200), null, 190;
select count(1) from int_drop;
drop table int_drop_part_0;
drop table int_drop_part_2;
drop table int_drop_part_3;
select count(1) from int_drop partition for(20);
select count(1) from int_drop  where f3 = 20;
select count(1) from int_drop;
select * from int_drop order by f1 limit 5;
ALTER TABLE int_drop ADD PARTITIONS 5; 
insert into int_drop select generate_series(201,210), null, 245;
select count(1) from int_drop;
select * from int_drop where f3 = 245 order by f1 desc limit 5;

--DDL--
alter table int_drop add f4 int, add f5 varchar(20);
insert into int_drop select generate_series(211,220), null, 249, 100, 'aaa';
select count(1) from int_drop where f4 = 100; 
alter table int_drop drop f4;
alter table int_drop rename f5 to f5s;
select count(1) from int_drop where f5s = 'aaa';
alter table int_drop alter f5s type varchar(30);
VACUUM int_drop;

	
--DML--
update int_drop set f5s='cccccccccccc' where f5s = 'aaa';
update int_drop set f5s='ddddddddddd' where f3=249;
delete from int_drop where f3 = 10;
delete from int_drop where f3 >= 1 and f3< 201;
insert into int_drop values(140,'2019-03-01',140,'aaa');
insert into int_drop values(90,'2019-02-01',90,'single-value-insert');
insert into int_drop values(240,'2019-05-01',240,'multi-value-insert'),(280,'2019-06-01',280,'multi-value-insert'),(330,'2019-07-01',330,'multi-value-insert');
insert into int_drop select generate_series(500, 2526),'2019-03-01',145,'a';
insert into int_drop select generate_series(500, 2526),'2019-07-01',345,'ffffff';
copy int_drop from stdin;
3000	'2019-08-01'	355	'hhhhhhh'
3001	'2019-08-02'	365	'hhhhhhh'
3002	'2019-08-03'	375	'hhhhhhh'
3003	'2019-08-04'	385	'hhhhhhh'
4000	'2019-09-11'	405	'lllllllll'
4001	'2019-09-12'	415	'lllllllll'
4002	'2019-09-13'	425	'lllllllll'
4003	'2019-09-14'	435	'lllllllll'
4004	'2019-09-15'	445	'lllllllll'
\.
copy (select * from int_drop where f3 > 400 order by f1) to stdout;

--truncate--
truncate table int_drop partition for(5);
truncate table int_drop partition for(1000);
truncate table int_drop partition for(370);
drop table int_drop;

-- IN expr partition pruning
create table t_in_test(a int, b int, c timestamp)
partition by range (c) begin
(timestamp without time zone '2017-09-01 0:0:0')
step (interval '1 month') partitions (12)
distribute by shard (a)
to group default_group;

insert into t_in_test values(1,1,'20170901');
insert into t_in_test values(2,2,'20171001');
insert into t_in_test values(3,3,'20171101');
insert into t_in_test values(3,3,'20171201');

explain (costs off) select * from t_in_test where c in ('20171001', '20171201');
set enable_fast_query_shipping to off;
explain (costs off) select * from t_in_test where c in ('20170901', '20171101');
reset enable_fast_query_shipping;
drop table t_in_test;

-- for February of common year timestamp partition, add sub table should be ok
create table t_time_range (a int, b int, c timestamp)
partition by range (c) begin
(timestamp without time zone '2022-02-26 0:0:0')
step (interval '1 day') partitions (3)
distribute by shard(a)
to group default_group;

insert into t_time_range values(1, 1, '2022-02-28');
insert into t_time_range values(1, 1, '2022-03-1');
ALTER TABLE t_time_range ADD PARTITIONS 1;
insert into t_time_range values(1, 1, '2022-03-1');
drop table t_time_range;

create table t_time_range (a int, b int, c timestamp)
partition by range (c) begin
(timestamp without time zone '2022-02-26 0:0:0')
step (interval '1 day') partitions (1)
distribute by shard(a)
to group default_group;

insert into t_time_range values(1, 1, '2022-02-26');
ALTER TABLE t_time_range ADD PARTITIONS 2;
insert into t_time_range values(1, 1, '2022-02-28');
insert into t_time_range values(1, 1, '2022-03-1');
ALTER TABLE t_time_range ADD PARTITIONS 1;
insert into t_time_range values(1, 1, '2022-03-1');
drop table t_time_range;

-- for February of leap year timestamp partition, add sub table should be ok
create table t_time_range (a int, b int, c timestamp)
partition by range (c) begin
(timestamp without time zone '2020-02-26 0:0:0')
step (interval '1 day') partitions (3)
distribute by shard(a)
to group default_group;

insert into t_time_range values(1, 1, '2020-02-26');
insert into t_time_range values(1, 1, '2020-02-27');
insert into t_time_range values(1, 1, '2020-02-28');
insert into t_time_range values(1, 1, '2020-02-29');
ALTER TABLE t_time_range ADD PARTITIONS 2;
insert into t_time_range values(1, 1, '2020-02-29');
insert into t_time_range values(1, 1, '2020-03-01');
drop table t_time_range;

