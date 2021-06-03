--explain analyze
create table a1(id int, num int, name text);
create table a2(id int, num int, name text);
--fqs case
explain (costs off,timing off,summary off,analyze,verbose)
insert into a1 values(1,generate_series(1,100),'a');
set enable_fast_query_shipping to off;
--insert into single value
explain (costs off,timing off,summary off,analyze,verbose)
insert into a1 values(2,1,'b');
--insert with set returning function
explain (costs off,timing off,summary off,analyze,verbose)
insert into a1 values(2,generate_series(2,100),'b');
explain (costs off,timing off,summary off,analyze,verbose)
insert into a1 values(3,generate_series(1,100),'c');
explain (costs off,timing off,summary off,analyze,verbose)
insert into a2 select * from a1;
reset enable_fast_query_shipping;

--normal cases
explain (costs off,timing off,summary off,analyze,verbose)
select count(*) from a1;
explain (costs off,timing off,summary off,analyze,verbose)
select num, count(*) cnt from a2 group by num order by cnt;
explain (costs off,timing off,summary off,analyze,verbose)
select * from a1, a2 where a1.num = a2.num;

--append
explain (costs off,timing off,summary off,analyze,verbose)
select max(num) from a1 union select min(num) from a1 order by 1;

--subplan
explain (costs off,timing off,summary off,analyze,verbose)
select * from a1 where id in (select count(*) from a2 where a1.num=a2.num);

--initplan
explain (costs off,timing off,summary off,analyze,verbose)
select * from a1 where num >= (select count(*) from a2 where name='a');
explain (costs off,timing off,summary off,analyze,verbose)
select * from a1 where num >= (select count(*) from a2 where name='b') order by id;
explain (costs off,timing off,summary off,analyze,verbose)
select * from a1 where num >= (select count(*) from a2 where name='c') limit 1;
explain (costs off,timing off,summary off,analyze,verbose)
select count(*) from a1 group by name having count(*) = (select count(*) from a2 where name='a');

--cleanup
drop table a1, a2;

