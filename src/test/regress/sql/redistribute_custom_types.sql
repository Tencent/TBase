--
-- redistribute custom types
--

-- enum type
drop table if exists enum_test;
drop type if exists enumtype;

create type enumtype AS enum ('Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun');
create table enum_test(a int, b enumtype) distribute by shard(a);

insert into enum_test(a,b) values(1,'Mon');
insert into enum_test(a,b) values(2,'Tue');
insert into enum_test(a,b) values(3,'Wed');
insert into enum_test(a,b) values(4,'Thu');
insert into enum_test(a,b) values(5,'Fri');
insert into enum_test(a,b) values(6,'Sat');
insert into enum_test(a,b) values(7,'Sun');

explain select count(*) from enum_test where a < 100 group by b;
select count(*) from enum_test where a < 100 group by b;


-- composite type
drop table if exists comptype_test;
drop type if exists comptype;

create type comptype as (f1 int, f2 int);
create table comptype_test(a int, b comptype) distribute by shard(a);

insert into comptype_test(a,b) values(1,(1,2));
insert into comptype_test(a,b) values(2,(2,3));
insert into comptype_test(a,b) values(3,(3,4));
insert into comptype_test(a,b) values(4,(4,5));
insert into comptype_test(a,b) values(5,(5,6));
insert into comptype_test(a,b) values(6,(6,7));

explain select count(*) from comptype_test where a < 100 group by b;
select count(*) from comptype_test where a < 100 group by b;


-- domain type
drop table if exists domaintype_test;
drop domain if exists domaintype;

create domain domaintype as int check(value < 100);
create table domaintype_test(a int, b domaintype) distribute by shard(a);

insert into domaintype_test(a,b) values(1,1);
insert into domaintype_test(a,b) values(2,2);
insert into domaintype_test(a,b) values(3,3);
insert into domaintype_test(a,b) values(4,4);
insert into domaintype_test(a,b) values(5,5);
insert into domaintype_test(a,b) values(6,6);

explain select count(*) from domaintype_test where a < 100 group by b;
select count(*) from domaintype_test where a < 100 group by b;

drop table enum_test;
drop table comptype_test;
drop table domaintype_test;

drop type enumtype;
drop type comptype;
drop type domaintype;