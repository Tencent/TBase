--
drop table if exists extent_test_1;
create table extent_test_1(c1 int,c2 int) distribute by hash(c1);
insert into extent_test_1 values(generate_series(1,10),1);
checkpoint;
select * from pg_extent_info('extent_test_1');

select * from pg_shard_scan_list('extent_test_1', 2234);
select * from pg_shard_alloc_list('extent_test_1', 2234);

insert into extent_test_1 values(1,generate_series(2, 378880));

select * from pg_shard_scan_list('extent_test_1', 2234);
select * from pg_shard_alloc_list('extent_test_1', 2234);

insert into extent_test_1 values(1,400000);
select * from pg_shard_scan_list('extent_test_1', 2234);
select * from pg_shard_alloc_list('extent_test_1', 2234);

insert into extent_test_1 values(1,generate_series(400001,778880));
select * from pg_shard_scan_list('extent_test_1', 2234);
select * from pg_shard_alloc_list('extent_test_1', 2234);

insert into extent_test_1 values(1,800000);
select * from pg_shard_scan_list('extent_test_1', 2234);
select * from pg_shard_alloc_list('extent_test_1', 2234);

update extent_test_1 set c2=c2+1 where c1=1;
select * from pg_shard_scan_list('extent_test_1', 2234);
select * from pg_shard_alloc_list('extent_test_1', 2234);

select count(1) from extent_test_1;
select ctid,* from extent_test_1 where c2%10000 = 0 order by c1 limit 20;
delete from extent_test_1 where c1=1 and c2<10000;
select * from pg_shard_scan_list('extent_test_1', 2234);
select * from pg_shard_alloc_list('extent_test_1', 2234);

vacuum extent_test_1;
select * from pg_shard_scan_list('extent_test_1', 2234);
select * from pg_shard_alloc_list('extent_test_1', 2234);

delete from extent_test_1 where c1=1 and c2>=40000 and c2<410000;
select * from pg_shard_scan_list('extent_test_1', 2234);
select * from pg_shard_alloc_list('extent_test_1', 2234);

vacuum extent_test_1;
select * from pg_shard_scan_list('extent_test_1', 2234);
select * from pg_shard_alloc_list('extent_test_1', 2234);

insert into extent_test_1 values(1,generate_series(1,9999));
select * from pg_shard_scan_list('extent_test_1', 2234);
select * from pg_shard_alloc_list('extent_test_1', 2234);

insert into extent_test_1 values(1,generate_series(400000,409999));
select * from pg_shard_scan_list('extent_test_1', 2234);
select * from pg_shard_alloc_list('extent_test_1', 2234);

