
-- test nestloop by shard

drop table if exists int8_tbl_s;
drop table if exists int4_tbl_s;
drop table if exists tenk1_s;
drop table if exists onek_s;

CREATE TABLE int8_tbl_s(q1 int8, q2 int8) distribute by shard(q1);
INSERT INTO int8_tbl_s VALUES('123','456');
INSERT INTO int8_tbl_s VALUES('123','4567890123456789');
INSERT INTO int8_tbl_s VALUES('4567890123456789','123');
INSERT INTO int8_tbl_s VALUES(+4567890123456789,'4567890123456789');
INSERT INTO int8_tbl_s VALUES('+4567890123456789','-4567890123456789');

CREATE TABLE int4_tbl_s(f1 int4) distribute by shard(f1);
INSERT INTO int4_tbl_s(f1) VALUES ('0');
INSERT INTO int4_tbl_s(f1) VALUES ('123456');
INSERT INTO int4_tbl_s(f1) VALUES ('-123456');


CREATE TABLE tenk1_s (
	unique1 int4,
	unique2 int4,
	two int4,
	four int4,
	ten int4,
	twenty int4,
	hundred int4,
	thousand int4,
	twothousand int4,
	fivethous int4,
	tenthous int4,
	odd int4,
	even int4,
	stringu1 name,
	stringu2 name,
	string4 name
) distribute by shard(unique1);

CREATE INDEX unique1_s ON tenk1_s USING btree(unique1 int4_ops);
CREATE INDEX unique2_s ON tenk1_s USING btree(unique2 int4_ops);
CREATE INDEX hundred_s ON tenk1_s USING btree(hundred int4_ops);
CREATE INDEX thous_tenthous_s ON tenk1_s (thousand, tenthous);

COPY tenk1_s FROM '/home/tbase/PG-XL-v10/src/test/regress/data/tenk.data';

CREATE TABLE onek_s (
	unique1 int4,
	unique2 int4,
	two int4,
	four int4,
	ten int4,
	twenty int4,
	hundred int4,
	thousand int4,
	twothousand int4,
	fivethous int4,
	tenthous int4,
	odd int4,
	even int4,
	stringu1 name,
	stringu2 name,
	string4 name
) distribute by shard(unique1);


CREATE INDEX onek_unique1_s ON onek_s USING btree(unique1 int4_ops);
CREATE INDEX onek_unique2_s ON onek_s USING btree(unique2 int4_ops);
CREATE INDEX onek_hundred_s ON onek_s USING btree(hundred int4_ops);
CREATE INDEX onek_stringu1_s ON onek_s USING btree(stringu1 name_ops);

COPY onek_s FROM '/home/tbase/PG-XL-v10/src/test/regress/data/onek.data';


set enable_hashjoin=off;
set enable_mergejoin=off;
set enable_nestloop=on;


explain (num_nodes off, nodes off, costs off)
select * from tenk1_s t1 left join
  (tenk1_s t2 join tenk1_s t3 on t2.thousand = t3.unique2)
  on t1.hundred = t2.hundred and t1.ten = t3.ten
where t1.unique1 = 1;

--select * from tenk1_s t1 left join
--  (tenk1_s t2 join tenk1_s t3 on t2.thousand = t3.unique2)
--  on t1.hundred = t2.hundred and t1.ten = t3.ten
--where t1.unique1 = 1;

explain (num_nodes off, nodes off, costs off)
select * from tenk1_s t1 left join
  (tenk1_s t2 join tenk1_s t3 on t2.thousand = t3.unique2)
  on t1.hundred = t2.hundred and t1.ten + t2.ten = t3.ten
where t1.unique1 = 1;

select * from tenk1_s t1 left join
  (tenk1_s t2 join tenk1_s t3 on t2.thousand = t3.unique2)
  on t1.hundred = t2.hundred and t1.ten + t2.ten = t3.ten
where t1.unique1 = 1;


explain (num_nodes off, nodes off, costs off)
select * from
(
  select unique1, q1, coalesce(unique1, -1) + q1 as fault
  from int8_tbl_s left join tenk1_s on (q2 = unique2)
) ss
where fault = 122
order by fault;

select * from
(
  select unique1, q1, coalesce(unique1, -1) + q1 as fault
  from int8_tbl_s left join tenk1_s on (q2 = unique2)
) ss
where fault = 122
order by fault;


explain (num_nodes off, nodes off, costs off)
select q1, unique2, thousand, hundred
  from int8_tbl_s a left join tenk1_s b on q1 = unique2
  where coalesce(thousand,123) = q1 and q1 = coalesce(hundred,123);
  
select q1, unique2, thousand, hundred
  from int8_tbl_s a left join tenk1_s b on q1 = unique2
  where coalesce(thousand,123) = q1 and q1 = coalesce(hundred,123);  
  

explain (num_nodes off, nodes off, costs off)
select f1, unique2, case when unique2 is null then f1 else 0 end
  from int4_tbl_s a left join tenk1_s b on f1 = unique2
  where (case when unique2 is null then f1 else 0 end) = 0;

select f1, unique2, case when unique2 is null then f1 else 0 end
  from int4_tbl_s a left join tenk1_s b on f1 = unique2
  where (case when unique2 is null then f1 else 0 end) = 0;  
  

explain (verbose, costs off)
select foo1.join_key as foo1_id, foo3.join_key AS foo3_id, bug_field from
  (values (0),(1)) foo1(join_key)
left join
  (select join_key, bug_field from
    (select ss1.join_key, ss1.bug_field from
      (select f1 as join_key, 666 as bug_field from int4_tbl_s i1) ss1
    ) foo2
   left join
    (select unique2 as join_key from tenk1_s i2) ss2
   using (join_key)
  ) foo3
using (join_key);
  
select foo1.join_key as foo1_id, foo3.join_key AS foo3_id, bug_field from
  (values (0),(1)) foo1(join_key)
left join
  (select join_key, bug_field from
    (select ss1.join_key, ss1.bug_field from
      (select f1 as join_key, 666 as bug_field from int4_tbl_s i1) ss1
    ) foo2
   left join
    (select unique2 as join_key from tenk1_s i2) ss2
   using (join_key)
  ) foo3
using (join_key);


explain (verbose, costs off)
select t1.unique1, t2.hundred
from onek_s t1, tenk1_s t2
where exists (select 1 from tenk1_s t3
              where t3.thousand = t1.unique1 and t3.tenthous = t2.hundred)
      and t1.unique1 < 1;  

--select t1.unique1, t2.hundred
--from onek_s t1, tenk1_s t2
--where exists (select 1 from tenk1_s t3
--              where t3.thousand = t1.unique1 and t3.tenthous = t2.hundred)
--      and t1.unique1 < 1;	  
  

reset enable_nestloop;
reset enable_hashjoin;
reset enable_mergejoin;

drop table int8_tbl_s;
drop table int4_tbl_s;
drop table tenk1_s;
drop table onek_s;
