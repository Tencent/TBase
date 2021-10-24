-- A function to create table on specified nodes 
create or replace function cr_table(tab_schema varchar, nodenums int[], distribution varchar)
returns void language plpgsql as $$
declare
	cr_command	varchar;
	nodes		varchar[];
	nodename	varchar;
	nodenames_query varchar;
	nodenames 	varchar;
	node 		int;
	sep			varchar;
	tmp_node	int;
	num_nodes	int;
begin
	nodenames_query := 'SELECT node_name FROM pgxc_node WHERE node_type = ''D'''; 
	cr_command := 'CREATE TABLE ' || tab_schema || ' DISTRIBUTE BY ' || distribution || ' TO NODE (';
	for nodename in execute nodenames_query loop
		nodes := array_append(nodes, nodename);
	end loop;
	nodenames := '';
	sep := '';
	num_nodes := array_length(nodes, 1);
	foreach node in array nodenums loop
		tmp_node := node;
		if (tmp_node < 1 or tmp_node > num_nodes) then
			tmp_node := tmp_node % num_nodes;
			if (tmp_node < 1) then
				tmp_node := num_nodes; 
			end if;
		end if;
		nodenames := nodenames || sep || nodes[tmp_node];
		sep := ', ';
	end loop;
	cr_command := cr_command || nodenames;
	cr_command := cr_command || ')';

	execute cr_command;
end;
$$;
-- This file contains tests for Fast Query Shipping (FQS) for queries involving
-- a single table

-- Testset 1 for distributed table (by roundrobin)
select create_table_nodes('tab1_rr(val int, val2 int)', '{1, 2, 3}'::int[], 'roundrobin', NULL);
insert into tab1_rr values (1, 2);
insert into tab1_rr values (2, 4);
insert into tab1_rr values (5, 3);
insert into tab1_rr values (7, 8);
insert into tab1_rr values (9, 2);
explain (verbose on, nodes off, num_nodes on, costs off) insert into tab1_rr values (9, 2);
-- simple select
-- should get FQSed
select val, val2 + 2, case val when val2 then 'val and val2 are same' else 'val and val2 are not same' end from tab1_rr where val2 = 4;
explain (verbose on, nodes off, costs off) select val, val2 + 2, case val when val2 then 'val and val2 are same' else 'val and val2 are not same' end from tab1_rr where val2 = 4;
-- should not get FQSed because of aggregates
select sum(val), avg(val), count(*) from tab1_rr;
explain (verbose on, nodes off, costs off) select sum(val), avg(val), count(*) from tab1_rr;
-- should not get FQSed because of window functions
select first_value(val) over (partition by val2 order by val) from tab1_rr;
explain (verbose on, nodes off, costs off) select first_value(val) over (partition by val2 order by val) from tab1_rr;
-- should not get FQSed because of LIMIT clause
select * from tab1_rr where val2 = 3 limit 1;
explain (verbose on, nodes off, costs off) select * from tab1_rr where val2 = 3 limit 1;
-- should not FQSed because of OFFSET clause
select * from tab1_rr where val2 = 4 offset 1;
explain (verbose on, nodes off, costs off) select * from tab1_rr where val2 = 4 offset 1;
-- should not get FQSed because of SORT clause
select * from tab1_rr order by val;
explain (verbose on, nodes off, costs off) select * from tab1_rr order by val;
-- should not get FQSed because of DISTINCT clause
select distinct val, val2 from tab1_rr where val2 = 8;
explain (verbose on, nodes off, costs off) select distinct val, val2 from tab1_rr where val2 = 8;
-- should not get FQSed because of GROUP clause
select val, val2 from tab1_rr where val2 = 8 group by val, val2;
explain (verbose on, nodes off, costs off) select val, val2 from tab1_rr where val2 = 8 group by val, val2;
-- should not get FQSed because of HAVING clause
select sum(val) from tab1_rr where val2 = 2 group by val2 having sum(val) > 1;
explain (verbose on, nodes off, costs off) select sum(val) from tab1_rr where val2 = 2 group by val2 having sum(val) > 1;

-- tests for node reduction by application of quals, for roundrobin node
-- reduction is not applicable. Having query not FQSed because of existence of ORDER BY,
-- implies that nodes did not get reduced.
select * from tab1_rr where val = 7;
explain (verbose on, nodes off, costs off) select * from tab1_rr where val = 7;
select * from tab1_rr where val = 7 or val = 2 order by val;
explain (verbose on, nodes off, costs off) select * from tab1_rr where val = 7 or val = 2 order by val;
select * from tab1_rr where val = 7 and val2 = 8;
explain (verbose on, nodes off, costs off) select * from tab1_rr where val = 7 and val2 = 8 order by val;
select * from tab1_rr where val = 3 + 4 and val2 = 8 order by val;
explain (verbose on, nodes off, costs off) select * from tab1_rr where val = 3 + 4 order by val;
select * from tab1_rr where val = char_length('len')+4 order by val;
explain (verbose on, nodes off, costs off) select * from tab1_rr where val = char_length('len')+4 order by val;
-- insert some more values 
insert into tab1_rr values (7, 2); 
select avg(val) from tab1_rr where val = 7;
explain (verbose on, nodes off, costs off) select avg(val) from tab1_rr where val = 7;
select val, val2 from tab1_rr where val = 7 order by val2;
explain (verbose on, nodes off, costs off) select val, val2 from tab1_rr where val = 7 order by val2;
select distinct val2 from tab1_rr where val = 7 order by val2;
explain (verbose on, nodes off, costs off) select distinct val2 from tab1_rr where val = 7 order by val2;
-- DMLs
update tab1_rr set val2 = 1000 where val = 7; 
explain (verbose on, nodes off, costs off) update tab1_rr set val2 = 1000 where val = 7; 
select * from tab1_rr where val = 7;
delete from tab1_rr where val = 7; 
explain (verbose on, costs off) delete from tab1_rr where val = 7; 
select * from tab1_rr where val = 7;

-- Testset 2 for distributed tables (by hash)
select cr_table('tab1_hash(val int, val2 int)', '{1, 2, 3}'::int[], 'hash(val)');
insert into tab1_hash values (1, 2);
insert into tab1_hash values (2, 4);
insert into tab1_hash values (5, 3);
insert into tab1_hash values (7, 8);
insert into tab1_hash values (9, 2);
explain (verbose on, costs off) insert into tab1_hash values (9, 2);
-- simple select
-- should get FQSed
select val, val2 + 2, case val when val2 then 'val and val2 are same' else 'val and val2 are not same' end from tab1_hash where val2 = 4;
explain (verbose on, nodes off, costs off) select val, val2 + 2, case val when val2 then 'val and val2 are same' else 'val and val2 are not same' end from tab1_hash where val2 = 2;
-- should not get FQSed because of aggregates
select sum(val), avg(val), count(*) from tab1_hash;
explain (verbose on, nodes off, costs off) select sum(val), avg(val), count(*) from tab1_hash;
-- should not get FQSed because of window functions
select first_value(val) over (partition by val2 order by val) from tab1_hash;
explain (verbose on, nodes off, costs off) select first_value(val) over (partition by val2 order by val) from tab1_hash;
-- should not get FQSed because of LIMIT clause
select * from tab1_hash where val2 = 3 limit 1;
explain (verbose on, nodes off, costs off) select * from tab1_hash where val2 = 3 limit 1;
-- should not FQSed because of OFFSET clause
select * from tab1_hash where val2 = 4 offset 1;
explain (verbose on, nodes off, costs off) select * from tab1_hash where val2 = 4 offset 1;
-- should not get FQSed because of SORT clause
select * from tab1_hash order by val;
explain (verbose on, nodes off, costs off) select * from tab1_hash order by val;
-- should get FQSed because DISTINCT clause contains distkey
select distinct val, val2 from tab1_hash where val2 = 8;
explain (verbose on, nodes off, costs off) select distinct val, val2 from tab1_hash where val2 = 8;
-- should get FQSed because GROUP BY clause uses distkey
select val, val2 from tab1_hash where val2 = 8 group by val, val2;
explain (verbose on, nodes off, costs off) select val, val2 from tab1_hash where val2 = 8 group by val, val2;
-- should not get FQSed because of HAVING clause
select sum(val) from tab1_hash where val2 = 2 group by val2 having sum(val) > 1;
explain (verbose on, nodes off, costs off) select sum(val) from tab1_hash where val2 = 2 group by val2 having sum(val) > 1;

-- tests for node reduction by application of quals. Having query FQSed because of
-- existence of ORDER BY, implies that nodes got reduced.
select * from tab1_hash where val = 7;
explain (verbose on, nodes off, costs off, num_nodes on) select * from tab1_hash where val = 7;
select * from tab1_hash where val = 7 or val = 2 order by val;
explain (verbose on, nodes off, costs off) select * from tab1_hash where val = 7 or val = 2 order by val;
select * from tab1_hash where val = 7 and val2 = 8;
explain (verbose on, nodes off, costs off, num_nodes on) select * from tab1_hash where val = 7 and val2 = 8;
select * from tab1_hash where val = 3 + 4 and val2 = 8;
explain (verbose on, nodes off, costs off, num_nodes on) select * from tab1_hash where val = 3 + 4;
select * from tab1_hash where val = char_length('len')+4;
explain (verbose on, nodes off, costs off, num_nodes on) select * from tab1_hash where val = char_length('len')+4;
-- insert some more values 
insert into tab1_hash values (7, 2); 
select avg(val) from tab1_hash where val = 7;
explain (verbose on, nodes off, costs off, num_nodes on) select avg(val) from tab1_hash where val = 7;
select val, val2 from tab1_hash where val = 7 order by val2;
explain (verbose on, nodes off, costs off, num_nodes on) select val, val2 from tab1_hash where val = 7 order by val2;
select distinct val2 from tab1_hash where val = 7 order by val2;
explain (verbose on, nodes off, costs off, num_nodes on) select distinct val2 from tab1_hash where val = 7 order by val2;
-- DMLs
update tab1_hash set val2 = 1000 where val = 7; 
explain (verbose on, nodes off, costs off) update tab1_hash set val2 = 1000 where val = 7; 
select * from tab1_hash where val = 7;
delete from tab1_hash where val = 7; 
explain (verbose on, costs off) delete from tab1_hash where val = 7; 
select * from tab1_hash where val = 7;

-- Testset 3 for distributed tables (by modulo)
select cr_table('tab1_modulo(val int, val2 int)', '{1, 2, 3}'::int[], 'modulo(val)');
insert into tab1_modulo values (1, 2);
insert into tab1_modulo values (2, 4);
insert into tab1_modulo values (5, 3);
insert into tab1_modulo values (7, 8);
insert into tab1_modulo values (9, 2);
explain (verbose on, costs off) insert into tab1_modulo values (9, 2);
-- simple select
-- should get FQSed
select val, val2 + 2, case val when val2 then 'val and val2 are same' else 'val and val2 are not same' end from tab1_modulo where val2 = 4;
explain (verbose on, nodes off, costs off) select val, val2 + 2, case val when val2 then 'val and val2 are same' else 'val and val2 are not same' end from tab1_modulo where val2 = 4;
-- should not get FQSed because of aggregates
select sum(val), avg(val), count(*) from tab1_modulo;
explain (verbose on, nodes off, costs off) select sum(val), avg(val), count(*) from tab1_modulo;
-- should not get FQSed because of window functions
select first_value(val) over (partition by val2 order by val) from tab1_modulo;
explain (verbose on, nodes off, costs off) select first_value(val) over (partition by val2 order by val) from tab1_modulo;
-- should not get FQSed because of LIMIT clause
select * from tab1_modulo where val2 = 3 limit 1;
explain (verbose on, nodes off, costs off) select * from tab1_modulo where val2 = 3 limit 1;
-- should not FQSed because of OFFSET clause
select * from tab1_modulo where val2 = 4 offset 1;
explain (verbose on, nodes off, costs off) select * from tab1_modulo where val2 = 4 offset 1;
-- should not get FQSed because of SORT clause
select * from tab1_modulo order by val;
explain (verbose on, nodes off, costs off) select * from tab1_modulo order by val;
-- should get FQSed because DISTINCT clause contains distkey
select distinct val, val2 from tab1_modulo where val2 = 8;
explain (verbose on, nodes off, costs off) select distinct val, val2 from tab1_modulo where val2 = 8;
-- should get FQSed because GROUP BY clause uses distkey
select val, val2 from tab1_modulo where val2 = 8 group by val, val2;
explain (verbose on, nodes off, costs off) select val, val2 from tab1_modulo where val2 = 8 group by val, val2;
-- should not get FQSed because of HAVING clause
select sum(val) from tab1_modulo where val2 = 2 group by val2 having sum(val) > 1;
explain (verbose on, nodes off, costs off) select sum(val) from tab1_modulo where val2 = 2 group by val2 having sum(val) > 1;

-- tests for node reduction by application of quals. Having query FQSed because of
-- existence of ORDER BY, implies that nodes got reduced.
select * from tab1_modulo where val = 7;
explain (verbose on, nodes off, costs off, num_nodes on) select * from tab1_modulo where val = 7;
select * from tab1_modulo where val = 7 or val = 2 order by val;
explain (verbose on, nodes off, costs off) select * from tab1_modulo where val = 7 or val = 2 order by val;
select * from tab1_modulo where val = 7 and val2 = 8;
explain (verbose on, nodes off, costs off, num_nodes on) select * from tab1_modulo where val = 7 and val2 = 8;
select * from tab1_modulo where val = 3 + 4 and val2 = 8;
explain (verbose on, nodes off, costs off, num_nodes on) select * from tab1_modulo where val = 3 + 4;
select * from tab1_modulo where val = char_length('len')+4;
explain (verbose on, nodes off, costs off, num_nodes on) select * from tab1_modulo where val = char_length('len')+4;
-- insert some more values 
insert into tab1_modulo values (7, 2); 
select avg(val) from tab1_modulo where val = 7;
explain (verbose on, nodes off, costs off, num_nodes on) select avg(val) from tab1_modulo where val = 7;
select val, val2 from tab1_modulo where val = 7 order by val2;
explain (verbose on, nodes off, costs off, num_nodes on) select val, val2 from tab1_modulo where val = 7 order by val2;
select distinct val2 from tab1_modulo where val = 7;
explain (verbose on, nodes off, costs off, num_nodes on) select distinct val2 from tab1_modulo where val = 7;
-- DMLs
update tab1_modulo set val2 = 1000 where val = 7; 
explain (verbose on, nodes off, costs off) update tab1_modulo set val2 = 1000 where val = 7; 
select * from tab1_modulo where val = 7;
delete from tab1_modulo where val = 7; 
explain (verbose on, costs off) delete from tab1_modulo where val = 7; 
select * from tab1_modulo where val = 7;

-- Testset 4 for replicated tables, for replicated tables, unless the expression
-- is itself unshippable, any query involving a single replicated table is shippable
select cr_table('tab1_replicated(val int, val2 int)', '{1, 2, 3}'::int[], 'replication');
insert into tab1_replicated values (1, 2);
insert into tab1_replicated values (2, 4);
insert into tab1_replicated values (5, 3);
insert into tab1_replicated values (7, 8);
insert into tab1_replicated values (9, 2);
explain (verbose on, nodes off, costs off) insert into tab1_replicated values (9, 2);
-- simple select
select * from tab1_replicated order by val;
explain (num_nodes on, verbose on, nodes off, costs off) select * from tab1_replicated;
select sum(val), avg(val), count(*) from tab1_replicated;
explain (num_nodes on, verbose on, nodes off, costs off) select sum(val), avg(val), count(*) from tab1_replicated;
select first_value(val) over (partition by val2 order by val) from tab1_replicated;
explain (num_nodes on, verbose on, nodes off, costs off) select first_value(val) over (partition by val2 order by val) from tab1_replicated;
select * from tab1_replicated where val2 = 2 limit 2;
explain (num_nodes on, verbose on, nodes off, costs off) select * from tab1_replicated where val2 = 2 limit 2;
select * from tab1_replicated where val2 = 4 offset 1;
explain (num_nodes on, verbose on, nodes off, costs off) select * from tab1_replicated where val2 = 4 offset 1;
select * from tab1_replicated order by val;
explain (num_nodes on, verbose on, nodes off, costs off) select * from tab1_replicated order by val;
select distinct val, val2 from tab1_replicated order by 1, 2;
explain (num_nodes on, verbose on, nodes off, costs off) select distinct val, val2 from tab1_replicated order by 1, 2;
explain (num_nodes on, verbose on, nodes off, costs off) select distinct val, val2 from tab1_replicated;
select val, val2 from tab1_replicated group by val, val2 order by 1, 2;
explain (num_nodes on, verbose on, nodes off, costs off) select val, val2 from tab1_replicated group by val, val2 order by 1, 2;
explain (num_nodes on, verbose on, nodes off, costs off) select val, val2 from tab1_replicated group by val, val2;
select sum(val) from tab1_replicated group by val2 having sum(val) > 1 order by 1;
explain (num_nodes on, verbose on, nodes off, costs off) select sum(val) from tab1_replicated group by val2 having sum(val) > 1 order by 1;
explain (num_nodes on, verbose on, nodes off, costs off) select sum(val) from tab1_replicated group by val2 having sum(val) > 1;
-- DMLs
update tab1_replicated set val2 = 1000 where val = 7; 
explain (verbose on, nodes off, costs off) update tab1_replicated set val2 = 1000 where val = 7; 
select * from tab1_replicated where val = 7;
delete from tab1_replicated where val = 7; 
explain (verbose on, costs off) delete from tab1_replicated where val = 7; 
select * from tab1_replicated where val = 7;

-- Constant subquery
create table subquery_fqs(id int, a varchar, c int) distribute by shard(id);
insert into subquery_fqs values(1,'gd', 2);
insert into subquery_fqs values(1,'zj', 2);
insert into subquery_fqs values(1,'sz', 2);
explain select * from subquery_fqs t join (select 1 id, 'gd' a, 2 c from dual union select 1 id, 'sz' a, 2 c union select 1 id, 'zj' a, 2 c from dual) t2 ON (t.id = t2.id and t.a = t2.a);
select * from subquery_fqs t join (select 1 id, 'gd' a, 2 c from dual union select 1 id, 'sz' a, 2 c union select 1 id, 'zj' a, 2 c from dual) t2 ON (t.id = t2.id and t.a = t2.a);

-- Support subquery FQS only if subquery distributed on same DN with main query(only 1 DN node)
explain  (num_nodes on, verbose on, nodes off, costs off) select * from subquery_fqs t1 where t1.id = 1 and t1.c IN (select c from subquery_fqs t2 where t2.id=1);
select * from subquery_fqs t1 where t1.id = 1 and t1.c IN (select c from subquery_fqs t2 where t2.id=1);
explain  (num_nodes on, verbose on, nodes off, costs off) select * from subquery_fqs t1 where t1.id = 1 and t1.c = (select c from subquery_fqs t2 where t2.id=1 order by c limit 1);
select * from subquery_fqs t1 where t1.id = 1 and t1.c = (select c from subquery_fqs t2 where t2.id=1 order by c limit 1);
explain  (num_nodes on, verbose on, nodes off, costs off) select * from subquery_fqs t1 where t1.id = 1 and t1.c = (select max(c) from subquery_fqs t2 where t2.id=1);
select * from subquery_fqs t1 where t1.id = 1 and t1.c = (select max(c) from subquery_fqs t2 where t2.id=1);
explain  (num_nodes on, verbose on, nodes off, costs off) select * from (select * from subquery_fqs where id = 1 order by c limit 1) where c = 2;
select * from (select * from subquery_fqs where id = 1 order by c limit 1) where c = 2;

set enable_oracle_compatible to true;
explain  (num_nodes on, verbose on, nodes off, costs off) select * from subquery_fqs t1 where t1.id in (1 ,1);
explain   select * from subquery_fqs t1 where t1.id in (1 ,1);
explain   select * from subquery_fqs t1 where t1.id in (1 ,1);
explain   select * from subquery_fqs t1 where t1.id in (1 ,1);
explain   select * from subquery_fqs t1 where t1.id in (1 ,1);
explain   select * from subquery_fqs t1 where t1.id in (1 ,1);
explain   select * from subquery_fqs t1 where t1.id in (1 ,1);
set enable_oracle_compatible to false;
explain  (num_nodes on, verbose on, nodes off, costs off) select * from subquery_fqs t1 where t1.id in (1 ,3);

drop table tab1_rr;
drop table tab1_hash;
drop table tab1_modulo;
drop table tab1_replicated;
drop table subquery_fqs;
drop function cr_table(varchar, int[], varchar); 
