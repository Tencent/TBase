-- This file contains testcases for JOINs, it does not test the expressions
-- create the tables first
-- A function to create table on specified nodes 
create or replace function cr_table(tab_schema varchar, nodenums int[], distribution varchar, cmd_suffix varchar)
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
	if (cmd_suffix is not null) then
		cr_command := cr_command  || ' ' || cmd_suffix;
	end if;

	execute cr_command;
end;
$$;

select cr_table('tab1_rep (val int, val2 int)', '{1, 2, 3}'::int[], 'replication', NULL);
insert into tab1_rep (select * from generate_series(1, 5) a, generate_series(1, 5) b);
select cr_table('tab2_rep', '{2, 3, 4}'::int[], 'replication', 'as select * from tab1_rep');
select cr_table('tab3_rep', '{1, 3}'::int[], 'replication', 'as select * from tab1_rep');
select cr_table('tab4_rep', '{2, 4}'::int[], 'replication', 'as select * from tab1_rep');
select cr_table('tab1_mod', '{1, 2, 3}'::int[], 'modulo(val)', 'as select * from tab1_rep');
select cr_table('tab2_mod', '{2, 4}'::int[], 'modulo(val)', 'as select * from tab1_rep');
select cr_table('tab3_mod', '{1, 2, 3}'::int[], 'modulo(val)', 'as select * from tab1_rep');

-- Join involving replicated tables only, all of them should be shippable
select * from tab1_rep, tab2_rep where tab1_rep.val = tab2_rep.val and
										tab1_rep.val2 = tab2_rep.val2 and
										tab1_rep.val > 1 and tab1_rep.val < 4;
explain (num_nodes on, nodes off, costs off, verbose on) select * from tab1_rep, tab2_rep where tab1_rep.val = tab2_rep.val and
										tab1_rep.val2 = tab2_rep.val2 and
										tab1_rep.val > 3 and tab1_rep.val < 5;
select * from tab1_rep natural join tab2_rep 
			where tab2_rep.val > 2 and tab2_rep.val < 5;
explain (num_nodes on, nodes off, costs off, verbose on) select * from tab1_rep natural join tab2_rep
			where tab2_rep.val > 2 and tab2_rep.val < 5;
select * from tab1_rep join tab2_rep using (val, val2) join tab3_rep using (val, val2)
									where tab1_rep.val > 0 and tab2_rep.val < 3; 
explain (num_nodes on, nodes off, costs off, verbose on) select * from tab1_rep join tab2_rep using (val, val2) join tab3_rep using (val, val2)
							where tab1_rep.val > 0 and tab2_rep.val < 3; 
select * from tab1_rep natural join tab2_rep natural join tab3_rep
			where tab1_rep.val > 0 and tab2_rep.val < 3;
explain (num_nodes on, nodes off, costs off, verbose on) select * from tab1_rep natural join tab2_rep natural join tab3_rep
			where tab1_rep.val > 0 and tab2_rep.val < 3;
-- make sure in Joins which are shippable and involve only one node, aggregates
-- are shipped to
select avg(tab1_rep.val) from tab1_rep natural join tab2_rep natural join tab3_rep
			where tab1_rep.val > 0 and tab2_rep.val < 3;
explain (num_nodes on, nodes off, costs off, verbose on) select avg(tab1_rep.val) from tab1_rep natural join tab2_rep natural join tab3_rep
			where tab1_rep.val > 0 and tab2_rep.val < 3;
-- the two replicated tables being joined do not have any node in common, the
-- query is not shippable
select * from tab3_rep natural join tab4_rep
			where tab3_rep.val > 2 and tab4_rep.val < 5;
explain (num_nodes on, nodes off, costs off, verbose on) select * from tab3_rep natural join tab4_rep
			where tab3_rep.val > 2 and tab4_rep.val < 5;

-- Join involving one distributed and one replicated table, with replicated
-- table existing on all nodes where distributed table exists. should be
-- shippable
select * from tab1_mod natural join tab1_rep
			where tab1_mod.val > 2 and tab1_rep.val < 4;
explain (verbose on, nodes off, costs off) select * from tab1_mod natural join tab1_rep
			where tab1_mod.val > 2 and tab1_rep.val < 4;

-- Join involving one distributed and one replicated table, with replicated
-- table existing on only some of the nodes where distributed table exists.
-- should not be shippable
select * from tab1_mod natural join tab4_rep
			where tab1_mod.val > 2 and tab4_rep.val < 4;
explain (verbose on, nodes off, costs off) select * from tab1_mod natural join tab4_rep
			where tab1_mod.val > 2 and tab4_rep.val < 4;

-- Join involving two distributed tables, never shipped
select * from tab1_mod natural join tab2_mod
			where tab1_mod.val > 2 and tab2_mod.val < 4;
explain (verbose on, nodes off, costs off) select * from tab1_mod natural join tab2_mod
			where tab1_mod.val > 2 and tab2_mod.val < 4;

-- Join involving a distributed table and two replicated tables, such that the
-- distributed table exists only on nodes common to replicated tables, try few
-- permutations
select * from tab2_rep natural join tab4_rep natural join tab2_mod
			where tab2_rep.val > 2 and tab4_rep.val < 4;
explain (verbose on, nodes off, costs off) select * from tab2_rep natural join tab4_rep natural join tab2_mod
			where tab2_rep.val > 2 and tab4_rep.val < 4;
select * from tab4_rep natural join tab2_rep natural join tab2_mod
			where tab2_rep.val > 2 and tab4_rep.val < 4;
explain (verbose on, nodes off, costs off) select * from tab4_rep natural join tab2_rep natural join tab2_mod
			where tab2_rep.val > 2 and tab4_rep.val < 4;
select * from tab2_rep natural join tab2_mod natural join tab4_rep
			where tab2_rep.val > 2 and tab4_rep.val < 4;
explain (verbose on, nodes off, costs off) select * from tab2_rep natural join tab2_mod natural join tab4_rep
			where tab2_rep.val > 2 and tab4_rep.val < 4;

-- qualifications on distributed tables
-- In case of 2,3,4 datanodes following join should get shipped completely
select * from tab1_mod natural join tab4_rep where tab1_mod.val = 1 order by tab1_mod.val2;
explain (verbose on, nodes off, costs off, num_nodes on) select * from tab1_mod natural join tab4_rep where tab1_mod.val = 1 order by tab1_mod.val2;
-- following join between distributed tables should get FQSed because both of
-- them reduce to a single node
select * from tab1_mod join tab2_mod using (val2)
		where tab1_mod.val = 1 and tab2_mod.val = 2 order by tab1_mod.val2;
explain (verbose on, nodes off, costs off, num_nodes on) select * from tab1_mod join tab2_mod using (val2)
		where tab1_mod.val = 1 and tab2_mod.val = 2 order by tab1_mod.val;

-- JOIN involving the distributed table with equi-JOIN on the distributed column
-- with same kind of distribution on same nodes.
select * from tab1_mod, tab3_mod where tab1_mod.val = tab3_mod.val and tab1_mod.val = 1;
explain (verbose on, nodes off, costs off) select * from tab1_mod, tab3_mod
			where tab1_mod.val = tab3_mod.val and tab1_mod.val = 1;
-- DMLs involving JOINs are not FQSed
explain (verbose on, nodes off, costs off) update tab1_mod set val2 = 1000 from tab2_mod 
		where tab1_mod.val = tab2_mod.val and tab1_mod. val2 = tab2_mod.val2;
explain (verbose on, nodes off, costs off) delete from tab1_mod using tab2_mod
		where tab1_mod.val = tab2_mod.val and tab1_mod.val2 = tab2_mod.val2;
explain (verbose on, nodes off, costs off) update tab1_rep set val2 = 1000 from tab2_rep
		where tab1_rep.val = tab2_rep.val and tab1_rep.val2 = tab2_rep.val2;
explain (verbose on, nodes off, costs off) delete from tab1_rep using tab2_rep 
		where tab1_rep.val = tab2_rep.val and tab1_rep.val2 = tab2_rep.val2;

drop table tab1_rep;
drop table tab2_rep;
drop table tab3_rep;
drop table tab4_rep;
drop table tab1_mod;
drop table tab2_mod;
drop function cr_table(varchar, int[], varchar, varchar);
