
create or replace function execute_on_all_nodes(query varchar)
returns setof record language plpgsql as $D$
declare
	node_sql	varchar;
	nodename	varchar;
	nodenames_query varchar;
	res			record;
begin
	nodenames_query := 'SELECT node_name FROM pgxc_node';
	for nodename in execute nodenames_query loop
		node_sql = 'EXECUTE DIRECT ON (' || nodename || ') $$ ' || 'SELECT ''' ||
nodename || '''::text AS nodename, subquery.* FROM (' || query || ') subquery $$';
--		raise notice '%', node_sql;
		FOR res IN EXECUTE node_sql LOOP
			RETURN NEXT res;
		END LOOP;
	end loop;
end;
$D$;

SELECT r.nodename, r.status
	FROM execute_on_all_nodes('SELECT pg_msgmodule_set(121, -1, -1, ''LOG'')')
AS r(nodename text, status bool);

SELECT r.nodename, r.status
	FROM execute_on_all_nodes('SELECT pg_msgmodule_set(97, -1, -1, ''LOG'')')
AS r(nodename text, status bool);

SELECT r.nodename, r.status
	FROM execute_on_all_nodes('SELECT pg_msgmodule_set(98, -1, -1, ''LOG'')')
AS r(nodename text, status bool);

SELECT r.nodename, r.status
	FROM execute_on_all_nodes('SELECT pg_msgmodule_set(99, -1, -1, ''LOG'')')
AS r(nodename text, status bool);

SELECT r.nodename, r.status
	FROM execute_on_all_nodes('SELECT pg_msgmodule_enable_all(true)')
AS r(nodename text, status bool);

