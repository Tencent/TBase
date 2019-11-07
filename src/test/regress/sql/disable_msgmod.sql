      
SELECT r.nodename, r.status
	FROM execute_on_all_nodes('SELECT pg_msgmodule_disable_all()')
AS r(nodename text, status bool);
