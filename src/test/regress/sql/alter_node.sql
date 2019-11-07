--case_1:
CREATE NODE cn01_1 WITH (TYPE='coordinator', HOST='172.16.0.3', PORT=15431);
CREATE NODE cn01 WITH (TYPE='coordinator', HOST='172.16.0.3', PORT=15432, CLUSTER='tbase_cluster');
CREATE NODE cn01 WITH (TYPE='coordinator', HOST='172.16.0.3', PORT=15433, CLUSTER='pgxc_cluster_slave');

--expect:
--cn01_1 and cn01 with(15432) in tbase_cluster, and cn01 with(15433) in pgxc_cluster_slave;
select node_name,node_type,node_port,node_host,node_cluster_name from pgxc_node where node_name in ('cn01', 'cn01_1')order by oid;

--case_2:
ALTER NODE cn01 WITH (HOST='10.112.111.147', PORT=30003, CLUSTER='tbase_cluster');
--expect:
--error, CLUSTER could not be modified.
select node_name,node_type,node_port,node_host,node_cluster_name from pgxc_node where node_name in ('cn01', 'cn01_1')order by oid;


--case_3:
ALTER NODE cn01 WITH (HOST='10.112.111.147', PORT=30003);
ALTER NODE cn01 in pgxc_cluster_slave WITH (HOST='10.112.111.147', PORT=30003);
--expect:
--ok
select node_name,node_type,node_port,node_host,node_cluster_name from pgxc_node where node_name in ('cn01', 'cn01_1')order by oid;

--case_4:
drop node cn01;
--expect:
--only cn01 in tbase_cluster dropped.
select node_name,node_type,node_port,node_host,node_cluster_name from pgxc_node where node_name in ('cn01', 'cn01_1')order by oid;

--case_5:
drop node cn01_1 in tbase_cluster;
drop node cn01 in pgxc_cluster_slave;
--expect
--cn01 in pgxc_cluster_slave dropped, cn01_1 dropped.
select node_name,node_type,node_port,node_host,node_cluster_name from pgxc_node where node_name in ('cn01', 'cn01_1')order by oid;


