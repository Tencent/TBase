#!/bin/bash
data_dir=$1
bin_dir=$2
version=$3

port=`grep -E '^\s*port\s*=' $data_dir/postgresql.conf |tail -n1 |awk -F '=' '{print $2}'`
host=`ip addr | grep 'state UP' -A2 | tail -n1 | awk '{print $2}' | cut -f1 -d '/'`
user=tbase
passwd=tbase@TBASE
dbname=postgres

execSql()
{
    local sql="$1"
    local db=$2
    local node_host="$3"
    local node_port=$4
    export LD_LIBRARY_PATH=${bin_dir}/lib:${LD_LIBRARY_PATH}
    export PATH=${bin_dir}/bin:${PATH}
    $bin_dir/bin/psql -h $node_host -p $node_port -d $db -U $user -t -c "$sql" | sed '/^\s*$/d'
}

getSeg()
{
    line="$1"
    segNum="$2"

    seg=$(echo "$line" | awk -F'|' '{print $segNum}' "segNum=$segNum")
    seg=$(echo $seg)
    echo $seg
}

nodeinfo=$(execSql "select node_host, node_port from pgxc_node where node_type='C' order by node_port asc limit 1" $dbname $host $port)

num=$(echo "$nodeinfo" | wc -l)
for ((i=1; i<=num; ++i)); do
    node_host=$(getSeg "$nodeinfo" 1)
    node_port=$(getSeg "$nodeinfo" 2)
    dbs=$(execSql "select datname from pg_database where datname !='template0'" $dbname $node_host $node_port)
    for db in ${dbs}
    do
        execSql "drop extension if exists pg_stat_log" $db $node_host $node_port
        if [ $? -eq 0 ]
        then
                echo "drop pg_stat_log on $node_host:$node_port:$db success"
        else
                echo "drop pg_stat_log on $node_host:$node_port:$db failed"
        fi
    done
done

