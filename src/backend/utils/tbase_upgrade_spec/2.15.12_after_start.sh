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
    export LD_LIBRARY_PATH=${bin_dir}/lib:${LD_LIBRARY_PATH} && export PATH=${bin_dir}/bin:${PATH} && $bin_dir/bin/psql -h $host -p $port -d $db -U $user -t -c "$sql" | sed '/^\s*$/d'
}

dbs=$(execSql "select datname from pg_database where datname !='template0'" $dbname)
for db in ${dbs}
do
        echo $db
        execSql "create extension pg_stat_log" $db
        if [ $? -eq 0 ]
        then
                echo "create pg_stat_log on $host:$port:$db success"
        else
                echo "create pg_stat_log on $host:$port:$db failed"
        fi
done