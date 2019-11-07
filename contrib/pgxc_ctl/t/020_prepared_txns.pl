use strict;
use warnings;
use Cwd;
use Config;
use TestLib;
use Test::More tests => 5;

my $dataDirRoot="~/DATA/pgxl/nodes/";
$ENV{'PGXC_CTL_HOME'} = '/tmp/pgxc_ctl';
my $PGXC_CTL_HOME=$ENV{'PGXC_CTL_HOME'};

#delete related dirs for cleanup
system("echo '==========clear existing configuration==========='");
system("rm -rf $dataDirRoot");
system("rm -rf $PGXC_CTL_HOME");

my $GTM_HOST="localhost";
my $COORD1_HOST="localhost";
my $COORD1_PORT=30001;
my $COORD2_PORT=30002;
my $COORD2_HOST="localhost";
my $DN1_HOST="localhost";
my $DN2_HOST="localhost";
my $DEFAULT_DB="postgres";
my $TEST_DB="testdb";

system("echo '==========prepare configuration==========='");
system_or_bail 'pgxc_ctl', 'prepare', 'config', 'empty' ;

system_or_bail 'pgxc_ctl', 'add', 'gtm', 'master', 'gtm', "$GTM_HOST", '20001', "$dataDirRoot/gtm" ;

system_or_bail 'pgxc_ctl', 'add', 'coordinator', 'master', 'coord1', "$COORD1_HOST", '30001', '30011', "$dataDirRoot/coord_master.1", 'none', 'none';

system_or_bail 'pgxc_ctl', 'add', 'coordinator', 'master', 'coord2', "$COORD2_HOST", '30002', '30012', "$dataDirRoot/coord_master.2", 'none', 'none';

system_or_bail 'pgxc_ctl', 'add', 'datanode', 'master', 'dn1', "$DN1_HOST", '40001', '40011', "$dataDirRoot/dn_master.1", 'none', 'none', 'none' ;

system_or_bail 'pgxc_ctl', 'add', 'datanode', 'master', 'dn2', "$DN2_HOST", '40002', '40012', "$dataDirRoot/dn_master.2", 'none', 'none', 'none' ;

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

#add data

system("echo '==========populate data==========='");

system_or_bail 'psql', '-p', "$COORD1_PORT", "$DEFAULT_DB",'-c', "CREATE DATABASE testdb;";

system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "SELECT * FROM pgxc_node;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "CREATE TABLE disttab(col1 int, col2 int) DISTRIBUTE BY HASH(col1);";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "CREATE TABLE repltab (col1 int, col2 int) DISTRIBUTE BY REPLICATION;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "INSERT INTO disttab VALUES(1,1);";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "INSERT INTO disttab VALUES(2,2);";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "INSERT INTO disttab VALUES(3,3);";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "INSERT INTO disttab VALUES(4,4);";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "INSERT INTO disttab VALUES(5,5);";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "INSERT INTO disttab VALUES(6,6);";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "INSERT INTO disttab VALUES(7,7);";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "INSERT INTO disttab VALUES(8,8);";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "INSERT INTO disttab VALUES(9,9);";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "INSERT INTO disttab VALUES(10,10);";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "INSERT INTO disttab VALUES(11,11);";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "INSERT INTO disttab VALUES(12,12);";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "INSERT INTO disttab VALUES(13,13);";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "INSERT INTO disttab VALUES(14,14);";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "INSERT INTO disttab VALUES(15,15);";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "INSERT INTO disttab VALUES(16,16);";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "INSERT INTO disttab VALUES(17,17);";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "INSERT INTO disttab VALUES(18,18);";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "INSERT INTO disttab VALUES(19,19);";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "INSERT INTO disttab VALUES(20,20);";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "INSERT INTO repltab VALUES (generate_series(1,100), generate_series(101, 200));";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "SELECT count(*) FROM disttab;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "SELECT xc_node_id, count(*) FROM disttab GROUP BY xc_node_id;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "SELECT xc_node_id, * FROM disttab;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "SELECT count(*) FROM repltab;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "SELECT xc_node_id, count(*) FROM repltab GROUP BY xc_node_id;";

# test with killing nodes
# do data sanity check

system("psql -p 30001 testdb -f t/prep_tx1.sql --echo-all --set AUTOCOMMIT=off &");
system("echo '==========kill dn1 -- with data==========='");
system_or_bail 'pgxc_ctl', 'kill', 'datanode', 'master', 'dn1' ;
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "select pg_sleep(3);";
system_or_bail 'pgxc_ctl', 'start', 'datanode', 'master', 'dn1' ;


system("echo '==========data sanity check==========='");

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "SELECT count(*) FROM disttab;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "SELECT xc_node_id, count(*) FROM disttab GROUP BY xc_node_id;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "SELECT count(*) FROM repltab;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "SELECT xc_node_id, count(*) FROM repltab GROUP BY xc_node_id;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "SELECT * from pg_prepared_xacts;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "EXECUTE DIRECT ON (dn1) 'SELECT * from pg_prepared_xacts;';";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "EXECUTE DIRECT ON (dn2) 'SELECT * from pg_prepared_xacts;';";
system("echo '==========checking on coord1==========='");
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "EXECUTE DIRECT ON (coord1) 'SELECT * from pg_prepared_xacts;';";
system("echo '==========checking on coord2==========='");
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "EXECUTE DIRECT ON (coord2) 'SELECT * from pg_prepared_xacts;';";


system("psql -p 30001 testdb -f t/prep_tx2.sql --echo-all --set AUTOCOMMIT=off &");
system("echo '==========kill coord1 -- with data==========='");
system_or_bail 'pgxc_ctl', 'kill', 'coordinator', 'master', 'coord1' ;
system_or_bail 'sleep', '3';
system_or_bail 'pgxc_ctl', 'start', 'coordinator', 'master', 'coord1' ;


system("echo '==========data sanity check==========='");

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "SELECT count(*) FROM disttab;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "SELECT xc_node_id, count(*) FROM disttab GROUP BY xc_node_id;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "SELECT count(*) FROM repltab;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "SELECT xc_node_id, count(*) FROM repltab GROUP BY xc_node_id;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "SELECT * from pg_prepared_xacts;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "EXECUTE DIRECT ON (dn1) 'SELECT * from pg_prepared_xacts;';";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "EXECUTE DIRECT ON (dn2) 'SELECT * from pg_prepared_xacts;';";
system("echo '==========checking on coord1==========='");
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "EXECUTE DIRECT ON (coord1) 'SELECT * from pg_prepared_xacts;';";
system("echo '==========checking on coord2==========='");
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "EXECUTE DIRECT ON (coord2) 'SELECT * from pg_prepared_xacts;';";



system("psql -p 30001 testdb -f t/prep_tx3.sql --echo-all --set AUTOCOMMIT=off &");
system("echo '==========kill coord2 -- with data==========='");
system_or_bail 'pgxc_ctl', 'kill', 'coordinator', 'master', 'coord2' ;
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "select pg_sleep(3);";
system_or_bail 'pgxc_ctl', 'start', 'coordinator', 'master', 'coord2' ;


system("echo '==========data sanity check==========='");

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "SELECT count(*) FROM disttab;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "SELECT xc_node_id, count(*) FROM disttab GROUP BY xc_node_id;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "SELECT count(*) FROM repltab;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "SELECT xc_node_id, count(*) FROM repltab GROUP BY xc_node_id;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "SELECT * from pg_prepared_xacts;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "EXECUTE DIRECT ON (dn1) 'SELECT * from pg_prepared_xacts;';";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "EXECUTE DIRECT ON (dn2) 'SELECT * from pg_prepared_xacts;';";
system("echo '==========checking on coord1==========='");
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "EXECUTE DIRECT ON (coord1) 'SELECT * from pg_prepared_xacts;';";
system("echo '==========checking on coord2==========='");
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "EXECUTE DIRECT ON (coord2) 'SELECT * from pg_prepared_xacts;';";



system("psql -p 30001 testdb -f t/prep_tx4.sql --echo-all --set AUTOCOMMIT=off &");
system("echo '==========kill gtm master -- with data==========='");
system_or_bail 'pgxc_ctl', 'kill', 'gtm', 'master', 'gtm' ;
system_or_bail 'sleep', '3';
system_or_bail 'pgxc_ctl', 'start', 'gtm', 'master', 'gtm' ;
system_or_bail 'sleep', '10';

system("echo '==========data sanity check==========='");

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "SELECT count(*) FROM disttab;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "SELECT xc_node_id, count(*) FROM disttab GROUP BY xc_node_id;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "SELECT count(*) FROM repltab;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "SELECT xc_node_id, count(*) FROM repltab GROUP BY xc_node_id;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "SELECT * from pg_prepared_xacts;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "EXECUTE DIRECT ON (dn1) 'SELECT * from pg_prepared_xacts;';";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "EXECUTE DIRECT ON (dn2) 'SELECT * from pg_prepared_xacts;';";
system("echo '==========checking on coord1==========='");
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "EXECUTE DIRECT ON (coord1) 'SELECT * from pg_prepared_xacts;';";
system("echo '==========checking on coord2==========='");
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "EXECUTE DIRECT ON (coord2) 'SELECT * from pg_prepared_xacts;';";



system("echo '==========commit prepared transactions==========='");

command_fails([ 'psql', '-p', "$COORD1_PORT", "$TEST_DB", '-c', "COMMIT PREPARED 'foo1';" ], 'cannot commit prepared transaction foo1 ');
command_fails([ 'psql', '-p', "$COORD1_PORT", "$TEST_DB", '-c', "COMMIT PREPARED 'foo2';" ], 'cannot commit prepared transaction foo2');
command_ok([ 'psql', '-p', "$COORD1_PORT", "$TEST_DB", '-c', "EXECUTE DIRECT ON (coord2) 'COMMIT PREPARED ''foo3'';';" ], 'commit prepared transaction foo3 directly on coord2 ');
command_ok([ 'psql', '-p', "$COORD2_PORT", "$TEST_DB", '-c', "COMMIT PREPARED 'foo3';" ], 'commit prepared transaction foo3 connecting to coord2 ');
command_fails([ 'psql', '-p', "$COORD1_PORT", "$TEST_DB", '-c', "COMMIT PREPARED 'foo4';" ], 'cannot commit prepared transaction foo4 ');

system("echo '==========data sanity check==========='");

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "SELECT count(*) FROM disttab;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "SELECT xc_node_id, count(*) FROM disttab GROUP BY xc_node_id;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "SELECT count(*) FROM repltab;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "SELECT xc_node_id, count(*) FROM repltab GROUP BY xc_node_id;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "SELECT * from pg_prepared_xacts;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "EXECUTE DIRECT ON (dn1) 'SELECT * from pg_prepared_xacts;';";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "EXECUTE DIRECT ON (dn2) 'SELECT * from pg_prepared_xacts;';";
system("echo '==========checking on coord1==========='");
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "EXECUTE DIRECT ON (coord1) 'SELECT * from pg_prepared_xacts;';";
system("echo '==========checking on coord2==========='");
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "EXECUTE DIRECT ON (coord2) 'SELECT * from pg_prepared_xacts;';";

#add cleanup
system_or_bail 'pgxc_ctl', 'clean', 'all' ;


#delete related dirs for cleanup
system("rm -rf $dataDirRoot");
system("rm -rf $PGXC_CTL_HOME");

