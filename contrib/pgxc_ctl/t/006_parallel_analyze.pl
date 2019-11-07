use strict;
use warnings;
use Cwd;
use Config;
use TestLib;
use Test::More tests => 1;

my $dataDirRoot="~/DATA/pgxl/nodes/";
$ENV{'PGXC_CTL_HOME'} = '/tmp/pgxc_ctl';
my $PGXC_CTL_HOME=$ENV{'PGXC_CTL_HOME'};

#delete related dirs for cleanup
system("rm -rf $dataDirRoot");
system("rm -rf $PGXC_CTL_HOME");

my $DEFAULT_DB="postgres";
my $TEST_DB="testdb";
my $COORD1_PORT=30001;


system_or_bail 'pgxc_ctl', 'prepare', 'minimal' ;

system_or_bail 'pgxc_ctl', 'init', 'all' ;

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

system_or_bail 'psql', '-p', "$COORD1_PORT", "$DEFAULT_DB",'-c', "CREATE DATABASE testdb;";

system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "CREATE TABLE ana1(col1 int, col2 text default 'ana1');";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "insert into ana1 select generate_series(1, 10000);";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "CREATE TABLE ana2(col1 int, col2 text default 'ana2');";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "insert into ana2 select generate_series(1, 10000);";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "CREATE TABLE ana3(col1 int, col2 text default 'ana3');";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "insert into ana3 select generate_series(1, 10000);";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "CREATE TABLE ana4(col1 int, col2 text default 'ana4');";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "insert into ana4 select generate_series(1, 10000);";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "CREATE TABLE ana5(col1 int, col2 text default 'ana5');";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "insert into ana5 select generate_series(1, 10000);";

# 30 jobs of anaylze running in parallel
system("psql -p 30001 testdb -f t/analyze_verbose.sql --echo-all --set AUTOCOMMIT=off &");
system("psql -p 30001 testdb -f t/analyze_verbose.sql --echo-all --set AUTOCOMMIT=off &");
system("psql -p 30001 testdb -f t/analyze_verbose.sql --echo-all --set AUTOCOMMIT=off &");
system("psql -p 30001 testdb -f t/analyze_verbose.sql --echo-all --set AUTOCOMMIT=off &");
system("psql -p 30001 testdb -f t/analyze_verbose.sql --echo-all --set AUTOCOMMIT=off &");
system("psql -p 30001 testdb -f t/analyze_verbose.sql --echo-all --set AUTOCOMMIT=off &");
system("psql -p 30001 testdb -f t/analyze_verbose.sql --echo-all --set AUTOCOMMIT=off &");
system("psql -p 30001 testdb -f t/analyze_verbose.sql --echo-all --set AUTOCOMMIT=off &");
system("psql -p 30001 testdb -f t/analyze_verbose.sql --echo-all --set AUTOCOMMIT=off &");
system("psql -p 30001 testdb -f t/analyze_verbose.sql --echo-all --set AUTOCOMMIT=off &");
system("psql -p 30001 testdb -f t/analyze_verbose.sql --echo-all --set AUTOCOMMIT=off &");
system("psql -p 30001 testdb -f t/analyze_verbose.sql --echo-all --set AUTOCOMMIT=off &");
system("psql -p 30001 testdb -f t/analyze_verbose.sql --echo-all --set AUTOCOMMIT=off &");
system("psql -p 30001 testdb -f t/analyze_verbose.sql --echo-all --set AUTOCOMMIT=off &");
system("psql -p 30001 testdb -f t/analyze_verbose.sql --echo-all --set AUTOCOMMIT=off &");
system("psql -p 30001 testdb -f t/analyze_verbose.sql --echo-all --set AUTOCOMMIT=off &");
system("psql -p 30001 testdb -f t/analyze_verbose.sql --echo-all --set AUTOCOMMIT=off &");
system("psql -p 30001 testdb -f t/analyze_verbose.sql --echo-all --set AUTOCOMMIT=off &");
system("psql -p 30001 testdb -f t/analyze_verbose.sql --echo-all --set AUTOCOMMIT=off &");
system("psql -p 30001 testdb -f t/analyze_verbose.sql --echo-all --set AUTOCOMMIT=off &");
system("psql -p 30001 testdb -f t/analyze_verbose.sql --echo-all --set AUTOCOMMIT=off &");
system("psql -p 30001 testdb -f t/analyze_verbose.sql --echo-all --set AUTOCOMMIT=off &");
system("psql -p 30001 testdb -f t/analyze_verbose.sql --echo-all --set AUTOCOMMIT=off &");
system("psql -p 30001 testdb -f t/analyze_verbose.sql --echo-all --set AUTOCOMMIT=off &");
system("psql -p 30001 testdb -f t/analyze_verbose.sql --echo-all --set AUTOCOMMIT=off &");
system("psql -p 30001 testdb -f t/analyze_verbose.sql --echo-all --set AUTOCOMMIT=off &");
system("psql -p 30001 testdb -f t/analyze_verbose.sql --echo-all --set AUTOCOMMIT=off &");
system("psql -p 30001 testdb -f t/analyze_verbose.sql --echo-all --set AUTOCOMMIT=off &");
system("psql -p 30001 testdb -f t/analyze_verbose.sql --echo-all --set AUTOCOMMIT=off &");
command_ok([ 'psql', '-p', "$COORD1_PORT", "$TEST_DB", '-c', "analyze verbose;" ], 'analyze verbose ');
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "select pg_sleep(5);";

system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "DROP TABLE ana1;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "DROP TABLE ana2;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "DROP TABLE ana3;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "DROP TABLE ana4;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "DROP TABLE ana5;";
#add cleanup
system_or_bail 'pgxc_ctl', 'clean', 'all' ;

#delete related dirs for cleanup
system("rm -rf $dataDirRoot");
system("rm -rf $PGXC_CTL_HOME");
