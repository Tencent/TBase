use strict;
use warnings;
use Cwd;
use Config;
use TestLib;
use Test::More tests => 11;

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

# parallel scripts that drop and recreate roles

command_ok([ 'psql', '-p', "$COORD1_PORT", "$DEFAULT_DB",'-c', "CREATE DATABASE testdb;"], 'create database testdb ');
system("psql -p 30001 testdb -f t/role_recreate.sql --echo-all --set AUTOCOMMIT=off &");
command_ok([ 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "CREATE TABLE rol1(col1 int, col2 text default 'rol1');"], 'create table rol1 ');
command_ok([ 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "insert into rol1 select generate_series(1, 10000);"], 'insert to rol1 table ');
command_ok([ 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "CREATE TABLE rol2(col1 int, col2 text default 'rol2');"], 'create rol2 table ');
command_ok([ 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "insert into rol2 select generate_series(1, 10000);"], 'insert to rol2 table ');
command_ok([ 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "CREATE TABLE rol3(col1 int, col2 text default 'rol3');"], 'create rol3 table ');
command_ok([ 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "insert into rol3 select generate_series(1, 10000);"], 'insert to rol3 table ');
command_ok([ 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "CREATE TABLE rol4(col1 int, col2 text default 'rol4');"], 'create rol4 table ');
command_ok([ 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "insert into rol4 select generate_series(1, 10000);"], 'insert to rol4 table ');
command_ok([ 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "CREATE TABLE rol5(col1 int, col2 text default 'rol5');"], 'create rol5 table ');
command_ok([ 'psql', '-p', "$COORD1_PORT", "$TEST_DB", '-c', "insert into rol5 select generate_series(1, 10000);" ], 'insert to rol5 table ');


system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "DROP TABLE rol1;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "DROP TABLE rol2;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "DROP TABLE rol3;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "DROP TABLE rol4;";
system_or_bail 'psql', '-p', "$COORD1_PORT", "$TEST_DB",'-c', "DROP TABLE rol5;";
#add cleanup
system_or_bail 'pgxc_ctl', 'clean', 'all' ;

#delete related dirs for cleanup
system("rm -rf $dataDirRoot");
system("rm -rf $PGXC_CTL_HOME");
