#!/bin/bash
#
# This script sets up test environment for pgxc_clean.
# Please note that all the prepared transactions are
# partially committed or aborted.
#
# You should configure PGPORT and PGHOST to connect, as
# well as node names for your test environment.
#
# Before you run this script, XC should be up and ready.
# Also, this may try to drop test databases.   You may need
# to run CLEAN CONNECTION satement for each coordinator in
# advance.
#

export PGPORT=52898
export PGHOST=localhost
sourcedb=postgres

{
psql -e postgres <<EOF
drop table if exists t_1;
begin;
create table t_1 (a int);
prepare transaction 'test1_1';
set xc_maintenance_mode = on;
execute direct on (cn002) 'rollback prepared ''test1_1'' ';
set xc_maintenance_mode = off;
select pg_sleep(70);
\q
EOF
}&

{
psql -e postgres <<EOF
drop table if exists t_2;
begin;
create table t_2 (a int);
prepare transaction 'test2_1';
set xc_maintenance_mode = on;
execute direct on (dn002) 'rollback prepared ''test2_1'' ';
set xc_maintenance_mode = off;
select pg_sleep(70);
\q
EOF
}&


{
psql -e postgres <<EOF
drop table if exists t_3;
begin;
create table t_3 (a int);
prepare transaction 'test3_1';
set xc_maintenance_mode = on;
execute direct on (dn003) 'rollback prepared ''test3_1'' ';
set xc_maintenance_mode = off;
select pg_sleep(70);
\q
EOF
}&

{
psql -e postgres <<EOF
drop table if exists t_4;
begin;
create table t_4 (a int);
prepare transaction 'test4_1';
set xc_maintenance_mode = on;
execute direct on (dn003) 'rollback prepared ''test4_1'' ';
set xc_maintenance_mode = off;
select pg_sleep(70);
\q
EOF
}&

{
psql -e postgres <<EOF
drop table if exists t_5;
begin;
create table t_5 (a int);
prepare transaction 'test5_1';
set xc_maintenance_mode = on;
execute direct on (dn003) 'rollback prepared ''test5_1'' ';
set xc_maintenance_mode = off;
select pg_sleep(70);
\q
EOF
}&

{
psql -e postgres <<EOF
drop table if exists t_6;
begin;
create table t_6 (a int);
prepare transaction 'test6_1';
set xc_maintenance_mode = on;
execute direct on (dn003) 'rollback prepared ''test6_1'' ';
set xc_maintenance_mode = off;
select pg_sleep(70);
\q
EOF
}&

{
psql -e postgres <<EOF
drop table if exists t_7;
begin;
create table t_7 (a int);
prepare transaction 'test7_1';
set xc_maintenance_mode = on;
execute direct on (dn003) 'rollback prepared ''test7_1'' ';
set xc_maintenance_mode = off;
select pg_sleep(70);
\q
EOF
}&


{
psql -e postgres <<EOF
drop table if exists t_8;
begin;
create table t_8 (a int);
prepare transaction 'test8_1';
set xc_maintenance_mode = on;
execute direct on (dn003) 'rollback prepared ''test8_1'' ';
set xc_maintenance_mode = off;
select pg_sleep(70);
\q
EOF
}&

{
psql -e postgres <<EOF
drop table if exists t_9;
begin;
create table t_9 (a int);
prepare transaction 'test9_1';
set xc_maintenance_mode = on;
execute direct on (dn003) 'rollback prepared ''test9_1'' ';
set xc_maintenance_mode = off;
select pg_sleep(70);
\q
EOF
}&



{
psql -e postgres <<EOF
drop table if exists t_10;
begin;
create table t_10 (a int);
prepare transaction 'test10_1';
set xc_maintenance_mode = on;
execute direct on (dn003) 'rollback prepared ''test10_1'' ';
set xc_maintenance_mode = off;
select pg_sleep(70);
\q
EOF
}&

wait

psql -e postgres <<EOF
select * from pg_clean_check_txn();
select * from pg_clean_execute();
\q
EOF

